package nsqd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/dirlock"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/statsd"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

const (
	TLSNotRequired = iota
	TLSRequiredExceptHTTP
	TLSRequired
)

type errStore struct {
	err error
}

type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	// 节点内递增的客户端 id 序号，为每个到来的客户端请求分配唯一的标识 id
	clientIDSequence int64

	sync.RWMutex
	ctx context.Context
	// ctxCancel cancels a context that main() is waiting on
	ctxCancel context.CancelFunc

	opts atomic.Value

	dl *dirlock.DirLock
	// 当前是否在加载数据(重启加载)
	isLoading int32
	isExiting int32
	errValue  atomic.Value
	startTime time.Time

	// nsqd 下的所有 topic 集合
	topicMap map[string]*Topic

	// 所连接的 nsqlookupd 服务信息，包括地址、端口等
	lookupPeers atomic.Value

	// nsqd 节点下的 tcp 服务，用于接收处理来自客户端的各种请求指令
	tcpServer *tcpServer

	// 不同端口的 listener
	tcpListener   net.Listener
	httpListener  net.Listener
	httpsListener net.Listener
	tlsConfig     *tls.Config

	poolSize int

	// TOPIC 和 CHANENL 状态发生变更时，会将变更内容写入到 notifyChan，由 lookupLoop 协程处理
	notifyChan           chan interface{}
	optsNotificationChan chan struct{}

	// 用于回收 goroutine
	exitChan  chan int
	waitGroup util.WaitGroupWrapper

	// 集群信息
	ci *clusterinfo.ClusterInfo
}

func New(opts *Options) (*NSQD, error) {
	var err error

	dataPath := opts.DataPath
	if opts.DataPath == "" {
		cwd, _ := os.Getwd()
		dataPath = cwd
	}
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}

	n := &NSQD{
		startTime:            time.Now(),
		topicMap:             make(map[string]*Topic),
		exitChan:             make(chan int),
		notifyChan:           make(chan interface{}),
		optsNotificationChan: make(chan struct{}, 1),
		dl:                   dirlock.New(dataPath),
	}
	n.ctx, n.ctxCancel = context.WithCancel(context.Background())
	httpcli := http_api.NewClient(nil, opts.HTTPClientConnectTimeout, opts.HTTPClientRequestTimeout)
	n.ci = clusterinfo.New(n.logf, httpcli)

	n.lookupPeers.Store([]*lookupPeer{})

	n.swapOpts(opts)
	n.errValue.Store(errStore{})

	err = n.dl.Lock()
	if err != nil {
		return nil, fmt.Errorf("failed to lock data-path: %v", err)
	}

	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 {
		return nil, errors.New("--max-deflate-level must be [1,9]")
	}

	if opts.ID < 0 || opts.ID >= 1024 {
		return nil, errors.New("--node-id must be [0,1024)")
	}

	if opts.TLSClientAuthPolicy != "" && opts.TLSRequired == TLSNotRequired {
		opts.TLSRequired = TLSRequired
	}

	// 构造 TLSConfig
	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config - %s", err)
	}
	if tlsConfig == nil && opts.TLSRequired != TLSNotRequired {
		return nil, errors.New("cannot require TLS client connections without TLS key and cert")
	}
	n.tlsConfig = tlsConfig

	for _, v := range opts.E2EProcessingLatencyPercentiles {
		if v <= 0 || v > 1 {
			return nil, fmt.Errorf("invalid E2E processing latency percentile: %v", v)
		}
	}

	n.logf(LOG_INFO, version.String("nsqd"))
	n.logf(LOG_INFO, "ID: %d", opts.ID)

	n.tcpServer = &tcpServer{nsqd: n}
	// 创建 TCP服务的监听器
	n.tcpListener, err = net.Listen(util.TypeOfAddr(opts.TCPAddress), opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}
	if opts.HTTPAddress != "" {
		n.httpListener, err = net.Listen(util.TypeOfAddr(opts.HTTPAddress), opts.HTTPAddress)
		if err != nil {
			return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPAddress, err)
		}
	}
	if n.tlsConfig != nil && opts.HTTPSAddress != "" {
		n.httpsListener, err = tls.Listen("tcp", opts.HTTPSAddress, n.tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPSAddress, err)
		}
	}
	if opts.BroadcastHTTPPort == 0 {
		tcpAddr, ok := n.RealHTTPAddr().(*net.TCPAddr)
		if ok {
			opts.BroadcastHTTPPort = tcpAddr.Port
		}
	}

	if opts.BroadcastTCPPort == 0 {
		tcpAddr, ok := n.RealTCPAddr().(*net.TCPAddr)
		if ok {
			opts.BroadcastTCPPort = tcpAddr.Port
		}
	}

	if opts.StatsdPrefix != "" {
		var port string = fmt.Sprint(opts.BroadcastHTTPPort)
		statsdHostKey := statsd.HostKey(net.JoinHostPort(opts.BroadcastAddress, port))
		prefixWithHost := strings.Replace(opts.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		opts.StatsdPrefix = prefixWithHost
	}

	return n, nil
}

func (n *NSQD) getOpts() *Options {
	return n.opts.Load().(*Options)
}

func (n *NSQD) swapOpts(opts *Options) {
	n.opts.Store(opts)
}

func (n *NSQD) triggerOptsNotification() {
	select {
	case n.optsNotificationChan <- struct{}{}:
	default:
	}
}

func (n *NSQD) RealTCPAddr() net.Addr {
	if n.tcpListener == nil {
		return &net.TCPAddr{}
	}
	return n.tcpListener.Addr()

}

func (n *NSQD) RealHTTPAddr() net.Addr {
	if n.httpListener == nil {
		return &net.TCPAddr{}
	}
	return n.httpListener.Addr()
}

func (n *NSQD) RealHTTPSAddr() *net.TCPAddr {
	if n.httpsListener == nil {
		return &net.TCPAddr{}
	}
	return n.httpsListener.Addr().(*net.TCPAddr)
}

func (n *NSQD) SetHealth(err error) {
	n.errValue.Store(errStore{err: err})
}

func (n *NSQD) IsHealthy() bool {
	return n.GetError() == nil
}

func (n *NSQD) GetError() error {
	errValue := n.errValue.Load()
	return errValue.(errStore).err
}

func (n *NSQD) GetHealth() string {
	err := n.GetError()
	if err != nil {
		return fmt.Sprintf("NOK - %s", err)
	}
	return "OK"
}

func (n *NSQD) GetStartTime() time.Time {
	return n.startTime
}

func (n *NSQD) Main() error {
	exitCh := make(chan error)
	var once sync.Once
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				n.logf(LOG_FATAL, "%s", err)
			}
			exitCh <- err
		})
	}

	// 启动 tcp 服务，提供 publish 和 subscribe
	n.waitGroup.Wrap(func() {
		exitFunc(protocol.TCPServer(n.tcpListener, n.tcpServer, n.logf))
	})

	// 启动 http server，提供 publish 流程有关的 api
	if n.httpListener != nil {
		httpServer := newHTTPServer(n, false, n.getOpts().TLSRequired == TLSRequired)
		n.waitGroup.Wrap(func() {
			exitFunc(http_api.Serve(n.httpListener, httpServer, "HTTP", n.logf))
		})
	}

	// 启动 https server
	if n.httpsListener != nil {
		httpsServer := newHTTPServer(n, true, true)
		n.waitGroup.Wrap(func() {
			exitFunc(http_api.Serve(n.httpsListener, httpsServer, "HTTPS", n.logf))
		})
	}

	// 异步启动一个 gorouting 执行 NSQD.queueScanLoop() 方法，该方法的用途就是定时轮询 inFlightPQ 和 deferredPQ，取出其中达到时间条件的消息进行超时重传
	n.waitGroup.Wrap(n.queueScanLoop)
	// 单独的协程处理和 nsqlookupd 之间的连接，负责定时同步nsqd的数据（topic,channel等）到 nsqlookupd
	n.waitGroup.Wrap(n.lookupLoop)
	// statsdLoop每隔一段时间将nsqd的一些统计信息通过udp协议发送给nsq_stat，上报的信息包括:
	// 每个topic的消息数量增量、消息所占的空间增量，每个channel的消息数量增量，超时的消息数量，deferred队列的消息数量，内存和GC的信息等等
	// 如果没有配置nsq_stat的地址，将不会启动
	if n.getOpts().StatsdAddress != "" {
		n.waitGroup.Wrap(n.statsdLoop)
	}

	err := <-exitCh
	return err
}

// Metadata is the collection of persistent information about the current NSQD.
type Metadata struct {
	Topics  []TopicMetadata `json:"topics"`
	Version string          `json:"version"`
}

// TopicMetadata is the collection of persistent information about a topic.
type TopicMetadata struct {
	Name     string            `json:"name"`
	Paused   bool              `json:"paused"`
	Channels []ChannelMetadata `json:"channels"`
}

// ChannelMetadata is the collection of persistent information about a channel.
type ChannelMetadata struct {
	Name   string `json:"name"`
	Paused bool   `json:"paused"`
}

func newMetadataFile(opts *Options) string {
	return path.Join(opts.DataPath, "nsqd.dat")
}

func readOrEmpty(fn string) ([]byte, error) {
	data, err := os.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read metadata from %s - %s", fn, err)
		}
	}
	return data, nil
}

func writeSyncFile(fn string, data []byte) error {
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	return err
}

func (n *NSQD) LoadMetadata() error {
	// 设置加载状态
	atomic.StoreInt32(&n.isLoading, 1)
	defer atomic.StoreInt32(&n.isLoading, 0)

	// 数据加载路径
	fn := newMetadataFile(n.getOpts())

	// 读取文件
	data, err := readOrEmpty(fn)
	if err != nil {
		return err
	}

	// 文件不存在，说明无需加载
	if data == nil {
		return nil // fresh start
	}

	// 解析文件中保存的数据，从这里可知这些数据是以 json 格式保存的
	var m Metadata
	err = json.Unmarshal(data, &m)
	if err != nil {
		return fmt.Errorf("failed to parse metadata in %s - %s", fn, err)
	}

	// 依次处理每个 topic 及其下的 channel
	for _, t := range m.Topics {
		if !protocol.IsValidTopicName(t.Name) {
			n.logf(LOG_WARN, "skipping creation of invalid topic %s", t.Name)
			continue
		}
		// 这里尝试获取 topic，不存在会创建新的
		topic := n.GetTopic(t.Name)
		if t.Paused {
			// 根据配置确定是否要将 topic 暂停
			topic.Pause()
		}
		for _, c := range t.Channels {
			if !protocol.IsValidChannelName(c.Name) {
				n.logf(LOG_WARN, "skipping creation of invalid channel %s", c.Name)
				continue
			}
			channel := topic.GetChannel(c.Name)
			if c.Paused {
				channel.Pause()
			}
		}
		topic.Start()
	}
	return nil
}

// GetMetadata retrieves the current topic and channel set of the NSQ daemon. If
// the ephemeral flag is set, ephemeral topics are also returned even though these
// are not saved to disk.
func (n *NSQD) GetMetadata(ephemeral bool) *Metadata {
	meta := &Metadata{
		Version: version.Binary,
	}
	for _, topic := range n.topicMap {
		if topic.ephemeral && !ephemeral {
			continue
		}
		topicData := TopicMetadata{
			Name:   topic.name,
			Paused: topic.IsPaused(),
		}
		topic.Lock()
		for _, channel := range topic.channelMap {
			if channel.ephemeral {
				continue
			}
			topicData.Channels = append(topicData.Channels, ChannelMetadata{
				Name:   channel.name,
				Paused: channel.IsPaused(),
			})
		}
		topic.Unlock()
		meta.Topics = append(meta.Topics, topicData)
	}
	return meta
}

func (n *NSQD) PersistMetadata() error {
	// persist metadata about what topics/channels we have, across restarts
	fileName := newMetadataFile(n.getOpts())

	n.logf(LOG_INFO, "NSQ: persisting topic/channel metadata to %s", fileName)

	data, err := json.Marshal(n.GetMetadata(false))
	if err != nil {
		return err
	}
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	err = writeSyncFile(tmpFileName, data)
	if err != nil {
		return err
	}
	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		return err
	}
	// technically should fsync DataPath here

	return nil
}

func (n *NSQD) Exit() {
	// 设置退出状态
	if !atomic.CompareAndSwapInt32(&n.isExiting, 0, 1) {
		// avoid double call
		return
	}
	// 依次关闭 tcpserver、httpserver、httpsserver
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}

	if n.tcpServer != nil {
		n.tcpServer.Close()
	}

	if n.httpListener != nil {
		n.httpListener.Close()
	}

	if n.httpsListener != nil {
		n.httpsListener.Close()
	}

	// 将 topic 和 channel 信息写入磁盘文件，重启时可以直接加载，无需重新创建
	n.Lock()
	err := n.PersistMetadata()
	if err != nil {
		n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
	}
	n.logf(LOG_INFO, "NSQ: closing topics")
	// 关闭所有的 topic 机器下属的 channel
	for _, topic := range n.topicMap {
		topic.Close()
	}
	n.Unlock()

	n.logf(LOG_INFO, "NSQ: stopping subsystems")
	close(n.exitChan)
	n.waitGroup.Wait()
	n.dl.Unlock()
	n.logf(LOG_INFO, "NSQ: bye")
	n.ctxCancel()
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
func (n *NSQD) GetTopic(topicName string) *Topic {
	// most likely we already have this topic, so try read lock first
	n.RLock()
	t, ok := n.topicMap[topicName]
	n.RUnlock()
	if ok {
		return t
	}

	n.Lock()

	// 尝试获取 topic
	t, ok = n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	}

	// 如果不存在则创建新的 topic
	deleteCallback := func(t *Topic) {
		n.DeleteExistingTopic(t.name)
	}
	t = NewTopic(topicName, n, deleteCallback)
	n.topicMap[topicName] = t

	n.Unlock()

	n.logf(LOG_INFO, "TOPIC(%s): created", t.name)
	// topic is created but messagePump not yet started

	// if this topic was created while loading metadata at startup don't do any further initialization
	// (topic will be "started" after loading completes)
	if atomic.LoadInt32(&n.isLoading) == 1 {
		return t
	}

	// if using lookupd, make a blocking call to get channels and immediately create them
	// to ensure that all channels receive published messages
	// 获取该 nsqd 服务连接的所有 nsqlookupd 服务地址
	lookupdHTTPAddrs := n.lookupdHTTPAddrs()
	if len(lookupdHTTPAddrs) > 0 {
		// 查询所有 nsqlookupd 服务上 该 topic 下注册的所有 channel
		channelNames, err := n.ci.GetLookupdTopicChannels(t.name, lookupdHTTPAddrs)
		if err != nil {
			n.logf(LOG_WARN, "failed to query nsqlookupd for channels to pre-create for topic %s - %s", t.name, err)
		}
		// 查询或创建 channel
		for _, channelName := range channelNames {
			if strings.HasSuffix(channelName, "#ephemeral") {
				continue // do not create ephemeral channel with no consumer client
			}
			t.GetChannel(channelName)
		}
	} else if len(n.getOpts().NSQLookupdTCPAddresses) > 0 {
		n.logf(LOG_ERROR, "no available nsqlookupd to query for channels to pre-create for topic %s", t.name)
	}

	// 创建 topic 时启动的 messagePump 协程会阻塞在 startChan 上，这里通过向 startChan 写入数据，接触该协程的阻塞
	t.Start()
	return t
}

// GetExistingTopic gets a topic only if it exists
func (n *NSQD) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic removes a topic only if it exists
func (n *NSQD) DeleteExistingTopic(topicName string) error {
	n.RLock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.RUnlock()
		return errors.New("topic does not exist")
	}
	n.RUnlock()

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	topic.Delete()

	n.Lock()
	delete(n.topicMap, topicName)
	n.Unlock()

	return nil
}

func (n *NSQD) Notify(v interface{}, persist bool) {
	// since the in-memory metadata is incomplete,
	// should not persist metadata while loading it.
	// nsqd will call `PersistMetadata` it after loading
	loading := atomic.LoadInt32(&n.isLoading) == 1
	n.waitGroup.Wrap(func() {
		// by selecting on exitChan we guarantee that
		// we do not block exit, see issue #123
		select {
		case <-n.exitChan:
		case n.notifyChan <- v:
			if loading || !persist {
				return
			}
			n.Lock()
			err := n.PersistMetadata()
			if err != nil {
				n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
			}
			n.Unlock()
		}
	})
}

// channels returns a flat slice of all channels in all topics
func (n *NSQD) channels() []*Channel {
	var channels []*Channel
	n.RLock()
	for _, t := range n.topicMap {
		t.RLock()
		for _, c := range t.channelMap {
			channels = append(channels, c)
		}
		t.RUnlock()
	}
	n.RUnlock()
	return channels
}

// resizePool adjusts the size of the pool of queueScanWorker goroutines
//
//	1 <= pool <= min(num * 0.25, QueueScanWorkerPoolMax)
func (n *NSQD) resizePool(num int, workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	// 确定协程池大小
	idealPoolSize := int(float64(num) * 0.25)
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > n.getOpts().QueueScanWorkerPoolMax {
		idealPoolSize = n.getOpts().QueueScanWorkerPoolMax
	}
	for {
		// 这种情况下无需改变协程池大小
		if idealPoolSize == n.poolSize {
			break
		} else if idealPoolSize < n.poolSize {
			// 协程数过多，需要减少
			// contract
			closeCh <- 1
			n.poolSize--
		} else {
			// expand
			// 协程数不够，需要增加新的协程
			n.waitGroup.Wrap(func() {
				// 每当通过 workCh 接收到 channel 时，会调用 Channel.processInFlightQueue(...) 方法，完成扫描任务
				n.queueScanWorker(workCh, responseCh, closeCh)
			})
			n.poolSize++
		}
	}
}

// queueScanWorker receives work (in the form of a channel) from queueScanLoop
// and processes the deferred and in-flight queues
// 定时轮询延时队列，找到满足时间条件的消息，调用 Channel.put(...) 方法发送消息
func (n *NSQD) queueScanWorker(workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	for {
		select {
		// queueScanLoop 周期地向 workCh 写入选中的 channel
		case c := <-workCh:
			now := time.Now().UnixNano()
			// 用于表示是否有消息被处理
			dirty := false
			// 尝试从 inFlightPQ 堆顶弹出达到执行时间的消息，然后调用 Channel.put(...) 方法将消息传递到某个订阅了该 channel 的 client 手中
			if c.processInFlightQueue(now) {
				// 这里如果有消息被处理，dirty 置为 true
				dirty = true
			}
			// 从延迟队列中弹出达到执行时间的消息
			if c.processDeferredQueue(now) {
				dirty = true
			}
			responseCh <- dirty
		case <-closeCh:
			return
		}
	}
}

// queueScanLoop runs in a single goroutine to process in-flight and deferred
// priority queues. It manages a pool of queueScanWorker (configurable max of
// QueueScanWorkerPoolMax (default: 4)) that process channels concurrently.
//
// It copies Redis's probabilistic expiration algorithm: it wakes up every
// QueueScanInterval (default: 100ms) to select a random QueueScanSelectionCount
// (default: 20) channels from a locally cached list (refreshed every
// QueueScanRefreshInterval (default: 5s)).
//
// If either of the queues had work to do the channel is considered "dirty".
//
// If QueueScanDirtyPercent (default: 25%) of the selected channels were dirty,
// the loop continues without sleep.
func (n *NSQD) queueScanLoop() {
	workCh := make(chan *Channel, n.getOpts().QueueScanSelectionCount)
	responseCh := make(chan bool, n.getOpts().QueueScanSelectionCount)
	closeCh := make(chan int)

	// 轮询时间间隔，周期性地扫描 infight 队列和 defered 队列，并处理其中达到发送时间的消息
	workTicker := time.NewTicker(n.getOpts().QueueScanInterval)
	// 周期性地更新 scan worker协程池队列
	refreshTicker := time.NewTicker(n.getOpts().QueueScanRefreshInterval)

	channels := n.channels()
	// 用于调整协程池中的协程数，协程池扩容时会启动 queueScanWorker 协程
	n.resizePool(len(channels), workCh, responseCh, closeCh)

	for {
		select {
		case <-workTicker.C:
			if len(channels) == 0 {
				continue
			}
		case <-refreshTicker.C:
			channels = n.channels()
			n.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		case <-n.exitChan:
			goto exit
		}

		num := n.getOpts().QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

	loop:
		// 随机选择一些 channel 作为将需要扫描的 channel，通过 workCh 发给 queueScanWorker 协程
		for _, i := range util.UniqRands(num, len(channels)) {
			workCh <- channels[i]
		}

		// queueScanWorker 协程从 workCh 中取出选中的 channel，从中读取并处理消息，如果有消息被处理，就会向 responseCh 中写入 true
		numDirty := 0
		for i := 0; i < num; i++ {
			if <-responseCh {
				numDirty++
			}
		}

		// 猜想这里是计算当前周期中有多少个 channel 有延迟消息或需要重新发送的消息，超过一定比率说明可能有大量消息积压，因此需要在当前周期多处理一批消息
		if float64(numDirty)/float64(num) > n.getOpts().QueueScanDirtyPercent {
			goto loop
		}
	}

exit:
	n.logf(LOG_INFO, "QUEUESCAN: closing")
	close(closeCh)
	workTicker.Stop()
	refreshTicker.Stop()
}

func buildTLSConfig(opts *Options) (*tls.Config, error) {
	var tlsConfig *tls.Config

	if opts.TLSCert == "" && opts.TLSKey == "" {
		return nil, nil
	}

	tlsClientAuthPolicy := tls.VerifyClientCertIfGiven

	// 指定服务器证书和私钥
	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
	if err != nil {
		return nil, err
	}
	switch opts.TLSClientAuthPolicy {
	case "require":
		tlsClientAuthPolicy = tls.RequireAnyClientCert
	case "require-verify":
		tlsClientAuthPolicy = tls.RequireAndVerifyClientCert
	default:
		tlsClientAuthPolicy = tls.NoClientCert
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsClientAuthPolicy,
		// 指定支持的最小和最大 TLS 版本
		MinVersion: opts.TLSMinVersion,
	}

	if opts.TLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := os.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			return nil, errors.New("failed to append certificate to pool")
		}
		// 指定根证书颁发机构的证书池，以便验证服务器证书
		tlsConfig.ClientCAs = tlsCertPool
	}

	return tlsConfig, nil
}

func (n *NSQD) IsAuthEnabled() bool {
	return len(n.getOpts().AuthHTTPAddresses) != 0
}

// Context returns a context that will be canceled when nsqd initiates the shutdown
func (n *NSQD) Context() context.Context {
	return n.ctx
}
