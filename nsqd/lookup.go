package nsqd

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/version"
)

func connectCallback(n *NSQD, hostname string) func(*lookupPeer) {
	return func(lp *lookupPeer) {
		ci := make(map[string]interface{})
		ci["version"] = version.Binary
		ci["tcp_port"] = n.getOpts().BroadcastTCPPort
		ci["http_port"] = n.getOpts().BroadcastHTTPPort
		ci["hostname"] = hostname
		ci["broadcast_address"] = n.getOpts().BroadcastAddress

		// 构造 identify 命令
		cmd, err := nsq.Identify(ci)
		if err != nil {
			lp.Close()
			return
		}

		// 向 nsqdlookup 发送 identify 命令，该命令包含了 nsqd 的基本信息(主机名、端口、版本等)
		resp, err := lp.Command(cmd)
		if err != nil {
			n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lp, cmd, err)
			return
		} else if bytes.Equal(resp, []byte("E_INVALID")) {
			n.logf(LOG_INFO, "LOOKUPD(%s): lookupd returned %s", lp, resp)
			lp.Close()
			return
		}

		err = json.Unmarshal(resp, &lp.Info)
		if err != nil {
			n.logf(LOG_ERROR, "LOOKUPD(%s): parsing response - %s", lp, resp)
			lp.Close()
			return
		}
		n.logf(LOG_INFO, "LOOKUPD(%s): peer info %+v", lp, lp.Info)
		if lp.Info.BroadcastAddress == "" {
			n.logf(LOG_ERROR, "LOOKUPD(%s): no broadcast address", lp)
		}

		// build all the commands first so we exit the lock(s) as fast as possible
		var commands []*nsq.Command
		n.RLock()
		for _, topic := range n.topicMap {
			topic.RLock()
			if len(topic.channelMap) == 0 {
				commands = append(commands, nsq.Register(topic.name, ""))
			} else {
				for _, channel := range topic.channelMap {
					commands = append(commands, nsq.Register(channel.topicName, channel.name))
				}
			}
			topic.RUnlock()
		}
		n.RUnlock()

		for _, cmd := range commands {
			n.logf(LOG_INFO, "LOOKUPD(%s): %s", lp, cmd)
			_, err := lp.Command(cmd)
			if err != nil {
				n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lp, cmd, err)
				return
			}
		}
	}
}

func (n *NSQD) lookupLoop() {
	var lookupPeers []*lookupPeer
	var lookupAddrs []string
	connect := true

	hostname, err := os.Hostname()
	if err != nil {
		n.logf(LOG_FATAL, "failed to get hostname - %s", err)
		os.Exit(1)
	}

	// for announcements, lookupd determines the host automatically
	// 这里创建一个定时器，默认时间间隔是 15 秒
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	// 也是经典的 for + select 方式
	for {
		if connect {
			// 连接每一个 nsqdlookup 服务
			for _, host := range n.getOpts().NSQLookupdTCPAddresses {
				if in(host, lookupAddrs) {
					continue
				}
				n.logf(LOG_INFO, "LOOKUP(%s): adding peer", host)
				// connectCallback
				lookupPeer := newLookupPeer(host, n.getOpts().MaxBodySize, n.logf,
					connectCallback(n, hostname))
				lookupPeer.Command(nil) // start the connection
				lookupPeers = append(lookupPeers, lookupPeer)
				lookupAddrs = append(lookupAddrs, host)
			}
			n.lookupPeers.Store(lookupPeers)
			connect = false
		}

		select {
		case <-ticker.C:
			// send a heartbeat and read a response (read detects closed conns)
			// 每隔一段时间，向 nsqlookupd 发送心跳，以维持存活状态
			// nsqlookupd 收到 nsqd 服务的 PING 命令请求后，会更新 lastUpdate 记录的时间，消费者通过 nsqlookupd 服务获取 nsqd 服务列表时，会过滤掉 lastUpdate 长时间未更新的 nsqd 节点
			for _, lookupPeer := range lookupPeers {
				n.logf(LOG_DEBUG, "LOOKUPD(%s): sending heartbeat", lookupPeer)
				cmd := nsq.Ping()
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lookupPeer, cmd, err)
				}
			}
		case val := <-n.notifyChan:
			// topic 或 channel 状态变更时(如新增 topic)，会将 topic 写入 notifyChan
			// 这里进行消费，向 NSQDLOOKUP 发送注册或反注册命令
			var cmd *nsq.Command
			var branch string

			switch val := val.(type) {
			case *Channel:
				// notify all nsqlookupds that a new channel exists, or that it's removed
				branch = "channel"
				channel := val
				if channel.Exiting() {
					cmd = nsq.UnRegister(channel.topicName, channel.name)
				} else {
					cmd = nsq.Register(channel.topicName, channel.name)
				}
			case *Topic:
				// notify all nsqlookupds that a new topic exists, or that it's removed
				branch = "topic"
				topic := val
				if topic.Exiting() {
					cmd = nsq.UnRegister(topic.name, "")
				} else {
					cmd = nsq.Register(topic.name, "")
				}
			}

			for _, lookupPeer := range lookupPeers {
				n.logf(LOG_INFO, "LOOKUPD(%s): %s %s", lookupPeer, branch, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lookupPeer, cmd, err)
				}
			}
		case <-n.optsNotificationChan:
			var tmpPeers []*lookupPeer
			var tmpAddrs []string
			for _, lp := range lookupPeers {
				if in(lp.addr, n.getOpts().NSQLookupdTCPAddresses) {
					tmpPeers = append(tmpPeers, lp)
					tmpAddrs = append(tmpAddrs, lp.addr)
					continue
				}
				n.logf(LOG_INFO, "LOOKUP(%s): removing peer", lp)
				lp.Close()
			}
			lookupPeers = tmpPeers
			lookupAddrs = tmpAddrs
			connect = true
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	n.logf(LOG_INFO, "LOOKUP: closing")
}

func in(s string, lst []string) bool {
	for _, v := range lst {
		if s == v {
			return true
		}
	}
	return false
}

func (n *NSQD) lookupdHTTPAddrs() []string {
	var lookupHTTPAddrs []string
	lookupPeers := n.lookupPeers.Load()
	if lookupPeers == nil {
		return nil
	}
	for _, lp := range lookupPeers.([]*lookupPeer) {
		if len(lp.Info.BroadcastAddress) <= 0 {
			continue
		}
		addr := net.JoinHostPort(lp.Info.BroadcastAddress, strconv.Itoa(lp.Info.HTTPPort))
		lookupHTTPAddrs = append(lookupHTTPAddrs, addr)
	}
	return lookupHTTPAddrs
}
