package protocol

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"

	"github.com/nsqio/nsq/internal/lg"
)

type TCPHandler interface {
	Handle(net.Conn)
}

func TCPServer(listener net.Listener, handler TCPHandler, logf lg.AppLogFunc) error {
	logf(lg.INFO, "TCP: listening on %s", listener.Addr())

	var wg sync.WaitGroup

	// 经典的 for + listen 方式
	for {
		// 接收 nsqd 服务请求
		clientConn, err := listener.Accept()
		if err != nil {
			// net.Error.Temporary() is deprecated, but is valid for accept
			// this is a hack to avoid a staticcheck error
			if te, ok := err.(interface{ Temporary() bool }); ok && te.Temporary() {
				logf(lg.WARN, "temporary Accept() failure - %s", err)
				// 主动让当前的协程让出 CPU
				runtime.Gosched()
				continue
			}
			// 非临时错误直接退出
			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("listener.Accept() error - %s", err)
			}
			break
		}

		wg.Add(1)
		// 每收到一个请求就启动一个单独的协程进行处理
		go func() {
			handler.Handle(clientConn)
			wg.Done()
		}()
	}

	// wait to return until all handler goroutines complete
	wg.Wait()

	logf(lg.INFO, "TCP: closing %s", listener.Addr())

	return nil
}
