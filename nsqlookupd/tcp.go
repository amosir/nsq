package nsqlookupd

import (
	"io"
	"net"
	"sync"

	"github.com/nsqio/nsq/internal/protocol"
)

type tcpServer struct {
	nsqlookupd *NSQLookupd
	conns      sync.Map
}

func (p *tcpServer) Handle(conn net.Conn) {
	p.nsqlookupd.logf(LOG_INFO, "TCP: new client(%s)", conn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	// 协议版本
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		p.nsqlookupd.logf(LOG_ERROR, "failed to read protocol version - %s", err)
		conn.Close()
		return
	}
	protocolMagic := string(buf)

	p.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): desired protocol magic '%s'",
		conn.RemoteAddr(), protocolMagic)

	// 当前只支持 V1 协议，非 V1协议会响应错误
	var prot protocol.Protocol
	switch protocolMagic {
	case "  V1":
		prot = &LookupProtocolV1{nsqlookupd: p.nsqlookupd}
	default:
		protocol.SendResponse(conn, []byte("E_BAD_PROTOCOL"))
		conn.Close()
		p.nsqlookupd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'",
			conn.RemoteAddr(), protocolMagic)
		return
	}

	client := prot.NewClient(conn)
	// 保存 nsqd 的地址和对应的连接
	p.conns.Store(conn.RemoteAddr(), client)
	// 处理 nsqd 的命令，连接正常时会不断地循环执行
	err = prot.IOLoop(client)
	if err != nil {
		p.nsqlookupd.logf(LOG_ERROR, "client(%s) - %s", conn.RemoteAddr(), err)
	}

	p.conns.Delete(conn.RemoteAddr())
	client.Close()
}

func (p *tcpServer) Close() {
	p.conns.Range(func(k, v interface{}) bool {
		v.(protocol.Client).Close()
		return true
	})
}
