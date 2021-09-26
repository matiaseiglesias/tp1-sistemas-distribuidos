package server

import (
	"fmt"
	"net"
)

// ClientConfig Configuration used by the client
type TcpServerConfig struct {
	ID            string
	ServerAddress string
	//LoopLapse     time.Duration
	//LoopPeriod    time.Duration
}

type TcpServer struct {
	config TcpServerConfig
	conn   net.Listener
}

func NewTcpServer(c TcpServerConfig) *TcpServer {
	ln, err := net.Listen("tcp", c.ServerAddress)
	if err != nil {
		fmt.Printf("Could not initialize server")
	}
	server := &TcpServer{
		config: c,
		conn:   ln,
	}
	return server

}

func AcceptNewConnection(s *TcpServer) {
	s.conn.Accept()
	fmt.Printf("New connection stablished")
}

func CloseConnection(s *TcpServer) {
	s.conn.Close()
}
