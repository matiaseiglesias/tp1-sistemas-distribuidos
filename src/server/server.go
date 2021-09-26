package server

import (
	"fmt"
	"net"

	"github.com/matiaseiglesias/tp1-sistemas-distribuidos/src/parser"
)

// ServerConfig Configuration used by the client
type ServerConfig struct {
	ID            string
	ServerAddress string
	NumParsers    int
	//LoopLapse     time.Duration
	//LoopPeriod    time.Duration
}

type LogServer struct {
	config         ServerConfig
	conn           net.Listener
	parser_channel chan net.Conn
}

func NewLogServer(c ServerConfig) *LogServer {
	ln, err := net.Listen("tcp", c.ServerAddress)
	if err != nil {
		fmt.Printf("Could not initialize server")
	}
	parser_ch := make(chan net.Conn, c.NumParsers)
	server := &LogServer{
		config:         c,
		conn:           ln,
		parser_channel: parser_ch,
	}
	return server

}

func AcceptNewConnection(s *LogServer) {
	conn, err := s.conn.Accept()
	if err != nil {
		fmt.Printf("Could not initialize server")
	}
	s.parser_channel <- conn
	fmt.Printf("New connection stablished")
}

func CloseConnection(s *LogServer) {
	close(s.parser_channel)
	s.conn.Close()
}

func InitPoolParser(s *LogServer) {
	p := parser.Parser{
		InConn: s.parser_channel,
	}

	go parser.Run(&p)
}

//type LogWriteConnection struct {
//	conn net.Conn
//}
//
//type LogReadConnection struct {
//	conn net.Conn
//}
//
