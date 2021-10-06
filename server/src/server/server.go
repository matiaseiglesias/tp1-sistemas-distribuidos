package server

import (
	"net"
	"sync"

	"github.com/matiaseiglesias/tp1-sistemas-distribuidos/src/parser"
	"github.com/sirupsen/logrus"
)

type ServerConfig struct {
	ID            string
	ServerAddress string
	NumParsers    int
}

type LogServer struct {
	config        ServerConfig
	conn          net.Listener
	ParserChannel chan net.Conn
	OutLogs       chan parser.Query
}

func NewLogServer(c ServerConfig) *LogServer {
	ln, err := net.Listen("tcp", c.ServerAddress)
	if err != nil {
		logrus.Info("Could not initialize server")
	}
	server := &LogServer{
		config:        c,
		conn:          ln,
		ParserChannel: make(chan net.Conn, c.NumParsers),
		OutLogs:       make(chan parser.Query, c.NumParsers),
	}

	for i := 0; i < c.NumParsers; i++ {
		parser := &parser.Parser{
			Id:      "",
			InConn:  &server.ParserChannel,
			OutLogs: &server.OutLogs,
		}
		go parser.Run()

	}
	return server

}

func (s *LogServer) acceptNewConnection() (net.Conn, error) {
	conn, err := s.conn.Accept()
	if err != nil {
		logrus.Info("Could not receive new connections")
		return nil, err
	}
	return conn, nil
}

func (s *LogServer) CloseConnection() {
	close(s.ParserChannel)
	close(s.OutLogs)
	s.conn.Close()
}

func (s *LogServer) Run(wg *sync.WaitGroup) {
	for {
		conn, err := s.acceptNewConnection()
		if err != nil {
			logrus.Info("Could not receive new connections")
			break // TODO solo en el caso de que se cierre la conexion
		}
		select {
		case s.ParserChannel <- conn:
		default:
			r := &parser.Response{Conn: conn}
			r.SendBusyServer()
			logrus.Info("Dropeo conexion")
			conn.Close()

		}
	}
	logrus.Info("Closing Read Server")
	wg.Done()
}

func (s *LogServer) GetOutLogChan() *chan parser.Query {
	return &s.OutLogs
}
