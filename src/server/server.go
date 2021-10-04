package server

import (
	"net"
	"sync"

	"github.com/matiaseiglesias/tp1-sistemas-distribuidos/src/parser"
	"github.com/sirupsen/logrus"
)

// ServerConfig Configuration used by the client

type Server interface {
	CloseConnection()
}

type ServerConfig struct {
	ID            string
	ServerAddress string
	NumParsers    int
}

type logServer struct {
	config ServerConfig
	conn   net.Listener
}

type ReadLogServer struct {
	Server        *logServer
	ParserChannel chan net.Conn
	OutLogs       chan parser.Query
}

type WriteLogServer struct {
	Server        *logServer
	ParserChannel chan net.Conn
	OutLogs       chan parser.Query
}

func newLogServer(c ServerConfig) *logServer {
	ln, err := net.Listen("tcp", c.ServerAddress)
	if err != nil {
		logrus.Info("Could not initialize server")
	}
	server := &logServer{
		config: c,
		conn:   ln,
	}
	return server

}

func (s *logServer) acceptNewConnection() (net.Conn, error) {
	conn, err := s.conn.Accept()
	if err != nil {
		logrus.Info("Could not receive new connections")
		return nil, err
	}
	return conn, nil
}

func (s *logServer) closeConnection() {
	s.conn.Close()
}

func NewReadLogServer(c ServerConfig) *ReadLogServer {

	server := &ReadLogServer{
		Server:        newLogServer(c),
		ParserChannel: make(chan net.Conn, c.NumParsers),
		OutLogs:       make(chan parser.Query, c.NumParsers),
	}
	for i := 0; i < c.NumParsers; i++ {
		parser := &parser.Parser{
			Id:      "Prueba_1",
			InConn:  &server.ParserChannel,
			OutLogs: &server.OutLogs,
		}
		go parser.Run()

	}
	return server
}

func NewWriteLogServer(c ServerConfig) *WriteLogServer {

	server := &WriteLogServer{
		Server:        newLogServer(c),
		ParserChannel: make(chan net.Conn, c.NumParsers),
		OutLogs:       make(chan parser.Query, c.NumParsers),
	}
	for i := 0; i < c.NumParsers; i++ {
		parser := &parser.Parser{
			Id:      "Prueba_1",
			InConn:  &server.ParserChannel,
			OutLogs: &server.OutLogs,
		}
		go parser.Run()

	}
	return server
}

func (s *ReadLogServer) Run(wg *sync.WaitGroup) {
	for {
		conn, err := s.Server.acceptNewConnection()
		if err != nil {
			logrus.Info("Could not receive new connections")
			break // TODO solo en el caso de que se cierre la conexion
		}
		select {
		case s.ParserChannel <- conn:
		default:
			logrus.Info("Dropeo conexion")

		}
	}
	logrus.Info("Closing Read Server")
	wg.Done()
}

func (s *WriteLogServer) Run(wg *sync.WaitGroup) {
	for {
		conn, err := s.Server.acceptNewConnection()
		if err != nil {
			logrus.Info("Could not receive new connections")
			break // TODO solo en el caso de que se cierre la conexion
		}
		select {
		case s.ParserChannel <- conn:
		default:
			logrus.Info("Dropeo conexion")
			conn.Close()

		}

	}
	logrus.Info("Closing Write Server")
	wg.Done()
}

func (s *WriteLogServer) GetOutLogChan() *chan parser.Query {
	return &s.OutLogs
}
func (s *ReadLogServer) GetOutLogChan() *chan parser.Query {
	return &s.OutLogs
}

func (s *ReadLogServer) CloseConnection() {
	close(s.ParserChannel)
	s.Server.closeConnection()
}

func (s *WriteLogServer) CloseConnection() {
	close(s.ParserChannel)
	s.Server.closeConnection()
}
