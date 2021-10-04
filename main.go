package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/matiaseiglesias/tp1-sistemas-distribuidos/src/file_manager"
	"github.com/matiaseiglesias/tp1-sistemas-distribuidos/src/server"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		logrus.Infof("Configuration could not be read from config file.")
		errors.Wrapf(err, "Configuration could not be read from config file.")
	}

	return v, nil
}

// InitLogger Receives the log level to be set in logrus as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		return err
	}

	logrus.SetLevel(level)
	return nil
}

func signalListener(signal chan os.Signal, finished chan bool, s []server.Server) {

	sig := <-signal
	logrus.Infof("Signal catched: ", sig)
	finished <- true
	for _, v := range s {
		v.CloseConnection()
	}
}

func initSignalListener(s []server.Server) (chan bool, error) {

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go signalListener(sigs, done, s)

	return done, nil

}

// PrintConfig Print all the configuration parameters of the program.
// For debugging purposes only
func PrintConfig(v *viper.Viper) {
	logrus.Infof("Client configuration")
	//logrus.Infof("Client ID: %s", v.GetString("id"))
	logrus.Infof("Read Server Address: %s", v.GetString("read_server.address"))
	logrus.Infof("Write Server Address: %s", v.GetString("write_server.address"))
	logrus.Infof("Parser number: %d", v.GetInt("parser.num"))
	//logrus.Infof("Loop Lapse: %v", v.GetDuration("loop.lapse"))
	//logrus.Infof("Loop Period: %v", v.GetDuration("loop.period"))
	logrus.Infof("Log Level: %s", v.GetString("log.level"))
}

func main() {
	v, err := InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Fatalf("%s", err)
	}

	// Print program config with debugging purposes
	PrintConfig(v)

	readServerConfig := server.ServerConfig{
		ServerAddress: v.GetString("read_server.address"),
		NumParsers:    v.GetInt("parser.num"),
	}

	writeServerConfig := server.ServerConfig{
		ServerAddress: v.GetString("write_server.address"),
		NumParsers:    v.GetInt("parser.num"),
	}

	logrus.Infof("Initalizating LogServer")
	rs := server.NewReadLogServer(readServerConfig)
	ws := server.NewWriteLogServer(writeServerConfig)

	f_manager := file_manager.NewFileManager(rs.GetOutLogChan(), ws.GetOutLogChan())
	go f_manager.Run()

	done, _ := initSignalListener([]server.Server{rs, ws})

	var wg sync.WaitGroup

	wg.Add(2)

	go rs.Run(&wg)
	go ws.Run(&wg)

	<-done

	wg.Wait()

	logrus.Infof("Closing connections")
}
