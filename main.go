package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/matiaseiglesias/tp1-sistemas-distribuidos/server"
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
		fmt.Printf("Configuration could not be read from config file.")
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

// PrintConfig Print all the configuration parameters of the program.
// For debugging purposes only
func PrintConfig(v *viper.Viper) {
	logrus.Infof("Client configuration")
	//logrus.Infof("Client ID: %s", v.GetString("id"))
	logrus.Infof("Server Address: %s", v.GetString("server.address"))
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

	serverConfig := server.TcpServerConfig{
		ServerAddress: v.GetString("server.address"),
		//ID:            v.GetString("id"),
		//LoopLapse:     v.GetDuration("loop.lapse"),
		//LoopPeriod:    v.GetDuration("loop.period"),
	}

	s := server.NewLogServer(serverConfig)
	server.AcceptNewConnection(s)
	server.InitPoolParser(s)

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter text: ")
	text, _ := reader.ReadString('\n')
	fmt.Println(text)

	server.CloseConnection(s)
}
