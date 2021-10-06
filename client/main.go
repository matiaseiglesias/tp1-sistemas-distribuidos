package main

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/matiaseiglesias/tp1-sistemas-distribuidos/src/parser"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.BindEnv("id")
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		logrus.Infof("Configuration could not be read from config file.")
		errors.Wrapf(err, "Configuration could not be read from config file.")
	}

	return v, nil
}

func PrintConfig(v *viper.Viper) {

	logrus.Infof("Client configuration")
	logrus.Infof("Client Id: %s", v.GetString("id"))
	logrus.Infof("Read Server Address: %s", v.GetString("read_server.address"))
	logrus.Infof("Write Server Address: %s", v.GetString("write_server.address"))
	logrus.Infof("Readers: %d", v.GetInt("logs.read"))
	logrus.Infof("Writers: %d", v.GetInt("logs.write"))
	logrus.Infof("Log Level: %s", v.GetString("log.level"))
}

func InitLogger(logLevel string) error {
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		return err
	}

	logrus.SetLevel(level)
	return nil
}

func write(wg *sync.WaitGroup, dir, id string) {
	conn, err := net.Dial("tcp", dir)
	if err != nil {
		log.Fatalf(
			" Could not connect to server. Error: %v",
			err,
		)
	}
	log := &parser.WriteQuery{
		AppId:   id,
		Message: "test",
		LogTags: []string{"test", "prueba", "go"},
		//LogTags:   []string{"test", "prueba", "go", "distro1"},
		Timestamp: time.Now(),
	}
	query := &parser.Query{
		Write: *log,
	}
	err = parser.SendQuery(conn, *query)
	if err != nil {
		fmt.Println(err)
		conn.Close()
		wg.Done()
		return
	}
	parser.ReceiveResponse(conn)
	//fmt.Println("Recibi el ack", ok)
	time.Sleep(250 * time.Millisecond)
	conn.Close()
	wg.Done()
}

func read(wg *sync.WaitGroup, dir, id string) {
	conn, err := net.Dial("tcp", dir)
	if err != nil {
		log.Fatalf(
			" Could not connect to server. Error: %v",
			err,
		)
		wg.Done()
		return
	}

	log := &parser.ReadQuery{
		AppId: id,
		From:  time.Now(),
		To:    time.Now().Add(time.Hour),
		//Tag:   "prueba",
		Pattern: "test",
	}
	query := &parser.Query{
		Read: *log,
	}
	err = parser.SendQuery(conn, *query)
	if err != nil {
		conn.Close()
		wg.Done()
		return
	}
	logs := parser.ReceiveLogs(conn)

	for _, v := range logs {
		fmt.Println(v)
	}

	conn.Close()
	wg.Done()
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

	n := v.GetInt("logs.write")
	m := v.GetInt("logs.read")
	DirWriteServer := v.GetString("write_server.address")
	DirReadServer := v.GetString("read_server.address")

	var wg sync.WaitGroup
	wg.Add(n + m)

	for i := 0; i < n; i++ {
		go write(&wg, DirWriteServer, v.GetString("id"))
	}

	for i := 0; i < m; i++ {
		go read(&wg, DirReadServer, v.GetString("id"))
	}

	wg.Wait()
}
