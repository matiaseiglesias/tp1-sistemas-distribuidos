package ioworker

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/matiaseiglesias/tp1-sistemas-distribuidos/src/parser"
)

//func Read(log parser.Log, name_file string) ([]parser.Log, error) {

type Ack struct {
	AppId string
	Read  bool
	Write bool
}

func hasTag(path, tag string) (bool, error) {

	e, _ := exists(path)
	if !e {
		return false, nil
	}

	file, err := os.Open(path + ".tag")
	if err != nil {
		fmt.Println("no se pude abrir el archivo", path)
		return false, errors.New("no se pude abrir el archivo")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K
	for scanner.Scan() {
		if tag == string(scanner.Bytes()) {
			return true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, errors.New("no se pude leer el archivo")
		log.Fatal(err)
	}
	return false, nil
}

func readFile(path string, logs *[]parser.WriteQuery, f parser.Filter) (*[]parser.WriteQuery, error) {

	e, _ := exists(path)
	if !e {
		return nil, errors.New("archivo inexistente")
	}
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("no se pude abrir el archivo", path)
		return nil, errors.New("no se pude abrir el archivo")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	for scanner.Scan() {
		p := &parser.WriteQuery{}
		json.Unmarshal(scanner.Bytes(), p)
		if p.PassFilter(f) {
			*logs = append(*logs, *p)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.New("no se pude leer el archivo")
		log.Fatal(err)
	}
	return logs, nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func readAll(id string, f parser.Filter) ([]parser.WriteQuery, error) {

	files, err := ioutil.ReadDir("./" + id + "/")
	if err != nil {
		log.Fatal(err)
	}
	logs := make([]parser.WriteQuery, 0)
	for _, file := range files {
		file_path := "./" + id + "/" + file.Name()
		if strings.HasSuffix(file_path, ".tag") {
			continue
		}
		if f.HasTag() {
			ok, _ := hasTag(file_path, f.Tag)
			if ok {
				readFile(file_path, &logs, f)
			}
		} else {
			readFile(file_path, &logs, f)
		}
	}
	return logs, nil
}

func readFromTo(id string, f parser.Filter, from, to time.Time) ([]parser.WriteQuery, error) {

	from = from.Truncate(time.Hour)
	to = to.Round(time.Hour)

	logs := make([]parser.WriteQuery, 0)
	for !from.Equal(to) {
		file_path := parser.ToPathName(id, from)
		from = from.Add(time.Hour)

		if f.HasTag() {
			ok, _ := hasTag(file_path, f.Tag)
			if ok {
				readFile(file_path, &logs, f)
			}
		} else {
			readFile(file_path, &logs, f)
		}
	}
	return logs, nil
}

func Read(logs *chan parser.Query, ack *chan Ack) {

	for l := range *logs {
		fmt.Println("--------------------------Leyendo--------------------------")
		knownApp, _ := exists("./" + l.Read.AppId)
		if !knownApp {
			r := parser.Response{Conn: l.Conn}
			r.SendUnkownAppId()
			l.Conn.Close()
			*ack <- Ack{
				AppId: l.Read.AppId,
				Read:  true,
			}
			continue
		}
		filter := l.Read.GetFilter()
		if l.Read.HasTimeFilter() {
			fmt.Println("Leyendo for dias")
			logs, _ := readFromTo(l.Read.AppId, filter, l.Read.From, l.Read.To)
			parser.SendLogs(l.Conn, &logs)
			*ack <- Ack{
				AppId: l.Read.AppId,
				Read:  true,
			}
		} else {
			fmt.Println("Leyendo Todo")
			logs, _ := readAll(l.Read.AppId, filter)
			parser.SendLogs(l.Conn, &logs)
			*ack <- Ack{
				AppId: l.Read.AppId,
				Read:  true,
			}
		}
		l.Conn.Close()
	}
}

func writeTags(path string, t []string) error {

	file, _ := os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	for _, tag := range t {
		file.Write([]byte(tag))
		file.WriteString("\n")
	}
	return nil
}

func Write(logs *chan parser.Query, ack *chan Ack) error {

	for l := range *logs {

		knownApp, _ := exists(l.Write.AppId)
		if !knownApp {
			err := os.Mkdir(l.Write.AppId, 0755)
			if err != nil {
				log.Fatal(err)
			}
		}
		path_name := parser.ToPathName(l.Write.AppId, l.Write.Timestamp)
		fmt.Println("nombre del archivo:", path_name)

		if l.Write.HasTags() {
			fmt.Println("escribiendo tangs")
			tag_path_name := path_name + ".tag"
			writeTags(tag_path_name, l.Write.LogTags)
		}
		file, _ := os.OpenFile(path_name, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
		to_file, _ := json.Marshal(l.Write)

		file.Write(to_file)
		file.WriteString("\n")
		file.Close()
		r := parser.Response{Conn: l.Conn}
		r.SendAckWriteLog()
		l.Conn.Close()

		*ack <- Ack{
			AppId: l.Write.AppId,
			Write: true,
		}
	}
	return nil
}
