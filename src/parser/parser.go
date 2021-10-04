package parser

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"time"
)

const UINT32_SIZE = 4
const MAX_BUFFER_SIZE = 1024

type Query struct {
	Read  ReadQuery
	Write WriteQuery
	Conn  net.Conn
}

type WriteQuery struct {
	AppId      string
	Initialize bool
	Message    string
	LogTags    []string
	Timestamp  time.Time
}

type ReadQuery struct {
	AppId      string
	Initialize bool
	From       time.Time
	To         time.Time
	Tag        string
	Pattern    string
}

type Parser struct {
	Id      string
	InConn  *chan net.Conn
	OutLogs *chan Query
}

type Filter struct {
	Tag     string
	Pattern string
}

func ToPathName(id string, t time.Time) string {
	year, month, day := t.Date()
	hour, _, _ := t.Truncate(time.Hour).Clock()

	return "./" + id + "/" + strconv.Itoa(hour) + ":00-" + strconv.Itoa(day) + month.String() + strconv.Itoa(year)

}

func receive_bytes(c net.Conn, size uint32) ([]byte, error) {
	buff := make([]byte, 0)
	tmp_buff := make([]byte, MAX_BUFFER_SIZE)

	bytes_recieved := uint32(0)

	for bytes_recieved < size {
		n, err := c.Read(tmp_buff)
		if err != nil {
			fmt.Println("Error al recibir un paquete", err)
			return nil, err
		}
		buff = append(buff, tmp_buff[:n]...)
		bytes_recieved += uint32(n)
	}
	if bytes_recieved > size {
		fmt.Println("Esperaba", size, "bytes y recibi: ", bytes_recieved)
		return nil, errors.New("wrong packet size")
	}
	return buff, nil
}

func receive(c net.Conn) []byte {

	length_b, err := receive_bytes(c, UINT32_SIZE)
	if err != nil {
		fmt.Println("Error al recibir un int ", err)
	}

	log_length := binary.LittleEndian.Uint32(length_b)

	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, uint32(1))
	if err != nil {
		fmt.Println("ERRor", err)
	}
	c.Write(buf.Bytes())

	fmt.Println("Bytes a recibir: ", log_length)

	log_b, err := receive_bytes(c, log_length)

	if err != nil {
		fmt.Println("Error al recibir el log ", err)
	}
	//fmt.Println("Mensaje recibido ", log_b)
	return log_b
}

func (q *Query) IsReadQuery() bool {
	return q.Read.Initialize
}

func (q *Query) IsWriteQuery() bool {
	return q.Write.Initialize
}

func (q *WriteQuery) HasTags() bool {
	return len(q.LogTags) > 0
}

func (q *WriteQuery) PassFilter(f Filter) bool {

	if !f.HasTag() && !f.HasPattern() {
		return true
	}
	ok := false
	if f.HasTag() {
		for _, a := range q.LogTags {
			if a == f.Tag {
				ok = true
				break
			}
		}
	}
	if f.HasPattern() {
		ok, _ = regexp.MatchString(f.Pattern, q.Message)
	}
	return ok
}

func (f *Filter) HasTag() bool {
	return len(f.Tag) > 0
}

func (f *Filter) HasPattern() bool {
	return len(f.Pattern) > 0
}

func (q *ReadQuery) HasTag() bool {
	return len(q.Tag) > 0
}

func (q *ReadQuery) HasPattern() bool {
	return len(q.Pattern) > 0
}
func (q *ReadQuery) HasTimeFilter() bool {
	return !q.From.IsZero() && !q.To.IsZero()
}

func (q *ReadQuery) GetFilter() Filter {
	return Filter{
		Tag:     q.Tag,
		Pattern: q.Pattern,
	}
}

func (p *Parser) Run() {

	for c := range *p.InConn {

		log_b := receive(c)
		res := &Query{}
		err := json.Unmarshal(log_b, res)

		if err != nil {
			fmt.Println("Error al serializar: ", err)
		}
		//fmt.Println(res)

		res.Conn = c

		*p.OutLogs <- *res
		//c.Close()
	}
}
