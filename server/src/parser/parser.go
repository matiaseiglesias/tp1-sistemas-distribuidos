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

const MAX_BUFFER_SIZE = 1024
const UINT32_SIZE = 4
const UINT8_SIZE = 1

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

type Response struct {
	Conn net.Conn
}

func NewResponse(c net.Conn) *Response {
	return &Response{Conn: c}
}

func (r *Response) sendCode(c uint8) error {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, c)
	if err != nil {
		return err
	}
	n, err := r.Conn.Write(buf.Bytes())
	if err != nil || n != 0 {
		return err
	}
	return nil
}

func (r *Response) SendAckWriteLog() error {
	return r.sendCode(uint8(0))
}
func (r *Response) SendWrongFormat() error {
	return r.sendCode(uint8(1))
}
func (r *Response) SendBusyServer() error {
	return r.sendCode(uint8(2))
}
func (r *Response) SendUnkownAppId() error {
	return r.sendCode(uint8(3))
}
func (r *Response) SendAck() error {
	return r.sendCode(uint8(4))
}
func (r *Response) SendNewLog() error {
	return r.sendCode(uint8(5))
}
func (r *Response) SendNoMoreLogs() error {
	return r.sendCode(uint8(6))
}

func ReceiveBytes(c net.Conn, size uint32) ([]byte, error) {
	buff := make([]byte, 0)
	tmp_buff := make([]byte, MAX_BUFFER_SIZE)

	bytes_recieved := uint32(0)

	for bytes_recieved < size {
		n, err := c.Read(tmp_buff)
		if err != nil {
			return nil, err
		}
		buff = append(buff, tmp_buff[:n]...)
		bytes_recieved += uint32(n)
	}
	if bytes_recieved > size {
		return nil, errors.New("wrong packet size")
	}
	return buff, nil
}

func SendBytes(c net.Conn, b []byte) error {

	sentBytes := 0
	bytesToSend := len(b)

	for sentBytes < bytesToSend {
		n, err := c.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
		sentBytes += n
	}
	return nil
}

func ReceiveResponse(c net.Conn) (uint8, error) {
	ack, err := ReceiveBytes(c, UINT8_SIZE)
	if err != nil {
		return 0, err
	}
	return uint8(ack[0]), nil
}

func SendLogs(c net.Conn, logs *[]WriteQuery) {

	for _, log := range *logs {

		r := &Response{Conn: c}
		r.SendNewLog()

		_, err := ReceiveResponse(c)
		if err != nil {
			return
		}
		log_b, _ := json.Marshal(log)

		length := uint32(len(log_b))
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, length)
		SendBytes(c, buf.Bytes())
		_, err = ReceiveResponse(c)
		if err != nil {
			return
		}
		SendBytes(c, log_b)
		_, err = ReceiveResponse(c)
		if err != nil {
			return
		}
	}
	r := &Response{Conn: c}
	r.SendNoMoreLogs()
}

func ReceiveLogs(c net.Conn) []WriteQuery {

	logs := make([]WriteQuery, 0)
	r := &Response{Conn: c}

	cmd, err := ReceiveResponse(c)
	if err != nil {
		return nil
	}
	for cmd == 5 {
		r.SendAck()

		length_b, err := ReceiveBytes(c, UINT32_SIZE)
		if err != nil {
			return nil
		}
		log_length := binary.LittleEndian.Uint32(length_b)
		r.SendAck()
		log_b, err := ReceiveBytes(c, log_length)
		if err != nil {
			return nil
		}
		p := &WriteQuery{}
		json.Unmarshal(log_b, p)
		logs = append(logs, *p)
		err = r.SendAck()
		if err != nil {
			return nil
		}
		cmd, err = ReceiveResponse(c)
		if err != nil {
			return nil
		}
	}
	return logs
}

func receiveQuery(c net.Conn) ([]byte, error) {

	length_b, err := ReceiveBytes(c, UINT32_SIZE)
	if err != nil {
		fmt.Println("Se borro la conexion ", err)
		return nil, err
	}

	log_length := binary.LittleEndian.Uint32(length_b)

	r := &Response{Conn: c}
	err = r.SendAck()
	if err != nil {
		return nil, err
	}
	log_b, err := ReceiveBytes(c, log_length)

	if err != nil {
		return nil, nil
	}
	return log_b, nil
}

func SendQuery(c net.Conn, q Query) error {

	ack, _ := ReceiveResponse(c)

	if ack == 2 {
		return errors.New("servidor ocupado")
	}

	jlog, err := json.Marshal(q)

	if err != nil {
		return err
	}

	length := uint32(len(jlog))

	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, length)
	if err != nil {
		return err
	}

	err = SendBytes(c, buf.Bytes())
	if err != nil {
		return err
	}

	ReceiveResponse(c)

	err = SendBytes(c, jlog)
	if err != nil {
		return err
	}
	return nil
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
		r := &Response{Conn: c}
		err := r.SendAck()
		if err != nil {
			//se dropeo la conexion
			continue
		}

		log_b, err := receiveQuery(c)
		if err != nil {
			continue
		}
		res := &Query{}
		err = json.Unmarshal(log_b, res)

		if err != nil {
			fmt.Println("Error al deserializar: ", err)
			r := &Response{Conn: c}
			r.SendWrongFormat()
			c.Close()
			continue
		}
		res.Conn = c

		*p.OutLogs <- *res
	}
}
