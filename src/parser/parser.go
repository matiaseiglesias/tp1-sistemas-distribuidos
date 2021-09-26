package parser

import (
	"bufio"
	"fmt"
	"net"
)

type Parser struct {
	ID     string
	InConn chan net.Conn
}

func Run(p *Parser) {

	for c := range p.inConn {
		msg, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Printf("Could not initialize server")
		}
		fmt.Println("msg: ", msg)
	}
}
