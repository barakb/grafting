package grafting

import (
	"encoding/gob"
	"fmt"
	"net"
	"testing"
	"time"
)

type addressable struct {
	address string
	out     chan Message
	in      chan Message
}

func (ts addressable) Address() string {
	return ts.address
}
func (ts addressable) OutboundChan() <-chan Message {
	return ts.out
}
func (ts addressable) InboundChan() chan<- Message {
	return ts.in
}

func TestTCPConnector(t *testing.T) {
	gob.Register(RequestVoteResponse{})

	addressable := addressable{"localhost:0", make(chan Message), make(chan Message)}
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Errorf("Failed to create test listener %v", err)
	}
	tcpConnector := NewTCPConnector(addressable, []string{listener.Addr().String()}, listener, 2)
	defer tcpConnector.Close()
	message := RequestVoteResponse{message: message{listener.Addr().String(), listener.Addr().String()},
		Term:    1,
		Granted: true,
	}

	select {
	case addressable.out <- message:
	case <-time.After(500 * time.Millisecond):
		t.Errorf("Failed to send %#v", message)
	}

	select {
	case msg := <-addressable.in:
		fmt.Printf("Got message %#v\n", msg)
	case <-time.After(500 * time.Millisecond):
		t.Errorf("Failed to reveive message %#v", message)
	}
}

