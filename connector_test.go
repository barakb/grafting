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

func TestTCPConnectorSendToServer(t *testing.T) {

	addressable := addressable{"localhost:0", make(chan Message), make(chan Message)}
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Errorf("Failed to create test listener %v", err)
	}
	tcpConnector := NewTCPConnector(addressable, []string{listener.Addr().String()}, listener, 2)
	defer tcpConnector.Close()
	message := RequestVoteResponse{Msg: Msg{listener.Addr().String(), listener.Addr().String()},
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

func TestTCPConnectorSendToClient(t *testing.T) {

	addressable := addressable{"localhost:0", make(chan Message), make(chan Message)}
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Errorf("Failed to create test listener %v", err)
	}
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Errorf("Failed to open connection to %s, %v", listener.Addr().String(), err)
	}
	defer client.Close()

	tcpConnector := NewTCPConnector(addressable, []string{listener.Addr().String()}, listener, 2)
	defer tcpConnector.Close()
	// foo is a logical name.
	// once the client send message from "foo" that is not one of the server list,
	// the connector will send any message to foo using this connections.
	//	fromFoo := RequestVoteResponse{Msg: Msg{F: "foo", T: listener.Addr().String()},
	//		Term:    1,
	//		Granted: true,
	//	}
	fromFoo := RequestVoteResponse{Msg: Msg{F: "foo", T: listener.Addr().String()},
		Term:    1,
		Granted: true,
	}

	// first send the toFoo message from the client to open connection from server to foo.
	enc := gob.NewEncoder(client)
	interfaceEncode(enc, fromFoo)
	select {
	case <-addressable.in:
	case <-time.After(500 * time.Millisecond):
		t.Errorf("Failed to send %#v", fromFoo)
	}

	// now send request from the server to foo
	toFoo := RequestVoteResponse{Msg: Msg{F: listener.Addr().String(), T: "foo"},
		Term:    1,
		Granted: true,
	}
	select {
	case addressable.out <- toFoo:
	case <-time.After(500 * time.Millisecond):
		t.Errorf("Failed to send %#v", toFoo)
	}

	dec := gob.NewDecoder(client)

	// message should be sent to client via the socket that the client opened.
	client.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	msg, err := interfaceDecode(dec)
	if err != nil {
		t.Errorf("Server failed to send message to client by logical name %#v", fromFoo)
	} else if msg.To() != "foo" {
		t.Errorf("Unexpected message %#v", msg)
	}
}

func interfaceDecode(dec *gob.Decoder) (Message, error) {
	// The decode will fail unless the concrete type on the wire has been
	// registered. We registered it in the calling function.
	var m Message
	err := dec.Decode(&m)
	return m, err
}

func interfaceEncode(enc *gob.Encoder, m Message) error {
	return enc.Encode(&m)
}
