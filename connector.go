package grafting

import (
	"encoding/gob"
	"fmt"
	logger "github.com/Sirupsen/logrus"
	"io"
	"net"
	"time"
)

type Encoder interface {
	Encode(e interface{}) error
}

type Decoder interface {
	Decode(e interface{}) error
}

type Closer interface {
	Close() error
}

type EncoderDecoderBuilder interface {
	Encoder(w io.Writer) Encoder
	Decoder(r io.Reader) Decoder
}

type Connector interface {
	Closer
	Send(m Message) error
}

type Conn interface {
	Encoder
	Decoder
	Closer
	To() string
}

type connector struct {
	addressable           Addressable
	remoteAddresses       []string
	inboundChannel        chan<- Message
	outboundChannel       <-chan Message
	pools                 *Pools
	encoderDecoderBuilder EncoderDecoderBuilder
	transportFactory      TransportFactory
	listener              net.Listener
	writeTimeout          time.Duration
	done                  chan struct{}
}

func (c connector) Close() error {
	close(c.done)
	c.pools.Close()
	return c.listener.Close()
}

func (c connector) Send(m Message) error {
	if con, err := c.pools.get(m.To()); err == nil {
		err = con.Encode(&m)
		if err == nil {
			c.pools.putBack(con)
		} else {
			con.Close()
		}
		return err
	} else {
		return err
	}
}

func (c connector) forwardSend() {
	for {
		select {
		case <-c.done:
			return
		case msg := <-c.addressable.OutboundChan():
			err := c.Send(msg)
			if err != nil {
				logger.Warn("Fail to send message %#v, error is %#v", msg, err)
			}
		}
	}
}

func (c connector) listen() {
	defer c.listener.Close()
	for {
		conn, err := c.listener.Accept()
		if err != nil {
			select {
			case <-c.done:
				return
			default:
				fmt.Println("Error accepting: ", err.Error())
			}
			continue
		}
		go c.handleRequest(conn)
	}
}
func (c connector) handleRequest(conn net.Conn) {
	connection := newConnection(conn, c.encoderDecoderBuilder, false, c.writeTimeout)
	defer func() {
		connection.closed = true
		connection.transport.Close()
	}()

	for {
		var m Message
		err := connection.decoder.Decode(&m)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error decoding: ", err.Error())
			}
			return
		}
		if connection.to == "" && m.From() != "" {
			connection.to = m.From()
			c.pools.putBack(connection)
		}
		// forward the message
		select {
		case <-c.done:
			return
		case c.addressable.InboundChan() <- m:
		}
	}
}

func NewTCPConnector(addressable Addressable, remoteAddresses []string, listener net.Listener, poolSize int, writeTimeout time.Duration) *connector {
	return NewConnector(addressable, remoteAddresses, TCPTransportFactory, listener, poolSize, writeTimeout)
}
func NewConnector(addressable Addressable, remoteAddresses []string, transportFactory TransportFactory, listener net.Listener, poolSize int, writeTimeout time.Duration) *connector {

	connector := connector{addressable: addressable, remoteAddresses: remoteAddresses, transportFactory: transportFactory, listener: listener, writeTimeout: writeTimeout}
	connector.done = make(chan struct{})
	connector.encoderDecoderBuilder = GobEncoderDecoderBuilder{}
	connector.pools = CreatePools(poolSize, writeTimeout, connector.encoderDecoderBuilder)
	for _, remoteAddress := range remoteAddresses {
		connector.pools.pools[remoteAddress] = createPoolFor(remoteAddress, poolSize, connector.encoderDecoderBuilder, transportFactory, true, writeTimeout)
	}
	for _ = range remoteAddresses {
		go connector.forwardSend()
	}
	go connector.listen()
	return &connector
}

type GobEncoderDecoderBuilder struct {
}

func (b GobEncoderDecoderBuilder) Encoder(w io.Writer) Encoder {
	return gob.NewEncoder(w)
}

func (b GobEncoderDecoderBuilder) Decoder(r io.Reader) Decoder {
	return gob.NewDecoder(r)
}
