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
}

type Transport interface {
	io.Reader
	io.Writer
	Closer
}

type TransportFactory func(address string) (Transport, error)

func TCPTransportFactory(address string) (Transport, error) {
	return net.Dial("tcp", address)
}

type connector struct {
	addressable           Addressable
	remoteAddresses       []string
	inboundChannel        chan <- Message
	outboundChannel       <-chan Message
	pools                 map[string]Pool
	encoderDecoderBuilder EncoderDecoderBuilder
	transportFactory      TransportFactory
	listener              net.Listener
	done                  chan struct{}
}

func (c connector) Close() error {
	close(c.done)
	for _, pool := range c.pools {
		pool.Close()
	}
	return c.listener.Close()
}

func (c connector) Send(m Message) error {
	if pool, ok := c.pools[m.To()]; ok {
		if con, err := pool.get(); err == nil {
			defer pool.putBack(con)
			return con.Encode(&m)
		} else {
			return err
		}
	}
	return fmt.Errorf("address not found %#v", m)
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
	defer conn.Close()
	decoder := c.encoderDecoderBuilder.Decoder(conn)
	for {
		var m Message
		err := decoder.Decode(&m)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error decoding: ", err.Error())
			}
			return
		}
		select {
		case <-c.done:
			return
		case c.addressable.InboundChan() <- m:
		}
	}

}


type Pool struct {
	remoteAddress         string
	connections           chan Conn
	encoderDecoderBuilder EncoderDecoderBuilder
	transportFactory      TransportFactory
	done                  chan struct{}
}

func (p Pool) get() (Conn, error) {
	select {
	case <-p.done:
		return nil, fmt.Errorf("pool %s is closed", p.remoteAddress)
	case res := <-p.connections:
		return res, nil
	case <-time.After(20 * time.Millisecond):
		return p.openNewConnection()
	}
}

func (p Pool) putBack(connection Conn) {
	select {
	case <-p.done:
		connection.Close()
	case p.connections <- connection:
	case <-time.After(20 * time.Millisecond):
		connection.Close()
	}
}

func (p Pool) Close() error {
	close(p.done)
	close(p.connections)
	for {
		select {
		case res, ok := <-p.connections:
			if !ok {
				return nil
			}
			res.Close()
		}
	}
}

func (p Pool) openNewConnection() (Conn, error) {
	c, err := p.transportFactory(p.remoteAddress)
	if err != nil {
		return nil, err
	}
	return &connection{c, p.encoderDecoderBuilder.Encoder(c), p.encoderDecoderBuilder.Decoder(c)}, nil
}

func createPoolFor(remoteAddress string, size int, encoderDecoderBuilder EncoderDecoderBuilder, transportFactory TransportFactory) Pool {
	return Pool{remoteAddress, make(chan Conn, size), encoderDecoderBuilder, transportFactory, make(chan struct{})}
}

func NewTCPConnector(addressable Addressable, remoteAddresses []string, listener net.Listener, poolSize int) *connector {
	return NewConnector(addressable, remoteAddresses, TCPTransportFactory, listener, poolSize)
}
func NewConnector(addressable Addressable, remoteAddresses []string, transportFactory TransportFactory, listener net.Listener, poolSize int) *connector {

	connector := connector{addressable: addressable, remoteAddresses: remoteAddresses, transportFactory: transportFactory, listener: listener}
	connector.done = make(chan struct{})
	connector.encoderDecoderBuilder = GobEncoderDecoderBuilder{}
	connector.pools = make(map[string]Pool, len(remoteAddresses))
	for _, remoteAddress := range remoteAddresses {
		connector.pools[remoteAddress] = createPoolFor(remoteAddress, poolSize, connector.encoderDecoderBuilder, transportFactory)
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

type connection struct {
	transport Transport
	encoder   Encoder
	decoder   Decoder
}

func (c connection) Close() error {
	return c.transport.Close()
}
func (c connection) Encode(e interface{}) error {
	return c.encoder.Encode(e)
}
func (c connection) Decode(e interface{}) error {
	return c.decoder.Decode(e)
}
