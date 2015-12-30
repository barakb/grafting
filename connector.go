package grafting

import (
	"bufio"
	"encoding/gob"
	"fmt"
	logger "github.com/Sirupsen/logrus"
	"io"
	"net"
	"sync"
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

type Transport interface {
	io.Reader
	io.Writer
	Closer
	SetWriteDeadline(t time.Time) error
}

type TransportFactory func(address string) (Transport, error)

func TCPTransportFactory(address string) (Transport, error) {
	return net.Dial("tcp", address)
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

type Pool struct {
	remoteAddress          string
	connections            chan Conn
	encoderDecoderBuilder  EncoderDecoderBuilder
	transportFactory       TransportFactory
	allowOpenNewConnection bool
	writeTimeout           time.Duration
	done                   chan struct{}
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
	if p.transportFactory != nil {
		c, err := p.transportFactory(p.remoteAddress)
		if err != nil {
			return nil, err
		}
		return newConnection(c, p.encoderDecoderBuilder, false, p.writeTimeout), nil
	}
	return nil, fmt.Errorf("pool %s can not create new connections", p.remoteAddress)
}

func createPoolFor(remoteAddress string, size int, encoderDecoderBuilder EncoderDecoderBuilder, transportFactory TransportFactory, allowOpenNewConnection bool, writeTimeout time.Duration) *Pool {
	return &Pool{remoteAddress, make(chan Conn, size), encoderDecoderBuilder, transportFactory, allowOpenNewConnection, writeTimeout, make(chan struct{})}
}

type Pools struct {
	sync.RWMutex
	pools                 map[string]*Pool
	maxSize               int
	writeTimeout          time.Duration
	encoderDecoderBuilder EncoderDecoderBuilder
}

func CreatePools(maxSize int, writeTimeout time.Duration, encoderDecoderBuilder EncoderDecoderBuilder) *Pools {
	return &Pools{pools: make(map[string]*Pool), maxSize: maxSize, writeTimeout: writeTimeout, encoderDecoderBuilder: encoderDecoderBuilder}
}
func (p *Pools) get(to string) (Conn, error) {
	p.RLock()
	pool, ok := p.pools[to]
	p.RUnlock()
	if !ok {
		pool = p.getOrCreatePool(to)
	}
	return pool.get()
}

func (p *Pools) getOrCreatePool(to string) *Pool {
	created := createPoolFor(to, p.maxSize, p.encoderDecoderBuilder, nil, false, p.writeTimeout)
	p.Lock()
	pool, ok := p.pools[to]
	if !ok {
		pool, p.pools[to] = created, created
	}
	p.Unlock()
	return pool
}

func (p *Pools) Close() error {
	p.Lock()
	for to, pool := range p.pools {
		pool.Close()
		delete(p.pools, to)
	}
	p.Unlock()
	return nil
}

func (p *Pools) putBack(connection Conn) {
	p.RLock()
	pool, ok := p.pools[connection.To()]
	p.RUnlock()
	if !ok {
		pool = p.getOrCreatePool(connection.To())
	}
	pool.putBack(connection)
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

type connection struct {
	transport    Transport
	encoder      Encoder
	decoder      Decoder
	closed       bool
	to           string
	initiateByMe bool
	writeTimeout time.Duration
	bufWriter    *bufio.Writer
}

func newConnection(transport Transport, encoderDecoderBuilder EncoderDecoderBuilder, initiateByMe bool, writeTimeout time.Duration) *connection {
	bufWriter := bufio.NewWriter(transport)
	return &connection{transport, encoderDecoderBuilder.Encoder(bufWriter), encoderDecoderBuilder.Decoder(transport), false, "", initiateByMe, writeTimeout, bufWriter}
}

func (c connection) Close() error {
	c.closed = true
	if c.initiateByMe {
		return c.transport.Close()
	}
	return nil
}

func (c connection) Encode(e interface{}) error {
	if err := c.encoder.Encode(e); err != nil {
		return err
	}
	if err := c.transport.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
		return err
	}
	if err := c.bufWriter.Flush(); err != nil {
		return err
	}
	return nil
}

func (c connection) Decode(e interface{}) error {
	return c.decoder.Decode(e)
}

func (c connection) To() string {
	return c.to
}
