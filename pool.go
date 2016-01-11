package grafting

import (
	"fmt"
	logger "github.com/Sirupsen/logrus"
	"sync"
	"time"
)

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
		//		logger.Infof("pool to %s return existing connection: %v", p.remoteAddress, res)
		return res, nil
	case <-time.After(20 * time.Millisecond):
		//	    logger.Infof("pool to %s open new connection", p.remoteAddress)
		return p.openNewConnection()
	}
}

func (p Pool) putBack(connection Conn) {
	select {
	case <-p.done:
		connection.Close()
	case p.connections <- connection:
		//		logger.Infof("pool to %s (putBack) connection: %v", p.remoteAddress, connection)
	case <-time.After(20 * time.Millisecond):
		//		logger.Infof("pool to %s (putBack) closing connection: %v", p.remoteAddress, connection)
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
		return newConnection(c, p.remoteAddress, p.encoderDecoderBuilder, false, p.writeTimeout), nil
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
	if connection.To() == "" {
		panic(fmt.Sprintf("failed to put in pool connection to empty target %#v", connection.To()))
	}
	p.RLock()
	pool, ok := p.pools[connection.To()]
	p.RUnlock()
	if !ok {
		logger.Debugf("pools: creating pool [%s] for connection %#v", connection.To(), connection)
		pool = p.getOrCreatePool(connection.To())
	}
	pool.putBack(connection)
}
