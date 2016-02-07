package grafting

import (
	"fmt"
	logger "github.com/Sirupsen/logrus"
	"net"
)

// A class that serve as endpoint to/from other server/client
// Each Endpoint read message from a common (to all endpoint to the same destination) blocking queue and send them to the destination.
// If Endpoint incoming is not nil, it will ready messages using its messageReaderWriter and put the messages in the incoming queue as well.
type Endpoint struct {
	messageReaderWriter MessageReaderWriterCloser
	sentQueue           BlockingQueue
	incoming            chan<- Message
	done                chan struct{}
	isDead              bool
	deadEndpoints       chan<- *Endpoint
}

func (ep *Endpoint) Close() error {
	close(ep.done)
	ep.isDead = true
	ep.deadEndpoints <- ep
	return nil
}

func (ep *Endpoint) onError(err error) {
	ep.Close()
}

func NewEndpoint(messageReaderWriter MessageReaderWriterCloser, sentQueue BlockingQueue, incoming chan<- Message, deadEndpoints chan<- *Endpoint) *Endpoint {
	res := &Endpoint{messageReaderWriter, sentQueue, incoming, make(chan struct{}), false, deadEndpoints}
	return res
}
func (ep *Endpoint) start() *Endpoint {
	if ep.incoming != nil {
		go ep.reader()
	}
	go ep.writer()
	return ep
}

func (ep *Endpoint) reader() {
	for {
		msg, err := ep.messageReaderWriter.Read()
		if err != nil {
			ep.onError(err)
			return
		}
		ep.incoming <- msg
	}
}

func (ep *Endpoint) writer() {
	for {
		msg, err := ep.sentQueue.Dequeue(ep.done)
		if err != nil {
			if ep.incoming != nil {
				ep.messageReaderWriter.Close()
			}
			ep.onError(err)
			return
		}
		if msg, ok := msg.(Message); ok {
			err = ep.messageReaderWriter.Write(msg)
			if err != nil {
				ep.onError(err)
				return
			}
		} else {
			ep.onError(fmt.Errorf("wrong messageType %#v", msg))
		}
	}
}

// Manage a set of endpoint to the same destination.
// If incoming channel is not nil it will asume it can create a new endpoints and fill
// the live endpoints when it is not full. (connection to server workflow)
//
// It will remove closed endpoints from live endpoints.
// I will allow to add  (push) endpoints (maybe instead of existing endpoint) (this is when remote server open connection)
// It prefers endpoints with incoming channel == nil (connections opened by peer)
//
type EndpointsManager struct {
	target          string
	incoming        chan<- Message
	done            chan struct{}
	liveEndpoints   chan *Endpoint
	sentQueue       BlockingQueue
	deadEndpoints   chan *Endpoint
	serverEndpoints chan *Endpoint
}

func NewEndpointsManager(target string, incoming chan<- Message) *EndpointsManager {
	epm := &EndpointsManager{target, incoming, make(chan struct{}), make(chan *Endpoint, 2), NewBlockingQueue(), make(chan *Endpoint), make(chan *Endpoint)}
	return epm.start()
}

func (epm *EndpointsManager) start() *EndpointsManager {
	if epm.incoming != nil {
		go epm.fillerCleaner()
	} else {
		go epm.cleaner()
	}

	return epm
}

func (epm *EndpointsManager) fillerCleaner() {
	ep := NewEndpoint(nil, epm.sentQueue, epm.incoming, epm.deadEndpoints)
	for {
		select {
		case <-epm.done:
			epm.closeAll()
			return
		case <-epm.deadEndpoints:
			epm.clean()
		case sep := <-epm.serverEndpoints:
			epm.push(sep)
		case epm.liveEndpoints <- ep:
			epm.connect(ep)
		}
	}
}

func (epm *EndpointsManager) connect(ep *Endpoint) {
	conn, err := net.Dial("tcp", epm.target)
	if err != nil {
		logger.Errorf("Failed to open connection to %s, error is %v", epm.target, err)
		ep.isDead = true
		epm.deadEndpoints <- ep
	} else {
		NewReaderWriter(conn)
		ep.start()
		ep = NewEndpoint(nil, epm.sentQueue, epm.incoming, epm.deadEndpoints)
	}
}

func (epm *EndpointsManager) cleaner() {
	for {
		select {
		case <-epm.done:
			epm.closeAll()
			return
		case <-epm.deadEndpoints:
			epm.clean()
		case sep := <-epm.serverEndpoints:
			epm.push(sep)
		}

	}
}

func (epm *EndpointsManager) push(sep *Endpoint) {
	sep.deadEndpoints = epm.deadEndpoints
	sep.sentQueue = epm.sentQueue
	select {
	case epm.liveEndpoints <- sep:
		sep.start()
	default:
		l := len(epm.liveEndpoints)
		for i := 0; i < l; i++ {
			lep := <-epm.liveEndpoints
			if lep.sentQueue != nil {
				go lep.Close()
				epm.liveEndpoints <- sep
				sep.start()
				return
			}
		}
	}
}

func (epm *EndpointsManager) closeAll() {
	for {
		select {
		case liveEndpint := <-epm.liveEndpoints:
			go liveEndpint.Close()
		default:
			return
		}
	}
}
func (epm *EndpointsManager) clean() {
	l := len(epm.liveEndpoints)
	for i := 0; i < l; i++ {
		ep := <-epm.liveEndpoints
		if !ep.isDead {
			epm.liveEndpoints <- ep
		}
	}
}

// write message to the blocking queue, one of the endpoints will take it and send it.
func (eps *EndpointsManager) Write(m Message) {
	eps.sentQueue.Enqueue(m)
}
