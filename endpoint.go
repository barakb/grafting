package grafting

import (
	"container/list"
	"sync"
)

//A class that serve as endpoint to/from other server/client

type Endpoint struct {
	isStreamOwner       bool
	messageReaderWriter MessageReaderWriterCloser
	sentQueue           BlockingQueue
	incoming            chan <- Message
	done                chan struct{}
	container           *Endpoints
}

func (ep *Endpoint) Close() error {
	close(ep.done)
	if ep.isStreamOwner {
		ep.messageReaderWriter.Close()
	}
	return nil
}

func (ep *Endpoint) Remove() {
	if container := ep.container; container != nil {
		container.remove(ep)
	}
}

func (ep *Endpoint) onError(err error) {
	ep.Remove()
	ep.Close()
}

func NewEndpoint(isStreamOwner bool, messageReaderWriter MessageReaderWriterCloser, sentQueue BlockingQueue, incoming chan Message) *Endpoint {
	res := &Endpoint{isStreamOwner, messageReaderWriter, sentQueue, incoming, make(chan struct{}), nil}
	if res.isStreamOwner {
		go func() {
			for {
				msg, err := messageReaderWriter.Read()
				if err != nil {
					res.onError(err)
					return
				}
				incoming <- msg
			}
		}()
	}
	go func() {
		for {
			msg, err := sentQueue.Dequeue(res.done)
			if err != nil {
				res.onError(err)
				return;
			}
			res.messageReaderWriter.Write(msg)
			if err != nil {
				res.onError(err)
				return;
			}
		}
	}()
	return res
}

//Collection of Endpoint to the same target
type Endpoints struct {
	sync.RWMutex
	target    string
	incoming  chan <- Message
	done      chan struct{}
	endpoints *list.List
	sentQueue BlockingQueue
}

func NewEndPoints(target string, incoming chan <- Message) *Endpoints {
	res := &Endpoints{sync.RWMutex{}, target, incoming, make(chan struct{}), list.New(), NewBlockingQueue()}
	return res
}

func (ep *Endpoints) Close() error {
	ep.sentQueue.Close()
	ep.Lock()
	defer ep.Unlock()
	close(ep.done)
	var next *list.Element
	for e := ep.endpoints.Front(); e != nil; e = next {
		e.Value.(*Endpoint).Close()
		next = e.Next()
		ep.endpoints.Remove(e)
	}
	return nil
}

func (eps *Endpoints) remove(ep *Endpoint) {
	eps.Lock()
	defer eps.Unlock()
	for e := eps.endpoints.Front(); e != nil; e = e.Next() {
		if e.Value == ep {
			eps.endpoints.Remove(e)
			return
		}
	}

}

func (eps *Endpoints) Write(m Message) {
	eps.sentQueue.Enqueue(m)
}

