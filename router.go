package grafting

import (
	logger "github.com/Sirupsen/logrus"
	"time"
)

type Addressable interface {
	Address() string
	OutboundChan() <-chan Message
	InboundChan() chan<- Message
}

type Router struct {
	targets      map[string]Addressable
	inboundQueue map[string]chan<- Message
	done         chan struct{}
}

// In case 0 < inboundChanSize the incoming trafic for this target an intermediat channel of size inboundChanSize
// will be created by the router and all trafic to this target will to this new channel before it sent to the target inbound buffer.
// The sending from this new buffer to the target inbound buffer will be tried for 1 second after that the message will be discarded.
func (router Router) Register(target Addressable, inboundChanSize int) {
	router.targets[target.Address()] = target
	go router.serveOutbound(target)
	if 0 < inboundChanSize {
		inboundChan := make(chan Message, inboundChanSize)
		go router.connect(inboundChan, target.InboundChan())
		router.inboundQueue[target.Address()] = inboundChan
	} else {
		router.inboundQueue[target.Address()] = target.InboundChan()
	}
}

func (router Router) Close() error {
	close(router.done)
	return nil
}

func NewRouter() Router {
	return Router{make(map[string]Addressable), make(map[string]chan<- Message), make(chan struct{})}
}

func (router Router) serveOutbound(target Addressable) {
	for {
		select {
		case <-router.done:
			return
		case message := <-target.OutboundChan():
			router.dispatch(message)
		}
	}
}
func (router Router) connect(sourceChan <-chan Message, targetChan chan<- Message) {
	for {
		select {
		case message := <-sourceChan:
			select {
			case <-router.done:
				return
			case targetChan <- message:
			case <-time.After(time.Millisecond * 60):
				router.drain(sourceChan)
			}
		case <-router.done:
			return
		}
	}
}

func (router Router) drain(sourceChan <-chan Message) {
	for i := 0; i < 100; i++ {
		select {
		case <-router.done:
			return
		case <-sourceChan:
		default:
			logger.Infof("%d drained ------------------------------------------------------------------------------------------- \n", i)
			return
		}
	}
}

func (router Router) dispatch(message Message) {
	if inboundChan, ok := router.inboundQueue[message.To()]; ok {
		select {
		case <-router.done:
		case inboundChan <- message:
		}
	}
}
