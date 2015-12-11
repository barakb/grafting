package go_rafting

import (
	"fmt"
	"time"
)

type RouteTarget interface {
	Address() string
	OutboundChan() chan Message
	InboundChan() chan Message
}

type Router struct {
	targets      map[string]RouteTarget
	inboundQueue map[string]chan Message
	done         chan interface{}
}

// In case 0 < inboundChanSize the incoming trafic for this target an intermediat channel of size inboundChanSize
// will be created by the router and all trafic to this target will to this new channel before it sent to the target inbound buffer.
// The sending from this new buffer to the target inbound buffer will be tried for 1 second after that the message will be discarded.
func (router Router) Register(target RouteTarget, inboundChanSize int) {
	router.targets[target.Address()] = target
	go router.serveOutbound(target)
	if 0 < inboundChanSize {
		inboundChan := make(chan Message)
		go router.connect(target.Address(), inboundChan, target.InboundChan())
		router.inboundQueue[target.Address()] = inboundChan
	} else {
		router.inboundQueue[target.Address()] = target.InboundChan()
	}
}

func (router Router) ShutDown() {
	close(router.done)
}

func NewRouter() Router {
	return Router{make(map[string]RouteTarget), make(map[string]chan Message), make(chan interface{})}
}

func (router Router) serveOutbound(target RouteTarget) {
	for {
		select {
		case <-router.done:
			return;
		case message := <-target.OutboundChan():
			router.dispatch(message)
			return
		}
	}
}
func (router Router) connect(target string, sourceChan <-chan Message, targetChan chan <- Message) {
	for {
		select {
		case message := <-sourceChan:
		// put it target output channel with 1 second timeout.
				select {
				case targetChan <- message:
					continue
				case <-time.After(time.Second * 1):
					fmt.Printf("failed to send message %v to target %s", message, target)
				case <-router.done:
					return
				}
		case <-router.done:
			return
		}
	}
}

func (router Router) dispatch(message Message) {
	if inboundChan, ok := router.inboundQueue[message.To()]; ok {
		select {
		case <-router.done:
			return;
		case inboundChan <- message:
			return
		}
	}
}
