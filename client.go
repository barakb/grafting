package grafting

import (
	"container/list"
	log "github.com/Sirupsen/logrus"
	"github.com/nu7hatch/gouuid"
	"sync"
	"time"
)

type pendingRequest struct {
	uid             *uuid.UUID
	responseChannel chan<- interface{}
	command         *StateMachineCommand
}

type Client struct {
	address             string
	inbound             chan Message
	outbound            chan Message
	requestsToHandle    chan *pendingRequest
	servers             []string
	pendingRequests     *list.List
	retryPendingTimer   <-chan time.Time
	retryTimeout        time.Duration
	pendingRequestQueue chan interface{}
	done                chan interface{}
}

func NewClient(address string, servers []string) *Client {
	res := &Client{address, make(chan Message), make(chan Message), make(chan *pendingRequest),
		servers, list.New(), nil, 5000, make(chan interface{}, 5), make(chan interface{})}
	go res.run()
	return res
}

func (client Client) Address() string {
	return client.address
}

func (client Client) OutboundChan() <-chan Message {
	return client.outbound
}

func (client Client) InboundChan() chan<- Message {
	return client.inbound
}

func (client Client) Close() error {
	close(client.done)
	return nil
}

func (client Client) Execute(cmd StateMachineCommand) <-chan interface{} {
	client.pendingRequestQueue <- true // should block if there is already 5 pending requests
	uuid, _ := uuid.NewV4()
	res := make(chan interface{}, 1)
	client.requestsToHandle <- &pendingRequest{uuid, res, &cmd}
	return res
}

func (client Client) run() {
	for {
		select {
		case <-client.done:
			return
		case req := <-client.requestsToHandle:
			client.handleRequest(req)
		case req := <-client.inbound:
			client.handleResponse(req)
		case <-client.retryPendingTimer:
			allDone := client.retryPendingRequests()
			if allDone {
				client.retryPendingTimer = nil
			} else {
				client.retryPendingTimer = time.After(client.retryTimeout * time.Millisecond)
			}
		}
	}
}
func (client Client) retryPendingRequests() (allDone bool) {
	if client.pendingRequests.Len() == 0 {
		return true
	}
	for e := client.pendingRequests.Front(); e != nil; e = e.Next() {
		client.broadcastRequest(e.Value.(*pendingRequest))
	}
	return false
}

func (client Client) handleRequest(req *pendingRequest) {
	if client.alreadySent(req) {
		return
	}
	client.pendingRequests.PushBack(req)
	client.retryPendingTimer = time.After(client.retryTimeout * time.Millisecond)
	client.broadcastRequest(req)
}

func (client Client) broadcastRequest(req *pendingRequest) {
	var wg sync.WaitGroup
	for _, server := range client.servers {
		wg.Add(1)
		go func(server string) {
			defer wg.Done()
			select {
			case client.outbound <- &StateMachineCommandRequest{Command: *req.command, Uid: req.uid, message: message{to: server, from: client.address}}:
				return
			case <-client.done:
				return
			}

		}(server)
	}
	wg.Wait()
}

func (client Client) handleResponse(req Message) {
	switch m := req.(type) {
	case *StateMachineCommandResponse:
		pendingRequest, ok := client.removePendingRequest(*m.Uid)
		if !ok {
			//todo how can this happen ?
			log.Warnf("%s ignoring expired StateMachineCommandResponse from %s: %#v\n", client.address, m.From(), req)
			return
		}
		pendingRequest.responseChannel <- m.ReturnValue
		<-client.pendingRequestQueue
	default:
		log.Warnf("%s ignoring unexpected message from %s: %#v\n", client.address, m.From(), req)
	}
}

func (client Client) alreadySent(req *pendingRequest) bool {
	for e := client.pendingRequests.Front(); e != nil; e = e.Next() {
		if e.Value.(*pendingRequest).uid == req.uid {
			return true
		}
	}
	return false
}

func (client Client) removePendingRequest(uid uuid.UUID) (*pendingRequest, bool) {
	for e := client.pendingRequests.Front(); e != nil; e = e.Next() {
		if *(e.Value.(*pendingRequest).uid) == uid {
			value := client.pendingRequests.Remove(e)
			return value.(*pendingRequest), true
		}
	}
	return nil, false
}
