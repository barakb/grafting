package grafting

import (
	log "github.com/Sirupsen/logrus"
	"github.com/nu7hatch/gouuid"
	"time"
)

type pendingRequest struct {
	uid             *uuid.UUID
	responseChannel chan<- interface{}
	command         *StateMachineCommand
	time            time.Time
}

type Client struct {
	address           string
	inbound           chan Message
	outbound          chan Message
	requestsToHandle  chan *pendingRequest
	servers           []string
	pendingRequests   map[string]*pendingRequest
	retryPendingTimer <-chan time.Time
	done              chan interface{}
}

func NewClient(address string, servers []string) *Client {
	res := &Client{address, make(chan Message), make(chan Message), make(chan *pendingRequest),
		servers, make(map[string]*pendingRequest), nil, make(chan interface{})}
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
	res := make(chan interface{}, 1)
	uuid, _ := uuid.NewV4()
	client.requestsToHandle <- &pendingRequest{uuid, res, &cmd, time.Now()}
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
				client.retryPendingTimer = time.After(5 * time.Second)
			}
		}
	}
}
func (client Client) retryPendingRequests() (allDone bool) {
	if len(client.pendingRequests) == 0 {
		return true
	}
	for _, pendingReq := range client.pendingRequests {
		client.broadcastRequest(pendingReq)
	}
	return false
}

func (client Client) handleRequest(req *pendingRequest) {
	client.pendingRequests[req.uid.String()] = req
	client.retryPendingTimer = time.After(5 * time.Second)
	client.broadcastRequest(req)
}

func (client Client) broadcastRequest(req *pendingRequest) {
	for _, server := range client.servers {
		go func(server string) {
			select {
			case client.outbound <- &StateMachineCommandRequest{Command: *req.command, Uid: req.uid, message: message{to: server, from: client.address}}:
				return
			case <-client.done:
				return
			}

		}(server)
	}
}

func (client Client) handleResponse(req Message) {
	switch m := req.(type) {
	case *StateMachineCommandResponse:
		pendingRequest, ok := client.pendingRequests[m.Uid.String()]
		if !ok {
			log.Warnf("%s ignoring expired StateMachineCommandResponse from %s: %#v\n", client.address, m.From(), req)
			return
		}
		pendingRequest.responseChannel <- m.ReturnValue
		delete(client.pendingRequests, m.Uid.String())
	default:
		log.Warnf("%s ignoring unexpected message from %s: %#v\n", client.address, m.From(), req)
	}
}
