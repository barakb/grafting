package grafting

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func Test3ServerReplicateData(t *testing.T) {
	server1 := NewServer("server1", []string{"server2", "server3"}, NewMemoryLog())
	server2 := NewServer("server2", []string{"server1", "server3"}, NewMemoryLog())
	server3 := NewServer("server3", []string{"server2", "server1"}, NewMemoryLog())
	servers := []*server{server1, server2, server3}
	router := NewRouter()
	defer router.Close()

	for _, server := range servers {
		router.Register(server, 20)
	}
	for _, server := range servers {
		server.eventsChan = make(chan StateChangeEvent, 100)
		go server.Run()
		defer server.Close()
	}

	leader, err := leader(server1, server2, server3)
	if err != nil {
		t.Error(err.Error())
	}
	req := &StateMachineCommandRequest{
		message: message{leader.id, "client"},
		Command: SetValue{"foo", "bar"},
	}
	leader.inboundChan <- req
	waitFor(func() bool {
		for _, server := range servers {
			if server.stateMachine["foo"] != "bar" {
				return false
			}
		}
		return true
	}, 250, fmt.Sprintf("leader state machine should contains key named %q\n", leader.stateMachine), t)
}

func leader(s1 *server, s2 *server, s3 *server) (*server, error) {
	timeOut := time.After(10 * time.Second)
	for {
		select {
		case e := <-s1.eventsChan:
			if e.To == LEADER {
				return s1, nil
			}
		case e := <-s2.eventsChan:
			if e.To == LEADER {
				return s2, nil
			}
		case e := <-s3.eventsChan:
			if e.To == LEADER {
				return s3, nil
			}
		case <-timeOut:
			return nil, errors.New("No leader")
		}
	}
}

type predicate func() bool

func waitFor(pred predicate, millis int, msg string, t *testing.T) {
	for i := 0; i < 10; i++ {
		if pred() {
			return
		}
		time.Sleep(time.Duration(millis) * time.Millisecond / 10)
	}
	if pred() {
		return
	}
	t.Error(msg)
}
