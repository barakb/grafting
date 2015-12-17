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
	fmt.Printf("leader is %s\n", leader.id)
	if err != nil {
		t.Error(err.Error())
	}
	client := NewClient("client", []string{"server1", "server2", "server3"})
	defer client.Close()
	router.Register(client, 20)

	res := <-client.Execute(SetValue{"foo", "bar"})
	if res != nil {
		t.Errorf("result of first command should be nil, instead %q\n", res)
	}
	res = <-client.Execute(SetValue{"foo", "bar1"})
	if res != "bar" {
		t.Errorf("result of second command should be %q, instead %q\n", "bar", res)
	}

	waitFor(func() bool {
		for _, server := range servers {
			if server.stateMachine["foo"] != "bar1" {
				return false
			}
		}
		return true
	}, 500, fmt.Sprintf("all state machine should contains key named %q with value named %q, server1:%q, server2:%q, server3:%q\n", "foo", "bar1", server1.stateMachine, server2.stateMachine, server3.stateMachine), t)
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
