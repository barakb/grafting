package grafting

import (
	"errors"
	"fmt"
	logger "github.com/Sirupsen/logrus"
	"net"
	"testing"
	"time"
)

func init() {
	logger.SetLevel(logger.InfoLevel)
}

func Test3ServersReplicateData(t *testing.T) {
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
	timeOut := time.After(30 * time.Second)
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
	//	defer logger.SetLevel(logger.InfoLevel)
	for i := 0; i < 10; i++ {
		if pred() {
			return
		}
		fmt.Printf("pred failed, sleeping for %v !\n", time.Duration(millis)*time.Millisecond/10)
		time.Sleep(time.Duration(millis) * time.Millisecond / 10)
	}
	if pred() {
		return
	}
	t.Error(msg)
}

func Test3TCPServersReplicateData(t *testing.T) {
	listeners := make([]net.Listener, 3, 3)
	var err error
	for i := range listeners {
		listeners[i], err = net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Errorf("failed to create a listener %v", err)
		}
	}
	others := make([][]string, 3, 3)
	for i := range listeners {
		others[i] = without(i, listeners)
	}

	server1 := NewServer(listeners[0].Addr().String()+":server0", others[0], NewMemoryLog())
	server2 := NewServer(listeners[1].Addr().String()+":server1", others[1], NewMemoryLog())
	server3 := NewServer(listeners[2].Addr().String()+":server2", others[2], NewMemoryLog())
	servers := []*server{server1, server2, server3}
	tcpConnector1 := NewTCPConnector(server1, others[0], listeners[0], 2, time.Millisecond*500)
	tcpConnector2 := NewTCPConnector(server2, others[1], listeners[1], 2, time.Millisecond*500)
	tcpConnector3 := NewTCPConnector(server3, others[2], listeners[2], 2, time.Millisecond*500)
	defer tcpConnector1.Close()
	defer tcpConnector2.Close()
	defer tcpConnector3.Close()
	for _, server := range servers {
		server.eventsChan = make(chan StateChangeEvent, 100)
		go server.Run()
		defer server.Close()
	}
	leader, err := leader(server1, server2, server3)
	if err != nil {
		t.Error(err.Error())
	}
	fmt.Printf("leader is %s\n", leader.id)

	//	t.Errorf("others are %#v", others)
}

func without(index int, listeners []net.Listener) []string {
	res := make([]string, len(listeners)-1, len(listeners)-1)
	writePos := 0
	for readPos, listener := range listeners {
		if index != readPos {
			res[writePos] = fmt.Sprintf("%s:server%d", listener.Addr().String(), readPos)
			writePos++
		}
	}
	return res
}
