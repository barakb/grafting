package go_rafting

import (
	"testing"
	"time"
)

type testTarget struct {
	address      string
	inboundChan  chan Message
	outboundChan chan Message
}

func (target *testTarget) Address() string {
	return target.address
}
func (target *testTarget) OutboundChan() chan Message {
	return target.outboundChan
}
func (target *testTarget) InboundChan() chan Message {
	return target.inboundChan
}

func newTestTarget(name string) *testTarget {
	return &testTarget{name, make(chan Message, 1), make(chan Message)}
}

func TestSelfRoute(t *testing.T) {
	router := NewRouter()
	testTarget := newTestTarget("target1")
	router.Register(testTarget, 1)
	messageSent := message{"from", "target1"}
	testTarget.outboundChan <- messageSent
	select {
	case messageReceived := <-testTarget.inboundChan:
		if messageSent != messageReceived {
			t.Logf("Expected same message, instead, sent %v received %v\n", messageSent, messageReceived)
		}
	case <-time.After(time.Second * 1):
		t.Logf("Message from outbound was not put on inbound")
	}
	router.Close()
}
