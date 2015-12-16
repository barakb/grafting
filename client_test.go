package grafting

import (
	"github.com/nu7hatch/gouuid"
	"testing"
	"time"
)

// test that:
// 1. client save up to 5 pending request, after that client request is blocked.
// 2. client retries pending request in the right order.
func TestClientMaxPendingRequest(t *testing.T) {
	client := NewClient("client", []string{"server1"})
	client.retryTimeout = 200 // set retry sending request every 100 millis
	var doneCheckRetries5Values = make(chan struct{})
	results := make([]<-chan interface{}, 6)
	uidMaps := make(map[int]*uuid.UUID)
	// check the order of the retries
	go func() {
		var nextExpectedValue = 0
		for {
			select {
			case <-doneCheckRetries5Values:
				return
			case m := <-client.outbound:
				req := m.(*StateMachineCommandRequest)
				cmd := req.Command.(SetValue)
				if cmd.Value == 0 {
					uidMaps[0] = req.Uid
					nextExpectedValue = 1
					continue
				}
				if cmd.Value != nextExpectedValue {
					t.Errorf("message retry does not preserve order, expected set value to %d, instead %d", nextExpectedValue, cmd.Value)
				}
				uidMaps[nextExpectedValue] = req.Uid
				nextExpectedValue = (nextExpectedValue + 1) % 6

			}
		}
	}()
	// execute 6 request of which only 5 should return and the 6 should block.
	go func() {
		for i := 0; i < 6; i++ {
			results[i] = client.Execute(SetValue{"foo", i})
		}
	}()

	time.Sleep(250 * time.Millisecond)
	if results[5] != nil {
		t.Errorf("client should *not* have send last message before first one resolved")
	}
	close(doneCheckRetries5Values)

	// reply to the first request this should remove it from the pending request and the 6 request should return.
	client.inbound <- &StateMachineCommandResponse{message: message{to: "client", from: "server1"},
		Uid: uidMaps[0], ReturnValue: nil,
	}

	time.Sleep(250 * time.Millisecond)
	if results[5] == nil {
		t.Errorf("client *should* have send last after that last one was resolved")
	}

}
