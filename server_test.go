package go_rafting

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCreateServer(t *testing.T) {
	server := NewServer("server1", []string{"server2", "server3"}, NewMemoryLog())
	if server.state != CANDIDATE {
		t.Errorf("newly created server should be in state %v instead %v\n", CANDIDATE, server.state)
	}
}

func TestCandidateSetup(t *testing.T) {
	server := NewServer("server1", []string{"server2", "server3"}, NewMemoryLog())
	go func() {
		i := 0
		for i < 2 {
			select {
			case <-server.outboundChan:
				i++
			}
		}
		server.Stop()
	}()
	server.candidateLoop()
}

func TestCandidateToLeader(t *testing.T) {
	server := NewServer("server1", []string{"server2", "server3"}, NewMemoryLog())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		i := 0
		for i < 2 {
			select {
			case m := <-server.outboundChan:
				go func() {
					server.inboundChan <- RequestVoteResponse{message: message{m.To(), m.From()},
						Term:    1,
						Granted: true,
					}
					wg.Done()
				}()
				i++
			}
		}
		wg.Wait()
		server.Stop()
	}()
	server.candidateLoop()
	wg.Wait()
	if !waitForState(server, LEADER) {
		t.Errorf("server should have bean leader (%d) instead %v\n", LEADER, server.state)
	}
}

func TestCandidateNobodyElected(t *testing.T) {
	server := NewServer("server1", []string{"server2", "server3"}, NewMemoryLog())
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		i := 0
		for i < 4 {
			select {
			case m := <-server.outboundChan:
				if i == 3 && server.state != CANDIDATE {
					t.Errorf("server should have bean candidate (%d) as long as 1 vote was not granted instead %v\n", CANDIDATE, server.state)
				}
				term := m.(*RequestVote).Term
				go func() {
					reply(server, RequestVoteResponse{message: message{m.To(), m.From()},
						Term:    term,
						Granted: term == 2,
					})
					wg.Done()
				}()
				i++
			}
		}
		wg.Wait()
		server.Stop()
	}()
	server.candidateLoop()
	wg.Wait()
	if !waitForState(server, LEADER) {
		t.Errorf("server should have bean leader (%d) instead %v\n", LEADER, server.state)
	}
}

func TestCandidateStepDown(t *testing.T) {
	server := NewServer("server1", []string{"server2", "server3"}, NewMemoryLog())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		i := 0
		for i < 2 {
			select {
			case m := <-server.outboundChan:
				go func() {
					reply(server, RequestVoteResponse{message: message{m.To(), m.From()},
						Term:    2,
						Granted: false,
					})
					wg.Done()
				}()
				i++
			}
		}
		wg.Wait()
	}()
	server.candidateLoop()
	wg.Wait()
	server.Stop()
	if !waitForState(server, FOLLOWER) {
		t.Errorf("server should have bean FOLLOWER (%d) instead %v\n", FOLLOWER, server.state)
	}
}

func TestFollowerBecomeCandidate(t *testing.T) {
	server := NewServer("server1", []string{"server2", "server3"}, NewMemoryLog())
	server.stepDown(Term(2), &RequestVoteResponse{})
	if !waitForState(server, FOLLOWER) {
		t.Errorf("server should have bean follower (%d) instead %v\n", FOLLOWER, server.state)
	}
	done := make(chan interface{})
	go func() {
		select {
		case <-done:
			return
		case <-server.outboundChan:

		}
	}()
	server.followerLoop()
	if !waitForState(server, CANDIDATE) {
		t.Errorf("server should have bean CANDIDATE (%d) instead %v\n", CANDIDATE, server.state)
	}
	server.Stop()
	close(done)
}

func TestBecomeLeader(t *testing.T) {
	server := NewServer("server1", []string{"server2", "server3"}, NewMemoryLog())
	done := make(chan interface{})
	var requestVoteGroup sync.WaitGroup
	requestVoteGroup.Add(2)
	go func() {
		for {
			select {
			case <-done:
				return
			case m := <-server.outboundChan:
				switch msg := m.(type) {
				case *RequestVote:
					go func() {
						server.inboundChan <- RequestVoteResponse{message: message{msg.To(), msg.From()},
							Term:    msg.Term,
							Granted: true,
						}
						requestVoteGroup.Done()
					}()
				default:
				}
			}
		}
	}()
	go server.Run()
	if !waitForState(server, CANDIDATE) {
		t.Errorf("server should have bean CANDIDATE (%d) instead %v\n", CANDIDATE, server.state)
	}
	requestVoteGroup.Wait()

	if !waitForState(server, LEADER) {
		t.Errorf("server should have bean LEADER (%d) instead %v\n", LEADER, server.state)
	}
	server.Stop()
	close(done)
}

func TestLeaderStepDownBecauseOfAppendEntriesResponse(t *testing.T) {
	server := NewServer("server1", []string{"server2", "server3"}, NewMemoryLog())
	server.eventsChan = make(chan StateChangeEvent, 100)
	done := make(chan interface{})
	go func() {
		for {
			select {
			case <-done:
				return
			case m := <-server.outboundChan:
				switch msg := m.(type) {
				case *RequestVote:
					go func() {
						server.inboundChan <- RequestVoteResponse{message: message{msg.To(), msg.From()},
							Term:    msg.Term,
							Granted: true,
						}
					}()
				case *AppendEntries:
					go func() {
						server.inboundChan <- AppendEntriesResponse{message: message{msg.To(), msg.From()},
							Term: Term(100),
						}
					}()

				default:
				}
			}
		}
	}()
	go server.Run()
	i := 0
out:
	for event := range server.eventsChan {
		switch i {
		case 0:
			if event.From != CANDIDATE && event.To != LEADER {
				t.Error("Should change state from CANDIDATE to LEADER instead", event)
			}
		case 1:
			if event.From != LEADER && event.To != FOLLOWER {
				t.Error("Should change state from LEADER to FOLLOWER instead", event)
			}
			break out

		}
		i += 1
	}
	server.Stop()
	close(done)
}

func TestLeaderStepDownBecauseOfRequestVote(t *testing.T) {
	server := NewServer("server1", []string{"server2", "server3"}, NewMemoryLog())
	server.eventsChan = make(chan StateChangeEvent, 100)
	done := make(chan interface{})
	go func() {
		for {
			select {
			case <-done:
				return
			case m := <-server.outboundChan:
				switch msg := m.(type) {
				case *RequestVote:
					go func() {
						server.inboundChan <- RequestVoteResponse{message: message{msg.To(), msg.From()},
							Term:    msg.Term,
							Granted: true,
						}
					}()
				case *AppendEntries:
					go func() {
						server.inboundChan <- RequestVote{message: message{msg.To(), msg.From()},
							Term: Term(msg.Term + 1),
						}
					}()

				default:
				}
			}
		}
	}()
	go server.Run()
	i := 0
out:
	for event := range server.eventsChan {
		switch i {
		case 0:
			if event.From != CANDIDATE && event.To != LEADER {
				t.Error("Should change state from CANDIDATE to LEADER instead", event)
			}
		case 1:
			if event.From != LEADER && event.To != FOLLOWER {
				t.Error("Should change state from LEADER to FOLLOWER instead", event)
			}
			break out

		}
		i += 1
	}
	server.Stop()
	close(done)
}

/*
func TestLeaderReplicateLogs(t *testing.T) {
	server := NewServer("server1", []string{"server2", "server3"}, NewMemoryLog())
	server.log.Append(LogEntry{1, Term(1)})
	server.log.Append(LogEntry{2, Term(2)})
	server.log.Append(LogEntry{3, Term(3)})
	fmt.Printf("server.log %#v\n", server.log)
	server.term = Term(3)
	server.eventsChan = make(chan StateChangeEvent, 100)
	done := make(chan interface{})
	seenFirstTerm := false
	countDown := 0
	go func() {
		for {
			select {
			case <-done:
				return
			case m := <-server.outboundChan:
				switch msg := m.(type) {
				case *RequestVote:
					go func() {
						server.inboundChan <- RequestVoteResponse{message: message{msg.To(), msg.From()},
							Term:    msg.Term,
							Granted: true,
						}
					}()
				case *AppendEntries:
					go func() {
						if msg.PrevIndex == 0 {
							seenFirstTerm = true
						}
						if !seenFirstTerm {
							countDown += 1
						}
						server.inboundChan <- AppendEntriesResponse{message: message{msg.To(), msg.From()},
							Term:    msg.Term,
							Success: seenFirstTerm,
						}
					}()

				default:
				}
			}
		}
	}()
	go server.Run()
	i := 0
out:
	for event := range server.eventsChan {
		switch i {
		case 0:
			if event.From != CANDIDATE && event.To != LEADER {
				t.Error("Should change state from CANDIDATE to LEADER instead", event)
			}
		case 1:
			if event.From != LEADER && event.To != FOLLOWER {
				t.Error("Should change state from LEADER to FOLLOWER instead", event)
			}
			break out

		}
		i += 1
	}
	server.Stop()
	close(done)

}
*/

//todo
// log replication.
// log sync.
// commit log.

func waitForState(server *server, state State) bool {
	for i := 0; i < 10; i++ {
		if server.state == state {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return server.state == state
}

func reply(server *server, message Message) {
	select {
	case server.inboundChan <- message:
		return
	case <-time.After(10 * time.Millisecond):
		fmt.Printf("Failed to reply %#v\n", message)
		return
	}
}
