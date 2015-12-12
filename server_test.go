package grafting

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
		server.Close()
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
					server.inboundChan <- &RequestVoteResponse{message: message{m.To(), m.From()},
						Term:    1,
						Granted: true,
					}
					wg.Done()
				}()
				i++
			}
		}
		wg.Wait()
		server.Close()
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
					reply(server, &RequestVoteResponse{message: message{m.To(), m.From()},
						Term:    term,
						Granted: term == 2,
					})
					wg.Done()
				}()
				i++
			}
		}
		wg.Wait()
		server.Close()
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
					reply(server, &RequestVoteResponse{message: message{m.To(), m.From()},
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
	server.Close()
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
	server.Close()
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
						server.inboundChan <- &RequestVoteResponse{message: message{msg.To(), msg.From()},
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
	server.Close()
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
						server.inboundChan <- &RequestVoteResponse{message: message{msg.To(), msg.From()},
							Term:    msg.Term,
							Granted: true,
						}
					}()
				case *AppendEntries:
					go func() {
						server.inboundChan <- &AppendEntriesResponse{message: message{msg.To(), msg.From()},
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
	server.Close()
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
						server.inboundChan <- &RequestVoteResponse{message: message{msg.To(), msg.From()},
							Term:    msg.Term,
							Granted: true,
						}
					}()
				case *AppendEntries:
					go func() {
						server.inboundChan <- &RequestVote{message: message{msg.To(), msg.From()},
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
	server.Close()
	close(done)
}

func TestLeaderReplicateLogs(t *testing.T) {
	server := NewServer("server1", []string{"server2", "server3"}, NewMemoryLog())
	server.log.Append(LogEntry{1, Term(1)})
	server.log.Append(LogEntry{SetValue{"foo1", "bar1"}, Term(2)})
	server.log.Append(LogEntry{SetValue{"foo2", "bar2"}, Term(4)})
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
						server.inboundChan <- &RequestVoteResponse{message: message{msg.To(), msg.From()},
							Term:    msg.Term,
							Granted: true,
						}
					}()
				case *AppendEntries:
					if seenFirstTerm && msg.PrevIndex == 3 && server.term == 4 && server.nextIndex["server2"] == 4 {
						server.Close()
						return
					}
					go func() {
						if msg.PrevIndex == 0 {
							seenFirstTerm = true
						}
						if !seenFirstTerm {
							countDown += 1
						}
						server.inboundChan <- &AppendEntriesResponse{message: message{msg.To(), msg.From()},
							Term:       msg.Term,
							MatchIndex: msg.PrevIndex + len(msg.Entries),
							Success:    seenFirstTerm,
						}
					}()

				default:
				}
			}
		}
	}()
	if server.commitIndex != 0 {
		t.Error("Commit index should be zero, instead", server.commitIndex)
	}
	server.Run()
	<-server.done
	if server.term != 4 {
		t.Error("server term should be 4, instead", server.term)
	}
	if server.state != LEADER {
		t.Error("Should be LEADER instead", server.state)
	}
	if server.nextIndex["server2"] != 4 {
		t.Error("server.nextIndex[\"server2\"] should be 4 instead", server.nextIndex["server2"])
	}
	if server.matchIndex["server2"] != 3 {
		t.Error("server.matchIndex[\"server2\"] should be 3 instead", server.matchIndex["server2"])
	}

	if server.commitIndex != 3 {
		t.Error("Commit index should be 3, instead", server.commitIndex)
	}

	bar, ok := server.stateMachine["foo2"]
	if !ok || bar != "bar2" {
		t.Errorf("After follower commit log entry 1 state machine[%q] should be %q, instead state machine is:%v", "foo2", "bar2", server.stateMachine)
	}

}

func TestFollowerReplicateLogs(t *testing.T) {
	server := NewServer("server1", []string{"server2", "server3"}, NewMemoryLog())
	go func() {
		term := Term(1000)
		server.inboundChan <- &AppendEntries{message: message{"server1", "server2"},
			Term:        term,
			PrevIndex:   1,
			PrevTerm:    term,
			Entries:     []LogEntry{},
			CommitIndex: 0,
		}
		server.inboundChan <- &AppendEntries{message: message{"server1", "server2"},
			Term:        term,
			PrevIndex:   0,
			PrevTerm:    term,
			Entries:     []LogEntry{{1, Term(1)}},
			CommitIndex: 0,
		}
	}()
	aa := make([]*AppendEntriesResponse, 0)
	go func() {
		for {
			select {
			case <-server.done:
				return
			case m := <-server.outboundChan:
				switch msg := m.(type) {
				case *AppendEntriesResponse:
					aa = append(aa, msg)
				case *RequestVote:
					if msg.Term == Term(1001) {
						server.Close()
					}
				default:
				}
			}
		}
	}()
	server.Run()
	first := aa[0]
	if first.Success != false {
		t.Error("First response should not success", first)
	}
	second := aa[1]
	if second.Success != true {
		t.Error("Second response should success", second)
	}
	if second.MatchIndex != 1 {
		t.Error("Second MatchIndex should be 1", second)
	}
	// check the log.
	if server.log.Length() != 1 {
		t.Error("Server log len should be 1", server.log)
	}
	le := server.log.Slice(0, 1)[0]
	if le.Term != Term(1) {
		t.Error("Server log first term should be 1", server.log)
	}
	if le.Command != 1 {
		t.Error("Server log first command should be 1", server.log)
	}
}

func TestFollowerReplicateTruncateLogs(t *testing.T) {
	server := NewServer("server1", []string{"server2", "server3"}, NewMemoryLog())
	server.log.Append(LogEntry{1, 1})
	server.log.Append(LogEntry{2, 1})
	go func() {
		term := Term(1000)
		server.inboundChan <- &AppendEntries{message: message{"server1", "server2"},
			Term:        term,
			PrevIndex:   1,
			PrevTerm:    term,
			Entries:     []LogEntry{},
			CommitIndex: 0,
		}
		server.inboundChan <- &AppendEntries{message: message{"server1", "server2"},
			Term:        term,
			PrevIndex:   0,
			PrevTerm:    term,
			Entries:     []LogEntry{{SetValue{"foo", "bar"}, Term(3)}},
			CommitIndex: 1,
		}
	}()
	aa := make([]*AppendEntriesResponse, 0)
	go func() {
		for {
			select {
			case <-server.done:
				return
			case m := <-server.outboundChan:
				switch msg := m.(type) {
				case *AppendEntriesResponse:
					aa = append(aa, msg)
				case *RequestVote:
					if msg.Term == Term(1001) {
						server.Close()
					}
				default:
				}
			}
		}
	}()
	if server.commitIndex != 0 {
		t.Error("Server commitIndex should be 0, instead", server.commitIndex)
	}
	server.Run()
	first := aa[0]
	if first.Success != false {
		t.Error("First response should not success, instead", first)
	}
	second := aa[1]
	if second.Success != true {
		t.Error("Second response should success, instead", second)
	}
	if second.MatchIndex != 1 {
		t.Error("Second MatchIndex should be 1, instead", second)
	}
	// check the log.
	if server.log.Length() != 1 {
		t.Error("Server log len should be 1, instead", server.log)
	}
	le := server.log.Slice(0, 1)[0]
	if le.Term != Term(3) {
		t.Error("Server log first term should be 1, instead", server.log)
	}
	if le.Term != Term(3) {
		t.Error("Server log first term should be 3, instead", server.log)
	}
	if server.commitIndex != 1 {
		t.Error("Server commitIndex should be 1, instead", server.commitIndex)
	}
	bar, ok := server.stateMachine["foo"]
	if !ok || bar != "bar" {
		t.Errorf("After follower commit log entry 1 state machine[%q] should be %q, instead state machine is:%v", "foo", "bar", server.stateMachine)
	}
}

//todo
// execute commands when updating commit index.

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
