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
					server.inboundChan <- &RequestVoteResponse{Msg: Msg{m.To(), m.From()},
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
					reply(server, &RequestVoteResponse{Msg: Msg{m.To(), m.From()},
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
					reply(server, &RequestVoteResponse{Msg: Msg{m.To(), m.From()},
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
	done := make(chan struct{})
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
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case m := <-server.outboundChan:
				switch msg := m.(type) {
				case *RequestVote:
					go func() {
						server.inboundChan <- &RequestVoteResponse{Msg: Msg{msg.To(), msg.From()},
							Term:    msg.Term,
							Granted: true,
						}
					}()
				default:
				}
			}
		}
	}()
	go server.Run()
	if !waitForState(server, LEADER) {
		t.Errorf("server should have bean LEADER (%d) instead %v\n", LEADER, server.state)
	}
	server.Close()
	close(done)
}

func TestLeaderStepDownBecauseOfAppendEntriesResponse(t *testing.T) {
	server := NewServer("server1", []string{"server2", "server3"}, NewMemoryLog())
	server.eventsChan = make(chan StateChangeEvent, 100)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case m := <-server.outboundChan:
				switch msg := m.(type) {
				case *RequestVote:
					go func() {
						server.inboundChan <- &RequestVoteResponse{Msg: Msg{msg.To(), msg.From()},
							Term:    msg.Term,
							Granted: true,
						}
					}()
				case *AppendEntries:
					go func() {
						server.inboundChan <- &AppendEntriesResponse{Msg: Msg{msg.To(), msg.From()},
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
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case m := <-server.outboundChan:
				switch msg := m.(type) {
				case *RequestVote:
					go func() {
						server.inboundChan <- &RequestVoteResponse{Msg: Msg{msg.To(), msg.From()},
							Term:    msg.Term,
							Granted: true,
						}
					}()
				case *AppendEntries:
					go func() {
						server.inboundChan <- &RequestVote{Msg: Msg{msg.To(), msg.From()},
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
	server.log.Append(LogEntry{Command: 1, Term: Term(1), Uid: newUID(), From: "client1"})
	server.log.Append(LogEntry{Command: SetValue{"foo1", "bar1"}, Term: Term(2), Uid: newUID(), From: "client1"})
	server.log.Append(LogEntry{Command: SetValue{"foo2", "bar2"}, Term: Term(4), Uid: newUID(), From: "client1"})
	server.term = Term(3)
	server.eventsChan = make(chan StateChangeEvent, 100)
	done := make(chan struct{})
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
						server.inboundChan <- &RequestVoteResponse{Msg: Msg{msg.To(), msg.From()},
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
						server.inboundChan <- &AppendEntriesResponse{Msg: Msg{msg.To(), msg.From()},
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
		server.inboundChan <- &AppendEntries{Msg: Msg{"server1", "server2"},
			Term:        term,
			PrevIndex:   1,
			PrevTerm:    term,
			Entries:     []LogEntry{},
			CommitIndex: 0,
		}
		server.inboundChan <- &AppendEntries{Msg: Msg{"server1", "server2"},
			Term:        term,
			PrevIndex:   0,
			PrevTerm:    term,
			Entries:     []LogEntry{{Command: 1, Term: Term(1), Uid: newUID(), From: "client1"}},
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
	server.log.Append(LogEntry{Command: 1, Term: 1, Uid: newUID(), From: "client1"})
	server.log.Append(LogEntry{Command: 2, Term: 1, Uid: newUID(), From: "client1"})
	go func() {
		term := Term(1000)
		server.inboundChan <- &AppendEntries{Msg: Msg{"server1", "server2"},
			Term:        term,
			PrevIndex:   1,
			PrevTerm:    term,
			Entries:     []LogEntry{},
			CommitIndex: 0,
		}
		server.inboundChan <- &AppendEntries{Msg: Msg{"server1", "server2"},
			Term:        term,
			PrevIndex:   0,
			PrevTerm:    term,
			Entries:     []LogEntry{{Command: SetValue{"foo", "bar"}, Term: Term(3), Uid: newUID(), From: "client1"}},
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

func TestServerSaveLast5RequestFromEachClient(t *testing.T) {
	server := NewServer("server1", []string{"server2", "server3"}, NewMemoryLog())
	becomeALeader(server)

	done := readServerOutbound(server)
	req1 := &StateMachineCommandRequest{Command: SetValue{"foo", "bar"}, Uid: newUID(), Msg: Msg{T: "server1", F: "client1"}}
	server.handleStateMachineCommand(req1)
	if server.log.Length() != 1 {
		t.Errorf("log should have one entry after server handle command instead %#v", server.log)
	}
	// retry req and see that no entry added to the log.
	server.handleStateMachineCommand(req1)
	if server.log.Length() != 1 {
		t.Errorf("log should have one entry after server handle command instead %#v", server.log)
	}
	// send another command and see that it is append to log
	req2 := &StateMachineCommandRequest{Command: SetValue{"foo", "bar1"}, Uid: newUID(), Msg: Msg{T: "server1", F: "client1"}}
	server.handleStateMachineCommand(req2)
	if server.log.Length() != 2 {
		t.Errorf("log should have 2 entries after server handle command instead %#v", server.log)
	}
	// make the server commit req1
	server.handleAppendEntriesResponse(&AppendEntriesResponse{Msg: Msg{"server1", "server2"},
		Term: server.term, Success: true, MatchIndex: 1,
	})
	if server.commitIndex != 1 {
		t.Errorf("server should commit the first log entry, instead %d", server.commitIndex)
	}
	becomeALeader(server)
	close(done)
	// send req 1 again expect to have the result in the outputbound channel.
	server.handleStateMachineCommand(req1)
	if server.log.Length() != 2 {
		t.Errorf("log should have 2 entries after server handle command instead %#v", server.log)
	}
	if server.commitIndex != 1 {
		t.Errorf("server should commit the first log entry, instead %d", server.commitIndex)
	}
	msg := <-server.outboundChan
	if msg.(*StateMachineCommandResponse).Uid.String() != req1.Uid.String() {
		t.Errorf("expect reply with uid %s instead got %#v", req1.Uid.String(), msg)
	}
}

func readServerOutbound(server *server) (done chan struct{}) {
	done = make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-server.outboundChan:
			}
		}
	}()
	return done
}

func becomeALeader(server *server) {
	server.voteGranted = make(map[string]bool, len(server.peers))
	server.matchIndex = make(map[string]int, len(server.peers))
	server.nextIndex = make(map[string]int, len(server.peers))
	for _, key := range server.peers {
		server.nextIndex[key] = 1
	}

	server.state = LEADER
	server.nextIndex = makeIntMap(server.peers, server.log.Length())
	for _, key := range server.peers {
		server.nextIndex[key] = server.log.NextIndex()
	}
	server.rpcDue = make(map[string]time.Time, len(server.peers))
	server.heartbeatDue = make(map[string]time.Time, len(server.peers))

}

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
