package grafting

import (
	logger "github.com/Sirupsen/logrus"
	"math/rand"
	"sort"
	"time"
)

func init() {
	logger.SetLevel(logger.InfoLevel)
}

type State int

const (
	FOLLOWER  State = 1
	CANDIDATE State = 2
	LEADER    State = 3
)

const RPC_TIMEOUT = 100 * time.Millisecond
const ELECTION_TIMEOUT = 150 * time.Millisecond

//const RPC_TIMEOUT = 10 * time.Second
//const ELECTION_TIMEOUT = 15 * time.Second

var TIME_ZERO time.Time = time.Time{}

type Term uint64

type server struct {
	id           string
	peers        []string
	state        State
	term         Term
	votedFor     string
	log          Log
	commitIndex  int
	voteGranted  map[string]bool
	matchIndex   map[string]int
	nextIndex    map[string]int
	quorumSize   int
	outboundChan chan Message
	inboundChan  chan Message
	rpcDue       map[string]time.Time
	heartbeatDue map[string]time.Time
	eventsChan   chan StateChangeEvent
	stateMachine StateMachine
	done         chan struct{}
}

func NewServer(id string, peers []string, log Log) *server {
	quorumSize := (len(peers) / 2) + 1
	return &server{
		id:           id,
		peers:        peers,
		log:          log,
		state:        CANDIDATE,
		quorumSize:   quorumSize,
		outboundChan: make(chan Message),
		inboundChan:  make(chan Message),
		rpcDue:       make(map[string]time.Time),
		heartbeatDue: make(map[string]time.Time),
		stateMachine: NewStateMachine(),
		done:         make(chan struct{}),
	}
}

func (server *server) Run() {
	for {
		select {
		case <-server.done:
			return
		default:
			switch server.state {
			case CANDIDATE:
				server.candidateLoop()
			case FOLLOWER:
				server.followerLoop()
			case LEADER:
				server.leaderLoop()
			}
		}
	}
}

func (server *server) Close() error {
	close(server.done)
	return nil
}

func (server *server) setState(state State) {
	if server.state != state {
		old := server.state
		server.state = state
		server.sendEvent(StateChangeEvent{old, state})
	}
}

func (server *server) sendEvent(event StateChangeEvent) {
	if server.eventsChan != nil {
		go func() {
			select {
			case <-server.done:
				return
			case server.eventsChan <- event:
				return
			default:
			}
		}()
	}
}

func (server *server) candidateLoop() {
	electionTimeout := server.startNewElection()
	timeoutTimer := time.After(electionTimeout)
	for server.state == CANDIDATE {
		logger.Debugf("-- server %s candidateLoop", server.id)
		select {
		case <-server.done:
			return
		case message := <-server.inboundChan:
			server.handleMessage(message)
			server.becomeLeader()
		case <-timeoutTimer:
			electionTimeout = server.startNewElection()
			timeoutTimer = time.After(electionTimeout)
		}
	}
	return
}

func (server *server) followerLoop() {
	electionTimeout := time.After(server.nextElectionTimeoutDuration())
	for server.state == FOLLOWER {
		logger.Debugf("-- server %s followerLoop", server.id)
		select {
		case <-server.done:
			return
		case message := <-server.inboundChan:
			resetTimeout := server.handleMessage(message)
			if resetTimeout {
				electionTimeout = time.After(server.nextElectionTimeoutDuration())
			}
		case <-electionTimeout:
			server.setState(CANDIDATE)
		}
	}
}

func (server *server) leaderLoop() {
	timer := server.sendAllAppendEntries()
	for server.state == LEADER {
		select {
		case <-server.done:
			return
		case message := <-server.inboundChan:
			server.handleMessage(message)
		case <-timer:
			timer = server.sendAllAppendEntries()
		}
	}
}

func (server *server) sendAllAppendEntries() <-chan time.Time {
	for _, peer := range server.peers {
		server.sendAppendEntries(peer)
	}
	return server.leaderTimer()
}

func (server *server) leaderTimer() <-chan time.Time {
	var res time.Time
	for _, key := range server.peers {
		if res.IsZero() || res.After(server.rpcDue[key]) {
			res = server.rpcDue[key]
		}
		if res.IsZero() || res.After(server.heartbeatDue[key]) {
			res = server.heartbeatDue[key]
		}
	}
	return time.After(durationUntil(res))
}

func (server *server) startNewElection() time.Duration {
	if server.state == FOLLOWER || server.state == CANDIDATE {
		logger.Debugf("%s starting new election", server.id)
		server.term += 1
		server.votedFor = server.id
		server.setState(CANDIDATE)
		server.voteGranted = make(map[string]bool, len(server.peers))
		server.matchIndex = make(map[string]int, len(server.peers))
		server.nextIndex = make(map[string]int, len(server.peers))
		for _, key := range server.peers {
			server.nextIndex[key] = 1
		}
		server.rpcDue = make(map[string]time.Time, len(server.peers))
		server.heartbeatDue = make(map[string]time.Time, len(server.peers))
		server.requestVote()
	}
	return server.nextElectionTimeoutDuration()
}

func (server *server) requestVote() {
	// send requestVote to each peer
	for _, peer := range server.peers {
		server.sendRequestVote(peer)
	}
}

func (server *server) becomeLeader() {
	if votes := countVotes(server.voteGranted); server.state == CANDIDATE && server.quorumSize <= votes {
		server.setState(LEADER)
		server.nextIndex = makeIntMap(server.peers, server.log.Length())
		for _, key := range server.peers {
			server.nextIndex[key] = server.log.NextIndex()
		}
		server.rpcDue = make(map[string]time.Time, len(server.peers))
		server.heartbeatDue = make(map[string]time.Time, len(server.peers))
		logger.Debugf("%s is LEADER", server.id)
	}
}

func (server *server) sendAppendEntries(peer string) {
	if server.state == LEADER &&
		(time.Now().After(server.heartbeatDue[peer]) ||
			(server.nextIndex[peer] <= server.log.Length() && time.Now().After(server.rpcDue[peer]))) {
		server.rpcDue[peer] = time.Now().Add(RPC_TIMEOUT)
		server.heartbeatDue[peer] = time.Now().Add(ELECTION_TIMEOUT / 2)
		prevIndex := server.nextIndex[peer] - 1
		lastIndex := min(prevIndex+1, server.log.Length())
		if server.matchIndex[peer]+1 < server.nextIndex[peer] {
			lastIndex = prevIndex
		}
		msg := &AppendEntries{Msg: Msg{server.id, peer},
			Term:        server.term,
			PrevIndex:   prevIndex,
			PrevTerm:    server.log.Term(prevIndex),
			Entries:     server.log.Slice(prevIndex, lastIndex),
			CommitIndex: min(server.commitIndex, lastIndex),
		}
		server.sendMessage(msg)
	}
}

func (server *server) sendRequestVote(peer string) time.Time {
	if server.state == CANDIDATE {
		server.sendMessage(&RequestVote{Msg: Msg{server.id, peer},
			Term:         server.term,
			LastLogTerm:  server.log.Term(server.log.Length()),
			LastLogIndex: server.log.Length(),
		})
	}
	return server.rpcDue[peer]
}

func (server *server) handleRequestVote(request *RequestVote) (granted bool) {
	if server.term < request.Term {
		server.stepDown(request.Term, request)
	}
	granted = false
	if server.term == request.Term && (server.votedFor == "" || server.votedFor == request.From()) &&
		(server.log.Term(server.log.Length()) < request.LastLogTerm ||
			(server.log.Term(server.log.Length()) == request.LastLogTerm &&
				server.log.Length() <= request.LastLogIndex)) {
		granted = true
		server.votedFor = request.From()
	}
	server.sendMessage(&RequestVoteResponse{Msg: Msg{server.id, request.From()},
		Term:    server.term,
		Granted: granted,
	})
	return granted
}

func (server *server) handleRequestVoteResponse(response *RequestVoteResponse) {
	if server.term < response.Term {
		server.stepDown(response.Term, response)
	}
	if server.state == CANDIDATE && server.term == response.Term {
		server.rpcDue[response.From()] = TIME_ZERO
		server.voteGranted[response.From()] = response.Granted
	}
}

func (server *server) handleAppendEntries(request *AppendEntries) {
	success := false
	matchIndex := 0
	if server.term < request.Term {
		server.stepDown(request.Term, request)
	}
	if server.term == request.Term {
		server.setState(FOLLOWER)
		if request.PrevIndex == 0 ||
			(request.PrevIndex <= server.log.Length() && server.log.Term(request.PrevIndex) == request.PrevTerm) {
			success = true
			index := request.PrevIndex
			for _, entry := range request.Entries {
				index += 1
				if server.log.Term(index) != entry.Term {
					for index-1 < server.log.Length() {
						server.log.RemoveLast()
					}
					server.log.Append(entry)
				}
			}
			matchIndex = index
			server.commit(max(server.commitIndex, request.CommitIndex))
		}
	}
	server.sendMessage(&AppendEntriesResponse{Msg: Msg{server.id, request.From()},
		Term:       server.term,
		Success:    success,
		MatchIndex: matchIndex,
	})
}

func (server *server) commit(commitIndex int) {
	for _, logEntry := range server.log.Slice(server.commitIndex, commitIndex) {
		if cmd, ok := logEntry.Command.(StateMachineCommand); ok {
			logger.Infof("%s: executing state machine command %#v", server.id, logEntry)
			res := cmd.Execute(server.stateMachine)
			server.log.Commit(logEntry, res)
			if server.state == LEADER {
				server.sendMessageAsync(&StateMachineCommandResponse{Msg: Msg{T: logEntry.From, F: server.id},
					Uid: logEntry.Uid, ReturnValue: res})
			}
		}
	}
	server.commitIndex = commitIndex
}

func (server *server) handleAppendEntriesResponse(response *AppendEntriesResponse) {
	if server.term < response.Term {
		server.stepDown(response.Term, response)
	}
	if server.state == LEADER && server.term == response.Term {
		if response.Success {
			//			log.Infof("server.matchIndex[response.From()] = %d, response.MatchIndex=%d", server.matchIndex[response.From()], response.MatchIndex)
			server.matchIndex[response.From()] = max(server.matchIndex[response.From()], response.MatchIndex)
			server.nextIndex[response.From()] = response.MatchIndex + 1
			server.advanceCommitIndex()
		} else {
			server.nextIndex[response.From()] = max(1, server.nextIndex[response.From()]-1)
		}
		server.rpcDue[response.From()] = TIME_ZERO
	}
}

func (server *server) handleStateMachineCommand(request *StateMachineCommandRequest) {
	if server.state == LEADER {
		// handle in process requests
		if res, found, hasValue := server.log.IsRequestPresent(request.F, (*request.Uid).String()); found {
			//			log.Infof("command uid:%s found:%v, hasValue:%v, res:%v", (*request.Uid).String(), found, hasValue, res)
			if hasValue {
				//send value again
				server.sendMessageAsync(&StateMachineCommandResponse{Msg: Msg{T: request.F, F: server.id},
					Uid: request.Uid, ReturnValue: res})
			}
			return
		}

		server.log.Append(LogEntry{request.Command, server.term, request.Uid, request.From()})
	}
}

func (server *server) sendMessageAsync(msg Message) {
	go func() {
		//		log.Infof("sending async message %#v", msg)
		server.sendMessage(msg)
	}()
}

func (server *server) advanceCommitIndex() {
	matchIndexes := make([]int, 0)
	matchIndexes = append(matchIndexes, server.log.Length())
	for _, value := range server.matchIndex {
		matchIndexes = append(matchIndexes, value)
	}

	sort.Sort(sort.Reverse(sort.IntSlice(matchIndexes)))
	//	sort.Ints(matchIndexes)

	n := matchIndexes[server.quorumSize-1]
	//	log.Infof("matchIndexes=%#v, server.quorumSize-1=%d, n=%d\n", matchIndexes, server.quorumSize-1, n)
	if server.state == LEADER && server.log.Term(n) == server.term {
		server.commit(max(server.commitIndex, n))
	}
}

func (server *server) handleMessage(message Message) (restartElectionTimeout bool) {
	logger.Debugf("%s <- %s: %#v", server.id, message.From(), message)
	switch m := message.(type) {
	case *RequestVote:
		return server.handleRequestVote(m)
	case *RequestVoteResponse:
		server.handleRequestVoteResponse(m)
		return false
	case *AppendEntries:
		server.handleAppendEntries(m)
		return true
	case *AppendEntriesResponse:
		server.handleAppendEntriesResponse(m)
		return false
	case *StateMachineCommandRequest:
		server.handleStateMachineCommand(m)
		return false

	case RequestVote:
		return server.handleRequestVote(&m)
	case RequestVoteResponse:
		server.handleRequestVoteResponse(&m)
		return false
	case AppendEntries:
		server.handleAppendEntries(&m)
		return true
	case AppendEntriesResponse:
		server.handleAppendEntriesResponse(&m)
		return false
	case StateMachineCommandRequest:
		server.handleStateMachineCommand(&m)
		return false
	default:
		logger.Warnf("%s ignoring unexpected message from %s: %#v\n", server.id, message.From(), message)
		return false
	}
}

func (server *server) stepDown(term Term, message Message) {
	if server.state != FOLLOWER {
		logger.Debugf("%s step down from %v -> %v at term %d, because of message %#v", server.id, server.state, FOLLOWER, server.term, message)
	}
	server.term = term
	server.setState(FOLLOWER)
	server.votedFor = ""
}

func (server *server) sendMessage(message Message) {
	select {
	case <-server.done:
		return
	case server.outboundChan <- message:
		return
	}
}

/*
 * RouteTarget methods
 */

func (server *server) Address() string {
	return server.id
}

func (server *server) OutboundChan() <-chan Message {
	return server.outboundChan
}

func (server *server) InboundChan() chan<- Message {
	return server.inboundChan
}

func countVotes(m map[string]bool) (res int) {
	for _, value := range m {
		if value {
			res += 1
		}
	}
	return res
}

/*
 * Functions
 */

func (server *server) nextElectionTimeoutDuration() time.Duration {
	res := time.Duration(int(ELECTION_TIMEOUT) + rand.Intn(int(ELECTION_TIMEOUT)/2))
	logger.Debugf("%s nextElectionTimeoutDuration:%v", server.id, res)
	return res
}
