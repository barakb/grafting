package go_rafting

import (
	log "github.com/Sirupsen/logrus"
	"math/rand"
	"sort"
	"time"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

type State int

const (
	FOLLOWER  State = 1
	CANDIDATE State = 2
	LEADER    State = 3
)

const RPC_TIMEOUT = 100 * time.Millisecond
const ELECTION_TIMEOUT = 150 * time.Millisecond

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
	done         chan interface{}
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
		done:         make(chan interface{}),
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

func (server *server) Stop() {
	close(server.done)
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
			case <-time.After(10 * time.Millisecond):
				return
			case server.eventsChan <- event:
				return
			}
		}()
	}
}

func (server *server) candidateLoop() {
	log.Infof("%s in candidate loop", server.id)
	server.startNewElection()
	electionTimeout := time.After(nextElectionTimeoutDuration())
	for server.state == CANDIDATE {
		select {
		case <-server.done:
			return
		case message := <-server.inboundChan:
			server.handleMessage(message)
			server.becomeLeader()
		case <-electionTimeout:
			server.startNewElection()
			electionTimeout = time.After(nextElectionTimeoutDuration())
		}
	}
	//	log.Infof("%s exiting candidate loop\n", server.id)
	return
}

func (server *server) followerLoop() {
	log.Infof("%s in follower loop", server.id)
	electionTimeout := time.After(ELECTION_TIMEOUT)
	for server.state == FOLLOWER {
		select {
		case <-server.done:
			return
		case message := <-server.inboundChan:
			resetTimeout := server.handleMessage(message)
			if resetTimeout {
				electionTimeout = time.After(ELECTION_TIMEOUT)
			}
		case <-electionTimeout:
			server.setState(CANDIDATE)
		}
	}
}

func (server *server) leaderLoop() {
	log.Infof("%s in leader loop", server.id)
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

func (server *server) startNewElection() {
	if server.state == FOLLOWER || server.state == CANDIDATE {
		log.Infof("%s starting new election", server.id)
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
		server.nextIndex = makeMap(server.peers, server.log.Length()+1)
		server.rpcDue = make(map[string]time.Time, len(server.peers))
		server.heartbeatDue = make(map[string]time.Time, len(server.peers))
		log.Infof("%s is LEADER", server.id)
	}
}

func (server *server) AdvanceCommitIndex() {
	matchIndexes := make([]int, len(server.peers)+1)
	matchIndexes = append(matchIndexes, server.log.Length())
	for _, value := range server.matchIndex {
		matchIndexes = append(matchIndexes, value)
	}
	sort.Ints(matchIndexes)
	n := matchIndexes[server.quorumSize-1]
	if server.state == LEADER && server.log.Term(n) == server.term {
		server.commitIndex = max(server.commitIndex, n)
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
		server.sendMessage(&AppendEntries{message: message{server.id, peer},
			Term:        server.term,
			PrevIndex:   prevIndex,
			PrevTerm:    server.log.Term(prevIndex),
			Entries:     server.log.Slice(prevIndex, lastIndex),
			CommitIndex: min(server.commitIndex, lastIndex),
		})

	}
}

func (server *server) sendRequestVote(peer string) time.Time {
	if server.state == CANDIDATE {
		server.sendMessage(&RequestVote{message: message{server.id, peer},
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
	server.sendMessage(&RequestVoteResponse{message: message{server.id, request.From()},
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
		if request.PrevIndex == 0 || (request.PrevIndex <= server.log.Length() && server.log.Term(request.PrevIndex) == request.PrevTerm) {
			success = true
			index := request.PrevIndex
			for _, entry := range request.Entries {
				index += 1
				if server.log.Term(index) != entry.Term {
					for index-1 < server.log.Length() {
						server.log.RemoveLast()
						server.log.Append(entry)
					}
				}
			}
			matchIndex = index
			server.commitIndex = max(server.commitIndex, request.CommitIndex)
		}
	}
	server.sendMessage(&AppendEntriesResponse{message: message{server.id, request.From()},
		Term:       server.term,
		Success:    success,
		MatchIndex: matchIndex,
	})
}

func (server *server) handleAppendEntriesResponse(response *AppendEntriesResponse) {
	if server.term < response.Term {
		server.stepDown(response.Term, response)
	}
	if server.state == LEADER && server.term == response.Term {
		if response.Success {
			server.matchIndex[response.From()] = max(server.matchIndex[response.From()], response.MatchIndex)
			server.nextIndex[response.From()] = response.MatchIndex + 1
		} else {
			server.nextIndex[response.From()] = max(1, server.nextIndex[response.From()]-1)
		}
		server.rpcDue[response.From()] = TIME_ZERO
	}
}

func (server *server) handleMessage(message Message) (restartElectionTimeout bool) {
	log.Infof("%s <- %#v", server.id, message)
	switch m := message.(type) {
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
	default:
		log.Warnf("%s ignoring unexpected message type %v\n", server.id, message)
		return false
	}
}

func (server *server) stepDown(term Term, message Message) {
	if server.state != FOLLOWER {
		log.Infof("%s step down from %v -> %v at term %d, because of message %#v", server.id, server.state, FOLLOWER, term, message)
	}
	server.term = term
	server.setState(FOLLOWER)
	server.votedFor = ""
}

func (server *server) sendMessage(message Message) {
	log.Infof("%s -> %#v", server.id, message)
	server.outboundChan <- message
}

/*
 * RouteTarget methods
 */

func (server *server) Address() string {
	return server.id
}

func (server *server) OutboundChan() chan Message {
	return server.outboundChan
}

func (server *server) InboundChan() chan Message {
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

func makeMap(keys []string, value int) (m map[string]int) {
	m = make(map[string]int)
	for _, key := range keys {
		m[key] = value
	}
	return m
}

func nextElectionTimeoutDuration() time.Duration {
	return time.Duration(int(ELECTION_TIMEOUT) + rand.Intn(int(ELECTION_TIMEOUT)/2))
}
