package go_rafting

import (
	"sort"
)

type State int

const (
	FOLLOWER State = 1 + iota
	CANDIDATE
	LEADER
)

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
	outboundChan chan<- interface{}
	inboundChan  <-chan interface{}
}

func NewServer(id string, peers []string) *server {
	quorumSize := (len(peers) / 2) + 1
	return &server{
		id:           id,
		peers:        peers,
		state:        CANDIDATE,
		quorumSize:   quorumSize,
		outboundChan: make(chan interface{}),
		inboundChan:  make(chan interface{})}
}

func (server *server) StartNewElection() {
	if server.state == FOLLOWER || server.state == CANDIDATE {
		server.term += 1
		server.votedFor = server.id
		server.state = CANDIDATE
		server.voteGranted = make(map[string]bool, len(server.peers))
		server.matchIndex = make(map[string]int, len(server.peers))
		server.nextIndex = make(map[string]int, len(server.peers))
		for _, key := range server.peers {
			server.nextIndex[key] = 1
		}
	}
}

func (server *server) BecomeLeader() {
	if votes := countVotes(server.voteGranted); server.state == CANDIDATE && server.quorumSize <= votes {
		server.state = LEADER
		server.nextIndex = makeMap(server.peers, server.log.Length()+1)
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
	if server.state == LEADER {
		prevIndex := server.nextIndex[peer] - 1
		lastIndex := min(prevIndex+1, server.log.Length())
		if server.matchIndex[peer]+1 < server.nextIndex[peer] {
			lastIndex = prevIndex
		}
		server.sendMessage(appendEntries{from: server.id,
			to:          peer,
			term:        server.term,
			prevIndex:   prevIndex,
			prevTerm:    server.log.Term(prevIndex),
			entries:     server.log.Slice(prevIndex, lastIndex),
			commitIndex: min(server.commitIndex, lastIndex)})
	}
}

func (s *server) sendMessage(message interface{}) {
	s.outboundChan <- message
}

func countVotes(m map[string]bool) (res int) {
	for _, value := range m {
		if value {
			res += 1
		}
	}
	return res
}

func makeMap(keys []string, value int) (m map[string]int) {
	for _, key := range keys {
		m[key] = value
	}
	return m
}
