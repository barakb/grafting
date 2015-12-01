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

type server struct {
	id          string
	peers       []string
	state       State
	term        uint64
	votedFor    string
	log         Log
	commitIndex int
	voteGranted map[string]bool
	matchIndex  map[string]int
	nextIndex   map[string]int
	quorumSize  int
}

func NewServer(id string, peers []string) *server {
	quorumSize := (len(peers) / 2) + 1
	return &server{id : id, peers : peers, state : CANDIDATE, quorumSize : quorumSize}
}

func (s *server)StartNewElection() {
	if s.state == FOLLOWER || s.state == CANDIDATE {
		s.term += 1
		s.votedFor = s.id
		s.state = CANDIDATE
		s.voteGranted = make(map[string]bool, len(s.peers))
		s.matchIndex = make(map[string]int, len(s.peers))
		s.nextIndex = make(map[string]int, len(s.peers))
		for _, key := range s.peers {
			s.nextIndex[key] = 1
		}
	}
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

func (s *server)BecomeLeader() {
	if votes := countVotes(s.voteGranted); s.state == CANDIDATE && s.quorumSize <= votes {
		s.state = LEADER
		s.nextIndex = makeMap(s.peers, s.log.Length() + 1);
	}
}

func (s *server)AdvanceCommitIndex() {
	matchIndexes := make([]int, len(s.peers) + 1)
	matchIndexes = append(matchIndexes, s.log.Length())
	for _, value := range s.matchIndex {
		matchIndexes = append(matchIndexes, value)
	}
	sort.Ints(matchIndexes)
	n := matchIndexes[s.quorumSize - 1];
	if term, err := s.log.Term(n); err == nil && s.state == LEADER &&  term == s.term {
		s.commitIndex = max(s.commitIndex, n);
	}
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}


