package grafting

import "encoding/gob"

func init() {
	gob.Register(RequestVote{})
	gob.Register(RequestVoteResponse{})
	gob.Register(AppendEntries{})
	gob.Register(AppendEntriesResponse{})
	gob.Register(StateChangeEvent{})
	gob.Register(Msg{})

}

type Message interface {
	From() string
	To() string
}

type Msg struct {
	F string
	T string
}

func (message Msg) From() string {
	return message.F
}

func (message Msg) To() string {
	return message.T
}

type RequestVote struct {
	Msg
	Term         Term
	LastLogTerm  Term
	LastLogIndex int
}

type RequestVoteResponse struct {
	Msg
	Term    Term
	Granted bool
}

type AppendEntries struct {
	Msg
	Term        Term
	PrevIndex   int
	PrevTerm    Term
	Entries     []LogEntry
	CommitIndex int
}

type AppendEntriesResponse struct {
	Msg
	Term       Term
	Success    bool
	MatchIndex int
}

type StateChangeEvent struct {
	From State
	To   State
}
