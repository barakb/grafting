package grafting

import "encoding/gob"

func init() {
	gob.Register(RequestVote{})
	gob.Register(RequestVoteResponse{})
	gob.Register(AppendEntries{})
	gob.Register(AppendEntriesResponse{})
	gob.Register(StateChangeEvent{})

}

type Message interface {
	From() string
	To() string
}

type message struct {
	from string
	to   string
}

func (message message) From() string {
	return message.from
}

func (message message) To() string {
	return message.to
}

type RequestVote struct {
	message
	Term         Term
	LastLogTerm  Term
	LastLogIndex int
}

type RequestVoteResponse struct {
	message
	Term    Term
	Granted bool
}

type AppendEntries struct {
	message
	Term        Term
	PrevIndex   int
	PrevTerm    Term
	Entries     []LogEntry
	CommitIndex int
}

type AppendEntriesResponse struct {
	message
	Term       Term
	Success    bool
	MatchIndex int
}

type StateChangeEvent struct {
	From State
	To   State
}
