package go_rafting

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
	term    Term
	granted bool
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
