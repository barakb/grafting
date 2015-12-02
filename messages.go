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
	term         Term
	lastLogTerm  Term
	lastLogIndex int
}

type RequestVoteResponse struct {
	message
	term    Term
	granted bool
}

type AppendEntries struct {
	message
	term        Term
	prevIndex   int
	prevTerm    Term
	entries     []interface{}
	commitIndex int
}

type AppendEntriesResponse struct {
	message
	term       Term
	success    bool
	matchIndex int
}
