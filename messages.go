package go_rafting

type requestVote struct {
	from         string
	to           string
	term         Term
	lastLogTerm  Term
	lastLogIndex int
}

type requestVoteResponse struct {
	from    string
	to      string
	term    Term
	granted bool
}

type appendEntries struct {
	from        string
	to          string
	term        Term
	prevIndex   int
	prevTerm    Term
	entries     []interface{}
	commitIndex int
}

type appendEntriesResponse struct {
	from       string
	to         string
	term       Term
	success    bool
	matchIndex int
}
