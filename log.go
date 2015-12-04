package go_rafting

type Log interface {
	Length() int
	Term(index int) Term
	Slice(from int, to int) []LogEntry
}

type LogEntry struct {
	Command interface{}
	Term    Term
}

type MemoryLog struct {
	entries []LogEntry
}

func (log MemoryLog) Length() int {
	return len(log.entries)
}

func (log MemoryLog) Term(index int) Term {
	return log.entries[index].Term
}

func (log MemoryLog) Slice(from int, to int) []LogEntry {
	return log.entries[from:to]
}

func NewMemoryLog() Log {
	return MemoryLog{make([]LogEntry, 100)}
}
