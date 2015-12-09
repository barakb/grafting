package go_rafting

type Log interface {
	Length() int
	Term(index int) Term
	Slice(from int, to int) []LogEntry
	RemoveLast() LogEntry
	Append(entry LogEntry)
}

type LogEntry struct {
	Command interface{}
	Term    Term
}

type MemoryLog struct {
	entries []LogEntry
	size    int
}

func (log MemoryLog) Length() int {
	return log.size
}

func (log MemoryLog) Term(index int) Term {
	if index < 1 || log.size < index{
		return Term(0)
	}
	index -= 1
	return log.entries[index].Term
}

func (log MemoryLog) Slice(from int, to int) []LogEntry {
	from = max(0, from -1)
	to = min(to - 1, log.size)
	return log.entries[from:min(to, log.size)]
}

func (log *MemoryLog) Append(entry LogEntry) {
	if log.size == len(log.entries) {
		log.entries = append(log.entries, entry)
	} else {
		log.entries[log.size] = entry
	}
	log.size += 1
}

func (log *MemoryLog) RemoveLast() LogEntry {
	res := log.entries[log.size-1]
	log.size -= 1
	return res
}

func NewMemoryLog() Log {
	return &MemoryLog{make([]LogEntry, 100), 0}
}
