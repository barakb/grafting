package grafting

import (
	"fmt"
	"github.com/nu7hatch/gouuid"
)

type Log interface {
	Length() int
	Term(index int) Term
	Slice(from int, to int) []LogEntry
	RemoveLast() LogEntry
	Append(entry LogEntry)
	NextIndex() int
}

type LogEntry struct {
	Command interface{}
	Term    Term
	Uid     *uuid.UUID
	from    string
}

type MemoryLog struct {
	entries []LogEntry
	size    int
}

func (log MemoryLog) Length() int {
	return log.size
}
func (log MemoryLog) NextIndex() int {
	return log.size + 1
}

func (log MemoryLog) Term(index int) Term {
	if index < 1 || log.size < index {
		return Term(0)
	}
	index -= 1
	return log.entries[index].Term
}

func (log MemoryLog) Slice(from int, to int) []LogEntry {
	return log.entries[from:to]
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

func (log MemoryLog) String() string {
	return fmt.Sprintf("MemoryLog{size:%d, entries:%v}", log.size, log.entries[0:log.size])
}

func NewMemoryLog() Log {
	return &MemoryLog{make([]LogEntry, 100), 0}
}
