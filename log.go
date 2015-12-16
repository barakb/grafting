package grafting

import (
	"container/list"
	"fmt"
	logger "github.com/Sirupsen/logrus"
	"github.com/nu7hatch/gouuid"
)

type Log interface {
	Length() int
	Term(index int) Term
	Slice(from int, to int) []LogEntry
	RemoveLast() LogEntry
	Append(entry LogEntry)
	NextIndex() int
	IsRequestPresent(clientName string, uid string) (interface{}, bool)
	Commit(entry LogEntry, res interface{})
}

type LogEntry struct {
	Command interface{}
	Term    Term
	Uid     *uuid.UUID
	From    string
}

type clientRequest struct {
	Entry  *LogEntry
	Result interface{}
}

type MemoryLog struct {
	entries            []LogEntry
	size               int
	clientLastRequests map[string]*list.List
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
	log.updateClientLastRequests(&clientRequest{Entry: &entry})
}

func (log *MemoryLog) RemoveLast() LogEntry {
	res := log.entries[log.size-1]
	log.size -= 1
	return res
}

func (log *MemoryLog) Commit(entry LogEntry, res interface{}) {
	if lst, ok := log.clientLastRequests[entry.From]; ok {
		for e := lst.Front(); e != nil; e = e.Next() {
			clientRequest := e.Value.(*clientRequest)
			if (*clientRequest.Entry.Uid).String() == (*entry.Uid).String() {
				clientRequest.Result = res
				return
			}
		}
	}
	logger.Fatalf("When commiting entry: %#v with result:%#v clientLastRequest not found in clientLastRequests %#v", entry, res, log.clientLastRequests)
}

func (log MemoryLog) IsRequestPresent(clientName string, uid string) (interface{}, bool) {
	if lst, ok := log.clientLastRequests[clientName]; ok {
		for e := lst.Front(); e != nil; e = e.Next() {
			clientRequest := e.Value.(*clientRequest)
			if (*clientRequest.Entry.Uid).String() == uid {
				return clientRequest.Result, true
			}
		}

	}
	return nil, false
}

func (log MemoryLog) String() string {
	return fmt.Sprintf("MemoryLog{size:%d, entries:%v}", log.size, log.entries[0:log.size])
}

func NewMemoryLog() Log {
	return &MemoryLog{make([]LogEntry, 100), 0, make(map[string]*list.List)}
}

func (log *MemoryLog) updateClientLastRequests(clientRequest *clientRequest) {
	lst, ok := log.clientLastRequests[clientRequest.Entry.From]
	if !ok {
		lst = list.New()
		log.clientLastRequests[clientRequest.Entry.From] = lst
	}
	lst.PushBack(clientRequest)
	if lst.Len() == 6 {
		lst.Remove(lst.Front())
	}
}
