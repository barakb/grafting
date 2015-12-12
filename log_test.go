package grafting

import (
	"fmt"
	"testing"
)

func TestCreateMemoryLog(t *testing.T) {
	memoryLog := NewMemoryLog()
	if memoryLog.Length() != 0 {
		t.Fatal("new log should have zero length, instead", memoryLog.Length())
	}
}

func TestMemoryLogTerm(t *testing.T) {
	log := NewMemoryLog()
	log.Append(LogEntry{1, 2})
	if term := log.Term(0); term != Term(0) {
		t.Fatal("log index start at 1 so logTerm(0) should returns 0 instead", term)
	}
}

func TestMemoryLogSlice(t *testing.T) {
	log := NewMemoryLog()
	log.Append(LogEntry{1, 1})
	log.Append(LogEntry{2, 2})
	log.Append(LogEntry{3, 3})
	log.Append(LogEntry{4, 4})
	s := log.Slice(0, 1)
	if len(s) != 1 {
		t.Fatal("slice should be of 1 len instead ", s)
	}
	if s[0].Command != 1 {
		t.Fatal("slice term should be of 1 len instead ", s)
	}
}

func TestMemoryLogAppend(t *testing.T) {
	log := NewMemoryLog()
	log.Append(LogEntry{1, 2})
	if log.Length() != 1 {
		t.Fatal("log length should be 1 instead", log.Length())
	}
	fmt.Printf("silce is %#v\n", log.Slice(0, 1))
	if log.Term(1) != 2 {
		t.Fatal("last log term should be 2 instead", log.Term(0))
	}
	last := log.RemoveLast()
	if last.Term != 2 {
		t.Fatal("last log term should be 2 instead", last.Term)
	}
	if log.Length() != 0 {
		t.Fatal("log should have zero length, instead", log.Length())
	}
}
