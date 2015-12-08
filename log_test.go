package go_rafting

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

func TestMemoryLogAppend(t *testing.T) {
	log := NewMemoryLog()
	log.Append(LogEntry{1, 2})
	if log.Length() != 1 {
		t.Fatal("log length should be 1 instead", log.Length())
	}
	fmt.Printf("silce is %#v\n", log.Slice(0, 1))
	if log.Term(0) != 2 {
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
