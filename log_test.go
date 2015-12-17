package grafting

import (
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
	log.Append(LogEntry{Command: 1, Term: 2})
	if term := log.Term(0); term != Term(0) {
		t.Fatal("log index start at 1 so logTerm(0) should returns 0 instead", term)
	}
}

func TestMemoryLogSlice(t *testing.T) {
	log := NewMemoryLog()
	log.Append(LogEntry{Command: 1, Term: 1})
	log.Append(LogEntry{Command: 2, Term: 2})
	log.Append(LogEntry{Command: 3, Term: 3})
	log.Append(LogEntry{Command: 4, Term: 4})
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
	log.Append(LogEntry{Command: 1, Term: 2})
	if log.Length() != 1 {
		t.Fatal("log length should be 1 instead", log.Length())
	}
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

func TestMemoryLogLastRequest(t *testing.T) {
	log := NewMemoryLog()
	uid := newUID()
	clientName := "client1"
	if value, found, _ := log.IsRequestPresent(clientName, (*uid).String()); found {
		t.Fatalf("there should be no last requst for client %s, instead found %#v", clientName, value)
	}
	log.Append(LogEntry{Command: 1, Term: 2, From: clientName, Uid: uid})
	if _, found, _ := log.IsRequestPresent(clientName, (*uid).String()); !found {
		t.Fatalf("there should be a last requst for client %s", clientName)
	}
	log.Commit(log.Slice(0, 1)[0], "foo")
	if value, found, _ := log.IsRequestPresent(clientName, (*uid).String()); !found || value != "foo" {
		t.Fatalf("there should be a last requst for client %s and the value should be %q, instead %#v", clientName, "foo", value)
	}
	for i := 0; i < 5; i++ {
		log.Append(LogEntry{Command: 1, Term: 2, From: clientName, Uid: newUID()})
	}
	if value, found, _ := log.IsRequestPresent(clientName, (*uid).String()); found {
		t.Fatalf("the old request uid=%s for client %s, should be purge by now, instead found %#v", (*uid).String(), clientName, value)
	}

}
