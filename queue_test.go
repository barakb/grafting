package grafting

import (
	logger "github.com/Sirupsen/logrus"
	"testing"
	"time"
)

func init() {
	logger.SetLevel(logger.InfoLevel)
}

func TestCloseEmptyQueue(t *testing.T) {
	q := NewBlockingQueue()
	err := q.Close()
	if err != nil {
		t.Errorf("Expected nil instead %#v", err)
	}
	e := q.Enqueue(1)
	if e != QueueClosedError {
		t.Errorf("Expected QueueClosedError instead %#v", e)
	}
	val, e := q.Dequeue(nil)
	if e != QueueClosedError {
		t.Errorf("Expected QueueClosedError instead %#v", e)
	}
	if val != nil {
		t.Errorf("Expected nil value instead %#v", val)
	}
}

func TestCloseQueueWithListener(t *testing.T) {
	q := NewBlockingQueue()
	go func() {
		_, e := q.Dequeue(nil)
		if e != QueueClosedError {
			t.Errorf("Expected QueueClosedError instead %#v", e)
		}
	}()
	time.Sleep(50 * time.Millisecond)
	err := q.Close()
	if err != nil {
		t.Errorf("Expected nil instead %#v", err)
	}
}
func TestDequeueDone(t *testing.T) {
	q := NewBlockingQueue()
	dequeueDone := make(chan struct{})
	go func() {
		_, e := q.Dequeue(dequeueDone)
		if e != DequeueDoneError {
			t.Errorf("Expected DequeueDoneError instead %#v", e)
		}
	}()
	time.Sleep(50 * time.Millisecond)
	close(dequeueDone)
	time.Sleep(50 * time.Millisecond)
	q.Close()
}

func TestEnqueueDequeue(t *testing.T) {
	q := NewBlockingQueue()

	err := q.Enqueue(1)
	if err != nil {
		t.Errorf("Expected nil instead %#v", err)
	}

	val, err := q.Dequeue(nil)
	if err != nil {
		t.Errorf("Expected nil instead %#v", err)
	}
	if 1 != val {
		t.Errorf("Expected 1 instead %#v", val)
	}

	err = q.Close()
	if err != nil {
		t.Errorf("Expected nil instead %#v", err)
	}
}

func TestDequeueEnqueue(t *testing.T) {
	q := NewBlockingQueue()
	go func() {
		v, e := q.Dequeue(nil)
		if e != nil {
			t.Errorf("Expected nil instead %#v", e)
		}
		if v != 1 {
			t.Errorf("Expected 1 instead %#v", v)
		}
	}()
	time.Sleep(50 * time.Millisecond)
	err := q.Enqueue(1)
	if err != nil {
		t.Errorf("Expected nil instead %#v", err)
	}
	err = q.Close()
	if err != nil {
		t.Errorf("Expected nil instead %#v", err)
	}
}
