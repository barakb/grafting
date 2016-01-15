package grafting

import (
	"container/list"
	"errors"
	"sync"
)

var QueueClosedError = errors.New("Queue: the queue is closed")

// BlockingQueue as in java BlockingQueue.
// Reader will be block when there are no elements in the queue.
// This can be used instead of channel when there is an enqueue logic that depends on the already enqueued elements.
// For example queue of messages that waiting to be send and an enqueue message can be replaced with an already in queue
// equivalent but older message to prevent queue overflow.
type BlockingQueue interface {
	Enqueue(value interface{}) error
	Dequeue() (interface{}, error)
	Close() error
}

// EnqueueFN holds the logic of how to add new item given queue state.
type EnqueueFN func(newItem interface{}, queuedItems *list.List)

func pushBackEnqueueFN(newItem interface{}, queuedItems *list.List) {
	queuedItems.PushBack(newItem)
}

type blockingQueue struct {
	mutex           *sync.Mutex
	items           *list.List
	dequeueRequests *list.List
	add             EnqueueFN
	done            chan struct{}
}

// Enqueue a value.
// returns QueueClosedError if queue is closed.
func (q blockingQueue) Enqueue(value interface{}) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	select {
	case <-q.done:
		return QueueClosedError
	default:
		if 0 < q.dequeueRequests.Len() {
			front := q.dequeueRequests.Front()
			respChan := q.dequeueRequests.Remove(front).(chan interface{})
			respChan <- value
			close(respChan)
		} else {
			q.items.PushBack(value)
		}
		return nil
	}
}

// Dequeue a value, will block until there is a value.
// returns QueueClosedError if queue is closed.
func (q blockingQueue) Dequeue() (interface{}, error) {
	var respChan chan interface{}
	q.mutex.Lock()
	select {
	case <-q.done:
		q.mutex.Unlock()
		return nil, QueueClosedError
	default:
		respChan = make(chan interface{}, 1)
		if 0 < q.items.Len() {
			respChan <- q.items.Remove(q.items.Front())
			close(respChan)
		} else {
			q.dequeueRequests.PushBack(respChan)
		}
		q.mutex.Unlock()
	}

	select {
	case <-q.done:
		return nil, QueueClosedError
	case resp := <-respChan:
		if resp == nil {
			return nil, QueueClosedError
		}
		return resp, nil
	}
}

// Close this queue, release all waiting threads with QueueClosedError
func (q blockingQueue) Close() error {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	close(q.done)
	var next *list.Element
	for e := q.dequeueRequests.Front(); e != nil; e = next {
		next = e.Next()
		close(q.dequeueRequests.Remove(e).(chan interface{}))
	}
	return nil
}

// Create blocking queue with custom add function
func NewBlockingQueueFN(add EnqueueFN) BlockingQueue {
	return &blockingQueue{&sync.Mutex{}, list.New(), list.New(), add, make(chan struct{})}
}

// Create a blocking queue with the default pushBackEnqueueFN add function
func NewBlockingQueue() BlockingQueue {
	return NewBlockingQueueFN(pushBackEnqueueFN)
}
