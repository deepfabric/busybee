package core

import (
	"sync"
	"testing"
	"time"

	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/fagongzi/util/protoc"
	"github.com/stretchr/testify/assert"
)

type testWorker struct {
	sync.RWMutex
	stopped bool
	wid     uint64
	events  []metapb.Event
	max     int
	batch   int
	from    uint32
	to      uint32
	handled map[uint32]interface{}
}

func (w *testWorker) onEvent(p uint32, offset uint64, event *metapb.Event) (bool, error) {
	w.Lock()
	defer w.Unlock()

	if len(w.events) >= w.max {
		return false, nil
	}

	if event.User.UserID >= w.from && event.User.UserID < w.to {
		w.events = append(w.events, *event)
	}

	return true, nil
}

func (w *testWorker) stop() {
	w.Lock()
	defer w.Unlock()

	w.stopped = true
}

func (w *testWorker) close() {
	w.Lock()
	defer w.Unlock()

	w.stopped = true
}

func (w *testWorker) workflowID() uint64 {
	return w.wid
}

func (w *testWorker) isStopped() bool {
	w.RLock()
	defer w.RUnlock()

	return w.stopped
}

func (w *testWorker) init() {

}

func (w *testWorker) handleEvent(func(p uint32, offset uint64)) bool {
	w.Lock()
	defer w.Unlock()

	n := len(w.events)
	if n == 0 {
		return false
	}

	if n > w.batch {
		n = w.batch
	}

	for i := 0; i < n; i++ {
		w.handled[w.events[0].User.UserID] = struct{}{}
		w.events = w.events[1:]
	}

	return true
}

func (w *testWorker) cachedEventSize() uint64 {
	return 0
}

func TestOnEvent(t *testing.T) {
	wait = time.Second

	r := newWorkerRunner(1, 0, "11", nil)
	w1 := &testWorker{
		wid:     1,
		max:     50,
		batch:   25,
		from:    1,
		to:      51,
		handled: make(map[uint32]interface{}),
	}
	w2 := &testWorker{
		wid:     2,
		max:     1000,
		batch:   1000,
		from:    51,
		to:      101,
		handled: make(map[uint32]interface{}),
	}
	r.addWorker("w1", w1, true)
	r.addWorker("w2", w2, true)

	go func() {
		hasEvent := true
		for {
			if !hasEvent {
				time.Sleep(time.Millisecond * 10)
			}

			r.RLock()
			for _, w := range r.workers {
				if w.handleEvent(r.completed) {
					hasEvent = true
				}
			}
			r.RUnlock()
		}
	}()

	go func() {
		offset := uint64(0)
		var items [][]byte
		for i := uint32(1); i <= 100; i++ {
			items = append(items, protoc.MustMarshal(&metapb.Event{
				User: &metapb.UserEvent{UserID: i},
			}))
			offset++
		}
		r.onEvent(100, offset, items)
	}()

	time.Sleep(time.Second * 6)
	assert.Equal(t, 50, len(w1.handled), "TestOnEvent failed")
	assert.Equal(t, 50, len(w2.handled), "TestOnEvent failed")
}
