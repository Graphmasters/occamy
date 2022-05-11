package occamy

import (
	"sync/atomic"
)

// region Atomic Int

type atomicInt struct {
	value *int32
}

func newAtomicInt(initial int) atomicInt {
	i := atomicInt{value: new(int32)}
	atomic.AddInt32(i.value, int32(initial))
	return i
}

func (i *atomicInt) decrease() {
	atomic.AddInt32(i.value, -1)
}

func (i *atomicInt) increase() {
	atomic.AddInt32(i.value, 1)
}

func (i *atomicInt) isEqual(other int) bool {
	return atomic.LoadInt32(i.value) == int32(other)
}

func (i *atomicInt) load() int {
	value := atomic.LoadInt32(i.value)
	return int(value)
}

// endregion

// region Semaphore

// semaphore is used to record a state. It can either be in the activated or
// deactivated state. It is NOT thread safe. It is up to the user to ensure that
// data is never modified or read simultaneously.
type semaphore struct {
	ch    chan struct{} // ch is closed when deactivated and not closed when activated
	state int32         // state is 0 when deactivated and 1 when activated
}

func newSemaphore(activated bool) *semaphore {
	semaphore := &semaphore{
		ch:    make(chan struct{}),
		state: 1,
	}

	if !activated {
		semaphore.deactivate()
	}

	return semaphore
}

func (s *semaphore) activate() {
	if atomic.LoadInt32(&s.state) == 0 {
		s.ch = make(chan struct{})
		atomic.StoreInt32(&s.state, 1)
	}
}

func (s *semaphore) deactivate() {
	if atomic.LoadInt32(&s.state) == 1 {
		close(s.ch)
		atomic.StoreInt32(&s.state, 0)
	}
}

func (s *semaphore) isActive() bool {
	return atomic.LoadInt32(&s.state) == 1
}

func (s *semaphore) deactivatedCh() <-chan struct{} {
	return s.ch
}

// endregion
