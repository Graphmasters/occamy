package occamy

import (
	"context"
	"sync"
	"time"
)

// properties represent the properties of the slot and task
type properties struct {
	state slotStatus // The state of the slot i.e. empty, protected or unprotected.
}

// externalTask represents a request message that was handled via the control
// handler. It is to be kept by the server and added if there is space in the
// expansion process.
type externalTask struct {
	task       Task        // The task
	details    TaskDetails // The task's details
	properties properties  // The task's properties
}

// slot represents a slot for a task in the server.
// Its data should never be manipulated directly.
type slot struct {
	task       Task        // The task assigned to the slot
	details    TaskDetails // The details of the task assigned to the slot set by the handler/task
	properties properties  // The properties of the task and slot set by occamy

	mutex    sync.Mutex // Lock to prevent clashes when accessing/modifying data.
	occupied *semaphore // occupied is activated when a task is set and deactivated when the slot is emptied.
	usable   *semaphore // usable is activated when there a task is set and deactivated when the slot is killed or emptied.
}

// newSlot creates a new empty slot.
func newSlot() *slot {
	slot := &slot{
		properties: properties{
			state: slotStatusEmpty,
		},
		occupied: newSemaphore(false),
		usable:   newSemaphore(false),
	}

	return slot
}

// doTask performs the task.
func (s *slot) doTask() error {
	s.mutex.Lock()
	task := s.task
	s.mutex.Unlock()

	if task == nil {
		return nil
	}

	ctx, cancel := s.newContext()
	defer cancel()
	return task.Do(ctx)
}

// empty removes all pointers and sets slotStatus to empty.
func (s *slot) empty() {
	s.mutex.Lock()
	if s.occupied.isActive() {
		s.task = nil
		s.details = TaskDetails{}
		s.properties = properties{}
		s.properties.state = slotStatusEmpty
		s.usable.deactivate()
		s.occupied.deactivate()
	}
	s.mutex.Unlock()
}

// expand creates additional tasks.
func (s *slot) expand(n int) []Task {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.task == nil {
		return nil
	}

	tasks := s.task.Expand(n)
	if len(tasks) == 0 {
		return nil
	}

	return tasks
}

// getTaskID gets the id of the task.
func (s *slot) getTaskID() string {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.task == nil {
		return ""
	}

	return s.details.ID
}

// getTaskGroup gets the group of the task.
func (s *slot) getTaskGroup() string {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.task == nil {
		return TaskGroupNone
	}

	return s.details.Group
}

func (s *slot) handleControlMsg(headers Headers, body []byte) error {
	s.mutex.Lock()
	if s.task == nil {
		s.mutex.Unlock()
		return nil
	}

	task := s.task
	s.mutex.Unlock()

	ctx, cancel := s.newContext()
	defer cancel()
	return task.Handle(ctx, headers, body)
}

// isEmpty returns if the slotStatus of the task is empty.
func (s *slot) isEmpty() bool {
	s.mutex.Lock()
	result := s.properties.state == slotStatusEmpty
	s.mutex.Unlock()
	return result
}

// isProtected returns if the slotStatus of the task is protected.
// nolint
func (s *slot) isProtected() bool {
	s.mutex.Lock()
	result := s.properties.state == slotStatusProtected
	s.mutex.Unlock()
	return result
}

// kill ends the task.
func (s *slot) kill() {
	s.mutex.Lock()
	s.usable.deactivate()
	s.mutex.Unlock()
}

func (s *slot) newContext() (context.Context, func()) {
	s.mutex.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	stopCh := s.usable.deactivatedCh()
	go func() {
		select {
		case <-ctx.Done():
		case <-stopCh:
			cancel()
		}
	}()
	s.mutex.Unlock()
	return ctx, cancel
}

// setTask sets the task along with the other relevant information.
// This must only ever be done on an empty task.
func (s *slot) setTask(task Task, properties properties) {
	if task == nil {
		return
	}

	s.mutex.Lock()
	s.task = task
	s.details = task.Details()
	s.properties = properties

	s.occupied.activate()
	s.usable.activate()
	s.mutex.Unlock()
}

func (s *slot) state() slotStatus {
	s.mutex.Lock()
	state := s.properties.state
	s.mutex.Unlock()
	return state
}

// waitTillEmpty waits until the slot is empty or until the timeout is reached.
func (s *slot) waitTillEmpty(timeout time.Duration) bool {
	s.mutex.Lock()
	ch := s.occupied.deactivatedCh()
	s.mutex.Unlock()
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}
