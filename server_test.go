package occamy_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/Graphmasters/occamy"
)

/*
Many tests in this file overlap in what they are testing. This is because the
server is stateful and testing one particular method usually requires other
calls to ensure the server is in the desired state.
*/

const (
	ShortDuration = 10 * time.Millisecond
)

func TestServer_HandleRequestMsg_Ack(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(StandardHandlerID, handler.Handle)
	mf := NewMessageFactory(StandardHandlerID)

	msg := mf.NewSimpleTaskMessage(false)
	server.HandleRequestMsg(msg)
	assertErrorIsNil(t, monitors.Error.nextError(), "error after adding first task")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources-1, 1, 0, 0, "resources after adding single task")

	controller.stop(msg.id, nil)
	time.Sleep(ShortDuration)
	assertErrorIsNil(t, monitors.Error.nextError(), "error after the task stopped")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources, 0, 0, 0, "resources after server shutdown")
	assertStringsEqual(t, MessageStatusAcked, msg.status, "")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

func TestServer_HandleRequestMsg_Requeue(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(StandardHandlerID, handler.Handle)
	mf := NewMessageFactory(StandardHandlerID)

	msg := mf.NewSimpleTaskMessage(false)
	server.HandleRequestMsg(msg)
	assertErrorIsNil(t, monitors.Error.nextError(), "error after adding first task")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources-1, 1, 0, 0, "resources after adding single task")
	assertStringsEqual(t, MessageStatusRequeued, msg.status, "")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

func TestServer_HandleRequestMsg_Reject(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(StandardHandlerID, handler.Handle)
	mf := NewMessageFactory(StandardHandlerID)

	msg := mf.NewSimpleTaskMessage(false)
	server.HandleRequestMsg(msg)
	assertErrorIsNil(t, monitors.Error.nextError(), "error after adding first task")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources-1, 1, 0, 0, "resources after adding single task")

	controller.stop(msg.id, occamy.ErrInvalidTask)
	time.Sleep(ShortDuration)
	assertErrorEqual(t, occamy.ErrInvalidTask, monitors.Error.nextError(), "no error after the task stopped")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources, 0, 0, 0, "resources after server shutdown")
	assertStringsEqual(t, MessageStatusRejected, msg.status, "")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

func TestServer_HandleRequestMsg_MultipleMessage(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(StandardHandlerID, handler.Handle)
	mf := NewMessageFactory(StandardHandlerID)

	msgs := mf.NewSimpleTaskMessages(6, true)
	handleRequestMessages(server, msgs)

	assertErrorIsNil(t, monitors.Error.nextError(), "errors after multiple messages")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources-6, 6, 0, 0, "resources after multiple message")

	controller.stop(msgs[0].id, nil)
	controller.stop(msgs[1].id, nil)
	time.Sleep(ShortDuration)
	assertErrorIsNil(t, monitors.Error.nextError(), "errors after stopping tasks")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources-4, 4, 0, 0, "resources after stopping tasks")

	controller.stop(msgs[2].id, occamy.ErrInvalidTask)
	controller.stop(msgs[3].id, occamy.ErrInvalidTask)
	time.Sleep(ShortDuration)
	assertErrorMonitorErrorCount(t, monitors.Error, 2, "check errors after tasks throwing errors")
	assertErrorIsOccamyError(t, occamy.ErrInvalidTask, monitors.Error.nextError(), "checking error after tasks throwing error (1)")
	assertErrorIsOccamyError(t, occamy.ErrInvalidTask, monitors.Error.nextError(), "checking error after tasks throwing error (2)")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources-2, 2, 0, 0, "resources after stopping tasks")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

func TestServer_HandleRequestMsg_OverloadServer(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(StandardHandlerID, handler.Handle)
	mf := NewMessageFactory(StandardHandlerID)

	msgs := mf.NewSimpleTaskMessages(DefaultResources+1, true)
	handleRequestMessages(server, msgs)
	assertErrorIsNotNil(t, monitors.Error.nextError(), "handling more requests than possible should trigger error")
	assertResourceMonitorStatusMatch(t, monitors.Resource, 0, DefaultResources, 0, 0, "resources after overloading")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

func TestServer_HandleRequestMsg_OverloadServerTwice(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(StandardHandlerID, handler.Handle)
	mf := NewMessageFactory(StandardHandlerID)

	msgs := mf.NewSimpleTaskMessages(DefaultResources+1, true)
	handleRequestMessages(server, msgs)
	assertErrorMonitorErrorCount(t, monitors.Error, 1, "handling more requests than possible should trigger error")
	assertErrorIsNotNil(t, monitors.Error.nextError(), "handling more requests than possible should trigger error")
	assertResourceMonitorStatusMatch(t, monitors.Resource, 0, DefaultResources, 0, 0, "resources after overloading")

	for _, msg := range msgs[:4] {
		controller.stop(msg.id, nil)
	}
	time.Sleep(ShortDuration)
	assertResourceMonitorStatusMatch(t, monitors.Resource, 4, DefaultResources-4, 0, 0, "resources after stopping some task")

	handleRequestMessages(server, mf.NewSimpleTaskMessages(5, true))
	assertErrorMonitorErrorCount(t, monitors.Error, 1, "handling more requests than possible should trigger error (second attempt)")
	assertErrorIsNotNil(t, monitors.Error.nextError(), "handling more requests than possible should trigger error (second attempt)")
	assertResourceMonitorStatusMatch(t, monitors.Resource, 0, DefaultResources, 0, 0, "resources after overloading (second attempt)")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_Shutdown tests that the shutdown can be successfully called and
// yield no errors.
func TestServer_Shutdown(t *testing.T) {
	server, monitors := NewServer(StandardHandlerID, nil)
	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_Shutdown_idempotent tests that shutdown can be successfully called
// multiple times.
func TestServer_Shutdown_idempotent(t *testing.T) {
	server, monitors := NewServer(StandardHandlerID, nil)
	testServerShutdownSuccess(t, server, monitors, DefaultResources)
	testServerShutdownSuccess(t, server, monitors, DefaultResources)
	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_Shutdown_idempotent_withTasks tests that shutdown can be
// successfully called multiple times when the server contains tasks.
func TestServer_Shutdown_idempotent_withTasks(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(StandardHandlerID, handler.Handle)
	mf := NewMessageFactory(StandardHandlerID)

	messages := mf.NewSimpleTaskMessages(4, false)
	handleRequestMessages(server, messages)
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources-4, 4, 0, 0, "resources after adding tasks")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_Shutdown_withTasks tests that shutdown empties all slots without
// any errors when the server contains tasks.
func TestServer_Shutdown_withTasks(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(StandardHandlerID, handler.Handle)
	mf := NewMessageFactory(StandardHandlerID)

	messages := mf.NewSimpleTaskMessages(4, false)
	handleRequestMessages(server, messages)
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources-4, 4, 0, 0, "resources after adding tasks")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_Shutdown_withUnstoppableTask tests that shutdown will record
// errors when the server contains unstoppable tasks.
func TestServer_Shutdown_withUnstoppableTask(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(StandardHandlerID, handler.Handle)
	mf := NewMessageFactory(StandardHandlerID)

	msg := mf.NewUnstoppableTaskMessage(false)
	server.HandleRequestMsg(msg)
	assertErrorIsNil(t, monitors.Error.nextError(), "error after adding first task")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources-1, 1, 0, 0, "resources after adding single task")

	shutdownServer(server)
	assertErrorIsOccamyError(t, occamy.ErrTaskNotKilled, monitors.Error.nextError(), "no error after shutdown")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources-1, 1, 0, 0, "resources after server shutdown")
}

// region Asserts

func assertErrorEqual(t *testing.T, expected, actual error, comment string) {
	if errors.Is(actual, expected) {
		return
	}

	t.Log(string(debug.Stack()))
	t.Logf("%s: actual error (%v) did not match expected (%v)", comment, actual, expected)
	t.FailNow()
}

func assertErrorIsNil(t *testing.T, err error, comment string) {
	if err == nil {
		return
	}

	t.Logf("%s: error was not nil: %v", comment, err)
	t.FailNow()
}

func assertErrorIsOccamyError(t *testing.T, expected occamy.BasicError, actual error, comment string) {
	if actual == nil {
		t.Logf("%s: error was nil", comment)
		t.FailNow()
	}

	switch explicitErr := actual.(type) {
	case occamy.BasicError:
		assertErrorEqual(t, expected, explicitErr, fmt.Sprintf("%s: error was a simple error", comment))
	case *occamy.DetailedError:
		assertErrorEqual(t, expected, explicitErr, fmt.Sprintf("%s: error was a detailed error but the simple part didn't match", comment))
	case *occamy.WrappedError:
		assertErrorEqual(t, expected, explicitErr, fmt.Sprintf("%s: error was a wrapped error but the simple part didn't match", comment))
	default:
		t.Logf("%s: error was not any known occamy error: %v", comment, actual)
		t.FailNow()
	}
}

func assertErrorIsNotNil(t *testing.T, err error, comment string) {
	if err != nil {
		return
	}

	t.Logf("%s: error was nil", comment)
	t.FailNow()
}

func assertErrorMonitorErrorCount(t *testing.T, em *ErrorMonitor, expected int, comment string) {
	if len(em.errors) == expected {
		return
	}

	t.Logf("%s: error monitor contains %d errors which does not match the expected amount of %d", comment, len(em.errors), expected)
	t.Log(string(debug.Stack()))
	t.FailNow()
}

func assertIntsEqual(t *testing.T, expected, actual int, comment string) {
	if expected == actual {
		return
	}

	t.Logf("%s: actual value (%d) does not match expected value %d", comment, actual, expected)
	t.Log(string(debug.Stack()))
	t.FailNow()
}

func assertStringsEqual(t *testing.T, expected, actual string, comment string) {
	if expected == actual {
		return
	}

	t.Logf("%s: actual value (%s) does not match expected value %s", comment, actual, expected)
	t.FailNow()
}

func assertResourceMonitorStatusMatch(t *testing.T, rm *ResourceMonitor, empty, protected, unprotectedInternal, unprotectedExternal int, comment string) {
	assertIntsEqual(t, empty, rm.statuses[occamy.SlotStatusEmpty], fmt.Sprintf("%s: mismatch in empty slots", comment))
	assertIntsEqual(t, protected, rm.statuses[occamy.SlotStatusProtected], fmt.Sprintf("%s: mismatch in protected slots", comment))
	assertIntsEqual(t, unprotectedInternal, rm.statuses[occamy.SlotStatusUnprotectedInternal], fmt.Sprintf("%s: mismatch in unprotected internal slots", comment))
	assertIntsEqual(t, unprotectedExternal, rm.statuses[occamy.SlotStatusUnprotectedExternal], fmt.Sprintf("%s: mismatch in unprotected external slots", comment))

}

// endregion

// region Handler - Standard

const StandardHandlerID = "standard_handler"

type StandardHandler struct {
	controller *TaskController
}

func NewStandardHandler(controller *TaskController) *StandardHandler {
	return &StandardHandler{controller: controller}
}

func (sh *StandardHandler) Handle(header occamy.Headers, body []byte) (occamy.Task, error) {
	data := &MessageData{}
	if err := json.Unmarshal(body, data); err != nil {
		return nil, &occamy.WrappedError{
			BasicErr: occamy.ErrInvalidBody,
			InnerErr: err,
		}
	}

	switch data.TaskGroup {
	case TaskGroupSimple:
		return NewSimpleTask(data.ID, data.Expandable, sh.controller), nil
	case TaskGroupUnstoppable:
		return NewUnstoppableTask(data.ID, data.Expandable), nil
	default:
		return nil, &occamy.DetailedError{
			BasicErr: occamy.ErrInvalidBody,
			Cause:    fmt.Sprintf("unknown task group: %s", data.TaskGroup),
		}
	}
}

// endregion Handler

// region Header Keys

const (
	HeaderKeyTaskID    = "task_id"
	HeaderKeyHandlerID = "handler_id"
)

// endregion

// region Message

type Message struct {
	id string

	headers occamy.Headers
	body    []byte

	mutex  *sync.Mutex
	status string
}

func (m *Message) Body() []byte {
	return m.body
}

func (m *Message) Headers() occamy.Headers {
	return m.headers
}

func (m *Message) Ack() error {
	m.setStatus(MessageStatusAcked)
	return nil
}

func (m *Message) Reject(requeue bool) error {
	if requeue {
		m.setStatus(MessageStatusRequeued)
		return nil
	}

	m.setStatus(MessageStatusRejected)
	return nil
}

func (m *Message) setStatus(status string) {
	m.mutex.Lock()
	m.status = status
	m.mutex.Unlock()
}

// endregion

// region Message Data

type MessageData struct {
	ID         string
	Expandable bool
	TaskGroup  string
}

// endregion

// region Message Factory

type MessageFactory struct {
	count   int
	handler string
	mutex   *sync.Mutex
}

func NewMessageFactory(handlerID string) *MessageFactory {
	return &MessageFactory{
		count:   0,
		handler: handlerID,
		mutex:   &sync.Mutex{},
	}
}

func (mf *MessageFactory) NewSimpleTaskMessage(expandable bool) *Message {
	return mf.convertToMessage(MessageData{
		ID:         mf.nextID(),
		Expandable: expandable,
		TaskGroup:  TaskGroupSimple,
	})
}

func (mf *MessageFactory) NewSimpleTaskMessages(n int, expandable bool) []*Message {
	messages := make([]*Message, n)
	for i := range messages {
		messages[i] = mf.NewSimpleTaskMessage(expandable)
	}

	return messages
}

func (mf *MessageFactory) NewUnstoppableTaskMessage(expandable bool) *Message {
	return mf.convertToMessage(MessageData{
		ID:         mf.nextID(),
		Expandable: expandable,
		TaskGroup:  TaskGroupUnstoppable,
	})
}

func (mf *MessageFactory) convertToMessage(data MessageData) *Message {
	headers := make(occamy.Headers)
	headers[HeaderKeyHandlerID] = mf.handler

	body, err := json.Marshal(data)
	if err != nil {
		panic(fmt.Sprintf("unable to continue test as message data could not be marshaled into the body of a message: %v", err))
	}

	return &Message{
		id:      data.ID,
		headers: nil,
		body:    body,
		status:  MessageStatusOpen,
		mutex:   &sync.Mutex{},
	}
}

func (mf *MessageFactory) nextID() string {
	mf.mutex.Lock()
	id := fmt.Sprintf("%03d", mf.count)
	mf.count++
	mf.mutex.Unlock()
	return id
}

// endregion

// region Message Status

const (
	MessageStatusOpen     = "open"
	MessageStatusAcked    = "acked"
	MessageStatusRejected = "rejected"
	MessageStatusRequeued = "requeued"
)

// endregion

// region Task Controller

type TaskController struct {
	errors    map[string]error
	stopChs   map[string]chan struct{}
	stopOnces map[string]*sync.Once
	mutex     *sync.Mutex
}

func NewTaskController() *TaskController {
	return &TaskController{
		errors:    map[string]error{},
		stopChs:   map[string]chan struct{}{},
		stopOnces: map[string]*sync.Once{},
		mutex:     &sync.Mutex{},
	}
}

func (tc *TaskController) register(id string) {
	tc.mutex.Lock()
	if _, ok := tc.stopChs[id]; !ok {
		tc.stopChs[id] = make(chan struct{})
		tc.stopOnces[id] = &sync.Once{}
	}
	tc.mutex.Unlock()
}

func (tc *TaskController) stop(id string, err error) {
	tc.mutex.Lock()
	tc.stopOnces[id].Do(func() {
		tc.errors[id] = err
		close(tc.stopChs[id])
	})
	tc.mutex.Unlock()
}

func (tc *TaskController) error(id string) error {
	tc.mutex.Lock()
	err := tc.errors[id]
	tc.mutex.Unlock()
	return err
}

func (tc *TaskController) stopCh(id string) <-chan struct{} {
	tc.mutex.Lock()
	ch := tc.stopChs[id]
	tc.mutex.Unlock()
	return ch
}

// endregion

// region Task - BasicErr

const TaskGroupSimple = "simple"

type SimpleTask struct {
	id         string
	expandable bool
	controller *TaskController

	stopCh   chan struct{}
	stopOnce *sync.Once
}

func NewSimpleTask(id string, expandable bool, controller *TaskController) *SimpleTask {
	controller.register(id)
	return &SimpleTask{
		id:         id,
		expandable: expandable,
		controller: controller,
		stopCh:     make(chan struct{}),
		stopOnce:   &sync.Once{},
	}
}

func (task *SimpleTask) Do(ctx context.Context) error {
	select {
	case <-task.stopCh:
		return nil
	case <-task.controller.stopCh(task.id):
		return task.controller.error(task.id)
	case <-ctx.Done():
		return occamy.ErrTaskInterrupted
	}
}

func (task *SimpleTask) Details() occamy.TaskDetails {
	return occamy.TaskDetails{
		Deadline: time.Now().Add(1000000 * time.Hour),
		ID:       task.id,
		Group:    TaskGroupUnstoppable,
	}
}

func (task *SimpleTask) Expand(n int) []occamy.Task {
	if !task.expandable {
		return nil
	}

	tasks := make([]occamy.Task, n)
	for i := range tasks {
		tasks[i] = NewSimpleTask(task.id, task.expandable, task.controller)
	}

	return tasks
}

func (task *SimpleTask) Handle(_ context.Context, _ occamy.Headers, _ []byte) error {
	return nil
}

// endregion

// region Task - Unstoppable

const TaskGroupUnstoppable = "unstoppable"

type UnstoppableTask struct {
	id         string
	expandable bool
}

func NewUnstoppableTask(id string, expandable bool) UnstoppableTask {
	return UnstoppableTask{
		id:         id,
		expandable: expandable,
	}
}

func (task UnstoppableTask) Do(_ context.Context) error {
	select {}
}

func (task UnstoppableTask) Details() occamy.TaskDetails {
	return occamy.TaskDetails{
		Deadline: time.Now().Add(1000000 * time.Hour),
		ID:       task.id,
		Group:    TaskGroupUnstoppable,
	}
}

func (task UnstoppableTask) Expand(n int) []occamy.Task {
	if !task.expandable {
		return nil
	}

	tasks := make([]occamy.Task, n)
	for i := range tasks {
		tasks[i] = UnstoppableTask{
			id:         task.id,
			expandable: true,
		}
	}

	return tasks
}

func (task UnstoppableTask) Handle(_ context.Context, _ occamy.Headers, _ []byte) error {
	return nil
}

// endregion

// region Server Setup

const (
	DefaultResources = 8
)

func NewServer(handlerID string, handler occamy.Handler) (*occamy.Server, Monitors) {
	monitors := NewMonitors(DefaultResources)
	server := occamy.NewServer(occamy.ServerConfig{
		Slots:               DefaultResources,
		ExpansionSlotBuffer: 0,
		ExpansionPeriod:     0,
		KillTimeout:         100 * time.Millisecond,
		HeaderKeyTaskID:     HeaderKeyTaskID,
		HeaderKeyHandlerID:  HeaderKeyHandlerID,
		HandlerID:           handlerID,
		Handler:             handler,
		Monitors: occamy.Monitors{
			Error:    monitors.Error,
			Latency:  monitors.Latency,
			Resource: monitors.Resource,
		},
	})

	return server, monitors
}

// endregion

// region Server Shutdown

// shutdownServer shuts down the server.
func shutdownServer(server *occamy.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	server.Shutdown(ctx)
	cancel()
}

// testServerShutdownSuccess shuts the server down is a success, that is, there
// are no errors (the error monitor should contain no errors before shutdown)
// and that there are only empty slots remaining.
func testServerShutdownSuccess(t *testing.T, server *occamy.Server, monitors Monitors, resources int) {
	shutdownServer(server)
	assertErrorIsNil(t, monitors.Error.nextError(), "error after shutdown")
	assertResourceMonitorStatusMatch(t, monitors.Resource, resources, 0, 0, 0, "resources after server shutdown")
}

// endregion

// region Server Handler Request Message

func handleRequestMessages(server *occamy.Server, messages []*Message) {
	for i := range messages {
		server.HandleRequestMsg(messages[i])
	}
}

// endregion

// region Monitors

type Monitors struct {
	Error    *ErrorMonitor
	Latency  occamy.NopLatencyMonitor
	Resource *ResourceMonitor
}

func NewMonitors(resources int) Monitors {
	return Monitors{
		Error:    &ErrorMonitor{},
		Latency:  occamy.NopLatencyMonitor{},
		Resource: NewResourceMonitor(resources),
	}
}

// endregion

// region Monitor - Error

type ErrorMonitor struct {
	errors []error
	mutex  sync.Mutex
}

func (em *ErrorMonitor) RecordError(err error) {
	em.mutex.Lock()
	em.errors = append(em.errors, err)
	em.mutex.Unlock()
}

func (em *ErrorMonitor) nextError() error {
	var err error
	em.mutex.Lock()
	if len(em.errors) > 0 {
		err = em.errors[0]
		em.errors = em.errors[1:]
	}
	em.mutex.Unlock()
	return err
}

// endregion

// region Monitor - Resource

type ResourceMonitor struct {
	handlers map[string]int
	groups   map[string]int
	statuses map[occamy.SlotStatus]int

	mutex *sync.Mutex
}

func NewResourceMonitor(resources int) *ResourceMonitor {
	rm := &ResourceMonitor{
		handlers: make(map[string]int),
		groups:   make(map[string]int),
		statuses: make(map[occamy.SlotStatus]int),
		mutex:    &sync.Mutex{},
	}

	rm.statuses[occamy.SlotStatusEmpty] += resources
	return rm
}

func (rm *ResourceMonitor) RecordTaskStarting(handler string, group string, status occamy.SlotStatus) {
	rm.mutex.Lock()
	rm.handlers[handler]++
	rm.groups[group]++
	rm.statuses[status]++
	rm.statuses[occamy.SlotStatusEmpty]--
	rm.mutex.Unlock()
}

func (rm *ResourceMonitor) RecordTaskStopping(handler string, group string, status occamy.SlotStatus) {
	rm.mutex.Lock()
	rm.handlers[handler]--
	rm.groups[group]--
	rm.statuses[status]--
	rm.statuses[occamy.SlotStatusEmpty]++
	rm.mutex.Unlock()
}

// endregion
