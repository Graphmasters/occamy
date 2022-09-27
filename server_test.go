package occamy_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Graphmasters/occamy"
)

/*
Many tests in this file overlap in what they are testing. This is because the
server is stateful and testing one particular method usually requires other
calls to ensure the server is in the desired state.

The tests have been written so the tests should fail if the occamy package
has been incorrectly implemented and panic if the test itself has been
incorrectly implemented.
*/

const (
	ShortDuration = 10 * time.Millisecond
)

// TestServer_ExpandTasks tests that the expand method can be called without error.
func TestServer_ExpandTasks(t *testing.T) {
	server, monitors := NewServer(nil)

	server.ExpandTasks()
	assertErrorIsNil(t, monitors.Error.nextError(), "error after expand")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources, 0, 0, 0, "resources after expand")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_ExpandTasks_expandableExternalTask tests that calling ExpandTasks while
// there is an "external" task will be added to a slot and after a second call
// of ExpandTasks will use all available resources.
func TestServer_ExpandTasks_expandableExternalTask(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	msg := mf.NewSimpleTaskMessage(true)
	server.HandleControlMsg(msg)
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources, 0, 0, 0, "resources after request message in control handler")

	time.Sleep(ShortDuration)
	server.ExpandTasks()
	assertErrorIsNil(t, monitors.Error.nextError(), "error after expand")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources-1, 0, 0, 1, "resources after expand")

	server.ExpandTasks()
	assertErrorIsNil(t, monitors.Error.nextError(), "error after second expand")
	assertResourceMonitorStatusMatch(t, monitors.Resource, 0, 0, 0, DefaultResources, "resources after second expand")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_Expand_expandableExternalTask tests that calling ExpandTasks while
// there is a protected expandable task will use all available resources.
func TestServer_ExpandTasks_expandableProtectedTask(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	msg := mf.NewSimpleTaskMessage(true)
	testHandleRequestMessageSuccess(t, server, monitors, msg, "")

	server.ExpandTasks()
	assertErrorIsNil(t, monitors.Error.nextError(), "error after expand")
	assertResourceMonitorStatusMatch(t, monitors.Resource, 0, 1, DefaultResources-1, 0, "resources after expand")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_ExpandTasks_expansionBuffer tests that the ExpandTasks method will respect
// the expansion buffer.
func TestServer_ExpandTasks_expansionBuffer(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	monitors := NewMonitors(DefaultResources)
	server := occamy.NewServer(occamy.ServerConfig{
		Slots:               DefaultResources,
		ExpansionSlotBuffer: 2,
		ExpansionPeriod:     0,
		KillTimeout:         100 * time.Millisecond,
		HeaderKeyTaskID:     HeaderKeyTaskID,
		Handler:             handler.Handle,
		Monitors: occamy.Monitors{
			Error:    monitors.Error,
			Latency:  monitors.Latency,
			Resource: monitors.Resource,
		},
	})
	mf := NewMessageFactory()

	msg := mf.NewSimpleTaskMessage(true)
	testHandleRequestMessageSuccess(t, server, monitors, msg, "")

	server.ExpandTasks()
	assertErrorIsNil(t, monitors.Error.nextError(), "error after expand")
	assertResourceMonitorStatusMatch(t, monitors.Resource, 2, 1, DefaultResources-3, 0, "resources after expand")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_ExpandTasks_unexpandableProtectedTask tests that ExpandTasks will do nothing
// when there is a task that can not be expanded.
func TestServer_ExpandTasks_unexpandableProtectedTask(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	msg := mf.NewSimpleTaskMessage(false)
	testHandleRequestMessageSuccess(t, server, monitors, msg, "")

	server.ExpandTasks()
	assertErrorIsNil(t, monitors.Error.nextError(), "error after expand")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources-1, 1, 0, 0, "resources after expand")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_HandleControlMsg tests that a control message can be handled.
func TestServer_HandleControlMsg(t *testing.T) {
	server, monitors := NewServer(nil)
	mf := NewMessageFactory()

	msg := mf.NewCancelMessage("non-existent")
	server.HandleControlMsg(msg)
	assertErrorIsNil(t, monitors.Error.nextError(), "unexpected error after control message")
	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_HandleControlMsg_cancelMultipleTasksWithSameID tests that if
// multiple task with the same ID are running the cancel message will be handled
// by all of them. This checks that the control message goes to multiple tasks
// with matching task IDs.
func TestServer_HandleControlMsg_cancelMultipleTasksWithSameID(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	msg := mf.NewSimpleTaskMessage(false)
	testHandleRequestMessageSuccess(t, server, monitors, msg, "handling single message")
	testHandleRequestMessageSuccess(t, server, monitors, msg, "handling single message")

	cancel := mf.NewCancelMessage(msg.id)
	server.HandleControlMsg(cancel)
	time.Sleep(ShortDuration)

	assertErrorIsNil(t, monitors.Error.nextError(), "unexpected error after control message")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources, 0, 0, 0, "resources after server shutdown")
	panicIfBoolIsFalse(controller.isStopped(msg.id), "task was should have been cancelled via a message not using the controller")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_HandleControlMsg_cancelTask tests that if a task is running it can
// be cancelled via a control message.
func TestServer_HandleControlMsg_cancelTask(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	msg := mf.NewSimpleTaskMessage(false)
	testHandleRequestMessageSuccess(t, server, monitors, msg, "handling single message")

	cancel := mf.NewCancelMessage(msg.id)
	server.HandleControlMsg(cancel)
	time.Sleep(ShortDuration)

	assertErrorIsNil(t, monitors.Error.nextError(), "unexpected error after control message")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources, 0, 0, 0, "resources after server shutdown")
	panicIfBoolIsFalse(controller.isStopped(msg.id), "task was should have been cancelled via a message not using the controller")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_HandleControlMsg_cancelTaskOneOfMultipleTasks tests that a that if
// multiple tasks are running one of them can be cancelled. This checks that
// the control message is only handled by the task with the task with the
// matching task ID.
func TestServer_HandleControlMsg_cancelTaskOneOfMultipleTasks(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	msgs := mf.NewSimpleTaskMessages(4, false)
	testHandleRequestMessagesSuccess(t, server, monitors, msgs, "handling single message")

	cancel := mf.NewCancelMessage(msgs[2].id)
	server.HandleControlMsg(cancel)
	time.Sleep(ShortDuration)

	assertErrorIsNil(t, monitors.Error.nextError(), "unexpected error after control message")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources-3, 3, 0, 0, "resources after server shutdown")
	assertMessageStatus(t, MessageStatusOpen, msgs[0], "message for running task")
	assertMessageStatus(t, MessageStatusOpen, msgs[1], "message for running task")
	assertMessageStatus(t, MessageStatusAcked, msgs[2], "message for cancelled task")
	assertMessageStatus(t, MessageStatusOpen, msgs[3], "message for running task")
	panicIfBoolIsFalse(controller.isStopped(msgs[2].id), "task was should have been cancelled via a message not using the controller")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_HandleControlMsg_requestMessage tests if a request message can
// be handled as a control message without leading to an error.
func TestServer_HandleControlMsg_requestMessage(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	msg := mf.NewSimpleTaskMessage(false)
	server.HandleControlMsg(msg)
	assertErrorIsNil(t, monitors.Error.nextError(), "unexpected error after request message handled as a control message")
	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_HandleRequestMsg tests that a message can be handled.
func TestServer_HandleRequestMsg(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	msg := mf.NewSimpleTaskMessage(false)
	testHandleRequestMessageSuccess(t, server, monitors, msg, "handling single message")
	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_HandleRequestMsg_messageAck tests that a messages can be handled
// and be an "acked"/"acknowledged".
func TestServer_HandleRequestMsg_messageAck(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	msg := mf.NewSimpleTaskMessage(false)
	testHandleRequestMessageSuccess(t, server, monitors, msg, "handling single message")

	controller.stop(msg.id, nil)
	time.Sleep(ShortDuration)
	assertErrorIsNil(t, monitors.Error.nextError(), "error after the task stopped")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources, 0, 0, 0, "resources after server shutdown")
	assertMessageStatus(t, MessageStatusAcked, msg, "message ack test")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_HandleRequestMsg_messageRequeue tests that a messages can be
// handled and be "requeued".
func TestServer_HandleRequestMsg_messageRequeue(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	msg := mf.NewSimpleTaskMessage(false)
	testHandleRequestMessageSuccess(t, server, monitors, msg, "handling single message")
	testServerShutdownSuccess(t, server, monitors, DefaultResources)
	assertMessageStatus(t, MessageStatusRequeued, msg, "message requeue test") // The message gets requeued after shutdown.
}

// TestServer_HandleRequestMsg_messageReject tests that a messages can be
// handled and be "rejected".
func TestServer_HandleRequestMsg_messageReject(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	msg := mf.NewSimpleTaskMessage(false)
	testHandleRequestMessageSuccess(t, server, monitors, msg, "handling single message")

	err := fmt.Errorf("error for TestServer_HandleRequestMsg_messageReject")
	controller.stop(msg.id, err)
	time.Sleep(ShortDuration)
	assertErrorEqual(t, err, monitors.Error.nextError(), "no error after the task stopped")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources, 0, 0, 0, "resources after server shutdown")
	assertMessageStatus(t, MessageStatusRejected, msg, "message reject test")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_HandleRequestMsg_multipleMessage tests that the server can
// successfully handle multiple messages.
func TestServer_HandleRequestMsg_multipleMessage(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	msgs := mf.NewSimpleTaskMessages(6, true)
	testHandleRequestMessagesSuccess(t, server, monitors, msgs, "handling multiple messages")

	controller.stop(msgs[0].id, nil)
	controller.stop(msgs[1].id, nil)
	time.Sleep(ShortDuration)
	assertErrorIsNil(t, monitors.Error.nextError(), "errors after stopping tasks")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources-4, 4, 0, 0, "resources after stopping tasks")

	controller.stop(msgs[2].id, occamy.NewError(fmt.Errorf("stopping msg 2"), occamy.ErrKindInvalidTask))
	controller.stop(msgs[3].id, occamy.NewError(fmt.Errorf("stopping msg 3"), occamy.ErrKindInvalidTask))
	time.Sleep(ShortDuration)
	assertErrorMonitorErrorCount(t, monitors.Error, 2, "check errors after tasks throwing errors")
	assertErrorIsOccamyError(t, occamy.ErrKindInvalidTask, monitors.Error.nextError(), "checking error after tasks throwing error (1)")
	assertErrorIsOccamyError(t, occamy.ErrKindInvalidTask, monitors.Error.nextError(), "checking error after tasks throwing error (2)")
	assertResourceMonitorStatusMatch(t, monitors.Resource, DefaultResources-2, 2, 0, 0, "resources after stopping tasks")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_HandleRequestMsg_overloadServer tests that the server will
// requeue messages if it is overloaded.
func TestServer_HandleRequestMsg_overloadServer(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	msgs := mf.NewSimpleTaskMessages(DefaultResources+1, true)
	handleRequestMessages(server, msgs)
	assertErrorIsNotNil(t, monitors.Error.nextError(), "handling more requests than possible should trigger error")
	assertResourceMonitorStatusMatch(t, monitors.Resource, 0, DefaultResources, 0, 0, "resources after overloading")
	assertMessageStatus(t, MessageStatusRequeued, msgs[DefaultResources], "overloaded server")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_HandleRequestMsg_overloadServer tests that the server will
// requeue messages if it is overloaded. The overloading is done twice.
func TestServer_HandleRequestMsg_overloadServerTwice(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

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

// TestServer_HandleRequestMsg_postExpansionExternal tests that unprotected
// external tasks can be overwritten by incoming request messages.
func TestServer_HandleRequestMsg_postExpansionExternal(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	msgA := mf.NewSimpleTaskMessage(true)
	server.HandleControlMsg(msgA)
	time.Sleep(ShortDuration)

	server.ExpandTasks()
	server.ExpandTasks()
	assertResourceMonitorStatusMatch(t, monitors.Resource, 0, 0, 0, DefaultResources, "resources after expansion")

	msgB := mf.NewSimpleTaskMessage(true)
	testHandleRequestMessageSuccess(t, server, monitors, msgB, "handling single message")
	assertResourceMonitorStatusMatch(t, monitors.Resource, 0, 1, 0, DefaultResources-1, "resources after expand")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_HandleRequestMsg_postExpansionExternal tests that unprotected
// internal tasks can be overwritten by incoming request messages.
func TestServer_HandleRequestMsg_postExpansionInternal(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	msgA := mf.NewSimpleTaskMessage(true)
	testHandleRequestMessageSuccess(t, server, monitors, msgA, "handling single message")

	server.ExpandTasks()
	assertResourceMonitorStatusMatch(t, monitors.Resource, 0, 1, DefaultResources-1, 0, "resources after expand")

	msgB := mf.NewSimpleTaskMessage(true)
	testHandleRequestMessageSuccess(t, server, monitors, msgB, "handling single message")
	assertResourceMonitorStatusMatch(t, monitors.Resource, 0, 2, DefaultResources-2, 0, "resources after expand")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_HandleRequestMsg_postExpansionWithExpansionBuffer tests that
// messages are correctly handled after expansion when there is an expansion
// buffer.
func TestServer_HandleRequestMsg_postExpansionWithExpansionBuffer(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	monitors := NewMonitors(DefaultResources)
	server := occamy.NewServer(occamy.ServerConfig{
		Slots:               DefaultResources,
		ExpansionSlotBuffer: 2,
		ExpansionPeriod:     0,
		KillTimeout:         100 * time.Millisecond,
		HeaderKeyTaskID:     HeaderKeyTaskID,
		Handler:             handler.Handle,
		Monitors: occamy.Monitors{
			Error:    monitors.Error,
			Latency:  monitors.Latency,
			Resource: monitors.Resource,
		},
	})
	mf := NewMessageFactory()

	msgA := mf.NewSimpleTaskMessage(true)
	testHandleRequestMessageSuccess(t, server, monitors, msgA, "handling single message")

	server.ExpandTasks()
	assertResourceMonitorStatusMatch(t, monitors.Resource, 2, 1, DefaultResources-3, 0, "resources after expansion")

	msgB := mf.NewSimpleTaskMessage(true)
	testHandleRequestMessageSuccess(t, server, monitors, msgB, "handling single message")
	assertResourceMonitorStatusMatch(t, monitors.Resource, 1, 2, DefaultResources-3, 0, "resources after another message")

	msgs := mf.NewSimpleTaskMessages(2, true)
	testHandleRequestMessagesSuccess(t, server, monitors, msgs, "")
	assertResourceMonitorStatusMatch(t, monitors.Resource, 0, 4, DefaultResources-4, 0, "resources after more messages")

	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_Shutdown tests that the shutdown can be successfully called and
// yield no errors.
func TestServer_Shutdown(t *testing.T) {
	server, monitors := NewServer(nil)
	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_Shutdown_idempotent tests that shutdown can be successfully called
// multiple times.
func TestServer_Shutdown_idempotent(t *testing.T) {
	server, monitors := NewServer(nil)
	testServerShutdownSuccess(t, server, monitors, DefaultResources)
	testServerShutdownSuccess(t, server, monitors, DefaultResources)
	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_Shutdown_idempotent_withTasks tests that shutdown can be
// successfully called multiple times when the server contains tasks.
func TestServer_Shutdown_idempotent_withTasks(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	messages := mf.NewSimpleTaskMessages(4, false)
	testHandleRequestMessagesSuccess(t, server, monitors, messages, "handling multiple tasks")
	testServerShutdownSuccess(t, server, monitors, DefaultResources)
	testServerShutdownSuccess(t, server, monitors, DefaultResources)
	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_Shutdown_withTasks tests that shutdown empties all slots without
// any errors when the server contains tasks.
func TestServer_Shutdown_withTasks(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	messages := mf.NewSimpleTaskMessages(4, false)
	testHandleRequestMessagesSuccess(t, server, monitors, messages, "handling multiple tasks")
	testServerShutdownSuccess(t, server, monitors, DefaultResources)
}

// TestServer_Shutdown_withUnstoppableTask tests that shutdown will record
// errors when the server contains unstoppable tasks.
func TestServer_Shutdown_withUnstoppableTask(t *testing.T) {
	controller := NewTaskController()
	handler := NewStandardHandler(controller)
	server, monitors := NewServer(handler.Handle)
	mf := NewMessageFactory()

	msg := mf.NewUnstoppableTaskMessage(false)
	testHandleRequestMessageSuccess(t, server, monitors, msg, "handling multiple tasks")

	shutdownServer(server)
	assertErrorIsOccamyError(t, occamy.ErrKindTaskNotKilled, monitors.Error.nextError(), "no error after shutdown")
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

func assertErrorIsOccamyError(t *testing.T, expectedKind occamy.ErrKind, err error, comment string) {
	if err == nil {
		t.Logf("%s: error was nil", comment)
		t.FailNow()
	}

	kind, ok := occamy.ExtractErrorKind(err)
	if !ok {
		t.Log(string(debug.Stack()))
		t.Logf("%s: error could not have its error kind extracted: %v", comment, err)
		t.FailNow()
	}

	if expectedKind != kind {
		t.Log(string(debug.Stack()))
		t.Logf("%s: error had kind %v that did not match expected kind %s", comment, kind, expectedKind)
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

func assertMessageStatus(t *testing.T, expected string, message *Message, comment string) {
	if message.status == expected {
		return
	}

	t.Logf("%s: message has status %s does not match expected status %s", comment, message.status, expected)
	t.FailNow()
}

func assertResourceMonitorStatusMatch(t *testing.T, rm *ResourceMonitor, empty, protected, unprotectedInternal, unprotectedExternal int, comment string) {
	assertIntsEqual(t, empty, rm.statuses[occamy.SlotStatusEmpty], fmt.Sprintf("%s: mismatch in empty slots", comment))
	assertIntsEqual(t, protected, rm.statuses[occamy.SlotStatusProtected], fmt.Sprintf("%s: mismatch in protected slots", comment))
	assertIntsEqual(t, unprotectedInternal, rm.statuses[occamy.SlotStatusUnprotectedInternal], fmt.Sprintf("%s: mismatch in unprotected internal slots", comment))
	assertIntsEqual(t, unprotectedExternal, rm.statuses[occamy.SlotStatusUnprotectedExternal], fmt.Sprintf("%s: mismatch in unprotected external slots", comment))

}

// endregion

// region Comment Helpers

func joinComments(primary, secondary string) string {
	switch {
	case primary == "":
		return secondary
	case secondary == "":
		return primary
	default:
		return fmt.Sprintf("%s: %s", primary, secondary)
	}
}

// endregion

// region Handler - Standard

type StandardHandler struct {
	controller *TaskController
}

func NewStandardHandler(controller *TaskController) *StandardHandler {
	return &StandardHandler{controller: controller}
}

func (sh *StandardHandler) Handle(header occamy.Headers, body []byte) (occamy.Task, error) {
	data := &MessageDataRequest{}
	if err := json.Unmarshal(body, data); err != nil {
		return nil, occamy.NewError(fmt.Errorf("failed to unmarshal as json: %w", err), occamy.ErrKindInvalidBody)
	}

	switch data.TaskGroup {
	case TaskGroupSimple:
		return NewSimpleTask(data.ID, data.Expandable, sh.controller), nil
	case TaskGroupUnstoppable:
		return NewUnstoppableTask(data.ID, data.Expandable), nil
	default:
		return nil, occamy.NewError(fmt.Errorf("unknown task group: %s", data.TaskGroup), occamy.ErrKindInvalidBody)
	}
}

// endregion Handler

// region Header Keys

const (
	HeaderKeyTaskID = "task_id"
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

// region Message Data - Request

type MessageDataRequest struct {
	ID         string
	Expandable bool
	TaskGroup  string
}

// endregion

// region Message Data - Control

type MessageDataControl struct {
	ID     string
	Cancel bool
}

// endregion

// region Message Factory

type MessageFactory struct {
	count int32
}

func NewMessageFactory() *MessageFactory {
	return &MessageFactory{
		count: 0,
	}
}

func (mf *MessageFactory) NewCancelMessage(id string) *Message {
	return mf.convertControlToMessage(MessageDataControl{
		ID:     id,
		Cancel: true,
	})
}

func (mf *MessageFactory) NewSimpleTaskMessage(expandable bool) *Message {
	return mf.convertRequestToMessage(MessageDataRequest{
		ID:         fmt.Sprintf("simple_%s", mf.nextIDSuffix()),
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
	return mf.convertRequestToMessage(MessageDataRequest{
		ID:         fmt.Sprintf("unstoppable_%s", mf.nextIDSuffix()),
		Expandable: expandable,
		TaskGroup:  TaskGroupUnstoppable,
	})
}

func (mf *MessageFactory) convertControlToMessage(data MessageDataControl) *Message {
	headers := make(occamy.Headers)
	headers[HeaderKeyTaskID] = data.ID

	body, err := json.Marshal(data)
	if err != nil {
		panic(fmt.Sprintf("unable to continue test as message data could not be marshaled into the body of a message: %v", err))
	}

	return &Message{
		id:      data.ID,
		headers: headers,
		body:    body,
		status:  MessageStatusOpen,
		mutex:   &sync.Mutex{},
	}
}

func (mf *MessageFactory) convertRequestToMessage(data MessageDataRequest) *Message {
	headers := make(occamy.Headers)

	body, err := json.Marshal(data)
	if err != nil {
		panic(fmt.Sprintf("unable to continue test as message data could not be marshaled into the body of a message: %v", err))
	}

	return &Message{
		id:      data.ID,
		headers: headers,
		body:    body,
		status:  MessageStatusOpen,
		mutex:   &sync.Mutex{},
	}
}

func (mf *MessageFactory) nextIDSuffix() string {
	value := atomic.AddInt32(&mf.count, 1)
	id := fmt.Sprintf("%03d", value)
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

// region Panic

// panicIfBoolIsFalse will panic if the value is false.
//
// This method should only be used when checking an internal mechanism of the
// test is as expected. The panic indicates that it is the test itself that has
// not been correctly implemented.
func panicIfBoolIsFalse(value bool, comment string) {
	if !value {
		return
	}

	panic(fmt.Sprintf("%s: boolean was true when expected to be false", comment))
}

// panicIfErrorMonitorContainsErrors will panic if error monitor contains errors.
//
// This method should only be used when checking an internal mechanism of the
// test is as expected. The panic indicates that it is the test itself that has
// not been correctly implemented.
func panicIfErrorMonitorContainsErrors(monitors Monitors, comment string) {
	if len(monitors.Error.errors) > 0 {
		panic(fmt.Sprintf("%s: error monitor contained errors", comment))
	}
}

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

func (tc *TaskController) error(id string) error {
	tc.mutex.Lock()
	err := tc.errors[id]
	tc.mutex.Unlock()
	return err
}

func (tc *TaskController) isStopped(id string) bool {
	tc.mutex.Lock()
	_, stopped := tc.errors[id]
	tc.mutex.Unlock()
	return stopped
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
	// The task is always registered before stopping to ensure that the channel
	// can always be closed.
	tc.register(id)
	tc.mutex.Lock()
	tc.stopOnces[id].Do(func() {
		tc.errors[id] = err
		close(tc.stopChs[id])
	})
	tc.mutex.Unlock()
}

func (tc *TaskController) stopCh(id string) <-chan struct{} {
	tc.mutex.Lock()
	ch := tc.stopChs[id]
	tc.mutex.Unlock()
	return ch
}

// endregion

// region Task - Simple

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
		return occamy.NewError(ctx.Err(), occamy.ErrKindTaskInterrupted)
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
		tasks[i] = NewSimpleTask(task.assistantID(), task.expandable, task.controller)
	}

	return tasks
}

func (task *SimpleTask) Handle(_ context.Context, _ occamy.Headers, body []byte) error {
	message := &MessageDataControl{}
	if err := json.Unmarshal(body, message); err != nil {
		return err
	}

	if message.ID != task.id {
		return fmt.Errorf("control message sent to wrong task")
	}

	if message.Cancel {
		task.stop()
	}

	return nil
}

func (task *SimpleTask) assistantID() string {
	return fmt.Sprintf("%s_assistant", task.id)
}

func (task *SimpleTask) stop() {
	task.stopOnce.Do(func() {
		close(task.stopCh)
	})
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

func NewServer(handler occamy.Handler) (*occamy.Server, Monitors) {
	monitors := NewMonitors(DefaultResources)
	server := occamy.NewServer(occamy.ServerConfig{
		Slots:               DefaultResources,
		ExpansionSlotBuffer: 0,
		ExpansionPeriod:     0,
		KillTimeout:         100 * time.Millisecond,
		HeaderKeyTaskID:     HeaderKeyTaskID,
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
	panicIfErrorMonitorContainsErrors(monitors, "error monitor must contain no errors before testing that a server can be properly shutdown")

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

// testHandleRequestMessageSuccess tests that the server can handle a message
// without an error and adjusting the resources appropriately.
func testHandleRequestMessageSuccess(t *testing.T, server *occamy.Server, monitors Monitors, message *Message, comment string) {
	panicIfErrorMonitorContainsErrors(monitors, "error monitor must contain no errors before testing that a message can be handled correctly")

	empty := monitors.Resource.statuses[occamy.SlotStatusEmpty]
	protected := monitors.Resource.statuses[occamy.SlotStatusProtected]
	unprotectedInternal := monitors.Resource.statuses[occamy.SlotStatusUnprotectedInternal]
	unprotectedExternal := monitors.Resource.statuses[occamy.SlotStatusUnprotectedExternal]

	// Adjusts the resources to the expected amount
	protected++
	switch {
	case empty > 0:
		empty--
	case unprotectedExternal > 0:
		unprotectedExternal--
	case unprotectedInternal > 0:
		unprotectedInternal--
	default:
		panic("test invalid: insufficient resources for the message to be successfully handled")
	}

	server.HandleRequestMsg(message)
	assertErrorIsNil(t, monitors.Error.nextError(), joinComments(comment, "unexpected error after adding task"))
	assertResourceMonitorStatusMatch(t, monitors.Resource, empty, protected, unprotectedInternal, unprotectedExternal, joinComments(comment, "resources after adding task"))
}

// testHandleRequestMessageSuccess tests that the server can handle messages
// without an error and adjusting the resources appropriately.
func testHandleRequestMessagesSuccess(t *testing.T, server *occamy.Server, monitors Monitors, messages []*Message, comment string) {
	for i := range messages {
		testHandleRequestMessageSuccess(t, server, monitors, messages[i], joinComments(comment, fmt.Sprintf("message %d", i)))
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
	groups   map[string]int
	statuses map[occamy.SlotStatus]int

	mutex *sync.Mutex
}

func NewResourceMonitor(resources int) *ResourceMonitor {
	rm := &ResourceMonitor{
		groups:   make(map[string]int),
		statuses: make(map[occamy.SlotStatus]int),
		mutex:    &sync.Mutex{},
	}

	rm.statuses[occamy.SlotStatusEmpty] += resources
	return rm
}

func (rm *ResourceMonitor) RecordTaskStarting(group string, status occamy.SlotStatus) {
	rm.mutex.Lock()
	rm.groups[group]++
	rm.statuses[status]++
	rm.statuses[occamy.SlotStatusEmpty]--
	rm.mutex.Unlock()
}

func (rm *ResourceMonitor) RecordTaskStopping(group string, status occamy.SlotStatus) {
	rm.mutex.Lock()
	rm.groups[group]--
	rm.statuses[status]--
	rm.statuses[occamy.SlotStatusEmpty]++
	rm.mutex.Unlock()
}

// endregion
