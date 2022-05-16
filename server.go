package occamy

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// Server is a server that handles incoming messages
// and passes them to the handler (only one is allowed). The task associated with
// the messages are performed and when possible spare
// resources are used by the handler for additional tasks.
// The server must always be created using the NewServer
// method.
type Server struct {
	// The config
	config ServerConfig

	// For monitoring resources and performance
	resourceCounts []atomicInt // The count of resources uses by each stat

	// For managing slots and tasks
	slots      []*slot    // The  tasks (this is slice is of size resourceCount)
	slotsMutex sync.Mutex // A lock to ensure only one task is being started at a time

	// For managing external tasks received
	externalTasks      []*externalTask // External tasks which can be run in unprotected slots
	externalTasksMutex sync.Mutex      // A lock to ensure the external tasks are modified safely

	// For closing
	close chan struct{}
	once  *sync.Once
}

// NewServer creates a new occamy server based using
// the configuration provided. The expansion process
// is automatically started.
func NewServer(config ServerConfig) *Server {
	// Initialises resource components
	server := &Server{
		config: config,
	}

	server.config.Monitors = ensureMonitorsAreValid(server.config.Monitors)

	server.resourceCounts = make([]atomicInt, numSlotStatuses)
	server.resourceCounts[slotStatusEmpty] = newAtomicInt(config.Slots)
	for i := slotStatusEmpty + 1; i < numSlotStatuses; i++ {
		server.resourceCounts[i] = newAtomicInt(0)
	}

	// Initialises slot components
	server.slots = make([]*slot, config.Slots)
	server.slotsMutex = sync.Mutex{}
	for i := range server.slots {
		server.slots[i] = newSlot()
	}

	// Initialising closing objects
	server.close = make(chan struct{})
	server.once = &sync.Once{}

	go startPeriodicProcess(server.expand, config.ExpansionPeriod, server.close)

	return server
}

// HandleControlMsg handles a control method.
//
// Control messages with a task ID set in the header will be passed on to ALL
// tasks which have matching IDs. If there is no task ID set then the message
// will be interpreted as an optional external request and stored to be used in
// the expansion process.
func (server *Server) HandleControlMsg(msg Message) {
	defer server.recordProcessDuration(time.Now(), ProcessHandleControlMessage)
	defer server.msgAck(msg)

	// Prepares and checks headers and deals with the special case that there
	// is no task ID header.
	headers := msg.Headers()
	idValue, ok := headers[server.config.HeaderKeyTaskID]
	if !ok {
		go server.handleExternalRequest(msg)
		return
	}

	// TODO: This blocks handling of control messages for all of occamy. It
	//   is unclear if this should change...
	wg := sync.WaitGroup{}

	id := idValue.(string)
	for i := range server.slots {
		s := server.slots[i]
		if s.isEmpty() || s.getTaskID() != id {
			continue
		}

		wg.Add(1)
		go func(slot *slot) {
			err := slot.handleControlMsg(headers, msg.Body())
			if err != nil {
				server.config.Monitors.Error.RecordError(err)
			}
			wg.Done()
		}(s)

	}

	wg.Wait()
}

// HandleRequestMsg handles an incoming request message.
//
// The handler defined in the server is used to generate a task. The task should
// be started immediately, unless an error is encountered in which case the
// message will be nacked.
func (server *Server) HandleRequestMsg(msg Message) {
	defer server.recordProcessDuration(time.Now(), ProcessHandleRequestMessage)

	headers := msg.Headers()
	err := server.checkRequestHeaders(headers)
	if err != nil {
		server.msgReject(msg, false)
		err = wrapErrorIfNotLocalErrorOrMismatch(err, ErrInvalidHeader)
		server.config.Monitors.Error.RecordError(err)
		return
	}

	task, err := server.config.Handler(headers, msg.Body())
	if err != nil {
		server.msgReject(msg, false)
		server.config.Monitors.Error.RecordError(err)
		return
	}

	properties := properties{
		state: slotStatusProtected,
	}

	if !server.addAndDoTask(task, properties, msg) {
		server.msgReject(msg, true)
		details := task.Details()
		err := NewDetailedError(ErrTaskNotAdded, fmt.Sprintf("unable to add task with id %s", details.ID))
		server.config.Monitors.Error.RecordError(err)
		return
	}
}

// Shutdown stops the expansion process and ends every task. It will allow
// some time for the tasks to gracefully stop.
func (server *Server) Shutdown(ctx context.Context) {
	server.once.Do(func() {
		// Closes channel which stops expansion
		close(server.close)

		// Checks if all slots become empty before context is cancelled.
		// This give task a chance to finish without interruption.
		ticker := time.NewTicker(100 * time.Millisecond)
		running := !server.resourceCounts[slotStatusEmpty].isEqual(server.config.Slots)
		for running {
			select {
			case <-ctx.Done():
				running = false
			case <-ticker.C:
				running = !server.resourceCounts[slotStatusEmpty].isEqual(server.config.Slots)
			}
		}
		ticker.Stop()

		// Kills all running tasks
		wg := sync.WaitGroup{}
		for i := range server.slots {
			wg.Add(1)
			go func(index int) {
				slot := server.slots[index]
				slot.kill()
				if !slot.waitTillEmpty(server.config.KillTimeout) {
					// TODO: Turn this into a standardised error
					server.config.Monitors.Error.RecordError(ErrTaskNotKilled)
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
	})
}

// addAndDoTask attempts to add a task into a suitable
// slot. A boolean is indicating if the task was
// successfully added.
func (server *Server) addAndDoTask(task Task, properties properties, msg Message) bool {
	server.slotsMutex.Lock()
	defer server.slotsMutex.Unlock()

	index, ok := server.findSuitableSlotIndex(properties.state)
	if !ok {
		return false
	}

	// Ends the task it will replace
	if !server.slots[index].isEmpty() {
		server.slots[index].kill()

		// Waits for the task to be emptied (i.e killed)
		if !server.slots[index].waitTillEmpty(server.config.KillTimeout) {
			server.config.Monitors.Error.RecordError(NewDetailedError(ErrTaskNotKilled, "failed to free task within time limit"))
			return false
		}
	}

	// Sets task in given slot
	server.slots[index].setTask(task, properties)

	slot := server.slots[index]
	taskName := slot.getTaskGroup()
	server.adjustResourcesTaskStarting(properties.state, taskName)
	go func() {
		startTime := time.Now()

		// Performs task and once it is finished declares it empty. The message is then acked or nacked.
		err := slot.doTask()
		switch {
		// TODO: The service shouldn't rely on users sending this error. It should
		//    always be the case that if the service is shutting down (the only
		//    reason a protected task would be interrupted) then the message
		//    should be be requeued UNLESS the task completed without any error.
		case err == ErrTaskInterrupted:
			defer server.msgReject(msg, true)
		case err != nil && properties.state == slotStatusProtected:
			defer server.msgReject(msg, false)
			server.config.Monitors.Error.RecordError(err)
		default:
			defer server.msgAck(msg)
		}

		server.recordTaskDuration(startTime, properties.state, taskName)
		server.adjustResourcesTaskStopping(properties.state, taskName)

		slot.empty()
	}()

	return true
}

func (server *Server) addToExternalTasks(handlerID string, task Task) {
	properties := properties{
		handler: handlerID,
		state:   slotStatusExternal,
	}

	server.externalTasksMutex.Lock()
	server.externalTasks = append(server.externalTasks, &externalTask{
		task:       task,
		details:    task.Details(),
		properties: properties,
	})
	server.externalTasksMutex.Unlock()
}

// adjustCount adjusts the resource handled.
func (server *Server) adjustResourcesTaskStarting(state slotStatus, taskType string) {
	server.resourceCounts[state].increase()
	server.resourceCounts[slotStatusEmpty].decrease()
	server.config.Monitors.Resource.RecordTaskStarting(server.config.HandlerID, taskType, state.export())
}

func (server *Server) adjustResourcesTaskStopping(state slotStatus, taskType string) {
	server.resourceCounts[state].decrease()
	server.resourceCounts[slotStatusEmpty].increase()
	server.config.Monitors.Resource.RecordTaskStopping(server.config.HandlerID, taskType, state.export())
}

// checkRequestHeader checks if the headers
// have the necessary valid key-value pairs
// for a request message.
func (server *Server) checkRequestHeaders(_ Headers) error {
	return nil
}

// findSuitableSlotIndex finds the index of suitable slot. This is achieved
// by finding the slot with the lowest priority slotStatus, starting at empty.
func (server *Server) findSuitableSlotIndex(state slotStatus) (int, bool) {
	targetSlotState := slotStatusEmpty
	targetSlotStateFound := false

	// Checks if the target slot slotStatus should be the empty slotStatus
	numEmpty := server.resourceCounts[slotStatusEmpty].load()
	if numEmpty > server.config.ExpansionSlotBuffer || (numEmpty > 0 && state == slotStatusProtected) {
		targetSlotStateFound = true
	}

	// Loops through the other possible target slot states
	if !targetSlotStateFound {
		targetSlotState++
		for !targetSlotStateFound && targetSlotState < state {
			if server.resourceCounts[targetSlotState].load() > 0 {
				targetSlotStateFound = true
				break
			}
			targetSlotState++
		}
	}

	if !targetSlotStateFound {
		return 0, false
	}

	// Finds a slot with a matching target slot slotStatus.
	for i, slot := range server.slots {
		if slot.state() == targetSlotState {
			return i, true
		}
	}

	return 0, false
}

// findSuitableSlotCount finds the number of suitable slots for a task with
// the given slotStatus. This includes empty slots and slots with lower importance.
func (server *Server) findSuitableSlotCount(state slotStatus) int {
	count := 0
	numEmpty := server.resourceCounts[slotStatusEmpty].load()
	if state == slotStatusProtected {
		count += numEmpty
	} else if numEmpty > server.config.ExpansionSlotBuffer {
		count += numEmpty - server.config.ExpansionSlotBuffer
	}

	targetState := slotStatusEmpty + 1
	for ; targetState < state; targetState++ {
		count += server.resourceCounts[slotStatusEmpty].load()
	}

	return count
}

// expand runs the expansion processes.
func (server *Server) expand() {
	server.expandLocal()
	server.expandExternal()
}

// expandExternal runs tasks that were added externally i.e. via the control
// message handler.
func (server *Server) expandExternal() {
	available := server.findSuitableSlotCount(slotStatusExternal)
	if len(server.externalTasks) == 0 || available <= 0 {
		return
	}

	server.externalTasksMutex.Lock()
	for len(server.externalTasks) > 0 && available > 0 {
		task := server.externalTasks[0]

		if task.details.Deadline.Before(time.Now()) {
			server.externalTasks = server.externalTasks[1:]
			continue
		}

		ok := server.addAndDoTask(task.task, task.properties, nil)
		if !ok {
			return
		}

		server.externalTasks = server.externalTasks[1:]
		available--
	}
	server.externalTasksMutex.Unlock()
}

// expandLocal expands the tasks already running on the server.
func (server *Server) expandLocal() {
	if server.resourceCounts[slotStatusEmpty].isEqual(server.config.Slots) || server.findSuitableSlotCount(slotStatusProtected-1) == 0 {
		return
	}

	defer server.recordProcessDuration(time.Now(), ProcessExpansion)

	// Sorts indices to prioritise by slotStatus
	indices := rand.Perm(server.config.Slots)
	sort.Slice(indices, func(i, j int) bool {
		return server.slots[i].properties.state > server.slots[j].properties.state
	})

	// Attempts to expand tasks (in the order in which they prioritised)
	for _, index := range indices {
		slot := server.slots[index]

		properties := properties{
			handler: slot.properties.handler,
			state:   slotStatusUnprotected,
		}
		if slot.properties.state == slotStatusExternal {
			properties.state = slotStatusExternal
		}

		available := server.findSuitableSlotCount(properties.state)
		if available == 0 {
			return
		}

		tasks := slot.expand(available)
		for _, task := range tasks {
			ok := server.addAndDoTask(task, properties, nil)
			if !ok {
				return
			}
		}
	}
}

// handleExternalRequest handles a request message from the control channel.
func (server *Server) handleExternalRequest(msg Message) {
	headers := msg.Headers()
	err := server.checkRequestHeaders(headers)
	if err != nil {
		server.config.Monitors.Error.RecordError(ErrInvalidHeader)
		return
	}

	task, err := server.config.Handler(headers, msg.Body())
	if err != nil {
		server.config.Monitors.Error.RecordError(err)
		return
	}

	server.addToExternalTasks(server.config.HandlerID, task)
}

// msgAck acknowledges the delivery of message i.e. it was successfully handled.
func (server *Server) msgAck(msg Message) {
	if msg == nil {
		return
	}

	err := msg.Ack()
	if err != nil {
		err = wrapErrorIfNotLocalErrorOrMismatch(err, ErrMessageNotAcked)
		server.config.Monitors.Error.RecordError(err)
	}
}

// msgReject negatively acknowledges the delivery of message i.e. it was unsuccessfully handled.
func (server *Server) msgReject(msg Message, requeue bool) {
	if msg == nil {
		return
	}

	err := msg.Reject(requeue)
	if err != nil {
		err = wrapErrorIfNotLocalErrorOrMismatch(err, ErrMessageNotNacked)
		server.config.Monitors.Error.RecordError(err)
	}
}

// recordTaskDuration records the task duration.
func (server *Server) recordTaskDuration(startTime time.Time, status slotStatus, taskType string) {
	latency := time.Since(startTime)
	server.config.Monitors.Latency.RecordTaskDuration(server.config.HandlerID, taskType, status.export(), latency)
}

// recordTaskDuration records the task duration.
func (server *Server) recordProcessDuration(startTime time.Time, process string) {
	latency := time.Since(startTime)
	server.config.Monitors.Latency.RecordProcessDuration(process, latency)
}
