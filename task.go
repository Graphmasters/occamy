package occamy

import (
	"context"
	"time"
)

const (
	TaskGroupNone string = ""
)

type TaskDetails struct {
	// Deadline is the deadline of the task, if it is known,
	// and is only relevant for tasks that that arrived via
	// the control channel.
	Deadline time.Time

	// The ID of the the task. Any control messages
	// for this task must have this ID included in the header.
	ID string

	// Group denotes the general group that task this belongs to. This
	// is used for monitoring usage.
	Group string
}

// Task represents a task that must be done.
type Task interface {
	// Do should perform the task and if a failure is encountered
	// one of the custom occamy errors should be returned.
	//
	// If the context provided is canceled the process must be stopped
	// immediately and ungracefully, and return the error
	// `ErrTaskInterrupted` which will cause the task to be run on
	// another server.
	Do(ctx context.Context) error

	Details() TaskDetails

	// Expand creates additional tasks. These tasks will be unprotected
	// and maybe cancelled at anytime.
	Expand(n int) []Task

	// Handle handles a control message. The error return should be
	// one of the custom occamy errors.
	Handle(ctx context.Context, headers Headers, body []byte) error
}
