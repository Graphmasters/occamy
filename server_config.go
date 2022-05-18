package occamy

import (
	"time"
)

// ServerConfig contains the necessary data to create an occamy server.
type ServerConfig struct {
	// Slots sets how many slots will be crated i.e. the maximum number of
	// simultaneous tasks allowed.
	Slots int

	// ExpansionSlotBuffer sets how many
	ExpansionSlotBuffer int

	// ExpansionPeriod sets how often the expansion process will be run. A value
	// of zero or less will mean that the expansion process will never be run.
	ExpansionPeriod time.Duration

	// KillTimeout sets the maximum duration allowed for task a task to be
	// killed in. This is important to set for allowing unprotected tasks
	// to be killed and replaced by protected ones.
	KillTimeout time.Duration

	// HeaderKeyTaskID sets the header key used to obtain the task ID for any
	// control message received. In other words the value in the header
	// corresponding to this key will be treated as the task ID.
	HeaderKeyTaskID string

	Handler Handler

	Monitors Monitors
}
