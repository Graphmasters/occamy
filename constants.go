package occamy

// region Process Names

const (
	ProcessHandleRequestMessage = "handle_request_message"
	ProcessHandleControlMessage = "handle_control_message"
	ProcessExpansion            = "expansion_process"
)

// endregion

// region Slot Status (exported)

type SlotStatus string

const (
	SlotStatusEmpty               SlotStatus = "empty"
	SlotStatusProtected           SlotStatus = "protected"
	SlotStatusUnprotectedInternal SlotStatus = "unprotected_internal"
	SlotStatusUnprotectedExternal SlotStatus = "unprotected_external"
)

// endregion

// region Slot Status (unexported)

// slotStatus represents the status of a slot as in integer. This makes it
// easier for comparisons. This isn't exported in case we wish to include
// another status at a future date.
type slotStatus uint8

const (
	slotStatusEmpty       slotStatus = iota // The slot is empty.
	slotStatusExternal                      // The slot contains an external task
	slotStatusUnprotected                   // The slot is unprotected meaning its task may be cancelled at any time.
	slotStatusProtected                     // The slot is protected meaning its task will be be done.
	numSlotStatuses
)

func (s slotStatus) export() SlotStatus {
	switch s {
	case slotStatusEmpty:
		return SlotStatusEmpty
	case slotStatusExternal:
		return SlotStatusUnprotectedExternal
	case slotStatusUnprotected:
		return SlotStatusUnprotectedInternal
	case slotStatusProtected:
		return SlotStatusProtected
	default:
		return "undefined"
	}
}

// endregion
