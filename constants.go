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
	slotStatusEmpty               slotStatus = iota // The slot is empty.
	slotStatusUnprotectedExternal                   // The slot contains an unprotected external task
	slotStatusUnprotectedInternal                   // The slot contains an unprotected internal task
	slotStatusProtected                             // The slot contains a protected task which must be done (i.e. only killed during shutdown).
	numSlotStatuses
)

func (s slotStatus) export() SlotStatus {
	switch s {
	case slotStatusEmpty:
		return SlotStatusEmpty
	case slotStatusUnprotectedExternal:
		return SlotStatusUnprotectedExternal
	case slotStatusUnprotectedInternal:
		return SlotStatusUnprotectedInternal
	case slotStatusProtected:
		return SlotStatusProtected
	default:
		return "undefined"
	}
}

// endregion
