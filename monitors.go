package occamy

import (
	"time"
)

// region Monitors (combined)

type Monitors struct {
	Error    ErrorMonitor
	Latency  LatencyMonitor
	Resource ResourceMonitor
}

func ensureMonitorsAreValid(monitors Monitors) Monitors {
	if monitors.Error == nil {
		monitors.Error = NopErrorMonitor{}
	}

	if monitors.Latency == nil {
		monitors.Latency = NopLatencyMonitor{}
	}

	if monitors.Resource == nil {
		monitors.Resource = NopResourceMonitor{}
	}

	return monitors
}

// endregion

// region Error Monitor

type ErrorMonitor interface {
	RecordError(err error)
}

type NopErrorMonitor struct{}

func (NopErrorMonitor) RecordError(_ error) {}

// endregion

// region Latency Monitor

type LatencyMonitor interface {
	RecordProcessDuration(process string, duration time.Duration)
	RecordTaskDuration(handler string, group string, status SlotStatus, duration time.Duration)
}

type NopLatencyMonitor struct{}

func (NopLatencyMonitor) RecordProcessDuration(_ string, _ time.Duration) {}

func (NopLatencyMonitor) RecordTaskDuration(_ string, _ string, _ SlotStatus, _ time.Duration) {}

// endregion

// region Resource Monitor

type ResourceMonitor interface {
	RecordTaskStarting(handler string, group string, status SlotStatus)
	RecordTaskStopping(handler string, group string, status SlotStatus)
}

type NopResourceMonitor struct{}

func (NopResourceMonitor) RecordTaskStarting(_ string, _ string, _ SlotStatus) {}

func (NopResourceMonitor) RecordTaskStopping(_ string, _ string, _ SlotStatus) {}

// endregion
