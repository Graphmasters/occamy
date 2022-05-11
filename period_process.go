package occamy

import (
	"time"
)

func startPeriodicProcess(process func(), period time.Duration, stopCh <-chan struct{}) {
	if period <= 0 {
		return
	}

	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			process()
		}
	}
}
