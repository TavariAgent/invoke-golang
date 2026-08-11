package invoke

import (
	"sync/atomic"
	"time"
)

// FracTimer — calibrated fractional microsecond timer.
// Spreads completions across OS tick boundaries using sequence position
// as a fraction of the measured tick size. Gives readable ordering on
// systems where the clock granularity is coarser than task duration.

type FracTimer struct {
	start  time.Time
	seq    atomic.Int64
	tickUs int64
}

func newFracTimer() *FracTimer {
	ft := &FracTimer{}

	// calibrate — spin until the OS clock actually advances
	// measures real tick size once, never assumed
	a := time.Now()
	for time.Now().Equal(a) {
	}
	ft.tickUs = time.Since(a).Microseconds()
	if ft.tickUs < 1 {
		ft.tickUs = 1
	}

	ft.start = time.Now()
	return ft
}

// ReadUs returns real elapsed microseconds plus a synthetic fractional
// offset derived from sequence position and measured tick size.
// Each call gets a unique slice of the current tick — ordering is real,
// spread is proportional to actual OS resolution.
func (ft *FracTimer) ReadUs() int64 {
	realUs := time.Since(ft.start).Microseconds()
	seq := ft.seq.Add(1)
	fracUs := (seq * ft.tickUs) / 1000
	return realUs + fracUs
}

// Reset resets the timer origin and sequence counter.
func (ft *FracTimer) Reset() {
	ft.seq.Store(0)
	ft.start = time.Now()
}

// TickUs returns the measured OS tick size in microseconds.
func (ft *FracTimer) TickUs() int64 {
	return ft.tickUs
}
