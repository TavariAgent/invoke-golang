package invoke

import (
	"runtime"
	"sync/atomic"
	"time"
)

type WorkerRecommendation struct {
	MinWorkers int
	MaxWorkers int
	Duration   time.Duration
	Throughput int64
}

func (e *Engine) CalibrateIdempotent(sampleSize int, duration time.Duration) WorkerRecommendation {
	if !e.running.Load() {
		e.emit(LogCalibrate, "invoke: calibrate skipped — engine not running")
		return WorkerRecommendation{}
	}

	var completed atomic.Int64

	start := time.Now()

	for range sampleSize {
		e.iPool.submit(iTask{fn: func() {
			completed.Add(1)
		}})
	}

	// spin until all tasks complete — measures real elapsed, not sleep window
	for completed.Load() < int64(sampleSize) {
		runtime.Gosched()
	}

	elapsed := time.Since(start)
	throughput := int64(float64(sampleSize) / elapsed.Seconds())

	cores := runtime.NumCPU()
	minimum := e.cfg.IWorkers
	maximum := min(e.cfg.IWorkers*2, cores)

	// throughput feedback — if per-worker rate is low, recommend scaling up
	perWorker := throughput / int64(max(e.cfg.IWorkers, 1))
	if perWorker < 500_000 {
		minimum = e.cfg.IWorkers + 1
		maximum = min(minimum*2, cores)
	}

	if minimum < 1 {
		minimum = 1
	}
	if maximum < minimum {
		maximum = minimum
	}

	rec := WorkerRecommendation{
		MinWorkers: minimum,
		MaxWorkers: maximum,
		Duration:   elapsed,
		Throughput: throughput,
	}

	e.emit(LogCalibrate, "invoke: calibration complete — throughput:%d/s min:%d max:%d",
		rec.Throughput, rec.MinWorkers, rec.MaxWorkers)

	return rec
}
