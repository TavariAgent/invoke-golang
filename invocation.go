package invoke

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync/atomic"
)

// --- Sentinel Errors --------------------------------------------------------

var (
	ErrEngineNotRunning = errors.New("invoke: engine not running")
	ErrEngineStopped    = errors.New("invoke: engine stopped")
	ErrNilTask          = errors.New("invoke: nil task rejected")
	ErrInvalidPriority  = errors.New("invoke: priority out of valid range")
	ErrUnknownType      = errors.New("invoke: unknown task type rejected")
	ErrQueueFull        = errors.New("invoke: worker queue full, task dropped")
)

// --- Log Enum ---------------------------------------------------------------

type LogEvent uint

const (
	LogDropped   LogEvent = 1 << iota // rejected or dropped tasks
	LogLifecycle                      // Start and Stop events
	LogCalibrate                      // calibration output
	LogWorkerErr                      // worker-level errors
	LogAll       LogEvent = ^LogEvent(0)
)

// --- Alloc Strategy ---------------------------------------------------------

type AllocStrategy int

const (
	AllocateAll  AllocStrategy = iota // consume all remaining cores
	AllocateSome                      // leave headroom for host system
)

// --- Logger Interface -------------------------------------------------------

type Logger interface {
	Printf(format string, args ...any)
}

// --- Config -----------------------------------------------------------------

type Config struct {
	Workers       int           // harmonic worker pool size
	IWorkers      int           // dedicated idempotent pool size
	ICoreWorkers  int           // iCore receipt lane — defaults to NumCPU if zero
	PWorkers      int           // protocol pool — typically 2
	Allocate      AllocStrategy // core allocation strategy
	HeadroomN     int           // cores reserved when using AllocateSome
	LogEvents     LogEvent      // zero value disables logging entirely
	Logger        Logger        // nil falls back to standard log package
	LogDir        string        // defaults to ./invoke-logs if empty
	NotifyDrops   bool          // false by default — terminal stays silent
	LogBufferSize int           // log channel depth — defaults to 512
}

// --- Engine -----------------------------------------------------------------

type Engine struct {
	cfg     Config
	running atomic.Bool
	stopCh  chan struct{}
	logCh   chan logEntry
	logger  Logger
	batcher *dropBatcher
	iPool   *iPool
	pPool   *pPool
	hPool   *hPool
	Timer   *FracTimer
}

func NewEngine(cfg Config) *Engine {
	e := &Engine{
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}
	if cfg.Logger != nil {
		e.logger = cfg.Logger
	} else if cfg.LogEvents != 0 {
		e.logger = log.Default()
	}
	if e.logger != nil {
		bufSize := cfg.LogBufferSize
		if bufSize == 0 {
			bufSize = 512
		}
		e.logCh = make(chan logEntry, bufSize)
	}
	e.batcher = newDropBatcher(cfg)
	return e
}

func (e *Engine) Start() {
	e.Timer = newFracTimer() // calibrate before anything else runs
	if e.logger != nil {
		go e.runLogger()
	}
	e.iPool = newIPool(e.cfg.IWorkers, e)
	e.pPool = newPPool(e.cfg.PWorkers, e)
	e.hPool = newHPool(e)
	e.iPool.start(e.cfg.IWorkers)
	e.iPool.startCores(e.resolveICoreWorkers()) // iCore lane attached here
	e.pPool.start(e.cfg.PWorkers)
	e.hPool.start(e.resolveWorkers())
	go e.runDropFlusher()
	e.running.Store(true)
	e.emit(LogLifecycle,
		"invoke: engine started — cores:%d iworkers:%d pworkers:%d workers:%d tick:%dµs",
		runtime.NumCPU(), e.cfg.IWorkers, e.cfg.PWorkers,
		e.resolveWorkers(), e.Timer.TickUs())
}

func (e *Engine) resolveICoreWorkers() int {
	if e.cfg.ICoreWorkers > 0 {
		return e.cfg.ICoreWorkers
	}
	return runtime.NumCPU() // one per core by default
}

func (e *Engine) Stop() {
	e.running.Store(false)
	e.iPool.stop()
	e.pPool.stop()
	e.hPool.stop()
	e.emit(LogLifecycle, "invoke: engine stopped")
	e.flushDropCounts()  // capture anything still in the counters
	close(e.stopCh)      // signal all goroutines including drop flusher
	e.batcher.flushNow() // write immediately, don't wait for the 2s timer
}

func (e *Engine) resolveWorkers() int {
	switch e.cfg.Allocate {
	case AllocateAll:
		remaining := runtime.NumCPU() - e.cfg.IWorkers - e.cfg.PWorkers
		if remaining < 1 {
			return 1
		}
		return remaining
	case AllocateSome:
		remaining := runtime.NumCPU() - e.cfg.IWorkers - e.cfg.PWorkers - e.cfg.HeadroomN
		if remaining < 1 {
			return 1
		}
		return remaining
	default:
		if e.cfg.Workers > 0 {
			return e.cfg.Workers
		}
		return 1
	}
}

// --- Internal ---------------------------------------------------------------

type logEntry struct {
	event LogEvent
	msg   string
}

func (e *Engine) emit(event LogEvent, format string, args ...any) {
	if e.logger == nil || e.cfg.LogEvents&event == 0 {
		return
	}
	select {
	case e.logCh <- logEntry{event, fmt.Sprintf(format, args...)}:
	default:
		// log channel full — drop silently, never block task path
	}
}

func (e *Engine) runLogger() {
	for {
		select {
		case entry := <-e.logCh:
			if entry.event == LogDropped {
				e.batcher.add(entry.msg)
			} else {
				e.logger.Printf(entry.msg)
			}
		case <-e.stopCh:
			// drain remaining
			for {
				select {
				case entry := <-e.logCh:
					if entry.event == LogDropped {
						e.batcher.add(entry.msg)
					} else {
						e.logger.Printf(entry.msg)
					}
				default:
					e.batcher.flushNow()
					return
				}
			}
		}
	}
}

func (e *Engine) SilenceTerminal() { e.batcher.Silence() }
func (e *Engine) ResumeTerminal()  { e.batcher.Resume() }
