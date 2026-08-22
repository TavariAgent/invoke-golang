package invoke

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
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
	TCPAddr       string        // ":7777" — empty disables TCP listener
	UDPAddr       string        // ":7778" — empty disables UDP listener
	HTTPAddr      string        // ":8080" — empty disables HTTP server
}

// --- Engine -----------------------------------------------------------------

type Engine struct {
	cfg        Config
	running    atomic.Bool
	stopCh     chan struct{}
	logCh      chan logEntry
	logger     Logger
	batcher    *dropBatcher
	iPool      *iPool
	pPool      *pPool
	hPool      *hPool
	timer      *FracTimer
	connection sync.Map
	tcpLn      net.Listener
	udpConn    *net.UDPConn
	table      *CommandTable
	httpSrv    *http.Server
	pages      sync.Map // key → []byte, preloaded at startup
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

func (e *Engine) AttachTable(ct *CommandTable) {
	e.table = ct
}

func (e *Engine) Start() {
	e.timer = newFracTimer() // calibrate before anything else runs
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
		e.resolveWorkers(), e.timer.TickUs())
	if e.cfg.TCPAddr != "" {
		go e.serveTCP()
	}
	if e.cfg.UDPAddr != "" {
		go e.serveUDP()
	}
}

const ackTimeout = 50 * time.Millisecond // real client responds immediately

type connEntry struct {
	conn  net.Conn
	mu    sync.Mutex // one writer at a time — serializes Push calls
	ackCh chan byte  // reader goroutine drops 0 or 1 here
}

func (e *Engine) resolveICoreWorkers() int {
	if e.cfg.ICoreWorkers > 0 {
		return e.cfg.ICoreWorkers
	}
	return runtime.NumCPU() // one per core by default
}

func (e *Engine) Stop() {
	e.running.Store(false)
	if e.tcpLn != nil {
		if err := e.tcpLn.Close(); err != nil {
			e.emit(LogDropped, "invoke: TCP listener close: %v", err)
			// continue — pools must still stop
		}
	}
	if e.udpConn != nil {
		if err := e.udpConn.Close(); err != nil {
			e.emit(LogDropped, "invoke: UDP conn close: %v", err)
		}
	}
	if e.httpSrv != nil {
		if err := e.httpSrv.Close(); err != nil {
			e.emit(LogDropped, "invoke: HTTP server close: %v", err)
		}
	}
	e.iPool.stop()
	e.pPool.stop()
	e.hPool.stop()
	e.emit(LogLifecycle, "invoke: engine stopped")
	e.flushDropCounts()
	close(e.stopCh)
	e.batcher.flushNow()
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

// Packet is the canonical wire type — everything on the wire is this.
// Push encodes it, the TCP listener decodes it. Both sides compile against
// the same struct so the format is never ambiguous.
type Packet struct {
	Command string            `json:"cmd"`
	Args    map[string]string `json:"args,omitempty"`
	Ts      int64             `json:"ts"`                // FracTimer.ReadUs() — ordering authority
	Payload []byte            `json:"payload,omitempty"` // bin data when present
}

type logEntry struct {
	event LogEvent
	msg   string
}

func (e *Engine) register(clientID string, conn net.Conn) *connEntry {
	entry := &connEntry{
		conn:  conn,
		ackCh: make(chan byte, 1), // buffered — reader never blocks on this
	}
	e.connection.Store(clientID, entry)
	e.emit(LogLifecycle, "invoke: client %q connected", clientID)
	return entry
}

func (e *Engine) unregister(clientID string) {
	if val, ok := e.connection.LoadAndDelete(clientID); ok {
		entry := val.(*connEntry)
		entry.mu.Lock() // wait for any write to finish first
		err := entry.conn.Close()
		entry.mu.Unlock()
		if err != nil {
			e.emit(LogDropped, "invoke: close error for %q: %v", clientID, err)
			return
		}
		e.emit(LogLifecycle, "invoke: client %q disconnected", clientID)
	}
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

// Timer returns the engine's calibrated fractional timer.
func (e *Engine) Timer() *FracTimer {
	return e.timer
}

func (e *Engine) SilenceTerminal() { e.batcher.Silence() }
func (e *Engine) ResumeTerminal()  { e.batcher.Resume() }
