package invoke

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"
)

// --- Factory ----------------------------------------------------------------

type Factory struct {
	engine *Engine
}

func NewFactory(e *Engine) *Factory {
	return &Factory{engine: e}
}

// AssignI submits an idempotent task to the dedicated iPool
func (f *Factory) AssignI(fn func()) error {
	if err := f.validate(fn); err != nil {
		return err
	}
	f.engine.iPool.submit(iTask{fn: fn})
	return nil
}

// AssignR submits a command receipt task to the iCore lane
func (f *Factory) AssignR(fn func()) error {
	if err := f.validate(fn); err != nil {
		return err
	}
	f.engine.iPool.submitR(rTask{fn: fn})
	return nil
}

// AssignP submits a protocol task to the dedicated pPool
func (f *Factory) AssignP(fn func()) error {
	if err := f.validate(fn); err != nil {
		return err
	}
	f.engine.pPool.submit(pTask{fn: fn})
	return nil
}

// AssignH submits a harmonic task to the priority-scheduled hPool
func (f *Factory) AssignH(p Priority, fn func()) error {
	if err := f.validate(fn); err != nil {
		return err
	}
	if err := f.validatePriority(p); err != nil {
		f.engine.emit(LogDropped, "invoke: hTask dropped — %v", err)
		return err
	}
	f.engine.hPool.submit(hTask{fn: fn, priority: p})
	return nil
}

// --- Validation — nothing dirty enters the engine --------------------------

func (f *Factory) validate(fn func()) error {
	if !f.engine.running.Load() {
		f.engine.emit(LogDropped, "invoke: task dropped — %v", ErrEngineNotRunning)
		return ErrEngineNotRunning
	}
	if fn == nil {
		f.engine.emit(LogDropped, "invoke: task dropped — %v", ErrNilTask)
		return ErrNilTask
	}
	return nil
}

func (f *Factory) validatePriority(p Priority) error {
	if p.Group < 0 || p.Order < 0.0 {
		return ErrInvalidPriority
	}
	return nil
}

func (f *Factory) Push(clientID string, pkt Packet) error {
	val, ok := f.engine.connection.Load(clientID)
	if !ok {
		f.engine.emit(LogDropped, "invoke: push — %q not connected", clientID)
		return fmt.Errorf("invoke: client %q not connected", clientID)
	}
	entry := val.(*connEntry)

	pkt.Ts = f.engine.timer.ReadUs()
	data, err := json.Marshal(pkt)
	if err != nil {
		return fmt.Errorf("invoke: push marshal: %w", err)
	}

	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(data)))
	copy(buf[4:], data)

	return f.AssignP(func() {
		entry.mu.Lock()
		defer entry.mu.Unlock()
		f.send(entry, buf)
	})
}

func (f *Factory) send(entry *connEntry, buf []byte) {
	if _, err := entry.conn.Write(buf); err != nil {
		f.engine.emit(LogDropped, "invoke: write failed: %v", err)
		return
	}
	select {
	case ack := <-entry.ackCh:
		if ack == 0 {
			f.engine.emit(LogDropped, "invoke: ack=0 — resending once")
			if _, err := entry.conn.Write(buf); err != nil {
				f.engine.emit(LogDropped, "invoke: resend failed (ack=0): %v", err)
			}
		}
	case <-time.After(ackTimeout):
		f.engine.emit(LogDropped, "invoke: ack timeout — resending once")
		if _, err := entry.conn.Write(buf); err != nil {
			f.engine.emit(LogDropped, "invoke: resend failed (timeout): %v", err)
		}
	}
}
