package invoke

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
