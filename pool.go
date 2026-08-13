package invoke

import (
	"container/heap"
	"sync"
	"sync/atomic"
)

// --- Internal Task Types ----------------------------------------------------

type iTask struct{ fn func() }
type pTask struct{ fn func() }
type hTask struct {
	fn       func()
	priority Priority
}

func (t iTask) run() { t.fn() }
func (t pTask) run() { t.fn() }
func (t hTask) run() { t.fn() }

type rTask struct{ fn func() }

func (t rTask) run() { t.fn() }

// --- Priority ---------------------------------------------------------------

type Priority struct {
	Group int
	Order float64
}

// --- hTask Heap (priority queue) --------------------------------------------

type hTaskHeap []hTask

func (h *hTaskHeap) Len() int      { return len(*h) }
func (h *hTaskHeap) Swap(i, j int) { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }
func (h *hTaskHeap) Less(i, j int) bool {
	if (*h)[i].priority.Group != (*h)[j].priority.Group {
		return (*h)[i].priority.Group < (*h)[j].priority.Group
	}
	return (*h)[i].priority.Order < (*h)[j].priority.Order
}
func (h *hTaskHeap) Push(x any) { *h = append(*h, x.(hTask)) }
func (h *hTaskHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	*h = old[:n-1]
	return t
}

// --- iPool — dedicated idempotent workers -----------------------------------

type iPool struct {
	mu       sync.Mutex
	cond     *sync.Cond
	queue    []iTask
	stopCh   chan struct{}
	engine   *Engine
	dropped  atomic.Int64 // standard lane drops
	rDropped atomic.Int64 // iCore receipt lane drops

	// iCore — command receipt lane
	rMu    sync.Mutex
	rCond  *sync.Cond
	rQueue []rTask
}

func (p *iPool) start(n int) {
	for range n {
		go p.run()
	}
	p.engine.emit(LogLifecycle, "invoke: iPool started — workers:%d", n)
}

func newIPool(n int, e *Engine) *iPool {
	p := &iPool{
		stopCh: make(chan struct{}),
		engine: e,
	}
	p.cond = sync.NewCond(&p.mu)
	p.rCond = sync.NewCond(&p.rMu)
	return p
}

func (p *iPool) startCores(n int) {
	for range n {
		go p.runCore()
	}
	p.engine.emit(LogLifecycle, "invoke: iCore started — workers:%d", n)
}

func (p *iPool) submitR(t rTask) bool {
	p.rMu.Lock()
	p.rQueue = append(p.rQueue, t)
	p.rMu.Unlock()
	p.rCond.Signal()
	return true
}

func (p *iPool) runCore() {
	for {
		p.rMu.Lock()
		for len(p.rQueue) == 0 {
			select {
			case <-p.stopCh:
				p.rMu.Unlock()
				return
			default:
				p.rCond.Wait()
			}
		}
		t := p.rQueue[0]
		p.rQueue = p.rQueue[1:]
		p.rMu.Unlock()
		t.run()
	}
}

func (p *iPool) submit(t iTask) bool {
	p.mu.Lock()
	p.queue = append(p.queue, t)
	p.mu.Unlock()
	p.cond.Signal()
	return true
}

func (p *iPool) run() {
	for {
		p.mu.Lock()
		for len(p.queue) == 0 {
			select {
			case <-p.stopCh:
				p.mu.Unlock()
				return
			default:
				p.cond.Wait()
			}
		}
		t := p.queue[0]
		p.queue = p.queue[1:]
		p.mu.Unlock()
		t.run()
	}
}

func (p *iPool) stop() {
	close(p.stopCh)
	p.cond.Broadcast()
	p.rCond.Broadcast()
	p.engine.emit(LogLifecycle, "invoke: iPool stopped")
}

// --- pPool — dedicated protocol workers -------------------------------------

type pPool struct {
	mu      sync.Mutex
	cond    *sync.Cond
	queue   []pTask
	stopCh  chan struct{}
	engine  *Engine
	dropped atomic.Int64
}

func newPPool(n int, e *Engine) *pPool {
	p := &pPool{
		stopCh: make(chan struct{}),
		engine: e,
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

func (p *pPool) start(n int) {
	for range n {
		go p.run()
	}
	p.engine.emit(LogLifecycle, "invoke: pPool started — workers:%d", n)
}

func (p *pPool) submit(t pTask) bool {
	p.mu.Lock()
	p.queue = append(p.queue, t)
	p.mu.Unlock()
	p.cond.Signal()
	return true
}

func (p *pPool) run() {
	for {
		p.mu.Lock()
		for len(p.queue) == 0 {
			select {
			case <-p.stopCh:
				p.mu.Unlock()
				return
			default:
				p.cond.Wait()
			}
		}
		t := p.queue[0]
		p.queue = p.queue[1:]
		p.mu.Unlock()
		t.run()
	}
}

func (p *pPool) stop() {
	close(p.stopCh)
	p.cond.Broadcast()
	p.engine.emit(LogLifecycle, "invoke: pPool stopped")
}

// --- hPool — harmonic priority-scheduled workers ----------------------------

type hPool struct {
	mu      sync.Mutex
	cond    *sync.Cond
	queue   hTaskHeap
	stopCh  chan struct{}
	engine  *Engine
	dropped atomic.Int64
}

func newHPool(e *Engine) *hPool {
	p := &hPool{
		stopCh: make(chan struct{}),
		engine: e,
	}
	p.cond = sync.NewCond(&p.mu)
	heap.Init(&p.queue)
	return p
}

func (p *hPool) start(n int) {
	for range n {
		go p.run()
	}
	p.engine.emit(LogLifecycle, "invoke: hPool started — workers:%d", n)
}

func (p *hPool) submit(t hTask) {
	p.mu.Lock()
	heap.Push(&p.queue, t)
	p.mu.Unlock()
	p.cond.Signal()
}

func (p *hPool) run() {
	for {
		p.mu.Lock()
		for p.queue.Len() == 0 {
			select {
			case <-p.stopCh:
				p.mu.Unlock()
				return
			default:
				p.cond.Wait()
			}
		}
		t := heap.Pop(&p.queue).(hTask)
		p.mu.Unlock()
		t.run()
	}
}

func (p *hPool) stop() {
	close(p.stopCh)
	p.cond.Broadcast()
	p.engine.emit(LogLifecycle, "invoke: hPool stopped")
}
