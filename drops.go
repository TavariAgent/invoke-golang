package invoke

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// --- Drop Batcher -----------------------------------------------------------

type dropBatcher struct {
	mu       sync.Mutex
	entries  []string
	timer    *time.Timer
	timeout  time.Duration
	logDir   string
	printer  func(string)
	silenced atomic.Bool
}

func (b *dropBatcher) Silence() { b.silenced.Store(true) }
func (b *dropBatcher) Resume()  { b.silenced.Store(false) }

func newDropBatcher(cfg Config) *dropBatcher {
	var printer func(string)
	if cfg.NotifyDrops {
		printer = func(msg string) { fmt.Println(msg) }
	} else {
		printer = func(string) {} // silent unless opted in
	}
	return &dropBatcher{
		timeout: 2 * time.Second,
		logDir:  cfg.LogDir,
		printer: printer,
	}
}

func (b *dropBatcher) add(msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries = append(b.entries, msg)
	if b.timer != nil {
		b.timer.Reset(b.timeout)
		return
	}
	b.timer = time.AfterFunc(b.timeout, b.flush)
}

func (b *dropBatcher) flush() {
	b.mu.Lock()
	entries := b.entries
	b.entries = nil
	b.timer = nil
	b.mu.Unlock()

	if len(entries) == 0 {
		return
	}

	path, err := b.writeLog(entries)
	if err != nil || b.silenced.Load() {
		return
	}
	b.printer(fmt.Sprintf("[invoke] %d drops logged → %s", len(entries), path))
}

func (b *dropBatcher) flushNow() {
	if b.timer != nil {
		b.timer.Stop()
	}
	b.flush()
}

func (b *dropBatcher) writeLog(entries []string) (string, error) {
	dir := b.logDir
	if dir == "" {
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		dir = filepath.Join(wd, "invoke-logs")
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}

	ts := time.Now().Format("2006-01-02-150405")
	path := filepath.Join(dir, fmt.Sprintf("drops-%s.log", ts))

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("invoke drop log — %s\n", time.Now().Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("total dropped: %d\n", len(entries)))
	sb.WriteString(strings.Repeat("─", 48) + "\n")
	for i, e := range entries {
		sb.WriteString(fmt.Sprintf("%04d  %s\n", i+1, e))
	}

	return path, os.WriteFile(path, []byte(sb.String()), 0644)
}

func (e *Engine) runDropFlusher() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.flushDropCounts()
		case <-e.stopCh:
			e.flushDropCounts()
			return
		}
	}
}

func (e *Engine) flushDropCounts() {
	iDrops := e.iPool.dropped.Swap(0)
	rDrops := e.iPool.rDropped.Swap(0) // iCore lane tracked separately
	pDrops := e.pPool.dropped.Swap(0)
	hDrops := e.hPool.dropped.Swap(0)

	if iDrops > 0 {
		e.batcher.add(fmt.Sprintf("iPool dropped %d tasks", iDrops))
	}
	if rDrops > 0 {
		e.batcher.add(fmt.Sprintf("iCore dropped %d commands", rDrops))
	}
	if pDrops > 0 {
		e.batcher.add(fmt.Sprintf("pPool dropped %d tasks", pDrops))
	}
	if hDrops > 0 {
		e.batcher.add(fmt.Sprintf("hPool dropped %d tasks", hDrops))
	}
}
