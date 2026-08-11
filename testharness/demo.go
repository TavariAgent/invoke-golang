package main

// demo.go — invoke raw throughput demo
//
// Shows all three pools working under configurable batch load.
// Every task is self-labelled so the output tells you exactly
// what the engine is doing and at what cost.
//
// Usage:
//   go run ./testharness/ demo           — default 10,000 tasks
//   go run ./testharness/ demo 50000     — custom batch size
//   go run ./testharness/ demo ramp      — ramp until ceiling found

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"invoke"
)

// --- Task descriptors -------------------------------------------------------
// Each task carries its own label so the output is self-documenting.

type demoTask struct {
	pool  string
	label string
	fn    func()
}

func buildTasks(n int) []demoTask {
	tasks := make([]demoTask, 0, n)

	// distribute evenly across pools — roughly 60% iPool, 30% hPool, 10% pPool
	for i := range n {
		switch {
		case i%10 == 0:
			tasks = append(tasks, demoTask{
				pool:  "P",
				label: fmt.Sprintf("dns-lookup-%d", i),
				fn: func() {
					// real network IO — this blocks on actual latency
					_, _ = net.LookupHost("localhost")
				},
			})

		case i%3 == 0:
			// range 100_000–1_100_000 — some are prime (expensive), most aren't (cheap)
			// this is the actual harmonic scenario: static difficulty, wildly variable time
			val := 100_000 + rand.Intn(1_000_000)
			tasks = append(tasks, demoTask{
				pool:  "H",
				label: fmt.Sprintf("prime-check-%d", val),
				fn: func() {
					_ = isPrime(val)
				},
			})

		default:
			// unique input every time — no caching, full hash cost each task
			input := fmt.Sprintf("invoke-task-%d-salt-%d", i, time.Now().UnixNano())
			tasks = append(tasks, demoTask{
				pool:  "I",
				label: fmt.Sprintf("sha256(%d)", i),
				fn: func() {
					h := sha256.New()
					for range 100 { // 100 rounds — simulates key-stretching
						h.Write([]byte(input))
					}
					_ = hex.EncodeToString(h.Sum(nil))
				},
			})
		}
	}
	return tasks
}

// --- Prime check — variable cost harmonic work ------------------------------

func isPrime(n int) bool {
	if n < 2 {
		return false
	}
	for i := 2; i*i <= n; i++ {
		if n%i == 0 {
			return false
		}
	}
	return true
}

// --- Run batch --------------------------------------------------------------

type batchResult struct {
	batchSize int
	submitted int
	completed int64
	dropped   int64
	elapsed   time.Duration
	tasksPerS int64
	iCount    int
	hCount    int
	pCount    int
}

func runBatch(factory *invoke.Factory, engine *invoke.Engine, n int) batchResult {
	tasks := buildTasks(n)

	var completed atomic.Int64
	var dropped atomic.Int64
	var wg sync.WaitGroup

	iCount, hCount, pCount := 0, 0, 0
	for _, t := range tasks {
		switch t.pool {
		case "I":
			iCount++
		case "H":
			hCount++
		case "P":
			pCount++
		}
	}

	start := time.Now()

	for _, t := range tasks {
		wg.Add(1)
		fn := func() {
			defer wg.Done()
			t.fn()
			completed.Add(1)
		}

		var err error
		switch t.pool {
		case "I":
			err = factory.AssignI(fn)
		case "H":
			err = factory.AssignH(invoke.Priority{Group: 1, Order: rand.Float64()}, fn)
		case "P":
			err = factory.AssignP(fn)
		}

		if err != nil {
			dropped.Add(1)
			wg.Done()
		}
	}
	wg.Wait()
	elapsed := time.Since(start)
	comp := completed.Load()
	drop := dropped.Load()

	tps := int64(0)
	if elapsed > 0 {
		tps = int64(float64(comp) / elapsed.Seconds())
	}

	return batchResult{
		batchSize: n,
		submitted: n,
		completed: comp,
		dropped:   drop,
		elapsed:   elapsed,
		tasksPerS: tps,
		iCount:    iCount,
		hCount:    hCount,
		pCount:    pCount,
	}
}

// --- Print result -----------------------------------------------------------

func printResult(r batchResult) {
	dropRate := float64(0)
	if r.submitted > 0 {
		dropRate = float64(r.dropped) / float64(r.submitted) * 100
	}

	verdict := "\033[32m✓ clean\033[m"
	if r.dropped > 0 && dropRate < 5 {
		verdict = "\033[33m~ marginal drops\033[m"
	} else if dropRate >= 5 {
		verdict = "\033[31m✗ ceiling hit\033[m"
	}

	fmt.Printf("  batch      : %d tasks\n", r.batchSize)
	fmt.Printf("  pool split : I:%-6d H:%-6d P:%d\n",
		r.iCount, r.hCount, r.pCount)
	fmt.Printf("  completed  : %d\n", r.completed)
	fmt.Printf("  dropped    : %d (%.1f%%)\n", r.dropped, dropRate)

	elapsedStr := r.elapsed.Round(time.Millisecond).String()
	if r.elapsed < time.Millisecond {
		elapsedStr = r.elapsed.Round(time.Microsecond).String()
	}
	fmt.Printf("  elapsed    : %s\n", elapsedStr)
	fmt.Printf("  throughput : \033[1m%d tasks/s\033[m\n", r.tasksPerS)
	fmt.Printf("  verdict    : %s\n\n", verdict)
}

// --- Ramp mode — find the ceiling -------------------------------------------

func runRamp(factory *invoke.Factory, engine *invoke.Engine) {
	fmt.Println("  ramping batch size until ceiling found...\n")
	fmt.Printf("  %-10s %-12s %-10s %-10s %s\n",
		"batch", "throughput", "dropped", "elapsed", "verdict")
	fmt.Printf("  %s\n", strings.Repeat("─", 56))

	sizes := []int{1000, 5000, 10000, 25000, 50000, 100000, 250000, 500000}

	for _, size := range sizes {
		r := runBatch(factory, engine, size)

		dropRate := float64(r.dropped) / float64(r.submitted) * 100
		verdict := "✓"
		color := "\033[32m"
		if dropRate >= 5 {
			verdict = "✗ ceiling"
			color = "\033[31m"
		} else if r.dropped > 0 {
			verdict = "~ marginal"
			color = "\033[33m"
		}

		fmt.Printf("  %-10d %-12d %-10d %-10s %s%s\033[m\n",
			size,
			r.tasksPerS,
			r.dropped,
			r.elapsed.Round(time.Millisecond),
			color, verdict,
		)

		// stop after ceiling is clearly hit
		if dropRate >= 20 {
			fmt.Printf("\n  ceiling found at %d tasks\n", size)
			break
		}

		// brief pause between batches so workers drain
		time.Sleep(500 * time.Millisecond)
	}
}

// --- Entry ------------------------------------------------------------------

func RunDemo(engine *invoke.Engine, factory *invoke.Factory) {
	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║       invoke — raw throughput        ║")
	fmt.Println("╚══════════════════════════════════════╝")
	fmt.Printf("\n  cores     : %d logical\n", runtime.NumCPU())
	fmt.Printf("  iWorkers  : 4 dedicated\n")
	fmt.Printf("  pWorkers  : 2 dedicated\n")
	fmt.Printf("  hWorkers  : %d harmonic (AllocateAll)\n\n",
		runtime.NumCPU()-6)

	// sort task types so output is predictable
	tasks := buildTasks(9)
	fmt.Println("  task types in this demo:")
	seen := map[string]bool{}
	for _, t := range tasks {
		if !seen[t.pool] {
			seen[t.pool] = true
			switch t.pool {
			case "I":
				fmt.Println("  [I] sha256 hash — idempotent, deterministic")
			case "H":
				fmt.Println("  [H] prime check — harmonic, variable cost")
			case "P":
				fmt.Println("  [P] echo ping   — protocol, lightweight")
			}
		}
	}
	// sort seen keys for consistent output
	_ = sort.Search

	fmt.Println()

	args := os.Args
	mode := "default"
	batchSize := 10000

	for i, arg := range args {
		if arg == "demo" {
			if i+1 < len(args) {
				if args[i+1] == "ramp" {
					mode = "ramp"
				} else if n, err := strconv.Atoi(args[i+1]); err == nil {
					batchSize = n
					mode = "single"
				}
			}
			break
		}
	}

	switch mode {
	case "ramp":
		section("ramp mode — finding the ceiling")
		runRamp(factory, engine)

	default:
		if mode == "default" {
			section(fmt.Sprintf("single batch — %d tasks (default)", batchSize))
		} else {
			section(fmt.Sprintf("single batch — %d tasks", batchSize))
		}
		r := runBatch(factory, engine, batchSize)
		printResult(r)
	}

	fmt.Println("✓ demo complete")
}
