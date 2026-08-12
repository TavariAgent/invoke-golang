package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"invoke"
)

// ── Typed result structs ────────────────────────────────────────────────────
//
// Every command in this demo uses Register[T], locking T at registration.
// finalize() checks each result against that exact type before it crosses the
// wire. A mismatch destroys the result — the caller gets an error, nothing
// unnarrowed escapes. The types below are what T becomes R for each command.

// MatrixResult carries both the full JSON representation and an embedded CSV
// of the same matrix data. Both formats are filled from the same computation —
// the caller gets the complete picture in one typed response.
type MatrixResult struct {
	Rows    int         `json:"rows"`
	Cols    int         `json:"cols"`
	Values  [][]float64 `json:"values"`
	Op      string      `json:"operation"` // "squared" | "transposed"
	Trace   float64     `json:"trace"`
	Det     float64     `json:"determinant"`
	CSV     string      `json:"csv"` // same matrix, comma-separated rows
	Elapsed string      `json:"elapsed"`
}

// ScatterResult documents which pool each piece of sub-work was routed to,
// and why. The command itself runs in an hPool goroutine. Sub-work is
// dispatched to iPool and pPool explicitly. Each routing decision is recorded.
type ScatterResult struct {
	InputHash   string   `json:"input_hash"`
	MatrixOp    string   `json:"matrix_op"` // ran inline on this hPool goroutine
	PrimeN      int      `json:"prime_n"`
	IsPrime     bool     `json:"is_prime"`
	PoolsRouted []string `json:"pools_routed"` // appended in dispatch order
	Notes       []string `json:"notes"`        // rationale for each pool choice
	Elapsed     string   `json:"elapsed"`
}

// ── Matrix helpers ──────────────────────────────────────────────────────────

func randomMatrix(rows, cols int) [][]float64 {
	m := make([][]float64, rows)
	for i := range m {
		m[i] = make([]float64, cols)
		for j := range m[i] {
			m[i][j] = math.Round((rand.Float64()*20-10)*1000) / 1000
		}
	}
	return m
}

func matTrace(m [][]float64) float64 {
	t := 0.0
	n := min(len(m), len(m[0]))
	for i := range n {
		t += m[i][i]
	}
	return math.Round(t*1000) / 1000
}

// det3x3 uses the Sarrus rule — only valid for 3×3.
func det3x3(m [][]float64) float64 {
	a, b, c := m[0], m[1], m[2]
	d := a[0]*(b[1]*c[2]-b[2]*c[1]) -
		a[1]*(b[0]*c[2]-b[2]*c[0]) +
		a[2]*(b[0]*c[1]-b[1]*c[0])
	return math.Round(d*1000) / 1000
}

// matSquare returns M × M. Assumes square matrix.
func matSquare(m [][]float64) [][]float64 {
	n := len(m)
	out := make([][]float64, n)
	for i := range out {
		out[i] = make([]float64, n)
		for j := range n {
			for k := range n {
				out[i][j] += m[i][k] * m[k][j]
			}
			out[i][j] = math.Round(out[i][j]*1000) / 1000
		}
	}
	return out
}

func matTranspose(m [][]float64) [][]float64 {
	rows, cols := len(m), len(m[0])
	out := make([][]float64, cols)
	for i := range out {
		out[i] = make([]float64, rows)
		for j := range rows {
			out[i][j] = m[j][i]
		}
	}
	return out
}

func matToCSV(m [][]float64) string {
	var sb strings.Builder
	for _, row := range m {
		parts := make([]string, len(row))
		for i, v := range row {
			parts[i] = fmt.Sprintf("%.3f", v)
		}
		sb.WriteString(strings.Join(parts, ","))
		sb.WriteByte('\n')
	}
	return sb.String()
}

// ── RunDropsDemo ────────────────────────────────────────────────────────────

func RunDropsDemo(engine *invoke.Engine, factory *invoke.Factory) {
	const port = ":29872"

	table := invoke.NewCommandTable(engine, factory)

	// matrix-json — hPool, Group 2, Order 0.1
	//
	// Squares a random 3×3 matrix. Returns full typed JSON with embedded CSV.
	// Group 2: runs after Group-1 tasks (highest urgency) drain, before Group-3.
	// Order 0.1 places it at the front of Group 2 relative to matrix-csv.
	invoke.Register[MatrixResult](table, "matrix-json",
		invoke.Priority{Group: 2, Order: 0.1},
		func(args map[string]string) (MatrixResult, error) {
			start := time.Now()
			m := randomMatrix(3, 3)
			sq := matSquare(m)
			return MatrixResult{
				Rows:    3,
				Cols:    3,
				Values:  sq,
				Op:      "squared",
				Trace:   matTrace(sq),
				Det:     det3x3(sq),
				CSV:     matToCSV(sq),
				Elapsed: time.Since(start).String(),
			}, nil
		},
	)

	// matrix-csv — hPool, Group 2, Order 0.2
	//
	// Transposes a random 3×3 matrix. Same Group as matrix-json; Order 0.2
	// places it after Order 0.1 within the group — matrix-json tasks drain
	// before matrix-csv tasks when both are queued concurrently.
	invoke.Register[MatrixResult](table, "matrix-csv",
		invoke.Priority{Group: 2, Order: 0.2},
		func(args map[string]string) (MatrixResult, error) {
			start := time.Now()
			m := randomMatrix(3, 3)
			t := matTranspose(m)
			return MatrixResult{
				Rows:    3,
				Cols:    3,
				Values:  t,
				Op:      "transposed",
				Trace:   matTrace(t),
				Det:     det3x3(t),
				CSV:     matToCSV(t),
				Elapsed: time.Since(start).String(),
			}, nil
		},
	)

	// scatter — hPool, Group 1, Order 0.1 (highest urgency in this demo)
	//
	// This command runs inside an hPool worker goroutine. From that goroutine
	// it explicitly routes sub-work to iPool and pPool. Each internal
	// submission states its pool, priority (where applicable), and reason.
	//
	// Routing map:
	//   iPool        — SHA-256 hash   — idempotent, deterministic, no side effects
	//   pPool        — simulated IO   — blocked goroutine must not hold a CPU slot
	//   hPool inline — matrix square  — runs in THIS goroutine, no extra submission
	//
	// The matrix op is intentionally NOT re-submitted to hPool. Submitting back
	// to hPool from within an hPool command and then blocking on the result holds
	// this worker slot while waiting for another slot to free — a deadlock risk
	// under full saturation. See /test/parity for the Group/Order side of this.
	invoke.Register[ScatterResult](table, "scatter",
		invoke.Priority{Group: 1, Order: 0.1},
		func(args map[string]string) (ScatterResult, error) {
			start := time.Now()
			input := args["input"]
			if input == "" {
				input = "invoke-scatter"
			}

			var (
				mu          sync.Mutex
				wg          sync.WaitGroup
				hashDigest  string
				primeN      = 999_983
				isPrimeVal  bool
				poolsRouted []string
				notes       []string
			)

			// iPool: hash is idempotent — same input always produces the same
			// digest, safe to retry on failure, no system state modified.
			// AssignI takes no Priority — iPool is FIFO, not priority-scheduled.
			wg.Add(1)
			factory.AssignI(func() {
				defer wg.Done()
				h := sha256.Sum256([]byte(input))
				mu.Lock()
				hashDigest = hex.EncodeToString(h[:])
				poolsRouted = append(poolsRouted, "iPool → SHA-256 hash")
				notes = append(notes, "iPool: deterministic output, safe to retry — no Priority needed (FIFO)")
				mu.Unlock()
			})

			// pPool: simulates IO-class work. A real command might do DNS
			// resolution or a TCP health check here. The sleep models the
			// blocked-goroutine pattern. The critical property: a goroutine
			// blocking here does not occupy an iPool or hPool worker slot.
			// Compute-heavy work does NOT belong in pPool in production.
			wg.Add(1)
			factory.AssignP(func() {
				defer wg.Done()
				time.Sleep(2 * time.Millisecond) // simulate syscall block
				mu.Lock()
				isPrimeVal = isPrime(primeN)
				poolsRouted = append(poolsRouted, "pPool → 2ms IO sim + prime check")
				notes = append(notes, "pPool: isolated — blocked goroutine cannot starve iPool or hPool workers")
				mu.Unlock()
			})

			wg.Wait()

			// hPool (inline): matrix op runs directly in this goroutine — the
			// same hPool worker executing this command. No submission needed.
			// This is correct: if we re-submitted to hPool and blocked on the
			// result, this slot would be held while waiting for another to free.
			m := randomMatrix(3, 3)
			sq := matSquare(m)
			matOp := fmt.Sprintf(
				"3×3 squared — trace:%.3f  det:%.3f\n%s",
				matTrace(sq), det3x3(sq), matToCSV(sq),
			)

			mu.Lock()
			poolsRouted = append(poolsRouted, "hPool (inline) → 3×3 matrix square")
			notes = append(notes, "hPool inline: avoids holding this slot while waiting for a second hPool slot")
			mu.Unlock()

			return ScatterResult{
				InputHash:   hashDigest,
				MatrixOp:    matOp,
				PrimeN:      primeN,
				IsPrime:     isPrimeVal,
				PoolsRouted: poolsRouted,
				Notes:       notes,
				Elapsed:     time.Since(start).String(),
			}, nil
		},
	)

	table.Seal()

	mux := http.NewServeMux()
	table.ServeHTTP(mux)

	mux.HandleFunc("/test/drops", func(w http.ResponseWriter, r *http.Request) {
		runDropsTest(factory, engine, w)
	})

	mux.HandleFunc("/test/parity", func(w http.ResponseWriter, r *http.Request) {
		runParityTest(factory, engine, w)
	})

	fmt.Printf("── invoke drops demo\n")
	fmt.Printf("  port : http://localhost%s\n\n", port)
	fmt.Printf("  GET /commands\n")
	fmt.Printf("  GET /command/matrix-json\n")
	fmt.Printf("  GET /command/matrix-csv\n")
	fmt.Printf("  GET /command/scatter?input=hello\n")
	fmt.Printf("  GET /test/drops       — batch flood + intentional drops\n")
	fmt.Printf("  GET /test/parity      — stomp condition with identical Group/Order\n\n")
	fmt.Printf("  curl http://localhost%s/command/scatter?input=invoke\n", port)
	fmt.Printf("  curl http://localhost%s/test/drops\n", port)
	fmt.Printf("  curl http://localhost%s/test/parity\n\n", port)

	http.ListenAndServe(port, mux)
}

// runDropsTest ──────────────────────────────────────────────────────────────
//
// Submits two batches of valid hPool tasks alongside intentionally malformed
// submissions. The malformed ones are rejected at factory validation, routed
// through emit(LogDropped) into the batcher, and flushed to a timestamped log
// file. With NotifyDrops:true the log path appears in the terminal.
//
// Group 1 (squared) drains completely before Group 3 (transposed) begins —
// phase ordering across a concurrent batch.
func runDropsTest(factory *invoke.Factory, engine *invoke.Engine, w http.ResponseWriter) {
	start := time.Now()

	const (
		jsonBatch = 15 // Group 1 — squared, higher priority, drains first
		csvBatch  = 15 // Group 3 — transposed, lower priority, drains after
		nilDrops  = 5  // nil fn → ErrNilTask
		badDrops  = 5  // negative Group/Order → ErrInvalidPriority
	)

	type taggedResult struct {
		Command string       `json:"command"`
		Result  MatrixResult `json:"result"`
	}

	resultCh := make(chan taggedResult, jsonBatch+csvBatch)
	var wg sync.WaitGroup
	var dropped atomic.Int64

	// Batch A: squared matrices at Group 1 — highest priority, complete first.
	// Each task gets a distinct Order so the heap sequences them deterministically.
	for i := range jsonBatch {
		wg.Add(1)
		order := float64(i+1) * 0.05
		factory.AssignH(invoke.Priority{Group: 1, Order: order}, func() {
			defer wg.Done()
			m := randomMatrix(3, 3)
			sq := matSquare(m)
			resultCh <- taggedResult{
				Command: "matrix-json",
				Result: MatrixResult{
					Rows: 3, Cols: 3, Values: sq, Op: "squared",
					Trace: matTrace(sq), Det: det3x3(sq), CSV: matToCSV(sq),
				},
			}
		})
	}

	// Batch B: transposed matrices at Group 3 — held in the heap until all
	// Group 1 tasks are exhausted.
	for i := range csvBatch {
		wg.Add(1)
		order := float64(i+1) * 0.05
		factory.AssignH(invoke.Priority{Group: 3, Order: order}, func() {
			defer wg.Done()
			m := randomMatrix(3, 3)
			t := matTranspose(m)
			resultCh <- taggedResult{
				Command: "matrix-csv",
				Result: MatrixResult{
					Rows: 3, Cols: 3, Values: t, Op: "transposed",
					Trace: matTrace(t), Det: det3x3(t), CSV: matToCSV(t),
				},
			}
		})
	}

	// Intentional bad submissions — exercise the drop batcher.
	// factory.validate() and validatePriority() reject these immediately,
	// call emit(LogDropped, ...), which routes to the batcher via logCh.
	// The batcher accumulates entries and flushes to a timestamped file.
	for range nilDrops {
		if err := factory.AssignI(nil); err != nil {
			dropped.Add(1) // ErrNilTask
		}
	}
	for range badDrops {
		if err := factory.AssignH(invoke.Priority{Group: -1, Order: -0.1}, func() {}); err != nil {
			dropped.Add(1) // ErrInvalidPriority
		}
	}

	wg.Wait()
	close(resultCh)

	var jsonResults []taggedResult
	var csvResults []taggedResult
	csvSample := ""

	for r := range resultCh {
		switch r.Command {
		case "matrix-json":
			jsonResults = append(jsonResults, r)
		case "matrix-csv":
			csvResults = append(csvResults, r)
			if csvSample == "" {
				csvSample = r.Result.CSV
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"submitted":    jsonBatch + csvBatch + nilDrops + badDrops,
		"completed":    jsonBatch + csvBatch,
		"dropped":      dropped.Load(),
		"drop_note":    "nil fn and negative priority rejected at factory — logged to batcher; check terminal for log path (NotifyDrops:true)",
		"json_results": jsonResults,
		"csv_results":  csvResults,
		"csv_sample":   csvSample,
		"elapsed":      time.Since(start).String(),
	})
}

// runParityTest ─────────────────────────────────────────────────────────────
//
// The parity/stomp condition: the hPool heap sorts tasks by Group then Order.
// When two tasks share identical Group AND Order, the heap cannot differentiate
// them. Execution order between them is undefined — whichever worker pops next
// gets one, but there is no guarantee which one.
//
// This is the only current "stomp condition" in the system: a task that should
// be sequenced ends up running in arbitrary order relative to its peers. It
// silently breaks any pipeline assumption the caller was relying on.
//
// The most common source of this bug:
//
//	goroutines spawned inside a command handler submit back to hPool using the
//	default zero-value Priority{} — Group:0, Order:0.0. All of them land at
//	the same heap coordinate. The heap cannot phase them. They stomp.
//
// The fix is always the same:
//
//	assign distinct Order values to any tasks that must run in a defined
//	sequence, even if they share a Group.
//
// Set A (Group 1, distinct Order): runs first, ordering is deterministic.
// Set B (Group 3, all Order 0.0): runs after Set A exhausts, ordering undefined.
func runParityTest(factory *invoke.Factory, engine *invoke.Engine, w http.ResponseWriter) {
	type parityEntry struct {
		Index       int     `json:"index"`
		Group       int     `json:"group"`
		Order       float64 `json:"order"`
		CompletedUs int64   `json:"completed_us"`
		Condition   string  `json:"condition"`
	}

	engine.Timer.Reset()
	results := make(chan parityEntry, 10)
	var wg sync.WaitGroup

	// Set A — Group 1 (highest priority), Orders 0.0 through 0.4.
	// Each task is distinguishable. The heap sequences them by Order.
	// Completion timestamps should reflect this ordering.
	for i := range 5 {
		i := i
		wg.Add(1)
		factory.AssignH(invoke.Priority{Group: 1, Order: float64(i) * 0.1}, func() {
			defer wg.Done()
			time.Sleep(500 * time.Microsecond)
			results <- parityEntry{
				Index:       i,
				Group:       1,
				Order:       float64(i) * 0.1,
				CompletedUs: engine.Timer.ReadUs(),
				Condition:   "distinct Order — sequence is deterministic within group",
			}
		})
	}

	// Set B — Group 3 (lowest priority), all at Order 0.0.
	// Group 3 begins only after all Group 1 tasks complete — the group
	// boundary is fine. The problem is within this set: five tasks at
	// identical coordinates. The heap picks whichever it sees first under
	// its internal ordering, which under concurrent push is effectively
	// arbitrary. Completion order will not reliably match index order.
	for i := range 5 {
		i := i
		wg.Add(1)
		factory.AssignH(invoke.Priority{Group: 3, Order: 0.0}, func() {
			defer wg.Done()
			time.Sleep(500 * time.Microsecond)
			results <- parityEntry{
				Index:       i + 5,
				Group:       3,
				Order:       0.0,
				CompletedUs: engine.Timer.ReadUs(),
				Condition:   "PARITY — identical Group+Order, sequence undefined",
			}
		})
	}

	wg.Wait()
	close(results)

	collected := make([]parityEntry, 0, 10)
	for e := range results {
		collected = append(collected, e)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"parity_condition": "tasks with identical Group+Order are indistinguishable to the heap",
		"consequence":      "execution order between equal-priority tasks is undefined — pipeline assumptions silently break",
		"common_source":    "goroutines inside a command handler that submit to hPool with default Priority{} all land at Group:0 Order:0.0 — immediate parity across all of them",
		"fix":              "always assign distinct Order values to tasks that must run in a defined sequence, even within the same Group",
		"results":          collected,
	})
}
