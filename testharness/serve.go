package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"runtime"
	"time"

	"invoke"
)

// --- Typed result structs — T declared here, finalize checks against these ---

type HashResult struct {
	Input  string `json:"input"`
	Digest string `json:"digest"`
	Rounds int    `json:"rounds"`
}

type PrimeResult struct {
	N       int    `json:"n"`
	IsPrime bool   `json:"is_prime"`
	Elapsed string `json:"elapsed"`
}

type PingResult struct {
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
	Worker    string `json:"worker"`
}

type FibResult struct {
	N       int    `json:"n"`
	Value   int    `json:"value"`
	Elapsed string `json:"elapsed"`
}

// --- Server entry point -----------------------------------------------------

func RunServe(engine *invoke.Engine, factory *invoke.Factory) {
	const port = ":29871"

	table := invoke.NewCommandTable(engine, factory)

	// [I] hash — idempotent, deterministic, typed
	invoke.Register[HashResult](table, "hash",
		invoke.Priority{Group: 1},
		func(args map[string]string) (HashResult, error) {
			input := args["input"]
			if input == "" {
				input = "invoke-default"
			}
			h := sha256.New()
			for range 100 {
				h.Write([]byte(input))
			}
			return HashResult{
				Input:  input,
				Digest: hex.EncodeToString(h.Sum(nil)),
				Rounds: 100,
			}, nil
		},
	)

	// [H] prime — harmonic, variable cost, typed
	invoke.Register[PrimeResult](table, "prime",
		invoke.Priority{Group: 2},
		func(args map[string]string) (PrimeResult, error) {
			n := 100_000 + rand.Intn(1_000_000)
			if v, ok := args["n"]; ok {
				_, err := fmt.Sscanf(v, "%d", &n)
				if err != nil {
					return PrimeResult{}, err
				}
			}
			start := time.Now()
			result := isPrime(n)
			return PrimeResult{
				N:       n,
				IsPrime: result,
				Elapsed: time.Since(start).String(),
			}, nil
		},
	)

	// [H] fib — harmonic, truly variable, typed
	invoke.Register[FibResult](table, "fib",
		invoke.Priority{Group: 2},
		func(args map[string]string) (FibResult, error) {
			n := 35
			if v, ok := args["n"]; ok {
				_, err := fmt.Sscanf(v, "%d", &n)
				if err != nil {
					return FibResult{}, err
				}
			}
			if n > 42 {
				n = 42 // safety ceiling — fib blows up fast
			}
			start := time.Now()
			val := fib(n)
			return FibResult{
				N:       n,
				Value:   val,
				Elapsed: time.Since(start).String(),
			}, nil
		},
	)

	// [P] ping — protocol, lightweight, typed
	invoke.Register[PingResult](table, "ping",
		invoke.Priority{Group: 3},
		func(args map[string]string) (PingResult, error) {
			return PingResult{
				Message:   "pong",
				Timestamp: time.Now().Format(time.RFC3339Nano),
				Worker:    fmt.Sprintf("goroutine on %d cores", runtime.NumCPU()),
			}, nil
		},
	)

	// seal — nothing registers after this point
	table.Seal()

	mux := http.NewServeMux()
	table.ServeHTTP(mux)

	// test suite endpoint — fires concurrent group ordering test
	mux.HandleFunc("/test/ordering", func(w http.ResponseWriter, r *http.Request) {
		runOrderingTest(table, factory, engine, w)
	})

	// health
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(map[string]any{
			"status": "ok",
			"cores":  runtime.NumCPU(),
			"sealed": true,
		})
		if err != nil {
			fmt.Println("error:", err)
			return
		}
	})

	fmt.Printf("── invoke server\n")
	fmt.Printf("  listening : http://localhost%s\n\n", port)
	fmt.Printf("  endpoints:\n")
	fmt.Printf("    GET  /health\n")
	fmt.Printf("    GET  /commands\n")
	fmt.Printf("    GET  /command/ping\n")
	fmt.Printf("    GET  /command/hash?input=hello\n")
	fmt.Printf("    GET  /command/prime?n=999983\n")
	fmt.Printf("    GET  /command/fib?n=38\n")
	fmt.Printf("    GET  /test/ordering\n\n")
	fmt.Printf("    curl http://localhost%s/command/ping\n", port)
	fmt.Printf("    curl http://localhost%s/command/prime?n=999983\n", port)
	fmt.Printf("    curl -X POST http://localhost%s/command/prime \\\n", port)
	fmt.Printf("         -H 'Content-Type: application/json' \\\n")
	fmt.Printf("         -d '{\"n\":\"999983\"}'\n")
	fmt.Printf("    curl http://localhost%s/test/ordering\n\n", port)
	fmt.Printf("  try:\n")
	fmt.Printf("    curl http://localhost%s/command/ping\n", port)
	fmt.Printf("    curl http://localhost%s/command/prime?n=999983\n", port)

	err := http.ListenAndServe(port, mux)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
}

// --- Ordering test — fires group 1 and group 3 concurrently ----------------
// group 1 primes cluster at lowest completed_us — they drain first by priority; group 3 pings surface only after all group-1 workers exhaust
func runOrderingTest(table *invoke.CommandTable, factory *invoke.Factory, engine *invoke.Engine, w http.ResponseWriter) {
	engine.Timer.Reset() // fresh origin for this test run
	type entry struct {
		Command     string  `json:"command"`
		Group       int     `json:"group"`
		Order       float64 `json:"order"`
		CompletedUs int64   `json:"completed_us"`
	}

	results := make(chan entry, 25)

	// flood hPool — 20 heavy primes spaced across Group 1
	// Order: 0.05, 0.10, 0.15 ... 1.00 — deep sequence, all group 1
	for i := range 20 {
		order := float64(i+1) * 0.05
		err := factory.AssignH(invoke.Priority{Group: 1, Order: order}, func() {
			isPrime(7_999_999)
			results <- entry{"prime", 1, order, engine.Timer.ReadUs()}
		})
		if err != nil {
			fmt.Println("error:", err)
			return
		}
	}

	for i := range 5 {
		order := float64(i+1) * 0.1
		err := factory.AssignH(invoke.Priority{Group: 3, Order: order}, func() {
			results <- entry{"ping", 3, order, engine.Timer.ReadUs()}
		})
		if err != nil {
			fmt.Println("error:", err)
			return
		}
	}

	collected := make([]entry, 0, 25)
	for range 25 {
		collected = append(collected, <-results)
	}

	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(map[string]any{
		"note":    "group 3 pings should cluster at lowest completed_us — no sleep, just priority",
		"results": collected,
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
}
