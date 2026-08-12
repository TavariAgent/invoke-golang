package main

import (
	"bufio"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"invoke"
)

const testPort = ":18432"

func main() {
	engine := invoke.NewEngine(invoke.Config{
		IWorkers:    4,
		PWorkers:    2,
		Allocate:    invoke.AllocateAll,
		LogEvents:   invoke.LogDropped | invoke.LogLifecycle | invoke.LogCalibrate,
		NotifyDrops: true,
	})
	engine.Start()
	defer engine.Stop()

	factory := invoke.NewFactory(engine)

	if len(os.Args) > 1 && os.Args[1] == "demo" {
		RunDemo(engine, factory)
		return
	}

	if len(os.Args) > 1 && os.Args[1] == "serve" {
		RunServe(engine, factory)
		return
	}

	if len(os.Args) > 1 && os.Args[1] == "drops" {
		RunDropsDemo(engine, factory)
		return
	}

	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║        invoke — test harness         ║")
	fmt.Println("╚══════════════════════════════════════╝")

	section("calibrate")
	rec := engine.CalibrateIdempotent(50000, 2*time.Second)
	fmt.Printf("  throughput  : %d tasks/s\n", rec.Throughput)
	fmt.Printf("  min workers : %d\n", rec.MinWorkers)
	fmt.Printf("  max workers : %d\n\n", rec.MaxWorkers)

	section("protocol pool — TCP handshake")
	testProtocol(factory)

	section("idempotent pool — sig / hash / salt")
	testSigValidator(factory)
	testHashComposer(factory)
	testSaltGenerator(factory)

	section("harmonic pool — fibonacci")
	testFibonacci(factory)

	section("dictionary race — idempotent rows vs keypress speed")
	testDictionaryRace(factory)

	fmt.Println("\n✓ all sections complete")
}

// ── helpers ──────────────────────────────────────────────────────────────────

func section(name string) {
	fmt.Printf("── %s\n", name)
}

// ── protocol ─────────────────────────────────────────────────────────────────

func testProtocol(factory *invoke.Factory) {
	ready := make(chan struct{})

	err := factory.AssignP(func() {
		ln, err := net.Listen("tcp", testPort)
		if err != nil {
			fmt.Printf("  [server] listen error: %v\n", err)
			return
		}
		defer func(ln net.Listener) {
			err := ln.Close()
			if err != nil {
				fmt.Printf("  [server] close error: %v\n", err)
			}
		}(ln)
		close(ready)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func(conn net.Conn) {
			err := conn.Close()
			if err != nil {
				fmt.Printf("  [server] close connection error: %v\n", err)
			}
		}(conn)
		buf := make([]byte, 64)
		n, _ := conn.Read(buf)
		_, err = conn.Write(buf[:n])
		if err != nil {
			return
		}
	})
	if err != nil {
		return
	}

	<-ready

	var wg sync.WaitGroup
	wg.Add(1)
	_ = factory.AssignP(func() {
		defer wg.Done()
		conn, err := net.Dial("tcp", testPort)
		if err != nil {
			fmt.Printf("  [client] dial error: %v\n", err)
			return
		}
		defer func(conn net.Conn) {
			err := conn.Close()
			if err != nil {
				fmt.Printf("  [client] close connection error: %v\n", err)
			}
		}(conn)
		msg := []byte("invoke:handshake")
		_, err = conn.Write(msg)
		if err != nil {
			return
		}
		buf := make([]byte, 64)
		n, _ := conn.Read(buf)
		fmt.Printf("  echo received : %s ✓\n\n", buf[:n])
	})
	wg.Wait()
}

// ── idempotent ────────────────────────────────────────────────────────────────

func testSigValidator(factory *invoke.Factory) {
	key := []byte("invoke-secret")
	msg := []byte("test-payload")
	var wg sync.WaitGroup

	for range 8 {
		wg.Add(1)
		err := factory.AssignI(func() {
			defer wg.Done()
			mac := hmac.New(sha256.New, key)
			mac.Write(msg)
			_ = hex.EncodeToString(mac.Sum(nil))
		})
		if err != nil {
			return
		}
	}
	wg.Wait()
	fmt.Println("  sig validator  : 8x HMAC-SHA256 ✓")
}

func testHashComposer(factory *invoke.Factory) {
	inputs := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	results := make([]string, len(inputs))
	var wg sync.WaitGroup

	for i, input := range inputs {
		wg.Add(1)
		err := factory.AssignI(func() {
			defer wg.Done()
			h := sha256.Sum256([]byte(input))
			results[i] = hex.EncodeToString(h[:8])
		})
		if err != nil {
			return
		}
	}
	wg.Wait()
	fmt.Printf("  hash composer  : %d digests — sample:%s... ✓\n", len(results), results[0])
}

func testSaltGenerator(factory *invoke.Factory) {
	count := 8
	salts := make([]string, count)
	var wg sync.WaitGroup

	for i := range count {
		wg.Add(1)
		err := factory.AssignI(func() {
			defer wg.Done()
			b := make([]byte, 16)
			_, err := rand.Read(b)
			if err != nil {
				return
			}
			salts[i] = hex.EncodeToString(b)
		})
		if err != nil {
			return
		}
	}
	wg.Wait()
	fmt.Printf("  salt generator : %d salts — sample:%s... ✓\n\n", count, salts[0][:16])
}

// ── harmonic — fibonacci ──────────────────────────────────────────────────────

func fib(n int) int {
	if n <= 1 {
		return n
	}
	return fib(n-1) + fib(n-2)
}

func testFibonacci(factory *invoke.Factory) {
	inputs := []int{35, 36, 37, 38}
	results := make([]int, len(inputs))
	var wg sync.WaitGroup

	for i, n := range inputs {
		wg.Add(1)
		err := factory.AssignH(invoke.Priority{Group: 1, Order: float64(i) * 0.1}, func() {
			defer wg.Done()
			start := time.Now()
			results[i] = fib(n)
			fmt.Printf("  fib(%d) = %-12d [%s]\n",
				n, results[i], time.Since(start).Round(time.Millisecond))
		})
		if err != nil {
			return
		}
	}
	wg.Wait()
	fmt.Println()
}

// ── dictionary race ───────────────────────────────────────────────────────────

func loadWords() []string {
	if f, err := os.Open("testharness/words.txt"); err == nil {
		defer func(f *os.File) {
			err := f.Close()
			if err != nil {
				fmt.Printf("  [words] close error: %v\n", err)
			}
		}(f)
		var words []string
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			w := strings.TrimSpace(strings.ToLower(sc.Text()))
			if len(w) > 0 {
				words = append(words, w)
			}
		}
		if len(words) > 0 {
			return words
		}
	}
	// built-in fallback
	return strings.Fields(`ability above accept access account achieve across action
		activity actually address administration admit affect after again age agency
		agree agreement ahead allow almost along already although always amount
		analysis animal another answer appear apply approach area argue around arrive
		attack attention authority available avoid back ball bank base battle become
		benefit better billion black blood blue board break bring business carry case
		cause century certain chance change character child choice choose church city
		claim class clear close community company compare concern condition congress
		consider continue control cost country court cover create crime culture dark
		data daughter deal death decade decision defense degree demand describe design
		detail develop difference difficult direction discover discuss drive during
		early east easy economic education effect effort election else energy enough
		enter environment establish evidence exactly example exist expect experience
		explain fact fall family feel field fight figure fill film find fire follow
		food force foreign forget form forward friend front fund future game give goal
		government great green ground group grow growth hand hang hard head health
		heart help high history hold home hope hour house human identify image imagine
		impact include increase industry information interest issue join keep kill kind
		knowledge land language large later lead learn leave legal less life light like
		likely line listen little live local long look lose loss love maintain major
		make management market matter media member memory mention method middle military
		million mind miss model money month morning move name national natural near
		need network never night note nothing number occur office often open operation
		opportunity option order organization outside pace paper part party pass peace
		perform period person place plan play point police policy political poor position
		possible power practice press prevent price private problem process produce
		program project property protect prove provide public question quickly race
		raise rate reach read ready real reason receive recent record reduce reflect
		relate remain remember report require research resource respond result return
		reveal right rise risk road role rule safe school season security seem send
		sense series serve service sign simple situation skill small something source
		south space speak special spend stand start state stay step still stop strategy
		street strong student study subject summer support system table task team
		technology term theory thing think threat through today together toward town
		trade training travel treat trial trouble true truth turn type understand
		value various victim view violence visit voice vote walk want water week weight
		west while whole window wish within without woman word work world worry write`)
}

func testDictionaryRace(factory *invoke.Factory) {
	words := loadWords()
	fmt.Printf("  dictionary    : %d words loaded\n", len(words))

	// bucket by first letter
	buckets := make(map[byte][]string)
	for _, w := range words {
		if len(w) > 0 {
			buckets[w[0]] = append(buckets[w[0]], w)
		}
	}

	query := "programming"
	pressInterval := 120 * time.Millisecond

	var totalTasks atomic.Int64
	var outpaced atomic.Int64

	fmt.Printf("  query         : %q @ %v/keypress\n\n", query, pressInterval)
	fmt.Printf("  %-14s %-8s %-10s %-10s %s\n", "prefix", "buckets", "matches", "elapsed", "result")
	fmt.Printf("  %s\n", strings.Repeat("─", 56))

	for i := 1; i <= len(query); i++ {
		prefix := query[:i]
		keyTime := time.Now()

		var wg sync.WaitGroup
		var matchCount atomic.Int64

		for _, group := range buckets {
			group, prefix := group, prefix
			wg.Add(1)
			totalTasks.Add(1)
			err := factory.AssignI(func() {
				defer wg.Done()
				for _, w := range group {
					if strings.HasPrefix(w, prefix) {
						matchCount.Add(1)
					}
				}
			})
			if err != nil {
				return
			}
		}

		doneCh := make(chan struct{})
		go func() {
			wg.Wait()
			close(doneCh)
		}()

		time.Sleep(pressInterval)
		elapsed := time.Since(keyTime)

		select {
		case <-doneCh:
			fmt.Printf("  %-14s %-8d %-10d %-10s ✓\n",
				prefix, len(buckets), matchCount.Load(),
				elapsed.Round(time.Millisecond))
		default:
			outpaced.Add(1)
			<-doneCh
			fmt.Printf("  %-14s %-8d %-10d %-10s ⚡ outpaced\n",
				prefix, len(buckets), matchCount.Load(),
				elapsed.Round(time.Millisecond))
		}
	}

	fmt.Printf("\n  total tasks : %d\n", totalTasks.Load())
	fmt.Printf("  outpaced    : %d / %d keypresses\n", outpaced.Load(), len(query))
	if outpaced.Load() == 0 {
		fmt.Println("  verdict     : engine won the race ✓")
	} else {
		fmt.Printf("  verdict     : typing outpaced engine — consider more iWorkers\n")
	}
}
