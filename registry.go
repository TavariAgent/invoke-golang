package invoke

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
)

type CommandFn func(args map[string]string) (any, error)

type command struct {
	name       string
	priority   Priority
	fn         CommandFn
	returnType reflect.Type // add — T locked at registration
}

type CommandTable struct {
	mu       sync.RWMutex
	commands map[string]*command
	engine   *Engine
	factory  *Factory
	sealed   atomic.Bool
}

func NewCommandTable(e *Engine, f *Factory) *CommandTable {
	return &CommandTable{
		commands: make(map[string]*command),
		engine:   e,
		factory:  f,
	}
}

func (ct *CommandTable) Seal() {
	ct.sealed.Store(true)
	ct.engine.emit(LogLifecycle, "invoke: command table sealed")
}

// Register — only callable at startup, never from a connection
func (ct *CommandTable) Register(name string, p Priority, fn CommandFn) {
	if ct.sealed.Load() {
		ct.engine.emit(LogDropped, "invoke: Register called after seal — %q rejected", name)
		return
	}
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.commands[name] = &command{
		name:     name,
		priority: p,
		fn:       fn,
	}
	ct.engine.emit(LogLifecycle, "invoke: command %q registered", name)
}

// Register[T] — generic entry point, T locks the return type into the command
// Call site: invoke.Register[ReportResult](table, "generate-report", p, fn)
func Register[T any](ct *CommandTable, name string, p Priority, fn func(map[string]string) (T, error)) {
	if ct.sealed.Load() {
		ct.engine.emit(LogDropped, "invoke: Register[T] called after seal — %q rejected", name)
		return
	}

	// catch non-serializable T at registration time, not at first request
	var zero T
	if _, err := json.Marshal(zero); err != nil {
		ct.engine.emit(LogDropped,
			"invoke: Register[T] rejected %q — T is not JSON-serializable: %v", name, err)
		return
	}

	ct.mu.Lock()
	defer ct.mu.Unlock()
	t := reflect.TypeFor[T]()
	ct.commands[name] = &command{
		name:     name,
		priority: p,
		fn: func(args map[string]string) (any, error) {
			return fn(args)
		},
		returnType: t,
	}
	ct.engine.emit(LogLifecycle, "invoke: command %q registered [T:%v]", name, t)
}

func (ct *CommandTable) Call(name string, args map[string]string) (any, error) {
	ct.mu.RLock()
	cmd, ok := ct.commands[name]
	ct.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("invoke: unknown command %q", name)
	}

	result := make(chan any, 1)
	errCh := make(chan error, 1)

	err := ct.factory.AssignH(cmd.priority, func() {
		defer func() {
			if r := recover(); r != nil {
				errCh <- fmt.Errorf("invoke: panic in %q: %v", name, r)
			}
		}()
		res, err := cmd.fn(args)
		if err != nil {
			errCh <- err
			return
		}
		result <- res
	})
	if err != nil {
		return nil, err
	}

	select {
	case res := <-result:
		// only finalize if type was declared via Register[T]
		if cmd.returnType != nil {
			return finalize(name, cmd.returnType, res)
		}
		return res, nil
	case err := <-errCh:
		return nil, err
	}
}

func finalize(name string, expected reflect.Type, result any) (any, error) {
	if result == nil {
		switch expected.Kind() {
		case reflect.Pointer, reflect.Interface, reflect.Slice,
			reflect.Map, reflect.Chan, reflect.Func:
			return nil, nil
		default:
			return nil, fmt.Errorf("invoke: %q — nil result for non-nilable type %v", name, expected)
		}
	}
	actual := reflect.TypeOf(result)
	if actual != expected {
		return nil, fmt.Errorf("invoke: %q — type mismatch: got %v expected %v — destroyed",
			name, actual, expected)
	}
	return result, nil
}

// Dispatch receives a packet from the TCP layer and executes the command.
// Type gate: if the command was registered, its types passed at registration.
// Format gate: Args and Payload are the sender's responsibility — we handle anything.
func (ct *CommandTable) Dispatch(clientID string, pkt Packet) {
	ct.mu.RLock()
	cmd, ok := ct.commands[pkt.Command]
	ct.mu.RUnlock()

	if !ok {
		ct.engine.emit(LogDropped, "invoke: dispatch — unknown command %q from %q", pkt.Command, clientID)
		return
	}

	ct.factory.AssignH(cmd.priority, func() {
		result, err := cmd.fn(pkt.Args)
		if err != nil {
			ct.engine.emit(LogDropped, "invoke: dispatch — %q error: %v", pkt.Command, err)
			return
		}
		if result == nil {
			return
		}
		data, err := json.Marshal(result)
		if err != nil {
			ct.engine.emit(LogDropped, "invoke: dispatch — marshal result for %q: %v", pkt.Command, err)
			return
		}
		ct.factory.Push(clientID, Packet{
			Command: pkt.Command,
			Payload: data,
		})
	})
}

// Route sends a packet to any connected client by ID.
// The command table is the routing authority — any client linked to it
// can be reached from any command handler.
func (ct *CommandTable) Route(targetID string, pkt Packet) error {
	return ct.factory.Push(targetID, pkt)
}

func (ct *CommandTable) ServeHTTP(mux *http.ServeMux) {
	// list available commands
	mux.HandleFunc("/commands", func(w http.ResponseWriter, r *http.Request) {
		ct.mu.RLock()
		names := make([]string, 0, len(ct.commands))
		for name := range ct.commands {
			names = append(names, name)
		}
		ct.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(map[string]any{"commands": names})
		if err != nil {
			return
		}
	})

	// call a command
	mux.HandleFunc("/command/", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Path[len("/command/"):]
		if name == "" {
			http.Error(w, "missing command name", http.StatusBadRequest)
			return
		}

		args := make(map[string]string)

		switch r.Method {
		case http.MethodGet:
			for k, v := range r.URL.Query() {
				if len(v) > 0 {
					args[k] = v[0]
				}
			}
		case http.MethodPost:
			if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
				http.Error(w, fmt.Sprintf("invoke: malformed body: %v", err), http.StatusBadRequest)
				return
			}
		default:
			http.Error(w, "invoke: method not allowed — use GET or POST", http.StatusMethodNotAllowed)
			return
		}

		result, err := ct.Call(name, args)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]any{
			"command": name,
			"result":  result,
		})
		if err != nil {
			return
		}
	})
}
