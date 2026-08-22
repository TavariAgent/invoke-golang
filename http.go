package invoke

import (
	"net/http"
)

// RegisterPage stores an HTML payload under a caller-defined key.
// The key can be any value — a plain identifier or an opaque token.
// Must be called before ServeHTML.
func (e *Engine) RegisterPage(key string, html []byte) {
	if key == "" {
		e.emit(LogDropped, "invoke: RegisterPage — empty key rejected")
		return
	}
	e.pages.Store(key, html)
	e.emit(LogLifecycle, "invoke: page registered under key %q", key)
}

// ServeHTML starts the HTTP server. A page is served only when the
// incoming X-Invoke-Key header matches a key registered via RegisterPage.
// Unmatched or missing keys return 403 — no information about which
// keys exist is ever revealed to the caller.
func (e *Engine) ServeHTML() {
	if e.cfg.HTTPAddr == "" {
		e.emit(LogDropped, "invoke: ServeHTML called but HTTPAddr not set — ignored")
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		key := r.Header.Get("X-Invoke-Key")
		if key == "" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		val, ok := e.pages.Load(key)
		if !ok {
			// always 403 — never reveal whether a key exists or not
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if _, err := w.Write(val.([]byte)); err != nil {
			e.emit(LogDropped, "invoke: HTTP write failed for key %q: %v", key, err)
			return
		}
	})
	e.httpSrv = &http.Server{
		Addr:    e.cfg.HTTPAddr,
		Handler: mux,
	}
	go func() {
		e.emit(LogLifecycle, "invoke: HTTP serving on %s", e.cfg.HTTPAddr)
		if err := e.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			e.emit(LogDropped, "invoke: HTTP server error: %v", err)
		}
	}()
}
