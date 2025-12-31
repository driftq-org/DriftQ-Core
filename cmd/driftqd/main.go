package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
	v1 "github.com/driftq-org/DriftQ-Core/internal/httpapi/v1"
	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

var (
	buildVersion = "dev"
	buildCommit  = "unknown"
)

type server struct {
	broker broker.Broker
}

type TestRouter struct{}

type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusRecorder) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusRecorder) Write(p []byte) (int, error) {
	// If WriteHeader wasn't called, net/http assumes 200.
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += n
	return n, err
}

func newRequestID() string {
	// 12 bytes => 24 hex chars (plenty for logs)
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		// extremely unlikely; fall back to timestamp-ish
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func remoteIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		return host
	}
	return addr
}

func withRequestLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		reqID := strings.TrimSpace(r.Header.Get("X-Request-Id"))
		if reqID == "" {
			reqID = newRequestID()
		}
		// echo back so clients can correlate
		w.Header().Set("X-Request-Id", reqID)

		rec := &statusRecorder{ResponseWriter: w}

		defer func() {
			dur := time.Since(start)

			logFn := slog.Info
			if rec.status >= 500 {
				logFn = slog.Error
			}

			logFn("http",
				"req_id", reqID,
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"bytes", rec.bytes,
				"duration_ms", dur.Milliseconds(),
				"remote_ip", remoteIP(r.RemoteAddr),
			)
		}()

		next.ServeHTTP(rec, r)
	})
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "info", "":
		fallthrough
	default:
		return slog.LevelInfo
	}
}

func normalizeOr(s, def string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return def
	}
	return s
}

func configureLogger(levelStr, formatStr string) *slog.Logger {
	level := parseLogLevel(levelStr)
	format := strings.ToLower(strings.TrimSpace(formatStr))
	if format == "" {
		format = "text"
	}

	opts := &slog.HandlerOptions{Level: level}

	var h slog.Handler
	switch format {
	case "json":
		h = slog.NewJSONHandler(os.Stderr, opts)
	case "text":
		fallthrough
	default:
		h = slog.NewTextHandler(os.Stderr, opts)
	}

	version := normalizeOr(buildVersion, "dev")
	commit := normalizeOr(buildCommit, "unknown")

	l := slog.New(h).With(
		"service", "driftqd",
		"version", version,
		"commit", commit,
	)

	slog.SetDefault(l)
	return l
}

func fatal(msg string, err error) {
	if err != nil {
		slog.Error(msg, "err", err)
	} else {
		slog.Error(msg)
	}
	os.Exit(1)
}

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	walPath := flag.String("wal", "driftq.wal", "path to WAL file")
	resetWAL := flag.Bool("reset-wal", false, "reset WAL by moving existing file aside (creates a .bak.<ts> file)")

	logLevel := flag.String("log-level", "info", "log level: debug|info|warn|error")
	logFormat := flag.String("log-format", "text", "log format: text|json")

	flag.Parse()

	logger := configureLogger(*logLevel, *logFormat)

	// Optional safe reset: move existing WAL aside so we start fresh
	if *resetWAL {
		if _, err := os.Stat(*walPath); err == nil {
			bak := fmt.Sprintf("%s.bak.%d", *walPath, time.Now().Unix())
			if err := os.Rename(*walPath, bak); err != nil {
				fatal("failed to reset WAL (rename)", err)
			}
			slog.Info("WAL reset", "from", *walPath, "to", bak)
		} else if !errors.Is(err, os.ErrNotExist) {
			fatal("failed to stat WAL", err)
		}
	}

	wal, err := storage.OpenFileWAL(*walPath)
	if err != nil {
		fatal("failed to open WAL", err)
	}
	defer wal.Close()

	b, err := broker.NewInMemoryBrokerFromWAL(wal)
	if err != nil {
		fatal("failed to replay WAL", err)
	}

	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	b.StartRedeliveryLoop(appCtx)
	b.SetRouter(TestRouter{})

	s := &server{broker: b}

	rootMux := http.NewServeMux()
	v1Mux := http.NewServeMux()

	// v1 routes
	v1Mux.HandleFunc("/healthz", s.requireMethod(http.MethodGet)(s.handleHealthz))
	v1Mux.HandleFunc("/produce", s.requireMethod(http.MethodPost)(s.handleProduce))
	v1Mux.HandleFunc("/consume", s.requireMethod(http.MethodGet)(s.handleConsume))
	v1Mux.HandleFunc("/ack", s.requireMethod(http.MethodPost)(s.handleAck))
	v1Mux.HandleFunc("/nack", s.requireMethod(http.MethodPost)(s.handleNack))
	v1Mux.HandleFunc("/topics", s.method(s.handleTopicsList, s.handleTopicsCreate))
	v1Mux.HandleFunc("/version", s.requireMethod(http.MethodGet)(s.handleVersion))

	// mount v1 under /v1/*
	rootMux.Handle("/v1/", http.StripPrefix("/v1", v1Mux))

	// block unversioned routes
	rootMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		v1.WriteError(w, http.StatusNotFound, "NOT_FOUND", "use /v1/* endpoints")
	})

	handler := withRequestLogging(rootMux)

	srv := &http.Server{
		Addr:         *addr,
		Handler:      handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 0,
		ErrorLog:     slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	slog.Info("broker starting", "addr", *addr)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fatal("http server error", err)
		}
	}()

	// Shutdown stuff
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	slog.Info("shutting down")
	appCancel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("http shutdown error", "err", err)
	}

	slog.Info("broker stopped")
}

// requireMethod wraps a handler and rejects non-allowed methods with JSON 405 + Allow
func (s *server) requireMethod(allowed ...string) func(http.HandlerFunc) http.HandlerFunc {
	allowSet := map[string]struct{}{}
	for _, m := range allowed {
		allowSet[m] = struct{}{}
	}
	allowHeader := strings.Join(allowed, ", ")

	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if _, ok := allowSet[r.Method]; !ok {
				if allowHeader != "" {
					w.Header().Set("Allow", allowHeader)
				}

				v1.WriteError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
				return
			}
			next(w, r)
		}
	}
}

// method dispatches GET/POST with proper Allow header and JSON 405
func (s *server) method(get, post http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if get == nil {
				w.Header().Set("Allow", http.MethodPost)
				v1.WriteError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
				return
			}
			get(w, r)
		case http.MethodPost:
			if post == nil {
				w.Header().Set("Allow", http.MethodGet)
				v1.WriteError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
				return
			}
			post(w, r)

		default:
			allow := make([]string, 0, 2)
			if get != nil {
				allow = append(allow, http.MethodGet)
			}

			if post != nil {
				allow = append(allow, http.MethodPost)
			}

			if len(allow) > 0 {
				w.Header().Set("Allow", strings.Join(allow, ", "))
			}
			v1.WriteError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		}
	}
}

func (s *server) handleVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		v1.MethodNotAllowed(w, http.MethodGet)
		return
	}

	version := strings.TrimSpace(buildVersion)
	if version == "" {
		version = "dev"
	}

	commit := strings.TrimSpace(buildCommit)
	if commit == "" {
		commit = "unknown"
	}

	type walEnabled interface{ WALEnabled() bool }
	walOn := false
	if b, ok := any(s.broker).(walEnabled); ok {
		walOn = b.WALEnabled()
	}

	v1.WriteJSON(w, http.StatusOK, v1.VersionResponse{
		Version:    version,
		Commit:     commit,
		WalEnabled: walOn,
	})
}

func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		v1.MethodNotAllowed(w, http.MethodGet)
		return
	}

	v1.WriteJSON(w, http.StatusOK, v1.HealthzResponse{Status: "ok"})
}

func (s *server) handleTopicsList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	topics, err := s.broker.ListTopics(ctx)
	if err != nil {
		v1.WriteError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}

	v1.WriteJSON(w, http.StatusOK, v1.TopicsListResponse{Topics: topics})
}

func (s *server) handleTopicsCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req v1.TopicsCreateRequest

	if r.Body != nil && r.ContentLength != 0 {
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json body: "+err.Error())
			return
		}
	} else {
		q := r.URL.Query()
		req.Name = strings.TrimSpace(q.Get("name"))

		if p := strings.TrimSpace(q.Get("partitions")); p != "" {
			n, err := strconv.Atoi(p)
			if err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partitions")
				return
			}
			req.Partitions = n
		}
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "name is required")
		return
	}

	partitions := req.Partitions
	if partitions == 0 {
		partitions = 1
	}

	if partitions <= 0 {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partitions")
		return
	}

	if err := s.broker.CreateTopic(ctx, name, partitions); err != nil {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	v1.WriteJSON(w, http.StatusCreated, v1.TopicsCreateResponse{
		Status:     "created",
		Name:       name,
		Partitions: partitions,
	})
}

func (s *server) handleProduce(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	toBrokerEnvelope := func(env *v1.Envelope) *broker.Envelope {
		if env == nil {
			return nil
		}

		out := &broker.Envelope{
			RunID:             env.RunID,
			StepID:            env.StepID,
			ParentStepID:      env.ParentStepID,
			TenantID:          env.TenantID,
			IdempotencyKey:    env.IdempotencyKey,
			TargetTopic:       env.TargetTopic,
			Deadline:          env.Deadline,
			PartitionOverride: env.PartitionOverride,
		}

		if env.RetryPolicy != nil {
			out.RetryPolicy = &broker.RetryPolicy{
				MaxAttempts:  env.RetryPolicy.MaxAttempts,
				BackoffMs:    env.RetryPolicy.BackoffMs,
				MaxBackoffMs: env.RetryPolicy.MaxBackoffMs,
			}
		}

		return out
	}

	parseDeadline := func(q url.Values) (*time.Time, error) {
		if v := strings.TrimSpace(q.Get("deadline_rfc3339")); v != "" {
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return nil, fmt.Errorf("invalid deadline_rfc3339: %w", err)
			}

			return &t, nil
		}

		if v := strings.TrimSpace(q.Get("deadline_ms")); v != "" {
			ms, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid deadline_ms: %w", err)
			}

			t := time.Unix(0, ms*int64(time.Millisecond))
			return &t, nil
		}

		return nil, nil
	}

	parseOptInt := func(q url.Values, key string) (*int, error) {
		v := strings.TrimSpace(q.Get(key))
		if v == "" {
			return nil, nil
		}

		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid %s", key)
		}
		return &n, nil
	}

	parseOptInt64 := func(q url.Values, key string) (*int64, error) {
		v := strings.TrimSpace(q.Get(key))
		if v == "" {
			return nil, nil
		}

		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid %s", key)
		}
		return &n, nil
	}

	var req v1.ProduceRequest

	// Prefer JSON body; fall back to query params
	if r.Body != nil && r.ContentLength != 0 {
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json body: "+err.Error())
			return
		}
	} else {
		q := r.URL.Query()

		req.Topic = q.Get("topic")
		req.Key = q.Get("key")
		req.Value = q.Get("value")

		env := &v1.Envelope{}
		anySet := false

		if v := strings.TrimSpace(q.Get("run_id")); v != "" {
			env.RunID = v
			anySet = true
		}

		if v := strings.TrimSpace(q.Get("step_id")); v != "" {
			env.StepID = v
			anySet = true
		}

		if v := strings.TrimSpace(q.Get("parent_step_id")); v != "" {
			env.ParentStepID = v
			anySet = true
		}

		// tenant_id (primary) + tenant (alias)
		if v := strings.TrimSpace(q.Get("tenant_id")); v != "" {
			env.TenantID = v
			anySet = true
		} else if v := strings.TrimSpace(q.Get("tenant")); v != "" {
			env.TenantID = v
			anySet = true
		}

		// idempotency_key (primary) + idem_key (alias)
		idem := strings.TrimSpace(q.Get("idempotency_key"))
		if idem == "" {
			idem = strings.TrimSpace(q.Get("idem_key"))
		}

		if idem != "" {
			env.IdempotencyKey = idem
			anySet = true
		}

		if v := strings.TrimSpace(q.Get("target_topic")); v != "" {
			env.TargetTopic = v
			anySet = true
		}

		if v := strings.TrimSpace(q.Get("partition_override")); v != "" {
			pi, err := strconv.Atoi(v)
			if err != nil || pi < 0 {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partition_override")
				return
			}
			env.PartitionOverride = &pi
			anySet = true
		}

		if dl, err := parseDeadline(q); err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		} else if dl != nil {
			env.Deadline = dl
			anySet = true
		}

		// Retry policy (query params)
		maxAttemptsPtr, err := parseOptInt(q, "retry_max_attempts")
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}

		backoffPtr, err := parseOptInt64(q, "retry_backoff_ms")
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}

		maxBackoffPtr, err := parseOptInt64(q, "retry_max_backoff_ms")
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}

		if maxAttemptsPtr != nil || backoffPtr != nil || maxBackoffPtr != nil {
			rp := &v1.RetryPolicy{}

			if maxAttemptsPtr != nil {
				if *maxAttemptsPtr < 0 {
					v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid retry_max_attempts")
					return
				}
				rp.MaxAttempts = *maxAttemptsPtr
			}

			if backoffPtr != nil {
				if *backoffPtr < 0 {
					v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid retry_backoff_ms")
					return
				}
				rp.BackoffMs = *backoffPtr
			}

			if maxBackoffPtr != nil {
				if *maxBackoffPtr < 0 {
					v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid retry_max_backoff_ms")
					return
				}
				rp.MaxBackoffMs = *maxBackoffPtr
			}

			if (backoffPtr != nil || maxBackoffPtr != nil) && rp.MaxAttempts <= 0 {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "retry_max_attempts must be > 0 when using retry backoff params")
				return
			}

			if rp.MaxAttempts != 0 || rp.BackoffMs != 0 || rp.MaxBackoffMs != 0 {
				env.RetryPolicy = rp
				anySet = true
			}
		}

		if anySet {
			req.Envelope = env
		}
	}

	if strings.TrimSpace(req.Topic) == "" || req.Value == "" {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "topic and value are required")
		return
	}

	msg := broker.Message{
		Key:      []byte(req.Key),
		Value:    []byte(req.Value),
		Envelope: toBrokerEnvelope(req.Envelope),
	}

	if err := s.broker.Produce(ctx, req.Topic, msg); err != nil {
		if errors.Is(err, broker.ErrProducerOverloaded) {
			retryAfter := 1 * time.Second
			reason := "overloaded"

			var oe *broker.ProducerOverloadError
			if errors.As(err, &oe) && oe != nil {
				if oe.RetryAfter > 0 {
					retryAfter = oe.RetryAfter
				}
				if oe.Reason != "" {
					reason = oe.Reason
				}
			}

			secs := int((retryAfter + time.Second - 1) / time.Second)
			w.Header().Set("Retry-After", strconv.Itoa(secs))

			v1.WriteJSON(w, http.StatusTooManyRequests, v1.ResourceExhaustedResponse{
				Error:        "RESOURCE_EXHAUSTED",
				Message:      err.Error(),
				Reason:       reason,
				RetryAfterMs: int(retryAfter / time.Millisecond),
			})
			return
		}

		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	v1.WriteJSON(w, http.StatusOK, v1.ProduceResponse{
		Status: "produced",
		Topic:  req.Topic,
	})
}

func (s *server) handleConsume(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	lease := 2 * time.Second
	var req v1.ConsumeRequest

	if r.Body != nil && r.ContentLength != 0 {
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json body: "+err.Error())
			return
		}
	} else {
		q := r.URL.Query()
		req.Topic = q.Get("topic")
		req.Group = q.Get("group")
		req.Owner = q.Get("owner")

		if v := strings.TrimSpace(q.Get("lease_ms")); v != "" {
			ms, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid lease_ms")
				return
			}
			req.LeaseMs = ms
		}
	}

	topic := strings.TrimSpace(req.Topic)
	group := strings.TrimSpace(req.Group)
	owner := strings.TrimSpace(req.Owner)

	if topic == "" || group == "" || owner == "" {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "topic, group, and owner are required")
		return
	}

	if req.LeaseMs < 0 {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid lease_ms")
		return
	}

	if req.LeaseMs > 0 {
		lease = time.Duration(req.LeaseMs) * time.Millisecond
	}

	// Use the new broker method (no type assertions now)
	ch, err := s.broker.ConsumeWithLease(ctx, topic, group, owner, lease)
	if err != nil {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	// stream NDJSON
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")

	flusher, ok := w.(http.Flusher)
	if !ok {
		v1.WriteError(w, http.StatusInternalServerError, "INTERNAL", "streaming not supported")
		return
	}

	enc := json.NewEncoder(w)

	for {
		select {
		case <-ctx.Done():
			return

		case m, ok := <-ch:
			if !ok {
				return
			}

			item := v1.ConsumeItem{
				Partition: m.Partition,
				Offset:    m.Offset,
				Attempts:  m.Attempts,
				Key:       string(m.Key),
				Value:     string(m.Value),
				LastError: m.LastError,
			}

			if m.Routing != nil {
				item.Routing = &v1.Routing{
					Label: m.Routing.Label,
					Meta:  m.Routing.Meta,
				}
			}

			if m.Envelope != nil {
				item.Envelope = &v1.Envelope{
					RunID:             m.Envelope.RunID,
					StepID:            m.Envelope.StepID,
					ParentStepID:      m.Envelope.ParentStepID,
					TenantID:          m.Envelope.TenantID,
					IdempotencyKey:    m.Envelope.IdempotencyKey,
					TargetTopic:       m.Envelope.TargetTopic,
					Deadline:          m.Envelope.Deadline,
					PartitionOverride: m.Envelope.PartitionOverride,
					RetryPolicy:       nil,
				}

				if m.Envelope.RetryPolicy != nil {
					item.Envelope.RetryPolicy = &v1.RetryPolicy{
						MaxAttempts:  m.Envelope.RetryPolicy.MaxAttempts,
						BackoffMs:    m.Envelope.RetryPolicy.BackoffMs,
						MaxBackoffMs: m.Envelope.RetryPolicy.MaxBackoffMs,
					}
				}
			}

			if err := enc.Encode(item); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *server) handleAck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var (
		topic string
		group string
		owner string
		part  int
		off   int64
	)

	if r.Body != nil && r.ContentLength != 0 {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "failed to read body")
			return
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(bodyBytes, &raw); err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json body: "+err.Error())
			return
		}

		allowed := map[string]struct{}{
			"topic":     {},
			"group":     {},
			"owner":     {},
			"partition": {},
			"offset":    {},
		}

		for k := range raw {
			if _, ok := allowed[k]; !ok {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "unknown field: "+k)
				return
			}
		}

		if v, ok := raw["topic"]; ok {
			_ = json.Unmarshal(v, &topic)
		}

		if v, ok := raw["group"]; ok {
			_ = json.Unmarshal(v, &group)
		}

		if v, ok := raw["owner"]; ok {
			_ = json.Unmarshal(v, &owner)
		}

		if v, ok := raw["partition"]; ok {
			if err := json.Unmarshal(v, &part); err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partition")
				return
			}
		}
		if v, ok := raw["offset"]; ok {
			if err := json.Unmarshal(v, &off); err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid offset")
				return
			}
		}
	} else {
		q := r.URL.Query()
		topic = q.Get("topic")
		group = q.Get("group")
		owner = q.Get("owner")

		pStr := strings.TrimSpace(q.Get("partition"))
		oStr := strings.TrimSpace(q.Get("offset"))
		if pStr == "" || oStr == "" {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "partition and offset are required")
			return
		}

		p64, err := strconv.ParseInt(pStr, 10, 32)
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partition")
			return
		}
		part = int(p64)

		off, err = strconv.ParseInt(oStr, 10, 64)
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid offset")
			return
		}
	}

	topic = strings.TrimSpace(topic)
	group = strings.TrimSpace(group)
	owner = strings.TrimSpace(owner)

	if topic == "" || group == "" || owner == "" {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "topic, group, and owner are required")
		return
	}

	if err := s.broker.AckIfOwner(ctx, topic, group, part, off, owner); err != nil {
		if errors.Is(err, broker.ErrNotOwner) {
			v1.WriteError(w, http.StatusConflict, "FAILED_PRECONDITION", "not owner")
			return
		}
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleNack(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var (
		topic  string
		group  string
		owner  string
		reason string
		part   int
		off    int64
	)

	if r.Body != nil && r.ContentLength != 0 {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "failed to read body")
			return
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(bodyBytes, &raw); err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json body: "+err.Error())
			return
		}

		allowed := map[string]struct{}{
			"topic":     {},
			"group":     {},
			"owner":     {},
			"partition": {},
			"offset":    {},
			"reason":    {},
		}

		for k := range raw {
			if _, ok := allowed[k]; !ok {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "unknown field: "+k)
				return
			}
		}

		if v, ok := raw["topic"]; ok {
			_ = json.Unmarshal(v, &topic)
		}

		if v, ok := raw["group"]; ok {
			_ = json.Unmarshal(v, &group)
		}

		if v, ok := raw["owner"]; ok {
			_ = json.Unmarshal(v, &owner)
		}

		if v, ok := raw["reason"]; ok {
			_ = json.Unmarshal(v, &reason)
		}

		if v, ok := raw["partition"]; ok {
			if err := json.Unmarshal(v, &part); err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partition")
				return
			}
		}
		if v, ok := raw["offset"]; ok {
			if err := json.Unmarshal(v, &off); err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid offset")
				return
			}
		}
	} else {
		q := r.URL.Query()
		topic = q.Get("topic")
		group = q.Get("group")
		owner = q.Get("owner")
		reason = q.Get("reason")

		pStr := strings.TrimSpace(q.Get("partition"))
		oStr := strings.TrimSpace(q.Get("offset"))

		if pStr == "" || oStr == "" {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "partition and offset are required")
			return
		}

		p64, err := strconv.ParseInt(pStr, 10, 32)
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partition")
			return
		}
		part = int(p64)

		off, err = strconv.ParseInt(oStr, 10, 64)
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid offset")
			return
		}
	}

	topic = strings.TrimSpace(topic)
	group = strings.TrimSpace(group)
	owner = strings.TrimSpace(owner)
	reason = strings.TrimSpace(reason)

	if topic == "" || group == "" || owner == "" {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "topic, group, and owner are required")
		return
	}

	if err := s.broker.Nack(ctx, topic, group, part, off, owner, reason); err != nil {
		if errors.Is(err, broker.ErrNotOwner) {
			v1.WriteError(w, http.StatusConflict, "FAILED_PRECONDITION", "not owner")
			return
		}
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (TestRouter) Route(_ context.Context, topic string, msg broker.Message) (broker.RoutingDecision, error) {
	return broker.RoutingDecision{
		Label: "test-label",
		Meta:  map[string]string{"source": "router"},
	}, nil
}
