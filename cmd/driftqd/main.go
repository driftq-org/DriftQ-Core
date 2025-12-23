package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

type server struct {
	broker broker.Broker
}

type TestRouter struct{}

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	// For now I just hardcode a WAL file in the current dir. Later I can make this a flag or env var.
	wal, err := storage.OpenFileWAL("driftq.wal")
	if err != nil {
		log.Fatalf("failed to open WAL: %v", err)
	}
	defer wal.Close()

	b, err := broker.NewInMemoryBrokerFromWAL(wal)
	if err != nil {
		log.Fatalf("failed to replay WAL: %v", err)
	}

	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	b.StartRedeliveryLoop(appCtx)

	b.SetRouter(TestRouter{})
	s := &server{broker: b}

	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/produce", s.handleProduce)
	mux.HandleFunc("/consume", s.handleConsume)
	mux.HandleFunc("/ack", s.handleAck)

	// Dev-only topic admin endpoints
	mux.HandleFunc("/topics", s.handleTopics)
	mux.HandleFunc("/nack", s.handleNack)

	srv := &http.Server{
		Addr:         *addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 0, // or 10 * time.Second
	}

	log.Printf("DriftQ broker starting on %s\n", *addr)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	// Shutdown stuff
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("shutting down...")
	appCancel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown error: %v", err)
	}

	fmt.Println("DriftQ broker stopped")
}

// handleTopics handles:
//
//	GET  /topics                           -> list topics
//	POST /topics?name=foo&partitions=3     -> create topic
//
// This is a dev-only HTTP surface; proper gRPC/HTTP APIs will replace it later
func (s *server) handleTopics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	switch r.Method {
	case http.MethodGet:
		topics, err := s.broker.ListTopics(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"topics": topics,
		})

	case http.MethodPost:
		name := r.URL.Query().Get("name")
		partitionsStr := r.URL.Query().Get("partitions")
		if partitionsStr == "" {
			partitionsStr = "1"
		}

		partitions, err := strconv.Atoi(partitionsStr)
		if err != nil || partitions <= 0 {
			http.Error(w, "invalid partitions", http.StatusBadRequest)
			return
		}

		if err := s.broker.CreateTopic(ctx, name, partitions); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("created\n"))

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *server) handleProduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()

	type produceRequest struct {
		Topic    string           `json:"topic"`
		Key      string           `json:"key,omitempty"`
		Value    string           `json:"value"`
		Envelope *broker.Envelope `json:"envelope,omitempty"`
	}

	parseDeadline := func(q url.Values) (*time.Time, error) {
		// accept either deadline_rfc3339 or deadline_ms
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

	var req produceRequest

	// Note: If JSON body exists, use it. Otherwise fall back to query params
	if r.Body != nil && r.ContentLength != 0 {
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			http.Error(w, "invalid json body: "+err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		q := r.URL.Query()

		req.Topic = q.Get("topic")
		req.Key = q.Get("key")
		req.Value = q.Get("value")

		// Query-param envelope support (labels skipped for now)
		env := &broker.Envelope{}
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

		if v := strings.TrimSpace(q.Get("tenant_id")); v != "" {
			env.TenantID = v
			anySet = true
		}

		if v := strings.TrimSpace(q.Get("idempotency_key")); v != "" {
			env.IdempotencyKey = v
			anySet = true
		}

		if v := strings.TrimSpace(q.Get("target_topic")); v != "" {
			env.TargetTopic = v
			anySet = true
		}

		if v := strings.TrimSpace(q.Get("partition_override")); v != "" {
			pi, err := strconv.Atoi(v)

			if err != nil || pi < 0 {
				http.Error(w, "invalid partition_override", http.StatusBadRequest)
				return
			}

			env.PartitionOverride = &pi
			anySet = true
		}

		if dl, err := parseDeadline(q); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		} else if dl != nil {
			env.Deadline = dl
			anySet = true
		}

		// Retry policy (query params)
		maxAttemptsPtr, err := parseOptInt(q, "retry_max_attempts")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		backoffPtr, err := parseOptInt64(q, "retry_backoff_ms")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		maxBackoffPtr, err := parseOptInt64(q, "retry_max_backoff_ms")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Validate + build RetryPolicy only if something was provided
		if maxAttemptsPtr != nil || backoffPtr != nil || maxBackoffPtr != nil {
			rp := &broker.RetryPolicy{}

			if maxAttemptsPtr != nil {
				if *maxAttemptsPtr < 0 {
					http.Error(w, "invalid retry_max_attempts", http.StatusBadRequest)
					return
				}
				rp.MaxAttempts = *maxAttemptsPtr
			}

			if backoffPtr != nil {
				if *backoffPtr < 0 {
					http.Error(w, "invalid retry_backoff_ms", http.StatusBadRequest)
					return
				}
				rp.BackoffMs = *backoffPtr
			}

			if maxBackoffPtr != nil {
				if *maxBackoffPtr < 0 {
					http.Error(w, "invalid retry_max_backoff_ms", http.StatusBadRequest)
					return
				}
				rp.MaxBackoffMs = *maxBackoffPtr
			}

			// If any backoff knobs are set, require maxAttempts > 0
			if (backoffPtr != nil || maxBackoffPtr != nil) && rp.MaxAttempts <= 0 {
				http.Error(w, "retry_max_attempts must be > 0 when using retry backoff params", http.StatusBadRequest)
				return
			}

			// If it's effectively empty, keep nil
			if rp.MaxAttempts != 0 || rp.BackoffMs != 0 || rp.MaxBackoffMs != 0 {
				env.RetryPolicy = rp
				anySet = true
			}
		}

		if anySet {
			req.Envelope = env
		}
	}

	if req.Topic == "" || req.Value == "" {
		http.Error(w, "topic and value are required", http.StatusBadRequest)
		return
	}

	msg := broker.Message{
		Key:      []byte(req.Key),
		Value:    []byte(req.Value),
		Envelope: req.Envelope,
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
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusTooManyRequests)

			_ = json.NewEncoder(w).Encode(map[string]any{
				"error":          "RESOURCE_EXHAUSTED",
				"reason":         reason,
				"message":        err.Error(),
				"retry_after_ms": int(retryAfter / time.Millisecond),
			})
			return
		}

		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("produced\n"))
}

func (s *server) handleConsume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()

	topic := r.URL.Query().Get("topic")
	group := r.URL.Query().Get("group")
	if topic == "" || group == "" {
		http.Error(w, "topic and group are required", http.StatusBadRequest)
		return
	}

	ch, err := s.broker.Consume(ctx, topic, group)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// We are going to stream NDJSON (one JSON object per line)
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
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

			obj := map[string]any{
				"partition":  m.Partition,
				"offset":     m.Offset,
				"attempts":   m.Attempts,
				"key":        string(m.Key),
				"value":      string(m.Value),
				"last_error": m.LastError,
			}

			if m.Routing != nil {
				obj["routing"] = map[string]any{
					"label": m.Routing.Label,
					"meta":  m.Routing.Meta,
				}
			}

			if m.Envelope != nil {
				obj["envelope"] = m.Envelope
			}

			if err := enc.Encode(obj); err != nil {
				return
			}

			flusher.Flush()
		}
	}
}

func (s *server) handleAck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()

	topic := r.URL.Query().Get("topic")
	group := r.URL.Query().Get("group")
	offsetStr := r.URL.Query().Get("offset")
	partitionStr := r.URL.Query().Get("partition")

	// CHANGE: require partition (no default)
	if topic == "" || group == "" || offsetStr == "" || partitionStr == "" {
		http.Error(w, "topic, group, partition, and offset are required", http.StatusBadRequest)
		return
	}

	partition, err := strconv.Atoi(partitionStr)
	if err != nil || partition < 0 {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid offset", http.StatusBadRequest)
		return
	}

	if err := s.broker.Ack(ctx, topic, group, partition, offset); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, _ = w.Write([]byte("acked\n"))
}

func (s *server) handleNack(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()

	topic := r.URL.Query().Get("topic")
	group := r.URL.Query().Get("group")
	offsetStr := r.URL.Query().Get("offset")
	partitionStr := r.URL.Query().Get("partition")
	owner := r.URL.Query().Get("owner")
	reason := r.URL.Query().Get("reason")

	if strings.TrimSpace(owner) == "" {
		http.Error(w, "owner is required", http.StatusBadRequest)
		return
	}

	if topic == "" || group == "" || offsetStr == "" || partitionStr == "" {
		http.Error(w, "topic, group, partition, and offset are required", http.StatusBadRequest)
		return
	}

	partition, err := strconv.Atoi(partitionStr)
	if err != nil || partition < 0 {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid offset", http.StatusBadRequest)
		return
	}

	if err := s.broker.Nack(ctx, topic, group, partition, offset, owner, reason); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, _ = w.Write([]byte("nacked\n"))
}

func (TestRouter) Route(_ context.Context, topic string, msg broker.Message) (broker.RoutingDecision, error) {
	return broker.RoutingDecision{
		Label: "test-label",
		Meta:  map[string]string{"source": "router"},
	}, nil
}
