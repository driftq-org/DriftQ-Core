package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
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
	v1 "github.com/driftq-org/DriftQ-Core/internal/httpapi/v1"
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

	rootMux := http.NewServeMux()
	v1Mux := http.NewServeMux()

	// v1 routes
	v1Mux.HandleFunc("/healthz", s.requireMethod(http.MethodGet)(s.handleHealthz))
	v1Mux.HandleFunc("/produce", s.requireMethod(http.MethodPost)(s.handleProduce))
	v1Mux.HandleFunc("/consume", s.requireMethod(http.MethodGet)(s.handleConsume))
	v1Mux.HandleFunc("/ack", s.requireMethod(http.MethodPost)(s.handleAck))
	v1Mux.HandleFunc("/nack", s.requireMethod(http.MethodPost)(s.handleNack))
	v1Mux.HandleFunc("/topics", s.method(s.handleTopicsList, s.handleTopicsCreate))

	// mount v1 under /v1/*
	rootMux.Handle("/v1/", http.StripPrefix("/v1", v1Mux))

	// block unversioned routes
	rootMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		v1.WriteError(w, http.StatusNotFound, "NOT_FOUND", "use /v1/* endpoints")
	})

	srv := &http.Server{
		Addr:         *addr,
		Handler:      rootMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 0,
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

		// optional
		if v := strings.TrimSpace(q.Get("lease_ms")); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid lease_ms")
				return
			}

			req.LeaseMs = n
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

	ch, err := s.broker.Consume(ctx, topic, group)
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
	var req v1.AckRequest

	// I got it so it prefers JSON body; falls back to query params
	if r.Body != nil && r.ContentLength != 0 {
		// Read body once so we can:
		// 1) decode with DisallowUnknownFields
		// 2) also verify required keys exist (so partition/offset can't silently default to 0)
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "failed to read body")
			return
		}

		dec := json.NewDecoder(bytes.NewReader(bodyBytes))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json body: "+err.Error())
			return
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(bodyBytes, &raw); err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json body")
			return
		}

		required := []string{"topic", "group", "owner", "partition", "offset"}
		for _, k := range required {
			if _, ok := raw[k]; !ok {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", k+" is required")
				return
			}
		}
	} else {
		q := r.URL.Query()

		req.Topic = q.Get("topic")
		req.Group = q.Get("group")
		req.Owner = q.Get("owner")

		partitionStr := strings.TrimSpace(q.Get("partition"))
		offsetStr := strings.TrimSpace(q.Get("offset"))

		// Hard-require these so we don't accidentally ack partition=0 offset=0
		if partitionStr == "" || offsetStr == "" {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "partition and offset are required")
			return
		}

		p, err := strconv.Atoi(partitionStr)
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partition")
			return
		}
		req.Partition = p

		o, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid offset")
			return
		}
		req.Offset = o
	}

	topic := strings.TrimSpace(req.Topic)
	group := strings.TrimSpace(req.Group)
	owner := strings.TrimSpace(req.Owner)

	if topic == "" || group == "" || owner == "" {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "topic, group, and owner are required")
		return
	}

	if req.Partition < 0 {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partition")
		return
	}

	if req.Offset < 0 {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid offset")
		return
	}

	if err := s.broker.AckIfOwner(ctx, topic, group, req.Partition, req.Offset, owner); err != nil {
		// Wrong owner => conflict
		if errors.Is(err, broker.ErrNotOwner) {
			v1.WriteError(w, http.StatusConflict, "NOT_OWNER", err.Error())
			return
		}

		v1.WriteError(w, http.StatusBadRequest, "FAILED_PRECONDITION", err.Error())
		return
	}

	v1.WriteJSON(w, http.StatusOK, v1.AckResponse{
		Status:    "acked",
		Topic:     topic,
		Group:     group,
		Partition: req.Partition,
		Offset:    req.Offset,
	})
}

func (s *server) handleNack(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req v1.NackRequest

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
		req.Reason = q.Get("reason")

		partitionStr := q.Get("partition")
		offsetStr := q.Get("offset")

		if partitionStr != "" {
			p, err := strconv.Atoi(partitionStr)

			if err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partition")
				return
			}

			req.Partition = p
		}

		if offsetStr != "" {
			o, err := strconv.ParseInt(offsetStr, 10, 64)

			if err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid offset")
				return
			}

			req.Offset = o
		}
	}

	topic := strings.TrimSpace(req.Topic)
	group := strings.TrimSpace(req.Group)
	owner := strings.TrimSpace(req.Owner)

	if owner == "" {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "owner is required")
		return
	}

	if topic == "" || group == "" {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "topic and group are required")
		return
	}

	if req.Partition < 0 {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partition")
		return
	}

	if req.Offset < 0 {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid offset")
		return
	}

	if err := s.broker.Nack(ctx, topic, group, req.Partition, req.Offset, owner, req.Reason); err != nil {
		v1.WriteError(w, http.StatusBadRequest, "FAILED_PRECONDITION", err.Error())
		return
	}

	v1.WriteJSON(w, http.StatusOK, v1.NackResponse{
		Status:    "nacked",
		Topic:     topic,
		Group:     group,
		Partition: req.Partition,
		Offset:    req.Offset,
		Owner:     owner,
		Reason:    req.Reason,
	})
}

func (TestRouter) Route(_ context.Context, topic string, msg broker.Message) (broker.RoutingDecision, error) {
	return broker.RoutingDecision{
		Label: "test-label",
		Meta:  map[string]string{"source": "router"},
	}, nil
}
