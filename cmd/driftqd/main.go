package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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

	// // Create broker instance (in-memory for now)
	// b := broker.NewInMemoryBroker()
	// s := &server{broker: b}

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

	topic := r.URL.Query().Get("topic")
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")

	if topic == "" || value == "" {
		http.Error(w, "topic and value are required", http.StatusBadRequest)
		return
	}

	msg := broker.Message{
		Key:   []byte(key),
		Value: []byte(value),
	}

	if err := s.broker.Produce(ctx, topic, msg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

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

	// We are going to stream NDJSON (one JSON object per line).
	w.Header().Set("Content-Type", "application/json")

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
				"partition": m.Partition,
				"offset":    m.Offset,
				"key":       string(m.Key),
				"value":     string(m.Value),
			}

			// If routing info exists, include it.
			if m.Routing != nil {
				obj["routing"] = map[string]any{
					"label": m.Routing.Label,
					"meta":  m.Routing.Meta,
				}
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

	if partitionStr == "" {
		partitionStr = "0" // default ONLY for now
	}

	partition, err := strconv.Atoi(partitionStr)
	if err != nil || partition < 0 {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}

	if topic == "" || group == "" || offsetStr == "" {
		http.Error(w, "topic, group, and offset are required", http.StatusBadRequest)
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

func (TestRouter) Route(_ context.Context, topic string, msg broker.Message) (broker.RoutingDecision, error) {
	return broker.RoutingDecision{
		Label: "test-label",
		Meta:  map[string]string{"source": "router"},
	}, nil
}
