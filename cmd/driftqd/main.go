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

	"github.com/BehnamAxo/DriftQ-Core/internal/broker"
)

type server struct {
	broker broker.Broker
}

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	// Create broker instance (in-memory for now)
	b := broker.NewInMemoryBroker()
	s := &server{broker: b}

	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})

	// Dev-only topic admin endpoints
	mux.HandleFunc("/topics", s.handleTopics)

	srv := &http.Server{
		Addr:         *addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
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
