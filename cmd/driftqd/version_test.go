package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
	v1 "github.com/driftq-org/DriftQ-Core/internal/httpapi/v1"
	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

func TestVersion_WALDisabled(t *testing.T) {
	prevV, prevC := buildVersion, buildCommit
	t.Cleanup(func() {
		buildVersion, buildCommit = prevV, prevC
	})

	buildVersion = "1.2.3"
	buildCommit = "abc123"

	s := &server{broker: broker.NewInMemoryBroker()}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/version", nil)

	s.requireMethod(http.MethodGet)(s.handleVersion)(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}

	var got v1.VersionResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v body=%s", err, rr.Body.String())
	}

	if got.Version != "1.2.3" {
		t.Fatalf("version: expected %q got %q", "1.2.3", got.Version)
	}

	if got.Commit != "abc123" {
		t.Fatalf("commit: expected %q got %q", "abc123", got.Commit)
	}

	if got.WalEnabled != false {
		t.Fatalf("wal_enabled: expected false got %v", got.WalEnabled)
	}
}

func TestVersion_WALEnabled(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "driftq.wal")
	wal, err := storage.OpenFileWAL(walPath)

	if err != nil {
		t.Fatalf("OpenFileWAL: %v", err)
	}
	defer wal.Close()

	b, err := broker.NewInMemoryBrokerFromWAL(wal)
	if err != nil {
		t.Fatalf("NewInMemoryBrokerFromWAL: %v", err)
	}

	s := &server{broker: b}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/version", nil)

	s.requireMethod(http.MethodGet)(s.handleVersion)(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}

	var got v1.VersionResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v body=%s", err, rr.Body.String())
	}

	if got.WalEnabled != true {
		t.Fatalf("wal_enabled: expected true got %v", got.WalEnabled)
	}
}
