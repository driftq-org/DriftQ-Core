# DriftQ Core

<p align='center'>
  <a href='https://drift-q.org' target="_blank">Website</a>
</p>

**Status:** Experimental / pre-release (v1 milestone in progress).
**Production:** Not production-ready. APIs and semantics may change without notice.

DriftQ Core is a lightweight, extensible message broker built in Go — designed for the AI-native era.

---

## What this is (today)

DriftQ Core currently implements a **minimal but real broker** with:
- topics + partitions
- consumer groups + per-partition offsets + acks
- **at-least-once** delivery (redelivery on ack timeout)
- durability via a simple **write-ahead log (WAL)** with replay on startup
- an **agent message envelope** (run/step IDs, routing controls, retry policy, tenant + idempotency fields)
- a pluggable **router hook** (“brain v0”) that can label messages and request routing overrides

What it is **not** (yet):
- production-ready software (experimental / pre-release)
- a stable API/SDK (expect breaking changes)
- a security or compliance solution
- a fully durable retry/DLQ system (durable retry state is still evolving)

---

## Semantics docs (v1 roadmap so far)

The best way to understand “what DriftQ guarantees” is the versioned semantics docs:

- `docs/v1/` — v1 milestone contracts (v1.1 → v1.6)
- `docs/architecture.md` — cross-cutting architecture notes

Start here:
- `docs/v1/v1.1-broker-core-consumer-groups.md`
- `docs/v1/v1.2-partitions-minimal.md`
- `docs/v1/v1.3-durability-wal.md`
- `docs/v1/v1.4-agent-message-envelope.md`
- `docs/v1/v1.5-ai-native-router-hook.md`
- `docs/v1/v1.6-idempotency.md`

---

## Architecture (current mental model)

Producers → Broker (`driftqd`) → Storage (WAL) → Consumers

Core components:
- **Broker:** message flow, partitions, consumer groups, offsets, redelivery
- **Storage:** WAL append + replay (MVP)
- **Router hook:** optional labeling + routing overrides (“brain v0”)
- **Envelope:** workflow/agent metadata carried with each message

---

## Requirements
- Go 1.23+
- Make
- Docker (optional)

---

## Quick Start (current repo layout)

```bash
# Clone
git clone https://github.com/driftq-org/driftq-core.git
cd driftq-core

# Run the dev broker (current CLI flags)
go run ./cmd/driftqd/main.go --addr :8080
```

Dev HTTP endpoints (MVP):
- `POST /topics?name=<topic>&partitions=<n>`
- `POST /produce` (JSON body) or `POST /produce?topic=...&value=...`
- `GET  /consume?topic=...&group=...` (streams NDJSON)
- `POST /ack?topic=...&group=...&partition=...&offset=...`

> Note: The README intentionally documents the **current dev surface**. This will be replaced by proper public APIs/SDKs in later milestones.

---

## Features (implemented)
- lightweight Go broker core (in-memory state)
- partitioned topics + consumer groups
- at-least-once delivery with ack-timeout redelivery
- WAL persistence for messages + offsets + replay on startup
- router hook for metadata labeling + routing overrides
- envelope fields for workflow identity + routing controls + retry/idempotency knobs (v1.4+)

---

## Roadmap (v1 milestones)

See `docs/v1/` for the exact contracts. High-level:
- v1.6: idempotency + “exactly-once-ish side effects” (worker-boundary dedupe)
- next: durable retry/DLQ state in WAL + DLQ routing + tests
- v1.7+: public APIs
- v1.8+: SDKs
- v1.9+: observability
- v1.10+: test suite hardening
- demo: end-to-end example pipeline

---

## Contributing

Contributions are welcome, but expect churn while v1 is in flight.

Suggested workflow:
1) Open an issue describing the change.
2) Link the relevant `docs/v1/v1.x-*.md` contract you’re touching.
3) Keep PRs small and milestone-scoped.

---

## License

Licensed under the [Apache License 2.0](./LICENSE).

**No warranty:** This software is provided “AS IS”, without warranty of any kind, express or implied.
