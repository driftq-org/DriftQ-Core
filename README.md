# DriftQ Core

<p align='center'>
  <a href='https://drift-q.org' target="_blank">Website</a>
</p>

A lightweight, extensible message broker built in Go — designed for the AI-native era.

**DriftQ Core** is the open-source, high-performance message broker powering the AI-native messaging platform DriftQ.

---

## Overview
DriftQ Core provides the foundational broker components:
- topics, partitions, producers, consumers
- at-least-once delivery
- durable write-ahead log (WAL)
- Prometheus metrics and OpenTelemetry traces
- pluggable policy layer (for AI-driven routing)

The core is **100% Go**, designed for simplicity, concurrency, and reliability.

---

## Architecture

Producers → Broker (driftqd) → Storage (WAL) → Consumers
- **Broker:** Manages message flow and offsets.
- **Storage:** WAL + compaction.
- **Scheduler:** Manages consumer groups and retries.
- **Policy:** External interface for DriftQ Brain (AI optimizer, optional).

---

## Requirements
- Go 1.23+
- Make
- Docker (optional)

---

## Quick Start
```bash
# Clone and build
git clone https://github.com/driftq-org/driftq-core.git
cd driftq-core
make build

# Run broker
./bin/driftqd --config ./deploy/config.yaml
```

Then open [http://localhost:8080](http://localhost:8080) to see metrics or UI.


## Features
- Lightweight Go broker with durable storage
- At-least-once delivery
- Partitioned topics and consumer groups
- Prometheus + OpenTelemetry metrics


 ## Roadmap
 - [ ] WAL persistence engine
 - [ ] Consumer group coordination
 - [ ] CLI for topic management
 - [ ] Policy gRPC interface (AI hook)


## Contributing
Contributions are welcome! Please open an issue or pull request to discuss changes.


## License
Licensed under the [Apache License 2.0](./LICENSE).
