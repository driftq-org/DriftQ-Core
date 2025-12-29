# DriftQ v1 — Core spec (consolidated)

This file consolidates the old `docs/v1/v1.*.md` documents into a single v1 spec.

- Scope: DriftQ-Core v1 semantics + `/v1/*` HTTP API surface.
- Included sub-versions: **v1.1 → v1.6**

## Contents

- HTTP API Reference (v1)
- Current implementation defaults (v1)
- v1.1 — Broker core + consumer groups
- v1.2 — Partitions (minimal)
- v1.3 — Durability (WAL)
- v1.4 — Agent message envelope
- v1.5 — AI-native router hook
- v1.6 — Idempotency


## HTTP API Reference (v1)

Base path: `/v1`

Notes:
- Most endpoints accept either **JSON body** or **query params** (dev convenience).
- JSON decoding uses `DisallowUnknownFields()` — unknown fields are rejected with 400.
- Unversioned routes are blocked (you only get `/v1/...`).

### Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/v1/healthz` | Liveness check |
| GET | `/v1/version` | Build/version info + whether WAL is enabled |
| GET | `/v1/topics` | List topics |
| POST | `/v1/topics` | Create topic |
| POST | `/v1/produce` | Produce a message |
| GET | `/v1/consume` | Stream messages (NDJSON) |
| POST | `/v1/ack` | Ack a message (must be owner) |
| POST | `/v1/nack` | Nack a message (must be owner) |

### Common error shape

Most error responses look like:

```json
{ "error": "SOME_CODE", "message": "human-readable message" }
```

405 responses include an `Allow` header.

---

### GET `/v1/healthz`

**Response (200):**
```json
{ "status": "ok" }
```

---

### GET `/v1/version`

**Response (200):**
```json
{ "version": "dev", "commit": "unknown", "wal_enabled": false }
```

---

### GET `/v1/topics`

**Response (200):**
```json
{ "topics": ["t1", "t2"] }
```

---

### POST `/v1/topics`

Create a topic.

**JSON body (recommended):**
```json
{ "name": "t1", "partitions": 3 }
```

**Query params (dev convenience):** `?name=t1&partitions=3`

**Response (201):**
```json
{ "status": "created", "name": "t1", "partitions": 3 }
```

---

### POST `/v1/produce`

Produce a message to a topic.

**JSON body (recommended):**
```json
{
  "topic": "t1",
  "key": "optional-key",
  "value": "payload-as-string",
  "envelope": {
    "run_id": "run_123",
    "step_id": "step_7",
    "parent_step_id": "step_3",
    "tenant_id": "tenant_a",
    "idempotency_key": "tenant_a:run_123:step_7",
    "target_topic": "tasks.enrich",
    "partition_override": 1,
    "deadline": "2025-12-21T12:00:00Z",
    "retry_policy": { "max_attempts": 5, "backoff_ms": 250, "max_backoff_ms": 5000 }
  }
}
```

**Query params (dev convenience):**
- Required: `topic`, `value`
- Optional: `key`
- Optional envelope fields:
  - `run_id`, `step_id`, `parent_step_id`
  - `tenant_id` (alias: `tenant`)
  - `idempotency_key` (alias: `idem_key`)
  - `target_topic`
  - `partition_override`
  - `deadline` (RFC3339 timestamp)
  - `retry_max_attempts`, `retry_backoff_ms`, `retry_max_backoff_ms`

**Response (200):**
```json
{ "status": "produced", "topic": "t1" }
```

**Overload response (429):**
- Header: `Retry-After: <seconds>`
- Body:
```json
{ "error": "RESOURCE_EXHAUSTED", "message": "...", "reason": "overloaded", "retry_after_ms": 1000 }
```

---

### GET `/v1/consume` (NDJSON stream)

Consume messages from a topic as a **stream** of JSON objects (one per line).  
Content-Type: `application/x-ndjson; charset=utf-8`

**Parameters (required):** `topic`, `group`, `owner`  
**Optional:** `lease_ms` (defaults to 2000ms)

**Query example:**
`/v1/consume?topic=t1&group=g1&owner=worker-a&lease_ms=2000`

**JSON body example:**
```json
{ "topic": "t1", "group": "g1", "owner": "worker-a", "lease_ms": 2000 }
```

**Stream item shape:**
```json
{
  "partition": 0,
  "offset": 12,
  "attempts": 1,
  "key": "optional-key",
  "value": "payload-as-string",
  "last_error": "",
  "routing": { "label": "some-label", "meta": { "k": "v" } },
  "envelope": {
    "run_id": "run_123",
    "step_id": "step_7",
    "parent_step_id": "step_3",
    "tenant_id": "tenant_a",
    "idempotency_key": "tenant_a:run_123:step_7",
    "target_topic": "tasks.enrich",
    "partition_override": 1,
    "deadline": "2025-12-21T12:00:00Z",
    "retry_policy": { "max_attempts": 5, "backoff_ms": 250, "max_backoff_ms": 5000 }
  }
}
```

---

### POST `/v1/ack`

Ack a message you previously received.

**JSON body:**
```json
{ "topic": "t1", "group": "g1", "partition": 0, "offset": 12, "owner": "worker-a" }
```

**Query params:** `?topic=t1&group=g1&partition=0&offset=12&owner=worker-a`

**Response:** `204 No Content`  
**If not owner:** `409 Conflict` with `{ "error": "FAILED_PRECONDITION", "message": "not owner" }`

---

### POST `/v1/nack`

Nack a message (record failure + schedule retry/redelivery).

**JSON body:**
```json
{ "topic": "t1", "group": "g1", "partition": 0, "offset": 12, "owner": "worker-a", "reason": "timeout calling upstream" }
```

**Query params:** `?topic=t1&group=g1&partition=0&offset=12&owner=worker-a&reason=...`

**Response:** `204 No Content`  
**If not owner:** `409 Conflict` with `{ "error": "FAILED_PRECONDITION", "message": "not owner" }`

## Current implementation defaults (v1)

These are **current** defaults in `internal/broker` (mainly tuned for testing/dev):

- `maxInFlight`: **2** unacked messages per `(topic, group, partition)`
- `ackTimeout`: **2s**
- redelivery tick: **250ms**
- producer backpressure limits per partition:
  - `maxPartitionMsgs`: **2**
  - `maxPartitionBytes`: **20** (bytes)
- idempotency TTL: **10 minutes**


---

## DriftQ v1.1 — Broker core + consumer groups (Semantics)

This document defines the **v1.1 contract** for the broker core and consumer groups so behavior stays consistent as DriftQ evolves.

### What we guarantee (and what we don’t)

DriftQ v1.1 provides a minimal but real message-queue core:

Guaranteed / intended behavior:
- Topics exist and have **N partitions** (minimal partitioning).
- Messages are persisted in the broker’s in-memory state (and may also be written to a WAL if configured).
- Consumers belong to a **consumer group** identified by a `group` string.
- Messages are dispatched so that **within a group, only one consumer receives a given delivery attempt** (no broadcast).
- Consumer progress is tracked with **per-partition offsets** per (topic, group, partition).
- Consumers must **Ack** messages to advance offsets and stop redelivery.
- If a message is delivered but not Acked within `ackTimeout`, the broker **may redeliver** it (at-least-once delivery).

Not guaranteed in v1.1:
- exactly-once delivery
- strict ordering across partitions (ordering is per partition)
- fairness across partitions under load beyond simple dispatch logic
- durable consumer offsets unless WAL is enabled and successfully appends

### Terms

- **Topic**: named stream of messages with one or more partitions.
- **Partition**: ordered log segment within a topic. Offsets increase per topic (current MVP) but dispatch is per partition.
- **Offset**: monotonically increasing position assigned to each message. Used to track consumer progress.
- **Consumer group**: a logical subscription name. Each group tracks its own offsets.
- **In-flight**: delivered to a consumer channel but not yet Acked.
- **Ack timeout**: if in-flight for too long without Ack, message is eligible for redelivery.

### APIs / surfaces (dev HTTP)

This section is consolidated and kept up-to-date in **HTTP API Reference** at the top of this file.
### Consumer groups and dispatch semantics

#### Group membership
- A consumer group is identified by `(topic, group)`.
- Multiple consumers can concurrently `Consume(topic, group)`.
- The broker maintains a list of consumer channels for each (topic, group).

#### No broadcast within a group
For a given (topic, group), each delivery attempt of a message is sent to **exactly one** consumer channel from that group.

#### Round-robin selection
Within a group, the broker selects the next consumer channel in a round-robin manner for each dispatch event.

### Offsets and acknowledgements

#### Offset tracking model
Offsets are tracked as:

**(topic, group, partition) → last-acked offset**

Notes:
- Stored offset represents **“last acked offset”**.
- If no offset exists yet for a partition, it is treated as `-1` (nothing acked).

#### Ack behavior
When the consumer Ack’s `(topic, group, partition, offset)`:
- If `offset` is **greater** than the current stored offset, the broker advances progress and (if WAL is enabled) appends an offset record.
- If `offset` is **<=** current stored offset, Ack is treated as a duplicate/late Ack; it is still considered “done” and the broker removes it from in-flight if present.

After Ack:
- the broker removes that offset from the in-flight set
- the broker dispatches any newly eligible messages

### In-flight limit (backpressure on dispatch)

The broker enforces a limit on the number of **unacked (in-flight) messages** per:

**(topic, group, partition)**

Behavior:
- if the in-flight set for a (topic, group, partition) reaches `maxInFlight`, the broker stops dispatching more messages for that partition to that group until Acks arrive.

### Ack timeout and redelivery (slow consumer handling)

If a message is delivered but not Acked within `ackTimeout`:
- the broker considers it eligible for redelivery
- the broker may redeliver it to a consumer in the same group
- the `attempts` counter on the delivered message increases per delivery attempt

This results in **at-least-once delivery** semantics. Consumers should be prepared to handle duplicates.

### Producer backpressure (buffer limits) and overload response

The broker enforces partition buffer limits to avoid unbounded memory growth. Two knobs exist:

- **max buffered messages per partition**
- **max buffered bytes per partition** (key + value sizes)

Behavior when limits are exceeded:
- produce is rejected with a backpressure/overload error
- HTTP layer returns **429 / RESOURCE_EXHAUSTED**, including a suggested retry delay (`Retry-After` / `retry_after_ms`)

### Durability notes (WAL)

If a WAL is configured:
- messages are appended to WAL before being committed to in-memory state
- consumer offsets are appended to WAL **only when advancing progress**
- on startup, the broker can replay WAL to reconstruct:
  - topics/partitions/messages
  - consumer offsets

If WAL append fails, the operation fails (produce/ack fail accordingly).

### Non-goals for v1.1

- DLQ routing
- exactly-once processing
- durable in-flight / retry schedule state
- observability/metrics beyond basic behavior
- production-grade APIs (v1.7+)

### Example flows

#### Basic consume + ack
1) Producer sends message to `topic=test`.
2) Consumer connects: `GET /consume?topic=test&group=g1`.
3) Broker delivers message with `(partition, offset)`.
4) Consumer processes and calls `POST /ack?...&partition=<p>&offset=<o>`.
5) Broker advances offset and stops redelivering that message.

#### Slow consumer (timeout redelivery)
1) Broker delivers message attempt=1.
2) Consumer does not Ack within `ackTimeout`.
3) Broker redelivers attempt=2 (possibly to a different consumer in the same group).
4) Consumer eventually Ack’s; broker advances offset and stops retries.

---

## DriftQ v1.2 — Partitions (minimal) (Semantics)

This document defines the **v1.2 contract** for partitions so partitioning behavior stays stable as DriftQ evolves.

### What we guarantee (and what we don’t)

Guaranteed / intended behavior:
- A topic is created with **N partitions** (`partitions >= 1`).
- Each message is assigned to **exactly one partition** at produce time.
- Within a single partition, messages are delivered in **offset order** (subject to at-least-once redelivery).
- Consumers receive `partition` and `offset` with every delivered message.
- Consumer offsets are tracked **per partition** per (topic, group, partition).

Not guaranteed in v1.2:
- strict ordering across partitions (there is no global order across the whole topic)
- perfect load balancing across partitions (depends on key distribution)
- re-partitioning (changing partition count after topic creation)
- exactly-once delivery

### Key terms

- **Partition**: an ordered stream within a topic. Each message belongs to one partition.
- **Key**: optional bytes used to deterministically choose a partition.
- **Partition override**: optional explicit partition selection via envelope (if provided).
- **Offset**: position assigned to a message for tracking and delivery progress.

### Partition representation (internal)

Internally, v1.2 represents topic state as:
- `topic.partitions`: a slice/array of partitions
- each partition holds a sequence of messages
- each message carries `Partition` and `Offset` fields

(Exact structs may evolve; this describes the behavior.)

### Partition selection (hash-based partitioning)

When producing a message to a topic with N partitions:

1) **Partition override (if provided) wins**
   - If `envelope.partition_override` is set, that partition index is used.
   - If the override is out of range (`<0` or `>= N`), produce fails.

2) Otherwise, **hash-based partitioning**
   - If `key` is non-empty, the broker computes a deterministic hash of the key and selects:
     - `partition = hash(key) % N`
   - If `key` is empty, the broker uses `partition = 0` (MVP default).

Stability contract:
- Given the same `key` and the same partition count `N`, the partition selection is deterministic.

### Produce/consume partition-awareness

#### Produce
- Each produced message is stored in exactly one partition.
- The assigned `partition` and `offset` are part of the broker’s message metadata.

#### Consume
- Consumers subscribe by `(topic, group)` (no per-partition subscribe API in v1.2).
- The broker dispatches from all partitions for that topic and group (subject to in-flight limits).
- Delivered messages include:
  - `partition`
  - `offset`
  - `attempts` (delivery attempt count)
  - payload (`key`, `value`)

### Ordering and duplicates

Within a partition:
- New messages are appended in offset order.
- Under normal operation, dispatch walks offsets forward per partition.

Because v1.2 supports retries/redelivery:
- A message may be delivered more than once.
- Consumers must treat duplicates as possible (at-least-once).

### Non-goals for v1.2

- partition rebalancing / consumer assignment strategies (Kafka-style rebalancing)
- dynamic partition expansion/shrink
- cross-partition transactions or ordering
- per-partition consumer APIs

### Example flows

#### Two keys across multiple partitions
Topic has `N=3` partitions.

- message A key="user:1" → partition = hash("user:1") % 3
- message B key="user:1" → same partition as A
- message C key="user:2" → possibly different partition

This ensures per-key ordering **within the selected partition**.

#### Partition override
Producer sets `envelope.partition_override = 2`:
- message always goes to partition 2 (if topic has at least 3 partitions)
- if topic has only 2 partitions (0 and 1), produce fails

---

## DriftQ v1.3 — Durability / WAL (Semantics)

This document defines the **v1.3 durability contract** for DriftQ’s Write-Ahead Log (WAL). The WAL exists so a broker restart can restore topics/messages and consumer offsets.

### What we guarantee (and what we don’t)

Guaranteed / intended behavior (when WAL is enabled and functioning):
- Produced messages are appended to the WAL **before** being committed to in-memory state.
- Consumer offset progress is appended to the WAL **only when progress advances**.
- On startup, the broker can **replay** the WAL to rebuild:
  - topics and their partitions (as observed in the WAL)
  - messages (topic/partition/offset/key/value + supported metadata)
  - consumer offsets (topic/group/partition -> last acked offset)

Not guaranteed in v1.3:
- durability if WAL append fails (operations fail instead)
- compaction/cleanup/retention of WAL
- transactional coupling across multiple records (e.g., “message + offset” atomic bundles)
- durable in-flight retry state (attempt counters, last_error, backoff timers) unless added later
- protection against disk corruption beyond basic file append semantics

### Terms

- **WAL**: an append-only log of records used to reconstruct state on restart.
- **Replay**: reading WAL records and rebuilding in-memory state.
- **Record**: a typed WAL entry (message record, offset record, etc.).
- **Last-acked offset**: the stored consumer position per (topic, group, partition).

### Record types (v1.3)

v1.3 stores two core record types:

#### 1) Message record
Represents a produced message written to durable storage.

Fields (conceptually):
- `topic`
- `partition`
- `offset`
- `key`
- `value`
- optional metadata (routing label/meta, envelope fields that are supported)

#### 2) Offset record
Represents consumer progress.

Fields (conceptually):
- `topic`
- `group`
- `partition`
- `offset` (last acked)

### Produce durability semantics

When `Produce(topic, msg)` succeeds with WAL enabled:
1) The broker appends a **message record** to the WAL.
2) If WAL append succeeds, the broker commits the message to in-memory topic/partition state.
3) If WAL append fails, produce fails and the message is **not** committed to memory.

This makes the WAL the source of truth for restart recovery.

### Ack / offset durability semantics

When `Ack(topic, group, partition, offset)` is called:
- The broker treats stored offsets as “**last acked offset**”.
- If the incoming `offset` is **<=** the stored offset, the Ack is considered duplicate/late:
  - it may still remove the message from in-flight,
  - but it does **not** append to WAL.
- If the incoming `offset` is **>** the stored offset, it advances progress:
  1) append an **offset record** to the WAL
  2) update the stored offset in memory

Rule:
- The broker only appends offset records when the stored offset moves forward.

### Replay semantics (startup recovery)

On startup, with WAL enabled:
- The broker reads all WAL records in order.
- For each **message record**:
  - ensure topic exists
  - ensure partition exists (grow as needed)
  - append the message to the in-memory partition log
  - update topic’s `nextOffset` to at least `offset+1`
- For each **offset record**:
  - ensure consumer offsets maps exist
  - update stored offset to the max of current and the replayed offset

Important detail:
- Topic partition counts during replay are derived from observed message partitions in the WAL.
  - (If a topic was created with N partitions but never had messages in some partitions, replay may not “know” about those empty partitions unless creation is also logged. v1.3 accepts this limitation.)

### Failure and crash expectations

- If the broker crashes after WAL append but before responding, a client may retry produce.
- Without additional producer idempotency, this could cause duplicates.
- v1.6 introduces idempotency semantics; this doc focuses only on WAL durability.

If WAL is not enabled (nil WAL):
- all state is in-memory only and is lost on restart.

### Non-goals for v1.3

- WAL compaction, retention, or segmenting
- checksums / corruption repair
- durable retry state (attempt counts, backoff schedule, last_error) — tracked separately
- durable topic creation records (topic metadata in WAL)

### Example flow

#### Restart recovery
1) Broker runs with WAL enabled and accepts produces + acks.
2) Broker process stops.
3) Broker restarts.
4) Broker replays WAL:
   - rebuilds topics/partitions/messages
   - restores consumer offsets
5) Broker resumes dispatch based on restored in-memory state.

---

## DriftQ v1.4 — Agent message envelope (run/step IDs + routing metadata) (Semantics)

> Note: The broker envelope supports extra metadata like `labels` and `dlq` internally, but the current **/v1 HTTP JSON schema does not accept those fields** (unknown fields are rejected).  
> In other words: you can read them in the code, but don’t send them in `/v1/produce` yet.

This document defines the **v1.4 contract** for DriftQ’s message envelope and routing metadata. The envelope is how DriftQ becomes “agent-native”: it carries workflow identity, routing controls, and policy knobs alongside the message payload.

### What we guarantee (and what we don’t)

Guaranteed / intended behavior:
- A message may include an optional **Envelope** that carries workflow/agent metadata.
- Envelope fields are treated as **metadata** (they do not change the payload bytes).
- The broker preserves the envelope on delivery (and when WAL is enabled, persists supported fields).
- The broker supports basic routing controls via the envelope:
  - `target_topic` (topic override)
  - `partition_override` (force a specific partition)
- The broker attaches optional **routing metadata** (label + meta map) to the message when a router is configured.

Not guaranteed in v1.4:
- any enforcement of labels (labels are informational only)
- authz/governance policies (tenant is carried but not enforced)
- schema validation beyond basic parsing/typing
- durable “exactly-once” semantics (handled in v1.6 via idempotency at the worker boundary)

### Envelope fields (v1.4)

#### Workflow identity
- `run_id`: identifier for an agent run / workflow execution
- `step_id`: identifier for a step/task within the run
- `parent_step_id`: optional parent step identifier for lineage

Intended use:
- tracing agent pipelines
- correlating retries to the same workflow step
- building “workflow-aware” tooling later (replay, observability, governance)

#### Labels
- `labels`: map of arbitrary key/value labels

Intended use:
- tagging (environment, model, priority, customer, feature flags)
- future policy routing / filtering / observability

#### Routing controls
- `target_topic`: if set, the broker routes the message to this topic instead of the producer-requested topic.
- `partition_override`: if set, the broker routes to that partition index (must be in range).

Priority:
- If a router is configured, router decisions may overwrite these routing controls (router “wins” for MVP).

#### Reliability / policy knobs (carried in v1.4)
v1.4 introduces these fields on the envelope so later milestones can use them:

- `idempotency_key` (used in v1.6 for dedupe)
- `deadline` (reject produce if already expired; used for time-bounded work)
- `retry_policy` (used in v1.6 for retry scheduling)
- `tenant_id` (carried now for future governance/multi-tenancy)

Note: In v1.4, these fields may be present even if not fully enforced beyond basic behavior.

### Routing metadata (router output)

If a router is configured, the broker can attach routing metadata to a message:
- `routing.label`: high-level label for observability/policy
- `routing.meta`: arbitrary string map for extra metadata (scores, source, etc.)

This metadata is informational in v1.4 and should not be relied on for correctness.

### How envelope interacts with produce/consume

#### Produce
- Producer sends `topic`, `key`, `value`, and optionally `envelope`.
- Broker determines the **final topic**:
  - default: the producer’s `topic`
  - overridden by `envelope.target_topic` if set (unless router overrides it)
- Broker determines the **partition**:
  - default: hash(key) % N (or partition 0 if key empty)
  - overridden by `envelope.partition_override` if set (unless router overrides it)

#### Consume
- Consumer receives the message payload plus:
  - `partition`, `offset`, `attempts`
  - `envelope` (if present)
  - `routing` (if present)

### Durability notes (WAL)

When WAL is enabled, supported envelope fields are persisted with the message record so they can be restored on replay.
(Exact field coverage depends on the current WAL entry schema. v1.4 contract is that envelope fields are preserved across delivery; durability is “best-effort per supported fields” until the WAL schema is extended.)

### Non-goals for v1.4

- enforcing tenant governance / quotas
- priority scheduling based on labels
- policy-based routing beyond basic router hook (v1.5)
- DLQ behavior (later milestone)
- schema registry / typed message envelopes

### Example envelope (HTTP-safe)

```json
{
  "run_id": "run_123",
  "step_id": "step_7",
  "parent_step_id": "step_3",
  "tenant_id": "tenant_a",
  "target_topic": "tasks.enrich",
  "partition_override": 1,
  "idempotency_key": "tenant_a:run_123:step_7",
  "deadline": "2025-12-21T12:00:00Z",
  "retry_policy": { "max_attempts": 5, "backoff_ms": 250, "max_backoff_ms": 5000 }
}
```

If you need arbitrary “labels” today, put them in `routing.meta` (router output) or inside your message payload.
### Example flow (agent pipeline)

1) Agent orchestrator produces a message with `run_id` and `step_id` for “summarize report”.
2) Router labels it `routing.label="summarization"` with `meta={"model":"gpt-4.1"}`.
3) Worker consumes it, uses `run_id/step_id` for tracing, and processes it.
4) On failure, retry policy + idempotency (v1.6) ensure safe retries without double side effects.

---

## DriftQ v1.5 — AI-native router hook (Brain v0) (Semantics)

This document defines the **v1.5 contract** for the “brain hook” (router) in DriftQ-Core. The goal is to make DriftQ **AI-native** by letting the core ask an external “brain” where a message should go and how it should be labeled—without embedding any AI dependencies in the core.

### What we guarantee (and what we don’t)

Guaranteed / intended behavior:
- DriftQ-Core exposes a **Router interface** (the “brain contract”).
- On produce, if a router is configured, the broker calls the router with `(ctx, producerTopic, msg)`.
- If the router returns successfully, DriftQ-Core stores the router’s decision as message metadata:
  - `routing.label`
  - `routing.meta`
- The router may also request routing controls:
  - `target_topic`
  - `partition_override`
- Router routing controls override producer-specified routing controls (router “wins” for MVP).
- If no router is configured, behavior is unchanged (normal produce semantics).

Not guaranteed in v1.5:
- any specific AI model or tooling (Agno/OpenAI/etc. live elsewhere)
- correctness of router decisions (router is advisory; core just applies it)
- policy enforcement, authz, quotas
- retries around router failures (router errors are currently “best-effort”)
- stable taxonomy for labels/meta (that evolves)

### Why this exists

Up to v1.4, DriftQ is “just” a solid queue:
- topics + partitions
- consumer groups + offsets + acks
- WAL + replay
- agent envelope metadata

v1.5 is where DriftQ becomes AI-native:
- The broker can ask a brain: **“what is this message?”** and **“where should it go?”**
- The decision is stored with the message so consumers, observability, and downstream tooling can use it.

### Router interface contract

Router is defined as:

- Input:
  - `topic`: the topic the producer requested (before any overrides)
  - `msg`: the message (payload + envelope if present)
- Output:
  - `RoutingDecision`:
    - `Label` (string)
    - `TargetTopic` (string, optional)
    - `PartitionOverride` (*int, optional)
    - `Meta` (map[string]string)

#### Router call semantics
- The router is invoked **during Produce**, before the message is committed to memory/WAL.
- If the router returns `(decision, nil)`:
  - `routing.label` and `routing.meta` are set on the message.
  - If `decision.target_topic` or `decision.partition_override` are set, they are applied (by writing into the message’s envelope).
- If the router returns an error:
  - v1.5 treats this as non-fatal to core behavior:
    - core can proceed without routing metadata, or skip applying overrides
  - (Exact behavior is implementation-defined for MVP; the stable contract is that router failure should not crash the broker.)

### Precedence rules

When multiple actors can influence routing:

1) **Router** (if configured and returns successfully)
2) **Producer envelope routing controls**
3) **Default routing logic** (hash(key) / partition 0 if key empty)

So:
- Router override wins over producer `target_topic` / `partition_override`.
- Producer routing controls win over default hashing.

### Stored metadata (what consumers see)

When routing is present, consumers receive:

- `routing.label`: string
- `routing.meta`: string map

This metadata is informational (for now). It can be used for:
- worker selection / filtering
- metrics and dashboards
- future policy routing or governance
- debugging (why did this message go here?)

### Durability notes (WAL)

When WAL is enabled, routing metadata is persisted with message records (as supported by the WAL entry schema), so it is restored on replay.

### Non-goals for v1.5

- running LLM calls inside the core
- embedding-specific storage, vector routing, policy DSLs
- retries/backoff around router calls
- router-driven fanout/multi-route (single route only in v1.5)
- security/governance enforcement

### Example decision

Router returns:

```json
{
  "label": "finance:classification",
  "target_topic": "tasks.finance.classify",
  "partition_override": null,
  "meta": {
    "source": "agno-router",
    "model": "gpt-4.1",
    "score": "0.92"
  }
}
```

Resulting behavior:
- message is stored with `routing.label` + `routing.meta`
- message is routed to `tasks.finance.classify` instead of producer topic
- consumer receives the routing metadata along with envelope/payload

### Practical worker pattern (preview)

Workers can use `routing.label` to decide how to handle messages, and use v1.6 idempotency to dedupe side effects across retries.

---

## DriftQ v1.6 — Idempotency + “Exactly-once-ish effects” (Semantics)

> Implementation note (current /v1 HTTP API): consumer-side idempotency is tied to **(topic, group, owner, lease)**.  
> `/v1/consume` requires `owner` (and optional `lease_ms`), and `/v1/ack` + `/v1/nack` require the same `owner` or they return **409**.

This document defines the **v1.6 contract** so future changes don’t drift the meaning of “exactly-once-ish.”

### What we guarantee (and what we don’t)

DriftQ v1.6 does **not** guarantee exactly-once delivery.

It guarantees **exactly-once-ish side effects at the consumer/worker boundary** *when the worker uses the idempotency helper correctly*.

That means:
- A message may be delivered more than once (at-least-once delivery behavior).
- The worker can ensure it does **not execute the same side effect twice** for the same idempotency identity.

Not guaranteed in v1.6:
- exactly-once delivery
- distributed transactions across external systems (DB/payment APIs/etc.)
- durable in-flight state (unless explicitly persisted in later work)

### Key terms

- **Delivery attempt**: each time the broker sends a message to a consumer channel.
  - Exposed as `attempts` on the delivered message.
  - Attempts increment **on delivery**, not on produce.
- **Ack**: consumer confirms success; broker advances the consumer offset.
- **Nack**: consumer reports failure and provides a failure reason; broker keeps the message eligible for retry.
- **last_error (message)**: latest known failure reason for a message offset while it is in-flight (v1.6: in-memory).
- **Idempotency status**: the broker’s registry state for an idempotency key.

### Idempotency identity / scope

An idempotent operation is identified by the tuple:

**(tenant_id, topic, idempotency_key)**

Notes:
- `tenant_id` may be empty; it is still part of the tuple (empty string is a valid value).
- `topic` is included in the tuple in v1.6. The same `idempotency_key` used in two different topics is treated as **different** operations.
- If `idempotency_key` is empty, **no idempotency behavior** is applied.

### Idempotency store status model (v1.6)

The in-memory idempotency registry stores:

- `PENDING`: operation started (in-flight)
- `COMMITTED`: operation completed successfully
- `FAILED`: operation failed

v1.6 behavior:
- `Begin()` creates/sets `PENDING`.
- `Commit()` sets `COMMITTED`.
- `Fail()` sets `FAILED`.
- If a key is `FAILED`, v1.6 allows retry by replacing `FAILED → PENDING` on a new `Begin()`.

### Producer-side idempotency (broker gate)

When a message includes `envelope.idempotency_key`, the broker performs a lightweight dedupe gate in `Produce()`.

Behavior:
- If the idempotency identity is already `COMMITTED`, `Produce()` is treated as success and the broker **does not enqueue a duplicate message**.
- If the identity is currently `PENDING`, the broker rejects the duplicate produce attempt (to avoid duplicates while the operation is in-flight).
- If the produce path fails after `Begin()`, the broker records `FAILED`.

This prevents duplicated *enqueued messages* for the same key. It does **not** guarantee exactly-once effects unless the consumer follows the consumer-side contract below.

### Consumer-side idempotency (exactly-once-ish effects)

To get “exactly-once-ish effects,” the worker MUST:

1) **Begin / Check “already done?”**
   - Use the broker’s idempotency helper on the consumer side.
   - If the helper reports “already committed,” the worker must **skip side effects** and treat the message as success (Ack it).

2) **Execute side effects**
   - Do the DB write / external call / etc.

3) **Record outcome**
   - On success: `MarkSuccess(...)` and then `Ack`.
   - On failure: `MarkFailure(...)` and optionally `Nack(reason=...)` (or simply do not Ack and let timeout retry happen).

Contract:
- If the same `(tenant_id, topic, idempotency_key)` is delivered again and the registry is `COMMITTED`, the worker can avoid double side effects.

### Retries and redelivery

- Messages become eligible for retry if they are delivered and not acked within `ackTimeout`.
- If `envelope.retry_policy` exists:
  - `max_attempts` limits delivery attempts (attempts count = number of deliveries).
  - `backoff_ms` and `max_backoff_ms` control retry scheduling/backoff.
- If `retry_policy` is absent, messages may still be redelivered due to timeout (MVP behavior).

#### Max attempts behavior (current v1.6 behavior)
If `max_attempts` is set and the message reaches that attempt count:
- the broker stops redelivering it
- the broker advances the consumer offset so it won’t resurrect on restart

DLQ routing is planned and tracked separately (see below).

### Failure reasons (`last_error`)

#### Message `last_error`
DriftQ tracks the latest failure reason per in-flight message offset.

- If the consumer calls `Nack(..., reason)`, the broker records that string as `last_error` for that message offset.
- If a message times out (no ack) and no explicit reason was recorded, the broker may set:
  - `last_error = "ack_timeout"`
- `last_error` is surfaced to consumers in the delivered message payload.

**v1.6 scope:** message `last_error` is **in-memory** and is lost on broker restart (durability may be added in a later PR).

#### Idempotency store `LastError`
The idempotency registry also stores a `LastError` string for `FAILED` operations.
This is a different concept from message `last_error`:
- idempotency `LastError` describes the operation keyed by `(tenant_id, topic, idempotency_key)`
- message `last_error` describes the most recent delivery failure reason for a specific message offset while retrying

### DLQ interaction (planned)

If a message exceeds retry policy (or is later marked non-retryable), DriftQ will route it to a DLQ topic (e.g., `dlq.<topic>`).

DLQ messages should include metadata:
- original `topic / partition / offset`
- `attempts`
- message `last_error`
- envelope fields like `tenant_id` and `idempotency_key`

### Example flows

#### Success on first attempt
- attempt=1 delivered
- worker does side effect
- worker `MarkSuccess` + `Ack`
- offset advances; no redelivery

#### Failure then success
- attempt=1 delivered
- worker fails and `Nack(reason="db_deadlock")` (or just no Ack)
- broker redelivers (attempt=2+) with `last_error` set
- worker succeeds, `MarkSuccess` + `Ack`
- retries stop

#### Duplicate delivery (consumer dedupe)
- attempt=1 delivered; worker succeeds; marks committed; acks
- message is delivered again (redelivery / duplicate / crash timing)
- worker begins and sees already committed → skips side effects → acks
