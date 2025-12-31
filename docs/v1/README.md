# DriftQ v1 HTTP API (MVP)

Base URL: `http://<host>:8080`

All API endpoints below are under `/v1/*` **except** `/metrics` (Prometheus) which is unversioned.

## Endpoints

### Health

**GET** `/v1/healthz`

Returns `200 OK`.

---

### Version

**GET** `/v1/version`

Returns JSON like:

```json
{"version":"dev","commit":"unknown","wal_enabled":true}
```

---

### Topics

**GET** `/v1/topics`

Lists topics.

**POST** `/v1/topics?name=<topic>&partitions=<n>`

Creates a topic.

Example:

```bash
curl -i -X POST "http://localhost:8080/v1/topics?name=t&partitions=1"
```

---

### Produce

**POST** `/v1/produce`

Accepts either:
- JSON body, or
- query params (useful for quick manual testing)

#### Query params (common)

- `topic` (required)
- `key` (optional)
- `value` (optional)

#### Envelope query params (optional)

- `tenant_id` (alias: `tenant`)
- `run_id`
- `step_id`
- `parent_step_id`
- `idempotency_key` (alias: `idem_key`)
- `deadline` (RFC3339 timestamp)
- `target_topic`
- `partition_override` (int)

#### Retry policy query params (optional)

- `retry_max_attempts`
- `retry_backoff_ms`
- `retry_max_backoff_ms`

Example:

```bash
curl -i -X POST "http://localhost:8080/v1/produce?topic=t&value=hello&retry_max_attempts=2"
```

If the broker is overloaded you may see `429 Too Many Requests` with a JSON error and `Retry-After`.

---

### Consume (streaming)

**GET** `/v1/consume`

Consumes messages for a consumer group. This endpoint **streams NDJSON**
(one JSON object per line) until the client disconnects.

#### Query params

- `topic` (required)
- `group` (required)
- `owner` (required)
- `lease_ms` (optional; defaults to 2000ms)

Example:

```bash
curl -N "http://localhost:8080/v1/consume?topic=t&group=g&owner=o&lease_ms=5000"
```

Output is a stream of `ConsumeItem` objects.

Important notes:
- If you never `ack`, messages will be redelivered after the lease/timeout path.
- If a message has a retry policy and exceeds `max_attempts`, it is routed to `dlq.<topic>`.

---

### Ack

**POST** `/v1/ack`

Marks a message as successfully processed (advances the committed offset if possible).

Query params:

- `topic` (required)
- `group` (required)
- `owner` (required)
- `partition` (required)
- `offset` (required)

Example:

```bash
curl -i -X POST "http://localhost:8080/v1/ack?topic=t&group=g&owner=o&partition=0&offset=0"
```

Returns `204 No Content` on success.

If the caller is not the current owner, returns `409 Conflict`.

---

### Nack

**POST** `/v1/nack`

Marks a message as failed (may schedule retry depending on policy).

Query params:

- `topic` (required)
- `group` (required)
- `owner` (required)
- `partition` (required)
- `offset` (required)
- `error` (optional; stored for debugging/metrics)

Example:

```bash
curl -i -X POST "http://localhost:8080/v1/nack?topic=t&group=g&owner=o&partition=0&offset=0&error=failed"
```

---

### Metrics (Prometheus)

**GET** `/metrics`

Exports Prometheus metrics:

- `inflight_messages{topic,group,partition}` (gauge)
- `consumer_lag{topic,group,partition}` (gauge)
- `dlq_messages_total{topic,reason}` (counter)
- `produce_rejected_total{reason}` (counter)

Example:

```bash
curl -s "http://localhost:8080/metrics" | findstr consumer_lag
```

## Error format

Most errors are JSON:

```json
{"error":"INVALID_ARGUMENT","message":"..."}
```

## Method handling

Calling an endpoint with the wrong HTTP method returns `405 Method Not Allowed`
and includes an `Allow` header.
