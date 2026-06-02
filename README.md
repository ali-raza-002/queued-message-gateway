# Async message delivery API

A message delivery service that answers fast and replies later. `POST /messages` stores the message and returns at once with a `message_id` and a per-conversation `seq`. A separate worker process picks the message up, asks a reply provider for an answer and writes the reply back. The client polls for the result. Built as a take-home exercise.

Postgres is both the database and the queue. Workers claim messages with `SELECT ... FOR UPDATE SKIP LOCKED`, so there is no broker to run. The schema enforces the ordering and idempotency rules, so they hold with any number of worker processes.

Stack: Python 3.12, FastAPI, SQLAlchemy 2.1 (async, asyncpg), PostgreSQL 16, pytest and ruff.

## What it does

- **Idempotent sends.** Repeating a `client_message_id` returns the original message with `200` and `Idempotent-Replay: true` instead of queueing a second one. This also holds when identical requests race.
- **Send order is reply order.** Each conversation numbers its messages (`seq`, gap-free). A message is not handed to a worker while an earlier one in the same conversation is still open, so a retried message is never overtaken.
- **Leases.** A claimed message carries a lease. If a worker dies mid-delivery, the reaper requeues the message when the lease expires.
- **Bounded retries.** Failures back off exponentially with jitter. After `MAX_ATTEMPTS` the message lands in `dead_letters`. A provider can raise `PermanentProviderError` to dead-letter a poison message at once.
- **Graceful shutdown.** On SIGTERM the worker gives in-flight messages a grace period, then hands the rest back without spending an attempt.
- **Database outages.** If the worker loses its database connection, it logs `database_unavailable` and keeps polling instead of exiting.
- **Pluggable replies.** `echo` (deterministic, offline), `simulated` (random delay and failure rate around echo or OpenAI, the default) and `openai` (any OpenAI-compatible endpoint).

## How a message moves

```mermaid
sequenceDiagram
    participant C as Client
    participant API as FastAPI
    participant DB as Postgres (tables + queue)
    participant W as Worker
    C->>API: POST /messages (client_message_id)
    API->>DB: insert once, allocate conversation seq
    API-->>C: 201 message_id, seq
    loop every POLL_INTERVAL_SECONDS
        W->>DB: claim due heads of conversations (SKIP LOCKED), take lease
        W->>W: reply provider
        W->>DB: COMPLETED, or retry with backoff, or dead letter
    end
    C->>API: GET /messages/{id}/status
    API-->>C: state, attempts, assistant_text
```

```mermaid
stateDiagram-v2
    [*] --> RECEIVED: POST /messages
    RECEIVED --> PROCESSING: claimed (attempt + 1)
    PROCESSING --> COMPLETED: provider replied
    PROCESSING --> RECEIVED: transient error, attempts left (backoff)
    PROCESSING --> RECEIVED: released on shutdown (attempt refunded)
    PROCESSING --> FAILED: attempts used up or permanent error
    PROCESSING --> TIMEOUT: lease expired (reaper)
    TIMEOUT --> RECEIVED: attempts left
    TIMEOUT --> FAILED: attempts used up
    FAILED --> [*]: row in dead_letters
    COMPLETED --> [*]
```

## Running with Docker Compose

```bash
cp .env.example .env    # optional: every value has a default
docker compose up --build
```

This starts `db`, `api` and `worker`. The API is on http://localhost:7221, with OpenAPI docs at `/docs`, and Postgres is on port 7222. The worker defaults to the `simulated` provider: 5 to 60 seconds of delay and a 20% failure rate, so retries and dead letters show up without any setup. For instant replies, set `REPLY_PROVIDER=echo` in `.env` and run `docker compose up -d worker`.

`.env` beside `docker-compose.yml` feeds the `${VAR}` values in the Compose file. Postgres reads `POSTGRES_PASSWORD` only when the volume is first created, so after changing it, run `docker compose down -v`.

## Running without Docker

SQLite works for local runs and for the default test suite, so Postgres and an API key are not needed:

```bash
cd backend
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

export DATABASE_URL="sqlite+aiosqlite:///./aila.db"
export REPLY_PROVIDER=echo

uvicorn app.main:app --port 7221   # terminal 1
python -m worker                   # terminal 2 (same exports)
```

Every setting is listed with its default in `backend/.env.example`. The app also reads a `backend/.env` file. SQLite serialises every write, so it is fine for trying the service but proves nothing about concurrent claims. The Postgres tests below cover those.

## API

| Endpoint | What it does |
|---|---|
| `POST /messages` | Queue a message. `201` when new, `200` + `Idempotent-Replay: true` on a repeat |
| `GET /messages/{message_id}/status` | State, attempts used and allowed, and the reply once there is one |
| `GET /conversations/{user_id}` | History in send order, grouped by conversation. Filters: `conversation_id`, `state`, `limit`, `offset` |
| `GET /dead-letters` | Messages that ran out of retries (`user_id`, `limit`, `offset`) |
| `GET /healthz` | Liveness |
| `GET /readyz` | Runs `SELECT 1`, so it returns `503` when the database is down |
| `GET /metrics` | Queue depth per state, as JSON |

### Examples

These were captured from the Compose stack. The full record, including the conversation listing, is in [`docs/api-session.json`](docs/api-session.json).

Send a message:

```bash
curl -s -X POST http://localhost:7221/messages \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"u1","conversation_id":"c1","text":"What is a tax deduction?","client_message_id":"msg-001"}'
```

```json
{
  "message_id": "6f8df625-8a4e-4c13-b47e-19dc60e9807d",
  "user_id": "u1",
  "conversation_id": "c1",
  "client_message_id": "msg-001",
  "seq": 1,
  "text": "What is a tax deduction?",
  "state": "RECEIVED",
  "assistant_text": null,
  "last_error": null,
  "attempt_count": 0,
  "provider": null,
  "created_at": "2026-11-07T20:24:08.007030Z",
  "updated_at": "2026-11-07T20:24:08.007030Z"
}
```

Sending the same body again answers `200 OK` with `Idempotent-Replay: true` and the same `message_id` and `seq`. The next message in `c1` gets `seq` 2.

Poll it once the worker has run (`GET /messages/6f8df625-.../status`):

```json
{
  "message_id": "6f8df625-8a4e-4c13-b47e-19dc60e9807d",
  "state": "COMPLETED",
  "assistant_text": "About What is a tax deduction? - the relevant rule is the standard deduction.",
  "last_error": null,
  "attempt_count": 1,
  "max_attempts": 3,
  "dead_lettered": false,
  "processing_started_at": "2026-11-07T20:24:08.346856Z",
  "completed_at": "2026-11-07T20:24:08.352126Z"
}
```

Blank text is refused with `422` (`"text must contain a non-whitespace character"`).

With the worker switched to `simulated` and `REPLY_FAIL_RATE=1.0`, a message is tried three times, backing off about 2 and then 4 seconds, and is then dead-lettered. `GET /dead-letters`:

```json
{
  "items": [
    {
      "id": "6bd21200-a230-4a2e-b554-5fff0831f08f",
      "message_id": "c1a5449b-a945-48a2-94a8-81c8c3845f4f",
      "user_id": "u2",
      "conversation_id": "c9",
      "seq": 1,
      "attempt_count": 3,
      "reason": "simulated upstream failure after 0.0s (attempt 3)",
      "created_at": "2026-11-07T20:24:18.303252Z"
    }
  ],
  "page": {"total": 1, "limit": 50, "offset": 0, "has_more": false}
}
```

`GET /metrics` at that point:

```json
{
  "received": 0, "processing": 0, "completed": 2, "failed": 1, "timeout": 0,
  "dead_letters": 1, "conversations": 2, "total": 3,
  "provider": "simulated", "max_attempts": 3, "strict_conversation_order": true
}
```

## Design notes

- **The claim is one statement.** `UPDATE messages SET state='PROCESSING', attempt_count=attempt_count+1, lease_expires_at=... WHERE id IN (SELECT id ... LIMIT n FOR UPDATE SKIP LOCKED) RETURNING ...`. Two workers polling at once skip each other's rows instead of blocking or double-claiming.
- **Writes are guarded by attempt.** Complete, retry and dead-letter all match on `state='PROCESSING' AND attempt_count=<the claimed attempt>`. A worker whose lease was reaped updates zero rows and logs `lease_lost` instead of overwriting the next attempt.
- **Delivery is at least once.** The provider is called between the claim and the acknowledgement. A crash in between means another attempt, so `ReplyRequest.idempotency_key` (`user:conversation:seq`) stays stable across retries, and the OpenAI provider sends it as `Idempotency-Key`.
- **`seq` comes from a counter row.** It comes from `UPDATE conversations SET next_seq = next_seq + 1 RETURNING`, which serialises writers per conversation. A lost idempotency race rolls back with its transaction, so no gap is left.
- **Head-of-line order can be switched off.** `STRICT_CONVERSATION_ORDER=false` drops the "no earlier open message" predicate when throughput matters more than order.
- **The worker never over-claims.** It claims only as many messages as it has free concurrency slots, because a claimed message is already spending its lease.

## Tests

```bash
cd backend
pytest              # SQLite, no services needed
ruff check . && ruff format --check .
```

`tests/test_postgres.py` is skipped unless `TEST_POSTGRES_URL` is set. It covers disjoint parallel claims, head-of-line order across four queues, racing idempotent replays, gap-free `seq` under 30 concurrent sends, and three workers delivering 20 messages in order. **It drops and recreates every table** in the database you point it at. To run it against the Compose database:

```bash
docker compose up -d db
docker compose exec db createdb -U aila aila_test
TEST_POSTGRES_URL=postgresql+asyncpg://aila:aila@localhost:7222/aila_test pytest
```

Use your own user and password if you changed them in `.env`.

## Limitations

- Tables are created on startup (`AUTO_CREATE_SCHEMA`). There are no migrations yet.
- No authentication: `user_id` is trusted as sent.
- Clients poll for replies. Server-sent events or websockets would be the next step.
- List endpoints use offset pagination.
- `/metrics` is JSON, not the Prometheus text format.

## License

MIT, see [LICENSE](LICENSE).
