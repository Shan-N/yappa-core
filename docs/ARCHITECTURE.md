# Architecture Deep Dive

## System Overview

realtime-ws is a **stateless WebSocket server** designed for multi-tenant real-time messaging. Each server instance holds ephemeral connection state in memory and relies on Redis Pub/Sub for cross-instance message delivery and Kafka + PostgreSQL for durable persistence.

---

## Component Breakdown

### 1. Entry Point (`main.rs`)

Loads environment variables from `.env`, initializes the `tracing` subscriber for structured logging, and delegates to `app::run()` with the four required config values.

### 2. Application Layer (`app.rs`)

**`AppState`** is the shared state passed to every Axum handler:

| Field      | Type                   | Purpose                                      |
|------------|------------------------|----------------------------------------------|
| `auth`     | `Auth`                 | JWT validation (Arc-wrapped, clone-cheap)     |
| `registry` | `ConnectionRegistry`   | In-memory connection + group tracking         |
| `kafka`    | `Kafka`                | Producer + consumer handle                    |
| `pubsub`   | `Arc<RedisManager>`    | Redis publish + subscribe                     |

**Startup sequence:**

1. Connect to PostgreSQL (`sqlx::PgPool`)
2. Run SQL migrations (idempotent `CREATE TABLE IF NOT EXISTS`)
3. Initialize Kafka (producer + consumer) with the PgPool
4. Build `AppState`
5. Spawn Redis Pub/Sub listener as a background task
6. Spawn Kafka consumer as a background task
7. Bind Axum router to `0.0.0.0:8080`
8. Serve with graceful shutdown on CTRL+C

### 3. Authentication (`auth/`)

```
auth/
├── mod.rs      Auth struct wrapping AuthConfig in Arc
├── jwt.rs      AuthConfig — HS256 decoding key + validation
└── claims.rs   Claims struct (tenant_id, user_id, exp)
```

- **Algorithm:** HS256 (HMAC-SHA256)
- **Required claims:** `tenant_id`, `user_id`, `exp`
- **Output:** `Identity { tenant_id, user_id }` (used for connection scoping)
- **Where:** Validated during HTTP upgrade, before the WebSocket is established

### 4. Connection Registry (`connection/mod.rs`)

A lock-free, concurrent in-memory data structure using `DashMap`:

```
Connections:  tenant_id → user_id → connection_id → mpsc::UnboundedSender<Message>
Groups:       tenant_id → group_id → DashSet<user_id>
```

**Key operations:**

| Method              | Complexity | Description                                       |
|---------------------|------------|---------------------------------------------------|
| `insert()`          | O(1)       | Register a new WebSocket connection               |
| `remove()`          | O(1)       | Unregister on disconnect, clean up empty maps     |
| `join_group()`      | O(1)       | Add user to a tenant-scoped group                 |
| `leave_group()`     | O(1)       | Remove user from a group                          |
| `send_msg_to_user()`| O(k)       | Send to all k connections of a user               |
| `send_msg_to_group()`| O(n×k)   | Fan-out to n members × k connections each         |

**Design notes:**
- Each WebSocket connection gets a unique UUID (`ConnectionId`)
- A single user can have multiple concurrent connections (multi-device)
- Group membership is per-tenant, in-memory only (lost on restart)

### 5. WebSocket Handler (`server/ws.rs`)

The handler lifecycle:

1. **Upgrade:** Extract Bearer token → validate JWT → upgrade to WebSocket
2. **Setup:** Split socket into sender/receiver halves, create unbounded mpsc channel, register in registry
3. **Send task:** Spawned tokio task that drains the mpsc channel → writes to WebSocket
4. **Recv task:** Spawned tokio task that reads WebSocket → parses JSON → routes:
   - `GroupMessage` → join/leave via registry
   - `WsMessage` → construct `ServerMessage` → publish to Redis + produce to Kafka
5. **Teardown:** When either task exits (client disconnect or error), remove from registry

### 6. Redis Pub/Sub (`redis/mod.rs`)

**Publishing:**

| Method          | Redis Channel                    | Used For         |
|-----------------|----------------------------------|------------------|
| `publish()`     | `user:{tenant_id}:{user_id}`    | DMs              |
| `publish_grp()` | `group:{group_id}`              | Groups/Community |

**Listener (single background task):**

- Pattern-subscribes to `user:*:*` and `group:*`
- Deserializes `ServerMessage` from each Redis message
- DM: delivers to recipient + echoes to sender via registry
- Group: looks up group members via registry, fan-out to each

### 7. Kafka (`kafka/`)

**Producer (`producer.rs`):**

- librdkafka `FutureProducer` with LZ4 compression
- Batched internally (up to 10K messages, 5ms linger)
- Leader-ack only (`acks=1`) for throughput
- Keyed by `channel_id` for partition locality

**Consumer (`consumer.rs`):**

- `StreamConsumer` with manual commit (no auto-commit)
- Batched consumption: up to 500 messages or 250ms interval
- On flush: delegates to `MessageBatcher` for bulk DB insert
- Commits offsets only after successful DB write

### 8. Database Layer (`db/mod.rs`)

**`MessageBatcher`:**

- Accumulates `ServerMessage` structs in a buffer
- On capacity threshold: performs bulk `INSERT` using PostgreSQL's `UNNEST` array expansion
- Single round-trip inserts multiple rows

**SQL:**
```sql
INSERT INTO messages (message_id, tenant_id, conversation_id, channel_type, channel_id, sender_id, content, created_at)
SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::uuid[], $4::text[], $5::text[], $6::text[], $7::text[], $8::timestamptz[])
```

---

## Data Flow Diagrams

### DM: user1 → user2

```
user1 (wscat)                    Server                          Redis                        user2 (wscat)
     │                              │                              │                              │
     │  {"channel_type":"DM",       │                              │                              │
     │   "user_id":"user2",         │                              │                              │
     │   "content":"hi"}            │                              │                              │
     │─────────────────────────────►│                              │                              │
     │                              │  PUBLISH user:t1:user2       │                              │
     │                              │─────────────────────────────►│                              │
     │                              │                              │  psubscribe match            │
     │                              │◄─────────────────────────────│                              │
     │                              │                              │                              │
     │                              │  registry.send(t1, user2)   │                              │
     │                              │─────────────────────────────────────────────────────────────►│
     │                              │  registry.send(t1, user1)   │                              │
     │◄─────────────────────────────│  (echo to sender)           │                              │
     │                              │                              │                              │
     │                              │  Kafka produce (async)      │                              │
     │                              │──────► Kafka ──────► Consumer ──────► PostgreSQL            │
```

### Group: user1 → group1 (members: user1, user2, user3)

```
user1 (wscat)                    Server                          Redis
     │                              │                              │
     │  {"channel_type":"GROUP",    │                              │
     │   "user_id":"group1",        │                              │
     │   "content":"hey all"}       │                              │
     │─────────────────────────────►│                              │
     │                              │  PUBLISH group:group1        │
     │                              │─────────────────────────────►│
     │                              │                              │  psubscribe match
     │                              │◄─────────────────────────────│
     │                              │                              │
     │                              │  lookup group1 members       │
     │                              │  → [user1, user2, user3]     │
     │                              │                              │
     │◄─────────────────────────────│  send to user1               │
     │                              │─────────────────────────────────► user2
     │                              │─────────────────────────────────► user3
```

---

## Concurrency Model

```
                    ┌─────────────────────────────────┐
                    │        Tokio Runtime             │
                    │    (multi-threaded, work-stealing)│
                    └─────────────────────────────────┘
                                   │
                    ┌──────────────┼──────────────────┐
                    │              │                   │
               ┌────┴────┐  ┌─────┴─────┐    ┌───────┴───────┐
               │  Axum   │  │  Redis    │    │    Kafka      │
               │ Handler │  │ Listener  │    │   Consumer    │
               │ (per    │  │ (1 task)  │    │   (1 task)    │
               │  conn)  │  └───────────┘    └───────────────┘
               └────┬────┘
                    │
          ┌─────────┼─────────┐
          │                   │
    ┌─────┴─────┐      ┌─────┴─────┐
    │  Send     │      │  Recv     │
    │  Task     │      │  Task     │
    │ (per conn)│      │ (per conn)│
    └───────────┘      └───────────┘
```

- Each WebSocket connection spawns **2 tokio tasks** (send + recv)
- Total tasks per connection: 2
- At 10K connections: 20K lightweight tasks (trivial for Tokio)
- Shared state accessed via `DashMap` (sharded, lock-free reads)

---

## Multi-tenancy Isolation

All data paths are scoped by `tenant_id`:

| Layer              | Isolation mechanism                                    |
|--------------------|--------------------------------------------------------|
| Connections        | `DashMap<tenant_id, DashMap<user_id, ...>>`           |
| Groups             | `DashMap<tenant_id, DashMap<group_id, ...>>`          |
| Redis channels     | `user:{tenant_id}:{user_id}`                          |
| PostgreSQL         | `tenant_id` column + index                            |
| JWT claims         | `tenant_id` extracted and immutable per connection    |

Cross-tenant data access is impossible at the protocol level — the `tenant_id` is derived from the JWT, not from client-supplied data (for DMs; group joins currently accept client-supplied `tenant_id`).

---

## Persistence Pipeline

```
WS Handler ──► Kafka Producer ──► Kafka Topic "messages" ──► Kafka Consumer ──► MessageBatcher ──► PostgreSQL
                  (async)             (partitioned by          (batch loop)       (UNNEST bulk)
                                       channel_id)
```

**Guarantees:**
- Messages are produced to Kafka **after** Redis publish (fire-and-forget to Kafka on the hot path)
- Kafka consumer uses **manual commits** — offsets are committed only after successful DB flush
- At-least-once delivery to the database (duplicates possible on crash between write and commit; `message_id` PK prevents duplicate rows)

---

## Graceful Shutdown

1. CTRL+C triggers `shutdown_signal()`
2. Axum stops accepting new connections, drains existing ones
3. Kafka consumer receives shutdown notification, flushes remaining buffer, commits offsets
4. Process exits
