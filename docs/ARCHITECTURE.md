# Architecture

## Overview

realtime-ws is a multi-tenant WebSocket server. A client opens a single WS connection, authenticates with a JWT, and can then send/receive DMs, group messages, and community broadcasts. Cross-node delivery uses Redis Pub/Sub. Durable storage goes through Kafka into PostgreSQL.

## Startup sequence

`main.rs` loads env vars, `app::run()` does the rest:

1. Connect to Postgres (`PgPoolOptions`, max 5 conns), run migrations via `include_str!`
2. Create `Kafka` handle (wraps a `FutureProducer` + `StreamConsumer`)
3. Create `RedisManager` with a `ConnectionManager` for publishing and a raw client for subscribing
4. Build `AppState` — holds `Auth`, `ConnectionRegistry`, `Kafka`, `Arc<RedisManager>`
5. Spawn Redis listener task (`pubsub.listener()`) — blocks on `on_message()` stream
6. Spawn Kafka consumer task (`kafka.spawn_consumer()`) — runs the batch loop
7. Bind Axum router (`/health`, `/ws`) with CORS layer, listen on `0.0.0.0:{PORT}`
8. Await graceful shutdown (SIGTERM or Ctrl+C), then `kafka.shutdown()` to flush remaining buffer

## Connection lifecycle

```
Client                           Server
  │                                │
  ├── GET /ws + Bearer JWT ───────▶│ ws_handler()
  │                                │   ├── extract token from Authorization header or ?token= query
  │                                │   ├── auth.authenticate(token) → Identity{tenant_id, user_id}
  │                                │   └── ws.on_upgrade() → handle_socket()
  │                                │
  │◀── WS Upgrade ────────────────│
  │                                │   handle_socket():
  │                                │     ├── socket.split() → (sender, receiver)
  │                                │     ├── mpsc::channel(256) → (tx, rx)
  │                                │     ├── registry.insert(identity, conn_id, tx)
  │                                │     ├── ConnectionGuard created (RAII cleanup on drop)
  │                                │     ├── spawn send_task: rx.recv() → sender.send() with 5s timeout
  │                                │     └── spawn recv_task: select! { message | heartbeat_tick }
  │                                │
  │◀── Ping (every 15s) ─────────│
  ├── Pong ──────────────────────▶│   resets last_activity
  │                                │
  │  (if no activity for 30s)      │   → timeout, remove from registry, break
  │                                │
  ├── Close frame ───────────────▶│   → break recv loop
  │                                │
  │  (either task ends)            │   → select! aborts the other task
  │                                │   → ConnectionGuard dropped → registry.remove()
```

## ConnectionRegistry

The core in-memory data structure. Two nested `DashMap` trees, both wrapped in `Arc` so the registry is `Clone`:

```
inner: DashMap<tenant_id, DashMap<user_id, DashMap<connection_id, mpsc::Sender>>>

groups: DashMap<tenant_id, DashMap<group_id, DashSet<user_id>>>
```

One user can have multiple connections (different devices). Each connection gets its own `mpsc::Sender<Message>` with a bounded channel of 256. A user is identified by `(tenant_id, user_id)`, a connection by a random `Uuid`.

### Key operations

- **insert** — adds `(conn_id, sender)` under the user's map, creating tenant/user levels as needed
- **remove** — removes the connection, then garbage-collects empty user and tenant maps (careful to drop DashMap guards before mutating parents to avoid deadlock)
- **send_msg_to_user** — iterates all connections for a user, `try_send()` on each. If a channel is full or closed, the connection is marked stale and evicted after iteration (not during, to avoid deadlock on DashMap)
- **send_msg_to_group** — collects user IDs from the group's `DashSet`, then calls `send_msg_to_user` for each
- **join/leave/create/delete_group** — manipulate the `groups` DashMap

### Concurrency notes

DashMap is sharded internally (lock-free reads, per-shard write locks). The code is careful about guard lifetimes — it checks `should_remove_user` / `should_remove_tenant` flags and drops inner guards before mutating outer maps. Stale connections are collected into a `Vec` during iteration and removed afterward to avoid holding a read guard while calling `remove()`.

## Message routing

When `handle_text_message` receives JSON, it tries to parse as `GroupMessage` first (has `msg_type` field), then as `WsMessage` (has `channel_type` field). This ordering matters — a message that could parse as both will be treated as a group command.

### DM flow

```
sender client
    │
    ├── WsMessage { channel_type: DM, user_id: "recipient", content: "..." }
    │
    ▼
handle_text_message()
    ├── build ServerMessage with:
    │     message_id: random UUID
    │     conversation_id: SHA-256(sorted(sender, recipient))[0..16] as UUID
    │     channel_id: recipient user_id
    │     sender_id: sender user_id
    │     timestamp: SystemTime::now() as unix secs
    │
    ├── pubsub.publish() → Redis channel "user:{tenant}:{recipient}"
    │
    └── kafka.produce("messages", channel_id, json bytes)
```

### Group/Community flow

```
sender client
    │
    ├── WsMessage { channel_type: GROUP, user_id: "group_id", content: "..." }
    │
    ▼
handle_text_message()
    ├── build ServerMessage (conversation_id = parse group_id as UUID or random)
    ├── pubsub.publish_grp() → Redis channel "group:{group_id}"
    └── kafka.produce("messages", channel_id, json bytes)
```

### Group join

```
sender client
    │
    ├── GroupMessage { msg_type: JOIN, tenant_id, group_id, user_id }
    │
    ▼
handle_text_message()
    ├── registry.join_group(tenant, group, user)
    ├── build ServerMessage with msg_type: "group_join"
    └── pubsub.publish_grp() → Redis "group:{group_id}"
            │
            ▼
        Redis listener (all nodes)
            ├── sees msg_type == "group_join"
            ├── registry.join_group() on this node too (cross-node sync)
            └── registry.send_msg_to_group() → deliver to all members
```

This is how group membership propagates across nodes — the join event goes through Redis, and every node's listener calls `join_group()` locally.

## Redis layer

`RedisManager` holds two things:
- `Client` — used to create a dedicated `PubSub` connection for the listener
- `ConnectionManager` — a multiplexed connection used by `publish()` and `publish_grp()` (cheaply cloneable, reconnects automatically)

### Publishing

- DM: `PUBLISH user:{tenant_id}:{user_id} <json>`
- Group: `PUBLISH group:{group_id} <json>`

### Listener

A single task subscribes to patterns `user:*:*` and `group:*` using `psubscribe`. For each incoming message:

1. Deserialize to `ServerMessage`
2. If `msg_type == "group_join"` → call `registry.join_group()` (cross-node group sync) + fan out to group
3. If `channel_type == DM` → `registry.send_msg_to_user(tenant, channel_id, msg)` — delivers to recipient
4. If `channel_type == Group/Community` → `registry.send_msg_to_group(tenant, channel_id, msg)` — delivers to all members

The listener is what enables horizontal scaling. Node A publishes to Redis, Node B's listener picks it up and pushes to local connections.

## Kafka persistence pipeline

### Producer

`FutureProducer` with:
- LZ4 compression
- Idempotent writes (`enable.idempotence = true`)
- Batching: up to 10k messages per batch, 5ms linger
- 1GB librdkafka buffer
- All replicas ack (`acks = all`)

Messages are produced with `channel_id` as the Kafka key (so all messages for the same channel land on the same partition, preserving order within a conversation).

### Consumer

`StreamConsumer` with manual commits, group id `realtime-ws-nodes`. Runs a `tokio::select!` loop with `biased` priority:

1. **Shutdown signal** (highest priority) — flush remaining buffer, commit, exit
2. **Timer (250ms)** — flush whatever's accumulated since last flush
3. **Message recv** — deserialize `ServerMessage`, push to buffer. If buffer hits 500, flush immediately

So messages land in Postgres within at most 250ms, or sooner if volume is high enough to fill a batch of 500.

### Flush

`flush_batch()` creates a `MessageBatcher`, pushes all messages, calls `flush()`. Then commits offsets asynchronously.

## Database layer

`MessageBatcher` accumulates messages and bulk-inserts using PostgreSQL's `UNNEST`:

```sql
INSERT INTO messages (message_id, tenant_id, conversation_id, channel_type, channel_id, sender_id, content, created_at)
SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::uuid[], $4::text[], $5::text[], $6::text[], $7::text[], $8::timestamptz[])
```

Each column is collected into a separate `Vec`, then passed as array bind parameters. The batcher has a capacity of 1000 (though the Kafka consumer flushes at 500, so it typically won't hit this).

On insert failure, messages are reconstructed from the decomposed column vectors and re-buffered for retry on the next flush cycle.

### Schema

```sql
messages (
    message_id      UUID PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    conversation_id UUID NOT NULL,
    channel_type    TEXT NOT NULL DEFAULT '',
    channel_id      TEXT NOT NULL DEFAULT '',
    sender_id       TEXT NOT NULL,
    content         TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
```

Indexes on `tenant_id`, `conversation_id`, `created_at`. Migrations run automatically at startup via `sqlx::raw_sql(include_str!(...))`.

## Auth

Simple JWT validation. `AuthConfig` holds a `DecodingKey` (HS256 symmetric). `validate_jwt()` decodes the token, requires `exp` claim, extracts `tenant_id` + `user_id` into an `Identity` struct. The `Auth` wrapper makes `AuthConfig` cheaply cloneable via `Arc`.

Token can be passed two ways:
- `Authorization: Bearer <token>` header
- `?token=<token>` query parameter

The `ws_handler` checks the header first, falls back to query param.

## Conversation IDs

DMs get a deterministic conversation ID: sort the two user IDs lexicographically, join with `:`, SHA-256 hash, take first 16 bytes as a UUID. This means the same pair of users always gets the same conversation_id regardless of who sends first.

Group/Community messages try to parse the group_id as a UUID for the conversation_id. If that fails, a random UUID is generated.

## Multi-tenancy

Everything is keyed by `tenant_id`:
- Registry: `tenant → user → connections`
- Groups: `tenant → group → members`
- Redis channels include tenant: `user:{tenant}:{user}`
- DB rows carry `tenant_id`

Tenants are fully isolated — a user in tenant A can't see connections, groups, or messages from tenant B.

## Graceful shutdown

`shutdown_signal()` waits for either SIGTERM or Ctrl+C using `tokio::select!`. When triggered:
1. `axum::serve` stops accepting new connections
2. `kafka.shutdown()` — notifies the consumer via `Notify`, which flushes remaining buffer and commits
3. Server exits
