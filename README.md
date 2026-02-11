# realtime-ws

A high-performance **multi-tenant real-time WebSocket messaging server** built in Rust. Supports direct messages, group chats, and community channels with cross-instance delivery via Redis Pub/Sub and durable persistence through Kafka → PostgreSQL.

## Architecture

```
Client (wscat / browser / mobile)
    │
    │  WS Upgrade + Bearer JWT
    ▼
┌──────────────────────────────────┐
│         Axum HTTP Server         │
│  GET /health → health check      │
│  GET /ws     → WebSocket upgrade  │
└──────────────┬───────────────────┘
               │
       ┌───────┴────────┐
       │   JWT Auth      │  HS256 · tenant_id + user_id + exp
       └───────┬────────┘
               │
       ┌───────┴────────────────┐
       │   WebSocket Handler     │
       │  • Parse JSON messages  │
       │  • Route by type        │
       │  • Group join/leave     │
       └───────┬────────────────┘
               │
       ┌───────┴────────────────────────┐
       │                                │
       ▼                                ▼
┌──────────────────┐          ┌──────────────────┐
│  Redis Pub/Sub   │          │   Connection     │
│  • publish DM    │          │   Registry       │
│  • publish group │          │   (DashMap)      │
│  • listener      │          │  • per tenant    │
└──────┬───────────┘          │  • per user      │
       │                      │  • per connection│
       │                      │  • per group     │
       │                      └──────────────────┘
       │                                │
       └──── listener dispatches ───────┘
              messages to local
              connections via registry
               │
       ┌───────┴────────────────┐
       │    Kafka Producer       │
       │  (async, batched,       │
       │   LZ4 compressed)      │
       └───────┬────────────────┘
               │
       ┌───────┴────────────────┐
       │   Kafka Consumer        │
       │  (batch up to 500 msgs  │
       │   or 250ms interval)    │
       └───────┬────────────────┘
               │
       ┌───────┴────────────────┐
       │   PostgreSQL            │
       │   (bulk UNNEST insert)  │
       └────────────────────────┘
```

## Tech Stack

| Component       | Technology                                    |
|-----------------|-----------------------------------------------|
| Language        | Rust (Edition 2024)                           |
| Async Runtime   | Tokio (full features)                         |
| HTTP / WS       | Axum 0.8                                      |
| Authentication  | JWT HS256 via `jsonwebtoken`                  |
| Pub/Sub         | Redis (`tokio-comp` async)                    |
| Persistence     | Kafka → PostgreSQL (via `rdkafka` + `sqlx`)   |
| Concurrency     | DashMap (lock-free concurrent hashmaps)        |
| Serialization   | serde + serde_json                            |
| Logging         | tracing + tracing-subscriber                  |
| Config          | dotenv                                        |
| IDs             | UUID v4                                       |

## Prerequisites

- **Rust** (edition 2024, stable toolchain)
- **Redis** (6.0+)
- **Apache Kafka** (with the `messages` topic created)
- **PostgreSQL** (14+)

### Quick start with Docker

```bash
# Redis
docker run -d --name redis -p 6379:6379 redis

# Kafka (KRaft mode, no Zookeeper)
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_CFG_NODE_ID=0 \
  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9093 \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  bitnami/kafka:latest

# Create the messages topic
docker exec kafka kafka-topics.sh \
  --create --topic messages \
  --bootstrap-server localhost:9092 --if-not-exists

# PostgreSQL
docker run -d --name postgres -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=realtime \
  postgres
```

## Setup

### 1. Environment variables

Create a `.env` file in the project root:

```env
JWT_SECRET=your-secret-key
REDIS_URL=redis://127.0.0.1:6379
KAFKA_BROKERS=localhost:9092
DATABASE_URL=postgres://postgres:postgres@localhost:5432/realtime
```

### 2. Build & Run

```bash
cargo build --release
cargo run
```

The server starts on `0.0.0.0:8080`. Database migrations run automatically on startup.

### 3. Health check

```bash
curl http://localhost:8080/health
# 200 OK
```

## Authentication

JWT tokens are validated on WebSocket upgrade. Tokens must be signed with **HS256** using the `JWT_SECRET` and contain these claims:

```json
{
  "tenant_id": "tenant1",
  "user_id": "user1",
  "exp": 1770832821
}
```

Pass the token via the `Authorization` header during the WebSocket handshake:

```
Authorization: Bearer <JWT_TOKEN>
```

### Generating a test token

```bash
node -e "
const crypto = require('crypto');
const header = Buffer.from(JSON.stringify({alg:'HS256',typ:'JWT'})).toString('base64url');
const payload = Buffer.from(JSON.stringify({
  tenant_id: 'tenant1',
  user_id: 'user1',
  exp: Math.floor(Date.now()/1000) + 3600
})).toString('base64url');
const sig = crypto.createHmac('sha256', 'your-secret-key')
  .update(header+'.'+payload).digest('base64url');
console.log(header+'.'+payload+'.'+sig);
"
```

## Connecting

```bash
npm install -g wscat

wscat -c ws://localhost:8080/ws \
  -H "Authorization: Bearer <YOUR_TOKEN>"
```

## WebSocket Protocol

All messages are JSON. The server distinguishes message types by attempting to parse incoming text as either a **GroupMessage** (join/leave) or a **WsMessage** (chat), in that order.

### Channel Types

| Type        | Value         | Description                           |
|-------------|---------------|---------------------------------------|
| DM          | `"DM"`        | Direct message between two users      |
| Group       | `"GROUP"`     | Small bounded group chat              |
| Community   | `"COMMUNITY"` | Large open channel (fan-out to group) |

---

### Client → Server

#### Join a group

```json
{
  "msg_type": "JOIN",
  "tenant_id": "tenant1",
  "group_id": "group1",
  "user_id": "user1"
}
```

#### Leave a group

```json
{
  "msg_type": "LEAVE",
  "tenant_id": "tenant1",
  "group_id": "group1",
  "user_id": "user1"
}
```

#### Send a DM

The `user_id` field is the **recipient's** user ID:

```json
{
  "channel_type": "DM",
  "user_id": "user2",
  "content": "hello!"
}
```

#### Send a group / community message

The `user_id` field is the **group ID**:

```json
{
  "channel_type": "GROUP",
  "user_id": "group1",
  "content": "hey everyone!"
}
```

```json
{
  "channel_type": "COMMUNITY",
  "user_id": "community1",
  "content": "announcement!"
}
```

---

### Server → Client

All delivered messages use the `ServerMessage` envelope:

```json
{
  "type": "chat",
  "message_id": "550e8400-e29b-41d4-a716-446655440000",
  "tenant_id": "tenant1",
  "channel_type": "DM",
  "channel_id": "user2",
  "sender_id": "user1",
  "timestamp": 1739280000,
  "conversation_id": "660e8400-e29b-41d4-a716-446655440000",
  "payload": {
    "text": "hello!",
    "meta": {}
  }
}
```

| Field             | Type   | Description                                       |
|-------------------|--------|---------------------------------------------------|
| `type`            | string | Always `"chat"`                                   |
| `message_id`      | UUID   | Unique message identifier                         |
| `tenant_id`       | string | Tenant scope                                      |
| `channel_type`    | string | `"DM"`, `"GROUP"`, or `"COMMUNITY"`               |
| `channel_id`      | string | Recipient user ID (DM) or group ID (GROUP/COMMUNITY) |
| `sender_id`       | string | Sender's user ID                                  |
| `timestamp`       | u64    | Unix timestamp (seconds)                          |
| `conversation_id` | UUID   | Conversation identifier                           |
| `payload.text`    | string | Message text                                      |
| `payload.meta`    | object | Arbitrary metadata                                |

## Message Flow

### DM flow

1. Client sends `WsMessage` with `channel_type: "DM"` and `user_id: "<recipient>"`
2. Server constructs `ServerMessage`, publishes to Redis channel `user:{tenant_id}:{recipient_id}`
3. Server produces the message to Kafka topic `messages` for persistence
4. Redis listener on each node receives the publish, delivers to **both** recipient and sender via the connection registry
5. Kafka consumer batches messages and bulk-inserts into PostgreSQL

### Group flow

1. Users must first **join** a group by sending a `GroupMessage` with `msg_type: "JOIN"`
2. Client sends `WsMessage` with `channel_type: "GROUP"` and `user_id: "<group_id>"`
3. Server publishes to Redis channel `group:{group_id}`
4. Redis listener looks up all members of the group in the connection registry and delivers to each

## Project Structure

```
src/
├── main.rs                 Entry point — loads .env, starts app
├── app.rs                  AppState, router setup, server startup
│
├── auth/
│   ├── mod.rs              Auth struct, Identity type, authenticate()
│   ├── jwt.rs              AuthConfig, JWT validation (HS256)
│   └── claims.rs           JWT Claims struct
│
├── connection/
│   └── mod.rs              ConnectionRegistry — WS connections & groups
│
├── protocol/
│   └── mod.rs              ChannelType, ServerMessage, GroupMessage
│
├── redis/
│   └── mod.rs              RedisManager — publish, subscribe, listener
│
├── kafka/
│   ├── mod.rs              Kafka handle (producer + consumer)
│   ├── producer.rs         KafkaProducer — async send with LZ4
│   └── consumer.rs         KafkaConsumer — batched consumption loop
│
├── db/
│   └── mod.rs              MessageBatcher — bulk UNNEST insert
│
├── server/
│   ├── mod.rs              Module exports
│   ├── health.rs           GET /health → 200 OK
│   └── ws.rs               WebSocket upgrade, auth, message routing
│
└── migrations/
    └── 001_create_messages.sql   Messages table DDL
```

## HTTP Endpoints

| Method | Path      | Description                          |
|--------|-----------|--------------------------------------|
| GET    | `/health` | Returns `200 OK`                     |
| GET    | `/ws`     | WebSocket upgrade (requires JWT)     |

## Database Schema

```sql
CREATE TABLE messages (
    message_id      UUID PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    conversation_id UUID NOT NULL,
    sender_id       TEXT NOT NULL,
    content         TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Indexes: `tenant_id`, `conversation_id`, `created_at`.

## Multi-tenancy

All data is scoped by `tenant_id`:

- Connections are stored as `tenant_id → user_id → connection_id → sender`
- Groups are stored as `tenant_id → group_id → Set<user_id>`
- Redis channels include the tenant: `user:{tenant_id}:{user_id}`
- Messages in PostgreSQL include `tenant_id` for query isolation

## Horizontal Scaling

Multiple instances can run behind a load balancer. Redis Pub/Sub ensures messages published on one node are delivered to users connected to other nodes. Each node:

1. Registers its local connections in an in-memory `ConnectionRegistry`
2. Publishes all messages to Redis
3. Subscribes to all relevant Redis patterns and dispatches to local connections

## Configuration Reference

| Variable        | Required | Description                          | Example                                     |
|-----------------|----------|--------------------------------------|---------------------------------------------|
| `JWT_SECRET`    | Yes      | HMAC secret for JWT validation       | `my-super-secret-key`                       |
| `REDIS_URL`     | Yes      | Redis connection URL                 | `redis://127.0.0.1:6379`                    |
| `KAFKA_BROKERS` | Yes      | Kafka bootstrap servers              | `localhost:9092`                             |
| `DATABASE_URL`  | Yes      | PostgreSQL connection string         | `postgres://user:pass@localhost:5432/realtime` |

## License

MIT
