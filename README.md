# Yappa-RT

**Production-grade, multi-tenant real-time WebSocket messaging infrastructure.**

Yappa-RT is a horizontally scalable WebSocket server designed for SaaS applications requiring real-time messaging. It supports direct messages (DM), group chats, and community channels with complete tenant isolation.

---

## Features

- **Multi-tenant by design** — Complete isolation between tenants with configurable user limits
- **Horizontal scalability** — Stateless servers with Redis pub/sub for cross-node messaging
- **JWT authentication** — Short-lived access tokens with refresh token rotation
- **Durable persistence** — Kafka-backed message storage with PostgreSQL
- **10 users/tenant limit** — Redis-backed atomic enforcement across all nodes
- **Optional Kafka** — Demo mode writes directly to PostgreSQL for simpler deployments

---

## Architecture

```
                    ┌─────────────────────┐
                    │    Load Balancer    │
                    └──────────┬──────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         ▼                     ▼                     ▼
   ┌───────────┐         ┌───────────┐         ┌───────────┐
   │  WS Node  │         │  WS Node  │         │  WS Node  │
   │ (Axum/    │         │ (Axum/    │         │ (Axum/    │
   │  Tokio)   │         │  Tokio)   │         │  Tokio)   │
   └─────┬─────┘         └─────┬─────┘         └─────┬─────┘
         │                     │                     │
         └─────────────────────┼─────────────────────┘
                               │
    ┌──────────────────────────┼──────────────────────────┐
    │                          │                          │
    ▼                          ▼                          ▼
┌────────┐              ┌────────────┐              ┌────────────┐
│ Redis  │              │   Kafka    │              │ PostgreSQL │
│Pub/Sub │              │ (optional) │              │            │
└────────┘              └────────────┘              └────────────┘
```

### Components

| Component | Tech | Purpose |
|-----------|------|---------|
| **yappa-rt** | Rust (Axum/Tokio) | WebSocket server, message routing, tenant limits |
| **yappa-auth** | Node.js (Express) | User authentication, JWT issuance, refresh tokens |
| **yappa-sdk** | TypeScript | Browser/Node.js client SDK with auto-reconnect |
| **Redis** | Redis 7 | Pub/sub for cross-node messaging, tenant limits, refresh tokens |
| **Kafka** | Kafka 3.7 | Durable message streaming (optional) |
| **PostgreSQL** | Postgres 16 | User storage, message persistence |

---

## Quick Start

### Prerequisites

- Docker 24.0+
- Docker Compose 2.20+

### 1. Clone Repositories

```bash
mkdir yappa && cd yappa
git clone https://github.com/your-org/realtime-ws.git
git clone https://github.com/your-org/yappa-auth.git
git clone https://github.com/your-org/yappa-sdk.git
```

### 2. Create Environment File

```bash
cat > .env << 'EOF'
# Security (CHANGE THESE!)
JWT_SECRET=change-me-to-32-random-characters-minimum
JWT_ISSUER=yappa-rt
JWT_AUDIENCE=realtime

# Deployment Mode
PERSISTENCE_MODE=direct

# Tenant Limits  
MAX_USERS_PER_TENANT=10

# CORS (your frontend origins)
CORS_ORIGINS=http://localhost:3000,http://localhost:5173

# Database (defaults work with docker-compose)
DATABASE_URL=postgres://realtime:realtime@postgres:5432/realtime
REDIS_URL=redis://redis:6379
EOF
```

### 3. Start Services

```bash
docker-compose up -d
```

### 4. Create a User

```bash
curl -X POST http://localhost:3001/api/register \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"demo","user_id":"alice","password":"secret123"}'
```

### 5. Login

```bash
curl -X POST http://localhost:3001/api/login \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"demo","user_id":"alice","password":"secret123"}'
# Response: {"access_token":"eyJ...","token_type":"Bearer","expires_in":300}
```

### 6. Connect with SDK

```javascript
import { RealtimeClient } from '@yappa-rs/yappa-sdk';

const client = new RealtimeClient({
  url: 'ws://localhost:8080/ws',
  token: 'your-access-token',
});

client.on('message', (msg) => console.log(msg));
await client.connect();
client.sendDM('bob', 'Hello!');
```

---

## Message Types

### Direct Messages (DM)

1:1 private messaging between two users.

```javascript
client.sendDM('user_id', 'Hello!');
```

### Group Messages

Small group chats with explicit membership. Users must join before sending.

```javascript
client.createGroup('group-id');
client.joinGroup('group-id');
client.sendGroupMessage('group-id', 'Hello group!');
client.leaveGroup('group-id');
```

### Community

Broadcast channels (same as groups, different semantics for your application).

---

## Authentication Flow

```
┌─────────┐     ┌─────────────┐     ┌──────────┐
│  User   │────▶│ yappa-auth  │────▶│  Redis   │
└─────────┘     └─────────────┘     │ Postgres │
     │                │             └──────────┘
     │   access_token (5min)
     │   refresh_token (7d, HTTP-only cookie)
     │
     ▼
┌─────────┐     ┌─────────────┐
│   SDK   │────▶│  yappa-rt   │
└─────────┘     └─────────────┘
     │                │
     │  WebSocket + JWT
     │                │
     │                ▼
     │          Validated
     │          (stateless)
     │
     ▼
  Connected
```

1. User logs in via `/api/login` with tenant_id, user_id, password
2. Auth service returns short-lived JWT (5 min) + sets HTTP-only refresh cookie
3. SDK connects to WebSocket with JWT in Authorization header
4. Server validates JWT statelessly (no DB lookup)
5. SDK auto-refreshes token before expiry

---

## Tenant Limits

The system enforces a maximum number of concurrent users per tenant:

- Configured via `MAX_USERS_PER_TENANT` (default: 10)
- Uses Redis SET + atomic Lua scripts
- Works across multiple nodes (not just single-instance)
- User can have multiple connections (multi-device)
- Returns HTTP 429 when limit reached

---

## Deployment Modes

### Demo Mode (`PERSISTENCE_MODE=direct`)

- Messages written directly to PostgreSQL
- No Kafka required
- Simpler setup, fewer resources
- Best for: Demos, development, low-traffic deployments

### Production Mode (`PERSISTENCE_MODE=kafka`)

- Messages go through Kafka for durability
- Supports message replay
- Higher throughput
- Best for: Production, horizontal scaling, compliance requirements

---

## API Reference

### Auth Service (yappa-auth)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/api/register` | POST | Create user. Body: `{tenant_id, user_id, password, display_name?}` |
| `/api/login` | POST | Login. Returns access token + sets refresh cookie |
| `/api/refresh` | POST | Refresh access token (uses cookie) |
| `/api/logout` | POST | Revoke refresh token |

### Realtime Server (yappa-rt)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/ws` | GET | WebSocket upgrade. Requires `Authorization: Bearer <token>` |

### WebSocket Protocol

**Client → Server:**

```json
// DM
{"channel_type":"DM","user_id":"recipient","content":"Hello"}

// Group join
{"msg_type":"JOIN","tenant_id":"demo","group_id":"general","user_id":"alice"}

// Group message
{"channel_type":"GROUP","user_id":"group-id","content":"Hello group"}
```

**Server → Client:**

```json
{
  "type": "chat",
  "message_id": "uuid",
  "tenant_id": "demo",
  "channel_type": "DM",
  "channel_id": "recipient",
  "sender_id": "alice",
  "timestamp": 1700000000,
  "conversation_id": "uuid",
  "payload": {"text": "Hello", "meta": {}}
}
```

---

## SDK Reference

### Installation

```bash
npm install @yappa-rs/yappa-sdk
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `url` | string | required | WebSocket URL (`ws://` or `wss://`) |
| `token` | string | required | JWT access token |
| `authMode` | `"header" \| "query"` | `"header"` | How to send token |
| `refreshUrl` | string | — | URL for token refresh |
| `reconnect` | boolean | true | Auto-reconnect on disconnect |
| `maxReconnectAttempts` | number | Infinity | Max reconnect tries |
| `dedup` | boolean | true | Deduplicate messages |
| `logLevel` | `"debug" \| "info" \| "warn" \| "error" \| "silent"` | `"warn"` | Logging level |
| `heartbeatTimeout` | number | 35000 | Disconnect after no activity (ms) |

### Events

| Event | Args | Description |
|-------|------|-------------|
| `connected` | — | Connection established |
| `disconnected` | `reason: string` | Connection lost |
| `reconnecting` | `attempt: number` | Reconnecting attempt |
| `reconnected` | — | Reconnection successful |
| `message` | `ServerMessage` | Any message |
| `dm` | `ServerMessage` | Direct message |
| `group_message` | `ServerMessage` | Group message |
| `group_join` | `ServerMessage` | User joined group |
| `error` | `RealtimeError` | Error occurred |

---

## Security

### What We Handle

- JWT with pinned HS256 algorithm
- Issuer (`iss`) and audience (`aud`) validation
- Short-lived access tokens (5 min default)
- HTTP-only, Secure, SameSite refresh cookies
- Tenant isolation at every layer
- CORS configuration required (no wildcard in production)

### What You Must Handle

- Generate a strong `JWT_SECRET` (32+ random characters)
- Enable HTTPS/WSS in production
- Configure `CORS_ORIGINS` properly
- Secure your PostgreSQL and Redis instances
- Keep dependencies updated

---

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `JWT_SECRET` | **Yes** | — | JWT signing secret (32+ chars) |
| `JWT_ISSUER` | No | `yappa-rt` | JWT issuer |
| `JWT_AUDIENCE` | No | `realtime` | JWT audience |
| `REDIS_URL` | **Yes** | — | Redis connection URL |
| `DATABASE_URL` | **Yes** | — | PostgreSQL connection URL |
| `KAFKA_BROKERS` | Conditional | — | Kafka brokers (required if `PERSISTENCE_MODE=kafka`) |
| `PERSISTENCE_MODE` | No | `kafka` | `direct` or `kafka` |
| `CORS_ORIGINS` | **Yes** | — | Comma-separated allowed origins |
| `MAX_USERS_PER_TENANT` | No | `10` | Max concurrent users per tenant |
| `PORT` | No | `8080` | Server port |

---

## Development

### Build Realtime Server

```bash
cd realtime-ws
cargo build --release
```

### Build Auth Service

```bash
cd yappa-auth
npm install
npm start
```

### Build SDK

```bash
cd yappa-sdk
npm install
npm run build
```

### Run Tests

```bash
# Server
cd realtime-ws && cargo test

# SDK
cd yappa-sdk && npm test
```

---

## Project Structure

```
yappa/
├── realtime-ws/           # Rust WebSocket server
│   ├── src/
│   │   ├── main.rs        # Entry point
│   │   ├── app.rs         # App wiring, server startup
│   │   ├── auth/          # JWT authentication
│   │   ├── connection/    # Connection registry
│   │   ├── db/            # Database operations
│   │   ├── kafka/         # Kafka producer/consumer
│   │   ├── limits/        # Tenant limiter (Redis)
│   │   ├── protocol/      # Message types
│   │   ├── redis/         # Redis manager
│   │   └── server/        # HTTP/WS handlers
│   ├── migrations/        # SQL migrations
│   └── Cargo.toml
│
├── yappa-auth/            # Node.js auth service
│   ├── src/
│   │   └── index.js       # Auth API
│   └── package.json
│
└── yappa-sdk/             # TypeScript SDK
    ├── src/
    │   ├── client.ts      # Main client
    │   ├── transport.ts   # WebSocket transport
    │   └── ...
    └── package.json
```

---

## License

MIT

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

---

## Support

- GitHub Issues: [github.com/your-org/realtime-ws/issues](https://github.com/your-org/realtime-ws/issues)
- Documentation: [docs/](./docs/)
