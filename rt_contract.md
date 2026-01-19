```md
# Realtime Core — Protocol Contract

Version: 1.0  
Scope: Rust WebSocket servers + Redis  
Guarantees: Scoped delivery, multi-node safe, reconnect tolerant

---

## 1. Connection Identity

Every WebSocket connection is uniquely identified by:

```

(tenant_id, user_id)

````

- `tenant_id` is mandatory
- `user_id` is mandatory
- Identity is immutable for the lifetime of the socket
- One user may have multiple concurrent connections (multi-device)

---

## 2. Authentication Contract

- Authentication happens **during WebSocket upgrade**
- JWT is provided via:
  - `Authorization: Bearer <token>` header
- Token must contain:
  - `tenant_id`
  - `user_id`
  - `exp`
- Invalid token → socket closed immediately
- Token tenant mismatch → rejected

---

## 3. Message Envelope (JSON)

All messages exchanged over WebSocket and Redis use JSON.

### 3.1 Client → Server (Publish)

```json
{
  "type": "message",
  "channel_type": "DM | GROUP | COMMUNITY",
  "channel_id": "string",
  "payload": {
    "text": "string",
    "meta": {}
  }
}
````

### Validation Rules

* `channel_type` must be valid
* `channel_id` must belong to tenant
* Payload size limits enforced
* Rate limit enforced before Redis write

---

### 3.2 Server → Client (Deliver)

```json
{
  "type": "message",
  "message_id": "uuid",
  "tenant_id": "string",
  "channel_type": "DM | GROUP | COMMUNITY",
  "channel_id": "string",
  "sender_id": "string",
  "timestamp": "unix_ms",
  "payload": {
    "text": "string",
    "meta": {}
  }
}
```

---

## 4. Channel Types

### 4.1 DM

* Exactly 2 users
* Delivered to all active sockets of both users

### 4.2 GROUP

* Small, bounded member list
* Fan-out via explicit member lookup

### 4.3 COMMUNITY

* Large membership
* Fan-out batched per WS node
* Presence best-effort only

---

## 5. Redis Keyspace Contract

### 5.1 Message Streams

```
stream:{tenant_id}:{channel_type}:{channel_id}
```

Example:

```
stream:t1:GROUP:g42
```

Fields stored in stream entry:

* `message_id`
* `tenant_id`
* `channel_type`
* `channel_id`
* `sender_id`
* `timestamp`
* `payload` (JSON string)

---

### 5.2 Consumer Groups

```
group:ws-nodes
consumer:{node_id}
```

* One consumer per WS node
* Messages ACKed after local fan-out

---

### 5.3 Presence Sets

```
presence:{tenant_id}:{channel_type}:{channel_id}
```

* `SADD` on connect
* `SREM` on disconnect
* TTL used as safety net

---

### 5.4 Rate Limiting Keys

```
rate:{tenant_id}:{user_id}
```

* Enforced via Redis Lua
* Sliding window strategy

---

## 6. Delivery Guarantees

* At-least-once delivery from Redis
* No cross-tenant leakage
* No delivery to offline users
* Duplicate delivery tolerated at client (idempotent `message_id`)

---

## 7. Failure Semantics

| Failure              | Behavior                |
| -------------------- | ----------------------- |
| Redis down           | Messages pause          |
| WS node crash        | Other nodes continue    |
| Client reconnect     | New socket bound        |
| Duplicate Redis read | Filter via `message_id` |
| Network flap         | Heartbeat cleans up     |

---

## 8. Explicit Non-Goals

* No message ordering guarantees across channels
* No offline storage beyond Redis streams
* No global presence accuracy

---

END OF CONTRACT
