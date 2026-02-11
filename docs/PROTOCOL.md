# WebSocket Protocol Reference

## Overview

All communication over the WebSocket connection uses **JSON text frames**. The server parses each incoming message by attempting deserialization in order:

1. **GroupMessage** — join/leave a group
2. **WsMessage** — send a chat message (DM, GROUP, or COMMUNITY)

If neither succeeds, the message is logged and discarded.

---

## Enums

### ChannelType

Serialized as `SCREAMING_SNAKE_CASE`.

| Variant     | JSON value    | Description                        |
|-------------|---------------|------------------------------------|
| `Dm`        | `"DM"`        | Direct message (1:1)               |
| `Group`     | `"GROUP"`     | Small bounded group chat           |
| `Community` | `"COMMUNITY"` | Large community channel            |

### GroupMessageType

Serialized as `SCREAMING_SNAKE_CASE`.

| Variant | JSON value | Description            |
|---------|------------|------------------------|
| `Join`  | `"JOIN"`   | Join a group           |
| `Leave` | `"LEAVE"`  | Leave a group          |

---

## Client → Server Messages

### GroupMessage (join/leave)

Used to manage group membership. Must be sent before a user can receive group messages.

```json
{
  "msg_type": "JOIN",
  "tenant_id": "tenant1",
  "group_id": "group1",
  "user_id": "user1"
}
```

| Field       | Type             | Required | Description                    |
|-------------|------------------|----------|--------------------------------|
| `msg_type`  | GroupMessageType  | Yes      | `"JOIN"` or `"LEAVE"`          |
| `tenant_id` | string           | Yes      | Tenant scope                   |
| `group_id`  | string           | Yes      | Target group identifier        |
| `user_id`   | string           | Yes      | User performing the action     |

**Responses:** None (silent). Check server logs for confirmation.

---

### WsMessage (chat)

Used to send a message over a channel.

```json
{
  "channel_type": "DM",
  "user_id": "user2",
  "content": "hello!"
}
```

| Field          | Type        | Required | Description                                            |
|----------------|-------------|----------|--------------------------------------------------------|
| `channel_type` | ChannelType | Yes      | `"DM"`, `"GROUP"`, or `"COMMUNITY"`                   |
| `user_id`      | string      | Yes      | Recipient user ID (DM) or group ID (GROUP/COMMUNITY)  |
| `content`      | string      | Yes      | Message text content                                   |

---

## Server → Client Messages

### ServerMessage

The canonical message envelope delivered to clients for all channel types.

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

| Field             | Type        | Description                                                  |
|-------------------|-------------|--------------------------------------------------------------|
| `type`            | string      | Message type identifier (currently always `"chat"`)          |
| `message_id`      | UUID (v4)   | Unique identifier for this message                           |
| `tenant_id`       | string      | Tenant the message belongs to                                |
| `channel_type`    | ChannelType | `"DM"`, `"GROUP"`, or `"COMMUNITY"`                         |
| `channel_id`      | string      | Recipient user ID (for DM) or group ID (for GROUP/COMMUNITY) |
| `sender_id`       | string      | User ID of the message sender                                |
| `timestamp`       | u64         | Unix timestamp in seconds                                    |
| `conversation_id` | UUID (v4)   | Conversation identifier                                      |
| `payload`         | object      | Message content                                              |
| `payload.text`    | string      | Message text                                                 |
| `payload.meta`    | object      | Arbitrary JSON metadata                                      |

---

## Message Routing

### DM routing

```
Client A ──► Server
                │
                ├──► Redis PUBLISH  user:{tenant_id}:{recipient_id}
                │
                └──► Kafka produce  topic=messages  key={channel_id}
                
Redis Listener (all nodes)
                │
                ├──► send to recipient (all connections)
                └──► send to sender (all connections, echo)
```

- Redis channel pattern: `user:{tenant_id}:{user_id}`
- Both sender and recipient receive the message

### Group / Community routing

```
Client A ──► Server
                │
                ├──► Redis PUBLISH  group:{group_id}
                │
                └──► Kafka produce  topic=messages  key={channel_id}
                
Redis Listener (all nodes)
                │
                └──► look up group members in registry
                     └──► send to each member (all connections)
```

- Redis channel pattern: `group:{group_id}`
- Only users who have sent a `JOIN` message receive group messages

---

## Error Handling

| Scenario                        | Behavior                                   |
|---------------------------------|--------------------------------------------|
| Missing `Authorization` header  | `401 Unauthorized` (HTTP, before upgrade)  |
| Invalid/expired JWT             | `401 Unauthorized` (HTTP, before upgrade)  |
| Unparseable JSON message        | Logged, silently discarded                 |
| Redis publish failure           | Error logged, message still sent to Kafka  |
| Kafka produce failure           | Error logged                               |
| Client disconnects              | Connection removed from registry           |

---

## Testing with wscat

### Connect as user1

```bash
wscat -c ws://localhost:8080/ws \
  -H "Authorization: Bearer <USER1_TOKEN>"
```

### Connect as user2

```bash
wscat -c ws://localhost:8080/ws \
  -H "Authorization: Bearer <USER2_TOKEN>"
```

### DM from user1 to user2

```json
{"channel_type":"DM","user_id":"user2","content":"hey there"}
```

### Join a group (both users)

```json
{"msg_type":"JOIN","tenant_id":"tenant1","group_id":"mygroup","user_id":"user1"}
```

### Send to group

```json
{"channel_type":"GROUP","user_id":"mygroup","content":"hello group!"}
```
