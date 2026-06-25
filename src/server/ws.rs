use axum::{
    extract::{State, WebSocketUpgrade, ws::{Message, WebSocket}},
    http::{HeaderMap, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use futures::{sink::SinkExt, stream::StreamExt};
use std::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::db::MessageBatcher;
use crate::protocol::{ChannelType, MessagePayload, ServerMessage};
use crate::{
    app::AppState,
    auth::Identity,
    connection::{CHANNEL_CAPACITY, ConnectionId, ConnectionRegistry},
    protocol::{GroupMessage, GroupMessageType, generate_dm_conversation_id},
};

#[derive(serde::Deserialize)]
pub struct WsMessage {
    pub channel_type: ChannelType,
    pub user_id: String,
    pub content: String,
}

const MAX_PAYLOAD_SIZE: usize = 64 * 1024;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);
const SEND_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(app_state): State<AppState>,
    uri: Uri,
) -> Response {
    let auth_header = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok());
    let query = uri.query().unwrap_or("");
    let query_params = query
        .split('&')
        .filter_map(|kv| {
            let mut parts = kv.splitn(2, '=');
            Some((parts.next()?, parts.next()?))
        })
        .collect::<std::collections::HashMap<_, _>>();

    let token = auth_header
        .and_then(|h| h.strip_prefix("Bearer "))
        .or_else(|| query_params.get("token").copied());

    let token = match token {
        Some(t) => t,
        None => {
            warn!("Missing or invalid Authorization header");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };
    let identity = match app_state.auth.authenticate(token) {
        Ok(id) => id,
        Err(e) => {
            error!("JWT auth error: {:?}", e);
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    if !app_state.limiter.acquire(&identity).await {
        warn!("Tenant {} at user capacity, rejecting {}", identity.tenant_id, identity.user_id);
        return StatusCode::from_u16(429).unwrap().into_response();
    }

    ws.on_upgrade(move |socket| handle_socket(socket, identity, app_state))
}

async fn handle_socket(socket: WebSocket, identity: Identity, app_state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let connection_id = Uuid::new_v4();

    let (tx, mut rx) = tokio::sync::mpsc::channel(CHANNEL_CAPACITY);
    app_state.registry.insert(&identity, connection_id, tx);

    // Auto-join user to "general" group on connect
    app_state.registry.join_group(&identity.tenant_id, "general", &identity.user_id);
    info!("User {} auto-joined #general", identity.user_id);

    let identity_for_guard = identity.clone();
    let limiter = app_state.limiter.clone();
    let registry = app_state.registry.clone();

    struct ConnectionGuard {
        registry: ConnectionRegistry,
        limiter: crate::limits::TenantLimiter,
        identity: Identity,
        connection_id: ConnectionId,
    }

    impl Drop for ConnectionGuard {
        fn drop(&mut self) {
            self.registry.remove(&self.identity, &self.connection_id);
            let limiter = self.limiter.clone();
            let identity = self.identity.clone();
            tokio::spawn(async move {
                limiter.release(&identity).await;
            });
            info!(
                "Connection guard cleanup: tenant={}, user={}, conn={}",
                self.identity.tenant_id, self.identity.user_id, self.connection_id
            );
        }
    }

    let _guard = ConnectionGuard {
        registry,
        limiter,
        identity: identity_for_guard,
        connection_id,
    };

    let mut send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match tokio::time::timeout(SEND_TIMEOUT, sender.send(msg)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    warn!("WebSocket send error: {}", e);
                    break;
                }
                Err(_) => {
                    warn!("WebSocket send timed out, dropping connection");
                    break;
                }
            }
        }
        let _ = sender.close().await;
    });

    let state_clone = app_state.clone();
    let identity_clone = identity.clone();

    let mut recv_task = tokio::spawn(async move {
        let mut heartbeat_interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        let mut last_activity = tokio::time::Instant::now();

        loop {
            tokio::select! {
                maybe_msg = receiver.next() => {
                    match maybe_msg {
                        Some(Ok(msg)) => {
                            last_activity = tokio::time::Instant::now();
                            match msg {
                                Message::Text(text) => {
                                    handle_text_message(&text, &identity_clone, &state_clone).await;
                                }
                                Message::Pong(_) => {}
                                Message::Ping(_) => {}
                                Message::Close(_) => {
                                    info!("Client sent close frame: user={}", identity_clone.user_id);
                                    break;
                                }
                                _ => {}
                            }
                        }
                        Some(Err(e)) => {
                            warn!("WebSocket recv error for user={}: {}", identity_clone.user_id, e);
                            break;
                        }
                        None => break,
                    }
                }
                _ = heartbeat_interval.tick() => {
                    if last_activity.elapsed() > HEARTBEAT_TIMEOUT {
                        warn!("Connection timed out: user={}", identity_clone.user_id);
                        break;
                    }
                    state_clone.registry.send_msg_to_user(
                        &identity_clone.tenant_id,
                        &identity_clone.user_id,
                        Message::Ping(vec![].into()),
                    );
                }
            }
        }
    });

    tokio::select! {
        _ = (&mut send_task) => { recv_task.abort(); },
        _ = (&mut recv_task) => { send_task.abort(); },
    }
}

async fn handle_text_message(text: &str, identity: &Identity, state: &AppState) {
    if let Ok(group_state) = serde_json::from_str::<GroupMessage>(text) {
        match group_state.msg_type {
            GroupMessageType::Join => {
                let conversation_id = group_state
                    .group_id
                    .parse::<Uuid>()
                    .unwrap_or_else(|_| Uuid::new_v4());

                let result = sqlx::query(
                    r#"
                    INSERT INTO group_members (conversation_id, tenant_id, user_id)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (conversation_id, user_id) DO NOTHING
                    "#
                )
                .bind(conversation_id)
                .bind(&identity.tenant_id)
                .bind(&identity.user_id)
                .execute(&state.db_pool)
                .await;

                if let Err(e) = result {
                    error!("Failed to add group member to database: {}", e);
                }

                state.registry.join_group(
                    &identity.tenant_id,
                    &group_state.group_id,
                    &identity.user_id,
                );
                let server_msg = ServerMessage {
                    msg_type: "group_join".to_string(),
                    message_id: Uuid::new_v4(),
                    tenant_id: identity.tenant_id.clone(),
                    channel_id: group_state.group_id.clone(),
                    channel_type: ChannelType::Group,
                    sender_id: identity.user_id.clone(),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    conversation_id,
                    payload: MessagePayload {
                        text: format!("{} has joined the group", identity.user_id),
                        meta: serde_json::json!({}),
                    },
                };
                if let Err(e) = state.pubsub.publish_grp(&group_state.group_id, &server_msg).await {
                    error!("Redis publish failed: {}", e);
                }
            }
            GroupMessageType::Leave => {
                let conversation_id = group_state
                    .group_id
                    .parse::<Uuid>()
                    .unwrap_or_else(|_| Uuid::new_v4());

                let result = sqlx::query(
                    "DELETE FROM group_members WHERE conversation_id = $1 AND user_id = $2"
                )
                .bind(conversation_id)
                .bind(&identity.user_id)
                .execute(&state.db_pool)
                .await;

                if let Err(e) = result {
                    error!("Failed to remove group member from database: {}", e);
                }

                state.registry.leave_group(
                    &identity.tenant_id,
                    &group_state.group_id,
                    &identity.user_id,
                );
                let server_msg = ServerMessage {
                    msg_type: "group_leave".to_string(),
                    message_id: Uuid::new_v4(),
                    tenant_id: identity.tenant_id.clone(),
                    channel_id: group_state.group_id.clone(),
                    channel_type: ChannelType::Group,
                    sender_id: identity.user_id.clone(),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    conversation_id,
                    payload: MessagePayload {
                        text: format!("{} has left the group", identity.user_id),
                        meta: serde_json::json!({}),
                    },
                };
                if let Err(e) = state.pubsub.publish_grp(&group_state.group_id, &server_msg).await {
                    error!("Redis publish failed: {}", e);
                }
            }
            GroupMessageType::Create => {
                let conversation_id = group_state
                    .group_id
                    .parse::<Uuid>()
                    .unwrap_or_else(|_| Uuid::new_v4());
                
                let result = sqlx::query(
                    r#"
                    INSERT INTO groups (conversation_id, tenant_id, name, created_by)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (tenant_id, name) DO NOTHING
                    "#
                )
                .bind(conversation_id)
                .bind(&identity.tenant_id)
                .bind(&group_state.group_id)
                .bind(&identity.user_id)
                .execute(&state.db_pool)
                .await;

                if let Err(e) = result {
                    error!("Failed to insert group to database: {}", e);
                }

                let result = sqlx::query(
                    r#"
                    INSERT INTO group_members (conversation_id, tenant_id, user_id)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (conversation_id, user_id) DO NOTHING
                    "#
                )
                .bind(conversation_id)
                .bind(&identity.tenant_id)
                .bind(&identity.user_id)
                .execute(&state.db_pool)
                .await;

                if let Err(e) = result {
                    error!("Failed to add creator to group_members: {}", e);
                }

                state.registry.create_group(&identity.tenant_id, &group_state.group_id);
                state.registry.join_group(&identity.tenant_id, &group_state.group_id, &identity.user_id);
                let server_msg = ServerMessage {
                    msg_type: "group_created".to_string(),
                    message_id: Uuid::new_v4(),
                    tenant_id: identity.tenant_id.clone(),
                    channel_id: group_state.group_id.clone(),
                    channel_type: ChannelType::Group,
                    sender_id: identity.user_id.clone(),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    conversation_id,
                    payload: MessagePayload {
                        text: format!("Group {} created by {}", group_state.group_id, identity.user_id),
                        meta: serde_json::json!({}),
                    },
                };
                if let Err(e) = state.pubsub.publish_grp(&group_state.group_id, &server_msg).await {
                    error!("Redis publish failed: {}", e);
                }
            }
            GroupMessageType::Delete => {
                state.registry.delete_group(&identity.tenant_id, &group_state.group_id);
            }
        }
    } else if let Ok(payload) = serde_json::from_str::<WsMessage>(text) {
        if payload.content.len() > MAX_PAYLOAD_SIZE {
            warn!("Payload too large from user {}: {} bytes", identity.user_id, payload.content.len());
            state.registry.send_msg_to_user(
                &identity.tenant_id,
                &identity.user_id,
                Message::Text("Payload too large".into()),
            );
            return;
        }

        match payload.channel_type {
            ChannelType::Dm => {
                let server_msg = ServerMessage {
                    message_id: Uuid::new_v4(),
                    msg_type: "chat".to_string(),
                    tenant_id: identity.tenant_id.clone(),
                    channel_type: payload.channel_type.clone(),
                    channel_id: payload.user_id.clone(),
                    sender_id: identity.user_id.clone(),
                    conversation_id: generate_dm_conversation_id(&identity.user_id, &payload.user_id),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    payload: MessagePayload {
                        text: payload.content.clone(),
                        meta: serde_json::json!({}),
                    },
                };
                info!("Received message to user {}: {}", payload.user_id, payload.content);

                // Publish to recipient
                if let Err(e) = state.pubsub.publish(&payload.user_id, &server_msg).await {
                    error!("Redis publish failed: {}", e);
                }
                // Also echo back to sender so they see their own message
                if let Err(e) = state.pubsub.publish(&identity.user_id, &server_msg).await {
                    error!("Redis publish to sender failed: {}", e);
                }

                persist_message(state, &server_msg).await;
            }
            ChannelType::Group | ChannelType::Community => {
                if !state.registry.is_user_in_group(&identity.tenant_id, &payload.user_id, &identity.user_id) {
                    warn!("User {} attempted to send to group {} without joining", identity.user_id, payload.user_id);
                    state.registry.send_msg_to_user(
                        &identity.tenant_id,
                        &identity.user_id,
                        Message::Text("You must join the group before sending messages".into()),
                    );
                    return;
                }
                let server_msg = ServerMessage {
                    message_id: Uuid::new_v4(),
                    msg_type: "chat".to_string(),
                    tenant_id: identity.tenant_id.clone(),
                    channel_type: payload.channel_type.clone(),
                    channel_id: payload.user_id.clone(),
                    sender_id: identity.user_id.clone(),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    conversation_id: payload.user_id.parse::<Uuid>().unwrap_or_else(|_| Uuid::new_v4()),
                    payload: MessagePayload {
                        text: payload.content.clone(),
                        meta: serde_json::json!({}),
                    },
                };
                info!("Received message to group {}: {}", payload.user_id, payload.content);

                if let Err(e) = state.pubsub.publish_grp(&payload.user_id, &server_msg).await {
                    error!("Redis publish failed: {}", e);
                }

                persist_message(state, &server_msg).await;
            }
        }
    } else {
        info!("Received non-parseable message: {}", text);
    }
}

async fn persist_message(state: &AppState, msg: &ServerMessage) {
    if let Some(ref kafka) = state.kafka {
        if let Ok(payload) = serde_json::to_vec(msg) {
            if let Err(e) = kafka.produce("messages", &msg.channel_id, &payload).await {
                error!("Kafka produce failed: {}", e);
            }
        }
    } else {
        let mut batcher = MessageBatcher::new(state.db_pool.clone());
        batcher.push(msg.clone()).await;
        batcher.flush().await;
    }
}
