use axum::{extract::{State, WebSocketUpgrade, ws::{Message, WebSocket}}, http::{HeaderMap, StatusCode, header}, response::{ IntoResponse, Response }};
use tracing::info;
use uuid::Uuid;
use futures::{sink::SinkExt ,stream::StreamExt };

use crate::{app::AppState, auth::Identity, protocol::{GroupMessage, GroupMessageType}};
// use crate::auth::Auth;
use crate::protocol::{ ServerMessage, MessagePayload, ChannelType };

#[derive(serde::Deserialize)]
pub struct WsMessage {
    pub channel_type: ChannelType,
    pub user_id: String,
    pub content: String,
}



pub async fn ws_handler(ws: WebSocketUpgrade, headers: HeaderMap, State(app_state): State<AppState>) -> Response {
    let auth_header = headers.get(header::AUTHORIZATION).and_then(|h| h.to_str().ok());

    let token = match auth_header.and_then(|h| h.strip_prefix("Bearer ")) {
        Some(t) => t,
        None => {
            info!("Missing or invalid Authorization header");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };
    let identity = match app_state.auth.authenticate(token) {
        Ok(id) => id,
        Err(e) => { 
            info!("Authentication failed for token: {}", token);
            info!("Error: {:?}", e);
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };
    ws.on_upgrade(move |socket| handle_socket(socket, identity, app_state))
}

async fn handle_socket(socket: WebSocket, identity: Identity, app_state: AppState) {

    let (mut sender, mut receiver) = socket.split();
    let connection_id = Uuid::new_v4();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    {
        app_state.registry.insert(&identity, connection_id, tx);
    }

    let mut send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    let state_clone = app_state.clone();
    let identity_clone = identity.clone();

    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            match msg {
                Message::Text(text) => {
                    if let Ok(group_state) = serde_json::from_str::<GroupMessage>(&text) {
                        match group_state.msg_type {
                            GroupMessageType::Join => {
                                state_clone.registry.join_group(&group_state.tenant_id, &group_state.group_id, &group_state.user_id);
                            }
                            GroupMessageType::Leave => {
                                state_clone.registry.leave_group(&group_state.tenant_id, &group_state.group_id, &group_state.user_id);
                            }
                        }
                    } else if let Ok(payload) = serde_json::from_str::<WsMessage>(&text) {
                        match payload.channel_type {
                            ChannelType::Dm => {
                            let server_msg = ServerMessage {
                            message_id: Uuid::new_v4(),
                            msg_type: "chat".to_string(),
                            tenant_id: identity_clone.tenant_id.clone(),
                            channel_type: payload.channel_type.clone(),
                            channel_id: payload.user_id.clone(),
                            sender_id: identity_clone.user_id.clone(),
                            conversation_id: Uuid::new_v4(),
                            timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
                            payload: MessagePayload {
                                text: payload.content.clone(),
                                meta: serde_json::json!({}),
                            },
                        };
                        info!("Received message to user {}: {}", payload.user_id, payload.content);

                        let _ = state_clone.pubsub.publish(&payload.user_id, &server_msg).await;

                        // Produce to Kafka for DB persistence
                        if let Ok(kafka_payload) = serde_json::to_vec(&server_msg) {
                            if let Err(e) = state_clone.kafka.produce("messages", &server_msg.channel_id, &kafka_payload).await {
                                tracing::error!("Kafka produce failed: {}", e);
                            }
                        }
                            },
                            ChannelType::Group | ChannelType::Community => {
                                let server_msg = ServerMessage {
                                    message_id: Uuid::new_v4(),
                                    msg_type: "chat".to_string(),
                                    tenant_id: identity_clone.tenant_id.clone(),
                                    channel_type: payload.channel_type.clone(),
                                    channel_id: payload.user_id.clone(), // here user_id is actually group_id
                                    sender_id: identity_clone.user_id.clone(),
                                    timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
                                    conversation_id: Uuid::new_v4(),
                                    payload: MessagePayload {
                                        text: payload.content.clone(),
                                        meta: serde_json::json!({}),
                                    },
                                };
                                info!("Received message to group {}: {}", payload.user_id, payload.content);
                                let _ = state_clone.pubsub.publish_grp(&payload.user_id, &server_msg).await;

                                // Produce to Kafka for DB persistence
                                if let Ok(kafka_payload) = serde_json::to_vec(&server_msg) {
                                    if let Err(e) = state_clone.kafka.produce("messages", &server_msg.channel_id, &kafka_payload).await {
                                        tracing::error!("Kafka produce failed: {}", e);
                                    }
                                }
                            }
                        }

                        // let registry = &state_clone.registry;
                        // registry.send_msg_to_user(&identity_clone.tenant_id, &payload.user_id, Message::Text(format!("Received from {}: {}", identity_clone.user_id, payload.content).into()));
                    } else {
                        info!("Received non-parseable message: {}", text);
                    }
                }
                Message::Close(_) => {
                    break;
                }
                _ => {}
            }
        }
    });

    tokio::select! {
        _ = (&mut send_task) => {},
        _ = (&mut recv_task) => {},
    }

    info!("WebSocket Established");
    info!("Client Identity: {:?}", identity);
    app_state.registry.remove(&identity, &connection_id);

}