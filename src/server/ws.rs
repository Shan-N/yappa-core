use axum::{extract::{State, WebSocketUpgrade, ws::{Message, WebSocket}}, http::{HeaderMap, StatusCode, header}, response::{ IntoResponse, Response }};
use tracing::info;
use uuid::Uuid;
use futures::{sink::SinkExt ,stream::StreamExt };

use crate::{app::AppState, auth::Identity};
// use crate::auth::Auth;

#[derive(serde::Deserialize)]
pub struct WsMessage {
    pub user_id: String,
    pub content: String,
}


pub async fn ws_handler(ws: WebSocketUpgrade, headers: HeaderMap, State(app_state): State<AppState>) -> Response {
    let auth_header = headers.get(header::AUTHORIZATION).and_then(|h| h.to_str().ok());

    let token = match auth_header.and_then(|h| h.strip_prefix("Bearer ")) {
        Some(t) => t,
        None => {
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };
    let identity = match app_state.auth.authenticate(token) {
        Ok(id) => id,
        Err(_) => { 
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
                    if let Ok(payload) = serde_json::from_str::<WsMessage>(&text) {
                        info!("Received message from user {}: {}", payload.user_id, payload.content);
                        let registry = &state_clone.registry;
                        registry.send_msg_to_user(&identity_clone.tenant_id, &payload.user_id, Message::Text(format!("Received from {}: {}", identity_clone.user_id, payload.content).into()));
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