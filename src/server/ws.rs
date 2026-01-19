use axum::{extract::{WebSocketUpgrade, ws::WebSocket}, response::Response};
use tracing::info;



pub async fn ws_handler(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(mut socket: WebSocket) {

    info!("WebSocket Established");
    info!("Socket Info: {:?}", socket);

    while let Some(msg) = socket.recv().await {
        let msg = if let Ok(msg) = msg {
            msg
        } else {
            // client disconnected
            return;
        };

        if socket.send(msg).await.is_err() {
            // client disconnected
            return;
        }
    }
}