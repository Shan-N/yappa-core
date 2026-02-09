use std::net::SocketAddr;

use axum::{Router, { routing::get }};
use tokio::{net::TcpListener, signal};
use tracing::info;


use crate::{auth::Auth, server::{ health::health, ws::ws_handler }};
use crate::connection::ConnectionRegistry;

#[derive(Clone)]
pub struct AppState {
    pub auth: Auth,
    pub registry: ConnectionRegistry,
}

async fn shutdown_signal() {
    signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C handler");

    info!("Shutdown signal received");
}

pub async fn run(jwt_secret: String) {
    let app_state = AppState {
        auth: Auth::new(&jwt_secret),
        registry: ConnectionRegistry::new(),
    };
    let router = Router::new()
    .route("/health", get(health))
    .route("/ws", get(ws_handler))
    .with_state(app_state);

    let addr = SocketAddr::from(([0,0,0,0], 8080));

    let cuh = TcpListener::bind(addr).await;
    let listener =  match cuh {
        Ok(l) => l,
        Err(e) => {
            panic!("Failed to bind to address {}: {}", addr, e);
        }
    };
    let _ = axum::serve(listener, router).with_graceful_shutdown(shutdown_signal()).await;
}

