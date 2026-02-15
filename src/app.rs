use std::{net::SocketAddr, sync::Arc};

use axum::http::Method;
use axum::{Router, routing::get};
use sqlx::postgres::PgPoolOptions;
use tokio::{net::TcpListener, signal};
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use crate::connection::ConnectionRegistry;
use crate::{
    auth::Auth,
    kafka::Kafka,
    redis::RedisManager,
    server::{health::health, ws::ws_handler},
};

#[derive(Clone)]
pub struct AppState {
    pub auth: Auth,
    pub registry: ConnectionRegistry,
    pub kafka: Kafka,
    pub pubsub: Arc<RedisManager>,
}

async fn shutdown_signal() {
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");
    tokio::select! {
        _ = signal::ctrl_c() => {},
        _ = sigterm.recv() => {},
    }
    info!("Shutdown signal received");
}

pub async fn run(
    jwt_secret: String,
    redis_url: String,
    kafka_brokers: String,
    database_url: String,
    port: u16,
) {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to Postgres for Kafka consumer");

    sqlx::raw_sql(include_str!("../migrations/001_create_messages.sql"))
        .execute(&pool)
        .await
        .expect("Failed to run database migrations");
    info!("Database migrations applied successfully");

    // sqlx::raw_sql(include_str!("../migrations/002_create_api_key.sql"))
    //     .execute(&pool)
    //     .await
    //     .expect("Failed to run database migrations");
    // info!("Database migrations applied successfully");

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST])
        .allow_headers(Any);

    let kafka = Kafka::new(&kafka_brokers, "realtime-ws-nodes", pool);
    let redis_manager = RedisManager::new(&redis_url)
        .await
        .expect("Failed to create RedisManager");
    let app_state = AppState {
        auth: Auth::new(&jwt_secret),
        registry: ConnectionRegistry::new(),
        kafka: kafka.clone(),
        pubsub: Arc::new(redis_manager),
    };
    let pubsub_clone = app_state.pubsub.clone();
    let registry_clone = app_state.registry.clone();
    tokio::spawn(async move {
        if let Err(e) = pubsub_clone.listener(registry_clone).await {
            tracing::error!("Error in Redis listener: {}", e);
        }
    });

    // Spawn Kafka consumer for DB ingestion (batch / bulk copy)
    let _consumer_handle = kafka.spawn_consumer(vec!["messages".to_string()]);
    let router = Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws_handler))
        .with_state(app_state)
        .layer(cors);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let listener_result = TcpListener::bind(addr).await;
    let listener = match listener_result {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("Failed to bind to address {}: {}", addr, e);
            return;
        }
    };

    let serve = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await;
    match serve {
        Ok(_) => info!("Server exited successfully"),
        Err(e) => tracing::error!("Server error: {}", e),
    }

    // Flush remaining Kafka consumer buffer and commit offsets
    kafka.shutdown();
    info!("Server shut down cleanly");
}
