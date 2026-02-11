use std::{net::SocketAddr, sync::Arc};

use axum::{Router, { routing::get }};
use tokio::{net::TcpListener, signal};
use tracing::info;


use crate::{auth::Auth, kafka::Kafka, redis::RedisManager, server::{ health::health, ws::ws_handler }};
use crate::connection::ConnectionRegistry;

#[derive(Clone)]
pub struct AppState {
    pub auth: Auth,
    pub registry: ConnectionRegistry,
    pub kafka: Kafka,
    pub pubsub: Arc<RedisManager>,
}

async fn shutdown_signal() {
    signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C handler");

    info!("Shutdown signal received");
}

pub async fn run(jwt_secret: String, redis_url: String, kafka_brokers: String, database_url: String) {
    let pool = sqlx::PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to Postgres");

    // Run migrations to ensure the messages table exists
    sqlx::raw_sql(include_str!("../migrations/001_create_messages.sql"))
        .execute(&pool)
        .await
        .expect("Failed to run database migrations");
    info!("Database migrations applied successfully");

    let kafka = Kafka::new(&kafka_brokers, "realtime-ws-nodes", pool);
    let app_state = AppState {
        auth: Auth::new(&jwt_secret),
        registry: ConnectionRegistry::new(),
        kafka: kafka.clone(),
        pubsub: Arc::new(RedisManager::new(&redis_url)),
    };
    let pubsub_clone = app_state.pubsub.clone();
    let registry_clone = app_state.registry.clone();
    tokio::spawn(async move {
        if let Err(e) = pubsub_clone.listener(registry_clone).await {
            eprintln!("Error in Redis listener: {}", e);
        }
    });

    // Spawn Kafka consumer for DB ingestion (batch / bulk copy)
    let _consumer_handle = kafka.spawn_consumer(vec!["messages".to_string()]);
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

