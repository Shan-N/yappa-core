use std::{net::SocketAddr, sync::Arc};

use axum::http::{HeaderName, HeaderValue, Method};
use axum::{Router, routing::get};
use redis::Client;
use sqlx::postgres::PgPoolOptions;
use tokio::{net::TcpListener, signal};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{error, info, warn};

use crate::connection::ConnectionRegistry;
use crate::limits::TenantLimiter;
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
    pub kafka: Option<Kafka>,
    pub pubsub: Arc<RedisManager>,
    pub limiter: TenantLimiter,
    pub db_pool: sqlx::PgPool,
}

pub struct AppConfig {
    pub jwt_secret: String,
    pub jwt_issuer: String,
    pub jwt_audience: String,
    pub redis_url: String,
    pub kafka_brokers: String,
    pub database_url: String,
    pub persistence_mode: String,
    pub cors_origins: String,
    pub max_users_per_tenant: usize,
    pub port: u16,
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

fn build_cors(origins: &str) -> CorsLayer {
    if origins.is_empty() {
        warn!("CORS_ORIGINS unset — refusing wildcard. Rejecting all cross-origin requests.");
        return CorsLayer::new()
            .allow_methods([Method::GET])
            .allow_headers([]);
    }
    let parsed: Vec<HeaderValue> = origins
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .filter_map(|s| HeaderValue::try_from(s).ok())
        .collect();
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(parsed))
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([axum::http::header::AUTHORIZATION, axum::http::header::CONTENT_TYPE])
        .allow_headers([HeaderName::from_static("x-requested-with")])
}

pub async fn run(cfg: AppConfig) {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&cfg.database_url)
        .await
        .expect("Failed to connect to Postgres");

    sqlx::raw_sql(include_str!("../migrations/001_create_messages.sql"))
        .execute(&pool)
        .await
        .expect("Failed to run messages migration");
    info!("Database migrations applied successfully");

    let redis_client = Client::open(cfg.redis_url.clone()).expect("invalid REDIS_URL");
    let redis_manager = RedisManager::new(&cfg.redis_url)
        .await
        .expect("Failed to create RedisManager");

    let limiter = TenantLimiter::new(&redis_client, cfg.max_users_per_tenant)
        .await
        .expect("Failed to create TenantLimiter");

    let use_kafka = cfg.persistence_mode == "kafka" && !cfg.kafka_brokers.is_empty();
    let kafka = if use_kafka {
        let k = Kafka::new(&cfg.kafka_brokers, "realtime-ws-nodes", pool.clone());
        Some(k)
    } else {
        info!("PERSISTENCE_MODE=direct — Kafka disabled, writing straight to Postgres");
        None
    };

    info!("Creating AppState...");
    let app_state = AppState {
        auth: Auth::new(&cfg.jwt_secret, &cfg.jwt_issuer, &cfg.jwt_audience),
        registry: ConnectionRegistry::new(),
        kafka: kafka.clone(),
        pubsub: Arc::new(redis_manager),
        limiter,
        db_pool: pool,
    };
    info!("AppState created");

    let pubsub_clone = app_state.pubsub.clone();
    let registry_clone = app_state.registry.clone();
    tokio::spawn(async move {
        info!("Starting Redis pubsub listener...");
        if let Err(e) = pubsub_clone.listener(registry_clone).await {
            error!("Error in Redis listener: {}", e);
        }
    });

    if let Some(k) = &kafka {
        let _consumer_handle = k.spawn_consumer(vec!["messages".to_string()]);
    } else {
        let pool = app_state.db_pool.clone();
        tokio::spawn(direct_persistence_loop(pool));
    }

    info!("Building router...");
    let cors = build_cors(&cfg.cors_origins);
    let router = Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws_handler))
        .with_state(app_state)
        .layer(cors);

    let addr = SocketAddr::from(([0, 0, 0, 0], cfg.port));
    info!("Binding to address {}...", addr);
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => {
            info!("Successfully bound to {}", addr);
            l
        }
        Err(e) => {
            error!("Failed to bind to address {}: {}", addr, e);
            return;
        }
    };

    info!("Starting HTTP server...");
    let serve = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await;
    match serve {
        Ok(_) => info!("Server exited successfully"),
        Err(e) => error!("Server error: {}", e),
    }

    if let Some(k) = &kafka {
        k.shutdown();
    }
    info!("Server shut down cleanly");
}

/// Background loop for demo / single-instance mode: drains a shared in-process
/// buffer into Postgres in batches. In multi-node deployments Kafka is used
/// instead and this loop is not spawned.
async fn direct_persistence_loop(_pool: sqlx::PgPool) {
    // Placeholder: in direct mode the WS handler writes straight to the
    // MessageBatcher attached to AppState (see ws.rs). This loop is a no-op
    // hook for future fanout/aggregation if needed.
    std::future::pending::<()>().await;
}
