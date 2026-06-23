mod app;
mod auth;
mod connection;
mod db;
mod kafka;
mod limits;
mod protocol;
mod redis;
mod server;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();

    let jwt_secret = dotenv::var("JWT_SECRET").expect("JWT_SECRET must be set in .env");
    let jwt_issuer = dotenv::var("JWT_ISSUER").unwrap_or_else(|_| "yappa-rt".to_string());
    let jwt_audience = dotenv::var("JWT_AUDIENCE").unwrap_or_else(|_| "realtime".to_string());
    let redis_url = dotenv::var("REDIS_URL").expect("REDIS_URL must be set in .env");
    let database_url = dotenv::var("DATABASE_URL").expect("DATABASE_URL must be set in .env");
    let kafka_brokers = dotenv::var("KAFKA_BROKERS").unwrap_or_default();
    let persistence_mode = dotenv::var("PERSISTENCE_MODE").unwrap_or_else(|_| "kafka".to_string());
    let cors_origins = dotenv::var("CORS_ORIGINS").unwrap_or_default();
    let max_users_per_tenant: usize = dotenv::var("MAX_USERS_PER_TENANT")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .expect("MAX_USERS_PER_TENANT must be a number");
    let port = dotenv::var("PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse::<u16>()
        .expect("PORT must be a valid u16");

    app::run(app::AppConfig {
        jwt_secret,
        jwt_issuer,
        jwt_audience,
        redis_url,
        kafka_brokers,
        database_url,
        persistence_mode,
        cors_origins,
        max_users_per_tenant,
        port,
    })
    .await;
}
