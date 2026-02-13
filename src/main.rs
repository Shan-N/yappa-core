mod app;
mod server;
mod auth;
mod connection;
mod protocol;
mod redis;
mod kafka;
mod db;


#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();
    
    let jwt_secret = dotenv::var("JWT_SECRET").expect("JWT_SECRET must be set in .env");
    let redis_url = dotenv::var("REDIS_URL").expect("REDIS_URL must be set in .env");
    let kafka_brokers = dotenv::var("KAFKA_BROKERS").expect("KAFKA_BROKERS must be set in .env");
    let database_url = dotenv::var("DATABASE_URL").expect("DATABASE_URL must be set in .env");

    app::run(jwt_secret, redis_url, kafka_brokers, database_url).await;
}
