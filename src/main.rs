use tracing::info;


mod app;
mod server;
mod auth;
mod connection;
mod protocol;
mod redis;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();
    
    let jwt_secret = dotenv::var("JWT_SECRET").expect("JWT_SECRET must be set in .env");
    info!("jwt_secret {} loaded from .env", jwt_secret);
    let redis_url = dotenv::var("REDIS_URL").expect("REDIS_URL must be set in .env");
    
    app::run(jwt_secret, redis_url).await;
}
