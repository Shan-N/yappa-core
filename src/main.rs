
mod app;
mod server;
mod auth;
mod connection;
mod protocol;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    let key = "JWT_SECRET";
    tracing_subscriber::fmt::init();
    let jwt_secret = dotenv::var(key).expect(&format!("{} must be set in .env", key));
    app::run(jwt_secret).await;
}
