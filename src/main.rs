
mod app;
mod server;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    app::run().await;
}
