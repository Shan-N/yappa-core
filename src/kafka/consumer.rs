use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{error, info, warn};

use crate::db::MessageBatcher;
use crate::protocol::ServerMessage;


const BATCH_MAX_SIZE: usize = 500; 
const BATCH_MAX_WAIT: Duration = Duration::from_millis(250);

pub struct KafkaConsumer {
    consumer: Arc<StreamConsumer>,
    shutdown: Arc<Notify>,
    pool: sqlx::PgPool,
}

impl KafkaConsumer {
    pub fn new(brokers: &str, group_id: &str, pool: sqlx::PgPool) -> Self {
        let consumer: StreamConsumer = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false") // manual commit after DB flush
            .set("auto.offset.reset", "earliest")
            .set("fetch.min.bytes", "1")
            .set("fetch.wait.max.ms", "100")
            .set("max.poll.interval.ms", "300000")
            .create()
            .expect("Kafka consumer creation failed");

        KafkaConsumer {
            consumer: Arc::new(consumer),
            shutdown: Arc::new(Notify::new()),
            pool,
        }
    }

    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    pub async fn run(&self, topics: &[&str]) -> anyhow::Result<()> {
        self.consumer.subscribe(topics)?;
        info!("Kafka consumer subscribed to {:?}", topics);

        let mut buf: Vec<ServerMessage> = Vec::with_capacity(BATCH_MAX_SIZE);

        loop {
            let deadline = tokio::time::sleep(BATCH_MAX_WAIT);
            tokio::pin!(deadline);

            tokio::select! {
                biased;

                _ = self.shutdown.notified() => {
                    if !buf.is_empty() {
                        Self::flush_batch(&mut buf, &self.pool).await;
                        self.commit()?;
                    }
                    info!("Kafka consumer shutting down");
                    return Ok(());
                }

                _ = &mut deadline => {
                    // interval elapsed — flush whatever we have
                    if !buf.is_empty() {
                        Self::flush_batch(&mut buf, &self.pool).await;
                        self.commit()?;
                    }
                }

                recv = self.consumer.recv() => {
                    match recv {
                        Ok(msg) => {
                            if let Some(payload) = msg.payload() {
                                match serde_json::from_slice::<ServerMessage>(payload) {
                                    Ok(server_msg) => {
                                        buf.push(server_msg);
                                        if buf.len() >= BATCH_MAX_SIZE {
                                            Self::flush_batch(&mut buf, &self.pool).await;
                                            self.commit()?;
                                        }
                                    }
                                    Err(e) => {
                                        warn!("Skipping malformed message: {}", e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("Kafka recv error: {}", e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        }
    }

    async fn flush_batch(batch: &mut Vec<ServerMessage>, pool: &sqlx::PgPool) {
        let count = batch.len();
        info!("Flushing batch of {} messages to DB", count);
        let mut message_batcher = MessageBatcher::new(pool.clone());

        for msg in batch.drain(..) {
            message_batcher.push(msg).await;
        }

        // Flush any remaining messages that didn't hit the capacity threshold
        message_batcher.flush().await;
    }

    fn commit(&self) -> anyhow::Result<()> {
        self.consumer
            .commit_consumer_state(CommitMode::Async)
            .map_err(|e| anyhow::anyhow!("Kafka commit failed: {}", e))
    }
}
