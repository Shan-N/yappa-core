pub mod producer;
pub mod consumer;

use std::sync::Arc;

use crate::kafka::consumer::KafkaConsumer;
use crate::kafka::producer::KafkaProducer;

/// Central handle for Kafka producer + consumer.
///
/// - Producer is shared across request handlers (cheaply cloneable via Arc).
/// - Consumer owns a background task that batches messages into bulk DB writes.
#[derive(Clone)]
pub struct Kafka {
    pub producer: Arc<KafkaProducer>,
    pub consumer: Arc<KafkaConsumer>,
}

impl Kafka {
    pub fn new(brokers: &str, consumer_group: &str, pool: sqlx::PgPool) -> Self {
        Kafka {
            producer: Arc::new(KafkaProducer::new(brokers)),
            consumer: Arc::new(KafkaConsumer::new(brokers, consumer_group, pool)),
        }
    }

    /// Convenience: fire a message onto a topic, keyed by channel_id.
    pub async fn produce(
        &self,
        topic: &str,
        key: &str,
        payload: &[u8],
    ) -> Result<(), rdkafka::error::KafkaError> {
        self.producer.send(topic, key, payload).await
    }

    /// Spawn the consumer loop as a background tokio task.
    /// Returns a `JoinHandle` so the caller can await graceful shutdown.
    pub fn spawn_consumer(&self, topics: Vec<String>) -> tokio::task::JoinHandle<()> {
        let consumer = self.consumer.clone();
        tokio::spawn(async move {
            let topic_refs: Vec<&str> = topics.iter().map(|s| s.as_str()).collect();
            if let Err(e) = consumer.run(&topic_refs).await {
                tracing::error!("Kafka consumer loop exited with error: {}", e);
            }
        })
    }

    /// Signal both producer flush and consumer shutdown.
    pub fn shutdown(&self) {
        self.consumer.shutdown();
    }
}