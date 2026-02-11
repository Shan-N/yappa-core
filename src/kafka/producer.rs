use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

pub struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {
    pub fn new(brokers: &str) -> Self {
        let producer = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("queue.buffering.max.messages", "100000")
            .set("queue.buffering.max.kbytes", "1048576") // 1 GB librdkafka buffer
            .set("batch.num.messages", "10000")
            .set("linger.ms", "5") // micro-batch latency
            .set("compression.type", "lz4")
            .set("acks", "1") // leader-ack for throughput
            .create::<FutureProducer>()
            .expect("Kafka producer creation failed");

        KafkaProducer { producer }
    }

    /// Send a keyed message. Returns after broker ack.
    pub async fn send(&self, topic: &str, key: &str, payload: &[u8]) -> Result<(), KafkaError> {
        let record = FutureRecord::to(topic)
            .key(key)
            .payload(payload);

        self.producer
            .send(record, Duration::from_secs(5))
            .await
            .map(|_delivery| ())
            .map_err(|(err, _owned_msg)| err)
    }
}