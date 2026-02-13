use sqlx::{PgPool, query};
use tracing::info;
use uuid::Uuid;
use chrono::{DateTime, Utc};

use crate::protocol::ServerMessage;


pub struct MessageBatcher {
    pool: PgPool,
    buffer: Vec<ServerMessage>,
    capacity: usize,
}

impl MessageBatcher {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            buffer: Vec::with_capacity(1000),
            capacity: 1000,
        }
    }

    pub async fn push(&mut self, msg: ServerMessage) {
        self.buffer.push(msg);
        if self.buffer.len() >= self.capacity {
            self.flush().await;
        }
    }

    pub async fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        let len = self.buffer.len();
        let mut ids: Vec<Uuid> = Vec::with_capacity(len);
        let mut tenants: Vec<String> = Vec::with_capacity(len);
        let mut channel_type: Vec<String> = Vec::with_capacity(len);
        let mut channel_id: Vec<String> = Vec::with_capacity(len);
        let mut conversations: Vec<Uuid> = Vec::with_capacity(len);
        let mut senders: Vec<String> = Vec::with_capacity(len);
        let mut contents: Vec<String> = Vec::with_capacity(len);
        let mut times: Vec<u64> = Vec::with_capacity(len);

        for m in self.buffer.drain(..) {
            ids.push(m.message_id);
            tenants.push(m.tenant_id);
            channel_type.push(format!("{:?}", m.channel_type));
            channel_id.push(m.channel_id.to_string());
            conversations.push(m.conversation_id);
            senders.push(m.sender_id);
            contents.push(m.payload.text);
            times.push(m.timestamp);
        }

        let raw_sql = r#"
            INSERT INTO messages (message_id, tenant_id, conversation_id, channel_type, channel_id, sender_id, content, created_at)
            SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::uuid[], $4::text[], $5::text[], $6::text[], $7::text[], $8::timestamptz[])
        "#;

        if let Err(e) = query(raw_sql)
            .bind(&ids[..])
            .bind(&tenants[..])
            .bind(&conversations[..])
            .bind(&channel_type[..])
            .bind(&channel_id[..])
            .bind(&senders[..])
            .bind(&contents[..])
            .bind(&times.iter().map(|ts| DateTime::<Utc>::from_timestamp(*ts as i64, 0).unwrap_or_default()).collect::<Vec<_>>()[..])
            .execute(&self.pool)
            .await
        {
            tracing::error!("Batch insert error: {e}");
            info!("Failed to insert batch of {} messages, retrying", len);
            // On error, re-buffer the messages for retry
            for i in 0..len {
                self.buffer.push(ServerMessage {
                    message_id: ids[i],
                    msg_type: "chat".to_string(),
                    tenant_id: tenants[i].clone(),
                    channel_type: match channel_type[i].as_str() {
                        "Group" => crate::protocol::ChannelType::Group,
                        "Dm" => crate::protocol::ChannelType::Dm,
                        _ => crate::protocol::ChannelType::Dm,
                    },
                    channel_id: channel_id[i].clone(),
                    sender_id: senders[i].clone(),
                    conversation_id: conversations[i],
                    timestamp: times[i],
                    payload: crate::protocol::MessagePayload {
                        text: contents[i].clone(),
                        meta: serde_json::json!({}),
                    },
                });
            }

        }

    }
}