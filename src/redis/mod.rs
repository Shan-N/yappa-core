use redis::Client;
use futures::StreamExt;
use axum::extract::ws::Message;

use crate::{connection::ConnectionRegistry, protocol::{ChannelType, ServerMessage}};



pub struct RedisManager {
    client: Client,
}

impl RedisManager {
    pub fn new(redis_url: &str) -> Self {
        Self { client: Client::open(redis_url).expect("Invalid Redis URL") }
    }

    pub async fn publish(&self, user_id: &str, msg: &ServerMessage) -> anyhow::Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let channel = format!("user:{}:{}", msg.tenant_id, user_id);
        let payload = serde_json::to_string(msg)?;
        redis::cmd("PUBLISH").arg(&channel).arg(&payload).query_async::<()>(&mut conn).await?;
        Ok(())
    }

    pub async fn publish_grp(&self, group_id: &str, msg: &ServerMessage) -> anyhow::Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let channel = format!("group:{}", group_id);
        let payload = serde_json::to_string(msg)?;
        redis::cmd("PUBLISH").arg(&channel).arg(&payload).query_async::<()>(&mut conn).await?;
        Ok(())
    }

    pub async fn listener(&self, registry: ConnectionRegistry) -> anyhow::Result<()> {
        let mut pubsub = self.client.get_async_pubsub().await?;
        pubsub.psubscribe("user:*:*").await?;
        pubsub.psubscribe("group:*").await?;
        let mut stream = pubsub.on_message();
        while let Some(msg) = stream.next().await {
            let payload: String = msg.get_payload()?;
            if let Ok(server_msg) = serde_json::from_str::<ServerMessage>(&payload) {
                let json_str = serde_json::to_string(&server_msg).unwrap_or_default();
                let ws_msg = Message::Text(json_str.into());

                match server_msg.channel_type {
                    ChannelType::Dm => {
                        // For DMs, channel_id is the recipient user_id
                        registry.send_msg_to_user(
                            &server_msg.tenant_id,
                            &server_msg.channel_id,
                            ws_msg.clone(),
                        );
                        // Also send to sender so they see their own message
                        registry.send_msg_to_user(
                            &server_msg.tenant_id,
                            &server_msg.sender_id,
                            ws_msg,
                        );
                    }
                    ChannelType::Group | ChannelType::Community => {
                        // For groups/communities, channel_id is the group_id
                        registry.send_msg_to_group(
                            &server_msg.tenant_id,
                            &server_msg.channel_id,
                            ws_msg,
                        );
                    }
                }
            }
        }
        Ok(())
    }
}