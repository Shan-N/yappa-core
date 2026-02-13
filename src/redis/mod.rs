use redis::{Client, aio::{ConnectionManager}};
use futures::StreamExt;
use axum::extract::ws::Message;

use crate::{connection::ConnectionRegistry, protocol::{ChannelType, ServerMessage}};



pub struct RedisManager {
    client: Client,
    conn: ConnectionManager,
}

impl RedisManager {
    pub async fn new(redis_url: &str) -> anyhow::Result<Self> {
        let client = Client::open(redis_url).expect("Redis URL not set");
        let manager = client.get_connection_manager().await?;
        Ok(Self {
            client,
            conn: manager,
        })
    }

    pub async fn publish(&self, user_id: &str, msg: &ServerMessage) -> anyhow::Result<()> {
        let mut conn = self.conn.clone();
        let channel = format!("user:{}:{}", msg.tenant_id, user_id);
        let payload = serde_json::to_string(msg)?;
        redis::cmd("PUBLISH").arg(&channel).arg(&payload).query_async::<()>(&mut conn).await?;
        Ok(())
    }

    pub async fn publish_grp(&self, group_id: &str, msg: &ServerMessage) -> anyhow::Result<()> {
        let mut conn = self.conn.clone();
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

                if server_msg.msg_type == "group_join" {
                    registry.join_group(&server_msg.tenant_id, &server_msg.channel_id, &server_msg.sender_id);
                    registry.send_msg_to_group(&server_msg.tenant_id, &server_msg.channel_id, ws_msg.clone());
                    continue;
                }

                match server_msg.channel_type {
                    ChannelType::Dm => {
                        // For DMs, channel_id is the recipient user_id
                        registry.send_msg_to_user(
                            &server_msg.tenant_id,
                            &server_msg.channel_id,
                            ws_msg.clone(),
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