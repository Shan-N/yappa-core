use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ChannelType {
    Dm,
    Group,
    Community,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GroupMessageType {
    Join,
    Leave,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagePayload {
    pub text: String,
    pub meta: serde_json::Value,
}


#[derive(Debug, Serialize, Clone, Deserialize)]
pub struct ServerMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub message_id: Uuid,
    pub tenant_id: String,
    pub channel_type: ChannelType,
    pub channel_id: String,
    pub sender_id: String,
    pub timestamp: u64,
    pub conversation_id: Uuid,
    pub payload: MessagePayload,
}

#[derive(Debug, Serialize, Clone, Deserialize)]
pub struct GroupMessage {
    pub msg_type: GroupMessageType,
    pub tenant_id: String,
    pub group_id: String,
    pub user_id: String,
}