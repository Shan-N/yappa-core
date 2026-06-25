use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::app::AppState;

#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    pub before: Option<String>,
    pub limit: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct MessageResponse {
    pub message_id: String,
    pub channel_type: String,
    pub channel_id: String,
    pub sender_id: String,
    pub text: String,
    pub timestamp: u64,
    pub conversation_id: String,
}

#[derive(Debug, Deserialize)]
pub struct ChannelPath {
    pub tenant_id: String,
    pub channel_type: String,
    pub channel_id: String,
}

pub async fn get_channel_history(
    Path(path): Path<ChannelPath>,
    Query(query): Query<HistoryQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(50).min(100);
    let before_clause = query.before.as_ref().map_or(String::new(), |b| {
        format!("AND created_at < to_timestamp({})", b)
    });

    let sql = format!(
        r#"
        SELECT message_id::text, channel_type, channel_id, sender_id, content as text, 
               EXTRACT(EPOCH FROM created_at)::bigint as timestamp, conversation_id::text as conversation_id
        FROM messages
        WHERE tenant_id = $1 
          AND channel_type = $2
          AND channel_id = $3
          {}
        ORDER BY created_at DESC
        LIMIT $4
        "#,
        before_clause
    );

    let rows = sqlx::query_as::<_, MessageRow>(&sql)
        .bind(&path.tenant_id)
        .bind(&path.channel_type)
        .bind(&path.channel_id)
        .bind(limit as i64)
        .fetch_all(&state.db_pool)
        .await;

    match rows {
        Ok(messages) => {
            let response: Vec<MessageResponse> = messages
                .into_iter()
                .map(|m| MessageResponse {
                    message_id: m.message_id,
                    channel_type: m.channel_type,
                    channel_id: m.channel_id,
                    sender_id: m.sender_id,
                    text: m.text,
                    timestamp: m.timestamp as u64,
                    conversation_id: m.conversation_id,
                })
                .collect();
            info!("Fetched {} messages for channel {}/{}/{}", response.len(), path.tenant_id, path.channel_type, path.channel_id);
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            error!("Failed to fetch history: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(Vec::<MessageResponse>::new())).into_response()
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct MessageRow {
    message_id: String,
    channel_type: String,
    channel_id: String,
    sender_id: String,
    text: String,
    timestamp: i64,
    conversation_id: String,
}
