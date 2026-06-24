use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

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
    let limit = query.limit.unwrap_or(50).min(100) as i64;

    let rows = match query.before {
        Some(before_cursor) => {
            sqlx::query_as!(
                MessageRow,
                r#"
                SELECT message_id, channel_type, channel_id, sender_id, content as text, 
                       EXTRACT(EPOCH FROM created_at)::bigint as timestamp, conversation_id::text as conversation_id
                FROM messages
                WHERE tenant_id = $1 
                  AND channel_type = $2
                  AND ((channel_type = 'Dm' AND channel_id = $3) 
                       OR (channel_type != 'Dm' AND channel_id = $4))
                  AND created_at < $5
                ORDER BY created_at DESC
                LIMIT $6
                "#,
                path.tenant_id,
                path.channel_type,
                path.channel_id,
                path.channel_id,
                before_cursor,
                limit
            )
            .fetch_all(&state.db_pool)
            .await
        }
        None => {
            sqlx::query_as!(
                MessageRow,
                r#"
                SELECT message_id, channel_type, channel_id, sender_id, content as text, 
                       EXTRACT(EPOCH FROM created_at)::bigint as timestamp, conversation_id::text as conversation_id
                FROM messages
                WHERE tenant_id = $1 
                  AND channel_type = $2
                  AND channel_id = $3
                ORDER BY created_at DESC
                LIMIT $4
                "#,
                path.tenant_id,
                path.channel_type,
                path.channel_id,
                limit
            )
            .fetch_all(&state.db_pool)
            .await
        }
    };

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
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to fetch history: {}", e);
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
