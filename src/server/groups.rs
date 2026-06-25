use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Json},
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::app::AppState;

#[derive(Debug, Serialize)]
pub struct GroupResponse {
    pub conversation_id: String,
    pub name: String,
    pub created_by: String,
    pub created_at: u64,
    pub member_count: i64,
}

#[derive(Debug, Deserialize)]
pub struct TenantPath {
    pub tenant_id: String,
}

pub async fn get_tenant_groups(
    Path(path): Path<TenantPath>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let rows = sqlx::query_as::<_, GroupRow>(
        r#"
        SELECT g.conversation_id::text, g.name, g.created_by,
               EXTRACT(EPOCH FROM g.created_at)::bigint as created_at,
               COUNT(gm.user_id) as member_count
        FROM groups g
        LEFT JOIN group_members gm ON g.conversation_id = gm.conversation_id
        WHERE g.tenant_id = $1
        GROUP BY g.conversation_id, g.name, g.created_by, g.created_at
        ORDER BY g.created_at DESC
        "#
    )
    .bind(&path.tenant_id)
    .fetch_all(&state.db_pool)
    .await;

    match rows {
        Ok(groups) => {
            let response: Vec<GroupResponse> = groups
                .into_iter()
                .map(|g| GroupResponse {
                    conversation_id: g.conversation_id,
                    name: g.name,
                    created_by: g.created_by,
                    created_at: g.created_at as u64,
                    member_count: g.member_count,
                })
                .collect();
            info!("Fetched {} groups for tenant {}", response.len(), path.tenant_id);
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            error!("Failed to fetch groups: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(Vec::<GroupResponse>::new())).into_response()
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct GroupRow {
    conversation_id: String,
    name: String,
    created_by: String,
    created_at: i64,
    member_count: i64,
}
