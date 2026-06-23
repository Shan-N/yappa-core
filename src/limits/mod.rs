use redis::{Client, aio::ConnectionManager};
use tracing::warn;

use crate::auth::Identity;

/// Cross-node tenant connection limiter backed by Redis.
///
/// Uses two keys per (tenant, user), colocated under a `{tenant_id}` hash tag
/// so the Lua scripts are safe under Redis Cluster:
///   - `online:{tenant_id}`            SET of currently-online user_ids
///   - `conncount:{tenant_id}:{user}`  per-user connection reference count
///
/// `acquire` is an atomic check-and-add: a user already online always succeeds
/// (multi-device), otherwise the tenant's distinct-user cap is enforced.
#[derive(Clone)]
pub struct TenantLimiter {
    conn: ConnectionManager,
    max_users: usize,
}

const ACQUIRE_SCRIPT: &str = r#"
local online = KEYS[1]
local refc   = KEYS[2]
local user  = ARGV[1]
local maxn  = tonumber(ARGV[2])
local cnt = redis.call('INCR', refc)
if cnt == 1 then
  if redis.call('SCARD', online) >= maxn then
    redis.call('DECR', refc)
    return 0
  end
  redis.call('SADD', online, user)
end
return 1
"#;

const RELEASE_SCRIPT: &str = r#"
local online = KEYS[1]
local refc   = KEYS[2]
local user  = ARGV[1]
local cnt = redis.call('DECR', refc)
if cnt <= 0 then
  redis.call('SREM', online, user)
  redis.call('DEL', refc)
end
return 1
"#;

impl TenantLimiter {
    pub async fn new(client: &Client, max_users: usize) -> anyhow::Result<Self> {
        let conn = client.get_connection_manager().await?;
        Ok(Self { conn, max_users })
    }

    fn keys(identity: &Identity) -> (String, String) {
        let tag = &identity.tenant_id;
        let online = format!("online:{{{tag}}}");
        let refc = format!("conncount:{{{tag}}}:{}", identity.user_id);
        (online, refc)
    }

    /// Returns `true` if the connection is allowed (user already online or under cap).
    pub async fn acquire(&self, identity: &Identity) -> bool {
        let (online, refc) = Self::keys(identity);
        let mut conn = self.conn.clone();
        let res: redis::RedisResult<i64> = redis::cmd("EVAL")
            .arg(ACQUIRE_SCRIPT)
            .arg(2)
            .arg(&online)
            .arg(&refc)
            .arg(&identity.user_id)
            .arg(self.max_users)
            .query_async(&mut conn)
            .await;
        match res {
            Ok(1) => true,
            Ok(_) => false,
            Err(e) => {
                warn!("TenantLimiter acquire failed (allowing): {e}");
                true
            }
        }
    }

    pub async fn release(&self, identity: &Identity) {
        let (online, refc) = Self::keys(identity);
        let mut conn = self.conn.clone();
        let res: redis::RedisResult<i64> = redis::cmd("EVAL")
            .arg(RELEASE_SCRIPT)
            .arg(2)
            .arg(&online)
            .arg(&refc)
            .arg(&identity.user_id)
            .query_async(&mut conn)
            .await;
        if let Err(e) = res {
            warn!("TenantLimiter release failed: {e}");
        }
    }
}
