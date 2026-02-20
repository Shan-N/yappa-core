use axum::extract::ws::Message;
use dashmap::{DashMap, DashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::auth::Identity;

// const MAX_TENANTS_USER: usize = 10000;
pub const CHANNEL_CAPACITY: usize = 256;

pub type ConnectionId = Uuid;

pub type SocketSender = mpsc::Sender<Message>;
// pub type Sender_Pressure = mpsc

#[derive(Debug, Clone)]
pub struct ConnectionRegistry {
    // tenant_id -> user_id -> connection_id -> sender
    inner: Arc<DashMap<String, DashMap<String, DashMap<ConnectionId, SocketSender>>>>,
    // tenant_id -> group_id -> set of user_ids
    groups: Arc<DashMap<String, DashMap<String, DashSet<String>>>>,
}

impl ConnectionRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            groups: Arc::new(DashMap::new()),
        }
    }

    pub fn insert(&self, identity: &Identity, connection_id: ConnectionId, sender: SocketSender) {
        self.inner
            .entry(identity.tenant_id.clone())
            .or_insert_with(DashMap::new)
            .entry(identity.user_id.clone())
            .or_insert_with(DashMap::new)
            .insert(connection_id, sender);
        tracing::info!(
            "Registered connection: tenant_id={}, user_id={}",
            identity.tenant_id,
            identity.user_id
        );
    }

    pub fn join_group(&self, tenant_id: &str, group_id: &str, user_id: &str) {
        self.groups
            .entry(tenant_id.to_string())
            .or_insert_with(DashMap::new)
            .entry(group_id.to_string())
            .or_default()
            .insert(user_id.to_string());
        tracing::info!(
            "User {} joined group {} in tenant {}",
            user_id,
            group_id,
            tenant_id
        );
    }

    pub fn leave_group(&self, tenant_id: &str, group_id: &str, user_id: &str) {
        if let Some(tenant_groups) = self.groups.get(tenant_id) {
            if let Some(members) = tenant_groups.get(group_id) {
                members.remove(user_id);
                tracing::info!(
                    "User {} left group {} in tenant {}",
                    user_id,
                    group_id,
                    tenant_id
                );
            }
        }
    }
    pub fn create_group(&self, tenant_id: &str, group_id: &str) {
        self.groups
            .entry(tenant_id.to_string())
            .or_insert_with(DashMap::new)
            .entry(group_id.to_string())
            .or_default();
        tracing::info!("Group {} created in tenant {}", group_id, tenant_id);
    }

    pub fn delete_group(&self, tenant_id: &str, group_id: &str) {
        if let Some(tenant_groups) = self.groups.get(tenant_id) {
            tenant_groups.remove(&group_id.to_string());
            tracing::info!("Group {} deleted in tenant {}", group_id, tenant_id);
        }
    }
    pub fn remove(&self, identity: &Identity, connection_id: &ConnectionId) {
        let should_remove_user;
        let should_remove_tenant;

        if let Some(users) = self.inner.get(&identity.tenant_id) {
            if let Some(connections) = users.get(&identity.user_id) {
                connections.remove(connection_id);
                should_remove_user = connections.is_empty();
            } else {
                return;
            }
            // Drop `connections` guard before mutating `users`
            if should_remove_user {
                users.remove(&identity.user_id);
            }
            should_remove_tenant = users.is_empty();
        } else {
            return;
        }
        // Drop `users` guard before mutating `self.inner`
        if should_remove_tenant {
            self.inner.remove(&identity.tenant_id);
        }
        tracing::info!(
            "Removed connection: tenant_id={}, user_id={}",
            identity.tenant_id,
            identity.user_id
        );
    }

    pub fn send_msg_to_user(&self, tenant_id: &str, user_id: &str, msg: Message) {
        if let Some(users) = self.inner.get(tenant_id) {
            let should_remove_user;
            if let Some(connections) = users.get(user_id) {
                let mut stale: Vec<ConnectionId> = Vec::new();
                for entry in connections.iter() {
                    let (conn_id, sender) = entry.pair();
                    match sender.try_send(msg.clone()) {
                        Ok(_) => {}
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            tracing::warn!(
                                "Channel full for tenant_id={}, user_id={}, connection_id={} — marking stale",
                                tenant_id,
                                user_id,
                                conn_id
                            );
                            stale.push(*conn_id);
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            tracing::warn!(
                                "Channel closed for tenant_id={}, user_id={}, connection_id={} — removing",
                                tenant_id,
                                user_id,
                                conn_id
                            );
                            stale.push(*conn_id);
                        }
                    }
                }
                // Remove stale connections outside of the iterator to avoid deadlock
                for conn_id in stale {
                    connections.remove(&conn_id);
                    tracing::info!(
                        "Evicted stale connection: tenant_id={}, user_id={}, conn_id={}",
                        tenant_id,
                        user_id,
                        conn_id
                    );
                }
                should_remove_user = connections.is_empty();
            } else {
                return;
            }
            // Drop `connections` guard before mutating `users`
            if should_remove_user {
                users.remove(user_id);
            }
        }
    }

    pub fn send_msg_to_group(&self, tenant_id: &str, group_id: &str, msg: Message) {
        let user_ids: Vec<String> = if let Some(tenant_groups) = self.groups.get(tenant_id) {
            if let Some(members) = tenant_groups.get(group_id) {
                members.iter().map(|id| id.clone()).collect()
            } else {
                return;
            }
        } else {
            return;
        };

        // Send message to each user in the group
        for user_id in user_ids {
            self.send_msg_to_user(tenant_id, &user_id, msg.clone());
        }
    }

    pub fn is_user_in_group(&self, tenant_id: &str, group_id: &str, user_id: &str) -> bool {
        if let Some(tenant_groups) = self.groups.get(tenant_id) {
            if let Some(members) = tenant_groups.get(group_id) {
                return members.contains(user_id);
            }
        }
        false
    }
}
