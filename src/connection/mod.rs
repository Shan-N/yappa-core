use std::sync::Arc;
use dashmap::{DashMap, DashSet};
use axum::extract::ws::Message;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::auth::Identity;


pub type ConnectionId = Uuid;

pub type SocketSender = mpsc::UnboundedSender<Message>;

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

    pub fn insert(
        &self,
        identity: &Identity,
        connection_id: ConnectionId,
        sender: SocketSender,
    ) {
        self.inner
            .entry(identity.tenant_id.clone())
            .or_default()
            .entry(identity.user_id.clone())
            .or_default()
            .insert(connection_id, sender);
        tracing::info!("Registered connection: tenant_id={}, user_id={}", identity.tenant_id, identity.user_id);
    }

    pub fn join_group(&self, tenant_id: &str, group_id: &str, user_id: &str) {
        self.groups
            .entry(tenant_id.to_string())
            .or_default()
            .entry(group_id.to_string())
            .or_default()
            .insert(user_id.to_string());
        tracing::info!("User {} joined group {} in tenant {}", user_id, group_id, tenant_id);
    }

    pub fn leave_group(&self, tenant_id: &str, group_id: &str, user_id: &str) {
        if let Some(tenant_groups) = self.groups.get(tenant_id) {
            if let Some(members) = tenant_groups.get(group_id) {
                members.remove(user_id);
                tracing::info!("User {} left group {} in tenant {}", user_id, group_id, tenant_id);
            }
        }
    }
    pub fn remove(
        &self, 
        identity: &Identity, 
        connection_id: &ConnectionId,
    ) {
        if let Some(users) = self.inner.get_mut(&identity.tenant_id) {
            if let Some(connections) = users.get_mut(&identity.user_id) {
                connections.remove(connection_id);
                if connections.is_empty() {
                    users.remove(&identity.user_id);
                }
                if users.is_empty() {
                    self.inner.remove(&identity.tenant_id);
                }
                tracing::info!("Removed connection: tenant_id={}, user_id={}", identity.tenant_id, identity.user_id);
            }
        } 
    }

    pub fn send_msg_to_user(&self, tenant_id: &str, user_id: &str, msg: Message) {
        if let Some(users) = self.inner.get(tenant_id) {
            if let Some(connections) = users.get(user_id) {
                for entry in connections.iter() {
                    let (conn_id, sender) = entry.pair();
                    if let Err(e) = sender.send(msg.clone()) {
                        tracing::error!("Failed to send message to connection {}: {}", conn_id, e);
                    }
                }
            }
        }
    }

    pub fn send_msg_to_group(&self, tenant_id: &str, group_id: &str, msg: Message) {
        // Get the list of users in the group to avoid holding lock during sends
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
}