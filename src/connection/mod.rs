use std::sync::Arc;
use dashmap::DashMap;
use axum::extract::ws::Message;
// use serde::de;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{auth::Identity};


pub type ConnectionId = Uuid;

pub type SocketSender = mpsc::UnboundedSender<Message>;

#[derive(Debug, Clone)]
pub struct ConnectionRegistry {
    // tenant_id -> user_id -> connection_id -> sender
    inner: Arc<DashMap<String, DashMap<String, DashMap<ConnectionId, SocketSender>>>>,
}

impl ConnectionRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }
    pub fn insert (
        &self,
        identity: &Identity,
        connection_id: ConnectionId,
        sender: SocketSender,
    ) {
       self.inner
            .entry(identity.tenant_id.clone())
            .or_default()
            .entry(identity.user_id.clone()) // For simplicity, using "global" as channel_id
            .or_default()
            .insert(connection_id, sender); 
        tracing::info!("Registered connection: tenant_id={}, user_id={}", identity.tenant_id, identity.user_id);
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
}