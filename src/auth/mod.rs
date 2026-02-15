use std::sync::Arc;

use crate::auth::{
    claims::Claims,
    jwt::{AuthConfig, AuthError},
};

pub mod claims;
mod jwt;
mod keys;

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct Identity {
    pub tenant_id: String,
    pub user_id: String,
}

impl From<Claims> for Identity {
    fn from(claims: Claims) -> Self {
        Identity {
            tenant_id: claims.tenant_id,
            user_id: claims.user_id,
        }
    }
}

#[derive(Clone)]
pub struct Auth {
    config: Arc<AuthConfig>,
}

impl Auth {
    pub fn new(secret: &str) -> Self {
        let config = Arc::new(AuthConfig::new(secret));
        Auth { config }
    }
    pub fn authenticate(&self, token: &str) -> Result<Identity, AuthError> {
        self.config.validate_jwt(token)
    }
}
