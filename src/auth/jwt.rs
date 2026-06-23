use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};

use crate::auth::{Identity, claims::Claims};

#[derive(Debug)]
pub enum AuthError {
    InvalidToken,
}

pub struct AuthConfig {
    decoding_key: DecodingKey,
    validation: Validation,
}

impl AuthConfig {
    pub fn new(secret: &str, issuer: &str, audience: &str) -> Self {
        let decoding_key = DecodingKey::from_secret(secret.as_bytes());
        let mut validation = Validation::new(Algorithm::HS256);
        // Pin to HS256 only — prevents alg-confusion / "none" attacks.
        validation.algorithms = vec![Algorithm::HS256];
        validation.required_spec_claims.insert("exp".to_string());
        validation.required_spec_claims.insert("iss".to_string());
        validation.required_spec_claims.insert("aud".to_string());
        validation.set_issuer(&[issuer]);
        validation.set_audience(&[audience]);
        validation.validate_exp = true;

        AuthConfig {
            decoding_key,
            validation,
        }
    }

    pub fn validate_jwt(&self, token: &str) -> Result<Identity, AuthError> {
        let data = decode::<Claims>(token, &self.decoding_key, &self.validation)
            .map_err(|e| {
                tracing::warn!("JWT validation failed: {}", e);
                AuthError::InvalidToken
            })?;
        Ok(Identity {
            tenant_id: data.claims.tenant_id,
            user_id: data.claims.user_id,
        })
    }
}
