use jsonwebtoken::{ Algorithm, DecodingKey, Validation, decode };

use crate::auth::{Identity};


#[derive(Debug)]
pub enum AuthError {
    InvalidToken,
}

pub struct AuthConfig {
    decoding_key: DecodingKey,
}

impl AuthConfig {
    pub fn new(secret: &str) -> Self {
        let decoding_key = DecodingKey::from_secret(secret.as_bytes());
        AuthConfig { decoding_key }
    }
    pub fn validate_jwt(&self, token: &str) -> Result<Identity, AuthError> {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.required_spec_claims.insert("exp".to_string());
        let data = decode::<Identity>(
            token,
            &self.decoding_key,
            &validation,
        ).map_err(|_| AuthError::InvalidToken)?;
        Ok(data.claims)
    }
}
