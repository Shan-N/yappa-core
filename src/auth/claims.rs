#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Claims {
    pub tenant_id: String,
    pub user_id: String,
    pub exp: usize,
    #[serde(default)]
    pub iss: Option<String>,
    #[serde(default)]
    pub aud: Option<String>,
}
