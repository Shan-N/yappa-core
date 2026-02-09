
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Claims {
    pub tenant_id: String,
    pub user_id: String,
    pub exp: usize,
}