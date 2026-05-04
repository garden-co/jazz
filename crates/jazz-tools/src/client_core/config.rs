#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub env: String,
    pub user_branch: String,
}

impl ClientConfig {
    pub fn new(env: impl Into<String>, user_branch: impl Into<String>) -> Self {
        Self {
            env: env.into(),
            user_branch: user_branch.into(),
        }
    }
}
