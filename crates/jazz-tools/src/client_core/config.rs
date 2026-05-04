use crate::query_manager::types::Schema;
use crate::sync_manager::DurabilityTier;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientRuntimeFlavor {
    BrowserMainThread,
    BrowserWorker,
    Node,
    ReactNative,
    Rust,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientStorageMode {
    Memory,
    Persistent,
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub app_id: String,
    pub schema: Schema,
    pub env: String,
    pub user_branch: String,
    pub storage_mode: ClientStorageMode,
    pub server_url: Option<String>,
    pub default_durability_tier: Option<DurabilityTier>,
    pub runtime_flavor: ClientRuntimeFlavor,
}

impl ClientConfig {
    pub fn memory_for_test(app_id: impl Into<String>, schema: Schema) -> Self {
        Self {
            app_id: app_id.into(),
            schema,
            env: "dev".to_string(),
            user_branch: "main".to_string(),
            storage_mode: ClientStorageMode::Memory,
            server_url: None,
            default_durability_tier: None,
            runtime_flavor: ClientRuntimeFlavor::Rust,
        }
    }
}
