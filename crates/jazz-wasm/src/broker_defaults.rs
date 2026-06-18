use serde::{Deserialize, Serialize};

pub(crate) const DEFAULT_BROKER_PING_INTERVAL_MS: u32 = 1_000;
pub(crate) const DEFAULT_BROKER_PONG_TIMEOUT_MS: u32 = 3_000;
pub(crate) const DEFAULT_BROKER_HELLO_TIMEOUT_MS: u32 = 5_000;
pub(crate) const DEFAULT_INITIAL_LEADERSHIP_TIMEOUT_MS: u32 = 100;
pub(crate) const DEFAULT_STORAGE_RESET_TIMEOUT_MS: u32 = 5_000;
pub(crate) const RESET_RECONNECT_ERROR: &str = "Browser broker restarted during storage reset";

pub(crate) const DEFAULT_FORCE_TAKEOVER_TIMEOUT_MS: u32 = 1_000;
pub(crate) const LEADER_FAILURE_RETRY_BACKOFF_MS: u64 = 1_000;
pub(crate) const INITIAL_FOLLOWER_ATTACHMENT_TIMEOUT_MS: u32 = 1_000;
pub(crate) const MAX_FOLLOWER_ATTACHMENT_TIMEOUT_MS: u32 = 30_000;
pub(crate) const COMPLETED_STORAGE_RESET_OUTCOME_TTL_MS: u64 = 30_000;
pub(crate) const MAX_COMPLETED_STORAGE_RESET_OUTCOMES: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BrokerVisibility {
    Visible,
    Hidden,
}
