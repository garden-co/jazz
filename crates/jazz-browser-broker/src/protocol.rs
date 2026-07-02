use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Visibility {
    Visible,
    Hidden,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub tab_id: String,
    pub visibility: Visibility,
    pub last_visible_at: i64,
}

fn is_false(value: &bool) -> bool {
    !value
}

// Serialization must spread-omit falsy optionals exactly like the JS senders
// (reportLeaderReady / reportStorageResetReady) — hence the skip attributes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TabMessage {
    #[serde(rename_all = "camelCase")]
    Hello {
        tab_id: String,
        app_id: String,
        db_name: String,
        fingerprint: String,
        visibility: Visibility,
        #[serde(skip_serializing_if = "Option::is_none")]
        force_takeover_timeout_ms: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        broker_ping_interval_ms: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        broker_pong_timeout_ms: Option<f64>,
    },
    #[serde(rename_all = "camelCase")]
    Visibility {
        broker_instance_id: String,
        visibility: Visibility,
    },
    #[serde(rename_all = "camelCase")]
    LeaderReady {
        broker_instance_id: String,
        leadership_id: u64,
        tab_lock_name: String,
        worker_lock_name: String,
        #[serde(default, skip_serializing_if = "is_false")]
        bridgeless_storage_reset: bool,
    },
    #[serde(rename_all = "camelCase")]
    LeaderFailed {
        broker_instance_id: String,
        leadership_id: u64,
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    FollowerPortAttached {
        broker_instance_id: String,
        leadership_id: u64,
        follower_tab_id: String,
    },
    #[serde(rename_all = "camelCase")]
    FollowerPortClosed {
        broker_instance_id: String,
        leadership_id: u64,
        follower_tab_id: String,
    },
    #[serde(rename_all = "camelCase")]
    SchemaReady {
        broker_instance_id: String,
        schema_fingerprint: String,
    },
    #[serde(rename_all = "camelCase")]
    StorageResetRequest {
        broker_instance_id: String,
        request_id: String,
    },
    #[serde(rename_all = "camelCase")]
    StorageResetReady {
        broker_instance_id: String,
        request_id: String,
        success: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_message: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Shutdown { broker_instance_id: String },
    #[serde(rename_all = "camelCase")]
    BrokerPong { broker_instance_id: String },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ControlMessage {
    #[serde(rename_all = "camelCase")]
    BrokerHello { broker_instance_id: String },
    #[serde(rename_all = "camelCase")]
    BrokerPing { broker_instance_id: String },
    #[serde(rename_all = "camelCase")]
    BecomeLeader {
        broker_instance_id: String,
        leadership_id: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        reset_request_id: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Demote {
        broker_instance_id: String,
        leadership_id: u64,
    },
    #[serde(rename_all = "camelCase")]
    LeaderReady {
        broker_instance_id: String,
        leader_tab_id: String,
        leadership_id: u64,
    },
    #[serde(rename_all = "camelCase")]
    FollowerReady {
        broker_instance_id: String,
        leader_tab_id: String,
        leadership_id: u64,
    },
    #[serde(rename_all = "camelCase")]
    CloseFollowerPort {
        broker_instance_id: String,
        leadership_id: u64,
    },
    #[serde(rename_all = "camelCase")]
    DetachFollowerPort {
        broker_instance_id: String,
        follower_tab_id: String,
        leadership_id: u64,
    },
    #[serde(rename_all = "camelCase")]
    StorageResetBegin {
        broker_instance_id: String,
        request_id: String,
        leadership_id: u64,
    },
    #[serde(rename_all = "camelCase")]
    StorageResetStarted {
        broker_instance_id: String,
        request_id: String,
    },
    #[serde(rename_all = "camelCase")]
    StorageResetFinished {
        broker_instance_id: String,
        request_id: String,
        success: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_message: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Unsupported {
        broker_instance_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        code: Option<String>,
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    SchemaBlocked {
        broker_instance_id: String,
        reason: String,
    },
    // The two port-carrying messages. The broker core never constructs these
    // (it emits AttachFollowerChannel commands instead); the tab client core
    // receives them with the `port` field stripped by the shell, which holds
    // the MessagePort and pairs it back on the matching Invoke* command.
    #[serde(rename_all = "camelCase")]
    AttachFollowerPort {
        broker_instance_id: String,
        follower_tab_id: String,
        leadership_id: u64,
    },
    #[serde(rename_all = "camelCase")]
    UseFollowerPort {
        broker_instance_id: String,
        leader_tab_id: String,
        leadership_id: u64,
    },
    /// Anything a future broker version might send: dropped by the tab client
    /// core exactly like the JS `switch` fell through unknown types.
    #[serde(other)]
    Unknown,
}

pub fn select_leader_candidate<'a, I>(candidates: I) -> Option<&'a Candidate>
where
    I: IntoIterator<Item = &'a Candidate>,
{
    let all: Vec<&Candidate> = candidates.into_iter().collect();
    let visible: Vec<&Candidate> = all
        .iter()
        .copied()
        .filter(|candidate| candidate.visibility == Visibility::Visible)
        .collect();
    let pool = if visible.is_empty() { &all } else { &visible };

    let mut selected: Option<&Candidate> = None;
    for candidate in pool {
        let Some(current) = selected else {
            selected = Some(candidate);
            continue;
        };
        if candidate.last_visible_at > current.last_visible_at
            || (candidate.last_visible_at == current.last_visible_at
                && candidate.tab_id > current.tab_id)
        {
            selected = Some(candidate);
        }
    }
    selected
}

pub fn is_stale_leadership_id(incoming: u64, current: u64) -> bool {
    incoming < current
}

pub fn normalize_positive_timeout(value: Option<f64>, fallback: u64) -> u64 {
    match value {
        Some(value) if value.is_finite() && value > 0.0 => value.floor().max(1.0) as u64,
        _ => fallback,
    }
}

pub fn normalize_force_takeover_timeout(value: Option<f64>) -> u64 {
    match value {
        Some(value) if value.is_finite() && value >= 0.0 => value.floor().max(0.0) as u64,
        _ => 1_000,
    }
}
