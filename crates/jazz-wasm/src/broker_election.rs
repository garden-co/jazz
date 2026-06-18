use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

pub use crate::broker_defaults::BrokerVisibility;
use crate::broker_defaults::{
    COMPLETED_STORAGE_RESET_OUTCOME_TTL_MS, DEFAULT_BROKER_PING_INTERVAL_MS,
    DEFAULT_BROKER_PONG_TIMEOUT_MS, DEFAULT_FORCE_TAKEOVER_TIMEOUT_MS,
    INITIAL_FOLLOWER_ATTACHMENT_TIMEOUT_MS, LEADER_FAILURE_RETRY_BACKOFF_MS,
    MAX_COMPLETED_STORAGE_RESET_OUTCOMES, MAX_FOLLOWER_ATTACHMENT_TIMEOUT_MS,
};
const INCOMPATIBLE_BROWSER_BROKER_CONFIGURATION_CODE: &str =
    "incompatible-browser-broker-configuration";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResetPhase {
    Preparing,
    Promoting,
    Reconnecting,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BrokerTimerId {
    BrokerPing,
    LeaderFailureRetry,
    FollowerAttachment {
        follower_tab_id: String,
        leadership_id: u32,
    },
    PreviousLeaderLocksForceTakeover {
        election_id: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TabConnected {
    pub tab_id: String,
    pub app_id: String,
    pub db_name: String,
    pub fingerprint: String,
    pub visibility: BrokerVisibility,
    pub now_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub force_takeover_timeout_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_ping_interval_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_pong_timeout_ms: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LeaderReady {
    pub tab_id: String,
    pub leadership_id: u32,
    pub tab_lock_name: String,
    pub worker_lock_name: String,
    pub bridgeless_storage_reset: bool,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageResetReady {
    pub tab_id: String,
    pub request_id: String,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BrokerEvent {
    TabConnected(TabConnected),
    VisibilityChanged {
        tab_id: String,
        visibility: BrokerVisibility,
        now_ms: u64,
    },
    SchemaReported {
        tab_id: String,
        schema_fingerprint: String,
    },
    LeaderReady(LeaderReady),
    LeaderFailed {
        tab_id: String,
        leadership_id: u32,
        reason: String,
        now_ms: u64,
    },
    FollowerPortAttached {
        leader_tab_id: String,
        follower_tab_id: String,
        leadership_id: u32,
        now_ms: u64,
    },
    FollowerPortClosed {
        leader_tab_id: String,
        follower_tab_id: String,
        leadership_id: u32,
        now_ms: u64,
    },
    StorageResetRequested {
        tab_id: String,
        request_id: String,
        now_ms: u64,
    },
    StorageResetReady(StorageResetReady),
    Shutdown {
        tab_id: String,
        now_ms: u64,
    },
    BrokerPong {
        tab_id: String,
        now_ms: u64,
    },
    TimerFired {
        timer_id: BrokerTimerId,
        now_ms: u64,
    },
    LeaderLockReleased {
        leadership_id: u32,
        now_ms: u64,
    },
    PreviousLeaderLocksReleased {
        election_id: u64,
        now_ms: u64,
    },
    ForceTakeoverComplete {
        election_id: u64,
        now_ms: u64,
    },
    ForceTakeoverFailed {
        election_id: u64,
        reason: String,
        now_ms: u64,
    },
}

impl BrokerEvent {
    fn now_ms(&self) -> Option<u64> {
        match self {
            BrokerEvent::TabConnected(event) => Some(event.now_ms),
            BrokerEvent::VisibilityChanged { now_ms, .. }
            | BrokerEvent::LeaderFailed { now_ms, .. }
            | BrokerEvent::FollowerPortAttached { now_ms, .. }
            | BrokerEvent::FollowerPortClosed { now_ms, .. }
            | BrokerEvent::StorageResetRequested { now_ms, .. }
            | BrokerEvent::Shutdown { now_ms, .. }
            | BrokerEvent::BrokerPong { now_ms, .. }
            | BrokerEvent::TimerFired { now_ms, .. }
            | BrokerEvent::LeaderLockReleased { now_ms, .. }
            | BrokerEvent::PreviousLeaderLocksReleased { now_ms, .. }
            | BrokerEvent::ForceTakeoverComplete { now_ms, .. }
            | BrokerEvent::ForceTakeoverFailed { now_ms, .. } => Some(*now_ms),
            BrokerEvent::LeaderReady(event) => Some(event.now_ms),
            BrokerEvent::StorageResetReady(event) => Some(event.now_ms),
            BrokerEvent::SchemaReported { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
pub enum BrokerControlMessage {
    BrokerHello {
        broker_instance_id: String,
    },
    Unsupported {
        broker_instance_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        code: Option<String>,
        reason: String,
    },
    BecomeLeader {
        broker_instance_id: String,
        leadership_id: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        reset_request_id: Option<String>,
    },
    Demote {
        broker_instance_id: String,
        leadership_id: u32,
    },
    LeaderReady {
        broker_instance_id: String,
        leader_tab_id: String,
        leadership_id: u32,
    },
    SchemaBlocked {
        broker_instance_id: String,
        reason: String,
    },
    BrokerPing {
        broker_instance_id: String,
    },
    AttachFollowerPort {
        broker_instance_id: String,
        follower_tab_id: String,
        leadership_id: u32,
    },
    UseFollowerPort {
        broker_instance_id: String,
        leader_tab_id: String,
        leadership_id: u32,
    },
    FollowerReady {
        broker_instance_id: String,
        leader_tab_id: String,
        leadership_id: u32,
    },
    CloseFollowerPort {
        broker_instance_id: String,
        leadership_id: u32,
    },
    DetachFollowerPort {
        broker_instance_id: String,
        follower_tab_id: String,
        leadership_id: u32,
    },
    StorageResetBegin {
        broker_instance_id: String,
        request_id: String,
        leadership_id: u32,
    },
    StorageResetStarted {
        broker_instance_id: String,
        request_id: String,
    },
    StorageResetFinished {
        broker_instance_id: String,
        request_id: String,
        success: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_message: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BrokerEffect {
    SendToTab {
        tab_id: String,
        message: BrokerControlMessage,
    },
    CloseTabPort {
        tab_id: String,
    },
    CloseReplacedTabPort {
        tab_id: String,
    },
    Broadcast {
        message: BrokerControlMessage,
    },
    ArmTimer {
        timer_id: BrokerTimerId,
        delay_ms: u32,
    },
    CancelTimer {
        timer_id: BrokerTimerId,
    },
    StartLeaderLockMonitor {
        leadership_id: u32,
        tab_lock_name: String,
        worker_lock_name: String,
    },
    CancelLeaderLockMonitor {
        leadership_id: u32,
    },
    WaitForPreviousLeaderLocks {
        election_id: u64,
        tab_lock_name: String,
        worker_lock_name: String,
    },
    StealPreviousLeaderLocks {
        election_id: u64,
        tab_lock_name: String,
        worker_lock_name: String,
    },
    AssignFollowerPort {
        leader_tab_id: String,
        follower_tab_id: String,
        leadership_id: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerSnapshot {
    pub namespace_app_id: Option<String>,
    pub namespace_db_name: Option<String>,
    pub namespace_fingerprint: Option<String>,
    pub schema_fingerprint: Option<String>,
    pub force_takeover_timeout_ms: Option<u32>,
    pub broker_pong_timeout_ms: Option<u32>,
    pub connected_tab_count: usize,
    pub leader_tab_id: Option<String>,
    pub leader_ready: bool,
    pub current_leadership_id: u32,
    pub replacement_election_id: Option<u64>,
    pub reset_phase: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BrokerElectionCore {
    broker_instance_id: String,
    namespace: Option<NamespaceState>,
    tabs: HashMap<String, TabState>,
    tab_order: Vec<String>,
    leader: Option<LeaderState>,
    current_leadership_id: u32,
    broker_ping_timer_armed: bool,
    leader_failure_retry_timer_armed: bool,
    failed_leader_retry_after_by_tab_id: HashMap<String, u64>,
    deferred_follower_attachments: HashSet<FollowerAttachmentKey>,
    pending_follower_attachments: HashSet<FollowerAttachmentKey>,
    attached_follower_ports: HashSet<FollowerAttachmentKey>,
    follower_attachment_retry_counts: HashMap<FollowerAttachmentKey, u32>,
    pending_lock_wait: Option<PendingLockWait>,
    reset_state: Option<ResetState>,
    completed_storage_reset_outcomes: Vec<StorageResetOutcome>,
    next_election_id: u64,
    last_now_ms: u64,
}

#[derive(Debug, Clone)]
struct NamespaceState {
    app_id: String,
    db_name: String,
    fingerprint: String,
    force_takeover_timeout_ms: u32,
    broker_ping_interval_ms: u32,
    broker_pong_timeout_ms: u32,
    schema_fingerprint: Option<String>,
}

#[derive(Debug, Clone)]
struct TabState {
    tab_id: String,
    visibility: BrokerVisibility,
    last_visible_at: u64,
    schema_fingerprint: Option<String>,
    last_pong_at: u64,
}

#[derive(Debug, Clone)]
struct LeaderState {
    tab_id: String,
    leadership_id: u32,
    ready: bool,
    tab_lock_name: Option<String>,
    worker_lock_name: Option<String>,
}

#[derive(Debug, Clone)]
struct ClearedLeaderState {
    leadership_id: u32,
    tab_lock_name: Option<String>,
    worker_lock_name: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingLockWaitOwner {
    ReplacementElection,
    StorageReset,
}

#[derive(Debug, Clone)]
struct PendingLockWait {
    election_id: u64,
    previous_leader: ClearedLeaderState,
    owner: PendingLockWaitOwner,
}

#[derive(Debug, Clone)]
struct ResetState {
    request_id: String,
    request_ids: Vec<String>,
    participants: HashSet<String>,
    prepared_tabs: HashSet<String>,
    errors: Vec<String>,
    previous_leader: Option<ClearedLeaderState>,
    promoted_leadership_id: Option<u32>,
    phase: ResetPhase,
}

#[derive(Debug, Clone)]
struct StorageResetOutcome {
    request_id: String,
    success: bool,
    error_message: Option<String>,
    finished_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FollowerAttachmentKey {
    follower_tab_id: String,
    leadership_id: u32,
}

impl BrokerElectionCore {
    pub fn new(broker_instance_id: String) -> Self {
        Self {
            broker_instance_id,
            namespace: None,
            tabs: HashMap::new(),
            tab_order: Vec::new(),
            leader: None,
            current_leadership_id: 0,
            broker_ping_timer_armed: false,
            leader_failure_retry_timer_armed: false,
            failed_leader_retry_after_by_tab_id: HashMap::new(),
            deferred_follower_attachments: HashSet::new(),
            pending_follower_attachments: HashSet::new(),
            attached_follower_ports: HashSet::new(),
            follower_attachment_retry_counts: HashMap::new(),
            pending_lock_wait: None,
            reset_state: None,
            completed_storage_reset_outcomes: Vec::new(),
            next_election_id: 1,
            last_now_ms: 0,
        }
    }

    pub fn handle(&mut self, event: BrokerEvent) -> Vec<BrokerEffect> {
        if let Some(now_ms) = event.now_ms() {
            self.last_now_ms = now_ms;
        }

        match event {
            BrokerEvent::TabConnected(event) => self.handle_tab_connected(event),
            BrokerEvent::VisibilityChanged {
                tab_id,
                visibility,
                now_ms,
            } => self.handle_visibility_changed(tab_id, visibility, now_ms),
            BrokerEvent::SchemaReported {
                tab_id,
                schema_fingerprint,
            } => self.handle_schema_reported(tab_id, schema_fingerprint),
            BrokerEvent::LeaderReady(event) => self.handle_leader_ready(event),
            BrokerEvent::LeaderFailed {
                tab_id,
                leadership_id,
                reason,
                now_ms,
            } => self.handle_leader_failed(tab_id, leadership_id, reason, now_ms),
            BrokerEvent::FollowerPortAttached {
                leader_tab_id,
                follower_tab_id,
                leadership_id,
                now_ms,
            } => self.handle_follower_port_attached(
                leader_tab_id,
                follower_tab_id,
                leadership_id,
                now_ms,
            ),
            BrokerEvent::FollowerPortClosed {
                leader_tab_id,
                follower_tab_id,
                leadership_id,
                now_ms: _,
            } => self.handle_follower_port_closed(leader_tab_id, follower_tab_id, leadership_id),
            BrokerEvent::StorageResetRequested {
                tab_id,
                request_id,
                now_ms,
            } => self.handle_storage_reset_requested(tab_id, request_id, now_ms),
            BrokerEvent::StorageResetReady(event) => self.handle_storage_reset_ready(event),
            BrokerEvent::Shutdown { tab_id, now_ms } => self.handle_shutdown(tab_id, now_ms),
            BrokerEvent::BrokerPong { tab_id, now_ms } => {
                if let Some(tab) = self.tabs.get_mut(&tab_id) {
                    tab.last_pong_at = now_ms;
                }
                Vec::new()
            }
            BrokerEvent::TimerFired { timer_id, now_ms } => {
                self.handle_timer_fired(timer_id, now_ms)
            }
            BrokerEvent::LeaderLockReleased {
                leadership_id,
                now_ms,
            } => self.handle_leader_lock_released(leadership_id, now_ms),
            BrokerEvent::PreviousLeaderLocksReleased {
                election_id,
                now_ms,
            }
            | BrokerEvent::ForceTakeoverComplete {
                election_id,
                now_ms,
            } => self.complete_previous_leader_lock_wait(election_id, now_ms),
            BrokerEvent::ForceTakeoverFailed {
                election_id,
                reason: _,
                now_ms: _,
            } => self.handle_force_takeover_failed(election_id),
        }
    }

    pub fn snapshot(&self) -> BrokerSnapshot {
        BrokerSnapshot {
            namespace_app_id: self.namespace.as_ref().map(|ns| ns.app_id.clone()),
            namespace_db_name: self.namespace.as_ref().map(|ns| ns.db_name.clone()),
            namespace_fingerprint: self.namespace.as_ref().map(|ns| ns.fingerprint.clone()),
            schema_fingerprint: self
                .namespace
                .as_ref()
                .and_then(|ns| ns.schema_fingerprint.clone()),
            force_takeover_timeout_ms: self
                .namespace
                .as_ref()
                .map(|ns| ns.force_takeover_timeout_ms),
            broker_pong_timeout_ms: self.namespace.as_ref().map(|ns| ns.broker_pong_timeout_ms),
            connected_tab_count: self.tabs.len(),
            leader_tab_id: self.leader.as_ref().map(|leader| leader.tab_id.clone()),
            leader_ready: self
                .leader
                .as_ref()
                .map(|leader| leader.ready)
                .unwrap_or(false),
            current_leadership_id: self.current_leadership_id,
            replacement_election_id: self
                .pending_lock_wait
                .as_ref()
                .filter(|wait| wait.owner == PendingLockWaitOwner::ReplacementElection)
                .map(|wait| wait.election_id),
            reset_phase: self.reset_state.as_ref().map(|reset| {
                match reset.phase {
                    ResetPhase::Preparing => "preparing",
                    ResetPhase::Promoting => "promoting",
                    ResetPhase::Reconnecting => "reconnecting",
                }
                .to_string()
            }),
        }
    }

    fn handle_tab_connected(&mut self, event: TabConnected) -> Vec<BrokerEffect> {
        let mut effects = Vec::new();
        self.prune_completed_storage_reset_outcomes(event.now_ms);

        if self.namespace.is_none() {
            self.namespace = Some(NamespaceState {
                app_id: event.app_id.clone(),
                db_name: event.db_name.clone(),
                fingerprint: event.fingerprint.clone(),
                force_takeover_timeout_ms: normalize_force_takeover_timeout(
                    event.force_takeover_timeout_ms,
                ),
                broker_ping_interval_ms: normalize_positive_timeout(
                    event.broker_ping_interval_ms,
                    DEFAULT_BROKER_PING_INTERVAL_MS,
                ),
                broker_pong_timeout_ms: normalize_positive_timeout(
                    event.broker_pong_timeout_ms,
                    DEFAULT_BROKER_PONG_TIMEOUT_MS,
                ),
                schema_fingerprint: None,
            });
        }

        if self.is_incompatible_namespace(&event) {
            effects.push(BrokerEffect::SendToTab {
                tab_id: event.tab_id.clone(),
                message: self.unsupported_configuration_message(),
            });
            effects.push(BrokerEffect::CloseTabPort {
                tab_id: event.tab_id,
            });
            return effects;
        }

        if self.tabs.contains_key(&event.tab_id) {
            effects.push(BrokerEffect::CloseReplacedTabPort {
                tab_id: event.tab_id.clone(),
            });
            effects.extend(self.clear_follower_attachment_state(&event.tab_id));
        } else {
            self.tab_order.push(event.tab_id.clone());
        }

        let last_visible_at = if event.visibility == BrokerVisibility::Visible {
            event.now_ms
        } else {
            0
        };
        self.tabs.insert(
            event.tab_id.clone(),
            TabState {
                tab_id: event.tab_id.clone(),
                visibility: event.visibility,
                last_visible_at,
                schema_fingerprint: None,
                last_pong_at: event.now_ms,
            },
        );

        effects.extend(self.start_broker_ping_timer(event.now_ms));
        effects.push(BrokerEffect::SendToTab {
            tab_id: event.tab_id.clone(),
            message: BrokerControlMessage::BrokerHello {
                broker_instance_id: self.broker_instance_id.clone(),
            },
        });
        effects.extend(self.redeliver_finished_storage_resets(&event.tab_id, event.now_ms));

        if let Some(reset_phase) = self.reset_state.as_ref().map(|reset| reset.phase) {
            effects.extend(self.add_tab_to_active_reset(&event.tab_id));
            if reset_phase != ResetPhase::Preparing {
                if let Some(leader) = self.leader.clone().filter(|leader| leader.ready) {
                    effects.push(BrokerEffect::SendToTab {
                        tab_id: event.tab_id.clone(),
                        message: BrokerControlMessage::LeaderReady {
                            broker_instance_id: self.broker_instance_id.clone(),
                            leader_tab_id: leader.tab_id.clone(),
                            leadership_id: leader.leadership_id,
                        },
                    });
                    effects.extend(self.assign_follower_ports(&leader));
                } else {
                    effects.extend(self.defer_follower_attachment_for_tab(&event.tab_id));
                }
            }
            return effects;
        }

        if self
            .leader
            .as_ref()
            .map(|leader| leader.tab_id == event.tab_id)
            .unwrap_or(false)
        {
            let leadership_id = self.leader.as_ref().map(|leader| leader.leadership_id);
            if let Some(leadership_id) = leadership_id {
                let (_cleared, clear_effects) = self.clear_leader(
                    leadership_id,
                    ClearLeaderOptions {
                        demote_leader: false,
                        remove_leader_tab: false,
                    },
                );
                effects.extend(clear_effects);
            }
        }

        if let Some(leader) = self.leader.clone().filter(|leader| leader.ready) {
            effects.push(BrokerEffect::SendToTab {
                tab_id: event.tab_id,
                message: BrokerControlMessage::LeaderReady {
                    broker_instance_id: self.broker_instance_id.clone(),
                    leader_tab_id: leader.tab_id.clone(),
                    leadership_id: leader.leadership_id,
                },
            });
            effects.extend(self.assign_follower_ports(&leader));
        } else {
            effects.extend(self.elect_if_needed(event.now_ms));
        }

        effects
    }

    fn handle_visibility_changed(
        &mut self,
        tab_id: String,
        visibility: BrokerVisibility,
        now_ms: u64,
    ) -> Vec<BrokerEffect> {
        if let Some(tab) = self.tabs.get_mut(&tab_id) {
            tab.visibility = visibility;
            if visibility == BrokerVisibility::Visible {
                tab.last_visible_at = now_ms;
            }
        }
        self.evict_stale_tabs(now_ms)
    }

    fn handle_schema_reported(
        &mut self,
        tab_id: String,
        schema_fingerprint: String,
    ) -> Vec<BrokerEffect> {
        if self.namespace.is_none() || !self.tabs.contains_key(&tab_id) {
            return Vec::new();
        }

        if self
            .namespace
            .as_ref()
            .and_then(|ns| ns.schema_fingerprint.as_ref())
            .is_none()
        {
            if let Some(namespace) = self.namespace.as_mut() {
                namespace.schema_fingerprint = Some(schema_fingerprint.clone());
            }
        }

        if let Some(tab) = self.tabs.get_mut(&tab_id) {
            tab.schema_fingerprint = Some(schema_fingerprint.clone());
        }

        let canonical = self
            .namespace
            .as_ref()
            .and_then(|ns| ns.schema_fingerprint.as_ref());
        if canonical != Some(&schema_fingerprint) {
            return self.block_tab_for_schema_mismatch(&tab_id);
        }

        if let Some(leader) = self.leader.clone().filter(|leader| leader.ready) {
            self.assign_follower_ports(&leader)
        } else if self.reset_state.is_some() {
            self.defer_follower_attachment_for_tab(&tab_id)
        } else {
            self.elect_if_needed(self.last_now_ms)
        }
    }

    fn handle_leader_ready(&mut self, event: LeaderReady) -> Vec<BrokerEffect> {
        let Some(current) = self.leader.as_mut() else {
            return self.demote_if_connected(&event.tab_id, event.leadership_id);
        };

        if current.tab_id != event.tab_id || current.leadership_id != event.leadership_id {
            return self.demote_if_connected(&event.tab_id, event.leadership_id);
        }

        if self
            .reset_state
            .as_ref()
            .map(|reset| reset.promoted_leadership_id == Some(event.leadership_id))
            .unwrap_or(false)
            && event.bridgeless_storage_reset
            && self
                .tabs
                .get(&event.tab_id)
                .and_then(|tab| tab.schema_fingerprint.as_ref())
                .is_none()
        {
            let (_cleared, mut effects) = self.clear_leader(
                event.leadership_id,
                ClearLeaderOptions {
                    demote_leader: true,
                    remove_leader_tab: false,
                },
            );
            effects.extend(self.finish_storage_reset(true, None, event.now_ms));
            return effects;
        }

        current.ready = true;
        current.tab_lock_name = Some(event.tab_lock_name.clone());
        current.worker_lock_name = Some(event.worker_lock_name.clone());
        let leader = current.clone();
        let is_reset_promoted_leader = self
            .reset_state
            .as_ref()
            .map(|reset| reset.promoted_leadership_id == Some(event.leadership_id))
            .unwrap_or(false);
        if is_reset_promoted_leader {
            if let Some(reset) = self.reset_state.as_mut() {
                reset.phase = ResetPhase::Reconnecting;
            }
        }

        let mut effects = vec![
            BrokerEffect::Broadcast {
                message: BrokerControlMessage::LeaderReady {
                    broker_instance_id: self.broker_instance_id.clone(),
                    leader_tab_id: leader.tab_id.clone(),
                    leadership_id: leader.leadership_id,
                },
            },
            BrokerEffect::StartLeaderLockMonitor {
                leadership_id: leader.leadership_id,
                tab_lock_name: event.tab_lock_name,
                worker_lock_name: event.worker_lock_name,
            },
        ];
        effects.extend(self.assign_follower_ports(&leader));

        if is_reset_promoted_leader {
            effects.extend(self.finish_storage_reset_if_reconnected(event.now_ms));
        }

        effects
    }

    fn handle_leader_failed(
        &mut self,
        tab_id: String,
        leadership_id: u32,
        reason: String,
        now_ms: u64,
    ) -> Vec<BrokerEffect> {
        if self
            .reset_state
            .as_ref()
            .map(|reset| reset.promoted_leadership_id == Some(leadership_id))
            .unwrap_or(false)
        {
            if let Some(reset) = self.reset_state.as_mut() {
                reset.errors.push(reason);
            }
            let mut effects = self.remove_tab(&tab_id, false, false, now_ms);
            effects.extend(self.remove_tab_from_active_reset(&tab_id, now_ms));
            self.leader = None;
            if let Some(reset) = self.reset_state.as_mut() {
                reset.promoted_leadership_id = None;
            }
            effects.extend(self.promote_reset_leader(now_ms));
            return effects;
        }

        if self
            .leader
            .as_ref()
            .map(|leader| leader.tab_id == tab_id && leader.leadership_id == leadership_id)
            .unwrap_or(false)
        {
            self.failed_leader_retry_after_by_tab_id
                .insert(tab_id, now_ms + LEADER_FAILURE_RETRY_BACKOFF_MS);
            let (cleared, mut effects) = self.clear_leader(
                leadership_id,
                ClearLeaderOptions {
                    demote_leader: true,
                    remove_leader_tab: false,
                },
            );
            effects.extend(self.schedule_replacement_election(cleared, now_ms));
            return effects;
        }

        Vec::new()
    }

    fn handle_follower_port_attached(
        &mut self,
        leader_tab_id: String,
        follower_tab_id: String,
        leadership_id: u32,
        now_ms: u64,
    ) -> Vec<BrokerEffect> {
        if !self
            .leader
            .as_ref()
            .map(|leader| {
                leader.tab_id == leader_tab_id
                    && leader.leadership_id == leadership_id
                    && leader.ready
            })
            .unwrap_or(false)
        {
            return Vec::new();
        }

        let key = FollowerAttachmentKey {
            follower_tab_id: follower_tab_id.clone(),
            leadership_id,
        };
        if !self.pending_follower_attachments.remove(&key) {
            return Vec::new();
        }
        self.follower_attachment_retry_counts.remove(&key);

        let Some(leader) = self.leader.as_ref() else {
            return Vec::new();
        };
        if !self.tabs.contains_key(&follower_tab_id) {
            return Vec::new();
        }

        self.attached_follower_ports.insert(key);
        let mut effects = vec![
            BrokerEffect::CancelTimer {
                timer_id: BrokerTimerId::FollowerAttachment {
                    follower_tab_id: follower_tab_id.clone(),
                    leadership_id,
                },
            },
            BrokerEffect::SendToTab {
                tab_id: follower_tab_id,
                message: BrokerControlMessage::FollowerReady {
                    broker_instance_id: self.broker_instance_id.clone(),
                    leader_tab_id: leader.tab_id.clone(),
                    leadership_id,
                },
            },
        ];
        effects.extend(self.finish_storage_reset_if_reconnected(now_ms));
        effects
    }

    fn handle_follower_port_closed(
        &mut self,
        leader_tab_id: String,
        follower_tab_id: String,
        leadership_id: u32,
    ) -> Vec<BrokerEffect> {
        if !self
            .leader
            .as_ref()
            .map(|leader| leader.tab_id == leader_tab_id && leader.leadership_id == leadership_id)
            .unwrap_or(false)
        {
            return Vec::new();
        }

        let mut effects = self.clear_follower_attachment_key(&follower_tab_id, leadership_id);
        let Some(leader) = self.leader.clone().filter(|leader| leader.ready) else {
            return effects;
        };
        effects.extend(self.assign_follower_ports(&leader));
        effects
    }

    fn handle_storage_reset_requested(
        &mut self,
        tab_id: String,
        request_id: String,
        now_ms: u64,
    ) -> Vec<BrokerEffect> {
        if let Some(reset) = self.reset_state.as_mut() {
            if !reset.request_ids.contains(&request_id) {
                reset.request_ids.push(request_id.clone());
            }
            if self.tabs.contains_key(&tab_id) {
                return vec![BrokerEffect::SendToTab {
                    tab_id,
                    message: BrokerControlMessage::StorageResetStarted {
                        broker_instance_id: self.broker_instance_id.clone(),
                        request_id,
                    },
                }];
            }
            return Vec::new();
        }

        let (previous_leader, mut effects) =
            if let Some(leadership_id) = self.leader.as_ref().map(|leader| leader.leadership_id) {
                self.clear_leader(
                    leadership_id,
                    ClearLeaderOptions {
                        demote_leader: false,
                        remove_leader_tab: false,
                    },
                )
            } else {
                (None, Vec::new())
            };

        let leadership_id = previous_leader
            .as_ref()
            .map(|leader| leader.leadership_id)
            .unwrap_or(self.current_leadership_id);
        let participants: HashSet<String> = self.all_tab_ids().into_iter().collect();
        self.reset_state = Some(ResetState {
            request_id: request_id.clone(),
            request_ids: vec![request_id.clone()],
            participants,
            prepared_tabs: HashSet::new(),
            errors: Vec::new(),
            previous_leader,
            promoted_leadership_id: None,
            phase: ResetPhase::Preparing,
        });

        if self.tabs.contains_key(&tab_id) {
            effects.push(BrokerEffect::SendToTab {
                tab_id,
                message: BrokerControlMessage::StorageResetStarted {
                    broker_instance_id: self.broker_instance_id.clone(),
                    request_id: request_id.clone(),
                },
            });
        }

        for participant in self.all_tab_ids() {
            effects.push(BrokerEffect::SendToTab {
                tab_id: participant,
                message: BrokerControlMessage::StorageResetBegin {
                    broker_instance_id: self.broker_instance_id.clone(),
                    request_id: request_id.clone(),
                    leadership_id,
                },
            });
        }

        effects.extend(self.continue_storage_reset_if_ready(now_ms));
        effects
    }

    fn handle_storage_reset_ready(&mut self, event: StorageResetReady) -> Vec<BrokerEffect> {
        let Some(reset) = self.reset_state.as_mut() else {
            return Vec::new();
        };
        if reset.request_id != event.request_id || reset.phase != ResetPhase::Preparing {
            return Vec::new();
        }
        if !reset.participants.contains(&event.tab_id) {
            return Vec::new();
        }
        if !event.success {
            reset.errors.push(event.error_message.unwrap_or_else(|| {
                format!("Tab {} failed to prepare storage reset", event.tab_id)
            }));
        }
        reset.prepared_tabs.insert(event.tab_id);
        self.continue_storage_reset_if_ready(event.now_ms)
    }

    fn handle_shutdown(&mut self, tab_id: String, now_ms: u64) -> Vec<BrokerEffect> {
        if self
            .leader
            .as_ref()
            .map(|leader| leader.tab_id == tab_id)
            .unwrap_or(false)
        {
            let leadership_id = self.leader.as_ref().unwrap().leadership_id;
            let reset_promoted = self
                .reset_state
                .as_ref()
                .map(|reset| {
                    reset.promoted_leadership_id == Some(leadership_id)
                        && reset.phase != ResetPhase::Preparing
                })
                .unwrap_or(false);
            let mut effects = self.remove_tab(&tab_id, false, false, now_ms);
            let (cleared, clear_effects) = self.clear_leader(
                leadership_id,
                ClearLeaderOptions {
                    demote_leader: false,
                    remove_leader_tab: false,
                },
            );
            effects.extend(clear_effects);
            effects.extend(self.remove_tab_from_active_reset(&tab_id, now_ms));
            if reset_promoted {
                if let Some(reset) = self.reset_state.as_mut() {
                    reset.promoted_leadership_id = None;
                }
                effects.extend(self.promote_reset_leader(now_ms));
                effects.extend(self.reset_if_idle());
                return effects;
            }
            effects.extend(self.schedule_replacement_election(cleared, now_ms));
            effects.extend(self.reset_if_idle());
            return effects;
        }

        let mut effects = self.remove_tab(&tab_id, false, true, now_ms);
        effects.extend(self.remove_tab_from_active_reset(&tab_id, now_ms));
        effects.extend(self.reset_if_idle());
        effects
    }

    fn handle_timer_fired(&mut self, timer_id: BrokerTimerId, now_ms: u64) -> Vec<BrokerEffect> {
        match timer_id {
            BrokerTimerId::BrokerPing => {
                self.broker_ping_timer_armed = false;
                let mut effects = self.send_broker_pings(now_ms);
                if self.namespace.is_some() && !self.tabs.is_empty() {
                    effects.extend(self.start_broker_ping_timer_without_immediate_ping());
                }
                effects
            }
            BrokerTimerId::LeaderFailureRetry => {
                self.leader_failure_retry_timer_armed = false;
                self.elect_if_needed(now_ms)
            }
            BrokerTimerId::FollowerAttachment {
                follower_tab_id,
                leadership_id,
            } => self.handle_follower_attachment_timer(follower_tab_id, leadership_id),
            BrokerTimerId::PreviousLeaderLocksForceTakeover { election_id } => {
                self.handle_previous_leader_locks_force_timer(election_id)
            }
        }
    }

    fn handle_leader_lock_released(
        &mut self,
        leadership_id: u32,
        now_ms: u64,
    ) -> Vec<BrokerEffect> {
        if !self
            .leader
            .as_ref()
            .map(|leader| leader.leadership_id == leadership_id)
            .unwrap_or(false)
        {
            return Vec::new();
        }
        let (cleared, mut effects) = self.clear_leader(
            leadership_id,
            ClearLeaderOptions {
                demote_leader: true,
                remove_leader_tab: true,
            },
        );
        effects.extend(self.schedule_replacement_election(cleared, now_ms));
        effects
    }

    fn handle_follower_attachment_timer(
        &mut self,
        follower_tab_id: String,
        leadership_id: u32,
    ) -> Vec<BrokerEffect> {
        let key = FollowerAttachmentKey {
            follower_tab_id: follower_tab_id.clone(),
            leadership_id,
        };
        let was_pending = self.pending_follower_attachments.remove(&key);
        let was_deferred = self.deferred_follower_attachments.remove(&key);
        if !was_pending && !was_deferred {
            return Vec::new();
        }

        if !self.tabs.contains_key(&follower_tab_id) {
            return Vec::new();
        };

        let retry_count = self
            .follower_attachment_retry_counts
            .get(&key)
            .copied()
            .unwrap_or(0)
            + 1;
        self.follower_attachment_retry_counts
            .insert(key, retry_count);

        let Some(leader) = self
            .leader
            .clone()
            .filter(|leader| leader.leadership_id == leadership_id)
        else {
            return Vec::new();
        };

        if leader.ready
            && self
                .reset_state
                .as_ref()
                .map(|reset| reset.phase == ResetPhase::Reconnecting)
                .unwrap_or(true)
        {
            self.assign_follower_ports(&leader)
        } else {
            self.defer_follower_attachment_for_tab(&follower_tab_id)
        }
    }

    fn handle_previous_leader_locks_force_timer(&self, election_id: u64) -> Vec<BrokerEffect> {
        // Stale timers intentionally no-op after the natural lock release wins
        // the race, or after a newer election replaces this wait.
        self.pending_lock_wait
            .as_ref()
            .filter(|wait| wait.election_id == election_id)
            .and_then(|wait| lock_names_from_cleared(&wait.previous_leader))
            .map(|(tab_lock_name, worker_lock_name)| {
                vec![BrokerEffect::StealPreviousLeaderLocks {
                    election_id,
                    tab_lock_name,
                    worker_lock_name,
                }]
            })
            .unwrap_or_default()
    }

    fn complete_previous_leader_lock_wait(
        &mut self,
        election_id: u64,
        now_ms: u64,
    ) -> Vec<BrokerEffect> {
        let Some(wait) = self
            .pending_lock_wait
            .as_ref()
            .filter(|wait| wait.election_id == election_id)
            .cloned()
        else {
            return Vec::new();
        };

        self.pending_lock_wait = None;
        let mut effects = vec![BrokerEffect::CancelTimer {
            timer_id: BrokerTimerId::PreviousLeaderLocksForceTakeover { election_id },
        }];
        match wait.owner {
            PendingLockWaitOwner::ReplacementElection => {
                effects.extend(self.elect_if_needed(now_ms));
            }
            PendingLockWaitOwner::StorageReset => {
                effects.extend(self.continue_storage_reset_after_previous_locks(now_ms));
            }
        }
        effects
    }

    fn handle_force_takeover_failed(&self, election_id: u64) -> Vec<BrokerEffect> {
        if !self
            .pending_lock_wait
            .as_ref()
            .map(|wait| wait.election_id == election_id)
            .unwrap_or(false)
        {
            return Vec::new();
        }

        let delay_ms = self
            .namespace
            .as_ref()
            .map(|ns| ns.force_takeover_timeout_ms)
            .unwrap_or(DEFAULT_FORCE_TAKEOVER_TIMEOUT_MS);
        vec![BrokerEffect::ArmTimer {
            timer_id: BrokerTimerId::PreviousLeaderLocksForceTakeover { election_id },
            delay_ms,
        }]
    }

    fn block_tab_for_schema_mismatch(&mut self, tab_id: &str) -> Vec<BrokerEffect> {
        let mut effects = vec![BrokerEffect::SendToTab {
            tab_id: tab_id.to_string(),
            message: BrokerControlMessage::SchemaBlocked {
                broker_instance_id: self.broker_instance_id.clone(),
                reason: "incompatible persistent browser schema".to_string(),
            },
        }];

        if let Some(leadership_id) = self
            .leader
            .as_ref()
            .filter(|leader| leader.tab_id == tab_id)
            .map(|leader| leader.leadership_id)
        {
            let (cleared, clear_effects) = self.clear_leader(
                leadership_id,
                ClearLeaderOptions {
                    demote_leader: true,
                    remove_leader_tab: false,
                },
            );
            effects.extend(clear_effects);
            effects.extend(self.schedule_replacement_election(cleared, self.last_now_ms));
        }

        effects
    }

    fn elect_if_needed(&mut self, now_ms: u64) -> Vec<BrokerEffect> {
        if self.reset_state.is_some()
            || self.pending_lock_wait.is_some()
            || self
                .leader
                .as_ref()
                .map(|leader| leader.ready)
                .unwrap_or(false)
            || self.tabs.is_empty()
        {
            return Vec::new();
        }

        if self.leader.is_some()
            && self
                .namespace
                .as_ref()
                .and_then(|ns| ns.schema_fingerprint.as_ref())
                .is_none()
        {
            return Vec::new();
        }

        let Some(candidate) = self.select_leader_candidate(now_ms) else {
            return self.schedule_leader_failure_retry_election(now_ms);
        };

        let mut effects = Vec::new();
        if let Some(current) = self.leader.as_ref() {
            let canonical = self
                .namespace
                .as_ref()
                .and_then(|ns| ns.schema_fingerprint.as_ref());
            let current_leader_is_canonical = self
                .tabs
                .get(&current.tab_id)
                .and_then(|tab| tab.schema_fingerprint.as_ref())
                == canonical;
            if current_leader_is_canonical || candidate.tab_id == current.tab_id {
                return Vec::new();
            }
            let (_cleared, clear_effects) = self.clear_leader(
                current.leadership_id,
                ClearLeaderOptions {
                    demote_leader: true,
                    remove_leader_tab: false,
                },
            );
            effects.extend(clear_effects);
        }

        self.current_leadership_id += 1;
        self.leader = Some(LeaderState {
            tab_id: candidate.tab_id.clone(),
            leadership_id: self.current_leadership_id,
            ready: false,
            tab_lock_name: self.current_leader_tab_lock_name(),
            worker_lock_name: self.current_leader_worker_lock_name(),
        });

        effects.push(BrokerEffect::SendToTab {
            tab_id: candidate.tab_id,
            message: BrokerControlMessage::BecomeLeader {
                broker_instance_id: self.broker_instance_id.clone(),
                leadership_id: self.current_leadership_id,
                reset_request_id: None,
            },
        });
        effects
    }

    fn demote_if_connected(&self, tab_id: &str, leadership_id: u32) -> Vec<BrokerEffect> {
        if !self.tabs.contains_key(tab_id) {
            return Vec::new();
        }
        vec![BrokerEffect::SendToTab {
            tab_id: tab_id.to_string(),
            message: BrokerControlMessage::Demote {
                broker_instance_id: self.broker_instance_id.clone(),
                leadership_id,
            },
        }]
    }

    fn select_leader_candidate(&mut self, now_ms: u64) -> Option<TabState> {
        let canonical_schema = self
            .namespace
            .as_ref()
            .and_then(|ns| ns.schema_fingerprint.clone());
        let mut candidates = Vec::new();
        for tab_id in self.all_tab_ids() {
            if self.is_leader_candidate_in_failure_backoff(&tab_id, now_ms) {
                continue;
            }
            let Some(tab) = self.tabs.get(&tab_id).cloned() else {
                continue;
            };
            if canonical_schema.is_some() && tab.schema_fingerprint != canonical_schema {
                continue;
            }
            candidates.push(tab);
        }

        let visible: Vec<TabState> = candidates
            .iter()
            .filter(|tab| tab.visibility == BrokerVisibility::Visible)
            .cloned()
            .collect();
        let pool = if visible.is_empty() {
            candidates
        } else {
            visible
        };

        pool.into_iter().max_by(|left, right| {
            left.last_visible_at
                .cmp(&right.last_visible_at)
                .then_with(|| left.tab_id.cmp(&right.tab_id))
        })
    }

    fn assign_follower_ports(&mut self, leader: &LeaderState) -> Vec<BrokerEffect> {
        if self
            .reset_state
            .as_ref()
            .map(|reset| reset.phase != ResetPhase::Reconnecting)
            .unwrap_or(false)
        {
            return Vec::new();
        }

        if !self.tabs.contains_key(&leader.tab_id) {
            return Vec::new();
        }

        let canonical_schema = self
            .namespace
            .as_ref()
            .and_then(|ns| ns.schema_fingerprint.clone());
        let follower_ids: Vec<String> = self
            .all_tab_ids()
            .into_iter()
            .filter_map(|tab_id| self.tabs.get(&tab_id).cloned())
            .filter(|tab| tab.tab_id != leader.tab_id)
            .filter(|tab| canonical_schema.is_none() || tab.schema_fingerprint == canonical_schema)
            .map(|tab| tab.tab_id)
            .collect();

        let mut effects = Vec::new();
        for follower_tab_id in follower_ids {
            let key = FollowerAttachmentKey {
                follower_tab_id: follower_tab_id.clone(),
                leadership_id: leader.leadership_id,
            };
            if self.pending_follower_attachments.contains(&key)
                || self.attached_follower_ports.contains(&key)
            {
                continue;
            }
            self.deferred_follower_attachments.remove(&key);
            let delay_ms = self.follower_attachment_timeout_ms(&key);
            self.pending_follower_attachments.insert(key);
            effects.push(BrokerEffect::AssignFollowerPort {
                leader_tab_id: leader.tab_id.clone(),
                follower_tab_id: follower_tab_id.clone(),
                leadership_id: leader.leadership_id,
            });
            effects.push(BrokerEffect::ArmTimer {
                timer_id: BrokerTimerId::FollowerAttachment {
                    follower_tab_id,
                    leadership_id: leader.leadership_id,
                },
                delay_ms,
            });
        }
        effects
    }

    fn defer_follower_attachment_for_tab(&mut self, follower_tab_id: &str) -> Vec<BrokerEffect> {
        let Some(leader) = self.leader.clone() else {
            return Vec::new();
        };
        let Some(tab) = self.tabs.get(follower_tab_id).cloned() else {
            return Vec::new();
        };
        if !self.should_assign_follower_port(&tab, &leader) {
            return Vec::new();
        }

        let key = FollowerAttachmentKey {
            follower_tab_id: follower_tab_id.to_string(),
            leadership_id: leader.leadership_id,
        };
        if self.pending_follower_attachments.contains(&key)
            || self.deferred_follower_attachments.contains(&key)
            || self.attached_follower_ports.contains(&key)
        {
            return Vec::new();
        }

        let delay_ms = self.follower_attachment_timeout_ms(&key);
        self.deferred_follower_attachments.insert(key);
        vec![BrokerEffect::ArmTimer {
            timer_id: BrokerTimerId::FollowerAttachment {
                follower_tab_id: follower_tab_id.to_string(),
                leadership_id: leader.leadership_id,
            },
            delay_ms,
        }]
    }

    fn defer_follower_attachments_for_leader(&mut self, leader: &LeaderState) -> Vec<BrokerEffect> {
        let follower_ids: Vec<String> = self
            .all_tab_ids()
            .into_iter()
            .filter_map(|tab_id| self.tabs.get(&tab_id).cloned())
            .filter(|tab| self.should_assign_follower_port(tab, leader))
            .map(|tab| tab.tab_id)
            .collect();

        let mut effects = Vec::new();
        for follower_tab_id in follower_ids {
            effects.extend(self.defer_follower_attachment_for_tab(&follower_tab_id));
        }
        effects
    }

    fn clear_leader(
        &mut self,
        leadership_id: u32,
        options: ClearLeaderOptions,
    ) -> (Option<ClearedLeaderState>, Vec<BrokerEffect>) {
        let Some(current) = self.leader.clone() else {
            return (None, Vec::new());
        };
        if current.leadership_id != leadership_id {
            return (None, Vec::new());
        }

        let mut effects = vec![BrokerEffect::CancelLeaderLockMonitor { leadership_id }];
        effects.extend(self.clear_all_follower_attachment_state());

        if options.demote_leader && self.tabs.contains_key(&current.tab_id) {
            effects.push(BrokerEffect::SendToTab {
                tab_id: current.tab_id.clone(),
                message: BrokerControlMessage::Demote {
                    broker_instance_id: self.broker_instance_id.clone(),
                    leadership_id,
                },
            });
        }

        for tab_id in self.all_tab_ids() {
            if tab_id == current.tab_id {
                continue;
            }
            effects.push(BrokerEffect::SendToTab {
                tab_id,
                message: BrokerControlMessage::CloseFollowerPort {
                    broker_instance_id: self.broker_instance_id.clone(),
                    leadership_id,
                },
            });
        }

        if options.remove_leader_tab {
            effects.extend(self.remove_tab(&current.tab_id, false, false, self.last_now_ms));
        }

        self.leader = None;
        (
            Some(ClearedLeaderState {
                leadership_id: current.leadership_id,
                tab_lock_name: current.tab_lock_name,
                worker_lock_name: current.worker_lock_name,
            }),
            effects,
        )
    }

    fn remove_tab(
        &mut self,
        tab_id: &str,
        close_port: bool,
        notify_leader: bool,
        now_ms: u64,
    ) -> Vec<BrokerEffect> {
        let Some(departed) = self.tabs.remove(tab_id) else {
            return Vec::new();
        };

        self.tab_order
            .retain(|ordered_tab_id| ordered_tab_id != tab_id);
        self.failed_leader_retry_after_by_tab_id.remove(tab_id);
        let mut effects = Vec::new();
        if notify_leader {
            if let Some(leader) = self
                .leader
                .as_ref()
                .filter(|leader| leader.tab_id != tab_id)
            {
                if self.tabs.contains_key(&leader.tab_id) {
                    effects.push(BrokerEffect::SendToTab {
                        tab_id: leader.tab_id.clone(),
                        message: BrokerControlMessage::DetachFollowerPort {
                            broker_instance_id: self.broker_instance_id.clone(),
                            follower_tab_id: tab_id.to_string(),
                            leadership_id: leader.leadership_id,
                        },
                    });
                }
            }
        }

        effects.extend(self.clear_follower_attachment_state(tab_id));
        effects.extend(self.reelect_schema_fingerprint_if_unheld(&departed, now_ms));
        if close_port {
            effects.push(BrokerEffect::CloseTabPort {
                tab_id: tab_id.to_string(),
            });
        }
        effects
    }

    fn remove_tab_from_active_reset(&mut self, tab_id: &str, now_ms: u64) -> Vec<BrokerEffect> {
        let Some(reset) = self.reset_state.as_mut() else {
            return Vec::new();
        };
        reset.participants.remove(tab_id);
        reset.prepared_tabs.remove(tab_id);
        self.continue_storage_reset_if_ready(now_ms)
    }

    fn reelect_schema_fingerprint_if_unheld(
        &mut self,
        departed: &TabState,
        now_ms: u64,
    ) -> Vec<BrokerEffect> {
        let Some(canonical) = self
            .namespace
            .as_ref()
            .and_then(|ns| ns.schema_fingerprint.clone())
        else {
            return Vec::new();
        };
        if departed.schema_fingerprint.as_deref() != Some(canonical.as_str()) {
            return Vec::new();
        }
        if self
            .tabs
            .values()
            .any(|tab| tab.schema_fingerprint.as_deref() == Some(canonical.as_str()))
        {
            return Vec::new();
        }

        let next = self
            .all_tab_ids()
            .into_iter()
            .filter_map(|tab_id| self.tabs.get(&tab_id))
            .find_map(|tab| tab.schema_fingerprint.clone());
        if let Some(namespace) = self.namespace.as_mut() {
            namespace.schema_fingerprint = next;
        }

        if self
            .namespace
            .as_ref()
            .and_then(|ns| ns.schema_fingerprint.as_ref())
            .is_none()
        {
            return Vec::new();
        }

        if let Some(leader) = self.leader.clone().filter(|leader| leader.ready) {
            self.assign_follower_ports(&leader)
        } else {
            self.elect_if_needed(now_ms)
        }
    }

    fn schedule_replacement_election(
        &mut self,
        previous_leader: Option<ClearedLeaderState>,
        now_ms: u64,
    ) -> Vec<BrokerEffect> {
        if self.pending_lock_wait.is_some() {
            return Vec::new();
        }
        let Some(previous_leader) = previous_leader else {
            return self.elect_if_needed(now_ms);
        };
        let Some((tab_lock_name, worker_lock_name)) = lock_names_from_cleared(&previous_leader)
        else {
            return self.elect_if_needed(now_ms);
        };

        let election_id = self.next_election_id;
        self.next_election_id += 1;
        self.pending_lock_wait = Some(PendingLockWait {
            election_id,
            previous_leader,
            owner: PendingLockWaitOwner::ReplacementElection,
        });

        vec![
            BrokerEffect::WaitForPreviousLeaderLocks {
                election_id,
                tab_lock_name: tab_lock_name.clone(),
                worker_lock_name: worker_lock_name.clone(),
            },
            BrokerEffect::ArmTimer {
                timer_id: BrokerTimerId::PreviousLeaderLocksForceTakeover { election_id },
                delay_ms: self.force_takeover_timeout_ms(),
            },
        ]
    }

    fn schedule_leader_failure_retry_election(&mut self, now_ms: u64) -> Vec<BrokerEffect> {
        if self.leader_failure_retry_timer_armed
            || self.reset_state.is_some()
            || self.pending_lock_wait.is_some()
            || self
                .leader
                .as_ref()
                .map(|leader| leader.ready)
                .unwrap_or(false)
        {
            return Vec::new();
        }

        let mut retry_at: Option<u64> = None;
        let tab_ids: Vec<String> = self
            .failed_leader_retry_after_by_tab_id
            .keys()
            .cloned()
            .collect();
        for tab_id in tab_ids {
            if !self.tabs.contains_key(&tab_id) {
                self.failed_leader_retry_after_by_tab_id.remove(&tab_id);
                continue;
            }
            if let Some(candidate_retry_at) = self.failed_leader_retry_after_by_tab_id.get(&tab_id)
            {
                retry_at = Some(retry_at.map_or(*candidate_retry_at, |current| {
                    current.min(*candidate_retry_at)
                }));
            }
        }

        let Some(retry_at) = retry_at else {
            return Vec::new();
        };
        self.leader_failure_retry_timer_armed = true;
        vec![BrokerEffect::ArmTimer {
            timer_id: BrokerTimerId::LeaderFailureRetry,
            delay_ms: retry_at.saturating_sub(now_ms) as u32,
        }]
    }

    fn continue_storage_reset_if_ready(&mut self, now_ms: u64) -> Vec<BrokerEffect> {
        let Some(reset) = self.reset_state.as_ref() else {
            return Vec::new();
        };
        if reset.phase != ResetPhase::Preparing {
            return Vec::new();
        }
        if reset
            .participants
            .iter()
            .any(|participant| !reset.prepared_tabs.contains(participant))
        {
            return Vec::new();
        }

        if let Some(reset) = self.reset_state.as_mut() {
            reset.phase = ResetPhase::Promoting;
        }

        let previous_leader = self
            .reset_state
            .as_ref()
            .and_then(|reset| reset.previous_leader.clone());
        let Some(previous_leader) = previous_leader else {
            return self.continue_storage_reset_after_previous_locks(now_ms);
        };
        let Some((tab_lock_name, worker_lock_name)) = lock_names_from_cleared(&previous_leader)
        else {
            return self.continue_storage_reset_after_previous_locks(now_ms);
        };

        let election_id = self.next_election_id;
        self.next_election_id += 1;
        self.pending_lock_wait = Some(PendingLockWait {
            election_id,
            previous_leader,
            owner: PendingLockWaitOwner::StorageReset,
        });
        vec![
            BrokerEffect::WaitForPreviousLeaderLocks {
                election_id,
                tab_lock_name: tab_lock_name.clone(),
                worker_lock_name: worker_lock_name.clone(),
            },
            BrokerEffect::ArmTimer {
                timer_id: BrokerTimerId::PreviousLeaderLocksForceTakeover { election_id },
                delay_ms: self.force_takeover_timeout_ms(),
            },
        ]
    }

    fn continue_storage_reset_after_previous_locks(&mut self, now_ms: u64) -> Vec<BrokerEffect> {
        let Some(reset) = self.reset_state.as_ref() else {
            return Vec::new();
        };
        if !reset.errors.is_empty() {
            return self.finish_storage_reset(false, Some(reset.errors.join("; ")), now_ms);
        }
        self.promote_reset_leader(now_ms)
    }

    fn promote_reset_leader(&mut self, now_ms: u64) -> Vec<BrokerEffect> {
        if self.reset_state.is_none() {
            return Vec::new();
        }

        let Some(candidate) = self.select_leader_candidate(now_ms) else {
            return self.finish_storage_reset(
                false,
                Some("No connected tab is available to reset storage".to_string()),
                now_ms,
            );
        };

        self.current_leadership_id += 1;
        let reset_request_id = self
            .reset_state
            .as_ref()
            .map(|reset| reset.request_id.clone());
        if let Some(reset) = self.reset_state.as_mut() {
            reset.promoted_leadership_id = Some(self.current_leadership_id);
        }

        let leader = LeaderState {
            tab_id: candidate.tab_id.clone(),
            leadership_id: self.current_leadership_id,
            ready: false,
            tab_lock_name: self.current_leader_tab_lock_name(),
            worker_lock_name: self.current_leader_worker_lock_name(),
        };
        self.leader = Some(leader.clone());

        let mut effects = vec![BrokerEffect::SendToTab {
            tab_id: candidate.tab_id,
            message: BrokerControlMessage::BecomeLeader {
                broker_instance_id: self.broker_instance_id.clone(),
                leadership_id: self.current_leadership_id,
                reset_request_id,
            },
        }];
        effects.extend(self.defer_follower_attachments_for_leader(&leader));
        effects
    }

    fn finish_storage_reset(
        &mut self,
        success: bool,
        error_message: Option<String>,
        now_ms: u64,
    ) -> Vec<BrokerEffect> {
        let Some(reset) = self.reset_state.take() else {
            return Vec::new();
        };

        let outcomes =
            self.remember_storage_reset_outcomes(reset.request_ids, success, error_message, now_ms);
        let mut effects = Vec::new();
        for tab_id in self.all_tab_ids() {
            for outcome in &outcomes {
                effects.push(BrokerEffect::SendToTab {
                    tab_id: tab_id.clone(),
                    message: self.storage_reset_finished_message(outcome),
                });
            }
        }
        if !success {
            effects.extend(self.elect_if_needed(now_ms));
        }
        effects
    }

    fn finish_storage_reset_if_reconnected(&mut self, now_ms: u64) -> Vec<BrokerEffect> {
        let Some(reset) = self.reset_state.as_ref() else {
            return Vec::new();
        };
        if reset.phase != ResetPhase::Reconnecting {
            return Vec::new();
        }
        let Some(leader) = self.leader.as_ref().filter(|leader| {
            leader.ready && Some(leader.leadership_id) == reset.promoted_leadership_id
        }) else {
            return Vec::new();
        };

        for participant in reset.participants.iter() {
            let Some(tab) = self.tabs.get(participant) else {
                continue;
            };
            if !self.should_assign_follower_port(tab, leader) {
                continue;
            }
            if !self
                .attached_follower_ports
                .contains(&FollowerAttachmentKey {
                    follower_tab_id: participant.clone(),
                    leadership_id: leader.leadership_id,
                })
            {
                return Vec::new();
            }
        }

        self.finish_storage_reset(true, None, now_ms)
    }

    fn add_tab_to_active_reset(&mut self, tab_id: &str) -> Vec<BrokerEffect> {
        let Some(reset) = self.reset_state.as_mut() else {
            return Vec::new();
        };
        if reset.phase != ResetPhase::Preparing || !self.tabs.contains_key(tab_id) {
            return Vec::new();
        }
        reset.participants.insert(tab_id.to_string());
        let leadership_id = reset
            .previous_leader
            .as_ref()
            .map(|leader| leader.leadership_id)
            .unwrap_or(self.current_leadership_id);
        vec![BrokerEffect::SendToTab {
            tab_id: tab_id.to_string(),
            message: BrokerControlMessage::StorageResetBegin {
                broker_instance_id: self.broker_instance_id.clone(),
                request_id: reset.request_id.clone(),
                leadership_id,
            },
        }]
    }

    fn start_broker_ping_timer(&mut self, now_ms: u64) -> Vec<BrokerEffect> {
        if self.broker_ping_timer_armed || self.namespace.is_none() {
            return Vec::new();
        }
        let mut effects = self.send_broker_pings(now_ms);
        effects.extend(self.start_broker_ping_timer_without_immediate_ping());
        effects
    }

    fn start_broker_ping_timer_without_immediate_ping(&mut self) -> Vec<BrokerEffect> {
        let Some(namespace) = self.namespace.as_ref() else {
            return Vec::new();
        };
        if self.broker_ping_timer_armed {
            return Vec::new();
        }
        self.broker_ping_timer_armed = true;
        vec![BrokerEffect::ArmTimer {
            timer_id: BrokerTimerId::BrokerPing,
            delay_ms: namespace.broker_ping_interval_ms,
        }]
    }

    fn stop_broker_ping_timer(&mut self) -> Vec<BrokerEffect> {
        if !self.broker_ping_timer_armed {
            return Vec::new();
        }
        self.broker_ping_timer_armed = false;
        vec![BrokerEffect::CancelTimer {
            timer_id: BrokerTimerId::BrokerPing,
        }]
    }

    fn send_broker_pings(&mut self, now_ms: u64) -> Vec<BrokerEffect> {
        if self.namespace.is_none() {
            return Vec::new();
        }
        let mut effects = Vec::new();
        for tab_id in self.all_tab_ids() {
            if self.is_broker_pong_timed_out(&tab_id, now_ms) {
                effects.extend(self.evict_tab(&tab_id, "missed broker pong", now_ms));
                continue;
            }
            if self.tabs.contains_key(&tab_id) {
                effects.push(BrokerEffect::SendToTab {
                    tab_id,
                    message: BrokerControlMessage::BrokerPing {
                        broker_instance_id: self.broker_instance_id.clone(),
                    },
                });
            }
        }
        effects
    }

    fn evict_stale_tabs(&mut self, now_ms: u64) -> Vec<BrokerEffect> {
        let mut effects = Vec::new();
        for tab_id in self.all_tab_ids() {
            if self.is_broker_pong_timed_out(&tab_id, now_ms) {
                effects.extend(self.evict_tab(&tab_id, "missed broker pong", now_ms));
            }
        }
        effects
    }

    fn evict_tab(&mut self, tab_id: &str, _reason: &str, now_ms: u64) -> Vec<BrokerEffect> {
        if !self.tabs.contains_key(tab_id) {
            return Vec::new();
        }

        let was_leader = self
            .leader
            .as_ref()
            .map(|leader| leader.tab_id == tab_id)
            .unwrap_or(false);
        let leadership_id = self.leader.as_ref().map(|leader| leader.leadership_id);
        let reset_promoted = leadership_id
            .and_then(|leadership_id| {
                self.reset_state.as_ref().map(|reset| {
                    reset.promoted_leadership_id == Some(leadership_id)
                        && reset.phase != ResetPhase::Preparing
                })
            })
            .unwrap_or(false);

        let mut effects = self.remove_tab(tab_id, true, true, now_ms);
        if was_leader {
            if let Some(leadership_id) = leadership_id {
                let (cleared, clear_effects) = self.clear_leader(
                    leadership_id,
                    ClearLeaderOptions {
                        demote_leader: false,
                        remove_leader_tab: false,
                    },
                );
                effects.extend(clear_effects);
                effects.extend(self.remove_tab_from_active_reset(tab_id, now_ms));
                if reset_promoted {
                    if let Some(reset) = self.reset_state.as_mut() {
                        reset.promoted_leadership_id = None;
                    }
                    effects.extend(self.promote_reset_leader(now_ms));
                    effects.extend(self.reset_if_idle());
                    return effects;
                }
                effects.extend(self.schedule_replacement_election(cleared, now_ms));
            }
        } else {
            effects.extend(self.remove_tab_from_active_reset(tab_id, now_ms));
        }
        effects.extend(self.reset_if_idle());
        effects
    }

    fn reset_if_idle(&mut self) -> Vec<BrokerEffect> {
        if !self.tabs.is_empty() {
            return Vec::new();
        }

        let mut effects = Vec::new();
        effects.extend(self.stop_broker_ping_timer());
        if self.leader_failure_retry_timer_armed {
            self.leader_failure_retry_timer_armed = false;
            effects.push(BrokerEffect::CancelTimer {
                timer_id: BrokerTimerId::LeaderFailureRetry,
            });
        }
        if let Some(wait) = self.pending_lock_wait.take() {
            effects.push(BrokerEffect::CancelTimer {
                timer_id: BrokerTimerId::PreviousLeaderLocksForceTakeover {
                    election_id: wait.election_id,
                },
            });
        }
        self.reset_state = None;
        effects.extend(self.clear_all_follower_attachment_state());
        self.namespace = None;
        self.leader = None;
        self.tab_order.clear();
        self.failed_leader_retry_after_by_tab_id.clear();
        effects
    }

    fn clear_all_follower_attachment_state(&mut self) -> Vec<BrokerEffect> {
        let mut timers: Vec<FollowerAttachmentKey> = self
            .pending_follower_attachments
            .iter()
            .chain(self.deferred_follower_attachments.iter())
            .cloned()
            .collect();
        sort_and_dedup_follower_attachment_keys(&mut timers);
        self.pending_follower_attachments.clear();
        self.deferred_follower_attachments.clear();
        self.attached_follower_ports.clear();
        self.follower_attachment_retry_counts.clear();
        timers
            .into_iter()
            .map(|key| BrokerEffect::CancelTimer {
                timer_id: BrokerTimerId::FollowerAttachment {
                    follower_tab_id: key.follower_tab_id,
                    leadership_id: key.leadership_id,
                },
            })
            .collect()
    }

    fn clear_follower_attachment_state(&mut self, follower_tab_id: &str) -> Vec<BrokerEffect> {
        let mut timers: Vec<FollowerAttachmentKey> = self
            .pending_follower_attachments
            .iter()
            .chain(self.deferred_follower_attachments.iter())
            .filter(|key| key.follower_tab_id == follower_tab_id)
            .cloned()
            .collect();
        sort_and_dedup_follower_attachment_keys(&mut timers);
        for key in &timers {
            self.pending_follower_attachments.remove(key);
            self.deferred_follower_attachments.remove(key);
            self.follower_attachment_retry_counts.remove(key);
        }
        self.attached_follower_ports
            .retain(|key| key.follower_tab_id != follower_tab_id);
        timers
            .into_iter()
            .map(|key| BrokerEffect::CancelTimer {
                timer_id: BrokerTimerId::FollowerAttachment {
                    follower_tab_id: key.follower_tab_id,
                    leadership_id: key.leadership_id,
                },
            })
            .collect()
    }

    fn clear_follower_attachment_key(
        &mut self,
        follower_tab_id: &str,
        leadership_id: u32,
    ) -> Vec<BrokerEffect> {
        let key = FollowerAttachmentKey {
            follower_tab_id: follower_tab_id.to_string(),
            leadership_id,
        };
        let had_pending = self.pending_follower_attachments.remove(&key);
        let had_deferred = self.deferred_follower_attachments.remove(&key);
        self.attached_follower_ports.remove(&key);
        self.follower_attachment_retry_counts.remove(&key);
        if had_pending || had_deferred {
            vec![BrokerEffect::CancelTimer {
                timer_id: BrokerTimerId::FollowerAttachment {
                    follower_tab_id: follower_tab_id.to_string(),
                    leadership_id,
                },
            }]
        } else {
            Vec::new()
        }
    }

    fn remember_storage_reset_outcomes(
        &mut self,
        request_ids: Vec<String>,
        success: bool,
        error_message: Option<String>,
        now_ms: u64,
    ) -> Vec<StorageResetOutcome> {
        let mut outcomes = Vec::new();
        for request_id in request_ids {
            self.completed_storage_reset_outcomes
                .retain(|outcome| outcome.request_id != request_id);
            let outcome = StorageResetOutcome {
                request_id,
                success,
                error_message: error_message.clone(),
                finished_at: now_ms,
            };
            self.completed_storage_reset_outcomes.push(outcome.clone());
            outcomes.push(outcome);
        }
        self.prune_completed_storage_reset_outcomes(now_ms);
        outcomes
    }

    fn prune_completed_storage_reset_outcomes(&mut self, now_ms: u64) {
        self.completed_storage_reset_outcomes.retain(|outcome| {
            now_ms.saturating_sub(outcome.finished_at) <= COMPLETED_STORAGE_RESET_OUTCOME_TTL_MS
        });
        while self.completed_storage_reset_outcomes.len() > MAX_COMPLETED_STORAGE_RESET_OUTCOMES {
            self.completed_storage_reset_outcomes.remove(0);
        }
    }

    fn redeliver_finished_storage_resets(
        &mut self,
        tab_id: &str,
        now_ms: u64,
    ) -> Vec<BrokerEffect> {
        self.prune_completed_storage_reset_outcomes(now_ms);
        self.completed_storage_reset_outcomes
            .iter()
            .map(|outcome| BrokerEffect::SendToTab {
                tab_id: tab_id.to_string(),
                message: self.storage_reset_finished_message(outcome),
            })
            .collect()
    }

    fn is_broker_pong_timed_out(&self, tab_id: &str, now_ms: u64) -> bool {
        let Some(namespace) = self.namespace.as_ref() else {
            return false;
        };
        let Some(tab) = self.tabs.get(tab_id) else {
            return false;
        };
        now_ms.saturating_sub(tab.last_pong_at) > namespace.broker_pong_timeout_ms as u64
    }

    fn is_leader_candidate_in_failure_backoff(&mut self, tab_id: &str, now_ms: u64) -> bool {
        let Some(retry_after) = self
            .failed_leader_retry_after_by_tab_id
            .get(tab_id)
            .copied()
        else {
            return false;
        };
        if retry_after <= now_ms {
            self.failed_leader_retry_after_by_tab_id.remove(tab_id);
            return false;
        }
        true
    }

    fn should_assign_follower_port(&self, tab: &TabState, leader: &LeaderState) -> bool {
        if tab.tab_id == leader.tab_id {
            return false;
        }
        let canonical = self
            .namespace
            .as_ref()
            .and_then(|ns| ns.schema_fingerprint.as_ref());
        canonical.is_none() || tab.schema_fingerprint.as_ref() == canonical
    }

    fn is_incompatible_namespace(&self, event: &TabConnected) -> bool {
        let Some(namespace) = self.namespace.as_ref() else {
            return false;
        };
        namespace.app_id != event.app_id
            || namespace.db_name != event.db_name
            || namespace.fingerprint != event.fingerprint
    }

    fn all_tab_ids(&self) -> Vec<String> {
        self.tab_order
            .iter()
            .filter(|tab_id| self.tabs.contains_key(*tab_id))
            .cloned()
            .collect()
    }

    fn current_leader_tab_lock_name(&self) -> Option<String> {
        self.namespace
            .as_ref()
            .map(|ns| format!("jazz-leader-tab:{}:{}", ns.app_id, ns.db_name))
    }

    fn current_leader_worker_lock_name(&self) -> Option<String> {
        self.namespace
            .as_ref()
            .map(|ns| format!("jazz-leader-worker:{}:{}", ns.app_id, ns.db_name))
    }

    fn force_takeover_timeout_ms(&self) -> u32 {
        self.namespace
            .as_ref()
            .map(|ns| ns.force_takeover_timeout_ms)
            .unwrap_or(DEFAULT_FORCE_TAKEOVER_TIMEOUT_MS)
    }

    fn follower_attachment_timeout_ms(&self, key: &FollowerAttachmentKey) -> u32 {
        let retry_count = self
            .follower_attachment_retry_counts
            .get(key)
            .copied()
            .unwrap_or(0);
        INITIAL_FOLLOWER_ATTACHMENT_TIMEOUT_MS
            .saturating_mul(2_u32.saturating_pow(retry_count))
            .min(MAX_FOLLOWER_ATTACHMENT_TIMEOUT_MS)
    }

    fn unsupported_configuration_message(&self) -> BrokerControlMessage {
        BrokerControlMessage::Unsupported {
            broker_instance_id: self.broker_instance_id.clone(),
            code: Some(INCOMPATIBLE_BROWSER_BROKER_CONFIGURATION_CODE.to_string()),
            reason: "incompatible persistent browser configuration".to_string(),
        }
    }

    fn storage_reset_finished_message(
        &self,
        outcome: &StorageResetOutcome,
    ) -> BrokerControlMessage {
        BrokerControlMessage::StorageResetFinished {
            broker_instance_id: self.broker_instance_id.clone(),
            request_id: outcome.request_id.clone(),
            success: outcome.success,
            error_message: outcome.error_message.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ClearLeaderOptions {
    demote_leader: bool,
    remove_leader_tab: bool,
}

fn lock_names_from_cleared(previous: &ClearedLeaderState) -> Option<(String, String)> {
    Some((
        previous.tab_lock_name.clone()?,
        previous.worker_lock_name.clone()?,
    ))
}

fn sort_and_dedup_follower_attachment_keys(keys: &mut Vec<FollowerAttachmentKey>) {
    keys.sort_by(|left, right| {
        left.leadership_id
            .cmp(&right.leadership_id)
            .then_with(|| left.follower_tab_id.cmp(&right.follower_tab_id))
    });
    keys.dedup();
}

#[wasm_bindgen(js_name = BrokerElection)]
pub struct BrokerElection {
    core: BrokerElectionCore,
}

#[wasm_bindgen(js_class = BrokerElection)]
impl BrokerElection {
    #[wasm_bindgen(constructor)]
    pub fn new(broker_instance_id: String) -> Self {
        Self {
            core: BrokerElectionCore::new(broker_instance_id),
        }
    }

    #[wasm_bindgen(js_name = handleEvent)]
    pub fn handle_event(&mut self, event: JsValue) -> Result<JsValue, JsError> {
        let event: BrokerEvent = serde_wasm_bindgen::from_value(event)
            .map_err(|err| JsError::new(&format!("broker event: {err}")))?;
        let effects = self.core.handle(event);
        serde_wasm_bindgen::to_value(&effects)
            .map_err(|err| JsError::new(&format!("broker effects: {err}")))
    }

    #[wasm_bindgen(js_name = snapshot)]
    pub fn snapshot_js(&self) -> Result<JsValue, JsError> {
        serde_wasm_bindgen::to_value(&self.core.snapshot())
            .map_err(|err| JsError::new(&format!("broker snapshot: {err}")))
    }
}

fn normalize_positive_timeout(value: Option<u32>, fallback: u32) -> u32 {
    value.filter(|value| *value > 0).unwrap_or(fallback).max(1)
}

fn normalize_force_takeover_timeout(value: Option<u32>) -> u32 {
    value.unwrap_or(DEFAULT_FORCE_TAKEOVER_TIMEOUT_MS)
}
