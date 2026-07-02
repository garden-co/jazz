use std::collections::HashMap;

use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};

use crate::protocol::{
    ControlMessage, TabMessage, Visibility, normalize_force_takeover_timeout,
    normalize_positive_timeout, select_leader_candidate,
};

const DEFAULT_FORCE_TAKEOVER_TIMEOUT_MS: u64 = 1_000;
const LEADER_FAILURE_RETRY_BACKOFF_MS: i64 = 1_000;
const INITIAL_FOLLOWER_ATTACHMENT_TIMEOUT_MS: u64 = 1_000;
const MAX_FOLLOWER_ATTACHMENT_TIMEOUT_MS: u64 = 30_000;
const COMPLETED_STORAGE_RESET_OUTCOME_TTL_MS: i64 = 30_000;
const MAX_COMPLETED_STORAGE_RESET_OUTCOMES: usize = 100;
const DEFAULT_BROKER_PING_INTERVAL_MS: u64 = 1_000;
const DEFAULT_BROKER_PONG_TIMEOUT_MS: u64 = 3_000;
const INCOMPATIBLE_BROWSER_BROKER_CONFIGURATION_CODE: &str =
    "incompatible-browser-broker-configuration";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PortId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProbeId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MonitorId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum TimerKey {
    BrokerPing,
    #[serde(rename_all = "camelCase")]
    FollowerAttachment {
        leadership_id: u64,
        follower_tab_id: String,
    },
    LeaderFailureRetry,
    #[serde(rename_all = "camelCase")]
    ForceTakeoverSleep {
        probe_id: ProbeId,
    },
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BrokerEvent {
    #[serde(rename_all = "camelCase")]
    PortMessage {
        port_id: PortId,
        message: TabMessage,
    },
    #[serde(rename_all = "camelCase")]
    TimerFired { timer: TimerKey },
    #[serde(rename_all = "camelCase")]
    LocksProbeResult {
        probe_id: ProbeId,
        all_acquired: bool,
    },
    #[serde(rename_all = "camelCase")]
    LocksStolen { probe_id: ProbeId },
    #[serde(rename_all = "camelCase")]
    LockMonitorTriggered { monitor_id: MonitorId },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BrokerCommand {
    Post {
        port_id: PortId,
        message: ControlMessage,
    },
    ClosePort {
        port_id: PortId,
    },
    #[serde(rename_all = "camelCase")]
    AttachFollowerChannel {
        leader_port_id: PortId,
        follower_port_id: PortId,
        leader_tab_id: String,
        follower_tab_id: String,
        leadership_id: u64,
    },
    SetTimer {
        timer: TimerKey,
        delay_ms: u64,
    },
    ClearTimer {
        timer: TimerKey,
    },
    ProbeLocks {
        probe_id: ProbeId,
        lock_names: Vec<String>,
    },
    StealLocks {
        probe_id: ProbeId,
        lock_names: Vec<String>,
    },
    MonitorLock {
        monitor_id: MonitorId,
        lock_name: String,
    },
    CancelLockMonitor {
        monitor_id: MonitorId,
    },
    #[serde(rename_all = "camelCase")]
    WarnStaleInstanceDrop {
        message_type: String,
        tab_id: String,
        stamped_instance_id: String,
    },
}

#[derive(Debug)]
pub struct BrokerCore {
    broker_instance_id: String,
    tabs: IndexMap<String, TabState>,
    // Port -> tab binding, set by the last hello on that port. Mirrors the JS
    // per-port closure variable: messages keep routing to the bound tab even
    // after the tab entry was removed or rebound to a newer port.
    port_tab_bindings: HashMap<PortId, String>,
    namespace: Option<Namespace>,
    leader: Option<LeaderState>,
    current_leadership_id: u64,
    pending_follower_attachments: IndexSet<AttachmentKey>,
    follower_attachment_retry_counts: HashMap<AttachmentKey, u32>,
    attached_follower_ports: IndexSet<AttachmentKey>,
    warned_stale_instance_drop: bool,
    replacement_election_in_flight: bool,
    replacement_election_generation: u64,
    broker_ping_timer_running: bool,
    leader_failure_retry_timer_running: bool,
    reset_state: Option<ResetState>,
    completed_storage_reset_outcomes: IndexMap<String, StorageResetOutcome>,
    failed_leader_retry_after_by_tab_id: HashMap<String, i64>,
    pending_takeover: Option<PendingTakeover>,
    next_probe_id: u64,
    next_monitor_id: u64,
}

#[derive(Debug, Clone)]
struct Namespace {
    app_id: String,
    db_name: String,
    fingerprint: String,
    force_takeover_timeout_ms: u64,
    broker_ping_interval_ms: u64,
    broker_pong_timeout_ms: u64,
    schema_fingerprint: Option<String>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct TabState {
    tab_id: String,
    app_id: String,
    db_name: String,
    fingerprint: String,
    schema_fingerprint: Option<String>,
    visibility: Visibility,
    last_visible_at: i64,
    port_id: PortId,
    last_pong_at: i64,
}

#[derive(Debug, Clone)]
struct LeaderState {
    tab_id: String,
    leadership_id: u64,
    ready: bool,
    tab_lock_name: Option<String>,
    worker_lock_name: Option<String>,
    tab_lock_monitor: Option<MonitorId>,
    worker_lock_monitor: Option<MonitorId>,
}

#[derive(Debug, Clone)]
struct ClearedLeaderState {
    leadership_id: u64,
    tab_lock_name: Option<String>,
    worker_lock_name: Option<String>,
}

#[derive(Debug, Clone)]
struct ResetState {
    request_id: String,
    request_ids: IndexSet<String>,
    participants: IndexSet<String>,
    prepared_tabs: IndexSet<String>,
    errors: Vec<String>,
    previous_leader: Option<ClearedLeaderState>,
    promoted_leadership_id: Option<u64>,
    phase: ResetPhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResetPhase {
    Preparing,
    Promoting,
    Reconnecting,
}

#[derive(Debug, Clone)]
struct StorageResetOutcome {
    request_id: String,
    success: bool,
    error_message: Option<String>,
    finished_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AttachmentKey {
    leadership_id: u64,
    follower_tab_id: String,
}

#[derive(Debug, Clone)]
struct PendingTakeover {
    probe_id: ProbeId,
    purpose: TakeoverPurpose,
    lock_names: Vec<String>,
    stage: TakeoverStage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TakeoverPurpose {
    ReplacementElection { generation: u64 },
    StorageReset { request_id: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TakeoverStage {
    Probing,
    Sleeping,
    Stealing,
}

impl BrokerCore {
    pub fn new(broker_instance_id: String) -> Self {
        Self {
            broker_instance_id,
            tabs: IndexMap::new(),
            port_tab_bindings: HashMap::new(),
            namespace: None,
            leader: None,
            current_leadership_id: 0,
            pending_follower_attachments: IndexSet::new(),
            follower_attachment_retry_counts: HashMap::new(),
            attached_follower_ports: IndexSet::new(),
            warned_stale_instance_drop: false,
            replacement_election_in_flight: false,
            replacement_election_generation: 0,
            broker_ping_timer_running: false,
            leader_failure_retry_timer_running: false,
            reset_state: None,
            completed_storage_reset_outcomes: IndexMap::new(),
            failed_leader_retry_after_by_tab_id: HashMap::new(),
            pending_takeover: None,
            next_probe_id: 0,
            next_monitor_id: 0,
        }
    }

    pub fn handle(&mut self, event: BrokerEvent, now_ms: i64) -> Vec<BrokerCommand> {
        let mut commands = Vec::new();
        match event {
            BrokerEvent::PortMessage { port_id, message } => {
                self.handle_port_message(port_id, message, now_ms, &mut commands);
            }
            BrokerEvent::TimerFired { timer } => {
                self.handle_timer_fired(timer, now_ms, &mut commands);
            }
            BrokerEvent::LocksProbeResult {
                probe_id,
                all_acquired,
            } => {
                self.handle_locks_probe_result(probe_id, all_acquired, now_ms, &mut commands);
            }
            BrokerEvent::LocksStolen { probe_id } => {
                self.handle_locks_stolen(probe_id, now_ms, &mut commands);
            }
            BrokerEvent::LockMonitorTriggered { monitor_id } => {
                self.handle_lock_monitor_triggered(monitor_id, now_ms, &mut commands);
            }
        }
        commands
    }

    fn handle_port_message(
        &mut self,
        port_id: PortId,
        message: TabMessage,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        if let TabMessage::Hello { .. } = message {
            self.handle_hello(port_id, message, now_ms, commands);
            return;
        }

        let Some(tab_id) = self.port_tab_bindings.get(&port_id).cloned() else {
            return;
        };
        self.handle_tab_message(tab_id, message, now_ms, commands);
    }

    fn handle_hello(
        &mut self,
        port_id: PortId,
        message: TabMessage,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let TabMessage::Hello {
            tab_id,
            app_id,
            db_name,
            fingerprint,
            visibility,
            force_takeover_timeout_ms,
            broker_ping_interval_ms,
            broker_pong_timeout_ms,
        } = message
        else {
            return;
        };

        if self.namespace.is_none() {
            self.namespace = Some(Namespace {
                app_id: app_id.clone(),
                db_name: db_name.clone(),
                fingerprint: fingerprint.clone(),
                force_takeover_timeout_ms: normalize_force_takeover_timeout(
                    force_takeover_timeout_ms,
                ),
                broker_ping_interval_ms: normalize_positive_timeout(
                    broker_ping_interval_ms,
                    DEFAULT_BROKER_PING_INTERVAL_MS,
                ),
                broker_pong_timeout_ms: normalize_positive_timeout(
                    broker_pong_timeout_ms,
                    DEFAULT_BROKER_PONG_TIMEOUT_MS,
                ),
                schema_fingerprint: None,
            });
        }

        let Some(namespace) = &self.namespace else {
            return;
        };

        if namespace.app_id != app_id
            || namespace.db_name != db_name
            || namespace.fingerprint != fingerprint
        {
            self.post(
                port_id,
                ControlMessage::Unsupported {
                    broker_instance_id: self.broker_instance_id.clone(),
                    code: Some(INCOMPATIBLE_BROWSER_BROKER_CONFIGURATION_CODE.to_string()),
                    reason: "incompatible persistent browser configuration".to_string(),
                },
                commands,
            );
            commands.push(BrokerCommand::ClosePort { port_id });
            self.port_tab_bindings.remove(&port_id);
            return;
        }
        self.port_tab_bindings.insert(port_id, tab_id.clone());

        if let Some(previous_tab) = self.tabs.get(&tab_id)
            && previous_tab.port_id != port_id
        {
            commands.push(BrokerCommand::ClosePort {
                port_id: previous_tab.port_id,
            });
        }
        self.clear_follower_attachment_state(&tab_id, commands);

        self.tabs.insert(
            tab_id.clone(),
            TabState {
                tab_id: tab_id.clone(),
                app_id,
                db_name,
                fingerprint,
                schema_fingerprint: None,
                visibility,
                last_visible_at: if visibility == Visibility::Visible {
                    now_ms
                } else {
                    0
                },
                port_id,
                last_pong_at: now_ms,
            },
        );

        self.start_broker_ping_timer(now_ms, commands);
        self.post(
            port_id,
            ControlMessage::BrokerHello {
                broker_instance_id: self.broker_instance_id.clone(),
            },
            commands,
        );
        self.redeliver_finished_storage_resets(port_id, now_ms, commands);

        if self.reset_state.is_some() {
            self.add_tab_to_active_reset(&tab_id, commands);
            return;
        }

        if self
            .leader
            .as_ref()
            .is_some_and(|leader| leader.tab_id == tab_id)
        {
            let leadership_id = self.leader.as_ref().map(|leader| leader.leadership_id);
            if let Some(leadership_id) = leadership_id {
                self.clear_leader(
                    leadership_id,
                    ClearLeaderOptions {
                        demote_leader: false,
                        remove_leader_tab: false,
                    },
                    now_ms,
                    commands,
                );
            }
        }

        if let Some(leader) = self.leader.clone().filter(|leader| leader.ready) {
            self.post(
                port_id,
                ControlMessage::LeaderReady {
                    broker_instance_id: self.broker_instance_id.clone(),
                    leader_tab_id: leader.tab_id.clone(),
                    leadership_id: leader.leadership_id,
                },
                commands,
            );
            self.assign_follower_ports(&leader, commands);
        } else {
            self.elect_if_needed(now_ms, commands);
        }
    }

    fn handle_tab_message(
        &mut self,
        tab_id: String,
        message: TabMessage,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        if let Some((broker_instance_id, message_type)) = broker_instance_stamp(&message)
            && broker_instance_id != self.broker_instance_id
        {
            if !self.warned_stale_instance_drop {
                self.warned_stale_instance_drop = true;
                commands.push(BrokerCommand::WarnStaleInstanceDrop {
                    message_type,
                    tab_id,
                    stamped_instance_id: broker_instance_id,
                });
            }
            return;
        }

        match message {
            TabMessage::Hello { .. } | TabMessage::Unknown => {}
            TabMessage::Visibility { visibility, .. } => {
                self.update_visibility(&tab_id, visibility, now_ms);
                self.evict_stale_tabs(now_ms, commands);
            }
            TabMessage::LeaderReady {
                leadership_id,
                tab_lock_name,
                worker_lock_name,
                bridgeless_storage_reset,
                ..
            } => {
                self.handle_leader_ready(
                    LeaderReadyInput {
                        tab_id: &tab_id,
                        leadership_id,
                        tab_lock_name,
                        worker_lock_name,
                        bridgeless_storage_reset,
                        now_ms,
                    },
                    commands,
                );
            }
            TabMessage::FollowerPortAttached {
                leadership_id,
                follower_tab_id,
                ..
            } => {
                if self.leader.as_ref().is_some_and(|leader| {
                    leader.tab_id == tab_id && leader.leadership_id == leadership_id
                }) {
                    self.mark_follower_port_attached(
                        &follower_tab_id,
                        leadership_id,
                        now_ms,
                        commands,
                    );
                }
            }
            TabMessage::FollowerPortClosed {
                leadership_id,
                follower_tab_id,
                ..
            } => {
                let leader = self.leader.clone();
                if leader.as_ref().is_some_and(|leader| {
                    leader.tab_id == tab_id && leader.leadership_id == leadership_id
                }) {
                    self.clear_follower_attachment_key(&follower_tab_id, leadership_id, commands);
                    if let Some(leader) = leader {
                        self.assign_follower_ports(&leader, commands);
                    }
                }
            }
            TabMessage::SchemaReady {
                schema_fingerprint, ..
            } => {
                self.handle_schema_ready(&tab_id, schema_fingerprint, now_ms, commands);
            }
            TabMessage::LeaderFailed {
                leadership_id,
                reason,
                ..
            } => {
                self.handle_leader_failed(&tab_id, leadership_id, reason, now_ms, commands);
            }
            TabMessage::StorageResetRequest { request_id, .. } => {
                self.start_storage_reset(&tab_id, request_id, now_ms, commands);
            }
            TabMessage::StorageResetReady {
                request_id,
                success,
                error_message,
                ..
            } => {
                self.handle_storage_reset_ready(
                    &tab_id,
                    &request_id,
                    success,
                    error_message,
                    now_ms,
                    commands,
                );
            }
            TabMessage::Shutdown { .. } => {
                self.handle_shutdown(&tab_id, now_ms, commands);
            }
            TabMessage::BrokerPong { .. } => {
                if let Some(tab) = self.tabs.get_mut(&tab_id) {
                    tab.last_pong_at = now_ms;
                }
            }
        }
    }

    fn handle_timer_fired(
        &mut self,
        timer: TimerKey,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        match timer {
            TimerKey::BrokerPing => {
                self.broker_ping_timer_running = false;
                self.send_broker_pings(now_ms, commands);
                if self.namespace.is_some() && !self.tabs.is_empty() {
                    self.start_broker_ping_timer(now_ms, commands);
                }
            }
            TimerKey::FollowerAttachment {
                leadership_id,
                follower_tab_id,
            } => {
                let key = AttachmentKey {
                    leadership_id,
                    follower_tab_id: follower_tab_id.clone(),
                };
                if !self.pending_follower_attachments.shift_remove(&key) {
                    return;
                }
                if !self
                    .leader
                    .as_ref()
                    .is_some_and(|leader| leader.ready && leader.leadership_id == leadership_id)
                {
                    return;
                }
                if !self.tabs.contains_key(&follower_tab_id) {
                    return;
                }
                let retry_count = self
                    .follower_attachment_retry_counts
                    .get(&key)
                    .copied()
                    .unwrap_or(0);
                self.follower_attachment_retry_counts
                    .insert(key, retry_count.saturating_add(1));
                if let Some(leader) = self.leader.clone() {
                    self.assign_follower_ports(&leader, commands);
                }
            }
            TimerKey::LeaderFailureRetry => {
                self.leader_failure_retry_timer_running = false;
                self.elect_if_needed(now_ms, commands);
            }
            TimerKey::ForceTakeoverSleep { probe_id } => {
                self.handle_force_takeover_sleep(probe_id, now_ms, commands);
            }
        }
    }

    fn handle_leader_ready(
        &mut self,
        input: LeaderReadyInput<'_>,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let LeaderReadyInput {
            tab_id,
            leadership_id,
            tab_lock_name,
            worker_lock_name,
            bridgeless_storage_reset,
            now_ms,
        } = input;
        if !self
            .leader
            .as_ref()
            .is_some_and(|leader| leader.tab_id == tab_id && leader.leadership_id == leadership_id)
        {
            if let Some(tab) = self.tabs.get(tab_id) {
                self.post(
                    tab.port_id,
                    ControlMessage::Demote {
                        broker_instance_id: self.broker_instance_id.clone(),
                        leadership_id,
                    },
                    commands,
                );
            }
            return;
        }

        let bridgeless_reset = self
            .reset_state
            .as_ref()
            .is_some_and(|reset| reset.promoted_leadership_id == Some(leadership_id))
            && bridgeless_storage_reset
            && self
                .tabs
                .get(tab_id)
                .and_then(|tab| tab.schema_fingerprint.as_ref())
                .is_none();

        if bridgeless_reset {
            self.clear_leader(
                leadership_id,
                ClearLeaderOptions {
                    demote_leader: true,
                    remove_leader_tab: false,
                },
                now_ms,
                commands,
            );
            if let Some(active_reset) = self.reset_state.clone() {
                self.finish_storage_reset(active_reset, true, None, now_ms, commands);
            }
            return;
        }

        if let Some(leader) = self.leader.as_mut() {
            leader.ready = true;
            leader.tab_lock_name = Some(tab_lock_name);
            leader.worker_lock_name = Some(worker_lock_name);
        }

        let Some(leader) = self.leader.clone() else {
            return;
        };
        self.announce_leader_ready(&leader, commands);
        self.start_leader_lock_monitors(&leader, commands);
        if self
            .reset_state
            .as_ref()
            .is_some_and(|reset| reset.promoted_leadership_id == Some(leadership_id))
        {
            if let Some(reset) = self.reset_state.as_mut() {
                reset.phase = ResetPhase::Reconnecting;
            }
            self.assign_follower_ports(&leader, commands);
            self.finish_storage_reset_if_reconnected(now_ms, commands);
            return;
        }
        self.assign_follower_ports(&leader, commands);
    }

    fn handle_leader_failed(
        &mut self,
        tab_id: &str,
        leadership_id: u64,
        reason: String,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        if self
            .reset_state
            .as_ref()
            .is_some_and(|reset| reset.promoted_leadership_id == Some(leadership_id))
        {
            if let Some(reset) = self.reset_state.as_mut() {
                reset.errors.push(reason);
            }
            self.remove_tab(
                tab_id,
                RemoveTabOptions {
                    close_port: false,
                    notify_leader: false,
                },
                now_ms,
                commands,
            );
            self.remove_tab_from_active_reset(tab_id, now_ms, commands);
            self.leader = None;
            if let Some(reset) = self.reset_state.as_mut() {
                reset.promoted_leadership_id = None;
            }
            self.promote_reset_leader(now_ms, commands);
            return;
        }

        if self
            .leader
            .as_ref()
            .is_some_and(|leader| leader.tab_id == tab_id && leader.leadership_id == leadership_id)
        {
            self.mark_leader_candidate_failed(tab_id, now_ms);
            let cleared = self.clear_leader(
                leadership_id,
                ClearLeaderOptions {
                    demote_leader: true,
                    remove_leader_tab: false,
                },
                now_ms,
                commands,
            );
            self.schedule_replacement_election(cleared, now_ms, commands);
        }
    }

    fn handle_shutdown(&mut self, tab_id: &str, now_ms: i64, commands: &mut Vec<BrokerCommand>) {
        if self
            .leader
            .as_ref()
            .is_some_and(|leader| leader.tab_id == tab_id)
        {
            let Some(leadership_id) = self.leader.as_ref().map(|leader| leader.leadership_id)
            else {
                return;
            };
            let active_reset = self.reset_state.clone();
            self.remove_tab(
                tab_id,
                RemoveTabOptions {
                    close_port: false,
                    notify_leader: false,
                },
                now_ms,
                commands,
            );
            let cleared = self.clear_leader(
                leadership_id,
                ClearLeaderOptions {
                    demote_leader: false,
                    remove_leader_tab: false,
                },
                now_ms,
                commands,
            );
            self.remove_tab_from_active_reset(tab_id, now_ms, commands);
            if active_reset.is_some_and(|reset| {
                reset.promoted_leadership_id == Some(leadership_id)
                    && reset.phase != ResetPhase::Preparing
            }) {
                if let Some(reset) = self.reset_state.as_mut() {
                    reset.promoted_leadership_id = None;
                }
                self.promote_reset_leader(now_ms, commands);
                self.reset_if_idle(commands);
                return;
            }
            self.schedule_replacement_election(cleared, now_ms, commands);
        } else {
            self.remove_tab(
                tab_id,
                RemoveTabOptions {
                    close_port: false,
                    notify_leader: true,
                },
                now_ms,
                commands,
            );
            self.remove_tab_from_active_reset(tab_id, now_ms, commands);
        }
        self.reset_if_idle(commands);
    }

    fn handle_locks_probe_result(
        &mut self,
        probe_id: ProbeId,
        all_acquired: bool,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let Some(pending) = self.pending_takeover.clone() else {
            return;
        };
        if pending.probe_id != probe_id || pending.stage != TakeoverStage::Probing {
            return;
        }
        if all_acquired {
            self.pending_takeover = None;
            self.finish_takeover(pending.purpose, now_ms, commands);
            return;
        }

        if let Some(pending_takeover) = self.pending_takeover.as_mut() {
            pending_takeover.stage = TakeoverStage::Sleeping;
        }
        commands.push(BrokerCommand::SetTimer {
            timer: TimerKey::ForceTakeoverSleep { probe_id },
            delay_ms: self.force_takeover_timeout_ms(),
        });
    }

    fn handle_force_takeover_sleep(
        &mut self,
        probe_id: ProbeId,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let Some(pending) = self.pending_takeover.clone() else {
            return;
        };
        if pending.probe_id != probe_id || pending.stage != TakeoverStage::Sleeping {
            return;
        }
        if !self.should_force_takeover(&pending.purpose) {
            self.pending_takeover = None;
            self.finish_takeover(pending.purpose, now_ms, commands);
            return;
        }
        if let Some(pending_takeover) = self.pending_takeover.as_mut() {
            pending_takeover.stage = TakeoverStage::Stealing;
        }
        commands.push(BrokerCommand::StealLocks {
            probe_id,
            lock_names: pending.lock_names,
        });
    }

    fn handle_locks_stolen(
        &mut self,
        probe_id: ProbeId,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let Some(pending) = self.pending_takeover.clone() else {
            return;
        };
        if pending.probe_id != probe_id || pending.stage != TakeoverStage::Stealing {
            return;
        }
        self.pending_takeover = None;
        self.finish_takeover(pending.purpose, now_ms, commands);
    }

    fn handle_lock_monitor_triggered(
        &mut self,
        monitor_id: MonitorId,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let Some(leadership_id) = self.leader.as_ref().and_then(|leader| {
            if leader.tab_lock_monitor == Some(monitor_id)
                || leader.worker_lock_monitor == Some(monitor_id)
            {
                Some(leader.leadership_id)
            } else {
                None
            }
        }) else {
            return;
        };
        self.handle_leader_lock_released(leadership_id, now_ms, commands);
    }

    fn update_visibility(&mut self, tab_id: &str, visibility: Visibility, now_ms: i64) {
        let Some(tab) = self.tabs.get_mut(tab_id) else {
            return;
        };
        tab.visibility = visibility;
        if visibility == Visibility::Visible {
            tab.last_visible_at = now_ms;
        }
    }

    fn elect_if_needed(&mut self, now_ms: i64, commands: &mut Vec<BrokerCommand>) {
        if self.reset_state.is_some() {
            return;
        }
        if self.replacement_election_in_flight {
            return;
        }
        if self.leader.as_ref().is_some_and(|leader| leader.ready) || self.tabs.is_empty() {
            return;
        }
        if self.leader.is_some()
            && self
                .namespace
                .as_ref()
                .and_then(|namespace| namespace.schema_fingerprint.as_ref())
                .is_none()
        {
            return;
        }

        let candidates = self.eligible_leader_candidates(now_ms);
        let Some(candidate) = select_leader_candidate(candidates.iter()) else {
            self.schedule_leader_failure_retry_election(now_ms, commands);
            return;
        };
        let candidate_tab_id = candidate.tab_id.clone();

        if let Some(current_leader) = self.leader.clone() {
            let current_leader_has_canonical_schema = self
                .tabs
                .get(&current_leader.tab_id)
                .and_then(|tab| tab.schema_fingerprint.as_ref())
                == self
                    .namespace
                    .as_ref()
                    .and_then(|namespace| namespace.schema_fingerprint.as_ref());
            if current_leader_has_canonical_schema {
                return;
            }
            if candidate_tab_id == current_leader.tab_id {
                return;
            }
            self.clear_leader(
                current_leader.leadership_id,
                ClearLeaderOptions {
                    demote_leader: true,
                    remove_leader_tab: false,
                },
                now_ms,
                commands,
            );
        }

        let Some(tab) = self.tabs.get(&candidate_tab_id).cloned() else {
            return;
        };
        let Some((tab_lock_name, worker_lock_name)) = self.current_leader_lock_names() else {
            return;
        };

        self.current_leadership_id = self.current_leadership_id.saturating_add(1);
        self.leader = Some(LeaderState {
            tab_id: tab.tab_id.clone(),
            leadership_id: self.current_leadership_id,
            ready: false,
            tab_lock_name: Some(tab_lock_name),
            worker_lock_name: Some(worker_lock_name),
            tab_lock_monitor: None,
            worker_lock_monitor: None,
        });

        self.post(
            tab.port_id,
            ControlMessage::BecomeLeader {
                broker_instance_id: self.broker_instance_id.clone(),
                leadership_id: self.current_leadership_id,
                reset_request_id: None,
            },
            commands,
        );
    }

    fn reset_if_idle(&mut self, commands: &mut Vec<BrokerCommand>) {
        if !self.tabs.is_empty() {
            return;
        }
        self.namespace = None;
        if let Some(leader) = self.leader.take() {
            self.cancel_leader_monitors(&leader, commands);
        }
        self.clear_all_follower_attachment_state(commands);
        self.reset_state = None;
        self.replacement_election_generation =
            self.replacement_election_generation.saturating_add(1);
        self.replacement_election_in_flight = false;
        if let Some(pending) = self.pending_takeover.take()
            && pending.stage == TakeoverStage::Sleeping
        {
            commands.push(BrokerCommand::ClearTimer {
                timer: TimerKey::ForceTakeoverSleep {
                    probe_id: pending.probe_id,
                },
            });
        }
        self.failed_leader_retry_after_by_tab_id.clear();
        self.stop_leader_failure_retry_timer(commands);
        self.stop_broker_ping_timer(commands);
    }

    fn remove_tab(
        &mut self,
        tab_id: &str,
        options: RemoveTabOptions,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) -> Option<TabState> {
        let tab = self.tabs.shift_remove(tab_id)?;

        if options.notify_leader
            && self
                .leader
                .as_ref()
                .is_some_and(|leader| leader.tab_id != tab_id)
        {
            self.notify_leader_to_detach_follower(tab_id, commands);
        }

        self.failed_leader_retry_after_by_tab_id.remove(tab_id);
        self.clear_follower_attachment_state(tab_id, commands);
        self.reelect_schema_fingerprint_if_unheld(&tab, now_ms, commands);
        if options.close_port {
            commands.push(BrokerCommand::ClosePort {
                port_id: tab.port_id,
            });
        }
        Some(tab)
    }

    fn notify_leader_to_detach_follower(
        &mut self,
        follower_tab_id: &str,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let Some(leader) = &self.leader else {
            return;
        };
        let Some(leader_tab) = self.tabs.get(&leader.tab_id) else {
            return;
        };
        self.post(
            leader_tab.port_id,
            ControlMessage::DetachFollowerPort {
                broker_instance_id: self.broker_instance_id.clone(),
                follower_tab_id: follower_tab_id.to_string(),
                leadership_id: leader.leadership_id,
            },
            commands,
        );
    }

    fn clear_follower_attachment_state(
        &mut self,
        follower_tab_id: &str,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let pending_keys: Vec<AttachmentKey> = self
            .pending_follower_attachments
            .iter()
            .filter(|key| key.follower_tab_id == follower_tab_id)
            .cloned()
            .collect();
        for key in pending_keys {
            self.clear_pending_follower_attachment(&key, commands);
        }

        let attached_keys: Vec<AttachmentKey> = self
            .attached_follower_ports
            .iter()
            .filter(|key| key.follower_tab_id == follower_tab_id)
            .cloned()
            .collect();
        for key in attached_keys {
            self.attached_follower_ports.shift_remove(&key);
        }
    }

    fn clear_follower_attachment_key(
        &mut self,
        follower_tab_id: &str,
        leadership_id: u64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let key = AttachmentKey {
            leadership_id,
            follower_tab_id: follower_tab_id.to_string(),
        };
        self.clear_pending_follower_attachment(&key, commands);
        self.attached_follower_ports.shift_remove(&key);
    }

    fn clear_all_follower_attachment_state(&mut self, commands: &mut Vec<BrokerCommand>) {
        let pending_keys: Vec<AttachmentKey> =
            self.pending_follower_attachments.iter().cloned().collect();
        for key in pending_keys {
            commands.push(BrokerCommand::ClearTimer {
                timer: TimerKey::FollowerAttachment {
                    leadership_id: key.leadership_id,
                    follower_tab_id: key.follower_tab_id,
                },
            });
        }
        self.pending_follower_attachments.clear();
        self.follower_attachment_retry_counts.clear();
        self.attached_follower_ports.clear();
    }

    fn clear_pending_follower_attachment(
        &mut self,
        key: &AttachmentKey,
        commands: &mut Vec<BrokerCommand>,
    ) {
        if self.pending_follower_attachments.shift_remove(key) {
            commands.push(BrokerCommand::ClearTimer {
                timer: TimerKey::FollowerAttachment {
                    leadership_id: key.leadership_id,
                    follower_tab_id: key.follower_tab_id.clone(),
                },
            });
        }
        self.follower_attachment_retry_counts.remove(key);
    }

    fn handle_schema_ready(
        &mut self,
        tab_id: &str,
        schema_fingerprint: String,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let Some(namespace) = self.namespace.as_mut() else {
            return;
        };
        let Some(tab) = self.tabs.get_mut(tab_id) else {
            return;
        };

        if namespace.schema_fingerprint.is_none() {
            namespace.schema_fingerprint = Some(schema_fingerprint.clone());
        }

        tab.schema_fingerprint = Some(schema_fingerprint.clone());

        if namespace.schema_fingerprint.as_ref() != Some(&schema_fingerprint) {
            self.block_tab_for_schema_mismatch(tab_id, now_ms, commands);
            return;
        }

        if let Some(leader) = self.leader.clone().filter(|leader| leader.ready) {
            self.assign_follower_ports(&leader, commands);
            return;
        }
        self.elect_if_needed(now_ms, commands);
    }

    fn block_tab_for_schema_mismatch(
        &mut self,
        tab_id: &str,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        if let Some(tab) = self.tabs.get(tab_id) {
            self.post(
                tab.port_id,
                ControlMessage::SchemaBlocked {
                    broker_instance_id: self.broker_instance_id.clone(),
                    reason: "incompatible persistent browser schema".to_string(),
                },
                commands,
            );
        }

        if self
            .leader
            .as_ref()
            .is_some_and(|leader| leader.tab_id == tab_id)
        {
            let leadership_id = self.leader.as_ref().map(|leader| leader.leadership_id);
            if let Some(leadership_id) = leadership_id {
                let cleared = self.clear_leader(
                    leadership_id,
                    ClearLeaderOptions {
                        demote_leader: true,
                        remove_leader_tab: false,
                    },
                    now_ms,
                    commands,
                );
                self.schedule_replacement_election(cleared, now_ms, commands);
            }
        }
    }

    fn reelect_schema_fingerprint_if_unheld(
        &mut self,
        departed: &TabState,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let Some(current_schema) = self
            .namespace
            .as_ref()
            .and_then(|namespace| namespace.schema_fingerprint.clone())
        else {
            return;
        };
        if departed.schema_fingerprint.as_ref() != Some(&current_schema) {
            return;
        }
        for tab in self.tabs.values() {
            if tab.schema_fingerprint.as_ref() == Some(&current_schema) {
                return;
            }
        }

        let next = self
            .tabs
            .values()
            .find_map(|tab| tab.schema_fingerprint.clone());
        if let Some(namespace) = self.namespace.as_mut() {
            namespace.schema_fingerprint = next.clone();
        }
        if next.is_none() {
            return;
        }

        if let Some(leader) = self.leader.clone().filter(|leader| leader.ready) {
            self.assign_follower_ports(&leader, commands);
        } else {
            self.elect_if_needed(now_ms, commands);
        }
    }

    fn announce_leader_ready(
        &mut self,
        next_leader: &LeaderState,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let posts: Vec<(PortId, ControlMessage)> = self
            .tabs
            .values()
            .map(|tab| {
                (
                    tab.port_id,
                    ControlMessage::LeaderReady {
                        broker_instance_id: self.broker_instance_id.clone(),
                        leader_tab_id: next_leader.tab_id.clone(),
                        leadership_id: next_leader.leadership_id,
                    },
                )
            })
            .collect();
        for (port_id, message) in posts {
            self.post(port_id, message, commands);
        }
    }

    fn start_leader_lock_monitors(
        &mut self,
        next_leader: &LeaderState,
        commands: &mut Vec<BrokerCommand>,
    ) {
        self.cancel_leader_monitors(next_leader, commands);
        let Some(tab_lock_name) = next_leader.tab_lock_name.clone() else {
            return;
        };
        let Some(worker_lock_name) = next_leader.worker_lock_name.clone() else {
            return;
        };

        let tab_lock_monitor = self.next_monitor_id();
        let worker_lock_monitor = self.next_monitor_id();
        if let Some(leader) = self
            .leader
            .as_mut()
            .filter(|leader| leader.leadership_id == next_leader.leadership_id)
        {
            leader.tab_lock_monitor = Some(tab_lock_monitor);
            leader.worker_lock_monitor = Some(worker_lock_monitor);
        }
        commands.push(BrokerCommand::MonitorLock {
            monitor_id: tab_lock_monitor,
            lock_name: tab_lock_name,
        });
        commands.push(BrokerCommand::MonitorLock {
            monitor_id: worker_lock_monitor,
            lock_name: worker_lock_name,
        });
    }

    fn handle_leader_lock_released(
        &mut self,
        leadership_id: u64,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        if self
            .leader
            .as_ref()
            .is_none_or(|leader| leader.leadership_id != leadership_id)
        {
            return;
        }
        let cleared = self.clear_leader(
            leadership_id,
            ClearLeaderOptions {
                demote_leader: true,
                remove_leader_tab: true,
            },
            now_ms,
            commands,
        );
        self.schedule_replacement_election(cleared, now_ms, commands);
    }

    fn clear_leader(
        &mut self,
        leadership_id: u64,
        options: ClearLeaderOptions,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) -> Option<ClearedLeaderState> {
        let current = self.leader.clone()?;
        if current.leadership_id != leadership_id {
            return None;
        }
        self.cancel_leader_monitors(&current, commands);
        self.clear_all_follower_attachment_state(commands);

        if options.demote_leader
            && let Some(leader_tab) = self.tabs.get(&current.tab_id)
        {
            self.post(
                leader_tab.port_id,
                ControlMessage::Demote {
                    broker_instance_id: self.broker_instance_id.clone(),
                    leadership_id,
                },
                commands,
            );
        }

        let close_posts: Vec<PortId> = self
            .tabs
            .values()
            .filter(|tab| tab.tab_id != current.tab_id)
            .map(|tab| tab.port_id)
            .collect();
        for port_id in close_posts {
            self.post(
                port_id,
                ControlMessage::CloseFollowerPort {
                    broker_instance_id: self.broker_instance_id.clone(),
                    leadership_id,
                },
                commands,
            );
        }

        if options.remove_leader_tab {
            self.remove_tab(
                &current.tab_id,
                RemoveTabOptions {
                    close_port: false,
                    notify_leader: false,
                },
                now_ms,
                commands,
            );
        }
        self.leader = None;
        Some(ClearedLeaderState {
            leadership_id: current.leadership_id,
            tab_lock_name: current.tab_lock_name,
            worker_lock_name: current.worker_lock_name,
        })
    }

    fn cancel_leader_monitors(&mut self, current: &LeaderState, commands: &mut Vec<BrokerCommand>) {
        if let Some(monitor_id) = current.tab_lock_monitor {
            commands.push(BrokerCommand::CancelLockMonitor { monitor_id });
        }
        if let Some(monitor_id) = current.worker_lock_monitor {
            commands.push(BrokerCommand::CancelLockMonitor { monitor_id });
        }
        if let Some(leader) = self
            .leader
            .as_mut()
            .filter(|leader| leader.leadership_id == current.leadership_id)
        {
            leader.tab_lock_monitor = None;
            leader.worker_lock_monitor = None;
        }
    }

    fn start_storage_reset(
        &mut self,
        requesting_tab_id: &str,
        request_id: String,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        if let Some(reset) = self.reset_state.as_mut() {
            reset.request_ids.insert(request_id.clone());
            if let Some(tab) = self.tabs.get(requesting_tab_id) {
                self.post(
                    tab.port_id,
                    ControlMessage::StorageResetStarted {
                        broker_instance_id: self.broker_instance_id.clone(),
                        request_id,
                    },
                    commands,
                );
            }
            return;
        }

        let previous_leader = match self.leader.as_ref().map(|leader| leader.leadership_id) {
            Some(leadership_id) => self.clear_leader(
                leadership_id,
                ClearLeaderOptions {
                    demote_leader: false,
                    remove_leader_tab: false,
                },
                now_ms,
                commands,
            ),
            None => None,
        };

        let leadership_id = previous_leader
            .as_ref()
            .map(|leader| leader.leadership_id)
            .unwrap_or(self.current_leadership_id);
        let mut request_ids = IndexSet::new();
        request_ids.insert(request_id.clone());
        self.reset_state = Some(ResetState {
            request_id: request_id.clone(),
            request_ids,
            participants: self.tabs.keys().cloned().collect(),
            prepared_tabs: IndexSet::new(),
            errors: Vec::new(),
            previous_leader,
            promoted_leadership_id: None,
            phase: ResetPhase::Preparing,
        });

        if let Some(requesting_tab) = self.tabs.get(requesting_tab_id) {
            self.post(
                requesting_tab.port_id,
                ControlMessage::StorageResetStarted {
                    broker_instance_id: self.broker_instance_id.clone(),
                    request_id: request_id.clone(),
                },
                commands,
            );
        }

        let posts: Vec<PortId> = self.tabs.values().map(|tab| tab.port_id).collect();
        for port_id in posts {
            self.post(
                port_id,
                ControlMessage::StorageResetBegin {
                    broker_instance_id: self.broker_instance_id.clone(),
                    request_id: request_id.clone(),
                    leadership_id,
                },
                commands,
            );
        }

        self.continue_storage_reset_if_ready(now_ms, commands);
    }

    fn add_tab_to_active_reset(&mut self, tab_id: &str, commands: &mut Vec<BrokerCommand>) {
        let Some(tab) = self.tabs.get(tab_id) else {
            return;
        };
        let port_id = tab.port_id;
        let Some((request_id, leadership_id)) =
            self.reset_state.as_mut().and_then(|active_reset| {
                if active_reset.phase != ResetPhase::Preparing {
                    return None;
                }
                active_reset.participants.insert(tab_id.to_string());
                Some((
                    active_reset.request_id.clone(),
                    active_reset
                        .previous_leader
                        .as_ref()
                        .map(|leader| leader.leadership_id)
                        .unwrap_or(self.current_leadership_id),
                ))
            })
        else {
            return;
        };
        self.post(
            port_id,
            ControlMessage::StorageResetBegin {
                broker_instance_id: self.broker_instance_id.clone(),
                request_id,
                leadership_id,
            },
            commands,
        );
    }

    fn handle_storage_reset_ready(
        &mut self,
        tab_id: &str,
        request_id: &str,
        success: bool,
        error_message: Option<String>,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let Some(active_reset) = self.reset_state.as_mut() else {
            return;
        };
        if active_reset.request_id != request_id || active_reset.phase != ResetPhase::Preparing {
            return;
        }
        if !active_reset.participants.contains(tab_id) {
            return;
        }
        if !success {
            active_reset.errors.push(
                error_message
                    .unwrap_or_else(|| format!("Tab {tab_id} failed to prepare storage reset")),
            );
        }
        active_reset.prepared_tabs.insert(tab_id.to_string());
        self.continue_storage_reset_if_ready(now_ms, commands);
    }

    fn continue_storage_reset_if_ready(&mut self, now_ms: i64, commands: &mut Vec<BrokerCommand>) {
        let Some(active_reset) = self.reset_state.as_ref() else {
            return;
        };
        if active_reset.phase != ResetPhase::Preparing {
            return;
        }
        for participant in &active_reset.participants {
            if !active_reset.prepared_tabs.contains(participant) {
                return;
            }
        }

        let previous_leader = active_reset.previous_leader.clone();
        let request_id = active_reset.request_id.clone();
        if let Some(reset) = self.reset_state.as_mut() {
            reset.phase = ResetPhase::Promoting;
        }
        self.start_takeover(
            previous_leader,
            TakeoverPurpose::StorageReset { request_id },
            now_ms,
            commands,
        );
    }

    fn promote_reset_leader(&mut self, now_ms: i64, commands: &mut Vec<BrokerCommand>) {
        let Some(active_reset) = self.reset_state.clone() else {
            return;
        };
        if self
            .reset_state
            .as_ref()
            .is_none_or(|reset| reset.request_id != active_reset.request_id)
        {
            return;
        }
        let candidates = self.eligible_leader_candidates(now_ms);
        let Some(candidate) = select_leader_candidate(candidates.iter()) else {
            self.finish_storage_reset(
                active_reset,
                false,
                Some("No connected tab is available to reset storage".to_string()),
                now_ms,
                commands,
            );
            return;
        };

        let Some(tab) = self.tabs.get(&candidate.tab_id).cloned() else {
            self.finish_storage_reset(
                active_reset,
                false,
                Some("No connected tab is available to reset storage".to_string()),
                now_ms,
                commands,
            );
            return;
        };
        let Some((tab_lock_name, worker_lock_name)) = self.current_leader_lock_names() else {
            self.finish_storage_reset(
                active_reset,
                false,
                Some("No connected tab is available to reset storage".to_string()),
                now_ms,
                commands,
            );
            return;
        };

        self.current_leadership_id = self.current_leadership_id.saturating_add(1);
        if let Some(reset) = self.reset_state.as_mut() {
            reset.promoted_leadership_id = Some(self.current_leadership_id);
        }
        self.leader = Some(LeaderState {
            tab_id: tab.tab_id.clone(),
            leadership_id: self.current_leadership_id,
            ready: false,
            tab_lock_name: Some(tab_lock_name),
            worker_lock_name: Some(worker_lock_name),
            tab_lock_monitor: None,
            worker_lock_monitor: None,
        });

        self.post(
            tab.port_id,
            ControlMessage::BecomeLeader {
                broker_instance_id: self.broker_instance_id.clone(),
                leadership_id: self.current_leadership_id,
                reset_request_id: Some(active_reset.request_id),
            },
            commands,
        );
    }

    fn finish_storage_reset(
        &mut self,
        completed_reset: ResetState,
        success: bool,
        error_message: Option<String>,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        if self
            .reset_state
            .as_ref()
            .is_some_and(|reset| reset.request_id == completed_reset.request_id)
        {
            self.reset_state = None;
        }

        let outcomes = self.remember_storage_reset_outcomes(
            completed_reset.request_ids,
            success,
            error_message,
            now_ms,
        );
        let ports: Vec<PortId> = self.tabs.values().map(|tab| tab.port_id).collect();
        for port_id in ports {
            for outcome in &outcomes {
                self.post_storage_reset_outcome(port_id, outcome, commands);
            }
        }

        if !success {
            self.elect_if_needed(now_ms, commands);
        }
    }

    fn remember_storage_reset_outcomes(
        &mut self,
        request_ids: IndexSet<String>,
        success: bool,
        error_message: Option<String>,
        now_ms: i64,
    ) -> Vec<StorageResetOutcome> {
        // The JS spread-omits falsy error messages, so an empty string is
        // remembered (and rebroadcast) as "no error message".
        let error_message = error_message.filter(|message| !message.is_empty());
        let mut outcomes = Vec::new();
        for request_id in request_ids {
            let outcome = StorageResetOutcome {
                request_id: request_id.clone(),
                success,
                error_message: error_message.clone(),
                finished_at: now_ms,
            };
            // Delete first: re-setting an existing key keeps its Map position,
            // which would make size eviction treat a re-finished id as oldest.
            self.completed_storage_reset_outcomes
                .shift_remove(&request_id);
            self.completed_storage_reset_outcomes
                .insert(request_id, outcome.clone());
            outcomes.push(outcome);
        }
        self.prune_completed_storage_reset_outcomes(now_ms);
        outcomes
    }

    fn redeliver_finished_storage_resets(
        &mut self,
        port_id: PortId,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        self.prune_completed_storage_reset_outcomes(now_ms);
        let outcomes: Vec<StorageResetOutcome> = self
            .completed_storage_reset_outcomes
            .values()
            .cloned()
            .collect();
        for outcome in outcomes {
            self.post_storage_reset_outcome(port_id, &outcome, commands);
        }
    }

    fn post_storage_reset_outcome(
        &mut self,
        port_id: PortId,
        outcome: &StorageResetOutcome,
        commands: &mut Vec<BrokerCommand>,
    ) {
        self.post(
            port_id,
            ControlMessage::StorageResetFinished {
                broker_instance_id: self.broker_instance_id.clone(),
                request_id: outcome.request_id.clone(),
                success: outcome.success,
                error_message: outcome.error_message.clone(),
            },
            commands,
        );
    }

    fn prune_completed_storage_reset_outcomes(&mut self, now_ms: i64) {
        let expired: Vec<String> = self
            .completed_storage_reset_outcomes
            .iter()
            .filter_map(|(request_id, outcome)| {
                if now_ms - outcome.finished_at > COMPLETED_STORAGE_RESET_OUTCOME_TTL_MS {
                    Some(request_id.clone())
                } else {
                    None
                }
            })
            .collect();
        for request_id in expired {
            self.completed_storage_reset_outcomes
                .shift_remove(&request_id);
        }

        while self.completed_storage_reset_outcomes.len() > MAX_COMPLETED_STORAGE_RESET_OUTCOMES {
            let Some(oldest_request_id) =
                self.completed_storage_reset_outcomes.keys().next().cloned()
            else {
                return;
            };
            self.completed_storage_reset_outcomes
                .shift_remove(&oldest_request_id);
        }
    }

    fn finish_storage_reset_if_reconnected(
        &mut self,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let Some(active_reset) = self.reset_state.clone() else {
            return;
        };
        if active_reset.phase != ResetPhase::Reconnecting {
            return;
        }
        let Some(leader) = self.leader.clone().filter(|leader| {
            leader.ready && active_reset.promoted_leadership_id == Some(leader.leadership_id)
        }) else {
            return;
        };

        for tab_id in &active_reset.participants {
            let Some(tab) = self.tabs.get(tab_id) else {
                continue;
            };
            if !self.should_assign_follower_port(tab, &leader) {
                continue;
            }
            let key = AttachmentKey {
                leadership_id: leader.leadership_id,
                follower_tab_id: tab_id.clone(),
            };
            if !self.attached_follower_ports.contains(&key) {
                return;
            }
        }

        self.finish_storage_reset(active_reset, true, None, now_ms, commands);
    }

    fn schedule_replacement_election(
        &mut self,
        previous_leader: Option<ClearedLeaderState>,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        if self.replacement_election_in_flight {
            return;
        }
        self.replacement_election_in_flight = true;
        self.replacement_election_generation =
            self.replacement_election_generation.saturating_add(1);
        let generation = self.replacement_election_generation;
        self.start_takeover(
            previous_leader,
            TakeoverPurpose::ReplacementElection { generation },
            now_ms,
            commands,
        );
    }

    fn start_takeover(
        &mut self,
        previous_leader: Option<ClearedLeaderState>,
        purpose: TakeoverPurpose,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let lock_names = match previous_leader {
            Some(previous_leader) => match (
                previous_leader.tab_lock_name,
                previous_leader.worker_lock_name,
            ) {
                (Some(tab_lock_name), Some(worker_lock_name)) => {
                    vec![tab_lock_name, worker_lock_name]
                }
                _ => {
                    self.finish_takeover(purpose, now_ms, commands);
                    return;
                }
            },
            None => {
                self.finish_takeover(purpose, now_ms, commands);
                return;
            }
        };

        if !self.should_force_takeover(&purpose) {
            self.finish_takeover(purpose, now_ms, commands);
            return;
        }

        let probe_id = self.next_probe_id();
        self.pending_takeover = Some(PendingTakeover {
            probe_id,
            purpose,
            lock_names: lock_names.clone(),
            stage: TakeoverStage::Probing,
        });
        commands.push(BrokerCommand::ProbeLocks {
            probe_id,
            lock_names,
        });
    }

    fn should_force_takeover(&self, purpose: &TakeoverPurpose) -> bool {
        match purpose {
            TakeoverPurpose::ReplacementElection { generation } => {
                self.replacement_election_in_flight
                    && self.replacement_election_generation == *generation
                    && self.leader.is_none()
            }
            TakeoverPurpose::StorageReset { request_id } => self
                .reset_state
                .as_ref()
                .is_some_and(|reset| reset.request_id == *request_id),
        }
    }

    fn finish_takeover(
        &mut self,
        purpose: TakeoverPurpose,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        match purpose {
            TakeoverPurpose::ReplacementElection { generation } => {
                if self.replacement_election_generation == generation {
                    self.replacement_election_in_flight = false;
                }
                if self.replacement_election_generation != generation {
                    return;
                }
                self.elect_if_needed(now_ms, commands);
            }
            TakeoverPurpose::StorageReset { request_id } => {
                let Some(active_reset) = self
                    .reset_state
                    .clone()
                    .filter(|reset| reset.request_id == request_id)
                else {
                    return;
                };
                if !active_reset.errors.is_empty() {
                    self.finish_storage_reset(
                        active_reset.clone(),
                        false,
                        Some(active_reset.errors.join("; ")),
                        now_ms,
                        commands,
                    );
                    return;
                }
                self.promote_reset_leader(now_ms, commands);
            }
        }
    }

    fn start_broker_ping_timer(&mut self, now_ms: i64, commands: &mut Vec<BrokerCommand>) {
        let Some(namespace) = &self.namespace else {
            return;
        };
        if self.broker_ping_timer_running {
            return;
        }
        let delay_ms = namespace.broker_ping_interval_ms;
        self.send_broker_pings(now_ms, commands);
        self.broker_ping_timer_running = true;
        commands.push(BrokerCommand::SetTimer {
            timer: TimerKey::BrokerPing,
            delay_ms,
        });
    }

    fn stop_broker_ping_timer(&mut self, commands: &mut Vec<BrokerCommand>) {
        if !self.broker_ping_timer_running {
            return;
        }
        self.broker_ping_timer_running = false;
        commands.push(BrokerCommand::ClearTimer {
            timer: TimerKey::BrokerPing,
        });
    }

    fn send_broker_pings(&mut self, now_ms: i64, commands: &mut Vec<BrokerCommand>) {
        if self.namespace.is_none() {
            return;
        }
        let tab_snapshot: Vec<TabState> = self.tabs.values().cloned().collect();
        for tab in tab_snapshot {
            if self.is_broker_pong_timed_out(&tab, now_ms) {
                self.evict_tab(&tab.tab_id, now_ms, commands);
                continue;
            }
            self.post(
                tab.port_id,
                ControlMessage::BrokerPing {
                    broker_instance_id: self.broker_instance_id.clone(),
                },
                commands,
            );
        }
    }

    fn evict_stale_tabs(&mut self, now_ms: i64, commands: &mut Vec<BrokerCommand>) {
        if self.namespace.is_none() {
            return;
        }
        let tab_snapshot: Vec<TabState> = self.tabs.values().cloned().collect();
        for tab in tab_snapshot {
            if self.is_broker_pong_timed_out(&tab, now_ms) {
                self.evict_tab(&tab.tab_id, now_ms, commands);
            }
        }
    }

    fn is_broker_pong_timed_out(&self, tab: &TabState, now_ms: i64) -> bool {
        self.namespace.as_ref().is_some_and(|namespace| {
            now_ms - tab.last_pong_at > namespace.broker_pong_timeout_ms as i64
        })
    }

    fn evict_tab(&mut self, tab_id: &str, now_ms: i64, commands: &mut Vec<BrokerCommand>) {
        if !self.tabs.contains_key(tab_id) {
            return;
        }
        self.remove_tab(
            tab_id,
            RemoveTabOptions {
                close_port: true,
                notify_leader: true,
            },
            now_ms,
            commands,
        );

        if self
            .leader
            .as_ref()
            .is_some_and(|leader| leader.tab_id == tab_id)
        {
            let Some(leadership_id) = self.leader.as_ref().map(|leader| leader.leadership_id)
            else {
                return;
            };
            let active_reset = self.reset_state.clone();
            let cleared = self.clear_leader(
                leadership_id,
                ClearLeaderOptions {
                    demote_leader: false,
                    remove_leader_tab: false,
                },
                now_ms,
                commands,
            );
            self.remove_tab_from_active_reset(tab_id, now_ms, commands);
            if active_reset.is_some_and(|reset| {
                reset.promoted_leadership_id == Some(leadership_id)
                    && reset.phase != ResetPhase::Preparing
            }) {
                if let Some(reset) = self.reset_state.as_mut() {
                    reset.promoted_leadership_id = None;
                }
                self.promote_reset_leader(now_ms, commands);
                self.reset_if_idle(commands);
                return;
            }
            self.schedule_replacement_election(cleared, now_ms, commands);
        } else {
            self.remove_tab_from_active_reset(tab_id, now_ms, commands);
        }
        self.reset_if_idle(commands);
    }

    fn assign_follower_ports(
        &mut self,
        next_leader: &LeaderState,
        commands: &mut Vec<BrokerCommand>,
    ) {
        if self
            .reset_state
            .as_ref()
            .is_some_and(|reset| reset.phase != ResetPhase::Reconnecting)
        {
            return;
        }
        let Some(leader_tab) = self.tabs.get(&next_leader.tab_id).cloned() else {
            return;
        };

        let followers: Vec<TabState> = self.tabs.values().cloned().collect();
        for follower in followers {
            if !self.should_assign_follower_port(&follower, next_leader) {
                continue;
            }
            let key = AttachmentKey {
                leadership_id: next_leader.leadership_id,
                follower_tab_id: follower.tab_id.clone(),
            };
            if self.pending_follower_attachments.contains(&key) {
                continue;
            }
            if self.attached_follower_ports.contains(&key) {
                continue;
            }

            self.mark_follower_port_pending(&follower.tab_id, next_leader.leadership_id, commands);
            commands.push(BrokerCommand::AttachFollowerChannel {
                leader_port_id: leader_tab.port_id,
                follower_port_id: follower.port_id,
                leader_tab_id: next_leader.tab_id.clone(),
                follower_tab_id: follower.tab_id,
                leadership_id: next_leader.leadership_id,
            });
        }
    }

    fn mark_follower_port_pending(
        &mut self,
        follower_tab_id: &str,
        leadership_id: u64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let key = AttachmentKey {
            leadership_id,
            follower_tab_id: follower_tab_id.to_string(),
        };
        self.pending_follower_attachments.insert(key.clone());
        let retry_count = self
            .follower_attachment_retry_counts
            .get(&key)
            .copied()
            .unwrap_or(0);
        let timeout_ms = INITIAL_FOLLOWER_ATTACHMENT_TIMEOUT_MS
            .saturating_mul(2_u64.saturating_pow(retry_count))
            .min(MAX_FOLLOWER_ATTACHMENT_TIMEOUT_MS);
        commands.push(BrokerCommand::SetTimer {
            timer: TimerKey::FollowerAttachment {
                leadership_id,
                follower_tab_id: follower_tab_id.to_string(),
            },
            delay_ms: timeout_ms,
        });
    }

    fn eligible_leader_candidates(&mut self, now_ms: i64) -> Vec<crate::protocol::Candidate> {
        let schema_fingerprint = self
            .namespace
            .as_ref()
            .and_then(|namespace| namespace.schema_fingerprint.clone());
        let mut candidates = Vec::new();
        let tab_ids: Vec<String> = self.tabs.keys().cloned().collect();
        for tab_id in tab_ids {
            if self.is_leader_candidate_in_failure_backoff(&tab_id, now_ms) {
                continue;
            }
            let Some(tab) = self.tabs.get(&tab_id) else {
                continue;
            };
            if schema_fingerprint
                .as_ref()
                .is_none_or(|schema| tab.schema_fingerprint.as_ref() == Some(schema))
            {
                candidates.push(crate::protocol::Candidate {
                    tab_id: tab.tab_id.clone(),
                    visibility: tab.visibility,
                    last_visible_at: tab.last_visible_at,
                });
            }
        }
        candidates
    }

    fn mark_leader_candidate_failed(&mut self, tab_id: &str, now_ms: i64) {
        self.failed_leader_retry_after_by_tab_id
            .insert(tab_id.to_string(), now_ms + LEADER_FAILURE_RETRY_BACKOFF_MS);
    }

    fn is_leader_candidate_in_failure_backoff(&mut self, tab_id: &str, now_ms: i64) -> bool {
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

    fn schedule_leader_failure_retry_election(
        &mut self,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        if self.leader_failure_retry_timer_running
            || self.reset_state.is_some()
            || self.replacement_election_in_flight
            || self.leader.as_ref().is_some_and(|leader| leader.ready)
        {
            return;
        }

        let mut retry_at: Option<i64> = None;
        let entries: Vec<(String, i64)> = self
            .failed_leader_retry_after_by_tab_id
            .iter()
            .map(|(tab_id, retry_at)| (tab_id.clone(), *retry_at))
            .collect();
        for (tab_id, candidate_retry_at) in entries {
            if !self.tabs.contains_key(&tab_id) {
                self.failed_leader_retry_after_by_tab_id.remove(&tab_id);
                continue;
            }
            retry_at = Some(
                retry_at
                    .map(|current| current.min(candidate_retry_at))
                    .unwrap_or(candidate_retry_at),
            );
        }
        let Some(retry_at) = retry_at else {
            return;
        };
        self.leader_failure_retry_timer_running = true;
        commands.push(BrokerCommand::SetTimer {
            timer: TimerKey::LeaderFailureRetry,
            delay_ms: retry_at.saturating_sub(now_ms) as u64,
        });
    }

    fn stop_leader_failure_retry_timer(&mut self, commands: &mut Vec<BrokerCommand>) {
        if !self.leader_failure_retry_timer_running {
            return;
        }
        self.leader_failure_retry_timer_running = false;
        commands.push(BrokerCommand::ClearTimer {
            timer: TimerKey::LeaderFailureRetry,
        });
    }

    fn current_leader_lock_names(&self) -> Option<(String, String)> {
        let namespace = self.namespace.as_ref()?;
        Some((
            format!("jazz-leader-tab:{}:{}", namespace.app_id, namespace.db_name),
            format!(
                "jazz-leader-worker:{}:{}",
                namespace.app_id, namespace.db_name
            ),
        ))
    }

    fn should_assign_follower_port(&self, tab: &TabState, next_leader: &LeaderState) -> bool {
        if tab.tab_id == next_leader.tab_id {
            return false;
        }
        self.namespace
            .as_ref()
            .and_then(|namespace| namespace.schema_fingerprint.as_ref())
            .is_none_or(|schema| tab.schema_fingerprint.as_ref() == Some(schema))
    }

    fn remove_tab_from_active_reset(
        &mut self,
        tab_id: &str,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let Some(active_reset) = self.reset_state.as_mut() else {
            return;
        };
        active_reset.participants.shift_remove(tab_id);
        active_reset.prepared_tabs.shift_remove(tab_id);
        self.continue_storage_reset_if_ready(now_ms, commands);
    }

    fn mark_follower_port_attached(
        &mut self,
        follower_tab_id: &str,
        leadership_id: u64,
        now_ms: i64,
        commands: &mut Vec<BrokerCommand>,
    ) {
        let key = AttachmentKey {
            leadership_id,
            follower_tab_id: follower_tab_id.to_string(),
        };
        if !self.pending_follower_attachments.contains(&key) {
            return;
        }
        self.clear_pending_follower_attachment(&key, commands);

        let Some(leader) = self
            .leader
            .clone()
            .filter(|leader| leader.leadership_id == leadership_id)
        else {
            return;
        };
        let Some(follower) = self.tabs.get(follower_tab_id) else {
            return;
        };
        self.attached_follower_ports.insert(key);
        self.post(
            follower.port_id,
            ControlMessage::FollowerReady {
                broker_instance_id: self.broker_instance_id.clone(),
                leader_tab_id: leader.tab_id,
                leadership_id,
            },
            commands,
        );
        if self.reset_state.as_ref().is_some_and(|reset| {
            reset.phase == ResetPhase::Reconnecting
                && reset.promoted_leadership_id == Some(leadership_id)
        }) {
            self.finish_storage_reset_if_reconnected(now_ms, commands);
        }
    }

    fn post(
        &mut self,
        port_id: PortId,
        message: ControlMessage,
        commands: &mut Vec<BrokerCommand>,
    ) {
        commands.push(BrokerCommand::Post { port_id, message });
    }

    fn force_takeover_timeout_ms(&self) -> u64 {
        self.namespace
            .as_ref()
            .map(|namespace| namespace.force_takeover_timeout_ms)
            .unwrap_or(DEFAULT_FORCE_TAKEOVER_TIMEOUT_MS)
    }

    fn next_probe_id(&mut self) -> ProbeId {
        self.next_probe_id = self.next_probe_id.saturating_add(1);
        ProbeId(self.next_probe_id)
    }

    fn next_monitor_id(&mut self) -> MonitorId {
        self.next_monitor_id = self.next_monitor_id.saturating_add(1);
        MonitorId(self.next_monitor_id)
    }
}

#[derive(Debug, Clone, Copy)]
struct ClearLeaderOptions {
    demote_leader: bool,
    remove_leader_tab: bool,
}

#[derive(Debug, Clone, Copy)]
struct RemoveTabOptions {
    close_port: bool,
    notify_leader: bool,
}

struct LeaderReadyInput<'a> {
    tab_id: &'a str,
    leadership_id: u64,
    tab_lock_name: String,
    worker_lock_name: String,
    bridgeless_storage_reset: bool,
    now_ms: i64,
}

fn broker_instance_stamp(message: &TabMessage) -> Option<(String, String)> {
    match message {
        TabMessage::Hello { .. } | TabMessage::Unknown => None,
        TabMessage::Visibility {
            broker_instance_id, ..
        } => Some((broker_instance_id.clone(), "visibility".to_string())),
        TabMessage::LeaderReady {
            broker_instance_id, ..
        } => Some((broker_instance_id.clone(), "leader-ready".to_string())),
        TabMessage::LeaderFailed {
            broker_instance_id, ..
        } => Some((broker_instance_id.clone(), "leader-failed".to_string())),
        TabMessage::FollowerPortAttached {
            broker_instance_id, ..
        } => Some((
            broker_instance_id.clone(),
            "follower-port-attached".to_string(),
        )),
        TabMessage::FollowerPortClosed {
            broker_instance_id, ..
        } => Some((
            broker_instance_id.clone(),
            "follower-port-closed".to_string(),
        )),
        TabMessage::SchemaReady {
            broker_instance_id, ..
        } => Some((broker_instance_id.clone(), "schema-ready".to_string())),
        TabMessage::StorageResetRequest {
            broker_instance_id, ..
        } => Some((
            broker_instance_id.clone(),
            "storage-reset-request".to_string(),
        )),
        TabMessage::StorageResetReady {
            broker_instance_id, ..
        } => Some((
            broker_instance_id.clone(),
            "storage-reset-ready".to_string(),
        )),
        TabMessage::Shutdown { broker_instance_id } => {
            Some((broker_instance_id.clone(), "shutdown".to_string()))
        }
        TabMessage::BrokerPong { broker_instance_id } => {
            Some((broker_instance_id.clone(), "broker-pong".to_string()))
        }
    }
}
