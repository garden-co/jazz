use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

pub use crate::broker_defaults::BrokerVisibility;
use crate::broker_defaults::{
    DEFAULT_BROKER_HELLO_TIMEOUT_MS, DEFAULT_BROKER_PING_INTERVAL_MS,
    DEFAULT_BROKER_PONG_TIMEOUT_MS, DEFAULT_INITIAL_LEADERSHIP_TIMEOUT_MS,
    DEFAULT_STORAGE_RESET_TIMEOUT_MS, RESET_RECONNECT_ERROR,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BrokerRole {
    Leader,
    Follower,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectRequested {
    pub app_id: String,
    pub db_name: String,
    pub tab_id: String,
    pub fingerprint: String,
    pub visibility: BrokerVisibility,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub force_takeover_timeout_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_ping_interval_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_pong_timeout_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_reset_timeout_ms: Option<u32>,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BrokerClientEvent {
    ConnectRequested(ConnectRequested),
    PublicCommand {
        command: BrokerClientCommand,
    },
    BrokerMessageReceived {
        message: BrokerControlMessage,
        #[serde(default = "default_respond_to_broker_ping")]
        respond_to_broker_ping: bool,
        now_ms: u64,
    },
    TimerFired {
        timer_id: u64,
        kind: BrokerClientTimerKind,
        now_ms: u64,
    },
    CallbackResolved {
        callback_id: u64,
        now_ms: u64,
    },
    CallbackRejected {
        callback_id: u64,
        error_message: String,
        now_ms: u64,
    },
    WorkerError {
        worker_id: u64,
        error_message: String,
        now_ms: u64,
    },
    PortMessageError {
        port_id: u64,
        now_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BrokerClientCommand {
    WaitForRole {
        role: BrokerRole,
        promise_id: u64,
        timeout_ms: u32,
    },
    ReportLeaderReady {
        leadership_id: u32,
        tab_lock_name: String,
        worker_lock_name: String,
        bridgeless_storage_reset: bool,
    },
    ReportLeaderFailed {
        leadership_id: u32,
        reason: String,
    },
    ReportVisibility {
        visibility: BrokerVisibility,
    },
    ReportFollowerPortAttached {
        follower_tab_id: String,
        leadership_id: u32,
    },
    ReportFollowerPortClosed {
        follower_tab_id: String,
        leadership_id: u32,
    },
    ReportSchemaReady {
        schema_fingerprint: String,
    },
    RequestStorageReset {
        request_id: String,
        start_promise_id: u64,
        completion_promise_id: u64,
    },
    ReportStorageResetReady {
        request_id: String,
        success: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_message: Option<String>,
    },
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BrokerClientTimerKind {
    BrokerHello,
    InitialLeadership,
    BrokerLiveness,
    RoleWaiter { promise_id: u64 },
    StorageResetStart { request_id: String, promise_id: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
pub enum BrowserBrokerTabMessage {
    Hello {
        tab_id: String,
        app_id: String,
        db_name: String,
        fingerprint: String,
        visibility: BrokerVisibility,
        #[serde(skip_serializing_if = "Option::is_none")]
        force_takeover_timeout_ms: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        broker_ping_interval_ms: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        broker_pong_timeout_ms: Option<u32>,
    },
    Visibility {
        broker_instance_id: String,
        visibility: BrokerVisibility,
    },
    LeaderReady {
        broker_instance_id: String,
        leadership_id: u32,
        tab_lock_name: String,
        worker_lock_name: String,
        #[serde(skip_serializing_if = "is_false")]
        bridgeless_storage_reset: bool,
    },
    LeaderFailed {
        broker_instance_id: String,
        leadership_id: u32,
        reason: String,
    },
    FollowerPortAttached {
        broker_instance_id: String,
        follower_tab_id: String,
        leadership_id: u32,
    },
    FollowerPortClosed {
        broker_instance_id: String,
        follower_tab_id: String,
        leadership_id: u32,
    },
    SchemaReady {
        broker_instance_id: String,
        schema_fingerprint: String,
    },
    StorageResetRequest {
        broker_instance_id: String,
        request_id: String,
    },
    StorageResetReady {
        broker_instance_id: String,
        request_id: String,
        success: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_message: Option<String>,
    },
    Shutdown {
        broker_instance_id: String,
    },
    BrokerPong {
        broker_instance_id: String,
    },
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
    BrokerPing {
        broker_instance_id: String,
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
    AttachFollowerPort {
        broker_instance_id: String,
        follower_tab_id: String,
        leadership_id: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        port_id: Option<u64>,
    },
    UseFollowerPort {
        broker_instance_id: String,
        leader_tab_id: String,
        leadership_id: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        port_id: Option<u64>,
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
    SchemaBlocked {
        broker_instance_id: String,
        reason: String,
    },
}

impl BrokerControlMessage {
    fn broker_instance_id(&self) -> &str {
        match self {
            BrokerControlMessage::BrokerHello { broker_instance_id }
            | BrokerControlMessage::Unsupported {
                broker_instance_id, ..
            }
            | BrokerControlMessage::BrokerPing { broker_instance_id }
            | BrokerControlMessage::BecomeLeader {
                broker_instance_id, ..
            }
            | BrokerControlMessage::Demote {
                broker_instance_id, ..
            }
            | BrokerControlMessage::LeaderReady {
                broker_instance_id, ..
            }
            | BrokerControlMessage::AttachFollowerPort {
                broker_instance_id, ..
            }
            | BrokerControlMessage::UseFollowerPort {
                broker_instance_id, ..
            }
            | BrokerControlMessage::FollowerReady {
                broker_instance_id, ..
            }
            | BrokerControlMessage::CloseFollowerPort {
                broker_instance_id, ..
            }
            | BrokerControlMessage::DetachFollowerPort {
                broker_instance_id, ..
            }
            | BrokerControlMessage::StorageResetBegin {
                broker_instance_id, ..
            }
            | BrokerControlMessage::StorageResetStarted {
                broker_instance_id, ..
            }
            | BrokerControlMessage::StorageResetFinished {
                broker_instance_id, ..
            }
            | BrokerControlMessage::SchemaBlocked {
                broker_instance_id, ..
            } => broker_instance_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BrokerClientCallback {
    BrokerPing,
    BecomeLeader {
        leadership_id: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        reset_request_id: Option<String>,
    },
    Demote {
        leadership_id: u32,
    },
    AttachFollowerPort {
        follower_tab_id: String,
        leadership_id: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        port_id: Option<u64>,
    },
    DetachFollowerPort {
        follower_tab_id: String,
        leadership_id: u32,
    },
    UseFollowerPort {
        leader_tab_id: String,
        leadership_id: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        port_id: Option<u64>,
    },
    FollowerReady {
        leader_tab_id: String,
        leadership_id: u32,
    },
    CloseFollowerPort {
        leadership_id: u32,
    },
    StorageResetBegin {
        request_id: String,
        leadership_id: u32,
    },
    SchemaBlocked {
        reason: String,
    },
    Reconnected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BrokerClientEffect {
    CreateSharedWorker {
        worker_id: u64,
        name: String,
    },
    AttachPortListeners {
        port_id: u64,
    },
    DetachPort {
        port_id: u64,
        close: bool,
    },
    PostToBroker {
        port_id: u64,
        message: BrowserBrokerTabMessage,
    },
    ArmTimer {
        timer_id: u64,
        kind: BrokerClientTimerKind,
        delay_ms: u32,
    },
    CancelTimer {
        timer_id: u64,
    },
    ReleaseMessagePort {
        port_id: u64,
    },
    InvokeCallback {
        callback_id: u64,
        callback: BrokerClientCallback,
    },
    ResolveConnect,
    RejectConnect {
        reason: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        code: Option<String>,
    },
    ResolvePublicPromise {
        promise_id: u64,
    },
    RejectPublicPromise {
        promise_id: u64,
        reason: String,
    },
    CloseClient {
        reason: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        code: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerClientSnapshot {
    pub broker_instance_id: Option<String>,
    pub role: BrokerRole,
    pub tab_id: String,
    pub leader_tab_id: Option<String>,
    pub leadership_id: u32,
    pub visibility: BrokerVisibility,
    pub closed: bool,
    pub reconnecting: bool,
}

#[derive(Debug, Clone)]
pub struct BrokerClientCore {
    config: Option<ClientConfig>,
    broker_instance_id: Option<String>,
    role: BrokerRole,
    leader_tab_id: Option<String>,
    leadership_id: u32,
    visibility: BrokerVisibility,
    closed: bool,
    reconnecting: bool,
    connecting: Option<ConnectingState>,
    current_port_id: Option<u64>,
    current_worker_id: Option<u64>,
    broker_liveness_timer_id: Option<u64>,
    role_waiters: Vec<RoleWaiter>,
    reset_start_waiters: BTreeMap<String, Vec<ResetStartWaiter>>,
    reset_completion_waiters: BTreeMap<String, Vec<u64>>,
    pending_reset_requests: Vec<PendingResetRequest>,
    pending_callbacks: HashMap<u64, PendingCallback>,
    next_worker_id: u64,
    next_port_id: u64,
    next_timer_id: u64,
    next_callback_id: u64,
}

#[derive(Debug, Clone)]
struct ClientConfig {
    app_id: String,
    db_name: String,
    tab_id: String,
    fingerprint: String,
    force_takeover_timeout_ms: Option<u32>,
    broker_ping_interval_ms: Option<u32>,
    broker_pong_timeout_ms: Option<u32>,
    storage_reset_timeout_ms: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionPurpose {
    Initial,
    Reconnect,
}

#[derive(Debug, Clone)]
struct ConnectingState {
    purpose: ConnectionPurpose,
    worker_id: u64,
    hello_timer_id: u64,
    initial_leadership_timer_id: Option<u64>,
}

#[derive(Debug, Clone)]
struct RoleWaiter {
    role: BrokerRole,
    promise_id: u64,
    timer_id: u64,
}

#[derive(Debug, Clone)]
struct ResetStartWaiter {
    promise_id: u64,
    timer_id: Option<u64>,
}

#[derive(Debug, Clone)]
struct PendingResetRequest {
    request_id: String,
    start_promise_id: u64,
}

#[derive(Debug, Clone)]
enum PendingCallback {
    BecomeLeaderFailed {
        leadership_id: u32,
    },
    StorageResetReady {
        request_id: String,
    },
    ReconnectDemote {
        previous_role: BrokerRole,
        previous_leadership_id: u32,
    },
}

impl Default for BrokerClientCore {
    fn default() -> Self {
        Self::new()
    }
}

impl BrokerClientCore {
    pub fn new() -> Self {
        Self {
            config: None,
            broker_instance_id: None,
            role: BrokerRole::Follower,
            leader_tab_id: None,
            leadership_id: 0,
            visibility: BrokerVisibility::Visible,
            closed: false,
            reconnecting: false,
            connecting: None,
            current_port_id: None,
            current_worker_id: None,
            broker_liveness_timer_id: None,
            role_waiters: Vec::new(),
            reset_start_waiters: BTreeMap::new(),
            reset_completion_waiters: BTreeMap::new(),
            pending_reset_requests: Vec::new(),
            pending_callbacks: HashMap::new(),
            next_worker_id: 1,
            next_port_id: 1,
            next_timer_id: 1,
            next_callback_id: 1,
        }
    }

    pub fn handle(&mut self, event: BrokerClientEvent) -> Vec<BrokerClientEffect> {
        match event {
            BrokerClientEvent::ConnectRequested(event) => self.handle_connect_requested(event),
            BrokerClientEvent::PublicCommand { command } => self.handle_public_command(command),
            BrokerClientEvent::BrokerMessageReceived {
                message,
                respond_to_broker_ping,
                now_ms,
            } => self.handle_broker_message(message, respond_to_broker_ping, now_ms),
            BrokerClientEvent::TimerFired {
                timer_id,
                kind,
                now_ms,
            } => self.handle_timer_fired(timer_id, kind, now_ms),
            BrokerClientEvent::CallbackResolved {
                callback_id,
                now_ms,
            } => self.handle_callback_resolved(callback_id, now_ms),
            BrokerClientEvent::CallbackRejected {
                callback_id,
                error_message,
                now_ms,
            } => self.handle_callback_rejected(callback_id, error_message, now_ms),
            BrokerClientEvent::WorkerError {
                worker_id,
                error_message,
                now_ms,
            } => self.handle_worker_error(worker_id, error_message, now_ms),
            BrokerClientEvent::PortMessageError { port_id, now_ms } => {
                if self.current_port_id == Some(port_id) {
                    self.start_reconnect(
                        format!("Browser broker port message error at {now_ms}"),
                        now_ms,
                    )
                } else {
                    Vec::new()
                }
            }
        }
    }

    pub fn snapshot(&self) -> BrokerClientSnapshot {
        BrokerClientSnapshot {
            broker_instance_id: self.broker_instance_id.clone(),
            role: self.role,
            tab_id: self
                .config
                .as_ref()
                .map(|config| config.tab_id.clone())
                .unwrap_or_default(),
            leader_tab_id: self.leader_tab_id.clone(),
            leadership_id: self.leadership_id,
            visibility: self.visibility,
            closed: self.closed,
            reconnecting: self.reconnecting,
        }
    }

    fn handle_connect_requested(&mut self, event: ConnectRequested) -> Vec<BrokerClientEffect> {
        if self.closed {
            return Vec::new();
        }

        self.config = Some(ClientConfig {
            app_id: event.app_id,
            db_name: event.db_name,
            tab_id: event.tab_id,
            fingerprint: event.fingerprint,
            force_takeover_timeout_ms: event.force_takeover_timeout_ms,
            broker_ping_interval_ms: event.broker_ping_interval_ms,
            broker_pong_timeout_ms: event.broker_pong_timeout_ms,
            storage_reset_timeout_ms: event.storage_reset_timeout_ms,
        });
        self.visibility = event.visibility;
        self.begin_connection(ConnectionPurpose::Initial)
    }

    fn handle_public_command(&mut self, command: BrokerClientCommand) -> Vec<BrokerClientEffect> {
        match command {
            BrokerClientCommand::WaitForRole {
                role,
                promise_id,
                timeout_ms,
            } => self.handle_wait_for_role(role, promise_id, timeout_ms),
            BrokerClientCommand::ReportLeaderReady {
                leadership_id,
                tab_lock_name,
                worker_lock_name,
                bridgeless_storage_reset,
            } => self.send_stamped(|broker_instance_id| BrowserBrokerTabMessage::LeaderReady {
                broker_instance_id,
                leadership_id,
                tab_lock_name,
                worker_lock_name,
                bridgeless_storage_reset,
            }),
            BrokerClientCommand::ReportLeaderFailed {
                leadership_id,
                reason,
            } => self.send_stamped(|broker_instance_id| BrowserBrokerTabMessage::LeaderFailed {
                broker_instance_id,
                leadership_id,
                reason,
            }),
            BrokerClientCommand::ReportVisibility { visibility } => {
                self.visibility = visibility;
                self.send_stamped(|broker_instance_id| BrowserBrokerTabMessage::Visibility {
                    broker_instance_id,
                    visibility,
                })
            }
            BrokerClientCommand::ReportFollowerPortAttached {
                follower_tab_id,
                leadership_id,
            } => self.send_stamped(|broker_instance_id| {
                BrowserBrokerTabMessage::FollowerPortAttached {
                    broker_instance_id,
                    follower_tab_id,
                    leadership_id,
                }
            }),
            BrokerClientCommand::ReportFollowerPortClosed {
                follower_tab_id,
                leadership_id,
            } => self.send_stamped(|broker_instance_id| {
                BrowserBrokerTabMessage::FollowerPortClosed {
                    broker_instance_id,
                    follower_tab_id,
                    leadership_id,
                }
            }),
            BrokerClientCommand::ReportSchemaReady { schema_fingerprint } => {
                self.send_stamped(|broker_instance_id| BrowserBrokerTabMessage::SchemaReady {
                    broker_instance_id,
                    schema_fingerprint,
                })
            }
            BrokerClientCommand::RequestStorageReset {
                request_id,
                start_promise_id,
                completion_promise_id,
            } => self.handle_storage_reset_request(
                request_id,
                start_promise_id,
                completion_promise_id,
            ),
            BrokerClientCommand::ReportStorageResetReady {
                request_id,
                success,
                error_message,
            } => {
                self.send_stamped(
                    |broker_instance_id| BrowserBrokerTabMessage::StorageResetReady {
                        broker_instance_id,
                        request_id,
                        success,
                        error_message,
                    },
                )
            }
            BrokerClientCommand::Shutdown => self.handle_shutdown(),
        }
    }

    fn handle_broker_message(
        &mut self,
        message: BrokerControlMessage,
        respond_to_broker_ping: bool,
        now_ms: u64,
    ) -> Vec<BrokerClientEffect> {
        if self.closed {
            return Vec::new();
        }

        if let Some(current_instance_id) = self.broker_instance_id.as_deref() {
            if message.broker_instance_id() != current_instance_id {
                return self.start_reconnect(
                    format!(
                        "Browser broker instance changed from {current_instance_id} to {}",
                        message.broker_instance_id()
                    ),
                    now_ms,
                );
            }
        }

        match message {
            BrokerControlMessage::BrokerHello { broker_instance_id } => {
                self.handle_broker_hello(broker_instance_id)
            }
            BrokerControlMessage::Unsupported { code, reason, .. } => {
                self.handle_unsupported(reason, code)
            }
            BrokerControlMessage::BrokerPing { broker_instance_id } => {
                let mut effects = self.refresh_broker_liveness_timer();
                effects.push(self.invoke_callback(BrokerClientCallback::BrokerPing, None));
                if respond_to_broker_ping {
                    if let Some(port_id) = self.current_port_id {
                        effects.push(BrokerClientEffect::PostToBroker {
                            port_id,
                            message: BrowserBrokerTabMessage::BrokerPong { broker_instance_id },
                        });
                    }
                }
                effects
            }
            BrokerControlMessage::BecomeLeader {
                leadership_id,
                reset_request_id,
                ..
            } => {
                self.leadership_id = leadership_id;
                let mut effects = vec![self.invoke_callback(
                    BrokerClientCallback::BecomeLeader {
                        leadership_id,
                        reset_request_id,
                    },
                    Some(PendingCallback::BecomeLeaderFailed { leadership_id }),
                )];
                effects.extend(self.complete_connection_if_waiting_for_leadership());
                effects
            }
            BrokerControlMessage::Demote { leadership_id, .. } => {
                if leadership_id == self.leadership_id {
                    self.role = BrokerRole::Follower;
                    self.leader_tab_id = None;
                }
                let mut effects = vec![
                    self.invoke_callback(BrokerClientCallback::Demote { leadership_id }, None)
                ];
                effects.extend(self.resolve_role_waiters());
                effects
            }
            BrokerControlMessage::LeaderReady {
                leader_tab_id,
                leadership_id,
                ..
            } => {
                self.leadership_id = leadership_id;
                self.role = if Some(leader_tab_id.as_str()) == self.config_tab_id() {
                    BrokerRole::Leader
                } else {
                    BrokerRole::Follower
                };
                self.leader_tab_id = Some(leader_tab_id);
                let mut effects = self.resolve_role_waiters();
                effects.extend(self.complete_connection_if_waiting_for_leadership());
                effects
            }
            BrokerControlMessage::AttachFollowerPort {
                follower_tab_id,
                leadership_id,
                port_id,
                ..
            } => {
                if leadership_id != self.leadership_id {
                    return release_message_port(port_id);
                }
                vec![self.invoke_callback(
                    BrokerClientCallback::AttachFollowerPort {
                        follower_tab_id,
                        leadership_id,
                        port_id,
                    },
                    None,
                )]
            }
            BrokerControlMessage::UseFollowerPort {
                leader_tab_id,
                leadership_id,
                port_id,
                ..
            } => {
                if self.leadership_id > 0 && leadership_id < self.leadership_id {
                    return release_message_port(port_id);
                }
                self.leadership_id = leadership_id;
                self.leader_tab_id = Some(leader_tab_id.clone());
                self.role = BrokerRole::Follower;
                let mut effects = vec![self.invoke_callback(
                    BrokerClientCallback::UseFollowerPort {
                        leader_tab_id,
                        leadership_id,
                        port_id,
                    },
                    None,
                )];
                effects.extend(self.complete_connection_if_waiting_for_leadership());
                effects
            }
            BrokerControlMessage::FollowerReady {
                leader_tab_id,
                leadership_id,
                ..
            } => {
                self.leadership_id = leadership_id;
                self.leader_tab_id = Some(leader_tab_id.clone());
                self.role = BrokerRole::Follower;
                let mut effects = vec![self.invoke_callback(
                    BrokerClientCallback::FollowerReady {
                        leader_tab_id,
                        leadership_id,
                    },
                    None,
                )];
                effects.extend(self.resolve_role_waiters());
                effects.extend(self.complete_connection_if_waiting_for_leadership());
                effects
            }
            BrokerControlMessage::CloseFollowerPort { leadership_id, .. } => {
                vec![self.invoke_callback(
                    BrokerClientCallback::CloseFollowerPort { leadership_id },
                    None,
                )]
            }
            BrokerControlMessage::DetachFollowerPort {
                follower_tab_id,
                leadership_id,
                ..
            } => {
                vec![self.invoke_callback(
                    BrokerClientCallback::DetachFollowerPort {
                        follower_tab_id,
                        leadership_id,
                    },
                    None,
                )]
            }
            BrokerControlMessage::StorageResetBegin {
                request_id,
                leadership_id,
                ..
            } => {
                let mut effects = self.resolve_reset_start_waiters(&request_id);
                effects.push(self.invoke_callback(
                    BrokerClientCallback::StorageResetBegin {
                        request_id: request_id.clone(),
                        leadership_id,
                    },
                    Some(PendingCallback::StorageResetReady { request_id }),
                ));
                effects
            }
            BrokerControlMessage::StorageResetStarted { request_id, .. } => {
                self.resolve_reset_start_waiters(&request_id)
            }
            BrokerControlMessage::StorageResetFinished {
                request_id,
                success,
                error_message,
                ..
            } => {
                let mut effects = self.resolve_reset_start_waiters(&request_id);
                effects.extend(self.resolve_reset_completion_waiters(
                    &request_id,
                    success,
                    error_message,
                ));
                effects
            }
            BrokerControlMessage::SchemaBlocked { reason, .. } => {
                vec![self.invoke_callback(BrokerClientCallback::SchemaBlocked { reason }, None)]
            }
        }
    }

    fn handle_timer_fired(
        &mut self,
        timer_id: u64,
        kind: BrokerClientTimerKind,
        now_ms: u64,
    ) -> Vec<BrokerClientEffect> {
        match kind {
            BrokerClientTimerKind::BrokerHello => {
                if self
                    .connecting
                    .as_ref()
                    .map(|connecting| connecting.hello_timer_id == timer_id)
                    .unwrap_or(false)
                {
                    self.connection_failed(
                        "Timed out waiting for browser broker hello".to_string(),
                        None,
                    )
                } else {
                    Vec::new()
                }
            }
            BrokerClientTimerKind::InitialLeadership => {
                if self
                    .connecting
                    .as_ref()
                    .and_then(|connecting| connecting.initial_leadership_timer_id)
                    == Some(timer_id)
                {
                    self.complete_connection()
                } else {
                    Vec::new()
                }
            }
            BrokerClientTimerKind::BrokerLiveness => {
                if self.broker_liveness_timer_id == Some(timer_id) {
                    self.broker_liveness_timer_id = None;
                    self.start_reconnect(
                        "Browser broker liveness timed out waiting for broker ping".to_string(),
                        now_ms,
                    )
                } else {
                    Vec::new()
                }
            }
            BrokerClientTimerKind::RoleWaiter { promise_id } => {
                self.handle_role_waiter_timeout(timer_id, promise_id)
            }
            BrokerClientTimerKind::StorageResetStart {
                request_id,
                promise_id,
            } => self.handle_storage_reset_start_timeout(timer_id, request_id, promise_id),
        }
    }

    fn handle_callback_resolved(
        &mut self,
        callback_id: u64,
        _now_ms: u64,
    ) -> Vec<BrokerClientEffect> {
        match self.pending_callbacks.remove(&callback_id) {
            Some(PendingCallback::BecomeLeaderFailed { .. }) => Vec::new(),
            Some(PendingCallback::StorageResetReady { request_id }) => self.send_stamped(
                |broker_instance_id| BrowserBrokerTabMessage::StorageResetReady {
                    broker_instance_id,
                    request_id,
                    success: true,
                    error_message: None,
                },
            ),
            Some(PendingCallback::ReconnectDemote {
                previous_role,
                previous_leadership_id,
            }) => self.continue_reconnect_after_demote(previous_role, previous_leadership_id),
            None => Vec::new(),
        }
    }

    fn handle_callback_rejected(
        &mut self,
        callback_id: u64,
        error_message: String,
        _now_ms: u64,
    ) -> Vec<BrokerClientEffect> {
        match self.pending_callbacks.remove(&callback_id) {
            Some(PendingCallback::BecomeLeaderFailed { leadership_id }) => {
                self.send_stamped(|broker_instance_id| BrowserBrokerTabMessage::LeaderFailed {
                    broker_instance_id,
                    leadership_id,
                    reason: error_message,
                })
            }
            Some(PendingCallback::StorageResetReady { request_id }) => self.send_stamped(
                |broker_instance_id| BrowserBrokerTabMessage::StorageResetReady {
                    broker_instance_id,
                    request_id,
                    success: false,
                    error_message: Some(error_message),
                },
            ),
            Some(PendingCallback::ReconnectDemote { .. }) => {
                self.close_with_error(error_message, None, true)
            }
            None => Vec::new(),
        }
    }

    fn handle_worker_error(
        &mut self,
        worker_id: u64,
        error_message: String,
        _now_ms: u64,
    ) -> Vec<BrokerClientEffect> {
        if self
            .connecting
            .as_ref()
            .map(|connecting| connecting.worker_id == worker_id)
            .unwrap_or(false)
        {
            self.connection_failed(
                format!("Browser broker SharedWorker failed to start: {error_message}"),
                None,
            )
        } else {
            Vec::new()
        }
    }

    fn handle_broker_hello(&mut self, broker_instance_id: String) -> Vec<BrokerClientEffect> {
        let Some(connecting) = self.connecting.as_ref() else {
            self.broker_instance_id = Some(broker_instance_id);
            return Vec::new();
        };

        let hello_timer_id = connecting.hello_timer_id;
        self.broker_instance_id = Some(broker_instance_id);

        let mut effects = vec![BrokerClientEffect::CancelTimer {
            timer_id: hello_timer_id,
        }];
        let initial_timer_id = self.alloc_timer_id();
        if let Some(connecting) = self.connecting.as_mut() {
            connecting.initial_leadership_timer_id = Some(initial_timer_id);
        }
        effects.push(BrokerClientEffect::ArmTimer {
            timer_id: initial_timer_id,
            kind: BrokerClientTimerKind::InitialLeadership,
            delay_ms: DEFAULT_INITIAL_LEADERSHIP_TIMEOUT_MS,
        });
        effects
    }

    fn handle_unsupported(
        &mut self,
        reason: String,
        code: Option<String>,
    ) -> Vec<BrokerClientEffect> {
        if self.connecting.is_some() {
            self.connection_failed(reason, code)
        } else {
            self.close_with_error(reason, code, true)
        }
    }

    fn handle_wait_for_role(
        &mut self,
        role: BrokerRole,
        promise_id: u64,
        timeout_ms: u32,
    ) -> Vec<BrokerClientEffect> {
        if self.closed {
            return vec![BrokerClientEffect::RejectPublicPromise {
                promise_id,
                reason: "Browser broker client closed".to_string(),
            }];
        }

        if self.role_available(role) {
            return vec![BrokerClientEffect::ResolvePublicPromise { promise_id }];
        }

        let timer_id = self.alloc_timer_id();
        self.role_waiters.push(RoleWaiter {
            role,
            promise_id,
            timer_id,
        });
        vec![BrokerClientEffect::ArmTimer {
            timer_id,
            kind: BrokerClientTimerKind::RoleWaiter { promise_id },
            delay_ms: timeout_ms,
        }]
    }

    fn handle_role_waiter_timeout(
        &mut self,
        timer_id: u64,
        promise_id: u64,
    ) -> Vec<BrokerClientEffect> {
        let Some(index) = self
            .role_waiters
            .iter()
            .position(|waiter| waiter.timer_id == timer_id && waiter.promise_id == promise_id)
        else {
            return Vec::new();
        };
        let waiter = self.role_waiters.remove(index);
        vec![BrokerClientEffect::RejectPublicPromise {
            promise_id,
            reason: format!(
                "Timed out waiting for broker role {}",
                role_name(waiter.role)
            ),
        }]
    }

    fn handle_storage_reset_request(
        &mut self,
        request_id: String,
        start_promise_id: u64,
        completion_promise_id: u64,
    ) -> Vec<BrokerClientEffect> {
        if self.closed {
            return vec![
                BrokerClientEffect::RejectPublicPromise {
                    promise_id: start_promise_id,
                    reason: "Browser broker client closed".to_string(),
                },
                BrokerClientEffect::RejectPublicPromise {
                    promise_id: completion_promise_id,
                    reason: "Browser broker client closed".to_string(),
                },
            ];
        }

        self.reset_completion_waiters
            .entry(request_id.clone())
            .or_default()
            .push(completion_promise_id);

        if self.can_post_to_broker() {
            let mut effects = self.post_storage_reset_request(&request_id);
            effects.extend(self.arm_storage_reset_start_timer(&request_id, start_promise_id));
            return effects;
        }

        self.pending_reset_requests.push(PendingResetRequest {
            request_id,
            start_promise_id,
        });
        Vec::new()
    }

    fn handle_storage_reset_start_timeout(
        &mut self,
        timer_id: u64,
        request_id: String,
        promise_id: u64,
    ) -> Vec<BrokerClientEffect> {
        let Some(waiters) = self.reset_start_waiters.get_mut(&request_id) else {
            return Vec::new();
        };
        let Some(index) = waiters.iter().position(|waiter| {
            waiter.timer_id == Some(timer_id) && waiter.promise_id == promise_id
        }) else {
            return Vec::new();
        };
        waiters.remove(index);
        if waiters.is_empty() {
            self.reset_start_waiters.remove(&request_id);
        }
        vec![BrokerClientEffect::RejectPublicPromise {
            promise_id,
            reason: format!("Timed out waiting for browser storage reset {request_id} to start"),
        }]
    }

    fn handle_shutdown(&mut self) -> Vec<BrokerClientEffect> {
        if self.closed {
            return Vec::new();
        }

        self.closed = true;
        self.reconnecting = false;
        let mut effects = Vec::new();
        if let (Some(port_id), Some(broker_instance_id)) =
            (self.current_port_id, self.broker_instance_id.clone())
        {
            effects.push(BrokerClientEffect::PostToBroker {
                port_id,
                message: BrowserBrokerTabMessage::Shutdown { broker_instance_id },
            });
        }
        effects.extend(self.cancel_connection_timers());
        if let Some(timer_id) = self.broker_liveness_timer_id.take() {
            effects.push(BrokerClientEffect::CancelTimer { timer_id });
        }
        if let Some(port_id) = self.current_port_id.take() {
            effects.push(BrokerClientEffect::DetachPort {
                port_id,
                close: true,
            });
        }
        effects.extend(self.reject_role_waiters("Browser broker client closed"));
        effects.extend(self.reject_all_reset_waiters("Browser broker client closed"));
        self.pending_reset_requests.clear();
        effects
    }

    fn begin_connection(&mut self, purpose: ConnectionPurpose) -> Vec<BrokerClientEffect> {
        let Some(config) = self.config.clone() else {
            return Vec::new();
        };

        let worker_id = self.alloc_worker_id();
        let port_id = self.alloc_port_id();
        let hello_timer_id = self.alloc_timer_id();
        self.current_worker_id = Some(worker_id);
        self.current_port_id = Some(port_id);
        self.connecting = Some(ConnectingState {
            purpose,
            worker_id,
            hello_timer_id,
            initial_leadership_timer_id: None,
        });

        vec![
            BrokerClientEffect::CreateSharedWorker {
                worker_id,
                name: format!("jazz-broker:{}:{}", config.app_id, config.db_name),
            },
            BrokerClientEffect::AttachPortListeners { port_id },
            BrokerClientEffect::PostToBroker {
                port_id,
                message: BrowserBrokerTabMessage::Hello {
                    tab_id: config.tab_id,
                    app_id: config.app_id,
                    db_name: config.db_name,
                    fingerprint: config.fingerprint,
                    visibility: self.visibility,
                    force_takeover_timeout_ms: config.force_takeover_timeout_ms,
                    broker_ping_interval_ms: config.broker_ping_interval_ms,
                    broker_pong_timeout_ms: config.broker_pong_timeout_ms,
                },
            },
            BrokerClientEffect::ArmTimer {
                timer_id: hello_timer_id,
                kind: BrokerClientTimerKind::BrokerHello,
                delay_ms: DEFAULT_BROKER_HELLO_TIMEOUT_MS,
            },
        ]
    }

    fn complete_connection_if_waiting_for_leadership(&mut self) -> Vec<BrokerClientEffect> {
        if self
            .connecting
            .as_ref()
            .and_then(|connecting| connecting.initial_leadership_timer_id)
            .is_some()
        {
            self.complete_connection()
        } else {
            Vec::new()
        }
    }

    fn complete_connection(&mut self) -> Vec<BrokerClientEffect> {
        let Some(connecting) = self.connecting.take() else {
            return Vec::new();
        };

        let mut effects = Vec::new();
        if let Some(timer_id) = connecting.initial_leadership_timer_id {
            effects.push(BrokerClientEffect::CancelTimer { timer_id });
        }
        effects.extend(self.refresh_broker_liveness_timer());

        match connecting.purpose {
            ConnectionPurpose::Initial => {
                effects.push(BrokerClientEffect::ResolveConnect);
            }
            ConnectionPurpose::Reconnect => {
                self.reconnecting = false;
                effects.extend(self.replay_after_reconnect());
                effects.push(self.invoke_callback(BrokerClientCallback::Reconnected, None));
            }
        }
        effects
    }

    fn connection_failed(
        &mut self,
        reason: String,
        code: Option<String>,
    ) -> Vec<BrokerClientEffect> {
        let purpose = self
            .connecting
            .as_ref()
            .map(|connecting| connecting.purpose)
            .unwrap_or(ConnectionPurpose::Initial);

        let mut effects = self.cancel_connection_timers();
        if let Some(port_id) = self.current_port_id.take() {
            effects.push(BrokerClientEffect::DetachPort {
                port_id,
                close: true,
            });
        }
        self.current_worker_id = None;
        self.broker_instance_id = None;

        match purpose {
            ConnectionPurpose::Initial => {
                self.closed = true;
                effects.push(BrokerClientEffect::RejectConnect { reason, code });
            }
            ConnectionPurpose::Reconnect => {
                effects.extend(self.close_with_error(reason, code, false));
            }
        }
        effects
    }

    fn start_reconnect(&mut self, _reason: String, _now_ms: u64) -> Vec<BrokerClientEffect> {
        if self.closed || self.reconnecting {
            return Vec::new();
        }

        self.reconnecting = true;
        let previous_role = self.role;
        let previous_leadership_id = self.leadership_id;
        self.broker_instance_id = None;
        self.role = BrokerRole::Follower;
        self.leader_tab_id = None;
        self.leadership_id = 0;

        let mut effects = self.cancel_connection_timers();
        if let Some(timer_id) = self.broker_liveness_timer_id.take() {
            effects.push(BrokerClientEffect::CancelTimer { timer_id });
        }
        effects.extend(self.reject_all_reset_waiters(RESET_RECONNECT_ERROR));
        self.pending_reset_requests.clear();

        if let Some(port_id) = self.current_port_id.take() {
            effects.push(BrokerClientEffect::DetachPort {
                port_id,
                close: true,
            });
        }

        if previous_leadership_id > 0 {
            effects.push(self.invoke_callback(
                BrokerClientCallback::Demote {
                    leadership_id: previous_leadership_id,
                },
                Some(PendingCallback::ReconnectDemote {
                    previous_role,
                    previous_leadership_id,
                }),
            ));
        } else {
            effects.extend(self.begin_connection(ConnectionPurpose::Reconnect));
        }

        effects
    }

    fn continue_reconnect_after_demote(
        &mut self,
        previous_role: BrokerRole,
        previous_leadership_id: u32,
    ) -> Vec<BrokerClientEffect> {
        if self.closed || !self.reconnecting {
            return Vec::new();
        }

        let mut effects = Vec::new();
        if previous_role == BrokerRole::Follower && previous_leadership_id > 0 {
            effects.push(self.invoke_callback(
                BrokerClientCallback::CloseFollowerPort {
                    leadership_id: previous_leadership_id,
                },
                None,
            ));
        }
        effects.extend(self.begin_connection(ConnectionPurpose::Reconnect));
        effects
    }

    fn replay_after_reconnect(&mut self) -> Vec<BrokerClientEffect> {
        let mut effects =
            self.send_stamped(|broker_instance_id| BrowserBrokerTabMessage::Visibility {
                broker_instance_id,
                visibility: self.visibility,
            });
        let pending = std::mem::take(&mut self.pending_reset_requests);
        for request in pending {
            effects.extend(self.post_storage_reset_request(&request.request_id));
            effects.extend(
                self.arm_storage_reset_start_timer(&request.request_id, request.start_promise_id),
            );
        }
        effects
    }

    fn refresh_broker_liveness_timer(&mut self) -> Vec<BrokerClientEffect> {
        let mut effects = Vec::new();
        if let Some(timer_id) = self.broker_liveness_timer_id.take() {
            effects.push(BrokerClientEffect::CancelTimer { timer_id });
        }
        if self.closed {
            return effects;
        }
        let timer_id = self.alloc_timer_id();
        self.broker_liveness_timer_id = Some(timer_id);
        effects.push(BrokerClientEffect::ArmTimer {
            timer_id,
            kind: BrokerClientTimerKind::BrokerLiveness,
            delay_ms: self.broker_liveness_timeout_ms(),
        });
        effects
    }

    fn post_storage_reset_request(&self, request_id: &str) -> Vec<BrokerClientEffect> {
        self.send_stamped(
            |broker_instance_id| BrowserBrokerTabMessage::StorageResetRequest {
                broker_instance_id,
                request_id: request_id.to_string(),
            },
        )
    }

    fn arm_storage_reset_start_timer(
        &mut self,
        request_id: &str,
        promise_id: u64,
    ) -> Vec<BrokerClientEffect> {
        let timer_id = self.alloc_timer_id();
        self.reset_start_waiters
            .entry(request_id.to_string())
            .or_default()
            .push(ResetStartWaiter {
                promise_id,
                timer_id: Some(timer_id),
            });
        vec![BrokerClientEffect::ArmTimer {
            timer_id,
            kind: BrokerClientTimerKind::StorageResetStart {
                request_id: request_id.to_string(),
                promise_id,
            },
            delay_ms: self.storage_reset_timeout_ms(),
        }]
    }

    fn resolve_reset_start_waiters(&mut self, request_id: &str) -> Vec<BrokerClientEffect> {
        let Some(waiters) = self.reset_start_waiters.remove(request_id) else {
            return Vec::new();
        };
        let mut effects = Vec::new();
        for waiter in waiters {
            if let Some(timer_id) = waiter.timer_id {
                effects.push(BrokerClientEffect::CancelTimer { timer_id });
            }
            effects.push(BrokerClientEffect::ResolvePublicPromise {
                promise_id: waiter.promise_id,
            });
        }
        effects
    }

    fn resolve_reset_completion_waiters(
        &mut self,
        request_id: &str,
        success: bool,
        error_message: Option<String>,
    ) -> Vec<BrokerClientEffect> {
        let Some(waiters) = self.reset_completion_waiters.remove(request_id) else {
            return Vec::new();
        };
        waiters
            .into_iter()
            .map(|promise_id| {
                if success {
                    BrokerClientEffect::ResolvePublicPromise { promise_id }
                } else {
                    BrokerClientEffect::RejectPublicPromise {
                        promise_id,
                        reason: error_message
                            .clone()
                            .unwrap_or_else(|| "Browser storage reset failed".to_string()),
                    }
                }
            })
            .collect()
    }

    fn resolve_role_waiters(&mut self) -> Vec<BrokerClientEffect> {
        let mut effects = Vec::new();
        let mut index = 0;
        while index < self.role_waiters.len() {
            if self.role_available(self.role_waiters[index].role) {
                let waiter = self.role_waiters.remove(index);
                effects.push(BrokerClientEffect::CancelTimer {
                    timer_id: waiter.timer_id,
                });
                effects.push(BrokerClientEffect::ResolvePublicPromise {
                    promise_id: waiter.promise_id,
                });
            } else {
                index += 1;
            }
        }
        effects
    }

    fn reject_role_waiters(&mut self, reason: &str) -> Vec<BrokerClientEffect> {
        self.role_waiters
            .drain(..)
            .flat_map(|waiter| {
                [
                    BrokerClientEffect::CancelTimer {
                        timer_id: waiter.timer_id,
                    },
                    BrokerClientEffect::RejectPublicPromise {
                        promise_id: waiter.promise_id,
                        reason: reason.to_string(),
                    },
                ]
            })
            .collect()
    }

    fn reject_all_reset_waiters(&mut self, reason: &str) -> Vec<BrokerClientEffect> {
        let mut effects = Vec::new();
        let reset_start_waiters = std::mem::take(&mut self.reset_start_waiters);
        for waiters in reset_start_waiters.into_values() {
            for waiter in waiters {
                if let Some(timer_id) = waiter.timer_id {
                    effects.push(BrokerClientEffect::CancelTimer { timer_id });
                }
                effects.push(BrokerClientEffect::RejectPublicPromise {
                    promise_id: waiter.promise_id,
                    reason: reason.to_string(),
                });
            }
        }
        let reset_completion_waiters = std::mem::take(&mut self.reset_completion_waiters);
        for promise_id in reset_completion_waiters.into_values().flatten() {
            effects.push(BrokerClientEffect::RejectPublicPromise {
                promise_id,
                reason: reason.to_string(),
            });
        }
        effects
    }

    fn send_stamped<F>(&self, make_message: F) -> Vec<BrokerClientEffect>
    where
        F: FnOnce(String) -> BrowserBrokerTabMessage,
    {
        if !self.can_post_to_broker() {
            return Vec::new();
        }
        let broker_instance_id = self.broker_instance_id.clone().unwrap_or_default();
        let port_id = self.current_port_id.unwrap_or_default();
        vec![BrokerClientEffect::PostToBroker {
            port_id,
            message: make_message(broker_instance_id),
        }]
    }

    fn can_post_to_broker(&self) -> bool {
        !self.closed
            && !self.reconnecting
            && self.broker_instance_id.is_some()
            && self.current_port_id.is_some()
    }

    fn invoke_callback(
        &mut self,
        callback: BrokerClientCallback,
        pending: Option<PendingCallback>,
    ) -> BrokerClientEffect {
        let callback_id = self.alloc_callback_id();
        if let Some(pending) = pending {
            self.pending_callbacks.insert(callback_id, pending);
        }
        BrokerClientEffect::InvokeCallback {
            callback_id,
            callback,
        }
    }

    fn close_with_error(
        &mut self,
        reason: String,
        code: Option<String>,
        include_detach: bool,
    ) -> Vec<BrokerClientEffect> {
        if self.closed {
            return Vec::new();
        }
        self.closed = true;
        self.reconnecting = false;

        let mut effects = self.cancel_connection_timers();
        if let Some(timer_id) = self.broker_liveness_timer_id.take() {
            effects.push(BrokerClientEffect::CancelTimer { timer_id });
        }
        if include_detach {
            if let Some(port_id) = self.current_port_id.take() {
                effects.push(BrokerClientEffect::DetachPort {
                    port_id,
                    close: true,
                });
            }
        }
        effects.extend(self.reject_role_waiters(&reason));
        effects.extend(self.reject_all_reset_waiters(&reason));
        self.pending_reset_requests.clear();
        self.broker_instance_id = None;
        effects.push(BrokerClientEffect::CloseClient { reason, code });
        effects
    }

    fn cancel_connection_timers(&mut self) -> Vec<BrokerClientEffect> {
        let Some(connecting) = self.connecting.take() else {
            return Vec::new();
        };
        let mut effects = vec![BrokerClientEffect::CancelTimer {
            timer_id: connecting.hello_timer_id,
        }];
        if let Some(timer_id) = connecting.initial_leadership_timer_id {
            effects.push(BrokerClientEffect::CancelTimer { timer_id });
        }
        effects
    }

    fn role_available(&self, role: BrokerRole) -> bool {
        self.role == role && self.leader_tab_id.is_some()
    }

    fn config_tab_id(&self) -> Option<&str> {
        self.config.as_ref().map(|config| config.tab_id.as_str())
    }

    fn broker_liveness_timeout_ms(&self) -> u32 {
        let Some(config) = self.config.as_ref() else {
            return DEFAULT_BROKER_PING_INTERVAL_MS + DEFAULT_BROKER_PONG_TIMEOUT_MS;
        };
        normalize_positive_timeout(
            config.broker_ping_interval_ms,
            DEFAULT_BROKER_PING_INTERVAL_MS,
        ) + normalize_positive_timeout(
            config.broker_pong_timeout_ms,
            DEFAULT_BROKER_PONG_TIMEOUT_MS,
        )
    }

    fn storage_reset_timeout_ms(&self) -> u32 {
        normalize_positive_timeout(
            self.config
                .as_ref()
                .and_then(|config| config.storage_reset_timeout_ms),
            DEFAULT_STORAGE_RESET_TIMEOUT_MS,
        )
    }

    fn alloc_worker_id(&mut self) -> u64 {
        let id = self.next_worker_id;
        self.next_worker_id += 1;
        id
    }

    fn alloc_port_id(&mut self) -> u64 {
        let id = self.next_port_id;
        self.next_port_id += 1;
        id
    }

    fn alloc_timer_id(&mut self) -> u64 {
        let id = self.next_timer_id;
        self.next_timer_id += 1;
        id
    }

    fn alloc_callback_id(&mut self) -> u64 {
        let id = self.next_callback_id;
        self.next_callback_id += 1;
        id
    }
}

#[wasm_bindgen(js_name = BrokerClient)]
pub struct BrokerClient {
    core: BrokerClientCore,
}

#[wasm_bindgen(js_class = BrokerClient)]
impl BrokerClient {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            core: BrokerClientCore::new(),
        }
    }

    #[wasm_bindgen(js_name = handleEvent)]
    pub fn handle_event(&mut self, event: JsValue) -> Result<JsValue, JsError> {
        let event: BrokerClientEvent = serde_wasm_bindgen::from_value(event)
            .map_err(|err| JsError::new(&format!("broker client event: {err}")))?;
        let effects = self.core.handle(event);
        serde_wasm_bindgen::to_value(&effects)
            .map_err(|err| JsError::new(&format!("broker client effects: {err}")))
    }

    #[wasm_bindgen(js_name = snapshot)]
    pub fn snapshot_js(&self) -> Result<JsValue, JsError> {
        serde_wasm_bindgen::to_value(&self.core.snapshot())
            .map_err(|err| JsError::new(&format!("broker client snapshot: {err}")))
    }
}

fn default_respond_to_broker_ping() -> bool {
    true
}

fn normalize_positive_timeout(value: Option<u32>, fallback: u32) -> u32 {
    value.filter(|value| *value > 0).unwrap_or(fallback).max(1)
}

fn release_message_port(port_id: Option<u64>) -> Vec<BrokerClientEffect> {
    port_id
        .map(|port_id| vec![BrokerClientEffect::ReleaseMessagePort { port_id }])
        .unwrap_or_default()
}

fn role_name(role: BrokerRole) -> &'static str {
    match role {
        BrokerRole::Leader => "leader",
        BrokerRole::Follower => "follower",
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}
