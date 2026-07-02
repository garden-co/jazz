use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::protocol::{ControlMessage, TabMessage, Visibility, normalize_positive_timeout};

const DEFAULT_BROKER_PING_INTERVAL_MS: u64 = 1_000;
const DEFAULT_BROKER_PONG_TIMEOUT_MS: u64 = 3_000;
const DEFAULT_STORAGE_RESET_TIMEOUT_MS: u64 = 5_000;

const CLIENT_CLOSED_MESSAGE: &str = "Browser broker client closed";
const RECONNECT_RESET_MESSAGE: &str = "Browser broker restarted during storage reset";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Leader,
    Follower,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum TabTimerKey {
    Liveness,
    #[serde(rename_all = "camelCase")]
    RoleWaiter {
        waiter_id: u64,
    },
    #[serde(rename_all = "camelCase")]
    ResetStartWaiter {
        waiter_id: u64,
    },
}

/// Outbound tab messages as produced by the public report*/send surface,
/// before the core stamps them with the broker instance id. `hello` is not
/// here: it is the one unstamped message and never flows through the core.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TabMessageInput {
    #[serde(rename_all = "camelCase")]
    Visibility {
        visibility: Visibility,
    },
    #[serde(rename_all = "camelCase")]
    LeaderReady {
        leadership_id: u64,
        tab_lock_name: String,
        worker_lock_name: String,
        #[serde(default)]
        bridgeless_storage_reset: bool,
    },
    #[serde(rename_all = "camelCase")]
    LeaderFailed {
        leadership_id: u64,
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    FollowerPortAttached {
        leadership_id: u64,
        follower_tab_id: String,
    },
    #[serde(rename_all = "camelCase")]
    FollowerPortClosed {
        leadership_id: u64,
        follower_tab_id: String,
    },
    #[serde(rename_all = "camelCase")]
    SchemaReady {
        schema_fingerprint: String,
    },
    #[serde(rename_all = "camelCase")]
    StorageResetRequest {
        request_id: String,
    },
    #[serde(rename_all = "camelCase")]
    StorageResetReady {
        request_id: String,
        success: bool,
        error_message: Option<String>,
    },
    Shutdown,
}

fn stamp(input: TabMessageInput, broker_instance_id: String) -> TabMessage {
    match input {
        TabMessageInput::Visibility { visibility } => TabMessage::Visibility {
            broker_instance_id,
            visibility,
        },
        TabMessageInput::LeaderReady {
            leadership_id,
            tab_lock_name,
            worker_lock_name,
            bridgeless_storage_reset,
        } => TabMessage::LeaderReady {
            broker_instance_id,
            leadership_id,
            tab_lock_name,
            worker_lock_name,
            bridgeless_storage_reset,
        },
        TabMessageInput::LeaderFailed {
            leadership_id,
            reason,
        } => TabMessage::LeaderFailed {
            broker_instance_id,
            leadership_id,
            reason,
        },
        TabMessageInput::FollowerPortAttached {
            leadership_id,
            follower_tab_id,
        } => TabMessage::FollowerPortAttached {
            broker_instance_id,
            leadership_id,
            follower_tab_id,
        },
        TabMessageInput::FollowerPortClosed {
            leadership_id,
            follower_tab_id,
        } => TabMessage::FollowerPortClosed {
            broker_instance_id,
            leadership_id,
            follower_tab_id,
        },
        TabMessageInput::SchemaReady { schema_fingerprint } => TabMessage::SchemaReady {
            broker_instance_id,
            schema_fingerprint,
        },
        TabMessageInput::StorageResetRequest { request_id } => TabMessage::StorageResetRequest {
            broker_instance_id,
            request_id,
        },
        TabMessageInput::StorageResetReady {
            request_id,
            success,
            error_message,
        } => TabMessage::StorageResetReady {
            broker_instance_id,
            request_id,
            success,
            error_message,
        },
        TabMessageInput::Shutdown => TabMessage::Shutdown { broker_instance_id },
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TabClientOptions {
    pub tab_id: String,
    pub broker_ping_interval_ms: Option<f64>,
    pub broker_pong_timeout_ms: Option<f64>,
    pub storage_reset_timeout_ms: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum TabClientEvent {
    /// A port is attached and control traffic can flow (initial connect right
    /// after core creation, or a reconnect's connectToBroker succeeding).
    PortAttached,
    /// Inbound control message. `stamped_instance_id` is the raw
    /// `brokerInstanceId` property read off the message object by the shell —
    /// carried separately so the instance-mismatch guard also covers message
    /// types this core does not know (the JS guard ran before its switch).
    #[serde(rename_all = "camelCase")]
    ControlMessage {
        message: ControlMessage,
        stamped_instance_id: Option<String>,
    },
    PortMessageError,
    #[serde(rename_all = "camelCase")]
    TimerFired {
        timer: TabTimerKey,
    },
    /// connect() finished its initial-leadership quiet window.
    ConnectCompleted,
    #[serde(rename_all = "camelCase")]
    RoleWaiterAdded {
        waiter_id: u64,
        role: Role,
        timeout_ms: u64,
    },
    #[serde(rename_all = "camelCase")]
    StorageResetRequested {
        request_id: String,
        start_waiter_id: u64,
        completion_waiter_id: u64,
    },
    #[serde(rename_all = "camelCase")]
    SendRequested {
        message: TabMessageInput,
    },
    #[serde(rename_all = "camelCase")]
    VisibilityReported {
        visibility: Visibility,
    },
    ShutdownRequested,
    /// Shell finished the reconnect choreography. `error` is the stringified
    /// failure, if any; the shell keeps the raw error to use as `cause`.
    #[serde(rename_all = "camelCase")]
    ReconnectFinished {
        error: Option<String>,
    },
}

/// How the shell should reject settled waiters: `ClosedError` means "use the
/// stored closedError instance", `Message` means "construct a new Error".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum WaiterRejection {
    ClosedError,
    #[serde(rename_all = "camelCase")]
    Message {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum TabClientCommand {
    #[serde(rename_all = "camelCase")]
    PostToBroker {
        message: TabMessage,
    },
    #[serde(rename_all = "camelCase")]
    SetTimer {
        timer: TabTimerKey,
        delay_ms: u64,
    },
    #[serde(rename_all = "camelCase")]
    ClearTimer {
        timer: TabTimerKey,
    },
    #[serde(rename_all = "camelCase")]
    SettleRoleWaiter {
        waiter_id: u64,
        rejection: Option<WaiterRejection>,
    },
    #[serde(rename_all = "camelCase")]
    SettleResetStartWaiters {
        waiter_ids: Vec<u64>,
        rejection: Option<WaiterRejection>,
    },
    #[serde(rename_all = "camelCase")]
    SettleResetWaiters {
        waiter_ids: Vec<u64>,
        rejection: Option<WaiterRejection>,
    },
    #[serde(rename_all = "camelCase")]
    InvokeOnBecomeLeader {
        leadership_id: u64,
        reset_request_id: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    InvokeOnDemote {
        leadership_id: u64,
    },
    #[serde(rename_all = "camelCase")]
    InvokeOnAttachFollowerPort {
        follower_tab_id: String,
        leadership_id: u64,
    },
    #[serde(rename_all = "camelCase")]
    InvokeOnUseFollowerPort {
        leadership_id: u64,
    },
    #[serde(rename_all = "camelCase")]
    InvokeOnFollowerReady {
        leadership_id: u64,
    },
    #[serde(rename_all = "camelCase")]
    InvokeOnCloseFollowerPort {
        leadership_id: u64,
    },
    #[serde(rename_all = "camelCase")]
    InvokeOnDetachFollowerPort {
        follower_tab_id: String,
        leadership_id: u64,
    },
    #[serde(rename_all = "camelCase")]
    InvokeOnStorageResetBegin {
        request_id: String,
        leadership_id: u64,
    },
    #[serde(rename_all = "camelCase")]
    InvokeOnSchemaBlocked {
        reason: String,
    },
    InvokeOnReconnected,
    /// onBrokerPing + optional pong. The pong decision (respondToBrokerPings
    /// can be a function) belongs to the shell; the pong must be stamped with
    /// the ping's own instance id, carried here.
    #[serde(rename_all = "camelCase")]
    HandleBrokerPing {
        broker_instance_id: String,
    },
    /// Detach listeners from and close the current port.
    DetachPort,
    /// Run the reconnect choreography: await onDemote when
    /// previous_leadership_id > 0, call onCloseFollowerPort when the previous
    /// role was follower with a leadership, connectToBroker, then feed
    /// ReconnectFinished back in.
    #[serde(rename_all = "camelCase")]
    StartReconnect {
        previous_role: Role,
        previous_leadership_id: u64,
    },
    /// Construct the typed error (code → IncompatibleBrowserBrokerConfigurationError,
    /// reconnect failures carry their cause), store it as closedError. Emitted
    /// before the waiter settlements that reference it.
    #[serde(rename_all = "camelCase")]
    CloseWithError {
        message: String,
        code: Option<String>,
        /// True when this close came from a reconnect failure: the shell uses
        /// the raw reconnect error it kept as the new error's `cause`.
        from_reconnect_failure: bool,
    },
    /// options.onClosed with the stored closedError. Fired after settlements.
    InvokeOnClosed,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TabClientSnapshot {
    pub broker_instance_id: Option<String>,
    pub role: Role,
    pub leader_tab_id: Option<String>,
    pub leadership_id: u64,
    pub closed: bool,
    pub reconnecting: bool,
}

#[derive(Debug)]
pub struct TabClientCore {
    tab_id: String,
    liveness_timeout_ms: u64,
    storage_reset_timeout_ms: u64,
    broker_instance_id: Option<String>,
    role: Role,
    leader_tab_id: Option<String>,
    leadership_id: u64,
    visibility: Visibility,
    closed: bool,
    reconnecting: bool,
    has_port: bool,
    liveness_armed: bool,
    queued_messages: Vec<TabMessage>,
    role_waiters: IndexMap<u64, Role>,
    /// (request_id, start_waiter_id, paired completion waiter to drop on start
    /// timeout — mirrors the JS removeResetWaiter in the catch path).
    reset_start_waiters: Vec<(String, u64, u64)>,
    reset_waiters: Vec<(String, u64)>,
}

impl TabClientCore {
    pub fn new(options: TabClientOptions) -> Self {
        let ping = normalize_positive_timeout(
            options.broker_ping_interval_ms,
            DEFAULT_BROKER_PING_INTERVAL_MS,
        );
        let pong = normalize_positive_timeout(
            options.broker_pong_timeout_ms,
            DEFAULT_BROKER_PONG_TIMEOUT_MS,
        );
        Self {
            tab_id: options.tab_id,
            liveness_timeout_ms: ping + pong,
            storage_reset_timeout_ms: normalize_positive_timeout(
                options.storage_reset_timeout_ms,
                DEFAULT_STORAGE_RESET_TIMEOUT_MS,
            ),
            broker_instance_id: None,
            role: Role::Follower,
            leader_tab_id: None,
            leadership_id: 0,
            visibility: Visibility::Visible,
            closed: false,
            reconnecting: false,
            has_port: false,
            liveness_armed: false,
            queued_messages: Vec::new(),
            role_waiters: IndexMap::new(),
            reset_start_waiters: Vec::new(),
            reset_waiters: Vec::new(),
        }
    }

    pub fn snapshot(&self) -> TabClientSnapshot {
        TabClientSnapshot {
            broker_instance_id: self.broker_instance_id.clone(),
            role: self.role,
            leader_tab_id: self.leader_tab_id.clone(),
            leadership_id: self.leadership_id,
            closed: self.closed,
            reconnecting: self.reconnecting,
        }
    }

    pub fn handle(&mut self, event: TabClientEvent) -> Vec<TabClientCommand> {
        let mut commands = Vec::new();
        match event {
            TabClientEvent::PortAttached => {
                self.has_port = true;
            }
            TabClientEvent::ControlMessage {
                message,
                stamped_instance_id,
            } => {
                self.handle_control_message(message, stamped_instance_id, &mut commands);
            }
            TabClientEvent::PortMessageError => {
                self.start_reconnect(&mut commands);
            }
            TabClientEvent::TimerFired { timer } => {
                self.handle_timer_fired(timer, &mut commands);
            }
            TabClientEvent::ConnectCompleted => {
                self.refresh_liveness_timer(&mut commands);
                self.flush_queued_messages(&mut commands);
            }
            TabClientEvent::RoleWaiterAdded {
                waiter_id,
                role,
                timeout_ms,
            } => {
                self.role_waiters.insert(waiter_id, role);
                commands.push(TabClientCommand::SetTimer {
                    timer: TabTimerKey::RoleWaiter { waiter_id },
                    delay_ms: timeout_ms,
                });
                // A leadership may already be in place (the shell re-checks by
                // registering unconditionally after its fast path).
                self.resolve_role_waiters(&mut commands);
            }
            TabClientEvent::StorageResetRequested {
                request_id,
                start_waiter_id,
                completion_waiter_id,
            } => {
                self.reset_start_waiters.push((
                    request_id.clone(),
                    start_waiter_id,
                    completion_waiter_id,
                ));
                self.reset_waiters
                    .push((request_id.clone(), completion_waiter_id));
                commands.push(TabClientCommand::SetTimer {
                    timer: TabTimerKey::ResetStartWaiter {
                        waiter_id: start_waiter_id,
                    },
                    delay_ms: self.storage_reset_timeout_ms,
                });
                self.send(
                    TabMessageInput::StorageResetRequest { request_id },
                    &mut commands,
                );
            }
            TabClientEvent::SendRequested { message } => {
                self.send(message, &mut commands);
            }
            TabClientEvent::VisibilityReported { visibility } => {
                self.visibility = visibility;
                self.send(TabMessageInput::Visibility { visibility }, &mut commands);
            }
            TabClientEvent::ShutdownRequested => {
                self.handle_shutdown(&mut commands);
            }
            TabClientEvent::ReconnectFinished { error } => {
                self.handle_reconnect_finished(error, &mut commands);
            }
        }
        commands
    }

    fn handle_control_message(
        &mut self,
        message: ControlMessage,
        stamped_instance_id: Option<String>,
        commands: &mut Vec<TabClientCommand>,
    ) {
        if let Some(current) = &self.broker_instance_id
            && stamped_instance_id.as_deref() != Some(current.as_str())
        {
            self.start_reconnect(commands);
            return;
        }

        match message {
            ControlMessage::BrokerHello { broker_instance_id } => {
                self.broker_instance_id = Some(broker_instance_id);
            }
            ControlMessage::BrokerPing { broker_instance_id } => {
                self.refresh_liveness_timer(commands);
                commands.push(TabClientCommand::HandleBrokerPing { broker_instance_id });
            }
            ControlMessage::BecomeLeader {
                leadership_id,
                reset_request_id,
                ..
            } => {
                self.leadership_id = leadership_id;
                commands.push(TabClientCommand::InvokeOnBecomeLeader {
                    leadership_id,
                    reset_request_id,
                });
            }
            ControlMessage::Demote { leadership_id, .. } => {
                if leadership_id == self.leadership_id {
                    self.role = Role::Follower;
                    self.leader_tab_id = None;
                    self.resolve_role_waiters(commands);
                }
                commands.push(TabClientCommand::InvokeOnDemote { leadership_id });
            }
            ControlMessage::LeaderReady {
                leader_tab_id,
                leadership_id,
                ..
            } => {
                self.leadership_id = leadership_id;
                self.role = if leader_tab_id == self.tab_id {
                    Role::Leader
                } else {
                    Role::Follower
                };
                self.leader_tab_id = Some(leader_tab_id);
                self.resolve_role_waiters(commands);
            }
            ControlMessage::AttachFollowerPort {
                follower_tab_id,
                leadership_id,
                ..
            } => {
                if leadership_id != self.leadership_id {
                    return;
                }
                commands.push(TabClientCommand::InvokeOnAttachFollowerPort {
                    follower_tab_id,
                    leadership_id,
                });
            }
            ControlMessage::UseFollowerPort {
                leader_tab_id,
                leadership_id,
                ..
            } => {
                self.leadership_id = leadership_id;
                self.leader_tab_id = Some(leader_tab_id);
                self.role = Role::Follower;
                commands.push(TabClientCommand::InvokeOnUseFollowerPort { leadership_id });
            }
            ControlMessage::FollowerReady {
                leader_tab_id,
                leadership_id,
                ..
            } => {
                self.leadership_id = leadership_id;
                self.leader_tab_id = Some(leader_tab_id);
                self.role = Role::Follower;
                commands.push(TabClientCommand::InvokeOnFollowerReady { leadership_id });
                self.resolve_role_waiters(commands);
            }
            ControlMessage::CloseFollowerPort { leadership_id, .. } => {
                commands.push(TabClientCommand::InvokeOnCloseFollowerPort { leadership_id });
            }
            ControlMessage::DetachFollowerPort {
                follower_tab_id,
                leadership_id,
                ..
            } => {
                commands.push(TabClientCommand::InvokeOnDetachFollowerPort {
                    follower_tab_id,
                    leadership_id,
                });
            }
            ControlMessage::StorageResetBegin {
                request_id,
                leadership_id,
                ..
            } => {
                self.resolve_reset_start_waiters(&request_id, commands);
                commands.push(TabClientCommand::InvokeOnStorageResetBegin {
                    request_id,
                    leadership_id,
                });
            }
            ControlMessage::StorageResetStarted { request_id, .. } => {
                self.resolve_reset_start_waiters(&request_id, commands);
            }
            ControlMessage::StorageResetFinished {
                request_id,
                success,
                error_message,
                ..
            } => {
                self.resolve_reset_start_waiters(&request_id, commands);
                self.resolve_reset_waiters(&request_id, success, error_message, commands);
            }
            ControlMessage::SchemaBlocked { reason, .. } => {
                commands.push(TabClientCommand::InvokeOnSchemaBlocked { reason });
            }
            ControlMessage::Unsupported { code, reason, .. } => {
                self.close_with_error(reason, code, false, commands);
            }
            ControlMessage::Unknown => {}
        }
    }

    fn handle_timer_fired(&mut self, timer: TabTimerKey, commands: &mut Vec<TabClientCommand>) {
        match timer {
            TabTimerKey::Liveness => {
                self.liveness_armed = false;
                self.start_reconnect(commands);
            }
            TabTimerKey::RoleWaiter { waiter_id } => {
                let Some(role) = self.role_waiters.shift_remove(&waiter_id) else {
                    return;
                };
                commands.push(TabClientCommand::SettleRoleWaiter {
                    waiter_id,
                    rejection: Some(WaiterRejection::Message {
                        message: format!("Timed out waiting for broker role {}", role_label(role)),
                    }),
                });
            }
            TabTimerKey::ResetStartWaiter { waiter_id } => {
                let Some(index) = self
                    .reset_start_waiters
                    .iter()
                    .position(|(_, start_id, _)| *start_id == waiter_id)
                else {
                    return;
                };
                let (request_id, start_id, completion_id) = self.reset_start_waiters.remove(index);
                // The JS catch path removes the completion waiter after the
                // start timeout rejects; its promise is abandoned unsettled.
                self.reset_waiters
                    .retain(|(_, waiter)| *waiter != completion_id);
                commands.push(TabClientCommand::SettleResetStartWaiters {
                    waiter_ids: vec![start_id],
                    rejection: Some(WaiterRejection::Message {
                        message: format!(
                            "Timed out waiting for browser storage reset {request_id} to start"
                        ),
                    }),
                });
            }
        }
    }

    fn send(&mut self, input: TabMessageInput, commands: &mut Vec<TabClientCommand>) {
        if self.closed {
            return;
        }
        let Some(broker_instance_id) = self.broker_instance_id.clone() else {
            return;
        };
        let message = stamp(input, broker_instance_id);
        if self.reconnecting {
            return;
        }
        if !self.has_port {
            self.queued_messages.push(message);
            return;
        }
        commands.push(TabClientCommand::PostToBroker { message });
    }

    fn flush_queued_messages(&mut self, commands: &mut Vec<TabClientCommand>) {
        if self.closed || self.reconnecting || !self.has_port {
            return;
        }
        for message in std::mem::take(&mut self.queued_messages) {
            commands.push(TabClientCommand::PostToBroker { message });
        }
    }

    fn refresh_liveness_timer(&mut self, commands: &mut Vec<TabClientCommand>) {
        if self.liveness_armed {
            commands.push(TabClientCommand::ClearTimer {
                timer: TabTimerKey::Liveness,
            });
        }
        if self.closed {
            self.liveness_armed = false;
            return;
        }
        self.liveness_armed = true;
        commands.push(TabClientCommand::SetTimer {
            timer: TabTimerKey::Liveness,
            delay_ms: self.liveness_timeout_ms,
        });
    }

    fn stop_liveness_timer(&mut self, commands: &mut Vec<TabClientCommand>) {
        if !self.liveness_armed {
            return;
        }
        self.liveness_armed = false;
        commands.push(TabClientCommand::ClearTimer {
            timer: TabTimerKey::Liveness,
        });
    }

    fn resolve_role_waiters(&mut self, commands: &mut Vec<TabClientCommand>) {
        let resolved: Vec<u64> = self
            .role_waiters
            .iter()
            .filter(|(_, role)| self.role == **role && self.leader_tab_id.is_some())
            .map(|(waiter_id, _)| *waiter_id)
            .collect();
        for waiter_id in resolved {
            self.role_waiters.shift_remove(&waiter_id);
            commands.push(TabClientCommand::ClearTimer {
                timer: TabTimerKey::RoleWaiter { waiter_id },
            });
            commands.push(TabClientCommand::SettleRoleWaiter {
                waiter_id,
                rejection: None,
            });
        }
    }

    fn reject_role_waiters(
        &mut self,
        rejection: WaiterRejection,
        commands: &mut Vec<TabClientCommand>,
    ) {
        for (waiter_id, _) in std::mem::take(&mut self.role_waiters) {
            commands.push(TabClientCommand::ClearTimer {
                timer: TabTimerKey::RoleWaiter { waiter_id },
            });
            commands.push(TabClientCommand::SettleRoleWaiter {
                waiter_id,
                rejection: Some(rejection.clone()),
            });
        }
    }

    fn resolve_reset_start_waiters(
        &mut self,
        request_id: &str,
        commands: &mut Vec<TabClientCommand>,
    ) {
        let mut waiter_ids = Vec::new();
        self.reset_start_waiters.retain(|(request, start_id, _)| {
            if request == request_id {
                waiter_ids.push(*start_id);
                false
            } else {
                true
            }
        });
        if waiter_ids.is_empty() {
            return;
        }
        for waiter_id in &waiter_ids {
            commands.push(TabClientCommand::ClearTimer {
                timer: TabTimerKey::ResetStartWaiter {
                    waiter_id: *waiter_id,
                },
            });
        }
        commands.push(TabClientCommand::SettleResetStartWaiters {
            waiter_ids,
            rejection: None,
        });
    }

    fn resolve_reset_waiters(
        &mut self,
        request_id: &str,
        success: bool,
        error_message: Option<String>,
        commands: &mut Vec<TabClientCommand>,
    ) {
        let mut waiter_ids = Vec::new();
        self.reset_waiters.retain(|(request, waiter_id)| {
            if request == request_id {
                waiter_ids.push(*waiter_id);
                false
            } else {
                true
            }
        });
        if waiter_ids.is_empty() {
            return;
        }
        let rejection = if success {
            None
        } else {
            Some(WaiterRejection::Message {
                message: error_message
                    .unwrap_or_else(|| "Browser storage reset failed".to_string()),
            })
        };
        commands.push(TabClientCommand::SettleResetWaiters {
            waiter_ids,
            rejection,
        });
    }

    fn reject_all_reset_waiters(
        &mut self,
        rejection: WaiterRejection,
        commands: &mut Vec<TabClientCommand>,
    ) {
        let start: Vec<(String, u64, u64)> = std::mem::take(&mut self.reset_start_waiters);
        if !start.is_empty() {
            for (_, waiter_id, _) in &start {
                commands.push(TabClientCommand::ClearTimer {
                    timer: TabTimerKey::ResetStartWaiter {
                        waiter_id: *waiter_id,
                    },
                });
            }
            commands.push(TabClientCommand::SettleResetStartWaiters {
                waiter_ids: start.iter().map(|(_, waiter_id, _)| *waiter_id).collect(),
                rejection: Some(rejection.clone()),
            });
        }
        let completion: Vec<(String, u64)> = std::mem::take(&mut self.reset_waiters);
        if !completion.is_empty() {
            commands.push(TabClientCommand::SettleResetWaiters {
                waiter_ids: completion.iter().map(|(_, waiter_id)| *waiter_id).collect(),
                rejection: Some(rejection),
            });
        }
    }

    fn start_reconnect(&mut self, commands: &mut Vec<TabClientCommand>) {
        if self.closed || self.reconnecting {
            return;
        }
        self.reconnecting = true;

        let previous_role = self.role;
        let previous_leadership_id = self.leadership_id;

        self.stop_liveness_timer(commands);
        self.broker_instance_id = None;
        self.role = Role::Follower;
        self.leader_tab_id = None;
        self.leadership_id = 0;
        self.queued_messages.clear();
        self.reject_all_reset_waiters(
            WaiterRejection::Message {
                message: RECONNECT_RESET_MESSAGE.to_string(),
            },
            commands,
        );

        if self.has_port {
            self.has_port = false;
            commands.push(TabClientCommand::DetachPort);
        }

        commands.push(TabClientCommand::StartReconnect {
            previous_role,
            previous_leadership_id,
        });
    }

    fn handle_reconnect_finished(
        &mut self,
        error: Option<String>,
        commands: &mut Vec<TabClientCommand>,
    ) {
        self.reconnecting = false;
        if let Some(message) = error {
            self.close_with_error(message, None, true, commands);
            return;
        }
        if self.closed {
            return;
        }
        self.send(
            TabMessageInput::Visibility {
                visibility: self.visibility,
            },
            commands,
        );
        commands.push(TabClientCommand::InvokeOnReconnected);
        self.flush_queued_messages(commands);
    }

    fn handle_shutdown(&mut self, commands: &mut Vec<TabClientCommand>) {
        if self.closed {
            return;
        }
        // The JS stamps the shutdown message before flipping `closed` and
        // posts it directly, bypassing send(); no instance id → no post.
        let shutdown_message = self
            .broker_instance_id
            .clone()
            .map(|broker_instance_id| TabMessage::Shutdown { broker_instance_id });
        self.closed = true;
        self.stop_liveness_timer(commands);
        self.queued_messages.clear();
        if let Some(message) = shutdown_message
            && self.has_port
        {
            commands.push(TabClientCommand::PostToBroker { message });
        }
        if self.has_port {
            self.has_port = false;
            commands.push(TabClientCommand::DetachPort);
        }
        let rejection = WaiterRejection::Message {
            message: CLIENT_CLOSED_MESSAGE.to_string(),
        };
        self.reject_role_waiters(rejection.clone(), commands);
        self.reject_all_reset_waiters(rejection, commands);
    }

    fn close_with_error(
        &mut self,
        message: String,
        code: Option<String>,
        from_reconnect_failure: bool,
        commands: &mut Vec<TabClientCommand>,
    ) {
        self.closed = true;
        commands.push(TabClientCommand::CloseWithError {
            message,
            code,
            from_reconnect_failure,
        });
        self.stop_liveness_timer(commands);
        self.queued_messages.clear();
        if self.has_port {
            self.has_port = false;
            commands.push(TabClientCommand::DetachPort);
        }
        self.reject_role_waiters(WaiterRejection::ClosedError, commands);
        self.reject_all_reset_waiters(WaiterRejection::ClosedError, commands);
        commands.push(TabClientCommand::InvokeOnClosed);
    }
}

fn role_label(role: Role) -> &'static str {
    match role {
        Role::Leader => "leader",
        Role::Follower => "follower",
    }
}
