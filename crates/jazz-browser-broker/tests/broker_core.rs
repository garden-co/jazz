use jazz_browser_broker::protocol::{ControlMessage, TabMessage, Visibility};
use jazz_browser_broker::{
    BrokerCommand, BrokerCore, BrokerEvent, MonitorId, PortId, ProbeId, TimerKey,
};

fn hello(tab_id: &str) -> TabMessage {
    hello_with_options(tab_id, Visibility::Visible, None, None, None)
}

fn hello_with_options(
    tab_id: &str,
    visibility: Visibility,
    force_takeover_timeout_ms: Option<f64>,
    broker_ping_interval_ms: Option<f64>,
    broker_pong_timeout_ms: Option<f64>,
) -> TabMessage {
    TabMessage::Hello {
        tab_id: tab_id.to_string(),
        app_id: "app".to_string(),
        db_name: "db".to_string(),
        fingerprint: "fingerprint".to_string(),
        visibility,
        force_takeover_timeout_ms,
        broker_ping_interval_ms,
        broker_pong_timeout_ms,
    }
}

#[test]
fn first_hello_latches_namespace_starts_liveness_and_promotes_the_tab() {
    let mut broker = BrokerCore::new("broker-a".to_string());

    let commands = broker.handle(
        BrokerEvent::PortMessage {
            port_id: PortId(1),
            message: hello("tab-a"),
        },
        10,
    );

    assert_eq!(
        commands,
        vec![
            BrokerCommand::Post {
                port_id: PortId(1),
                message: ControlMessage::BrokerPing {
                    broker_instance_id: "broker-a".to_string(),
                },
            },
            BrokerCommand::SetTimer {
                timer: TimerKey::BrokerPing,
                delay_ms: 1_000,
            },
            BrokerCommand::Post {
                port_id: PortId(1),
                message: ControlMessage::BrokerHello {
                    broker_instance_id: "broker-a".to_string(),
                },
            },
            BrokerCommand::Post {
                port_id: PortId(1),
                message: ControlMessage::BecomeLeader {
                    broker_instance_id: "broker-a".to_string(),
                    leadership_id: 1,
                    reset_request_id: None,
                },
            },
        ]
    );
}

#[test]
fn leader_ready_announces_monitors_and_assigns_followers_in_insertion_order() {
    let mut broker = BrokerCore::new("broker-a".to_string());
    broker.handle(
        BrokerEvent::PortMessage {
            port_id: PortId(1),
            message: hello("tab-a"),
        },
        10,
    );
    assert_eq!(
        broker.handle(
            BrokerEvent::PortMessage {
                port_id: PortId(2),
                message: hello("tab-b"),
            },
            20,
        ),
        vec![BrokerCommand::Post {
            port_id: PortId(2),
            message: ControlMessage::BrokerHello {
                broker_instance_id: "broker-a".to_string(),
            },
        }]
    );

    let commands = broker.handle(
        BrokerEvent::PortMessage {
            port_id: PortId(1),
            message: TabMessage::LeaderReady {
                broker_instance_id: "broker-a".to_string(),
                leadership_id: 1,
                tab_lock_name: "tab-lock".to_string(),
                worker_lock_name: "worker-lock".to_string(),
                bridgeless_storage_reset: false,
            },
        },
        30,
    );

    assert_eq!(
        commands,
        vec![
            BrokerCommand::Post {
                port_id: PortId(1),
                message: ControlMessage::LeaderReady {
                    broker_instance_id: "broker-a".to_string(),
                    leader_tab_id: "tab-a".to_string(),
                    leadership_id: 1,
                },
            },
            BrokerCommand::Post {
                port_id: PortId(2),
                message: ControlMessage::LeaderReady {
                    broker_instance_id: "broker-a".to_string(),
                    leader_tab_id: "tab-a".to_string(),
                    leadership_id: 1,
                },
            },
            BrokerCommand::MonitorLock {
                monitor_id: MonitorId(1),
                lock_name: "tab-lock".to_string(),
            },
            BrokerCommand::MonitorLock {
                monitor_id: MonitorId(2),
                lock_name: "worker-lock".to_string(),
            },
            BrokerCommand::SetTimer {
                timer: TimerKey::FollowerAttachment {
                    leadership_id: 1,
                    follower_tab_id: "tab-b".to_string(),
                },
                delay_ms: 1_000,
            },
            BrokerCommand::AttachFollowerChannel {
                leader_port_id: PortId(1),
                follower_port_id: PortId(2),
                leader_tab_id: "tab-a".to_string(),
                follower_tab_id: "tab-b".to_string(),
                leadership_id: 1,
            },
        ]
    );

    let commands = broker.handle(
        BrokerEvent::PortMessage {
            port_id: PortId(1),
            message: TabMessage::FollowerPortAttached {
                broker_instance_id: "broker-a".to_string(),
                leadership_id: 1,
                follower_tab_id: "tab-b".to_string(),
            },
        },
        40,
    );

    assert_eq!(
        commands,
        vec![
            BrokerCommand::ClearTimer {
                timer: TimerKey::FollowerAttachment {
                    leadership_id: 1,
                    follower_tab_id: "tab-b".to_string(),
                },
            },
            BrokerCommand::Post {
                port_id: PortId(2),
                message: ControlMessage::FollowerReady {
                    broker_instance_id: "broker-a".to_string(),
                    leader_tab_id: "tab-a".to_string(),
                    leadership_id: 1,
                },
            },
        ]
    );
}

#[test]
fn incompatible_hello_gets_unsupported_and_does_not_join_the_namespace() {
    let mut broker = BrokerCore::new("broker-a".to_string());
    broker.handle(
        BrokerEvent::PortMessage {
            port_id: PortId(1),
            message: hello("tab-a"),
        },
        10,
    );

    let commands = broker.handle(
        BrokerEvent::PortMessage {
            port_id: PortId(2),
            message: TabMessage::Hello {
                tab_id: "tab-b".to_string(),
                app_id: "app".to_string(),
                db_name: "db".to_string(),
                fingerprint: "other-fingerprint".to_string(),
                visibility: Visibility::Visible,
                force_takeover_timeout_ms: None,
                broker_ping_interval_ms: None,
                broker_pong_timeout_ms: None,
            },
        },
        20,
    );

    assert_eq!(
        commands,
        vec![
            BrokerCommand::Post {
                port_id: PortId(2),
                message: ControlMessage::Unsupported {
                    broker_instance_id: "broker-a".to_string(),
                    code: Some("incompatible-browser-broker-configuration".to_string()),
                    reason: "incompatible persistent browser configuration".to_string(),
                },
            },
            BrokerCommand::ClosePort { port_id: PortId(2) },
        ]
    );

    assert!(
        broker
            .handle(
                BrokerEvent::PortMessage {
                    port_id: PortId(2),
                    message: TabMessage::Visibility {
                        broker_instance_id: "broker-a".to_string(),
                        visibility: Visibility::Hidden,
                    },
                },
                30,
            )
            .is_empty()
    );
}

#[test]
fn stale_instance_drops_warn_once_without_applying_the_message() {
    let mut broker = BrokerCore::new("broker-a".to_string());
    broker.handle(
        BrokerEvent::PortMessage {
            port_id: PortId(1),
            message: hello("tab-a"),
        },
        10,
    );

    assert_eq!(
        broker.handle(
            BrokerEvent::PortMessage {
                port_id: PortId(1),
                message: TabMessage::Visibility {
                    broker_instance_id: "broker-old".to_string(),
                    visibility: Visibility::Hidden,
                },
            },
            20,
        ),
        vec![BrokerCommand::WarnStaleInstanceDrop {
            message_type: "visibility".to_string(),
            tab_id: "tab-a".to_string(),
            stamped_instance_id: "broker-old".to_string(),
        }]
    );

    assert!(
        broker
            .handle(
                BrokerEvent::PortMessage {
                    port_id: PortId(1),
                    message: TabMessage::SchemaReady {
                        broker_instance_id: "broker-old".to_string(),
                        schema_fingerprint: "schema-a".to_string(),
                    },
                },
                30,
            )
            .is_empty()
    );
}

#[test]
fn broker_ping_timer_preserves_the_double_ping_boundary_quirk() {
    let mut broker = BrokerCore::new("broker-a".to_string());
    broker.handle(
        BrokerEvent::PortMessage {
            port_id: PortId(1),
            message: hello_with_options("tab-a", Visibility::Visible, None, Some(5.0), Some(10.0)),
        },
        0,
    );

    let commands = broker.handle(
        BrokerEvent::TimerFired {
            timer: TimerKey::BrokerPing,
        },
        10,
    );

    assert_eq!(
        commands,
        vec![
            BrokerCommand::Post {
                port_id: PortId(1),
                message: ControlMessage::BrokerPing {
                    broker_instance_id: "broker-a".to_string(),
                },
            },
            BrokerCommand::Post {
                port_id: PortId(1),
                message: ControlMessage::BrokerPing {
                    broker_instance_id: "broker-a".to_string(),
                },
            },
            BrokerCommand::SetTimer {
                timer: TimerKey::BrokerPing,
                delay_ms: 5,
            },
        ]
    );
}

#[test]
fn storage_reset_promotes_a_reset_leader_after_all_participants_prepare() {
    let mut broker = BrokerCore::new("broker-a".to_string());
    broker.handle(
        BrokerEvent::PortMessage {
            port_id: PortId(1),
            message: hello("tab-a"),
        },
        10,
    );
    broker.handle(
        BrokerEvent::PortMessage {
            port_id: PortId(1),
            message: TabMessage::LeaderReady {
                broker_instance_id: "broker-a".to_string(),
                leadership_id: 1,
                tab_lock_name: "tab-lock".to_string(),
                worker_lock_name: "worker-lock".to_string(),
                bridgeless_storage_reset: false,
            },
        },
        20,
    );

    assert_eq!(
        broker.handle(
            BrokerEvent::PortMessage {
                port_id: PortId(1),
                message: TabMessage::StorageResetRequest {
                    broker_instance_id: "broker-a".to_string(),
                    request_id: "reset-a".to_string(),
                },
            },
            30,
        ),
        vec![
            BrokerCommand::CancelLockMonitor {
                monitor_id: MonitorId(1),
            },
            BrokerCommand::CancelLockMonitor {
                monitor_id: MonitorId(2),
            },
            BrokerCommand::Post {
                port_id: PortId(1),
                message: ControlMessage::StorageResetStarted {
                    broker_instance_id: "broker-a".to_string(),
                    request_id: "reset-a".to_string(),
                },
            },
            BrokerCommand::Post {
                port_id: PortId(1),
                message: ControlMessage::StorageResetBegin {
                    broker_instance_id: "broker-a".to_string(),
                    request_id: "reset-a".to_string(),
                    leadership_id: 1,
                },
            },
        ]
    );

    assert_eq!(
        broker.handle(
            BrokerEvent::PortMessage {
                port_id: PortId(1),
                message: TabMessage::StorageResetReady {
                    broker_instance_id: "broker-a".to_string(),
                    request_id: "reset-a".to_string(),
                    success: true,
                    error_message: None,
                },
            },
            40,
        ),
        vec![BrokerCommand::ProbeLocks {
            probe_id: ProbeId(1),
            lock_names: vec!["tab-lock".to_string(), "worker-lock".to_string()],
        }]
    );

    assert_eq!(
        broker.handle(
            BrokerEvent::LocksProbeResult {
                probe_id: ProbeId(1),
                all_acquired: true,
            },
            50,
        ),
        vec![BrokerCommand::Post {
            port_id: PortId(1),
            message: ControlMessage::BecomeLeader {
                broker_instance_id: "broker-a".to_string(),
                leadership_id: 2,
                reset_request_id: Some("reset-a".to_string()),
            },
        }]
    );
}

#[test]
fn broker_command_serde_uses_camel_case_shell_fields() {
    let value = serde_json::to_value(BrokerCommand::SetTimer {
        timer: TimerKey::FollowerAttachment {
            leadership_id: 7,
            follower_tab_id: "tab-b".to_string(),
        },
        delay_ms: 1_000,
    })
    .expect("command should serialize");

    assert_eq!(
        value,
        serde_json::json!({
            "kind": "setTimer",
            "timer": {
                "kind": "followerAttachment",
                "leadershipId": 7,
                "followerTabId": "tab-b",
            },
            "delayMs": 1_000,
        })
    );
}

// ---------------------------------------------------------------------------
// Scenario helpers
// ---------------------------------------------------------------------------

const BROKER: &str = "broker-a";

fn new_broker() -> BrokerCore {
    BrokerCore::new(BROKER.to_string())
}

fn connect(broker: &mut BrokerCore, port: u64, tab: &str, now: i64) -> Vec<BrokerCommand> {
    broker.handle(
        BrokerEvent::PortMessage {
            port_id: PortId(port),
            message: hello(tab),
        },
        now,
    )
}

fn send(broker: &mut BrokerCore, port: u64, message: TabMessage, now: i64) -> Vec<BrokerCommand> {
    broker.handle(
        BrokerEvent::PortMessage {
            port_id: PortId(port),
            message,
        },
        now,
    )
}

fn leader_ready(leadership_id: u64) -> TabMessage {
    TabMessage::LeaderReady {
        broker_instance_id: BROKER.to_string(),
        leadership_id,
        tab_lock_name: "tab-lock".to_string(),
        worker_lock_name: "worker-lock".to_string(),
        bridgeless_storage_reset: false,
    }
}

fn schema_ready(fingerprint: &str) -> TabMessage {
    TabMessage::SchemaReady {
        broker_instance_id: BROKER.to_string(),
        schema_fingerprint: fingerprint.to_string(),
    }
}

fn storage_reset_request(request_id: &str) -> TabMessage {
    TabMessage::StorageResetRequest {
        broker_instance_id: BROKER.to_string(),
        request_id: request_id.to_string(),
    }
}

fn storage_reset_ready(request_id: &str, success: bool, error: Option<&str>) -> TabMessage {
    TabMessage::StorageResetReady {
        broker_instance_id: BROKER.to_string(),
        request_id: request_id.to_string(),
        success,
        error_message: error.map(str::to_string),
    }
}

fn shutdown() -> TabMessage {
    TabMessage::Shutdown {
        broker_instance_id: BROKER.to_string(),
    }
}

fn broker_pong() -> TabMessage {
    TabMessage::BrokerPong {
        broker_instance_id: BROKER.to_string(),
    }
}

fn become_leader_posts(commands: &[BrokerCommand]) -> Vec<(u64, u64, Option<String>)> {
    commands
        .iter()
        .filter_map(|command| match command {
            BrokerCommand::Post {
                port_id,
                message:
                    ControlMessage::BecomeLeader {
                        leadership_id,
                        reset_request_id,
                        ..
                    },
            } => Some((port_id.0, *leadership_id, reset_request_id.clone())),
            _ => None,
        })
        .collect()
}

fn reset_finished_posts(commands: &[BrokerCommand]) -> Vec<(u64, String, bool, Option<String>)> {
    commands
        .iter()
        .filter_map(|command| match command {
            BrokerCommand::Post {
                port_id,
                message:
                    ControlMessage::StorageResetFinished {
                        request_id,
                        success,
                        error_message,
                        ..
                    },
            } => Some((
                port_id.0,
                request_id.clone(),
                *success,
                error_message.clone(),
            )),
            _ => None,
        })
        .collect()
}

fn demote_posts(commands: &[BrokerCommand]) -> Vec<(u64, u64)> {
    commands
        .iter()
        .filter_map(|command| match command {
            BrokerCommand::Post {
                port_id,
                message: ControlMessage::Demote { leadership_id, .. },
            } => Some((port_id.0, *leadership_id)),
            _ => None,
        })
        .collect()
}

/// Boot one ready leader (tab-a on port 1, leadership 1) plus an attached
/// follower (tab-b on port 2).
fn boot_leader_and_attached_follower(broker: &mut BrokerCore) {
    connect(broker, 1, "tab-a", 0);
    send(broker, 1, leader_ready(1), 10);
    connect(broker, 2, "tab-b", 20);
    send(
        broker,
        1,
        TabMessage::FollowerPortAttached {
            broker_instance_id: BROKER.to_string(),
            leadership_id: 1,
            follower_tab_id: "tab-b".to_string(),
        },
        30,
    );
}

// ---------------------------------------------------------------------------
// Election and leadership
// ---------------------------------------------------------------------------

#[test]
fn foreign_leader_ready_gets_a_targeted_demote() {
    let mut broker = new_broker();
    connect(&mut broker, 1, "tab-a", 10);
    connect(&mut broker, 2, "tab-b", 20);

    // tab-b claims a leadership it was never granted.
    let commands = send(&mut broker, 2, leader_ready(9), 30);

    assert_eq!(
        commands,
        vec![BrokerCommand::Post {
            port_id: PortId(2),
            message: ControlMessage::Demote {
                broker_instance_id: BROKER.to_string(),
                leadership_id: 9,
            },
        }]
    );
}

#[test]
fn schema_ready_keeps_a_pending_leader_that_holds_the_canonical_fingerprint() {
    let mut broker = new_broker();
    connect(&mut broker, 1, "tab-a", 10);

    // tab-a is the not-ready leader; reporting the first fingerprint latches it
    // as canonical and must not restart the election.
    let commands = send(&mut broker, 1, schema_ready("schema-1"), 20);

    assert!(become_leader_posts(&commands).is_empty());
    assert!(demote_posts(&commands).is_empty());
}

#[test]
fn lock_monitor_loss_runs_demote_takeover_steal_and_reelection() {
    let mut broker = new_broker();
    connect(&mut broker, 1, "tab-a", 0);
    connect(&mut broker, 2, "tab-b", 5);
    send(&mut broker, 1, leader_ready(1), 10);

    let commands = broker.handle(
        BrokerEvent::LockMonitorTriggered {
            monitor_id: MonitorId(1),
        },
        40,
    );
    assert_eq!(
        commands,
        vec![
            BrokerCommand::CancelLockMonitor {
                monitor_id: MonitorId(1),
            },
            BrokerCommand::CancelLockMonitor {
                monitor_id: MonitorId(2),
            },
            BrokerCommand::ClearTimer {
                timer: TimerKey::FollowerAttachment {
                    leadership_id: 1,
                    follower_tab_id: "tab-b".to_string(),
                },
            },
            BrokerCommand::Post {
                port_id: PortId(1),
                message: ControlMessage::Demote {
                    broker_instance_id: BROKER.to_string(),
                    leadership_id: 1,
                },
            },
            BrokerCommand::Post {
                port_id: PortId(2),
                message: ControlMessage::CloseFollowerPort {
                    broker_instance_id: BROKER.to_string(),
                    leadership_id: 1,
                },
            },
            BrokerCommand::ProbeLocks {
                probe_id: ProbeId(1),
                lock_names: vec!["tab-lock".to_string(), "worker-lock".to_string()],
            },
        ]
    );

    // Locks still held by the crashed leader: sleep, then steal, then elect.
    assert_eq!(
        broker.handle(
            BrokerEvent::LocksProbeResult {
                probe_id: ProbeId(1),
                all_acquired: false,
            },
            50,
        ),
        vec![BrokerCommand::SetTimer {
            timer: TimerKey::ForceTakeoverSleep {
                probe_id: ProbeId(1),
            },
            delay_ms: 1_000,
        }]
    );
    assert_eq!(
        broker.handle(
            BrokerEvent::TimerFired {
                timer: TimerKey::ForceTakeoverSleep {
                    probe_id: ProbeId(1),
                },
            },
            1_050,
        ),
        vec![BrokerCommand::StealLocks {
            probe_id: ProbeId(1),
            lock_names: vec!["tab-lock".to_string(), "worker-lock".to_string()],
        }]
    );
    assert_eq!(
        become_leader_posts(&broker.handle(
            BrokerEvent::LocksStolen {
                probe_id: ProbeId(1),
            },
            1_060,
        )),
        vec![(2, 2, None)]
    );
}

#[test]
fn failed_leader_sits_out_backoff_and_is_reelected_by_the_retry_timer() {
    let mut broker = new_broker();
    connect(&mut broker, 1, "tab-a", 10);

    // Sole tab fails leadership at t=30: backoff until t=1030.
    let commands = send(
        &mut broker,
        1,
        TabMessage::LeaderFailed {
            broker_instance_id: BROKER.to_string(),
            leadership_id: 1,
            reason: "boom".to_string(),
        },
        30,
    );
    assert_eq!(demote_posts(&commands), vec![(1, 1)]);
    assert!(matches!(
        commands.last(),
        Some(BrokerCommand::ProbeLocks { .. })
    ));

    // Takeover completes but the only candidate is still in backoff: the
    // retry timer must be armed with the remaining delay, not re-elect now.
    assert_eq!(
        broker.handle(
            BrokerEvent::LocksProbeResult {
                probe_id: ProbeId(1),
                all_acquired: true,
            },
            40,
        ),
        vec![BrokerCommand::SetTimer {
            timer: TimerKey::LeaderFailureRetry,
            delay_ms: 990,
        }]
    );

    // Once the backoff expires the retry election promotes the tab again.
    assert_eq!(
        become_leader_posts(&broker.handle(
            BrokerEvent::TimerFired {
                timer: TimerKey::LeaderFailureRetry,
            },
            1_030,
        )),
        vec![(1, 2, None)]
    );
}

#[test]
fn re_hello_of_the_ready_leader_steps_it_down_and_reelects() {
    let mut broker = new_broker();
    connect(&mut broker, 1, "tab-a", 0);
    send(&mut broker, 1, leader_ready(1), 10);
    connect(&mut broker, 2, "tab-b", 20);

    // Same tab id arrives on a new port (e.g. a reconnect).
    let commands = connect(&mut broker, 3, "tab-a", 30);

    assert_eq!(
        commands,
        vec![
            BrokerCommand::ClosePort { port_id: PortId(1) },
            BrokerCommand::Post {
                port_id: PortId(3),
                message: ControlMessage::BrokerHello {
                    broker_instance_id: BROKER.to_string(),
                },
            },
            BrokerCommand::CancelLockMonitor {
                monitor_id: MonitorId(1),
            },
            BrokerCommand::CancelLockMonitor {
                monitor_id: MonitorId(2),
            },
            BrokerCommand::ClearTimer {
                timer: TimerKey::FollowerAttachment {
                    leadership_id: 1,
                    follower_tab_id: "tab-b".to_string(),
                },
            },
            BrokerCommand::Post {
                port_id: PortId(2),
                message: ControlMessage::CloseFollowerPort {
                    broker_instance_id: BROKER.to_string(),
                    leadership_id: 1,
                },
            },
            BrokerCommand::Post {
                port_id: PortId(3),
                message: ControlMessage::BecomeLeader {
                    broker_instance_id: BROKER.to_string(),
                    leadership_id: 2,
                    reset_request_id: None,
                },
            },
        ]
    );
}

#[test]
fn hello_to_a_ready_leader_gets_leader_ready_and_a_follower_channel() {
    let mut broker = new_broker();
    connect(&mut broker, 1, "tab-a", 0);
    send(&mut broker, 1, leader_ready(1), 10);

    let commands = connect(&mut broker, 2, "tab-b", 20);

    assert_eq!(
        commands,
        vec![
            BrokerCommand::Post {
                port_id: PortId(2),
                message: ControlMessage::BrokerHello {
                    broker_instance_id: BROKER.to_string(),
                },
            },
            BrokerCommand::Post {
                port_id: PortId(2),
                message: ControlMessage::LeaderReady {
                    broker_instance_id: BROKER.to_string(),
                    leader_tab_id: "tab-a".to_string(),
                    leadership_id: 1,
                },
            },
            BrokerCommand::SetTimer {
                timer: TimerKey::FollowerAttachment {
                    leadership_id: 1,
                    follower_tab_id: "tab-b".to_string(),
                },
                delay_ms: 1_000,
            },
            BrokerCommand::AttachFollowerChannel {
                leader_port_id: PortId(1),
                follower_port_id: PortId(2),
                leader_tab_id: "tab-a".to_string(),
                follower_tab_id: "tab-b".to_string(),
                leadership_id: 1,
            },
        ]
    );
}

// ---------------------------------------------------------------------------
// Liveness
// ---------------------------------------------------------------------------

#[test]
fn pong_timeout_is_strict_and_evicts_the_follower_on_the_visibility_sweep() {
    let mut broker = new_broker();
    boot_leader_and_attached_follower(&mut broker);
    send(&mut broker, 1, broker_pong(), 100);

    // tab-b last ponged at its hello (t=20). Exactly at the 3000ms boundary it
    // survives (strictly-greater check)…
    let commands = send(
        &mut broker,
        1,
        TabMessage::Visibility {
            broker_instance_id: BROKER.to_string(),
            visibility: Visibility::Visible,
        },
        3_020,
    );
    assert!(commands.is_empty());

    // …one millisecond later it is evicted: leader told to detach, port closed.
    let commands = send(
        &mut broker,
        1,
        TabMessage::Visibility {
            broker_instance_id: BROKER.to_string(),
            visibility: Visibility::Visible,
        },
        3_021,
    );
    assert_eq!(
        commands,
        vec![
            BrokerCommand::Post {
                port_id: PortId(1),
                message: ControlMessage::DetachFollowerPort {
                    broker_instance_id: BROKER.to_string(),
                    follower_tab_id: "tab-b".to_string(),
                    leadership_id: 1,
                },
            },
            BrokerCommand::ClosePort { port_id: PortId(2) },
        ]
    );
}

// ---------------------------------------------------------------------------
// Schema fingerprints
// ---------------------------------------------------------------------------

#[test]
fn first_schema_report_elects_a_canonical_holder_over_a_fingerprint_less_leader() {
    let mut broker = new_broker();
    connect(&mut broker, 1, "tab-a", 0);
    connect(&mut broker, 2, "tab-b", 5);

    // tab-b latches the canonical fingerprint first: the fingerprint-less
    // pending leader tab-a is demoted and tab-b promoted straight away.
    let commands = send(&mut broker, 2, schema_ready("schema-1"), 10);

    assert_eq!(demote_posts(&commands), vec![(1, 1)]);
    assert_eq!(become_leader_posts(&commands), vec![(2, 2, None)]);
}

#[test]
fn mismatching_schema_gets_blocked_and_a_mismatching_leader_is_replaced() {
    let mut broker = new_broker();
    connect(&mut broker, 1, "tab-a", 0);
    send(&mut broker, 1, schema_ready("schema-1"), 5);
    send(&mut broker, 1, leader_ready(1), 10);
    connect(&mut broker, 2, "tab-b", 20);

    // The ready leader re-reports a non-canonical fingerprint: it is blocked,
    // demoted, and a replacement takeover starts on its locks.
    let commands = send(&mut broker, 1, schema_ready("schema-2"), 30);
    assert_eq!(
        commands,
        vec![
            BrokerCommand::Post {
                port_id: PortId(1),
                message: ControlMessage::SchemaBlocked {
                    broker_instance_id: BROKER.to_string(),
                    reason: "incompatible persistent browser schema".to_string(),
                },
            },
            BrokerCommand::CancelLockMonitor {
                monitor_id: MonitorId(1),
            },
            BrokerCommand::CancelLockMonitor {
                monitor_id: MonitorId(2),
            },
            BrokerCommand::Post {
                port_id: PortId(1),
                message: ControlMessage::Demote {
                    broker_instance_id: BROKER.to_string(),
                    leadership_id: 1,
                },
            },
            BrokerCommand::Post {
                port_id: PortId(2),
                message: ControlMessage::CloseFollowerPort {
                    broker_instance_id: BROKER.to_string(),
                    leadership_id: 1,
                },
            },
            BrokerCommand::ProbeLocks {
                probe_id: ProbeId(1),
                lock_names: vec!["tab-lock".to_string(), "worker-lock".to_string()],
            },
        ]
    );

    // Takeover completes but no tab holds the canonical fingerprint: nothing
    // to elect until one reports it.
    let commands = broker.handle(
        BrokerEvent::LocksProbeResult {
            probe_id: ProbeId(1),
            all_acquired: true,
        },
        40,
    );
    assert!(commands.is_empty());

    // tab-b reporting the canonical fingerprint recovers the namespace.
    let commands = send(&mut broker, 2, schema_ready("schema-1"), 50);
    assert_eq!(become_leader_posts(&commands), vec![(2, 2, None)]);
}

#[test]
fn departure_of_the_last_canonical_holder_adopts_the_blocked_fingerprint() {
    let mut broker = new_broker();
    connect(&mut broker, 1, "tab-a", 0);
    send(&mut broker, 1, schema_ready("schema-1"), 5);
    send(&mut broker, 1, leader_ready(1), 10);
    connect(&mut broker, 2, "tab-b", 20);
    send(&mut broker, 2, schema_ready("schema-2"), 25);

    // The only holder of the canonical fingerprint leaves: the namespace
    // re-elects schema-2 so the blocked tab can recover without a reload.
    let commands = send(&mut broker, 1, shutdown(), 40);
    assert!(matches!(
        commands.last(),
        Some(BrokerCommand::ProbeLocks { .. })
    ));

    let commands = broker.handle(
        BrokerEvent::LocksProbeResult {
            probe_id: ProbeId(1),
            all_acquired: true,
        },
        50,
    );
    assert_eq!(become_leader_posts(&commands), vec![(2, 2, None)]);
}

// ---------------------------------------------------------------------------
// Follower attachment retries
// ---------------------------------------------------------------------------

#[test]
fn attachment_timeouts_back_off_exponentially_and_reset_when_the_key_clears() {
    let mut broker = new_broker();
    connect(&mut broker, 1, "tab-a", 0);
    send(&mut broker, 1, leader_ready(1), 10);
    connect(&mut broker, 2, "tab-b", 20);

    let timer = TimerKey::FollowerAttachment {
        leadership_id: 1,
        follower_tab_id: "tab-b".to_string(),
    };

    // First timeout: retry count 1 → 2000ms.
    let commands = broker.handle(
        BrokerEvent::TimerFired {
            timer: timer.clone(),
        },
        1_020,
    );
    assert!(commands.contains(&BrokerCommand::SetTimer {
        timer: timer.clone(),
        delay_ms: 2_000,
    }));

    // Second timeout: retry count 2 → 4000ms.
    let commands = broker.handle(
        BrokerEvent::TimerFired {
            timer: timer.clone(),
        },
        3_020,
    );
    assert!(commands.contains(&BrokerCommand::SetTimer {
        timer: timer.clone(),
        delay_ms: 4_000,
    }));

    // A successful attach clears the retry count…
    send(
        &mut broker,
        1,
        TabMessage::FollowerPortAttached {
            broker_instance_id: BROKER.to_string(),
            leadership_id: 1,
            follower_tab_id: "tab-b".to_string(),
        },
        3_030,
    );
    // …so when the leader reports the port closed, reassignment starts back at
    // the initial 1000ms timeout.
    let commands = send(
        &mut broker,
        1,
        TabMessage::FollowerPortClosed {
            broker_instance_id: BROKER.to_string(),
            leadership_id: 1,
            follower_tab_id: "tab-b".to_string(),
        },
        3_040,
    );
    assert!(commands.contains(&BrokerCommand::SetTimer {
        timer,
        delay_ms: 1_000,
    }));
}

// ---------------------------------------------------------------------------
// Storage reset
// ---------------------------------------------------------------------------

#[test]
fn bridgeless_fresh_namespace_reset_steps_the_placeholder_leader_down() {
    let mut broker = new_broker();
    connect(&mut broker, 1, "tab-a", 0);
    send(&mut broker, 1, leader_ready(1), 10);
    send(&mut broker, 1, storage_reset_request("reset-1"), 20);
    send(
        &mut broker,
        1,
        storage_reset_ready("reset-1", true, None),
        30,
    );
    let commands = broker.handle(
        BrokerEvent::LocksProbeResult {
            probe_id: ProbeId(1),
            all_acquired: true,
        },
        40,
    );
    assert_eq!(
        become_leader_posts(&commands),
        vec![(1, 2, Some("reset-1".to_string()))]
    );

    // Fresh namespace: no schema fingerprint was ever reported, and the
    // promoted leader declares it has no client to rebuild a bridge from.
    let commands = send(
        &mut broker,
        1,
        TabMessage::LeaderReady {
            broker_instance_id: BROKER.to_string(),
            leadership_id: 2,
            tab_lock_name: "tab-lock".to_string(),
            worker_lock_name: "worker-lock".to_string(),
            bridgeless_storage_reset: true,
        },
        50,
    );
    assert_eq!(demote_posts(&commands), vec![(1, 2)]);
    assert_eq!(
        reset_finished_posts(&commands),
        vec![(1, "reset-1".to_string(), true, None)]
    );
}

#[test]
fn duplicate_reset_requests_join_and_settle_together() {
    let mut broker = new_broker();
    boot_leader_and_attached_follower(&mut broker);

    send(&mut broker, 1, storage_reset_request("reset-1"), 40);
    // Second requester while the reset is active: acknowledged, no new reset.
    let commands = send(&mut broker, 2, storage_reset_request("reset-2"), 45);
    assert_eq!(
        commands,
        vec![BrokerCommand::Post {
            port_id: PortId(2),
            message: ControlMessage::StorageResetStarted {
                broker_instance_id: BROKER.to_string(),
                request_id: "reset-2".to_string(),
            },
        }]
    );

    send(
        &mut broker,
        1,
        storage_reset_ready("reset-1", true, None),
        50,
    );
    send(
        &mut broker,
        2,
        storage_reset_ready("reset-1", true, None),
        55,
    );
    let commands = broker.handle(
        BrokerEvent::LocksProbeResult {
            probe_id: ProbeId(1),
            all_acquired: true,
        },
        60,
    );
    let promotions = become_leader_posts(&commands);
    assert_eq!(promotions, vec![(2, 2, Some("reset-1".to_string()))]);

    // Promoted leader (tab-b, no schema fingerprint) finishes bridgelessly:
    // both request ids settle, broadcast to both tabs.
    let commands = send(
        &mut broker,
        2,
        TabMessage::LeaderReady {
            broker_instance_id: BROKER.to_string(),
            leadership_id: 2,
            tab_lock_name: "tab-lock".to_string(),
            worker_lock_name: "worker-lock".to_string(),
            bridgeless_storage_reset: true,
        },
        70,
    );
    assert_eq!(
        reset_finished_posts(&commands),
        vec![
            (1, "reset-1".to_string(), true, None),
            (1, "reset-2".to_string(), true, None),
            (2, "reset-1".to_string(), true, None),
            (2, "reset-2".to_string(), true, None),
        ]
    );
}

#[test]
fn failed_preparation_fails_the_reset_with_joined_errors_and_reelects() {
    let mut broker = new_broker();
    boot_leader_and_attached_follower(&mut broker);

    send(&mut broker, 1, storage_reset_request("reset-1"), 40);
    send(
        &mut broker,
        1,
        storage_reset_ready("reset-1", true, None),
        50,
    );
    send(
        &mut broker,
        2,
        storage_reset_ready("reset-1", false, Some("disk locked")),
        55,
    );
    let commands = broker.handle(
        BrokerEvent::LocksProbeResult {
            probe_id: ProbeId(1),
            all_acquired: true,
        },
        60,
    );

    assert_eq!(
        reset_finished_posts(&commands),
        vec![
            (
                1,
                "reset-1".to_string(),
                false,
                Some("disk locked".to_string())
            ),
            (
                2,
                "reset-1".to_string(),
                false,
                Some("disk locked".to_string())
            ),
        ]
    );
    // A failed reset immediately re-elects.
    assert_eq!(become_leader_posts(&commands).len(), 1);
}

#[test]
fn promoted_reset_leader_shutdown_repromotes_another_participant() {
    let mut broker = new_broker();
    boot_leader_and_attached_follower(&mut broker);

    send(&mut broker, 1, storage_reset_request("reset-1"), 40);
    send(
        &mut broker,
        1,
        storage_reset_ready("reset-1", true, None),
        50,
    );
    send(
        &mut broker,
        2,
        storage_reset_ready("reset-1", true, None),
        55,
    );
    let commands = broker.handle(
        BrokerEvent::LocksProbeResult {
            probe_id: ProbeId(1),
            all_acquired: true,
        },
        60,
    );
    // tab-b (most recently visible) is promoted first.
    assert_eq!(
        become_leader_posts(&commands),
        vec![(2, 2, Some("reset-1".to_string()))]
    );

    // The promoted leader dies before finishing: the reset re-promotes tab-a
    // instead of hanging.
    let commands = send(&mut broker, 2, shutdown(), 70);
    assert_eq!(
        become_leader_posts(&commands),
        vec![(1, 3, Some("reset-1".to_string()))]
    );
}

#[test]
fn hello_joins_a_preparing_reset_but_gets_nothing_in_later_phases() {
    let mut broker = new_broker();
    connect(&mut broker, 1, "tab-a", 0);
    send(&mut broker, 1, leader_ready(1), 10);
    send(&mut broker, 1, storage_reset_request("reset-1"), 20);

    // Preparing: the new tab becomes a participant and receives the begin.
    let commands = connect(&mut broker, 2, "tab-b", 30);
    assert_eq!(
        commands.last(),
        Some(&BrokerCommand::Post {
            port_id: PortId(2),
            message: ControlMessage::StorageResetBegin {
                broker_instance_id: BROKER.to_string(),
                request_id: "reset-1".to_string(),
                leadership_id: 1,
            },
        })
    );

    // Both participants prepare → promoting phase.
    send(
        &mut broker,
        1,
        storage_reset_ready("reset-1", true, None),
        40,
    );
    send(
        &mut broker,
        2,
        storage_reset_ready("reset-1", true, None),
        45,
    );

    // Mid-promotion hello: broker-hello only, no join, no leader-ready.
    let commands = connect(&mut broker, 3, "tab-c", 50);
    assert_eq!(
        commands,
        vec![BrokerCommand::Post {
            port_id: PortId(3),
            message: ControlMessage::BrokerHello {
                broker_instance_id: BROKER.to_string(),
            },
        }]
    );
}

#[test]
fn finished_reset_outcomes_are_redelivered_within_ttl_and_pruned_after() {
    let mut broker = new_broker();
    connect(&mut broker, 1, "tab-a", 0);
    send(&mut broker, 1, leader_ready(1), 10);
    send(&mut broker, 1, storage_reset_request("reset-1"), 20);
    send(
        &mut broker,
        1,
        storage_reset_ready("reset-1", true, None),
        30,
    );
    broker.handle(
        BrokerEvent::LocksProbeResult {
            probe_id: ProbeId(1),
            all_acquired: true,
        },
        40,
    );
    // Bridgeless finish at t=50 (fresh namespace).
    send(
        &mut broker,
        1,
        TabMessage::LeaderReady {
            broker_instance_id: BROKER.to_string(),
            leadership_id: 2,
            tab_lock_name: "tab-lock".to_string(),
            worker_lock_name: "worker-lock".to_string(),
            bridgeless_storage_reset: true,
        },
        50,
    );

    // Within the 30s TTL: a rejoining tab gets the outcome redelivered.
    let commands = connect(&mut broker, 2, "tab-b", 5_000);
    assert_eq!(
        reset_finished_posts(&commands),
        vec![(2, "reset-1".to_string(), true, None)]
    );

    // Past the TTL: pruned, nothing redelivered.
    let commands = connect(&mut broker, 3, "tab-c", 50 + 30_001);
    assert!(reset_finished_posts(&commands).is_empty());
}

// ---------------------------------------------------------------------------
// Idle reset and namespace relatch
// ---------------------------------------------------------------------------

#[test]
fn last_tab_leaving_stops_liveness_and_relatches_the_namespace() {
    let mut broker = new_broker();
    connect(&mut broker, 1, "tab-a", 0);

    let commands = send(&mut broker, 1, shutdown(), 10);
    assert!(commands.contains(&BrokerCommand::ClearTimer {
        timer: TimerKey::BrokerPing,
    }));

    // A hello with a different fingerprint is now accepted: the namespace
    // latched by the departed tab was cleared.
    let commands = broker.handle(
        BrokerEvent::PortMessage {
            port_id: PortId(2),
            message: TabMessage::Hello {
                tab_id: "tab-b".to_string(),
                app_id: "app-2".to_string(),
                db_name: "db-2".to_string(),
                fingerprint: "fingerprint-2".to_string(),
                visibility: Visibility::Visible,
                force_takeover_timeout_ms: None,
                broker_ping_interval_ms: None,
                broker_pong_timeout_ms: None,
            },
        },
        20,
    );
    assert_eq!(become_leader_posts(&commands), vec![(2, 2, None)]);
}

// ---------------------------------------------------------------------------
// Port binding fidelity
// ---------------------------------------------------------------------------

#[test]
fn messages_from_a_replaced_port_still_route_to_the_bound_tab() {
    let mut broker = new_broker();
    connect(&mut broker, 1, "tab-a", 0);
    send(&mut broker, 1, leader_ready(1), 10);
    // Same tab re-hellos on a new port; the old port stays bound to tab-a
    // (mirroring the JS per-port closure).
    connect(&mut broker, 2, "tab-a", 20);

    let commands = send(&mut broker, 1, storage_reset_request("reset-9"), 30);

    // The acknowledgement goes to the tab's *current* port.
    assert!(commands.contains(&BrokerCommand::Post {
        port_id: PortId(2),
        message: ControlMessage::StorageResetStarted {
            broker_instance_id: BROKER.to_string(),
            request_id: "reset-9".to_string(),
        },
    }));
}

#[test]
fn messages_from_an_unknown_port_are_dropped() {
    let mut broker = new_broker();
    connect(&mut broker, 1, "tab-a", 0);

    assert!(send(&mut broker, 7, broker_pong(), 10).is_empty());
    assert!(send(&mut broker, 7, storage_reset_request("reset-1"), 20).is_empty());
}

// ---------------------------------------------------------------------------
// Event serde (wasm boundary shapes)
// ---------------------------------------------------------------------------

#[test]
fn broker_events_deserialize_from_the_shell_wire_shapes() {
    let event: BrokerEvent = serde_json::from_value(serde_json::json!({
        "kind": "portMessage",
        "portId": 3,
        "message": { "type": "broker-pong", "brokerInstanceId": "broker-a" }
    }))
    .expect("port message event should deserialize");
    assert_eq!(
        event,
        BrokerEvent::PortMessage {
            port_id: PortId(3),
            message: TabMessage::BrokerPong {
                broker_instance_id: "broker-a".to_string(),
            },
        }
    );

    let event: BrokerEvent = serde_json::from_value(serde_json::json!({
        "kind": "locksProbeResult",
        "probeId": 4,
        "allAcquired": true
    }))
    .expect("probe result event should deserialize");
    assert_eq!(
        event,
        BrokerEvent::LocksProbeResult {
            probe_id: ProbeId(4),
            all_acquired: true,
        }
    );

    let event: BrokerEvent = serde_json::from_value(serde_json::json!({
        "kind": "timerFired",
        "timer": { "kind": "followerAttachment", "leadershipId": 2, "followerTabId": "tab-b" }
    }))
    .expect("timer event should deserialize");
    assert_eq!(
        event,
        BrokerEvent::TimerFired {
            timer: TimerKey::FollowerAttachment {
                leadership_id: 2,
                follower_tab_id: "tab-b".to_string(),
            },
        }
    );
}

#[test]
fn completed_reset_outcomes_cap_at_100_and_refinish_moves_to_the_back_of_eviction() {
    let mut broker = new_broker();
    connect(&mut broker, 1, "tab-a", 0);

    let mut next_leadership = 1_u64;
    let mut run_bridgeless_reset = |broker: &mut BrokerCore, request_id: &str, first: bool| {
        send(broker, 1, storage_reset_request(request_id), 0);
        let commands = send(broker, 1, storage_reset_ready(request_id, true, None), 0);
        let commands = if first {
            // The initial (never-ready) leader was cleared with lock names:
            // the reset probes them before promoting.
            assert!(matches!(
                commands.last(),
                Some(BrokerCommand::ProbeLocks { .. })
            ));
            broker.handle(
                BrokerEvent::LocksProbeResult {
                    probe_id: ProbeId(1),
                    all_acquired: true,
                },
                0,
            )
        } else {
            commands
        };
        next_leadership += 1;
        assert_eq!(
            become_leader_posts(&commands),
            vec![(1, next_leadership, Some(request_id.to_string()))]
        );
        send(
            broker,
            1,
            TabMessage::LeaderReady {
                broker_instance_id: BROKER.to_string(),
                leadership_id: next_leadership,
                tab_lock_name: "tab-lock".to_string(),
                worker_lock_name: "worker-lock".to_string(),
                bridgeless_storage_reset: true,
            },
            0,
        );
    };

    for i in 1..=100 {
        run_bridgeless_reset(&mut broker, &format!("r{i}"), i == 1);
    }
    // Re-finishing r1 must move it to the back of the eviction order
    // (delete-before-insert), so the next overflow evicts r2, not r1.
    run_bridgeless_reset(&mut broker, "r1", false);
    run_bridgeless_reset(&mut broker, "r101", false);

    let commands = connect(&mut broker, 2, "tab-b", 0);
    let redelivered: Vec<String> = reset_finished_posts(&commands)
        .into_iter()
        .map(|(_, request_id, _, _)| request_id)
        .collect();

    assert_eq!(redelivered.len(), 100);
    assert!(!redelivered.contains(&"r2".to_string()));
    assert_eq!(redelivered.first().map(String::as_str), Some("r3"));
    assert_eq!(&redelivered[98..], &["r1".to_string(), "r101".to_string()]);
}
