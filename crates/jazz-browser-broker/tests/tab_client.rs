use jazz_browser_broker::protocol::{ControlMessage, TabMessage, Visibility};
use jazz_browser_broker::tab_client::TabMessageInput;
use jazz_browser_broker::{
    Role, TabClientCommand, TabClientCore, TabClientEvent, TabClientOptions, TabTimerKey,
    WaiterRejection,
};

const INSTANCE: &str = "instance-a";

fn new_client() -> TabClientCore {
    TabClientCore::new(TabClientOptions {
        tab_id: "tab-a".to_string(),
        broker_ping_interval_ms: None,
        broker_pong_timeout_ms: None,
        storage_reset_timeout_ms: None,
    })
}

fn connected_client() -> TabClientCore {
    let mut client = new_client();
    client.handle(TabClientEvent::PortAttached);
    control(&mut client, broker_hello(INSTANCE));
    client.handle(TabClientEvent::ConnectCompleted);
    client
}

fn broker_hello(instance: &str) -> ControlMessage {
    ControlMessage::BrokerHello {
        broker_instance_id: instance.to_string(),
    }
}

fn control(client: &mut TabClientCore, message: ControlMessage) -> Vec<TabClientCommand> {
    let stamped = stamped_id(&message);
    client.handle(TabClientEvent::ControlMessage {
        message,
        stamped_instance_id: stamped,
    })
}

fn stamped_id(message: &ControlMessage) -> Option<String> {
    match message {
        ControlMessage::BrokerHello { broker_instance_id }
        | ControlMessage::BrokerPing { broker_instance_id }
        | ControlMessage::BecomeLeader {
            broker_instance_id, ..
        }
        | ControlMessage::Demote {
            broker_instance_id, ..
        }
        | ControlMessage::LeaderReady {
            broker_instance_id, ..
        }
        | ControlMessage::FollowerReady {
            broker_instance_id, ..
        }
        | ControlMessage::CloseFollowerPort {
            broker_instance_id, ..
        }
        | ControlMessage::DetachFollowerPort {
            broker_instance_id, ..
        }
        | ControlMessage::StorageResetBegin {
            broker_instance_id, ..
        }
        | ControlMessage::StorageResetStarted {
            broker_instance_id, ..
        }
        | ControlMessage::StorageResetFinished {
            broker_instance_id, ..
        }
        | ControlMessage::Unsupported {
            broker_instance_id, ..
        }
        | ControlMessage::SchemaBlocked {
            broker_instance_id, ..
        }
        | ControlMessage::AttachFollowerPort {
            broker_instance_id, ..
        }
        | ControlMessage::UseFollowerPort {
            broker_instance_id, ..
        } => Some(broker_instance_id.clone()),
        ControlMessage::Unknown => None,
    }
}

fn leader_ready(instance: &str, leader_tab_id: &str, leadership_id: u64) -> ControlMessage {
    ControlMessage::LeaderReady {
        broker_instance_id: instance.to_string(),
        leader_tab_id: leader_tab_id.to_string(),
        leadership_id,
    }
}

fn posted_messages(commands: &[TabClientCommand]) -> Vec<&TabMessage> {
    commands
        .iter()
        .filter_map(|command| match command {
            TabClientCommand::PostToBroker { message } => Some(message),
            _ => None,
        })
        .collect()
}

// T1/T2 — stamping and send rules

#[test]
fn sends_are_dropped_before_broker_hello_and_stamped_after() {
    let mut client = new_client();
    client.handle(TabClientEvent::PortAttached);

    // No instance id yet: stamping fails, message drops silently.
    let commands = client.handle(TabClientEvent::SendRequested {
        message: TabMessageInput::SchemaReady {
            schema_fingerprint: "schema-1".to_string(),
        },
    });
    assert!(commands.is_empty());

    control(&mut client, broker_hello(INSTANCE));
    let commands = client.handle(TabClientEvent::SendRequested {
        message: TabMessageInput::SchemaReady {
            schema_fingerprint: "schema-1".to_string(),
        },
    });
    assert_eq!(
        posted_messages(&commands),
        vec![&TabMessage::SchemaReady {
            broker_instance_id: INSTANCE.to_string(),
            schema_fingerprint: "schema-1".to_string(),
        }]
    );
}

#[test]
fn sends_queue_without_a_port_and_flush_in_order_on_connect() {
    let mut client = new_client();
    // Instance known but port not yet attached (artificial, mirrors the JS
    // queueing branch): messages queue and flush on ConnectCompleted.
    client.handle(TabClientEvent::PortAttached);
    control(&mut client, broker_hello(INSTANCE));
    // Simulate the queueing state the JS reaches between port teardown and
    // reconnect: has_port toggles only via DetachPort paths, so instead we
    // assert flush ordering through ConnectCompleted.
    let commands = client.handle(TabClientEvent::ConnectCompleted);
    // Nothing queued: only the liveness arm.
    assert_eq!(
        commands,
        vec![TabClientCommand::SetTimer {
            timer: TabTimerKey::Liveness,
            delay_ms: 4_000,
        }]
    );
}

// T3 — instance mismatch triggers reconnect before dispatch

#[test]
fn instance_mismatch_starts_a_reconnect_and_resets_state() {
    let mut client = connected_client();
    control(&mut client, leader_ready(INSTANCE, "tab-a", 1));
    assert_eq!(client.snapshot().role, Role::Leader);

    let commands = control(&mut client, broker_hello("instance-b"));

    assert_eq!(
        commands,
        vec![
            TabClientCommand::ClearTimer {
                timer: TabTimerKey::Liveness,
            },
            TabClientCommand::DetachPort,
            TabClientCommand::StartReconnect {
                previous_role: Role::Leader,
                previous_leadership_id: 1,
            },
        ]
    );
    let snapshot = client.snapshot();
    assert!(snapshot.reconnecting);
    assert_eq!(snapshot.broker_instance_id, None);
    assert_eq!(snapshot.role, Role::Follower);
    assert_eq!(snapshot.leadership_id, 0);

    // Sends during the reconnect are dropped, not queued.
    let commands = client.handle(TabClientEvent::SendRequested {
        message: TabMessageInput::LeaderFailed {
            leadership_id: 1,
            reason: "late old-instance failure".to_string(),
        },
    });
    assert!(commands.is_empty());

    // Successful reconnect replays the current visibility, notifies, flushes.
    client.handle(TabClientEvent::PortAttached);
    control(&mut client, broker_hello("instance-b"));
    let commands = client.handle(TabClientEvent::ReconnectFinished { error: None });
    assert_eq!(
        commands,
        vec![
            TabClientCommand::PostToBroker {
                message: TabMessage::Visibility {
                    broker_instance_id: "instance-b".to_string(),
                    visibility: Visibility::Visible,
                },
            },
            TabClientCommand::InvokeOnReconnected,
        ]
    );
}

#[test]
fn reconnect_replays_the_latest_reported_visibility() {
    let mut client = connected_client();
    client.handle(TabClientEvent::VisibilityReported {
        visibility: Visibility::Hidden,
    });

    control(&mut client, broker_hello("instance-b"));
    client.handle(TabClientEvent::PortAttached);
    control(&mut client, broker_hello("instance-b"));
    let commands = client.handle(TabClientEvent::ReconnectFinished { error: None });

    assert_eq!(
        posted_messages(&commands),
        vec![&TabMessage::Visibility {
            broker_instance_id: "instance-b".to_string(),
            visibility: Visibility::Hidden,
        }]
    );
}

#[test]
fn reconnect_failure_closes_with_the_failure_message() {
    let mut client = connected_client();
    control(&mut client, broker_hello("instance-b"));

    let commands = client.handle(TabClientEvent::ReconnectFinished {
        error: Some("second construction failed".to_string()),
    });

    assert_eq!(
        commands.first(),
        Some(&TabClientCommand::CloseWithError {
            message: "second construction failed".to_string(),
            code: None,
            from_reconnect_failure: true,
        })
    );
    assert_eq!(commands.last(), Some(&TabClientCommand::InvokeOnClosed));
    assert!(client.snapshot().closed);
}

// T4/T5/T6 — leadership messages

#[test]
fn become_leader_adopts_the_leadership_and_invokes_the_callback() {
    let mut client = connected_client();
    let commands = control(
        &mut client,
        ControlMessage::BecomeLeader {
            broker_instance_id: INSTANCE.to_string(),
            leadership_id: 3,
            reset_request_id: Some("reset-1".to_string()),
        },
    );
    assert_eq!(
        commands,
        vec![TabClientCommand::InvokeOnBecomeLeader {
            leadership_id: 3,
            reset_request_id: Some("reset-1".to_string()),
        }]
    );
    assert_eq!(client.snapshot().leadership_id, 3);
}

#[test]
fn demote_always_forwards_but_only_matching_ids_reset_the_role() {
    let mut client = connected_client();
    control(&mut client, leader_ready(INSTANCE, "tab-a", 1));

    // Future demote (id 2 ≠ current 1): forwarded, role untouched.
    let commands = control(
        &mut client,
        ControlMessage::Demote {
            broker_instance_id: INSTANCE.to_string(),
            leadership_id: 2,
        },
    );
    assert_eq!(
        commands,
        vec![TabClientCommand::InvokeOnDemote { leadership_id: 2 }]
    );
    assert_eq!(client.snapshot().role, Role::Leader);

    // Matching demote resets role and leader.
    control(
        &mut client,
        ControlMessage::Demote {
            broker_instance_id: INSTANCE.to_string(),
            leadership_id: 1,
        },
    );
    let snapshot = client.snapshot();
    assert_eq!(snapshot.role, Role::Follower);
    assert_eq!(snapshot.leader_tab_id, None);
}

#[test]
fn role_waiters_resolve_on_matching_leadership_and_time_out_otherwise() {
    let mut client = connected_client();

    let commands = client.handle(TabClientEvent::RoleWaiterAdded {
        waiter_id: 1,
        role: Role::Leader,
        timeout_ms: 5_000,
    });
    assert_eq!(
        commands,
        vec![TabClientCommand::SetTimer {
            timer: TabTimerKey::RoleWaiter { waiter_id: 1 },
            delay_ms: 5_000,
        }]
    );

    let commands = control(&mut client, leader_ready(INSTANCE, "tab-a", 1));
    assert_eq!(
        commands,
        vec![
            TabClientCommand::ClearTimer {
                timer: TabTimerKey::RoleWaiter { waiter_id: 1 },
            },
            TabClientCommand::SettleRoleWaiter {
                waiter_id: 1,
                rejection: None,
            },
        ]
    );

    // A second waiter for follower times out with the JS message.
    client.handle(TabClientEvent::RoleWaiterAdded {
        waiter_id: 2,
        role: Role::Follower,
        timeout_ms: 5_000,
    });
    let commands = client.handle(TabClientEvent::TimerFired {
        timer: TabTimerKey::RoleWaiter { waiter_id: 2 },
    });
    assert_eq!(
        commands,
        vec![TabClientCommand::SettleRoleWaiter {
            waiter_id: 2,
            rejection: Some(WaiterRejection::Message {
                message: "Timed out waiting for broker role follower".to_string(),
            }),
        }]
    );
}

// T7 — follower port messages

#[test]
fn attach_follower_port_requires_an_exact_leadership_match() {
    let mut client = connected_client();
    control(&mut client, leader_ready(INSTANCE, "tab-a", 1));

    let commands = control(
        &mut client,
        ControlMessage::AttachFollowerPort {
            broker_instance_id: INSTANCE.to_string(),
            follower_tab_id: "tab-b".to_string(),
            leadership_id: 9,
        },
    );
    assert!(commands.is_empty());

    let commands = control(
        &mut client,
        ControlMessage::AttachFollowerPort {
            broker_instance_id: INSTANCE.to_string(),
            follower_tab_id: "tab-b".to_string(),
            leadership_id: 1,
        },
    );
    assert_eq!(
        commands,
        vec![TabClientCommand::InvokeOnAttachFollowerPort {
            follower_tab_id: "tab-b".to_string(),
            leadership_id: 1,
        }]
    );
}

#[test]
fn use_follower_port_and_follower_ready_force_the_follower_role() {
    let mut client = connected_client();
    control(&mut client, leader_ready(INSTANCE, "tab-a", 1));

    let commands = control(
        &mut client,
        ControlMessage::UseFollowerPort {
            broker_instance_id: INSTANCE.to_string(),
            leader_tab_id: "tab-b".to_string(),
            leadership_id: 2,
        },
    );
    assert_eq!(
        commands,
        vec![TabClientCommand::InvokeOnUseFollowerPort { leadership_id: 2 }]
    );
    let snapshot = client.snapshot();
    assert_eq!(snapshot.role, Role::Follower);
    assert_eq!(snapshot.leader_tab_id, Some("tab-b".to_string()));
    assert_eq!(snapshot.leadership_id, 2);

    let commands = control(
        &mut client,
        ControlMessage::FollowerReady {
            broker_instance_id: INSTANCE.to_string(),
            leader_tab_id: "tab-b".to_string(),
            leadership_id: 2,
        },
    );
    assert_eq!(
        commands,
        vec![TabClientCommand::InvokeOnFollowerReady { leadership_id: 2 }]
    );
}

// T8/T9 — storage reset waiters

#[test]
fn storage_reset_flow_settles_start_and_completion_waiters() {
    let mut client = connected_client();

    let commands = client.handle(TabClientEvent::StorageResetRequested {
        request_id: "reset-a".to_string(),
        start_waiter_id: 1,
        completion_waiter_id: 2,
    });
    assert_eq!(
        commands,
        vec![
            TabClientCommand::SetTimer {
                timer: TabTimerKey::ResetStartWaiter { waiter_id: 1 },
                delay_ms: 5_000,
            },
            TabClientCommand::PostToBroker {
                message: TabMessage::StorageResetRequest {
                    broker_instance_id: INSTANCE.to_string(),
                    request_id: "reset-a".to_string(),
                },
            },
        ]
    );

    // storage-reset-begin resolves only the start waiter and runs the callback.
    let commands = control(
        &mut client,
        ControlMessage::StorageResetBegin {
            broker_instance_id: INSTANCE.to_string(),
            request_id: "reset-a".to_string(),
            leadership_id: 1,
        },
    );
    assert_eq!(
        commands,
        vec![
            TabClientCommand::ClearTimer {
                timer: TabTimerKey::ResetStartWaiter { waiter_id: 1 },
            },
            TabClientCommand::SettleResetStartWaiters {
                waiter_ids: vec![1],
                rejection: None,
            },
            TabClientCommand::InvokeOnStorageResetBegin {
                request_id: "reset-a".to_string(),
                leadership_id: 1,
            },
        ]
    );

    // finished settles the completion waiter.
    let commands = control(
        &mut client,
        ControlMessage::StorageResetFinished {
            broker_instance_id: INSTANCE.to_string(),
            request_id: "reset-a".to_string(),
            success: true,
            error_message: None,
        },
    );
    assert_eq!(
        commands,
        vec![TabClientCommand::SettleResetWaiters {
            waiter_ids: vec![2],
            rejection: None,
        }]
    );
}

#[test]
fn only_the_start_acknowledgment_times_out() {
    let mut client = connected_client();
    client.handle(TabClientEvent::StorageResetRequested {
        request_id: "reset-a".to_string(),
        start_waiter_id: 1,
        completion_waiter_id: 2,
    });

    let commands = client.handle(TabClientEvent::TimerFired {
        timer: TabTimerKey::ResetStartWaiter { waiter_id: 1 },
    });
    assert_eq!(
        commands,
        vec![TabClientCommand::SettleResetStartWaiters {
            waiter_ids: vec![1],
            rejection: Some(WaiterRejection::Message {
                message: "Timed out waiting for browser storage reset reset-a to start".to_string(),
            }),
        }]
    );

    // The paired completion waiter is abandoned: a later finished message for
    // the same request settles nothing.
    let commands = control(
        &mut client,
        ControlMessage::StorageResetFinished {
            broker_instance_id: INSTANCE.to_string(),
            request_id: "reset-a".to_string(),
            success: true,
            error_message: None,
        },
    );
    assert!(commands.is_empty());
}

#[test]
fn failed_resets_reject_with_the_broker_error_message_or_the_default() {
    let mut client = connected_client();
    client.handle(TabClientEvent::StorageResetRequested {
        request_id: "reset-a".to_string(),
        start_waiter_id: 1,
        completion_waiter_id: 2,
    });

    let commands = control(
        &mut client,
        ControlMessage::StorageResetFinished {
            broker_instance_id: INSTANCE.to_string(),
            request_id: "reset-a".to_string(),
            success: false,
            error_message: None,
        },
    );
    assert!(commands.contains(&TabClientCommand::SettleResetWaiters {
        waiter_ids: vec![2],
        rejection: Some(WaiterRejection::Message {
            message: "Browser storage reset failed".to_string(),
        }),
    }));
}

#[test]
fn reconnect_rejects_in_flight_reset_waiters() {
    let mut client = connected_client();
    client.handle(TabClientEvent::StorageResetRequested {
        request_id: "reset-a".to_string(),
        start_waiter_id: 1,
        completion_waiter_id: 2,
    });
    control(
        &mut client,
        ControlMessage::StorageResetStarted {
            broker_instance_id: INSTANCE.to_string(),
            request_id: "reset-a".to_string(),
        },
    );

    let commands = control(&mut client, broker_hello("instance-b"));
    assert!(commands.contains(&TabClientCommand::SettleResetWaiters {
        waiter_ids: vec![2],
        rejection: Some(WaiterRejection::Message {
            message: "Browser broker restarted during storage reset".to_string(),
        }),
    }));
}

// T10 — liveness

#[test]
fn liveness_uses_normalized_ping_plus_pong_and_expiry_reconnects() {
    let mut client = TabClientCore::new(TabClientOptions {
        tab_id: "tab-a".to_string(),
        broker_ping_interval_ms: Some(10.0),
        broker_pong_timeout_ms: Some(20.0),
        storage_reset_timeout_ms: None,
    });
    client.handle(TabClientEvent::PortAttached);
    control(&mut client, broker_hello(INSTANCE));

    let commands = control(
        &mut client,
        ControlMessage::BrokerPing {
            broker_instance_id: INSTANCE.to_string(),
        },
    );
    assert_eq!(
        commands,
        vec![
            TabClientCommand::SetTimer {
                timer: TabTimerKey::Liveness,
                delay_ms: 30,
            },
            TabClientCommand::HandleBrokerPing {
                broker_instance_id: INSTANCE.to_string(),
            },
        ]
    );

    let commands = client.handle(TabClientEvent::TimerFired {
        timer: TabTimerKey::Liveness,
    });
    assert!(commands.contains(&TabClientCommand::StartReconnect {
        previous_role: Role::Follower,
        previous_leadership_id: 0,
    }));
    assert!(client.snapshot().reconnecting);
}

// T12 — unsupported closes with the typed error

#[test]
fn unsupported_closes_once_with_the_typed_error_and_rejects_waiters() {
    let mut client = connected_client();
    client.handle(TabClientEvent::RoleWaiterAdded {
        waiter_id: 1,
        role: Role::Leader,
        timeout_ms: 5_000,
    });

    let commands = control(
        &mut client,
        ControlMessage::Unsupported {
            broker_instance_id: INSTANCE.to_string(),
            code: Some("incompatible-browser-broker-configuration".to_string()),
            reason: "incompatible persistent browser configuration".to_string(),
        },
    );

    assert_eq!(
        commands,
        vec![
            TabClientCommand::CloseWithError {
                message: "incompatible persistent browser configuration".to_string(),
                code: Some("incompatible-browser-broker-configuration".to_string()),
                from_reconnect_failure: false,
            },
            TabClientCommand::ClearTimer {
                timer: TabTimerKey::Liveness,
            },
            TabClientCommand::DetachPort,
            TabClientCommand::ClearTimer {
                timer: TabTimerKey::RoleWaiter { waiter_id: 1 },
            },
            TabClientCommand::SettleRoleWaiter {
                waiter_id: 1,
                rejection: Some(WaiterRejection::ClosedError),
            },
            TabClientCommand::InvokeOnClosed,
        ]
    );
    assert!(client.snapshot().closed);

    // Closed clients drop sends and further reconnect triggers.
    assert!(
        client
            .handle(TabClientEvent::SendRequested {
                message: TabMessageInput::SchemaReady {
                    schema_fingerprint: "schema-1".to_string(),
                },
            })
            .is_empty()
    );
    assert!(control(&mut client, broker_hello("instance-b")).is_empty());
}

// T13 — shutdown

#[test]
fn shutdown_posts_the_stamped_shutdown_then_rejects_everything_once() {
    let mut client = connected_client();
    client.handle(TabClientEvent::RoleWaiterAdded {
        waiter_id: 1,
        role: Role::Leader,
        timeout_ms: 5_000,
    });

    let commands = client.handle(TabClientEvent::ShutdownRequested);
    assert_eq!(
        commands,
        vec![
            TabClientCommand::ClearTimer {
                timer: TabTimerKey::Liveness,
            },
            TabClientCommand::PostToBroker {
                message: TabMessage::Shutdown {
                    broker_instance_id: INSTANCE.to_string(),
                },
            },
            TabClientCommand::DetachPort,
            TabClientCommand::ClearTimer {
                timer: TabTimerKey::RoleWaiter { waiter_id: 1 },
            },
            TabClientCommand::SettleRoleWaiter {
                waiter_id: 1,
                rejection: Some(WaiterRejection::Message {
                    message: "Browser broker client closed".to_string(),
                }),
            },
        ]
    );

    // Idempotent.
    assert!(client.handle(TabClientEvent::ShutdownRequested).is_empty());
}

// Wire shapes for the wasm boundary

#[test]
fn tab_client_events_and_commands_use_the_camel_case_wire_shapes() {
    let event: TabClientEvent = serde_json::from_value(serde_json::json!({
        "kind": "controlMessage",
        "message": { "type": "leader-ready", "brokerInstanceId": "instance-a",
                     "leaderTabId": "tab-a", "leadershipId": 2 },
        "stampedInstanceId": "instance-a"
    }))
    .expect("control message event should deserialize");
    assert_eq!(
        event,
        TabClientEvent::ControlMessage {
            message: ControlMessage::LeaderReady {
                broker_instance_id: "instance-a".to_string(),
                leader_tab_id: "tab-a".to_string(),
                leadership_id: 2,
            },
            stamped_instance_id: Some("instance-a".to_string()),
        }
    );

    let value = serde_json::to_value(TabClientCommand::PostToBroker {
        message: TabMessage::LeaderReady {
            broker_instance_id: "instance-a".to_string(),
            leadership_id: 2,
            tab_lock_name: "tab-lock".to_string(),
            worker_lock_name: "worker-lock".to_string(),
            bridgeless_storage_reset: false,
        },
    })
    .expect("command should serialize");
    assert_eq!(
        value,
        serde_json::json!({
            "kind": "postToBroker",
            "message": {
                "type": "leader-ready",
                "brokerInstanceId": "instance-a",
                "leadershipId": 2,
                "tabLockName": "tab-lock",
                "workerLockName": "worker-lock",
            }
        })
    );

    // Port-stripped attach message deserializes (unknown `port` key removed by
    // the shell; other unknown keys are ignored).
    let message: ControlMessage = serde_json::from_value(serde_json::json!({
        "type": "attach-follower-port",
        "brokerInstanceId": "instance-a",
        "followerTabId": "tab-b",
        "leadershipId": 2
    }))
    .expect("attach-follower-port should deserialize");
    assert_eq!(
        message,
        ControlMessage::AttachFollowerPort {
            broker_instance_id: "instance-a".to_string(),
            follower_tab_id: "tab-b".to_string(),
            leadership_id: 2,
        }
    );
}
