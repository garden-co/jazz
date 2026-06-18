use jazz_wasm::broker_client::{
    BrokerClientCallback, BrokerClientCommand, BrokerClientCore, BrokerClientEffect,
    BrokerClientEvent, BrokerClientTimerKind, BrokerControlMessage, BrokerRole, BrokerVisibility,
    BrowserBrokerTabMessage, ConnectRequested,
};

fn connect_event() -> BrokerClientEvent {
    BrokerClientEvent::ConnectRequested(ConnectRequested {
        app_id: "app".to_string(),
        db_name: "db".to_string(),
        tab_id: "tab-a".to_string(),
        fingerprint: "fingerprint".to_string(),
        visibility: BrokerVisibility::Visible,
        force_takeover_timeout_ms: None,
        broker_ping_interval_ms: Some(10),
        broker_pong_timeout_ms: Some(20),
        storage_reset_timeout_ms: Some(30),
        now_ms: 1,
    })
}

fn broker_hello(now_ms: u64) -> BrokerClientEvent {
    BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::BrokerHello {
            broker_instance_id: "instance-a".to_string(),
        },
        respond_to_broker_ping: true,
        now_ms,
    }
}

fn connected_client() -> BrokerClientCore {
    let mut client = BrokerClientCore::new();
    let _ = client.handle(connect_event());
    let _ = client.handle(broker_hello(2));
    let _ = client.handle(BrokerClientEvent::TimerFired {
        timer_id: 2,
        kind: BrokerClientTimerKind::InitialLeadership,
        now_ms: 103,
    });
    client
}

#[test]
fn broker_client_events_serialize_with_stable_js_field_names() {
    let event = connect_event();

    let json = serde_json::to_value(event).unwrap();
    assert_eq!(json["type"], "connectRequested");
    assert_eq!(json["tabId"], "tab-a");
    assert_eq!(json["storageResetTimeoutMs"], 30);
}

#[test]
fn broker_client_effects_serialize_with_stable_js_field_names() {
    let effect = BrokerClientEffect::ArmTimer {
        timer_id: 7,
        kind: BrokerClientTimerKind::BrokerLiveness,
        delay_ms: 30,
    };

    let json = serde_json::to_value(effect).unwrap();
    assert_eq!(json["type"], "armTimer");
    assert_eq!(json["timerId"], 7);
    assert_eq!(json["kind"]["type"], "brokerLiveness");
    assert_eq!(json["delayMs"], 30);
}

#[test]
fn connect_requests_worker_and_posts_hello() {
    let mut client = BrokerClientCore::new();
    let effects = client.handle(connect_event());

    assert_eq!(
        effects,
        vec![
            BrokerClientEffect::CreateSharedWorker {
                worker_id: 1,
                name: "jazz-broker:app:db".to_string(),
            },
            BrokerClientEffect::AttachPortListeners { port_id: 1 },
            BrokerClientEffect::PostToBroker {
                port_id: 1,
                message: BrowserBrokerTabMessage::Hello {
                    tab_id: "tab-a".to_string(),
                    app_id: "app".to_string(),
                    db_name: "db".to_string(),
                    fingerprint: "fingerprint".to_string(),
                    visibility: BrokerVisibility::Visible,
                    force_takeover_timeout_ms: None,
                    broker_ping_interval_ms: Some(10),
                    broker_pong_timeout_ms: Some(20),
                },
            },
            BrokerClientEffect::ArmTimer {
                timer_id: 1,
                kind: BrokerClientTimerKind::BrokerHello,
                delay_ms: 5_000,
            },
        ],
    );
}

#[test]
fn broker_hello_adopts_instance_and_arms_initial_leadership_timer() {
    let mut client = BrokerClientCore::new();
    let _ = client.handle(connect_event());

    let effects = client.handle(broker_hello(2));

    assert_eq!(
        effects,
        vec![
            BrokerClientEffect::CancelTimer { timer_id: 1 },
            BrokerClientEffect::ArmTimer {
                timer_id: 2,
                kind: BrokerClientTimerKind::InitialLeadership,
                delay_ms: 100,
            },
        ],
    );
    assert_eq!(
        client.snapshot().broker_instance_id.as_deref(),
        Some("instance-a")
    );
}

#[test]
fn visibility_updates_are_stamped_with_active_broker_instance() {
    let mut client = connected_client();

    let effects = client.handle(BrokerClientEvent::PublicCommand {
        command: BrokerClientCommand::ReportVisibility {
            visibility: BrokerVisibility::Hidden,
        },
    });

    assert_eq!(
        effects,
        vec![BrokerClientEffect::PostToBroker {
            port_id: 1,
            message: BrowserBrokerTabMessage::Visibility {
                broker_instance_id: "instance-a".to_string(),
                visibility: BrokerVisibility::Hidden,
            },
        }],
    );
    assert_eq!(client.snapshot().visibility, BrokerVisibility::Hidden);
}

#[test]
fn messages_without_broker_instance_are_dropped() {
    let mut client = BrokerClientCore::new();
    let _ = client.handle(connect_event());

    let effects = client.handle(BrokerClientEvent::PublicCommand {
        command: BrokerClientCommand::ReportSchemaReady {
            schema_fingerprint: "schema-a".to_string(),
        },
    });

    assert!(effects.is_empty());
}

#[test]
fn leader_ready_sets_leader_role_for_own_tab() {
    let mut client = connected_client();

    let effects = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::LeaderReady {
            broker_instance_id: "instance-a".to_string(),
            leader_tab_id: "tab-a".to_string(),
            leadership_id: 1,
        },
        respond_to_broker_ping: true,
        now_ms: 3,
    });

    assert!(effects.is_empty());
    assert_eq!(client.snapshot().role, BrokerRole::Leader);
    assert_eq!(client.snapshot().leader_tab_id.as_deref(), Some("tab-a"));
    assert_eq!(client.snapshot().leadership_id, 1);
}

#[test]
fn become_leader_callback_failure_reports_leader_failed() {
    let mut client = connected_client();

    let effects = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::BecomeLeader {
            broker_instance_id: "instance-a".to_string(),
            leadership_id: 2,
            reset_request_id: None,
        },
        respond_to_broker_ping: true,
        now_ms: 3,
    });

    assert_eq!(
        effects,
        vec![BrokerClientEffect::InvokeCallback {
            callback_id: 1,
            callback: BrokerClientCallback::BecomeLeader {
                leadership_id: 2,
                reset_request_id: None,
            },
        }],
    );

    let effects = client.handle(BrokerClientEvent::CallbackRejected {
        callback_id: 1,
        error_message: "boom".to_string(),
        now_ms: 4,
    });

    assert_eq!(
        effects,
        vec![BrokerClientEffect::PostToBroker {
            port_id: 1,
            message: BrowserBrokerTabMessage::LeaderFailed {
                broker_instance_id: "instance-a".to_string(),
                leadership_id: 2,
                reason: "boom".to_string(),
            },
        }],
    );
}

#[test]
fn future_demote_invokes_callback_without_clearing_current_role() {
    let mut client = connected_client();
    let _ = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::LeaderReady {
            broker_instance_id: "instance-a".to_string(),
            leader_tab_id: "tab-a".to_string(),
            leadership_id: 1,
        },
        respond_to_broker_ping: true,
        now_ms: 3,
    });

    let effects = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::Demote {
            broker_instance_id: "instance-a".to_string(),
            leadership_id: 2,
        },
        respond_to_broker_ping: true,
        now_ms: 4,
    });

    assert_eq!(
        effects,
        vec![BrokerClientEffect::InvokeCallback {
            callback_id: 1,
            callback: BrokerClientCallback::Demote { leadership_id: 2 },
        }],
    );
    assert_eq!(client.snapshot().role, BrokerRole::Leader);
    assert_eq!(client.snapshot().leadership_id, 1);
}

#[test]
fn wait_for_role_resolves_after_follower_ready() {
    let mut client = connected_client();

    let effects = client.handle(BrokerClientEvent::PublicCommand {
        command: BrokerClientCommand::WaitForRole {
            role: BrokerRole::Follower,
            promise_id: 42,
            timeout_ms: 5_000,
        },
    });

    assert_eq!(
        effects,
        vec![BrokerClientEffect::ArmTimer {
            timer_id: 4,
            kind: BrokerClientTimerKind::RoleWaiter { promise_id: 42 },
            delay_ms: 5_000,
        }],
    );

    let effects = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::FollowerReady {
            broker_instance_id: "instance-a".to_string(),
            leader_tab_id: "leader-a".to_string(),
            leadership_id: 3,
        },
        respond_to_broker_ping: true,
        now_ms: 4,
    });

    assert_eq!(
        effects,
        vec![
            BrokerClientEffect::InvokeCallback {
                callback_id: 1,
                callback: BrokerClientCallback::FollowerReady {
                    leader_tab_id: "leader-a".to_string(),
                    leadership_id: 3,
                },
            },
            BrokerClientEffect::CancelTimer { timer_id: 4 },
            BrokerClientEffect::ResolvePublicPromise { promise_id: 42 },
        ],
    );
}

#[test]
fn stale_follower_port_messages_release_transferred_ports() {
    let mut client = connected_client();
    let _ = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::LeaderReady {
            broker_instance_id: "instance-a".to_string(),
            leader_tab_id: "tab-a".to_string(),
            leadership_id: 5,
        },
        respond_to_broker_ping: true,
        now_ms: 4,
    });

    let stale_attach_effects = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::AttachFollowerPort {
            broker_instance_id: "instance-a".to_string(),
            follower_tab_id: "tab-b".to_string(),
            leadership_id: 4,
            port_id: Some(7),
        },
        respond_to_broker_ping: true,
        now_ms: 5,
    });
    assert_eq!(
        stale_attach_effects,
        vec![BrokerClientEffect::ReleaseMessagePort { port_id: 7 }],
    );

    let stale_use_effects = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::UseFollowerPort {
            broker_instance_id: "instance-a".to_string(),
            leader_tab_id: "tab-old".to_string(),
            leadership_id: 4,
            port_id: Some(8),
        },
        respond_to_broker_ping: true,
        now_ms: 6,
    });
    assert_eq!(
        stale_use_effects,
        vec![BrokerClientEffect::ReleaseMessagePort { port_id: 8 }],
    );
    assert_eq!(client.snapshot().role, BrokerRole::Leader);
    assert_eq!(client.snapshot().leadership_id, 5);
}

#[test]
fn reconnect_replays_latest_visibility_and_pending_reset() {
    let mut client = connected_client();
    let _ = client.handle(BrokerClientEvent::PublicCommand {
        command: BrokerClientCommand::ReportVisibility {
            visibility: BrokerVisibility::Hidden,
        },
    });

    let effects = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::BrokerPing {
            broker_instance_id: "instance-b".to_string(),
        },
        respond_to_broker_ping: true,
        now_ms: 10,
    });

    assert_eq!(
        effects,
        vec![
            BrokerClientEffect::CancelTimer { timer_id: 3 },
            BrokerClientEffect::DetachPort {
                port_id: 1,
                close: true,
            },
            BrokerClientEffect::CreateSharedWorker {
                worker_id: 2,
                name: "jazz-broker:app:db".to_string(),
            },
            BrokerClientEffect::AttachPortListeners { port_id: 2 },
            BrokerClientEffect::PostToBroker {
                port_id: 2,
                message: BrowserBrokerTabMessage::Hello {
                    tab_id: "tab-a".to_string(),
                    app_id: "app".to_string(),
                    db_name: "db".to_string(),
                    fingerprint: "fingerprint".to_string(),
                    visibility: BrokerVisibility::Hidden,
                    force_takeover_timeout_ms: None,
                    broker_ping_interval_ms: Some(10),
                    broker_pong_timeout_ms: Some(20),
                },
            },
            BrokerClientEffect::ArmTimer {
                timer_id: 4,
                kind: BrokerClientTimerKind::BrokerHello,
                delay_ms: 5_000,
            },
        ],
    );

    let effects = client.handle(BrokerClientEvent::PublicCommand {
        command: BrokerClientCommand::RequestStorageReset {
            request_id: "reset-a".to_string(),
            start_promise_id: 10,
            completion_promise_id: 11,
        },
    });
    assert!(effects.is_empty());

    let _ = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::BrokerHello {
            broker_instance_id: "instance-b".to_string(),
        },
        respond_to_broker_ping: true,
        now_ms: 11,
    });

    let effects = client.handle(BrokerClientEvent::TimerFired {
        timer_id: 5,
        kind: BrokerClientTimerKind::InitialLeadership,
        now_ms: 112,
    });

    assert_eq!(
        effects,
        vec![
            BrokerClientEffect::CancelTimer { timer_id: 5 },
            BrokerClientEffect::ArmTimer {
                timer_id: 6,
                kind: BrokerClientTimerKind::BrokerLiveness,
                delay_ms: 30,
            },
            BrokerClientEffect::PostToBroker {
                port_id: 2,
                message: BrowserBrokerTabMessage::Visibility {
                    broker_instance_id: "instance-b".to_string(),
                    visibility: BrokerVisibility::Hidden,
                },
            },
            BrokerClientEffect::PostToBroker {
                port_id: 2,
                message: BrowserBrokerTabMessage::StorageResetRequest {
                    broker_instance_id: "instance-b".to_string(),
                    request_id: "reset-a".to_string(),
                },
            },
            BrokerClientEffect::ArmTimer {
                timer_id: 7,
                kind: BrokerClientTimerKind::StorageResetStart {
                    request_id: "reset-a".to_string(),
                    promise_id: 10,
                },
                delay_ms: 30,
            },
            BrokerClientEffect::InvokeCallback {
                callback_id: 1,
                callback: BrokerClientCallback::Reconnected,
            },
        ],
    );
}

#[test]
fn reconnect_replayed_storage_reset_resolves_start_waiter_once() {
    let mut client = connected_client();
    let _ = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::BrokerPing {
            broker_instance_id: "instance-b".to_string(),
        },
        respond_to_broker_ping: true,
        now_ms: 10,
    });
    let _ = client.handle(BrokerClientEvent::PublicCommand {
        command: BrokerClientCommand::RequestStorageReset {
            request_id: "reset-a".to_string(),
            start_promise_id: 10,
            completion_promise_id: 11,
        },
    });
    let _ = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::BrokerHello {
            broker_instance_id: "instance-b".to_string(),
        },
        respond_to_broker_ping: true,
        now_ms: 11,
    });
    let _ = client.handle(BrokerClientEvent::TimerFired {
        timer_id: 5,
        kind: BrokerClientTimerKind::InitialLeadership,
        now_ms: 112,
    });

    let effects = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::StorageResetStarted {
            broker_instance_id: "instance-b".to_string(),
            request_id: "reset-a".to_string(),
        },
        respond_to_broker_ping: true,
        now_ms: 113,
    });

    assert_eq!(
        effects,
        vec![
            BrokerClientEffect::CancelTimer { timer_id: 7 },
            BrokerClientEffect::ResolvePublicPromise { promise_id: 10 },
        ],
    );
}

#[test]
fn storage_reset_start_timeout_rejects_only_start_waiter() {
    let mut client = connected_client();
    let _ = client.handle(BrokerClientEvent::PublicCommand {
        command: BrokerClientCommand::RequestStorageReset {
            request_id: "reset-a".to_string(),
            start_promise_id: 10,
            completion_promise_id: 11,
        },
    });

    let effects = client.handle(BrokerClientEvent::TimerFired {
        timer_id: 4,
        kind: BrokerClientTimerKind::StorageResetStart {
            request_id: "reset-a".to_string(),
            promise_id: 10,
        },
        now_ms: 40,
    });

    assert_eq!(
        effects,
        vec![BrokerClientEffect::RejectPublicPromise {
            promise_id: 10,
            reason: "Timed out waiting for browser storage reset reset-a to start".to_string(),
        }],
    );

    let effects = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::StorageResetFinished {
            broker_instance_id: "instance-a".to_string(),
            request_id: "reset-a".to_string(),
            success: true,
            error_message: None,
        },
        respond_to_broker_ping: true,
        now_ms: 41,
    });

    assert_eq!(
        effects,
        vec![BrokerClientEffect::ResolvePublicPromise { promise_id: 11 }],
    );
}

#[test]
fn broker_ping_callback_can_suppress_pong() {
    let mut client = connected_client();

    let effects = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::BrokerPing {
            broker_instance_id: "instance-a".to_string(),
        },
        respond_to_broker_ping: false,
        now_ms: 10,
    });

    assert!(effects.iter().any(|effect| {
        matches!(
            effect,
            BrokerClientEffect::InvokeCallback {
                callback: BrokerClientCallback::BrokerPing,
                ..
            }
        )
    }));
    assert!(
        effects.iter().all(|effect| {
            !matches!(
                effect,
                BrokerClientEffect::PostToBroker {
                    message: BrowserBrokerTabMessage::BrokerPong { .. },
                    ..
                }
            )
        }),
        "broker-pong should not be emitted when respond_to_broker_ping is false: {effects:#?}",
    );
}

#[test]
fn reconnect_during_storage_reset_rejects_active_waiters() {
    let mut client = connected_client();
    let _ = client.handle(BrokerClientEvent::PublicCommand {
        command: BrokerClientCommand::RequestStorageReset {
            request_id: "reset-a".to_string(),
            start_promise_id: 10,
            completion_promise_id: 11,
        },
    });

    let effects = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::BrokerPing {
            broker_instance_id: "instance-b".to_string(),
        },
        respond_to_broker_ping: true,
        now_ms: 10,
    });

    assert!(effects.contains(&BrokerClientEffect::RejectPublicPromise {
        promise_id: 10,
        reason: "Browser broker restarted during storage reset".to_string(),
    }));
    assert!(effects.contains(&BrokerClientEffect::RejectPublicPromise {
        promise_id: 11,
        reason: "Browser broker restarted during storage reset".to_string(),
    }));
}

#[test]
fn reconnect_rejects_reset_waiters_in_request_order() {
    let mut client = connected_client();
    let _ = client.handle(BrokerClientEvent::PublicCommand {
        command: BrokerClientCommand::RequestStorageReset {
            request_id: "reset-b".to_string(),
            start_promise_id: 10,
            completion_promise_id: 11,
        },
    });
    let _ = client.handle(BrokerClientEvent::PublicCommand {
        command: BrokerClientCommand::RequestStorageReset {
            request_id: "reset-a".to_string(),
            start_promise_id: 12,
            completion_promise_id: 13,
        },
    });

    let effects = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::BrokerPing {
            broker_instance_id: "instance-b".to_string(),
        },
        respond_to_broker_ping: true,
        now_ms: 10,
    });

    let rejected_promise_ids: Vec<u64> = effects
        .iter()
        .filter_map(|effect| match effect {
            BrokerClientEffect::RejectPublicPromise { promise_id, .. } => Some(*promise_id),
            _ => None,
        })
        .collect();
    assert_eq!(rejected_promise_ids, vec![12, 10, 13, 11]);
}

#[test]
fn storage_reset_begin_callback_success_posts_ready() {
    let mut client = connected_client();

    let effects = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::StorageResetBegin {
            broker_instance_id: "instance-a".to_string(),
            request_id: "reset-a".to_string(),
            leadership_id: 3,
        },
        respond_to_broker_ping: true,
        now_ms: 10,
    });

    assert_eq!(
        effects,
        vec![BrokerClientEffect::InvokeCallback {
            callback_id: 1,
            callback: BrokerClientCallback::StorageResetBegin {
                request_id: "reset-a".to_string(),
                leadership_id: 3,
            },
        }],
    );

    let effects = client.handle(BrokerClientEvent::CallbackResolved {
        callback_id: 1,
        now_ms: 11,
    });

    assert_eq!(
        effects,
        vec![BrokerClientEffect::PostToBroker {
            port_id: 1,
            message: BrowserBrokerTabMessage::StorageResetReady {
                broker_instance_id: "instance-a".to_string(),
                request_id: "reset-a".to_string(),
                success: true,
                error_message: None,
            },
        }],
    );
}

#[test]
fn storage_reset_begin_callback_failure_posts_not_ready() {
    let mut client = connected_client();

    let effects = client.handle(BrokerClientEvent::BrokerMessageReceived {
        message: BrokerControlMessage::StorageResetBegin {
            broker_instance_id: "instance-a".to_string(),
            request_id: "reset-a".to_string(),
            leadership_id: 3,
        },
        respond_to_broker_ping: true,
        now_ms: 10,
    });

    assert_eq!(
        effects,
        vec![BrokerClientEffect::InvokeCallback {
            callback_id: 1,
            callback: BrokerClientCallback::StorageResetBegin {
                request_id: "reset-a".to_string(),
                leadership_id: 3,
            },
        }],
    );

    let effects = client.handle(BrokerClientEvent::CallbackRejected {
        callback_id: 1,
        error_message: "reset failed".to_string(),
        now_ms: 11,
    });

    assert_eq!(
        effects,
        vec![BrokerClientEffect::PostToBroker {
            port_id: 1,
            message: BrowserBrokerTabMessage::StorageResetReady {
                broker_instance_id: "instance-a".to_string(),
                request_id: "reset-a".to_string(),
                success: false,
                error_message: Some("reset failed".to_string()),
            },
        }],
    );
}

#[test]
fn broker_client_snapshot_serializes_for_js() {
    let client = connected_client();
    let value = serde_json::to_value(client.snapshot()).unwrap();
    assert_eq!(value["brokerInstanceId"], "instance-a");
    assert_eq!(value["role"], "follower");
    assert_eq!(value["tabId"], "tab-a");
}
