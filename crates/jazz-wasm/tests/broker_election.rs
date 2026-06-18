use jazz_wasm::broker_election::{
    BrokerControlMessage, BrokerEffect, BrokerElectionCore, BrokerEvent, BrokerTimerId,
    BrokerVisibility, LeaderReady, StorageResetReady, TabConnected,
};

const BROKER_ID: &str = "broker-test";
const APP_ID: &str = "app";
const DB_NAME: &str = "db";
const FINGERPRINT: &str = "fingerprint-a";
const SCHEMA_A: &str = "schema-a";
const SCHEMA_B: &str = "schema-b";

fn connect(tab_id: &str, now_ms: u64) -> BrokerEvent {
    connect_with_options(tab_id, now_ms, None, None, None)
}

fn connect_with_options(
    tab_id: &str,
    now_ms: u64,
    force_takeover_timeout_ms: Option<u32>,
    broker_ping_interval_ms: Option<u32>,
    broker_pong_timeout_ms: Option<u32>,
) -> BrokerEvent {
    BrokerEvent::TabConnected(TabConnected {
        tab_id: tab_id.to_string(),
        app_id: APP_ID.to_string(),
        db_name: DB_NAME.to_string(),
        fingerprint: FINGERPRINT.to_string(),
        visibility: BrokerVisibility::Visible,
        now_ms,
        force_takeover_timeout_ms,
        broker_ping_interval_ms,
        broker_pong_timeout_ms,
    })
}

fn schema(tab_id: &str, schema_fingerprint: &str) -> BrokerEvent {
    BrokerEvent::SchemaReported {
        tab_id: tab_id.to_string(),
        schema_fingerprint: schema_fingerprint.to_string(),
    }
}

fn tab_lock_name() -> String {
    format!("jazz-leader-tab:{APP_ID}:{DB_NAME}")
}

fn worker_lock_name() -> String {
    format!("jazz-leader-worker:{APP_ID}:{DB_NAME}")
}

fn leader_ready(tab_id: &str, leadership_id: u32, now_ms: u64) -> BrokerEvent {
    BrokerEvent::LeaderReady(LeaderReady {
        tab_id: tab_id.to_string(),
        leadership_id,
        tab_lock_name: tab_lock_name(),
        worker_lock_name: worker_lock_name(),
        bridgeless_storage_reset: false,
        now_ms,
    })
}

fn storage_ready(tab_id: &str, request_id: &str, success: bool, now_ms: u64) -> BrokerEvent {
    BrokerEvent::StorageResetReady(StorageResetReady {
        tab_id: tab_id.to_string(),
        request_id: request_id.to_string(),
        success,
        error_message: if success {
            None
        } else {
            Some("prepare exploded".to_string())
        },
        now_ms,
    })
}

fn request_storage_reset(tab_id: &str, request_id: &str, now_ms: u64) -> BrokerEvent {
    BrokerEvent::StorageResetRequested {
        tab_id: tab_id.to_string(),
        request_id: request_id.to_string(),
        now_ms,
    }
}

fn send_to(tab_id: &str, message: BrokerControlMessage) -> BrokerEffect {
    BrokerEffect::SendToTab {
        tab_id: tab_id.to_string(),
        message,
    }
}

fn message_become_leader(
    leadership_id: u32,
    reset_request_id: Option<&str>,
) -> BrokerControlMessage {
    BrokerControlMessage::BecomeLeader {
        broker_instance_id: BROKER_ID.to_string(),
        leadership_id,
        reset_request_id: reset_request_id.map(str::to_string),
    }
}

fn message_demote(leadership_id: u32) -> BrokerControlMessage {
    BrokerControlMessage::Demote {
        broker_instance_id: BROKER_ID.to_string(),
        leadership_id,
    }
}

fn message_storage_reset_finished(
    request_id: &str,
    success: bool,
    error_message: Option<&str>,
) -> BrokerControlMessage {
    BrokerControlMessage::StorageResetFinished {
        broker_instance_id: BROKER_ID.to_string(),
        request_id: request_id.to_string(),
        success,
        error_message: error_message.map(str::to_string),
    }
}

fn assert_contains_effect(effects: &[BrokerEffect], expected: BrokerEffect) {
    assert!(
        effects.contains(&expected),
        "expected effect {expected:?} in {effects:#?}",
    );
}

fn assert_not_contains_steal(effects: &[BrokerEffect]) {
    assert!(
        effects
            .iter()
            .all(|effect| !matches!(effect, BrokerEffect::StealPreviousLeaderLocks { .. })),
        "expected no stale lock-steal effect, got {effects:#?}",
    );
}

fn find_wait_election(effects: &[BrokerEffect]) -> u64 {
    effects
        .iter()
        .find_map(|effect| match effect {
            BrokerEffect::WaitForPreviousLeaderLocks { election_id, .. } => Some(*election_id),
            _ => None,
        })
        .expect("expected a previous-lock wait effect")
}

fn complete_previous_locks(election_id: u64, now_ms: u64) -> BrokerEvent {
    BrokerEvent::PreviousLeaderLocksReleased {
        election_id,
        now_ms,
    }
}

fn force_takeover_timer(election_id: u64, now_ms: u64) -> BrokerEvent {
    BrokerEvent::TimerFired {
        timer_id: BrokerTimerId::PreviousLeaderLocksForceTakeover { election_id },
        now_ms,
    }
}

fn finish_force_takeover(election_id: u64, now_ms: u64) -> BrokerEvent {
    BrokerEvent::ForceTakeoverComplete {
        election_id,
        now_ms,
    }
}

fn fail_force_takeover(election_id: u64, now_ms: u64) -> BrokerEvent {
    BrokerEvent::ForceTakeoverFailed {
        election_id,
        reason: "steal failed".to_string(),
        now_ms,
    }
}

fn follower_attached(
    leader_tab_id: &str,
    follower_tab_id: &str,
    leadership_id: u32,
) -> BrokerEvent {
    BrokerEvent::FollowerPortAttached {
        leader_tab_id: leader_tab_id.to_string(),
        follower_tab_id: follower_tab_id.to_string(),
        leadership_id,
        now_ms: 100,
    }
}

fn setup_ready_leader_with_follower() -> BrokerElectionCore {
    let mut broker = BrokerElectionCore::new(BROKER_ID.to_string());
    let _ = broker.handle(connect("tab-a", 10));
    let _ = broker.handle(schema("tab-a", SCHEMA_A));
    let _ = broker.handle(leader_ready("tab-a", 1, 20));
    let _ = broker.handle(connect("tab-b", 30));
    let _ = broker.handle(schema("tab-b", SCHEMA_A));
    let _ = broker.handle(follower_attached("tab-a", "tab-b", 1));
    broker
}

fn setup_ready_leader_with_two_followers() -> BrokerElectionCore {
    let mut broker = setup_ready_leader_with_follower();
    let _ = broker.handle(connect("tab-c", 40));
    let _ = broker.handle(schema("tab-c", SCHEMA_A));
    let _ = broker.handle(follower_attached("tab-a", "tab-c", 1));
    broker
}

#[test]
fn one_namespace_elects_one_leader() {
    let mut broker = BrokerElectionCore::new("broker-test".to_string());

    let effects = broker.handle(connect("tab-a", 10));

    assert_eq!(
        effects,
        vec![
            BrokerEffect::SendToTab {
                tab_id: "tab-a".to_string(),
                message: BrokerControlMessage::BrokerPing {
                    broker_instance_id: "broker-test".to_string(),
                },
            },
            BrokerEffect::ArmTimer {
                timer_id: BrokerTimerId::BrokerPing,
                delay_ms: 1_000,
            },
            BrokerEffect::SendToTab {
                tab_id: "tab-a".to_string(),
                message: BrokerControlMessage::BrokerHello {
                    broker_instance_id: "broker-test".to_string(),
                },
            },
            BrokerEffect::SendToTab {
                tab_id: "tab-a".to_string(),
                message: BrokerControlMessage::BecomeLeader {
                    broker_instance_id: "broker-test".to_string(),
                    leadership_id: 1,
                    reset_request_id: None,
                },
            },
        ],
    );

    let snapshot = broker.snapshot();
    assert_eq!(snapshot.leader_tab_id.as_deref(), Some("tab-a"));
    assert_eq!(snapshot.current_leadership_id, 1);
    assert_eq!(snapshot.connected_tab_count, 1);
}

#[test]
fn mismatched_configuration_is_rejected() {
    let mut broker = BrokerElectionCore::new("broker-test".to_string());
    let _ = broker.handle(connect("tab-a", 10));

    let effects = broker.handle(BrokerEvent::TabConnected(TabConnected {
        tab_id: "tab-b".to_string(),
        app_id: "app".to_string(),
        db_name: "db".to_string(),
        fingerprint: "fingerprint-b".to_string(),
        visibility: BrokerVisibility::Visible,
        now_ms: 20,
        force_takeover_timeout_ms: None,
        broker_ping_interval_ms: None,
        broker_pong_timeout_ms: None,
    }));

    assert_eq!(
        effects,
        vec![
            BrokerEffect::SendToTab {
                tab_id: "tab-b".to_string(),
                message: BrokerControlMessage::Unsupported {
                    broker_instance_id: "broker-test".to_string(),
                    code: Some("incompatible-browser-broker-configuration".to_string()),
                    reason: "incompatible persistent browser configuration".to_string(),
                },
            },
            BrokerEffect::CloseTabPort {
                tab_id: "tab-b".to_string(),
            },
        ],
    );

    assert_eq!(broker.snapshot().connected_tab_count, 1);
}

#[test]
fn leader_ready_announces_and_assigns_followers() {
    let mut broker = BrokerElectionCore::new("broker-test".to_string());
    let _ = broker.handle(connect("tab-a", 10));
    let _ = broker.handle(BrokerEvent::SchemaReported {
        tab_id: "tab-a".to_string(),
        schema_fingerprint: "schema-a".to_string(),
    });
    let _ = broker.handle(connect("tab-b", 20));
    let _ = broker.handle(BrokerEvent::SchemaReported {
        tab_id: "tab-b".to_string(),
        schema_fingerprint: "schema-a".to_string(),
    });

    let effects = broker.handle(BrokerEvent::LeaderReady(LeaderReady {
        tab_id: "tab-a".to_string(),
        leadership_id: 1,
        tab_lock_name: "tab-lock".to_string(),
        worker_lock_name: "worker-lock".to_string(),
        bridgeless_storage_reset: false,
        now_ms: 30,
    }));

    assert_eq!(
        effects,
        vec![
            BrokerEffect::Broadcast {
                message: BrokerControlMessage::LeaderReady {
                    broker_instance_id: "broker-test".to_string(),
                    leader_tab_id: "tab-a".to_string(),
                    leadership_id: 1,
                },
            },
            BrokerEffect::StartLeaderLockMonitor {
                leadership_id: 1,
                tab_lock_name: "tab-lock".to_string(),
                worker_lock_name: "worker-lock".to_string(),
            },
            BrokerEffect::AssignFollowerPort {
                leader_tab_id: "tab-a".to_string(),
                follower_tab_id: "tab-b".to_string(),
                leadership_id: 1,
            },
            BrokerEffect::ArmTimer {
                timer_id: BrokerTimerId::FollowerAttachment {
                    follower_tab_id: "tab-b".to_string(),
                    leadership_id: 1,
                },
                delay_ms: 1_000,
            },
        ],
    );
}

#[test]
fn follower_ports_retry_until_attached() {
    let mut broker = BrokerElectionCore::new("broker-test".to_string());
    let _ = broker.handle(connect("tab-a", 10));
    let _ = broker.handle(BrokerEvent::SchemaReported {
        tab_id: "tab-a".to_string(),
        schema_fingerprint: "schema-a".to_string(),
    });
    let _ = broker.handle(connect("tab-b", 20));
    let _ = broker.handle(BrokerEvent::SchemaReported {
        tab_id: "tab-b".to_string(),
        schema_fingerprint: "schema-a".to_string(),
    });
    let _ = broker.handle(BrokerEvent::LeaderReady(LeaderReady {
        tab_id: "tab-a".to_string(),
        leadership_id: 1,
        tab_lock_name: "tab-lock".to_string(),
        worker_lock_name: "worker-lock".to_string(),
        bridgeless_storage_reset: false,
        now_ms: 30,
    }));

    let retry_effects = broker.handle(BrokerEvent::TimerFired {
        timer_id: BrokerTimerId::FollowerAttachment {
            follower_tab_id: "tab-b".to_string(),
            leadership_id: 1,
        },
        now_ms: 1_020,
    });

    assert_eq!(
        retry_effects,
        vec![
            BrokerEffect::AssignFollowerPort {
                leader_tab_id: "tab-a".to_string(),
                follower_tab_id: "tab-b".to_string(),
                leadership_id: 1,
            },
            BrokerEffect::ArmTimer {
                timer_id: BrokerTimerId::FollowerAttachment {
                    follower_tab_id: "tab-b".to_string(),
                    leadership_id: 1,
                },
                delay_ms: 2_000,
            },
        ],
    );

    let attached_effects = broker.handle(BrokerEvent::FollowerPortAttached {
        leader_tab_id: "tab-a".to_string(),
        follower_tab_id: "tab-b".to_string(),
        leadership_id: 1,
        now_ms: 3_020,
    });

    assert_eq!(
        attached_effects,
        vec![
            BrokerEffect::CancelTimer {
                timer_id: BrokerTimerId::FollowerAttachment {
                    follower_tab_id: "tab-b".to_string(),
                    leadership_id: 1,
                },
            },
            BrokerEffect::SendToTab {
                tab_id: "tab-b".to_string(),
                message: BrokerControlMessage::FollowerReady {
                    broker_instance_id: "broker-test".to_string(),
                    leader_tab_id: "tab-a".to_string(),
                    leadership_id: 1,
                },
            },
        ],
    );

    let stale_retry_effects = broker.handle(BrokerEvent::TimerFired {
        timer_id: BrokerTimerId::FollowerAttachment {
            follower_tab_id: "tab-b".to_string(),
            leadership_id: 1,
        },
        now_ms: 3_020,
    });
    assert!(stale_retry_effects.is_empty());
}

#[test]
fn schema_mismatch_blocks_tab() {
    let mut broker = BrokerElectionCore::new("broker-test".to_string());
    let _ = broker.handle(connect("tab-a", 10));
    let _ = broker.handle(BrokerEvent::SchemaReported {
        tab_id: "tab-a".to_string(),
        schema_fingerprint: "schema-a".to_string(),
    });
    let _ = broker.handle(connect("tab-b", 20));

    let effects = broker.handle(BrokerEvent::SchemaReported {
        tab_id: "tab-b".to_string(),
        schema_fingerprint: "schema-b".to_string(),
    });

    assert_eq!(
        effects,
        vec![BrokerEffect::SendToTab {
            tab_id: "tab-b".to_string(),
            message: BrokerControlMessage::SchemaBlocked {
                broker_instance_id: "broker-test".to_string(),
                reason: "incompatible persistent browser schema".to_string(),
            },
        },],
    );
}

#[test]
fn schema_mismatched_tab_is_adopted_when_canonical_holder_departs() {
    let mut broker = BrokerElectionCore::new(BROKER_ID.to_string());
    let _ = broker.handle(connect("tab-a", 10));
    let _ = broker.handle(schema("tab-a", SCHEMA_A));
    let _ = broker.handle(leader_ready("tab-a", 1, 20));
    let _ = broker.handle(connect("tab-b", 30));
    let blocked = broker.handle(schema("tab-b", SCHEMA_B));
    assert_contains_effect(
        &blocked,
        send_to(
            "tab-b",
            BrokerControlMessage::SchemaBlocked {
                broker_instance_id: BROKER_ID.to_string(),
                reason: "incompatible persistent browser schema".to_string(),
            },
        ),
    );

    let shutdown_effects = broker.handle(BrokerEvent::Shutdown {
        tab_id: "tab-a".to_string(),
        now_ms: 40,
    });
    let election_id = find_wait_election(&shutdown_effects);

    let promotion_effects = broker.handle(complete_previous_locks(election_id, 50));

    assert_contains_effect(
        &promotion_effects,
        send_to("tab-b", message_become_leader(2, None)),
    );
    let snapshot = broker.snapshot();
    assert_eq!(snapshot.schema_fingerprint.as_deref(), Some(SCHEMA_B));
    assert_eq!(snapshot.leader_tab_id.as_deref(), Some("tab-b"));
    assert_eq!(snapshot.current_leadership_id, 2);
}

#[test]
fn replacement_leader_survives_non_ready_schema_leader_shutdown() {
    let mut broker = BrokerElectionCore::new(BROKER_ID.to_string());
    let _ = broker.handle(connect("tab-a", 10));
    let _ = broker.handle(schema("tab-a", SCHEMA_A));
    let _ = broker.handle(connect("tab-b", 20));
    let _ = broker.handle(schema("tab-b", SCHEMA_B));

    let effects = broker.handle(BrokerEvent::Shutdown {
        tab_id: "tab-a".to_string(),
        now_ms: 30,
    });

    assert_contains_effect(&effects, send_to("tab-b", message_become_leader(2, None)));
    let snapshot = broker.snapshot();
    assert_eq!(snapshot.schema_fingerprint.as_deref(), Some(SCHEMA_B));
    assert_eq!(snapshot.leader_tab_id.as_deref(), Some("tab-b"));
    assert!(!snapshot.leader_ready);
}

#[test]
fn current_leader_lock_release_promotes_follower() {
    let mut broker = setup_ready_leader_with_follower();

    let release_effects = broker.handle(BrokerEvent::LeaderLockReleased {
        leadership_id: 1,
        now_ms: 40,
    });
    let election_id = find_wait_election(&release_effects);
    assert_eq!(broker.snapshot().connected_tab_count, 1);

    let promotion_effects = broker.handle(complete_previous_locks(election_id, 50));

    assert_contains_effect(
        &promotion_effects,
        send_to("tab-b", message_become_leader(2, None)),
    );
    assert_eq!(broker.snapshot().leader_tab_id.as_deref(), Some("tab-b"));
    assert_eq!(broker.snapshot().current_leadership_id, 2);
}

#[test]
fn leadership_ids_stay_monotonic_after_idle_reset() {
    let mut broker = BrokerElectionCore::new(BROKER_ID.to_string());
    let _ = broker.handle(connect("tab-a", 10));
    let shutdown_effects = broker.handle(BrokerEvent::Shutdown {
        tab_id: "tab-a".to_string(),
        now_ms: 20,
    });

    assert_contains_effect(
        &shutdown_effects,
        BrokerEffect::CancelTimer {
            timer_id: BrokerTimerId::BrokerPing,
        },
    );
    assert_eq!(broker.snapshot().connected_tab_count, 0);
    assert_eq!(broker.snapshot().current_leadership_id, 1);

    let reconnect_effects = broker.handle(connect("tab-b", 30));

    assert_contains_effect(
        &reconnect_effects,
        send_to("tab-b", message_become_leader(2, None)),
    );
    assert_eq!(broker.snapshot().current_leadership_id, 2);
}

#[test]
fn stuck_leader_locks_are_stolen_before_replacement_promotion() {
    let mut broker = setup_ready_leader_with_follower();

    let shutdown_effects = broker.handle(BrokerEvent::Shutdown {
        tab_id: "tab-a".to_string(),
        now_ms: 40,
    });
    let election_id = find_wait_election(&shutdown_effects);

    let steal_effects = broker.handle(force_takeover_timer(election_id, 1_040));
    assert_eq!(
        steal_effects,
        vec![BrokerEffect::StealPreviousLeaderLocks {
            election_id,
            tab_lock_name: tab_lock_name(),
            worker_lock_name: worker_lock_name(),
        }],
    );

    let promotion_effects = broker.handle(finish_force_takeover(election_id, 1_050));

    assert_contains_effect(
        &promotion_effects,
        send_to("tab-b", message_become_leader(2, None)),
    );
    assert_eq!(broker.snapshot().leader_tab_id.as_deref(), Some("tab-b"));
}

#[test]
fn failed_force_takeover_does_not_promote_replacement_leader() {
    let mut broker = setup_ready_leader_with_follower();

    let shutdown_effects = broker.handle(BrokerEvent::Shutdown {
        tab_id: "tab-a".to_string(),
        now_ms: 40,
    });
    let election_id = find_wait_election(&shutdown_effects);
    let _ = broker.handle(force_takeover_timer(election_id, 1_040));

    let failure_effects = broker.handle(fail_force_takeover(election_id, 1_050));

    assert_eq!(
        failure_effects,
        vec![BrokerEffect::ArmTimer {
            timer_id: BrokerTimerId::PreviousLeaderLocksForceTakeover { election_id },
            delay_ms: 1_000,
        }],
    );
    assert_eq!(broker.snapshot().leader_tab_id.as_deref(), None);
    assert_eq!(broker.snapshot().replacement_election_id, Some(election_id));
}

#[test]
fn failed_force_takeover_keeps_storage_reset_wait_pending() {
    let mut broker = setup_ready_leader_with_two_followers();
    let request_id = "reset-a";

    let _ = broker.handle(request_storage_reset("tab-b", request_id, 50));
    let _ = broker.handle(storage_ready("tab-a", request_id, true, 60));
    let _ = broker.handle(storage_ready("tab-b", request_id, true, 61));
    let ready_effects = broker.handle(storage_ready("tab-c", request_id, true, 62));
    let election_id = find_wait_election(&ready_effects);
    let _ = broker.handle(force_takeover_timer(election_id, 1_062));

    let failure_effects = broker.handle(fail_force_takeover(election_id, 1_063));

    assert_eq!(
        failure_effects,
        vec![BrokerEffect::ArmTimer {
            timer_id: BrokerTimerId::PreviousLeaderLocksForceTakeover { election_id },
            delay_ms: 1_000,
        }],
    );
    assert_eq!(broker.snapshot().reset_phase.as_deref(), Some("promoting"));

    let retry_effects = broker.handle(force_takeover_timer(election_id, 2_063));

    assert_eq!(
        retry_effects,
        vec![BrokerEffect::StealPreviousLeaderLocks {
            election_id,
            tab_lock_name: tab_lock_name(),
            worker_lock_name: worker_lock_name(),
        }],
    );
}

#[test]
fn stale_replacement_election_does_not_steal_new_leader_reused_locks() {
    let mut broker = setup_ready_leader_with_follower();
    let shutdown_effects = broker.handle(BrokerEvent::Shutdown {
        tab_id: "tab-a".to_string(),
        now_ms: 40,
    });
    let election_id = find_wait_election(&shutdown_effects);
    let _ = broker.handle(complete_previous_locks(election_id, 50));

    let stale_timer_effects = broker.handle(force_takeover_timer(election_id, 1_040));
    let stale_completion_effects = broker.handle(finish_force_takeover(election_id, 1_050));

    assert_not_contains_steal(&stale_timer_effects);
    assert!(stale_completion_effects.is_empty());
    assert_eq!(broker.snapshot().leader_tab_id.as_deref(), Some("tab-b"));
}

#[test]
fn promoted_tab_without_schema_is_replaced_before_ready() {
    let mut broker = BrokerElectionCore::new(BROKER_ID.to_string());
    let _ = broker.handle(connect("tab-a", 10));
    let _ = broker.handle(connect("tab-b", 20));

    let effects = broker.handle(schema("tab-b", SCHEMA_A));

    assert_contains_effect(&effects, send_to("tab-a", message_demote(1)));
    assert_contains_effect(&effects, send_to("tab-b", message_become_leader(2, None)));
    assert_eq!(broker.snapshot().leader_tab_id.as_deref(), Some("tab-b"));
}

#[test]
fn failed_candidate_is_demoted_without_closing_tab() {
    let mut broker = BrokerElectionCore::new(BROKER_ID.to_string());
    let _ = broker.handle(connect("tab-a", 10));

    let effects = broker.handle(BrokerEvent::LeaderFailed {
        tab_id: "tab-a".to_string(),
        leadership_id: 1,
        reason: "boom".to_string(),
        now_ms: 20,
    });

    assert_contains_effect(&effects, send_to("tab-a", message_demote(1)));
    assert!(
        !effects.iter().any(
            |effect| matches!(effect, BrokerEffect::CloseTabPort { tab_id } if tab_id == "tab-a")
        ),
        "failed candidate should remain connected: {effects:#?}",
    );
    assert_eq!(broker.snapshot().connected_tab_count, 1);
}

#[test]
fn failed_candidate_that_never_reports_ready_does_not_block_future_promotion() {
    let mut broker = BrokerElectionCore::new(BROKER_ID.to_string());
    let _ = broker.handle(connect("tab-a", 10));
    let failed_effects = broker.handle(BrokerEvent::LeaderFailed {
        tab_id: "tab-a".to_string(),
        leadership_id: 1,
        reason: "worker bootstrap hung".to_string(),
        now_ms: 20,
    });
    let election_id = find_wait_election(&failed_effects);
    let _ = broker.handle(connect("tab-b", 30));

    let promotion_effects = broker.handle(complete_previous_locks(election_id, 40));

    assert_contains_effect(
        &promotion_effects,
        send_to("tab-b", message_become_leader(2, None)),
    );
    assert_eq!(broker.snapshot().leader_tab_id.as_deref(), Some("tab-b"));
}

#[test]
fn stale_candidate_reporting_ready_after_replacement_is_demoted() {
    let mut broker = BrokerElectionCore::new(BROKER_ID.to_string());
    let _ = broker.handle(connect("tab-a", 10));
    let _ = broker.handle(connect("tab-b", 20));
    let _ = broker.handle(schema("tab-b", SCHEMA_A));

    let effects = broker.handle(leader_ready("tab-a", 1, 30));

    assert_eq!(effects, vec![send_to("tab-a", message_demote(1))]);
    assert_eq!(broker.snapshot().leader_tab_id.as_deref(), Some("tab-b"));
}

#[test]
fn missed_broker_pongs_evict_leader_and_promote_follower() {
    let mut broker = BrokerElectionCore::new(BROKER_ID.to_string());
    let _ = broker.handle(connect_with_options(
        "tab-a",
        0,
        Some(50),
        Some(50),
        Some(100),
    ));
    let _ = broker.handle(schema("tab-a", SCHEMA_A));
    let _ = broker.handle(leader_ready("tab-a", 1, 10));
    let _ = broker.handle(connect("tab-b", 50));
    let _ = broker.handle(schema("tab-b", SCHEMA_A));

    let timeout_effects = broker.handle(BrokerEvent::TimerFired {
        timer_id: BrokerTimerId::BrokerPing,
        now_ms: 101,
    });
    let election_id = find_wait_election(&timeout_effects);
    assert_contains_effect(
        &timeout_effects,
        BrokerEffect::CloseTabPort {
            tab_id: "tab-a".to_string(),
        },
    );

    let promotion_effects = broker.handle(complete_previous_locks(election_id, 110));

    assert_contains_effect(
        &promotion_effects,
        send_to("tab-b", message_become_leader(2, None)),
    );
    assert_eq!(broker.snapshot().leader_tab_id.as_deref(), Some("tab-b"));
}

#[test]
fn evicted_follower_reconnects_and_is_reattached() {
    let mut broker = BrokerElectionCore::new(BROKER_ID.to_string());
    let _ = broker.handle(connect_with_options(
        "tab-a",
        0,
        Some(50),
        Some(50),
        Some(100),
    ));
    let _ = broker.handle(schema("tab-a", SCHEMA_A));
    let _ = broker.handle(leader_ready("tab-a", 1, 10));
    let _ = broker.handle(connect("tab-b", 50));
    let _ = broker.handle(schema("tab-b", SCHEMA_A));
    let _ = broker.handle(follower_attached("tab-a", "tab-b", 1));
    let _ = broker.handle(BrokerEvent::BrokerPong {
        tab_id: "tab-a".to_string(),
        now_ms: 140,
    });

    let eviction_effects = broker.handle(BrokerEvent::TimerFired {
        timer_id: BrokerTimerId::BrokerPing,
        now_ms: 151,
    });

    assert_contains_effect(
        &eviction_effects,
        BrokerEffect::CloseTabPort {
            tab_id: "tab-b".to_string(),
        },
    );
    assert_contains_effect(
        &eviction_effects,
        send_to(
            "tab-a",
            BrokerControlMessage::DetachFollowerPort {
                broker_instance_id: BROKER_ID.to_string(),
                follower_tab_id: "tab-b".to_string(),
                leadership_id: 1,
            },
        ),
    );

    let _ = broker.handle(connect("tab-b", 160));
    let reattach_effects = broker.handle(schema("tab-b", SCHEMA_A));

    assert_contains_effect(
        &reattach_effects,
        BrokerEffect::AssignFollowerPort {
            leader_tab_id: "tab-a".to_string(),
            follower_tab_id: "tab-b".to_string(),
            leadership_id: 1,
        },
    );
}

#[test]
fn evicted_tab_reconnect_does_not_duplicate_fanout() {
    let mut broker = BrokerElectionCore::new(BROKER_ID.to_string());
    let _ = broker.handle(connect_with_options(
        "tab-a",
        0,
        Some(50),
        Some(50),
        Some(100),
    ));
    let _ = broker.handle(schema("tab-a", SCHEMA_A));
    let _ = broker.handle(leader_ready("tab-a", 1, 10));
    let _ = broker.handle(connect("tab-b", 50));
    let _ = broker.handle(schema("tab-b", SCHEMA_A));
    let _ = broker.handle(follower_attached("tab-a", "tab-b", 1));
    let _ = broker.handle(BrokerEvent::BrokerPong {
        tab_id: "tab-a".to_string(),
        now_ms: 140,
    });

    let _ = broker.handle(BrokerEvent::TimerFired {
        timer_id: BrokerTimerId::BrokerPing,
        now_ms: 151,
    });
    let _ = broker.handle(connect("tab-b", 160));
    let _ = broker.handle(BrokerEvent::BrokerPong {
        tab_id: "tab-a".to_string(),
        now_ms: 180,
    });
    let _ = broker.handle(BrokerEvent::BrokerPong {
        tab_id: "tab-b".to_string(),
        now_ms: 180,
    });

    let ping_effects = broker.handle(BrokerEvent::TimerFired {
        timer_id: BrokerTimerId::BrokerPing,
        now_ms: 190,
    });

    let tab_b_pings = ping_effects
        .iter()
        .filter(|effect| {
            matches!(
                effect,
                BrokerEffect::SendToTab {
                    tab_id,
                    message: BrokerControlMessage::BrokerPing { .. },
                } if tab_id == "tab-b"
            )
        })
        .count();
    assert_eq!(
        tab_b_pings, 1,
        "reconnected tab should receive one broker ping: {ping_effects:#?}",
    );
}

#[test]
fn replacement_election_prefers_visible_tab_after_visibility_change() {
    let mut broker = BrokerElectionCore::new(BROKER_ID.to_string());
    let _ = broker.handle(connect("tab-a", 10));
    let _ = broker.handle(schema("tab-a", SCHEMA_A));
    let _ = broker.handle(leader_ready("tab-a", 1, 20));
    let _ = broker.handle(connect("tab-b", 30));
    let _ = broker.handle(schema("tab-b", SCHEMA_A));
    let _ = broker.handle(connect("tab-c", 40));
    let _ = broker.handle(schema("tab-c", SCHEMA_A));
    let _ = broker.handle(BrokerEvent::VisibilityChanged {
        tab_id: "tab-b".to_string(),
        visibility: BrokerVisibility::Visible,
        now_ms: 50,
    });
    let _ = broker.handle(BrokerEvent::VisibilityChanged {
        tab_id: "tab-b".to_string(),
        visibility: BrokerVisibility::Hidden,
        now_ms: 60,
    });

    let shutdown_effects = broker.handle(BrokerEvent::Shutdown {
        tab_id: "tab-a".to_string(),
        now_ms: 70,
    });
    let election_id = find_wait_election(&shutdown_effects);
    let replacement_effects = broker.handle(complete_previous_locks(election_id, 80));

    assert_contains_effect(
        &replacement_effects,
        send_to("tab-c", message_become_leader(2, None)),
    );
    assert_eq!(broker.snapshot().leader_tab_id.as_deref(), Some("tab-c"));
}

#[test]
fn storage_reset_prepares_promotes_reconnects_and_finishes() {
    let mut broker = setup_ready_leader_with_two_followers();
    let request_id = "reset-a";

    let begin_effects = broker.handle(request_storage_reset("tab-b", request_id, 50));

    assert_contains_effect(
        &begin_effects,
        send_to(
            "tab-b",
            BrokerControlMessage::StorageResetStarted {
                broker_instance_id: BROKER_ID.to_string(),
                request_id: request_id.to_string(),
            },
        ),
    );
    for tab_id in ["tab-a", "tab-b", "tab-c"] {
        assert_contains_effect(
            &begin_effects,
            send_to(
                tab_id,
                BrokerControlMessage::StorageResetBegin {
                    broker_instance_id: BROKER_ID.to_string(),
                    request_id: request_id.to_string(),
                    leadership_id: 1,
                },
            ),
        );
    }
    assert_eq!(broker.snapshot().reset_phase.as_deref(), Some("preparing"));

    assert!(broker
        .handle(storage_ready("tab-a", request_id, true, 60))
        .is_empty());
    assert!(broker
        .handle(storage_ready("tab-b", request_id, true, 61))
        .is_empty());
    let ready_effects = broker.handle(storage_ready("tab-c", request_id, true, 62));
    let election_id = find_wait_election(&ready_effects);
    assert_eq!(broker.snapshot().reset_phase.as_deref(), Some("promoting"));

    let promote_effects = broker.handle(complete_previous_locks(election_id, 70));
    assert_contains_effect(
        &promote_effects,
        send_to("tab-c", message_become_leader(2, Some(request_id))),
    );

    let leader_ready_effects = broker.handle(leader_ready("tab-c", 2, 80));
    assert_eq!(
        broker.snapshot().reset_phase.as_deref(),
        Some("reconnecting")
    );
    assert_contains_effect(
        &leader_ready_effects,
        BrokerEffect::AssignFollowerPort {
            leader_tab_id: "tab-c".to_string(),
            follower_tab_id: "tab-a".to_string(),
            leadership_id: 2,
        },
    );
    assert_contains_effect(
        &leader_ready_effects,
        BrokerEffect::AssignFollowerPort {
            leader_tab_id: "tab-c".to_string(),
            follower_tab_id: "tab-b".to_string(),
            leadership_id: 2,
        },
    );

    assert!(broker
        .handle(follower_attached("tab-c", "tab-a", 2))
        .iter()
        .all(|effect| !matches!(
            effect,
            BrokerEffect::SendToTab {
                message: BrokerControlMessage::StorageResetFinished { .. },
                ..
            }
        )));
    let finished_effects = broker.handle(follower_attached("tab-c", "tab-b", 2));
    for tab_id in ["tab-a", "tab-b", "tab-c"] {
        assert_contains_effect(
            &finished_effects,
            send_to(
                tab_id,
                message_storage_reset_finished(request_id, true, None),
            ),
        );
    }
    assert_eq!(broker.snapshot().reset_phase, None);
    assert!(broker.snapshot().leader_ready);
}

#[test]
fn storage_reset_ignores_ready_messages_from_departed_participants() {
    let mut broker = setup_ready_leader_with_follower();
    let request_id = "reset-late";
    let _ = broker.handle(request_storage_reset("tab-b", request_id, 40));

    let shutdown_effects = broker.handle(BrokerEvent::Shutdown {
        tab_id: "tab-a".to_string(),
        now_ms: 41,
    });
    assert!(
        !shutdown_effects.iter().any(|effect| matches!(
            effect,
            BrokerEffect::SendToTab {
                message: BrokerControlMessage::BecomeLeader { .. },
                ..
            }
        )),
        "departing participant should not trigger promotion before remaining tabs are ready: {shutdown_effects:#?}",
    );

    let departed_ready = broker.handle(storage_ready("tab-a", request_id, true, 42));
    assert!(departed_ready.is_empty());

    let remaining_ready = broker.handle(storage_ready("tab-b", request_id, true, 43));
    let election_id = find_wait_election(&remaining_ready);
    let promotion_effects = broker.handle(complete_previous_locks(election_id, 50));

    assert_contains_effect(
        &promotion_effects,
        send_to("tab-b", message_become_leader(2, Some(request_id))),
    );
}

#[test]
fn follower_connecting_during_reset_promotion_gets_deferred_attachment_retry() {
    let mut broker = setup_ready_leader_with_follower();
    let request_id = "reset-mid-connect";
    let _ = broker.handle(request_storage_reset("tab-a", request_id, 40));
    assert!(broker
        .handle(storage_ready("tab-a", request_id, true, 41))
        .is_empty());
    let ready_effects = broker.handle(storage_ready("tab-b", request_id, true, 42));
    let election_id = find_wait_election(&ready_effects);
    let promote_effects = broker.handle(complete_previous_locks(election_id, 50));
    assert_contains_effect(
        &promote_effects,
        send_to("tab-b", message_become_leader(2, Some(request_id))),
    );
    assert_eq!(broker.snapshot().reset_phase.as_deref(), Some("promoting"));

    let _ = broker.handle(connect("tab-c", 55));
    let schema_effects = broker.handle(schema("tab-c", SCHEMA_A));

    assert_contains_effect(
        &schema_effects,
        BrokerEffect::ArmTimer {
            timer_id: BrokerTimerId::FollowerAttachment {
                follower_tab_id: "tab-c".to_string(),
                leadership_id: 2,
            },
            delay_ms: 1_000,
        },
    );
    assert!(
        schema_effects
            .iter()
            .all(|effect| !matches!(effect, BrokerEffect::AssignFollowerPort { .. })),
        "deferred attachment should not transfer a port while reset promotion is still blocked: {schema_effects:#?}",
    );

    let retry_effects = broker.handle(BrokerEvent::TimerFired {
        timer_id: BrokerTimerId::FollowerAttachment {
            follower_tab_id: "tab-c".to_string(),
            leadership_id: 2,
        },
        now_ms: 1_055,
    });
    assert_contains_effect(
        &retry_effects,
        BrokerEffect::ArmTimer {
            timer_id: BrokerTimerId::FollowerAttachment {
                follower_tab_id: "tab-c".to_string(),
                leadership_id: 2,
            },
            delay_ms: 2_000,
        },
    );
    assert!(
        retry_effects
            .iter()
            .all(|effect| !matches!(effect, BrokerEffect::AssignFollowerPort { .. })),
        "retry should stay deferred until the promoted leader is ready: {retry_effects:#?}",
    );

    let leader_ready_effects = broker.handle(leader_ready("tab-b", 2, 2_000));
    assert_contains_effect(
        &leader_ready_effects,
        BrokerEffect::AssignFollowerPort {
            leader_tab_id: "tab-b".to_string(),
            follower_tab_id: "tab-c".to_string(),
            leadership_id: 2,
        },
    );
}

#[test]
fn active_storage_reset_settles_every_requester() {
    let mut broker = setup_ready_leader_with_follower();
    let first_request_id = "reset-a";
    let second_request_id = "reset-b";

    let _ = broker.handle(request_storage_reset("tab-a", first_request_id, 40));
    let joined_effects = broker.handle(request_storage_reset("tab-b", second_request_id, 41));
    assert_eq!(
        joined_effects,
        vec![send_to(
            "tab-b",
            BrokerControlMessage::StorageResetStarted {
                broker_instance_id: BROKER_ID.to_string(),
                request_id: second_request_id.to_string(),
            },
        )],
    );

    assert!(broker
        .handle(storage_ready("tab-a", first_request_id, true, 42))
        .is_empty());
    let ready_effects = broker.handle(storage_ready("tab-b", first_request_id, true, 43));
    let election_id = find_wait_election(&ready_effects);
    let promote_effects = broker.handle(complete_previous_locks(election_id, 50));
    assert_contains_effect(
        &promote_effects,
        send_to("tab-b", message_become_leader(2, Some(first_request_id))),
    );
    let _ = broker.handle(leader_ready("tab-b", 2, 60));
    let finished_effects = broker.handle(follower_attached("tab-b", "tab-a", 2));

    for request_id in [first_request_id, second_request_id] {
        for tab_id in ["tab-a", "tab-b"] {
            assert_contains_effect(
                &finished_effects,
                send_to(
                    tab_id,
                    message_storage_reset_finished(request_id, true, None),
                ),
            );
        }
    }
}

#[test]
fn completed_storage_reset_outcomes_are_redelivered_to_reconnecting_tabs() {
    let mut broker = setup_ready_leader_with_follower();
    let request_id = "reset-redeliver";
    let _ = broker.handle(request_storage_reset("tab-a", request_id, 40));
    let _ = broker.handle(storage_ready("tab-a", request_id, true, 41));
    let ready_effects = broker.handle(storage_ready("tab-b", request_id, true, 42));
    let election_id = find_wait_election(&ready_effects);
    let _ = broker.handle(complete_previous_locks(election_id, 50));
    let _ = broker.handle(leader_ready("tab-b", 2, 60));
    let _ = broker.handle(follower_attached("tab-b", "tab-a", 2));

    let reconnect_effects = broker.handle(connect("tab-a", 70));

    assert_contains_effect(
        &reconnect_effects,
        send_to(
            "tab-a",
            message_storage_reset_finished(request_id, true, None),
        ),
    );
}

#[test]
fn same_tab_follower_reconnects_with_fresh_follower_port() {
    let mut broker = setup_ready_leader_with_follower();

    let shutdown_effects = broker.handle(BrokerEvent::Shutdown {
        tab_id: "tab-b".to_string(),
        now_ms: 110,
    });
    assert_contains_effect(
        &shutdown_effects,
        send_to(
            "tab-a",
            BrokerControlMessage::DetachFollowerPort {
                broker_instance_id: BROKER_ID.to_string(),
                follower_tab_id: "tab-b".to_string(),
                leadership_id: 1,
            },
        ),
    );

    let reconnect_effects = broker.handle(connect("tab-b", 120));
    assert_contains_effect(
        &reconnect_effects,
        send_to(
            "tab-b",
            BrokerControlMessage::LeaderReady {
                broker_instance_id: BROKER_ID.to_string(),
                leader_tab_id: "tab-a".to_string(),
                leadership_id: 1,
            },
        ),
    );
    let schema_effects = broker.handle(schema("tab-b", SCHEMA_A));
    assert_contains_effect(
        &schema_effects,
        BrokerEffect::AssignFollowerPort {
            leader_tab_id: "tab-a".to_string(),
            follower_tab_id: "tab-b".to_string(),
            leadership_id: 1,
        },
    );
}

#[test]
fn same_tab_follower_rehello_without_eviction_gets_fresh_follower_port() {
    let mut broker = setup_ready_leader_with_follower();

    let rehello_effects = broker.handle(connect("tab-b", 120));
    assert_contains_effect(
        &rehello_effects,
        BrokerEffect::CloseReplacedTabPort {
            tab_id: "tab-b".to_string(),
        },
    );
    let schema_effects = broker.handle(schema("tab-b", SCHEMA_A));

    assert_contains_effect(
        &schema_effects,
        BrokerEffect::AssignFollowerPort {
            leader_tab_id: "tab-a".to_string(),
            follower_tab_id: "tab-b".to_string(),
            leadership_id: 1,
        },
    );
}

#[test]
fn same_tab_ready_leader_reconnects_to_live_broker_and_is_repromoted_after_schema() {
    let mut broker = BrokerElectionCore::new(BROKER_ID.to_string());
    let _ = broker.handle(connect("tab-a", 10));
    let _ = broker.handle(schema("tab-a", SCHEMA_A));
    let _ = broker.handle(leader_ready("tab-a", 1, 20));

    let rehello_effects = broker.handle(connect("tab-a", 30));
    assert_contains_effect(
        &rehello_effects,
        BrokerEffect::CloseReplacedTabPort {
            tab_id: "tab-a".to_string(),
        },
    );
    assert!(
        rehello_effects.iter().all(|effect| !matches!(
            effect,
            BrokerEffect::SendToTab {
                message: BrokerControlMessage::BecomeLeader { .. },
                ..
            }
        )),
        "leader should wait for cached schema to be re-reported after reconnect: {rehello_effects:#?}",
    );

    let schema_effects = broker.handle(schema("tab-a", SCHEMA_A));

    assert_contains_effect(
        &schema_effects,
        send_to("tab-a", message_become_leader(2, None)),
    );
    assert_eq!(broker.snapshot().leader_tab_id.as_deref(), Some("tab-a"));
}

#[test]
fn same_tab_non_ready_leader_reconnects_to_live_broker_and_is_repromoted() {
    let mut broker = BrokerElectionCore::new(BROKER_ID.to_string());
    let _ = broker.handle(connect("tab-a", 10));

    let rehello_effects = broker.handle(connect("tab-a", 20));

    assert_contains_effect(
        &rehello_effects,
        BrokerEffect::CloseReplacedTabPort {
            tab_id: "tab-a".to_string(),
        },
    );
    assert_contains_effect(
        &rehello_effects,
        send_to("tab-a", message_become_leader(2, None)),
    );
    assert_eq!(broker.snapshot().leader_tab_id.as_deref(), Some("tab-a"));
}

#[test]
fn follower_port_closed_is_reattached() {
    let mut broker = setup_ready_leader_with_follower();

    let effects = broker.handle(BrokerEvent::FollowerPortClosed {
        leader_tab_id: "tab-a".to_string(),
        follower_tab_id: "tab-b".to_string(),
        leadership_id: 1,
        now_ms: 120,
    });

    assert_contains_effect(
        &effects,
        BrokerEffect::AssignFollowerPort {
            leader_tab_id: "tab-a".to_string(),
            follower_tab_id: "tab-b".to_string(),
            leadership_id: 1,
        },
    );
    assert_contains_effect(
        &effects,
        BrokerEffect::ArmTimer {
            timer_id: BrokerTimerId::FollowerAttachment {
                follower_tab_id: "tab-b".to_string(),
                leadership_id: 1,
            },
            delay_ms: 1_000,
        },
    );
}

#[test]
fn failed_storage_reset_still_allows_normal_election_afterwards() {
    let mut broker = setup_ready_leader_with_follower();
    let request_id = "reset-failed";
    let _ = broker.handle(request_storage_reset("tab-a", request_id, 40));
    assert!(broker
        .handle(storage_ready("tab-a", request_id, true, 41))
        .is_empty());
    let ready_effects = broker.handle(storage_ready("tab-b", request_id, false, 42));
    let election_id = find_wait_election(&ready_effects);

    let failed_effects = broker.handle(complete_previous_locks(election_id, 50));

    for tab_id in ["tab-a", "tab-b"] {
        assert_contains_effect(
            &failed_effects,
            send_to(
                tab_id,
                message_storage_reset_finished(request_id, false, Some("prepare exploded")),
            ),
        );
    }
    assert_contains_effect(
        &failed_effects,
        send_to("tab-b", message_become_leader(2, None)),
    );
    assert_eq!(broker.snapshot().reset_phase, None);
}

#[test]
fn promoted_reset_leader_eviction_continues_reset_with_another_tab() {
    let mut broker = BrokerElectionCore::new(BROKER_ID.to_string());
    let _ = broker.handle(connect_with_options(
        "tab-a",
        0,
        Some(50),
        Some(20),
        Some(60),
    ));
    let _ = broker.handle(schema("tab-a", SCHEMA_A));
    let _ = broker.handle(leader_ready("tab-a", 1, 10));
    let _ = broker.handle(connect("tab-b", 20));
    let _ = broker.handle(schema("tab-b", SCHEMA_A));
    let _ = broker.handle(follower_attached("tab-a", "tab-b", 1));

    let request_id = "reset-evict";
    let _ = broker.handle(request_storage_reset("tab-a", request_id, 30));
    assert!(broker
        .handle(storage_ready("tab-a", request_id, true, 31))
        .is_empty());
    let ready_effects = broker.handle(storage_ready("tab-b", request_id, true, 32));
    let election_id = find_wait_election(&ready_effects);
    let promote_effects = broker.handle(complete_previous_locks(election_id, 40));
    assert_contains_effect(
        &promote_effects,
        send_to("tab-b", message_become_leader(2, Some(request_id))),
    );

    let _ = broker.handle(BrokerEvent::BrokerPong {
        tab_id: "tab-a".to_string(),
        now_ms: 70,
    });
    let eviction_effects = broker.handle(BrokerEvent::TimerFired {
        timer_id: BrokerTimerId::BrokerPing,
        now_ms: 81,
    });

    assert_contains_effect(
        &eviction_effects,
        BrokerEffect::CloseTabPort {
            tab_id: "tab-b".to_string(),
        },
    );
    assert_contains_effect(
        &eviction_effects,
        send_to("tab-a", message_become_leader(3, Some(request_id))),
    );

    let finished_effects = broker.handle(leader_ready("tab-a", 3, 90));
    assert_contains_effect(
        &finished_effects,
        send_to(
            "tab-a",
            message_storage_reset_finished(request_id, true, None),
        ),
    );
    assert_eq!(broker.snapshot().leader_tab_id.as_deref(), Some("tab-a"));
    assert_eq!(broker.snapshot().reset_phase, None);
}

#[test]
fn effects_serialize_with_stable_js_field_names() {
    let effect = BrokerEffect::SendToTab {
        tab_id: "tab-a".to_string(),
        message: BrokerControlMessage::BecomeLeader {
            broker_instance_id: "broker-test".to_string(),
            leadership_id: 7,
            reset_request_id: None,
        },
    };

    let json = serde_json::to_value(&effect).expect("effect should serialize");

    assert_eq!(
        json,
        serde_json::json!({
            "type": "sendToTab",
            "tabId": "tab-a",
            "message": {
                "type": "become-leader",
                "brokerInstanceId": "broker-test",
                "leadershipId": 7
            }
        }),
    );
}

#[test]
fn events_serialize_with_stable_js_field_names() {
    let event = connect("tab-a", 10);

    let json = serde_json::to_value(&event).expect("event should serialize");

    assert_eq!(
        json,
        serde_json::json!({
            "type": "tabConnected",
            "tabId": "tab-a",
            "appId": "app",
            "dbName": "db",
            "fingerprint": "fingerprint-a",
            "visibility": "visible",
            "nowMs": 10
        }),
    );
}
