use jazz_browser_broker::protocol::{
    Candidate, ControlMessage, TabMessage, Visibility, is_stale_leadership_id,
    normalize_force_takeover_timeout, normalize_positive_timeout, select_leader_candidate,
};

#[test]
fn select_leader_candidate_prefers_newest_visible_tab_and_breaks_ties_by_tab_id() {
    let candidates = vec![
        Candidate {
            tab_id: "tab-a".to_string(),
            visibility: Visibility::Hidden,
            last_visible_at: 30,
        },
        Candidate {
            tab_id: "tab-b".to_string(),
            visibility: Visibility::Visible,
            last_visible_at: 10,
        },
        Candidate {
            tab_id: "tab-c".to_string(),
            visibility: Visibility::Visible,
            last_visible_at: 10,
        },
    ];

    let selected = select_leader_candidate(&candidates).expect("expected a leader candidate");

    assert_eq!(selected.tab_id, "tab-c");
}

#[test]
fn timeout_normalization_matches_the_typescript_helpers() {
    assert_eq!(normalize_positive_timeout(None, 3_000), 3_000);
    assert_eq!(normalize_positive_timeout(Some(f64::NAN), 3_000), 3_000);
    assert_eq!(normalize_positive_timeout(Some(0.0), 3_000), 3_000);
    assert_eq!(normalize_positive_timeout(Some(2.9), 3_000), 2);

    assert_eq!(normalize_force_takeover_timeout(None), 1_000);
    assert_eq!(normalize_force_takeover_timeout(Some(f64::INFINITY)), 1_000);
    assert_eq!(normalize_force_takeover_timeout(Some(-1.0)), 1_000);
    assert_eq!(normalize_force_takeover_timeout(Some(0.0)), 0);
    assert_eq!(normalize_force_takeover_timeout(Some(2.9)), 2);
}

#[test]
fn leadership_ids_are_stale_only_when_strictly_less_than_current() {
    assert!(is_stale_leadership_id(1, 2));
    assert!(!is_stale_leadership_id(2, 2));
    assert!(!is_stale_leadership_id(3, 2));
}

#[test]
fn tab_message_serde_uses_kebab_tags_camel_fields_and_omits_unknown_types() {
    let hello: TabMessage = serde_json::from_value(serde_json::json!({
        "type": "hello",
        "tabId": "tab-a",
        "appId": "app",
        "dbName": "db",
        "fingerprint": "fp",
        "visibility": "visible",
        "extraIgnored": true
    }))
    .expect("hello should deserialize");

    assert_eq!(
        hello,
        TabMessage::Hello {
            tab_id: "tab-a".to_string(),
            app_id: "app".to_string(),
            db_name: "db".to_string(),
            fingerprint: "fp".to_string(),
            visibility: Visibility::Visible,
            force_takeover_timeout_ms: None,
            broker_ping_interval_ms: None,
            broker_pong_timeout_ms: None,
        }
    );

    let unknown: TabMessage =
        serde_json::from_value(serde_json::json!({ "type": "future-message" }))
            .expect("unknown messages should deserialize to the drop variant");

    assert_eq!(unknown, TabMessage::Unknown);
}

#[test]
fn control_message_serde_omits_absent_optional_fields() {
    let value = serde_json::to_value(ControlMessage::StorageResetFinished {
        broker_instance_id: "broker-a".to_string(),
        request_id: "reset-a".to_string(),
        success: true,
        error_message: None,
    })
    .expect("control message should serialize");

    assert_eq!(
        value,
        serde_json::json!({
            "type": "storage-reset-finished",
            "brokerInstanceId": "broker-a",
            "requestId": "reset-a",
            "success": true
        })
    );
}

#[test]
fn select_leader_candidate_falls_back_to_hidden_tabs_when_none_are_visible() {
    let candidates = vec![
        Candidate {
            tab_id: "tab-a".to_string(),
            visibility: Visibility::Hidden,
            last_visible_at: 30,
        },
        Candidate {
            tab_id: "tab-b".to_string(),
            visibility: Visibility::Hidden,
            last_visible_at: 10,
        },
    ];

    let selected = select_leader_candidate(&candidates).expect("expected a leader candidate");

    assert_eq!(selected.tab_id, "tab-a");
}
