#[cfg(feature = "client")]
use crate::JazzClient;

use super::*;

#[cfg(feature = "client")]
fn attributed_to(principal: &str) -> WriteContext {
    WriteContext {
        attribution: Some(principal.into()),
        ..Default::default()
    }
}

#[cfg(feature = "client")]
async fn next_subscription_delta(stream: &mut crate::SubscriptionStream) -> crate::OrderedRowDelta {
    tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("subscription should emit a delta")
        .expect("subscription stream should stay open")
}

#[cfg(feature = "client")]
#[tokio::test]
async fn magic_columns_reactively_track_update_and_delete_permissions() {
    let schema = magic_introspection_schema();
    let client = JazzClient::test_client(schema).await;

    let protected = client
        .insert("protected", crate::row_input!("data" => "initial"), None)
        .expect("seed protected row")
        .0;

    let query = QueryBuilder::new("protected")
        .select(&["data", "$canRead", "$canEdit", "$canDelete"])
        .build();
    let mut subscription = client
        .for_session(Session::new("alice"))
        .subscribe(query.clone())
        .await
        .expect("subscribe with session");

    let initial_delta = next_subscription_delta(&mut subscription).await;
    assert!(
        initial_delta.added.iter().any(|row| row.id == protected),
        "initial subscription delta should include protected row"
    );
    let initial_values = client
        .for_session(Session::new("alice"))
        .query(query.clone(), None)
        .await
        .expect("query protected as alice")
        .pop()
        .expect("initial protected row")
        .1;
    assert_eq!(
        initial_values,
        vec![
            Value::Text("initial".into()),
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Boolean(false),
        ]
    );

    client
        .insert("admins", crate::row_input!("user_id" => "alice"), None)
        .expect("grant alice admin");

    let dependency_delta = next_subscription_delta(&mut subscription).await;
    assert!(
        dependency_delta
            .updated
            .iter()
            .any(|row| row.id == protected),
        "magic columns should re-evaluate existing row"
    );
    let updated_values = client
        .for_session(Session::new("alice"))
        .query(query, None)
        .await
        .expect("query protected as alice")
        .pop()
        .expect("updated protected row")
        .1;
    assert_eq!(
        updated_values,
        vec![
            Value::Text("initial".into()),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
        ]
    );

    client
        .for_session(Session::new("alice"))
        .update(
            protected,
            vec![("data".into(), Value::Text("updated".into()))],
        )
        .expect("magic $canEdit should match actual update permission");
    client
        .for_session(Session::new("alice"))
        .delete(protected)
        .expect("magic $canDelete should match actual delete permission");
}

#[cfg(feature = "client")]
#[tokio::test]
async fn magic_columns_return_null_without_session_and_do_not_change_default_output_shape() {
    let schema = magic_introspection_schema();
    let client = JazzClient::test_client(schema).await;

    client
        .insert("protected", crate::row_input!("data" => "initial"), None)
        .expect("seed protected row");
    client
        .insert("admins", crate::row_input!("user_id" => "alice"), None)
        .expect("grant alice admin");

    let projected_query = QueryBuilder::new("protected")
        .select(&["data", "$canRead", "$canEdit", "$canDelete"])
        .build();
    let projected_values = client
        .query(projected_query, None)
        .await
        .expect("query protected without session")
        .pop()
        .expect("projected protected row")
        .1;
    assert_eq!(
        projected_values,
        vec![
            Value::Text("initial".into()),
            Value::Null,
            Value::Null,
            Value::Null
        ]
    );

    let filtered_query = QueryBuilder::new("protected")
        .filter_eq("$canDelete", Value::Boolean(true))
        .build();
    let filtered_values = client
        .for_session(Session::new("alice"))
        .query(filtered_query, None)
        .await
        .expect("query protected as alice")
        .pop()
        .expect("filtered protected row")
        .1;
    assert_eq!(filtered_values, vec![Value::Text("initial".into())]);
}

#[cfg(feature = "client")]
#[tokio::test]
async fn provenance_magic_columns_capture_insert_update_and_system_authors() {
    let schema = provenance_notes_schema();
    let client = JazzClient::test_client(schema).await;

    let note = client
        .for_session(Session::new("alice"))
        .insert("notes", crate::row_input!("title" => "draft"))
        .expect("alice-authored note should insert")
        .0;

    let initial = client
        .query(
            QueryBuilder::new("notes")
                .filter_eq("title", Value::Text("draft".into()))
                .select(&[
                    "title",
                    "$createdBy",
                    "$updatedBy",
                    "$createdAt",
                    "$updatedAt",
                ])
                .build(),
            None,
        )
        .await
        .expect("query initial note");
    assert_eq!(initial.len(), 1, "draft note should be queryable");
    assert_eq!(
        initial[0].1[0],
        Value::Text("draft".into()),
        "projected title should decode"
    );
    assert_eq!(initial[0].1[1], Value::Text("alice".into()));
    assert_eq!(initial[0].1[2], Value::Text("alice".into()));
    let Value::Timestamp(initial_created_at) = initial[0].1[3] else {
        panic!("$createdAt should decode as a timestamp")
    };
    let Value::Timestamp(initial_updated_at) = initial[0].1[4] else {
        panic!("$updatedAt should decode as a timestamp")
    };
    assert_eq!(
        initial_created_at, initial_updated_at,
        "fresh inserts should initialize created/updated timestamps together"
    );

    client
        .update(
            note,
            vec![("title".into(), Value::Text("revised".into()))],
            Some(attributed_to("bob")),
        )
        .expect("attributed update should succeed without a session");

    let updated = client
        .query(
            QueryBuilder::new("notes")
                .filter_eq("title", Value::Text("revised".into()))
                .select(&[
                    "title",
                    "$createdBy",
                    "$updatedBy",
                    "$createdAt",
                    "$updatedAt",
                ])
                .build(),
            None,
        )
        .await
        .expect("query updated note");
    assert_eq!(updated.len(), 1, "updated note should remain queryable");
    assert_eq!(updated[0].1[0], Value::Text("revised".into()));
    assert_eq!(updated[0].1[1], Value::Text("alice".into()));
    assert_eq!(updated[0].1[2], Value::Text("bob".into()));
    let Value::Timestamp(updated_created_at) = updated[0].1[3] else {
        panic!("updated $createdAt should decode as a timestamp")
    };
    let Value::Timestamp(updated_updated_at) = updated[0].1[4] else {
        panic!("updated $updatedAt should decode as a timestamp")
    };
    assert_eq!(
        updated_created_at, initial_created_at,
        "created_at should be preserved across updates"
    );
    assert!(
        updated_updated_at >= initial_updated_at,
        "updated_at should move forward on update"
    );

    let updated_by_bob = client
        .query(
            QueryBuilder::new("notes")
                .filter_eq("$updatedBy", Value::Text("bob".into()))
                .select(&["title", "$updatedBy"])
                .build(),
            None,
        )
        .await
        .expect("query notes updated by bob");
    assert_eq!(updated_by_bob.len(), 1);
    assert_eq!(
        updated_by_bob[0].1,
        vec![Value::Text("revised".into()), Value::Text("bob".into())]
    );

    client
        .insert("notes", crate::row_input!("title" => "system note"), None)
        .expect("system-authored note should insert without a session");
    let system = client
        .query(
            QueryBuilder::new("notes")
                .filter_eq("title", Value::Text("system note".into()))
                .select(&["title", "$createdBy", "$updatedBy"])
                .build(),
            None,
        )
        .await
        .expect("query system-authored note");
    assert_eq!(system.len(), 1);
    assert_eq!(
        system[0].1,
        vec![
            Value::Text("system note".into()),
            Value::Text(SYSTEM_PRINCIPAL_ID.into()),
            Value::Text(SYSTEM_PRINCIPAL_ID.into()),
        ]
    );
}

#[cfg(feature = "client")]
#[tokio::test]
async fn provenance_magic_columns_allow_explicit_updated_at_override() {
    let schema = provenance_notes_schema();
    let client = JazzClient::test_client(schema).await;

    let note = client
        .for_session(Session::new("alice"))
        .insert("notes", crate::row_input!("title" => "draft"))
        .expect("alice-authored note should insert")
        .0;

    let initial = client
        .query(
            QueryBuilder::new("notes")
                .filter_eq("title", Value::Text("draft".into()))
                .select(&["$createdAt", "$updatedAt"])
                .build(),
            None,
        )
        .await
        .expect("query initial note timestamps");
    assert_eq!(initial.len(), 1, "draft note should be queryable");
    let Value::Timestamp(initial_created_at) = initial[0].1[0] else {
        panic!("$createdAt should decode as a timestamp")
    };

    let custom_updated_at = initial_created_at + 10_000;
    let bob_backfill = WriteContext {
        updated_at: Some(custom_updated_at),
        ..attributed_to("bob")
    };

    client
        .update(
            note,
            vec![("title".into(), Value::Text("backfilled".into()))],
            Some(bob_backfill),
        )
        .expect("explicit updated_at override should succeed");

    let updated = client
        .query(
            QueryBuilder::new("notes")
                .filter_eq("title", Value::Text("backfilled".into()))
                .select(&[
                    "title",
                    "$createdBy",
                    "$updatedBy",
                    "$createdAt",
                    "$updatedAt",
                ])
                .build(),
            None,
        )
        .await
        .expect("query backfilled note");
    assert_eq!(updated.len(), 1, "backfilled note should remain queryable");
    assert_eq!(updated[0].1[0], Value::Text("backfilled".into()));
    assert_eq!(updated[0].1[1], Value::Text("alice".into()));
    assert_eq!(updated[0].1[2], Value::Text("bob".into()));
    let Value::Timestamp(updated_created_at) = updated[0].1[3] else {
        panic!("updated $createdAt should decode as a timestamp")
    };
    let Value::Timestamp(updated_updated_at) = updated[0].1[4] else {
        panic!("updated $updatedAt should decode as a timestamp")
    };
    assert_eq!(updated_created_at, initial_created_at);
    assert_eq!(updated_updated_at, custom_updated_at);
}

#[cfg(feature = "client")]
#[tokio::test]
async fn created_by_permissions_allow_creators_and_hide_system_rows() {
    let schema = authorship_permissions_schema();
    let client = JazzClient::test_client(schema).await;

    let alice_session = Session::new("alice");
    let bob_session = Session::new("bob");

    let alice_owned = client
        .for_session(alice_session.clone())
        .insert("notes", crate::row_input!("title" => "alice-owned"))
        .expect("creator-based insert policy should allow alice")
        .0;
    let alice_attributed = client
        .insert(
            "notes",
            crate::row_input!("title" => "alice-attributed"),
            Some(attributed_to("alice")),
        )
        .expect("backend-attributed note should stamp alice as creator")
        .0;
    client
        .insert("notes", crate::row_input!("title" => "system-owned"), None)
        .expect("system note should insert");

    let alice_visible = client
        .for_session(alice_session.clone())
        .query(
            QueryBuilder::new("notes")
                .select(&["title", "$createdBy"])
                .order_by("title")
                .build(),
            None,
        )
        .await
        .expect("query notes as alice");
    assert_eq!(
        alice_visible
            .iter()
            .map(|(_, values)| values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Text("alice-attributed".into()),
                Value::Text("alice".into()),
            ],
            vec![
                Value::Text("alice-owned".into()),
                Value::Text("alice".into())
            ],
        ],
        "alice should only see notes authored as alice"
    );

    let bob_visible = client
        .for_session(bob_session.clone())
        .query(QueryBuilder::new("notes").select(&["title"]).build(), None)
        .await
        .expect("query notes as bob");
    assert!(
        bob_visible.is_empty(),
        "bob should not see alice/system notes"
    );

    let bob_update_err = client
        .for_session(bob_session.clone())
        .update(
            alice_owned,
            vec![("title".into(), Value::Text("bob edit".into()))],
        )
        .expect_err("non-creator update should be denied");
    assert_client_policy_denied(bob_update_err, "notes", Operation::Update);

    let bob_delete_err = client
        .for_session(bob_session)
        .delete(alice_owned)
        .expect_err("non-creator delete should be denied");
    assert_client_policy_denied(bob_delete_err, "notes", Operation::Delete);

    client
        .for_session(alice_session.clone())
        .update(
            alice_attributed,
            vec![(
                "title".into(),
                Value::Text("alice-attributed-updated".into()),
            )],
        )
        .expect("creator should be able to update attributed rows");
    client
        .for_session(alice_session.clone())
        .delete(alice_owned)
        .expect("creator should be able to delete her own row");

    let alice_after_mutations = client
        .for_session(alice_session)
        .query(
            QueryBuilder::new("notes")
                .select(&["title"])
                .order_by("title")
                .build(),
            None,
        )
        .await
        .expect("query notes as alice after mutations");
    assert_eq!(
        alice_after_mutations
            .iter()
            .map(|(_, values)| values[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Text("alice-attributed-updated".into())],
        "alice should retain access to the surviving creator-owned row"
    );
}
