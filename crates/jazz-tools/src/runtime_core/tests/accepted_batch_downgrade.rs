use super::*;

use crate::batch_fate::BatchFate;
use crate::metadata::{MetadataKey, RowProvenance};
use crate::row_format::decode_row;
use crate::row_histories::{RowState, StoredRowBatch};
use crate::sync_manager::RowMetadata;

fn profiles_schema() -> Schema {
    let owner = PolicyExpr::eq_session("ownerUserId", vec!["user_id".into()]);
    SchemaBuilder::new()
        .table(
            TableSchema::builder("profiles")
                .column("ownerUserId", ColumnType::Text)
                .column("firstName", ColumnType::Text)
                .column("lastName", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(owner.clone())
                        .with_insert(PolicyExpr::False)
                        .with_update(Some(owner.clone()), owner.clone())
                        .with_delete(PolicyExpr::False),
                ),
        )
        .build()
}

fn profiles_descriptor(schema: &Schema) -> &crate::query_manager::types::RowDescriptor {
    &schema[&TableName::new("profiles")].columns
}

fn profiles_row_metadata() -> HashMap<String, String> {
    HashMap::from([(MetadataKey::Table.to_string(), "profiles".to_string())])
}

fn owner_write_context() -> WriteContext {
    WriteContext {
        session: Some(Session::new("owner-1")),
        attribution: None,
        updated_at: None,
        batch_mode: None,
        batch_id: None,
        target_branch_name: None,
    }
}

fn profiles_values(first: &str, last: &str) -> HashMap<String, Value> {
    HashMap::from([
        (
            "ownerUserId".to_string(),
            Value::Text("owner-1".to_string()),
        ),
        ("firstName".to_string(), Value::Text(first.to_string())),
        ("lastName".to_string(), Value::Text(last.to_string())),
    ])
}

/// One-shot bidirectional pump (the shared `sync_server_with_clients` is hardcoded to `MemoryStorage`).
fn pump<S: Storage, Sch: Scheduler>(
    server: &mut RuntimeCore<S, Sch>,
    server_id: ServerId,
    client: &mut RuntimeCore<S, Sch>,
    client_id: ClientId,
) {
    for _ in 0..12 {
        let mut any = false;
        client.batched_tick();
        for entry in client.sync_sender().take() {
            if entry.destination == Destination::Server(server_id) {
                any = true;
                server.park_sync_message(InboxEntry {
                    source: Source::Client(client_id),
                    payload: entry.payload,
                });
            }
        }
        server.batched_tick();
        server.immediate_tick();
        server.batched_tick();
        for entry in server.sync_sender().take() {
            if entry.destination == Destination::Client(client_id) {
                any = true;
                client.park_sync_message(InboxEntry {
                    source: Source::Server(server_id),
                    payload: entry.payload,
                });
            }
        }
        client.batched_tick();
        client.immediate_tick();
        if !any {
            break;
        }
    }
}

/// Bidirectional pump for `C_backend ↔ S ↔ C_owner`, routing each server output
/// to its destination client (never `take()`-ing the whole outbox into one client).
#[allow(clippy::too_many_arguments)]
fn pump3<S: Storage, Sch: Scheduler>(
    server: &mut RuntimeCore<S, Sch>,
    server_id_for_backend: ServerId,
    server_id_for_owner: ServerId,
    c_backend: &mut RuntimeCore<S, Sch>,
    backend_client_id: ClientId,
    c_owner: &mut RuntimeCore<S, Sch>,
    owner_client_id: ClientId,
) {
    for _ in 0..16 {
        let mut any = false;

        c_backend.batched_tick();
        for entry in c_backend.sync_sender().take() {
            if entry.destination == Destination::Server(server_id_for_backend) {
                any = true;
                server.park_sync_message(InboxEntry {
                    source: Source::Client(backend_client_id),
                    payload: entry.payload,
                });
            }
        }

        c_owner.batched_tick();
        for entry in c_owner.sync_sender().take() {
            if entry.destination == Destination::Server(server_id_for_owner) {
                any = true;
                server.park_sync_message(InboxEntry {
                    source: Source::Client(owner_client_id),
                    payload: entry.payload,
                });
            }
        }

        server.batched_tick();
        server.immediate_tick();
        server.batched_tick();
        for entry in server.sync_sender().take() {
            match entry.destination {
                Destination::Client(cid) if cid == backend_client_id => {
                    any = true;
                    c_backend.park_sync_message(InboxEntry {
                        source: Source::Server(server_id_for_backend),
                        payload: entry.payload,
                    });
                }
                Destination::Client(cid) if cid == owner_client_id => {
                    any = true;
                    c_owner.park_sync_message(InboxEntry {
                        source: Source::Server(server_id_for_owner),
                        payload: entry.payload,
                    });
                }
                _ => {}
            }
        }

        c_backend.batched_tick();
        c_backend.immediate_tick();
        c_owner.batched_tick();
        c_owner.immediate_tick();

        if !any {
            break;
        }
    }
}

fn new_server<S: Storage>(
    storage: S,
    schema: Schema,
    app_name: &str,
) -> RuntimeCore<S, NoopScheduler> {
    let schema_manager = SchemaManager::new(
        SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer),
        schema.clone(),
        AppId::from_name(app_name),
        "dev",
        "main",
    )
    .unwrap();
    let mut server = new_test_core(schema_manager, storage, NoopScheduler);
    server.immediate_tick();
    server
        .schema_manager_mut()
        .query_manager_mut()
        .set_authorization_schema(schema);
    server
        .schema_manager_mut()
        .query_manager_mut()
        .require_authorization_schema();
    server
}

fn new_client<S: Storage>(
    storage: S,
    schema: Schema,
    app_name: &str,
) -> RuntimeCore<S, NoopScheduler> {
    let schema_manager = SchemaManager::new(
        SyncManager::new(),
        schema,
        AppId::from_name(app_name),
        "dev",
        "main",
    )
    .unwrap();
    let mut client = new_test_core(schema_manager, storage, NoopScheduler);
    client.immediate_tick();
    client
}

#[cfg(feature = "sqlite")]
#[test]
fn owner_update_of_backend_created_row_is_not_destroyed() {
    use crate::storage::SqliteStorage;

    let schema = profiles_schema();
    let server_dir = tempfile::TempDir::new().unwrap();
    let client_dir = tempfile::TempDir::new().unwrap();

    let mut s = new_server(
        SqliteStorage::open(server_dir.path().join("jazz.sqlite")).unwrap(),
        schema.clone(),
        "accepted-batch-base",
    );
    let mut c = new_client(
        SqliteStorage::open(client_dir.path().join("jazz.sqlite")).unwrap(),
        schema.clone(),
        "accepted-batch-base",
    );

    let ((b0_row_id, _values), b0_batch_id) = s
        .insert("profiles", profiles_values("Ada", ""), None)
        .unwrap();
    s.immediate_tick();
    let branch = s.schema_manager().branch_name();

    assert!(
        !matches!(
            s.storage()
                .load_authoritative_batch_fate(b0_batch_id)
                .unwrap(),
            Some(BatchFate::Rejected { .. })
        ),
        "backend insert should be accepted by the server, not rejected"
    );

    let client_id = ClientId::new();
    let server_id = ServerId::new();
    s.add_client(client_id, Some(Session::new("owner-1")));
    c.add_server(server_id);

    let _handle = c
        .subscribe(
            QueryBuilder::new("profiles").build(),
            |_delta| {},
            Some(Session::new("owner-1")),
        )
        .unwrap();

    pump(&mut s, server_id, &mut c, client_id);

    assert!(
        c.storage()
            .load_visible_region_row("profiles", branch.as_str(), b0_row_id)
            .unwrap()
            .is_some(),
        "client should have received the backend-created row before updating it"
    );

    c.update(
        b0_row_id,
        vec![("lastName".into(), Value::Text("Lovelace".into()))],
        Some(&owner_write_context()),
    )
    .unwrap();
    c.immediate_tick();

    pump(&mut s, server_id, &mut c, client_id);
    s.immediate_tick();

    assert!(
        !matches!(
            s.storage()
                .load_authoritative_batch_fate(b0_batch_id)
                .unwrap(),
            Some(BatchFate::Rejected { .. })
        ),
        "owner update must not turn the accepted backend insert into a rejection"
    );

    let server_row = s
        .storage()
        .load_visible_region_row("profiles", branch.as_str(), b0_row_id)
        .unwrap()
        .expect("backend-created row must survive the owner update on the server");

    let descriptor = profiles_descriptor(&schema);
    let server_values = decode_row(descriptor, &server_row.data).unwrap();
    assert_eq!(
        server_values[2],
        Value::Text("Lovelace".into()),
        "the owner update should have applied to the surviving server row"
    );

    assert!(
        c.storage()
            .load_visible_region_row("profiles", branch.as_str(), b0_row_id)
            .unwrap()
            .is_some(),
        "the row must also survive on the client"
    );
}

#[test]
fn server_refuses_divergent_resend_of_accepted_batch_without_destroying_it() {
    let schema = profiles_schema();
    let mut s = new_server(
        MemoryStorage::new(),
        schema.clone(),
        "accepted-batch-divergent",
    );

    let ((b0_row_id, _values), b0_batch_id) = s
        .insert("profiles", profiles_values("Ada", ""), None)
        .unwrap();
    s.immediate_tick();
    let branch = s.schema_manager().branch_name();

    assert!(
        !matches!(
            s.storage()
                .load_authoritative_batch_fate(b0_batch_id)
                .unwrap(),
            Some(BatchFate::Rejected { .. })
        ),
        "backend insert should be accepted, not rejected"
    );
    let stored = s
        .storage()
        .load_visible_region_row("profiles", branch.as_str(), b0_row_id)
        .unwrap()
        .expect("backend insert should materialise a visible row");

    let client_id = ClientId::new();
    s.add_client(client_id, Some(Session::new("owner-1")));

    let descriptor = profiles_descriptor(&schema);
    let divergent_data = encode_row(
        descriptor,
        &[
            Value::Text("owner-1".into()),
            Value::Text("Mallory".into()),
            Value::Text("".into()),
        ],
    )
    .unwrap();
    let resent = StoredRowBatch::new_with_batch_id(
        b0_batch_id,
        b0_row_id,
        stored.branch.as_str(),
        Vec::new(),
        divergent_data,
        RowProvenance::for_insert("owner-1", stored.updated_at + 1),
        profiles_row_metadata(),
        RowState::VisibleDirect,
        None,
    );

    s.push_sync_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: b0_row_id,
                metadata: profiles_row_metadata(),
            }),
            row: resent,
        },
    });
    s.immediate_tick();

    assert!(
        !matches!(
            s.storage()
                .load_authoritative_batch_fate(b0_batch_id)
                .unwrap(),
            Some(BatchFate::Rejected { .. })
        ),
        "a divergent resend must not downgrade the accepted batch to rejected"
    );

    let survivor = s
        .storage()
        .load_visible_region_row("profiles", branch.as_str(), b0_row_id)
        .unwrap()
        .expect("the accepted row must survive a divergent resend");
    let values = decode_row(descriptor, &survivor.data).unwrap();
    assert_eq!(
        values[1],
        Value::Text("Ada".into()),
        "the divergent resend must neither destroy nor overwrite the accepted row"
    );
}

#[test]
fn server_treats_identical_resend_of_accepted_batch_as_replay() {
    let schema = profiles_schema();
    let mut s = new_server(
        MemoryStorage::new(),
        schema.clone(),
        "accepted-batch-identical",
    );

    let ((b0_row_id, _values), b0_batch_id) = s
        .insert("profiles", profiles_values("Ada", ""), None)
        .unwrap();
    s.immediate_tick();
    let branch = s.schema_manager().branch_name();

    let stored = s
        .storage()
        .load_visible_region_row("profiles", branch.as_str(), b0_row_id)
        .unwrap()
        .expect("backend insert should materialise a visible row");

    let client_id = ClientId::new();
    s.add_client(client_id, Some(Session::new("owner-1")));

    s.push_sync_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: b0_row_id,
                metadata: profiles_row_metadata(),
            }),
            row: stored.clone(),
        },
    });
    s.immediate_tick();

    assert!(
        !matches!(
            s.storage()
                .load_authoritative_batch_fate(b0_batch_id)
                .unwrap(),
            Some(BatchFate::Rejected { .. })
        ),
        "an identical resend must remain a no-op replay, not a rejection"
    );

    let survivor = s
        .storage()
        .load_visible_region_row("profiles", branch.as_str(), b0_row_id)
        .unwrap()
        .expect("the accepted row must survive an identical resend");
    let descriptor = profiles_descriptor(&schema);
    let values = decode_row(descriptor, &survivor.data).unwrap();
    assert_eq!(
        values[1],
        Value::Text("Ada".into()),
        "an identical resend must leave the accepted row unchanged"
    );
}

#[cfg(feature = "sqlite")]
#[test]
fn owner_update_of_a_delivered_row_keeps_it_on_the_client() {
    use crate::storage::SqliteStorage;

    let schema = profiles_schema();
    let server_dir = tempfile::TempDir::new().unwrap();
    let client_dir = tempfile::TempDir::new().unwrap();

    let mut s = new_server(
        SqliteStorage::open(server_dir.path().join("jazz.sqlite")).unwrap(),
        schema.clone(),
        "accepted-batch-client-survival",
    );
    let mut c = new_client(
        SqliteStorage::open(client_dir.path().join("jazz.sqlite")).unwrap(),
        schema.clone(),
        "accepted-batch-client-survival",
    );

    let ((b0_row_id, _values), _) = s
        .insert("profiles", profiles_values("Ada", ""), None)
        .unwrap();
    s.immediate_tick();
    let branch = s.schema_manager().branch_name();

    let client_id = ClientId::new();
    let server_id = ServerId::new();
    s.add_client(client_id, Some(Session::new("owner-1")));
    c.add_server(server_id);

    let _handle = c
        .subscribe(
            QueryBuilder::new("profiles").build(),
            |_delta| {},
            Some(Session::new("owner-1")),
        )
        .unwrap();

    pump(&mut s, server_id, &mut c, client_id);

    assert!(
        c.storage()
            .load_visible_region_row("profiles", branch.as_str(), b0_row_id)
            .unwrap()
            .is_some(),
        "client should have received the backend-created row"
    );

    c.update(
        b0_row_id,
        vec![("lastName".into(), Value::Text("Lovelace".into()))],
        Some(&owner_write_context()),
    )
    .unwrap();
    c.immediate_tick();

    pump(&mut s, server_id, &mut c, client_id);
    s.immediate_tick();
    pump(&mut s, server_id, &mut c, client_id);

    let surviving = c
        .storage()
        .load_visible_region_row("profiles", branch.as_str(), b0_row_id)
        .unwrap()
        .expect("the backend-created row must survive the owner update on the client");
    let descriptor = profiles_descriptor(&schema);
    let values = decode_row(descriptor, &surviving.data).unwrap();
    assert_eq!(
        values[2],
        Value::Text("Lovelace".into()),
        "the owner update should apply on the client rather than destroying the row"
    );
}

#[cfg(feature = "sqlite")]
#[test]
fn an_updated_backend_row_stays_writable_by_owner_and_backend() {
    use crate::storage::SqliteStorage;

    let schema = profiles_schema();
    let server_dir = tempfile::TempDir::new().unwrap();
    let owner_dir = tempfile::TempDir::new().unwrap();
    let backend_dir = tempfile::TempDir::new().unwrap();

    let mut s = new_server(
        SqliteStorage::open(server_dir.path().join("jazz.sqlite")).unwrap(),
        schema.clone(),
        "accepted-batch-writable",
    );
    let mut c_owner = new_client(
        SqliteStorage::open(owner_dir.path().join("jazz.sqlite")).unwrap(),
        schema.clone(),
        "accepted-batch-writable",
    );
    let mut c_backend = new_client(
        SqliteStorage::open(backend_dir.path().join("jazz.sqlite")).unwrap(),
        schema.clone(),
        "accepted-batch-writable",
    );

    let branch = s.schema_manager().branch_name();
    let owner_client_id = ClientId::new();
    let server_id_for_owner = ServerId::new();
    let backend_client_id = ClientId::new();
    let server_id_for_backend = ServerId::new();

    s.add_client(owner_client_id, Some(Session::new("owner-1")));
    c_owner.add_server(server_id_for_owner);

    s.add_client(backend_client_id, None);
    s.ensure_client_as_backend(backend_client_id);
    s.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_session(backend_client_id, Session::new("owner-1"));
    c_backend.add_server(server_id_for_backend);

    let _owner_handle = c_owner
        .subscribe(
            QueryBuilder::new("profiles").build(),
            |_delta| {},
            Some(Session::new("owner-1")),
        )
        .unwrap();
    let _backend_handle = c_backend
        .subscribe(
            QueryBuilder::new("profiles").build(),
            |_delta| {},
            Some(Session::new("owner-1")),
        )
        .unwrap();
    pump3(
        &mut s,
        server_id_for_backend,
        server_id_for_owner,
        &mut c_backend,
        backend_client_id,
        &mut c_owner,
        owner_client_id,
    );

    let ((row_id, _values), row_batch_id) = c_backend
        .insert("profiles", profiles_values("Ada", ""), None)
        .unwrap();
    c_backend.immediate_tick();
    pump3(
        &mut s,
        server_id_for_backend,
        server_id_for_owner,
        &mut c_backend,
        backend_client_id,
        &mut c_owner,
        owner_client_id,
    );

    assert!(
        c_owner
            .storage()
            .load_visible_region_row("profiles", branch.as_str(), row_id)
            .unwrap()
            .is_some(),
        "the owner must receive the backend-created row before updating it"
    );

    c_owner
        .update(
            row_id,
            vec![("lastName".into(), Value::Text("Lovelace".into()))],
            Some(&owner_write_context()),
        )
        .expect("owner must be able to update the backend-created row");
    c_owner.immediate_tick();
    pump3(
        &mut s,
        server_id_for_backend,
        server_id_for_owner,
        &mut c_backend,
        backend_client_id,
        &mut c_owner,
        owner_client_id,
    );

    c_backend
        .update(
            row_id,
            vec![("firstName".into(), Value::Text("Grace".into()))],
            None,
        )
        .expect("backend must still be able to update the row — it is not a tombstone");
    c_backend.immediate_tick();
    pump3(
        &mut s,
        server_id_for_backend,
        server_id_for_owner,
        &mut c_backend,
        backend_client_id,
        &mut c_owner,
        owner_client_id,
    );

    let descriptor = profiles_descriptor(&schema);

    let owner_row = c_owner
        .storage()
        .load_visible_region_row("profiles", branch.as_str(), row_id)
        .unwrap()
        .expect("the row must remain present on the owner through repeated edits");
    let owner_values = decode_row(descriptor, &owner_row.data).unwrap();
    assert_eq!(
        owner_values[1],
        Value::Text("Grace".into()),
        "the backend update should propagate to the owner"
    );
    assert_eq!(
        owner_values[2],
        Value::Text("Lovelace".into()),
        "the owner's edit should persist through the backend update"
    );

    let server_row = s
        .storage()
        .load_visible_region_row("profiles", branch.as_str(), row_id)
        .unwrap()
        .expect("the row must remain present and writable on the server");
    let server_values = decode_row(descriptor, &server_row.data).unwrap();
    assert_eq!(
        server_values[1],
        Value::Text("Grace".into()),
        "the backend update should apply on the server"
    );
    assert_eq!(
        server_values[2],
        Value::Text("Lovelace".into()),
        "the owner's edit should persist on the server"
    );

    assert!(
        !matches!(
            s.storage()
                .load_authoritative_batch_fate(row_batch_id)
                .unwrap(),
            Some(BatchFate::Rejected { .. })
        ),
        "the row's batch must not be downgraded to rejected on the server"
    );
}
