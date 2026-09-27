//! Exercise the public peer path with independently resident databases.
//!
//! The Db facade lets this fixture drive each peer separately. A background
//! JazzServer cannot expose the serving peer's thread-local reconstruction
//! counter. That counter is the sole internal observation: returned rows alone
//! cannot establish that a discarded witness payload was never reconstructed.
//! All schema, write, query, admission and delivery operations use public APIs.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::task::{Context, Poll, Waker};

mod common;

use jazz::block_on;
use jazz::db::{
    ClientRelayScope, Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, SerializedReadResult,
};
use jazz::groove::large_values::full_materializations_for_test;
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, ObjectId, SchemaBuilder, TableSchemaBuilder, Value as PublicValue};
use jazz_testkit::duplex_transport::duplex;

struct AssertAdmittedTransport(Box<dyn jazz::db::Transport>);

impl jazz::db::Transport for AssertAdmittedTransport {
    fn send(
        &mut self,
        message: jazz::protocol::SyncMessage,
    ) -> Result<(), jazz::wire::TransportError> {
        if let jazz::protocol::SyncMessage::SubscribeRejected { reason, .. } = &message {
            panic!("fixture subscription was rejected: {reason:?}");
        }
        self.0.send(message)
    }
    fn try_recv(&mut self) -> Option<jazz::protocol::SyncMessage> {
        self.0.try_recv()
    }
}

fn cells(input: HashMap<String, PublicValue>) -> BTreeMap<String, Value> {
    input
        .into_iter()
        .map(|(key, value)| {
            let value = match value {
                PublicValue::Text(value) => Value::String(value),
                PublicValue::Uuid(value) => Value::Uuid(*value.uuid()),
                PublicValue::Bytea(value) => Value::Bytes(value),
                other => panic!("unexpected fixture cell: {other:?}"),
            };
            (key, value)
        })
        .collect()
}

fn insert(db: &Db<MemoryStorage>, table: &str, id: RowUuid, input: HashMap<String, PublicValue>) {
    let write = block_on(db.insert(
        table,
        cells(input),
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .expect("insert fixture row");
    db.finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
        .expect("settle the authored version");
}

/// Each fresh read still obtains independent coverage from its serving peer.
fn read(
    foreground: &Db<MemoryStorage>,
    owner: &Db<MemoryStorage>,
    query: &Query,
    opts: ReadOpts,
) -> (Vec<jazz::node::CurrentRow>, u64, u64) {
    let query = postcard::to_allocvec(query).unwrap();
    let mut context = Context::from_waker(Waker::noop());
    let started = std::time::Instant::now();
    let before = full_materializations_for_test();
    let mut owner_rebuilds = 0;
    let result = {
        let mut read = Box::pin(foreground.all_serialized_query(
            &query,
            opts,
            None,
            None,
            None,
            true,
            || started.elapsed().as_secs() >= 10,
            |handle| foreground.detach_query(handle),
        ));
        let mut result = None;
        for _ in 0..128 {
            if let Poll::Ready(rows) = read.as_mut().poll(&mut context) {
                result = Some(rows.expect("fresh covered read"));
                break;
            }
            block_on(foreground.tick()).expect("foreground turn");
            let owner_before = full_materializations_for_test();
            block_on(owner.tick()).expect("serving peer turn");
            owner_rebuilds += full_materializations_for_test() - owner_before;
        }
        result.unwrap_or_else(|| {
            panic!(
                "resident read did not complete: foreground={} owner={}",
                foreground.query_delivery_diagnostics_for_test(),
                owner.query_delivery_diagnostics_for_test(),
            )
        })
    };
    let rebuilds = full_materializations_for_test() - before;
    for _ in 0..4 {
        block_on(foreground.tick()).unwrap();
        block_on(owner.tick()).unwrap();
    }
    let SerializedReadResult::Rows(rows) = result else {
        panic!("plain query must return rows");
    };
    (rows, owner_rebuilds, rebuilds)
}

fn resident_reads(core: bool) {
    let alice = AuthorSubject::for_test_bytes([0xb1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    let policy = common::read_and_allow_all_writes(common::session_eq(
        "owner",
        &["user", "identity", "subject"],
    ));
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("folders")
                    .column("label", ColumnType::Text)
                    .policies(common::allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("files")
                    .column("label", ColumnType::Text)
                    .column("owner", ColumnType::Text)
                    .column("contents", ColumnType::Bytea)
                    .fk_column("folder", "folders")
                    .policies(policy),
            )
            .build(),
    )
    .unwrap();
    let families = schema.column_families();
    let names = families.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = MemoryStorage::new(&names).unwrap();
    let seed = block_on(Db::open_history_complete(DbConfig::new(
        schema.clone(),
        storage.clone(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xb3; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let folder = RowUuid::from_bytes([0xb4; 16]);
    insert(
        &seed,
        "folders",
        folder,
        jazz::row_input!("label" => "folder"),
    );
    let files = [(0xb5, 128 * 1024), (0xb6, 2 * 1024 * 1024)].map(|(id, len)| {
        let id = RowUuid::from_bytes([id; 16]);
        let bytes = (0..len)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        insert(
            &seed,
            "files",
            id,
            jazz::row_input!(
                "label" => "visible.bin", "owner" => alice.principal_parts().1,
                "folder" => ObjectId::from_uuid(folder.0), "contents" => bytes.clone()
            ),
        );
        (id, bytes)
    });
    let hidden = RowUuid::from_bytes([0xb7; 16]);
    if core {
        insert(
            &seed,
            "files",
            hidden,
            jazz::row_input!(
                "label" => "hidden.bin", "owner" => bob.principal_parts().1,
                "folder" => ObjectId::from_uuid(folder.0), "contents" => vec![7_u8; 128 * 1024]
            ),
        );
    }
    block_on(seed.close()).unwrap();
    drop(seed);
    let snapshot = storage.export_snapshot().unwrap();
    let config = |tag, author| {
        let storage = MemoryStorage::default();
        storage.import_snapshot(&snapshot).unwrap();
        DbConfig::new(
            schema.clone(),
            storage,
            DbIdentity {
                node: NodeUuid::from_bytes([tag; 16]),
                author,
            },
        )
    };
    let owner = if core {
        block_on(Db::open_history_complete(config(
            0xb8,
            AuthorSubject::SYSTEM,
        )))
        .unwrap()
    } else {
        // SAFETY: this isolated fixture owns the partition; it contains only
        // Alice's files and the public folder admitted to that same scope.
        let scope = unsafe {
            ClientRelayScope::from_admitted_storage_owner("native-witness-test".into(), alice)
        };
        block_on(unsafe { Db::open_scope_isolated_client_relay(config(0xb8, alice), scope) })
            .unwrap()
    };
    let foreground = block_on(Db::open(config(0xb9, alice))).unwrap();
    foreground.set_non_durable_client();
    let claims =
        jazz::tools::policy_claims::canonical_policy_binding_claims(&alice, BTreeMap::new());
    foreground.set_identity_claims(alice, claims.clone());
    let (foreground_transport, owner_transport) = duplex();
    let _upstream = block_on(foreground.connect_upstream(foreground_transport));
    let _subscriber = owner.accept_subscriber_with_claims(
        Box::new(AssertAdmittedTransport(owner_transport)),
        alice,
        claims,
    );
    let table = schema
        .tables()
        .iter()
        .find(|table| table.name == "files")
        .unwrap();
    // A serving Core admits Global registrations. The browser worker serves
    // the foreground's ordinary Local registration within its admitted scope.
    let opts = ReadOpts {
        tier: if core {
            jazz::tx::DurabilityTier::Global
        } else {
            jazz::tx::DurabilityTier::Local
        },
        local_updates: jazz::db::LocalUpdates::Immediate,
        ..ReadOpts::default()
    };
    for (id, bytes) in &files {
        let query = Query::from("files")
            .filter(eq(col("id"), lit(id.0)))
            .limit(1);
        for _ in 0..2 {
            let (rows, owner_rebuilds, rebuilds) = read(&foreground, &owner, &query, opts.clone());
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].row_uuid(), *id);
            assert_eq!(
                rows[0].cell(table, "contents"),
                Some(Value::Bytes(bytes.clone()))
            );
            assert_eq!(rows[0].cell(table, "folder"), Some(Value::Uuid(folder.0)));
            assert_eq!(
                owner_rebuilds, 0,
                "serving a native witness never rebuilds its payload"
            );
            assert_eq!(
                rebuilds, 1,
                "only the requested public value is reconstructed"
            );
        }
    }
    if core {
        let query = Query::from("files")
            .filter(eq(col("id"), lit(hidden.0)))
            .limit(1);
        let (rows, owner_rebuilds, _) = read(&foreground, &owner, &query, opts.clone());
        assert!(
            rows.is_empty(),
            "resident bytes cannot bypass fresh Core authorization"
        );
        assert_eq!(owner_rebuilds, 0);
    }
    block_on(foreground.close()).unwrap();
    block_on(owner.close()).unwrap();
}

#[test]
fn core_serves_native_witnesses_without_rebuilding_resident_payloads() {
    resident_reads(true);
}

#[test]
fn scope_relay_serves_native_witnesses_without_rebuilding_resident_payloads() {
    resident_reads(false);
}
