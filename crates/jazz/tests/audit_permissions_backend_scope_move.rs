//! Audit (state-space, permissions-backend): a row moves out of a reader's
//! scope while the reader holds an optimistic write to it, and a backend
//! replica's request scopes see access granted and revoked on the core.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, ReadTier, RemoteLinkHint, SerializedReadResult,
    SubscriptionEvent, SubscriptionStream, Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema, exists, outer_eq, session_eq};

const ME: [&str; 3] = ["user", "identity", "subject"];

/// Members of a room read and edit its docs; a doc can be moved to
/// another room by anyone who may edit it.
fn schema() -> JazzSchema {
    let member = exists(
        "memberships",
        vec![outer_eq("room", "room"), session_eq("member", &ME)],
    );
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("rooms")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("memberships")
                    .fk_column("room", "rooms")
                    .column("member", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("docs")
                    .fk_column("room", "rooms")
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(member.clone())
                            .with_insert(member.clone())
                            .with_update(Some(member), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build(),
    )
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x40 + n; 16])
}

fn subject(n: u8) -> Value {
    Value::String(user(n).principal_parts().1)
}

fn config(node: u8, author: AuthorSubject) -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}

fn server() -> Db {
    block_on(Db::open_history_complete(config(0, AuthorSubject::SYSTEM))).unwrap()
}

fn replica(node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(config(node, author))).unwrap()
}

fn id(kind: u8, n: u8) -> RowUuid {
    RowUuid::from_bytes([kind, n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
}

fn cells(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

fn seed(server: &Db, table: &str, row: RowUuid, values: BTreeMap<String, Value>) {
    server
        .seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, values)
        .unwrap();
}

fn connect(server: &Db, client: &Db, who: AuthorSubject) -> Upstream {
    let (client_side, server_side) = duplex();
    let upstream = block_on(client.connect_upstream(client_side));
    client.set_remote_link_hint(RemoteLinkHint::Live);
    server.accept_subscriber(server_side, who).unwrap();
    upstream
}

fn local(db: &Db, table: &str) -> Vec<(RowUuid, Value)> {
    let prepared = db.prepare_query(&Query::from(table)).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let mut rows: Vec<(RowUuid, Value)> = block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| {
            let title = row
                .cells()
                .find_map(|(name, value)| (name == "title").then(|| value.cloned()).flatten())
                .unwrap_or(Value::String(String::new()));
            (row.row_uuid(), title)
        })
        .collect();
    rows.sort_by_key(|(row, _)| *row);
    rows
}

fn pump(server: &Db, clients: &[&Db]) {
    for _ in 0..6 {
        for client in clients {
            block_on(client.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
    }
    for client in clients {
        block_on(client.tick()).unwrap();
    }
}

fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..200 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not complete");
}

fn rows_of(stream: &mut SubscriptionStream, rows: &mut Vec<RowUuid>) {
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta {
            reset,
            added,
            removed,
            ..
        } = event
        {
            if reset {
                rows.clear();
            }
            rows.retain(|row| !removed.iter().any(|gone| gone.row_uuid == *row));
            rows.extend(added.iter().map(|row| row.row_uuid()));
        }
    }
    rows.sort();
}

/// Rooms 1 and 2; Ada (0) is a member of room 1 only, Bo (1) of both. One
/// doc in room 1.
fn world(server: &Db) {
    for n in 1..=2 {
        seed(server, "rooms", id(1, n), cells(&[("name", "r".into())]));
    }
    seed(
        server,
        "memberships",
        id(2, 1),
        cells(&[("room", Value::Uuid(id(1, 1).0)), ("member", subject(0))]),
    );
    for (m, room) in [(2u8, 1u8), (3, 2)] {
        seed(
            server,
            "memberships",
            id(2, m),
            cells(&[("room", Value::Uuid(id(1, room).0)), ("member", subject(1))]),
        );
    }
    seed(
        server,
        "docs",
        id(3, 1),
        cells(&[("room", Value::Uuid(id(1, 1).0)), ("title", "v0".into())]),
    );
}

/// ```text
/// ada ──subscribe(docs)──► server              (holds doc in room 1)
/// ada ─ offline ─ update(doc.title = "ada")    (optimistic, pending)
/// bo  ──update(doc.room = room 2)──► server     (doc leaves ada's scope)
/// ada ◄─ reconnect ─ server rejects her update (USING: not a member)
/// expect: ada's update is reported rejected, her store and her live
///         subscription hold neither her optimistic title nor the doc
/// ```
#[test]
fn audit_optimistic_update_to_a_row_that_left_scope_is_rolled_back_and_evicted() {
    let server = server();
    world(&server);
    let ada = replica(1, user(0));
    let bo = replica(2, user(1));
    let ada_link = connect(&server, &ada, user(0));
    let _bo_link = connect(&server, &bo, user(1));
    let docs = ada.prepare_query(&Query::from("docs")).unwrap();
    let mut live = block_on(ada.subscribe(&docs, ReadOpts::default())).unwrap();
    let bo_docs = bo.prepare_query(&Query::from("docs")).unwrap();
    let _bo_live = block_on(bo.subscribe(&bo_docs, ReadOpts::default())).unwrap();
    pump(&server, &[&ada, &bo]);
    let mut seen = Vec::new();
    rows_of(&mut live, &mut seen);
    assert_eq!(seen, [id(3, 1)]);

    assert!(ada.detach_connection(&ada_link));
    let mine = block_on(ada.update(
        "docs",
        id(3, 1),
        cells(&[("title", "ada".into())]),
        Default::default(),
    ))
    .unwrap();
    assert_eq!(local(&ada, "docs"), [(id(3, 1), Value::from("ada"))]);

    let moved = block_on(bo.update(
        "docs",
        id(3, 1),
        cells(&[("room", Value::Uuid(id(1, 2).0))]),
        Default::default(),
    ))
    .unwrap();
    drive(
        &[&bo, &server, &bo],
        bo.wait_for_transaction(moved.mergeable_tx_id(), DurabilityTier::Global),
    )
    .unwrap();

    let _ada_link = connect(&server, &ada, user(0));
    let fate = drive(
        &[&ada, &server, &ada],
        ada.wait_for_transaction(mine.mergeable_tx_id(), DurabilityTier::Global),
    );
    pump(&server, &[&ada, &bo]);
    rows_of(&mut live, &mut seen);

    let held = local(&ada, "docs");
    let server_title = local(&server, "docs");
    assert!(
        fate.is_err() && held.is_empty() && seen.is_empty(),
        "fate: {:?}; ada holds {held:?}; ada's subscription lists {seen:?}; server: {server_title:?}",
        fate.map_err(|error| error.message)
    );
    assert_eq!(server_title, [(id(3, 1), Value::from("v0"))]);
}

/// A backend replica serves a request scope for Ada; she is then added to
/// room 2, where a doc lives. Her next request-scoped read (and an
/// identity subscription the backend opened for her) must see it, and the
/// subscription must drop it again when the membership goes.
#[test]
fn audit_backend_identity_subscription_follows_grant_and_revocation() {
    let server = server();
    world(&server);
    seed(
        &server,
        "docs",
        id(3, 2),
        cells(&[("room", Value::Uuid(id(1, 2).0)), ("title", "r2".into())]),
    );
    let backend = replica(0xbe, AuthorSubject::SYSTEM);
    let _link = connect(&server, &backend, AuthorSubject::SYSTEM);
    let read = |opts: ReadOpts| {
        let bytes = postcard::to_allocvec(&Query::from("docs")).unwrap();
        let result = drive(
            &[&backend, &server, &backend],
            backend.all_serialized_query(
                &bytes,
                opts,
                None,
                Some((user(0), BTreeMap::new())),
                None,
            ),
        );
        match result.unwrap() {
            SerializedReadResult::Rows(rows) => {
                let mut ids: Vec<RowUuid> = rows.iter().map(|row| row.row_uuid()).collect();
                ids.sort();
                ids
            }
            other => panic!("expected rows, got {other:?}"),
        }
    };
    let first = ReadOpts {
        first_load: jazz::db::FirstLoad::WaitForRemote { timeout_ms: 60_000 },
        ..ReadOpts::for_read_tier(ReadTier::LocalFirst)
    };
    assert_eq!(read(first.clone()), [id(3, 1)]);
    let prepared = backend
        .prepare_query(&Query::from("docs"))
        .unwrap()
        .with_identity_claims(user(0), BTreeMap::new());
    let mut live =
        block_on(backend.subscribe_for_identity(&prepared, first.clone(), user(0))).unwrap();
    pump(&server, &[&backend]);
    let mut seen = Vec::new();
    rows_of(&mut live, &mut seen);
    assert_eq!(seen, [id(3, 1)]);

    // Granted: Ada joins room 2.
    seed(
        &server,
        "memberships",
        id(2, 9),
        cells(&[("room", Value::Uuid(id(1, 2).0)), ("member", subject(0))]),
    );
    block_on(server.tick()).unwrap();
    pump(&server, &[&backend]);
    rows_of(&mut live, &mut seen);
    let mut problems = Vec::new();
    if seen != [id(3, 1), id(3, 2)] {
        problems.push(format!("subscription after grant: {seen:?}"));
    }
    for (name, opts) in [
        ("local-first wait", first.clone()),
        ("local-first", ReadOpts::for_read_tier(ReadTier::LocalFirst)),
    ] {
        let rows = read(opts);
        if rows != [id(3, 1), id(3, 2)] {
            problems.push(format!("{name} read after grant: {rows:?}"));
        }
    }

    // Revoked again.
    block_on(server.delete("memberships", id(2, 9), Default::default())).unwrap();
    pump(&server, &[&backend]);
    rows_of(&mut live, &mut seen);
    if seen != [id(3, 1)] {
        problems.push(format!("subscription after revocation: {seen:?}"));
    }
    for (name, opts) in [
        ("local-first wait", first),
        ("local-first", ReadOpts::for_read_tier(ReadTier::LocalFirst)),
    ] {
        let rows = read(opts);
        if rows != [id(3, 1)] {
            problems.push(format!("{name} read after revocation: {rows:?}"));
        }
    }
    assert!(problems.is_empty(), "{problems:#?}");
}
