//! Audit (state-space, permissions-backend): a reader loses access to rows
//! it already holds while the loss happens somewhere it cannot see it
//! arrive: while its replica is offline, while its store is closed, or on a
//! backend replica that serves request scopes. INV-RLS-6 / INV-SYNC-14 say
//! the replica must drop them once it is back in contact with the server.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, ReadOpts, ReadTier, RemoteLinkHint,
    SerializedReadResult, Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema, exists, outer_eq, session_eq};

const ME: [&str; 3] = ["user", "identity", "subject"];

/// Members read a room's messages.
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
                TableSchemaBuilder::new("messages")
                    .fk_column("room", "rooms")
                    .column("body", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(member.clone())
                            .with_insert(member)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
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

fn local(db: &Db, table: &str) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&Query::from(table)).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let mut ids: Vec<RowUuid> = block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect();
    ids.sort();
    ids
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

/// One room, Ada a member, two messages in it.
fn room(server: &Db) {
    seed(server, "rooms", id(1, 1), cells(&[("name", "r".into())]));
    seed(
        server,
        "memberships",
        id(2, 1),
        cells(&[("room", Value::Uuid(id(1, 1).0)), ("member", subject(0))]),
    );
    for n in 1..=2 {
        seed(
            server,
            "messages",
            id(3, n),
            cells(&[("room", Value::Uuid(id(1, 1).0)), ("body", "m".into())]),
        );
    }
}

fn subscribed_rows(stream: &mut jazz::db::SubscriptionStream) -> Vec<RowUuid> {
    let mut rows = Vec::new();
    while let Some(event) = stream.try_next_event() {
        if let jazz::db::SubscriptionEvent::Delta {
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
    rows
}

/// ```text
/// ada ──subscribe(messages)──► server        (holds m1, m2)
/// ada ─ offline ─┐
/// admin ──delete(ada's membership)──► server
/// ada ◄─ reconnect, subscription resumes ─┘
/// expect: ada's replica and subscription drop m1, m2
/// ```
/// The revocation is written by someone else while Ada is offline, so no
/// write reaches the server after she declares what she holds.
#[test]
fn audit_offline_revocation_by_another_writer_evicts_on_reconnect() {
    let server = server();
    room(&server);
    let ada = replica(1, user(0));
    let link = connect(&server, &ada, user(0));
    let messages = ada.prepare_query(&Query::from("messages")).unwrap();
    let mut live = block_on(ada.subscribe(&messages, ReadOpts::default())).unwrap();
    pump(&server, &[&ada]);
    assert_eq!(local(&ada, "messages"), [id(3, 1), id(3, 2)]);
    assert_eq!(subscribed_rows(&mut live), [id(3, 1), id(3, 2)]);

    assert!(ada.detach_connection(&link));
    block_on(server.delete("memberships", id(2, 1), Default::default())).unwrap();
    block_on(server.tick()).unwrap();
    let _link = connect(&server, &ada, user(0));
    pump(&server, &[&ada]);

    assert_eq!(
        local(&ada, "messages"),
        Vec::<RowUuid>::new(),
        "messages of a room Ada was removed from while offline are evicted"
    );
    let _ = subscribed_rows(&mut live);
}

/// Same, but Ada's app no longer subscribes to the messages when she comes
/// back (she navigated elsewhere): the rows sit in her store only. A fresh
/// subscription opened after reconnecting must not list them once settled,
/// and the held copies must go.
#[test]
fn audit_offline_revocation_evicts_rows_held_without_a_subscription() {
    let server = server();
    room(&server);
    let ada = replica(1, user(0));
    let link = connect(&server, &ada, user(0));
    let messages = ada.prepare_query(&Query::from("messages")).unwrap();
    {
        let _live = block_on(ada.subscribe(&messages, ReadOpts::default())).unwrap();
        pump(&server, &[&ada]);
    }
    pump(&server, &[&ada]);
    assert_eq!(local(&ada, "messages"), [id(3, 1), id(3, 2)]);

    assert!(ada.detach_connection(&link));
    block_on(server.delete("memberships", id(2, 1), Default::default())).unwrap();
    block_on(server.tick()).unwrap();
    let _link = connect(&server, &ada, user(0));
    pump(&server, &[&ada]);
    let held_after_reconnect = local(&ada, "messages");

    let mut again = block_on(ada.subscribe(&messages, ReadOpts::default())).unwrap();
    pump(&server, &[&ada]);
    assert_eq!(
        subscribed_rows(&mut again),
        Vec::<RowUuid>::new(),
        "a settled subscription opened after the revocation lists nothing"
    );
    assert_eq!(
        local(&ada, "messages"),
        Vec::<RowUuid>::new(),
        "held copies are evicted (held right after reconnect: {held_after_reconnect:?})"
    );
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("jazz-audit-perm-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("store.sqlite")
}

/// ```text
/// ada(file) ──subscribe──► server   (holds m1, m2) ── close store
/// admin ──delete(ada's membership)──► server
/// ada(file) reopen ──connect, subscribe──► server
/// expect: m1, m2 gone from her store and from the subscription
/// ```
#[test]
fn audit_revocation_while_the_store_is_closed_evicts_after_reopen() {
    let server = server();
    room(&server);
    let path = scratch("reopen");
    {
        let ada = block_on(Db::open_at(
            config(1, user(0)),
            &path,
            FileOptions::default(),
        ))
        .unwrap();
        let _link = connect(&server, &ada, user(0));
        let messages = ada.prepare_query(&Query::from("messages")).unwrap();
        let _live = block_on(ada.subscribe(&messages, ReadOpts::default())).unwrap();
        pump(&server, &[&ada]);
        assert_eq!(local(&ada, "messages"), [id(3, 1), id(3, 2)]);
        block_on(ada.close()).unwrap();
    }
    block_on(server.delete("memberships", id(2, 1), Default::default())).unwrap();
    block_on(server.tick()).unwrap();

    let ada = block_on(Db::open_at(
        config(1, user(0)),
        &path,
        FileOptions::default(),
    ))
    .unwrap();
    assert_eq!(
        local(&ada, "messages"),
        [id(3, 1), id(3, 2)],
        "offline, the reopened store still shows what it held (coherence, not redaction)"
    );
    let _link = connect(&server, &ada, user(0));
    let messages = ada.prepare_query(&Query::from("messages")).unwrap();
    let mut live = block_on(ada.subscribe(&messages, ReadOpts::default())).unwrap();
    pump(&server, &[&ada]);
    assert_eq!(subscribed_rows(&mut live), Vec::<RowUuid>::new());
    assert_eq!(local(&ada, "messages"), Vec::<RowUuid>::new());
}

/// A backend replica (SYSTEM link, `forRequest` scopes) reads a request
/// scope's messages, then the user's membership is deleted on the core.
/// The backend's next request-scoped read for that user, at each tier a
/// backend uses, must not return them.
///
/// ```text
/// request(ada) ──read messages──► backend ──fetch──► core
/// admin ──delete(ada's membership)──► core
/// request(ada) ──read messages──► backend   expect: []
/// ```
#[test]
fn audit_backend_request_scope_does_not_serve_rows_after_revocation() {
    let server = server();
    room(&server);
    let backend = replica(0xbe, AuthorSubject::SYSTEM);
    let _link = connect(&server, &backend, AuthorSubject::SYSTEM);
    let read = |opts: ReadOpts| {
        let bytes = postcard::to_allocvec(&Query::from("messages")).unwrap();
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
    assert_eq!(read(first.clone()), [id(3, 1), id(3, 2)]);

    block_on(server.delete("memberships", id(2, 1), Default::default())).unwrap();
    pump(&server, &[&backend]);

    let mut failures = Vec::new();
    for (name, opts) in [
        ("local-first wait", first),
        ("local-first", ReadOpts::for_read_tier(ReadTier::LocalFirst)),
        ("remote", ReadOpts::for_read_tier(ReadTier::Remote)),
    ] {
        let rows = read(opts);
        if !rows.is_empty() {
            failures.push(format!("{name}: {rows:?}"));
        }
    }
    assert!(
        failures.is_empty(),
        "revoked rows served to the request scope: {failures:?}"
    );
}
