//! The server tick's follow memo (`sync::FollowMemo`): while a tick ships
//! to its sessions, what one session's followed key finds (the current
//! rows of a supporting table with that value) is read once for every
//! session following the same key; each session's policies still apply to
//! the rows afterwards. The memo holds only while no row changed on the
//! store's connection since it read them: it is dropped on the first use
//! after a write or a rollback.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, ReadTier, block_on, follow_memo_stats_for_test,
    set_follow_memo_oracle_for_test, set_follow_read_hook_for_test,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema, physical_column};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("users")
                    .column("name", ColumnType::Text)
                    .nullable_fk_column("referrer", "users")
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("senderId", "users")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open(node: u8) -> Db {
    let config = DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap()
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

fn text(s: &str) -> Value {
    Value::String(s.to_owned())
}

/// The names of the users `db` holds, by id.
fn users(db: &Db, tier: ReadTier) -> BTreeMap<RowUuid, String> {
    let prepared = db.prepare_query(&Query::from("users")).unwrap();
    let opts = ReadOpts {
        tier,
        ..ReadOpts::default()
    };
    block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| {
            let name = row
                .cells()
                .find_map(|(name, value)| match (name, value) {
                    ("name", Some(Value::String(s))) => Some(s.clone()),
                    _ => None,
                })
                .unwrap_or_default();
            (row.row_uuid(), name)
        })
        .collect()
}

fn pump(server: &Db, clients: &[Db]) {
    for _ in 0..4 {
        for client in clients {
            block_on(client.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
    }
    for client in clients {
        block_on(client.tick()).unwrap();
    }
}

/// A server with three users and two messages (from users 1 and 2), and
/// `replicas` replicas each reading the messages with their senders (the
/// returned streams keep their reads live).
fn setup(replicas: u8) -> (Db, Vec<Db>, Vec<Box<dyn std::any::Any>>) {
    setup_reading(replicas, Query::from("messages").include("senderId"))
}

/// [`setup`], each replica reading `query`; user 4 was referred by user 3.
fn setup_reading(replicas: u8, query: Query) -> (Db, Vec<Db>, Vec<Box<dyn std::any::Any>>) {
    set_follow_memo_oracle_for_test(true);
    let server = open(0);
    for (n, name) in [(1, "ada"), (2, "bob"), (3, "cy")] {
        server
            .seed_settled_mergeable_for_bootstrap(
                "users",
                id(1, n),
                AuthorSubject::SYSTEM,
                cells(&[("name", text(name))]),
            )
            .unwrap();
    }
    server
        .seed_settled_mergeable_for_bootstrap(
            "users",
            id(1, 4),
            AuthorSubject::SYSTEM,
            cells(&[("name", text("dee")), ("referrer", Value::Uuid(id(1, 3).0))]),
        )
        .unwrap();
    for (n, sender) in [(1, 1), (2, 2)] {
        server
            .seed_settled_mergeable_for_bootstrap(
                "messages",
                id(2, n),
                AuthorSubject::SYSTEM,
                cells(&[
                    ("senderId", Value::Uuid(id(1, sender).0)),
                    ("body", text(&format!("m{n}"))),
                ]),
            )
            .unwrap();
    }
    let connect = |node: u8| {
        let client = open(node);
        let (client_side, server_side) = duplex();
        let _ = block_on(client.connect_upstream(client_side));
        server
            .accept_subscriber(server_side, AuthorSubject::SYSTEM)
            .unwrap();
        client
    };
    let clients: Vec<Db> = (1..=replicas).map(connect).collect();
    let mut streams: Vec<Box<dyn std::any::Any>> = Vec::new();
    for client in &clients {
        let prepared = client.prepare_query(&query).unwrap();
        streams.push(Box::new(
            block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap(),
        ));
    }
    pump(&server, &clients);
    (server, clients, streams)
}

/// Move message 2 to sender 3 on the server: in the next tick each
/// replica's result ships the new sender, followed by its id.
fn move_to_cy(server: &Db) {
    block_on(server.update(
        "messages",
        id(2, 2),
        cells(&[("senderId", Value::Uuid(id(1, 3).0))]),
        Default::default(),
    ))
    .unwrap();
}

/// Rename user 3 with raw SQL on the store's connection: a change no
/// engine path made, standing in for one a fan-out step might make.
fn rename_cy(conn: &rusqlite::Connection) {
    let name = physical_column(conn, "users", "name");
    let changed = conn
        .execute(
            &format!("UPDATE users SET {name} = 'cy, renamed' WHERE id = ?1"),
            rusqlite::params![id(1, 3).0.as_bytes().to_vec()],
        )
        .unwrap();
    assert!(changed >= 1, "the hook renamed the followed user");
}

/// What each replica holds of user 3, sorted (the order sessions are
/// served in is the store's).
fn shipped(clients: &[Db]) -> Vec<String> {
    let mut names: Vec<String> = clients
        .iter()
        .map(|client| users(client, ReadTier::LocalOnly)[&id(1, 3)].clone())
        .collect();
    names.sort();
    names
}

/// M1. Two sessions following one key in one tick: the second session's
/// follow reuses the rows the first one read.
#[test]
fn two_sessions_following_one_key_in_a_tick_read_it_once() {
    let (server, clients, _streams) = setup(2);
    let before = follow_memo_stats_for_test();
    move_to_cy(&server);
    pump(&server, &clients);
    let after = follow_memo_stats_for_test();
    assert!(
        after.hits > before.hits,
        "the second session reused the first one's read: {before:?} -> {after:?}"
    );
    // Nothing in a fan-out changes rows: a step that came to would drop
    // the memo on every tick and take the reuse away without any wrong
    // result to show it.
    assert_eq!(
        after.dropped, before.dropped,
        "no tick dropped its memo: {before:?} -> {after:?}"
    );
    for client in &clients {
        assert_eq!(users(client, ReadTier::LocalOnly)[&id(1, 3)], "cy");
    }
}

/// M2. A row that changes on the store's connection inside the fan-out
/// (here, a test hook writing the followed user right after the first
/// session read it) drops the memo: the session served next reads again
/// and ships the row as it is now, not the first session's copy. (The raw
/// write notifies nothing, so the replica served first keeps "cy".)
#[test]
fn a_row_changed_inside_the_fan_out_drops_the_follow_memo() {
    let (server, clients, _streams) = setup(2);
    let wrote = std::rc::Rc::new(std::cell::Cell::new(false));
    let once = wrote.clone();
    set_follow_read_hook_for_test(Some(Box::new(move |conn, table, _column| {
        if table != "users" || once.replace(true) {
            return;
        }
        rename_cy(conn);
    })));
    // The memo's own oracle would flag a stale reuse before the replica
    // shows it; off here, so the replica's copy is what the gate reads.
    set_follow_memo_oracle_for_test(false);
    let before = follow_memo_stats_for_test();
    move_to_cy(&server);
    pump(&server, &clients);
    set_follow_read_hook_for_test(None);
    let after = follow_memo_stats_for_test();
    assert!(wrote.get(), "the hook ran inside the fan-out");
    assert!(
        after.dropped > before.dropped,
        "the change dropped the memo: {before:?} -> {after:?}"
    );
    assert_eq!(
        users(&server, ReadTier::LocalOnly)[&id(1, 3)],
        "cy, renamed",
        "the server holds the renamed user"
    );
    assert_eq!(
        shipped(&clients),
        ["cy", "cy, renamed"],
        "the session served after the write shipped the user as the store holds it"
    );
}

/// M4. One value followed through two columns of one table in one tick
/// (a message's sender by `users.id`, and the users that sender referred
/// by `users.referrer`) is two keys: each finds its own rows.
#[test]
fn one_value_followed_through_two_columns_is_two_keys() {
    let query = Query::from("messages")
        .include("senderId")
        .array_subquery(ArraySubquery::new(
            "referred", "users", "referrer", "senderId",
        ));
    let (server, clients, _streams) = setup_reading(1, query);
    move_to_cy(&server);
    pump(&server, &clients);
    let held = users(&clients[0], ReadTier::LocalOnly);
    assert_eq!(
        held.get(&id(1, 3)).map(String::as_str),
        Some("cy"),
        "the new sender: {held:?}"
    );
    assert_eq!(
        held.get(&id(1, 4)).map(String::as_str),
        Some("dee"),
        "whom it referred: {held:?}"
    );
}

/// M3. A rollback changes rows back without counting as a change. Here
/// the hook opens a transaction and renames the followed user after the
/// first session's read, and rolls it back after the second session's
/// (which the write made read again, and whose copy of the renamed user
/// the memo then keeps): the third session reads again and ships the user
/// as the store holds it, not the renamed copy the rollback undid.
#[test]
fn a_rollback_inside_the_fan_out_drops_the_follow_memo() {
    let (server, clients, _streams) = setup(3);
    let reads = std::rc::Rc::new(std::cell::Cell::new(0));
    let count = reads.clone();
    set_follow_read_hook_for_test(Some(Box::new(move |conn, table, _column| {
        if table != "users" {
            return;
        }
        count.set(count.get() + 1);
        match count.get() {
            1 => {
                conn.execute_batch("BEGIN").unwrap();
                rename_cy(conn);
            }
            2 => conn.execute_batch("ROLLBACK").unwrap(),
            _ => {}
        }
    })));
    set_follow_memo_oracle_for_test(false);
    let before = follow_memo_stats_for_test();
    move_to_cy(&server);
    pump(&server, &clients);
    set_follow_read_hook_for_test(None);
    let after = follow_memo_stats_for_test();
    assert_eq!(
        reads.get(),
        3,
        "each session read the user: before the write, after it, after the rollback"
    );
    assert!(
        after.dropped >= before.dropped + 2,
        "the write and the rollback each dropped the memo: {before:?} -> {after:?}"
    );
    assert_eq!(
        users(&server, ReadTier::LocalOnly)[&id(1, 3)],
        "cy",
        "the rename was rolled back"
    );
    assert_eq!(
        shipped(&clients),
        ["cy", "cy", "cy, renamed"],
        "only the session served inside the transaction holds the renamed user"
    );
}
