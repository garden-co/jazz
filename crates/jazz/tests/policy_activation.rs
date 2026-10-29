//! Activating new policies on a live authority re-checks what its replicas
//! hold: rows a tighter read policy hides leave every subscribed replica.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, ReadTier, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

use common::{compile_schema, read_and_allow_all_writes};

fn schema(read: PolicyExpr) -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("users")
                    .column("name", ColumnType::Text)
                    .policies(read_and_allow_all_writes(read)),
            )
            .build(),
    )
}

fn open(node: u8, author: AuthorSubject) -> Db {
    let config = DbConfig::new(
        schema(PolicyExpr::True),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    );
    block_on(Db::open(config)).unwrap()
}

fn local(db: &Db) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&Query::from("users")).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect()
}

fn pump(server: &Db, client: &Db) {
    for _ in 0..4 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

#[test]
fn tighter_read_policy_evicts_held_rows_from_subscribed_replicas() {
    let server = open(0, AuthorSubject::SYSTEM);
    let row = RowUuid::from_bytes([7; 16]);
    server
        .seed_settled_mergeable_for_bootstrap(
            "users",
            row,
            AuthorSubject::SYSTEM,
            BTreeMap::from([("name".to_owned(), Value::String("ada".into()))]),
        )
        .unwrap();
    let alice = AuthorSubject::for_test_bytes([0x55; 16]);
    let client = open(1, alice);
    let (client_side, server_side) = duplex();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice);
    let prepared = client.prepare_query(&Query::from("users")).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(local(&client), [row]);

    block_on(server.activate_schema(schema(PolicyExpr::False))).unwrap();
    pump(&server, &client);
    assert_eq!(
        local(&client),
        [],
        "the row alice can no longer read leaves"
    );
}
