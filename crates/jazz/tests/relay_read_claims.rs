//! Connection claims remain authoritative when a relay serves reads.

use crate::common::{compile_schema, read_and_allow_all_writes, session_eq};
#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, ReadTier, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, provider_claim_key};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use std::collections::BTreeMap;
use std::future::Future;
use std::task::{Context, Poll, Waker};

fn claims(region: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([(provider_claim_key("region"), Value::String(region.into()))])
}

fn open(node: u8, author: AuthorSubject) -> Db {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("region", ColumnType::Text)
                    .policies(read_and_allow_all_writes(session_eq(
                        "region",
                        &["claims", "region"],
                    ))),
            )
            .build(),
    );
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    );
    if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap()
}

fn drive<T>(nodes: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(result) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return result;
        }
        for db in nodes {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("relay read did not finish");
}

/// Alice reads through a backend relay with host claims for region A. An absent
/// connection override inherits A; empty claims grant no rows; region B
/// grants only B. Serving an override must not change the host's claims.
///
/// authority (A + B) -> relay -> Alice (absent / empty / B)
#[test]
fn relay_reads_inherit_only_absent_connection_claims() {
    let alice = AuthorSubject::for_test_bytes([1; 16]);
    for (connection_claims, expected) in [
        (None, vec!["a"]),
        (Some(BTreeMap::new()), vec![]),
        (Some(claims("b")), vec!["b"]),
    ] {
        let authority = open(0, AuthorSubject::SYSTEM);
        let relay = open(1, AuthorSubject::SYSTEM);
        relay.set_identity_claims(alice, claims("a"));
        for (id, region) in [(1, "a"), (2, "b")] {
            authority
                .seed_settled_mergeable_for_bootstrap(
                    "notes",
                    RowUuid::from_bytes([id; 16]),
                    AuthorSubject::SYSTEM,
                    BTreeMap::from([("region".into(), Value::String(region.into()))]),
                )
                .unwrap();
        }
        // This trusted backend link can supply both regions. The downstream
        // session's claims, rather than an upstream filter, must limit reads.
        let (up, down) = duplex_transport::duplex();
        let _relay_upstream = block_on(relay.connect_upstream(up));
        authority.accept_subscriber(down, AuthorSubject::SYSTEM);
        for (node, override_claims, expected) in
            [(2, connection_claims, expected), (3, None, vec!["a"])]
        {
            let foreground = open(node, alice);
            let (up, down) = duplex_transport::duplex();
            let _foreground_upstream = block_on(foreground.connect_upstream(up));
            relay.accept_subscriber_with_claims(down, alice, override_claims);
            let query = postcard::to_allocvec(&Query::from("notes").select(["region"])).unwrap();
            let rows = drive(
                &[&foreground, &relay, &authority],
                foreground.all_serialized_query(
                    &query,
                    ReadOpts {
                        tier: ReadTier::Remote,
                        ..Default::default()
                    },
                    None,
                    None,
                    None,
                    false,
                    || false,
                    |attachment| foreground.detach_query(attachment),
                ),
            )
            .unwrap();
            let jazz::db::SerializedReadResult::Rows(rows) = rows else {
                panic!("rows");
            };
            let regions: Vec<_> = rows
                .iter()
                .map(|row| {
                    row.cells()
                        .find_map(|(name, value)| match (name, value) {
                            ("region", Some(Value::String(value))) => Some(value.as_str()),
                            _ => None,
                        })
                        .expect("region")
                })
                .collect();
            assert_eq!(regions, expected);
        }
    }
}
