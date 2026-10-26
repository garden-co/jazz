//! A new authority read cannot reuse an old subscription's settlement marker.
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, SerializedReadResult,
    SubscriptionEvent, SubscriptionStream, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::{Pin, pin},
    task::{Context, Poll, Waker},
};
fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([1; 16])
}
fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("allowed", ColumnType::Boolean)
                    .column("text", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::eq_literal(
                        "allowed",
                        jazz::tools::Value::Boolean(true),
                    ))),
            )
            .build(),
    )
    .unwrap()
}
fn open(node: u8) -> Db {
    let authority = node == 90;
    let db = block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: if authority {
                AuthorSubject::SYSTEM
            } else {
                alice()
            },
        },
    )))
    .unwrap();
    if authority {
        db.set_authority();
        db.initialize_authority_catalogue().unwrap();
    }
    db
}
fn connect(client: &Db, server: &Db) {
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice());
}
fn turn(dbs: &[&Db]) {
    for db in dbs {
        block_on(db.tick()).unwrap();
    }
}
fn poll<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
fn warm(db: &Db, dbs: &[&Db]) -> SubscriptionStream {
    let prepared = db.prepare_query(&Query::from("notes")).unwrap();
    let mut stream =
        block_on(db.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    for _ in 0..100 {
        if let Some(SubscriptionEvent::Delta {
            settled: true,
            added,
            ..
        }) = stream.try_next_event()
        {
            assert_eq!(added.len(), 1);
            return stream;
        }
        turn(dbs);
    }
    panic!("subscription did not warm")
}
async fn read(tab: &Db, query: &[u8]) -> Result<SerializedReadResult, jazz::db::Error> {
    tab.all_serialized_query(
        query,
        ReadOpts::for_read_tier(ReadTier::Remote),
        None,
        None,
        None,
        false,
        || false,
        |_| {},
    )
    .await
}

/// Bob revokes Alice's cached note while her tab/owner retain a settled live
/// subscription. A new strict remote read must wait for the authority and then
/// return no rows, rather than reusing the old subscription's global marker.
#[test]
fn retained_subscriptions_cannot_settle_a_new_remote_read_before_the_authority() {
    for (private, warm_tab) in [(false, false), (false, true), (true, false), (true, true)] {
        let server = open(90);
        let row = RowUuid::from_bytes([7; 16]);
        block_on(server.insert(
            "notes",
            BTreeMap::from([
                ("allowed".into(), Value::Bool(true)),
                (
                    "text".into(),
                    Value::String("Alice's protected note".into()),
                ),
            ]),
            InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        ))
        .unwrap();
        let owner = open(50);
        if private {
            owner.set_scope_isolated_storage_owner().unwrap();
        }
        let tab = open(1);
        if private {
            tab.set_private_owner_client().unwrap();
        } else {
            tab.set_non_durable_client();
        }
        connect(&owner, &server);
        connect(&tab, &owner);
        let _stream = warm(
            if warm_tab { &tab } else { &owner },
            &[&tab, &owner, &server, &owner, &tab],
        );
        block_on(server.update(
            "notes",
            row,
            BTreeMap::from([("allowed".into(), Value::Bool(false))]),
            Default::default(),
        ))
        .unwrap();
        let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
        let mut pending = pin!(read(&tab, &query));
        for _ in 0..4 {
            assert!(
                poll(pending.as_mut()).is_pending(),
                "private={private}, warm_tab={warm_tab}: old settlement answered a new authority read"
            );
            turn(&[&tab, &owner, &tab]);
        }
        let mut result = None;
        for _ in 0..100 {
            turn(&[&tab, &owner, &server, &owner, &tab]);
            if let Poll::Ready(rows) = poll(pending.as_mut()) {
                result = Some(rows.unwrap());
                break;
            }
        }
        let Some(SerializedReadResult::Rows(rows)) = result else {
            panic!("authority read did not finish")
        };
        assert!(
            rows.is_empty(),
            "private={private}, warm_tab={warm_tab}: revoked note survived fresh authority read"
        );
    }
}

/// Alice's detached tab misses Bob's revocation. Reconnecting to her warm owner
/// must revalidate the declared cache before settling a Remote subscription;
/// a later grant must restore the same row through that live subscription.
#[test]
fn reconnect_revalidates_cached_rows_at_every_owner_hop() {
    for private in [false, true] {
        let server = open(90);
        let row = RowUuid::from_bytes([8; 16]);
        block_on(server.insert(
            "notes",
            BTreeMap::from([
                ("allowed".into(), Value::Bool(true)),
                ("text".into(), Value::String("Alice's cached note".into())),
            ]),
            InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        ))
        .unwrap();
        let owner = open(50);
        if private {
            owner.set_scope_isolated_storage_owner().unwrap();
        }
        let tab = open(1);
        if private {
            tab.set_private_owner_client().unwrap();
        } else {
            tab.set_non_durable_client();
        }
        connect(&owner, &server);
        let (up, down) = duplex();
        let upstream = block_on(tab.connect_upstream(up));
        let subscriber = owner.accept_subscriber(down, alice());
        let dbs = [&tab, &owner, &server, &owner, &tab];
        let _owner_stream = warm(&owner, &dbs);
        let mut stream = warm(&tab, &dbs);
        assert!(tab.detach_connection(&upstream));
        owner.disconnect_subscriber(subscriber);
        block_on(server.update(
            "notes",
            row,
            BTreeMap::from([("allowed".into(), Value::Bool(false))]),
            Default::default(),
        ))
        .unwrap();
        for _ in 0..12 {
            turn(&[&owner, &server, &owner]);
        }
        let prepared = owner.prepare_query(&Query::from("notes")).unwrap();
        assert!(
            block_on(owner.all(&prepared, ReadOpts::for_read_tier(ReadTier::Remote)))
                .unwrap()
                .is_empty()
        );
        connect(&tab, &owner);
        for _ in 0..20 {
            turn(&dbs);
        }
        let prepared = tab.prepare_query(&Query::from("notes")).unwrap();
        assert!(
            block_on(tab.all(&prepared, ReadOpts::for_read_tier(ReadTier::Remote)))
                .unwrap()
                .is_empty(),
            "private={private}: detached tab retained revoked row"
        );
        assert!(std::iter::from_fn(|| stream.try_next_event()).any(|event| matches!(event, SubscriptionEvent::Delta { removed, .. } if removed.iter().any(|gone| gone.row_uuid == row))));
        block_on(server.update(
            "notes",
            row,
            BTreeMap::from([("allowed".into(), Value::Bool(true))]),
            Default::default(),
        ))
        .unwrap();
        for _ in 0..20 {
            turn(&dbs);
        }
        let rows = block_on(tab.all(&prepared, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
        assert_eq!(
            rows.len(),
            1,
            "private={private}: restored grant did not reach the tab"
        );
        assert_eq!(rows[0].row_uuid(), row);
        assert!(std::iter::from_fn(|| stream.try_next_event()).any(|event| matches!(event, SubscriptionEvent::Delta { added, .. } if added.iter().any(|found| found.row_uuid() == row))));
    }
}
