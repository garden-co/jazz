use crate::accepted_bundle_reads::{open, schema, seed};
use crate::accepted_rpc_reads::alice;
use crate::common::{compile_schema, read_and_allow_all_writes};
use jazz::db::block_on;
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};
use std::collections::BTreeMap;

/// Alice stages INSERT or UPDATE after an empty or nonempty accepted read.
/// Bob revokes write policies before commit, sometimes during a durable hold.
/// Unpinned handles must refuse without publication; selected pins remain fixed.
#[test]
fn accepted_predicates_do_not_keep_revoked_staged_write_permission() {
    use jazz::db::InsertOptions;
    use jazz::db::shard::{CommitRef, Held, HeldRow};
    use jazz::tools::OpenTransactionId;
    use std::task::{Context, Poll, Waker};
    for kind in 0..4 {
        for empty in [false, true] {
            for update in [false, true] {
                for held in [false, true] {
                    let server = open(9, AuthorSubject::SYSTEM, true, true);
                    seed(&server);
                    let version = compile_schema(&schema(true)).version_id();
                    let selected = match kind {
                        0 => None,
                        1 => Some(
                            block_on(server.register_schema_view(compile_schema(&schema(true))))
                                .unwrap(),
                        ),
                        2 => Some(server.governed_view(version).unwrap()),
                        _ => {
                            let bundle = block_on(server.publish_bundle(&schema(true))).unwrap();
                            server.check_session_pin(alice(), Some(&bundle)).unwrap();
                            Some(server.pinned_view(version, Some(&bundle)).unwrap().unwrap())
                        }
                    };
                    let db = selected.as_ref().unwrap_or(&server);
                    let id = OpenTransactionId::new();
                    block_on(db.begin_serializable_for_identity(id, alice())).unwrap();
                    let mut query = Query::from("notes");
                    if empty {
                        query.limit = Some(0);
                    }
                    let query = db.prepare_query(&query).unwrap();
                    let read = block_on(
                        db.accepted_transaction_query(id, query, false, 1000)
                            .unwrap(),
                    )
                    .unwrap();
                    assert_eq!(read.evidence.len(), usize::from(!empty));
                    let row = RowUuid::from_bytes([if update { 4 } else { 77 }; 16]);
                    let cells = BTreeMap::from([("body".into(), Value::String("changed".into()))]);
                    if update {
                        block_on(
                            db.tx_ref(id)
                                .update("notes", row, cells, Default::default()),
                        )
                        .unwrap();
                    } else {
                        block_on(db.tx_ref(id).insert(
                            "notes",
                            cells,
                            InsertOptions {
                                row_id: Some(row),
                                ..Default::default()
                            },
                        ))
                        .unwrap();
                    }
                    let part = CommitRef {
                        node: [55; 16],
                        tx: 1,
                    };
                    if held {
                        assert!(matches!(
                            server
                                .shard()
                                .hold(
                                    part,
                                    &[HeldRow {
                                        table: "notes".into(),
                                        row: *row.0.as_bytes()
                                    }],
                                    b"held part"
                                )
                                .unwrap(),
                            Held::At(_)
                        ));
                    }
                    let mut commit = Box::pin(db.commit_serializable_handle(id));
                    let mut cx = Context::from_waker(Waker::noop());
                    if held {
                        assert!(std::future::Future::poll(commit.as_mut(), &mut cx).is_pending());
                    }
                    let revoked = SchemaBuilder::new()
                        .table(
                            TableSchemaBuilder::new("notes")
                                .column("body", ColumnType::Text)
                                .policies(
                                    read_and_allow_all_writes(PolicyExpr::True)
                                        .with_insert(PolicyExpr::False)
                                        .with_update(Some(PolicyExpr::False), PolicyExpr::False),
                                ),
                        )
                        .build();
                    block_on(server.activate_schema(compile_schema(&revoked))).unwrap();
                    if held {
                        server.shard().release(part).unwrap();
                    }
                    let Poll::Ready(result) = std::future::Future::poll(commit.as_mut(), &mut cx)
                    else {
                        panic!("commit stayed pending")
                    };
                    assert_eq!(
                        result.is_ok(),
                        kind == 3,
                        "kind={kind}, empty={empty}, update={update}, held={held}: {result:?}"
                    );
                    let query = server.prepare_query(&Query::from("notes")).unwrap();
                    let rows = block_on(server.all(&query, Default::default())).unwrap();
                    assert_eq!(
                        rows.iter()
                            .any(|row| row.cell("body") == Some(Value::String("changed".into()))),
                        kind == 3
                    );
                }
            }
        }
    }
}
