//! Ordinary SELECT policy contracts, without created-row/candidate policies.
use super::common;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, SubscriptionEvent, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::collections::BTreeMap;

fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
}
fn id(n: u128) -> RowUuid {
    RowUuid(uuid::Uuid::from_u128(n))
}
fn allowed() -> PolicyExpr {
    PolicyExpr::eq_literal("allowed", jazz::tools::Value::Boolean(true))
}
fn equality(text: &str) -> PolicyExpr {
    PolicyExpr::eq_literal("body", jazz::tools::Value::Text(text.to_owned()))
}
fn authority(policy: PolicyExpr) -> Db {
    authority_kind(policy, ColumnType::Text)
}
fn authority_kind(policy: PolicyExpr, kind: ColumnType) -> Db {
    let db = block_on(Db::open(configuration(policy, kind))).unwrap();
    db.set_authority();
    assert!(db.table_identity("items").unwrap().is_some());
    db
}
fn public_configuration(policy: PolicyExpr, kind: ColumnType) -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("items")
                .column("body", kind)
                .column("allowed", ColumnType::Boolean)
                .policies(
                    TablePolicies::new()
                        .with_select(policy)
                        .with_insert(PolicyExpr::True)
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                        .with_delete(PolicyExpr::True),
                ),
        )
        .build()
}
fn configuration(policy: PolicyExpr, kind: ColumnType) -> DbConfig {
    let public = public_configuration(policy, kind);
    // The backend may serve multiple users; every tested read/subscription
    // below supplies the real unprivileged Alice policy subject explicitly.
    DbConfig::new(
        common::compile_schema(&public),
        DbIdentity {
            node: NodeUuid::from_bytes([93; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}
fn put(db: &Db, n: u128, body: &str, allow: bool) {
    block_on(db.upsert(
        "items",
        id(n),
        BTreeMap::from([
            ("body".into(), Value::String(body.to_owned())),
            ("allowed".into(), Value::Bool(allow)),
        ]),
        Default::default(),
    ))
    .unwrap();
}
fn visible(db: &Db, query: Query) -> Result<Vec<RowUuid>, jazz::db::Error> {
    let prepared = db.prepare_query(&query).unwrap();
    block_on(db.all_for_identity(&prepared, ReadOpts::default(), alice())).map(|rows| {
        let mut ids = rows.iter().map(|r| r.row_uuid()).collect::<Vec<_>>();
        ids.sort();
        ids
    })
}

/// Alice's ordinary Eq/NOT-Eq policy compares the complete logical suffix;
/// another conjunct still denies rows even when their content matches.
#[test]
fn ordinary_read_policy_long_literal_and_negative_keep_the_other_policy() {
    let literal = "a".repeat(70 * 1024);
    let mut different = literal.clone();
    different.replace_range(different.len() - 1.., "b");
    for mode in 0..3 {
        let cmp = equality(&literal);
        let db = authority(PolicyExpr::and(vec![
            allowed(),
            match mode {
                0 => cmp,
                1 => PolicyExpr::not(cmp),
                _ => PolicyExpr::Cmp {
                    column: "body".into(),
                    op: jazz::tools::CmpOp::Ne,
                    value: jazz::tools::PolicyValue::Literal(jazz::tools::Value::Text(
                        literal.clone(),
                    )),
                },
            },
        ]));
        put(&db, 1, &literal, true);
        put(&db, 2, &different, true);
        put(&db, 3, &literal, false);
        assert_eq!(
            visible(&db, Query::from("items")).unwrap(),
            vec![id(if mode != 0 { 2 } else { 1 })]
        );
    }
}

/// Bob withdraws Alice's ordinary row grant after an opening. No cached
/// logical answer may retain the row in Alice's next subscription frontier.
#[test]
fn ordinary_read_policy_subscription_withdrawal_clears_the_answer_frontier() {
    let literal = "a".repeat(70 * 1024);
    let db = authority(PolicyExpr::and(vec![allowed(), equality(&literal)]));
    put(&db, 1, &literal, true);
    let prepared = db.prepare_query(&Query::from("items")).unwrap();
    let mut sub =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), alice())).unwrap();
    match sub.try_next_event().expect("opening") {
        SubscriptionEvent::Delta { added, .. } => assert_eq!(
            added.iter().map(|r| r.row_uuid()).collect::<Vec<_>>(),
            vec![id(1)]
        ),
        other => panic!("unexpected {other:?}"),
    }
    put(&db, 1, &literal, false);
    block_on(db.tick()).unwrap();
    match sub.try_next_event().expect("withdrawal") {
        SubscriptionEvent::Delta { removed, .. } => assert_eq!(
            removed.iter().map(|r| r.row_uuid).collect::<Vec<_>>(),
            vec![id(1)]
        ),
        other => panic!("unexpected {other:?}"),
    }
    assert!(visible(&db, Query::from("items")).unwrap().is_empty());
}

/// An unrelated row beyond Alice's point query must not be prewarmed. The
/// same row IS a dependency of her unfiltered read, even behind LIMIT 1:
/// its oversized logical value refuses that whole operation, not a prefix.
#[test]
fn ordinary_read_policy_byte_budget_is_not_pagination_or_column_prewarm() {
    let literal = "a".repeat(70 * 1024);
    let db = authority(equality(&literal));
    put(&db, 1, &literal, true);
    put(&db, 2, &"b".repeat(9 * 1024 * 1024), true);
    let point = Query::from("items").filter(jazz::query::eq(
        jazz::query::col("id"),
        jazz::query::lit(Value::Uuid(id(1).0)),
    ));
    assert_eq!(visible(&db, point.clone()).unwrap(), vec![id(1)]);
    let error = visible(&db, Query::from("items").limit(1)).unwrap_err();
    assert!(error.message.contains("budget"), "{error}");
    // A failed preparation leaves neither scratch answers nor a poisoned
    // progress hook/savepoint behind for the next genuine bounded read.
    assert_eq!(visible(&db, point).unwrap(), vec![id(1)]);
}

/// Alice's negative predicate still has a cumulative dependency-row limit.
/// Pagination cannot turn an overflow into a seemingly complete grant.
#[test]
fn ordinary_read_policy_negative_row_overflow_refuses_then_recovers() {
    let literal = "a".repeat(70 * 1024);
    let db = authority(PolicyExpr::not(equality(&literal)));
    for n in 1..=513 {
        put(&db, n, "short", true);
    }
    let error = visible(&db, Query::from("items").limit(1).offset(512)).unwrap_err();
    assert!(error.message.contains("budget"), "{error}");
    let point = Query::from("items").filter(jazz::query::eq(
        jazz::query::col("id"),
        jazz::query::lit(Value::Uuid(id(1).0)),
    ));
    assert_eq!(visible(&db, point).unwrap(), vec![id(1)]);
}

/// Alice's bytea policy compares all bytes, including a differing non-UTF8
/// suffix beyond the inline threshold; text coercion cannot supply a grant.
#[test]
fn ordinary_read_policy_bytea_keeps_binary_type_and_full_suffix() {
    let literal = vec![0xff; 70 * 1024];
    let mut different = literal.clone();
    *different.last_mut().unwrap() = 0xfe;
    let db = authority_kind(
        PolicyExpr::eq_literal("body", jazz::tools::Value::Bytea(literal.clone())),
        ColumnType::Bytea,
    );
    for (n, bytes) in [(1, literal), (2, different)] {
        block_on(db.upsert(
            "items",
            id(n),
            BTreeMap::from([
                ("body".into(), Value::Bytes(bytes)),
                ("allowed".into(), Value::Bool(true)),
            ]),
            Default::default(),
        ))
        .unwrap();
    }
    assert_eq!(visible(&db, Query::from("items")).unwrap(), vec![id(1)]);
}

/// A long BLOB literal must not become equal to Alice's TEXT solely because
/// the logical bytes agree. This unsupported type combination fails closed.
#[test]
fn ordinary_read_policy_cross_kind_literal_cannot_grant_text_rows() {
    let literal = "a".repeat(70 * 1024);
    let public = public_configuration(
        PolicyExpr::eq_literal(
            "body",
            jazz::tools::Value::Bytea(literal.as_bytes().to_vec()),
        ),
        ColumnType::Text,
    );
    // The existing public schema type-checker rejects this before a Db can
    // open. This is admission evidence, not runtime proof of the later guard.
    let error = jazz::schema::JazzSchema::new(&public)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("OperandTypeMismatch"),
        "{}",
        error.chars().take(160).collect::<String>()
    );
}

/// Alice's explicitly unsupported long-literal IN list must not obtain the
/// new equality capability through an unmetered list-lowering shortcut.
#[test]
fn ordinary_read_policy_long_in_list_remains_explicitly_refused() {
    let literal = "a".repeat(70 * 1024);
    let db = authority(PolicyExpr::InList {
        column: "body".into(),
        values: vec![jazz::tools::PolicyValue::Literal(jazz::tools::Value::Text(
            literal.clone(),
        ))],
    });
    put(&db, 1, &literal, true);
    let error = visible(&db, Query::from("items")).unwrap_err();
    assert!(error.message.contains("65536"), "{error}");
}

/// Alice can read equal logical bytes after a tail edit and its inverse;
/// logical equality does not depend on an epoch/cell's stored identity.
#[test]
fn ordinary_read_policy_edited_cells_keep_logical_equality() {
    let literal = "a".repeat(70 * 1024);
    let db = authority(equality(&literal));
    put(&db, 1, &literal, true);
    put(&db, 2, &literal, true);
    block_on(db.append_value("items", id(2), "body", b"x".to_vec())).unwrap();
    assert_eq!(visible(&db, Query::from("items")).unwrap(), vec![id(1)]);
    block_on(db.splice_value(
        "items",
        id(2),
        "body",
        jazz::db::LargeValueUpdateSplice {
            at: literal.len() as u64,
            delete: 1,
            insert: Vec::new(),
        },
        Default::default(),
    ))
    .unwrap();
    assert_eq!(
        visible(&db, Query::from("items")).unwrap(),
        vec![id(1), id(2)]
    );
}

/// Alice retains permission for both versions. A frozen read must prepare
/// the old cell at its captured cut, not the newer physical current value.
#[test]
fn ordinary_read_policy_preparation_and_rows_share_the_captured_cut() {
    let before = "a".repeat(70 * 1024);
    let mut after = before.clone();
    after.replace_range(after.len() - 1.., "b");
    let db = authority(PolicyExpr::or(vec![equality(&before), equality(&after)]));
    put(&db, 1, &before, true);
    let snapshot = block_on(db.capture_snapshot()).unwrap();
    put(&db, 1, &after, true);
    let prepared = db.prepare_query(&Query::from("items")).unwrap();
    let frozen = block_on(db.all_for_identity(
        &prepared,
        ReadOpts {
            read_view: jazz::protocol::ReadViewSpec {
                source: jazz::protocol::ReadViewSourceSpec::Snapshot { snapshot },
                include_deleted: false,
            },
            ..ReadOpts::default()
        },
        alice(),
    ))
    .unwrap();
    let current = block_on(db.all_for_identity(&prepared, ReadOpts::default(), alice())).unwrap();
    assert_eq!(frozen.len(), 1);
    assert_eq!(current.len(), 1);
    assert_eq!(frozen[0].cell("body"), Some(Value::String(before)));
    assert_eq!(current[0].cell("body"), Some(Value::String(after)));
}

/// Fault injection is only at the public chunk-store boundary. Policy/schema,
/// authenticated reader and all database evaluation remain the real core path.
#[derive(Clone, Default)]
struct FaultStore {
    objects: std::sync::Arc<std::sync::Mutex<BTreeMap<jazz::chunk_store::NodeKey, Vec<u8>>>>,
    mode: std::sync::Arc<std::sync::atomic::AtomicU8>,
    gets: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}
impl jazz::chunk_store::ChunkStore for FaultStore {
    fn put<'a>(
        &'a self,
        node: &'a jazz::chunk_store::NodeKey,
        bytes: &'a [u8],
    ) -> jazz::chunk_store::StoreFuture<'a, ()> {
        Box::pin(async move {
            self.objects.lock().unwrap().insert(*node, bytes.to_vec());
            Ok(())
        })
    }
    fn get<'a>(
        &'a self,
        node: &'a jazz::chunk_store::NodeKey,
    ) -> jazz::chunk_store::StoreFuture<'a, Option<bytes::Bytes>> {
        Box::pin(async move { self.get_now(node).expect("synchronous test store") })
    }
    fn delete<'a>(
        &'a self,
        node: &'a jazz::chunk_store::NodeKey,
    ) -> jazz::chunk_store::StoreFuture<'a, ()> {
        Box::pin(async move {
            self.objects.lock().unwrap().remove(node);
            Ok(())
        })
    }
    fn get_now(
        &self,
        node: &jazz::chunk_store::NodeKey,
    ) -> Option<Result<Option<bytes::Bytes>, jazz::db::Error>> {
        use std::sync::atomic::Ordering::SeqCst;
        self.gets.fetch_add(1, SeqCst);
        if self.mode.load(SeqCst) == 1 {
            return Some(Ok(None));
        }
        let mut value = self.objects.lock().unwrap().get(node).cloned();
        if self.mode.load(SeqCst) == 2
            && let Some(bytes) = value.as_mut()
        {
            if let Some(first) = bytes.first_mut() {
                *first ^= 1;
            } else {
                bytes.push(1);
            }
        }
        Some(Ok(value.map(bytes::Bytes::from)))
    }
}

/// Alice's negative policy cannot turn missing/corrupt bytes into a grant.
/// Reopen clears the database cache; a final genuine-byte retry succeeds.
#[test]
fn ordinary_read_policy_missing_and_corrupt_chunks_refuse_before_negative_grants() {
    use std::sync::atomic::Ordering::SeqCst;
    let literal = "a".repeat(70 * 1024);
    let mut body = literal.clone();
    body.replace_range(body.len() - 1.., "b");
    let policy = PolicyExpr::not(equality(&literal));
    let store = FaultStore::default();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("policy.sqlite");
    let open = || {
        block_on(Db::open_at(
            configuration(policy.clone(), ColumnType::Text)
                .with_chunk_store(std::sync::Arc::new(store.clone())),
            &path,
            jazz::db::FileOptions::default(),
        ))
        .unwrap()
    };
    let db = open();
    db.set_authority();
    put(&db, 1, &body, true);
    for _ in 0..8 {
        block_on(db.tick()).unwrap();
    }
    assert!(
        !store.objects.lock().unwrap().is_empty(),
        "real settled chunks must be offloaded"
    );
    block_on(db.close()).unwrap();
    drop(db);
    for mode in [1, 2, 0] {
        store.mode.store(mode, SeqCst);
        store.gets.store(0, SeqCst);
        let db = open();
        db.set_authority();
        let result = visible(&db, Query::from("items"));
        assert!(
            store.gets.load(SeqCst) > 0,
            "the policy must actually ask for offloaded bytes"
        );
        if mode == 0 {
            assert_eq!(result.unwrap(), vec![id(1)]);
        } else {
            assert_eq!(result.unwrap_err().code, jazz::db::ErrorCode::Storage);
        }
        block_on(db.close()).unwrap();
        drop(db);
    }
}

/// Alice must not gain a root through NOT EXISTS when both indexed UUID
/// correlation keys are NULL and Bob's support body matches the long literal.
#[test]
fn ordinary_read_policy_nullable_negative_dependency_refuses() {
    let literal = "a".repeat(70 * 1024);
    let correlation = PolicyExpr::Cmp {
        column: "key".into(),
        op: jazz::tools::CmpOp::Eq,
        value: jazz::tools::PolicyValue::SessionRef(vec!["__jazz_outer_row".into(), "key".into()]),
    };
    let root_policy = PolicyExpr::not(PolicyExpr::Exists {
        table: "support".into(),
        condition: Box::new(PolicyExpr::and(vec![correlation, equality(&literal)])),
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("roots")
                .nullable_column("key", ColumnType::Uuid)
                .policies(
                    TablePolicies::new()
                        .with_select(root_policy)
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchemaBuilder::new("support")
                .nullable_column("key", ColumnType::Uuid)
                .column("body", ColumnType::Text)
                .index_only(["key"])
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True),
                ),
        )
        .build();
    // Public conversion currently refuses NOT EXISTS. Keep this original
    // shape as an admission negative; the private walker has a separate test.
    let error = jazz::schema::JazzSchema::new(&schema)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("do not support Exists"),
        "{}",
        error.chars().take(160).collect::<String>()
    );
}

/// Alice's NOT reverse inheritance cannot treat an indexed UUID-array edge
/// as a missing scalar edge and expose the root Bob's support policy covers.
#[test]
fn ordinary_read_policy_reverse_array_negative_dependency_refuses() {
    let literal = "a".repeat(70 * 1024);
    let root_policy = PolicyExpr::not(PolicyExpr::InheritsReferencing {
        operation: jazz::tools::Operation::Select,
        source_table: "support".into(),
        via_column: "roots".into(),
        max_depth: Some(1),
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("roots").policies(
                TablePolicies::new()
                    .with_select(root_policy)
                    .with_insert(PolicyExpr::True),
            ),
        )
        .table(
            TableSchemaBuilder::new("support")
                .array_fk_column("roots", "roots")
                .column("body", ColumnType::Text)
                .index_only(["roots"])
                .policies(
                    TablePolicies::new()
                        .with_select(equality(&literal))
                        .with_insert(PolicyExpr::True),
                ),
        )
        .build();
    // Do not extend public policy support just to reach a private guard.
    let error = jazz::schema::JazzSchema::new(&schema)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("do not support InheritsReferencing"),
        "{}",
        error.chars().take(160).collect::<String>()
    );
}

/// Alice's new SELECT-policy support must not widen ordinary query filters.
/// Even after a successful policy read, a >64KiB caller filter still refuses.
#[test]
fn ordinary_read_policy_support_does_not_enable_long_query_filters() {
    let literal = "a".repeat(70 * 1024);
    let db = authority(equality(&literal));
    put(&db, 1, &literal, true);
    assert_eq!(visible(&db, Query::from("items")).unwrap(), vec![id(1)]);
    let filtered = Query::from("items").filter(jazz::query::eq(
        jazz::query::col("body"),
        jazz::query::lit(Value::String(literal)),
    ));
    let error = db.prepare_query(&filtered).unwrap_err();
    assert!(error.message.contains("65536"), "{error}");
    assert_eq!(visible(&db, Query::from("items")).unwrap(), vec![id(1)]);
}

/// Alice's excessive read-only VM work must refuse within her open batch;
/// its earlier staged write survives, a bounded read works, and commit works.
///
/// alice: stage -> costly ordinary read refuses -> point read -> commit
#[test]
fn ordinary_read_policy_vm_refusal_preserves_staged_overlay() {
    let literal = "a".repeat(70 * 1024);
    let db = authority(PolicyExpr::not(equality(&literal)));
    for n in 1..=511 {
        put(&db, n, "short", true);
    }
    // Exactly 512 roots including the staged row, below the sentinel. Each
    // of 900 distinct inline text ranges must be evaluated per row, including
    // the normal large-capable-cell kind guard. The test must observe the
    // work-budget refusal, not an SQL depth error or a row-limit refusal.
    let costly = db
        .prepare_query(
            &Query::from("items").filter(jazz::query::all_of((0..900).map(|n| {
                jazz::query::gt(
                    jazz::query::col("body"),
                    jazz::query::lit(Value::String(format!("a{n:04}"))),
                )
            }))),
        )
        .unwrap();
    let point = db
        .prepare_query(&Query::from("items").filter(jazz::query::eq(
            jazz::query::col("id"),
            jazz::query::lit(Value::Uuid(id(512).0)),
        )))
        .unwrap();
    let batch = jazz::tools::OpenTransactionId::new();
    block_on(db.begin_mergeable_for_identity(batch, alice())).unwrap();
    let tx = db.tx_ref(batch);
    block_on(tx.upsert(
        "items",
        id(512),
        BTreeMap::from([
            ("body".into(), Value::String("staged".into())),
            ("allowed".into(), Value::Bool(true)),
        ]),
        Default::default(),
    ))
    .unwrap();
    let error = block_on(tx.all_prepared_at(&costly, false, None, jazz::db::ReadTier::LocalOnly))
        .unwrap_err();
    assert!(error.message.contains("budget"), "{error}");
    let rows =
        block_on(tx.all_prepared_at(&point, false, None, jazz::db::ReadTier::LocalOnly)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cell("body"), Some(Value::String("staged".into())));
    block_on(db.commit_mergeable_handle(batch)).unwrap();
    assert_eq!(
        visible(
            &db,
            Query::from("items").filter(jazz::query::eq(
                jazz::query::col("id"),
                jazz::query::lit(Value::Uuid(id(512).0)),
            ))
        )
        .unwrap(),
        vec![id(512)]
    );
}

/// The trusted backend registers Alice/Bob's actual account claims through
/// the existing host API. Equal content never lets either reader inherit the
/// other's grant, and withdrawing Alice's claim cannot retain a cached grant.
#[test]
fn ordinary_read_policy_keeps_authenticated_reader_claim_isolation() {
    let literal = "a".repeat(70 * 1024);
    let public = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("items")
                .column("owner", ColumnType::Text)
                .column("body", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::and(vec![
                            common::session_eq("owner", &["claims", "account"]),
                            equality(&literal),
                        ]))
                        .with_insert(PolicyExpr::True),
                ),
        )
        .build();
    let db = block_on(Db::open(DbConfig::new(
        common::compile_schema(&public),
        DbIdentity {
            node: NodeUuid::from_bytes([96; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    db.set_authority();
    let bob = AuthorSubject::authenticated("https://issuer.example", "bob").unwrap();
    for (who, name) in [(alice(), "alice"), (bob, "bob")] {
        db.set_identity_claims(
            who,
            BTreeMap::from([("\0claims:account".into(), Value::String(name.into()))]),
        );
    }
    for (n, owner) in [(1, "alice"), (2, "bob")] {
        block_on(db.upsert(
            "items",
            id(n),
            BTreeMap::from([
                ("owner".into(), Value::String(owner.into())),
                ("body".into(), Value::String(literal.clone())),
            ]),
            Default::default(),
        ))
        .unwrap();
    }
    let query = db.prepare_query(&Query::from("items")).unwrap();
    let read = |who| {
        block_on(db.all_for_identity(&query, ReadOpts::default(), who))
            .unwrap()
            .iter()
            .map(|r| r.row_uuid())
            .collect::<Vec<_>>()
    };
    assert_eq!(read(alice()), vec![id(1)]);
    assert_eq!(read(bob), vec![id(2)]);
    db.set_identity_claims(alice(), BTreeMap::new());
    assert!(read(alice()).is_empty());
    assert_eq!(read(bob), vec![id(2)]);
}

/// Alice's full bounded-policy read works, but partial projections remain
/// explicitly outside this extension instead of using an unbound decoder.
#[test]
fn ordinary_read_policy_partial_projection_refuses_then_full_read_works() {
    let literal = "a".repeat(70 * 1024);
    let db = authority(equality(&literal));
    put(&db, 1, &literal, true);
    let mut query = Query::from("items");
    query.select = Some(vec!["body".into()]);
    query.partial.push(jazz::query::PartialSelect {
        column: "body".into(),
        part: jazz::query::Part::TextUtf8 { from: 0, to: 4 },
    });
    let error = visible(&db, query).unwrap_err();
    assert!(error.message.contains("partial projections"), "{error}");
    assert_eq!(visible(&db, Query::from("items")).unwrap(), vec![id(1)]);
}

// The ordinary compiler inherits USING-or-CHECK for UPDATE. A CHECK-only
// parent's further dependency must be discovered before evaluating the row;
// candidate-specific USING defaults must not suppress that ordinary walk.
fn check_only_update_nested_inheritance(reverse: bool) {
    use jazz::tools::Operation;
    let literal = "a".repeat(70 * 1024);
    let parent_check = PolicyExpr::Exists {
        table: "grants".into(),
        condition: Box::new(PolicyExpr::and(vec![
            PolicyExpr::Cmp {
                column: "id".into(),
                op: jazz::tools::CmpOp::Eq,
                value: jazz::tools::PolicyValue::SessionRef(vec![
                    "__jazz_outer_row".into(),
                    "grant".into(),
                ]),
            },
            equality(&literal),
        ])),
    };
    let root_policy = if reverse {
        PolicyExpr::InheritsReferencing {
            operation: Operation::Update,
            source_table: "parents".into(),
            via_column: "item".into(),
            max_depth: Some(3),
        }
    } else {
        PolicyExpr::Inherits {
            operation: Operation::Update,
            via_column: "parent".into(),
            max_depth: Some(3),
        }
    };
    let mut items = TableSchemaBuilder::new("items").policies(
        TablePolicies::new()
            .with_select(root_policy)
            .with_insert(PolicyExpr::True),
    );
    let mut parents = TableSchemaBuilder::new("parents")
        .fk_column("grant", "grants")
        .policies(
            TablePolicies::new()
                .with_select(PolicyExpr::True)
                .with_insert(PolicyExpr::True)
                .with_update(None, parent_check),
        );
    if reverse {
        parents = parents
            .fk_column("item", "items")
            .index_only(["item", "grant"]);
    } else {
        items = items.fk_column("parent", "parents");
    }
    let public = SchemaBuilder::new()
        .table(items)
        .table(parents)
        .table(
            TableSchemaBuilder::new("grants")
                .column("body", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(equality(&literal))
                        .with_insert(PolicyExpr::True)
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True),
                ),
        )
        .build();
    let db = block_on(Db::open(DbConfig::new(
        common::compile_schema(&public),
        DbIdentity {
            node: NodeUuid::from_bytes([96; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    db.set_authority();
    assert!(db.table_identity("items").unwrap().is_some());
    let put_grant = |n, body: &str| {
        block_on(db.upsert(
            "grants",
            id(n),
            BTreeMap::from([("body".into(), Value::String(body.to_owned()))]),
            Default::default(),
        ))
        .unwrap();
    };
    for n in 1..=2 {
        put_grant(n + 20, if n == 1 { &literal } else { "nonmatching" });
        let mut parent = BTreeMap::from([("grant".into(), Value::Uuid(id(n + 20).0))]);
        let mut item = BTreeMap::new();
        if reverse {
            parent.insert("item".into(), Value::Uuid(id(n).0));
        } else {
            item.insert("parent".into(), Value::Uuid(id(n + 10).0));
        }
        block_on(db.upsert("parents", id(n + 10), parent, Default::default())).unwrap();
        block_on(db.upsert("items", id(n), item, Default::default())).unwrap();
    }
    // All visibility uses the real ordinary Alice subject, not backend setup.
    assert_eq!(visible(&db, Query::from("items")).unwrap(), vec![id(1)]);
    put_grant(21, "no longer matches");
    assert!(visible(&db, Query::from("items")).unwrap().is_empty());
    put_grant(22, &literal);
    assert_eq!(visible(&db, Query::from("items")).unwrap(), vec![id(2)]);
}

#[test]
fn ordinary_read_policy_check_only_update_nested_forward() {
    check_only_update_nested_inheritance(false);
}

#[test]
fn ordinary_read_policy_check_only_update_nested_reverse() {
    check_only_update_nested_inheritance(true);
}
