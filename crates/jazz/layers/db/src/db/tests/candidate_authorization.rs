//! Same-commit insert authorization through public transactions and real authority sync.

use super::*;

#[cfg(feature = "testing")]
async fn create_parent_child_exclusively() -> (crate::db::Db, crate::tx::Fate) {
    use crate::db::{Db, DbConfig, DbIdentity, ExclusiveTxOps};
    use groove::storage::TestStorage;

    let correlation = PublicPolicyExpr::eq_session(
        "id",
        vec!["__jazz_outer_row".to_owned(), "parent_id".to_owned()],
    );
    let child_policy = PublicPolicyExpr::exists_including_created("parents", correlation);
    let schema = PublicSchemaBuilder::new()
        .table(
            PublicTableSchemaBuilder::new("parents")
                .column("title", PublicColumnType::Text)
                .policies(
                    PublicTablePolicies::new()
                        .with_select(PublicPolicyExpr::True)
                        .with_insert(PublicPolicyExpr::eq_session(
                            "$createdBy",
                            vec!["user".to_owned()],
                        )),
                ),
        )
        .table(
            PublicTableSchemaBuilder::new("children")
                .fk_column("parent_id", "parents")
                .column("title", PublicColumnType::Text)
                .policies(
                    PublicTablePolicies::new()
                        .with_select(PublicPolicyExpr::True)
                        .with_insert(child_policy),
                ),
        )
        .build();
    let schema = crate::schema::JazzSchema::new(&schema).expect("compile public policies");
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let author = crate::ids::AuthorSubject::authenticated("https://issuer.example", "alice")
        .expect("valid Alice principal")
        .with_account(crate::account_registry::AccountId(uuid::Uuid::from_u128(1)));
    let alice = Db::open(DbConfig::new(
        schema.clone(),
        TestStorage::new(&refs),
        DbIdentity {
            node: crate::ids::NodeUuid::from_bytes([0xa1; 16]),
            author,
        },
    ))
    .await
    .expect("open Alice's local database");
    let authority = Db::open_history_complete(DbConfig::new(
        schema,
        TestStorage::new(&refs),
        DbIdentity {
            node: crate::ids::NodeUuid::from_bytes([0xc1; 16]),
            author: crate::ids::AuthorSubject::SYSTEM,
        },
    ))
    .await
    .expect("open real core authority");
    let (upstream, downstream) = duplex();
    alice.connect_upstream(upstream).await;
    let _peer = authority.accept_subscriber(downstream, author);
    for _ in 0..32 {
        alice.tick().await.expect("progress Alice's handshake");
        authority
            .tick()
            .await
            .expect("progress authority handshake");
    }
    let tx_id = crate::db::OpenTransactionId::new();
    alice
        .begin_exclusive(tx_id)
        .await
        .expect("open exclusive create");
    let tx = alice.exclusive_tx_ref(tx_id);
    // Both upserts capture exact absence; neither row is seeded outside the bundle.
    tx.upsert(
        "parents",
        crate::ids::RowUuid(uuid::Uuid::from_u128(10)),
        BTreeMap::from([(
            "title".to_owned(),
            Value::String("Alice's parent".to_owned()),
        )]),
        Default::default(),
    )
    .await
    .expect("stage independently authorized parent");
    tx.upsert(
        "children",
        crate::ids::RowUuid(uuid::Uuid::from_u128(11)),
        BTreeMap::from([
            (
                "parent_id".to_owned(),
                Value::Uuid(uuid::Uuid::from_u128(10)),
            ),
            (
                "title".to_owned(),
                Value::String("Alice's child".to_owned()),
            ),
        ]),
        Default::default(),
    )
    .await
    .expect("stage child dependent on the same exclusive bundle");
    let committed = alice
        .commit_exclusive_handle(tx_id)
        .await
        .expect("publish exclusive bundle");
    for _ in 0..128 {
        alice.tick().await.expect("send exclusive bundle");
        authority.tick().await.expect("process exclusive bundle");
        if !matches!(
            alice.write_state(committed).expect("read settlement").fate,
            crate::tx::Fate::Pending
        ) {
            break;
        }
    }
    let state = alice.write_state(committed).expect("read final settlement");
    assert!(
        !matches!(state.fate, crate::tx::Fate::Pending),
        "real authority must settle the bundle"
    );
    (alice, state.fate)
}

/// Alice can atomically create a parent and its dependent child when the
/// child's INSERT policy explicitly admits independently authorized creates.
///
/// alice -- exclusive parent + child, exact absence --> authority
///       <-- whole bundle accepted; both public rows visible --
#[cfg(feature = "testing")]
#[test]
fn candidate_exists_accepts_authorized_parent_child_exclusive_create() {
    crate::db::block_on(async {
        let (alice, fate) = create_parent_child_exclusively().await;
        assert_eq!(
            fate,
            crate::tx::Fate::Accepted,
            "authorized parent and dependent child must be accepted atomically"
        );
        let schema = alice
            .catalogue_schema(alice.current_write_schema().unwrap().schema)
            .unwrap();
        let parents = schema
            .tables()
            .iter()
            .find(|table| table.name == "parents")
            .unwrap();
        let children = schema
            .tables()
            .iter()
            .find(|table| table.name == "children")
            .unwrap();
        assert_eq!(
            alice
                .local_current_row("parents", crate::ids::RowUuid(uuid::Uuid::from_u128(10)))
                .await
                .unwrap()
                .map(|row| (row.row_uuid(), row.cell(parents, "title"))),
            Some((
                crate::ids::RowUuid(uuid::Uuid::from_u128(10)),
                Some(Value::String("Alice's parent".to_owned()))
            )),
        );
        assert_eq!(
            alice
                .local_current_row("children", crate::ids::RowUuid(uuid::Uuid::from_u128(11)))
                .await
                .unwrap()
                .map(|row| (
                    row.row_uuid(),
                    row.cell(children, "parent_id"),
                    row.cell(children, "title")
                )),
            Some((
                crate::ids::RowUuid(uuid::Uuid::from_u128(11)),
                Some(Value::Uuid(uuid::Uuid::from_u128(10))),
                Some(Value::String("Alice's child".to_owned())),
            )),
        );
    });
}


#[cfg(feature = "testing")]
mod proof_graph {
    use super::*;
    use crate::db::{Db, DbConfig, DbIdentity, ExclusiveTxOps};
    use crate::ids::{AuthorSubject, NodeUuid, RowUuid};
    use crate::tx::{Fate, RejectionReason};
    use groove::storage::TestStorage;

    struct Fixture {
        alice: Db,
        authority: Db,
    }

    fn author(name: &str) -> AuthorSubject {
        AuthorSubject::authenticated("https://issuer.example", name)
            .unwrap()
            .with_account(crate::account_registry::AccountId(uuid::Uuid::from_u128(
                if name == "alice" { 1 } else { 2 },
            )))
    }

    fn row(id: u128) -> RowUuid {
        RowUuid(uuid::Uuid::from_u128(id))
    }

    fn outer(column: &str, source: &str) -> PublicPolicyExpr {
        PublicPolicyExpr::eq_session(
            column,
            vec!["__jazz_outer_row".to_owned(), source.to_owned()],
        )
    }

    fn parent(include_created: bool, extra_correlation: bool) -> PublicPolicyExpr {
        let mut predicates = vec![outer("id", "parent_id")];
        if extra_correlation {
            predicates.push(outer("scope", "scope"));
        }
        let condition = PublicPolicyExpr::and(predicates);
        if include_created {
            PublicPolicyExpr::exists_including_created("nodes", condition)
        } else {
            PublicPolicyExpr::Exists {
                table: "nodes".to_owned(),
                condition: Box::new(condition),
            }
        }
    }

    fn policy(dependent: PublicPolicyExpr) -> PublicPolicyExpr {
        PublicPolicyExpr::and(vec![
            PublicPolicyExpr::eq_session("$createdBy", vec!["user".to_owned()]),
            PublicPolicyExpr::eq_literal("allowed", PublicValue::Boolean(true)),
            PublicPolicyExpr::or(vec![
                PublicPolicyExpr::eq_literal("seed", PublicValue::Boolean(true)),
                dependent,
            ]),
        ])
    }

    fn schema(insert: PublicPolicyExpr) -> PublicSchema {
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("nodes")
                    .fk_column("parent_id", "nodes")
                    .column("scope", PublicColumnType::Text)
                    .column("seed", PublicColumnType::Boolean)
                    .column("allowed", PublicColumnType::Boolean)
                    .policies(
                        PublicTablePolicies::new()
                            .with_select(PublicPolicyExpr::True)
                            .with_insert(insert)
                            .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True)
                            .with_delete(PublicPolicyExpr::True),
                    ),
            )
            .build()
    }

    // The public core Db accepts Groove cells, not row_input!'s public schema values.
    fn cells(parent: u128, seed: bool, allowed: bool, scope: &str) -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("parent_id".to_owned(), Value::Uuid(row(parent).0)),
            ("scope".to_owned(), Value::String(scope.to_owned())),
            ("seed".to_owned(), Value::Bool(seed)),
            ("allowed".to_owned(), Value::Bool(allowed)),
        ])
    }

    impl Fixture {
        async fn new(schema: PublicSchema, link_author: AuthorSubject) -> Self {
            let schema = crate::schema::JazzSchema::new(&schema).unwrap();
            let families = schema.column_families();
            let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
            let alice = Db::open(DbConfig::new(
                schema.clone(),
                TestStorage::new(&refs),
                DbIdentity {
                    node: NodeUuid::from_bytes([0x71; 16]),
                    author: author("alice"),
                },
            ))
            .await
            .unwrap();
            let authority = Db::open_history_complete(DbConfig::new(
                schema,
                TestStorage::new(&refs),
                DbIdentity {
                    node: NodeUuid::from_bytes([0x72; 16]),
                    author: AuthorSubject::SYSTEM,
                },
            ))
            .await
            .unwrap();
            let (upstream, downstream) = duplex();
            alice.connect_upstream(upstream).await;
            authority.accept_subscriber(downstream, link_author);
            let fixture = Self { alice, authority };
            fixture.tick(32).await;
            fixture
        }

        async fn tick(&self, count: usize) {
            for _ in 0..count {
                self.alice.tick().await.unwrap();
                self.authority.tick().await.unwrap();
            }
        }

        async fn stage(&self, rows: &[(u128, BTreeMap<String, Value>)]) -> TxId {
            let id = crate::db::OpenTransactionId::new();
            self.alice.begin_exclusive(id).await.unwrap();
            let tx = self.alice.exclusive_tx_ref(id);
            for (id, cells) in rows {
                tx.upsert("nodes", row(*id), cells.clone(), Default::default())
                    .await
                    .unwrap();
            }
            self.alice.commit_exclusive_handle(id).await.unwrap()
        }

        async fn settle(&self, tx: TxId) -> Fate {
            for _ in 0..128 {
                self.tick(1).await;
                let fate = self.alice.write_state(tx).unwrap().fate;
                if fate != Fate::Pending {
                    return fate;
                }
            }
            panic!("history-complete authority did not settle candidate proof");
        }

        async fn assert_rows(&self, rows: &[(u128, BTreeMap<String, Value>)], accepted: bool) {
            let schema = self
                .alice
                .catalogue_schema(self.alice.current_write_schema().unwrap().schema)
                .unwrap();
            let table = schema
                .tables()
                .iter()
                .find(|table| table.name == "nodes")
                .unwrap();
            for (id, expected) in rows {
                let current = self
                    .alice
                    .local_current_row("nodes", row(*id))
                    .await
                    .unwrap();
                if accepted {
                    let current = current.expect("accepted row visible");
                    assert_eq!(current.row_uuid(), row(*id));
                    for name in ["parent_id", "scope", "seed", "allowed"] {
                        assert_eq!(current.cell(table, name), expected.get(name).cloned());
                    }
                } else {
                    assert!(current.is_none(), "the rejected unit left row {id} visible");
                    assert!(
                        self.authority
                            .local_current_row("nodes", row(*id))
                            .await
                            .unwrap()
                            .is_none(),
                        "the authority partially accepted row {id}",
                    );
                }
            }
        }

        async fn check(&self, rows: &[(u128, BTreeMap<String, Value>)], accepted: bool) {
            let tx = self.stage(rows).await;
            assert_eq!(
                self.settle(tx).await,
                if accepted {
                    Fate::Accepted
                } else {
                    Fate::Rejected(RejectionReason::AuthorizationDenied)
                },
            );
            self.assert_rows(rows, accepted).await;
        }
    }

    /// Reverse coordinate order requires dependency wakeups, not table order
    /// or a single scan. Every row is in the same physical table.
    #[test]
    fn candidate_chain_reaches_fixed_point_in_reverse_row_order() {
        crate::db::block_on(async {
            let fixture = Fixture::new(schema(policy(parent(true, true))), author("alice")).await;
            fixture
                .check(
                    &[
                        (1, cells(2, false, true, "room")),
                        (2, cells(3, false, true, "room")),
                        (3, cells(3, true, true, "room")),
                    ],
                    true,
                )
                .await;
        });
    }

    #[test]
    fn candidate_failed_parent_rolls_back_independent_seed_and_child() {
        crate::db::block_on(async {
            let fixture = Fixture::new(schema(policy(parent(true, false))), author("alice")).await;
            fixture
                .check(
                    &[
                        (1, cells(2, false, true, "room")),
                        (2, cells(2, true, false, "room")),
                        (3, cells(3, true, true, "room")),
                    ],
                    false,
                )
                .await;
        });
    }

    #[test]
    fn candidate_self_and_mutual_cycles_without_seed_are_denied() {
        crate::db::block_on(async {
            for rows in [
                vec![(1, cells(1, false, true, "room"))],
                vec![
                    (1, cells(2, false, true, "room")),
                    (2, cells(1, false, true, "room")),
                ],
            ] {
                let fixture =
                    Fixture::new(schema(policy(parent(true, false))), author("alice")).await;
                fixture.check(&rows, false).await;
            }
        });
    }

    #[test]
    fn candidate_independent_or_arm_seeds_a_mutual_cycle() {
        crate::db::block_on(async {
            let fixture = Fixture::new(schema(policy(parent(true, false))), author("alice")).await;
            fixture
                .check(
                    &[
                        (1, cells(2, false, true, "room")),
                        (2, cells(1, true, true, "room")),
                    ],
                    true,
                )
                .await;
        });
    }

    #[test]
    fn candidate_extra_correlation_cannot_borrow_another_room() {
        crate::db::block_on(async {
            let fixture = Fixture::new(schema(policy(parent(true, true))), author("alice")).await;
            fixture
                .check(
                    &[
                        (1, cells(2, false, true, "first")),
                        (2, cells(2, true, true, "second")),
                    ],
                    false,
                )
                .await;
        });
    }

    #[test]
    fn candidate_unmarked_same_table_alias_cannot_borrow_marked_evidence() {
        crate::db::block_on(async {
            let fixture = Fixture::new(
                schema(policy(PublicPolicyExpr::and(vec![
                    parent(true, false),
                    parent(false, false),
                ]))),
                author("alice"),
            )
            .await;
            fixture
                .check(
                    &[
                        (1, cells(2, false, true, "room")),
                        (2, cells(2, true, true, "room")),
                    ],
                    false,
                )
                .await;
        });
    }

    #[test]
    fn candidate_source_keeps_accepted_rows_when_created_rows_are_added() {
        crate::db::block_on(async {
            let fixture = Fixture::new(schema(policy(parent(true, false))), author("alice")).await;
            fixture
                .check(&[(10, cells(10, true, true, "room"))], true)
                .await;
            fixture
                .check(
                    &[
                        (1, cells(2, false, true, "room")),
                        (2, cells(10, false, true, "room")),
                    ],
                    true,
                )
                .await;
        });
    }

    #[test]
    fn candidate_session_subject_cannot_be_spoofed_by_made_by() {
        crate::db::block_on(async {
            let fixture = Fixture::new(schema(policy(parent(true, false))), author("bob")).await;
            fixture
                .check(
                    &[
                        (1, cells(2, false, true, "room")),
                        (2, cells(2, true, true, "room")),
                    ],
                    false,
                )
                .await;
        });
    }

    #[test]
    fn candidate_updated_parent_is_not_same_commit_created_evidence() {
        crate::db::block_on(async {
            let fixture = Fixture::new(schema(policy(parent(true, true))), author("alice")).await;
            fixture
                .check(&[(10, cells(10, true, true, "old"))], true)
                .await;
            let tx = fixture
                .stage(&[
                    (1, cells(10, false, true, "new")),
                    (10, cells(10, true, true, "new")),
                ])
                .await;
            assert_eq!(
                fixture.settle(tx).await,
                Fate::Rejected(RejectionReason::AuthorizationDenied)
            );
            fixture
                .assert_rows(&[(1, cells(10, false, true, "new"))], false)
                .await;
            fixture
                .assert_rows(&[(10, cells(10, true, true, "old"))], true)
                .await;
        });
    }

    #[test]
    fn candidate_proof_work_overflow_rejects_the_complete_unit() {
        crate::db::block_on(async {
            let fixture = Fixture::new(schema(policy(parent(true, true))), author("alice")).await;
            let rows = (1..=1500)
                .map(|id| {
                    (
                        id,
                        cells(
                            if id == 1500 { id } else { id + 1 },
                            id == 1500,
                            true,
                            "room",
                        ),
                    )
                })
                .collect::<Vec<_>>();
            fixture.check(&rows, false).await;
        });
    }

    /// Public schema admission must reject non-monotone/other-operation uses,
    /// before either a client or authority can deploy an ambiguous policy.
    #[test]
    fn candidate_negative_select_update_and_delete_policies_are_rejected() {
        let marked = parent(true, false);
        for policies in [
            PublicTablePolicies::new().with_select(marked.clone()),
            PublicTablePolicies::new().with_update(None, marked.clone()),
            PublicTablePolicies::new().with_delete(marked.clone()),
            PublicTablePolicies::new().with_insert(PublicPolicyExpr::Not(Box::new(marked))),
        ] {
            let schema = PublicSchemaBuilder::new()
                .table(
                    PublicTableSchemaBuilder::new("nodes")
                        .fk_column("parent_id", "nodes")
                        .policies(policies),
                )
                .build();
            assert!(crate::schema::JazzSchema::new(&schema).is_err());
        }
    }

    #[test]
    fn candidate_nested_marked_occurrences_preserve_parent_correlations() {
        crate::db::block_on(async {
            let dependent = PublicPolicyExpr::exists_including_created(
                "nodes",
                PublicPolicyExpr::and(vec![outer("id", "parent_id"), parent(true, true)]),
            );
            let fixture = Fixture::new(schema(policy(dependent)), author("alice")).await;
            fixture
                .check(
                    &[
                        (1, cells(2, false, true, "room")),
                        (2, cells(3, false, true, "room")),
                        (3, cells(3, true, true, "room")),
                    ],
                    true,
                )
                .await;
        });
    }

    #[test]
    fn candidate_nested_unmarked_occurrence_remains_accepted_only() {
        crate::db::block_on(async {
            let dependent = PublicPolicyExpr::exists_including_created(
                "nodes",
                PublicPolicyExpr::and(vec![outer("id", "parent_id"), parent(false, true)]),
            );
            let fixture = Fixture::new(schema(policy(dependent)), author("alice")).await;
            fixture
                .check(
                    &[
                        (1, cells(2, false, true, "room")),
                        (2, cells(3, false, true, "room")),
                        (3, cells(3, true, true, "room")),
                    ],
                    false,
                )
                .await;
        });
    }

    #[test]
    fn candidate_conflicting_absent_inserts_accept_at_most_one_unit() {
        crate::db::block_on(async {
            let public_schema = schema(policy(parent(true, true)));
            let fixture = Fixture::new(public_schema.clone(), author("alice")).await;
            let compiled = crate::schema::JazzSchema::new(&public_schema).unwrap();
            let families = compiled.column_families();
            let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
            let second = Db::open(DbConfig::new(
                compiled,
                TestStorage::new(&refs),
                DbIdentity {
                    node: NodeUuid::from_bytes([0x73; 16]),
                    author: author("alice"),
                },
            ))
            .await
            .unwrap();
            let (upstream, downstream) = duplex();
            second.connect_upstream(upstream).await;
            fixture
                .authority
                .accept_subscriber(downstream, author("alice"));
            for _ in 0..32 {
                fixture.tick(1).await;
                second.tick().await.unwrap();
            }
            // Both replicas capture absence before either unit reaches authority.
            let first_tx = fixture
                .stage(&[
                    (1, cells(2, false, true, "first")),
                    (2, cells(2, true, true, "first")),
                ])
                .await;
            let open = crate::db::OpenTransactionId::new();
            second.begin_exclusive(open).await.unwrap();
            let tx = second.exclusive_tx_ref(open);
            for (id, input) in [
                (1, cells(2, false, true, "second")),
                (2, cells(2, true, true, "second")),
            ] {
                tx.upsert("nodes", row(id), input, Default::default())
                    .await
                    .unwrap();
            }
            let second_tx = second.commit_exclusive_handle(open).await.unwrap();
            for _ in 0..128 {
                fixture.tick(1).await;
                second.tick().await.unwrap();
            }
            let first = fixture.alice.write_state(first_tx).unwrap().fate;
            let second = second.write_state(second_tx).unwrap().fate;
            assert!(
                matches!(
                    (&first, &second),
                    (Fate::Accepted, Fate::Rejected(_)) | (Fate::Rejected(_), Fate::Accepted)
                ),
                "exact absence must select one whole unit: first={first:?}, second={second:?}",
            );
            let scope = if first == Fate::Accepted {
                "first"
            } else {
                "second"
            };
            let schema = fixture
                .authority
                .catalogue_schema(fixture.authority.current_write_schema().unwrap().schema)
                .unwrap();
            let table = schema
                .tables()
                .iter()
                .find(|table| table.name == "nodes")
                .unwrap();
            for id in [1, 2] {
                assert_eq!(
                    fixture
                        .authority
                        .local_current_row("nodes", row(id))
                        .await
                        .unwrap()
                        .unwrap()
                        .cell(table, "scope"),
                    Some(Value::String(scope.to_owned())),
                );
            }
        });
    }

    #[test]
    fn candidate_exclusive_branch_write_is_rejected_before_publication() {
        crate::db::block_on(async {
            let fixture = Fixture::new(schema(policy(parent(true, false))), author("alice")).await;
            let open = crate::db::OpenTransactionId::new();
            fixture.alice.begin_exclusive(open).await.unwrap();
            let tx = fixture.alice.exclusive_tx_ref(open);
            let error = tx
                .upsert(
                    "nodes",
                    row(1),
                    cells(1, true, true, "draft"),
                    crate::db::UpsertOptions {
                        target: crate::db::WriteTarget::BranchView {
                            head: crate::protocol::BranchSelector::new([(
                                "scope",
                                Value::String("draft".to_owned()),
                            )]),
                            base: None,
                        },
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(error.code, crate::db::ErrorCode::Schema);
            fixture.alice.abandon_exclusive_handle(open).unwrap();
            fixture
                .assert_rows(&[(1, cells(1, true, true, "draft"))], false)
                .await;
        });
    }

    #[test]
    fn candidate_mergeable_insert_can_use_accepted_evidence_or_independent_or_seed() {
        crate::db::block_on(async {
            let fixture = Fixture::new(schema(policy(parent(true, false))), author("alice")).await;
            let seed = fixture
                .alice
                .upsert(
                    "nodes",
                    row(10),
                    cells(10, true, true, "room"),
                    Default::default(),
                )
                .await
                .unwrap();
            assert_eq!(fixture.settle(seed.mergeable_tx_id()).await, Fate::Accepted);
            let child = fixture
                .alice
                .upsert(
                    "nodes",
                    row(1),
                    cells(10, false, true, "room"),
                    Default::default(),
                )
                .await
                .unwrap();
            assert_eq!(
                fixture.settle(child.mergeable_tx_id()).await,
                Fate::Accepted
            );
            fixture
                .assert_rows(
                    &[
                        (1, cells(10, false, true, "room")),
                        (10, cells(10, true, true, "room")),
                    ],
                    true,
                )
                .await;
        });
    }

    #[test]
    fn candidate_mergeable_unit_cannot_publish_created_witnesses() {
        crate::db::block_on(async {
            use crate::db::MergeableTxOps;
            let fixture = Fixture::new(schema(policy(parent(true, false))), author("alice")).await;
            let open = crate::db::OpenTransactionId::new();
            fixture.alice.begin_mergeable(open).await.unwrap();
            let tx = fixture.alice.mergeable_tx_ref(open);
            let rows = [
                (1, cells(2, false, true, "room")),
                (2, cells(2, true, true, "room")),
            ];
            for (id, cells) in &rows {
                tx.upsert("nodes", row(*id), cells.clone(), Default::default())
                    .await
                    .unwrap();
            }
            let committed = fixture.alice.commit_mergeable_handle(open).await.unwrap();
            assert_eq!(
                fixture.settle(committed).await,
                Fate::Rejected(RejectionReason::AuthorizationDenied)
            );
            fixture.assert_rows(&rows, false).await;
        });
    }

    #[test]
    fn candidate_proof_cannot_use_unrelated_authority_local_pending_data() {
        crate::db::block_on(async {
            let fixture = Fixture::new(schema(policy(parent(true, false))), author("alice")).await;
            let unrelated = fixture
                .authority
                .upsert(
                    "nodes",
                    row(10),
                    cells(10, true, true, "room"),
                    Default::default(),
                )
                .await
                .unwrap();
            assert_eq!(
                fixture
                    .authority
                    .write_state(unrelated.mergeable_tx_id())
                    .unwrap()
                    .fate,
                Fate::Pending
            );
            fixture
                .check(&[(1, cells(10, false, true, "room"))], false)
                .await;
            assert_eq!(
                fixture
                    .authority
                    .write_state(unrelated.mergeable_tx_id())
                    .unwrap()
                    .fate,
                Fate::Pending
            );
        });
    }

    #[test]
    fn candidate_reverse_reference_postings_authorize_only_matching_parent() {
        crate::db::block_on(async {
            let reverse = PublicPolicyExpr::exists_including_created(
                "nodes",
                PublicPolicyExpr::and(vec![outer("parent_id", "id"), outer("scope", "scope")]),
            );
            let fixture = Fixture::new(schema(policy(reverse)), author("alice")).await;
            fixture
                .check(
                    &[
                        (1, cells(1, false, true, "room")),
                        (2, cells(1, true, true, "room")),
                    ],
                    true,
                )
                .await;
        });
    }

    fn mixed_schema(required_local_witness: bool) -> PublicSchema {
        let local_support = PublicPolicyExpr::Exists {
            table: "support".to_owned(),
            condition: Box::new(outer("id", "support_id")),
        };
        let target = if required_local_witness {
            "unmarked"
        } else {
            "parents"
        };
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("support")
                    .column("name", PublicColumnType::Text)
                    .policies(
                        PublicTablePolicies::new()
                            .with_select(PublicPolicyExpr::True)
                            .with_insert(PublicPolicyExpr::True),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("unmarked")
                    .fk_column("support_id", "support")
                    .policies(
                        PublicTablePolicies::new()
                            .with_select(PublicPolicyExpr::True)
                            .with_insert(local_support),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("parents")
                    .column("name", PublicColumnType::Text)
                    .policies(
                        PublicTablePolicies::new()
                            .with_select(PublicPolicyExpr::True)
                            .with_insert(PublicPolicyExpr::True),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("children")
                    .fk_column("parent_id", target)
                    .policies(
                        PublicTablePolicies::new()
                            .with_select(PublicPolicyExpr::True)
                            .with_insert(PublicPolicyExpr::exists_including_created(
                                target,
                                outer("id", "parent_id"),
                            )),
                    ),
            )
            .build()
    }

    async fn mixed_local_admission_case(required_local_witness: bool) {
        let fixture = Fixture::new(mixed_schema(required_local_witness), author("alice")).await;
        // This row is Local, not accepted Global proof evidence.
        let support = fixture
            .authority
            .upsert(
                "support",
                row(10),
                BTreeMap::from([("name".to_owned(), Value::String("pending".to_owned()))]),
                Default::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            fixture
                .authority
                .write_state(support.mergeable_tx_id())
                .unwrap()
                .fate,
            Fate::Pending
        );
        let open = crate::db::OpenTransactionId::new();
        fixture.alice.begin_exclusive(open).await.unwrap();
        let tx = fixture.alice.exclusive_tx_ref(open);
        tx.upsert(
            "unmarked",
            row(1),
            BTreeMap::from([("support_id".to_owned(), Value::Uuid(row(10).0))]),
            Default::default(),
        )
        .await
        .unwrap();
        tx.upsert(
            "parents",
            row(2),
            BTreeMap::from([("name".to_owned(), Value::String("independent".to_owned()))]),
            Default::default(),
        )
        .await
        .unwrap();
        tx.upsert(
            "children",
            row(3),
            BTreeMap::from([(
                "parent_id".to_owned(),
                Value::Uuid(row(if required_local_witness { 1 } else { 2 }).0),
            )]),
            Default::default(),
        )
        .await
        .unwrap();
        let committed = fixture.alice.commit_exclusive_handle(open).await.unwrap();
        assert_eq!(
            fixture.settle(committed).await,
            Fate::Rejected(RejectionReason::AuthorizationDenied),
        );
        for (table, id) in [("unmarked", 1), ("parents", 2), ("children", 3)] {
            for db in [&fixture.alice, &fixture.authority] {
                assert!(
                    db.local_current_row(table, row(id))
                        .await
                        .unwrap()
                        .is_none(),
                    "unaccepted policy evidence must reject the whole unit"
                );
            }
        }
        assert_eq!(
            fixture
                .authority
                .write_state(support.mergeable_tx_id())
                .unwrap()
                .fate,
            Fate::Pending
        );
    }

    #[test]
    fn candidate_mixed_unit_requires_accepted_evidence_for_unrelated_writes() {
        crate::db::block_on(async {
            mixed_local_admission_case(false).await;
        });
    }

    #[test]
    fn candidate_mixed_unit_cannot_promote_local_admission_to_global_evidence() {
        crate::db::block_on(async {
            mixed_local_admission_case(true).await;
        });
    }

    /// Alice cannot keep a marked INSERT grant that her same transaction revokes.
    /// alice -> authority: support admin -> viewer + marked child -> whole unit denied
    #[test]
    fn candidate_marked_insert_cannot_keep_a_same_unit_demoted_grant() {
        crate::db::block_on(async {
            let public_schema = PublicSchemaBuilder::new()
                .table(
                    PublicTableSchemaBuilder::new("support")
                        .column("role", PublicColumnType::Text)
                        .policies(
                            PublicTablePolicies::new()
                                .with_select(PublicPolicyExpr::True)
                                .with_insert(PublicPolicyExpr::True)
                                .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True),
                        ),
                )
                .table(
                    PublicTableSchemaBuilder::new("nodes")
                        .fk_column("parent_id", "support")
                        .column("scope", PublicColumnType::Text)
                        .column("seed", PublicColumnType::Boolean)
                        .column("allowed", PublicColumnType::Boolean)
                        .policies(
                            PublicTablePolicies::new()
                                .with_select(PublicPolicyExpr::True)
                                .with_insert(PublicPolicyExpr::exists_including_created(
                                    "support",
                                    PublicPolicyExpr::and(vec![
                                        outer("id", "parent_id"),
                                        PublicPolicyExpr::eq_literal(
                                            "role",
                                            PublicValue::Text("admin".to_owned()),
                                        ),
                                    ]),
                                )),
                        ),
                )
                .build();
            let fixture = Fixture::new(public_schema, author("alice")).await;
            let support = fixture
                .alice
                .upsert(
                    "support",
                    row(10),
                    BTreeMap::from([("role".to_owned(), Value::String("admin".to_owned()))]),
                    Default::default(),
                )
                .await
                .unwrap();
            assert_eq!(
                fixture.settle(support.mergeable_tx_id()).await,
                Fate::Accepted
            );
            let open = crate::db::OpenTransactionId::new();
            fixture.alice.begin_exclusive(open).await.unwrap();
            let tx = fixture.alice.exclusive_tx_ref(open);
            tx.upsert(
                "support",
                row(10),
                BTreeMap::from([("role".to_owned(), Value::String("viewer".to_owned()))]),
                Default::default(),
            )
            .await
            .unwrap();
            tx.upsert(
                "nodes",
                row(1),
                cells(10, false, true, "new"),
                Default::default(),
            )
            .await
            .unwrap();
            let committed = fixture.alice.commit_exclusive_handle(open).await.unwrap();
            assert_eq!(
                fixture.settle(committed).await,
                Fate::Rejected(RejectionReason::AuthorizationDenied),
            );
            for db in [&fixture.alice, &fixture.authority] {
                assert!(
                    db.local_current_row("nodes", row(1))
                        .await
                        .unwrap()
                        .is_none()
                );
                let support = db
                    .local_current_row("support", row(10))
                    .await
                    .unwrap()
                    .unwrap();
                let schema = db
                    .catalogue_schema(db.current_write_schema().unwrap().schema)
                    .unwrap();
                let table = schema
                    .tables()
                    .iter()
                    .find(|table| table.name == "support")
                    .unwrap();
                assert_eq!(
                    support.cell(table, "role"),
                    Some(Value::String("admin".to_owned()))
                );
            }
        });
    }

    #[test]
    fn candidate_unselected_insert_marker_keeps_update_evidence_accepted_only() {
        crate::db::block_on(async {
            let support_condition = outer("id", "parent_id");
            let public_schema = PublicSchemaBuilder::new()
                .table(
                    PublicTableSchemaBuilder::new("support")
                        .column("name", PublicColumnType::Text)
                        .policies(
                            PublicTablePolicies::new()
                                .with_select(PublicPolicyExpr::True)
                                .with_insert(PublicPolicyExpr::True),
                        ),
                )
                .table(
                    PublicTableSchemaBuilder::new("nodes")
                        .fk_column("parent_id", "support")
                        .column("scope", PublicColumnType::Text)
                        .column("seed", PublicColumnType::Boolean)
                        .column("allowed", PublicColumnType::Boolean)
                        .policies(
                            PublicTablePolicies::new()
                                .with_select(PublicPolicyExpr::True)
                                .with_insert(PublicPolicyExpr::or(vec![
                                    PublicPolicyExpr::eq_literal(
                                        "seed",
                                        PublicValue::Boolean(true),
                                    ),
                                    PublicPolicyExpr::exists_including_created(
                                        "support",
                                        support_condition.clone(),
                                    ),
                                ]))
                                .with_update(
                                    Some(PublicPolicyExpr::Exists {
                                        table: "support".to_owned(),
                                        condition: Box::new(support_condition),
                                    }),
                                    PublicPolicyExpr::True,
                                ),
                        ),
                )
                .build();
            let fixture = Fixture::new(public_schema, author("alice")).await;
            fixture
                .check(&[(1, cells(10, true, true, "old"))], true)
                .await;
            let support = fixture
                .authority
                .upsert(
                    "support",
                    row(10),
                    BTreeMap::from([("name".to_owned(), Value::String("pending".to_owned()))]),
                    Default::default(),
                )
                .await
                .unwrap();
            assert_eq!(
                fixture
                    .authority
                    .write_state(support.mergeable_tx_id())
                    .unwrap()
                    .fate,
                Fate::Pending
            );
            let update = fixture
                .alice
                .upsert(
                    "nodes",
                    row(1),
                    cells(10, false, true, "updated"),
                    Default::default(),
                )
                .await
                .unwrap();
            assert_eq!(
                fixture.settle(update.mergeable_tx_id()).await,
                Fate::Rejected(RejectionReason::AuthorizationDenied),
            );
            fixture
                .assert_rows(&[(1, cells(10, true, true, "old"))], true)
                .await;
            assert_eq!(
                fixture
                    .authority
                    .write_state(support.mergeable_tx_id())
                    .unwrap()
                    .fate,
                Fate::Pending
            );
        });
    }
}
