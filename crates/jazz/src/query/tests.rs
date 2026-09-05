#[cfg(test)]
mod tests {
    use super::*;
    use groove::records::{EnumCase, EnumSchema, RecordDescriptor, ValueType};
    use groove::schema::{ColumnSchema, ColumnType};
    use crate::tools::public_schema::{
        ColumnType as PublicColumnType, SchemaBuilder as PublicSchemaBuilder,
        TableSchemaBuilder as PublicTableSchemaBuilder,
    };

    fn schema() -> RuntimeSchema {
        RuntimeSchema::new([
            TableSchema::new(
                "issues",
                [
                    ColumnSchema::new("title", ColumnType::String),
                    ColumnSchema::new("state", ColumnType::String),
                    ColumnSchema::new("assignee", ColumnType::Uuid),
                    ColumnSchema::new("project", ColumnType::Uuid),
                    ColumnSchema::new("priority", ColumnType::U64),
                    ColumnSchema::new("labels", ColumnType::String.array_of()),
                    ColumnSchema::new("snoozed_until", ColumnType::U64.nullable()),
                ],
            )
            .with_reference("assignee", "users")
            .with_reference("project", "projects"),
            TableSchema::new(
                "issue_tags",
                [
                    ColumnSchema::new("issue", ColumnType::Uuid),
                    ColumnSchema::new("tag", ColumnType::Uuid),
                ],
            )
            .with_reference("issue", "issues")
            .with_reference("tag", "tags"),
            TableSchema::new(
                "projects",
                [
                    ColumnSchema::new("name", ColumnType::String),
                    ColumnSchema::new("org", ColumnType::Uuid),
                ],
            )
            .with_reference("org", "orgs"),
            TableSchema::new("orgs", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new("tags", [ColumnSchema::new("name", ColumnType::String)]),
        ])
    }

    #[test]
    fn builder_validate_and_canonicalize_round_trip() {
        let query = Query::from("issues")
            .filter(eq(col("assignee"), param("user")))
            .filter(ne(col("state"), lit("done")))
            .join_via("issue_tags", "issue", [eq(col("tag"), param("tag"))])
            .include("project.org");
        let validated = query.validate_runtime(&schema()).unwrap();
        assert_eq!(validated.query().table, "issues");
        assert_eq!(validated.params().len(), 2);
        assert_eq!(validated.params()["user"], ColumnType::Uuid);
        assert_eq!(validated.params()["tag"], ColumnType::Uuid);
        assert!(!validated.canonical_bytes().is_empty());
    }

    #[test]
    fn integer_literals_normalize_to_comparison_column_width() {
        let schema = RuntimeSchema::new([TableSchema::new(
            "metrics",
            [
                ColumnSchema::new("narrow", ColumnType::I32),
                ColumnSchema::new("wide", ColumnType::I64),
            ],
        )]);

        let widened = Query::from("metrics")
            .filter(gt(col("wide"), lit(9)))
            .validate_runtime(&schema)
            .expect("i32 literal widens for an i64 column");
        let explicitly_wide = Query::from("metrics")
            .filter(gt(col("wide"), lit(9_i64)))
            .validate_runtime(&schema)
            .unwrap();
        assert_eq!(
            widened.query().filters,
            vec![gt(col("wide"), lit(9_i64))]
        );
        assert_eq!(widened.shape_id(), explicitly_wide.shape_id());

        let narrowed = Query::from("metrics")
            .filter(eq(col("narrow"), lit(9_i64)))
            .validate_runtime(&schema)
            .expect("in-range i64 literal narrows for an i32 column");
        assert_eq!(
            narrowed.query().filters,
            vec![eq(col("narrow"), lit(9))]
        );

        let inferred_in = Query::from("metrics")
            .filter(in_list(col("wide"), [lit(9), lit(10_i64)]))
            .validate_runtime(&schema)
            .expect("IN candidates normalize to the column width");
        let explicit_in = Query::from("metrics")
            .filter(in_list(col("wide"), [lit(9_i64), lit(10_i64)]))
            .validate_runtime(&schema)
            .unwrap();
        assert_eq!(inferred_in.shape_id(), explicit_in.shape_id());

        let out_of_range = Query::from("metrics")
            .filter(eq(
                col("narrow"),
                lit(i64::from(i32::MAX) + 1),
            ))
            .validate_runtime(&schema)
            .unwrap_err();
        assert_eq!(out_of_range, QueryError::OperandTypeMismatch);

        let column_width_mismatch = Query::from("metrics")
            .filter(eq(col("narrow"), col("wide")))
            .validate_runtime(&schema)
            .unwrap_err();
        assert_eq!(column_width_mismatch, QueryError::OperandTypeMismatch);
    }

    #[test]
    fn relation_predicate_or_true_is_always_true() {
        let predicate = RelationPredicate::Or(vec![
            RelationPredicate::True,
            RelationPredicate::Cmp {
                left: RelationColumnRef {
                    scope: Some("issues".to_owned()),
                    column: "state".to_owned(),
                },
                op: RelationCmpOp::Eq,
                right: RelationValueRef::Literal(serde_json::Value::String("open".to_owned())),
            },
        ]);

        assert_eq!(
            relation_predicate_to_query_predicate(&predicate).unwrap(),
            None
        );
    }

    #[test]
    fn relation_predicate_not_true_is_always_false() {
        let predicate = RelationPredicate::Not(Box::new(RelationPredicate::True));

        assert_eq!(
            relation_predicate_to_query_predicate(&predicate).unwrap(),
            Some((String::new(), Predicate::Any(Vec::new())))
        );
    }

    // This focused lowerer test is intentional: relation IR is the NAPI/WASM
    // boundary, while end-to-end matching and subscription deltas are covered
    // at the Groove runtime boundary below it.
    #[test]
    fn relation_payload_enum_match_lowers_and_validates_against_case_fields() {
        let event_payload = RecordDescriptor::new([("level", ValueType::I32)]);
        let schema = RuntimeSchema::new([TableSchema::new(
            "events",
            [ColumnSchema::new(
                "event",
                ColumnType::Enum(Box::new(
                    EnumSchema::new("event", [EnumCase::new("message", event_payload)]).unwrap(),
                )),
            )],
        )]);
        let relation = RelationQuery {
            rel: RelationExpr::Project {
                input: Box::new(RelationExpr::Filter {
                    input: Box::new(RelationExpr::TableScan {
                        table: "events".to_owned(),
                        alias: None,
                    }),
                    predicate: RelationPredicate::EnumMatch {
                        column: RelationColumnRef {
                            scope: Some("events".to_owned()),
                            column: "event".to_owned(),
                        },
                        case: "message".to_owned(),
                        payload: Box::new(RelationPredicate::Cmp {
                            left: RelationColumnRef {
                                scope: None,
                                column: "level".to_owned(),
                            },
                            op: RelationCmpOp::Eq,
                            right: RelationValueRef::Literal(serde_json::json!({
                                "type": "Integer",
                                "value": 2,
                            })),
                        }),
                    },
                }),
                columns: vec![RelationProjectColumn {
                    alias: "event".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("events".to_owned()),
                        column: "event".to_owned(),
                    }),
                }],
            },
        };

        let query = relation_query_to_query(&relation).unwrap();
        assert_eq!(
            query.filters,
            vec![Predicate::EnumMatch {
                column: "event".to_owned(),
                case: "message".to_owned(),
                payload: Box::new(Predicate::Eq(
                    Operand::Column("level".to_owned()),
                    Operand::Literal(Value::I32(2)),
                )),
            }]
        );
        assert!(query.validate_runtime(&schema).is_ok());
    }

    #[test]
    fn enum_match_validation_uses_selected_case_fields_not_outer_table_fields() {
        let payload =
            RecordDescriptor::new([("case_only", ValueType::String), ("shared", ValueType::I32)]);
        let schema = RuntimeSchema::new([TableSchema::new(
            "events",
            [
                ColumnSchema::new("shared", ColumnType::String),
                ColumnSchema::new("outer_only", ColumnType::String),
                ColumnSchema::new(
                    "event",
                    ColumnType::Enum(Box::new(
                        EnumSchema::new("event", [EnumCase::new("message", payload)]).unwrap(),
                    )),
                ),
            ],
        )]);
        let matched = |payload| Predicate::EnumMatch {
            column: "event".to_owned(),
            case: "message".to_owned(),
            payload: Box::new(payload),
        };

        assert!(
            Query::from("events")
                .filter(matched(Predicate::Eq(
                    Operand::Column("case_only".to_owned()),
                    Operand::Literal(Value::String("present only in the case".to_owned())),
                )))
                .validate_runtime(&schema)
                .is_ok()
        );
        assert!(
            Query::from("events")
                .filter(matched(Predicate::Eq(
                    Operand::Column("outer_only".to_owned()),
                    Operand::Literal(Value::String("outer".to_owned())),
                )))
                .validate_runtime(&schema)
                .is_err()
        );
        assert!(
            Query::from("events")
                .filter(matched(Predicate::Eq(
                    Operand::Column("shared".to_owned()),
                    Operand::Literal(Value::String("outer type".to_owned())),
                )))
                .validate_runtime(&schema)
                .is_err()
        );
    }

    #[test]
    fn contains_param_array_against_column_infers_array_type() {
        let validated = Query::from("issues")
            .filter(contains(param("teams"), col("assignee")))
            .validate_runtime(&schema())
            .unwrap();

        assert_eq!(
            validated.params()["teams"],
            ColumnType::Array(Box::new(ColumnType::Uuid))
        );
    }

    #[test]
    fn validates_same_table_reachability_correlation_column() {
        let schema = RuntimeSchema::new([
            TableSchema::new("resources", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new(
                "access_edges",
                [
                    ColumnSchema::new("resource_id", ColumnType::Uuid),
                    ColumnSchema::new("team_id", ColumnType::Uuid),
                    ColumnSchema::new("administrator", ColumnType::Bool),
                ],
            )
            .with_reference("resource_id", "resources")
            .with_reference("team_id", "teams"),
            TableSchema::new(
                "team_entry",
                [
                    ColumnSchema::new("team_id", ColumnType::Uuid),
                    ColumnSchema::new("target_id", ColumnType::Uuid),
                ],
            )
            .with_reference("team_id", "teams")
            .with_reference("target_id", "teams"),
            TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)]),
        ]);

        Query::from("access_edges")
            .reachable_via_with_access_filters(
                "access_edges",
                "resource_id",
                "team_id",
                claim("user_id"),
                [eq(col("administrator"), lit(false))],
                "team_entry",
                "team_id",
                "target_id",
                [],
            )
            .validate_runtime(&schema)
            .unwrap();
    }

    #[test]
    fn filter_order_does_not_change_shape_id() {
        let schema = schema();
        let left = Query::from("issues")
            .filter(eq(col("assignee"), param("user")))
            .filter(ne(col("state"), lit("done")))
            .validate_runtime(&schema)
            .unwrap();
        let right = Query::from("issues")
            .filter(ne(lit("done"), col("state")))
            .filter(eq(param("user"), col("assignee")))
            .validate_runtime(&schema)
            .unwrap();
        assert_eq!(left.shape_id(), right.shape_id());
    }

    #[test]
    fn validates_boolean_operators_projection_includes_and_pagination() {
        let query = Query::from("issues")
            .filter(all_of([
                any_of([
                    eq(col("state"), lit("open")),
                    eq(col("state"), lit("blocked")),
                ]),
                in_list(col("state"), [lit("open"), lit("blocked")]),
                not(ne(col("assignee"), param("user"))),
                gt(col("priority"), lit(1_u64)),
                gte(col("priority"), lit(2_u64)),
                lt(col("priority"), lit(10_u64)),
                lte(col("priority"), lit(9_u64)),
                gt(col("title"), lit("bug")),
                gte(col("title"), lit("bug")),
                lt(col("title"), lit("z")),
                lte(col("title"), lit("z")),
                contains(col("title"), lit("api")),
                contains(col("labels"), lit("backend")),
                is_null(col("snoozed_until")),
            ]))
            .include_with(Include::new("project.org").join_mode(JoinMode::Holes))
            .select(["title", "state", "$createdAt"])
            .offset(5)
            .limit(10);

        let validated = query.validate_runtime(&schema()).unwrap();
        assert_eq!(validated.params()["user"], ColumnType::Uuid);
        assert_eq!(validated.query().offset, 5);
        assert_eq!(validated.query().limit, Some(10));
        assert_eq!(
            validated.query().select.as_deref(),
            Some(
                [
                    "$createdAt".to_owned(),
                    "state".to_owned(),
                    "title".to_owned()
                ]
                .as_slice()
            )
        );
        assert_eq!(validated.query().includes[0].join_mode, JoinMode::Holes);
    }

    #[test]
    fn validates_array_subquery_shape_without_execution() {
        // Internal test: array-subquery execution is not implemented yet, but
        // shape validation/canonical identity are query-module responsibilities.
        let validated = Query::from("issues")
            .array_subquery(
                ArraySubquery::new("tags", "issue_tags", "issue", "id")
                    .filter(eq(col("tag"), param("tag")))
                    .select(["tag"])
                    .order_by("tag", OrderDirection::Asc)
                    .limit(5)
                    .requirement(ArraySubqueryRequirement::AtLeastOne)
                    .nested(
                        ArraySubquery::new("tagRows", "tags", "id", "tag")
                            .select(["name"])
                            .limit(5),
                    ),
            )
            .validate_runtime(&schema())
            .unwrap();

        assert_eq!(validated.params()["tag"], ColumnType::Uuid);
        let subquery = &validated.query().array_subqueries[0];
        assert_eq!(subquery.column_name, "tags");
        assert_eq!(subquery.nested_arrays[0].column_name, "tagRows");
        assert_eq!(
            subquery.select.as_deref(),
            Some(["tag".to_owned()].as_slice())
        );
    }

    #[test]
    fn array_subquery_order_does_not_change_shape_id() {
        // Internal test: canonicalization should be stable before execution is
        // exposed through black-box relation payload tests.
        let left = Query::from("issues")
            .array_subquery(
                ArraySubquery::new("tags", "issue_tags", "issue", "id")
                    .filter(eq(col("tag"), param("tag")))
                    .filter(ne(col("issue"), param("issue")))
                    .limit(10),
            )
            .array_subquery(
                ArraySubquery::new("projectIssues", "issues", "project", "project").limit(10),
            )
            .validate_runtime(&schema())
            .unwrap();
        let right = Query::from("issues")
            .array_subquery(
                ArraySubquery::new("projectIssues", "issues", "project", "project").limit(10),
            )
            .array_subquery(
                ArraySubquery::new("tags", "issue_tags", "issue", "id")
                    .filter(ne(col("issue"), param("issue")))
                    .filter(eq(col("tag"), param("tag")))
                    .limit(10),
            )
            .validate_runtime(&schema())
            .unwrap();

        assert_eq!(left.shape_id(), right.shape_id());
    }

    #[test]
    fn flat_join_rejects_combinations_outside_its_executable_envelope() {
        fn flat() -> Query {
            Query::from("issues").flat_join("issue_tags", "issues.id", "issue_tags.issue")
        }

        let mut cases = Vec::new();
        let mut query = flat();
        query.flat_join.as_mut().unwrap().sources.clear();
        cases.push(query);
        let mut query = flat();
        query.select = Some(vec!["title".to_owned()]);
        cases.push(query);
        let mut query = flat();
        query.order_by.push(OrderBy {
            column: "title".to_owned(),
            direction: OrderDirection::Asc,
        });
        cases.push(query);
        let mut query = flat();
        query.limit = Some(1);
        cases.push(query);
        let mut query = flat();
        query.offset = 1;
        cases.push(query);
        let mut query = flat();
        query
            .array_subqueries
            .push(ArraySubquery::new("tags", "issue_tags", "issue", "id"));
        cases.push(query);
        cases.push(flat().join_via("issue_tags", "issue", []));
        cases.push(flat().aggregate([Aggregate::count()]));
        cases.push(flat().include("project"));
        cases.push(flat().inherits("project"));
        let mut query = flat();
        query.policy_branches.push(PolicyBranch {
            filters: Vec::new(),
            joins: Vec::new(),
            reachable: Vec::new(),
            inherits: Vec::new(),
        });
        cases.push(query);

        for query in cases {
            assert!(matches!(
                query.validate_runtime(&schema()),
                Err(QueryError::UnsupportedFlatJoinCombination { .. })
            ));
        }
    }

    #[test]
    fn flat_join_allows_nullable_scalar_keys() {
        let schema = RuntimeSchema::new([
            TableSchema::new(
                "parents",
                [ColumnSchema::new(
                    "child_id",
                    ColumnType::Uuid.nullable(),
                )],
            ),
            TableSchema::new(
                "children",
                [ColumnSchema::new(
                    "parent_id",
                    ColumnType::Uuid.nullable(),
                )],
            ),
        ]);

        for (left, right) in [
            ("parents.child_id", "children.id"),
            ("parents.id", "children.parent_id"),
        ] {
            let mut query = Query::from("parents");
            query.flat_join = Some(FlatJoin {
                root_alias: None,
                sources: vec![FlatJoinSource {
                    table: "children".to_owned(),
                    alias: None,
                    on: FlatJoinOn {
                        left: left.to_owned(),
                        right: right.to_owned(),
                    },
                }],
            });
            query.validate_runtime(&schema).unwrap();
        }
    }

    /// Flat-join filters preserve declared `id` field types on every source,
    /// while `_id` remains the UUID spelling for the physical row identity.
    #[test]
    fn flat_join_filters_distinguish_declared_id_from_physical_id() {
        let source = PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("parents").column("id", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("children")
                    .column("id", PublicColumnType::Text)
                    .fk_column("parent", "parents"),
            )
            .build();
        let schema = JazzSchema::new(&source).expect("flat-join public schema compiles");

        let query = Query::from(table("parents").alias("parent"))
            .flat_join(table("children").alias("child"), "parent._id", "child.parent")
            .filter(eq(col("parent.id"), lit("parent-key")))
            .filter(eq(col("child.id"), lit("child-key")))
            .filter(eq(
                col("child._id"),
                lit(uuid::Uuid::from_u128(0x1234)),
            ));

        assert!(query.validate(&schema).is_ok());
    }

    #[test]
    fn flat_join_allows_array_element_keys() {
        let schema = RuntimeSchema::new([
            TableSchema::new(
                "parents",
                [ColumnSchema::new(
                    "child_ids",
                    ColumnType::Uuid.array_of(),
                )],
            ),
            TableSchema::new(
                "children",
                [ColumnSchema::new(
                    "parent_ids",
                    ColumnType::Uuid.array_of(),
                )],
            ),
        ]);

        for (left, right) in [
            ("parents.child_ids", "children.id"),
            ("parents.id", "children.parent_ids"),
        ] {
            let mut query = Query::from("parents");
            query.flat_join = Some(FlatJoin {
                root_alias: None,
                sources: vec![FlatJoinSource {
                    table: "children".to_owned(),
                    alias: None,
                    on: FlatJoinOn {
                        left: left.to_owned(),
                        right: right.to_owned(),
                    },
                }],
            });
            query.validate_runtime(&schema).unwrap();
        }
    }

    #[test]
    fn rejects_invalid_array_subquery_shapes() {
        let schema = schema();

        let err = Query::from("issues")
            .array_subquery(ArraySubquery::new("bad", "missing", "issue", "id"))
            .validate_runtime(&schema)
            .unwrap_err();
        assert!(matches!(err, QueryError::UnknownTable(_)));

        let err = Query::from("issues")
            .array_subquery(ArraySubquery::new("bad", "issue_tags", "missing", "id"))
            .validate_runtime(&schema)
            .unwrap_err();
        assert!(matches!(err, QueryError::UnknownColumn { .. }));

        let err = Query::from("issues")
            .array_subquery(ArraySubquery::new("bad", "issue_tags", "issue", "title"))
            .validate_runtime(&schema)
            .unwrap_err();
        assert_eq!(err, QueryError::OperandTypeMismatch);

        let err = Query::from("issues")
            .array_subquery(ArraySubquery::new("dupe", "issue_tags", "issue", "id"))
            .array_subquery(ArraySubquery::new("dupe", "issues", "id", "id"))
            .validate_runtime(&schema)
            .unwrap_err();
        assert!(matches!(err, QueryError::BadIncludePath { .. }));
    }

    #[test]
    fn array_subqueries_are_unbounded_by_default_and_include_offset_in_identity() {
        let schema = schema();
        let unbounded = Query::from("issues").array_subquery(ArraySubquery::new(
            "tags",
            "issue_tags",
            "issue",
            "id",
        ));
        assert_eq!(
            unbounded
                .validate_runtime(&schema)
                .unwrap()
                .query()
                .array_subqueries[0]
                .limit,
            None
        );

        let nested_unbounded = Query::from("issues").array_subquery(
            ArraySubquery::new("tags", "issue_tags", "issue", "id")
                .limit(1)
                .nested(ArraySubquery::new("tagRows", "tags", "id", "tag")),
        );
        nested_unbounded.validate_runtime(&schema).unwrap();

        let zero = Query::from("issues").array_subquery(
            ArraySubquery::new("tags", "issue_tags", "issue", "id")
                .offset(2)
                .limit(0),
        );
        let one = Query::from("issues").array_subquery(
            ArraySubquery::new("tags", "issue_tags", "issue", "id")
                .offset(3)
                .limit(0),
        );
        let zero = zero.validate_runtime(&schema).unwrap();
        assert_eq!(zero.query().array_subqueries[0].offset, 2);
        assert_ne!(zero.shape_id(), one.validate_runtime(&schema).unwrap().shape_id());

        Query::from("issues")
            .array_subquery(ArraySubquery::new("tags", "issue_tags", "issue", "id").offset(2))
            .validate_runtime(&schema)
            .unwrap();
    }

    #[test]
    fn validates_order_by_columns_and_preserves_key_order() {
        let err = Query::from("issues")
            .order_by("$createdBy", OrderDirection::Asc)
            .validate_runtime(&schema())
            .unwrap_err();
        assert_eq!(
            err,
            QueryError::UnsupportedAuthorOrdering {
                column: "$createdBy".to_owned()
            }
        );

        let validated = Query::from("issues")
            .order_by("state", OrderDirection::Asc)
            .order_by("priority", OrderDirection::Desc)
            .validate_runtime(&schema())
            .unwrap();
        assert_eq!(
            validated.query().order_by,
            vec![
                OrderBy {
                    column: "state".to_owned(),
                    direction: OrderDirection::Asc,
                },
                OrderBy {
                    column: "priority".to_owned(),
                    direction: OrderDirection::Desc,
                },
            ]
        );

        let err = Query::from("issues")
            .order_by("missing", OrderDirection::Asc)
            .validate_runtime(&schema())
            .unwrap_err();
        assert!(matches!(err, QueryError::UnknownColumn { .. }));
    }

    #[test]
    fn validates_aggregate_columns_types_grouping_and_ordering() {
        let validated = Query::from("issues")
            .aggregate([
                Aggregate::count(),
                Aggregate::sum("priority"),
                Aggregate::min("priority"),
                Aggregate::max("priority"),
            ])
            .group_by("state")
            .order_by("state", OrderDirection::Asc)
            .order_by("count", OrderDirection::Desc)
            .validate_runtime(&schema())
            .unwrap();
        let aggregate = validated.query().aggregate.as_ref().unwrap();
        assert_eq!(aggregate.group_by.as_deref(), Some("state"));
        assert_eq!(aggregate.aggregates.len(), 4);

        let err = Query::from("issues")
            .sum("title")
            .validate_runtime(&schema())
            .unwrap_err();
        assert_eq!(err, QueryError::OperandTypeMismatch);

        let err = Query::from("issues")
            .count()
            .group_by("missing")
            .validate_runtime(&schema())
            .unwrap_err();
        assert!(matches!(err, QueryError::UnknownColumn { .. }));

        let valid_user_alias = Query::from("issues")
            .aggregate([Aggregate::sum("priority").alias("user_total")])
            .validate_runtime(&schema())
            .expect("explicit aliases are logical names, not compiler fields");
        assert_eq!(
            valid_user_alias
                .query()
                .aggregate
                .as_ref()
                .unwrap()
                .aggregates[0]
                .alias,
            "user_total"
        );
        let err = Query::from("issues")
            .aggregate([Aggregate::sum("priority").alias("__jazz_aggregate_user_total")])
            .validate_runtime(&schema())
            .unwrap_err();
        assert!(matches!(err, QueryError::ReservedAggregateAlias(_)));

        let err = Query::from("issues")
            .count()
            .order_by("priority", OrderDirection::Asc)
            .validate_runtime(&schema())
            .unwrap_err();
        assert!(matches!(err, QueryError::UnknownColumn { .. }));
    }

    #[test]
    fn semantic_difference_changes_shape_id() {
        let schema = schema();
        let left = Query::from("issues")
            .filter(eq(col("assignee"), param("user")))
            .validate_runtime(&schema)
            .unwrap();
        let right = Query::from("issues")
            .filter(ne(col("assignee"), param("user")))
            .validate_runtime(&schema)
            .unwrap();
        assert_ne!(left.shape_id(), right.shape_id());
    }

    #[test]
    fn schema_version_context_changes_shape_id() {
        let base = schema();
        let evolved = RuntimeSchema::new([
            TableSchema::new(
                "issues",
                [
                    ColumnSchema::new("title", ColumnType::String),
                    ColumnSchema::new("state", ColumnType::String),
                    ColumnSchema::new("assignee", ColumnType::Uuid),
                    ColumnSchema::new("body", ColumnType::String),
                ],
            ),
            TableSchema::new(
                "issue_tags",
                [
                    ColumnSchema::new("issue", ColumnType::Uuid),
                    ColumnSchema::new("tag", ColumnType::Uuid),
                ],
            )
            .with_reference("issue", "issues")
            .with_reference("tag", "tags"),
            TableSchema::new("projects", [ColumnSchema::new("org", ColumnType::Uuid)])
                .with_reference("org", "orgs"),
            TableSchema::new("orgs", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new("tags", [ColumnSchema::new("name", ColumnType::String)]),
        ]);
        let query = Query::from("issues").filter(eq(col("assignee"), param("user")));
        let left = query.validate_runtime(&base).unwrap();
        let right = query.validate_runtime(&evolved).unwrap();

        assert_eq!(left.canonical_bytes(), right.canonical_bytes());
        assert_ne!(left.schema_version(), right.schema_version());
        assert_ne!(left.shape_id(), right.shape_id());
    }

    #[test]
    fn binding_type_mismatch_errors() {
        let validated = Query::from("issues")
            .filter(eq(col("assignee"), param("user")))
            .validate_runtime(&schema())
            .unwrap();
        let err = validated
            .bind(BTreeMap::from([(
                "user".to_owned(),
                Value::String("not-a-uuid".to_owned()),
            )]))
            .unwrap_err();
        assert!(matches!(err, QueryError::ParamTypeMismatch { .. }));
    }

    #[test]
    fn application_claims_are_not_statically_typed() {
        Query::from("issues")
            .filter(eq(col("state"), claim("sub")))
            .validate_runtime(&schema())
            .unwrap();
    }

    #[test]
    fn claim_column_matched_types_still_validate() {
        Query::from("issues")
            .filter(eq(col("assignee"), claim("sub")))
            .validate_runtime(&schema())
            .unwrap();

        Query::from("issues")
            .filter(eq(col("state"), claim("user_id")))
            .validate_runtime(&schema())
            .unwrap();
    }

    #[test]
    fn include_path_resolution_errors_on_bad_path() {
        let err = Query::from("issues")
            .include("project.missing")
            .validate_runtime(&schema())
            .unwrap_err();
        assert!(matches!(err, QueryError::UnknownColumn { .. }));
        let err = Query::from("issues")
            .include("title.name")
            .validate_runtime(&schema())
            .unwrap_err();
        assert!(matches!(err, QueryError::BadIncludePath { .. }));
    }

    #[test]
    fn binding_id_uses_canonical_binding_values() {
        let validated = Query::from("issues")
            .filter(eq(col("assignee"), param("user")))
            .validate_runtime(&schema())
            .unwrap();
        let user = uuid::uuid!("00000000-0000-0000-0000-000000000001");
        let binding = validated
            .bind(BTreeMap::from([("user".to_owned(), Value::Uuid(user))]))
            .unwrap();
        assert_eq!(
            binding.binding_id(),
            BindingId(uuid::Uuid::new_v5(
                &QUERY_NAMESPACE,
                binding.canonical_bytes()
            ))
        );
    }

    #[test]
    fn canonical_bytes_stability_golden() {
        let validated = Query::from("issues")
            .filter(eq(col("assignee"), param("user")))
            .filter(ne(col("state"), lit("done")))
            .join_via("issue_tags", "issue", [eq(col("tag"), param("tag"))])
            .include("project.org")
            .validate_runtime(&schema())
            .unwrap();
        // Branch-view selection is part of the canonical query shape, even
        // when it selects the shared/default branch-local row.
        assert_eq!(
            validated.shape_id().0.to_string(),
            "17acefa1-5bd0-53c6-8451-02ee5bad1a5d"
        );
    }
}
