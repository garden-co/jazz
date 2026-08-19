//! lowering query-evaluation tests.

use super::*;

#[test]
fn prepared_relation_terminal_keeps_branch_discriminator_in_public_payload() {
    // Prepared subscriptions wrap each production terminal in a routed
    // projection. `versioned_row_ref_fields` is the public payload list
    // supplied to that projection; omitting this field makes lowering look
    // correct while the decoder receives no branch witness.
    let versioned_ref = |prefix: &str| {
        let branch_field = format!("{prefix}_branch_or_prefix");
        VersionedRowRefSchema {
            row: super::super::query_engine::RowRefSchema {
                source_field: format!("{prefix}_source"),
                table_field: format!("{prefix}_table"),
                row_field: format!("{prefix}_row"),
            },
            version: Some(ResultMembershipVersionSchema::Content(
                super::super::query_engine::ContentVersionFields {
                    tx_time_field: format!("{prefix}_tx_time"),
                    tx_node_field: format!("{prefix}_tx_node"),
                },
            )),
            branch_or_prefix_field: Some(branch_field),
        }
    };
    let schema = super::super::query_engine::ProgramFactSchema::RelationEdges(
        super::super::query_engine::RelationEdgeSchema {
            source: versioned_ref("source"),
            path_field: "path".to_owned(),
            target: versioned_ref("target"),
            kind_field: "kind".to_owned(),
            depth_field: None,
            edge_id_field: None,
            branch_field: None,
            role_field: None,
            order_field: None,
            hole_state_field: None,
        },
    );
    let public_payload = fact_public_fields(&schema).expect("relation facts are routable");
    for prefix in ["source", "target"] {
        let branch_field = format!("{prefix}_branch_or_prefix");
        assert!(
            public_payload.contains(&branch_field),
            "prepared routed {prefix} payload must retain its branch discriminator"
        );
    }
}

#[test]
fn lowered_groove_graph_differs_for_distinct_read_views() {
    let schema = JazzSchema::new([TableSchema::new(
        "docs",
        [ColumnSchema::new("title", ColumnType::String)],
    )]);
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xa4; 16]), schema.clone());
    let shape = Query::from("docs").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let identity = AuthorId::SYSTEM;
    let branch_id = BranchId::from_bytes([0xbe; 16]);
    node.create_branch(branch_id).unwrap();

    let current_graph = lowered_current_app_rows_graph(
        &mut node,
        &shape,
        &binding,
        identity,
        &ReadViewSpec::default(),
    );
    let branch_graph = lowered_current_app_rows_graph(
        &mut node,
        &shape,
        &binding,
        identity,
        &ReadViewSpec {
            source: ReadViewSourceSpec::Branch {
                branch: branch_id.0,
            },
            ..ReadViewSpec::default()
        },
    );

    assert_ne!(
        current_graph, branch_graph,
        "read-view source must be encoded in the lowered Groove descriptor graph"
    );
}

#[test]
fn exclusive_join_shape_uses_shared_snapshot_lowering() {
    let schema = schema();
    let (_client_dir, mut client) =
        open_node_with_uuid(NodeUuid::from_bytes([1; 16]), schema.clone());
    let alice = author(1);
    client
        .commit_mergeable_settled(
            MergeableCommit::new("issues", row(1), 10).cells(BTreeMap::from([(
                "title".to_owned(),
                Value::String("issue".to_owned()),
            )])),
        )
        .unwrap();
    client
        .commit_mergeable_settled(MergeableCommit::new("issue_members", row(2), 11).cells(
            BTreeMap::from([
                ("issue".to_owned(), Value::Uuid(row(1).0)),
                ("user".to_owned(), Value::Uuid(alice.0)),
            ]),
        ))
        .unwrap();

    let shape = Query::from("issues")
        .join_via("issue_members", "issue", [eq(col("user"), param("user"))])
        .validate(&schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([("user".to_owned(), Value::Uuid(alice.0))]))
        .unwrap();

    let open = OpenTransactionId::new();
    client.open_exclusive(open).unwrap();
    let rows = client
        .tx_query(open, &shape, &binding)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(rows, BTreeSet::from([row(1)]));
}
