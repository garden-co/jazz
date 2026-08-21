//! read sources query-evaluation tests.

use super::*;

#[test]
fn branch_source_witness_discriminator_tracks_each_row_lineage() {
    let table = TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)]);
    let row = current_row_from_cells(
        &table,
        row(0xf3),
        &BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
    )
    .expect("build projected row");
    let metadata = BTreeMap::from([(
        SourceMetadataRequirement::VersionWitnesses,
        SourceMetadataFields::VersionWitnesses {
            schema_version_field: "schema_version".to_owned(),
            tx_time_field: "tx_time".to_owned(),
            tx_node_field: "tx_node_id".to_owned(),
            branch_or_prefix_field: Some("branch_id".to_owned()),
        },
    )]);
    let descriptor = current_row_descriptor_with_hidden_source_fields(&table, &metadata);
    let branch = BranchId::from_bytes([0xf4; 16]);
    let root_record = inline_branch_current_record(
        &table,
        &descriptor,
        &row,
        SchemaVersionAlias(1),
        (TxTime(1), NodeAlias(1)),
        None,
    )
    .expect("encode root/base row in branch view");
    let overlay_record = inline_branch_current_record(
        &table,
        &descriptor,
        &row,
        SchemaVersionAlias(1),
        (TxTime(2), NodeAlias(1)),
        Some(branch),
    )
    .expect("encode branch overlay row");
    let branch_idx = descriptor.field_index("branch_id").expect("branch field");
    assert!(matches!(
        BorrowedRecord::new(&root_record, &descriptor).get_idx(branch_idx),
        Ok(Value::Nullable(None))
    ));
    assert!(matches!(
        BorrowedRecord::new(&overlay_record, &descriptor).get_idx(branch_idx),
        Ok(Value::Nullable(Some(value))) if matches!(*value, Value::Uuid(id) if id == branch.0)
    ));
}

#[test]
fn reverse_table_lens_projects_membership_and_content_version_sources() {
    // This is intentionally an internal assertion: the public subscription
    // regression proves the observable row result, while this checks that
    // both inputs to its content-version semi-join select the same source.
    let base = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("users").column("email", PublicColumnType::Text)),
    );
    let evolved = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("people").column("email", PublicColumnType::Text)),
    );
    let evolved_payload = SchemaVersion::new(evolved);
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xa2; 16]), base.clone());
    node.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
        author: AuthorId::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(SchemaLineagePublication::new(
            evolved_payload.clone(),
            MigrationLens::new(
                base.version_id(),
                evolved_payload.id,
                vec![TableLens {
                    source_table: "users".to_owned(),
                    target_table: "people".to_owned(),
                    ops: vec![LensOp::RenameTable {
                        from: "users".to_owned(),
                        to: "people".to_owned(),
                    }],
                }],
            ),
            Vec::<String>::new(),
            Vec::<String>::new(),
        )),
    })
    .unwrap();
    node.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();

    let shape = Query::from("users").validate(&base).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let query_request = node
        .current_query_program_request(
            &shape,
            &binding,
            DurabilityTier::Global,
            AuthorId::SYSTEM,
            CurrentQueryProgramOutput::MaintainedView,
            &ReadViewSpec::default(),
            None,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();
    let read_view = query_request.reads.primary;
    let source_request = SourceRequest {
        source: root_source_id("users"),
        visibility: RowVisibility::Visible,
        authorization: SourceAuthorizationRequest::System,
        requirements: SourceRequirements {
            app_fields: FieldRequirement::All,
            metadata: BTreeSet::from([SourceMetadataRequirement::VersionPayloads]),
        },
    };
    let expected_people_current = physical_global_current_table_name(
        node.physical_table_id_for_schema(evolved_payload.id, "people")
            .unwrap(),
    );
    let mut resolver = CurrentQuerySourceResolver {
        node: &mut node,
        read_view: &read_view,
        prepare_branch_subscription_sources: false,
        inline_sources: BTreeMap::new(),
        access_paths: BTreeMap::new(),
        current_projection_targets: BTreeMap::new(),
    };

    assert!(resolver.needs_projected_current_source("users"));
    let resolved = resolver.resolve_source(&source_request).unwrap();
    assert_eq!(
        resolver.current_projection_targets.len(),
        1,
        "the Global source and its content-version sidecar share one cached projection target",
    );
    let content_version = resolved
        .content_version
        .expect("version-payload requirements need a content-version source");

    assert!(
        format!("{:?}", resolved.graph).contains(&expected_people_current),
        "membership source must include the shared physical current table"
    );
    assert!(
        format!("{:?}", content_version.graph).contains(&expected_people_current),
        "content-version source must include the shared physical current table"
    );
}

#[test]
fn historical_cut_bounded_source_matches_full_scan_graph() {
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("docs").column("title", PublicColumnType::Text)),
    );
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0x31; 16]), schema);
    let table = node.table("docs").expect("docs table").clone();
    let first = row(0x31);
    let second = row(0x32);
    commit_global_cells(
        &mut node,
        "docs",
        first,
        BTreeMap::from([("title".to_owned(), Value::String("first".to_owned()))]),
        1_000,
        1,
    );
    commit_global_cells(
        &mut node,
        "docs",
        second,
        BTreeMap::from([("title".to_owned(), Value::String("second".to_owned()))]),
        1_001,
        2,
    );
    let delete_tx = node
        .commit_mergeable(
            MergeableCommit::new("docs", first, 1_002).deletion(DeletionEvent::Deleted),
        )
        .expect("commit delete");
    node.apply_fate_update(
        delete_tx,
        Fate::Accepted,
        Some(GlobalTime(3)),
        Some(DurabilityTier::Global),
    )
    .expect("accept delete");
    // Keep an unrelated later write in the same table to ensure the full-scan
    // control has more history available than the bounded cut should read.
    commit_global_cells(
        &mut node,
        "docs",
        row(0x33),
        BTreeMap::from([("title".to_owned(), Value::String("later".to_owned()))]),
        1_003,
        4,
    );

    node.reset_query_engine_read_metrics();
    let shape = Query::from("docs")
        .validate(&node.catalogue.schema)
        .expect("shape");
    let binding = shape.bind(BTreeMap::new()).expect("binding");
    let bounded = current_titles(
        &table,
        node.query_rows_at(&shape, &binding, GlobalTime(2))
            .expect("bounded historical query"),
    );
    let selected_metrics = node.query_engine_read_metrics().clone();
    let full = historical_titles_via_full_scan(&mut node, &table, GlobalTime(2));

    assert_eq!(bounded, full);
    assert_eq!(selected_metrics.source_global_time_range_scans, 1);
    assert_eq!(selected_metrics.source_full_scans, 0);
}

#[test]
fn historical_cut_reads_only_table_global_time_range() {
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("docs").column("title", PublicColumnType::Text)),
    );
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0x32; 16]), schema);
    let table = node.table("docs").expect("docs table").clone();
    let shape = Query::from("docs")
        .validate(&node.catalogue.schema)
        .expect("shape");
    let binding = shape.bind(BTreeMap::new()).expect("binding");
    commit_global_cells(
        &mut node,
        "docs",
        row(0x41),
        BTreeMap::from([("title".to_owned(), Value::String("at-cut".to_owned()))]),
        1_000,
        1,
    );
    for idx in 0_u8..40 {
        commit_global_cells(
            &mut node,
            "docs",
            row((0x50 + idx) as usize),
            BTreeMap::from([("title".to_owned(), Value::String(format!("later-{idx}")))]),
            1_010 + idx as u64,
            2 + idx as u64,
        );
    }

    node.reset_query_engine_read_metrics();
    node.reset_storage_read_metrics();
    let rows = current_titles(
        &table,
        node.query_rows_at(&shape, &binding, GlobalTime(1))
            .expect("bounded historical query"),
    );
    let read_metrics = node.take_storage_read_metrics();
    let selected_metrics = node.query_engine_read_metrics().clone();

    assert_eq!(
        rows,
        BTreeMap::from([(row(0x41), Value::String("at-cut".to_owned()))])
    );
    assert_eq!(selected_metrics.source_global_time_range_scans, 1);
    assert_eq!(
        read_metrics.global_changes_indexes.ranges, 1,
        "bounded cut should use one by_table_global_time range"
    );
    assert!(
        read_metrics.global_changes_indexes.reads <= 2,
        "small cut should not read the later same-table history: {:?}",
        read_metrics.global_changes_indexes
    );
    assert!(
        read_metrics.global_changes_rows.reads <= 2,
        "small cut should not fetch later same-table change rows: {:?}",
        read_metrics.global_changes_rows
    );
}

#[test]
fn denormalized_current_content_witness_matches_history_payload_bytes() {
    let (_dir, mut node) = open_node();
    let first = commit_global_cells(
        &mut node,
        "issues",
        row(11),
        BTreeMap::from([
            ("title".to_owned(), Value::String("first".to_owned())),
            ("state".to_owned(), Value::String("open".to_owned())),
            ("assignee".to_owned(), Value::Uuid(author(1).0)),
            ("priority".to_owned(), Value::U64(1)),
        ]),
        1_000,
        1,
    );
    let second = node
        .commit_mergeable(
            MergeableCommit::new("issues", row(11), 1_100)
                .made_by(AuthorId::SYSTEM)
                .parents(vec![first])
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("second".to_owned())),
                    ("state".to_owned(), Value::String("closed".to_owned())),
                    ("assignee".to_owned(), Value::Uuid(author(2).0)),
                    ("priority".to_owned(), Value::U64(2)),
                ])),
        )
        .expect("commit second version");
    node.apply_fate_update(
        second,
        Fate::Accepted,
        Some(GlobalTime(2)),
        Some(DurabilityTier::Global),
    )
    .expect("accept second version");

    let table = node.table("issues").expect("issues table").clone();
    let current_source = node
        .physical_current_source_graph(
            node.catalogue.current_schema_version_id,
            "issues",
            PhysicalCurrentClass::Global,
        )
        .expect("physical current source")
        .project(maintained_view_history_storage_field_names(&table));
    let current_deltas = node
        .database
        .query_graph(current_source)
        .expect("query denormalized current payload");
    let current_rows = current_deltas
        .iter()
        .filter(|(_, weight)| *weight > 0)
        .map(|(record, _)| record.raw().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(current_rows.len(), 1);

    let history_source = node
        .physical_history_source_graph(node.catalogue.current_schema_version_id, "issues")
        .expect("physical history source");
    let history_deltas = node
        .database
        .query_graph(
            history_source
                .project(maintained_view_history_storage_field_names(&table))
                .filter(
                    PredicateExpr::And(vec![
                        PredicateExpr::eq("row_uuid", Value::Uuid(row(11).0)),
                        PredicateExpr::eq("tx_time", Value::U64(second.time.0)),
                    ])
                    .canonicalize(),
                ),
        )
        .expect("query canonical history payload");
    let history_rows = history_deltas
        .iter()
        .filter(|(_, weight)| *weight > 0)
        .map(|(record, _)| record.raw().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(history_rows.len(), 1);
    assert_eq!(
        current_rows[0], history_rows[0],
        "denormalized current witness payload must byte-match canonical history payload"
    );
}
