//! read sources query-evaluation tests.

use super::*;

/// This is intentionally structural: write-policy admission supplies inline
/// rows before there is a public result to inspect. A provenance-only policy
/// requirement must still acquire the hidden version capability used by the
/// policy program; callers must not have to request that capability separately.
#[test]
fn inline_policy_provenance_requirement_synthesizes_version_witnesses() {
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("docs").column("title", PublicColumnType::Text)),
    );
    let table = &schema.tables[0];
    let requirements = SourceRequirements {
        app_fields: FieldRequirement::None,
        metadata: BTreeSet::from([SourceMetadataRequirement::Provenance(
            ProvenanceField::CreatedBy,
        )]),
    };
    assert!(
        !requirements
            .metadata
            .contains(&SourceMetadataRequirement::VersionWitnesses),
        "the policy request itself must remain provenance-only"
    );
    let candidate = current_row_from_cells(
        table,
        row(0x21),
        &BTreeMap::from([("title".to_owned(), Value::String("inline".to_owned()))]),
    )
    .unwrap();

    let (_graph, descriptor, metadata) = inline_current_graph_with_source_metadata_for_test(
        table,
        vec![candidate],
        SchemaVersionAlias(7),
        "inline-policy",
        &requirements,
    )
    .unwrap();

    assert!(matches!(
        metadata.get(&SourceMetadataRequirement::VersionWitnesses),
        Some(SourceMetadataFields::VersionWitnesses {
            schema_version_field,
            tx_time_field,
            tx_node_field,
            branch_or_prefix_field: None,
        }) if schema_version_field == "schema_version"
            && tx_time_field == "tx_time"
            && tx_node_field == "tx_node_id"
    ));
    for field in [
        "table",
        "layer",
        "schema_version",
        "parents",
        "authored_columns",
    ] {
        assert!(
            descriptor.field_index(field).is_some(),
            "synthesized witness descriptor must carry {field}"
        );
    }
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
    node.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
        author: AuthorSubject::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(
            node.author_schema_lineage_publication(
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
            )
            .unwrap(),
        ),
    })
    .unwrap();
    node.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
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
            AuthorSubject::SYSTEM,
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
    let mut resolver = JazzSourceGraphPreparer {
        node: &mut node,
        read_view: &read_view,
        inline_sources: BTreeMap::new(),
        access_paths: BTreeMap::new(),
        count_access_path_metrics: true,
        current_projection_targets: BTreeMap::new(),
    };

    assert!(resolver.needs_projected_current_source("users"));
    let resolved = resolver.prepare_source_graph(&source_request).unwrap();
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
        .commit_mergeable_settled(
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
fn denormalized_current_content_witness_projects_history_provenance_to_unix_milliseconds() {
    let (_dir, mut node) = open_node();
    let first = commit_global_cells(
        &mut node,
        "issues",
        row(11),
        BTreeMap::from([
            ("title".to_owned(), Value::String("first".to_owned())),
            ("state".to_owned(), Value::String("open".to_owned())),
            ("assignee".to_owned(), Value::Uuid(author(1).test_uuid())),
            ("priority".to_owned(), Value::U64(1)),
        ]),
        1_000,
        1,
    );
    let second = node
        .commit_mergeable_settled(
            MergeableCommit::new("issues", row(11), 1_100)
                .made_by(AuthorSubject::SYSTEM)
                .parents(vec![first])
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("second".to_owned())),
                    ("state".to_owned(), Value::String("closed".to_owned())),
                    ("assignee".to_owned(), Value::Uuid(author(2).test_uuid())),
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
        .map(|(record, _)| {
            (
                record
                    .get_u64(record.descriptor().field_index("created_at").unwrap())
                    .unwrap(),
                record
                    .get_u64(record.descriptor().field_index("updated_at").unwrap())
                    .unwrap(),
            )
        })
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
        .map(|(record, _)| {
            (
                record
                    .get_u64(record.descriptor().field_index("created_at").unwrap())
                    .unwrap(),
                record
                    .get_u64(record.descriptor().field_index("updated_at").unwrap())
                    .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(history_rows.len(), 1);
    assert_eq!(
        current_rows[0].0,
        TxTime(history_rows[0].0).physical_ms(),
        "current created_at must expose the history HLC's physical milliseconds"
    );
    assert_eq!(
        current_rows[0].1,
        TxTime(history_rows[0].1).physical_ms(),
        "current updated_at must expose the history HLC's physical milliseconds"
    );
}
