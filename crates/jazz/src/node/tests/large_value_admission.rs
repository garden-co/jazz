fn large_value_admission_commits(
) -> (
    JazzSchema,
    MergeableCommit,
    Vec<MergeableCommit>,
    String,
    RowUuid,
    String,
) {
    let schema = two_column_schema();
    let owner_table = schema.tables.iter().find(|table| table.name == "todos").unwrap();
    let column = owner_table
        .columns
        .iter()
        .find(|column| column.name == "title")
        .unwrap();
    let large_schema = column.large_value.as_ref().unwrap();
    let owner = row(0xa1);
    let text = "generated-node-admission ".repeat(512);
    let domain = crate::large_values::LargeValueOwnerDomain::new("todos", owner.0).unwrap();
    let mut rows = crate::large_values::MemoryLargeValueNodeRows::default();
    let physical = crate::large_values::LargeValue::create(
        crate::large_values::ValueKind::String,
        &domain,
        text.as_bytes(),
        large_schema.inline_up_to as usize,
        crate::large_values::ContentTree::new(Default::default()).unwrap(),
        &mut rows,
    )
    .unwrap()
    .encode_storage_value(large_schema)
    .unwrap();
    let owner_commit = MergeableCommit::new("todos", owner, 10).cells(BTreeMap::from([
        ("title".to_owned(), physical),
        ("body".to_owned(), Value::String("ordinary body".to_owned())),
    ]));
    let node_commits = rows
        .into_rows()
        .map(|node| {
            MergeableCommit::new(node.table_name(), RowUuid(node.row_id), 10)
                .cells(node.cells(Default::default()).unwrap())
        })
        .collect::<Vec<_>>();
    let node_table = crate::large_values::large_value_node_table_name("todos");
    (schema, owner_commit, node_commits, text, owner, node_table)
}

/// Generated node rows are admitted only when Alice's owner write exposes the
/// exact descriptor closure, and the resulting ordinary Jazz row materializes
/// through that closure.
///
/// alice ──owner + canonical nodes──► node admission ──► materialized todo
#[test]
fn generated_large_value_transaction_admits_the_exact_owner_closure() {
    let (schema, owner, nodes, text, row_uuid, _) = large_value_admission_commits();
    let (_dir, mut node) = open_node_with_schema(node(0xa1), schema);
    let mut commits = vec![owner];
    commits.extend(nodes);
    node.commit_mergeable_many(commits).unwrap();

    let rows = node.current_rows("todos", DurabilityTier::Local).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row_uuid);
    assert_eq!(rows[0].cell_at(0), Some(Value::String(text)));
}

/// Mallory cannot insert an otherwise canonical hidden node without an owner
/// descriptor in the same transaction.
///
/// mallory ──forged node──► admission ──✗──► hidden table
#[test]
fn generated_large_value_node_rejects_forged_insert_without_owner_mutation() {
    let (schema, _, nodes, _, _, _) = large_value_admission_commits();
    let (_dir, mut node) = open_node_with_schema(node(0xa2), schema);
    let error = node.commit_mergeable(nodes.into_iter().next().unwrap()).unwrap_err();
    assert!(matches!(error, Error::InvalidMergeableCommit(_)));
}

/// Mallory cannot substitute either an arbitrary node identity or arbitrary
/// node bytes into the generated table, even when an owner write is present.
///
/// mallory ──tampered node + owner──► admission ──✗──► history
#[test]
fn generated_large_value_node_rejects_malformed_identity_and_payload() {
    let (schema, owner, nodes, _, _, _) = large_value_admission_commits();
    for (column, replacement) in [
        ("content_id", Value::Bytes(vec![0x44; 32])),
        ("payload", Value::Bytes(vec![0xff, 0x00])),
    ] {
        let (_dir, mut node) = open_node_with_schema(node(0xa3), schema.clone());
        let mut forged = nodes[0].clone();
        forged.cells.insert(column.to_owned(), replacement);
        let error = node
            .commit_mergeable_many(vec![owner.clone(), forged])
            .unwrap_err();
        assert!(matches!(error, Error::InvalidMergeableCommit(_)));
    }
}

/// Mallory cannot smuggle a valid but unreachable generated node alongside an
/// inline owner update.
///
/// mallory ──owner + orphan node──► admission ──✗──► history
#[test]
fn generated_large_value_node_rejects_orphan_closure_members() {
    let (schema, _owner, nodes, _, owner_row, _) = large_value_admission_commits();
    let (_dir, mut node) = open_node_with_schema(node(0xa4), schema);
    let inline_owner = MergeableCommit::new("todos", owner_row, 10).cells(BTreeMap::from([
        ("title".to_owned(), Value::String("small".to_owned())),
        ("body".to_owned(), Value::String("ordinary body".to_owned())),
    ]));
    let error = node
        .commit_mergeable_many(vec![inline_owner, nodes.into_iter().next().unwrap()])
        .unwrap_err();
    assert!(matches!(error, Error::InvalidMergeableCommit(_)));
}

#[test]
fn generated_large_value_descriptor_rejects_a_missing_root_without_pending_nodes() {
    let (schema, owner, _, _, _, _) = large_value_admission_commits();
    let (_dir, mut node) = open_node_with_schema(node(0xaa), schema);
    let error = node.commit_mergeable(owner).unwrap_err();
    assert!(matches!(error, Error::InvalidMergeableCommit(_)));
}

#[test]
fn generated_large_value_descriptor_rejects_dishonest_stored_root_metrics() {
    let (schema, owner, nodes, _, owner_row, _) = large_value_admission_commits();
    let (_dir, mut node) = open_node_with_schema(node(0xab), schema.clone());
    let mut initial = vec![owner.clone()];
    initial.extend(nodes);
    node.commit_mergeable_many(initial).unwrap();

    let table = schema.tables.iter().find(|table| table.name == "todos").unwrap();
    let large_schema = table.columns[0].large_value.as_ref().unwrap();
    let stored = owner.cells.get("title").unwrap();
    let mut value = crate::large_values::LargeValue::decode_storage_value(large_schema, stored)
        .unwrap();
    let crate::large_values::LargeValue::Chunked(chunked) = &mut value else {
        panic!("fixture must be chunked");
    };
    chunked.root_byte_len += 1;
    let forged = MergeableCommit::new("todos", owner_row, 20).cells(BTreeMap::from([
        (
            "title".to_owned(),
            value.encode_storage_value(large_schema).unwrap(),
        ),
        ("body".to_owned(), Value::String("ordinary body".to_owned())),
    ]));
    let error = node.commit_mergeable(forged).unwrap_err();
    assert!(matches!(error, Error::InvalidMergeableCommit(_)));
}

/// Once Alice has admitted a node, neither an update-shaped rewrite nor a
/// deletion can enter its immutable hidden table.
///
/// alice ──valid owner + nodes──► history
/// mallory ──rewrite/delete node──► admission ──✗──► history
#[test]
fn generated_large_value_node_rejects_mutation_and_delete() {
    let (schema, owner, nodes, _, _, node_table) = large_value_admission_commits();
    let (_dir, mut node) = open_node_with_schema(node(0xa5), schema);
    let mut commits = vec![owner];
    commits.extend(nodes.clone());
    let tx = node.commit_mergeable_many(commits).unwrap();

    let mut rewrite = nodes[0].clone();
    rewrite.parents = vec![tx];
    assert!(matches!(
        node.commit_mergeable(rewrite),
        Err(Error::InvalidMergeableCommit(_))
    ));
    assert!(matches!(
        node.commit_mergeable(
            MergeableCommit::new(node_table, nodes[0].row_uuid, 20)
                .deletion(DeletionEvent::Deleted)
        ),
        Err(Error::InvalidMergeableCommit(_))
    ));
}

/// Mallory cannot bypass local admission by serializing a valid owner version
/// with a payload-tampered hidden-node version directly onto the wire.
///
/// mallory ──crafted commit unit──► authority ──✗──► rejected fate
#[test]
fn generated_large_value_node_wire_admission_rejects_crafted_payload() {
    let (schema, owner, nodes, _, _, node_table) = large_value_admission_commits();
    let (_source_dir, mut source) = open_node_with_schema(node(0xa6), schema.clone());
    let mut commits = vec![owner];
    commits.extend(nodes);
    let tx_id = source.commit_mergeable_many(commits).unwrap();
    let SyncMessage::CommitUnit { tx, mut versions } = source.commit_unit_for(tx_id).unwrap() else {
        panic!("local mergeable commit must serialize as a commit unit");
    };
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == node_table)
        .unwrap();
    let index = versions
        .iter()
        .position(|version| version.table() == node_table)
        .unwrap();
    let original = &versions[index];
    let mut cells = table
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| (column.name.clone(), original.cell_at(index).unwrap()))
        .collect::<BTreeMap<_, _>>();
    cells.insert("payload".to_owned(), Value::Bytes(vec![0xf0, 0x0d]));
    versions[index] = VersionRecord::from_cells(
        table,
        original.schema_version(),
        original.row_uuid(),
        original.parents(),
        original.created_by(),
        original.created_at(),
        original.updated_by(),
        original.updated_at(),
        &cells,
        original.deletion(),
    )
    .unwrap()
    .with_branch_key(original.branch_key().clone())
    .with_authored_columns(original.authored_columns().cloned());

    let (_receiver_dir, mut receiver) = open_node_with_schema(node(0xa7), schema);
    let updates = receiver.ingest_commit_unit(tx, versions, 100).unwrap();
    assert!(matches!(
        updates.as_slice(),
        [SyncMessage::FateUpdate {
            fate: Fate::Rejected(RejectionReason::MalformedCommit(reason)),
            ..
        }] if reason.contains("generated large-value node")
    ));
    assert!(receiver.query_table_versions("todos").unwrap().is_empty());
}

#[test]
fn generated_large_value_node_wire_admission_rejects_missing_root_closure() {
    let (schema, owner, nodes, _, _, node_table) = large_value_admission_commits();
    let (_source_dir, mut source) = open_node_with_schema(node(0xac), schema.clone());
    let mut commits = vec![owner];
    commits.extend(nodes);
    let tx_id = source.commit_mergeable_many(commits).unwrap();
    let SyncMessage::CommitUnit {
        mut tx,
        mut versions,
    } = source.commit_unit_for(tx_id).unwrap()
    else {
        panic!("local mergeable commit must serialize as a commit unit");
    };
    versions.retain(|version| version.table() != node_table);
    tx.n_total_writes = versions.len() as u32;

    let (_receiver_dir, mut receiver) = open_node_with_schema(node(0xad), schema);
    let updates = receiver.ingest_commit_unit(tx, versions, 100).unwrap();
    assert!(matches!(
        updates.as_slice(),
        [SyncMessage::FateUpdate {
            fate: Fate::Rejected(RejectionReason::MalformedCommit(reason)),
            ..
        }] if reason.contains("large-value descriptor")
    ));
    assert!(receiver.query_table_versions("todos").unwrap().is_empty());
}

/// Alice's untouched owner-plus-node commit unit remains admissible at a
/// receiving authority and materializes the same logical text there.
///
/// alice ──canonical commit unit──► authority ──accept──► materialized todo
#[test]
fn generated_large_value_node_wire_admission_accepts_canonical_closure() {
    let (schema, owner, nodes, text, row_uuid, _) = large_value_admission_commits();
    let (_source_dir, mut source) = open_node_with_schema(node(0xa8), schema.clone());
    let mut commits = vec![owner];
    commits.extend(nodes);
    let tx_id = source.commit_mergeable_many(commits).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = source.commit_unit_for(tx_id).unwrap() else {
        panic!("local mergeable commit must serialize as a commit unit");
    };

    let (_receiver_dir, mut receiver) = open_node_with_schema(node(0xa9), schema);
    let updates = receiver.ingest_commit_unit(tx, versions, 100).unwrap();
    assert!(updates.iter().any(|update| matches!(
        update,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    )));
    let rows = receiver.current_rows("todos", DurabilityTier::Local).unwrap();
    assert_eq!(rows[0].row_uuid(), row_uuid);
    assert_eq!(rows[0].cell_at(0), Some(Value::String(text)));
}
