use crate::tx::{
    ContributionComponent, ContributionCoordinate, ContributionDot, ContributionMergeProvenance,
    ContributionSubstitution,
};

#[test]
fn opening_existing_storage_recovers_mirrors_and_high_water_marks() {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    let first_tx;
    {
        let mut node = open_node_at(&temp_dir, schema.clone());
        first_tx = node
            .commit_mergeable_settled(
                MergeableCommit::new("todos", row(9), 10).cells(BTreeMap::from([(
                    "title".to_owned(),
                    "persisted".to_owned(),
                )])),
            )
            .unwrap();
    }

    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    let mut reopened = NodeState::new(node(1), schema, storage).unwrap();

    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(9), title_cells("persisted"))])
    );
    assert_eq!(
        reopened.transaction_state_settled(first_tx).unwrap(),
        (Fate::Pending, None, DurabilityTier::Local)
    );
    let next_tx = reopened
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(10), 11).cells(BTreeMap::from([(
                "title".to_owned(),
                "after restart".to_owned(),
            )])),
        )
        .unwrap();
    assert_eq!(next_tx.time, TxTime::from(11));
}

#[test]
fn contribution_merge_provenance_survives_reopen() {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    let tx_id = TxId::new(TxTime::from(10), node(1));
    let provenance = canonical_contribution_provenance(tx_id);
    {
        let mut core = open_node_at(&temp_dir, schema.clone());
        core.ingest_commit_unit_settled(
            Transaction {
                tx_id,
                kind: TxKind::Mergeable,
                n_total_writes: 1,
                made_by: AuthorSubject::SYSTEM,
                permission_subject: None,
                base_snapshot: None,
                row_read_set: None,
                absent_read_set: None,
                predicate_read_set: None,
                user_metadata_json: None,
                contribution_merge: Some(provenance.clone()),
            },
            vec![version_record(row(9), Vec::new(), title_cells("merged"), None)],
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap();
        assert_eq!(
            core.transaction_record(tx_id).unwrap().contribution_merge,
            Some(provenance.clone())
        );
    }

    let mut reopened = reopen_node_at(&temp_dir, node(1), schema);
    assert_eq!(
        reopened
            .transaction_record(tx_id)
            .unwrap()
            .contribution_merge,
        Some(provenance)
    );
}

#[test]
fn contribution_provenance_persists_column_components_as_physical_ids() {
    // The public transaction still carries a logical column name.  Only the
    // durable system-table record crosses the physical-storage boundary.
    let schema = schema();
    let (_dir, core) = open_node_with_schema(node(1), schema.clone());
    let tx_id = TxId::new(TxTime::from(10), node(1));
    let provenance = canonical_contribution_provenance(tx_id);
    let title_id = core.catalogue.physical_mappings[&schema.version_id()].tables["todos"].columns
        ["title"];

    let stored = core
        .contribution_merge_storage_value(Some(&provenance))
        .unwrap();
    // Both the target and its source coordinate carry the same physical slot.
    let table_id = core.catalogue.physical_mappings[&schema.version_id()].tables["todos"].table_id;
    assert_eq!(
        stored_contribution_coordinate_ids(stored),
        vec![(table_id.0, title_id.0), (table_id.0, title_id.0)]
    );
}

#[test]
fn contribution_provenance_survives_compatible_column_rename_and_reopen() {
    let base = schema();
    let renamed_schema = renamed_tasks_schema();
    let renamed = SchemaVersion::new(renamed_schema);
    let (dir, mut core) = open_node_with_schema(node(1), base.clone());
    let tx_id = TxId::new(TxTime::from(10), node(1));
    let provenance = canonical_contribution_provenance(tx_id);
    let title_id = core.catalogue.physical_mappings[&base.version_id()].tables["todos"].columns
        ["title"];
    core.ingest_commit_unit_settled(
        Transaction {
            tx_id,
            kind: TxKind::Mergeable,
            n_total_writes: 1,
            made_by: AuthorSubject::SYSTEM,
            permission_subject: None,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json: None,
            contribution_merge: Some(provenance.clone()),
        },
        vec![version_record(row(9), Vec::new(), title_cells("merged"), None)],
        u64::MAX - SKEW_TOLERANCE_MS,
    )
    .unwrap();
    publish_schema_lineage(
        &mut core,
        renamed.clone(),
        MigrationLens::new(
            base.version_id(),
            renamed.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "tasks".to_owned(),
                ops: vec![
                    LensOp::RenameTable {
                        from: "todos".to_owned(),
                        to: "tasks".to_owned(),
                    },
                    LensOp::RenameColumn {
                        from: "title".to_owned(),
                        to: "name".to_owned(),
                    },
                ],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: renamed.id,
        },
    })
    .unwrap();
    assert_eq!(
        core.catalogue.physical_mappings[&renamed.id].tables["tasks"].columns["name"],
        title_id
    );

    drop(core);
    let mut reopened = reopen_node_at(&dir, node(1), base);
    assert_eq!(
        reopened.transaction_record(tx_id).unwrap().contribution_merge,
        Some(provenance)
    );
}

fn stored_contribution_coordinate_ids(value: Value) -> Vec<(u64, u64)> {
    let Value::Nullable(Some(record)) = value else {
        panic!("fixture stores contribution provenance")
    };
    let Value::Record(record) = *record else {
        panic!("fixture contribution provenance is a record")
    };
    let record = ContributionMergeStorageRecord::new(record);
    record
        .substitutions()
        .unwrap()
        .into_iter()
        .flat_map(|substitution| {
            let Value::Record(substitution) = substitution else {
                panic!("fixture substitution is a record")
            };
            let substitution = ContributionSubstitutionStorageRecord::new(substitution);
            let target = contribution_coordinate_ids(substitution.target().unwrap());
            let sources = substitution
                .sources()
                .unwrap()
                .into_iter()
                .map(|source| {
                    let Value::Record(source) = source else {
                        panic!("fixture source is a record")
                    };
                    let source = ContributionDotStorageRecord::new(source);
                    contribution_coordinate_ids(source.coordinate().unwrap())
                });
            std::iter::once(target).chain(sources)
        })
        .collect()
}

fn contribution_coordinate_ids(record: OwnedRecord) -> (u64, u64) {
    let coordinate = ContributionCoordinateStorageRecord::new(record);
    let component = coordinate.component().unwrap();
    (
        coordinate.physical_table_id().unwrap(),
        ContributionColumnStorageRecord::new(component.into_record())
            .physical_column_id()
            .unwrap(),
    )
}

/// Rewrite the otherwise valid internal system-table payload to exercise
/// recovery's fail-closed physical-identity checks. Public APIs only admit
/// logical column names, so they cannot construct this corruption.
fn with_stored_contribution_column_id(value: Value, physical_column_id: u64) -> Value {
    with_stored_contribution_coordinate_ids(value, None, physical_column_id)
}

fn with_stored_contribution_table_id(value: Value, physical_table_id: u64) -> Value {
    with_stored_contribution_coordinate_ids(value, Some(physical_table_id), 1)
}

fn with_stored_contribution_coordinate_ids(
    value: Value,
    physical_table_id: Option<u64>,
    physical_column_id: u64,
) -> Value {
    let Value::Nullable(Some(record)) = value else {
        panic!("fixture stores contribution provenance")
    };
    let Value::Record(record) = *record else {
        panic!("fixture contribution provenance is a record")
    };
    let descriptor = record.descriptor().clone();
    let record = ContributionMergeStorageRecord::new(record);
    let substitutions = record
        .substitutions()
        .unwrap()
        .into_iter()
        .map(|substitution| {
            let Value::Record(substitution) = substitution else {
                panic!("fixture substitution is a record")
            };
            Value::Record(rewrite_contribution_substitution_column_id(
                substitution,
                physical_table_id,
                physical_column_id,
            ))
        })
        .collect();
    let rewritten = ContributionMergeStorageRecord::encode(
        &descriptor,
        record.source().unwrap(),
        record.target().unwrap(),
        substitutions,
    )
    .unwrap()
    .record()
    .clone();
    records::RecordField::to_value(&Some(rewritten))
}

fn rewrite_contribution_substitution_column_id(
    record: OwnedRecord,
    physical_table_id: Option<u64>,
    physical_column_id: u64,
) -> OwnedRecord {
    let descriptor = record.descriptor().clone();
    let record = ContributionSubstitutionStorageRecord::new(record);
    let sources = record
        .sources()
        .unwrap()
        .into_iter()
        .map(|source| {
            let Value::Record(source) = source else {
                panic!("fixture source is a record")
            };
            let descriptor = source.descriptor().clone();
            let source = ContributionDotStorageRecord::new(source);
            Value::Record(
                ContributionDotStorageRecord::encode(
                    &descriptor,
                    source.tx_time().unwrap(),
                    source.tx_node().unwrap(),
                    rewrite_contribution_coordinate_column_id(
                        source.coordinate().unwrap(),
                        physical_table_id,
                        physical_column_id,
                    ),
                )
                .unwrap()
                .record()
                .clone(),
            )
        })
        .collect();
    ContributionSubstitutionStorageRecord::encode(
        &descriptor,
        rewrite_contribution_coordinate_column_id(
            record.target().unwrap(),
            physical_table_id,
            physical_column_id,
        ),
        sources,
    )
    .unwrap()
    .record()
    .clone()
}

fn rewrite_contribution_coordinate_column_id(
    record: OwnedRecord,
    physical_table_id: Option<u64>,
    physical_column_id: u64,
) -> OwnedRecord {
    let descriptor = record.descriptor().clone();
    let records::ValueType::Enum(component_schema) = record_field_type(&descriptor, 4) else {
        panic!("fixture component has an enum schema")
    };
    let record = ContributionCoordinateStorageRecord::new(record);
    let component = record.component().unwrap();
    let case = component_schema.case(component.tag()).unwrap();
    assert_eq!(case.name, "column", "fixture uses a column component");
    let component = records::EnumValue::new(
        component.tag(),
        ContributionColumnStorageRecord::encode(&case.payload, physical_column_id)
            .unwrap()
            .record()
            .clone(),
    );
    ContributionCoordinateStorageRecord::encode(
        &descriptor,
        record.branch_key().unwrap(),
        physical_table_id.unwrap_or_else(|| record.physical_table_id().unwrap()),
        record.row_uuid().unwrap(),
        record.layer().unwrap(),
        component,
    )
    .unwrap()
    .record()
    .clone()
}

fn canonical_contribution_provenance(tx_id: TxId) -> ContributionMergeProvenance {
    let coordinate = ContributionCoordinate {
        branch_key: BranchKey::default(),
        table: "todos".to_owned(),
        row_uuid: row(9),
        layer: MergeAspect::Content,
        component: ContributionComponent::Column("title".to_owned()),
    };
    ContributionMergeProvenance::canonical(
        BranchKey::default(),
        BranchKey::default(),
        vec![ContributionSubstitution {
            target: coordinate.clone(),
            sources: vec![ContributionDot {
                tx_id,
                coordinate,
            }],
        }],
    )
    .unwrap()
}

/// Persist a structurally valid, but semantically non-canonical, contribution
/// record through the system-table encoder. Opening must reject it before a
/// node becomes resident; normal public commit APIs cannot construct it.
fn reopen_with_noncanonical_contribution_provenance(
    mutate: impl FnOnce(&mut ContributionMergeProvenance),
) -> Error {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    let tx_id = TxId::new(TxTime::from(10), node(1));
    {
        let mut core = open_node_at(&temp_dir, schema.clone());
        core.ingest_commit_unit_settled(
            Transaction {
                tx_id,
                kind: TxKind::Mergeable,
                n_total_writes: 1,
                made_by: AuthorSubject::SYSTEM,
                permission_subject: None,
                base_snapshot: None,
                row_read_set: None,
                absent_read_set: None,
                predicate_read_set: None,
                user_metadata_json: None,
                contribution_merge: Some(canonical_contribution_provenance(tx_id)),
            },
            vec![version_record(row(9), Vec::new(), title_cells("merged"), None)],
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap();

        let mut stored = core.query_transaction(tx_id).unwrap().unwrap();
        mutate(
            stored
                .tx
                .contribution_merge
                .as_mut()
                .expect("fixture stores provenance"),
        );
        let mut batch = core.database.open_batch();
        batch.update(
            "jazz_transactions",
            transaction_values(
                stored.node_alias,
                &stored.tx,
                stored.fate,
                stored.global_time,
                stored.durability,
                core.contribution_merge_storage_value(stored.tx.contribution_merge.as_ref())
                    .unwrap(),
            ),
        );
        let applied = crate::db::block_on(core.database.apply_batch(batch)).unwrap();
        let persisted = crate::db::block_on(applied.persist());
        core.database.finish_persistence(persisted).unwrap();
    }

    let column_families = schema.column_families();
    let references = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &references).unwrap();
    match NodeState::new(node(1), schema, storage).resolve() {
        Ok(_) => panic!("opening non-canonical contribution provenance must fail"),
        Err(error) => error,
    }
}

fn reopen_with_corrupt_contribution_coordinate(
    mutate: impl FnOnce(Value) -> Value,
) -> Error {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    let tx_id = TxId::new(TxTime::from(10), node(1));
    {
        let mut core = open_node_at(&temp_dir, schema.clone());
        core.ingest_commit_unit_settled(
            Transaction {
                tx_id,
                kind: TxKind::Mergeable,
                n_total_writes: 1,
                made_by: AuthorSubject::SYSTEM,
                permission_subject: None,
                base_snapshot: None,
                row_read_set: None,
                absent_read_set: None,
                predicate_read_set: None,
                user_metadata_json: None,
                contribution_merge: Some(canonical_contribution_provenance(tx_id)),
            },
            vec![version_record(row(9), Vec::new(), title_cells("merged"), None)],
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap();

        let stored = core.query_transaction(tx_id).unwrap().unwrap();
        let contribution_merge = mutate(
            core.contribution_merge_storage_value(stored.tx.contribution_merge.as_ref())
                .unwrap(),
        );
        let mut batch = core.database.open_batch();
        batch.update(
            "jazz_transactions",
            transaction_values(
                stored.node_alias,
                &stored.tx,
                stored.fate,
                stored.global_time,
                stored.durability,
                contribution_merge,
            ),
        );
        let applied = crate::db::block_on(core.database.apply_batch(batch)).unwrap();
        let persisted = crate::db::block_on(applied.persist());
        core.database.finish_persistence(persisted).unwrap();
    }

    let column_families = schema.column_families();
    let references = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &references).unwrap();
    match NodeState::new(node(1), schema, storage).resolve() {
        Ok(_) => panic!("opening corrupt contribution coordinate storage must fail"),
        Err(error) => error,
    }
}

fn reopen_with_invalid_contribution_column_id(physical_column_id: u64) -> Error {
    reopen_with_corrupt_contribution_coordinate(|value| {
        with_stored_contribution_column_id(value, physical_column_id)
    })
}

fn reopen_with_invalid_contribution_table_id(physical_table_id: u64) -> Error {
    reopen_with_corrupt_contribution_coordinate(|value| {
        with_stored_contribution_table_id(value, physical_table_id)
    })
}

#[test]
fn reopen_rejects_zero_contribution_physical_column_id_before_residency() {
    let error = reopen_with_invalid_contribution_column_id(0);
    assert!(matches!(
        error,
        Error::InvalidStoredValue("stored contribution physical column id must be nonzero")
    ));
}

#[test]
fn reopen_rejects_unknown_contribution_physical_column_id_before_residency() {
    let error = reopen_with_invalid_contribution_column_id(u64::MAX);
    assert!(matches!(
        error,
        Error::InvalidStoredValue(
            "stored contribution physical column id is absent from its table mapping"
        )
    ));
}

#[test]
fn reopen_rejects_zero_contribution_physical_table_id_before_residency() {
    let error = reopen_with_invalid_contribution_table_id(0);
    assert!(matches!(
        error,
        Error::InvalidStoredValue("stored contribution physical table id must be nonzero")
    ));
}

#[test]
fn reopen_rejects_unknown_contribution_physical_table_id_before_residency() {
    let error = reopen_with_invalid_contribution_table_id(u64::MAX);
    assert!(matches!(
        error,
        Error::InvalidStoredValue(
            "stored contribution physical table id is absent from the catalogue"
        )
    ));
}

#[test]
fn reopen_rejects_noncanonical_contribution_source_dots() {
    // alice's persisted provenance has a valid record shape, but an unsorted
    // source-dot array.  This must not silently become canonical on recovery.
    let error = reopen_with_noncanonical_contribution_provenance(|provenance| {
        let source = provenance.substitutions[0].sources[0].clone();
        provenance.substitutions[0].sources.push(ContributionDot {
            tx_id: TxId::new(TxTime::from(9), source.tx_id.node),
            coordinate: source.coordinate,
        });
    });
    assert!(matches!(
        error,
        Error::InvalidStoredValue("transaction contribution provenance must be canonical")
    ));
}

#[test]
fn reopen_rejects_duplicate_contribution_source_dots() {
    // A duplicate source is also structurally valid, but provenance identity
    // must be set-like so downstream expansion cannot double-count it.
    let error = reopen_with_noncanonical_contribution_provenance(|provenance| {
        let source = provenance.substitutions[0].sources[0].clone();
        provenance.substitutions[0].sources.push(source);
    });
    assert!(matches!(
        error,
        Error::InvalidStoredValue("transaction contribution provenance must be canonical")
    ));
}

#[test]
fn reopen_rejects_duplicate_contribution_substitution_targets() {
    // alice's on-disk record is structurally decodable, but maps one derived
    // target twice.  Recovery must fail before rebuilding any resident state.
    let error = reopen_with_noncanonical_contribution_provenance(|provenance| {
        provenance
            .substitutions
            .push(provenance.substitutions[0].clone());
    });
    assert!(matches!(
        error,
        Error::InvalidStoredValue("transaction contribution provenance must be canonical")
    ));
}

#[cfg(feature = "testing")]
#[test]
fn open_receipt_counts_physical_recovery_scans_exactly() {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    {
        let mut node = open_node_at(&temp_dir, schema.clone());
        for (id, time) in [(1, 10), (2, 11)] {
            node.commit_mergeable_settled(
                MergeableCommit::new("todos", row(id), time).cells(title_cells("persisted")),
            )
            .unwrap();
        }
        crate::db::block_on(node.database.close()).unwrap();
    }

    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    let (_reopened, receipt) = NodeState::new_with_open_receipt_for_test(
        node(1),
        schema,
        storage,
        false,
    )
    .unwrap();

    // The nullable global-time index is the actual physical access path:
    // local pending transactions remain in its `None` bucket and must not be
    // decoded by bounded `Some`-range recovery.
    assert_eq!(receipt.global_time_records_scanned, 0);
    assert_eq!(receipt.accepted_global_times, 0);
    assert_eq!(receipt.ahead_current_entries, 2);
}

#[cfg(feature = "testing")]
#[test]
fn open_receipt_attributes_catalogue_finalization_when_aliases_are_first_persisted() {
    // A fresh store plants the finalization work: both aliases are absent and
    // must be inserted after recovery. This catches an exported receipt phase
    // that is left at its Default::default() value.
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    let (_node, receipt) = NodeState::new_with_open_receipt_for_test(
        node(1),
        schema,
        storage,
        false,
    )
    .unwrap();

    assert!(
        !receipt.finalize_catalogue.is_zero(),
        "first-open alias persistence must be attributed to catalogue finalization"
    );
}

#[test]
fn opening_defers_malformed_current_row_to_read() {
    // This is necessarily an internal regression test: planting malformed
    // persisted bytes requires direct storage access, and the core point-read
    // path is where the persisted row key is available for error context.
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    {
        let mut node = open_node_at(&temp_dir, schema.clone());
        let tx_id = node
            .commit_mergeable_settled(
                MergeableCommit::new("todos", row(0xff), 10).cells(title_cells("persisted")),
            )
            .unwrap();
        node.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalTime(1)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
        let table = physical_global_current_table_name(
            node.physical_table_id_for_schema(schema.version_id(), "todos")
                .unwrap(),
        );
        let raw = node
            .database
            .primary_key_get_raw(
                &table,
                &[
                    Value::Bytes(BranchKey::default().canonical_bytes()),
                    Value::Uuid(row(0xff).0),
                ],
            )
            .unwrap()
            .unwrap();
        let variant_tag = raw.variant_tag();
        let (key, raw) = raw.into_parts();
        crate::db::block_on(node.database.close()).unwrap();
        drop(node);

        let cfs = schema.column_families();
        let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
        let storage =
            groove::storage::LayoutStorage::new(storage, StorageLayout::jazz_class_v1()).unwrap();
        storage
            .set(
                table.clone(),
                key,
                groove::records::encode_variant_record(variant_tag, &raw[..1]),
            )
            .unwrap();
        storage.close().unwrap();
    }

    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    let mut reopened = NodeState::new(node(1), schema, storage).unwrap();
    let error = reopened
        .local_current_row("todos", row(0xff))
        .expect_err("malformed current row must fail when read");
    assert!(
        matches!(
            error,
            Error::MalformedCurrentRow(ref details)
                if details.table == "todos" && details.row_uuid == row(0xff)
        ),
        "unexpected current-row read error: {error}"
    );
}

#[test]
fn recovery_sweeps_ahead_rows_for_globally_fated_transactions() {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    let tx_id;
    {
        let mut node = open_node_at(&temp_dir, schema.clone());
        tx_id = node
            .commit_mergeable_settled(
                MergeableCommit::new("todos", row(12), 10).cells(title_cells("crash window")),
            )
            .unwrap();
        assert_eq!(ahead_current_row_count(&mut node, "todos"), 1);

        let mut stored = node.query_transaction(tx_id).unwrap().unwrap();
        stored.fate = Fate::Accepted;
        stored.global_time = Some(GlobalTime(1));
        stored.durability = DurabilityTier::Global;
        let version = node.query_versions_for_tx(tx_id).unwrap().remove(0);
        let mut batch = node.database.open_batch();
        batch.update(
            "jazz_transactions",
            transaction_values(
                stored.node_alias,
                &stored.tx,
                stored.fate.clone(),
                stored.global_time,
                stored.durability,
                node.contribution_merge_storage_value(stored.tx.contribution_merge.as_ref())
                    .unwrap(),
            ),
        );
        node.write_global_current_update(&mut batch, &version, GlobalTime(1))
            .unwrap();
        let applied = crate::db::block_on(node.database.apply_batch(batch)).unwrap();
let persisted = crate::db::block_on(applied.persist());
node.database.finish_persistence(persisted).unwrap();
        assert_eq!(ahead_current_row_count(&mut node, "todos"), 1);
    }

    reset_query_versions_for_tx_call_count();
    let mut reopened = reopen_node_at(&temp_dir, node(1), schema);
    assert!(
        query_versions_for_tx_call_count() > 0,
        "crash recovery must sweep fated ahead-current leftovers"
    );
    assert_eq!(ahead_current_row_count(&mut reopened, "todos"), 0);
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(12), title_cells("crash window"))])
    );
}

#[test]
fn clean_close_reopen_skips_fated_ahead_current_sweep() {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    {
        let mut node = open_node_at(&temp_dir, schema.clone());
        let tx_id = node
            .commit_mergeable_settled(
                MergeableCommit::new("todos", row(13), 10).cells(title_cells("clean close")),
            )
            .unwrap();
        assert_eq!(ahead_current_row_count(&mut node, "todos"), 1);
        node.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalTime(1)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
        assert_eq!(ahead_current_row_count(&mut node, "todos"), 0);
        node.close().unwrap();
        node.close().unwrap();
    }

    reset_query_versions_for_tx_call_count();
    let mut reopened = reopen_node_at(&temp_dir, node(1), schema);
    assert_eq!(
        query_versions_for_tx_call_count(),
        0,
        "clean close marker should skip crash-only ahead-current sweep"
    );
    assert_eq!(ahead_current_row_count(&mut reopened, "todos"), 0);
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(13), title_cells("clean close"))])
    );
}

#[test]
fn unclean_reopen_skips_fated_sweep_through_consistency_marker() {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    {
        let mut node = open_node_at(&temp_dir, schema.clone());
        let tx_id = node
            .commit_mergeable_settled(
                MergeableCommit::new("todos", row(14), 10)
                    .cells(title_cells("periodic marker")),
            )
            .unwrap();
        assert_eq!(ahead_current_row_count(&mut node, "todos"), 1);
        node.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalTime(1)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
        assert_eq!(ahead_current_row_count(&mut node, "todos"), 0);
    }

    reset_query_versions_for_tx_call_count();
    let mut reopened = reopen_node_at(&temp_dir, node(1), schema);
    assert_eq!(
        query_versions_for_tx_call_count(),
        0,
        "periodic consistency marker should skip crash-only ahead-current sweep"
    );
    assert_eq!(ahead_current_row_count(&mut reopened, "todos"), 0);
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(14), title_cells("periodic marker"))])
    );
}

#[test]
fn unclean_reopen_sweeps_only_transactions_after_consistency_marker() {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    {
        let mut node = open_node_at(&temp_dir, schema.clone());
        for offset in 0_u8..20 {
            let tx_id = node
                .commit_mergeable_settled(
                    MergeableCommit::new("todos", row(100 + offset), 10 + u64::from(offset))
                        .cells(title_cells("before marker")),
                )
                .unwrap();
            node.apply_fate_update(
                tx_id,
                Fate::Accepted,
                Some(GlobalTime(1 + u64::from(offset))),
                Some(DurabilityTier::Global),
            )
            .unwrap();
        }
        assert_eq!(ahead_current_row_count(&mut node, "todos"), 0);

        let crash_tx = node
            .commit_mergeable_settled(
                MergeableCommit::new("todos", row(200), 1000)
                    .cells(title_cells("after marker crash window")),
            )
            .unwrap();
        mark_accepted_without_ahead_cleanup(&mut node, crash_tx, GlobalTime(1000));
        assert_eq!(ahead_current_row_count(&mut node, "todos"), 1);
    }

    reset_query_versions_for_tx_call_count();
    let mut reopened = reopen_node_at(&temp_dir, node(1), schema);
    assert_eq!(
        query_versions_for_tx_call_count(),
        1,
        "recovery should sweep only fated transactions newer than the marker"
    );
    assert_eq!(ahead_current_row_count(&mut reopened, "todos"), 0);
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .filter(|(row_uuid, _)| *row_uuid == row(200))
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(200), title_cells("after marker crash window"))])
    );
}

#[test]
fn recovery_rebuilds_only_pending_parent_edges_and_prunes_on_acceptance() {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    let parent;
    let child;
    {
        let mut node = open_node_at(&temp_dir, schema.clone());
        let tx = OpenTransactionId::new();
        node.open_exclusive(tx).unwrap();
        node.tx_write(tx, "todos", row(1), title_cells("parent"), None)
            .unwrap();
        let (parent_tx, _unit) = node.commit_exclusive_settled(tx, AuthorSubject::SYSTEM, 10).unwrap();
        parent = parent_tx;
        child = node
            .commit_mergeable_settled(
                MergeableCommit::new("todos", row(2), 11)
                    .parents(vec![parent])
                    .cells(title_cells("child")),
            )
            .unwrap();
        assert_eq!(
            node.rejections.child_txs_by_parent.get(&parent),
            Some(&BTreeSet::from([child]))
        );
    }

    let mut reopened = reopen_node_at(&temp_dir, node(1), schema);
    assert_eq!(
        reopened.rejections.child_txs_by_parent.get(&parent),
        Some(&BTreeSet::from([child]))
    );
    reopened
        .apply_fate_update(
            parent,
            Fate::Accepted,
            Some(GlobalTime(1)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
    assert!(reopened.rejections.child_txs_by_parent.is_empty());
}

fn mark_accepted_without_ahead_cleanup<S>(node: &mut NodeState<S>, tx_id: TxId, global_time: GlobalTime)
where
    S: OrderedKvStorage,
{
    let mut stored = node.query_transaction(tx_id).unwrap().unwrap();
    stored.fate = Fate::Accepted;
    stored.global_time = Some(global_time);
    stored.durability = DurabilityTier::Global;
    let version = node.query_versions_for_tx(tx_id).unwrap().remove(0);
    let mut batch = node.database.open_batch();
    batch.update(
        "jazz_transactions",
        transaction_values(
            stored.node_alias,
            &stored.tx,
            stored.fate.clone(),
            stored.global_time,
            stored.durability,
            node.contribution_merge_storage_value(stored.tx.contribution_merge.as_ref())
                .unwrap(),
        ),
    );
    node.write_global_current_update(&mut batch, &version, global_time)
        .unwrap();
    let applied = crate::db::block_on(node.database.apply_batch(batch)).unwrap();
let persisted = crate::db::block_on(applied.persist());
node.database.finish_persistence(persisted).unwrap();
}

#[test]
fn recovery_rebuilds_global_clock_from_accepted_transactions() {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    {
        let mut node = open_history_complete_node_at(&temp_dir, schema.clone());
        let first = node
            .commit_mergeable_settled(
                MergeableCommit::new("todos", row(1), 10).cells(title_cells("first")),
            )
            .unwrap();
        let first_global_time = node.allocate_global_time_for_test();
        node.apply_fate_update(
            first,
            Fate::Accepted,
            Some(first_global_time),
            Some(DurabilityTier::Global),
        )
        .unwrap();
        let second = node
            .commit_mergeable_settled(
                MergeableCommit::new("todos", row(2), 11).cells(title_cells("second")),
            )
            .unwrap();
        let second_global_time = node.allocate_global_time_for_test();
        node.apply_fate_update(
            second,
            Fate::Accepted,
            Some(second_global_time),
            Some(DurabilityTier::Global),
        )
        .unwrap();
        assert_eq!(node.clock.committed_global_time, second_global_time);
        assert_eq!(node.clock.global_time_register, second_global_time);
    }

    let reopened = reopen_history_complete_node_at(&temp_dir, node(1), schema);
    assert_eq!(
        reopened.clock.committed_global_time,
        GlobalTime::new(11, 0).unwrap()
    );
    assert_eq!(reopened.clock.global_time_register, reopened.clock.committed_global_time);
}

// This must remain an internal regression: reaching the last sequence requires
// planting the authority clock at u64::MAX, which ordinary public writes cannot
// feasibly do. It proves the boundary itself, while the recovery test below
// proves that a persisted last sequence retains the exhausted state on reopen.
#[test]
fn global_time_allocates_max_once_then_stays_exhausted() {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    let mut node = open_node_at(&temp_dir, schema);
    node.clock.global_time_register = GlobalTime(u64::MAX - 1);

    let last_tx = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(22), 22).cells(title_cells("last sequence")),
        )
        .unwrap();
    node.finalize_local_mergeable_commit_settled(last_tx).unwrap();
    assert_eq!(
        node.transaction_state_settled(last_tx).unwrap(),
        (
            Fate::Accepted,
            Some(GlobalTime(u64::MAX)),
            DurabilityTier::Global,
        )
    );
    assert_eq!(node.clock.global_time_register, GlobalTime(u64::MAX));

    let after_exhaustion = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(23), 23).cells(title_cells("after exhaustion")),
        )
        .unwrap();
    assert!(matches!(
        node.finalize_local_mergeable_commit_settled(after_exhaustion),
        Err(Error::ClockOverflow(crate::time::HlcOverflow {
            physical_ms: crate::time::HLC_MAX_PHYSICAL_MS,
        }))
    ));
    assert_eq!(
        node.transaction_state_settled(after_exhaustion).unwrap(),
        (Fate::Pending, None, DurabilityTier::Local)
    );
}

// This must remain an internal recovery regression: planting a persisted
// u64::MAX sequence is not reachable through the public allocation API.
#[test]
fn recovery_rejects_global_time_allocation_after_max() {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    {
        let mut node = open_node_at(&temp_dir, schema.clone());
        let tx_id = node
            .commit_mergeable_settled(
                MergeableCommit::new("todos", row(24), 24).cells(title_cells("last sequence")),
            )
            .unwrap();
        mark_accepted_without_ahead_cleanup(&mut node, tx_id, GlobalTime(u64::MAX));
    }

    let mut reopened = reopen_node_at(&temp_dir, node(1), schema);
    assert_eq!(
        reopened.clock.applied_global_times_after_frontier,
        BTreeSet::from([GlobalTime(u64::MAX)])
    );
    assert_eq!(reopened.clock.global_time_register, GlobalTime(u64::MAX));

    let next_tx = reopened
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(25), 25).cells(title_cells("after exhaustion")),
        )
        .unwrap();
    assert!(matches!(
        reopened.finalize_local_mergeable_commit_settled(next_tx),
        Err(Error::ClockOverflow(crate::time::HlcOverflow {
            physical_ms: crate::time::HLC_MAX_PHYSICAL_MS,
        }))
    ));
}

#[test]
fn reopen_refuses_preexisting_sequenced_non_global_transaction() {
    // This is necessarily an internal recovery test: old receivers could
    // persist this malformed peer state before admission validation existed.
    // Opening must surface it, never rewrite the durable audit history.
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    {
        let mut node = open_node_at(&temp_dir, schema.clone());
        let tx_id = node
            .commit_mergeable_settled(
                MergeableCommit::new("todos", row(3), 10).cells(title_cells("persisted")),
            )
            .unwrap();
        let stored = node.query_transaction(tx_id).unwrap().unwrap();
        let mut batch = node.database.open_batch();
        batch.update(
            "jazz_transactions",
            transaction_values(
                stored.node_alias,
                &stored.tx,
                Fate::Accepted,
                Some(GlobalTime(7)),
                DurabilityTier::Edge,
                node.contribution_merge_storage_value(stored.tx.contribution_merge.as_ref())
                    .unwrap(),
            ),
        );
        let applied = crate::db::block_on(node.database.apply_batch(batch)).unwrap();
let persisted = crate::db::block_on(applied.persist());
node.database.finish_persistence(persisted).unwrap();
    }

    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    let error = match NodeState::new(node(1), schema, storage).resolve() {
        Ok(_) => panic!("reopen must refuse impossible persisted durability"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        Error::InvalidStoredValue("global timestamp requires Global durability")
    ));
}

// This is necessarily an internal mechanism regression test: the public API
// exposes the restored upload queue but not candidate-record work. It seeds
// transaction states through node ingest APIs, then compares the null-slice
// lookup against the former full-decode predicate.
fn pending_replay_fixture_transaction(tx_id: TxId, made_by: AuthorSubject) -> Transaction {
    Transaction {
        tx_id,
        kind: TxKind::Mergeable,
        n_total_writes: 0,
        made_by,
        permission_subject: None,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        user_metadata_json: None,
        contribution_merge: None,
    }
}

fn seed_pending_replay_state(
    node: &mut NodeState<RocksDbStorage>,
    tx_id: TxId,
    made_by: AuthorSubject,
    fate: Fate,
    global_time: Option<GlobalTime>,
    durability: DurabilityTier,
) {
    node.ingest_relay_commit_unit(pending_replay_fixture_transaction(tx_id, made_by), Vec::new())
        .unwrap();
    if !matches!(fate, Fate::Pending) || global_time.is_some() || durability != DurabilityTier::Local
    {
        node.apply_fate_update(tx_id, fate, global_time, Some(durability))
            .unwrap();
    }
}

fn legacy_pending_transaction_ids_for(
    node: &mut NodeState<RocksDbStorage>,
    local_node: NodeUuid,
    author: AuthorSubject,
) -> PendingTransactionScan {
    let mut scan = PendingTransactionScan::default();
    for tx_id in node.transaction_ids().unwrap() {
        scan.records_visited += 1;
        let transaction = node.query_transaction(tx_id).unwrap().unwrap();
        scan.full_transactions_decoded += 1;
        if transaction.tx.tx_id.node == local_node
            && transaction.tx.made_by == author
            && matches!(transaction.fate, Fate::Pending | Fate::Accepted)
            && transaction.durability < DurabilityTier::Global
        {
            scan.tx_ids.push(tx_id);
        }
    }
    scan.tx_ids.sort();
    scan
}

// Replay discovery is an internal storage boundary, so a delegating failpoint
// is the narrowest way to prove the public recovery API propagates an index
// scan failure instead of interpreting it as an empty replay set.
#[derive(Clone)]
struct FailReplayScanStorage {
    inner: MemoryStorage,
    fail_scans: Rc<Cell<bool>>,
}

impl FailReplayScanStorage {
    fn new(column_families: &[&str]) -> Self {
        Self {
            inner: MemoryStorage::new(column_families).expect("valid memory storage families"),
            fail_scans: Rc::new(Cell::new(false)),
        }
    }
}

impl OrderedKvStorage for FailReplayScanStorage {
    fn get(&self, cf: String, key: Vec<u8>) -> groove::storage::StorageFuture<'_, Result<Option<StorageValue>, groove::storage::Error>> {
        self.inner.get(cf, key)
    }

    fn put_if_absent(&self, cf: String, key: Vec<u8>, value: Vec<u8>) -> groove::storage::StorageFuture<'_, Result<Option<StorageValue>, groove::storage::Error>> {
        self.inner.put_if_absent(cf, key, value)
    }

    fn compare_and_delete(&self, cf: String, key: Vec<u8>, expected: Vec<u8>) -> groove::storage::StorageFuture<'_, Result<bool, groove::storage::Error>> {
        self.inner.compare_and_delete(cf, key, expected)
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> groove::storage::StorageFuture<'_, Result<(), groove::storage::Error>> {
        self.inner.set(cf, key, value)
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> groove::storage::StorageFuture<'_, Result<(), groove::storage::Error>> {
        self.inner.delete(cf, key)
    }

    fn scan(&self, request: groove::storage::ScanRequest) -> groove::storage::StorageFuture<'_, Result<groove::storage::StorageScan<'_>, groove::storage::Error>> {
        if self.fail_scans.replace(false) {
            return Box::pin(async { Err(groove::storage::Error::InvalidStorageLayout("injected replay index scan failure".to_owned())) });
        }
        self.inner.scan(request)
    }

    fn write_many(&self, operations: Vec<groove::storage::OwnedWriteOperation>) -> groove::storage::StorageFuture<'_, Result<(), groove::storage::Error>> {
        self.inner.write_many(operations)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.inner.column_family_names()
    }
}

impl ReopenableStorage for FailReplayScanStorage {
    fn reopen(self, column_families: Vec<String>) -> groove::storage::StorageFuture<'static, Result<Self, groove::storage::Error>> {
        Box::pin(async move {
            let Self { inner, fail_scans } = self;
            Ok(Self { inner: inner.reopen(column_families).await?, fail_scans })
        })
    }
}

#[test]
fn pending_replay_index_scan_failure_is_not_treated_as_empty() {
    let schema = schema();
    let column_families = schema.column_families();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = FailReplayScanStorage::new(&column_family_refs);
    let mut node_under_test = NodeState::new(node(1), schema, storage.clone()).unwrap();
    let tx_id = node_under_test
        .commit_mergeable_settled(MergeableCommit::new("todos", row(4), 10).cells(title_cells("pending")))
        .unwrap();

    storage.fail_scans.set(true);
    let error = node_under_test
        .pending_transaction_ids_for(node(1), AuthorSubject::SYSTEM)
        .unwrap_err();
    assert!(error.to_string().contains("injected replay index scan failure"));

    assert_eq!(
        node_under_test
            .pending_transaction_ids_for(node(1), AuthorSubject::SYSTEM)
            .unwrap(),
        vec![tx_id],
        "a failed discovery scan must not clear replayable state"
    );
}

#[test]
fn pending_replay_null_slice_is_a_superset_then_filters_fate_and_identity() {
    let (_dir, mut node_under_test) = open_node();
    let local_node = node(1);
    let local_author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let other_author = AuthorSubject::for_test_bytes([0xb2; 16]);
    let other_node = node(2);
    let states = [
        (local_node, local_author, Fate::Pending, None, DurabilityTier::Local),
        (local_node, local_author, Fate::Accepted, None, DurabilityTier::Edge),
        (
            local_node,
            local_author,
            Fate::Accepted,
            Some(GlobalTime(7)),
            DurabilityTier::Global,
        ),
        (
            local_node,
            local_author,
            Fate::Rejected(RejectionReason::AuthorizationDenied),
            None,
            DurabilityTier::Local,
        ),
        (local_node, other_author, Fate::Pending, None, DurabilityTier::Local),
        (other_node, local_author, Fate::Pending, None, DurabilityTier::Local),
    ];
    for (offset, (tx_node, author, fate, global_time, durability)) in states.into_iter().enumerate()
    {
        seed_pending_replay_state(
            &mut node_under_test,
            TxId::new(TxTime::from((offset + 1) as u64), tx_node),
            author,
            fate,
            global_time,
            durability,
        );
    }

    let legacy = legacy_pending_transaction_ids_for(&mut node_under_test, local_node, local_author);
    let null_slice = node_under_test
        .pending_transaction_scan_for(local_node, local_author)
        .unwrap();
    assert_eq!(null_slice.tx_ids, legacy.tx_ids);
    assert_eq!(null_slice.tx_ids.len(), 2);
    assert_eq!(null_slice.records_visited, 5);
    assert_eq!(null_slice.full_transactions_decoded, 0);
}

const SERVER_UNSETTLED_OTHER_IDENTITIES: usize = 256;
const SERVER_REJECTED_NULL_SEQUENCE: usize = 16;

fn pending_replay_lookup_work(settled_history: usize) -> (PendingTransactionScan, PendingTransactionScan) {
    let (_dir, mut node_under_test) = open_node();
    let local_node = node(1);
    let local_author = AuthorSubject::for_test_bytes([0xa1; 16]);
    for offset in 0..settled_history {
        seed_pending_replay_state(
            &mut node_under_test,
            TxId::new(TxTime::from((offset + 1) as u64), local_node),
            local_author,
            Fate::Accepted,
            Some(GlobalTime((offset + 1) as u64)),
            DurabilityTier::Global,
        );
    }
    for offset in 0..SERVER_UNSETTLED_OTHER_IDENTITIES {
        seed_pending_replay_state(
            &mut node_under_test,
            TxId::new(TxTime::from(10_000 + offset as u64), node(0x40 + (offset / 4) as u8)),
            AuthorSubject::for_test_bytes([offset as u8; 16]),
            Fate::Pending,
            None,
            DurabilityTier::Local,
        );
    }
    for offset in 0..SERVER_REJECTED_NULL_SEQUENCE {
        seed_pending_replay_state(
            &mut node_under_test,
            TxId::new(TxTime::from(20_000 + offset as u64), node(0xe0)),
            AuthorSubject::for_test_bytes([0xf0; 16]),
            Fate::Rejected(RejectionReason::AuthorizationDenied),
            None,
            DurabilityTier::Local,
        );
    }
    for (offset, fate, durability) in [
        (0, Fate::Pending, DurabilityTier::Local),
        (1, Fate::Accepted, DurabilityTier::Edge),
    ] {
        seed_pending_replay_state(
            &mut node_under_test,
            TxId::new(TxTime::from(30_000 + offset), local_node),
            local_author,
            fate,
            None,
            durability,
        );
    }
    let legacy = legacy_pending_transaction_ids_for(&mut node_under_test, local_node, local_author);
    let null_slice = node_under_test
        .pending_transaction_scan_for(local_node, local_author)
        .unwrap();
    (legacy, null_slice)
}

#[test]
fn pending_replay_null_slice_work_is_independent_of_settled_history() {
    let (legacy_empty, null_empty) = pending_replay_lookup_work(0);
    let (legacy_small, null_small) = pending_replay_lookup_work(8);
    let (legacy_large, null_large) = pending_replay_lookup_work(128);
    let server_null_slice = SERVER_UNSETTLED_OTHER_IDENTITIES + SERVER_REJECTED_NULL_SEQUENCE + 2;

    assert_eq!(null_empty.records_visited, server_null_slice);
    assert_eq!(null_small.records_visited, server_null_slice);
    assert_eq!(null_large.records_visited, server_null_slice);
    assert_eq!(null_empty.full_transactions_decoded, 0);
    assert_eq!(null_small.full_transactions_decoded, 0);
    assert_eq!(null_large.full_transactions_decoded, 0);
    assert_eq!(null_large.tx_ids.len(), 2);
    // The #1295 replay-state index would visit these two local candidates.
    // The existing full scan visits and reconstructs every retained record.
    assert_eq!(legacy_empty.records_visited, server_null_slice);
    assert_eq!(legacy_small.records_visited, server_null_slice + 8);
    assert_eq!(legacy_large.records_visited, server_null_slice + 128);
    assert_eq!(legacy_large.full_transactions_decoded, legacy_large.records_visited);
    assert!(legacy_large.records_visited > legacy_small.records_visited);
}

#[test]
fn reopen_replay_lookup_keeps_local_pending_write() {
    let schema = schema();
    let (node_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let tx_id = writer
        .commit_mergeable_settled(MergeableCommit::new("todos", row(4), 10).cells(title_cells("keep me")))
        .unwrap();
    drop(writer);

    let mut reopened = reopen_node_at(&node_dir, node(1), schema);
    assert_eq!(
        reopened.pending_transaction_ids_for(node(1), AuthorSubject::SYSTEM).unwrap(),
        vec![tx_id]
    );
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(4), title_cells("keep me"))])
    );
}

#[test]
fn reopen_replay_deduplicates_pending_ahead_current_keys_per_table_and_layer() {
    let schema = todos_notes_schema();
    let shared_row = row(0x4d);
    let (node_dir, mut writer) = open_node_with_schema(node(0x41), schema.clone());

    // One wire commit deliberately carries content and deletion records with
    // the same row identity and transaction identity. The raw keys therefore
    // coincide within a layer; only the physical table and layer distinguish
    // all four pending projections.
    let replay_tx = writer
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("todos", shared_row, 10).cells(title_cells("todo")),
            MergeableCommit::new("notes", shared_row, 10).cells(BTreeMap::from([(
                "body".to_owned(),
                v("note"),
            )])),
            MergeableCommit::new("todos", shared_row, 10).deletion(DeletionEvent::Deleted),
            MergeableCommit::new("notes", shared_row, 10).deletion(DeletionEvent::Deleted),
        ])
        .unwrap();
    let replay_unit = writer.commit_unit_for(replay_tx).unwrap();

    let mappings = &writer.catalogue.physical_mappings[&schema.version_id()].tables;
    let todos_id = mappings["todos"].table_id;
    let notes_id = mappings["notes"].table_id;
    assert_ne!(todos_id, notes_id);
    // This receipt intentionally inspects the private physical projection:
    // public rows cannot reveal whether exact pending replay duplicated it.
    for table_id in [todos_id, notes_id] {
        assert_eq!(
            writer
                .database
                .primary_key_scan_raw(&physical_ahead_current_table_name(table_id), &[])
                .unwrap()
                .len(),
            1,
        );
        assert_eq!(
            writer
                .database
                .primary_key_scan_raw(
                    &physical_register_ahead_current_table_name(table_id),
                    &[],
                )
                .unwrap()
                .len(),
            1,
        );
    }

    drop(writer);
    let mut reader = reopen_node_at(&node_dir, node(0x41), schema);
    let SyncMessage::CommitUnit { tx, versions } = replay_unit else {
        panic!("pending commit must have a wire unit");
    };
    // View/peer replay is not a fate authority: it re-ingests the exact wire
    // payload as pending and must leave the reconstructed projections intact.
    reader
        .ingest_known_transaction(tx, versions, Fate::Pending, None, DurabilityTier::Local)
        .resolve()
        .unwrap();
    for table_id in [todos_id, notes_id] {
        assert_eq!(ahead_current_row_count(&mut reader, if table_id == todos_id { "todos" } else { "notes" }), 2);
    }

    let distinct_tx = reader
        .commit_mergeable_settled(
            MergeableCommit::new("todos", shared_row, 20).cells(title_cells("distinct key")),
        )
        .unwrap();
    assert_eq!(ahead_current_row_count(&mut reader, "todos"), 3);
    assert_eq!(ahead_current_row_count(&mut reader, "notes"), 2);

    reader
        .apply_sync_message_settled(SyncMessage::FateUpdate {
            tx_id: distinct_tx,
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: None,
        })
        .unwrap();
    assert_eq!(ahead_current_row_count(&mut reader, "todos"), 2);
    assert_eq!(ahead_current_row_count(&mut reader, "notes"), 2);
}

#[test]
fn reopen_in_place_recovers_history_watermarks_pending_edges_and_rehydrates_peer() {
    let (_dir, mut core) = open_node_with_uuid(node(0x3a));
    let mut peer = PeerState::new();
    let accepted = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row(3), 9).cells(title_cells("accepted")))
        .unwrap();
    core.apply_fate_update(
        accepted,
        Fate::Accepted,
        Some(GlobalTime(7)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let parent_tx = OpenTransactionId::new();
    core.open_exclusive(parent_tx).unwrap();
    core.tx_write(parent_tx, "todos", row(1), title_cells("parent"), None)
        .unwrap();
    let (parent, _unit) = core
        .commit_exclusive_settled(parent_tx, AuthorSubject::SYSTEM, 10)
        .unwrap();
    let child = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(2), 11)
                .parents(vec![parent])
                .cells(title_cells("child")),
        )
        .unwrap();
    let update = peer.current_rows_update(&mut core, "todos").unwrap();
    assert!(matches!(update, SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { .. })));

    let mut reopened = core.reopen_in_place().unwrap();
    assert_eq!(
        reopened.transaction_state_settled(accepted).unwrap(),
        (Fate::Accepted, Some(GlobalTime(7)), DurabilityTier::Global)
    );
    assert_eq!(
        reopened.rejections.child_txs_by_parent.get(&parent),
        Some(&BTreeSet::from([child]))
    );
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(3), title_cells("accepted"))])
    );

    let rehydrated = peer.rehydrate_current_rows(&mut reopened, "todos").unwrap();
    assert!(matches!(rehydrated, SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { .. })));
}
#[test]
fn empty_string_cells_and_absent_cells_survive_restart() {
    let schema = two_column_schema();
    let (node_dir, mut local_node) = open_node_with_schema(node(1), schema.clone());
    let empty_row = row(1);
    let absent_row = row(2);

    local_node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", empty_row, 10).cells(title_cells(String::new())),
        )
        .unwrap();
    local_node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", absent_row, 11)
                .cells(BTreeMap::from([("body".to_owned(), "body".to_owned())])),
        )
        .unwrap();
    let expected = BTreeMap::from([
        (empty_row, title_cells(String::new())),
        (absent_row, BTreeMap::from([("body".to_owned(), v("body"))])),
    ]);
    assert_eq!(
        local_node
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        expected
    );

    drop(local_node);
    let mut reopened = reopen_node_at(&node_dir, node(1), schema);
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        expected
    );
}
#[test]
fn empty_string_cells_survive_restart_in_core_merge_version() {
    let schema = two_column_schema();
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(1), schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(2), schema.clone());
    let (core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let merged_row = row(7);

    let left = writer_a
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", merged_row, 10).cells(title_cells(String::new())),
        )
        .unwrap()
        .1;
    let right = writer_b
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", merged_row, 11)
                .cells(BTreeMap::from([("body".to_owned(), "body".to_owned())])),
        )
        .unwrap()
        .1;
    core.apply_sync_message_settled(left).unwrap();
    core.apply_sync_message_settled(right).unwrap();

    let expected = vec![(
        merged_row,
        BTreeMap::from([
            ("title".to_owned(), v(String::new())),
            ("body".to_owned(), v("body")),
        ]),
    )];
    assert_eq!(
        core.current_rows("todos", DurabilityTier::Global).unwrap(),
        expected
    );

    drop(core);
    let mut reopened = reopen_node_at(&core_dir, node(9), schema);
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Global)
            .unwrap(),
        expected
    );
}
#[test]
fn persisted_currency_tables_match_history_rows_after_reopen() {
    let schema = two_column_schema();
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(1), schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(2), schema.clone());
    let (core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let merged_row = row(7);

    let left = writer_a
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", merged_row, 10).cells(title_cells(String::new())),
        )
        .unwrap()
        .1;
    let right = writer_b
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", merged_row, 11)
                .cells(BTreeMap::from([("body".to_owned(), "body".to_owned())])),
        )
        .unwrap()
        .1;
    core.apply_sync_message_settled(left).unwrap();
    core.apply_sync_message_settled(right).unwrap();
    assert_currency_tables_match_storage(&mut core, "todos");

    drop(core);
    let mut reopened = reopen_node_at(&core_dir, node(9), schema);
    assert_currency_tables_match_storage(&mut reopened, "todos");
}
#[test]
fn recovery_ignores_foreign_tx_ids_when_restoring_next_own_ingest_seq() {
    let schema = schema();
    let (node_dir, mut node_a) = open_node_with_schema(node(1), schema.clone());
    let own = node_a
        .commit_mergeable_settled(MergeableCommit::new("todos", row(1), 10).cells(title_cells("own")))
        .unwrap();
    assert_eq!(own.time, TxTime::from(10));

    let foreign = TxId::new(TxTime::from(500), node(2));
    node_a
        .ingest_relay_commit_unit(
            Transaction {
                tx_id: foreign,
                kind: TxKind::Mergeable,
                n_total_writes: 1,
                made_by: AuthorSubject::SYSTEM,
                permission_subject: None,
                base_snapshot: None,
                row_read_set: None,
                absent_read_set: None,
                predicate_read_set: None,
                user_metadata_json: None,
                contribution_merge: None,
            },
            vec![version_record(
                row(2),
                Vec::new(),
                title_cells("foreign"),
                None,
            )],
        )
        .unwrap();

    drop(node_a);
    let mut reopened = reopen_node_at(&node_dir, node(1), schema);
    let next_own = reopened
        .commit_mergeable_settled(MergeableCommit::new("todos", row(3), 12).cells(title_cells("next")))
        .unwrap();
    assert_eq!(next_own.time, TxTime::new(500, 1));
}
#[test]
fn row_history_reports_versions_flags_and_audit_records_across_restart() {
    let (_writer_a_dir, mut writer_a) = open_node_with_uuid(node(1));
    let (_writer_b_dir, mut writer_b) = open_node_with_uuid(node(2));
    let (core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);

    let left = commit_mergeable_global(
        &mut writer_a,
        &mut core,
        MergeableCommit::new("todos", row, 10).cells(title_cells("left")),
    );
    let right = commit_mergeable_global(
        &mut writer_b,
        &mut core,
        MergeableCommit::new("todos", row, 11).cells(title_cells("right")),
    );
    let deleted = commit_mergeable_global(
        &mut writer_a,
        &mut core,
        MergeableCommit::new("todos", row, 20).deletion(DeletionEvent::Deleted),
    );
    let restored = commit_mergeable_global(
        &mut writer_a,
        &mut core,
        MergeableCommit::new("todos", row, 21).deletion(DeletionEvent::Restored),
    );

    let tx_id = OpenTransactionId::new();
    core.open_exclusive(tx_id).unwrap();
    core.tx_read(tx_id, "todos", row).unwrap();
    core.tx_write(tx_id, "todos", row, title_cells("exclusive"), None)
        .unwrap();
    let (exclusive, _unit) = core.commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 30).unwrap();
    let exclusive_global_time = core.allocate_global_time_for_test();
    core.apply_fate_update(
        exclusive,
        Fate::Accepted,
        Some(exclusive_global_time),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    sync_current_rows_to(&mut core, &mut writer_b, 43);
    let rejected_tx = OpenTransactionId::new();
    writer_b.open_exclusive(rejected_tx).unwrap();
    writer_b.tx_read(rejected_tx, "todos", row).unwrap();
    commit_mergeable_global(
        &mut writer_a,
        &mut core,
        MergeableCommit::new("todos", row, 40).cells(BTreeMap::from([(
            "title".to_owned(),
            "intervening".to_owned(),
        )])),
    );
    writer_b
        .tx_write(rejected_tx, "todos", row, title_cells("rejected"), None)
        .unwrap();
    let (rejected, unit) = writer_b
        .commit_exclusive_settled(rejected_tx, AuthorSubject::SYSTEM, 41)
        .unwrap();
    let [fate] = core.apply_sync_message_settled(unit).unwrap().try_into().unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id: rejected,
            fate: Fate::Rejected(RejectionReason::ExclusiveConflict),
            global_time: None,
            durability: None,
        }
    );

    let history = core.row_history("todos", row).unwrap();
    assert!(history
        .windows(2)
        .all(|pair| pair[0].tx_id().time.sort_key(pair[0].tx_id().node)
            <= pair[1].tx_id().time.sort_key(pair[1].tx_id().node)));
    assert!(history.iter().any(|entry| entry.tx_id() == left));
    assert!(history.iter().any(|entry| entry.tx_id() == right));
    assert!(history.iter().any(|entry| {
        entry.tx_id().node == node(9)
            && entry.parents().contains(&left)
            && entry.parents().contains(&right)
            && entry.layer() == MergeAspect::Content
            && entry.fate() == Fate::Accepted
            && entry.global_time().is_some()
            && entry.durability() == DurabilityTier::Global
    }));
    assert!(history.iter().any(|entry| {
        entry.tx_id() == deleted
            && entry.layer() == MergeAspect::Deletion
            && entry.deletion() == Some(DeletionEvent::Deleted)
            && !entry.is_locally_current()
            && !entry.is_globally_current()
    }));
    assert!(history.iter().any(|entry| {
        entry.tx_id() == restored
            && entry.layer() == MergeAspect::Deletion
            && entry.deletion() == Some(DeletionEvent::Restored)
            && entry.is_locally_current()
            && entry.is_globally_current()
    }));
    assert!(history.iter().any(|entry| {
        entry.tx_id() == exclusive
            && entry.kind() == TxKind::Exclusive
            && entry.made_by() == AuthorSubject::SYSTEM
            && entry.cell(&schema().tables[0], "title") == Some(v("exclusive"))
            && entry.parents().len() == 1
    }));
    assert!(!history.iter().any(|entry| entry.tx_id() == rejected));
    assert_eq!(
        core.transaction_record(rejected).unwrap().fate,
        Fate::Rejected(RejectionReason::ExclusiveConflict)
    );

    drop(core);
    let mut reopened = reopen_node_at(&core_dir, node(9), schema());
    assert_eq!(reopened.row_history("todos", row).unwrap(), history);
    assert_eq!(
        reopened.transaction_record(rejected).unwrap().fate,
        Fate::Rejected(RejectionReason::ExclusiveConflict)
    );
}
#[test]
fn transaction_metadata_round_trips_through_recovery() {
    let (dir, mut local_node) = open_node_with_uuid(node(1));
    let row = row(7);
    let merge = local_node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row, 10)
                .cells(title_cells("merge"))
                .user_metadata(r#"{"source":"merge"}"#.to_owned()),
        )
        .unwrap();

    let tx_id = OpenTransactionId::new();
    local_node.open_exclusive(tx_id).unwrap();
    local_node
        .tx_set_metadata(tx_id, r#"{"source":"exclusive"}"#.to_owned())
        .unwrap();
    local_node
        .tx_write(tx_id, "todos", row, title_cells("exclusive"), None)
        .unwrap();
    let (exclusive, _) = local_node
        .commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 11)
        .unwrap();

    assert_eq!(
        local_node
            .transaction_record(merge)
            .unwrap()
            .user_metadata_json,
        Some(r#"{"source":"merge"}"#.to_owned())
    );
    assert_eq!(
        local_node
            .transaction_record(exclusive)
            .unwrap()
            .user_metadata_json,
        Some(r#"{"source":"exclusive"}"#.to_owned())
    );

    drop(local_node);
    let mut reopened = reopen_node_at(&dir, node(1), schema());
    assert_eq!(
        reopened
            .transaction_record(merge)
            .unwrap()
            .user_metadata_json,
        Some(r#"{"source":"merge"}"#.to_owned())
    );
    assert_eq!(
        reopened
            .transaction_record(exclusive)
            .unwrap()
            .user_metadata_json,
        Some(r#"{"source":"exclusive"}"#.to_owned())
    );
}
