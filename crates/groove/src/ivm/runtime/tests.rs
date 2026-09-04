use super::*;
use crate::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey,
};
use crate::storage::{MemoryStorage, OwnedStorage, RecordStore, TestStorage, TestStorageOperation};
use std::rc::Rc;
use std::task::{Context, Poll};

#[futures_test::test]
async fn terminal_collect_canonicalization_emits_net_remove_before_net_insert() {
    let record = |label: u8| Bytes::from(vec![label]);
    let keyed = |record: Bytes, weight| (vec![1], Vec::new(), record, weight);

    // An update followed by deletion of its post-image is a final delete,
    // not an ambiguous replacement. An intermediate update that cancels
    // within the batch disappears, while independent roots stay ordered.
    let canonical = canonicalize_collect_by_terminal_weights(vec![
        keyed(record(1), -1),
        keyed(record(2), 1),
        keyed(record(2), -1),
        keyed(record(3), 1),
        (vec![2], Vec::new(), record(4), 1),
        (vec![2], Vec::new(), record(4), -1),
    ]);
    assert_eq!(
        canonical,
        vec![
            (vec![1], Vec::new(), record(1), -1),
            (vec![1], Vec::new(), record(3), 1)
        ]
    );

    // Grouped/interleaved multiple replacements retain only the final
    // post-image and put the pre-image removal before it.
    let replacements = canonicalize_collect_by_terminal_weights(vec![
        keyed(record(1), -1),
        keyed(record(2), 1),
        keyed(record(2), -1),
        keyed(record(3), 1),
    ]);
    assert_eq!(
        replacements,
        vec![
            (vec![1], Vec::new(), record(1), -1),
            (vec![1], Vec::new(), record(3), 1)
        ]
    );
}

#[futures_test::test]
async fn raw_variant_case_registry_refresh_rejects_projection_replacement() {
    // A live physical registry may append a case, but refreshing its raw
    // source descriptor must not become a back door for changing how an
    // already-installed case maps its fields.
    let descriptor = |variants: &[&str]| {
        RecordDescriptor::new([(
            "status",
            ValueType::EnumTag(
                records::ScalarEnumSchema::new("status", variants.iter().copied())
                    .unwrap()
                    .with_registry_id(0x71),
            ),
        )])
    };
    let case = |source, mapping| VariantProjectionCase::Project {
        source,
        project: MapProjectOp {
            expressions: Vec::new(),
            mapping,
        },
        raw_projection: None,
        omit_unrepresentable_enum_rows: false,
    };
    let current = case(descriptor(&["open"]), vec![(0, 0)]);
    let appended = case(descriptor(&["open", "closed"]), vec![(0, 0)]);
    assert!(current.can_refresh_registries_to(&appended));

    let replaced_mapping = case(descriptor(&["open", "closed"]), Vec::new());
    assert!(
        !current.can_refresh_registries_to(&replaced_mapping),
        "registry refresh must not replace an existing projection mapping"
    );
    let incompatible_type = case(
        RecordDescriptor::new([("status", ValueType::String)]),
        vec![(0, 0)],
    );
    assert!(
        !current.can_refresh_registries_to(&incompatible_type),
        "registry refresh must reject a field-type mutation"
    );
}

#[futures_test::test]
async fn payload_enum_remap_changes_only_the_case_tag() {
    let descriptor = RecordDescriptor::new([("value", ValueType::String)]);
    let value = Value::Enum(
        EnumValue::create(2, descriptor, &[Value::String("later".to_owned())]).unwrap(),
    );
    let remapped = remap_enum(value, &[Some(0), Some(1), Some(3)]).unwrap();
    let Value::Enum(remapped) = remapped else {
        panic!("expected payload enum");
    };
    assert_eq!(remapped.tag(), 3);
    assert_eq!(
        remapped.record().to_values().unwrap(),
        vec![Value::String("later".to_owned())]
    );
}

#[futures_test::test]
async fn recursive_enum_projection_reencodes_nullable_array_tuple_and_record_occurrences() {
    let physical = ValueType::EnumTag(
        records::ScalarEnumSchema::new("physical", ["draft", "snoozed", "archived"]).unwrap(),
    );
    let authored = ValueType::EnumTag(
        records::ScalarEnumSchema::new("authored", ["draft", "archived"]).unwrap(),
    );
    let source = ValueType::Tuple(vec![
        ValueType::Nullable(Box::new(ValueType::Array(Box::new(physical.clone())))),
        ValueType::Record(Box::new(RecordDescriptor::new([("state", physical)]))),
    ]);
    let target = ValueType::Tuple(vec![
        ValueType::Nullable(Box::new(ValueType::Array(Box::new(authored.clone())))),
        ValueType::Record(Box::new(RecordDescriptor::new([("state", authored)]))),
    ]);
    let remaps = RecursiveEnumRemaps {
        scalar: BTreeMap::from([
            (
                "root/tuple/0/nullable/array".to_owned(),
                vec![Some(0), None, Some(1)],
            ),
            (
                "root/tuple/1/record/state".to_owned(),
                vec![Some(0), None, Some(1)],
            ),
        ]),
        payload: BTreeMap::new(),
        payload_children: BTreeMap::new(),
    };
    let record = RecordDescriptor::new([(
        "state",
        ValueType::EnumTag(
            records::ScalarEnumSchema::new("physical", ["draft", "snoozed", "archived"]).unwrap(),
        ),
    )]);
    let value = Value::Tuple(vec![
        Value::Nullable(Some(Box::new(Value::Array(vec![Value::EnumTag(2)])))),
        Value::Record(OwnedRecord::new(
            record.create(&[Value::EnumTag(2)]).unwrap(),
            record,
        )),
    ]);
    let projected = remap_recursive_enum_value(value, &source, &target, &remaps, "root").unwrap();
    let Value::Tuple(values) = projected else {
        panic!("tuple expected")
    };
    assert_eq!(
        values[0],
        Value::Nullable(Some(Box::new(Value::Array(vec![Value::EnumTag(1)]))))
    );
    let Value::Record(record) = &values[1] else {
        panic!("record expected")
    };
    assert_eq!(record.to_values().unwrap(), vec![Value::EnumTag(1)]);
}

#[futures_test::test]
async fn recursive_enum_projection_remaps_payload_cases_and_nested_payload_enums() {
    let physical_scalar = ValueType::EnumTag(
        records::ScalarEnumSchema::new("physical", ["draft", "snoozed", "archived"]).unwrap(),
    );
    let authored_scalar = ValueType::EnumTag(
        records::ScalarEnumSchema::new("authored", ["draft", "archived"]).unwrap(),
    );
    let physical_payload = ValueType::Enum(Box::new(
        EnumSchema::new(
            "physical-payload",
            [
                records::EnumCase::new(
                    "case-a",
                    RecordDescriptor::new([("state", physical_scalar.clone())]),
                ),
                records::EnumCase::new(
                    "case-b",
                    RecordDescriptor::new(Vec::<(String, ValueType)>::new()),
                ),
            ],
        )
        .unwrap(),
    ));
    let authored_payload = ValueType::Enum(Box::new(
        EnumSchema::new(
            "authored-payload",
            [records::EnumCase::new(
                "case-a",
                RecordDescriptor::new([("state", authored_scalar)]),
            )],
        )
        .unwrap(),
    ));
    let source = ValueType::Record(Box::new(RecordDescriptor::new([(
        "nested",
        physical_payload.clone(),
    )])));
    let target = ValueType::Record(Box::new(RecordDescriptor::new([(
        "nested",
        authored_payload.clone(),
    )])));
    let remaps = RecursiveEnumRemaps {
        scalar: BTreeMap::from([(
            "root/record/nested/case/introduced-a/0/record/state".to_owned(),
            vec![Some(0), None, Some(1)],
        )]),
        payload: BTreeMap::from([("root/record/nested".to_owned(), vec![Some(0), None])]),
        // The payload's physical tag is an interned local value.  Its
        // descendant path must instead remain rooted in the durable case
        // identity, so a concurrent schema may use the same local tag for
        // an unrelated case without redirecting this scalar remap.
        payload_children: BTreeMap::from([(
            "root/record/nested".to_owned(),
            vec![
                Some("root/record/nested/case/introduced-a/0".to_owned()),
                None,
            ],
        )]),
    };
    let ValueType::Enum(payload_schema) = physical_payload else {
        panic!("payload expected")
    };
    let payload = payload_schema.case(0).unwrap().payload;
    let nested = Value::Enum(EnumValue::create(0, payload, &[Value::EnumTag(2)]).unwrap());
    let ValueType::Record(source_record) = &source else {
        panic!("record expected")
    };
    let value = Value::Record(OwnedRecord::new(
        source_record.create(&[nested]).unwrap(),
        **source_record,
    ));
    let projected = remap_recursive_enum_value(value, &source, &target, &remaps, "root").unwrap();
    let Value::Record(record) = projected else {
        panic!("record expected")
    };
    let Value::Enum(nested) = record.to_values().unwrap().pop().unwrap() else {
        panic!("enum expected")
    };
    assert_eq!(nested.tag(), 0);
    assert_eq!(
        nested.record().to_values().unwrap(),
        vec![Value::EnumTag(1)]
    );

    // A physical case which the target schema cannot name must fail rather
    // than becoming a default case or silently removing the row.
    let unknown = Value::Enum(
        EnumValue::create(
            1,
            RecordDescriptor::new(Vec::<(String, ValueType)>::new()),
            &[],
        )
        .unwrap(),
    );
    let value = Value::Record(OwnedRecord::new(
        source_record.create(&[unknown]).unwrap(),
        **source_record,
    ));
    assert!(matches!(
        remap_recursive_enum_value(value, &source, &target, &remaps, "root"),
        Err(IvmRuntimeError::EnumProjectionAbsent { tag: 1 })
    ));
}

#[futures_test::test]
async fn collect_by_terminal_records_preserve_nullable_descriptor_wrappers() {
    let uuid = uuid::Uuid::from_bytes([7; 16]);

    assert_eq!(
        collect_by_output_value(
            &ValueType::Nullable(Box::new(ValueType::I32)),
            Value::I32(3),
        ),
        Value::Nullable(Some(Box::new(Value::I32(3))))
    );
    assert_eq!(
        collect_by_output_value(
            &ValueType::Nullable(Box::new(ValueType::Uuid)),
            Value::Uuid(uuid),
        ),
        Value::Nullable(Some(Box::new(Value::Uuid(uuid))))
    );
    assert_eq!(
        collect_by_output_value(
            &ValueType::Nullable(Box::new(ValueType::I32)),
            Value::Nullable(None),
        ),
        Value::Nullable(None)
    );
}

#[futures_test::test]
async fn root_ordering_rewrites_insert_indices_and_emits_moves_after_payload_edits() {
    // Internal coverage is intentional: the public browser matrix proves
    // end-to-end ordering, while this pins the terminal operation protocol
    // ordering that is not otherwise observable through the public API.
    let key = |byte| vec![byte];
    let before = BTreeMap::from([(key(1), 0), (key(2), 1), (key(3), 2)]);
    let after = BTreeMap::from([(key(3), 0), (key(4), 1), (key(1), 2), (key(2), 3)]);
    let mut terminal = TerminalDeltas {
        operations: vec![
            TerminalOperation {
                root_descriptor: RecordDescriptor::default(),
                root_key: key(2),
                path: Vec::new(),
                edit: TerminalEdit::Update {
                    key: key(2),
                    value: vec![22],
                },
            },
            TerminalOperation {
                root_descriptor: RecordDescriptor::default(),
                root_key: key(4),
                path: Vec::new(),
                edit: TerminalEdit::Insert {
                    index: 0,
                    key: key(4),
                    value: vec![44],
                },
            },
        ],
    };

    apply_root_ordering_operations(&before, &after, RecordDescriptor::default(), &mut terminal);

    assert!(matches!(
        terminal.operations[1].edit,
        TerminalEdit::Insert { index: 1, .. }
    ));
    assert!(matches!(
        terminal.operations[0].edit,
        TerminalEdit::Update { .. }
    ));
    assert_eq!(
        terminal.operations[2..]
            .iter()
            .map(|operation| match &operation.edit {
                TerminalEdit::Move { key, index } => (key.clone(), *index),
                edit => panic!("expected root move after payload edits, got {edit:?}"),
            })
            .collect::<Vec<_>>(),
        vec![(key(3), 0), (key(4), 1)]
    );
}

#[futures_test::test]
async fn root_ordering_emits_moves_without_payload_terminal_edits() {
    // A policy-scope re-entry can reorder visible roots without any
    // payload edit. The subscription output descriptor, rather than a
    // payload operation, supplies the descriptor for the following move.
    let key = |byte| vec![byte];
    let before = BTreeMap::from([(key(1), 0), (key(2), 1)]);
    let after = BTreeMap::from([(key(2), 0), (key(1), 1)]);
    let descriptor = RecordDescriptor::new([("id", ValueType::U64)]);
    let mut terminal = TerminalDeltas {
        operations: Vec::new(),
    };

    apply_root_ordering_operations(&before, &after, descriptor, &mut terminal);

    assert_eq!(
        terminal.operations,
        vec![TerminalOperation {
            root_descriptor: descriptor,
            root_key: key(2),
            path: Vec::new(),
            edit: TerminalEdit::Move {
                key: key(2),
                index: 0,
            },
        }]
    );
}

fn albums_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

fn indexed_albums_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("albums_by_title", ["title"]))])
}

fn albums_artists_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "albums",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("artist_id", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "artists",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("name", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

fn edges_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "edges",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("src", ColumnType::U64),
            ColumnSchema::new("dst", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

fn indexed_edges_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "edges",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("src", ColumnType::U64),
            ColumnSchema::new("dst", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("by_src", ["src"]))])
}

fn reach_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([
        ("src", ColumnType::U64.clone()),
        ("dst", ColumnType::U64.clone()),
    ])
}

#[futures_test::test]
async fn top_by_distinguishes_finite_max_from_unbounded_limit() {
    // Direct helper coverage is intentional: constructing more than
    // u64::MAX derivations through public tables is not feasible, while
    // synthetic weights exercise the semantic boundary without expanding
    // multiplicity into individual records.
    let descriptor = RecordDescriptor::new([("id", ValueType::U64)]);
    let records = [1, 2, 3]
        .into_iter()
        .map(|id| {
            (
                Bytes::from(descriptor.create(&[Value::U64(id)]).unwrap()),
                i64::MAX,
            )
        })
        .collect::<Vec<_>>();
    let top_by = |limit| TopByOp {
        group_fields: Vec::new(),
        group_field_indices: Vec::new(),
        order_fields: vec![TopByOrderField {
            field: "id".to_owned(),
            direction: TopByDirection::Asc,
        }],
        tie_fields: Vec::new(),
        sort_field_indices: vec![0],
        sort_directions: vec![TopByDirection::Asc],
        offset: 0,
        limit,
    };

    let finite = top_by_window_from_records(
        descriptor,
        records.clone(),
        &top_by(TopByLimit::Finite(u64::MAX)),
    )
    .unwrap();
    assert_eq!(
        finite
            .into_iter()
            .map(|(_, weight)| weight)
            .collect::<Vec<_>>(),
        [i64::MAX, i64::MAX, 1]
    );

    let unbounded =
        top_by_window_from_records(descriptor, records, &top_by(TopByLimit::Unbounded)).unwrap();
    assert_eq!(
        unbounded
            .into_iter()
            .map(|(_, weight)| weight)
            .collect::<Vec<_>>(),
        [i64::MAX, i64::MAX, i64::MAX]
    );
}

fn recursive_reach_graph() -> GraphBuilder {
    recursive_reach_graph_with_limit(16)
}

fn recursive_reach_graph_with_limit(max_iters: usize) -> GraphBuilder {
    let seed = GraphBuilder::table("edges").project(["src", "dst"]);
    let edge_pairs = GraphBuilder::table("edges").project(["src", "dst"]);
    let frontier = GraphBuilder::frontier_source("frontier", reach_descriptor());
    let step = GraphBuilder::join(frontier, edge_pairs, ["dst"], ["src"]).project_fields([
        crate::ivm::ProjectField::renamed("left.src", "src"),
        crate::ivm::ProjectField::renamed("right.dst", "dst"),
    ]);
    GraphBuilder::recursive(seed, step, "frontier", max_iters)
}

fn recursive_reach_with_renamed_inputs_graph() -> GraphBuilder {
    let descriptor = RecordDescriptor::new([("from", ColumnType::U64), ("to", ColumnType::U64)]);
    let seed = GraphBuilder::table("edges").project_fields([
        crate::ivm::ProjectField::renamed("src", "from"),
        crate::ivm::ProjectField::renamed("dst", "to"),
    ]);
    let edge_pairs = GraphBuilder::table("edges").project_fields([
        crate::ivm::ProjectField::renamed("src", "edge_from"),
        crate::ivm::ProjectField::renamed("dst", "edge_to"),
    ]);
    let frontier = GraphBuilder::frontier_source("frontier", descriptor);
    let step = GraphBuilder::join(frontier, edge_pairs, ["to"], ["edge_from"]).project_fields([
        crate::ivm::ProjectField::renamed("left.from", "from"),
        crate::ivm::ProjectField::renamed("right.edge_to", "to"),
    ]);
    GraphBuilder::recursive(seed, step, "frontier", 16)
}

async fn write_edge_rows(
    storage: &impl OrderedKvStorage,
    edges: &RecordDescriptor,
    rows: &[(u64, u64, u64)],
) {
    let store = RecordStore::new(storage, "edges", edges);
    let operations = rows
        .iter()
        .map(|(id, src, dst)| {
            let record = edges
                .create(&[Value::U64(*id), Value::U64(*src), Value::U64(*dst)])
                .unwrap();
            let encoded = crate::records::encode_variant_record(0, &record);
            let key = id.to_be_bytes();
            store.set(&key, &encoded)
        })
        .collect();
    store.write_many(operations).await.unwrap();
}

fn edge_table_delta(edges: RecordDescriptor, rows: &[(u64, u64, u64)]) -> TableDelta {
    TableDelta {
        variant_tag: 0,
        table: "edges".to_owned(),
        descriptor: edges,
        deltas: rows
            .iter()
            .map(|(id, src, dst)| RecordDelta {
                record: edges
                    .create(&[Value::U64(*id), Value::U64(*src), Value::U64(*dst)])
                    .unwrap()
                    .into(),
                weight: 1,
            })
            .collect(),
    }
}

fn recursive_state_snapshot(
    runtime: &IvmRuntime,
    node: NodeId,
) -> (Vec<RecordDelta>, bool, Option<u64>) {
    let key = OperatorStateKey {
        scope: ScopeId::root(),
        node,
    };
    let Some(OperatorState::Recursive(state)) = runtime.operator_states.get(&key) else {
        panic!("recursive state missing for {node:?}");
    };
    let state = state.value();
    let mut accumulated = state.accumulated_deltas();
    accumulated.sort_by(|left, right| left.record.cmp(&right.record));
    (
        accumulated,
        state.step_arrangements_hydrated(),
        state.hydrated_input_generation(),
    )
}

fn recursive_reach_from_graph(src: u64) -> GraphBuilder {
    let seed = GraphBuilder::table("edges")
        .filter(PredicateExpr::eq("src", Value::U64(src)))
        .project(["src", "dst"]);
    let edge_pairs = GraphBuilder::table("edges").project(["src", "dst"]);
    let frontier = GraphBuilder::frontier_source("frontier", reach_descriptor());
    let step = GraphBuilder::join(frontier, edge_pairs, ["dst"], ["src"]).project_fields([
        crate::ivm::ProjectField::renamed("left.src", "src"),
        crate::ivm::ProjectField::renamed("right.dst", "dst"),
    ]);
    GraphBuilder::recursive(seed, step, "frontier", 16)
}

fn recursive_reach_from_with_union_step_graph(src: u64) -> GraphBuilder {
    let seed = GraphBuilder::table("edges")
        .filter(PredicateExpr::eq("src", Value::U64(src)))
        .project(["src", "dst"]);
    let edge_pairs = GraphBuilder::table("edges").project(["src", "dst"]);
    let frontier = GraphBuilder::frontier_source("frontier", reach_descriptor());
    let expanded = GraphBuilder::join(frontier.clone(), edge_pairs, ["dst"], ["src"])
        .project_fields([
            crate::ivm::ProjectField::renamed("left.src", "src"),
            crate::ivm::ProjectField::renamed("right.dst", "dst"),
        ]);
    let step = GraphBuilder::union([frontier, expanded]);
    GraphBuilder::recursive(seed, step, "frontier", 16)
}

async fn assert_auto_family_matches_direct_with_prepared_count<S1, S2>(
    schema: DatabaseSchema,
    families: &[GraphBuilder],
    table_deltas: Vec<TableDelta>,
    storage_familied: Rc<S1>,
    storage_direct: Rc<S2>,
    expected_prepared_shapes: usize,
) where
    S1: OrderedKvStorage + 'static,
    S2: OrderedKvStorage + 'static,
{
    let mut familied = IvmRuntime::new(schema.clone()).unwrap();
    let mut direct = IvmRuntime::new(schema).unwrap();
    direct.set_auto_direct_family_enabled(false);

    let mut familied_subscriptions = Vec::with_capacity(families.len());
    let mut direct_subscriptions = Vec::with_capacity(families.len());
    for graph in families.iter().cloned() {
        familied_subscriptions.push(
            familied
                .subscribe_one_sink(graph.clone(), &storage_familied)
                .await
                .unwrap(),
        );
        direct_subscriptions.push(
            direct
                .subscribe_one_sink(graph, &storage_direct)
                .await
                .unwrap(),
        );
    }

    assert_eq!(familied.prepared_shapes.len(), expected_prepared_shapes);
    for (familied_subscription, direct_subscription) in familied_subscriptions
        .iter()
        .zip(direct_subscriptions.iter())
    {
        assert_eq!(
            familied_subscription.recv().unwrap(),
            direct_subscription.recv().unwrap()
        );
    }

    familied
        .tick(table_deltas.clone(), &storage_familied)
        .await
        .expect("familied tick");
    direct
        .tick(table_deltas, &storage_direct)
        .await
        .expect("direct tick");
    for (familied_subscription, direct_subscription) in familied_subscriptions
        .iter()
        .zip(direct_subscriptions.iter())
    {
        match (
            familied_subscription.try_recv(),
            direct_subscription.try_recv(),
        ) {
            (Ok(familied), Ok(direct)) => assert_eq!(familied, direct),
            (Err(TryRecvError::Empty), Err(TryRecvError::Empty)) => {}
            (familied, direct) => {
                panic!("familied/direct notification mismatch: {familied:?} != {direct:?}");
            }
        }
    }
}

async fn assert_auto_family_matches_direct<S1, S2>(
    schema: DatabaseSchema,
    families: &[GraphBuilder],
    table_deltas: Vec<TableDelta>,
    storage_familied: Rc<S1>,
    storage_direct: Rc<S2>,
) where
    S1: OrderedKvStorage + 'static,
    S2: OrderedKvStorage + 'static,
{
    assert_auto_family_matches_direct_with_prepared_count(
        schema,
        families,
        table_deltas,
        storage_familied,
        storage_direct,
        1,
    )
    .await;
}

#[futures_test::test]
async fn direct_literal_subscriptions_share_auto_family_and_keep_direct_output() {
    let schema = albums_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let storage = Rc::new(
        crate::storage::MemoryStorage::new(&["albums"]).expect("valid memory storage families"),
    );
    let first = runtime
        .subscribe_one_sink(
            GraphBuilder::table("albums")
                .filter(PredicateExpr::eq("id", Value::U64(1)))
                .project(["title"]),
            &storage,
        )
        .await
        .unwrap();
    let second = runtime
        .subscribe_one_sink(
            GraphBuilder::table("albums")
                .filter(PredicateExpr::eq("id", Value::U64(2)))
                .project(["title"]),
            &storage,
        )
        .await
        .unwrap();

    assert!(first.recv().unwrap().is_empty());
    assert!(second.recv().unwrap().is_empty());
    assert_eq!(runtime.prepared_shapes.len(), 1);
    assert_eq!(
        runtime
            .binding_sources
            .values()
            .next()
            .unwrap()
            .refcounts
            .len(),
        2
    );

    let albums = schema.table("albums").unwrap().record_schema();
    runtime
        .tick(
            vec![TableDelta {
                variant_tag: 0,
                table: "albums".to_owned(),
                descriptor: albums,
                deltas: vec![
                    RecordDelta {
                        record: albums
                            .create(&[Value::U64(1), Value::String("one".to_owned())])
                            .unwrap()
                            .into(),
                        weight: 1,
                    },
                    RecordDelta {
                        record: albums
                            .create(&[Value::U64(2), Value::String("two".to_owned())])
                            .unwrap()
                            .into(),
                        weight: 1,
                    },
                ],
            }],
            &storage,
        )
        .await
        .unwrap();

    assert_eq!(
        first.recv().unwrap().to_values().unwrap(),
        vec![(vec![Value::String("one".to_owned())], 1)]
    );
    assert_eq!(
        second.recv().unwrap().to_values().unwrap(),
        vec![(vec![Value::String("two".to_owned())], 1)]
    );
}

#[futures_test::test]
async fn hydration_memo_survives_empty_ticks_without_replaying_deltas() {
    let schema = albums_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let storage = Rc::new(
        crate::storage::MemoryStorage::new(&["albums"]).expect("valid memory storage families"),
    );
    let subscription = runtime
        .subscribe_one_sink(GraphBuilder::table("albums"), &storage)
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());
    assert!(runtime.eval_memo.keys().any(|key| key.tick_epoch.is_none()));

    runtime.tick(Vec::new(), &storage).await.unwrap();
    assert!(subscription.try_recv().is_err());
    assert!(runtime.eval_memo.keys().any(|key| key.tick_epoch.is_none()));

    let albums = schema.table("albums").unwrap().record_schema();
    let row = albums
        .create(&[Value::U64(1), Value::String("Blue Train".to_owned())])
        .unwrap();
    runtime
        .tick(
            vec![TableDelta {
                variant_tag: 0,
                table: "albums".to_owned(),
                descriptor: albums,
                deltas: vec![RecordDelta {
                    record: row.into(),
                    weight: 1,
                }],
            }],
            &storage,
        )
        .await
        .unwrap();
    assert_eq!(subscription.recv().unwrap().deltas.len(), 1);

    runtime.tick(Vec::new(), &storage).await.unwrap();
    assert!(subscription.try_recv().is_err());
}

async fn write_two_album_rows(storage: &impl OrderedKvStorage, albums: &RecordDescriptor) {
    let store = RecordStore::new(storage, "albums", albums);
    let first = albums
        .create(&[Value::U64(1), Value::String("one".to_owned())])
        .unwrap();
    let second = albums
        .create(&[Value::U64(2), Value::String("two".to_owned())])
        .unwrap();
    let first = crate::records::encode_variant_record(0, &first);
    let second = crate::records::encode_variant_record(0, &second);
    store
        .write_many(vec![store.set(b"1", &first), store.set(b"2", &second)])
        .await
        .unwrap();
}

fn album_count_graph() -> GraphBuilder {
    GraphBuilder::aggregate(
        GraphBuilder::table("albums"),
        Vec::<String>::new(),
        [AggregateExpr {
            function: AggregateFunction::Count,
            expression: None,
            distinct: false,
            output_name: Some("count".to_owned()),
        }],
    )
}

#[futures_test::test]
async fn aggregate_subscription_hydration_reuses_current_shared_arrangements() {
    let schema = albums_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let storage = Rc::new(MemoryStorage::new(&["albums"]).expect("valid memory storage families"));
    let albums = schema.table("albums").unwrap().record_schema();
    write_two_album_rows(&storage, &albums).await;

    let first = runtime
        .subscribe_one_sink(album_count_graph(), &storage)
        .await
        .unwrap();
    let first_snapshot = first.recv().unwrap();
    assert_eq!(
        first_snapshot.to_values().unwrap(),
        vec![(vec![Value::U64(2)], 1)]
    );
    let after_first = runtime.stats();
    assert!(after_first.hydration_memo_computes > 0);

    let mut fresh_runtime = IvmRuntime::new(schema).unwrap();
    let fresh = fresh_runtime
        .subscribe_one_sink(album_count_graph(), &storage)
        .await
        .unwrap()
        .recv()
        .unwrap();

    let second = runtime
        .subscribe_one_sink(album_count_graph(), &storage)
        .await
        .unwrap();
    let reused = second.recv().unwrap();
    let after_second = runtime.stats();

    assert_eq!(reused, fresh);
    assert_eq!(
        after_second.hydration_memo_computes, after_first.hydration_memo_computes,
        "second identical subscriber should reuse the hydrated aggregate output"
    );
    assert!(
        after_second.hydration_memo_hits > after_first.hydration_memo_hits,
        "second identical subscriber should record a hydration memo hit"
    );
}

/// Alice, Bob, and Carol share a collector after its disposable output memo
/// is evicted. Rehydrating Bob/Carol must not multiply Alice's resident rows.
/// Alice opens -> evict memo -> Bob opens -> evict -> Carol opens -> update/delete.
/// Internal coverage is intentional: forcing this pure-cache eviction is not
/// a public database operation; the canvas scenario covers the public Db path.
#[futures_test::test]
async fn shared_root_collector_rehydration_does_not_multiply_rows() {
    let schema = albums_schema();
    let albums = schema.table("albums").unwrap().record_schema();
    let mut runtime = IvmRuntime::new(schema).unwrap();
    let storage = Rc::new(MemoryStorage::new(&["albums"]).unwrap());
    write_two_album_rows(&storage, &albums).await;
    let graph = GraphBuilder::collect_root_ordered(
        GraphBuilder::table("albums"),
        ["id"],
        [
            crate::ivm::CollectByField::named("id"),
            crate::ivm::CollectByField::named("title"),
        ],
        Vec::<crate::ivm::TopByOrder>::new(),
        ["id"],
        0,
        TopByLimit::Unbounded,
    );
    let mut subscriptions = Vec::new();
    for _ in 0..3 {
        runtime.evict_eval_memo_for_tests(0, 0);
        let subscription = runtime
            .subscribe([("rows", graph.clone())], &storage)
            .unwrap();
        runtime.drive_pending_incremental().await.unwrap();
        let initial = subscription.try_recv().unwrap();
        let operations = &initial.terminal_sinks["rows"].operations;
        assert_eq!(operations.len(), 2);
        assert!(
            operations
                .iter()
                .all(|op| matches!(op.edit, TerminalEdit::Insert { .. }))
        );
        subscriptions.push(subscription);
    }
    let old = albums
        .create(&[Value::U64(1), Value::String("one".into())])
        .unwrap();
    let new = albums
        .create(&[Value::U64(1), Value::String("updated".into())])
        .unwrap();
    runtime
        .tick(
            vec![TableDelta {
                variant_tag: 0,
                table: "albums".into(),
                descriptor: albums,
                deltas: vec![
                    RecordDelta {
                        record: old.into(),
                        weight: -1,
                    },
                    RecordDelta {
                        record: new.clone().into(),
                        weight: 1,
                    },
                ],
            }],
            &storage,
        )
        .await
        .unwrap();
    for subscription in &subscriptions {
        let update = subscription.try_recv().unwrap();
        let operations = &update.terminal_sinks["rows"].operations;
        assert_eq!(operations.len(), 1);
        let TerminalEdit::Update { value, .. } = &operations[0].edit else {
            panic!("expected one update: {operations:?}");
        };
        assert_eq!(
            OwnedRecord::new(value.clone(), operations[0].root_descriptor)
                .to_values()
                .unwrap(),
            vec![Value::U64(1), Value::String("updated".into())]
        );
    }
    runtime
        .tick(
            vec![TableDelta {
                variant_tag: 0,
                table: "albums".into(),
                descriptor: albums,
                deltas: vec![RecordDelta {
                    record: new.into(),
                    weight: -1,
                }],
            }],
            &storage,
        )
        .await
        .unwrap();
    for subscription in &subscriptions {
        let update = subscription.try_recv().unwrap();
        assert!(matches!(
            update.terminal_sinks["rows"].operations.as_slice(),
            [TerminalOperation {
                edit: TerminalEdit::Remove { .. },
                ..
            }]
        ));
    }
    runtime.tick(Vec::new(), &storage).await.unwrap();
    assert!(
        subscriptions
            .iter()
            .all(|subscription| subscription.try_recv().is_err())
    );
}

#[futures_test::test]
async fn one_shot_aggregate_hydration_does_not_satisfy_subscription_arrangement_seed() {
    let schema = albums_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let storage = Rc::new(MemoryStorage::new(&["albums"]).expect("valid memory storage families"));
    let albums = schema.table("albums").unwrap().record_schema();
    write_two_album_rows(&storage, &albums).await;

    let one_shot = runtime
        .query_snapshot(album_count_graph(), &storage)
        .await
        .unwrap();
    assert_eq!(
        one_shot.to_values().unwrap(),
        vec![(vec![Value::U64(2)], 1)]
    );
    let after_one_shot = runtime.stats();
    assert_eq!(after_one_shot.arrangement_count, 0);

    let subscription = runtime
        .subscribe_one_sink(album_count_graph(), &storage)
        .await
        .unwrap();
    let snapshot = subscription.recv().unwrap();
    let after_subscribe = runtime.stats();

    assert_eq!(snapshot, one_shot);
    assert!(
        after_subscribe.hydration_memo_computes > after_one_shot.hydration_memo_computes,
        "subscription hydration must rebuild when a one-shot memo has no current arrangement"
    );
    assert_eq!(after_subscribe.arrangement_count, 1);
}

#[futures_test::test]
async fn pending_subscription_drains_match_unbounded_when_eval_memo_is_evicted_before_drain() {
    let schema = albums_schema();
    let albums = schema.table("albums").unwrap().record_schema();

    let run = async |evict_before_drain: bool| {
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let storage = Rc::new(
            crate::storage::MemoryStorage::new(&["albums"]).expect("valid memory storage families"),
        );
        let subscription = runtime
            .subscribe_one_sink(GraphBuilder::table("albums"), &storage)
            .await
            .unwrap();
        assert!(subscription.recv().unwrap().is_empty());

        let row = albums
            .create(&[Value::U64(1), Value::String("Blue Train".to_owned())])
            .unwrap();
        runtime
            .tick(
                vec![TableDelta {
                    variant_tag: 0,
                    table: "albums".to_owned(),
                    descriptor: albums,
                    deltas: vec![RecordDelta {
                        record: row.into(),
                        weight: 1,
                    }],
                }],
                &storage,
            )
            .await
            .unwrap();

        if evict_before_drain {
            runtime.evict_eval_memo_for_tests(0, 0);
            assert!(
                runtime.eval_memo.is_empty(),
                "the eval memo is a pure cache and may be fully evicted while subscription output is pending"
            );
        }

        let delivered = subscription.recv().unwrap();

        if evict_before_drain {
            runtime.evict_eval_memo_for_tests(0, 0);
            assert!(
                runtime.eval_memo.is_empty(),
                "draining subscription output must not depend on eval memo entries"
            );
        }

        delivered
    };

    assert_eq!(run(true).await, run(false).await);
}

#[futures_test::test]
async fn memo_context_digest_distinguishes_frontier_binding_values() {
    let descriptor = reach_descriptor();
    let left = RecordDeltas {
        descriptor,
        deltas: vec![RecordDelta {
            record: descriptor
                .create(&[Value::U64(1), Value::U64(2)])
                .unwrap()
                .into(),
            weight: 1,
        }],
    };
    let right = RecordDeltas {
        descriptor,
        deltas: vec![RecordDelta {
            record: descriptor
                .create(&[Value::U64(1), Value::U64(3)])
                .unwrap()
                .into(),
            weight: 1,
        }],
    };

    assert_ne!(record_deltas_digest(&left), record_deltas_digest(&right));
    assert_eq!(record_deltas_digest(&left), record_deltas_digest(&left));
}

#[futures_test::test]
async fn project_emits_copied_literal_and_null_columns() {
    let schema = albums_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let storage = Rc::new(
        crate::storage::MemoryStorage::new(&["albums"]).expect("valid memory storage families"),
    );
    let subscription = runtime
        .subscribe_one_sink(
            GraphBuilder::table("albums").project_fields([
                ProjectField::renamed("id", "id"),
                ProjectField::literal(
                    "event_kind",
                    LiteralValue::String("result_content".to_owned()),
                ),
                ProjectField::null_typed(
                    "missing_title",
                    ValueType::Nullable(Box::new(ValueType::String)),
                ),
            ]),
            &storage,
        )
        .await
        .unwrap();

    assert!(subscription.recv().unwrap().is_empty());
    assert!(runtime.graph.nodes().values().any(|node| {
        let OpType::MapProject(project) = &node.descriptor.operator else {
            return false;
        };
        project.mapping == vec![(0, 0)]
            && project.expressions.len() == 3
            && !projection_uses_raw_copy(
                &project.expressions,
                &project.mapping,
                node.descriptor.output.records(),
            )
    }));

    let albums = schema.table("albums").unwrap().record_schema();
    runtime
        .tick(
            vec![TableDelta {
                variant_tag: 0,
                table: "albums".to_owned(),
                descriptor: albums,
                deltas: vec![
                    RecordDelta {
                        record: albums
                            .create(&[Value::U64(1), Value::String("one".to_owned())])
                            .unwrap()
                            .into(),
                        weight: 2,
                    },
                    RecordDelta {
                        record: albums
                            .create(&[Value::U64(2), Value::String("two".to_owned())])
                            .unwrap()
                            .into(),
                        weight: -3,
                    },
                ],
            }],
            &storage,
        )
        .await
        .unwrap();

    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        vec![
            (
                vec![
                    Value::U64(1),
                    Value::String("result_content".to_owned()),
                    Value::Nullable(None),
                ],
                2,
            ),
            (
                vec![
                    Value::U64(2),
                    Value::String("result_content".to_owned()),
                    Value::Nullable(None),
                ],
                -3,
            ),
        ]
    );
}

#[futures_test::test]
async fn project_typed_literal_preserves_nested_nullable_null_type() {
    let schema = albums_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let storage = Rc::new(
        crate::storage::MemoryStorage::new(&["albums"]).expect("valid memory storage families"),
    );
    let subscription = runtime
        .subscribe_one_sink(
            GraphBuilder::table("albums").project_fields([
                ProjectField::renamed("id", "id"),
                ProjectField::literal_typed(
                    "default_value",
                    LiteralValue::Nullable(Some(Box::new(LiteralValue::Nullable(None)))),
                    ValueType::Nullable(Box::new(ValueType::Nullable(Box::new(ValueType::String)))),
                ),
            ]),
            &storage,
        )
        .await
        .unwrap();

    assert!(subscription.recv().unwrap().is_empty());
    let albums = schema.table("albums").unwrap().record_schema();
    runtime
        .tick(
            vec![TableDelta {
                variant_tag: 0,
                table: "albums".to_owned(),
                descriptor: albums,
                deltas: vec![RecordDelta {
                    record: albums
                        .create(&[Value::U64(1), Value::String("one".to_owned())])
                        .unwrap()
                        .into(),
                    weight: 1,
                }],
            }],
            &storage,
        )
        .await
        .unwrap();

    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        vec![(
            vec![
                Value::U64(1),
                Value::Nullable(Some(Box::new(Value::Nullable(None)))),
            ],
            1,
        )],
    );
}

#[futures_test::test]
async fn cold_project_hydration_materializes_literal_and_typed_null_columns() {
    let schema = albums_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let storage = Rc::new(
        crate::storage::MemoryStorage::new(&["albums"]).expect("valid memory storage families"),
    );
    let albums = schema.table("albums").unwrap().record_schema();
    let store = RecordStore::new(&storage, "albums", &albums);
    let first = albums
        .create(&[Value::U64(1), Value::String("one".to_owned())])
        .unwrap();
    let second = albums
        .create(&[Value::U64(2), Value::String("two".to_owned())])
        .unwrap();
    let first = crate::records::encode_variant_record(0, &first);
    let second = crate::records::encode_variant_record(0, &second);

    store
        .write_many(vec![store.set(b"1", &first), store.set(b"2", &second)])
        .await
        .unwrap();

    let subscription = runtime
        .subscribe_one_sink(
            GraphBuilder::table("albums").project_fields([
                ProjectField::renamed("id", "id"),
                ProjectField::literal("event_kind", LiteralValue::String("cold".to_owned())),
                ProjectField::null_typed(
                    "missing_title",
                    ValueType::Nullable(Box::new(ValueType::String)),
                ),
            ]),
            &storage,
        )
        .await
        .unwrap();

    let mut initial = subscription.recv().unwrap().to_values().unwrap();
    initial.sort_by_key(|(values, _)| {
        let Value::U64(id) = values[0] else {
            unreachable!()
        };
        id
    });
    assert_eq!(
        initial,
        vec![
            (
                vec![
                    Value::U64(1),
                    Value::String("cold".to_owned()),
                    Value::Nullable(None),
                ],
                1,
            ),
            (
                vec![
                    Value::U64(2),
                    Value::String("cold".to_owned()),
                    Value::Nullable(None),
                ],
                1,
            ),
        ]
    );
}

#[futures_test::test]
async fn pure_copy_project_lowers_with_full_fast_mapping() {
    let schema = albums_schema();
    let mut runtime = IvmRuntime::new(schema).unwrap();
    let storage = Rc::new(
        crate::storage::MemoryStorage::new(&["albums"]).expect("valid memory storage families"),
    );
    let subscription = runtime
        .subscribe_one_sink(
            GraphBuilder::table("albums").project(["id", "title"]),
            &storage,
        )
        .await
        .unwrap();

    assert!(subscription.recv().unwrap().is_empty());
    assert!(runtime.graph.nodes().values().any(|node| {
        let OpType::MapProject(project) = &node.descriptor.operator else {
            return false;
        };
        project.mapping == vec![(0, 0), (0, 1)]
            && project.expressions.len() == 2
            && project
                .expressions
                .iter()
                .all(|expr| matches!(expr.expression, PlanExpr::Field(_)))
    }));
}

#[futures_test::test]
async fn auto_family_hidden_field_does_not_collide_with_user_column() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "records",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("__auto_binding_0", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let storage = Rc::new(
        crate::storage::MemoryStorage::new(&["records"]).expect("valid memory storage families"),
    );
    let first = runtime
        .subscribe_one_sink(
            GraphBuilder::table("records")
                .filter(PredicateExpr::eq("id", Value::U64(1)))
                .project(["__auto_binding_0"]),
            &storage,
        )
        .await
        .unwrap();
    let second = runtime
        .subscribe_one_sink(
            GraphBuilder::table("records")
                .filter(PredicateExpr::eq("id", Value::U64(2)))
                .project(["__auto_binding_0"]),
            &storage,
        )
        .await
        .unwrap();
    assert!(first.recv().unwrap().is_empty());
    assert!(second.recv().unwrap().is_empty());
    assert_eq!(runtime.prepared_shapes.len(), 1);

    let descriptor = schema.table("records").unwrap().record_schema();
    runtime
        .tick(
            vec![TableDelta {
                variant_tag: 0,
                table: "records".to_owned(),
                descriptor,
                deltas: vec![
                    RecordDelta {
                        record: descriptor
                            .create(&[Value::U64(1), Value::String("visible-one".to_owned())])
                            .unwrap()
                            .into(),
                        weight: 1,
                    },
                    RecordDelta {
                        record: descriptor
                            .create(&[Value::U64(2), Value::String("visible-two".to_owned())])
                            .unwrap()
                            .into(),
                        weight: 1,
                    },
                ],
            }],
            &storage,
        )
        .await
        .unwrap();

    assert_eq!(
        first.recv().unwrap().to_values().unwrap(),
        vec![(vec![Value::String("visible-one".to_owned())], 1)]
    );
    assert_eq!(
        second.recv().unwrap().to_values().unwrap(),
        vec![(vec![Value::String("visible-two".to_owned())], 1)]
    );
}

#[futures_test::test]
async fn auto_family_multi_join_is_byte_identical_to_direct_path() {
    let schema = DatabaseSchema::new([
        TableSchema::new(
            "albums",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("artist_id", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "artists",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("name", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "labels",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("artist_id", ColumnType::U64),
                ColumnSchema::new("label", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ]);
    let graph = |artist_id| {
        let albums = GraphBuilder::table("albums")
            .filter(PredicateExpr::eq("artist_id", Value::U64(artist_id)));
        let album_artists = GraphBuilder::join(
            albums,
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        )
        .project_fields([
            ProjectField::renamed("left.id", "album_id"),
            ProjectField::renamed("left.artist_id", "artist_id"),
            ProjectField::renamed("left.title", "title"),
            ProjectField::renamed("right.name", "artist"),
        ]);
        GraphBuilder::join(
            album_artists,
            GraphBuilder::table("labels"),
            ["artist_id"],
            ["artist_id"],
        )
        .project_fields([
            ProjectField::renamed("left.title", "title"),
            ProjectField::renamed("left.artist", "artist"),
            ProjectField::renamed("right.label", "label"),
        ])
    };
    let albums = schema.table("albums").unwrap().record_schema();
    let artists = schema.table("artists").unwrap().record_schema();
    let labels = schema.table("labels").unwrap().record_schema();
    let deltas = vec![
        TableDelta {
            variant_tag: 0,
            table: "artists".to_owned(),
            descriptor: artists,
            deltas: vec![RecordDelta {
                record: artists
                    .create(&[Value::U64(7), Value::String("Alice".to_owned())])
                    .unwrap()
                    .into(),
                weight: 1,
            }],
        },
        TableDelta {
            variant_tag: 0,
            table: "labels".to_owned(),
            descriptor: labels,
            deltas: vec![RecordDelta {
                record: labels
                    .create(&[
                        Value::U64(70),
                        Value::U64(7),
                        Value::String("Impulse".to_owned()),
                    ])
                    .unwrap()
                    .into(),
                weight: 1,
            }],
        },
        TableDelta {
            variant_tag: 0,
            table: "albums".to_owned(),
            descriptor: albums,
            deltas: vec![RecordDelta {
                record: albums
                    .create(&[
                        Value::U64(700),
                        Value::U64(7),
                        Value::String("Journey".to_owned()),
                    ])
                    .unwrap()
                    .into(),
                weight: 1,
            }],
        },
    ];
    let familied_storage = crate::storage::MemoryStorage::new(&["albums", "artists", "labels"])
        .expect("valid memory storage families");
    let direct_storage = crate::storage::MemoryStorage::new(&["albums", "artists", "labels"])
        .expect("valid memory storage families");
    assert_auto_family_matches_direct(
        schema,
        &[graph(7), graph(8)],
        deltas,
        Rc::new(familied_storage),
        Rc::new(direct_storage),
    )
    .await;
}

#[futures_test::test]
async fn auto_family_recursive_shape_falls_back_to_byte_identical_direct_path() {
    let schema = edges_schema();
    let edges = schema.table("edges").unwrap().record_schema();
    let deltas = vec![TableDelta {
        variant_tag: 0,
        table: "edges".to_owned(),
        descriptor: edges,
        deltas: vec![
            RecordDelta {
                record: edges
                    .create(&[Value::U64(1), Value::U64(1), Value::U64(2)])
                    .unwrap()
                    .into(),
                weight: 1,
            },
            RecordDelta {
                record: edges
                    .create(&[Value::U64(2), Value::U64(2), Value::U64(3)])
                    .unwrap()
                    .into(),
                weight: 1,
            },
            RecordDelta {
                record: edges
                    .create(&[Value::U64(3), Value::U64(9), Value::U64(10)])
                    .unwrap()
                    .into(),
                weight: 1,
            },
        ],
    }];
    let familied_storage =
        crate::storage::MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let direct_storage =
        crate::storage::MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    assert_auto_family_matches_direct_with_prepared_count(
        schema,
        &[recursive_reach_from_graph(1), recursive_reach_from_graph(9)],
        deltas,
        Rc::new(familied_storage),
        Rc::new(direct_storage),
        0,
    )
    .await;
}

#[futures_test::test]
async fn auto_family_arg_max_by_shape_is_byte_identical_to_direct_path() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "scores",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("group_id", ColumnType::U64),
            ColumnSchema::new("score", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let graph = |group_id| {
        GraphBuilder::arg_max_by(
            GraphBuilder::table("scores")
                .filter(PredicateExpr::eq("group_id", Value::U64(group_id))),
            ["group_id"],
            ["score"],
        )
        .project(["id", "group_id", "score"])
    };
    let scores = schema.table("scores").unwrap().record_schema();
    let deltas = vec![TableDelta {
        variant_tag: 0,
        table: "scores".to_owned(),
        descriptor: scores,
        deltas: vec![
            RecordDelta {
                record: scores
                    .create(&[Value::U64(1), Value::U64(1), Value::U64(10)])
                    .unwrap()
                    .into(),
                weight: 1,
            },
            RecordDelta {
                record: scores
                    .create(&[Value::U64(2), Value::U64(1), Value::U64(20)])
                    .unwrap()
                    .into(),
                weight: 1,
            },
            RecordDelta {
                record: scores
                    .create(&[Value::U64(3), Value::U64(2), Value::U64(15)])
                    .unwrap()
                    .into(),
                weight: 1,
            },
        ],
    }];
    let familied_storage =
        crate::storage::MemoryStorage::new(&["scores"]).expect("valid memory storage families");
    let direct_storage =
        crate::storage::MemoryStorage::new(&["scores"]).expect("valid memory storage families");
    assert_auto_family_matches_direct(
        schema,
        &[graph(1), graph(2)],
        deltas,
        Rc::new(familied_storage),
        Rc::new(direct_storage),
    )
    .await;
}

#[futures_test::test]
async fn auto_family_excluded_recursive_shape_falls_back_to_direct_path() {
    let schema = edges_schema();
    let mut runtime = IvmRuntime::new(schema).unwrap();
    let storage = Rc::new(
        crate::storage::MemoryStorage::new(&["edges"]).expect("valid memory storage families"),
    );
    let first = runtime
        .subscribe_one_sink(recursive_reach_from_with_union_step_graph(1), &storage)
        .await
        .unwrap();
    let second = runtime
        .subscribe_one_sink(recursive_reach_from_with_union_step_graph(9), &storage)
        .await
        .unwrap();

    assert!(first.recv().unwrap().is_empty());
    assert!(second.recv().unwrap().is_empty());
    assert!(runtime.prepared_shapes.is_empty());
    assert!(matches!(
        runtime
            .multisink_subscriptions
            .get(&first.id())
            .unwrap()
            .target,
        MultisinkSubscriptionTarget::Direct
    ));
    assert!(matches!(
        runtime
            .multisink_subscriptions
            .get(&second.id())
            .unwrap()
            .target,
        MultisinkSubscriptionTarget::Direct
    ));
}

#[futures_test::test]
async fn subscription_retainers_keep_output_ancestors_alive() {
    let schema = albums_schema();
    let mut runtime = IvmRuntime::new(schema).unwrap();
    let storage = Rc::new(MemoryStorage::new(&["albums"]).expect("valid memory storage families"));
    let subscription = runtime
        .subscribe_one_sink(
            GraphBuilder::table("albums")
                .filter(PredicateExpr::gt("id", Value::U64(10)))
                .project(["title"]),
            &storage,
        )
        .await
        .unwrap();
    let output = runtime.subscription_output_node(subscription.id()).unwrap();
    let retained = runtime.retained_node_ids();

    assert_eq!(retained.len(), 3);
    assert!(retained.contains(&output));
    assert!(runtime.graph().node(output).is_some());
}

#[futures_test::test]
async fn deep_retained_only_graph_ticks_through_the_dependency_queue() {
    let schema = albums_schema();
    let albums = schema.table("albums").unwrap().record_schema();
    let mut runtime = IvmRuntime::new(schema).unwrap();
    let storage = Rc::new(
        crate::storage::MemoryStorage::new(&["albums"]).expect("valid memory storage families"),
    );
    let mut graph = GraphBuilder::table("albums");
    for _ in 0..64 {
        graph = graph.filter(PredicateExpr::gt("id", Value::U64(0)));
    }
    let output = runtime.add_dedup_graph(&graph).unwrap().node;
    runtime.add_retainer(output, Retainer::PreparedShape("deep-retained".to_owned()));
    let row = albums
        .create(&[Value::U64(1), Value::String("Blue Train".to_owned())])
        .unwrap();

    runtime
        .tick(
            vec![TableDelta {
                variant_tag: 0,
                table: "albums".to_owned(),
                descriptor: albums,
                deltas: vec![RecordDelta {
                    record: row.into(),
                    weight: 1,
                }],
            }],
            &storage,
        )
        .await
        .unwrap();

    assert!(runtime.retained_node_ids().contains(&output));
}

#[test]
fn deeply_nested_recursive_graph_compiles_on_a_server_sized_stack() {
    // Recursive INHERITS policy lowering can produce a deeply nested, but
    // finite, seed. Installing its Recursive node also collects its read
    // tables, so both compilation and recursive source discovery must remain
    // iterative on the dedicated server shell's ordinary thread stack.
    let compiled = std::thread::Builder::new()
        .name("ivm-deep-graph-receipt".to_owned())
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            let schema = albums_schema();
            let albums = schema
                .table("albums")
                .expect("albums table")
                .record_schema();
            let mut runtime = IvmRuntime::new(schema).expect("build runtime");
            let mut graph = GraphBuilder::table("albums");
            for _ in 0..8_192 {
                graph = graph.filter(PredicateExpr::gt("id", Value::U64(0)));
            }
            let graph = GraphBuilder::recursive(
                graph,
                GraphBuilder::frontier_source("frontier", albums),
                "frontier",
                1,
            );
            let compiled = runtime
                .add_dedup_graph(&graph)
                .expect("compile deeply nested recursive graph");
            let recursive_node = runtime
                .graph
                .node(compiled.node)
                .expect("compiled recursive node");
            let OpType::Recursive(recursive) = &recursive_node.descriptor.operator else {
                panic!("compiled root must remain recursive");
            };
            assert_eq!(recursive.read_tables, ["albums"]);
            // The receipt targets compilation and recursive source discovery;
            // dropping a deliberately 8k-deep builder would independently
            // recurse through Boxes after that work has completed.
            std::mem::forget(graph);
        })
        .expect("spawn server-sized stack receipt")
        .join();

    assert!(
        compiled.is_ok(),
        "deep recursive graph compilation and source discovery must complete"
    );
}

#[test]
fn deeply_nested_retained_graph_ticks_on_a_server_sized_stack() {
    // The server shell polls retained graphs directly. This receipt keeps that
    // evaluator path separate from compilation: a valid, deeply nested policy
    // carrier must not recursively poll one future per graph node.
    let completed = std::thread::Builder::new()
        .name("ivm-deep-tick-receipt".to_owned())
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            futures::executor::block_on(async {
                let schema = albums_schema();
                let albums = schema
                    .table("albums")
                    .expect("albums table")
                    .record_schema();
                let mut runtime = IvmRuntime::new(schema).expect("build runtime");
                let storage =
                    MemoryStorage::new(&["albums"]).expect("valid memory storage families");
                let mut seed = GraphBuilder::table("albums");
                for _ in 0..1_024 {
                    seed = seed.filter(PredicateExpr::gt("id", Value::U64(0)));
                }
                let graph = GraphBuilder::recursive(
                    seed,
                    GraphBuilder::frontier_source("frontier", albums.clone()),
                    "frontier",
                    1,
                );
                let output = runtime
                    .add_dedup_graph(&graph)
                    .expect("compile deeply nested graph")
                    .node;
                runtime.add_retainer(output, Retainer::PreparedShape("deep-tick".to_owned()));
                let row = albums
                    .create(&[Value::U64(1), Value::String("Blue Train".to_owned())])
                    .expect("create album row");
                runtime
                    .tick(
                        vec![TableDelta {
                            variant_tag: 0,
                            table: "albums".to_owned(),
                            descriptor: albums,
                            deltas: vec![RecordDelta {
                                record: row.into(),
                                weight: 1,
                            }],
                        }],
                        &storage,
                    )
                    .await
                    .expect("evaluate deeply nested graph");
                assert!(runtime.retained_node_ids().contains(&output));
                // This receipt exercises evaluation; derived GraphBuilder
                // destruction is an unrelated recursive path.
                std::mem::forget(graph);
            });
        })
        .expect("spawn server-sized stack receipt")
        .join();

    assert!(
        completed.is_ok(),
        "retained deep graph evaluation must not recurse through the owner stack"
    );
}

#[futures_test::test]
async fn unsubscribe_eagerly_collects_unretained_ephemeral_nodes_and_state() {
    let schema = albums_schema();
    let mut runtime = IvmRuntime::new(schema).unwrap();
    let storage = Rc::new(MemoryStorage::new(&["albums"]).expect("valid memory storage families"));
    let subscription = runtime
        .subscribe_one_sink(GraphBuilder::table("albums"), &storage)
        .await
        .unwrap();
    let output = runtime.subscription_output_node(subscription.id()).unwrap();

    assert_eq!(runtime.retained_node_ids().len(), 1);
    assert!(
        runtime
            .node_meta
            .get(&output)
            .is_some_and(|meta| !meta.retainers.is_empty())
    );

    assert!(runtime.unsubscribe(subscription.id()));

    assert!(runtime.retained_node_ids().is_empty());
    assert!(runtime.graph().node(output).is_none());
    assert!(!runtime.node_meta.contains_key(&output));
}

#[futures_test::test]
async fn identical_subscriptions_share_one_node_with_multiple_retainers() {
    let schema = albums_schema();
    let mut runtime = IvmRuntime::new(schema).unwrap();
    let storage = Rc::new(MemoryStorage::new(&["albums"]).expect("valid memory storage families"));
    let graph = || {
        GraphBuilder::table("albums")
            .filter(PredicateExpr::gt("id", Value::U64(10)))
            .project(["title"])
    };

    let first = runtime.subscribe_one_sink(graph(), &storage).await.unwrap();
    let second = runtime.subscribe_one_sink(graph(), &storage).await.unwrap();
    let output = runtime.subscription_output_node(first.id()).unwrap();

    assert_eq!(Some(output), runtime.subscription_output_node(second.id()));
    assert_eq!(
        runtime
            .node_meta
            .get(&output)
            .map(|meta| meta.retainers.len()),
        Some(2)
    );

    assert!(runtime.unsubscribe(first.id()));
    assert!(runtime.graph().node(output).is_some());
    assert_eq!(
        runtime
            .node_meta
            .get(&output)
            .map(|meta| meta.retainers.len()),
        Some(1)
    );

    assert!(runtime.unsubscribe(second.id()));
    assert!(runtime.graph().node(output).is_none());
    assert!(!runtime.node_meta.contains_key(&output));
}

#[futures_test::test]
async fn durable_schema_nodes_are_runtime_retainer_roots() {
    let schema = indexed_albums_schema();
    let runtime = IvmRuntime::new(schema).unwrap();
    let retained = runtime.retained_node_ids();
    let durable_nodes = retained
        .iter()
        .copied()
        .filter(|node| {
            runtime
                .graph()
                .node(*node)
                .is_some_and(|node| node.is_durable())
        })
        .collect::<Vec<_>>();

    assert_eq!(durable_nodes.len(), 1);
    assert_eq!(retained.len(), 3);
}

#[futures_test::test]
async fn unsupported_query_operator_variants_are_not_executable() {
    let schema = albums_schema();
    let storage = Rc::new(
        crate::storage::MemoryStorage::new(&["albums"]).expect("valid memory storage families"),
    );
    let mut runtime = IvmRuntime::new(schema).unwrap();
    let input = runtime
        .add_dedup_graph(&GraphBuilder::table("albums"))
        .unwrap()
        .node;
    let output = *runtime.table_descriptor("albums").unwrap();

    let unsupported = [OpType::Distinct, OpType::Negate];

    for operator in unsupported {
        let node = runtime.graph.dedup_node(
            NodeDescriptor::new(operator, [input], output),
            NodeDurability::Ephemeral,
        );
        assert!(matches!(
            runtime
                .hydration_snapshot(node, &storage, HydrationMode::Ordinary)
                .await,
            Err(IvmRuntimeError::UnsupportedOperator)
        ));
    }
}

#[futures_test::test]
async fn stale_as_of_state_rejects_wrong_or_backward_logical_time() {
    let mut state = AsOf::<usize, SubTick>::new(7);

    assert!(matches!(
        state.value_at(SubTick {
            tick: 0,
            sub_tick: 0
        }),
        Err(IvmRuntimeError::StaleRuntimeState { .. })
    ));
    state
        .mark_forward_as_of(SubTick {
            tick: 0,
            sub_tick: 2,
        })
        .unwrap();
    assert_eq!(
        *state
            .value_at(SubTick {
                tick: 0,
                sub_tick: 2,
            })
            .unwrap(),
        7
    );
    assert!(matches!(
        state.value_at(SubTick {
            tick: 0,
            sub_tick: 1
        }),
        Err(IvmRuntimeError::StaleRuntimeState { .. })
    ));
    assert!(matches!(
        state.mark_forward_as_of(SubTick {
            tick: 0,
            sub_tick: 1
        }),
        Err(IvmRuntimeError::OutOfOrderRuntimeState { .. })
    ));
}

#[futures_test::test]
async fn similar_join_subscriptions_share_context_independent_base_arrangements() {
    let schema = albums_artists_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let storage =
        Rc::new(MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families"));
    let first = runtime
        .subscribe_one_sink(
            GraphBuilder::join(
                GraphBuilder::table("albums"),
                GraphBuilder::table("artists"),
                ["artist_id"],
                ["id"],
            ),
            &storage,
        )
        .await
        .unwrap();
    let second = runtime
        .subscribe_one_sink(
            GraphBuilder::join(
                GraphBuilder::table("albums").filter(PredicateExpr::gt("id", Value::U64(0))),
                GraphBuilder::table("artists"),
                ["artist_id"],
                ["id"],
            ),
            &storage,
        )
        .await
        .unwrap();

    let artist_arrangement_nodes = runtime
        .graph()
        .nodes()
        .values()
        .filter(|node| {
            matches!(
                (&node.descriptor.operator, node.descriptor.output),
                (
                    OpType::Arrange(ArrangeOp { fields, .. }),
                    NodeOutput::Arrangement(ArrangementDescriptor { records })
                ) if fields == &["id"] && records == schema.table("artists").unwrap().record_schema()
            )
        })
        .map(|node| node.id)
        .collect::<Vec<_>>();
    assert_eq!(artist_arrangement_nodes.len(), 1);

    let albums = schema.table("albums").unwrap().record_schema();
    let artists = schema.table("artists").unwrap().record_schema();
    runtime
        .tick(
            vec![
                TableDelta {
                    variant_tag: 0,
                    table: "albums".to_owned(),
                    descriptor: albums,
                    deltas: vec![RecordDelta {
                        record: albums
                            .create(&[
                                Value::U64(7),
                                Value::U64(11),
                                Value::String("Blue Train".to_owned()),
                            ])
                            .unwrap()
                            .into(),
                        weight: 1,
                    }],
                },
                TableDelta {
                    variant_tag: 0,
                    table: "artists".to_owned(),
                    descriptor: artists,
                    deltas: vec![RecordDelta {
                        record: artists
                            .create(&[Value::U64(11), Value::String("John Coltrane".to_owned())])
                            .unwrap()
                            .into(),
                        weight: 1,
                    }],
                },
            ],
            &storage,
        )
        .await
        .unwrap();

    let artist_arrangements = runtime
        .arrangement_states
        .keys()
        .filter(|key| {
            key.scope == ScopeId::root()
                && runtime.graph().node(key.input).is_some_and(|node| {
                    matches!(
                        (&node.descriptor.operator, node.descriptor.output),
                        (
                            OpType::Arrange(ArrangeOp { fields, .. }),
                            NodeOutput::Arrangement(ArrangementDescriptor { records })
                        ) if fields == &["id"] && records == artists
                    )
                })
        })
        .count();

    assert_eq!(artist_arrangements, 1);
    let stats = runtime.stats();
    assert_eq!(stats.arrangement_count, 3);
    assert!(stats.arrangement_rows >= 2);
    assert!(stats.arrangement_encoded_bytes > 0);
    assert!(stats.logical_nodes_requested > stats.deduped_graph_nodes as u64);
    assert!(stats.dedupe_ratio() < 1.0);

    assert!(runtime.unsubscribe(first.id()));
    assert!(
        runtime.graph().node(artist_arrangement_nodes[0]).is_some(),
        "the shared arrangement node must survive its first consumer"
    );
    assert!(runtime.unsubscribe(second.id()));
    assert!(
        runtime.graph().node(artist_arrangement_nodes[0]).is_none(),
        "the final consumer must release the arrangement through graph reachability"
    );
    assert_eq!(runtime.stats().arrangement_count, 0);
}

#[futures_test::test]
async fn recursive_recompute_reuses_graph_nodes_without_persisting_contextual_child_state() {
    let schema = edges_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let storage = Rc::new(MemoryStorage::new(&["edges"]).expect("valid memory storage families"));
    let first = runtime
        .subscribe_one_sink(recursive_reach_graph(), &storage)
        .await
        .unwrap();
    let second = runtime
        .subscribe_one_sink(recursive_reach_graph(), &storage)
        .await
        .unwrap();

    assert_eq!(
        runtime.subscription_output_node(first.id()),
        runtime.subscription_output_node(second.id())
    );

    let edges = schema.table("edges").unwrap().record_schema();
    let table_delta = TableDelta {
        variant_tag: 0,
        table: "edges".to_owned(),
        descriptor: edges,
        deltas: vec![
            RecordDelta {
                record: edges
                    .create(&[Value::U64(1), Value::U64(1), Value::U64(2)])
                    .unwrap()
                    .into(),
                weight: 1,
            },
            RecordDelta {
                record: edges
                    .create(&[Value::U64(2), Value::U64(2), Value::U64(3)])
                    .unwrap()
                    .into(),
                weight: 1,
            },
        ],
    };
    runtime.tick(vec![table_delta], &storage).await.unwrap();

    assert!(
        runtime
            .operator_states
            .keys()
            .all(|key| key.scope == ScopeId::root()),
        "recursive recomputation should not leave per-context child state in runtime"
    );
}

/// A positive recursive update is visible only once the tick is ready.  The
/// public subscription receives the seed edge and its newly derived path as a
/// single committed delta, with no duplicate delivery.
#[futures_test::test]
async fn recursive_positive_tick_commits_new_facts_exactly_once() {
    let schema = edges_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let storage = Rc::new(MemoryStorage::new(&["edges"]).expect("valid memory storage families"));
    let edges = schema.table("edges").unwrap().record_schema();
    write_edge_rows(&storage, &edges, &[(1, 1, 2)]).await;

    let subscription = runtime
        .subscribe_one_sink(recursive_reach_graph_with_limit(16), &storage)
        .await
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(2)], 1)]
    );

    let metrics = runtime
        .tick(vec![edge_table_delta(edges, &[(2, 2, 3)])], &storage)
        .await
        .unwrap();
    assert_eq!(metrics.table_delta_records, 1);
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (vec![Value::U64(1), Value::U64(3)], 1),
            (vec![Value::U64(2), Value::U64(3)], 1),
        ]
    );
    assert!(
        subscription.try_recv().is_err(),
        "one positive recursive tick must publish exactly one notification"
    );
}

/// Positive recursive maintenance must let the step graph transform both the
/// frontier and table inputs before the join computes new paths.
#[futures_test::test]
async fn recursive_positive_tick_applies_transforms_before_join() {
    let schema = edges_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let storage = Rc::new(MemoryStorage::new(&["edges"]).expect("valid memory storage families"));
    let edges = schema.table("edges").unwrap().record_schema();
    write_edge_rows(&storage, &edges, &[(1, 1, 2)]).await;

    let subscription = runtime
        .subscribe_one_sink(recursive_reach_with_renamed_inputs_graph(), &storage)
        .await
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(2)], 1)]
    );

    runtime
        .tick(vec![edge_table_delta(edges, &[(2, 2, 3)])], &storage)
        .await
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (vec![Value::U64(1), Value::U64(3)], 1),
            (vec![Value::U64(2), Value::U64(3)], 1),
        ]
    );
    assert!(subscription.try_recv().is_err());
}

/// The positive path accepts new facts before discovering that the next
/// iteration exceeds its safety bound.  A semantic failure must not commit
/// that partial closure, advance the tick/frontier counters, or retain
/// changed arrangement/memo accounting.
#[futures_test::test]
async fn recursive_iteration_limit_rolls_back_partial_positive_tick() {
    // This runtime-level seam is intentional: Database fail-stop makes the
    // post-error operator state inaccessible, while rollback must cover the
    // closure, arrangement, memo, and logical-time state together.
    let schema = edges_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let storage = Rc::new(MemoryStorage::new(&["edges"]).expect("valid memory storage families"));
    let edges = schema.table("edges").unwrap().record_schema();
    write_edge_rows(&storage, &edges, &[(1, 1, 2)]).await;

    let subscription = runtime
        .subscribe_one_sink(recursive_reach_graph_with_limit(1), &storage)
        .await
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(2)], 1)]
    );

    let output = runtime.subscription_output_node(subscription.id()).unwrap();
    let before_state = recursive_state_snapshot(&runtime, output);
    let before_stats = runtime.stats();
    let before_tick = runtime.current_tick;
    let before_table_frontiers = runtime.table_frontiers.clone();

    let error = runtime
        .tick(
            vec![edge_table_delta(edges, &[(2, 2, 3), (3, 3, 4)])],
            &storage,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        IvmRuntimeError::RecursiveIterationLimit { max_iters: 1, .. }
    ));

    assert_eq!(
        recursive_state_snapshot(&runtime, output),
        before_state,
        "failed recursion must retain the last committed closure and arrangement state"
    );
    assert_eq!(
        runtime.stats(),
        before_stats,
        "failed recursion must not retain staged arrangement or memo accounting"
    );
    assert_eq!(
        runtime.current_tick, before_tick,
        "a failed recursive tick must not advance logical time"
    );
    assert_eq!(
        runtime.table_frontiers, before_table_frontiers,
        "a failed recursive tick must not advance input frontiers"
    );
    assert!(
        subscription.try_recv().is_err(),
        "a failed recursive tick must not expose staged facts"
    );
}

/// Derived durable index changes are part of a recursive tick's publication
/// boundary. A limit failure after producing a partial closure must not leak
/// its index writes into physical storage.
#[futures_test::test]
async fn recursive_iteration_limit_does_not_leak_staged_durable_index_writes() {
    let schema = indexed_edges_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let storage =
        Rc::new(MemoryStorage::new(&["edges", "indices"]).expect("valid memory storage families"));
    let edges = schema.table("edges").unwrap().record_schema();
    write_edge_rows(&storage, &edges, &[(1, 1, 2)]).await;

    let subscription = runtime
        .subscribe_one_sink(recursive_reach_graph_with_limit(1), &storage)
        .await
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(2)], 1)]
    );

    let error = runtime
        .tick(
            vec![edge_table_delta(edges, &[(2, 2, 3), (3, 3, 4)])],
            &storage,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        IvmRuntimeError::RecursiveIterationLimit { max_iters: 1, .. }
    ));

    let mut index_rows = storage
        .scan(crate::storage::ScanRequest::prefix(
            "indices".to_owned(),
            Vec::new(),
        ))
        .await
        .unwrap();
    assert!(
        index_rows.next_batch().await.unwrap().is_none(),
        "failed recursion must discard derived durable index writes"
    );
    assert!(subscription.try_recv().is_err());
}

#[futures_test::test]
async fn definitely_uncommitted_durable_flush_failure_discards_staged_state_and_notifications() {
    let schema = indexed_edges_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let (storage, control) = TestStorage::controlled(&["edges", "indices"]);
    let storage = Rc::new(storage);
    let edges = schema.table("edges").unwrap().record_schema();
    let initial_storage = Rc::new(MemoryStorage::new(&["edges", "indices"]).unwrap());
    let subscription = runtime
        .subscribe_one_sink(GraphBuilder::table("edges"), &initial_storage)
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());
    let before_tick = runtime.current_tick;

    control.take_observed();
    control.fail_next_uncommitted(TestStorageOperation::WriteMany);
    let mut tick = Box::pin(runtime.tick(vec![edge_table_delta(edges, &[(1, 1, 2)])], &storage));
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut result = None;
    for _ in 0..16 {
        if let Poll::Ready(outcome) = tick.as_mut().poll(&mut cx) {
            result = Some(outcome);
            break;
        }
    }
    assert!(
        result.is_some(),
        "flush failure must resolve rather than retain an un-wakeable tick"
    );
    assert!(result.unwrap().is_err());
    drop(tick);

    assert_eq!(runtime.current_tick, before_tick);
    assert!(subscription.try_recv().is_err());
    assert_eq!(
        control
            .observed()
            .into_iter()
            .filter(|operation| *operation == TestStorageOperation::WriteMany)
            .count(),
        1,
        "the staged durable batch is submitted exactly once"
    );
}

/// This is intentionally an internal runtime test: the public Database owner
/// converts this runtime state into `DatabasePoisoned`, while this fixture
/// proves the lower-level direct evaluator never reuses its pre-flush state.
#[futures_test::test]
async fn commit_then_lost_flush_acknowledgement_poisoned_direct_runtime() {
    let schema = indexed_edges_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let (storage, control) = TestStorage::controlled(&["edges", "indices"]);
    let storage = Rc::new(storage);
    let edges = schema.table("edges").unwrap().record_schema();
    let initial_storage = Rc::new(MemoryStorage::new(&["edges", "indices"]).unwrap());
    let subscription = runtime
        .subscribe_one_sink(GraphBuilder::table("edges"), &initial_storage)
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    control.take_observed();
    control.lose_next_write_many_acknowledgement();
    let error = runtime
        .tick(vec![edge_table_delta(edges, &[(1, 1, 2)])], &storage)
        .await
        .expect_err("a lost acknowledgement must fail closed");
    assert!(matches!(error, IvmRuntimeError::Storage(_)));
    assert!(subscription.try_recv().is_err());
    let mut index_rows = storage
        .scan(crate::storage::ScanRequest::prefix(
            "indices".to_owned(),
            Vec::new(),
        ))
        .await
        .unwrap();
    assert!(
        index_rows.next_batch().await.unwrap().is_some(),
        "the injected fault must lose only the acknowledgement, after the batch commits"
    );

    let error = runtime
        .tick(Vec::new(), &storage)
        .await
        .expect_err("an indeterminate flush must prevent reuse of the old evaluator");
    assert!(matches!(
        error,
        IvmRuntimeError::PersistenceOutcomeIndeterminate
    ));
}

#[futures_test::test]
async fn cancelling_pending_durable_flush_discards_the_uninstalled_tick() {
    let schema = indexed_edges_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let (storage, control) = TestStorage::controlled(&["edges", "indices"]);
    let storage = Rc::new(storage);
    let edges = schema.table("edges").unwrap().record_schema();
    let initial_storage = Rc::new(MemoryStorage::new(&["edges", "indices"]).unwrap());
    let subscription = runtime
        .subscribe_one_sink(GraphBuilder::table("edges"), &initial_storage)
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());
    let before_tick = runtime.current_tick;

    control.take_observed();
    control.pause_on(TestStorageOperation::WriteMany);
    let mut tick = Box::pin(runtime.tick(vec![edge_table_delta(edges, &[(1, 1, 2)])], &storage));
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    for _ in 0..8 {
        if matches!(tick.as_mut().poll(&mut cx), Poll::Pending)
            && control
                .observed()
                .contains(&TestStorageOperation::WriteMany)
        {
            break;
        }
    }
    assert_eq!(
        control
            .observed()
            .into_iter()
            .filter(|operation| *operation == TestStorageOperation::WriteMany)
            .count(),
        1,
        "the pending flush owns one physical submission"
    );
    drop(tick);

    assert_eq!(runtime.current_tick, before_tick);
    assert!(subscription.try_recv().is_err());
    let mut index_rows = storage
        .scan(crate::storage::ScanRequest::prefix(
            "indices".to_owned(),
            Vec::new(),
        ))
        .await
        .unwrap();
    assert!(index_rows.next_batch().await.unwrap().is_none());
    let error = runtime
        .tick(Vec::new(), &storage)
        .await
        .expect_err("dropping a started flush must poison the direct runtime");
    assert!(matches!(
        error,
        IvmRuntimeError::PersistenceOutcomeIndeterminate
    ));
}

/// Resident Database writes use the same staged evaluator as direct ticks, but
/// scoped failures must discard it rather than install its partial closure.
#[futures_test::test]
async fn resident_recursive_iteration_limit_discards_staged_state() {
    let schema = edges_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let storage = Rc::new(MemoryStorage::new(&["edges"]).expect("valid memory storage families"));
    let edges = schema.table("edges").unwrap().record_schema();
    write_edge_rows(&storage, &edges, &[(1, 1, 2)]).await;

    let subscription = runtime
        .subscribe_one_sink(recursive_reach_graph_with_limit(1), &storage)
        .await
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(2)], 1)]
    );
    let output = runtime.subscription_output_node(subscription.id()).unwrap();
    let before_tick = runtime.current_tick;
    let before_frontiers = runtime.table_frontiers.clone();
    runtime
        .tick_resident_staged(
            vec![edge_table_delta(edges, &[(2, 2, 3), (3, 3, 4)])],
            OwnedStorage::new(Rc::clone(&storage)),
            false,
            None,
        )
        .await
        .unwrap();

    assert!(
        !runtime.operator_states.contains_key(&OperatorStateKey {
            scope: ScopeId::root(),
            node: output,
        }),
        "a scoped resident failure removes its recursive state rather than installing a partial closure"
    );
    assert_eq!(
        runtime.stats().recursive_state_count,
        0,
        "a scoped resident failure retains no recursive closure"
    );
    assert_eq!(
        runtime.current_tick, before_tick,
        "a scoped resident failure must not advance logical time"
    );
    assert_eq!(
        runtime.table_frontiers, before_frontiers,
        "a scoped resident failure must not advance input frontiers"
    );
}

/// A large existing closure is a scale canary for the positive path: adding an
/// isolated edge must process only the new frontier, not feed every old fact
/// back through the recursive step.
#[futures_test::test]
async fn recursive_positive_tick_does_not_reprocess_full_existing_closure() {
    let schema = edges_schema();
    let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
    let storage = Rc::new(MemoryStorage::new(&["edges"]).expect("valid memory storage families"));
    let edges = schema.table("edges").unwrap().record_schema();
    let rows = (1..=48).map(|id| (id, id, id + 1)).collect::<Vec<_>>();
    write_edge_rows(&storage, &edges, &rows).await;

    let subscription = runtime
        .subscribe_one_sink(recursive_reach_graph_with_limit(64), &storage)
        .await
        .unwrap();
    let initial = subscription.recv().unwrap();
    assert!(
        initial.deltas.len() > 100,
        "fixture must establish a meaningfully large recursive closure"
    );

    let metrics = runtime
        .tick(
            vec![edge_table_delta(edges, &[(10_000, 10_000, 10_001)])],
            &storage,
        )
        .await
        .unwrap();
    assert!(
        metrics.records_processed < 128,
        "isolated positive edge should not reprocess the full closure: {} records",
        metrics.records_processed
    );
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(10_000), Value::U64(10_001)], 1)]
    );
    assert!(subscription.try_recv().is_err());
}

#[futures_test::test]
async fn key_encoding_preserves_value_order_for_index_range_scans() {
    let mut encoded = [
        Value::U64(1),
        Value::U64(256),
        Value::String("aa".to_owned()),
        Value::String("b".to_owned()),
        Value::F64(f64::NEG_INFINITY),
        Value::F64(-1.0),
        Value::F64(-0.0),
        Value::F64(0.0),
        Value::F64(1.0),
        Value::F64(f64::INFINITY),
        Value::Bytes(b"a\0b".to_vec()),
        Value::Bytes(b"a\0c".to_vec()),
    ]
    .into_iter()
    .map(|value| {
        let mut key = Vec::new();
        encode_key_part(&mut key, &value).unwrap();
        (value, key)
    })
    .collect::<Vec<_>>();

    encoded.sort_by(|left, right| left.1.cmp(&right.1));

    assert_eq!(
        encoded
            .into_iter()
            .map(|(value, _)| value)
            .collect::<Vec<_>>(),
        [
            Value::U64(1),
            Value::U64(256),
            Value::F64(f64::NEG_INFINITY),
            Value::F64(-1.0),
            Value::F64(-0.0),
            Value::F64(0.0),
            Value::F64(1.0),
            Value::F64(f64::INFINITY),
            Value::String("aa".to_owned()),
            Value::String("b".to_owned()),
            Value::Bytes(b"a\0b".to_vec()),
            Value::Bytes(b"a\0c".to_vec()),
        ]
    );
    let mut key = Vec::new();
    assert!(matches!(
        encode_key_part(&mut key, &Value::F64(f64::NAN)),
        Err(IvmRuntimeError::RecordEncoding(
            records::Error::InvalidF64NaN
        ))
    ));
}

#[futures_test::test]
async fn record_values_canonicalize_delta_identity_and_are_rejected_as_arrangement_keys() {
    // This is deliberately a runtime-level test: consolidation identifies
    // records by their encoded bytes, so the relevant observable is the
    // delta batch produced by the maintained runtime rather than a record
    // field accessor alone.
    let child = RecordDescriptor::new([("id", ValueType::U64)]);
    let first = records::OwnedRecord::new(child.create(&[Value::U64(7)]).unwrap(), child);
    let second = records::OwnedRecord::new(child.create(&[Value::U64(7)]).unwrap(), child);
    let descriptor = RecordDescriptor::new([("child", ValueType::Record(Box::new(child)))]);
    let first_parent = descriptor.create(&[Value::Record(first)]).unwrap();
    let second_parent = descriptor.create(&[Value::Record(second)]).unwrap();

    assert_eq!(first_parent, second_parent);
    assert!(
        consolidate_deltas(vec![
            RecordDelta {
                record: first_parent.into(),
                weight: 1,
            },
            RecordDelta {
                record: second_parent.into(),
                weight: -1,
            },
        ])
        .is_empty()
    );

    let record = descriptor
        .create(&[Value::Record(records::OwnedRecord::new(
            child.create(&[Value::U64(7)]).unwrap(),
            child,
        ))])
        .unwrap();
    assert!(matches!(
        encoded_record_key_part(descriptor, &record, &[0]),
        Err(IvmRuntimeError::UnsupportedJoinKey)
    ));
}
