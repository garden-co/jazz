//! A retained subscription must not rebuild the large columns its projection
//! drops, neither for its first result nor for any later update (#3830).
//!
//! One-shot reads already keep such columns physical (#3471). A retained
//! subscription still rebuilt every large value of every row it published,
//! then dropped the columns its `select` excludes. On a replica that holds
//! only the row, rebuilding means fetching every chunk first. A folder listing
//! that selected only file names therefore downloaded each file completely,
//! and published none of its rows until the last chunk had arrived.
//!
//! As in `large_value_read_scaling`, these tests observe Groove's test-only
//! count of complete large-value rebuilds (`full_materializations_for_test`):
//! the public rows are the same either way, and the defect is precisely the
//! work those rows do not show. Each count is compared with the same write
//! made while no subscription is open, so a write's own work never counts
//! against the subscription. Every other assertion is on public results.

use std::collections::BTreeMap;

mod common;

use common::{allow_all_policies, compile_schema};
use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, DeleteOptions, InsertOptions, LocalUpdates, Propagation, ReadOpts,
    SubscriptionEvent, SubscriptionOutputRow, SubscriptionStream, UpdateOptions,
};
use jazz::groove::ivm::{TerminalEdit, TerminalOperation, TerminalPathSegment};
use jazz::groove::large_values::{LEAF_MAX_BYTES, full_materializations_for_test};
use jazz::groove::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::CurrentRow;
use jazz::query::{ArraySubquery, OrderDirection, Query, col, gte, lit};
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

const FILES: &str = "files";

fn row(seed: u8) -> RowUuid {
    RowUuid::from_bytes([seed; 16])
}

fn files_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(FILES)
                    .column("name", ColumnType::Text)
                    .column("notes", ColumnType::Text)
                    .column("contents", ColumnType::Bytea)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open_db(schema: JazzSchema, seed: u8) -> Db {
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(DbConfig::new(
        schema,
        TestStorage::new(&refs),
        DbIdentity {
            node: NodeUuid::from_bytes([seed; 16]),
            author: AuthorSubject::for_test_bytes([0xa1; 16]),
        },
    )))
    .expect("open db")
}

fn table(schema: &JazzSchema) -> TableSchema {
    schema
        .tables()
        .iter()
        .find(|table| table.name == FILES)
        .expect("files table")
        .clone()
}

/// Deterministic bytes spanning several chunk leaves.
fn contents(seed: u8, leaves: usize) -> Vec<u8> {
    (0..LEAF_MAX_BYTES * leaves + 17)
        .map(|index| (index % 251) as u8 ^ seed)
        .collect()
}

fn large_notes(prefix: &str) -> String {
    format!("{prefix}{}", "n".repeat(LEAF_MAX_BYTES * 2))
}

fn insert_file(db: &Db, id: RowUuid, name: &str, bytes: &[u8]) {
    let write = block_on(db.insert(
        FILES,
        BTreeMap::from([
            ("name".to_owned(), Value::String(name.to_owned())),
            ("notes".to_owned(), Value::String(large_notes(name))),
            ("contents".to_owned(), Value::Bytes(bytes.to_vec())),
        ]),
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .expect("insert file");
    block_on(write.wait(DurabilityTier::Local)).expect("local durability");
}

fn rename_file(db: &Db, id: RowUuid, name: &str) {
    let write = block_on(db.update(
        FILES,
        id,
        BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
        UpdateOptions::default(),
    ))
    .expect("rename file");
    block_on(write.wait(DurabilityTier::Local)).expect("local durability");
}

fn replace_contents(db: &Db, id: RowUuid, bytes: &[u8]) {
    let write = block_on(db.update(
        FILES,
        id,
        BTreeMap::from([("contents".to_owned(), Value::Bytes(bytes.to_vec()))]),
        UpdateOptions::default(),
    ))
    .expect("replace contents");
    block_on(write.wait(DurabilityTier::Local)).expect("local durability");
}

fn delete_file(db: &Db, id: RowUuid) {
    let write = block_on(db.delete(FILES, id, DeleteOptions::default())).expect("delete file");
    block_on(write.wait(DurabilityTier::Local)).expect("local durability");
}

fn opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

/// Runs `work` and returns its result with the number of complete
/// large-value rebuilds it caused on this thread.
fn counting<T>(work: impl FnOnce() -> T) -> (T, u64) {
    let before = full_materializations_for_test();
    let result = work();
    (result, full_materializations_for_test() - before)
}

fn subscribe(db: &Db, query: Query) -> SubscriptionStream {
    let prepared = db.prepare_query(&query).expect("prepare subscription");
    block_on(db.subscribe(&prepared, opts())).expect("subscribe")
}

struct Delta {
    reset: bool,
    added: Vec<CurrentRow>,
    updated: Vec<CurrentRow>,
    removed: Vec<RowUuid>,
}

fn next_delta(subscription: &mut SubscriptionStream) -> Delta {
    match block_on(subscription.next_event()) {
        Some(SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            ..
        }) => Delta {
            reset,
            added: added.into_iter().map(|output| output.row).collect(),
            updated: updated.into_iter().map(|output| output.row).collect(),
            removed: removed
                .into_iter()
                .map(|removed| removed.row_uuid)
                .collect(),
        },
        other => panic!("expected a subscription delta, got {other:?}"),
    }
}

fn name(table: &TableSchema, row: &CurrentRow) -> Option<Value> {
    row.cell(table, "name")
}

/// A listing subscription that projects the large columns away never
/// rebuilds them: not for its first result, not when a file is added,
/// renamed or deleted. Its rows carry the selected column only.
///
/// alice ──insert a.bin, b.bin (large notes and contents)──► db
/// alice ──subscribe select(name)──► {a.bin, b.bin}, 0 rebuilds
/// alice ──insert c.bin───────────► +c.bin, as many rebuilds as with no subscription
/// alice ──rename b.bin → b2.bin──► ~b2.bin, as many rebuilds as with no subscription
/// alice ──delete a.bin───────────► −a.bin
#[test]
fn listing_subscription_never_rebuilds_excluded_large_columns() {
    let schema = files_schema();
    let table = table(&schema);
    let db = open_db(schema, 0x81);

    // Baselines: the same writes while no subscription is open.
    let ((), insert_baseline) =
        counting(|| insert_file(&db, row(0x82), "baseline.bin", &contents(0x82, 3)));
    let ((), rename_baseline) = counting(|| rename_file(&db, row(0x82), "baseline-2.bin"));
    delete_file(&db, row(0x82));

    insert_file(&db, row(0x83), "a.bin", &contents(0x83, 3));
    insert_file(&db, row(0x84), "b.bin", &contents(0x84, 3));

    let ((mut listing, opening), rebuilds) = counting(|| {
        let mut listing = subscribe(&db, Query::from(FILES).select(["name"]));
        let opening = next_delta(&mut listing);
        (listing, opening)
    });
    assert!(opening.reset);
    assert_eq!(
        rebuilds, 0,
        "a listing's first result must not rebuild the large columns it projects away"
    );
    let mut names = opening
        .added
        .iter()
        .map(|published| match name(&table, published) {
            Some(Value::String(listed)) => listed,
            other => panic!("expected a listed name, got {other:?}"),
        })
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, ["a.bin", "b.bin"]);
    for published in &opening.added {
        assert_eq!(published.cell(&table, "contents"), None);
        assert_eq!(published.cell(&table, "notes"), None);
    }

    let (added, rebuilds) = counting(|| {
        insert_file(&db, row(0x85), "c.bin", &contents(0x85, 3));
        next_delta(&mut listing)
    });
    assert_eq!(
        rebuilds, insert_baseline,
        "adding a file must not rebuild its contents for a listing"
    );
    assert_eq!(
        added
            .added
            .iter()
            .map(CurrentRow::row_uuid)
            .collect::<Vec<_>>(),
        vec![row(0x85)]
    );
    assert_eq!(
        name(&table, &added.added[0]),
        Some(Value::String("c.bin".to_owned()))
    );
    assert_eq!(added.added[0].cell(&table, "contents"), None);

    let (renamed, rebuilds) = counting(|| {
        rename_file(&db, row(0x84), "b2.bin");
        next_delta(&mut listing)
    });
    assert_eq!(
        rebuilds, rename_baseline,
        "renaming a file must not rebuild its contents for a listing"
    );
    let renamed_rows = renamed
        .updated
        .iter()
        .chain(&renamed.added)
        .filter(|published| published.row_uuid() == row(0x84))
        .collect::<Vec<_>>();
    assert_eq!(renamed_rows.len(), 1, "the renamed row is republished once");
    assert_eq!(
        name(&table, renamed_rows[0]),
        Some(Value::String("b2.bin".to_owned()))
    );

    // The retraction names the physical record the listing published.
    delete_file(&db, row(0x83));
    let deleted = next_delta(&mut listing);
    assert_eq!(deleted.removed, vec![row(0x83)]);
    assert!(deleted.added.is_empty());
}

/// A listing and a subscription that selects the contents may share graph
/// nodes. Each keeps its own representation: the listing never sees the
/// contents, while the other subscription still receives them in full, for
/// its first result and after the contents change.
///
/// alice ──subscribe select(name)──────────► listing
/// alice ──subscribe select(name, contents)─► viewer
/// alice ──insert d.bin────────────────────► listing +d.bin, viewer +d.bin with contents
/// alice ──replace d.bin's contents────────► viewer ~d.bin with the new contents
/// alice ──read *──────────────────────────► complete notes and contents
#[test]
fn listing_and_content_subscriptions_keep_their_own_representation() {
    let schema = files_schema();
    let table = table(&schema);
    let db = open_db(schema, 0x91);
    let first = contents(0x92, 3);
    insert_file(&db, row(0x92), "first.bin", &first);

    let mut listing = subscribe(&db, Query::from(FILES).select(["name"]));
    let mut viewer = subscribe(&db, Query::from(FILES).select(["name", "contents"]));
    let listed = next_delta(&mut listing);
    let viewed = next_delta(&mut viewer);
    assert!(listed.reset && viewed.reset);
    assert_eq!(listed.added.len(), 1);
    assert_eq!(listed.added[0].cell(&table, "contents"), None);
    assert_eq!(viewed.added.len(), 1);
    assert_eq!(
        viewed.added[0].cell(&table, "contents"),
        Some(Value::Bytes(first))
    );

    let added = contents(0x93, 4);
    insert_file(&db, row(0x93), "d.bin", &added);
    let listed = next_delta(&mut listing);
    let viewed = next_delta(&mut viewer);
    assert_eq!(
        listed
            .added
            .iter()
            .map(CurrentRow::row_uuid)
            .collect::<Vec<_>>(),
        vec![row(0x93)]
    );
    assert_eq!(listed.added[0].cell(&table, "contents"), None);
    assert_eq!(
        viewed
            .added
            .iter()
            .map(CurrentRow::row_uuid)
            .collect::<Vec<_>>(),
        vec![row(0x93)]
    );
    assert_eq!(
        viewed.added[0].cell(&table, "contents"),
        Some(Value::Bytes(added))
    );

    let replaced = contents(0x94, 5);
    replace_contents(&db, row(0x93), &replaced);
    let viewed = next_delta(&mut viewer);
    let changed = viewed
        .updated
        .iter()
        .chain(&viewed.added)
        .find(|published| published.row_uuid() == row(0x93))
        .expect("the viewer republishes the changed file");
    assert_eq!(
        changed.cell(&table, "contents"),
        Some(Value::Bytes(replaced.clone()))
    );

    let whole = db
        .prepare_query(&Query::from(FILES))
        .expect("prepare whole rows");
    let rows = db.read(&whole).expect("whole rows");
    let changed = rows
        .iter()
        .find(|published| published.row_uuid() == row(0x93))
        .expect("listed");
    assert_eq!(
        changed.cell(&table, "notes"),
        Some(Value::String(large_notes("d.bin")))
    );
    assert_eq!(
        changed.cell(&table, "contents"),
        Some(Value::Bytes(replaced))
    );
}

// ---------------------------------------------------------------------------
// Operators that read large values keep reading them.
//
// Keeping the dropped columns physical must never starve an operator of a
// value it inspects. The tests below pin the three places that now read only
// part of a record: the collector that renders an include, the TopBy that
// orders a listing's root, and a filter or sort on a large column the listing
// does not select.
// ---------------------------------------------------------------------------

const FOLDERS: &str = "folders";
const ENTRIES: &str = "entries";

fn folders_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(FOLDERS)
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new(ENTRIES)
                    .fk_column("folder_id", FOLDERS)
                    .column("name", ColumnType::Text)
                    .column("notes", ColumnType::Text)
                    .column("contents", ColumnType::Bytea)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn insert_folder(db: &Db, id: RowUuid) {
    let write = block_on(db.insert(
        FOLDERS,
        BTreeMap::from([("title".to_owned(), Value::String("docs".to_owned()))]),
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .expect("insert folder");
    block_on(write.wait(DurabilityTier::Local)).expect("local durability");
}

fn insert_entry(db: &Db, id: RowUuid, folder: RowUuid, name: &str, notes: &str, bytes: &[u8]) {
    let write = block_on(db.insert(
        ENTRIES,
        BTreeMap::from([
            ("folder_id".to_owned(), Value::Uuid(folder.0)),
            ("name".to_owned(), Value::String(name.to_owned())),
            ("notes".to_owned(), Value::String(notes.to_owned())),
            ("contents".to_owned(), Value::Bytes(bytes.to_vec())),
        ]),
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .expect("insert entry");
    block_on(write.wait(DurabilityTier::Local)).expect("local durability");
}

fn update_cells(db: &Db, table: &str, id: RowUuid, cells: BTreeMap<String, Value>) {
    let write =
        block_on(db.update(table, id, cells, UpdateOptions::default())).expect("update row");
    block_on(write.wait(DurabilityTier::Local)).expect("local durability");
}

/// Large notes that sort by `prefix`: the prefix leads, and the filler keeps
/// every value indirect.
fn sorting_notes(prefix: &str) -> String {
    format!("{prefix}{}", "n".repeat(LEAF_MAX_BYTES * 2))
}

/// The public event parts these tests fold.
struct Event {
    reset: bool,
    added: Vec<SubscriptionOutputRow>,
    updated: Vec<SubscriptionOutputRow>,
    removed: Vec<RowUuid>,
    terminal_operations: Vec<TerminalOperation>,
}

fn next_event(subscription: &mut SubscriptionStream) -> Event {
    match block_on(subscription.next_event()) {
        Some(SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            terminal_operations,
            ..
        }) => Event {
            reset,
            added,
            updated,
            removed: removed
                .into_iter()
                .map(|removed| removed.row_uuid)
                .collect(),
            terminal_operations,
        },
        other => panic!("expected a subscription delta, got {other:?}"),
    }
}

/// A flat ordered listing, folded from subscription events exactly as a
/// client applies them: removals, then every added or moved row at its
/// published index.
#[derive(Default)]
struct OrderedRows {
    rows: Vec<CurrentRow>,
}

impl OrderedRows {
    fn apply(&mut self, event: Event) {
        if event.reset {
            self.rows.clear();
        }
        let placed = event
            .added
            .iter()
            .chain(&event.updated)
            .map(|output| output.row_uuid())
            .collect::<Vec<_>>();
        self.rows.retain(|row| {
            !event.removed.contains(&row.row_uuid()) && !placed.contains(&row.row_uuid())
        });
        let mut placed = event
            .added
            .into_iter()
            .chain(event.updated)
            .collect::<Vec<_>>();
        placed.sort_by_key(|output| output.index);
        for output in placed {
            let index = output.index.min(self.rows.len());
            self.rows.insert(index, output.row);
        }
    }

    fn ids(&self) -> Vec<RowUuid> {
        self.rows.iter().map(CurrentRow::row_uuid).collect()
    }
}

/// The child array of one parent, folded from the parent rows a subscription
/// publishes and from its descendant edits under `relation`.
struct IncludedRows {
    relation: &'static str,
    children: Vec<OwnedRecord>,
}

impl IncludedRows {
    fn new(relation: &'static str) -> Self {
        Self {
            relation,
            children: Vec::new(),
        }
    }

    fn apply(&mut self, event: Event) {
        for parent in event.added.iter().chain(&event.updated) {
            let (descriptor, raw) = parent.encoded_record();
            match unwrap_nullable(
                descriptor
                    .bind(raw)
                    .get(self.relation)
                    .expect("parent carries its include"),
            ) {
                Value::Array(children) => {
                    self.children = children
                        .into_iter()
                        .map(|child| match unwrap_nullable(child) {
                            Value::Record(child) => child,
                            other => panic!("expected an included record, got {other:?}"),
                        })
                        .collect();
                }
                other => panic!("expected an included array, got {other:?}"),
            }
        }
        for operation in event.terminal_operations {
            if operation.path != [TerminalPathSegment::Collection(self.relation.to_owned())] {
                continue;
            }
            let child_descriptor = included_descriptor(&operation.root_descriptor, self.relation);
            match operation.edit {
                TerminalEdit::Insert { index, value, .. } => {
                    let index = index.min(self.children.len());
                    self.children
                        .insert(index, OwnedRecord::new(value, child_descriptor));
                }
                TerminalEdit::Update { key, value } => {
                    let position = self.position(&key);
                    self.children[position] = OwnedRecord::new(value, child_descriptor);
                }
                TerminalEdit::Remove { key } => {
                    let position = self.position(&key);
                    self.children.remove(position);
                }
                TerminalEdit::Move { key, index } => {
                    let position = self.position(&key);
                    let child = self.children.remove(position);
                    let index = index.min(self.children.len());
                    self.children.insert(index, child);
                }
            }
        }
    }

    /// A child edit key encodes the child's row identity, its first field.
    fn position(&self, key: &[u8]) -> usize {
        self.children
            .iter()
            .position(|child| {
                let id = child_id(child);
                key.windows(16).any(|window| window == id.0.as_bytes())
            })
            .expect("the edit addresses a published child")
    }

    fn ids(&self) -> Vec<RowUuid> {
        self.children.iter().map(child_id).collect()
    }

    fn cell(&self, id: RowUuid, column: &str) -> Option<Value> {
        let child = self
            .children
            .iter()
            .find(|child| child_id(child) == id)
            .expect("the child is included");
        record_cell(child, column)
    }
}

fn unwrap_nullable(value: Value) -> Value {
    match value {
        Value::Nullable(Some(inner)) => unwrap_nullable(*inner),
        other => other,
    }
}

fn included_descriptor(root: &RecordDescriptor, relation: &str) -> RecordDescriptor {
    let index = root
        .field_index(relation)
        .expect("the root descriptor names its include");
    let mut ty = &root.fields()[index].value_type;
    loop {
        match ty {
            ValueType::Nullable(inner) | ValueType::Array(inner) => ty = &**inner,
            ValueType::Record(descriptor) => return **descriptor,
            other => panic!("expected an included record type, got {other:?}"),
        }
    }
}

fn child_id(child: &OwnedRecord) -> RowUuid {
    match unwrap_nullable(child.get_idx(0).expect("child identity")) {
        Value::Uuid(id) => RowUuid(id),
        other => panic!("expected a child row id, got {other:?}"),
    }
}

/// An included column by its public name, whatever carrier spelling the
/// collector gives it; `None` when the include does not carry the column.
fn record_cell(record: &OwnedRecord, column: &str) -> Option<Value> {
    let index = record.descriptor().fields().iter().position(|field| {
        field
            .name
            .as_deref()
            .is_some_and(|name| name == column || name.ends_with(&format!("_{column}")))
    })?;
    match unwrap_nullable(record.get_idx(index).expect("included cell")) {
        Value::Nullable(None) => None,
        value => Some(value),
    }
}

/// An include that selects a large column delivers it in full, for the first
/// result and after the value is replaced; the collector must still rebuild
/// every field it renders.
///
/// alice ──subscribe folders + include entries(name, contents)──► full bytes
/// alice ──replace a.bin's contents──────────────────────────────► new full bytes
#[test]
fn an_include_that_selects_a_large_column_receives_it_in_full() {
    let db = open_db(folders_schema(), 0xa1);
    let folder = row(0xa2);
    insert_folder(&db, folder);
    let first = contents(0xa3, 3);
    insert_entry(&db, row(0xa3), folder, "a.bin", &sorting_notes("a"), &first);

    let mut viewer = subscribe(
        &db,
        Query::from(FOLDERS).array_subquery(
            ArraySubquery::new(ENTRIES, ENTRIES, "folder_id", "id").select(["name", "contents"]),
        ),
    );
    let mut included = IncludedRows::new(ENTRIES);
    let opening = next_event(&mut viewer);
    assert!(opening.reset);
    included.apply(opening);
    assert_eq!(included.ids(), vec![row(0xa3)]);
    assert_eq!(
        included.cell(row(0xa3), "contents"),
        Some(Value::Bytes(first))
    );
    assert_eq!(included.cell(row(0xa3), "notes"), None);

    let replaced = contents(0xa4, 4);
    update_cells(
        &db,
        ENTRIES,
        row(0xa3),
        BTreeMap::from([("contents".to_owned(), Value::Bytes(replaced.clone()))]),
    );
    included.apply(next_event(&mut viewer));
    assert_eq!(included.ids(), vec![row(0xa3)]);
    assert_eq!(
        included.cell(row(0xa3), "contents"),
        Some(Value::Bytes(replaced))
    );
    assert_eq!(included.cell(row(0xa3), "notes"), None);
}

/// An include ordered by a large string it does not select, with a limit,
/// keeps its order and window: the collector must rebuild its sort field. A
/// change that moves a child across the limit swaps the window's members.
///
/// alice ──subscribe include entries(name) order by notes limit 2──► [a, c]
/// alice ──set e's notes to "b…"───────────────────────────────────► [a, e]
#[test]
fn an_include_ordered_by_a_large_column_keeps_its_window() {
    let db = open_db(folders_schema(), 0xb1);
    let folder = row(0xb2);
    insert_folder(&db, folder);
    for (seed, prefix) in [(0xb3, "c"), (0xb4, "a"), (0xb5, "e")] {
        insert_entry(
            &db,
            row(seed),
            folder,
            &format!("{prefix}.bin"),
            &sorting_notes(prefix),
            &contents(seed, 2),
        );
    }

    let mut listing = subscribe(
        &db,
        Query::from(FOLDERS).array_subquery(
            ArraySubquery::new(ENTRIES, ENTRIES, "folder_id", "id")
                .select(["name"])
                .order_by("notes", OrderDirection::Asc)
                .limit(2),
        ),
    );
    let mut included = IncludedRows::new(ENTRIES);
    let opening = next_event(&mut listing);
    assert!(opening.reset);
    included.apply(opening);
    assert_eq!(included.ids(), vec![row(0xb4), row(0xb3)]);
    for id in included.ids() {
        assert_eq!(included.cell(id, "contents"), None);
    }

    update_cells(
        &db,
        ENTRIES,
        row(0xb5),
        BTreeMap::from([("notes".to_owned(), Value::String(sorting_notes("b")))]),
    );
    included.apply(next_event(&mut listing));
    assert_eq!(
        included.ids(),
        vec![row(0xb4), row(0xb5)],
        "the moved child enters the window and the last one leaves it"
    );
    assert_eq!(
        included.cell(row(0xb5), "name"),
        Some(Value::String("e.bin".to_owned()))
    );
}

/// A listing that selects only names but filters and orders on a large string
/// keeps its membership and order: the filter and the root ordering must
/// rebuild the notes, while the rows still carry no contents.
///
/// alice ──subscribe select(name) where notes ≥ "b" order by notes──► [b, d, e]
/// alice ──set e's notes to "bb…"──────────────────────────────────► [e, b, d]
/// alice ──set a's notes to "z…"───────────────────────────────────► [e, b, d, a]
/// alice ──rename d → d2───────────────────────────────────────────► [e, b, d2, a]
#[test]
fn a_listing_filtered_and_ordered_by_a_large_column_keeps_its_order() {
    let schema = files_schema();
    let table = table(&schema);
    let db = open_db(schema, 0xc1);
    for (seed, prefix) in [(0xc2, "a"), (0xc3, "c"), (0xc4, "e"), (0xc5, "d")] {
        let write = block_on(db.insert(
            FILES,
            BTreeMap::from([
                ("name".to_owned(), Value::String(format!("{prefix}.bin"))),
                ("notes".to_owned(), Value::String(sorting_notes(prefix))),
                ("contents".to_owned(), Value::Bytes(contents(seed, 2))),
            ]),
            InsertOptions {
                row_id: Some(row(seed)),
                ..Default::default()
            },
        ))
        .expect("insert file");
        block_on(write.wait(DurabilityTier::Local)).expect("local durability");
    }

    let mut listing = subscribe(
        &db,
        Query::from(FILES)
            .select(["name"])
            .filter(gte(col("notes"), lit("b")))
            .order_by("notes", OrderDirection::Asc),
    );
    let mut rows = OrderedRows::default();
    let opening = next_event(&mut listing);
    assert!(opening.reset);
    rows.apply(opening);
    assert_eq!(rows.ids(), vec![row(0xc3), row(0xc5), row(0xc4)]);

    update_cells(
        &db,
        FILES,
        row(0xc4),
        BTreeMap::from([("notes".to_owned(), Value::String(sorting_notes("bb")))]),
    );
    rows.apply(next_event(&mut listing));
    assert_eq!(rows.ids(), vec![row(0xc4), row(0xc3), row(0xc5)]);

    update_cells(
        &db,
        FILES,
        row(0xc2),
        BTreeMap::from([("notes".to_owned(), Value::String(sorting_notes("z")))]),
    );
    rows.apply(next_event(&mut listing));
    assert_eq!(
        rows.ids(),
        vec![row(0xc4), row(0xc3), row(0xc5), row(0xc2)],
        "a row whose notes now pass the filter joins at its sorted place"
    );

    rename_file(&db, row(0xc5), "d2.bin");
    rows.apply(next_event(&mut listing));
    assert_eq!(rows.ids(), vec![row(0xc4), row(0xc3), row(0xc5), row(0xc2)]);
    let names = rows
        .rows
        .iter()
        .map(|listed| name(&table, listed))
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        ["e.bin", "c.bin", "d2.bin", "a.bin"].map(|listed| Some(Value::String(listed.to_owned())))
    );
    for listed in &rows.rows {
        assert_eq!(listed.cell(&table, "contents"), None);
        assert_eq!(listed.cell(&table, "notes"), None);
    }
}
