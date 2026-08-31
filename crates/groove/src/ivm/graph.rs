//! Hash-consed IVM graph IR and builder API.
//!
//! This module owns graph identity: [`GraphBuilder`] is the user/planner-facing
//! IR, [`NodeDescriptor`] is the validated runtime descriptor, and [`IvmGraph`]
//! deduplicates compatible nodes by descriptor hash while retaining reverse
//! edges for graph maintenance and GC. Operator payload structs live in
//! [`super::op_types`]; lowering from queries lives in [`super::planner`]; the
//! tick loop that evaluates the graph lives in [`super::runtime`].

use std::{
    hash::{BuildHasher, Hash, Hasher},
    sync::Arc,
};

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use crate::records::{RecordDescriptor, Value, ValueType, collect_by_ordered_scalar};
use thiserror::Error;

use super::op_types::*;

/// User-facing graph construction API before deduplication.
///
/// Builders refer to table and field names directly; the runtime resolves those
/// names against the database schema when a graph is subscribed, queried, or
/// prepared.
///
/// Recursive child edges are immutable shared allocations. This keeps copies
/// of a deeply lowered graph shallow while preserving the builder's ordinary
/// value semantics for callers that replace an enclosing node.
///
/// ```rust
/// # futures::executor::block_on(async {
/// use groove::db::{Database, GraphBuilder, PredicateExpr};
/// use groove::ivm::ProjectField;
/// use groove::records::Value;
/// use groove::schema::{
///     ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
/// };
/// use groove::storage::MemoryStorage;
///
/// let schema = DatabaseSchema::new([
///     TableSchema::new("albums", [
///         ColumnSchema::new("id", ColumnType::U64),
///         ColumnSchema::new("artist_id", ColumnType::U64),
///         ColumnSchema::new("title", ColumnType::String),
///     ])
///     .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
///     TableSchema::new("artists", [
///         ColumnSchema::new("id", ColumnType::U64),
///         ColumnSchema::new("name", ColumnType::String),
///     ])
///     .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
/// ]);
/// let mut database = Database::new(schema, MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families")).await?;
///
/// let mut batch = database.open_batch();
/// batch.insert("artists", vec![Value::U64(1), Value::String("Wayne Shorter".into())]);
/// batch.insert("artists", vec![Value::U64(2), Value::String("McCoy Tyner".into())]);
/// batch.insert("albums", vec![Value::U64(10), Value::U64(1), Value::String("Speak No Evil".into())]);
/// batch.insert("albums", vec![Value::U64(11), Value::U64(2), Value::String("Expansions".into())]);
/// let applied = database.apply_batch(batch).await?;
/// let persisted = applied.persist().await;
/// database.finish_persistence(persisted)?;
///
/// let albums = GraphBuilder::table("albums")
///     .filter(PredicateExpr::eq("title", Value::String("Speak No Evil".into())));
/// let artists = GraphBuilder::table("artists");
/// let graph = GraphBuilder::join(albums, artists, ["artist_id"], ["id"]).project_fields([
///     ProjectField::renamed("left.title", "album"),
///     ProjectField::renamed("right.name", "artist"),
/// ]);
///
/// let rows = database.query_graph(graph).await?;
/// assert_eq!(
///     rows.to_values()?,
///     vec![(
///         vec![
///             Value::String("Speak No Evil".into()),
///             Value::String("Wayne Shorter".into()),
///         ],
///         1,
///     )]
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # }).unwrap();
/// ```
///
/// Prepared graph shapes use a named [`GraphBuilder::binding_source`] node. The
/// source name passed to [`Database::prepare`] must match the binding source in
/// the graph.
///
/// ```rust
/// # futures::executor::block_on(async {
/// use groove::db::{Database, GraphBuilder};
/// use groove::ivm::ProjectField;
/// use groove::records::{RecordDescriptor, Value};
/// use groove::schema::{
///     ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
/// };
/// use groove::storage::MemoryStorage;
///
/// let schema = DatabaseSchema::new([TableSchema::new("albums", [
///     ColumnSchema::new("id", ColumnType::U64),
///     ColumnSchema::new("artist_id", ColumnType::U64),
///     ColumnSchema::new("title", ColumnType::String),
/// ])
/// .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
/// let mut database = Database::new(schema, MemoryStorage::new(&["albums"]).expect("valid memory storage families")).await?;
///
/// let binding_descriptor = RecordDescriptor::new([("artist_id", ColumnType::U64.clone())]);
/// let graph = GraphBuilder::join(
///     GraphBuilder::binding_source("artist_params", binding_descriptor),
///     GraphBuilder::table("albums"),
///     ["artist_id"],
///     ["artist_id"],
/// )
/// .project_fields([
///     ProjectField::renamed("right.artist_id", "artist_id"),
///     ProjectField::renamed("right.id", "id"),
///     ProjectField::renamed("right.title", "title"),
/// ]);
///
/// let shape = database.prepare_one_sink(graph, "artist_params", binding_descriptor, ["artist_id"]).await?;
/// let subscription = database.bind_shape_one_sink(shape.id(), &[Value::U64(1)]).await?;
/// assert!(subscription.recv()?.is_empty());
///
/// let mut batch = database.open_batch();
/// batch.insert("albums", vec![Value::U64(10), Value::U64(1), Value::String("Speak No Evil".into())]);
/// batch.insert("albums", vec![Value::U64(11), Value::U64(2), Value::String("Expansions".into())]);
/// let applied = database.apply_batch(batch).await?;
/// let persisted = applied.persist().await;
/// database.finish_persistence(persisted)?;
///
/// assert_eq!(
///     subscription.recv()?.to_values()?,
///     vec![(
///         vec![
///             Value::U64(1),
///             Value::U64(10),
///             Value::String("Speak No Evil".into()),
///         ],
///         1,
///     )]
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # }).unwrap();
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum GraphBuilder {
    Table {
        table: String,
        scan: Option<StaticScanSpec>,
        variant_projection: Option<String>,
    },
    InlineRecords {
        output: RecordDescriptor,
        records: Vec<Vec<u8>>,
    },
    Index {
        table: String,
        index: String,
        scan: Option<StaticScanSpec>,
        /// Additional index prefixes whose primary keys must also match.
        /// Row projection sources intersect these encoded index entries before
        /// fetching table records, so surviving rows are decoded only once.
        intersections: Vec<(String, StaticScanSpec)>,
        /// When present, fetch indexed table rows and project their variants
        /// instead of exposing the index's encoded key/value records.
        row_projection: Option<String>,
    },
    FrontierSource {
        binding: FrontierName,
        output: RecordDescriptor,
    },
    BindingSource {
        shape: String,
        output: RecordDescriptor,
    },
    Recursive {
        seed: Arc<GraphBuilder>,
        step: Arc<GraphBuilder>,
        frontier: FrontierName,
        max_iters: usize,
        /// A semantic depth bound truncates the next frontier; a fixpoint
        /// safety limit reports non-convergence instead.
        truncate_at_max_iters: bool,
    },
    Filter {
        input: Arc<GraphBuilder>,
        predicate: PredicateExpr,
        comparison: ValueComparison,
    },
    UnwrapNullable {
        input: Arc<GraphBuilder>,
        field: FieldRef,
    },
    Unnest {
        input: Arc<GraphBuilder>,
        array_field: FieldRef,
        element_field: String,
    },
    VariantProject {
        input: Arc<GraphBuilder>,
        field: FieldRef,
        case: String,
    },
    Project {
        input: Arc<GraphBuilder>,
        fields: Vec<ProjectField>,
    },
    StreamingChecksum {
        input: Arc<GraphBuilder>,
        field: FieldRef,
        output_field: String,
        window_bytes: usize,
        max_bytes_per_turn: usize,
    },
    Union {
        inputs: Vec<Arc<GraphBuilder>>,
    },
    Join {
        left: Arc<GraphBuilder>,
        right: Arc<GraphBuilder>,
        left_on: Vec<FieldRef>,
        right_on: Vec<FieldRef>,
        comparison: ValueComparison,
    },
    SemiJoin {
        left: Arc<GraphBuilder>,
        right: Arc<GraphBuilder>,
        left_on: Vec<FieldRef>,
        right_on: Vec<FieldRef>,
        comparison: ValueComparison,
    },
    AntiJoin {
        left: Arc<GraphBuilder>,
        right: Arc<GraphBuilder>,
        left_on: Vec<FieldRef>,
        right_on: Vec<FieldRef>,
        comparison: ValueComparison,
    },
    ArgMaxBy {
        input: Arc<GraphBuilder>,
        group_cols: Vec<FieldRef>,
        order_cols: Vec<FieldRef>,
    },
    ArgMinBy {
        input: Arc<GraphBuilder>,
        group_cols: Vec<FieldRef>,
        order_cols: Vec<FieldRef>,
    },
    TopBy {
        input: Arc<GraphBuilder>,
        group_cols: Vec<FieldRef>,
        order_cols: Vec<TopByOrder>,
        tie_cols: Vec<FieldRef>,
        offset: u64,
        limit: TopByLimit,
    },
    CollectBy {
        input: Arc<GraphBuilder>,
        collect: Box<CollectByBuilder>,
    },
    Aggregate {
        input: Arc<GraphBuilder>,
        group_cols: Vec<FieldRef>,
        aggregates: Vec<AggregateExpr>,
    },
}

/// Public builder payload for a terminal [`GraphBuilder::CollectBy`] node.
///
/// Kept boxed in the enum so adding terminal metadata does not inflate the
/// recursive graph-builder value carried by unrelated query paths.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CollectByBuilder {
    pub mode: CollectByMode,
    pub group_cols: Vec<FieldRef>,
    pub parent_fields: Vec<CollectByField>,
    pub child_fields: Vec<CollectByField>,
    pub collection_field: String,
    /// Recursive collect-mode slots.  Empty preserves the original one-slot
    /// `collect_by` shape.
    pub slots: Vec<CollectBySlotBuilder>,
    /// Flat output projection used by [`CollectByMode::Expand`].
    pub tuple_fields: Vec<CollectByField>,
    /// Ordered source-row identity fields used by [`CollectByMode::Expand`].
    pub occurrence_id_cols: Vec<FieldRef>,
    pub order_cols: Vec<TopByOrder>,
    pub tie_cols: Vec<FieldRef>,
    pub offset: u64,
    pub limit: TopByLimit,
}

/// Public descriptor for one named child array of a tree `CollectBy`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CollectBySlotBuilder {
    /// Source fields identifying the parent record that owns this slot.
    pub group_cols: Vec<FieldRef>,
    /// Additional source fields carried with a child solely so nested slots
    /// can address that child. These fields are intentionally not part of the
    /// child projection or its rendered descriptor.
    ///
    /// Every owner-key field must also be a grouping field for this slot. That
    /// makes it stable for the child record it accompanies and prevents this
    /// metadata channel from changing the observable collection shape.
    pub owner_key_cols: Vec<FieldRef>,
    pub child_fields: Vec<CollectByField>,
    pub collection_field: String,
    pub slots: Vec<CollectBySlotBuilder>,
    pub order_cols: Vec<TopByOrder>,
    pub tie_cols: Vec<FieldRef>,
    /// Optional boolean input field marking records that contribute a real
    /// child to this slot. This lets callers retain parent anchor records for
    /// empty collections without treating the anchor as a null child.
    pub presence_col: Option<FieldRef>,
    pub offset: u64,
    pub limit: TopByLimit,
}

/// Descriptor recursion is bounded independently of Jazz's wire receiver:
/// Groove validates executable graph shape before it can allocate runtime
/// state, whereas the v4 receiver validates untrusted decoded bytes.
pub const MAX_COLLECT_BY_TREE_DEPTH: usize = 16;

impl CollectBySlotBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        group_cols: impl IntoIterator<Item = impl Into<String>>,
        child_fields: impl IntoIterator<Item = CollectByField>,
        collection_field: impl Into<String>,
        slots: impl IntoIterator<Item = CollectBySlotBuilder>,
        order_cols: impl IntoIterator<Item = TopByOrder>,
        tie_cols: impl IntoIterator<Item = impl Into<String>>,
        offset: u64,
        limit: TopByLimit,
    ) -> Self {
        Self {
            group_cols: group_cols.into_iter().map(FieldRef::name).collect(),
            owner_key_cols: Vec::new(),
            child_fields: child_fields.into_iter().collect(),
            collection_field: collection_field.into(),
            slots: slots.into_iter().collect(),
            order_cols: order_cols.into_iter().collect(),
            tie_cols: tie_cols.into_iter().map(FieldRef::name).collect(),
            presence_col: None,
            offset,
            limit,
        }
    }

    /// Require a true boolean marker before an input record contributes to
    /// this slot. Unmarked records still may serve as parent anchors.
    pub fn with_presence_col(mut self, presence_col: impl Into<String>) -> Self {
        self.presence_col = Some(FieldRef::name(presence_col));
        self
    }

    /// Carry non-rendered grouping fields to nested slots.
    ///
    /// This is for execution metadata such as a maintained query's route key;
    /// it deliberately does not alter the child record descriptor.
    pub fn with_owner_key_cols(
        mut self,
        owner_key_cols: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.owner_key_cols = owner_key_cols.into_iter().map(FieldRef::name).collect();
        self
    }
}

/// Field reference carried by graph builders.
///
/// Public constructors keep accepting names. SQL planning may resolve names
/// once and emit `Resolved` references directly.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum FieldRef {
    Name(String),
    Resolved(usize),
}

impl FieldRef {
    pub fn name(name: impl Into<String>) -> Self {
        Self::Name(name.into())
    }

    pub fn resolved(index: usize) -> Self {
        Self::Resolved(index)
    }

    pub fn display_name(&self) -> String {
        match self {
            Self::Name(name) => name.clone(),
            Self::Resolved(index) => format!("#{index}"),
        }
    }
}

impl GraphBuilder {
    pub fn table(table: impl Into<String>) -> Self {
        Self::Table {
            table: table.into(),
            scan: None,
            variant_projection: None,
        }
    }

    pub fn table_scan(table: impl Into<String>, scan: StaticScanSpec) -> Self {
        Self::Table {
            table: table.into(),
            scan: Some(scan),
            variant_projection: None,
        }
    }

    /// Read a heterogeneous table through one fixed-output projection target.
    ///
    /// Projection cases live in the runtime registry rather than this builder,
    /// so registering another source discriminator does not replace the graph
    /// node or disturb active subscriptions.
    pub fn variant_source(table: impl Into<String>, projection_target: impl Into<String>) -> Self {
        Self::Table {
            table: table.into(),
            scan: None,
            variant_projection: Some(projection_target.into()),
        }
    }

    /// Read a bounded range of a heterogeneous table through one fixed-output
    /// projection target.
    pub fn variant_source_scan(
        table: impl Into<String>,
        projection_target: impl Into<String>,
        scan: StaticScanSpec,
    ) -> Self {
        Self::Table {
            table: table.into(),
            scan: Some(scan),
            variant_projection: Some(projection_target.into()),
        }
    }

    pub fn inline_records(
        output: RecordDescriptor,
        records: impl IntoIterator<Item = Vec<u8>>,
    ) -> Self {
        Self::InlineRecords {
            output,
            records: records.into_iter().collect(),
        }
    }

    pub fn values(
        output: RecordDescriptor,
        rows: impl IntoIterator<Item = impl AsRef<[Value]>>,
    ) -> Result<Self, crate::records::Error> {
        let records = rows
            .into_iter()
            .map(|row| output.create(row.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::inline_records(output, records))
    }

    pub fn index(table: impl Into<String>, index: impl Into<String>) -> Self {
        Self::Index {
            table: table.into(),
            index: index.into(),
            scan: None,
            intersections: Vec::new(),
            row_projection: None,
        }
    }

    pub fn index_scan(
        table: impl Into<String>,
        index: impl Into<String>,
        scan: StaticScanSpec,
    ) -> Self {
        Self::Index {
            table: table.into(),
            index: index.into(),
            scan: Some(scan),
            intersections: Vec::new(),
            row_projection: None,
        }
    }

    /// Read table rows selected by a durable secondary index through one
    /// fixed-output variant projection target.
    pub fn variant_index_scan(
        table: impl Into<String>,
        index: impl Into<String>,
        projection_target: impl Into<String>,
        scan: StaticScanSpec,
    ) -> Self {
        Self::Index {
            table: table.into(),
            index: index.into(),
            scan: Some(scan),
            intersections: Vec::new(),
            row_projection: Some(projection_target.into()),
        }
    }

    /// Read projected table rows selected by the intersection of durable
    /// secondary-index scans.
    pub fn variant_index_intersection_scan(
        table: impl Into<String>,
        index: impl Into<String>,
        scan: StaticScanSpec,
        intersections: impl IntoIterator<Item = (String, StaticScanSpec)>,
        projection_target: impl Into<String>,
    ) -> Self {
        Self::Index {
            table: table.into(),
            index: index.into(),
            scan: Some(scan),
            intersections: intersections.into_iter().collect(),
            row_projection: Some(projection_target.into()),
        }
    }

    pub fn frontier_source(binding: impl Into<String>, output: RecordDescriptor) -> Self {
        Self::FrontierSource {
            binding: FrontierName(binding.into()),
            output,
        }
    }

    pub fn binding_source(shape: impl Into<String>, output: RecordDescriptor) -> Self {
        Self::BindingSource {
            shape: shape.into(),
            output,
        }
    }

    pub fn recursive(
        seed: GraphBuilder,
        step: GraphBuilder,
        frontier: impl Into<String>,
        max_iters: usize,
    ) -> Self {
        Self::recursive_with_limit(seed, step, frontier, max_iters, false)
    }

    /// Build recursion whose iteration count is an observable depth cutoff.
    ///
    /// Unlike [`Self::recursive`]'s non-convergence guard, reaching this bound
    /// discards the next frontier and returns every fact accumulated through
    /// `max_iters` recursive steps. A zero bound returns only the seed.
    pub fn recursive_bounded(
        seed: GraphBuilder,
        step: GraphBuilder,
        frontier: impl Into<String>,
        max_iters: usize,
    ) -> Self {
        Self::recursive_with_limit(seed, step, frontier, max_iters, true)
    }

    fn recursive_with_limit(
        seed: GraphBuilder,
        step: GraphBuilder,
        frontier: impl Into<String>,
        max_iters: usize,
        truncate_at_max_iters: bool,
    ) -> Self {
        Self::Recursive {
            seed: Arc::new(seed),
            step: Arc::new(step),
            frontier: FrontierName(frontier.into()),
            max_iters,
            truncate_at_max_iters,
        }
    }

    pub fn union(inputs: impl IntoIterator<Item = GraphBuilder>) -> Self {
        Self::Union {
            inputs: inputs.into_iter().map(Arc::new).collect(),
        }
    }

    /// Return all builder fragments in child-before-parent order without
    /// recursion. Runtime setup uses this for deeply nested valid query and
    /// policy graphs, which must not consume the owner thread's call stack.
    pub(crate) fn postorder(&self) -> Vec<&Self> {
        let mut pending = vec![(self, false)];
        let mut ordered = Vec::new();
        while let Some((graph, visited)) = pending.pop() {
            if visited {
                ordered.push(graph);
                continue;
            }
            pending.push((graph, true));
            match graph {
                Self::Filter { input, .. }
                | Self::Project { input, .. }
                | Self::StreamingChecksum { input, .. }
                | Self::UnwrapNullable { input, .. }
                | Self::Unnest { input, .. }
                | Self::VariantProject { input, .. }
                | Self::ArgMaxBy { input, .. }
                | Self::ArgMinBy { input, .. }
                | Self::TopBy { input, .. }
                | Self::CollectBy { input, .. }
                | Self::Aggregate { input, .. } => pending.push((input, false)),
                Self::Union { inputs } => {
                    pending.extend(inputs.iter().rev().map(|input| (input.as_ref(), false)));
                }
                Self::Join { left, right, .. }
                | Self::SemiJoin { left, right, .. }
                | Self::AntiJoin { left, right, .. } => {
                    pending.push((right, false));
                    pending.push((left, false));
                }
                Self::Recursive { seed, step, .. } => {
                    pending.push((step, false));
                    pending.push((seed, false));
                }
                Self::Table { .. }
                | Self::InlineRecords { .. }
                | Self::Index { .. }
                | Self::FrontierSource { .. }
                | Self::BindingSource { .. } => {}
            }
        }
        ordered
    }

    pub fn join(
        left: GraphBuilder,
        right: GraphBuilder,
        left_on: impl IntoIterator<Item = impl Into<String>>,
        right_on: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::Join {
            left: Arc::new(left),
            right: Arc::new(right),
            left_on: left_on.into_iter().map(FieldRef::name).collect(),
            right_on: right_on.into_iter().map(FieldRef::name).collect(),
            comparison: ValueComparison::Exact,
        }
    }

    /// Join using policy value comparison semantics.
    pub fn policy_join(
        left: GraphBuilder,
        right: GraphBuilder,
        left_on: impl IntoIterator<Item = impl Into<String>>,
        right_on: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::Join {
            left: Arc::new(left),
            right: Arc::new(right),
            left_on: left_on.into_iter().map(FieldRef::name).collect(),
            right_on: right_on.into_iter().map(FieldRef::name).collect(),
            comparison: ValueComparison::Policy,
        }
    }

    pub fn semi_join(
        left: GraphBuilder,
        right: GraphBuilder,
        left_on: impl IntoIterator<Item = impl Into<String>>,
        right_on: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::SemiJoin {
            left: Arc::new(left),
            right: Arc::new(right),
            left_on: left_on.into_iter().map(FieldRef::name).collect(),
            right_on: right_on.into_iter().map(FieldRef::name).collect(),
            comparison: ValueComparison::Exact,
        }
    }

    pub fn anti_join(
        left: GraphBuilder,
        right: GraphBuilder,
        left_on: impl IntoIterator<Item = impl Into<String>>,
        right_on: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::AntiJoin {
            left: Arc::new(left),
            right: Arc::new(right),
            left_on: left_on.into_iter().map(FieldRef::name).collect(),
            right_on: right_on.into_iter().map(FieldRef::name).collect(),
            comparison: ValueComparison::Exact,
        }
    }

    pub fn arg_max_by(
        input: GraphBuilder,
        group_cols: impl IntoIterator<Item = impl Into<String>>,
        order_cols: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::ArgMaxBy {
            input: Arc::new(input),
            group_cols: group_cols.into_iter().map(FieldRef::name).collect(),
            order_cols: order_cols.into_iter().map(FieldRef::name).collect(),
        }
    }

    pub fn arg_min_by(
        input: GraphBuilder,
        group_cols: impl IntoIterator<Item = impl Into<String>>,
        order_cols: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::ArgMinBy {
            input: Arc::new(input),
            group_cols: group_cols.into_iter().map(FieldRef::name).collect(),
            order_cols: order_cols.into_iter().map(FieldRef::name).collect(),
        }
    }

    pub fn top_by(
        input: GraphBuilder,
        group_cols: impl IntoIterator<Item = impl Into<String>>,
        order_cols: impl IntoIterator<Item = TopByOrder>,
        tie_cols: impl IntoIterator<Item = impl Into<String>>,
        offset: u64,
        limit: TopByLimit,
    ) -> Self {
        Self::TopBy {
            input: Arc::new(input),
            group_cols: group_cols.into_iter().map(FieldRef::name).collect(),
            order_cols: order_cols.into_iter().collect(),
            tie_cols: tie_cols.into_iter().map(FieldRef::name).collect(),
            offset,
            limit,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn collect_by(
        input: GraphBuilder,
        group_cols: impl IntoIterator<Item = impl Into<String>>,
        parent_fields: impl IntoIterator<Item = CollectByField>,
        child_fields: impl IntoIterator<Item = CollectByField>,
        collection_field: impl Into<String>,
        order_cols: impl IntoIterator<Item = TopByOrder>,
        tie_cols: impl IntoIterator<Item = impl Into<String>>,
        offset: u64,
        limit: TopByLimit,
    ) -> Self {
        Self::CollectBy {
            input: Arc::new(input),
            collect: Box::new(CollectByBuilder {
                mode: CollectByMode::Collect,
                group_cols: group_cols.into_iter().map(FieldRef::name).collect(),
                parent_fields: parent_fields.into_iter().collect(),
                child_fields: child_fields.into_iter().collect(),
                collection_field: collection_field.into(),
                slots: Vec::new(),
                tuple_fields: Vec::new(),
                occurrence_id_cols: Vec::new(),
                order_cols: order_cols.into_iter().collect(),
                tie_cols: tie_cols.into_iter().map(FieldRef::name).collect(),
                offset,
                limit,
            }),
        }
    }

    /// Render one parent with a recursive tree of named child arrays.
    #[allow(clippy::too_many_arguments)]
    pub fn collect_by_tree(
        input: GraphBuilder,
        group_cols: impl IntoIterator<Item = impl Into<String>>,
        parent_fields: impl IntoIterator<Item = CollectByField>,
        slots: impl IntoIterator<Item = CollectBySlotBuilder>,
    ) -> Self {
        Self::collect_by_tree_ordered(
            input,
            group_cols,
            parent_fields,
            slots,
            Vec::<TopByOrder>::new(),
            Vec::<String>::new(),
            0,
            TopByLimit::Unbounded,
        )
    }

    /// Render a tree terminal whose roots also have an explicit public order
    /// and window.
    #[allow(clippy::too_many_arguments)]
    pub fn collect_by_tree_ordered(
        input: GraphBuilder,
        group_cols: impl IntoIterator<Item = impl Into<String>>,
        parent_fields: impl IntoIterator<Item = CollectByField>,
        slots: impl IntoIterator<Item = CollectBySlotBuilder>,
        order_cols: impl IntoIterator<Item = TopByOrder>,
        tie_cols: impl IntoIterator<Item = impl Into<String>>,
        offset: u64,
        limit: TopByLimit,
    ) -> Self {
        Self::CollectBy {
            input: Arc::new(input),
            collect: Box::new(CollectByBuilder {
                mode: CollectByMode::Collect,
                group_cols: group_cols.into_iter().map(FieldRef::name).collect(),
                parent_fields: parent_fields.into_iter().collect(),
                child_fields: Vec::new(),
                collection_field: String::new(),
                slots: slots.into_iter().collect(),
                tuple_fields: Vec::new(),
                occurrence_id_cols: Vec::new(),
                order_cols: order_cols.into_iter().collect(),
                tie_cols: tie_cols.into_iter().map(FieldRef::name).collect(),
                offset,
                limit,
            }),
        }
    }

    /// Render one public root record without child collection slots.
    #[allow(clippy::too_many_arguments)]
    pub fn collect_root_ordered(
        input: GraphBuilder,
        group_cols: impl IntoIterator<Item = impl Into<String>>,
        parent_fields: impl IntoIterator<Item = CollectByField>,
        order_cols: impl IntoIterator<Item = TopByOrder>,
        tie_cols: impl IntoIterator<Item = impl Into<String>>,
        offset: u64,
        limit: TopByLimit,
    ) -> Self {
        Self::CollectBy {
            input: Arc::new(input),
            collect: Box::new(CollectByBuilder {
                mode: CollectByMode::Root,
                group_cols: group_cols.into_iter().map(FieldRef::name).collect(),
                parent_fields: parent_fields.into_iter().collect(),
                child_fields: Vec::new(),
                collection_field: String::new(),
                slots: Vec::new(),
                tuple_fields: Vec::new(),
                occurrence_id_cols: Vec::new(),
                order_cols: order_cols.into_iter().collect(),
                tie_cols: tie_cols.into_iter().map(FieldRef::name).collect(),
                offset,
                limit,
            }),
        }
    }

    /// Render the selected rows of a grouped ordered stream as flat tuples.
    ///
    /// `occurrence_id_cols` are source-row ids in root-then-join order. They
    /// address the rendered tuple independently of its bytes, so equal tuples
    /// from different joins remain distinct while ambiguous duplicate ids are
    /// rejected by the terminal.
    #[allow(clippy::too_many_arguments)]
    pub fn collect_by_expand(
        input: GraphBuilder,
        group_cols: impl IntoIterator<Item = impl Into<String>>,
        tuple_fields: impl IntoIterator<Item = CollectByField>,
        occurrence_id_cols: impl IntoIterator<Item = impl Into<String>>,
        order_cols: impl IntoIterator<Item = TopByOrder>,
        tie_cols: impl IntoIterator<Item = impl Into<String>>,
        offset: u64,
        limit: TopByLimit,
    ) -> Self {
        Self::CollectBy {
            input: Arc::new(input),
            collect: Box::new(CollectByBuilder {
                mode: CollectByMode::Expand,
                group_cols: group_cols.into_iter().map(FieldRef::name).collect(),
                parent_fields: Vec::new(),
                child_fields: Vec::new(),
                collection_field: String::new(),
                slots: Vec::new(),
                tuple_fields: tuple_fields.into_iter().collect(),
                occurrence_id_cols: occurrence_id_cols.into_iter().map(FieldRef::name).collect(),
                order_cols: order_cols.into_iter().collect(),
                tie_cols: tie_cols.into_iter().map(FieldRef::name).collect(),
                offset,
                limit,
            }),
        }
    }

    /// Alias for callers that prefer the terminal mode before its name.
    #[allow(clippy::too_many_arguments)]
    pub fn expand_by(
        input: GraphBuilder,
        group_cols: impl IntoIterator<Item = impl Into<String>>,
        tuple_fields: impl IntoIterator<Item = CollectByField>,
        occurrence_id_cols: impl IntoIterator<Item = impl Into<String>>,
        order_cols: impl IntoIterator<Item = TopByOrder>,
        tie_cols: impl IntoIterator<Item = impl Into<String>>,
        offset: u64,
        limit: TopByLimit,
    ) -> Self {
        Self::collect_by_expand(
            input,
            group_cols,
            tuple_fields,
            occurrence_id_cols,
            order_cols,
            tie_cols,
            offset,
            limit,
        )
    }

    pub fn aggregate(
        input: GraphBuilder,
        group_cols: impl IntoIterator<Item = impl Into<String>>,
        aggregates: impl IntoIterator<Item = AggregateExpr>,
    ) -> Self {
        Self::Aggregate {
            input: Arc::new(input),
            group_cols: group_cols.into_iter().map(FieldRef::name).collect(),
            aggregates: aggregates.into_iter().collect(),
        }
    }

    pub fn filter(self, predicate: PredicateExpr) -> Self {
        Self::Filter {
            input: Arc::new(self),
            predicate,
            comparison: ValueComparison::Exact,
        }
    }

    /// Filter using policy value comparison semantics.
    pub fn policy_filter(self, predicate: PredicateExpr) -> Self {
        Self::Filter {
            input: Arc::new(self),
            predicate,
            comparison: ValueComparison::Policy,
        }
    }

    pub fn unwrap_nullable(self, field: impl Into<String>) -> Self {
        Self::UnwrapNullable {
            input: Arc::new(self),
            field: FieldRef::name(field),
        }
    }

    pub fn unnest(self, array_field: impl Into<String>, element_field: impl Into<String>) -> Self {
        Self::Unnest {
            input: Arc::new(self),
            array_field: FieldRef::name(array_field),
            element_field: element_field.into(),
        }
    }

    /// Select one named case from an enum field. Nonmatching rows emit no
    /// delta; matching rows emit the case's fixed payload descriptor.
    pub fn variant_project(self, field: impl Into<String>, case: impl Into<String>) -> Self {
        Self::VariantProject {
            input: Arc::new(self),
            field: FieldRef::name(field),
            case: case.into(),
        }
    }

    pub fn project(self, fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::Project {
            input: Arc::new(self),
            fields: fields.into_iter().map(ProjectField::named).collect(),
        }
    }

    pub fn project_fields(self, fields: impl IntoIterator<Item = ProjectField>) -> Self {
        Self::Project {
            input: Arc::new(self),
            fields: fields.into_iter().collect(),
        }
    }

    /// Internal conformance operator for exercising bounded streaming-node
    /// scheduling and scaling invariants. It is public so black-box integration
    /// and benchmark harnesses can build a graph through the ordinary API; it
    /// is not intended as an endorsed application-level checksum facility.
    #[doc(hidden)]
    pub fn streaming_checksum(
        self,
        field: impl Into<String>,
        output_field: impl Into<String>,
        window_bytes: usize,
        max_bytes_per_turn: usize,
    ) -> Self {
        Self::StreamingChecksum {
            input: Arc::new(self),
            field: FieldRef::name(field),
            output_field: output_field.into(),
            window_bytes,
            max_bytes_per_turn,
        }
    }
}

/// Field selected by a Project builder, optionally renamed.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProjectField {
    pub expression: ProjectExpr,
    pub output_name: String,
}

impl ProjectField {
    pub fn named(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            expression: ProjectExpr::Field(FieldRef::name(name.clone())),
            output_name: name,
        }
    }

    pub fn renamed(source_name: impl Into<String>, output_name: impl Into<String>) -> Self {
        Self {
            expression: ProjectExpr::Field(FieldRef::name(source_name)),
            output_name: output_name.into(),
        }
    }

    pub fn renamed_resolved(source_idx: usize, output_name: impl Into<String>) -> Self {
        Self {
            expression: ProjectExpr::Field(FieldRef::resolved(source_idx)),
            output_name: output_name.into(),
        }
    }

    pub fn literal(output_name: impl Into<String>, value: impl Into<LiteralValue>) -> Self {
        Self {
            expression: ProjectExpr::Literal(value.into()),
            output_name: output_name.into(),
        }
    }

    pub fn literal_typed(
        output_name: impl Into<String>,
        value: impl Into<LiteralValue>,
        value_type: ValueType,
    ) -> Self {
        Self {
            expression: ProjectExpr::TypedLiteral {
                value: value.into(),
                value_type,
            },
            output_name: output_name.into(),
        }
    }

    /// Create a null projection with the legacy default type `Nullable(Bytes)`.
    /// Use [`Self::null_typed`] when the output schema matters.
    pub fn null(output_name: impl Into<String>) -> Self {
        Self::null_typed(output_name, ValueType::Nullable(Box::new(ValueType::Bytes)))
    }

    pub fn null_typed(output_name: impl Into<String>, value_type: ValueType) -> Self {
        Self {
            expression: ProjectExpr::Null(value_type),
            output_name: output_name.into(),
        }
    }

    pub fn nullable(source_name: impl Into<String>, output_name: impl Into<String>) -> Self {
        Self {
            expression: ProjectExpr::Nullable(FieldRef::name(source_name)),
            output_name: output_name.into(),
        }
    }

    pub fn nullable_resolved(source_idx: usize, output_name: impl Into<String>) -> Self {
        Self {
            expression: ProjectExpr::Nullable(FieldRef::resolved(source_idx)),
            output_name: output_name.into(),
        }
    }

    pub fn nullable_flat(source_name: impl Into<String>, output_name: impl Into<String>) -> Self {
        Self {
            expression: ProjectExpr::NullableFlat(FieldRef::name(source_name)),
            output_name: output_name.into(),
        }
    }

    /// Remap a scalar enum's compact source discriminant into another
    /// descriptor's discriminant. `None` is a deliberate non-total projection
    /// and fails execution rather than fabricating a case.
    pub fn enum_tag_remap(
        source_name: impl Into<String>,
        output_name: impl Into<String>,
        tags: Vec<Option<u8>>,
    ) -> Self {
        Self {
            expression: ProjectExpr::EnumTagRemap {
                source: FieldRef::name(source_name),
                tags,
            },
            output_name: output_name.into(),
        }
    }

    /// Remap a payload enum's case tag while retaining its selected payload.
    /// As for scalar enums, `None` is a deliberate non-total projection.
    pub fn enum_remap(
        source_name: impl Into<String>,
        output_name: impl Into<String>,
        tags: Vec<Option<u32>>,
    ) -> Self {
        Self {
            expression: ProjectExpr::EnumRemap {
                source: FieldRef::name(source_name),
                tags,
            },
            output_name: output_name.into(),
        }
    }

    /// Recursively re-encode enum tags at every mapped nested occurrence.
    /// This is descriptor-aware and therefore deliberately cannot take the
    /// raw-copy projection fast path.
    pub fn recursive_enum_remap(
        source_name: impl Into<String>,
        output_name: impl Into<String>,
        target: ValueType,
        remaps: RecursiveEnumRemaps,
    ) -> Self {
        Self {
            expression: ProjectExpr::RecursiveEnumRemap {
                source: FieldRef::name(source_name),
                target,
                remaps,
                omit_unrepresentable: false,
            },
            output_name: output_name.into(),
        }
    }

    /// Recursively re-encode enum tags and omit a row when a target schema
    /// cannot represent one of its cases. This is reserved for Jazz's
    /// compatibility boundary; ordinary descriptor errors still surface.
    pub fn recursive_enum_remap_omitting_unrepresentable(
        source_name: impl Into<String>,
        output_name: impl Into<String>,
        target: ValueType,
        remaps: RecursiveEnumRemaps,
    ) -> Self {
        Self {
            expression: ProjectExpr::RecursiveEnumRemap {
                source: FieldRef::name(source_name),
                target,
                remaps,
                omit_unrepresentable: true,
            },
            output_name: output_name.into(),
        }
    }

    pub fn source(&self) -> Option<&FieldRef> {
        match &self.expression {
            ProjectExpr::Field(source)
            | ProjectExpr::Nullable(source)
            | ProjectExpr::NullableFlat(source)
            | ProjectExpr::EnumTagRemap { source, .. }
            | ProjectExpr::EnumRemap { source, .. }
            | ProjectExpr::RecursiveEnumRemap { source, .. } => Some(source),
            ProjectExpr::Literal(_) | ProjectExpr::TypedLiteral { .. } | ProjectExpr::Null(_) => {
                None
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ProjectExpr {
    Field(FieldRef),
    Literal(LiteralValue),
    TypedLiteral {
        value: LiteralValue,
        value_type: ValueType,
    },
    Null(ValueType),
    Nullable(FieldRef),
    NullableFlat(FieldRef),
    EnumTagRemap {
        source: FieldRef,
        tags: Vec<Option<u8>>,
    },
    EnumRemap {
        source: FieldRef,
        tags: Vec<Option<u32>>,
    },
    RecursiveEnumRemap {
        source: FieldRef,
        target: ValueType,
        remaps: RecursiveEnumRemaps,
        omit_unrepresentable: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopByOrder {
    pub field: FieldRef,
    pub direction: TopByDirection,
}

/// Named source field in a `CollectBy` parent or child projection.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CollectByField {
    pub field: FieldRef,
    pub output_name: String,
    /// Remove at most one nullable wrapper while rendering the terminal field.
    /// This is useful when a flat union makes an otherwise required field
    /// nullable solely to represent an absent child row.
    pub unwrap_nullable: bool,
}

impl CollectByField {
    pub fn named(field: impl Into<String>) -> Self {
        let field = field.into();
        Self {
            field: FieldRef::name(field.clone()),
            output_name: field,
            unwrap_nullable: false,
        }
    }

    pub fn renamed(field: impl Into<String>, output_name: impl Into<String>) -> Self {
        Self {
            field: FieldRef::name(field),
            output_name: output_name.into(),
            unwrap_nullable: false,
        }
    }

    pub fn renamed_unwrap_nullable(
        field: impl Into<String>,
        output_name: impl Into<String>,
    ) -> Self {
        Self {
            field: FieldRef::name(field),
            output_name: output_name.into(),
            unwrap_nullable: true,
        }
    }
}

impl TopByOrder {
    pub fn asc(field: impl Into<String>) -> Self {
        Self {
            field: FieldRef::name(field),
            direction: TopByDirection::Asc,
        }
    }

    pub fn desc(field: impl Into<String>) -> Self {
        Self {
            field: FieldRef::name(field),
            direction: TopByDirection::Desc,
        }
    }
}

fn collect_projection_output_type(
    input: &ValueType,
    projection: &CollectByProjection,
) -> Option<ValueType> {
    if !projection.unwrap_nullable {
        return Some(input.clone());
    }
    match input {
        ValueType::Nullable(inner) => Some((**inner).clone()),
        other => Some(other.clone()),
    }
}

/// Deduplicated DAG of IVM node descriptors.
#[derive(Clone, Debug, Default)]
pub struct IvmGraph {
    /// Deduplicated node specs. The `NodeId` is derived from the full
    /// descriptor, and insertion asserts that collisions do not merge specs.
    nodes: HashMap<NodeId, GraphNode>,
    table_sources: HashMap<String, HashSet<NodeId>>,
    binding_sources: HashMap<String, HashSet<NodeId>>,
}

impl IvmGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn dedup_node(&mut self, descriptor: NodeDescriptor, durability: NodeDurability) -> NodeId {
        self.validate_node(&descriptor)
            .expect("invalid IVM graph node descriptor");

        let id = descriptor.node_id();
        if let Some(existing) = self.nodes.get(&id) {
            assert_eq!(
                existing.descriptor, descriptor,
                "IVM node id collision for incompatible descriptors"
            );
            return id;
        }

        for input in &descriptor.inputs {
            if let Some(input_node) = self.nodes.get_mut(input) {
                input_node.children.insert(id);
            }
        }

        match &descriptor.operator {
            OpType::TableSource(source) => {
                self.table_sources
                    .entry(source.table.clone())
                    .or_default()
                    .insert(id);
            }
            OpType::IndexSource(source) => {
                self.table_sources
                    .entry(source.table.clone())
                    .or_default()
                    .insert(id);
            }
            OpType::BindingSource(source) => {
                self.binding_sources
                    .entry(source.shape.clone())
                    .or_default()
                    .insert(id);
            }
            OpType::FrontierSource(source) => {
                self.binding_sources
                    .entry(source.binding.0.clone())
                    .or_default()
                    .insert(id);
            }
            _ => {}
        }

        self.nodes
            .insert(id, GraphNode::new(descriptor, durability));
        id
    }

    /// Validate a descriptor against the graph's already-built inputs.
    ///
    /// This keeps terminality structural even for callers that construct
    /// descriptors directly instead of coming through `GraphBuilder`.
    pub fn validate_node(&self, descriptor: &NodeDescriptor) -> Result<(), GraphValidationError> {
        let input_outputs = descriptor
            .inputs
            .iter()
            .filter_map(|input| self.nodes.get(input).map(|node| node.descriptor.output))
            .collect::<Vec<_>>();
        descriptor.validate(&input_outputs)?;
        let consumes_collect_by = descriptor.inputs.iter().any(|input| {
            self.nodes
                .get(input)
                .is_some_and(|node| matches!(node.descriptor.operator, OpType::CollectBy(_)))
        });
        if consumes_collect_by {
            return Err(GraphValidationError::CollectByInputIsTerminal);
        }
        Ok(())
    }

    pub fn node(&self, id: NodeId) -> Option<&GraphNode> {
        self.nodes.get(&id)
    }

    pub fn node_mut(&mut self, id: NodeId) -> Option<&mut GraphNode> {
        self.nodes.get_mut(&id)
    }

    pub fn nodes(&self) -> &HashMap<NodeId, GraphNode> {
        &self.nodes
    }

    pub(crate) fn affected_nodes<'a>(
        &self,
        tables: impl IntoIterator<Item = &'a str>,
        bindings: impl IntoIterator<Item = &'a str>,
    ) -> std::collections::HashSet<NodeId> {
        let mut affected = std::collections::HashSet::new();
        let mut pending = tables
            .into_iter()
            .filter_map(|table| self.table_sources.get(table))
            .chain(
                bindings
                    .into_iter()
                    .filter_map(|binding| self.binding_sources.get(binding)),
            )
            .flat_map(|nodes| nodes.iter().copied())
            .collect::<Vec<_>>();
        while let Some(node) = pending.pop() {
            if !affected.insert(node) {
                continue;
            }
            if let Some(graph_node) = self.nodes.get(&node) {
                pending.extend(graph_node.children.iter().copied());
            }
        }
        affected
    }

    pub fn mark_ancestors<S>(&self, id: NodeId, retained: &mut std::collections::HashSet<NodeId, S>)
    where
        S: BuildHasher,
    {
        if !retained.insert(id) {
            return;
        }

        if let Some(node) = self.nodes.get(&id) {
            for input in &node.descriptor.inputs {
                self.mark_ancestors(*input, retained);
            }
        }
    }

    pub fn remove_node(&mut self, id: NodeId) {
        let Some(node) = self.nodes.remove(&id) else {
            return;
        };

        for input in node.descriptor.inputs {
            if let Some(input_node) = self.nodes.get_mut(&input) {
                input_node.children.remove(&id);
            }
        }
        match &node.descriptor.operator {
            OpType::TableSource(source) => {
                remove_source_node(&mut self.table_sources, &source.table, id);
            }
            OpType::IndexSource(source) => {
                remove_source_node(&mut self.table_sources, &source.table, id);
            }
            OpType::BindingSource(source) => {
                remove_source_node(&mut self.binding_sources, &source.shape, id);
            }
            OpType::FrontierSource(source) => {
                remove_source_node(&mut self.binding_sources, &source.binding.0, id);
            }
            _ => {}
        }
    }
}

fn remove_source_node(sources: &mut HashMap<String, HashSet<NodeId>>, source: &str, node: NodeId) {
    let remove_source = sources.get_mut(source).is_some_and(|nodes| {
        nodes.remove(&node);
        nodes.is_empty()
    });
    if remove_source {
        sources.remove(source);
    }
}

/// One deduplicated node plus reverse edges for graph maintenance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphNode {
    pub id: NodeId,
    /// Pure node spec: operator, inputs, and output encoding.
    pub descriptor: NodeDescriptor,
    /// Durable nodes are retained even without subscriptions because they
    /// maintain storage-backed indices.
    pub durability: NodeDurability,
    /// Reverse edges make eager GC cheap when subscriptions go away.
    pub children: HashSet<NodeId>,
}

impl GraphNode {
    fn new(descriptor: NodeDescriptor, durability: NodeDurability) -> Self {
        Self {
            id: descriptor.node_id(),
            descriptor,
            durability,
            children: HashSet::default(),
        }
    }

    pub fn is_durable(&self) -> bool {
        matches!(self.durability, NodeDurability::Durable { .. })
    }
}

/// Canonical node spec used to derive node identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeDescriptor {
    /// Structural operator payload used for node identity.
    ///
    /// Descriptors are the sharing boundary for Groove nodes. Any literal or
    /// policy input that can affect output must be encoded here or in an input
    /// descriptor before cross-retainer reuse is valid.
    pub operator: OpType,
    pub inputs: Vec<NodeId>,
    pub output: NodeOutput,
}

/// The typed value produced by an IVM graph node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NodeOutput {
    Records(RecordDescriptor),
    Arrangement(ArrangementDescriptor),
}

impl NodeOutput {
    pub fn records(self) -> RecordDescriptor {
        match self {
            Self::Records(descriptor) => descriptor,
            Self::Arrangement(arrangement) => arrangement.records,
        }
    }
}

impl From<RecordDescriptor> for NodeOutput {
    fn from(value: RecordDescriptor) -> Self {
        Self::Records(value)
    }
}

/// Identity and record type of a reusable indexed graph dependency.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ArrangementDescriptor {
    pub records: RecordDescriptor,
}

impl NodeDescriptor {
    pub fn new(
        operator: OpType,
        inputs: impl IntoIterator<Item = NodeId>,
        output: impl Into<NodeOutput>,
    ) -> Self {
        Self {
            operator,
            inputs: inputs.into_iter().collect(),
            output: output.into(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        // Keep node ids deterministic across runs. They are still guarded by a
        // descriptor equality check on deduplication, so collisions fail loudly.
        let mut hasher = StableNodeHasher::default();
        self.hash(&mut hasher);
        NodeId(hasher.finish())
    }

    pub fn validate(&self, input_outputs: &[NodeOutput]) -> Result<(), GraphValidationError> {
        if self.inputs.len() != input_outputs.len() {
            return Err(GraphValidationError::InputDescriptorArityMismatch {
                inputs: self.inputs.len(),
                descriptors: input_outputs.len(),
            });
        }

        let typed_inputs = input_outputs;
        let arrangement_consumer = matches!(
            self.operator,
            OpType::Join(_)
                | OpType::SemiJoin(_)
                | OpType::AntiJoin(_)
                | OpType::ArgMaxBy(_)
                | OpType::ArgMinBy(_)
                | OpType::TopBy(_)
                | OpType::CollectBy(_)
                | OpType::Aggregate(_)
        );
        if arrangement_consumer {
            expect_arrangement_inputs(typed_inputs)?;
        } else if typed_inputs
            .iter()
            .any(|input| matches!(input, NodeOutput::Arrangement(_)))
        {
            return Err(GraphValidationError::InvalidNodeOutput);
        }
        let input_outputs = typed_inputs
            .iter()
            .copied()
            .map(NodeOutput::records)
            .collect::<Vec<_>>();
        let output = self.output.records();
        if !matches!(self.operator, OpType::Arrange(_))
            && matches!(self.output, NodeOutput::Arrangement(_))
        {
            return Err(GraphValidationError::InvalidNodeOutput);
        }
        match &self.operator {
            OpType::TableSource(_)
            | OpType::IndexSource(_)
            | OpType::InlineRecords(_)
            | OpType::FrontierSource(_)
            | OpType::BindingSource(_) => expect_arity(&self.inputs, 0),
            OpType::Arrange(_) => {
                expect_arity(&self.inputs, 1)?;
                if !matches!(typed_inputs[0], NodeOutput::Records(_))
                    || !matches!(self.output, NodeOutput::Arrangement(_))
                {
                    return Err(GraphValidationError::InvalidNodeOutput);
                }
                expect_same_output(&output, &input_outputs[0])
            }
            OpType::Filter(_) | OpType::Distinct | OpType::Negate => {
                expect_arity(&self.inputs, 1)?;
                expect_same_output(&output, &input_outputs[0])
            }
            OpType::ArgMaxBy(arg_max_by) => {
                expect_arity(&self.inputs, 1)?;
                expect_arrangement_inputs(typed_inputs)?;
                expect_same_output(&output, &input_outputs[0])?;
                for &field_idx in arg_max_by
                    .group_field_indices
                    .iter()
                    .chain(&arg_max_by.comparison_field_indices)
                {
                    if field_idx >= input_outputs[0].fields().len() {
                        return Err(GraphValidationError::FieldIndexOutOfBounds {
                            index: field_idx,
                            len: input_outputs[0].fields().len(),
                        });
                    }
                }
                Ok(())
            }
            OpType::ArgMinBy(arg_min_by) => {
                expect_arity(&self.inputs, 1)?;
                expect_arrangement_inputs(typed_inputs)?;
                expect_same_output(&output, &input_outputs[0])?;
                for &field_idx in arg_min_by
                    .group_field_indices
                    .iter()
                    .chain(&arg_min_by.comparison_field_indices)
                {
                    if field_idx >= input_outputs[0].fields().len() {
                        return Err(GraphValidationError::FieldIndexOutOfBounds {
                            index: field_idx,
                            len: input_outputs[0].fields().len(),
                        });
                    }
                }
                Ok(())
            }
            OpType::TopBy(top_by) => {
                expect_arity(&self.inputs, 1)?;
                expect_arrangement_inputs(typed_inputs)?;
                expect_same_output(&output, &input_outputs[0])?;
                for &field_idx in top_by
                    .group_field_indices
                    .iter()
                    .chain(&top_by.sort_field_indices)
                {
                    if field_idx >= input_outputs[0].fields().len() {
                        return Err(GraphValidationError::FieldIndexOutOfBounds {
                            index: field_idx,
                            len: input_outputs[0].fields().len(),
                        });
                    }
                }
                for &field_idx in &top_by.sort_field_indices {
                    if !collect_by_ordered_scalar(&input_outputs[0].fields()[field_idx].value_type)
                    {
                        return Err(GraphValidationError::TopBySortFieldMustBeOrderable);
                    }
                }
                Ok(())
            }
            OpType::CollectBy(collect_by) => {
                expect_arity(&self.inputs, 1)?;
                expect_arrangement_inputs(typed_inputs)?;
                if collect_by.mode == CollectByMode::Expand {
                    if collect_by.tuple_fields.is_empty()
                        || collect_by.occurrence_id_field_indices.is_empty()
                        || collect_by.sort_field_indices.len() != collect_by.sort_directions.len()
                        || collect_by.sort_field_indices.len()
                            != collect_by.order_fields.len() + collect_by.tie_fields.len()
                        || collect_by.order_fields.is_empty()
                        || collect_by.tie_fields.is_empty()
                        || output.fields().len() != collect_by.tuple_fields.len()
                    {
                        return Err(GraphValidationError::CollectByOutputDescriptorMismatch);
                    }
                    for field in collect_by
                        .group_field_indices
                        .iter()
                        .chain(&collect_by.sort_field_indices)
                        .chain(&collect_by.occurrence_id_field_indices)
                        .chain(collect_by.tuple_fields.iter().map(|field| &field.field_idx))
                    {
                        if *field >= input_outputs[0].fields().len() {
                            return Err(GraphValidationError::FieldIndexOutOfBounds {
                                index: *field,
                                len: input_outputs[0].fields().len(),
                            });
                        }
                    }
                    for (output_field, projection) in
                        output.fields().iter().zip(&collect_by.tuple_fields)
                    {
                        let input_field = &input_outputs[0].fields()[projection.field_idx];
                        if output_field.name.as_deref() != Some(projection.output_name.as_str())
                            || collect_projection_output_type(&input_field.value_type, projection)
                                .as_ref()
                                != Some(&output_field.value_type)
                        {
                            return Err(GraphValidationError::CollectByOutputDescriptorMismatch);
                        }
                    }
                    for &field_idx in collect_by
                        .group_field_indices
                        .iter()
                        .chain(&collect_by.sort_field_indices)
                        .chain(&collect_by.occurrence_id_field_indices)
                    {
                        let value_type = &input_outputs[0].fields()[field_idx].value_type;
                        let scalar = collect_by_ordered_scalar(value_type);
                        if !scalar || value_type.contains_record() {
                            return Err(GraphValidationError::CollectByKeyFieldMustBeScalar);
                        }
                    }
                    return Ok(());
                }
                if collect_by.mode == CollectByMode::Root {
                    if !collect_by.slots.is_empty()
                        || !collect_by.child_fields.is_empty()
                        || !collect_by.collection_field.is_empty()
                        || output.fields().len() != collect_by.parent_fields.len()
                        || collect_by.sort_field_indices.len() != collect_by.sort_directions.len()
                        || collect_by.sort_field_indices.len()
                            != collect_by.order_fields.len() + collect_by.tie_fields.len()
                        || collect_by.order_fields.is_empty()
                        || collect_by.tie_fields.is_empty()
                    {
                        return Err(GraphValidationError::CollectByOutputDescriptorMismatch);
                    }
                    for (output_field, projection) in
                        output.fields().iter().zip(&collect_by.parent_fields)
                    {
                        let input_field = input_outputs[0]
                            .fields()
                            .get(projection.field_idx)
                            .ok_or(GraphValidationError::FieldIndexOutOfBounds {
                                index: projection.field_idx,
                                len: input_outputs[0].fields().len(),
                            })?;
                        if output_field.name.as_deref() != Some(projection.output_name.as_str())
                            || collect_projection_output_type(&input_field.value_type, projection)
                                .as_ref()
                                != Some(&output_field.value_type)
                        {
                            return Err(GraphValidationError::CollectByOutputDescriptorMismatch);
                        }
                    }
                    for &field_idx in collect_by
                        .group_field_indices
                        .iter()
                        .chain(&collect_by.sort_field_indices)
                    {
                        let value_type = input_outputs[0]
                            .fields()
                            .get(field_idx)
                            .ok_or(GraphValidationError::FieldIndexOutOfBounds {
                                index: field_idx,
                                len: input_outputs[0].fields().len(),
                            })?
                            .value_type
                            .clone();
                        if !collect_by_ordered_scalar(&value_type) || value_type.contains_record() {
                            return Err(GraphValidationError::CollectByKeyFieldMustBeScalar);
                        }
                    }
                    return Ok(());
                }
                if !collect_by.slots.is_empty() {
                    if output.fields().len()
                        != collect_by.parent_fields.len() + collect_by.slots.len()
                    {
                        return Err(GraphValidationError::CollectByOutputDescriptorMismatch);
                    }
                    for (output_field, projection) in output
                        .fields()
                        .iter()
                        .take(collect_by.parent_fields.len())
                        .zip(&collect_by.parent_fields)
                    {
                        let input_field = input_outputs[0]
                            .fields()
                            .get(projection.field_idx)
                            .ok_or(GraphValidationError::FieldIndexOutOfBounds {
                                index: projection.field_idx,
                                len: input_outputs[0].fields().len(),
                            })?;
                        if output_field.name.as_deref() != Some(projection.output_name.as_str())
                            || collect_projection_output_type(&input_field.value_type, projection)
                                .as_ref()
                                != Some(&output_field.value_type)
                        {
                            return Err(GraphValidationError::CollectByOutputDescriptorMismatch);
                        }
                    }
                    for (output_field, slot) in output
                        .fields()
                        .iter()
                        .skip(collect_by.parent_fields.len())
                        .zip(&collect_by.slots)
                    {
                        if output_field.name.as_deref() != Some(slot.collection_field.as_str())
                            || output_field.value_type
                                != ValueType::Array(Box::new(ValueType::Record(Box::new(
                                    slot.child_descriptor,
                                ))))
                        {
                            return Err(GraphValidationError::CollectByOutputDescriptorMismatch);
                        }
                    }
                    return Ok(());
                }
                if collect_by.collection_field_index >= output.fields().len() {
                    return Err(GraphValidationError::FieldIndexOutOfBounds {
                        index: collect_by.collection_field_index,
                        len: output.fields().len(),
                    });
                }
                let expected_collection = ValueType::Array(Box::new(ValueType::Record(Box::new(
                    collect_by.child_descriptor,
                ))));
                if output.fields()[collect_by.collection_field_index].value_type
                    != expected_collection
                {
                    return Err(GraphValidationError::CollectByOutputDescriptorMismatch);
                }
                if output.fields().len() != collect_by.parent_fields.len() + 1
                    || output.fields()[collect_by.collection_field_index]
                        .name
                        .as_deref()
                        != Some(collect_by.collection_field.as_str())
                {
                    return Err(GraphValidationError::CollectByOutputDescriptorMismatch);
                }
                for field in collect_by
                    .group_field_indices
                    .iter()
                    .chain(&collect_by.sort_field_indices)
                    .chain(
                        collect_by
                            .parent_fields
                            .iter()
                            .map(|field| &field.field_idx),
                    )
                    .chain(collect_by.child_fields.iter().map(|field| &field.field_idx))
                {
                    if *field >= input_outputs[0].fields().len() {
                        return Err(GraphValidationError::FieldIndexOutOfBounds {
                            index: *field,
                            len: input_outputs[0].fields().len(),
                        });
                    }
                }
                if collect_by.collection_field_index != collect_by.parent_fields.len()
                    || collect_by.child_descriptor.fields().len() != collect_by.child_fields.len()
                    || collect_by.sort_field_indices.len() != collect_by.sort_directions.len()
                    || collect_by.sort_field_indices.len()
                        != collect_by.order_fields.len() + collect_by.tie_fields.len()
                    || collect_by.order_fields.is_empty()
                    || collect_by.tie_fields.is_empty()
                {
                    return Err(GraphValidationError::CollectByOutputDescriptorMismatch);
                }
                for (output_field, projection) in output
                    .fields()
                    .iter()
                    .take(collect_by.parent_fields.len())
                    .zip(&collect_by.parent_fields)
                {
                    let input_field = &input_outputs[0].fields()[projection.field_idx];
                    if output_field.name.as_deref() != Some(projection.output_name.as_str())
                        || collect_projection_output_type(&input_field.value_type, projection)
                            .as_ref()
                            != Some(&output_field.value_type)
                    {
                        return Err(GraphValidationError::CollectByOutputDescriptorMismatch);
                    }
                }
                for (child_field, projection) in collect_by
                    .child_descriptor
                    .fields()
                    .iter()
                    .zip(&collect_by.child_fields)
                {
                    let input_field = &input_outputs[0].fields()[projection.field_idx];
                    if child_field.name.as_deref() != Some(projection.output_name.as_str())
                        || collect_projection_output_type(&input_field.value_type, projection)
                            .as_ref()
                            != Some(&child_field.value_type)
                    {
                        return Err(GraphValidationError::CollectByOutputDescriptorMismatch);
                    }
                }
                for &field_idx in collect_by
                    .group_field_indices
                    .iter()
                    .chain(&collect_by.sort_field_indices)
                {
                    let value_type = &input_outputs[0].fields()[field_idx].value_type;
                    let scalar = collect_by_ordered_scalar(value_type);
                    if !scalar || value_type.contains_record() {
                        return Err(GraphValidationError::CollectByKeyFieldMustBeScalar);
                    }
                }
                Ok(())
            }
            OpType::UnwrapNullable(unwrap) => {
                expect_arity(&self.inputs, 1)?;
                if unwrap.field_idx >= input_outputs[0].fields().len() {
                    return Err(GraphValidationError::FieldIndexOutOfBounds {
                        index: unwrap.field_idx,
                        len: input_outputs[0].fields().len(),
                    });
                }
                Ok(())
            }
            OpType::Unnest(unnest) => {
                expect_arity(&self.inputs, 1)?;
                if unnest.array_field_idx >= input_outputs[0].fields().len() {
                    return Err(GraphValidationError::FieldIndexOutOfBounds {
                        index: unnest.array_field_idx,
                        len: input_outputs[0].fields().len(),
                    });
                }
                Ok(())
            }
            OpType::VariantProject(variant_project) => {
                expect_arity(&self.inputs, 1)?;
                if variant_project.field_idx >= input_outputs[0].fields().len() {
                    return Err(GraphValidationError::FieldIndexOutOfBounds {
                        index: variant_project.field_idx,
                        len: input_outputs[0].fields().len(),
                    });
                }
                Ok(())
            }
            OpType::MapProject(project) => {
                expect_arity(&self.inputs, 1)?;
                for &(_, field_idx) in &project.mapping {
                    if field_idx >= input_outputs[0].fields().len() {
                        return Err(GraphValidationError::FieldIndexOutOfBounds {
                            index: field_idx,
                            len: input_outputs[0].fields().len(),
                        });
                    }
                }
                if !project.expressions.is_empty()
                    && project.expressions.len() != output.fields().len()
                {
                    return Err(GraphValidationError::OutputFieldCountMismatch {
                        expected: project.expressions.len(),
                        actual: output.fields().len(),
                    });
                }
                if project.expressions.is_empty() && project.mapping.len() != output.fields().len()
                {
                    return Err(GraphValidationError::OutputFieldCountMismatch {
                        expected: project.mapping.len(),
                        actual: output.fields().len(),
                    });
                }
                Ok(())
            }
            OpType::StreamingChecksum(checksum) => {
                expect_arity(&self.inputs, 1)?;
                if checksum.field_idx >= input_outputs[0].fields().len() {
                    return Err(GraphValidationError::FieldIndexOutOfBounds {
                        index: checksum.field_idx,
                        len: input_outputs[0].fields().len(),
                    });
                }
                if checksum.window_bytes == 0 || checksum.max_bytes_per_turn == 0 {
                    return Err(GraphValidationError::OutputDescriptorMismatch);
                }
                Ok(())
            }
            OpType::IndexBy(index) => {
                expect_arity(&self.inputs, 1)?;
                for &field_idx in index.key_fields.iter().chain(&index.value_fields) {
                    if field_idx >= input_outputs[0].fields().len() {
                        return Err(GraphValidationError::FieldIndexOutOfBounds {
                            index: field_idx,
                            len: input_outputs[0].fields().len(),
                        });
                    }
                }
                Ok(())
            }
            OpType::Persist(persist) => {
                expect_arity(&self.inputs, 1)?;
                expect_same_output(&output, &input_outputs[0])?;
                for &field_idx in &persist.key_fields {
                    if field_idx >= output.fields().len() {
                        return Err(GraphValidationError::FieldIndexOutOfBounds {
                            index: field_idx,
                            len: output.fields().len(),
                        });
                    }
                }
                Ok(())
            }
            OpType::Join(join) | OpType::SemiJoin(join) | OpType::AntiJoin(join) => {
                expect_arity(&self.inputs, 2)?;
                expect_arrangement_inputs(typed_inputs)?;
                if join.left_descriptor != input_outputs[0]
                    || join.right_descriptor != input_outputs[1]
                {
                    return Err(GraphValidationError::JoinInputDescriptorMismatch);
                }
                if join.left_key.len() != join.right_key.len() {
                    return Err(GraphValidationError::JoinKeyArityMismatch {
                        left: join.left_key.len(),
                        right: join.right_key.len(),
                    });
                }
                Ok(())
            }
            OpType::Union => {
                if self.inputs.is_empty() {
                    return Ok(());
                }
                for input_output in input_outputs {
                    expect_same_output(&output, &input_output)?;
                }
                Ok(())
            }
            OpType::Aggregate(_) => {
                expect_arity(&self.inputs, 1)?;
                expect_arrangement_inputs(typed_inputs)?;
                if let OpType::Aggregate(aggregate) = &self.operator {
                    for &field_idx in &aggregate.group_field_indices {
                        if field_idx >= input_outputs[0].fields().len() {
                            return Err(GraphValidationError::FieldIndexOutOfBounds {
                                index: field_idx,
                                len: input_outputs[0].fields().len(),
                            });
                        }
                    }
                }
                Ok(())
            }
            OpType::Recursive(_) => expect_arity(&self.inputs, 2),
        }
    }
}

fn expect_arity(inputs: &[NodeId], expected: usize) -> Result<(), GraphValidationError> {
    if inputs.len() == expected {
        Ok(())
    } else {
        Err(GraphValidationError::InputArityMismatch {
            expected,
            actual: inputs.len(),
        })
    }
}

fn expect_same_output(
    expected: &RecordDescriptor,
    actual: &RecordDescriptor,
) -> Result<(), GraphValidationError> {
    if expected.registry_compatible_with(actual) {
        Ok(())
    } else {
        Err(GraphValidationError::OutputDescriptorMismatch)
    }
}

fn expect_arrangement_inputs(inputs: &[NodeOutput]) -> Result<(), GraphValidationError> {
    if inputs
        .iter()
        .all(|input| matches!(input, NodeOutput::Arrangement(_)))
    {
        Ok(())
    } else {
        Err(GraphValidationError::InvalidNodeOutput)
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum GraphValidationError {
    #[error("operator has an incompatible typed output")]
    InvalidNodeOutput,
    #[error("field index {index} out of bounds for {len} fields")]
    FieldIndexOutOfBounds { index: usize, len: usize },
    #[error("expected {expected} inputs, got {actual}")]
    InputArityMismatch { expected: usize, actual: usize },
    #[error("input count {inputs} does not match descriptor count {descriptors}")]
    InputDescriptorArityMismatch { inputs: usize, descriptors: usize },
    #[error("join input descriptors do not match")]
    JoinInputDescriptorMismatch,
    #[error("join key arity mismatch: {left} vs {right}")]
    JoinKeyArityMismatch { left: usize, right: usize },
    #[error("output descriptor mismatch")]
    OutputDescriptorMismatch,
    #[error("expected {expected} output fields, got {actual}")]
    OutputFieldCountMismatch { expected: usize, actual: usize },
    #[error("collect_by output descriptor does not match its projection and collection slot")]
    CollectByOutputDescriptorMismatch,
    #[error("collect_by group, order, and tie fields must be scalar and non-record-valued")]
    CollectByKeyFieldMustBeScalar,
    #[error("top_by order and tie fields must be orderable scalar values")]
    TopBySortFieldMustBeOrderable,
    #[error("collect_by is terminal-only and cannot be an input node")]
    CollectByInputIsTerminal,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum OpType {
    TableSource(TableSourceOp),
    IndexSource(IndexSourceOp),
    InlineRecords(InlineRecordsOp),
    FrontierSource(FrontierSourceOp),
    BindingSource(BindingSourceOp),
    Arrange(ArrangeOp),
    ArgMaxBy(ArgMaxByOp),
    ArgMinBy(ArgMinByOp),
    TopBy(TopByOp),
    /// Box the terminal collector descriptor so its wide payload does not
    /// inflate every recursive graph-compilation frame.
    CollectBy(Box<CollectByOp>),
    Recursive(RecursiveOp),
    Persist(PersistOp),
    Filter(FilterOp),
    MapProject(MapProjectOp),
    StreamingChecksum(StreamingChecksumOp),
    UnwrapNullable(UnwrapNullableOp),
    Unnest(UnnestOp),
    VariantProject(VariantProjectOp),
    IndexBy(IndexByOp),
    Join(JoinOp),
    SemiJoin(JoinOp),
    AntiJoin(JoinOp),
    Union,
    Negate,
    Distinct,
    Aggregate(AggregateOp),
}

/// A reusable indexed view of one record-producing input.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ArrangeOp {
    pub fields: Vec<String>,
    pub comparison: ValueComparison,
}

/// Identity of a deduplicated graph node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u64);

/// Tiny deterministic hasher for in-memory node ids.
#[derive(Clone, Debug)]
struct StableNodeHasher {
    hash: u64,
}

impl Default for StableNodeHasher {
    fn default() -> Self {
        Self {
            hash: 0xcbf2_9ce4_8422_2325,
        }
    }
}

impl Hasher for StableNodeHasher {
    fn finish(&self) -> u64 {
        self.hash
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.hash ^= u64::from(*byte);
            self.hash = self.hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum NodeDurability {
    Ephemeral,
    Durable { storage: DurableStorage },
}

/// Durable node storage location and key namespace.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DurableStorage {
    pub column_family: String,
    pub key_prefix: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Retainer {
    Subscription(String),
    PreparedShape(String),
    DurableSchemaObject(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::ValueType;

    fn output() -> RecordDescriptor {
        RecordDescriptor::new([("f0", ValueType::U64)])
    }

    fn string_output() -> RecordDescriptor {
        RecordDescriptor::new([("f0", ValueType::String)])
    }

    #[test]
    fn cloning_a_deep_builder_is_stack_bounded() {
        // Lowering policy graphs can compose many unary operators. Keep this
        // far beyond the depth that recursive `Box` cloning can handle on the
        // ordinary 2 MiB test-thread stack, and assert immutable child arcs
        // keep a clone shallow.
        const DEPTH: usize = 4_096;
        let mut graph = GraphBuilder::table("rows");
        for _ in 0..DEPTH {
            graph = graph.project(["id"]);
        }

        let cloned = graph.clone();
        assert_eq!(cloned.postorder().len(), DEPTH + 1);

        // The owned builder's derived recursive destructor is a separate
        // concern from cloning. Avoid making this regression receipt depend
        // on that destructor while it validates the clone boundary directly.
        std::mem::forget(graph);
        std::mem::forget(cloned);
    }

    #[test]
    fn identical_descriptors_reuse_the_same_node_id() {
        let mut graph = IvmGraph::new();
        let descriptor = NodeDescriptor::new(
            OpType::TableSource(TableSourceOp {
                table: "albums".to_owned(),
                scan: None,
                variant_projection: None,
            }),
            [],
            output(),
        );

        let first = graph.dedup_node(descriptor.clone(), NodeDurability::Ephemeral);
        let second = graph.dedup_node(descriptor, NodeDurability::Ephemeral);

        assert_eq!(first, second);
        assert_eq!(graph.nodes.len(), 1);
    }

    #[test]
    fn node_identity_is_descriptor_only_not_retainer_scope() {
        let descriptor = NodeDescriptor::new(
            OpType::TableSource(TableSourceOp {
                table: "albums".to_owned(),
                scan: None,
                variant_projection: None,
            }),
            [],
            output(),
        );
        let id = descriptor.node_id();

        let subscription_retainer = Retainer::Subscription("subscriber-a".to_owned());
        let prepared_retainer = Retainer::PreparedShape("shape-b".to_owned());

        assert_ne!(subscription_retainer, prepared_retainer);
        assert_eq!(
            id,
            descriptor.node_id(),
            "retainer tags must not participate in graph node identity"
        );
    }

    #[test]
    #[should_panic(expected = "IVM node id collision for incompatible descriptors")]
    fn dedup_node_rejects_hash_collisions_with_different_descriptors() {
        let mut graph = IvmGraph::new();
        let descriptor = NodeDescriptor::new(
            OpType::TableSource(TableSourceOp {
                table: "albums".to_owned(),
                scan: None,
                variant_projection: None,
            }),
            [],
            output(),
        );
        let colliding_descriptor = NodeDescriptor::new(
            OpType::TableSource(TableSourceOp {
                table: "artists".to_owned(),
                scan: None,
                variant_projection: None,
            }),
            [],
            output(),
        );
        graph.nodes.insert(
            descriptor.node_id(),
            GraphNode::new(colliding_descriptor, NodeDurability::Ephemeral),
        );

        graph.dedup_node(descriptor, NodeDurability::Ephemeral);
    }

    #[test]
    fn graph_tracks_children_for_inputs() {
        let mut graph = IvmGraph::new();
        let input = graph.dedup_node(
            NodeDescriptor::new(
                OpType::TableSource(TableSourceOp {
                    table: "albums".to_owned(),
                    scan: None,
                    variant_projection: None,
                }),
                [],
                output(),
            ),
            NodeDurability::Ephemeral,
        );
        let filter = graph.dedup_node(
            NodeDescriptor::new(
                OpType::Filter(FilterOp {
                    predicate: PredicateExpr::gt("id", crate::records::Value::U64(10)),
                    comparison: ValueComparison::Exact,
                }),
                [input],
                output(),
            ),
            NodeDurability::Ephemeral,
        );

        assert!(graph.node(input).unwrap().children.contains(&filter));
    }

    #[test]
    fn remove_node_detaches_edges() {
        let mut graph = IvmGraph::new();
        let input = graph.dedup_node(
            NodeDescriptor::new(
                OpType::TableSource(TableSourceOp {
                    table: "albums".to_owned(),
                    scan: None,
                    variant_projection: None,
                }),
                [],
                output(),
            ),
            NodeDurability::Durable {
                storage: DurableStorage {
                    column_family: "albums".to_owned(),
                    key_prefix: Vec::new(),
                },
            },
        );
        let filter = graph.dedup_node(
            NodeDescriptor::new(
                OpType::Filter(FilterOp {
                    predicate: PredicateExpr::gt("id", crate::records::Value::U64(10)),
                    comparison: ValueComparison::Exact,
                }),
                [input],
                output(),
            ),
            NodeDurability::Ephemeral,
        );

        graph.remove_node(filter);

        assert!(graph.node(input).unwrap().children.is_empty());
        assert!(graph.node(filter).is_none());
    }

    #[test]
    fn validation_rejects_wrong_filter_arity() {
        let descriptor = NodeDescriptor::new(
            OpType::Filter(FilterOp {
                predicate: PredicateExpr::gt("id", crate::records::Value::U64(10)),
                comparison: ValueComparison::Exact,
            }),
            [],
            output(),
        );

        assert_eq!(
            descriptor.validate(&[]),
            Err(GraphValidationError::InputArityMismatch {
                expected: 1,
                actual: 0,
            })
        );
    }

    #[test]
    fn validation_rejects_project_mapping_past_input_fields() {
        let input = output();
        let descriptor = NodeDescriptor::new(
            OpType::MapProject(MapProjectOp {
                expressions: Vec::new(),
                mapping: vec![(0, 1)],
            }),
            [NodeId(1)],
            output(),
        );

        assert_eq!(
            descriptor.validate(&[NodeOutput::Records(input)]),
            Err(GraphValidationError::FieldIndexOutOfBounds { index: 1, len: 1 })
        );
    }

    #[test]
    fn validation_rejects_top_by_unorderable_tie_fields() {
        let input = RecordDescriptor::new([
            ("rank", ValueType::U64),
            ("tie", ValueType::Array(Box::new(ValueType::U64))),
        ]);
        let descriptor = NodeDescriptor::new(
            OpType::TopBy(TopByOp {
                group_fields: Vec::new(),
                group_field_indices: Vec::new(),
                order_fields: vec![TopByOrderField {
                    field: "rank".to_owned(),
                    direction: TopByDirection::Asc,
                }],
                tie_fields: vec!["tie".to_owned()],
                // `sort_field_indices` is deliberately the concatenation of
                // order and tie fields. Validate every member, not only the
                // user-visible order prefix, because both are compared by the
                // runtime's TopBy key.
                sort_field_indices: vec![0, 1],
                sort_directions: vec![TopByDirection::Asc, TopByDirection::Asc],
                offset: 0,
                limit: TopByLimit::Finite(1),
            }),
            [NodeId(1)],
            input,
        );

        assert_eq!(
            descriptor.validate(&[NodeOutput::Arrangement(ArrangementDescriptor {
                records: input,
            })]),
            Err(GraphValidationError::TopBySortFieldMustBeOrderable),
        );
    }

    #[test]
    fn validation_rejects_union_inputs_with_different_outputs() {
        let descriptor = NodeDescriptor::new(OpType::Union, [NodeId(1), NodeId(2)], output());

        assert_eq!(
            descriptor.validate(&[
                NodeOutput::Records(output()),
                NodeOutput::Records(string_output()),
            ]),
            Err(GraphValidationError::OutputDescriptorMismatch)
        );
    }

    #[test]
    fn validation_keeps_record_and_arrangement_edges_distinct() {
        let filter = NodeDescriptor::new(
            OpType::Filter(FilterOp {
                predicate: PredicateExpr::is_not_null("f0"),
                comparison: ValueComparison::Exact,
            }),
            [NodeId(1)],
            output(),
        );
        assert_eq!(
            filter.validate(&[NodeOutput::Arrangement(ArrangementDescriptor {
                records: output(),
            })]),
            Err(GraphValidationError::InvalidNodeOutput),
        );

        let arrangement = NodeDescriptor::new(
            OpType::Arrange(ArrangeOp {
                fields: vec!["f0".to_owned()],
                comparison: ValueComparison::Exact,
            }),
            [NodeId(1)],
            NodeOutput::Arrangement(ArrangementDescriptor { records: output() }),
        );
        assert_eq!(
            arrangement.validate(&[NodeOutput::Arrangement(ArrangementDescriptor {
                records: output(),
            })]),
            Err(GraphValidationError::InvalidNodeOutput),
        );
    }

    #[test]
    fn validation_rejects_join_key_arity_mismatches() {
        let descriptor = NodeDescriptor::new(
            OpType::Join(JoinOp {
                kind: JoinOpKind::Inner,
                left_key: vec![PlanExpr::field("f0".to_owned())],
                right_key: vec![
                    PlanExpr::field("f0".to_owned()),
                    PlanExpr::field("f1".to_owned()),
                ],
                left_descriptor: output(),
                right_descriptor: output(),
                residual_predicate: None,
                comparison: ValueComparison::Exact,
            }),
            [NodeId(1), NodeId(2)],
            RecordDescriptor::new([("left.f0", ValueType::U64), ("right.f0", ValueType::U64)]),
        );

        assert_eq!(
            descriptor.validate(&[
                NodeOutput::Arrangement(ArrangementDescriptor { records: output() }),
                NodeOutput::Arrangement(ArrangementDescriptor { records: output() }),
            ]),
            Err(GraphValidationError::JoinKeyArityMismatch { left: 1, right: 2 })
        );
    }

    #[test]
    fn validation_rejects_persist_key_fields_outside_output() {
        let descriptor = NodeDescriptor::new(
            OpType::Persist(PersistOp {
                name: "albums_by_title".to_owned(),
                storage: DurableStorage {
                    column_family: "indices".to_owned(),
                    key_prefix: Vec::new(),
                },
                key_fields: vec![1],
                unique: false,
            }),
            [NodeId(1)],
            output(),
        );

        assert_eq!(
            descriptor.validate(&[NodeOutput::Records(output())]),
            Err(GraphValidationError::FieldIndexOutOfBounds { index: 1, len: 1 })
        );
    }

    #[test]
    fn validation_rejects_ordinary_consumers_over_collect_by() {
        let mut graph = IvmGraph::new();
        let source = graph.dedup_node(
            NodeDescriptor::new(
                OpType::TableSource(TableSourceOp {
                    table: "rows".to_owned(),
                    scan: None,
                    variant_projection: None,
                }),
                [],
                output(),
            ),
            NodeDurability::Ephemeral,
        );
        let child = output();
        let arrangement = graph.dedup_node(
            NodeDescriptor::new(
                OpType::Arrange(ArrangeOp {
                    fields: vec!["f0".to_owned()],
                    comparison: ValueComparison::Exact,
                }),
                [source],
                NodeOutput::Arrangement(ArrangementDescriptor { records: output() }),
            ),
            NodeDurability::Ephemeral,
        );
        let collected_output = RecordDescriptor::new([
            ("f0", ValueType::U64),
            (
                "children",
                ValueType::Array(Box::new(ValueType::Record(Box::new(child)))),
            ),
        ]);
        let collector = graph.dedup_node(
            NodeDescriptor::new(
                OpType::CollectBy(Box::new(CollectByOp {
                    mode: CollectByMode::Collect,
                    group_fields: vec!["f0".to_owned()],
                    group_field_indices: vec![0],
                    parent_fields: vec![CollectByProjection {
                        field: "f0".to_owned(),
                        field_idx: 0,
                        output_name: "f0".to_owned(),
                        unwrap_nullable: false,
                    }],
                    child_fields: vec![CollectByProjection {
                        field: "f0".to_owned(),
                        field_idx: 0,
                        output_name: "f0".to_owned(),
                        unwrap_nullable: false,
                    }],
                    child_descriptor: child,
                    collection_field: "children".to_owned(),
                    collection_field_index: 1,
                    slots: Vec::new(),
                    tuple_fields: Vec::new(),
                    occurrence_id_fields: Vec::new(),
                    occurrence_id_field_indices: Vec::new(),
                    order_fields: vec![TopByOrderField {
                        field: "f0".to_owned(),
                        direction: TopByDirection::Asc,
                    }],
                    tie_fields: vec!["f0".to_owned()],
                    sort_field_indices: vec![0, 0],
                    sort_directions: vec![TopByDirection::Asc, TopByDirection::Asc],
                    offset: 0,
                    limit: TopByLimit::Finite(1),
                })),
                [arrangement],
                collected_output,
            ),
            NodeDurability::Ephemeral,
        );
        let consumers = [
            NodeDescriptor::new(
                OpType::Filter(FilterOp {
                    predicate: PredicateExpr::is_not_null("f0"),
                    comparison: ValueComparison::Exact,
                }),
                [collector],
                collected_output,
            ),
            NodeDescriptor::new(
                OpType::MapProject(MapProjectOp {
                    expressions: Vec::new(),
                    mapping: vec![(0, 0)],
                }),
                [collector],
                output(),
            ),
        ];

        for consumer in consumers {
            assert_eq!(
                graph.validate_node(&consumer),
                Err(GraphValidationError::CollectByInputIsTerminal)
            );
        }
    }
}
