//! Hash-consed IVM graph IR and builder API.
//!
//! This module owns graph identity: [`GraphBuilder`] is the user/planner-facing
//! IR, [`NodeDescriptor`] is the validated runtime descriptor, and [`IvmGraph`]
//! deduplicates compatible nodes by descriptor hash while retaining reverse
//! edges for graph maintenance and GC. Operator payload structs live in
//! [`super::op_types`]; lowering from queries lives in [`super::planner`]; the
//! tick loop that evaluates the graph lives in [`super::runtime`].

use std::hash::{BuildHasher, Hash, Hasher};

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use crate::records::{RecordDescriptor, Value, ValueType};
use thiserror::Error;

use super::op_types::*;

/// User-facing graph construction API before deduplication.
///
/// Builders refer to table and field names directly; the runtime resolves those
/// names against the database schema when a graph is subscribed, queried, or
/// prepared.
///
/// ```rust
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
/// let mut database = Database::new(schema, MemoryStorage::new(&["albums", "artists"]))?;
///
/// let mut batch = database.open_batch();
/// batch.insert("artists", vec![Value::U64(1), Value::String("Wayne Shorter".into())]);
/// batch.insert("artists", vec![Value::U64(2), Value::String("McCoy Tyner".into())]);
/// batch.insert("albums", vec![Value::U64(10), Value::U64(1), Value::String("Speak No Evil".into())]);
/// batch.insert("albums", vec![Value::U64(11), Value::U64(2), Value::String("Expansions".into())]);
/// database.commit_batch(batch)?;
///
/// let albums = GraphBuilder::table("albums")
///     .filter(PredicateExpr::eq("title", Value::String("Speak No Evil".into())));
/// let artists = GraphBuilder::table("artists");
/// let graph = GraphBuilder::join(albums, artists, ["artist_id"], ["id"]).project_fields([
///     ProjectField::renamed("left.title", "album"),
///     ProjectField::renamed("right.name", "artist"),
/// ]);
///
/// let rows = database.query_graph(graph)?;
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
/// ```
///
/// Prepared graph shapes use a named [`GraphBuilder::binding_source`] node. The
/// source name passed to [`crate::db::Database::prepare`] must match the binding
/// source in the graph.
///
/// ```rust
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
/// let mut database = Database::new(schema, MemoryStorage::new(&["albums"]))?;
///
/// let binding_descriptor = RecordDescriptor::new([("artist_id", ColumnType::U64.value_type())]);
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
/// let shape = database.prepare_one_sink(graph, "artist_params", binding_descriptor, ["artist_id"])?;
/// let subscription = database.bind_shape_one_sink(shape.id(), &[Value::U64(1)])?;
/// assert!(subscription.recv()?.is_empty());
///
/// let mut batch = database.open_batch();
/// batch.insert("albums", vec![Value::U64(10), Value::U64(1), Value::String("Speak No Evil".into())]);
/// batch.insert("albums", vec![Value::U64(11), Value::U64(2), Value::String("Expansions".into())]);
/// database.commit_batch(batch)?;
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
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum GraphBuilder {
    /// Read a base table's rows and, once maintained, its future deltas.
    Table {
        table: String,
        /// Optional key restriction; see [`StaticScanSpec`].
        scan: Option<StaticScanSpec>,
    },
    /// A fixed in-memory set of encoded rows (snapshot-only test/data input).
    InlineRecords {
        output: RecordDescriptor,
        records: Vec<Vec<u8>>,
    },
    /// Read a schema-declared secondary index of a table.
    Index {
        table: String,
        index: String,
        /// Optional restriction over the *index* key.
        scan: Option<StaticScanSpec>,
    },
    /// Read the frontier of the enclosing [`GraphBuilder::Recursive`] node —
    /// the rows produced by the previous iteration of the fixpoint.
    FrontierSource {
        binding: FrontierName,
        output: RecordDescriptor,
    },
    /// Read the bound parameter rows of a prepared shape (see the second
    /// example above).
    BindingSource {
        shape: String,
        output: RecordDescriptor,
    },
    /// Fixpoint: start from `seed`, then repeatedly run `step` (which reads
    /// the previous iteration through a matching `FrontierSource`) until
    /// nothing changes or `max_iters` is hit.
    Recursive {
        seed: Box<GraphBuilder>,
        step: Box<GraphBuilder>,
        frontier: FrontierName,
        max_iters: usize,
    },
    /// Keep only rows matching `predicate`.
    Filter {
        input: Box<GraphBuilder>,
        predicate: PredicateExpr,
    },
    /// Unwrap one `Nullable(T)` field to `T`, dropping rows where it is NULL.
    UnwrapNullable {
        input: Box<GraphBuilder>,
        field: FieldRef,
    },
    /// Expand an array field into one output row per element.
    Unnest {
        input: Box<GraphBuilder>,
        array_field: FieldRef,
        /// Name of the output field that carries the current element.
        element_field: String,
    },
    /// Pick, rename, wrap, or synthesize output fields.
    Project {
        input: Box<GraphBuilder>,
        fields: Vec<ProjectField>,
    },
    /// Add up the inputs' weighted record sets (SQL `UNION ALL`). All inputs
    /// must share one row layout.
    Union { inputs: Vec<GraphBuilder> },
    /// Inner equi-join: `left_on[i] = right_on[i]` for every `i`. Output
    /// fields are addressed as `left.<name>` / `right.<name>`.
    Join {
        left: Box<GraphBuilder>,
        right: Box<GraphBuilder>,
        left_on: Vec<FieldRef>,
        right_on: Vec<FieldRef>,
    },
    /// Semi join: left rows that have at least one match on the right; the
    /// output layout is the left layout (right columns are not produced).
    SemiJoin {
        left: Box<GraphBuilder>,
        right: Box<GraphBuilder>,
        left_on: Vec<FieldRef>,
        right_on: Vec<FieldRef>,
    },
    /// Anti join: left rows with *no* match on the right; output layout is
    /// the left layout.
    AntiJoin {
        left: Box<GraphBuilder>,
        right: Box<GraphBuilder>,
        left_on: Vec<FieldRef>,
        right_on: Vec<FieldRef>,
    },
    /// Per group, keep the row that is largest by `order_cols`.
    ArgMaxBy {
        input: Box<GraphBuilder>,
        group_cols: Vec<FieldRef>,
        order_cols: Vec<FieldRef>,
    },
    /// Per group, keep the row that is smallest by `order_cols`.
    ArgMinBy {
        input: Box<GraphBuilder>,
        group_cols: Vec<FieldRef>,
        order_cols: Vec<FieldRef>,
    },
    /// Per group, keep an ordered window of rows (SQL `LIMIT`/`OFFSET` per
    /// partition).
    TopBy {
        input: Box<GraphBuilder>,
        group_cols: Vec<FieldRef>,
        order_cols: Vec<TopByOrder>,
        /// Extra fields appended to the sort key so equal-ordered rows have
        /// a stable, deterministic order.
        tie_cols: Vec<FieldRef>,
        offset: u64,
        limit: TopByLimit,
    },
    /// Grouped aggregates (`count` / `sum` / ...), one output row per group.
    Aggregate {
        input: Box<GraphBuilder>,
        group_cols: Vec<FieldRef>,
        aggregates: Vec<AggregateExpr>,
    },
}

/// Field reference carried by graph builders.
///
/// Public constructors keep accepting names. SQL planning may resolve names
/// once and emit `Resolved` references directly.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum FieldRef {
    /// A field named as declared, resolved against the input layout later.
    Name(String),
    /// An already-resolved logical field index.
    Resolved(usize),
}

impl FieldRef {
    /// A by-name reference.
    pub fn name(name: impl Into<String>) -> Self {
        Self::Name(name.into())
    }

    /// A by-index reference.
    pub fn resolved(index: usize) -> Self {
        Self::Resolved(index)
    }

    /// Human-readable form for error messages: the name itself, or `#idx`
    /// for resolved references.
    pub fn display_name(&self) -> String {
        match self {
            Self::Name(name) => name.clone(),
            Self::Resolved(index) => format!("#{index}"),
        }
    }
}

impl GraphBuilder {
    /// Starts a graph from a base table (all rows, no scan restriction).
    pub fn table(table: impl Into<String>) -> Self {
        Self::Table {
            table: table.into(),
            scan: None,
        }
    }

    /// Starts a graph from the part of a base table selected by `scan`.
    pub fn table_scan(table: impl Into<String>, scan: StaticScanSpec) -> Self {
        Self::Table {
            table: table.into(),
            scan: Some(scan),
        }
    }

    /// Starts a graph from already-encoded rows.
    ///
    /// * `output` — the rows' layout.
    /// * `records` — the encoded rows; prefer [`Self::values`] when starting
    ///   from logical values.
    pub fn inline_records(
        output: RecordDescriptor,
        records: impl IntoIterator<Item = Vec<u8>>,
    ) -> Self {
        Self::InlineRecords {
            output,
            records: records.into_iter().collect(),
        }
    }

    /// Starts a graph from logical rows, encoding each against `output`.
    ///
    /// * `output` — the rows' layout.
    /// * `rows` — one `[Value]` slice per row, in `output` field order.
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

    /// Starts a graph from a schema-declared secondary index.
    pub fn index(table: impl Into<String>, index: impl Into<String>) -> Self {
        Self::Index {
            table: table.into(),
            index: index.into(),
            scan: None,
        }
    }

    /// Starts a graph from the part of an index selected by `scan` (the scan
    /// values are index-key parts).
    pub fn index_scan(
        table: impl Into<String>,
        index: impl Into<String>,
        scan: StaticScanSpec,
    ) -> Self {
        Self::Index {
            table: table.into(),
            index: index.into(),
            scan: Some(scan),
        }
    }

    /// The frontier input of a recursive step graph; `binding` must match
    /// the enclosing [`Self::recursive`] call's `frontier` name.
    pub fn frontier_source(binding: impl Into<String>, output: RecordDescriptor) -> Self {
        Self::FrontierSource {
            binding: FrontierName(binding.into()),
            output,
        }
    }

    /// The parameter input of a prepared shape; `shape` must match the
    /// source name later passed to `Database::prepare`.
    pub fn binding_source(shape: impl Into<String>, output: RecordDescriptor) -> Self {
        Self::BindingSource {
            shape: shape.into(),
            output,
        }
    }

    /// Builds a fixpoint node.
    ///
    /// * `seed` — the initial rows (iteration 0).
    /// * `step` — the graph run each iteration; it reads the previous
    ///   iteration's rows through a [`Self::frontier_source`] named
    ///   `frontier`.
    /// * `frontier` — the name tying `step`'s frontier source to this node.
    /// * `max_iters` — hard stop for fixpoints that never settle.
    pub fn recursive(
        seed: GraphBuilder,
        step: GraphBuilder,
        frontier: impl Into<String>,
        max_iters: usize,
    ) -> Self {
        Self::Recursive {
            seed: Box::new(seed),
            step: Box::new(step),
            frontier: FrontierName(frontier.into()),
            max_iters,
        }
    }

    /// Adds up the inputs' rows (`UNION ALL`); all inputs must share one
    /// layout.
    pub fn union(inputs: impl IntoIterator<Item = GraphBuilder>) -> Self {
        Self::Union {
            inputs: inputs.into_iter().collect(),
        }
    }

    /// Inner equi-join of two graphs.
    ///
    /// * `left` / `right` — the two inputs.
    /// * `left_on` / `right_on` — field names matched position by position:
    ///   `join(albums, artists, ["artist_id"], ["id"])` keeps pairs where
    ///   `albums.artist_id = artists.id`.
    ///
    /// Downstream projections address the output as `left.<field>` /
    /// `right.<field>`.
    pub fn join(
        left: GraphBuilder,
        right: GraphBuilder,
        left_on: impl IntoIterator<Item = impl Into<String>>,
        right_on: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::Join {
            left: Box::new(left),
            right: Box::new(right),
            left_on: left_on.into_iter().map(FieldRef::name).collect(),
            right_on: right_on.into_iter().map(FieldRef::name).collect(),
        }
    }

    /// Semi join: keeps left rows that have at least one right match; the
    /// output layout stays the left layout. Arguments as in [`Self::join`].
    pub fn semi_join(
        left: GraphBuilder,
        right: GraphBuilder,
        left_on: impl IntoIterator<Item = impl Into<String>>,
        right_on: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::SemiJoin {
            left: Box::new(left),
            right: Box::new(right),
            left_on: left_on.into_iter().map(FieldRef::name).collect(),
            right_on: right_on.into_iter().map(FieldRef::name).collect(),
        }
    }

    /// Anti join: keeps left rows with *no* right match; the output layout
    /// stays the left layout. Arguments as in [`Self::join`].
    pub fn anti_join(
        left: GraphBuilder,
        right: GraphBuilder,
        left_on: impl IntoIterator<Item = impl Into<String>>,
        right_on: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::AntiJoin {
            left: Box::new(left),
            right: Box::new(right),
            left_on: left_on.into_iter().map(FieldRef::name).collect(),
            right_on: right_on.into_iter().map(FieldRef::name).collect(),
        }
    }

    /// Per group of `group_cols`, keeps the row that is largest by
    /// `order_cols`.
    pub fn arg_max_by(
        input: GraphBuilder,
        group_cols: impl IntoIterator<Item = impl Into<String>>,
        order_cols: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::ArgMaxBy {
            input: Box::new(input),
            group_cols: group_cols.into_iter().map(FieldRef::name).collect(),
            order_cols: order_cols.into_iter().map(FieldRef::name).collect(),
        }
    }

    /// Per group of `group_cols`, keeps the row that is smallest by
    /// `order_cols`.
    pub fn arg_min_by(
        input: GraphBuilder,
        group_cols: impl IntoIterator<Item = impl Into<String>>,
        order_cols: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::ArgMinBy {
            input: Box::new(input),
            group_cols: group_cols.into_iter().map(FieldRef::name).collect(),
            order_cols: order_cols.into_iter().map(FieldRef::name).collect(),
        }
    }

    /// Per group of `group_cols`, keeps an ordered window of rows.
    ///
    /// * `order_cols` — the sort order, each entry with its own direction.
    /// * `tie_cols` — appended to the sort key to break ties
    ///   deterministically.
    /// * `offset` / `limit` — the window: skip `offset` rows, keep `limit`.
    pub fn top_by(
        input: GraphBuilder,
        group_cols: impl IntoIterator<Item = impl Into<String>>,
        order_cols: impl IntoIterator<Item = TopByOrder>,
        tie_cols: impl IntoIterator<Item = impl Into<String>>,
        offset: u64,
        limit: TopByLimit,
    ) -> Self {
        Self::TopBy {
            input: Box::new(input),
            group_cols: group_cols.into_iter().map(FieldRef::name).collect(),
            order_cols: order_cols.into_iter().collect(),
            tie_cols: tie_cols.into_iter().map(FieldRef::name).collect(),
            offset,
            limit,
        }
    }

    /// Grouped aggregates: one output row per `group_cols` group, holding
    /// the group key plus each aggregate in `aggregates`.
    pub fn aggregate(
        input: GraphBuilder,
        group_cols: impl IntoIterator<Item = impl Into<String>>,
        aggregates: impl IntoIterator<Item = AggregateExpr>,
    ) -> Self {
        Self::Aggregate {
            input: Box::new(input),
            group_cols: group_cols.into_iter().map(FieldRef::name).collect(),
            aggregates: aggregates.into_iter().collect(),
        }
    }

    /// Keeps only rows matching `predicate` (chained form).
    pub fn filter(self, predicate: PredicateExpr) -> Self {
        Self::Filter {
            input: Box::new(self),
            predicate,
        }
    }

    /// Unwraps a `Nullable(T)` field to `T`, dropping rows where it is NULL
    /// (chained form).
    pub fn unwrap_nullable(self, field: impl Into<String>) -> Self {
        Self::UnwrapNullable {
            input: Box::new(self),
            field: FieldRef::name(field),
        }
    }

    /// Expands `array_field` into one row per element; the element lands in
    /// a new `element_field` column (chained form).
    pub fn unnest(self, array_field: impl Into<String>, element_field: impl Into<String>) -> Self {
        Self::Unnest {
            input: Box::new(self),
            array_field: FieldRef::name(array_field),
            element_field: element_field.into(),
        }
    }

    /// Keeps the named fields, unrenamed (chained form). Use
    /// [`Self::project_fields`] for renames, literals, and nullable wraps.
    pub fn project(self, fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::Project {
            input: Box::new(self),
            fields: fields.into_iter().map(ProjectField::named).collect(),
        }
    }

    /// Full projection control: one [`ProjectField`] per output field
    /// (chained form).
    pub fn project_fields(self, fields: impl IntoIterator<Item = ProjectField>) -> Self {
        Self::Project {
            input: Box::new(self),
            fields: fields.into_iter().collect(),
        }
    }
}

/// Field selected by a Project builder, optionally renamed.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProjectField {
    /// What the output field holds; see [`ProjectExpr`].
    pub expression: ProjectExpr,
    /// The output field's name.
    pub output_name: String,
}

impl ProjectField {
    /// Copies a field, keeping its name.
    pub fn named(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            expression: ProjectExpr::Field(FieldRef::name(name.clone())),
            output_name: name,
        }
    }

    /// Copies `source_name` under a new name — for example
    /// `ProjectField::renamed("left.title", "album")` after a join.
    pub fn renamed(source_name: impl Into<String>, output_name: impl Into<String>) -> Self {
        Self {
            expression: ProjectExpr::Field(FieldRef::name(source_name)),
            output_name: output_name.into(),
        }
    }

    /// Like [`Self::renamed`], with the source given as a resolved index.
    pub fn renamed_resolved(source_idx: usize, output_name: impl Into<String>) -> Self {
        Self {
            expression: ProjectExpr::Field(FieldRef::resolved(source_idx)),
            output_name: output_name.into(),
        }
    }

    /// A constant output field.
    pub fn literal(output_name: impl Into<String>, value: impl Into<LiteralValue>) -> Self {
        Self {
            expression: ProjectExpr::Literal(value.into()),
            output_name: output_name.into(),
        }
    }

    /// Create a null projection with the legacy default type `Nullable(Bytes)`.
    /// Use [`Self::null_typed`] when the output schema matters.
    pub fn null(output_name: impl Into<String>) -> Self {
        Self::null_typed(output_name, ValueType::Nullable(Box::new(ValueType::Bytes)))
    }

    /// A NULL constant output field of the given type.
    pub fn null_typed(output_name: impl Into<String>, value_type: ValueType) -> Self {
        Self {
            expression: ProjectExpr::Null(value_type),
            output_name: output_name.into(),
        }
    }

    /// Copies `source_name` wrapped as a *present* nullable, for feeding a
    /// non-nullable field into a nullable output column.
    pub fn nullable(source_name: impl Into<String>, output_name: impl Into<String>) -> Self {
        Self {
            expression: ProjectExpr::Nullable(FieldRef::name(source_name)),
            output_name: output_name.into(),
        }
    }

    /// Like [`Self::nullable`], with the source given as a resolved index.
    pub fn nullable_resolved(source_idx: usize, output_name: impl Into<String>) -> Self {
        Self {
            expression: ProjectExpr::Nullable(FieldRef::resolved(source_idx)),
            output_name: output_name.into(),
        }
    }

    /// Like [`Self::nullable`], but copies the field unchanged when it is
    /// already nullable instead of double-wrapping it.
    pub fn nullable_flat(source_name: impl Into<String>, output_name: impl Into<String>) -> Self {
        Self {
            expression: ProjectExpr::NullableFlat(FieldRef::name(source_name)),
            output_name: output_name.into(),
        }
    }

    /// The input field this projection reads, when it reads one (`None` for
    /// literals and NULLs).
    pub fn source(&self) -> Option<&FieldRef> {
        match &self.expression {
            ProjectExpr::Field(source)
            | ProjectExpr::Nullable(source)
            | ProjectExpr::NullableFlat(source) => Some(source),
            ProjectExpr::Literal(_) | ProjectExpr::Null(_) => None,
        }
    }
}

/// Builder-side form of one projected output field; the planner lowers this
/// into [`PlanExpr`] with names resolved.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ProjectExpr {
    /// Copy an input field.
    Field(FieldRef),
    /// A constant.
    Literal(LiteralValue),
    /// A typed NULL constant.
    Null(ValueType),
    /// An input field wrapped as a present nullable.
    Nullable(FieldRef),
    /// Like `Nullable`, but copied unchanged when already nullable.
    NullableFlat(FieldRef),
}

/// One `ORDER BY` entry of a [`GraphBuilder::TopBy`] builder.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopByOrder {
    /// The field to order by.
    pub field: FieldRef,
    /// Ascending or descending.
    pub direction: TopByDirection,
}

impl TopByOrder {
    /// Ascending order on `field`.
    pub fn asc(field: impl Into<String>) -> Self {
        Self {
            field: FieldRef::name(field),
            direction: TopByDirection::Asc,
        }
    }

    /// Descending order on `field`.
    pub fn desc(field: impl Into<String>) -> Self {
        Self {
            field: FieldRef::name(field),
            direction: TopByDirection::Desc,
        }
    }
}

/// Deduplicated DAG of IVM node descriptors.
#[derive(Clone, Debug, Default)]
pub struct IvmGraph {
    /// Deduplicated node specs. The `NodeId` is derived from the full
    /// descriptor, and insertion asserts that collisions do not merge specs.
    nodes: HashMap<NodeId, GraphNode>,
}

impl IvmGraph {
    /// An empty graph.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a node, or returns the existing one when an identical
    /// descriptor is already present — this is where graph sharing happens:
    /// two queries lowering to the same operator get one node, one state.
    ///
    /// * `descriptor` — the node spec; validated against its inputs' output
    ///   layouts (inputs must already be in the graph).
    /// * `durability` — whether the node maintains storage-backed state and
    ///   must outlive its subscribers.
    ///
    /// Panics on an invalid descriptor and on the (loudly checked) case of
    /// two different descriptors hashing to one [`NodeId`].
    pub fn dedup_node(&mut self, descriptor: NodeDescriptor, durability: NodeDurability) -> NodeId {
        let input_outputs = descriptor
            .inputs
            .iter()
            .filter_map(|input| self.nodes.get(input).map(|node| node.descriptor.output))
            .collect::<Vec<_>>();
        descriptor
            .validate(&input_outputs)
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

        self.nodes
            .insert(id, GraphNode::new(descriptor, durability));
        id
    }

    /// Looks a node up by id.
    pub fn node(&self, id: NodeId) -> Option<&GraphNode> {
        self.nodes.get(&id)
    }

    /// Mutable node lookup.
    pub fn node_mut(&mut self, id: NodeId) -> Option<&mut GraphNode> {
        self.nodes.get_mut(&id)
    }

    /// All nodes, keyed by id.
    pub fn nodes(&self) -> &HashMap<NodeId, GraphNode> {
        &self.nodes
    }

    /// Adds `id` and everything it (transitively) reads to `retained`.
    ///
    /// Garbage collection calls this for every live sink; nodes not marked
    /// by any sink can then be removed.
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

    /// Removes a node and detaches it from its inputs' reverse edges.
    /// Removing an id that is not present is a no-op.
    pub fn remove_node(&mut self, id: NodeId) {
        let Some(node) = self.nodes.remove(&id) else {
            return;
        };

        for input in node.descriptor.inputs {
            if let Some(input_node) = self.nodes.get_mut(&input) {
                input_node.children.remove(&id);
            }
        }
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

    /// `true` when the node maintains storage-backed state (see
    /// [`NodeDurability`]).
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
    pub output: RecordDescriptor,
}

impl NodeDescriptor {
    /// Assembles a descriptor from its three identity parts.
    ///
    /// * `operator` — the operator payload.
    /// * `inputs` — ids of the nodes this one reads, in operator order (for
    ///   a join: left then right).
    /// * `output` — the layout of the rows this node produces.
    pub fn new(
        operator: OpType,
        inputs: impl IntoIterator<Item = NodeId>,
        output: RecordDescriptor,
    ) -> Self {
        Self {
            operator,
            inputs: inputs.into_iter().collect(),
            output,
        }
    }

    /// The node's identity: a deterministic hash of the whole descriptor.
    /// Stable across runs, so persisted references stay valid.
    pub fn node_id(&self) -> NodeId {
        // Keep node ids deterministic across runs. They are still guarded by a
        // descriptor equality check on deduplication, so collisions fail loudly.
        let mut hasher = StableNodeHasher::default();
        self.hash(&mut hasher);
        NodeId(hasher.finish())
    }

    /// Checks the descriptor's internal consistency against its inputs.
    ///
    /// * `input_outputs` — the output layout of each input node, in the same
    ///   order as `self.inputs`.
    ///
    /// Per operator this enforces the input count, that pass-through
    /// operators (filter, distinct, persist, ...) declare the same output
    /// layout as their input, that resolved field indices are in bounds, and
    /// that join descriptors and key arities line up.
    pub fn validate(&self, input_outputs: &[RecordDescriptor]) -> Result<(), GraphValidationError> {
        if self.inputs.len() != input_outputs.len() {
            return Err(GraphValidationError::InputDescriptorArityMismatch {
                inputs: self.inputs.len(),
                descriptors: input_outputs.len(),
            });
        }

        match &self.operator {
            OpType::TableSource(_)
            | OpType::IndexSource(_)
            | OpType::InlineRecords(_)
            | OpType::FrontierSource(_)
            | OpType::BindingSource(_) => expect_arity(&self.inputs, 0),
            OpType::Filter(_) | OpType::Distinct | OpType::Negate => {
                expect_arity(&self.inputs, 1)?;
                expect_same_output(&self.output, &input_outputs[0])
            }
            OpType::ArgMaxBy(arg_max_by) => {
                expect_arity(&self.inputs, 1)?;
                expect_same_output(&self.output, &input_outputs[0])?;
                for &field_idx in arg_max_by
                    .group_field_indices
                    .iter()
                    .chain(&arg_max_by.primary_key_field_indices)
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
                expect_same_output(&self.output, &input_outputs[0])?;
                for &field_idx in arg_min_by
                    .group_field_indices
                    .iter()
                    .chain(&arg_min_by.primary_key_field_indices)
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
                expect_same_output(&self.output, &input_outputs[0])?;
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
                    && project.expressions.len() != self.output.fields().len()
                {
                    return Err(GraphValidationError::OutputFieldCountMismatch {
                        expected: project.expressions.len(),
                        actual: self.output.fields().len(),
                    });
                }
                if project.expressions.is_empty()
                    && project.mapping.len() != self.output.fields().len()
                {
                    return Err(GraphValidationError::OutputFieldCountMismatch {
                        expected: project.mapping.len(),
                        actual: self.output.fields().len(),
                    });
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
                expect_same_output(&self.output, &input_outputs[0])?;
                for &field_idx in &persist.key_fields {
                    if field_idx >= self.output.fields().len() {
                        return Err(GraphValidationError::FieldIndexOutOfBounds {
                            index: field_idx,
                            len: self.output.fields().len(),
                        });
                    }
                }
                Ok(())
            }
            OpType::Join(join) | OpType::SemiJoin(join) | OpType::AntiJoin(join) => {
                expect_arity(&self.inputs, 2)?;
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
                    expect_same_output(&self.output, input_output)?;
                }
                Ok(())
            }
            OpType::Aggregate(_) => {
                expect_arity(&self.inputs, 1)?;
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

/// Checks that a node has exactly `expected` inputs.
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

/// Checks that a pass-through node's declared output layout matches its
/// input's.
fn expect_same_output(
    expected: &RecordDescriptor,
    actual: &RecordDescriptor,
) -> Result<(), GraphValidationError> {
    if expected == actual {
        Ok(())
    } else {
        Err(GraphValidationError::OutputDescriptorMismatch)
    }
}

/// Ways a [`NodeDescriptor`] can fail [`NodeDescriptor::validate`]; each
/// variant's `#[error]` string states the violated rule.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum GraphValidationError {
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
}

/// The operator of one validated graph node. Most variants carry a payload
/// struct from [`super::op_types`]; the payload-free ones are described
/// here.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum OpType {
    /// Base table deltas; see [`TableSourceOp`].
    TableSource(TableSourceOp),
    /// Durable schema index; see [`IndexSourceOp`].
    IndexSource(IndexSourceOp),
    /// Fixed in-memory rows; see [`InlineRecordsOp`].
    InlineRecords(InlineRecordsOp),
    /// Recursive frontier input; see [`FrontierSourceOp`].
    FrontierSource(FrontierSourceOp),
    /// Prepared-shape parameter input; see [`BindingSourceOp`].
    BindingSource(BindingSourceOp),
    /// Per-group maximum row; see [`ArgMaxByOp`].
    ArgMaxBy(ArgMaxByOp),
    /// Per-group minimum row; see [`ArgMinByOp`].
    ArgMinBy(ArgMinByOp),
    /// Per-group ordered window; see [`TopByOp`].
    TopBy(TopByOp),
    /// Fixpoint; see [`RecursiveOp`].
    Recursive(RecursiveOp),
    /// Durable write-through; see [`PersistOp`].
    Persist(PersistOp),
    /// Predicate filter; see [`FilterOp`].
    Filter(FilterOp),
    /// Projection; see [`MapProjectOp`].
    MapProject(MapProjectOp),
    /// Nullable unwrap; see [`UnwrapNullableOp`].
    UnwrapNullable(UnwrapNullableOp),
    /// Array expansion; see [`UnnestOp`].
    Unnest(UnnestOp),
    /// Keyed arrangement construction; see [`IndexByOp`].
    IndexBy(IndexByOp),
    /// Inner join; see [`JoinOp`].
    Join(JoinOp),
    /// Semi join (left rows with a match), reusing the [`JoinOp`] payload.
    SemiJoin(JoinOp),
    /// Anti join (left rows without a match), reusing the [`JoinOp`] payload.
    AntiJoin(JoinOp),
    /// Adds up all inputs' weighted record sets (`UNION ALL`); no payload.
    Union,
    /// Negates every weight (`+n` becomes `-n`); the subtraction half of
    /// set-difference shapes.
    Negate,
    /// Collapses every positive multiplicity to weight 1.
    Distinct,
    /// Grouped aggregates; see [`AggregateOp`].
    Aggregate(AggregateOp),
}

/// Identity of a deduplicated graph node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u64);

/// Tiny deterministic hasher for in-memory node ids (FNV-1a). The std
/// `DefaultHasher` is randomly seeded per process, which would make node
/// ids differ between runs.
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

/// Whether a node's state lives only in memory or is backed by storage.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum NodeDurability {
    /// In-memory only; the node can be dropped when nothing subscribes
    /// through it anymore.
    Ephemeral,
    /// Backed by storage (for example a persisted index); retained even with
    /// no subscribers, because the stored state must stay maintained.
    Durable { storage: DurableStorage },
}

/// Durable node storage location and key namespace.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DurableStorage {
    /// Which storage column family holds the entries.
    pub column_family: String,
    /// Byte prefix in front of every key, so several durable nodes can
    /// share one column family without colliding.
    pub key_prefix: Vec<u8>,
}

/// Why a graph node is being kept alive. Retainers deliberately do *not*
/// participate in node identity — the same node can serve many retainers.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Retainer {
    /// A live subscription reads through the node.
    Subscription(String),
    /// A prepared shape's graph contains the node.
    PreparedShape(String),
    /// A schema object (for example a declared index) requires the node.
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
    fn identical_descriptors_reuse_the_same_node_id() {
        let mut graph = IvmGraph::new();
        let descriptor = NodeDescriptor::new(
            OpType::TableSource(TableSourceOp {
                table: "albums".to_owned(),
                scan: None,
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
            }),
            [],
            output(),
        );
        let colliding_descriptor = NodeDescriptor::new(
            OpType::TableSource(TableSourceOp {
                table: "artists".to_owned(),
                scan: None,
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
            descriptor.validate(&[input]),
            Err(GraphValidationError::FieldIndexOutOfBounds { index: 1, len: 1 })
        );
    }

    #[test]
    fn validation_rejects_union_inputs_with_different_outputs() {
        let descriptor = NodeDescriptor::new(OpType::Union, [NodeId(1), NodeId(2)], output());

        assert_eq!(
            descriptor.validate(&[output(), string_output()]),
            Err(GraphValidationError::OutputDescriptorMismatch)
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
            }),
            [NodeId(1), NodeId(2)],
            RecordDescriptor::new([("left.f0", ValueType::U64), ("right.f0", ValueType::U64)]),
        );

        assert_eq!(
            descriptor.validate(&[output(), output()]),
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
            descriptor.validate(&[output()]),
            Err(GraphValidationError::FieldIndexOutOfBounds { index: 1, len: 1 })
        );
    }
}
