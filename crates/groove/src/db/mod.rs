//! Schema-aware database facade over records, storage, and the IVM runtime.
//!
//! This module owns the public [`Database`] API: opening a schema on an
//! [`OrderedKvStorage`], encoding user rows through [`RecordDescriptor`],
//! maintaining primary/secondary durable storage entries, and synchronously
//! ticking [`IvmRuntime`] after committed batches. Query planning and graph
//! execution live in [`crate::ivm`]; binary row layout lives in
//! [`crate::records`]; storage durability lives below the [`OrderedKvStorage`]
//! seam. New readers should start here to see how commits become table deltas
//! and how subscriptions are exposed above the engine.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::str;

use web_time::{Duration, Instant};

use crate::ivm::runtime::{durable_index_key_prefix, encode_key_part};
use crate::ivm::{
    IvmRuntime, PlannerError, QueryParameter, RecordDelta, RecordDeltas, RuntimeStats, TableDelta,
    TickMetrics, plan_prepared_shape, plan_query,
};
use crate::queries::Query;
use crate::records::{self, BorrowedRecord, OwnedRecord, Record, RecordDescriptor, Value};
use crate::schema::{
    ColumnType, DatabaseSchema, DirectRecordStoreSchema, IndexSchema, IntegerKeyType, PrimaryKey,
    PrimaryKeyColumn, PrimaryKeyType, TableSchema,
};
use crate::storage::{
    LayoutStorage, OrderedKvStorage, OwnedWriteOperation, RecordStore, StagedWriteOverlay,
    StagedWriteState, StorageLayout, WindowConsolidation, WriteOperation,
    is_windowed_history_table,
};
use thiserror::Error;

pub use crate::ivm::{
    GraphBuilder, IvmRuntimeError, MultisinkDeltas, MultisinkSubscription, PredicateExpr,
    PreparedShapeId, RoutedMultisinkTerminal, Subscription, SubscriptionId,
};

/// Schema-aware database facade over storage and IVM subscriptions.
pub struct Database<S> {
    storage: LayoutStorage<S>,
    /// Owns query/index maintenance over the storage-backed base tables.
    ivm_runtime: IvmRuntime,
    last_commit_metrics: Option<CommitMetrics>,
    last_tick_metrics: Option<TickMetrics>,
    storage_read_metrics: RefCell<StorageReadMetrics>,
    converged_history_window_stores: RefCell<HashSet<String>>,
    poisoned: bool,
}

impl<S> Database<S>
where
    S: OrderedKvStorage,
{
    /// Open a schema-aware database over an ordered key/value store.
    ///
    /// `Database::new` does not create storage column families itself. The
    /// caller supplies storage that already has the table/index families needed
    /// by the schema; [`crate::storage::MemoryStorage`] is convenient for tests
    /// and examples.
    ///
    /// ```rust
    /// use groove::db::Database;
    /// use groove::schema::{
    ///     ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType,
    ///     PrimaryKey, TableSchema,
    /// };
    /// use groove::storage::MemoryStorage;
    ///
    /// let schema = DatabaseSchema::new([TableSchema::new(
    ///     "albums",
    ///     [
    ///         ColumnSchema::new("id", ColumnType::U64),
    ///         ColumnSchema::new("title", ColumnType::String),
    ///         ColumnSchema::new("year", ColumnType::U64),
    ///     ],
    /// )
    /// .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// let storage = MemoryStorage::new(&["albums", "indices"]);
    ///
    /// let database = Database::new(schema, storage)?;
    /// assert!(database.last_commit_metrics().is_none());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn new(schema: DatabaseSchema, storage: S) -> Result<Self, Error> {
        Self::new_with_storage_layout(schema, storage, StorageLayout::Identity)
    }

    pub fn new_with_storage_layout(
        schema: DatabaseSchema,
        storage: S,
        storage_layout: StorageLayout,
    ) -> Result<Self, Error> {
        let ivm_runtime = IvmRuntime::new(schema)?;
        Ok(Self {
            storage: LayoutStorage::new(storage, storage_layout)?,
            ivm_runtime,
            last_commit_metrics: None,
            last_tick_metrics: None,
            storage_read_metrics: RefCell::new(StorageReadMetrics::default()),
            converged_history_window_stores: RefCell::new(HashSet::new()),
            poisoned: false,
        })
    }

    /// Return approximate live bytes for one backing class/column family when
    /// the storage backend exposes that optional capability.
    pub fn approximate_class_bytes(&self, cf: &str) -> Result<Option<u64>, Error> {
        Ok(self.storage.approximate_class_bytes(cf)?)
    }

    pub fn into_storage(self) -> S {
        self.storage.into_inner()
    }

    pub fn close(&self) -> Result<(), Error> {
        Ok(self.storage.close()?)
    }

    pub fn set_auto_direct_family_enabled(&mut self, enabled: bool) {
        self.ivm_runtime.set_auto_direct_family_enabled(enabled);
    }

    /// Include arrangement and recursive-state size walks in future tick metrics.
    ///
    /// The default is `false` because those walks are diagnostic-only and scale
    /// with retained runtime state rather than with the current commit.
    pub fn set_tick_runtime_stats_enabled(&mut self, enabled: bool) {
        self.ivm_runtime.set_tick_runtime_stats_enabled(enabled);
    }

    /// Compute full runtime stats on demand.
    pub fn runtime_stats(&self) -> RuntimeStats {
        self.ivm_runtime.stats()
    }

    fn durable_indices_store_with_storage<'a, T>(
        &'a self,
        storage: &'a T,
        descriptor: &'a RecordDescriptor,
    ) -> RecordStore<'a, T>
    where
        T: OrderedKvStorage,
    {
        RecordStore::new(storage, "indices", descriptor)
    }

    pub fn open_batch(&self) -> DatabaseBatch {
        DatabaseBatch::default()
    }

    /// Open a staged batch whose reads observe writes already added to the
    /// batch. Committing the staged batch runs exactly one IVM tick and one
    /// storage write, just like [`Database::commit_batch`].
    pub fn open_staged_batch(&mut self) -> StagedDatabaseBatch<'_, S> {
        StagedDatabaseBatch {
            database: self,
            batch: DatabaseBatch::default(),
        }
    }

    /// Return a typed handle for a schema-declared direct record store.
    ///
    /// Direct stores use record encoding and order-preserving typed primary
    /// keys, but bypass table batches, index maintenance, query planning, and
    /// IVM ticks.
    ///
    /// ```rust
    /// use groove::db::Database;
    /// use groove::records::{RecordDescriptor, Value, ValueType};
    /// use groove::schema::{DatabaseSchema, DirectRecordStoreSchema};
    /// use groove::storage::MemoryStorage;
    ///
    /// let schema = DatabaseSchema::new([]).with_direct_record_store(
    ///     DirectRecordStoreSchema::new(
    ///         "album_art",
    ///         RecordDescriptor::new([("album_id", ValueType::U64), ("side", ValueType::String)]),
    ///         RecordDescriptor::new([("bytes", ValueType::Bytes)]),
    ///     ),
    /// );
    /// let column_families = schema.column_families();
    /// let storage = MemoryStorage::new(&column_families);
    /// let database = Database::new(schema, storage)?;
    ///
    /// let art = database.direct_record_store("album_art")?;
    /// art.set(
    ///     &[Value::U64(1), Value::String("front".into())],
    ///     &[Value::Bytes(b"front-cover-bytes".to_vec())],
    /// )?;
    ///
    /// let stored = art.get(&[Value::U64(1), Value::String("front".into())])?;
    /// assert_eq!(stored.unwrap().get("bytes")?, Value::Bytes(b"front-cover-bytes".to_vec()));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn direct_record_store(&self, name: &str) -> Result<DirectRecordStore<'_, S>, Error> {
        let schema = self.direct_record_store_schema(name)?;
        Ok(DirectRecordStore {
            storage: &self.storage,
            name: schema.name.clone(),
            key: schema.key_descriptor(),
            value: schema.value_descriptor(),
        })
    }

    /// Subscribe to an IVM graph and receive an initial snapshot followed by
    /// deltas from committed batches.
    ///
    /// ```rust
    /// # use groove::db::{Database, GraphBuilder};
    /// # use groove::records::Value;
    /// # use groove::schema::{ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema};
    /// # use groove::storage::MemoryStorage;
    /// # fn db() -> Result<Database<MemoryStorage>, groove::db::Error> {
    /// #     let schema = DatabaseSchema::new([TableSchema::new("albums", [
    /// #         ColumnSchema::new("id", ColumnType::U64),
    /// #         ColumnSchema::new("title", ColumnType::String),
    /// #         ColumnSchema::new("year", ColumnType::U64),
    /// #     ]).with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// #       .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// #     Database::new(schema, MemoryStorage::new(&["albums", "indices"]))
    /// # }
    /// # let mut database = db()?;
    /// let subscription = database.subscribe_one_sink(GraphBuilder::table("albums"))?;
    /// assert!(subscription.recv()?.is_empty());
    ///
    /// let mut batch = database.open_batch();
    /// batch.insert(
    ///     "albums",
    ///     vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)],
    /// );
    /// database.commit_batch(batch)?;
    ///
    /// assert_eq!(
    ///     subscription.recv()?.to_values()?,
    ///     vec![(vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)], 1)]
    /// );
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn subscribe_one_sink(&mut self, graph: GraphBuilder) -> Result<Subscription, Error> {
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.ivm_runtime
            .subscribe_one_sink(graph, &storage)
            .map_err(Error::IvmRuntime)
    }

    /// Subscribe to several named IVM graph outputs as one logical stream.
    ///
    /// The initial message includes every sink, even if that sink is empty.
    /// Later messages are sent only when at least one sink has deltas.
    pub fn subscribe<I, K>(&mut self, sinks: I) -> Result<MultisinkSubscription, Error>
    where
        I: IntoIterator<Item = (K, GraphBuilder)>,
        K: Into<String>,
    {
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.ivm_runtime
            .subscribe(sinks, &storage)
            .map_err(Error::IvmRuntime)
    }

    /// Subscribe to a SQL-ish query by letting the planner lower it into an IVM
    /// graph.
    ///
    /// ```rust
    /// # use groove::db::Database;
    /// # use groove::queries::{BinaryOp, Expr, Query, Select, SelectItem, TableRef};
    /// # use groove::records::Value;
    /// # use groove::schema::{ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema};
    /// # use groove::storage::MemoryStorage;
    /// # fn db() -> Result<Database<MemoryStorage>, groove::db::Error> {
    /// #     let schema = DatabaseSchema::new([TableSchema::new("albums", [
    /// #         ColumnSchema::new("id", ColumnType::U64),
    /// #         ColumnSchema::new("title", ColumnType::String),
    /// #         ColumnSchema::new("year", ColumnType::U64),
    /// #     ]).with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// #       .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// #     Database::new(schema, MemoryStorage::new(&["albums", "indices"]))
    /// # }
    /// # let mut database = db()?;
    /// # let mut batch = database.open_batch();
    /// # batch.insert("albums", vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)]);
    /// # batch.insert("albums", vec![Value::U64(2), Value::String("Blue Train".into()), Value::U64(1957)]);
    /// # database.commit_batch(batch)?;
    /// let query = Query::Select(Box::new(
    ///     Select::new([SelectItem::expr(Expr::column("title"))])
    ///         .from([TableRef::named("albums")])
    ///         .where_(Expr::binary(
    ///             Expr::column("year"),
    ///             BinaryOp::Eq,
    ///             Expr::Literal(Value::U64(1959)),
    ///         )),
    /// ));
    /// let subscription = database.subscribe_query(query)?;
    ///
    /// assert_eq!(
    ///     subscription.recv()?.to_values()?,
    ///     vec![(vec![Value::String("Kind of Blue".into())], 1)]
    /// );
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn subscribe_query(&mut self, query: Query) -> Result<Subscription, Error> {
        let planned = plan_query(&query, self.ivm_runtime.schema())?;
        self.subscribe_one_sink(planned.graph)
    }

    /// Prepare a parameterized SQL-ish query shape once so callers can bind many
    /// concrete parameter sets without replanning.
    ///
    /// ```rust
    /// # use groove::db::Database;
    /// # use groove::queries::{BinaryOp, Expr, Query, Select, SelectItem, TableRef};
    /// # use groove::records::Value;
    /// # use groove::schema::{ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema};
    /// # use groove::storage::MemoryStorage;
    /// # let schema = DatabaseSchema::new([TableSchema::new("albums", [
    /// #     ColumnSchema::new("id", ColumnType::U64),
    /// #     ColumnSchema::new("title", ColumnType::String),
    /// #     ColumnSchema::new("year", ColumnType::U64),
    /// # ]).with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// #   .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]))?;
    /// let query = Query::Select(Box::new(
    ///     Select::new([SelectItem::Wildcard])
    ///         .from([TableRef::named("albums")])
    ///         .where_(Expr::binary(
    ///             Expr::column("year"),
    ///             BinaryOp::Eq,
    ///             Expr::parameter("year"),
    ///         )),
    /// ));
    ///
    /// let prepared = database.prepare_query(query)?;
    /// assert_eq!(prepared.parameters()[0].name, "year");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn prepare_query(&mut self, query: Query) -> Result<PreparedShape, Error> {
        let planned = plan_prepared_shape(&query, self.ivm_runtime.schema())?;
        let output = RecordDescriptor::new(
            planned
                .public_output
                .iter()
                .map(|field| (field.name.clone(), field.value_type.clone())),
        );
        let shape = self.prepare_one_sink(
            planned.planned.graph,
            planned.shape,
            planned.binding_descriptor,
            planned.output_key_fields,
        )?;
        Ok(PreparedShape {
            id: shape.id(),
            parameters: planned.parameters,
            output,
        })
    }

    /// Bind a prepared query shape by named parameter.
    ///
    /// ```rust
    /// # use groove::db::Database;
    /// # use groove::queries::{BinaryOp, Expr, Query, Select, SelectItem, TableRef};
    /// # use groove::records::Value;
    /// # use groove::schema::{ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema};
    /// # use groove::storage::MemoryStorage;
    /// # let schema = DatabaseSchema::new([TableSchema::new("albums", [
    /// #     ColumnSchema::new("id", ColumnType::U64),
    /// #     ColumnSchema::new("title", ColumnType::String),
    /// #     ColumnSchema::new("year", ColumnType::U64),
    /// # ]).with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// #   .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]))?;
    /// # let mut batch = database.open_batch();
    /// # batch.insert("albums", vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)]);
    /// # database.commit_batch(batch)?;
    /// # let query = Query::Select(Box::new(Select::new([SelectItem::Wildcard]).from([TableRef::named("albums")]).where_(Expr::binary(Expr::column("year"), BinaryOp::Eq, Expr::parameter("year")))));
    /// # let prepared = database.prepare_query(query)?;
    /// let subscription = database.bind(&prepared, &[("year", Value::U64(1959))])?;
    ///
    /// assert_eq!(
    ///     subscription.recv()?.to_values()?,
    ///     vec![(
    ///         vec![
    ///             Value::U64(1),
    ///             Value::String("Kind of Blue".into()),
    ///             Value::U64(1959),
    ///         ],
    ///         1,
    ///     )]
    /// );
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn bind(
        &mut self,
        prepared: &PreparedShape,
        bindings: &[(&str, Value)],
    ) -> Result<Subscription, Error> {
        let mut values = Vec::with_capacity(prepared.parameters.len());
        for parameter in &prepared.parameters {
            let matching = bindings
                .iter()
                .filter(|(name, _)| *name == parameter.name)
                .collect::<Vec<_>>();
            match matching.as_slice() {
                [(_, value)] => values.push(value.clone()),
                [] => return Err(Error::MissingParameter(parameter.name.clone())),
                _ => return Err(Error::DuplicateParameter(parameter.name.clone())),
            }
        }
        for (name, _) in bindings {
            if !prepared
                .parameters
                .iter()
                .any(|parameter| parameter.name == *name)
            {
                return Err(Error::UnknownParameter((*name).to_owned()));
            }
        }
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.ivm_runtime
            .bind_shape_one_sink_with_output(prepared.id, &values, prepared.output, &storage)
            .map_err(Error::IvmRuntime)
    }

    /// Prepare a one-sink parameterized graph shape directly.
    ///
    /// Most callers should prefer [`Database::prepare_query`]. This lower-level
    /// API is useful when a caller already has a [`GraphBuilder`]. Internally it
    /// is just sugar over [`Database::prepare`]: the graph is
    /// registered as the single route-carrying terminal, `output_key_fields`
    /// name the hidden route fields, and [`Database::bind_shape_one_sink`] adapts the
    /// one sink back to a [`Subscription`].
    pub fn prepare_one_sink(
        &mut self,
        graph: GraphBuilder,
        binding_source_shape: impl Into<String>,
        binding_descriptor: RecordDescriptor,
        output_key_fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<crate::ivm::PreparedShape, Error> {
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.ivm_runtime
            .prepare_one_sink(
                graph,
                binding_source_shape,
                binding_descriptor,
                output_key_fields,
                &storage,
            )
            .map_err(Error::IvmRuntime)
    }

    /// Prepare a one-sink shape with separate public-output and route-carrying
    /// graph descriptions.
    ///
    /// This is convenience sugar over [`Database::prepare`],
    /// not a separate prepared-subscription implementation. `routing_graph` is
    /// the graph Groove maintains and routes by; `output_graph` only supplies
    /// the subscriber-visible field names and types that are projected from the
    /// routed terminal.
    pub fn prepare_one_sink_with_routing(
        &mut self,
        output_graph: GraphBuilder,
        routing_graph: GraphBuilder,
        binding_source_shape: impl Into<String>,
        binding_descriptor: RecordDescriptor,
        routing_key_fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<crate::ivm::PreparedShape, Error> {
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.ivm_runtime
            .prepare_one_sink_with_routing(
                output_graph,
                routing_graph,
                binding_source_shape,
                binding_descriptor,
                routing_key_fields,
                &storage,
            )
            .map_err(Error::IvmRuntime)
    }

    /// Prepare the canonical parameterized multisink shape.
    ///
    /// Each terminal graph carries hidden route columns plus public output
    /// columns. Binding appends ordinary filter/project graph nodes for each
    /// sink, so callers with one-sink needs should treat [`Database::prepare_one_sink`]
    /// and [`Database::prepare_one_sink_with_routing`] as thin convenience wrappers.
    pub fn prepare(
        &mut self,
        terminals: impl IntoIterator<Item = RoutedMultisinkTerminal>,
        binding_source_shape: impl Into<String>,
        binding_descriptor: RecordDescriptor,
    ) -> Result<crate::ivm::PreparedShape, Error> {
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.ivm_runtime
            .prepare(
                terminals,
                binding_source_shape,
                binding_descriptor,
                &storage,
            )
            .map_err(Error::IvmRuntime)
    }

    /// Bind a prepared one-sink graph shape by positional values.
    ///
    /// ```rust
    /// # use groove::db::{Database, GraphBuilder};
    /// # use groove::ivm::ProjectField;
    /// # use groove::records::{RecordDescriptor, Value};
    /// # use groove::schema::{ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema};
    /// # use groove::storage::MemoryStorage;
    /// # let schema = DatabaseSchema::new([TableSchema::new("albums", [
    /// #     ColumnSchema::new("id", ColumnType::U64),
    /// #     ColumnSchema::new("title", ColumnType::String),
    /// #     ColumnSchema::new("year", ColumnType::U64),
    /// # ]).with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// #   .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]))?;
    /// let binding_descriptor = RecordDescriptor::new([("year", ColumnType::U64.value_type())]);
    /// let shape = database.prepare_one_sink(
    ///     GraphBuilder::join(
    ///         GraphBuilder::binding_source("year_params", binding_descriptor),
    ///         GraphBuilder::table("albums"),
    ///         ["year"],
    ///         ["year"],
    ///     )
    ///     .project_fields([
    ///         ProjectField::renamed("right.id", "id"),
    ///         ProjectField::renamed("right.title", "title"),
    ///         ProjectField::renamed("right.year", "year"),
    ///     ]),
    ///     "year_params",
    ///     binding_descriptor,
    ///     ["id"],
    /// )?;
    ///
    /// let subscription = database.bind_shape_one_sink(shape.id(), &[Value::U64(1959)])?;
    /// assert!(subscription.recv()?.is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn bind_shape_one_sink(
        &mut self,
        shape: PreparedShapeId,
        binding_values: &[Value],
    ) -> Result<Subscription, Error> {
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.ivm_runtime
            .bind_shape_one_sink(shape, binding_values, &storage)
            .map_err(Error::IvmRuntime)
    }

    /// Bind a prepared one-sink graph shape while projecting subscriber-visible
    /// rows.
    ///
    /// This adapts the one routed multisink terminal back to [`Subscription`].
    /// The prepared terminal may contain hidden routing fields from
    /// `output_key_fields` or `routing_key_fields`; `public_output` selects the
    /// descriptor that bound subscribers receive.
    pub fn bind_shape_one_sink_with_output(
        &mut self,
        shape: PreparedShapeId,
        binding_values: &[Value],
        public_output: RecordDescriptor,
    ) -> Result<Subscription, Error> {
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.ivm_runtime
            .bind_shape_one_sink_with_output(shape, binding_values, public_output, &storage)
            .map_err(Error::IvmRuntime)
    }

    /// Bind a routed multisink shape by positional values.
    pub fn bind_shape(
        &mut self,
        shape: PreparedShapeId,
        binding_values: &[Value],
    ) -> Result<MultisinkSubscription, Error> {
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.ivm_runtime
            .bind_shape(shape, binding_values, &storage)
            .map_err(Error::IvmRuntime)
    }

    /// Run a one-shot SQL-ish query against the current storage snapshot.
    ///
    /// ```rust
    /// # use groove::db::Database;
    /// # use groove::queries::{BinaryOp, Expr, Query, Select, SelectItem, TableRef};
    /// # use groove::records::Value;
    /// # use groove::schema::{ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema};
    /// # use groove::storage::MemoryStorage;
    /// # let schema = DatabaseSchema::new([TableSchema::new("albums", [
    /// #     ColumnSchema::new("id", ColumnType::U64),
    /// #     ColumnSchema::new("title", ColumnType::String),
    /// #     ColumnSchema::new("year", ColumnType::U64),
    /// # ]).with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// #   .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]))?;
    /// # let mut batch = database.open_batch();
    /// # batch.insert("albums", vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)]);
    /// # batch.insert("albums", vec![Value::U64(2), Value::String("Blue Train".into()), Value::U64(1957)]);
    /// # database.commit_batch(batch)?;
    /// let query = Query::Select(Box::new(
    ///     Select::new([SelectItem::expr(Expr::column("title"))])
    ///         .from([TableRef::named("albums")])
    ///         .where_(Expr::binary(
    ///             Expr::column("year"),
    ///             BinaryOp::Eq,
    ///             Expr::Literal(Value::U64(1959)),
    ///         )),
    /// ));
    ///
    /// let rows = database.query(query)?;
    /// assert_eq!(rows.to_values()?, vec![(vec![Value::String("Kind of Blue".into())], 1)]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn query(&mut self, query: Query) -> Result<RecordDeltas, Error> {
        let planned = plan_query(&query, self.ivm_runtime.schema())?;
        self.query_graph(planned.graph)
    }

    /// Run a one-shot graph query against the current storage snapshot.
    ///
    /// This is the public database-level entry point for the runtime's
    /// snapshot-query path.
    ///
    /// ```rust
    /// # use groove::db::{Database, GraphBuilder, PredicateExpr};
    /// # use groove::records::Value;
    /// # use groove::schema::{ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema};
    /// # use groove::storage::MemoryStorage;
    /// # let schema = DatabaseSchema::new([TableSchema::new("albums", [
    /// #     ColumnSchema::new("id", ColumnType::U64),
    /// #     ColumnSchema::new("title", ColumnType::String),
    /// #     ColumnSchema::new("year", ColumnType::U64),
    /// # ]).with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// #   .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]))?;
    /// # let mut batch = database.open_batch();
    /// # batch.insert("albums", vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)]);
    /// # database.commit_batch(batch)?;
    /// let rows = database.query_graph(
    ///     GraphBuilder::table("albums").filter(PredicateExpr::eq("year", Value::U64(1959))),
    /// )?;
    ///
    /// assert_eq!(
    ///     rows.to_values()?,
    ///     vec![(vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)], 1)]
    /// );
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn query_graph(&mut self, graph: GraphBuilder) -> Result<RecordDeltas, Error> {
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.ivm_runtime
            .query_snapshot(graph, &storage)
            .map_err(Error::IvmRuntime)
    }

    /// Run several named graph outputs against the same current storage
    /// snapshot without registering a live subscription.
    pub fn query_graphs<I, K>(&mut self, sinks: I) -> Result<MultisinkDeltas, Error>
    where
        I: IntoIterator<Item = (K, GraphBuilder)>,
        K: Into<String>,
    {
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.ivm_runtime
            .query_snapshots(sinks, &storage)
            .map_err(Error::IvmRuntime)
    }

    /// Return decoded records whose explicit schema index exactly matches the
    /// supplied index-column key.
    ///
    /// The read observes all committed batches. Reads while the caller still
    /// holds an uncommitted [`DatabaseBatch`] observe the pre-batch state.
    pub fn index_get(
        &self,
        table: &str,
        index_name: &str,
        key: &[Value],
    ) -> Result<Vec<Record<'_>>, Error> {
        let index = self.index(table, index_name)?;
        if key.len() != index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: key.len(),
            });
        }
        self.index_scan(table, index_name, key)
    }

    /// Return decoded records whose explicit schema index starts with the
    /// supplied index-column prefix, in persisted index-key order.
    ///
    /// The read observes all committed batches. Reads while the caller still
    /// holds an uncommitted [`DatabaseBatch`] observe the pre-batch state.
    ///
    /// ```rust
    /// # use groove::db::Database;
    /// # use groove::records::Value;
    /// # use groove::schema::{ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema};
    /// # use groove::storage::MemoryStorage;
    /// # let schema = DatabaseSchema::new([TableSchema::new("albums", [
    /// #     ColumnSchema::new("id", ColumnType::U64),
    /// #     ColumnSchema::new("title", ColumnType::String),
    /// #     ColumnSchema::new("year", ColumnType::U64),
    /// # ]).with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// #   .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]))?;
    /// # let mut batch = database.open_batch();
    /// # batch.insert("albums", vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)]);
    /// # batch.insert("albums", vec![Value::U64(2), Value::String("Blue Train".into()), Value::U64(1957)]);
    /// # database.commit_batch(batch)?;
    /// let rows = database.index_scan("albums", "albums_by_year", &[Value::U64(1959)])?;
    ///
    /// assert_eq!(rows.len(), 1);
    /// assert_eq!(rows[0].get("title")?, Value::String("Kind of Blue".into()));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn index_scan(
        &self,
        table: &str,
        index_name: &str,
        prefix: &[Value],
    ) -> Result<Vec<Record<'_>>, Error> {
        let index = self.index(table, index_name)?;
        if prefix.len() > index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: prefix.len(),
            });
        }
        let raw_entries = self.index_scan_raw(table, index_name, prefix)?;
        self.decode_index_records(table, index_name, raw_entries)
    }

    /// Return decoded records whose explicit schema index is in the supplied
    /// logical index-key range.
    ///
    /// The lower bound is inclusive. The upper bound is exclusive at the
    /// logical-key level and includes non-unique primary-key suffixes for that
    /// logical prefix.
    ///
    /// ```rust
    /// # use groove::db::Database;
    /// # use groove::records::Value;
    /// # use groove::schema::{ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema};
    /// # use groove::storage::MemoryStorage;
    /// # let schema = DatabaseSchema::new([TableSchema::new("albums", [
    /// #     ColumnSchema::new("id", ColumnType::U64),
    /// #     ColumnSchema::new("title", ColumnType::String),
    /// #     ColumnSchema::new("year", ColumnType::U64),
    /// # ]).with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// #   .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]))?;
    /// # let mut batch = database.open_batch();
    /// # batch.insert("albums", vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)]);
    /// # batch.insert("albums", vec![Value::U64(2), Value::String("Blue Train".into()), Value::U64(1957)]);
    /// # batch.insert("albums", vec![Value::U64(3), Value::String("A Love Supreme".into()), Value::U64(1965)]);
    /// # database.commit_batch(batch)?;
    /// let rows = database.index_scan_range(
    ///     "albums",
    ///     "albums_by_year",
    ///     &[Value::U64(1957)],
    ///     &[Value::U64(1960)],
    /// )?;
    ///
    /// let titles = rows
    ///     .iter()
    ///     .map(|row| row.get("title"))
    ///     .collect::<Result<Vec<_>, _>>()?;
    /// assert_eq!(
    ///     titles,
    ///     vec![Value::String("Blue Train".into()), Value::String("Kind of Blue".into())]
    /// );
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn index_scan_range(
        &self,
        table: &str,
        index_name: &str,
        start: &[Value],
        end: &[Value],
    ) -> Result<Vec<Record<'_>>, Error> {
        let index = self.index(table, index_name)?;
        if start.len() > index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: start.len(),
            });
        }
        if end.len() > index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: end.len(),
            });
        }
        let raw_entries = self.index_scan_range_raw(table, index_name, start, end)?;
        self.decode_index_records(table, index_name, raw_entries)
    }

    fn decode_index_records(
        &self,
        table: &str,
        index_name: &str,
        raw_entries: Vec<EncodedKeyValue<'_>>,
    ) -> Result<Vec<Record<'_>>, Error> {
        let descriptor = self
            .ivm_runtime
            .table_descriptor(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
        let _ = index_name;
        Ok(raw_entries
            .into_iter()
            .map(|entry| descriptor.bind_owned(entry.into_parts().1))
            .collect())
    }

    /// Return decoded records whose primary key starts with the supplied key
    /// prefix, in primary-key order.
    ///
    /// The read observes all committed batches. Reads while the caller still
    /// holds an uncommitted [`DatabaseBatch`] observe the pre-batch state.
    pub fn primary_key_scan(
        &self,
        table: &str,
        prefix: &[Value],
    ) -> Result<Vec<Record<'_>>, Error> {
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.primary_key_scan_with_storage(&storage, table, prefix)
    }

    fn primary_key_scan_with_storage<'a, T>(
        &'a self,
        storage: &T,
        table: &str,
        prefix: &[Value],
    ) -> Result<Vec<Record<'a>>, Error>
    where
        T: OrderedKvStorage,
    {
        let raw = self.primary_key_scan_raw_with_storage(storage, table, prefix)?;
        let descriptor = self
            .ivm_runtime
            .table_descriptor(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
        Ok(raw
            .into_iter()
            .map(|entry| descriptor.bind_owned(entry.into_parts().1))
            .collect())
    }

    /// Return encoded records whose primary key starts with the supplied key
    /// prefix, in primary-key order.
    ///
    /// The read observes all committed batches. Reads while the caller still
    /// holds an uncommitted [`DatabaseBatch`] observe the pre-batch state.
    pub fn primary_key_scan_raw(
        &self,
        table: &str,
        prefix: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'_>>, Error> {
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.primary_key_scan_raw_with_storage(&storage, table, prefix)
    }

    /// Return encoded primary-key records while also observing writes already
    /// staged in `batch`.
    pub fn primary_key_scan_raw_in_batch(
        &self,
        batch: &DatabaseBatch,
        table: &str,
        prefix: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'_>>, Error> {
        self.ensure_batch_storage_txn(batch)?;
        let overlay = StagedWriteOverlay::new(&self.storage, &batch.txn_operations);
        let storage = MeteredStorage::new(&overlay, &self.storage_read_metrics);
        self.primary_key_scan_raw_with_storage(&storage, table, prefix)
    }

    /// Return one encoded record by its full primary key.
    ///
    /// This is the point-read counterpart to [`Self::primary_key_scan_raw`].
    /// `key` must provide every primary-key column; callers that need a prefix
    /// or range must use the scan APIs.
    pub fn primary_key_get_raw(
        &self,
        table: &str,
        key: &[Value],
    ) -> Result<Option<EncodedKeyValue<'_>>, Error> {
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.primary_key_get_raw_with_storage(&storage, table, key)
    }

    /// Return one encoded primary-key record while also observing writes
    /// already staged in `batch`.
    pub fn primary_key_get_raw_in_batch(
        &self,
        batch: &DatabaseBatch,
        table: &str,
        key: &[Value],
    ) -> Result<Option<EncodedKeyValue<'_>>, Error> {
        self.ensure_batch_storage_txn(batch)?;
        let table_schema = self.table(table)?;
        let primary_key = table_schema
            .primary_key
            .as_ref()
            .ok_or_else(|| Error::MissingPrimaryKey(table.to_owned()))?;
        if key.len() != primary_key.columns.len() {
            return Err(Error::PrimaryKeyArity {
                table: table.to_owned(),
                expected: primary_key.columns.len(),
                actual: key.len(),
            });
        }
        let descriptor = self
            .ivm_runtime
            .table_descriptor(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
        let mut encoded_key = Vec::new();
        for (value, column) in key.iter().zip(&primary_key.columns) {
            ensure_primary_key_value_type(table_schema, column, value)?;
            encode_primary_key_part(&mut encoded_key, value);
        }
        let staged_contains_key = batch
            .txn_operations
            .borrow_mut()
            .contains_key(table, &encoded_key);
        if !staged_contains_key {
            let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
            let key_descriptor = primary_key_descriptor(primary_key);
            let store = record_store_for_table(&storage, table, Some(key_descriptor), descriptor);
            return Ok(store
                .get_raw(&encoded_key)?
                .map(|value| EncodedKeyValue::new(encoded_key, value, descriptor)));
        }

        let overlay = StagedWriteOverlay::new(&self.storage, &batch.txn_operations);
        let storage = MeteredStorage::new(&overlay, &self.storage_read_metrics);
        let key_descriptor = primary_key_descriptor(primary_key);
        let store = record_store_for_table(&storage, table, Some(key_descriptor), descriptor);
        Ok(store
            .get_raw(&encoded_key)?
            .map(|value| EncodedKeyValue::new(encoded_key, value, descriptor)))
    }

    fn primary_key_get_raw_with_storage<'a, T>(
        &'a self,
        storage: &T,
        table: &str,
        key: &[Value],
    ) -> Result<Option<EncodedKeyValue<'a>>, Error>
    where
        T: OrderedKvStorage,
    {
        let table_schema = self.table(table)?;
        let primary_key = table_schema
            .primary_key
            .as_ref()
            .ok_or_else(|| Error::MissingPrimaryKey(table.to_owned()))?;
        if key.len() != primary_key.columns.len() {
            return Err(Error::PrimaryKeyArity {
                table: table.to_owned(),
                expected: primary_key.columns.len(),
                actual: key.len(),
            });
        }
        let descriptor = self
            .ivm_runtime
            .table_descriptor(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
        let mut encoded_key = Vec::new();
        for (value, column) in key.iter().zip(&primary_key.columns) {
            ensure_primary_key_value_type(table_schema, column, value)?;
            encode_primary_key_part(&mut encoded_key, value);
        }
        let key_descriptor = primary_key_descriptor(primary_key);
        let store = record_store_for_table(storage, table, Some(key_descriptor), descriptor);
        Ok(store
            .get_raw(&encoded_key)?
            .map(|value| EncodedKeyValue::new(encoded_key, value, descriptor)))
    }

    fn primary_key_scan_raw_with_storage<'a, T>(
        &'a self,
        storage: &T,
        table: &str,
        prefix: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'a>>, Error>
    where
        T: OrderedKvStorage,
    {
        let table_schema = self.table(table)?;
        let primary_key = table_schema
            .primary_key
            .as_ref()
            .ok_or_else(|| Error::MissingPrimaryKey(table.to_owned()))?;
        if prefix.len() > primary_key.columns.len() {
            return Err(Error::PrimaryKeyArity {
                table: table.to_owned(),
                expected: primary_key.columns.len(),
                actual: prefix.len(),
            });
        }
        let descriptor = self
            .ivm_runtime
            .table_descriptor(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
        let mut key_prefix = Vec::new();
        for (value, column) in prefix.iter().zip(&primary_key.columns) {
            ensure_primary_key_value_type(table_schema, column, value)?;
            encode_primary_key_part(&mut key_prefix, value);
        }
        let key_descriptor = primary_key_descriptor(primary_key);
        let store = record_store_for_table(storage, table, Some(key_descriptor), descriptor);
        Ok(store
            .prefix(&key_prefix)?
            .into_iter()
            .map(|(key, value)| EncodedKeyValue::new(key, value, descriptor))
            .collect())
    }

    fn primary_key_last_raw_with_storage<'a, T>(
        &'a self,
        storage: &T,
        table: &str,
        prefix: &[Value],
    ) -> Result<Option<EncodedKeyValue<'a>>, Error>
    where
        T: OrderedKvStorage,
    {
        let table_schema = self.table(table)?;
        let primary_key = table_schema
            .primary_key
            .as_ref()
            .ok_or_else(|| Error::MissingPrimaryKey(table.to_owned()))?;
        if prefix.len() > primary_key.columns.len() {
            return Err(Error::PrimaryKeyArity {
                table: table.to_owned(),
                expected: primary_key.columns.len(),
                actual: prefix.len(),
            });
        }
        let descriptor = self
            .ivm_runtime
            .table_descriptor(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
        let mut key_prefix = Vec::new();
        for (value, column) in prefix.iter().zip(&primary_key.columns) {
            ensure_primary_key_value_type(table_schema, column, value)?;
            encode_primary_key_part(&mut key_prefix, value);
        }
        let key_descriptor = primary_key_descriptor(primary_key);
        let store = record_store_for_table(storage, table, Some(key_descriptor), descriptor);
        Ok(store
            .last_with_prefix(&key_prefix)?
            .map(|(key, value)| EncodedKeyValue::new(key, value, descriptor)))
    }

    /// Return encoded records for an explicit primary-key logical range.
    ///
    /// The lower bound is inclusive. The upper bound is exclusive. Bounds must
    /// provide the full primary key.
    pub fn primary_key_scan_range_raw(
        &self,
        table: &str,
        start: &[Value],
        end: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'_>>, Error> {
        let table_schema = self.table(table)?;
        let primary_key = table_schema
            .primary_key
            .as_ref()
            .ok_or_else(|| Error::MissingPrimaryKey(table.to_owned()))?;
        if start.len() != primary_key.columns.len() {
            return Err(Error::PrimaryKeyArity {
                table: table.to_owned(),
                expected: primary_key.columns.len(),
                actual: start.len(),
            });
        }
        if end.len() != primary_key.columns.len() {
            return Err(Error::PrimaryKeyArity {
                table: table.to_owned(),
                expected: primary_key.columns.len(),
                actual: end.len(),
            });
        }
        let descriptor = self
            .ivm_runtime
            .table_descriptor(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
        let mut start_key = Vec::new();
        for (value, column) in start.iter().zip(&primary_key.columns) {
            ensure_primary_key_value_type(table_schema, column, value)?;
            encode_primary_key_part(&mut start_key, value);
        }
        let mut end_key = Vec::new();
        for (value, column) in end.iter().zip(&primary_key.columns) {
            ensure_primary_key_value_type(table_schema, column, value)?;
            encode_primary_key_part(&mut end_key, value);
        }
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        let key_descriptor = primary_key_descriptor(primary_key);
        let store = record_store_for_table(&storage, table, Some(key_descriptor), descriptor);
        Ok(store
            .range(&start_key, &end_key)?
            .into_iter()
            .map(|(key, value)| EncodedKeyValue::new(key, value, descriptor))
            .collect())
    }

    /// Return the last encoded record whose primary key starts with the
    /// supplied key prefix.
    ///
    /// The read observes all committed batches. Reads while the caller still
    /// holds an uncommitted [`DatabaseBatch`] observe the pre-batch state.
    pub fn primary_key_last_raw(
        &self,
        table: &str,
        prefix: &[Value],
    ) -> Result<Option<EncodedKeyValue<'_>>, Error> {
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.primary_key_last_raw_with_storage(&storage, table, prefix)
    }

    /// Return the last encoded primary-key record while also observing writes
    /// already staged in `batch`.
    pub fn primary_key_last_raw_in_batch(
        &self,
        batch: &DatabaseBatch,
        table: &str,
        prefix: &[Value],
    ) -> Result<Option<EncodedKeyValue<'_>>, Error> {
        self.ensure_batch_storage_txn(batch)?;
        let overlay = StagedWriteOverlay::new(&self.storage, &batch.txn_operations);
        let storage = MeteredStorage::new(&overlay, &self.storage_read_metrics);
        self.primary_key_last_raw_with_storage(&storage, table, prefix)
    }

    /// Return the last encoded record whose primary key starts with `prefix`
    /// and whose full primary key is less than or equal to `upper`.
    ///
    /// `upper` must provide the full primary key. The read observes all
    /// committed batches. Reads while the caller still holds an uncommitted
    /// [`DatabaseBatch`] observe the pre-batch state.
    pub fn primary_key_last_before_or_at_raw(
        &self,
        table: &str,
        prefix: &[Value],
        upper: &[Value],
    ) -> Result<Option<EncodedKeyValue<'_>>, Error> {
        let table_schema = self.table(table)?;
        let primary_key = table_schema
            .primary_key
            .as_ref()
            .ok_or_else(|| Error::MissingPrimaryKey(table.to_owned()))?;
        if prefix.len() > primary_key.columns.len() {
            return Err(Error::PrimaryKeyArity {
                table: table.to_owned(),
                expected: primary_key.columns.len(),
                actual: prefix.len(),
            });
        }
        if upper.len() != primary_key.columns.len() {
            return Err(Error::PrimaryKeyArity {
                table: table.to_owned(),
                expected: primary_key.columns.len(),
                actual: upper.len(),
            });
        }
        let descriptor = self
            .ivm_runtime
            .table_descriptor(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
        let mut key_prefix = Vec::new();
        for (value, column) in prefix.iter().zip(&primary_key.columns) {
            ensure_primary_key_value_type(table_schema, column, value)?;
            encode_primary_key_part(&mut key_prefix, value);
        }
        let mut upper_key = Vec::new();
        for (value, column) in upper.iter().zip(&primary_key.columns) {
            ensure_primary_key_value_type(table_schema, column, value)?;
            encode_primary_key_part(&mut upper_key, value);
        }
        if !upper_key.starts_with(&key_prefix) {
            return Ok(None);
        }
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        let key_descriptor = primary_key_descriptor(primary_key);
        let store = record_store_for_table(&storage, table, Some(key_descriptor), descriptor);
        Ok(store
            .last_with_prefix_before_or_at(&key_prefix, &upper_key)?
            .map(|(key, value)| EncodedKeyValue::new(key, value, descriptor)))
    }

    /// Consolidate plain records for an opted-in physical record store into
    /// bounded codec windows.
    ///
    /// This is deliberately explicit for now: hot writes still land as plain
    /// records, and future flush/checkpoint plumbing can call this at a safe
    /// consolidation boundary.
    pub fn consolidate_table_windows(
        &self,
        table: &str,
        max_records: usize,
    ) -> Result<WindowConsolidation, Error> {
        let table_schema = self.table(table)?;
        let Some(primary_key) = table_schema.primary_key.as_ref() else {
            return Err(Error::MissingPrimaryKey(table.to_owned()));
        };
        let descriptor = self
            .ivm_runtime
            .table_descriptor(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
        let key_descriptor = primary_key_descriptor(primary_key);
        let store = record_store_for_table(&self.storage, table, Some(key_descriptor), descriptor);
        store.consolidate_windows(max_records).map_err(Error::from)
    }

    /// Consolidate at most `max_windows` codec windows across opted-in history
    /// tables in this schema.
    ///
    /// This is intended for caller-owned post-tick maintenance: the write path
    /// remains plain-record first, and this bounded pass rewrites old runs in
    /// atomic storage batches between runtime ticks.
    pub fn consolidate_history_windows(
        &self,
        max_records_per_window: usize,
        max_windows: usize,
    ) -> Result<WindowConsolidation, Error> {
        let mut remaining_windows = max_windows;
        let mut total = WindowConsolidation::default();
        if max_records_per_window == 0 || max_windows == 0 {
            return Ok(total);
        }
        if self.all_history_window_stores_converged() {
            return Ok(total);
        }
        for table in &self.ivm_runtime.schema().tables {
            if remaining_windows == 0 {
                break;
            }
            if !is_windowed_history_table(&table.name) {
                continue;
            }
            if self.history_window_store_converged(&table.name) {
                continue;
            }
            let Some(primary_key) = table.primary_key.as_ref() else {
                continue;
            };
            let Some(descriptor) = self.ivm_runtime.table_descriptor(&table.name) else {
                continue;
            };
            let key_descriptor = primary_key_descriptor(primary_key);
            let store = record_store_for_table(
                &self.storage,
                &table.name,
                Some(key_descriptor),
                descriptor,
            );
            let next = store
                .consolidate_full_windows_bounded(max_records_per_window, remaining_windows)
                .map_err(Error::from)?;
            if next.windows == 0 {
                self.mark_history_window_store_converged(&table.name);
            }
            remaining_windows = remaining_windows.saturating_sub(next.windows);
            total.windows += next.windows;
            total.records += next.records;
        }
        for store in &self.ivm_runtime.schema().direct_record_stores {
            if remaining_windows == 0 {
                break;
            }
            if !is_windowed_history_table(&store.name) {
                continue;
            }
            if self.history_window_store_converged(&store.name) {
                continue;
            }
            let key_descriptor = RecordDescriptor::new(store.key.clone());
            let descriptor = RecordDescriptor::new(store.value.clone());
            let record_store = record_store_for_table(
                &self.storage,
                &store.name,
                Some(key_descriptor),
                &descriptor,
            );
            let next = record_store
                .consolidate_full_windows_bounded(max_records_per_window, remaining_windows)
                .map_err(Error::from)?;
            if next.windows == 0 {
                self.mark_history_window_store_converged(&store.name);
            }
            remaining_windows = remaining_windows.saturating_sub(next.windows);
            total.windows += next.windows;
            total.records += next.records;
        }
        Ok(total)
    }

    fn all_history_window_stores_converged(&self) -> bool {
        let converged_stores = self.converged_history_window_stores.borrow();
        let mut saw_history_store = false;

        for table in &self.ivm_runtime.schema().tables {
            if !is_windowed_history_table(&table.name) {
                continue;
            }
            saw_history_store = true;
            if !converged_stores.contains(&table.name) {
                return false;
            }
        }

        for store in &self.ivm_runtime.schema().direct_record_stores {
            if !is_windowed_history_table(&store.name) {
                continue;
            }
            saw_history_store = true;
            if !converged_stores.contains(&store.name) {
                return false;
            }
        }

        saw_history_store
    }

    fn history_window_store_converged(&self, store: &str) -> bool {
        self.converged_history_window_stores
            .borrow()
            .contains(store)
    }

    fn mark_history_window_store_converged(&self, store: &str) {
        self.converged_history_window_stores
            .borrow_mut()
            .insert(store.to_owned());
    }

    fn mark_history_window_store_dirty(&self, store: &str) {
        self.converged_history_window_stores
            .borrow_mut()
            .remove(store);
    }

    /// Return encoded records whose explicit schema index exactly matches the
    /// supplied index-column key.
    ///
    /// The read observes all committed batches. Reads while the caller still
    /// holds an uncommitted [`DatabaseBatch`] observe the pre-batch state.
    pub fn index_get_raw(
        &self,
        table: &str,
        index_name: &str,
        key: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'_>>, Error> {
        let index = self.index(table, index_name)?;
        if key.len() != index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: key.len(),
            });
        }
        self.index_scan_raw(table, index_name, key)
    }

    /// Return encoded records whose explicit schema index starts with the
    /// supplied index-column prefix.
    ///
    /// The read observes all committed batches. Reads while the caller still
    /// holds an uncommitted [`DatabaseBatch`] observe the pre-batch state.
    pub fn index_scan_raw(
        &self,
        table: &str,
        index_name: &str,
        prefix: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'_>>, Error> {
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.index_scan_raw_with_storage(&storage, table, index_name, prefix)
    }

    /// Return encoded index-probe records while also observing writes already
    /// staged in `batch`.
    pub fn index_scan_raw_in_batch(
        &self,
        batch: &DatabaseBatch,
        table: &str,
        index_name: &str,
        prefix: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'_>>, Error> {
        self.ensure_batch_storage_txn(batch)?;
        let overlay = StagedWriteOverlay::new(&self.storage, &batch.txn_operations);
        let storage = MeteredStorage::new(&overlay, &self.storage_read_metrics);
        self.index_scan_raw_with_storage(&storage, table, index_name, prefix)
    }

    fn index_scan_raw_with_storage<'a, T>(
        &'a self,
        storage: &T,
        table: &str,
        index_name: &str,
        prefix: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'a>>, Error>
    where
        T: OrderedKvStorage,
    {
        let index = self.index(table, index_name)?;
        if prefix.len() > index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: prefix.len(),
            });
        }
        let storage_prefix = self.persisted_index_scan_prefix(table, index_name, prefix)?;
        let index_descriptor = index_record_descriptor();
        let raw_entries = self
            .durable_indices_store_with_storage(storage, &index_descriptor)
            .prefix(&storage_prefix)?;
        self.decode_raw_index_entries_with_storage(storage, table, index_name, raw_entries)
    }

    /// Return the last encoded record whose explicit schema index starts with
    /// the supplied index-column prefix.
    pub fn index_last_raw(
        &self,
        table: &str,
        index_name: &str,
        prefix: &[Value],
    ) -> Result<Option<EncodedKeyValue<'_>>, Error> {
        let index = self.index(table, index_name)?;
        if prefix.len() > index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: prefix.len(),
            });
        }
        let storage_prefix = self.persisted_index_scan_prefix(table, index_name, prefix)?;
        let index_descriptor = index_record_descriptor();
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        let Some(raw_entry) = self
            .durable_indices_store_with_storage(&storage, &index_descriptor)
            .last_with_prefix(&storage_prefix)?
        else {
            return Ok(None);
        };
        Ok(self
            .decode_raw_index_entries_with_storage(&storage, table, index_name, vec![raw_entry])?
            .into_iter()
            .next())
    }

    /// Return encoded records for an explicit schema index logical-key range.
    ///
    /// The read observes all committed batches. Reads while the caller still
    /// holds an uncommitted [`DatabaseBatch`] observe the pre-batch state.
    pub fn index_scan_range_raw(
        &self,
        table: &str,
        index_name: &str,
        start: &[Value],
        end: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'_>>, Error> {
        let index = self.index(table, index_name)?;
        if start.len() > index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: start.len(),
            });
        }
        if end.len() > index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: end.len(),
            });
        }
        let start = self.persisted_index_scan_prefix(table, index_name, start)?;
        let end = self.persisted_index_scan_prefix(table, index_name, end)?;
        let index_descriptor = index_record_descriptor();
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        let raw_entries = self
            .durable_indices_store_with_storage(&storage, &index_descriptor)
            .range(&start, &end)?;
        self.decode_raw_index_entries_with_storage(&storage, table, index_name, raw_entries)
    }

    fn decode_raw_index_entries_with_storage<'a, T>(
        &'a self,
        storage: &T,
        table: &str,
        index_name: &str,
        raw_entries: Vec<crate::storage::KeyValue>,
    ) -> Result<Vec<EncodedKeyValue<'a>>, Error>
    where
        T: OrderedKvStorage,
    {
        let table_schema = self.table(table)?;
        let descriptor = self
            .ivm_runtime
            .table_descriptor(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
        let key_descriptor = table_schema
            .primary_key
            .as_ref()
            .map(primary_key_descriptor);
        let store = record_store_for_table(storage, table, key_descriptor, descriptor);
        let index_descriptor = index_record_descriptor();
        let mut records = Vec::new();
        for (storage_key, persisted_record) in raw_entries {
            let index_record = index_descriptor.bind(&persisted_record);
            let primary_key = persisted_index_primary_key(
                table_schema,
                index_name,
                self.index(table, index_name)?,
                &storage_key,
                &index_record.get("value")?,
            )?;
            if let Some(record) = store.get_raw(&primary_key)? {
                records.push(EncodedKeyValue::new(primary_key, record, descriptor));
            } else if table_schema.primary_key.is_some() {
                return Err(Error::InvalidPersistedIndex(index_name.to_owned()));
            }
        }
        Ok(records)
    }

    pub fn unsubscribe(&mut self, subscription_id: SubscriptionId) -> bool {
        self.ivm_runtime
            .unsubscribe_with_storage(subscription_id, &self.storage)
            .unwrap_or(false)
    }

    /// Run one IVM tick without base-table writes.
    ///
    /// Commiting a batch ticks automatically; `flush` is useful after creating
    /// subscriptions when callers want to drain any pending initial work through
    /// the same public tick path.
    ///
    /// ```rust
    /// # use groove::db::{Database, GraphBuilder};
    /// # use groove::schema::{ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema};
    /// # use groove::storage::MemoryStorage;
    /// # let schema = DatabaseSchema::new([TableSchema::new("albums", [
    /// #     ColumnSchema::new("id", ColumnType::U64),
    /// #     ColumnSchema::new("title", ColumnType::String),
    /// #     ColumnSchema::new("year", ColumnType::U64),
    /// # ]).with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// #   .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]))?;
    /// let subscription = database.subscribe_one_sink(GraphBuilder::table("albums"))?;
    /// assert!(subscription.recv()?.is_empty());
    ///
    /// database.flush()?;
    /// assert!(database.last_tick_metrics().is_some());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn flush(&mut self) -> Result<(), Error> {
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        let tick = self
            .ivm_runtime
            .tick(Vec::new(), &storage)
            .map_err(Error::IvmRuntime)?;
        self.last_tick_metrics = Some(tick);
        Ok(())
    }

    pub fn last_commit_metrics(&self) -> Option<&CommitMetrics> {
        self.last_commit_metrics.as_ref()
    }

    pub fn last_tick_metrics(&self) -> Option<&TickMetrics> {
        self.last_tick_metrics.as_ref()
    }

    pub fn storage_read_metrics(&self) -> StorageReadMetrics {
        *self.storage_read_metrics.borrow()
    }

    pub fn reset_storage_read_metrics(&self) {
        *self.storage_read_metrics.borrow_mut() = StorageReadMetrics::default();
    }

    pub fn take_storage_read_metrics(&self) -> StorageReadMetrics {
        let metrics = self.storage_read_metrics();
        self.reset_storage_read_metrics();
        metrics
    }

    /// Commit a batch of table writes and synchronously tick maintained views.
    ///
    /// ```rust
    /// # use groove::db::Database;
    /// # use groove::records::Value;
    /// # use groove::schema::{ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema};
    /// # use groove::storage::MemoryStorage;
    /// # let schema = DatabaseSchema::new([TableSchema::new("albums", [
    /// #     ColumnSchema::new("id", ColumnType::U64),
    /// #     ColumnSchema::new("title", ColumnType::String),
    /// #     ColumnSchema::new("year", ColumnType::U64),
    /// # ]).with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// #   .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]))?;
    /// let mut batch = database.open_batch();
    /// batch.insert(
    ///     "albums",
    ///     vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)],
    /// );
    /// database.commit_batch(batch)?;
    ///
    /// let rows = database.primary_key_scan("albums", &[Value::U64(1)])?;
    /// assert_eq!(rows[0].get("title")?, Value::String("Kind of Blue".into()));
    /// assert_eq!(database.last_commit_metrics().unwrap().storage_write_count, 2);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn commit_batch(&mut self, batch: DatabaseBatch) -> Result<(), Error> {
        let pending_writes = self.pending_writes_from_batch(batch)?;
        self.commit_pending_writes(pending_writes)
    }

    pub fn update_raw(
        &mut self,
        table: &str,
        key: PrimaryKeyValue,
        record: Vec<u8>,
    ) -> Result<(), Error> {
        self.table(table)?;
        self.commit_pending_writes(vec![PendingTableWrite::Set {
            mode: WriteMode::Update,
            table: table.to_owned(),
            key: key.into_bytes(),
            record,
        }])
    }

    fn commit_pending_writes(
        &mut self,
        pending_writes: Vec<PendingTableWrite>,
    ) -> Result<(), Error> {
        let descriptors = pending_writes
            .iter()
            .map(|write| self.table_descriptor(write.table()))
            .collect::<Result<Vec<_>, _>>()?;
        let stores = pending_writes
            .iter()
            .zip(&descriptors)
            .map(|(write, descriptor)| {
                let key_descriptor = self
                    .table(write.table())
                    .ok()
                    .and_then(|table| table.primary_key.as_ref().map(primary_key_descriptor));
                record_store_for_table(&self.storage, write.table(), key_descriptor, descriptor)
            })
            .collect::<Vec<_>>();
        let table_deltas = compute_table_deltas(&pending_writes, &stores)?;
        let base_operations = pending_writes
            .iter()
            .zip(&stores)
            .map(|(write, store)| match write {
                PendingTableWrite::Set { key, record, .. } => store.set(key, record),
                PendingTableWrite::Delete { key, .. } => store.delete(key),
            })
            .collect::<Vec<_>>();
        let mut staged_operations = base_operations
            .iter()
            .map(|operation| owned_write_operation(operation))
            .collect::<Vec<_>>();
        let tick_start = Instant::now();
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        let tick = self
            .ivm_runtime
            .tick_staged(table_deltas, &storage, &mut staged_operations)
            .map_err(Error::IvmRuntime)?;
        let ivm_tick_time = tick_start.elapsed();
        let operations = staged_operations
            .iter()
            .map(OwnedWriteOperation::as_write_operation)
            .collect::<Vec<_>>();
        let storage_writes = StorageWriteMetrics::from_operations(&operations);
        let storage_write_count = storage_writes.total.count;
        let storage_write_bytes = storage_writes.total.bytes;
        let storage_start = Instant::now();
        let txn = self.storage.begin_txn();
        drop(operations);
        txn.stage_owned_operations(staged_operations);
        if let Err(error) = txn.commit() {
            // The runtime has already advanced in memory by this point. The v0
            // policy is to make the Database instance fatal on final commit
            // failure rather than serve possibly torn in-memory state.
            self.poisoned = true;
            return Err(Error::from(error));
        }
        for write in &pending_writes {
            let table = write.table();
            if is_windowed_history_table(table) {
                self.mark_history_window_store_dirty(table);
            }
        }
        let storage_write_time = storage_start.elapsed();
        self.last_tick_metrics = Some(tick.clone());
        self.last_commit_metrics = Some(CommitMetrics {
            storage_write_time,
            ivm_tick_time,
            storage_write_count,
            storage_write_bytes,
            storage_writes,
            tick,
        });
        Ok(())
    }

    fn pending_writes_from_batch(
        &self,
        batch: DatabaseBatch,
    ) -> Result<Vec<PendingTableWrite>, Error> {
        self.pending_writes_from_operations(&batch.operations)
    }

    fn pending_writes_from_operations(
        &self,
        operations: &[BatchOperation],
    ) -> Result<Vec<PendingTableWrite>, Error> {
        let mut pending_writes = Vec::with_capacity(operations.len());

        for operation in operations {
            pending_writes.push(self.pending_write_from_operation(operation)?);
        }

        Ok(pending_writes)
    }

    fn ensure_batch_storage_txn(&self, batch: &DatabaseBatch) -> Result<(), Error> {
        let mut txn_operations = batch.txn_operations.borrow_mut();
        while batch.txn_indexed_operations.get() < batch.operations.len() {
            let operation = &batch.operations[batch.txn_indexed_operations.get()];
            let pending = self.pending_write_from_operation(operation)?;
            txn_operations.stage(self.owned_storage_operation_for_pending(&pending)?);
            batch
                .txn_indexed_operations
                .set(batch.txn_indexed_operations.get() + 1);
        }
        Ok(())
    }

    fn owned_storage_operation_for_pending(
        &self,
        pending: &PendingTableWrite,
    ) -> Result<OwnedWriteOperation, Error> {
        Ok(match pending {
            PendingTableWrite::Set { key, record, .. } => OwnedWriteOperation::Set {
                cf: pending.table().to_owned(),
                key: key.clone(),
                value: record.clone(),
            },
            PendingTableWrite::Delete { key, .. } => OwnedWriteOperation::Delete {
                cf: pending.table().to_owned(),
                key: key.clone(),
            },
        })
    }

    fn pending_write_from_operation(
        &self,
        operation: &BatchOperation,
    ) -> Result<PendingTableWrite, Error> {
        match operation {
            BatchOperation::Insert { table, values } => {
                let table_schema = self.table(table)?;
                let descriptor = self.table_descriptor(table)?;
                let record = encode_record(table_schema, descriptor, values)?;
                let key = primary_key_bytes(table_schema, descriptor, &record)?;
                Ok(PendingTableWrite::Set {
                    mode: WriteMode::Insert,
                    table: table.clone(),
                    key,
                    record,
                })
            }
            BatchOperation::InsertRaw { table, key, record } => {
                self.table(table)?;
                Ok(PendingTableWrite::Set {
                    mode: WriteMode::Insert,
                    table: table.clone(),
                    key: key.clone().into_bytes(),
                    record: record.clone(),
                })
            }
            BatchOperation::InsertRawFresh { table, key, record } => {
                self.table(table)?;
                Ok(PendingTableWrite::Set {
                    mode: WriteMode::InsertFresh,
                    table: table.clone(),
                    key: key.clone().into_bytes(),
                    record: record.clone(),
                })
            }
            BatchOperation::Update { table, values } => {
                let table_schema = self.table(table)?;
                let descriptor = self.table_descriptor(table)?;
                let record = encode_record(table_schema, descriptor, values)?;
                let key = primary_key_bytes(table_schema, descriptor, &record)?;
                Ok(PendingTableWrite::Set {
                    mode: WriteMode::Update,
                    table: table.clone(),
                    key,
                    record,
                })
            }
            BatchOperation::UpdateRaw { table, key, record } => {
                self.table(table)?;
                Ok(PendingTableWrite::Set {
                    mode: WriteMode::Update,
                    table: table.clone(),
                    key: key.clone().into_bytes(),
                    record: record.clone(),
                })
            }
            BatchOperation::Delete { table, key } => {
                self.table(table)?;
                Ok(PendingTableWrite::Delete {
                    table: table.clone(),
                    key: key.clone().into_bytes(),
                })
            }
        }
    }

    fn table(&self, table: &str) -> Result<&TableSchema, Error> {
        self.ensure_not_poisoned()?;
        self.ivm_runtime
            .table(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))
    }

    fn table_descriptor(&self, table: &str) -> Result<RecordDescriptor, Error> {
        self.ensure_not_poisoned()?;
        self.ivm_runtime
            .table_descriptor(table)
            .copied()
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))
    }

    fn ensure_not_poisoned(&self) -> Result<(), Error> {
        if self.poisoned {
            Err(Error::DatabasePoisoned)
        } else {
            Ok(())
        }
    }

    fn index(&self, table: &str, index_name: &str) -> Result<&crate::schema::IndexSchema, Error> {
        self.ensure_not_poisoned()?;
        self.ivm_runtime
            .index(table, index_name)
            .ok_or_else(|| Error::IndexNotFound {
                table: table.to_owned(),
                index: index_name.to_owned(),
            })
    }

    fn direct_record_store_schema(&self, store: &str) -> Result<&DirectRecordStoreSchema, Error> {
        self.ensure_not_poisoned()?;
        self.ivm_runtime
            .direct_record_store(store)
            .ok_or_else(|| Error::DirectRecordStoreNotFound(store.to_owned()))
    }

    fn persisted_index_scan_prefix(
        &self,
        table: &str,
        index_name: &str,
        prefix: &[Value],
    ) -> Result<Vec<u8>, Error> {
        let table_schema = self.table(table)?;
        let index = self.index(table, index_name)?;
        let mut logical_key = Vec::new();
        for (value, column_name) in prefix.iter().zip(&index.columns) {
            let column = table_schema
                .columns
                .iter()
                .find(|column| column.name == *column_name)
                .ok_or_else(|| {
                    Error::InvalidPersistedIndex(format!(
                        "index {index_name} references unknown column {column_name}"
                    ))
                })?;
            encode_index_prefix_part(&mut logical_key, value, &column.column_type)?;
        }
        let mut storage_prefix = durable_index_key_prefix(table, index_name);
        if !logical_key.is_empty() {
            // Persist stores IndexBy's logical bytes as a Value::Bytes key field.
            // For prefix scans we emit the Bytes tag and escaped payload bytes
            // without the terminal 00 00, so longer non-unique keys remain in range.
            storage_prefix.push(7);
            for byte in logical_key {
                if byte == 0 {
                    storage_prefix.extend([0, 0xff]);
                } else {
                    storage_prefix.push(byte);
                }
            }
        }
        Ok(storage_prefix)
    }
}

/// Typed facade over one schema-declared direct record store.
///
/// ```
/// use groove::db::Database;
/// use groove::records::{RecordDescriptor, Value, ValueType};
/// use groove::schema::{DatabaseSchema, DirectRecordStoreSchema};
/// use groove::storage::MemoryStorage;
///
/// let schema = DatabaseSchema::new([]).with_direct_record_store(DirectRecordStoreSchema::new(
///     "album_art",
///     RecordDescriptor::new([("album_id", ValueType::U64)]),
///     RecordDescriptor::new([("bytes", ValueType::Bytes)]),
/// ));
/// let column_families = schema.column_families();
/// let storage = MemoryStorage::new(&column_families);
/// let database = Database::new(schema, storage)?;
/// let art = database.direct_record_store("album_art")?;
///
/// art.set(&[Value::U64(1)], &[Value::Bytes(b"front-cover-bytes".to_vec())])?;
/// assert_eq!(
///     art.get(&[Value::U64(1)])?.unwrap().get("bytes")?,
///     Value::Bytes(b"front-cover-bytes".to_vec())
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct DirectRecordStore<'a, S> {
    storage: &'a LayoutStorage<S>,
    name: String,
    key: RecordDescriptor,
    value: RecordDescriptor,
}

impl<S> DirectRecordStore<'_, S>
where
    S: OrderedKvStorage,
{
    /// Return the schema-declared column family name backing this direct store.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn key_descriptor(&self) -> &RecordDescriptor {
        &self.key
    }

    pub fn value_descriptor(&self) -> &RecordDescriptor {
        &self.value
    }

    pub fn set(&self, key: &[Value], value: &[Value]) -> Result<(), Error> {
        let key = self.key_bytes(key)?;
        let record = self.value.create(value)?;
        self.storage
            .write_many(&[WriteOperation::set(&self.name, &key, &record)])
            .map_err(Error::from)
    }

    pub fn get(&self, key: &[Value]) -> Result<Option<Record<'_>>, Error> {
        let key = self.key_bytes(key)?;
        Ok(self
            .record_store()
            .get_raw(&key)?
            .map(|record| self.value.bind_owned(record)))
    }

    pub fn delete(&self, key: &[Value]) -> Result<(), Error> {
        let key = self.key_bytes(key)?;
        self.storage
            .write_many(&[WriteOperation::delete(&self.name, &key)])
            .map_err(Error::from)
    }

    pub fn range(&self, start: &[Value], end: &[Value]) -> Result<Vec<Record<'_>>, Error> {
        let start = self.key_prefix_bytes(start)?;
        let end = self.key_prefix_bytes(end)?;
        self.record_store()
            .range(&start, &end)?
            .into_iter()
            .map(|(_, value)| Ok(self.value.bind_owned(value)))
            .collect()
    }

    pub fn range_entries(
        &self,
        start: &[Value],
        end: &[Value],
    ) -> Result<Vec<DirectRecordStoreEntry<'_>>, Error> {
        let start = self.key_prefix_bytes(start)?;
        let end = self.key_prefix_bytes(end)?;
        self.record_store()
            .range(&start, &end)?
            .into_iter()
            .map(|(key, value)| {
                Ok(DirectRecordStoreEntry {
                    key: self.decode_key(&key)?,
                    value: self.value.bind_owned(value),
                })
            })
            .collect()
    }

    pub fn prefix(&self, prefix: &[Value]) -> Result<Vec<Record<'_>>, Error> {
        let prefix = self.key_prefix_bytes(prefix)?;
        self.record_store()
            .prefix(&prefix)?
            .into_iter()
            .map(|(_, value)| Ok(self.value.bind_owned(value)))
            .collect()
    }

    pub fn prefix_entries(
        &self,
        prefix: &[Value],
    ) -> Result<Vec<DirectRecordStoreEntry<'_>>, Error> {
        let prefix = self.key_prefix_bytes(prefix)?;
        self.record_store()
            .prefix(&prefix)?
            .into_iter()
            .map(|(key, value)| {
                Ok(DirectRecordStoreEntry {
                    key: self.decode_key(&key)?,
                    value: self.value.bind_owned(value),
                })
            })
            .collect()
    }

    pub fn write_many(&self, operations: &[DirectRecordStoreWrite]) -> Result<(), Error> {
        let mut encoded = Vec::with_capacity(operations.len());
        for operation in operations {
            match operation {
                DirectRecordStoreWrite::Set { key, value } => {
                    encoded.push(OwnedWriteOperation::Set {
                        cf: self.name.clone(),
                        key: self.key_bytes(key)?,
                        value: self.value.create(value)?,
                    });
                }
                DirectRecordStoreWrite::Delete { key } => {
                    encoded.push(OwnedWriteOperation::Delete {
                        cf: self.name.clone(),
                        key: self.key_bytes(key)?,
                    });
                }
            }
        }
        let borrowed = encoded
            .iter()
            .map(OwnedWriteOperation::as_write_operation)
            .collect::<Vec<_>>();
        self.storage.write_many(&borrowed).map_err(Error::from)
    }

    fn key_bytes(&self, values: &[Value]) -> Result<Vec<u8>, Error> {
        if values.len() != self.key.fields().len() {
            return Err(records::Error::ArityMismatch {
                expected: self.key.fields().len(),
                actual: values.len(),
            }
            .into());
        }
        self.key_prefix_bytes(values)
    }

    fn key_prefix_bytes(&self, values: &[Value]) -> Result<Vec<u8>, Error> {
        if values.len() > self.key.fields().len() {
            return Err(records::Error::ArityMismatch {
                expected: self.key.fields().len(),
                actual: values.len(),
            }
            .into());
        }
        let prefix_descriptor =
            RecordDescriptor::new(self.key.fields().iter().take(values.len()).map(|field| {
                (
                    field.name.clone().expect("direct store fields are named"),
                    field.value_type.clone(),
                )
            }));
        let _ = prefix_descriptor.create(values)?;
        let mut bytes = Vec::new();
        for value in values {
            encode_primary_key_part(&mut bytes, value);
        }
        Ok(bytes)
    }

    fn record_store(&self) -> RecordStore<'_, LayoutStorage<S>> {
        if is_windowed_history_table(&self.name) {
            RecordStore::new_windowed(self.storage, &self.name, self.key, &self.value)
        } else {
            RecordStore::new(self.storage, &self.name, &self.value)
        }
    }

    fn decode_key(&self, key: &[u8]) -> Result<Vec<Value>, Error> {
        let mut remaining = key;
        let mut values = Vec::with_capacity(self.key.fields().len());
        for field in self.key.fields() {
            values.push(decode_primary_key_part(&mut remaining, &field.value_type)?);
        }
        if !remaining.is_empty() {
            return Err(Error::InvalidDirectRecordStoreKey(self.name.clone()));
        }
        Ok(values)
    }
}

pub struct DirectRecordStoreEntry<'a> {
    pub key: Vec<Value>,
    pub value: Record<'a>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DirectRecordStoreWrite {
    Set { key: Vec<Value>, value: Vec<Value> },
    Delete { key: Vec<Value> },
}

/// Prepared parameterized subscription shape produced from a SQL-ish query.
#[derive(Clone, Debug)]
pub struct PreparedShape {
    id: PreparedShapeId,
    parameters: Vec<QueryParameter>,
    output: RecordDescriptor,
}

impl PreparedShape {
    pub fn id(&self) -> PreparedShapeId {
        self.id
    }

    pub fn parameters(&self) -> &[QueryParameter] {
        &self.parameters
    }

    pub fn output(&self) -> &RecordDescriptor {
        &self.output
    }
}

/// Timing and write-size split for the most recent committed batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitMetrics {
    pub storage_write_time: Duration,
    pub ivm_tick_time: Duration,
    pub storage_write_count: usize,
    pub storage_write_bytes: usize,
    pub storage_writes: StorageWriteMetrics,
    pub tick: TickMetrics,
}

/// Durable storage-write counts split by stable Jazz logical destinations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageWriteMetrics {
    pub total: StorageWriteBucket,
    pub history_rows: StorageWriteBucket,
    pub history_indexes: StorageWriteBucket,
    pub global_current_rows: StorageWriteBucket,
    pub global_current_indexes: StorageWriteBucket,
    pub register_global_current_rows: StorageWriteBucket,
    pub global_changes_rows: StorageWriteBucket,
    pub global_changes_indexes: StorageWriteBucket,
    pub transactions_rows: StorageWriteBucket,
    pub transactions_indexes: StorageWriteBucket,
    pub other: StorageWriteBucket,
}

impl StorageWriteMetrics {
    fn from_operations(operations: &[crate::storage::WriteOperation<'_>]) -> Self {
        let mut metrics = Self::default();
        for operation in operations {
            metrics.record(operation);
        }
        metrics
    }

    fn record(&mut self, operation: &crate::storage::WriteOperation<'_>) {
        let bytes = write_operation_bytes(operation);
        self.total.record(bytes);
        match storage_write_destination(operation) {
            StorageWriteDestination::HistoryRows => self.history_rows.record(bytes),
            StorageWriteDestination::HistoryIndexes => self.history_indexes.record(bytes),
            StorageWriteDestination::GlobalCurrentRows => self.global_current_rows.record(bytes),
            StorageWriteDestination::GlobalCurrentIndexes => {
                self.global_current_indexes.record(bytes)
            }
            StorageWriteDestination::RegisterGlobalCurrentRows => {
                self.register_global_current_rows.record(bytes)
            }
            StorageWriteDestination::GlobalChangesRows => self.global_changes_rows.record(bytes),
            StorageWriteDestination::GlobalChangesIndexes => {
                self.global_changes_indexes.record(bytes)
            }
            StorageWriteDestination::TransactionsRows => self.transactions_rows.record(bytes),
            StorageWriteDestination::TransactionsIndexes => self.transactions_indexes.record(bytes),
            StorageWriteDestination::Other => self.other.record(bytes),
        }
    }
}

/// Count and encoded key/value bytes for one storage-write bucket.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageWriteBucket {
    pub count: usize,
    pub bytes: usize,
}

impl StorageWriteBucket {
    fn record(&mut self, bytes: usize) {
        self.count += 1;
        self.bytes += bytes;
    }
}

/// Durable storage-read counts split by stable Jazz logical destinations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageReadMetrics {
    pub total: StorageReadBucket,
    pub history_rows: StorageReadBucket,
    pub history_indexes: StorageReadBucket,
    pub global_current_rows: StorageReadBucket,
    pub global_current_indexes: StorageReadBucket,
    pub register_global_current_rows: StorageReadBucket,
    pub global_changes_rows: StorageReadBucket,
    pub global_changes_indexes: StorageReadBucket,
    pub transactions_rows: StorageReadBucket,
    pub transactions_indexes: StorageReadBucket,
    pub other: StorageReadBucket,
}

impl StorageReadMetrics {
    fn record_point(&mut self, cf: &str, key: &[u8]) {
        self.record_destination(storage_read_destination(cf, key), 1, 1);
    }

    fn record_range(&mut self, cf: &str, key: &[u8]) {
        self.record_destination(storage_read_destination(cf, key), 0, 1);
    }

    fn record_range_row(&mut self, cf: &str, key: &[u8]) {
        self.record_destination(storage_read_destination(cf, key), 1, 0);
    }

    fn record_destination(
        &mut self,
        destination: StorageReadDestination,
        reads: usize,
        ranges: usize,
    ) {
        self.total.record(reads, ranges);
        match destination {
            StorageReadDestination::HistoryRows => self.history_rows.record(reads, ranges),
            StorageReadDestination::HistoryIndexes => self.history_indexes.record(reads, ranges),
            StorageReadDestination::GlobalCurrentRows => {
                self.global_current_rows.record(reads, ranges)
            }
            StorageReadDestination::GlobalCurrentIndexes => {
                self.global_current_indexes.record(reads, ranges)
            }
            StorageReadDestination::RegisterGlobalCurrentRows => {
                self.register_global_current_rows.record(reads, ranges)
            }
            StorageReadDestination::GlobalChangesRows => {
                self.global_changes_rows.record(reads, ranges)
            }
            StorageReadDestination::GlobalChangesIndexes => {
                self.global_changes_indexes.record(reads, ranges)
            }
            StorageReadDestination::TransactionsRows => {
                self.transactions_rows.record(reads, ranges)
            }
            StorageReadDestination::TransactionsIndexes => {
                self.transactions_indexes.record(reads, ranges)
            }
            StorageReadDestination::Other => self.other.record(reads, ranges),
        }
    }
}

/// Count of logical storage records read and logical key ranges touched.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageReadBucket {
    pub reads: usize,
    pub ranges: usize,
}

impl StorageReadBucket {
    fn record(&mut self, reads: usize, ranges: usize) {
        self.reads += reads;
        self.ranges += ranges;
    }
}

pub(crate) struct MeteredStorage<'a, S> {
    storage: &'a S,
    metrics: &'a RefCell<StorageReadMetrics>,
}

impl<'a, S> MeteredStorage<'a, S> {
    pub(crate) fn new(storage: &'a S, metrics: &'a RefCell<StorageReadMetrics>) -> Self {
        Self { storage, metrics }
    }
}

impl<S> OrderedKvStorage for MeteredStorage<'_, S>
where
    S: OrderedKvStorage,
{
    fn get(
        &self,
        cf: &crate::storage::ColumnFamilyName,
        key: &crate::storage::Key,
    ) -> Result<Option<crate::storage::Value>, crate::storage::Error> {
        self.metrics.borrow_mut().record_point(cf, key);
        self.storage.get(cf, key)
    }

    fn set(
        &self,
        cf: &crate::storage::ColumnFamilyName,
        key: &crate::storage::Key,
        value: &[u8],
    ) -> Result<(), crate::storage::Error> {
        self.storage.set(cf, key, value)
    }

    fn delete(
        &self,
        cf: &crate::storage::ColumnFamilyName,
        key: &crate::storage::Key,
    ) -> Result<(), crate::storage::Error> {
        self.storage.delete(cf, key)
    }

    fn scan_range(
        &self,
        cf: &crate::storage::ColumnFamilyName,
        start: &crate::storage::Key,
        end: &crate::storage::Key,
        visit: &mut crate::storage::ScanVisitor<'_>,
    ) -> Result<(), crate::storage::Error> {
        self.metrics.borrow_mut().record_range(cf, start);
        self.storage.scan_range(cf, start, end, &mut |key, value| {
            self.metrics.borrow_mut().record_range_row(cf, key);
            visit(key, value)
        })
    }

    fn scan_prefix(
        &self,
        cf: &crate::storage::ColumnFamilyName,
        prefix: &crate::storage::Key,
        visit: &mut crate::storage::ScanVisitor<'_>,
    ) -> Result<(), crate::storage::Error> {
        self.metrics.borrow_mut().record_range(cf, prefix);
        self.storage.scan_prefix(cf, prefix, &mut |key, value| {
            self.metrics.borrow_mut().record_range_row(cf, key);
            visit(key, value)
        })
    }

    fn scan_prefix_reverse(
        &self,
        cf: &crate::storage::ColumnFamilyName,
        prefix: &crate::storage::Key,
        visit: &mut crate::storage::ScanVisitor<'_>,
    ) -> Result<(), crate::storage::Error> {
        self.metrics.borrow_mut().record_range(cf, prefix);
        self.storage
            .scan_prefix_reverse(cf, prefix, &mut |key, value| {
                self.metrics.borrow_mut().record_range_row(cf, key);
                visit(key, value)
            })
    }

    fn last_with_prefix(
        &self,
        cf: &crate::storage::ColumnFamilyName,
        prefix: &crate::storage::Key,
    ) -> Result<Option<crate::storage::KeyValue>, crate::storage::Error> {
        self.metrics.borrow_mut().record_range(cf, prefix);
        let value = self.storage.last_with_prefix(cf, prefix)?;
        if let Some((key, _)) = &value {
            self.metrics.borrow_mut().record_range_row(cf, key);
        }
        Ok(value)
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: &crate::storage::ColumnFamilyName,
        prefix: &crate::storage::Key,
        upper: &crate::storage::Key,
    ) -> Result<Option<crate::storage::KeyValue>, crate::storage::Error> {
        self.metrics.borrow_mut().record_range(cf, prefix);
        let value = self
            .storage
            .last_with_prefix_before_or_at(cf, prefix, upper)?;
        if let Some((key, _)) = &value {
            self.metrics.borrow_mut().record_range_row(cf, key);
        }
        Ok(value)
    }

    fn write_many(
        &self,
        operations: &[crate::storage::WriteOperation<'_>],
    ) -> Result<(), crate::storage::Error> {
        self.storage.write_many(operations)
    }
}

/// Owned raw database entry with a lazy encoded-record view over the value.
#[derive(Clone, Debug)]
pub struct EncodedKeyValue<'a> {
    key: Vec<u8>,
    value: Vec<u8>,
    descriptor: &'a RecordDescriptor,
}

impl<'a> EncodedKeyValue<'a> {
    pub fn new(key: Vec<u8>, value: Vec<u8>, descriptor: &'a RecordDescriptor) -> Self {
        Self {
            key,
            value,
            descriptor,
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn raw(&self) -> &[u8] {
        &self.value
    }

    pub fn into_parts(self) -> (Vec<u8>, Vec<u8>) {
        (self.key, self.value)
    }

    pub fn record(&self) -> BorrowedRecord<'_> {
        BorrowedRecord::new(&self.value, self.descriptor)
    }

    pub fn owned_record(self) -> OwnedRecord {
        OwnedRecord::new(self.value, *self.descriptor)
    }
}

fn write_operation_bytes(operation: &crate::storage::WriteOperation<'_>) -> usize {
    match operation {
        crate::storage::WriteOperation::Set { key, value, .. } => key.len() + value.len(),
        crate::storage::WriteOperation::Delete { key, .. } => key.len(),
        crate::storage::WriteOperation::Delta { key, delta, .. } => key.len() + delta.payload.len(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StorageWriteDestination {
    HistoryRows,
    HistoryIndexes,
    GlobalCurrentRows,
    GlobalCurrentIndexes,
    RegisterGlobalCurrentRows,
    GlobalChangesRows,
    GlobalChangesIndexes,
    TransactionsRows,
    TransactionsIndexes,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StorageReadDestination {
    HistoryRows,
    HistoryIndexes,
    GlobalCurrentRows,
    GlobalCurrentIndexes,
    RegisterGlobalCurrentRows,
    GlobalChangesRows,
    GlobalChangesIndexes,
    TransactionsRows,
    TransactionsIndexes,
    Other,
}

fn storage_write_destination(
    operation: &crate::storage::WriteOperation<'_>,
) -> StorageWriteDestination {
    match operation {
        crate::storage::WriteOperation::Set { cf, key, .. }
        | crate::storage::WriteOperation::Delete { cf, key }
        | crate::storage::WriteOperation::Delta { cf, key, .. } => {
            if *cf == "indices" {
                storage_index_write_destination(key)
            } else {
                storage_table_write_destination(cf)
            }
        }
    }
}

fn storage_table_write_destination(table: &str) -> StorageWriteDestination {
    if table == "jazz_global_changes" {
        StorageWriteDestination::GlobalChangesRows
    } else if table == "jazz_transactions" {
        StorageWriteDestination::TransactionsRows
    } else if table.starts_with("jazz_")
        && table.ends_with("_register_global_current")
        && !table.contains("_ahead_current")
    {
        StorageWriteDestination::RegisterGlobalCurrentRows
    } else if table.starts_with("jazz_")
        && table.ends_with("_global_current")
        && !table.contains("_register_global_current")
        && !table.contains("_ahead_current")
    {
        StorageWriteDestination::GlobalCurrentRows
    } else if table.starts_with("jazz_") && table.ends_with("_history") {
        StorageWriteDestination::HistoryRows
    } else {
        StorageWriteDestination::Other
    }
}

fn storage_index_write_destination(key: &[u8]) -> StorageWriteDestination {
    let Some((table, index)) = durable_index_table_and_name(key) else {
        return StorageWriteDestination::Other;
    };
    if table == "jazz_global_changes"
        && (index == "by_global_seq" || index == "by_table_global_seq")
    {
        StorageWriteDestination::GlobalChangesIndexes
    } else if table == "jazz_transactions" && index == "by_global_seq" {
        StorageWriteDestination::TransactionsIndexes
    } else if table.starts_with("jazz_")
        && table.ends_with("_global_current")
        && !table.contains("_register_global_current")
        && index.starts_with("by_user_")
    {
        StorageWriteDestination::GlobalCurrentIndexes
    } else if table.starts_with("jazz_") && table.ends_with("_history") && index == "by_tx" {
        StorageWriteDestination::HistoryIndexes
    } else {
        StorageWriteDestination::Other
    }
}

fn storage_read_destination(cf: &str, key: &[u8]) -> StorageReadDestination {
    if cf == "indices" {
        storage_index_read_destination(key)
    } else {
        storage_table_read_destination(cf)
    }
}

fn storage_table_read_destination(table: &str) -> StorageReadDestination {
    match storage_table_write_destination(table) {
        StorageWriteDestination::HistoryRows => StorageReadDestination::HistoryRows,
        StorageWriteDestination::GlobalCurrentRows => StorageReadDestination::GlobalCurrentRows,
        StorageWriteDestination::RegisterGlobalCurrentRows => {
            StorageReadDestination::RegisterGlobalCurrentRows
        }
        StorageWriteDestination::GlobalChangesRows => StorageReadDestination::GlobalChangesRows,
        StorageWriteDestination::TransactionsRows => StorageReadDestination::TransactionsRows,
        _ => StorageReadDestination::Other,
    }
}

fn storage_index_read_destination(key: &[u8]) -> StorageReadDestination {
    match storage_index_write_destination(key) {
        StorageWriteDestination::HistoryIndexes => StorageReadDestination::HistoryIndexes,
        StorageWriteDestination::GlobalCurrentIndexes => {
            StorageReadDestination::GlobalCurrentIndexes
        }
        StorageWriteDestination::GlobalChangesIndexes => {
            StorageReadDestination::GlobalChangesIndexes
        }
        StorageWriteDestination::TransactionsIndexes => StorageReadDestination::TransactionsIndexes,
        _ => StorageReadDestination::Other,
    }
}

fn durable_index_table_and_name(key: &[u8]) -> Option<(&str, &str)> {
    let table_end = key.iter().position(|byte| *byte == 0)?;
    let rest = key.get(table_end + 1..)?;
    let index_end = rest.iter().position(|byte| *byte == 0)?;
    let table = str::from_utf8(&key[..table_end]).ok()?;
    let index = str::from_utf8(&rest[..index_end]).ok()?;
    Some((table, index))
}

fn owned_write_operation(operation: &crate::storage::WriteOperation<'_>) -> OwnedWriteOperation {
    match operation {
        crate::storage::WriteOperation::Set { cf, key, value } => OwnedWriteOperation::Set {
            cf: (*cf).to_owned(),
            key: (*key).to_vec(),
            value: (*value).to_vec(),
        },
        crate::storage::WriteOperation::Delete { cf, key } => OwnedWriteOperation::Delete {
            cf: (*cf).to_owned(),
            key: (*key).to_vec(),
        },
        crate::storage::WriteOperation::Delta { cf, key, delta } => OwnedWriteOperation::Delta {
            cf: (*cf).to_owned(),
            key: (*key).to_vec(),
            delta: (*delta).clone(),
        },
    }
}

enum PendingTableWrite {
    /// Insert and update share the same storage operation after validation.
    /// Delta computation decides whether an old record must be retracted first.
    Set {
        mode: WriteMode,
        table: String,
        key: Vec<u8>,
        record: Vec<u8>,
    },
    Delete {
        table: String,
        key: Vec<u8>,
    },
}

#[derive(Clone, Copy)]
enum WriteMode {
    Insert,
    InsertFresh,
    Update,
}

impl PendingTableWrite {
    fn table(&self) -> &str {
        match self {
            Self::Set { table, .. } | Self::Delete { table, .. } => table,
        }
    }

    fn key(&self) -> &[u8] {
        match self {
            Self::Set { key, .. } | Self::Delete { key, .. } => key,
        }
    }

    fn delta_from_current(
        &self,
        descriptor: RecordDescriptor,
        current: Option<Vec<u8>>,
    ) -> TableDelta {
        let deltas = match self {
            Self::Set { record, .. } => {
                let mut deltas = current
                    .into_iter()
                    .map(|record| RecordDelta {
                        record: record.into(),
                        weight: -1,
                    })
                    .collect::<Vec<_>>();
                deltas.push(RecordDelta {
                    record: record.clone().into(),
                    weight: 1,
                });
                deltas
            }
            Self::Delete { .. } => current
                .into_iter()
                .map(|record| RecordDelta {
                    record: record.into(),
                    weight: -1,
                })
                .collect(),
        };

        TableDelta {
            table: self.table().to_owned(),
            descriptor,
            deltas,
        }
    }
}

fn compute_table_deltas<S>(
    pending_writes: &[PendingTableWrite],
    stores: &[RecordStore<'_, S>],
) -> Result<Vec<TableDelta>, Error>
where
    S: OrderedKvStorage,
{
    // Reads see earlier writes in the same batch through this overlay. Without
    // it, same-key insert/update/delete sequences emit deltas against stale
    // pre-batch storage and corrupt maintained views.
    let mut overlay = HashMap::<(String, Vec<u8>), Option<Vec<u8>>>::new();
    let mut table_deltas = Vec::with_capacity(pending_writes.len());

    for (write, store) in pending_writes.iter().zip(stores) {
        let overlay_key = (write.table().to_owned(), write.key().to_vec());
        let current = if let Some(record) = overlay.get(&overlay_key) {
            record.clone()
        } else if matches!(
            write,
            PendingTableWrite::Set {
                mode: WriteMode::InsertFresh,
                ..
            }
        ) {
            None
        } else {
            store.get_raw(write.key())?
        };
        if matches!(
            write,
            PendingTableWrite::Set {
                mode: WriteMode::Insert,
                ..
            }
        ) && current.is_some()
        {
            return Err(Error::DuplicatePrimaryKey {
                table: write.table().to_owned(),
                key: write.key().to_vec(),
            });
        }
        table_deltas.push(write.delta_from_current(*store.descriptor(), current.clone()));
        let next = match write {
            PendingTableWrite::Set { record, .. } => Some(record.clone()),
            PendingTableWrite::Delete { .. } => None,
        };
        overlay.insert(overlay_key, next);
    }

    Ok(consolidate_table_deltas(table_deltas))
}

fn record_store_for_table<'a, S>(
    storage: &'a S,
    table: &'a str,
    key_descriptor: Option<RecordDescriptor>,
    descriptor: &'a RecordDescriptor,
) -> RecordStore<'a, S>
where
    S: OrderedKvStorage,
{
    if is_windowed_history_table(table)
        && let Some(key_descriptor) = key_descriptor
    {
        RecordStore::new_windowed(storage, table, key_descriptor, descriptor)
    } else {
        RecordStore::new(storage, table, descriptor)
    }
}

fn primary_key_descriptor(primary_key: &PrimaryKey) -> RecordDescriptor {
    RecordDescriptor::new(primary_key.columns.iter().map(|column| {
        (
            column.column.clone(),
            column.key_type.column_type().value_type(),
        )
    }))
}

fn consolidate_table_deltas(table_deltas: Vec<TableDelta>) -> Vec<TableDelta> {
    let mut by_table = HashMap::<String, (RecordDescriptor, HashMap<bytes::Bytes, i64>)>::new();
    for table_delta in table_deltas {
        let (_, records) = by_table
            .entry(table_delta.table)
            .or_insert_with(|| (table_delta.descriptor, HashMap::new()));
        for delta in table_delta.deltas {
            *records.entry(delta.record).or_default() += delta.weight;
        }
    }
    by_table
        .into_iter()
        .filter_map(|(table, (descriptor, records))| {
            let deltas = records
                .into_iter()
                .filter_map(|(record, weight)| {
                    (weight != 0).then_some(RecordDelta { record, weight })
                })
                .collect::<Vec<_>>();
            (!deltas.is_empty()).then_some(TableDelta {
                table,
                descriptor,
                deltas,
            })
        })
        .collect()
}

/// Mutable staged table writes whose reads observe writes already added to the
/// stage. Commit runs one normal database batch commit, so current callers of
/// [`Database::commit_batch`] and staged callers share the final tick/write path.
pub struct StagedDatabaseBatch<'a, S>
where
    S: OrderedKvStorage,
{
    database: &'a mut Database<S>,
    batch: DatabaseBatch,
}

impl<S> StagedDatabaseBatch<'_, S>
where
    S: OrderedKvStorage,
{
    pub fn reserve(&mut self, additional: usize) {
        self.batch.reserve(additional);
    }

    pub fn insert(&mut self, table: impl Into<String>, values: Vec<Value>) {
        self.batch.insert(table, values);
    }

    pub fn insert_raw(&mut self, table: impl Into<String>, key: PrimaryKeyValue, record: Vec<u8>) {
        self.batch.insert_raw(table, key, record);
    }

    pub fn update(&mut self, table: impl Into<String>, values: Vec<Value>) {
        self.batch.update(table, values);
    }

    pub fn update_raw(&mut self, table: impl Into<String>, key: PrimaryKeyValue, record: Vec<u8>) {
        self.batch.update_raw(table, key, record);
    }

    pub fn delete(&mut self, table: impl Into<String>, key: PrimaryKeyValue) {
        self.batch.delete(table, key);
    }

    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    pub fn primary_key_scan(
        &self,
        table: &str,
        prefix: &[Value],
    ) -> Result<Vec<Record<'_>>, Error> {
        self.database.ensure_batch_storage_txn(&self.batch)?;
        let overlay = StagedWriteOverlay::new(&self.database.storage, &self.batch.txn_operations);
        let storage = MeteredStorage::new(&overlay, &self.database.storage_read_metrics);
        self.database
            .primary_key_scan_with_storage(&storage, table, prefix)
    }

    pub fn primary_key_scan_raw(
        &self,
        table: &str,
        prefix: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'_>>, Error> {
        self.database.ensure_batch_storage_txn(&self.batch)?;
        let overlay = StagedWriteOverlay::new(&self.database.storage, &self.batch.txn_operations);
        let storage = MeteredStorage::new(&overlay, &self.database.storage_read_metrics);
        self.database
            .primary_key_scan_raw_with_storage(&storage, table, prefix)
    }

    pub fn primary_key_last_raw(
        &self,
        table: &str,
        prefix: &[Value],
    ) -> Result<Option<EncodedKeyValue<'_>>, Error> {
        self.database.ensure_batch_storage_txn(&self.batch)?;
        let overlay = StagedWriteOverlay::new(&self.database.storage, &self.batch.txn_operations);
        let storage = MeteredStorage::new(&overlay, &self.database.storage_read_metrics);
        self.database
            .primary_key_last_raw_with_storage(&storage, table, prefix)
    }

    pub fn commit(self) -> Result<(), Error> {
        self.database.commit_batch(self.batch)
    }
}

/// Mutable collection of table writes committed atomically at storage level.
#[derive(Clone, Debug, Default)]
pub struct DatabaseBatch {
    operations: Vec<BatchOperation>,
    txn_operations: RefCell<StagedWriteState>,
    txn_indexed_operations: Cell<usize>,
}

impl PartialEq for DatabaseBatch {
    fn eq(&self, other: &Self) -> bool {
        self.operations == other.operations
    }
}

impl DatabaseBatch {
    pub fn reserve(&mut self, additional: usize) {
        self.operations.reserve(additional);
    }

    pub fn insert(&mut self, table: impl Into<String>, values: Vec<Value>) {
        self.push_operation(BatchOperation::Insert {
            table: table.into(),
            values,
        });
    }

    pub fn insert_raw(&mut self, table: impl Into<String>, key: PrimaryKeyValue, record: Vec<u8>) {
        self.push_operation(BatchOperation::InsertRaw {
            table: table.into(),
            key,
            record,
        });
    }

    /// Stage a raw insert whose caller has already proven that the key is absent.
    ///
    /// This avoids a storage lookup during delta computation. It is only sound for
    /// internal append-only tables whose enclosing transaction identity proves
    /// freshness; ordinary insert callers must use [`Self::insert_raw`].
    pub fn insert_raw_fresh(
        &mut self,
        table: impl Into<String>,
        key: PrimaryKeyValue,
        record: Vec<u8>,
    ) {
        self.push_operation(BatchOperation::InsertRawFresh {
            table: table.into(),
            key,
            record,
        });
    }

    pub fn update(&mut self, table: impl Into<String>, values: Vec<Value>) {
        self.push_operation(BatchOperation::Update {
            table: table.into(),
            values,
        });
    }

    pub fn update_raw(&mut self, table: impl Into<String>, key: PrimaryKeyValue, record: Vec<u8>) {
        self.push_operation(BatchOperation::UpdateRaw {
            table: table.into(),
            key,
            record,
        });
    }

    pub fn delete(&mut self, table: impl Into<String>, key: PrimaryKeyValue) {
        self.push_operation(BatchOperation::Delete {
            table: table.into(),
            key,
        });
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    fn push_operation(&mut self, operation: BatchOperation) {
        self.operations.push(operation);
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum BatchOperation {
    Insert {
        table: String,
        values: Vec<Value>,
    },
    InsertRaw {
        table: String,
        key: PrimaryKeyValue,
        record: Vec<u8>,
    },
    InsertRawFresh {
        table: String,
        key: PrimaryKeyValue,
        record: Vec<u8>,
    },
    Update {
        table: String,
        values: Vec<Value>,
    },
    UpdateRaw {
        table: String,
        key: PrimaryKeyValue,
        record: Vec<u8>,
    },
    Delete {
        table: String,
        key: PrimaryKeyValue,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrimaryKeyValue {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
    Composite(Vec<PrimaryKeyValue>),
}

impl PrimaryKeyValue {
    fn into_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::new();
        match self {
            Self::U8(value) => encode_primary_key_part(&mut bytes, &Value::U8(value)),
            Self::U16(value) => encode_primary_key_part(&mut bytes, &Value::U16(value)),
            Self::U32(value) => encode_primary_key_part(&mut bytes, &Value::U32(value)),
            Self::U64(value) => encode_primary_key_part(&mut bytes, &Value::U64(value)),
            Self::Bool(value) => encode_primary_key_part(&mut bytes, &Value::Bool(value)),
            Self::String(value) => encode_primary_key_part(&mut bytes, &Value::String(value)),
            Self::Bytes(value) => encode_primary_key_part(&mut bytes, &Value::Bytes(value)),
            Self::Uuid(value) => encode_primary_key_part(&mut bytes, &Value::Uuid(value)),
            Self::Composite(values) => {
                for value in values {
                    bytes.extend(value.into_bytes());
                }
            }
        }
        bytes
    }
}

fn encode_record(
    table: &TableSchema,
    descriptor: RecordDescriptor,
    values: &[Value],
) -> Result<Vec<u8>, Error> {
    if table.columns.len() != values.len() {
        return Err(records::Error::ArityMismatch {
            expected: table.columns.len(),
            actual: values.len(),
        }
        .into());
    }
    // Callers provide values in SQL declaration order. RecordDescriptor stores
    // fixed-width fields first, so we reorder here before positional encoding.
    let values_by_descriptor_order = descriptor
        .fields()
        .iter()
        .map(|field| {
            let name = field
                .name
                .as_deref()
                .ok_or(records::Error::FieldNotFound("<unnamed>".to_owned()))?;
            let declaration_idx = table
                .columns
                .iter()
                .position(|column| column.name == name)
                .ok_or_else(|| records::Error::FieldNotFound(name.to_owned()))?;
            values
                .get(declaration_idx)
                .cloned()
                .ok_or(records::Error::ArityMismatch {
                    expected: table.columns.len(),
                    actual: values.len(),
                })
        })
        .collect::<Result<Vec<_>, records::Error>>()?;
    Ok(descriptor.create(&values_by_descriptor_order)?)
}

fn primary_key_bytes(
    table: &TableSchema,
    record_schema: RecordDescriptor,
    record: &[u8],
) -> Result<Vec<u8>, Error> {
    let primary_key = table
        .primary_key
        .as_ref()
        .ok_or_else(|| Error::MissingPrimaryKey(table.name.clone()))?;

    let mut bytes = Vec::new();
    for column in &primary_key.columns {
        let value = record_schema.get(record, &column.column)?;
        ensure_primary_key_value_type(table, column, &value)?;
        encode_primary_key_part(&mut bytes, &value);
    }
    Ok(bytes)
}

fn persisted_index_primary_key(
    table: &TableSchema,
    index_name: &str,
    index: &IndexSchema,
    storage_key: &[u8],
    stored_value: &Value,
) -> Result<Vec<u8>, Error> {
    let logical_key = persisted_index_logical_key(table, index_name, storage_key)?;
    if index_key_covers_primary_key(table, index)? {
        return primary_key_from_index_columns(table, index_name, index, &logical_key);
    }
    if let Some(primary_key) =
        primary_key_from_appended_index_suffix(table, index_name, index, &logical_key)?
    {
        return Ok(primary_key);
    }
    let Value::Bytes(primary_key) = stored_value else {
        return Err(Error::InvalidPersistedIndex(index_name.to_owned()));
    };
    validate_primary_key_bytes(table, index_name, primary_key)?;
    Ok(primary_key.clone())
}

fn persisted_index_logical_key(
    table: &TableSchema,
    index_name: &str,
    storage_key: &[u8],
) -> Result<Vec<u8>, Error> {
    let prefix = durable_index_key_prefix(&table.name, index_name);
    let mut remaining = storage_key
        .strip_prefix(prefix.as_slice())
        .ok_or_else(|| Error::InvalidPersistedIndex(index_name.to_owned()))?;
    expect_persisted_index_key_tag(&mut remaining, index_name, 7)?;
    let logical_key = decode_persisted_index_ordered_bytes(&mut remaining, index_name)?;
    if !remaining.is_empty() {
        return Err(Error::InvalidPersistedIndex(index_name.to_owned()));
    }
    Ok(logical_key)
}

fn index_key_covers_primary_key(table: &TableSchema, index: &IndexSchema) -> Result<bool, Error> {
    let primary_key = table
        .primary_key
        .as_ref()
        .ok_or_else(|| Error::MissingPrimaryKey(table.name.clone()))?;
    Ok(primary_key
        .columns
        .iter()
        .all(|primary_key_column| index.columns.contains(&primary_key_column.column)))
}

fn primary_key_from_index_columns(
    table: &TableSchema,
    index_name: &str,
    index: &IndexSchema,
    logical_key: &[u8],
) -> Result<Vec<u8>, Error> {
    let primary_key = table
        .primary_key
        .as_ref()
        .ok_or_else(|| Error::MissingPrimaryKey(table.name.clone()))?;
    let mut remaining = logical_key;
    let mut index_values = Vec::with_capacity(index.columns.len());
    for column_name in &index.columns {
        let column = table
            .columns
            .iter()
            .find(|column| column.name == *column_name)
            .ok_or_else(|| Error::InvalidPersistedIndex(index_name.to_owned()))?;
        index_values.push(decode_index_key_part(
            &mut remaining,
            &column.column_type,
            index_name,
        )?);
    }
    if !remaining.is_empty() {
        return Err(Error::InvalidPersistedIndex(index_name.to_owned()));
    }

    let mut bytes = Vec::new();
    for primary_key_column in &primary_key.columns {
        let index_position = index
            .columns
            .iter()
            .position(|column| column == &primary_key_column.column)
            .ok_or_else(|| Error::InvalidPersistedIndex(index_name.to_owned()))?;
        let value = index_values
            .get(index_position)
            .ok_or_else(|| Error::InvalidPersistedIndex(index_name.to_owned()))?;
        ensure_primary_key_value_type(table, primary_key_column, value)?;
        encode_primary_key_part(&mut bytes, value);
    }
    Ok(bytes)
}

fn primary_key_from_appended_index_suffix(
    table: &TableSchema,
    index_name: &str,
    index: &IndexSchema,
    logical_key: &[u8],
) -> Result<Option<Vec<u8>>, Error> {
    let mut remaining = logical_key;
    for column_name in &index.columns {
        let column = table
            .columns
            .iter()
            .find(|column| column.name == *column_name)
            .ok_or_else(|| Error::InvalidPersistedIndex(index_name.to_owned()))?;
        let _ = decode_index_key_part(&mut remaining, &column.column_type, index_name)?;
    }
    if remaining.first() != Some(&0xff) {
        return Ok(None);
    }
    let primary_key = remaining[1..].to_vec();
    validate_primary_key_bytes(table, index_name, &primary_key)?;
    Ok(Some(primary_key))
}

fn validate_primary_key_bytes(
    table: &TableSchema,
    index_name: &str,
    primary_key: &[u8],
) -> Result<(), Error> {
    let table_primary_key = table
        .primary_key
        .as_ref()
        .ok_or_else(|| Error::MissingPrimaryKey(table.name.clone()))?;
    let mut remaining = primary_key;
    for column in &table_primary_key.columns {
        decode_primary_key_part(&mut remaining, &column.key_type.column_type().value_type())
            .map_err(|_| Error::InvalidPersistedIndex(index_name.to_owned()))?;
    }
    if !remaining.is_empty() {
        return Err(Error::InvalidPersistedIndex(index_name.to_owned()));
    }
    Ok(())
}

fn ensure_primary_key_value_type(
    table: &TableSchema,
    column: &PrimaryKeyColumn,
    value: &Value,
) -> Result<(), Error> {
    match (&column.key_type, value) {
        (PrimaryKeyType::Integer(IntegerKeyType::U8), Value::U8(_))
        | (PrimaryKeyType::Integer(IntegerKeyType::U16), Value::U16(_))
        | (PrimaryKeyType::Integer(IntegerKeyType::U32), Value::U32(_))
        | (PrimaryKeyType::Integer(IntegerKeyType::U64), Value::U64(_))
        | (PrimaryKeyType::Bool, Value::Bool(_))
        | (PrimaryKeyType::String, Value::String(_))
        | (PrimaryKeyType::Bytes, Value::Bytes(_))
        | (PrimaryKeyType::Uuid, Value::Uuid(_)) => Ok(()),
        _ => Err(Error::PrimaryKeyTypeMismatch {
            table: table.name.clone(),
            column: column.column.clone(),
        }),
    }
}

fn encode_primary_key_part(key: &mut Vec<u8>, value: &Value) {
    match value {
        Value::U8(value) => {
            key.push(0);
            key.push(*value);
        }
        Value::U16(value) => {
            key.push(1);
            key.extend(value.to_be_bytes());
        }
        Value::U32(value) => {
            key.push(2);
            key.extend(value.to_be_bytes());
        }
        Value::U64(value) => {
            key.push(3);
            key.extend(value.to_be_bytes());
        }
        Value::I64(value) => {
            key.push(13);
            key.extend(order_preserving_i64_bits(*value).to_be_bytes());
        }
        Value::Bool(value) => {
            key.push(5);
            key.push(u8::from(*value));
        }
        Value::String(value) => {
            key.push(6);
            encode_ordered_bytes(key, value.as_bytes());
        }
        Value::Enum(value) => {
            key.push(0);
            key.push(*value);
        }
        Value::Bytes(value) => {
            key.push(7);
            encode_ordered_bytes(key, value);
        }
        Value::Uuid(value) => {
            key.push(10);
            key.extend_from_slice(value.as_bytes());
        }
        Value::Tuple(values) => {
            key.push(11);
            for value in values {
                encode_primary_key_part(key, value);
            }
        }
        Value::F64(_) | Value::Array(_) | Value::Nullable(_) => {
            unreachable!("unsupported primary-key value type was validated before encoding")
        }
    }
}

fn encode_ordered_bytes(key: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            key.extend([0, 0xff]);
        } else {
            key.push(*byte);
        }
    }
    key.extend([0, 0]);
}

fn order_preserving_i64_bits(value: i64) -> u64 {
    (value as u64) ^ (1_u64 << 63)
}

fn decode_primary_key_part(
    bytes: &mut &[u8],
    value_type: &records::ValueType,
) -> Result<Value, Error> {
    match value_type {
        records::ValueType::U8 => {
            expect_key_tag(bytes, 0)?;
            let value = take_key_bytes(bytes, 1)?[0];
            Ok(Value::U8(value))
        }
        records::ValueType::U16 => {
            expect_key_tag(bytes, 1)?;
            let value = u16::from_be_bytes(
                take_key_bytes(bytes, 2)?
                    .try_into()
                    .expect("slice has u16 length"),
            );
            Ok(Value::U16(value))
        }
        records::ValueType::U32 => {
            expect_key_tag(bytes, 2)?;
            let value = u32::from_be_bytes(
                take_key_bytes(bytes, 4)?
                    .try_into()
                    .expect("slice has u32 length"),
            );
            Ok(Value::U32(value))
        }
        records::ValueType::U64 => {
            expect_key_tag(bytes, 3)?;
            let value = u64::from_be_bytes(
                take_key_bytes(bytes, 8)?
                    .try_into()
                    .expect("slice has u64 length"),
            );
            Ok(Value::U64(value))
        }
        records::ValueType::I64 => {
            expect_key_tag(bytes, 13)?;
            let value = u64::from_be_bytes(
                take_key_bytes(bytes, 8)?
                    .try_into()
                    .expect("slice has i64 length"),
            );
            Ok(Value::I64((value ^ (1_u64 << 63)) as i64))
        }
        records::ValueType::Bool => {
            expect_key_tag(bytes, 5)?;
            match take_key_bytes(bytes, 1)?[0] {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                _ => Err(Error::InvalidDirectRecordStoreKey("bool".to_owned())),
            }
        }
        records::ValueType::String => {
            expect_key_tag(bytes, 6)?;
            let value = decode_ordered_bytes(bytes)?;
            Ok(Value::String(String::from_utf8(value).map_err(|_| {
                Error::InvalidDirectRecordStoreKey("string".to_owned())
            })?))
        }
        records::ValueType::Bytes => {
            expect_key_tag(bytes, 7)?;
            Ok(Value::Bytes(decode_ordered_bytes(bytes)?))
        }
        records::ValueType::Uuid => {
            expect_key_tag(bytes, 10)?;
            let value = uuid::Uuid::from_bytes(
                take_key_bytes(bytes, 16)?
                    .try_into()
                    .expect("slice has uuid length"),
            );
            Ok(Value::Uuid(value))
        }
        records::ValueType::Enum(_) => {
            expect_key_tag(bytes, 0)?;
            let value = take_key_bytes(bytes, 1)?[0];
            Ok(Value::Enum(value))
        }
        records::ValueType::F64
        | records::ValueType::Array(_)
        | records::ValueType::Nullable(_)
        | records::ValueType::Tuple(_) => Err(Error::InvalidDirectRecordStoreKey(
            "unsupported direct record store key type".to_owned(),
        )),
    }
}

fn decode_index_key_part(
    bytes: &mut &[u8],
    column_type: &ColumnType,
    index_name: &str,
) -> Result<Value, Error> {
    match column_type {
        ColumnType::U8 => {
            expect_persisted_index_key_tag(bytes, index_name, 0)?;
            Ok(Value::U8(
                take_persisted_index_key_bytes(bytes, index_name, 1)?[0],
            ))
        }
        ColumnType::U16 => {
            expect_persisted_index_key_tag(bytes, index_name, 1)?;
            Ok(Value::U16(u16::from_be_bytes(
                take_persisted_index_key_bytes(bytes, index_name, 2)?
                    .try_into()
                    .expect("slice has u16 length"),
            )))
        }
        ColumnType::U32 => {
            expect_persisted_index_key_tag(bytes, index_name, 2)?;
            Ok(Value::U32(u32::from_be_bytes(
                take_persisted_index_key_bytes(bytes, index_name, 4)?
                    .try_into()
                    .expect("slice has u32 length"),
            )))
        }
        ColumnType::U64 => {
            expect_persisted_index_key_tag(bytes, index_name, 3)?;
            Ok(Value::U64(u64::from_be_bytes(
                take_persisted_index_key_bytes(bytes, index_name, 8)?
                    .try_into()
                    .expect("slice has u64 length"),
            )))
        }
        ColumnType::I64 => {
            expect_persisted_index_key_tag(bytes, index_name, 13)?;
            Ok(Value::I64(
                (u64::from_be_bytes(
                    take_persisted_index_key_bytes(bytes, index_name, 8)?
                        .try_into()
                        .expect("slice has i64 length"),
                ) ^ (1_u64 << 63)) as i64,
            ))
        }
        ColumnType::F64 => {
            expect_persisted_index_key_tag(bytes, index_name, 4)?;
            let ordered = u64::from_be_bytes(
                take_persisted_index_key_bytes(bytes, index_name, 8)?
                    .try_into()
                    .expect("slice has u64 length"),
            );
            let bits = if ordered & (1 << 63) != 0 {
                ordered ^ (1 << 63)
            } else {
                !ordered
            };
            Ok(Value::F64(f64::from_bits(bits)))
        }
        ColumnType::Bool => {
            expect_persisted_index_key_tag(bytes, index_name, 5)?;
            match take_persisted_index_key_bytes(bytes, index_name, 1)?[0] {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                _ => Err(Error::InvalidPersistedIndex(index_name.to_owned())),
            }
        }
        ColumnType::String => {
            expect_persisted_index_key_tag(bytes, index_name, 6)?;
            let value = decode_persisted_index_ordered_bytes(bytes, index_name)?;
            Ok(Value::String(String::from_utf8(value).map_err(|_| {
                Error::InvalidPersistedIndex(index_name.to_owned())
            })?))
        }
        ColumnType::Bytes => {
            expect_persisted_index_key_tag(bytes, index_name, 7)?;
            Ok(Value::Bytes(decode_persisted_index_ordered_bytes(
                bytes, index_name,
            )?))
        }
        ColumnType::Uuid => {
            expect_persisted_index_key_tag(bytes, index_name, 10)?;
            Ok(Value::Uuid(uuid::Uuid::from_bytes(
                take_persisted_index_key_bytes(bytes, index_name, 16)?
                    .try_into()
                    .expect("slice has uuid length"),
            )))
        }
        ColumnType::Enum(schema) => {
            expect_persisted_index_key_tag(bytes, index_name, 0)?;
            let discriminant = take_persisted_index_key_bytes(bytes, index_name, 1)?[0];
            schema
                .variant(discriminant)
                .map_err(|_| Error::InvalidPersistedIndex(index_name.to_owned()))?;
            Ok(Value::Enum(discriminant))
        }
        ColumnType::Tuple(members) => {
            expect_persisted_index_key_tag(bytes, index_name, 11)?;
            let mut values = Vec::with_capacity(members.len());
            for member in members {
                values.push(decode_index_key_part(bytes, member, index_name)?);
            }
            Ok(Value::Tuple(values))
        }
        ColumnType::Nullable(inner) => {
            match take_persisted_index_key_bytes(bytes, index_name, 1)?[0] {
                8 => Ok(Value::Nullable(None)),
                9 => Ok(Value::Nullable(Some(Box::new(decode_index_key_part(
                    bytes, inner, index_name,
                )?)))),
                _ => Err(Error::InvalidPersistedIndex(index_name.to_owned())),
            }
        }
        ColumnType::Array(_) => Err(Error::InvalidPersistedIndex(index_name.to_owned())),
    }
}

fn expect_key_tag(bytes: &mut &[u8], expected: u8) -> Result<(), Error> {
    let actual = take_key_bytes(bytes, 1)?[0];
    if actual == expected {
        Ok(())
    } else {
        Err(Error::InvalidDirectRecordStoreKey("tag".to_owned()))
    }
}

fn take_key_bytes<'a>(bytes: &mut &'a [u8], len: usize) -> Result<&'a [u8], Error> {
    if bytes.len() < len {
        return Err(Error::InvalidDirectRecordStoreKey("truncated".to_owned()));
    }
    let (head, tail) = bytes.split_at(len);
    *bytes = tail;
    Ok(head)
}

fn expect_persisted_index_key_tag(
    bytes: &mut &[u8],
    index_name: &str,
    expected: u8,
) -> Result<(), Error> {
    let actual = take_persisted_index_key_bytes(bytes, index_name, 1)?[0];
    if actual == expected {
        Ok(())
    } else {
        Err(Error::InvalidPersistedIndex(index_name.to_owned()))
    }
}

fn take_persisted_index_key_bytes<'a>(
    bytes: &mut &'a [u8],
    index_name: &str,
    len: usize,
) -> Result<&'a [u8], Error> {
    if bytes.len() < len {
        return Err(Error::InvalidPersistedIndex(index_name.to_owned()));
    }
    let (head, tail) = bytes.split_at(len);
    *bytes = tail;
    Ok(head)
}

fn decode_persisted_index_ordered_bytes(
    bytes: &mut &[u8],
    index_name: &str,
) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    loop {
        let byte = take_persisted_index_key_bytes(bytes, index_name, 1)?[0];
        if byte != 0 {
            out.push(byte);
            continue;
        }
        match take_persisted_index_key_bytes(bytes, index_name, 1)?[0] {
            0 => return Ok(out),
            0xff => out.push(0),
            _ => return Err(Error::InvalidPersistedIndex(index_name.to_owned())),
        }
    }
}

fn decode_ordered_bytes(bytes: &mut &[u8]) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    loop {
        let byte = take_key_bytes(bytes, 1)?[0];
        if byte != 0 {
            out.push(byte);
            continue;
        }
        match take_key_bytes(bytes, 1)?[0] {
            0 => return Ok(out),
            0xff => out.push(0),
            _ => return Err(Error::InvalidDirectRecordStoreKey("bytes".to_owned())),
        }
    }
}

fn index_record_descriptor() -> RecordDescriptor {
    static DESCRIPTOR: std::sync::OnceLock<RecordDescriptor> = std::sync::OnceLock::new();
    *DESCRIPTOR.get_or_init(|| {
        RecordDescriptor::new([
            ("key", records::ValueType::Bytes),
            ("value", records::ValueType::Bytes),
        ])
    })
}

fn encode_index_prefix_part(
    key: &mut Vec<u8>,
    value: &Value,
    column_type: &ColumnType,
) -> Result<(), Error> {
    match (value, column_type) {
        (Value::String(variant), ColumnType::Enum(schema)) => {
            encode_key_part(key, &Value::U8(schema.discriminant(variant)?))
                .map_err(Error::IvmRuntime)
        }
        (Value::Enum(discriminant), ColumnType::Enum(schema)) => {
            schema
                .variant(*discriminant)
                .map_err(Error::RecordEncoding)?;
            encode_key_part(key, &Value::U8(*discriminant)).map_err(Error::IvmRuntime)
        }
        (Value::Nullable(None), ColumnType::Nullable(_)) => {
            encode_key_part(key, &Value::Nullable(None)).map_err(Error::IvmRuntime)
        }
        (Value::Nullable(Some(value)), ColumnType::Nullable(inner)) => {
            let mut encoded = Vec::new();
            encode_index_prefix_part(&mut encoded, value, inner)?;
            let mut wrapped = Vec::new();
            wrapped.push(9);
            wrapped.extend(encoded);
            key.extend(wrapped);
            Ok(())
        }
        _ => encode_key_part(key, value).map_err(Error::IvmRuntime),
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("database instance is poisoned after a failed atomic commit")]
    DatabasePoisoned,
    #[error("duplicate primary key for table {table}: {key:?}")]
    DuplicatePrimaryKey { table: String, key: Vec<u8> },
    #[error("duplicate query parameter binding: {0}")]
    DuplicateParameter(String),
    #[error(transparent)]
    IvmRuntime(#[from] IvmRuntimeError),
    #[error("invalid persisted index contents: {0}")]
    InvalidPersistedIndex(String),
    #[error("index key arity mismatch for {index}: expected at most {expected}, got {actual}")]
    IndexKeyArity {
        index: String,
        expected: usize,
        actual: usize,
    },
    #[error("index not found: {table}.{index}")]
    IndexNotFound { table: String, index: String },
    #[error("missing query parameter binding: {0}")]
    MissingParameter(String),
    #[error("table has no primary key: {0}")]
    MissingPrimaryKey(String),
    #[error("primary key arity mismatch for {table}: expected at most {expected}, got {actual}")]
    PrimaryKeyArity {
        table: String,
        expected: usize,
        actual: usize,
    },
    #[error("primary key type mismatch for {table}.{column}")]
    PrimaryKeyTypeMismatch { table: String, column: String },
    #[error(transparent)]
    QueryPlanning(#[from] PlannerError),
    #[error(transparent)]
    RecordEncoding(#[from] records::Error),
    #[error("direct record store not found: {0}")]
    DirectRecordStoreNotFound(String),
    #[error("invalid direct record store key: {0}")]
    InvalidDirectRecordStoreKey(String),
    #[error(transparent)]
    Storage(Box<crate::storage::Error>),
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("unknown query parameter binding: {0}")]
    UnknownParameter(String),
}

impl From<crate::storage::Error> for Error {
    fn from(error: crate::storage::Error) -> Self {
        Self::Storage(Box::new(error))
    }
}

#[cfg(test)]
mod tests;
