use super::*;

impl<S> Database<S>
where
    S: ResidentStorage,
{
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
        self.ensure_not_poisoned()?;
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
        self.ensure_not_poisoned()?;
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
        self.ensure_not_poisoned()?;
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
        self.ensure_not_poisoned()?;
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
        self.ensure_not_poisoned()?;
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
        self.ensure_not_poisoned()?;
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
        self.ensure_not_poisoned()?;
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
    /// let binding_descriptor = RecordDescriptor::new([("year", ColumnType::U64.clone())]);
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
        self.ensure_not_poisoned()?;
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
        self.ensure_not_poisoned()?;
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
        self.ensure_not_poisoned()?;
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
        self.ensure_not_poisoned()?;
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
        self.ensure_not_poisoned()?;
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.ivm_runtime
            .query_snapshots(sinks, &storage)
            .map_err(Error::IvmRuntime)
    }

    pub fn unsubscribe(&mut self, subscription_id: SubscriptionId) -> bool {
        self.ivm_runtime
            .unsubscribe_with_storage(subscription_id, &self.storage)
            .unwrap_or(false)
    }

    /// Retire subscriptions whose receiving handles have already been
    /// dropped, even when no later data delta exists to discover the closed
    /// notification channel.
    pub fn prune_dropped_subscriptions(&mut self) -> Result<usize, Error> {
        self.ensure_not_poisoned()?;
        self.ivm_runtime
            .prune_dropped_subscriptions_with_storage(&self.storage)
            .map_err(Error::IvmRuntime)
    }
}
