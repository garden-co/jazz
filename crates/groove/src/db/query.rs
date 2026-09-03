use super::*;

impl Database {
    /// Allocate an opaque mutable input source for graphs maintained by this
    /// database. The identity is runtime-local and cannot be reused after the
    /// database closes.
    pub fn allocate_input_source(&mut self, descriptor: RecordDescriptor) -> InputSourceId {
        self.ivm_runtime.allocate_input_source(descriptor)
    }

    /// Atomically replace multiple runtime-owned source record sets and drive
    /// them through the same IVM graph used by ordinary table changes.
    pub async fn replace_input_sources(
        &mut self,
        replacements: impl IntoIterator<Item = InputSourceReplacement>,
    ) -> Result<TickMetrics, Error> {
        self.ensure_not_poisoned()?;
        let overlay = Rc::new(StagedWriteOverlay::new_owned(
            Rc::clone(&self.storage),
            Rc::clone(&self.resident_writes),
        ));
        let storage = Rc::new(MeteredStorage::new_owned(
            overlay,
            Rc::clone(&self.storage_read_metrics),
        ));
        let metrics = match self
            .ivm_runtime
            .replace_input_sources(replacements, &storage)
            .await
        {
            Ok(metrics) => metrics,
            // These are all preflight failures: record decoding, runtime
            // ownership, and descriptor compatibility are checked before the
            // first source refcount changes. They are ordinary recoverable
            // caller errors, so a following valid replacement may proceed.
            Err(
                error @ (IvmRuntimeError::RecordEncoding(_)
                | IvmRuntimeError::ForeignInputSource
                | IvmRuntimeError::InputSourceRetired
                | IvmRuntimeError::BindingSourceDescriptorMismatch(_)),
            ) => {
                return Err(Error::IvmRuntime(error));
            }
            // A tick can have touched graph/operator state after inputs were
            // installed. Database commits use the same fail-closed rule: do
            // not expose a possibly half-evaluated runtime to later calls.
            Err(error) => {
                self.poisoned = true;
                return Err(Error::IvmRuntime(error));
            }
        };
        self.last_tick_metrics = Some(metrics.clone());
        if let Err(error) = self.drive_resident_progress_now() {
            self.poisoned = true;
            return Err(error);
        }
        Ok(metrics)
    }

    /// Atomically apply set-like record additions and removals to
    /// runtime-owned inputs, then drive one ordinary IVM tick.
    pub async fn apply_input_source_deltas(
        &mut self,
        deltas: impl IntoIterator<Item = InputSourceDelta>,
    ) -> Result<TickMetrics, Error> {
        self.ensure_not_poisoned()?;
        let overlay = Rc::new(StagedWriteOverlay::new_owned(
            Rc::clone(&self.storage),
            Rc::clone(&self.resident_writes),
        ));
        let storage = Rc::new(MeteredStorage::new_owned(
            overlay,
            Rc::clone(&self.storage_read_metrics),
        ));
        let metrics = match self
            .ivm_runtime
            .apply_input_source_deltas(deltas, &storage)
            .await
        {
            Ok(metrics) => metrics,
            Err(
                error @ (IvmRuntimeError::RecordEncoding(_)
                | IvmRuntimeError::ForeignInputSource
                | IvmRuntimeError::InputSourceRetired
                | IvmRuntimeError::BindingSourceDescriptorMismatch(_)),
            ) => return Err(Error::IvmRuntime(error)),
            Err(error) => {
                self.poisoned = true;
                return Err(Error::IvmRuntime(error));
            }
        };
        self.last_tick_metrics = Some(metrics.clone());
        if let Err(error) = self.drive_resident_progress_now() {
            self.poisoned = true;
            return Err(error);
        }
        Ok(metrics)
    }

    /// Retract and permanently retire runtime-local input identities. A
    /// retired source stays empty in already-compiled graphs and cannot be
    /// replaced again.
    pub async fn retire_input_sources(
        &mut self,
        ids: impl IntoIterator<Item = InputSourceId>,
    ) -> Result<TickMetrics, Error> {
        self.ensure_not_poisoned()?;
        let overlay = Rc::new(StagedWriteOverlay::new_owned(
            Rc::clone(&self.storage),
            Rc::clone(&self.resident_writes),
        ));
        let storage = Rc::new(MeteredStorage::new_owned(
            overlay,
            Rc::clone(&self.storage_read_metrics),
        ));
        let metrics = match self.ivm_runtime.retire_input_sources(ids, &storage).await {
            Ok(metrics) => metrics,
            Err(
                error @ (IvmRuntimeError::ForeignInputSource | IvmRuntimeError::InputSourceRetired),
            ) => {
                return Err(Error::IvmRuntime(error));
            }
            Err(error) => {
                self.poisoned = true;
                return Err(Error::IvmRuntime(error));
            }
        };
        self.last_tick_metrics = Some(metrics.clone());
        if let Err(error) = self.drive_resident_progress_now() {
            self.poisoned = true;
            return Err(error);
        }
        Ok(metrics)
    }

    /// Drain continuation turns which the IVM itself has explicitly scheduled
    /// for already-resident work. Stop as soon as a storage/chunk request is
    /// genuinely pending: this direct API has no durable owner waker to retain
    /// for external readiness, and callers may install one with
    /// [`Self::drive_ready_progress_with_waker`] or [`Self::poll_subscription`].
    pub(super) fn drive_resident_progress_now(&mut self) -> Result<(), Error> {
        loop {
            // Never use a new direct call as an excuse to poll a cold
            // evaluation again. Scan the runtime's per-evaluation signals so
            // an older cold hydration cannot hide later resident work.
            if !self.ivm_runtime.has_resident_continuation() {
                return Ok(());
            }
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            match self.poll_resident_progress(&mut cx) {
                std::task::Poll::Ready(result) => return result,
                // The runtime records an explicit resident continuation when
                // a bounded CPU slice yields. Storage may self-wake before it
                // is ready, so it remains pending even if it woke this no-op
                // waker.
                std::task::Poll::Pending if self.ivm_runtime.has_resident_continuation() => {}
                std::task::Poll::Pending => return Ok(()),
            }
        }
    }

    /// A direct Database API call owns CPU-only continuations that it starts.
    /// It must publish their terminal output before returning, but it must not
    /// turn a cold storage request into a blocking call without a runtime
    /// owner to resume it later.
    pub(super) fn drain_self_scheduled_resident_progress(&mut self) -> Result<(), Error> {
        self.drive_resident_progress_now()
    }

    /// Whether a suspended IVM evaluation still needs a future owner turn.
    ///
    /// An external chunk completion can wake an evaluation which then starts a
    /// second asynchronous operation (for example, durable install metadata).
    /// Once the chunk request itself is complete, callers must still keep
    /// polling this work until the runtime reaches a terminal state.
    pub fn has_pending_progress(&self) -> bool {
        self.ivm_runtime.has_pending_incremental()
    }

    /// Drive every suspended incremental evaluation until the runtime is
    /// either quiescent or waiting for storage.
    ///
    /// Resident writes already perform this same work before `apply_batch`
    /// returns. Runtime owners use this poll boundary to resume cold work when
    /// its storage futures wake; individual subscriptions do not own separate
    /// evaluators.
    pub fn poll_progress(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        if let Err(error) = self.ensure_not_poisoned() {
            return std::task::Poll::Ready(Err(error));
        }
        let progress = self.ivm_runtime.poll_pending_incremental(cx);
        self.refresh_resident_writes();
        match progress {
            std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(Ok(())),
            std::task::Poll::Ready(Err(error)) => {
                self.poisoned = true;
                std::task::Poll::Ready(Err(Error::IvmRuntime(error)))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    /// Poll only explicitly runnable CPU continuations. This is the direct
    /// API counterpart to [`Self::poll_progress`]: it must never repoll a
    /// storage-pending evaluation merely because another direct operation
    /// started later.
    fn poll_resident_progress(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        if let Err(error) = self.ensure_not_poisoned() {
            return std::task::Poll::Ready(Err(error));
        }
        let progress = self.ivm_runtime.poll_resident_incremental(cx);
        self.refresh_resident_writes();
        match progress {
            std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(Ok(())),
            std::task::Poll::Ready(Err(error)) => {
                self.poisoned = true;
                std::task::Poll::Ready(Err(Error::IvmRuntime(error)))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    /// Await completion of all currently suspended incremental evaluation.
    pub async fn drive_progress(&mut self) -> Result<(), Error> {
        std::future::poll_fn(|cx| self.poll_progress(cx)).await
    }

    /// Drive every currently runnable incremental evaluation, returning once
    /// the runtime is either complete or waiting for storage.
    ///
    /// Unlike [`Self::drive_progress`], this does not hold an unrelated owner
    /// loop open while cold inputs are being acquired. Hosts that need a wake
    /// after this short turn can use [`Self::drive_ready_progress_with_waker`].
    pub async fn drive_ready_progress(&mut self) -> Result<(), Error> {
        self.drive_ready_progress_with_waker(None).await
    }

    /// Drive currently runnable work without waiting for cold storage, using
    /// `progress_waker` for any storage request that remains pending.
    ///
    /// A non-blocking owner turn returns immediately after it discovers cold
    /// work. Its executor waker is therefore no longer a useful continuation
    /// target. Runtime shells that can arrange another owner turn supply their
    /// own durable wake bridge here; callers without one retain the historical
    /// externally-driven polling behaviour.
    pub async fn drive_ready_progress_with_waker(
        &mut self,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<(), Error> {
        let progress = match progress_waker {
            Some(progress_waker) => {
                std::future::poll_fn(|_| {
                    let mut progress_cx = std::task::Context::from_waker(progress_waker);
                    std::task::Poll::Ready(self.poll_progress(&mut progress_cx))
                })
                .await
            }
            None => std::future::poll_fn(|cx| std::task::Poll::Ready(self.poll_progress(cx))).await,
        };
        match progress {
            std::task::Poll::Ready(result) => result,
            std::task::Poll::Pending => Ok(()),
        }
    }

    pub fn poll_subscription(
        &mut self,
        subscription: &Subscription,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<RecordDeltas, Error>> {
        if let std::task::Poll::Ready(event) = subscription.poll_next_event(cx) {
            return std::task::Poll::Ready(match event {
                SubscriptionEvent::Update(update) => Ok(update.deltas),
                SubscriptionEvent::Error(SubscriptionError::Ended) => Err(Error::SubscriptionEnded),
                SubscriptionEvent::Error(error) => Err(Error::SubscriptionFailed(error)),
            });
        }
        if let std::task::Poll::Ready(result) = self.poll_progress(cx)
            && let Err(error) = result
        {
            return std::task::Poll::Ready(Err(error));
        }
        subscription.poll_next_event(cx).map(|event| match event {
            SubscriptionEvent::Update(update) => Ok(update.deltas),
            SubscriptionEvent::Error(SubscriptionError::Ended) => Err(Error::SubscriptionEnded),
            SubscriptionEvent::Error(error) => Err(Error::SubscriptionFailed(error)),
        })
    }

    pub async fn next_subscription(
        &mut self,
        subscription: &Subscription,
    ) -> Result<RecordDeltas, Error> {
        std::future::poll_fn(|cx| self.poll_subscription(subscription, cx)).await
    }

    pub fn poll_subscription_with_publication(
        &mut self,
        subscription: &Subscription,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<PublicationUpdate<RecordDeltas>, Error>> {
        if let std::task::Poll::Ready(event) = subscription.poll_next_event(cx) {
            return std::task::Poll::Ready(match event {
                SubscriptionEvent::Update(update) => Ok(update),
                SubscriptionEvent::Error(SubscriptionError::Ended) => Err(Error::SubscriptionEnded),
                SubscriptionEvent::Error(error) => Err(Error::SubscriptionFailed(error)),
            });
        }
        if let std::task::Poll::Ready(result) = self.poll_progress(cx)
            && let Err(error) = result
        {
            return std::task::Poll::Ready(Err(error));
        }
        subscription.poll_next_event(cx).map(|event| match event {
            SubscriptionEvent::Update(update) => Ok(update),
            SubscriptionEvent::Error(SubscriptionError::Ended) => Err(Error::SubscriptionEnded),
            SubscriptionEvent::Error(error) => Err(Error::SubscriptionFailed(error)),
        })
    }

    pub async fn next_subscription_with_publication(
        &mut self,
        subscription: &Subscription,
    ) -> Result<PublicationUpdate<RecordDeltas>, Error> {
        std::future::poll_fn(|cx| self.poll_subscription_with_publication(subscription, cx)).await
    }

    pub fn poll_multisink_subscription(
        &mut self,
        subscription: &MultisinkSubscription,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<MultisinkDeltas, Error>> {
        if let std::task::Poll::Ready(event) = subscription.poll_next_event(cx) {
            return std::task::Poll::Ready(match event {
                SubscriptionEvent::Update(update) => Ok(update.deltas),
                SubscriptionEvent::Error(SubscriptionError::Ended) => Err(Error::SubscriptionEnded),
                SubscriptionEvent::Error(error) => Err(Error::SubscriptionFailed(error)),
            });
        }
        if let std::task::Poll::Ready(result) = self.poll_progress(cx)
            && let Err(error) = result
        {
            return std::task::Poll::Ready(Err(error));
        }
        subscription.poll_next_event(cx).map(|event| match event {
            SubscriptionEvent::Update(update) => Ok(update.deltas),
            SubscriptionEvent::Error(SubscriptionError::Ended) => Err(Error::SubscriptionEnded),
            SubscriptionEvent::Error(error) => Err(Error::SubscriptionFailed(error)),
        })
    }

    pub async fn next_multisink_subscription(
        &mut self,
        subscription: &MultisinkSubscription,
    ) -> Result<MultisinkDeltas, Error> {
        std::future::poll_fn(|cx| self.poll_multisink_subscription(subscription, cx)).await
    }

    pub fn poll_multisink_subscription_with_publication(
        &mut self,
        subscription: &MultisinkSubscription,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<PublicationUpdate<MultisinkDeltas>, Error>> {
        if let std::task::Poll::Ready(event) = subscription.poll_next_event(cx) {
            return std::task::Poll::Ready(match event {
                SubscriptionEvent::Update(update) => Ok(update),
                SubscriptionEvent::Error(SubscriptionError::Ended) => Err(Error::SubscriptionEnded),
                SubscriptionEvent::Error(error) => Err(Error::SubscriptionFailed(error)),
            });
        }
        if let std::task::Poll::Ready(result) = self.poll_progress(cx)
            && let Err(error) = result
        {
            return std::task::Poll::Ready(Err(error));
        }
        subscription.poll_next_event(cx).map(|event| match event {
            SubscriptionEvent::Update(update) => Ok(update),
            SubscriptionEvent::Error(SubscriptionError::Ended) => Err(Error::SubscriptionEnded),
            SubscriptionEvent::Error(error) => Err(Error::SubscriptionFailed(error)),
        })
    }

    pub async fn next_multisink_subscription_with_publication(
        &mut self,
        subscription: &MultisinkSubscription,
    ) -> Result<PublicationUpdate<MultisinkDeltas>, Error> {
        std::future::poll_fn(|cx| {
            self.poll_multisink_subscription_with_publication(subscription, cx)
        })
        .await
    }

    /// Subscribe to an IVM graph and receive an initial snapshot followed by
    /// deltas from committed batches.
    ///
    /// ```rust
    /// # futures::executor::block_on(async {
    /// # use groove::db::{Database, GraphBuilder};
    /// # use groove::records::Value;
    /// # use groove::schema::{ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema};
    /// # use groove::storage::MemoryStorage;
    /// # async fn db() -> Result<Database, groove::db::Error> {
    /// #     let schema = DatabaseSchema::new([TableSchema::new("albums", [
    /// #         ColumnSchema::new("id", ColumnType::U64),
    /// #         ColumnSchema::new("title", ColumnType::String),
    /// #         ColumnSchema::new("year", ColumnType::U64),
    /// #     ]).with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// #       .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// #     Database::new(schema, MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families")).await
    /// # }
    /// # let mut database = db().await?;
    /// let subscription = database.subscribe_one_sink(GraphBuilder::table("albums")).await?;
    /// assert!(subscription.recv()?.is_empty());
    ///
    /// let mut batch = database.open_batch();
    /// batch.insert(
    ///     "albums",
    ///     vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)],
    /// );
    /// let applied = database.apply_batch(batch).await?;
    /// let persisted = applied.persist().await;
    /// database.finish_persistence(persisted)?;
    ///
    /// assert_eq!(
    ///     subscription.recv()?.to_values()?,
    ///     vec![(vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)], 1)]
    /// );
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }).unwrap();
    /// ```
    pub async fn subscribe_one_sink(&mut self, graph: GraphBuilder) -> Result<Subscription, Error> {
        self.ensure_not_poisoned()?;
        let overlay = Rc::new(StagedWriteOverlay::new_owned(
            Rc::clone(&self.storage),
            Rc::clone(&self.resident_writes),
        ));
        let storage = Rc::new(MeteredStorage::new_owned(
            overlay,
            Rc::clone(&self.storage_read_metrics),
        ));
        let subscription = self
            .ivm_runtime
            .subscribe_one_sink_with_waker(graph, &storage, None)
            .await
            .map_err(Error::IvmRuntime)?;
        // A direct async opening owns the immediately-resident continuation
        // chain, but it must not await a cold read with no durable owner to
        // resume it. Drain only self-scheduled cooperative slices here.
        self.drive_resident_progress_now()?;
        Ok(subscription)
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
        let subscription = self.subscribe_with_waker(sinks, None)?;
        self.drive_resident_progress_now()?;
        Ok(subscription)
    }

    /// Internal owner-loop subscription entrypoint.  A bounded opening poll
    /// may encounter cold storage; `progress_waker` remains its continuation
    /// instead of the transient opening task's waker.
    #[doc(hidden)]
    pub fn subscribe_with_waker<I, K>(
        &mut self,
        sinks: I,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<MultisinkSubscription, Error>
    where
        I: IntoIterator<Item = (K, GraphBuilder)>,
        K: Into<String>,
    {
        self.ensure_not_poisoned()?;
        let overlay = Rc::new(StagedWriteOverlay::new_owned(
            Rc::clone(&self.storage),
            Rc::clone(&self.resident_writes),
        ));
        let storage = Rc::new(MeteredStorage::new_owned(
            overlay,
            Rc::clone(&self.storage_read_metrics),
        ));
        self.ivm_runtime
            .subscribe_with_waker(sinks, &storage, progress_waker)
            .map_err(Error::IvmRuntime)
    }

    /// Subscribe to a SQL-ish query by letting the planner lower it into an IVM
    /// graph.
    ///
    /// ```rust
    /// # futures::executor::block_on(async {
    /// # use groove::db::Database;
    /// # use groove::queries::{BinaryOp, Expr, Query, Select, SelectItem, TableRef};
    /// # use groove::records::Value;
    /// # use groove::schema::{ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema};
    /// # use groove::storage::MemoryStorage;
    /// # async fn db() -> Result<Database, groove::db::Error> {
    /// #     let schema = DatabaseSchema::new([TableSchema::new("albums", [
    /// #         ColumnSchema::new("id", ColumnType::U64),
    /// #         ColumnSchema::new("title", ColumnType::String),
    /// #         ColumnSchema::new("year", ColumnType::U64),
    /// #     ]).with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// #       .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// #     Database::new(schema, MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families")).await
    /// # }
    /// # let mut database = db().await?;
    /// # let mut batch = database.open_batch();
    /// # batch.insert("albums", vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)]);
    /// # batch.insert("albums", vec![Value::U64(2), Value::String("Blue Train".into()), Value::U64(1957)]);
    /// # let applied = database.apply_batch(batch).await?;
    /// # let persisted = applied.persist().await;
    /// # database.finish_persistence(persisted)?;
    /// let query = Query::Select(Box::new(
    ///     Select::new([SelectItem::expr(Expr::column("title"))])
    ///         .from([TableRef::named("albums")])
    ///         .where_(Expr::binary(
    ///             Expr::column("year"),
    ///             BinaryOp::Eq,
    ///             Expr::Literal(Value::U64(1959)),
    ///         )),
    /// ));
    /// let subscription = database.subscribe_query(query).await?;
    ///
    /// assert_eq!(
    ///     subscription.recv()?.to_values()?,
    ///     vec![(vec![Value::String("Kind of Blue".into())], 1)]
    /// );
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }).unwrap();
    /// ```
    pub async fn subscribe_query(&mut self, query: Query) -> Result<Subscription, Error> {
        let planned = plan_query(&query, self.ivm_runtime.schema())?;
        self.subscribe_one_sink(planned.graph).await
    }

    /// Prepare a parameterized SQL-ish query shape once so callers can bind many
    /// concrete parameter sets without replanning.
    ///
    /// ```rust
    /// # futures::executor::block_on(async {
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
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families")).await?;
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
    /// let prepared = database.prepare_query(query).await?;
    /// assert_eq!(prepared.parameters()[0].name, "year");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }).unwrap();
    /// ```
    pub async fn prepare_query(&mut self, query: Query) -> Result<PreparedShape, Error> {
        self.ensure_not_poisoned()?;
        let planned = plan_prepared_shape(&query, self.ivm_runtime.schema())?;
        let output = RecordDescriptor::new(
            planned
                .public_output
                .iter()
                .map(|field| (field.name.clone(), field.value_type.clone())),
        );
        let shape = self
            .prepare_one_sink(
                planned.planned.graph,
                planned.shape,
                planned.binding_descriptor,
                planned.output_key_fields,
            )
            .await?;
        Ok(PreparedShape {
            id: shape.id(),
            parameters: planned.parameters,
            output,
        })
    }

    /// Bind a prepared query shape by named parameter.
    ///
    /// ```rust
    /// # futures::executor::block_on(async {
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
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families")).await?;
    /// # let mut batch = database.open_batch();
    /// # batch.insert("albums", vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)]);
    /// # let applied = database.apply_batch(batch).await?;
    /// # let persisted = applied.persist().await;
    /// # database.finish_persistence(persisted)?;
    /// # let query = Query::Select(Box::new(Select::new([SelectItem::Wildcard]).from([TableRef::named("albums")]).where_(Expr::binary(Expr::column("year"), BinaryOp::Eq, Expr::parameter("year")))));
    /// # let prepared = database.prepare_query(query).await?;
    /// let subscription = database.bind(&prepared, &[("year", Value::U64(1959))]).await?;
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
    /// # }).unwrap();
    /// ```
    pub async fn bind(
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
        let overlay = Rc::new(StagedWriteOverlay::new_owned(
            Rc::clone(&self.storage),
            Rc::clone(&self.resident_writes),
        ));
        let storage = Rc::new(MeteredStorage::new_owned(
            overlay,
            Rc::clone(&self.storage_read_metrics),
        ));
        let subscription = self
            .ivm_runtime
            .bind_shape_one_sink_with_output_and_waker(
                prepared.id,
                &values,
                prepared.output,
                &storage,
                None,
            )
            .map_err(Error::IvmRuntime)?;
        self.drive_resident_progress_now()?;
        Ok(subscription)
    }

    /// Prepare a one-sink parameterized graph shape directly.
    ///
    /// Most callers should prefer [`Database::prepare_query`]. This lower-level
    /// API is useful when a caller already has a [`GraphBuilder`]. Internally it
    /// is just sugar over [`Database::prepare`]: the graph is
    /// registered as the single route-carrying terminal, `output_key_fields`
    /// name the hidden route fields, and [`Database::bind_shape_one_sink`] adapts the
    /// one sink back to a [`Subscription`].
    pub async fn prepare_one_sink(
        &mut self,
        graph: GraphBuilder,
        binding_source_shape: impl Into<String>,
        binding_descriptor: RecordDescriptor,
        output_key_fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<crate::ivm::PreparedShape, Error> {
        self.ensure_not_poisoned()?;
        let overlay = StagedWriteOverlay::new(&self.storage, &self.resident_writes);
        let storage = MeteredStorage::new(&overlay, &self.storage_read_metrics);
        self.ivm_runtime
            .prepare_one_sink(
                graph,
                binding_source_shape,
                binding_descriptor,
                output_key_fields,
                &storage,
            )
            .await
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
    pub async fn prepare_one_sink_with_routing(
        &mut self,
        output_graph: GraphBuilder,
        routing_graph: GraphBuilder,
        binding_source_shape: impl Into<String>,
        binding_descriptor: RecordDescriptor,
        routing_key_fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<crate::ivm::PreparedShape, Error> {
        self.ensure_not_poisoned()?;
        let overlay = StagedWriteOverlay::new(&self.storage, &self.resident_writes);
        let storage = MeteredStorage::new(&overlay, &self.storage_read_metrics);
        self.ivm_runtime
            .prepare_one_sink_with_routing(
                output_graph,
                routing_graph,
                binding_source_shape,
                binding_descriptor,
                routing_key_fields,
                &storage,
            )
            .await
            .map_err(Error::IvmRuntime)
    }

    /// Prepare the canonical parameterized multisink shape.
    ///
    /// Each terminal graph carries hidden route columns plus public output
    /// columns. Binding appends ordinary filter/project graph nodes for each
    /// sink, so callers with one-sink needs should treat [`Database::prepare_one_sink`]
    /// and [`Database::prepare_one_sink_with_routing`] as thin convenience wrappers.
    pub async fn prepare(
        &mut self,
        terminals: impl IntoIterator<Item = RoutedMultisinkTerminal>,
        binding_source_shape: impl Into<String>,
        binding_descriptor: RecordDescriptor,
    ) -> Result<crate::ivm::PreparedShape, Error> {
        self.ensure_not_poisoned()?;
        let overlay = StagedWriteOverlay::new(&self.storage, &self.resident_writes);
        let storage = MeteredStorage::new(&overlay, &self.storage_read_metrics);
        self.ivm_runtime
            .prepare(
                terminals,
                binding_source_shape,
                binding_descriptor,
                &storage,
            )
            .await
            .map_err(Error::IvmRuntime)
    }

    /// Bind a prepared one-sink graph shape by positional values.
    ///
    /// ```rust
    /// # futures::executor::block_on(async {
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
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families")).await?;
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
    /// ).await?;
    ///
    /// let subscription = database.bind_shape_one_sink(shape.id(), &[Value::U64(1959)]).await?;
    /// assert!(subscription.recv()?.is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }).unwrap();
    /// ```
    pub async fn bind_shape_one_sink(
        &mut self,
        shape: PreparedShapeId,
        binding_values: &[Value],
    ) -> Result<Subscription, Error> {
        self.ensure_not_poisoned()?;
        let overlay = Rc::new(StagedWriteOverlay::new_owned(
            Rc::clone(&self.storage),
            Rc::clone(&self.resident_writes),
        ));
        let storage = Rc::new(MeteredStorage::new_owned(
            overlay,
            Rc::clone(&self.storage_read_metrics),
        ));
        let subscription = self
            .ivm_runtime
            .bind_shape_one_sink_with_waker(shape, binding_values, &storage, None)
            .map_err(Error::IvmRuntime)?;
        self.drive_resident_progress_now()?;
        Ok(subscription)
    }

    /// Bind a prepared one-sink graph shape while projecting subscriber-visible
    /// rows.
    ///
    /// This adapts the one routed multisink terminal back to [`Subscription`].
    /// The prepared terminal may contain hidden routing fields from
    /// `output_key_fields` or `routing_key_fields`; `public_output` selects the
    /// descriptor that bound subscribers receive.
    pub async fn bind_shape_one_sink_with_output(
        &mut self,
        shape: PreparedShapeId,
        binding_values: &[Value],
        public_output: RecordDescriptor,
    ) -> Result<Subscription, Error> {
        self.ensure_not_poisoned()?;
        let overlay = Rc::new(StagedWriteOverlay::new_owned(
            Rc::clone(&self.storage),
            Rc::clone(&self.resident_writes),
        ));
        let storage = Rc::new(MeteredStorage::new_owned(
            overlay,
            Rc::clone(&self.storage_read_metrics),
        ));
        let subscription = self
            .ivm_runtime
            .bind_shape_one_sink_with_output_and_waker(
                shape,
                binding_values,
                public_output,
                &storage,
                None,
            )
            .map_err(Error::IvmRuntime)?;
        self.drive_resident_progress_now()?;
        Ok(subscription)
    }

    /// Bind a routed multisink shape by positional values.
    pub async fn bind_shape(
        &mut self,
        shape: PreparedShapeId,
        binding_values: &[Value],
    ) -> Result<MultisinkSubscription, Error> {
        let subscription = self
            .bind_shape_with_waker(shape, binding_values, None)
            .await?;
        self.drive_resident_progress_now()?;
        Ok(subscription)
    }

    /// Internal owner-loop binding entrypoint; see [`Self::subscribe_with_waker`].
    #[doc(hidden)]
    pub async fn bind_shape_with_waker(
        &mut self,
        shape: PreparedShapeId,
        binding_values: &[Value],
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<MultisinkSubscription, Error> {
        self.ensure_not_poisoned()?;
        let overlay = Rc::new(StagedWriteOverlay::new_owned(
            Rc::clone(&self.storage),
            Rc::clone(&self.resident_writes),
        ));
        let storage = Rc::new(MeteredStorage::new_owned(
            overlay,
            Rc::clone(&self.storage_read_metrics),
        ));
        self.ivm_runtime
            .bind_shape_with_waker(shape, binding_values, &storage, progress_waker)
            .map_err(Error::IvmRuntime)
    }

    /// Run a one-shot SQL-ish query against the current storage snapshot.
    ///
    /// ```rust
    /// # futures::executor::block_on(async {
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
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families")).await?;
    /// # let mut batch = database.open_batch();
    /// # batch.insert("albums", vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)]);
    /// # batch.insert("albums", vec![Value::U64(2), Value::String("Blue Train".into()), Value::U64(1957)]);
    /// # let applied = database.apply_batch(batch).await?;
    /// # let persisted = applied.persist().await;
    /// # database.finish_persistence(persisted)?;
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
    /// let rows = database.query(query).await?;
    /// assert_eq!(rows.to_values()?, vec![(vec![Value::String("Kind of Blue".into())], 1)]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }).unwrap();
    /// ```
    pub async fn query(&mut self, query: Query) -> Result<RecordDeltas, Error> {
        let planned = plan_query(&query, self.ivm_runtime.schema())?;
        self.query_graph(planned.graph).await
    }

    /// Run a one-shot graph query against the current storage snapshot.
    ///
    /// This is the public database-level entry point for the runtime's
    /// snapshot-query path.
    ///
    /// ```rust
    /// # futures::executor::block_on(async {
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
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families")).await?;
    /// # let mut batch = database.open_batch();
    /// # batch.insert("albums", vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)]);
    /// # let applied = database.apply_batch(batch).await?;
    /// # let persisted = applied.persist().await;
    /// # database.finish_persistence(persisted)?;
    /// let rows = database.query_graph(
    ///     GraphBuilder::table("albums").filter(PredicateExpr::eq("year", Value::U64(1959))),
    /// ).await?;
    ///
    /// assert_eq!(
    ///     rows.to_values()?,
    ///     vec![(vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)], 1)]
    /// );
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }).unwrap();
    /// ```
    pub async fn query_graph(&mut self, graph: GraphBuilder) -> Result<RecordDeltas, Error> {
        self.ensure_not_poisoned()?;
        let overlay = StagedWriteOverlay::new(&self.storage, &self.resident_writes);
        let storage = MeteredStorage::new(&overlay, &self.storage_read_metrics);
        self.ivm_runtime
            .query_snapshot(graph, &storage)
            .await
            .map_err(Error::IvmRuntime)
    }

    /// Run several named graph outputs against the same current storage
    /// snapshot without registering a live subscription.
    pub async fn query_graphs<I, K>(&mut self, sinks: I) -> Result<MultisinkDeltas, Error>
    where
        I: IntoIterator<Item = (K, GraphBuilder)>,
        K: Into<String>,
    {
        self.ensure_not_poisoned()?;
        let overlay = StagedWriteOverlay::new(&self.storage, &self.resident_writes);
        let storage = MeteredStorage::new(&overlay, &self.storage_read_metrics);
        self.ivm_runtime
            .query_snapshots(sinks, &storage)
            .await
            .map_err(Error::IvmRuntime)
    }

    pub fn unsubscribe(&mut self, subscription_id: SubscriptionId) -> bool {
        self.ivm_runtime.unsubscribe(subscription_id)
    }

    /// Retire a prepared graph after all bindings have been unsubscribed.
    /// Long-lived callers should retain and reuse their shapes; one-shot
    /// callers must retire dynamically compiled shapes to release graph nodes.
    pub fn retire_prepared_shape(&mut self, shape: PreparedShapeId) -> Result<(), Error> {
        self.ensure_not_poisoned()?;
        self.ivm_runtime
            .retire_prepared_shape(shape)
            .map_err(Error::IvmRuntime)
    }

    /// Retire subscriptions whose receiving handles have already been
    /// dropped, even when no later data delta exists to discover the closed
    /// notification channel.
    pub async fn prune_dropped_subscriptions(&mut self) -> Result<usize, Error> {
        self.ensure_not_poisoned()?;
        self.ivm_runtime
            .prune_dropped_subscriptions_with_storage(&self.storage)
            .await
            .map_err(Error::IvmRuntime)
    }
}
