//! Database construction, schema views, write-state waiting, and connection controls.

use super::*;

impl<S> Db<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Configure Jazz-owned ingress and expiry policy for unpublished large values.
    pub fn set_large_value_staging_policy(&self, policy: crate::node::LargeValueStagingPolicy) {
        self.node.set_large_value_staging_policy(policy);
    }

    /// Run one host-driven staging-expiry maintenance pass.
    pub async fn evict_expired_staged_large_values(&self) -> Result<usize, Error> {
        self.node.evict_expired_staged_large_values().await
    }

    /// Open a database over the supplied storage and recover local state.
    ///
    /// ```rust
    /// # use jazz::db::{Db, DbConfig, DbIdentity, SeededRowIdSource};
    /// # use jazz::db::doctest_support::{block_on, schema, MemoryStorage};
    /// # use jazz::ids::{AuthorSubject, NodeUuid};
    /// let schema = schema();
    /// let column_families = schema.column_families();
    /// let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    /// let storage = MemoryStorage::new(&refs).expect("valid memory storage families");
    ///
    /// let db = block_on(Db::open(DbConfig {
    ///     schema,
    ///     storage,
    ///     identity: DbIdentity {
    ///         node: NodeUuid::from_bytes([1; 16]),
    ///         author: AuthorSubject::for_test_bytes([2; 16]),
    ///     },
    ///     id_source: Some(Box::new(SeededRowIdSource::new(1))),
    /// }))?;
    ///
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert!(db.read(&todos)?.is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn open(config: DbConfig<S>) -> Result<Self, Error> {
        let schema_version_id = config.schema.version_id();
        let schema_views = Rc::new(RefCell::new(BTreeMap::from([(
            SchemaViewId::for_schema(&config.schema),
            config.schema.clone(),
        )])));
        let node =
            NodeState::new(config.identity.node, config.schema.clone(), config.storage).await?;
        let node = Node::new(node);
        node.restore_pending_uploads(config.identity)?;
        let row_id_source_guarantees_fresh = config.id_source.is_none();
        Ok(Self {
            schema: config.schema,
            schema_version_id,
            schema_view_is_fixed: false,
            schema_views,
            identity: config.identity,
            node: Rc::new(node),
            row_id_source: Rc::new(RefCell::new(
                config
                    .id_source
                    .unwrap_or_else(|| Box::new(ProductionRowIdSource)),
            )),
            row_id_source_guarantees_fresh,
            next_now_ms: Rc::new(Cell::new(1)),
            backend_attribution: false,
        })
    }

    /// Open a Db allowed to record external provenance while preserving this
    /// Db's identity for permission admission.
    ///
    /// # Safety
    /// The caller must authenticate trusted backend authority before calling
    /// this constructor and must not expose it to ordinary application code.
    #[doc(hidden)]
    pub async unsafe fn open_with_backend_attribution(config: DbConfig<S>) -> Result<Self, Error> {
        let mut db = Self::open(config).await?;
        db.backend_attribution = true;
        Ok(db)
    }

    #[cfg(feature = "testing")]
    /// Open a database and return internal node-open phase timings for benchmarks.
    pub async fn open_with_receipt_for_test(
        config: DbConfig<S>,
    ) -> Result<(Self, DbOpenReceipt), Error> {
        let schema_version_id = config.schema.version_id();
        let schema_views = Rc::new(RefCell::new(BTreeMap::from([(
            SchemaViewId::for_schema(&config.schema),
            config.schema.clone(),
        )])));
        let (node, receipt) = NodeState::new_with_open_receipt_for_test(
            config.identity.node,
            config.schema.clone(),
            config.storage,
            false,
        )
        .await?;
        let row_id_source_guarantees_fresh = config.id_source.is_none();
        let db = Self {
            schema: config.schema,
            schema_version_id,
            schema_view_is_fixed: false,
            schema_views,
            identity: config.identity,
            node: Rc::new(Node::new(node)),
            row_id_source: Rc::new(RefCell::new(
                config
                    .id_source
                    .unwrap_or_else(|| Box::new(ProductionRowIdSource)),
            )),
            row_id_source_guarantees_fresh,
            next_now_ms: Rc::new(Cell::new(1)),
            backend_attribution: false,
        };
        Ok((db, receipt))
    }

    /// Open a database as a history-complete serving core.
    ///
    /// This mode is intended for server shells and tests that own authoritative
    /// in-memory history rather than a partial client replica.
    pub async fn open_history_complete(config: DbConfig<S>) -> Result<Self, Error> {
        let schema_version_id = config.schema.version_id();
        let schema_views = Rc::new(RefCell::new(BTreeMap::from([(
            SchemaViewId::for_schema(&config.schema),
            config.schema.clone(),
        )])));
        let node = NodeState::new_history_complete(
            config.identity.node,
            config.schema.clone(),
            config.storage,
        )
        .await?;
        let row_id_source_guarantees_fresh = config.id_source.is_none();
        Ok(Self {
            schema: config.schema,
            schema_version_id,
            schema_view_is_fixed: false,
            schema_views,
            identity: config.identity,
            node: Rc::new(Node::new(node)),
            row_id_source: Rc::new(RefCell::new(
                config
                    .id_source
                    .unwrap_or_else(|| Box::new(ProductionRowIdSource)),
            )),
            row_id_source_guarantees_fresh,
            next_now_ms: Rc::new(Cell::new(1)),
            backend_attribution: false,
        })
    }

    /// History-complete counterpart to [`Db::open_with_backend_attribution`].
    ///
    /// # Safety
    /// The caller must have authenticated trusted backend authority.
    #[doc(hidden)]
    pub async unsafe fn open_history_complete_with_backend_attribution(
        config: DbConfig<S>,
    ) -> Result<Self, Error> {
        let mut db = Self::open_history_complete(config).await?;
        db.backend_attribution = true;
        Ok(db)
    }

    /// Open an edge whose durable store has no authority catalogue yet.
    ///
    /// This is deliberately narrower than [`Db::open`]: callers may only use
    /// it to receive one connection-authenticated catalogue snapshot and then
    /// select one of the snapshot's admitted schema views.  Until then the
    /// node has no application schema and rejects ordinary data/sync work.
    #[cfg(feature = "runtime")]
    pub(crate) async fn open_catalogue_uninitialized_edge(
        config: DbConfig<S>,
    ) -> Result<Self, Error> {
        let bootstrap_schema = JazzSchema::empty();
        let schema_version_id = bootstrap_schema.version_id();
        let schema_views = Rc::new(RefCell::new(BTreeMap::from([(
            SchemaViewId::for_schema(&bootstrap_schema),
            bootstrap_schema.clone(),
        )])));
        let node =
            NodeState::new_catalogue_uninitialized(config.identity.node, config.storage).await?;
        let node = Node::new(node);
        node.restore_pending_uploads(config.identity)?;
        let row_id_source_guarantees_fresh = config.id_source.is_none();
        Ok(Self {
            schema: bootstrap_schema,
            schema_version_id,
            schema_view_is_fixed: false,
            schema_views,
            identity: config.identity,
            node: Rc::new(node),
            row_id_source: Rc::new(RefCell::new(
                config
                    .id_source
                    .unwrap_or_else(|| Box::new(ProductionRowIdSource)),
            )),
            row_id_source_guarantees_fresh,
            next_now_ms: Rc::new(Cell::new(1)),
            backend_attribution: false,
        })
    }

    /// Install a complete catalogue received over the authenticated upstream
    /// bootstrap link.  This is intentionally crate-private: ordinary wire
    /// dispatch must never turn an arbitrary peer's snapshot into authority.
    #[cfg(feature = "runtime")]
    pub(crate) fn apply_trusted_catalogue_snapshot(
        &self,
        snapshot: crate::protocol::CatalogueSnapshot,
    ) -> Result<(), Error> {
        let outcome = crate::db::block_on(
            self.node
                .node
                .borrow_mut()
                .apply_trusted_catalogue_snapshot(snapshot),
        )?;
        crate::db::block_on(self.finish_publication_outcome(outcome))
    }

    #[cfg(feature = "testing")]
    pub(crate) fn set_catalogue_activation_failpoint(
        &self,
        failpoint: crate::node::CatalogueActivationFailpoint,
    ) {
        self.node
            .node
            .borrow_mut()
            .set_catalogue_activation_failpoint(failpoint);
    }

    /// Produce the authority's complete catalogue for the privileged
    /// snapshot-only transport exchange.
    #[cfg(feature = "runtime")]
    pub(crate) fn trusted_catalogue_snapshot(
        &self,
    ) -> Result<crate::protocol::CatalogueSnapshot, Error> {
        Ok(self.node.node.borrow().catalogue_snapshot()?)
    }

    /// Return the active authority-admitted schema, failing closed when this
    /// dynamic edge still has no bootstrap receipt.
    #[cfg(feature = "runtime")]
    pub(crate) fn trusted_current_catalogue_schema(&self) -> Result<JazzSchema, Error> {
        let node = self.node.node.borrow();
        let pointer = node.current_write_schema()?;
        node.catalogue_schemas()
            .get(&pointer.schema)
            .map(|schema| schema.schema.clone())
            .ok_or_else(|| Error::new(ErrorCode::Schema, "active catalogue schema is missing"))
    }

    #[cfg(feature = "runtime")]
    pub(crate) fn catalogue_bootstrap_is_ready(&self) -> bool {
        self.node.node.borrow().catalogue_bootstrap_state()
            == crate::node::CatalogueBootstrapState::Ready
    }

    /// Register a typed schema view on this database owner.
    ///
    /// Registration is process-local and idempotent by the exact typed schema
    /// content. It does not publish a catalogue entry or select the current
    /// write schema. The returned handle shares the owner's node, open batches,
    /// connections, row-id source, and logical clock while validating typed
    /// operations against this exact schema view.
    pub async fn register_schema_view(&self, schema: JazzSchema) -> Result<Self, Error> {
        let schema_version_id = schema.version_id();
        let schema_view_id = SchemaViewId::for_schema(&schema);
        self.admit_local_schema_view_if_needed(&schema).await?;
        {
            let node = self.node.node.borrow();
            let admitted = node
                .catalogue_schemas()
                .get(&schema_version_id)
                .ok_or_else(|| Error::new(ErrorCode::Schema, "registered schema is missing"))?;
            if !schema_policy_metadata_matches(&admitted.schema, &schema) {
                return Err(Error::new(
                    ErrorCode::Schema,
                    "schema view policy metadata conflicts with its admitted structural schema",
                ));
            }
            if !schema_index_metadata_matches(&admitted.schema, &schema) {
                return Err(Error::new(
                    ErrorCode::Schema,
                    "schema view index metadata conflicts with its admitted structural schema",
                ));
            }
        }
        let mut views = self.schema_views.borrow_mut();
        if let Some(existing) = views.get(&schema_view_id) {
            if existing != &schema {
                return Err(Error::new(
                    ErrorCode::Schema,
                    format!("schema view id collision for {schema_view_id:?}"),
                ));
            }
        } else {
            views.insert(schema_view_id, schema.clone());
        }
        drop(views);
        Ok(Self {
            schema,
            schema_version_id,
            schema_view_is_fixed: true,
            schema_views: Rc::clone(&self.schema_views),
            identity: self.identity,
            node: Rc::clone(&self.node),
            row_id_source: Rc::clone(&self.row_id_source),
            row_id_source_guarantees_fresh: self.row_id_source_guarantees_fresh,
            next_now_ms: Rc::clone(&self.next_now_ms),
            backend_attribution: self.backend_attribution,
        })
    }

    /// Attach an already-registered typed schema view to this owner.
    pub async fn schema_view(&self, schema_view_id: SchemaViewId) -> Result<Self, Error> {
        let schema = self
            .schema_views
            .borrow()
            .get(&schema_view_id)
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::Schema,
                    format!("schema view {schema_view_id:?} is not registered"),
                )
            })?;
        self.register_schema_view(schema).await
    }

    /// Canonical id of this handle's typed schema view.
    pub fn schema_view_id(&self) -> SchemaViewId {
        SchemaViewId::for_schema(&self.schema)
    }

    /// Admit the first application schema into an owner deliberately opened
    /// with the empty schema. This is the local-first bootstrap equivalent of
    /// having opened the runtime with that schema originally; later schemas
    /// still arrive through ordinary catalogue lineage publication.
    async fn admit_local_schema_view_if_needed(&self, schema: &JazzSchema) -> Result<(), Error> {
        let empty_schema = JazzSchema::empty();
        let empty_id = empty_schema.version_id();
        let target_id = schema.version_id();
        let (source, catalogue_seq, bootstrap_current) = {
            let node = self.node.node.borrow();
            if node.catalogue_schemas().contains_key(&target_id) {
                return Ok(());
            }
            let current = node.current_write_schema().map_err(Error::from)?;
            let source = node
                .catalogue_schemas()
                .get(&current.schema)
                .map(|version| version.schema.clone())
                .ok_or_else(|| Error::new(ErrorCode::Schema, "current schema view is missing"))?;
            (
                source,
                node.active_catalogue_seq().saturating_add(1),
                current.schema == empty_id && node.catalogue_schemas().len() == 1,
            )
        };
        let (lens, new_tables, dropped_tables) = direct_schema_view_lens(&source, schema)?;
        let outcome = {
            let mut node = self.node.node.lock().await;
            let publication = node
                .author_schema_lineage_publication(
                    SchemaVersion::new(schema.clone()),
                    lens,
                    new_tables,
                    dropped_tables,
                )
                .map_err(Error::from)?;
            node.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
                author: AuthorSubject::SYSTEM,
                catalogue_seq,
                publication: Box::new(publication),
            })
            .await?
        };
        self.finish_publication_outcome(outcome).await?;
        if bootstrap_current {
            let outcome = {
                let mut node = self.node.node.lock().await;
                node.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
                    author: AuthorSubject::SYSTEM,
                    pointer: CurrentWriteSchema {
                        revision: 1,
                        schema: target_id,
                    },
                })
                .await?
            };
            self.finish_publication_outcome(outcome).await?;
        }
        Ok(())
    }

    /// Flush node-local maintenance state, write a clean-close marker, and
    /// close storage without blocking the caller's executor.
    pub async fn close(&self) -> Result<(), Error> {
        if self.schema_view_is_fixed {
            return Ok(());
        }
        // Close finalization admission before the first await. This makes the
        // queued retirement set and durable close one lifecycle transition:
        // a stream dropped while storage is shutting down is either in this
        // drain or already part of the retired terminal runtime.
        self.node.begin_subscription_finalization_shutdown();
        self.node.drain_subscription_finalizations().await?;
        self.node.node.lock().await.close().await?;
        self.node.retire_subscription_runtime_after_close();
        Ok(())
    }

    /// Configure this database as the optimistic, non-durable side of a
    /// browser client/worker pair. This must be called before application
    /// writes begin.
    pub fn set_non_durable_client(&self) {
        self.node.set_non_durable_client();
    }

    /// Configure this durable process as the internal browser relay that owns
    /// fresh upstream authority sessions for client Edge reads.
    #[doc(hidden)]
    pub fn set_relay_authority_session_owner(&self) {
        self.node.set_relay_authority_session_owner();
    }

    /// Restore unsettled writes relayed from a browser client sharing this
    /// worker's author. Browser workers persist main-thread transactions whose
    /// node differs from the worker node, so ordinary local-origin recovery
    /// cannot discover them after a cold worker restart.
    #[doc(hidden)]
    pub fn restore_browser_relay_pending_uploads(&self) -> Result<(), Error> {
        self.node
            .restore_browser_relay_pending_uploads(self.identity.author)
    }

    /// Let a single-threaded host return resident writes synchronously while
    /// its tick loop owns suspendable persistence and later peer visibility.
    pub fn set_deferred_local_persistence(&self, deferred: bool) {
        self.node.set_deferred_local_persistence(deferred);
    }

    /// Configure this client database's first-snapshot durability cadence.
    ///
    /// Servers do not call this client-only setting and retain their existing
    /// storage durability behavior.
    pub fn set_initial_sync_flush_cadence(
        &self,
        cadence: InitialSyncFlushCadence,
    ) -> Result<(), Error> {
        Ok(crate::db::block_on(
            self.node
                .node
                .borrow_mut()
                .set_initial_sync_flush_cadence(cadence.writes()),
        )?)
    }

    /// Seed a settled mergeable row for server bootstrap/import flows.
    ///
    /// This bypasses the client pending-upload path and immediately finalizes
    /// the commit in local history. It is intended only for history-complete
    /// server bootstrap/import state, not for general application writes or
    /// pending client write semantics.
    pub fn seed_settled_mergeable_for_bootstrap(
        &self,
        table: &str,
        row: RowUuid,
        made_by: AuthorSubject,
        mut cells: RowCells,
    ) -> Result<TxId, Error> {
        let (write_schema, write_schema_version) = self.current_write_schema_for_query()?;
        let table_schema = write_schema
            .tables
            .iter()
            .find(|candidate| candidate.name == table)
            .ok_or_else(|| Error::new(ErrorCode::Schema, format!("unknown table {table}")))?;
        for column in &table_schema.columns {
            if !cells.contains_key(&column.name)
                && let Some(default) = &column.default
            {
                cells.insert(
                    column.name.clone(),
                    default_cell_for_column_type(&column.column_type, default),
                );
            }
        }
        let published = crate::db::block_on(
            self.node.node.borrow_mut().commit_mergeable_in_schema(
                write_schema_version,
                MergeableCommit::new(table, row, self.next_now_ms())
                    .made_by(made_by)
                    .cells(cells),
            ),
        )?;
        let tx_id = published.tx_id;
        crate::db::block_on(
            self.finish_publication_outcome(PublicationOutcome::published((), published)),
        )?;
        let outcome = crate::db::block_on(
            self.node
                .node
                .borrow_mut()
                .finalize_local_mergeable_commit(tx_id),
        )?;
        crate::db::block_on(self.finish_publication_outcome(outcome))?;
        self.node.mark_subscriber_connections_dirty();
        Ok(tx_id)
    }

    /// Test/bench-only authority finalization for a locally committed mergeable
    /// transaction.
    ///
    /// This allows scale fixtures to use the ordinary batched transaction API
    /// before performing the same self-acceptance step as
    /// [`Db::seed_settled_mergeable_for_bootstrap`].
    pub fn finalize_local_mergeable_commit_for_test(&self, tx_id: TxId) -> Result<(), Error> {
        let outcome = crate::db::block_on(
            self.node
                .node
                .borrow_mut()
                .finalize_local_mergeable_commit(tx_id),
        )?;
        crate::db::block_on(self.finish_publication_outcome(outcome))?;
        self.node.mark_subscriber_connections_dirty();
        Ok(())
    }

    /// Return the locally observed fate and durability for a write transaction.
    pub fn write_state(&self, tx_id: TxId) -> Result<WriteState, Error> {
        let Some((fate, global_time, durability)) =
            crate::db::block_on(self.node.node.borrow_mut().transaction_state(tx_id))
        else {
            return Err(Error::new(
                ErrorCode::NotObserved,
                format!("transaction {tx_id:?} is not known locally"),
            ));
        };
        Ok(WriteState {
            fate,
            global_time,
            durability,
        })
    }

    /// Wait until `tx_id` reaches `tier` or is rejected.
    ///
    /// An explicit wait consumes a rejection, preventing the same failure from
    /// subsequently being delivered through [`Db::on_mutation_error`]. The
    /// check/register/recheck sequence keeps that ownership decision inside
    /// the database and closes the race with an already-observed rejection.
    pub async fn wait_for_transaction(
        &self,
        tx_id: TxId,
        tier: DurabilityTier,
    ) -> Result<TxId, Error> {
        loop {
            if let Some(outcome) = self.node.transaction_wait_outcome(tx_id, tier) {
                return outcome;
            }
            let state_change = self.node.register_write_state_waiter(tx_id);
            if let Some(outcome) = self.node.transaction_wait_outcome(tx_id, tier) {
                drop(state_change);
                return outcome;
            }
            state_change.await;
        }
    }

    /// Callback form of [`Db::wait_for_transaction`] for bindings that cannot
    /// drive a thread-affine Rust future directly.
    pub fn wait_for_transaction_with(
        &self,
        tx_id: TxId,
        tier: DurabilityTier,
        callback: impl FnOnce(Result<TxId, Error>) + 'static,
    ) {
        self.node
            .wait_for_transaction_with(tx_id, tier, Box::new(callback));
    }

    /// Wait until this database observes another state transition for `tx_id`.
    ///
    /// Callers should always check [`Db::write_state`] before and after
    /// registering this future; this method is a wake primitive, not a predicate.
    pub fn next_write_state_change(&self, tx_id: TxId) -> WriteStateChange {
        self.node.register_write_state_waiter(tx_id)
    }

    /// Register the binding callback for rejected local transactions that no
    /// active application waiter consumed.
    pub fn on_mutation_error(&self, callback: MutationErrorCallback) {
        self.node.set_mutation_error_callback(Some(callback));
    }

    /// Remove the current mutation-error callback.
    pub fn clear_mutation_error_callback(&self) {
        self.node.set_mutation_error_callback(None);
    }

    /// Attach this `Db` to an upstream peer over a binding-supplied transport.
    ///
    /// The returned [`PeerConnection`] carries this Db's subscriptions upstream
    /// under this Db's own identity and applies the updates that come back.
    /// An unfated commit unit is interpreted from this receiving Db's role: an
    /// ordinary Local Db records it as Pending/Local, while the structurally
    /// separate history-complete path remains the Core authority.
    /// The binding drives it by calling [`PeerConnection::tick`] (or
    /// [`Db::tick`]) whenever it has staged inbound bytes or wants to flush.
    pub async fn connect_upstream(
        &self,
        transport: Box<dyn Transport>,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.node.connect_upstream(transport).await
    }

    /// Install or clear the scheduler used to wake this database's live peer
    /// connections when local writes, subscription registrations, or transport
    /// events create sync work.
    pub fn set_tick_scheduler(&self, scheduler: Option<Rc<dyn TickScheduler>>) {
        self.node.set_scheduler(scheduler);
    }

    /// Configure automatic edge-cache byte-budget eviction.
    ///
    /// `None` disables automatic eviction and preserves the historical manual
    /// `evict_cold` behavior.
    pub fn set_edge_cache_budget(&self, budget: Option<EdgeCacheBudget>) {
        self.node.set_edge_cache_budget(budget);
    }

    /// Ask the installed scheduler to service pending peer-connection work.
    pub fn schedule_tick(&self, urgency: TickUrgency) {
        self.node.schedule_tick(urgency);
    }

    /// Request a one-shot permission decision from the authenticated upstream
    /// serving authority. Dropping the returned future cancels local delivery;
    /// late or replayed responses are ignored by request id.
    pub fn request_permission_advice(
        &self,
        action: PermissionAdviceAction,
    ) -> PermissionAdviceFuture {
        self.node.request_permission_advice(action)
    }

    /// Resolve outstanding permission preflights as `Unknown` and suppress
    /// requests that have not reached the transport yet.
    pub fn cancel_permission_advice_request(&self, request_id: PermissionAdviceRequestId) {
        self.node.cancel_permission_advice_request(request_id);
    }

    /// Accept a subscriber connection served under `identity`.
    ///
    /// The accepting Db owns the ingestion semantics. A Local Db persists
    /// unfated commits as Pending/Local and forwards them upstream; a
    /// history-complete Db applies Core authority semantics.
    pub fn accept_subscriber(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.node.accept_subscriber(transport, identity)
    }

    /// Accept a subscriber connection served under `identity` with auth claims.
    pub fn accept_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.node
            .accept_subscriber_with_claims(transport, identity, claims)
    }

    /// Accept a subscriber connection with explicit auth claims and upload trust mode.
    pub fn accept_subscriber_with_claims_and_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.node
            .accept_subscriber_with_claims_and_trust(transport, identity, claims, trust)
    }

    /// Accept an edge-terminated subscriber with session claims.
    pub fn accept_edge_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.node
            .accept_edge_subscriber_with_claims(transport, identity, claims)
    }

    /// Accept a subscriber whose host shell is wired as an edge fate authority.
    pub fn accept_edge_authority_subscriber_with_claims_and_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.node
            .accept_edge_authority_subscriber_with_claims_and_trust(
                transport, identity, claims, trust,
            )
    }

    /// Accept a reconnecting subscriber, resuming from a previous cursor.
    pub fn accept_subscriber_with_resume(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        cursor: ResumeCursor,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.node
            .accept_subscriber_with_resume(transport, identity, cursor)
    }

    /// Detach a previously attached peer connection from this database.
    pub fn detach_connection(&self, connection: &Rc<LocalMutex<PeerConnection<S>>>) -> bool {
        self.node.detach_connection(connection)
    }

    /// Service every connection once (a convenience over
    /// [`PeerConnection::tick`] for the common single-upstream client).
    pub async fn tick(&self) -> Result<(), Error> {
        self.node.drain_subscription_finalizations().await?;
        self.node.settle_local_publications().await?;
        self.node.tick().await.map(|_| ())
    }

    /// Service every connection once and return binding-observable wake counts.
    pub async fn tick_stats(&self) -> Result<DbTickStats, Error> {
        self.node.drain_subscription_finalizations().await?;
        self.node.settle_local_publications().await?;
        self.node.tick().await
    }

    pub(super) async fn refresh_subscriptions(&self) -> Result<usize, Error> {
        let refreshed = self.node.refresh_subscriptions().await?;
        if refreshed > 0 {
            self.node.mark_subscriber_connections_dirty();
        }
        Ok(refreshed)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only history-class byte estimate. This is intentionally the
    /// cheap physical-class counter, not a logical table-prefix scan.
    pub async fn history_class_bytes_for_test(&self) -> Result<Option<u64>, Error> {
        Ok(self
            .node
            .node
            .lock()
            .await
            .history_class_bytes_for_test()
            .await?)
    }

    /// Apply an in-memory-only mutation to the compiled current schema.
    ///
    /// The database must already have opened from a valid public source schema.
    /// This exists only for tests whose subject is an intentionally invalid
    /// lowered state; the mutation is never persisted or published.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub fn mutate_current_compiled_schema_for_test(
        &self,
        mutate: impl FnOnce(&mut crate::schema::RuntimeSchema),
    ) {
        self.node
            .node
            .borrow_mut()
            .mutate_current_schema_for_testing(mutate);
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only encoded storage byte estimate across Jazz physical
    /// classes.
    pub async fn encoded_storage_bytes_for_test(&self) -> Result<u64, Error> {
        Ok(self
            .node
            .node
            .lock()
            .await
            .encoded_storage_bytes_for_test()
            .await?)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only durability boundary for harnesses that reopen the same
    /// storage path immediately after a synthetic lifecycle transition.
    pub async fn flush_for_test(&self) -> Result<(), Error> {
        Ok(self.node.node.lock().await.drive_query_runtime().await?)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only reset for logical storage-read attribution.
    pub fn reset_storage_read_metrics_for_test(&self) {
        self.node.node.borrow().reset_storage_read_metrics();
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only drain for logical storage-read attribution.
    pub fn take_storage_read_metrics_for_test(&self) -> groove::db::StorageReadMetrics {
        self.node.node.borrow().take_storage_read_metrics()
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only snapshot of sync-path counters.
    pub fn sync_metrics_for_test(&self) -> crate::node::SyncMetrics {
        self.node.node.borrow().sync_metrics().clone()
    }

    #[cfg(any(test, feature = "testing"))]
    /// Test/bench-only runtime diagnostics used by performance receipts.
    pub fn runtime_stats_for_test(&self) -> groove::ivm::RuntimeStats {
        self.node.node.borrow().runtime_stats_for_test()
    }

    #[cfg(feature = "testing")]
    /// Test-only count of relay-owned upstream usage sites. This deliberately
    /// counts wire owners rather than coverage evaluators, so reconnect tests
    /// can prove a detached downstream session left no orphaned owner behind.
    pub fn relay_upstream_subscription_owner_count_for_test(&self) -> usize {
        self.node.relay_upstream_subscription_owners.borrow().len()
    }

    #[cfg(feature = "testing")]
    /// Test-only count of relay-registered downstream wire usage sites.
    pub fn relay_registered_query_binding_count_for_test(&self) -> usize {
        self.node
            .node
            .borrow()
            .registered_query_binding_count_for_test()
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only maintained subscription sizing diagnostics used by
    /// warm-cache performance receipts.
    pub fn maintained_subscription_size_receipts_for_test(
        &self,
    ) -> Vec<MaintainedSubscriptionSizeReceipt> {
        self.node
            .subscriptions
            .borrow()
            .iter()
            .filter_map(Weak::upgrade)
            .filter_map(|state| {
                let state = state.borrow();
                let SubscriptionKind::Prepared {
                    shape,
                    binding,
                    maintained_subscription,
                } = &state.kind;
                let maintained_subscription = maintained_subscription.as_ref()?;
                let snapshot = &state.snapshot;
                let snapshot_bytes = encode_relation_snapshot_for_size(snapshot)
                    .map(|bytes| bytes.len())
                    .unwrap_or_default();
                let reset_frame_bytes = encode_subscription_reset_frame_for_size(
                    state.read_tier,
                    state.settled,
                    snapshot,
                )
                .map(|bytes| bytes.len())
                .unwrap_or_default();
                Some(MaintainedSubscriptionSizeReceipt {
                    name: shape.query().table.clone(),
                    shape_id: shape.shape_id().0,
                    binding_id: binding.binding_id().0,
                    rows: snapshot.rows.len(),
                    root_rows: snapshot.root_count,
                    relation_edges: snapshot.edges.len(),
                    footprint: DbMaintainedSubscriptionFootprint::from_local(
                        maintained_subscription.footprint(),
                    ),
                    snapshot_bytes,
                    reset_frame_bytes,
                    validation_tuple_estimate_bytes: validation_tuple_estimate_bytes(
                        shape,
                        binding,
                        state.author,
                        state.read_tier,
                        &state.read_view,
                    ),
                })
            })
            .collect()
    }
}

fn schema_policy_metadata_matches(left: &JazzSchema, right: &JazzSchema) -> bool {
    left.tables.len() == right.tables.len()
        && left.tables.iter().all(|left_table| {
            right.tables.iter().any(|right_table| {
                left_table.name == right_table.name
                    && left_table.read_policy == right_table.read_policy
                    && left_table.write_policies == right_table.write_policies
            })
        })
}

fn schema_index_metadata_matches(left: &JazzSchema, right: &JazzSchema) -> bool {
    left.tables.len() == right.tables.len()
        && left.tables.iter().all(|left_table| {
            right.tables.iter().any(|right_table| {
                left_table.name == right_table.name
                    && left_table.indexed_columns == right_table.indexed_columns
            })
        })
}

#[cfg(feature = "testing")]
#[derive(Clone, Debug, PartialEq, Eq)]
/// Test/bench-only sizing receipt for one active maintained subscription.
pub struct MaintainedSubscriptionSizeReceipt {
    /// Debug label for the subscription, currently the root query table.
    pub name: String,
    /// Stable query shape id.
    pub shape_id: uuid::Uuid,
    /// Stable binding id.
    pub binding_id: uuid::Uuid,
    /// Materialized snapshot row count, including related rows.
    pub rows: usize,
    /// Materialized root row count.
    pub root_rows: usize,
    /// Materialized relation/include edge count.
    pub relation_edges: usize,
    /// Approximate maintained-view and local control-state footprint.
    pub footprint: DbMaintainedSubscriptionFootprint,
    /// Postcard bytes for the materialized relation snapshot shape used by native runtimes.
    pub snapshot_bytes: usize,
    /// Postcard bytes for the native reset delta row payload.
    pub reset_frame_bytes: usize,
    /// Estimated validation tuple bytes for a future warm-cache key.
    pub validation_tuple_estimate_bytes: usize,
}

#[cfg(feature = "testing")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
/// Test/bench-only approximate heap footprint for a maintained subscription.
pub struct DbMaintainedSubscriptionFootprint {
    /// Active result-current rows in the maintained index.
    pub result_rows: usize,
    /// Result weight map entries, including non-positive transient entries.
    pub result_weights: usize,
    /// Result payload map entries retained for projected/synthetic output.
    pub result_payloads: usize,
    /// Active readable version identities retained by full record identity.
    pub version_identities: usize,
    /// Entries reachable through the version-by-transaction index.
    pub version_tx_entries: usize,
    /// Active replacement winner entries across content and deletion maps.
    pub replacement_entries: usize,
    /// Approximate heap bytes retained by result_weights.
    pub result_weights_bytes: usize,
    /// Approximate heap bytes retained by result_payloads.
    pub result_payloads_bytes: usize,
    /// Approximate heap bytes retained by WeightedVersionIndex.
    pub versions_bytes: usize,
    /// Approximate heap bytes retained by ReplacementIndex.
    pub replacements_bytes: usize,
    /// Approximate heap bytes retained by maintained-view indexes.
    pub maintained_heap_bytes: usize,
    /// Lowered terminal schema count.
    pub terminal_schemas: usize,
    /// Approximate heap bytes retained by terminal schemas.
    pub terminal_schemas_bytes: usize,
    /// Table schema count retained by the local subscription.
    pub tables: usize,
    /// Local result-set member count.
    pub result_set: usize,
    /// Local result payload count.
    pub local_result_payloads: usize,
    /// Local program fact count.
    pub program_facts: usize,
    /// Approximate heap bytes retained by local subscription control state.
    pub control_state_bytes: usize,
    /// Approximate maintained plus local control-state heap bytes.
    pub total_heap_bytes: usize,
}

#[cfg(feature = "testing")]
impl DbMaintainedSubscriptionFootprint {
    fn from_local(footprint: crate::node::LocalMaintainedViewSubscriptionFootprint) -> Self {
        Self {
            result_rows: footprint.maintained.result_rows,
            result_weights: footprint.maintained.result_weights,
            result_payloads: footprint.maintained.result_payloads,
            version_identities: footprint.maintained.version_identities,
            version_tx_entries: footprint.maintained.version_tx_entries,
            replacement_entries: footprint.maintained.replacement_entries,
            result_weights_bytes: footprint.maintained.result_weights_bytes,
            result_payloads_bytes: footprint.maintained.result_payloads_bytes,
            versions_bytes: footprint.maintained.versions_bytes,
            replacements_bytes: footprint.maintained.replacements_bytes,
            maintained_heap_bytes: footprint.maintained.total_heap_bytes,
            terminal_schemas: footprint.terminal_schemas.terminal_schemas,
            terminal_schemas_bytes: footprint.terminal_schemas.terminal_schemas_bytes,
            tables: footprint.tables,
            result_set: footprint.result_set,
            local_result_payloads: footprint.result_payloads,
            program_facts: footprint.program_facts,
            control_state_bytes: footprint.control_state_bytes,
            total_heap_bytes: footprint.total_heap_bytes,
        }
    }
}

#[cfg(feature = "testing")]
#[derive(serde::Serialize)]
struct SizeRelationSnapshot<'a> {
    root_count: u64,
    rows: Vec<SizeRowBatch<'a>>,
}

#[cfg(feature = "testing")]
#[derive(serde::Serialize)]
struct SizeSubscriptionDelta<'a> {
    added: Vec<SizeRowBatch<'a>>,
    updated: Vec<SizeRowBatch<'a>>,
    removed: Vec<SizeRemovedRow>,
}

#[cfg(feature = "testing")]
#[derive(serde::Serialize)]
struct SizeRowBatch<'a> {
    table: &'a str,
    descriptor: groove::records::RecordDescriptor,
    rows: Vec<SizeRow<'a>>,
}

#[cfg(feature = "testing")]
#[derive(serde::Serialize)]
struct SizeRow<'a> {
    row_id: RowUuid,
    deleted: bool,
    raw: &'a [u8],
}

#[cfg(feature = "testing")]
#[derive(serde::Serialize)]
struct SizeRemovedRow {
    table: String,
    row_id: RowUuid,
}

#[cfg(feature = "testing")]
fn encode_relation_snapshot_for_size(
    snapshot: &RelationSnapshot,
) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&SizeRelationSnapshot {
        root_count: snapshot.root_count as u64,
        rows: size_row_batches(&snapshot.rows),
    })
}

#[cfg(feature = "testing")]
fn encode_subscription_reset_frame_for_size(
    _tier: DurabilityTier,
    _settled: bool,
    snapshot: &RelationSnapshot,
) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&SizeSubscriptionDelta {
        added: size_row_batches(&snapshot.rows),
        updated: Vec::new(),
        removed: Vec::new(),
    })
}

#[cfg(feature = "testing")]
fn size_row_batches(rows: &[CurrentRow]) -> Vec<SizeRowBatch<'_>> {
    let mut batches = Vec::<SizeRowBatch<'_>>::new();
    for row in rows {
        let (descriptor, raw) = row.encoded_record();
        match batches.last_mut() {
            Some(batch) if batch.table == row.table() && batch.descriptor == *descriptor => {
                batch.rows.push(size_row(row, raw));
            }
            _ => batches.push(SizeRowBatch {
                table: row.table(),
                descriptor: *descriptor,
                rows: vec![size_row(row, raw)],
            }),
        }
    }
    batches
}

#[cfg(feature = "testing")]
fn size_row<'a>(row: &CurrentRow, raw: &'a [u8]) -> SizeRow<'a> {
    SizeRow {
        row_id: row.row_uuid(),
        deleted: row.is_deleted(),
        raw,
    }
}

#[cfg(feature = "testing")]
fn validation_tuple_estimate_bytes(
    shape: &ValidatedQuery,
    binding: &Binding,
    author: AuthorSubject,
    tier: DurabilityTier,
    read_view: &ReadViewSpec,
) -> usize {
    #[derive(serde::Serialize)]
    struct ValidationTuple<'a> {
        shape_id: uuid::Uuid,
        binding_id: uuid::Uuid,
        schema_version: SchemaVersionId,
        canonical_query: &'a [u8],
        canonical_binding: &'a [u8],
        author: AuthorSubject,
        tier: DurabilityTier,
        read_view: &'a ReadViewSpec,
    }

    postcard::to_allocvec(&ValidationTuple {
        shape_id: shape.shape_id().0,
        binding_id: binding.binding_id().0,
        schema_version: shape.schema_version(),
        canonical_query: shape.canonical_bytes(),
        canonical_binding: binding.canonical_bytes(),
        author,
        tier,
        read_view,
    })
    .map(|bytes| bytes.len())
    .unwrap_or_default()
}
