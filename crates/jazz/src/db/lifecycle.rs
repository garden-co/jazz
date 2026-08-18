//! Database construction, schema views, write-state waiting, and connection controls.

use super::mutations::MutationPrepareError;
use super::subscriptions::SubscriptionOpenError;
use super::*;
use crate::node::{DemandDrivenNode, PollableNodeOpen};

/// Pollable construction for a high-level database backed by ordered async
/// storage. The resulting owner keeps the familiar synchronous `Db` facade
/// and its durable runtime together.
#[doc(hidden)]
pub struct PollableDbOpen {
    opening: Option<PollableNodeOpen>,
    runtime: Option<DemandDrivenNode>,
    schema: JazzSchema,
    identity: DbIdentity,
    id_source: Option<Box<dyn RowIdSource>>,
}

/// High-level database facade plus the async owner of its resident node.
#[doc(hidden)]
pub struct DemandDrivenDb {
    database: Db<groove::storage::DemandLoadedStorage>,
    runtime: DemandDrivenNode,
}

impl PollableDbOpen {
    #[doc(hidden)]
    pub fn new(
        schema: JazzSchema,
        identity: DbIdentity,
        persistence: Box<dyn groove::storage::async_ordered::OrderedKvStorage>,
    ) -> Self {
        Self {
            opening: Some(PollableNodeOpen::new(
                identity.node,
                schema.clone(),
                persistence,
            )),
            runtime: None,
            schema,
            identity,
            id_source: None,
        }
    }

    #[doc(hidden)]
    pub fn with_id_source(mut self, id_source: impl RowIdSource + 'static) -> Self {
        self.id_source = Some(Box::new(id_source));
        self
    }

    #[doc(hidden)]
    pub fn poll(&mut self, context: &mut Context<'_>) -> Poll<Result<DemandDrivenDb, Error>> {
        if self.runtime.is_none() {
            let runtime: DemandDrivenNode = match self
                .opening
                .as_mut()
                .expect("incomplete database opening retains node opening")
                .poll(context)
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                Poll::Ready(Ok(runtime)) => runtime,
            };
            self.opening = None;
            self.runtime = Some(runtime);
        }
        let pending = match self
            .runtime
            .as_mut()
            .expect("database opening retains its ready runtime")
            .poll_pending_transaction_ids(context, self.identity.node, self.identity.author)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
            Poll::Ready(Ok(pending)) => pending,
        };
        let runtime = self
            .runtime
            .take()
            .expect("completed database opening takes its runtime");
        let node = Node::from_shared(runtime.shared_resident());
        node.set_non_durable_client();
        node.restore_prepared_pending_uploads(pending);
        let schema_version_id = self.schema.version_id();
        let schema_views = Rc::new(RefCell::new(BTreeMap::from([(
            SchemaViewId::for_schema(&self.schema),
            self.schema.clone(),
        )])));
        let database = Db {
            schema: self.schema.clone(),
            schema_version_id,
            schema_view_is_fixed: false,
            schema_views,
            identity: self.identity,
            node: Rc::new(node),
            row_id_source: Rc::new(RefCell::new(
                self.id_source
                    .take()
                    .unwrap_or_else(|| Box::<ProductionRowIdSource>::default()),
            )),
            next_now_ms: Rc::new(Cell::new(1)),
        };
        Poll::Ready(Ok(DemandDrivenDb { database, runtime }))
    }
}

impl DemandDrivenDb {
    /// Start a logical query without touching durable storage.
    pub fn table(&self, table: impl Into<String>) -> Query {
        self.database.table(table)
    }

    /// Compile a logical query. Durable source acquisition happens when the
    /// resulting query is read or subscribed, not while its shape is built.
    pub fn prepare_query(&self, query: &Query) -> Result<PreparedQuery, Error> {
        self.database.prepare_query(query)
    }

    pub fn write_state(&self, tx_id: TxId) -> Result<WriteState, Error> {
        self.database.write_state(tx_id)
    }

    #[cfg(test)]
    pub(crate) fn runtime_stats_for_test(&self) -> groove::ivm::RuntimeStats {
        self.database.runtime_stats_for_test()
    }

    #[doc(hidden)]
    pub fn poll_persistence(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Error>> {
        match self.runtime.poll_persistence(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => Poll::Ready(result.map_err(Into::into)),
        }
    }

    /// Attach an upstream whose durable ingress is owned by this asynchronous
    /// database rather than by the synchronous peer tick.
    pub fn connect_upstream(
        &self,
        transport: Box<dyn Transport>,
    ) -> Rc<RefCell<PeerConnection<groove::storage::DemandLoadedStorage>>> {
        let connection = self.database.connect_upstream(transport);
        connection.borrow_mut().enable_external_durable_ingress();
        connection
    }

    /// Drive peer work without replaying a frame across an asynchronous
    /// storage suspension. At most one durable frame owns the persistence
    /// boundary at a time; ordinary connection-control traffic continues
    /// through the resident peer tick.
    pub fn poll_tick(&mut self, context: &mut Context<'_>) -> Poll<Result<DbTickStats, Error>> {
        let connections = self.database.node.connections.borrow().clone();
        for connection in &connections {
            connection.borrow_mut().stage_available_inbound();
            let staged_catalogue = { connection.borrow().staged_catalogue_snapshot() };
            if let Some(snapshot) = staged_catalogue {
                match self
                    .runtime
                    .poll_apply_peer_catalogue_snapshot(context, &snapshot)
                {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                    Poll::Ready(Ok(())) => {
                        connection.borrow_mut().complete_staged_catalogue_snapshot()
                    }
                }
                continue;
            }
            let staged_branch_metadata = { connection.borrow().staged_branch_metadata() };
            if let Some(metadata) = staged_branch_metadata {
                match self
                    .runtime
                    .poll_apply_peer_branch_metadata(context, &metadata)
                {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                    Poll::Ready(Ok(())) => connection
                        .borrow_mut()
                        .complete_staged_branch_metadata(metadata.branch_id),
                }
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
            let staged_repair = { connection.borrow().staged_row_version_repair() };
            if let Some((requests, bundles)) = staged_repair {
                match self
                    .runtime
                    .poll_apply_peer_repair_payloads(context, &requests, &bundles)
                {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                    Poll::Ready(Ok(())) => {
                        connection.borrow_mut().complete_staged_row_version_repair()
                    }
                }
                // Completion re-stages the original ViewUpdate at the head of
                // this same link. Yield before the legacy node tick can consume
                // it; the next owner poll acquires and publishes it through the
                // typed receiver boundary.
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
            let relay = connection.borrow().staged_relay_commit();
            if let Some((tx, versions)) = relay {
                let tx_id = tx.tx_id;
                match self
                    .runtime
                    .poll_ingest_relay_commit_unit(context, tx, versions)
                {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                    Poll::Ready(Ok(())) => {
                        connection.borrow_mut().complete_staged_relay_commit(tx_id);
                    }
                }
                continue;
            }
            let accepted_fate = connection.borrow().staged_accepted_fate();
            if let Some((tx_id, global_seq, durability)) = accepted_fate {
                match self.runtime.poll_apply_peer_fate_update(
                    context,
                    tx_id,
                    Fate::Accepted,
                    global_seq,
                    durability,
                ) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                    Poll::Ready(Ok(())) => {
                        connection.borrow_mut().complete_staged_accepted_fate(tx_id)
                    }
                }
            }
            let staged_view = { connection.borrow().staged_ready_view_update() };
            if let Some((message, parts)) = staged_view {
                let missing = match self
                    .runtime
                    .poll_missing_peer_view_update_refs(context, &message)
                {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                    Poll::Ready(Ok(missing)) => missing,
                };
                if missing.is_empty() {
                    match self
                        .runtime
                        .poll_apply_peer_view_updates(context, std::slice::from_ref(&parts))
                    {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                        Poll::Ready(Ok(())) => {
                            connection.borrow_mut().complete_staged_view_update(&parts)
                        }
                    }
                    continue;
                }
            }
        }

        let stats = match self.database.node.tick() {
            Ok(stats) => stats,
            Err(error) => return Poll::Ready(Err(error)),
        };
        if connections.iter().any(|connection| {
            let connection = connection.borrow();
            connection.staged_catalogue_snapshot().is_some()
                || connection.staged_branch_metadata().is_some()
                || connection.staged_row_version_repair().is_some()
                || connection.staged_relay_commit().is_some()
                || connection.staged_accepted_fate().is_some()
        }) {
            context.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(Ok(stats))
        }
    }

    /// Write the clean-close marker, durably drain all earlier mutations, then
    /// flush and close the ordered backend. Consuming the owner makes this
    /// lifecycle transition unambiguously terminal.
    pub async fn close(mut self) -> Result<(), Error> {
        let database = &self.database;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || database.node.node.borrow_mut().close(),
                crate::node::missing_node_open_input,
            )
        })
        .await
        .map_err(Error::from)?;
        std::future::poll_fn(|context| self.runtime.poll_close(context))
            .await
            .map_err(Error::from)
    }

    /// Create a local snapshot-base branch through the ordered async owner.
    pub async fn create_branch(&mut self) -> Result<crate::ids::BranchId, Error> {
        let branch = crate::ids::BranchId(uuid::Uuid::now_v7());
        self.create_branch_with_id(branch).await?;
        Ok(branch)
    }

    /// Create a local snapshot-base branch with a caller-supplied stable id.
    pub async fn create_branch_with_id(
        &mut self,
        branch: crate::ids::BranchId,
    ) -> Result<(), Error> {
        let author = self.database.identity.author;
        std::future::poll_fn(|context| self.runtime.poll_create_branch(context, branch, author))
            .await
            .map(|_| ())
            .map_err(Error::from)
    }

    /// Insert into a branch overlay. A missing physical partition is prepared
    /// off-runtime, then its schema, durable marker, and first row publish in
    /// the same resolving poll.
    pub async fn insert_on_branch(
        &mut self,
        branch: crate::ids::BranchId,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        let (row_uuid, commit) = self.database.prepare_insert_commit(table, cells)?;
        let schema = self.database.schema_version_id;
        let tx_id = std::future::poll_fn(|context| {
            self.runtime.poll_mergeable_many_on_branch_in_schema(
                context,
                branch,
                schema,
                std::slice::from_ref(&commit),
            )
        })
        .await?;
        let local_tier = self.database.finalize_local_commit(tx_id)?;
        self.database.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.database.node.node),
            row_uuid,
            tx_id,
            local_tier,
        })
    }

    #[cfg(test)]
    pub(crate) fn resident_node_for_test(
        &self,
    ) -> Rc<RefCell<crate::node::NodeState<groove::storage::DemandLoadedStorage>>> {
        Rc::clone(&self.database.node.node)
    }

    /// Poll a high-level one-shot read through query-driven durable loading.
    ///
    /// If every required input is resident, this returns `Ready` on the first
    /// poll. A cold durable dependency suspends the operation, admits exactly
    /// that dependency, and retries the same resident evaluation.
    #[doc(hidden)]
    pub fn poll_all(
        &mut self,
        context: &mut Context<'_>,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Poll<Result<Vec<CurrentRow>, Error>> {
        if let Err(error) = ensure_supported_read_view(&opts) {
            return Poll::Ready(Err(error));
        }
        let database = &self.database;
        let author = database.identity.author;
        match self.runtime.poll_resident_operation(context, || {
            if let ReadViewSourceSpec::Branch { branch } = &opts.read_view.source {
                database.node.node.borrow_mut().acquire_branch_read_inputs(
                    &prepared.shape,
                    &prepared.binding,
                    crate::ids::BranchId(*branch),
                    author,
                    false,
                )?;
            }
            database.all_resident(
                &prepared,
                &opts,
                author,
                QueryAuthorizationMode::ClientLocal,
            )
        }) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => Poll::Ready(result.map_err(Into::into)),
        }
    }

    /// Run a high-level one-shot read, suspending only for cold durable input.
    #[doc(hidden)]
    pub async fn all(
        &mut self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        std::future::poll_fn(|context| self.poll_all(context, prepared, opts.clone())).await
    }

    /// Poll a structured relation snapshot through exact durable acquisition.
    #[doc(hidden)]
    pub fn poll_relation_snapshot(
        &mut self,
        context: &mut Context<'_>,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Poll<Result<RelationSnapshot, Error>> {
        if let Err(error) = ensure_supported_read_view(&opts) {
            return Poll::Ready(Err(error));
        }
        if opts.include_deleted {
            return Poll::Ready(Err(Error::new(
                ErrorCode::Query,
                "relation snapshots do not support include_deleted yet",
            )));
        }
        let database = &self.database;
        let author = database.identity.author;
        match self.runtime.poll_resident_operation(context, || {
            if let ReadViewSourceSpec::Branch { branch } = &opts.read_view.source {
                database.node.node.borrow_mut().acquire_branch_read_inputs(
                    &prepared.shape,
                    &prepared.binding,
                    crate::ids::BranchId(*branch),
                    author,
                    false,
                )?;
            }
            database.relation_snapshot_resident(
                prepared,
                &opts,
                author,
                QueryAuthorizationMode::ClientLocal,
            )
        }) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => Poll::Ready(result.map_err(Into::into)),
        }
    }

    /// Read a structured relation snapshot, suspending only for cold inputs.
    #[doc(hidden)]
    pub async fn relation_snapshot(
        &mut self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        std::future::poll_fn(|context| self.poll_relation_snapshot(context, prepared, opts.clone()))
            .await
    }

    /// Materialize the canonical public result tree from an acquired snapshot.
    #[doc(hidden)]
    pub async fn result_tree(
        &mut self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<ResultTree, Error> {
        let snapshot = self.relation_snapshot(prepared, opts).await?;
        materialize_result_tree(prepared.shape.query(), snapshot)
    }

    /// Poll a high-level subscription opening through query-driven durable
    /// loading. A suspended attempt cannot leave a registered Groove
    /// subscription or public Jazz subscription state behind.
    #[doc(hidden)]
    pub fn poll_subscribe(
        &mut self,
        context: &mut Context<'_>,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Poll<Result<SubscriptionStream, Error>> {
        let database = &self.database;
        let author = database.identity.author;
        match self.runtime.poll_operation(
            context,
            || {
                database.open_subscription_resident(
                    prepared,
                    opts.clone(),
                    author,
                    QueryAuthorizationMode::ClientLocal,
                )
            },
            SubscriptionOpenError::missing_input,
        ) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => Poll::Ready(result.map_err(SubscriptionOpenError::into_api)),
        }
    }

    /// Open a high-level subscription, suspending only for cold durable input.
    #[doc(hidden)]
    pub async fn subscribe(
        &mut self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<SubscriptionStream, Error> {
        std::future::poll_fn(|context| self.poll_subscribe(context, prepared, opts.clone())).await
    }

    /// Insert through the async owner. Durable prerequisites may suspend, but
    /// the resident publish, public subscription refresh, and returned handle
    /// all occur in the same resolving poll.
    #[doc(hidden)]
    pub async fn insert(
        &mut self,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        let (row_uuid, commit) = self.database.prepare_insert_commit(table, cells)?;
        let database = &self.database;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || database.acquire_insert_target_for_owner(table, row_uuid),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)?;
        let schema = self.database.schema_version_id;
        let tx_id = std::future::poll_fn(|context| {
            self.runtime
                .poll_mergeable_commit_in_schema(context, schema, &commit)
        })
        .await
        .map_err(Error::from)?;
        let local_tier = self.database.finalize_local_commit(tx_id)?;
        self.database.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.database.node.node),
            row_uuid,
            tx_id,
            local_tier,
        })
    }

    /// Update one row through typed acquisition followed by one resident
    /// publish. A cold existing row may suspend; no write is visible until the
    /// resolving poll, whose subscription refresh is synchronous.
    #[doc(hidden)]
    pub async fn update(
        &mut self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        if patch.is_empty() {
            let database = &self.database;
            let (tx_id, local_tier) = std::future::poll_fn(|context| {
                self.runtime.poll_operation(
                    context,
                    || database.prepare_noop_update_for_owner(table, row),
                    MutationPrepareError::missing_input,
                )
            })
            .await
            .map_err(MutationPrepareError::into_api)?;
            return Ok(WriteHandle {
                node: Rc::downgrade(&self.database.node.node),
                row_uuid: row,
                tx_id,
                local_tier,
            });
        }
        let now_ms = self.database.next_now_ms();
        let database = &self.database;
        let prepared = std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || database.prepare_update_commit_for_owner(table, row, patch.clone(), now_ms),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)?;
        let schema = self.database.schema_version_id;
        let tx_id = std::future::poll_fn(|context| {
            self.runtime
                .poll_mergeable_commit_in_schema(context, schema, &prepared)
        })
        .await
        .map_err(Error::from)?;
        let local_tier = self.database.finalize_local_commit(tx_id)?;
        self.database.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.database.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    /// Soft-delete one row through typed acquisition followed by one resident
    /// publish. Existing local observers see the deletion in the resolving
    /// poll, before asynchronous durability may complete.
    #[doc(hidden)]
    pub async fn delete(
        &mut self,
        table: &str,
        row: RowUuid,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        let now_ms = self.database.next_now_ms();
        let database = &self.database;
        let prepared = std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || database.prepare_delete_commit_for_owner(table, row, now_ms),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)?;
        let schema = self.database.schema_version_id;
        let tx_id = std::future::poll_fn(|context| {
            self.runtime
                .poll_mergeable_commit_in_schema(context, schema, &prepared)
        })
        .await
        .map_err(Error::from)?;
        let local_tier = self.database.finalize_local_commit(tx_id)?;
        self.database.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.database.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    /// Restore one deleted row as a two-write atomic transaction. The content
    /// version and restore witness become visible together in the resolving
    /// poll; neither is published while cold parents are still loading.
    #[doc(hidden)]
    pub async fn restore(
        &mut self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        let now_ms = self.database.next_now_ms();
        let database = &self.database;
        let prepared = std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || database.prepare_restore_commits_for_owner(table, row, cells.clone(), now_ms),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)?;
        let schema = self.database.schema_version_id;
        let tx_id = std::future::poll_fn(|context| {
            self.runtime
                .poll_mergeable_many_in_schema(context, schema, &prepared)
        })
        .await
        .map_err(Error::from)?;
        let local_tier = self.database.finalize_local_commit(tx_id)?;
        self.database.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.database.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    /// Insert or update one caller-selected row id through the common typed
    /// acquisition boundary. The absent/existing decision is completed before
    /// the single resident publication.
    #[doc(hidden)]
    pub async fn upsert(
        &mut self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        let now_ms = self.database.next_now_ms();
        let database = &self.database;
        let prepared = std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || database.prepare_upsert_commit_for_owner(table, row, cells.clone(), now_ms),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)?;
        let schema = self.database.schema_version_id;
        let tx_id = std::future::poll_fn(|context| {
            self.runtime
                .poll_mergeable_commit_in_schema(context, schema, &prepared)
        })
        .await
        .map_err(Error::from)?;
        let local_tier = self.database.finalize_local_commit(tx_id)?;
        self.database.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.database.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    /// Open a staged mergeable transaction owned by this async database.
    pub async fn begin_mergeable(&mut self) -> Result<OpenBatchId, Error> {
        let id = OpenBatchId::new();
        let database = &self.database;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || database.begin_mergeable_for_owner(id),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)?;
        Ok(id)
    }

    /// Stage an insert in an open mergeable transaction. Staged writes remain
    /// invisible until [`DemandDrivenDb::commit_mergeable`] publishes them.
    pub async fn mergeable_insert(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<(), Error> {
        let now_ms = self.database.next_now_ms();
        let database = &self.database;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    database.stage_mergeable_insert_for_owner(
                        tx_id,
                        table,
                        row,
                        cells.clone(),
                        now_ms,
                    )
                },
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
    }

    pub async fn mergeable_update(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<(), Error> {
        let now_ms = self.database.next_now_ms();
        let database = &self.database;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    database.stage_mergeable_update_for_owner(
                        tx_id,
                        table,
                        row,
                        patch.clone(),
                        now_ms,
                    )
                },
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
    }

    pub async fn mergeable_delete(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
    ) -> Result<(), Error> {
        let now_ms = self.database.next_now_ms();
        let database = &self.database;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || database.stage_mergeable_delete_for_owner(tx_id, table, row, now_ms),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
    }

    pub async fn mergeable_restore(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<(), Error> {
        let now_ms = self.database.next_now_ms();
        let database = &self.database;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    database.stage_mergeable_restore_for_owner(
                        tx_id,
                        table,
                        row,
                        cells.clone(),
                        now_ms,
                    )
                },
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
    }

    /// Read through an open transaction's private overlay. Query source
    /// acquisition is driven by the normalized source set before lowering.
    pub async fn transaction_all(
        &mut self,
        tx_id: OpenBatchId,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        let database = &self.database;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || database.transaction_all_for_owner(tx_id, prepared, opts.clone()),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
    }

    /// Publish every staged write as one resident and durable transaction.
    pub async fn commit_mergeable(&mut self, tx_id: OpenBatchId) -> Result<TxId, Error> {
        let fallback_now_ms = self.database.next_now_ms();
        let committed = std::future::poll_fn(|context| {
            self.runtime
                .poll_mergeable_open(context, tx_id, fallback_now_ms)
        })
        .await
        .map_err(Error::from)?;
        self.database.finalize_local_commit(committed)?;
        self.database.refresh_subscriptions()?;
        Ok(committed)
    }

    /// Abandon a staged transaction without publishing any of its writes.
    pub fn abandon_mergeable(&mut self, tx_id: OpenBatchId) -> Result<(), Error> {
        self.database.abandon_transaction_handle(tx_id)
    }

    /// Open a serializable transaction over the current local snapshot.
    pub async fn begin_exclusive(&mut self) -> Result<OpenBatchId, Error> {
        let id = OpenBatchId::new();
        let database = &self.database;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || database.begin_exclusive_for_owner(id),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)?;
        Ok(id)
    }

    pub async fn exclusive_read(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<RowCells>, Error> {
        let database = &self.database;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || database.exclusive_read_for_owner(tx_id, table, row),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
    }

    pub async fn exclusive_insert(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<(), Error> {
        let now_ms = self.database.next_now_ms();
        let database = &self.database;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    database.stage_exclusive_insert_for_owner(
                        tx_id,
                        table,
                        row,
                        cells.clone(),
                        now_ms,
                    )
                },
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
    }

    pub async fn exclusive_update(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<(), Error> {
        let mut cells = self
            .exclusive_read(tx_id, table, row)
            .await?
            .unwrap_or_default();
        cells.extend(patch);
        self.exclusive_insert(tx_id, table, row, cells).await
    }

    pub async fn exclusive_delete(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
    ) -> Result<(), Error> {
        let now_ms = self.database.next_now_ms();
        let database = &self.database;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || database.stage_exclusive_delete_for_owner(tx_id, table, row, now_ms),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
    }

    pub async fn exclusive_restore(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<(), Error> {
        let now_ms = self.database.next_now_ms();
        let database = &self.database;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    database.stage_exclusive_restore_for_owner(
                        tx_id,
                        table,
                        row,
                        cells.clone(),
                        now_ms,
                    )
                },
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
    }

    /// Revalidate the fixed snapshot, then atomically publish the exclusive
    /// transaction without consuming its handle during cold acquisition.
    pub async fn commit_exclusive(&mut self, tx_id: OpenBatchId) -> Result<TxId, Error> {
        let now_ms = self.database.next_now_ms();
        let made_by = self.database.identity.author;
        let (committed, unit) = std::future::poll_fn(|context| {
            self.runtime
                .poll_exclusive_open(context, tx_id, made_by, now_ms)
        })
        .await
        .map_err(Error::from)?;
        self.database
            .finalize_local_exclusive_unit(committed, unit)?;
        self.database.refresh_subscriptions()?;
        Ok(committed)
    }

    pub fn abandon_exclusive(&mut self, tx_id: OpenBatchId) -> Result<(), Error> {
        self.database.abandon_exclusive_handle(tx_id)
    }
}

impl<S> Db<S>
where
    S: ResidentStorage + ReopenableStorage + 'static,
{
    /// Open a database over the supplied storage and recover local state.
    ///
    /// ```rust
    /// # use jazz::db::{Db, DbConfig, DbIdentity, SeededRowIdSource};
    /// # use jazz::db::doctest_support::{block_on, schema, MemoryStorage};
    /// # use jazz::ids::{AuthorId, NodeUuid};
    /// let schema = schema();
    /// let column_families = schema.column_families();
    /// let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    /// let storage = MemoryStorage::new(&refs);
    ///
    /// let db = block_on(Db::open(DbConfig {
    ///     schema,
    ///     storage,
    ///     identity: DbIdentity {
    ///         node: NodeUuid::from_bytes([1; 16]),
    ///         author: AuthorId::from_bytes([2; 16]),
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
        let node = NodeState::new(config.identity.node, config.schema.clone(), config.storage)?;
        let node = Node::new(node);
        node.restore_pending_uploads(config.identity)?;
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
            next_now_ms: Rc::new(Cell::new(1)),
        })
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
        )?;
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
            next_now_ms: Rc::new(Cell::new(1)),
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
        )?;
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
            next_now_ms: Rc::new(Cell::new(1)),
        })
    }

    /// Open an edge whose durable store has no authority catalogue yet.
    ///
    /// This is deliberately narrower than [`Db::open`]: callers may only use
    /// it to receive one connection-authenticated catalogue snapshot and then
    /// select one of the snapshot's admitted schema views.  Until then the
    /// node has no application schema and rejects ordinary data/sync work.
    #[cfg(any(feature = "runtime", test))]
    pub(crate) async fn open_catalogue_uninitialized_edge(
        config: DbConfig<S>,
    ) -> Result<Self, Error> {
        let bootstrap_schema = JazzSchema::new([]);
        let schema_version_id = bootstrap_schema.version_id();
        let schema_views = Rc::new(RefCell::new(BTreeMap::from([(
            SchemaViewId::for_schema(&bootstrap_schema),
            bootstrap_schema.clone(),
        )])));
        let node = NodeState::new_catalogue_uninitialized(config.identity.node, config.storage)?;
        let node = Node::new(node);
        node.restore_pending_uploads(config.identity)?;
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
            next_now_ms: Rc::new(Cell::new(1)),
        })
    }

    /// Install a complete catalogue received over the authenticated upstream
    /// bootstrap link.  This is intentionally crate-private: ordinary wire
    /// dispatch must never turn an arbitrary peer's snapshot into authority.
    #[cfg(any(feature = "runtime", test))]
    pub(crate) fn apply_trusted_catalogue_snapshot(
        &self,
        snapshot: crate::protocol::CatalogueSnapshot,
    ) -> Result<(), Error> {
        Ok(self
            .node
            .node
            .borrow_mut()
            .apply_trusted_catalogue_snapshot(snapshot)?)
    }

    #[cfg(any(test, feature = "testing"))]
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
    #[cfg(any(feature = "runtime", test))]
    pub(crate) fn trusted_catalogue_snapshot(
        &self,
    ) -> Result<crate::protocol::CatalogueSnapshot, Error> {
        Ok(self.node.node.borrow().catalogue_snapshot()?)
    }

    /// Return the active authority-admitted schema, failing closed when this
    /// dynamic edge still has no bootstrap receipt.
    #[cfg(any(feature = "runtime", test))]
    pub(crate) fn trusted_current_catalogue_schema(&self) -> Result<JazzSchema, Error> {
        let node = self.node.node.borrow();
        let pointer = node.current_write_schema()?;
        node.catalogue_schemas()
            .get(&pointer.schema)
            .map(|schema| schema.schema.clone())
            .ok_or_else(|| Error::new(ErrorCode::Schema, "active catalogue schema is missing"))
    }

    #[cfg(any(feature = "runtime", test))]
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
    pub fn register_schema_view(&self, schema: JazzSchema) -> Result<Self, Error> {
        let schema_version_id = schema.version_id();
        let schema_view_id = SchemaViewId::for_schema(&schema);
        self.admit_local_schema_view_if_needed(&schema)?;
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
            next_now_ms: Rc::clone(&self.next_now_ms),
        })
    }

    /// Attach an already-registered typed schema view to this owner.
    pub fn schema_view(&self, schema_view_id: SchemaViewId) -> Result<Self, Error> {
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
        self.register_schema_view(schema)
    }

    /// Canonical id of this handle's typed schema view.
    pub fn schema_view_id(&self) -> SchemaViewId {
        SchemaViewId::for_schema(&self.schema)
    }

    /// Admit the first application schema into an owner deliberately opened
    /// with the empty schema. This is the local-first bootstrap equivalent of
    /// having opened the runtime with that schema originally; later schemas
    /// still arrive through ordinary catalogue lineage publication.
    fn admit_local_schema_view_if_needed(&self, schema: &JazzSchema) -> Result<(), Error> {
        let empty_schema = JazzSchema::new([]);
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
        let publication = SchemaLineagePublication::new(
            SchemaVersion::new(schema.clone()),
            lens,
            new_tables,
            dropped_tables,
        );
        let mut node = self.node.node.borrow_mut();
        node.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq,
            publication: Box::new(publication),
        })?;
        if bootstrap_current {
            node.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
                author: AuthorId::SYSTEM,
                pointer: CurrentWriteSchema {
                    revision: 1,
                    schema: target_id,
                },
            })?;
        }
        Ok(())
    }

    /// Flush node-local maintenance state, write a clean-close marker, and
    /// close the underlying storage.
    pub fn close(&self) -> Result<(), Error> {
        if self.schema_view_is_fixed {
            return Ok(());
        }
        Ok(self.node.node.borrow_mut().close()?)
    }

    /// Configure this database as the optimistic, non-durable side of a
    /// browser client/worker pair. This must be called before application
    /// writes begin.
    pub fn set_non_durable_client(&self) {
        self.node.set_non_durable_client();
    }

    /// Configure this client database's first-snapshot durability cadence.
    ///
    /// Servers do not call this client-only setting and retain their existing
    /// storage durability behavior.
    pub fn set_initial_sync_flush_cadence(
        &self,
        cadence: InitialSyncFlushCadence,
    ) -> Result<(), Error> {
        Ok(self
            .node
            .node
            .borrow_mut()
            .set_initial_sync_flush_cadence(cadence.writes())?)
    }

    /// Create a snapshot-base branch immediately in local durable storage.
    ///
    /// Branch creation is local-first: no serving node round trip is required.
    /// The authenticated database identity is recorded as the immutable creator.
    pub fn create_branch(&self) -> Result<crate::ids::BranchId, Error> {
        let branch = crate::ids::BranchId(uuid::Uuid::now_v7());
        self.create_branch_with_id(branch)?;
        Ok(branch)
    }

    /// Create a local snapshot-base branch with a caller-supplied stable id.
    pub fn create_branch_with_id(&self, branch: crate::ids::BranchId) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .create_branch_as(branch, self.identity.author)?;
        Ok(())
    }

    /// Insert a row into a locally-created branch and queue it for ordinary sync.
    pub fn insert_on_branch(
        &self,
        branch: crate::ids::BranchId,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let row = self.row_id_source.borrow_mut().next_row_id();
        let cells = self.apply_insert_defaults(table, cells)?;
        let tx_id = self
            .node
            .node
            .borrow_mut()
            .commit_mergeable_on_branch_in_schema(
                branch,
                self.schema_version_id,
                MergeableCommit::new(table, row, self.next_now_ms())
                    .made_by(self.identity.author)
                    .cells(cells),
            )?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
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
        made_by: AuthorId,
        cells: RowCells,
    ) -> Result<TxId, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        let tx_id = self.node.node.borrow_mut().commit_mergeable_in_schema(
            self.schema_version_id,
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(made_by)
                .cells(cells),
        )?;
        self.node
            .node
            .borrow_mut()
            .finalize_local_mergeable_commit(tx_id)?;
        self.refresh_subscriptions()?;
        self.node.mark_subscriber_connections_dirty();
        Ok(tx_id)
    }

    /// Seed a branch-local mergeable row for history-complete server bootstrap
    /// or import flows.
    ///
    /// The resulting row is evaluated through the ordinary branch read-view
    /// lowering path; this does not provide an application-facing branch write
    /// facade.
    pub fn seed_branch_mergeable_for_bootstrap(
        &self,
        branch: crate::ids::BranchId,
        table: &str,
        row: RowUuid,
        made_by: AuthorId,
        cells: RowCells,
    ) -> Result<TxId, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        let mut node = self.node.node.borrow_mut();
        if node.branch_record(branch).is_none() {
            node.create_branch(branch)?;
        }
        let tx_id = node.commit_mergeable_on_branch_in_schema(
            branch,
            self.schema_version_id,
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(made_by)
                .cells(cells),
        )?;
        node.finalize_local_mergeable_commit(tx_id)?;
        drop(node);
        self.refresh_subscriptions()?;
        self.node.mark_subscriber_connections_dirty();
        Ok(tx_id)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only authority finalization for a locally committed mergeable
    /// transaction.
    ///
    /// This allows scale fixtures to use the ordinary batched transaction API
    /// before performing the same self-acceptance step as
    /// [`Db::seed_settled_mergeable_for_bootstrap`].
    pub fn finalize_local_mergeable_commit_for_test(&self, tx_id: TxId) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .finalize_local_mergeable_commit(tx_id)?;
        self.refresh_subscriptions()?;
        self.node.mark_subscriber_connections_dirty();
        Ok(())
    }

    /// Return the locally observed fate and durability for a write transaction.
    pub fn write_state(&self, tx_id: TxId) -> Result<WriteState, Error> {
        let Some((fate, _, durability)) = self.node.node.borrow_mut().transaction_state(tx_id)
        else {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "transaction is not known locally",
            ));
        };
        Ok(WriteState { fate, durability })
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
    pub fn connect_upstream(
        &self,
        transport: Box<dyn Transport>,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node.connect_upstream(transport)
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
        identity: AuthorId,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node.accept_subscriber(transport, identity)
    }

    /// Accept a subscriber connection served under `identity` with auth claims.
    pub fn accept_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node
            .accept_subscriber_with_claims(transport, identity, claims)
    }

    /// Accept a subscriber connection with explicit auth claims and upload trust mode.
    pub fn accept_subscriber_with_claims_and_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node
            .accept_subscriber_with_claims_and_trust(transport, identity, claims, trust)
    }

    /// Accept an edge-terminated subscriber with session claims.
    pub fn accept_edge_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node
            .accept_edge_subscriber_with_claims(transport, identity, claims)
    }

    /// Accept a subscriber whose host shell is wired as an edge fate authority.
    pub fn accept_edge_authority_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node
            .accept_edge_authority_subscriber_with_claims(transport, identity, claims)
    }

    /// Accept a reconnecting subscriber, resuming from a previous cursor.
    pub fn accept_subscriber_with_resume(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        cursor: ResumeCursor,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node
            .accept_subscriber_with_resume(transport, identity, cursor)
    }

    /// Detach a previously attached peer connection from this database.
    pub fn detach_connection(&self, connection: &Rc<RefCell<PeerConnection<S>>>) -> bool {
        self.node.detach_connection(connection)
    }

    /// Service every connection once (a convenience over
    /// [`PeerConnection::tick`] for the common single-upstream client).
    pub fn tick(&self) -> Result<(), Error> {
        self.node.tick().map(|_| ())
    }

    /// Service every connection once and return binding-observable wake counts.
    pub fn tick_stats(&self) -> Result<DbTickStats, Error> {
        self.node.tick()
    }

    pub(super) fn refresh_subscriptions(&self) -> Result<usize, Error> {
        let refreshed = self.node.refresh_subscriptions()?;
        if refreshed > 0 {
            self.node.mark_subscriber_connections_dirty();
        }
        Ok(refreshed)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only history-class byte estimate. This is intentionally the
    /// cheap physical-class counter, not a logical table-prefix scan.
    pub fn history_class_bytes_for_test(&self) -> Result<Option<u64>, Error> {
        Ok(self.node.node.borrow().history_class_bytes_for_test()?)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only encoded storage byte estimate across Jazz physical
    /// classes.
    pub fn encoded_storage_bytes_for_test(&self) -> Result<u64, Error> {
        Ok(self.node.node.borrow().encoded_storage_bytes_for_test()?)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only durability boundary for harnesses that reopen the same
    /// storage path immediately after a synthetic lifecycle transition.
    pub fn flush_for_test(&self) -> Result<(), Error> {
        Ok(self.node.node.borrow_mut().flush_query_runtime()?)
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
    left.branch_read_policy == right.branch_read_policy
        && left.branch_write_policy == right.branch_write_policy
        && left.tables.len() == right.tables.len()
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
    author: AuthorId,
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
        author: AuthorId,
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
