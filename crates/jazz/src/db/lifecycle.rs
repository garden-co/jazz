//! Database construction, schema views, write-state waiting, and connection controls.

use super::mutations::MutationPrepareError;
use super::subscriptions::SubscriptionOpenError;
use super::*;
use crate::ids::{BranchId, MigrationLensId};
use crate::node::{DemandDrivenNode, DemandDrivenNodeOpen};

/// Demand-driven construction for a high-level database backed by ordered
/// async storage. The resulting owner keeps the familiar synchronous `Db` facade
/// and its durable runtime together.
#[doc(hidden)]
pub struct DemandDrivenDbOpen {
    opening: Option<DemandDrivenNodeOpen>,
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

/// Immutable typed-schema selection for one [`DemandDrivenDb`] owner.
///
/// This token carries no node, storage, scheduler, or mutable state. Bindings
/// may clone it freely and must route operations back through the unique owner.
#[derive(Clone, Debug)]
pub struct DemandDrivenView {
    schema_view_id: SchemaViewId,
}

/// A short-lived typed view borrowed from the unique async database owner.
///
/// It owns no storage or runtime. Dropping it merely releases the mutable
/// borrow so another schema view can operate on the same resident node.
#[doc(hidden)]
pub struct DemandDrivenViewDb<'a> {
    database: Db<groove::storage::DemandLoadedStorage>,
    runtime: &'a mut DemandDrivenNode,
}

impl DemandDrivenDbOpen {
    #[doc(hidden)]
    pub fn new(
        schema: JazzSchema,
        identity: DbIdentity,
        persistence: Box<dyn groove::storage::async_ordered::OrderedKvStorage>,
    ) -> Self {
        Self {
            opening: Some(DemandDrivenNodeOpen::new(
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
    pub fn new_history_complete(
        schema: JazzSchema,
        identity: DbIdentity,
        persistence: Box<dyn groove::storage::async_ordered::OrderedKvStorage>,
    ) -> Self {
        Self {
            opening: Some(DemandDrivenNodeOpen::new_history_complete(
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
    pub fn new_catalogue_uninitialized(
        identity: DbIdentity,
        persistence: Box<dyn groove::storage::async_ordered::OrderedKvStorage>,
    ) -> Self {
        Self {
            opening: Some(DemandDrivenNodeOpen::new_catalogue_uninitialized(
                identity.node,
                persistence,
            )),
            runtime: None,
            schema: JazzSchema::new([]),
            identity,
            id_source: None,
        }
    }

    #[doc(hidden)]
    pub fn with_id_source(mut self, id_source: impl RowIdSource + 'static) -> Self {
        self.id_source = Some(Box::new(id_source));
        self
    }

    fn with_boxed_id_source(mut self, id_source: Option<Box<dyn RowIdSource>>) -> Self {
        self.id_source = id_source;
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
    /// Open an ordinary database over a storage backend that completes the
    /// ordered asynchronous contract immediately.
    pub async fn open_immediate<S>(config: DbConfig<S>) -> Result<Self, Error>
    where
        S: ResidentStorage + ReopenableStorage + 'static,
    {
        let DbConfig {
            schema,
            storage,
            identity,
            id_source,
        } = config;
        let mut opening = DemandDrivenDbOpen::new(
            schema,
            identity,
            Box::new(groove::storage::async_ordered::ImmediateStorage::new(
                storage,
            )),
        )
        .with_boxed_id_source(id_source);
        std::future::poll_fn(|context| opening.poll(context)).await
    }

    /// Open a history-complete authority over an immediately completing
    /// ordered backend.
    pub async fn open_history_complete_immediate<S>(config: DbConfig<S>) -> Result<Self, Error>
    where
        S: ResidentStorage + ReopenableStorage + 'static,
    {
        let DbConfig {
            schema,
            storage,
            identity,
            id_source,
        } = config;
        let mut opening = DemandDrivenDbOpen::new_history_complete(
            schema,
            identity,
            Box::new(groove::storage::async_ordered::ImmediateStorage::new(
                storage,
            )),
        )
        .with_boxed_id_source(id_source);
        std::future::poll_fn(|context| opening.poll(context)).await
    }

    /// Open a blank dynamic-edge catalogue over an immediately completing
    /// ordered backend. The store remains unavailable to application sessions
    /// until an authenticated authority snapshot is adopted.
    pub async fn open_catalogue_uninitialized_immediate<S>(
        config: DbConfig<S>,
    ) -> Result<Self, Error>
    where
        S: ResidentStorage + ReopenableStorage + 'static,
    {
        let DbConfig {
            storage,
            identity,
            id_source,
            ..
        } = config;
        let mut opening = DemandDrivenDbOpen::new_catalogue_uninitialized(
            identity,
            Box::new(groove::storage::async_ordered::ImmediateStorage::new(
                storage,
            )),
        )
        .with_boxed_id_source(id_source);
        std::future::poll_fn(|context| opening.poll(context)).await
    }

    /// Start a logical query without touching durable storage.
    pub fn table(&self, table: impl Into<String>) -> Query {
        self.database.table(table)
    }

    /// Compile a logical query. Durable source acquisition happens when the
    /// resulting query is read or subscribed, not while its shape is built.
    pub fn prepare_query(&self, query: &Query) -> Result<PreparedQuery, Error> {
        self.database.prepare_query(query)
    }

    /// Return the typed view selected when this owner opened.
    pub fn default_view(&self) -> DemandDrivenView {
        DemandDrivenView {
            schema_view_id: self.database.schema_view_id(),
        }
    }

    /// Admit and register a typed schema, then return an inert selection token.
    /// Catalogue control state is a startup-resident invariant; only its
    /// resulting ordered write may suspend here.
    pub async fn register_schema_view(
        &mut self,
        schema: JazzSchema,
    ) -> Result<DemandDrivenView, Error> {
        let registered = self.database.register_schema_view(schema)?;
        let schema_view_id = registered.schema_view_id();
        std::future::poll_fn(|context| self.runtime.poll_persistence(context))
            .await
            .map_err(Error::from)?;
        Ok(DemandDrivenView { schema_view_id })
    }

    fn facade_for_view(
        &self,
        view: &DemandDrivenView,
    ) -> Result<Db<groove::storage::DemandLoadedStorage>, Error> {
        self.database.schema_view(view.schema_view_id)
    }

    /// Borrow an operational typed view from this owner. The facade cannot
    /// outlive the owner borrow and therefore cannot become a second owner.
    pub fn view<'a>(
        &'a mut self,
        view: &DemandDrivenView,
    ) -> Result<DemandDrivenViewDb<'a>, Error> {
        let database = self.facade_for_view(view)?;
        Ok(DemandDrivenViewDb {
            database,
            runtime: &mut self.runtime,
        })
    }

    /// Compile a query against an explicit typed view of this owner.
    pub fn prepare_query_in_view(
        &self,
        view: &DemandDrivenView,
        query: &Query,
    ) -> Result<PreparedQuery, Error> {
        self.facade_for_view(view)?.prepare_query(query)
    }

    pub fn write_state(&self, tx_id: TxId) -> Result<WriteState, Error> {
        self.database.write_state(tx_id)
    }

    pub fn wait_for_transaction_with(
        &self,
        tx_id: TxId,
        tier: DurabilityTier,
        callback: impl FnOnce(Result<TxId, Error>) + 'static,
    ) {
        self.database
            .wait_for_transaction_with(tx_id, tier, callback);
    }

    pub fn set_tick_scheduler(&self, scheduler: Option<Rc<dyn TickScheduler>>) {
        self.database.set_tick_scheduler(scheduler);
    }

    pub fn on_mutation_error(&self, callback: MutationErrorCallback) {
        self.database.on_mutation_error(callback);
    }

    pub fn set_non_durable_client(&self) {
        self.database.set_non_durable_client();
    }

    pub fn set_upstream_durability_floor(&self, tier: DurabilityTier) {
        self.database.set_upstream_durability_floor(tier);
    }

    pub fn set_initial_sync_flush_cadence(
        &self,
        cadence: InitialSyncFlushCadence,
    ) -> Result<(), Error> {
        self.database.set_initial_sync_flush_cadence(cadence)
    }

    pub fn abandon_transaction(&mut self, tx_id: OpenBatchId) -> Result<(), Error> {
        self.database.abandon_transaction_handle(tx_id)
    }

    #[cfg(any(feature = "runtime", test))]
    pub async fn apply_trusted_catalogue_snapshot(
        &mut self,
        snapshot: crate::protocol::CatalogueSnapshot,
    ) -> Result<(), Error> {
        std::future::poll_fn(|context| {
            self.runtime
                .poll_apply_peer_catalogue_snapshot(context, &snapshot)
        })
        .await
        .map_err(Into::into)
    }

    #[cfg(any(feature = "runtime", test))]
    pub fn trusted_catalogue_snapshot(&self) -> Result<crate::protocol::CatalogueSnapshot, Error> {
        self.database.trusted_catalogue_snapshot()
    }

    #[cfg(any(feature = "runtime", test))]
    pub fn trusted_current_catalogue_schema(&self) -> Result<JazzSchema, Error> {
        self.database.trusted_current_catalogue_schema()
    }

    #[cfg(any(feature = "runtime", test))]
    pub fn catalogue_bootstrap_is_ready(&self) -> bool {
        self.database.catalogue_bootstrap_is_ready()
    }

    async fn apply_trusted_catalogue_message(
        &mut self,
        message: SyncMessage,
    ) -> Result<Vec<SyncMessage>, Error> {
        self.database.check_catalogue_admin()?;
        std::future::poll_fn(|context| {
            self.runtime
                .poll_apply_trusted_catalogue_message(context, &message)
        })
        .await
        .map_err(Into::into)
    }

    pub async fn publish_schema(
        &mut self,
        schema: SchemaVersion,
    ) -> Result<Vec<SyncMessage>, Error> {
        self.apply_trusted_catalogue_message(SyncMessage::PublishSchema {
            author: self.database.identity.author,
            schema: Box::new(schema),
        })
        .await
    }

    pub async fn publish_lens(&mut self, lens: MigrationLens) -> Result<Vec<SyncMessage>, Error> {
        self.apply_trusted_catalogue_message(SyncMessage::PublishLens {
            author: self.database.identity.author,
            lens,
        })
        .await
    }

    pub async fn publish_schema_with_lens(
        &mut self,
        catalogue_seq: u64,
        publication: SchemaLineagePublication,
    ) -> Result<Vec<SyncMessage>, Error> {
        self.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: self.database.identity.author,
            catalogue_seq,
            publication: Box::new(publication),
        })
        .await
    }

    pub async fn set_current_write_schema(
        &mut self,
        pointer: CurrentWriteSchema,
    ) -> Result<Vec<SyncMessage>, Error> {
        self.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
            author: self.database.identity.author,
            pointer,
        })
        .await
    }

    /// Seed one authority-settled row through the same pending commit and
    /// authority-ingest boundaries used by network writes.
    pub async fn seed_settled_mergeable_for_bootstrap(
        &mut self,
        table: &str,
        row: RowUuid,
        made_by: AuthorId,
        cells: RowCells,
    ) -> Result<TxId, Error> {
        let cells = self.database.apply_insert_defaults(table, cells)?;
        let commit = MergeableCommit::new(table, row, self.database.next_now_ms())
            .made_by(made_by)
            .cells(cells);
        let schema = self.database.schema_version_id;
        let tx_id = std::future::poll_fn(|context| {
            self.runtime
                .poll_mergeable_commit_in_schema(context, schema, &commit)
        })
        .await
        .map_err(Error::from)?;
        let SyncMessage::CommitUnit { tx, versions } = self
            .database
            .node
            .node
            .borrow_mut()
            .commit_unit_for(tx_id)?
        else {
            unreachable!("mergeable commit unit has one canonical wire shape")
        };
        std::future::poll_fn(|context| {
            self.runtime.poll_ingest_commit_unit(
                context,
                tx.clone(),
                versions.clone(),
                commit.now_ms,
                None,
            )
        })
        .await
        .map_err(Error::from)?;
        self.database.refresh_subscriptions()?;
        self.database.node.mark_subscriber_connections_dirty();
        Ok(tx_id)
    }

    /// Attach an authority-admitted typed schema to this owner.
    pub fn select_schema_view(&mut self, schema: JazzSchema) -> Result<(), Error> {
        self.database = self.database.register_schema_view(schema)?;
        Ok(())
    }

    pub fn set_edge_cache_budget(&self, budget: Option<crate::node::EdgeCacheBudget>) {
        self.database.set_edge_cache_budget(budget);
    }

    pub fn current_write_schema(&self) -> Result<CurrentWriteSchema, Error> {
        self.database.current_write_schema()
    }

    pub fn catalogue_schema(&self, schema: SchemaVersionId) -> Option<JazzSchema> {
        self.database.catalogue_schema(schema)
    }

    pub fn active_catalogue_seq(&self) -> u64 {
        self.database.active_catalogue_seq()
    }

    pub fn catalogue_lens(&self, lens: MigrationLensId) -> Option<MigrationLens> {
        self.database.catalogue_lens(lens)
    }

    pub fn set_permissions_ready(&self, ready: bool) -> Result<(), Error> {
        self.database.set_permissions_ready(ready)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn set_catalogue_activation_failpoint(
        &self,
        failpoint: crate::node::CatalogueActivationFailpoint,
    ) {
        self.database.set_catalogue_activation_failpoint(failpoint);
    }

    pub async fn seed_branch_mergeable_for_bootstrap(
        &mut self,
        branch: BranchId,
        table: &str,
        row: RowUuid,
        made_by: AuthorId,
        cells: RowCells,
    ) -> Result<TxId, Error> {
        if self
            .database
            .node
            .node
            .borrow()
            .branch_record(branch)
            .is_none()
        {
            self.create_branch_with_id(branch).await?;
        }
        let cells = self.database.apply_insert_defaults(table, cells)?;
        let commit = MergeableCommit::new(table, row, self.database.next_now_ms())
            .made_by(made_by)
            .cells(cells);
        let schema = self.database.schema_version_id;
        let tx_id = std::future::poll_fn(|context| {
            self.runtime.poll_mergeable_many_on_branch_in_schema(
                context,
                branch,
                schema,
                std::slice::from_ref(&commit),
            )
        })
        .await
        .map_err(Error::from)?;
        let SyncMessage::CommitUnit { tx, versions } = self
            .database
            .node
            .node
            .borrow_mut()
            .commit_unit_for(tx_id)?
        else {
            unreachable!("branch mergeable commit has one canonical wire shape")
        };
        std::future::poll_fn(|context| {
            self.runtime.poll_ingest_commit_unit(
                context,
                tx.clone(),
                versions.clone(),
                commit.now_ms,
                None,
            )
        })
        .await
        .map_err(Error::from)?;
        self.database.refresh_subscriptions()?;
        self.database.node.mark_subscriber_connections_dirty();
        Ok(tx_id)
    }

    pub async fn tick(&mut self) -> Result<DbTickStats, Error> {
        std::future::poll_fn(|context| self.poll_tick(context)).await
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

    /// Accept a subscriber whose storage-mutating frames are retained by this
    /// asynchronous owner until their durable publication completes.
    pub fn accept_subscriber(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
    ) -> Rc<RefCell<PeerConnection<groove::storage::DemandLoadedStorage>>> {
        let connection = self.database.accept_subscriber(transport, identity);
        connection.borrow_mut().enable_external_durable_ingress();
        connection
    }

    pub fn accept_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<RefCell<PeerConnection<groove::storage::DemandLoadedStorage>>> {
        let connection = self
            .database
            .accept_subscriber_with_claims(transport, identity, claims);
        connection.borrow_mut().enable_external_durable_ingress();
        connection
    }

    pub fn accept_subscriber_with_claims_and_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
    ) -> Rc<RefCell<PeerConnection<groove::storage::DemandLoadedStorage>>> {
        let connection = self
            .database
            .accept_subscriber_with_claims_and_trust(transport, identity, claims, trust);
        connection.borrow_mut().enable_external_durable_ingress();
        connection
    }

    pub fn accept_edge_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<RefCell<PeerConnection<groove::storage::DemandLoadedStorage>>> {
        let connection = self
            .database
            .accept_edge_subscriber_with_claims(transport, identity, claims);
        connection.borrow_mut().enable_external_durable_ingress();
        connection
    }

    pub fn accept_edge_authority_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<RefCell<PeerConnection<groove::storage::DemandLoadedStorage>>> {
        let connection = self
            .database
            .accept_edge_authority_subscriber_with_claims(transport, identity, claims);
        connection.borrow_mut().enable_external_durable_ingress();
        connection
    }

    pub fn accept_subscriber_with_resume(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        cursor: ResumeCursor,
    ) -> Rc<RefCell<PeerConnection<groove::storage::DemandLoadedStorage>>> {
        let connection = self
            .database
            .accept_subscriber_with_resume(transport, identity, cursor);
        connection.borrow_mut().enable_external_durable_ingress();
        connection
    }

    pub fn detach_connection(
        &self,
        connection: &Rc<RefCell<PeerConnection<groove::storage::DemandLoadedStorage>>>,
    ) -> bool {
        self.database.detach_connection(connection)
    }

    /// Drive peer work without replaying a frame across an asynchronous
    /// storage suspension. At most one durable frame owns the persistence
    /// boundary at a time; ordinary connection-control traffic continues
    /// through the resident peer tick.
    pub fn poll_tick(&mut self, context: &mut Context<'_>) -> Poll<Result<DbTickStats, Error>> {
        let connections = self.database.node.connections.borrow().clone();
        for connection in &connections {
            connection.borrow_mut().stage_available_inbound();
            match self.runtime.poll_acquire_resident(context, |node| {
                connection
                    .borrow_mut()
                    .prepare_staged_subscription_inputs(node)
            }) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                Poll::Ready(Ok(())) => {}
            }
            let pending_session_branch = connection.borrow().pending_session_branch_metadata();
            if let Some(metadata) = pending_session_branch {
                let identity = connection
                    .borrow()
                    .session_identity()
                    .expect("pending session metadata retains its subscriber identity");
                match self
                    .runtime
                    .poll_apply_session_branch_metadata(context, &metadata, identity)
                {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                    Poll::Ready(Ok(None)) => {}
                    Poll::Ready(Ok(Some(responses))) => {
                        connection
                            .borrow_mut()
                            .complete_session_branch_metadata(&metadata, false, responses);
                    }
                }
            }
            let staged_session_branch = connection.borrow().staged_session_branch_metadata();
            if let Some(metadata) = staged_session_branch {
                let identity = connection
                    .borrow()
                    .session_identity()
                    .expect("staged session metadata retains its subscriber identity");
                match self
                    .runtime
                    .poll_apply_session_branch_metadata(context, &metadata, identity)
                {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                    Poll::Ready(Ok(None)) => connection
                        .borrow_mut()
                        .park_staged_session_branch_metadata(),
                    Poll::Ready(Ok(Some(responses))) => {
                        connection
                            .borrow_mut()
                            .complete_session_branch_metadata(&metadata, true, responses);
                    }
                }
                continue;
            }
            let staged_catalogue_message = connection.borrow().staged_catalogue_message();
            if let Some((message, ingest_context)) = staged_catalogue_message {
                match self.runtime.poll_apply_peer_catalogue_message(
                    context,
                    &message,
                    ingest_context,
                ) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                    Poll::Ready(Ok(responses)) => {
                        connection
                            .borrow_mut()
                            .complete_staged_catalogue_message(&message, responses);
                    }
                }
                continue;
            }
            let staged_catalogue = { connection.borrow().staged_catalogue_snapshot() };
            if let Some(snapshot) = staged_catalogue {
                match self
                    .runtime
                    .poll_apply_peer_catalogue_snapshot(context, &snapshot)
                {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                    Poll::Ready(Ok(())) => {
                        connection.borrow_mut().complete_staged_catalogue_snapshot();
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
                    Poll::Ready(Ok(())) => {
                        connection
                            .borrow_mut()
                            .complete_staged_branch_metadata(metadata.branch_id);
                    }
                }
                if connections
                    .iter()
                    .any(|connection| connection.borrow().has_externally_applied_inbound())
                {
                    self.database.node.mark_subscriber_connections_dirty();
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
                        connection.borrow_mut().complete_staged_row_version_repair();
                    }
                }
                // Completion re-stages the original ViewUpdate at the head of
                // this same link. Yield before the legacy node tick can consume
                // it; the next owner poll acquires and publishes it through the
                // typed receiver boundary.
                if connections
                    .iter()
                    .any(|connection| connection.borrow().has_externally_applied_inbound())
                {
                    self.database.node.mark_subscriber_connections_dirty();
                }
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
            let subscriber_relay = connection.borrow_mut().staged_subscriber_relay_commit();
            if let Some((tx, versions, kind)) = subscriber_relay {
                let tx_id = tx.tx_id;
                match self
                    .runtime
                    .poll_ingest_relay_commit_unit(context, tx, versions)
                {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => {
                        connection
                            .borrow_mut()
                            .abort_staged_subscriber_relay_commit(tx_id, kind);
                        return Poll::Ready(Err(error.into()));
                    }
                    Poll::Ready(Ok(())) => {
                        connection
                            .borrow_mut()
                            .complete_staged_subscriber_relay_commit(tx_id, kind);
                    }
                }
                continue;
            }
            let subscriber_authority = connection.borrow().staged_subscriber_authority_commit();
            if let Some((tx, versions, ingest_context)) = subscriber_authority {
                let tx_id = tx.tx_id;
                if !connection
                    .borrow()
                    .staged_subscriber_authority_is_prepared(tx_id)
                {
                    match self.runtime.poll_acquire_resident(context, |node| {
                        connection
                            .borrow_mut()
                            .prepare_staged_subscriber_authority(node, tx_id, &versions)
                    }) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                        Poll::Ready(Ok(())) => {}
                    }
                }
                match self.runtime.poll_ingest_commit_unit(
                    context,
                    tx,
                    versions,
                    u64::MAX - crate::node::SKEW_TOLERANCE_MS,
                    Some(ingest_context),
                ) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                    Poll::Ready(Ok(responses)) => {
                        connection
                            .borrow_mut()
                            .complete_staged_subscriber_authority_commit(tx_id, responses);
                    }
                }
                continue;
            }
            let owned_fate = connection.borrow().staged_owned_fate();
            if let Some((tx_id, fate, global_seq, durability)) = owned_fate {
                match self
                    .runtime
                    .poll_apply_peer_fate_update(context, tx_id, fate, global_seq, durability)
                {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                    Poll::Ready(Ok(())) => {
                        connection.borrow_mut().complete_staged_owned_fate(tx_id);
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
                            connection.borrow_mut().complete_staged_view_update(&parts);
                        }
                    }
                    continue;
                }
            }
        }

        if connections
            .iter()
            .any(|connection| connection.borrow().has_externally_applied_inbound())
        {
            // Typed async ingress has already mutated the resident query
            // state. Publish that invalidation before acquiring subscriber
            // outputs; the legacy synchronous tick runs after acquisition and
            // is therefore too late to arm this owner poll.
            self.database.node.mark_subscriber_connections_dirty();
        }

        // Typed ingress above may change any subscriber's maintained view.
        // Prepare outbound refreshes only after every connection has had its
        // resident mutation admitted, never against a pre-ingress snapshot.
        for connection in &connections {
            match self.runtime.poll_acquire_resident(context, |node| {
                connection
                    .borrow_mut()
                    .prepare_subscriber_serving_inputs(node)
            }) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                Poll::Ready(Ok(())) => {}
            }
        }

        let parked_tails = connections
            .iter()
            .map(|connection| connection.borrow_mut().park_staged_inbound_tail())
            .collect::<Vec<_>>();
        let tick_result = self.database.node.tick();
        for (connection, tail) in connections.iter().zip(parked_tails) {
            connection.borrow_mut().restore_staged_inbound_tail(tail);
        }
        let stats = match tick_result {
            Ok(stats) => stats,
            Err(error) => return Poll::Ready(Err(error)),
        };
        if connections.iter().any(|connection| {
            let connection = connection.borrow();
            connection.has_staged_inbound()
                || connection.staged_catalogue_snapshot().is_some()
                || connection.staged_branch_metadata().is_some()
                || connection.staged_row_version_repair().is_some()
                || connection.staged_relay_commit().is_some()
                || connection.staged_owned_fate().is_some()
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

    /// Run a one-shot read through an explicit typed view of this owner.
    pub async fn all_in_view(
        &mut self,
        view: &DemandDrivenView,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.view(view)?.all(prepared, opts).await
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

    /// Read a structured snapshot through an explicit typed view.
    pub async fn relation_snapshot_in_view(
        &mut self,
        view: &DemandDrivenView,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        self.view(view)?.relation_snapshot(prepared, opts).await
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

    /// Materialize a public result tree through an explicit typed view.
    pub async fn result_tree_in_view(
        &mut self,
        view: &DemandDrivenView,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<ResultTree, Error> {
        let snapshot = self.relation_snapshot_in_view(view, prepared, opts).await?;
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

    /// Open a subscription through an explicit typed view.
    pub async fn subscribe_in_view(
        &mut self,
        view: &DemandDrivenView,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<SubscriptionStream, Error> {
        self.view(view)?.subscribe(prepared, opts).await
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
                    || database.prepare_noop_update_for_owner(table, row, database.identity.author),
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
                || {
                    database.prepare_update_commit_for_owner(
                        table,
                        row,
                        patch.clone(),
                        now_ms,
                        database.identity.author,
                    )
                },
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
                || {
                    database.prepare_upsert_commit_for_owner(
                        table,
                        row,
                        cells.clone(),
                        now_ms,
                        database.identity.author,
                    )
                },
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
                || database.begin_mergeable_for_owner(id, database.identity.author),
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
        let fallback_count = self
            .database
            .node
            .node
            .borrow()
            .mergeable_open_missing_timestamp_count(tx_id)?;
        let fallback_now_ms = (0..fallback_count)
            .map(|_| self.database.next_now_ms())
            .collect::<Vec<_>>();
        let committed = std::future::poll_fn(|context| {
            self.runtime
                .poll_mergeable_open(context, tx_id, &fallback_now_ms)
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
                || database.begin_exclusive_for_owner(id, database.identity.author),
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

impl DemandDrivenViewDb<'_> {
    /// Start a logical query in this typed view without loading storage.
    pub fn table(&self, table: impl Into<String>) -> Query {
        self.database.table(table)
    }

    /// Compile a logical query in this typed view.
    pub fn prepare_query(&self, query: &Query) -> Result<PreparedQuery, Error> {
        self.database.prepare_query(query)
    }

    pub async fn all_relation_query(
        &mut self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: Option<AuthorId>,
    ) -> Result<RelationSnapshot, Error> {
        ensure_default_read_view(&opts)?;
        let query = relation_query_to_query(query)?;
        let prepared = self.prepare_query(&query)?;
        let rows = match author {
            Some(author) => self.all_for_identity(&prepared, opts, author).await?,
            None => self.all(&prepared, opts).await?,
        };
        Ok(RelationSnapshot {
            root_count: rows.len(),
            rows,
            edges: Vec::new(),
        })
    }

    /// Run a one-shot query, loading only missing physical inputs.
    pub async fn all(
        &mut self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        let database = &self.database;
        let author = database.identity.author;
        std::future::poll_fn(|context| {
            if let Err(error) = ensure_supported_read_view(&opts) {
                return Poll::Ready(Err(error));
            }
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
                database.all_resident(prepared, &opts, author, QueryAuthorizationMode::ClientLocal)
            }) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => Poll::Ready(result.map_err(Into::into)),
            }
        })
        .await
    }

    /// Run a one-shot query as an explicit terminated-session identity.
    pub async fn all_for_identity(
        &mut self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let database = &self.database;
        std::future::poll_fn(|context| {
            if let Err(error) = ensure_supported_read_view(&opts) {
                return Poll::Ready(Err(error));
            }
            match self.runtime.poll_resident_operation(context, || {
                database.all_resident(
                    prepared,
                    &opts,
                    author,
                    QueryAuthorizationMode::TrustedServing,
                )
            }) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => Poll::Ready(result.map_err(Into::into)),
            }
        })
        .await
    }

    /// Materialize a structured relation snapshot in this typed view.
    pub async fn relation_snapshot(
        &mut self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        let database = &self.database;
        let author = database.identity.author;
        std::future::poll_fn(|context| {
            if let Err(error) = ensure_supported_read_view(&opts) {
                return Poll::Ready(Err(error));
            }
            if opts.include_deleted {
                return Poll::Ready(Err(Error::new(
                    ErrorCode::Query,
                    "relation snapshots do not support include_deleted yet",
                )));
            }
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
        })
        .await
    }

    pub async fn relation_snapshot_for_identity(
        &mut self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<RelationSnapshot, Error> {
        let database = &self.database;
        std::future::poll_fn(|context| {
            match self.runtime.poll_resident_operation(context, || {
                database.relation_snapshot_resident(
                    prepared,
                    &opts,
                    author,
                    QueryAuthorizationMode::TrustedServing,
                )
            }) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => Poll::Ready(result.map_err(Into::into)),
            }
        })
        .await
    }

    /// Materialize the canonical public result tree in this typed view.
    pub async fn result_tree(
        &mut self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<ResultTree, Error> {
        let snapshot = self.relation_snapshot(prepared, opts).await?;
        materialize_result_tree(prepared.shape.query(), snapshot)
    }

    /// Open a maintained subscription in this typed view.
    pub async fn subscribe(
        &mut self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<SubscriptionStream, Error> {
        let database = &self.database;
        let author = database.identity.author;
        std::future::poll_fn(|context| {
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
        })
        .await
    }

    pub async fn subscribe_for_identity(
        &mut self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<SubscriptionStream, Error> {
        let database = &self.database;
        std::future::poll_fn(|context| {
            match self.runtime.poll_operation(
                context,
                || {
                    database.open_subscription_resident(
                        prepared,
                        opts.clone(),
                        author,
                        QueryAuthorizationMode::TrustedServing,
                    )
                },
                SubscriptionOpenError::missing_input,
            ) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => Poll::Ready(result.map_err(SubscriptionOpenError::into_api)),
            }
        })
        .await
    }

    pub async fn subscribe_relation_query(
        &mut self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: Option<AuthorId>,
    ) -> Result<SubscriptionStream, Error> {
        let query = relation_query_to_query(query)?;
        let prepared = self.prepare_query(&query)?;
        match author {
            Some(author) => self.subscribe_for_identity(&prepared, opts, author).await,
            None => self.subscribe(&prepared, opts).await,
        }
    }

    pub fn set_identity_claims(
        &self,
        author: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Result<(), Error> {
        self.database.set_identity_claims(author, claims);
        Ok(())
    }

    pub fn local_current_row(
        &self,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<CurrentRow>, Error> {
        self.database.local_current_row(table, row)
    }

    pub fn attach_query_with_opts(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<QueryAttachment, Error> {
        self.database.attach_query_with_opts(prepared, opts)
    }

    pub fn attach_query_for_identity_with_opts(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<QueryAttachment, Error> {
        self.database
            .attach_query_with_opts_for_identity(prepared, opts, author)
    }

    pub fn query_attachment_is_covered(&self, attachment: &QueryAttachment) -> Result<bool, Error> {
        Ok(self.database.query_attachment_is_covered(attachment))
    }

    pub fn detach_query(&self, attachment: QueryAttachment) -> Result<(), Error> {
        self.database.detach_query(attachment);
        Ok(())
    }

    async fn publish_mergeable(
        &mut self,
        commits: &[MergeableCommit],
        row: RowUuid,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        let schema = self.database.schema_version_id;
        let tx_id = std::future::poll_fn(|context| {
            self.runtime
                .poll_mergeable_many_in_schema(context, schema, commits)
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

    /// Insert one caller-selected row through this typed view.
    pub async fn insert_with_id(
        &mut self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        made_by: Option<AuthorId>,
        now_ms: Option<u64>,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        let permission_subject = made_by;
        let author = made_by.unwrap_or(self.database.identity.author);
        let cells = self.database.apply_insert_defaults(table, cells)?;
        let mut commit = MergeableCommit::new(
            table,
            row,
            now_ms.unwrap_or_else(|| self.database.next_now_ms()),
        )
        .made_by(author)
        .cells(cells);
        if let Some(subject) = permission_subject {
            commit = commit.permission_subject(subject);
        }
        let database = &self.database;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || database.acquire_insert_target_for_owner(table, row),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)?;
        self.publish_mergeable(std::slice::from_ref(&commit), row)
            .await
    }

    /// Update one row through this typed view.
    pub async fn update(
        &mut self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        made_by: Option<AuthorId>,
        now_ms: Option<u64>,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        let permission_subject = made_by;
        let author = made_by.unwrap_or(self.database.identity.author);
        if patch.is_empty() {
            let database = &self.database;
            let (tx_id, local_tier) = std::future::poll_fn(|context| {
                self.runtime.poll_operation(
                    context,
                    || database.prepare_noop_update_for_owner(table, row, author),
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
        let now_ms = now_ms.unwrap_or_else(|| self.database.next_now_ms());
        let database = &self.database;
        let mut commit = std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    database.prepare_update_commit_for_owner(
                        table,
                        row,
                        patch.clone(),
                        now_ms,
                        author,
                    )
                },
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)?
        .made_by(author);
        if let Some(subject) = permission_subject {
            commit = commit.permission_subject(subject);
        }
        self.publish_mergeable(std::slice::from_ref(&commit), row)
            .await
    }

    /// Insert or update one caller-selected row through this typed view.
    pub async fn upsert(
        &mut self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        made_by: Option<AuthorId>,
        now_ms: Option<u64>,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        let now_ms = now_ms.unwrap_or_else(|| self.database.next_now_ms());
        let permission_subject = made_by;
        let author = made_by.unwrap_or(self.database.identity.author);
        let database = &self.database;
        let mut commit = std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    database.prepare_upsert_commit_for_owner(
                        table,
                        row,
                        cells.clone(),
                        now_ms,
                        author,
                    )
                },
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)?
        .made_by(author);
        if let Some(subject) = permission_subject {
            commit = commit.permission_subject(subject);
        }
        self.publish_mergeable(std::slice::from_ref(&commit), row)
            .await
    }

    /// Soft-delete one row through this typed view.
    pub async fn delete(
        &mut self,
        table: &str,
        row: RowUuid,
        made_by: Option<AuthorId>,
        now_ms: Option<u64>,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        let now_ms = now_ms.unwrap_or_else(|| self.database.next_now_ms());
        let permission_subject = made_by;
        let author = made_by.unwrap_or(self.database.identity.author);
        let database = &self.database;
        let mut commit = std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || database.prepare_delete_commit_for_owner(table, row, now_ms),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)?
        .made_by(author);
        if let Some(subject) = permission_subject {
            commit = commit.permission_subject(subject);
        }
        self.publish_mergeable(std::slice::from_ref(&commit), row)
            .await
    }

    /// Restore one row through this typed view.
    pub async fn restore(
        &mut self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        made_by: Option<AuthorId>,
        now_ms: Option<u64>,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        let now_ms = now_ms.unwrap_or_else(|| self.database.next_now_ms());
        let permission_subject = made_by;
        let author = made_by.unwrap_or(self.database.identity.author);
        let database = &self.database;
        let commits = std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || database.prepare_restore_commits_for_owner(table, row, cells.clone(), now_ms),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)?
        .into_iter()
        .map(|commit| {
            let commit = commit.made_by(author);
            match permission_subject {
                Some(subject) => commit.permission_subject(subject),
                None => commit,
            }
        })
        .collect::<Vec<_>>();
        self.publish_mergeable(&commits, row).await
    }

    /// Open a caller-addressed mergeable batch in this typed view.
    pub async fn begin_mergeable(
        &mut self,
        id: OpenBatchId,
        author: Option<AuthorId>,
    ) -> Result<(), Error> {
        let database = &self.database;
        let author = author.unwrap_or(database.identity.author);
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || database.begin_mergeable_for_owner(id, author),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
    }

    pub async fn mergeable_insert(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = now_ms.unwrap_or_else(|| self.database.next_now_ms());
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
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = now_ms.unwrap_or_else(|| self.database.next_now_ms());
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
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = now_ms.unwrap_or_else(|| self.database.next_now_ms());
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
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = now_ms.unwrap_or_else(|| self.database.next_now_ms());
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

    /// Open a caller-addressed exclusive batch in this typed view.
    pub async fn begin_exclusive(&mut self, id: OpenBatchId) -> Result<(), Error> {
        let database = &self.database;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || database.begin_exclusive_for_owner(id, database.identity.author),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
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
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = now_ms.unwrap_or_else(|| self.database.next_now_ms());
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
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let mut cells = self
            .exclusive_read(tx_id, table, row)
            .await?
            .unwrap_or_default();
        cells.extend(patch);
        self.exclusive_insert(tx_id, table, row, cells, now_ms)
            .await
    }

    pub async fn exclusive_delete(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = now_ms.unwrap_or_else(|| self.database.next_now_ms());
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
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = now_ms.unwrap_or_else(|| self.database.next_now_ms());
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
        Ok(self.with_registered_schema_view(schema))
    }

    fn with_registered_schema_view(&self, schema: JazzSchema) -> Self {
        let schema_version_id = schema.version_id();
        Self {
            schema,
            schema_version_id,
            schema_view_is_fixed: true,
            schema_views: Rc::clone(&self.schema_views),
            identity: self.identity,
            node: Rc::clone(&self.node),
            row_id_source: Rc::clone(&self.row_id_source),
            next_now_ms: Rc::clone(&self.next_now_ms),
        }
    }

    /// Attach an already-registered typed schema view to this owner.
    pub fn schema_view(&self, schema_view_id: SchemaViewId) -> Result<Self, Error> {
        let registered = self
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
        let schema_version_id = registered.version_id();
        let schema = {
            let node = self.node.node.borrow();
            let admitted = node
                .catalogue_schemas()
                .get(&schema_version_id)
                .ok_or_else(|| Error::new(ErrorCode::Schema, "registered schema is missing"))?;
            schema_with_authoritative_runtime_metadata(registered, &admitted.schema)
        };
        Ok(self.with_registered_schema_view(schema))
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

    /// Declare the durability guaranteed by this database's immediate
    /// upstream. Browser main-thread runtimes use `Local` for their persistent
    /// worker; direct server connections retain the default `Global` floor.
    pub fn set_upstream_durability_floor(&self, tier: DurabilityTier) {
        self.node.set_upstream_durability_floor(tier);
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

/// Combine one registered typed shape with the authority-admitted metadata
/// that may change without changing its structural schema version.
fn schema_with_authoritative_runtime_metadata(
    mut registered: JazzSchema,
    admitted: &JazzSchema,
) -> JazzSchema {
    registered.branch_read_policy = admitted.branch_read_policy.clone();
    registered.branch_write_policy = admitted.branch_write_policy.clone();
    for table in &mut registered.tables {
        let Some(admitted_table) = admitted
            .tables
            .iter()
            .find(|candidate| candidate.name == table.name)
        else {
            continue;
        };
        table.read_policy = admitted_table.read_policy.clone();
        table.write_policies = admitted_table.write_policies.clone();
        table.indexed_columns = admitted_table.indexed_columns.clone();
    }
    registered
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
