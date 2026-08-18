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
pub struct DbOpen {
    opening: Option<DemandDrivenNodeOpen>,
    runtime: Option<DemandDrivenNode>,
    schema: JazzSchema,
    identity: DbIdentity,
    id_source: Option<Box<dyn RowIdSource>>,
}

/// High-level database facade plus the async owner of its resident node.
#[doc(hidden)]
pub struct Db {
    pub(super) schema: JazzSchema,
    pub(super) schema_version_id: SchemaVersionId,
    pub(super) schema_views: Rc<RefCell<BTreeMap<SchemaViewId, JazzSchema>>>,
    pub(super) identity: DbIdentity,
    pub(super) node: Rc<Node<groove::storage::DemandLoadedStorage>>,
    pub(super) row_id_source: Rc<RefCell<Box<dyn RowIdSource>>>,
    pub(super) next_now_ms: Rc<Cell<u64>>,
    runtime: DemandDrivenNode,
}

/// Immutable typed-schema selection for one [`Db`] owner.
///
/// This token carries no node, storage, scheduler, or mutable state. Bindings
/// may clone it freely and must route operations back through the unique owner.
#[derive(Clone, Debug)]
pub struct DbSchemaView {
    schema_view_id: SchemaViewId,
}

impl DbSchemaView {
    /// Canonical identity of this registered typed schema selection.
    pub fn schema_view_id(&self) -> SchemaViewId {
        self.schema_view_id
    }
}

/// A short-lived typed view borrowed from the unique async database owner.
///
/// It owns no storage or runtime. Dropping it merely releases the mutable
/// borrow so another schema view can operate on the same resident node.
#[doc(hidden)]
pub struct DbView<'a> {
    owner: &'a mut Db,
    schema: JazzSchema,
    schema_version_id: SchemaVersionId,
}

/// Acquire every cold canonical witness required by the current local
/// maintained-view deltas, then publish their callbacks synchronously.
///
/// Every async-owner mutation path ends here. The resident Jazz reducer stays
/// synchronous, while an async backend never leaks `NotResident` through
/// subscription publication.
async fn refresh_demand_driven_subscriptions(
    owner_node: &Node<groove::storage::DemandLoadedStorage>,
    runtime: &mut DemandDrivenNode,
) -> Result<usize, Error> {
    runtime.publish_query_runtime_updates()?;
    std::future::poll_fn(|context| {
        runtime
            .poll_acquire_resident(context, |state| {
                owner_node.prepare_subscription_refresh_inputs(state)
            })
            .map_err(Error::from)
    })
    .await?;
    let refreshed = owner_node.refresh_subscriptions()?;
    if refreshed > 0 {
        owner_node.mark_subscriber_connections_dirty();
    }
    Ok(refreshed)
}

impl DbOpen {
    #[doc(hidden)]
    pub fn new(
        schema: JazzSchema,
        identity: DbIdentity,
        persistence: Box<dyn groove::storage::async_ordered::AsyncOrderedKvStorage>,
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
        persistence: Box<dyn groove::storage::async_ordered::AsyncOrderedKvStorage>,
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
        persistence: Box<dyn groove::storage::async_ordered::AsyncOrderedKvStorage>,
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
    pub fn poll(&mut self, context: &mut Context<'_>) -> Poll<Result<Db, Error>> {
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
        let node = Rc::new(node);
        let row_id_source = Rc::new(RefCell::new(
            self.id_source
                .take()
                .unwrap_or_else(|| Box::<ProductionRowIdSource>::default()),
        ));
        Poll::Ready(Ok(Db {
            schema: self.schema.clone(),
            schema_version_id,
            schema_views,
            identity: self.identity,
            node,
            row_id_source,
            next_now_ms: Rc::new(Cell::new(1)),
            runtime,
        }))
    }
}

impl Db {
    pub(crate) fn next_now_ms(&self) -> u64 {
        let now = self.next_now_ms.get();
        self.next_now_ms.set(now.saturating_add(1));
        now
    }

    fn finalize_local_commit(&self, tx_id: TxId) -> Result<DurabilityTier, Error> {
        self.node.queue_pending_upload(tx_id, None);
        Ok(self.node.node.borrow().authored_commit_durability())
    }

    fn abandon_open_transaction(&self, tx_id: OpenBatchId) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .abandon_tx(tx_id)
            .map_err(Into::into)
    }

    pub(super) fn table_schema(&self, table: &str) -> Result<&TableSchema, Error> {
        self.schema
            .tables
            .iter()
            .find(|candidate| candidate.name == table)
            .ok_or_else(|| Error::new(ErrorCode::Schema, format!("unknown table {table}")))
    }

    pub(super) fn apply_insert_defaults(
        &self,
        table: &str,
        cells: RowCells,
    ) -> Result<RowCells, Error> {
        mutations::apply_insert_defaults_loaded(&self.schema, table, cells)
    }

    fn check_catalogue_admin(&self) -> Result<(), Error> {
        if self.identity.author == AuthorId::SYSTEM {
            Ok(())
        } else {
            Err(Error::new(
                ErrorCode::Protocol,
                "catalogue updates require a serving Node",
            ))
        }
    }

    fn prepare_insert_commit(
        &self,
        table: &str,
        cells: RowCells,
    ) -> Result<(RowUuid, MergeableCommit), Error> {
        let row = self.row_id_source.borrow_mut().next_row_id();
        let cells = self.apply_insert_defaults(table, cells)?;
        Ok((
            row,
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(self.identity.author)
                .cells(cells),
        ))
    }

    fn acquire_insert_target(
        node: &Node<groove::storage::DemandLoadedStorage>,
        table: &str,
        row: RowUuid,
    ) -> Result<(), MutationPrepareError> {
        let (content_parent, deletion_parent) = {
            let mut node = node.node.borrow_mut();
            let _empty_history = node
                .row_history(table, row)
                .map_err(MutationPrepareError::Node)?;
            (
                node.local_content_winner_tx_id(table, row)
                    .map_err(MutationPrepareError::Node)?,
                node.local_deletion_winner_tx_id(table, row)
                    .map_err(MutationPrepareError::Node)?,
            )
        };
        if deletion_parent.is_some() {
            return Err(MutationPrepareError::Api(row_already_deleted(row)));
        }
        if content_parent.is_some() {
            return Err(MutationPrepareError::Api(Error::new(
                ErrorCode::WriteRejected,
                format!("encoding error: object already exists: {}", row.0),
            )));
        }
        Ok(())
    }

    fn default_view_db(&mut self) -> Result<DbView<'_>, Error> {
        Ok(DbView {
            schema: self.schema.clone(),
            schema_version_id: self.schema_version_id,
            owner: self,
        })
    }

    pub(super) async fn refresh_subscriptions_prepared(&mut self) -> Result<usize, Error> {
        refresh_demand_driven_subscriptions(&self.node, &mut self.runtime).await
    }

    /// Open an ordinary database over a storage backend that completes the
    /// ordered asynchronous contract immediately.
    pub async fn open<S>(config: DbConfig<S>) -> Result<Self, Error>
    where
        S: OrderedKvStorage + ReopenableStorage + 'static,
    {
        let DbConfig {
            schema,
            storage,
            identity,
            id_source,
        } = config;
        let mut opening = DbOpen::new(
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
    pub async fn open_history_complete<S>(config: DbConfig<S>) -> Result<Self, Error>
    where
        S: OrderedKvStorage + ReopenableStorage + 'static,
    {
        let DbConfig {
            schema,
            storage,
            identity,
            id_source,
        } = config;
        let mut opening = DbOpen::new_history_complete(
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
    pub async fn open_catalogue_uninitialized<S>(config: DbConfig<S>) -> Result<Self, Error>
    where
        S: OrderedKvStorage + ReopenableStorage + 'static,
    {
        let DbConfig {
            storage,
            identity,
            id_source,
            ..
        } = config;
        let mut opening = DbOpen::new_catalogue_uninitialized(
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
        Query::from(table)
    }

    /// Compile a logical query. Durable source acquisition happens when the
    /// resulting query is read or subscribed, not while its shape is built.
    pub fn prepare_query(&self, query: &Query) -> Result<PreparedQuery, Error> {
        self.prepare_query_bound(query, BTreeMap::new())
    }

    pub fn prepare_query_bound(
        &self,
        query: &Query,
        params: BTreeMap<String, Value>,
    ) -> Result<PreparedQuery, Error> {
        let node = self.node.node.borrow();
        let current = node.current_write_schema().map_err(Error::from)?;
        let schema = if current.schema == self.schema_version_id {
            self.schema.clone()
        } else {
            node.catalogue_schemas()
                .get(&current.schema)
                .map(|schema| schema.schema.clone())
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::Schema,
                        format!(
                            "current write schema {:?} is missing from catalogue",
                            current.schema
                        ),
                    )
                })?
        };
        drop(node);
        reads::prepare_query_bound_loaded(&self.node, &schema, current.schema, query, params)
    }

    fn prepare_query_for_schema(
        &self,
        query: &Query,
        schema: &JazzSchema,
        schema_version: SchemaVersionId,
    ) -> Result<PreparedQuery, Error> {
        reads::prepare_query_loaded(&self.node, schema, schema_version, query)
    }

    /// Read the default typed view with the default local query options.
    pub async fn read(&mut self, prepared: &PreparedQuery) -> Result<Vec<CurrentRow>, Error> {
        self.all(prepared, ReadOpts::default()).await
    }

    #[cfg(any(test, feature = "testing"))]
    pub async fn read_profiled(
        &mut self,
        prepared: &PreparedQuery,
    ) -> Result<(Vec<CurrentRow>, QueryReadProfile), Error> {
        let node = Rc::clone(&self.node);
        std::future::poll_fn(|context| {
            self.runtime.poll_resident_operation(context, || {
                let mut state = node.node.borrow_mut();
                let token = state.groove_runtime_token();
                state.query_rows_local_preview_profiled(
                    &prepared.shape,
                    &prepared.binding,
                    prepared.plan_for_tier(DurabilityTier::Local, token),
                )
            })
        })
        .await
        .map_err(Into::into)
    }

    /// Read at most one row from the default typed view.
    pub async fn one(&mut self, prepared: &PreparedQuery) -> Result<Option<CurrentRow>, Error> {
        let mut rows = self.read(prepared).await?;
        if rows.len() > 1 {
            return Err(Error::new(
                ErrorCode::Query,
                format!("expected at most one row, got {}", rows.len()),
            ));
        }
        Ok(rows.pop())
    }

    /// Run a one-shot query as an explicit terminated-session identity.
    pub async fn all_for_identity(
        &mut self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.default_view_db()?
            .all_for_identity(prepared, opts, author)
            .await
    }

    /// Insert one caller-selected row through the default typed view.
    pub async fn insert_with_id(
        &mut self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        self.default_view_db()?
            .insert_with_id(table, row, cells, None, None)
            .await
    }

    pub async fn insert_with_id_at_ms(
        &mut self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        self.default_view_db()?
            .insert_with_id(table, row, cells, None, Some(now_ms))
            .await
    }

    pub async fn all_relation_snapshot(
        &mut self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        self.relation_snapshot(prepared, opts).await
    }

    pub async fn insert_with_id_for_identity(
        &mut self,
        author: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        self.default_view_db()?
            .insert_with_id(table, row, cells, Some(author), None)
            .await
    }

    pub async fn insert_for_identity(
        &mut self,
        author: AuthorId,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        let row = self.row_id_source.borrow_mut().next_row_id();
        self.default_view_db()?
            .insert_with_id(table, row, cells, Some(author), None)
            .await
    }

    pub async fn upsert_for_identity(
        &mut self,
        author: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        self.default_view_db()?
            .upsert(table, row, cells, Some(author), None)
            .await
    }

    pub async fn update_for_identity(
        &mut self,
        author: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        self.default_view_db()?
            .update(table, row, cells, Some(author), None)
            .await
    }

    pub async fn delete_for_identity(
        &mut self,
        author: AuthorId,
        table: &str,
        row: RowUuid,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        self.default_view_db()?
            .delete(table, row, Some(author), None)
            .await
    }

    #[cfg(any(test, feature = "testing"))]
    pub async fn update_at_ms(
        &mut self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        self.default_view_db()?
            .update(table, row, cells, None, Some(now_ms))
            .await
    }

    #[cfg(any(test, feature = "testing"))]
    pub async fn delete_at_ms(
        &mut self,
        table: &str,
        row: RowUuid,
        now_ms: u64,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        self.default_view_db()?
            .delete(table, row, None, Some(now_ms))
            .await
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn tick_stats(&self) -> Result<DbTickStats, Error> {
        self.node.tick()
    }

    #[cfg(feature = "testing")]
    pub fn reset_storage_read_metrics_for_test(&self) {
        self.node.node.borrow().reset_storage_read_metrics();
    }

    #[cfg(feature = "testing")]
    pub fn take_storage_read_metrics_for_test(&self) -> groove::db::StorageReadMetrics {
        self.node.node.borrow().take_storage_read_metrics()
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn active_groove_subscriptions_for_test(&self) -> usize {
        self.runtime_stats_for_test().active_subscriptions
    }

    pub fn set_identity_claims(
        &self,
        author: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Result<(), Error> {
        let changed = {
            let mut node = self.node.node.borrow_mut();
            let previous_revision = node.session_claim_revision(author);
            node.set_session_claims(author, claims);
            node.session_claim_revision(author) != previous_revision
        };
        if changed {
            self.node.schedule_tick(TickUrgency::Deferred);
        }
        Ok(())
    }

    pub fn can_insert(&self, _table: &str, _cells: RowCells) -> Result<PermissionAdvice, Error> {
        Ok(PermissionAdvice::Unknown)
    }

    pub fn can_read(&self, _table: &str, _row: RowUuid) -> Result<PermissionAdvice, Error> {
        Ok(PermissionAdvice::Unknown)
    }

    pub fn can_update(&self, _table: &str, _row: RowUuid) -> Result<PermissionAdvice, Error> {
        Ok(PermissionAdvice::Unknown)
    }

    pub fn can_delete(&self, _table: &str, _row: RowUuid) -> Result<PermissionAdvice, Error> {
        Ok(PermissionAdvice::Unknown)
    }

    pub fn attach_query_with_opts(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<QueryAttachment, Error> {
        self.node
            .attach_query_with_opts(prepared, opts, self.identity.author)
    }

    pub fn attach_query_with_opts_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<QueryAttachment, Error> {
        self.node.attach_query_with_opts(prepared, opts, author)
    }

    pub fn query_attachment_is_covered(&self, attachment: &QueryAttachment) -> bool {
        self.node.query_attachment_is_covered(attachment)
    }

    pub fn detach_query(&self, attachment: QueryAttachment) {
        self.node.detach_query(attachment);
    }

    #[cfg(feature = "testing")]
    pub async fn flush_for_test(&mut self) -> Result<(), Error> {
        self.runtime.publish_query_runtime_updates()?;
        std::future::poll_fn(|context| self.runtime.poll_persistence(context))
            .await
            .map_err(Error::from)?;
        Ok(self.node.node.borrow_mut().flush_query_runtime()?)
    }

    #[cfg(feature = "testing")]
    pub fn encoded_storage_bytes_for_test(&self) -> Result<u64, Error> {
        Ok(self.node.node.borrow().encoded_storage_bytes_for_test()?)
    }

    #[cfg(feature = "testing")]
    pub fn sync_metrics_for_test(&self) -> crate::node::SyncMetrics {
        self.node.node.borrow().sync_metrics().clone()
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn runtime_stats_for_test(&self) -> groove::ivm::RuntimeStats {
        self.node.node.borrow().runtime_stats_for_test()
    }

    #[cfg(feature = "testing")]
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
                    snapshot_bytes: encode_relation_snapshot_for_size(snapshot)
                        .map(|bytes| bytes.len())
                        .unwrap_or_default(),
                    reset_frame_bytes: encode_subscription_reset_frame_for_size(snapshot)
                        .map(|bytes| bytes.len())
                        .unwrap_or_default(),
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

    /// Return the typed view selected when this owner opened.
    pub fn default_view(&self) -> DbSchemaView {
        DbSchemaView {
            schema_view_id: SchemaViewId::for_schema(&self.schema),
        }
    }

    /// Recover an already-registered typed schema selection by identity.
    pub fn schema_view(&self, schema_view_id: SchemaViewId) -> Result<DbSchemaView, Error> {
        if self.schema_views.borrow().contains_key(&schema_view_id) {
            Ok(DbSchemaView { schema_view_id })
        } else {
            Err(Error::new(
                ErrorCode::Schema,
                format!("schema view {schema_view_id:?} is not registered"),
            ))
        }
    }

    /// Admit and register a typed schema, then return an inert selection token.
    /// Catalogue control state is a startup-resident invariant; only its
    /// resulting ordered write may suspend here.
    pub async fn register_schema_view(
        &mut self,
        schema: JazzSchema,
    ) -> Result<DbSchemaView, Error> {
        self.register_schema_view_resident(schema.clone())?;
        let schema_view_id = SchemaViewId::for_schema(&schema);
        std::future::poll_fn(|context| self.runtime.poll_persistence(context))
            .await
            .map_err(Error::from)?;
        Ok(DbSchemaView { schema_view_id })
    }

    fn schema_for_view(&self, view: &DbSchemaView) -> Result<JazzSchema, Error> {
        let registered = self
            .schema_views
            .borrow()
            .get(&view.schema_view_id)
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::Schema,
                    format!("schema view {:?} is not registered", view.schema_view_id),
                )
            })?;
        let version = registered.version_id();
        let node = self.node.node.borrow();
        let admitted = node
            .catalogue_schemas()
            .get(&version)
            .ok_or_else(|| Error::new(ErrorCode::Schema, "registered schema is missing"))?;
        Ok(schema_with_authoritative_runtime_metadata(
            registered,
            &admitted.schema,
        ))
    }

    fn register_schema_view_resident(&mut self, schema: JazzSchema) -> Result<(), Error> {
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
        self.schema_version_id = schema_version_id;
        self.schema = schema;
        Ok(())
    }

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

    /// Borrow an operational typed view from this owner. The facade cannot
    /// outlive the owner borrow and therefore cannot become a second owner.
    pub fn view<'a>(&'a mut self, view: &DbSchemaView) -> Result<DbView<'a>, Error> {
        let schema = self.schema_for_view(view)?;
        Ok(DbView {
            schema_version_id: schema.version_id(),
            schema,
            owner: self,
        })
    }

    /// Compile a query against an explicit typed view of this owner.
    pub fn prepare_query_in_view(
        &self,
        view: &DbSchemaView,
        query: &Query,
    ) -> Result<PreparedQuery, Error> {
        let schema = self.schema_for_view(view)?;
        self.prepare_query_for_schema(query, &schema, schema.version_id())
    }

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

    pub fn row_provenance(&self, row: &CurrentRow) -> Result<Option<RowProvenance>, Error> {
        self.node
            .node
            .borrow_mut()
            .row_provenance(row)
            .map_err(Into::into)
    }

    pub fn next_write_state_change(&self, tx_id: TxId) -> WriteStateChange {
        self.node.register_write_state_waiter(tx_id)
    }

    pub fn wait_for_transaction_with(
        &self,
        tx_id: TxId,
        tier: DurabilityTier,
        callback: impl FnOnce(Result<TxId, Error>) + 'static,
    ) {
        self.node
            .wait_for_transaction_with(tx_id, tier, Box::new(callback));
    }

    /// Wait until an observed transaction reaches the requested durability.
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

    /// Historical reads require a serving authority rather than a partial local owner.
    pub async fn at(
        &mut self,
        _seq: GlobalSeq,
        _prepared: &PreparedQuery,
    ) -> Result<Vec<CurrentRow>, Error> {
        Err(Error::new(
            ErrorCode::HistoricalReadRequiresServer,
            "historical read requires server evaluation",
        ))
    }

    /// Attribute a local insert only to the authenticated local identity.
    pub async fn insert_attributed(
        &mut self,
        made_by: AuthorId,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        if made_by != self.identity.author {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                "client writes cannot attribute provenance to another identity",
            ));
        }
        self.insert(table, cells).await
    }

    pub fn set_tick_scheduler(&self, scheduler: Option<Rc<dyn TickScheduler>>) {
        self.node.set_scheduler(scheduler);
    }

    pub fn on_mutation_error(&self, callback: MutationErrorCallback) {
        self.node.set_mutation_error_callback(Some(callback));
    }

    pub fn request_permission_advice(
        &self,
        action: PermissionAdviceAction,
    ) -> PermissionAdviceFuture {
        self.node.request_permission_advice(action)
    }

    pub fn cancel_permission_advice_request(&self, request_id: PermissionAdviceRequestId) {
        self.node.cancel_permission_advice_request(request_id);
    }

    pub fn set_non_durable_client(&self) {
        self.node.set_non_durable_client();
    }

    pub fn set_upstream_durability_floor(&self, tier: DurabilityTier) {
        self.node.set_upstream_durability_floor(tier);
    }

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

    pub fn abandon_transaction(&mut self, tx_id: OpenBatchId) -> Result<(), Error> {
        self.abandon_open_transaction(tx_id)
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
        Ok(self.node.node.borrow().catalogue_snapshot()?)
    }

    #[cfg(any(feature = "runtime", test))]
    pub fn trusted_current_catalogue_schema(&self) -> Result<JazzSchema, Error> {
        let node = self.node.node.borrow();
        let pointer = node.current_write_schema()?;
        node.catalogue_schemas()
            .get(&pointer.schema)
            .map(|schema| schema.schema.clone())
            .ok_or_else(|| Error::new(ErrorCode::Schema, "active catalogue schema is missing"))
    }

    #[cfg(any(feature = "runtime", test))]
    pub fn catalogue_bootstrap_is_ready(&self) -> bool {
        self.node.node.borrow().catalogue_bootstrap_state()
            == crate::node::CatalogueBootstrapState::Ready
    }

    async fn apply_trusted_catalogue_message(
        &mut self,
        message: SyncMessage,
    ) -> Result<Vec<SyncMessage>, Error> {
        self.check_catalogue_admin()?;
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
            author: self.identity.author,
            schema: Box::new(schema),
        })
        .await
    }

    pub async fn publish_lens(&mut self, lens: MigrationLens) -> Result<Vec<SyncMessage>, Error> {
        self.apply_trusted_catalogue_message(SyncMessage::PublishLens {
            author: self.identity.author,
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
            author: self.identity.author,
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
            author: self.identity.author,
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
        let cells = self.apply_insert_defaults(table, cells)?;
        let commit = MergeableCommit::new(table, row, self.next_now_ms())
            .made_by(made_by)
            .cells(cells);
        let schema = self.schema_version_id;
        let tx_id = std::future::poll_fn(|context| {
            self.runtime
                .poll_mergeable_commit_in_schema(context, schema, &commit)
        })
        .await
        .map_err(Error::from)?;
        let SyncMessage::CommitUnit { tx, versions } =
            self.node.node.borrow_mut().commit_unit_for(tx_id)?
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
        self.refresh_subscriptions_prepared().await?;
        self.node.mark_subscriber_connections_dirty();
        Ok(tx_id)
    }

    /// Attach an authority-admitted typed schema to this owner.
    pub fn select_schema_view(&mut self, schema: JazzSchema) -> Result<(), Error> {
        self.register_schema_view_resident(schema)?;
        Ok(())
    }

    pub fn set_edge_cache_budget(&self, budget: Option<crate::node::EdgeCacheBudget>) {
        self.node.set_edge_cache_budget(budget);
    }

    pub fn current_write_schema(&self) -> Result<CurrentWriteSchema, Error> {
        self.node
            .node
            .borrow()
            .current_write_schema()
            .map_err(Into::into)
    }

    pub fn catalogue_schema(&self, schema: SchemaVersionId) -> Option<JazzSchema> {
        self.node
            .node
            .borrow()
            .catalogue_schemas()
            .get(&schema)
            .map(|schema| schema.schema.clone())
    }

    pub fn active_catalogue_seq(&self) -> u64 {
        self.node.node.borrow().active_catalogue_seq()
    }

    pub fn catalogue_lens(&self, lens: MigrationLensId) -> Option<MigrationLens> {
        self.node
            .node
            .borrow()
            .catalogue_lenses()
            .get(&lens)
            .cloned()
    }

    pub fn set_permissions_ready(&self, ready: bool) -> Result<(), Error> {
        self.node.set_permissions_ready(ready)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn set_catalogue_activation_failpoint(
        &self,
        failpoint: crate::node::CatalogueActivationFailpoint,
    ) {
        self.node
            .node
            .borrow_mut()
            .set_catalogue_activation_failpoint(failpoint);
    }

    pub async fn seed_branch_mergeable_for_bootstrap(
        &mut self,
        branch: BranchId,
        table: &str,
        row: RowUuid,
        made_by: AuthorId,
        cells: RowCells,
    ) -> Result<TxId, Error> {
        if self.node.node.borrow().branch_record(branch).is_none() {
            self.create_branch_with_id(branch).await?;
        }
        let cells = self.apply_insert_defaults(table, cells)?;
        let commit = MergeableCommit::new(table, row, self.next_now_ms())
            .made_by(made_by)
            .cells(cells);
        let schema = self.schema_version_id;
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
        let SyncMessage::CommitUnit { tx, versions } =
            self.node.node.borrow_mut().commit_unit_for(tx_id)?
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
        self.refresh_subscriptions_prepared().await?;
        self.node.mark_subscriber_connections_dirty();
        Ok(tx_id)
    }

    pub async fn tick(&mut self) -> Result<DbTickStats, Error> {
        if self.node.deliver_pending_mutation_errors() {
            std::future::poll_fn(|context| self.runtime.poll_persistence(context))
                .await
                .map_err(Error::from)?;
        }
        std::future::poll_fn(|context| self.poll_tick(context)).await
    }

    /// Exercise only the synchronous resident peer phase. External durable
    /// frames must remain staged for the asynchronous owner around this phase.
    #[cfg(test)]
    pub(crate) fn resident_peer_tick_for_test(&self) -> Result<DbTickStats, Error> {
        for connection in self.node.connections.borrow().iter() {
            connection.borrow_mut().stage_available_inbound();
        }
        self.node.tick()
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
        let connection = self.node.connect_upstream(transport);
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
        let connection = self.node.accept_subscriber(transport, identity);
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
            .node
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
            .node
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
            .node
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
            .node
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
            .node
            .accept_subscriber_with_resume(transport, identity, cursor);
        connection.borrow_mut().enable_external_durable_ingress();
        connection
    }

    pub fn detach_connection(
        &self,
        connection: &Rc<RefCell<PeerConnection<groove::storage::DemandLoadedStorage>>>,
    ) -> bool {
        self.node.detach_connection(connection)
    }

    /// Drive peer work without replaying a frame across an asynchronous
    /// storage suspension. At most one durable frame owns the persistence
    /// boundary at a time; ordinary connection-control traffic continues
    /// through the resident peer tick.
    pub fn poll_tick(&mut self, context: &mut Context<'_>) -> Poll<Result<DbTickStats, Error>> {
        let connections = self.node.connections.borrow().clone();
        for connection in &connections {
            connection.borrow_mut().stage_available_inbound();
            match self.runtime.poll_acquire_resident(context, |node| {
                connection
                    .borrow_mut()
                    .prepare_local_subscriber_restore(node)
            }) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                Poll::Ready(Ok(())) => {}
            }
            match self.runtime.poll_acquire_resident(context, |node| {
                connection
                    .borrow_mut()
                    .prepare_pending_upstream_inputs(node)
            }) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                Poll::Ready(Ok(())) => {}
            }
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
                    self.node.mark_subscriber_connections_dirty();
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
                // this same link. Yield before the synchronous resident peer
                // tick can consume it; the next owner poll acquires and
                // publishes it through the typed receiver boundary.
                if connections
                    .iter()
                    .any(|connection| connection.borrow().has_externally_applied_inbound())
                {
                    self.node.mark_subscriber_connections_dirty();
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
                        Poll::Ready(Err(error)) => {
                            connection.borrow_mut().complete_staged_view_update(&parts);
                            return Poll::Ready(Err(error.into()));
                        }
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
            // outputs; the synchronous resident peer tick runs after
            // acquisition and is therefore too late to arm this owner poll.
            self.node.mark_subscriber_connections_dirty();
        }

        if let Err(error) = self.runtime.publish_query_runtime_updates() {
            return Poll::Ready(Err(error.into()));
        }

        // Typed ingress above may change any subscriber's maintained view.
        // Acquire local publication and outbound serving witnesses in one
        // residency attempt: a later acquisition replaces the demand-loaded
        // working set and would otherwise evict inputs needed by the resident
        // protocol tick below.
        match self.runtime.poll_acquire_resident(context, |node| {
            for connection in &connections {
                connection
                    .borrow_mut()
                    .prepare_subscriber_serving_inputs(node)?;
            }
            self.node.prepare_subscription_refresh_inputs(node)?;
            Ok(())
        }) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
            Poll::Ready(Ok(())) => {}
        }

        let parked_tails = connections
            .iter()
            .map(|connection| connection.borrow_mut().park_staged_inbound_tail())
            .collect::<Vec<_>>();
        let tick_result = self.node.tick_after_mutation_error_delivery();
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
        let node = Rc::clone(&self.node);
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || node.node.borrow_mut().close(),
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
        let author = self.identity.author;
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
        let (row_uuid, commit) = self.prepare_insert_commit(table, cells)?;
        let schema = self.schema_version_id;
        let tx_id = std::future::poll_fn(|context| {
            self.runtime.poll_mergeable_many_on_branch_in_schema(
                context,
                branch,
                schema,
                std::slice::from_ref(&commit),
            )
        })
        .await?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions_prepared().await?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid,
            tx_id,
            local_tier,
        })
    }

    #[cfg(test)]
    pub(crate) fn resident_node_for_test(
        &self,
    ) -> Rc<RefCell<crate::node::NodeState<groove::storage::DemandLoadedStorage>>> {
        Rc::clone(&self.node.node)
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
        let node = Rc::clone(&self.node);
        let author = self.identity.author;
        match self.runtime.poll_resident_operation(context, || {
            if let ReadViewSourceSpec::Branch { branch } = &opts.read_view.source {
                node.node.borrow_mut().acquire_branch_read_inputs(
                    &prepared.shape,
                    &prepared.binding,
                    crate::ids::BranchId(*branch),
                    author,
                    false,
                )?;
            }
            reads::all_loaded(
                &node,
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
        view: &DbSchemaView,
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
        let node = Rc::clone(&self.node);
        let author = self.identity.author;
        match self.runtime.poll_resident_operation(context, || {
            if let ReadViewSourceSpec::Branch { branch } = &opts.read_view.source {
                node.node.borrow_mut().acquire_branch_read_inputs(
                    &prepared.shape,
                    &prepared.binding,
                    crate::ids::BranchId(*branch),
                    author,
                    false,
                )?;
            }
            reads::relation_snapshot_loaded(
                &node,
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
        view: &DbSchemaView,
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

    /// Materialize the canonical public result tree for a one-shot query.
    pub async fn all_result_tree(
        &mut self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<ResultTree, Error> {
        self.result_tree(prepared, opts).await
    }

    /// Materialize a public result tree through an explicit typed view.
    pub async fn result_tree_in_view(
        &mut self,
        view: &DbSchemaView,
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
        let author = self.identity.author;
        let node = Rc::clone(&self.node);
        match self.runtime.poll_operation(
            context,
            || {
                node.open_subscription_resident(
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

    /// Open a trusted-serving subscription as an explicit identity.
    pub async fn subscribe_for_identity(
        &mut self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<SubscriptionStream, Error> {
        let node = Rc::clone(&self.node);
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    node.open_subscription_resident(
                        prepared,
                        opts.clone(),
                        author,
                        QueryAuthorizationMode::TrustedServing,
                    )
                },
                SubscriptionOpenError::missing_input,
            )
        })
        .await
        .map_err(SubscriptionOpenError::into_api)
    }

    /// Open a maintained subscription for an output-changing relation query.
    pub async fn subscribe_relation_query(
        &mut self,
        query: &RelationQuery,
        opts: ReadOpts,
    ) -> Result<SubscriptionStream, Error> {
        let prepared = self.prepare_query(&relation_query_to_query(query)?)?;
        self.subscribe(&prepared, opts).await
    }

    /// Materialize an output-changing relation query through the default view.
    pub async fn all_relation_query(
        &mut self,
        query: &RelationQuery,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        self.default_view_db()?
            .all_relation_query(query, opts, None)
            .await
    }

    /// Materialize a relation snapshot as an explicit serving identity.
    pub async fn all_relation_snapshot_for_identity(
        &mut self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<RelationSnapshot, Error> {
        self.default_view_db()?
            .relation_snapshot_for_identity(prepared, opts, author)
            .await
    }

    /// Open a subscription through an explicit typed view.
    pub async fn subscribe_in_view(
        &mut self,
        view: &DbSchemaView,
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
        let (row_uuid, commit) = self.prepare_insert_commit(table, cells)?;
        self.table_schema(table)?;
        let node = Rc::clone(&self.node);
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || Self::acquire_insert_target(&node, table, row_uuid),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)?;
        let schema = self.schema_version_id;
        let tx_id = std::future::poll_fn(|context| {
            self.runtime
                .poll_mergeable_commit_in_schema(context, schema, &commit)
        })
        .await
        .map_err(Error::from)?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions_prepared().await?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
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
        let schema = self.schema.clone();
        let node = Rc::clone(&self.node);
        let identity = self.identity;
        if patch.is_empty() {
            let (tx_id, local_tier) = std::future::poll_fn(|context| {
                self.runtime.poll_operation(
                    context,
                    || {
                        mutations::prepare_noop_update_loaded(
                            &schema,
                            &node,
                            table,
                            row,
                            identity.author,
                        )
                    },
                    MutationPrepareError::missing_input,
                )
            })
            .await
            .map_err(MutationPrepareError::into_api)?;
            return Ok(WriteHandle {
                node: Rc::downgrade(&self.node.node),
                row_uuid: row,
                tx_id,
                local_tier,
            });
        }
        let now_ms = self.next_now_ms();
        let prepared = std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    mutations::prepare_update_loaded(
                        &schema,
                        &node,
                        identity,
                        table,
                        row,
                        patch.clone(),
                        now_ms,
                        identity.author,
                    )
                },
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)?;
        let schema = self.schema_version_id;
        let tx_id = std::future::poll_fn(|context| {
            self.runtime
                .poll_mergeable_commit_in_schema(context, schema, &prepared)
        })
        .await
        .map_err(Error::from)?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions_prepared().await?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
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
        let now_ms = self.next_now_ms();
        let schema = self.schema.clone();
        let node = Rc::clone(&self.node);
        let identity = self.identity;
        let prepared = std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || mutations::prepare_delete_loaded(&schema, &node, identity, table, row, now_ms),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)?;
        let schema = self.schema_version_id;
        let tx_id = std::future::poll_fn(|context| {
            self.runtime
                .poll_mergeable_commit_in_schema(context, schema, &prepared)
        })
        .await
        .map_err(Error::from)?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions_prepared().await?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
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
        let now_ms = self.next_now_ms();
        let schema = self.schema.clone();
        let node = Rc::clone(&self.node);
        let identity = self.identity;
        let prepared = std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    mutations::prepare_restore_loaded(
                        &schema,
                        &node,
                        identity,
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
        .map_err(MutationPrepareError::into_api)?;
        let schema = self.schema_version_id;
        let tx_id = std::future::poll_fn(|context| {
            self.runtime
                .poll_mergeable_many_in_schema(context, schema, &prepared)
        })
        .await
        .map_err(Error::from)?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions_prepared().await?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
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
        let now_ms = self.next_now_ms();
        let schema = self.schema.clone();
        let node = Rc::clone(&self.node);
        let identity = self.identity;
        let prepared = std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    mutations::prepare_upsert_loaded(
                        &schema,
                        &node,
                        identity,
                        table,
                        row,
                        cells.clone(),
                        now_ms,
                        identity.author,
                    )
                },
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)?;
        let schema = self.schema_version_id;
        let tx_id = std::future::poll_fn(|context| {
            self.runtime
                .poll_mergeable_commit_in_schema(context, schema, &prepared)
        })
        .await
        .map_err(Error::from)?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions_prepared().await?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    /// Open a staged mergeable transaction owned by this async database.
    pub async fn begin_mergeable(&mut self) -> Result<OpenBatchId, Error> {
        let id = OpenBatchId::new();
        self.begin_mergeable_with_id(id).await?;
        Ok(id)
    }

    /// Open a caller-addressed staged mergeable transaction.
    pub async fn begin_mergeable_with_id(&mut self, id: OpenBatchId) -> Result<(), Error> {
        let node = Rc::clone(&self.node);
        let _schema = self.schema.clone();
        let _schema_version = self.schema_version_id;
        let identity = self.identity;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || transactions::begin_mergeable_loaded(&node, id, identity.author),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
    }

    /// Stage an insert in an open mergeable transaction. Staged writes remain
    /// invisible until [`Db::commit_mergeable`] publishes them.
    pub async fn mergeable_insert(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<(), Error> {
        let now_ms = self.next_now_ms();
        let node = Rc::clone(&self.node);
        let schema = self.schema.clone();
        let schema_version = self.schema_version_id;
        let _identity = self.identity;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    transactions::stage_mergeable_insert_loaded(
                        &schema,
                        &node,
                        schema_version,
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
        let now_ms = self.next_now_ms();
        let node = Rc::clone(&self.node);
        let _schema = self.schema.clone();
        let schema_version = self.schema_version_id;
        let _identity = self.identity;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    transactions::stage_mergeable_update_loaded(
                        &node,
                        schema_version,
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
        let now_ms = self.next_now_ms();
        let node = Rc::clone(&self.node);
        let _schema = self.schema.clone();
        let schema_version = self.schema_version_id;
        let _identity = self.identity;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    transactions::stage_mergeable_delete_loaded(
                        &node,
                        schema_version,
                        tx_id,
                        table,
                        row,
                        now_ms,
                    )
                },
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
        let now_ms = self.next_now_ms();
        let node = Rc::clone(&self.node);
        let schema = self.schema.clone();
        let schema_version = self.schema_version_id;
        let _identity = self.identity;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    transactions::stage_mergeable_restore_loaded(
                        &schema,
                        &node,
                        schema_version,
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
        let node = Rc::clone(&self.node);
        let _schema = self.schema.clone();
        let _schema_version = self.schema_version_id;
        let _identity = self.identity;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || transactions::transaction_all_loaded(&node, tx_id, prepared, &opts, None),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
    }

    /// Read one row with this open mergeable batch's staged writes overlaid.
    pub async fn mergeable_read(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<RowCells>, Error> {
        let node = Rc::clone(&self.node);
        let schema_version = self.schema_version_id;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || transactions::exclusive_read_loaded(&node, schema_version, tx_id, table, row),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
    }

    pub async fn transaction_all_for_identity(
        &mut self,
        tx_id: OpenBatchId,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let node = Rc::clone(&self.node);
        let _schema = self.schema.clone();
        let _schema_version = self.schema_version_id;
        let _identity = self.identity;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    transactions::transaction_all_loaded(
                        &node,
                        tx_id,
                        prepared,
                        &opts,
                        Some(author),
                    )
                },
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
    }

    /// Publish every staged write as one resident and durable transaction.
    pub async fn commit_mergeable(&mut self, tx_id: OpenBatchId) -> Result<TxId, Error> {
        let fallback_count = self
            .node
            .node
            .borrow()
            .mergeable_open_missing_timestamp_count(tx_id)?;
        let fallback_now_ms = (0..fallback_count)
            .map(|_| self.next_now_ms())
            .collect::<Vec<_>>();
        let committed = std::future::poll_fn(|context| {
            self.runtime
                .poll_mergeable_open(context, tx_id, &fallback_now_ms)
        })
        .await
        .map_err(Error::from)?;
        self.finalize_local_commit(committed)?;
        self.refresh_subscriptions_prepared().await?;
        Ok(committed)
    }

    /// Abandon a staged transaction without publishing any of its writes.
    pub fn abandon_mergeable(&mut self, tx_id: OpenBatchId) -> Result<(), Error> {
        self.abandon_open_transaction(tx_id)
    }

    /// Open a serializable transaction over the current local snapshot.
    pub async fn begin_exclusive(&mut self) -> Result<OpenBatchId, Error> {
        let id = OpenBatchId::new();
        let node = Rc::clone(&self.node);
        let _schema = self.schema.clone();
        let _schema_version = self.schema_version_id;
        let identity = self.identity;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || transactions::begin_exclusive_loaded(&node, id, identity.author),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)?;
        Ok(id)
    }

    pub async fn begin_exclusive_with_id(&mut self, id: OpenBatchId) -> Result<(), Error> {
        let node = Rc::clone(&self.node);
        let author = self.identity.author;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || transactions::begin_exclusive_loaded(&node, id, author),
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
        let node = Rc::clone(&self.node);
        let _schema = self.schema.clone();
        let schema_version = self.schema_version_id;
        let _identity = self.identity;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || transactions::exclusive_read_loaded(&node, schema_version, tx_id, table, row),
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
        let now_ms = self.next_now_ms();
        let node = Rc::clone(&self.node);
        let schema = self.schema.clone();
        let schema_version = self.schema_version_id;
        let _identity = self.identity;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    transactions::stage_exclusive_insert_loaded(
                        &schema,
                        &node,
                        schema_version,
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
        let now_ms = self.next_now_ms();
        let node = Rc::clone(&self.node);
        let _schema = self.schema.clone();
        let schema_version = self.schema_version_id;
        let _identity = self.identity;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    transactions::stage_exclusive_delete_loaded(
                        &node,
                        schema_version,
                        tx_id,
                        table,
                        row,
                        now_ms,
                    )
                },
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
        let now_ms = self.next_now_ms();
        let node = Rc::clone(&self.node);
        let schema = self.schema.clone();
        let schema_version = self.schema_version_id;
        let _identity = self.identity;
        std::future::poll_fn(|context| {
            self.runtime.poll_operation(
                context,
                || {
                    transactions::stage_exclusive_restore_loaded(
                        &schema,
                        &node,
                        schema_version,
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
        let now_ms = self.next_now_ms();
        let made_by = self.identity.author;
        let (committed, unit) = std::future::poll_fn(|context| {
            self.runtime
                .poll_exclusive_open(context, tx_id, made_by, now_ms)
        })
        .await
        .map_err(Error::from)?;
        self.node.queue_pending_upload(committed, Some(unit));
        self.refresh_subscriptions_prepared().await?;
        Ok(committed)
    }

    pub fn abandon_exclusive(&mut self, tx_id: OpenBatchId) -> Result<(), Error> {
        self.abandon_open_transaction(tx_id)
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
pub struct MaintainedSubscriptionSizeReceipt {
    pub name: String,
    pub shape_id: uuid::Uuid,
    pub binding_id: uuid::Uuid,
    pub rows: usize,
    pub root_rows: usize,
    pub relation_edges: usize,
    pub footprint: DbMaintainedSubscriptionFootprint,
    pub snapshot_bytes: usize,
    pub reset_frame_bytes: usize,
    pub validation_tuple_estimate_bytes: usize,
}

#[cfg(feature = "testing")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
/// Test/bench-only approximate heap footprint for a maintained subscription.
pub struct DbMaintainedSubscriptionFootprint {
    /// Active result-current rows in the maintained index.
    pub result_rows: usize,
    /// Result weight map entries, including transient non-positive entries.
    pub result_weights: usize,
    /// Projected or synthetic result payload entries.
    pub result_payloads: usize,
    /// Active readable version identities.
    pub version_identities: usize,
    /// Entries reachable through the version-by-transaction index.
    pub version_tx_entries: usize,
    /// Active replacement winner entries.
    pub replacement_entries: usize,
    /// Approximate heap bytes retained by result weights.
    pub result_weights_bytes: usize,
    /// Approximate heap bytes retained by result payloads.
    pub result_payloads_bytes: usize,
    /// Approximate heap bytes retained by version indexes.
    pub versions_bytes: usize,
    /// Approximate heap bytes retained by replacement indexes.
    pub replacements_bytes: usize,
    /// Approximate heap bytes retained by maintained-view indexes.
    pub maintained_heap_bytes: usize,
    /// Lowered terminal schema count.
    pub terminal_schemas: usize,
    /// Approximate heap bytes retained by terminal schemas.
    pub terminal_schemas_bytes: usize,
    /// Table schema count retained by the subscription.
    pub tables: usize,
    /// Local result-set member count.
    pub result_set: usize,
    /// Local result payload count.
    pub local_result_payloads: usize,
    /// Local program fact count.
    pub program_facts: usize,
    /// Groove delta batches awaiting durable witnesses.
    pub pending_delta_batches: usize,
    /// Approximate heap bytes retained by pending delta batches.
    pub pending_delta_bytes: usize,
    /// Approximate heap bytes retained by local control state.
    pub control_state_bytes: usize,
    /// Maintained plus local control-state heap bytes.
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
            pending_delta_batches: footprint.pending_delta_batches,
            pending_delta_bytes: footprint.pending_delta_bytes,
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
    snapshot: &RelationSnapshot,
) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&SizeSubscriptionDelta {
        added: size_row_batches(&snapshot.rows),
    })
}

#[cfg(feature = "testing")]
fn size_row_batches(rows: &[CurrentRow]) -> Vec<SizeRowBatch<'_>> {
    let mut batches = Vec::<SizeRowBatch<'_>>::new();
    for row in rows {
        let (descriptor, raw) = row.encoded_record();
        match batches.last_mut() {
            Some(batch) if batch.table == row.table() && batch.descriptor == *descriptor => {
                batch.rows.push(SizeRow {
                    row_id: row.row_uuid(),
                    deleted: row.is_deleted(),
                    raw,
                });
            }
            _ => batches.push(SizeRowBatch {
                table: row.table(),
                descriptor: *descriptor,
                rows: vec![SizeRow {
                    row_id: row.row_uuid(),
                    deleted: row.is_deleted(),
                    raw,
                }],
            }),
        }
    }
    batches
}

#[cfg(feature = "testing")]
fn validation_tuple_estimate_bytes(
    shape: &ValidatedQuery,
    binding: &Binding,
    author: AuthorId,
    tier: DurabilityTier,
    read_view: &ReadViewSpec,
) -> usize {
    postcard::to_allocvec(&(
        shape.shape_id().0,
        binding.binding_id().0,
        shape.schema_version(),
        shape.canonical_bytes(),
        binding.canonical_bytes(),
        author,
        tier,
        read_view,
    ))
    .map(|bytes| bytes.len())
    .unwrap_or_default()
}

impl DbView<'_> {
    fn next_now_ms(&self) -> u64 {
        self.owner.next_now_ms()
    }

    async fn refresh_subscriptions_prepared(&mut self) -> Result<usize, Error> {
        refresh_demand_driven_subscriptions(&self.owner.node, &mut self.owner.runtime).await
    }

    /// Start a logical query in this typed view without loading storage.
    pub fn table(&self, table: impl Into<String>) -> Query {
        Query::from(table)
    }

    /// Compile a logical query in this typed view.
    pub fn prepare_query(&self, query: &Query) -> Result<PreparedQuery, Error> {
        self.owner
            .prepare_query_for_schema(query, &self.schema, self.schema_version_id)
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
        let _node = Rc::clone(&self.owner.node);
        let author = self.owner.identity.author;
        let node = Rc::clone(&self.owner.node);
        std::future::poll_fn(|context| {
            if let Err(error) = ensure_supported_read_view(&opts) {
                return Poll::Ready(Err(error));
            }
            match self.owner.runtime.poll_resident_operation(context, || {
                if let ReadViewSourceSpec::Branch { branch } = &opts.read_view.source {
                    node.node.borrow_mut().acquire_branch_read_inputs(
                        &prepared.shape,
                        &prepared.binding,
                        crate::ids::BranchId(*branch),
                        author,
                        false,
                    )?;
                }
                reads::all_loaded(
                    &node,
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

    /// Run a one-shot query as an explicit terminated-session identity.
    pub async fn all_for_identity(
        &mut self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let node = Rc::clone(&self.owner.node);
        std::future::poll_fn(|context| {
            if let Err(error) = ensure_supported_read_view(&opts) {
                return Poll::Ready(Err(error));
            }
            match self.owner.runtime.poll_resident_operation(context, || {
                reads::all_loaded(
                    &node,
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
        let node = Rc::clone(&self.owner.node);
        let author = self.owner.identity.author;
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
            match self.owner.runtime.poll_resident_operation(context, || {
                if let ReadViewSourceSpec::Branch { branch } = &opts.read_view.source {
                    node.node.borrow_mut().acquire_branch_read_inputs(
                        &prepared.shape,
                        &prepared.binding,
                        crate::ids::BranchId(*branch),
                        author,
                        false,
                    )?;
                }
                reads::relation_snapshot_loaded(
                    &node,
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
        let node = Rc::clone(&self.owner.node);
        std::future::poll_fn(|context| {
            match self.owner.runtime.poll_resident_operation(context, || {
                reads::relation_snapshot_loaded(
                    &node,
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
        let author = self.owner.identity.author;
        let node = Rc::clone(&self.owner.node);
        std::future::poll_fn(|context| {
            match self.owner.runtime.poll_operation(
                context,
                || {
                    node.open_subscription_resident(
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
        let node = Rc::clone(&self.owner.node);
        std::future::poll_fn(|context| {
            match self.owner.runtime.poll_operation(
                context,
                || {
                    node.open_subscription_resident(
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
        self.owner.set_identity_claims(author, claims)
    }

    pub fn can_insert(&self, table: &str, cells: RowCells) -> Result<PermissionAdvice, Error> {
        self.owner.can_insert(table, cells)
    }

    pub fn local_current_row(
        &self,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<CurrentRow>, Error> {
        self.owner.table_schema(table)?;
        Ok(self
            .owner
            .node
            .node
            .borrow_mut()
            .local_current_row(table, row)?)
    }

    pub fn attach_query_with_opts(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<QueryAttachment, Error> {
        self.owner.attach_query_with_opts(prepared, opts)
    }

    pub fn attach_query_for_identity_with_opts(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<QueryAttachment, Error> {
        self.owner
            .attach_query_with_opts_for_identity(prepared, opts, author)
    }

    pub fn query_attachment_is_covered(&self, attachment: &QueryAttachment) -> bool {
        self.owner.query_attachment_is_covered(attachment)
    }

    pub fn detach_query(&self, attachment: QueryAttachment) {
        self.owner.detach_query(attachment)
    }

    async fn publish_mergeable(
        &mut self,
        commits: &[MergeableCommit],
        row: RowUuid,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        let schema = self.schema_version_id;
        let tx_id = std::future::poll_fn(|context| {
            self.owner
                .runtime
                .poll_mergeable_many_in_schema(context, schema, commits)
        })
        .await
        .map_err(Error::from)?;
        let local_tier = self.owner.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions_prepared().await?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.owner.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    /// Insert one generated row through this typed view.
    pub async fn insert(
        &mut self,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<groove::storage::DemandLoadedStorage>, Error> {
        let row = self.owner.row_id_source.borrow_mut().next_row_id();
        self.insert_with_id(table, row, cells, None, None).await
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
        let author = made_by.unwrap_or(self.owner.identity.author);
        let cells = self.owner.apply_insert_defaults(table, cells)?;
        let mut commit =
            MergeableCommit::new(table, row, now_ms.unwrap_or_else(|| self.next_now_ms()))
                .made_by(author)
                .cells(cells);
        if let Some(subject) = permission_subject {
            commit = commit.permission_subject(subject);
        }
        let schema = self.schema.clone();
        let node = Rc::clone(&self.owner.node);
        std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || mutations::acquire_insert_target_loaded(&schema, &node, table, row),
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
        let author = made_by.unwrap_or(self.owner.identity.author);
        let schema = self.schema.clone();
        let node = Rc::clone(&self.owner.node);
        let identity = self.owner.identity;
        if patch.is_empty() {
            let (tx_id, local_tier) = std::future::poll_fn(|context| {
                self.owner.runtime.poll_operation(
                    context,
                    || mutations::prepare_noop_update_loaded(&schema, &node, table, row, author),
                    MutationPrepareError::missing_input,
                )
            })
            .await
            .map_err(MutationPrepareError::into_api)?;
            return Ok(WriteHandle {
                node: Rc::downgrade(&self.owner.node.node),
                row_uuid: row,
                tx_id,
                local_tier,
            });
        }
        let now_ms = now_ms.unwrap_or_else(|| self.next_now_ms());
        let mut commit = std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || {
                    mutations::prepare_update_loaded(
                        &schema,
                        &node,
                        identity,
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
        let now_ms = now_ms.unwrap_or_else(|| self.next_now_ms());
        let permission_subject = made_by;
        let author = made_by.unwrap_or(self.owner.identity.author);
        let schema = self.schema.clone();
        let node = Rc::clone(&self.owner.node);
        let identity = self.owner.identity;
        let mut commit = std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || {
                    mutations::prepare_upsert_loaded(
                        &schema,
                        &node,
                        identity,
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
        let now_ms = now_ms.unwrap_or_else(|| self.next_now_ms());
        let permission_subject = made_by;
        let author = made_by.unwrap_or(self.owner.identity.author);
        let schema = self.schema.clone();
        let node = Rc::clone(&self.owner.node);
        let identity = self.owner.identity;
        let mut commit = std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || mutations::prepare_delete_loaded(&schema, &node, identity, table, row, now_ms),
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
        let now_ms = now_ms.unwrap_or_else(|| self.next_now_ms());
        let permission_subject = made_by;
        let author = made_by.unwrap_or(self.owner.identity.author);
        let schema = self.schema.clone();
        let node = Rc::clone(&self.owner.node);
        let identity = self.owner.identity;
        let commits = std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || {
                    mutations::prepare_restore_loaded(
                        &schema,
                        &node,
                        identity,
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
        let node = Rc::clone(&self.owner.node);
        let _schema = self.schema.clone();
        let _schema_version = self.schema_version_id;
        let identity = self.owner.identity;
        let author = author.unwrap_or(identity.author);
        std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || transactions::begin_mergeable_loaded(&node, id, author),
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
        let now_ms = now_ms.unwrap_or_else(|| self.next_now_ms());
        let node = Rc::clone(&self.owner.node);
        let schema = self.schema.clone();
        let schema_version = self.schema_version_id;
        let _identity = self.owner.identity;
        std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || {
                    transactions::stage_mergeable_insert_loaded(
                        &schema,
                        &node,
                        schema_version,
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
        let now_ms = now_ms.unwrap_or_else(|| self.next_now_ms());
        let node = Rc::clone(&self.owner.node);
        let _schema = self.schema.clone();
        let schema_version = self.schema_version_id;
        let _identity = self.owner.identity;
        std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || {
                    transactions::stage_mergeable_update_loaded(
                        &node,
                        schema_version,
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
        let now_ms = now_ms.unwrap_or_else(|| self.next_now_ms());
        let node = Rc::clone(&self.owner.node);
        let _schema = self.schema.clone();
        let schema_version = self.schema_version_id;
        let _identity = self.owner.identity;
        std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || {
                    transactions::stage_mergeable_delete_loaded(
                        &node,
                        schema_version,
                        tx_id,
                        table,
                        row,
                        now_ms,
                    )
                },
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
        let now_ms = now_ms.unwrap_or_else(|| self.next_now_ms());
        let node = Rc::clone(&self.owner.node);
        let schema = self.schema.clone();
        let schema_version = self.schema_version_id;
        let _identity = self.owner.identity;
        std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || {
                    transactions::stage_mergeable_restore_loaded(
                        &schema,
                        &node,
                        schema_version,
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
        let node = Rc::clone(&self.owner.node);
        let _schema = self.schema.clone();
        let _schema_version = self.schema_version_id;
        let _identity = self.owner.identity;
        std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || transactions::transaction_all_loaded(&node, tx_id, prepared, &opts, None),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
    }

    /// Read one row with this open mergeable batch's staged writes overlaid.
    pub async fn mergeable_read(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<RowCells>, Error> {
        let node = Rc::clone(&self.owner.node);
        let schema_version = self.schema_version_id;
        std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || transactions::exclusive_read_loaded(&node, schema_version, tx_id, table, row),
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
    }

    pub async fn transaction_all_for_identity(
        &mut self,
        tx_id: OpenBatchId,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let node = Rc::clone(&self.owner.node);
        let _schema = self.schema.clone();
        let _schema_version = self.schema_version_id;
        let _identity = self.owner.identity;
        std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || {
                    transactions::transaction_all_loaded(
                        &node,
                        tx_id,
                        prepared,
                        &opts,
                        Some(author),
                    )
                },
                MutationPrepareError::missing_input,
            )
        })
        .await
        .map_err(MutationPrepareError::into_api)
    }

    /// Open a caller-addressed exclusive batch in this typed view.
    pub async fn begin_exclusive(&mut self, id: OpenBatchId) -> Result<(), Error> {
        let node = Rc::clone(&self.owner.node);
        let _schema = self.schema.clone();
        let _schema_version = self.schema_version_id;
        let identity = self.owner.identity;
        std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || transactions::begin_exclusive_loaded(&node, id, identity.author),
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
        let node = Rc::clone(&self.owner.node);
        let _schema = self.schema.clone();
        let schema_version = self.schema_version_id;
        let _identity = self.owner.identity;
        std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || transactions::exclusive_read_loaded(&node, schema_version, tx_id, table, row),
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
        let now_ms = now_ms.unwrap_or_else(|| self.next_now_ms());
        let node = Rc::clone(&self.owner.node);
        let schema = self.schema.clone();
        let schema_version = self.schema_version_id;
        let _identity = self.owner.identity;
        std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || {
                    transactions::stage_exclusive_insert_loaded(
                        &schema,
                        &node,
                        schema_version,
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
        let now_ms = now_ms.unwrap_or_else(|| self.next_now_ms());
        let node = Rc::clone(&self.owner.node);
        let _schema = self.schema.clone();
        let schema_version = self.schema_version_id;
        let _identity = self.owner.identity;
        std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || {
                    transactions::stage_exclusive_delete_loaded(
                        &node,
                        schema_version,
                        tx_id,
                        table,
                        row,
                        now_ms,
                    )
                },
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
        let now_ms = now_ms.unwrap_or_else(|| self.next_now_ms());
        let node = Rc::clone(&self.owner.node);
        let schema = self.schema.clone();
        let schema_version = self.schema_version_id;
        let _identity = self.owner.identity;
        std::future::poll_fn(|context| {
            self.owner.runtime.poll_operation(
                context,
                || {
                    transactions::stage_exclusive_restore_loaded(
                        &schema,
                        &node,
                        schema_version,
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
