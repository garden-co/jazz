//! Prepared, one-shot, relation, and result-tree read APIs.

use super::*;

impl<S> Db<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Start a query rooted at `table`.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db};
    /// # use jazz::query::{col, eq, lit};
    /// let db = block_on(open_todos_db())?;
    /// let open_todos = db
    ///     .table("todos")
    ///     .filter(eq(col("done"), lit(false)))
    ///     .select(["title", "done"]);
    ///
    /// let open_todos = db.prepare_query(&open_todos)?;
    /// assert!(db.read(&open_todos)?.is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn table(&self, table: impl Into<String>) -> Query {
        Query::from(table)
    }

    /// Prepare a query for repeated reads or subscriptions.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// let db = block_on(open_todos_db())?;
    /// let write = block_on(db.insert("todos", todo_cells("write docs", false)))?;
    /// let todo = write.row_uuid();
    ///
    /// let query = db.prepare_query(&db.table("todos"))?;
    /// let rows = db.read(&query)?;
    /// assert_eq!(rows.len(), 1);
    /// assert_eq!(rows[0].row_uuid(), todo);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn prepare_query(&self, query: &Query) -> Result<PreparedQuery, Error> {
        self.prepare_query_bound(query, BTreeMap::new())
    }

    /// Prepare a query with explicit parameter bindings.
    pub fn prepare_query_bound(
        &self,
        query: &Query,
        params: BTreeMap<String, Value>,
    ) -> Result<PreparedQuery, Error> {
        let (schema, schema_version) = self.current_write_schema_for_query()?;
        self.prepare_query_bound_for_schema(query, params, &schema, schema_version)
    }

    /// Prepare a query against the schema this database handle was opened with.
    ///
    /// Typed client facades are pinned to that schema even when a catalogue
    /// snapshot advances or rolls back the separate current-write pointer.
    #[cfg(feature = "runtime")]
    pub(crate) fn prepare_query_for_open_schema(
        &self,
        query: &Query,
    ) -> Result<PreparedQuery, Error> {
        self.prepare_query_bound_for_schema(
            query,
            BTreeMap::new(),
            &self.schema,
            self.schema_version_id,
        )
    }

    fn prepare_query_bound_for_schema(
        &self,
        query: &Query,
        params: BTreeMap<String, Value>,
        schema: &JazzSchema,
        schema_version: SchemaVersionId,
    ) -> Result<PreparedQuery, Error> {
        let shape = query.validate_with_schema_version(schema, schema_version)?;
        let binding = shape.bind(params)?;
        let (local_plan, global_plan) = if should_install_prepared_plan(&shape)
            && !self.node.node.borrow().uses_schema_projected_read(&shape)
        {
            let mut node = self.node.node.borrow_mut();
            (
                Some(super::block_on(node.prepared_query_plan(
                    &shape,
                    &binding,
                    DurabilityTier::Local,
                    AuthorId::SYSTEM,
                ))?),
                Some(super::block_on(node.prepared_query_plan(
                    &shape,
                    &binding,
                    DurabilityTier::Global,
                    AuthorId::SYSTEM,
                ))?),
            )
        } else {
            (None, None)
        };
        let groove_runtime_token = self.node.node.borrow().groove_runtime_token();
        Ok(PreparedQuery {
            shape,
            binding,
            local_plan,
            global_plan,
            groove_runtime_token,
        })
    }

    /// Synchronously read rows for a prepared query.
    ///
    /// This is a synchronous local-preview read. Upstream/server settled
    /// coverage is tracked separately by query attachments and durability-aware
    /// subscription reads.
    pub fn read(&self, prepared: &PreparedQuery) -> Result<Vec<CurrentRow>, Error> {
        let mut node = self.node.node.borrow_mut();
        let groove_runtime_token = node.groove_runtime_token();
        super::block_on(node.query_rows_local_preview(
            &prepared.shape,
            &prepared.binding,
            prepared.plan_for_tier(DurabilityTier::Local, groove_runtime_token),
        ))
        .map_err(Into::into)
    }

    #[cfg(any(test, feature = "testing"))]
    /// Test-only count of live Groove maintained subscriptions.
    pub fn active_groove_subscriptions_for_test(&self) -> usize {
        self.node
            .node
            .borrow()
            .runtime_stats_for_test()
            .active_subscriptions
    }

    /// Synchronously read rows and attribute work inside the node query path.
    ///
    /// The returned rows are identical to [`Self::read`]. This diagnostic
    /// variant exists so persisted-read benchmarks can locate first-read cost
    /// without adding clocks to the ordinary read path.
    pub fn read_profiled(
        &self,
        prepared: &PreparedQuery,
    ) -> Result<(Vec<CurrentRow>, QueryReadProfile), Error> {
        let mut node = self.node.node.borrow_mut();
        let groove_runtime_token = node.groove_runtime_token();
        super::block_on(node.query_rows_local_preview_profiled(
            &prepared.shape,
            &prepared.binding,
            prepared.plan_for_tier(DurabilityTier::Local, groove_runtime_token),
        ))
        .map_err(Into::into)
    }

    /// Synchronously read exactly one local row if present.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// let db = block_on(open_todos_db())?;
    /// let todo = block_on(db.insert("todos", todo_cells("first item", false)))?.row_uuid();
    ///
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// let found = db.one(&todos)?;
    /// assert_eq!(found.map(|row| row.row_uuid()), Some(todo));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn one(&self, prepared: &PreparedQuery) -> Result<Option<CurrentRow>, Error> {
        Ok(self.read(prepared)?.into_iter().next())
    }

    /// Resolve creator/updater provenance for a row returned by this database.
    pub fn row_provenance(&self, row: &CurrentRow) -> Result<Option<RowProvenance>, Error> {
        self.node
            .node
            .borrow_mut()
            .row_provenance(row)
            .map_err(Into::into)
    }

    /// Read local settled history at an exact global sequence cut.
    ///
    /// History-incomplete facades return `HistoricalReadRequiresServer` from
    /// the node layer instead of answering from a partial local prefix
    /// (ch11/INV-BRANCH-4).
    pub fn at(
        &self,
        position: GlobalSeq,
        prepared: &PreparedQuery,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.at_prepared(position, prepared)
    }

    fn at_prepared(
        &self,
        position: GlobalSeq,
        prepared: &PreparedQuery,
    ) -> Result<Vec<CurrentRow>, Error> {
        super::block_on(
            self.node
                .node
                .borrow_mut()
                .at(position)
                .read(&prepared.shape, &prepared.binding),
        )
        .map_err(Into::into)
    }

    /// Tier-gated one-shot read.
    ///
    /// ```rust
    /// # use jazz::db::{ReadOpts, LocalUpdates, Propagation};
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::tx::DurabilityTier;
    /// let db = block_on(open_todos_db())?;
    /// block_on(db.insert("todos", todo_cells("visible locally", false)))?;
    ///
    /// let opts = ReadOpts {
    ///     tier: DurabilityTier::Local,
    ///     local_updates: LocalUpdates::Immediate,
    ///     propagation: Propagation::LocalOnly,
    ///     include_deleted: false,
    ///     ..ReadOpts::default()
    /// };
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// let rows = block_on(db.all(&todos, opts))?;
    /// assert_eq!(rows.len(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn all(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.all_for_identity_in_authorization_mode(
            prepared,
            opts,
            self.identity.author,
            QueryAuthorizationMode::ClientLocal,
        )
        .await
    }

    /// Tier-gated one-shot read evaluated by a trusted host as `author`.
    ///
    /// Ordinary client reads use [`Db::all`] and never re-run read policy over
    /// locally received data. This explicit identity entry point is reserved
    /// for serving/request hosts that own policy enforcement before emission.
    pub async fn all_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.all_for_identity_in_authorization_mode(
            prepared,
            opts,
            author,
            QueryAuthorizationMode::TrustedServing,
        )
        .await
    }

    async fn all_for_identity_in_authorization_mode(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<Vec<CurrentRow>, Error> {
        let tier = effective_read_tier(&opts);
        let mut node = self.node.node.borrow_mut();
        match &opts.read_view.source {
            ReadViewSourceSpec::Current => {}
            ReadViewSourceSpec::Branch { branch } if !opts.include_deleted => {
                return match authorization_mode {
                    QueryAuthorizationMode::TrustedServing => node
                        .query_rows_on_branch_for_link(
                            crate::ids::BranchId(*branch),
                            &prepared.shape,
                            &prepared.binding,
                            author,
                        )
                        .await
                        .map_err(Into::into),
                    QueryAuthorizationMode::ClientLocal if tier < DurabilityTier::Edge => node
                        .query_rows_on_branch_for_client(
                            crate::ids::BranchId(*branch),
                            &prepared.shape,
                            &prepared.binding,
                            author,
                        )
                        .await
                        .map_err(Into::into),
                    QueryAuthorizationMode::ClientLocal => node
                        .query_rows_for_client_read_view(
                            &prepared.shape,
                            &prepared.binding,
                            self.node
                                .upstream_register_shape_options(
                                    tier,
                                    opts.read_view.clone(),
                                    opts.propagation == Propagation::Full,
                                )
                                .tier,
                            &opts.read_view,
                        )
                        .await
                        .map_err(Into::into),
                };
            }
            _ => ensure_default_read_view(&opts)?,
        }
        match (opts.include_deleted, authorization_mode) {
            (true, mode) => {
                node.query_rows_including_deleted_in_authorization_mode(
                    &prepared.shape,
                    &prepared.binding,
                    tier,
                    None,
                    author,
                    mode,
                )
                .await
            }
            (false, QueryAuthorizationMode::TrustedServing) => {
                node.query_rows_with_prepared_plan_for_identity(
                    &prepared.shape,
                    &prepared.binding,
                    tier,
                    None,
                    author,
                )
                .await
            }
            (false, QueryAuthorizationMode::ClientLocal) => {
                // A client consumes identity-scoped rows emitted by its
                // trusted upstream; local reads must not apply policy again.
                node.query_rows_for_client(&prepared.shape, &prepared.binding, tier, author)
                    .await
            }
        }
        .map_err(Into::into)
    }

    /// Tier-gated one-shot relation read evaluated as the database identity.
    pub async fn all_relation_snapshot(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        ensure_supported_read_view(&opts)?;
        if opts.include_deleted {
            return Err(Error::new(
                ErrorCode::Query,
                "relation snapshots do not support include_deleted yet",
            ));
        }
        let tier = effective_read_tier(&opts);
        self.node
            .node
            .borrow_mut()
            .query_relation_snapshot_for_client(
                &prepared.shape,
                &prepared.binding,
                tier,
                self.identity.author,
                &opts.read_view,
            )
            .await
            .map_err(Into::into)
    }

    /// Tier-gated one-shot relation read evaluated as `author`.
    pub async fn all_relation_snapshot_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<RelationSnapshot, Error> {
        ensure_supported_read_view(&opts)?;
        if opts.include_deleted {
            return Err(Error::new(
                ErrorCode::Query,
                "relation snapshots do not support include_deleted yet",
            ));
        }
        let tier = effective_read_tier(&opts);
        self.node
            .node
            .borrow_mut()
            .query_relation_snapshot_for_serving_in_read_view(
                &prepared.shape,
                &prepared.binding,
                tier,
                author,
                &opts.read_view,
            )
            .await
            .map_err(Into::into)
    }

    /// Tier-gated canonical structured result read.
    ///
    /// This is the sole Jazz-boundary materialization of relation facts into
    /// recursive output. Wire delivery deliberately remains on its v3 carrier
    /// until the structured delivery migration.
    pub async fn all_result_tree(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<ResultTree, Error> {
        let snapshot = self.all_relation_snapshot(prepared, opts).await?;
        materialize_result_tree(prepared.shape.query(), snapshot)
    }

    /// Tier-gated one-shot output-changing relation read evaluated as the database identity.
    pub async fn all_relation_query(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        ensure_default_read_view(&opts)?;
        let query = relation_query_to_query(query)?;
        let prepared = self.prepare_query(&query)?;
        // Output-changing relation queries currently normalize to a single
        // root row set. They have no array payload edges, so request ordinary
        // app rows instead of the relation-snapshot fact output (which is
        // reserved for correlated array/path materialization).
        let rows = self.all(&prepared, opts).await?;
        Ok(RelationSnapshot {
            root_count: rows.len(),
            rows,
            edges: Vec::new(),
        })
    }

    /// Tier-gated one-shot output-changing relation read evaluated as `author`.
    pub async fn all_relation_query_for_identity(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<RelationSnapshot, Error> {
        ensure_default_read_view(&opts)?;
        let query = relation_query_to_query(query)?;
        let prepared = self.prepare_query(&query)?;
        // Output-changing relation queries currently normalize to a single
        // root row set.  They have no array payload edges, so request ordinary
        // app rows instead of the relation-snapshot fact output (which is
        // reserved for correlated array/path materialization).
        let rows = self.all_for_identity(&prepared, opts, author).await?;
        Ok(RelationSnapshot {
            root_count: rows.len(),
            rows,
            edges: Vec::new(),
        })
    }
}
