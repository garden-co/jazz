//! Row insertion, update, deletion, restoration, and authorization.

use super::*;
use crate::node::{ContributionMergeRequest, ContributionMergeRow};
use crate::protocol::{BranchSelector, BranchViewBase};

impl<S> Db<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Calculate and commit novel contributions from one exact branch key into
    /// another. This requires a history-complete database and emits an ordinary
    /// mergeable transaction when the target does not already represent every
    /// selected contribution.
    pub fn merge_branch_contributions(
        &self,
        source: BranchSelector,
        target: BranchSelector,
        rows: impl IntoIterator<Item = ContributionMergeRow>,
    ) -> Result<Option<TxId>, Error> {
        let tx_id =
            self.node
                .node
                .borrow_mut()
                .merge_branch_contributions(ContributionMergeRequest {
                    source,
                    target,
                    rows: rows.into_iter().collect(),
                    made_by: self.identity.author,
                    permission_subject: Some(self.identity.author),
                    now_ms: self.next_now_ms(),
                })?;
        if let Some(tx_id) = tx_id {
            self.finalize_local_commit(tx_id)?;
            self.refresh_subscriptions()?;
        }
        Ok(tx_id)
    }

    /// Insert a row locally, generating a uuidv7-shaped row id.
    ///
    /// The generated id is available from [`WriteHandle::row_uuid`].
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db};
    /// # use jazz::tx::DurabilityTier;
    /// let db = block_on(open_todos_db())?;
    /// let write = db.insert("todos", jazz::row! { title: "new todo", done: false })?;
    /// let row = write.row_uuid();
    /// block_on(write.wait(DurabilityTier::Local))?;
    ///
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert_eq!(db.read(&todos)?.len(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn insert(&self, table: &str, cells: RowCells) -> Result<WriteHandle<S>, Error> {
        let row = self.row_id_source.borrow_mut().next_row_id();
        self.write_mergeable(
            self.identity.author,
            None,
            table,
            row,
            cells,
            Vec::new(),
            None,
        )
        .await
    }

    /// Insert a row while attributing provenance to `made_by`.
    ///
    /// The Db's authenticated identity remains the write-policy subject. Client
    /// facades can only write as themselves; trusted-backend attribution is a
    /// serving-node concern on inbound commit-unit ingestion.
    pub async fn insert_attributed(
        &self,
        made_by: AuthorId,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let row = self.row_id_source.borrow_mut().next_row_id();
        self.write_mergeable_as_session_subject(made_by, table, row, cells, Vec::new(), None)
            .await
    }

    /// Insert a row with a caller-supplied id.
    ///
    /// This is a niche path for imports from legacy systems or other cases
    /// where row identity already exists. New local rows should generally use
    /// [`Db::insert`] so the database generates the id.
    pub async fn insert_with_id(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_absent(table, row, self.identity.author)
            .await?;
        self.write_mergeable(
            self.identity.author,
            None,
            table,
            row,
            cells,
            Vec::new(),
            None,
        )
        .await
    }

    /// Insert one exact branch-local row with a caller-supplied row id.
    pub async fn insert_with_id_in_branch(
        &self,
        table: &str,
        branch: BranchSelector,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_exact_branch_row_absent(table, &branch, row)
            .await?;
        self.write_mergeable_at_ms_with_authorship_in_branch(
            self.identity.author,
            None,
            table,
            row,
            cells,
            Vec::new(),
            None,
            None,
            self.next_now_ms(),
            branch,
        )
    }

    /// Insert one exact branch-local row while evaluating policy as `identity`.
    pub async fn insert_with_id_in_branch_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        branch: BranchSelector,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_exact_branch_row_absent(table, &branch, row)
            .await?;
        self.write_mergeable_at_ms_with_authorship_in_branch(
            identity,
            Some(identity),
            table,
            row,
            cells,
            Vec::new(),
            None,
            None,
            self.next_now_ms(),
            branch,
        )
    }

    /// Insert a caller-id row while attributing provenance to `made_by`.
    ///
    /// See [`Db::insert_attributed`] for the security boundary.
    pub async fn insert_with_id_attributed(
        &self,
        made_by: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_absent(table, row, self.identity.author)
            .await?;
        self.write_mergeable_as_session_subject(made_by, table, row, cells, Vec::new(), None)
            .await
    }

    /// Insert a row while evaluating write policy as `identity`.
    pub async fn insert_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let row = self.row_id_source.borrow_mut().next_row_id();
        self.insert_with_id_for_identity(identity, table, row, cells)
            .await
    }

    /// Insert a caller-id row with an explicit millisecond provenance time.
    pub async fn insert_with_id_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_absent(table, row, self.identity.author)
            .await?;
        self.write_mergeable_at_ms(
            self.identity.author,
            None,
            table,
            row,
            cells,
            Vec::new(),
            None,
            now_ms,
        )
        .await
    }

    /// Insert a caller-id row while evaluating write policy as `identity`.
    ///
    /// This is a trusted serving-node API for terminated backend/request
    /// sessions. It records provenance as `identity` and evaluates policy as
    /// the same identity, without changing the Db's own authority.
    pub async fn insert_with_id_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_absent(table, row, identity).await?;
        let cells = self.apply_insert_defaults(table, cells)?;
        // Client writes are admitted structurally and staged optimistically.
        // A trusted serving authority evaluates policy and returns the fate.
        self.write_mergeable(
            identity,
            Some(identity),
            table,
            row,
            cells,
            Vec::new(),
            None,
        )
        .await
    }

    /// Insert a caller-id row for `identity` with an explicit millisecond provenance time.
    pub async fn insert_with_id_for_identity_at_ms(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_absent(table, row, identity).await?;
        let cells = self.apply_insert_defaults(table, cells)?;
        // See `insert_with_id_for_identity`: policy fate belongs to the
        // trusted serving authority, not this local client admission path.
        self.write_mergeable_at_ms(
            identity,
            Some(identity),
            table,
            row,
            cells,
            Vec::new(),
            None,
            now_ms,
        )
        .await
    }

    /// Advise whether an insert may be allowed.
    ///
    /// A `Db` is ordinarily a client-local replica, whose policy evidence may
    /// be incomplete. It therefore never turns a local policy evaluation into
    /// an allow/deny result. Use an explicitly trusted serving authority for a
    /// final decision.
    pub fn can_insert(&self, _table: &str, _cells: RowCells) -> Result<PermissionAdvice, Error> {
        Ok(PermissionAdvice::Unknown)
    }

    /// Evaluate an insert for a test-only serving-path probe without writing.
    #[cfg(test)]
    pub(crate) fn authorize_insert_for_identity(
        &self,
        table: &str,
        cells: RowCells,
        identity: AuthorId,
    ) -> Result<PermissionAdvice, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.node
            .node
            .borrow_mut()
            .dry_run_mergeable_write_allows_for_view(
                &self.schema,
                MergeableCommit::new(table, RowUuid::from_bytes([0; 16]), 0)
                    .made_by(identity)
                    .permission_subject(identity)
                    .cells(cells),
            )
            .map(|allowed| {
                if allowed {
                    PermissionAdvice::Allowed
                } else {
                    PermissionAdvice::Denied
                }
            })
            .map_err(Into::into)
    }

    /// Update a row locally; omitted fields keep their current local value.
    ///
    /// ```rust
    /// # use std::collections::BTreeMap;
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::ids::RowUuid;
    /// # use jazz::groove::records::Value;
    /// let db = block_on(open_todos_db())?;
    /// let todo = RowUuid::from_bytes([1; 16]);
    /// db.insert_with_id("todos", todo, todo_cells("draft", false))?;
    ///
    /// db.update(
    ///     "todos",
    ///     todo,
    ///     BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    /// )?;
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert_eq!(db.read(&todos)?.len(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn update(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        if patch.is_empty() {
            return self
                .no_op_update_handle_for_client(table, row, self.identity.author)
                .await;
        }
        let (cells, parent, authored_columns) =
            self.merge_existing_cells(table, row, patch).await?;
        self.write_mergeable_with_authored_columns(
            self.identity.author,
            None,
            table,
            row,
            cells,
            parent.into_iter().collect(),
            None,
            authored_columns,
        )
        .await
    }

    /// Patch one exact branch-local row.
    pub fn update_in_branch(
        &self,
        table: &str,
        branch: BranchSelector,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        if patch.is_empty() {
            return Err(Error::new(
                ErrorCode::Schema,
                "exact branch update requires at least one authored column",
            ));
        }
        let mut node = self.node.node.borrow_mut();
        let Some(mut cells) = node.visible_current_cells_in_branch(table, &branch, row)? else {
            return Err(Error::new(
                ErrorCode::NotObserved,
                format!("branch-local row not observed: {}", row.0),
            ));
        };
        let parent = node.local_content_winner_tx_id_in_branch(table, &branch, row)?;
        drop(node);
        let authored_columns = patch.keys().cloned().collect();
        cells.extend(patch);
        self.write_mergeable_at_ms_with_authorship_in_branch(
            self.identity.author,
            None,
            table,
            row,
            cells,
            parent.into_iter().collect(),
            None,
            Some(authored_columns),
            self.next_now_ms(),
            branch,
        )
    }

    /// Patch one exact branch-local row while evaluating policy as `identity`.
    pub fn update_in_branch_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        branch: BranchSelector,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        if patch.is_empty() {
            return Err(Error::new(
                ErrorCode::Schema,
                "exact branch update requires at least one authored column",
            ));
        }
        let mut node = self.node.node.borrow_mut();
        let Some(mut cells) = node.visible_current_cells_in_branch(table, &branch, row)? else {
            return Err(Error::new(
                ErrorCode::NotObserved,
                format!("branch-local row not observed: {}", row.0),
            ));
        };
        let parent = node.local_content_winner_tx_id_in_branch(table, &branch, row)?;
        drop(node);
        let authored_columns = patch.keys().cloned().collect();
        cells.extend(patch);
        self.write_mergeable_at_ms_with_authorship_in_branch(
            identity,
            Some(identity),
            table,
            row,
            cells,
            parent.into_iter().collect(),
            None,
            Some(authored_columns),
            self.next_now_ms(),
            branch,
        )
    }

    /// Patch a row through a head-over-base view, copying inherited content
    /// into the head branch-local row without a cross-branch causal parent.
    pub fn update_in_branch_view(
        &self,
        table: &str,
        head: BranchSelector,
        base: Option<BranchViewBase>,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        if self
            .node
            .node
            .borrow_mut()
            .visible_current_cells_in_branch(table, &head, row)?
            .is_some()
        {
            return self.update_in_branch(table, head, row, patch);
        }
        let Some(mut inherited) = self
            .node
            .node
            .borrow_mut()
            .visible_current_cells_in_branch_view(table, &head, base.as_ref(), row)?
        else {
            return Err(Error::new(
                ErrorCode::NotObserved,
                format!("row is not visible in branch view: {}", row.0),
            ));
        };
        inherited.extend(patch);
        self.insert_with_id_in_branch(table, head, row, inherited)
    }

    /// Patch through a branch view while evaluating policy as `identity`.
    pub fn update_in_branch_view_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        head: BranchSelector,
        base: Option<BranchViewBase>,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        if self
            .node
            .node
            .borrow_mut()
            .visible_current_cells_in_branch(table, &head, row)?
            .is_some()
        {
            return self.update_in_branch_for_identity(identity, table, head, row, patch);
        }
        let Some(mut inherited) = self
            .node
            .node
            .borrow_mut()
            .visible_current_cells_in_branch_view(table, &head, base.as_ref(), row)?
        else {
            return Err(Error::new(
                ErrorCode::NotObserved,
                format!("row is not visible in branch view: {}", row.0),
            ));
        };
        inherited.extend(patch);
        self.insert_with_id_in_branch_for_identity(identity, table, head, row, inherited)
    }

    /// Insert or patch one exact branch-local row.
    pub fn upsert_in_branch(
        &self,
        table: &str,
        branch: BranchSelector,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let exists = self
            .node
            .node
            .borrow_mut()
            .visible_current_cells_in_branch(table, &branch, row)?
            .is_some();
        if exists {
            self.update_in_branch(table, branch, row, cells)
        } else {
            self.insert_with_id_in_branch(table, branch, row, cells)
        }
    }

    /// Update a row with an explicit millisecond provenance time.
    pub async fn update_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        if patch.is_empty() {
            return self
                .no_op_update_handle_for_client(table, row, self.identity.author)
                .await;
        }
        let (cells, parent, authored_columns) =
            self.merge_existing_cells(table, row, patch).await?;
        self.write_mergeable_at_ms_with_authorship(
            self.identity.author,
            None,
            table,
            row,
            cells,
            parent.into_iter().collect(),
            None,
            Some(authored_columns),
            now_ms,
        )
        .await
    }

    /// Update a row while attributing provenance to `made_by`.
    ///
    /// See [`Db::insert_attributed`] for the security boundary.
    pub async fn update_attributed(
        &self,
        made_by: AuthorId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.check_attribution_allowed(made_by)?;
        if patch.is_empty() {
            return self
                .no_op_update_handle_for_client(table, row, self.identity.author)
                .await;
        }
        let (cells, parent, authored_columns) =
            self.merge_existing_cells(table, row, patch).await?;
        self.write_mergeable_as_session_subject_with_authored_columns(
            made_by,
            table,
            row,
            cells,
            parent.into_iter().collect(),
            None,
            authored_columns,
        )
        .await
    }

    /// Update a row while evaluating write policy as `identity`.
    pub async fn update_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        if patch.is_empty() {
            return self
                .no_op_update_handle_for_identity(table, row, identity)
                .await;
        }
        let (cells, parent, authored_columns) = self
            .merge_existing_cells_for_identity(table, row, patch, identity)
            .await?;
        let parents = parent.into_iter().collect::<Vec<_>>();
        self.write_mergeable_with_authored_columns(
            identity,
            Some(identity),
            table,
            row,
            cells,
            parents,
            None,
            authored_columns,
        )
        .await
    }

    /// Update a row for `identity` with an explicit millisecond provenance time.
    pub async fn update_for_identity_at_ms(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        if patch.is_empty() {
            return self
                .no_op_update_handle_for_identity(table, row, identity)
                .await;
        }
        let (cells, parent, authored_columns) = self
            .merge_existing_cells_for_identity(table, row, patch, identity)
            .await?;
        let parents = parent.into_iter().collect::<Vec<_>>();
        self.write_mergeable_at_ms_with_authorship(
            identity,
            Some(identity),
            table,
            row,
            cells,
            parents,
            None,
            Some(authored_columns),
            now_ms,
        )
        .await
    }

    /// Upsert a row locally.
    ///
    /// This explicit-id path is primarily for importing rows from legacy
    /// systems. New local rows should generally use [`Db::insert`] and then
    /// update the returned [`WriteHandle::row_uuid`] when needed.
    ///
    /// ```rust
    /// # use std::collections::BTreeMap;
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::ids::RowUuid;
    /// # use jazz::groove::records::Value;
    /// let db = block_on(open_todos_db())?;
    /// let todo = RowUuid::from_bytes([1; 16]);
    ///
    /// db.upsert("todos", todo, todo_cells("created", false))?;
    /// db.upsert(
    ///     "todos",
    ///     todo,
    ///     BTreeMap::from([("title".to_owned(), Value::String("renamed".to_owned()))]),
    /// )?;
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert_eq!(db.one(&todos)?.unwrap().row_uuid(), todo);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn upsert(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row).await?;
        let (cells, parents, authored_columns) = if self
            .upsert_target_for_client_identity(table, row, self.identity.author)
            .await?
            .is_some()
        {
            let (cells, parent, authored_columns) =
                self.merge_existing_cells(table, row, cells).await?;
            (cells, parent.into_iter().collect(), Some(authored_columns))
        } else {
            (cells, Vec::new(), None)
        };
        self.write_mergeable_at_ms_with_authorship(
            self.identity.author,
            None,
            table,
            row,
            cells,
            parents,
            None,
            authored_columns,
            self.next_now_ms(),
        )
        .await
    }

    /// Upsert a row with an explicit millisecond provenance time.
    pub async fn upsert_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row).await?;
        let (cells, parents, authored_columns) = if self
            .upsert_target_for_client_identity(table, row, self.identity.author)
            .await?
            .is_some()
        {
            let (cells, parent, authored_columns) =
                self.merge_existing_cells(table, row, cells).await?;
            (cells, parent.into_iter().collect(), Some(authored_columns))
        } else {
            (cells, Vec::new(), None)
        };
        self.write_mergeable_at_ms_with_authorship(
            self.identity.author,
            None,
            table,
            row,
            cells,
            parents,
            None,
            authored_columns,
            now_ms,
        )
        .await
    }

    /// Upsert a row while evaluating write policy as `identity`.
    pub async fn upsert_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row).await?;
        let (cells, parents, authored_columns) = if self
            .upsert_target_for_trusted_identity(table, row, identity)
            .await?
            .is_some()
        {
            let (cells, parent, authored_columns) = self
                .merge_existing_cells_for_identity(table, row, cells, identity)
                .await?;
            (cells, parent.into_iter().collect(), Some(authored_columns))
        } else {
            (cells, Vec::new(), None)
        };
        self.write_mergeable_at_ms_with_authorship(
            identity,
            Some(identity),
            table,
            row,
            cells,
            parents,
            None,
            authored_columns,
            self.next_now_ms(),
        )
        .await
    }

    /// Upsert a row for `identity` with an explicit millisecond provenance time.
    pub async fn upsert_for_identity_at_ms(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row).await?;
        let (cells, parents, authored_columns) = if self
            .upsert_target_for_trusted_identity(table, row, identity)
            .await?
            .is_some()
        {
            let (cells, parent, authored_columns) = self
                .merge_existing_cells_for_identity(table, row, cells, identity)
                .await?;
            (cells, parent.into_iter().collect(), Some(authored_columns))
        } else {
            (cells, Vec::new(), None)
        };
        self.write_mergeable_at_ms_with_authorship(
            identity,
            Some(identity),
            table,
            row,
            cells,
            parents,
            None,
            authored_columns,
            now_ms,
        )
        .await
    }

    /// Soft-delete a row locally.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::ids::RowUuid;
    /// let db = block_on(open_todos_db())?;
    /// let todo = RowUuid::from_bytes([1; 16]);
    /// db.insert_with_id("todos", todo, todo_cells("remove me", false))?;
    ///
    /// db.delete("todos", todo)?;
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert!(db.read(&todos)?.is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn delete(&self, table: &str, row: RowUuid) -> Result<WriteHandle<S>, Error> {
        self.delete_at_ms_option(table, row, None).await
    }

    /// Soft-delete one exact branch-local row.
    pub fn delete_in_branch(
        &self,
        table: &str,
        branch: BranchSelector,
        row: RowUuid,
    ) -> Result<WriteHandle<S>, Error> {
        let mut node = self.node.node.borrow_mut();
        if node
            .visible_current_cells_in_branch(table, &branch, row)?
            .is_none()
        {
            return Err(Error::new(
                ErrorCode::NotObserved,
                format!("branch-local row not observed: {}", row.0),
            ));
        }
        let parents = node
            .local_deletion_winner_tx_id_in_branch(table, &branch, row)?
            .or(node.local_content_winner_tx_id_in_branch(table, &branch, row)?)
            .into_iter()
            .collect();
        drop(node);
        self.write_mergeable_at_ms_with_authorship_in_branch(
            self.identity.author,
            None,
            table,
            row,
            BTreeMap::new(),
            parents,
            Some(DeletionEvent::Deleted),
            None,
            self.next_now_ms(),
            branch,
        )
    }

    /// Delete one exact branch-local row while evaluating policy as `identity`.
    pub fn delete_in_branch_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        branch: BranchSelector,
        row: RowUuid,
    ) -> Result<WriteHandle<S>, Error> {
        let mut node = self.node.node.borrow_mut();
        if node
            .visible_current_cells_in_branch(table, &branch, row)?
            .is_none()
        {
            return Err(Error::new(
                ErrorCode::NotObserved,
                format!("branch-local row not observed: {}", row.0),
            ));
        }
        let parents = node
            .local_deletion_winner_tx_id_in_branch(table, &branch, row)?
            .or(node.local_content_winner_tx_id_in_branch(table, &branch, row)?)
            .into_iter()
            .collect();
        drop(node);
        self.write_mergeable_at_ms_with_authorship_in_branch(
            identity,
            Some(identity),
            table,
            row,
            BTreeMap::new(),
            parents,
            Some(DeletionEvent::Deleted),
            None,
            self.next_now_ms(),
            branch,
        )
    }

    /// Delete a row through a head-over-base view. An inherited base row is
    /// masked by a deletion register in the head branch-local row.
    pub fn delete_in_branch_view(
        &self,
        table: &str,
        head: BranchSelector,
        base: Option<BranchViewBase>,
        row: RowUuid,
    ) -> Result<WriteHandle<S>, Error> {
        if self
            .node
            .node
            .borrow_mut()
            .visible_current_cells_in_branch(table, &head, row)?
            .is_some()
        {
            return self.delete_in_branch(table, head, row);
        }
        if self
            .node
            .node
            .borrow_mut()
            .visible_current_cells_in_branch_view(table, &head, base.as_ref(), row)?
            .is_none()
        {
            return Err(Error::new(
                ErrorCode::NotObserved,
                format!("row is not visible in branch view: {}", row.0),
            ));
        }
        let parent = self
            .node
            .node
            .borrow_mut()
            .local_deletion_winner_tx_id_in_branch(table, &head, row)?;
        self.write_mergeable_at_ms_with_authorship_in_branch(
            self.identity.author,
            None,
            table,
            row,
            BTreeMap::new(),
            parent.into_iter().collect(),
            Some(DeletionEvent::Deleted),
            None,
            self.next_now_ms(),
            head,
        )
    }

    /// Delete through a branch view while evaluating policy as `identity`.
    pub fn delete_in_branch_view_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        head: BranchSelector,
        base: Option<BranchViewBase>,
        row: RowUuid,
    ) -> Result<WriteHandle<S>, Error> {
        if self
            .node
            .node
            .borrow_mut()
            .visible_current_cells_in_branch(table, &head, row)?
            .is_some()
        {
            return self.delete_in_branch_for_identity(identity, table, head, row);
        }
        if self
            .node
            .node
            .borrow_mut()
            .visible_current_cells_in_branch_view(table, &head, base.as_ref(), row)?
            .is_none()
        {
            return Err(Error::new(
                ErrorCode::NotObserved,
                format!("row is not visible in branch view: {}", row.0),
            ));
        }
        let parent = self
            .node
            .node
            .borrow_mut()
            .local_deletion_winner_tx_id_in_branch(table, &head, row)?;
        self.write_mergeable_at_ms_with_authorship_in_branch(
            identity,
            Some(identity),
            table,
            row,
            BTreeMap::new(),
            parent.into_iter().collect(),
            Some(DeletionEvent::Deleted),
            None,
            self.next_now_ms(),
            head,
        )
    }

    /// Restore the deletion register of one exact branch-local row.
    pub fn restore_in_branch(
        &self,
        table: &str,
        branch: BranchSelector,
        row: RowUuid,
    ) -> Result<WriteHandle<S>, Error> {
        let parent = self
            .node
            .node
            .borrow_mut()
            .local_deletion_winner_tx_id_in_branch(table, &branch, row)?
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::NotObserved,
                    format!("branch deletion not observed: {}", row.0),
                )
            })?;
        self.write_mergeable_at_ms_with_authorship_in_branch(
            self.identity.author,
            None,
            table,
            row,
            BTreeMap::new(),
            vec![parent],
            Some(DeletionEvent::Restored),
            None,
            self.next_now_ms(),
            branch,
        )
    }

    /// Restore an exact branch-local row and replace its content atomically.
    pub fn restore_with_cells_in_branch(
        &self,
        table: &str,
        branch: BranchSelector,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.restore_with_cells_in_branch_for_identity(
            self.identity.author,
            None,
            table,
            branch,
            row,
            cells,
        )
    }

    /// Restore an exact branch-local row while evaluating policy as `identity`.
    pub fn restore_with_cells_in_branch_as_identity(
        &self,
        identity: AuthorId,
        table: &str,
        branch: BranchSelector,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.restore_with_cells_in_branch_for_identity(
            identity,
            Some(identity),
            table,
            branch,
            row,
            cells,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn restore_with_cells_in_branch_for_identity(
        &self,
        made_by: AuthorId,
        permission_subject: Option<AuthorId>,
        table: &str,
        branch: BranchSelector,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        let (content_parents, deletion_parents) = {
            let mut node = self.node.node.borrow_mut();
            let deletion_parent = node
                .local_deletion_winner_tx_id_in_branch(table, &branch, row)?
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::NotObserved,
                        format!("branch deletion not observed: {}", row.0),
                    )
                })?;
            (
                node.local_content_winner_tx_id_in_branch(table, &branch, row)?
                    .into_iter()
                    .collect::<Vec<_>>(),
                vec![deletion_parent],
            )
        };
        let content = MergeableCommit::new(table, row, self.next_now_ms())
            .branch(branch.clone())
            .made_by(made_by)
            .parents(content_parents)
            .cells(cells);
        let deletion = MergeableCommit::new(table, row, self.next_now_ms())
            .branch(branch)
            .made_by(made_by)
            .parents(deletion_parents)
            .cells(BTreeMap::<String, Value>::new())
            .deletion(DeletionEvent::Restored);
        let (content, deletion) = match permission_subject {
            Some(subject) => (
                content.permission_subject(subject),
                deletion.permission_subject(subject),
            ),
            None => (content, deletion),
        };
        let tx_id = self
            .node
            .node
            .borrow_mut()
            .commit_mergeable_many_in_schema(self.schema_version_id, vec![content, deletion])?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    /// Soft-delete a row with explicit millisecond provenance time.
    pub async fn delete_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.delete_at_ms_option(table, row, Some(now_ms)).await
    }

    pub(super) async fn delete_at_ms_option(
        &self,
        table: &str,
        row: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row).await?;
        let (parents, _) = self.row_layer_parents(table, row).await?;
        match now_ms {
            Some(now_ms) => {
                self.write_mergeable_at_ms(
                    self.identity.author,
                    None,
                    table,
                    row,
                    BTreeMap::new(),
                    parents,
                    Some(DeletionEvent::Deleted),
                    now_ms,
                )
                .await
            }
            None => {
                self.write_mergeable(
                    self.identity.author,
                    None,
                    table,
                    row,
                    BTreeMap::new(),
                    parents,
                    Some(DeletionEvent::Deleted),
                )
                .await
            }
        }
    }

    /// Soft-delete a row while attributing provenance to `made_by`.
    ///
    /// See [`Db::insert_attributed`] for the security boundary.
    pub async fn delete_attributed(
        &self,
        made_by: AuthorId,
        table: &str,
        row: RowUuid,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row).await?;
        let (parents, _) = self.row_layer_parents(table, row).await?;
        self.write_mergeable_as_session_subject(
            made_by,
            table,
            row,
            BTreeMap::new(),
            parents,
            Some(DeletionEvent::Deleted),
        )
        .await
    }

    /// Soft-delete a row while evaluating write policy as `identity`.
    pub async fn delete_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
    ) -> Result<WriteHandle<S>, Error> {
        self.delete_for_identity_at_ms_option(identity, table, row, None)
            .await
    }

    /// Soft-delete a row while evaluating write policy as `identity`, with explicit time.
    pub async fn delete_for_identity_at_ms(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.delete_for_identity_at_ms_option(identity, table, row, Some(now_ms))
            .await
    }

    async fn delete_for_identity_at_ms_option(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row).await?;
        let (parents, _) = self.row_layer_parents(table, row).await?;
        match now_ms {
            Some(now_ms) => {
                self.write_mergeable_at_ms(
                    identity,
                    Some(identity),
                    table,
                    row,
                    BTreeMap::new(),
                    parents,
                    Some(DeletionEvent::Deleted),
                    now_ms,
                )
                .await
            }
            None => {
                self.write_mergeable(
                    identity,
                    Some(identity),
                    table,
                    row,
                    BTreeMap::new(),
                    parents,
                    Some(DeletionEvent::Deleted),
                )
                .await
            }
        }
    }

    /// Advise whether a read may be allowed. Client-local replicas return
    /// `Unknown` rather than using locally available rows as policy evidence.
    pub fn can_read(&self, _table: &str, _row: RowUuid) -> Result<PermissionAdvice, Error> {
        Ok(PermissionAdvice::Unknown)
    }

    /// Evaluate a read for the serving path without disclosing data.
    pub(crate) fn authorize_read_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        author: AuthorId,
    ) -> Result<PermissionAdvice, Error> {
        self.table_schema(table)?;
        self.node
            .node
            .borrow_mut()
            .dry_run_read_current_allows(table, row, author)
            .map(|allowed| {
                if allowed {
                    PermissionAdvice::Allowed
                } else {
                    PermissionAdvice::Denied
                }
            })
            .map_err(Into::into)
    }

    /// Advise whether an update may be allowed. Client-local replicas return
    /// `Unknown` rather than using locally available rows as policy evidence.
    pub fn can_update(&self, _table: &str, _row: RowUuid) -> Result<PermissionAdvice, Error> {
        Ok(PermissionAdvice::Unknown)
    }

    /// Attach process-local auth claims for `identity`.
    pub fn set_identity_claims(&self, identity: AuthorId, claims: BTreeMap<String, Value>) {
        let changed = {
            let mut node = self.node.node.borrow_mut();
            let previous_revision = node.session_claim_revision(identity);
            node.set_session_claims(identity, claims);
            node.session_claim_revision(identity) != previous_revision
        };
        if changed {
            self.node.schedule_tick(TickUrgency::Deferred);
        }
    }

    /// Advise whether a delete may be allowed. Client-local replicas return
    /// `Unknown` rather than using locally available rows as policy evidence.
    pub fn can_delete(&self, _table: &str, _row: RowUuid) -> Result<PermissionAdvice, Error> {
        Ok(PermissionAdvice::Unknown)
    }

    /// Evaluate a delete for a test-only serving-path probe without writing.
    #[cfg(test)]
    pub(crate) fn authorize_delete_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        author: AuthorId,
    ) -> Result<PermissionAdvice, Error> {
        self.table_schema(table)?;
        self.node
            .node
            .borrow_mut()
            .dry_run_delete_current_allows(table, row, author)
            .map(|allowed| {
                if allowed {
                    PermissionAdvice::Allowed
                } else {
                    PermissionAdvice::Denied
                }
            })
            .map_err(Into::into)
    }

    /// Restore a row locally, applying defaults for omitted columns.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::ids::RowUuid;
    /// let db = block_on(open_todos_db())?;
    /// let todo = RowUuid::from_bytes([1; 16]);
    /// db.insert_with_id("todos", todo, todo_cells("archived", false))?;
    /// db.delete("todos", todo)?;
    ///
    /// db.restore("todos", todo, todo_cells("restored", false))?;
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert_eq!(db.one(&todos)?.unwrap().row_uuid(), todo);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn restore(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.ensure_row_deleted(table, row, self.identity.author)
            .await?;
        let (content_parents, deletion_parents) = {
            let mut node = self.node.node.lock().await;
            let content_parents = node
                .local_content_winner_tx_id(table, row)
                .await?
                .into_iter()
                .collect::<Vec<_>>();
            let deletion_parents = node
                .local_deletion_winner_tx_id(table, row)
                .await?
                .into_iter()
                .collect::<Vec<_>>();
            (content_parents, deletion_parents)
        };
        let published = self
            .node
            .node
            .lock()
            .await
            .commit_mergeable_many_in_schema(
                self.schema_version_id,
                vec![
                    MergeableCommit::new(table, row, self.next_now_ms())
                        .made_by(self.identity.author)
                        .parents(content_parents)
                        .cells(cells),
                    MergeableCommit::new(table, row, self.next_now_ms())
                        .made_by(self.identity.author)
                        .parents(deletion_parents)
                        .cells(BTreeMap::<String, Value>::new())
                        .deletion(DeletionEvent::Restored),
                ],
            )
            .await?;
        self.finish_published_write(row, published).await
    }

    /// Restore a row while evaluating write policy as `identity`.
    pub async fn restore_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.ensure_row_deleted(table, row, identity).await?;
        let (content_parents, deletion_parents) = {
            let mut node = self.node.node.lock().await;
            let content_parents = node
                .local_content_winner_tx_id(table, row)
                .await?
                .into_iter()
                .collect::<Vec<_>>();
            let deletion_parents = node
                .local_deletion_winner_tx_id(table, row)
                .await?
                .into_iter()
                .collect::<Vec<_>>();
            (content_parents, deletion_parents)
        };
        let published = self
            .node
            .node
            .lock()
            .await
            .commit_mergeable_many_in_schema(
                self.schema_version_id,
                vec![
                    MergeableCommit::new(table, row, self.next_now_ms())
                        .made_by(identity)
                        .permission_subject(identity)
                        .parents(content_parents)
                        .cells(cells),
                    MergeableCommit::new(table, row, self.next_now_ms())
                        .made_by(identity)
                        .permission_subject(identity)
                        .parents(deletion_parents)
                        .cells(BTreeMap::<String, Value>::new())
                        .deletion(DeletionEvent::Restored),
                ],
            )
            .await?;
        self.finish_published_write(row, published).await
    }

    async fn write_mergeable_as_session_subject(
        &self,
        made_by: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
    ) -> Result<WriteHandle<S>, Error> {
        self.check_attribution_allowed(made_by)?;
        self.write_mergeable(
            made_by,
            Some(self.identity.author),
            table,
            row,
            cells,
            parents,
            deletion,
        )
        .await
    }

    async fn write_mergeable_as_session_subject_with_authored_columns(
        &self,
        made_by: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
        authored_columns: BTreeSet<String>,
    ) -> Result<WriteHandle<S>, Error> {
        self.check_attribution_allowed(made_by)?;
        self.write_mergeable_with_authored_columns(
            made_by,
            Some(self.identity.author),
            table,
            row,
            cells,
            parents,
            deletion,
            authored_columns,
        )
        .await
    }

    /// Restore a row with an explicit millisecond provenance time.
    pub async fn restore_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.ensure_row_deleted(table, row, self.identity.author)
            .await?;
        let (content_parents, deletion_parents) = self.row_layer_parents(table, row).await?;
        let published = self
            .node
            .node
            .lock()
            .await
            .commit_mergeable_many_in_schema(
                self.schema_version_id,
                vec![
                    MergeableCommit::new(table, row, now_ms)
                        .made_by(self.identity.author)
                        .parents(content_parents)
                        .cells(cells),
                    MergeableCommit::new(table, row, now_ms)
                        .made_by(self.identity.author)
                        .parents(deletion_parents)
                        .cells(BTreeMap::<String, Value>::new())
                        .deletion(DeletionEvent::Restored),
                ],
            )
            .await?;
        self.finish_published_write(row, published).await
    }

    /// Restore a row for `identity` with an explicit millisecond provenance time.
    pub async fn restore_for_identity_at_ms(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.ensure_row_deleted(table, row, identity).await?;
        let (content_parents, deletion_parents) = self.row_layer_parents(table, row).await?;
        let published = self
            .node
            .node
            .lock()
            .await
            .commit_mergeable_many_in_schema(
                self.schema_version_id,
                vec![
                    MergeableCommit::new(table, row, now_ms)
                        .made_by(identity)
                        .permission_subject(identity)
                        .parents(content_parents)
                        .cells(cells),
                    MergeableCommit::new(table, row, now_ms)
                        .made_by(identity)
                        .permission_subject(identity)
                        .parents(deletion_parents)
                        .cells(BTreeMap::<String, Value>::new())
                        .deletion(DeletionEvent::Restored),
                ],
            )
            .await?;
        self.finish_published_write(row, published).await
    }

    async fn write_mergeable(
        &self,
        made_by: AuthorId,
        permission_subject: Option<AuthorId>,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
    ) -> Result<WriteHandle<S>, Error> {
        self.write_mergeable_at_ms(
            made_by,
            permission_subject,
            table,
            row,
            cells,
            parents,
            deletion,
            self.next_now_ms(),
        )
        .await
    }

    async fn write_mergeable_at_ms(
        &self,
        made_by: AuthorId,
        permission_subject: Option<AuthorId>,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.write_mergeable_at_ms_with_authorship(
            made_by,
            permission_subject,
            table,
            row,
            cells,
            parents,
            deletion,
            None,
            now_ms,
        )
        .await
    }

    async fn write_mergeable_with_authored_columns(
        &self,
        made_by: AuthorId,
        permission_subject: Option<AuthorId>,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
        authored_columns: BTreeSet<String>,
    ) -> Result<WriteHandle<S>, Error> {
        self.write_mergeable_at_ms_with_authorship(
            made_by,
            permission_subject,
            table,
            row,
            cells,
            parents,
            deletion,
            Some(authored_columns),
            self.next_now_ms(),
        )
        .await
    }

    async fn write_mergeable_at_ms_with_authorship(
        &self,
        made_by: AuthorId,
        permission_subject: Option<AuthorId>,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
        authored_columns: Option<BTreeSet<String>>,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.write_mergeable_at_ms_with_authorship_in_branch(
            made_by,
            permission_subject,
            table,
            row,
            cells,
            parents,
            deletion,
            authored_columns,
            now_ms,
            BranchSelector::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn write_mergeable_at_ms_with_authorship_in_branch(
        &self,
        made_by: AuthorId,
        permission_subject: Option<AuthorId>,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
        authored_columns: Option<BTreeSet<String>>,
        now_ms: u64,
        branch: BranchSelector,
    ) -> Result<WriteHandle<S>, Error> {
        let operation = if deletion == Some(DeletionEvent::Deleted) {
            "DELETE"
        } else if parents.is_empty() {
            "INSERT"
        } else {
            "UPDATE"
        };
        let cells = if operation == "INSERT" {
            self.apply_insert_defaults(table, cells)?
        } else {
            cells
        };
        let mut commit = MergeableCommit::new(table, row, now_ms)
            .branch(branch)
            .made_by(made_by)
            .parents(parents)
            .cells(cells);
        if let Some(authored_columns) = authored_columns {
            commit = commit.authored_columns(authored_columns);
        }
        if let Some(subject) = permission_subject {
            commit = commit.permission_subject(subject);
        }
        if let Some(deletion) = deletion {
            commit = commit.deletion(deletion);
        }
        // Db is an untrusted client: structurally valid writes are staged and
        // sent optimistically. A serving authority assigns the policy fate.
        let published = self
            .node
            .node
            .lock()
            .await
            .commit_mergeable_in_schema(self.schema_version_id, commit)
            .await?;
        self.finish_published_write(row, published).await
    }

    async fn finish_published_write(
        &self,
        row: RowUuid,
        published: PublishedTransaction,
    ) -> Result<WriteHandle<S>, Error> {
        let tx_id = published.tx_id;
        self.finish_publication_outcome(PublicationOutcome::published((), published))
            .await?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    pub(super) async fn finish_publication_outcome<T>(
        &self,
        outcome: PublicationOutcome<T>,
    ) -> Result<T, Error> {
        let PublicationOutcome {
            value,
            publications,
        } = outcome;
        if publications.is_empty() {
            return Ok(value);
        }
        self.refresh_subscriptions().await?;
        let mut persisted = Vec::with_capacity(publications.len());
        for publication in &publications {
            persisted.push(publication.persist().await);
        }
        let mut node = self.node.node.lock().await;
        for persistence in persisted {
            node.settle_published_transaction(persistence)?;
        }
        Ok(value)
    }

    fn check_attribution_allowed(&self, made_by: AuthorId) -> Result<(), Error> {
        if made_by == self.identity.author {
            return Ok(());
        }
        Err(Error::new(
            ErrorCode::WriteRejected,
            "attribution requires a trusted serving node",
        ))
    }

    pub(super) fn check_catalogue_admin(&self) -> Result<(), Error> {
        if self.identity.author == AuthorId::SYSTEM {
            return Ok(());
        }
        Err(Error::new(
            ErrorCode::Protocol,
            "catalogue updates require a serving Node",
        ))
    }

    /// Finalize a locally-committed exclusive transaction. A `Core` authority
    /// validates and accepts/rejects it now, using the in-memory commit unit
    /// (which still carries `base_snapshot` and the read sets); other roles
    /// queue it for upstream, leaving it Pending/Local.
    pub(super) fn finalize_local_exclusive_unit(
        &self,
        tx_id: TxId,
        unit: SyncMessage,
    ) -> Result<DurabilityTier, Error> {
        self.node.queue_pending_upload(tx_id, Some(unit));
        Ok(self.node.node.borrow().authored_commit_durability())
    }

    /// Client writes stay pending at this runtime's authored durability until
    /// peer durability or fate updates arrive over a connection.
    pub(super) fn finalize_local_commit(&self, tx_id: TxId) -> Result<DurabilityTier, Error> {
        self.node.queue_pending_upload(tx_id, None);
        Ok(self.node.node.borrow().authored_commit_durability())
    }

    pub(super) fn next_now_ms(&self) -> u64 {
        let next = self.next_now_ms.get();
        self.next_now_ms.set(next + 1);
        next
    }

    pub(super) fn current_write_schema_for_query(
        &self,
    ) -> Result<(JazzSchema, SchemaVersionId), Error> {
        if self.schema_view_is_fixed {
            return Ok((self.schema.clone(), self.schema_version_id));
        }
        let node = self.node.node.borrow();
        let current = node.current_write_schema().map_err(Error::from)?;
        if current.schema == self.schema_version_id {
            return Ok((self.schema.clone(), self.schema_version_id));
        }
        node.catalogue_schemas()
            .get(&current.schema)
            .map(|schema| (schema.schema.clone(), current.schema))
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::Schema,
                    format!(
                        "current write schema {:?} is missing from catalogue",
                        current.schema
                    ),
                )
            })
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
        mut cells: RowCells,
    ) -> Result<RowCells, Error> {
        let table_schema = self.table_schema(table)?;
        for column in &table_schema.columns {
            if !cells.contains_key(&column.name) {
                if let Some(default) = &column.default {
                    cells.insert(
                        column.name.clone(),
                        default_cell_for_column_type(&column.column_type, default),
                    );
                }
            }
        }
        Ok(cells)
    }

    async fn upsert_target_for_client_identity(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorId,
    ) -> Result<Option<CurrentRow>, Error> {
        let target = self
            .local_row_for_client_identity(table, row, identity)
            .await?;
        if target.is_some() {
            return Ok(target);
        }
        // A policy-filtered point read cannot by itself distinguish an absent
        // row from an existing row hidden from this identity. Upsert needs
        // exactly that distinction: a genuinely absent target follows INSERT
        // policy and does not require read permission, while merging into an
        // existing target must not expose or copy hidden cells.
        if self.local_current_row(table, row).await?.is_none() {
            return Ok(None);
        }
        if identity == AuthorId::SYSTEM || self.table_schema(table)?.read_policy.is_none() {
            return Ok(None);
        }
        Err(read_for_write_denied("UPSERT", table))
    }

    async fn upsert_target_for_trusted_identity(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorId,
    ) -> Result<Option<CurrentRow>, Error> {
        let target = self
            .local_row_for_trusted_identity(table, row, identity)
            .await?;
        if target.is_some() {
            return Ok(target);
        }
        // Trusted serving evaluates the identity's real read policy before
        // merging an existing row. A hidden existing row must not be treated
        // as an insert target.
        if self.local_current_row(table, row).await?.is_none() {
            return Ok(None);
        }
        if identity == AuthorId::SYSTEM || self.table_schema(table)?.read_policy.is_none() {
            return Ok(None);
        }
        Err(read_for_write_denied("UPSERT", table))
    }

    /// Read one locally-current row by primary key without evaluating a table
    /// query. This backend-scoped helper is used by import/upsert bridges that
    /// already operate with database authority and need an O(row) existence
    /// check before staging a write.
    pub async fn local_current_row(
        &self,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<CurrentRow>, Error> {
        self.table_schema(table)?;
        Ok(self
            .node
            .node
            .lock()
            .await
            .local_current_row(table, row)
            .await?)
    }

    async fn ensure_row_absent(
        &self,
        table: &str,
        row: RowUuid,
        _identity: AuthorId,
    ) -> Result<(), Error> {
        self.table_schema(table)?;
        let (content_parent, deletion_parent) = {
            let mut node = self.node.node.lock().await;
            (
                node.local_content_winner_tx_id(table, row).await?,
                node.local_deletion_winner_tx_id(table, row).await?,
            )
        };
        if deletion_parent.is_some() {
            return Err(row_already_deleted(row));
        }
        if content_parent.is_some() {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                format!("encoding error: object already exists: {}", row.0),
            ));
        }
        Ok(())
    }

    async fn ensure_exact_branch_row_absent(
        &self,
        table: &str,
        branch: &BranchSelector,
        row: RowUuid,
    ) -> Result<(), Error> {
        self.table_schema(table)?;
        let mut node = self.node.node.lock().await;
        let content = node
            .local_content_winner_tx_id_in_branch(table, branch, row)
            .await?;
        let deletion = node
            .local_deletion_winner_tx_id_in_branch(table, branch, row)
            .await?;
        if deletion.is_some() {
            return Err(row_already_deleted(row));
        }
        if content.is_some() {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                format!("encoding error: branch-local row already exists: {}", row.0),
            ));
        }
        Ok(())
    }

    async fn ensure_row_deleted(
        &self,
        table: &str,
        row: RowUuid,
        _identity: AuthorId,
    ) -> Result<(), Error> {
        self.table_schema(table)?;
        let deleted = self
            .node
            .node
            .lock()
            .await
            .local_deletion_winner_tx_id(table, row)
            .await?
            .is_some();
        if deleted {
            Ok(())
        } else {
            Err(Error::new(
                ErrorCode::WriteRejected,
                format!("row not deleted: {}", row.0),
            ))
        }
    }

    async fn ensure_row_not_deleted(&self, table: &str, row: RowUuid) -> Result<(), Error> {
        self.table_schema(table)?;
        let deleted = self
            .node
            .node
            .lock()
            .await
            .local_deletion_winner_tx_id(table, row)
            .await?
            .is_some();
        if deleted {
            Err(row_already_deleted(row))
        } else {
            Ok(())
        }
    }

    async fn row_layer_parents(
        &self,
        table: &str,
        row: RowUuid,
    ) -> Result<(Vec<TxId>, Vec<TxId>), Error> {
        let mut node = self.node.node.lock().await;
        let content_parents = node
            .local_content_winner_tx_id(table, row)
            .await?
            .into_iter()
            .collect::<Vec<_>>();
        let deletion_parents = node
            .local_deletion_winner_tx_id(table, row)
            .await?
            .into_iter()
            .collect::<Vec<_>>();
        Ok((content_parents, deletion_parents))
    }

    async fn local_row_for_client_identity(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorId,
    ) -> Result<Option<CurrentRow>, Error> {
        let query = self.prepare_query(&Query::from(table))?;
        Ok(self
            .node
            .node
            .lock()
            .await
            .query_rows_for_client(
                &query.shape,
                &query.binding,
                DurabilityTier::Local,
                identity,
            )?
            .into_iter()
            .find(|candidate| candidate.row_uuid() == row))
    }

    async fn local_row_for_trusted_identity(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorId,
    ) -> Result<Option<CurrentRow>, Error> {
        let query = self.prepare_query(&Query::from(table))?;
        Ok(self
            .node
            .node
            .lock()
            .await
            .query_rows_with_prepared_plan_for_identity(
                &query.shape,
                &query.binding,
                DurabilityTier::Local,
                None,
                identity,
            )?
            .into_iter()
            .find(|candidate| candidate.row_uuid() == row))
    }

    async fn no_op_update_handle_for_client(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorId,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row).await?;
        let existing = self
            .local_row_for_client_identity(table, row, identity)
            .await?
            .ok_or_else(|| read_for_write_denied("partial UPDATE", table))?;
        let tx_id = self
            .node
            .node
            .lock()
            .await
            .current_row_tx_id(&existing)
            .await
            .ok_or_else(|| Error::new(ErrorCode::NotObserved, "current row has no transaction"))?;
        let local_tier = self.write_state(tx_id)?.durability;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    async fn no_op_update_handle_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorId,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row).await?;
        let existing = self
            .local_row_for_trusted_identity(table, row, identity)
            .await?
            .ok_or_else(|| read_for_write_denied("partial UPDATE", table))?;
        let tx_id = self
            .node
            .node
            .lock()
            .await
            .current_row_tx_id(&existing)
            .await
            .ok_or_else(|| Error::new(ErrorCode::NotObserved, "current row has no transaction"))?;
        let local_tier = self.write_state(tx_id)?.durability;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    async fn merge_existing_cells(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<(RowCells, Option<TxId>, BTreeSet<String>), Error> {
        self.merge_existing_cells_for_client_identity(table, row, patch, self.identity.author)
            .await
    }

    async fn merge_existing_cells_for_client_identity(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        identity: AuthorId,
    ) -> Result<(RowCells, Option<TxId>, BTreeSet<String>), Error> {
        let table_schema = self.table_schema(table)?;
        self.ensure_row_not_deleted(table, row).await?;
        if table_schema
            .columns
            .iter()
            .all(|column| patch.contains_key(&column.name))
        {
            // A full-row write does not observe user data. Its causal parent is
            // storage bookkeeping, so obtain only that parent with system
            // authority rather than evaluating the writer's read policy.
            let parent = match self.local_current_row(table, row).await? {
                Some(existing) => {
                    self.node
                        .node
                        .lock()
                        .await
                        .current_row_tx_id(&existing)
                        .await
                }
                None => None,
            };
            let authored_columns = patch.keys().cloned().collect();
            return Ok((patch, parent, authored_columns));
        }
        let mut cells = BTreeMap::new();
        let existing = self
            .local_row_for_client_identity(table, row, identity)
            .await?
            .ok_or_else(|| read_for_write_denied("partial UPDATE", table))?;
        for column in &table_schema.columns {
            if let Some(value) = existing.cell(table_schema, &column.name) {
                cells.insert(
                    column.name.clone(),
                    default_cell_for_column_type(&column.column_type, &value),
                );
            }
        }
        let parent = self
            .node
            .node
            .lock()
            .await
            .current_row_tx_id(&existing)
            .await;
        let authored_columns = patch.keys().cloned().collect();
        cells.extend(patch);
        Ok((cells, parent, authored_columns))
    }

    async fn merge_existing_cells_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        identity: AuthorId,
    ) -> Result<(RowCells, Option<TxId>, BTreeSet<String>), Error> {
        let table_schema = self.table_schema(table)?;
        self.ensure_row_not_deleted(table, row).await?;
        if table_schema
            .columns
            .iter()
            .all(|column| patch.contains_key(&column.name))
        {
            let parent = match self.local_current_row(table, row).await? {
                Some(existing) => {
                    self.node
                        .node
                        .lock()
                        .await
                        .current_row_tx_id(&existing)
                        .await
                }
                None => None,
            };
            let authored_columns = patch.keys().cloned().collect();
            return Ok((patch, parent, authored_columns));
        }
        if self.authorize_read_for_identity(table, row, identity)? != PermissionAdvice::Allowed {
            return Err(read_for_write_denied("partial UPDATE", table));
        }
        let mut cells = BTreeMap::new();
        let existing = self
            .local_row_for_trusted_identity(table, row, identity)
            .await?
            .ok_or_else(|| read_for_write_denied("partial UPDATE", table))?;
        for column in &table_schema.columns {
            if let Some(value) = existing.cell(table_schema, &column.name) {
                cells.insert(
                    column.name.clone(),
                    default_cell_for_column_type(&column.column_type, &value),
                );
            }
        }
        let parent = self
            .node
            .node
            .lock()
            .await
            .current_row_tx_id(&existing)
            .await;
        let authored_columns = patch.keys().cloned().collect();
        cells.extend(patch);
        Ok((cells, parent, authored_columns))
    }
}
