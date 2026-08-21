//! Mergeable and exclusive transaction handles and staging.

use super::*;

impl<S> Db<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Build a mergeable transaction that commits multiple writes under one id.
    pub fn mergeable_tx(&self) -> Result<MergeableTx<'_, S>, Error> {
        let tx_id = OpenTransactionId::new();
        self.begin_mergeable(tx_id)?;
        Ok(MergeableTx {
            db: self,
            tx_id,
            committed: false,
        })
    }

    /// Run `callback` in a mergeable transaction and commit all staged writes as one transaction.
    ///
    /// If `callback` returns an error, the transaction is dropped without committing. Reads and
    /// writes through the [`MergeableTx`] observe earlier writes staged in the same callback.
    pub fn transaction<T>(
        &self,
        callback: impl FnOnce(&mut MergeableTx<'_, S>) -> Result<T, Error>,
    ) -> Result<(T, TxId), Error> {
        let mut tx = self.mergeable_tx()?;
        let value = callback(&mut tx)?;
        let tx_id = tx.commit()?;
        Ok((value, tx_id))
    }

    /// Build a mergeable transaction authored and permission-checked as `author`.
    pub fn mergeable_tx_for_identity(&self, author: AuthorId) -> Result<MergeableTx<'_, S>, Error> {
        let tx_id = OpenTransactionId::new();
        self.begin_mergeable_for_identity(tx_id, author)?;
        Ok(MergeableTx {
            db: self,
            tx_id,
            committed: false,
        })
    }

    /// Run `callback` in a mergeable transaction authored and permission-checked as `author`.
    ///
    /// If `callback` returns an error, the transaction is dropped without committing.
    pub fn transaction_for_identity<T>(
        &self,
        author: AuthorId,
        callback: impl FnOnce(&mut MergeableTx<'_, S>) -> Result<T, Error>,
    ) -> Result<(T, TxId), Error> {
        let mut tx = self.mergeable_tx_for_identity(author)?;
        let value = callback(&mut tx)?;
        let tx_id = tx.commit()?;
        Ok((value, tx_id))
    }

    /// Open a mergeable transaction and return its id.
    ///
    /// The caller owns this transaction's lifetime and must commit it with
    /// [`Db::commit_mergeable_handle`] or abandon it with
    /// [`Db::abandon_transaction_handle`]. Perform its writes through a
    /// [`MergeableTxRef`], which can be reconstructed from this id for each
    /// foreign-function call. Rust callers that want RAII should use
    /// [`Db::mergeable_tx`] instead.
    pub fn begin_mergeable(&self, id: OpenTransactionId) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .open_mergeable(id, self.identity.author, None)
            .map_err(Into::into)
    }

    /// Open a mergeable transaction authored and permission-checked as `author`.
    ///
    /// See [`Db::begin_mergeable`] for ownership and operation-handle guidance.
    pub fn begin_mergeable_for_identity(
        &self,
        id: OpenTransactionId,
        author: AuthorId,
    ) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .open_mergeable(id, author, Some(author))
            .map_err(Into::into)
    }

    /// Return a non-owning operations handle for an already-open mergeable transaction.
    ///
    /// This handle never closes the transaction when dropped, so it is suitable
    /// for a single call in a binding that retains `tx_id` between calls. Its
    /// CRUD API is defined by [`MergeableTxOps`] and is shared with the owning
    /// [`MergeableTx`] handle.
    pub fn mergeable_tx_ref(&self, tx_id: OpenTransactionId) -> MergeableTxRef<'_, S> {
        MergeableTxRef { db: self, tx_id }
    }

    pub(super) fn stage_mergeable_insert(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        let cells = self.apply_insert_defaults(table, cells)?;
        self.node
            .node
            .borrow_mut()
            .tx_write_mergeable_in_schema(
                tx_id,
                self.schema_version_id,
                table,
                row,
                cells,
                None,
                Vec::new(),
                now_ms,
                false,
            )
            .map_err(Into::into)
    }

    pub(super) fn stage_mergeable_insert_in_branch(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        branch: BranchSelector,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        let cells = self.apply_insert_defaults(table, cells)?;
        self.node
            .node
            .borrow_mut()
            .tx_write_mergeable_in_schema_and_branch(
                tx_id,
                self.schema_version_id,
                table,
                row,
                cells,
                None,
                Vec::new(),
                now_ms,
                false,
                branch,
            )?;
        Ok(())
    }

    pub(super) fn stage_mergeable_update(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        self.node
            .node
            .borrow_mut()
            .tx_patch_mergeable_in_schema(tx_id, self.schema_version_id, table, row, patch, now_ms)
            .map_err(Into::into)
    }

    pub(super) fn stage_mergeable_update_in_branch_view(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        head: BranchSelector,
        base: Option<BranchViewBase>,
        row: RowUuid,
        patch: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        if patch.is_empty() {
            return Err(Error::new(
                ErrorCode::Schema,
                "branch-view update requires at least one authored column",
            ));
        }
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        let head_cells = self
            .node
            .node
            .borrow_mut()
            .visible_current_cells_in_branch(table, &head, row)?;
        if head_cells.is_some() {
            self.node
                .node
                .borrow_mut()
                .tx_patch_mergeable_in_schema_and_branch(
                    tx_id,
                    self.schema_version_id,
                    table,
                    row,
                    patch,
                    now_ms,
                    head,
                )?;
            return Ok(());
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
        self.stage_mergeable_insert_in_branch(tx_id, table, head, row, inherited, now_ms)
    }

    pub(super) fn stage_mergeable_delete(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        self.node
            .node
            .borrow_mut()
            .tx_write_mergeable_in_schema(
                tx_id,
                self.schema_version_id,
                table,
                row,
                BTreeMap::new(),
                Some(DeletionEvent::Deleted),
                Vec::new(),
                now_ms,
                false,
            )
            .map_err(Into::into)
    }

    pub(super) fn stage_mergeable_delete_in_branch_view(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        head: BranchSelector,
        base: Option<BranchViewBase>,
        row: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
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
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        self.node
            .node
            .borrow_mut()
            .tx_write_mergeable_in_schema_and_branch(
                tx_id,
                self.schema_version_id,
                table,
                row,
                BTreeMap::new(),
                Some(DeletionEvent::Deleted),
                Vec::new(),
                now_ms,
                true,
                head,
            )?;
        Ok(())
    }

    pub(super) fn stage_mergeable_restore(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        let cells = self.apply_insert_defaults(table, cells)?;
        let mut node = self.node.node.borrow_mut();
        let content_parents = node
            .local_content_winner_tx_id(table, row)?
            .into_iter()
            .collect();
        let deletion_parents = node
            .local_deletion_winner_tx_id(table, row)?
            .into_iter()
            .collect();
        node.tx_write_mergeable_in_schema(
            tx_id,
            self.schema_version_id,
            table,
            row,
            cells,
            None,
            content_parents,
            now_ms,
            true,
        )?;
        node.tx_write_mergeable_in_schema(
            tx_id,
            self.schema_version_id,
            table,
            row,
            BTreeMap::new(),
            Some(DeletionEvent::Restored),
            deletion_parents,
            now_ms,
            true,
        )?;
        Ok(())
    }

    pub(super) fn stage_mergeable_restore_in_branch(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        branch: BranchSelector,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        let cells = self.apply_insert_defaults(table, cells)?;
        let mut node = self.node.node.borrow_mut();
        let content_parents = node
            .local_content_winner_tx_id_in_branch(table, &branch, row)?
            .into_iter()
            .collect();
        let deletion_parents = node
            .local_deletion_winner_tx_id_in_branch(table, &branch, row)?
            .into_iter()
            .collect();
        node.tx_write_mergeable_in_schema_and_branch(
            tx_id,
            self.schema_version_id,
            table,
            row,
            cells,
            None,
            content_parents,
            now_ms,
            true,
            branch.clone(),
        )?;
        node.tx_write_mergeable_in_schema_and_branch(
            tx_id,
            self.schema_version_id,
            table,
            row,
            BTreeMap::new(),
            Some(DeletionEvent::Restored),
            deletion_parents,
            now_ms,
            true,
            branch,
        )?;
        Ok(())
    }

    /// Commit an owned mergeable transaction handle.
    pub fn commit_mergeable_handle(&self, open_tx_id: OpenTransactionId) -> Result<TxId, Error> {
        let tx_id = self
            .node
            .node
            .borrow_mut()
            .commit_mergeable_open(open_tx_id, || self.next_now_ms())?;
        self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(tx_id)
    }

    /// Abandon an owned open transaction handle.
    pub fn abandon_transaction_handle(&self, open_tx_id: OpenTransactionId) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .abandon_tx(open_tx_id)
            .map_err(Into::into)
    }

    /// Open an exclusive transaction over the current local snapshot.
    ///
    /// This is the owning, RAII flavour. It abandons an uncommitted transaction
    /// on drop. Use [`Db::exclusive_tx_ref`] only when another layer retains the
    /// `OpenTransactionId` and owns that lifetime explicitly.
    pub fn exclusive_tx(&self) -> Result<ExclusiveTx<'_, S>, Error> {
        let tx_id = OpenTransactionId::new();
        self.open_exclusive_handle(tx_id)?;
        Ok(ExclusiveTx {
            db: self,
            tx_id,
            committed: false,
        })
    }

    /// Open an exclusive transaction and return its id.
    ///
    /// The caller owns this transaction's lifetime and must commit it with
    /// [`Db::commit_exclusive_handle`] or abandon it with
    /// [`Db::abandon_exclusive_handle`]. Perform its operations through an
    /// [`ExclusiveTxRef`]. Rust callers that want RAII should use
    /// [`Db::exclusive_tx`] instead.
    pub fn begin_exclusive(&self, id: OpenTransactionId) -> Result<(), Error> {
        self.open_exclusive_handle(id)
    }

    /// Return a non-owning operations handle for an already-open exclusive transaction.
    ///
    /// This handle never closes the transaction when dropped, so it is suitable
    /// for a single call in a binding that retains `tx_id` between calls. Its
    /// CRUD API is defined by [`ExclusiveTxOps`] and is shared with the owning
    /// [`ExclusiveTx`] handle.
    pub fn exclusive_tx_ref(&self, tx_id: OpenTransactionId) -> ExclusiveTxRef<'_, S> {
        ExclusiveTxRef { db: self, tx_id }
    }

    pub(super) fn exclusive_read(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<RowCells>, Error> {
        self.node
            .node
            .borrow_mut()
            .tx_read_in_schema(tx_id, self.schema_version_id, table, row)
            .map_err(Into::into)
    }

    pub(super) fn transaction_all(
        &self,
        tx_id: OpenTransactionId,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.transaction_all_in_authorization_mode(
            tx_id,
            prepared,
            self.identity.author,
            opts,
            QueryAuthorizationMode::ClientLocal,
        )
    }

    pub(crate) fn transaction_all_for_identity(
        &self,
        tx_id: OpenTransactionId,
        prepared: &PreparedQuery,
        author: AuthorId,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.transaction_all_in_authorization_mode(
            tx_id,
            prepared,
            author,
            opts,
            QueryAuthorizationMode::TrustedServing,
        )
    }

    fn transaction_all_in_authorization_mode(
        &self,
        tx_id: OpenTransactionId,
        prepared: &PreparedQuery,
        author: AuthorId,
        opts: ReadOpts,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<Vec<CurrentRow>, Error> {
        ensure_default_read_view(&opts)?;
        let mut node = self.node.node.borrow_mut();
        match authorization_mode {
            QueryAuthorizationMode::ClientLocal => node
                .tx_query_with_options(
                    tx_id,
                    &prepared.shape,
                    &prepared.binding,
                    opts.include_deleted,
                )
                .map_err(Into::into),
            QueryAuthorizationMode::TrustedServing => node
                .tx_query_for_identity_with_options(
                    tx_id,
                    &prepared.shape,
                    &prepared.binding,
                    author,
                    opts.include_deleted,
                )
                .map_err(Into::into),
        }
    }

    pub(super) fn stage_exclusive_insert(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<(), Error> {
        let now_ms = self.next_now_ms();
        let cells = self.apply_insert_defaults(table, cells)?;
        self.node
            .node
            .borrow_mut()
            .tx_write_in_schema_at_ms(
                tx_id,
                self.schema_version_id,
                table,
                row,
                cells,
                None,
                Some(now_ms),
            )
            .map_err(Into::into)
    }

    pub(super) fn stage_exclusive_delete(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
    ) -> Result<(), Error> {
        let now_ms = self.next_now_ms();
        self.node
            .node
            .borrow_mut()
            .tx_write_in_schema_at_ms(
                tx_id,
                self.schema_version_id,
                table,
                row,
                BTreeMap::<String, Value>::new(),
                Some(DeletionEvent::Deleted),
                Some(now_ms),
            )
            .map_err(Into::into)
    }

    pub(super) fn stage_exclusive_restore(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<(), Error> {
        let now_ms = self.next_now_ms();
        let cells = self.apply_insert_defaults(table, cells)?;
        let mut node = self.node.node.borrow_mut();
        // Restore needs one content version and one deletion-register version:
        // `tx_write` rejects a version carrying both. The layers have separate
        // winners and parent chains; see `restore`'s `local_*_winner_tx_id` pair.
        // Keep this staged form aligned with the committed restore path.
        node.tx_write_in_schema_at_ms(
            tx_id,
            self.schema_version_id,
            table,
            row,
            cells,
            None,
            Some(now_ms),
        )?;
        node.tx_write_in_schema_at_ms(
            tx_id,
            self.schema_version_id,
            table,
            row,
            BTreeMap::<String, Value>::new(),
            Some(DeletionEvent::Restored),
            Some(now_ms),
        )?;
        Ok(())
    }

    /// Commit an owned exclusive transaction handle.
    pub fn commit_exclusive_handle(&self, open_tx_id: OpenTransactionId) -> Result<TxId, Error> {
        let (tx_id, unit) = self.node.node.borrow_mut().commit_exclusive(
            open_tx_id,
            self.identity.author,
            self.next_now_ms(),
        )?;
        self.finalize_local_exclusive_unit(tx_id, unit)?;
        self.refresh_subscriptions()?;
        Ok(tx_id)
    }

    /// Abandon an owned exclusive transaction handle.
    pub fn abandon_exclusive_handle(&self, open_tx_id: OpenTransactionId) -> Result<(), Error> {
        self.abandon_transaction_handle(open_tx_id)
    }

    pub(crate) fn open_exclusive_handle(&self, id: OpenTransactionId) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .open_exclusive_for_identity(id, self.identity.author)
            .map_err(Into::into)
    }
}
