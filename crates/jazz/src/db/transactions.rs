//! Mergeable and exclusive transaction handles and staging.

use super::*;

impl<S> Db<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Return whether two schema facades share one open-transaction runtime.
    ///
    /// This compares the private runtime capability, not caller-controlled ids.
    #[doc(hidden)]
    pub fn shares_runtime_with(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.node, &other.node)
    }

    /// Build a mergeable transaction that commits multiple writes under one id.
    pub async fn mergeable_tx(&self) -> Result<MergeableTx<'_, S>, Error> {
        let tx_id = OpenTransactionId::new();
        self.begin_mergeable(tx_id).await?;
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
    pub async fn transaction<T>(
        &self,
        callback: impl AsyncFnOnce(&mut MergeableTx<'_, S>) -> Result<T, Error>,
    ) -> Result<(T, TxId), Error> {
        let mut tx = self.mergeable_tx().await?;
        let value = callback(&mut tx).await?;
        let tx_id = tx.commit().await?;
        Ok((value, tx_id))
    }

    /// Build a mergeable transaction authored and permission-checked as `author`.
    pub async fn mergeable_tx_for_identity(
        &self,
        author: AuthorSubject,
    ) -> Result<MergeableTx<'_, S>, Error> {
        let tx_id = OpenTransactionId::new();
        self.begin_mergeable_for_identity(tx_id, author).await?;
        Ok(MergeableTx {
            db: self,
            tx_id,
            committed: false,
        })
    }

    /// Run `callback` in a mergeable transaction authored and permission-checked as `author`.
    ///
    /// If `callback` returns an error, the transaction is dropped without committing.
    pub async fn transaction_for_identity<T>(
        &self,
        author: AuthorSubject,
        callback: impl AsyncFnOnce(&mut MergeableTx<'_, S>) -> Result<T, Error>,
    ) -> Result<(T, TxId), Error> {
        let mut tx = self.mergeable_tx_for_identity(author).await?;
        let value = callback(&mut tx).await?;
        let tx_id = tx.commit().await?;
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
    pub async fn begin_mergeable(&self, id: OpenTransactionId) -> Result<(), Error> {
        self.ensure_mutation_operation_admitted()?;
        self.node
            .node
            .lock()
            .await
            .open_mergeable(id, self.identity.author, None)
            .await
            .map_err(Into::into)
    }

    /// Open a mergeable transaction authored and permission-checked as `author`.
    ///
    /// See [`Db::begin_mergeable`] for ownership and operation-handle guidance.
    pub async fn begin_mergeable_for_identity(
        &self,
        id: OpenTransactionId,
        author: AuthorSubject,
    ) -> Result<(), Error> {
        self.ensure_mutation_operation_admitted()?;
        self.node
            .node
            .lock()
            .await
            .open_mergeable(id, author, Some(author))
            .await
            .map_err(Into::into)
    }

    /// Open a mergeable transaction admitted as this Db while retaining an
    /// external provenance author for every staged write.
    #[doc(hidden)]
    pub async fn begin_mergeable_attributed(
        &self,
        id: OpenTransactionId,
        made_by: AuthorSubject,
    ) -> Result<(), Error> {
        self.ensure_mutation_operation_admitted()?;
        if made_by != self.identity.author && !self.backend_attribution {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                "attribution requires a trusted serving node",
            ));
        }
        self.node
            .node
            .lock()
            .await
            .open_mergeable(id, made_by, Some(self.identity.author))
            .await
            .map_err(Into::into)
    }

    /// Queue mergeable transaction admission behind earlier owner operations.
    #[doc(hidden)]
    pub fn enqueue_begin_mergeable(
        &self,
        id: OpenTransactionId,
        author: Option<AuthorSubject>,
        attribution: Option<AuthorSubject>,
    ) -> Result<(), Error> {
        if attribution.is_some() && author.is_some() {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                "attributed transaction cannot override admission identity",
            ));
        }
        if let Some(made_by) = attribution
            && made_by != self.identity.author
            && !self.backend_attribution
        {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                "attribution requires a trusted serving node",
            ));
        }
        let db = self.clone_for_owner_operation();
        self.node.enqueue_transaction_operation(
            id,
            Box::pin(async move {
                match (author, attribution) {
                    (_, Some(attribution)) => db.begin_mergeable_attributed(id, attribution).await,
                    (Some(author), None) => db.begin_mergeable_for_identity(id, author).await,
                    (None, None) => db.begin_mergeable(id).await,
                }
            }),
        )
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

    async fn reject_attributed_mergeable_branch(
        &self,
        tx_id: OpenTransactionId,
    ) -> Result<(), Error> {
        if self
            .node
            .node
            .lock()
            .await
            .mergeable_transaction_is_attributed(tx_id)?
        {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                "backend-attributed transactions do not support branch targets",
            ));
        }
        Ok(())
    }

    pub(super) async fn stage_mergeable_insert(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
        known_fresh_row: bool,
    ) -> Result<(), Error> {
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        let cells = self.apply_insert_defaults(table, cells)?;
        let mut node = self.node.node.lock().await;
        node.tx_write_mergeable_in_schema(
            tx_id,
            self.schema_version_id,
            table,
            row,
            cells,
            None,
            Vec::new(),
            now_ms,
            false,
            known_fresh_row,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn stage_mergeable_insert_in_branch(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        branch: BranchSelector,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
        known_fresh_row: bool,
    ) -> Result<(), Error> {
        self.stage_mergeable_insert_in_branch_with_verified_inherited_cells(
            tx_id,
            table,
            branch,
            row,
            cells,
            now_ms,
            known_fresh_row,
            None,
            false,
        )
        .await
    }

    /// Stage an exact branch write and, only for a branch-view fallback, carry
    /// the engine-read base cells that may safely retain indirect values.
    #[allow(clippy::too_many_arguments)]
    async fn stage_mergeable_insert_in_branch_with_verified_inherited_cells(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        branch: BranchSelector,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
        known_fresh_row: bool,
        verified_inherited_cells: Option<RowCells>,
        replace_pending_deletion: bool,
    ) -> Result<(), Error> {
        self.reject_attributed_mergeable_branch(tx_id).await?;
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        let cells = self.apply_insert_defaults(table, cells)?;
        let mut node = self.node.node.lock().await;
        node.tx_write_mergeable_in_schema_and_branch_with_verified_inherited_cells(
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
            known_fresh_row,
            verified_inherited_cells,
            replace_pending_deletion,
        )?;
        Ok(())
    }

    pub(super) async fn stage_mergeable_update(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.require_mergeable_transaction_read_visibility(tx_id, table, row, "UPDATE")
            .await?;
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        self.node
            .node
            .lock()
            .await
            .tx_patch_mergeable_in_schema(tx_id, self.schema_version_id, table, row, patch, now_ms)
            .await
            .map_err(Into::into)
    }

    pub(super) async fn stage_mergeable_update_in_branch_view(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        head: BranchSelector,
        base: Option<BranchViewBase>,
        row: RowUuid,
        patch: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.reject_attributed_mergeable_branch(tx_id).await?;
        if patch.is_empty() {
            return Err(Error::new(
                ErrorCode::Schema,
                "branch-view update requires at least one authored column",
            ));
        }
        let permission_subject = self
            .node
            .node
            .lock()
            .await
            .mergeable_transaction_permission_subject(tx_id)?;
        if let Some(identity) = permission_subject {
            self.visible_branch_view_cells_for_identity(table, &head, base.as_ref(), row, identity)
                .await?
                .ok_or_else(|| read_for_write_denied("UPDATE", table))?;
        }
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        let head_cells = self
            .node
            .node
            .lock()
            .await
            .visible_current_cells_in_branch(table, &head, row)
            .await?;
        if head_cells.is_some() {
            self.node
                .node
                .lock()
                .await
                .tx_patch_mergeable_in_schema_and_branch(
                    tx_id,
                    self.schema_version_id,
                    table,
                    row,
                    patch,
                    now_ms,
                    head,
                    false,
                )
                .await?;
            return Ok(());
        }
        let Some(mut inherited) = self
            .node
            .node
            .lock()
            .await
            .visible_current_cells_in_branch_view(table, &head, base.as_ref(), row)
            .await?
        else {
            return Err(Error::new(
                ErrorCode::NotObserved,
                format!("row is not visible in branch view: {}", row.0),
            ));
        };
        let verified_inherited_cells = inherited.clone();
        inherited.extend(patch);
        self.stage_mergeable_insert_in_branch_with_verified_inherited_cells(
            tx_id,
            table,
            head,
            row,
            inherited,
            now_ms,
            false,
            Some(verified_inherited_cells),
            false,
        )
        .await
    }

    pub(super) async fn require_mergeable_transaction_upsert_visibility(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
    ) -> Result<(), Error> {
        self.require_mergeable_transaction_read_visibility(tx_id, table, row, "UPSERT")
            .await
    }

    async fn require_mergeable_transaction_read_visibility(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
        operation: &str,
    ) -> Result<(), Error> {
        let permission_subject = self
            .node
            .node
            .lock()
            .await
            .mergeable_transaction_permission_subject(tx_id)?;
        let Some(identity) = permission_subject else {
            return Ok(());
        };
        // Resolve against this transaction's fixed snapshot plus its staged
        // overlay. A session may update/upsert a row it inserted earlier in
        // the same transaction, while a hidden snapshot or overlay row still
        // follows the same non-disclosing denial path.
        let target = self.transaction_read(tx_id, table, row).await?;
        let visible = match (&target, self.table_schema(table)?.read_policy.clone()) {
            (Some(_), _) if identity == AuthorSubject::SYSTEM => true,
            (Some(_), None) => true,
            (Some(_), Some(policy)) => {
                self.node
                    .node
                    .lock()
                    .await
                    .read_policy_query_allows_open_tx_row(
                        tx_id,
                        &policy,
                        self.schema_version_id,
                        row,
                        identity,
                    )
                    .await?
            }
            (None, _) => false,
        };
        if target.is_some() && visible {
            return Ok(());
        }
        if target.is_some() || operation == "UPDATE" {
            return Err(read_for_write_denied(operation, table));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn stage_mergeable_upsert_in_branch_view(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        head: BranchSelector,
        base: Option<BranchViewBase>,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.reject_attributed_mergeable_branch(tx_id).await?;
        self.ensure_branch_view_row_not_deleted(table, &head, base.as_ref(), row)
            .await?;
        let permission_subject = self
            .node
            .node
            .lock()
            .await
            .mergeable_transaction_permission_subject(tx_id)?;
        let mut node = self.node.node.lock().await;
        let (head_exists, staged_head, replace_pending_deletion) = match node
            .tx_current_row_state_in_branch(tx_id, table, row, &head)
            .await?
        {
            TransactionBranchRowState::Visible { staged, .. } => (true, staged, false),
            TransactionBranchRowState::PendingDeletion => (
                node.visible_current_cells_in_branch(table, &head, row)
                    .await?
                    .is_some(),
                false,
                true,
            ),
            TransactionBranchRowState::Absent => (false, false, false),
        };
        let inherited = if head_exists {
            None
        } else {
            node.visible_current_cells_in_branch_view(table, &head, base.as_ref(), row)
                .await?
        };
        drop(node);

        if head_exists {
            if let Some(identity) = permission_subject {
                let visible = if staged_head {
                    match self.table_schema(table)?.read_policy.clone() {
                        None => true,
                        Some(_) if identity == AuthorSubject::SYSTEM => true,
                        Some(policy) => {
                            self.node
                                .node
                                .lock()
                                .await
                                .read_policy_query_allows_open_tx_row(
                                    tx_id,
                                    &policy,
                                    self.schema_version_id,
                                    row,
                                    identity,
                                )
                                .await?
                        }
                    }
                } else {
                    self.visible_branch_view_cells_for_identity(
                        table,
                        &head,
                        base.as_ref(),
                        row,
                        identity,
                    )
                    .await?
                    .is_some()
                };
                if !visible {
                    return Err(read_for_write_denied("UPSERT", table));
                }
            }
            if cells.is_empty() {
                return Err(Error::new(
                    ErrorCode::Schema,
                    "branch upsert update requires at least one authored column",
                ));
            }
            let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
            self.node
                .node
                .lock()
                .await
                .tx_patch_mergeable_in_schema_and_branch(
                    tx_id,
                    self.schema_version_id,
                    table,
                    row,
                    cells,
                    now_ms,
                    head,
                    replace_pending_deletion,
                )
                .await?;
            return Ok(());
        }

        if inherited.is_some()
            && let Some(identity) = permission_subject
            && self
                .visible_branch_view_cells_for_identity(table, &head, base.as_ref(), row, identity)
                .await?
                .is_none()
        {
            return Err(read_for_write_denied("UPSERT", table));
        }
        let verified_inherited_cells = inherited.clone();
        let mut inserted = inherited.unwrap_or_default();
        inserted.extend(cells);
        self.stage_mergeable_insert_in_branch_with_verified_inherited_cells(
            tx_id,
            table,
            head,
            row,
            inserted,
            now_ms,
            false,
            verified_inherited_cells,
            replace_pending_deletion,
        )
        .await
    }

    pub(super) async fn stage_mergeable_delete(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        self.node
            .node
            .lock()
            .await
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
                false,
            )
            .await
            .map_err(Into::into)
    }

    pub(super) async fn stage_mergeable_delete_in_branch_view(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        head: BranchSelector,
        base: Option<BranchViewBase>,
        row: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.reject_attributed_mergeable_branch(tx_id).await?;
        if self
            .node
            .node
            .lock()
            .await
            .visible_current_cells_in_branch_view(table, &head, base.as_ref(), row)
            .await?
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
            .lock()
            .await
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
                false,
            )?;
        Ok(())
    }

    pub(super) async fn stage_mergeable_restore(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.reject_attributed_mergeable_branch(tx_id).await?;
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        let cells = self.apply_insert_defaults(table, cells)?;
        let mut node = self.node.node.lock().await;
        let content_parents = node
            .local_content_winner_tx_id(table, row)
            .await?
            .into_iter()
            .collect();
        let deletion_parents = node
            .local_deletion_winner_tx_id(table, row)
            .await?
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
            false,
        )
        .await?;
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
            false,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn stage_mergeable_restore_in_branch(
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
        let mut node = self.node.node.lock().await;
        let content_parents = node
            .local_content_winner_tx_id_in_branch(table, &branch, row)
            .await?
            .into_iter()
            .collect();
        let deletion_parents = node
            .local_deletion_winner_tx_id_in_branch(table, &branch, row)
            .await?
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
            false,
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
            false,
        )?;
        Ok(())
    }

    /// Commit an owned mergeable transaction handle.
    pub async fn commit_mergeable_handle(
        &self,
        open_tx_id: OpenTransactionId,
    ) -> Result<TxId, Error> {
        let published = self
            .node
            .node
            .lock()
            .await
            .commit_mergeable_open(open_tx_id, || self.next_now_ms())
            .await?;
        let tx_id = published.tx_id;
        if self.node.defer_local_persistence.get() {
            self.admit_deferred_local_publication(published, None)
                .await?;
        } else {
            self.finish_publication_outcome(PublicationOutcome::published((), published))
                .await?;
            self.finalize_local_commit(tx_id)?;
        }
        Ok(tx_id)
    }

    /// Reserve the final identity and retain mergeable commit/finalization on
    /// the node owner. Binding callers use this after synchronous staging so
    /// cold parent refresh or persistence cannot strand the commit future.
    #[doc(hidden)]
    pub fn enqueue_commit_mergeable_handle(
        &self,
        open_tx_id: OpenTransactionId,
    ) -> Result<WriteHandle<S>, Error> {
        let now_ms = self.next_now_ms();
        let tx_id = self.reserve_transaction_id_at_ms(now_ms)?;
        let db = self.clone_for_reserved_transaction(tx_id);
        let status = self.node.enqueue_transaction_commit(
            open_tx_id,
            tx_id,
            Box::pin(async move {
                let published = db
                    .node
                    .node
                    .lock()
                    .await
                    .commit_mergeable_open_at(open_tx_id, tx_id, || now_ms)
                    .await?;
                debug_assert_eq!(published.tx_id, tx_id);
                if db.node.defer_local_persistence.get() {
                    db.node.queue_local_publication(published, None);
                } else {
                    db.finish_publication_outcome(PublicationOutcome::published((), published))
                        .await?;
                    db.finalize_local_commit(tx_id)?;
                }
                Ok(())
            }),
        );
        Ok(self.queued_write_handle(RowUuid::from_bytes([0; 16]), tx_id, status, None))
    }

    /// Abandon an owned open transaction handle.
    pub fn abandon_transaction_handle(&self, open_tx_id: OpenTransactionId) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .abandon_tx(open_tx_id)
            .map_err(Into::into)
    }

    /// Queue rollback after all earlier admission/staging work. Missing state
    /// is accepted because a failed queued begin is already terminal.
    #[doc(hidden)]
    pub fn enqueue_abandon_transaction_handle(&self, open_tx_id: OpenTransactionId) {
        let db = self.clone_for_owner_operation();
        self.node.enqueue_transaction_cleanup(Box::pin(async move {
            let mut node = db.node.node.lock().await;
            match node.abandon_tx(open_tx_id) {
                Ok(()) | Err(crate::node::Error::MissingOpenBatch(_)) => Ok(()),
                Err(error) => Err(error.into()),
            }
        }));
    }

    /// Open an exclusive transaction over the current local snapshot.
    ///
    /// This is the owning, RAII flavour. It abandons an uncommitted transaction
    /// on drop. Use [`Db::exclusive_tx_ref`] only when another layer retains the
    /// `OpenTransactionId` and owns that lifetime explicitly.
    pub async fn exclusive_tx(&self) -> Result<ExclusiveTx<'_, S>, Error> {
        let tx_id = OpenTransactionId::new();
        self.open_exclusive_handle(tx_id).await?;
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
    pub async fn begin_exclusive(&self, id: OpenTransactionId) -> Result<(), Error> {
        self.ensure_mutation_operation_admitted()?;
        self.open_exclusive_handle(id).await
    }

    /// Open an exclusive transaction whose identity is fixed for its lifetime.
    ///
    /// Transaction-local reads, authorization, provenance, and commit
    /// attribution all use `author`; subsequent calls cannot replace it.
    #[doc(hidden)]
    pub async fn begin_exclusive_for_identity(
        &self,
        id: OpenTransactionId,
        author: AuthorSubject,
    ) -> Result<(), Error> {
        self.ensure_mutation_operation_admitted()?;
        self.open_exclusive_handle_for_identity(id, author).await
    }

    /// Queue exclusive snapshot admission behind earlier owner operations.
    #[doc(hidden)]
    pub fn enqueue_begin_exclusive(
        &self,
        id: OpenTransactionId,
        author: Option<AuthorSubject>,
    ) -> Result<(), Error> {
        let db = self.clone_for_owner_operation();
        self.node.enqueue_transaction_operation(
            id,
            Box::pin(async move {
                match author {
                    Some(author) => db.begin_exclusive_for_identity(id, author).await,
                    None => db.begin_exclusive(id).await,
                }
            }),
        )
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

    #[doc(hidden)]
    pub fn enqueue_transaction_insert(
        &self,
        id: OpenTransactionId,
        exclusive: bool,
        table: String,
        cells: RowCells,
        mut options: InsertOptions,
    ) -> Result<RowUuid, Error> {
        let row = options
            .row_id
            .unwrap_or_else(|| self.row_id_source.borrow_mut().next_row_id());
        options.row_id = Some(row);
        let db = self.clone_for_owner_operation();
        self.node.enqueue_transaction_operation(
            id,
            Box::pin(async move {
                if exclusive {
                    db.exclusive_tx_ref(id)
                        .insert(&table, cells, options)
                        .await?;
                } else {
                    db.mergeable_tx_ref(id)
                        .insert(&table, cells, options)
                        .await?;
                }
                Ok(())
            }),
        )?;
        Ok(row)
    }

    #[doc(hidden)]
    pub fn enqueue_transaction_update(
        &self,
        id: OpenTransactionId,
        exclusive: bool,
        table: String,
        row: RowUuid,
        patch: RowCells,
        options: UpdateOptions,
    ) -> Result<(), Error> {
        let db = self.clone_for_owner_operation();
        self.node.enqueue_transaction_operation(
            id,
            Box::pin(async move {
                if exclusive {
                    db.exclusive_tx_ref(id)
                        .update(&table, row, patch, options)
                        .await
                } else {
                    db.mergeable_tx_ref(id)
                        .update(&table, row, patch, options)
                        .await
                }
            }),
        )
    }

    #[doc(hidden)]
    pub fn enqueue_transaction_upsert(
        &self,
        id: OpenTransactionId,
        exclusive: bool,
        table: String,
        row: RowUuid,
        cells: RowCells,
        options: UpsertOptions,
    ) -> Result<(), Error> {
        let db = self.clone_for_owner_operation();
        self.node.enqueue_transaction_operation(
            id,
            Box::pin(async move {
                if exclusive {
                    db.exclusive_tx_ref(id)
                        .upsert(&table, row, cells, options)
                        .await
                } else {
                    db.mergeable_tx_ref(id)
                        .upsert(&table, row, cells, options)
                        .await
                }
            }),
        )
    }

    #[doc(hidden)]
    pub fn enqueue_transaction_delete(
        &self,
        id: OpenTransactionId,
        exclusive: bool,
        table: String,
        row: RowUuid,
        options: DeleteOptions,
    ) -> Result<(), Error> {
        let db = self.clone_for_owner_operation();
        self.node.enqueue_transaction_operation(
            id,
            Box::pin(async move {
                if exclusive {
                    db.exclusive_tx_ref(id).delete(&table, row, options).await
                } else {
                    db.mergeable_tx_ref(id).delete(&table, row, options).await
                }
            }),
        )
    }

    #[doc(hidden)]
    pub fn enqueue_transaction_restore(
        &self,
        id: OpenTransactionId,
        exclusive: bool,
        table: String,
        row: RowUuid,
        cells: Option<RowCells>,
        options: RestoreOptions,
    ) -> Result<(), Error> {
        let db = self.clone_for_owner_operation();
        self.node.enqueue_transaction_operation(
            id,
            Box::pin(async move {
                if exclusive {
                    db.exclusive_tx_ref(id)
                        .restore(&table, row, cells, options)
                        .await
                } else {
                    db.mergeable_tx_ref(id)
                        .restore(&table, row, cells, options)
                        .await
                }
            }),
        )
    }

    pub(super) async fn transaction_read(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<RowCells>, Error> {
        let mut cells = self.transaction_read_raw(tx_id, table, row).await?;
        let node = self.node.node.lock().await;
        if let Some(cells) = &mut cells {
            node.hydrate_large_value_cells(cells).await?;
        }
        Ok(cells)
    }

    /// Read the storage-form transaction row used as a mutation base.
    ///
    /// Mutation staging must preserve existing large-value locators; public
    /// transaction reads hydrate those values separately above.
    async fn transaction_read_raw(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<RowCells>, Error> {
        let cells = self
            .node
            .node
            .lock()
            .await
            .tx_read_in_schema(tx_id, self.schema_version_id, table, row)
            .await?;
        Ok(cells)
    }

    pub(super) async fn transaction_all(
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
        .await
    }

    pub(crate) async fn transaction_all_for_identity(
        &self,
        tx_id: OpenTransactionId,
        prepared: &PreparedQuery,
        author: AuthorSubject,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.transaction_all_in_authorization_mode(
            tx_id,
            prepared,
            author,
            opts,
            QueryAuthorizationMode::TrustedServing,
        )
        .await
    }

    pub(super) async fn transaction_relation_snapshot(
        &self,
        tx_id: OpenTransactionId,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        self.transaction_relation_snapshot_in_authorization_mode(
            tx_id,
            prepared,
            self.identity.author,
            opts,
            QueryAuthorizationMode::ClientLocal,
        )
        .await
    }

    pub(crate) async fn transaction_relation_snapshot_for_identity(
        &self,
        tx_id: OpenTransactionId,
        prepared: &PreparedQuery,
        author: AuthorSubject,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        self.transaction_relation_snapshot_in_authorization_mode(
            tx_id,
            prepared,
            author,
            opts,
            QueryAuthorizationMode::TrustedServing,
        )
        .await
    }

    async fn transaction_relation_snapshot_in_authorization_mode(
        &self,
        tx_id: OpenTransactionId,
        prepared: &PreparedQuery,
        author: AuthorSubject,
        opts: ReadOpts,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<RelationSnapshot, Error> {
        ensure_default_read_view(&opts)?;
        let mut node = self.node.node.lock().await;
        let mut snapshot = match authorization_mode {
            QueryAuthorizationMode::ClientLocal => node
                .tx_relation_snapshot_with_options(
                    tx_id,
                    &prepared.shape,
                    &prepared.binding,
                    opts.include_deleted,
                )
                .await
                .map_err(Error::from)?,
            QueryAuthorizationMode::TrustedServing => node
                .tx_relation_snapshot_for_identity_with_options(
                    tx_id,
                    &prepared.shape,
                    &prepared.binding,
                    author,
                    opts.include_deleted,
                )
                .await
                .map_err(Error::from)?,
        };
        node.hydrate_current_rows(&mut snapshot.rows).await?;
        Ok(snapshot)
    }

    async fn transaction_all_in_authorization_mode(
        &self,
        tx_id: OpenTransactionId,
        prepared: &PreparedQuery,
        author: AuthorSubject,
        opts: ReadOpts,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<Vec<CurrentRow>, Error> {
        ensure_default_read_view(&opts)?;
        let mut node = self.node.node.lock().await;
        let mut rows = match authorization_mode {
            QueryAuthorizationMode::ClientLocal => node
                .tx_query_with_options(
                    tx_id,
                    &prepared.shape,
                    &prepared.binding,
                    opts.include_deleted,
                )
                .await
                .map_err(Error::from)?,
            QueryAuthorizationMode::TrustedServing => node
                .tx_query_for_identity_with_options(
                    tx_id,
                    &prepared.shape,
                    &prepared.binding,
                    author,
                    opts.include_deleted,
                )
                .await
                .map_err(Error::from)?,
        };
        node.hydrate_current_rows(&mut rows).await?;
        Ok(rows)
    }

    pub(super) async fn transaction_current_rows(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut node = self.node.node.lock().await;
        let mut rows = node.tx_current_rows(tx_id, table).await?;
        node.hydrate_current_rows(&mut rows).await?;
        Ok(rows)
    }

    pub(super) async fn stage_exclusive_insert(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        updated_at_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = updated_at_ms.unwrap_or_else(|| self.next_now_ms());
        let cells = self.apply_insert_defaults(table, cells)?;
        self.node
            .node
            .lock()
            .await
            .tx_write_in_schema_at_ms(
                tx_id,
                self.schema_version_id,
                table,
                row,
                cells,
                None,
                Some(now_ms),
            )
            .await
            .map_err(Into::into)
    }

    pub(super) async fn stage_exclusive_update(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        updated_at_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = updated_at_ms.unwrap_or_else(|| self.next_now_ms());
        let mut cells = self
            .exclusive_transaction_target_for_write(tx_id, table, row, "UPDATE", false)
            .await?
            .expect("exclusive UPDATE requires a visible target");
        cells.extend(patch);
        self.node
            .node
            .lock()
            .await
            .tx_write_in_schema_at_ms(
                tx_id,
                self.schema_version_id,
                table,
                row,
                cells,
                None,
                Some(now_ms),
            )
            .await?;
        Ok(())
    }

    pub(super) async fn stage_exclusive_upsert(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        updated_at_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = updated_at_ms.unwrap_or_else(|| self.next_now_ms());
        let mut cells = self
            .exclusive_transaction_target_for_write(tx_id, table, row, "UPSERT", true)
            .await?
            .unwrap_or_default();
        cells.extend(patch);
        let cells = self.apply_insert_defaults(table, cells)?;
        self.node
            .node
            .lock()
            .await
            .tx_write_in_schema_at_ms(
                tx_id,
                self.schema_version_id,
                table,
                row,
                cells,
                None,
                Some(now_ms),
            )
            .await?;
        Ok(())
    }

    /// Resolve an exclusive mutation target through the identity fixed when
    /// the transaction opened, then record the corresponding snapshot row or
    /// absence read used by optimistic conflict validation.
    async fn exclusive_transaction_target_for_write(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
        operation: &str,
        allow_absent: bool,
    ) -> Result<Option<RowCells>, Error> {
        let identity = self
            .node
            .node
            .lock()
            .await
            .exclusive_transaction_bound_author(tx_id)?;
        let read_policy = self.table_schema(table)?.read_policy.clone();
        // This authoritative point read distinguishes a hidden target from a
        // genuinely absent one and records the exact snapshot/absence read for
        // conflict detection. Its result is never returned to the session.
        let target = self.transaction_read_raw(tx_id, table, row).await?;
        let visible = match (&target, read_policy) {
            (Some(_), _) if identity == AuthorSubject::SYSTEM => true,
            (Some(_), None) => true,
            (Some(_), Some(policy)) => {
                self.node
                    .node
                    .lock()
                    .await
                    .read_policy_query_allows_open_tx_row(
                        tx_id,
                        &policy,
                        self.schema_version_id,
                        row,
                        identity,
                    )
                    .await?
            }
            (None, _) => false,
        };
        if target.is_some() && !visible || target.is_none() && !allow_absent {
            return Err(read_for_write_denied(operation, table));
        }
        Ok(target)
    }

    pub(super) async fn stage_exclusive_delete(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
        updated_at_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = updated_at_ms.unwrap_or_else(|| self.next_now_ms());
        self.node
            .node
            .lock()
            .await
            .tx_write_in_schema_at_ms(
                tx_id,
                self.schema_version_id,
                table,
                row,
                BTreeMap::<String, Value>::new(),
                Some(DeletionEvent::Deleted),
                Some(now_ms),
            )
            .await
            .map_err(Into::into)
    }

    pub(super) async fn stage_exclusive_restore(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        updated_at_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = updated_at_ms.unwrap_or_else(|| self.next_now_ms());
        let cells = self.apply_insert_defaults(table, cells)?;
        let mut node = self.node.node.lock().await;
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
        )
        .await?;
        node.tx_write_in_schema_at_ms(
            tx_id,
            self.schema_version_id,
            table,
            row,
            BTreeMap::<String, Value>::new(),
            Some(DeletionEvent::Restored),
            Some(now_ms),
        )
        .await?;
        Ok(())
    }

    /// Commit an owned exclusive transaction handle.
    pub async fn commit_exclusive_handle(
        &self,
        open_tx_id: OpenTransactionId,
    ) -> Result<TxId, Error> {
        let (published, unit) = self
            .node
            .node
            .lock()
            .await
            .commit_exclusive_bound(open_tx_id, self.next_now_ms())
            .await?;
        self.finish_exclusive_publication(published, unit).await
    }

    /// Reserve the final identity and retain exclusive serializability,
    /// publication, and finalization on the node owner.
    #[doc(hidden)]
    pub fn enqueue_commit_exclusive_handle(
        &self,
        open_tx_id: OpenTransactionId,
    ) -> Result<WriteHandle<S>, Error> {
        let now_ms = self.next_now_ms();
        let tx_id = self.reserve_transaction_id_at_ms(now_ms)?;
        let db = self.clone_for_reserved_transaction(tx_id);
        let status = self.node.enqueue_transaction_commit(
            open_tx_id,
            tx_id,
            Box::pin(async move {
                let (published, unit) = db
                    .node
                    .node
                    .lock()
                    .await
                    .commit_exclusive_bound_at(open_tx_id, tx_id)
                    .await?;
                debug_assert_eq!(published.tx_id, tx_id);
                let committed = db.finish_exclusive_publication(published, unit).await?;
                debug_assert_eq!(committed, tx_id);
                Ok(())
            }),
        );
        Ok(self.queued_write_handle(RowUuid::from_bytes([0; 16]), tx_id, status, None))
    }

    /// Commit an owned exclusive transaction as an explicit policy identity.
    ///
    /// Bindings that expose session-scoped transactions use this rather than
    /// the connection's default identity so a trusted backend cannot silently
    /// turn a `for_session` transaction into a system-authored commit.
    #[cfg_attr(not(feature = "testing"), allow(dead_code))]
    pub(crate) async fn commit_exclusive_handle_for_identity(
        &self,
        open_tx_id: OpenTransactionId,
        author: AuthorSubject,
    ) -> Result<TxId, Error> {
        let (published, unit) = self
            .node
            .node
            .lock()
            .await
            .commit_exclusive(open_tx_id, author, self.next_now_ms())
            .await?;
        self.finish_exclusive_publication(published, unit).await
    }

    async fn finish_exclusive_publication(
        &self,
        published: PublishedTransaction,
        unit: SyncMessage,
    ) -> Result<TxId, Error> {
        let tx_id = published.tx_id;
        if self.node.defer_local_persistence.get() {
            self.admit_deferred_local_publication(published, Some(unit))
                .await?;
        } else {
            self.finish_publication_outcome(PublicationOutcome::published((), published))
                .await?;
            self.node.queue_pending_upload(tx_id, Some(unit));
        }
        Ok(tx_id)
    }

    /// Abandon an owned exclusive transaction handle.
    pub fn abandon_exclusive_handle(&self, open_tx_id: OpenTransactionId) -> Result<(), Error> {
        self.abandon_transaction_handle(open_tx_id)
    }

    pub(crate) async fn open_exclusive_handle(&self, id: OpenTransactionId) -> Result<(), Error> {
        self.open_exclusive_handle_for_identity(id, self.identity.author)
            .await
    }

    async fn open_exclusive_handle_for_identity(
        &self,
        id: OpenTransactionId,
        author: AuthorSubject,
    ) -> Result<(), Error> {
        self.node
            .node
            .lock()
            .await
            .open_exclusive_for_identity(id, author)
            .await
            .map_err(Into::into)
    }
}
