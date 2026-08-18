/// Pollable, query-driven node construction over one resident working set.
///
/// Each attempt runs as an isolated resident-storage transaction. A missing
/// durable input discards that attempt, admits exactly the requested input, and
/// retries. The first successful transaction becomes the node's real resident
/// view, and metadata it created is durably committed before the node becomes
/// ready.
#[doc(hidden)]
pub struct PollableNodeOpen {
    node_uuid: NodeUuid,
    schema: JazzSchema,
    history_complete: bool,
    cache: groove::storage::DemandLoadedStorage,
    persistence: Option<Box<dyn groove::storage::async_ordered::OrderedKvStorage>>,
    acquisition: groove::db::StorageAcquisition,
    phase: NodeOpenPhase,
}

enum NodeOpenPhase {
    Acquiring,
    Finalizing {
        requests:
            std::collections::VecDeque<groove::storage::async_ordered::OwnedStorageRequest>,
        node: NodeState<groove::storage::DemandLoadedStorage>,
        cache: groove::storage::DemandLoadedStorage,
    },
    Complete,
}

/// Ready Jazz node together with the durable session that supplies cold inputs
/// and persists resident mutations.
///
/// This is the durable owner produced by [`PollableNodeOpen`]. Keeping these
/// resources together prevents a ready resident node from outliving the async
/// storage session whose ordering and cancellation state it depends on.
#[doc(hidden)]
pub struct DemandDrivenNode {
    node: std::rc::Rc<std::cell::RefCell<NodeState<groove::storage::DemandLoadedStorage>>>,
    cache: groove::storage::DemandLoadedStorage,
    persistence: Box<dyn groove::storage::async_ordered::OrderedKvStorage>,
    acquisition: groove::db::StorageAcquisition,
    pending_persistence:
        std::collections::VecDeque<groove::storage::async_ordered::OwnedStorageRequest>,
    pending_shutdown:
        Option<std::collections::VecDeque<groove::storage::async_ordered::OwnedStorageRequest>>,
    pending_fate: Option<PendingFateApplication>,
    pending_ingress: Option<PendingDurableIngress>,
    pending_peer_fate: Option<PendingPeerFate>,
    persistence_failed: bool,
}

struct PendingFateApplication {
    root: ingest::FateUpdateRequest,
    steps: std::collections::VecDeque<ingest::FateUpdateRequest>,
}

enum PendingDurableIngress {
    Authority {
        request: ingest::AuthorityCommitRequest,
        responses: Vec<SyncMessage>,
        publication: groove::db::DurablePublicationScope,
    },
    Relay {
        tx: Transaction,
        versions: Vec<VersionRecord>,
        publication: groove::db::DurablePublicationScope,
    },
}

struct PendingPeerFate {
    request: ingest::FateUpdateRequest,
    publication: groove::db::DurablePublicationScope,
    resident_done: bool,
}

impl DemandDrivenNode {
    /// Access the synchronously resident Jazz core.
    pub fn resident(
        &self,
    ) -> std::cell::Ref<'_, NodeState<groove::storage::DemandLoadedStorage>> {
        self.node.borrow()
    }

    pub(crate) fn shared_resident(
        &self,
    ) -> std::rc::Rc<std::cell::RefCell<NodeState<groove::storage::DemandLoadedStorage>>> {
        std::rc::Rc::clone(&self.node)
    }

    /// Acquire every cold input for a local mutation before synchronously
    /// publishing that mutation into the resident node and its IVM.
    ///
    /// `prepare` may read durable-backed node state and may therefore suspend;
    /// it must not publish application state or emit durable writes. `publish`
    /// runs exactly once only after preparation succeeds. A correct prepared
    /// operation cannot encounter another cold input while publishing.
    /// Immediate storage drives acquisition and publication in the first poll;
    /// a genuinely asynchronous backend returns `Pending` without invoking
    /// `publish`.
    fn poll_local_operation<P, T>(
        &mut self,
        context: &mut std::task::Context<'_>,
        mut prepare: impl FnMut(
            &mut NodeState<groove::storage::DemandLoadedStorage>,
        ) -> Result<P, Error>,
        publish: impl FnOnce(
            &mut NodeState<groove::storage::DemandLoadedStorage>,
            P,
        ) -> Result<T, Error>,
    ) -> std::task::Poll<Result<T, Error>> {
        if let Err(error) = self.ensure_persistence_usable() {
            return std::task::Poll::Ready(Err(error));
        }
        if let Err(error) = self.ensure_durable_publication_idle() {
            return std::task::Poll::Ready(Err(error));
        }
        let prepared = match self.acquisition.poll(
            self.persistence.as_mut(),
            &self.cache,
            context,
            || prepare(&mut self.node.borrow_mut()),
            missing_node_open_input,
        ) {
            std::task::Poll::Pending => return std::task::Poll::Pending,
            std::task::Poll::Ready(Err(error)) => {
                self.discard_failed_operation_writes();
                return std::task::Poll::Ready(Err(error));
            }
            std::task::Poll::Ready(Ok(prepared)) => prepared,
        };
        let result = publish(&mut self.node.borrow_mut(), prepared);
        match &result {
            Ok(_) => self.collect_persistence_unit(),
            Err(error) if is_not_resident(error) => {
                self.fail_persistence();
                return std::task::Poll::Ready(result);
            }
            Err(_) => self.discard_failed_operation_writes(),
        }
        std::task::Poll::Ready(result)
    }

    /// Commit one mergeable write through the native acquire-then-publish
    /// operation boundary.
    pub fn poll_mergeable_commit(
        &mut self,
        context: &mut std::task::Context<'_>,
        commit: &MergeableCommit,
    ) -> std::task::Poll<Result<TxId, Error>> {
        self.poll_local_operation(
            context,
            |node| node.prepare_current_mergeable_commit(commit.clone()),
            |node, prepared| node.publish_prepared_mergeable_commit(prepared),
        )
    }

    /// Commit one mergeable write authored against an explicit schema view.
    pub fn poll_mergeable_commit_in_schema(
        &mut self,
        context: &mut std::task::Context<'_>,
        schema: SchemaVersionId,
        commit: &MergeableCommit,
    ) -> std::task::Poll<Result<TxId, Error>> {
        self.poll_local_operation(
            context,
            |node| node.prepare_mergeable_commit_in_schema(schema, commit.clone()),
            |node, prepared| node.publish_prepared_mergeable_commit(prepared),
        )
    }

    /// Commit several mergeable writes as one transaction under an explicit
    /// authored schema. All durable inputs are acquired before the atomic
    /// resident publication.
    pub fn poll_mergeable_many_in_schema(
        &mut self,
        context: &mut std::task::Context<'_>,
        schema: SchemaVersionId,
        commits: &[MergeableCommit],
    ) -> std::task::Poll<Result<TxId, Error>> {
        self.poll_local_operation(
            context,
            |node| node.prepare_mergeable_many_in_schema(schema, commits.to_vec()),
            |node, prepared| node.publish_prepared_mergeable_commit(prepared),
        )
    }

    /// Commit a branch-local write together with any lazily-created physical
    /// partition through one acquire/publish operation.
    pub fn poll_mergeable_many_on_branch_in_schema(
        &mut self,
        context: &mut std::task::Context<'_>,
        branch_id: BranchId,
        schema: SchemaVersionId,
        commits: &[MergeableCommit],
    ) -> std::task::Poll<Result<TxId, Error>> {
        self.poll_local_operation(
            context,
            |node| {
                node.prepare_mergeable_many_on_branch_in_schema(
                    branch_id,
                    schema,
                    commits.to_vec(),
                )
            },
            |node, prepared| node.publish_prepared_branch_mergeable_commit(prepared),
        )
    }

    /// Commit a staged mergeable transaction without consuming its open handle
    /// until all cold parents and patch inputs have been acquired.
    pub fn poll_mergeable_open(
        &mut self,
        context: &mut std::task::Context<'_>,
        open_batch_id: OpenBatchId,
        fallback_now_ms: u64,
    ) -> std::task::Poll<Result<TxId, Error>> {
        self.poll_local_operation(
            context,
            |node| node.prepare_mergeable_open(open_batch_id, fallback_now_ms),
            |node, prepared| node.publish_prepared_mergeable_open(prepared),
        )
    }

    /// Validate and publish a staged exclusive transaction without consuming
    /// its handle or advancing the local clock during cold acquisition.
    pub fn poll_exclusive_open(
        &mut self,
        context: &mut std::task::Context<'_>,
        open_batch_id: OpenBatchId,
        made_by: AuthorId,
        now_ms: u64,
    ) -> std::task::Poll<Result<(TxId, SyncMessage), Error>> {
        self.poll_local_operation(
            context,
            |node| node.prepare_exclusive_commit(open_batch_id, made_by, now_ms),
            |node, prepared| node.publish_prepared_exclusive_commit(prepared),
        )
    }

    /// Create one local branch through the same acquire-then-publish boundary
    /// as application-row writes.
    pub fn poll_create_branch(
        &mut self,
        context: &mut std::task::Context<'_>,
        branch_id: BranchId,
        created_by: AuthorId,
    ) -> std::task::Poll<Result<BranchRecord, Error>> {
        self.poll_local_operation(
            context,
            |node| node.prepare_branch_creation(branch_id, created_by),
            |node, prepared| node.publish_branch_creation(prepared),
        )
    }

    /// Ingest one authority commit and release its fate only after the exact
    /// resident transition is durably committed.
    ///
    /// Preparation may suspend for cold validation inputs. The publishing
    /// poll advances the real runtime once, but a retained Groove publication
    /// scope quarantines subscription callbacks and all other operations are
    /// withheld until storage commits. Immediate storage completes all phases
    /// in this same call.
    pub fn poll_ingest_commit_unit(
        &mut self,
        context: &mut std::task::Context<'_>,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
        ingest_context: Option<CommitUnitIngestContext>,
    ) -> std::task::Poll<Result<Vec<SyncMessage>, Error>> {
        if let Err(error) = self.ensure_persistence_usable() {
            return std::task::Poll::Ready(Err(error));
        }
        let request = ingest::AuthorityCommitRequest {
            tx,
            versions,
            now_ms,
            ingest_context,
        };
        match &self.pending_ingress {
            Some(PendingDurableIngress::Authority { request: pending, .. })
                if pending != &request =>
            {
                return std::task::Poll::Ready(Err(Error::InvalidStoredValue(
                    "a different authority commit was polled while one is pending",
                )));
            }
            Some(PendingDurableIngress::Relay { .. }) => {
                return std::task::Poll::Ready(Err(Error::InvalidStoredValue(
                    "a relay commit is awaiting durable completion",
                )));
            }
            None => {
                if let Err(error) = self.ensure_durable_publication_idle() {
                    return std::task::Poll::Ready(Err(error));
                }
                let prepared = match self.acquisition.poll(
                    self.persistence.as_mut(),
                    &self.cache,
                    context,
                    || {
                        self.node.borrow_mut().prepare_authority_commit(
                            request.tx.clone(),
                            request.versions.clone(),
                            request.now_ms,
                            request.ingest_context,
                        )
                    },
                    missing_node_open_input,
                ) {
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                    std::task::Poll::Ready(Err(error)) => {
                        self.discard_failed_operation_writes();
                        return std::task::Poll::Ready(Err(error));
                    }
                    std::task::Poll::Ready(Ok(prepared)) => prepared,
                };
                let publication = match self
                    .node
                    .borrow_mut()
                    .database
                    .begin_durable_publication_scope()
                {
                    Ok(publication) => publication,
                    Err(error) => return std::task::Poll::Ready(Err(error.into())),
                };
                let publish_result = {
                    self.node
                        .borrow_mut()
                        .publish_prepared_authority_commit(prepared)
                };
                let responses = match publish_result {
                    Ok(responses) => responses,
                    Err(error) => {
                        publication.abort(&mut self.node.borrow_mut().database);
                        if is_not_resident(&error) {
                            self.fail_persistence();
                        } else {
                            self.discard_failed_operation_writes();
                        }
                        return std::task::Poll::Ready(Err(error));
                    }
                };
                self.collect_persistence_unit();
                self.pending_ingress = Some(PendingDurableIngress::Authority {
                    request: request.clone(),
                    responses,
                    publication,
                });
            }
            Some(_) => {}
        }
        match self.poll_persistence_queue(context) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(Ok(())) => {
                let pending = self
                    .pending_ingress
                    .take()
                    .expect("completed authority commit retains publication state");
                let PendingDurableIngress::Authority {
                    responses,
                    publication,
                    ..
                } = pending
                else {
                    unreachable!("authority poll retains authority ingress")
                };
                publication.finish(&mut self.node.borrow_mut().database);
                std::task::Poll::Ready(Ok(responses))
            }
            std::task::Poll::Ready(Err(error)) => {
                let pending = self
                    .pending_ingress
                    .take()
                    .expect("failed authority commit retains publication state");
                let PendingDurableIngress::Authority { publication, .. } = pending else {
                    unreachable!("authority poll retains authority ingress")
                };
                publication.abort(&mut self.node.borrow_mut().database);
                std::task::Poll::Ready(Err(error))
            }
        }
    }

    /// Ingest one unfated commit at a Local relay through the same typed
    /// acquire-then-publish boundary as local writes and authority ingress.
    pub fn poll_ingest_relay_commit_unit(
        &mut self,
        context: &mut std::task::Context<'_>,
        tx: Transaction,
        versions: Vec<VersionRecord>,
    ) -> std::task::Poll<Result<(), Error>> {
        if let Err(error) = self.ensure_persistence_usable() {
            return std::task::Poll::Ready(Err(error));
        }
        let versions = canonical_versions(versions);
        match &self.pending_ingress {
            Some(PendingDurableIngress::Relay {
                tx: pending_tx,
                versions: pending_versions,
                ..
            }) if pending_tx != &tx || pending_versions != &versions => {
                return std::task::Poll::Ready(Err(Error::InvalidStoredValue(
                    "a different relay commit was polled while one is pending",
                )));
            }
            Some(PendingDurableIngress::Authority { .. }) => {
                return std::task::Poll::Ready(Err(Error::InvalidStoredValue(
                    "an authority commit is awaiting durable completion",
                )));
            }
            None => {
                if let Err(error) = self.ensure_durable_publication_idle() {
                    return std::task::Poll::Ready(Err(error));
                }
                let prepared = match self.acquisition.poll(
                    self.persistence.as_mut(),
                    &self.cache,
                    context,
                    || {
                        self.node
                            .borrow_mut()
                            .prepare_relay_commit(tx.clone(), versions.clone())
                    },
                    missing_node_open_input,
                ) {
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                    std::task::Poll::Ready(Err(error)) => {
                        self.discard_failed_operation_writes();
                        return std::task::Poll::Ready(Err(error));
                    }
                    std::task::Poll::Ready(Ok(prepared)) => prepared,
                };
                let publication = match self
                    .node
                    .borrow_mut()
                    .database
                    .begin_durable_publication_scope()
                {
                    Ok(publication) => publication,
                    Err(error) => return std::task::Poll::Ready(Err(error.into())),
                };
                let publish_result = {
                    self.node
                        .borrow_mut()
                        .publish_prepared_relay_commit(prepared)
                };
                if let Err(error) = publish_result {
                    publication.abort(&mut self.node.borrow_mut().database);
                    if is_not_resident(&error) {
                        self.fail_persistence();
                    } else {
                        self.discard_failed_operation_writes();
                    }
                    return std::task::Poll::Ready(Err(error));
                }
                self.collect_persistence_unit();
                self.pending_ingress = Some(PendingDurableIngress::Relay {
                    tx: tx.clone(),
                    versions: versions.clone(),
                    publication,
                });
            }
            Some(_) => {}
        }
        match self.poll_persistence_queue(context) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(Ok(())) => {
                let pending = self
                    .pending_ingress
                    .take()
                    .expect("completed relay commit retains publication state");
                let PendingDurableIngress::Relay { publication, .. } = pending else {
                    unreachable!("relay poll retains relay ingress")
                };
                publication.finish(&mut self.node.borrow_mut().database);
                std::task::Poll::Ready(Ok(()))
            }
            std::task::Poll::Ready(Err(error)) => {
                let pending = self
                    .pending_ingress
                    .take()
                    .expect("failed relay commit retains publication state");
                let PendingDurableIngress::Relay { publication, .. } = pending else {
                    unreachable!("relay poll retains relay ingress")
                };
                publication.abort(&mut self.node.borrow_mut().database);
                std::task::Poll::Ready(Err(error))
            }
        }
    }

    /// Apply one authority fate, including any causally rejected descendants.
    ///
    /// Each step acquires its cold inputs before publication. Cascade steps are
    /// retained across polls and prepared only after their parent publishes,
    /// so current-row and rejection indexes are never planned against stale
    /// resident state.
    pub fn poll_apply_fate_update(
        &mut self,
        context: &mut std::task::Context<'_>,
        tx_id: TxId,
        fate: Fate,
        global_seq: Option<GlobalSeq>,
        durability: Option<DurabilityTier>,
    ) -> std::task::Poll<Result<(), Error>> {
        self.poll_apply_fate_update_inner(
            context,
            tx_id,
            fate,
            global_seq,
            durability,
            false,
        )
    }

    fn poll_apply_fate_update_inner(
        &mut self,
        context: &mut std::task::Context<'_>,
        tx_id: TxId,
        fate: Fate,
        global_seq: Option<GlobalSeq>,
        durability: Option<DurabilityTier>,
        peer_scope_active: bool,
    ) -> std::task::Poll<Result<(), Error>> {
        if let Err(error) = self.ensure_persistence_usable() {
            return std::task::Poll::Ready(Err(error));
        }
        if !peer_scope_active
            && let Err(error) = self.ensure_durable_publication_idle()
        {
            return std::task::Poll::Ready(Err(error));
        }
        let root = ingest::FateUpdateRequest {
            tx_id,
            fate,
            global_seq,
            durability,
        };
        match &self.pending_fate {
            Some(pending) if pending.root != root => {
                return std::task::Poll::Ready(Err(Error::InvalidStoredValue(
                    "a different fate update was polled while one is pending",
                )));
            }
            None => {
                self.pending_fate = Some(PendingFateApplication {
                    root: root.clone(),
                    steps: std::collections::VecDeque::from([root]),
                });
            }
            Some(_) => {}
        }
        loop {
            let request = self
                .pending_fate
                .as_ref()
                .and_then(|pending| pending.steps.front())
                .cloned()
                .expect("an active fate application retains a step");
            let prepared = match self.acquisition.poll(
                self.persistence.as_mut(),
                &self.cache,
                context,
                || self.node.borrow_mut().prepare_fate_update(request.clone()),
                missing_node_open_input,
            ) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(Err(error)) => {
                    self.pending_fate = None;
                    self.discard_failed_operation_writes();
                    return std::task::Poll::Ready(Err(error));
                }
                std::task::Poll::Ready(Ok(prepared)) => prepared,
            };
            let publish_result = {
                self.node
                    .borrow_mut()
                    .publish_prepared_fate_update(prepared)
            };
            match publish_result {
                Ok(cascades) => {
                    self.collect_persistence_unit();
                    let pending = self
                        .pending_fate
                        .as_mut()
                        .expect("published fate retains its operation");
                    pending.steps.pop_front();
                    pending.steps.extend(cascades);
                    if pending.steps.is_empty() {
                        self.pending_fate = None;
                        return std::task::Poll::Ready(Ok(()));
                    }
                }
                Err(error) => {
                    self.pending_fate = None;
                    if is_not_resident(&error) {
                        self.fail_persistence();
                    } else {
                        self.discard_failed_operation_writes();
                    }
                    return std::task::Poll::Ready(Err(error));
                }
            }
        }
    }

    /// Apply a fate received from a peer while retaining the frame's external
    /// publication boundary until every resident cascade unit is durable.
    pub fn poll_apply_peer_fate_update(
        &mut self,
        context: &mut std::task::Context<'_>,
        tx_id: TxId,
        fate: Fate,
        global_seq: Option<GlobalSeq>,
        durability: Option<DurabilityTier>,
    ) -> std::task::Poll<Result<(), Error>> {
        let request = ingest::FateUpdateRequest {
            tx_id,
            fate,
            global_seq,
            durability,
        };
        match &self.pending_peer_fate {
            Some(pending) if pending.request != request => {
                return std::task::Poll::Ready(Err(Error::InvalidStoredValue(
                    "a different peer fate was polled while one is pending",
                )));
            }
            None => {
                if let Err(error) = self.ensure_durable_publication_idle() {
                    return std::task::Poll::Ready(Err(error));
                }
                let publication = match self
                    .node
                    .borrow_mut()
                    .database
                    .begin_durable_publication_scope()
                {
                    Ok(publication) => publication,
                    Err(error) => return std::task::Poll::Ready(Err(error.into())),
                };
                self.pending_peer_fate = Some(PendingPeerFate {
                    request: request.clone(),
                    publication,
                    resident_done: false,
                });
            }
            Some(_) => {}
        }

        if !self
            .pending_peer_fate
            .as_ref()
            .expect("peer fate retains state")
            .resident_done
        {
            match self.poll_apply_fate_update_inner(
                context,
                request.tx_id,
                request.fate.clone(),
                request.global_seq,
                request.durability,
                true,
            ) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(Err(error)) => {
                    let pending = self
                        .pending_peer_fate
                        .take()
                        .expect("failed peer fate retains publication state");
                    pending
                        .publication
                        .abort(&mut self.node.borrow_mut().database);
                    return std::task::Poll::Ready(Err(error));
                }
                std::task::Poll::Ready(Ok(())) => {
                    self.pending_peer_fate
                        .as_mut()
                        .expect("published peer fate retains state")
                        .resident_done = true;
                }
            }
        }

        match self.poll_persistence_queue(context) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(Ok(())) => {
                let pending = self
                    .pending_peer_fate
                    .take()
                    .expect("durable peer fate retains publication state");
                pending
                    .publication
                    .finish(&mut self.node.borrow_mut().database);
                std::task::Poll::Ready(Ok(()))
            }
            std::task::Poll::Ready(Err(error)) => {
                let pending = self
                    .pending_peer_fate
                    .take()
                    .expect("failed peer fate retains publication state");
                pending
                    .publication
                    .abort(&mut self.node.borrow_mut().database);
                std::task::Poll::Ready(Err(error))
            }
        }
    }

    /// Poll a restartable query or subscription operation. It may suspend only
    /// while acquiring a missing durable input; once ready, evaluation runs on
    /// the same resident node used by local writes.
    pub(crate) fn poll_operation<T, E>(
        &mut self,
        context: &mut std::task::Context<'_>,
        operation: impl FnMut() -> Result<T, E>,
        missing_input: impl Fn(E) -> Result<
            groove::storage::async_ordered::OwnedStorageOperation,
            E,
        >,
    ) -> std::task::Poll<Result<T, E>>
    where
        E: From<Error> + From<groove::storage::Error>,
    {
        if let Err(error) = self.ensure_persistence_usable() {
            return std::task::Poll::Ready(Err(error.into()));
        }
        if let Err(error) = self.ensure_durable_publication_idle() {
            return std::task::Poll::Ready(Err(error.into()));
        }
        let result = self.acquisition.poll(
            self.persistence.as_mut(),
            &self.cache,
            context,
            operation,
            missing_input,
        );
        match &result {
            std::task::Poll::Ready(Ok(_)) => self.collect_persistence_unit(),
            std::task::Poll::Ready(Err(_)) => self.discard_failed_operation_writes(),
            std::task::Poll::Pending => {}
        }
        result
    }

    pub(crate) fn poll_resident_operation<T>(
        &mut self,
        context: &mut std::task::Context<'_>,
        operation: impl FnMut() -> Result<T, Error>,
    ) -> std::task::Poll<Result<T, Error>> {
        self.poll_operation(context, operation, missing_node_open_input)
    }

    fn poll_query<T>(
        &mut self,
        context: &mut std::task::Context<'_>,
        mut operation: impl FnMut(
            &mut NodeState<groove::storage::DemandLoadedStorage>,
        ) -> Result<T, Error>,
    ) -> std::task::Poll<Result<T, Error>> {
        let node = std::rc::Rc::clone(&self.node);
        self.poll_resident_operation(context, || operation(&mut node.borrow_mut()))
    }

    /// Poll one current-table read without exposing the mutable resident node
    /// across the asynchronous storage boundary.
    pub fn poll_current_rows(
        &mut self,
        context: &mut std::task::Context<'_>,
        table: &str,
        tier: DurabilityTier,
    ) -> std::task::Poll<Result<Vec<CurrentRow>, Error>> {
        self.poll_query(context, |node| node.current_rows(table, tier))
    }

    /// Poll one immutable row-history read through the same acquisition owner.
    pub fn poll_row_history(
        &mut self,
        context: &mut std::task::Context<'_>,
        table: &str,
        row: RowUuid,
    ) -> std::task::Poll<Result<Vec<HistoryEntry>, Error>> {
        self.poll_query(context, |node| node.row_history(table, row))
    }

    pub(crate) fn poll_pending_transaction_ids(
        &mut self,
        context: &mut std::task::Context<'_>,
        node_uuid: NodeUuid,
        author: AuthorId,
    ) -> std::task::Poll<Result<Vec<TxId>, Error>> {
        self.poll_query(context, |node| {
            node.pending_transaction_ids_for(node_uuid, author)
        })
    }

    /// Poll a history subscription opening. Once returned, later prepared
    /// local writes enqueue their callback delta in the same resident publish
    /// stack that makes one-shot reads visible.
    pub fn poll_subscribe_history(
        &mut self,
        context: &mut std::task::Context<'_>,
        table: &str,
    ) -> std::task::Poll<Result<Subscription, Error>> {
        self.poll_query(context, |node| node.subscribe_history(table))
    }

    /// Poll the oldest durable batch. Later batches cannot overtake it.
    pub fn poll_persistence(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        if self.pending_ingress.is_some() || self.pending_peer_fate.is_some() {
            return std::task::Poll::Ready(Err(Error::InvalidStoredValue(
                "ingress persistence must be polled through its typed operation",
            )));
        }
        self.poll_persistence_queue(context)
    }

    /// Drain every resident journal, flush the ordered durable boundary, and
    /// close the backend session in order. This is terminal for the owner.
    pub fn poll_close(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        if self.pending_shutdown.is_none() {
            match self.poll_persistence(context) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(Err(error)) => {
                    return std::task::Poll::Ready(Err(error));
                }
                std::task::Poll::Ready(Ok(())) => {}
            }
            self.pending_shutdown = Some(std::collections::VecDeque::from([
                groove::storage::async_ordered::OwnedStorageRequest::new(
                    groove::storage::async_ordered::OwnedStorageOperation::Flush,
                ),
                groove::storage::async_ordered::OwnedStorageRequest::new(
                    groove::storage::async_ordered::OwnedStorageOperation::Close,
                ),
            ]));
        }
        loop {
            let Some(request) = self
                .pending_shutdown
                .as_ref()
                .and_then(|requests| requests.front())
            else {
                return std::task::Poll::Ready(Ok(()));
            };
            match self.persistence.poll_request(request, context) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(Ok(response))
                    if storage_response_matches(request.operation(), &response) =>
                {
                    self.pending_shutdown
                        .as_mut()
                        .expect("active shutdown retains its requests")
                        .pop_front();
                }
                std::task::Poll::Ready(Ok(response)) => {
                    self.fail_persistence();
                    return std::task::Poll::Ready(Err(Error::Storage(
                        groove::storage::Error::Backend {
                            backend: "demand-driven-node",
                            message: format!(
                                "node shutdown returned unexpected response {response:?}"
                            ),
                        },
                    )));
                }
                std::task::Poll::Ready(Err(error)) => {
                    self.fail_persistence();
                    return std::task::Poll::Ready(Err(error.into()));
                }
            }
        }
    }

    fn poll_persistence_queue(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        if let Err(error) = self.ensure_persistence_usable() {
            return std::task::Poll::Ready(Err(error));
        }
        self.collect_persistence_unit();
        loop {
            let Some(request) = self.pending_persistence.front() else {
                return std::task::Poll::Ready(Ok(()));
            };
            match self.persistence.poll_request(request, context) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(Ok(response))
                    if storage_response_matches(request.operation(), &response) =>
                {
                    self.pending_persistence.pop_front();
                }
                std::task::Poll::Ready(Ok(response)) => {
                    let error = Error::Storage(groove::storage::Error::Backend {
                        backend: "demand-driven-node",
                        message: format!("node commit returned unexpected response {response:?}"),
                    });
                    self.fail_persistence();
                    return std::task::Poll::Ready(Err(error));
                }
                std::task::Poll::Ready(Err(error)) => {
                    self.fail_persistence();
                    return std::task::Poll::Ready(Err(error.into()));
                }
            }
        }
    }

    fn collect_persistence_unit(&mut self) {
        let column_families = self
            .node
            .borrow()
            .database
            .take_demand_loaded_pending_column_families();
        if !column_families.is_empty() {
            self.pending_persistence.push_back(
                groove::storage::async_ordered::OwnedStorageRequest::new(
                    groove::storage::async_ordered::OwnedStorageOperation::EnsureColumnFamilies(
                        column_families,
                    ),
                ),
            );
        }
        let operations = self
            .node
            .borrow()
            .database
            .take_demand_loaded_pending_writes();
        if !operations.is_empty() {
            self.pending_persistence.push_back(
                groove::storage::async_ordered::OwnedStorageRequest::new(
                    groove::storage::async_ordered::OwnedStorageOperation::Commit(operations),
                ),
            );
        }
    }

    fn discard_failed_operation_writes(&mut self) {
        let node = self.node.borrow();
        let writes = node.database.take_demand_loaded_pending_writes();
        let column_families = node
            .database
            .take_demand_loaded_pending_column_families();
        drop(node);
        if !writes.is_empty() || !column_families.is_empty() {
            // An operation that emitted a durable batch before failing has an
            // ambiguous resident outcome. Publish none of it and prevent this
            // runtime from accepting dependent work until a clean reopen.
            self.fail_persistence();
        }
    }

    fn ensure_persistence_usable(&self) -> Result<(), Error> {
        if self.persistence_failed {
            Err(Error::Groove(groove::db::Error::DatabasePoisoned))
        } else {
            Ok(())
        }
    }

    fn ensure_durable_publication_idle(&self) -> Result<(), Error> {
        if self.pending_ingress.is_some() || self.pending_peer_fate.is_some() {
            Err(Error::InvalidStoredValue(
                "a durable publication is awaiting completion",
            ))
        } else {
            Ok(())
        }
    }

    fn fail_persistence(&mut self) {
        self.persistence_failed = true;
        for request in self.pending_persistence.drain(..) {
            let _ = self.persistence.cancel_request(request.id());
        }
    }
}

fn storage_response_matches(
    operation: &groove::storage::async_ordered::OwnedStorageOperation,
    response: &groove::storage::async_ordered::OwnedStorageResponse,
) -> bool {
    matches!(
        (operation, response),
        (
            groove::storage::async_ordered::OwnedStorageOperation::EnsureColumnFamilies(_),
            groove::storage::async_ordered::OwnedStorageResponse::ColumnFamiliesReady
        ) | (
            groove::storage::async_ordered::OwnedStorageOperation::Commit(_),
            groove::storage::async_ordered::OwnedStorageResponse::Committed
        ) | (
            groove::storage::async_ordered::OwnedStorageOperation::Flush,
            groove::storage::async_ordered::OwnedStorageResponse::Flushed
        ) | (
            groove::storage::async_ordered::OwnedStorageOperation::Close,
            groove::storage::async_ordered::OwnedStorageResponse::Closed
        )
    )
}

impl Drop for DemandDrivenNode {
    fn drop(&mut self) {
        let _ = self.acquisition.cancel(self.persistence.as_mut());
        for request in self.pending_persistence.drain(..) {
            let _ = self.persistence.cancel_request(request.id());
        }
        if let Some(requests) = self.pending_shutdown.as_mut() {
            for request in requests.drain(..) {
                let _ = self.persistence.cancel_request(request.id());
            }
        }
    }
}

impl PollableNodeOpen {
    fn finish(
        &mut self,
        mut node: NodeState<groove::storage::DemandLoadedStorage>,
        cache: groove::storage::DemandLoadedStorage,
    ) -> DemandDrivenNode {
        // Durable completion belongs to this wrapper, not the synchronous
        // NodeState call stack. Authored rows therefore enter the resident
        // view at None and gain stronger durability only through the normal
        // acknowledged sync/fate path.
        node.set_non_durable_client();
        DemandDrivenNode {
            node: std::rc::Rc::new(std::cell::RefCell::new(node)),
            cache,
            persistence: self
                .persistence
                .take()
                .expect("ready node takes its persistence session"),
            acquisition: groove::db::StorageAcquisition::default(),
            pending_persistence: std::collections::VecDeque::new(),
            pending_shutdown: None,
            pending_fate: None,
            pending_ingress: None,
            pending_peer_fate: None,
            persistence_failed: false,
        }
    }

    #[doc(hidden)]
    pub fn new(
        node_uuid: NodeUuid,
        schema: JazzSchema,
        persistence: Box<dyn groove::storage::async_ordered::OrderedKvStorage>,
    ) -> Self {
        Self::with_history_complete(node_uuid, schema, persistence, false)
    }

    #[doc(hidden)]
    pub fn new_history_complete(
        node_uuid: NodeUuid,
        schema: JazzSchema,
        persistence: Box<dyn groove::storage::async_ordered::OrderedKvStorage>,
    ) -> Self {
        Self::with_history_complete(node_uuid, schema, persistence, true)
    }

    fn with_history_complete(
        node_uuid: NodeUuid,
        schema: JazzSchema,
        persistence: Box<dyn groove::storage::async_ordered::OrderedKvStorage>,
        history_complete: bool,
    ) -> Self {
        let column_families = schema.column_families();
        let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
        Self {
            node_uuid,
            schema,
            history_complete,
            cache: groove::storage::DemandLoadedStorage::new(&refs),
            persistence: Some(persistence),
            acquisition: groove::db::StorageAcquisition::default(),
            phase: NodeOpenPhase::Acquiring,
        }
    }

    #[doc(hidden)]
    pub fn poll(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<DemandDrivenNode, Error>> {
        loop {
            if let NodeOpenPhase::Finalizing { requests, .. } = &self.phase {
                let request = requests
                    .front()
                    .expect("finalizing node opening retains at least one request");
                match self
                    .persistence
                    .as_mut()
                    .expect("active node opening owns persistence")
                    .poll_request(request, context)
                {
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                    std::task::Poll::Ready(Ok(response))
                        if storage_response_matches(request.operation(), &response) =>
                    {
                        let NodeOpenPhase::Finalizing { requests, .. } = &mut self.phase else {
                            unreachable!("finalizing phase was just matched")
                        };
                        requests.pop_front();
                        if requests.is_empty() {
                            let NodeOpenPhase::Finalizing { node, cache, .. } = std::mem::replace(
                                &mut self.phase,
                                NodeOpenPhase::Complete,
                            ) else {
                                unreachable!("finalizing phase was just matched")
                            };
                            return std::task::Poll::Ready(Ok(self.finish(node, cache)));
                        }
                        continue;
                    }
                    std::task::Poll::Ready(Ok(response)) => {
                        self.phase = NodeOpenPhase::Complete;
                        return std::task::Poll::Ready(Err(Error::Storage(
                            groove::storage::Error::Backend {
                                backend: "pollable-node-open",
                                message: format!(
                                    "node-open commit returned unexpected response {response:?}"
                                ),
                            },
                        )));
                    }
                    std::task::Poll::Ready(Err(error)) => {
                        self.phase = NodeOpenPhase::Complete;
                        return std::task::Poll::Ready(Err(error.into()));
                    }
                }
            }
            assert!(
                matches!(self.phase, NodeOpenPhase::Acquiring),
                "completed node opening cannot be polled again"
            );
            let node_uuid = self.node_uuid;
            let schema = self.schema.clone();
            let history_complete = self.history_complete;
            match self.acquisition.poll(
                self.persistence
                    .as_mut()
                    .expect("active node opening owns persistence")
                    .as_mut(),
                &self.cache,
                context,
                || {
                    let transaction = self.cache.begin_transaction()?;
                    let node = construct_pollable_node(
                        node_uuid,
                        schema.clone(),
                        history_complete,
                        transaction.clone(),
                    )?;
                    Ok((node, transaction))
                },
                missing_node_open_input,
            ) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(Ok((node, transaction))) => {
                    let column_families = node
                        .database
                        .take_demand_loaded_pending_column_families();
                    let writes = node.database.take_demand_loaded_pending_writes();
                    let mut requests = std::collections::VecDeque::new();
                    if !column_families.is_empty() {
                        requests.push_back(
                            groove::storage::async_ordered::OwnedStorageRequest::new(
                                groove::storage::async_ordered::OwnedStorageOperation::EnsureColumnFamilies(
                                    column_families,
                                ),
                            ),
                        );
                    }
                    if !writes.is_empty() {
                        requests.push_back(
                            groove::storage::async_ordered::OwnedStorageRequest::new(
                                groove::storage::async_ordered::OwnedStorageOperation::Commit(
                                    writes,
                                ),
                            ),
                        );
                    }
                    if requests.is_empty() {
                        self.phase = NodeOpenPhase::Complete;
                        return std::task::Poll::Ready(Ok(self.finish(node, transaction)));
                    }
                    self.phase = NodeOpenPhase::Finalizing {
                        requests,
                        node,
                        cache: transaction,
                    };
                }
                std::task::Poll::Ready(Err(error)) => {
                    self.phase = NodeOpenPhase::Complete;
                    return std::task::Poll::Ready(Err(error));
                }
            }
        }
    }
}

impl Drop for PollableNodeOpen {
    fn drop(&mut self) {
        let Some(persistence) = self.persistence.as_mut() else {
            return;
        };
        let _ = self.acquisition.cancel(persistence.as_mut());
        if let NodeOpenPhase::Finalizing { requests, .. } = &self.phase {
            for request in requests {
                let _ = persistence.cancel_request(request.id());
            }
        }
    }
}

fn construct_pollable_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
    history_complete: bool,
    cache: groove::storage::DemandLoadedStorage,
) -> Result<NodeState<groove::storage::DemandLoadedStorage>, Error> {
    if history_complete {
        NodeState::new_history_complete(node_uuid, schema, cache)
    } else {
        NodeState::new(node_uuid, schema, cache)
    }
}

pub(crate) fn missing_node_open_input(
    error: Error,
) -> Result<groove::storage::async_ordered::OwnedStorageOperation, Error> {
    match error {
        Error::Storage(groove::storage::Error::NotResident { request }) => Ok(*request),
        Error::Groove(groove::db::Error::Storage(error)) => match *error {
            groove::storage::Error::NotResident { request } => Ok(*request),
            error => Err(Error::Storage(error)),
        },
        Error::Groove(groove::db::Error::IvmRuntime(
            groove::ivm::IvmRuntimeError::Storage(groove::storage::Error::NotResident { request }),
        )) => Ok(*request),
        error => Err(error),
    }
}

fn is_not_resident(error: &Error) -> bool {
    matches!(error, Error::Storage(groove::storage::Error::NotResident { .. }))
        || matches!(
            error,
            Error::Groove(groove::db::Error::Storage(error))
                if matches!(error.as_ref(), groove::storage::Error::NotResident { .. })
        )
        || matches!(
            error,
            Error::Groove(groove::db::Error::IvmRuntime(
                groove::ivm::IvmRuntimeError::Storage(
                    groove::storage::Error::NotResident { .. }
                )
            ))
        )
}
