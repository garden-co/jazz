//! Node-owned orchestration for upstream and subscriber connections.
//!
//! The node runtime owns shared connection state, scheduling, pending uploads,
//! subscription refresh, write-state notification, and connection lifecycle.

use super::peer_connection::{
    ConnectionLink, PeerConnection, mutation_error_event, take_pending_mutation_error_delivery,
};
use super::*;

/// Node-owned participant surface for upstream and subscriber connections.
pub struct Node<S>
where
    S: OrderedKvStorage,
{
    pub(super) node: SharedNodeState<S>,
    pub(super) subscriptions: SubscriptionList,
    pub(super) outbox: Outbox,
    pub(super) upstream_subscriptions: PendingUpstreamCommands,
    pub(super) latest_coverage_subscriptions: LatestCoverageSubscriptions,
    pub(super) upstream_coverage_refcounts: UpstreamCoverageRefCounts,
    pub(super) awaiting_initial_authority_coverage: AwaitingInitialAuthorityCoverage,
    pub(super) active_authority_view_receipts: ActiveAuthorityViewReceipts,
    pub(super) coverage_refresh_generations: CoverageRefreshGenerations,
    pub(super) query_coverage_registrations: QueryCoverageRegistrations,
    pub(super) upstream_subscription_owners: UpstreamSubscriptionOwners,
    pub(super) connections: RefCell<Vec<Rc<LocalMutex<PeerConnection<S>>>>>,
    pub(super) scheduler: SharedTickScheduler,
    pub(super) write_state_waiters: WriteStateWaiters,
    pub(super) permission_advice_waiters: PermissionAdviceWaiters,
    pub(super) edge_fate_routes: EdgeFateRoutes,
    pub(super) local_fate_routes: LocalFateRoutes,
    pub(super) admitted_upstream_authorities: AdmittedUpstreamAuthorities,
    pub(super) admitted_upstream_authority: Rc<RefCell<Option<AuthorityContext>>>,
    pub(super) mutation_errors: SharedMutationErrors,
    pub(super) next_write_state_waiter_id: Cell<u64>,
    pub(super) next_subscription_nonce: Cell<u64>,
    pub(super) subscriber_dirty_epoch: Rc<Cell<u64>>,
    pub(super) edge_cache_budget: Cell<Option<EdgeCacheBudget>>,
    pub(super) upstream_durability_floor: Cell<DurabilityTier>,
}

impl<S> Node<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Wrap a node for serving subscriber links.
    pub fn new(node: NodeState<S>) -> Self {
        let pending_mutation_errors = node
            .rejected_transactions()
            .into_iter()
            .filter_map(|tx_id| {
                node.rejected_transaction(tx_id)
                    .map(|rejected| (tx_id, mutation_error_event(rejected)))
            })
            .collect();
        Self {
            node: Rc::new(futures::lock::Mutex::new(node)),
            subscriptions: Rc::new(RefCell::new(Vec::new())),
            outbox: Rc::new(RefCell::new(Vec::new())),
            upstream_subscriptions: Rc::new(RefCell::new(Vec::new())),
            latest_coverage_subscriptions: Rc::new(RefCell::new(BTreeMap::new())),
            upstream_coverage_refcounts: Rc::new(RefCell::new(BTreeMap::new())),
            awaiting_initial_authority_coverage: Rc::new(RefCell::new(BTreeSet::new())),
            active_authority_view_receipts: Rc::new(RefCell::new(None)),
            coverage_refresh_generations: Rc::new(RefCell::new(BTreeMap::new())),
            query_coverage_registrations: Rc::new(RefCell::new(BTreeMap::new())),
            upstream_subscription_owners: Rc::new(RefCell::new(BTreeMap::new())),
            connections: RefCell::new(Vec::new()),
            scheduler: Rc::new(RefCell::new(None)),
            write_state_waiters: Rc::new(RefCell::new(BTreeMap::new())),
            mutation_errors: Rc::new(RefCell::new(MutationErrorState {
                callback: None,
                pending: pending_mutation_errors,
            })),
            next_write_state_waiter_id: Cell::new(1),
            next_subscription_nonce: Cell::new(1),
            permission_advice_waiters: Rc::new(RefCell::new(BTreeMap::new())),
            edge_fate_routes: Rc::new(RefCell::new(BTreeMap::new())),
            local_fate_routes: Rc::new(RefCell::new(BTreeMap::new())),
            admitted_upstream_authorities: Rc::new(RefCell::new(Vec::new())),
            admitted_upstream_authority: Rc::new(RefCell::new(None)),
            subscriber_dirty_epoch: Rc::new(Cell::new(0)),
            edge_cache_budget: Cell::new(None),
            upstream_durability_floor: Cell::new(DurabilityTier::Global),
        }
    }

    pub(super) fn upstream_register_shape_options(
        &self,
        tier: DurabilityTier,
        read_view: ReadViewSpec,
        propagate_upstream: bool,
    ) -> RegisterShapeOptions {
        upstream_register_shape_options(
            tier,
            read_view,
            self.upstream_durability_floor.get(),
            propagate_upstream,
        )
    }

    /// Ordinary `Db::open` nodes are Local receivers. Only the structurally
    /// separate history-complete path acts as the Core fate authority.
    fn receives_commits_as_local(&self) -> bool {
        !self.node.borrow().is_history_complete()
    }

    /// Borrow the served node.
    pub fn node(&self) -> SharedNodeState<S> {
        Rc::clone(&self.node)
    }

    pub(super) fn set_non_durable_client(&self) {
        self.node.borrow_mut().set_non_durable_client();
        self.upstream_durability_floor.set(DurabilityTier::Local);
    }

    /// Change whether subscriber links may serve their registered views.
    /// Publishing a permissions head always rehydrates every live view, so a
    /// tighter head retracts rows without requiring a reconnect.
    pub fn set_permissions_ready(&self, ready: bool) -> Result<(), Error> {
        self.node.borrow_mut().set_permissions_ready(ready);
        if ready {
            for connection in self.connections.borrow().iter() {
                connection.borrow_mut().rehydrate_subscriber_views()?;
            }
        }
        Ok(())
    }

    pub(super) fn queue_pending_upload(&self, tx_id: TxId, unit: Option<SyncMessage>) {
        let mut outbox = self.outbox.borrow_mut();
        if outbox.iter().any(|pending| pending.tx_id == tx_id) {
            return;
        }
        outbox.push(PendingUpload { tx_id, unit });
        drop(outbox);
        self.mark_subscriber_connections_dirty();
        self.schedule_tick(TickUrgency::Deferred);
    }

    /// Restore locally originated, unsettled durable writes into the
    /// process-local upload queue after reopening client storage.
    pub(super) fn restore_pending_uploads(&self, identity: DbIdentity) -> Result<(), Error> {
        let mut node = self.node.borrow_mut();
        let pending = node.pending_transaction_ids_for(identity.node, identity.author);
        let pending = crate::db::block_on(pending)?;
        drop(node);
        let mut restored = HashSet::new();
        for tx_id in pending {
            if restored.insert(tx_id) {
                self.queue_pending_upload(tx_id, None);
            }
        }
        Ok(())
    }

    fn restore_local_subscriber(
        &self,
        author: AuthorId,
        downstream_fates: &PendingDownstreamFates,
    ) -> Result<(), Error> {
        let mut node = self.node.borrow_mut();
        let pending = node.pending_transaction_ids_for_author(author);
        let pending = crate::db::block_on(pending)?;
        drop(node);
        let pending_set = pending.iter().copied().collect::<BTreeSet<_>>();
        let mut replay_units = Vec::new();
        let mut visited = BTreeSet::new();
        {
            let mut node = self.node.borrow_mut();
            for tx_id in &pending {
                crate::db::block_on(collect_local_replay_commit_units(
                    &mut node,
                    *tx_id,
                    &mut visited,
                    &mut replay_units,
                ))?;
            }
        }
        for (tx_id, unit) in replay_units {
            // A reopened main-thread runtime has no transaction history. Send
            // accepted causal ancestors before each pending unit so the latter
            // can be ingested before its Local ack or later authority fate.
            downstream_fates.borrow_mut().push(unit.clone());
            if pending_set.contains(&tx_id) {
                self.queue_pending_upload(tx_id, Some(unit));
            }
        }
        for tx_id in pending {
            register_local_fate_route(&self.local_fate_routes, tx_id, downstream_fates);
        }
        queue_local_acknowledgements(&self.local_fate_routes, &self.node);
        Ok(())
    }

    pub(super) fn mark_subscriber_connections_dirty(&self) {
        let next = self.subscriber_dirty_epoch.get().wrapping_add(1);
        self.subscriber_dirty_epoch.set(next);
        for connection in self.connections.borrow().iter() {
            let mut connection = connection.borrow_mut();
            if let ConnectionLink::Subscriber { serve_dirty, .. } = &mut connection.link {
                *serve_dirty = true;
                connection.observed_subscriber_dirty_epoch.set(next);
            }
        }
    }

    #[cfg(feature = "testing")]
    /// Test/bench harnesses that mutate the served [`NodeState`] directly must
    /// mark subscriber links dirty. Production writes go through `Db`/sync
    /// boundaries that call this as a boundary effect.
    pub fn mark_subscriber_connections_dirty_for_test(&self) {
        self.mark_subscriber_connections_dirty();
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only encoded storage byte estimate across Jazz physical
    /// classes.
    pub fn encoded_storage_bytes_for_test(&self) -> Result<u64, Error> {
        Ok(self.node.borrow().encoded_storage_bytes_for_test()?)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only runtime diagnostics used by performance receipts.
    pub fn runtime_stats_for_test(&self) -> groove::ivm::RuntimeStats {
        self.node.borrow().runtime_stats_for_test()
    }

    pub(super) fn next_subscription_key(
        &self,
        shape: &ValidatedQuery,
        read_view: crate::protocol::ReadViewKey,
    ) -> SubscriptionKey {
        let nonce = self.next_subscription_nonce.get();
        self.next_subscription_nonce.set(nonce.saturating_add(1));
        SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: crate::query::BindingId(uuid::Uuid::new_v5(
                &crate::query::QUERY_NAMESPACE,
                &nonce.to_be_bytes(),
            )),
            read_view,
        }
    }

    pub(super) fn set_scheduler(&self, scheduler: Option<Rc<dyn TickScheduler>>) {
        *self.scheduler.borrow_mut() = scheduler;
    }

    pub(super) fn set_edge_cache_budget(&self, budget: Option<EdgeCacheBudget>) {
        self.edge_cache_budget.set(budget);
    }

    pub(super) fn schedule_tick(&self, urgency: TickUrgency) {
        schedule_tick_in(&self.scheduler, urgency);
    }

    pub(super) fn set_mutation_error_callback(&self, callback: Option<MutationErrorCallback>) {
        let should_schedule = {
            let mut state = self.mutation_errors.borrow_mut();
            state.callback = callback;
            state.callback.is_some() && !state.pending.is_empty()
        };
        if should_schedule {
            self.schedule_tick(TickUrgency::Immediate);
        }
    }

    fn consume_mutation_error(&self, tx_id: TxId) -> Result<bool, Error> {
        let pending = self.mutation_errors.borrow_mut().pending.remove(&tx_id);
        let retained = self.node.borrow().rejected_transaction(tx_id).is_some();
        if retained {
            crate::db::block_on(self.node.borrow_mut().discard_rejection(tx_id))?;
        }
        Ok(pending.is_some() || retained)
    }

    pub(super) fn transaction_wait_outcome(
        &self,
        tx_id: TxId,
        tier: DurabilityTier,
    ) -> Option<Result<TxId, Error>> {
        let state = crate::db::block_on(self.node.borrow_mut().transaction_state(tx_id));
        let Some((fate, _, durability)) = state else {
            return Some(Err(Error::new(
                ErrorCode::NotObserved,
                format!("transaction {tx_id:?} is not known locally"),
            )));
        };
        match fate {
            Fate::Rejected(reason) => {
                if let Err(error) = self.consume_mutation_error(tx_id) {
                    tracing::warn!(?tx_id, %error, "failed to consume waited mutation error");
                }
                Some(Err(write_rejected(tx_id, reason)))
            }
            Fate::Pending | Fate::Accepted if durability >= tier => Some(Ok(tx_id)),
            Fate::Pending | Fate::Accepted => None,
        }
    }

    pub(super) fn wait_for_transaction_with(
        self: &Rc<Self>,
        tx_id: TxId,
        tier: DurabilityTier,
        callback: Box<dyn FnOnce(Result<TxId, Error>)>,
    ) {
        if let Some(outcome) = self.transaction_wait_outcome(tx_id, tier) {
            callback(outcome);
            return;
        }
        let node = Rc::clone(self);
        self.register_write_state_callback(
            tx_id,
            Box::new(move || node.wait_for_transaction_with(tx_id, tier, callback)),
        );
    }

    fn deliver_pending_mutation_errors(&self) {
        let Some((callback, events)) = take_pending_mutation_error_delivery(&self.mutation_errors)
        else {
            return;
        };
        for (tx_id, event) in events {
            if let Err(error) = crate::db::block_on(self.node.borrow_mut().discard_rejection(tx_id))
            {
                tracing::warn!(?tx_id, %error, "failed to acknowledge delivered mutation error");
            }
            callback(&event);
        }
    }

    pub(super) fn request_permission_advice(
        &self,
        action: PermissionAdviceAction,
    ) -> PermissionAdviceFuture {
        let request_id = PermissionAdviceRequestId(*uuid::Uuid::new_v4().as_bytes());
        let (sender, receiver) = oneshot::channel();
        self.permission_advice_waiters
            .borrow_mut()
            .insert(request_id, sender);
        self.upstream_subscriptions
            .borrow_mut()
            .push(PendingUpstreamCommand::AuthorizationScopeIntent { request_id, action });
        self.schedule_tick(TickUrgency::Immediate);
        PermissionAdviceFuture {
            waiters: Rc::clone(&self.permission_advice_waiters),
            request_id,
            receiver,
        }
    }

    pub(super) fn cancel_permission_advice_request(&self, request_id: PermissionAdviceRequestId) {
        self.permission_advice_waiters
            .borrow_mut()
            .remove(&request_id);
        for connection in self.connections.borrow().iter() {
            let mut connection = connection.borrow_mut();
            if let ConnectionLink::Upstream {
                scope_lease_manager,
                ..
            } = &mut connection.link
            {
                let mut empty = false;
                for request in scope_lease_manager.requests.values_mut() {
                    if request.waiters.remove(&request_id) {
                        empty |= request.waiters.is_empty();
                    }
                }
                if empty {
                    scope_lease_manager
                        .requests
                        .retain(|_, request| !request.waiters.is_empty());
                }
            }
        }
    }

    pub(super) fn register_write_state_waiter(&self, tx_id: TxId) -> WriteStateChange {
        let waiter_id = self.next_write_state_waiter_id.get();
        self.next_write_state_waiter_id
            .set(waiter_id.wrapping_add(1).max(1));
        let (sender, receiver) = oneshot::channel();
        self.write_state_waiters
            .borrow_mut()
            .entry(tx_id)
            .or_default()
            .push(WriteStateWaiter {
                id: waiter_id,
                notify: WriteStateWaiterNotify::Future(sender),
            });
        WriteStateChange {
            waiters: Rc::clone(&self.write_state_waiters),
            tx_id,
            waiter_id,
            receiver,
        }
    }

    fn register_write_state_callback(&self, tx_id: TxId, callback: Box<dyn FnOnce()>) {
        let waiter_id = self.next_write_state_waiter_id.get();
        self.next_write_state_waiter_id
            .set(waiter_id.wrapping_add(1).max(1));
        self.write_state_waiters
            .borrow_mut()
            .entry(tx_id)
            .or_default()
            .push(WriteStateWaiter {
                id: waiter_id,
                notify: WriteStateWaiterNotify::Callback(callback),
            });
    }

    pub(super) async fn refresh_subscriptions(&self) -> Result<usize, Error> {
        refresh_subscriptions_in(
            &self.node,
            &self.subscriptions,
            &self.active_authority_view_receipts,
        )
        .await
    }

    /// Attach this node to an upstream peer over a binding-supplied transport.
    pub fn connect_upstream(
        &self,
        transport: Box<dyn Transport>,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        let local_receiver = self.receives_commits_as_local();
        let session_context = transport.connection_session_context();
        let connection_epoch = session_context
            .map(|context| context.local.epoch)
            .unwrap_or_else(|| uuid::Uuid::new_v4().as_u128() as u64);
        // Durable settled-view state remains available for known-state
        // payload repair, but a new upstream (including an edge switch) owns
        // no settlement receipts until it sends a fresh ViewUpdate.
        *self.active_authority_view_receipts.borrow_mut() = Some(AuthorityViewReceipts {
            connection_epoch,
            confirmation_floor: self.node.borrow().committed_global_time(),
            binding_views: BTreeSet::new(),
        });
        for state in self.subscriptions.borrow().iter().filter_map(Weak::upgrade) {
            let mut state = state.borrow_mut();
            if state.propagates_upstream {
                state.requires_authority_receipt = true;
            }
        }
        // A replacement link invalidates the prior link's receipt before the
        // new transport can deliver anything. Publish that demotion now rather
        // than letting cached rows remain settled until the next tick.
        let _ = self.refresh_subscriptions();
        let expected_scope_authority = session_context
            .filter(|context| {
                context.negotiated_features & crate::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS != 0
            })
            .map(|context| AuthorityContext {
                authority: *context.remote.node.as_bytes(),
                link: *context.link_identity.as_bytes(),
                connection_id: connection_epoch,
                connection_epoch: context.remote.epoch,
                claims_revision: 0,
                policy_epoch: 0,
                authorization_progress: 0,
                settled_through: 0,
            });
        // Keep every admitted link eligible, but bind each downstream route
        // to one stable selected owner. A newly connected parallel upstream
        // must not silently replace the owner (or settle its parked writes).
        if let Some(context) = expected_scope_authority {
            let mut eligible = self.admitted_upstream_authorities.borrow_mut();
            if !eligible.contains(&context) {
                eligible.push(context);
            }
            if self.admitted_upstream_authority.borrow().is_none() {
                *self.admitted_upstream_authority.borrow_mut() = Some(context);
                // Routes parked while no authority was connected retain their
                // downstream obligation. Bind them to this first successor
                // and restore each commit to the shared outbox: the former
                // authority may already have suppressed its upload, while
                // this newly connected successor has not seen it.
                let mut routes = self.edge_fate_routes.borrow_mut();
                let routed_txs = routes.keys().copied().collect::<Vec<_>>();
                for pending in routes.values_mut() {
                    for route in pending.iter_mut() {
                        route.authority = Some(context);
                    }
                }
                drop(routes);
                let mut outbox = self.outbox.borrow_mut();
                for tx_id in routed_txs {
                    if !outbox.iter().any(|pending| pending.tx_id == tx_id) {
                        outbox.push(PendingUpload {
                            tx_id,
                            unit: crate::db::block_on(
                                self.node.borrow_mut().commit_unit_for(tx_id),
                            )
                            .ok(),
                        });
                    }
                }
            }
        }
        // Carry queued and already-registered subscriptions upstream immediately.
        let mut pending = self
            .upstream_subscriptions
            .borrow_mut()
            .drain(..)
            .collect::<Vec<_>>();
        let mut pending_subscriptions = pending
            .iter()
            .filter_map(|command| {
                let PendingUpstreamCommand::Subscribe(subscription) = command else {
                    return None;
                };
                Some(subscription.subscription)
            })
            .collect::<BTreeSet<_>>();
        for registration in self.query_coverage_registrations.borrow().values() {
            if pending_subscriptions.insert(registration.subscription.subscription) {
                pending.push(PendingUpstreamCommand::Subscribe(
                    registration.subscription.clone(),
                ));
            }
        }
        for state_rc in self.subscriptions.borrow().iter().filter_map(Weak::upgrade) {
            {
                let state = state_rc.borrow();
                if !state.propagates_upstream {
                    continue;
                }
                let SubscriptionKind::Prepared { shape, binding, .. } = &state.kind;
                let opts = self.upstream_register_shape_options(
                    state.read_tier,
                    state.read_view.clone(),
                    state.remote_propagate_upstream,
                );
                let coverage = coverage_key(shape, binding, opts.clone());
                let subscription = self
                    .upstream_subscription_owners
                    .borrow()
                    .iter()
                    .find_map(|(subscription, owners)| {
                        owners
                            .iter()
                            .any(|owner| {
                                owner
                                    .upgrade()
                                    .is_some_and(|owner| Rc::ptr_eq(&owner, &state_rc))
                            })
                            .then_some(*subscription)
                    })
                    .unwrap_or_else(|| self.next_subscription_key(shape, opts.read_view_key()));
                self.latest_coverage_subscriptions
                    .borrow_mut()
                    .insert(coverage, subscription);
                if pending_subscriptions.insert(subscription) {
                    pending.push(PendingUpstreamCommand::Subscribe(
                        PendingUpstreamSubscription {
                            subscription,
                            shape: shape.clone(),
                            binding: binding.clone(),
                            opts,
                            identity: state.author,
                        },
                    ));
                }
            }
        }
        let connection = Rc::new(LocalMutex::new(PeerConnection {
            transport,
            staged_inbound: VecDeque::new(),
            node: Rc::clone(&self.node),
            subscriptions: Rc::clone(&self.subscriptions),
            upstream_subscription_owners: Rc::clone(&self.upstream_subscription_owners),
            latest_coverage_subscriptions: Rc::clone(&self.latest_coverage_subscriptions),
            awaiting_initial_authority_coverage: Rc::clone(
                &self.awaiting_initial_authority_coverage,
            ),
            active_authority_view_receipts: Rc::clone(&self.active_authority_view_receipts),
            scheduler: Rc::clone(&self.scheduler),
            write_state_waiters: Rc::clone(&self.write_state_waiters),
            permission_advice_waiters: Rc::clone(&self.permission_advice_waiters),
            edge_fate_routes: Rc::clone(&self.edge_fate_routes),
            local_fate_routes: Rc::clone(&self.local_fate_routes),
            admitted_upstream_authority: Rc::clone(&self.admitted_upstream_authority),
            downstream_fates: Rc::new(RefCell::new(Vec::new())),
            mutation_errors: Rc::clone(&self.mutation_errors),
            subscriber_dirty_epoch: Rc::clone(&self.subscriber_dirty_epoch),
            observed_subscriber_dirty_epoch: Cell::new(self.subscriber_dirty_epoch.get()),
            observed_session_claim_revision: Cell::new(0),
            connection_epoch,
            startup_error: None,
            link: ConnectionLink::Upstream {
                local_receiver,
                pending,
                upstream_subscriptions: Rc::clone(&self.upstream_subscriptions),
                announced_shapes: BTreeSet::new(),
                sent_session_claim_revisions: BTreeMap::new(),
                outbox: Rc::clone(&self.outbox),
                uploaded: BTreeSet::new(),
                pending_row_version_repairs: VecDeque::new(),
                scope_view_cuts: BTreeMap::new(),
                scope_receipts: BTreeMap::new(),
                expected_scope_authority,
                scope_lease_manager: AuthorizationScopeLeaseManager::default(),
            },
            last_resume_bytes: None,
        }));
        self.connections.borrow_mut().push(Rc::clone(&connection));
        self.schedule_tick(TickUrgency::Immediate);
        connection
    }

    /// Accept a subscriber connection served under `identity`.
    ///
    /// Local-vs-authority behavior is derived from this receiving node, not
    /// selected by the connecting client.
    pub fn accept_subscriber(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.accept_subscriber_with_trust(transport, identity, CommitUnitTrust::Session)
    }

    /// Accept a subscriber connection with explicit auth claims.
    pub fn accept_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.accept_subscriber_with_claims_and_trust(
            transport,
            identity,
            claims,
            CommitUnitTrust::Session,
        )
    }

    /// Accept a subscriber connection with an explicit commit-upload trust mode.
    pub fn accept_subscriber_with_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        trust: CommitUnitTrust,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.accept_subscriber_with_resume_and_trust(
            transport,
            identity,
            trust,
            BTreeMap::new(),
            None,
        )
    }

    /// Accept a subscriber connection with explicit auth claims and upload trust mode.
    pub fn accept_subscriber_with_claims_and_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.accept_subscriber_with_resume_and_trust(transport, identity, trust, claims, None)
    }

    /// Accept an edge-terminated subscriber with explicit auth claims.
    pub fn accept_edge_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.accept_subscriber_with_peer(
            transport,
            identity,
            CommitUnitTrust::Session,
            claims,
            None,
            PeerState::edge_client(identity),
            false,
        )
    }

    /// Accept a subscriber whose host shell is wired as an edge fate authority.
    pub fn accept_edge_authority_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.accept_subscriber_with_peer(
            transport,
            identity,
            CommitUnitTrust::Session,
            claims,
            None,
            PeerState::edge_client(identity),
            true,
        )
    }

    /// Accept a reconnecting subscriber, resuming from a previous cursor.
    pub fn accept_subscriber_with_resume(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        cursor: ResumeCursor,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.accept_subscriber_with_resume_and_trust(
            transport,
            identity,
            CommitUnitTrust::Session,
            BTreeMap::new(),
            Some(cursor),
        )
    }

    fn accept_subscriber_with_resume_and_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        trust: CommitUnitTrust,
        claims: BTreeMap<String, Value>,
        cursor: Option<ResumeCursor>,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        let peer = if self.receives_commits_as_local() {
            PeerState::relay()
        } else {
            match trust {
                CommitUnitTrust::TrustedBackend => {
                    PeerState::edge_client_with_permission_identity(identity, AuthorId::SYSTEM)
                }
                CommitUnitTrust::Session => PeerState::client_link(identity),
            }
        };
        self.accept_subscriber_with_peer(transport, identity, trust, claims, cursor, peer, false)
    }

    fn accept_subscriber_with_peer(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        trust: CommitUnitTrust,
        claims: BTreeMap<String, Value>,
        cursor: Option<ResumeCursor>,
        peer: PeerState,
        edge_authority: bool,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        let local_receiver = self.receives_commits_as_local() && !edge_authority;
        let (peer, ingest_context, session_claims, session_claim_revision) = match cursor {
            Some(cursor) => {
                assert_eq!(
                    cursor.ingest_context.identity, identity,
                    "a resume cursor may only be used by its authenticated identity"
                );
                (
                    cursor.peer,
                    cursor.ingest_context,
                    cursor.session_claims,
                    cursor.session_claim_revision,
                )
            }
            None => (
                peer,
                CommitUnitIngestContext {
                    identity,
                    trust,
                    edge_authority,
                },
                claims,
                0,
            ),
        };
        let connection_epoch = transport
            .connection_session_context()
            .map(|context| context.local.epoch)
            .unwrap_or_else(|| uuid::Uuid::new_v4().as_u128() as u64);
        let downstream_fates = Rc::new(RefCell::new(Vec::new()));
        let startup_error = local_receiver
            .then(|| self.restore_local_subscriber(identity, &downstream_fates))
            .and_then(Result::err);
        let connection = Rc::new(LocalMutex::new(PeerConnection {
            transport,
            staged_inbound: VecDeque::new(),
            node: Rc::clone(&self.node),
            subscriptions: Rc::clone(&self.subscriptions),
            upstream_subscription_owners: Rc::clone(&self.upstream_subscription_owners),
            latest_coverage_subscriptions: Rc::clone(&self.latest_coverage_subscriptions),
            awaiting_initial_authority_coverage: Rc::clone(
                &self.awaiting_initial_authority_coverage,
            ),
            active_authority_view_receipts: Rc::clone(&self.active_authority_view_receipts),
            scheduler: Rc::clone(&self.scheduler),
            write_state_waiters: Rc::clone(&self.write_state_waiters),
            permission_advice_waiters: Rc::clone(&self.permission_advice_waiters),
            edge_fate_routes: Rc::clone(&self.edge_fate_routes),
            local_fate_routes: Rc::clone(&self.local_fate_routes),
            admitted_upstream_authority: Rc::clone(&self.admitted_upstream_authority),
            downstream_fates,
            mutation_errors: Rc::clone(&self.mutation_errors),
            subscriber_dirty_epoch: Rc::clone(&self.subscriber_dirty_epoch),
            observed_subscriber_dirty_epoch: Cell::new(self.subscriber_dirty_epoch.get()),
            observed_session_claim_revision: Cell::new(session_claim_revision),
            connection_epoch,
            startup_error,
            link: ConnectionLink::Subscriber {
                peer,
                ingest_context,
                session_claims,
                session_claim_revision,
                local_receiver,
                outbox: Rc::clone(&self.outbox),
                upstream_subscriptions: Rc::clone(&self.upstream_subscriptions),
                served: BTreeMap::new(),
                coverage_groups: BTreeMap::new(),
                shape_registrations: BTreeMap::new(),
                deferred_subscribe_rejections: VecDeque::new(),
                served_current_rows: BTreeMap::new(),
                scope_purposes: BTreeMap::new(),
                scope_aggregates: BTreeMap::new(),
                authority_scope_hydrations: BTreeMap::new(),
                authority_scope_hydration_count: 0,
                serve_dirty: true,
            },
            last_resume_bytes: None,
        }));
        self.connections.borrow_mut().push(Rc::clone(&connection));
        self.schedule_tick(TickUrgency::Immediate);
        connection
    }

    /// Detach a previously attached peer connection from this node.
    pub fn detach_connection(&self, connection: &Rc<LocalMutex<PeerConnection<S>>>) -> bool {
        let connection_ref = connection.borrow();
        let (authority, upstream_epoch) = match &connection_ref.link {
            ConnectionLink::Upstream {
                expected_scope_authority,
                ..
            } => (
                *expected_scope_authority,
                Some(connection_ref.connection_epoch),
            ),
            ConnectionLink::Subscriber { .. } => (None, None),
        };
        drop(connection_ref);
        let mut connections = self.connections.borrow_mut();
        let before = connections.len();
        connections.retain(|candidate| !Rc::ptr_eq(candidate, connection));
        let detached = connections.len() != before;
        drop(connections);
        if detached
            && let Some(epoch) = upstream_epoch
            && self
                .active_authority_view_receipts
                .borrow()
                .as_ref()
                .is_some_and(|receipts| receipts.connection_epoch == epoch)
        {
            // A parallel upstream that survived the selected link is a new
            // active authority epoch for settlement purposes. Its older
            // receipt was retired by the switch, so it must confirm the view
            // again before cached rows can become settled.
            // Retire B before staging A's queued frames: otherwise applying a
            // row-changing A update during the handoff could briefly publish
            // it under B's now-dead receipt.
            *self.active_authority_view_receipts.borrow_mut() = None;
            let fallback_connection =
                self.connections
                    .borrow()
                    .iter()
                    .rev()
                    .find_map(|connection| {
                        let connection_ref = connection.borrow();
                        matches!(&connection_ref.link, ConnectionLink::Upstream { .. })
                            .then(|| Rc::clone(connection))
                    });
            if let Some(connection) = &fallback_connection {
                connection
                    .borrow_mut()
                    .stage_inbound_without_authority_receipt();
            }
            *self.active_authority_view_receipts.borrow_mut() =
                fallback_connection.map(|connection| AuthorityViewReceipts {
                    connection_epoch: connection.borrow().connection_epoch,
                    confirmation_floor: self.node.borrow().committed_global_time(),
                    binding_views: BTreeSet::new(),
                });
            // Cached rows remain readable as stale/local state, but their
            // settled receipt died with this authority connection.
            let _ = self.refresh_subscriptions();
        }
        if detached && let Some(authority) = authority {
            let mut eligible = self.admitted_upstream_authorities.borrow_mut();
            eligible.retain(|candidate| *candidate != authority);
            if *self.admitted_upstream_authority.borrow() == Some(authority) {
                // Old routes cannot migrate to a replacement authority: they
                // must be rebound explicitly to the deterministic handoff
                // owner. Both upstreams share the upload outbox, so B may
                // already have the unit; clearing this route would strand an
                // Edge-Accepted caller forever.
                let handoff = eligible.first().copied();
                *self.admitted_upstream_authority.borrow_mut() = handoff;
                let mut routes = self.edge_fate_routes.borrow_mut();
                if let Some(handoff) = handoff {
                    routes.retain(|_, pending| {
                        pending.retain(|route| route.queue.upgrade().is_some());
                        for route in pending.iter_mut() {
                            if route.authority == Some(authority) {
                                route.authority = Some(handoff);
                            }
                        }
                        !pending.is_empty()
                    });
                    let routed_txs = routes.keys().copied().collect::<Vec<_>>();
                    drop(routes);
                    // Re-drive through the successor even when it had sent
                    // the unit before becoming owner. Its per-link uploaded
                    // set is an optimization, never a fate authority token.
                    for candidate in self.connections.borrow().iter() {
                        let mut candidate = candidate.borrow_mut();
                        let ConnectionLink::Upstream {
                            expected_scope_authority,
                            uploaded,
                            outbox,
                            ..
                        } = &mut candidate.link
                        else {
                            continue;
                        };
                        if *expected_scope_authority != Some(handoff) {
                            continue;
                        }
                        for tx_id in &routed_txs {
                            uploaded.remove(tx_id);
                            let mut outbox = outbox.borrow_mut();
                            if !outbox.iter().any(|pending| pending.tx_id == *tx_id) {
                                outbox.push(PendingUpload {
                                    tx_id: *tx_id,
                                    unit: crate::db::block_on(
                                        self.node.borrow_mut().commit_unit_for(*tx_id),
                                    )
                                    .ok(),
                                });
                            }
                        }
                    }
                    self.schedule_tick(TickUrgency::Immediate);
                } else {
                    // No successor yet: preserve bounded live downstream
                    // routes for a later admitted authority.  Clearing them
                    // after an Edge acceptance would strand the caller.
                    routes.retain(|_, pending| {
                        pending.retain(|route| route.queue.upgrade().is_some());
                        for route in pending.iter_mut() {
                            if route.authority == Some(authority) {
                                route.authority = None;
                            }
                        }
                        !pending.is_empty()
                    });
                    self.schedule_tick(TickUrgency::Immediate);
                }
            }
        }
        detached
    }

    /// Service every accepted subscriber connection once.
    pub async fn tick(&self) -> Result<DbTickStats, Error> {
        self.deliver_pending_mutation_errors();
        let mut stats = DbTickStats::default();
        let mut remote_sync_applied = false;
        // A later subscriber can mutate Core state after an earlier peer link
        // has already had its turn in this pass.  Remember that generation so
        // the post-receive serve pass below reaches that earlier link too;
        // websocket hosts are event-driven and need not provide unrelated
        // follow-up traffic just to flush a freshly accepted row.
        let subscriber_dirty_epoch_before = self.subscriber_dirty_epoch.get();
        let connections = self.connections.borrow().clone();
        for connection in &connections {
            let next = connection.lock().await.tick().await?;
            stats.subscription_events += next.subscription_events;
            stats.remote_sync_applied += next.remote_sync_applied;
            remote_sync_applied |= next.remote_sync_applied > 0;
        }
        let subscriber_state_changed =
            self.subscriber_dirty_epoch.get() != subscriber_dirty_epoch_before;
        if remote_sync_applied || subscriber_state_changed {
            for connection in &connections {
                let should_tick = {
                    let mut connection = connection.lock().await;
                    connection.mark_subscriber_dirty() || subscriber_state_changed
                };
                if should_tick {
                    let next = connection.lock().await.tick().await?;
                    stats.subscription_events += next.subscription_events;
                    stats.remote_sync_applied += next.remote_sync_applied;
                }
            }
        }
        if let Some(budget) = self.edge_cache_budget.get() {
            let mut pins = crate::peer::PeerEvictionPins::default();
            for connection in &connections {
                pins.extend(connection.lock().await.eviction_pins());
            }
            self.node
                .lock()
                .await
                .enforce_edge_cache_budget(&pins, budget)
                .await?;
        }
        self.prune_settled_outbox_uploads();
        Ok(stats)
    }

    fn prune_settled_outbox_uploads(&self) {
        let mut outbox = self.outbox.borrow_mut();
        if outbox.is_empty() {
            return;
        }
        let mut node = self.node.borrow_mut();
        outbox.retain(|pending| {
            let state = crate::db::block_on(node.transaction_state(pending.tx_id));
            let Some((fate, _, durability)) = state else {
                return true;
            };
            matches!(fate, Fate::Pending | Fate::Accepted) && durability < DurabilityTier::Global
        });
    }
}

async fn optimistic_transaction_row_keys_for_query<S>(
    node: &SharedNodeState<S>,
    cache: &mut BTreeMap<AuthorId, BTreeSet<(String, RowUuid)>>,
    shape: &ValidatedQuery,
    author: AuthorId,
) -> Result<BTreeSet<(String, RowUuid)>, Error>
where
    S: OrderedKvStorage,
{
    let row_keys = match cache.entry(author) {
        std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::btree_map::Entry::Vacant(entry) => {
            let transactions = node
                .lock()
                .await
                .unresolved_transaction_ids_for_author(author)
                .await?;
            let row_keys = node
                .lock()
                .await
                .transaction_row_keys(&transactions)
                .await?;
            entry.insert(row_keys)
        }
    };
    Ok(node
        .borrow()
        .transaction_row_keys_for_query(shape, row_keys))
}

/// Re-evaluate every live subscription against the node and push a delta event
/// for any whose rows changed. Shared by local writes
/// ([`Db::refresh_subscriptions`]) and by inbound sync application
/// ([`PeerConnection::tick`]).
pub(super) async fn refresh_subscriptions_in<S>(
    node: &SharedNodeState<S>,
    subscriptions: &SubscriptionList,
    active_authority_view_receipts: &ActiveAuthorityViewReceipts,
) -> Result<usize, Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let mut retained = Vec::new();
    let mut changed = 0;
    let mut optimistic_row_keys_by_author = BTreeMap::new();
    let pending_authoritative_resets = node
        .lock()
        .await
        .take_pending_authoritative_reset_binding_views();
    let mut consumed_authoritative_resets = BTreeSet::new();
    node.lock().await.flush_query_runtime().await?;
    for weak in subscriptions.borrow().iter() {
        let Some(state) = weak.upgrade() else {
            continue;
        };
        let (
            read_tier,
            remote_read_tier,
            requires_authority_receipt,
            remote_propagate_upstream,
            read_view,
            previous_source,
            previous_settled,
            author,
            authorization_mode,
            terminal_rows,
        ) = {
            let state = state.borrow();
            (
                state.read_tier,
                state.remote_read_tier,
                state.requires_authority_receipt,
                state.remote_propagate_upstream,
                state.read_view.clone(),
                state.snapshot_source,
                state.settled,
                state.author,
                state.authorization_mode,
                state.terminal_rows,
            )
        };
        let groove_runtime_token = node.borrow().groove_runtime_token();
        if state.borrow().groove_runtime_token != groove_runtime_token {
            let (shape, binding) = {
                let state = state.borrow();
                match &state.kind {
                    SubscriptionKind::Prepared { shape, binding, .. } => {
                        (shape.clone(), binding.clone())
                    }
                }
            };
            let stale_subscription_id = {
                let state = state.borrow();
                match &state.kind {
                    SubscriptionKind::Prepared {
                        maintained_subscription,
                        ..
                    } => maintained_subscription
                        .as_ref()
                        .map(LocalMaintainedViewSubscription::subscription_id),
                }
            };
            // The Jazz runtime token invalidates prepared plans, while the
            // Groove runtime itself remains alive. Retire the old maintained
            // handle before installing its replacement so two descriptor
            // generations cannot consume the next physical delta.
            if let Some(subscription_id) = stale_subscription_id {
                node.lock()
                    .await
                    .unsubscribe_groove_subscription(subscription_id)
                    .await;
            }
            let (shape, binding, prepared_plan) = node
                .lock()
                .await
                .prepare_query_binding_for_link_in_authorization_mode(
                    &shape,
                    &binding,
                    read_tier,
                    author,
                    authorization_mode,
                )
                .await?;
            let (previous_snapshot, previous_snapshot_index) = {
                let state_ref = state.borrow();
                (state_ref.snapshot.clone(), state_ref.snapshot_index.clone())
            };
            let (maintained, mut snapshot) = node
                .lock()
                .await
                .open_maintained_view_subscription_in_authorization_mode(
                    &shape,
                    &binding,
                    author,
                    read_tier,
                    &read_view,
                    Some(prepared_plan),
                    authorization_mode,
                )
                .await?;
            let delivered_binding_view = BindingViewKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: RegisterShapeOptions {
                    tier: read_tier,
                    read_view: read_view.clone(),
                    ..RegisterShapeOptions::default()
                }
                .read_view_key(),
            };
            // From this point, cleanup must target the replacement runtime
            // subscription even if a later fallible refresh step aborts.
            {
                let mut state_ref = state.borrow_mut();
                let subscription_id = maintained.subscription_id();
                match &mut state_ref.kind {
                    SubscriptionKind::Prepared {
                        maintained_subscription,
                        ..
                    } => *maintained_subscription = Some(maintained),
                }
                state_ref
                    .local_subscription_cleanup
                    .set(Some((groove_runtime_token, subscription_id)));
            }
            let settled_tier = remote_read_tier.unwrap_or(read_tier);
            let settled_binding_view = BindingViewKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: RegisterShapeOptions {
                    tier: settled_tier,
                    read_view: read_view.clone(),
                    propagate_upstream: remote_propagate_upstream,
                }
                .read_view_key(),
            };
            let pending_binding_view = pending_authoritative_resets
                .contains(&delivered_binding_view)
                .then_some(delivered_binding_view)
                .or_else(|| {
                    pending_authoritative_resets
                        .contains(&settled_binding_view)
                        .then_some(settled_binding_view)
                });
            let authoritative_binding_view = pending_binding_view.unwrap_or(settled_binding_view);
            let local_overlay_row_keys = if authorization_mode
                == QueryAuthorizationMode::ClientLocal
                && read_tier == DurabilityTier::Local
                && remote_read_tier.is_some_and(|tier| tier >= DurabilityTier::Edge)
                && node.borrow().authored_commit_durability() == DurabilityTier::None
                && active_authority_view_receipts.borrow().is_some()
                && supports_pending_overlay_reconciliation(shape.query())
            {
                optimistic_transaction_row_keys_for_query(
                    node,
                    &mut optimistic_row_keys_by_author,
                    &shape,
                    author,
                )
                .await?
            } else {
                BTreeSet::new()
            };
            let has_conflicting_local_overlay = {
                let state_ref = state.borrow();
                let SubscriptionKind::Prepared {
                    maintained_subscription,
                    ..
                } = &state_ref.kind;
                maintained_subscription.as_ref().is_some_and(|maintained| {
                    node.borrow()
                        .local_maintained_authority_reconciliation_conflicts(
                            maintained,
                            authoritative_binding_view,
                            &local_overlay_row_keys,
                        )
                })
            };
            if authorization_mode == QueryAuthorizationMode::ClientLocal
                && remote_read_tier.is_some()
                && shape.query().aggregate.is_none()
            {
                let mut state_ref = state.borrow_mut();
                let SubscriptionKind::Prepared {
                    maintained_subscription,
                    ..
                } = &mut state_ref.kind;
                if let Some(maintained) = maintained_subscription.as_mut() {
                    node.borrow()
                        .seed_local_maintained_authoritative_generation(
                            maintained,
                            authoritative_binding_view,
                        );
                    if has_conflicting_local_overlay {
                        node.borrow()
                            .defer_local_maintained_authority_reconciliation(maintained);
                    }
                }
            }
            if let Some(binding_view) = pending_binding_view {
                if has_conflicting_local_overlay {
                    let mut state_ref = state.borrow_mut();
                    let SubscriptionKind::Prepared {
                        maintained_subscription,
                        ..
                    } = &mut state_ref.kind;
                    let maintained = maintained_subscription
                        .as_mut()
                        .expect("replacement maintained subscription installed");
                    let (update, suppressed) = node
                        .lock()
                        .await
                        .drain_local_maintained_view_subscription_preserving_rows(
                            maintained,
                            Some(binding_view),
                            &local_overlay_row_keys,
                        )
                        .await?;
                    debug_assert!(suppressed);
                    if let Some(update) = update {
                        let mut snapshot_index = RelationSnapshotIndex::from_snapshot(&snapshot);
                        let _ = apply_maintained_update_to_snapshot(
                            &mut snapshot,
                            &mut snapshot_index,
                            update,
                            read_tier,
                            previous_settled,
                            terminal_rows,
                        );
                    }
                    consumed_authoritative_resets.insert(binding_view);
                } else {
                    let authoritative = node
                        .lock()
                        .await
                        .authoritative_reset_snapshot_for_binding_view(&shape, binding_view)
                        .await?;
                    if let Some(authoritative) = authoritative {
                        let mut state_ref = state.borrow_mut();
                        let SubscriptionKind::Prepared {
                            maintained_subscription,
                            ..
                        } = &mut state_ref.kind;
                        let maintained = maintained_subscription
                            .as_mut()
                            .expect("replacement maintained subscription installed");
                        node.lock()
                            .await
                            .reset_local_maintained_view_subscription_from_binding_view(
                                maintained,
                                binding_view,
                            )
                            .await?;
                        snapshot = authoritative;
                        consumed_authoritative_resets.insert(binding_view);
                    }
                }
            }
            let root_occurrence_ids = if shape.query().aggregate.is_some() {
                snapshot
                    .rows
                    .iter()
                    .map(|row| {
                        crate::tools::OutputOccurrenceId::single_source(
                            crate::tools::ObjectId::from_uuid(row.row_uuid().0),
                        )
                    })
                    .collect()
            } else {
                let state_ref = state.borrow();
                let SubscriptionKind::Prepared {
                    maintained_subscription,
                    ..
                } = &state_ref.kind;
                maintained_subscription
                    .as_ref()
                    .expect("replacement maintained subscription installed")
                    .root_occurrence_ids()
                    .to_vec()
            };
            let settled = subscription_is_settled(
                &node.borrow(),
                active_authority_view_receipts,
                &shape,
                &binding,
                settled_tier,
                read_view.clone(),
                remote_propagate_upstream,
                requires_authority_receipt,
            );
            let mut event = subscription_delta_event_with_reset(
                read_tier,
                settled,
                &previous_snapshot,
                &snapshot,
                false,
                terminal_rows,
            );
            if let SubscriptionEvent::Delta {
                reset,
                publishable,
                added,
                updated,
                removed,
                ..
            } = &mut event
            {
                *reset = true;
                *added =
                    subscription_outputs_with_occurrence_sidecar(&snapshot, &root_occurrence_ids)?;
                updated.clear();
                *removed = reset_removed_roots(
                    &previous_snapshot,
                    &previous_snapshot_index,
                    &root_occurrence_ids,
                );
                *publishable = settled || !added.is_empty() || !removed.is_empty();
            }
            let mut state_ref = state.borrow_mut();
            state_ref.groove_runtime_token = groove_runtime_token;
            state_ref.snapshot = relation_snapshot_with_delta_slack(&snapshot);
            state_ref.snapshot_index = RelationSnapshotIndex::from_snapshot(&state_ref.snapshot);
            state_ref.snapshot_index.roots = root_occurrence_ids
                .into_iter()
                .enumerate()
                .map(|(index, occurrence)| (occurrence, index))
                .collect();
            state_ref.snapshot_source = SubscriptionSnapshotSource::LocalMaintained;
            state_ref.settled = settled;
            if state_ref.sender.unbounded_send(event).is_ok() {
                changed += 1;
            }
            drop(state_ref);
            retained.push(Rc::downgrade(&state));
            continue;
        }
        let (mut snapshot, mut snapshot_source, settled, snapshot_tier, force_reset_event) = {
            let mut state_ref = state.borrow_mut();
            let local_snapshot_is_empty =
                state_ref.snapshot.root_count == 0 && state_ref.snapshot.edges.is_empty();
            match &mut state_ref.kind {
                SubscriptionKind::Prepared {
                    shape,
                    binding,
                    maintained_subscription,
                } => {
                    let shape = shape.clone();
                    let binding = binding.clone();
                    let has_maintained_subscription = maintained_subscription.is_some();
                    let remote_settled_tier = remote_read_tier.filter(|tier| {
                        node.borrow().has_settled_result_set(BindingViewKey {
                            shape_id: shape.shape_id(),
                            binding_id: binding.binding_id(),
                            read_view: RegisterShapeOptions {
                                tier: *tier,
                                read_view: read_view.clone(),
                                propagate_upstream: remote_propagate_upstream,
                            }
                            .read_view_key(),
                        })
                    });
                    let settled_tier = remote_read_tier.unwrap_or(read_tier);
                    let settled_binding_view = BindingViewKey {
                        shape_id: shape.shape_id(),
                        binding_id: binding.binding_id(),
                        read_view: RegisterShapeOptions {
                            tier: settled_tier,
                            read_view: read_view.clone(),
                            propagate_upstream: remote_propagate_upstream,
                        }
                        .read_view_key(),
                    };
                    let delivered_binding_view = BindingViewKey {
                        shape_id: shape.shape_id(),
                        binding_id: binding.binding_id(),
                        read_view: RegisterShapeOptions {
                            tier: read_tier,
                            read_view: read_view.clone(),
                            ..RegisterShapeOptions::default()
                        }
                        .read_view_key(),
                    };
                    let authoritative_reset_binding_view =
                        if pending_authoritative_resets.contains(&delivered_binding_view) {
                            delivered_binding_view
                        } else {
                            settled_binding_view
                        };
                    let authoritative_reset_pending =
                        pending_authoritative_resets.contains(&authoritative_reset_binding_view);
                    let authority_reconciliation_due = authoritative_reset_pending
                        || maintained_subscription.as_ref().is_some_and(|maintained| {
                            node.borrow().local_maintained_authority_reconciliation_due(
                                maintained,
                                authoritative_reset_binding_view,
                            )
                        });
                    let local_overlay_row_keys = if authorization_mode
                        == QueryAuthorizationMode::ClientLocal
                        && read_tier == DurabilityTier::Local
                        && remote_read_tier.is_some_and(|tier| tier >= DurabilityTier::Edge)
                        && node.borrow().authored_commit_durability() == DurabilityTier::None
                        && active_authority_view_receipts.borrow().is_some()
                        && supports_pending_overlay_reconciliation(shape.query())
                        && authority_reconciliation_due
                    {
                        optimistic_transaction_row_keys_for_query(
                            node,
                            &mut optimistic_row_keys_by_author,
                            &shape,
                            author,
                        )
                        .await?
                    } else {
                        BTreeSet::new()
                    };
                    let has_conflicting_local_overlay =
                        maintained_subscription.as_ref().is_some_and(|maintained| {
                            node.borrow()
                                .local_maintained_authority_reconciliation_conflicts(
                                    maintained,
                                    authoritative_reset_binding_view,
                                    &local_overlay_row_keys,
                                )
                        });
                    if authoritative_reset_pending {
                        consumed_authoritative_resets.insert(authoritative_reset_binding_view);
                    }
                    if node
                        .borrow()
                        .publication_deferred_for_binding_view(settled_binding_view)
                        || node
                            .borrow()
                            .publication_deferred_for_binding_view(delivered_binding_view)
                    {
                        if authoritative_reset_pending {
                            node.borrow_mut()
                                .defer_authoritative_reset_for_binding_view(
                                    authoritative_reset_binding_view,
                                );
                        }
                        retained.push(Rc::downgrade(&state));
                        continue;
                    }
                    let peer_terminal_operations = node
                        .borrow_mut()
                        .take_pending_terminal_operations(delivered_binding_view);
                    let snapshot_tier = remote_settled_tier.unwrap_or(read_tier);
                    // The browser worker owns the durable baseline, while the
                    // main Db owns the application subscription and its
                    // optimistic overlay. Reconcile a worker reset through the
                    // maintained view below so a delayed hydration snapshot
                    // cannot replace newer main-thread writes.
                    let reconciles_remote_authoritative_membership = authorization_mode
                        == QueryAuthorizationMode::ClientLocal
                        && remote_read_tier.is_some()
                        && supports_pending_overlay_reconciliation(shape.query())
                        && (has_conflicting_local_overlay
                            || (remote_read_tier.is_some_and(|tier| tier < DurabilityTier::Edge)
                                && node.borrow().authored_commit_durability()
                                    == DurabilityTier::None));
                    // Preserve the reset boundary unless it would overwrite
                    // an unsettled local row. A browser worker's Local handoff
                    // is an internal baseline update, while Edge/Global
                    // handoffs remain public authority boundaries.
                    let authoritative_reset = authoritative_reset_pending
                        && (!reconciles_remote_authoritative_membership
                            || (local_snapshot_is_empty && !has_conflicting_local_overlay));
                    if authoritative_reset && terminal_rows {
                        let Some(maintained) = maintained_subscription.as_mut() else {
                            return Err(Error::new(
                                ErrorCode::Protocol,
                                "structured subscription lost its Groove terminal",
                            ));
                        };
                        // A structural-patch stream deliberately does not keep
                        // facade-level replacement rows current. Re-open the
                        // Groove terminal at an authoritative boundary so the
                        // reset is a fresh complete value and subsequent FIFO
                        // patches are relative to exactly that value.
                        let (replacement, snapshot) = node
                            .lock()
                            .await
                            .open_maintained_view_subscription_in_authorization_mode(
                                &shape,
                                &binding,
                                author,
                                read_tier,
                                &read_view,
                                None,
                                authorization_mode,
                            )
                            .await?;
                        *maintained = replacement;
                        let settled = subscription_is_settled(
                            &node.borrow(),
                            active_authority_view_receipts,
                            &shape,
                            &binding,
                            settled_tier,
                            read_view,
                            remote_propagate_upstream,
                            requires_authority_receipt,
                        );
                        (
                            snapshot,
                            SubscriptionSnapshotSource::LocalMaintained,
                            settled,
                            snapshot_tier,
                            true,
                        )
                    } else if authoritative_reset {
                        let authoritative_snapshot = {
                            let mut node_ref = node.lock().await;
                            match node_ref
                                .authoritative_reset_snapshot_for_binding_view(
                                    &shape,
                                    authoritative_reset_binding_view,
                                )
                                .await
                            {
                                Ok(snapshot) => snapshot,
                                Err(crate::node::Error::MissingTransaction(_)) => {
                                    node_ref.record_authoritative_reset_missing_payload_fallback();
                                    node_ref.defer_authoritative_reset_for_binding_view(
                                        authoritative_reset_binding_view,
                                    );
                                    None
                                }
                                Err(error) => return Err(error.into()),
                            }
                        };
                        let authoritative_snapshot_available = authoritative_snapshot.is_some();
                        let maintained_update = if let Some(maintained) =
                            maintained_subscription.as_mut()
                        {
                            let mut node_ref = node.lock().await;
                            if authoritative_snapshot_available {
                                match node_ref
                                    .drain_local_maintained_view_subscription_state(
                                        maintained, None,
                                    )
                                    .await
                                {
                                    Ok(_) => {
                                        node_ref
                                            .reset_local_maintained_view_subscription_from_binding_view(
                                                maintained,
                                                authoritative_reset_binding_view,
                                            ).await?;
                                        None
                                    }
                                    Err(error) => return Err(error.into()),
                                }
                            } else {
                                match node_ref
                                    .drain_local_maintained_view_subscription(maintained, None)
                                    .await
                                {
                                    Ok(update) => update,
                                    Err(crate::node::Error::MissingTransaction(_)) => {
                                        node_ref
                                            .record_authoritative_reset_missing_payload_fallback();
                                        node_ref.defer_authoritative_reset_for_binding_view(
                                            authoritative_reset_binding_view,
                                        );
                                        retained.push(Rc::downgrade(&state));
                                        continue;
                                    }
                                    Err(error) => return Err(error.into()),
                                }
                            }
                        } else {
                            None
                        };
                        let (mut snapshot, force_reset_event) =
                            if let Some(snapshot) = authoritative_snapshot {
                                (snapshot, true)
                            } else {
                                let fallback = {
                                    let mut node_ref = node.lock().await;
                                    match node_ref
                                        .subscription_snapshot_in_authorization_mode(
                                            &shape,
                                            &binding,
                                            snapshot_tier,
                                            author,
                                            &read_view,
                                            authorization_mode,
                                        )
                                        .await
                                    {
                                        Ok(snapshot) => snapshot,
                                        Err(crate::node::Error::MissingTransaction(_)) => {
                                            node_ref
                                            .record_authoritative_reset_missing_payload_fallback();
                                            node_ref.defer_authoritative_reset_for_binding_view(
                                                authoritative_reset_binding_view,
                                            );
                                            retained.push(Rc::downgrade(&state));
                                            continue;
                                        }
                                        Err(error) => return Err(error.into()),
                                    }
                                };
                                (fallback, false)
                            };
                        if let Some(update) = maintained_update {
                            let mut snapshot_index =
                                RelationSnapshotIndex::from_snapshot(&snapshot);
                            let _ = apply_maintained_update_to_snapshot(
                                &mut snapshot,
                                &mut snapshot_index,
                                update,
                                snapshot_tier,
                                previous_settled,
                                terminal_rows,
                            );
                        }
                        let settled = subscription_is_settled(
                            &node.borrow(),
                            active_authority_view_receipts,
                            &shape,
                            &binding,
                            settled_tier,
                            read_view,
                            remote_propagate_upstream,
                            requires_authority_receipt,
                        );
                        (
                            snapshot,
                            SubscriptionSnapshotSource::LinkSnapshot,
                            settled,
                            snapshot_tier,
                            force_reset_event,
                        )
                    } else {
                        if terminal_rows && !peer_terminal_operations.is_empty() {
                            let terminal_layout = maintained_subscription
                                .as_ref()
                                .and_then(|maintained| maintained.terminal_root_layout().cloned());
                            if let Some(maintained) = maintained_subscription.as_mut() {
                                // The serving terminal is authoritative for
                                // structural publication. Advance the local
                                // Groove mirror for future resets without
                                // publishing its redundant reconstruction.
                                node.lock()
                                    .await
                                    .drain_local_maintained_view_subscription_state(
                                        maintained, None,
                                    )
                                    .await?;
                            }
                            let settled = subscription_is_settled(
                                &node.borrow(),
                                active_authority_view_receipts,
                                &shape,
                                &binding,
                                settled_tier,
                                read_view.clone(),
                                remote_propagate_upstream,
                                requires_authority_receipt,
                            );
                            let state_ref = &mut *state_ref;
                            let event = SubscriptionEvent::Delta {
                                reset: false,
                                publishable: true,
                                added: Vec::new(),
                                updated: Vec::new(),
                                removed: Vec::new(),
                                terminal_operations: peer_terminal_operations,
                                terminal_layout,
                                settled,
                                tier: snapshot_tier,
                            };
                            state_ref.settled = settled;
                            retained.push(Rc::downgrade(&state));
                            if state_ref.sender.unbounded_send(event).is_ok() {
                                changed += 1;
                            }
                            continue;
                        }
                        let (maintained_update, suppressed_authoritative_change) =
                            if let Some(maintained) = maintained_subscription.as_mut() {
                                let mut node_ref = node.lock().await;
                                // Every client-local remote subscription must
                                // drain against the authority's binding view. The
                                // non-durable browser runtime additionally uses
                                // that same view to preserve its local overlay;
                                // restricting the view to only that runtime makes
                                // ordinary Local clients miss a later authority
                                // revoke until a further refresh.
                                let authoritative_binding_view = (authorization_mode
                                    == QueryAuthorizationMode::ClientLocal
                                    && remote_read_tier.is_some()
                                    && shape.query().aggregate.is_none())
                                .then_some(settled_binding_view);
                                match node_ref
                                    .drain_local_maintained_view_subscription_preserving_rows(
                                        maintained,
                                        authoritative_binding_view,
                                        &local_overlay_row_keys,
                                    )
                                    .await
                                {
                                    Ok(update) => update,
                                    Err(crate::node::Error::MissingTransaction(_)) => {
                                        node_ref
                                            .record_authoritative_reset_missing_payload_fallback();
                                        node_ref.defer_authoritative_reset_for_binding_view(
                                            authoritative_reset_binding_view,
                                        );
                                        retained.push(Rc::downgrade(&state));
                                        continue;
                                    }
                                    Err(error) => return Err(error.into()),
                                }
                            } else {
                                (None, false)
                            };
                        if let Some(update) = maintained_update {
                            if terminal_rows {
                                if !update.terminal_operations.is_empty() {
                                    let settled = subscription_is_settled(
                                        &node.borrow(),
                                        active_authority_view_receipts,
                                        &shape,
                                        &binding,
                                        settled_tier,
                                        read_view,
                                        remote_propagate_upstream,
                                        requires_authority_receipt,
                                    );
                                    let state_ref = &mut *state_ref;
                                    let event = SubscriptionEvent::Delta {
                                        reset: false,
                                        publishable: true,
                                        added: Vec::new(),
                                        updated: Vec::new(),
                                        removed: Vec::new(),
                                        terminal_operations: update.terminal_operations,
                                        terminal_layout: update.terminal_layout,
                                        settled,
                                        tier: snapshot_tier,
                                    };
                                    state_ref.settled = settled;
                                    retained.push(Rc::downgrade(&state));
                                    if state_ref.sender.unbounded_send(event).is_ok() {
                                        changed += 1;
                                    }
                                    continue;
                                }
                                let Some(maintained) = maintained_subscription.as_ref() else {
                                    return Err(Error::new(
                                        ErrorCode::Protocol,
                                        "structured subscription lost its Groove terminal",
                                    ));
                                };
                                let materialized = node
                                    .lock()
                                    .await
                                    .materialize_local_maintained_relation_snapshot_with_occurrences(
                                        maintained,
                                    )
                                    .await?;
                                let snapshot = materialized.snapshot;
                                let current_root_occurrences = materialized.root_occurrence_ids;
                                let settled = subscription_is_settled(
                                    &node.borrow(),
                                    active_authority_view_receipts,
                                    &shape,
                                    &binding,
                                    settled_tier,
                                    read_view,
                                    remote_propagate_upstream,
                                    requires_authority_receipt,
                                );
                                let state_ref = &mut *state_ref;
                                let previous_root_occurrences = snapshot_root_occurrences(
                                    &state_ref.snapshot,
                                    &state_ref.snapshot_index,
                                )?;
                                let event = subscription_terminal_delta_event(
                                    snapshot_tier,
                                    settled,
                                    &state_ref.snapshot,
                                    &previous_root_occurrences,
                                    &snapshot,
                                    &current_root_occurrences,
                                )?;
                                state_ref.snapshot = relation_snapshot_with_delta_slack(&snapshot);
                                state_ref.snapshot_index =
                                    relation_snapshot_index_with_root_occurrences(
                                        &state_ref.snapshot,
                                        &current_root_occurrences,
                                    )?;
                                state_ref.snapshot_source =
                                    SubscriptionSnapshotSource::LocalMaintained;
                                state_ref.settled = settled;
                                retained.push(Rc::downgrade(&state));
                                if state_ref.sender.unbounded_send(event).is_ok() {
                                    changed += 1;
                                }
                                continue;
                            } else {
                                let state_ref = &mut *state_ref;
                                let previous_snapshot = state_ref.snapshot.clone();
                                let previous_snapshot_index = state_ref.snapshot_index.clone();
                                let authoritative_membership_changed =
                                    update.authoritative_membership_changed;
                                let mut event = apply_maintained_update_to_snapshot(
                                    &mut state_ref.snapshot,
                                    &mut state_ref.snapshot_index,
                                    update,
                                    snapshot_tier,
                                    previous_settled,
                                    terminal_rows,
                                );
                                if authoritative_membership_changed {
                                    order_maintained_snapshot_roots(
                                        &node.borrow(),
                                        &shape.query(),
                                        &mut state_ref.snapshot,
                                        &mut state_ref.snapshot_index,
                                    )?;
                                    // Authority reconciliation carries row
                                    // additions/removals without positions.
                                    // Re-publish the first changed ordered
                                    // suffix so consumers apply TopBy order.
                                    event = subscription_terminal_delta_event(
                                        snapshot_tier,
                                        previous_settled,
                                        &previous_snapshot,
                                        &snapshot_root_occurrences(
                                            &previous_snapshot,
                                            &previous_snapshot_index,
                                        )?,
                                        &state_ref.snapshot,
                                        &snapshot_root_occurrences(
                                            &state_ref.snapshot,
                                            &state_ref.snapshot_index,
                                        )?,
                                    )?;
                                }
                                state_ref.snapshot_source =
                                    SubscriptionSnapshotSource::LocalMaintained;
                                let settled = subscription_is_settled(
                                    &node.borrow(),
                                    active_authority_view_receipts,
                                    &shape,
                                    &binding,
                                    settled_tier,
                                    read_view,
                                    remote_propagate_upstream,
                                    requires_authority_receipt,
                                ) && node
                                    .borrow()
                                    .relation_snapshot_has_materialized_required_cells(
                                        shape.query(),
                                        &state_ref.snapshot,
                                    )?;
                                state_ref.settled = settled;
                                retained.push(Rc::downgrade(&state));
                                if let SubscriptionEvent::Delta {
                                    settled: event_settled,
                                    ..
                                } = &mut event
                                {
                                    *event_settled = settled;
                                }
                                if state_ref.sender.unbounded_send(event).is_ok() {
                                    changed += 1;
                                }
                                continue;
                            }
                        }
                        let preserve_local_overlay = suppressed_authoritative_change;
                        let (snapshot, snapshot_source) = if terminal_rows {
                            (
                                state_ref.snapshot.clone(),
                                SubscriptionSnapshotSource::LocalMaintained,
                            )
                        } else if preserve_local_overlay {
                            (state_ref.snapshot.clone(), previous_source)
                        } else if remote_settled_tier.is_some() {
                            let previous = state_ref.snapshot.clone();
                            if (previous.root_count == 0 && previous.edges.is_empty()
                                || !read_view.is_default())
                                && node
                                    .borrow()
                                    .has_settled_result_set(authoritative_reset_binding_view)
                            {
                                let authoritative_snapshot = {
                                    let mut node_ref = node.lock().await;
                                    match node_ref
                                        .authoritative_reset_snapshot_for_binding_view(
                                            &shape,
                                            authoritative_reset_binding_view,
                                        )
                                        .await
                                    {
                                        Ok(snapshot) => snapshot,
                                        Err(crate::node::Error::MissingTransaction(_)) => {
                                            node_ref
                                                .record_authoritative_reset_missing_payload_fallback();
                                            node_ref.defer_authoritative_reset_for_binding_view(
                                                authoritative_reset_binding_view,
                                            );
                                            None
                                        }
                                        Err(error) => return Err(error.into()),
                                    }
                                };
                                if let Some(snapshot) = authoritative_snapshot {
                                    (snapshot, SubscriptionSnapshotSource::LinkSnapshot)
                                } else {
                                    let fallback = {
                                        let mut node_ref = node.lock().await;
                                        match node_ref
                                            .subscription_snapshot_in_authorization_mode(
                                                &shape,
                                                &binding,
                                                snapshot_tier,
                                                author,
                                                &read_view,
                                                authorization_mode,
                                            )
                                            .await
                                        {
                                            Ok(snapshot) => snapshot,
                                            Err(crate::node::Error::MissingTransaction(_)) => {
                                                node_ref
                                                    .record_authoritative_reset_missing_payload_fallback();
                                                node_ref
                                                    .defer_authoritative_reset_for_binding_view(
                                                        authoritative_reset_binding_view,
                                                    );
                                                retained.push(Rc::downgrade(&state));
                                                continue;
                                            }
                                            Err(error) => return Err(error.into()),
                                        }
                                    };
                                    (fallback, SubscriptionSnapshotSource::LinkSnapshot)
                                }
                            } else {
                                let remote_snapshot = {
                                    let mut node_ref = node.lock().await;
                                    match node_ref
                                        .subscription_snapshot_in_authorization_mode(
                                            &shape,
                                            &binding,
                                            snapshot_tier,
                                            author,
                                            &read_view,
                                            authorization_mode,
                                        )
                                        .await
                                    {
                                        Ok(snapshot) => snapshot,
                                        Err(crate::node::Error::MissingTransaction(_)) => {
                                            node_ref
                                                .record_authoritative_reset_missing_payload_fallback();
                                            node_ref.defer_authoritative_reset_for_binding_view(
                                                authoritative_reset_binding_view,
                                            );
                                            retained.push(Rc::downgrade(&state));
                                            continue;
                                        }
                                        Err(error) => return Err(error.into()),
                                    }
                                };
                                (remote_snapshot, SubscriptionSnapshotSource::LinkSnapshot)
                            }
                        } else if has_maintained_subscription {
                            let previous = state_ref.snapshot.clone();
                            (previous.clone(), previous_source)
                        } else {
                            (
                                node.lock()
                                    .await
                                    .subscription_snapshot_in_authorization_mode(
                                        &shape,
                                        &binding,
                                        read_tier,
                                        author,
                                        &read_view,
                                        authorization_mode,
                                    )
                                    .await?,
                                SubscriptionSnapshotSource::LinkSnapshot,
                            )
                        };
                        let settled = subscription_is_settled(
                            &node.borrow(),
                            active_authority_view_receipts,
                            &shape,
                            &binding,
                            settled_tier,
                            read_view,
                            remote_propagate_upstream,
                            requires_authority_receipt,
                        );
                        (
                            snapshot,
                            snapshot_source,
                            settled,
                            snapshot_tier,
                            preserve_local_overlay,
                        )
                    }
                }
            }
        };
        let (previous, previous_source, has_maintained_flat_tuple_subscription) = {
            let state = state.borrow();
            (
                state.snapshot.clone(),
                state.snapshot_source,
                matches!(
                    &state.kind,
                    SubscriptionKind::Prepared {
                        shape,
                        maintained_subscription: Some(_),
                        ..
                    } if shape.query().flat_join.is_some()
                ),
            )
        };
        // A link snapshot is a relation facade without the terminal's flat
        // tuple occurrence sidecar. Once a maintained terminal owns this
        // stream, replace it from the terminal rather than installing an
        // unaddressable facade snapshot.
        if !force_reset_event
            && has_maintained_flat_tuple_subscription
            && previous_source == SubscriptionSnapshotSource::LocalMaintained
            && snapshot_source == SubscriptionSnapshotSource::LinkSnapshot
        {
            let materialized = {
                let state = state.borrow();
                let SubscriptionKind::Prepared {
                    maintained_subscription: Some(maintained),
                    ..
                } = &state.kind
                else {
                    unreachable!("checked maintained subscription above");
                };
                node.lock()
                    .await
                    .materialize_local_maintained_relation_snapshot_with_occurrences(maintained)
                    .await?
            };
            snapshot = materialized.snapshot;
            snapshot_source = SubscriptionSnapshotSource::LocalMaintained;
        }
        if force_reset_event || snapshot != previous || settled != previous_settled {
            let mut state = state.borrow_mut();
            let event = if force_reset_event {
                subscription_delta_event_with_reset(
                    snapshot_tier,
                    settled,
                    &previous,
                    &snapshot,
                    true,
                    terminal_rows,
                )
            } else {
                subscription_delta_event(
                    snapshot_tier,
                    settled,
                    &previous,
                    &snapshot,
                    terminal_rows,
                )
            };
            state.snapshot = relation_snapshot_with_delta_slack(&snapshot);
            state.snapshot_index = maintained_snapshot_index_or_row_index(
                &mut node.borrow_mut(),
                &state.kind,
                &state.snapshot,
            )?;
            state.snapshot_source = snapshot_source;
            state.settled = settled;
            if state.sender.unbounded_send(event).is_ok() {
                changed += 1;
            }
        }
        retained.push(Rc::downgrade(&state));
    }
    for pending in pending_authoritative_resets.difference(&consumed_authoritative_resets) {
        node.borrow_mut()
            .defer_authoritative_reset_for_binding_view(*pending);
    }
    *subscriptions.borrow_mut() = retained;
    Ok(changed)
}

pub(super) fn register_upstream_subscription_owner(
    owners: &UpstreamSubscriptionOwners,
    handles: &[UpstreamCoverageHandle],
    state: &Rc<RefCell<SubscriptionState>>,
) {
    let weak = Rc::downgrade(state);
    let mut owners = owners.borrow_mut();
    for handle in handles {
        owners
            .entry(handle.subscription)
            .or_default()
            .push(weak.clone());
    }
}

pub(super) fn unregister_upstream_subscription_owner(
    owners: &UpstreamSubscriptionOwners,
    subscription: SubscriptionKey,
    state: &Weak<RefCell<SubscriptionState>>,
) {
    let mut owners = owners.borrow_mut();
    let Some(entries) = owners.get_mut(&subscription) else {
        return;
    };
    entries.retain(|entry| !entry.ptr_eq(state) && entry.strong_count() > 0);
    if entries.is_empty() {
        owners.remove(&subscription);
    }
}

pub(super) fn route_upstream_subscription_rejection(
    subscriptions: &SubscriptionList,
    owners: &UpstreamSubscriptionOwners,
    subscription: SubscriptionKey,
    reason: SubscribeRejectReason,
) -> usize {
    let mut delivered = 0;
    if let Some(entries) = owners.borrow_mut().get_mut(&subscription) {
        entries.retain(|entry| entry.strong_count() > 0);
        for state in entries.iter().filter_map(Weak::upgrade) {
            let event = SubscriptionEvent::Rejected {
                reason: reason.clone(),
            };
            if state.borrow().sender.unbounded_send(event).is_ok() {
                delivered += 1;
            }
        }
        if delivered > 0 {
            return delivered;
        }
    }

    for state in subscriptions.borrow().iter().filter_map(Weak::upgrade) {
        let state_ref = state.borrow();
        if !state_ref.propagates_upstream {
            continue;
        }
        let SubscriptionKind::Prepared { shape, binding, .. } = &state_ref.kind;
        let read_view = RegisterShapeOptions {
            tier: state_ref.remote_read_tier.unwrap_or(state_ref.read_tier),
            read_view: state_ref.read_view.clone(),
            propagate_upstream: state_ref.remote_propagate_upstream,
        }
        .read_view_key();
        if shape.shape_id() != subscription.shape_id || read_view != subscription.read_view {
            continue;
        }
        if subscription.binding_id != BindingId(uuid::Uuid::nil())
            && binding.binding_id() != subscription.binding_id
        {
            continue;
        }
        let event = SubscriptionEvent::Rejected {
            reason: reason.clone(),
        };
        if state_ref.sender.unbounded_send(event).is_ok() {
            delivered += 1;
        }
    }
    delivered
}

/// Binding-supplied transport for one peer link.
///
/// The `Db` writes outbound messages with [`Transport::send`] and pulls inbound
/// ones with [`Transport::try_recv`]; the binding owns the actual socket and
/// scheduling and bridges these to real I/O on its own runtime. Both methods are
/// non-blocking — `try_recv` returning `None` means "nothing staged right now,"
/// not "closed" (a disconnect surface lands with a later B slice). This is the
/// single seam that keeps the async boundary *between* nodes, never inside `Db`.
pub trait Transport {
    /// Hand an outbound message to the binding's wire.
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError>;
    /// Pull the next inbound message the binding has staged, if any.
    fn try_recv(&mut self) -> Option<SyncMessage>;

    /// Immutable endpoint facts accepted by authenticated session admission.
    /// Semantic messages never self-assert this context.
    fn connection_session_context(&self) -> Option<ConnectionSessionContext> {
        None
    }
}

/// Handshake/session facts for one accepted connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionSessionContext {
    /// This endpoint's authority identity and fresh epoch.
    pub local: WireAuthorityEndpoint,
    /// Authenticated remote authority identity and fresh epoch.
    pub remote: WireAuthorityEndpoint,
    /// Authenticated session identity terminated by this link.
    pub link_identity: AuthorId,
    /// Features accepted for this connection.
    pub negotiated_features: WireFeatures,
}
