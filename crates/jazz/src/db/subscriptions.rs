//! Subscription opening, query attachment, coverage, and cleanup.

use super::*;

impl<S> Db<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Subscribe to a query and return a stream of materialized subscription events.
    ///
    /// ```rust
    /// # use jazz::db::{LocalUpdates, Propagation, ReadOpts, SubscriptionEvent};
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::tx::DurabilityTier;
    /// let db = block_on(open_todos_db())?;
    /// let query = db.prepare_query(&db.table("todos"))?;
    /// let mut subscription = block_on(db.subscribe(
    ///     &query,
    ///     ReadOpts {
    ///         tier: DurabilityTier::Local,
    ///         local_updates: LocalUpdates::Immediate,
    ///         propagation: Propagation::LocalOnly,
    ///         include_deleted: false,
    ///         ..ReadOpts::default()
    ///     },
    /// ))?;
    /// let opened = block_on(subscription.next_event()).unwrap();
    /// let SubscriptionEvent::Delta { reset, added, .. } = opened else {
    ///     panic!("expected reset delta");
    /// };
    /// assert!(reset);
    /// assert!(added.is_empty());
    ///
    /// block_on(db.insert(
    ///     "todos",
    ///     todo_cells("notify subscribers", false),
    ///     Default::default(),
    /// ))?;
    /// let changed = block_on(subscription.next_event()).unwrap();
    /// let SubscriptionEvent::Delta { added, updated, removed, .. } = changed else {
    ///     panic!("expected subscription delta");
    /// };
    /// assert_eq!(added.len(), 1);
    /// assert!(updated.is_empty());
    /// assert!(removed.is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn subscribe(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<SubscriptionStream, Error> {
        self.open_subscription(
            prepared,
            opts,
            self.identity.author,
            QueryAuthorizationMode::ClientLocal,
        )
        .await
    }

    /// Subscribe to a query evaluated as `author`.
    pub async fn subscribe_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorSubject,
    ) -> Result<SubscriptionStream, Error> {
        self.open_subscription(
            prepared,
            opts,
            author,
            QueryAuthorizationMode::TrustedServing,
        )
        .await
    }

    /// Subscribe to an output-changing relation query.
    pub async fn subscribe_relation_query(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
    ) -> Result<SubscriptionStream, Error> {
        self.open_relation_subscription(
            query,
            opts,
            self.identity.author,
            QueryAuthorizationMode::ClientLocal,
        )
        .await
    }

    /// Subscribe to an output-changing relation query evaluated as `author`.
    pub async fn subscribe_relation_query_for_identity(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: AuthorSubject,
    ) -> Result<SubscriptionStream, Error> {
        self.open_relation_subscription(query, opts, author, QueryAuthorizationMode::TrustedServing)
            .await
    }

    /// Attach a one-shot usage-site query coverage request.
    ///
    /// Bindings call this before an edge/global one-shot read, drive
    /// [`Db::tick`] until [`Db::query_attachment_is_covered`] is true, read, then
    /// call [`Db::detach_query`].
    pub fn attach_query_with_opts(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<QueryAttachment, Error> {
        ensure_supported_read_view(&opts)?;
        let upstream_opts = self.node.upstream_register_shape_options(
            effective_read_tier(&opts),
            opts.read_view.clone(),
            opts.propagation == Propagation::Full,
        );
        self.attach_or_refresh_query_coverage(
            &prepared.shape,
            &prepared.binding,
            upstream_opts,
            self.identity.author,
        )
    }

    /// Attach a one-shot usage-site query coverage request evaluated as `author`.
    pub fn attach_query_with_opts_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorSubject,
    ) -> Result<QueryAttachment, Error> {
        ensure_supported_read_view(&opts)?;
        let upstream_opts = self.node.upstream_register_shape_options(
            effective_read_tier(&opts),
            opts.read_view.clone(),
            opts.propagation == Propagation::Full,
        );
        let (shape, binding, _) =
            super::block_on(self.node.node.borrow_mut().prepare_query_binding_for_link(
                &prepared.shape,
                &prepared.binding,
                upstream_opts.tier,
                author,
            ))?;
        self.attach_or_refresh_query_coverage(&shape, &binding, upstream_opts, author)
    }

    fn attach_or_refresh_query_coverage(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        upstream_opts: RegisterShapeOptions,
        identity: AuthorSubject,
    ) -> Result<QueryAttachment, Error> {
        let requires_current_authority_receipt = upstream_opts.tier >= DurabilityTier::Edge;
        let binding_view = BindingViewKey::new(
            shape.shape_id(),
            binding.binding_id(),
            upstream_opts.read_view_key(),
        );
        let required_after = self
            .node
            .node
            .borrow()
            .applied_view_update_generation(binding_view);
        let coverage = coverage_key(shape, binding, upstream_opts.clone());
        // Edge/Global one-shots may borrow a still-live maintained stream:
        // refreshing that exact wire subscription cannot be confused with a
        // detached predecessor. Otherwise they own a fresh subscription key.
        // Local-only usage retains the existing coverage-reuse semantics.
        if self
            .node
            .upstream_coverage_refcounts
            .borrow()
            .contains_key(&coverage)
            && let Some(subscription) = self
                .node
                .latest_coverage_subscriptions
                .borrow()
                .get(&coverage)
                .copied()
            && !self
                .node
                .query_coverage_registrations
                .borrow()
                .contains_key(&subscription)
            && (!requires_current_authority_receipt
                || self
                    .node
                    .upstream_subscription_owners
                    .borrow()
                    .get(&subscription)
                    .is_some_and(|owners| owners.iter().any(|owner| owner.strong_count() > 0)))
        {
            *self
                .node
                .upstream_coverage_refcounts
                .borrow_mut()
                .entry(coverage.clone())
                .or_insert(0) += 1;
            let pending_subscription = PendingUpstreamSubscription {
                subscription,
                shape: shape.clone(),
                binding: binding.clone(),
                opts: upstream_opts.clone(),
                identity,
                policy_binding: None,
            };
            self.register_query_coverage(coverage.clone(), pending_subscription.clone(), false);
            let mut refreshes = self.node.coverage_refresh_generations.borrow_mut();
            if refreshes.get(&coverage).copied() != Some(required_after) {
                refreshes.insert(coverage.clone(), required_after);
                self.node
                    .upstream_subscriptions
                    .borrow_mut()
                    .push(PendingUpstreamCommand::Subscribe(pending_subscription));
                self.node.schedule_tick(TickUrgency::Immediate);
            }
            return Ok(QueryAttachment {
                subscriptions: vec![subscription],
                required_after: vec![(binding_view, required_after)],
                requires_current_authority_receipt,
                registrations: vec![subscription],
                refreshes: vec![(coverage, required_after)],
            });
        }
        let subscription = self.attach_query_shape_binding_with_opts(
            shape,
            binding,
            upstream_opts.clone(),
            identity,
        )?;
        *self
            .node
            .upstream_coverage_refcounts
            .borrow_mut()
            .entry(coverage.clone())
            .or_insert(0) += 1;
        self.register_query_coverage(
            coverage.clone(),
            PendingUpstreamSubscription {
                subscription,
                shape: shape.clone(),
                binding: binding.clone(),
                opts: upstream_opts,
                identity,
                policy_binding: None,
            },
            true,
        );
        Ok(QueryAttachment {
            subscriptions: vec![subscription],
            required_after: vec![(binding_view, required_after)],
            requires_current_authority_receipt,
            registrations: vec![subscription],
            refreshes: Vec::new(),
        })
    }

    fn register_query_coverage(
        &self,
        coverage: CoverageKey,
        subscription: PendingUpstreamSubscription,
        owns_subscription: bool,
    ) {
        let mut registrations = self.node.query_coverage_registrations.borrow_mut();
        registrations
            .entry(subscription.subscription)
            .and_modify(|registration| registration.ref_count += 1)
            .or_insert(QueryCoverageRegistration {
                coverage,
                subscription,
                owns_subscription,
                ref_count: 1,
            });
    }

    fn attach_query_shape_binding_with_opts(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
        identity: AuthorSubject,
    ) -> Result<SubscriptionKey, Error> {
        let subscription = self.node.next_subscription_key(shape, opts.read_view_key());
        self.node
            .upstream_subscriptions
            .borrow_mut()
            .push(PendingUpstreamCommand::Subscribe(
                PendingUpstreamSubscription {
                    subscription,
                    shape: shape.clone(),
                    binding: binding.clone(),
                    opts: opts.clone(),
                    identity,
                    policy_binding: None,
                },
            ));
        self.node
            .latest_coverage_subscriptions
            .borrow_mut()
            .insert(coverage_key(shape, binding, opts), subscription);
        self.node.schedule_tick(TickUrgency::Immediate);
        Ok(subscription)
    }

    /// Attach a one-shot usage-site query coverage request at the default tier.
    pub fn attach_query(&self, prepared: &PreparedQuery) -> Result<QueryAttachment, Error> {
        self.attach_query_with_opts(prepared, ReadOpts::default())
    }

    /// Return whether each usage-site attachment has observed a newer logical
    /// server receipt than the one it captured during registration.
    pub fn query_attachment_is_covered(&self, attachment: &QueryAttachment) -> bool {
        let node = self.node.node.borrow();
        let active_receipts = self.node.active_authority_view_receipts.borrow();
        let has_current_authority_receipt = active_receipts.as_ref().is_some_and(|receipts| {
            attachment
                .required_after
                .iter()
                .all(|(binding_view, _)| receipts.binding_views.contains(binding_view))
                && attachment
                    .subscriptions
                    .iter()
                    .all(|subscription| receipts.subscriptions.contains(subscription))
        });
        let covered = attachment
            .required_after
            .iter()
            .all(|(binding_view, required_after)| {
                node.applied_view_update_generation(*binding_view) > *required_after
                    && !node.opening_pending_for_binding_view(*binding_view)
            })
            && (!attachment.requires_current_authority_receipt || has_current_authority_receipt);
        drop(node);
        drop(active_receipts);
        if covered {
            let mut refreshes = self.node.coverage_refresh_generations.borrow_mut();
            for (coverage, generation) in &attachment.refreshes {
                if refreshes.get(coverage).copied() == Some(*generation) {
                    refreshes.remove(coverage);
                }
            }
        }
        covered
    }

    #[cfg(any(test, feature = "testing"))]
    /// Test-only counts of live coverage groups and usage-site registrations.
    pub fn query_coverage_attachment_counts_for_test(&self) -> (usize, usize) {
        (
            self.node.upstream_coverage_refcounts.borrow().len(),
            self.node.query_coverage_registrations.borrow().len(),
        )
    }

    #[cfg(any(test, feature = "testing"))]
    /// Internal receipt-lifetime coverage needs to inspect state that has no
    /// public equivalent: detached Local overlays are intentionally best-effort.
    pub fn settled_authoritative_receipt_counts_for_test(&self) -> (usize, usize) {
        self.node
            .node
            .borrow()
            .settled_authoritative_receipt_counts_for_test()
    }

    #[cfg(any(test, feature = "testing"))]
    /// Hold the async node owner until this future is cancelled.
    ///
    /// This is a test-only suspension point for cancellation and contention
    /// contracts that cannot be reproduced by borrowing the private node.
    pub async fn hold_node_owner_for_test(&self) {
        let _node = self.node.node.lock().await;
        std::future::pending::<()>().await;
    }

    /// Detach a one-shot query coverage request.
    pub fn detach_query(&self, attachment: QueryAttachment) {
        if let Some(receipts) = self
            .node
            .active_authority_view_receipts
            .borrow_mut()
            .as_mut()
        {
            for subscription in &attachment.registrations {
                receipts.subscriptions.remove(subscription);
            }
        }
        let mut removed_subscriptions = Vec::new();
        let mut registrations = self.node.query_coverage_registrations.borrow_mut();
        for subscription in attachment.registrations {
            let Some(registration) = registrations.get_mut(&subscription) else {
                continue;
            };
            let coverage = registration.coverage.clone();
            let owns_subscription = registration.owns_subscription;
            registration.ref_count = registration.ref_count.saturating_sub(1);
            let last_registration = registration.ref_count == 0;
            if last_registration {
                registrations.remove(&subscription);
            }
            let mut coverage_refcounts = self.node.upstream_coverage_refcounts.borrow_mut();
            let Some(count) = coverage_refcounts.get_mut(&coverage) else {
                continue;
            };
            *count = count.saturating_sub(1);
            let last_coverage_pin = *count == 0;
            if last_coverage_pin {
                coverage_refcounts.remove(&coverage);
                self.node
                    .awaiting_initial_authority_coverage
                    .borrow_mut()
                    .remove(&coverage);
            }
            let has_live_stream_owner = self
                .node
                .upstream_subscription_owners
                .borrow()
                .get(&subscription)
                .is_some_and(|owners| owners.iter().any(|owner| owner.strong_count() > 0));
            if (owns_subscription && last_registration && !has_live_stream_owner)
                || last_coverage_pin
            {
                removed_subscriptions.push((subscription, coverage));
            }
        }
        drop(registrations);
        for (subscription, coverage) in removed_subscriptions {
            self.node.node.borrow_mut().apply_unsubscribe(subscription);
            let replacement = self
                .node
                .query_coverage_registrations
                .borrow()
                .values()
                .find(|registration| registration.coverage == coverage)
                .map(|registration| registration.subscription.subscription);
            let mut latest = self.node.latest_coverage_subscriptions.borrow_mut();
            if latest.get(&coverage) == Some(&subscription) {
                if let Some(replacement) = replacement {
                    latest.insert(coverage.clone(), replacement);
                } else {
                    latest.remove(&coverage);
                }
            }
            drop(latest);
            self.node
                .upstream_subscriptions
                .borrow_mut()
                .push(PendingUpstreamCommand::Unsubscribe(subscription));
        }
        self.node.schedule_tick(TickUrgency::Immediate);
    }

    async fn open_subscription(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorSubject,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<SubscriptionStream, Error> {
        ensure_supported_subscription_read_opts(&opts)?;
        self.validate_prepared_shape_for_registration(prepared)
            .await?;
        let requested_read_tier = effective_read_tier(&opts);
        let authored_commit_durability = self.node.node.lock().await.authored_commit_durability();
        let read_tier = if opts.local_updates == LocalUpdates::Immediate
            && opts.propagation == Propagation::Full
            && supports_pending_overlay_reconciliation(prepared.shape.query())
            && authored_commit_durability == DurabilityTier::None
        {
            DurabilityTier::Local
        } else {
            requested_read_tier
        };
        self.node
            .node
            .lock()
            .await
            .ensure_peer_maintained_subscription_view_supported(
                &prepared.shape,
                &prepared.binding,
                read_tier,
                author,
                &opts.read_view,
                authorization_mode,
            )
            .await?;
        let (local_shape, local_binding, _local_plan) = self
            .node
            .node
            .lock()
            .await
            .prepare_query_binding_for_link_in_authorization_mode(
                &prepared.shape,
                &prepared.binding,
                read_tier,
                author,
                authorization_mode,
            )
            .await?;
        // The subscription opener performs one bounded IVM poll. Keep the
        // current host scheduler as the cold-storage continuation owner,
        // rather than the short-lived foreground future opening this stream.
        let progress_waker = self.node.query_runtime_waker();
        let (subscription, snapshot) = self
            .node
            .node
            .lock()
            .await
            .open_maintained_view_subscription_in_authorization_mode_with_waker(
                &local_shape,
                &local_binding,
                author,
                read_tier,
                &opts.read_view,
                Some(_local_plan),
                authorization_mode,
                progress_waker.as_ref(),
            )
            .await?;
        let root_occurrence_ids = subscription.root_occurrence_ids().to_vec();
        let local_subscription_id = subscription.subscription_id();
        let local_node = Rc::clone(&self.node.node);
        let local_runtime_token = local_node.lock().await.groove_runtime_token();
        let local_subscription_cleanup = Rc::new(Cell::new(Some((
            local_runtime_token,
            local_subscription_id,
        ))));
        let local_cleanup_handle = Rc::clone(&local_subscription_cleanup);
        let local_cleanup_node = Rc::clone(&self.node);
        let mut local_cleanup = CleanupGuard::new(Box::new(move || {
            // Opening failed before a public state existed; this is the one
            // intentionally ID-based cleanup path.
            local_cleanup_node.enqueue_subscription_finalization(PendingSubscriptionFinalization {
                state: None,
                opening_local: local_cleanup_handle.take(),
                acknowledgement: None,
            });
        }));
        // A projected ordered root needs terminal patches even without nested
        // arrays: an unprojected sort-key mutation can move a visible row
        // without changing the projected payload. Unprojected roots retain
        // ordinary row deltas, including scope re-entry membership changes.
        let terminal_rows = !local_shape.query().array_subqueries.is_empty()
            || (local_shape.query().select.is_some() && !local_shape.query().order_by.is_empty());
        let mut maintained_subscription = Some(subscription);
        let mut state_shape = local_shape;
        let mut state_binding = local_binding;
        let mut remote_read_tier = None;
        let mut requires_authority_receipt = false;
        let mut upstream_subscription_handles = Vec::new();
        let mut suppress_provisional_opening = false;
        let remote_propagate_upstream = opts.propagation == Propagation::Full;
        // A non-durable browser client must still ask its durable worker for a
        // local-only view. The wire flag stops that request at the worker.
        let propagates_upstream = remote_propagate_upstream
            || self.node.upstream_durability_floor.get() == DurabilityTier::Local;
        if propagates_upstream {
            let upstream_opts = self.node.upstream_register_shape_options(
                requested_read_tier,
                opts.read_view.clone(),
                remote_propagate_upstream,
            );
            let (shape, binding) = if upstream_opts.tier == read_tier {
                (state_shape.clone(), state_binding.clone())
            } else {
                let (shape, binding, _) = self
                    .node
                    .node
                    .lock()
                    .await
                    .prepare_query_binding_for_link_in_authorization_mode(
                        &prepared.shape,
                        &prepared.binding,
                        upstream_opts.tier,
                        author,
                        authorization_mode,
                    )
                    .await?;
                (shape, binding)
            };
            state_shape = shape.clone();
            state_binding = binding.clone();
            remote_read_tier = Some(upstream_opts.tier);
            // Edge/Global cache possession is never a settlement receipt,
            // even when this subscription opens before an upstream exists.
            // The eventual connection must send its own ViewUpdate.
            requires_authority_receipt = upstream_opts.tier >= DurabilityTier::Edge;
            let opened = self.open_subscription_upstream_coverage(
                &shape,
                &binding,
                upstream_opts,
                author,
                authorization_mode,
            )?;
            upstream_subscription_handles = opened.handles;
            suppress_provisional_opening = authorization_mode
                == QueryAuthorizationMode::ClientLocal
                && requested_read_tier >= DurabilityTier::Edge
                && opened.awaits_initial_authority_response
                && snapshot.root_count == 0
                && snapshot.edges.is_empty();
        }
        let settled_tier = remote_read_tier.unwrap_or(read_tier);
        if authorization_mode == QueryAuthorizationMode::ClientLocal
            && remote_read_tier.is_some()
            && state_shape.query().aggregate.is_none()
        {
            let binding_view_key = BindingViewKey {
                shape_id: state_shape.shape_id(),
                binding_id: state_binding.binding_id(),
                read_view: RegisterShapeOptions {
                    tier: settled_tier,
                    read_view: opts.read_view.clone(),
                    propagate_upstream: remote_propagate_upstream,
                    ..RegisterShapeOptions::default()
                }
                .read_view_key(),
            };
            if let Some(maintained) = maintained_subscription.as_mut() {
                self.node
                    .node
                    .lock()
                    .await
                    .seed_local_maintained_authoritative_generation(maintained, binding_view_key);
            }
        }
        let settled = {
            let node = self.node.node.lock().await;
            subscription_is_settled(
                &node,
                &self.node.active_authority_view_receipts,
                &state_shape,
                &state_binding,
                settled_tier,
                opts.read_view.clone(),
                remote_propagate_upstream,
                requires_authority_receipt,
            )
        };
        // An empty local opening carries no observable result information at
        // an Edge/Global request.  Until the authority replies, publishing it
        // would let a public subscription report a provisional empty view as
        // its first delivery.  `awaits_initial_authority_response` is only
        // known while opening a fresh upstream handle, but an already-open
        // link has the same receipt requirement.
        suppress_provisional_opening |= authorization_mode == QueryAuthorizationMode::ClientLocal
            && requested_read_tier >= DurabilityTier::Edge
            && remote_read_tier.is_some()
            && !settled
            && snapshot.root_count == 0
            && snapshot.edges.is_empty();
        let (sender, receiver) = unbounded();
        let initial_outputs =
            subscription_outputs_with_occurrence_sidecar(&snapshot, &root_occurrence_ids)?;
        let state_snapshot = relation_snapshot_with_delta_slack(&snapshot);
        let mut snapshot_index = RelationSnapshotIndex::from_snapshot(&state_snapshot);
        snapshot_index.roots = root_occurrence_ids
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, occurrence)| (occurrence, index))
            .collect();
        let closed = Rc::new(Cell::new(false));
        let state = Rc::new(RefCell::new(SubscriptionState {
            closed: Rc::clone(&closed),
            terminal_rows,
            kind: SubscriptionKind::Prepared {
                shape: state_shape,
                binding: state_binding,
                maintained_subscription,
            },
            groove_runtime_token: self.node.node.lock().await.groove_runtime_token(),
            local_subscription_cleanup: Rc::clone(&local_subscription_cleanup),
            upstream_subscription_handles,
            propagates_upstream,
            author,
            authorization_mode,
            read_tier,
            remote_read_tier,
            requires_authority_receipt,
            remote_propagate_upstream,
            read_view: opts.read_view.clone(),
            snapshot: state_snapshot,
            snapshot_index,
            snapshot_source: SubscriptionSnapshotSource::LocalMaintained,
            settled,
            sender,
        }));
        state
            .borrow()
            .sender
            .unbounded_send(SubscriptionEvent::Delta {
                reset: true,
                publishable: !suppress_provisional_opening,
                added: initial_outputs,
                updated: Vec::new(),
                removed: Vec::new(),
                terminal_operations: Vec::new(),
                settled,
                tier: read_tier,
            })
            .map_err(|_| Error::new(ErrorCode::Protocol, "subscription receiver closed"))?;
        self.node
            .subscriptions
            .borrow_mut()
            .push(Rc::downgrade(&state));
        // The guard covers fallible opening after the local maintained view
        // exists. On success, replace it with one command carrying local and
        // upstream cleanup so Drop never touches the async node mutex.
        drop(local_cleanup.take());
        let cleanup: SubscriptionCleanup = {
            register_upstream_subscription_owner(
                &self.node.upstream_subscription_owners,
                &state.borrow().upstream_subscription_handles,
                &state,
            );
            let node = Rc::clone(&self.node);
            let state = Rc::clone(&state);
            Box::new(move |acknowledgement| {
                closed.set(true);
                let finalization_node = acknowledgement.as_ref().map(|_| Rc::clone(&node));
                node.enqueue_subscription_finalization(PendingSubscriptionFinalization {
                    state: Some(state),
                    opening_local: None,
                    acknowledgement,
                });
                finalization_node.map(|node| {
                    Box::pin(async move {
                        node.drain_subscription_finalizations().await?;
                        Ok(())
                    }) as SubscriptionFinalizationFuture
                })
            })
        };
        Ok(SubscriptionStream {
            receiver,
            _state: state,
            cleanup: Some(cleanup),
            finalization: None,
            terminated: false,
        })
    }

    async fn validate_prepared_shape_for_registration(
        &self,
        prepared: &PreparedQuery,
    ) -> Result<(), Error> {
        let ast = ShapeAst::from_validated(&prepared.shape);
        let validation = {
            let node = self.node.node.lock().await;
            validate_shape_ast_for_registration(&node, prepared.shape.shape_id(), &ast)
        };
        validation.map(|_| ()).map_err(Error::from)
    }

    async fn open_relation_subscription(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: AuthorSubject,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<SubscriptionStream, Error> {
        ensure_supported_subscription_read_opts(&opts)?;
        let query = relation_query_to_query(query)?;
        let prepared = self.prepare_query(&query)?;
        self.open_subscription(&prepared, opts, author, authorization_mode)
            .await
    }

    fn open_subscription_upstream_coverage(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
        identity: AuthorSubject,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<OpenedUpstreamCoverage, Error> {
        super::block_on(
            self.node
                .node
                .borrow_mut()
                .ensure_peer_maintained_subscription_view_supported(
                    shape,
                    binding,
                    opts.tier,
                    identity,
                    &opts.read_view,
                    authorization_mode,
                ),
        )?;
        let coverage = coverage_key(shape, binding, opts.clone());
        if self
            .node
            .upstream_coverage_refcounts
            .borrow()
            .contains_key(&coverage)
        {
            if let Some(subscription) = self
                .node
                .latest_coverage_subscriptions
                .borrow()
                .get(&coverage)
                .copied()
            {
                *self
                    .node
                    .upstream_coverage_refcounts
                    .borrow_mut()
                    .entry(coverage.clone())
                    .or_insert(0) += 1;
                let awaits_initial_authority_response = self
                    .node
                    .awaiting_initial_authority_coverage
                    .borrow()
                    .contains(&coverage);
                return Ok(OpenedUpstreamCoverage {
                    handles: vec![UpstreamCoverageHandle {
                        coverage,
                        subscription,
                    }],
                    awaits_initial_authority_response,
                });
            }
        }
        let subscription =
            self.attach_query_shape_binding_with_opts(shape, binding, opts, identity)?;
        *self
            .node
            .upstream_coverage_refcounts
            .borrow_mut()
            .entry(coverage.clone())
            .or_insert(0) += 1;
        let has_live_upstream = self
            .node
            .connections
            .borrow()
            .iter()
            .any(|connection| matches!(&connection.borrow().link, ConnectionLink::Upstream(_)));
        if has_live_upstream {
            self.node
                .awaiting_initial_authority_coverage
                .borrow_mut()
                .insert(coverage.clone());
        }
        Ok(OpenedUpstreamCoverage {
            handles: vec![UpstreamCoverageHandle {
                coverage,
                subscription,
            }],
            awaits_initial_authority_response: has_live_upstream,
        })
    }
}
