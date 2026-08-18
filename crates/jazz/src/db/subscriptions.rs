//! Subscription opening, query attachment, coverage, and cleanup.

use super::*;

pub(super) enum SubscriptionOpenError {
    Node(crate::node::Error),
    Api(Error),
}

impl From<crate::node::Error> for SubscriptionOpenError {
    fn from(error: crate::node::Error) -> Self {
        Self::Node(error)
    }
}

impl From<groove::storage::Error> for SubscriptionOpenError {
    fn from(error: groove::storage::Error) -> Self {
        Self::Node(crate::node::Error::Storage(error))
    }
}

impl SubscriptionOpenError {
    pub(super) fn into_api(self) -> Error {
        match self {
            Self::Node(error) => error.into(),
            Self::Api(error) => error,
        }
    }

    pub(super) fn missing_input(
        self,
    ) -> Result<Vec<groove::storage::async_ordered::OwnedStorageOperation>, Self> {
        match self {
            Self::Node(error) => crate::node::missing_node_open_input(error).map_err(Self::Node),
            error => Err(error),
        }
    }
}

impl<S> Node<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Attach a one-shot usage-site query coverage request.
    ///
    /// Bindings call this before an edge/global one-shot read, drive
    /// [`Db::tick`] until [`Db::query_attachment_is_covered`] is true, read, then
    /// call [`Db::detach_query`].
    /// Attach a one-shot usage-site query coverage request evaluated as `author`.
    pub(super) fn attach_query_with_opts(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<QueryAttachment, Error> {
        ensure_supported_read_view(&opts)?;
        let upstream_opts = self.upstream_register_shape_options(
            effective_read_tier(&opts),
            opts.read_view.clone(),
            opts.propagation == Propagation::Full,
        );
        let (shape, binding, _) = self.node.borrow_mut().prepare_query_binding_for_link(
            &prepared.shape,
            &prepared.binding,
            upstream_opts.tier,
            author,
        )?;
        self.attach_or_refresh_query_coverage(&shape, &binding, upstream_opts, author)
    }

    fn attach_or_refresh_query_coverage(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        upstream_opts: RegisterShapeOptions,
        identity: AuthorId,
    ) -> Result<QueryAttachment, Error> {
        let requires_current_authority_receipt = upstream_opts.tier >= DurabilityTier::Edge;
        let binding_view = BindingViewKey::new(
            shape.shape_id(),
            binding.binding_id(),
            upstream_opts.read_view_key(),
        );
        let required_after = self
            .node
            .borrow()
            .applied_view_update_generation(binding_view);
        let coverage = coverage_key(shape, binding, upstream_opts.clone());
        if self
            .upstream_coverage_refcounts
            .borrow()
            .contains_key(&coverage)
            && let Some(subscription) = self
                .latest_coverage_subscriptions
                .borrow()
                .get(&coverage)
                .copied()
            && !self
                .query_coverage_registrations
                .borrow()
                .contains_key(&subscription)
        {
            *self
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
            };
            self.register_query_coverage(coverage.clone(), pending_subscription.clone(), false);
            let mut refreshes = self.coverage_refresh_generations.borrow_mut();
            if refreshes.get(&coverage).copied() != Some(required_after) {
                refreshes.insert(coverage.clone(), required_after);
                self.upstream_subscriptions
                    .borrow_mut()
                    .push(PendingUpstreamCommand::Subscribe(pending_subscription));
                self.schedule_tick(TickUrgency::Immediate);
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
        let mut registrations = self.query_coverage_registrations.borrow_mut();
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
        identity: AuthorId,
    ) -> Result<SubscriptionKey, Error> {
        let subscription = self.next_subscription_key(shape, opts.read_view_key());
        self.upstream_subscriptions
            .borrow_mut()
            .push(PendingUpstreamCommand::Subscribe(
                PendingUpstreamSubscription {
                    subscription,
                    shape: shape.clone(),
                    binding: binding.clone(),
                    opts: opts.clone(),
                    identity,
                },
            ));
        self.latest_coverage_subscriptions
            .borrow_mut()
            .insert(coverage_key(shape, binding, opts), subscription);
        self.schedule_tick(TickUrgency::Immediate);
        Ok(subscription)
    }

    /// Return whether each usage-site attachment has observed a newer logical
    /// server receipt than the one it captured during registration.
    pub fn query_attachment_is_covered(&self, attachment: &QueryAttachment) -> bool {
        let node = self.node.borrow();
        let active_receipts = self.active_authority_view_receipts.borrow();
        let covered = attachment
            .required_after
            .iter()
            .all(|(binding_view, required_after)| {
                node.applied_view_update_generation(*binding_view) > *required_after
                    && !node.opening_pending_for_binding_view(*binding_view)
                    && (!attachment.requires_current_authority_receipt
                        || active_receipts
                            .as_ref()
                            .is_some_and(|receipts| receipts.binding_views.contains(binding_view)))
            });
        drop(node);
        drop(active_receipts);
        if covered {
            let mut refreshes = self.coverage_refresh_generations.borrow_mut();
            for (coverage, generation) in &attachment.refreshes {
                if refreshes.get(coverage).copied() == Some(*generation) {
                    refreshes.remove(coverage);
                }
            }
        }
        covered
    }

    /// Detach a one-shot query coverage request.
    pub fn detach_query(&self, attachment: QueryAttachment) {
        let mut removed_subscriptions = Vec::new();
        let mut registrations = self.query_coverage_registrations.borrow_mut();
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
            let mut coverage_refcounts = self.upstream_coverage_refcounts.borrow_mut();
            let Some(count) = coverage_refcounts.get_mut(&coverage) else {
                continue;
            };
            *count = count.saturating_sub(1);
            let last_coverage_pin = *count == 0;
            if last_coverage_pin {
                coverage_refcounts.remove(&coverage);
                self.awaiting_initial_authority_coverage
                    .borrow_mut()
                    .remove(&coverage);
            }
            let has_live_stream_owner = self
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
            self.node.borrow_mut().apply_unsubscribe(subscription);
            let replacement = self
                .query_coverage_registrations
                .borrow()
                .values()
                .find(|registration| registration.coverage == coverage)
                .map(|registration| registration.subscription.subscription);
            let mut latest = self.latest_coverage_subscriptions.borrow_mut();
            if latest.get(&coverage) == Some(&subscription) {
                if let Some(replacement) = replacement {
                    latest.insert(coverage.clone(), replacement);
                } else {
                    latest.remove(&coverage);
                }
            }
            drop(latest);
            self.upstream_subscriptions
                .borrow_mut()
                .push(PendingUpstreamCommand::Unsubscribe(subscription));
        }
        self.schedule_tick(TickUrgency::Immediate);
    }

    pub(super) fn open_subscription_resident(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<SubscriptionStream, SubscriptionOpenError> {
        ensure_supported_subscription_read_opts(&opts).map_err(SubscriptionOpenError::Api)?;
        self.validate_prepared_shape_for_registration(prepared)
            .map_err(SubscriptionOpenError::Api)?;
        let read_tier = effective_read_tier(&opts);
        if authorization_mode == QueryAuthorizationMode::ClientLocal
            && read_tier == DurabilityTier::Local
            && let ReadViewSourceSpec::Branch { branch } = &opts.read_view.source
        {
            self.node
                .borrow_mut()
                .acquire_branch_read_inputs(
                    &prepared.shape,
                    &prepared.binding,
                    crate::ids::BranchId(*branch),
                    author,
                    true,
                )
                .map_err(SubscriptionOpenError::Node)?;
        }
        let remote_propagate_upstream = opts.propagation == Propagation::Full;
        // A non-durable browser client must still ask its durable worker for a
        // local-only view. The wire flag stops that request at the worker.
        let propagates_upstream = remote_propagate_upstream
            || self.node.borrow().upstream_durability_floor() == DurabilityTier::Local;
        // Acquire both the local and possible remote-tier programs before the
        // first real Groove subscription is retained. This keeps every later
        // façade-side registration in the non-suspending publish phase.
        if propagates_upstream {
            let upstream_opts = self.upstream_register_shape_options(
                read_tier,
                opts.read_view.clone(),
                remote_propagate_upstream,
            );
            let (shape, binding) = if upstream_opts.tier == read_tier {
                (prepared.shape.clone(), prepared.binding.clone())
            } else {
                let (shape, binding, _) = self
                    .node
                    .borrow_mut()
                    .prepare_query_binding_for_link_in_authorization_mode(
                        &prepared.shape,
                        &prepared.binding,
                        upstream_opts.tier,
                        author,
                        authorization_mode,
                    )
                    .map_err(SubscriptionOpenError::Node)?;
                (shape, binding)
            };
            self.node
                .borrow_mut()
                .ensure_peer_maintained_subscription_view_supported(
                    &shape,
                    &binding,
                    upstream_opts.tier,
                    author,
                    &opts.read_view,
                    authorization_mode,
                )
                .map_err(SubscriptionOpenError::Node)?;
        }
        self.node
            .borrow_mut()
            .ensure_peer_maintained_subscription_view_supported(
                &prepared.shape,
                &prepared.binding,
                read_tier,
                author,
                &opts.read_view,
                authorization_mode,
            )
            .map_err(SubscriptionOpenError::Node)?;
        let (local_shape, local_binding, _local_plan) = self
            .node
            .borrow_mut()
            .prepare_query_binding_for_link_in_authorization_mode(
                &prepared.shape,
                &prepared.binding,
                read_tier,
                author,
                authorization_mode,
            )
            .map_err(SubscriptionOpenError::Node)?;
        let (subscription, snapshot) = self
            .node
            .borrow_mut()
            .open_maintained_view_subscription_in_authorization_mode(
                &local_shape,
                &local_binding,
                author,
                read_tier,
                &opts.read_view,
                Some(_local_plan),
                authorization_mode,
            )
            .map_err(SubscriptionOpenError::Node)?;
        let root_occurrence_ids = subscription.root_occurrence_ids().to_vec();
        let local_subscription_id = subscription.subscription_id();
        let local_node = Rc::clone(&self.node);
        let local_runtime_token = local_node.borrow().groove_runtime_token();
        let local_subscription_cleanup = Rc::new(Cell::new(Some((
            local_runtime_token,
            local_subscription_id,
        ))));
        let local_cleanup_handle = Rc::clone(&local_subscription_cleanup);
        let mut local_cleanup = CleanupGuard::new(Box::new(move || {
            let mut node = local_node.borrow_mut();
            if let Some((runtime_token, subscription_id)) = local_cleanup_handle.get()
                && node.groove_runtime_token() == runtime_token
            {
                node.unsubscribe_groove_subscription(subscription_id);
            }
        }));
        let mut maintained_subscription = Some(subscription);
        // A projected ordered root needs terminal patches even without nested
        // arrays: an unprojected sort-key mutation can move a visible row
        // without changing the projected payload. Unprojected roots retain
        // ordinary row deltas, including scope re-entry membership changes.
        let terminal_rows = !local_shape.query().array_subqueries.is_empty()
            || (local_shape.query().select.is_some() && !local_shape.query().order_by.is_empty());
        let mut state_shape = local_shape;
        let mut state_binding = local_binding;
        let mut remote_read_tier = None;
        let mut requires_authority_receipt = false;
        let mut upstream_subscription_handles = Vec::new();
        let mut suppress_provisional_opening = false;
        if propagates_upstream {
            let upstream_opts = self.upstream_register_shape_options(
                effective_read_tier(&opts),
                opts.read_view.clone(),
                remote_propagate_upstream,
            );
            let (shape, binding) = if upstream_opts.tier == read_tier {
                (state_shape.clone(), state_binding.clone())
            } else {
                let (shape, binding, _) = self
                    .node
                    .borrow_mut()
                    .prepare_query_binding_for_link_in_authorization_mode(
                        &prepared.shape,
                        &prepared.binding,
                        upstream_opts.tier,
                        author,
                        authorization_mode,
                    )
                    .map_err(SubscriptionOpenError::Node)?;
                (shape, binding)
            };
            state_shape = shape.clone();
            state_binding = binding.clone();
            remote_read_tier = Some(upstream_opts.tier);
            // Edge/Global cache possession is never a settlement receipt,
            // even when this subscription opens before an upstream exists.
            // The eventual connection must send its own ViewUpdate.
            requires_authority_receipt = upstream_opts.tier >= DurabilityTier::Edge;
            let opened = self
                .open_subscription_upstream_coverage(
                    &shape,
                    &binding,
                    upstream_opts,
                    author,
                    authorization_mode,
                )
                .map_err(SubscriptionOpenError::Api)?;
            upstream_subscription_handles = opened.handles;
            suppress_provisional_opening = authorization_mode
                == QueryAuthorizationMode::ClientLocal
                && read_tier >= DurabilityTier::Edge
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
                }
                .read_view_key(),
            };
            if let Some(maintained) = maintained_subscription.as_mut() {
                self.node
                    .borrow()
                    .seed_local_maintained_authoritative_result_membership(
                        maintained,
                        binding_view_key,
                    );
            }
        }
        let settled = subscription_is_settled(
            &self.node.borrow(),
            &self.active_authority_view_receipts,
            &state_shape,
            &state_binding,
            settled_tier,
            opts.read_view.clone(),
            remote_propagate_upstream,
            requires_authority_receipt,
        );
        // An empty local opening carries no observable result information at
        // an Edge/Global request.  Until the authority replies, publishing it
        // would let a public subscription report a provisional empty view as
        // its first delivery.  `awaits_initial_authority_response` is only
        // known while opening a fresh upstream handle, but an already-open
        // link has the same receipt requirement.
        suppress_provisional_opening |= authorization_mode == QueryAuthorizationMode::ClientLocal
            && read_tier >= DurabilityTier::Edge
            && remote_read_tier.is_some()
            && !settled
            && snapshot.root_count == 0
            && snapshot.edges.is_empty();
        let (sender, receiver) = unbounded();
        let initial_outputs =
            subscription_outputs_with_occurrence_sidecar(&snapshot, &root_occurrence_ids)
                .map_err(SubscriptionOpenError::Api)?;
        let state_snapshot = relation_snapshot_with_delta_slack(&snapshot);
        let mut snapshot_index = RelationSnapshotIndex::from_snapshot(&state_snapshot);
        snapshot_index.roots = root_occurrence_ids
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, occurrence)| (occurrence, index))
            .collect();
        let state = Rc::new(RefCell::new(SubscriptionState {
            terminal_rows,
            kind: SubscriptionKind::Prepared {
                shape: state_shape,
                binding: state_binding,
                maintained_subscription,
            },
            groove_runtime_token: self.node.borrow().groove_runtime_token(),
            local_subscription_cleanup,
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
                terminal_layout: None,
                settled,
                tier: read_tier,
            })
            .map_err(|_| {
                SubscriptionOpenError::Api(Error::new(
                    ErrorCode::Protocol,
                    "subscription receiver closed",
                ))
            })?;
        self.subscriptions.borrow_mut().push(Rc::downgrade(&state));
        let cleanup = if upstream_subscription_handles.is_empty() {
            local_cleanup.take()
        } else {
            let owner = Rc::downgrade(&state);
            register_upstream_subscription_owner(
                &self.upstream_subscription_owners,
                &upstream_subscription_handles,
                &state,
            );
            let upstream_cleanup =
                self.upstream_subscription_cleanup(upstream_subscription_handles, owner);
            let local_cleanup = local_cleanup.take();
            Box::new(move || {
                local_cleanup();
                upstream_cleanup();
            })
        };
        Ok(SubscriptionStream {
            receiver,
            _state: state,
            cleanup: Some(cleanup),
        })
    }

    fn validate_prepared_shape_for_registration(
        &self,
        prepared: &PreparedQuery,
    ) -> Result<(), Error> {
        let ast = ShapeAst::from_validated(&prepared.shape);
        let validation = {
            let node = self.node.borrow();
            validate_shape_ast_for_registration(&node, prepared.shape.shape_id(), &ast)
        };
        validation.map(|_| ()).map_err(Error::from)
    }

    fn open_subscription_upstream_coverage(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
        identity: AuthorId,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<OpenedUpstreamCoverage, Error> {
        self.node
            .borrow_mut()
            .ensure_peer_maintained_subscription_view_supported(
                shape,
                binding,
                opts.tier,
                identity,
                &opts.read_view,
                authorization_mode,
            )?;
        let coverage = coverage_key(shape, binding, opts.clone());
        if self
            .upstream_coverage_refcounts
            .borrow()
            .contains_key(&coverage)
        {
            if let Some(subscription) = self
                .latest_coverage_subscriptions
                .borrow()
                .get(&coverage)
                .copied()
            {
                *self
                    .upstream_coverage_refcounts
                    .borrow_mut()
                    .entry(coverage.clone())
                    .or_insert(0) += 1;
                let awaits_initial_authority_response = self
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
            .upstream_coverage_refcounts
            .borrow_mut()
            .entry(coverage.clone())
            .or_insert(0) += 1;
        let has_live_upstream =
            self.connections.borrow().iter().any(|connection| {
                matches!(&connection.borrow().link, ConnectionLink::Upstream { .. })
            });
        if has_live_upstream {
            self.awaiting_initial_authority_coverage
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

    fn upstream_subscription_cleanup(
        &self,
        upstream_subscriptions: Vec<UpstreamCoverageHandle>,
        owner: Weak<RefCell<SubscriptionState>>,
    ) -> Box<dyn FnOnce()> {
        let node = Rc::clone(&self.node);
        let latest_coverage_subscriptions = Rc::clone(&self.latest_coverage_subscriptions);
        let upstream_coverage_refcounts = Rc::clone(&self.upstream_coverage_refcounts);
        let awaiting_initial_authority_coverage =
            Rc::clone(&self.awaiting_initial_authority_coverage);
        let upstream_subscription_owners = Rc::clone(&self.upstream_subscription_owners);
        let pending_upstream_subscriptions = Rc::clone(&self.upstream_subscriptions);
        let scheduler = Rc::clone(&self.scheduler);
        Box::new(move || {
            for handle in upstream_subscriptions {
                unregister_upstream_subscription_owner(
                    &upstream_subscription_owners,
                    handle.subscription,
                    &owner,
                );
                let mut refcounts = upstream_coverage_refcounts.borrow_mut();
                let Some(count) = refcounts.get_mut(&handle.coverage) else {
                    continue;
                };
                *count = count.saturating_sub(1);
                if *count > 0 {
                    continue;
                }
                refcounts.remove(&handle.coverage);
                awaiting_initial_authority_coverage
                    .borrow_mut()
                    .remove(&handle.coverage);
                drop(refcounts);
                let upstream_subscription = handle.subscription;
                node.borrow_mut().apply_unsubscribe(upstream_subscription);
                latest_coverage_subscriptions
                    .borrow_mut()
                    .retain(|coverage, subscription| {
                        coverage != &handle.coverage && *subscription != upstream_subscription
                    });
                pending_upstream_subscriptions
                    .borrow_mut()
                    .push(PendingUpstreamCommand::Unsubscribe(upstream_subscription));
            }
            schedule_tick_in(&scheduler, TickUrgency::Immediate);
        })
    }
}
