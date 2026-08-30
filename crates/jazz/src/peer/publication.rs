fn binding_settlement_time<S>(
    node: &NodeState<S>,
    subscription: SubscriptionKey,
    shape: &ValidatedQuery,
    binding: &Binding,
) -> GlobalTime
where
    S: OrderedKvStorage,
{
    let key = crate::protocol::BindingViewKey::new(
        shape.shape_id(),
        binding.binding_id(),
        subscription.read_view,
    );
    node.settled_through_for_binding_view(key)
        .unwrap_or_else(|| node.committed_global_time())
}

fn canonical_subscription_settlement_time<S>(
    node: &NodeState<S>,
    subscription: SubscriptionKey,
) -> GlobalTime
where
    S: OrderedKvStorage,
{
    node.settled_through_for_binding_view(
        crate::protocol::BindingViewKey::from_canonical_subscription_key(subscription),
    )
    .unwrap_or_else(|| node.committed_global_time())
}

fn ordinary_flat_row_duplicate_view(
    shape: &ValidatedQuery,
    current_members: &BTreeSet<ResultMemberEntry>,
    removed_members_are_ordinary: bool,
    canonical_read_view: crate::protocol::ReadViewKey,
    canonical_program_facts_empty: bool,
    source_had_program_fact_transitions: bool,
) -> bool {
    let query = shape.query();
    query.flat_join.is_none()
        && query.includes.is_empty()
        && query.array_subqueries.is_empty()
        && query.aggregate.is_none()
        && canonical_read_view == RegisterShapeOptions::default().read_view_key()
        && canonical_program_facts_empty
        && !source_had_program_fact_transitions
        && removed_members_are_ordinary
        && current_members.iter().all(ordinary_current_content_member)
}

fn ordinary_current_content_member(member: &ResultMemberEntry) -> bool {
    matches!(member, ResultMemberEntry::Row(row) if row.layer == crate::protocol::ResultRowLayer::Content
        && row.deletion_tx.is_none()
        && row.source == crate::protocol::ResultRowSource::Current
        && row.branch_or_prefix.is_none()
        && row.batch.is_none())
}

impl PeerState {
    pub(super) fn has_maintained_subscription(&self, subscription: SubscriptionKey) -> bool {
        self.publication_states
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .is_some()
    }

    fn fast_cursor_authorization_matches(
        &self,
        subscription: SubscriptionKey,
        known_state: &Option<KnownStateDeclaration>,
    ) -> bool {
        match self.role {
            PeerRole::Relay => true,
            PeerRole::ClientLink { .. } => {
                self.publication_states.get(&subscription).is_some_and(|state| {
                    state.has_served_authorization_progress
                        && fast_authorization_progress(known_state)
                            == Some(state.authorization_progress)
                })
            }
        }
    }

    pub(crate) fn needs_catalogue_snapshot(&self, fingerprint: [u8; 32]) -> bool {
        self.announced_catalogue_fingerprint != Some(fingerprint)
    }

    pub(crate) fn mark_catalogue_snapshot_announced(&mut self, fingerprint: [u8; 32]) {
        self.announced_catalogue_fingerprint = Some(fingerprint);
    }

    /// Construct a permanent relay peer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a permanent relay peer.
    pub fn relay() -> Self {
        Self::default()
    }

    /// Construct a peer link that terminates one client author identity.
    pub fn client_link(identity: AuthorSubject) -> Self {
        Self {
            role: PeerRole::ClientLink { identity },
            ..Self::default()
        }
    }

    /// Construct an edge-boundary peer that terminates one client author identity.
    pub fn edge_client(identity: AuthorSubject) -> Self {
        Self::client_link(identity)
    }

    /// Construct an edge peer whose wire identity and read-policy identity differ.
    ///
    /// Trusted backend websocket links still speak as their concrete peer identity
    /// for session/resume validation, but served reads must bypass row policies.
    pub fn edge_client_with_permission_identity(
        identity: AuthorSubject,
        permission_identity: AuthorSubject,
    ) -> Self {
        Self {
            role: PeerRole::ClientLink { identity },
            permission_identity: Some(permission_identity),
            ..Self::default()
        }
    }

    /// Return the named role for this peer link.
    pub fn role(&self) -> PeerRole {
        self.role
    }

    /// Return the wire/session identity for this peer link.
    pub fn link_identity(&self) -> AuthorSubject {
        self.role.identity()
    }

    /// Return the identity used to evaluate reads on this peer link.
    pub fn identity(&self) -> AuthorSubject {
        self.permission_identity
            .unwrap_or_else(|| self.role.identity())
    }

    fn clear_stale_groove_runtime_handles<S>(
        &mut self,
        node: &NodeState<S>,
        subscription: SubscriptionKey,
    ) where
        S: OrderedKvStorage,
    {
        let current_token = node.groove_runtime_token();
        if self.publication_states.get(&subscription).is_some_and(|state| {
            state
                .groove_runtime_token
                .is_some_and(|token| token != current_token)
        }) {
            if let Some(state) = self.publication_states.get_mut(&subscription) {
                state.clear_groove_runtime_handles();
            }
            self.refresh_maintained_subscription_view_footprint(subscription);
        }
    }

    fn ensure_query_subscription_registered<S>(
        &self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<(), Error>
    where
        S: OrderedKvStorage,
    {
        node.register_query_subscription_for_peer(
            shape.shape_id(),
            ShapeAst::from_validated(shape),
            Subscribe {
                shape_id: shape.shape_id(),
                subscription,
                values: binding_values_in_param_order(shape, binding),
                known_state: None,
            },
        )?;
        Ok(())
    }

    /// Builds a full current-row view update, using tx-level refs for complete
    /// transaction payloads in this peer's inventory and bundles for new or
    /// partial view payload.
    pub async fn current_rows_update<S>(
        &mut self,
        node: &mut NodeState<S>,
        table: &str,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        let (shape, binding) = node.whole_table_shape_binding(table)?;
        let opts = RegisterShapeOptions::default();
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: opts.read_view_key(),
        };
        self.clear_stale_groove_runtime_handles(node, subscription);
        self.ensure_query_subscription_registered(node, subscription, &shape, &binding)?;
        let needs_prepare = self
            .publication_states
            .get(&subscription)
            .and_then(|state| state.prepared_query.as_ref())
            .is_none_or(|prepared| !prepared.has_runtime_plan());
        if needs_prepare {
            let plan =
                node.mark_peer_maintained_query_shape_cache(&shape, &binding, opts.tier);
            let cached = CachedPeerQueryPlan::with_plan(&opts, plan);
            let state = self.publication_states.entry(subscription).or_default();
            state.prepared_query = Some(cached);
            state.groove_runtime_token = Some(node.groove_runtime_token());
        } else {
            self.publication_states.entry(subscription).or_default();
        }
        let (tier, read_view): (DurabilityTier, std::sync::Arc<ReadViewSpec>) = self
            .publication_states
            .get(&subscription)
            .and_then(|state| state.prepared_query.as_ref())
            .map(CachedPeerQueryPlan::context)
            .ok_or(Error::InvalidStoredValue(
                "maintained subscription view is missing prepared state",
            ))?;
        let previous_member_result_set = self
            .publication_states
            .get(&subscription)
            .map(PeerSubscriptionState::member_result_set)
            .unwrap_or_default();
        if self
            .publication_states
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .is_none()
        {
            if let Some(update) = self.rehydrate_query_maintained_subscription_view(
                node,
                MaintainedRehydrateRequest {
                    shape: &shape,
                    binding: &binding,
                    subscription,
                    previous_member_result_set: &previous_member_result_set,
                    reset_result_set: false,
                    result_table_filter: Some(table),
                    tier,
                    read_view: &read_view,
                    purpose: RehydratePurpose::Query,
                },
                None,
            )
            .await?
            {
                return Ok(update);
            }
            node.drive_query_runtime().await?;
        }
        if self
            .publication_states
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .is_some()
        {
            if let Some(update) = self.query_update_maintained_subscription_view(
                node,
                &shape,
                &binding,
                subscription,
                Some(table),
                None,
            )
            .await?
            {
                return Ok(update);
            }
            node.drive_query_runtime().await?;
            return self
                .query_update_maintained_subscription_view(
                    node,
                    &shape,
                    &binding,
                    subscription,
                    Some(table),
                    None,
                )
                .await?
                .ok_or(Error::InvalidStoredValue(
                "maintained hydration ended without an initial publication",
            ));
        }
        unreachable!("maintained subscription view state is either absent or present")
    }

    /// Builds a query-binding view update, using tx-level refs for complete
    /// transaction payloads in this peer's inventory and bundles for new or
    /// partial view payload.
    pub async fn query_update<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        self.query_update_inner(node, shape, binding).await
    }

    /// Build an incremental view update addressed to a usage-site subscription.
    pub async fn query_update_for_subscription<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        self.await_query_update_for_subscription_with_opts(
            node,
            subscription,
            shape,
            binding,
            RegisterShapeOptions::default(),
        )
        .await
    }

    /// Build an incremental view update addressed to a usage-site subscription,
    /// preserving the read view and tier used when the shape was registered.
    pub async fn query_update_for_subscription_with_opts<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
    ) -> Result<Option<SyncMessage>, Error>
    where
        S: OrderedKvStorage,
    {
        self.query_update_for_subscription_with_opts_and_waker(
            node,
            subscription,
            shape,
            binding,
            opts,
            None,
        )
        .await
    }

    pub(crate) async fn query_update_for_subscription_with_opts_and_waker<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<Option<SyncMessage>, Error>
    where
        S: OrderedKvStorage,
    {
        self.query_update_inner_for_subscription(
            node,
            subscription,
            shape,
            binding,
            opts,
            progress_waker,
        )
        .await
    }

    async fn query_update_inner<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: Default::default(),
        };
        self.await_query_update_for_subscription_with_opts(
            node,
            subscription,
            shape,
            binding,
            RegisterShapeOptions::default(),
        )
        .await
    }

    async fn await_query_update_for_subscription_with_opts<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        if let Some(update) = self
            .query_update_inner_for_subscription(
                node,
                subscription,
                shape,
                binding,
                opts.clone(),
                None,
            )
            .await?
        {
            return Ok(update);
        }
        node.drive_query_runtime().await?;
        self.query_update_inner_for_subscription(node, subscription, shape, binding, opts, None)
            .await?
            .ok_or(Error::InvalidStoredValue(
                "maintained hydration ended without a query publication",
            ))
    }

    async fn query_update_inner_for_subscription<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<Option<SyncMessage>, Error>
    where
        S: OrderedKvStorage,
    {
        self.clear_stale_groove_runtime_handles(node, subscription);
        self.ensure_query_subscription_registered(node, subscription, shape, binding)?;
        let Some(state) = self.publication_states.get(&subscription) else {
            return Ok(Some(SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                subscription,
                settled_through: binding_settlement_time(node, subscription, shape, binding),
                reset_result_set: false,
                version_carriers: Vec::new(),
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
                terminal_operations: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            })));
        };
        if state.maintained_subscription_view.is_some() {
            return self.query_update_maintained_subscription_view(
                node,
                shape,
                binding,
                subscription,
                None,
                progress_waker,
            )
            .await;
        }
        let previous_member_result_set = self
            .publication_states
            .get(&subscription)
            .map(PeerSubscriptionState::member_result_set)
            .unwrap_or_default();
        let cached_context = self
            .publication_states
            .get(&subscription)
            .and_then(|state| state.prepared_query.as_ref())
            .map(|prepared| (prepared.context(), prepared.has_runtime_plan()));
        let ((tier, read_view), has_runtime_plan) = if let Some(context) = cached_context {
            context
        } else {
            ((opts.tier, std::sync::Arc::new(opts.read_view.clone())), false)
        };
        if !has_runtime_plan {
            let plan = node.mark_peer_maintained_query_shape_cache(shape, binding, tier);
            let state = self.publication_states.entry(subscription).or_default();
            if let Some(prepared) = &mut state.prepared_query {
                prepared.replace_runtime_plan(plan);
            } else {
                state.prepared_query = Some(CachedPeerQueryPlan::with_context(
                    tier,
                    read_view.clone(),
                    plan,
                ));
            }
            state.groove_runtime_token = Some(node.groove_runtime_token());
        }
        self.rehydrate_query_maintained_subscription_view(
            node,
            MaintainedRehydrateRequest {
                shape,
                binding,
                subscription,
                previous_member_result_set: &previous_member_result_set,
                reset_result_set: false,
                result_table_filter: None,
                tier,
                read_view: &read_view,
                purpose: RehydratePurpose::Query,
            },
            progress_waker,
        )
        .await
    }

    async fn query_update_maintained_subscription_view<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        result_table_filter: Option<&str>,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<Option<SyncMessage>, Error>
    where
        S: OrderedKvStorage,
    {
        let trace_rehydrate = std::env::var_os("JAZZ_REHYDRATE_TRACE").is_some();
        let trace_start = Instant::now();
        if trace_rehydrate {
            node.reset_storage_read_metrics();
        }
        let transitions = self.drain_maintained_subscription_view_changes(
            node,
            shape,
            subscription,
            result_table_filter,
            progress_waker,
        )
        .await?;
        if !self
            .publication_states
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .is_some_and(|maintained| maintained.initial_received)
        {
            return Ok(None);
        }
        let drain_elapsed = trace_start.elapsed();
        let drain_reads = trace_rehydrate.then(|| node.take_storage_read_metrics());
        let ResultTransitions {
            authoritative_membership_changed: _,
            authoritative_member_adds: _,
            adds: result_member_adds,
            removes: mut result_member_removes,
            result_payload_adds: _,
            result_payload_removes: _,
            program_fact_adds,
            program_fact_removes,
            allow_storage_witness_fallback,
            observed_result_delta_batches,
            requires_authoritative_membership_reconcile,
            terminal_operations,
        } = transitions;
        let result_add_count = result_member_adds.len();
        let result_remove_count = result_member_removes.len();
        let fact_add_count = program_fact_adds.len();
        let fact_remove_count = program_fact_removes.len();
        let previous_member_result_set = self
            .publication_states
            .get(&subscription)
            .map(PeerSubscriptionState::member_result_set)
            .unwrap_or_default();
        if requires_authoritative_membership_reconcile
            || (observed_result_delta_batches > 0
                && result_member_adds.is_empty()
                && result_member_removes.is_empty()
                && terminal_operations.is_empty()
                && program_fact_adds.is_empty()
                && program_fact_removes.is_empty())
        {
            let (tier, read_view) = self
                .publication_states
                .get(&subscription)
                .and_then(|state| state.prepared_query.as_ref())
                .map(CachedPeerQueryPlan::context)
                .ok_or(Error::InvalidStoredValue(
                    "maintained subscription view is missing prepared state",
                ))?;
            return self.rehydrate_query_maintained_subscription_view(
                node,
                MaintainedRehydrateRequest {
                    shape,
                    binding,
                    subscription,
                    previous_member_result_set: &previous_member_result_set,
                    reset_result_set: false,
                    result_table_filter,
                    tier,
                    read_view: &read_view,
                    purpose: RehydratePurpose::Query,
                },
                progress_waker,
            )
            .await;
        }
        if let Some(state) = self.publication_states.get(&subscription) {
            result_member_removes.extend(replacement_removals(state, &result_member_adds));
        }
        result_member_removes = result_member_removes
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if maintained_view_update_is_empty(
            &result_member_adds,
            &result_member_removes,
            &terminal_operations,
            &program_fact_adds,
            &program_fact_removes,
        ) {
            return Ok(Some(SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                subscription,
                settled_through: binding_settlement_time(node, subscription, shape, binding),
                reset_result_set: false,
                version_carriers: Vec::new(),
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
                terminal_operations: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            })));
        }
        let previous_result_tx_ids = previous_member_result_set
            .iter()
            .filter_map(ResultMemberEntry::as_row)
            .map(|(_, _, tx_id)| tx_id)
            .collect::<BTreeSet<_>>();
        let tier = self
            .publication_states
            .get(&subscription)
            .and_then(|state| state.prepared_query.as_ref())
            .map(CachedPeerQueryPlan::tier)
            .ok_or(Error::InvalidStoredValue(
                "maintained subscription view is missing prepared state",
            ))?;
        let peer_complete_tx_payloads = self.acknowledged_complete_tx_payloads();
        let known_state = self.downstream_known_states.get(&subscription).cloned();
        let previous_program_facts = self
            .publication_states
            .get(&subscription)
            .map(PeerSubscriptionState::program_fact_set)
            .unwrap_or_default();
        let bundle_start = Instant::now();
        if trace_rehydrate {
            node.reset_storage_read_metrics();
        }
        let update = {
            let maintained = &self
                .publication_states
                .get(&subscription)
                .and_then(|state| state.maintained_subscription_view.as_ref())
                .ok_or(Error::InvalidStoredValue(
                    "maintained subscription view subscription missing",
                ))?
                .maintained;
            node.view_update_for_maintained_result_members(
                crate::node::MaintainedViewBundleInputs {
                    subscription,
                    peer_complete_tx_payloads,
                    known_state,
                    complete_exclusive_payloads: self.ship_complete_exclusive_payloads
                        && self.role == PeerRole::Relay,
                    previous_result_set: previous_result_tx_ids,
                    previous_program_facts,
                    flat_tuple_source_tables:
                        crate::node::FlatTupleSourceTables::for_query(shape),
                    result_member_adds,
                    result_member_removes,
                    program_fact_adds,
                    program_fact_removes,
                    identity: self.identity(),
                    tier,
                    maintained_facts: maintained,
                    allow_storage_witness_fallback,
                },
            )
        };
        let mut update = update.await?;
        if let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            terminal_operations: outgoing,
            ..
        }) = &mut update
        {
            *outgoing = terminal_operations;
        }
        let bundle_elapsed = bundle_start.elapsed();
        let bundle_reads = trace_rehydrate.then(|| node.take_storage_read_metrics());
        if trace_rehydrate {
            let bundle_count = match &update {
                SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                    version_carriers,
                    ..
                }) => view_update_singleton_bundles(version_carriers).len(),
                _ => 0,
            };
            let drain_reads = drain_reads.expect("trace reads captured");
            let bundle_reads = bundle_reads.expect("trace reads captured");
            eprintln!(
                "JAZZ_REHYDRATE_TRACE stage=update subscription={subscription:?} drain_ms={} bundle_ms={} adds={} removes={} fact_adds={} fact_removes={} bundles={} fallback={} drain_reads={} drain_ranges={} bundle_reads={} bundle_ranges={}",
                drain_elapsed.as_millis(),
                bundle_elapsed.as_millis(),
                result_add_count,
                result_remove_count,
                fact_add_count,
                fact_remove_count,
                bundle_count,
                allow_storage_witness_fallback,
                drain_reads.total.reads,
                drain_reads.total.ranges,
                bundle_reads.total.reads,
                bundle_reads.total.ranges,
            );
        }
        self.metrics.maintained_subscription_view.hits_out += 1;
        self.refresh_maintained_subscription_view_footprint(subscription);
        self.record_outgoing_view_update(&update);
        Ok(Some(update))
    }

    async fn drain_maintained_subscription_view_changes<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        subscription: SubscriptionKey,
        result_table_filter: Option<&str>,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<ResultTransitions, Error>
    where
        S: OrderedKvStorage,
    {
        node.drive_ready_query_runtime_with_waker(progress_waker)
            .await?;
        let previous_member_result_set = self
            .publication_states
            .get(&subscription)
            .map(PeerSubscriptionState::member_result_set)
            .unwrap_or_default();
        let previous_program_fact_set = self
            .publication_states
            .get(&subscription)
            .map(PeerSubscriptionState::program_fact_set)
            .unwrap_or_default();
        let output_tables = self
            .publication_states
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .map(|maintained| maintained.tables.clone())
            .unwrap_or_default();
        let source_binding_view = self
            .publication_states
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .and_then(|maintained| maintained.source_binding_view);
        let aggregate_is_policy_scoped = shape.query().aggregate.is_some()
            && node
                .table(shape.query().table.as_str())?
                .read_policy
                .is_some();
        let mut states = BTreeMap::<ResultMemberEntry, (bool, bool)>::new();
        let mut program_fact_adds = Vec::new();
        let mut program_fact_removes = Vec::new();
        let mut allow_storage_witness_fallback = false;
        let mut observed_result_delta_batches = 0_usize;
        let mut requires_authoritative_membership_reconcile = false;
        let mut terminal_operations = Vec::new();
        {
            let Some(maintained_subscription_view) = self
                .publication_states
                .get_mut(&subscription)
                .and_then(|state| state.maintained_subscription_view.as_mut())
            else {
                return Ok(ResultTransitions::default());
            };
            loop {
                match maintained_subscription_view.subscription.try_recv() {
                    Ok(deltas) => {
                        maintained_subscription_view.initial_received = true;
                        self.metrics.maintained_subscription_view.delta_batches_in += 1;
                        let transitions = maintained_subscription_view
                            .maintained
                            .apply_multisink_deltas(
                                deltas,
                                &maintained_subscription_view.terminal_schemas,
                                &maintained_subscription_view.tables,
                                &node.node_aliases,
                            )?;
                        observed_result_delta_batches += transitions.observed_result_delta_batches;
                        requires_authoritative_membership_reconcile |=
                            transitions.requires_authoritative_membership_reconcile;
                        terminal_operations.extend(transitions.terminal_operations);
                        program_fact_adds.extend(filter_program_facts_for_result_table(
                            transitions.program_fact_adds,
                            result_table_filter,
                            &output_tables,
                        ));
                        program_fact_removes.extend(filter_program_facts_for_result_table(
                            transitions.program_fact_removes,
                            result_table_filter,
                            &output_tables,
                        ));
                        for member in transitions.adds {
                            let before = previous_member_result_set.contains(&member);
                            states
                                .entry(member)
                                .and_modify(|(_, after)| *after = true)
                                .or_insert((before, true));
                        }
                        for member in transitions.removes {
                            let before = previous_member_result_set.contains(&member);
                            states
                                .entry(member)
                                .and_modify(|(_, after)| *after = false)
                                .or_insert((before, false));
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break,
                }
            }
        }
        if self.role == PeerRole::Relay
            && result_table_filter.is_none()
            && let Some(settled) = node.settled_result_transitions_for_subscription(
                subscription,
                source_binding_view,
                &previous_member_result_set,
                &previous_program_fact_set,
                result_table_filter,
                &output_tables,
            )?
        {
            allow_storage_witness_fallback |= settled.allow_storage_witness_fallback;
            for member in settled.adds {
                let before = previous_member_result_set.contains(&member);
                states
                    .entry(member)
                    .and_modify(|(_, after)| *after = true)
                    .or_insert((before, true));
            }
            for member in settled.removes {
                let before = previous_member_result_set.contains(&member);
                states
                    .entry(member)
                    .and_modify(|(_, after)| *after = false)
                    .or_insert((before, false));
            }
            program_fact_adds.extend(settled.program_fact_adds);
            program_fact_removes.extend(settled.program_fact_removes);
        }
        let mut result_member_adds = Vec::new();
        let mut result_member_removes = Vec::new();
        for (member, (before, after)) in states {
            let Some(table_name) = member.table_name() else {
                continue;
            };
            if !matches!(member, ResultMemberEntry::Synthetic { .. })
                && result_table_filter.is_some_and(|table| table_name != table)
            {
                continue;
            }
            if !output_tables.contains_key(table_name)
                && (!matches!(member, ResultMemberEntry::Synthetic { .. })
                    || aggregate_is_policy_scoped)
            {
                continue;
            }
            match (before, after) {
                (false, true) => result_member_adds.push(member),
                (true, false) => result_member_removes.push(member),
                _ => {}
            }
        }
        Ok(ResultTransitions {
            authoritative_membership_changed: false,
            authoritative_member_adds: BTreeSet::new(),
            adds: result_member_adds,
            removes: result_member_removes,
            result_payload_adds: Vec::new(),
            result_payload_removes: Vec::new(),
            program_fact_adds,
            program_fact_removes,
            allow_storage_witness_fallback,
            observed_result_delta_batches,
            requires_authoritative_membership_reconcile,
            terminal_operations,
        })
    }

    async fn rehydrate_query_maintained_subscription_view<S>(
        &mut self,
        node: &mut NodeState<S>,
        request: MaintainedRehydrateRequest<'_>,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<Option<SyncMessage>, Error>
    where
        S: OrderedKvStorage,
    {
        let MaintainedRehydrateRequest {
            shape,
            binding,
            subscription,
            previous_member_result_set,
            reset_result_set,
            result_table_filter,
            tier,
            read_view,
            purpose,
        } = request;
        let trace_rehydrate = std::env::var_os("JAZZ_REHYDRATE_TRACE").is_some();
        let open_start = Instant::now();
        if trace_rehydrate {
            node.reset_storage_read_metrics();
        }
        let relay_edge_requires_authority_source = purpose == RehydratePurpose::Query
            && self.role == PeerRole::Relay
            && tier == DurabilityTier::Edge
            && node.relay_edge_query_requires_authority_source(shape, binding)?;
        let opened = match purpose {
            // A relay's selected Edge child is the browser half of a durable
            // worker authority receipt. The selection is deliberately narrow:
            // only a window or policy-scoped exact-ID read would change
            // semantics if evaluated from the relay's local cache.
            RehydratePurpose::Query if relay_edge_requires_authority_source => {
                node.open_seeded_relay_edge_subscription_view_with_waker(
                    shape,
                    binding,
                    self.identity(),
                    read_view,
                    progress_waker,
                )
                    .await
            }
            RehydratePurpose::Query => {
                node.open_seeded_maintained_subscription_view_with_waker(
                    shape,
                    binding,
                    self.identity(),
                    tier,
                    read_view,
                    progress_waker,
                )
                .await
            }
            RehydratePurpose::AuthorizationSupport => node
                .open_seeded_authorization_support_subscription_view_with_waker(
                    shape,
                    binding,
                    self.identity(),
                    tier,
                    read_view,
                    progress_waker,
                )
                .await,
        };
        let source_binding_view = relay_edge_requires_authority_source
            .then(|| node.relay_edge_subscription_source_binding_view_key(shape, binding, read_view))
            .flatten();
        let (receiver, mut maintained, terminal_schemas, transitions, tables, initial_received) =
            match opened {
            Ok(opened) => opened,
            Err(Error::AuthorizationSupportMissingClaim(_))
                if purpose == RehydratePurpose::AuthorizationSupport =>
            {
                let update = SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                    subscription,
                    settled_through: binding_settlement_time(node, subscription, shape, binding),
                    reset_result_set,
                    version_carriers: Vec::new(),
                    peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                    result_member_adds: Vec::new(),
                    result_member_removes: previous_member_result_set.iter().cloned().collect(),
                    terminal_operations: Vec::new(),
                    program_fact_adds: Vec::new(),
                    program_fact_removes: Vec::new(),
                });
                self.record_outgoing_view_update(&update);
                self.publication_states
                    .entry(subscription)
                    .or_default()
                    .has_served_authorization_progress = true;
                return Ok(Some(update));
            }
            Err(error) => return Err(error),
            };
        let retains_structured_terminal = !shape.query().array_subqueries.is_empty()
            || !shape.query().order_by.is_empty();
        if !retains_structured_terminal {
            maintained.discard_structured_app_rows();
        }
        if !initial_received {
            let maintained_subscription = MaintainedSubscriptionViewSubscription {
                subscription: receiver,
                maintained,
                terminal_schemas,
                tables,
                source_binding_view,
                initial_received: false,
            };
            let state = self.publication_states.entry(subscription).or_default();
            state.maintained_subscription_view = Some(maintained_subscription);
            state.groove_runtime_token = Some(node.groove_runtime_token());
            self.refresh_maintained_subscription_view_footprint(subscription);
            return Ok(None);
        }
        let open_elapsed = open_start.elapsed();
        let open_reads = trace_rehydrate.then(|| node.take_storage_read_metrics());
        let raw_add_count = transitions.adds.len();
        let raw_remove_count = transitions.removes.len();
        let raw_fact_add_count = transitions.program_fact_adds.len();
        let filter_start = Instant::now();
        let output_tables = tables.clone();
        let aggregate_is_policy_scoped = shape.query().aggregate.is_some()
            && node
                .table(shape.query().table.as_str())?
                .read_policy
                .is_some();
        let known_state = self.downstream_known_states.get(&subscription).cloned();
        let previous_program_facts = self
            .publication_states
            .get(&subscription)
            .map(PeerSubscriptionState::program_fact_set)
            .unwrap_or_default();
        let known_membership_position = fast_current_membership_position(&known_state);
        let authorization_matches =
            self.fast_cursor_authorization_matches(subscription, &known_state);
        let watermark = binding_settlement_time(node, subscription, shape, binding);
        let simple_membership_delta =
            transitions.program_fact_adds.is_empty() && transitions.program_fact_removes.is_empty();
        let mut result_member_adds = transitions
            .adds
            .into_iter()
            .filter(|member| {
                let Some(table_name) = member.table_name() else {
                    return false;
                };
                (matches!(member, ResultMemberEntry::Synthetic { .. })
                    || result_table_filter.is_none_or(|table| table_name == table))
                    && (output_tables.contains_key(table_name)
                        || (matches!(member, ResultMemberEntry::Synthetic { .. })
                            && !aggregate_is_policy_scoped))
            })
            .collect::<Vec<_>>();
        let current_member_result_set = result_member_adds.iter().cloned().collect::<BTreeSet<_>>();
        let mut result_member_removes = previous_member_result_set
            .difference(&current_member_result_set)
            .cloned()
            .collect::<Vec<_>>();
        // A live reconciliation is still a delta to the receiver's existing
        // result set.  Rehydration used for an explicit reset is different:
        // the receiver discards that set and needs the complete replacement.
        // In particular, a deletion witness may force reconciliation alongside
        // an ordinary result delta; do not resend retained window members as
        // additions in that case.
        if !reset_result_set {
            result_member_adds.retain(|member| !previous_member_result_set.contains(member));
        }
        // The downstream cursor tracks data progress, not authorization. A
        // removed prior member or newly visible pre-cursor member cannot be
        // reconstructed from that cursor, so it cannot safely suppress the
        // authoritative membership diff or the payload needed to apply it.
        // A relay has no per-client authorization boundary. A client may only
        // reuse a pre-cursor membership diff when this peer retained the view
        // and the receiver echoes its exact server-stamped authorization
        // generation. Fresh, legacy, and tokenless client declarations keep
        // the #1266 authoritative reset.
        let cursor_membership_mismatch = !authorization_matches
            && known_membership_position.is_some_and(|position| {
                fast_cursor_requires_authoritative_reset(
                    position,
                    previous_member_result_set,
                    &current_member_result_set,
                )
            });
        let (program_fact_adds, program_fact_removes, reset_result_set) = if reset_result_set
            && !cursor_membership_mismatch
            && let Some(position) = known_membership_position
            && watermark.0 > 0
            && position >= watermark
        {
            result_member_adds.clear();
            result_member_removes.clear();
            (Vec::new(), Vec::new(), false)
        } else if reset_result_set
            && !cursor_membership_mismatch
            && simple_membership_delta
            && let Some(position) = known_membership_position
            && result_member_adds
                .iter()
                .any(|member| member_settle_position(member).is_some())
        {
            result_member_adds.retain(|member| {
                member_settle_position(member).is_none_or(|settled| settled > position)
            });
            result_member_removes.clear();
            (Vec::new(), Vec::new(), false)
        } else {
            (
                transitions.program_fact_adds,
                transitions.program_fact_removes,
                reset_result_set,
            )
        };
        let bundle_known_state = if cursor_membership_mismatch {
            None
        } else {
            known_state.clone()
        };
        let filter_elapsed = filter_start.elapsed();
        let peer_complete_tx_payloads = self.acknowledged_complete_tx_payloads();
        let result_add_count = result_member_adds.len();
        let result_remove_count = result_member_removes.len();
        let trace_positioned_members = trace_rehydrate.then(|| {
            result_member_adds
                .iter()
                .filter(|member| member_settle_position(member).is_some())
                .count()
        });
        let trace_known_state = trace_rehydrate.then(|| format!("{known_state:?}"));
        let bundle_start = Instant::now();
        if trace_rehydrate {
            node.reset_storage_read_metrics();
        }
        let update = node.view_update_for_maintained_result_members(
            crate::node::MaintainedViewBundleInputs {
                subscription,
                peer_complete_tx_payloads,
                known_state: bundle_known_state,
                complete_exclusive_payloads: self.ship_complete_exclusive_payloads
                    && self.role == PeerRole::Relay,
                previous_result_set: BTreeSet::new(),
                // A non-reset rehydrate retains the receiver's existing fact
                // set, so tuple-source closure must be diffed against it.
                // A reset clears that receiver set before additions apply.
                previous_program_facts: if reset_result_set {
                    BTreeSet::new()
                } else {
                    previous_program_facts
                },
                flat_tuple_source_tables: crate::node::FlatTupleSourceTables::for_query(shape),
                result_member_adds,
                result_member_removes,
                program_fact_adds,
                program_fact_removes,
                identity: self.identity(),
                tier,
                maintained_facts: &maintained,
                allow_storage_witness_fallback: false,
            },
        );
        let mut update = match update.await {
            Ok(update) => update,
            Err(err) => {
                node.unsubscribe_groove_subscription(receiver.id());
                return Err(err);
            }
        };
        let bundle_elapsed = bundle_start.elapsed();
        let bundle_reads = trace_rehydrate.then(|| node.take_storage_read_metrics());
        if reset_result_set {
            view_update_reset_result_set(&mut update);
        }
        if trace_rehydrate {
            let bundle_count = match &update {
                SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                    version_carriers,
                    ..
                }) => view_update_singleton_bundles(version_carriers).len(),
                _ => 0,
            };
            let open_reads = open_reads.expect("trace reads captured");
            let bundle_reads = bundle_reads.expect("trace reads captured");
            let positioned_members = trace_positioned_members.expect("trace positioned members");
            let known_state = trace_known_state.expect("trace known state");
            eprintln!(
                "JAZZ_REHYDRATE_TRACE stage=rehydrate table={} subscription={subscription:?} reset={} known_state={} positioned_members={} open_ms={} filter_ms={} bundle_ms={} raw_adds={} raw_removes={} raw_fact_adds={} adds={} removes={} bundles={} open_reads={} open_ranges={} open_read_buckets={} bundle_reads={} bundle_ranges={}",
                shape.query().table,
                reset_result_set,
                known_state,
                positioned_members,
                open_elapsed.as_millis(),
                filter_elapsed.as_millis(),
                bundle_elapsed.as_millis(),
                raw_add_count,
                raw_remove_count,
                raw_fact_add_count,
                result_add_count,
                result_remove_count,
                bundle_count,
                open_reads.total.reads,
                open_reads.total.ranges,
                storage_read_metrics_buckets(&open_reads),
                bundle_reads.total.reads,
                bundle_reads.total.ranges,
            );
        }
        let maintained_subscription = MaintainedSubscriptionViewSubscription {
            subscription: receiver,
            maintained,
            terminal_schemas,
            tables,
            source_binding_view,
            initial_received: true,
        };
        let state = self.publication_states.entry(subscription).or_default();
        state.maintained_subscription_view = Some(maintained_subscription);
        state.groove_runtime_token = Some(node.groove_runtime_token());
        self.record_outgoing_view_update(&update);
        self.publication_states
            .entry(subscription)
            .or_default()
            .has_served_authorization_progress = true;
        self.metrics.maintained_subscription_view.hits_out += 1;
        self.refresh_maintained_subscription_view_footprint(subscription);
        Ok(Some(update))
    }

    /// Build a reset-result_set current-row view update.
    pub async fn rehydrate_current_rows<S>(
        &mut self,
        node: &mut NodeState<S>,
        table: &str,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        let (shape, binding) = node.whole_table_shape_binding(table)?;
        self.rehydrate_query(node, &shape, &binding).await
    }

    /// Build a reset-result-set query-binding view update.
    pub async fn rehydrate_query<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        self.rehydrate_query_with_opts(node, shape, binding, RegisterShapeOptions::default())
            .await
    }

    /// Build a reset-result-set query-binding view update with registration options.
    pub async fn rehydrate_query_with_opts<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: opts.read_view_key(),
        };
        if let Some(update) = self
            .rehydrate_query_for_subscription_with_opts(
                node,
                subscription,
                shape,
                binding,
                opts,
            )
            .await?
        {
            return Ok(update);
        }
        node.drive_query_runtime().await?;
        self.rehydrate_query_for_subscription_from_maintained_subscription(
            node,
            subscription,
            subscription,
            shape,
        )
        .await?
        .ok_or(Error::InvalidStoredValue(
            "query hydration ended without an initial publication",
        ))
    }

    /// Build a reset-result-set query view update for a usage-site subscription.
    pub async fn rehydrate_query_for_subscription_with_opts<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
    ) -> Result<Option<SyncMessage>, Error>
    where
        S: OrderedKvStorage,
    {
        self.rehydrate_query_for_subscription_with_opts_and_waker(
            node,
            subscription,
            shape,
            binding,
            opts,
            None,
        )
        .await
    }

    /// Owner-loop variant retaining the tick owner's cold-query wake route.
    pub(crate) async fn rehydrate_query_for_subscription_with_opts_and_waker<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<Option<SyncMessage>, Error>
    where
        S: OrderedKvStorage,
    {
        self.rehydrate_query_for_subscription_with_purpose(
            node,
            subscription,
            shape,
            binding,
            opts,
            RehydratePurpose::Query,
            progress_waker,
        )
        .await
    }

    async fn rehydrate_query_for_subscription_with_purpose<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
        purpose: RehydratePurpose,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<Option<SyncMessage>, Error>
    where
        S: OrderedKvStorage,
    {
        self.clear_stale_groove_runtime_handles(node, subscription);
        self.ensure_query_subscription_registered(node, subscription, shape, binding)?;
        let previous_member_result_set = self
            .publication_states
            .get(&subscription)
            .map(PeerSubscriptionState::member_result_set)
            .unwrap_or_default();
        let previous_program_fact_set = self
            .publication_states
            .get(&subscription)
            .map(PeerSubscriptionState::program_fact_set)
            .unwrap_or_default();
        let previous_member_index = self
            .publication_states
            .get(&subscription)
            .map(|state| state.member_index.clone())
            .unwrap_or_default();
        let known_state = self.downstream_known_states.get(&subscription).cloned();
        let retained_authorization = self.publication_states.get(&subscription).and_then(|state| {
            state
                .has_served_authorization_progress
                .then_some(state.authorization_progress)
        });
        self.forget_subscription_with_node(node, subscription);
        if let Some(known_state) = known_state {
            self.downstream_known_states
                .insert(subscription, known_state);
        }
        let plan = node.mark_peer_maintained_query_shape_cache(shape, binding, opts.tier);
        let cached = CachedPeerQueryPlan::with_plan(&opts, plan);
        let (tier, read_view) = cached.context();
        let state = self.publication_states.entry(subscription).or_default();
        state.prepared_query = Some(cached);
        state.groove_runtime_token = Some(node.groove_runtime_token());
        state.result_member_set = previous_member_result_set.clone();
        state.program_fact_set = previous_program_fact_set;
        state.member_index = previous_member_index;
        if let Some(authorization_progress) = retained_authorization {
            state.authorization_progress = authorization_progress;
            state.has_served_authorization_progress = true;
        }
        self.rehydrate_query_maintained_subscription_view(
            node,
            MaintainedRehydrateRequest {
                shape,
                binding,
                subscription,
                previous_member_result_set: &previous_member_result_set,
                reset_result_set: true,
                result_table_filter: None,
                tier,
                read_view: &read_view,
                purpose,
            },
            progress_waker,
        )
        .await
    }

    /// Hydrate an authority-owned authorization proof using its admitted
    /// permission subject. A trusted backend link normally serves ordinary
    /// reads as SYSTEM, but that bypass must not leak into a proof of a
    /// particular session's policy clauses.
    pub(crate) async fn rehydrate_authorization_support_query_for_identity<S>(
        &mut self,
        node: &mut NodeState<S>,
        identity: AuthorSubject,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        let previous_role = self.role;
        let previous_permission_identity = self.permission_identity;
        self.role = PeerRole::ClientLink { identity };
        self.permission_identity = Some(identity);
        let update = self
            .rehydrate_query_for_subscription_with_purpose(
                node,
                subscription,
                shape,
                binding,
                opts,
                RehydratePurpose::AuthorizationSupport,
                None,
            )
            .await
            .and_then(|update| {
                update.ok_or(Error::InvalidStoredValue(
                    "authorization hydration suspended outside an owner-loop subscription",
                ))
            });
        self.role = previous_role;
        self.permission_identity = previous_permission_identity;
        update
    }

    /// Build a usage-site update from an already-maintained canonical subscription.
    pub async fn rehydrate_query_for_subscription_from_maintained_subscription<S>(
        &mut self,
        node: &mut NodeState<S>,
        maintained_subscription: SubscriptionKey,
        target_subscription: SubscriptionKey,
        shape: &ValidatedQuery,
    ) -> Result<Option<SyncMessage>, Error>
    where
        S: OrderedKvStorage,
    {
        self.rehydrate_query_for_subscription_from_maintained_subscription_and_waker(
            node,
            maintained_subscription,
            target_subscription,
            shape,
            None,
        )
        .await
    }

    pub(crate) async fn rehydrate_query_for_subscription_from_maintained_subscription_and_waker<S>(
        &mut self,
        node: &mut NodeState<S>,
        maintained_subscription: SubscriptionKey,
        target_subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<Option<SyncMessage>, Error>
    where
        S: OrderedKvStorage,
    {
        self.clear_stale_groove_runtime_handles(node, maintained_subscription);
        let source_transitions = self.drain_maintained_subscription_view_changes(
            node,
            shape,
            maintained_subscription,
            None,
            progress_waker,
        )
        .await?;
        if !self
            .publication_states
            .get(&maintained_subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .is_some_and(|maintained| maintained.initial_received)
        {
            return Ok(None);
        }
        let ResultTransitions {
            authoritative_membership_changed: _,
            authoritative_member_adds: _,
            adds: source_adds,
            removes: source_removes,
            result_payload_adds: _,
            result_payload_removes: _,
            program_fact_adds: source_program_fact_adds,
            program_fact_removes: source_program_fact_removes,
            allow_storage_witness_fallback: source_allow_storage_witness_fallback,
            observed_result_delta_batches: _,
            requires_authoritative_membership_reconcile: _,
            terminal_operations: source_terminal_operations,
        } = source_transitions;
        let known_state = self
            .downstream_known_states
            .get(&target_subscription)
            .cloned();
        let known_membership_position = fast_current_membership_position(&known_state);
        let authorization_matches =
            self.fast_cursor_authorization_matches(maintained_subscription, &known_state);
        let removed_members_are_ordinary =
            source_removes.iter().all(ordinary_current_content_member);
        let source_had_program_fact_transitions =
            !source_program_fact_adds.is_empty() || !source_program_fact_removes.is_empty();
        let client_link = self.role != PeerRole::Relay;
        let flat_row_removes = (client_link
            && authorization_matches
            && removed_members_are_ordinary
            && maintained_subscription.read_view
                == RegisterShapeOptions::default().read_view_key()
            && !source_had_program_fact_transitions
            && self.publication_states[&maintained_subscription]
                .program_fact_set
                .is_empty())
        .then(|| source_removes.clone());
        if !source_adds.is_empty()
            || !source_removes.is_empty()
            || !source_terminal_operations.is_empty()
            || !source_program_fact_adds.is_empty()
            || !source_program_fact_removes.is_empty()
        {
            self.apply_outgoing_view_update_result_set(&SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                subscription: maintained_subscription,
                settled_through: canonical_subscription_settlement_time(
                    node,
                    maintained_subscription,
                ),
                reset_result_set: false,
                version_carriers: Vec::new(),
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                result_member_adds: source_adds,
                result_member_removes: source_removes,
                terminal_operations: source_terminal_operations,
                program_fact_adds: source_program_fact_adds,
                program_fact_removes: source_program_fact_removes,
            }));
        }
        let canonical_state = self
            .publication_states
            .get(&maintained_subscription)
            .ok_or(Error::InvalidStoredValue(
                "coverage group subscription is missing peer state",
            ))?;
        let current_result_member_set = &canonical_state.result_member_set;
        let can_forward_flat_removals = client_link
            && ordinary_flat_row_duplicate_view(
                shape,
                &current_result_member_set,
                removed_members_are_ordinary,
                maintained_subscription.read_view,
                canonical_state.program_fact_set.is_empty(),
                source_had_program_fact_transitions,
            );
        let authorization_mismatch = client_link
            && known_membership_position.is_some()
            && !authorization_matches;
        let target_result_member_removes = if can_forward_flat_removals && authorization_matches {
            flat_row_removes.unwrap_or_default()
        } else {
            Vec::new()
        };
        let tier = self
            .publication_states
            .get(&maintained_subscription)
            .and_then(|state| state.prepared_query.as_ref())
            .map(CachedPeerQueryPlan::tier)
            .ok_or(Error::InvalidStoredValue(
                "coverage group subscription is missing prepared state",
            ))?;
        let peer_complete_tx_payloads = self.acknowledged_complete_tx_payloads();
        let mut reset_result_set = true;
        let result_member_adds = if !authorization_mismatch
            && let Some(position) = known_membership_position
            && canonical_subscription_settlement_time(node, maintained_subscription).0 > 0
            && position >= canonical_subscription_settlement_time(node, maintained_subscription)
        {
            reset_result_set = false;
            Vec::new()
        } else if !authorization_mismatch
            && let Some(position) = known_membership_position
            && current_result_member_set
                .iter()
                .any(|member| member_settle_position(member).is_some())
        {
            reset_result_set = false;
            current_result_member_set
                .iter()
                .filter(|member| {
                    member_settle_position(member).is_none_or(|settled| settled > position)
                })
                .cloned()
                .collect()
        } else {
            current_result_member_set.iter().cloned().collect()
        };
        let update = {
            let maintained = &self
                .publication_states
                .get(&maintained_subscription)
                .and_then(|state| state.maintained_subscription_view.as_ref())
                .ok_or(Error::InvalidStoredValue(
                    "coverage group subscription is missing maintained state",
                ))?
                .maintained;
            node.view_update_for_maintained_result_members(
                crate::node::MaintainedViewBundleInputs {
                    subscription: target_subscription,
                    peer_complete_tx_payloads,
                    known_state: (!authorization_mismatch)
                        .then_some(known_state)
                        .flatten(),
                    complete_exclusive_payloads: self.ship_complete_exclusive_payloads
                        && self.role == PeerRole::Relay,
                    previous_result_set: BTreeSet::new(),
                    previous_program_facts: BTreeSet::new(),
                    // Rehydration forwards the canonical membership through a
                    // new downstream subscription, but its fact vocabulary is
                    // still the validated query's vocabulary. Flat tuples need
                    // one contributor role per joined source so the receiver's
                    // ordinary one-shot path can rebuild the same tuple from
                    // immutable source versions instead of depending only on
                    // the reset payload.
                    flat_tuple_source_tables:
                        crate::node::FlatTupleSourceTables::for_query(shape),
                    result_member_adds,
                    result_member_removes: target_result_member_removes,
                    program_fact_adds: Vec::new(),
                    program_fact_removes: Vec::new(),
                    identity: self.identity(),
                    tier,
                    maintained_facts: maintained,
                    allow_storage_witness_fallback: source_allow_storage_witness_fallback,
                },
            )
        };
        let mut update = update.await?;
        if reset_result_set {
            view_update_reset_result_set(&mut update);
        }
        self.record_outgoing_view_update_metadata(&update);
        self.metrics.maintained_subscription_view.hits_out += 1;
        self.refresh_maintained_subscription_view_footprint(maintained_subscription);
        Ok(Some(update))
    }

}
