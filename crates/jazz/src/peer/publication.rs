impl PeerState {
    fn fast_cursor_authorization_matches(
        &self,
        subscription: SubscriptionKey,
        known_state: &Option<KnownStateDeclaration>,
    ) -> bool {
        match self.role {
            PeerRole::Relay => true,
            PeerRole::ClientLink { .. } => {
                self.subscriptions.get(&subscription).is_some_and(|state| {
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
    pub fn client_link(identity: AuthorId) -> Self {
        Self {
            role: PeerRole::ClientLink { identity },
            ..Self::default()
        }
    }

    /// Construct an edge-boundary peer that terminates one client author identity.
    pub fn edge_client(identity: AuthorId) -> Self {
        Self::client_link(identity)
    }

    /// Construct an edge peer whose wire identity and read-policy identity differ.
    ///
    /// Trusted backend websocket links still speak as their concrete peer identity
    /// for session/resume validation, but served reads must bypass row policies.
    pub fn edge_client_with_permission_identity(
        identity: AuthorId,
        permission_identity: AuthorId,
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
    pub fn link_identity(&self) -> AuthorId {
        self.role.identity()
    }

    /// Return the identity used to evaluate reads on this peer link.
    pub fn identity(&self) -> AuthorId {
        self.permission_identity
            .unwrap_or_else(|| self.role.identity())
    }

    fn clear_stale_groove_runtime_handles<S>(
        &mut self,
        node: &NodeState<S>,
        subscription: SubscriptionKey,
    ) where
        S: ResidentStorage,
    {
        let current_token = node.groove_runtime_token();
        if self.subscriptions.get(&subscription).is_some_and(|state| {
            state
                .groove_runtime_token
                .is_some_and(|token| token != current_token)
        }) {
            if let Some(state) = self.subscriptions.get_mut(&subscription) {
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
        S: ResidentStorage,
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
    pub fn current_rows_update<S>(
        &mut self,
        node: &mut NodeState<S>,
        table: &str,
    ) -> Result<SyncMessage, Error>
    where
        S: ResidentStorage,
    {
        let (shape, binding) = node.whole_table_shape_binding(table)?;
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: RegisterShapeOptions::default().read_view_key(),
        };
        self.clear_stale_groove_runtime_handles(node, subscription);
        self.ensure_query_subscription_registered(node, subscription, &shape, &binding)?;
        let needs_prepare = self
            .subscriptions
            .get(&subscription)
            .and_then(|state| state.prepared_query.as_ref())
            .is_none();
        if needs_prepare {
            let plan = node.mark_peer_maintained_query_shape_cache(
                &shape,
                &binding,
                DurabilityTier::Global,
            );
            let cached = CachedPeerQueryPlan::with_plan(DurabilityTier::Global, plan);
            let state = self.subscriptions.entry(subscription).or_default();
            state.prepared_query = Some(cached);
            state.groove_runtime_token = Some(node.groove_runtime_token());
        } else {
            self.subscriptions.entry(subscription).or_default();
        }
        let previous_member_result_set = self
            .subscriptions
            .get(&subscription)
            .map(PeerSubscriptionState::member_result_set)
            .unwrap_or_default();
        if self
            .subscriptions
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .is_none()
        {
            return self.rehydrate_query_maintained_subscription_view(
                node,
                MaintainedRehydrateRequest {
                    shape: &shape,
                    binding: &binding,
                    subscription,
                    previous_member_result_set: &previous_member_result_set,
                    reset_result_set: false,
                    result_table_filter: Some(table),
                    tier: DurabilityTier::Global,
                    read_view: &ReadViewSpec::default(),
                    purpose: RehydratePurpose::Query,
                },
            );
        }
        if self
            .subscriptions
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .is_some()
        {
            return self.query_update_maintained_subscription_view(
                node,
                &shape,
                &binding,
                subscription,
                Some(table),
                true,
            );
        }
        unreachable!("maintained subscription view state is either absent or present")
    }

    /// Builds a query-binding view update, using tx-level refs for complete
    /// transaction payloads in this peer's inventory and bundles for new or
    /// partial view payload.
    pub fn query_update<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<SyncMessage, Error>
    where
        S: ResidentStorage,
    {
        self.query_update_inner(node, shape, binding)
    }

    /// Build an incremental view update addressed to a usage-site subscription.
    pub fn query_update_for_subscription<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<SyncMessage, Error>
    where
        S: ResidentStorage,
    {
        self.query_update_for_subscription_with_opts(
            node,
            subscription,
            shape,
            binding,
            RegisterShapeOptions::default(),
        )
    }

    /// Build an incremental view update addressed to a usage-site subscription,
    /// preserving the read view and tier used when the shape was registered.
    pub fn query_update_for_subscription_with_opts<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
    ) -> Result<SyncMessage, Error>
    where
        S: ResidentStorage,
    {
        self.query_update_inner_for_subscription(node, subscription, shape, binding, opts, true)
    }

    /// Build an incremental view update after the caller has already flushed
    /// the shared Groove runtime for this refresh.
    pub(crate) fn query_update_for_subscription_with_opts_after_runtime_flush<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
    ) -> Result<SyncMessage, Error>
    where
        S: ResidentStorage,
    {
        self.query_update_inner_for_subscription(node, subscription, shape, binding, opts, false)
    }

    fn query_update_inner<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<SyncMessage, Error>
    where
        S: ResidentStorage,
    {
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: Default::default(),
        };
        self.query_update_inner_for_subscription(
            node,
            subscription,
            shape,
            binding,
            RegisterShapeOptions::default(),
            true,
        )
    }

    fn query_update_inner_for_subscription<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
        flush_query_runtime: bool,
    ) -> Result<SyncMessage, Error>
    where
        S: ResidentStorage,
    {
        self.clear_stale_groove_runtime_handles(node, subscription);
        self.ensure_query_subscription_registered(node, subscription, shape, binding)?;
        let Some(state) = self.subscriptions.get(&subscription) else {
            return Ok(SyncMessage::ViewUpdate {
                subscription,
                settled_through: node.applied_global_watermark(),
                reset_result_set: false,
                version_carriers: Vec::new(),
                version_bundles: Vec::new(),
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
                terminal_operations: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            });
        };
        if state.maintained_subscription_view.is_some() {
            return self.query_update_maintained_subscription_view(
                node,
                shape,
                binding,
                subscription,
                None,
                flush_query_runtime,
            );
        }
        let previous_member_result_set = self
            .subscriptions
            .get(&subscription)
            .map(PeerSubscriptionState::member_result_set)
            .unwrap_or_default();
        if self
            .subscriptions
            .get(&subscription)
            .and_then(|state| state.prepared_query.as_ref())
            .is_none()
        {
            let plan = node.mark_peer_maintained_query_shape_cache(shape, binding, opts.tier);
            let state = self.subscriptions.entry(subscription).or_default();
            state.prepared_query = Some(CachedPeerQueryPlan::with_plan(opts.tier, plan));
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
                tier: opts.tier,
                read_view: &opts.read_view,
                purpose: RehydratePurpose::Query,
            },
        )
    }

    fn query_update_maintained_subscription_view<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        _binding: &Binding,
        subscription: SubscriptionKey,
        result_table_filter: Option<&str>,
        flush_query_runtime: bool,
    ) -> Result<SyncMessage, Error>
    where
        S: ResidentStorage,
    {
        let trace_rehydrate = std::env::var_os("JAZZ_REHYDRATE_TRACE").is_some();
        let trace_start = Instant::now();
        if trace_rehydrate {
            node.reset_storage_read_metrics();
        }
        let transitions = self
            .subscriptions
            .get_mut(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_mut())
            .and_then(|maintained| maintained.pending_transitions.take())
            .map(Ok)
            .unwrap_or_else(|| {
                self.drain_maintained_subscription_view_changes(
                    node,
                    shape,
                    subscription,
                    result_table_filter,
                    flush_query_runtime,
                )
            })?;
        let retry_transitions = transitions.clone();
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
            structured_app_row_changes: _,
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
            .subscriptions
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
            let tier = self
                .subscriptions
                .get(&subscription)
                .and_then(|state| state.prepared_query.as_ref())
                .map(CachedPeerQueryPlan::tier)
                .ok_or(Error::InvalidStoredValue(
                    "maintained subscription view is missing prepared state",
                ))?;
            return self.rehydrate_query_maintained_subscription_view(
                node,
                MaintainedRehydrateRequest {
                    shape,
                    binding: _binding,
                    subscription,
                    previous_member_result_set: &previous_member_result_set,
                    reset_result_set: false,
                    result_table_filter,
                    tier,
                    read_view: &ReadViewSpec::default(),
                    purpose: RehydratePurpose::Query,
                },
            );
        }
        if let Some(state) = self.subscriptions.get(&subscription) {
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
            return Ok(SyncMessage::ViewUpdate {
                subscription,
                settled_through: node.applied_global_watermark(),
                reset_result_set: false,
                version_carriers: Vec::new(),
                version_bundles: Vec::new(),
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
                terminal_operations: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            });
        }
        let previous_result_tx_ids = previous_member_result_set
            .iter()
            .filter_map(ResultMemberEntry::as_row)
            .map(|(_, _, tx_id)| tx_id)
            .collect::<BTreeSet<_>>();
        let tier = self
            .subscriptions
            .get(&subscription)
            .and_then(|state| state.prepared_query.as_ref())
            .map(CachedPeerQueryPlan::tier)
            .ok_or(Error::InvalidStoredValue(
                "maintained subscription view is missing prepared state",
            ))?;
        let peer_complete_tx_payloads = self.acknowledged_complete_tx_payloads();
        let known_state = self
            .subscriptions
            .get(&subscription)
            .and_then(|state| state.known_state.clone());
        let previous_program_facts = self
            .subscriptions
            .get(&subscription)
            .map(PeerSubscriptionState::program_fact_set)
            .unwrap_or_default();
        let bundle_start = Instant::now();
        if trace_rehydrate {
            node.reset_storage_read_metrics();
        }
        let update = {
            let maintained = &self
                .subscriptions
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
                    complete_exclusive_payloads: self.ship_complete_exclusive_payloads,
                    previous_result_set: previous_result_tx_ids,
                    previous_program_facts,
                    flat_tuple_source_tables: flat_tuple_source_tables(shape),
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
        let mut update = match update {
            Ok(update) => update,
            Err(error) => {
                if let Some(maintained) = self
                    .subscriptions
                    .get_mut(&subscription)
                    .and_then(|state| state.maintained_subscription_view.as_mut())
                {
                    maintained.pending_transitions = Some(retry_transitions);
                }
                return Err(error);
            }
        };
        if let SyncMessage::ViewUpdate {
            terminal_operations: outgoing,
            ..
        } = &mut update
        {
            *outgoing = terminal_operations;
        }
        let bundle_elapsed = bundle_start.elapsed();
        let bundle_reads = trace_rehydrate.then(|| node.take_storage_read_metrics());
        if trace_rehydrate {
            let bundle_count = match &update {
                SyncMessage::ViewUpdate {
                    version_carriers,
                    version_bundles,
                    ..
                } => view_update_singleton_bundles(version_carriers, version_bundles).len(),
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
        Ok(update)
    }

    fn drain_maintained_subscription_view_changes<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        subscription: SubscriptionKey,
        result_table_filter: Option<&str>,
        flush_query_runtime: bool,
    ) -> Result<ResultTransitions, Error>
    where
        S: ResidentStorage,
    {
        if flush_query_runtime {
            node.flush_query_runtime()?;
        }
        let previous_member_result_set = self
            .subscriptions
            .get(&subscription)
            .map(PeerSubscriptionState::member_result_set)
            .unwrap_or_default();
        let previous_program_fact_set = self
            .subscriptions
            .get(&subscription)
            .map(PeerSubscriptionState::program_fact_set)
            .unwrap_or_default();
        let output_tables = self
            .subscriptions
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .map(|maintained| maintained.tables.clone())
            .unwrap_or_default();
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
                .subscriptions
                .get_mut(&subscription)
                .and_then(|state| state.maintained_subscription_view.as_mut())
            else {
                return Ok(ResultTransitions::default());
            };
            loop {
                match maintained_subscription_view.subscription.try_recv() {
                    Ok(deltas) => {
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
            structured_app_row_changes: BTreeSet::new(),
            allow_storage_witness_fallback,
            observed_result_delta_batches,
            requires_authoritative_membership_reconcile,
            terminal_operations,
        })
    }

    fn rehydrate_query_maintained_subscription_view<S>(
        &mut self,
        node: &mut NodeState<S>,
        request: MaintainedRehydrateRequest<'_>,
    ) -> Result<SyncMessage, Error>
    where
        S: ResidentStorage,
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
        let opened = match purpose {
            RehydratePurpose::Query => node.open_seeded_maintained_subscription_view(
                shape,
                binding,
                self.identity(),
                tier,
                read_view,
            ),
            RehydratePurpose::AuthorizationSupport => node
                .open_seeded_authorization_support_subscription_view(
                    shape,
                    binding,
                    self.identity(),
                    tier,
                    read_view,
                ),
        };
        let (receiver, maintained, terminal_schemas, transitions, tables) = match opened {
            Ok(opened) => opened,
            Err(Error::AuthorizationSupportMissingClaim(_))
                if purpose == RehydratePurpose::AuthorizationSupport =>
            {
                let update = SyncMessage::ViewUpdate {
                    subscription,
                    settled_through: node.applied_global_watermark(),
                    reset_result_set,
                    version_carriers: Vec::new(),
                    version_bundles: Vec::new(),
                    peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                    result_member_adds: Vec::new(),
                    result_member_removes: previous_member_result_set.iter().cloned().collect(),
                    terminal_operations: Vec::new(),
                    program_fact_adds: Vec::new(),
                    program_fact_removes: Vec::new(),
                };
                self.record_outgoing_view_update(&update);
                self.subscriptions
                    .entry(subscription)
                    .or_default()
                    .has_served_authorization_progress = true;
                return Ok(update);
            }
            Err(error) => return Err(error),
        };
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
        let known_state = self
            .subscriptions
            .get(&subscription)
            .and_then(|state| state.known_state.clone());
        let previous_program_facts = self
            .subscriptions
            .get(&subscription)
            .map(PeerSubscriptionState::program_fact_set)
            .unwrap_or_default();
        let known_membership_position = fast_current_membership_position(&known_state);
        let authorization_matches =
            self.fast_cursor_authorization_matches(subscription, &known_state);
        let watermark = node.applied_global_watermark();
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
                complete_exclusive_payloads: self.ship_complete_exclusive_payloads,
                previous_result_set: BTreeSet::new(),
                // A non-reset rehydrate retains the receiver's existing fact
                // set, so tuple-source closure must be diffed against it.
                // A reset clears that receiver set before additions apply.
                previous_program_facts: if reset_result_set {
                    BTreeSet::new()
                } else {
                    previous_program_facts
                },
                flat_tuple_source_tables: flat_tuple_source_tables(shape),
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
        let bundle_elapsed = bundle_start.elapsed();
        let bundle_reads = trace_rehydrate.then(|| node.take_storage_read_metrics());
        let mut update = match update {
            Ok(update) => update,
            Err(err) => {
                node.unsubscribe_groove_subscription(receiver.id());
                return Err(err);
            }
        };
        if reset_result_set {
            view_update_reset_result_set(&mut update);
        }
        if trace_rehydrate {
            let bundle_count = match &update {
                SyncMessage::ViewUpdate {
                    version_carriers,
                    version_bundles,
                    ..
                } => view_update_singleton_bundles(version_carriers, version_bundles).len(),
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
            pending_transitions: None,
        };
        let state = self.subscriptions.entry(subscription).or_default();
        state.maintained_subscription_view = Some(maintained_subscription);
        state.groove_runtime_token = Some(node.groove_runtime_token());
        self.record_outgoing_view_update(&update);
        self.subscriptions
            .entry(subscription)
            .or_default()
            .has_served_authorization_progress = true;
        self.metrics.maintained_subscription_view.hits_out += 1;
        self.refresh_maintained_subscription_view_footprint(subscription);
        Ok(update)
    }

    /// Build a reset-result_set current-row view update.
    pub fn rehydrate_current_rows<S>(
        &mut self,
        node: &mut NodeState<S>,
        table: &str,
    ) -> Result<SyncMessage, Error>
    where
        S: ResidentStorage,
    {
        let (shape, binding) = node.whole_table_shape_binding(table)?;
        self.rehydrate_query(node, &shape, &binding)
    }

    /// Build a reset-result-set query-binding view update.
    pub fn rehydrate_query<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<SyncMessage, Error>
    where
        S: ResidentStorage,
    {
        self.rehydrate_query_with_opts(node, shape, binding, RegisterShapeOptions::default())
    }

    /// Build a reset-result-set query-binding view update with registration options.
    pub fn rehydrate_query_with_opts<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
    ) -> Result<SyncMessage, Error>
    where
        S: ResidentStorage,
    {
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: opts.read_view_key(),
        };
        self.rehydrate_query_for_subscription_with_opts(node, subscription, shape, binding, opts)
    }

    /// Build a reset-result-set query view update for a usage-site subscription.
    pub fn rehydrate_query_for_subscription_with_opts<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
    ) -> Result<SyncMessage, Error>
    where
        S: ResidentStorage,
    {
        self.rehydrate_query_for_subscription_with_purpose(
            node,
            subscription,
            shape,
            binding,
            opts,
            RehydratePurpose::Query,
        )
    }

    fn rehydrate_query_for_subscription_with_purpose<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
        purpose: RehydratePurpose,
    ) -> Result<SyncMessage, Error>
    where
        S: ResidentStorage,
    {
        self.clear_stale_groove_runtime_handles(node, subscription);
        self.ensure_query_subscription_registered(node, subscription, shape, binding)?;
        let previous_member_result_set = self
            .subscriptions
            .get(&subscription)
            .map(PeerSubscriptionState::member_result_set)
            .unwrap_or_default();
        let previous_program_fact_set = self
            .subscriptions
            .get(&subscription)
            .map(PeerSubscriptionState::program_fact_set)
            .unwrap_or_default();
        let previous_member_index = self
            .subscriptions
            .get(&subscription)
            .map(|state| state.member_index.clone())
            .unwrap_or_default();
        let known_state = self
            .subscriptions
            .get(&subscription)
            .and_then(|state| state.known_state.clone());
        let retained_authorization = self.subscriptions.get(&subscription).and_then(|state| {
            state
                .has_served_authorization_progress
                .then_some(state.authorization_progress)
        });
        self.forget_subscription_with_node(node, subscription);
        let plan = node.mark_peer_maintained_query_shape_cache(shape, binding, opts.tier);
        let cached = CachedPeerQueryPlan::with_plan(opts.tier, plan);
        let state = self.subscriptions.entry(subscription).or_default();
        state.prepared_query = Some(cached);
        state.groove_runtime_token = Some(node.groove_runtime_token());
        state.known_state = known_state;
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
                tier: opts.tier,
                read_view: &opts.read_view,
                purpose,
            },
        )
    }

    pub(crate) fn rehydrate_authorization_support_query<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
    ) -> Result<SyncMessage, Error>
    where
        S: ResidentStorage,
    {
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: opts.read_view_key(),
        };
        self.rehydrate_query_for_subscription_with_purpose(
            node,
            subscription,
            shape,
            binding,
            opts,
            RehydratePurpose::AuthorizationSupport,
        )
    }

    /// Build a reset-result-set update for a usage-site subscription from an
    /// already-maintained canonical subscription.
    pub fn rehydrate_query_for_subscription_from_maintained_subscription<S>(
        &mut self,
        node: &mut NodeState<S>,
        maintained_subscription: SubscriptionKey,
        target_subscription: SubscriptionKey,
        shape: &ValidatedQuery,
    ) -> Result<SyncMessage, Error>
    where
        S: ResidentStorage,
    {
        self.clear_stale_groove_runtime_handles(node, maintained_subscription);
        let source_transitions = self.drain_maintained_subscription_view_changes(
            node,
            shape,
            maintained_subscription,
            None,
            true,
        )?;
        let ResultTransitions {
            authoritative_membership_changed: _,
            authoritative_member_adds: _,
            adds: source_adds,
            removes: source_removes,
            result_payload_adds: _,
            result_payload_removes: _,
            program_fact_adds: source_program_fact_adds,
            program_fact_removes: source_program_fact_removes,
            structured_app_row_changes: _,
            allow_storage_witness_fallback: source_allow_storage_witness_fallback,
            observed_result_delta_batches: _,
            requires_authoritative_membership_reconcile: _,
            terminal_operations: source_terminal_operations,
        } = source_transitions;
        if !source_adds.is_empty()
            || !source_removes.is_empty()
            || !source_terminal_operations.is_empty()
            || !source_program_fact_adds.is_empty()
            || !source_program_fact_removes.is_empty()
        {
            self.apply_outgoing_view_update_result_set(&SyncMessage::ViewUpdate {
                subscription: maintained_subscription,
                settled_through: node.applied_global_watermark(),
                reset_result_set: false,
                version_carriers: Vec::new(),
                version_bundles: Vec::new(),
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                result_member_adds: source_adds,
                result_member_removes: source_removes,
                terminal_operations: source_terminal_operations,
                program_fact_adds: source_program_fact_adds,
                program_fact_removes: source_program_fact_removes,
            });
        }
        let mut result_member_adds = self
            .subscriptions
            .get(&maintained_subscription)
            .ok_or(Error::InvalidStoredValue(
                "coverage group subscription is missing peer state",
            ))?
            .member_result_set()
            .into_iter()
            .collect::<Vec<_>>();
        let tier = self
            .subscriptions
            .get(&maintained_subscription)
            .and_then(|state| state.prepared_query.as_ref())
            .map(CachedPeerQueryPlan::tier)
            .ok_or(Error::InvalidStoredValue(
                "coverage group subscription is missing prepared state",
            ))?;
        let peer_complete_tx_payloads = self.acknowledged_complete_tx_payloads();
        let known_state = self
            .subscriptions
            .get(&target_subscription)
            .and_then(|state| state.known_state.clone());
        let known_membership_position = fast_current_membership_position(&known_state);
        let mut reset_result_set = true;
        if let Some(position) = known_membership_position
            && node.applied_global_watermark().0 > 0
            && position >= node.applied_global_watermark()
        {
            result_member_adds.clear();
            reset_result_set = false;
        } else if let Some(position) = known_membership_position
            && result_member_adds
                .iter()
                .any(|member| member_settle_position(member).is_some())
        {
            result_member_adds.retain(|member| {
                member_settle_position(member).is_none_or(|settled| settled > position)
            });
            reset_result_set = false;
        }
        let update = {
            let maintained = &self
                .subscriptions
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
                    known_state,
                    complete_exclusive_payloads: self.ship_complete_exclusive_payloads,
                    previous_result_set: BTreeSet::new(),
                    previous_program_facts: BTreeSet::new(),
                    flat_tuple_source_tables: Vec::new(),
                    result_member_adds,
                    result_member_removes: Vec::new(),
                    program_fact_adds: Vec::new(),
                    program_fact_removes: Vec::new(),
                    identity: self.identity(),
                    tier,
                    maintained_facts: maintained,
                    allow_storage_witness_fallback: source_allow_storage_witness_fallback,
                },
            )
        };
        let mut update = update?;
        if reset_result_set {
            view_update_reset_result_set(&mut update);
        }
        self.record_outgoing_view_update_metadata(&update);
        self.metrics.maintained_subscription_view.hits_out += 1;
        self.refresh_maintained_subscription_view_footprint(maintained_subscription);
        Ok(update)
    }

}
