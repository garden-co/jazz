impl PeerState {
    /// Ingest a client mergeable commit unit at an edge boundary.
    ///
    /// The edge gates admission on the first settled permission-scope
    /// subscription for the affected tables and writer. If a scope was not
    /// settled before this call, the unit remains outside edge history and can
    /// be admitted exactly once by
    /// [`Self::drain_deferred_edge_fates`] after the registered scope settles.
    pub async fn ingest_edge_mergeable_commit_unit<S>(
        &mut self,
        node: &mut NodeState<S>,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error>
    where
        S: OrderedKvStorage + ReopenableStorage,
    {
        self.evict_idle_edge_scope_subscriptions(node, now_ms);
        if tx.kind != TxKind::Mergeable {
            return Err(Error::UnsupportedCommitUnit(
                "edge fate deferral only supports mergeable commit units",
            ));
        }
        let permission_identity = self.identity();
        if let Some(scope_subscriptions) = self.unsettled_authority_scope_subscriptions(
            node,
            permission_identity,
            &versions,
            Some(tx.tx_id),
            true,
        )
        .await?
        {
            if let Some(existing) = self.deferred_edge_fates.get(&tx.tx_id) {
                // The durable ingest path rejects two different commit units
                // for one transaction id.  Deferred admission sits before that
                // path, so retain the same conflict boundary here rather than
                // silently treating a conflicting upload as a retransmit.
                // Version order is transport-insignificant and is normalized
                // by NodeState on eventual admission.
                let mut existing_versions = existing.versions.clone();
                existing_versions.sort();
                let mut incoming_versions = versions;
                incoming_versions.sort();
                if existing.tx != tx || existing_versions != incoming_versions {
                    return Err(Error::ConflictingCommitUnit(tx.tx_id));
                }
            } else {
                for subscription in &scope_subscriptions {
                    self.retain_edge_scope_subscription(*subscription);
                }
                self.deferred_edge_fates.insert(
                    tx.tx_id,
                    DeferredEdgeFate {
                        tx,
                        versions,
                        now_ms,
                        permission_identity,
                        scope_subscriptions,
                    },
                );
            }
            return Ok(PublicationOutcome::settled(Vec::new()));
        }
        node.ingest_edge_authority_mergeable_commit_unit_with_identity(
            tx,
            versions,
            now_ms,
            permission_identity,
        )
        .await
    }

    /// Assign fates for edge-ingested writes whose permission scopes have now
    /// delivered an initial settled result.
    pub async fn drain_deferred_edge_fates<S>(
        &mut self,
        node: &mut NodeState<S>,
        now_ms: u64,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error>
    where
        S: OrderedKvStorage + ReopenableStorage,
    {
        self.evict_idle_edge_scope_subscriptions(node, now_ms);
        let deferred = self
            .deferred_edge_fates
            .iter()
            .map(|(tx_id, fate)| (*tx_id, fate.clone()))
            .collect::<Vec<_>>();
        let mut updates = PublicationOutcome::settled(Vec::new());
        for (tx_id, fate) in deferred {
            if self
                .unsettled_authority_scope_subscriptions(
                    node,
                    fate.permission_identity,
                    &fate.versions,
                    Some(tx_id),
                    false,
                )
                .await?
                .is_some()
            {
                continue;
            }
            self.deferred_edge_fates.remove(&tx_id);
            for subscription in fate.scope_subscriptions {
                self.release_edge_scope_subscription(node, subscription, now_ms);
            }
            updates.extend(
                node.ingest_edge_authority_mergeable_commit_unit_with_identity(
                    fate.tx,
                    fate.versions,
                    fate.now_ms,
                    fate.permission_identity,
                )
                .await?,
            );
        }
        Ok(updates)
    }

    /// Number of edge fate assignments currently parked on permission scopes.
    pub fn deferred_edge_fate_count(&self) -> usize {
        self.deferred_edge_fates.len()
    }

    /// Number of distinct permission-scope subscriptions retained by deferred
    /// edge fate gates.
    pub fn edge_scope_subscription_count(&self) -> usize {
        self.edge_scope_subscription_refs.len()
    }

    fn record_outgoing_view_update_metadata(&mut self, update: &SyncMessage) {
        let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            version_carriers,
            peer_payload_inventory,
            result_member_adds,
            result_member_removes,
            ..
        }) = update
        else {
            return;
        };

        let singleton_bundles = view_update_singleton_bundles(version_carriers);
        self.metrics.view_updates_out += 1;
        self.metrics.version_bundles_out += singleton_bundles.len() as u64;
        self.metrics.complete_tx_payload_refs_out +=
            peer_payload_inventory.complete_tx_payloads.len() as u64;
        self.metrics.result_adds_out += result_member_adds.len() as u64;
        self.metrics.result_removes_out += result_member_removes.len() as u64;

        self.metrics.duplicate_version_bundles_out += singleton_bundles
            .iter()
            .filter(|bundle| bundle_contains_complete_tx_payload(bundle))
            .filter(|bundle| self.shipped_complete_tx_payloads.contains(&bundle.tx.tx_id))
            .count() as u64;
    }

    /// Establish the same all-clause aggregate proof used by wire advice
    /// before a terminal authority admits a client commit.  The action list is
    /// reconstructed by `NodeState` from the actual version records, so
    /// insert, update (including candidate patch), and delete each compile the
    /// correct policy clauses rather than sharing a placeholder update.
    pub(crate) async fn prove_terminal_commit_authorization<S>(
        &mut self,
        node: &mut NodeState<S>,
        writer: AuthorSubject,
        versions: &[VersionRecord],
        candidate_tx_id: TxId,
    ) -> Result<(), Error>
    where
        S: OrderedKvStorage,
    {
        for action in node
            .authorization_actions_for_versions_in_transaction(versions, Some(candidate_tx_id))
            .await?
        {
            let scope = node.authorization_support_scope(writer, &action)?;
            if scope.subscriptions.is_empty() {
                continue;
            }
            let mut aggregate = AuthorityScopeAggregate::new(
                scope
                    .subscriptions
                    .iter()
                    .map(|(shape, binding)| (shape.shape_id(), binding.binding_id()))
                    .collect(),
            );
            for (shape, binding) in scope.subscriptions {
                let subscription = SubscriptionKey {
                    shape_id: shape.shape_id(),
                    binding_id: binding.binding_id(),
                    read_view: scope.options.read_view_key(),
                };
                if !aggregate.register(subscription, (shape.shape_id(), binding.binding_id())) {
                    continue;
                }
                let (cut, progress) = if self
                    .publication_states
                    .get(&subscription)
                    .is_some_and(|state| state.maintained_subscription_view.is_some())
                {
                    (
                        node.committed_global_time(),
                        self.authorization_progress_for_subscription(subscription),
                    )
                } else {
                    let update = self
                        .rehydrate_authorization_support_query_for_identity(
                            node,
                            writer,
                            subscription,
                            &shape,
                            &binding,
                            scope.options.clone(),
                        )
                        .await;
                    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                        settled_through, ..
                    }) = update?
                    else {
                        return Err(Error::UnsupportedSyncMessage(
                            "terminal authority support hydration did not return a view",
                        ));
                    };
                    (
                        settled_through,
                        self.authorization_progress_for_subscription(subscription),
                    )
                };
                let _ = aggregate.apply(subscription, cut, progress);
            }
            if aggregate.bounds().is_none() {
                return Err(Error::UnsupportedSyncMessage(
                    "terminal authority support proof is incomplete",
                ));
            }
            self.authority_scope_proofs = self.authority_scope_proofs.saturating_add(1);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn terminal_authority_scope_proof_count(&self) -> u64 {
        self.authority_scope_proofs
    }

    /// Locally hydrate the same action-specific support clauses that an
    /// admitted upstream authority would send in `AuthorizationScopeView`.
    /// Terminal cores do not put those views on a wire, but they must never
    /// fall back to the historical table-wide permission query.
    async fn unsettled_authority_scope_subscriptions<S>(
        &mut self,
        node: &mut NodeState<S>,
        writer: AuthorSubject,
        versions: &[VersionRecord],
        candidate_tx_id: Option<TxId>,
        retained_scope_is_unsettled: bool,
    ) -> Result<Option<Vec<SubscriptionKey>>, Error>
    where
        S: OrderedKvStorage,
    {
        let mut unsettled = Vec::new();
        for action in node
            .authorization_actions_for_versions_in_transaction(versions, candidate_tx_id)
            .await?
        {
            let scope = node.authorization_support_scope(writer, &action)?;
            if scope.subscriptions.is_empty() {
                // A policy with no support clauses is structurally complete;
                // its terminal decision is evaluated by the same authority
                // path without inventing an empty receipt.
                continue;
            }
            let mut aggregate = AuthorityScopeAggregate::new(
                scope
                    .subscriptions
                    .iter()
                    .map(|(shape, binding)| (shape.shape_id(), binding.binding_id()))
                    .collect(),
            );
            for (shape, binding) in scope.subscriptions {
                let subscription = SubscriptionKey {
                    shape_id: shape.shape_id(),
                    binding_id: binding.binding_id(),
                    read_view: scope.options.read_view_key(),
                };
                if !aggregate.register(subscription, (shape.shape_id(), binding.binding_id())) {
                    // The compiler may reach the same canonical clause through
                    // more than one policy edge.  It remains one support
                    // proof clause, never a second maintained subscription.
                    continue;
                }
                if retained_scope_is_unsettled
                    && self
                        .edge_scope_subscription_refs
                        .contains_key(&subscription)
                {
                    unsettled.push(subscription);
                    continue;
                }
                if self
                    .publication_states
                    .get(&subscription)
                    .is_some_and(|state| state.maintained_subscription_view.is_some())
                {
                    let _ = aggregate.apply(
                        subscription,
                        node.committed_global_time(),
                        self.authorization_progress_for_subscription(subscription),
                    );
                    continue;
                }
                let rehydrate = self
                    .rehydrate_authorization_support_query_for_identity(
                        node,
                        writer,
                        subscription,
                        &shape,
                        &binding,
                        scope.options.clone(),
                    )
                    .await;
                let update = rehydrate?;
                let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                    settled_through, ..
                }) = update
                else {
                    return Err(Error::UnsupportedSyncMessage(
                        "authority support hydration did not return a view",
                    ));
                };
                let _ = aggregate.apply(
                    subscription,
                    settled_through,
                    self.authorization_progress_for_subscription(subscription),
                );
                // This legacy direct-PeerState entry point is retained only
                // for compatibility tests.  Db edge admission uses the
                // authority-owned upstream receipt path; a caller already
                // parked here still waits for its next drain turn.
                unsettled.push(subscription);
            }
            if aggregate.bounds().is_none() {
                // Preserve the exact subscriptions that still need a newer
                // clause view so an existing parked caller can retain them.
                let missing = aggregate
                    .expected_support()
                    .iter()
                    .filter_map(|(shape_id, binding_id)| {
                        let subscription = SubscriptionKey {
                            shape_id: *shape_id,
                            binding_id: *binding_id,
                            read_view: scope.options.read_view_key(),
                        };
                        (!unsettled.contains(&subscription)).then_some(subscription)
                    })
                    .collect::<Vec<_>>();
                unsettled.extend(missing);
            }
        }
        if unsettled.is_empty() {
            Ok(None)
        } else {
            Ok(Some(unsettled))
        }
    }

    fn retain_edge_scope_subscription(&mut self, subscription: SubscriptionKey) {
        self.idle_edge_scope_subscriptions.remove(&subscription);
        *self
            .edge_scope_subscription_refs
            .entry(subscription)
            .or_default() += 1;
    }

    fn release_edge_scope_subscription<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        now_ms: u64,
    ) where
        S: OrderedKvStorage,
    {
        let Some(refcount) = self.edge_scope_subscription_refs.get_mut(&subscription) else {
            return;
        };
        *refcount -= 1;
        if *refcount == 0 {
            self.edge_scope_subscription_refs.remove(&subscription);
            if edge_scope_ttl_ms() == 0 {
                self.forget_subscription_with_node(node, subscription);
            } else {
                self.idle_edge_scope_subscriptions
                    .insert(subscription, now_ms);
            }
        }
    }

    fn evict_idle_edge_scope_subscriptions<S>(&mut self, node: &mut NodeState<S>, now_ms: u64)
    where
        S: OrderedKvStorage,
    {
        let ttl_ms = edge_scope_ttl_ms();
        if ttl_ms == 0 {
            let idle = std::mem::take(&mut self.idle_edge_scope_subscriptions);
            for subscription in idle.into_keys() {
                self.forget_subscription_with_node(node, subscription);
            }
            return;
        }

        let expired = self
            .idle_edge_scope_subscriptions
            .iter()
            .filter_map(|(subscription, idle_since_ms)| {
                (now_ms.saturating_sub(*idle_since_ms) >= ttl_ms).then_some(*subscription)
            })
            .collect::<Vec<_>>();
        for subscription in expired {
            self.idle_edge_scope_subscriptions.remove(&subscription);
            self.forget_subscription_with_node(node, subscription);
        }
    }

    fn record_outgoing_view_update(&mut self, update: &SyncMessage) {
        self.record_outgoing_view_update_metadata(update);
        self.apply_outgoing_view_update_result_set(update);
    }

    fn refresh_maintained_subscription_view_footprint(&mut self, subscription: SubscriptionKey) {
        self.metrics.maintained_subscription_view.footprint = self
            .publication_states
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .map(|maintained| maintained.maintained.footprint())
            .map(MaintainedSubscriptionViewMetricsFootprint::from)
            .unwrap_or_default();
    }

    fn apply_outgoing_view_update_result_set(&mut self, update: &SyncMessage) {
        let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            reset_result_set,
            result_member_adds,
            result_member_removes,
            program_fact_adds,
            program_fact_removes,
            ..
        }) = update
        else {
            return;
        };
        let state = self.publication_states.entry(*subscription).or_default();
        if *reset_result_set {
            state.result_member_set.clear();
            state.program_fact_set.clear();
            state.member_index.clear();
        }
        for member in result_member_removes {
            state.result_member_set.remove(member);
            apply_contribution_remove(state, std::iter::once(member), &mut Vec::new());
        }
        for fact in program_fact_removes {
            state.program_fact_set.remove(fact);
        }
        for member in result_member_adds {
            state.result_member_set.insert(member.clone());
            apply_contribution_add(
                state,
                std::iter::once(member),
                &mut Vec::new(),
                &mut Vec::new(),
            );
        }
        state
            .program_fact_set
            .extend(program_fact_adds.iter().cloned());
        // Diagnostic-only invariant check: detecting duplicate content versions
        // in the result set requires materializing and scanning it, which is
        // wasted work in release where the debug_assert compiles out. Gate the
        // whole scan to debug builds so it never runs on the release hot path
        // (this sat under the measured record_outgoing_view_update hotspot).
        #[cfg(debug_assertions)]
        {
            if let Some((row_key, first, second)) =
                duplicate_physical_row_result_set(&state.result_member_set)
            {
                debug_assert!(
                    first == second,
                    "peer subscription {subscription:?} has multiple content versions for physical output row {row_key:?}: {first:?} and {second:?}"
                );
            }
        }
    }
}
