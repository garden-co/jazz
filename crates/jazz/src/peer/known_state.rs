impl PeerState {
    /// Build a reset current-row view for `table`.
    pub fn reset_current_rows<S>(
        &mut self,
        node: &mut NodeState<S>,
        table: &str,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        self.rehydrate_current_rows(node, table)
    }

    /// Drops only the per-subscription result_set cache. Version payload dedup
    /// is per-peer and survives subscription rehydration.
    pub fn forget_subscription(&mut self, subscription: SubscriptionKey) {
        self.subscriptions.remove(&subscription);
    }

    /// Record a downstream known-state declaration for a usage-site subscription.
    pub fn declare_known_state(
        &mut self,
        subscription: SubscriptionKey,
        declaration: Option<KnownStateDeclaration>,
    ) {
        self.subscriptions
            .entry(subscription)
            .or_default()
            .known_state = declaration;
    }

    /// Advance retained per-binding authorization generations after this
    /// reader's authority is rebuilt.
    pub(crate) fn advance_authorization_progress(&mut self) {
        for state in self.subscriptions.values_mut() {
            state.authorization_progress = state
                .authorization_progress
                .checked_add(1)
                .expect("authorization progress overflow must stop reset suppression");
        }
    }

    pub(crate) fn authorization_progress_for_subscription(
        &self,
        subscription: SubscriptionKey,
    ) -> u64 {
        self.subscriptions
            .get(&subscription)
            .map_or(0, |state| state.authorization_progress)
    }

    /// Drop one subscription and eagerly unregister any maintained Groove
    /// subscription from the runtime before dropping the receiver.
    pub fn forget_subscription_with_node<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
    ) -> bool
    where
        S: OrderedKvStorage,
    {
        let Some(mut state) = self.subscriptions.remove(&subscription) else {
            return false;
        };
        let unsubscribed = state.groove_runtime_token == Some(node.groove_runtime_token())
            && state
                .maintained_subscription_view
                .take()
                .is_some_and(|maintained| {
                    node.unsubscribe_groove_subscription(maintained.subscription.id())
                });
        drop(state);
        unsubscribed
    }

    /// Drop one query-binding result set on this peer.
    pub fn forget_query_binding(&mut self, shape: &ValidatedQuery, binding: &Binding) {
        self.forget_subscription(SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: Default::default(),
        });
    }

    /// Drop one query-binding result set and eagerly unregister any maintained
    /// Groove subscription from the runtime before dropping the receiver.
    pub fn forget_query_binding_with_node<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> bool
    where
        S: OrderedKvStorage,
    {
        self.forget_subscription_with_node(
            node,
            SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: Default::default(),
            },
        )
    }

    /// Return transaction refs whose complete payload bundles have shipped on this peer.
    pub fn shipped_complete_tx_payloads(&self) -> &BTreeSet<TxId> {
        &self.shipped_complete_tx_payloads
    }

    fn acknowledged_complete_tx_payloads(&self) -> BTreeSet<TxId> {
        // Complete-payload inventory refs are only safe once the receiver has
        // explicitly acknowledged durable application. Until the protocol grows
        // that ack, every served update must carry the required bundles again.
        BTreeSet::new()
    }

    /// Configure whether accepted exclusive transactions should ship complete
    /// payloads so the downstream can safely author later exclusive
    /// transactions from refreshed state.
    pub fn set_ship_complete_exclusive_payloads(&mut self, enabled: bool) {
        self.ship_complete_exclusive_payloads = enabled;
    }

    /// Snapshot peer-owned pin-set roots for edge-cache eviction.
    pub fn eviction_pins(&self) -> PeerEvictionPins {
        PeerEvictionPins {
            deferred_edge_fate_txs: self.deferred_edge_fates.keys().copied().collect(),
            referenced_scope_subscriptions: self
                .edge_scope_subscription_refs
                .keys()
                .chain(self.idle_edge_scope_subscriptions.keys())
                .copied()
                .collect(),
        }
    }

    /// Forget complete-tx payload dedup markers for transactions whose local
    /// payloads were evicted, so a standard rehydrate may resend them.
    pub fn forget_evicted_versions(&mut self, tx_ids: impl IntoIterator<Item = TxId>) -> usize {
        tx_ids
            .into_iter()
            .filter(|tx_id| self.shipped_complete_tx_payloads.remove(tx_id))
            .count()
    }

    /// Serve exact row-version repair fetches for this peer.
    pub fn handle_row_versions_fetch<S>(
        &mut self,
        node: &mut NodeState<S>,
        message: SyncMessage,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: OrderedKvStorage,
    {
        let SyncMessage::FetchRowVersions { requests } = message else {
            return Err(Error::UnsupportedSyncMessage(
                "non-row-version-fetch peer request",
            ));
        };
        validate_fetch_row_versions(&requests).map_err(|_| {
            Error::UnsupportedSyncMessage("row-version repair request exceeds limit")
        })?;
        self.serve_row_versions(node, &requests)
    }

    /// Build repair-lane responses for visible requested row-version payloads.
    pub fn serve_row_versions<S>(
        &mut self,
        node: &mut NodeState<S>,
        requests: &[RowVersionRef],
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: OrderedKvStorage,
    {
        let versions = node.row_version_payloads_for_refs(requests, self.identity())?;
        Ok(vec![SyncMessage::RowVersionPayloads {
            version_bundles: versions,
        }])
    }

    /// Return current result_set for one subscription.
    pub fn subscription_result_sets(
        &self,
        subscription: SubscriptionKey,
    ) -> Option<BTreeSet<TxId>> {
        self.subscriptions
            .get(&subscription)
            .map(PeerSubscriptionState::previous_tx_ids)
    }

    /// Return this peer's maintained subscription view counters and latest footprint.
    pub fn maintained_subscription_view_metrics(&self) -> MaintainedSubscriptionViewMetrics {
        *self.metrics.maintained_subscription_view
    }

}
