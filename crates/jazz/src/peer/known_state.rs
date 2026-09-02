impl PeerState {
    /// Build a reset current-row view for `table`.
    pub async fn reset_current_rows<S>(
        &mut self,
        node: &mut NodeState<S>,
        table: &str,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        self.rehydrate_current_rows(node, table).await
    }

    /// Drops only the per-subscription result_set cache. Version payload dedup
    /// is per-peer and survives subscription rehydration.
    pub fn forget_subscription(&mut self, subscription: SubscriptionKey) {
        self.publication_states.remove(&subscription);
        self.downstream_known_states.remove(&subscription);
    }

    /// Record a downstream known-state declaration for a usage-site subscription.
    pub fn declare_known_state(
        &mut self,
        subscription: SubscriptionKey,
        declaration: Option<KnownStateDeclaration>,
    ) {
        if let Some(declaration) = declaration {
            self.downstream_known_states
                .insert(subscription, declaration);
        } else {
            self.downstream_known_states.remove(&subscription);
        }
    }

    /// Advance retained per-binding authorization generations after this
    /// reader's authority is rebuilt.
    pub(crate) fn advance_authorization_progress(&mut self) {
        for state in self.publication_states.values_mut() {
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
        self.publication_states
            .get(&subscription)
            .map_or(0, |state| state.authorization_progress)
    }

    /// Carry an already-served authorization generation to a replacement
    /// maintained usage. Claim rebinding changes the coverage-group key, but
    /// does not reset the concrete downstream usages' generation: a fresh
    /// reset from the replacement must remain comparable with their retained
    /// fast cursors.
    pub(crate) fn retain_authorization_progress_for_subscription(
        &mut self,
        subscription: SubscriptionKey,
        authorization_progress: u64,
    ) {
        let state = self.publication_states.entry(subscription).or_default();
        state.authorization_progress = authorization_progress;
        state.has_served_authorization_progress = true;
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
        let Some(mut state) = self.publication_states.remove(&subscription) else {
            self.downstream_known_states.remove(&subscription);
            // `ensure_query_subscription_registered` may have installed the
            // node-side shape before this peer had enough state to create a
            // maintained receiver. Retire that owner too; the node-side
            // removal is idempotent for an already-forgotten publication.
            node.release_query_subscription_for_peer(self.publication_owner, subscription);
            return false;
        };
        let admitted_policy_binding = state.policy_binding.as_ref().map(|(identity, claims)| {
            crate::protocol::PolicyBindingKey::from_canonical_parts(*identity, claims.clone())
        });
        self.downstream_known_states.remove(&subscription);
        let unsubscribed = if state.groove_runtime_token == Some(node.groove_runtime_token()) {
            state
                .maintained_subscription_view
                .take()
                .is_some_and(|maintained| {
                    node.unsubscribe_groove_subscription(maintained.subscription.id())
                })
        } else {
            false
        };
        drop(state);
        if let Some(policy_binding) = admitted_policy_binding {
            node.release_query_subscription_for_peer_with_admitted_policy_binding(
                self.publication_owner,
                subscription,
                policy_binding,
            );
        } else {
            node.release_query_subscription_for_peer(self.publication_owner, subscription);
        }
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

    /// Configure a trusted writer relay to receive complete accepted exclusive
    /// transaction payloads, so it can safely author later exclusive
    /// transactions from refreshed state. Ordinary readers intentionally keep
    /// view-scoped delivery; publication also ignores this preference for
    /// identity-scoped client links.
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
    pub async fn handle_row_versions_fetch<S>(
        &mut self,
        node: &mut NodeState<S>,
        message: SyncMessage,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: OrderedKvStorage,
    {
        let SyncMessage::FetchRowVersions { requests, .. } = message else {
            return Err(Error::UnsupportedSyncMessage(
                "non-row-version-fetch peer request",
            ));
        };
        validate_fetch_row_versions(&requests).map_err(|_| {
            Error::UnsupportedSyncMessage("row-version repair request exceeds limit")
        })?;
        if self.role() == PeerRole::Relay {
            return Err(Error::InvalidStoredValue(
                "relay row-version repair requires an explicit immutable policy binding",
            ));
        }
        let identity = self.permission_subject().ok_or(Error::InvalidStoredValue(
            "direct repair is missing a terminated permission subject",
        ))?;
        let claims = node.session_claims_for(identity);
        self.serve_row_versions(
            node,
            &requests,
            RepairServingContext::Authority {
                policy_binding: (identity, claims),
            },
        )
        .await
    }

    /// Build repair-lane responses for visible requested row-version payloads.
    pub(crate) async fn serve_row_versions<S>(
        &mut self,
        node: &mut NodeState<S>,
        requests: &[RowVersionRef],
        context: RepairServingContext,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: OrderedKvStorage,
    {
        let versions = match context {
            RepairServingContext::Authority {
                policy_binding: (identity, claims),
            } => node
                .scoped_active_session_claims(identity, claims)
                .row_version_payloads_for_refs(
                    requests,
                    crate::node::RowVersionRepairAuthorization::EnforceReadPolicy(identity),
                )
                .await?,
            RepairServingContext::ScopeIsolatedClientRelay => node
                .row_version_payloads_for_refs(
                    requests,
                    crate::node::RowVersionRepairAuthorization::RetainedScopeLedger,
                )
                .await?,
        };
        Ok(vec![SyncMessage::RowVersionPayloads {
            version_bundles: versions,
        }])
    }

    /// Return current result_set for one subscription.
    pub fn subscription_result_sets(
        &self,
        subscription: SubscriptionKey,
    ) -> Option<BTreeSet<TxId>> {
        self.publication_states
            .get(&subscription)
            .map(PeerSubscriptionState::previous_tx_ids)
    }

    /// Return this peer's maintained subscription view counters and latest footprint.
    pub fn maintained_subscription_view_metrics(&self) -> MaintainedSubscriptionViewMetrics {
        *self.metrics.maintained_subscription_view
    }

}

/// Chosen by the topology boundary, rather than inferred from `PeerRole`.
/// Multiplexed relays receive no retained-knowledge capability and must ask an
/// authority to serve repairs.
pub(crate) enum RepairServingContext {
    Authority {
        policy_binding: (AuthorSubject, BTreeMap<String, groove::records::Value>),
    },
    ScopeIsolatedClientRelay,
}
