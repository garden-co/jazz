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

/// Reconcile a retained downstream result set against a cold maintained-view
/// snapshot that contains a static deletion witness. The active membership is
/// authoritative for removals. Publishability only gates members the
/// downstream has not already received, because Stream A can arrive before
/// Stream B's content witness during cold hydration.
fn reconcile_retained_members_after_initial_deletion_witness(
    states: &mut BTreeMap<ResultMemberEntry, (bool, bool)>,
    previous_members: &BTreeSet<ResultMemberEntry>,
    active_members: &BTreeSet<ResultMemberEntry>,
    published_members: &BTreeSet<ResultMemberEntry>,
) {
    for member in previous_members.union(active_members) {
        let was_published = previous_members.contains(member);
        let is_published = active_members.contains(member)
            && (was_published || published_members.contains(member));
        states.insert(member.clone(), (was_published, is_published));
    }
}

/// Canonical reconciliation retained by the coverage-group owner while it
/// publishes established siblings before attempting a fallible clone reset.
pub(crate) struct ReconciledMaintainedSubscriptionClone {
    pub(crate) canonical_update: Option<SyncMessage>,
    source_removes: Vec<ResultMemberEntry>,
    source_had_program_fact_transitions: bool,
    allow_storage_witness_fallback: bool,
}

struct MaintainedCanonicalUpdate {
    update: SyncMessage,
    allow_storage_witness_fallback: bool,
}

#[cfg(test)]
std::thread_local! {
    static FAIL_NEXT_CLONED_SUBSCRIPTION_RESET: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(crate) fn fail_next_cloned_subscription_reset_for_test() {
    FAIL_NEXT_CLONED_SUBSCRIPTION_RESET.with(|fail| fail.set(true));
}

impl PeerState {
    pub(crate) fn has_maintained_subscription(&self, subscription: SubscriptionKey) -> bool {
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

    /// Read the settlement watermark from the exact receipt retained by the
    /// served subscription. Never route this through a `BindingViewKey`:
    /// two policy scopes may share that key while having different watermarks.
    ///
    /// A missing exact source is only valid for a genuinely direct,
    /// unscoped publication. Its local receipt may use the current committed
    /// time when it has not separately settled. A policy-scoped source is
    /// different: absence of that *particular* receipt must remain the zero
    /// watermark rather than overclaiming an unrelated committed time.
    fn settlement_time_for_publication<S>(
        &self,
        node: &NodeState<S>,
        subscription: SubscriptionKey,
        binding_view: crate::protocol::BindingViewKey,
    ) -> GlobalTime
    where
        S: OrderedKvStorage,
    {
        if let Some(authority_result_key) = self
            .publication_states
            .get(&subscription)
            .and_then(|state| state.authority_result_source.as_ref())
        {
            return node
                .settled_through_for_authority_result(authority_result_key)
                .unwrap_or_default();
        }
        node.settled_through_for_authority_result(&AuthorityResultKey::unscoped(binding_view))
            .unwrap_or_else(|| node.committed_global_time())
    }

    fn binding_settlement_time<S>(
        &self,
        node: &NodeState<S>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> GlobalTime
    where
        S: OrderedKvStorage,
    {
        self.settlement_time_for_publication(
            node,
            subscription,
            crate::protocol::BindingViewKey::new(
                shape.shape_id(),
                binding.binding_id(),
                subscription.read_view,
            ),
        )
    }

    pub(crate) fn canonical_subscription_settlement_time<S>(
        &self,
        node: &NodeState<S>,
        subscription: SubscriptionKey,
    ) -> GlobalTime
    where
        S: OrderedKvStorage,
    {
        self.settlement_time_for_publication(
            node,
            subscription,
            crate::protocol::BindingViewKey::from_canonical_subscription_key(subscription),
        )
    }

    pub(crate) fn needs_catalogue_snapshot(&self, fingerprint: [u8; 32]) -> bool {
        self.announced_catalogue_fingerprint != Some(fingerprint)
    }

    pub(crate) fn mark_catalogue_snapshot_announced(&mut self, fingerprint: [u8; 32]) {
        self.announced_catalogue_fingerprint = Some(fingerprint);
    }

    /// Construct a standalone SYSTEM-scoped peer.
    ///
    /// This is suitable for direct/no-waker helpers. Network relays must use
    /// [`Self::relay`] so a missing admitted policy binding fails closed.
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a permanent relay peer.
    pub fn relay() -> Self {
        Self {
            role: PeerRole::Relay,
            transport_capability: RelayTransportCapability::MultiplexedRelay,
            ..Self::default()
        }
    }

    /// Construct a subjectless scope-isolated relay with its one
    /// handshake-admitted foreground session. This is topology-private:
    /// callers on the wire never select this value.
    #[allow(dead_code)] // constructed only by the private serving admission path
    pub(crate) fn scope_isolated_relay(
        identity: AuthorSubject,
        claims: BTreeMap<String, groove::records::Value>,
        admission_epoch: u64,
    ) -> Self {
        Self {
            role: PeerRole::Relay,
            transport_capability: RelayTransportCapability::ScopeIsolatedClientRelay {
                binding: DelegatedSessionBinding { identity, claims },
                admission_epoch,
            },
            ..Self::default()
        }
    }

    pub(crate) fn admits_relay_binding(
        &self,
        binding: &(AuthorSubject, BTreeMap<String, groove::records::Value>),
    ) -> bool {
        matches!(
            &self.transport_capability,
            RelayTransportCapability::ScopeIsolatedClientRelay { binding: admitted, .. }
                if admitted.identity == binding.0 && admitted.claims == binding.1
        )
    }

    /// The one immutable user binding selected by server-side scope-relay
    /// admission. This stays on the transport capability rather than in the
    /// connection's mutable session-claims slot: raw `SessionClaims` frames
    /// and host-side refresh helpers must not replace it mid-connection.
    pub(crate) fn admitted_scope_relay_binding(
        &self,
    ) -> Option<&DelegatedSessionBinding> {
        match &self.transport_capability {
            RelayTransportCapability::ScopeIsolatedClientRelay { binding, .. } => Some(binding),
            RelayTransportCapability::OrdinarySession
            | RelayTransportCapability::MultiplexedRelay => None,
        }
    }

    /// Replace the per-attachment capability epoch after the server has
    /// detached and resumed this scope-isolated relay. The authenticated
    /// binding stays immutable, but a resumed transport must never retain the
    /// capability issued to its previous physical attachment.
    ///
    /// This intentionally offers no caller-provided epoch: only the server's
    /// resume path can advance a capability it already admitted.
    #[cfg(feature = "runtime")]
    pub(crate) fn refresh_scope_relay_admission_epoch(&mut self) -> bool {
        let RelayTransportCapability::ScopeIsolatedClientRelay {
            admission_epoch, ..
        } = &mut self.transport_capability
        else {
            return false;
        };
        *admission_epoch = admission_epoch
            .checked_add(1)
            .expect("scope-isolated relay admission epoch exhausted");
        true
    }

    #[cfg(test)]
    pub(crate) fn scope_relay_admission_epoch_for_test(&self) -> Option<u64> {
        match self.transport_capability {
            RelayTransportCapability::ScopeIsolatedClientRelay {
                admission_epoch, ..
            } => Some(admission_epoch),
            RelayTransportCapability::OrdinarySession
            | RelayTransportCapability::MultiplexedRelay => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn scope_relay_binding_for_test(
        &self,
    ) -> Option<(AuthorSubject, BTreeMap<String, groove::records::Value>)> {
        self.admitted_scope_relay_binding()
            .map(|binding| (binding.identity, binding.claims.clone()))
    }

    pub(crate) fn rejects_raw_session_claims(&self) -> bool {
        matches!(
            self.transport_capability,
            RelayTransportCapability::ScopeIsolatedClientRelay { .. }
        )
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

    /// Return the principal terminated by this peer link, if it terminates
    /// one. A relay is an explicit transport capability, never a synthetic
    /// SYSTEM session.
    pub fn link_identity(&self) -> Option<AuthorSubject> {
        self.role.permission_subject()
    }

    /// Compatibility accessor for direct client links. Relay callers must use
    /// an admitted per-request binding rather than treating transport as a
    /// principal.
    pub fn identity(&self) -> AuthorSubject {
        self.link_identity()
            .expect("relay transport has no identity; use an admitted policy binding")
    }

    /// Return the principal that may be used for policy composition.
    ///
    /// A trusted serving client link may deliberately carry an explicit
    /// internal principal such as SYSTEM. Relay transport itself never has a
    /// permission subject, so callers must bind an admitted session per usage
    /// site instead of falling back to a synthetic identity.
    pub fn permission_subject(&self) -> Option<AuthorSubject> {
        self.permission_identity.or(self.role.permission_subject())
    }

    /// Bind the authorization snapshot selected at subscriber admission to one
    /// usage site. This is intentionally stored with the subscription because
    /// a trusted relay may multiplex distinct sessions on one transport.
    pub fn set_subscription_policy_binding(
        &mut self,
        subscription: SubscriptionKey,
        binding: (AuthorSubject, BTreeMap<String, groove::records::Value>),
    ) {
        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
            eprintln!(
                "JAZZ_COVERED_INPUT_TRACE stage=served_policy_binding peer={:p} owner={} role={:?} subscription={subscription:?} identity={:?} claims={:?}",
                self, self.publication_owner, self.role, binding.0, binding.1,
            );
        }
        self.publication_states
            .entry(subscription)
            .or_default()
            .policy_binding = Some(binding);
    }

    /// Associate a relay-owned maintained receiver with the precise upstream
    /// authority receipt that supplies its membership. This is intentionally
    /// separate from its policy binding: the former is a local lifecycle
    /// handle, while the latter is the admitted authorization context.
    pub(crate) fn set_subscription_authority_result_source(
        &mut self,
        subscription: SubscriptionKey,
        authority_result_key: AuthorityResultKey,
    ) {
        let state = self.publication_states.entry(subscription).or_default();
        state
            .local_authority
            .replace_source(authority_result_key.clone(), 0);
        state.authority_result_source = Some(authority_result_key);
    }

    /// Mark a scope-relay served usage as pending the exact U source installed
    /// above. This is set only by non-authoritative admission and survives
    /// maintained-view replacement; definitive publication clears it.
    pub(crate) fn set_subscription_awaiting_selected_authority_source(
        &mut self,
        subscription: SubscriptionKey,
        awaiting: bool,
    ) {
        self.publication_states
            .entry(subscription)
            .or_default()
            .awaiting_selected_authority_source = awaiting;
    }

    /// The exact authority receipt selected for this concrete downstream
    /// usage. This is lifecycle metadata, not a permission lookup: callers
    /// use it only to preserve an opening result until that source settles.
    pub(crate) fn subscription_authority_result_source(
        &self,
        subscription: SubscriptionKey,
    ) -> Option<&AuthorityResultKey> {
        self.publication_states
            .get(&subscription)
            .and_then(|state| state.authority_result_source.as_ref())
    }

    pub(crate) fn subscription_policy_binding(
        &self,
        subscription: SubscriptionKey,
    ) -> Option<(AuthorSubject, BTreeMap<String, groove::records::Value>)> {
        self.publication_states
            .get(&subscription)
            .and_then(|state| state.policy_binding.clone())
    }

    /// Return the immutable policy snapshot for a served subscription.
    ///
    /// Network admission records this before any owner-loop rehydrate can run.
    /// A relay must never substitute its SYSTEM transport identity when a
    /// multiplexed subscriber's binding was lost.
    #[track_caller]
    fn served_subscription_policy_binding(
        &self,
        subscription: SubscriptionKey,
    ) -> Result<(AuthorSubject, BTreeMap<String, groove::records::Value>), Error> {
        self.subscription_policy_binding(subscription).ok_or_else(|| {
            if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                eprintln!(
                    "JAZZ_COVERED_INPUT_TRACE stage=missing_served_policy_binding peer={:p} owner={} role={:?} subscription={subscription:?} states={:?} caller={}",
                    self,
                    self.publication_owner,
                    self.role,
                    self.publication_states.keys().collect::<Vec<_>>(),
                    std::panic::Location::caller(),
                );
            }
            Error::InvalidStoredValue("served subscription is missing its immutable policy binding")
        })
    }

    /// Standalone no-waker helpers serve one peer identity directly. Their
    /// owner is not a multiplexing transport, so this explicit identity
    /// fallback is sound. Owner-loop paths bypass this helper and fail closed.
    fn ensure_direct_internal_subscription_policy_binding<S>(
        &mut self,
        node: &NodeState<S>,
        subscription: SubscriptionKey,
    ) -> Result<(), Error>
    where
        S: OrderedKvStorage,
    {
        // A caller that has already supplied a usage-site snapshot may use
        // the shared rehydrate helpers for either direct or relay serving.
        // Only the fallback below is restricted to a direct, single-session
        // peer.
        if self.subscription_policy_binding(subscription).is_some() {
            return Ok(());
        }
        if self.role == PeerRole::Relay {
            return Err(Error::InvalidStoredValue(
                "relay subscription requires an explicit immutable policy binding",
            ));
        }
        // A direct peer terminates exactly one session, so it may take a
        // one-time immutable snapshot from this node's admitted session
        // state.  The snapshot is intentionally installed only when the
        // usage site is first opened: later claim changes are handled by
        // the owner loop rebuilding its explicitly bound views.
        let identity = self.permission_subject().ok_or(Error::InvalidStoredValue(
            "direct subscription is missing a terminated permission subject",
        ))?;
        self.set_subscription_policy_binding(
            subscription,
            (identity, node.session_claims_for(identity)),
        );
        Ok(())
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

    fn replace_maintained_subscription_view<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        replacement: MaintainedSubscriptionViewSubscription,
    ) where
        S: OrderedKvStorage,
    {
        let runtime_token = node.groove_runtime_token();
        let stale = {
            let state = self.publication_states.entry(subscription).or_default();
            let previous_runtime_token = state.groove_runtime_token;
            let stale = state.maintained_subscription_view.replace(replacement);
            state.groove_runtime_token = Some(runtime_token);
            (previous_runtime_token == Some(runtime_token))
                .then_some(stale)
                .flatten()
        };
        if let Some(stale) = stale {
            node.unsubscribe_groove_subscription(stale.subscription.id());
        }
        self.refresh_maintained_subscription_view_footprint(subscription);
    }

    /// A strict relay query is owned by one exact upstream authority receipt.
    /// Do not let a cold receiver opened before that receipt was live survive
    /// the handoff: it has already resolved the empty pre-receipt source and
    /// can never observe the source becoming populated.  Retiring it lets the
    /// normal rehydrate path open the *same* receiver against the now-live
    /// source; it does not create a second result or relax exact binding.
    fn retire_cold_relay_authority_receiver<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        source: &AuthorityResultKey,
    ) where
        S: OrderedKvStorage,
    {
        if !node.has_settled_authority_result(source) {
            return;
        }
        let stale = self
            .publication_states
            .get_mut(&subscription)
            .and_then(|state| {
                state
                    .maintained_subscription_view
                    .as_ref()
                    .is_some_and(|maintained| {
                        !maintained.initial_received
                            && maintained.source_authority_result.as_ref() == Some(source)
                    })
                    .then(|| state.maintained_subscription_view.take())
                    .flatten()
            });
        if let Some(stale) = stale {
            node.unsubscribe_groove_subscription(stale.subscription.id());
            self.refresh_maintained_subscription_view_footprint(subscription);
        }
    }

    fn requires_selected_authority_source(
        &self,
        subscription: SubscriptionKey,
        purpose: RehydratePurpose,
    ) -> bool {
        purpose == RehydratePurpose::Query
            && self
                .publication_states
                .get(&subscription)
                .is_some_and(|state| state.awaiting_selected_authority_source)
    }

    fn selected_authority_source(
        &self,
        subscription: SubscriptionKey,
    ) -> Option<AuthorityResultKey> {
        self.publication_states
            .get(&subscription)
            .and_then(|state| state.authority_result_source.clone())
    }

    fn ensure_query_subscription_registered<S>(
        &self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
        policy_binding: &(AuthorSubject, BTreeMap<String, groove::records::Value>),
    ) -> Result<(), Error>
    where
        S: OrderedKvStorage,
    {
        node.register_query_subscription_for_peer(
            self.publication_owner,
            shape.shape_id(),
            ShapeAst::from_validated(shape),
            Subscribe {
                shape_id: shape.shape_id(),
                subscription,
                values: binding_values_in_param_order(shape, binding),
                known_state: None,
                delegated_session: None,
            },
            crate::protocol::PolicyBindingKey::from_canonical_parts(
                policy_binding.0,
                policy_binding.1.clone(),
            ),
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
        // `current_rows_update` is the direct-peer counterpart to
        // `rehydrate_query_with_opts`: it opens a served maintained view, so
        // it needs the same explicit direct-peer binding before that view can
        // evaluate policy.  In particular, do not make the maintained-view
        // code infer a fallback identity: owner-loop and relay callers must
        // still have installed the admitted subscriber snapshot themselves.
        self.ensure_direct_internal_subscription_policy_binding(node, subscription)?;
        self.clear_stale_groove_runtime_handles(node, subscription);
        let policy_binding = self.served_subscription_policy_binding(subscription)?;
        self.ensure_query_subscription_registered(
            node,
            subscription,
            &shape,
            &binding,
            &policy_binding,
        )?;
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
        if self.requires_selected_authority_source(subscription, RehydratePurpose::Query)
            && let Some(source) = self.selected_authority_source(subscription)
        {
            self.retire_cold_relay_authority_receiver(node, subscription, &source);
        }
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
                    // The first receiver closure is a complete successor,
                    // not a result-member delta.  Its reset bit claims the
                    // exact ProgramSourceCoverage manifest so a client can
                    // atomically install and settle its local source graph.
                    reset_result_set: true,
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
        let policy_binding = self.served_subscription_policy_binding(subscription)?;
        self.ensure_query_subscription_registered(
            node,
            subscription,
            shape,
            binding,
            &policy_binding,
        )?;
        let Some(_) = self.publication_states.get(&subscription) else {
            return Ok(Some(SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                subscription,
                settled_through: self.binding_settlement_time(node, subscription, shape, binding),
                reset_result_set: false,
                version_carriers: Vec::new(),
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            })));
        };
        if self.requires_selected_authority_source(subscription, RehydratePurpose::Query)
            && let Some(source) = self.selected_authority_source(subscription)
        {
            self.retire_cold_relay_authority_receiver(node, subscription, &source);
        }
        if self
            .publication_states
            .get(&subscription)
            .is_some_and(|state| state.maintained_subscription_view.is_some())
        {
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
        self.query_update_maintained_subscription_view_with_metadata(
            node,
            shape,
            binding,
            subscription,
            result_table_filter,
            progress_waker,
        )
        .await
        .map(|update| update.map(|update| update.update))
    }

    async fn query_update_maintained_subscription_view_with_metadata<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        result_table_filter: Option<&str>,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<Option<MaintainedCanonicalUpdate>, Error>
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
        let initial_state = self
            .publication_states
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref());
        if !initial_state.is_some_and(|maintained| maintained.initial_received) {
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
            terminal_operations: _,
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
        let public_result_is_silent = result_member_adds.is_empty() && result_member_removes.is_empty();
        // Deletion witnesses require a one-shot reconciliation only when the
        // result terminal itself was silent. When Groove already emitted the
        // complete public delta, reopening the maintained view would discard
        // that delta and repeatedly rediscover the same deletion witness.
        if public_result_is_silent
            && (requires_authoritative_membership_reconcile
                || (observed_result_delta_batches > 0
                    && program_fact_adds.is_empty()
                    && program_fact_removes.is_empty()))
        {
            let (tier, read_view) = self
                .publication_states
                .get(&subscription)
                .and_then(|state| state.prepared_query.as_ref())
                .map(CachedPeerQueryPlan::context)
                .ok_or(Error::InvalidStoredValue(
                    "maintained subscription view is missing prepared state",
                ))?;
            return self
                .rehydrate_query_maintained_subscription_view(
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
                .await
                .map(|update| {
                    update.map(|update| MaintainedCanonicalUpdate {
                        update,
                        allow_storage_witness_fallback: false,
                    })
                });
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
            &program_fact_adds,
            &program_fact_removes,
        ) {
            return Ok(Some(MaintainedCanonicalUpdate {
                update: SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                    subscription,
                    settled_through: self.binding_settlement_time(node, subscription, shape, binding),
                    reset_result_set: false,
                    version_carriers: Vec::new(),
                    peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                    result_member_adds: Vec::new(),
                    result_member_removes: Vec::new(),
                    program_fact_adds: Vec::new(),
                    program_fact_removes: Vec::new(),
                }),
                allow_storage_witness_fallback: false,
            }));
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
        let bundle_start = Instant::now();
        if trace_rehydrate {
            node.reset_storage_read_metrics();
        }
        let (policy_identity, policy_claims) = self.served_subscription_policy_binding(subscription)?;
        let update = {
            let mut scoped = node.scoped_active_session_claims(policy_identity, policy_claims);
            let maintained = &self
                .publication_states
                .get(&subscription)
                .and_then(|state| state.maintained_subscription_view.as_ref())
                .ok_or(Error::InvalidStoredValue(
                    "maintained subscription view subscription missing",
                ))?
                .maintained;
            scoped.view_update_for_maintained_result_members(
                crate::node::MaintainedViewBundleInputs {
                    subscription,
                    peer_complete_tx_payloads,
                    known_state,
                    complete_exclusive_payloads: self.ship_complete_exclusive_payloads
                        && self.role == PeerRole::Relay,
                    previous_result_set: previous_result_tx_ids,
                    result_member_adds,
                    result_member_removes,
                    program_fact_adds,
                    program_fact_removes,
                    identity: policy_identity,
                    tier,
                    maintained_facts: maintained,
                    allow_storage_witness_fallback,
                },
            ).await
        };
        let update = update?;
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
                "JAZZ_REHYDRATE_TRACE stage=update table={} subscription={subscription:?} drain_ms={} bundle_ms={} adds={} removes={} fact_adds={} fact_removes={} bundles={} fallback={} drain_reads={} drain_ranges={} bundle_reads={} bundle_ranges={}",
                shape.query().table,
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
        Ok(Some(MaintainedCanonicalUpdate {
            update,
            allow_storage_witness_fallback,
        }))
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
        // Relay Edge children own a receiver-local graph. Before draining its
        // terminal, atomically replace every compiled input source from the
        // exact selected authority closure. Do not let the generic
        // trusted-serving drain observe an authority output or a stale source
        // frontier.
        let receiver_install = self
            .publication_states
            .get_mut(&subscription)
            .and_then(|state| {
                let maintained = state.maintained_subscription_view.as_mut()?;
                let source = maintained.source_authority_result.clone()?;
                let receiver = maintained.covered_input_receiver.take()?;
                Some((source, receiver, maintained.result_schema_version))
            });
        if let Some((source, mut receiver, schema_version)) = receiver_install {
            let due = node.covered_input_receiver_reconciliation_due(&receiver, &source);
            let replacement = if due {
                node.replace_covered_input_receiver(&mut receiver, schema_version, &source)
                    .await
            } else {
                Ok(true)
            };
            self.publication_states
                .get_mut(&subscription)
                .expect("maintained receiver state survives replacement")
                .maintained_subscription_view
                .as_mut()
                .expect("maintained receiver state survives replacement")
                .covered_input_receiver = Some(receiver);
            if !replacement? {
                // Pending is not an empty strict result. Leave the receiver
                // attached and wait for the exact claimed source closure.
                return Ok(ResultTransitions::default());
            }
        }
        node.drive_ready_query_runtime_with_waker(progress_waker)
            .await?;
        let previous_member_result_set = self
            .publication_states
            .get(&subscription)
            .map(PeerSubscriptionState::member_result_set)
            .unwrap_or_default();
        let output_tables = self
            .publication_states
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .map(|maintained| maintained.tables.clone())
            .unwrap_or_default();
        let maintained_source_authority_result = self
            .publication_states
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .and_then(|maintained| maintained.source_authority_result.clone());
        // A Local+Full relay read opens immediately against worker-local
        // knowledge, so its maintained receiver deliberately has no selected
        // authority source at opening. Once the independently propagated
        // exact source is live, reconcile against it without changing the
        // initial Local latency contract.
        let source_authority_result = maintained_source_authority_result.clone().or_else(|| {
            self.selected_authority_source(subscription)
                .filter(|source| node.has_settled_authority_result(source))
        });
        let aggregate_is_policy_scoped = shape.query().aggregate.is_some()
            && node
                .table(shape.query().table.as_str())?
                .read_policy
                .is_some();
        let mut states = BTreeMap::<ResultMemberEntry, (bool, bool)>::new();
        let mut program_fact_adds = Vec::new();
        let mut program_fact_removes = Vec::new();
        let allow_storage_witness_fallback = false;
        let mut observed_result_delta_batches = 0_usize;
        let mut requires_authoritative_membership_reconcile = false;
        let mut initial_deletion_witness = false;
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
                        // A completed cold hydration delivers its complete
                        // snapshot through this same receiver. A static
                        // deletion witness in it is not an incremental
                        // membership transition and must not recursively
                        // reopen this subscription. It can, however, prove
                        // that a retained downstream member is now absent;
                        // below we diff this hydrated view's complete exposed
                        // membership against that retained state.
                        let initial_snapshot = !maintained_subscription_view.initial_received;
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
                        if initial_snapshot {
                            initial_deletion_witness |=
                                transitions.requires_authoritative_membership_reconcile;
                        } else {
                            observed_result_delta_batches +=
                                transitions.observed_result_delta_batches;
                            requires_authoritative_membership_reconcile |=
                                transitions.requires_authoritative_membership_reconcile;
                        }
                        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some()
                            && (!transitions.adds.is_empty()
                                || !transitions.program_fact_adds.is_empty())
                        {
                            eprintln!(
                                "JAZZ_COVERED_INPUT_TRACE stage=publication_filter subscription={subscription:?} table={} result_filter={result_table_filter:?} authority={source_authority_result:?} members={:?} raw_facts={:?}",
                                shape.query().table,
                                transitions.adds,
                                transitions.program_fact_adds,
                            );
                        }
                        // Groove terminals belong to the local host binding ABI.
                        // Peer sync carries the maintained program's covered inputs,
                        // never this authority-owned output.
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
        if initial_deletion_witness {
            let (hydrated_active_members, hydrated_published_members) = self
                .publication_states
                .get(&subscription)
                .and_then(|state| state.maintained_subscription_view.as_ref())
                .map(|view| {
                    (
                        view.maintained
                            .active_result_members()
                            .into_iter()
                            .collect::<BTreeSet<_>>(),
                        view.maintained.published_result_members().clone(),
                    )
                })
                .ok_or(Error::InvalidStoredValue(
                    "initial maintained subscription snapshot missing after receive",
                ))?;
            reconcile_retained_members_after_initial_deletion_witness(
                &mut states,
                &previous_member_result_set,
                &hydrated_active_members,
                &hydrated_published_members,
            );
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
            terminal_operations: Vec::new(),
            allow_storage_witness_fallback,
            observed_result_delta_batches,
            requires_authoritative_membership_reconcile,
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
        let relay_edge_requires_authority_source =
            self.requires_selected_authority_source(subscription, purpose);
        // The downstream usage registration chose this policy scope.  Carry
        // that exact receipt into source resolution; the shared binding-view
        // key alone is not an authority identity in a multiplexed relay.
        let source_authority_result_key = if relay_edge_requires_authority_source {
            // The downstream opening can be serviced before the relay's
            // upstream Subscribe has been registered locally. That is normal
            // owner-loop ordering, not an invalid subscription. Suspend this
            // opening until the connection records its exact upstream usage
            // source; guessing from the group key would leak or erase a
            // sibling policy's membership.
            let Some(source) = self.selected_authority_source(subscription) else {
                return Ok(None);
            };
            // This group is semantically owned by the selected upstream
            // receipt.  Do not open a cold maintained receiver against an
            // empty pre-settlement source: it cannot observe the later
            // membership handoff and would turn a strict read into a
            // provisional empty result.  The next dirty tick after this
            // exact source becomes live re-enters this same rehydrate path.
            if !node.has_settled_authority_result(&source) {
                return Ok(None);
            }
            Some(source)
        } else {
            None
        };
        let (policy_identity, policy_claims) = self.served_subscription_policy_binding(subscription)?;
        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
            eprintln!(
                "JAZZ_COVERED_INPUT_TRACE stage=rehydrate peer={:p} owner={} subscription={subscription:?} identity={policy_identity:?} source={source_authority_result_key:?} purpose={purpose:?}",
                self,
                self.publication_owner,
            );
        }
        let opened = {
            let mut scoped = node.scoped_active_session_claims(policy_identity, policy_claims);
            match purpose {
            // A relay's selected Edge child is the browser half of a durable
            // worker authority receipt. Every strict Edge child consumes the
            // same authority-selected membership, including unbounded
            // filtered queries whose supporting rows are absent locally.
            RehydratePurpose::Query if relay_edge_requires_authority_source => {
                scoped.open_seeded_relay_edge_subscription_view_with_waker(
                    shape,
                    binding,
                    policy_identity,
                    read_view,
                    source_authority_result_key
                        .clone()
                        .expect("strict relay source resolved above"),
                    progress_waker,
                )
                    .await
                    .map(|(receiver, maintained, schemas, transitions, tables, received, inputs)| {
                        (
                            receiver,
                            maintained,
                            schemas,
                            transitions,
                            tables,
                            received,
                            Some(inputs),
                        )
                    })
            }
            RehydratePurpose::Query => {
                scoped.open_seeded_maintained_subscription_view_with_waker(
                    shape,
                    binding,
                    policy_identity,
                    tier,
                    read_view,
                    progress_waker,
                )
                .await
                .map(|(receiver, maintained, schemas, transitions, tables, received)| {
                    (
                        receiver,
                        maintained,
                        schemas,
                        transitions,
                        tables,
                        received,
                        None,
                    )
                })
            }
            RehydratePurpose::AuthorizationSupport => scoped
                .open_seeded_authorization_support_subscription_view_with_waker(
                    shape,
                    binding,
                    policy_identity,
                    tier,
                    read_view,
                    progress_waker,
                )
                .await
                .map(|(receiver, maintained, schemas, transitions, tables, received)| {
                    (
                        receiver,
                        maintained,
                        schemas,
                        transitions,
                        tables,
                        received,
                        None,
                    )
                }),
            }
        };
        let (
            receiver,
            mut maintained,
            terminal_schemas,
            mut transitions,
            tables,
            mut initial_received,
            mut covered_input_receiver,
        ) =
            match opened {
            Ok(opened) => opened,
            Err(Error::AuthorizationSupportMissingClaim(_))
                if purpose == RehydratePurpose::AuthorizationSupport =>
            {
                let update = SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                    subscription,
                    settled_through: self.binding_settlement_time(node, subscription, shape, binding),
                    reset_result_set,
                    version_carriers: Vec::new(),
                    peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                    result_member_adds: Vec::new(),
                    result_member_removes: previous_member_result_set.iter().cloned().collect(),
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
        if let (Some(authority_key), Some(receiver_inputs)) = (
            source_authority_result_key.as_ref(),
            covered_input_receiver.as_mut(),
        ) {
            let installed = node
                .replace_covered_input_receiver(
                    receiver_inputs,
                    shape.schema_version(),
                    authority_key,
                )
                .await?;
            if !installed {
                // A strict relay cannot publish the graph's pre-closure empty
                // opening. Retain the receiver for the next exact receipt.
                initial_received = false;
            } else {
                node.drive_ready_query_runtime_with_waker(progress_waker).await?;
                loop {
                    match receiver.try_recv() {
                        Ok(deltas) => {
                            initial_received = true;
                            let extra = maintained.apply_multisink_deltas(
                                deltas,
                                &terminal_schemas,
                                &tables,
                                &node.node_aliases,
                            )?;
                            transitions.adds.extend(extra.adds);
                            transitions.removes.extend(extra.removes);
                            transitions
                                .result_payload_adds
                                .extend(extra.result_payload_adds);
                            transitions
                                .result_payload_removes
                                .extend(extra.result_payload_removes);
                            transitions
                                .program_fact_adds
                                .extend(extra.program_fact_adds);
                            transitions
                                .program_fact_removes
                                .extend(extra.program_fact_removes);
                            transitions
                                .terminal_operations
                                .extend(extra.terminal_operations);
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => break,
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            return Err(Error::SubscriptionClosed);
                        }
                    }
                }
            }
        }
        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
            eprintln!(
                "JAZZ_COVERED_INPUT_TRACE stage=rehydrate_opened owner={} subscription={subscription:?} identity={policy_identity:?} initial={initial_received} adds={:?} facts={:?}",
                self.publication_owner,
                transitions.adds,
                transitions.program_fact_adds,
            );
        }
        if !terminal_schemas.has_root_collector() {
            maintained.discard_structured_app_rows();
        }
        if !initial_received {
            let maintained_subscription = MaintainedSubscriptionViewSubscription {
                subscription: receiver,
                maintained,
                terminal_schemas,
                tables,
                covered_input_receiver,
                result_schema_version: shape.schema_version(),
                source_authority_result: source_authority_result_key.clone(),
                initial_received: false,
            };
            self.replace_maintained_subscription_view(
                node,
                subscription,
                maintained_subscription,
            );
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
        let known_membership_position = fast_current_membership_position(&known_state);
        let authorization_matches =
            self.fast_cursor_authorization_matches(subscription, &known_state);
        let watermark = self.binding_settlement_time(node, subscription, shape, binding);
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
        let (policy_identity, policy_claims) = self.served_subscription_policy_binding(subscription)?;
        let update = {
            let mut scoped = node.scoped_active_session_claims(policy_identity, policy_claims);
            scoped.view_update_for_maintained_result_members(
            crate::node::MaintainedViewBundleInputs {
                subscription,
                peer_complete_tx_payloads,
                known_state: bundle_known_state,
                complete_exclusive_payloads: self.ship_complete_exclusive_payloads
                    && self.role == PeerRole::Relay,
                previous_result_set: BTreeSet::new(),
                result_member_adds,
                result_member_removes,
                program_fact_adds,
                program_fact_removes,
                identity: policy_identity,
                tier,
                maintained_facts: &maintained,
                allow_storage_witness_fallback: false,
            },
            ).await
        };
        let mut update = match update {
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
            covered_input_receiver,
            result_schema_version: shape.schema_version(),
            source_authority_result: source_authority_result_key,
            initial_received: true,
        };
        self.replace_maintained_subscription_view(node, subscription, maintained_subscription);
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
                opts.clone(),
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
        self.ensure_direct_internal_subscription_policy_binding(node, subscription)?;
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
        let previous_local_authority = self
            .publication_states
            .get(&subscription)
            .map(|state| state.local_authority.clone())
            .unwrap_or_default();
        let known_state = self.downstream_known_states.get(&subscription).cloned();
        let retained_authorization = self.publication_states.get(&subscription).and_then(|state| {
            state
                .has_served_authorization_progress
                .then_some(state.authorization_progress)
        });
        // `forget_subscription_with_node` below retires the old maintained
        // runtime, but the relay's exact upstream receipt is immutable
        // lifecycle metadata for this usage site. Carry it through the
        // replacement rather than making the new receiver rediscover a
        // source from its synthetic group key.
        let authority_result_source = self
            .publication_states
            .get(&subscription)
            .and_then(|state| state.authority_result_source.clone());
        let awaiting_selected_authority_source = self
            .publication_states
            .get(&subscription)
            .is_some_and(|state| state.awaiting_selected_authority_source);
        let policy_binding = self.served_subscription_policy_binding(subscription)?;
        // Retire the old publication before retaining its replacement.  The
        // served policy helper creates a lightweight publication state, so
        // registering first would make this teardown release the just-created
        // outbound shape owner on an initial hydration too.
        self.forget_subscription_with_node(node, subscription);
        self.ensure_query_subscription_registered(
            node,
            subscription,
            shape,
            binding,
            &policy_binding,
        )?;
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
        state.local_authority = previous_local_authority;
        state.policy_binding = Some(policy_binding);
        state.authority_result_source = authority_result_source;
        state.awaiting_selected_authority_source = awaiting_selected_authority_source;
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
        claims: BTreeMap<String, Value>,
        subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        // Terminal authorization support is an authority-owned usage site,
        // not a wire Subscribe. The caller supplies the connection's admitted
        // claim snapshot before owner-loop maintenance can run: the node's
        // author-keyed compatibility cache can already have been overwritten
        // by a sibling session for this same identity.
        self.set_subscription_policy_binding(subscription, (identity, claims));
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
        self.ensure_direct_internal_subscription_policy_binding(node, target_subscription)?;
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
            terminal_operations: _,
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
            || !source_program_fact_adds.is_empty()
            || !source_program_fact_removes.is_empty()
        {
            self.apply_outgoing_view_update_result_set(&SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                subscription: maintained_subscription,
                settled_through: self
                    .canonical_subscription_settlement_time(node, maintained_subscription),
                reset_result_set: false,
                version_carriers: Vec::new(),
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                result_member_adds: source_adds,
                result_member_removes: source_removes,
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
        let current_program_fact_set = canonical_state.program_fact_set.clone();
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
            && self
                .canonical_subscription_settlement_time(node, maintained_subscription)
                .0 > 0
            && position
                >= self.canonical_subscription_settlement_time(node, maintained_subscription)
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
        let (policy_identity, policy_claims) =
            self.served_subscription_policy_binding(target_subscription)?;
        let update = {
            let mut scoped = node.scoped_active_session_claims(policy_identity, policy_claims);
            let maintained = &self
                .publication_states
                .get(&maintained_subscription)
                .and_then(|state| state.maintained_subscription_view.as_ref())
                .ok_or(Error::InvalidStoredValue(
                    "coverage group subscription is missing maintained state",
                ))?
                .maintained;
            scoped.view_update_for_maintained_result_members(
                crate::node::MaintainedViewBundleInputs {
                    subscription: target_subscription,
                    peer_complete_tx_payloads,
                    known_state: (!authorization_mismatch)
                        .then_some(known_state)
                        .flatten(),
                    complete_exclusive_payloads: self.ship_complete_exclusive_payloads
                        && self.role == PeerRole::Relay,
                    previous_result_set: BTreeSet::new(),
                    result_member_adds,
                    result_member_removes: target_result_member_removes,
                    // A newly attached usage site starts with no
                    // subscription-scoped facts even when it shares the
                    // canonical evaluator. Rehydrate the evaluator's complete
                    // current fact closure; forwarding only future deltas
                    // leaves array/join dependencies absent after a one-shot
                    // attachment races their first update.
                    program_fact_adds: current_program_fact_set.iter().cloned().collect(),
                    program_fact_removes: Vec::new(),
                    identity: policy_identity,
                    tier,
                    maintained_facts: maintained,
                    allow_storage_witness_fallback: source_allow_storage_witness_fallback,
                },
            ).await
        };
        let mut update = update?;
        if reset_result_set {
            view_update_reset_result_set(&mut update);
        }
        self.record_outgoing_view_update_metadata(&update);
        self.metrics.maintained_subscription_view.hits_out += 1;
        self.refresh_maintained_subscription_view_footprint(maintained_subscription);
        Ok(Some(update))
    }

    /// Consume canonical maintained-view work and return its publishable
    /// transition without starting the fallible usage-site reset.
    pub(crate) async fn reconcile_maintained_subscription_for_clone<S>(
        &mut self,
        node: &mut NodeState<S>,
        maintained_subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        binding: &Binding,
        _opts: &RegisterShapeOptions,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<Option<ReconciledMaintainedSubscriptionClone>, Error>
    where
        S: OrderedKvStorage,
    {
        self.clear_stale_groove_runtime_handles(node, maintained_subscription);
        let Some(canonical) = self
            .query_update_maintained_subscription_view_with_metadata(
                node,
                shape,
                binding,
                maintained_subscription,
                None,
                progress_waker,
            )
            .await?
        else {
            return Ok(None);
        };
        let (
            source_removes,
            source_had_program_fact_transitions,
            canonical_update_is_empty,
        ) = match &canonical.update {
            SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                result_member_adds,
                result_member_removes,
                program_fact_adds,
                program_fact_removes,
                ..
            }) => (
                result_member_removes.clone(),
                !program_fact_adds.is_empty() || !program_fact_removes.is_empty(),
                maintained_view_update_is_empty(
                    result_member_adds,
                    result_member_removes,
                    program_fact_adds,
                    program_fact_removes,
                ),
            ),
            _ => {
                return Err(Error::InvalidStoredValue(
                    "coverage group canonical update is not a view update",
                ));
            }
        };
        Ok(Some(ReconciledMaintainedSubscriptionClone {
            canonical_update: (!canonical_update_is_empty).then_some(canonical.update),
            source_removes,
            source_had_program_fact_transitions,
            allow_storage_witness_fallback: canonical.allow_storage_witness_fallback,
        }))
    }

    /// Assemble a usage-site reset after the caller has published the canonical
    /// transition to every established sibling.
    pub(crate) async fn rehydrate_query_for_subscription_from_reconciled_maintained_subscription<
        S,
    >(
        &mut self,
        node: &mut NodeState<S>,
        maintained_subscription: SubscriptionKey,
        target_subscription: SubscriptionKey,
        shape: &ValidatedQuery,
        reconciled: ReconciledMaintainedSubscriptionClone,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        #[cfg(test)]
        if FAIL_NEXT_CLONED_SUBSCRIPTION_RESET.with(|fail| fail.take()) {
            return Err(Error::InvalidStoredValue(
                "injected cloned subscription reset failure",
            ));
        }
        let ReconciledMaintainedSubscriptionClone {
            canonical_update: _,
            source_removes,
            source_had_program_fact_transitions,
            allow_storage_witness_fallback,
        } = reconciled;
        let known_state = self
            .downstream_known_states
            .get(&target_subscription)
            .cloned();
        let known_membership_position = fast_current_membership_position(&known_state);
        let authorization_matches =
            self.fast_cursor_authorization_matches(maintained_subscription, &known_state);
        let removed_members_are_ordinary =
            source_removes.iter().all(ordinary_current_content_member);
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

        let canonical_state = self
            .publication_states
            .get(&maintained_subscription)
            .ok_or(Error::InvalidStoredValue(
                "coverage group subscription is missing peer state",
            ))?;
        let current_result_member_set = &canonical_state.result_member_set;
        let current_program_fact_set = canonical_state.program_fact_set.clone();
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
            && self
                .canonical_subscription_settlement_time(node, maintained_subscription)
                .0 > 0
            && position
                >= self.canonical_subscription_settlement_time(node, maintained_subscription)
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
        let (policy_identity, _) = self.served_subscription_policy_binding(target_subscription)?;
        let target_reset = {
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
                    result_member_adds,
                    result_member_removes: target_result_member_removes,
                    // A newly attached usage site starts with no
                    // subscription-scoped facts even when it shares the
                    // canonical evaluator. Rehydrate the evaluator's complete
                    // current fact closure; forwarding only future deltas
                    // leaves array/join dependencies absent after a one-shot
                    // attachment races their first update.
                    program_fact_adds: current_program_fact_set.iter().cloned().collect(),
                    program_fact_removes: Vec::new(),
                    identity: policy_identity,
                    tier,
                    maintained_facts: maintained,
                    allow_storage_witness_fallback,
                },
            )
        };
        let mut target_reset = target_reset.await?;
        if reset_result_set {
            view_update_reset_result_set(&mut target_reset);
        }
        self.record_outgoing_view_update_metadata(&target_reset);
        self.metrics.maintained_subscription_view.hits_out += 1;
        self.refresh_maintained_subscription_view_footprint(maintained_subscription);
        Ok(target_reset)
    }

}
