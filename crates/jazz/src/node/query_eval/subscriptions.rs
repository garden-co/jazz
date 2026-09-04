//! Subscription registration, routing, known-state, and reset lifecycle.
//!
//! This module owns the control-plane state that maps shapes and bindings to
//! durable view identities. Query compilation, maintained terminal reduction,
//! and row materialization remain separate stages.

use super::*;
#[derive(Clone, Copy)]
enum ShapeReclamation {
    One(ShapeId),
}

impl ShapeReclamation {
    fn contains(self, shape_id: ShapeId) -> bool {
        match self {
            Self::One(reclaimed) => reclaimed == shape_id,
        }
    }
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(in crate::node) fn register_shape_with_options(
        &mut self,
        shape_id: ShapeId,
        ast: ShapeAst,
        opts: RegisterShapeOptions,
    ) -> Result<(), Error> {
        let shape = self.validate_shape_ast_for_registration(shape_id, &ast)?;
        self.query
            .registered_shape_options
            .insert((shape_id, opts.read_view_key()), opts.clone());
        // Legacy/default subscriptions carry `ReadViewKey::default()` rather
        // than recomputing the options key. Preserve the received options at
        // that wire identity as well; this is an alias, not reconstruction.
        if opts == RegisterShapeOptions::default() {
            self.query
                .registered_shape_options
                .insert((shape_id, ReadViewKey::default()), opts.clone());
        }
        self.retain_validated_shape_registration(shape_id, ast, shape)?;
        self.query.locally_registered_shapes.insert(shape_id);
        Ok(())
    }

    /// Retain a peer shape together with the exact read-view/compiler options
    /// carried by its registration.  A `SubscriptionKey` names this option
    /// identity, so shape ownership alone cannot reconstruct it later.
    pub(crate) fn register_shape_for_peer_with_options(
        &mut self,
        peer: u64,
        shape_id: ShapeId,
        ast: ShapeAst,
        opts: RegisterShapeOptions,
    ) -> Result<(), Error> {
        if let Some(existing) = self.parking.parked_shape_registrations.get(&shape_id)
            && existing != &ast
        {
            return Err(Error::InvalidStoredValue(
                "conflicting parked shape registration",
            ));
        }
        let shape = self.validate_shape_ast_for_registration(shape_id, &ast)?;
        let already_owned = self
            .query
            .peer_shape_owners
            .get(&shape_id)
            .is_some_and(|owners| owners.contains(&peer));
        if !already_owned {
            let peer_shape_count = self
                .query
                .peer_shape_owners
                .values()
                .filter(|owners| owners.contains(&peer))
                .count();
            if peer_shape_count >= crate::protocol_limits::MAX_SHAPE_REGISTRATIONS_PER_PEER {
                return Err(Error::UnsupportedSyncMessage(
                    "peer shape registration limit exceeded",
                ));
            }
            if !self.query.peer_shape_owners.contains_key(&shape_id)
                && self.query.peer_shape_owners.len()
                    >= crate::protocol_limits::MAX_RETAINED_PEER_SHAPES
            {
                return Err(Error::UnsupportedSyncMessage(
                    "global shape registration limit exceeded",
                ));
            }
        }

        self.query
            .registered_shape_options
            .insert((shape_id, opts.read_view_key()), opts.clone());
        if opts == RegisterShapeOptions::default() {
            self.query
                .registered_shape_options
                .insert((shape_id, ReadViewKey::default()), opts);
        }
        self.retain_validated_shape_registration(shape_id, ast, shape)?;
        self.query
            .peer_shape_owners
            .entry(shape_id)
            .or_default()
            .insert(peer);
        Ok(())
    }

    fn retain_validated_shape_registration(
        &mut self,
        shape_id: ShapeId,
        ast: ShapeAst,
        shape: Option<ValidatedQuery>,
    ) -> Result<(), Error> {
        let Some(shape) = shape else {
            if self
                .parking
                .parked_shape_registrations
                .insert(shape_id, ast)
                .is_none()
            {
                self.sync_metrics.parked_catalogue_shapes += 1;
            }
            return Ok(());
        };
        self.query.registered_shapes.insert(shape_id, shape);
        self.drain_parked_binding_deltas_for_shape(shape_id)
    }

    pub(crate) fn validate_shape_ast_for_registration(
        &self,
        shape_id: ShapeId,
        ast: &ShapeAst,
    ) -> Result<Option<ValidatedQuery>, Error> {
        if ast.version != ShapeAst::VERSION {
            return Err(Error::InvalidStoredValue("unsupported query AST version"));
        }
        let schema = if ast.schema_version == self.catalogue.current_schema_version_id {
            &self.catalogue.schema
        } else {
            let Some(schema) = self.catalogue.catalogue_schemas.get(&ast.schema_version) else {
                return Ok(None);
            };
            &schema.schema
        };
        let shape = match &ast.body {
            ShapeBody::Query(query) => {
                query.validate_with_schema_version(schema, ast.schema_version)?
            }
            ShapeBody::Relation(relation) => relation_query_to_query(relation)?
                .validate_with_schema_version(schema, ast.schema_version)?,
        };
        if shape.shape_id() != shape_id {
            return Err(Error::InvalidStoredValue("shape id does not match AST"));
        }
        Ok(Some(shape))
    }

    pub(in crate::node) fn drain_parked_shape_registrations(&mut self) -> Result<(), Error> {
        let ready = self
            .parking
            .parked_shape_registrations
            .iter()
            .filter_map(|(shape_id, ast)| {
                self.catalogue
                    .catalogue_schemas
                    .contains_key(&ast.schema_version)
                    .then_some((*shape_id, ast.clone()))
            })
            .collect::<Vec<_>>();
        for (shape_id, ast) in ready {
            let shape = self
                .validate_shape_ast_for_registration(shape_id, &ast)?
                .ok_or(Error::InvalidStoredValue(
                    "catalogued shape registration remained unresolved",
                ))?;
            self.parking.parked_shape_registrations.remove(&shape_id);
            self.sync_metrics.parked_catalogue_shapes_resolved += 1;
            self.retain_validated_shape_registration(shape_id, ast, Some(shape))?;
        }
        Ok(())
    }

    pub(crate) fn release_shape_for_peer(&mut self, peer: u64, shape_id: ShapeId) {
        let became_unowned = {
            let Some(owners) = self.query.peer_shape_owners.get_mut(&shape_id) else {
                return;
            };
            if !owners.remove(&peer) {
                return;
            }
            owners.is_empty()
        };
        if !became_unowned {
            return;
        }
        self.query.peer_shape_owners.remove(&shape_id);
        self.reclaim_shape_if_unowned(shape_id);
    }

    pub(crate) fn release_shapes_for_peer(&mut self, peer: u64) {
        let mut newly_unowned = BTreeSet::new();
        self.query.peer_shape_owners.retain(|shape_id, owners| {
            owners.remove(&peer);
            if owners.is_empty() {
                newly_unowned.insert(*shape_id);
                false
            } else {
                true
            }
        });
        for shape_id in newly_unowned {
            self.reclaim_shape_if_unowned(shape_id);
        }
    }

    /// Retain a shape while this node is actively serving it to one concrete
    /// downstream publication.  Unlike `register_shape_for_peer`, this owner
    /// is local process state and must be retired with the `PeerState` that
    /// created it.
    pub(crate) fn register_query_subscription_for_peer(
        &mut self,
        publication_owner: u64,
        shape_id: ShapeId,
        ast: ShapeAst,
        opts: RegisterShapeOptions,
        subscribe: Subscribe,
        policy_binding: crate::protocol::PolicyBindingKey,
    ) -> Result<(), Error> {
        let shape = self.validate_shape_ast_for_registration(shape_id, &ast)?;
        if opts.read_view_key() != subscribe.subscription.read_view {
            return Err(Error::InvalidStoredValue(
                "served subscription read-view does not match its registration options",
            ));
        }
        self.query
            .registered_shape_options
            .insert((shape_id, opts.read_view_key()), opts.clone());
        if opts == RegisterShapeOptions::default() {
            self.query
                .registered_shape_options
                .insert((shape_id, ReadViewKey::default()), opts);
        }
        self.retain_validated_shape_registration(shape_id, ast, shape)?;
        let subscription = subscribe.subscription;
        self.apply_subscribe_with_admitted_policy_binding(subscribe, policy_binding.clone())?;
        self.query
            .outbound_shape_owners
            .entry(shape_id)
            .or_default()
            .insert((publication_owner, subscription));
        self.query
            .outbound_binding_owners
            .entry((subscription, policy_binding))
            .or_default()
            .insert(publication_owner);
        Ok(())
    }

    /// Release one served publication's ownership.  A shape is reclaimed only
    /// after the final local registration, inbound peer registration, and
    /// outbound publication has gone away.
    pub(crate) fn release_query_subscription_for_peer(
        &mut self,
        publication_owner: u64,
        subscription: SubscriptionKey,
    ) {
        let Some(policy_binding) = self
            .unique_registered_binding_for_subscription(subscription)
            .and_then(|binding| binding.authority_result_key.policy_binding.clone())
        else {
            return;
        };
        self.release_query_subscription_for_peer_with_admitted_policy_binding(
            publication_owner,
            subscription,
            policy_binding,
        );
    }

    /// Release one served usage under the immutable policy scope that admitted
    /// it. A peer may share its wire handle with another scope, so this is the
    /// only valid teardown path for multiplexed serving.
    pub(crate) fn release_query_subscription_for_peer_with_admitted_policy_binding(
        &mut self,
        publication_owner: u64,
        subscription: SubscriptionKey,
        policy_binding: crate::protocol::PolicyBindingKey,
    ) {
        let shape_id = subscription.shape_id;
        let Some(shape_owners) = self.query.outbound_shape_owners.get_mut(&shape_id) else {
            return;
        };
        if !shape_owners.remove(&(publication_owner, subscription)) {
            return;
        }
        let became_unowned = shape_owners.is_empty();
        if became_unowned {
            self.query.outbound_shape_owners.remove(&shape_id);
        }
        let binding_became_unowned = self
            .query
            .outbound_binding_owners
            .get_mut(&(subscription, policy_binding.clone()))
            .is_some_and(|owners| {
                owners.remove(&publication_owner);
                owners.is_empty()
            });
        if binding_became_unowned {
            self.query
                .outbound_binding_owners
                .remove(&(subscription, policy_binding.clone()));
            self.apply_unsubscribe_with_admitted_policy_binding(subscription, policy_binding);
        }
        self.reclaim_shape_if_unowned(shape_id);
    }

    fn reclaim_shape_if_unowned(&mut self, shape_id: ShapeId) {
        if self.query.locally_registered_shapes.contains(&shape_id)
            || self.query.peer_shape_owners.contains_key(&shape_id)
            || self.query.outbound_shape_owners.contains_key(&shape_id)
        {
            return;
        }
        self.reclaim_shapes(ShapeReclamation::One(shape_id));
    }

    fn reclaim_shapes(&mut self, reclaimed: ShapeReclamation) {
        match reclaimed {
            ShapeReclamation::One(shape_id) => {
                self.parking.parked_shape_registrations.remove(&shape_id);
                self.parking.parked_binding_deltas.remove(&shape_id);
                self.query.registered_shapes.remove(&shape_id);
                self.query
                    .registered_shape_options
                    .retain(|(registered_shape, _), _| *registered_shape != shape_id);
                self.query.registered_bindings.remove(&shape_id);
            }
        }
        self.query
            .query_shape_cache
            .retain(|(shape_id, _, _), _| !reclaimed.contains(*shape_id));
        self.query
            .applied_view_update_generations
            .retain(|key, _| !reclaimed.contains(key.shape_id));
        self.query
            .authority_results
            .retain(|key, _| !reclaimed.contains(key.binding_view.shape_id));
        self.query
            .settled_result_sets
            .retain(|key, _| !reclaimed.contains(key.shape_id));
        self.query
            .settled_result_row_index
            .retain(|key, _| !reclaimed.contains(key.shape_id));
        self.query
            .settled_program_facts
            .retain(|key, _| !reclaimed.contains(key.shape_id));
        self.query
            .settled_through_by_binding_view
            .retain(|key, _| !reclaimed.contains(key.shape_id));
        self.query
            .authorization_progress_by_binding_view
            .retain(|key, _| !reclaimed.contains(key.shape_id));
        self.query
            .known_state_declared_binding_views
            .retain(|key| !reclaimed.contains(key.shape_id));
        self.query
            .initial_hydration_binding_views
            .retain(|key| !reclaimed.contains(key.shape_id));
        self.query
            .deferred_publication_binding_views
            .retain(|key| !reclaimed.contains(key.shape_id));
        self.query
            .pending_authoritative_reset_binding_views
            .retain(|key| !reclaimed.contains(key.shape_id));
        self.query
            .pending_opening_binding_views
            .retain(|key| !reclaimed.contains(key.shape_id));
        self.query
            .outbound_binding_owners
            .retain(|(subscription, _), _| !reclaimed.contains(subscription.shape_id));
    }

    #[cfg(test)]
    pub(crate) fn outbound_shape_owner_count_for_test(&self, shape_id: ShapeId) -> usize {
        self.query
            .outbound_shape_owners
            .get(&shape_id)
            .map_or(0, BTreeSet::len)
    }

    pub(in crate::node) fn apply_subscribe(&mut self, subscribe: Subscribe) -> Result<(), Error> {
        let Some(shape) = self
            .query
            .registered_shapes
            .get(&subscribe.shape_id)
            .cloned()
        else {
            self.parking
                .parked_binding_deltas
                .entry(subscribe.shape_id)
                .or_default()
                .push(subscribe);
            return Ok(());
        };
        self.apply_known_shape_subscribe(&shape, subscribe)
    }

    /// Register a server-admitted subscriber usage under the exact immutable
    /// policy snapshot selected by its connection. This is deliberately a
    /// privileged server-side entry point: direct clients do not put their
    /// claims on the wire, and a relay's transport identity must never be used
    /// to recreate the subscriber's policy scope later.
    pub(crate) fn apply_subscribe_with_admitted_policy_binding(
        &mut self,
        subscribe: Subscribe,
        policy_binding: crate::protocol::PolicyBindingKey,
    ) -> Result<(), Error> {
        if let Some(delegated) = &subscribe.delegated_session
            && crate::protocol::PolicyBindingKey::from_delegated_session(delegated)
                != policy_binding
        {
            return Err(Error::InvalidStoredValue(
                "delegated subscription policy binding disagrees with admission",
            ));
        }
        let Some(shape) = self
            .query
            .registered_shapes
            .get(&subscribe.shape_id)
            .cloned()
        else {
            // Server admission always registers the shape before this call.
            // Parking only the wire Subscribe would lose the immutable policy
            // binding and could later reopen it under an unrelated reader.
            return Err(Error::InvalidStoredValue(
                "admitted subscription referenced unregistered shape",
            ));
        };
        if subscribe.values.len() != shape.params().len() {
            return Err(Error::InvalidStoredValue("binding arity mismatch"));
        }
        let value_map = shape
            .params()
            .keys()
            .cloned()
            .zip(subscribe.values.iter().cloned())
            .collect::<BTreeMap<_, _>>();
        let binding = shape.bind(value_map)?;
        let binding_view_key = BindingViewKey {
            shape_id: subscribe.shape_id,
            binding_id: binding.binding_id(),
            read_view: subscribe.subscription.read_view,
        };
        self.apply_known_shape_subscribe_with_authority_result_key(
            &shape,
            subscribe,
            AuthorityResultKey::policy_scoped(binding_view_key, policy_binding),
        )
    }

    /// Return the exact policy-scoped durable identity fixed at subscription
    /// admission. Wire updates contain only the usage handle and cannot choose
    /// or reconstruct this identity themselves.
    pub(crate) fn authority_result_key_for_subscription(
        &self,
        subscription: SubscriptionKey,
    ) -> Result<AuthorityResultKey, Error> {
        self.unique_registered_binding_for_subscription(subscription)
            .map(|binding| binding.authority_result_key.clone())
            .or_else(|| {
                self.canonical_whole_table_binding_view_key(subscription)
                    .ok()
                    .flatten()
                    .map(AuthorityResultKey::unscoped)
            })
            .ok_or(Error::InvalidStoredValue(
                "subscription referenced unregistered binding",
            ))
    }

    /// Return an authority receipt only when the canonical binding view names
    /// exactly one policy scope.  This is a compatibility lookup for callers
    /// that do not yet carry a usage subscription; it intentionally refuses
    /// to guess in a multiplexed relay. New relay paths must carry the exact
    /// `AuthorityResultKey` from registration instead.
    pub(crate) fn unique_authority_result_key_for_binding_view(
        &self,
        binding_view: BindingViewKey,
    ) -> Option<AuthorityResultKey> {
        let mut matches = self
            .query
            .authority_results
            .keys()
            .filter(|key| key.binding_view == binding_view)
            .cloned();
        let first = matches.next()?;
        matches.next().is_none().then_some(first)
    }

    /// Borrow a result only when its canonical binding names one exact
    /// authority scope. Binding-only client helpers must fail closed in a
    /// multiplexed relay rather than selecting whichever session arrived
    /// last. Relay serving paths carry `AuthorityResultKey` explicitly.
    #[cfg(test)]
    pub(crate) fn authority_result_state_for_binding_view(
        &self,
        binding_view: BindingViewKey,
    ) -> Option<&AuthorityResultState> {
        self.unique_authority_result_key_for_binding_view(binding_view)
            .and_then(|key| self.query.authority_results.get(&key))
    }

    fn drain_parked_binding_deltas_for_shape(&mut self, shape_id: ShapeId) -> Result<(), Error> {
        let Some(deltas) = self.parking.parked_binding_deltas.remove(&shape_id) else {
            return Ok(());
        };
        let Some(shape) = self.query.registered_shapes.get(&shape_id).cloned() else {
            self.parking.parked_binding_deltas.insert(shape_id, deltas);
            return Ok(());
        };
        for subscribe in deltas {
            self.apply_known_shape_subscribe(&shape, subscribe)?;
        }
        Ok(())
    }

    fn apply_known_shape_subscribe(
        &mut self,
        shape: &ValidatedQuery,
        subscribe: Subscribe,
    ) -> Result<(), Error> {
        if subscribe.values.len() != shape.params().len() {
            return Err(Error::InvalidStoredValue("binding arity mismatch"));
        }
        let value_map = shape
            .params()
            .keys()
            .cloned()
            .zip(subscribe.values.iter().cloned())
            .collect::<BTreeMap<_, _>>();
        let binding = shape.bind(value_map)?;
        let binding_view_key = BindingViewKey {
            shape_id: subscribe.shape_id,
            binding_id: binding.binding_id(),
            read_view: subscribe.subscription.read_view,
        };
        let authority_result_key = subscribe
            .delegated_session
            .as_ref()
            .map(crate::protocol::PolicyBindingKey::from_delegated_session)
            .map(|policy| AuthorityResultKey::policy_scoped(binding_view_key, policy))
            .unwrap_or_else(|| AuthorityResultKey::unscoped(binding_view_key));
        self.apply_known_shape_subscribe_with_authority_result_key(
            shape,
            subscribe,
            authority_result_key,
        )
    }

    fn apply_known_shape_subscribe_with_authority_result_key(
        &mut self,
        shape: &ValidatedQuery,
        subscribe: Subscribe,
        authority_result_key: AuthorityResultKey,
    ) -> Result<(), Error> {
        if subscribe.values.len() != shape.params().len() {
            return Err(Error::InvalidStoredValue("binding arity mismatch"));
        }
        let value_map = shape
            .params()
            .keys()
            .cloned()
            .zip(subscribe.values.iter().cloned())
            .collect::<BTreeMap<_, _>>();
        let binding = shape.bind(value_map)?;
        let binding_view_key = BindingViewKey {
            shape_id: subscribe.shape_id,
            binding_id: binding.binding_id(),
            read_view: subscribe.subscription.read_view,
        };
        let options = self
            .query
            .registered_shape_options
            .get(&(subscribe.shape_id, subscribe.subscription.read_view))
            .cloned()
            .or_else(|| {
                (subscribe.subscription.read_view == ReadViewKey::default())
                    .then(|| {
                        self.query
                            .registered_shape_options
                            .iter()
                            .filter(|((shape_id, _), options)| {
                                *shape_id == subscribe.shape_id
                                    && **options == RegisterShapeOptions::default()
                            })
                            .map(|(_, options)| options.clone())
                            .next()
                    })
                    .flatten()
            })
            .ok_or(Error::InvalidStoredValue(
                "subscription referenced unregistered read-view options",
            ))?;
        if authority_result_key.binding_view != binding_view_key {
            return Err(Error::InvalidStoredValue(
                "subscription authority result binding view disagrees with usage",
            ));
        }
        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
            eprintln!(
                "JAZZ_COVERED_INPUT_TRACE stage=subscribe_receipt binding={binding_view_key:?} delegated={:?} authority={authority_result_key:?}",
                subscribe.delegated_session,
            );
        }
        // A new wire subscription needs a fresh authority receipt. It may
        // retire only its own policy-scoped retained local page; another
        // scope with the same public binding remains isolated.
        if self
            .query
            .retained_root_window_sources
            .remove(&authority_result_key)
            .is_some()
        {
            self.clear_settled_result_view(authority_result_key.clone());
        }
        if subscribe.known_state.is_some() {
            self.query
                .known_state_declared_binding_views
                .insert(binding_view_key);
        } else {
            self.query
                .known_state_declared_binding_views
                .remove(&binding_view_key);
        }
        self.query
            .authority_results
            .entry(authority_result_key.clone())
            .or_default()
            .known_state_declared = subscribe.known_state.is_some();
        let registered_key = registered_binding_usage_key(
            subscribe.subscription,
            authority_result_key.policy_binding.clone(),
        );
        self.query
            .registered_bindings
            .entry(subscribe.shape_id)
            .or_default()
            .insert(
                registered_key,
                RegisteredBinding {
                    values: subscribe.values,
                    read_view: subscribe.subscription.read_view,
                    binding_view_key,
                    authority_result_key,
                    options,
                    compiler_identity: subscribe
                        .delegated_session
                        .as_ref()
                        .map(|session| session.identity.clone())
                        .unwrap_or(AuthorSubject::SYSTEM),
                },
            );
        Ok(())
    }

    pub(crate) fn apply_unsubscribe(&mut self, subscription: SubscriptionKey) {
        let Some(registered_key) = self.unique_registered_binding_usage_key(subscription) else {
            // A bare wire handle is deliberately insufficient to retire an
            // ambiguous multiplexed policy scope. Authenticated serving paths
            // call `apply_unsubscribe_with_admitted_policy_binding` instead.
            return;
        };
        self.apply_unsubscribe_registered_binding(subscription, registered_key);
    }

    /// Retire exactly the policy scope selected by authenticated connection
    /// admission. This is intentionally internal: a client-controlled
    /// Subscribe payload must never select the scope later used for teardown.
    pub(crate) fn apply_unsubscribe_with_admitted_policy_binding(
        &mut self,
        subscription: SubscriptionKey,
        policy_binding: crate::protocol::PolicyBindingKey,
    ) {
        self.apply_unsubscribe_registered_binding(
            subscription,
            registered_binding_usage_key(subscription, Some(policy_binding)),
        );
    }

    fn apply_unsubscribe_registered_binding(
        &mut self,
        subscription: SubscriptionKey,
        registered_key: RegisteredBindingUsageKey,
    ) {
        let registered = self
            .query
            .registered_bindings
            .get(&subscription.shape_id)
            .and_then(|bindings| bindings.get(&registered_key))
            .cloned();
        let Some(registered) = registered else {
            return;
        };
        let binding_view_key = registered.binding_view_key;
        let authority_result_key = registered.authority_result_key;
        let retained_local_window = self
            .query
            .registered_shapes
            .get(&subscription.shape_id)
            .filter(|_| self.authored_commit_durability == DurabilityTier::None)
            .map(RetainedRootWindowSource::for_shape)
            .filter(RetainedRootWindowSource::is_bounded)
            .filter(|_| {
                self.query
                    .authority_results
                    .get(&authority_result_key)
                    .is_some_and(|state| {
                        matches!(state.source_closure, AuthoritySourceClosure::Claimed { .. })
                    })
            });
        if let Some(bindings) = self
            .query
            .registered_bindings
            .get_mut(&subscription.shape_id)
        {
            bindings.remove(&registered_key);
        }
        if !self.registered_binding_resolves_to_authority_result_key(&authority_result_key) {
            // Registered bindings are the receipt ownership record. Once the
            // last downstream usage site releases this exact binding view,
            // revoke its authority-selected membership rather than retaining
            // a browser cache after scope teardown.
            if let Some(window) = retained_local_window {
                self.query
                    .retained_root_window_sources
                    .insert(authority_result_key.clone(), window);
            } else {
                self.retire_authority_result_view(authority_result_key);
            }
            self.query.settled_program_facts.remove(&binding_view_key);
            self.query
                .known_state_declared_binding_views
                .remove(&binding_view_key);
            self.query
                .initial_hydration_binding_views
                .remove(&binding_view_key);
            self.query
                .pending_opening_binding_views
                .remove(&binding_view_key);
        }
    }

    #[cfg(any(test, feature = "testing"))]
    /// Test-only count of live wire binding registrations. This is deliberately
    /// usage-site state, rather than the deduplicated evaluator count.
    pub fn registered_query_binding_count_for_test(&self) -> usize {
        self.query
            .registered_bindings
            .values()
            .map(BTreeMap::len)
            .sum()
    }

    #[cfg(any(test, feature = "testing"))]
    /// Internal receipt-lifetime coverage needs to observe canonical caches:
    /// public reads intentionally treat a Local overlay as best-effort.
    pub fn settled_authoritative_receipt_counts_for_test(&self) -> (usize, usize) {
        (
            self.query
                .authority_results
                .iter()
                .filter(|(key, state)| {
                    !self.query.retained_root_window_sources.contains_key(key)
                        && (state.live_settled
                            || state.settled_through.is_some()
                            || !state.settled_result_set.is_empty()
                            || !state.settled_program_facts.is_empty())
                })
                .count(),
            self.query
                .authority_results
                .iter()
                .filter(|(key, state)| {
                    !self.query.retained_root_window_sources.contains_key(*key)
                        && !state.settled_program_facts.is_empty()
                })
                .count(),
        )
    }

    fn registered_binding_resolves_to_binding_view_key(
        &self,
        binding_view_key: BindingViewKey,
    ) -> bool {
        let Some(bindings) = self
            .query
            .registered_bindings
            .get(&binding_view_key.shape_id)
        else {
            return false;
        };
        bindings.values().any(|registered| {
            if registered.read_view != binding_view_key.read_view {
                return false;
            }
            registered.binding_view_key == binding_view_key
        })
    }

    fn registered_binding_resolves_to_authority_result_key(
        &self,
        authority_result_key: &AuthorityResultKey,
    ) -> bool {
        self.query
            .registered_bindings
            .get(&authority_result_key.binding_view.shape_id)
            .is_some_and(|bindings| {
                bindings
                    .values()
                    .any(|registered| registered.authority_result_key == *authority_result_key)
            })
    }

    /// Forget a recovered authority result that has no live wire owner.
    ///
    /// Settled result membership is durable, but relay registration ownership
    /// is intentionally process-local. A reopened relay therefore cannot use
    /// an ownerless `RelayAuthoritySession` view to satisfy a new downstream
    /// usage site: it must first receive a current authoritative reset.
    pub(crate) fn invalidate_ownerless_settled_result_view(
        &mut self,
        binding_view_key: BindingViewKey,
    ) -> bool {
        if self.registered_binding_resolves_to_binding_view_key(binding_view_key)
            || !self
                .query
                .authority_results
                .keys()
                .any(|key| key.binding_view == binding_view_key)
        {
            return false;
        }

        let authority_result_keys = self
            .query
            .authority_results
            .keys()
            .filter(|key| key.binding_view == binding_view_key)
            .cloned()
            .collect::<Vec<_>>();
        for authority_result_key in authority_result_keys {
            self.retire_authority_result_view(authority_result_key);
        }
        self.query.settled_program_facts.remove(&binding_view_key);
        self.query
            .settled_through_by_binding_view
            .remove(&binding_view_key);
        self.query
            .authorization_progress_by_binding_view
            .remove(&binding_view_key);
        self.query
            .pending_authoritative_reset_binding_views
            .remove(&binding_view_key);
        self.query
            .deferred_publication_binding_views
            .remove(&binding_view_key);
        true
    }

    /// Exact receipt variant for a usage subscription that carries delegated
    /// policy context. Unlike the binding-only compatibility facade, this
    /// never searches across sessions.
    pub(crate) fn has_settled_authority_result(
        &self,
        authority_result_key: &AuthorityResultKey,
    ) -> bool {
        self.query
            .authority_results
            .get(authority_result_key)
            .is_some_and(|state| state.live_settled)
            && !self
                .query
                .retained_root_window_sources
                .contains_key(authority_result_key)
    }

    pub(crate) fn applied_authority_result_generation(
        &self,
        authority_result_key: &AuthorityResultKey,
    ) -> u64 {
        self.query
            .authority_results
            .get(authority_result_key)
            .map_or(0, |state| state.applied_view_update_generation)
    }

    /// Generation of the exact authority-selected source frontier, if this
    /// usage site has claimed one. This is intentionally not the raw
    /// ViewUpdate counter: later incremental receipt frames can advance that
    /// counter without replacing the covered source closure.
    pub(crate) fn authority_source_closure_generation(
        &self,
        authority_result_key: &AuthorityResultKey,
    ) -> Option<u64> {
        self.query
            .authority_results
            .get(authority_result_key)
            .and_then(|state| match state.source_closure {
                crate::node::AuthoritySourceClosure::Pending => None,
                crate::node::AuthoritySourceClosure::Claimed { generation } => Some(generation),
            })
    }

    #[cfg(test)]
    pub(crate) fn reset_subscription_snapshot_for_link_call_count(&mut self) {
        SUBSCRIPTION_SNAPSHOT_FOR_LINK_CALLS.with(|calls| calls.set(0));
    }

    #[cfg(test)]
    pub(crate) fn subscription_snapshot_for_link_call_count(&self) -> usize {
        SUBSCRIPTION_SNAPSHOT_FOR_LINK_CALLS.with(std::cell::Cell::get)
    }

    /// Drain exact authority receipts whose next publication must be a reset.
    ///
    /// A binding view is merely a local cache address. It cannot represent a
    /// lifecycle event once different delegated sessions share that view.
    pub(crate) fn take_pending_authoritative_resets(&mut self) -> BTreeSet<AuthorityResultKey> {
        self.query
            .authority_results
            .iter_mut()
            .filter_map(|(key, state)| {
                state.pending_authoritative_reset.then(|| {
                    state.pending_authoritative_reset = false;
                    key.clone()
                })
            })
            .collect()
    }

    pub(crate) fn defer_authoritative_reset(&mut self, authority_result_key: &AuthorityResultKey) {
        self.query
            .authority_results
            .entry(authority_result_key.clone())
            .or_default()
            .pending_authoritative_reset = true;
    }

    pub(crate) fn publication_deferred_for_authority_result(
        &self,
        authority_result_key: &AuthorityResultKey,
    ) -> bool {
        self.query
            .authority_results
            .get(authority_result_key)
            .is_some_and(|state| state.deferred_publication)
    }

    pub(crate) fn opening_pending_for_authority_result(
        &self,
        authority_result_key: &AuthorityResultKey,
    ) -> bool {
        self.query
            .authority_results
            .get(authority_result_key)
            .is_some_and(|state| state.pending_opening)
    }

    /// Return the settlement watermark for one exact authority result.
    ///
    /// A binding view can be shared by several delegated policy snapshots on
    /// a relay. Publication code that already has a usage-site receipt must
    /// use this exact lookup rather than the binding-view compatibility
    /// facade, which deliberately refuses to select a sibling scope.
    pub(crate) fn settled_through_for_authority_result(
        &self,
        authority_result_key: &AuthorityResultKey,
    ) -> Option<GlobalTime> {
        self.query
            .authority_results
            .get(authority_result_key)
            .and_then(|state| state.settled_through)
    }

    pub(crate) async fn known_state_declaration_for_subscription(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        values: &[Value],
        identity: AuthorSubject,
        policy_binding: Option<&(AuthorSubject, BTreeMap<String, Value>)>,
    ) -> Result<Option<KnownStateDeclaration>, Error> {
        let binding_view_key = BindingViewKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: subscription.read_view,
        };
        // This declaration is assembled before the outbound Subscribe is
        // registered locally, so resolve its exact future receipt from the
        // same immutable delegated policy snapshot that will be sent with it.
        // Do not inspect a sibling receipt just because it shares the
        // canonical binding view.
        let authority_result_key = policy_binding
            .map(|(identity, claims)| {
                AuthorityResultKey::policy_scoped(
                    binding_view_key,
                    crate::protocol::PolicyBindingKey::from_canonical_parts(
                        *identity,
                        claims.clone(),
                    ),
                )
            })
            .unwrap_or_else(|| AuthorityResultKey::unscoped(binding_view_key));
        if !self.has_settled_authority_result(&authority_result_key) {
            // An absent/retired live receipt cannot be recreated from its
            // durable cursor alone: that cursor does not restore the source
            // manifest or facts. Full startup recovery loads these together;
            // a new usage here must instead await a fresh authority closure.
            // Slow exact declarations are still known-state declarations: they
            // must describe a binding view the server has previously settled
            // for this client. A purely local first subscription could include
            // rows the serving peer has not observed yet; truncating that to an
            // exact set would silently overclaim and can make stale rehydrate
            // responses suppress local live state.
            return Ok(None);
        }
        if let Some(position) = self
            .query
            .authority_results
            .get(&authority_result_key)
            .and_then(|state| state.settled_through)
        {
            let authorization_progress = self
                .query
                .authority_results
                .get(&authority_result_key)
                .and_then(|state| state.authorization_progress);
            return Ok(Some(match authorization_progress {
                Some(authorization_progress) => {
                    KnownStateDeclaration::FastWithAuthorizationProgress {
                        completeness: KnownStateCompleteness::FastCurrentMembership,
                        position,
                        authorization_progress,
                    }
                }
                None => KnownStateDeclaration::Fast {
                    completeness: KnownStateCompleteness::FastCurrentMembership,
                    position,
                },
            }));
        }
        // A live exact receipt without a fast watermark still proves which
        // membership this process received, but cannot claim currentness at a
        // global cursor. Fall through to the bounded exact version set.
        let mut refs = Vec::new();
        for row in self
            .query_rows_for_link(shape, binding, DurabilityTier::Local, identity)
            .await?
        {
            let Some(tx_id) = self.current_row_tx_id(&row).await else {
                continue;
            };
            refs.push(RowVersionRef::new(
                row.table().to_owned(),
                row.row_uuid(),
                tx_id,
            ));
        }
        refs.sort();
        refs.dedup();
        if refs.is_empty() {
            return Ok(None);
        }
        Ok(exact_known_state_declaration_if_within_limits(
            shape.shape_id(),
            subscription,
            values,
            refs,
        ))
    }

    #[allow(dead_code)]
    pub(crate) fn subscription_is_known_state_declared(
        &self,
        subscription: SubscriptionKey,
    ) -> Result<bool, Error> {
        let authority_result_key = match self.authority_result_key_for_subscription(subscription) {
            Ok(authority_result_key) => authority_result_key,
            Err(Error::InvalidStoredValue(
                "subscription referenced unregistered shape"
                | "subscription referenced unregistered binding",
            )) => return Ok(false),
            Err(error) => return Err(error),
        };
        Ok(self
            .query
            .authority_results
            .get(&authority_result_key)
            .is_some_and(|state| state.known_state_declared))
    }

    pub(crate) fn binding_view_key_for_subscription(
        &self,
        subscription: SubscriptionKey,
    ) -> Result<BindingViewKey, Error> {
        if let Some(registered) = self.unique_registered_binding_for_subscription(subscription) {
            return Ok(registered.binding_view_key);
        }
        if let Some(binding_view_key) = self.canonical_whole_table_binding_view_key(subscription)? {
            return Ok(binding_view_key);
        }
        Err(Error::InvalidStoredValue(
            "subscription referenced unregistered binding",
        ))
    }

    /// Resolve a wire handle only when one admitted scope owns it. Callers at
    /// authenticated ingress use their exact admission binding instead; this
    /// compatibility lookup must never choose a sibling scope by arrival
    /// order.
    pub(super) fn unique_registered_binding_for_subscription(
        &self,
        subscription: SubscriptionKey,
    ) -> Option<&RegisteredBinding> {
        let bindings = self.query.registered_bindings.get(&subscription.shape_id)?;
        let mut matches = bindings.iter().filter_map(|(key, binding)| {
            (key.0 == subscription.binding_id && key.1 == subscription.read_view).then_some(binding)
        });
        let binding = matches.next()?;
        matches.next().is_none().then_some(binding)
    }

    fn unique_registered_binding_usage_key(
        &self,
        subscription: SubscriptionKey,
    ) -> Option<RegisteredBindingUsageKey> {
        let bindings = self.query.registered_bindings.get(&subscription.shape_id)?;
        let mut matches = bindings
            .keys()
            .filter(|key| key.0 == subscription.binding_id && key.1 == subscription.read_view);
        let key = matches.next()?.clone();
        matches.next().is_none().then_some(key)
    }

    fn canonical_whole_table_binding_view_key(
        &self,
        subscription: SubscriptionKey,
    ) -> Result<Option<BindingViewKey>, Error> {
        for table in &self.catalogue.schema.tables {
            if self.whole_table_subscription_key(&table.name)? == subscription {
                return Ok(Some(BindingViewKey::from_canonical_subscription_key(
                    subscription,
                )));
            }
        }
        Ok(None)
    }
}

fn registered_binding_usage_key(
    subscription: SubscriptionKey,
    policy_binding: Option<crate::protocol::PolicyBindingKey>,
) -> RegisteredBindingUsageKey {
    (
        subscription.binding_id,
        subscription.read_view,
        policy_binding,
    )
}
