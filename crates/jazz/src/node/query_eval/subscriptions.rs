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
    pub(in crate::node) fn register_shape(
        &mut self,
        shape_id: ShapeId,
        ast: ShapeAst,
    ) -> Result<(), Error> {
        let shape = self.validate_shape_ast_for_registration(shape_id, &ast)?;
        self.retain_validated_shape_registration(shape_id, ast, shape)?;
        self.query.locally_registered_shapes.insert(shape_id);
        Ok(())
    }

    pub(crate) fn register_shape_for_peer(
        &mut self,
        peer: u64,
        shape_id: ShapeId,
        ast: ShapeAst,
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
        subscribe: Subscribe,
    ) -> Result<(), Error> {
        let shape = self.validate_shape_ast_for_registration(shape_id, &ast)?;
        self.retain_validated_shape_registration(shape_id, ast, shape)?;
        let subscription = subscribe.subscription;
        self.apply_subscribe(subscribe)?;
        self.query
            .outbound_shape_owners
            .entry(shape_id)
            .or_default()
            .insert((publication_owner, subscription));
        self.query
            .outbound_binding_owners
            .entry(subscription)
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
            .get_mut(&subscription)
            .is_some_and(|owners| {
                owners.remove(&publication_owner);
                owners.is_empty()
            });
        if binding_became_unowned {
            self.query.outbound_binding_owners.remove(&subscription);
            self.apply_unsubscribe(subscription);
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
            .pending_terminal_operations_by_binding_view
            .retain(|key, _| !reclaimed.contains(key.shape_id));
        self.query
            .outbound_binding_owners
            .retain(|subscription, _| !reclaimed.contains(subscription.shape_id));
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

    /// Return the exact policy-scoped durable identity fixed at subscription
    /// admission. Wire updates contain only the usage handle and cannot choose
    /// or reconstruct this identity themselves.
    pub(crate) fn authority_result_key_for_subscription(
        &self,
        subscription: SubscriptionKey,
    ) -> Result<AuthorityResultKey, Error> {
        self.query
            .registered_bindings
            .get(&subscription.shape_id)
            .and_then(|bindings| bindings.get(&(subscription.binding_id, subscription.read_view)))
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
        // A new wire subscription needs an authority receipt. Discard a
        // browser-only materialized-window interpretation before any opening
        // reset can otherwise preserve its old member set.
        if self
            .query
            .local_materialized_window_binding_views
            .remove(&binding_view_key)
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
        self.query
            .registered_bindings
            .entry(subscribe.shape_id)
            .or_default()
            .insert(
                (
                    subscribe.subscription.binding_id,
                    subscribe.subscription.read_view,
                ),
                RegisteredBinding {
                    values: subscribe.values,
                    read_view: subscribe.subscription.read_view,
                    binding_view_key,
                    authority_result_key,
                },
            );
        Ok(())
    }

    pub(crate) fn apply_unsubscribe(&mut self, subscription: SubscriptionKey) {
        let binding_view_key = self.binding_view_key_for_subscription(subscription).ok();
        let authority_result_key = self
            .authority_result_key_for_subscription(subscription)
            .ok();
        let retain_local_materialized_window = binding_view_key.is_some_and(|binding_view_key| {
            self.authored_commit_durability == DurabilityTier::None
                && self
                    .query
                    .registered_shapes
                    .get(&subscription.shape_id)
                    .is_some_and(|shape| shape.query().offset != 0)
                && self
                    .query
                    .authority_results
                    .get(&AuthorityResultKey::unscoped(binding_view_key))
                    .is_some_and(|state| !state.settled_result_set.is_empty())
        });
        if let Some(bindings) = self
            .query
            .registered_bindings
            .get_mut(&subscription.shape_id)
        {
            bindings.remove(&(subscription.binding_id, subscription.read_view));
        }
        if let Some(binding_view_key) = binding_view_key
            && let Some(authority_result_key) = authority_result_key
            && !self.registered_binding_resolves_to_authority_result_key(&authority_result_key)
        {
            // Registered bindings are the receipt ownership record. Once the
            // last downstream usage site releases this exact binding view,
            // revoke its authority-selected membership rather than retaining
            // a browser cache after scope teardown.
            if retain_local_materialized_window {
                self.query
                    .local_materialized_window_binding_views
                    .insert(binding_view_key);
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
                    !self
                        .query
                        .local_materialized_window_binding_views
                        .contains(&key.binding_view)
                        && (state.live_settled
                            || state.settled_through.is_some()
                            || !state.settled_result_set.is_empty()
                            || !state.settled_program_facts.is_empty())
                })
                .count(),
            self.query
                .authority_results
                .values()
                .filter(|state| !state.settled_program_facts.is_empty())
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
            .pending_terminal_operations_by_binding_view
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
                .local_materialized_window_binding_views
                .contains(&authority_result_key.binding_view)
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

    #[cfg(test)]
    pub(crate) fn reset_subscription_snapshot_for_link_call_count(&mut self) {
        SUBSCRIPTION_SNAPSHOT_FOR_LINK_CALLS.with(|calls| calls.set(0));
    }

    #[cfg(test)]
    pub(crate) fn subscription_snapshot_for_link_call_count(&self) -> usize {
        SUBSCRIPTION_SNAPSHOT_FOR_LINK_CALLS.with(std::cell::Cell::get)
    }

    #[cfg(test)]
    pub(crate) fn inject_pending_authoritative_reset_for_test(
        &mut self,
        binding_view_key: BindingViewKey,
        members: impl IntoIterator<Item = ResultMemberEntry>,
        settled_through: GlobalTime,
    ) {
        let authority_result_key = AuthorityResultKey::unscoped(binding_view_key);
        self.clear_settled_result_view(authority_result_key.clone());
        for member in members {
            self.insert_settled_result_member_indexed(authority_result_key.clone(), member);
        }
        self.query
            .authority_results
            .entry(authority_result_key)
            .or_default()
            .settled_through = Some(settled_through);
        self.query
            .authority_results
            .entry(AuthorityResultKey::unscoped(binding_view_key))
            .or_default()
            .pending_authoritative_reset = true;
    }

    #[cfg(test)]
    pub(crate) fn inject_pending_authoritative_reset_with_program_facts_for_test(
        &mut self,
        binding_view_key: BindingViewKey,
        members: impl IntoIterator<Item = ResultMemberEntry>,
        program_facts: impl IntoIterator<Item = ProgramFactEntry>,
        settled_through: GlobalTime,
    ) {
        self.inject_pending_authoritative_reset_for_test(
            binding_view_key,
            members,
            settled_through,
        );
        self.query
            .authority_results
            .entry(AuthorityResultKey::unscoped(binding_view_key))
            .or_default()
            .settled_program_facts = program_facts.into_iter().collect();
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

    pub(crate) fn take_pending_terminal_operations(
        &mut self,
        authority_result_key: &AuthorityResultKey,
    ) -> Vec<groove::ivm::TerminalOperation> {
        std::mem::take(
            &mut self
                .query
                .authority_results
                .entry(authority_result_key.clone())
                .or_default()
                .pending_terminal_operations,
        )
    }

    pub(crate) fn defer_authoritative_reset(&mut self, authority_result_key: &AuthorityResultKey) {
        self.query
            .authority_results
            .entry(authority_result_key.clone())
            .or_default()
            .pending_authoritative_reset = true;
    }

    #[cfg(test)]
    pub(crate) fn has_pending_authoritative_reset_for_test(
        &self,
        binding_view_key: BindingViewKey,
    ) -> bool {
        self.authority_result_state_for_binding_view(binding_view_key)
            .is_some_and(|state| state.pending_authoritative_reset)
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

    pub(crate) fn settled_result_transitions_for_subscription(
        &self,
        subscription: SubscriptionKey,
        source_authority_result: Option<AuthorityResultKey>,
        previous_member_result_set: &BTreeSet<ResultMemberEntry>,
        previous_program_fact_set: &BTreeSet<ProgramFactEntry>,
        result_table_filter: Option<&str>,
        output_tables: &BTreeMap<String, TableSchema>,
    ) -> Result<Option<super::maintained_subscription_view::ResultTransitions>, Error> {
        let authority_result_key = source_authority_result
            .map(Ok)
            .unwrap_or_else(|| self.authority_result_key_for_subscription(subscription))?;
        // Settled binding views are shared by canonical query binding, while a
        // table read policy is identity-scoped. Never relay a synthetic
        // aggregate from that shared cache across an identity boundary; the
        // per-peer maintained program remains the authority for policy-shaped
        // aggregate output.
        let shared_view_has_read_policy = self
            .query
            .registered_shapes
            .get(&subscription.shape_id)
            .and_then(|shape| self.table(shape.query().table.as_str()).ok())
            .is_some_and(TableSchema::has_any_policy);
        let Some(authority_result) = self.query.authority_results.get(&authority_result_key) else {
            return Ok(None);
        };
        let settled_members = &authority_result.settled_result_set;
        let settled_facts = self
            .query
            .authority_results
            .get(&authority_result_key)
            .map(|state| &state.settled_program_facts)
            .cloned()
            .unwrap_or_default();
        let member_is_visible = |member: &ResultMemberEntry| {
            let Some(table_name) = member.table_name() else {
                return false;
            };
            result_table_filter.is_none_or(|table| table_name == table)
                && (output_tables.contains_key(table_name)
                    || (matches!(member, ResultMemberEntry::Synthetic { .. })
                        && !shared_view_has_read_policy))
        };
        let current = settled_members
            .iter()
            .filter(|member| member_is_visible(member))
            .cloned()
            .collect::<BTreeSet<_>>();
        let previous = previous_member_result_set
            .iter()
            .filter(|member| member_is_visible(member))
            .cloned()
            .collect::<BTreeSet<_>>();
        let fact_is_visible = |fact: &ProgramFactEntry| match fact {
            ProgramFactEntry::ResultPayload(payload) => member_is_visible(&payload.member),
            _ => true,
        };
        let current_facts = settled_facts
            .into_iter()
            .filter(fact_is_visible)
            .collect::<BTreeSet<_>>();
        let previous_facts = previous_program_fact_set
            .iter()
            .filter(|fact| fact_is_visible(fact))
            .cloned()
            .collect::<BTreeSet<_>>();
        let program_fact_adds = current_facts
            .difference(&previous_facts)
            .cloned()
            .collect::<Vec<_>>();
        let program_fact_removes = previous_facts
            .difference(&current_facts)
            .cloned()
            .collect::<Vec<_>>();
        // A synthetic aggregate member is meaningful only together with its
        // payload fact. In particular, an empty aggregate has a member and a
        // payload whose aggregate field is `Nullable(None)`; it is not a
        // member with a missing payload. Carry both representations through
        // the settled-view handoff so facade materialization can retain that
        // distinction.
        let result_payload_adds = program_fact_adds
            .iter()
            .filter_map(|fact| match fact {
                ProgramFactEntry::ResultPayload(payload) => {
                    Some((payload.member.clone(), payload.clone()))
                }
                _ => None,
            })
            .collect();
        let result_payload_removes = program_fact_removes
            .iter()
            .filter_map(|fact| match fact {
                ProgramFactEntry::ResultPayload(payload) => Some(payload.member.clone()),
                _ => None,
            })
            .collect();
        Ok(Some(
            super::maintained_subscription_view::ResultTransitions {
                authoritative_membership_changed: false,
                authoritative_member_adds: BTreeSet::new(),
                adds: current.difference(&previous).cloned().collect(),
                removes: previous.difference(&current).cloned().collect(),
                result_payload_adds,
                result_payload_removes,
                program_fact_adds,
                program_fact_removes,
                allow_storage_witness_fallback: true,
                observed_result_delta_batches: 0,
                requires_authoritative_membership_reconcile: false,
                terminal_operations: Vec::new(),
            },
        ))
    }

    pub(crate) async fn authoritative_reset_snapshot_for_authority_result(
        &mut self,
        shape: &ValidatedQuery,
        authority_result_key: &AuthorityResultKey,
    ) -> Result<Option<RelationSnapshot>, Error> {
        let Some(authority_result) = self.query.authority_results.get(authority_result_key) else {
            return Ok(None);
        };
        let result_members = authority_result.settled_result_set.clone();
        let program_facts = authority_result.settled_program_facts.clone();
        let result_payloads = program_facts
            .iter()
            .filter_map(|fact| match fact {
                ProgramFactEntry::ResultPayload(payload) => {
                    Some((payload.member.clone(), payload.clone()))
                }
                _ => None,
            })
            .collect::<BTreeMap<_, _>>();

        let result_table = shape.query().table.as_str();
        let mut rows = Vec::new();
        let mut row_keys = BTreeSet::new();
        for member in result_members.iter().filter(|member| {
            is_public_result_member(member, result_table, shape.query().aggregate.is_some())
        }) {
            let Some(row) = self
                .materialize_authoritative_reset_member(shape.query(), member, &result_payloads)
                .await?
            else {
                continue;
            };
            row_keys.insert((row.table().to_owned(), row.row_uuid()));
            rows.push(row);
        }
        // Result-member ordering is for identity and deduplication, not public
        // query rank. Membership/windowing is already lowered; only restore the
        // selected roots to their advertised order before sending a reset.
        self.apply_query_order_in_schema(shape.query(), shape.schema_version(), &mut rows)?;
        if shape.query().flat_join.is_none() {
            self.apply_projection_in_schema(shape.query(), shape.schema_version(), &mut rows)?;
        }
        let root_count = rows.len();
        let mut edges = Vec::new();
        for fact in program_facts {
            let ProgramFactEntry::RelationEdge(edge) = fact else {
                continue;
            };
            // Program facts retain canonical authored identity.  The public
            // relation snapshot, including its removal index, is keyed in the
            // subscription read schema; project the edge identity alongside
            // the row it references rather than mixing canonical `users`
            // with a materialized `people` row.
            let read_edge = self
                .project_relation_edge_through_read_schema(&edge, shape.schema_version())
                .await?;
            if row_keys.insert((read_edge.target_table.clone(), read_edge.target_row))
                && let Some(version) = &edge.target_version
                && let Some(row) = self
                    .materialize_authoritative_reset_relation_edge_target(
                        shape.schema_version(),
                        edge.target_table.as_str(),
                        edge.target_row,
                        version,
                    )
                    .await?
            {
                rows.push(row);
            }
            edges.push(read_edge);
        }
        Ok(Some(RelationSnapshot {
            root_count,
            rows,
            edges,
        }))
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
            let _ = self.load_known_state_fact(binding_view_key).await?;
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
        if let Some(registered) = self
            .query
            .registered_bindings
            .get(&subscription.shape_id)
            .and_then(|bindings| bindings.get(&(subscription.binding_id, subscription.read_view)))
        {
            return Ok(registered.binding_view_key);
        }
        if let Some(binding_view_key) = self.canonical_whole_table_binding_view_key(subscription)? {
            return Ok(binding_view_key);
        }
        Err(Error::InvalidStoredValue(
            "subscription referenced unregistered binding",
        ))
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
