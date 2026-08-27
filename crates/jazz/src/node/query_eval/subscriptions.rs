//! Subscription registration, routing, known-state, and reset lifecycle.
//!
//! This module owns the control-plane state that maps shapes and bindings to
//! durable view identities. Query compilation, maintained terminal reduction,
//! and row materialization remain separate stages.

use super::*;

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(in crate::node) fn register_shape(
        &mut self,
        shape_id: ShapeId,
        ast: ShapeAst,
    ) -> Result<(), Error> {
        if ast.version != ShapeAst::VERSION {
            return Err(Error::InvalidStoredValue("unsupported query AST version"));
        }
        let schema = if ast.schema_version == self.catalogue.current_schema_version_id {
            &self.catalogue.schema
        } else {
            let Some(schema) = self.catalogue.catalogue_schemas.get(&ast.schema_version) else {
                self.sync_metrics.parked_catalogue_shapes += 1;
                self.parking
                    .parked_shape_registrations
                    .insert(shape_id, ast);
                return Ok(());
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
        self.query.registered_shapes.insert(shape_id, shape);
        self.drain_parked_binding_deltas_for_shape(shape_id)?;
        Ok(())
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
            self.parking.parked_shape_registrations.remove(&shape_id);
            self.sync_metrics.parked_catalogue_shapes_resolved += 1;
            self.register_shape(shape_id, ast)?;
        }
        Ok(())
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

    pub(crate) fn register_query_subscription_for_peer(
        &mut self,
        shape_id: ShapeId,
        ast: ShapeAst,
        subscribe: Subscribe,
    ) -> Result<(), Error> {
        self.register_shape(shape_id, ast)?;
        self.apply_subscribe(subscribe)
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
        // A new wire subscription needs an authority receipt. Discard a
        // browser-only materialized-window interpretation before any opening
        // reset can otherwise preserve its old member set.
        if self
            .query
            .local_materialized_window_binding_views
            .remove(&binding_view_key)
        {
            self.clear_settled_result_view(binding_view_key);
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
                },
            );
        Ok(())
    }

    pub(crate) fn apply_unsubscribe(&mut self, subscription: SubscriptionKey) {
        let binding_view_key = self.binding_view_key_for_subscription(subscription).ok();
        let retain_local_materialized_window = binding_view_key.is_some_and(|binding_view_key| {
            self.authored_commit_durability == DurabilityTier::None
                && self
                    .query
                    .registered_shapes
                    .get(&subscription.shape_id)
                    .is_some_and(|shape| shape.query().offset != 0)
                && self
                    .query
                    .settled_result_sets
                    .contains_key(&binding_view_key)
        });
        if let Some(bindings) = self
            .query
            .registered_bindings
            .get_mut(&subscription.shape_id)
        {
            bindings.remove(&(subscription.binding_id, subscription.read_view));
        }
        if let Some(binding_view_key) = binding_view_key
            && !self.registered_binding_resolves_to_binding_view_key(binding_view_key)
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
                self.clear_settled_result_view(binding_view_key);
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

    #[cfg(feature = "testing")]
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
                .settled_result_sets
                .keys()
                .filter(|key| {
                    !self
                        .query
                        .local_materialized_window_binding_views
                        .contains(key)
                })
                .count(),
            self.query.settled_program_facts.len(),
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

    pub(crate) fn has_settled_result_set(&self, binding_view_key: BindingViewKey) -> bool {
        self.query
            .settled_result_sets
            .contains_key(&binding_view_key)
            && !self
                .query
                .local_materialized_window_binding_views
                .contains(&binding_view_key)
    }

    pub(crate) fn applied_view_update_generation(&self, binding_view_key: BindingViewKey) -> u64 {
        self.query
            .applied_view_update_generations
            .get(&binding_view_key)
            .copied()
            .unwrap_or_default()
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
        self.clear_settled_result_view(binding_view_key);
        for member in members {
            self.insert_settled_result_member_indexed(binding_view_key, member);
        }
        self.query
            .settled_through_by_binding_view
            .insert(binding_view_key, settled_through);
        self.query
            .pending_authoritative_reset_binding_views
            .insert(binding_view_key);
    }

    pub(crate) fn take_pending_authoritative_reset_binding_views(
        &mut self,
    ) -> BTreeSet<BindingViewKey> {
        std::mem::take(&mut self.query.pending_authoritative_reset_binding_views)
    }

    pub(crate) fn has_pending_authoritative_reset(&self) -> bool {
        !self
            .query
            .pending_authoritative_reset_binding_views
            .is_empty()
    }

    pub(crate) fn take_pending_terminal_operations(
        &mut self,
        binding_view_key: BindingViewKey,
    ) -> Vec<groove::ivm::TerminalOperation> {
        self.query
            .pending_terminal_operations_by_binding_view
            .remove(&binding_view_key)
            .unwrap_or_default()
    }

    pub(crate) fn defer_authoritative_reset_for_binding_view(
        &mut self,
        binding_view_key: BindingViewKey,
    ) {
        self.query
            .pending_authoritative_reset_binding_views
            .insert(binding_view_key);
    }

    #[cfg(test)]
    pub(crate) fn has_pending_authoritative_reset_for_test(
        &self,
        binding_view_key: BindingViewKey,
    ) -> bool {
        self.query
            .pending_authoritative_reset_binding_views
            .contains(&binding_view_key)
    }

    pub(crate) fn publication_deferred_for_binding_view(
        &self,
        binding_view_key: BindingViewKey,
    ) -> bool {
        self.query
            .deferred_publication_binding_views
            .contains(&binding_view_key)
    }

    pub(crate) fn opening_pending_for_binding_view(
        &self,
        binding_view_key: BindingViewKey,
    ) -> bool {
        self.query
            .pending_opening_binding_views
            .contains(&binding_view_key)
    }

    pub(crate) fn settled_result_transitions_for_subscription(
        &self,
        subscription: SubscriptionKey,
        source_binding_view: Option<BindingViewKey>,
        previous_member_result_set: &BTreeSet<ResultMemberEntry>,
        previous_program_fact_set: &BTreeSet<ProgramFactEntry>,
        result_table_filter: Option<&str>,
        output_tables: &BTreeMap<String, TableSchema>,
    ) -> Result<Option<super::maintained_subscription_view::ResultTransitions>, Error> {
        let binding_view_key = source_binding_view
            .map(Ok)
            .unwrap_or_else(|| self.binding_view_key_for_subscription(subscription))?;
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
        let Some(settled_members) = self.query.settled_result_sets.get(&binding_view_key) else {
            return Ok(None);
        };
        let settled_facts = self
            .query
            .settled_program_facts
            .get(&binding_view_key)
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

    pub(crate) async fn authoritative_reset_snapshot_for_binding_view(
        &mut self,
        shape: &ValidatedQuery,
        binding_view_key: BindingViewKey,
    ) -> Result<Option<RelationSnapshot>, Error> {
        let Some(result_members) = self
            .query
            .settled_result_sets
            .get(&binding_view_key)
            .cloned()
        else {
            return Ok(None);
        };
        let program_facts = self
            .query
            .settled_program_facts
            .get(&binding_view_key)
            .cloned()
            .unwrap_or_default();
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

    pub(crate) fn settled_through_for_binding_view(
        &self,
        binding_view_key: BindingViewKey,
    ) -> Option<GlobalTime> {
        self.query
            .settled_through_by_binding_view
            .get(&binding_view_key)
            .copied()
    }

    pub(crate) async fn known_state_declaration_for_subscription(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        values: &[Value],
        identity: AuthorSubject,
    ) -> Result<Option<KnownStateDeclaration>, Error> {
        let binding_view_key = BindingViewKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: subscription.read_view,
        };
        if !self.has_settled_result_set(binding_view_key) {
            let _ = self.load_known_state_fact(binding_view_key).await?;
            // Slow exact declarations are still known-state declarations: they
            // must describe a binding view the server has previously settled
            // for this client. A purely local first subscription could include
            // rows the serving peer has not observed yet; truncating that to an
            // exact set would silently overclaim and can make stale rehydrate
            // responses suppress local live state.
            return Ok(None);
        }
        if let Some(position) = self.settled_through_for_binding_view(binding_view_key) {
            let authorization_progress = self
                .query
                .authorization_progress_by_binding_view
                .get(&binding_view_key)
                .copied();
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
        if let Some(position) = self.load_known_state_fact(binding_view_key).await? {
            return Ok(Some(KnownStateDeclaration::Fast {
                completeness: KnownStateCompleteness::FastCurrentMembership,
                position,
            }));
        }
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
        let binding_view_key = match self.binding_view_key_for_subscription(subscription) {
            Ok(binding_view_key) => binding_view_key,
            Err(Error::InvalidStoredValue(
                "subscription referenced unregistered shape"
                | "subscription referenced unregistered binding",
            )) => return Ok(false),
            Err(error) => return Err(error),
        };
        Ok(self
            .query
            .known_state_declared_binding_views
            .contains(&binding_view_key))
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
