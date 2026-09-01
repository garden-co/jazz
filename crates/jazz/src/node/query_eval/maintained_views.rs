//! Local maintained-view state, reconciliation, resets, and terminal deltas.
//!
//! Subscription control-plane routing lives in the subscriptions module; this
//! module owns the live Groove subscription, authoritative membership state,
//! cached materialization inputs, and conversion of terminal changes into
//! public deltas.

use super::*;

pub(crate) struct LocalMaintainedViewSubscription {
    pub(super) subscription: MultisinkSubscription,
    pub(super) _retained_prepared_plan: Option<SubscriptionPreparedPlan>,
    pub(super) maintained: MaintainedSubscriptionView,
    pub(super) terminal_schemas: MaintainedTerminalSchemas,
    pub(super) tables: BTreeMap<String, TableSchema>,
    pub(super) result_query: JazzQuery,
    pub(super) result_table: String,
    pub(super) result_schema_version: SchemaVersionId,
    pub(super) binding_view_key: BindingViewKey,
    pub(super) result_select: Option<Vec<String>>,
    pub(super) result_set: BTreeSet<ResultMemberEntry>,
    pub(super) local_authority: LocalAuthorityReconciliation,
    pub(super) result_payloads: BTreeMap<ResultMemberEntry, ResultMemberPayloadEntry>,
    pub(super) program_facts: BTreeSet<ProgramFactEntry>,
    pub(super) root_occurrence_ids: Vec<OutputOccurrenceId>,
    pub(super) initial_received: bool,
}

impl LocalMaintainedViewSubscription {
    pub(crate) fn terminal_root_layout(&self) -> Option<&crate::db::TerminalRootLayout> {
        self.terminal_schemas.terminal_root_layout()
    }
}

/// A plan retained solely to keep a maintained subscription graph alive.
/// Its provenance is established by the compiler path that produced it, so a
/// caller cannot relabel a ClientLocal plan as TrustedServing after the fact.
pub(crate) struct SubscriptionPreparedPlan {
    pub(super) plan: PreparedQueryPlanHandle,
    pub(super) authorization_mode: QueryAuthorizationMode,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg(feature = "testing")]
pub(crate) struct LocalMaintainedViewSubscriptionFootprint {
    pub(crate) maintained: MaintainedSubscriptionViewFootprint,
    pub(crate) terminal_schemas: MaintainedTerminalSchemasFootprint,
    pub(crate) tables: usize,
    pub(crate) result_set: usize,
    pub(crate) result_payloads: usize,
    pub(crate) program_facts: usize,
    pub(crate) control_state_bytes: usize,
    pub(crate) total_heap_bytes: usize,
}

impl LocalMaintainedViewSubscription {
    pub(crate) fn subscription_id(&self) -> groove::ivm::SubscriptionId {
        self.subscription.id()
    }

    pub(crate) fn root_occurrence_ids(&self) -> &[OutputOccurrenceId] {
        &self.root_occurrence_ids
    }

    #[cfg(test)]
    pub(crate) fn retained_plan_authorization_mode(&self) -> Option<QueryAuthorizationMode> {
        self._retained_prepared_plan
            .as_ref()
            .map(|plan| plan.authorization_mode)
    }

    #[cfg(feature = "testing")]
    pub(crate) fn footprint(&self) -> LocalMaintainedViewSubscriptionFootprint {
        let maintained = self.maintained.footprint();
        let terminal_schemas = self.terminal_schemas.footprint();
        let tables_bytes = self
            .tables
            .iter()
            .map(|(name, schema)| name.len() + std::mem::size_of_val(schema))
            .sum::<usize>()
            + self.tables.len() * 96;
        let result_set_bytes = self
            .result_set
            .iter()
            .map(|member| {
                postcard::to_allocvec(member)
                    .map(|bytes| bytes.len())
                    .unwrap_or(0)
            })
            .sum::<usize>()
            + self.result_set.len() * 64;
        let result_payloads_bytes = self
            .result_payloads
            .iter()
            .map(|(member, payload)| {
                postcard::to_allocvec(member)
                    .map(|bytes| bytes.len())
                    .unwrap_or(0)
                    + postcard::to_allocvec(payload)
                        .map(|bytes| bytes.len())
                        .unwrap_or(0)
            })
            .sum::<usize>()
            + self.result_payloads.len() * 96;
        let program_facts_bytes = self
            .program_facts
            .iter()
            .map(|fact| {
                postcard::to_allocvec(fact)
                    .map(|bytes| bytes.len())
                    .unwrap_or(0)
            })
            .sum::<usize>()
            + self.program_facts.len() * 64;
        let deferred_authoritative_row_keys_bytes = self
            .local_authority
            .deferred_row_keys()
            .iter()
            .map(|(table, _)| table.len() + std::mem::size_of::<RowUuid>())
            .sum::<usize>()
            + self.local_authority.deferred_row_keys().len() * 64;
        let control_state_bytes = terminal_schemas.terminal_schemas_bytes
            + tables_bytes
            + self.result_table.len()
            + self
                .result_select
                .as_ref()
                .map(|columns| columns.iter().map(String::len).sum::<usize>())
                .unwrap_or_default()
            + result_set_bytes
            + result_payloads_bytes
            + program_facts_bytes
            + deferred_authoritative_row_keys_bytes;
        LocalMaintainedViewSubscriptionFootprint {
            maintained,
            terminal_schemas,
            tables: self.tables.len(),
            result_set: self.result_set.len(),
            result_payloads: self.result_payloads.len(),
            program_facts: self.program_facts.len(),
            control_state_bytes,
            total_heap_bytes: maintained.total_heap_bytes + control_state_bytes,
        }
    }
}

pub(crate) enum LocalMaintainedViewSubscriptionUpdate {
    /// Flat membership owns public occurrence rows. Groove root operations
    /// only order the root groups those occurrences belong to.
    Flat {
        authoritative_membership_changed: bool,
        added: Vec<(OutputOccurrenceId, CurrentRow)>,
        removed: Vec<OutputOccurrenceId>,
        terminal_operations: Vec<groove::ivm::TerminalOperation>,
    },
    /// Structured output is published directly from Groove's terminal tree.
    Structured {
        terminal_operations: Vec<groove::ivm::TerminalOperation>,
    },
}

fn result_member_matches_row_keys(
    member: &ResultMemberEntry,
    row_keys: &BTreeSet<(String, RowUuid)>,
) -> bool {
    result_member_matching_row_key(member, row_keys).is_some()
}

fn result_member_matching_row_key(
    member: &ResultMemberEntry,
    row_keys: &BTreeSet<(String, RowUuid)>,
) -> Option<(String, RowUuid)> {
    let row = member.as_real_row()?;
    let row_key = (row.table.to_string(), row.row_uuid);
    row_keys.contains(&row_key).then_some(row_key)
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    #[allow(dead_code)] // Test-only and feature-gated direct view callers keep the no-owner form.
    pub(crate) async fn open_maintained_view_subscription_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
        tier: DurabilityTier,
        read_view: &ReadViewSpec,
        retained_prepared_plan: Option<SubscriptionPreparedPlan>,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<(LocalMaintainedViewSubscription, RelationSnapshot), Error> {
        self.open_maintained_view_subscription_in_authorization_mode_with_waker(
            shape,
            binding,
            identity,
            tier,
            read_view,
            retained_prepared_plan,
            authorization_mode,
            None,
        )
        .await
    }

    /// Owner-loop variant that preserves a durable wake route while opening
    /// cold maintained subscription hydration.
    pub(crate) async fn open_maintained_view_subscription_in_authorization_mode_with_waker(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
        tier: DurabilityTier,
        read_view: &ReadViewSpec,
        retained_prepared_plan: Option<SubscriptionPreparedPlan>,
        authorization_mode: QueryAuthorizationMode,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<(LocalMaintainedViewSubscription, RelationSnapshot), Error> {
        if let Some(retained) = retained_prepared_plan.as_ref() {
            if retained.authorization_mode != authorization_mode {
                return Err(Error::InvalidStoredValue(
                    "maintained subscription retained a plan from another authorization mode",
                ));
            }
            debug_assert!(std::sync::Arc::strong_count(&retained.plan) > 0);
        }
        let settled_binding_view = (authorization_mode == QueryAuthorizationMode::ClientLocal)
            .then(|| {
                self.client_settled_binding_view_key_for_query(shape, binding, tier, read_view)
            })
            .flatten();
        let (subscription, maintained, terminal_schemas, transitions, tables, initial_received) =
            self.open_seeded_maintained_subscription_view_in_authorization_mode(
                shape,
                binding,
                identity,
                tier,
                read_view,
                authorization_mode,
                settled_binding_view,
                None,
                PreparedClaimBindingMode::Strict,
                progress_waker,
            )
            .await?;
        let mut local = LocalMaintainedViewSubscription {
            subscription,
            _retained_prepared_plan: retained_prepared_plan,
            maintained,
            terminal_schemas,
            tables,
            result_query: shape.query().clone(),
            result_table: shape.query().table.clone(),
            result_schema_version: shape.schema_version(),
            binding_view_key: settled_binding_view.unwrap_or_else(|| {
                BindingViewKey::new(
                    shape.shape_id(),
                    binding.binding_id(),
                    RegisterShapeOptions {
                        tier,
                        read_view: read_view.clone(),
                        ..RegisterShapeOptions::default()
                    }
                    .read_view_key(),
                )
            }),
            result_select: shape.query().select.clone(),
            result_set: BTreeSet::new(),
            local_authority: LocalAuthorityReconciliation::default(),
            result_payloads: BTreeMap::new(),
            program_facts: BTreeSet::new(),
            root_occurrence_ids: Vec::new(),
            initial_received,
        };
        let _initial_delta = self
            .apply_local_maintained_view_transitions(&mut local, transitions)
            .await?;
        let initial = self
            .materialize_local_maintained_relation_snapshot_with_occurrences(&local)
            .await?;
        local.root_occurrence_ids = initial.root_occurrence_ids;
        Ok((local, initial.snapshot))
    }

    #[allow(dead_code)] // Test-only direct callers use the no-owner form.
    pub(crate) async fn drain_local_maintained_view_subscription(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        authoritative_result_key: Option<AuthorityResultKey>,
    ) -> Result<Option<LocalMaintainedViewSubscriptionUpdate>, Error> {
        self.drain_local_maintained_view_subscription_with_waker(
            local,
            authoritative_result_key,
            None,
        )
        .await
    }

    pub(crate) async fn drain_local_maintained_view_subscription_with_waker(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        authoritative_result_key: Option<AuthorityResultKey>,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<Option<LocalMaintainedViewSubscriptionUpdate>, Error> {
        self.drain_local_maintained_view_subscription_preserving_rows_with_waker(
            local,
            authoritative_result_key,
            &BTreeSet::new(),
            progress_waker,
        )
        .await
        .map(|(update, _)| update)
    }

    #[allow(dead_code)] // Test-only direct callers use the no-owner form.
    pub(crate) async fn drain_local_maintained_view_subscription_preserving_rows(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        authoritative_result_key: Option<AuthorityResultKey>,
        preserved_row_keys: &BTreeSet<(String, RowUuid)>,
    ) -> Result<(Option<LocalMaintainedViewSubscriptionUpdate>, bool), Error> {
        self.drain_local_maintained_view_subscription_preserving_rows_with_waker(
            local,
            authoritative_result_key,
            preserved_row_keys,
            None,
        )
        .await
    }

    pub(crate) async fn drain_local_maintained_view_subscription_preserving_rows_with_waker(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        authoritative_result_key: Option<AuthorityResultKey>,
        preserved_row_keys: &BTreeSet<(String, RowUuid)>,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<(Option<LocalMaintainedViewSubscriptionUpdate>, bool), Error> {
        let (transitions, suppressed_authoritative_change) = self
            .drain_local_maintained_view_subscription_transitions(
                local,
                authoritative_result_key,
                preserved_row_keys,
                progress_waker,
            )
            .await?;
        let Some(transitions) = transitions else {
            return Ok((None, suppressed_authoritative_change));
        };
        let update = self
            .apply_local_maintained_view_transitions(local, transitions)
            .await?;
        Ok((Some(update), suppressed_authoritative_change))
    }

    #[allow(dead_code)] // Test-only direct callers use the no-owner form.
    pub(crate) async fn drain_local_maintained_view_subscription_state(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        authoritative_result_key: Option<AuthorityResultKey>,
    ) -> Result<bool, Error> {
        self.drain_local_maintained_view_subscription_state_with_waker(
            local,
            authoritative_result_key,
            None,
        )
        .await
    }

    pub(crate) async fn drain_local_maintained_view_subscription_state_with_waker(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        authoritative_result_key: Option<AuthorityResultKey>,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<bool, Error> {
        let (Some(transitions), _) = self
            .drain_local_maintained_view_subscription_transitions(
                local,
                authoritative_result_key,
                &BTreeSet::new(),
                progress_waker,
            )
            .await?
        else {
            return Ok(false);
        };
        let _ = self
            .apply_local_maintained_view_transitions_inner(local, transitions, false)
            .await?;
        Ok(true)
    }

    pub(crate) async fn reset_local_maintained_view_subscription_from_binding_view(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        authority_result_key: &AuthorityResultKey,
    ) -> Result<(), Error> {
        // Settled result sets can include support members used to maintain relations or
        // policies. The occurrence sidecar describes only public query roots, matching
        // the authoritative snapshot's `root_count`, so exclude those support members.
        local.result_set = self
            .query
            .authority_results
            .get(authority_result_key)
            .map(|state| &state.settled_result_set)
            .map(|members| {
                members
                    .iter()
                    .filter(|member| {
                        is_public_result_member(
                            member,
                            local.result_table.as_str(),
                            local.result_query.aggregate.is_some(),
                        )
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        local.local_authority.replace_source(
            authority_result_key.clone(),
            self.applied_authority_result_generation(authority_result_key),
        );
        local.program_facts = self
            .query
            .authority_results
            .get(authority_result_key)
            .map(|state| state.settled_program_facts.clone())
            .unwrap_or_default();
        if local.result_query.aggregate.is_some() {
            local
                .maintained
                .replace_aggregate_result_state(&local.result_set, &local.program_facts);
        }
        local.result_payloads = local
            .program_facts
            .iter()
            .filter_map(|fact| match fact {
                ProgramFactEntry::ResultPayload(payload)
                    if is_public_result_member(
                        &payload.member,
                        local.result_table.as_str(),
                        local.result_query.aggregate.is_some(),
                    ) =>
                {
                    Some((payload.member.clone(), payload.clone()))
                }
                _ => None,
            })
            .collect();
        // An authoritative reset replaces membership without flowing through
        // the ordinary local transition reducer. Rebuild the occurrence
        // sidecar from exactly that new state before the caller pairs it with
        // the reset snapshot; retaining the opening vector makes a later
        // reset fail its root-count invariant (or, worse, pair wrong roots).
        local.root_occurrence_ids = self
            .materialize_local_maintained_relation_snapshot_with_occurrences(local)
            .await?
            .root_occurrence_ids;
        Ok(())
    }

    pub(crate) fn seed_local_maintained_authoritative_generation(
        &self,
        local: &mut LocalMaintainedViewSubscription,
        authority_result_key: &AuthorityResultKey,
    ) {
        local.local_authority.replace_source(
            authority_result_key.clone(),
            self.applied_authority_result_generation(authority_result_key),
        );
    }

    pub(crate) fn defer_local_maintained_authority_reconciliation(
        &self,
        local: &mut LocalMaintainedViewSubscription,
    ) {
        local
            .local_authority
            .defer(local.local_authority.deferred_row_keys().clone());
    }

    pub(crate) fn local_maintained_authority_reconciliation_conflicts(
        &self,
        local: &LocalMaintainedViewSubscription,
        authority_result_key: &AuthorityResultKey,
        preserved_row_keys: &BTreeSet<(String, RowUuid)>,
    ) -> bool {
        let remote_members = self
            .query
            .authority_results
            .get(authority_result_key)
            .map(|state| state.settled_result_set.clone())
            .unwrap_or_default();
        local
            .result_set
            .symmetric_difference(&remote_members)
            .any(|member| result_member_matches_row_keys(member, preserved_row_keys))
    }

    pub(crate) fn local_maintained_authority_reconciliation_due(
        &self,
        local: &LocalMaintainedViewSubscription,
        authority_result_key: &AuthorityResultKey,
    ) -> bool {
        local.local_authority.is_due(
            authority_result_key,
            self.applied_authority_result_generation(authority_result_key),
        )
    }

    async fn drain_local_maintained_view_subscription_transitions(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        authoritative_result_key: Option<AuthorityResultKey>,
        preserved_row_keys: &BTreeSet<(String, RowUuid)>,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<
        (
            Option<super::maintained_subscription_view::ResultTransitions>,
            bool,
        ),
        Error,
    > {
        self.drive_ready_query_runtime_with_waker(progress_waker)
            .await?;
        if local.result_query.aggregate.is_some()
            && let Some(authority_result) =
                self.authority_result_state_for_binding_view(local.binding_view_key)
        {
            let remote_members = &authority_result.settled_result_set;
            let remote_facts = &authority_result.settled_program_facts;
            let visible_members = remote_members
                .iter()
                .filter(|member| {
                    is_public_result_member(
                        member,
                        local.result_table.as_str(),
                        local.result_query.aggregate.is_some(),
                    )
                })
                .cloned()
                .collect::<BTreeSet<_>>();
            let visible_facts = remote_facts
                .iter()
                .filter(|fact| match fact {
                    ProgramFactEntry::ResultPayload(payload) => is_public_result_member(
                        &payload.member,
                        local.result_table.as_str(),
                        local.result_query.aggregate.is_some(),
                    ),
                    _ => false,
                })
                .cloned()
                .collect::<BTreeSet<_>>();
            if visible_members != local.result_set || visible_facts != local.program_facts {
                let mut transitions = super::maintained_subscription_view::ResultTransitions {
                    adds: visible_members
                        .difference(&local.result_set)
                        .cloned()
                        .collect(),
                    removes: local
                        .result_set
                        .difference(&visible_members)
                        .cloned()
                        .collect(),
                    program_fact_adds: visible_facts
                        .difference(&local.program_facts)
                        .cloned()
                        .collect(),
                    program_fact_removes: local
                        .program_facts
                        .difference(&visible_facts)
                        .cloned()
                        .collect(),
                    ..Default::default()
                };
                transitions.result_payload_adds = transitions
                    .program_fact_adds
                    .iter()
                    .filter_map(|fact| match fact {
                        ProgramFactEntry::ResultPayload(payload) => {
                            Some((payload.member.clone(), payload.clone()))
                        }
                        _ => None,
                    })
                    .collect();
                transitions.result_payload_removes = transitions
                    .program_fact_removes
                    .iter()
                    .filter_map(|fact| match fact {
                        ProgramFactEntry::ResultPayload(payload) => Some(payload.member.clone()),
                        _ => None,
                    })
                    .collect();
                return Ok((Some(transitions), false));
            }
        }
        let mut states = BTreeMap::<ResultMemberEntry, (bool, bool)>::new();
        let mut payload_states = BTreeMap::<
            ResultMemberEntry,
            (
                Option<ResultMemberPayloadEntry>,
                Option<ResultMemberPayloadEntry>,
            ),
        >::new();
        let mut fact_states = BTreeMap::<ProgramFactEntry, (bool, bool)>::new();
        let mut terminal_operations = Vec::new();
        let mut authoritative_membership_changed = false;
        let mut authoritative_member_adds = BTreeSet::new();
        let mut suppressed_authoritative_change = false;
        let mut suppressed_authoritative_row_keys = BTreeSet::new();
        if let Some(ref authority_result_key) = authoritative_result_key {
            let authoritative_generation =
                self.applied_authority_result_generation(authority_result_key);
            // Local optimistic changes can advance the maintained graph
            // without any newer serving-peer membership decision. Keep them
            // visible until an authoritative generation advances.
            if local
                .local_authority
                .is_due(&authority_result_key, authoritative_generation)
            {
                let mut protected_row_keys = preserved_row_keys.clone();
                if authoritative_generation == local.local_authority.generation() {
                    protected_row_keys
                        .extend(local.local_authority.deferred_row_keys().iter().cloned());
                }
                let remote_members = self
                    .query
                    .authority_results
                    .get(&authority_result_key)
                    .map(|state| state.settled_result_set.clone())
                    .unwrap_or_default();
                let remote_payloads = self
                    .query
                    .authority_results
                    .get(&authority_result_key)
                    .into_iter()
                    .flat_map(|state| state.settled_program_facts.iter())
                    .filter_map(|fact| match fact {
                        ProgramFactEntry::ResultPayload(payload) => {
                            Some((payload.member.clone(), payload.clone()))
                        }
                        _ => None,
                    })
                    .collect::<BTreeMap<_, _>>();
                let remote_facts = self
                    .query
                    .authority_results
                    .get(&authority_result_key)
                    .map(|state| state.settled_program_facts.clone())
                    .unwrap_or_default();
                let mut candidate_reconciliation = local.local_authority.clone();
                if candidate_reconciliation.source() != Some(&authority_result_key) {
                    candidate_reconciliation
                        .replace_source(authority_result_key.clone(), authoritative_generation);
                }
                let exact_terminal_operations =
                    self.take_pending_terminal_operations(authority_result_key);
                let authority_delta = candidate_reconciliation
                    .reconcile(
                        authority_result_key,
                        authoritative_generation,
                        &local.result_set,
                        &local.program_facts,
                        remote_members,
                        remote_facts,
                        exact_terminal_operations,
                    )
                    .expect("the current exact authority source must reconcile");
                // The local maintained graph may intentionally be behind the
                // authority frontier (for example, an Edge-tier window over a
                // client-local database).  An authoritative ViewUpdate is not
                // merely a revocation signal: it is the current membership
                // decision for that frontier.  Import members newly admitted
                // there so a row promoted across a TopBy boundary is delivered
                // even though none of its locally-visible source facts changed.
                for entry in authority_delta.member_adds {
                    if let Some(row_key) =
                        result_member_matching_row_key(&entry, &protected_row_keys)
                    {
                        suppressed_authoritative_change = true;
                        suppressed_authoritative_row_keys.insert(row_key);
                        continue;
                    }
                    if !local.result_set.contains(&entry) {
                        let materializable = if remote_payloads.contains_key(&entry) {
                            true
                        } else if let Some(row) = self
                            .materialize_local_maintained_view_result_member(local, &entry)
                            .await?
                        {
                            let table = self.table(row.table())?;
                            current_row_has_required_subscription_cells(
                                &row,
                                table,
                                local.result_select.as_deref(),
                            )
                        } else {
                            false
                        };
                        // Authority membership can arrive before the admitted
                        // row's readable content bundle. Do not publish a
                        // synthetic placeholder root: the ordinary maintained
                        // source transition will add it once that content is
                        // locally materializable.
                        if !materializable {
                            continue;
                        }
                        authoritative_membership_changed = true;
                        authoritative_member_adds.insert(entry.clone());
                        states.insert(entry.clone(), (false, true));
                        if let Some(payload) = remote_payloads.get(&entry) {
                            payload_states.insert(
                                entry.clone(),
                                (
                                    local.result_payloads.get(&entry).cloned(),
                                    Some(payload.clone()),
                                ),
                            );
                        }
                    }
                }
                for entry in authority_delta.member_removes {
                    if let Some(row_key) =
                        result_member_matching_row_key(&entry, &protected_row_keys)
                    {
                        suppressed_authoritative_change = true;
                        suppressed_authoritative_row_keys.insert(row_key);
                        continue;
                    }
                    // Replace the exact prior member even when its output
                    // occurrence remains visible through a new content
                    // version. The snapshot reducer coalesces the matching
                    // occurrence add/remove into one replacement.
                    if local.result_set.contains(&entry) {
                        authoritative_membership_changed = true;
                        states.insert(entry.clone(), (true, false));
                        if local.result_payloads.contains_key(&entry) {
                            payload_states.insert(
                                entry.clone(),
                                (local.result_payloads.get(&entry).cloned(), None),
                            );
                        }
                    }
                }
                for fact in authority_delta.fact_adds {
                    let before = local.program_facts.contains(&fact);
                    fact_states.insert(fact, (before, true));
                }
                for fact in authority_delta.fact_removes {
                    let before = local.program_facts.contains(&fact);
                    fact_states.insert(fact, (before, false));
                }
                terminal_operations.extend(authority_delta.terminal_operations);
                if suppressed_authoritative_change {
                    local
                        .local_authority
                        .defer(suppressed_authoritative_row_keys);
                } else {
                    local.local_authority = candidate_reconciliation;
                }
            }
        }
        loop {
            match local.subscription.try_recv() {
                Ok(deltas) => {
                    local.initial_received = true;
                    let transitions = local.maintained.apply_multisink_deltas(
                        deltas,
                        &local.terminal_schemas,
                        &local.tables,
                        &self.node_aliases,
                    )?;
                    terminal_operations.extend(transitions.terminal_operations);
                    for entry in transitions.adds {
                        let before = local.result_set.contains(&entry);
                        states
                            .entry(entry)
                            .and_modify(|(_, after)| *after = true)
                            .or_insert((before, true));
                    }
                    for entry in transitions.removes {
                        let before = local.result_set.contains(&entry);
                        states
                            .entry(entry)
                            .and_modify(|(_, after)| *after = false)
                            .or_insert((before, false));
                    }
                    for member in transitions.result_payload_removes {
                        let before = local.result_payloads.get(&member).cloned();
                        payload_states
                            .entry(member)
                            .and_modify(|(_, after)| *after = None)
                            .or_insert((before, None));
                    }
                    for (member, payload) in transitions.result_payload_adds {
                        let before = local.result_payloads.get(&member).cloned();
                        payload_states
                            .entry(member)
                            .and_modify(|(_, after)| *after = Some(payload.clone()))
                            .or_insert((before, Some(payload)));
                    }
                    for fact in transitions.program_fact_adds {
                        let before = local.program_facts.contains(&fact);
                        fact_states
                            .entry(fact)
                            .and_modify(|(_, after)| *after = true)
                            .or_insert((before, true));
                    }
                    for fact in transitions.program_fact_removes {
                        let before = local.program_facts.contains(&fact);
                        fact_states
                            .entry(fact)
                            .and_modify(|(_, after)| *after = false)
                            .or_insert((before, false));
                    }
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    return Err(Error::SubscriptionClosed);
                }
            }
        }
        if states.is_empty()
            && payload_states.is_empty()
            && fact_states.is_empty()
            && terminal_operations.is_empty()
        {
            return Ok((None, suppressed_authoritative_change));
        }
        let mut transitions = super::maintained_subscription_view::ResultTransitions {
            authoritative_membership_changed,
            authoritative_member_adds,
            terminal_operations,
            ..Default::default()
        };
        for (entry, (before, after)) in states {
            match (before, after) {
                (false, true) => transitions.adds.push(entry),
                (true, false) => transitions.removes.push(entry),
                _ => {}
            }
        }
        for (member, (before, after)) in payload_states {
            match (before, after) {
                (None, Some(payload)) => transitions.result_payload_adds.push((member, payload)),
                (Some(_), None) => transitions.result_payload_removes.push(member),
                (Some(before), Some(after)) if before != after => {
                    transitions.result_payload_removes.push(member.clone());
                    transitions.result_payload_adds.push((member, after));
                }
                _ => {}
            }
        }
        for (fact, (before, after)) in fact_states {
            match (before, after) {
                (false, true) => transitions.program_fact_adds.push(fact),
                (true, false) => transitions.program_fact_removes.push(fact),
                _ => {}
            }
        }
        // Aggregate facts are the payload vocabulary for synthetic result
        // members. Preserve their current values alongside membership while
        // coalescing a multisink batch; otherwise a present NULL or a revised
        // aggregate can be mistaken for an absent payload at materialization.
        if local.result_query.aggregate.is_some() {
            transitions.result_payload_adds = transitions
                .program_fact_adds
                .iter()
                .filter_map(|fact| match fact {
                    ProgramFactEntry::ResultPayload(payload) => {
                        Some((payload.member.clone(), payload.clone()))
                    }
                    _ => None,
                })
                .collect();
            transitions.result_payload_removes = transitions
                .program_fact_removes
                .iter()
                .filter_map(|fact| match fact {
                    ProgramFactEntry::ResultPayload(payload) => Some(payload.member.clone()),
                    _ => None,
                })
                .collect();
        }
        Ok((Some(transitions), suppressed_authoritative_change))
    }

    async fn apply_local_maintained_view_transitions(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        transitions: super::maintained_subscription_view::ResultTransitions,
    ) -> Result<LocalMaintainedViewSubscriptionUpdate, Error> {
        self.apply_local_maintained_view_transitions_inner(local, transitions, true)
            .await
    }

    async fn apply_local_maintained_view_transitions_inner(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        transitions: super::maintained_subscription_view::ResultTransitions,
        materialize_update: bool,
    ) -> Result<LocalMaintainedViewSubscriptionUpdate, Error> {
        let structured_output = !local.result_query.array_subqueries.is_empty();
        let authoritative_membership_changed = transitions.authoritative_membership_changed;
        let authoritative_member_adds = transitions.authoritative_member_adds;
        let terminal_operations = transitions.terminal_operations.clone();
        let aggregate_replacements = transitions
            .adds
            .iter()
            .filter(|member| {
                is_public_aggregate_result_member(
                    member,
                    local.result_table.as_str(),
                    local.result_query.aggregate.is_some(),
                )
            })
            .map(aggregate_result_member_row_uuid)
            .collect::<Result<BTreeSet<_>, _>>()?;
        let mut added = Vec::new();
        let mut removed = Vec::new();
        for member in transitions.result_payload_removes {
            local.result_payloads.remove(&member);
        }
        for (member, payload) in transitions.result_payload_adds {
            if is_public_result_member(
                &member,
                local.result_table.as_str(),
                local.result_query.aggregate.is_some(),
            ) {
                local.result_payloads.insert(member, payload);
            }
        }
        for member in transitions.adds {
            if !is_public_result_member(
                &member,
                local.result_table.as_str(),
                local.result_query.aggregate.is_some(),
            ) {
                continue;
            }
            // Authority-scope re-entry can surface a new internal result
            // member for an occurrence the public facade already tracks. In
            // that case replace the stale member so the current scope wins.
            // Ordinary content updates keep their member identity: replacing
            // them here turns an update into a duplicate add and leaves the
            // public subscription with the old payload.
            replace_stale_authoritative_occurrence_member(
                &mut local.result_set,
                &mut local.result_payloads,
                &authoritative_member_adds,
                &member,
                local.result_table.as_str(),
                local.result_query.aggregate.is_some(),
            )?;
            if local.result_set.insert(member.clone()) && materialize_update && !structured_output {
                if let Some(row) = self
                    .materialize_local_maintained_view_result_member(local, &member)
                    .await?
                    && let Some(occurrence_id) = public_result_member_occurrence_id(
                        &member,
                        local.result_table.as_str(),
                        local.result_query.aggregate.is_some(),
                    )?
                {
                    added.push((occurrence_id, row));
                }
            }
        }
        for member in transitions.removes {
            if !is_public_result_member(
                &member,
                local.result_table.as_str(),
                local.result_query.aggregate.is_some(),
            ) {
                continue;
            }
            if local.result_set.remove(&member) {
                if materialize_update && !structured_output {
                    if let Some(occurrence_id) = member.output_occurrence_id() {
                        removed.push(occurrence_id);
                    } else if is_public_aggregate_result_member(
                        &member,
                        local.result_table.as_str(),
                        local.result_query.aggregate.is_some(),
                    ) {
                        let row_uuid = aggregate_result_member_row_uuid(&member)?;
                        let replacement_is_current = local.result_set.iter().any(|candidate| {
                            is_public_aggregate_result_member(
                                candidate,
                                local.result_table.as_str(),
                                local.result_query.aggregate.is_some(),
                            ) && aggregate_result_member_row_uuid(candidate)
                                .is_ok_and(|candidate_uuid| candidate_uuid == row_uuid)
                        });
                        if !aggregate_replacements.contains(&row_uuid) && !replacement_is_current {
                            removed.push(OutputOccurrenceId::single_source(ObjectId::from_uuid(
                                row_uuid.0,
                            )));
                        }
                    }
                }
            }
        }
        for fact in transitions.program_fact_removes {
            local.program_facts.remove(&fact);
        }
        for fact in transitions.program_fact_adds {
            local.program_facts.insert(fact);
        }
        Ok(if structured_output {
            LocalMaintainedViewSubscriptionUpdate::Structured {
                terminal_operations,
            }
        } else {
            LocalMaintainedViewSubscriptionUpdate::Flat {
                authoritative_membership_changed,
                added,
                removed,
                terminal_operations,
            }
        })
    }
}
