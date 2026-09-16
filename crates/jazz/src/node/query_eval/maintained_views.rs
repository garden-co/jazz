//! Local maintained-view state, reconciliation, resets, and terminal deltas.
//!
//! Subscription control-plane routing lives in the subscriptions module; this
//! module owns the live Groove subscription, authoritative membership state,
//! cached materialization inputs, and conversion of terminal changes into
//! public deltas.

use super::*;
use crate::ids::GlobalPhysicalTableId;
use crate::protocol::SupportingRow;

pub(crate) struct LocalMaintainedViewSubscription {
    pub(super) subscription: MultisinkSubscription,
    pub(super) _retained_prepared_plan: Option<SubscriptionPreparedPlan>,
    pub(super) maintained: MaintainedSubscriptionView,
    pub(super) terminal_schemas: MaintainedTerminalSchemas,
    pub(super) tables: BTreeMap<String, TableSchema>,
    pub(super) result_query: JazzQuery,
    pub(super) result_table: String,
    pub(super) result_schema_version: SchemaVersionId,
    pub(super) result_select: Option<Vec<String>>,
    pub(super) result_relation_projection: Option<Vec<crate::query::RelationProjectColumn>>,
    pub(super) result_relation_projections:
        Option<BTreeMap<String, Vec<crate::query::RelationProjectColumn>>>,
    pub(super) result_set: BTreeSet<ResultMemberEntry>,
    pub(super) result_payloads: BTreeMap<ResultMemberEntry, ResultMemberPayloadEntry>,
    pub(super) program_facts: BTreeSet<ProgramFactEntry>,
    pub(super) root_occurrence_ids: Vec<OutputOccurrenceId>,
    pub(super) initial_received: bool,
    /// The one receiver-owned source frontier feeding this local graph.
    pub(super) covered_input_receiver: CoveredInputReceiver,
}

#[derive(Clone, Debug)]
pub(crate) struct CoveredInputSource {
    pub(super) id: InputSourceId,
    pub(super) table: groove::Intern<String>,
    pub(super) descriptor: RecordDescriptor,
}

/// Runtime-local state for the exact, policy-scoped source closure installed
/// into one receiver graph. It deliberately contains no authority output:
/// the graph derives its own terminal from these inputs.
#[derive(Debug, Default)]
pub(crate) struct CoveredInputReceiver {
    /// One runtime dataset per physical table in this receiver's read schema.
    pub(crate) sources: BTreeMap<GlobalPhysicalTableId, CoveredInputSource>,
    pub(crate) read_view: ReadViewSpec,
    local_authority: LocalAuthorityReconciliation,
    /// `None` is pending, never an implicit empty closure.
    installed_closure: Option<(AuthorityResultKey, u64)>,
    /// Last exact source-closure receipt applied to the runtime inputs. This
    /// is intentionally unrelated to generic ViewUpdate sequencing: frames
    /// with no source change do not advance it.
    installed_generation: Option<u64>,
    /// Exact content facts currently materialized in each runtime input. A
    /// deletion-layer fact participates in closure provenance but deliberately
    /// has no source tuple, so it is not retained here.
    installed_records: BTreeMap<GlobalPhysicalTableId, BTreeMap<SupportingRow, Vec<u8>>>,
}

impl CoveredInputReceiver {
    pub(crate) fn new(
        sources: BTreeMap<GlobalPhysicalTableId, CoveredInputSource>,
        read_view: ReadViewSpec,
    ) -> Self {
        Self {
            sources,
            read_view,
            ..Default::default()
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }
}

impl LocalMaintainedViewSubscription {
    /// Seed a facade's retained decoder at its opening boundary. Copy the
    /// compiler-addressed collections already decoded by the local terminal;
    /// do not infer collection semantics from arbitrary array-valued columns.
    pub(crate) fn decoded_terminal_records(
        &self,
    ) -> Result<
        BTreeMap<OutputOccurrenceId, crate::db::terminal_record::TerminalRecordState>,
        crate::db::Error,
    > {
        self.maintained
            .decoded_terminal_records()
            .iter()
            .map(|(key, record)| {
                Ok((
                    crate::db::terminal_root_occurrence_id_with_root_union(
                        key,
                        self.terminal_root_layout()
                            .is_some_and(|layout| layout.root_union_arm),
                    )?,
                    record.clone(),
                ))
            })
            .collect()
    }

    pub(crate) fn terminal_root_layout(&self) -> Option<&crate::db::TerminalRootLayout> {
        self.terminal_schemas.terminal_root_layout()
    }

    pub(crate) fn has_root_collector(&self) -> bool {
        self.terminal_schemas.has_root_collector()
    }

    /// Whether this graph has consumed its first local terminal
    /// batch. Before this point an empty materialization is pending, not an
    /// authorization result.
    pub(crate) fn initial_snapshot_received(&self) -> bool {
        self.initial_received
    }

    pub(super) fn relation_projection_for_member(
        &self,
        member: &ResultMemberEntry,
    ) -> Result<Option<&Vec<crate::query::RelationProjectColumn>>, Error> {
        let occurrence = super::public_result_member_occurrence_id(
            member,
            self.result_table.as_str(),
            self.result_query.aggregate.is_some(),
        )?
        .ok_or(Error::InvalidStoredValue(
            "maintained union member has no occurrence identity",
        ))?;
        self.relation_projection_for_occurrence(&occurrence)
    }

    pub(super) fn relation_projection_for_occurrence(
        &self,
        occurrence: &OutputOccurrenceId,
    ) -> Result<Option<&Vec<crate::query::RelationProjectColumn>>, Error> {
        let Some(projections) = &self.result_relation_projections else {
            return Ok(self.result_relation_projection.as_ref());
        };
        let label = occurrence
            .union_arms()
            .iter()
            .find_map(|(position, label)| (*position == 0).then_some(label))
            .ok_or(Error::InvalidStoredValue(
                "maintained union member has no position-0 arm label",
            ))?;
        projections
            .get(label)
            .map(Some)
            .ok_or(Error::InvalidStoredValue(
                "maintained union member has an unknown arm label",
            ))
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
    /// Whether a storage publication can affect this maintained view's inputs.
    pub(crate) fn uses_logical_tables(
        &self,
        changed_logical_tables: &std::collections::HashSet<String>,
    ) -> bool {
        self.maintained.targeted_refresh_uncertain
            || self
                .maintained
                .targeted_refresh_tables
                .iter()
                .any(|table| changed_logical_tables.contains(table))
    }

    pub(crate) fn root_occurrence_ids(&self) -> &[OutputOccurrenceId] {
        &self.root_occurrence_ids
    }

    pub(crate) fn has_covered_input_sources(&self) -> bool {
        !self.covered_input_receiver.is_empty()
    }

    pub(crate) fn has_installed_covered_closure(
        &self,
        authority_result_key: &AuthorityResultKey,
        generation: u64,
    ) -> bool {
        self.covered_input_receiver
            .installed_closure
            .as_ref()
            .is_some_and(|(source, installed_generation)| {
                source == authority_result_key && *installed_generation == generation
            })
    }

    #[cfg(test)]
    pub(crate) fn compiled_authorization_mode(&self) -> Option<QueryAuthorizationMode> {
        self.maintained.compiled_authorization_mode
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
        let control_state_bytes = terminal_schemas.terminal_schemas_bytes
            + tables_bytes
            + self.result_table.len()
            + self
                .result_select
                .as_ref()
                .map(|columns| columns.iter().map(String::len).sum::<usize>())
                .unwrap_or_default()
            + self
                .result_relation_projection
                .as_ref()
                .map(|columns| {
                    postcard::to_allocvec(columns)
                        .map(|bytes| bytes.len())
                        .unwrap_or_default()
                })
                .unwrap_or_default()
            + result_set_bytes
            + result_payloads_bytes
            + self
                .result_relation_projections
                .as_ref()
                .map(|projections| {
                    postcard::to_allocvec(projections)
                        .map(|bytes| bytes.len())
                        .unwrap_or_default()
                })
                .unwrap_or_default()
            + program_facts_bytes;
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
        added: Vec<(OutputOccurrenceId, CurrentRow)>,
        removed: Vec<OutputOccurrenceId>,
    },
    /// Aggregate terminals retain every group, while the public facade shows
    /// an ordered window of those groups. Re-materialize that complete facade
    /// after each transition so displaced page members are retracted too.
    AggregateWindow {
        snapshot: RelationSnapshot,
        occurrence_ids: Vec<OutputOccurrenceId>,
    },
    /// Structured output is published directly from Groove's terminal tree.
    Structured {
        terminal_operations: Vec<groove::ivm::TerminalOperation>,
    },
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
            false,
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
        pending_overlay: bool,
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
        // A newly attached local receiver must immediately consume an
        // already-settled *exact* authority closure.  The binding-view key is
        // only routing; resolve its unique scoped receipt here rather than
        // opening an empty receiver and letting facade refresh infer output
        // from a sibling/authority result.
        let settled_authority_result_key = settled_binding_view.and_then(|binding_view| {
            self.unique_authority_result_key_for_binding_view(binding_view)
        });
        let (
            subscription,
            maintained,
            terminal_schemas,
            transitions,
            tables,
            initial_received,
            covered_input_sources,
        ) = self
            .open_seeded_maintained_subscription_view_in_authorization_mode(
                shape,
                binding,
                identity,
                tier,
                read_view,
                RegisterShapeOptions {
                    tier,
                    read_view: read_view.clone(),
                    ..RegisterShapeOptions::default()
                }
                .read_view_key(),
                authorization_mode,
                settled_binding_view,
                settled_authority_result_key.clone(),
                PreparedClaimBindingMode::Strict,
                pending_overlay,
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
            result_select: shape.query().select.clone(),
            result_relation_projection: shape
                .query()
                .relation
                .as_ref()
                .map(crate::query::relation_output_projection)
                .transpose()?
                .map(|(_, columns)| columns),
            result_relation_projections: shape
                .query()
                .relation
                .as_ref()
                .filter(|relation| crate::query::relation_union_parts(&relation.rel).is_some())
                .map(crate::query::relation_union_leaf_projections)
                .transpose()?,
            result_set: BTreeSet::new(),
            result_payloads: BTreeMap::new(),
            program_facts: BTreeSet::new(),
            root_occurrence_ids: Vec::new(),
            initial_received,
            covered_input_receiver: CoveredInputReceiver::new(
                covered_input_sources,
                read_view.clone(),
            ),
        };
        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
            eprintln!(
                "JAZZ_COVERED_INPUT_TRACE stage=open root_terminal={} sources={} initial_received={}",
                local.has_root_collector(),
                local.covered_input_receiver.sources.len(),
                local.initial_received,
            );
        }
        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
            eprintln!(
                "JAZZ_COVERED_INPUT_TRACE stage=initial_transitions adds={} removes={} facts_adds={} facts_removes={} terminal_ops={}",
                transitions.adds.len(),
                transitions.removes.len(),
                transitions.program_fact_adds.len(),
                transitions.program_fact_removes.len(),
                transitions.terminal_operations.len(),
            );
        }
        let _initial_delta = self
            .apply_local_maintained_view_transitions(&mut local, transitions)
            .await?;
        if local.has_covered_input_sources()
            && let Some(authority_result_key) = settled_authority_result_key.clone()
        {
            // Local terminal initialization and authority closure readiness are
            // independent. A missing closure must not erase a consumed initial
            // batch: an eventual empty closure need not produce another batch.
            // Publication still checks the exact authority receipt separately.
            self.install_opened_local_covered_receiver(
                &mut local,
                &authority_result_key,
                progress_waker,
            )
            .await?;
        }
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

    /// Install an already-claimed exact authority closure into a newly opened
    /// receiver, drive the one shared Groove graph to quiescence, and fold the
    /// resulting local terminal into its retained state.  Both ordinary late
    /// client opening and seeded relay-edge opening use this sequence: neither
    /// may read an authority result/output cache to synthesize its reset.
    ///
    /// `None` means the receipt has not claimed a complete closure yet.  A
    /// present empty transition is a valid empty exact closure and remains
    /// distinct from that pending state.
    pub(crate) async fn install_opened_local_covered_receiver(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        authority_result_key: &AuthorityResultKey,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<Option<super::maintained_subscription_view::ResultTransitions>, Error> {
        if !self
            .replace_local_maintained_covered_inputs(local, authority_result_key)
            .await?
        {
            return Ok(None);
        }
        self.drive_ready_query_runtime_with_waker(progress_waker)
            .await?;
        let (transitions, _) = self
            .drain_local_maintained_view_subscription_transitions(
                local,
                Some(authority_result_key.clone()),
                &BTreeSet::new(),
                progress_waker,
            )
            .await?;
        let transitions = transitions.unwrap_or_default();
        // Keep the retained receiver snapshot in lockstep with the exact same
        // terminal batch the caller publishes.  `false` avoids a second
        // facade materialization; state still folds terminal ordering/moves.
        let _ = self
            .apply_local_maintained_view_transitions_inner(local, transitions.clone(), false)
            .await?;
        Ok(Some(transitions))
    }

    pub(crate) fn local_maintained_authority_reconciliation_due(
        &self,
        local: &LocalMaintainedViewSubscription,
        authority_result_key: &AuthorityResultKey,
    ) -> bool {
        self.covered_input_receiver_reconciliation_due(
            &local.covered_input_receiver,
            authority_result_key,
        )
    }

    pub(crate) fn covered_input_receiver_reconciliation_due(
        &self,
        receiver: &CoveredInputReceiver,
        authority_result_key: &AuthorityResultKey,
    ) -> bool {
        // A local-knowledge graph has no authority input slots to replace.
        // Remote receipts can advance sync without gating or retiring its
        // locally stored rows and pending writes.
        if receiver.is_empty() {
            return false;
        }
        let Some(authority_result) = self.query.authority_results.get(authority_result_key) else {
            return false;
        };
        let crate::node::AuthoritySourceClosure::Claimed { generation } =
            authority_result.source_closure
        else {
            return false;
        };
        receiver
            .local_authority
            .is_due(authority_result_key, generation)
    }

    /// Replace the exact authority-covered source frontier of a receiver's
    /// local maintained graph. The authority selects and ships the input
    /// closure; this function neither re-runs policy nor reads an arbitrary
    /// current winner as a fallback.
    ///
    /// Every compiled source occurrence is keyed by full `ProgramSourceId`.
    /// A fact for an unknown source is a protocol/compile mismatch, not an
    /// opportunity to guess by table or collector name. Replacements are
    /// submitted as one database batch so the graph observes only the old or
    /// new closure, never a cross-source mixture.
    pub(crate) async fn replace_local_maintained_covered_inputs(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        authority_result_key: &AuthorityResultKey,
    ) -> Result<bool, Error> {
        let Some(authority_result) = self.query.authority_results.get(authority_result_key) else {
            return Err(Error::InvalidStoredValue(
                "covered input reconciliation has no exact authority receipt",
            ));
        };
        let closure_generation = match authority_result.source_closure {
            crate::node::AuthoritySourceClosure::Pending => {
                return Ok(false);
            }
            crate::node::AuthoritySourceClosure::Claimed { generation } => generation,
        };
        let installed_for_authority = local
            .covered_input_receiver
            .installed_closure
            .as_ref()
            .is_some_and(|(key, _)| key == authority_result_key);
        if !installed_for_authority {
            return self
                .replace_covered_input_receiver(
                    &mut local.covered_input_receiver,
                    local.result_schema_version,
                    authority_result_key,
                )
                .await;
        }
        // Generic ViewUpdate frames may change facts unrelated to the receiver
        // input frontier.  Only the source-closure generation names the state
        // an incremental source delta may be applied to.
        if local.covered_input_receiver.installed_generation == Some(closure_generation) {
            return Ok(false);
        }
        // A claimed closure with no retained deltas is a reset successor. It
        // intentionally replaces all sources; only a non-reset publication
        // carries the predecessor-preserving incremental record below.
        if authority_result.source_incrementals.is_empty() {
            return self
                .replace_covered_input_receiver(
                    &mut local.covered_input_receiver,
                    local.result_schema_version,
                    authority_result_key,
                )
                .await;
        }
        let incremental = crate::node::coalesce_authority_source_incrementals(
            &authority_result.source_incrementals,
            local
                .covered_input_receiver
                .installed_generation
                .expect("installed receiver has a source receipt"),
            closure_generation,
        )?;
        self.apply_covered_input_receiver_incremental(
            &mut local.covered_input_receiver,
            local.result_schema_version,
            authority_result_key,
            incremental,
        )
        .await
    }

    /// Apply one authority frame's exact source delta to the already-installed
    /// receiver frontier. This is intentionally distinct from reset: it never
    /// scans/replaces an unchanged source set.
    async fn apply_covered_input_receiver_incremental(
        &mut self,
        receiver: &mut CoveredInputReceiver,
        result_schema_version: SchemaVersionId,
        authority_result_key: &AuthorityResultKey,
        incremental: crate::node::AuthoritySourceIncremental,
    ) -> Result<bool, Error> {
        if receiver.installed_generation != Some(incremental.predecessor_generation) {
            return Err(Error::InvalidStoredValue(
                "covered input incremental delta does not name receiver predecessor",
            ));
        }
        // Stage the local successor before changing Groove or receipt state.
        // The runtime validates its complete batch before mutation; keeping
        // this mirror staged gives the same all-or-nothing failure boundary.
        let mut staged_records = BTreeMap::<SupportingRow, Option<Vec<u8>>>::new();
        let mut changes = BTreeMap::<GlobalPhysicalTableId, (Vec<Vec<u8>>, Vec<Vec<u8>>)>::new();
        for input in &incremental.removes {
            let Some(records) = receiver.installed_records.get(&input.physical_table) else {
                return Err(Error::InvalidStoredValue(
                    "incremental covered input removes an unknown source occurrence",
                ));
            };
            if let Some(record) = records.get(input) {
                staged_records.insert(input.clone(), None);
                changes
                    .entry(input.physical_table)
                    .or_default()
                    .1
                    .push(record.clone());
            } else if input.version.layer == ResultRowLayer::Content {
                return Err(Error::InvalidStoredValue(
                    "incremental covered input removes a content witness absent from receiver predecessor",
                ));
            }
        }
        for input in &incremental.adds {
            let runtime_source =
                receiver
                    .sources
                    .get(&input.physical_table)
                    .ok_or(Error::InvalidStoredValue(
                        "incremental covered input names no compiled source occurrence",
                    ))?;
            let Some(record) = self
                .covered_input_runtime_record(
                    input,
                    runtime_source,
                    result_schema_version,
                    &receiver.read_view,
                )
                .await?
            else {
                continue;
            };
            let records = receiver
                .installed_records
                .get(&input.physical_table)
                .expect("receiver records are initialized with every compiled source");
            let present = match staged_records.get(input) {
                Some(record) => record.is_some(),
                None => records.contains_key(input),
            };
            if present {
                return Err(Error::InvalidStoredValue(
                    "incremental covered input duplicates a receiver content witness",
                ));
            }
            staged_records.insert(input.clone(), Some(record.clone()));
            changes
                .entry(input.physical_table)
                .or_default()
                .0
                .push(record);
        }
        let deltas = changes
            .into_iter()
            .map(|(source, (adds, removes))| {
                let runtime_source = receiver
                    .sources
                    .get(&source)
                    .expect("changed source was validated above");
                groove::ivm::InputSourceDelta {
                    id: runtime_source.id,
                    descriptor: runtime_source.descriptor.clone(),
                    adds,
                    removes,
                }
            })
            .collect::<Vec<_>>();
        let metrics = self
            .database
            .apply_input_source_deltas(deltas)
            .await
            .map_err(Error::Groove)?;
        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
            eprintln!(
                "JAZZ_COVERED_INPUT_TRACE stage=incremental_receiver_delta predecessor={} generation={} tick={} processed={}",
                incremental.predecessor_generation,
                incremental.generation,
                metrics.tick,
                metrics.records_processed,
            );
        }
        receiver
            .local_authority
            .replace_source(authority_result_key.clone(), incremental.generation);
        receiver.installed_closure = Some((authority_result_key.clone(), incremental.generation));
        receiver.installed_generation = Some(incremental.generation);
        // Every fallible preparation and the atomic runtime batch succeeded.
        // Commit only touched mirror entries; untouched records never clone.
        for (input, record) in staged_records {
            let records = receiver
                .installed_records
                .get_mut(&input.physical_table)
                .expect("staged source was validated before runtime mutation");
            match record {
                Some(record) => {
                    records.insert(input, record);
                }
                None => {
                    records.remove(&input);
                }
            }
        }
        Ok(true)
    }
    /// Install one exact authority closure into a receiver-owned source map.
    /// Both the facade and relay publication use this same source-only path.
    pub(crate) async fn replace_covered_input_receiver(
        &mut self,
        receiver: &mut CoveredInputReceiver,
        result_schema_version: SchemaVersionId,
        authority_result_key: &AuthorityResultKey,
    ) -> Result<bool, Error> {
        if receiver.sources.is_empty() {
            return Ok(false);
        }
        // A local graph may be prepared before the first trusted catalogue
        // replaces its bootstrap identities. Bind its existing table inputs to
        // that catalogue at the snapshot boundary, not to a stale bootstrap UUID.
        // This is once per table, never an occurrence expansion or row lookup.
        let remapping = receiver
            .sources
            .iter()
            .map(|(old, source)| {
                Ok((
                    *old,
                    self.scope_physical_table(result_schema_version, source.table.as_str())?,
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        for (old, new) in remapping {
            if old != new {
                let source = receiver
                    .sources
                    .remove(&old)
                    .expect("compiled table exists");
                receiver.sources.insert(new, source);
            }
        }
        // A newer authority generation invalidates the old source closure
        // before any validation.  In particular, a reset cannot reuse a
        // detached receipt just because its rows happen to look identical.
        receiver.installed_closure = None;
        let authority_result = self
            .query
            .authority_results
            .get(authority_result_key)
            .ok_or(Error::InvalidStoredValue(
                "covered input replacement has no exact authority receipt",
            ))?;
        let closure_generation = match authority_result.source_closure {
            crate::node::AuthoritySourceClosure::Pending => {
                // Opening a usage site is not a claim that every source is
                // empty.  Keep strict receivers pending until an exact reset
                // manifest arrives; this is deliberately not an error.
                if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                    eprintln!(
                        "JAZZ_COVERED_INPUT_TRACE stage=covered_closure_pending sources={}",
                        receiver.sources.len(),
                    );
                }
                return Ok(false);
            }
            crate::node::AuthoritySourceClosure::Claimed { generation } => generation,
        };
        let inputs = authority_result
            .covered_input_versions
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut records = receiver
            .sources
            .keys()
            .cloned()
            .map(|source| (source, BTreeMap::new()))
            .collect::<BTreeMap<_, BTreeMap<SupportingRow, Vec<u8>>>>();

        for input in inputs {
            let Some(runtime_source) = receiver.sources.get(&input.physical_table) else {
                return Err(Error::InvalidStoredValue(
                    "authority covered input names no compiled source occurrence",
                ));
            };
            let Some(record) = self
                .covered_input_runtime_record(
                    &input,
                    runtime_source,
                    result_schema_version,
                    &receiver.read_view,
                )
                .await?
            else {
                continue;
            };
            records
                .get_mut(&input.physical_table)
                .expect("source presence was checked above")
                .insert(input, record);
        }
        let replacement_record_counts = records
            .iter()
            .map(|(source, rows)| (source.clone(), rows.len()))
            .collect::<Vec<_>>();
        let replacements = receiver
            .sources
            .iter()
            .map(|(source, runtime_source)| InputSourceReplacement {
                id: runtime_source.id,
                descriptor: runtime_source.descriptor.clone(),
                records: records
                    .get(source)
                    .map(|rows| rows.values().cloned().collect())
                    .unwrap_or_default(),
            })
            .collect::<Vec<_>>();

        let replacement_metrics = self
            .database
            .replace_input_sources(replacements)
            .await
            .map_err(Error::Groove)?;
        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
            eprintln!(
                "JAZZ_COVERED_INPUT_TRACE stage=replaced sources={replacement_record_counts:?} tick={} processed={} notifications={} notification_records={}",
                replacement_metrics.tick,
                replacement_metrics.records_processed,
                replacement_metrics.notifications_sent,
                replacement_metrics.notification_records,
            );
        }
        receiver
            .local_authority
            .replace_source(authority_result_key.clone(), closure_generation);
        receiver.installed_closure = Some((authority_result_key.clone(), closure_generation));
        receiver.installed_generation = Some(closure_generation);
        receiver.installed_records = records;
        Ok(true)
    }

    /// Decode one exact covered source fact into its descriptor-bound runtime
    /// tuple. Deletion witnesses are validated but intentionally contribute no
    /// tuple: their absence retracts a previously installed content carrier.
    async fn covered_input_runtime_record(
        &mut self,
        input: &SupportingRow,
        runtime_source: &CoveredInputSource,
        result_schema_version: SchemaVersionId,
        read_view: &ReadViewSpec,
    ) -> Result<Option<Vec<u8>>, Error> {
        let source_table =
            self.table_in_schema(runtime_source.table.as_str(), result_schema_version)?;
        let version = self
            .covered_input_version(input, result_schema_version)
            .await?
            .ok_or(Error::MissingTransaction(input.version.tx))?;
        if version.layer() == VersionLayer::Deletion {
            return Ok(None);
        }
        let row = self
            .projected_current_row_from_materialized_version_in_read_schema(
                result_schema_version,
                &version,
            )?
            .ok_or(Error::InvalidStoredValue(
                "authority covered content version cannot materialize a current row",
            ))?;
        // Initial resets and incremental edits share the same logical branch
        // projection, while the encoded witness below retains the physical
        // supplying branch. A base row shown through a head is still a base
        // version; neither projection nor a relay hop can relabel its identity.
        let row = self.project_covered_input_row_in_read_view(
            &source_table,
            result_schema_version,
            read_view,
            &version,
            row,
        )?;
        if row.table() != source_table.name {
            return Err(Error::InvalidStoredValue(
                "authority covered input does not project into its compiled source schema",
            ));
        }
        // Cells are projected for the requested read schema, but the hidden
        // witness identifies immutable history. Keep its authored alias so
        // publication reloads the original record rather than treating the
        // logical projection as a synthetic version in the read schema.
        let schema_alias = version.schema_version_alias();
        Ok(Some(super::read_sources::covered_input_record(
            &source_table,
            &runtime_source.descriptor,
            &row,
            schema_alias,
            &version.branch_key(),
        )?))
    }

    fn project_covered_input_row_in_read_view(
        &mut self,
        table: &TableSchema,
        schema: SchemaVersionId,
        read_view: &ReadViewSpec,
        version: &VersionRow,
        row: CurrentRow,
    ) -> Result<CurrentRow, Error> {
        let ReadViewSourceSpec::BranchView { head, .. } = &read_view.source else {
            return Ok(row);
        };
        let schema_definition = &self
            .catalogue
            .catalogue_schemas
            .get(&schema)
            .ok_or(Error::InvalidStoredValue(
                "covered input read schema is unknown",
            ))?
            .schema;
        let (head, _) = schema_definition
            .project_branch_view_selector(table, head)
            .map_err(Error::InvalidBranchKey)?;
        self.materialize_branch_view_winners(
            table.name.as_str(),
            schema,
            &head,
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::from([(version.row_uuid(), version.clone())]),
            BTreeMap::new(),
        )?
        .into_iter()
        .next()
        .ok_or(Error::InvalidStoredValue(
            "covered branch input did not materialize",
        ))
    }

    async fn drain_local_maintained_view_subscription_transitions(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        authoritative_result_key: Option<AuthorityResultKey>,
        _preserved_row_keys: &BTreeSet<(String, RowUuid)>,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<
        (
            Option<super::maintained_subscription_view::ResultTransitions>,
            bool,
        ),
        Error,
    > {
        if let Some(authority_result_key) = authoritative_result_key.as_ref()
            && self.local_maintained_authority_reconciliation_due(local, authority_result_key)
        {
            // Drive the receiver graph from the exact source closure before
            // draining its terminal. This replaces the former authority
            // membership/result shortcut.
            let replaced = self
                .replace_local_maintained_covered_inputs(local, authority_result_key)
                .await?;
            if !replaced {
                // The exact usage site has not received a closure manifest
                // yet. Do not drain/publish the graph's opening state as a
                // strict remote result; the runtime will retain the pending
                // authority reset and try again once the manifest arrives.
                return Ok((None, false));
            }
        }
        self.drive_ready_query_runtime_with_waker(progress_waker)
            .await?;
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
        loop {
            match local.subscription.try_recv() {
                Ok(deltas) => {
                    local.initial_received = true;
                    #[cfg(feature = "testing")]
                    if local.result_table == "group"
                        && std::env::var_os("JAZZ_HISTORY_DEBUG").is_some()
                    {
                        eprintln!(
                            "HISTORY_GROUP_RUNTIME sinks={} terminal_sinks={}",
                            deltas.sinks.len(),
                            deltas.terminal_sinks.len()
                        );
                    }
                    if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                        eprintln!(
                            "JAZZ_COVERED_INPUT_TRACE stage=drain sinks={} terminals={}",
                            deltas.sinks.len(),
                            deltas.terminal_sinks.len(),
                        );
                    }
                    let transitions = local.maintained.apply_multisink_deltas(
                        deltas,
                        &local.terminal_schemas,
                        &local.tables,
                        &self.node_aliases,
                    )?;
                    terminal_operations.extend(transitions.terminal_operations);
                    if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                        eprintln!(
                            "JAZZ_COVERED_INPUT_TRACE stage=transitions terminal_ops={} adds={} removes={}",
                            terminal_operations.len(),
                            transitions.adds.len(),
                            transitions.removes.len(),
                        );
                    }
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
        if drained_transition_is_empty(
            states.is_empty(),
            payload_states.is_empty(),
            fact_states.is_empty(),
            &terminal_operations,
        ) {
            return Ok((None, false));
        }
        let mut transitions = super::maintained_subscription_view::ResultTransitions {
            authoritative_membership_changed: false,
            authoritative_member_adds: BTreeSet::new(),
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
        Ok((Some(transitions), false))
    }

    async fn apply_local_maintained_view_transitions(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        transitions: super::maintained_subscription_view::ResultTransitions,
    ) -> Result<LocalMaintainedViewSubscriptionUpdate, Error> {
        self.apply_local_maintained_view_transitions_inner(local, transitions, true)
            .await
    }

    pub(super) async fn apply_local_maintained_view_transitions_inner(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        transitions: super::maintained_subscription_view::ResultTransitions,
        materialize_update: bool,
    ) -> Result<LocalMaintainedViewSubscriptionUpdate, Error> {
        // The compiler, not the query's surface syntax, selects the output
        // owner. Every public root collector (flat or nested) publishes via
        // its own terminal state and positional operations.
        let structured_output = local.terminal_schemas.has_root_collector();
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
                    if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                        eprintln!(
                            "JAZZ_COVERED_INPUT_TRACE stage=local_maintained_added table={} occurrence={occurrence_id:?}",
                            row.table(),
                        );
                    }
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
        } else if materialize_update && local.result_query.aggregate.is_some() {
            let materialized = self
                .materialize_local_maintained_relation_snapshot_with_occurrences(local)
                .await?;
            local.root_occurrence_ids = materialized.root_occurrence_ids.clone();
            LocalMaintainedViewSubscriptionUpdate::AggregateWindow {
                snapshot: materialized.snapshot,
                occurrence_ids: materialized.root_occurrence_ids,
            }
        } else {
            if !terminal_operations.is_empty() {
                return Err(Error::InvalidStoredValue(
                    "non-terminal app-row path emitted collector terminal operations",
                ));
            }
            LocalMaintainedViewSubscriptionUpdate::Flat { added, removed }
        })
    }
}

/// A root collector can change only positions or a projected payload.  Those
/// edits deliberately have no membership, payload-witness, or program-fact
/// delta, but they are still a public receiver transition.
fn drained_transition_is_empty(
    states_empty: bool,
    payload_states_empty: bool,
    fact_states_empty: bool,
    terminal_operations: &[groove::ivm::TerminalOperation],
) -> bool {
    states_empty && payload_states_empty && fact_states_empty && terminal_operations.is_empty()
}

#[cfg(test)]
mod terminal_transition_tests {
    use super::*;
    use groove::ivm::{TerminalEdit, TerminalOperation};

    fn operation(edit: TerminalEdit) -> TerminalOperation {
        TerminalOperation {
            root_descriptor: RecordDescriptor::default(),
            root_key: vec![0x01],
            path: Vec::new(),
            edit,
        }
    }

    #[test]
    fn terminal_only_root_batches_are_not_dropped() {
        let edits = [
            TerminalEdit::Insert {
                index: 0,
                key: vec![0x01],
                value: Vec::new(),
            },
            TerminalEdit::Update {
                key: vec![0x01],
                value: Vec::new(),
            },
            TerminalEdit::Remove { key: vec![0x01] },
            TerminalEdit::Move {
                key: vec![0x01],
                index: 0,
            },
        ];
        for edit in edits {
            assert!(
                !drained_transition_is_empty(true, true, true, &[operation(edit)]),
                "a collector operation is a public transition even without fact deltas"
            );
        }
        assert!(drained_transition_is_empty(true, true, true, &[]));
    }
}
