impl PeerState {
    fn record_outgoing_view_update_metadata(&mut self, update: &SyncMessage) {
        if let SyncMessage::ViewUpdate(view) = update
            && view.supporting_rows.is_snapshot()
            && !view.peer_payload_inventory.opening_pending
        {
            // A generic fresh snapshot supersedes any incremental publisher
            // baseline. The maintained path installs its prepared successor
            // after this metadata call; other paths rebuild once on next drain.
            let state = self
                .publication_states
                .entry(view.subscription)
                .or_default();
            state.supporting_revision = None;
        }
        let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            version_carriers,
            peer_payload_inventory,
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
    pub async fn prove_terminal_commit_authorization<S>(
        &mut self,
        node: &mut NodeState<S>,
        writer: AuthorSubject,
        claims: BTreeMap<String, Value>,
        versions: &[VersionRecord],
        candidate_tx_id: TxId,
    ) -> Result<bool, Error>
    where
        S: OrderedKvStorage,
    {
        self.prove_terminal_commit(node, writer, claims, versions, candidate_tx_id, true)
            .await
    }

    /// Establish the terminal support proof without evaluating the final
    /// write policies. For admission paths whose terminal ingest evaluates
    /// those policies itself, so a second evaluation here would be discarded.
    pub async fn prove_terminal_commit_support<S>(
        &mut self,
        node: &mut NodeState<S>,
        writer: AuthorSubject,
        claims: BTreeMap<String, Value>,
        versions: &[VersionRecord],
        candidate_tx_id: TxId,
    ) -> Result<(), Error>
    where
        S: OrderedKvStorage,
    {
        self.prove_terminal_commit(node, writer, claims, versions, candidate_tx_id, false)
            .await
            .map(|_| ())
    }

    async fn prove_terminal_commit<S>(
        &mut self,
        node: &mut NodeState<S>,
        writer: AuthorSubject,
        claims: BTreeMap<String, Value>,
        versions: &[VersionRecord],
        candidate_tx_id: TxId,
        evaluate_write_policies: bool,
    ) -> Result<bool, Error>
    where
        S: OrderedKvStorage,
    {
        // SYSTEM is the trusted backend policy subject. Row-policy admission
        // already bypasses it, so it must not try to hydrate an authorization
        // support proof: claim and join predicates have no SYSTEM session to
        // bind and are irrelevant to the bypass decision.
        if writer == AuthorSubject::SYSTEM {
            return Ok(true);
        }
        // Both support hydration and the final policy evaluation below read
        // the active session scope. Keep the immutable admitted snapshot
        // installed for the entire proof; the author-keyed compatibility map
        // is neither sufficient nor safe for a scope-isolated relay.
        let mut node = node.scoped_active_session_claims(writer, claims.clone());
        for action in node
            .authorization_actions_for_versions_in_transaction(versions, Some(candidate_tx_id))
            .await?
        {
            // Terminal proof is bound to this connection's immutable admitted
            // snapshot. Never fall back to the node's author-keyed
            // compatibility map: a scope relay deliberately keeps its binding
            // out of that mutable map, and same-author sessions may differ.
            let scope =
                node.authorization_support_scope_for_session(writer, Some(&claims), &action)?;
            // This node is the sole authority for every table it serves, so
            // each policy input is already at the authority cut in its own
            // storage and the final evaluation below reads it directly.
            // Hydrating the clauses as maintained support views would only
            // re-prove that, and each view materializes every row the policy
            // query matches for the connection's lifetime. Cross-authority
            // support (INV-SHARD-13) has to be bound to the candidate's
            // dependency closure; see #3794.
            if !scope.subscriptions.is_empty() {
                self.authority_scope_proofs = self.authority_scope_proofs.saturating_add(1);
            }
        }
        // The terminal result is evaluated under this exact snapshot; a
        // claim-only policy must not become an implicit grant.
        if !evaluate_write_policies {
            return Ok(true);
        }
        for version in versions {
            if !node
                .version_satisfies_write_policy(version, writer, candidate_tx_id, versions)
                .await?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn terminal_authority_scope_proof_count(&self) -> u64 {
        self.authority_scope_proofs
    }

    fn record_outgoing_view_update<S: OrderedKvStorage>(
        &mut self,
        _node: &NodeState<S>,
        _schema: crate::ids::SchemaVersionId,
        update: &SyncMessage,
    ) -> Result<(), Error> {
        self.record_outgoing_view_update_metadata(update);
        if let SyncMessage::ViewUpdate(view) = update {
            let state = self
                .publication_states
                .entry(view.subscription)
                .or_default();
            state.supporting_revision = Some(view.supporting_rows.revision());
            if let Some(maintained) = &mut state.maintained_subscription_view {
                maintained.maintained.acknowledge_peer_source_closure();
            }
        }
        Ok(())
    }

    fn apply_outgoing_view_delta(
        &mut self,
        subscription: SubscriptionKey,
        reset_input_set: bool,
        result_member_adds: &[ResultMemberEntry],
        result_member_removes: &[ResultMemberEntry],
    ) {
        let state = self.publication_states.entry(subscription).or_default();
        // This path records an independently supplied delta rather than the
        // maintained journal's exact successor. Re-establish its baseline if
        // this subscription later returns to maintained publication.
        if reset_input_set && let Some(view) = &mut state.maintained_subscription_view {
            view.maintained.forget_peer_source_closure_baseline();
        }
        if reset_input_set {
            state.supporting_revision = None;
            state.result_member_set.clear();
            state.member_index.clear();
        }
        for member in result_member_removes {
            state.result_member_set.remove(member);
            apply_contribution_remove(state, std::iter::once(member), &mut Vec::new());
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
