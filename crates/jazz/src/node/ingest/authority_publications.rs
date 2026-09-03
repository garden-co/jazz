impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Reconstruct an edge's coherent upload from accepted canonical history.
    ///
    /// Call only after settling edge admission and generated merge publications.
    /// Globally acknowledged parents already exist at core, so traversal stops
    /// there rather than walking a row's complete historical ancestry.
    pub async fn edge_authority_publication_for(
        &mut self,
        tx_id: TxId,
    ) -> Result<crate::protocol::AuthorityPublication, Error> {
        let original = self.query_versions_for_tx(tx_id).await?;
        let records = original
            .iter()
            .map(|row| self.version_record_from_row(row))
            .collect::<Result<Vec<_>, _>>()?;
        let rows = self.merge_rows_for_versions(&records)?;
        let mut pending = vec![tx_id];
        for (table, branch, row) in rows {
            let table_id = self
                .physical_table_id_for_schema(self.catalogue.current_write_schema.schema, &table)?;
            for head in self.merge_head_tx_ids(table_id, &branch, row).await? {
                let state = self
                    .query_transaction(head)
                    .await?
                    .ok_or(Error::MissingTransaction(head))?;
                if state.fate == Fate::Accepted && state.durability == DurabilityTier::Edge {
                    pending.push(head);
                }
            }
        }
        let mut commits = BTreeMap::new();
        while let Some(current) = pending.pop() {
            if commits.contains_key(&current) {
                continue;
            }
            let stored = self
                .query_transaction(current)
                .await?
                .ok_or(Error::MissingTransaction(current))?;
            if stored.fate != Fate::Accepted || stored.durability < DurabilityTier::Edge {
                return Err(Error::InvalidStoredValue(
                    "authority publication requires accepted persisted transactions",
                ));
            }
            let tx = stored.tx;
            let versions = self
                .query_versions_for_tx(current)
                .await?
                .iter()
                .map(|row| self.version_record_from_row(row))
                .collect::<Result<Vec<_>, _>>()?;
            for version in &versions {
                for parent in version.parents() {
                    let state = self
                        .query_transaction(parent)
                        .await?
                        .ok_or(Error::MissingTransaction(parent))?;
                    if state.durability < DurabilityTier::Global {
                        pending.push(parent);
                    }
                }
            }
            commits.insert(
                current,
                crate::protocol::AuthorityCommitUnit { tx, versions },
            );
        }
        Ok(crate::protocol::AuthorityPublication {
            tx_id,
            commits: commits.into_values().collect(),
        })
    }

    /// Finalize one publication from a host-authenticated edge authority.
    ///
    /// This trusted-host API is not permission admission for an ordinary
    /// client or relay. Network callers must prove authority capability first.
    /// Individual transactions retain their own fates/global sequence numbers;
    /// only frontier reconciliation is delayed until the whole group is present.
    pub async fn ingest_edge_authority_publication(
        &mut self,
        publication: crate::protocol::AuthorityPublication,
        now_ms: u64,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error> {
        self.require_catalogue_ready()?;
        if publication.commits.is_empty()
            || !publication
                .commits
                .iter()
                .any(|unit| unit.tx.tx_id == publication.tx_id)
            || publication
                .commits
                .windows(2)
                .any(|pair| pair[0].tx.tx_id >= pair[1].tx.tx_id)
        {
            return Err(Error::InvalidStoredValue(
                "authority publication must have an anchor and strictly ordered unique transactions",
            ));
        }
        let mut known = BTreeSet::new();
        let mut affected_rows = BTreeSet::new();
        for unit in &publication.commits {
            if unit.tx.kind != TxKind::Mergeable
                || !commit_unit_write_count_matches(&unit.tx, unit.versions.len())
                || commit_unit_limit_violation(&unit.versions).is_some()
                || crate::protocol::validate_version_records(&unit.versions).is_err()
                || self
                    .malformed_authored_version_reason(&unit.versions)
                    .is_some()
            {
                return Err(Error::InvalidStoredValue(
                    "authority publication contains an invalid complete mergeable transaction",
                ));
            }
            self.validate_contribution_merge_operation_identities(&unit.tx)?;
            for version in &unit.versions {
                for parent in version.parents() {
                    if !known.contains(&parent) && self.query_transaction(parent).await?.is_none() {
                        return Err(Error::MissingTransaction(parent));
                    }
                }
            }
            affected_rows.extend(self.merge_rows_for_versions(&unit.versions)?);
            known.insert(unit.tx.tx_id);
        }
        let mut outcome = PublicationOutcome::settled(Vec::new());
        for unit in publication.commits {
            let tx_id = unit.tx.tx_id;
            let accepted = self
                .finalize_edge_accepted_mergeable_commit_unit_with_reconciliation(
                    unit.tx,
                    unit.versions,
                    now_ms,
                    false,
                )
                .await?;
            if !accepted.value.iter().any(|message| matches!(message,
                SyncMessage::FateUpdate { tx_id: admitted, fate: Fate::Accepted, .. } if *admitted == tx_id
            )) {
                return Err(Error::InvalidStoredValue("authority publication member did not reach global acceptance"));
            }
            outcome.append_outcome(accepted);
        }
        outcome.append_outcome(
            self.create_merge_versions_for_rows(
                affected_rows.into_iter().collect(),
                MergeAuthority::Core,
            )
            .await?,
        );
        Ok(outcome)
    }
}
