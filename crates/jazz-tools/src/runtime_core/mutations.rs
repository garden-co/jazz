use std::collections::{HashMap, HashSet};

use super::*;
use crate::object::BranchName;
use crate::sync_manager::{
    MutationOperation, MutationOutcomeState, MutationRejection, current_timestamp_micros,
};

pub(crate) const MAX_RETAINED_UNACKNOWLEDGED_REJECTIONS: usize = 10_000;

impl<S: Storage, Sch: Scheduler, Sy: SyncSender> RuntimeCore<S, Sch, Sy> {
    pub(crate) fn record_local_mutation(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        table: Option<String>,
        operation: MutationOperation,
        commit_ids: Vec<CommitId>,
        previous_commit_ids: Vec<CommitId>,
    ) -> Result<Option<MutationId>, RuntimeError> {
        if !self.mutation_journal_enabled || commit_ids.is_empty() {
            return Ok(None);
        }

        let previous_object_outcome = self.get_object_outcome(object_id)?;
        let mutation_id = MutationId::new();
        let record = MutationRecord {
            id: mutation_id,
            object_id,
            branch_name,
            table,
            operation,
            commit_ids: commit_ids.clone(),
            previous_commit_ids,
            recorded_at_micros: current_timestamp_micros(),
            highest_acked_tier: None,
            outcome: MutationOutcomeState::Pending,
        };

        self.storage
            .put_mutation_record(record)
            .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?;

        for commit_id in commit_ids {
            self.commit_to_mutation.insert(commit_id, mutation_id);
        }
        self.mutation_events
            .push(MutationEvent::Recorded { mutation_id });
        self.enqueue_object_outcome_event_if_changed(object_id, previous_object_outcome)?;

        Ok(Some(mutation_id))
    }

    pub(crate) fn advance_mutation_ack(
        &mut self,
        commit_id: CommitId,
        tier: DurabilityTier,
    ) -> Result<(), RuntimeError> {
        if !self.mutation_journal_enabled {
            return Ok(());
        }

        let Some(mut record) = self.load_mutation_record_for_commit(commit_id)? else {
            return Ok(());
        };

        if record
            .highest_acked_tier
            .is_some_and(|existing| existing >= tier)
        {
            return Ok(());
        }

        let mutation_id = record.id;
        record.highest_acked_tier = Some(tier);
        self.storage
            .put_mutation_record(record)
            .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?;
        self.mutation_events
            .push(MutationEvent::AckAdvanced { mutation_id, tier });

        Ok(())
    }

    pub(crate) fn register_persisted_mutation_waiter(
        &mut self,
        mutation_id: MutationId,
        tier: DurabilityTier,
    ) -> PersistedMutationReceiver {
        let (sender, receiver) = oneshot::channel();
        self.ack_watchers
            .entry(mutation_id)
            .or_default()
            .push(PersistedMutationWatcher { tier, sender });
        receiver
    }

    pub(crate) fn handle_received_mutation_outcome(
        &mut self,
        outcome: MutationOutcome,
    ) -> Result<(), RuntimeError> {
        self.mutation_outcomes.push(outcome.clone());

        if !self.mutation_journal_enabled {
            return Ok(());
        }

        let mut mutation_ids = HashSet::new();
        for &commit_id in outcome.commit_ids() {
            if let Some(record) = self.load_mutation_record_for_commit(commit_id)? {
                mutation_ids.insert(record.id);
            }
        }

        match outcome {
            MutationOutcome::Accepted(_) => {
                for mutation_id in mutation_ids {
                    self.mark_mutation_accepted(mutation_id)?;
                }
            }
            MutationOutcome::Rejected(rejection) => {
                for mutation_id in mutation_ids {
                    self.apply_rejection_to_mutation_chain(mutation_id, rejection.clone())?;
                }
            }
        }

        self.enforce_rejected_mutation_retention_cap(MAX_RETAINED_UNACKNOWLEDGED_REJECTIONS)?;

        Ok(())
    }

    pub(crate) fn acknowledge_mutation_outcome_inner(
        &mut self,
        mutation_id: MutationId,
    ) -> Result<(), RuntimeError> {
        if !self.mutation_journal_enabled {
            return Ok(());
        }

        let Some(record) = self
            .storage
            .load_mutation_record(mutation_id)
            .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?
        else {
            return Ok(());
        };

        match &record.outcome {
            MutationOutcomeState::Pending => Ok(()),
            MutationOutcomeState::Accepted => self.delete_mutation_record(record, true),
            MutationOutcomeState::Rejected(_) => self.prune_rejection_group(record.id, true),
            MutationOutcomeState::SupersededByRejection { root_mutation_id } => {
                self.prune_rejection_group(*root_mutation_id, true)
            }
        }
    }

    pub(crate) fn load_mutation_record_for_commit(
        &mut self,
        commit_id: CommitId,
    ) -> Result<Option<MutationRecord>, RuntimeError> {
        if let Some(mutation_id) = self.commit_to_mutation.get(&commit_id).copied() {
            return self
                .storage
                .load_mutation_record(mutation_id)
                .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)));
        }

        let record = self
            .storage
            .load_mutation_record_by_commit(commit_id)
            .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?;

        if let Some(record) = &record {
            for &mapped_commit_id in &record.commit_ids {
                self.commit_to_mutation.insert(mapped_commit_id, record.id);
            }
        }

        Ok(record)
    }

    pub(crate) fn list_object_outcomes_inner(
        &self,
    ) -> Result<Vec<ObjectOutcomeEvent>, RuntimeError> {
        let mut by_object: HashMap<ObjectId, (u8, u64, ObjectOutcomeState)> = HashMap::new();

        for record in self
            .storage
            .list_mutation_records_by_outcome(MutationOutcomeFilter::Accepted)
            .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?
        {
            let candidate = (
                0,
                record.recorded_at_micros,
                ObjectOutcomeState::Accepted {
                    mutation_id: record.id,
                },
            );
            upsert_object_outcome_candidate(&mut by_object, record.object_id, candidate);
        }

        for record in self
            .storage
            .list_mutation_records_by_outcome(MutationOutcomeFilter::Pending)
            .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?
        {
            let candidate = (
                1,
                record.recorded_at_micros,
                ObjectOutcomeState::Pending {
                    mutation_id: record.id,
                },
            );
            upsert_object_outcome_candidate(&mut by_object, record.object_id, candidate);
        }

        for record in self
            .storage
            .list_mutation_records_by_outcome(MutationOutcomeFilter::Rejected)
            .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?
        {
            let Some(rejection) = (match &record.outcome {
                MutationOutcomeState::Rejected(rejection) => Some(rejection),
                _ => None,
            }) else {
                continue;
            };

            let candidate = (
                2,
                rejection.rejected_at_micros,
                ObjectOutcomeState::Errored {
                    mutation_id: record.id,
                    code: rejection.code,
                    reason: rejection.reason.clone(),
                },
            );
            upsert_object_outcome_candidate(&mut by_object, record.object_id, candidate);
        }

        let mut outcomes = by_object
            .into_iter()
            .map(|(object_id, (_, _, outcome))| ObjectOutcomeEvent {
                object_id,
                outcome: Some(outcome),
            })
            .collect::<Vec<_>>();
        outcomes.sort_by_key(|event| *event.object_id.uuid());
        Ok(outcomes)
    }

    fn load_mutation_records_for_object(
        &mut self,
        object_id: ObjectId,
    ) -> Result<Vec<MutationRecord>, RuntimeError> {
        let records = self
            .storage
            .list_mutation_records_for_object(object_id)
            .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?;

        for record in &records {
            for &commit_id in &record.commit_ids {
                self.commit_to_mutation.insert(commit_id, record.id);
            }
        }

        Ok(records)
    }

    fn mark_mutation_accepted(&mut self, mutation_id: MutationId) -> Result<(), RuntimeError> {
        let Some(mut record) = self
            .storage
            .load_mutation_record(mutation_id)
            .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?
        else {
            return Ok(());
        };

        if matches!(record.outcome, MutationOutcomeState::Accepted) {
            return Ok(());
        }

        let previous_object_outcome = self.get_object_outcome(record.object_id)?;
        let object_id = record.object_id;
        record.outcome = MutationOutcomeState::Accepted;
        let highest_acked_tier = record.highest_acked_tier;
        self.storage
            .put_mutation_record(record)
            .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?;
        self.mutation_events
            .push(MutationEvent::Accepted { mutation_id });
        self.resolve_persisted_waiters(mutation_id, highest_acked_tier);
        self.enqueue_object_outcome_event_if_changed(object_id, previous_object_outcome)?;
        Ok(())
    }

    fn apply_rejection_to_mutation_chain(
        &mut self,
        root_mutation_id: MutationId,
        rejection: MutationRejection,
    ) -> Result<(), RuntimeError> {
        let Some(root_record) = self
            .storage
            .load_mutation_record(root_mutation_id)
            .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?
        else {
            return Ok(());
        };
        let previous_object_outcome = self.get_object_outcome(root_record.object_id)?;

        let deactivated_commit_ids = {
            let root_commit_ids: HashSet<_> = root_record.commit_ids.iter().copied().collect();
            self.schema_manager
                .query_manager_mut()
                .sync_manager_mut()
                .object_manager
                .deactivate_commit_chain(
                    &mut self.storage,
                    root_record.object_id,
                    root_record.branch_name,
                    root_commit_ids,
                )
                .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?
        };

        let related_records = self.load_mutation_records_for_object(root_record.object_id)?;
        let mut deactivated_commit_ids: HashSet<_> = deactivated_commit_ids.into_iter().collect();
        if deactivated_commit_ids.is_empty() {
            deactivated_commit_ids.extend(root_record.commit_ids.iter().copied());
        }

        for mut record in related_records.into_iter().filter(|record| {
            record.branch_name == root_record.branch_name
                && record
                    .commit_ids
                    .iter()
                    .any(|commit_id| deactivated_commit_ids.contains(commit_id))
        }) {
            if record.id == root_mutation_id {
                if matches_rejection(&record.outcome, &rejection) {
                    continue;
                }
                record.outcome = MutationOutcomeState::Rejected(rejection.clone());
                self.storage
                    .put_mutation_record(record)
                    .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?;
                self.mutation_events.push(MutationEvent::Rejected {
                    mutation_id: root_mutation_id,
                    rejection: rejection.clone(),
                });
                self.reject_persisted_waiters(
                    root_mutation_id,
                    PersistedMutationError {
                        mutation_id: root_mutation_id,
                        root_mutation_id,
                        rejection: rejection.clone(),
                    },
                );
            } else {
                if matches!(
                    record.outcome,
                    MutationOutcomeState::SupersededByRejection {
                        root_mutation_id: existing_root
                    } if existing_root == root_mutation_id
                ) {
                    continue;
                }
                let mutation_id = record.id;
                record.outcome = MutationOutcomeState::SupersededByRejection { root_mutation_id };
                self.storage
                    .put_mutation_record(record)
                    .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?;
                self.mutation_events
                    .push(MutationEvent::SupersededByRejection {
                        mutation_id,
                        root_mutation_id,
                    });
                self.reject_persisted_waiters(
                    mutation_id,
                    PersistedMutationError {
                        mutation_id,
                        root_mutation_id,
                        rejection: rejection.clone(),
                    },
                );
            }
        }

        self.enqueue_object_outcome_event_if_changed(
            root_record.object_id,
            previous_object_outcome,
        )?;

        Ok(())
    }

    fn prune_rejection_group(
        &mut self,
        root_mutation_id: MutationId,
        emit_ack_events: bool,
    ) -> Result<(), RuntimeError> {
        let Some(root_record) = self
            .storage
            .load_mutation_record(root_mutation_id)
            .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?
        else {
            return Ok(());
        };

        let records = self.load_mutation_records_for_object(root_record.object_id)?;
        let records_to_ack: Vec<_> = records
            .into_iter()
            .filter(|record| {
                record.branch_name == root_record.branch_name
                    && (record.id == root_mutation_id
                        || matches!(
                            record.outcome,
                            MutationOutcomeState::SupersededByRejection {
                                root_mutation_id: existing_root
                            } if existing_root == root_mutation_id
                        ))
            })
            .collect();

        let commit_ids_to_prune: HashSet<_> = records_to_ack
            .iter()
            .flat_map(|record| record.commit_ids.iter().copied())
            .collect();

        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .object_manager
            .prune_inactive_commits(
                &mut self.storage,
                root_record.object_id,
                root_record.branch_name,
                &commit_ids_to_prune,
            )
            .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?;

        for record in records_to_ack {
            self.delete_mutation_record(record, emit_ack_events)?;
        }

        Ok(())
    }

    fn delete_mutation_record(
        &mut self,
        record: MutationRecord,
        emit_ack_event: bool,
    ) -> Result<(), RuntimeError> {
        let previous_object_outcome = self.get_object_outcome(record.object_id)?;
        let object_id = record.object_id;
        self.storage
            .delete_mutation_record(record.id)
            .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?;
        self.ack_watchers.remove(&record.id);
        for commit_id in record.commit_ids {
            self.commit_to_mutation.remove(&commit_id);
        }
        if emit_ack_event {
            self.mutation_events.push(MutationEvent::Acknowledged {
                mutation_id: record.id,
            });
        }
        self.enqueue_object_outcome_event_if_changed(object_id, previous_object_outcome)?;
        Ok(())
    }

    pub(crate) fn resolve_persisted_waiters(
        &mut self,
        mutation_id: MutationId,
        highest_acked_tier: Option<DurabilityTier>,
    ) {
        let Some(highest_acked_tier) = highest_acked_tier else {
            return;
        };
        let Some(watchers) = self.ack_watchers.remove(&mutation_id) else {
            return;
        };

        let mut remaining = Vec::new();
        for watcher in watchers {
            if highest_acked_tier >= watcher.tier {
                let _ = watcher.sender.send(Ok(()));
            } else {
                remaining.push(watcher);
            }
        }

        if !remaining.is_empty() {
            self.ack_watchers.insert(mutation_id, remaining);
        }
    }

    fn reject_persisted_waiters(&mut self, mutation_id: MutationId, error: PersistedMutationError) {
        if let Some(watchers) = self.ack_watchers.remove(&mutation_id) {
            for watcher in watchers {
                let _ = watcher.sender.send(Err(error.clone()));
            }
        }
    }

    pub(crate) fn enforce_rejected_mutation_retention_cap(
        &mut self,
        limit: usize,
    ) -> Result<(), RuntimeError> {
        let mut rejected_roots = self
            .storage
            .list_mutation_records_by_outcome(MutationOutcomeFilter::Rejected)
            .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?;

        if rejected_roots.len() <= limit {
            return Ok(());
        }

        rejected_roots.sort_by_key(|record| match &record.outcome {
            MutationOutcomeState::Rejected(rejection) => rejection.rejected_at_micros,
            _ => u64::MAX,
        });

        let overflow = rejected_roots.len() - limit;
        for record in rejected_roots.into_iter().take(overflow) {
            self.prune_rejection_group(record.id, false)?;
        }

        Ok(())
    }

    fn enqueue_object_outcome_event_if_changed(
        &mut self,
        object_id: ObjectId,
        previous: Option<ObjectOutcomeState>,
    ) -> Result<(), RuntimeError> {
        let current = self.get_object_outcome(object_id)?;
        if current != previous {
            self.object_outcome_events.push(ObjectOutcomeEvent {
                object_id,
                outcome: current,
            });
        }
        Ok(())
    }
}

fn matches_rejection(outcome: &MutationOutcomeState, rejection: &MutationRejection) -> bool {
    matches!(outcome, MutationOutcomeState::Rejected(existing) if existing == rejection)
}

fn upsert_object_outcome_candidate(
    by_object: &mut HashMap<ObjectId, (u8, u64, ObjectOutcomeState)>,
    object_id: ObjectId,
    candidate: (u8, u64, ObjectOutcomeState),
) {
    match by_object.get(&object_id) {
        Some((existing_priority, existing_timestamp, _))
            if *existing_priority > candidate.0
                || (*existing_priority == candidate.0 && *existing_timestamp >= candidate.1) => {}
        _ => {
            by_object.insert(object_id, candidate);
        }
    }
}
