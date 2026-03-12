use std::collections::HashSet;

use super::*;
use crate::object::BranchName;
use crate::sync_manager::{
    MutationOperation, MutationOutcomeState, MutationRejection, current_timestamp_micros,
};

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
            MutationOutcomeState::Accepted => self.delete_mutation_record(record),
            MutationOutcomeState::Rejected(_) => self.acknowledge_rejection_group(record.id),
            MutationOutcomeState::SupersededByRejection { root_mutation_id } => {
                self.acknowledge_rejection_group(*root_mutation_id)
            }
        }
    }

    fn load_mutation_record_for_commit(
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

        record.outcome = MutationOutcomeState::Accepted;
        self.storage
            .put_mutation_record(record)
            .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?;
        self.mutation_events
            .push(MutationEvent::Accepted { mutation_id });
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
            }
        }

        Ok(())
    }

    fn acknowledge_rejection_group(
        &mut self,
        root_mutation_id: MutationId,
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
            self.delete_mutation_record(record)?;
        }

        Ok(())
    }

    fn delete_mutation_record(&mut self, record: MutationRecord) -> Result<(), RuntimeError> {
        self.storage
            .delete_mutation_record(record.id)
            .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?;
        for commit_id in record.commit_ids {
            self.commit_to_mutation.remove(&commit_id);
        }
        self.mutation_events.push(MutationEvent::Acknowledged {
            mutation_id: record.id,
        });
        Ok(())
    }
}

fn matches_rejection(outcome: &MutationOutcomeState, rejection: &MutationRejection) -> bool {
    matches!(outcome, MutationOutcomeState::Rejected(existing) if existing == rejection)
}
