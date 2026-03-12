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

        for mutation_id in mutation_ids {
            let Some(mut record) = self
                .storage
                .load_mutation_record(mutation_id)
                .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?
            else {
                continue;
            };

            match &outcome {
                MutationOutcome::Accepted(_) => {
                    if matches!(record.outcome, MutationOutcomeState::Accepted) {
                        continue;
                    }
                    record.outcome = MutationOutcomeState::Accepted;
                    self.storage
                        .put_mutation_record(record)
                        .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?;
                    self.mutation_events
                        .push(MutationEvent::Accepted { mutation_id });
                }
                MutationOutcome::Rejected(rejection) => {
                    if matches_rejection(&record.outcome, rejection) {
                        continue;
                    }
                    record.outcome = MutationOutcomeState::Rejected(rejection.clone());
                    self.storage
                        .put_mutation_record(record)
                        .map_err(|err| RuntimeError::WriteError(format!("{:?}", err)))?;
                    self.mutation_events.push(MutationEvent::Rejected {
                        mutation_id,
                        rejection: rejection.clone(),
                    });
                }
            }
        }

        Ok(())
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
}

fn matches_rejection(outcome: &MutationOutcomeState, rejection: &MutationRejection) -> bool {
    matches!(outcome, MutationOutcomeState::Rejected(existing) if existing == rejection)
}
