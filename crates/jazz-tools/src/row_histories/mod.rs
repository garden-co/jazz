//! Row-history data types, codecs, and apply algorithms.
//!
//! Split into three submodules so each piece is independently navigable:
//! - [`types`]: data types (BatchId, RowState, QueryRowBatch, StoredRowBatch,
//!   RowMetadata, VisibleRowEntry, error types)
//! - [`codecs`]: descriptor builders and flat-row encode/decode
//! - [`apply`]: `apply_row_batch`, `patch_row_batch_state`, and the visible-row
//!   computation helpers they depend on

mod apply;
mod codecs;
mod types;

pub(crate) use apply::visible_row_preview_from_history_rows;
pub use apply::{apply_row_batch, patch_row_batch_state};
pub(crate) use codecs::{
    FlatRowCodecs, decode_flat_history_row_with_codecs, decode_flat_visible_row_entry_with_codecs,
    flat_row_codecs,
};
pub use codecs::{
    compute_row_digest, decode_flat_history_row, decode_flat_visible_row_entry,
    encode_flat_history_row, encode_flat_visible_row_entry, history_row_physical_descriptor,
    visible_row_physical_descriptor,
};
pub use types::{
    ApplyRowBatchResult, BatchId, HistoryScan, QueryRowBatch, RowHistoryError, RowMetadata,
    RowState, RowVisibilityChange, StoredRowBatch, VisibleRowEntry,
};

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use uuid::Uuid;

    use super::*;
    use crate::metadata::{DeleteKind, RowProvenance};
    use crate::object::ObjectId;
    use crate::query_manager::types::{
        ColumnDescriptor, ColumnMergeStrategy, ColumnType, RowDescriptor, Value,
    };
    use crate::row_format::{decode_row, encode_row};
    use crate::sync_manager::DurabilityTier;

    fn visible_row(updated_at: u64, confirmed_tier: Option<DurabilityTier>) -> StoredRowBatch {
        StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            vec![updated_at as u8],
            RowProvenance::for_insert("alice".to_string(), updated_at),
            HashMap::new(),
            RowState::VisibleDirect,
            confirmed_tier,
        )
    }

    #[test]
    fn flat_visible_row_binary_roundtrips_retained_visible_columns() {
        let user_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("done", ColumnType::Boolean).nullable(),
        ]);
        let global = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(21)),
            "main",
            Vec::new(),
            encode_row(
                &user_descriptor,
                &[Value::Text("ship it".into()), Value::Boolean(true)],
            )
            .expect("encode global row"),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::from([("source".to_string(), "global".to_string())]),
            RowState::VisibleDirect,
            Some(DurabilityTier::GlobalServer),
        );
        let current = StoredRowBatch::new(
            global.row_id,
            "main",
            vec![global.batch_id()],
            encode_row(
                &user_descriptor,
                &[Value::Text("ship it".into()), Value::Boolean(false)],
            )
            .expect("encode current row"),
            RowProvenance::for_update(&global.row_provenance(), "bob".to_string(), 30),
            HashMap::from([("source".to_string(), "local".to_string())]),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let entry = VisibleRowEntry {
            current_row: current,
            branch_frontier: vec![global.batch_id()],
            worker_batch_id: None,
            edge_batch_id: Some(global.batch_id()),
            global_batch_id: Some(global.batch_id()),
            winner_batch_pool: Vec::new(),
            current_winner_ordinals: None,
            worker_winner_ordinals: None,
            edge_winner_ordinals: None,
            global_winner_ordinals: None,
            merge_artifacts: Some(vec![1, 2, 3, 4]),
        };

        let encoded =
            encode_flat_visible_row_entry(&user_descriptor, &entry).expect("encode flat visible");
        let decoded = decode_flat_visible_row_entry(
            &user_descriptor,
            entry.current_row.row_id,
            entry.current_row.branch.as_str(),
            &encoded,
        )
        .expect("decode flat visible");

        assert_eq!(decoded.current_row.row_id, entry.current_row.row_id);
        assert_eq!(decoded.current_row.batch_id(), entry.current_row.batch_id());
        assert_eq!(decoded.current_row.branch, entry.current_row.branch);
        assert!(decoded.current_row.parents.is_empty());
        assert_eq!(decoded.current_row.updated_at, entry.current_row.updated_at);
        assert_eq!(decoded.current_row.created_by, entry.current_row.created_by);
        assert_eq!(decoded.current_row.created_at, entry.current_row.created_at);
        assert_eq!(decoded.current_row.updated_by, entry.current_row.updated_by);
        assert_eq!(decoded.current_row.state, entry.current_row.state);
        assert_eq!(
            decoded.current_row.confirmed_tier,
            entry.current_row.confirmed_tier
        );
        assert_eq!(
            decoded.current_row.delete_kind,
            entry.current_row.delete_kind
        );
        assert!(decoded.current_row.metadata.is_empty());
        assert_eq!(decoded.current_row.data, entry.current_row.data);
        assert_eq!(decoded.branch_frontier, entry.branch_frontier);
        assert_eq!(decoded.worker_batch_id, entry.worker_batch_id);
        assert_eq!(decoded.edge_batch_id, entry.edge_batch_id);
        assert_eq!(decoded.global_batch_id, entry.global_batch_id);
        assert_eq!(decoded.merge_artifacts, entry.merge_artifacts);
    }

    #[test]
    fn visible_row_entry_omits_tier_pointers_when_current_is_globally_confirmed() {
        let current = visible_row(30, Some(DurabilityTier::GlobalServer));
        let entry = VisibleRowEntry::rebuild(current.clone(), std::slice::from_ref(&current));

        assert_eq!(entry.branch_frontier, vec![current.batch_id()]);
        assert_eq!(entry.worker_batch_id, None);
        assert_eq!(entry.edge_batch_id, None);
        assert_eq!(entry.global_batch_id, None);
        assert_eq!(entry.merge_artifacts, None);
    }

    #[test]
    fn visible_row_entry_resolves_tier_fallback_chain() {
        let global = visible_row(10, Some(DurabilityTier::GlobalServer));
        let edge = StoredRowBatch::new(
            global.row_id,
            "main",
            vec![global.batch_id()],
            vec![2],
            RowProvenance::for_update(&global.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::EdgeServer),
        );
        let current = StoredRowBatch::new(
            global.row_id,
            "main",
            vec![edge.batch_id()],
            vec![3],
            RowProvenance::for_update(&edge.row_provenance(), "alice".to_string(), 30),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let history = vec![global.clone(), edge.clone(), current.clone()];

        let entry = VisibleRowEntry::rebuild(current.clone(), &history);

        assert_eq!(entry.branch_frontier, vec![current.batch_id()]);
        assert_eq!(entry.worker_batch_id, None);
        assert_eq!(entry.edge_batch_id, Some(edge.batch_id()));
        assert_eq!(entry.global_batch_id, Some(global.batch_id()));
        assert_eq!(
            entry.batch_id_for_tier(DurabilityTier::Local),
            Some(current.batch_id())
        );
        assert_eq!(
            entry.batch_id_for_tier(DurabilityTier::EdgeServer),
            Some(edge.batch_id())
        );
        assert_eq!(
            entry.batch_id_for_tier(DurabilityTier::GlobalServer),
            Some(global.batch_id())
        );
    }

    #[test]
    fn visible_row_entry_returns_none_when_no_version_meets_required_tier() {
        let current = visible_row(30, Some(DurabilityTier::Local));
        let entry = VisibleRowEntry::rebuild(current.clone(), std::slice::from_ref(&current));

        assert_eq!(entry.branch_frontier, vec![current.batch_id()]);
        assert_eq!(entry.batch_id_for_tier(DurabilityTier::EdgeServer), None);
        assert_eq!(entry.batch_id_for_tier(DurabilityTier::GlobalServer), None);
    }

    #[test]
    fn visible_row_entry_preserves_multiple_branch_tips() {
        let base = visible_row(10, Some(DurabilityTier::Local));
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            vec![1],
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            vec![2],
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let entry = VisibleRowEntry::rebuild(right.clone(), &[base, left.clone(), right.clone()]);

        assert_eq!(
            entry.branch_frontier,
            vec![left.batch_id(), right.batch_id()]
        );
    }

    #[test]
    fn visible_row_entry_merges_conflicting_field_updates() {
        let descriptor = user_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("alice-title".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let entry = VisibleRowEntry::rebuild_with_descriptor(
            &descriptor,
            &[base, left.clone(), right.clone()],
        )
        .unwrap()
        .expect("merged visible entry");

        assert_eq!(
            decode_row(&descriptor, &entry.current_row.data).unwrap(),
            vec![Value::Text("alice-title".into()), Value::Boolean(true)]
        );
        assert_eq!(entry.current_row.batch_id(), right.batch_id());
        assert_eq!(entry.current_row.updated_by.as_str(), "bob");
        assert_eq!(
            entry.branch_frontier,
            vec![left.batch_id(), right.batch_id()]
        );
    }

    #[test]
    fn visible_row_entry_applies_counter_merge_strategy_per_column() {
        let descriptor = counter_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Integer(5)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("alice-title".into()), Value::Integer(7)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Integer(4)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let entry = VisibleRowEntry::rebuild_with_descriptor(
            &descriptor,
            &[base, left.clone(), right.clone()],
        )
        .unwrap()
        .expect("merged visible entry");

        assert_eq!(
            decode_row(&descriptor, &entry.current_row.data).unwrap(),
            vec![Value::Text("alice-title".into()), Value::Integer(6)]
        );
        assert_eq!(entry.current_row.batch_id(), right.batch_id());
        assert_eq!(entry.current_row.updated_by.as_str(), "bob");
        assert_eq!(
            entry.branch_frontier,
            vec![left.batch_id(), right.batch_id()]
        );
        assert_eq!(
            entry.winner_batch_pool,
            vec![left.batch_id(), right.batch_id()]
        );
        assert_eq!(entry.current_winner_ordinals, Some(vec![0, 1]));
    }

    #[test]
    fn visible_row_entry_uses_consumer_schema_merge_strategy() {
        let counter_descriptor = counter_descriptor();
        let lww_descriptor = lww_integer_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &counter_descriptor,
                &[Value::Text("task".into()), Value::Integer(5)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &counter_descriptor,
                &[Value::Text("alice-title".into()), Value::Integer(7)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &counter_descriptor,
                &[Value::Text("task".into()), Value::Integer(4)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let history = vec![base, left, right];

        let counter_entry = VisibleRowEntry::rebuild_with_descriptor(&counter_descriptor, &history)
            .unwrap()
            .expect("counter merged visible entry");
        let lww_entry = VisibleRowEntry::rebuild_with_descriptor(&lww_descriptor, &history)
            .unwrap()
            .expect("lww merged visible entry");

        assert_eq!(
            decode_row(&counter_descriptor, &counter_entry.current_row.data).unwrap(),
            vec![Value::Text("alice-title".into()), Value::Integer(6)]
        );
        assert_eq!(
            decode_row(&lww_descriptor, &lww_entry.current_row.data).unwrap(),
            vec![Value::Text("alice-title".into()), Value::Integer(4)]
        );
    }

    #[test]
    fn visible_row_entry_errors_when_counter_merge_overflows() {
        let descriptor = counter_only_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(&descriptor, &[Value::Integer(i32::MAX - 1)]).unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(&descriptor, &[Value::Integer(i32::MAX)]).unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(&descriptor, &[Value::Integer(i32::MAX)]).unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let error = VisibleRowEntry::rebuild_with_descriptor(&descriptor, &[base, left, right])
            .expect_err("counter overflow should fail");

        assert!(
            error.to_string().contains("overflow"),
            "expected overflow error, got {error}"
        );
    }

    #[test]
    fn visible_row_entry_merges_accepted_transactional_rows_but_ignores_staging_and_rejected_rows()
    {
        let descriptor = user_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let accepted_transaction = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("txn-title".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleTransactional,
            Some(DurabilityTier::Local),
        );
        let direct = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let staging = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[
                    Value::Text("staging-should-not-win".into()),
                    Value::Boolean(false),
                ],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "mallory".to_string(), 30),
            HashMap::new(),
            RowState::StagingPending,
            None,
        );
        let rejected = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[
                    Value::Text("rejected-should-not-win".into()),
                    Value::Boolean(false),
                ],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "mallory".to_string(), 31),
            HashMap::new(),
            RowState::Rejected,
            None,
        );

        let entry = VisibleRowEntry::rebuild_with_descriptor(
            &descriptor,
            &[
                base,
                accepted_transaction,
                direct.clone(),
                staging,
                rejected,
            ],
        )
        .unwrap()
        .expect("merged visible entry");

        assert_eq!(
            decode_row(&descriptor, &entry.current_row.data).unwrap(),
            vec![Value::Text("txn-title".into()), Value::Boolean(true)]
        );
        assert_eq!(entry.current_row.batch_id(), direct.batch_id());
        assert_eq!(entry.current_row.updated_by.as_str(), "bob");
    }

    #[test]
    fn visible_row_entry_roundtrips_current_winner_ordinals() {
        let descriptor = user_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("alice-title".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let entry = VisibleRowEntry::rebuild_with_descriptor(
            &descriptor,
            &[base, left.clone(), right.clone()],
        )
        .unwrap()
        .expect("merged visible entry");
        assert_eq!(
            entry.winner_batch_pool,
            vec![left.batch_id(), right.batch_id()]
        );
        assert_eq!(entry.current_winner_ordinals, Some(vec![0, 1]));

        let encoded =
            encode_flat_visible_row_entry(&descriptor, &entry).expect("encode merged visible row");
        let decoded = decode_flat_visible_row_entry(
            &descriptor,
            entry.current_row.row_id,
            entry.current_row.branch.as_str(),
            &encoded,
        )
        .expect("decode merged visible row");

        assert_eq!(decoded.winner_batch_pool, entry.winner_batch_pool);
        assert_eq!(
            decoded.current_winner_ordinals,
            entry.current_winner_ordinals
        );
        assert_eq!(decoded.edge_winner_ordinals, None);
        assert_eq!(decoded.global_winner_ordinals, None);
    }

    #[test]
    fn visible_row_entry_materializes_tier_preview_when_batch_id_matches_current() {
        let descriptor = user_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::GlobalServer),
        );
        let worker_done = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let edge_title = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("edge-title".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 30),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::EdgeServer),
        );
        let history_rows = vec![base.clone(), worker_done.clone(), edge_title.clone()];
        let row_by_batch_id = history_rows
            .iter()
            .cloned()
            .map(|row| (row.batch_id(), row))
            .collect::<HashMap<_, _>>();

        let entry = VisibleRowEntry::rebuild_with_descriptor(&descriptor, &history_rows)
            .unwrap()
            .expect("visible entry");

        assert_eq!(entry.current_row.batch_id(), edge_title.batch_id());
        assert_eq!(entry.edge_batch_id, Some(edge_title.batch_id()));
        assert_eq!(entry.edge_winner_ordinals, None);

        let edge_preview = entry
            .materialize_preview_for_tier_from_loaded_rows(
                &descriptor,
                DurabilityTier::EdgeServer,
                &row_by_batch_id,
            )
            .unwrap()
            .expect("edge preview");
        assert_eq!(
            decode_row(&descriptor, &edge_preview.data).unwrap(),
            vec![Value::Text("edge-title".into()), Value::Boolean(false)]
        );
    }

    #[test]
    fn visible_row_entry_persists_merged_tier_override_ordinals() {
        let descriptor = user_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::GlobalServer),
        );
        let edge_title = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("edge-title".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::EdgeServer),
        );
        let edge_done = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::EdgeServer),
        );
        let worker_current = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![edge_title.batch_id(), edge_done.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("edge-title".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&edge_done.row_provenance(), "charlie".to_string(), 30),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let history_rows = vec![
            base.clone(),
            edge_title.clone(),
            edge_done.clone(),
            worker_current.clone(),
        ];
        let row_by_batch_id = history_rows
            .iter()
            .cloned()
            .map(|row| (row.batch_id(), row))
            .collect::<HashMap<_, _>>();

        let entry = VisibleRowEntry::rebuild_with_descriptor(&descriptor, &history_rows)
            .unwrap()
            .expect("visible entry");

        assert_eq!(entry.current_row.batch_id(), worker_current.batch_id());
        assert_eq!(entry.current_winner_ordinals, None);
        assert_eq!(entry.edge_batch_id, Some(edge_done.batch_id()));
        assert_eq!(
            entry.winner_batch_pool,
            vec![edge_title.batch_id(), edge_done.batch_id()]
        );
        assert_eq!(entry.edge_winner_ordinals, Some(vec![0, 1]));

        let edge_preview = entry
            .materialize_preview_for_tier_from_loaded_rows(
                &descriptor,
                DurabilityTier::EdgeServer,
                &row_by_batch_id,
            )
            .unwrap()
            .expect("edge preview");
        assert_eq!(edge_preview.batch_id(), edge_done.batch_id());
        assert_eq!(
            decode_row(&descriptor, &edge_preview.data).unwrap(),
            vec![Value::Text("edge-title".into()), Value::Boolean(true)]
        );
    }

    fn user_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("done", ColumnType::Boolean),
        ])
    }

    fn lww_integer_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("count", ColumnType::Integer),
        ])
    }

    fn counter_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("count", ColumnType::Integer)
                .merge_strategy(ColumnMergeStrategy::Counter),
        ])
    }

    fn counter_only_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("count", ColumnType::Integer)
                .merge_strategy(ColumnMergeStrategy::Counter),
        ])
    }

    #[test]
    fn history_row_physical_descriptor_appends_nullable_user_columns() {
        let descriptor = history_row_physical_descriptor(&user_descriptor());

        let title = descriptor
            .column("title")
            .expect("physical descriptor should contain title");
        assert!(title.nullable, "physical user columns should be nullable");

        let done = descriptor
            .column("done")
            .expect("physical descriptor should contain done");
        assert!(done.nullable, "physical user columns should be nullable");
    }

    #[test]
    fn history_row_physical_descriptor_omits_key_derived_and_marker_columns() {
        let descriptor = history_row_physical_descriptor(&user_descriptor());

        assert_eq!(
            descriptor
                .columns
                .iter()
                .filter(|column| column.name == "_jazz_batch_id")
                .count(),
            0,
            "flat history rows should not store batch identity from the key in the payload"
        );
        assert!(
            descriptor.column("_jazz_format_id").is_none(),
            "flat history rows should not need an in-payload format marker once decoding is key-aware"
        );
        assert!(
            descriptor.column("_jazz_row_id").is_none(),
            "flat history rows should not store row id from the key in the payload"
        );
        assert!(
            descriptor.column("_jazz_branch").is_none(),
            "flat history rows should not store branch from the key in the payload"
        );
    }

    #[test]
    fn visible_row_physical_descriptor_keeps_current_batch_id_but_omits_marker() {
        let descriptor = visible_row_physical_descriptor(&user_descriptor());

        assert!(
            descriptor.column("_jazz_format_id").is_none(),
            "visible rows should not need an in-payload format marker once keyed decoding is available"
        );
        assert_eq!(
            descriptor
                .columns
                .iter()
                .filter(|column| column.name == "_jazz_batch_id")
                .count(),
            1,
            "visible rows should keep the current visible batch id in the flat payload"
        );
        assert!(
            descriptor.column("_jazz_row_id").is_none(),
            "visible rows should derive row id from the storage key"
        );
        assert!(
            descriptor.column("_jazz_branch").is_none(),
            "visible rows should derive branch from the storage key"
        );
        assert!(
            descriptor.column("_jazz_parents").is_none(),
            "visible rows should not duplicate history parents in the hot visible payload"
        );
        assert!(
            descriptor.column("_jazz_metadata").is_none(),
            "visible rows should not duplicate history metadata in the hot visible payload"
        );
        assert!(
            descriptor.column("_jazz_is_deleted").is_none(),
            "visible rows should derive deletion state from delete_kind in the hot payload"
        );
    }

    #[test]
    fn flat_visible_row_common_case_omits_empty_arrays_and_metadata() {
        let descriptor = user_descriptor();
        let current = visible_row(10, Some(DurabilityTier::Local));
        let entry = VisibleRowEntry::rebuild(current.clone(), std::slice::from_ref(&current));

        let encoded =
            encode_flat_visible_row_entry(&descriptor, &entry).expect("encode visible row");
        let values = decode_row(&visible_row_physical_descriptor(&descriptor), &encoded)
            .expect("decode visible row");

        assert_eq!(
            values[8],
            Value::Null,
            "singleton frontier matching current batch should be implicit"
        );
    }

    #[test]
    fn flat_history_row_binary_roundtrips_user_and_system_columns() {
        let user_descriptor = user_descriptor();
        let user_values = vec![Value::Text("Write docs".into()), Value::Boolean(false)];
        let user_data = crate::row_format::encode_row(&user_descriptor, &user_values).unwrap();
        let row = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(42)),
            "main",
            vec![BatchId([9; 16])],
            user_data.clone(),
            RowProvenance {
                created_by: "alice".to_string(),
                created_at: 100,
                updated_by: "bob".to_string(),
                updated_at: 123,
            },
            HashMap::from([("source".to_string(), "test".to_string())]),
            RowState::VisibleTransactional,
            Some(DurabilityTier::EdgeServer),
        );

        let encoded =
            encode_flat_history_row(&user_descriptor, &row).expect("encode flat history row");
        let decoded = decode_flat_history_row(
            &user_descriptor,
            row.row_id,
            row.branch.as_str(),
            row.batch_id(),
            &encoded,
        )
        .expect("decode flat history row");

        assert_eq!(decoded, row);

        let physical_descriptor = history_row_physical_descriptor(&user_descriptor);
        let physical_values = decode_row(&physical_descriptor, &encoded).expect("decode values");
        assert_eq!(
            physical_values[physical_descriptor.column_index("title").unwrap()],
            Value::Text("Write docs".into())
        );
        assert_eq!(
            physical_values[physical_descriptor.column_index("done").unwrap()],
            Value::Boolean(false)
        );
    }

    #[test]
    fn flat_history_row_binary_roundtrips_nonempty_metadata() {
        let user_descriptor = user_descriptor();
        let row = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(44)),
            "main",
            vec![BatchId([3; 16])],
            encode_row(
                &user_descriptor,
                &[Value::Text("Ship".into()), Value::Boolean(true)],
            )
            .expect("encode user row"),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::from([
                ("source".to_string(), "local".to_string()),
                ("kind".to_string(), "task".to_string()),
            ]),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let encoded =
            encode_flat_history_row(&user_descriptor, &row).expect("encode flat history row");
        let decoded = decode_flat_history_row(
            &user_descriptor,
            row.row_id,
            row.branch.as_str(),
            row.batch_id(),
            &encoded,
        )
        .expect("decode flat history row");

        assert_eq!(decoded.metadata, row.metadata);
    }

    #[test]
    fn flat_history_row_hard_delete_uses_null_user_columns() {
        let user_descriptor = user_descriptor();
        let deleted = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(43)),
            "main",
            vec![BatchId([7; 16])],
            vec![],
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::from([(
                crate::metadata::MetadataKey::Delete.to_string(),
                "hard".to_string(),
            )]),
            RowState::VisibleDirect,
            None,
        );

        let encoded =
            encode_flat_history_row(&user_descriptor, &deleted).expect("encode hard delete");
        let physical_descriptor = history_row_physical_descriptor(&user_descriptor);
        let physical_values = decode_row(&physical_descriptor, &encoded).expect("decode values");

        assert_eq!(
            physical_values[physical_descriptor.column_index("title").unwrap()],
            Value::Null
        );
        assert_eq!(
            physical_values[physical_descriptor.column_index("done").unwrap()],
            Value::Null
        );

        let decoded = decode_flat_history_row(
            &user_descriptor,
            deleted.row_id,
            deleted.branch.as_str(),
            deleted.batch_id(),
            &encoded,
        )
        .expect("decode hard delete");
        assert_eq!(decoded.data.as_ref(), &[] as &[u8]);
        assert!(decoded.is_hard_deleted());
    }

    #[test]
    fn flat_history_row_binary_compacts_hot_enums_to_single_bytes() {
        let user_descriptor = user_descriptor();
        let mut row = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(45)),
            "main",
            vec![BatchId([4; 16])],
            encode_row(
                &user_descriptor,
                &[Value::Text("Compact".into()), Value::Boolean(false)],
            )
            .expect("encode user row"),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::new(),
            RowState::VisibleTransactional,
            Some(DurabilityTier::EdgeServer),
        );
        row.delete_kind = Some(DeleteKind::Hard);

        let encoded =
            encode_flat_history_row(&user_descriptor, &row).expect("encode flat history row");
        let descriptor = history_row_physical_descriptor(&user_descriptor);
        let layout = crate::row_format::compiled_row_layout(&descriptor);

        let state = crate::row_format::column_bytes_with_layout(
            &descriptor,
            layout.as_ref(),
            &encoded,
            descriptor.column_index("_jazz_state").unwrap(),
        )
        .expect("read state bytes")
        .expect("state should be present");
        let tier = crate::row_format::column_bytes_with_layout(
            &descriptor,
            layout.as_ref(),
            &encoded,
            descriptor.column_index("_jazz_confirmed_tier").unwrap(),
        )
        .expect("read tier bytes")
        .expect("tier should be present");
        let delete_kind = crate::row_format::column_bytes_with_layout(
            &descriptor,
            layout.as_ref(),
            &encoded,
            descriptor.column_index("_jazz_delete_kind").unwrap(),
        )
        .expect("read delete kind bytes")
        .expect("delete kind should be present");

        assert_eq!(state.len(), 1);
        assert_eq!(tier.len(), 1);
        assert_eq!(delete_kind.len(), 1);
    }

    #[test]
    fn direct_row_writes_use_batch_identity() {
        let provenance = RowProvenance::for_insert("alice".to_string(), 100);
        let first = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(101)),
            "main",
            Vec::new(),
            vec![1, 2, 3],
            provenance.clone(),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        assert_eq!(
            first.batch_id(),
            first.batch_id,
            "direct visible rows should publish under their batch identity"
        );
    }
}
