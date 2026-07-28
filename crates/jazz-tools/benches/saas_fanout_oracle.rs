//! Independent document-page oracle and subscription audit helpers for SaaS
//! fan-out benchmarks.
//!
//! The oracle deliberately knows nothing about Jazz query evaluation or row
//! policies. It orders deterministic document metadata by
//! `(updated_at DESC, row_uuid DESC)`, keeps active/draft unarchived rows, and
//! caps pages at 100 rows. Callers may apply an additional predicate (or supply
//! an entirely caller-computed expected page) for permission branches.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use jazz::db::{SubscriptionEvent, SubscriptionStream};
use jazz::ids::RowUuid;
use jazz::tx::DurabilityTier;

/// The SaaS list page size exercised by the fan-out benchmark.
pub const TOP_PAGE_SIZE: usize = 100;

/// Compact status metadata used by the independent oracle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocumentStatus {
    Active,
    Draft,
    Other,
}

impl DocumentStatus {
    /// Convert fixture status text without retaining an allocated string per
    /// document.
    pub fn from_label(label: &str) -> Self {
        match label {
            "active" => Self::Active,
            "draft" => Self::Draft,
            _ => Self::Other,
        }
    }

    fn appears_in_document_list(self) -> bool {
        matches!(self, Self::Active | Self::Draft)
    }
}

/// Metadata sufficient to compute a team document-list page independently of
/// Jazz.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DocumentMetadata {
    pub row_uuid: RowUuid,
    pub team: RowUuid,
    pub updated_at: u64,
    pub status: DocumentStatus,
    pub archived: bool,
}

impl DocumentMetadata {
    pub fn new(
        row_uuid: RowUuid,
        team: RowUuid,
        updated_at: u64,
        status: DocumentStatus,
        archived: bool,
    ) -> Self {
        Self {
            row_uuid,
            team,
            updated_at,
            status,
            archived,
        }
    }

    /// The non-permission portion of the benchmark list predicate.
    pub fn appears_in_document_list(&self) -> bool {
        !self.archived && self.status.appears_in_document_list()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RankKey {
    updated_at: u64,
    row_uuid: RowUuid,
}

impl From<DocumentMetadata> for RankKey {
    fn from(document: DocumentMetadata) -> Self {
        Self {
            updated_at: document.updated_at,
            row_uuid: document.row_uuid,
        }
    }
}

/// An ordered, duplicate-free expected page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExpectedPage {
    rows: Vec<RowUuid>,
}

impl ExpectedPage {
    /// Validate a caller-provided page. This is intentionally public so policy
    /// fixtures can compute their own visible page and still use the same event
    /// auditing helpers.
    pub fn new(rows: impl IntoIterator<Item = RowUuid>) -> Result<Self, String> {
        let rows = rows.into_iter().collect::<Vec<_>>();
        if rows.len() > TOP_PAGE_SIZE {
            return Err(format!(
                "expected page has {} rows, exceeding Top-{TOP_PAGE_SIZE}",
                rows.len()
            ));
        }
        let unique = rows.iter().copied().collect::<BTreeSet<_>>();
        if unique.len() != rows.len() {
            return Err("expected page contains duplicate row ids".to_owned());
        }
        Ok(Self { rows })
    }

    fn known_valid(rows: Vec<RowUuid>) -> Self {
        debug_assert!(rows.len() <= TOP_PAGE_SIZE);
        debug_assert_eq!(
            rows.iter().copied().collect::<BTreeSet<_>>().len(),
            rows.len()
        );
        Self { rows }
    }

    pub fn rows(&self) -> &[RowUuid] {
        &self.rows
    }

    pub fn row_set(&self) -> BTreeSet<RowUuid> {
        self.rows.iter().copied().collect()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl AsRef<[RowUuid]> for ExpectedPage {
    fn as_ref(&self) -> &[RowUuid] {
        self.rows()
    }
}

/// Exact before/after membership change for one expected page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageTransition {
    pub before: ExpectedPage,
    pub after: ExpectedPage,
    pub added: BTreeSet<RowUuid>,
    pub removed: BTreeSet<RowUuid>,
}

impl PageTransition {
    pub fn between(before: ExpectedPage, after: ExpectedPage) -> Self {
        let before_rows = before.row_set();
        let after_rows = after.row_set();
        let added = after_rows.difference(&before_rows).copied().collect();
        let removed = before_rows.difference(&after_rows).copied().collect();
        Self {
            before,
            after,
            added,
            removed,
        }
    }
}

/// Deterministic per-team document oracle.
///
/// It stores only teams/documents inserted by the caller. A large benchmark can
/// therefore seed the oracle for subscribed teams only, avoiding another full
/// copy of a 500k-row database fixture.
#[derive(Debug, Default)]
pub struct PerTeamTop100Oracle {
    documents: BTreeMap<RowUuid, DocumentMetadata>,
    ranked_by_team: BTreeMap<RowUuid, BTreeSet<RankKey>>,
}

impl PerTeamTop100Oracle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or replace document metadata. Moving an existing row between
    /// teams or changing its ordering key updates both indexes.
    pub fn upsert(&mut self, document: DocumentMetadata) -> Option<DocumentMetadata> {
        let previous = self.documents.insert(document.row_uuid, document);
        if let Some(previous) = previous {
            self.remove_rank(previous);
        }
        self.ranked_by_team
            .entry(document.team)
            .or_default()
            .insert(document.into());
        previous
    }

    pub fn remove(&mut self, row_uuid: RowUuid) -> Option<DocumentMetadata> {
        let document = self.documents.remove(&row_uuid)?;
        self.remove_rank(document);
        Some(document)
    }

    pub fn document(&self, row_uuid: RowUuid) -> Option<&DocumentMetadata> {
        self.documents.get(&row_uuid)
    }

    pub fn len(&self) -> usize {
        self.documents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    pub fn team_document_count(&self, team: RowUuid) -> usize {
        self.ranked_by_team.get(&team).map_or(0, BTreeSet::len)
    }

    /// Compute the list page before row-policy restrictions.
    pub fn page(&self, team: RowUuid) -> ExpectedPage {
        self.page_where(team, |_| true)
    }

    /// Compute the list page with a caller-provided permission predicate.
    ///
    /// The active/draft and unarchived list predicate is always applied first.
    /// The extra predicate can model any policy fixture expressible from the
    /// metadata known to the caller.
    pub fn page_where(
        &self,
        team: RowUuid,
        mut permission_visible: impl FnMut(&DocumentMetadata) -> bool,
    ) -> ExpectedPage {
        let Some(ranked) = self.ranked_by_team.get(&team) else {
            return ExpectedPage::default();
        };
        let rows = ranked
            .iter()
            .rev()
            .filter_map(|key| self.documents.get(&key.row_uuid))
            .filter(|document| document.appears_in_document_list() && permission_visible(document))
            .take(TOP_PAGE_SIZE)
            .map(|document| document.row_uuid)
            .collect();
        ExpectedPage::known_valid(rows)
    }

    /// Convenience wrapper for a permission oracle that has already computed
    /// the visible row ids.
    pub fn page_restricted_to(
        &self,
        team: RowUuid,
        visible_rows: &BTreeSet<RowUuid>,
    ) -> ExpectedPage {
        self.page_where(team, |document| visible_rows.contains(&document.row_uuid))
    }

    fn remove_rank(&mut self, document: DocumentMetadata) {
        let mut remove_team = false;
        if let Some(ranked) = self.ranked_by_team.get_mut(&document.team) {
            ranked.remove(&document.into());
            remove_team = ranked.is_empty();
        }
        if remove_team {
            self.ranked_by_team.remove(&document.team);
        }
    }
}

/// Root-row membership reconstructed from subscription events.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ObservedPage {
    rows: BTreeSet<RowUuid>,
}

impl ObservedPage {
    pub fn from_expected(expected: impl AsRef<[RowUuid]>) -> Result<Self, String> {
        let expected = ExpectedPage::new(expected.as_ref().iter().copied())?;
        Ok(Self {
            rows: expected.row_set(),
        })
    }

    pub fn rows(&self) -> &BTreeSet<RowUuid> {
        &self.rows
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn assert_matches(
        &self,
        label: &str,
        expected: impl AsRef<[RowUuid]>,
    ) -> Result<(), String> {
        let expected = ExpectedPage::new(expected.as_ref().iter().copied())?;
        let expected_rows = expected.row_set();
        if self.rows == expected_rows {
            return Ok(());
        }
        let missing = expected_rows
            .difference(&self.rows)
            .copied()
            .collect::<Vec<_>>();
        let unexpected = self
            .rows
            .difference(&expected_rows)
            .copied()
            .collect::<Vec<_>>();
        Err(format!(
            "{label} snapshot mismatch: actual={}, expected={}, missing={missing:?}, unexpected={unexpected:?}",
            self.rows.len(),
            expected_rows.len()
        ))
    }
}

/// One normalized root-row delta.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeltaReceipt {
    pub reset: bool,
    /// Preserves producer order for diagnostics; reset membership is set-like.
    pub added: Vec<RowUuid>,
    pub updated: Vec<RowUuid>,
    pub removed: Vec<RowUuid>,
    pub settled: bool,
    pub tier: DurabilityTier,
}

impl DeltaReceipt {
    pub fn added_set(&self) -> BTreeSet<RowUuid> {
        self.added.iter().copied().collect()
    }

    pub fn updated_set(&self) -> BTreeSet<RowUuid> {
        self.updated.iter().copied().collect()
    }

    pub fn removed_set(&self) -> BTreeSet<RowUuid> {
        self.removed.iter().copied().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.updated.is_empty() && self.removed.is_empty()
    }
}

/// Exact sequence of queued deltas observed during one drain.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EventReceipt {
    pub deltas: Vec<DeltaReceipt>,
}

impl EventReceipt {
    pub fn event_count(&self) -> usize {
        self.deltas.len()
    }

    pub fn reset_count(&self) -> usize {
        self.deltas.iter().filter(|delta| delta.reset).count()
    }

    pub fn added_count(&self) -> usize {
        self.deltas.iter().map(|delta| delta.added.len()).sum()
    }

    pub fn updated_count(&self) -> usize {
        self.deltas.iter().map(|delta| delta.updated.len()).sum()
    }

    pub fn removed_count(&self) -> usize {
        self.deltas.iter().map(|delta| delta.removed.len()).sum()
    }

    pub fn is_quiet(&self) -> bool {
        self.deltas.is_empty()
    }
}

/// Initial event validation result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedInitialReset {
    pub observed: ObservedPage,
    pub receipt: DeltaReceipt,
}

/// Consume and validate the immediately queued initial reset.
pub fn take_initial_reset(
    label: &str,
    stream: &mut SubscriptionStream,
    expected_page: impl AsRef<[RowUuid]>,
) -> Result<ValidatedInitialReset, String> {
    let event = stream
        .try_next_event()
        .ok_or_else(|| format!("{label} subscription did not queue an initial reset"))?;
    validate_initial_reset(label, event, expected_page)
}

/// Validate an initial reset against any caller-provided expected page.
pub fn validate_initial_reset(
    label: &str,
    event: SubscriptionEvent,
    expected_page: impl AsRef<[RowUuid]>,
) -> Result<ValidatedInitialReset, String> {
    let expected = ExpectedPage::new(expected_page.as_ref().iter().copied())?;
    let receipt = decode_flat_document_delta(label, event)?;
    if !receipt.reset {
        return Err(format!(
            "{label} initial event was not a reset: {receipt:?}"
        ));
    }
    if !receipt.updated.is_empty() || !receipt.removed.is_empty() {
        return Err(format!(
            "{label} initial reset contained updated/removed rows: {receipt:?}"
        ));
    }
    let actual_rows = receipt.added_set();
    let expected_rows = expected.row_set();
    if actual_rows != expected_rows {
        let missing = expected_rows
            .difference(&actual_rows)
            .copied()
            .take(8)
            .collect::<Vec<_>>();
        let unexpected = actual_rows
            .difference(&expected_rows)
            .copied()
            .take(8)
            .collect::<Vec<_>>();
        return Err(format!(
            "{label} initial reset membership mismatch: actual={}, expected={}, missing={missing:?}, unexpected={unexpected:?}",
            actual_rows.len(),
            expected_rows.len()
        ));
    }
    Ok(ValidatedInitialReset {
        observed: ObservedPage {
            rows: expected.row_set(),
        },
        receipt,
    })
}

/// Fold one event into a root-row snapshot and return its exact receipt.
pub fn fold_event(
    label: &str,
    observed: &mut ObservedPage,
    event: SubscriptionEvent,
) -> Result<DeltaReceipt, String> {
    let receipt = decode_flat_document_delta(label, event)?;
    if receipt.reset {
        if !receipt.removed.is_empty() || !receipt.updated.is_empty() {
            return Err(format!(
                "{label} reset contained updated/removed rows and cannot be folded exactly: {receipt:?}"
            ));
        }
        observed.rows.clear();
    }

    for row_uuid in &receipt.removed {
        if !observed.rows.remove(row_uuid) {
            return Err(format!(
                "{label} removed row {row_uuid:?} that was absent from the observed page"
            ));
        }
    }
    for row_uuid in &receipt.added {
        if !observed.rows.insert(*row_uuid) {
            return Err(format!(
                "{label} added row {row_uuid:?} that was already in the observed page"
            ));
        }
    }
    for row_uuid in &receipt.updated {
        if !observed.rows.contains(row_uuid) {
            return Err(format!(
                "{label} updated row {row_uuid:?} that was absent from the observed page"
            ));
        }
    }
    Ok(receipt)
}

/// Drain one stream without blocking.
pub fn drain_stream(
    label: &str,
    stream: &mut SubscriptionStream,
    observed: &mut ObservedPage,
) -> Result<EventReceipt, String> {
    let mut receipt = EventReceipt::default();
    while let Some(event) = stream.try_next_event() {
        receipt.deltas.push(fold_event(label, observed, event)?);
    }
    Ok(receipt)
}

/// One borrowed stream/snapshot pair for [`drain_streams`].
pub struct StreamAuditTarget<'a> {
    pub label: &'a str,
    pub stream: &'a mut SubscriptionStream,
    pub observed: &'a mut ObservedPage,
}

impl<'a> StreamAuditTarget<'a> {
    pub fn new(
        label: &'a str,
        stream: &'a mut SubscriptionStream,
        observed: &'a mut ObservedPage,
    ) -> Self {
        Self {
            label,
            stream,
            observed,
        }
    }
}

/// Receipt for one stream that emitted at least one event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotifiedStreamReceipt {
    pub index: usize,
    pub label: String,
    pub events: EventReceipt,
}

/// O(N) audit receipt. Quiet streams allocate no per-stream receipt.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamDrainReceipt {
    pub streams_scanned: usize,
    pub total_events: usize,
    pub notified: Vec<NotifiedStreamReceipt>,
}

impl StreamDrainReceipt {
    pub fn notified_streams(&self) -> usize {
        self.notified.len()
    }

    pub fn is_quiet(&self) -> bool {
        self.total_events == 0
    }

    pub fn receipt_for(&self, index: usize) -> Option<&EventReceipt> {
        self.notified
            .binary_search_by_key(&index, |receipt| receipt.index)
            .ok()
            .map(|position| &self.notified[position])
            .map(|receipt| &receipt.events)
    }

    pub fn assert_quiet(&self, label: &str) -> Result<(), String> {
        if self.is_quiet() {
            Ok(())
        } else {
            Err(format!(
                "{label} unexpectedly notified {} of {} streams with {} events: {:?}",
                self.notified_streams(),
                self.streams_scanned,
                self.total_events,
                self.notified
            ))
        }
    }
}

/// Drain each stream exactly once. Complexity is O(number of streams + queued
/// events), and only streams with events are retained in the returned receipt.
pub fn drain_streams<'a>(
    targets: impl IntoIterator<Item = StreamAuditTarget<'a>>,
) -> Result<StreamDrainReceipt, String> {
    let mut audit = StreamDrainReceipt::default();
    for (index, target) in targets.into_iter().enumerate() {
        audit.streams_scanned += 1;
        let events = drain_stream(target.label, target.stream, target.observed)?;
        audit.total_events += events.event_count();
        if !events.is_quiet() {
            audit.notified.push(NotifiedStreamReceipt {
                index,
                label: target.label.to_owned(),
                events,
            });
        }
    }
    Ok(audit)
}

fn decode_flat_document_delta(
    label: &str,
    event: SubscriptionEvent,
) -> Result<DeltaReceipt, String> {
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        added_related,
        added_edges,
        removed_edges,
        settled,
        tier,
    } = event
    else {
        return match event {
            SubscriptionEvent::Rejected { reason } => {
                Err(format!("{label} subscription was rejected: {reason:?}"))
            }
            SubscriptionEvent::Closed => Err(format!("{label} subscription closed unexpectedly")),
            SubscriptionEvent::Delta { .. } => unreachable!(),
        };
    };

    if !added_related.is_empty() || !added_edges.is_empty() || !removed_edges.is_empty() {
        return Err(format!(
            "{label} flat document subscription emitted relation payloads: related={}, added_edges={}, removed_edges={}",
            added_related.len(),
            added_edges.len(),
            removed_edges.len()
        ));
    }

    let added = added
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();
    let updated = updated
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();
    let removed = removed
        .into_iter()
        .map(|row| row.row_uuid)
        .collect::<Vec<_>>();

    ensure_unique(label, "added", &added)?;
    ensure_unique(label, "updated", &updated)?;
    ensure_unique(label, "removed", &removed)?;
    ensure_disjoint(label, "added", &added, "updated", &updated)?;
    ensure_disjoint(label, "added", &added, "removed", &removed)?;
    ensure_disjoint(label, "updated", &updated, "removed", &removed)?;

    Ok(DeltaReceipt {
        reset,
        added,
        updated,
        removed,
        settled,
        tier,
    })
}

fn ensure_unique(label: &str, field: &str, rows: &[RowUuid]) -> Result<(), String> {
    let unique = rows.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() == rows.len() {
        Ok(())
    } else {
        Err(format!(
            "{label} event contains duplicate {field} row ids: {rows:?}"
        ))
    }
}

fn ensure_disjoint(
    label: &str,
    left_name: &str,
    left: &[RowUuid],
    right_name: &str,
    right: &[RowUuid],
) -> Result<(), String> {
    let left = left.iter().copied().collect::<BTreeSet<_>>();
    let right = right.iter().copied().collect::<BTreeSet<_>>();
    let overlap = left.intersection(&right).copied().collect::<Vec<_>>();
    if overlap.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{label} event rows appear in both {left_name} and {right_name}: {overlap:?}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(tag: u8, index: u64) -> RowUuid {
        let mut bytes = [tag; 16];
        bytes[8..].copy_from_slice(&index.to_be_bytes());
        RowUuid::from_bytes(bytes)
    }

    #[test]
    fn oracle_filters_and_orders_with_row_id_ties() {
        let team = row(0x71, 1);
        let mut oracle = PerTeamTop100Oracle::new();
        for document in [
            DocumentMetadata::new(row(0x72, 1), team, 10, DocumentStatus::Active, false),
            DocumentMetadata::new(row(0x72, 2), team, 10, DocumentStatus::Draft, false),
            DocumentMetadata::new(row(0x72, 3), team, 11, DocumentStatus::Other, false),
            DocumentMetadata::new(row(0x72, 4), team, 12, DocumentStatus::Active, true),
            DocumentMetadata::new(row(0x72, 5), team, 9, DocumentStatus::Active, false),
        ] {
            oracle.upsert(document);
        }

        assert_eq!(
            oracle.page(team).rows(),
            &[row(0x72, 2), row(0x72, 1), row(0x72, 5)]
        );
    }

    #[test]
    fn oracle_upsert_moves_rank_and_page_transition_is_exact() {
        let team = row(0x73, 1);
        let document = row(0x74, 1);
        let mut oracle = PerTeamTop100Oracle::new();
        oracle.upsert(DocumentMetadata::new(
            document,
            team,
            1,
            DocumentStatus::Active,
            false,
        ));
        let before = oracle.page(team);
        oracle.upsert(DocumentMetadata::new(
            document,
            team,
            2,
            DocumentStatus::Active,
            true,
        ));
        let after = oracle.page(team);
        let transition = PageTransition::between(before, after);

        assert_eq!(transition.added, BTreeSet::new());
        assert_eq!(transition.removed, BTreeSet::from([document]));
    }
}
