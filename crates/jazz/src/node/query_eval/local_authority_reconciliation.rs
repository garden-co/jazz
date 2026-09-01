//! Shared Local-plus-authority membership reconciliation.
//!
//! This state is deliberately independent of receiver ownership. Both the DB
//! facade and peer publication keep their own receiver/materialization state,
//! but authority provenance and source replacement must follow one contract.

use super::*;

#[derive(Clone, Debug, Default)]
pub(crate) struct LocalAuthorityReconciliation {
    source: Option<AuthorityResultKey>,
    generation: u64,
    confirmed_members: BTreeSet<ResultMemberEntry>,
    confirmed_facts: BTreeSet<ProgramFactEntry>,
    deferred: bool,
    deferred_row_keys: BTreeSet<(String, RowUuid)>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct LocalAuthorityDelta {
    pub(crate) member_adds: Vec<ResultMemberEntry>,
    pub(crate) member_removes: Vec<ResultMemberEntry>,
    pub(crate) fact_adds: Vec<ProgramFactEntry>,
    pub(crate) fact_removes: Vec<ProgramFactEntry>,
}

impl LocalAuthorityReconciliation {
    pub(crate) fn source(&self) -> Option<&AuthorityResultKey> {
        self.source.as_ref()
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn is_due(&self, source: &AuthorityResultKey, generation: u64) -> bool {
        self.source.as_ref() != Some(source) || self.deferred || generation != self.generation
    }

    pub(crate) fn deferred_row_keys(&self) -> &BTreeSet<(String, RowUuid)> {
        &self.deferred_row_keys
    }

    pub(crate) fn defer(&mut self, row_keys: BTreeSet<(String, RowUuid)>) {
        self.deferred = true;
        self.deferred_row_keys = row_keys;
    }

    pub(crate) fn replace_source(&mut self, source: AuthorityResultKey, generation: u64) {
        if self.source.as_ref() == Some(&source) {
            // Re-registering the same immutable source cannot make an older
            // authority snapshot current again. Callers commonly install a
            // source before they have observed its current generation.
            self.generation = self.generation.max(generation);
            return;
        }
        self.confirmed_members.clear();
        self.confirmed_facts.clear();
        self.deferred = false;
        self.deferred_row_keys.clear();
        self.source = Some(source);
        self.generation = generation;
    }

    /// Reconcile one exact immutable source snapshot with the currently
    /// visible Local result. Stale source/generation updates are ignored.
    pub(crate) fn reconcile(
        &mut self,
        source: &AuthorityResultKey,
        generation: u64,
        visible_members: &BTreeSet<ResultMemberEntry>,
        visible_facts: &BTreeSet<ProgramFactEntry>,
        exact_members: BTreeSet<ResultMemberEntry>,
        exact_facts: BTreeSet<ProgramFactEntry>,
    ) -> Option<LocalAuthorityDelta> {
        if self.source.as_ref() != Some(source) || generation < self.generation {
            return None;
        }
        let delta = LocalAuthorityDelta {
            member_adds: exact_members.difference(visible_members).cloned().collect(),
            member_removes: self
                .confirmed_members
                .difference(&exact_members)
                .cloned()
                .collect(),
            fact_adds: exact_facts.difference(visible_facts).cloned().collect(),
            fact_removes: self
                .confirmed_facts
                .difference(&exact_facts)
                .cloned()
                .collect(),
        };
        self.generation = generation;
        self.confirmed_members = exact_members;
        self.confirmed_facts = exact_facts;
        self.deferred = false;
        self.deferred_row_keys.clear();
        Some(delta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(byte: u8) -> ResultMemberEntry {
        ResultMemberEntry::Synthetic {
            table: "docs".to_owned(),
            row: vec![byte],
            replacement: SyntheticReplacementToken::from_encoded_record(vec![byte]),
        }
    }

    fn source(byte: u8) -> AuthorityResultKey {
        AuthorityResultKey::unscoped(BindingViewKey::new(
            ShapeId(uuid::Uuid::from_bytes([byte; 16])),
            BindingId(uuid::Uuid::from_bytes([byte; 16])),
            ReadViewKey::default(),
        ))
    }

    #[test]
    fn authority_echo_deduplicates_then_removal_retires_only_confirmed_member() {
        let local = member(1);
        let remote = member(2);
        let key = source(3);
        let mut state = LocalAuthorityReconciliation::default();
        state.replace_source(key.clone(), 0);
        let first = state
            .reconcile(
                &key,
                1,
                &BTreeSet::from([local.clone()]),
                &BTreeSet::new(),
                BTreeSet::from([local.clone(), remote.clone()]),
                BTreeSet::new(),
            )
            .unwrap();
        assert_eq!(first.member_adds, vec![remote.clone()]);
        let second = state
            .reconcile(
                &key,
                2,
                &BTreeSet::from([local.clone(), remote.clone()]),
                &BTreeSet::new(),
                BTreeSet::from([local.clone()]),
                BTreeSet::new(),
            )
            .unwrap();
        assert_eq!(second.member_removes, vec![remote]);
    }

    #[test]
    fn source_replacement_preserves_unconfirmed_local_and_rejects_stale_source() {
        let local = member(1);
        let old = source(2);
        let fresh = source(3);
        let mut state = LocalAuthorityReconciliation::default();
        state.replace_source(old.clone(), 1);
        state.replace_source(fresh.clone(), 0);
        assert!(
            state
                .reconcile(
                    &old,
                    2,
                    &BTreeSet::from([local]),
                    &BTreeSet::new(),
                    BTreeSet::new(),
                    BTreeSet::new(),
                )
                .is_none()
        );
    }

    #[test]
    fn same_source_replacement_never_lowers_generation_watermark() {
        let key = source(4);
        let mut state = LocalAuthorityReconciliation::default();
        state.replace_source(key.clone(), 10);
        state.replace_source(key.clone(), 0);
        assert_eq!(state.generation(), 10);
        assert!(
            state
                .reconcile(
                    &key,
                    5,
                    &BTreeSet::new(),
                    &BTreeSet::new(),
                    BTreeSet::new(),
                    BTreeSet::new(),
                )
                .is_none(),
            "an older generation must remain stale after same-source registration",
        );
    }

    #[test]
    fn actual_source_replacement_resets_generation_and_confirmation() {
        let confirmed = member(5);
        let old = source(6);
        let fresh = source(7);
        let mut state = LocalAuthorityReconciliation::default();
        state.replace_source(old.clone(), 10);
        state
            .reconcile(
                &old,
                10,
                &BTreeSet::new(),
                &BTreeSet::new(),
                BTreeSet::from([confirmed.clone()]),
                BTreeSet::new(),
            )
            .unwrap();
        state.replace_source(fresh.clone(), 0);
        assert_eq!(state.generation(), 0);
        let delta = state
            .reconcile(
                &fresh,
                1,
                &BTreeSet::from([confirmed]),
                &BTreeSet::new(),
                BTreeSet::new(),
                BTreeSet::new(),
            )
            .unwrap();
        assert!(
            delta.member_removes.is_empty(),
            "a new source must not retract membership confirmed only by the old source",
        );
    }

    #[test]
    fn an_unseen_source_is_due_even_when_its_generation_is_zero() {
        let first = source(8);
        let replacement = source(9);
        let mut state = LocalAuthorityReconciliation::default();
        assert!(state.is_due(&first, 0));
        state.replace_source(first.clone(), 0);
        assert!(!state.is_due(&first, 0));
        assert!(state.is_due(&replacement, 0));
    }
}
