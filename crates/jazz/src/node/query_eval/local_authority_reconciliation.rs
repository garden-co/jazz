//! Exact authority-closure generation tracking for a receiver-owned graph.
//!
//! Authority result members are deliberately not reconciled here. A receiver
//! replaces exact covered sources and derives every public terminal locally.

use super::*;

#[derive(Clone, Debug, Default)]
pub(crate) struct LocalAuthorityReconciliation {
    source: Option<AuthorityResultKey>,
    generation: u64,
}

impl LocalAuthorityReconciliation {
    pub(crate) fn is_due(&self, source: &AuthorityResultKey, generation: u64) -> bool {
        self.source.as_ref() != Some(source) || generation != self.generation
    }

    pub(crate) fn replace_source(&mut self, source: AuthorityResultKey, generation: u64) {
        if self.source.as_ref() == Some(&source) {
            // Re-registering the same immutable source cannot make an older
            // authority snapshot current again. Callers commonly install a
            // source before they have observed its current generation.
            self.generation = self.generation.max(generation);
            return;
        }
        self.source = Some(source);
        self.generation = generation;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(byte: u8) -> AuthorityResultKey {
        AuthorityResultKey::unscoped(BindingViewKey::new(
            ShapeId(uuid::Uuid::from_bytes([byte; 16])),
            BindingId(uuid::Uuid::from_bytes([byte; 16])),
            ReadViewKey::default(),
        ))
    }

    #[test]
    fn same_source_replacement_never_lowers_generation_watermark() {
        let key = source(4);
        let mut state = LocalAuthorityReconciliation::default();
        state.replace_source(key.clone(), 10);
        state.replace_source(key.clone(), 0);
        assert!(!state.is_due(&key, 10));
    }

    #[test]
    fn actual_source_replacement_resets_generation() {
        let old = source(6);
        let fresh = source(7);
        let mut state = LocalAuthorityReconciliation::default();
        state.replace_source(old.clone(), 10);
        state.replace_source(fresh.clone(), 0);
        assert!(!state.is_due(&fresh, 0));
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
