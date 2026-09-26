//! Catalogue mutation boundary for the disposable announcement fingerprint.
//!
//! The state is private here so every mutable access, including mutations on
//! a cloned planning catalogue, invalidates the fingerprint automatically.
//! Restoring an unchanged clone restores only a fingerprint for that clone's
//! exact state. Cache misses retain the existing full snapshot validation.

use super::SchemaCatalogueState;
use std::{
    cell::Cell,
    ops::{Deref, DerefMut},
};

#[derive(Clone, Debug)]
pub(super) struct SchemaCatalogue {
    state: SchemaCatalogueState,
    announcement_fingerprint: Cell<Option<[u8; 32]>>,
}

impl From<SchemaCatalogueState> for SchemaCatalogue {
    fn from(state: SchemaCatalogueState) -> Self {
        Self {
            state,
            announcement_fingerprint: Cell::new(None),
        }
    }
}

impl Deref for SchemaCatalogue {
    type Target = SchemaCatalogueState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl DerefMut for SchemaCatalogue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.announcement_fingerprint.set(None);
        &mut self.state
    }
}

impl SchemaCatalogue {
    pub(super) fn into_state(self) -> SchemaCatalogueState {
        self.state
    }

    pub(super) fn announcement_fingerprint(&self) -> Option<[u8; 32]> {
        self.announcement_fingerprint.get()
    }

    pub(super) fn remember_announcement_fingerprint(&self, fingerprint: [u8; 32]) {
        self.announcement_fingerprint.set(Some(fingerprint));
    }
}
