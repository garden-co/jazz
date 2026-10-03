//! Catalogue mutation boundary for the disposable announcement fingerprint.
//!
//! The state is private here so every mutable access, including mutations on
//! a cloned planning catalogue, invalidates the fingerprint automatically.
//! Restoring an unchanged clone restores only a fingerprint for that clone's
//! exact state. Cache misses retain the existing full snapshot validation.

use super::{
    CompiledLensCacheKey, CompiledLensPath, LensPathCacheKey, LensPathDirection, MigrationLensId,
    PhysicalCurrentWinnerProjections, PhysicalWritePlanCache, SchemaCatalogueState,
};
use std::{
    cell::Cell,
    collections::BTreeMap,
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

    // Derived caches are not part of the announced catalogue snapshot. Filling
    // or resetting them on hot read and write paths must keep the fingerprint;
    // otherwise every cache miss re-serializes and re-hashes the catalogue on
    // the next sync turn.

    pub(super) fn lens_path_cache_mut(
        &mut self,
    ) -> &mut BTreeMap<LensPathCacheKey, Option<Vec<(MigrationLensId, LensPathDirection)>>> {
        &mut self.state.lens_path_cache
    }

    pub(super) fn compiled_lens_cache_mut(
        &mut self,
    ) -> &mut BTreeMap<CompiledLensCacheKey, Option<CompiledLensPath>> {
        &mut self.state.compiled_lens_cache
    }

    pub(super) fn physical_write_plan_cache_mut(&mut self) -> &mut PhysicalWritePlanCache {
        &mut self.state.physical_write_plan_cache
    }

    pub(super) fn physical_current_winner_projections_mut(
        &mut self,
    ) -> &mut PhysicalCurrentWinnerProjections {
        &mut self.state.physical_current_winner_projections
    }
}
