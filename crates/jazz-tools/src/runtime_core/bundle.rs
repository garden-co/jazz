use super::*;
use crate::sync_bundle::{SyncBundle, apply_query_bundle, compose_query_bundle};

impl<S: Storage, Sch: Scheduler> RuntimeCore<S, Sch> {
    /// Compose a sync bundle of this runtime's CRDT state for `query` under
    /// `session`, ready to ship to a cold client. Non-destructive: sync already
    /// queued for live peers is left in place.
    pub fn compose_query_bundle(&mut self, query: Query, session: Option<Session>) -> SyncBundle {
        // schema_manager and storage are disjoint fields, so both can be borrowed
        // mutably to drive the composer against this runtime's own state.
        compose_query_bundle(
            self.schema_manager.query_manager_mut(),
            &mut self.storage,
            query,
            session,
        )
    }

    /// Apply a composed bundle to this runtime, seeding its store before sync
    /// connects so SSR-hydrated rows are present on first paint.
    pub fn apply_query_bundle(&mut self, bundle: &SyncBundle) {
        apply_query_bundle(
            self.schema_manager.query_manager_mut(),
            &mut self.storage,
            bundle,
        );
    }
}
