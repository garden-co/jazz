//! Temporary ownership of the Groove database during node rebuilds.
//!
//! Rebuilding a [`super::NodeState`] needs to move the database out while its
//! catalogue-derived layouts are reconstructed. Keeping that transition in a
//! dedicated slot makes the `Option` invariant local and leaves the node core
//! focused on node state and lifecycle.

use std::ops::{Deref, DerefMut};

use groove::db::Database;

pub(super) struct DatabaseSlot {
    database: Option<Database>,
}

impl DatabaseSlot {
    pub(super) fn new(database: Database) -> Self {
        Self {
            database: Some(database),
        }
    }

    pub(super) fn take(&mut self) -> Database {
        self.database
            .take()
            .expect("node database slot must be populated outside rebuild")
    }

    pub(super) fn replace(&mut self, database: Database) {
        debug_assert!(self.database.is_none());
        self.database = Some(database);
    }

    pub(super) fn into_inner(mut self) -> Database {
        self.take()
    }
}

impl Deref for DatabaseSlot {
    type Target = Database;

    fn deref(&self) -> &Self::Target {
        self.database
            .as_ref()
            .expect("node database slot must be populated")
    }
}

impl DerefMut for DatabaseSlot {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.database
            .as_mut()
            .expect("node database slot must be populated")
    }
}
