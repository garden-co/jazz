//! Ownership of the Groove database during node construction and extraction.
//! Live catalogue rebuilds replace the runtime in place.

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
