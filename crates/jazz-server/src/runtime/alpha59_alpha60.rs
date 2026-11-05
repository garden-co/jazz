//! Runtime access for `jazz-tools alpha59-alpha60`, which imports an
//! alpha.59 server data dir: it writes rows through the runtime owner's own
//! `Db` (the public Db API) on the thread that owns it.
//!
//! Only for the alpha59-alpha60 importer; it goes when the importer does.
#![allow(missing_docs)]

use jazz::db::Db;
use jazz::schema::JazzSchema;

use super::ServerRuntimeHandle;

impl ServerRuntimeHandle {
    /// Every schema version the runtime catalogue admitted. The catalogue
    /// stores schemas without policies; the store's current schema carries
    /// the active permissions, and a view of that version must bring them.
    pub async fn migrate_import_schemas(&self) -> Result<Vec<JazzSchema>, String> {
        self.run(|owner| {
            let current = owner.db.current_schema();
            Ok(owner
                .catalogue
                .schemas
                .values()
                .map(|schema| {
                    if schema.version_id() == current.version_id() {
                        current.clone()
                    } else {
                        schema.clone()
                    }
                })
                .collect())
        })
        .await
    }

    /// Run `job` on the owner thread with the runtime store.
    pub async fn migrate_import_with_db<T: Send + 'static>(
        &self,
        job: impl FnOnce(&Db) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String> {
        self.run(move |owner| job(&owner.db)).await
    }
}
