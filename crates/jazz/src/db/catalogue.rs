//! Catalogue schema and migration-lens publication APIs.

use super::*;

impl<S> Db<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Allocate the immutable global physical identities for a proposed
    /// descendant lineage from the active source manifest.
    ///
    /// This only authors a payload; trusted catalogue admission remains the
    /// authority-only operation performed by [`Self::publish_schema_with_lens`].
    /// Keeping the two steps separate lets a test or client prepare a correct
    /// descendant without ever gaining permission to publish it.
    pub fn author_schema_lineage_publication(
        &self,
        schema: SchemaVersion,
        lens: MigrationLens,
        new_tables: impl IntoIterator<Item = impl Into<String>>,
        dropped_tables: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<SchemaLineagePublication, Error> {
        self.node
            .node
            .borrow()
            .author_schema_lineage_publication(schema, lens, new_tables, dropped_tables)
            .map_err(Into::into)
    }

    /// Publish an immutable schema-version payload through the catalogue lane.
    pub async fn publish_schema(&self, schema: SchemaVersion) -> Result<Vec<SyncMessage>, Error> {
        self.check_catalogue_admin()?;
        let outcome = self
            .node
            .node
            .lock()
            .await
            .apply_trusted_catalogue_message(SyncMessage::PublishSchema {
                author: self.identity.author,
                schema: Box::new(schema),
            })
            .await?;
        self.finish_publication_outcome(outcome).await
    }

    /// Atomically publish a non-genesis schema and its lineage-defining lens.
    pub async fn publish_schema_with_lens(
        &self,
        catalogue_seq: u64,
        publication: SchemaLineagePublication,
    ) -> Result<Vec<SyncMessage>, Error> {
        self.check_catalogue_admin()?;
        let outcome = self
            .node
            .node
            .lock()
            .await
            .apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
                author: self.identity.author,
                catalogue_seq,
                publication: Box::new(publication),
            })
            .await?;
        self.finish_publication_outcome(outcome).await
    }

    /// Publish an immutable migration lens through the catalogue lane.
    pub async fn publish_lens(&self, lens: MigrationLens) -> Result<Vec<SyncMessage>, Error> {
        self.check_catalogue_admin()?;
        let outcome = self
            .node
            .node
            .lock()
            .await
            .apply_trusted_catalogue_message(SyncMessage::PublishLens {
                author: self.identity.author,
                lens,
            })
            .await?;
        self.finish_publication_outcome(outcome).await
    }

    /// Set the current write-schema pointer through the catalogue lane.
    pub async fn set_current_write_schema(
        &self,
        pointer: CurrentWriteSchema,
    ) -> Result<Vec<SyncMessage>, Error> {
        self.check_catalogue_admin()?;
        let outcome = self
            .node
            .node
            .lock()
            .await
            .apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
                author: self.identity.author,
                pointer,
            })
            .await?;
        self.finish_publication_outcome(outcome).await
    }

    /// Set whether this authority may settle session-scoped reads and writes.
    /// Enabling it rehydrates all live subscriber views.
    pub fn set_permissions_ready(&self, ready: bool) -> Result<(), Error> {
        self.node.set_permissions_ready(ready)
    }

    /// Return the current write-schema pointer known to this database.
    pub fn current_write_schema(&self) -> Result<CurrentWriteSchema, Error> {
        self.node
            .node
            .borrow()
            .current_write_schema()
            .map_err(Into::into)
    }

    /// Return a published schema-version payload known to this database.
    pub fn catalogue_schema(&self, schema: SchemaVersionId) -> Option<JazzSchema> {
        self.node
            .node
            .borrow()
            .catalogue_schemas()
            .get(&schema)
            .map(|schema| schema.schema.clone())
    }

    /// Highest contiguously activated authoritative catalogue position.
    pub fn active_catalogue_seq(&self) -> u64 {
        self.node.node.borrow().active_catalogue_seq()
    }

    /// Return a published migration lens known to this database.
    pub fn catalogue_lens(&self, lens: crate::ids::MigrationLensId) -> Option<MigrationLens> {
        self.node
            .node
            .borrow()
            .catalogue_lenses()
            .get(&lens)
            .cloned()
    }
}
