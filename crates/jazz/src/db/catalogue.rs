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
    ///
    /// See also [`Self::catalogue_table_identity`] for portable table identities.
    pub fn active_catalogue_seq(&self) -> u64 {
        self.node.node.borrow().active_catalogue_seq()
    }

    /// Resolve a table name in an accepted schema to its portable lineage UUID.
    ///
    /// Compatible renames retain this identity. Unknown schemas or table names
    /// return `None`; authored or pending catalogue proposals are not accepted
    /// identities. This does not initialise a schema, grant access or allocate IDs.
    ///
    /// # Errors
    /// Returns an error when the catalogue is uninitialised or unusable.
    pub fn catalogue_table_identity(
        &self,
        schema: SchemaVersionId,
        table: &str,
    ) -> Result<Option<crate::ids::GlobalPhysicalTableId>, Error> {
        self.node
            .node
            .borrow()
            .catalogue_table_identity(schema, table)
            .map_err(Into::into)
    }

    /// Resolve a table's portable identity in this handle's schema view.
    ///
    /// Fixed views retain their schema; the owner follows the current write
    /// schema. Waits for the node lock when storage work is in progress.
    /// Missing tables return `None`; this does not grant access or allocate IDs.
    ///
    /// # Errors
    /// Returns an error when the catalogue is uninitialised or unusable.
    pub async fn table_identity(
        &self,
        table: &str,
    ) -> Result<Option<crate::ids::GlobalPhysicalTableId>, Error> {
        let node = self.node.node.lock().await;
        let schema = if self.schema_view_is_fixed {
            self.schema_version_id
        } else {
            node.current_write_schema()?.schema
        };
        node.catalogue_table_identity(schema, table)
            .map_err(Into::into)
    }

    /// Resolve a column's portable identity in this handle's accepted schema view.
    ///
    /// Compatible renames retain the column epoch. Missing columns return `None`;
    /// this read does not initialise a schema, allocate an identity or grant access.
    ///
    /// # Errors
    /// Returns an error when the catalogue is uninitialised or unusable.
    pub async fn column_identity(
        &self,
        table: &str,
        column: &str,
    ) -> Result<Option<crate::ids::GlobalPhysicalColumnId>, Error> {
        let node = self.node.node.lock().await;
        let schema = if self.schema_view_is_fixed {
            self.schema_version_id
        } else {
            node.current_write_schema()?.schema
        };
        node.catalogue_column_identity(schema, table, column)
            .map_err(Into::into)
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
