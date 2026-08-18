//! Row insertion, update, deletion, restoration, and authorization.

use super::*;

impl Db {
    #[cfg(test)]
    pub(crate) fn authorize_insert_for_identity(
        &self,
        table: &str,
        cells: RowCells,
        identity: AuthorId,
    ) -> Result<PermissionAdvice, Error> {
        let cells = apply_insert_defaults_loaded(&self.schema, table, cells)?;
        self.node
            .node
            .borrow_mut()
            .dry_run_mergeable_write_allows_for_view(
                &self.schema,
                MergeableCommit::new(table, RowUuid::from_bytes([0; 16]), 0)
                    .made_by(identity)
                    .permission_subject(identity)
                    .cells(cells),
            )
            .map(|allowed| {
                if allowed {
                    PermissionAdvice::Allowed
                } else {
                    PermissionAdvice::Denied
                }
            })
            .map_err(Into::into)
    }

    #[cfg(test)]
    pub(crate) fn authorize_read_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        author: AuthorId,
    ) -> Result<PermissionAdvice, Error> {
        self.table_schema(table)?;
        self.node
            .node
            .borrow_mut()
            .dry_run_read_current_allows(table, row, author)
            .map(|allowed| {
                if allowed {
                    PermissionAdvice::Allowed
                } else {
                    PermissionAdvice::Denied
                }
            })
            .map_err(Into::into)
    }

    #[cfg(test)]
    pub(crate) fn authorize_delete_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        author: AuthorId,
    ) -> Result<PermissionAdvice, Error> {
        self.table_schema(table)?;
        self.node
            .node
            .borrow_mut()
            .dry_run_delete_current_allows(table, row, author)
            .map(|allowed| {
                if allowed {
                    PermissionAdvice::Allowed
                } else {
                    PermissionAdvice::Denied
                }
            })
            .map_err(Into::into)
    }
}

fn table_schema_loaded<'a>(schema: &'a JazzSchema, table: &str) -> Result<&'a TableSchema, Error> {
    schema
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
        .ok_or_else(|| Error::new(ErrorCode::Schema, format!("unknown table {table}")))
}

pub(super) fn apply_insert_defaults_loaded(
    schema: &JazzSchema,
    table: &str,
    mut cells: RowCells,
) -> Result<RowCells, Error> {
    for column in &table_schema_loaded(schema, table)?.columns {
        if !cells.contains_key(&column.name) {
            if let Some(default) = &column.default {
                cells.insert(
                    column.name.clone(),
                    default_cell_for_column_type(&column.column_type, default),
                );
            }
        }
    }
    Ok(cells)
}

pub(super) fn prepare_update_loaded(
    schema: &JazzSchema,
    node: &Node<groove::storage::DemandLoadedStorage>,
    identity: DbIdentity,
    table: &str,
    row: RowUuid,
    patch: RowCells,
    now_ms: u64,
    author: AuthorId,
) -> Result<MergeableCommit, MutationPrepareError> {
    let table_schema = table_schema_loaded(schema, table)
        .map_err(MutationPrepareError::Api)?
        .clone();
    if node
        .node
        .borrow_mut()
        .local_deletion_winner_tx_id(table, row)
        .map_err(MutationPrepareError::Node)?
        .is_some()
    {
        return Err(MutationPrepareError::Api(row_already_deleted(row)));
    }
    let (cells, parent, authored_columns) = if table_schema
        .columns
        .iter()
        .all(|column| patch.contains_key(&column.name))
    {
        let existing = node
            .node
            .borrow_mut()
            .local_current_row(table, row)
            .map_err(MutationPrepareError::Node)?;
        let parent = existing
            .as_ref()
            .and_then(|row| node.node.borrow_mut().current_row_tx_id(row));
        let authored_columns = patch.keys().cloned().collect();
        (patch, parent, authored_columns)
    } else {
        let query = Query::from(table).filter(crate::query::eq(
            crate::query::col("id"),
            crate::query::lit(Value::Uuid(row.0)),
        ));
        let prepared = reads::prepare_query_loaded(node, schema, schema.version_id(), &query)
            .map_err(MutationPrepareError::Api)?;
        let existing = node
            .node
            .borrow_mut()
            .query_rows_for_client(
                &prepared.shape,
                &prepared.binding,
                DurabilityTier::Local,
                author,
            )
            .map_err(MutationPrepareError::Node)?
            .into_iter()
            .find(|candidate| candidate.row_uuid() == row)
            .ok_or_else(|| {
                MutationPrepareError::Api(read_for_write_denied("partial UPDATE", table))
            })?;
        let mut cells = BTreeMap::new();
        for column in &table_schema.columns {
            if let Some(value) = existing.cell(&table_schema, &column.name) {
                cells.insert(
                    column.name.clone(),
                    default_cell_for_column_type(&column.column_type, &value),
                );
            }
        }
        let parent = node.node.borrow_mut().current_row_tx_id(&existing);
        let authored_columns = patch.keys().cloned().collect();
        cells.extend(patch);
        (cells, parent, authored_columns)
    };
    Ok(MergeableCommit::new(table, row, now_ms)
        .made_by(identity.author)
        .parents(parent.into_iter().collect())
        .cells(cells)
        .authored_columns(authored_columns))
}

pub(super) fn acquire_insert_target_loaded(
    schema: &JazzSchema,
    node: &Node<groove::storage::DemandLoadedStorage>,
    table: &str,
    row: RowUuid,
) -> Result<(), MutationPrepareError> {
    table_schema_loaded(schema, table).map_err(MutationPrepareError::Api)?;
    let (content_parent, deletion_parent) = {
        let mut state = node.node.borrow_mut();
        let _ = state
            .row_history(table, row)
            .map_err(MutationPrepareError::Node)?;
        (
            state
                .local_content_winner_tx_id(table, row)
                .map_err(MutationPrepareError::Node)?,
            state
                .local_deletion_winner_tx_id(table, row)
                .map_err(MutationPrepareError::Node)?,
        )
    };
    if deletion_parent.is_some() {
        return Err(MutationPrepareError::Api(row_already_deleted(row)));
    }
    if content_parent.is_some() {
        return Err(MutationPrepareError::Api(Error::new(
            ErrorCode::WriteRejected,
            format!("encoding error: object already exists: {}", row.0),
        )));
    }
    Ok(())
}

pub(super) fn prepare_noop_update_loaded(
    schema: &JazzSchema,
    node: &Node<groove::storage::DemandLoadedStorage>,
    table: &str,
    row: RowUuid,
    author: AuthorId,
) -> Result<(TxId, DurabilityTier), MutationPrepareError> {
    table_schema_loaded(schema, table).map_err(MutationPrepareError::Api)?;
    if node
        .node
        .borrow_mut()
        .local_deletion_winner_tx_id(table, row)
        .map_err(MutationPrepareError::Node)?
        .is_some()
    {
        return Err(MutationPrepareError::Api(row_already_deleted(row)));
    }
    let query = Query::from(table).filter(crate::query::eq(
        crate::query::col("id"),
        crate::query::lit(Value::Uuid(row.0)),
    ));
    let prepared = reads::prepare_query_loaded(node, schema, schema.version_id(), &query)
        .map_err(MutationPrepareError::Api)?;
    let existing = node
        .node
        .borrow_mut()
        .query_rows_for_client(
            &prepared.shape,
            &prepared.binding,
            DurabilityTier::Local,
            author,
        )
        .map_err(MutationPrepareError::Node)?
        .into_iter()
        .find(|candidate| candidate.row_uuid() == row)
        .ok_or_else(|| MutationPrepareError::Api(read_for_write_denied("partial UPDATE", table)))?;
    let tx_id = node
        .node
        .borrow_mut()
        .current_row_tx_id(&existing)
        .ok_or_else(|| {
            MutationPrepareError::Api(Error::new(
                ErrorCode::NotObserved,
                "current row has no transaction",
            ))
        })?;
    let durability = node
        .node
        .borrow_mut()
        .transaction_state(tx_id)
        .map(|(_, _, durability)| durability)
        .ok_or_else(|| {
            MutationPrepareError::Api(Error::new(
                ErrorCode::NotObserved,
                "transaction is not known locally",
            ))
        })?;
    Ok((tx_id, durability))
}

pub(super) fn prepare_delete_loaded(
    schema: &JazzSchema,
    node: &Node<groove::storage::DemandLoadedStorage>,
    identity: DbIdentity,
    table: &str,
    row: RowUuid,
    now_ms: u64,
) -> Result<MergeableCommit, MutationPrepareError> {
    table_schema_loaded(schema, table).map_err(MutationPrepareError::Api)?;
    let (content_parent, deletion_parent) = {
        let mut state = node.node.borrow_mut();
        (
            state
                .local_content_winner_tx_id(table, row)
                .map_err(MutationPrepareError::Node)?,
            state
                .local_deletion_winner_tx_id(table, row)
                .map_err(MutationPrepareError::Node)?,
        )
    };
    if deletion_parent.is_some() {
        return Err(MutationPrepareError::Api(row_already_deleted(row)));
    }
    Ok(MergeableCommit::new(table, row, now_ms)
        .made_by(identity.author)
        .parents(content_parent.into_iter().collect())
        .cells(BTreeMap::<String, Value>::new())
        .deletion(DeletionEvent::Deleted))
}

pub(super) fn prepare_restore_loaded(
    schema: &JazzSchema,
    node: &Node<groove::storage::DemandLoadedStorage>,
    identity: DbIdentity,
    table: &str,
    row: RowUuid,
    cells: RowCells,
    now_ms: u64,
) -> Result<Vec<MergeableCommit>, MutationPrepareError> {
    let cells =
        apply_insert_defaults_loaded(schema, table, cells).map_err(MutationPrepareError::Api)?;
    let (content_parent, deletion_parent) = {
        let mut state = node.node.borrow_mut();
        (
            state
                .local_content_winner_tx_id(table, row)
                .map_err(MutationPrepareError::Node)?,
            state
                .local_deletion_winner_tx_id(table, row)
                .map_err(MutationPrepareError::Node)?,
        )
    };
    let deletion_parent = deletion_parent.ok_or_else(|| {
        MutationPrepareError::Api(Error::new(
            ErrorCode::WriteRejected,
            format!("row not deleted: {}", row.0),
        ))
    })?;
    Ok(vec![
        MergeableCommit::new(table, row, now_ms)
            .made_by(identity.author)
            .parents(content_parent.into_iter().collect())
            .cells(cells),
        MergeableCommit::new(table, row, now_ms)
            .made_by(identity.author)
            .parents(vec![deletion_parent])
            .cells(BTreeMap::<String, Value>::new())
            .deletion(DeletionEvent::Restored),
    ])
}

pub(super) fn prepare_upsert_loaded(
    schema: &JazzSchema,
    node: &Node<groove::storage::DemandLoadedStorage>,
    identity: DbIdentity,
    table: &str,
    row: RowUuid,
    patch: RowCells,
    now_ms: u64,
    author: AuthorId,
) -> Result<MergeableCommit, MutationPrepareError> {
    let table_schema = table_schema_loaded(schema, table).map_err(MutationPrepareError::Api)?;
    if node
        .node
        .borrow_mut()
        .local_deletion_winner_tx_id(table, row)
        .map_err(MutationPrepareError::Node)?
        .is_some()
    {
        return Err(MutationPrepareError::Api(row_already_deleted(row)));
    }
    let query = Query::from(table).filter(crate::query::eq(
        crate::query::col("id"),
        crate::query::lit(Value::Uuid(row.0)),
    ));
    let prepared = reads::prepare_query_loaded(node, schema, schema.version_id(), &query)
        .map_err(MutationPrepareError::Api)?;
    let visible = node
        .node
        .borrow_mut()
        .query_rows_for_client(
            &prepared.shape,
            &prepared.binding,
            DurabilityTier::Local,
            author,
        )
        .map_err(MutationPrepareError::Node)?
        .into_iter()
        .find(|candidate| candidate.row_uuid() == row);
    if visible.is_some() {
        return prepare_update_loaded(schema, node, identity, table, row, patch, now_ms, author);
    }
    let raw_existing = node
        .node
        .borrow_mut()
        .local_current_row(table, row)
        .map_err(MutationPrepareError::Node)?;
    if raw_existing.is_some() && author != AuthorId::SYSTEM && table_schema.read_policy.is_some() {
        return Err(MutationPrepareError::Api(read_for_write_denied(
            "UPSERT", table,
        )));
    }
    let cells =
        apply_insert_defaults_loaded(schema, table, patch).map_err(MutationPrepareError::Api)?;
    Ok(MergeableCommit::new(table, row, now_ms)
        .made_by(identity.author)
        .cells(cells))
}

pub(super) enum MutationPrepareError {
    Node(crate::node::Error),
    Api(Error),
}

impl From<crate::node::Error> for MutationPrepareError {
    fn from(error: crate::node::Error) -> Self {
        Self::Node(error)
    }
}

impl From<groove::storage::Error> for MutationPrepareError {
    fn from(error: groove::storage::Error) -> Self {
        Self::Node(crate::node::Error::Storage(error))
    }
}

impl MutationPrepareError {
    pub(super) fn into_api(self) -> Error {
        match self {
            Self::Node(error) => error.into(),
            Self::Api(error) => error,
        }
    }

    pub(super) fn missing_input(
        self,
    ) -> Result<groove::storage::async_ordered::OwnedStorageOperation, Self> {
        match self {
            Self::Node(error) => crate::node::missing_node_open_input(error).map_err(Self::Node),
            error => Err(error),
        }
    }
}
