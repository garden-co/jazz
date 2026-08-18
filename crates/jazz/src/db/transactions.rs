//! Mergeable and exclusive transaction handles and staging.

use super::mutations::MutationPrepareError;
use super::*;

pub(super) fn begin_mergeable_loaded(
    node: &Node<groove::storage::DemandLoadedStorage>,
    id: OpenBatchId,
    author: AuthorId,
) -> Result<(), MutationPrepareError> {
    node.node
        .borrow_mut()
        .open_mergeable(id, author, None)
        .map_err(MutationPrepareError::Node)
}

pub(super) fn stage_mergeable_insert_loaded(
    schema: &JazzSchema,
    node: &Node<groove::storage::DemandLoadedStorage>,
    schema_version: SchemaVersionId,
    tx_id: OpenBatchId,
    table: &str,
    row: RowUuid,
    cells: RowCells,
    now_ms: u64,
) -> Result<(), MutationPrepareError> {
    let cells = mutations::apply_insert_defaults_loaded(schema, table, cells)
        .map_err(MutationPrepareError::Api)?;
    node.node
        .borrow_mut()
        .tx_write_mergeable_in_schema(
            tx_id,
            schema_version,
            table,
            row,
            cells,
            None,
            Vec::new(),
            Some(now_ms),
            false,
        )
        .map_err(MutationPrepareError::Node)
}

pub(super) fn stage_mergeable_update_loaded(
    node: &Node<groove::storage::DemandLoadedStorage>,
    schema_version: SchemaVersionId,
    tx_id: OpenBatchId,
    table: &str,
    row: RowUuid,
    patch: RowCells,
    now_ms: u64,
) -> Result<(), MutationPrepareError> {
    node.node
        .borrow_mut()
        .tx_patch_mergeable_in_schema(tx_id, schema_version, table, row, patch, Some(now_ms))
        .map_err(MutationPrepareError::Node)
}

pub(super) fn stage_mergeable_delete_loaded(
    node: &Node<groove::storage::DemandLoadedStorage>,
    schema_version: SchemaVersionId,
    tx_id: OpenBatchId,
    table: &str,
    row: RowUuid,
    now_ms: u64,
) -> Result<(), MutationPrepareError> {
    node.node
        .borrow_mut()
        .tx_write_mergeable_in_schema(
            tx_id,
            schema_version,
            table,
            row,
            BTreeMap::<String, Value>::new(),
            Some(DeletionEvent::Deleted),
            Vec::new(),
            Some(now_ms),
            false,
        )
        .map_err(MutationPrepareError::Node)
}

pub(super) fn stage_mergeable_restore_loaded(
    schema: &JazzSchema,
    node: &Node<groove::storage::DemandLoadedStorage>,
    schema_version: SchemaVersionId,
    tx_id: OpenBatchId,
    table: &str,
    row: RowUuid,
    cells: RowCells,
    now_ms: u64,
) -> Result<(), MutationPrepareError> {
    let cells = mutations::apply_insert_defaults_loaded(schema, table, cells)
        .map_err(MutationPrepareError::Api)?;
    let mut state = node.node.borrow_mut();
    let content_parents = state
        .local_content_winner_tx_id(table, row)
        .map_err(MutationPrepareError::Node)?
        .into_iter()
        .collect();
    let deletion_parents = state
        .local_deletion_winner_tx_id(table, row)
        .map_err(MutationPrepareError::Node)?
        .into_iter()
        .collect();
    state
        .tx_write_mergeable_in_schema(
            tx_id,
            schema_version,
            table,
            row,
            cells,
            None,
            content_parents,
            Some(now_ms),
            true,
        )
        .map_err(MutationPrepareError::Node)?;
    state
        .tx_write_mergeable_in_schema(
            tx_id,
            schema_version,
            table,
            row,
            BTreeMap::<String, Value>::new(),
            Some(DeletionEvent::Restored),
            deletion_parents,
            Some(now_ms),
            true,
        )
        .map_err(MutationPrepareError::Node)
}

pub(super) fn begin_exclusive_loaded(
    node: &Node<groove::storage::DemandLoadedStorage>,
    id: OpenBatchId,
    author: AuthorId,
) -> Result<(), MutationPrepareError> {
    node.node
        .borrow_mut()
        .open_exclusive_for_identity(id, author)
        .map_err(MutationPrepareError::Node)
}

pub(super) fn transaction_all_loaded(
    node: &Node<groove::storage::DemandLoadedStorage>,
    tx_id: OpenBatchId,
    prepared: &PreparedQuery,
    opts: &ReadOpts,
    author: Option<AuthorId>,
) -> Result<Vec<CurrentRow>, MutationPrepareError> {
    ensure_default_read_view(opts).map_err(MutationPrepareError::Api)?;
    let mut state = node.node.borrow_mut();
    match author {
        Some(author) => state.tx_query_for_identity_with_options(
            tx_id,
            &prepared.shape,
            &prepared.binding,
            author,
            opts.include_deleted,
        ),
        None => state.tx_query_with_options(
            tx_id,
            &prepared.shape,
            &prepared.binding,
            opts.include_deleted,
        ),
    }
    .map_err(MutationPrepareError::Node)
}

pub(super) fn exclusive_read_loaded(
    node: &Node<groove::storage::DemandLoadedStorage>,
    schema_version: SchemaVersionId,
    tx_id: OpenBatchId,
    table: &str,
    row: RowUuid,
) -> Result<Option<RowCells>, MutationPrepareError> {
    node.node
        .borrow_mut()
        .tx_read_in_schema(tx_id, schema_version, table, row)
        .map_err(MutationPrepareError::Node)
}

pub(super) fn stage_exclusive_insert_loaded(
    schema: &JazzSchema,
    node: &Node<groove::storage::DemandLoadedStorage>,
    schema_version: SchemaVersionId,
    tx_id: OpenBatchId,
    table: &str,
    row: RowUuid,
    cells: RowCells,
    now_ms: u64,
) -> Result<(), MutationPrepareError> {
    let cells = mutations::apply_insert_defaults_loaded(schema, table, cells)
        .map_err(MutationPrepareError::Api)?;
    node.node
        .borrow_mut()
        .tx_write_in_schema_at_ms(tx_id, schema_version, table, row, cells, None, Some(now_ms))
        .map_err(MutationPrepareError::Node)
}

pub(super) fn stage_exclusive_delete_loaded(
    node: &Node<groove::storage::DemandLoadedStorage>,
    schema_version: SchemaVersionId,
    tx_id: OpenBatchId,
    table: &str,
    row: RowUuid,
    now_ms: u64,
) -> Result<(), MutationPrepareError> {
    node.node
        .borrow_mut()
        .tx_write_in_schema_at_ms(
            tx_id,
            schema_version,
            table,
            row,
            BTreeMap::<String, Value>::new(),
            Some(DeletionEvent::Deleted),
            Some(now_ms),
        )
        .map_err(MutationPrepareError::Node)
}

pub(super) fn stage_exclusive_restore_loaded(
    schema: &JazzSchema,
    node: &Node<groove::storage::DemandLoadedStorage>,
    schema_version: SchemaVersionId,
    tx_id: OpenBatchId,
    table: &str,
    row: RowUuid,
    cells: RowCells,
    now_ms: u64,
) -> Result<(), MutationPrepareError> {
    let cells = mutations::apply_insert_defaults_loaded(schema, table, cells)
        .map_err(MutationPrepareError::Api)?;
    let mut state = node.node.borrow_mut();
    state
        .tx_write_in_schema_at_ms(tx_id, schema_version, table, row, cells, None, Some(now_ms))
        .map_err(MutationPrepareError::Node)?;
    state
        .tx_write_in_schema_at_ms(
            tx_id,
            schema_version,
            table,
            row,
            BTreeMap::<String, Value>::new(),
            Some(DeletionEvent::Restored),
            Some(now_ms),
        )
        .map_err(MutationPrepareError::Node)
}
