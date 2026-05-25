pub(crate) fn accepted_history_join(row_alias: &str, tx_alias: &str) -> String {
    format!(
        "JOIN jazz_tx {tx_alias} ON {tx_alias}.tx_id = {row_alias}.j_tx_id \
         AND {tx_alias}.status = 'global_durable_accepted' \
         AND {tx_alias}.global_epoch <= ?"
    )
}

pub(crate) fn latest_accepted_history_predicate(
    table_name: &str,
    row_alias: &str,
    tx_alias: &str,
    newer_alias: &str,
    newer_tx_alias: &str,
) -> String {
    format!(
        "NOT EXISTS (
           SELECT 1
           FROM {table_name} {newer_alias}
           JOIN jazz_tx {newer_tx_alias} ON {newer_tx_alias}.tx_id = {newer_alias}.j_tx_id
           WHERE {newer_alias}.j_branch_id = {row_alias}.j_branch_id
             AND {newer_alias}.j_row_id = {row_alias}.j_row_id
             AND {newer_tx_alias}.status = 'global_durable_accepted'
             AND {newer_tx_alias}.global_epoch <= ?
             AND ({newer_tx_alias}.global_epoch, {newer_alias}.j_tx_id) >
                 ({tx_alias}.global_epoch, {row_alias}.j_tx_id)
         )"
    )
}
