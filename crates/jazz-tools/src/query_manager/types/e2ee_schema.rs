//! E2EE schema normalization: `$keys` companion tables and validation.
//!
//! Mirrored by the TS side in `packages/jazz-tools/src/codegen/schema-reader.ts`;
//! the two must emit identical schemas (column order is normative — it feeds
//! the schema hash).

use crate::query_manager::policy::PolicyExpr;

use super::policy::{OperationPolicy, TablePolicies};
use super::schema::{ColumnType, Schema, TableName, TableSchema, TableSchemaBuilder};

/// Suffix of framework-generated sealed-key companion tables.
pub const E2EE_KEYS_TABLE_SUFFIX: &str = "$keys";

/// Companion table name for a space table.
pub fn e2ee_keys_table_name(space_table: &str) -> String {
    format!("{space_table}{E2EE_KEYS_TABLE_SUFFIX}")
}

fn session_authenticated() -> PolicyExpr {
    PolicyExpr::SessionIsNotNull {
        path: vec!["user_id".to_string()],
    }
}

/// Build the `$keys` companion table for a space table (spec §3).
pub fn e2ee_keys_table_schema(space_table: &str) -> (TableName, TableSchema) {
    let (name, mut table) = TableSchemaBuilder::new(&e2ee_keys_table_name(space_table))
        .fk_column("space_id", space_table)
        .column("key_id", ColumnType::Uuid)
        .column("recipient_user_id", ColumnType::Uuid)
        .column("recipient_public_key", ColumnType::Text)
        .column("sealed_key", ColumnType::Bytea)
        .build_named();

    let mut policies = TablePolicies::default();
    // World-readable: sealed copies are useless without the recipient's
    // private key, and open reads keep sync trivial.
    policies.select = OperationPolicy::using(PolicyExpr::True);
    // Open authenticated insert; bogus rows are ignored on unseal failure.
    // Tighten to members-only once created_by permissions land.
    policies.insert = OperationPolicy::with_check(session_authenticated());
    // No update clause: key rows are immutable (share = insert,
    // revoke = delete). Enforcing runtimes deny missing clauses.
    // v1 delete is open-authenticated; "own rows + creator" needs created_by.
    policies.delete = OperationPolicy::using(session_authenticated());
    table.policies = policies;

    (name, table)
}

/// Inject `$keys` companions for every `encryption_space` table.
/// Idempotent: existing entries are left untouched.
pub fn expand_e2ee_keys_tables(schema: &mut Schema) {
    let space_tables: Vec<String> = schema
        .iter()
        .filter(|(_, table)| table.encryption_space)
        .map(|(name, _)| name.as_str().to_string())
        .collect();
    for space_table in space_tables {
        let (name, table) = e2ee_keys_table_schema(&space_table);
        schema.entry(name).or_insert(table);
    }
}

/// Exclude encrypted columns from indexing. Tables that indexed everything
/// (`indexed_columns: None`) get an explicit subset without encrypted columns;
/// explicit subsets are left alone (validation rejects bad ones).
pub fn normalize_e2ee_indexes(schema: &mut Schema) {
    for table in schema.values_mut() {
        let has_encrypted = table
            .columns
            .columns
            .iter()
            .any(|c| c.encrypted_with.is_some());
        if !has_encrypted || table.indexed_columns.is_some() {
            continue;
        }
        table.indexed_columns = Some(
            table
                .columns
                .columns
                .iter()
                .filter(|c| c.encrypted_with.is_none())
                .map(|c| c.name.clone())
                .collect(),
        );
    }
}

fn policy_expr_references_column(expr: &PolicyExpr, column: &str) -> bool {
    match expr {
        PolicyExpr::Cmp { column: c, .. }
        | PolicyExpr::IsNull { column: c }
        | PolicyExpr::IsNotNull { column: c }
        | PolicyExpr::Contains { column: c, .. }
        | PolicyExpr::In { column: c, .. }
        | PolicyExpr::InList { column: c, .. } => c == column,
        // InheritsReferencing's via_column lives on the *source* table, so
        // matching it here is conservative (may flag a same-named plaintext
        // column elsewhere); erring toward rejection is the safe direction.
        PolicyExpr::Inherits { via_column, .. }
        | PolicyExpr::InheritsReferencing { via_column, .. } => via_column == column,
        PolicyExpr::Exists { condition, .. } => policy_expr_references_column(condition, column),
        PolicyExpr::And(exprs) | PolicyExpr::Or(exprs) => exprs
            .iter()
            .any(|e| policy_expr_references_column(e, column)),
        PolicyExpr::Not(inner) => policy_expr_references_column(inner, column),
        // Session-only predicates touch no row columns. ExistsRel carries
        // relation IR whose column references are validated by the relation
        // layer; encrypted columns there are a known v1 gap (spec §11).
        PolicyExpr::SessionCmp { .. }
        | PolicyExpr::SessionIsNull { .. }
        | PolicyExpr::SessionIsNotNull { .. }
        | PolicyExpr::SessionContains { .. }
        | PolicyExpr::SessionInList { .. }
        | PolicyExpr::ExistsRel { .. }
        | PolicyExpr::True
        | PolicyExpr::False => false,
    }
}

/// Validate E2EE schema rules (spec §2):
/// - user table names must not contain `$` (only generated `$keys` companions);
/// - `encrypted_with` must name a non-nullable sibling ref column whose target
///   is an `encryption_space` table;
/// - encrypted columns cannot be indexed or referenced by policies.
pub fn validate_e2ee_schema(schema: &Schema) -> Result<(), String> {
    for (table_name, table) in schema {
        let name = table_name.as_str();
        if name.contains('$') {
            let base = name.strip_suffix(E2EE_KEYS_TABLE_SUFFIX);
            let is_generated_companion = base.is_some_and(|base| {
                !base.contains('$')
                    && schema
                        .get(&TableName::new(base))
                        .is_some_and(|t| t.encryption_space)
            });
            if !is_generated_companion {
                return Err(format!(
                    "table '{name}': '$' is reserved for framework tables"
                ));
            }
        }

        for col in &table.columns.columns {
            let Some(space_ref) = &col.encrypted_with else {
                continue;
            };
            let col_name = col.name.as_str();
            let Some(ref_col) = table
                .columns
                .columns
                .iter()
                .find(|c| c.name.as_str() == space_ref.as_str())
            else {
                return Err(format!(
                    "table '{name}': encrypted column '{col_name}' names unknown ref column '{space_ref}'"
                ));
            };
            if ref_col.nullable {
                return Err(format!(
                    "table '{name}': encrypted column '{col_name}' requires a non-nullable ref column '{space_ref}'"
                ));
            }
            let Some(target) = &ref_col.references else {
                return Err(format!(
                    "table '{name}': encrypted column '{col_name}' requires '{space_ref}' to be a ref column"
                ));
            };
            let Some(target_table) = schema.get(target) else {
                return Err(format!(
                    "table '{name}': encrypted column '{col_name}' references unknown table '{target}'"
                ));
            };
            if !target_table.encryption_space {
                return Err(format!(
                    "table '{name}': encrypted column '{col_name}' references '{target}', which is not an encryption space"
                ));
            }
            if let Some(indexed) = &table.indexed_columns {
                if indexed.iter().any(|c| c.as_str() == col_name) {
                    return Err(format!(
                        "table '{name}': encrypted column '{col_name}' cannot be indexed"
                    ));
                }
            }

            let policies = [
                &table.policies.select,
                &table.policies.insert,
                &table.policies.update,
                &table.policies.delete,
            ];
            for policy in policies {
                for expr in [&policy.using, &policy.with_check].into_iter().flatten() {
                    if policy_expr_references_column(expr, col_name) {
                        return Err(format!(
                            "table '{name}': policy references encrypted column '{col_name}'"
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}
