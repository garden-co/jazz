//! Bridge the administrative catalogue into the WebSocket-serving core shell.

use std::collections::{BTreeMap, HashMap};

use jazz::groove::records::Value as CoreValue;
use jazz::protocol::{LensOp as CoreLensOp, MigrationLens, TableLens};
use jazz::schema::JazzSchema;

use jazz::tools::public_schema::{Schema, SchemaHash, TableName, Value};
use jazz::tools::schema_lens::{Lens, LensOp};

use super::{ServerRuntimeHandle, ServerState};

/// Publish newly admitted catalogue entries into the active runtime shell.
///
/// The caller persists administrative entries first. This shared bridge then
/// uses the first schema to bootstrap an absent runtime, admits later schemas
/// atomically with lineage lenses, and applies the current permissions head
/// last so it alone selects the write schema.
pub(crate) async fn publish_runtime_catalogue(
    state: &ServerState,
    schemas: &[Schema],
    lenses: &[Lens],
) -> Result<(), String> {
    #[cfg(test)]
    state.run_runtime_catalogue_before_publication_hook_for_test();
    // A bridge re-reads the durable permissions head before queueing the shell
    // update. Keep the read and queued update ordered with every other bridge:
    // otherwise a later head can install first and then be overwritten by this
    // older bridge.
    let _publication = state.runtime_catalogue_publication.lock().await;
    if state.runtime().is_none() && state.core_server_shell_storage_config.is_none() {
        return Ok(());
    }

    let mut shell = state.runtime();
    let supplied_schemas = schemas
        .iter()
        .cloned()
        .map(|schema| (SchemaHash::compute(&schema), schema))
        .collect::<HashMap<_, _>>();
    for schema in schemas {
        let runtime_schema = JazzSchema::new(schema)
            .map_err(|error| format!("convert catalogue schema for runtime: {error}"))?;
        runtime_shell(state, &mut shell, runtime_schema)?;
    }

    for lens in lenses {
        let source_schema = known_schema(state, &supplied_schemas, lens.source_hash)?;
        let target_schema = known_schema(state, &supplied_schemas, lens.target_hash)?;
        let runtime_lens = convert_lens(lens, &source_schema, &target_schema)?;
        let new_tables = lens
            .forward
            .ops
            .iter()
            .filter_map(|op| match op {
                LensOp::AddTable { table, .. } => Some(table.as_str().to_owned()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let dropped_tables = lens
            .forward
            .ops
            .iter()
            .filter_map(|op| match op {
                LensOp::RemoveTable { table, .. } => Some(table.as_str().to_owned()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let initial_schema = JazzSchema::new(&source_schema)
            .map_err(|error| format!("convert lens source schema for runtime: {error}"))?;
        let target_runtime = JazzSchema::new(&target_schema)
            .map_err(|error| format!("convert lens target schema: {error}"))?;
        let runtime_shell = runtime_shell(state, &mut shell, initial_schema)?;
        runtime_shell
            .publish_schema_with_lens(target_runtime, runtime_lens, new_tables, dropped_tables)
            .await
            .map_err(|error| format!("publish schema lineage to runtime shell: {error}"))?;
    }

    let permissions = state
        .catalogue
        .current_permissions(&state.catalogue_store)
        .map_err(|error| format!("read permissions head for runtime: {error}"))?;
    #[cfg(test)]
    state.run_runtime_catalogue_after_permissions_read_hook_for_test();
    let Some(permissions) = permissions else {
        return Ok(());
    };
    let mut schema = known_schema(state, &supplied_schemas, permissions.head.schema_hash)?;
    let structural_runtime = JazzSchema::new(&schema)
        .map_err(|error| format!("convert permissions lineage source schema: {error}"))?;
    let lineage_source = structural_runtime.version_id();
    let runtime_shell = runtime_shell(state, &mut shell, structural_runtime)?;
    for (table_name, policies) in permissions.permissions {
        let table = schema.get_mut(&table_name).ok_or_else(|| {
            format!(
                "permissions head references table {} absent from schema {}",
                table_name.as_str(),
                permissions.head.schema_hash
            )
        })?;
        table.policies = policies;
    }
    runtime_shell
        .publish_permissions_source(schema, lineage_source)
        .await
        .map_err(|error| format!("compile and publish permissions source: {error}"))?;
    Ok(())
}

fn runtime_shell(
    state: &ServerState,
    shell: &mut Option<ServerRuntimeHandle>,
    initial_schema: jazz::schema::JazzSchema,
) -> Result<ServerRuntimeHandle, String> {
    if let Some(shell) = shell.clone() {
        return Ok(shell);
    }
    let started = state.start_core_server_shell(initial_schema)?;
    *shell = Some(started.clone());
    Ok(started)
}

fn known_schema(
    state: &ServerState,
    supplied_schemas: &HashMap<SchemaHash, Schema>,
    hash: SchemaHash,
) -> Result<Schema, String> {
    if let Some(schema) = state
        .catalogue
        .known_schema(&state.catalogue_store, &hash)
        .map_err(|error| format!("read catalogue schema {hash}: {error}"))?
        .or_else(|| supplied_schemas.get(&hash).cloned())
    {
        return Ok(schema);
    }

    let empty_schema = Schema::new();
    if hash == SchemaHash::compute(&empty_schema) {
        return Ok(empty_schema);
    }

    Err(format!("catalogue schema {hash} is missing"))
}

fn convert_lens(lens: &Lens, source: &Schema, target: &Schema) -> Result<MigrationLens, String> {
    let source_runtime =
        JazzSchema::new(source).map_err(|error| format!("convert lens source schema: {error}"))?;
    let target_runtime =
        JazzSchema::new(target).map_err(|error| format!("convert lens target schema: {error}"))?;

    let renamed_tables = lens
        .forward
        .ops
        .iter()
        .filter_map(|op| match op {
            LensOp::RenameTable { old_name, new_name } => {
                Some((old_name.as_str(), new_name.as_str()))
            }
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let mut table_lenses = source
        .iter()
        .filter_map(|(source_name, _)| {
            let source_name = source_name.as_str();
            let target_name = renamed_tables
                .get(source_name)
                .copied()
                .unwrap_or(source_name);
            target
                .contains_key(&TableName::from(target_name))
                .then(|| TableLens {
                    source_table: source_name.to_owned(),
                    target_table: target_name.to_owned(),
                    ops: (source_name != target_name)
                        .then(|| CoreLensOp::RenameTable {
                            from: source_name.to_owned(),
                            to: target_name.to_owned(),
                        })
                        .into_iter()
                        .collect(),
                })
        })
        .collect::<Vec<_>>();

    for op in &lens.forward.ops {
        let (table_name, runtime_op) = match op {
            LensOp::RenameTable { .. } | LensOp::AddTable { .. } | LensOp::RemoveTable { .. } => {
                continue;
            }
            LensOp::AddColumn {
                table,
                column,
                default,
                ..
            } => (
                table.as_str(),
                CoreLensOp::AddColumn {
                    column: column.clone(),
                    default: public_value_to_core(default.clone())?,
                },
            ),
            LensOp::RemoveColumn {
                table,
                column,
                default,
                ..
            } => (
                table.as_str(),
                CoreLensOp::DropColumn {
                    column: column.clone(),
                    backwards_default: public_value_to_core(default.clone())?,
                },
            ),
            LensOp::RenameColumn {
                table,
                old_name,
                new_name,
            } => (
                table.as_str(),
                CoreLensOp::RenameColumn {
                    from: old_name.clone(),
                    to: new_name.clone(),
                },
            ),
        };
        let table_lens = table_lenses
            .iter_mut()
            .find(|candidate| {
                candidate.source_table == table_name || candidate.target_table == table_name
            })
            .ok_or_else(|| format!("lens operation references unknown table {table_name}"))?;
        table_lens.ops.push(runtime_op);
    }

    MigrationLens::new(
        source_runtime.version_id(),
        target_runtime.version_id(),
        table_lenses,
    )
    .map_err(str::to_owned)
}

fn public_value_to_core(value: Value) -> Result<CoreValue, String> {
    match value {
        Value::Boolean(value) => Ok(CoreValue::Bool(value)),
        Value::Text(value) => Ok(CoreValue::String(value)),
        Value::Integer(value) => Ok(CoreValue::I32(value)),
        Value::BigInt(value) => Ok(CoreValue::I64(value)),
        Value::Double(value) => Ok(CoreValue::F64(value)),
        Value::Timestamp(value) => Ok(CoreValue::U64(value)),
        Value::Uuid(value) => Ok(CoreValue::Uuid(*value.uuid())),
        Value::Bytea(value) => Ok(CoreValue::Bytes(value)),
        Value::Null => Ok(CoreValue::Nullable(None)),
        Value::Array(values) => values
            .into_iter()
            .map(public_value_to_core)
            .collect::<Result<Vec<_>, _>>()
            .map(CoreValue::Array),
        Value::Enum { .. } => Err(
            "migration lens enum payload default is not supported by the runtime core".to_owned(),
        ),
        Value::TransactionId(_) | Value::Row { .. } => {
            Err("migration lens default is not supported by the runtime core".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_lens_defaults_preserve_logical_signed_scalars_and_nested_arrays() {
        for value in [i32::MIN, -1, 0, i32::MAX] {
            assert_eq!(
                public_value_to_core(Value::Integer(value)),
                Ok(CoreValue::I32(value))
            );
        }
        for value in [i64::MIN, -1, 0, i64::MAX] {
            assert_eq!(
                public_value_to_core(Value::BigInt(value)),
                Ok(CoreValue::I64(value))
            );
        }

        assert_eq!(
            public_value_to_core(Value::Array(vec![
                Value::Integer(-7),
                Value::Array(vec![
                    Value::Integer(8),
                    Value::BigInt(i64::MIN),
                    Value::Null,
                ]),
            ])),
            Ok(CoreValue::Array(vec![
                CoreValue::I32(-7),
                CoreValue::Array(vec![
                    CoreValue::I32(8),
                    CoreValue::I64(i64::MIN),
                    CoreValue::Nullable(None),
                ]),
            ]))
        );
    }
}
