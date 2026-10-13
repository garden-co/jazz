//! Lower public catalogue lenses to the runtime vocabulary.
use super::{Lens, LensOp};
use crate::protocol::{LensOp as CoreLensOp, MigrationLens, TableLens};
use crate::schema::JazzSchema;
use crate::tools::public_schema::{Schema, TableName, Value};
use groove::records::Value as CoreValue;
use std::collections::BTreeMap;

pub fn compile_lens(
    lens: &Lens,
    source: &Schema,
    target: &Schema,
) -> Result<MigrationLens, String> {
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
