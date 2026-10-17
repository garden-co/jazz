//! Node query-row adapter for the shared canonical aggregate result codec.
use super::{CurrentRow, Error};
use crate::tx::AggregateReadResult;
use groove::records::{Value, ValueType};
pub(super) use jazz_protocol::aggregate_read_result::{
    AggregateResultCell, transaction_aggregate_read_malformation,
};
fn invalid(what: &'static str) -> Error {
    Error::InvalidStoredValue(what)
}
fn aggregate_row_cells(row: &CurrentRow) -> Result<Vec<AggregateResultCell>, Error> {
    let borrowed = row.record.borrowed();
    let fields = row.record.descriptor().fields();
    let mut cells = Vec::new();
    for (idx, field) in row.publication_fields.iter().enumerate() {
        let Some(name) = field.application_name() else {
            continue;
        };
        let declared = &fields
            .get(idx)
            .ok_or(invalid("aggregate row field has no descriptor entry"))?
            .value_type;
        let value = match (borrowed.get_idx(idx)?, declared) {
            (Value::Nullable(None), _) => None,
            (Value::Nullable(Some(value)), ValueType::Nullable(inner)) => {
                Some((inner.as_ref().clone(), *value))
            }
            (Value::Nullable(Some(_)), _) => {
                return Err(invalid("aggregate row value differs from its descriptor"));
            }
            (value, value_type) => Some((value_type.clone(), value)),
        };
        cells.push(AggregateResultCell {
            name: name.to_owned(),
            value,
        });
    }
    Ok(cells)
}

/// Encode the result an aggregate read returned: one group per output row.
pub(super) fn aggregate_read_result_from_rows<'a>(
    rows: impl IntoIterator<Item = &'a CurrentRow>,
) -> Result<AggregateReadResult, Error> {
    Ok(
        jazz_protocol::aggregate_read_result::encode_aggregate_result_groups(
            rows.into_iter()
                .map(aggregate_row_cells)
                .collect::<Result<_, _>>()?,
        )?,
    )
}

#[cfg(test)]
pub(super) use jazz_protocol::aggregate_read_result::decode_aggregate_read_result;
