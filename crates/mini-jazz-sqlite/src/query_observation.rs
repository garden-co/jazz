use crate::query_api::BuiltQuery;
use crate::sync::QueryReadRecord;
use crate::types::RowView;
use crate::{Error, Result};
use serde_json::{json, Value as JsonValue};

pub(crate) fn built_query_from_read(read: &QueryReadRecord) -> Result<BuiltQuery> {
    let query_value = read
        .value
        .get("query")
        .cloned()
        .unwrap_or_else(|| read.value.clone());
    let query = BuiltQuery::from_json_value(query_value)?;
    if read.table != query.table {
        return Err(Error::new("query read table does not match descriptor"));
    }
    Ok(query)
}

pub(crate) fn built_query_read_value(query: &BuiltQuery, rows: &[RowView]) -> JsonValue {
    let mut value = query.to_json_value();
    if let JsonValue::Object(object) = &mut value {
        object.insert("observed_ids".to_owned(), json!(observed_row_ids(rows)));
    }
    value
}

pub(crate) fn observed_row_ids(rows: &[RowView]) -> Vec<String> {
    rows.iter().map(|row| row.id.clone()).collect()
}

pub(crate) fn observed_ids_from_query_value(value: &JsonValue) -> Result<Vec<String>> {
    let Some(observed_ids) = value.get("observed_ids") else {
        return Ok(Vec::new());
    };
    observed_ids
        .as_array()
        .ok_or_else(|| Error::new("observed_ids expects an array"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::new("observed_ids expects string row ids"))
        })
        .collect()
}

pub(crate) fn support_window_query(query: &BuiltQuery) -> Result<BuiltQuery> {
    let offset = query.offset.unwrap_or(0);
    if offset == 0 {
        return Ok(query.clone());
    }

    let mut support_query = query.clone();
    support_query.offset = None;
    support_query.limit = query
        .limit
        .map(|limit| {
            offset
                .checked_add(limit)
                .ok_or_else(|| Error::new("query limit plus offset is too large"))
        })
        .transpose()?;
    Ok(support_query)
}
