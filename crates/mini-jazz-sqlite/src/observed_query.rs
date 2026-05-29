use crate::query_api::BuiltQuery;
use crate::query_observation::{built_query_from_read, observed_ids_from_query_value};
use crate::sync::QueryReadRecord;
use crate::Result;
use serde_json::Value as JsonValue;

#[derive(Clone, Debug)]
pub(crate) enum ObservedQuery {
    RecursiveRefs {
        root_id: String,
    },
    Built {
        query: BuiltQuery,
        observed_ids: Vec<String>,
    },
    Absent,
}

pub(crate) fn decode(read: &QueryReadRecord) -> Result<ObservedQuery> {
    match read.op.as_str() {
        "recursive_refs" => {
            let Some(root_id) = read.value.as_str() else {
                return Err(crate::Error::new("recursive refs expects root id string"));
            };
            Ok(ObservedQuery::RecursiveRefs {
                root_id: root_id.to_owned(),
            })
        }
        "query" => Ok(ObservedQuery::Built {
            query: built_query_from_read(read)?,
            observed_ids: observed_ids_from_query_value(&read.value)?,
        }),
        "absent" => Ok(ObservedQuery::Absent),
        op => Err(crate::Error::new(format!(
            "unsupported observed query op {op}"
        ))),
    }
}

pub(crate) fn identity_value(read: &QueryReadRecord) -> Result<JsonValue> {
    match decode(read)? {
        ObservedQuery::Built { query, .. } => Ok(query.to_json_value()),
        _ => Ok(read.value.clone()),
    }
}
