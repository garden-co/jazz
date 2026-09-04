use super::ClaimPath;
use crate::schema::TableSchema;
use groove::records::DescriptorField;

pub(crate) const USER_COLUMN_PREFIX: &str = "user_";
/// Physical namespace for aggregate result values.
///
/// Aggregate aliases are public-facing names and can legally collide with a
/// grouped source column (for example, `group_by("sum_score").sum("score")`).
/// Keep aggregate values separate from source-row fields in compiler records.
pub(crate) const AGGREGATE_OUTPUT_PREFIX: &str = "__jazz_aggregate_";
pub(crate) const LEFT_JOIN_PREFIX: &str = "left.";
pub(crate) const RIGHT_JOIN_PREFIX: &str = "right.";
pub(crate) const CLOSURE_REQUIRED_ELEMENT: &str = "__closure_required_element";

const ROUTE_PARAM_PREFIX: &str = "__jazz_route_";
const CLAIM_PARAM_PREFIX: &str = "__jazz_claim_";

pub(crate) fn user_column_field(column: &str) -> String {
    format!("{USER_COLUMN_PREFIX}{column}")
}

/// Canonical physical `CurrentRow` field order. Query readers and public
/// maintained terminals use the same row representation for shape-default
/// values, so this belongs to the compiler vocabulary rather than either
/// materialization path.
pub(crate) fn current_row_field_names(table: &TableSchema) -> Vec<String> {
    let mut fields = vec!["row_uuid".to_owned()];
    fields.extend(
        table
            .columns
            .iter()
            .map(|column| user_column_field(&column.name)),
    );
    fields.extend([
        "$createdBy".to_owned(),
        "$createdAt".to_owned(),
        "$updatedBy".to_owned(),
        "$updatedAt".to_owned(),
        "tx_time".to_owned(),
        "tx_node_id".to_owned(),
    ]);
    fields
}

pub(crate) fn aggregate_output_field(output: &str) -> String {
    aggregate_output_column(output)
}

/// Aggregate values in an application `CurrentRow` use the normal physical
/// cell namespace.  The unprefixed aggregate field is a compiler-internal
/// graph record name only; it must be normalized before it crosses the
/// app-row boundary.
pub(crate) fn aggregate_output_app_field(output: &str) -> String {
    user_column_field(&aggregate_output_field(output))
}

pub(crate) fn aggregate_output_column(output: &str) -> String {
    if output.starts_with(AGGREGATE_OUTPUT_PREFIX) {
        output.to_owned()
    } else {
        format!("{AGGREGATE_OUTPUT_PREFIX}{output}")
    }
}

pub(crate) fn aggregate_output_logical_name(column: &str) -> Option<&str> {
    column.strip_prefix(AGGREGATE_OUTPUT_PREFIX)
}

/// A typed application identity may use the spelling of an engine field.
/// Only unchanged engine carriers are private; an explicit different identity
/// must retain its application name.
pub(crate) fn descriptor_public_name(field: &DescriptorField) -> Option<&str> {
    let name = field.logical_name()?;
    if field.name.as_deref() != Some(name) {
        return Some(name);
    }
    if matches!(
        name,
        "$createdBy"
            | "$createdAt"
            | "$updatedBy"
            | "$updatedAt"
            | "created_by"
            | "created_at"
            | "updated_by"
            | "updated_at"
            | "branch_key"
            | "row_uuid"
            | "tx_time"
            | "tx_node_id"
            | "schema_version"
            | "parents"
            | "authored_columns"
            | "global_time"
            | "settle_position"
    ) {
        return None;
    }
    aggregate_output_logical_name(name).or_else(|| (!name.starts_with("__jazz_")).then_some(name))
}

pub(crate) fn join_field(prefix: &str, field: &str) -> String {
    format!("{prefix}{field}")
}

pub(crate) fn left_field(field: &str) -> String {
    join_field(LEFT_JOIN_PREFIX, field)
}

pub(crate) fn right_field(field: &str) -> String {
    join_field(RIGHT_JOIN_PREFIX, field)
}

pub(crate) fn route_param_field(param: &str) -> String {
    format!("{ROUTE_PARAM_PREFIX}{param}")
}

pub(crate) fn route_param_from_field(field: &str) -> Option<&str> {
    field.strip_prefix(ROUTE_PARAM_PREFIX)
}

pub(crate) fn claim_param_field(path: &ClaimPath) -> String {
    if let [segment] = path.0.as_slice()
        && !segment.contains('_')
        && !segment.contains(':')
    {
        return format!("{CLAIM_PARAM_PREFIX}{segment}");
    }
    let mut field = format!("{CLAIM_PARAM_PREFIX}v1:");
    for segment in &path.0 {
        field.push_str(&segment.len().to_string());
        field.push(':');
        field.push_str(segment);
    }
    field
}

pub(crate) fn claim_path_from_param_field(field: &str) -> Option<ClaimPath> {
    let mut rest = field.strip_prefix(CLAIM_PARAM_PREFIX)?;
    if let Some(typed) = rest.strip_prefix("typed:") {
        let (len, tail) = typed.split_once(':')?;
        let len = len.parse::<usize>().ok()?;
        if tail.len() <= len || tail.as_bytes().get(len) != Some(&b':') {
            return None;
        }
        return claim_path_from_param_field(&tail[len + 1..]);
    }
    if !rest.starts_with("v1:") {
        return Some(ClaimPath(rest.split('_').map(str::to_owned).collect()));
    }
    rest = rest.strip_prefix("v1:")?;
    let mut segments = Vec::new();
    while !rest.is_empty() {
        let (len, tail) = rest.split_once(':')?;
        let len = len.parse::<usize>().ok()?;
        if tail.len() < len {
            return None;
        }
        let (segment, next) = tail.split_at(len);
        segments.push(segment.to_owned());
        rest = next;
    }
    Some(ClaimPath(segments))
}

pub(crate) fn table_user_column_field(table: &str, column: &str) -> String {
    format!("user__{table}__{column}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_param_fields_round_trip_underscore_and_nested_paths() {
        for path in [
            ClaimPath(vec!["is_admin".to_owned()]),
            ClaimPath(vec!["team_claim".to_owned(), "is_admin".to_owned()]),
        ] {
            assert_eq!(
                claim_path_from_param_field(&claim_param_field(&path)),
                Some(path)
            );
        }
    }
}
