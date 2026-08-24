use super::ClaimPath;

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
const CLAIM_ROUTE_TOKEN_PREFIX: &str = "__jazz_claim_route_v1:";

pub(crate) fn user_column_field(column: &str) -> String {
    format!("{USER_COLUMN_PREFIX}{column}")
}

pub(crate) fn logical_user_column(field: &str) -> &str {
    field.strip_prefix(USER_COLUMN_PREFIX).unwrap_or(field)
}

pub(crate) fn aggregate_output_field(output: &str) -> String {
    aggregate_output_column(output)
}

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

/// Hidden scalar route carrier for a typed claim. The path encoding is
/// length-delimited so nested paths cannot collide with underscore-separated
/// legacy claim parameter names.
pub(crate) fn claim_route_token_field(path: &ClaimPath) -> String {
    let mut field = CLAIM_ROUTE_TOKEN_PREFIX.to_owned();
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

    #[test]
    fn non_scalar_claim_routes_use_distinct_hidden_field_names() {
        let teams = ClaimPath(vec!["team_ids".to_owned()]);
        let roles = ClaimPath(vec!["roles".to_owned()]);
        let teams_route = claim_route_token_field(&teams);
        let roles_route = claim_route_token_field(&roles);
        assert_ne!(teams_route, roles_route);
        assert!(teams_route.starts_with(CLAIM_ROUTE_TOKEN_PREFIX));
        assert!(roles_route.starts_with(CLAIM_ROUTE_TOKEN_PREFIX));
    }
}
