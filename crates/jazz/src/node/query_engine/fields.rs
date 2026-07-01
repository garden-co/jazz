use super::ClaimPath;

pub(crate) const USER_COLUMN_PREFIX: &str = "user_";
pub(crate) const LEFT_JOIN_PREFIX: &str = "left.";
pub(crate) const RIGHT_JOIN_PREFIX: &str = "right.";
pub(crate) const CLOSURE_REQUIRED_ELEMENT: &str = "__closure_required_element";

const ROUTE_PARAM_PREFIX: &str = "__jazz_route_";
const CLAIM_PARAM_PREFIX: &str = "__jazz_claim_";

pub(crate) fn user_column_field(column: &str) -> String {
    format!("{USER_COLUMN_PREFIX}{column}")
}

pub(crate) fn logical_user_column(field: &str) -> &str {
    field.strip_prefix(USER_COLUMN_PREFIX).unwrap_or(field)
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
