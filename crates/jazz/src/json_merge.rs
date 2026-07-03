//! JSON document rung-3 merge strategy.
//!
//! The v1 strategy canonicalizes whole authored JSON values by sorting object
//! keys and emitting compact JSON. Text-at-path merges use a string-local diff
//! rather than projecting document-level ops into substring ranges.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::{Map, Number, Value};

use crate::merge_strategy::{
    CanonicalizeInput, MergeStrategy, MergeStrategyInput, MergeStrategyOutput,
};
use crate::text_merge::{TextMergeError, TieBreak, diff, merge_concurrent_ops};

/// Stable strategy id for the built-in JSON document strategy.
pub const JSON_MERGE_STRATEGY_ID: &str = "builtin.json-document-v1";
/// Current implementation version for [`JsonMergeStrategy`].
pub const JSON_MERGE_STRATEGY_VERSION: u32 = 1;

/// Built-in JSON document strategy.
#[derive(Clone, Debug, Default)]
pub struct JsonMergeStrategy;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum PathKind {
    Lww,
    Set,
    Counter,
    List,
    Text,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct JsonMergeConfig {
    #[serde(default)]
    paths: BTreeMap<String, PathKind>,
}

impl MergeStrategy for JsonMergeStrategy {
    fn id(&self) -> &str {
        JSON_MERGE_STRATEGY_ID
    }

    fn version(&self) -> u32 {
        JSON_MERGE_STRATEGY_VERSION
    }

    fn canonicalize(
        &self,
        authored: &[u8],
        _input: &CanonicalizeInput,
    ) -> Result<Option<Vec<u8>>, TextMergeError> {
        let value = parse_json(authored)?;
        Ok(Some(canonical_json_bytes(&value)))
    }

    fn structural_proximity(&self, input: &MergeStrategyInput) -> bool {
        let Ok(base) = parse_json(&input.base) else {
            return true;
        };
        let Ok(left) = parse_json(&input.left.materialized) else {
            return true;
        };
        let Ok(right) = parse_json(&input.right.materialized) else {
            return true;
        };
        let mut left_paths = BTreeSet::new();
        let mut right_paths = BTreeSet::new();
        changed_paths(&base, &left, Vec::new(), &mut left_paths);
        changed_paths(&base, &right, Vec::new(), &mut right_paths);
        left_paths.iter().any(|left| {
            right_paths
                .iter()
                .any(|right| paths_are_same_or_adjacent(left, right))
        })
    }

    fn merge(&self, input: &MergeStrategyInput) -> Result<MergeStrategyOutput, TextMergeError> {
        let config = config(input)?;
        let base = parse_json(&input.base)?;
        let left = parse_json(&input.left.materialized)?;
        let right = parse_json(&input.right.materialized)?;
        let merged = merge_json_value(&base, &left, &right, &[], &config)?;
        let canonical = canonical_json_bytes(&merged);
        Ok(MergeStrategyOutput {
            op_against_base: diff(&input.base, &canonical),
            strategy_id: self.id().to_owned(),
            strategy_version: self.version(),
        })
    }
}

fn config(input: &MergeStrategyInput) -> Result<JsonMergeConfig, TextMergeError> {
    if input.spec.config.is_empty() {
        return Ok(JsonMergeConfig::default());
    }
    serde_json::from_slice(&input.spec.config).map_err(|_| TextMergeError::StrategyInputInvalid)
}

fn parse_json(bytes: &[u8]) -> Result<Value, TextMergeError> {
    serde_json::from_slice(bytes).map_err(|_| TextMergeError::StrategyInputInvalid)
}

fn merge_json_value(
    base: &Value,
    left: &Value,
    right: &Value,
    path: &[String],
    config: &JsonMergeConfig,
) -> Result<Value, TextMergeError> {
    if left == right {
        return Ok(left.clone());
    }
    if left == base {
        return Ok(right.clone());
    }
    if right == base {
        return Ok(left.clone());
    }

    match kind_for_path(path, config) {
        Some(PathKind::Lww) => Ok(right.clone()),
        Some(PathKind::Set) => Ok(merge_set(base, left, right)),
        Some(PathKind::Counter) => {
            Ok(merge_counter(base, left, right).unwrap_or_else(|| right.clone()))
        }
        Some(PathKind::List) => Ok(merge_list(base, left, right).unwrap_or_else(|| right.clone())),
        Some(PathKind::Text) => merge_text(base, left, right),
        None => match (base, left, right) {
            (Value::Object(base), Value::Object(left), Value::Object(right)) => {
                merge_object(base, left, right, path, config)
            }
            _ => Ok(right.clone()),
        },
    }
}

fn merge_object(
    base: &Map<String, Value>,
    left: &Map<String, Value>,
    right: &Map<String, Value>,
    path: &[String],
    config: &JsonMergeConfig,
) -> Result<Value, TextMergeError> {
    let keys = base
        .keys()
        .chain(left.keys())
        .chain(right.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut merged = Map::new();
    for key in keys {
        let base_value = base.get(&key).unwrap_or(&Value::Null);
        let left_value = left.get(&key);
        let right_value = right.get(&key);
        let value = match (left_value, right_value) {
            (Some(left), Some(right)) => {
                let mut child = path.to_vec();
                child.push(key.clone());
                Some(merge_json_value(base_value, left, right, &child, config)?)
            }
            (Some(left), None) => {
                if base.contains_key(&key) && left == base_value {
                    None
                } else {
                    Some(left.clone())
                }
            }
            (None, Some(right)) => {
                if base.contains_key(&key) && right == base_value {
                    None
                } else {
                    Some(right.clone())
                }
            }
            (None, None) => None,
        };
        if let Some(value) = value {
            merged.insert(key, value);
        }
    }
    Ok(Value::Object(merged))
}

fn kind_for_path(path: &[String], config: &JsonMergeConfig) -> Option<PathKind> {
    config.paths.get(&path.join(".")).copied()
}

fn merge_set(base: &Value, left: &Value, right: &Value) -> Value {
    let (Some(base), Some(left), Some(right)) =
        (base.as_array(), left.as_array(), right.as_array())
    else {
        return right.clone();
    };
    let base = value_set(base);
    let left = value_set(left);
    let right = value_set(right);
    let all = base
        .iter()
        .chain(left.iter())
        .chain(right.iter())
        .cloned()
        .collect::<BTreeSet<_>>();
    Value::Array(
        all.into_iter()
            .filter_map(|canonical| {
                let was_present = base.contains(&canonical);
                let left_has = left.contains(&canonical);
                let right_has = right.contains(&canonical);
                let keep = if was_present {
                    left_has && right_has
                } else {
                    left_has || right_has
                };
                keep.then(|| serde_json::from_str(&canonical).expect("canonical set value"))
            })
            .collect(),
    )
}

fn value_set(values: &[Value]) -> BTreeSet<String> {
    values
        .iter()
        .map(|value| String::from_utf8(canonical_json_bytes(value)).expect("json is utf8"))
        .collect()
}

fn merge_counter(base: &Value, left: &Value, right: &Value) -> Option<Value> {
    let base = number_i128(base)?;
    let left = number_i128(left)?;
    let right = number_i128(right)?;
    i64::try_from(left + right - base)
        .ok()
        .map(|value| Value::Number(Number::from(value)))
}

fn number_i128(value: &Value) -> Option<i128> {
    value
        .as_i64()
        .map(i128::from)
        .or_else(|| value.as_u64().map(|value| value as i128))
}

fn merge_list(base: &Value, left: &Value, right: &Value) -> Option<Value> {
    let (Some(base), Some(left), Some(right)) =
        (base.as_array(), left.as_array(), right.as_array())
    else {
        return None;
    };
    let left_edits = list_edits(base, left);
    let right_edits = list_edits(base, right);
    let mut merged = Vec::new();
    for pos in 0..=base.len() {
        append_list_inserts(&mut merged, &left_edits.inserts, pos);
        append_list_inserts(&mut merged, &right_edits.inserts, pos);
        if pos < base.len()
            && !left_edits.deletes.contains(&pos)
            && !right_edits.deletes.contains(&pos)
        {
            merged.push(base[pos].clone());
        }
    }
    Some(Value::Array(merged))
}

#[derive(Clone, Debug, Default)]
struct ListEdits {
    inserts: BTreeMap<usize, Vec<Value>>,
    deletes: BTreeSet<usize>,
}

fn list_edits(base: &[Value], side: &[Value]) -> ListEdits {
    let mut edits = ListEdits::default();
    let mut base_pos = 0usize;
    for value in side {
        if base_pos < base.len() && value == &base[base_pos] {
            base_pos += 1;
        } else {
            edits
                .inserts
                .entry(base_pos)
                .or_default()
                .push(value.clone());
        }
    }
    for pos in base_pos..base.len() {
        edits.deletes.insert(pos);
    }
    edits
}

fn append_list_inserts(merged: &mut Vec<Value>, inserts: &BTreeMap<usize, Vec<Value>>, pos: usize) {
    if let Some(values) = inserts.get(&pos) {
        merged.extend(values.iter().cloned());
    }
}

fn merge_text(base: &Value, left: &Value, right: &Value) -> Result<Value, TextMergeError> {
    let (Some(base), Some(left), Some(right)) = (base.as_str(), left.as_str(), right.as_str())
    else {
        return Ok(right.clone());
    };
    let left_op = diff(base.as_bytes(), left.as_bytes());
    let right_op = diff(base.as_bytes(), right.as_bytes());
    let merged = merge_concurrent_ops(
        base.as_bytes(),
        [(&left_op, TieBreak(1)), (&right_op, TieBreak(2))],
    )?;
    String::from_utf8(merged)
        .map(Value::String)
        .map_err(|_| TextMergeError::StrategyInputInvalid)
}

fn changed_paths(base: &Value, value: &Value, path: Vec<String>, out: &mut BTreeSet<Vec<String>>) {
    if base == value {
        return;
    }
    match (base, value) {
        (Value::Object(base), Value::Object(value)) => {
            let keys = base
                .keys()
                .chain(value.keys())
                .cloned()
                .collect::<BTreeSet<_>>();
            for key in keys {
                let mut child = path.clone();
                child.push(key.clone());
                changed_paths(
                    base.get(&key).unwrap_or(&Value::Null),
                    value.get(&key).unwrap_or(&Value::Null),
                    child,
                    out,
                );
            }
        }
        _ => {
            out.insert(path);
        }
    }
}

fn paths_are_same_or_adjacent(left: &[String], right: &[String]) -> bool {
    left == right
        || left.starts_with(right)
        || right.starts_with(left)
        || (!left.is_empty()
            && !right.is_empty()
            && left[..left.len() - 1] == right[..right.len() - 1])
}

/// Return compact canonical JSON bytes with lexicographically sorted object keys.
pub fn canonical_json_bytes(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    write_canonical_json(value, &mut out);
    out
}

fn write_canonical_json(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(true) => out.extend_from_slice(b"true"),
        Value::Bool(false) => out.extend_from_slice(b"false"),
        Value::Number(number) => out.extend_from_slice(number.to_string().as_bytes()),
        Value::String(string) => {
            out.extend_from_slice(
                serde_json::to_string(string)
                    .expect("string json")
                    .as_bytes(),
            );
        }
        Value::Array(values) => {
            out.push(b'[');
            for (idx, value) in values.iter().enumerate() {
                if idx > 0 {
                    out.push(b',');
                }
                write_canonical_json(value, out);
            }
            out.push(b']');
        }
        Value::Object(map) => {
            out.push(b'{');
            for (idx, (key, value)) in map.iter().collect::<BTreeMap<_, _>>().iter().enumerate() {
                if idx > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(serde_json::to_string(key).expect("key json").as_bytes());
                out.push(b':');
                write_canonical_json(value, out);
            }
            out.push(b'}');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{NodeUuid, SchemaVersionId};
    use crate::merge_strategy::testing::{Expected, IntentionCase, run_intention_case};
    use crate::schema::TextMergeSpec;
    use crate::time::TxTime;
    use crate::tx::TxId;

    fn spec(paths: &[(&str, &str)]) -> TextMergeSpec {
        let paths = paths
            .iter()
            .map(|(path, kind)| ((*path).to_owned(), Value::String((*kind).to_owned())))
            .collect::<Map<_, _>>();
        let config = Value::Object(Map::from_iter([("paths".to_owned(), Value::Object(paths))]));
        TextMergeSpec::new(
            JSON_MERGE_STRATEGY_ID,
            JSON_MERGE_STRATEGY_VERSION,
            canonical_json_bytes(&config),
        )
    }

    fn run(base: &str, left: &str, right: &str, spec: TextMergeSpec, expected: &str) {
        let strategy = JsonMergeStrategy;
        run_intention_case(
            &strategy,
            IntentionCase {
                base: base.as_bytes().to_vec(),
                side_a: left.as_bytes().to_vec(),
                side_b: right.as_bytes().to_vec(),
                spec,
                expected: Expected::Exact(expected.as_bytes().to_vec()),
            },
            SchemaVersionId(uuid::Uuid::from_u128(1)),
            TxId::new(TxTime(1), NodeUuid(uuid::Uuid::from_u128(1))),
            TxId::new(TxTime(2), NodeUuid(uuid::Uuid::from_u128(2))),
        )
        .unwrap();
    }

    #[test]
    fn disjoint_path_edits_both_apply() {
        run(
            r#"{"a":1,"b":2}"#,
            r#"{"a":3,"b":2}"#,
            r#"{"a":1,"b":4}"#,
            spec(&[]),
            r#"{"a":3,"b":4}"#,
        );
    }

    #[test]
    fn same_scalar_conflict_uses_lww_by_tx_id() {
        run(
            r#"{"a":1}"#,
            r#"{"a":2}"#,
            r#"{"a":3}"#,
            spec(&[]),
            r#"{"a":3}"#,
        );
    }

    #[test]
    fn set_add_add_and_add_remove_merge_membership() {
        run(
            r#"{"tags":["a"]}"#,
            r#"{"tags":["a","b"]}"#,
            r#"{"tags":["b"]}"#,
            spec(&[("tags", "set")]),
            r#"{"tags":["b"]}"#,
        );
        run(
            r#"{"tags":[]}"#,
            r#"{"tags":["a"]}"#,
            r#"{"tags":["b"]}"#,
            spec(&[("tags", "set")]),
            r#"{"tags":["a","b"]}"#,
        );
    }

    #[test]
    fn counter_concurrent_increments_are_summed() {
        run(
            r#"{"n":10}"#,
            r#"{"n":13}"#,
            r#"{"n":17}"#,
            spec(&[("n", "counter")]),
            r#"{"n":20}"#,
        );
    }

    #[test]
    fn list_concurrent_inserts_use_tie_break_order() {
        run(
            r#"{"items":["a","d"]}"#,
            r#"{"items":["a","b","d"]}"#,
            r#"{"items":["a","c","d"]}"#,
            spec(&[("items", "list")]),
            r#"{"items":["a","b","c","d"]}"#,
        );
    }

    #[test]
    fn list_reorder_and_insert_use_sequence_walk() {
        run(
            r#"{"items":["a","b"]}"#,
            r#"{"items":["b","a"]}"#,
            r#"{"items":["a","b","c"]}"#,
            spec(&[("items", "list")]),
            r#"{"items":["b","a","c"]}"#,
        );
    }

    #[test]
    fn text_at_path_merges_with_string_local_diff() {
        run(
            r#"{"body":"ac"}"#,
            r#"{"body":"abc"}"#,
            r#"{"body":"aXc"}"#,
            spec(&[("body", "text")]),
            r#"{"body":"abXc"}"#,
        );
    }

    #[test]
    fn nested_object_recursion_merges_children() {
        run(
            r#"{"outer":{"a":1,"b":2}}"#,
            r#"{"outer":{"a":3,"b":2}}"#,
            r#"{"outer":{"a":1,"b":4}}"#,
            spec(&[]),
            r#"{"outer":{"a":3,"b":4}}"#,
        );
    }

    #[test]
    fn canonical_form_invariance() {
        run(
            "{\n  \"b\": 2, \"a\": 1\n}",
            r#"{"b":2,"a":3}"#,
            r#"{"a":1,"b":4}"#,
            spec(&[]),
            r#"{"a":3,"b":4}"#,
        );
    }
}
