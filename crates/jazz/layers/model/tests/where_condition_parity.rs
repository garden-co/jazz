//! What each `{ column, op, value }` where condition means, for every client.
//!
//! This is a lower-level test on purpose: `fixtures/where_condition_parity.json`
//! is the shared contract between the FFI's `where` entries and TS's
//! `where({ … })` lowering (jazz#5929: they disagreed on `isNull: false`).
//! This test pins that the Rust decoder gives each fixture case its meaning;
//! `packages/jazz-tools/src/runtime/where-condition-parity.test.ts` runs the
//! same fixture through TS's lowering.

use jazz_model::query::where_condition::{
    WhereCompare, WhereTest, WhereValueRole, decode_where_condition,
};
use jazz_model::query::{Operand, Predicate};
use serde_json::{Value, json};

#[derive(serde::Deserialize)]
struct Case {
    description: String,
    condition: Value,
    means: Value,
}

fn cases() -> Vec<Case> {
    serde_json::from_str(include_str!("fixtures/where_condition_parity.json"))
        .expect("parity fixture parses")
}

fn meaning(condition: &Value) -> Value {
    let Ok(decoded) = decode_where_condition(condition) else {
        return json!("refused");
    };
    match decoded.test {
        WhereTest::Compare(compare, _) => json!({ "compare": match compare {
            WhereCompare::Eq => "eq",
            WhereCompare::Ne => "ne",
            WhereCompare::Lt => "lt",
            WhereCompare::Lte => "lte",
            WhereCompare::Gt => "gt",
            WhereCompare::Gte => "gte",
        } }),
        WhereTest::Null(is_null) => json!({ "isNull": is_null }),
        WhereTest::In { values, negated } => json!({ "in": values.len(), "negated": negated }),
        WhereTest::Contains(_) => json!({ "contains": true }),
    }
}

/// Every fixture condition decodes to the meaning the fixture records, or is
/// refused where it says so.
#[test]
fn where_conditions_mean_what_the_parity_fixture_says() {
    let cases = cases();
    assert!(!cases.is_empty());
    for case in cases {
        assert_eq!(
            meaning(&case.condition),
            case.means,
            "{}: {}",
            case.description,
            case.condition
        );
    }
}

/// `isNull: false` and `notIn` lower to negations of the positive test, and
/// `contains` asks for its value as a part of the column.
#[test]
fn negative_conditions_lower_to_negated_predicates() {
    let lower = |condition: Value| {
        decode_where_condition(&condition)
            .unwrap()
            .to_predicate(Operand::Column("doneAt".into()), |value, role| {
                Ok::<_, ()>(Operand::Column(format!("{role:?}:{value}")))
            })
            .unwrap()
    };
    let column = || Operand::Column("doneAt".into());
    assert_eq!(
        lower(json!({ "column": "doneAt", "op": "isNull", "value": false })),
        Predicate::Not(Box::new(Predicate::IsNull(column())))
    );
    assert_eq!(
        lower(json!({ "column": "doneAt", "op": "notIn", "value": [1] })),
        Predicate::Not(Box::new(Predicate::In(
            column(),
            vec![Operand::Column(format!("{:?}:1", WhereValueRole::Whole))]
        )))
    );
    assert_eq!(
        lower(json!({ "column": "doneAt", "op": "contains", "value": "x" })),
        Predicate::Contains(
            column(),
            Operand::Column(format!("{:?}:\"x\"", WhereValueRole::ContainsPart))
        )
    );
}
