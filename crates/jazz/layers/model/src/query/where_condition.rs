//! The meaning of a where condition, `{ "column", "op", "value"? }`, decided
//! once for every client: TS's `where({ … })` lowering and the FFI's `where`
//! entries. Values stay JSON here; each caller types them against the column
//! (the condition grammar doesn't depend on how a value is typed).
//!
//! Operators and their values:
//!
//! | op | value | means |
//! |---|---|---|
//! | `eq`, `ne` | a value, or `null` | compare; `eq: null` is "is null", `ne: null` "is not null" |
//! | `lt`, `lte`, `gt`, `gte` | a value, not `null` | compare |
//! | `in`, `notIn` | an array of values, none `null` | membership, or its negation |
//! | `contains` | a value, not `null` | substring or array element |
//! | `isNull` | `true`, `false`, or absent (= `true`) | `false` means "is not null" |
//! | `isNotNull` | `true` or absent | "is not null" |
//!
//! Which operators a column's type supports is a separate rule.

use serde_json::Value as Json;

use super::{Operand, Predicate};

/// An ordered comparison or (in)equality.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WhereCompare {
    /// `eq`.
    Eq,
    /// `ne`.
    Ne,
    /// `lt`.
    Lt,
    /// `lte`.
    Lte,
    /// `gt`.
    Gt,
    /// `gte`.
    Gte,
}

/// What a decoded condition tests, with its JSON value(s) still untyped.
#[derive(Clone, Debug, PartialEq)]
pub enum WhereTest<'a> {
    /// The column compared with a non-null value.
    Compare(WhereCompare, &'a Json),
    /// The column is (`true`) or is not (`false`) null.
    Null(bool),
    /// The column is one of `values` (`negated`: is none of them).
    In {
        /// The non-null candidates.
        values: &'a [Json],
        /// `notIn`.
        negated: bool,
    },
    /// The column contains the non-null value: a substring of a text column,
    /// an element of an array column.
    Contains(&'a Json),
}

/// A decoded where condition.
#[derive(Clone, Debug, PartialEq)]
pub struct WhereCondition<'a> {
    /// The column as written (possibly table-qualified); callers resolve it.
    pub column: &'a str,
    /// What it tests.
    pub test: WhereTest<'a>,
}

/// How a condition value relates to its column, for typing it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WhereValueRole {
    /// A whole column value (comparisons and membership).
    Whole,
    /// The part `contains` looks for: text in a text column, an element in
    /// an array column.
    ContainsPart,
}

/// A where condition that doesn't decode.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WhereConditionError {
    /// Not a JSON object.
    #[error("a where condition must be an object")]
    NotAnObject,
    /// `column` missing or not a string.
    #[error("a where condition needs a \"column\" string")]
    MissingColumn,
    /// `op` missing or not a string.
    #[error("a where condition needs an \"op\" string")]
    MissingOp,
    /// An operator nobody defines.
    #[error("unknown op {0:?}")]
    UnknownOp(String),
    /// An operator that needs a value got none.
    #[error("\"{op}\" on \"{column}\" needs a value")]
    MissingValue {
        /// The column.
        column: String,
        /// The operator.
        op: &'static str,
    },
    /// `null` where only `eq`/`ne` accept it.
    #[error("\"{op}\" on \"{column}\" can't compare with null: use \"isNull\"")]
    NullValue {
        /// The column.
        column: String,
        /// The operator.
        op: &'static str,
    },
    /// `in`/`notIn` without an array.
    #[error("\"{op}\" on \"{column}\" needs an array value")]
    NotAnArray {
        /// The column.
        column: String,
        /// The operator.
        op: &'static str,
    },
    /// `null` among `in`/`notIn` candidates.
    #[error(
        "\"{op}\" on \"{column}\" does not accept null members; test null with \"isNull\" separately"
    )]
    NullMember {
        /// The column.
        column: String,
        /// The operator.
        op: &'static str,
    },
    /// `isNull`/`isNotNull` with a value that isn't allowed.
    #[error("\"{op}\" on \"{column}\" takes {expected}")]
    BadNullTest {
        /// The column.
        column: String,
        /// The operator.
        op: &'static str,
        /// What it takes.
        expected: &'static str,
    },
}

/// Decodes one where condition.
pub fn decode_where_condition(condition: &Json) -> Result<WhereCondition<'_>, WhereConditionError> {
    let object = condition
        .as_object()
        .ok_or(WhereConditionError::NotAnObject)?;
    let column = object
        .get("column")
        .and_then(Json::as_str)
        .ok_or(WhereConditionError::MissingColumn)?;
    let op = object
        .get("op")
        .and_then(Json::as_str)
        .ok_or(WhereConditionError::MissingOp)?;
    let value = object.get("value");
    let op: &'static str = match op {
        "eq" => "eq",
        "ne" => "ne",
        "lt" => "lt",
        "lte" => "lte",
        "gt" => "gt",
        "gte" => "gte",
        "in" => "in",
        "notIn" => "notIn",
        "contains" => "contains",
        "isNull" => "isNull",
        "isNotNull" => "isNotNull",
        other => return Err(WhereConditionError::UnknownOp(other.to_owned())),
    };
    let owned = || column.to_owned();
    let present = || {
        value.ok_or_else(|| WhereConditionError::MissingValue {
            column: owned(),
            op,
        })
    };
    let non_null = || {
        let value = present()?;
        if value.is_null() {
            return Err(WhereConditionError::NullValue {
                column: owned(),
                op,
            });
        }
        Ok(value)
    };
    let compare = |compare: WhereCompare| Ok(WhereTest::Compare(compare, non_null()?));
    let test = match op {
        "eq" | "ne" => {
            let value = present()?;
            match (op, value.is_null()) {
                ("eq", true) => WhereTest::Null(true),
                (_, true) => WhereTest::Null(false),
                ("eq", false) => WhereTest::Compare(WhereCompare::Eq, value),
                _ => WhereTest::Compare(WhereCompare::Ne, value),
            }
        }
        "lt" => compare(WhereCompare::Lt)?,
        "lte" => compare(WhereCompare::Lte)?,
        "gt" => compare(WhereCompare::Gt)?,
        "gte" => compare(WhereCompare::Gte)?,
        "in" | "notIn" => {
            let values = present()?
                .as_array()
                .ok_or_else(|| WhereConditionError::NotAnArray {
                    column: owned(),
                    op,
                })?;
            if values.iter().any(Json::is_null) {
                return Err(WhereConditionError::NullMember {
                    column: owned(),
                    op,
                });
            }
            WhereTest::In {
                values,
                negated: op == "notIn",
            }
        }
        "contains" => WhereTest::Contains(non_null()?),
        "isNull" => match value {
            None => WhereTest::Null(true),
            Some(Json::Bool(is_null)) => WhereTest::Null(*is_null),
            Some(_) => {
                return Err(WhereConditionError::BadNullTest {
                    column: owned(),
                    op,
                    expected: "true or false",
                });
            }
        },
        _ => match value {
            None | Some(Json::Bool(true)) => WhereTest::Null(false),
            Some(_) => {
                return Err(WhereConditionError::BadNullTest {
                    column: owned(),
                    op,
                    expected: "no value, or true",
                });
            }
        },
    };
    Ok(WhereCondition { column, test })
}

impl WhereCondition<'_> {
    /// The condition as an engine predicate on `column`, typing each value
    /// with `lower`.
    pub fn to_predicate<E>(
        &self,
        column: Operand,
        mut lower: impl FnMut(&Json, WhereValueRole) -> Result<Operand, E>,
    ) -> Result<Predicate, E> {
        Ok(match &self.test {
            WhereTest::Compare(compare, value) => {
                let value = lower(value, WhereValueRole::Whole)?;
                match compare {
                    WhereCompare::Eq => Predicate::Eq(column, value),
                    WhereCompare::Ne => Predicate::Ne(column, value),
                    WhereCompare::Lt => Predicate::Lt(column, value),
                    WhereCompare::Lte => Predicate::Lte(column, value),
                    WhereCompare::Gt => Predicate::Gt(column, value),
                    WhereCompare::Gte => Predicate::Gte(column, value),
                }
            }
            WhereTest::Null(true) => Predicate::IsNull(column),
            WhereTest::Null(false) => Predicate::Not(Box::new(Predicate::IsNull(column))),
            WhereTest::In { values, negated } => {
                let values = values
                    .iter()
                    .map(|value| lower(value, WhereValueRole::Whole))
                    .collect::<Result<Vec<_>, E>>()?;
                let membership = Predicate::In(column, values);
                if *negated {
                    Predicate::Not(Box::new(membership))
                } else {
                    membership
                }
            }
            WhereTest::Contains(value) => {
                Predicate::Contains(column, lower(value, WhereValueRole::ContainsPart)?)
            }
        })
    }
}
