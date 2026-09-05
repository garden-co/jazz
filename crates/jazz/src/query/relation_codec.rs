// Typed Postcard representation for relation queries.
//
// `serde_json::Value` is deliberately kept out of the non-human serializer:
// Postcard has no stable representation for it. `WireJson` is the complete,
// typed recursive literal tree carried by the relation-query Postcard grammar.

use serde::de::{self, DeserializeSeed, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};

const MAX_RELATION_BYTES: usize = 1 << 20;
const MAX_RELATION_DEPTH: usize = 128;
const MAX_RELATION_ITEMS: usize = 4096;
const MAX_RELATION_STRING_BYTES: usize = 1 << 16;

/// Failure while converting a relation query to or from typed Postcard.
#[derive(Debug, thiserror::Error)]
pub enum RelationWireError {
    /// The carrier exceeded its byte bound.
    #[error("relation query is too large")]
    TooLarge,
    /// Postcard rejected the typed relation tree.
    #[error("malformed Postcard relation query: {0}")]
    Postcard(#[from] postcard::Error),
    /// A JSON float cannot be represented.
    #[error("relation query contains a non-finite JSON number")]
    NonFiniteNumber,
    /// A bounded relation-wire invariant failed.
    #[error("relation query {0}")]
    Invalid(&'static str),
}

type WireResult<T> = Result<T, RelationWireError>;

#[derive(Default)]
struct DeserializeBudget {
    depth: usize,
    nodes: usize,
    strings: usize,
}

thread_local! {
    static DESERIALIZE_BUDGET: std::cell::RefCell<Option<DeserializeBudget>> = const { std::cell::RefCell::new(None) };
}

struct DeserializeBudgetScope;
impl DeserializeBudgetScope {
    fn enter<E: de::Error>() -> Result<Self, E> {
        DESERIALIZE_BUDGET.with(|state| {
            let mut state = state.borrow_mut();
            if state.is_some() {
                return Err(E::custom("nested relation-query decoder"));
            }
            *state = Some(DeserializeBudget::default());
            Ok(Self)
        })
    }
}
impl Drop for DeserializeBudgetScope {
    fn drop(&mut self) {
        DESERIALIZE_BUDGET.with(|state| *state.borrow_mut() = None);
    }
}

fn budget_enter<E: de::Error>() -> Result<(), E> {
    DESERIALIZE_BUDGET.with(|state| {
        let mut state = state.borrow_mut();
        let state = state
            .as_mut()
            .ok_or_else(|| E::custom("relation-query budget missing"))?;
        state.depth += 1;
        state.nodes += 1;
        if state.depth > MAX_RELATION_DEPTH + 1 || state.nodes > MAX_RELATION_ITEMS {
            Err(E::custom("relation-query tree limit"))
        } else {
            Ok(())
        }
    })
}
fn budget_node<E: de::Error>() -> Result<(), E> {
    DESERIALIZE_BUDGET.with(|state| {
        let mut state = state.borrow_mut();
        let state = state
            .as_mut()
            .ok_or_else(|| E::custom("relation-query budget missing"))?;
        state.nodes += 1;
        if state.nodes > MAX_RELATION_ITEMS {
            Err(E::custom("relation-query node limit"))
        } else {
            Ok(())
        }
    })
}
fn budget_leave() {
    DESERIALIZE_BUDGET.with(|state| {
        if let Some(state) = state.borrow_mut().as_mut() {
            state.depth = state.depth.saturating_sub(1);
        }
    });
}
fn budget_string<E: de::Error>(value: &str) -> Result<(), E> {
    DESERIALIZE_BUDGET.with(|state| {
        let mut state = state.borrow_mut();
        let state = state
            .as_mut()
            .ok_or_else(|| E::custom("relation-query budget missing"))?;
        state.strings = state
            .strings
            .checked_add(value.len())
            .ok_or_else(|| E::custom("relation-query byte limit"))?;
        if value.len() > MAX_RELATION_STRING_BYTES || state.strings > MAX_RELATION_BYTES {
            Err(E::custom("relation-query string limit"))
        } else {
            Ok(())
        }
    })
}

fn deserialize_bounded_string<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<String, D::Error> {
    struct BoundedString;
    impl<'de> Visitor<'de> for BoundedString {
        type Value = String;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a bounded UTF-8 relation string")
        }
        fn visit_borrowed_str<E: de::Error>(self, value: &'de str) -> Result<String, E> {
            budget_string(value)?;
            Ok(value.to_owned())
        }
        fn visit_str<E: de::Error>(self, value: &str) -> Result<String, E> {
            budget_string(value)?;
            Ok(value.to_owned())
        }
        fn visit_string<E: de::Error>(self, value: String) -> Result<String, E> {
            budget_string(&value)?;
            Ok(value)
        }
    }
    deserializer.deserialize_string(BoundedString)
}

fn deserialize_bounded_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: de::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct BoundedVec<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> Visitor<'de> for BoundedVec<T> {
        type Value = Vec<T>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a bounded relation collection")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Vec<T>, A::Error> {
            let mut values = Vec::new();
            while let Some(value) = sequence.next_element()? {
                if values.len() == MAX_RELATION_ITEMS {
                    return Err(de::Error::custom("relation-query collection limit"));
                }
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(BoundedVec(std::marker::PhantomData))
}

fn deserialize_bounded_option_string<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    struct OptionString;
    impl<'de> Visitor<'de> for OptionString {
        type Value = Option<String>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("an optional bounded relation string")
        }
        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_some<D: de::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            deserialize_bounded_string(d).map(Some)
        }
    }
    deserializer.deserialize_option(OptionString)
}

fn deserialize_bounded_string_vec<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<String>, D::Error> {
    struct StringSeed;
    impl<'de> DeserializeSeed<'de> for StringSeed {
        type Value = String;
        fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            deserialize_bounded_string(d)
        }
    }
    struct Values;
    impl<'de> Visitor<'de> for Values {
        type Value = Vec<String>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("bounded relation strings")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
            let mut values = Vec::new();
            while let Some(value) = sequence.next_element_seed(StringSeed)? {
                if values.len() == MAX_RELATION_ITEMS {
                    return Err(de::Error::custom("relation-query collection limit"));
                }
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Values)
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct WireRelationQuery {
    rel: WireRelationExpr,
}

impl<'de> Deserialize<'de> for WireRelationQuery {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct BorrowedWireRelationQuery {
            #[serde(deserialize_with = "deserialize_expr")]
            rel: WireRelationExpr,
        }
        let _scope = DeserializeBudgetScope::enter::<D::Error>()?;
        let value = BorrowedWireRelationQuery::deserialize(deserializer)?;
        Ok(Self { rel: value.rel })
    }
}

fn deserialize_expr<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<WireRelationExpr, D::Error> {
    budget_enter::<D::Error>()?;
    let result = WireRelationExpr::deserialize(deserializer);
    budget_leave();
    result
}
fn deserialize_expr_box<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<Box<WireRelationExpr>, D::Error> {
    deserialize_expr(deserializer).map(Box::new)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum WireRelationExpr {
    TableScan {
        #[serde(deserialize_with = "deserialize_bounded_string")]
        table: String,
        #[serde(deserialize_with = "deserialize_bounded_option_string")]
        alias: Option<String>,
    },
    Filter {
        #[serde(deserialize_with = "deserialize_expr_box")]
        input: Box<WireRelationExpr>,
        #[serde(deserialize_with = "deserialize_predicate")]
        predicate: WireRelationPredicate,
    },
    Union {
        #[serde(deserialize_with = "deserialize_bounded_vec")]
        inputs: Vec<WireRelationUnionArm>,
    },
    Join {
        #[serde(deserialize_with = "deserialize_expr_box")]
        left: Box<WireRelationExpr>,
        #[serde(deserialize_with = "deserialize_expr_box")]
        right: Box<WireRelationExpr>,
        #[serde(deserialize_with = "deserialize_join_condition_vec")]
        on: Vec<WireRelationJoinCondition>,
        join_kind: WireRelationJoinKind,
    },
    Project {
        #[serde(deserialize_with = "deserialize_expr_box")]
        input: Box<WireRelationExpr>,
        #[serde(deserialize_with = "deserialize_project_column_vec")]
        columns: Vec<WireRelationProjectColumn>,
    },
    Gather {
        #[serde(deserialize_with = "deserialize_expr_box")]
        seed: Box<WireRelationExpr>,
        #[serde(deserialize_with = "deserialize_expr_box")]
        step: Box<WireRelationExpr>,
        #[serde(deserialize_with = "deserialize_key")]
        frontier_key: WireRelationKeyRef,
        bound: WireRecursionBound,
        #[serde(deserialize_with = "deserialize_key_vec")]
        dedupe_key: Vec<WireRelationKeyRef>,
    },
    Distinct {
        #[serde(deserialize_with = "deserialize_expr_box")]
        input: Box<WireRelationExpr>,
        #[serde(deserialize_with = "deserialize_key_vec")]
        key: Vec<WireRelationKeyRef>,
    },
    OrderBy {
        #[serde(deserialize_with = "deserialize_expr_box")]
        input: Box<WireRelationExpr>,
        #[serde(deserialize_with = "deserialize_order_by_vec")]
        terms: Vec<WireRelationOrderBy>,
    },
    Offset {
        #[serde(deserialize_with = "deserialize_expr_box")]
        input: Box<WireRelationExpr>,
        offset: u32,
    },
    Limit {
        #[serde(deserialize_with = "deserialize_expr_box")]
        input: Box<WireRelationExpr>,
        limit: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct WireRelationUnionArm {
    #[serde(deserialize_with = "deserialize_bounded_string")]
    label: String,
    #[serde(deserialize_with = "deserialize_expr")]
    input: WireRelationExpr,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum WireRelationPredicate {
    Cmp {
        left: WireRelationColumnRef,
        op: WireRelationCmpOp,
        #[serde(deserialize_with = "deserialize_value")]
        right: WireRelationValueRef,
    },
    IsNull {
        column: WireRelationColumnRef,
    },
    IsNotNull {
        column: WireRelationColumnRef,
    },
    In {
        left: WireRelationColumnRef,
        #[serde(deserialize_with = "deserialize_value_vec")]
        values: Vec<WireRelationValueRef>,
    },
    Contains {
        left: WireRelationColumnRef,
        #[serde(deserialize_with = "deserialize_value")]
        right: WireRelationValueRef,
    },
    EnumMatch {
        column: WireRelationColumnRef,
        #[serde(deserialize_with = "deserialize_bounded_string")]
        case: String,
        #[serde(deserialize_with = "deserialize_predicate_box")]
        payload: Box<WireRelationPredicate>,
    },
    And(#[serde(deserialize_with = "deserialize_predicate_vec")] Vec<WireRelationPredicate>),
    Or(#[serde(deserialize_with = "deserialize_predicate_vec")] Vec<WireRelationPredicate>),
    Not(#[serde(deserialize_with = "deserialize_predicate_box")] Box<WireRelationPredicate>),
    True,
    False,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum WireRelationCmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WireRelationColumnRef {
    #[serde(deserialize_with = "deserialize_bounded_option_string")]
    scope: Option<String>,
    #[serde(deserialize_with = "deserialize_bounded_string")]
    column: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum WireRelationValueRef {
    Literal(#[serde(deserialize_with = "deserialize_json")] WireJson),
    Param(#[serde(deserialize_with = "deserialize_bounded_string")] String),
    SessionRef(#[serde(deserialize_with = "deserialize_bounded_string_vec")] Vec<String>),
    OuterColumn(WireRelationColumnRef),
    FrontierColumn(WireRelationColumnRef),
    RowId(WireRelationRowIdRef),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum WireRelationRowIdRef {
    Current,
    Outer,
    Frontier,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum WireRelationJoinKind {
    Inner,
    Left,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WireRelationJoinCondition {
    left: WireRelationColumnRef,
    right: WireRelationColumnRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum WireRelationKeyRef {
    Column(WireRelationColumnRef),
    RowId(WireRelationRowIdRef),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum WireRelationProjectExpr {
    Column(WireRelationColumnRef),
    RowId(WireRelationRowIdRef),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WireRelationProjectColumn {
    #[serde(deserialize_with = "deserialize_bounded_string")]
    alias: String,
    expr: WireRelationProjectExpr,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WireRelationOrderBy {
    column: WireRelationColumnRef,
    direction: WireOrderDirection,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum WireOrderDirection {
    Asc,
    Desc,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum WireRecursionBound {
    Fixpoint,
    MaxDepth(u32),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum WireJson {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(u64),
    String(#[serde(deserialize_with = "deserialize_bounded_string")] String),
    Array(#[serde(deserialize_with = "deserialize_json_vec")] Vec<WireJson>),
    Object(#[serde(deserialize_with = "deserialize_json_object")] Vec<(String, WireJson)>),
}

fn deserialize_predicate<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<WireRelationPredicate, D::Error> {
    budget_enter::<D::Error>()?;
    let result = WireRelationPredicate::deserialize(deserializer);
    budget_leave();
    result
}
fn deserialize_predicate_box<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<Box<WireRelationPredicate>, D::Error> {
    deserialize_predicate(deserializer).map(Box::new)
}
fn deserialize_predicate_vec<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<WireRelationPredicate>, D::Error> {
    struct Seed;
    impl<'de> DeserializeSeed<'de> for Seed {
        type Value = WireRelationPredicate;
        fn deserialize<D: de::Deserializer<'de>>(
            self,
            deserializer: D,
        ) -> Result<Self::Value, D::Error> {
            deserialize_predicate(deserializer)
        }
    }
    struct Values;
    impl<'de> Visitor<'de> for Values {
        type Value = Vec<WireRelationPredicate>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("bounded relation predicates")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
            let mut values = Vec::new();
            while let Some(value) = sequence.next_element_seed(Seed)? {
                if values.len() == MAX_RELATION_ITEMS {
                    return Err(de::Error::custom("relation-query collection limit"));
                }
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Values)
}
fn deserialize_json<'de, D: de::Deserializer<'de>>(deserializer: D) -> Result<WireJson, D::Error> {
    budget_enter::<D::Error>()?;
    let result = WireJson::deserialize(deserializer);
    budget_leave();
    result
}
fn deserialize_json_vec<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<WireJson>, D::Error> {
    struct Seed;
    impl<'de> DeserializeSeed<'de> for Seed {
        type Value = WireJson;
        fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            deserialize_json(d)
        }
    }
    struct Values;
    impl<'de> Visitor<'de> for Values {
        type Value = Vec<WireJson>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("bounded JSON values")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
            let mut values = Vec::new();
            while let Some(value) = sequence.next_element_seed(Seed)? {
                if values.len() == MAX_RELATION_ITEMS {
                    return Err(de::Error::custom("relation-query collection limit"));
                }
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Values)
}

fn deserialize_json_object<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<(String, WireJson)>, D::Error> {
    struct StringSeed;
    impl<'de> DeserializeSeed<'de> for StringSeed {
        type Value = String;
        fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            deserialize_bounded_string(d)
        }
    }
    struct JsonSeed;
    impl<'de> DeserializeSeed<'de> for JsonSeed {
        type Value = WireJson;
        fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            deserialize_json(d)
        }
    }
    struct Entry;
    impl<'de> DeserializeSeed<'de> for Entry {
        type Value = (String, WireJson);
        fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            struct Tuple;
            impl<'de> Visitor<'de> for Tuple {
                type Value = (String, WireJson);
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("a JSON object entry")
                }
                fn visit_seq<A: SeqAccess<'de>>(self, mut s: A) -> Result<Self::Value, A::Error> {
                    let key = s
                        .next_element_seed(StringSeed)?
                        .ok_or_else(|| de::Error::custom("object key"))?;
                    let value = s
                        .next_element_seed(JsonSeed)?
                        .ok_or_else(|| de::Error::custom("object value"))?;
                    Ok((key, value))
                }
            }
            d.deserialize_tuple(2, Tuple)
        }
    }
    struct Entries;
    impl<'de> Visitor<'de> for Entries {
        type Value = Vec<(String, WireJson)>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("bounded JSON object")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut s: A) -> Result<Self::Value, A::Error> {
            let mut values = Vec::new();
            while let Some(value) = s.next_element_seed(Entry)? {
                if values.len() == MAX_RELATION_ITEMS {
                    return Err(de::Error::custom("relation-query collection limit"));
                }
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Entries)
}

macro_rules! bounded_node_vec {
    ($function:ident, $seed:ident, $type:ty, $single:ident) => {
        fn $function<'de, D: de::Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Vec<$type>, D::Error> {
            struct $seed;
            impl<'de> DeserializeSeed<'de> for $seed {
                type Value = $type;
                fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<$type, D::Error> {
                    $single(d)
                }
            }
            struct Values;
            impl<'de> Visitor<'de> for Values {
                type Value = Vec<$type>;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("bounded relation nodes")
                }
                fn visit_seq<A: SeqAccess<'de>>(self, mut s: A) -> Result<Self::Value, A::Error> {
                    let mut values = Vec::new();
                    while let Some(value) = s.next_element_seed($seed)? {
                        if values.len() == MAX_RELATION_ITEMS {
                            return Err(de::Error::custom("relation-query collection limit"));
                        }
                        values.push(value);
                    }
                    Ok(values)
                }
            }
            deserializer.deserialize_seq(Values)
        }
    };
}
fn deserialize_value<'de, D: de::Deserializer<'de>>(
    d: D,
) -> Result<WireRelationValueRef, D::Error> {
    budget_node::<D::Error>()?;
    WireRelationValueRef::deserialize(d)
}
fn deserialize_key<'de, D: de::Deserializer<'de>>(d: D) -> Result<WireRelationKeyRef, D::Error> {
    budget_node::<D::Error>()?;
    WireRelationKeyRef::deserialize(d)
}
fn deserialize_join_condition<'de, D: de::Deserializer<'de>>(
    d: D,
) -> Result<WireRelationJoinCondition, D::Error> {
    budget_node::<D::Error>()?;
    WireRelationJoinCondition::deserialize(d)
}
fn deserialize_project_column<'de, D: de::Deserializer<'de>>(
    d: D,
) -> Result<WireRelationProjectColumn, D::Error> {
    budget_node::<D::Error>()?;
    WireRelationProjectColumn::deserialize(d)
}
fn deserialize_order_by<'de, D: de::Deserializer<'de>>(
    d: D,
) -> Result<WireRelationOrderBy, D::Error> {
    budget_node::<D::Error>()?;
    WireRelationOrderBy::deserialize(d)
}
bounded_node_vec!(
    deserialize_value_vec,
    ValueSeed,
    WireRelationValueRef,
    deserialize_value
);
bounded_node_vec!(
    deserialize_key_vec,
    KeySeed,
    WireRelationKeyRef,
    deserialize_key
);
bounded_node_vec!(
    deserialize_join_condition_vec,
    JoinSeed,
    WireRelationJoinCondition,
    deserialize_join_condition
);
bounded_node_vec!(
    deserialize_project_column_vec,
    ProjectSeed,
    WireRelationProjectColumn,
    deserialize_project_column
);
bounded_node_vec!(
    deserialize_order_by_vec,
    OrderSeed,
    WireRelationOrderBy,
    deserialize_order_by
);

impl TryFrom<&RelationQuery> for WireRelationQuery {
    type Error = RelationWireError;
    fn try_from(value: &RelationQuery) -> WireResult<Self> {
        Ok(Self {
            rel: wire_expr(&value.rel)?,
        })
    }
}
impl TryFrom<WireRelationQuery> for RelationQuery {
    type Error = RelationWireError;
    fn try_from(value: WireRelationQuery) -> WireResult<Self> {
        Ok(Self {
            rel: relation_expr(value.rel)?,
        })
    }
}

fn u32_dimension(value: usize) -> WireResult<u32> {
    u32::try_from(value).map_err(|_| RelationWireError::Invalid("dimension exceeds u32"))
}
fn wire_column(value: &RelationColumnRef) -> WireRelationColumnRef {
    WireRelationColumnRef {
        scope: value.scope.clone(),
        column: value.column.clone(),
    }
}
fn relation_column(value: WireRelationColumnRef) -> RelationColumnRef {
    RelationColumnRef {
        scope: value.scope,
        column: value.column,
    }
}
fn wire_row_id(value: RelationRowIdRef) -> WireRelationRowIdRef {
    match value {
        RelationRowIdRef::Current => WireRelationRowIdRef::Current,
        RelationRowIdRef::Outer => WireRelationRowIdRef::Outer,
        RelationRowIdRef::Frontier => WireRelationRowIdRef::Frontier,
    }
}
fn relation_row_id(value: WireRelationRowIdRef) -> RelationRowIdRef {
    match value {
        WireRelationRowIdRef::Current => RelationRowIdRef::Current,
        WireRelationRowIdRef::Outer => RelationRowIdRef::Outer,
        WireRelationRowIdRef::Frontier => RelationRowIdRef::Frontier,
    }
}
fn wire_key(value: &RelationKeyRef) -> WireRelationKeyRef {
    match value {
        RelationKeyRef::Column(column) => WireRelationKeyRef::Column(wire_column(column)),
        RelationKeyRef::RowId(row_id) => WireRelationKeyRef::RowId(wire_row_id(*row_id)),
    }
}
fn relation_key(value: WireRelationKeyRef) -> RelationKeyRef {
    match value {
        WireRelationKeyRef::Column(column) => RelationKeyRef::Column(relation_column(column)),
        WireRelationKeyRef::RowId(row_id) => RelationKeyRef::RowId(relation_row_id(row_id)),
    }
}
fn wire_json(value: &serde_json::Value) -> WireResult<WireJson> {
    Ok(match value {
        serde_json::Value::Null => WireJson::Null,
        serde_json::Value::Bool(value) => WireJson::Bool(*value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                WireJson::I64(value)
            } else if let Some(value) = value.as_u64() {
                WireJson::U64(value)
            } else {
                WireJson::F64(
                    value
                        .as_f64()
                        .ok_or(RelationWireError::NonFiniteNumber)?
                        .to_bits(),
                )
            }
        }
        serde_json::Value::String(value) => WireJson::String(value.clone()),
        serde_json::Value::Array(values) => {
            WireJson::Array(values.iter().map(wire_json).collect::<WireResult<_>>()?)
        }
        serde_json::Value::Object(values) => WireJson::Object(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), wire_json(value)?)))
                .collect::<WireResult<_>>()?,
        ),
    })
}
fn relation_json(value: WireJson) -> WireResult<serde_json::Value> {
    Ok(match value {
        WireJson::Null => serde_json::Value::Null,
        WireJson::Bool(value) => serde_json::Value::Bool(value),
        WireJson::I64(value) => serde_json::Value::Number(value.into()),
        WireJson::U64(value) => serde_json::Value::Number(value.into()),
        WireJson::F64(value) => serde_json::Value::Number(
            serde_json::Number::from_f64(f64::from_bits(value))
                .ok_or(RelationWireError::NonFiniteNumber)?,
        ),
        WireJson::String(value) => serde_json::Value::String(value),
        WireJson::Array(values) => serde_json::Value::Array(
            values
                .into_iter()
                .map(relation_json)
                .collect::<WireResult<_>>()?,
        ),
        WireJson::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, relation_json(value)?)))
                .collect::<WireResult<_>>()?,
        ),
    })
}

fn wire_value(value: &RelationValueRef) -> WireResult<WireRelationValueRef> {
    Ok(match value {
        RelationValueRef::Literal(value) => WireRelationValueRef::Literal(wire_json(value)?),
        RelationValueRef::Param(value) => WireRelationValueRef::Param(value.clone()),
        RelationValueRef::SessionRef(value) => WireRelationValueRef::SessionRef(value.clone()),
        RelationValueRef::OuterColumn(value) => {
            WireRelationValueRef::OuterColumn(wire_column(value))
        }
        RelationValueRef::FrontierColumn(value) => {
            WireRelationValueRef::FrontierColumn(wire_column(value))
        }
        RelationValueRef::RowId(value) => WireRelationValueRef::RowId(wire_row_id(*value)),
    })
}
fn relation_value(value: WireRelationValueRef) -> WireResult<RelationValueRef> {
    Ok(match value {
        WireRelationValueRef::Literal(value) => RelationValueRef::Literal(relation_json(value)?),
        WireRelationValueRef::Param(value) => RelationValueRef::Param(value),
        WireRelationValueRef::SessionRef(value) => RelationValueRef::SessionRef(value),
        WireRelationValueRef::OuterColumn(value) => {
            RelationValueRef::OuterColumn(relation_column(value))
        }
        WireRelationValueRef::FrontierColumn(value) => {
            RelationValueRef::FrontierColumn(relation_column(value))
        }
        WireRelationValueRef::RowId(value) => RelationValueRef::RowId(relation_row_id(value)),
    })
}
fn wire_predicate(value: &RelationPredicate) -> WireResult<WireRelationPredicate> {
    Ok(match value {
        RelationPredicate::Cmp { left, op, right } => WireRelationPredicate::Cmp {
            left: wire_column(left),
            op: match op {
                RelationCmpOp::Eq => WireRelationCmpOp::Eq,
                RelationCmpOp::Ne => WireRelationCmpOp::Ne,
                RelationCmpOp::Lt => WireRelationCmpOp::Lt,
                RelationCmpOp::Le => WireRelationCmpOp::Le,
                RelationCmpOp::Gt => WireRelationCmpOp::Gt,
                RelationCmpOp::Ge => WireRelationCmpOp::Ge,
            },
            right: wire_value(right)?,
        },
        RelationPredicate::IsNull { column } => WireRelationPredicate::IsNull {
            column: wire_column(column),
        },
        RelationPredicate::IsNotNull { column } => WireRelationPredicate::IsNotNull {
            column: wire_column(column),
        },
        RelationPredicate::In { left, values } => WireRelationPredicate::In {
            left: wire_column(left),
            values: values.iter().map(wire_value).collect::<WireResult<_>>()?,
        },
        RelationPredicate::Contains { left, right } => WireRelationPredicate::Contains {
            left: wire_column(left),
            right: wire_value(right)?,
        },
        RelationPredicate::EnumMatch {
            column,
            case,
            payload,
        } => WireRelationPredicate::EnumMatch {
            column: wire_column(column),
            case: case.clone(),
            payload: Box::new(wire_predicate(payload)?),
        },
        RelationPredicate::And(values) => WireRelationPredicate::And(
            values
                .iter()
                .map(wire_predicate)
                .collect::<WireResult<_>>()?,
        ),
        RelationPredicate::Or(values) => WireRelationPredicate::Or(
            values
                .iter()
                .map(wire_predicate)
                .collect::<WireResult<_>>()?,
        ),
        RelationPredicate::Not(value) => {
            WireRelationPredicate::Not(Box::new(wire_predicate(value)?))
        }
        RelationPredicate::True => WireRelationPredicate::True,
        RelationPredicate::False => WireRelationPredicate::False,
    })
}
fn relation_predicate(value: WireRelationPredicate) -> WireResult<RelationPredicate> {
    Ok(match value {
        WireRelationPredicate::Cmp { left, op, right } => RelationPredicate::Cmp {
            left: relation_column(left),
            op: match op {
                WireRelationCmpOp::Eq => RelationCmpOp::Eq,
                WireRelationCmpOp::Ne => RelationCmpOp::Ne,
                WireRelationCmpOp::Lt => RelationCmpOp::Lt,
                WireRelationCmpOp::Le => RelationCmpOp::Le,
                WireRelationCmpOp::Gt => RelationCmpOp::Gt,
                WireRelationCmpOp::Ge => RelationCmpOp::Ge,
            },
            right: relation_value(right)?,
        },
        WireRelationPredicate::IsNull { column } => RelationPredicate::IsNull {
            column: relation_column(column),
        },
        WireRelationPredicate::IsNotNull { column } => RelationPredicate::IsNotNull {
            column: relation_column(column),
        },
        WireRelationPredicate::In { left, values } => RelationPredicate::In {
            left: relation_column(left),
            values: values
                .into_iter()
                .map(relation_value)
                .collect::<WireResult<_>>()?,
        },
        WireRelationPredicate::Contains { left, right } => RelationPredicate::Contains {
            left: relation_column(left),
            right: relation_value(right)?,
        },
        WireRelationPredicate::EnumMatch {
            column,
            case,
            payload,
        } => RelationPredicate::EnumMatch {
            column: relation_column(column),
            case,
            payload: Box::new(relation_predicate(*payload)?),
        },
        WireRelationPredicate::And(values) => RelationPredicate::And(
            values
                .into_iter()
                .map(relation_predicate)
                .collect::<WireResult<_>>()?,
        ),
        WireRelationPredicate::Or(values) => RelationPredicate::Or(
            values
                .into_iter()
                .map(relation_predicate)
                .collect::<WireResult<_>>()?,
        ),
        WireRelationPredicate::Not(value) => {
            RelationPredicate::Not(Box::new(relation_predicate(*value)?))
        }
        WireRelationPredicate::True => RelationPredicate::True,
        WireRelationPredicate::False => RelationPredicate::False,
    })
}

fn wire_expr(value: &RelationExpr) -> WireResult<WireRelationExpr> {
    Ok(match value {
        RelationExpr::TableScan { table, alias } => WireRelationExpr::TableScan {
            table: table.clone(),
            alias: alias.clone(),
        },
        RelationExpr::Filter { input, predicate } => WireRelationExpr::Filter {
            input: Box::new(wire_expr(input)?),
            predicate: wire_predicate(predicate)?,
        },
        RelationExpr::Union { inputs } => WireRelationExpr::Union {
            inputs: inputs
                .iter()
                .map(|arm| {
                    Ok(WireRelationUnionArm {
                        label: arm.label.clone(),
                        input: wire_expr(&arm.input)?,
                    })
                })
                .collect::<WireResult<_>>()?,
        },
        RelationExpr::Join {
            left,
            right,
            on,
            join_kind,
        } => WireRelationExpr::Join {
            left: Box::new(wire_expr(left)?),
            right: Box::new(wire_expr(right)?),
            on: on
                .iter()
                .map(|condition| WireRelationJoinCondition {
                    left: wire_column(&condition.left),
                    right: wire_column(&condition.right),
                })
                .collect(),
            join_kind: match join_kind {
                RelationJoinKind::Inner => WireRelationJoinKind::Inner,
                RelationJoinKind::Left => WireRelationJoinKind::Left,
            },
        },
        RelationExpr::Project { input, columns } => WireRelationExpr::Project {
            input: Box::new(wire_expr(input)?),
            columns: columns
                .iter()
                .map(|column| WireRelationProjectColumn {
                    alias: column.alias.clone(),
                    expr: match &column.expr {
                        RelationProjectExpr::Column(value) => {
                            WireRelationProjectExpr::Column(wire_column(value))
                        }
                        RelationProjectExpr::RowId(value) => {
                            WireRelationProjectExpr::RowId(wire_row_id(*value))
                        }
                    },
                })
                .collect(),
        },
        RelationExpr::Gather {
            seed,
            step,
            frontier_key,
            bound,
            dedupe_key,
        } => WireRelationExpr::Gather {
            seed: Box::new(wire_expr(seed)?),
            step: Box::new(wire_expr(step)?),
            frontier_key: wire_key(frontier_key),
            bound: match bound {
                RecursionBound::Fixpoint => WireRecursionBound::Fixpoint,
                RecursionBound::MaxDepth(value) => {
                    WireRecursionBound::MaxDepth(u32_dimension(*value)?)
                }
            },
            dedupe_key: dedupe_key.iter().map(wire_key).collect(),
        },
        RelationExpr::Distinct { input, key } => WireRelationExpr::Distinct {
            input: Box::new(wire_expr(input)?),
            key: key.iter().map(wire_key).collect(),
        },
        RelationExpr::OrderBy { input, terms } => WireRelationExpr::OrderBy {
            input: Box::new(wire_expr(input)?),
            terms: terms
                .iter()
                .map(|term| WireRelationOrderBy {
                    column: wire_column(&term.column),
                    direction: match term.direction {
                        OrderDirection::Asc => WireOrderDirection::Asc,
                        OrderDirection::Desc => WireOrderDirection::Desc,
                    },
                })
                .collect(),
        },
        RelationExpr::Offset { input, offset } => WireRelationExpr::Offset {
            input: Box::new(wire_expr(input)?),
            offset: u32_dimension(*offset)?,
        },
        RelationExpr::Limit { input, limit } => WireRelationExpr::Limit {
            input: Box::new(wire_expr(input)?),
            limit: u32_dimension(*limit)?,
        },
    })
}
fn relation_expr(value: WireRelationExpr) -> WireResult<RelationExpr> {
    Ok(match value {
        WireRelationExpr::TableScan { table, alias } => RelationExpr::TableScan { table, alias },
        WireRelationExpr::Filter { input, predicate } => RelationExpr::Filter {
            input: Box::new(relation_expr(*input)?),
            predicate: relation_predicate(predicate)?,
        },
        WireRelationExpr::Union { inputs } => RelationExpr::Union {
            inputs: inputs
                .into_iter()
                .map(|arm| {
                    Ok(RelationUnionArm {
                        label: arm.label,
                        input: relation_expr(arm.input)?,
                    })
                })
                .collect::<WireResult<_>>()?,
        },
        WireRelationExpr::Join {
            left,
            right,
            on,
            join_kind,
        } => RelationExpr::Join {
            left: Box::new(relation_expr(*left)?),
            right: Box::new(relation_expr(*right)?),
            on: on
                .into_iter()
                .map(|condition| RelationJoinCondition {
                    left: relation_column(condition.left),
                    right: relation_column(condition.right),
                })
                .collect(),
            join_kind: match join_kind {
                WireRelationJoinKind::Inner => RelationJoinKind::Inner,
                WireRelationJoinKind::Left => RelationJoinKind::Left,
            },
        },
        WireRelationExpr::Project { input, columns } => RelationExpr::Project {
            input: Box::new(relation_expr(*input)?),
            columns: columns
                .into_iter()
                .map(|column| RelationProjectColumn {
                    alias: column.alias,
                    expr: match column.expr {
                        WireRelationProjectExpr::Column(value) => {
                            RelationProjectExpr::Column(relation_column(value))
                        }
                        WireRelationProjectExpr::RowId(value) => {
                            RelationProjectExpr::RowId(relation_row_id(value))
                        }
                    },
                })
                .collect(),
        },
        WireRelationExpr::Gather {
            seed,
            step,
            frontier_key,
            bound,
            dedupe_key,
        } => RelationExpr::Gather {
            seed: Box::new(relation_expr(*seed)?),
            step: Box::new(relation_expr(*step)?),
            frontier_key: relation_key(frontier_key),
            bound: match bound {
                WireRecursionBound::Fixpoint => RecursionBound::Fixpoint,
                WireRecursionBound::MaxDepth(value) => RecursionBound::MaxDepth(value as usize),
            },
            dedupe_key: dedupe_key.into_iter().map(relation_key).collect(),
        },
        WireRelationExpr::Distinct { input, key } => RelationExpr::Distinct {
            input: Box::new(relation_expr(*input)?),
            key: key.into_iter().map(relation_key).collect(),
        },
        WireRelationExpr::OrderBy { input, terms } => RelationExpr::OrderBy {
            input: Box::new(relation_expr(*input)?),
            terms: terms
                .into_iter()
                .map(|term| RelationOrderBy {
                    column: relation_column(term.column),
                    direction: match term.direction {
                        WireOrderDirection::Asc => OrderDirection::Asc,
                        WireOrderDirection::Desc => OrderDirection::Desc,
                    },
                })
                .collect(),
        },
        WireRelationExpr::Offset { input, offset } => RelationExpr::Offset {
            input: Box::new(relation_expr(*input)?),
            offset: offset as usize,
        },
        WireRelationExpr::Limit { input, limit } => RelationExpr::Limit {
            input: Box::new(relation_expr(*input)?),
            limit: limit as usize,
        },
    })
}

pub(crate) fn relation_query_to_wire(value: &RelationQuery) -> WireResult<WireRelationQuery> {
    let wire = WireRelationQuery::try_from(value)?;
    ensure_wire_size(&wire)?;
    validate_wire(&wire)?;
    Ok(wire)
}
pub(crate) fn relation_query_from_wire(value: WireRelationQuery) -> WireResult<RelationQuery> {
    ensure_wire_size(&value)?;
    validate_wire(&value)?;
    RelationQuery::try_from(value)
}
fn ensure_wire_size(value: &WireRelationQuery) -> WireResult<()> {
    if postcard::experimental::serialized_size(value)? > MAX_RELATION_BYTES {
        Err(RelationWireError::TooLarge)
    } else {
        Ok(())
    }
}
/// Encode the typed relation-query Postcard payload used by direct native reads.
pub fn encode_relation_query_postcard(value: &RelationQuery) -> WireResult<Vec<u8>> {
    let wire = relation_query_to_wire(value)?;
    let bytes = postcard::to_allocvec(&wire)?;
    if bytes.len() > MAX_RELATION_BYTES {
        return Err(RelationWireError::TooLarge);
    }
    Ok(bytes)
}
/// Decode a typed relation-query Postcard payload used by direct native reads.
pub fn decode_relation_query_postcard(bytes: &[u8]) -> WireResult<RelationQuery> {
    if bytes.len() > MAX_RELATION_BYTES {
        return Err(RelationWireError::TooLarge);
    }
    let wire = crate::wire::decode_postcard_exact::<WireRelationQuery>(bytes)?;
    relation_query_from_wire(wire)
}
fn validate_wire(value: &WireRelationQuery) -> WireResult<()> {
    struct Validator {
        nodes: usize,
        strings: usize,
    }
    impl Validator {
        fn node(&mut self) -> WireResult<()> {
            self.nodes = self
                .nodes
                .checked_add(1)
                .ok_or(RelationWireError::TooLarge)?;
            if self.nodes > MAX_RELATION_ITEMS {
                return Err(RelationWireError::Invalid("node count exceeds limit"));
            }
            Ok(())
        }
        fn collection(&self, len: usize) -> WireResult<()> {
            if len > MAX_RELATION_ITEMS {
                Err(RelationWireError::Invalid("collection exceeds limit"))
            } else {
                Ok(())
            }
        }
        fn text(&mut self, value: &str) -> WireResult<()> {
            if value.len() > MAX_RELATION_STRING_BYTES {
                return Err(RelationWireError::Invalid("string exceeds limit"));
            }
            self.strings = self
                .strings
                .checked_add(value.len())
                .ok_or(RelationWireError::TooLarge)?;
            if self.strings > MAX_RELATION_BYTES {
                Err(RelationWireError::TooLarge)
            } else {
                Ok(())
            }
        }
        fn column(&mut self, value: &WireRelationColumnRef) -> WireResult<()> {
            if let Some(scope) = &value.scope {
                self.text(scope)?;
            }
            self.text(&value.column)
        }
        fn key(&mut self, value: &WireRelationKeyRef) -> WireResult<()> {
            self.node()?;
            match value {
                WireRelationKeyRef::Column(column) => self.column(column),
                WireRelationKeyRef::RowId(_) => Ok(()),
            }
        }
        fn value(&mut self, value: &WireRelationValueRef, depth: usize) -> WireResult<()> {
            self.node()?;
            match value {
                WireRelationValueRef::Literal(value) => self.json(value, depth + 1),
                WireRelationValueRef::Param(value) => self.text(value),
                WireRelationValueRef::SessionRef(values) => {
                    self.collection(values.len())?;
                    for value in values {
                        self.text(value)?;
                    }
                    Ok(())
                }
                WireRelationValueRef::OuterColumn(value)
                | WireRelationValueRef::FrontierColumn(value) => self.column(value),
                WireRelationValueRef::RowId(_) => Ok(()),
            }
        }
        fn json(&mut self, value: &WireJson, depth: usize) -> WireResult<()> {
            if depth > MAX_RELATION_DEPTH {
                return Err(RelationWireError::Invalid("depth exceeds limit"));
            }
            self.node()?;
            match value {
                WireJson::F64(bits) if !f64::from_bits(*bits).is_finite() => {
                    Err(RelationWireError::NonFiniteNumber)
                }
                WireJson::String(value) => self.text(value),
                WireJson::Array(values) => {
                    self.collection(values.len())?;
                    for value in values {
                        self.json(value, depth + 1)?;
                    }
                    Ok(())
                }
                WireJson::Object(entries) => {
                    self.collection(entries.len())?;
                    let mut previous: Option<&str> = None;
                    for (key, value) in entries {
                        self.text(key)?;
                        if previous.is_some_and(|previous| previous >= key.as_str()) {
                            return Err(RelationWireError::Invalid(
                                "object keys are not canonical",
                            ));
                        }
                        previous = Some(key);
                        self.json(value, depth + 1)?;
                    }
                    Ok(())
                }
                _ => Ok(()),
            }
        }
        fn predicate(&mut self, value: &WireRelationPredicate, depth: usize) -> WireResult<()> {
            if depth > MAX_RELATION_DEPTH {
                return Err(RelationWireError::Invalid("depth exceeds limit"));
            }
            self.node()?;
            match value {
                WireRelationPredicate::Cmp { left, right, .. } => {
                    self.column(left)?;
                    self.value(right, depth + 1)
                }
                WireRelationPredicate::IsNull { column }
                | WireRelationPredicate::IsNotNull { column } => self.column(column),
                WireRelationPredicate::In { left, values } => {
                    self.column(left)?;
                    self.collection(values.len())?;
                    for value in values {
                        self.value(value, depth + 1)?;
                    }
                    Ok(())
                }
                WireRelationPredicate::Contains { left, right } => {
                    self.column(left)?;
                    self.value(right, depth + 1)
                }
                WireRelationPredicate::EnumMatch {
                    column,
                    case,
                    payload,
                } => {
                    self.column(column)?;
                    self.text(case)?;
                    self.predicate(payload, depth + 1)
                }
                WireRelationPredicate::And(values) | WireRelationPredicate::Or(values) => {
                    self.collection(values.len())?;
                    for value in values {
                        self.predicate(value, depth + 1)?;
                    }
                    Ok(())
                }
                WireRelationPredicate::Not(value) => self.predicate(value, depth + 1),
                WireRelationPredicate::True | WireRelationPredicate::False => Ok(()),
            }
        }
        fn expr(&mut self, value: &WireRelationExpr, depth: usize) -> WireResult<()> {
            if depth > MAX_RELATION_DEPTH {
                return Err(RelationWireError::Invalid("depth exceeds limit"));
            }
            self.node()?;
            match value {
                WireRelationExpr::TableScan { table, alias } => {
                    self.text(table)?;
                    if let Some(alias) = alias {
                        self.text(alias)?;
                    }
                    Ok(())
                }
                WireRelationExpr::Filter { input, predicate } => {
                    self.expr(input, depth + 1)?;
                    self.predicate(predicate, depth + 1)
                }
                WireRelationExpr::Union { inputs } => {
                    self.collection(inputs.len())?;
                    let mut labels = BTreeSet::new();
                    for arm in inputs {
                        self.text(&arm.label)?;
                        if arm.label.is_empty()
                            || arm.label.len() > 4096
                            || arm.label.contains('\0')
                            || !labels.insert(&arm.label)
                        {
                            return Err(RelationWireError::Invalid("invalid union label"));
                        }
                        self.expr(&arm.input, depth + 1)?;
                    }
                    Ok(())
                }
                WireRelationExpr::Join {
                    left, right, on, ..
                } => {
                    self.expr(left, depth + 1)?;
                    self.expr(right, depth + 1)?;
                    self.collection(on.len())?;
                    for condition in on {
                        self.node()?;
                        self.column(&condition.left)?;
                        self.column(&condition.right)?;
                    }
                    Ok(())
                }
                WireRelationExpr::Project { input, columns } => {
                    self.expr(input, depth + 1)?;
                    self.collection(columns.len())?;
                    for column in columns {
                        self.node()?;
                        self.text(&column.alias)?;
                        match &column.expr {
                            WireRelationProjectExpr::Column(value) => self.column(value)?,
                            WireRelationProjectExpr::RowId(_) => {}
                        }
                    }
                    Ok(())
                }
                WireRelationExpr::Gather {
                    seed,
                    step,
                    frontier_key,
                    dedupe_key,
                    ..
                } => {
                    self.expr(seed, depth + 1)?;
                    self.expr(step, depth + 1)?;
                    self.key(frontier_key)?;
                    self.collection(dedupe_key.len())?;
                    for key in dedupe_key {
                        self.key(key)?;
                    }
                    Ok(())
                }
                WireRelationExpr::Distinct { input, key } => {
                    self.expr(input, depth + 1)?;
                    self.collection(key.len())?;
                    for key in key {
                        self.key(key)?;
                    }
                    Ok(())
                }
                WireRelationExpr::OrderBy { input, terms } => {
                    self.expr(input, depth + 1)?;
                    self.collection(terms.len())?;
                    for term in terms {
                        self.node()?;
                        self.column(&term.column)?;
                    }
                    Ok(())
                }
                WireRelationExpr::Offset { input, .. } | WireRelationExpr::Limit { input, .. } => {
                    self.expr(input, depth + 1)
                }
            }
        }
    }
    let mut validator = Validator {
        nodes: 0,
        strings: 0,
    };
    validator.expr(&value.rel, 0)
}
#[cfg(test)]
mod relation_postcard_tests {
    use super::*;

    #[derive(serde::Deserialize)]
    struct Corpus {
        cases: Vec<CorpusCase>,
    }
    #[derive(serde::Deserialize)]
    struct CorpusCase {
        name: String,
        relation: serde_json::Value,
        postcard_hex: String,
    }

    #[test]
    fn typed_postcard_corpus_is_current() {
        let source = include_str!("../../fixtures/relation_query_postcard.json");
        let corpus: Corpus = serde_json::from_str(source).unwrap();
        if std::env::var_os("JAZZ_UPDATE_RELATION_POSTCARD_CORPUS").is_some() {
            let mut updated = source.to_owned();
            for case in corpus.cases {
                let query: RelationQuery = serde_json::from_value(case.relation).unwrap();
                let actual = hex::encode(encode_relation_query_postcard(&query).unwrap());
                updated = updated.replacen(
                    &format!(r#""postcard_hex": "{}""#, case.postcard_hex),
                    &format!(r#""postcard_hex": "{}""#, actual),
                    1,
                );
            }
            std::fs::write(
                concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/relation_query_postcard.json"
                ),
                updated,
            )
            .unwrap();
            return;
        }
        for case in corpus.cases {
            let query: RelationQuery = serde_json::from_value(case.relation).unwrap();
            assert_eq!(
                hex::encode(encode_relation_query_postcard(&query).unwrap()),
                case.postcard_hex,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn typed_postcard_relation_round_trips_literal_kinds() {
        let query = RelationQuery {
            rel: RelationExpr::Filter {
                input: Box::new(RelationExpr::TableScan {
                    table: "rows".into(),
                    alias: None,
                }),
                predicate: RelationPredicate::In {
                    left: RelationColumnRef {
                        scope: None,
                        column: "value".into(),
                    },
                    values: vec![
                        RelationValueRef::Literal(serde_json::json!(-4)),
                        RelationValueRef::Literal(serde_json::json!(4)),
                        RelationValueRef::Literal(serde_json::json!({"nested": [true, null, 1.5]})),
                    ],
                },
            },
        };
        let bytes = encode_relation_query_postcard(&query).unwrap();
        assert!(!bytes.starts_with(b"JRQ\x01"));
        assert_eq!(decode_relation_query_postcard(&bytes).unwrap(), query);
    }

    #[test]
    fn typed_postcard_rejects_trailing_or_noncanonical_payloads() {
        let query = RelationQuery {
            rel: RelationExpr::TableScan {
                table: "rows".into(),
                alias: None,
            },
        };
        let mut bytes = encode_relation_query_postcard(&query).unwrap();
        bytes.push(0);
        assert!(decode_relation_query_postcard(&bytes).is_err());
    }

    #[test]
    fn typed_postcard_rejects_deep_filter_before_ast_construction() {
        let table_scan = [0, 1, b't', 0];
        let mut boundary = vec![1; MAX_RELATION_DEPTH];
        boundary.extend([0, 1, b't', 0]);
        boundary.extend(std::iter::repeat_n(9, MAX_RELATION_DEPTH));
        assert!(decode_relation_query_postcard(&boundary).is_ok());

        let mut relation = vec![1; MAX_RELATION_DEPTH + 1];
        relation.extend([0, 1, b't', 0]);
        relation.extend(std::iter::repeat_n(9, MAX_RELATION_DEPTH + 1));
        assert!(std::panic::catch_unwind(|| decode_relation_query_postcard(&relation)).is_ok());
        assert!(decode_relation_query_postcard(&relation).is_err());
        assert_eq!(
            decode_relation_query_postcard(&table_scan).unwrap(),
            RelationQuery {
                rel: RelationExpr::TableScan {
                    table: "t".into(),
                    alias: None,
                },
            }
        );

        let mut query = Query::from("t");
        query.relation = Some(RelationQuery {
            rel: RelationExpr::TableScan {
                table: "t".into(),
                alias: None,
            },
        });
        let mut query_bytes = postcard::to_allocvec(&query).unwrap();
        assert!(query_bytes.ends_with(&table_scan));
        query_bytes.truncate(query_bytes.len() - table_scan.len());
        query_bytes.extend(&relation);
        assert!(std::panic::catch_unwind(|| postcard::from_bytes::<Query>(&query_bytes)).is_ok());
        assert!(postcard::from_bytes::<Query>(&query_bytes).is_err());
        let valid_query_bytes = postcard::to_allocvec(&query).unwrap();
        assert_eq!(
            crate::wire::decode_postcard_exact::<Query>(&valid_query_bytes).unwrap(),
            query
        );

        let shape = crate::protocol::ShapeAst::new_relation(
            RelationQuery {
                rel: RelationExpr::TableScan {
                    table: "t".into(),
                    alias: None,
                },
            },
            crate::ids::SchemaVersionId(uuid::Uuid::nil()),
        );
        let mut shape_bytes = postcard::to_allocvec(&shape).unwrap();
        assert!(shape_bytes.ends_with(&table_scan));
        shape_bytes.truncate(shape_bytes.len() - table_scan.len());
        shape_bytes.extend(&relation);
        assert!(
            std::panic::catch_unwind(|| postcard::from_bytes::<crate::protocol::ShapeAst>(
                &shape_bytes
            ))
            .is_ok()
        );
        assert!(postcard::from_bytes::<crate::protocol::ShapeAst>(&shape_bytes).is_err());
        let valid_shape_bytes = postcard::to_allocvec(&shape).unwrap();
        assert_eq!(
            crate::wire::decode_postcard_exact::<crate::protocol::ShapeAst>(&valid_shape_bytes)
                .unwrap(),
            shape
        );

        let mut union = Vec::new();
        for _ in 0..=MAX_RELATION_DEPTH {
            union.extend([2, 1, 1, b'x']);
        }
        union.extend([0, 1, b't', 0]);
        assert!(std::panic::catch_unwind(|| decode_relation_query_postcard(&union)).is_ok());
        assert!(decode_relation_query_postcard(&union).is_err());
    }

    #[test]
    fn typed_postcard_node_limit_matches_union_and_value_boundaries() {
        let union = RelationQuery {
            rel: RelationExpr::Union {
                inputs: (0..(MAX_RELATION_ITEMS - 1))
                    .map(|index| RelationUnionArm {
                        label: format!("u{index}"),
                        input: RelationExpr::TableScan {
                            table: "t".into(),
                            alias: None,
                        },
                    })
                    .collect(),
            },
        };
        assert!(encode_relation_query_postcard(&union).is_ok());

        let values = RelationQuery {
            rel: RelationExpr::Filter {
                input: Box::new(RelationExpr::TableScan {
                    table: "t".into(),
                    alias: None,
                }),
                predicate: RelationPredicate::In {
                    left: RelationColumnRef {
                        scope: None,
                        column: "c".into(),
                    },
                    values: std::iter::repeat_n(
                        RelationValueRef::RowId(RelationRowIdRef::Current),
                        MAX_RELATION_ITEMS - 3,
                    )
                    .collect(),
                },
            },
        };
        assert!(encode_relation_query_postcard(&values).is_ok());
    }

    #[test]
    fn query_and_shape_carry_relation_postcard_recursively() {
        let relation = RelationQuery {
            rel: RelationExpr::Filter {
                input: Box::new(RelationExpr::TableScan {
                    table: "rows".into(),
                    alias: None,
                }),
                predicate: RelationPredicate::Cmp {
                    left: RelationColumnRef {
                        scope: None,
                        column: "value".into(),
                    },
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::Literal(serde_json::json!(1.5)),
                },
            },
        };
        let mut query = Query::from("rows");
        query.relation = Some(relation.clone());
        let query_bytes = postcard::to_allocvec(&query).unwrap();
        assert_eq!(postcard::from_bytes::<Query>(&query_bytes).unwrap(), query);

        let shape = crate::protocol::ShapeAst::new_relation(
            relation,
            crate::ids::SchemaVersionId(uuid::Uuid::nil()),
        );
        let shape_bytes = postcard::to_allocvec(&shape).unwrap();
        assert_eq!(
            postcard::from_bytes::<crate::protocol::ShapeAst>(&shape_bytes).unwrap(),
            shape
        );
    }
}
