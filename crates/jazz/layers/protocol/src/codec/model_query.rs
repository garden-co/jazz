//! Sync-wire codecs for the query AST (`jazz_model::query`): the shape
//! carried by `RegisterShape` and by a transaction's predicate reads, plus
//! shape and binding ids.

use std::cell::RefCell;

use crate::codec::{CodecError, Wire, WireCodec, get_len, read_u8, record_encode_error};
use crate::query::policy_limits::PolicyExpressionBudget;
use crate::query::{
    Aggregate, AggregateFunction, AggregateQuery, ArraySubquery, ArraySubqueryRequirement,
    BindingId, FlatJoin, FlatJoinOn, FlatJoinSource, Include, InheritsOperation, InheritsVia,
    JoinCorrelation, JoinMode, JoinSourceLookup, JoinTarget, JoinVia, Operand, OrderBy,
    OrderDirection, PolicyBranch, Predicate, Query, ReachableSeed, ReachableVia, RecursionBound,
    RelationQuery, ShapeId, relation_query_wire,
};

macro_rules! uuid_newtype {
    ($($ty:ident),+) => {$(
        impl WireCodec for $ty {
            fn put(&self, buf: &mut Vec<u8>) {
                self.0.put(buf);
            }

            fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
                uuid::Uuid::get(data, offset).map(Self)
            }
        }
    )+};
}

uuid_newtype!(ShapeId, BindingId);

/// `Query::relation` keeps the typed relation-IR Postcard encoding.
struct RelationField;

impl Wire<Option<RelationQuery>> for RelationField {
    fn put(&self, buf: &mut Vec<u8>, value: &Option<RelationQuery>) {
        if let Err(message) = relation_query_wire::encode_postcard(value, buf) {
            record_encode_error(CodecError::Encode { message });
        }
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<Option<RelationQuery>, CodecError> {
        let (value, used) = relation_query_wire::decode_postcard(&data[*offset..])
            .map_err(|message| CodecError::DecodeError { message })?;
        *offset += used;
        Ok(value)
    }
}

crate::wire_struct! {
    for Query {
        table,
        filters,
        joins,
        flat_join,
        policy_branches,
        reachable,
        inherits,
        includes,
        array_subqueries,
        select,
        order_by,
        aggregate,
        limit,
        offset,
        relation: RelationField,
    }
}

crate::wire_struct! { for FlatJoin { root_alias, sources } }
crate::wire_struct! { for FlatJoinSource { table, alias, on } }
crate::wire_struct! { for FlatJoinOn { left, right } }
crate::wire_struct! { for PolicyBranch { filters, joins, reachable, inherits } }
crate::wire_struct! { for Include { path, join_mode, require } }
crate::wire_struct! {
    for ArraySubquery {
        column_name,
        table,
        inner_column,
        outer_column,
        filters,
        select,
        order_by,
        limit,
        offset,
        requirement,
        nested_arrays,
    }
}
crate::wire_struct! { for OrderBy { column, direction } }
crate::wire_struct! { for AggregateQuery { aggregates, group_by } }
crate::wire_struct! { for Aggregate { function, column, alias } }
crate::wire_struct! {
    for JoinVia {
        table,
        on_column,
        target,
        source_column,
        source_lookup,
        correlated_filters,
        filters,
        nested_joins,
    }
}
crate::wire_struct! { for JoinCorrelation { join_column, source_column } }
crate::wire_struct! { for JoinSourceLookup { table, row_id_source_column, value_column } }
crate::wire_struct! {
    for ReachableVia {
        access_table,
        access_row_column,
        access_team_column,
        access_team_target,
        from,
        access_filters,
        edge_table,
        edge_member_column,
        edge_parent_column,
        edge_filters,
        bound,
        seed,
    }
}
crate::wire_struct! { for ReachableSeed { table, user_column, user_claim, team_column, filters } }
crate::wire_struct! { for InheritsVia { parent_column, operation, max_depth } }

crate::wire_enum! { for JoinMode { 0 => Inner, 1 => Holes } }
crate::wire_enum! {
    for ArraySubqueryRequirement { 0 => Optional, 1 => AtLeastOne, 2 => MatchCorrelationCardinality }
}
crate::wire_enum! { for OrderDirection { 0 => Asc, 1 => Desc } }
crate::wire_enum! { for AggregateFunction { 0 => Count, 1 => Sum, 2 => Avg, 3 => Min, 4 => Max } }
crate::wire_enum! { for JoinTarget { 0 => Column, 1 => RowId, 2 => Uncorrelated } }
crate::wire_enum! { for InheritsOperation { 0 => Select, 1 => Insert, 2 => Update, 3 => Delete } }
crate::wire_enum! { for RecursionBound { 0 => Fixpoint, 1 => MaxDepth(depth) } }
crate::wire_enum! {
    for Operand { 0 => Column(name), 1 => Param(name), 2 => Claim(name), 3 => Literal(value) }
}

thread_local! {
    /// Nesting depth and node budget of the predicate tree being decoded on
    /// this thread; depth 0 means no decode is in progress. Every outermost
    /// predicate gets a fresh budget, as with the model's serde decoder.
    static PREDICATE_DECODE: RefCell<(usize, PolicyExpressionBudget)> = RefCell::default();
}

/// Leaves one predicate nesting level, resetting the budget at the root.
struct LeavePredicate;

impl Drop for LeavePredicate {
    fn drop(&mut self) {
        PREDICATE_DECODE.with_borrow_mut(|(depth, budget)| {
            *depth -= 1;
            if *depth == 0 {
                *budget = PolicyExpressionBudget::default();
            }
        });
    }
}

impl WireCodec for Predicate {
    fn put(&self, buf: &mut Vec<u8>) {
        let pair = |buf: &mut Vec<u8>, tag: u8, left: &Operand, right: &Operand| {
            buf.push(tag);
            left.put(buf);
            right.put(buf);
        };
        match self {
            Self::All(children) => {
                buf.push(0);
                children.put(buf);
            }
            Self::Any(children) => {
                buf.push(1);
                children.put(buf);
            }
            Self::Not(child) => {
                buf.push(2);
                child.put(buf);
            }
            Self::Eq(left, right) => pair(buf, 3, left, right),
            Self::Ne(left, right) => pair(buf, 4, left, right),
            Self::In(left, values) => {
                buf.push(5);
                left.put(buf);
                values.put(buf);
            }
            Self::Gt(left, right) => pair(buf, 6, left, right),
            Self::Gte(left, right) => pair(buf, 7, left, right),
            Self::Lt(left, right) => pair(buf, 8, left, right),
            Self::Lte(left, right) => pair(buf, 9, left, right),
            Self::Contains(left, right) => pair(buf, 10, left, right),
            Self::EnumMatch {
                column,
                case,
                payload,
            } => {
                buf.push(11);
                column.put(buf);
                case.put(buf);
                payload.put(buf);
            }
            Self::IsNull(operand) => {
                buf.push(12);
                operand.put(buf);
            }
        }
    }

    /// Counts every node against the budget of the outermost predicate at its
    /// nesting depth before its contents are read, and refuses an `All`/`Any`
    /// child count the budget cannot hold before reading any child.
    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        let _leave = PREDICATE_DECODE.with_borrow_mut(|(depth, _)| {
            *depth += 1;
            LeavePredicate
        });
        PREDICATE_DECODE.with_borrow_mut(|(depth, budget)| budget.enter(*depth))?;
        let children = |data: &[u8], offset: &mut usize| -> Result<Vec<Predicate>, CodecError> {
            let len = get_len(data, offset)?;
            PREDICATE_DECODE.with_borrow(|(_, budget)| budget.reserve(len))?;
            let mut children = Vec::with_capacity(len);
            for _ in 0..len {
                children.push(Predicate::get(data, offset)?);
            }
            Ok(children)
        };
        let pair = |data: &[u8], offset: &mut usize| -> Result<(Operand, Operand), CodecError> {
            Ok((Operand::get(data, offset)?, Operand::get(data, offset)?))
        };
        Ok(match read_u8(data, offset)? {
            0 => Self::All(children(data, offset)?),
            1 => Self::Any(children(data, offset)?),
            2 => Self::Not(Box::get(data, offset)?),
            3 => pair(data, offset).map(|(l, r)| Self::Eq(l, r))?,
            4 => pair(data, offset).map(|(l, r)| Self::Ne(l, r))?,
            5 => Self::In(Operand::get(data, offset)?, WireCodec::get(data, offset)?),
            6 => pair(data, offset).map(|(l, r)| Self::Gt(l, r))?,
            7 => pair(data, offset).map(|(l, r)| Self::Gte(l, r))?,
            8 => pair(data, offset).map(|(l, r)| Self::Lt(l, r))?,
            9 => pair(data, offset).map(|(l, r)| Self::Lte(l, r))?,
            10 => pair(data, offset).map(|(l, r)| Self::Contains(l, r))?,
            11 => Self::EnumMatch {
                column: String::get(data, offset)?,
                case: String::get(data, offset)?,
                payload: Box::get(data, offset)?,
            },
            12 => Self::IsNull(Operand::get(data, offset)?),
            tag => {
                return Err(CodecError::InvalidTypeTag {
                    tag,
                    context: "Predicate",
                });
            }
        })
    }
}

#[cfg(test)]
mod tests {
    //! Differential tests: the hand codecs must spell exactly the bytes the
    //! serde derives give Postcard, so they are checked against Postcard
    //! directly rather than through a public API.

    use super::*;
    use crate::codec::{decode_exact, encode, take_encode_error};
    use crate::query::{
        RelationCmpOp, RelationExpr, RelationPredicate, RelationValueRef, claim, col, eq, lit,
        param,
    };
    use groove::records::Value;
    use serde::{Serialize, de::DeserializeOwned};
    use std::fmt::Debug;

    fn same<T: WireCodec + Serialize + DeserializeOwned + PartialEq + Debug>(value: &T) {
        let bytes = postcard::to_allocvec(value).unwrap();
        assert_eq!(encode(value), bytes, "{value:?}");
        assert_eq!(&decode_exact::<T>(&bytes).unwrap(), value);
        assert_eq!(&postcard::from_bytes::<T>(&bytes).unwrap(), value);
    }

    fn every_predicate() -> Vec<Predicate> {
        let (a, b) = (col("a"), param("b"));
        vec![
            Predicate::All(vec![]),
            Predicate::All(vec![eq(a.clone(), b.clone()), Predicate::Any(vec![])]),
            Predicate::Any(vec![Predicate::IsNull(claim("c"))]),
            Predicate::Not(Box::new(Predicate::All(vec![]))),
            Predicate::Eq(a.clone(), lit(Value::U64(u64::MAX))),
            Predicate::Ne(a.clone(), b.clone()),
            Predicate::In(a.clone(), vec![]),
            Predicate::In(a.clone(), vec![b.clone(), lit(Value::String("x".into()))]),
            Predicate::Gt(a.clone(), b.clone()),
            Predicate::Gte(a.clone(), b.clone()),
            Predicate::Lt(a.clone(), b.clone()),
            Predicate::Lte(a.clone(), b.clone()),
            Predicate::Contains(a.clone(), b.clone()),
            Predicate::EnumMatch {
                column: "kind".into(),
                case: "done".into(),
                payload: Box::new(Predicate::IsNull(Operand::Claim("x".into()))),
            },
            Predicate::IsNull(Operand::Literal(Value::Nullable(None))),
        ]
    }

    fn join(nested: Vec<JoinVia>, target: JoinTarget) -> JoinVia {
        JoinVia {
            table: "tags".into(),
            on_column: "issue".into(),
            target,
            source_column: Some("id".into()),
            source_lookup: Some(JoinSourceLookup {
                table: "t".into(),
                row_id_source_column: "r".into(),
                value_column: "v".into(),
            }),
            correlated_filters: vec![JoinCorrelation {
                join_column: "j".into(),
                source_column: "s".into(),
            }],
            filters: every_predicate(),
            nested_joins: nested,
        }
    }

    fn reachable(bound: RecursionBound, seed: Option<ReachableSeed>) -> ReachableVia {
        ReachableVia {
            access_table: "access".into(),
            access_row_column: "row".into(),
            access_team_column: "team".into(),
            access_team_target: JoinTarget::RowId,
            from: claim("team"),
            access_filters: vec![Predicate::All(vec![])],
            edge_table: "edges".into(),
            edge_member_column: "member".into(),
            edge_parent_column: "parent".into(),
            edge_filters: vec![],
            bound,
            seed,
        }
    }

    fn full_query() -> Query {
        let mut query = Query::from("issues");
        query.filters = every_predicate();
        query.joins = vec![
            join(
                vec![join(vec![], JoinTarget::Uncorrelated)],
                JoinTarget::Column,
            ),
            JoinVia {
                source_column: None,
                source_lookup: None,
                ..join(vec![], JoinTarget::RowId)
            },
        ];
        query.flat_join = Some(FlatJoin {
            root_alias: Some("i".into()),
            sources: vec![FlatJoinSource {
                table: "users".into(),
                alias: None,
                on: FlatJoinOn {
                    left: "i.owner".into(),
                    right: "users.id".into(),
                },
            }],
        });
        query.policy_branches = vec![PolicyBranch {
            filters: vec![eq(col("x"), lit(Value::Bool(true)))],
            joins: vec![],
            reachable: vec![reachable(RecursionBound::Fixpoint, None)],
            inherits: vec![InheritsVia {
                parent_column: "p".into(),
                operation: InheritsOperation::Delete,
                max_depth: Some(0),
            }],
        }];
        query.reachable = vec![reachable(
            RecursionBound::MaxDepth(300),
            Some(ReachableSeed {
                table: "seeds".into(),
                user_column: Some("u".into()),
                user_claim: None,
                team_column: "team".into(),
                filters: vec![Predicate::Any(vec![])],
            }),
        )];
        query.inherits = [
            InheritsOperation::Select,
            InheritsOperation::Insert,
            InheritsOperation::Update,
        ]
        .into_iter()
        .map(|operation| InheritsVia {
            parent_column: "parent".into(),
            operation,
            max_depth: None,
        })
        .collect();
        query.includes = vec![
            Include::new("owner"),
            Include::new("a.b")
                .join_mode(JoinMode::Holes)
                .require_includes(),
        ];
        query.array_subqueries = vec![
            ArraySubquery::new("comments", "comments", "issue", "id")
                .filter(eq(col("x"), param("y")))
                .select(["body"])
                .order_by("at", OrderDirection::Desc)
                .limit(5)
                .offset(1 << 20)
                .requirement(ArraySubqueryRequirement::MatchCorrelationCardinality)
                .nested(
                    ArraySubquery::new("likes", "likes", "comment", "id")
                        .requirement(ArraySubqueryRequirement::AtLeastOne),
                ),
        ];
        query.select = Some(vec!["title".into(), String::new()]);
        query.order_by = vec![OrderBy {
            column: "title".into(),
            direction: OrderDirection::Asc,
        }];
        query.aggregate = Some(Box::new(AggregateQuery {
            aggregates: vec![
                Aggregate::count(),
                Aggregate::sum("a"),
                Aggregate::avg("a"),
                Aggregate::min("a"),
                Aggregate::max("a").alias("m"),
            ],
            group_by: Some("g".into()),
        }));
        query.limit = Some(usize::MAX);
        query.offset = 128;
        query
    }

    fn relation() -> RelationQuery {
        RelationQuery {
            rel: RelationExpr::Filter {
                input: Box::new(RelationExpr::TableScan {
                    table: "issues".into(),
                    alias: Some("i".into()),
                }),
                predicate: RelationPredicate::Cmp {
                    left: "title".into(),
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::Literal(serde_json::json!({"a": [1, 2.5, null]})),
                },
            },
        }
    }

    #[test]
    fn query_tree_matches_postcard() {
        same(&Query::from("issues"));
        same(&full_query());
        let mut with_relation = Query::from("issues");
        with_relation.relation = Some(relation());
        same(&with_relation);
        assert_eq!(take_encode_error(), None);
        for predicate in every_predicate() {
            same(&predicate);
        }
        same(&Operand::Param("p".into()));
        same(&RecursionBound::MaxDepth(0));
        same(&ShapeId(uuid::Uuid::from_bytes([7; 16])));
        same(&BindingId(uuid::Uuid::nil()));
    }

    #[test]
    fn relation_encode_failure_is_recorded() {
        let mut query = Query::from("issues");
        query.relation = Some(RelationQuery {
            rel: RelationExpr::TableScan {
                table: "t".repeat(1 << 17),
                alias: None,
            },
        });
        assert!(postcard::to_allocvec(&query).is_err());
        encode(&query);
        assert!(matches!(
            take_encode_error(),
            Some(CodecError::Encode { .. })
        ));
        assert_eq!(take_encode_error(), None);
    }

    #[test]
    fn predicate_decode_keeps_the_policy_limits() {
        use crate::protocol_limits::{MAX_POLICY_EXPRESSION_DEPTH, MAX_POLICY_EXPRESSION_NODES};

        // Depth: a chain of `Not` one deeper than allowed.
        let mut deep = Predicate::All(vec![]);
        for _ in 0..MAX_POLICY_EXPRESSION_DEPTH {
            deep = Predicate::Not(Box::new(deep));
        }
        let bytes = postcard::to_allocvec(&deep).unwrap();
        assert!(postcard::from_bytes::<Predicate>(&bytes).is_err());
        assert!(matches!(
            decode_exact::<Predicate>(&bytes),
            Err(CodecError::ProtocolLimit(error)) if error.limit == "MAX_POLICY_EXPRESSION_DEPTH"
        ));
        // At the limit it decodes, and the budget is per outermost predicate.
        let Predicate::Not(inner) = deep else {
            unreachable!()
        };
        same(&*inner);
        same(&vec![(*inner).clone(), *inner]);

        // Node count: a child-count header the budget cannot hold.
        let mut bytes = vec![0];
        crate::codec::put_len(&mut bytes, MAX_POLICY_EXPRESSION_NODES);
        assert!(postcard::from_bytes::<Predicate>(&bytes).is_err());
        assert!(matches!(
            decode_exact::<Predicate>(&bytes),
            Err(CodecError::ProtocolLimit(error)) if error.limit == "MAX_POLICY_EXPRESSION_NODES"
        ));
        // The whole node budget fits exactly; one more node does not.
        let full = Predicate::Any(vec![
            Predicate::All(vec![]);
            MAX_POLICY_EXPRESSION_NODES - 1
        ]);
        same(&full);
        let over = Predicate::Any(vec![Predicate::All(vec![]); MAX_POLICY_EXPRESSION_NODES]);
        assert!(postcard::from_bytes::<Predicate>(&encode(&over)).is_err());
        assert!(decode_exact::<Predicate>(&encode(&over)).is_err());
        // A failed decode leaves no state behind for the next one.
        same(&Predicate::All(vec![]));
        assert_eq!(
            decode_exact::<Predicate>(&[13]),
            Err(CodecError::InvalidTypeTag {
                tag: 13,
                context: "Predicate"
            })
        );
    }
}
