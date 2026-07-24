//! Query lowering, name resolution, and type checking for IVM graphs.
//!
//! This module owns the path from [`crate::queries::Query`] to a normalized
//! [`LogicalPlan`] and executable [`GraphBuilder`]. It resolves table aliases,
//! field names, join keys, CTE scopes, parameter relations for prepared shapes,
//! and output descriptors. It does not execute operators or store state; the
//! runtime consumes the graph builders produced here, while the query AST
//! module remains syntax-only.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

use crate::queries::{
    BinaryOp, Expr, JoinConstraint, JoinKind, Query, Select, SelectItem, SelectQuantifier,
    SetOperator, SetQuantifier, TableRef, UnaryOp,
};
use crate::records::ValueType;
use crate::schema::{DatabaseSchema, TableSchema};
use thiserror::Error;

use super::{GraphBuilder, LiteralValue, PredicateExpr, PredicateKind, ProjectField};

/// Query plan plus executable graph produced by lowering.
#[derive(Clone, Debug, PartialEq)]
pub struct PlannedQuery {
    /// Normalized, type-checked representation useful for tests and future
    /// optimization passes.
    pub logical: LogicalPlan,
    /// Executable subscription graph produced from the logical plan.
    pub graph: GraphBuilder,
    /// The query's output columns, in order, with resolved types.
    pub output: Vec<LogicalField>,
}

impl PlannedQuery {
    /// The record layout of the query's result rows, built from `output`.
    pub fn output_descriptor(&self) -> crate::records::RecordDescriptor {
        crate::records::RecordDescriptor::new(
            self.output
                .iter()
                .map(|field| (field.name.clone(), field.value_type.clone())),
        )
    }
}

/// A planned parameterized query, ready to be installed as a prepared shape.
///
/// Produced by [`plan_prepared_shape`] from a query containing `:name`
/// parameters. The parameters become a maintained binding relation the graph
/// joins against, so binding a value is a data change rather than a new
/// query.
#[derive(Clone, Debug, PartialEq)]
pub struct PlannedPreparedShape {
    /// The plan and graph, with the binding relation already spliced in.
    pub planned: PlannedQuery,
    /// The shape's derived name; all binding sources in the graph carry it.
    pub shape: String,
    /// The distinct parameters, sorted by name.
    pub parameters: Vec<QueryParameter>,
    /// Record layout of one binding row: one field per parameter.
    pub binding_descriptor: crate::records::RecordDescriptor,
    /// The output columns that carry the parameter values, used by the
    /// runtime to route each output row back to the binding that produced
    /// it.
    pub output_key_fields: Vec<String>,
    /// The output columns visible to subscribers — `planned.output` minus
    /// the internal binding columns.
    pub public_output: Vec<LogicalField>,
}

/// One named query parameter with its resolved type.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QueryParameter {
    /// The parameter's name, without the `:` prefix.
    pub name: String,
    /// The type inferred from the column the parameter is compared to.
    pub value_type: ValueType,
}

/// Normalized relational plan between the SQL AST and the graph builder.
///
/// Every variant carries its resolved output `fields`, so name resolution
/// happens once, here, and later passes only look fields up.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalPlan {
    /// Read one base table.
    Scan {
        table: String,
        /// The SQL alias, when the table was aliased in `FROM`.
        alias: Option<String>,
        fields: Vec<LogicalField>,
    },
    /// Read a prepared shape's bound parameter rows (one row per binding);
    /// created for `column = :param` predicates.
    BindingRelation {
        shape: String,
        fields: Vec<LogicalField>,
    },
    /// Keep rows matching `predicate`.
    Filter {
        input: Box<LogicalPlan>,
        predicate: PredicateExpr,
        fields: Vec<LogicalField>,
    },
    /// Reorder, rename, or drop columns.
    Project {
        input: Box<LogicalPlan>,
        fields: Vec<LogicalField>,
    },
    /// Inner equi-join of two plans.
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        /// Resolved source field names on each side. Runtime lowering turns
        /// these into `PlanExpr::Field` and descriptor positions.
        left_on: Vec<String>,
        right_on: Vec<String>,
        fields: Vec<LogicalField>,
    },
    /// Sum of the inputs' rows (`UNION ALL`); inputs must be
    /// column-compatible.
    UnionAll {
        inputs: Vec<LogicalPlan>,
        fields: Vec<LogicalField>,
    },
}

impl LogicalPlan {
    /// The plan's resolved output columns, in order.
    pub fn fields(&self) -> &[LogicalField] {
        match self {
            Self::Scan { fields, .. }
            | Self::BindingRelation { fields, .. }
            | Self::Filter { fields, .. }
            | Self::Project { fields, .. }
            | Self::Join { fields, .. }
            | Self::UnionAll { fields, .. } => fields,
        }
    }

    /// Recursively canonicalizes the plan — today that means canonicalizing
    /// every filter predicate (see [`PredicateExpr::canonicalize`]) — so
    /// logically identical queries lower to identical graphs and share
    /// nodes.
    pub fn canonicalize(self) -> Self {
        match self {
            Self::Filter {
                input,
                predicate,
                fields,
            } => Self::Filter {
                input: Box::new(input.canonicalize()),
                predicate: predicate.canonicalize(),
                fields,
            },
            Self::BindingRelation { .. } => self,
            Self::Project { input, fields } => Self::Project {
                input: Box::new(input.canonicalize()),
                fields,
            },
            Self::Join {
                left,
                right,
                left_on,
                right_on,
                fields,
            } => Self::Join {
                left: Box::new(left.canonicalize()),
                right: Box::new(right.canonicalize()),
                left_on,
                right_on,
                fields,
            },
            Self::UnionAll { inputs, fields } => Self::UnionAll {
                inputs: inputs.into_iter().map(Self::canonicalize).collect(),
                fields,
            },
            plan => plan,
        }
    }
}

/// Resolved output field in a logical plan.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalField {
    /// Table alias or relation qualifier visible to SQL name resolution.
    pub qualifier: Option<String>,
    /// Output name after projection/aliasing.
    pub name: String,
    /// Original record field used when building Project nodes.
    pub source_name: String,
    /// The field's resolved type.
    pub value_type: ValueType,
}

/// Plans an ordinary (parameter-free) query.
///
/// * `query` — the SQL-ish AST to lower.
/// * `schema` — the database schema names are resolved against.
///
/// Returns the logical plan, the executable [`GraphBuilder`], and the
/// resolved output columns. A query containing `:name` parameters is
/// rejected — those go through [`plan_prepared_shape`] instead.
pub fn plan_query(query: &Query, schema: &DatabaseSchema) -> Result<PlannedQuery, PlannerError> {
    let planned = Planner::new(schema).plan_query(query)?;
    if !planned.parameters.is_empty() {
        return Err(PlannerError::UnsupportedQuery(
            "query parameters require prepare_query",
        ));
    }
    Ok(planned.planned)
}

/// Plans a parameterized query into a prepared shape.
///
/// * `query` — the AST; it must contain at least one `:name` parameter,
///   each used in a `column = :name` equality.
/// * `schema` — the database schema names are resolved against.
///
/// Each parameter equality becomes a join against the shape's binding
/// relation, so all bindings of the shape share one maintained graph. The
/// result carries everything the runtime needs to install the shape: the
/// graph, the binding row layout, and which output columns route rows back
/// to their binding.
pub fn plan_prepared_shape(
    query: &Query,
    schema: &DatabaseSchema,
) -> Result<PlannedPreparedShape, PlannerError> {
    let planned = Planner::new(schema).plan_query(query)?;
    if planned.parameters.is_empty() {
        return Err(PlannerError::UnsupportedQuery(
            "prepare_query requires at least one query parameter",
        ));
    }
    let binding_descriptor = crate::records::RecordDescriptor::new(
        planned
            .parameters
            .iter()
            .map(|parameter| (parameter.name.clone(), parameter.value_type.clone())),
    );
    let output_key_fields = planned
        .parameters
        .iter()
        .map(|parameter| parameter.name.clone())
        .collect();
    let public_output = planned
        .planned
        .output
        .iter()
        .filter(|field| field.qualifier.as_deref() != Some(BINDING_QUALIFIER))
        .cloned()
        .collect();
    Ok(PlannedPreparedShape {
        planned: planned.planned,
        shape: planned.shape,
        parameters: planned.parameters,
        binding_descriptor,
        output_key_fields,
        public_output,
    })
}

/// Stateful lowering context for one query planning operation.
struct Planner<'a> {
    schema: &'a DatabaseSchema,
    /// Non-recursive CTEs in the current WITH scope. Recursive CTE lowering is
    /// intentionally rejected until recursive SQL planning is designed.
    ctes: HashMap<String, LogicalPlan>,
}

impl<'a> Planner<'a> {
    fn new(schema: &'a DatabaseSchema) -> Self {
        Self {
            schema,
            ctes: HashMap::new(),
        }
    }

    /// The full lowering pipeline: lower the AST, canonicalize, collect any
    /// binding parameters, name the shape and stamp it into the plan's
    /// binding relations, then build the executable graph.
    fn plan_query(&mut self, query: &Query) -> Result<PreparedPlan, PlannerError> {
        let mut logical = self.lower_query(query)?.canonicalize();
        let parameters = collect_binding_fields(&logical);
        let shape = if parameters.is_empty() {
            String::new()
        } else {
            let shape = prepared_shape_name(&logical, &parameters);
            rewrite_param_shape(&mut logical, &shape);
            shape
        };
        let graph = graph_from_logical(&logical)?;
        let output = logical.fields().to_vec();
        Ok(PreparedPlan {
            planned: PlannedQuery {
                logical,
                graph,
                output,
            },
            parameters,
            shape,
        })
    }

    /// Lowers one query level: SELECTs, `UNION ALL` set queries, and
    /// non-recursive WITH scopes (each CTE is planned once and cloned where
    /// referenced).
    fn lower_query(&mut self, query: &Query) -> Result<LogicalPlan, PlannerError> {
        match query {
            Query::Select(select) => self.lower_select(select),
            Query::Set(set) => {
                if set.op != SetOperator::Union || set.quantifier != SetQuantifier::All {
                    return Err(PlannerError::UnsupportedQuery(
                        "only UNION ALL set queries are currently lowerable",
                    ));
                }
                let left = self.lower_query(&set.left)?;
                let right = self.lower_query(&set.right)?;
                if comparable_fields(left.fields(), right.fields()) {
                    Ok(LogicalPlan::UnionAll {
                        fields: left.fields().to_vec(),
                        inputs: vec![left, right],
                    })
                } else {
                    Err(PlannerError::OutputMismatch)
                }
            }
            Query::With(with) => {
                if with.recursive {
                    return Err(PlannerError::UnsupportedQuery(
                        "recursive CTE lowering is not implemented yet",
                    ));
                }
                let old_ctes = self.ctes.clone();
                for cte in &with.ctes {
                    let plan = self.lower_query(&cte.query)?;
                    self.ctes.insert(cte.name.clone(), plan);
                }
                let result = self.lower_query(&with.query);
                self.ctes = old_ctes;
                result
            }
        }
    }

    /// Lowers a SELECT: FROM first, then WHERE (splitting `column = :param`
    /// equalities off into a binding join before filtering on the rest),
    /// then the projection, finally re-appending any binding columns the
    /// projection dropped (the runtime needs them for routing).
    fn lower_select(&mut self, select: &Select) -> Result<LogicalPlan, PlannerError> {
        if select.quantifier != SelectQuantifier::All {
            return Err(PlannerError::UnsupportedQuery(
                "SELECT DISTINCT lowering is not implemented yet",
            ));
        }
        if !select.group_by.is_empty()
            || select.having.is_some()
            || !select.order_by.is_empty()
            || select.limit.is_some()
            || select.offset.is_some()
        {
            return Err(PlannerError::UnsupportedQuery(
                "GROUP BY, HAVING, ORDER BY, LIMIT, and OFFSET are not implemented yet",
            ));
        }

        let mut input = self.lower_from(&select.from)?;
        let mut binding_source_fields = Vec::<(String, LogicalField)>::new();
        if let Some(predicate) = &select.selection {
            let (binding_predicates, residual) =
                self.extract_binding_predicates(predicate, input.fields())?;
            if !binding_predicates.is_empty() {
                binding_source_fields = binding_predicates
                    .iter()
                    .map(|predicate| (predicate.parameter.clone(), predicate.field.clone()))
                    .collect();
                input = place_binding_join(input, binding_predicates)?;
            }
            if let Some(residual) = residual {
                let predicate = self
                    .lower_predicate(&residual, input.fields())?
                    .canonicalize();
                input = LogicalPlan::Filter {
                    fields: input.fields().to_vec(),
                    input: Box::new(input),
                    predicate,
                };
            }
        }
        let binding_fields = input
            .fields()
            .iter()
            .filter(|field| field.qualifier.as_deref() == Some(BINDING_QUALIFIER))
            .cloned()
            .collect::<Vec<_>>();
        let mut projected = self.lower_projection(input, &select.projection)?;
        if !binding_fields.is_empty() {
            projected =
                append_missing_binding_fields(projected, binding_fields, &binding_source_fields)?;
        }
        Ok(projected)
    }

    /// Lowers the FROM clause; exactly one table reference (which may be a
    /// join tree) is supported today.
    fn lower_from(&mut self, from: &[TableRef]) -> Result<LogicalPlan, PlannerError> {
        let mut refs = from.iter();
        let first = refs
            .next()
            .ok_or(PlannerError::UnsupportedQuery("SELECT without FROM"))?;
        let plan = self.lower_table_ref(first)?;
        if refs.next().is_some() {
            return Err(PlannerError::UnsupportedQuery(
                "implicit joins need an ON clause for now",
            ));
        }
        Ok(plan)
    }

    /// Lowers one FROM entry: a named table (CTE names shadow schema
    /// tables), or a join tree. Derived tables are not lowerable yet.
    fn lower_table_ref(&mut self, table_ref: &TableRef) -> Result<LogicalPlan, PlannerError> {
        match table_ref {
            TableRef::Named { name, alias } => {
                let table_name = single_name(&name.0)?;
                if let Some(cte) = self.ctes.get(table_name) {
                    return Ok(cte.clone());
                }
                let table = self
                    .schema
                    .table(table_name)
                    .ok_or_else(|| PlannerError::TableNotFound(table_name.to_owned()))?;
                Ok(scan_plan(
                    table,
                    alias.as_ref().map(|alias| alias.name.clone()),
                ))
            }
            TableRef::Join {
                left,
                right,
                kind,
                constraint,
            } => self.lower_join(left, right, kind, constraint),
            TableRef::Derived { .. } => Err(PlannerError::UnsupportedQuery(
                "derived table lowering is not implemented yet",
            )),
        }
    }

    /// Lowers one join: inner joins with an `ON` clause of AND-ed column
    /// equalities only. The output fields are both sides' fields with
    /// `left.` / `right.` source prefixes (see [`join_fields`]).
    fn lower_join(
        &mut self,
        left: &TableRef,
        right: &TableRef,
        kind: &JoinKind,
        constraint: &JoinConstraint,
    ) -> Result<LogicalPlan, PlannerError> {
        if kind != &JoinKind::Inner {
            return Err(PlannerError::UnsupportedQuery(
                "only inner equi-join lowering is implemented",
            ));
        }
        let left = self.lower_table_ref(left)?;
        let right = self.lower_table_ref(right)?;
        let JoinConstraint::On(predicate) = constraint else {
            return Err(PlannerError::UnsupportedQuery(
                "only ON join constraints are implemented",
            ));
        };
        let (left_on, right_on) = lower_join_keys(predicate, left.fields(), right.fields())?;
        let fields = join_fields(left.fields(), right.fields());
        Ok(LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            left_on,
            right_on,
            fields,
        })
    }

    /// Lowers the SELECT list into a Project node: wildcards expand to the
    /// input's fields, qualified wildcards to one qualifier's fields, and
    /// plain column items resolve (and optionally alias) one field each.
    fn lower_projection(
        &self,
        input: LogicalPlan,
        projection: &[SelectItem],
    ) -> Result<LogicalPlan, PlannerError> {
        let fields = if projection.is_empty() {
            input.fields().to_vec()
        } else {
            let mut fields = Vec::new();
            for item in projection {
                match item {
                    SelectItem::Wildcard => fields.extend(input.fields().iter().cloned()),
                    SelectItem::QualifiedWildcard(qualifier) => {
                        let qualifier = qualifier.join(".");
                        let qualified_fields = input
                            .fields()
                            .iter()
                            .filter(|field| field.qualifier.as_deref() == Some(&qualifier))
                            .cloned()
                            .collect::<Vec<_>>();
                        if qualified_fields.is_empty() {
                            return Err(PlannerError::ColumnNotFound(format!("{qualifier}.*")));
                        }
                        fields.extend(qualified_fields);
                    }
                    SelectItem::Expr { expr, alias } => {
                        let field = self.resolve_projected_field(input.fields(), expr, alias)?;
                        fields.push(field);
                    }
                }
            }
            fields
        };
        Ok(LogicalPlan::Project {
            input: Box::new(input),
            fields,
        })
    }

    /// Resolves one projected column against the input fields, applying the
    /// `AS alias` (an aliased field loses its qualifier: it is a new name).
    fn resolve_projected_field(
        &self,
        input_fields: &[LogicalField],
        expr: &Expr,
        alias: &Option<String>,
    ) -> Result<LogicalField, PlannerError> {
        let Expr::Column(column) = expr else {
            return Err(PlannerError::UnsupportedExpression(
                "only column projection is currently lowerable",
            ));
        };
        let mut field = resolve_column(input_fields, &column.qualifier, &column.name)?.clone();
        if let Some(alias) = alias {
            field.name = alias.clone();
            field.qualifier = None;
        }
        Ok(field)
    }

    /// Lowers a WHERE expression to a [`PredicateExpr`]: AND/OR recurse,
    /// comparisons go through [`Self::lower_comparison`], and `IS [NOT]
    /// NULL` through [`Self::lower_unary_predicate`].
    fn lower_predicate(
        &self,
        expr: &Expr,
        fields: &[LogicalField],
    ) -> Result<PredicateExpr, PlannerError> {
        match expr {
            Expr::Binary {
                left,
                op: BinaryOp::And,
                right,
            } => Ok(PredicateExpr::And(vec![
                self.lower_predicate(left, fields)?,
                self.lower_predicate(right, fields)?,
            ])),
            Expr::Binary {
                left,
                op: BinaryOp::Or,
                right,
            } => Ok(PredicateExpr::Or(vec![
                self.lower_predicate(left, fields)?,
                self.lower_predicate(right, fields)?,
            ])),
            Expr::Binary { left, op, right } => self.lower_comparison(left, op, right, fields),
            Expr::Unary { op, expr } => self.lower_unary_predicate(op, expr, fields),
            _ => Err(PlannerError::UnsupportedExpression(
                "only binary and IS NULL predicates are currently lowerable",
            )),
        }
    }

    /// Lowers `column IS NULL` / `column IS NOT NULL`.
    fn lower_unary_predicate(
        &self,
        op: &UnaryOp,
        expr: &Expr,
        fields: &[LogicalField],
    ) -> Result<PredicateExpr, PlannerError> {
        let Expr::Column(column) = expr else {
            return Err(PlannerError::UnsupportedExpression(
                "IS NULL predicates require a column operand",
            ));
        };
        let field = resolve_column(fields, &column.qualifier, &column.name)?;
        match op {
            UnaryOp::IsNull => Ok(PredicateExpr::IsNull {
                field: field.name.clone(),
            }),
            UnaryOp::IsNotNull => Ok(PredicateExpr::IsNotNull {
                field: field.name.clone(),
            }),
            _ => Err(PlannerError::UnsupportedExpression(
                "only IS NULL and IS NOT NULL unary predicates are lowerable",
            )),
        }
    }

    /// Lowers one comparison into field-vs-literal form, type-checking the
    /// operands. A literal-on-the-left comparison is flipped
    /// ([`PredicateKind::reversed`]) so the field always comes first.
    fn lower_comparison(
        &self,
        left: &Expr,
        op: &BinaryOp,
        right: &Expr,
        fields: &[LogicalField],
    ) -> Result<PredicateExpr, PlannerError> {
        let kind = match op {
            BinaryOp::Eq => PredicateKind::Eq,
            BinaryOp::NotEq => PredicateKind::Neq,
            BinaryOp::Gt => PredicateKind::Gt,
            BinaryOp::GtEq => PredicateKind::GtEq,
            BinaryOp::Lt => PredicateKind::Lt,
            BinaryOp::LtEq => PredicateKind::LtEq,
            _ => {
                return Err(PlannerError::UnsupportedExpression(
                    "only comparison predicates are currently lowerable",
                ));
            }
        };
        let left = self.lower_predicate_operand(left, fields)?;
        let right = self.lower_predicate_operand(right, fields)?;
        type_check_comparison(&left, &right)?;

        match (left, right) {
            (PredicateOperand::Field(field), PredicateOperand::Literal(value)) => {
                let value = normalize_literal_for_field(value, &field.value_type)?;
                reject_null_literal(&value)?;
                Ok(PredicateExpr::from_field_literal(kind, field.name, value))
            }
            (PredicateOperand::Literal(value), PredicateOperand::Field(field)) => {
                let value = normalize_literal_for_field(value, &field.value_type)?;
                reject_null_literal(&value)?;
                Ok(PredicateExpr::from_field_literal(
                    kind.reversed(),
                    field.name,
                    value,
                ))
            }
            _ => Err(PlannerError::UnsupportedExpression(
                "only field-literal predicates are executable for now",
            )),
        }
    }

    /// Classifies one comparison operand as a resolved field or a literal.
    fn lower_predicate_operand(
        &self,
        expr: &Expr,
        fields: &[LogicalField],
    ) -> Result<PredicateOperand, PlannerError> {
        match expr {
            Expr::Column(column) => {
                let field = resolve_column(fields, &column.qualifier, &column.name)?;
                Ok(PredicateOperand::Field(field.clone()))
            }
            Expr::Literal(value) => Ok(PredicateOperand::Literal(value.clone().into())),
            Expr::Null => Ok(PredicateOperand::Literal(LiteralValue::Nullable(None))),
            Expr::Parameter(_) => Err(PlannerError::UnsupportedExpression(
                "only equality parameter predicates are supported",
            )),
            _ => Err(PlannerError::UnsupportedExpression(
                "only column and literal scalar expressions are currently lowerable",
            )),
        }
    }

    /// Splits a WHERE expression into its `column = :param` conjuncts (which
    /// become binding joins) and the residual expression (which stays an
    /// ordinary filter). Only top-level AND chains are split; a parameter
    /// anywhere else is rejected by the binding lowering below.
    fn extract_binding_predicates(
        &self,
        expr: &Expr,
        fields: &[LogicalField],
    ) -> Result<(Vec<BindingPredicate>, Option<Expr>), PlannerError> {
        if let Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } = expr
        {
            let (mut left_params, left_residual) = self.extract_binding_predicates(left, fields)?;
            let (right_params, right_residual) = self.extract_binding_predicates(right, fields)?;
            left_params.extend(right_params);
            let residual = match (left_residual, right_residual) {
                (Some(left), Some(right)) => Some(Expr::binary(left, BinaryOp::And, right)),
                (Some(expr), None) | (None, Some(expr)) => Some(expr),
                (None, None) => None,
            };
            return Ok((left_params, residual));
        }

        if contains_bindingeter(expr) {
            let param = self.lower_binding_predicate(expr, fields)?;
            Ok((vec![param], None))
        } else {
            Ok((Vec::new(), Some(expr.clone())))
        }
    }

    /// Lowers one parameter conjunct; only `column = :param` (either side)
    /// is supported.
    fn lower_binding_predicate(
        &self,
        expr: &Expr,
        fields: &[LogicalField],
    ) -> Result<BindingPredicate, PlannerError> {
        let Expr::Binary {
            left,
            op: BinaryOp::Eq,
            right,
        } = expr
        else {
            return Err(PlannerError::UnsupportedExpression(
                "only equality parameter predicates are supported",
            ));
        };
        match (left.as_ref(), right.as_ref()) {
            (Expr::Column(column), Expr::Parameter(parameter))
            | (Expr::Parameter(parameter), Expr::Column(column)) => {
                let field = resolve_column(fields, &column.qualifier, &column.name)?;
                Ok(BindingPredicate {
                    field: field.clone(),
                    parameter: parameter.clone(),
                })
            }
            _ => Err(PlannerError::UnsupportedExpression(
                "only column = parameter predicates are supported",
            )),
        }
    }
}

/// Internal planning result before the parameter-free/prepared split:
/// the planned query plus whatever parameters were found (empty for plain
/// queries) and the derived shape name (empty when parameter-free).
struct PreparedPlan {
    planned: PlannedQuery,
    parameters: Vec<QueryParameter>,
    shape: String,
}

/// One extracted `column = :parameter` conjunct.
#[derive(Clone, Debug)]
struct BindingPredicate {
    /// The resolved column the parameter is compared against.
    field: LogicalField,
    /// The parameter's name.
    parameter: String,
}

/// Qualifier marking internal binding-relation columns, so they can be told
/// apart from user columns and hidden from the public output.
const BINDING_QUALIFIER: &str = "__bindings";

/// Builds the executable graph for a plan, with no field pruning at the
/// root (the query's full output is wanted).
fn graph_from_logical(plan: &LogicalPlan) -> Result<GraphBuilder, PlannerError> {
    graph_from_logical_required(plan, None)
}

/// Builds the executable graph for a plan.
///
/// * `plan` — the logical plan to translate.
/// * `required` — when set, the fields the *parent* actually needs; the
///   subtree then projects down to exactly those. This is how projections
///   are pushed below joins instead of materializing full-width rows.
fn graph_from_logical_required(
    plan: &LogicalPlan,
    required: Option<&[LogicalField]>,
) -> Result<GraphBuilder, PlannerError> {
    match plan {
        LogicalPlan::Scan { table, fields, .. } => {
            let graph = GraphBuilder::table(table.clone());
            project_required_graph(graph, fields, required)
        }
        LogicalPlan::BindingRelation { shape, fields } => Ok(GraphBuilder::binding_source(
            shape.clone(),
            crate::records::RecordDescriptor::new(
                fields
                    .iter()
                    .map(|field| (field.name.clone(), field.value_type.clone())),
            ),
        )),
        LogicalPlan::Filter {
            input,
            predicate,
            fields,
        } => {
            let graph = graph_from_logical_required(input, None)?.filter(predicate.clone());
            project_required_graph(graph, fields, required)
        }
        LogicalPlan::Project { input, fields } if fields == input.fields() => {
            graph_from_logical_required(input, required)
        }
        LogicalPlan::Project { input, fields } => graph_from_logical_required(input, Some(fields)),
        LogicalPlan::Join {
            left,
            right,
            left_on,
            right_on,
            ..
        } => {
            let required = required.unwrap_or_else(|| plan.fields());
            let left_required =
                join_child_required_fields(left.fields(), left_on, required, "left")?;
            let right_required =
                join_child_required_fields(right.fields(), right_on, required, "right")?;
            let left_graph = graph_from_logical_required(left, Some(&left_required))?;
            let right_graph = graph_from_logical_required(right, Some(&right_required))?;
            let left_keys = left_on
                .iter()
                .map(|key| field_name_for_source(&left_required, key))
                .collect::<Result<Vec<_>, _>>()?;
            let right_keys = right_on
                .iter()
                .map(|key| field_name_for_source(&right_required, key))
                .collect::<Result<Vec<_>, _>>()?;
            let join = GraphBuilder::join(left_graph, right_graph, left_keys, right_keys);
            project_join_required_graph(join, &left_required, &right_required, required)
        }
        LogicalPlan::UnionAll { inputs, .. } => inputs
            .iter()
            .map(graph_from_logical)
            .collect::<Result<Vec<_>, _>>()
            .map(GraphBuilder::union),
    }
}

/// Projects a join's `left.<field>` / `right.<field>` output down to the
/// parent's required fields, translating each required field through the
/// (already pruned) child field lists.
fn project_join_required_graph(
    join: GraphBuilder,
    left_required: &[LogicalField],
    right_required: &[LogicalField],
    required: &[LogicalField],
) -> Result<GraphBuilder, PlannerError> {
    Ok(join.project_fields(required.iter().map(|field| {
        if let Some(source_name) = field.source_name.strip_prefix("left.") {
            let child = field_for_source(left_required, source_name)
                .expect("required left field was selected from child");
            ProjectField::renamed(format!("left.{}", child.name), field.name.clone())
        } else if let Some(source_name) = field.source_name.strip_prefix("right.") {
            let child = field_for_source(right_required, source_name)
                .expect("required right field was selected from child");
            ProjectField::renamed(format!("right.{}", child.name), field.name.clone())
        } else {
            ProjectField::renamed(field.source_name.clone(), field.name.clone())
        }
    })))
}

/// Appends a Project node when the parent needs fewer/renamed fields;
/// passes the graph through untouched when it already matches.
fn project_required_graph(
    graph: GraphBuilder,
    available: &[LogicalField],
    required: Option<&[LogicalField]>,
) -> Result<GraphBuilder, PlannerError> {
    let Some(required) = required else {
        return Ok(graph);
    };
    if required == available {
        return Ok(graph);
    }
    Ok(graph.project_fields(
        required
            .iter()
            .map(|field| ProjectField::renamed(field.source_name.clone(), field.name.clone())),
    ))
}

/// Computes which of a join child's fields are actually needed: the child's
/// join keys plus every parent-required field whose source is prefixed with
/// this child's side (`"left"` or `"right"`), deduplicated.
fn join_child_required_fields(
    child_fields: &[LogicalField],
    join_keys: &[String],
    parent_required: &[LogicalField],
    side: &str,
) -> Result<Vec<LogicalField>, PlannerError> {
    let mut fields = Vec::<LogicalField>::new();
    for key in join_keys {
        push_unique_field(&mut fields, field_for_source(child_fields, key)?.clone());
    }
    let prefix = format!("{side}.");
    for field in parent_required {
        if let Some(source_name) = field.source_name.strip_prefix(&prefix) {
            push_unique_field(
                &mut fields,
                field_for_source(child_fields, source_name)?.clone(),
            );
        }
    }
    Ok(fields)
}

/// Appends `field` unless a field with the same source is already present.
fn push_unique_field(fields: &mut Vec<LogicalField>, field: LogicalField) {
    if !fields
        .iter()
        .any(|candidate| candidate.source_name == field.source_name)
    {
        fields.push(field);
    }
}

/// Finds the field whose `source_name` is `source_name`.
fn field_for_source<'a>(
    fields: &'a [LogicalField],
    source_name: &str,
) -> Result<&'a LogicalField, PlannerError> {
    fields
        .iter()
        .find(|field| field.source_name == source_name)
        .ok_or_else(|| PlannerError::ColumnNotFound(source_name.to_owned()))
}

/// The output name of the field with the given `source_name`.
fn field_name_for_source(
    fields: &[LogicalField],
    source_name: &str,
) -> Result<String, PlannerError> {
    Ok(field_for_source(fields, source_name)?.name.clone())
}

/// Joins `input` against a fresh binding relation on the extracted
/// parameter equalities.
///
/// One binding column is created per distinct parameter (a parameter used
/// twice must see the same column type both times). The relation's shape
/// name is filled in later by [`rewrite_param_shape`], once the whole plan
/// is known and the name can be derived from it.
fn add_binding_join(
    input: LogicalPlan,
    predicates: Vec<BindingPredicate>,
) -> Result<LogicalPlan, PlannerError> {
    let mut param_types = BTreeMap::<String, ValueType>::new();
    for predicate in &predicates {
        if let Some(existing) = param_types.get(&predicate.parameter) {
            if existing != &predicate.field.value_type {
                return Err(PlannerError::TypeMismatch {
                    left: existing.clone(),
                    right: predicate.field.value_type.clone(),
                });
            }
        } else {
            param_types.insert(
                predicate.parameter.clone(),
                predicate.field.value_type.clone(),
            );
        }
    }
    let binding_fields = param_types
        .into_iter()
        .map(|(name, value_type)| LogicalField {
            qualifier: Some(BINDING_QUALIFIER.to_owned()),
            source_name: name.clone(),
            name,
            value_type,
        })
        .collect::<Vec<_>>();
    let left_on = predicates
        .iter()
        .map(|predicate| predicate.field.source_name.clone())
        .collect::<Vec<_>>();
    let right_on = predicates
        .iter()
        .map(|predicate| predicate.parameter.clone())
        .collect::<Vec<_>>();
    let param_relation = LogicalPlan::BindingRelation {
        shape: String::new(),
        fields: binding_fields,
    };
    let fields = join_fields(input.fields(), param_relation.fields());
    Ok(LogicalPlan::Join {
        left: Box::new(input),
        right: Box::new(param_relation),
        left_on,
        right_on,
        fields,
    })
}

/// Decides where the binding join goes. When every parameter constrains
/// columns of one table, the join is pushed down next to that table's scan
/// — the parameter then narrows rows *before* any other join instead of
/// filtering the joined result. Otherwise it wraps the whole input.
fn place_binding_join(
    input: LogicalPlan,
    predicates: Vec<BindingPredicate>,
) -> Result<LogicalPlan, PlannerError> {
    let qualifier = predicates
        .iter()
        .map(|predicate| predicate.field.qualifier.as_deref())
        .collect::<std::collections::BTreeSet<_>>();
    if qualifier.len() == 1
        && let Some(Some(qualifier)) = qualifier.first().copied()
        && let Some(pushed) = push_param_join_into_qualifier(input.clone(), &predicates, qualifier)?
    {
        return Ok(pushed);
    }
    add_binding_join(input, predicates)
}

/// Recursively descends to the scan owning `qualifier` and wraps *it* in
/// the binding join, re-deriving the field lists (and join keys) on the way
/// back up. Returns `None` when no rewrite applies (the caller then joins
/// at the top instead).
fn push_param_join_into_qualifier(
    input: LogicalPlan,
    predicates: &[BindingPredicate],
    qualifier: &str,
) -> Result<Option<LogicalPlan>, PlannerError> {
    match input {
        LogicalPlan::Scan {
            table,
            alias,
            fields,
        } if fields
            .iter()
            .any(|field| field.qualifier.as_deref() == Some(qualifier)) =>
        {
            let local_predicates = predicates
                .iter()
                .map(|predicate| {
                    let field =
                        resolve_field_by_output_identity(&fields, &predicate.field)?.clone();
                    Ok(BindingPredicate {
                        field,
                        parameter: predicate.parameter.clone(),
                    })
                })
                .collect::<Result<Vec<_>, PlannerError>>()?;
            Ok(Some(add_binding_join(
                LogicalPlan::Scan {
                    table,
                    alias,
                    fields,
                },
                local_predicates,
            )?))
        }
        LogicalPlan::Join {
            left,
            right,
            left_on,
            right_on,
            ..
        } => {
            let left = *left;
            let right = *right;
            if plan_has_qualifier(&left, qualifier)
                && let Some(new_left) =
                    push_param_join_into_qualifier(left.clone(), predicates, qualifier)?
            {
                let new_left_on = remap_join_keys(left.fields(), new_left.fields(), &left_on)?;
                let fields = join_fields(new_left.fields(), right.fields());
                return Ok(Some(LogicalPlan::Join {
                    left: Box::new(new_left),
                    right: Box::new(right),
                    left_on: new_left_on,
                    right_on,
                    fields,
                }));
            }
            if plan_has_qualifier(&right, qualifier)
                && let Some(new_right) =
                    push_param_join_into_qualifier(right.clone(), predicates, qualifier)?
            {
                let new_right_on = remap_join_keys(right.fields(), new_right.fields(), &right_on)?;
                let fields = join_fields(left.fields(), new_right.fields());
                return Ok(Some(LogicalPlan::Join {
                    left: Box::new(left),
                    right: Box::new(new_right),
                    left_on,
                    right_on: new_right_on,
                    fields,
                }));
            }
            Ok(None)
        }
        LogicalPlan::Filter {
            input,
            predicate,
            fields: _,
        } => {
            let old_input = *input;
            if let Some(new_input) =
                push_param_join_into_qualifier(old_input.clone(), predicates, qualifier)?
            {
                Ok(Some(LogicalPlan::Filter {
                    fields: new_input.fields().to_vec(),
                    input: Box::new(new_input),
                    predicate,
                }))
            } else {
                Ok(None)
            }
        }
        LogicalPlan::Project { .. }
        | LogicalPlan::UnionAll { .. }
        | LogicalPlan::BindingRelation { .. }
        | LogicalPlan::Scan { .. } => Ok(None),
    }
}

/// `true` when any of the plan's output fields belongs to `qualifier`.
fn plan_has_qualifier(plan: &LogicalPlan, qualifier: &str) -> bool {
    plan.fields()
        .iter()
        .any(|field| field.qualifier.as_deref() == Some(qualifier))
}

/// Finds the field with the same `(qualifier, name)` as `target` — the
/// stable identity that survives re-deriving field lists during rewrites.
fn resolve_field_by_output_identity<'a>(
    fields: &'a [LogicalField],
    target: &LogicalField,
) -> Result<&'a LogicalField, PlannerError> {
    fields
        .iter()
        .find(|field| field.qualifier == target.qualifier && field.name == target.name)
        .ok_or_else(|| PlannerError::ColumnNotFound(target.name.clone()))
}

/// Translates join-key source names from a child's old field list to its
/// rewritten one (the binding-join pushdown prefixes sources, so keys must
/// follow).
fn remap_join_keys(
    old_fields: &[LogicalField],
    new_fields: &[LogicalField],
    keys: &[String],
) -> Result<Vec<String>, PlannerError> {
    keys.iter()
        .map(|key| {
            let old_field = old_fields
                .iter()
                .find(|field| &field.source_name == key)
                .ok_or_else(|| PlannerError::ColumnNotFound(key.clone()))?;
            let new_field = resolve_field_by_output_identity(new_fields, old_field)?;
            Ok(new_field.source_name.clone())
        })
        .collect()
}

/// Re-appends binding columns the user's projection dropped — the runtime
/// needs them in the output to route rows to their binding. A user column
/// may share a binding column's name only when it *is* that parameter's
/// source column; anything else is an ambiguous collision and is rejected.
fn append_missing_binding_fields(
    input: LogicalPlan,
    binding_fields: Vec<LogicalField>,
    binding_source_fields: &[(String, LogicalField)],
) -> Result<LogicalPlan, PlannerError> {
    let (input, mut fields) = match input {
        LogicalPlan::Project { input, fields } => (*input, fields),
        input => {
            let fields = input.fields().to_vec();
            (input, fields)
        }
    };
    for field in binding_fields {
        if let Some(existing) = fields.iter().find(|candidate| candidate.name == field.name) {
            let Some((_, source)) = binding_source_fields
                .iter()
                .find(|(parameter, _)| parameter == &field.name)
            else {
                return Err(PlannerError::UnsupportedQuery(
                    "projected output names must not collide with parameter names",
                ));
            };
            if existing.qualifier != source.qualifier || existing.name != source.name {
                return Err(PlannerError::UnsupportedQuery(
                    "projected output names must not collide with parameter names",
                ));
            }
        } else {
            fields.push(field);
        }
    }
    Ok(LogicalPlan::Project {
        input: Box::new(input),
        fields,
    })
}

/// Collects every parameter appearing in the plan's binding relations,
/// deduplicated and sorted by name.
fn collect_binding_fields(plan: &LogicalPlan) -> Vec<QueryParameter> {
    let mut parameters = BTreeMap::<String, ValueType>::new();
    collect_binding_fields_inner(plan, &mut parameters);
    parameters
        .into_iter()
        .map(|(name, value_type)| QueryParameter { name, value_type })
        .collect()
}

fn collect_binding_fields_inner(plan: &LogicalPlan, parameters: &mut BTreeMap<String, ValueType>) {
    match plan {
        LogicalPlan::BindingRelation { fields, .. } => {
            for field in fields {
                parameters.insert(field.name.clone(), field.value_type.clone());
            }
        }
        LogicalPlan::Filter { input, .. } | LogicalPlan::Project { input, .. } => {
            collect_binding_fields_inner(input, parameters);
        }
        LogicalPlan::Join { left, right, .. } => {
            collect_binding_fields_inner(left, parameters);
            collect_binding_fields_inner(right, parameters);
        }
        LogicalPlan::UnionAll { inputs, .. } => {
            for input in inputs {
                collect_binding_fields_inner(input, parameters);
            }
        }
        LogicalPlan::Scan { .. } => {}
    }
}

/// Stamps the derived shape name into every binding relation of the plan
/// (they are created with an empty name before the name can be computed).
fn rewrite_param_shape(plan: &mut LogicalPlan, shape: &str) {
    match plan {
        LogicalPlan::BindingRelation {
            shape: param_shape, ..
        } => *param_shape = shape.to_owned(),
        LogicalPlan::Filter { input, .. } | LogicalPlan::Project { input, .. } => {
            rewrite_param_shape(input, shape);
        }
        LogicalPlan::Join { left, right, .. } => {
            rewrite_param_shape(left, shape);
            rewrite_param_shape(right, shape);
        }
        LogicalPlan::UnionAll { inputs, .. } => {
            for input in inputs {
                rewrite_param_shape(input, shape);
            }
        }
        LogicalPlan::Scan { .. } => {}
    }
}

/// Derives the shape's name by hashing the plan (with shape names blanked,
/// to avoid circularity) plus its parameters — so two identical
/// parameterized queries share one shape and one maintained graph.
fn prepared_shape_name(plan: &LogicalPlan, parameters: &[QueryParameter]) -> String {
    let mut plan = plan.clone();
    rewrite_param_shape(&mut plan, "");
    let mut hasher = DefaultHasher::new();
    // Prepared shape names are in-memory only. A collision would share compute
    // between unrelated shapes, so durable identities must use a stronger key.
    plan.hash(&mut hasher);
    parameters.hash(&mut hasher);
    format!("prepared_{:016x}", hasher.finish())
}

/// `true` when the expression contains a `:parameter` anywhere reachable
/// (subqueries are deliberately not descended into).
fn contains_bindingeter(expr: &Expr) -> bool {
    match expr {
        Expr::Parameter(_) => true,
        Expr::Unary { expr, .. } => contains_bindingeter(expr),
        Expr::Binary { left, right, .. } => {
            contains_bindingeter(left) || contains_bindingeter(right)
        }
        Expr::Between {
            expr, low, high, ..
        } => contains_bindingeter(expr) || contains_bindingeter(low) || contains_bindingeter(high),
        Expr::InList { expr, list, .. } => {
            contains_bindingeter(expr) || list.iter().any(contains_bindingeter)
        }
        Expr::InSubquery { expr, .. } => contains_bindingeter(expr),
        Expr::Exists { .. } => false,
        Expr::Function(function) => function.args.iter().any(function_arg_contains_bindingeter),
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand.as_deref().is_some_and(contains_bindingeter)
                || when_then
                    .iter()
                    .any(|(when, then)| contains_bindingeter(when) || contains_bindingeter(then))
                || else_expr.as_deref().is_some_and(contains_bindingeter)
        }
        Expr::Cast { expr, .. } => contains_bindingeter(expr),
        Expr::Subquery(_) | Expr::CorrelatedSubquery(_) => false,
        Expr::Literal(_) | Expr::Null | Expr::Column(_) => false,
    }
}

fn function_arg_contains_bindingeter(arg: &crate::queries::FunctionArg) -> bool {
    match arg {
        crate::queries::FunctionArg::Expr(expr) => contains_bindingeter(expr),
        crate::queries::FunctionArg::Wildcard => false,
    }
}

/// Builds the Scan plan for a table: one field per column, qualified by the
/// alias when given, otherwise by the table name.
fn scan_plan(table: &TableSchema, alias: Option<String>) -> LogicalPlan {
    let qualifier = alias.clone().or_else(|| Some(table.name.clone()));
    let fields = table
        .columns
        .iter()
        .map(|column| LogicalField {
            qualifier: qualifier.clone(),
            name: column.name.clone(),
            source_name: column.name.clone(),
            value_type: column.column_type.value_type(),
        })
        .collect();
    LogicalPlan::Scan {
        table: table.name.clone(),
        alias,
        fields,
    }
}

/// Resolves a (possibly qualified) column name against the fields in scope.
/// Exactly one field may match: none is [`PlannerError::ColumnNotFound`],
/// several are [`PlannerError::AmbiguousColumn`].
fn resolve_column<'a>(
    fields: &'a [LogicalField],
    qualifier: &[String],
    name: &str,
) -> Result<&'a LogicalField, PlannerError> {
    let qualifier = (!qualifier.is_empty()).then(|| qualifier.join("."));
    let matches = fields
        .iter()
        .filter(|field| {
            field.name == name
                && qualifier
                    .as_ref()
                    .is_none_or(|qualifier| field.qualifier.as_deref() == Some(qualifier))
        })
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [field] => Ok(field),
        [] => Err(PlannerError::ColumnNotFound(name.to_owned())),
        _ => Err(PlannerError::AmbiguousColumn(name.to_owned())),
    }
}

/// One side of a comparison after classification.
#[derive(Clone, Debug)]
enum PredicateOperand {
    Field(LogicalField),
    Literal(LiteralValue),
}

impl PredicateOperand {
    /// The operand's type: always known for fields, best-effort for
    /// literals (a bare NULL has none).
    fn value_type(&self) -> Option<ValueType> {
        match self {
            Self::Field(field) => Some(field.value_type.clone()),
            Self::Literal(value) => value.value_type(),
        }
    }
}

/// Rejects `field = NULL`-style comparisons: SQL says they are never true,
/// and the executable predicates have no way to express that, so they are
/// refused instead of silently mis-evaluated. Use `IS NULL`.
fn reject_null_literal(value: &LiteralValue) -> Result<(), PlannerError> {
    if matches!(value, LiteralValue::Nullable(None)) {
        return Err(PlannerError::UnsupportedExpression(
            "NULL literal predicates are not executable yet",
        ));
    }
    Ok(())
}

/// Rejects comparisons between incompatible types when both sides' types
/// are known (an untyped side, like a bare NULL, passes here and is handled
/// elsewhere).
fn type_check_comparison(
    left: &PredicateOperand,
    right: &PredicateOperand,
) -> Result<(), PlannerError> {
    let left_type = left.value_type();
    let right_type = right.value_type();
    match (left_type, right_type) {
        (Some(left_type), Some(right_type)) if !comparable_value_types(&left_type, &right_type) => {
            Err(PlannerError::TypeMismatch {
                left: left_type,
                right: right_type,
            })
        }
        _ => Ok(()),
    }
}

/// Adapts a literal to the field it is compared with. Today that means one
/// thing: a string literal compared to an enum column is converted to the
/// variant's discriminant (so `color = 'green'` becomes `color = Enum(1)`).
fn normalize_literal_for_field(
    value: LiteralValue,
    field_type: &ValueType,
) -> Result<LiteralValue, PlannerError> {
    match (value, field_type) {
        (LiteralValue::String(variant), ValueType::Enum(schema)) => schema
            .discriminant(&variant)
            .map(LiteralValue::Enum)
            .map_err(|_| PlannerError::TypeMismatch {
                left: field_type.clone(),
                right: ValueType::String,
            }),
        (LiteralValue::String(variant), ValueType::Nullable(inner))
            if matches!(inner.as_ref(), ValueType::Enum(_)) =>
        {
            let ValueType::Enum(schema) = inner.as_ref() else {
                unreachable!();
            };
            schema
                .discriminant(&variant)
                .map(LiteralValue::Enum)
                .map_err(|_| PlannerError::TypeMismatch {
                    left: field_type.clone(),
                    right: ValueType::String,
                })
        }
        (value, _) => Ok(value),
    }
}

/// Whether two types may be compared: equal types, either side's nullable
/// wrapper ignored, and the enum-vs-string pairing (normalized later by
/// [`normalize_literal_for_field`]).
fn comparable_value_types(left: &ValueType, right: &ValueType) -> bool {
    if left == right {
        return true;
    }
    let left_unwrapped = match left {
        ValueType::Nullable(inner) => inner.as_ref(),
        value_type => value_type,
    };
    let right_unwrapped = match right {
        ValueType::Nullable(inner) => inner.as_ref(),
        value_type => value_type,
    };
    if matches!(
        (left_unwrapped, right_unwrapped),
        (ValueType::Enum(_), ValueType::String) | (ValueType::String, ValueType::Enum(_))
    ) {
        return true;
    }
    match (left, right) {
        (ValueType::Nullable(left), right) => left.as_ref() == right,
        (left, ValueType::Nullable(right)) => left == right.as_ref(),
        _ => false,
    }
}

/// Lowers an `ON` clause of AND-ed column equalities into parallel key
/// lists. Each equality's columns are resolved left-side/right-side; when
/// that fails, the swapped assignment is tried (`ON artists.id =
/// albums.artist_id` works too). Key types must match exactly.
fn lower_join_keys(
    predicate: &Expr,
    left_fields: &[LogicalField],
    right_fields: &[LogicalField],
) -> Result<(Vec<String>, Vec<String>), PlannerError> {
    if let Expr::Binary {
        left,
        op: BinaryOp::And,
        right,
    } = predicate
    {
        let (mut left_on, mut right_on) = lower_join_keys(left, left_fields, right_fields)?;
        let (next_left_on, next_right_on) = lower_join_keys(right, left_fields, right_fields)?;
        left_on.extend(next_left_on);
        right_on.extend(next_right_on);
        return Ok((left_on, right_on));
    }

    let Expr::Binary {
        left,
        op: BinaryOp::Eq,
        right,
    } = predicate
    else {
        return Err(PlannerError::UnsupportedExpression(
            "only equality join predicates are implemented",
        ));
    };
    let Expr::Column(left_column) = left.as_ref() else {
        return Err(PlannerError::UnsupportedExpression(
            "join keys must be columns",
        ));
    };
    let Expr::Column(right_column) = right.as_ref() else {
        return Err(PlannerError::UnsupportedExpression(
            "join keys must be columns",
        ));
    };

    let left_match = resolve_column(left_fields, &left_column.qualifier, &left_column.name);
    let right_match = resolve_column(right_fields, &right_column.qualifier, &right_column.name);
    if let (Ok(left_field), Ok(right_field)) = (left_match, right_match) {
        if left_field.value_type != right_field.value_type {
            return Err(PlannerError::TypeMismatch {
                left: left_field.value_type.clone(),
                right: right_field.value_type.clone(),
            });
        }
        return Ok((
            vec![left_field.source_name.clone()],
            vec![right_field.source_name.clone()],
        ));
    }

    let left_field = resolve_column(left_fields, &right_column.qualifier, &right_column.name)?;
    let right_field = resolve_column(right_fields, &left_column.qualifier, &left_column.name)?;
    if left_field.value_type != right_field.value_type {
        return Err(PlannerError::TypeMismatch {
            left: left_field.value_type.clone(),
            right: right_field.value_type.clone(),
        });
    }
    Ok((
        vec![left_field.source_name.clone()],
        vec![right_field.source_name.clone()],
    ))
}

/// A join's output field list: both sides' fields with their SQL names and
/// qualifiers kept, and their source names prefixed `left.` / `right.` to
/// address the joined record's columns.
fn join_fields(left: &[LogicalField], right: &[LogicalField]) -> Vec<LogicalField> {
    left.iter()
        .map(|field| LogicalField {
            qualifier: field.qualifier.clone(),
            name: field.name.clone(),
            source_name: format!("left.{}", field.source_name),
            value_type: field.value_type.clone(),
        })
        .chain(right.iter().map(|field| LogicalField {
            qualifier: field.qualifier.clone(),
            name: field.name.clone(),
            source_name: format!("right.{}", field.source_name),
            value_type: field.value_type.clone(),
        }))
        .collect()
}

/// Whether two field lists are `UNION ALL`-compatible: same length, same
/// types position by position (names may differ; the left side's win).
fn comparable_fields(left: &[LogicalField], right: &[LogicalField]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.value_type == right.value_type)
}

/// Unwraps a one-part object name; qualified names (`db.table`) are not
/// supported yet.
fn single_name(name: &[String]) -> Result<&str, PlannerError> {
    match name {
        [name] => Ok(name),
        _ => Err(PlannerError::UnsupportedQuery(
            "only single-part object names are implemented",
        )),
    }
}

/// Ways planning can fail. The `Unsupported*` variants carry a static
/// message naming the exact SQL shape that is not lowerable yet.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum PlannerError {
    #[error("ambiguous column: {0}")]
    AmbiguousColumn(String),
    #[error("column not found: {0}")]
    ColumnNotFound(String),
    #[error("query outputs do not match")]
    OutputMismatch,
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("type mismatch: {left:?} vs {right:?}")]
    TypeMismatch { left: ValueType, right: ValueType },
    #[error("{0}")]
    UnsupportedExpression(&'static str),
    #[error("{0}")]
    UnsupportedQuery(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queries::{
        ColumnRef, Cte, ObjectName, OrderByExpr, SetQuery, TableAlias, WithQuery,
    };
    use crate::records::Value;
    use crate::schema::{ColumnSchema, ColumnType};

    fn schema() -> DatabaseSchema {
        DatabaseSchema::new([
            TableSchema::new(
                "albums",
                [
                    ColumnSchema::new("id", ColumnType::U64),
                    ColumnSchema::new("artist_id", ColumnType::U64),
                    ColumnSchema::new("title", ColumnType::String),
                ],
            ),
            TableSchema::new(
                "artists",
                [
                    ColumnSchema::new("id", ColumnType::U64),
                    ColumnSchema::new("name", ColumnType::String),
                ],
            ),
        ])
    }

    #[test]
    fn lowers_select_filter_project_to_logical_plan_and_graph() {
        let query = Query::Select(Box::new(
            Select::new([SelectItem::expr(Expr::column("title"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    Expr::column("id"),
                    BinaryOp::Gt,
                    Expr::Literal(Value::U64(10)),
                )),
        ));

        let planned = plan_query(&query, &schema()).unwrap();

        assert_eq!(
            planned.graph,
            GraphBuilder::table("albums")
                .filter(PredicateExpr::gt("id", Value::U64(10)))
                .project(["title"])
        );
        assert!(matches!(planned.logical, LogicalPlan::Project { .. }));
    }

    #[test]
    fn canonicalizes_and_predicates_for_stable_graphs() {
        let a = Expr::binary(
            Expr::column("id"),
            BinaryOp::Gt,
            Expr::Literal(Value::U64(10)),
        );
        let b = Expr::binary(
            Expr::column("title"),
            BinaryOp::Eq,
            Expr::Literal(Value::String("Kind of Blue".to_owned())),
        );
        let query_ab = Query::Select(Box::new(
            Select::new([SelectItem::Wildcard])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(a.clone(), BinaryOp::And, b.clone())),
        ));
        let query_ba = Query::Select(Box::new(
            Select::new([SelectItem::Wildcard])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(b, BinaryOp::And, a)),
        ));

        assert_eq!(
            plan_query(&query_ab, &schema()).unwrap().graph,
            plan_query(&query_ba, &schema()).unwrap().graph
        );
    }

    #[test]
    fn predicate_or_canonicalizes_and_lowers_to_graph_filter() {
        let a = Expr::binary(
            Expr::column("id"),
            BinaryOp::Gt,
            Expr::Literal(Value::U64(10)),
        );
        let b = Expr::binary(
            Expr::column("title"),
            BinaryOp::Eq,
            Expr::Literal(Value::String("Kind of Blue".to_owned())),
        );
        let query_ab = Query::Select(Box::new(
            Select::new([SelectItem::Wildcard])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(a.clone(), BinaryOp::Or, b.clone())),
        ));
        let query_ba = Query::Select(Box::new(
            Select::new([SelectItem::Wildcard])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(b, BinaryOp::Or, a)),
        ));

        assert_eq!(
            plan_query(&query_ab, &schema()).unwrap().graph,
            plan_query(&query_ba, &schema()).unwrap().graph
        );
    }

    #[test]
    fn resolves_qualified_join_keys_and_lowers_inner_join() {
        let query = Query::Select(Box::new(Select::new([SelectItem::Wildcard]).from([
            TableRef::Join {
                left: Box::new(TableRef::Named {
                    name: ObjectName::single("albums"),
                    alias: Some(TableAlias::new("a")),
                }),
                right: Box::new(TableRef::Named {
                    name: ObjectName::single("artists"),
                    alias: Some(TableAlias::new("r")),
                }),
                kind: JoinKind::Inner,
                constraint: JoinConstraint::On(Expr::binary(
                    Expr::Column(ColumnRef::qualified(["a"], "artist_id")),
                    BinaryOp::Eq,
                    Expr::Column(ColumnRef::qualified(["r"], "id")),
                )),
            },
        ])));

        let planned = plan_query(&query, &schema()).unwrap();

        assert_eq!(
            planned.graph,
            GraphBuilder::join(
                GraphBuilder::table("albums").project(["artist_id", "id", "title"]),
                GraphBuilder::table("artists"),
                ["artist_id"],
                ["id"]
            )
            .project_fields([
                ProjectField::renamed("left.id", "id"),
                ProjectField::renamed("left.artist_id", "artist_id"),
                ProjectField::renamed("left.title", "title"),
                ProjectField::renamed("right.id", "id"),
                ProjectField::renamed("right.name", "name"),
            ])
        );
    }

    #[test]
    fn lowers_union_all() {
        let left = Query::Select(Box::new(
            Select::new([SelectItem::expr(Expr::column("id"))]).from([TableRef::named("albums")]),
        ));
        let right = Query::Select(Box::new(
            Select::new([SelectItem::expr(Expr::column("id"))]).from([TableRef::named("artists")]),
        ));
        let query = Query::Set(Box::new(SetQuery {
            left,
            op: SetOperator::Union,
            right,
            quantifier: SetQuantifier::All,
        }));

        assert_eq!(
            plan_query(&query, &schema()).unwrap().graph,
            GraphBuilder::union([
                GraphBuilder::table("albums").project(["id"]),
                GraphBuilder::table("artists").project(["id"])
            ])
        );
    }

    #[test]
    fn lowers_simple_ctes_and_restores_scope_after_planning() {
        let cte = Cte::new(
            "album_ids",
            Query::Select(Box::new(
                Select::new([SelectItem::expr(Expr::column("id"))])
                    .from([TableRef::named("albums")]),
            )),
        );
        let query = Query::With(Box::new(WithQuery::new(
            [cte],
            Query::Select(Box::new(
                Select::new([SelectItem::expr(Expr::column("id"))])
                    .from([TableRef::named("album_ids")]),
            )),
        )));
        let schema = schema();
        let mut planner = Planner::new(&schema);

        let planned = planner.plan_query(&query).unwrap();

        assert_eq!(
            planned.planned.graph,
            GraphBuilder::table("albums").project(["id"])
        );
        let outside_cte = Query::Select(Box::new(
            Select::new([SelectItem::expr(Expr::column("id"))])
                .from([TableRef::named("album_ids")]),
        ));
        assert!(matches!(
            planner.plan_query(&outside_cte),
            Err(PlannerError::TableNotFound(table)) if table == "album_ids"
        ));
    }

    #[test]
    fn rejects_recursive_ctes_until_recursive_lowering_exists() {
        let cte = Cte::new(
            "album_ids",
            Query::Select(Box::new(
                Select::new([SelectItem::expr(Expr::column("id"))])
                    .from([TableRef::named("albums")]),
            )),
        );
        let query = Query::With(Box::new(
            WithQuery::new(
                [cte],
                Query::Select(Box::new(
                    Select::new([SelectItem::expr(Expr::column("id"))])
                        .from([TableRef::named("album_ids")]),
                )),
            )
            .recursive(),
        ));

        assert!(matches!(
            plan_query(&query, &schema()),
            Err(PlannerError::UnsupportedQuery(
                "recursive CTE lowering is not implemented yet"
            ))
        ));
    }

    #[test]
    fn rejects_ambiguous_unqualified_columns_after_join() {
        let query = Query::Select(Box::new(
            Select::new([SelectItem::expr(Expr::column("id"))]).from([TableRef::Join {
                left: Box::new(TableRef::named("albums").aliased("a")),
                right: Box::new(TableRef::named("artists").aliased("r")),
                kind: JoinKind::Inner,
                constraint: JoinConstraint::On(Expr::binary(
                    Expr::Column(ColumnRef::qualified(["a"], "artist_id")),
                    BinaryOp::Eq,
                    Expr::Column(ColumnRef::qualified(["r"], "id")),
                )),
            }]),
        ));

        assert!(matches!(
            plan_query(&query, &schema()),
            Err(PlannerError::AmbiguousColumn(column)) if column == "id"
        ));
    }

    #[test]
    fn rejects_known_but_unsupported_select_shapes() {
        let distinct = Query::Select(Box::new(
            Select::new([SelectItem::expr(Expr::column("id"))])
                .from([TableRef::named("albums")])
                .distinct(),
        ));
        assert!(matches!(
            plan_query(&distinct, &schema()),
            Err(PlannerError::UnsupportedQuery(
                "SELECT DISTINCT lowering is not implemented yet"
            ))
        ));

        let ordered = Query::Select(Box::new(
            Select::new([SelectItem::expr(Expr::column("id"))])
                .from([TableRef::named("albums")])
                .order_by([OrderByExpr::asc(Expr::column("title"))]),
        ));
        assert!(matches!(
            plan_query(&ordered, &schema()),
            Err(PlannerError::UnsupportedQuery(
                "GROUP BY, HAVING, ORDER BY, LIMIT, and OFFSET are not implemented yet"
            ))
        ));
    }

    #[test]
    fn rejects_non_inner_joins_and_non_union_all_sets() {
        let left_join = Query::Select(Box::new(Select::new([SelectItem::Wildcard]).from([
            TableRef::Join {
                left: Box::new(TableRef::named("albums")),
                right: Box::new(TableRef::named("artists")),
                kind: JoinKind::Left,
                constraint: JoinConstraint::On(Expr::binary(
                    Expr::Column(ColumnRef::qualified(["albums"], "artist_id")),
                    BinaryOp::Eq,
                    Expr::Column(ColumnRef::qualified(["artists"], "id")),
                )),
            },
        ])));
        assert!(matches!(
            plan_query(&left_join, &schema()),
            Err(PlannerError::UnsupportedQuery(
                "only inner equi-join lowering is implemented"
            ))
        ));

        let except = Query::Set(Box::new(SetQuery {
            left: Query::Select(Box::new(
                Select::new([SelectItem::expr(Expr::column("id"))])
                    .from([TableRef::named("albums")]),
            )),
            op: SetOperator::Except,
            right: Query::Select(Box::new(
                Select::new([SelectItem::expr(Expr::column("id"))])
                    .from([TableRef::named("artists")]),
            )),
            quantifier: SetQuantifier::All,
        }));
        assert!(matches!(
            plan_query(&except, &schema()),
            Err(PlannerError::UnsupportedQuery(
                "only UNION ALL set queries are currently lowerable"
            ))
        ));
    }

    #[test]
    fn rejects_unknown_columns_and_type_mismatches() {
        let unknown = Query::Select(Box::new(
            Select::new([SelectItem::expr(Expr::column("missing"))])
                .from([TableRef::named("albums")]),
        ));
        assert!(matches!(
            plan_query(&unknown, &schema()),
            Err(PlannerError::ColumnNotFound(column)) if column == "missing"
        ));

        let mismatch = Query::Select(Box::new(
            Select::new([SelectItem::Wildcard])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    Expr::column("id"),
                    BinaryOp::Eq,
                    Expr::Literal(Value::String("nope".to_owned())),
                )),
        ));
        assert!(matches!(
            plan_query(&mismatch, &schema()),
            Err(PlannerError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn prepares_parameter_equality_as_param_relation_join() {
        let query = Query::Select(Box::new(
            Select::new([SelectItem::expr(Expr::column("id"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    Expr::column("artist_id"),
                    BinaryOp::Eq,
                    Expr::parameter("artist"),
                )),
        ));
        let planned = plan_prepared_shape(&query, &schema()).unwrap();

        assert_eq!(
            planned.parameters,
            vec![QueryParameter {
                name: "artist".to_owned(),
                value_type: ValueType::U64
            }]
        );
        assert_eq!(planned.output_key_fields, vec!["artist"]);
        assert!(matches!(
            plan_query(&query, &schema()),
            Err(PlannerError::UnsupportedQuery(
                "query parameters require prepare_query"
            ))
        ));
    }

    #[test]
    fn rejects_non_equality_parameter_predicates() {
        let query = Query::Select(Box::new(
            Select::new([SelectItem::expr(Expr::column("id"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    Expr::column("artist_id"),
                    BinaryOp::Gt,
                    Expr::parameter("artist"),
                )),
        ));

        assert!(matches!(
            plan_prepared_shape(&query, &schema()),
            Err(PlannerError::UnsupportedExpression(
                "only equality parameter predicates are supported"
            ))
        ));
    }

    #[test]
    fn pushes_parameter_relation_to_constrained_scan_before_joining() {
        let query = Query::Select(Box::new(
            Select::new([
                SelectItem::expr(Expr::Column(ColumnRef::qualified(["albums"], "id"))),
                SelectItem::expr(Expr::Column(ColumnRef::qualified(["artists"], "name"))),
            ])
            .from([TableRef::Join {
                left: Box::new(TableRef::named("albums")),
                right: Box::new(TableRef::named("artists")),
                kind: JoinKind::Inner,
                constraint: JoinConstraint::On(Expr::binary(
                    Expr::Column(ColumnRef::qualified(["albums"], "artist_id")),
                    BinaryOp::Eq,
                    Expr::Column(ColumnRef::qualified(["artists"], "id")),
                )),
            }])
            .where_(Expr::binary(
                Expr::Column(ColumnRef::qualified(["albums"], "artist_id")),
                BinaryOp::Eq,
                Expr::parameter("artist"),
            )),
        ));
        let planned = plan_prepared_shape(&query, &schema()).unwrap();
        let LogicalPlan::Project { input, .. } = planned.planned.logical else {
            panic!("expected final project")
        };
        let LogicalPlan::Join { left, .. } = *input else {
            panic!("expected outer join")
        };
        let LogicalPlan::Join { right, .. } = *left else {
            panic!("expected params joined into constrained scan")
        };
        assert!(matches!(*right, LogicalPlan::BindingRelation { .. }));
    }

    #[test]
    fn rejects_projection_aliases_that_collide_with_parameter_names() {
        let query = Query::Select(Box::new(
            Select::new([SelectItem::aliased(Expr::column("id"), "artist")])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    Expr::column("artist_id"),
                    BinaryOp::Eq,
                    Expr::parameter("artist"),
                )),
        ));

        assert!(matches!(
            plan_prepared_shape(&query, &schema()),
            Err(PlannerError::UnsupportedQuery(
                "projected output names must not collide with parameter names"
            ))
        ));
    }
}
