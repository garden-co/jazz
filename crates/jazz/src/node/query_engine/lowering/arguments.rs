//! Pure bind-time recipes, separate from logical/wire identity and authority.
use super::*;
use std::{cell::RefCell, ops::Deref, sync::Arc};

pub(super) struct LoweringContext<'a> {
    request: &'a QueryProgramRequest,
    arguments: Option<&'a RefCell<ProgramArgumentRecipes>>,
    literals: &'a [Value],
}

impl<'a> LoweringContext<'a> {
    pub(super) fn concrete(request: &'a QueryProgramRequest) -> Self {
        Self {
            request,
            arguments: None,
            literals: &[],
        }
    }

    pub(super) fn compiling(
        request: &'a QueryProgramRequest,
        arguments: &'a RefCell<ProgramArgumentRecipes>,
    ) -> Self {
        Self {
            request,
            arguments: Some(arguments),
            literals: &[],
        }
    }

    pub(super) fn predicate_argument(
        &self,
        predicate: &PredicateExpr,
        source_id: &SourceId,
        source: &ResolvedSource,
    ) -> Option<GroovePredicateExpr> {
        if !predicate_contains_argument(predicate) {
            return None;
        }
        let mut arguments = self.arguments?.borrow_mut();
        let slot = arguments.predicates.len() as u32;
        arguments.predicates.push(PredicateArgument {
            predicate: predicate.clone(),
            source_id: source_id.clone(),
            source: source.clone(),
        });
        Some(GroovePredicateExpr::TemplateArgument {
            slot,
            fields: source_fields(source).collect(),
        })
    }

    pub(super) fn literal(&self, slot: u32) -> Result<Value, UnsupportedReason> {
        self.literals.get(slot as usize).cloned().ok_or_else(|| {
            UnsupportedReason::Runtime(format!("compiler literal {slot} is not bound"))
        })
    }

    pub(super) fn literal_type(&self, slot: u32) -> Option<ValueType> {
        self.arguments?
            .borrow()
            .literal_types
            .get(slot as usize)?
            .clone()
    }

    pub(super) fn scalar_argument(
        &self,
        value: NormalizedValueRef,
        ty: ValueType,
        coerce: bool,
    ) -> Option<groove::ivm::ProjectExpr> {
        let mut arguments = self.arguments?.borrow_mut();
        // Multiple witnesses often project the identical route. One argument
        // recipe owns its validation/coercion, not each terminal occurrence.
        let recipe = ScalarArgument {
            value,
            ty: ty.clone(),
            coerce,
        };
        let slot = arguments
            .scalars
            .iter()
            .position(|existing| existing == &recipe)
            .unwrap_or_else(|| {
                let slot = arguments.scalars.len();
                arguments.scalars.push(recipe);
                slot
            });
        Some(groove::ivm::ProjectExpr::TemplateArgument {
            slot: slot as u32,
            value_type: ty,
        })
    }
}

impl Deref for LoweringContext<'_> {
    type Target = QueryProgramRequest;
    fn deref(&self) -> &Self::Target {
        self.request
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct ProgramArgumentRecipes {
    predicates: Vec<PredicateArgument>,
    scalars: Vec<ScalarArgument>,
    pub(super) literal_types: Vec<Option<ValueType>>,
}

#[derive(Clone, Debug)]
struct PredicateArgument {
    predicate: PredicateExpr,
    source_id: SourceId,
    // Only blueprint source metadata; never an admitted instance's live input.
    source: ResolvedSource,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ScalarArgument {
    value: NormalizedValueRef,
    ty: ValueType,
    coerce: bool,
}

impl ProgramArgumentRecipes {
    pub(super) fn with_literal_types(literal_types: Vec<Option<ValueType>>) -> Self {
        Self {
            literal_types,
            ..Self::default()
        }
    }
    #[cfg(any(test, feature = "testing"))]
    pub(super) fn is_empty(&self) -> bool {
        self.predicates.is_empty() && self.scalars.is_empty()
    }
    pub(super) fn bind(
        &self,
        graphs: &[GraphBuilder],
        inputs: &[groove::ivm::TemplateGraphInput],
        request: &QueryProgramRequest,
        literals: &[Value],
    ) -> Result<Vec<GraphBuilder>, UnsupportedReason> {
        let context = LoweringContext {
            request,
            arguments: None,
            literals,
        };
        let predicates = self
            .predicates
            .iter()
            .map(|recipe| {
                lower_predicate(
                    &recipe.predicate,
                    &recipe.source_id,
                    &recipe.source,
                    &context,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let scalars = self
            .scalars
            .iter()
            .map(|recipe| {
                let value = match &recipe.value {
                    NormalizedValueRef::TemplateLiteral(slot) => context.literal(*slot)?,
                    NormalizedValueRef::Param(name) => request
                        .input
                        .binding
                        .values
                        .get(name)
                        .cloned()
                        .ok_or_else(|| {
                            UnsupportedReason::Operator(format!(
                                "binding parameter '{name}' is not bound"
                            ))
                        })?,
                    NormalizedValueRef::Claim(path) => claim_value(path, &request.policy)?,
                    NormalizedValueRef::Literal(bytes) => postcard::from_bytes::<Value>(bytes)
                        .map_err(|err| {
                            UnsupportedReason::Operator(format!("literal decoding failed: {err}"))
                        })?,
                    _ => {
                        return Err(UnsupportedReason::Operator(
                            "template scalar must be a parameter, claim or literal".into(),
                        ));
                    }
                };
                let value = if recipe.coerce {
                    crate::node::query_eval::coerce_prepared_binding_value(value, &recipe.ty)
                } else {
                    value
                };
                groove::ivm::TemplateScalarArgument::new(value, recipe.ty.clone()).map_err(|err| {
                    UnsupportedReason::Operator(format!("template scalar binding failed: {err}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        groove::ivm::bind_template_program(graphs, inputs, &predicates, Arc::from(scalars)).map_err(
            |err| UnsupportedReason::Runtime(format!("template argument binding failed: {err}")),
        )
    }
}

fn predicate_contains_argument(predicate: &PredicateExpr) -> bool {
    let dynamic = |value: &NormalizedValueRef| {
        matches!(
            value,
            NormalizedValueRef::Param(_) | NormalizedValueRef::TemplateLiteral(_)
        )
    };
    match predicate {
        PredicateExpr::True | PredicateExpr::False => false,
        PredicateExpr::Compare { left, right, .. } => dynamic(left) || dynamic(right),
        PredicateExpr::In { value, options } => dynamic(value) || options.iter().any(dynamic),
        PredicateExpr::ArrayContains { value, needle }
        | PredicateExpr::TextContains { value, needle } => dynamic(value) || dynamic(needle),
        PredicateExpr::IsNull(value) | PredicateExpr::IsNotNull(value) => dynamic(value),
        PredicateExpr::EnumMatch { value, payload, .. } => {
            dynamic(value) || predicate_contains_argument(payload)
        }
        PredicateExpr::And(children) | PredicateExpr::Or(children) => {
            children.iter().any(predicate_contains_argument)
        }
        PredicateExpr::Not(child) => predicate_contains_argument(child),
    }
}

/// Lift ordinary literal operands into a *compiler-local* argument namespace.
/// Do not reuse named query parameters: that would introduce binding joins and
/// authorization route columns which the admitted request does not own.
pub(super) fn extract_template_literals(
    request: &mut QueryProgramRequest,
) -> Result<Vec<Value>, UnsupportedReason> {
    fn value(
        value: &mut NormalizedValueRef,
        literals: &mut Vec<Value>,
    ) -> Result<(), UnsupportedReason> {
        if let NormalizedValueRef::Literal(bytes) = value {
            let decoded = postcard::from_bytes(bytes).map_err(|err| {
                UnsupportedReason::Operator(format!("literal decoding failed: {err}"))
            })?;
            *value = NormalizedValueRef::TemplateLiteral(literals.len() as u32);
            literals.push(decoded);
        }
        Ok(())
    }
    fn predicate(
        expression: &mut PredicateExpr,
        literals: &mut Vec<Value>,
    ) -> Result<(), UnsupportedReason> {
        match expression {
            PredicateExpr::Compare { left, right, .. } => {
                value(left, literals)?;
                value(right, literals)?;
            }
            PredicateExpr::In {
                value: input,
                options,
            } => {
                value(input, literals)?;
                for option in options {
                    value(option, literals)?;
                }
            }
            PredicateExpr::ArrayContains {
                value: input,
                needle,
            }
            | PredicateExpr::TextContains {
                value: input,
                needle,
            } => {
                value(input, literals)?;
                value(needle, literals)?;
            }
            PredicateExpr::IsNull(input) | PredicateExpr::IsNotNull(input) => {
                value(input, literals)?
            }
            PredicateExpr::EnumMatch {
                value: input,
                payload,
                ..
            } => {
                value(input, literals)?;
                predicate(payload, literals)?;
            }
            PredicateExpr::And(children) | PredicateExpr::Or(children) => {
                for child in children {
                    predicate(child, literals)?;
                }
            }
            PredicateExpr::Not(child) => predicate(child, literals)?,
            PredicateExpr::True | PredicateExpr::False => {}
        }
        Ok(())
    }
    let mut literals = Vec::new();
    for node in request.input.shape.nodes.values_mut() {
        match node {
            RowSetExpr::Filter {
                predicate: filter, ..
            } => predicate(filter, &mut literals)?,
            RowSetExpr::Project { columns, .. } => {
                for column in columns {
                    value(&mut column.value, &mut literals)?;
                }
            }
            // Values in semantic planner markers, keys, and closure metadata
            // stay exact. They are not automatically scalar execution inputs.
            _ => {}
        }
    }
    if !literals.is_empty() {
        // Only this private compiler copy loses its public facade identity.
        // Exact structural equality and literal types still gate cache reuse;
        // the completed program retains the original admitted request.
        request.input.shape.identity.shape_id = ShapeId(uuid::Uuid::nil());
        request.input.shape.identity.canonical.clear();
    }
    Ok(literals)
}
