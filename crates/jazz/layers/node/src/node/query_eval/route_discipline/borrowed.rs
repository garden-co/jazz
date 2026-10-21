//! A proof view over immutable declarative topology. A template invocation
//! gets its own source-slot context; identical template pointers in different
//! invocations must never alias a proof node. No operator graph is reconstructed.

use super::{RouteGraph, RouteViolation, children};
use groove::db::Database;
use groove::ivm::GraphBuilder;
use groove::ivm::runtime::IvmRuntimeError;
use groove::records::RecordDescriptor;
use std::cell::RefCell;
use std::collections::HashMap;

type Key = (usize, usize);

struct Node<'g> {
    operator: &'g GraphBuilder,
    children: Vec<(usize, usize)>,
}

pub(super) struct View<'db, 'g> {
    database: &'db Database,
    outputs: RefCell<Vec<Option<RecordDescriptor>>>,
    pub(super) has_lookups: bool,
    nodes: Vec<Node<'g>>,
    memo: HashMap<Key, usize>,
    contexts: Vec<Vec<usize>>,
}

enum Work<'g> {
    Visit(&'g GraphBuilder, usize),
    Finish(&'g GraphBuilder, usize),
    Template(&'g GraphBuilder, usize),
    Alias(Key, Key),
}

fn key(node: &GraphBuilder, context: usize) -> Key {
    (std::ptr::from_ref(node) as usize, context)
}

impl<'db, 'g> View<'db, 'g> {
    pub(super) fn new(database: &'db Database) -> Self {
        Self {
            database,
            outputs: RefCell::new(Vec::new()),
            has_lookups: false,
            nodes: Vec::new(),
            memo: HashMap::new(),
            contexts: vec![Vec::new()],
        }
    }

    pub(super) fn build(&mut self, root: &'g GraphBuilder) -> Result<usize, RouteViolation> {
        let mut pending = vec![Work::Visit(root, 0)];
        while let Some(work) = pending.pop() {
            match work {
                Work::Alias(original, replacement) => {
                    self.memo.insert(original, self.memo[&replacement]);
                }
                Work::Visit(node, context) => {
                    if self.memo.contains_key(&key(node, context)) {
                        continue;
                    }
                    match node {
                        GraphBuilder::RecursiveStepWitness { .. } => {
                            return Err(RouteViolation(
                                "recursive witness needs expanded analysis".into(),
                            ));
                        }
                        GraphBuilder::TemplateInput {
                            input: Some(input), ..
                        } => {
                            pending.push(Work::Alias(key(node, context), key(input, context)));
                            pending.push(Work::Visit(input, context));
                        }
                        GraphBuilder::TemplateInput {
                            slot, input: None, ..
                        } => {
                            let input = self.contexts[context]
                                .get(*slot as usize)
                                .copied()
                                .ok_or_else(|| RouteViolation("unbound template input".into()))?;
                            self.memo.insert(key(node, context), input);
                        }
                        GraphBuilder::TypedTemplate { .. } => {
                            pending.push(Work::Template(node, context));
                            pending.extend(children(node).map(|child| Work::Visit(child, context)));
                        }
                        _ => {
                            pending.push(Work::Finish(node, context));
                            pending.extend(children(node).map(|child| Work::Visit(child, context)));
                        }
                    }
                }
                Work::Template(node, context) => {
                    let GraphBuilder::TypedTemplate {
                        program,
                        inputs,
                        predicates,
                        scalars,
                    } = node
                    else {
                        unreachable!()
                    };
                    let fallback = program
                        .declarative_analysis_shape(inputs, predicates, scalars)
                        .map_err(|error| {
                            RouteViolation(format!("invalid template arguments: {error:?}"))
                        })?;
                    let bound_inputs = inputs
                        .iter()
                        .map(|input| self.memo[&key(input, context)])
                        .collect();
                    let nested = self.contexts.len();
                    self.contexts.push(bound_inputs);
                    pending.push(Work::Alias(key(node, context), key(fallback, nested)));
                    pending.push(Work::Visit(fallback, nested));
                }
                Work::Finish(node, context) => {
                    let children = children(node)
                        .map(|child| (key(child, context).0, self.memo[&key(child, context)]))
                        .collect::<Vec<_>>();
                    let id = self.nodes.len();
                    self.nodes.push(Node {
                        operator: node,
                        children,
                    });
                    self.outputs.get_mut().push(None);
                    self.has_lookups |= matches!(node, GraphBuilder::Lookup { .. });
                    self.memo.insert(key(node, context), id);
                }
            }
        }
        Ok(self.memo[&key(root, 0)])
    }
}

impl<'g> RouteGraph<'g> for View<'_, 'g> {
    type Node = usize;
    fn operator(&self, node: usize) -> &'g GraphBuilder {
        self.nodes[node].operator
    }
    fn child(&self, node: usize, child: &'g GraphBuilder) -> usize {
        self.nodes[node]
            .children
            .iter()
            .find(|(pointer, _)| *pointer == key(child, 0).0)
            .expect("operator input")
            .1
    }
    fn key(&self, node: usize) -> usize {
        node
    }
    fn output(&self, node: usize) -> Result<RecordDescriptor, RouteViolation> {
        let mut outputs = self.outputs.borrow_mut();
        let mut pending = vec![(node, false)];
        while let Some((id, expanded)) = pending.pop() {
            if outputs[id].is_some() {
                continue;
            }
            let node = &self.nodes[id];
            if !expanded {
                pending.push((id, true));
                pending.extend(node.children.iter().map(|(_, child)| (*child, false)));
                continue;
            }
            let output = self
                .database
                .graph_operator_output_descriptor(node.operator, |child| {
                    node.children
                        .iter()
                        .find(|(pointer, _)| *pointer == key(child, 0).0)
                        .and_then(|(_, id)| outputs[*id])
                        .ok_or(IvmRuntimeError::UnsupportedOperator)
                })
                .map_err(|error| {
                    RouteViolation(format!("cannot describe borrowed operator: {error:?}"))
                })?;
            outputs[id] = Some(output);
        }
        Ok(outputs[node].expect("inferred requested output"))
    }
    fn children(&self, node: usize) -> Vec<usize> {
        self.nodes[node]
            .children
            .iter()
            .map(|(_, id)| *id)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::query_eval::route_discipline::{
        check_route_discipline, check_routes, expand_templates,
    };
    use groove::ivm::{
        PredicateExpr, ProjectField, TemplateGraphInput, bind_template_graphs,
        compile_template_graphs,
    };
    use groove::records::{Value, ValueType};
    use groove::schema::DatabaseSchema;
    use groove::storage::MemoryStorage;
    use std::collections::{BTreeMap, BTreeSet};

    /// Alice's binding input and Bob's constant input instantiate the same
    /// compiled template but have different route dependencies. Internal
    /// analysis is necessary: result parity alone cannot detect a proof memo
    /// that accidentally treats another binding's graph as independent.
    #[test]
    fn template_invocations_keep_distinct_borrowed_proof_contexts() {
        futures::executor::block_on(async {
            let db = Database::new(DatabaseSchema::new([]), MemoryStorage::new(&[]).unwrap())
                .await
                .unwrap();
            let output = RecordDescriptor::new([("id", ValueType::String)]);
            let template = GraphBuilder::TemplateInput {
                slot: 0,
                output,
                input: None,
            }
            .filter(PredicateExpr::eq("id", Value::String("alice".into())))
            .project_fields([ProjectField::named("id")]);
            let compiled = compile_template_graphs(&[template]).unwrap();
            let bind = |source| {
                bind_template_graphs(
                    &compiled,
                    &[TemplateGraphInput::with_output_contract(source, output)],
                )
                .unwrap()
                .remove(0)
            };
            let alice = bind(GraphBuilder::binding_source("alice", output));
            let bob =
                bind(GraphBuilder::values(output, [vec![Value::String("bob".into())]]).unwrap());
            let slots = BTreeMap::from([("id".into(), "__jazz_route_id".into())]);
            let mut view = View::new(&db);
            for (name, graph, accepted) in [("bob", &bob, true), ("alice", &alice, false)] {
                let root = view.build(graph).unwrap();
                let borrowed = check_routes([(name, root, BTreeSet::new())], &slots, &view);
                let expanded = expand_templates(graph);
                let reference =
                    check_route_discipline([(name, &expanded, BTreeSet::new())], &slots, |graph| {
                        db.graph_output_descriptor(graph)
                    });
                assert_eq!(borrowed.is_ok(), accepted, "{name}: {borrowed:?}");
                assert_eq!(borrowed.is_ok(), reference.is_ok());
            }
        });
    }
}
