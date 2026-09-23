//! Source preparation is per-instance; pure relational lowering is per family.
use super::*;
use std::collections::VecDeque;
use std::sync::Arc;

fn trace_template(event: &str) {
    #[cfg(any(test, feature = "testing"))]
    if std::env::var_os("JAZZ_QUERY_TEMPLATE_TRACE").is_some() {
        eprintln!("JAZZ_QUERY_TEMPLATE_TRACE {event}");
    }
    let _ = event;
}

/// Only immutable, unbound descriptions live here. In particular, this cache
/// never owns a receiver InputSource, an index probe, or authorized rows.
#[derive(Clone, Debug, Default)]
pub(crate) struct QueryProgramTemplateCache {
    entries: VecDeque<TemplateEntry>,
    physical: groove::ivm::TypedGraphTemplateCache,
    #[cfg(any(test, feature = "testing"))]
    pub(crate) hits: usize,
    #[cfg(any(test, feature = "testing"))]
    pub(crate) argument_hits: usize,
}

#[derive(Clone, Debug)]
struct TemplateEntry {
    request: QueryProgramRequest,
    literal_types: Vec<Option<ValueType>>,
    sources: ResolvedQuerySources,
    program: TemplateState,
}

/// A family is promoted on its second sighting. The first request takes the
/// ordinary concrete path, so single-use queries never pay for building a
/// reusable program next to the one they install.
#[derive(Clone, Debug)]
enum TemplateState {
    Seen,
    Unsupported,
    Ready(Arc<ParameterizedProgram>),
}

#[derive(Clone, Debug)]
struct ParameterizedProgram {
    program: QueryProgram,
    arguments: ProgramArgumentRecipes,
}

impl QueryProgramTemplateCache {
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.physical.clear();
    }

    fn typed_program(&mut self, mut program: QueryProgram) -> QueryProgram {
        let mut graphs = program
            .lowered
            .terminals
            .iter()
            .map(|terminal| terminal.graph.clone())
            .collect::<Vec<_>>();
        graphs.extend(program.lowered.internal_app_rows_graph.iter().cloned());
        if let Ok(typed) = self.physical.compile(&graphs) {
            let mut typed = typed.into_iter();
            for terminal in &mut program.lowered.terminals {
                terminal.graph = typed.next().expect("typed terminal");
            }
            if program.lowered.internal_app_rows_graph.is_some() {
                program.lowered.internal_app_rows_graph = typed.next();
            }
            trace_template("typed_bound");
            #[cfg(any(test, feature = "testing"))]
            if std::env::var_os("JAZZ_QUERY_TEMPLATE_TRACE").is_some() {
                let (compiled, reused) = self.physical.counters();
                eprintln!("JAZZ_QUERY_TEMPLATE_TRACE physical compiled={compiled} reused={reused}");
            }
        }
        program
    }

    pub(crate) fn lower(
        &mut self,
        compilation: QueryProgramCompilation,
        sources: ResolvedQuerySources,
        explain: ExplainPlan,
        describe: impl Fn(GraphBuilder) -> Result<groove::ivm::TemplateGraphInput, groove::db::Error>,
    ) -> QueryCompileResult {
        #[cfg(any(test, feature = "testing"))]
        if std::env::var_os("JAZZ_QUERY_TEMPLATE_TRACE").is_some() {
            eprintln!(
                "JAZZ_QUERY_TEMPLATE_REQUEST prepared={} values={}",
                compilation.request.input.binding.source_shape.is_some(),
                compilation.request.input.binding.values.len()
            );
        }
        // Admission/source preparation used the exact request. Pure lowering
        // has no access to values: every dynamic use must emit a bind-time
        // recipe, or fail closed to ordinary concrete lowering.
        let mut template_request = compilation.request.clone();
        let literals = match super::arguments::extract_template_literals(&mut template_request) {
            Ok(literals) => literals,
            Err(_) => return lower_resolved_query_program(compilation, sources, explain),
        };
        let literal_types = literals
            .iter()
            .map(|value| LiteralValue::from(value.clone()).value_type())
            .collect::<Vec<_>>();
        template_request.input.binding.values.clear();
        template_request.input.binding.id = BindingId(uuid::Uuid::nil());
        let mut template_sources = sources.clone();
        let mut source_graphs = Vec::new();
        for source in sources.values() {
            source_graphs.push(&source.graph);
            source_graphs.extend(source.content_version.iter().map(|s| &s.graph));
            source_graphs.extend(source.deletion_register.iter().map(|s| &s.graph));
            source_graphs.extend(source.authorized_deletion_preimage.iter().map(|s| &s.graph));
        }
        let Ok((blueprints, inputs)) =
            groove::ivm::split_template_sources(&source_graphs, describe)
        else {
            trace_template("descriptor_fallback");
            return lower_resolved_query_program(compilation, sources, explain);
        };
        let mut blueprints = blueprints.into_iter();
        for source in template_sources.values_mut() {
            source.graph = blueprints.next().expect("source blueprint");
            if let Some(content) = &mut source.content_version {
                content.graph = blueprints.next().expect("content blueprint");
            }
            if let Some(deletion) = &mut source.deletion_register {
                deletion.graph = blueprints.next().expect("deletion blueprint");
            }
            if let Some(preimage) = &mut source.authorized_deletion_preimage {
                preimage.graph = blueprints.next().expect("preimage blueprint");
            }
        }
        // Compare typed contracts, not recursively formatted descriptors or
        // per-operator structural hashes. The cheap normalized shape id rejects
        // unrelated entries before exact request and source-contract equality.
        let Some(index) = self.entries.iter().position(|entry| {
            entry.request.input.shape.identity.shape_id
                == template_request.input.shape.identity.shape_id
                && entry.request == template_request
                && entry.literal_types == literal_types
                && entry.sources == template_sources
        }) else {
            if self.entries.len() == 256 {
                self.entries.pop_front();
            }
            self.entries.push_back(TemplateEntry {
                request: template_request,
                literal_types,
                sources: template_sources,
                program: TemplateState::Seen,
            });
            trace_template("seen");
            return lower_resolved_query_program(compilation, sources, explain);
        };
        let template = match &self.entries[index].program {
            TemplateState::Ready(template) => template.clone(),
            TemplateState::Unsupported => {
                trace_template("unsupported_hit");
                return lower_resolved_query_program(compilation, sources, explain);
            }
            TemplateState::Seen => {
                let arguments = std::cell::RefCell::new(
                    ProgramArgumentRecipes::with_literal_types(literal_types),
                );
                let template = QueryProgramCompilation::analyze(template_request)
                    .and_then(|template_compilation| {
                        lower_resolved_query_program_with_source_parameters(
                            template_compilation,
                            template_sources,
                            ExplainPlan::default(),
                            &BTreeMap::new(),
                            Some(&arguments),
                        )
                    })
                    .ok()
                    .map(|program| {
                        Arc::new(ParameterizedProgram {
                            program: self.typed_program(program),
                            arguments: arguments.into_inner(),
                        })
                    });
                trace_template(if template.is_some() {
                    "compiled"
                } else {
                    "unsupported"
                });
                let Some(template) = template else {
                    // Some projections and predicates still require a concrete
                    // scalar. Never pretend such a product is binding-independent.
                    self.entries[index].program = TemplateState::Unsupported;
                    return lower_resolved_query_program(compilation, sources, explain);
                };
                self.entries[index].program = TemplateState::Ready(template.clone());
                template
            }
        };
        #[cfg(any(test, feature = "testing"))]
        {
            self.hits += 1;
            self.argument_hits += usize::from(!template.arguments.is_empty());
        }
        trace_template("bound");
        let mut program = template.program.clone();
        let mut graphs = program
            .lowered
            .terminals
            .iter()
            .map(|terminal| terminal.graph.clone())
            .collect::<Vec<_>>();
        graphs.extend(program.lowered.internal_app_rows_graph.iter().cloned());
        let bound = match template
            .arguments
            .bind(&graphs, &inputs, &compilation.request, &literals)
        {
            Ok(graphs) => graphs,
            Err(_) => {
                trace_template("argument_fallback");
                return lower_resolved_query_program(compilation, sources, explain);
            }
        };
        let mut bound = bound.into_iter();
        for terminal in &mut program.lowered.terminals {
            terminal.graph = bound.next().expect("terminal bound");
        }
        if program.lowered.internal_app_rows_graph.is_some() {
            program.lowered.internal_app_rows_graph = bound.next();
        }
        program.request = compilation.request;
        let capabilities = program.explain.capabilities.clone();
        program.explain = explain_with_request(&program.request, explain);
        program.explain.capabilities.extend(capabilities);
        Ok(program)
    }
}
