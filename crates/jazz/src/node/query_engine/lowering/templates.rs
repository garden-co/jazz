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
    #[cfg(any(test, feature = "testing"))]
    pub(crate) hits: usize,
}

#[derive(Clone, Debug)]
struct TemplateEntry {
    request: QueryProgramRequest,
    sources: ResolvedQuerySources,
    source_parameters: BTreeMap<u32, ParameterDomain>,
    program: Option<Arc<QueryProgram>>,
}

impl QueryProgramTemplateCache {
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }

    pub(crate) fn lower(
        &mut self,
        compilation: QueryProgramCompilation,
        sources: ResolvedQuerySources,
        explain: ExplainPlan,
        describe: impl Fn(GraphBuilder) -> Result<groove::ivm::TemplateGraphInput, groove::db::Error>,
    ) -> QueryCompileResult {
        // Private inline programs still bake literals into their shape. Until
        // scalar slots exist, preparing a fresh template for each such shape
        // adds work without enabling cross-binding reuse. Keep their ordinary
        // compiler path, rather than charging all receivers for this boundary.
        if compilation.request.input.binding.source_shape.is_none() {
            return lower_resolved_query_program(compilation, sources, explain);
        }
        // Prepared programs may omit values only if unbound lowering succeeds.
        let mut template_compilation = compilation.clone();
        template_compilation.request.input.binding.values.clear();
        template_compilation.request.input.binding.id = BindingId(uuid::Uuid::nil());
        let mut template_sources = sources.clone();
        let mut inputs = Vec::new();
        let mut source_parameters = BTreeMap::new();
        let mut make_slot = |graph: &mut GraphBuilder| -> Result<(), groove::db::Error> {
            let mut parameters = ParameterDomain::default();
            collect_binding_source_params(graph, &mut parameters);
            let input = describe(graph.clone())?;
            let output = input.descriptor();
            source_parameters.insert(inputs.len() as u32, parameters);
            let slot = GraphBuilder::TemplateInput {
                slot: inputs.len() as u32,
                output,
                input: None,
            };
            *graph = slot;
            inputs.push(input);
            Ok(())
        };
        for source in template_sources.values_mut() {
            let result = (|| {
                make_slot(&mut source.graph)?;
                if let Some(content) = &mut source.content_version {
                    make_slot(&mut content.graph)?;
                }
                if let Some(deletion) = &mut source.deletion_register {
                    make_slot(&mut deletion.graph)?;
                }
                if let Some(preimage) = &mut source.authorized_deletion_preimage {
                    make_slot(&mut preimage.graph)?;
                }
                Ok::<_, groove::db::Error>(())
            })();
            if result.is_err() {
                trace_template("descriptor_fallback");
                // Source preparation may use a not-yet-installed binding
                // descriptor. The ordinary compiler retains its admission path.
                return lower_resolved_query_program(compilation, sources, explain);
            }
        }
        // Compare typed contracts, not recursively formatted descriptors or
        // per-operator structural hashes. The cheap normalized shape id rejects
        // unrelated entries before exact request and source-contract equality.
        let template = if let Some(entry) = self.entries.iter().find(|entry| {
            entry.request.input.shape.identity.shape_id
                == template_compilation.request.input.shape.identity.shape_id
                && entry.request == template_compilation.request
                && entry.sources == template_sources
                && entry.source_parameters == source_parameters
        }) {
            let Some(template) = &entry.program else {
                trace_template("unsupported_hit");
                return lower_resolved_query_program(compilation, sources, explain);
            };
            #[cfg(any(test, feature = "testing"))]
            {
                self.hits += 1;
            }
            template.clone()
        } else {
            let template_request = template_compilation.request.clone();
            let template = lower_resolved_query_program_with_source_parameters(
                template_compilation,
                template_sources.clone(),
                ExplainPlan::default(),
                &source_parameters,
            )
            .ok()
            .map(|mut program| {
                let mut graphs = program
                    .lowered
                    .terminals
                    .iter()
                    .map(|terminal| terminal.graph.clone())
                    .collect::<Vec<_>>();
                graphs.extend(program.lowered.internal_app_rows_graph.iter().cloned());
                if let Ok(typed) = groove::ivm::compile_template_graphs(&graphs) {
                    let mut typed = typed.into_iter();
                    for terminal in &mut program.lowered.terminals {
                        terminal.graph = typed.next().expect("typed terminal");
                    }
                    if program.lowered.internal_app_rows_graph.is_some() {
                        program.lowered.internal_app_rows_graph = typed.next();
                    }
                    trace_template("typed_compiled");
                }
                Arc::new(program)
            });
            if self.entries.len() == 64 {
                self.entries.pop_front();
            }
            self.entries.push_back(TemplateEntry {
                request: template_request,
                sources: template_sources,
                source_parameters,
                program: template.clone(),
            });
            trace_template(if template.is_some() {
                "compiled"
            } else {
                "unsupported"
            });
            let Some(template) = template else {
                // Some projections and predicates still require a concrete
                // scalar. Never pretend such a product is binding-independent.
                return lower_resolved_query_program(compilation, sources, explain);
            };
            template
        };
        trace_template("bound");
        let mut program = (*template).clone();
        let mut graphs = program
            .lowered
            .terminals
            .iter()
            .map(|terminal| terminal.graph.clone())
            .collect::<Vec<_>>();
        graphs.extend(program.lowered.internal_app_rows_graph.iter().cloned());
        let bound = groove::ivm::bind_template_graphs(&graphs, &inputs)
            .expect("template inputs and slot descriptors are constructed together");
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
