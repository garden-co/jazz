/* template.h: port of crates/groove/src/ivm/template.rs (+ template/arguments.rs,
 * template/sources.rs): immutable operator topology with typed instance-owned
 * inputs. */
#ifndef GROOVE_IVM_TEMPLATE_H
#define GROOVE_IVM_TEMPLATE_H
#include "graph.h"

enum {
    GROOVE_TEMPLATE_ERR_SCALAR_ARGUMENT = 0x0A40,
    GROOVE_TEMPLATE_ERR_PREDICATE_ARGUMENT,
    GROOVE_TEMPLATE_ERR_PREDICATE_ARITY,
    GROOVE_TEMPLATE_ERR_MISSING_INPUT,
    GROOVE_TEMPLATE_ERR_DESCRIPTOR_MISMATCH,
};
jz_err groove__ivm__template__TemplateBindingError__scalar_argument(u32 slot);
jz_err groove__ivm__template__TemplateBindingError__predicate_argument(u32 slot);
jz_err groove__ivm__template__TemplateBindingError__predicate_arity(void);
jz_err groove__ivm__template__TemplateBindingError__missing_input(u32 slot);
jz_err groove__ivm__template__TemplateBindingError__descriptor_mismatch(u32 slot);

enum { GROOVE_TNREF_INPUT, GROOVE_TNREF_NODE };
typedef struct groove_TemplateNodeRef { u8 tag; u32 index; } groove_TemplateNodeRef;
typedef JZ_VEC(groove_TemplateNodeRef) groove_TemplateNodeRefVec;
void groove__ivm__template__TemplateNodeRef__Debug__fmt(jz_str *out, const groove_TemplateNodeRef *v);

typedef struct groove_TemplateNode {
    groove_NodeDescriptor descriptor;
    groove_TemplateNodeRefVec inputs;
    bool has_predicate; u32 predicate;
} groove_TemplateNode;
typedef JZ_VEC(groove_TemplateNode) groove_TemplateNodeVec;
typedef JZ_VEC(groove_RecordDescriptor) groove_RecordDescriptorVec;

/* Arc<TypedGraphTemplate>: a jz_rc payload */
struct groove_TypedGraphTemplate {
    u64 identity;
    groove_TemplateNodeVec nodes;
    groove_RecordDescriptorVec inputs;
    groove_RecordDescriptor output;
    groove_TemplateNodeRef root;
    bool has_ordering; groove_TemplateNodeRef ordering;
    bool terminal;
    groove_GraphBuilder fallback;
    groove_PredicateVec predicate_markers;
};
void groove__ivm__template__TypedGraphTemplate__drop(groove_TypedGraphTemplate *v);
groove_RecordDescriptor groove__ivm__template__TypedGraphTemplate__output_descriptor(const groove_TypedGraphTemplate *self);

typedef struct groove_TemplateSlice { groove_UsizeVec inputs, predicates; } groove_TemplateSlice;
typedef struct groove_CompiledForestEntry { groove_TypedGraphTemplate *template_; groove_TemplateSlice slice; } groove_CompiledForestEntry;
/* Arc<[(Arc<TypedGraphTemplate>, TemplateSlice)]>: jz_rc payload */
typedef struct groove_CompiledForest { u32 len; groove_CompiledForestEntry items[]; } groove_CompiledForest;
typedef struct groove_TemplateCacheEntry { u64 key; groove_GraphBuilderVec graphs; groove_CompiledForest *forest; } groove_TemplateCacheEntry;

typedef struct groove_TypedGraphTemplateCache {
    jz_deque entries; /* VecDeque<(u64, Vec<GraphBuilder>, CompiledForest)> */
    u64 compilations, reuses;
} groove_TypedGraphTemplateCache;
void groove__ivm__template__TypedGraphTemplateCache__Default__default(groove_TypedGraphTemplateCache *self);
void groove__ivm__template__TypedGraphTemplateCache__drop(groove_TypedGraphTemplateCache *self);
void groove__ivm__template__TypedGraphTemplateCache__counters(const groove_TypedGraphTemplateCache *self, u64 *compilations, u64 *reuses);
void groove__ivm__template__TypedGraphTemplateCache__clear(groove_TypedGraphTemplateCache *self);

typedef struct groove_TemplateGraphInput { groove_GraphArc graph; groove_RecordDescriptor output; } groove_TemplateGraphInput;
typedef JZ_VEC(groove_TemplateGraphInput) groove_TemplateGraphInputVec;
groove_TemplateGraphInput groove__ivm__template__TemplateGraphInput__new(groove_GraphBuilder graph, groove_RecordDescriptor output);
groove_RecordDescriptor groove__ivm__template__TemplateGraphInput__descriptor(const groove_TemplateGraphInput *self);
groove_TemplateGraphInput groove__ivm__template__TemplateGraphInput__with_output_contract(groove_GraphBuilder graph, groove_RecordDescriptor output);
void groove__ivm__template__TemplateGraphInput__drop(groove_TemplateGraphInput *self);
void groove__ivm__template__TemplateGraphInputVec__drop(groove_TemplateGraphInputVec *v);

/* template/arguments.rs: TemplateArguments<'a> */
typedef struct groove_TemplateArguments {
    const groove_PredicateExpr *predicates; u32 n_predicates;
    groove_ScalarArgSlice *const *scalars; /* &Arc<[TemplateScalarArgument]> */
} groove_TemplateArguments;
jz_err groove__ivm__template__arguments__TemplateScalarArgument__new(groove_Value value, groove_ValueType value_type, groove_TemplateScalarArgument *out);
jz_err groove__ivm__template__arguments__bind_project_expression(groove_ProjectExpr *expression, const groove_TemplateScalarArgument *arguments, u32 n);
jz_err groove__ivm__template__arguments__TemplateArguments__bind_node(const groove_TemplateArguments *self, groove_GraphBuilder *node);
jz_err groove__ivm__template__arguments__bind_template_arguments(const groove_GraphBuilder *graphs, u32 n, const groove_PredicateExpr *predicates, u32 np,
                                                                 groove_ScalarArgSlice *scalars, groove_GraphBuilderVec *out);

jz_err groove__ivm__template__TypedGraphTemplate__bind_declarative(const groove_TypedGraphTemplate *self, const groove_GraphArc *inputs, u32 ni,
                                                                   const groove_PredicateExpr *predicates, u32 np, groove_GraphBuilder *out);
jz_err groove__ivm__template__TypedGraphTemplate__bind_declarative_with_arguments(const groove_TypedGraphTemplate *self, const groove_GraphArc *inputs, u32 ni,
                                                                                  const groove_PredicateExpr *predicates, u32 np,
                                                                                  const groove_TemplateScalarArgument *scalars, u32 ns, groove_GraphBuilder *out);
jz_err groove__ivm__template__compile_template_graphs(const groove_GraphBuilder *graphs, u32 n, groove_GraphBuilderVec *out);
jz_err groove__ivm__template__bind_template_graphs(const groove_GraphBuilder *graphs, u32 n, const groove_TemplateGraphInput *inputs, u32 ni, groove_GraphBuilderVec *out);
jz_err groove__ivm__template__bind_template_program(const groove_GraphBuilder *graphs, u32 n, const groove_TemplateGraphInput *inputs, u32 ni,
                                                    const groove_PredicateExpr *predicates, u32 np, groove_ScalarArgSlice *scalars, groove_GraphBuilderVec *out);
jz_err groove__ivm__template__bind_template_graphs_inner(const groove_GraphBuilder *graphs, u32 n, const groove_TemplateGraphInput *inputs, u32 ni,
                                                         bool descend_bound, const groove_TemplateArguments *arguments, groove_GraphBuilderVec *out);

/* template/sources.rs: describe: impl Fn(GraphBuilder) -> Result<TemplateGraphInput, E> */
typedef jz_err (*groove_TemplateDescribeFn)(void *ctx, groove_GraphBuilder graph, groove_TemplateGraphInput *out);
jz_err groove__ivm__template__sources__match_template_sources(const groove_GraphBuilder *const *graphs, u32 n,
                                                              const groove_GraphBuilder *const *blueprints, u32 nb,
                                                              groove_TemplateDescribeFn describe, void *ctx,
                                                              bool *matched, groove_TemplateGraphInputVec *out);
jz_err groove__ivm__template__sources__split_template_sources(const groove_GraphBuilder *const *graphs, u32 n,
                                                              groove_TemplateDescribeFn describe, void *ctx,
                                                              groove_GraphBuilderVec *out_graphs, groove_TemplateGraphInputVec *out_inputs);

/* other units (ivm/runtime) */
typedef i8 (*groove_GraphPairHook)(void *ctx, const groove_GraphBuilder *left, const groove_GraphBuilder *right); /* -1 = None */
bool groove__ivm__runtime__subscriptions__graph_builders_equal_with(const groove_GraphBuilder *left, const groove_GraphBuilder *right,
                                                                    groove_GraphPairHook hook, void *ctx);
jz_err groove__ivm__runtime__typed_template__TypedGraphTemplateCache__compile(groove_TypedGraphTemplateCache *self, const groove_GraphBuilder *graphs,
                                                                              u32 n, groove_GraphBuilderVec *out);

#endif
