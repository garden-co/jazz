/* lowering.h: lowering-internal vocabulary shared by query_engine/lowering.rs and its
 * submodules (planning, requirements, arguments, collect_layout, closure,
 * graph_lowering, terminals, templates). In Rust these are pub(super) items of one
 * module tree; in C they are one internal header. */
#ifndef JAZZ_ENGINE_QUERY_ENGINE_LOWERING_H
#define JAZZ_ENGINE_QUERY_ENGINE_LOWERING_H
#include "mod.h"

/* ---------------------------------------------------------------- error helpers
 * Result<T, UnsupportedReason> -> a boxed UnsupportedReason (NULL = Ok) plus out-params. */
typedef jazz_engine_UnsupportedReason *jazz_engine_UrErr;
#define JE_UR_TRY(expr) do { jazz_engine_UrErr _ue = (expr); if (_ue) return _ue; } while (0)
/* `?` from a Result<_, UnsupportedReason> inside a CapabilityResult fn (`.map_err(single_gap_report)?`) */
#define JE_UR_CAP_TRY(expr) do { jazz_engine_UrErr _ue = (expr); if (_ue) return jazz_engine__ur_into_cap(_ue); } while (0)
static inline jazz_engine_UrErr jazz_engine__ur_box(jazz_engine_UnsupportedReason r) {
    return (jazz_engine_UrErr)jz_memdup(&r, sizeof r);
}
static inline jazz_engine_UrErr jazz_engine__ur_op(jz_str message) {
    jazz_engine_UnsupportedReason r = jazz_engine__ur_new(JAZZ_ENGINE_UR_OPERATOR);
    r.message = message;
    return jazz_engine__ur_box(r);
}
static inline jazz_engine_UrErr jazz_engine__ur_rt(jz_str message) {
    jazz_engine_UnsupportedReason r = jazz_engine__ur_new(JAZZ_ENGINE_UR_RUNTIME);
    r.message = message;
    return jazz_engine__ur_box(r);
}
#define JE_UR_OP(lit) jazz_engine__ur_op(jz_str_from(lit))
#define JE_UR_RT(lit) jazz_engine__ur_rt(jz_str_from(lit))
static inline jazz_engine_UnsupportedReason jazz_engine__ur_take(jazz_engine_UrErr e) {
    jazz_engine_UnsupportedReason r = *e;
    jz_free(e);
    return r;
}
static inline void jazz_engine__ur_free(jazz_engine_UrErr e) {
    if (e) { JE_RES(UnsupportedReason__drop)(e); jz_free(e); }
}
/* single_gap_report(gap) from a boxed reason */
static inline jazz_engine_CapErr jazz_engine__ur_into_cap(jazz_engine_UrErr e) { return jazz_engine__cap_single(jazz_engine__ur_take(e)); }
/* Debug-format one value into a fresh string (for "{:?}" arguments) */
#define JE_DBG(var, fmtfn, ...) jz_str var = {0}; fmtfn(&var, __VA_ARGS__)
#define JE_STR_ARG(s) (int)(s).len, (s).ptr

/* ================================================================ planning.rs */
#define JE_PLAN(x) jazz_engine__query_engine__lowering__planning__##x
#define JAZZ_ENGINE_MAX_ROW_SET_NESTING_DEPTH 256u

typedef struct jazz_engine_LinearStep jazz_engine_LinearStep;
typedef JZ_VEC(jazz_engine_LinearStep) jazz_engine_LinearStepVec;
enum { JAZZ_ENGINE_LROOT_SOURCE, JAZZ_ENGINE_LROOT_VALUE, JAZZ_ENGINE_LROOT_FRONTIER };
typedef struct jazz_engine_LinearRoot {
    u8 tag;
    jazz_engine_SourceId source; u8 visibility;             /* Source */
    jz_str shape; u8 mode;                                  /* Value */
    jazz_engine_FrontierId frontier;                        /* Frontier */
    jazz_engine_ValueSourceColumnVec columns;               /* Value / Frontier */
} jazz_engine_LinearRoot;
typedef struct jazz_engine_LinearCurrentRoot { jazz_engine_LinearRoot root; jazz_engine_LinearStepVec steps; } jazz_engine_LinearCurrentRoot;

typedef struct jazz_engine_RelationInputPlan jazz_engine_RelationInputPlan;
typedef struct jazz_engine_UnionBranchPlan jazz_engine_UnionBranchPlan;
typedef JZ_VEC(jazz_engine_UnionBranchPlan) jazz_engine_UnionBranchPlanVec;
typedef struct jazz_engine_UnionPlan { jazz_engine_UnionBranchPlanVec branches; jazz_engine_LinearStepVec terminal_steps; } jazz_engine_UnionPlan;
typedef struct jazz_engine_RecursiveRelationPlan {
    jazz_engine_LinearCurrentRoot seed, step;
    jazz_engine_FrontierId frontier;
    jazz_engine_NormalizedValueRef frontier_key;
    jazz_engine_NvrVec dedupe_keys;
    jazz_model_RecursionBound bound;
} jazz_engine_RecursiveRelationPlan;
enum { JAZZ_ENGINE_RIP_LINEAR, JAZZ_ENGINE_RIP_UNION, JAZZ_ENGINE_RIP_RECURSIVE };
struct jazz_engine_RelationInputPlan {
    u8 tag;
    union { jazz_engine_LinearCurrentRoot linear; jazz_engine_UnionPlan union_; jazz_engine_RecursiveRelationPlan recursive; } u;
};
struct jazz_engine_UnionBranchPlan { jz_str label; jazz_engine_RelationInputPlan plan; };

typedef struct jazz_engine_CorrelatedPathPlan jazz_engine_CorrelatedPathPlan;
typedef JZ_VEC(jazz_engine_CorrelatedPathPlan) jazz_engine_CorrelatedPathPlanVec;
struct jazz_engine_CorrelatedPathPlan {
    jazz_engine_LinearCurrentRoot parent, child;
    jazz_engine_ProgramPathId path;
    jazz_engine_PredicateExpr correlation;
    u8 requirement;
    jazz_engine_LinearStepVec output_steps;
    jazz_engine_CorrelatedPathPlanVec siblings, nested;
};
enum { JAZZ_ENGINE_AQP_LINEAR, JAZZ_ENGINE_AQP_UNION, JAZZ_ENGINE_AQP_CORRELATED_PATH, JAZZ_ENGINE_AQP_RECURSIVE_RELATION };
typedef struct jazz_engine_AnalyzedQueryPlan {
    u8 tag;
    union {
        jazz_engine_LinearCurrentRoot linear;
        jazz_engine_UnionPlan union_;
        jazz_engine_CorrelatedPathPlan correlated;
        jazz_engine_RecursiveRelationPlan recursive;
    } u;
} jazz_engine_AnalyzedQueryPlan;

enum { JAZZ_ENGINE_STEP_FILTER, JAZZ_ENGINE_STEP_JOIN, JAZZ_ENGINE_STEP_PROJECT, JAZZ_ENGINE_STEP_ORDER_BY, JAZZ_ENGINE_STEP_SLICE, JAZZ_ENGINE_STEP_AGGREGATE };
struct jazz_engine_LinearStep {
    u8 tag;
    union {
        jazz_engine_PredicateExpr filter;
        struct { jazz_engine_RelationInputPlan *right; u8 mode; jazz_engine_PredicateExpr on; } join;
        jazz_engine_RowProjectionVec project;
        jazz_engine_OrderKeyVec order_by;
        struct {
            jazz_engine_NvrVec partition_by;
            bool has_limit; u32 limit; u32 offset;
            jazz_engine_NvrVec tie_breaker;
            bool has_rank_output; jazz_engine_TypedOutputField rank_output;
        } slice;
        struct { jazz_engine_NvrVec group_by; jazz_engine_AggregateExprVec outputs; } aggregate;
    } u;
};

typedef struct jazz_engine_LinearFragment { const jazz_engine_LinearRoot *root; const jazz_engine_LinearStep *steps; u32 nsteps; } jazz_engine_LinearFragment;
typedef struct jazz_engine_PlanFragments {
    JZ_VEC(jazz_engine_LinearFragment) linears;
    JZ_VEC(const jazz_engine_PredicateExpr *) correlations;
    JZ_VEC(const jazz_engine_RecursiveRelationPlan *) recursives;
} jazz_engine_PlanFragments;
void jazz_engine__plan_fragments_drop(jazz_engine_PlanFragments *f);

/* derives */
void JE_PLAN(LinearRoot__Clone__clone)(jazz_engine_LinearRoot *dst, const jazz_engine_LinearRoot *src);
void JE_PLAN(LinearRoot__drop)(jazz_engine_LinearRoot *r);
void JE_PLAN(LinearRoot__Debug__fmt)(jz_str *out, const jazz_engine_LinearRoot *r);
void JE_PLAN(LinearCurrentRoot__Clone__clone)(jazz_engine_LinearCurrentRoot *dst, const jazz_engine_LinearCurrentRoot *src);
void JE_PLAN(LinearCurrentRoot__drop)(jazz_engine_LinearCurrentRoot *r);
void JE_PLAN(LinearCurrentRoot__Debug__fmt)(jz_str *out, const jazz_engine_LinearCurrentRoot *r);
void JE_PLAN(LinearStep__Clone__clone)(jazz_engine_LinearStep *dst, const jazz_engine_LinearStep *src);
void JE_PLAN(LinearStep__drop)(jazz_engine_LinearStep *s);
void JE_PLAN(LinearStep__Debug__fmt)(jz_str *out, const jazz_engine_LinearStep *s);
void jazz_engine__steps_clone(jazz_engine_LinearStepVec *dst, const jazz_engine_LinearStepVec *src);
void jazz_engine__steps_drop(jazz_engine_LinearStepVec *v);
void jazz_engine__step_clone_v(void *d, const void *s);
void jazz_engine__step_drop_v(void *s);
void JE_PLAN(RelationInputPlan__Clone__clone)(jazz_engine_RelationInputPlan *dst, const jazz_engine_RelationInputPlan *src);
void JE_PLAN(RelationInputPlan__drop)(jazz_engine_RelationInputPlan *p);
void JE_PLAN(RelationInputPlan__Debug__fmt)(jz_str *out, const jazz_engine_RelationInputPlan *p);
void JE_PLAN(UnionPlan__Clone__clone)(jazz_engine_UnionPlan *dst, const jazz_engine_UnionPlan *src);
void JE_PLAN(UnionPlan__drop)(jazz_engine_UnionPlan *p);
void JE_PLAN(RecursiveRelationPlan__Clone__clone)(jazz_engine_RecursiveRelationPlan *dst, const jazz_engine_RecursiveRelationPlan *src);
void JE_PLAN(RecursiveRelationPlan__drop)(jazz_engine_RecursiveRelationPlan *p);
void JE_PLAN(CorrelatedPathPlan__Clone__clone)(jazz_engine_CorrelatedPathPlan *dst, const jazz_engine_CorrelatedPathPlan *src);
void JE_PLAN(CorrelatedPathPlan__drop)(jazz_engine_CorrelatedPathPlan *p);
void JE_PLAN(AnalyzedQueryPlan__Clone__clone)(jazz_engine_AnalyzedQueryPlan *dst, const jazz_engine_AnalyzedQueryPlan *src);
void JE_PLAN(AnalyzedQueryPlan__drop)(jazz_engine_AnalyzedQueryPlan *p);

/* functions */
const jazz_engine_SourceId *JE_PLAN(LinearRoot__source)(const jazz_engine_LinearRoot *r);
const jazz_engine_SourceId *JE_PLAN(AnalyzedQueryPlan__root_source)(const jazz_engine_AnalyzedQueryPlan *p);
const char *JE_PLAN(AnalyzedQueryPlan__capability_label)(const jazz_engine_AnalyzedQueryPlan *p);
const jazz_engine_SourceId *JE_PLAN(first_step_source)(const jazz_engine_LinearStep *steps, u32 n);
const jazz_engine_SourceId *JE_PLAN(UnionPlan__root_source)(const jazz_engine_UnionPlan *p);
const jazz_engine_SourceId *JE_PLAN(RecursiveRelationPlan__root_source)(const jazz_engine_RecursiveRelationPlan *p);
const jazz_engine_SourceId *JE_PLAN(RecursiveRelationPlan__seed_source)(const jazz_engine_RecursiveRelationPlan *p);
const jazz_engine_SourceId *JE_PLAN(RecursiveRelationPlan__step_source)(const jazz_engine_RecursiveRelationPlan *p);
const jazz_engine_SourceId *JE_PLAN(RelationInputPlan__root_source)(const jazz_engine_RelationInputPlan *p);
/* Err(Vec<UnsupportedReason>): returns false and fills *gaps */
bool JE_PLAN(analyze_query_plan)(const jazz_engine_QueryProgramRequest *request, jazz_engine_AnalyzedQueryPlan *out, jazz_engine_UnsupportedReasonVec *gaps);
void JE_PLAN(validate_output_capabilities)(const jazz_engine_QueryProgramRequest *request, const jazz_engine_AnalyzedQueryPlan *plan, jazz_engine_UnsupportedReasonVec *gaps);
/* root_aggregate_step: false = None */
bool JE_PLAN(root_aggregate_step)(const jazz_engine_AnalyzedQueryPlan *plan, const jazz_engine_NvrVec **group_by, const jazz_engine_AggregateExprVec **outputs);
void JE_PLAN(collect_plan_fragments)(const jazz_engine_AnalyzedQueryPlan *plan, jazz_engine_PlanFragments *out);
void JE_PLAN(program_sources)(const jazz_engine_QueryProgramRequest *request, const jazz_engine_AnalyzedQueryPlan *plan, jz_btree *out);
void JE_PLAN(source_visibilities)(const jazz_engine_AnalyzedQueryPlan *plan, jz_btree *out); /* SourceId -> u8 */
bool JE_PLAN(source_current_tier)(const jazz_engine_QueryProgramRequest *request, const jazz_engine_SourceId *source, u8 *tier);
jazz_engine_UrErr JE_PLAN(analyze_relation_input_node)(const jz_str *node_id, const jz_btree *nodes, jz_btree *visited, jazz_engine_RelationInputPlan *out);
extern const jz_btree_desc jazz_engine__SOURCE_ID_U8_MAP_DESC; /* BTreeMap<SourceId, RowVisibility> */

/* ================================================================ requirements.rs */
#define JE_REQ(x) jazz_engine__query_engine__lowering__requirements__##x
/* -> BTreeMap<SourceId, SourceRequirements> */
jazz_engine_CapErr JE_REQ(source_requirements)(const jazz_engine_QueryProgramRequest *request, const jazz_engine_AnalyzedQueryPlan *plan, jz_btree *out);

/* ================================================================ closure.rs (forward) */
#define JE_CLO(x) jazz_engine__query_engine__lowering__closure__##x
typedef JZ_VEC(const jazz_engine_ClosurePathSegment *) jazz_engine_SegmentRefVec;
void JE_CLO(closure_path_segments)(const jazz_engine_ClosurePath *path, jazz_engine_SegmentRefVec *out);

/* ================================================================ terminals.rs (forward) */
#define JE_TERM(x) jazz_engine__query_engine__lowering__terminals__##x
void JE_TERM(flat_join_payload_fields)(const jazz_engine_AnalyzedQueryPlan *plan, jazz_engine_TypedOutputFieldVec *out);

/* ================================================================ lowering.rs: parameter domains */
#define JE_LOW(x) jazz_engine__query_engine__lowering__##x
/* ClaimParameter { path, ty } has exactly ProgramClaimParam's layout; one C type serves both. */
typedef jazz_engine_ProgramClaimParam jazz_engine_ClaimParameter;
typedef struct jazz_engine_ParameterDomain {
    jz_btree user_params;    /* String -> ColumnType */
    jz_btree claim_params;   /* String -> ClaimParameter */
    jz_btree routing_params; /* BTreeSet<String> */
} jazz_engine_ParameterDomain;
void JE_LOW(ParameterDomain__Default__default)(jazz_engine_ParameterDomain *d);
void JE_LOW(ParameterDomain__Clone__clone)(jazz_engine_ParameterDomain *dst, const jazz_engine_ParameterDomain *src);
void JE_LOW(ParameterDomain__drop)(jazz_engine_ParameterDomain *d);
bool JE_LOW(ParameterDomain__PartialEq__eq)(const jazz_engine_ParameterDomain *a, const jazz_engine_ParameterDomain *b);
bool jazz_engine__claim_parameter_eq(const jazz_engine_ClaimParameter *a, const jazz_engine_ClaimParameter *b);
jazz_engine_UrErr JE_LOW(parameter_domain_for_request)(const jazz_engine_QueryProgramRequest *request, jazz_engine_ParameterDomain *out);
/* claim_binding_slot: false = None */
bool JE_LOW(claim_binding_slot)(const jazz_engine_ParameterDomain *domain, const jazz_engine_ClaimPath *path, jz_str *out);
extern const jz_btree_desc jazz_engine__U32_PARAM_DOMAIN_MAP_DESC; /* BTreeMap<u32, ParameterDomain> */

/* ================================================================ arguments.rs */
#define JE_ARG(x) jazz_engine__query_engine__lowering__arguments__##x
typedef struct jazz_engine_OptValueType { bool some; groove_ValueType ty; } jazz_engine_OptValueType;
typedef JZ_VEC(jazz_engine_OptValueType) jazz_engine_OptValueTypeVec;
typedef struct jazz_engine_PredicateArgument {
    jazz_engine_PredicateExpr predicate;
    jazz_engine_SourceId source_id;
    jazz_engine_ResolvedSource source; /* only blueprint source metadata */
} jazz_engine_PredicateArgument;
typedef struct jazz_engine_ScalarArgument { jazz_engine_NormalizedValueRef value; groove_ValueType ty; bool coerce; } jazz_engine_ScalarArgument;
typedef struct jazz_engine_ProgramArgumentRecipes {
    JZ_VEC(jazz_engine_PredicateArgument) predicates;
    JZ_VEC(jazz_engine_ScalarArgument) scalars;
    jazz_engine_OptValueTypeVec literal_types;
} jazz_engine_ProgramArgumentRecipes;
/* LoweringContext<'a>. The RefCell<ProgramArgumentRecipes> is a plain mutable pointer and the
 * OnceCell parameter domain is a lazily filled slot (state 0 = empty, 1 = Ok, 2 = Err). */
typedef struct jazz_engine_LoweringContext {
    const jazz_engine_QueryProgramRequest *request;
    jazz_engine_ProgramArgumentRecipes *arguments; /* NULL = None */
    const groove_Value *literals; u32 nliterals;
    u8 parameter_domain_state;
    jazz_engine_ParameterDomain parameter_domain;
    jazz_engine_UnsupportedReason parameter_domain_err;
} jazz_engine_LoweringContext;
void JE_ARG(LoweringContext__concrete)(jazz_engine_LoweringContext *ctx, const jazz_engine_QueryProgramRequest *request);
void JE_ARG(LoweringContext__compiling)(jazz_engine_LoweringContext *ctx, const jazz_engine_QueryProgramRequest *request,
                                       jazz_engine_ProgramArgumentRecipes *arguments);
void JE_ARG(LoweringContext__drop)(jazz_engine_LoweringContext *ctx);
jazz_engine_UrErr JE_ARG(LoweringContext__parameter_domain)(jazz_engine_LoweringContext *ctx, const jazz_engine_ParameterDomain **out);
/* predicate_argument: false = None */
bool JE_ARG(LoweringContext__predicate_argument)(jazz_engine_LoweringContext *ctx, const jazz_engine_PredicateExpr *predicate,
                                                const jazz_engine_SourceId *source_id, const jazz_engine_ResolvedSource *source,
                                                groove_PredicateExpr *out);
jazz_engine_UrErr JE_ARG(LoweringContext__literal)(const jazz_engine_LoweringContext *ctx, u32 slot, groove_Value *out);
bool JE_ARG(LoweringContext__literal_type)(const jazz_engine_LoweringContext *ctx, u32 slot, groove_ValueType *out);
/* scalar_argument: consumes value and ty; false = None (they are dropped) */
bool JE_ARG(LoweringContext__scalar_argument)(jazz_engine_LoweringContext *ctx, jazz_engine_NormalizedValueRef value, groove_ValueType ty,
                                             bool coerce, groove_ProjectExpr *out);
void JE_ARG(ProgramArgumentRecipes__with_literal_types)(jazz_engine_ProgramArgumentRecipes *out, jazz_engine_OptValueTypeVec literal_types);
void JE_ARG(ProgramArgumentRecipes__Clone__clone)(jazz_engine_ProgramArgumentRecipes *dst, const jazz_engine_ProgramArgumentRecipes *src);
void JE_ARG(ProgramArgumentRecipes__drop)(jazz_engine_ProgramArgumentRecipes *r);
jazz_engine_UrErr JE_ARG(ProgramArgumentRecipes__bind)(const jazz_engine_ProgramArgumentRecipes *self, groove_GraphBuilder *const *graphs, u32 ngraphs,
                                                      const groove_TemplateGraphInput *inputs, u32 ninputs,
                                                      const jazz_engine_QueryProgramRequest *request, const groove_Value *literals, u32 nliterals,
                                                      groove_GraphBuilderVec *out);
jazz_engine_UrErr JE_ARG(extract_template_literals)(jazz_engine_QueryProgramRequest *request, groove_ValueVec *out);
void jazz_engine__opt_vt_drop_v(void *p);
void jazz_engine__opt_vt_clone_v(void *d, const void *s);

/* ================================================================ graph_lowering.rs (forward) */
#define JE_GL(x) jazz_engine__query_engine__lowering__graph_lowering__##x
jazz_engine_UrErr JE_GL(lower_predicate)(const jazz_engine_PredicateExpr *predicate, const jazz_engine_SourceId *source_id,
                                         const jazz_engine_ResolvedSource *source, jazz_engine_LoweringContext *request, groove_PredicateExpr *out);
jazz_engine_UrErr JE_GL(claim_value)(const jazz_engine_ClaimPath *path, const jazz_engine_PolicyContext *policy, groove_Value *out);
void JE_GL(source_fields)(const jazz_engine_ResolvedSource *source, jazz_engine_StrVec *out);

/* ================================================================ lowering.rs: programs and terminals */
typedef struct jazz_engine_LoweredTerminal {
    jz_str sink;
    groove_GraphBuilder *graph;
    jazz_engine_OutputTerminalSchema output;
} jazz_engine_LoweredTerminal;
typedef JZ_VEC(jazz_engine_LoweredTerminal) jazz_engine_LoweredTerminalVec;
typedef struct jazz_engine_LoweredGraph {
    jazz_engine_LoweredTerminalVec terminals;
    jz_btree shared_witness_sinks;           /* String -> String */
    groove_GraphBuilder *internal_app_rows_graph; /* NULL = None */
    jazz_engine_ParameterDomain parameters;
    jazz_engine_ProgramOutputSchemas output;
    jz_btree maintained_terminal_tables;     /* String -> TableSchema */
    jz_btree targeted_refresh_tables;        /* BTreeSet<String> */
    bool targeted_refresh_uncertain;
} jazz_engine_LoweredGraph;
typedef struct jazz_engine_QueryProgram {
    jazz_engine_QueryProgramRequest request;
    jazz_engine_LoweredGraph lowered;
    jz_btree source_descriptors;               /* ProgramSourceId -> RecordDescriptor */
    jz_btree covered_input_source_descriptors; /* ProgramSourceId -> RecordDescriptor */
    jazz_engine_ExplainPlan explain;
} jazz_engine_QueryProgram;
typedef struct jazz_engine_QueryProgramCompilation {
    jazz_engine_QueryProgramRequest request;
    jazz_engine_AnalyzedQueryPlan *plan; /* Box */
    jazz_engine_SourceRequestVec sources;
} jazz_engine_QueryProgramCompilation;
extern const jz_btree_desc jazz_engine__PROGRAM_SOURCE_DESCRIPTOR_MAP_DESC;
extern const jz_btree_desc jazz_engine__STR_TABLE_SCHEMA_MAP_DESC;
void JE_LOW(LoweredTerminal__Clone__clone)(jazz_engine_LoweredTerminal *dst, const jazz_engine_LoweredTerminal *src);
void JE_LOW(LoweredTerminal__drop)(jazz_engine_LoweredTerminal *t);
void jazz_engine__terminal_drop_v(void *t);
void jazz_engine__terminal_clone_v(void *d, const void *s);
void JE_LOW(LoweredGraph__Clone__clone)(jazz_engine_LoweredGraph *dst, const jazz_engine_LoweredGraph *src);
void JE_LOW(LoweredGraph__drop)(jazz_engine_LoweredGraph *g);
bool JE_LOW(LoweredGraph__is_execution_terminal)(const jazz_engine_LoweredGraph *g, const jazz_engine_LoweredTerminal *t);
void JE_LOW(QueryProgram__Clone__clone)(jazz_engine_QueryProgram *dst, const jazz_engine_QueryProgram *src);
void JE_LOW(QueryProgram__drop)(jazz_engine_QueryProgram *p);
void JE_LOW(QueryProgramCompilation__Clone__clone)(jazz_engine_QueryProgramCompilation *dst, const jazz_engine_QueryProgramCompilation *src);
void JE_LOW(QueryProgramCompilation__drop)(jazz_engine_QueryProgramCompilation *c);
jazz_engine_CapErr JE_LOW(QueryProgramCompilation__analyze)(jazz_engine_QueryProgramRequest request, jazz_engine_QueryProgramCompilation *out);
jazz_engine_CapErr JE_LOW(query_program_source_requests)(const jazz_engine_QueryProgramRequest *request, jazz_engine_SourceRequestVec *out);
bool JE_LOW(authorized_deletion_preimage_source_request)(const jazz_engine_SourceRequest *request, jazz_engine_SourceRequest *out);
jazz_engine_CapErr JE_LOW(prepare_and_lower_query_program)(jazz_engine_QueryProgramCompilation compilation, jazz_engine_SourceGraphPreparer *preparer,
                                                          jazz_engine_QueryProgram *out);
jazz_engine_CapErr JE_LOW(prepare_query_program_sources)(const jazz_engine_QueryProgramCompilation *compilation, jazz_engine_SourceGraphPreparer *preparer,
                                                        jz_btree *sources, jazz_engine_ExplainPlan *explain);
jazz_engine_CapErr JE_LOW(lower_resolved_query_program)(jazz_engine_QueryProgramCompilation compilation, jz_btree resolved_sources,
                                                       jazz_engine_ExplainPlan explain, jazz_engine_QueryProgram *out);
jazz_engine_CapErr JE_LOW(lower_resolved_query_program_with_source_parameters)(jazz_engine_QueryProgramCompilation compilation, jz_btree resolved_sources,
    jazz_engine_ExplainPlan explain, const jz_btree *source_parameters, jazz_engine_ProgramArgumentRecipes *arguments, jazz_engine_QueryProgram *out);
/* graph_declared_output_fields: false = None */
bool JE_LOW(graph_declared_output_fields)(const groove_GraphBuilder *graph, jz_btree *out);
jazz_engine_ExplainPlan JE_LOW(explain_with_request)(const jazz_engine_QueryProgramRequest *request, jazz_engine_ExplainPlan explain);
jazz_engine_CapErr JE_LOW(single_gap_report)(jazz_engine_UnsupportedReason gap);
/* union_member: members is BTreeMap<SourceId, GraphBuilder>; consumes source and graph */
void JE_LOW(union_member)(jz_btree *members, jazz_engine_SourceId source, groove_GraphBuilder *graph);
extern const jz_btree_desc jazz_engine__SOURCE_ID_GRAPH_MAP_DESC;
void JE_LOW(collect_binding_source_params)(const groove_GraphBuilder *graph, jazz_engine_ParameterDomain *domain);
void JE_LOW(collect_binding_source_params_with_slots)(const groove_GraphBuilder *graph, jazz_engine_ParameterDomain *domain, const jz_btree *slots);
bool JE_LOW(graph_contains_input_source)(const groove_GraphBuilder *graph);
typedef JZ_VEC(const groove_GraphBuilder *) jazz_engine_GraphRefVec;
void JE_LOW(graph_builder_postorder)(const groove_GraphBuilder *graph, jazz_engine_GraphRefVec *out);

/* ================================================================ graph_lowering.rs: relation inputs */
typedef struct jazz_engine_LoweredRelationInput {
    groove_GraphBuilder *graph;
    bool has_root_source; jazz_engine_ResolvedSource root_source;
    jz_btree fields;                /* BTreeSet<String> */
    jz_btree nullable_fields;       /* BTreeSet<String> */
    jz_btree nullable_field_depths; /* String -> usize */
    bool has_union_occurrence_carrier; jz_str union_label_field, union_row_field;
} jazz_engine_LoweredRelationInput;
void JE_GL(LoweredRelationInput__Clone__clone)(jazz_engine_LoweredRelationInput *dst, const jazz_engine_LoweredRelationInput *src);
void JE_GL(LoweredRelationInput__drop)(jazz_engine_LoweredRelationInput *l);
jazz_engine_UrErr JE_GL(lower_plan_steps)(groove_GraphBuilder *graph, const jazz_engine_AnalyzedQueryPlan *plan, const jazz_engine_ResolvedSource *root_source,
                                          const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, jazz_engine_LoweredRelationInput *out);

/* ================================================================ terminals.rs (forward) */
jazz_engine_CapErr JE_TERM(lowered_terminals)(groove_GraphBuilder *graph, jazz_engine_LoweringContext *request, const jazz_engine_AnalyzedQueryPlan *plan,
                                              const jazz_engine_ResolvedSource *source, const jz_btree *resolved_sources,
                                              const jazz_engine_ParameterDomain *parameter_domain, const jz_btree *routing_param_fields,
                                              const jz_btree *available_fields, jazz_engine_LoweredTerminalVec *out);
jazz_engine_CapErr JE_TERM(validate_app_row_publication_schema)(const jazz_engine_AppRowSchema *rows);
void JE_TERM(shared_witness_sinks)(const jazz_engine_LoweredTerminal *terminals, u32 n, jz_btree *out);
void JE_TERM(output_routing_fields)(const jazz_engine_ProgramFactOutput *output, jz_btree *out);

/* ================================================================ graph_lowering.rs: shared helpers (forward) */
/* source_field_type: NULL = None */
const groove_ValueType *JE_GL(source_field_type)(const jazz_engine_ResolvedSource *source, jz_slice field);
/* consumes parent */
jazz_engine_UrErr JE_GL(lower_correlated_path_relation_graph_from_parent)(const jazz_engine_CorrelatedPathPlan *path, groove_GraphBuilder *parent,
    const jazz_engine_ResolvedSource *parent_source, const jz_btree *resolved_sources, jazz_engine_LoweringContext *request,
    bool retain_child_window, jazz_engine_LoweredRelationInput *out);

/* ================================================================ terminals.rs: collector layout types */
enum { JAZZ_ENGINE_CFO_SOURCE_ROW, JAZZ_ENGINE_CFO_DERIVED };
typedef struct jazz_engine_CollectFlatField {
    jz_str input, output;
    groove_ValueType value_type, output_value_type;
    jazz_engine_OptStr source_field, source_public_name;
    u8 origin;
    bool is_row_id, is_presence, is_output;
} jazz_engine_CollectFlatField;
typedef JZ_VEC(jazz_engine_CollectFlatField) jazz_engine_CollectFlatFieldVec;
typedef struct jazz_engine_CollectSlotLayout jazz_engine_CollectSlotLayout;
typedef JZ_VEC(jazz_engine_CollectSlotLayout) jazz_engine_CollectSlotLayoutVec;
struct jazz_engine_CollectSlotLayout {
    jazz_engine_ProgramPathId path;
    jz_str collection_field;
    jazz_engine_CollectFlatFieldVec fields;
    jz_str row_id_input, presence_input;
    jazz_engine_OptStr reference_array_input;
    bool reference_order;
    groove_TopByOrderVec order_cols;
    jazz_engine_StrVec tie_cols;
    u64 offset; groove_TopByLimit limit;
    jazz_engine_CollectSlotLayoutVec children;
};
typedef struct jazz_engine_CollectLayout {
    jazz_engine_CollectFlatFieldVec root_fields;
    jazz_engine_StrVec root_occurrence_inputs;
    groove_TopByOrderVec root_order_cols;
    jazz_engine_StrVec root_tie_cols;
    u64 root_offset; groove_TopByLimit root_limit;
    jazz_engine_CollectSlotLayoutVec slots;
} jazz_engine_CollectLayout;
void JE_TERM(CollectFlatField__drop)(jazz_engine_CollectFlatField *f);
void JE_TERM(CollectSlotLayout__drop)(jazz_engine_CollectSlotLayout *s);
void JE_TERM(CollectLayout__drop)(jazz_engine_CollectLayout *l);
/* Vec<(String, ValueType)> uses TypedOutputField's {name, ty} layout */
jazz_engine_CapErr JE_TERM(root_join_occurrence_fields)(const jazz_engine_AnalyzedQueryPlan *plan, const jz_btree *resolved_sources,
                                                        jazz_engine_LoweringContext *request, jazz_engine_TypedOutputFieldVec *out);
/* NULL = None */
const jazz_engine_CorrelatedPathPlan *JE_TERM(find_nested_correlated_path)(const jazz_engine_CorrelatedPathPlan *parent, const jazz_engine_ProgramPathId *path);

/* ================================================================ collect_layout.rs */
#define JE_CL(x) jazz_engine__query_engine__lowering__collect_layout__##x
jz_str JE_LOW(public_root_field_name)(const jazz_engine_CollectFlatField *field);
jazz_engine_CapErr JE_CL(collect_layout)(const jazz_engine_AppProjectionTree *projection, const jazz_engine_AnalyzedQueryPlan *plan,
    const jazz_engine_ResolvedSource *root_source, const jz_btree *resolved_sources, jazz_engine_LoweringContext *request,
    const jz_btree *routing_param_fields, const jazz_engine_ParameterDomain *parameter_domain, const jz_btree *available_fields,
    jazz_engine_CollectLayout *out);
groove_ValueType JE_CL(collect_unwrapped_output_type)(const jazz_engine_ResolvedSource *source, jz_slice source_field, const groove_ValueType *fallback);
groove_ValueType JE_CL(collect_slot_input_type)(const groove_ValueType *output_value_type);
/* consume graph */
jazz_engine_CapErr JE_CL(root_collect_context_graph)(groove_GraphBuilder *graph, const jazz_engine_ResolvedSource *source,
    const jazz_engine_CollectLayout *layout, bool source_positions_are_stable, groove_GraphBuilder **out);
jazz_engine_CapErr JE_CL(collect_anchor_graph)(groove_GraphBuilder *graph, const jazz_engine_ResolvedSource *root_source,
    const jazz_engine_CollectLayout *layout, bool source_positions_are_stable, groove_GraphBuilder **out);
jazz_engine_CapErr JE_CL(lower_collect_slot_graphs)(const jazz_engine_CollectSlotLayout *slot, const jazz_engine_CorrelatedPathPlan *path,
    groove_GraphBuilder *parent_graph, const jazz_engine_ResolvedSource *parent_source, const jazz_engine_ResolvedSource *root_source,
    const jazz_engine_CollectLayout *layout, const jz_btree *inherited_flat_fields, const jz_btree *resolved_sources,
    jazz_engine_LoweringContext *request, groove_GraphBuilderVec *out);
groove_CollectBySlotBuilder JE_CL(collect_slot_builder)(const jazz_engine_CollectSlotLayout *slot, jz_slice parent_row_id, const jz_btree *route_fields);
jazz_engine_CapErr JE_CL(collect_output_descriptor)(const jazz_engine_CollectLayout *layout, groove_RecordDescriptor *out);

/* ================================================================ graph_lowering.rs / terminals.rs used by closure.rs (forward) */
jazz_engine_UrErr JE_GL(lower_relation_input_for_contributor)(const jazz_engine_RelationInputPlan *plan, const jz_btree *resolved_sources,
    jazz_engine_LoweringContext *request, jazz_engine_LoweredRelationInput *out);
groove_GraphBuilder *JE_GL(unwrap_nullable_join_key)(groove_GraphBuilder *graph, jz_str field, u32 nullable_depth);
/* root_graph: NULL = None (consumed); lowered: NULL = None (BTreeMap<usize, LoweredRelationInput>) */
jazz_engine_UrErr JE_GL(lower_recursive_relation_cached)(groove_GraphBuilder *root_graph, const jazz_engine_RecursiveRelationPlan *relation,
    const jazz_engine_ResolvedSource *root_source, const jz_btree *resolved_sources, jazz_engine_LoweringContext *request,
    const jz_btree *lowered, jazz_engine_LoweredRelationInput *out);
jazz_engine_UrErr JE_GL(lower_recursive_seed_membership)(const jazz_engine_RecursiveRelationPlan *relation, const jazz_engine_ResolvedSource *seed_source,
    const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, jazz_engine_LoweredRelationInput *out);
jazz_engine_UrErr JE_GL(lower_root_to_relation_key_pairs)(const jazz_engine_PredicateExpr *predicate, const jazz_engine_ResolvedSource *root_source,
    const jazz_engine_RelationInputPlan *right_plan, const jazz_engine_LoweredRelationInput *right_output, jazz_engine_LoweringContext *request,
    jazz_engine_StrVec *left_keys, jazz_engine_StrVec *right_keys);
jazz_engine_UrErr JE_GL(lower_relation_key_ref)(const jazz_engine_NormalizedValueRef *value, const jazz_engine_RelationInputPlan *plan,
    const jazz_engine_LoweredRelationInput *output, jazz_engine_LoweringContext *request, jz_str *out);
extern const jz_btree_desc jazz_engine__U32_LRI_MAP_DESC; /* BTreeMap<usize, LoweredRelationInput> */
/* consumes visible */
jazz_engine_CapErr JE_TERM(source_rows_for_visible_graph)(const jazz_engine_ResolvedSource *source, groove_GraphBuilder *visible,
    const jz_btree *route_fields, groove_GraphBuilder **out);
groove_ProjectFieldVec JE_TERM(project_source_fields_with_routes)(const jazz_engine_ResolvedSource *source, const jz_btree *route_fields);
groove_ProjectFieldVec JE_TERM(project_source_fields_with_routes_from_prefix)(const jazz_engine_ResolvedSource *source, const char *prefix,
    const jz_btree *route_fields);
groove_ProjectFieldVec JE_TERM(project_source_fields_from_prefix)(const jazz_engine_ResolvedSource *source, const char *prefix);

/* ================================================================ closure.rs */
typedef struct jazz_engine_ClosureLowering {
    groove_GraphBuilder *visible_root;
    jz_btree result_members; /* SourceId -> GraphBuilder */
} jazz_engine_ClosureLowering;
void JE_CLO(ClosureLowering__drop)(jazz_engine_ClosureLowering *c);
/* consumes root_graph */
jazz_engine_CapErr JE_CLO(lower_closure_membership)(groove_GraphBuilder *root_graph, jazz_engine_LoweringContext *request,
    const jazz_engine_AnalyzedQueryPlan *plan, const jazz_engine_ResolvedSource *root_source, const jz_btree *resolved_sources,
    const jz_btree *route_fields, const jz_btree *root_carrier_fields, jazz_engine_ClosureLowering *out);
jazz_engine_CapErr JE_CLO(reachable_contribution_membership_graph)(groove_GraphBuilder *visible_root, const jazz_engine_ReachableContribution *contribution,
    const jazz_engine_ResolvedSource *root_source, const jazz_engine_ResolvedSource *contribution_source, const jz_btree *nodes,
    const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, const jz_btree *route_fields, groove_GraphBuilder **out);
jazz_engine_CapErr JE_CLO(reachable_step_witness_membership_graph)(const jazz_engine_ReachableContribution *contribution, const jz_btree *nodes,
    const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, const jz_btree *route_fields, groove_GraphBuilder **out);
/* *has = false is Ok(None) */
jazz_engine_CapErr JE_CLO(reachable_seed_membership_graph)(const jazz_engine_ReachableContribution *contribution, const jz_btree *nodes,
    const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, const jz_btree *route_fields,
    bool *has, jazz_engine_SourceId *source, groove_GraphBuilder **graph);
jazz_engine_CapErr JE_CLO(join_contribution_membership_graph)(groove_GraphBuilder *visible_root, const jazz_engine_JoinContribution *contribution,
    const jazz_engine_ResolvedSource *root_source, const jazz_engine_ResolvedSource *contribution_source, const jz_btree *nodes,
    const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, const jz_btree *route_fields, groove_GraphBuilder **out);
jazz_engine_CapErr JE_CLO(inherited_contribution_membership_graph)(groove_GraphBuilder *visible_root, const jazz_engine_InheritedContribution *contribution,
    const jazz_engine_ResolvedSource *root_source, const jazz_engine_ResolvedSource *parent_source, const jz_btree *nodes,
    const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, const jz_btree *route_fields, groove_GraphBuilder **out);
jazz_engine_CapErr JE_CLO(flat_join_contribution_membership_graph)(groove_GraphBuilder *visible_root, const jazz_engine_JoinContribution *contribution,
    const jazz_engine_ResolvedSource *contribution_source, const jz_btree *nodes, const jz_btree *resolved_sources,
    jazz_engine_LoweringContext *request, const jz_btree *route_fields, groove_GraphBuilder **out);
groove_ProjectFieldVec JE_CLO(project_join_contribution_fields_with_root_routes)(const jazz_engine_ResolvedSource *source, const jz_btree *route_fields);

/* ================================================================ templates.rs */
#define JE_TPL(x) jazz_engine__query_engine__lowering__templates__##x
typedef struct jazz_engine_QueryProgramTemplateCache {
    jz_deque entries; /* VecDeque<TemplateEntry> (entry layout private to templates.c) */
    groove_TypedGraphTemplateCache *physical;
} jazz_engine_QueryProgramTemplateCache;
void JE_TPL(QueryProgramTemplateCache__Default__default)(jazz_engine_QueryProgramTemplateCache *c);
void JE_TPL(QueryProgramTemplateCache__drop)(jazz_engine_QueryProgramTemplateCache *c);
void JE_TPL(QueryProgramTemplateCache__clear)(jazz_engine_QueryProgramTemplateCache *c);
/* consumes compilation, sources and explain; describe is `impl Fn(GraphBuilder) -> Result<TemplateGraphInput, _>` */
jazz_engine_CapErr JE_TPL(QueryProgramTemplateCache__lower)(jazz_engine_QueryProgramTemplateCache *self, jazz_engine_QueryProgramCompilation compilation,
    jz_btree sources, jazz_engine_ExplainPlan explain, groove_TemplateDescribeFn describe, void *describe_ctx, jazz_engine_QueryProgram *out);

/* ================================================================ graph_lowering.rs: further shared items */
#define JAZZ_ENGINE_FIXPOINT_MAX_ITERS 128u
jazz_engine_UrErr JE_GL(lower_relation_input)(const jazz_engine_RelationInputPlan *plan, const jz_btree *resolved_sources,
    jazz_engine_LoweringContext *request, jazz_engine_LoweredRelationInput *out);
jazz_engine_UrErr JE_GL(lower_correlated_path_relation_graph)(const jazz_engine_CorrelatedPathPlan *path, const jazz_engine_ResolvedSource *root_source,
    const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, jazz_engine_LoweredRelationInput *out);
jazz_engine_UrErr JE_GL(lower_linear_plan_steps)(groove_GraphBuilder *graph, const jazz_engine_LinearCurrentRoot *plan, const jazz_engine_ResolvedSource *root_source,
    const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, jazz_engine_LoweredRelationInput *out);
groove_GraphBuilder *JE_GL(policy_join_if_needed)(groove_GraphBuilder *left, groove_GraphBuilder *right, jazz_engine_StrVec left_on,
    jazz_engine_StrVec right_on, jazz_engine_LoweringContext *request);
jazz_engine_UrErr JE_GL(lower_path_key_pair)(const jazz_engine_PredicateExpr *predicate, const jazz_engine_SourceId *parent_source_id,
    const jazz_engine_ResolvedSource *parent_source, const jazz_engine_SourceId *child_source_id, const jazz_engine_ResolvedSource *child_source,
    jazz_engine_LoweringContext *request, jz_str *parent_key, jz_str *child_key);
/* resolved_source_descriptor_index: false = None */
bool JE_GL(resolved_source_descriptor_index)(const jazz_engine_ResolvedSource *source, jz_slice field, u32 *out);
groove_LiteralValue JE_GL(coerce_literal_for_value_type)(groove_LiteralValue value, const groove_ValueType *value_type);
jazz_engine_UrErr JE_GL(require_source_field)(const jazz_engine_ResolvedSource *source, jz_slice field, jz_str *out);
bool JE_GL(has_explicit_closure_path)(const jazz_engine_NormalizedRowSetShape *shape);
/* terminals.rs helpers used by graph lowering */
groove_ProjectFieldVec JE_TERM(project_source_fields_from_prefix_rewrapping_nullable)(const jazz_engine_ResolvedSource *source, const char *prefix,
    const jz_str *nullable_field /* NULL = None */);
jazz_engine_UrErr JE_TERM(route_literal_project_field)(jz_slice route_field, jazz_engine_LoweringContext *request, groove_ProjectField *out);
/* NULL = None */
const groove_DescriptorField *JE_TERM(collect_window_source_field)(const jazz_engine_ResolvedSource *source, const jazz_engine_NormalizedValueRef *value);

#endif
