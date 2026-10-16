/* lowering.rs: one lowering pass from an analyzed request to Groove graphs, plus
 * the parameter-domain and graph-structure helpers its submodules share. */
#include "lowering.h"


#define F(x) jazz_engine__query_engine__fields__##x

/* ================================================================ ParameterDomain */
void JE_LOW(ParameterDomain__Default__default)(jazz_engine_ParameterDomain *d) {
    jz_btree_init(&d->user_params, &jazz_engine__STR_VT_MAP_DESC);
    jz_btree_init(&d->claim_params, &jazz_engine__STR_CLAIM_PARAM_MAP_DESC);
    jz_btree_init(&d->routing_params, &jazz_engine__STRSET_DESC);
}
void JE_LOW(ParameterDomain__Clone__clone)(jazz_engine_ParameterDomain *dst, const jazz_engine_ParameterDomain *src) {
    jazz_engine__strvtmap_clone(&dst->user_params, &src->user_params);
    jz_btree_clone(&dst->claim_params, &src->claim_params, jazz_engine__str_clone_v, jazz_engine__claim_param_clone_v);
    jazz_engine__strset_clone(&dst->routing_params, &src->routing_params);
}
void JE_LOW(ParameterDomain__drop)(jazz_engine_ParameterDomain *d) {
    jz_btree_drop(&d->user_params);
    jz_btree_drop(&d->claim_params);
    jz_btree_drop(&d->routing_params);
}
static bool claim_path_eq(const jazz_engine_ClaimPath *a, const jazz_engine_ClaimPath *b) { return jazz_engine__strvec_eq(&a->segments, &b->segments); }
bool jazz_engine__claim_parameter_eq(const jazz_engine_ClaimParameter *a, const jazz_engine_ClaimParameter *b) {
    return claim_path_eq(&a->path, &b->path) && groove__records__values__ValueType__PartialEq__eq(&a->ty, &b->ty);
}
static bool claim_parameter_eq_v(const void *a, const void *b) { return jazz_engine__claim_parameter_eq(a, b); }
bool JE_LOW(ParameterDomain__PartialEq__eq)(const jazz_engine_ParameterDomain *a, const jazz_engine_ParameterDomain *b) {
    return jz_btree_eq(&a->user_params, &b->user_params, jazz_engine__vt_eq_v)
           && jz_btree_eq(&a->claim_params, &b->claim_params, claim_parameter_eq_v)
           && jz_btree_eq(&a->routing_params, &b->routing_params, NULL);
}
static void param_domain_drop_v(void *d) { JE_LOW(ParameterDomain__drop)(d); }
const jz_btree_desc jazz_engine__U32_PARAM_DOMAIN_MAP_DESC = {sizeof(u32), sizeof(jazz_engine_ParameterDomain), jz_cmp_u32, NULL, param_domain_drop_v};

/* ================================================================ container descriptors */
static int program_source_id_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const jazz_protocol_ProgramSourceId *x = a, *y = b;
    int c = jz_str_cmp(x->table, y->table);
    if (c) return c;
    return jz_slice_cmp(jz_slice_of(x->path.ptr, x->path.len), jz_slice_of(y->path.ptr, y->path.len));
}
static void program_source_id_drop_v(void *p) { jz_vec_drop(&((jazz_protocol_ProgramSourceId *)p)->path, 1, NULL); }
static void program_source_id_clone_v(void *d, const void *s) {
    jazz_protocol_ProgramSourceId *dst = d;
    const jazz_protocol_ProgramSourceId *src = s;
    dst->table = src->table;
    jz_vec_clone(&dst->path, &src->path, 1, NULL);
}
static void descriptor_copy_v(void *d, const void *s) { *(groove_RecordDescriptor *)d = *(const groove_RecordDescriptor *)s; }
const jz_btree_desc jazz_engine__PROGRAM_SOURCE_DESCRIPTOR_MAP_DESC = {
    sizeof(jazz_protocol_ProgramSourceId), sizeof(groove_RecordDescriptor), program_source_id_cmp, program_source_id_drop_v, NULL
};
static void table_schema_drop_v(void *t) { jazz_model__schema__TableSchema__drop(t); }
static void table_schema_clone_v(void *d, const void *s) { jazz_model__schema__TableSchema__Clone__clone(d, s); }
const jz_btree_desc jazz_engine__STR_TABLE_SCHEMA_MAP_DESC = {sizeof(jz_str), sizeof(jazz_model_TableSchema), jz_cmp_str, jazz_engine__str_drop_v, table_schema_drop_v};
const jz_btree_desc jazz_engine__SOURCE_ID_GRAPH_MAP_DESC = {
    sizeof(jazz_engine_SourceId), sizeof(groove_GraphBuilder *), jazz_engine__sid_cmp_v, jazz_engine__sid_drop_v, groove__ivm__graph__GraphBuilder__drop_v
};

/* ================================================================ terminals / programs: derives */
void JE_LOW(LoweredTerminal__Clone__clone)(jazz_engine_LoweredTerminal *dst, const jazz_engine_LoweredTerminal *src) {
    dst->sink = jz_str_clone(&src->sink);
    dst->graph = groove_gb_clone(src->graph);
    JE_SCH(OutputTerminalSchema__Clone__clone)(&dst->output, &src->output);
}
void JE_LOW(LoweredTerminal__drop)(jazz_engine_LoweredTerminal *t) {
    jz_str_drop(&t->sink);
    groove_gb_drop(t->graph);
    JE_SCH(OutputTerminalSchema__drop)(&t->output);
}
void jazz_engine__terminal_drop_v(void *t) { JE_LOW(LoweredTerminal__drop)(t); }
void jazz_engine__terminal_clone_v(void *d, const void *s) { JE_LOW(LoweredTerminal__Clone__clone)(d, s); }

void JE_LOW(LoweredGraph__Clone__clone)(jazz_engine_LoweredGraph *dst, const jazz_engine_LoweredGraph *src) {
    jz_vec_clone(&dst->terminals, &src->terminals, sizeof(jazz_engine_LoweredTerminal), jazz_engine__terminal_clone_v);
    jz_btree_clone(&dst->shared_witness_sinks, &src->shared_witness_sinks, jazz_engine__str_clone_v, jazz_engine__str_clone_v);
    dst->internal_app_rows_graph = src->internal_app_rows_graph ? groove_gb_clone(src->internal_app_rows_graph) : NULL;
    JE_LOW(ParameterDomain__Clone__clone)(&dst->parameters, &src->parameters);
    jz_vec_clone(&dst->output.row_set, &src->output.row_set, sizeof(jazz_engine_OutputTerminalSchema), jazz_engine__ots_clone_v);
    jz_btree_clone(&dst->maintained_terminal_tables, &src->maintained_terminal_tables, jazz_engine__str_clone_v, table_schema_clone_v);
    jazz_engine__strset_clone(&dst->targeted_refresh_tables, &src->targeted_refresh_tables);
    dst->targeted_refresh_uncertain = src->targeted_refresh_uncertain;
}
void JE_LOW(LoweredGraph__drop)(jazz_engine_LoweredGraph *g) {
    jz_vec_drop(&g->terminals, sizeof(jazz_engine_LoweredTerminal), jazz_engine__terminal_drop_v);
    jz_btree_drop(&g->shared_witness_sinks);
    groove_gb_drop(g->internal_app_rows_graph);
    JE_LOW(ParameterDomain__drop)(&g->parameters);
    JE_SCH(ProgramOutputSchemas__drop)(&g->output);
    jz_btree_drop(&g->maintained_terminal_tables);
    jz_btree_drop(&g->targeted_refresh_tables);
}
/* LoweredGraph::execution_terminals() filter */
bool JE_LOW(LoweredGraph__is_execution_terminal)(const jazz_engine_LoweredGraph *g, const jazz_engine_LoweredTerminal *t) {
    return !jz_btree_contains(&g->shared_witness_sinks, &t->sink);
}
void JE_LOW(QueryProgram__Clone__clone)(jazz_engine_QueryProgram *dst, const jazz_engine_QueryProgram *src) {
    JE_IN(QueryProgramRequest__Clone__clone)(&dst->request, &src->request);
    JE_LOW(LoweredGraph__Clone__clone)(&dst->lowered, &src->lowered);
    jz_btree_clone(&dst->source_descriptors, &src->source_descriptors, program_source_id_clone_v, descriptor_copy_v);
    jz_btree_clone(&dst->covered_input_source_descriptors, &src->covered_input_source_descriptors, program_source_id_clone_v, descriptor_copy_v);
    JE_RES(ExplainPlan__Clone__clone)(&dst->explain, &src->explain);
}
void JE_LOW(QueryProgram__drop)(jazz_engine_QueryProgram *p) {
    JE_IN(QueryProgramRequest__drop)(&p->request);
    JE_LOW(LoweredGraph__drop)(&p->lowered);
    jz_btree_drop(&p->source_descriptors);
    jz_btree_drop(&p->covered_input_source_descriptors);
    JE_RES(ExplainPlan__drop)(&p->explain);
}
void JE_LOW(QueryProgramCompilation__Clone__clone)(jazz_engine_QueryProgramCompilation *dst, const jazz_engine_QueryProgramCompilation *src) {
    JE_IN(QueryProgramRequest__Clone__clone)(&dst->request, &src->request);
    dst->plan = JZ_NEW(jazz_engine_AnalyzedQueryPlan);
    JE_PLAN(AnalyzedQueryPlan__Clone__clone)(dst->plan, src->plan);
    jz_vec_clone(&dst->sources, &src->sources, sizeof(jazz_engine_SourceRequest), jazz_engine__source_request_clone_v);
}
void JE_LOW(QueryProgramCompilation__drop)(jazz_engine_QueryProgramCompilation *c) {
    JE_IN(QueryProgramRequest__drop)(&c->request);
    if (c->plan) {
        JE_PLAN(AnalyzedQueryPlan__drop)(c->plan);
        jz_free(c->plan);
    }
    jz_vec_drop(&c->sources, sizeof(jazz_engine_SourceRequest), jazz_engine__source_request_drop_v);
}

/* ================================================================ reports */
static jazz_engine_CapErr report_with(jazz_engine_UnsupportedReasonVec gaps, jazz_engine_ExplainPlan explain) {
    jazz_engine_CapabilityReport *r = JZ_NEW(jazz_engine_CapabilityReport);
    r->gaps = gaps;
    r->explain = explain;
    return r;
}
static jazz_engine_CapErr report_one(jazz_engine_UnsupportedReason gap, jazz_engine_ExplainPlan explain) {
    jazz_engine_UnsupportedReasonVec gaps = {0};
    JZ_VEC_PUSH(gaps, gap);
    return report_with(gaps, explain);
}
static jazz_engine_ExplainPlan explain_clone(const jazz_engine_ExplainPlan *e) {
    jazz_engine_ExplainPlan c;
    JE_RES(ExplainPlan__Clone__clone)(&c, e);
    return c;
}
jazz_engine_CapErr JE_LOW(single_gap_report)(jazz_engine_UnsupportedReason gap) {
    jazz_engine_ExplainPlan explain;
    memset(&explain, 0, sizeof explain);
    return report_one(gap, explain);
}

jazz_engine_ExplainPlan JE_LOW(explain_with_request)(const jazz_engine_QueryProgramRequest *request, jazz_engine_ExplainPlan explain) {
    jz_str_drop(&explain.input);
    memset(&explain.input, 0, sizeof explain.input);
    JE_IN(RowSetProgramInput__Debug__fmt)(&explain.input, &request->input);
    jz_str s = {0};
    JE_READ(QueryReadSet__Debug__fmt)(&s, &request->reads);
    jz_vec_insert(&explain.read, 0, &s, sizeof s);
    jz_str p = {0};
    JE_POL(PolicyContext__Debug__fmt)(&p, &request->policy);
    jz_vec_insert(&explain.policy, 0, &p, sizeof p);
    jz_str o = {0};
    JE_OUT(RowSetOutputRequest__Debug__fmt)(&o, &request->output);
    jz_vec_insert(&explain.output, 0, &o, sizeof o);
    return explain;
}

/* ================================================================ source requests */
static jazz_engine_CapErr JE_LOW(binding_user_param_types)(const jazz_engine_ProgramBinding *binding, jz_btree *out) {
    jazz_engine__strvtmap_clone(out, &binding->extra_user_params);
    jz_str *name; void *unused;
    JZ_BTREE_FOR(&binding->values, name, unused) {
        if (jz_btree_contains(&binding->claim_params, name)) continue;
        const groove_ValueType *ty = jz_btree_get(&binding->param_types, name);
        if (!ty) {
            jz_btree_drop(out);
            jazz_engine_UnsupportedReason r = jazz_engine__ur_new(JAZZ_ENGINE_UR_RUNTIME);
            r.message = jz_format("binding parameter '%.*s' is missing a validated type", JE_STR_ARG(*name));
            return JE_LOW(single_gap_report)(r);
        }
        jz_str k = jz_str_clone(name);
        groove_ValueType v = groove_vt_clone(ty);
        jazz_engine__btree_put(out, &k, &v);
    }
    return NULL;
}

static void covered_input_trace(const jazz_engine_SourceId *source, const jazz_engine_PolicyContext *policy, const char *mode,
                                const jazz_engine_SourceAuthorizationRequest *authorization) {
    jz_str s = {0}, p = {0}, a = {0};
    sid_debug(&s, source);
    JE_POL(PolicyContext__Debug__fmt)(&p, policy);
    if (authorization) JE_RES(SourceAuthorizationRequest__Debug__fmt)(&a, authorization);
    else jz_str_push_cstr(&a, "system");
    jz_trace_event(0, "jazz_engine",
                   "JAZZ_COVERED_INPUT_TRACE stage=source_authorization mode=%s source=%.*s policy=%.*s authorization=%.*s",
                   mode, JE_STR_ARG(s), JE_STR_ARG(p), JE_STR_ARG(a));
    jz_str_drop(&s);
    jz_str_drop(&p);
    jz_str_drop(&a);
}

static jazz_engine_CapErr JE_LOW(source_authorization_for_source)(const jazz_engine_QueryProgramRequest *request, const jazz_engine_SourceId *source,
                                                                  jazz_engine_SourceAuthorizationRequest *out) {
    memset(out, 0, sizeof *out);
    /* Client-local results are scoped by the upstream emission boundary. */
    if (request->authorization_mode == JAZZ_ENGINE_AUTH_CLIENT_LOCAL) {
        if (jazz_types__debug_env__covered_input_trace()) covered_input_trace(source, &request->policy, "client_local", NULL);
        out->tag = JAZZ_ENGINE_SAR_SYSTEM;
        return NULL;
    }
    const jazz_engine_PolicyContext *policy = &request->policy;
    switch (policy->tag) {
    case JAZZ_ENGINE_PCTX_SYSTEM: out->tag = JAZZ_ENGINE_SAR_SYSTEM; break;
    case JAZZ_ENGINE_PCTX_AUTHORIZATION_SUBPLAN:
        /* the protected source itself, and auxiliary closure/payload sources, read under system authority */
        out->tag = JAZZ_ENGINE_SAR_SYSTEM;
        break;
    case JAZZ_ENGINE_PCTX_IDENTITY: {
        out->tag = JAZZ_ENGINE_SAR_POLICY_FILTERED;
        out->permission_subject = policy->permission_subject;
        jazz_engine_PolicyAuthorizationPlan *plan = &out->plan;
        JE_CAP_TRY(JE_LOW(binding_user_param_types)(&request->input.binding, &plan->binding_user_params));
        sid_clone(&plan->protected_source, source);
        plan->role = JAZZ_ENGINE_PDR_READ;
        plan->protected_row_field = jz_str_from("row_uuid");
        plan->has_binding_source_shape = request->input.binding.has_source_shape;
        if (plan->has_binding_source_shape) plan->binding_source_shape = jz_str_clone(&request->input.binding.source_shape);
        jz_btree_clone(&plan->binding_claim_params, &request->input.binding.claim_params, jazz_engine__str_clone_v, jazz_engine__claim_param_clone_v);
        break;
    }
    }
    if (jazz_types__debug_env__covered_input_trace()) covered_input_trace(source, &request->policy, "trusted_serving", out);
    return NULL;
}

static jazz_engine_CapErr JE_LOW(source_requests_for_plan)(const jazz_engine_QueryProgramRequest *request, const jazz_engine_AnalyzedQueryPlan *plan,
                                                           jazz_engine_SourceRequestVec *out) {
    memset(out, 0, sizeof *out);
    jz_btree visibilities, requirements;
    JE_PLAN(source_visibilities)(plan, &visibilities);
    jazz_engine_CapErr err = JE_REQ(source_requirements)(request, plan, &requirements);
    if (err) { jz_btree_drop(&visibilities); return err; }
    jazz_engine_SourceId source;
    jazz_engine_SourceRequirements reqs;
    while (jz_btree_pop_first(&requirements, &source, &reqs)) {
        jazz_engine_SourceRequest r;
        const u8 *vis = jz_btree_get(&visibilities, &source);
        r.visibility = vis ? *vis : JAZZ_ENGINE_VIS_VISIBLE;
        err = JE_LOW(source_authorization_for_source)(request, &source, &r.authorization);
        if (err) {
            sid_drop(&source);
            JE_RES(SourceRequirements__drop)(&reqs);
            jz_vec_drop(out, sizeof(jazz_engine_SourceRequest), jazz_engine__source_request_drop_v);
            break;
        }
        r.source = source;
        r.requirements = reqs;
        JZ_VEC_PUSH(*out, r);
    }
    jz_btree_drop(&requirements);
    jz_btree_drop(&visibilities);
    return err;
}

jazz_engine_CapErr JE_LOW(QueryProgramCompilation__analyze)(jazz_engine_QueryProgramRequest request, jazz_engine_QueryProgramCompilation *out) {
    jazz_engine_AnalyzedQueryPlan plan;
    jazz_engine_UnsupportedReasonVec gaps;
    if (!JE_PLAN(analyze_query_plan)(&request, &plan, &gaps)) {
        jazz_engine_ExplainPlan explain;
        memset(&explain, 0, sizeof explain);
        jazz_engine__strvec_push_cstr(&explain.capabilities, "only current-source row-set lowering is implemented");
        jazz_engine_CapErr e = report_with(gaps, JE_LOW(explain_with_request)(&request, explain));
        JE_IN(QueryProgramRequest__drop)(&request);
        return e;
    }
    jazz_engine_SourceRequestVec sources;
    jazz_engine_CapErr err = JE_LOW(source_requests_for_plan)(&request, &plan, &sources);
    if (err) {
        JE_PLAN(AnalyzedQueryPlan__drop)(&plan);
        JE_IN(QueryProgramRequest__drop)(&request);
        return err;
    }
    out->request = request;
    out->plan = jz_memdup(&plan, sizeof plan);
    out->sources = sources;
    return NULL;
}

/* Analyze the logical source requests for one program without preparing any source. */
jazz_engine_CapErr JE_LOW(query_program_source_requests)(const jazz_engine_QueryProgramRequest *request, jazz_engine_SourceRequestVec *out) {
    jazz_engine_AnalyzedQueryPlan plan;
    jazz_engine_UnsupportedReasonVec gaps;
    if (!JE_PLAN(analyze_query_plan)(request, &plan, &gaps)) {
        jazz_engine_ExplainPlan explain;
        memset(&explain, 0, sizeof explain);
        return report_with(gaps, JE_LOW(explain_with_request)(request, explain));
    }
    jazz_engine_CapErr err = JE_LOW(source_requests_for_plan)(request, &plan, out);
    JE_PLAN(AnalyzedQueryPlan__drop)(&plan);
    return err;
}

/* The policy-filtered include-deleted sibling, only for a visible source that requested
 * an exact deletion-register witness. */
bool JE_LOW(authorized_deletion_preimage_source_request)(const jazz_engine_SourceRequest *request, jazz_engine_SourceRequest *out) {
    jazz_engine_SourceMetadataRequirement deletion = jazz_engine__smr_simple(JAZZ_ENGINE_SMR_DELETION_MARKERS);
    if (request->visibility != JAZZ_ENGINE_VIS_VISIBLE || !jz_btree_contains(&request->requirements.metadata, &deletion)) return false;
    JE_RES(SourceRequest__Clone__clone)(out, request);
    out->visibility = JAZZ_ENGINE_VIS_INCLUDE_DELETED;
    /* The sibling supplies only the policy-filtered preimage. */
    jz_btree_clear(&out->requirements.metadata);
    return true;
}

/* ================================================================ preparation (async -> synchronous) */
static jazz_engine_UnsupportedReason source_gap_reason(jazz_engine_SourceResolutionError *err, jz_str *request_debug) {
    jazz_engine_UnsupportedReason r = jazz_engine__ur_new(JAZZ_ENGINE_UR_SOURCE);
    r.source = err->gap; /* moved */
    memset(&err->gap, 0, sizeof err->gap);
    memset(request_debug, 0, sizeof *request_debug);
    /* {:#?} is rendered with the compact Debug form */
    if (err->request) JE_RES(SourceRequest__Debug__fmt)(request_debug, err->request);
    JE_RES(SourceResolutionError__drop)(err);
    return r;
}

jazz_engine_CapErr JE_LOW(prepare_query_program_sources)(const jazz_engine_QueryProgramCompilation *compilation, jazz_engine_SourceGraphPreparer *preparer,
                                                        jz_btree *resolved_sources, jazz_engine_ExplainPlan *explain_out) {
    jazz_engine_ExplainPlan explain;
    memset(&explain, 0, sizeof explain);
    const jazz_engine_QueryProgramRequest *request = &compilation->request;
    jz_btree_init(resolved_sources, &jazz_engine__SOURCE_ID_RESOLVED_MAP_DESC);
    JZ_VEC_FOR(const jazz_engine_SourceRequest, source_request, compilation->sources) {
        jazz_engine_ResolvedSource resolved;
        jazz_engine_SourceResolutionError err;
        memset(&resolved, 0, sizeof resolved);
        if (!preparer->prepare_source_graph(preparer->self, source_request, &resolved, &err)) {
            jz_str dbg;
            jazz_engine_UnsupportedReason gap = source_gap_reason(&err, &dbg);
            jazz_engine_ExplainPlan failure = explain_clone(&explain);
            JZ_VEC_PUSH(failure.read, jz_format("failed source request: %.*s", JE_STR_ARG(dbg)));
            jz_str_drop(&dbg);
            JE_RES(ExplainPlan__drop)(&explain);
            jz_btree_drop(resolved_sources);
            return report_one(gap, JE_LOW(explain_with_request)(request, failure));
        }
        jazz_engine_SourceRequest preimage_request;
        if (resolved.has_deletion_register && JE_LOW(authorized_deletion_preimage_source_request)(source_request, &preimage_request)) {
            jazz_engine_ResolvedSource preimage;
            memset(&preimage, 0, sizeof preimage);
            bool ok = preparer->prepare_source_graph(preparer->self, &preimage_request, &preimage, &err);
            JE_RES(SourceRequest__drop)(&preimage_request);
            if (!ok) {
                jz_str dbg;
                jazz_engine_UnsupportedReason gap = source_gap_reason(&err, &dbg);
                jazz_engine_ExplainPlan failure;
                memset(&failure, 0, sizeof failure);
                JZ_VEC_PUSH(failure.read, jz_format("failed authorized deletion preimage request: %.*s", JE_STR_ARG(dbg)));
                jz_str_drop(&dbg);
                JE_RES(ResolvedSource__drop)(&resolved);
                JE_RES(ExplainPlan__drop)(&explain);
                jz_btree_drop(resolved_sources);
                return report_one(gap, JE_LOW(explain_with_request)(request, failure));
            }
            if (resolved.has_authorized_deletion_preimage) {
                groove_gb_drop(resolved.authorized_deletion_preimage.graph);
                jz_btree_drop(&resolved.authorized_deletion_preimage.routing_fields);
            }
            resolved.has_authorized_deletion_preimage = true;
            resolved.authorized_deletion_preimage.graph = preimage.graph;
            resolved.authorized_deletion_preimage.routing_fields = preimage.routing_fields;
            preimage.graph = NULL;
            jz_btree_init(&preimage.routing_fields, &jazz_engine__STRSET_DESC);
            JE_RES(ResolvedSource__drop)(&preimage);
        }
        /* A receiver-local maintained subscription may replace an authority-approved source
         * closure through runtime-owned input sources (mapped back by ProgramSourceId). */
        jz_str s = {0}, tier = {0};
        sid_debug(&s, &source_request->source);
        u8 t;
        if (JE_PLAN(source_current_tier)(request, &source_request->source, &t)) {
            jz_str_push_cstr(&tier, "Some(");
            jazz_engine__tier_debug(&tier, t);
            jz_str_push_char(&tier, ')');
        } else {
            jz_str_push_cstr(&tier, "None");
        }
        JZ_VEC_PUSH(explain.physical, jz_format("source %.*s (%.*s) -> resolved table %.*s", JE_STR_ARG(s), JE_STR_ARG(tier),
                                                JE_STR_ARG(resolved.table_schema->name)));
        jz_str_drop(&s);
        jz_str_drop(&tier);
        jazz_engine_SourceId key = jazz_engine__sid_cloned(&source_request->source);
        jazz_engine__btree_put(resolved_sources, &key, &resolved);
    }
    *explain_out = explain;
    return NULL;
}

/* Prepare concrete source descriptions, then synchronously lower the program. */
jazz_engine_CapErr JE_LOW(prepare_and_lower_query_program)(jazz_engine_QueryProgramCompilation compilation, jazz_engine_SourceGraphPreparer *preparer,
                                                          jazz_engine_QueryProgram *out) {
    jz_btree sources;
    jazz_engine_ExplainPlan explain;
    jazz_engine_CapErr err = JE_LOW(prepare_query_program_sources)(&compilation, preparer, &sources, &explain);
    if (err) {
        JE_LOW(QueryProgramCompilation__drop)(&compilation);
        return err;
    }
    return JE_LOW(lower_resolved_query_program)(compilation, sources, explain, out);
}

/* Purely lower a request whose Groove sources have already been prepared. */
jazz_engine_CapErr JE_LOW(lower_resolved_query_program)(jazz_engine_QueryProgramCompilation compilation, jz_btree resolved_sources,
                                                       jazz_engine_ExplainPlan explain, jazz_engine_QueryProgram *out) {
    jz_btree empty;
    jz_btree_init(&empty, &jazz_engine__U32_PARAM_DOMAIN_MAP_DESC);
    jazz_engine_CapErr err = JE_LOW(lower_resolved_query_program_with_source_parameters)(compilation, resolved_sources, explain, &empty, NULL, out);
    jz_btree_drop(&empty);
    return err;
}

/* ================================================================ routed-output verification */
typedef struct { bool some; jz_btree set; } opt_strset;
static void opt_strset_drop_v(void *p) {
    opt_strset *o = p;
    if (o->some) jz_btree_drop(&o->set);
}
static const jz_hmap_desc DECLARED_FIELDS_DESC = {sizeof(void *), sizeof(opt_strset), jazz_engine__ptr_hash_v, jazz_engine__ptr_eq_v, NULL, opt_strset_drop_v};

/* Declared output fields per builder node, memoized by node address. */
typedef struct { jz_hmap outputs; } jazz_engine_DeclaredOutputFields;
static void declared_fields_init(jazz_engine_DeclaredOutputFields *d) { jz_hmap_init(&d->outputs, &DECLARED_FIELDS_DESC); }
static void declared_fields_drop(jazz_engine_DeclaredOutputFields *d) { jz_hmap_drop(&d->outputs); }

static bool JE_LOW(descriptor_named_fields)(groove_RecordDescriptor descriptor, jz_btree *out) {
    u32 n;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(descriptor, &n);
    jz_btree_init(out, &jazz_engine__STRSET_DESC);
    for (u32 i = 0; i < n; i++) {
        if (!fields[i].has_name) {
            jz_btree_drop(out);
            return false;
        }
        jazz_engine__strset_insert_clone(out, &fields[i].name);
    }
    return true;
}

static bool child_output(const jz_hmap *outputs, const groove_GraphBuilder *child, jz_btree *out) {
    const void *key = child;
    const opt_strset *o = jz_hmap_get(outputs, &key);
    if (!o) jz_panic("postorder visits graph children before their parent");
    if (!o->some) return false;
    jazz_engine__strset_clone(out, &o->set);
    return true;
}

static void strset_intersect(jz_btree *acc, const jz_btree *other) {
    jz_btree result;
    jz_btree_init(&result, &jazz_engine__STRSET_DESC);
    jz_str k;
    while (jz_btree_pop_first(acc, &k, NULL)) {
        if (jz_btree_contains(other, &k)) jz_btree_insert(&result, &k, NULL, NULL);
        else jz_str_drop(&k);
    }
    jz_btree_drop(acc);
    *acc = result;
}

static bool JE_LOW(declared_node_fields)(const groove_GraphBuilder *node, const jz_hmap *outputs, jz_btree *out) {
    switch (node->tag) {
    case GROOVE_GB_TEMPLATE_INPUT: return JE_LOW(descriptor_named_fields)(node->u.template_input.output, out);
    case GROOVE_GB_TYPED_TEMPLATE:
        return JE_LOW(descriptor_named_fields)(groove__ivm__template__TypedGraphTemplate__output_descriptor(node->u.typed_template.program), out);
    case GROOVE_GB_INLINE_RECORDS: return JE_LOW(descriptor_named_fields)(node->u.inline_records.output, out);
    case GROOVE_GB_FRONTIER_SOURCE: return JE_LOW(descriptor_named_fields)(node->u.frontier_source.output, out);
    case GROOVE_GB_BINDING_SOURCE: return JE_LOW(descriptor_named_fields)(node->u.binding_source.output, out);
    case GROOVE_GB_INPUT_SOURCE: return false;
    case GROOVE_GB_PROJECT:
        jz_btree_init(out, &jazz_engine__STRSET_DESC);
        JZ_VEC_FOR(const groove_ProjectField, f, node->u.project.fields) jazz_engine__strset_insert_clone(out, &f->output_name);
        return true;
    case GROOVE_GB_STREAMING_CHECKSUM: {
        if (!child_output(outputs, node->u.streaming_checksum.input, out)) return false;
        const groove_FieldRef *field = &node->u.streaming_checksum.field;
        if (field->tag == GROOVE_FIELDREF_RESOLVED) {
            jz_btree_drop(out);
            return false;
        }
        jz_btree_remove(out, &field->name, NULL, NULL);
        jazz_engine__strset_insert_clone(out, &node->u.streaming_checksum.output_field);
        return true;
    }
    case GROOVE_GB_AGGREGATE: {
        jz_btree_init(out, &jazz_engine__STRSET_DESC);
        JZ_VEC_FOR(const groove_FieldRef, f, node->u.aggregate.group_cols) {
            jz_str n = groove__ivm__graph__FieldRef__display_name(f);
            jazz_engine__btree_set_add(out, &n);
        }
        for (u32 i = 0; i < node->u.aggregate.aggregates.len; i++) {
            const groove_AggregateExpr *a = &node->u.aggregate.aggregates.ptr[i];
            jz_str n = a->has_output_name ? jz_str_clone(&a->output_name) : jz_format("aggregate_%u", i);
            jazz_engine__btree_set_add(out, &n);
        }
        return true;
    }
    case GROOVE_GB_COLLECT_BY: {
        jz_btree_init(out, &jazz_engine__STRSET_DESC);
        const groove_CollectByBuilder *c = node->u.collect_by.collect;
        JZ_VEC_FOR(const groove_CollectByField, f, c->parent_fields) jazz_engine__strset_insert_clone(out, &f->output_name);
        jazz_engine__strset_insert_clone(out, &c->collection_field);
        return true;
    }
    case GROOVE_GB_FILTER: return child_output(outputs, node->u.filter.input, out);
    case GROOVE_GB_UNWRAP_NULLABLE: return child_output(outputs, node->u.unwrap_nullable.input, out);
    case GROOVE_GB_VARIANT_PROJECT: return child_output(outputs, node->u.variant_project.input, out);
    case GROOVE_GB_ARG_MAX_BY:
    case GROOVE_GB_ARG_MIN_BY: return child_output(outputs, node->u.arg_by.input, out);
    case GROOVE_GB_TOP_BY: return child_output(outputs, node->u.top_by.input, out);
    case GROOVE_GB_SEMI_JOIN:
    case GROOVE_GB_ANTI_JOIN: return child_output(outputs, node->u.join.left, out);
    case GROOVE_GB_UNNEST:
        if (!child_output(outputs, node->u.unnest.input, out)) return false;
        jazz_engine__strset_insert_clone(out, &node->u.unnest.element_field);
        return true;
    case GROOVE_GB_RECURSIVE: return child_output(outputs, node->u.recursive.seed, out);
    case GROOVE_GB_RECURSIVE_STEP_WITNESS: {
        const groove_GraphBuilder *r = node->u.recursive_step_witness.recursive;
        if (r->tag == GROOVE_GB_RECURSIVE && r->u.recursive.step_witness) return child_output(outputs, r->u.recursive.step_witness, out);
        return false;
    }
    case GROOVE_GB_UNION: {
        const groove_GraphBuilderVec *inputs = &node->u.union_.inputs;
        if (!inputs->len) return false;
        if (!child_output(outputs, inputs->ptr[0], out)) return false;
        for (u32 i = 1; i < inputs->len; i++) {
            jz_btree other;
            if (!child_output(outputs, inputs->ptr[i], &other)) {
                jz_btree_drop(out);
                return false;
            }
            strset_intersect(out, &other);
            jz_btree_drop(&other);
        }
        return true;
    }
    case GROOVE_GB_JOIN: {
        jz_btree left, right;
        if (!child_output(outputs, node->u.join.left, &left)) return false;
        if (!child_output(outputs, node->u.join.right, &right)) {
            jz_btree_drop(&left);
            return false;
        }
        jz_btree_init(out, &jazz_engine__STRSET_DESC);
        jz_str *k; void *unused;
        JZ_BTREE_FOR(&left, k, unused) {
            jz_str f = F(left_field)(JZ_S(k));
            jazz_engine__btree_set_add(out, &f);
        }
        JZ_BTREE_FOR(&right, k, unused) {
            jz_str f = F(right_field)(JZ_S(k));
            jazz_engine__btree_set_add(out, &f);
        }
        jz_btree_drop(&left);
        jz_btree_drop(&right);
        return true;
    }
    case GROOVE_GB_TABLE:
    case GROOVE_GB_INDEX:
    case GROOVE_GB_LOOKUP:
    default:
        return false;
    }
}

static void declared_fields_derive(jazz_engine_DeclaredOutputFields *d, const groove_GraphBuilder *graph) {
    jazz_engine_GraphRefVec order;
    JE_LOW(graph_builder_postorder)(graph, &order);
    for (u32 i = 0; i < order.len; i++) {
        const void *key = order.ptr[i];
        if (jz_hmap_get(&d->outputs, &key)) continue;
        opt_strset v;
        v.some = JE_LOW(declared_node_fields)(order.ptr[i], &d->outputs, &v.set);
        jz_hmap_insert(&d->outputs, &key, &v, NULL);
    }
    jz_vec_drop(&order, sizeof(void *), NULL);
}

/* -> the declared fields of graph, NULL = None (borrowed from the memo) */
static const jz_btree *declared_fields_of(jazz_engine_DeclaredOutputFields *d, const groove_GraphBuilder *graph) {
    /* Iterative: policy lowering can build deeply nested finite graphs. */
    const void *key = graph;
    if (!jz_hmap_get(&d->outputs, &key)) declared_fields_derive(d, graph);
    const opt_strset *o = jz_hmap_get(&d->outputs, &key);
    if (!o) jz_panic("the graph root was derived");
    return o->some ? &o->set : NULL;
}

bool JE_LOW(graph_declared_output_fields)(const groove_GraphBuilder *graph, jz_btree *out) {
    jazz_engine_DeclaredOutputFields d;
    declared_fields_init(&d);
    const jz_btree *fields = declared_fields_of(&d, graph);
    bool some = fields != NULL;
    if (some) jazz_engine__strset_clone(out, fields);
    declared_fields_drop(&d);
    return some;
}

static void JE_LOW(terminal_schema_routing_fields)(const jazz_engine_OutputTerminalSchema *output, const jz_btree *routing_params, jz_btree *out) {
    if (output->tag == JAZZ_ENGINE_OTS_APP_ROWS) {
        jz_btree_init(out, &jazz_engine__STRSET_DESC);
        jz_str *k; void *unused;
        JZ_BTREE_FOR(&output->u.app_rows.hidden_fields, k, unused)
            if (jz_btree_contains(routing_params, k)) jazz_engine__strset_insert_clone(out, k);
        return;
    }
    JE_TERM(output_routing_fields)(&output->u.fact, out);
}

static jazz_engine_CapErr JE_LOW(verify_routed_terminal_outputs)(const jazz_engine_LoweredTerminalVec *terminals, const jazz_engine_ParameterDomain *parameters,
                                                                 const jazz_engine_QueryProgramRequest *request, const jazz_engine_ExplainPlan *explain,
                                                                 jazz_engine_DeclaredOutputFields *declared_fields) {
    JZ_VEC_FOR(const jazz_engine_LoweredTerminal, terminal, *terminals) {
        jz_btree expected;
        JE_LOW(terminal_schema_routing_fields)(&terminal->output, &parameters->routing_params, &expected);
        if (expected.len == 0) {
            jz_btree_drop(&expected);
            continue;
        }
        const jz_btree *actual = declared_fields_of(declared_fields, terminal->graph);
        if (!actual) {
            jz_btree_drop(&expected);
            jazz_engine_UnsupportedReason r = jazz_engine__ur_new(JAZZ_ENGINE_UR_RUNTIME);
            r.message = jz_format("routed terminal '%.*s' output fields could not be verified", JE_STR_ARG(terminal->sink));
            return report_one(r, JE_LOW(explain_with_request)(request, explain_clone(explain)));
        }
        jz_str *field; void *unused;
        JZ_BTREE_FOR(&expected, field, unused) {
            if (jz_btree_contains(actual, field)) continue;
            jazz_engine_UnsupportedReason r = jazz_engine__ur_new(JAZZ_ENGINE_UR_RUNTIME);
            r.message = jz_format("routed terminal '%.*s' is missing route field '%.*s'", JE_STR_ARG(terminal->sink), JE_STR_ARG(*field));
            jz_btree_drop(&expected);
            return report_one(r, JE_LOW(explain_with_request)(request, explain_clone(explain)));
        }
        jz_btree_drop(&expected);
    }
    return NULL;
}

static jazz_engine_CapErr JE_LOW(covered_input_source_descriptors)(const jazz_engine_LoweredTerminalVec *terminals, jz_btree *out) {
    jz_btree_init(out, &jazz_engine__PROGRAM_SOURCE_DESCRIPTOR_MAP_DESC);
    JZ_VEC_FOR(const jazz_engine_LoweredTerminal, terminal, *terminals) {
        if (terminal->output.tag != JAZZ_ENGINE_OTS_FACT) continue;
        const jazz_engine_ProgramFactOutput *fact = &terminal->output.u.fact;
        if (fact->key.tag != JAZZ_ENGINE_FACT_VERSION_WITNESSES || fact->schema.tag != JAZZ_ENGINE_PFS_VERSION_WITNESSES) continue;
        const jazz_engine_VersionWitnessSchemas *schema = &fact->schema.u.witnesses;
        if (!schema->has_content) continue;
        const jazz_engine_VersionWitnessSchema *witness = &schema->content;
        groove_RecordDescriptor *existing = jz_btree_get(out, &witness->source);
        bool conflict = existing && *existing != witness->descriptor;
        if (existing) {
            *existing = witness->descriptor;
        } else {
            jazz_protocol_ProgramSourceId k;
            program_source_id_clone_v(&k, &witness->source);
            jz_btree_insert(out, &k, &witness->descriptor, NULL);
        }
        if (conflict) {
            jz_btree_drop(out);
            jazz_engine_UnsupportedReason r = jazz_engine__ur_new(JAZZ_ENGINE_UR_RUNTIME);
            r.message = jz_str_from("covered input source has conflicting compiled descriptors");
            return JE_LOW(single_gap_report)(r);
        }
    }
    return NULL;
}

/* ================================================================ the lowering pass */
typedef struct { const jazz_engine_ParameterDomain *domain; } routing_keep_ctx;
static bool keep_routing_param(const void *k, void *v, void *c) {
    (void)v;
    const jazz_engine_ParameterDomain *p = ((routing_keep_ctx *)c)->domain;
    const jz_str *field = k;
    jz_slice param;
    if (F(route_param_from_field)(JZ_S(field), &param)) {
        jz_str view = jazz_engine__str_view(param);
        if (jz_btree_contains(&p->user_params, &view)) return true;
    }
    jazz_engine_ClaimPath path;
    if (F(claim_path_from_param_field)(JZ_S(field), &path)) {
        JE_IN(ClaimPath__drop)(&path);
        if (jz_btree_contains(&p->claim_params, field)) return true;
    }
    return false;
}
static void retain_routable_params(jazz_engine_ParameterDomain *parameters) {
    routing_keep_ctx c = {parameters};
    jz_btree_retain(&parameters->routing_params, keep_routing_param, &c);
}

jazz_engine_CapErr JE_LOW(lower_resolved_query_program_with_source_parameters)(jazz_engine_QueryProgramCompilation compilation, jz_btree resolved_sources,
    jazz_engine_ExplainPlan explain, const jz_btree *source_parameters, jazz_engine_ProgramArgumentRecipes *arguments, jazz_engine_QueryProgram *out) {
    jazz_engine_QueryProgramRequest request = compilation.request;
    jazz_engine_AnalyzedQueryPlan *plan = compilation.plan;
    jz_vec_drop(&compilation.sources, sizeof(jazz_engine_SourceRequest), jazz_engine__source_request_drop_v);

    jazz_engine_LoweringContext context;
    if (arguments) JE_ARG(LoweringContext__compiling)(&context, &request, arguments);
    else JE_ARG(LoweringContext__concrete)(&context, &request);

    jazz_engine_CapErr err = NULL;
    jazz_engine_LoweredRelationInput lowered;
    jazz_engine_ParameterDomain parameters;
    jazz_engine_LoweredTerminalVec terminals = {0};
    jazz_engine_DeclaredOutputFields declared_fields;
    jz_btree covered;
    bool have_lowered = false, have_parameters = false, have_declared = false;
    groove_GraphBuilder *internal_app_rows_graph = NULL;
    jazz_engine_ResolvedSource resolved_root;

    const jazz_engine_ResolvedSource *root = jz_btree_get(&resolved_sources, JE_PLAN(AnalyzedQueryPlan__root_source)(plan));
    if (!root) {
        jazz_engine_UnsupportedReason r = jazz_engine__ur_new(JAZZ_ENGINE_UR_RUNTIME);
        r.message = jz_str_from("root source was not resolved");
        err = report_one(r, JE_LOW(explain_with_request)(&request, explain_clone(&explain)));
        goto fail_early;
    }
    JE_RES(ResolvedSource__Clone__clone)(&resolved_root, root);
    jazz_engine__strvec_push_cstr(&explain.capabilities, JE_PLAN(AnalyzedQueryPlan__capability_label)(plan));

    jazz_engine_UrErr gap = JE_GL(lower_plan_steps)(groove_gb_clone(resolved_root.graph), plan, &resolved_root, &resolved_sources, &context, &lowered);
    if (gap) {
        err = report_one(jazz_engine__ur_take(gap), JE_LOW(explain_with_request)(&request, explain_clone(&explain)));
        goto fail;
    }
    have_lowered = true;

    gap = JE_LOW(parameter_domain_for_request)(&request, &parameters);
    if (gap) {
        err = report_one(jazz_engine__ur_take(gap), JE_LOW(explain_with_request)(&request, explain_clone(&explain)));
        goto fail;
    }
    have_parameters = true;
    JE_LOW(collect_binding_source_params_with_slots)(lowered.graph, &parameters, source_parameters);
    retain_routable_params(&parameters);

    const jazz_engine_NvrVec *agg_group; const jazz_engine_AggregateExprVec *agg_outputs;
    if (request.output.has_app_rows && !JE_PLAN(root_aggregate_step)(plan, &agg_group, &agg_outputs))
        internal_app_rows_graph = groove_gb_clone(lowered.graph);
    groove_GraphBuilder *lowered_graph = lowered.graph;
    lowered.graph = NULL; /* moved into lowered_terminals */
    err = JE_TERM(lowered_terminals)(lowered_graph, &context, plan, &resolved_root, &resolved_sources, &parameters, &parameters.routing_params,
                                     &lowered.fields, &terminals);
    if (err) goto fail;
    JZ_VEC_FOR(const jazz_engine_LoweredTerminal, terminal, terminals) {
        if (terminal->output.tag != JAZZ_ENGINE_OTS_APP_ROWS) continue;
        err = JE_TERM(validate_app_row_publication_schema)(&terminal->output.u.app_rows);
        if (err) goto fail;
    }
    /* Terminals share most of their graph: one per-node memo serves both passes. */
    declared_fields_init(&declared_fields);
    have_declared = true;
    err = JE_LOW(verify_routed_terminal_outputs)(&terminals, &parameters, &request, &explain, &declared_fields);
    if (err) goto fail;
    jazz_engine_ProgramOutputSchemas output = {{0}};
    JZ_VEC_FOR(const jazz_engine_LoweredTerminal, terminal, terminals) {
        jazz_engine_OutputTerminalSchema o;
        JE_SCH(OutputTerminalSchema__Clone__clone)(&o, &terminal->output);
        JZ_VEC_PUSH(output.row_set, o);
    }
    /* Receivers may allocate only the occurrences with a post-policy residual witness terminal. */
    err = JE_LOW(covered_input_source_descriptors)(&terminals, &covered);
    if (err) {
        JE_SCH(ProgramOutputSchemas__drop)(&output);
        goto fail;
    }
    JZ_VEC_FOR(const jazz_engine_LoweredTerminal, terminal, terminals)
        JE_LOW(collect_binding_source_params_with_slots)(terminal->graph, &parameters, source_parameters);
    retain_routable_params(&parameters);
    err = JE_LOW(verify_routed_terminal_outputs)(&terminals, &parameters, &request, &explain, &declared_fields);
    if (err) {
        JE_SCH(ProgramOutputSchemas__drop)(&output);
        jz_btree_drop(&covered);
        goto fail;
    }
    declared_fields_drop(&declared_fields);
    have_declared = false;

    /* Copy each table schema once, keeping the last occurrence. */
    jz_btree maintained;
    jz_btree_init(&maintained, &jazz_engine__STR_TABLE_SCHEMA_MAP_DESC);
    {
        JZ_VEC(const jazz_engine_ResolvedSource *) values = {0};
        jazz_engine_SourceId *k; jazz_engine_ResolvedSource *v;
        JZ_BTREE_FOR(&resolved_sources, k, v) JZ_VEC_PUSH(values, (const jazz_engine_ResolvedSource *)v);
        for (u32 i = values.len; i-- > 0;) {
            const jazz_model_TableSchema *schema = values.ptr[i]->table_schema;
            if (jz_btree_contains(&maintained, &schema->name)) continue;
            jz_str name = jz_str_clone(&schema->name);
            jazz_model_TableSchema copy;
            jazz_model__schema__TableSchema__Clone__clone(&copy, schema);
            jz_btree_insert(&maintained, &name, &copy, NULL);
        }
        jz_vec_drop(&values, sizeof(void *), NULL);
    }
    jz_btree targeted;
    jz_btree_init(&targeted, &jazz_engine__STRSET_DESC);
    {
        jz_str *k; void *v;
        JZ_BTREE_FOR(&maintained, k, v) jazz_engine__strset_insert_clone(&targeted, k);
    }

    memset(out, 0, sizeof *out);
    JE_TERM(shared_witness_sinks)(terminals.ptr, terminals.len, &out->lowered.shared_witness_sinks);
    out->lowered.terminals = terminals;
    out->lowered.internal_app_rows_graph = internal_app_rows_graph;
    out->lowered.parameters = parameters;
    out->lowered.output = output;
    out->lowered.maintained_terminal_tables = maintained;
    out->lowered.targeted_refresh_tables = targeted;
    out->lowered.targeted_refresh_uncertain = false;
    jz_btree_init(&out->source_descriptors, &jazz_engine__PROGRAM_SOURCE_DESCRIPTOR_MAP_DESC);
    {
        jazz_engine_SourceId *source; jazz_engine_ResolvedSource *resolved;
        JZ_BTREE_FOR(&resolved_sources, source, resolved) {
            jazz_protocol_ProgramSourceId id;
            JE_IN(SourceId__program_source_id)(source, &id);
            groove_RecordDescriptor d = resolved->row_shape.descriptor;
            jazz_engine__btree_put(&out->source_descriptors, &id, &d);
        }
    }
    out->covered_input_source_descriptors = covered;
    out->request = request;
    out->explain = explain;

    JE_GL(LoweredRelationInput__drop)(&lowered);
    JE_RES(ResolvedSource__drop)(&resolved_root);
    JE_ARG(LoweringContext__drop)(&context);
    JE_PLAN(AnalyzedQueryPlan__drop)(plan);
    jz_free(plan);
    jz_btree_drop(&resolved_sources);
    return NULL;

fail:
    if (have_declared) declared_fields_drop(&declared_fields);
    jz_vec_drop(&terminals, sizeof(jazz_engine_LoweredTerminal), jazz_engine__terminal_drop_v);
    groove_gb_drop(internal_app_rows_graph);
    if (have_parameters) JE_LOW(ParameterDomain__drop)(&parameters);
    if (have_lowered) JE_GL(LoweredRelationInput__drop)(&lowered);
    JE_RES(ResolvedSource__drop)(&resolved_root);
fail_early:
    JE_ARG(LoweringContext__drop)(&context);
    JE_PLAN(AnalyzedQueryPlan__drop)(plan);
    jz_free(plan);
    jz_btree_drop(&resolved_sources);
    JE_RES(ExplainPlan__drop)(&explain);
    JE_IN(QueryProgramRequest__drop)(&request);
    return err;
}

/* ================================================================ membership unions */
/* Adds graph to the membership of source, unioning with any graph already recorded. */
void JE_LOW(union_member)(jz_btree *members, jazz_engine_SourceId source, groove_GraphBuilder *graph) {
    groove_GraphBuilder **slot = jz_btree_get(members, &source);
    if (slot) {
        groove_GraphBuilderVec inputs = {0};
        JZ_VEC_PUSH(inputs, *slot);
        JZ_VEC_PUSH(inputs, graph);
        *slot = groove__ivm__graph__GraphBuilder__union(inputs);
        sid_drop(&source);
        return;
    }
    jz_btree_insert(members, &source, &graph, NULL);
}

/* ================================================================ parameter domains */
static bool JE_LOW(claim_route_is_ordered_scalar)(const groove_ColumnType *ty) { return groove__records__values__collect_by_ordered_scalar(ty); }

static bool JE_LOW(source_value_ref)(const jazz_engine_NormalizedValueRef *value) {
    return value->tag == JAZZ_ENGINE_NVR_SOURCE_FIELD || (value->tag == JAZZ_ENGINE_NVR_ROW_ID && value->sub == JAZZ_ENGINE_ROWID_SOURCE);
}

static jz_str JE_LOW(param_route_field)(const jz_str *param) {
    jazz_engine_ClaimPath path;
    if (F(claim_path_from_param_field)(JZ_S(param), &path)) {
        JE_IN(ClaimPath__drop)(&path);
        return jz_str_clone(param);
    }
    return F(route_param_field)(JZ_S(param));
}

static void JE_LOW(collect_equality_filter_route_params)(const jazz_engine_PredicateExpr *predicate, jz_btree *routing) {
    if (predicate->tag == JAZZ_ENGINE_PRED_AND) {
        JZ_VEC_FOR(const jazz_engine_PredicateExpr, p, predicate->u.children) JE_LOW(collect_equality_filter_route_params)(p, routing);
        return;
    }
    if (predicate->tag != JAZZ_ENGINE_PRED_COMPARE || predicate->op != JAZZ_ENGINE_CMP_EQ) return;
    const jazz_engine_NormalizedValueRef *left = &predicate->u.pair.left, *right = &predicate->u.pair.right;
    if (JE_LOW(source_value_ref)(left) && right->tag == JAZZ_ENGINE_NVR_PARAM) {
        jz_str f = JE_LOW(param_route_field)(&right->u.param);
        jazz_engine__btree_set_add(routing, &f);
    } else if (JE_LOW(source_value_ref)(right) && left->tag == JAZZ_ENGINE_NVR_PARAM) {
        jz_str f = JE_LOW(param_route_field)(&left->u.param);
        jazz_engine__btree_set_add(routing, &f);
    }
}

typedef JZ_VEC(jazz_engine_ClaimPath) claim_path_vec;
static void JE_LOW(collect_equality_filter_claims)(const jazz_engine_PredicateExpr *predicate, claim_path_vec *claims) {
    if (predicate->tag == JAZZ_ENGINE_PRED_AND) {
        JZ_VEC_FOR(const jazz_engine_PredicateExpr, p, predicate->u.children) JE_LOW(collect_equality_filter_claims)(p, claims);
        return;
    }
    if (predicate->tag != JAZZ_ENGINE_PRED_COMPARE || predicate->op != JAZZ_ENGINE_CMP_EQ) return;
    const jazz_engine_NormalizedValueRef *left = &predicate->u.pair.left, *right = &predicate->u.pair.right;
    jazz_engine_ClaimPath c;
    if (JE_LOW(source_value_ref)(left) && right->tag == JAZZ_ENGINE_NVR_CLAIM) {
        JE_IN(ClaimPath__Clone__clone)(&c, &right->u.claim);
        JZ_VEC_PUSH(*claims, c);
    } else if (JE_LOW(source_value_ref)(right) && left->tag == JAZZ_ENGINE_NVR_CLAIM) {
        JE_IN(ClaimPath__Clone__clone)(&c, &left->u.claim);
        JZ_VEC_PUSH(*claims, c);
    }
}

static void JE_LOW(parameter_domain)(const jazz_engine_NormalizedRowSetShape *shape, jazz_engine_ParameterDomain *domain) {
    JE_LOW(ParameterDomain__Default__default)(domain);
    jz_str *id; jazz_engine_RowSetExpr *node;
    JZ_BTREE_FOR(&shape->nodes, id, node) {
        if (node->tag == JAZZ_ENGINE_EXPR_VALUE_SOURCE && node->u.value_source.mode == JAZZ_ENGINE_VSM_BINDING) {
            JZ_VEC_FOR(const jazz_engine_ValueSourceColumn, column, node->u.value_source.columns) {
                if (column->value.tag == JAZZ_ENGINE_NVR_PARAM) {
                    const jz_str *param = &column->value.u.param;
                    jz_str k = jz_str_clone(param);
                    groove_ValueType t = groove_vt_clone(&column->ty);
                    jazz_engine__btree_put(&domain->user_params, &k, &t);
                    jz_str route = F(route_param_field)(JZ_S(param));
                    jazz_engine__btree_set_add(&domain->routing_params, &route);
                } else if (column->value.tag == JAZZ_ENGINE_NVR_CLAIM) {
                    jz_str param = F(claim_param_field)(&column->value.u.claim);
                    jazz_engine_ClaimParameter claim;
                    JE_IN(ClaimPath__Clone__clone)(&claim.path, &column->value.u.claim);
                    claim.ty = groove_vt_clone(&column->ty);
                    jz_str k = jz_str_clone(&param);
                    jazz_engine__btree_put(&domain->claim_params, &k, &claim);
                    if (JE_LOW(claim_route_is_ordered_scalar)(&column->ty)) jazz_engine__btree_set_add(&domain->routing_params, &param);
                    else jz_str_drop(&param);
                }
            }
        } else if (node->tag == JAZZ_ENGINE_EXPR_FILTER) {
            JE_LOW(collect_equality_filter_route_params)(&node->u.filter.predicate, &domain->routing_params);
        }
        /* every other node (including Aggregate, INV-LOWER-13) contributes no parameters */
    }
}

/* The one binding slot carrying path, when a source equality against that claim can join
 * the binding source instead of inlining its value. */
bool JE_LOW(claim_binding_slot)(const jazz_engine_ParameterDomain *domain, const jazz_engine_ClaimPath *path, jz_str *out) {
    const jz_str *found_slot = NULL;
    const jazz_engine_ClaimParameter *found = NULL;
    u32 count = 0;
    jz_str *slot; jazz_engine_ClaimParameter *claim;
    JZ_BTREE_FOR(&domain->claim_params, slot, claim) {
        if (!claim_path_eq(&claim->path, path)) continue;
        if (++count == 1) { found_slot = slot; found = claim; }
        else break;
    }
    if (count == 1 && JE_LOW(claim_route_is_ordered_scalar)(&found->ty)) {
        *out = jz_str_clone(found_slot);
        return true;
    }
    return false;
}

jazz_engine_UrErr JE_LOW(parameter_domain_for_request)(const jazz_engine_QueryProgramRequest *request, jazz_engine_ParameterDomain *out) {
    const jazz_engine_ProgramBinding *binding = &request->input.binding;
    jazz_engine_ParameterDomain domain;
    JE_LOW(parameter_domain)(&request->input.shape, &domain);
    /* The binding-source identity describes the validated parameter contract. */
    if (binding->has_source_shape) {
        jz_str *name; groove_ValueType *ty;
        JZ_BTREE_FOR(&binding->param_types, name, ty) {
            jz_str k = jz_str_clone(name);
            groove_ValueType t = groove_vt_clone(ty);
            jazz_engine__btree_put(&domain.user_params, &k, &t);
        }
    }
    {
        jz_str *name; groove_ValueType *ty;
        JZ_BTREE_FOR(&binding->extra_user_params, name, ty) {
            const groove_ValueType *existing = jz_btree_get(&domain.user_params, name);
            if (existing && !groove__records__values__ValueType__PartialEq__eq(existing, ty)) {
                JE_LOW(ParameterDomain__drop)(&domain);
                return jazz_engine__ur_rt(jz_format("binding parameter '%.*s' has inconsistent validated types", JE_STR_ARG(*name)));
            }
            jz_str k = jz_str_clone(name);
            groove_ValueType t = groove_vt_clone(ty);
            jazz_engine__btree_put(&domain.user_params, &k, &t);
        }
    }
    if (binding->claim_params.len == 0) {
        *out = domain;
        return NULL;
    }

    {
        jz_str *name; jazz_engine_ProgramClaimParam *param;
        JZ_BTREE_FOR(&binding->claim_params, name, param) {
            const jazz_engine_ClaimParameter *existing = jz_btree_get(&domain.claim_params, name);
            if (existing && !jazz_engine__claim_parameter_eq(existing, param)) {
                JE_LOW(ParameterDomain__drop)(&domain);
                return JE_UR_RT("pre-retarget claim parameter domain diverged from lowered binding sources");
            }
        }
        JZ_BTREE_FOR(&binding->claim_params, name, param) {
            jz_btree_remove(&domain.user_params, name, NULL, NULL);
            jz_str k = jz_str_clone(name);
            jazz_engine_ClaimParameter claim;
            JE_IN(ProgramClaimParam__Clone__clone)(&claim, param);
            jazz_engine__btree_put(&domain.claim_params, &k, &claim);
        }
    }
    if (binding->has_source_shape) {
        /* A filter equating a source field with a carried claim lowers to a join against
         * the binding source, so that claim routes the graph's rows (#4017). */
        claim_path_vec claim_paths = {0};
        jz_str *id; jazz_engine_RowSetExpr *node;
        JZ_BTREE_FOR(&request->input.shape.nodes, id, node)
            if (node->tag == JAZZ_ENGINE_EXPR_FILTER) JE_LOW(collect_equality_filter_claims)(&node->u.filter.predicate, &claim_paths);
        JZ_VEC_FOR(jazz_engine_ClaimPath, path, claim_paths) {
            jz_str slot;
            if (JE_LOW(claim_binding_slot)(&domain, path, &slot)) jazz_engine__btree_set_add(&domain.routing_params, &slot);
            JE_IN(ClaimPath__drop)(path);
        }
        jz_vec_drop(&claim_paths, sizeof(jazz_engine_ClaimPath), NULL);
    }
    *out = domain;
    return NULL;
}

/* ================================================================ binding-source parameters */
void JE_LOW(collect_binding_source_params)(const groove_GraphBuilder *graph, jazz_engine_ParameterDomain *domain) {
    jz_btree empty;
    jz_btree_init(&empty, &jazz_engine__U32_PARAM_DOMAIN_MAP_DESC);
    JE_LOW(collect_binding_source_params_with_slots)(graph, domain, &empty);
    jz_btree_drop(&empty);
}

void JE_LOW(collect_binding_source_params_with_slots)(const groove_GraphBuilder *graph, jazz_engine_ParameterDomain *domain, const jz_btree *slots) {
    jazz_engine_GraphRefVec order;
    JE_LOW(graph_builder_postorder)(graph, &order);
    for (u32 i = 0; i < order.len; i++) {
        const groove_GraphBuilder *node = order.ptr[i];
        if (node->tag == GROOVE_GB_TEMPLATE_INPUT) {
            const jazz_engine_ParameterDomain *parameters = jz_btree_get(slots, &node->u.template_input.slot);
            if (parameters) {
                jz_str *name; groove_ValueType *ty;
                JZ_BTREE_FOR(&parameters->user_params, name, ty) {
                    if (jz_btree_contains(&domain->user_params, name)) continue;
                    jz_str k = jz_str_clone(name);
                    groove_ValueType t = groove_vt_clone(ty);
                    jz_btree_insert(&domain->user_params, &k, &t, NULL);
                }
                jazz_engine_ClaimParameter *claim;
                JZ_BTREE_FOR(&parameters->claim_params, name, claim) {
                    if (jz_btree_contains(&domain->claim_params, name)) continue;
                    jz_str k = jz_str_clone(name);
                    jazz_engine_ClaimParameter c;
                    JE_IN(ProgramClaimParam__Clone__clone)(&c, claim);
                    jz_btree_insert(&domain->claim_params, &k, &c, NULL);
                }
                void *unused;
                JZ_BTREE_FOR(&parameters->routing_params, name, unused) jazz_engine__strset_insert_clone(&domain->routing_params, name);
            }
        }
        if (node->tag != GROOVE_GB_BINDING_SOURCE) continue;
        u32 n;
        const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(node->u.binding_source.output, &n);
        for (u32 f = 0; f < n; f++) {
            if (!fields[f].has_name) continue;
            const jz_str *name = &fields[f].name;
            jazz_engine_ClaimPath path;
            if (F(claim_path_from_param_field)(JZ_S(name), &path)) {
                if (!jz_btree_contains(&domain->claim_params, name)) {
                    jz_str k = jz_str_clone(name);
                    jazz_engine_ClaimParameter c = {path, groove_vt_clone(&fields[f].value_type)};
                    jz_btree_insert(&domain->claim_params, &k, &c, NULL);
                } else {
                    JE_IN(ClaimPath__drop)(&path);
                }
                if (JE_LOW(claim_route_is_ordered_scalar)(&fields[f].value_type)) jazz_engine__strset_insert_clone(&domain->routing_params, name);
            } else {
                if (!jz_btree_contains(&domain->user_params, name)) {
                    jz_str k = jz_str_clone(name);
                    groove_ValueType t = groove_vt_clone(&fields[f].value_type);
                    jz_btree_insert(&domain->user_params, &k, &t, NULL);
                }
                jz_str route = F(route_param_field)(JZ_S(name));
                jazz_engine__btree_set_add(&domain->routing_params, &route);
            }
        }
    }
    jz_vec_drop(&order, sizeof(void *), NULL);
}

/* Jazz query lowering owns declarative sources only: a mutable Groove input is
 * composed after lowering. */
bool JE_LOW(graph_contains_input_source)(const groove_GraphBuilder *graph) {
    jazz_engine_GraphRefVec order;
    JE_LOW(graph_builder_postorder)(graph, &order);
    bool any = false;
    for (u32 i = 0; i < order.len && !any; i++) any = order.ptr[i]->tag == GROOVE_GB_INPUT_SOURCE;
    jz_vec_drop(&order, sizeof(void *), NULL);
    return any;
}

/* Builder nodes in child-before-parent order without consuming the calling stack.
 * Shared subgraphs are returned once, keyed by address. */
typedef struct { const groove_GraphBuilder *node; bool visited; } postorder_item;
void JE_LOW(graph_builder_postorder)(const groove_GraphBuilder *graph, jazz_engine_GraphRefVec *out) {
    JZ_VEC(postorder_item) pending = {0};
    jz_hmap expanded;
    jz_hmap_init(&expanded, &jazz_engine__PTRSET_HDESC);
    memset(out, 0, sizeof *out);
    postorder_item first = {graph, false};
    JZ_VEC_PUSH(pending, first);
#define PUSH_CHILD(g) do { postorder_item _p = {(g), false}; JZ_VEC_PUSH(pending, _p); } while (0)
    while (pending.len) {
        postorder_item item = pending.ptr[--pending.len];
        const groove_GraphBuilder *node = item.node;
        if (item.visited) {
            JZ_VEC_PUSH(*out, node);
            continue;
        }
        /* A DAG has no cycles: a node expanded earlier has been (or will be) emitted. */
        const void *key = node;
        bool inserted;
        jz_hmap_entry(&expanded, &key, &inserted);
        if (!inserted) continue;
        postorder_item again = {node, true};
        JZ_VEC_PUSH(pending, again);
        switch (node->tag) {
        case GROOVE_GB_TYPED_TEMPLATE:
            for (u32 i = node->u.typed_template.inputs.len; i-- > 0;) PUSH_CHILD(node->u.typed_template.inputs.ptr[i]);
            break;
        case GROOVE_GB_TEMPLATE_INPUT:
            if (node->u.template_input.input) PUSH_CHILD(node->u.template_input.input);
            break;
        case GROOVE_GB_RECURSIVE:
            if (node->u.recursive.step_witness) PUSH_CHILD(node->u.recursive.step_witness);
            PUSH_CHILD(node->u.recursive.step);
            PUSH_CHILD(node->u.recursive.seed);
            break;
        case GROOVE_GB_RECURSIVE_STEP_WITNESS: PUSH_CHILD(node->u.recursive_step_witness.recursive); break;
        case GROOVE_GB_FILTER: PUSH_CHILD(node->u.filter.input); break;
        case GROOVE_GB_UNWRAP_NULLABLE: PUSH_CHILD(node->u.unwrap_nullable.input); break;
        case GROOVE_GB_UNNEST: PUSH_CHILD(node->u.unnest.input); break;
        case GROOVE_GB_VARIANT_PROJECT: PUSH_CHILD(node->u.variant_project.input); break;
        case GROOVE_GB_PROJECT: PUSH_CHILD(node->u.project.input); break;
        case GROOVE_GB_STREAMING_CHECKSUM: PUSH_CHILD(node->u.streaming_checksum.input); break;
        case GROOVE_GB_ARG_MAX_BY:
        case GROOVE_GB_ARG_MIN_BY: PUSH_CHILD(node->u.arg_by.input); break;
        case GROOVE_GB_TOP_BY: PUSH_CHILD(node->u.top_by.input); break;
        case GROOVE_GB_COLLECT_BY: PUSH_CHILD(node->u.collect_by.input); break;
        case GROOVE_GB_AGGREGATE: PUSH_CHILD(node->u.aggregate.input); break;
        case GROOVE_GB_LOOKUP: PUSH_CHILD(node->u.lookup.keys); break;
        case GROOVE_GB_UNION:
            for (u32 i = node->u.union_.inputs.len; i-- > 0;) PUSH_CHILD(node->u.union_.inputs.ptr[i]);
            break;
        case GROOVE_GB_JOIN:
        case GROOVE_GB_SEMI_JOIN:
        case GROOVE_GB_ANTI_JOIN:
            PUSH_CHILD(node->u.join.right);
            PUSH_CHILD(node->u.join.left);
            break;
        default: /* Table / InlineRecords / InputSource / Index / FrontierSource / BindingSource */
            break;
        }
    }
#undef PUSH_CHILD
    jz_vec_drop(&pending, sizeof(postorder_item), NULL);
    jz_hmap_drop(&expanded);
}
