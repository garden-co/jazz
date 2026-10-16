/* jazz_node::node::query_eval::lowering
 *
 * Lower normalized query programs into executable Groove graphs and terminals.
 * This stage defines terminal schemas, routing fields, fact payloads, and
 * prepared-parameter layouts. It consumes normalized requests and produces
 * executable graph descriptions; source choice and result materialization live
 * in their neighboring stages.
 *
 * Napkin C port (unit node-qe-a). Async fns are synchronous; NodeState<S> has
 * one copy (storage behind a vtable). Rc<()> leases are jz_rc payloads and a
 * Weak is the same pointer checked through jz_rc_strong. */
#include "jz_rt.h"
#include "jazz_node/_ext_node-qe-a.h"
#include "jazz_node/node/query_eval/read_sources.h"
#include "jazz_node/node/query_eval/lowering.h"
#include "jazz_node/node/query_eval/normalization.h"

#define SL(s) jz_str_slice(&(s))
#define LW(x) jazz_node__node__query_eval__lowering__##x
#define LWN(x) jazz_node__node__query_eval__lowering__NodeState__##x

static void str_drop_v(void *p) { jz_str_drop((jz_str *)p); }
static void str_clone_v(void *d, const void *s) { *(jz_str *)d = jz_str_clone((const jz_str *)s); }
static void strvec_drop(jz_vec_str *v) { jz_vec_drop(v, sizeof(jz_str), str_drop_v); }
static void strvec_push(jz_vec_str *v, jz_str s) { jz_vec_push(v, &s, sizeof(jz_str)); }
static void strvec_push_clone(jz_vec_str *v, const jz_str *s) { strvec_push(v, jz_str_clone(s)); }
static void strvec_push_opt(jz_vec_str *v, bool has, const jz_str *s) { if (has) strvec_push_clone(v, s); }
static void strvec_extend_set(jz_vec_str *v, const jz_btree *set) {
    jz_str *k; void *u;
    JZ_BTREE_FOR(set, k, u) strvec_push_clone(v, k);
}
static void strvec_extend_map_values(jz_vec_str *v, const jz_btree *map) {
    void *k; jz_str *val;
    JZ_BTREE_FOR(map, k, val) strvec_push_clone(v, val);
}
static void strvec_append(jz_vec_str *dst, jz_vec_str *src) {
    jz_vec_extend(dst, src->ptr, src->len, sizeof(jz_str));
    jz_free(src->ptr); src->ptr = NULL; src->len = src->cap = 0;
}
static jz_slice app_rows_sink(void) { return jz_slice_of(JAZZ_APP_ROWS_SINK, (u32)__builtin_strlen(JAZZ_APP_ROWS_SINK)); }
static bool str_is(const jz_str *s, jz_slice x) { return jz_slice_cmp(jz_str_slice(s), x) == 0; }
static bool err_msg_contains(const jz_err *e, const char *needle) {
    if (!e->msg) return false;
    u32 n = (u32)__builtin_strlen(needle);
    const jz_str *m = e->msg;
    if (m->len < n) return false;
    for (u32 i = 0; i + n <= m->len; i++)
        if (__builtin_memcmp(m->ptr + i, needle, n) == 0) return true;
    return false;
}
static jz_err query_capability_from_report(const jz_str *report) {
    return JZN_QUERY_CAPABILITY("%.*s", (int)report->len, report->ptr);
}

/* ---------- PolicyAuthorizationGraph (derive Clone) ---------- */
void jazz_node_PolicyAuthorizationGraph_drop(void *p) {
    jazz_node_PolicyAuthorizationGraph *g = p;
    groove_GraphBuilder_free(g->graph);
    jz_btree_drop(&g->route_fields);
    jz_btree_drop(&g->access_paths);
}
void jazz_node_PolicyAuthorizationGraph_clone(void *dst, const void *src) {
    jazz_node_PolicyAuthorizationGraph *d = dst; const jazz_node_PolicyAuthorizationGraph *s = src;
    d->graph = groove_GraphBuilder_clone(s->graph);
    jz_btree_clone(&d->route_fields, &s->route_fields, str_clone_v, NULL);
    jazz_node_AccessPaths_clone(&d->access_paths, &s->access_paths);
}
const jz_btree_desc jazz_node_PolicyAuthorizationGraphCache_desc = {
    sizeof(jz_str), sizeof(jazz_node_PolicyAuthorizationGraph), jz_cmp_str, str_drop_v, jazz_node_PolicyAuthorizationGraph_drop };

typedef JZ_VEC(jazz_node_ScopedPolicyAuthorizationGraphReplacement) ReplacementVec;
static void replacement_drop(void *p) {
    jazz_node_ScopedPolicyAuthorizationGraphReplacement *r = p;
    if (r->has_previous) jazz_node_PolicyAuthorizationGraph_drop(&r->previous);
}
static void replacement_vec_drop(void *p) { jz_vec_drop(p, sizeof(jazz_node_ScopedPolicyAuthorizationGraphReplacement), replacement_drop); }
const jz_btree_desc jazz_node_PolicyAuthorizationGraphReplacements_desc = {
    sizeof(jz_str), sizeof(ReplacementVec), jz_cmp_str, str_drop_v, replacement_vec_drop };

void jazz_node_SupportedQueryProgram_drop(void *p) {
    jazz_node_SupportedQueryProgram *s = p;
    if (s->program) { jazz_engine_QueryProgram_drop(s->program); jz_free(s->program); s->program = NULL; }
}

static void prepared_param_clone(jazz_node_PreparedQueryParam *d, const jazz_node_PreparedQueryParam *s) {
    d->name = jz_str_clone(&s->name);
    groove_ValueType_clone(&d->ty, &s->ty);
    d->source_tag = s->source_tag;
    if (s->source_tag == JZN_PQPS_CLAIM) jazz_engine_ClaimPath_clone(&d->claim, &s->claim);
    else { d->claim.segments.ptr = NULL; d->claim.segments.len = d->claim.segments.cap = 0; }
}
static void prepared_params_drop(jazz_node_PreparedQueryParamVec *v) { jz_vec_drop(v, sizeof(jazz_node_PreparedQueryParam), jazz_node_PreparedQueryParam_drop); }

/* ---------- HydrationSubscription ---------- */
typedef struct HydrationSubscription {
    groove_Database *database;
    groove_MultisinkSubscription *subscription; /* Option */
    bool has_prepared_shape; u64 prepared_shape;
    /* The shape came from `prepare_shared`: other retained bindings may hold
     * it, so release it only if none does. */
    bool shared_shape;
} HydrationSubscription;

static jz_err jazz_node__node__query_eval__lowering__HydrationSubscription__release(HydrationSubscription *self) {
    if (self->subscription) {
        groove_MultisinkSubscription *subscription = self->subscription; self->subscription = NULL;
        groove__db__Database__unsubscribe(self->database, groove__ivm__MultisinkSubscription__id(subscription));
    }
    if (self->has_prepared_shape) {
        self->has_prepared_shape = false;
        u64 shape = self->prepared_shape;
        if (self->shared_shape) {
            groove__db__Database__release_shared_prepared_shape(self->database, shape);
        } else {
            jz_err e = groove__db__Database__retire_prepared_shape(self->database, shape);
            if (JZ_IS_ERR(e)) return jzn_groove(e);
        }
    }
    return JZ_OK;
}

/* Drop: a poisoned database can reject retirement. Its ordinary shutdown
 * still owns that state; never panic while cancelling another error. */
static void jazz_node__node__query_eval__lowering__HydrationSubscription__Drop__drop(HydrationSubscription *self) {
    jz_err e = jazz_node__node__query_eval__lowering__HydrationSubscription__release(self);
    jz_err_drop(&e);
}

/* ---------- terminal schemas ---------- */
static jz_err jazz_node__node__query_eval__lowering__app_row_terminal_schema(const jazz_engine_ProgramOutputSchemas *output, const jazz_engine_AppRowSchema **out) {
    for (u32 i = 0; i < output->row_set.len; i++) {
        const jazz_engine_OutputTerminalSchema *terminal = &output->row_set.ptr[i];
        if (terminal->tag == JAZZ_OTS_APP_ROWS) { *out = &terminal->rows; return JZ_OK; }
    }
    return JZN_INVALID_STORED("query program did not emit app row terminal");
}

static jz_err jazz_node__node__query_eval__lowering__app_row_terminal_fields(const jazz_engine_ProgramOutputSchemas *output, jz_vec_str *out) {
    out->ptr = NULL; out->len = out->cap = 0;
    const jazz_engine_AppRowSchema *app_rows;
    JZ_TRY(jazz_node__node__query_eval__lowering__app_row_terminal_schema(output, &app_rows));
    const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(app_rows->descriptor);
    for (u32 i = 0; i < fields->len; i++) {
        if (!fields->ptr[i].has_name) { strvec_drop(out); return JZN_INVALID_STORED("app row terminal field must be named"); }
        strvec_push_clone(out, &fields->ptr[i].name);
    }
    return JZ_OK;
}

jz_err LW(app_row_terminal_route_eligible_fields)(const jazz_engine_ProgramOutputSchemas *output, jz_vec_str *out) {
    out->ptr = NULL; out->len = out->cap = 0;
    const jazz_engine_AppRowSchema *app_rows;
    JZ_TRY(jazz_node__node__query_eval__lowering__app_row_terminal_schema(output, &app_rows));
    JZ_TRY(jazz_node__node__query_eval__lowering__app_row_terminal_fields(output, out));
    for (u32 i = 0; i < app_rows->hidden_fields.len; i++) strvec_push_clone(out, &app_rows->hidden_fields.ptr[i]);
    return JZ_OK;
}

jz_err LW(materialization_app_row_schema)(const jazz_node_PreparedQueryPlan *plan, const jazz_engine_QueryProgram *program, jazz_engine_AppRowSchema *out) {
    if (plan) {
        const jazz_engine_AppRowSchema *schema = jazz_node__node__PreparedQueryPlan__app_row_schema(plan);
        if (!schema) return JZN_INVALID_STORED("prepared plan has no app-row schema");
        jazz_engine_AppRowSchema_clone(out, schema);
        return JZ_OK;
    }
    if (!program) return JZN_INVALID_STORED("materialization has no lowered program");
    const jazz_engine_AppRowSchema *schema;
    JZ_TRY(jazz_node__node__query_eval__lowering__app_row_terminal_schema(&program->lowered.output, &schema));
    jazz_engine_AppRowSchema_clone(out, schema);
    return JZ_OK;
}

jz_err LW(lowered_terminal_graph)(const jazz_engine_QueryProgram *program, jz_slice sink, groove_GraphBuilder **out) {
    for (u32 i = 0; i < program->lowered.terminals.len; i++) {
        const jazz_engine_LoweredTerminal *terminal = &program->lowered.terminals.ptr[i];
        if (str_is(&terminal->sink, sink)) { *out = groove_GraphBuilder_clone(terminal->graph); return JZ_OK; }
    }
    *out = NULL;
    return JZN_QUERY_LOWERING("query program did not emit sink %.*s", (int)sink.len, (const char *)sink.ptr);
}

jz_err LW(lowered_app_rows_graph)(const jazz_engine_QueryProgram *program, groove_GraphBuilder **out) {
    return LW(lowered_terminal_graph)(program, app_rows_sink(), out);
}

/* Whether one-shot materialization substitutes the program's storage-shaped
 * internal graph for its public app-row terminal. */
static bool jazz_node__node__query_eval__lowering__materializes_internal_app_rows_graph(const jazz_engine_QueryProgram *program) {
    const jazz_engine_RowSetOutputRequest *output = &program->request.output;
    bool publishes_structured_tree = output->has_app_rows && output->app_rows.projection.tag == JAZZ_PP_TREE
        && output->app_rows.projection.tree.paths.len != 0;
    bool public_root_owns_membership = false;
    const jazz_engine_NormalizedRowSetShape *shape = &program->request.input.shape;
    for (u32 i = 0; i < shape->closure_paths.len; i++) {
        const jazz_engine_ClosurePath *path = &shape->closure_paths.ptr[i];
        if (path->tag == 1 /* ExplicitInclude */ && path->has_root_gate) { public_root_owns_membership = true; break; }
    }
    bool existence_only = output->has_app_rows && !output->app_rows.public_terminal;
    return !(existence_only || publishes_structured_tree || public_root_owns_membership)
        && program->lowered.internal_app_rows_graph != NULL;
}

jz_err LW(lowered_materialization_app_rows_graph)(const jazz_engine_QueryProgram *program, groove_GraphBuilder **out) {
    if (program->lowered.internal_app_rows_graph && jazz_node__node__query_eval__lowering__materializes_internal_app_rows_graph(program)) {
        *out = groove_GraphBuilder_clone(program->lowered.internal_app_rows_graph);
        return JZ_OK;
    }
    return LW(lowered_app_rows_graph)(program, out);
}

groove_Sink *LW(lowered_program_sinks)(const jazz_engine_QueryProgram *program, u32 *n) {
    u32 count = 0;
    const jazz_engine_LoweredTerminal **terminals = jazz_engine__query_engine__lowering__LoweredProgram__execution_terminals(&program->lowered, &count);
    groove_Sink *sinks = count ? jz_alloc(sizeof(groove_Sink) * count) : NULL;
    for (u32 i = 0; i < count; i++) {
        sinks[i].sink = jz_str_clone(&terminals[i]->sink);
        sinks[i].graph = groove_GraphBuilder_clone(terminals[i]->graph);
    }
    jz_free((void *)terminals);
    *n = count;
    return sinks;
}

jazz_node_PreparedQueryParamVec LW(prepared_params_from_domain)(const jazz_engine_ParameterDomain *parameters) {
    jazz_node_PreparedQueryParamVec params = {0};
    jz_str *name; groove_ColumnType *ty;
    JZ_BTREE_FOR(&parameters->user_params, name, ty) {
        jazz_node_PreparedQueryParam p = {0};
        p.name = jz_str_clone(name);
        groove_ValueType_clone(&p.ty, ty);
        p.source_tag = JZN_PQPS_USER;
        jz_vec_push(&params, &p, sizeof p);
    }
    jz_str *cname; jazz_engine_ProgramClaimParam *claim;
    JZ_BTREE_FOR(&parameters->claim_params, cname, claim) {
        jazz_node_PreparedQueryParam p = {0};
        p.name = jz_str_clone(cname);
        groove_ValueType_clone(&p.ty, &claim->ty);
        p.source_tag = JZN_PQPS_CLAIM;
        jazz_engine_ClaimPath_clone(&p.claim, &claim->path);
        jz_vec_push(&params, &p, sizeof p);
    }
    return params;
}

static jz_str jazz_node__node__query_eval__lowering__prepared_param_route_field(const jazz_node_PreparedQueryParam *param) {
    switch (param->source_tag) {
    case JZN_PQPS_USER: return jazz_engine__query_engine__fields__route_param_field(SL(param->name));
    default: return jz_str_clone(&param->name);
    }
}

jz_vec_str LW(prepared_route_param_names)(const jazz_engine_ParameterDomain *parameters) {
    jazz_node_PreparedQueryParamVec params = LW(prepared_params_from_domain)(parameters);
    jz_vec_str out = {0};
    for (u32 i = 0; i < params.len; i++) {
        jz_str field = jazz_node__node__query_eval__lowering__prepared_param_route_field(&params.ptr[i]);
        if (jz_btree_contains(&parameters->routing_params, &field)) strvec_push(&out, field);
        else jz_str_drop(&field);
    }
    prepared_params_drop(&params);
    return out;
}

typedef JZ_VEC(u32) U32Vec;
static U32Vec jazz_node__node__query_eval__lowering__prepared_route_value_indices(const jazz_node_PreparedQueryParam *params, u32 nparams, const jz_vec_str *route_fields) {
    U32Vec out = {0};
    for (u32 r = 0; r < route_fields->len; r++) {
        bool found = false; u32 pos = 0;
        for (u32 i = 0; i < nparams; i++) {
            jz_str field = jazz_node__node__query_eval__lowering__prepared_param_route_field(&params[i]);
            bool eq = jz_str_eq(&field, &route_fields->ptr[r]);
            jz_str_drop(&field);
            if (eq) { found = true; pos = i; break; }
        }
        if (!found) jz_panic("terminal route fields come from the prepared parameter domain");
        jz_vec_push(&out, &pos, sizeof pos);
    }
    return out;
}

jz_vec_str LW(terminal_route_fields)(const jz_str *route_params, u32 nparams, const jz_str *eligible, u32 neligible) {
    jz_btree route_eligible_fields; jz_btree_init(&route_eligible_fields, &jazz_node_StrSet_desc);
    for (u32 i = 0; i < neligible; i++) { jz_str c = jz_str_clone(&eligible[i]); jz_btree_insert(&route_eligible_fields, &c, NULL, NULL); }
    jz_vec_str out = {0};
    for (u32 i = 0; i < nparams; i++)
        if (jz_btree_contains(&route_eligible_fields, &route_params[i])) strvec_push_clone(&out, &route_params[i]);
    jz_btree_drop(&route_eligible_fields);
    return out;
}

static jz_err jazz_node__node__query_eval__lowering__terminal_public_fields(const jazz_engine_OutputTerminalSchema *terminal, jz_vec_str *out) {
    switch (terminal->tag) {
    case JAZZ_OTS_APP_ROWS: return LW(descriptor_field_names)(terminal->rows.descriptor, out);
    default: return LW(fact_public_fields)(&terminal->fact.schema, out);
    }
}

jz_err LW(terminal_route_eligible_fields)(const jazz_engine_OutputTerminalSchema *terminal, jz_vec_str *out) {
    JZ_TRY(jazz_node__node__query_eval__lowering__terminal_public_fields(terminal, out));
    if (terminal->tag == JAZZ_OTS_APP_ROWS)
        for (u32 i = 0; i < terminal->rows.hidden_fields.len; i++) strvec_push_clone(out, &terminal->rows.hidden_fields.ptr[i]);
    return JZ_OK;
}

static jz_vec_str jazz_node__node__query_eval__lowering__result_membership_version_fields(const jazz_engine_ResultMembershipVersionSchema *schema);
static jz_vec_str jazz_node__node__query_eval__lowering__version_witness_public_fields(const jz_str *role_field, const jazz_engine_VersionWitnessSchema *schema);

jz_err LW(fact_public_fields)(const jazz_engine_ProgramFactSchema *schema, jz_vec_str *out) {
    out->ptr = NULL; out->len = out->cap = 0;
    switch (schema->tag) {
    case JAZZ_PFS_AUTHORIZED_ROWS: {
        const jazz_engine_AuthorizedRowsSchema *s = &schema->authorized_rows;
        strvec_push_clone(out, &s->row_field);
        strvec_extend_set(out, &s->routing_param_fields);
        return JZ_OK;
    }
    case JAZZ_PFS_RESULT_MEMBERSHIP: {
        const jazz_engine_ResultMembershipSchema *s = &schema->result_membership;
        strvec_push_clone(out, &s->table_field);
        strvec_push_clone(out, &s->row_field);
        for (u32 i = 0; i < s->occurrence_id_fields.len; i++)
            if (!jz_str_eq(&s->occurrence_id_fields.ptr[i], &s->row_field)) strvec_push_clone(out, &s->occurrence_id_fields.ptr[i]);
        /* A union arm is part of a result occurrence address, just like
         * its paired joined-row id. Retaining only the id causes prepared
         * maintained subscriptions to publish a descriptor that cannot be
         * decoded against the membership schema. */
        strvec_extend_map_values(out, &s->occurrence_union_arm_fields);
        strvec_push_opt(out, s->has_branch_or_prefix_field, &s->branch_or_prefix_field);
        jz_vec_str version = jazz_node__node__query_eval__lowering__result_membership_version_fields(&s->version);
        strvec_append(out, &version);
        strvec_push_opt(out, s->has_settle_position_field, &s->settle_position_field);
        strvec_extend_set(out, &s->routing_param_fields);
        for (u32 i = 0; i < s->payload_fields.len; i++) strvec_push_clone(out, &s->payload_fields.ptr[i].name);
        return JZ_OK;
    }
    case JAZZ_PFS_AGGREGATE_RESULT: {
        const jazz_engine_AggregateResultFactSchema *s = &schema->aggregate_result;
        strvec_push_clone(out, &s->synthetic.table_field);
        strvec_push_clone(out, &s->synthetic.row_field);
        for (u32 pass = 0; pass < 2; pass++) {
            const groove_DescriptorFieldVec *fields = pass == 0 ? &s->group_key_fields : &s->value_fields;
            for (u32 i = 0; i < fields->len; i++) {
                if (!fields->ptr[i].has_name) jz_panic("lowered aggregate field is named");
                strvec_push_clone(out, &fields->ptr[i].name);
            }
        }
        strvec_extend_set(out, &s->routing_param_fields);
        return JZ_OK;
    }
    case JAZZ_PFS_RELATION_EDGES: {
        const jazz_engine_RelationEdgesSchema *s = &schema->relation_edges;
        jz_vec_str source = LW(versioned_row_ref_fields)(&s->source);
        strvec_append(out, &source);
        strvec_push_clone(out, &s->path_field);
        jz_vec_str target = LW(versioned_row_ref_fields)(&s->target);
        strvec_append(out, &target);
        strvec_push_clone(out, &s->kind_field);
        strvec_push_opt(out, s->has_depth_field, &s->depth_field);
        strvec_push_opt(out, s->has_edge_id_field, &s->edge_id_field);
        strvec_push_opt(out, s->has_branch_field, &s->branch_field);
        strvec_push_opt(out, s->has_role_field, &s->role_field);
        strvec_push_opt(out, s->has_order_field, &s->order_field);
        strvec_push_opt(out, s->has_hole_state_field, &s->hole_state_field);
        return JZ_OK;
    }
    case JAZZ_PFS_VERSION_WITNESSES:
    case JAZZ_PFS_REPLACEMENT_WITNESSES: {
        const jazz_engine_VersionWitnessSchemas *s = &schema->witnesses;
        const jazz_engine_VersionWitnessSchema *witness = s->content ? s->content : s->deletion;
        if (!witness) return JZN_INVALID_STORED("version witness fact schema has no terminal schema");
        *out = jazz_node__node__query_eval__lowering__version_witness_public_fields(&s->role_field, witness);
        strvec_extend_set(out, &s->routing_param_fields);
        return JZ_OK;
    }
    case JAZZ_PFS_PROGRAM_SOURCE_COVERAGE:
        strvec_push(out, jz_str_from("complete"));
        strvec_extend_set(out, &schema->routed.routing_param_fields);
        return JZ_OK;
    case JAZZ_PFS_PATH_CORRELATION_COVERAGE: return JZN_INVALID_STORED("path correlation coverage facts are not prepared yet");
    case JAZZ_PFS_READ_FRONTIER_SETTLED: return JZN_INVALID_STORED("read frontier facts are not prepared yet");
    case JAZZ_PFS_COMPLETE_TX_PAYLOAD_COVERAGE: return JZN_INVALID_STORED("complete transaction coverage facts are not prepared yet");
    case JAZZ_PFS_VIEW_COMPLETE_EXCLUSIVE_COVERAGE: return JZN_INVALID_STORED("view-complete coverage facts are not prepared yet");
    case JAZZ_PFS_POLICY_DECISION: return JZN_INVALID_STORED("policy decision facts are not prepared yet");
    case JAZZ_PFS_POLICY_WITNESSES: return JZN_INVALID_STORED("policy witness facts are not prepared yet");
    case JAZZ_PFS_CONTRIBUTING_MEMBERS: return JZN_INVALID_STORED("contributing member facts are not prepared yet");
    case JAZZ_PFS_PREDICATE_READS: return JZN_INVALID_STORED("predicate-read facts are not prepared yet");
    case JAZZ_PFS_PREDICATE_OUTPUT_SET: return JZN_INVALID_STORED("predicate output set facts are not prepared yet");
    case JAZZ_PFS_POINT_READS: return JZN_INVALID_STORED("point-read facts are not prepared yet");
    default: jz_panic("internal error: entered unreachable code");
    }
    return JZ_OK;
}

/* ---------- PolicyDependencyFootprint ---------- */
typedef struct PolicyDependencyFootprint { jz_btree tables; /* BTreeSet<String> */ bool uncertain; } PolicyDependencyFootprint;

static void footprint_insert_table(PolicyDependencyFootprint *self, const jz_str *table) {
    jz_str t = jz_str_clone(table);
    jz_btree_insert(&self->tables, &t, NULL, NULL);
}

static void jazz_node__node__query_eval__lowering__PolicyDependencyFootprint__include(PolicyDependencyFootprint *self, const jazz_engine_QueryProgramRequest *dependency) {
    jazz_engine_SourceId *source; void *expr;
    JZ_BTREE_FOR(&dependency->reads.primary.sources, source, expr) footprint_insert_table(self, &source->table);
    void *fk; jazz_engine_ReadView *read;
    JZ_BTREE_FOR(&dependency->reads.fact_reads, fk, read) {
        jazz_engine_SourceId *s; void *e;
        JZ_BTREE_FOR(&read->sources, s, e) footprint_insert_table(self, &s->table);
    }
}

jz_str LW(policy_authorization_graph_cache_key)(const jazz_engine_QueryProgramRequest *request) {
    jz_str out = {0};
    jazz_engine_QueryProgramRequest_debug(&out, request);
    return out;
}

jz_btree LW(output_routing_fields_for_query_eval)(const jazz_engine_ProgramFactOutput *output) {
    jz_btree out;
    const jazz_engine_ProgramFactSchema *s = &output->schema;
    switch (s->tag) {
    case JAZZ_PFS_AUTHORIZED_ROWS: jz_btree_clone(&out, &s->authorized_rows.routing_param_fields, str_clone_v, NULL); break;
    case JAZZ_PFS_RESULT_MEMBERSHIP: jz_btree_clone(&out, &s->result_membership.routing_param_fields, str_clone_v, NULL); break;
    case JAZZ_PFS_AGGREGATE_RESULT: jz_btree_clone(&out, &s->aggregate_result.routing_param_fields, str_clone_v, NULL); break;
    case JAZZ_PFS_PROGRAM_SOURCE_COVERAGE:
    case JAZZ_PFS_READ_FRONTIER_SETTLED: jz_btree_clone(&out, &s->routed.routing_param_fields, str_clone_v, NULL); break;
    case JAZZ_PFS_VERSION_WITNESSES:
    case JAZZ_PFS_REPLACEMENT_WITNESSES: jz_btree_clone(&out, &s->witnesses.routing_param_fields, str_clone_v, NULL); break;
    default: jz_btree_init(&out, &jazz_node_StrSet_desc); break;
    }
    return out;
}

static jz_vec_str jazz_node__node__query_eval__lowering__version_witness_public_fields(const jz_str *role_field, const jazz_engine_VersionWitnessSchema *schema) {
    jz_vec_str fields = {0};
    strvec_push_clone(&fields, role_field);
    strvec_push_clone(&fields, &schema->identity.table_field);
    strvec_push_clone(&fields, &schema->identity.row_field);
    strvec_push(&fields, jz_str_from("content_tx_time"));
    strvec_push(&fields, jz_str_from("content_tx_node_id"));
    strvec_push_clone(&fields, &schema->identity.tx_time_field);
    strvec_push_clone(&fields, &schema->identity.tx_node_field);
    strvec_push_clone(&fields, &schema->identity.schema_field);
    strvec_push_clone(&fields, &schema->authored_columns_field);
    strvec_push_clone(&fields, &schema->created_by_field);
    strvec_push_clone(&fields, &schema->created_at_field);
    strvec_push_clone(&fields, &schema->updated_by_field);
    strvec_push_clone(&fields, &schema->updated_at_field);
    strvec_push_clone(&fields, &schema->deletion_field);
    strvec_extend_map_values(&fields, &schema->user_fields);
    strvec_push_opt(&fields, schema->identity.has_branch_or_prefix_field, &schema->identity.branch_or_prefix_field);
    return fields;
}

jz_err LW(descriptor_field_names)(groove_RecordDescriptor descriptor, jz_vec_str *out) {
    out->ptr = NULL; out->len = out->cap = 0;
    const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(descriptor);
    for (u32 i = 0; i < fields->len; i++) {
        if (!fields->ptr[i].has_name) { strvec_drop(out); return JZN_INVALID_STORED("query-engine terminal field must be named"); }
        strvec_push_clone(out, &fields->ptr[i].name);
    }
    return JZ_OK;
}

static jz_vec_str jazz_node__node__query_eval__lowering__row_ref_fields(const jazz_engine_QueryEngineRowRefSchema *schema) {
    jz_vec_str fields = {0};
    strvec_push_clone(&fields, &schema->source_field);
    strvec_push_clone(&fields, &schema->table_field);
    strvec_push_clone(&fields, &schema->row_field);
    return fields;
}

jz_vec_str LW(versioned_row_ref_fields)(const jazz_engine_VersionedRowRefSchema *schema) {
    jz_vec_str fields = jazz_node__node__query_eval__lowering__row_ref_fields(&schema->row);
    strvec_push_opt(&fields, schema->has_branch_or_prefix_field, &schema->branch_or_prefix_field);
    if (schema->version) {
        jz_vec_str v = jazz_node__node__query_eval__lowering__result_membership_version_fields(schema->version);
        strvec_append(&fields, &v);
    }
    return fields;
}

static jz_vec_str jazz_node__node__query_eval__lowering__content_version_fields(const jazz_engine_ContentVersionFields *schema) {
    jz_vec_str fields = {0};
    strvec_push_clone(&fields, &schema->tx_time_field);
    strvec_push_clone(&fields, &schema->tx_node_field);
    return fields;
}

static jz_vec_str jazz_node__node__query_eval__lowering__version_identity_fields(const jazz_engine_VersionIdentityFields *schema) {
    jz_vec_str fields = {0};
    strvec_push_clone(&fields, &schema->table_field);
    strvec_push_clone(&fields, &schema->row_field);
    strvec_push_clone(&fields, &schema->tx_time_field);
    strvec_push_clone(&fields, &schema->tx_node_field);
    strvec_push_clone(&fields, &schema->schema_field);
    strvec_push_clone(&fields, &schema->layer_field);
    strvec_push_opt(&fields, schema->has_batch_id_field, &schema->batch_id_field);
    strvec_push_opt(&fields, schema->has_branch_or_prefix_field, &schema->branch_or_prefix_field);
    strvec_push_opt(&fields, schema->has_row_digest_field, &schema->row_digest_field);
    return fields;
}

static jz_vec_str jazz_node__node__query_eval__lowering__result_membership_version_fields(const jazz_engine_ResultMembershipVersionSchema *schema) {
    jz_vec_str fields = jazz_node__node__query_eval__lowering__content_version_fields(&schema->content);
    if (schema->tag == JAZZ_RMV_CONTENT_OR_DELETION) {
        jz_vec_str d = jazz_node__node__query_eval__lowering__version_identity_fields(&schema->deletion);
        strvec_append(&fields, &d);
        strvec_push_clone(&fields, &schema->deletion_state_field);
    }
    return fields;
}

/* ---------- admission / compiled program caches ---------- */
#define COMPILED_QUERY_PROGRAM_CACHE_MAX_ENTRIES 32u

static bool jazz_node__node__query_eval__lowering__query_program_source_cache_safe(const jazz_engine_SourceExpr *source) {
    switch (source->tag) {
    case JAZZ_SE_VISIBLE_CURRENT: return source->data.tag == JAZZ_DS_CURRENT;
    case JAZZ_SE_WITH_OVERLAYS:
        if (source->overlays.entries.len != 0) return false;
        return jazz_node__node__query_eval__lowering__query_program_source_cache_safe(source->input);
    case JAZZ_SE_LENS_PROJECT: return jazz_node__node__query_eval__lowering__query_program_source_cache_safe(source->input);
    case JAZZ_SE_MERGE:
        for (u32 i = 0; i < source->merge_inputs.len; i++)
            if (!jazz_node__node__query_eval__lowering__query_program_source_cache_safe(&source->merge_inputs.ptr[i])) return false;
        return true;
    default: return false;
    }
}

static bool jazz_node__node__query_eval__lowering__query_program_sources_cache_safe(const jazz_engine_QueryProgramRequest *request) {
    void *k; jazz_engine_SourceExpr *expr;
    JZ_BTREE_FOR(&request->reads.primary.sources, k, expr)
        if (!jazz_node__node__query_eval__lowering__query_program_source_cache_safe(expr)) return false;
    void *fk; jazz_engine_ReadView *read;
    JZ_BTREE_FOR(&request->reads.fact_reads, fk, read) {
        void *sk; jazz_engine_SourceExpr *e;
        JZ_BTREE_FOR(&read->sources, sk, e)
            if (!jazz_node__node__query_eval__lowering__query_program_source_cache_safe(e)) return false;
    }
    return true;
}

static bool jazz_node__node__query_eval__lowering__query_program_cache_safe(const jazz_engine_QueryProgramRequest *request) {
    return request->authorization_mode == JAZZ_QAM_TRUSTED_SERVING
        && jazz_node__node__query_eval__lowering__query_program_sources_cache_safe(request);
}

static jz_str jazz_node__node__query_eval__lowering__query_program_cache_key(const jazz_engine_QueryProgramRequest *request, const jz_btree *access_paths) {
    jz_str req = {0}, paths = {0};
    jazz_engine_QueryProgramRequest_debug(&req, request);
    jazz_node_AccessPaths_debug(&paths, access_paths);
    jz_str out = jz_format("request=%.*s;access_paths=%.*s", (int)req.len, req.ptr, (int)paths.len, paths.ptr);
    jz_str_drop(&req); jz_str_drop(&paths);
    return out;
}

static bool jazz_node__node__query_eval__lowering__admission_program_key(const jazz_engine_QueryProgramRequest *request, const jz_btree *access_paths, u8 out[32]) {
    if (!((request->authorization_mode == JAZZ_QAM_TRUSTED_SERVING || request->authorization_mode == JAZZ_QAM_CLIENT_LOCAL)
          && jazz_node__node__query_eval__lowering__query_program_sources_cache_safe(request)))
        return false;
    jz_str key = jazz_node__node__query_eval__lowering__query_program_cache_key(request, access_paths);
    jz_blake3((const u8 *)key.ptr, key.len, out);
    jz_str_drop(&key);
    return true;
}

static jazz_node_SupportedQueryProgram *find_supported(jazz_node_NodeState *self, const u8 key[32]) {
    jz_deque *q = &self->query.supported_query_program_requests;
    for (u32 i = 0; i < q->len; i++) {
        jazz_node_SupportedQueryProgram *entry = jz_deque_at(q, i);
        if (__builtin_memcmp(entry->fingerprint, key, 32) == 0) return entry;
    }
    return NULL;
}

static void jazz_node__node__query_eval__lowering__NodeState__remember_supported_query_program(jazz_node_NodeState *self, const u8 key[32], jazz_engine_QueryProgram *program /* Option, owned */) {
    jz_deque *q = &self->query.supported_query_program_requests;
    jazz_node_SupportedQueryProgram *entry = find_supported(self, key);
    if (entry) {
        if (!program) return;
        /* The compiler already recorded the proof. Release any old product
         * before attaching this one with the same handoff budget. */
        jazz_node_SupportedQueryProgram_drop(entry);
    } else {
        /* FIFO eviction only causes recompilation. There is no lifetime
         * admission quota and failed/cancelled compilation is never cached. */
        enum { MAX_ADMISSIONS = 256 };
        if (q->len == MAX_ADMISSIONS) {
            jazz_node_SupportedQueryProgram old;
            if (jz_deque_pop_front(q, &old)) jazz_node_SupportedQueryProgram_drop(&old);
        }
        jazz_node_SupportedQueryProgram fresh;
        __builtin_memcpy(fresh.fingerprint, key, 32);
        fresh.program = NULL;
        jz_deque_push_back(q, &fresh);
    }
    if (program) {
        /* Keep a bounded compiled-program budget independently of the
         * larger proof budget. The first installer takes ownership, so used
         * programs do not occupy this handoff budget. */
        u32 held = 0;
        for (u32 i = 0; i < q->len; i++) if (((jazz_node_SupportedQueryProgram *)jz_deque_at(q, i))->program) held++;
        if (held >= JAZZ_ADMISSION_HANDOFF_MAX_PROGRAMS) {
            for (u32 i = 0; i < q->len; i++) {
                jazz_node_SupportedQueryProgram *oldest = jz_deque_at(q, i);
                if (oldest->program) { jazz_node_SupportedQueryProgram_drop(oldest); break; }
            }
        }
        jazz_node_SupportedQueryProgram *slot = find_supported(self, key);
        if (!slot) jz_panic("proof inserted above");
        slot->program = program;
    }
}

static jazz_engine_QueryProgram *box_program(jazz_engine_QueryProgram *program) {
    return jz_memdup(program, sizeof *program);
}

/* Build the request (including strict claims) on every call. Remember a
 * successful exact-context proof and hand its compiler output to a matching
 * installer, which still owns its own evaluator and binding. */
jz_err LWN(ensure_query_program_request_supported)(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, jz_btree access_paths) {
    /* Branches, overlays, inline snapshots and covered inputs likewise stay on
     * the ordinary path. No data-sensitive source is admitted from a
     * remembered success. */
    u8 key[32];
    bool has_key = jazz_node__node__query_eval__lowering__admission_program_key(&request, &access_paths, key);
    if (has_key && find_supported(self, key)) {
        jazz_engine_QueryProgramRequest_drop(&request);
        jz_btree_drop(&access_paths);
        return JZ_OK;
    }
    jazz_engine_QueryProgram program;
    JZ_TRY(LWN(compile_query_program_request_with_access_paths)(self, request, access_paths, &program));
    if (has_key) jazz_node__node__query_eval__lowering__NodeState__remember_supported_query_program(self, key, box_program(&program));
    else jazz_engine_QueryProgram_drop(&program);
    return JZ_OK;
}

jz_err LWN(compile_query_program_request)(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, jazz_engine_QueryProgram *out) {
    jz_btree access_paths; jz_btree_init(&access_paths, &jazz_node_AccessPaths_desc);
    return LWN(compile_query_program_request_with_access_paths)(self, request, access_paths, out);
}

jz_err LWN(compile_query_program_request_with_access_paths)(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, jz_btree access_paths, jazz_engine_QueryProgram *out) {
    u8 key[32];
    bool has_key = jazz_node__node__query_eval__lowering__admission_program_key(&request, &access_paths, key);
    if (has_key) {
        jazz_node_SupportedQueryProgram *entry = find_supported(self, key);
        if (entry && entry->program) {
            *out = *entry->program;
            jz_free(entry->program);
            entry->program = NULL;
            jazz_engine_QueryProgramRequest_drop(&request);
            jz_btree_drop(&access_paths);
            return JZ_OK;
        }
    }
    bool has_cache_key = jazz_node__node__query_eval__lowering__query_program_cache_safe(&request);
    jz_str cache_key = {0};
    if (has_cache_key) {
        cache_key = jazz_node__node__query_eval__lowering__query_program_cache_key(&request, &access_paths);
        jazz_engine_QueryProgram **cached = jz_btree_get(&self->query.compiled_query_program_cache, &cache_key);
        if (cached) {
            jazz_engine_QueryProgram_clone(out, *cached);
            if (has_key) jazz_node__node__query_eval__lowering__NodeState__remember_supported_query_program(self, key, NULL);
            jz_str_drop(&cache_key);
            jazz_engine_QueryProgramRequest_drop(&request);
            jz_btree_drop(&access_paths);
            return JZ_OK;
        }
    }
    jz_btree inline_sources; jz_btree_init(&inline_sources, &jazz_node_CurrentRowsBySource_desc);
    jz_err e = LWN(compile_query_program_request_with_inline_sources_and_access_paths)(self, request, inline_sources, access_paths, out);
    if (JZ_IS_ERR(e)) { if (has_cache_key) jz_str_drop(&cache_key); return e; }
    if (has_cache_key) {
        jz_btree *cache = &self->query.compiled_query_program_cache;
        void *first_key, *first_val;
        if (cache->len >= COMPILED_QUERY_PROGRAM_CACHE_MAX_ENTRIES && jz_btree_first(cache, &first_key, &first_val)) {
            jz_str eviction_key = jz_str_clone((const jz_str *)first_key);
            jz_btree_remove(cache, &eviction_key, NULL, NULL);
            jz_str_drop(&eviction_key);
        }
        jazz_engine_QueryProgram *arc = jz_rc_new(sizeof(jazz_engine_QueryProgram), jazz_engine_QueryProgram_drop);
        jazz_engine_QueryProgram_clone(arc, out);
        jz_btree_insert(cache, &cache_key, &arc, NULL);
    }
    /* The order can be reversed: a foreground installs locally before it
     * receives RegisterShape. Its successful compilation is already the exact
     * capability proof; later admission need not compile it again. */
    if (has_key) jazz_node__node__query_eval__lowering__NodeState__remember_supported_query_program(self, key, NULL);
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__lowering__NodeState__compile_query_program_request_with_inline_sources_and_access_paths_inner(
    jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, jz_btree inline_sources, jz_btree access_paths,
    jz_btree covered_input_sources, jz_btree covered_input_descriptors, bool count_access_path_metrics,
    bool has_bounded, jazz_engine_SourceId bounded_source, groove_GraphBuilder *bounded_register,
    jazz_node_TransactionWriteOverlay transaction_overlay, jazz_engine_QueryProgram *out);

jz_err LWN(compile_query_program_request_with_inline_sources_and_access_paths)(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request,
    jz_btree inline_sources, jz_btree access_paths, jazz_engine_QueryProgram *out) {
    jz_btree covered_sources; jz_btree_init(&covered_sources, &jazz_node_SourceGraphs_desc);
    jz_btree covered_descriptors; jz_btree_init(&covered_descriptors, &jazz_node_SourceDescriptors_desc);
    return LWN(compile_query_program_request_with_inline_sources_access_paths_and_covered_inputs)(self, request, inline_sources, access_paths,
        covered_sources, covered_descriptors, out);
}

/* Compile a receiver-local authority-covered program. Each entry is an
 * already allocated, runtime-owned input keyed by its complete normalized
 * source occurrence. */
jz_err LWN(compile_query_program_request_with_inline_sources_access_paths_and_covered_inputs)(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request,
    jz_btree inline_sources, jz_btree access_paths, jz_btree covered_input_sources, jz_btree covered_input_descriptors, jazz_engine_QueryProgram *out) {
    jazz_engine_SourceId none = {0};
    return jazz_node__node__query_eval__lowering__NodeState__compile_query_program_request_with_inline_sources_and_access_paths_inner(self, request,
        inline_sources, access_paths, covered_input_sources, covered_input_descriptors, true, false, none, NULL,
        jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__Default__default(), out);
}

/* Compile a write-policy candidate program whose committed evidence is
 * overlaid with the candidate transaction's own writes (INV-RLS-9). The
 * overlay is request-owned data, so the result is never cached. */
jz_err LWN(compile_query_program_request_with_inline_sources_and_transaction_overlay)(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request,
    jz_btree inline_sources, jz_btree access_paths, jazz_node_TransactionWriteOverlay transaction_overlay, jazz_engine_QueryProgram *out) {
    jz_btree covered_sources; jz_btree_init(&covered_sources, &jazz_node_SourceGraphs_desc);
    jz_btree covered_descriptors; jz_btree_init(&covered_descriptors, &jazz_node_SourceDescriptors_desc);
    jazz_engine_SourceId none = {0};
    return jazz_node__node__query_eval__lowering__NodeState__compile_query_program_request_with_inline_sources_and_access_paths_inner(self, request,
        inline_sources, access_paths, covered_sources, covered_descriptors, true, false, none, NULL, transaction_overlay, out);
}

jz_err LWN(compile_query_program_request_with_shared_access_paths)(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request,
    jz_btree access_paths, bool has_bounded, jazz_engine_SourceId bounded_source, groove_GraphBuilder *bounded_register, jazz_engine_QueryProgram *out) {
    jz_btree inline_sources; jz_btree_init(&inline_sources, &jazz_node_CurrentRowsBySource_desc);
    jz_btree covered_sources; jz_btree_init(&covered_sources, &jazz_node_SourceGraphs_desc);
    jz_btree covered_descriptors; jz_btree_init(&covered_descriptors, &jazz_node_SourceDescriptors_desc);
    return jazz_node__node__query_eval__lowering__NodeState__compile_query_program_request_with_inline_sources_and_access_paths_inner(self, request,
        inline_sources, access_paths, covered_sources, covered_descriptors, false, has_bounded, bounded_source, bounded_register,
        jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__Default__default(), out);
}

jz_err LWN(compile_query_program_request_with_bounded_deletion_register)(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request,
    jz_btree access_paths, jazz_engine_SourceId bounded_source, groove_GraphBuilder *bounded_register, jazz_engine_QueryProgram *out) {
    /* The inline register is bound to this snapshot. A cached program would
     * retain stale deletion state after a later write. */
    jz_btree inline_sources; jz_btree_init(&inline_sources, &jazz_node_CurrentRowsBySource_desc);
    jz_btree covered_sources; jz_btree_init(&covered_sources, &jazz_node_SourceGraphs_desc);
    jz_btree covered_descriptors; jz_btree_init(&covered_descriptors, &jazz_node_SourceDescriptors_desc);
    return jazz_node__node__query_eval__lowering__NodeState__compile_query_program_request_with_inline_sources_and_access_paths_inner(self, request,
        inline_sources, access_paths, covered_sources, covered_descriptors, true, true, bounded_source, bounded_register,
        jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__Default__default(), out);
}

static jz_str source_keys_debug(const jz_btree *map) {
    jz_str out = jz_str_from("[");
    jazz_engine_SourceId *k; void *v; bool first = true;
    JZ_BTREE_FOR(map, k, v) {
        if (!first) jz_str_push_cstr(&out, ", ");
        first = false;
        jz_str d = {0}; jazz_engine_SourceId_debug(&d, k);
        jz_str_push(&out, d.ptr, d.len); jz_str_drop(&d);
    }
    jz_str_push_char(&out, ']');
    return out;
}

static void jazz_node__node__query_eval__lowering__NodeState__begin_scoped_policy_authorization_graph_replacement(jazz_node_NodeState *self, jz_slice cache_key, void *lease);
static void jazz_node__node__query_eval__lowering__NodeState__restore_scoped_policy_authorization_graphs(jazz_node_NodeState *self, void *lease);
static jz_err jazz_node__node__query_eval__lowering__NodeState__prepare_query_program_policy_dependencies(jazz_node_NodeState *self,
    const jazz_engine_QueryProgramRequest *request, const jazz_engine_SourceRequest *source_requests, u32 nsources, const jz_btree *outer_access_paths,
    bool has_bounded, const jazz_engine_SourceId *bounded_source, const groove_GraphBuilder *bounded_register, void *lease, PolicyDependencyFootprint *out);

typedef struct DescribeCtx { jazz_node_NodeState *node; } DescribeCtx;
static jz_err describe_template_input_cb(void *ctx, const groove_GraphBuilder *g, groove_RecordDescriptor *out) {
    return groove__db__Database__describe_template_input(&((DescribeCtx *)ctx)->node->database, g, out);
}

static jz_err jazz_node__node__query_eval__lowering__NodeState__compile_query_program_request_with_inline_sources_and_access_paths_inner(
    jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, jz_btree inline_sources, jz_btree access_paths,
    jz_btree covered_input_sources, jz_btree covered_input_descriptors, bool count_access_path_metrics,
    bool has_bounded, jazz_engine_SourceId bounded_source, groove_GraphBuilder *bounded_register,
    jazz_node_TransactionWriteOverlay transaction_overlay, jazz_engine_QueryProgram *out) {
    LWN(restore_expired_policy_compilation_state)(self);
    if (jazz_types__debug_env__covered_input_trace() && covered_input_sources.len != 0) {
        jz_str requested = source_keys_debug(&request.reads.primary.sources);
        jz_str runtime = source_keys_debug(&covered_input_sources);
        jazz_types__debug_env__eprint(jz_format("JAZZ_COVERED_INPUT_TRACE stage=compile_receiver_program requested_sources=%.*s runtime_sources=%.*s",
            (int)requested.len, requested.ptr, (int)runtime.len, runtime.ptr));
        jz_str_drop(&requested); jz_str_drop(&runtime);
    }
    void *policy_replacement_lease = jz_rc_new(1, NULL); /* Rc<()> */
    jazz_engine_QueryProgramCompilation *compilation = NULL;
    jz_str report_debug = {0};
    jz_err err = JZ_OK;
    if (!jazz_engine__query_engine__QueryProgramCompilation__analyze(request, &compilation, &report_debug)) {
        err = query_capability_from_report(&report_debug);
        jz_str_drop(&report_debug);
        jz_rc_release(policy_replacement_lease);
        jz_btree_drop(&inline_sources); jz_btree_drop(&access_paths);
        jz_btree_drop(&covered_input_sources); jz_btree_drop(&covered_input_descriptors);
        if (has_bounded) { jazz_engine_SourceId_drop(&bounded_source); groove_GraphBuilder_free(bounded_register); }
        jazz_node_TransactionWriteOverlay_drop(&transaction_overlay);
        return err;
    }
    const jazz_engine_QueryProgramRequest *req = jazz_engine__query_engine__QueryProgramCompilation__request(compilation);
    u32 nsources = 0;
    const jazz_engine_SourceRequest *sources = jazz_engine__query_engine__QueryProgramCompilation__sources(compilation, &nsources);
    /* The transaction overlay reaches only this program's own sources (INV-RLS-21). */
    if (jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__is_active(&transaction_overlay))
        for (u32 i = 0; i < nsources; i++)
            if (sources[i].authorization.tag == JAZZ_SAR_POLICY_FILTERED)
                jz_panic("a write-policy subplan under a transaction overlay read a policy-filtered source");
    PolicyDependencyFootprint footprint;
    err = jazz_node__node__query_eval__lowering__NodeState__prepare_query_program_policy_dependencies(self, req, sources, nsources, &access_paths,
        has_bounded, &bounded_source, bounded_register, policy_replacement_lease, &footprint);
    if (JZ_IS_ERR(err)) {
        jz_rc_release(policy_replacement_lease);
        jz_btree_drop(&inline_sources); jz_btree_drop(&access_paths);
        jz_btree_drop(&covered_input_sources); jz_btree_drop(&covered_input_descriptors);
        if (has_bounded) { jazz_engine_SourceId_drop(&bounded_source); groove_GraphBuilder_free(bounded_register); }
        jazz_node_TransactionWriteOverlay_drop(&transaction_overlay);
        return err;
    }
    bool trace = jazz_node__node__query_eval__read_sources__capability_trace_enabled();
    jazz_engine_QueryProgramRequest trace_request;
    if (trace) jazz_engine_QueryProgramRequest_clone(&trace_request, req);
    jazz_engine_ReadView read_view; jazz_engine_ReadView_clone(&read_view, &req->reads.primary);
    jazz_node_JazzSourceGraphPreparer resolver = {0};
    resolver.has_local_unavailable_scope = jazz_node__node__query_eval__unavailable_inputs__local_unavailable_policy_binding(req, &resolver.local_unavailable_scope);
    resolver.node = self;
    resolver.read_view = &read_view;
    resolver.inline_sources = inline_sources;
    resolver.covered_input_sources = covered_input_sources;
    resolver.covered_input_descriptors = covered_input_descriptors;
    resolver.access_paths = access_paths;
    resolver.has_bounded_deletion_register = has_bounded;
    resolver.bounded_source = bounded_source;
    resolver.bounded_register = bounded_register;
    resolver.count_access_path_metrics = count_access_path_metrics;
    jz_btree_init(&resolver.current_projection_targets, &jazz_node_SourceStrings_desc);
    resolver.policy_subplan = req->policy.tag == JAZZ_PC_AUTHORIZATION_SUBPLAN;
    resolver.transaction_overlay = transaction_overlay;
    jazz_types_NodeUuid node_uuid = resolver.node->node_uuid;
    bool has_node_alias = resolver.node->has_self_node_alias;
    u64 node_alias = resolver.node->self_node_alias;

    jazz_engine_PreparedSources *prepared = NULL; void *explain = NULL;
    jazz_engine_CapabilityReport *report = NULL;
    bool ok = jazz_engine__query_engine__prepare_query_program_sources(compilation, &resolver, &jazz_node_JazzSourceGraphPreparer_SourceGraphPreparer_vtbl,
        &prepared, &explain, &report);
    if (ok) {
        DescribeCtx ctx = { resolver.node };
        ok = jazz_engine__query_engine__QueryProgramTemplateCache__lower(resolver.node->query.query_program_templates, compilation, prepared, explain,
            describe_template_input_cb, &ctx, out, &report);
    }
    if (ok) {
        jazz_node__node__query_eval__route_discipline__NodeState__scope_program_binding_source(resolver.node, out);
        jz_str *t; void *u;
        JZ_BTREE_FOR(&footprint.tables, t, u) {
            jz_str c = jz_str_clone(t);
            jz_btree_insert(&out->lowered.targeted_refresh_tables, &c, NULL, NULL);
        }
        out->lowered.targeted_refresh_uncertain |= footprint.uncertain;
    }
    jz_btree_drop(&footprint.tables);
    jazz_node__node__query_eval__lowering__NodeState__restore_scoped_policy_authorization_graphs(resolver.node, policy_replacement_lease);
    if (trace) {
        jazz_node__node__query_eval__read_sources__trace_capability_compile(node_uuid, has_node_alias, node_alias, &trace_request,
            ok ? (const void *)out : NULL, ok ? NULL : (const void *)report);
        jazz_engine_QueryProgramRequest_drop(&trace_request);
    }
    jazz_node_JazzSourceGraphPreparer_drop(&resolver);
    jazz_engine_ReadView_drop(&read_view);
    jz_rc_release(policy_replacement_lease);
    if (!ok) {
        jz_str debug = {0};
        jazz_engine_CapabilityReport_debug(&debug, report);
        jazz_engine_CapabilityReport_free(report);
        err = query_capability_from_report(&debug);
        jz_str_drop(&debug);
        return err;
    }
    return JZ_OK;
}

/* ---------- policy dependency preparation ---------- */
typedef struct PolicyDependency {
    jazz_engine_QueryProgramRequest dependency;
    bool has_path; jazz_node_CurrentAccessPath path;
    groove_GraphBuilder *register_; /* Option */
} PolicyDependency;
static void policy_dependency_drop(void *p) {
    PolicyDependency *d = p;
    jazz_engine_QueryProgramRequest_drop(&d->dependency);
    if (d->has_path) jazz_node_CurrentAccessPath_drop(&d->path);
    if (d->register_) groove_GraphBuilder_free(d->register_);
}
static const jz_btree_desc grouped_desc = { sizeof(jz_str), sizeof(PolicyDependency), jz_cmp_str, str_drop_v, policy_dependency_drop };

static jazz_node_PolicyAuthorizationGraph empty_policy_authorization_graph(void) {
    jazz_node_PolicyAuthorizationGraph g;
    g.graph = jazz_node__node__query_eval__read_sources__empty_authorized_row_id_graph();
    jz_btree_init(&g.route_fields, &jazz_node_StrSet_desc);
    jz_btree_init(&g.access_paths, &jazz_node_AccessPaths_desc);
    return g;
}

static jz_err jazz_node__node__query_eval__lowering__NodeState__prepare_query_program_policy_dependencies(jazz_node_NodeState *self,
    const jazz_engine_QueryProgramRequest *request, const jazz_engine_SourceRequest *source_requests, u32 nsources, const jz_btree *outer_access_paths,
    bool has_bounded, const jazz_engine_SourceId *bounded_source, const groove_GraphBuilder *bounded_register, void *lease, PolicyDependencyFootprint *out) {
    /* A deletion terminal carries the raw register but must be gated by the
     * same source occurrence resolved with its deleted preimage. Preload that
     * policy dependency before the source preparer reaches the sibling. */
    JZ_VEC(jazz_engine_SourceRequest) policy_source_requests = {0};
    for (u32 i = 0; i < nsources; i++) {
        jazz_engine_SourceRequest c; jazz_engine_SourceRequest_clone(&c, &source_requests[i]);
        jz_vec_push(&policy_source_requests, &c, sizeof c);
    }
    for (u32 i = 0; i < nsources; i++) {
        jazz_engine_SourceRequest pre;
        if (jazz_engine__query_engine__lowering__authorized_deletion_preimage_source_request(&source_requests[i], &pre))
            jz_vec_push(&policy_source_requests, &pre, sizeof pre);
    }
    jazz_engine_ReadView read_view; jazz_engine_ReadView_clone(&read_view, &request->reads.primary);
    JZ_VEC(PolicyDependency) dependencies = {0};
    PolicyDependencyFootprint footprint; jz_btree_init(&footprint.tables, &jazz_node_StrSet_desc); footprint.uncertain = false;
    jz_err err = JZ_OK;
    {
        jazz_node_JazzSourceGraphPreparer preparer = {0};
        preparer.has_local_unavailable_scope = false;
        preparer.node = self;
        preparer.read_view = &read_view;
        jz_btree_init(&preparer.inline_sources, &jazz_node_CurrentRowsBySource_desc);
        jz_btree_init(&preparer.covered_input_sources, &jazz_node_SourceGraphs_desc);
        jz_btree_init(&preparer.covered_input_descriptors, &jazz_node_SourceDescriptors_desc);
        jz_btree_init(&preparer.access_paths, &jazz_node_AccessPaths_desc);
        preparer.has_bounded_deletion_register = false;
        preparer.count_access_path_metrics = true;
        jz_btree_init(&preparer.current_projection_targets, &jazz_node_SourceStrings_desc);
        preparer.policy_subplan = false;
        preparer.transaction_overlay = jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__Default__default();
        for (u32 i = 0; i < policy_source_requests.len; i++) {
            const jazz_engine_SourceRequest *source = &policy_source_requests.ptr[i];
            if (source->authorization.tag != JAZZ_SAR_POLICY_FILTERED) continue;
            bool some; jazz_engine_QueryProgramRequest dependency;
            err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__policy_dependency_request(&preparer, source, &some, &dependency);
            if (JZ_IS_ERR(err)) break;
            if (some) {
                jazz_node__node__query_eval__lowering__PolicyDependencyFootprint__include(&footprint, &dependency);
                /* Match the outer occurrence before translating to the
                 * authorization program's protected root. */
                PolicyDependency d = {0};
                d.dependency = dependency;
                const jazz_node_CurrentAccessPath *path = jz_btree_get(outer_access_paths, &source->source);
                if (path) { d.has_path = true; jazz_node_CurrentAccessPath_clone(&d.path, path); }
                if (has_bounded && jazz_engine_SourceId_eq(bounded_source, &source->source)) d.register_ = groove_GraphBuilder_clone(bounded_register);
                jz_vec_push(&dependencies, &d, sizeof d);
            } else {
                /* Unsupported policy shapes must not silently become stale
                 * subscriptions. Keep the protected table and make local
                 * refresh conservative until a complete dependency graph is
                 * available. */
                footprint.uncertain = true;
                footprint_insert_table(&footprint, &source->source.table);
            }
        }
        jazz_node_JazzSourceGraphPreparer_drop(&preparer);
    }
    jz_vec_drop(&policy_source_requests, sizeof(jazz_engine_SourceRequest), jazz_engine_SourceRequest_drop);
    jazz_engine_ReadView_drop(&read_view);
    if (JZ_IS_ERR(err)) {
        jz_vec_drop(&dependencies, sizeof(PolicyDependency), policy_dependency_drop);
        jz_btree_drop(&footprint.tables);
        return err;
    }
    jz_btree grouped; jz_btree_init(&grouped, &grouped_desc);
    for (u32 i = 0; i < dependencies.len; i++) {
        PolicyDependency *d = &dependencies.ptr[i];
        jz_str key = LW(policy_authorization_graph_cache_key)(&d->dependency);
        PolicyDependency *entry = jz_btree_get(&grouped, &key);
        if (!entry) {
            jz_btree_insert(&grouped, &key, d, NULL); /* moves (dependency, path, register) */
            continue;
        }
        jz_str_drop(&key);
        /* The same reusable proof may serve more than one occurrence.
         * Specialize only when every consumer has the same candidate domain;
         * an unrestricted consumer forces the ordinary proof. */
        bool same_path = entry->has_path == d->has_path && (!d->has_path || jazz_node_CurrentAccessPath_eq(&entry->path, &d->path));
        bool same_register = (entry->register_ == NULL) == (d->register_ == NULL)
            && (!d->register_ || groove_GraphBuilder_eq(entry->register_, d->register_));
        if (!same_path || !same_register) {
            if (entry->has_path) { jazz_node_CurrentAccessPath_drop(&entry->path); entry->has_path = false; }
            if (entry->register_) { groove_GraphBuilder_free(entry->register_); entry->register_ = NULL; }
        }
        policy_dependency_drop(d);
    }
    jz_free(dependencies.ptr);

    jz_str cache_key; PolicyDependency group;
    while (jz_btree_pop_first(&grouped, &cache_key, &group)) {
        jazz_engine_QueryProgramRequest dependency = group.dependency;
        bool has_candidate = dependency.policy.tag == JAZZ_PC_AUTHORIZATION_SUBPLAN && group.has_path;
        if (has_candidate) {
            jz_btree access_paths; jz_btree_init(&access_paths, &jazz_node_AccessPaths_desc);
            jazz_engine_SourceId protected_source; jazz_engine_SourceId_clone(&protected_source, &dependency.policy.protected_source);
            jz_btree_insert(&access_paths, &protected_source, &group.path, NULL);
            /* This request-owned proof covers every OR arm over the protected
             * root, never just the arm providing a claim. The scoped
             * replacement is restored after compilation (including
             * cancellation), leaving reusable policy caches neutral. */
            jazz_node__node__query_eval__lowering__NodeState__begin_scoped_policy_authorization_graph_replacement(self, SL(cache_key), lease);
            bool has_bounded_register = false; jazz_engine_SourceId reg_source = {0}; groove_GraphBuilder *reg_graph = NULL;
            if (dependency.policy.tag == JAZZ_PC_AUTHORIZATION_SUBPLAN && group.register_) {
                has_bounded_register = true;
                jazz_engine_SourceId_clone(&reg_source, &dependency.policy.protected_source);
                reg_graph = group.register_;
            } else if (group.register_) {
                groove_GraphBuilder_free(group.register_);
            }
            jazz_node_PolicyAuthorizationGraph graph;
            jz_err e = jazz_node__node__query_eval__authorization__NodeState__point_policy_authorization_row_id_graph(self, dependency, access_paths,
                has_bounded_register, reg_source, reg_graph, &graph);
            if (!JZ_IS_ERR(e)) {
                jz_btree_insert(&self->query.policy_authorization_graph_cache, &cache_key, &graph, NULL);
            } else if (e.code == JZN_E_QueryCapability && err_msg_contains(&e, "PolicyProofCycle")) {
                jz_str_drop(&cache_key);
                jazz_node__node__query_eval__lowering__NodeState__restore_scoped_policy_authorization_graphs(self, lease);
                jz_btree_drop(&grouped); jz_btree_drop(&footprint.tables);
                return e;
            } else if (e.code == JZN_E_QueryCapability) {
                jz_err_drop(&e);
                jazz_node_PolicyAuthorizationGraph empty = empty_policy_authorization_graph();
                jz_btree_insert(&self->query.policy_authorization_graph_cache, &cache_key, &empty, NULL);
            } else {
                jz_str_drop(&cache_key);
                jazz_node__node__query_eval__lowering__NodeState__restore_scoped_policy_authorization_graphs(self, lease);
                jz_btree_drop(&grouped); jz_btree_drop(&footprint.tables);
                return e;
            }
        } else {
            if (group.has_path) jazz_node_CurrentAccessPath_drop(&group.path);
            if (group.register_) groove_GraphBuilder_free(group.register_);
            jazz_node_PolicyAuthorizationGraph graph;
            jz_err e = jazz_node__node__query_eval__authorization__NodeState__policy_authorization_row_id_graph(self, dependency, &graph);
            if (!JZ_IS_ERR(e)) {
                jazz_node_PolicyAuthorizationGraph_drop(&graph);
                jz_str_drop(&cache_key);
            } else if (e.code == JZN_E_QueryCapability && err_msg_contains(&e, "PolicyProofCycle")) {
                jz_str_drop(&cache_key);
                jazz_node__node__query_eval__lowering__NodeState__restore_scoped_policy_authorization_graphs(self, lease);
                jz_btree_drop(&grouped); jz_btree_drop(&footprint.tables);
                return e;
            } else if (e.code == JZN_E_QueryCapability) {
                jz_err_drop(&e);
                jazz_node__node__query_eval__lowering__NodeState__begin_scoped_policy_authorization_graph_replacement(self, SL(cache_key), lease);
                jazz_node_PolicyAuthorizationGraph empty = empty_policy_authorization_graph();
                jz_btree_insert(&self->query.policy_authorization_graph_cache, &cache_key, &empty, NULL);
            } else {
                jz_str_drop(&cache_key);
                jazz_node__node__query_eval__lowering__NodeState__restore_scoped_policy_authorization_graphs(self, lease);
                jz_btree_drop(&grouped); jz_btree_drop(&footprint.tables);
                return e;
            }
        }
    }
    jz_btree_drop(&grouped);
    *out = footprint;
    return JZ_OK;
}

/* ---------- scoped policy authorization graph replacements ---------- */
static bool lease_expired(void *lease) { return jz_rc_strong(lease) == 0; }
static bool keep_live_proof(void *item, void *ctx) { (void)ctx; return !lease_expired(((jazz_node_PolicyProofStackEntry *)item)->lease); }
static void proof_entry_drop(void *p) { jz_str_drop(&((jazz_node_PolicyProofStackEntry *)p)->table); }

static void jazz_node__node__query_eval__lowering__NodeState__restore_scoped_policy_authorization_graph(jazz_node_NodeState *self, jz_slice cache_key);

void LWN(restore_expired_policy_compilation_state)(jazz_node_NodeState *self) {
    jz_vec_retain(&self->query.policy_proof_stack, sizeof(jazz_node_PolicyProofStackEntry), keep_live_proof, NULL, proof_entry_drop);
    jz_vec_str expired = {0};
    jz_str *key; ReplacementVec *replacements;
    JZ_BTREE_FOR(&self->query.policy_authorization_graph_replacements, key, replacements)
        if (replacements->len && lease_expired(replacements->ptr[replacements->len - 1].lease)) strvec_push_clone(&expired, key);
    for (u32 i = 0; i < expired.len; i++) {
        for (;;) {
            ReplacementVec *r = jz_btree_get(&self->query.policy_authorization_graph_replacements, &expired.ptr[i]);
            if (!(r && r->len && lease_expired(r->ptr[r->len - 1].lease))) break;
            jazz_node__node__query_eval__lowering__NodeState__restore_scoped_policy_authorization_graph(self, SL(expired.ptr[i]));
        }
    }
    strvec_drop(&expired);
}

bool LWN(policy_authorization_graph_cache_get)(jazz_node_NodeState *self, jz_slice cache_key, jazz_node_PolicyAuthorizationGraph *out) {
    LWN(restore_expired_policy_compilation_state)(self);
    jz_str key = jz_str_from_slice(cache_key);
    const jazz_node_PolicyAuthorizationGraph *g = jz_btree_get(&self->query.policy_authorization_graph_cache, &key);
    jz_str_drop(&key);
    if (!g) return false;
    jazz_node_PolicyAuthorizationGraph_clone(out, g);
    return true;
}

static void jazz_node__node__query_eval__lowering__NodeState__begin_scoped_policy_authorization_graph_replacement(jazz_node_NodeState *self, jz_slice cache_key, void *lease) {
    LWN(restore_expired_policy_compilation_state)(self);
    jz_str key = jz_str_from_slice(cache_key);
    jazz_node_ScopedPolicyAuthorizationGraphReplacement replacement = {0};
    replacement.has_previous = jz_btree_remove(&self->query.policy_authorization_graph_cache, &key, NULL, &replacement.previous);
    replacement.lease = lease; /* Rc::downgrade */
    ReplacementVec *stack = jz_btree_entry(&self->query.policy_authorization_graph_replacements, &key, NULL);
    jz_vec_push(stack, &replacement, sizeof replacement);
}

static void jazz_node__node__query_eval__lowering__NodeState__restore_scoped_policy_authorization_graphs(jazz_node_NodeState *self, void *lease) {
    jz_vec_str keys = {0};
    jz_str *key; ReplacementVec *replacements;
    JZ_BTREE_FOR(&self->query.policy_authorization_graph_replacements, key, replacements)
        if (replacements->len && replacements->ptr[replacements->len - 1].lease == lease) strvec_push_clone(&keys, key);
    for (u32 i = 0; i < keys.len; i++)
        jazz_node__node__query_eval__lowering__NodeState__restore_scoped_policy_authorization_graph(self, SL(keys.ptr[i]));
    strvec_drop(&keys);
}

static void jazz_node__node__query_eval__lowering__NodeState__restore_scoped_policy_authorization_graph(jazz_node_NodeState *self, jz_slice cache_key) {
    jz_str key = jz_str_from_slice(cache_key);
    ReplacementVec *replacements = jz_btree_get(&self->query.policy_authorization_graph_replacements, &key);
    if (!replacements) { jz_str_drop(&key); return; }
    if (replacements->len == 0) jz_panic("policy graph replacement stack is non-empty");
    jazz_node_ScopedPolicyAuthorizationGraphReplacement replacement = replacements->ptr[--replacements->len];
    bool emptied = replacements->len == 0;
    if (emptied) jz_btree_remove(&self->query.policy_authorization_graph_replacements, &key, NULL, NULL);
    jz_btree_remove(&self->query.policy_authorization_graph_cache, &key, NULL, NULL);
    if (replacement.has_previous) {
        jz_btree_insert(&self->query.policy_authorization_graph_cache, &key, &replacement.previous, NULL);
    } else {
        jz_btree_remove(&self->query.policy_authorization_graph_cache, &key, NULL, NULL);
        jz_str_drop(&key);
    }
}

/* ---------- prepared plans and subscriptions ---------- */

/* Fields a routed install publishes from the materialization graph. One-shot
 * materialization may substitute the storage-shaped internal graph for the
 * public app-row terminal; that graph's own fields are published then. */
static jz_err jazz_node__node__query_eval__lowering__NodeState__materialization_app_row_fields(jazz_node_NodeState *self, const jazz_engine_QueryProgram *program,
    const groove_GraphBuilder *graph, jz_vec_str *out) {
    if (jazz_node__node__query_eval__lowering__materializes_internal_app_rows_graph(program)) {
        groove_RecordDescriptor d;
        JZ_TRY(jzn_groove(groove__db__Database__graph_output_descriptor(&self->database, graph, &d)));
        return LW(descriptor_field_names)(d, out);
    }
    return jazz_node__node__query_eval__lowering__app_row_terminal_fields(&program->lowered.output, out);
}

static groove_RecordDescriptor binding_descriptor_for(const jazz_node_PreparedQueryParamVec *params) {
    groove_DescriptorFieldVec fields = {0};
    for (u32 i = 0; i < params->len; i++) {
        groove_ValueType ty; groove_ValueType_clone(&ty, &params->ptr[i].ty);
        groove_DescriptorField f = groove__records__DescriptorField__new(jz_str_clone(&params->ptr[i].name), ty);
        jz_vec_push(&fields, &f, sizeof f);
    }
    return groove__records__RecordDescriptor__new(fields);
}

jz_err LWN(prepared_query_plan_from_program)(jazz_node_NodeState *self, const jazz_engine_QueryProgram *program,
    const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_node_PreparedQueryPlan *out) {
    (void)shape; (void)binding;
    const jazz_engine_AppRowSchema *schema;
    JZ_TRY(jazz_node__node__query_eval__lowering__app_row_terminal_schema(&program->lowered.output, &schema));
    jazz_engine_AppRowSchema output; jazz_engine_AppRowSchema_clone(&output, schema);
    groove_GraphBuilder *graph;
    jz_err e = LW(lowered_materialization_app_rows_graph)(program, &graph);
    if (JZ_IS_ERR(e)) { jazz_engine_AppRowSchema_drop(&output); return e; }
    jz_vec_str app_row_fields;
    e = jazz_node__node__query_eval__lowering__NodeState__materialization_app_row_fields(self, program, graph, &app_row_fields);
    if (JZ_IS_ERR(e)) { groove_GraphBuilder_free(graph); jazz_engine_AppRowSchema_drop(&output); return e; }
    jazz_node_PreparedQueryParamVec params = LW(prepared_params_from_domain)(&program->lowered.parameters);
    jz_vec_str eligible;
    e = LW(app_row_terminal_route_eligible_fields)(&program->lowered.output, &eligible);
    if (JZ_IS_ERR(e)) {
        prepared_params_drop(&params); strvec_drop(&app_row_fields); groove_GraphBuilder_free(graph); jazz_engine_AppRowSchema_drop(&output);
        return e;
    }
    jz_btree route_eligible_fields; jz_btree_init(&route_eligible_fields, &jazz_node_StrSet_desc);
    for (u32 i = 0; i < eligible.len; i++) jz_btree_insert(&route_eligible_fields, &eligible.ptr[i], NULL, NULL);
    jz_free(eligible.ptr);
    /* A terminal may expose only a subset of the program's routes. Keep that
     * terminal's routes as the exact binding-value prefix Groove zips against. */
    jz_vec_str route_params = {0};
    for (u32 i = 0; i < params.len; i++) {
        jz_str field = jazz_node__node__query_eval__lowering__prepared_param_route_field(&params.ptr[i]);
        if (jz_btree_contains(&route_eligible_fields, &field)) strvec_push(&route_params, field);
        else jz_str_drop(&field);
    }
    jz_btree_drop(&route_eligible_fields);
    groove_RecordDescriptor binding_descriptor = binding_descriptor_for(&params);
    __builtin_memset(out, 0, sizeof *out);
    if (params.len == 0) {
        strvec_drop(&route_params); strvec_drop(&app_row_fields); prepared_params_drop(&params);
        out->tag = JZN_PQP_GRAPH; out->graph = graph; out->output = output;
        return JZ_OK;
    }
    jz_str binding_source_shape = program->request.input.binding.has_source_shape
        ? jz_str_clone(&program->request.input.binding.source_shape)
        : jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_prepared_params(params.ptr, params.len);
    jz_vec_str route_fields = route_params;
    U32Vec route_value_indices = jazz_node__node__query_eval__lowering__prepared_route_value_indices(params.ptr, params.len, &route_fields);
    groove_RoutedMultisinkTerminalVec terminals = {0};
    groove_RoutedMultisinkTerminal t;
    t.sink = jz_str_from_slice(app_rows_sink()); t.graph = graph; t.route_fields = route_fields; t.public_fields = app_row_fields;
    t.route_value_indices.ptr = route_value_indices.ptr; t.route_value_indices.len = route_value_indices.len; t.route_value_indices.cap = route_value_indices.cap;
    jz_vec_push(&terminals, &t, sizeof t);
    u64 prepared;
    e = groove__db__Database__prepare(&self->database, terminals, binding_source_shape, binding_descriptor, &prepared);
    if (JZ_IS_ERR(e)) { prepared_params_drop(&params); jazz_engine_AppRowSchema_drop(&output); return jzn_groove(e); }
    out->tag = JZN_PQP_PREPARED; out->shape = prepared; out->params = params; out->output = output;
    return JZ_OK;
}

static void covered_trace_error(const char *stage, const jz_err *e) {
    if (!jazz_types__debug_env__covered_input_trace()) return;
    int n = e->msg ? (int)e->msg->len : 0; const char *p = e->msg ? e->msg->ptr : "";
    jazz_types__debug_env__eprint(jz_format("JAZZ_COVERED_INPUT_TRACE stage=%s error=%.*s", stage, n, p));
}

/* The same installation and binding path serves one-result and retained
 * consumers. Only a first-result private shape leaves a caller-owned
 * retirement obligation; retained bindings own their private or shared shapes. */
static jz_err jazz_node__node__query_eval__lowering__NodeState__install_lowered_program_subscription(jazz_node_NodeState *self, jazz_engine_QueryProgram program,
    const jazz_model_Binding *binding, jz_str binding_source_shape, jazz_node_PreparedClaimBindingMode mode, void *progress_waker,
    groove_SubscriptionLifetime lifetime, groove_RootIndirectValues root_indirect_values, const jz_vec_str *app_row_public_fields /* Option */,
    groove_MultisinkSubscription **out_subscription, bool *has_shape, u64 *out_shape) {
    *has_shape = false;
    /* Subscription opening performs one bounded IVM poll. When that poll finds
     * cold storage, retain the node owner's wake route. */
    jazz_node_PreparedQueryParamVec params = LW(prepared_params_from_domain)(&program.lowered.parameters);
    jz_vec_str route_params = LW(prepared_route_param_names)(&program.lowered.parameters);
    jz_err e;
    if (params.len == 0) {
        u32 n; groove_Sink *sinks = LW(lowered_program_sinks)(&program, &n);
        e = groove__db__Database__subscribe_with_lifetime_and_root_values(&self->database, sinks, n, lifetime, root_indirect_values, progress_waker, out_subscription);
        prepared_params_drop(&params); strvec_drop(&route_params); jz_str_drop(&binding_source_shape);
        jazz_engine_QueryProgram_drop(&program);
        if (JZ_IS_ERR(e)) { covered_trace_error("subscribe_receiver_error", &e); return jzn_groove(e); }
        return JZ_OK;
    }
    groove_RecordDescriptor binding_descriptor = binding_descriptor_for(&params);
    groove_ValueVec values;
    e = jazz_node__node__query_eval__prepared_bindings__binding_values_for_plan(binding, params.ptr, params.len, &program.request.policy, mode, &values);
    if (JZ_IS_ERR(e)) goto fail_early;
    groove_RoutedMultisinkTerminalVec terminals = {0};
    u32 nterm = 0;
    const jazz_engine_LoweredTerminal **exec = jazz_engine__query_engine__lowering__LoweredProgram__execution_terminals(&program.lowered, &nterm);
    for (u32 i = 0; i < nterm; i++) {
        const jazz_engine_LoweredTerminal *terminal = exec[i];
        jz_vec_str public_fields = {0};
        if (app_row_public_fields && str_is(&terminal->sink, app_rows_sink())) {
            jz_vec_clone(&public_fields, app_row_public_fields, sizeof(jz_str), str_clone_v);
        } else {
            e = jazz_node__node__query_eval__lowering__terminal_public_fields(&terminal->output, &public_fields);
            if (JZ_IS_ERR(e)) break;
        }
        jz_vec_str eligible;
        e = LW(terminal_route_eligible_fields)(&terminal->output, &eligible);
        if (JZ_IS_ERR(e)) { strvec_drop(&public_fields); break; }
        jz_vec_str route_fields = LW(terminal_route_fields)(route_params.ptr, route_params.len, eligible.ptr, eligible.len);
        strvec_drop(&eligible);
        U32Vec idx = jazz_node__node__query_eval__lowering__prepared_route_value_indices(params.ptr, params.len, &route_fields);
        groove_RoutedMultisinkTerminal t;
        t.sink = jz_str_clone(&terminal->sink); t.graph = groove_GraphBuilder_clone(terminal->graph);
        t.route_fields = route_fields; t.public_fields = public_fields;
        t.route_value_indices.ptr = idx.ptr; t.route_value_indices.len = idx.len; t.route_value_indices.cap = idx.cap;
        jz_vec_push(&terminals, &t, sizeof t);
    }
    jz_free((void *)exec);
    if (JZ_IS_ERR(e)) { jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop_v); goto fail_early; }
    /* Retained subscribers of identical terminals share one prepared shape,
     * which retires itself with its last retained binding (#4017).
     * First-result reads retire their private shape on return. */
    bool shared_shape = lifetime == GROOVE_SL_RETAINED;
    u64 prepared;
    e = shared_shape
        ? groove__db__Database__prepare_shared(&self->database, terminals, binding_source_shape, binding_descriptor, &prepared)
        : groove__db__Database__prepare(&self->database, terminals, binding_source_shape, binding_descriptor, &prepared);
    prepared_params_drop(&params); strvec_drop(&route_params); jazz_engine_QueryProgram_drop(&program);
    if (JZ_IS_ERR(e)) {
        jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop_v);
        covered_trace_error("prepare_receiver_error", &e);
        return jzn_groove(e);
    }
    /* prepare() allocates a caller-owned shape. Own it before the binding so
     * a failure during cold hydration also releases it. */
    HydrationSubscription owner = { &self->database, NULL, true, prepared, shared_shape };
    e = groove__db__Database__bind_shape_with_lifetime_and_root_values(owner.database, prepared, &values, lifetime, root_indirect_values, progress_waker, out_subscription);
    jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop_v);
    if (JZ_IS_ERR(e)) {
        covered_trace_error("bind_receiver_error", &e);
        jazz_node__node__query_eval__lowering__HydrationSubscription__Drop__drop(&owner);
        return jzn_groove(e);
    }
    owner.has_prepared_shape = false;
    jazz_node__node__query_eval__lowering__HydrationSubscription__Drop__drop(&owner);
    if (lifetime == GROOVE_SL_FIRST_RESULT) { *has_shape = true; *out_shape = prepared; }
    return JZ_OK;
fail_early:
    prepared_params_drop(&params); strvec_drop(&route_params); jz_str_drop(&binding_source_shape);
    jazz_engine_QueryProgram_drop(&program);
    return e;
}

/* Choose which root fields the retained subscription rebuilds into logical
 * large values. Fields kept physical must be dropped, or hydrated, before rows
 * cross a public boundary. */
jz_err LWN(subscribe_lowered_program_with_root_values)(jazz_node_NodeState *self, jazz_engine_QueryProgram program, const jazz_model_Binding *binding,
    jz_str binding_source_shape, jazz_node_PreparedClaimBindingMode mode, void *progress_waker, groove_RootIndirectValues root_indirect_values,
    groove_MultisinkSubscription **out) {
    bool has_shape; u64 shape;
    return jazz_node__node__query_eval__lowering__NodeState__install_lowered_program_subscription(self, program, binding, binding_source_shape, mode,
        progress_waker, GROOVE_SL_RETAINED, root_indirect_values, NULL, out, &has_shape, &shape);
}

static bool keep_app_rows_terminal(void *item, void *ctx) {
    (void)ctx;
    return str_is(&((jazz_engine_LoweredTerminal *)item)->sink, app_rows_sink());
}
static void lowered_terminal_drop(void *p) {
    jazz_engine_LoweredTerminal *t = p;
    jz_str_drop(&t->sink);
    groove_GraphBuilder_free(t->graph);
    /* output schema owned by the engine mirror; dropped with the program */
}

/* `root_indirect_values` decides which root fields the result rebuilds into
 * logical large values. */
jz_err LWN(hydrate_lowered_program_once)(jazz_node_NodeState *self, jazz_engine_QueryProgram program, const jazz_model_Binding *binding,
    groove_RootIndirectValues root_indirect_values, groove_RecordDeltas **out) {
    /* Hydrate through the same live installation as a retained consumer. */
    groove_GraphBuilder *graph;
    jz_err e = LW(lowered_materialization_app_rows_graph)(&program, &graph);
    if (JZ_IS_ERR(e)) { jazz_engine_QueryProgram_drop(&program); return e; }
    bool has_public = false; jz_vec_str app_row_public_fields = {0};
    if (jazz_node__node__query_eval__lowering__materializes_internal_app_rows_graph(&program)) {
        e = jazz_node__node__query_eval__lowering__NodeState__materialization_app_row_fields(self, &program, graph, &app_row_public_fields);
        if (JZ_IS_ERR(e)) { groove_GraphBuilder_free(graph); jazz_engine_QueryProgram_drop(&program); return e; }
        has_public = true;
    }
    jz_vec_retain(&program.lowered.terminals, sizeof(jazz_engine_LoweredTerminal), keep_app_rows_terminal, NULL, lowered_terminal_drop);
    if (program.lowered.terminals.len == 0) jz_panic("index out of bounds: the len is 0 but the index is 0");
    groove_GraphBuilder_free(program.lowered.terminals.ptr[0].graph);
    program.lowered.terminals.ptr[0].graph = graph;
    jz_str binding_source_shape;
    if (program.request.input.binding.has_source_shape) {
        binding_source_shape = jz_str_clone(&program.request.input.binding.source_shape);
    } else {
        jazz_node_PreparedQueryParamVec params = LW(prepared_params_from_domain)(&program.lowered.parameters);
        binding_source_shape = jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_prepared_params(params.ptr, params.len);
        prepared_params_drop(&params);
    }
    groove_MultisinkSubscription *subscription; bool has_shape; u64 prepared_shape;
    e = jazz_node__node__query_eval__lowering__NodeState__install_lowered_program_subscription(self, program, binding, binding_source_shape,
        JZN_PCBM_STRICT, NULL, GROOVE_SL_FIRST_RESULT, root_indirect_values, has_public ? &app_row_public_fields : NULL,
        &subscription, &has_shape, &prepared_shape);
    if (has_public) strvec_drop(&app_row_public_fields);
    JZ_TRY(e);
    HydrationSubscription owner = { &self->database, subscription, has_shape, prepared_shape, false };
    /* Unlike a retained consumer, this call owns progress until its complete
     * initial multisink update, including an empty result. */
    u32 tag = 0; groove_MultisinkDeltas *snapshot = NULL; jz_err event_error = JZ_OK;
    jz_err result = groove__db__Database__poll_initial_event(owner.database, owner.subscription, &tag, &snapshot, &event_error);
    jz_err release = jazz_node__node__query_eval__lowering__HydrationSubscription__release(&owner);
    if (JZ_IS_ERR(result)) { jz_err_drop(&release); return jzn_groove(result); }
    if (tag == GROOVE_SE_ERROR) { jz_err_drop(&release); return jzn_groove(event_error); }
    if (JZ_IS_ERR(release)) { if (snapshot) { groove_MultisinkDeltas_drop(snapshot); jz_free(snapshot); } return release; }
    return jazz_node__node__query_eval__take_required_sink_deltas(snapshot, app_rows_sink(), out);
}
