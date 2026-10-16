/* lowering/templates.rs: source preparation is per-instance; pure relational
 * lowering is per family. The cache holds only immutable, unbound descriptions.
 *
 * trace_template() only prints under cfg(test)/feature "testing"; in the shipped
 * build it is a no-op, so its call sites are omitted here. The `hits` and
 * `argument_hits` counters are cfg(test/testing) fields and are omitted too. */
#include "../lowering.h"

#define JAZZ_ENGINE_TEMPLATE_CACHE_CAPACITY 256u

/* TemplateState { Seen, Unsupported, Ready(Arc<ParameterizedProgram>) } */
enum { TEMPLATE_SEEN, TEMPLATE_UNSUPPORTED, TEMPLATE_READY };
typedef struct ParameterizedProgram {
    jazz_engine_QueryProgram program;
    jazz_engine_ProgramArgumentRecipes arguments;
} ParameterizedProgram;
typedef struct TemplateEntry {
    jazz_engine_QueryProgramRequest request;
    jazz_engine_OptValueTypeVec literal_types;
    jz_btree sources; /* ResolvedQuerySources */
    u8 program_tag;
    ParameterizedProgram *ready; /* Arc payload (TEMPLATE_READY) */
} TemplateEntry;

static void parameterized_program_drop(void *p) {
    ParameterizedProgram *pp = p;
    JE_LOW(QueryProgram__drop)(&pp->program);
    JE_ARG(ProgramArgumentRecipes__drop)(&pp->arguments);
}
static void template_entry_drop_v(void *p) {
    TemplateEntry *e = p;
    JE_IN(QueryProgramRequest__drop)(&e->request);
    jz_vec_drop(&e->literal_types, sizeof(jazz_engine_OptValueType), jazz_engine__opt_vt_drop_v);
    jz_btree_drop(&e->sources);
    if (e->program_tag == TEMPLATE_READY) jz_rc_release(e->ready);
}

void JE_TPL(QueryProgramTemplateCache__Default__default)(jazz_engine_QueryProgramTemplateCache *c) {
    jz_deque_init(&c->entries, sizeof(TemplateEntry));
    c->physical = groove__ivm__runtime__typed_template__TypedGraphTemplateCache__default();
}
void JE_TPL(QueryProgramTemplateCache__drop)(jazz_engine_QueryProgramTemplateCache *c) {
    jz_deque_drop(&c->entries, template_entry_drop_v);
    groove__ivm__runtime__typed_template__TypedGraphTemplateCache__drop(c->physical);
}
void JE_TPL(QueryProgramTemplateCache__clear)(jazz_engine_QueryProgramTemplateCache *c) {
    TemplateEntry e;
    while (jz_deque_pop_front(&c->entries, &e)) template_entry_drop_v(&e);
    groove__ivm__runtime__typed_template__TypedGraphTemplateCache__clear(c->physical);
}

/* terminal graphs, then the internal app-rows graph when present */
static void program_graphs(const jazz_engine_QueryProgram *program, groove_GraphBuilderVec *out) {
    groove_GraphBuilderVec graphs = {0};
    JZ_VEC_FOR(const jazz_engine_LoweredTerminal, t, program->lowered.terminals) JZ_VEC_PUSH(graphs, groove_gb_clone(t->graph));
    if (program->lowered.internal_app_rows_graph) JZ_VEC_PUSH(graphs, groove_gb_clone(program->lowered.internal_app_rows_graph));
    *out = graphs;
}
/* moves replacement graphs back into the program in program_graphs order */
static void program_replace_graphs(jazz_engine_QueryProgram *program, groove_GraphBuilderVec *graphs, const char *what) {
    u32 next = 0;
    JZ_VEC_FOR(jazz_engine_LoweredTerminal, t, program->lowered.terminals) {
        if (next >= graphs->len) jz_panic(what);
        groove_gb_drop(t->graph);
        t->graph = graphs->ptr[next++];
    }
    if (program->lowered.internal_app_rows_graph) {
        groove_gb_drop(program->lowered.internal_app_rows_graph);
        program->lowered.internal_app_rows_graph = next < graphs->len ? graphs->ptr[next++] : NULL;
    }
    for (u32 i = next; i < graphs->len; i++) groove_gb_drop(graphs->ptr[i]);
    jz_vec_drop(graphs, sizeof(groove_GraphBuilder *), NULL);
}

static void typed_program(jazz_engine_QueryProgramTemplateCache *self, jazz_engine_QueryProgram *program) {
    groove_GraphBuilderVec graphs, typed = {0};
    program_graphs(program, &graphs);
    jz_err e = groove__ivm__runtime__typed_template__TypedGraphTemplateCache__compile(self->physical, graphs.ptr, graphs.len, &typed);
    jz_vec_drop(&graphs, sizeof(groove_GraphBuilder *), groove__ivm__graph__GraphBuilder__drop_v);
    if (e.code) { jz_err_drop(&e); return; }
    program_replace_graphs(program, &typed, "typed terminal");
}

/* Source graphs in the fixed order used for blueprint slot numbering. */
static void source_graph_list(const jz_btree *sources, jazz_engine_GraphRefVec *out) {
    jazz_engine_GraphRefVec graphs = {0};
    const jazz_engine_SourceId *k;
    const jazz_engine_ResolvedSource *s;
    JZ_BTREE_FOR(sources, k, s) {
        JZ_VEC_PUSH(graphs, (const groove_GraphBuilder *)s->graph);
        if (s->has_content_version) JZ_VEC_PUSH(graphs, (const groove_GraphBuilder *)s->content_version.graph);
        if (s->has_deletion_register) JZ_VEC_PUSH(graphs, (const groove_GraphBuilder *)s->deletion_register.graph);
        if (s->has_authorized_deletion_preimage) JZ_VEC_PUSH(graphs, (const groove_GraphBuilder *)s->authorized_deletion_preimage.graph);
    }
    *out = graphs;
}

/* derived PartialEq for SourceMetadataFields / SourceRowShape */
static bool optstr_parts_eq(bool ha, const jz_str *a, bool hb, const jz_str *b) { return ha == hb && (!ha || jz_str_eq(a, b)); }
static bool metadata_fields_eq_v(const void *x, const void *y) {
    const jazz_engine_SourceMetadataFields *a = x, *b = y;
    return a->tag == b->tag && jz_str_eq(&a->a, &b->a) && jz_str_eq(&a->b, &b->b) && jz_str_eq(&a->c, &b->c) &&
           optstr_parts_eq(a->has_opt0, &a->opt0, b->has_opt0, &b->opt0) && optstr_parts_eq(a->has_opt1, &a->opt1, b->has_opt1, &b->opt1);
}
static bool row_shape_eq(const jazz_engine_SourceRowShape *a, const jazz_engine_SourceRowShape *b) {
    return sid_eq(&a->source, &b->source) && a->descriptor == b->descriptor && jz_str_eq(&a->row_uuid_field, &b->row_uuid_field) &&
           jz_btree_eq(&a->metadata, &b->metadata, metadata_fields_eq_v);
}

/* Everything a resolved source contributes to a family key except its graphs. */
static bool source_metadata_equal(const jz_btree *left, const jz_btree *right) {
    if (left->len != right->len) return false;
    jz_btree_iter il, ir;
    jz_btree_iter_begin(&il, left);
    jz_btree_iter_begin(&ir, right);
    const jazz_engine_SourceId *lk, *rk;
    const jazz_engine_ResolvedSource *l, *r;
    while (jz_btree_next(&il, (void **)&lk, (void **)&l) && jz_btree_next(&ir, (void **)&rk, (void **)&r)) {
        if (!sid_eq(lk, rk) || !jz_btree_eq(&l->stored_column_ids, &r->stored_column_ids, jz_uuid_eq_v) ||
            !jazz_model__schema__TableSchema__PartialEq__eq(l->table_schema, r->table_schema) || !row_shape_eq(&l->row_shape, &r->row_shape) ||
            !jz_btree_eq(&l->routing_fields, &r->routing_fields, NULL) || l->requires_result_payload != r->requires_result_payload ||
            !optstr_parts_eq(l->has_content_version, &l->content_version.row_uuid_field, r->has_content_version, &r->content_version.row_uuid_field) ||
            !optstr_parts_eq(l->has_deletion_register, &l->deletion_register.row_uuid_field, r->has_deletion_register, &r->deletion_register.row_uuid_field) ||
            l->has_authorized_deletion_preimage != r->has_authorized_deletion_preimage ||
            (l->has_authorized_deletion_preimage &&
             !jz_btree_eq(&l->authorized_deletion_preimage.routing_fields, &r->authorized_deletion_preimage.routing_fields, NULL)))
            return false;
    }
    return true;
}

static bool literal_types_eq(const jazz_engine_OptValueTypeVec *a, const jazz_engine_OptValueTypeVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) {
        if (a->ptr[i].some != b->ptr[i].some) return false;
        if (a->ptr[i].some && !groove__records__values__ValueType__PartialEq__eq(&a->ptr[i].ty, &b->ptr[i].ty)) return false;
    }
    return true;
}

/* ordinary concrete path: consumes compilation, sources and explain */
static jazz_engine_CapErr fallback(jazz_engine_QueryProgramCompilation compilation, jz_btree sources, jazz_engine_ExplainPlan explain,
                                   jazz_engine_QueryProgram *out) {
    return JE_LOW(lower_resolved_query_program)(compilation, sources, explain, out);
}

jazz_engine_CapErr JE_TPL(QueryProgramTemplateCache__lower)(jazz_engine_QueryProgramTemplateCache *self, jazz_engine_QueryProgramCompilation compilation,
    jz_btree sources, jazz_engine_ExplainPlan explain, groove_TemplateDescribeFn describe, void *describe_ctx, jazz_engine_QueryProgram *out) {
    /* Pure lowering has no access to values: every dynamic use must emit a bind-time recipe,
     * or fail closed to ordinary concrete lowering. */
    jazz_engine_QueryProgramRequest template_request;
    JE_IN(QueryProgramRequest__Clone__clone)(&template_request, &compilation.request);
    groove_ValueVec literals = {0};
    jazz_engine_UrErr ue = JE_ARG(extract_template_literals)(&template_request, &literals);
    if (ue) {
        jazz_engine__ur_free(ue);
        JE_IN(QueryProgramRequest__drop)(&template_request);
        return fallback(compilation, sources, explain, out);
    }
    jazz_engine_OptValueTypeVec literal_types = {0};
    JZ_VEC_FOR(const groove_Value, value, literals) {
        groove_Value copy;
        groove__records__values__Value__Clone__clone(&copy, value);
        groove_LiteralValue lit;
        groove__ivm__op_types__LiteralValue__From_Value__from(&lit, &copy);
        jazz_engine_OptValueType t;
        memset(&t, 0, sizeof t);
        t.some = groove__ivm__op_types__LiteralValue__value_type(&lit, &t.ty);
        groove__ivm__op_types__LiteralValue__drop(&lit);
        JZ_VEC_PUSH(literal_types, t);
    }
    jz_btree_clear(&template_request.input.binding.values);
    memset(&template_request.input.binding.id, 0, sizeof template_request.input.binding.id); /* BindingId(Uuid::nil()) */
    jazz_engine_GraphRefVec source_graphs;
    source_graph_list(&sources, &source_graphs);
    /* A family hit matches the fresh sources against the stored blueprints in place. */
    bool found = false;
    u32 index = 0;
    groove_TemplateGraphInputVec inputs = {0};
    for (u32 i = 0; i < self->entries.len; i++) {
        TemplateEntry *entry = jz_deque_at(&self->entries, i);
        if (!jz_uuid_eq(&entry->request.input.shape.identity.shape_id.u, &template_request.input.shape.identity.shape_id.u) ||
            !literal_types_eq(&entry->literal_types, &literal_types) || !source_metadata_equal(&entry->sources, &sources) ||
            !JE_IN(QueryProgramRequest__PartialEq__eq)(&entry->request, &template_request))
            continue;
        jazz_engine_GraphRefVec blueprints;
        source_graph_list(&entry->sources, &blueprints);
        bool matched = false;
        jz_err e = groove__ivm__template__sources__match_template_sources((groove_GraphBuilder *const *)source_graphs.ptr, source_graphs.len,
                                                                          (groove_GraphBuilder *const *)blueprints.ptr, blueprints.len, describe,
                                                                          describe_ctx, &matched, &inputs);
        jz_vec_drop(&blueprints, sizeof(void *), NULL);
        if (e.code) {
            /* descriptor_fallback */
            jz_err_drop(&e);
            goto concrete;
        }
        if (matched) { found = true; index = i; break; }
    }
    if (!found) {
        /* First sighting: record the family's blueprint and take the ordinary concrete path. */
        groove_GraphBuilderVec blueprints = {0};
        groove_TemplateGraphInputVec unused = {0};
        jz_err e = groove__ivm__template__sources__split_template_sources((groove_GraphBuilder *const *)source_graphs.ptr, source_graphs.len, describe,
                                                                          describe_ctx, &blueprints, &unused);
        if (e.code) { jz_err_drop(&e); goto concrete; }
        jz_vec_drop(&unused, sizeof(groove_TemplateGraphInput), groove__ivm__template__TemplateGraphInput__drop_v);
        jz_btree template_sources;
        jz_btree_clone(&template_sources, &sources, jazz_engine__sid_clone_v, jazz_engine__resolved_clone_v);
        u32 next = 0;
#define NEXT_BLUEPRINT(slot, what) do { if (next >= blueprints.len) jz_panic(what); groove_gb_drop(slot); slot = blueprints.ptr[next++]; } while (0)
        const jazz_engine_SourceId *k;
        jazz_engine_ResolvedSource *s;
        JZ_BTREE_FOR(&template_sources, k, s) {
            NEXT_BLUEPRINT(s->graph, "source blueprint");
            if (s->has_content_version) NEXT_BLUEPRINT(s->content_version.graph, "content blueprint");
            if (s->has_deletion_register) NEXT_BLUEPRINT(s->deletion_register.graph, "deletion blueprint");
            if (s->has_authorized_deletion_preimage) NEXT_BLUEPRINT(s->authorized_deletion_preimage.graph, "preimage blueprint");
        }
#undef NEXT_BLUEPRINT
        for (u32 i = next; i < blueprints.len; i++) groove_gb_drop(blueprints.ptr[i]);
        jz_vec_drop(&blueprints, sizeof(groove_GraphBuilder *), NULL);
        if (self->entries.len == JAZZ_ENGINE_TEMPLATE_CACHE_CAPACITY) {
            TemplateEntry old;
            jz_deque_pop_front(&self->entries, &old);
            template_entry_drop_v(&old);
        }
        TemplateEntry entry;
        memset(&entry, 0, sizeof entry);
        entry.request = template_request;
        entry.literal_types = literal_types;
        entry.sources = template_sources;
        entry.program_tag = TEMPLATE_SEEN;
        jz_deque_push_back(&self->entries, &entry);
        /* seen */
        jz_vec_drop(&source_graphs, sizeof(void *), NULL);
        jz_vec_drop(&literals, sizeof(groove_Value), groove__records__values__Value__drop_v);
        return fallback(compilation, sources, explain, out);
    }
    jz_vec_drop(&source_graphs, sizeof(void *), NULL);
    TemplateEntry *entry = jz_deque_at(&self->entries, index);
    ParameterizedProgram *template_;
    switch (entry->program_tag) {
    case TEMPLATE_READY:
        template_ = jz_rc_clone(entry->ready);
        JE_IN(QueryProgramRequest__drop)(&template_request);
        jz_vec_drop(&literal_types, sizeof(jazz_engine_OptValueType), jazz_engine__opt_vt_drop_v);
        break;
    case TEMPLATE_UNSUPPORTED:
        /* unsupported_hit */
        JE_IN(QueryProgramRequest__drop)(&template_request);
        jz_vec_drop(&literal_types, sizeof(jazz_engine_OptValueType), jazz_engine__opt_vt_drop_v);
        goto concrete_inputs;
    default: {
        jazz_engine_ProgramArgumentRecipes arguments;
        JE_ARG(ProgramArgumentRecipes__with_literal_types)(&arguments, literal_types);
        jazz_engine_QueryProgramCompilation template_compilation;
        jazz_engine_QueryProgram program;
        jazz_engine_CapErr err = JE_LOW(QueryProgramCompilation__analyze)(template_request, &template_compilation);
        if (!err) {
            jz_btree entry_sources, no_parameters;
            jz_btree_clone(&entry_sources, &entry->sources, jazz_engine__sid_clone_v, jazz_engine__resolved_clone_v);
            jz_btree_init(&no_parameters, &jazz_engine__U32_PARAM_DOMAIN_MAP_DESC);
            jazz_engine_ExplainPlan empty;
            memset(&empty, 0, sizeof empty);
            err = JE_LOW(lower_resolved_query_program_with_source_parameters)(template_compilation, entry_sources, empty, &no_parameters, &arguments, &program);
            jz_btree_drop(&no_parameters);
        }
        if (err) {
            /* Some projections and predicates still require a concrete scalar. */
            JE_RES(CapabilityReport__free)(err);
            JE_ARG(ProgramArgumentRecipes__drop)(&arguments);
            entry->program_tag = TEMPLATE_UNSUPPORTED;
            goto concrete_inputs;
        }
        typed_program(self, &program);
        template_ = jz_rc_new(sizeof(ParameterizedProgram), parameterized_program_drop);
        template_->program = program;
        template_->arguments = arguments;
        entry->program_tag = TEMPLATE_READY;
        entry->ready = jz_rc_clone(template_);
        break;
    }
    }
    /* bound */
    jazz_engine_QueryProgram program;
    JE_LOW(QueryProgram__Clone__clone)(&program, &template_->program);
    groove_GraphBuilderVec graphs, bound = {0};
    program_graphs(&program, &graphs);
    ue = JE_ARG(ProgramArgumentRecipes__bind)(&template_->arguments, graphs.ptr, graphs.len, inputs.ptr, inputs.len, &compilation.request,
                                              literals.ptr, literals.len, &bound);
    jz_vec_drop(&graphs, sizeof(groove_GraphBuilder *), groove__ivm__graph__GraphBuilder__drop_v);
    jz_rc_release(template_);
    if (ue) {
        /* argument_fallback */
        jazz_engine__ur_free(ue);
        JE_LOW(QueryProgram__drop)(&program);
        goto concrete_inputs;
    }
    program_replace_graphs(&program, &bound, "terminal bound");
    JE_IN(QueryProgramRequest__drop)(&program.request);
    program.request = compilation.request;
    JE_PLAN(AnalyzedQueryPlan__drop)(compilation.plan);
    jz_free(compilation.plan);
    jz_vec_drop(&compilation.sources, sizeof(jazz_engine_SourceRequest), jazz_engine__source_request_drop_v);
    jazz_engine_StrVec capabilities;
    jazz_engine__strvec_clone(&capabilities, &program.explain.capabilities);
    JE_RES(ExplainPlan__drop)(&program.explain);
    program.explain = JE_LOW(explain_with_request)(&program.request, explain);
    jz_vec_extend(&program.explain.capabilities, capabilities.ptr, capabilities.len, sizeof(jz_str));
    jz_vec_drop(&capabilities, sizeof(jz_str), NULL);
    jz_vec_drop(&inputs, sizeof(groove_TemplateGraphInput), groove__ivm__template__TemplateGraphInput__drop_v);
    jz_vec_drop(&literals, sizeof(groove_Value), groove__records__values__Value__drop_v);
    jz_btree_drop(&sources);
    *out = program;
    return NULL;

concrete:
    jz_vec_drop(&source_graphs, sizeof(void *), NULL);
    JE_IN(QueryProgramRequest__drop)(&template_request);
    jz_vec_drop(&literal_types, sizeof(jazz_engine_OptValueType), jazz_engine__opt_vt_drop_v);
concrete_inputs:
    jz_vec_drop(&inputs, sizeof(groove_TemplateGraphInput), groove__ivm__template__TemplateGraphInput__drop_v);
    jz_vec_drop(&literals, sizeof(groove_Value), groove__records__values__Value__drop_v);
    return fallback(compilation, sources, explain, out);
}
