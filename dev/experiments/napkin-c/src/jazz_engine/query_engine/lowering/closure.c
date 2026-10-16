/* lowering/closure.rs: closure-path membership and required-include lowering.
 * Kept apart from terminal construction so the authorization-shaped graph
 * construction can be audited in isolation. */
#include "../lowering.h"

#define F(x) jazz_engine__query_engine__fields__##x
#define PF(x) groove__ivm__graph__ProjectField__##x
#define GB(x) groove__ivm__graph__GraphBuilder__##x
#define CRE JAZZ_ENGINE_CLOSURE_REQUIRED_ELEMENT

static jazz_engine_StrVec two(jz_str a, jz_str b) { jazz_engine_StrVec v = {0}; JZ_VEC_PUSH(v, a); JZ_VEC_PUSH(v, b); return v; }

void JE_CLO(ClosureLowering__drop)(jazz_engine_ClosureLowering *c) {
    groove_gb_drop(c->visible_root);
    jz_btree_drop(&c->result_members);
}

/* ---------------------------------------------------------------- closure_path_segments */
void JE_CLO(closure_path_segments)(const jazz_engine_ClosurePath *path, jazz_engine_SegmentRefVec *out) {
    jazz_engine_SegmentRefVec v = {0};
    if (path->tag == JAZZ_ENGINE_CLOSURE_IMPLICIT_ROOT_REFERENCE) {
        JZ_VEC_PUSH(v, &path->segment);
    } else {
        JZ_VEC_FOR(const jazz_engine_ClosurePathSegment, s, path->segments) JZ_VEC_PUSH(v, s);
    }
    *out = v;
}

/* ---------------------------------------------------------------- helpers */
static jz_str source_key_for_required(const groove_ValueType *source_key_type, const jz_str *source_key) {
    return source_key_type->tag == GROOVE_VT_ARRAY ? jz_str_from(CRE) : jz_str_clone(source_key);
}

/* consumes graph and field; returns the innermost non-nullable type through *value_type */
static groove_GraphBuilder *unwrap_nullable_layers(groove_GraphBuilder *graph, jz_str field, const groove_ValueType **value_type) {
    const groove_ValueType *t = *value_type;
    while (t->tag == GROOVE_VT_NULLABLE) {
        graph = GB(unwrap_nullable)(graph, jz_str_clone(&field));
        t = t->u.inner;
    }
    jz_str_drop(&field);
    *value_type = t;
    return graph;
}

static jazz_engine_CapErr not_projected(const jz_str *source_key) {
    JE_DBG(d, jazz_engine__fmt__str, source_key);
    jazz_engine_CapErr e = jazz_engine__cap_operator_fmt("closure source field %.*s is not projected", JE_STR_ARG(d));
    jz_str_drop(&d);
    return e;
}

/* ---------------------------------------------------------------- required includes */
static jazz_engine_CapErr required_closure_parent_graph_from_segment(groove_GraphBuilder *parent_graph, const jazz_engine_ClosurePathSegment *segments,
    u32 nsegments, u32 index, u8 root_gate, const jazz_engine_ResolvedSource *parent_source, const jz_btree *resolved_sources,
    const jz_btree *route_fields, groove_GraphBuilder **out) {
    if (index >= nsegments) { *out = parent_graph; return NULL; }
    const jazz_engine_ClosurePathSegment *segment = &segments[index];
    const jazz_engine_ResolvedSource *target;
    jazz_engine_CapErr err = JE_RES(ResolvedSource__lookup)(resolved_sources, &segment->target, "closure target", &target);
    if (err) { groove_gb_drop(parent_graph); return err; }
    jz_btree no_route_fields;
    jz_btree_init(&no_route_fields, &jazz_engine__STRSET_DESC);
    groove_GraphBuilder *target_valid;
    err = required_closure_parent_graph_from_segment(groove_gb_clone(target->graph), segments, nsegments, index + 1, root_gate, target,
                                                     resolved_sources, &no_route_fields, &target_valid);
    if (err) { groove_gb_drop(parent_graph); return err; }
    jz_str source_key = F(user_column_field)(JZ_S(&segment->source_field));
    const groove_ValueType *source_key_type = JE_GL(source_field_type)(parent_source, JZ_S(&source_key));
    if (!source_key_type) {
        err = not_projected(&source_key);
        jz_str_drop(&source_key);
        groove_gb_drop(parent_graph);
        groove_gb_drop(target_valid);
        return err;
    }
    jz_str parent_row_uuid = jz_str_clone(&parent_source->row_shape.row_uuid_field);
    jz_str target_row_uuid = jz_str_clone(&target->row_shape.row_uuid_field);
    const groove_ValueType *required_key_type = source_key_type;
    groove_GraphBuilder *required_base = unwrap_nullable_layers(groove_gb_clone(parent_graph), jz_str_clone(&source_key), &required_key_type);
    bool is_array = required_key_type->tag == GROOVE_VT_ARRAY;
    groove_GraphBuilder *required = is_array ? GB(unnest)(required_base, jz_str_clone(&source_key), jz_str_from(CRE)) : required_base;
    jz_str left_key = is_array ? jz_str_from(CRE) : jz_str_clone(&source_key);
    bool left_is_element = jz_str_eq_cstr(&left_key, CRE);
    groove_ProjectFieldVec covered_fields = JE_TERM(project_source_fields_with_routes_from_prefix)(parent_source, JAZZ_ENGINE_LEFT_JOIN_PREFIX, route_fields);
    if (left_is_element) JZ_VEC_PUSH(covered_fields, PF(renamed)(jz_str_from("left.__closure_required_element"), jz_str_from(CRE)));
    groove_GraphBuilder *covered = GB(project_fields)(
        GB(join)(groove_gb_clone(required), target_valid, jazz_engine__strvec_one(jz_str_clone(&left_key)), jazz_engine__strvec_one(target_row_uuid)),
        covered_fields);
    if (root_gate == JAZZ_ENGINE_GATE_INNER && !is_array) {
        /* The unwrapped copy is only a predicate witness: select the authoritative parent rows by identity. */
        groove_ProjectFieldVec named = {0};
        JZ_VEC_PUSH(named, PF(named)(jz_str_clone(&parent_row_uuid)));
        groove_GraphBuilder *covered_roots = GB(project_fields)(covered, named);
        *out = GB(project_fields)(GB(semi_join)(parent_graph, covered_roots, jazz_engine__strvec_one(jz_str_clone(&parent_row_uuid)),
                                                jazz_engine__strvec_one(parent_row_uuid)),
                                  JE_TERM(project_source_fields_with_routes)(parent_source, route_fields));
        groove_gb_drop(required);
        jz_str_drop(&left_key);
        jz_str_drop(&source_key);
        return NULL;
    }
    groove_GraphBuilder *missing_join;
    jz_str right_key = source_key_for_required(required_key_type, &source_key);
    if (left_is_element)
        missing_join = GB(anti_join)(groove_gb_clone(required), groove_gb_clone(covered), two(jz_str_clone(&parent_row_uuid), left_key),
                                     two(jz_str_clone(&parent_row_uuid), right_key));
    else
        missing_join = GB(anti_join)(groove_gb_clone(required), groove_gb_clone(covered), jazz_engine__strvec_one(left_key), jazz_engine__strvec_one(right_key));
    groove_GraphBuilder *missing = GB(project_fields)(missing_join, JE_TERM(project_source_fields_with_routes)(parent_source, route_fields));
    groove_gb_drop(required);
    jz_str_drop(&source_key);
    groove_GraphBuilder *all_required_refs_resolve =
        GB(anti_join)(parent_graph, missing, jazz_engine__strvec_one(jz_str_clone(&parent_row_uuid)), jazz_engine__strvec_one(jz_str_clone(&parent_row_uuid)));
    if (root_gate == JAZZ_ENGINE_GATE_REQUIRED) {
        groove_gb_drop(covered);
        jz_str_drop(&parent_row_uuid);
        *out = all_required_refs_resolve;
        return NULL;
    }
    groove_GraphBuilder *first_cover =
        GB(arg_min_by)(covered, jazz_engine__strvec_one(jz_str_clone(&parent_row_uuid)), jazz_engine__strvec_one(jz_str_clone(&parent_row_uuid)));
    *out = GB(project_fields)(GB(join)(all_required_refs_resolve, first_cover, jazz_engine__strvec_one(jz_str_clone(&parent_row_uuid)),
                                       jazz_engine__strvec_one(parent_row_uuid)),
                              JE_TERM(project_source_fields_with_routes_from_prefix)(parent_source, JAZZ_ENGINE_LEFT_JOIN_PREFIX, route_fields));
    return NULL;
}

static jazz_engine_CapErr required_closure_parent_graph(groove_GraphBuilder *parent_graph, const jazz_engine_ClosurePathSegmentVec *segments, u8 root_gate,
    const jazz_engine_ResolvedSource *root_source, const jz_btree *resolved_sources, const jz_btree *route_fields, groove_GraphBuilder **out) {
    return required_closure_parent_graph_from_segment(parent_graph, segments->ptr, segments->len, 0, root_gate, root_source, resolved_sources,
                                                      route_fields, out);
}

/* ---------------------------------------------------------------- closure membership per path */
typedef struct { u32 index; jazz_engine_SourceId source; groove_GraphBuilder *graph; } ClosureOutput;
typedef JZ_VEC(ClosureOutput) ClosureOutputVec;
static void closure_output_drop_v(void *p) {
    ClosureOutput *o = p;
    sid_drop(&o->source);
    groove_gb_drop(o->graph);
}

static jazz_engine_CapErr closure_membership_graph_for_path(groove_GraphBuilder *root_graph, const jazz_engine_ClosurePath *path,
    const jazz_engine_ResolvedSource *root_source, const jz_btree *resolved_sources, const jz_btree *route_fields, ClosureOutputVec *out) {
    jazz_engine_SegmentRefVec segments;
    JE_CLO(closure_path_segments)(path, &segments);
    bool can_lower_as_parent_semijoin = route_fields->len == 0 && path->tag == JAZZ_ENGINE_CLOSURE_IMPLICIT_ROOT_REFERENCE;
    groove_ProjectFieldVec first = JE_TERM(project_source_fields_with_routes)(root_source, route_fields);
    JZ_VEC_PUSH(first, PF(renamed)(jz_str_clone(&root_source->row_shape.row_uuid_field), jz_str_from("__closure_root_row_uuid")));
    groove_GraphBuilder *current_graph = GB(project_fields)(root_graph, first);
    /* Keep references to source shape metadata rather than cloning whole resolved sources. */
    const jazz_engine_ResolvedSource *current_source = root_source;
    /* Binding routes the current graph carries: later joins key on the routes both sides carry. */
    jz_btree carried_routes;
    jazz_engine__strset_clone(&carried_routes, route_fields);
    ClosureOutputVec outputs = {0};
    jazz_engine_CapErr err = NULL;
    for (u32 index = 0; index < segments.len; index++) {
        const jazz_engine_ClosurePathSegment *segment = segments.ptr[index];
        const jazz_engine_ResolvedSource *target;
        err = JE_RES(ResolvedSource__lookup)(resolved_sources, &segment->target, "closure target", &target);
        if (err) goto fail;
        jz_str source_key = F(user_column_field)(JZ_S(&segment->source_field));
        const groove_ValueType *source_key_type = JE_GL(source_field_type)(current_source, JZ_S(&source_key));
        if (!source_key_type) {
            err = not_projected(&source_key);
            jz_str_drop(&source_key);
            goto fail;
        }
        groove_GraphBuilder *joined;
        if (can_lower_as_parent_semijoin) {
            const groove_ValueType *kt = source_key_type;
            groove_GraphBuilder *source_base = unwrap_nullable_layers(current_graph, jz_str_clone(&source_key), &kt);
            current_graph = NULL;
            groove_GraphBuilder *source_keys =
                kt->tag == GROOVE_VT_ARRAY ? GB(unnest)(source_base, jz_str_clone(&source_key), jz_str_from(CRE)) : source_base;
            jz_str key = source_key_for_required(kt, &source_key);
            jz_str_drop(&source_key);
            groove_ProjectFieldVec named = {0};
            JZ_VEC_PUSH(named, PF(named)(jz_str_clone(&key)));
            joined = GB(semi_join)(groove_gb_clone(target->graph), GB(project_fields)(source_keys, named),
                                   jazz_engine__strvec_one(jz_str_clone(&target->row_shape.row_uuid_field)), jazz_engine__strvec_one(key));
            /* The root carries no binding route here; the target keeps its own policy routes. */
            joined = GB(project_fields)(joined, JE_TERM(project_source_fields_with_routes)(target, &target->routing_fields));
        } else {
            jazz_engine_StrVec shared_routes = {0}, target_only_routes = {0};
            const jz_str *r; void *unit;
            JZ_BTREE_FOR(&carried_routes, r, unit) if (jz_btree_contains(&target->routing_fields, r)) jazz_engine__strvec_push_clone(&shared_routes, r);
            JZ_BTREE_FOR(&target->routing_fields, r, unit) if (!jz_btree_contains(&carried_routes, r)) jazz_engine__strvec_push_clone(&target_only_routes, r);
            jazz_engine_StrVec left_on = {0}, right_on = {0};
            JZ_VEC_PUSH(left_on, jz_str_clone(&source_key));
            JZ_VEC_FOR(jz_str, s, shared_routes) jazz_engine__strvec_push_clone(&left_on, s);
            JZ_VEC_PUSH(right_on, jz_str_clone(&target->row_shape.row_uuid_field));
            JZ_VEC_FOR(jz_str, s, shared_routes) jazz_engine__strvec_push_clone(&right_on, s);
            groove_GraphBuilder *j = GB(join)(GB(unwrap_nullable)(current_graph, source_key), groove_gb_clone(target->graph), left_on, right_on);
            current_graph = NULL;
            groove_ProjectFieldVec fields = JE_TERM(project_source_fields_from_prefix)(target, JAZZ_ENGINE_RIGHT_JOIN_PREFIX);
            JZ_VEC_PUSH(fields, PF(renamed)(jz_str_from("left.__closure_root_row_uuid"), jz_str_from("__closure_root_row_uuid")));
            JZ_BTREE_FOR(&carried_routes, r, unit) JZ_VEC_PUSH(fields, PF(renamed)(F(left_field)(JZ_S(r)), jz_str_clone(r)));
            JZ_VEC_FOR(jz_str, s, target_only_routes) JZ_VEC_PUSH(fields, PF(renamed)(F(right_field)(JZ_S(s)), jz_str_clone(s)));
            joined = GB(project_fields)(j, fields);
            jazz_engine__strvec_drop(&shared_routes);
            jazz_engine__strvec_drop(&target_only_routes);
        }
        {
            const jz_str *r; void *unit;
            JZ_BTREE_FOR(&target->routing_fields, r, unit) jazz_engine__strset_insert_clone(&carried_routes, r);
        }
        ClosureOutput o;
        o.index = index;
        sid_clone(&o.source, &segment->target);
        if (index + 1 == segments.len) {
            /* The terminal graph is only consumed by the result-member collector: move it. */
            o.graph = joined;
            JZ_VEC_PUSH(outputs, o);
            break;
        }
        o.graph = groove_gb_clone(joined);
        JZ_VEC_PUSH(outputs, o);
        current_graph = joined;
        current_source = target;
    }
    groove_gb_drop(current_graph);
    jz_btree_drop(&carried_routes);
    jz_vec_drop(&segments, sizeof(void *), NULL);
    *out = outputs;
    return NULL;
fail:
    groove_gb_drop(current_graph);
    jz_btree_drop(&carried_routes);
    jz_vec_drop(&segments, sizeof(void *), NULL);
    jz_vec_drop(&outputs, sizeof(ClosureOutput), closure_output_drop_v);
    return err;
}

/* ---------------------------------------------------------------- lower_closure_membership */
jazz_engine_CapErr JE_CLO(lower_closure_membership)(groove_GraphBuilder *root_graph, jazz_engine_LoweringContext *request,
    const jazz_engine_AnalyzedQueryPlan *plan, const jazz_engine_ResolvedSource *root_source, const jz_btree *resolved_sources,
    const jz_btree *route_fields, const jz_btree *root_carrier_fields, jazz_engine_ClosureLowering *out) {
    groove_GraphBuilder *visible_root = root_graph;
    const jazz_engine_ClosurePathVec *paths = &request->request->input.shape.closure_paths;
    jazz_engine_CapErr err;
    JZ_VEC_FOR(const jazz_engine_ClosurePath, path, *paths) {
        if (path->tag != JAZZ_ENGINE_CLOSURE_EXPLICIT_INCLUDE || !path->has_root_gate) continue;
        err = required_closure_parent_graph(visible_root, &path->segments, path->root_gate, root_source, resolved_sources, root_carrier_fields,
                                            &visible_root);
        if (err) return err;
    }
    jz_btree result_members;
    jz_btree_init(&result_members, &jazz_engine__SOURCE_ID_GRAPH_MAP_DESC);
    JZ_VEC_FOR(const jazz_engine_ClosurePath, path, *paths) {
        jazz_engine_TypedOutputFieldVec flat;
        JE_TERM(flat_join_payload_fields)(plan, &flat);
        bool no_flat = flat.len == 0;
        jz_vec_drop(&flat, sizeof(jazz_engine_TypedOutputField), jazz_engine__tof_drop_v);
        groove_GraphBuilder *input;
        if (no_flat) {
            input = groove_gb_clone(visible_root);
        } else {
            err = JE_TERM(source_rows_for_visible_graph)(root_source, groove_gb_clone(visible_root), route_fields, &input);
            if (err) goto fail;
        }
        ClosureOutputVec outputs;
        err = closure_membership_graph_for_path(input, path, root_source, resolved_sources, route_fields, &outputs);
        if (err) goto fail;
        for (u32 i = 0; i < outputs.len; i++) {
            ClosureOutput *o = &outputs.ptr[i];
            const jazz_engine_ResolvedSource *resolved_source = jz_btree_get(resolved_sources, &o->source);
            if (!resolved_source) { closure_output_drop_v(o); continue; }
            /* A closure member depends on the root's routes and on its own source policy routes. */
            jz_btree member_routes;
            jazz_engine__strset_clone(&member_routes, route_fields);
            const jz_str *r; void *unit;
            JZ_BTREE_FOR(&resolved_source->routing_fields, r, unit) jazz_engine__strset_insert_clone(&member_routes, r);
            groove_GraphBuilder *graph = GB(project_fields)(o->graph, JE_TERM(project_source_fields_with_routes)(resolved_source, &member_routes));
            jz_btree_drop(&member_routes);
            JE_LOW(union_member)(&result_members, o->source, graph);
        }
        jz_vec_drop(&outputs, sizeof(ClosureOutput), NULL);
    }
    out->visible_root = visible_root;
    out->result_members = result_members;
    return NULL;
fail:
    groove_gb_drop(visible_root);
    jz_btree_drop(&result_members);
    return err;
}

/* ---------------------------------------------------------------- contributions */
groove_ProjectFieldVec JE_CLO(project_join_contribution_fields_with_root_routes)(const jazz_engine_ResolvedSource *source, const jz_btree *route_fields) {
    /* The routing carrier belongs to the admitted left/root frontier: project it once from left. */
    groove_ProjectFieldVec fields = {0};
    u32 n;
    const groove_DescriptorField *d = groove__records__RecordDescriptor__fields(source->row_shape.descriptor, &n);
    for (u32 i = 0; i < n; i++) {
        if (!d[i].has_name || jz_btree_contains(route_fields, &d[i].name)) continue;
        JZ_VEC_PUSH(fields, PF(renamed)(jz_format("%s%.*s", JAZZ_ENGINE_RIGHT_JOIN_PREFIX, JE_STR_ARG(d[i].name)), jz_str_clone(&d[i].name)));
    }
    const jz_str *f; void *unit;
    JZ_BTREE_FOR(route_fields, f, unit)
        JZ_VEC_PUSH(fields, PF(renamed)(jz_format("%s%.*s", JAZZ_ENGINE_LEFT_JOIN_PREFIX, JE_STR_ARG(*f)), jz_str_clone(f)));
    return fields;
}

/* `ReachableVia` names the application-level `id` column even when a table uses the implicit row id. */
static jz_str reachable_root_reference_field(const jazz_engine_ResolvedSource *source, const jz_str *field) {
    jz_str user_field = F(user_column_field)(JZ_S(field));
    u32 n;
    const groove_DescriptorField *d = groove__records__RecordDescriptor__fields(source->row_shape.descriptor, &n);
    bool present = false;
    for (u32 i = 0; i < n; i++)
        if (d[i].has_name && jz_str_eq(&d[i].name, &user_field)) { present = true; break; }
    if (present || !jz_str_eq_cstr(field, "id")) return user_field;
    jz_str_drop(&user_field);
    return jz_str_clone(&source->row_shape.row_uuid_field);
}

static jazz_engine_CapErr analyze_contribution_input(const jz_str *input, const jz_btree *nodes, jazz_engine_RelationInputPlan *plan) {
    jz_btree visited;
    jz_btree_init(&visited, &jazz_engine__STRSET_DESC);
    jazz_engine_UrErr ue = JE_PLAN(analyze_relation_input_node)(input, nodes, &visited, plan);
    jz_btree_drop(&visited);
    return ue ? jazz_engine__ur_into_cap(ue) : NULL;
}

jazz_engine_CapErr JE_CLO(reachable_contribution_membership_graph)(groove_GraphBuilder *visible_root, const jazz_engine_ReachableContribution *contribution,
    const jazz_engine_ResolvedSource *root_source, const jazz_engine_ResolvedSource *contribution_source, const jz_btree *nodes,
    const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, const jz_btree *route_fields, groove_GraphBuilder **out) {
    jazz_engine_RelationInputPlan plan;
    jazz_engine_CapErr err = analyze_contribution_input(&contribution->access_input, nodes, &plan);
    if (err) { groove_gb_drop(visible_root); return err; }
    jazz_engine_LoweredRelationInput lowered;
    jazz_engine_UrErr ue = JE_GL(lower_relation_input_for_contributor)(&plan, resolved_sources, request, &lowered);
    JE_PLAN(RelationInputPlan__drop)(&plan);
    if (ue) { groove_gb_drop(visible_root); return jazz_engine__ur_into_cap(ue); }
    jz_str join_field = reachable_root_reference_field(contribution_source, &contribution->root_ref_field);
    if (!jz_btree_contains(&lowered.fields, &join_field)) {
        err = JE_RES(CapabilityReport__explained)(
            jazz_engine__ur_take(jazz_engine__ur_op(jz_format("reachable contribution %.*s does not provide root reference field %.*s",
                                                              JE_STR_ARG(contribution->id), JE_STR_ARG(join_field)))),
            jz_str_from("reachable contribution payload requires root reference field"));
        jz_str_drop(&join_field);
        JE_GL(LoweredRelationInput__drop)(&lowered);
        groove_gb_drop(visible_root);
        return err;
    }
    groove_GraphBuilder *contribution_graph = lowered.graph;
    lowered.graph = NULL;
    if (jz_btree_contains(&lowered.nullable_fields, &join_field))
        contribution_graph = JE_GL(unwrap_nullable_join_key)(contribution_graph, jz_str_clone(&join_field), 1);
    JE_GL(LoweredRelationInput__drop)(&lowered);
    *out = GB(project_fields)(GB(join)(visible_root, contribution_graph, jazz_engine__strvec_one(jz_str_clone(&root_source->row_shape.row_uuid_field)),
                                       jazz_engine__strvec_one(join_field)),
                              JE_CLO(project_join_contribution_fields_with_root_routes)(contribution_source, route_fields));
    return NULL;
}

static const jazz_engine_RecursiveRelationPlan *recursive_relation_for_edge(const jazz_engine_RelationInputPlan *plan, const jazz_engine_SourceId *edge_source) {
    JZ_VEC(const jazz_engine_RelationInputPlan *) pending = {0};
    JZ_VEC_PUSH(pending, plan);
    const jazz_engine_RecursiveRelationPlan *found = NULL;
    while (pending.len) {
        const jazz_engine_RelationInputPlan *p = pending.ptr[--pending.len];
        switch (p->tag) {
        case JAZZ_ENGINE_RIP_RECURSIVE: {
            const jazz_engine_SourceId *step = JE_PLAN(RecursiveRelationPlan__step_source)(&p->u.recursive);
            if (step && sid_eq(step, edge_source)) { found = &p->u.recursive; goto done; }
            break;
        }
        case JAZZ_ENGINE_RIP_LINEAR:
            for (u32 i = p->u.linear.steps.len; i-- > 0;) {
                const jazz_engine_LinearStep *step = &p->u.linear.steps.ptr[i];
                if (step->tag == JAZZ_ENGINE_STEP_JOIN) JZ_VEC_PUSH(pending, (const jazz_engine_RelationInputPlan *)step->u.join.right);
            }
            break;
        case JAZZ_ENGINE_RIP_UNION:
            for (u32 i = p->u.union_.branches.len; i-- > 0;) JZ_VEC_PUSH(pending, (const jazz_engine_RelationInputPlan *)&p->u.union_.branches.ptr[i].plan);
            break;
        }
    }
done:
    jz_vec_drop(&pending, sizeof(void *), NULL);
    return found;
}

static jazz_engine_CapErr no_recursive_for_edge(const jazz_engine_ReachableContribution *contribution, const char *what) {
    JE_DBG(d, sid_debug, &contribution->edge_source);
    jazz_engine_CapErr e = jazz_engine__cap_operator_fmt("reachable contribution %.*s has no recursive %s for edge source %.*s",
                                                         JE_STR_ARG(contribution->id), what, JE_STR_ARG(d));
    jz_str_drop(&d);
    return e;
}

static void routes_intersection(const jz_btree *a, const jz_btree *b, jz_btree *out) {
    jz_btree_init(out, &jazz_engine__STRSET_DESC);
    const jz_str *r; void *unit;
    JZ_BTREE_FOR(a, r, unit) if (jz_btree_contains(b, r)) jazz_engine__strset_insert_clone(out, r);
}

jazz_engine_CapErr JE_CLO(reachable_step_witness_membership_graph)(const jazz_engine_ReachableContribution *contribution, const jz_btree *nodes,
    const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, const jz_btree *route_fields, groove_GraphBuilder **out) {
    jazz_engine_RelationInputPlan plan;
    jazz_engine_CapErr err = analyze_contribution_input(&contribution->access_input, nodes, &plan);
    if (err) return err;
    const jazz_engine_RecursiveRelationPlan *recursive = recursive_relation_for_edge(&plan, &contribution->edge_source);
    if (!recursive) { err = no_recursive_for_edge(contribution, "step"); goto done; }
    const jazz_engine_SourceId *root_id = JE_PLAN(RecursiveRelationPlan__root_source)(recursive);
    if (!root_id) { err = JE_RES(CapabilityReport__operator)(jz_str_from("recursive step witness requires a source root")); goto done; }
    const jazz_engine_ResolvedSource *root_source = jz_btree_get(resolved_sources, root_id);
    if (!root_source) {
        JE_DBG(d, sid_debug, root_id);
        err = jazz_engine__cap_runtime_fmt("recursive witness root %.*s was not resolved", JE_STR_ARG(d));
        jz_str_drop(&d);
        goto done;
    }
    const jazz_engine_ResolvedSource *edge_source;
    err = JE_RES(ResolvedSource__lookup)(resolved_sources, &contribution->edge_source, "recursive edge witness", &edge_source);
    if (err) goto done;
    jazz_engine_LoweredRelationInput lowered;
    jazz_engine_UrErr ue = JE_GL(lower_recursive_relation_cached)(NULL, recursive, root_source, resolved_sources, request, NULL, &lowered);
    if (ue) { err = jazz_engine__ur_into_cap(ue); goto done; }
    jz_btree routes;
    routes_intersection(&edge_source->routing_fields, route_fields, &routes);
    groove_GraphBuilder *g = lowered.graph;
    lowered.graph = NULL;
    JE_GL(LoweredRelationInput__drop)(&lowered);
    *out = GB(project_fields)(GB(recursive_step_witness)(g), JE_CLO(project_join_contribution_fields_with_root_routes)(edge_source, &routes));
    jz_btree_drop(&routes);
done:
    JE_PLAN(RelationInputPlan__drop)(&plan);
    return err;
}

jazz_engine_CapErr JE_CLO(reachable_seed_membership_graph)(const jazz_engine_ReachableContribution *contribution, const jz_btree *nodes,
    const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, const jz_btree *route_fields,
    bool *has, jazz_engine_SourceId *source, groove_GraphBuilder **graph) {
    *has = false;
    jazz_engine_RelationInputPlan plan;
    jazz_engine_CapErr err = analyze_contribution_input(&contribution->access_input, nodes, &plan);
    if (err) return err;
    const jazz_engine_RecursiveRelationPlan *recursive = recursive_relation_for_edge(&plan, &contribution->edge_source);
    if (!recursive) { err = no_recursive_for_edge(contribution, "seed"); goto done; }
    /* A literal seed has no physical source occurrence to ship or certify. */
    const jazz_engine_SourceId *seed_id = JE_PLAN(RecursiveRelationPlan__seed_source)(recursive);
    if (!seed_id) goto done;
    const jazz_engine_ResolvedSource *seed_source;
    err = JE_RES(ResolvedSource__lookup)(resolved_sources, seed_id, "recursive seed", &seed_source);
    if (err) goto done;
    jazz_engine_LoweredRelationInput seed;
    jazz_engine_UrErr ue = JE_GL(lower_recursive_seed_membership)(recursive, seed_source, resolved_sources, request, &seed);
    if (ue) { err = jazz_engine__ur_into_cap(ue); goto done; }
    jz_btree routes;
    routes_intersection(&seed_source->routing_fields, route_fields, &routes);
    groove_GraphBuilder *g = seed.graph;
    seed.graph = NULL;
    JE_GL(LoweredRelationInput__drop)(&seed);
    *has = true;
    sid_clone(source, seed_id);
    *graph = GB(project_fields)(g, JE_TERM(project_source_fields_with_routes)(seed_source, &routes));
    jz_btree_drop(&routes);
done:
    JE_PLAN(RelationInputPlan__drop)(&plan);
    return err;
}

/* missing_join_key closure: FnOnce(&str) -> Box<CapabilityReport> */
typedef jazz_engine_CapErr (*MissingJoinKeyFn)(const jz_str *join_key, const void *ctx);

static jazz_engine_CapErr relation_membership_graph(groove_GraphBuilder *visible_root, const jz_str *input, const jazz_engine_PredicateExpr *membership,
    MissingJoinKeyFn missing_join_key, const void *missing_ctx, const jazz_engine_ResolvedSource *root_source,
    const jazz_engine_ResolvedSource *target_source, const jz_btree *nodes, const jz_btree *resolved_sources, jazz_engine_LoweringContext *request,
    const jz_btree *route_fields, groove_GraphBuilder **out) {
    jazz_engine_RelationInputPlan plan;
    jazz_engine_CapErr err = analyze_contribution_input(input, nodes, &plan);
    if (err) { groove_gb_drop(visible_root); return err; }
    jazz_engine_LoweredRelationInput lowered;
    jazz_engine_UrErr ue = JE_GL(lower_relation_input_for_contributor)(&plan, resolved_sources, request, &lowered);
    if (ue) { JE_PLAN(RelationInputPlan__drop)(&plan); groove_gb_drop(visible_root); return jazz_engine__ur_into_cap(ue); }
    jazz_engine_StrVec root_keys = {0}, join_keys = {0};
    ue = JE_GL(lower_root_to_relation_key_pairs)(membership, root_source, &plan, &lowered, request, &root_keys, &join_keys);
    JE_PLAN(RelationInputPlan__drop)(&plan);
    if (ue) { err = jazz_engine__ur_into_cap(ue); goto fail; }
    JZ_VEC_FOR(jz_str, k, join_keys) {
        if (!jz_btree_contains(&lowered.fields, k)) { err = missing_join_key(k, missing_ctx); goto fail_keys; }
    }
    groove_GraphBuilder *relation_graph = lowered.graph;
    lowered.graph = NULL;
    jz_btree unwrapped;
    jz_btree_init(&unwrapped, &jazz_engine__STRSET_DESC);
    JZ_VEC_FOR(jz_str, k, join_keys) {
        if (jz_btree_contains(&lowered.nullable_fields, k) && !jz_btree_contains(&unwrapped, k)) {
            jazz_engine__strset_insert_clone(&unwrapped, k);
            relation_graph = JE_GL(unwrap_nullable_join_key)(relation_graph, jz_str_clone(k), 1);
        }
    }
    jz_btree_drop(&unwrapped);
    JE_GL(LoweredRelationInput__drop)(&lowered);
    *out = GB(project_fields)(GB(join)(visible_root, relation_graph, root_keys, join_keys),
                              JE_CLO(project_join_contribution_fields_with_root_routes)(target_source, route_fields));
    return NULL;
fail_keys:
    jazz_engine__strvec_drop(&root_keys);
    jazz_engine__strvec_drop(&join_keys);
fail:
    JE_GL(LoweredRelationInput__drop)(&lowered);
    groove_gb_drop(visible_root);
    return err;
}

static jazz_engine_CapErr join_contribution_missing_key(const jz_str *join_key, const void *ctx) {
    const jazz_engine_JoinContribution *c = ctx;
    return JE_RES(CapabilityReport__explained)(
        jazz_engine__ur_take(jazz_engine__ur_op(jz_format("join contribution %.*s does not provide join key field %.*s", JE_STR_ARG(c->id), JE_STR_ARG(*join_key)))),
        jz_str_from("join contribution payload requires the normalized join key"));
}

jazz_engine_CapErr JE_CLO(join_contribution_membership_graph)(groove_GraphBuilder *visible_root, const jazz_engine_JoinContribution *contribution,
    const jazz_engine_ResolvedSource *root_source, const jazz_engine_ResolvedSource *contribution_source, const jz_btree *nodes,
    const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, const jz_btree *route_fields, groove_GraphBuilder **out) {
    return relation_membership_graph(visible_root, &contribution->input, &contribution->membership, join_contribution_missing_key, contribution,
                                     root_source, contribution_source, nodes, resolved_sources, request, route_fields, out);
}

static jazz_engine_CapErr inherited_contribution_missing_key(const jz_str *join_key, const void *ctx) {
    const jazz_engine_InheritedContribution *c = ctx;
    return jazz_engine__cap_operator_fmt("inherited contribution %.*s does not provide parent key field %.*s", JE_STR_ARG(c->id), JE_STR_ARG(*join_key));
}

/* Derive the parent rows consumed by a caller-requested `inherits` semi-join. */
jazz_engine_CapErr JE_CLO(inherited_contribution_membership_graph)(groove_GraphBuilder *visible_root, const jazz_engine_InheritedContribution *contribution,
    const jazz_engine_ResolvedSource *root_source, const jazz_engine_ResolvedSource *parent_source, const jz_btree *nodes,
    const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, const jz_btree *route_fields, groove_GraphBuilder **out) {
    return relation_membership_graph(visible_root, &contribution->input, &contribution->membership, inherited_contribution_missing_key, contribution,
                                     root_source, parent_source, nodes, resolved_sources, request, route_fields, out);
}

/* Resolve a normalized value through the compiler-owned `flat_join:output` projection. */
static jazz_engine_UrErr flat_join_visible_field(const jazz_engine_NormalizedValueRef *value, const jz_btree *nodes, jazz_engine_TypedOutputField *out) {
    jz_str key = jz_str_from("flat_join:output");
    const jazz_engine_RowSetExpr *expr = jz_btree_get(nodes, &key);
    jz_str_drop(&key);
    if (!expr || expr->tag != JAZZ_ENGINE_EXPR_PROJECT) return JE_UR_OP("flat join contribution is missing its output projection");
    JZ_VEC_FOR(const jazz_engine_RowProjection, column, expr->u.project.columns) {
        if (nvr_eq(&column->value, value)) { tof_clone(out, &column->output); return NULL; }
    }
    return JE_UR_OP("flat join contribution key is not retained by the output projection");
}

/* Derive one flat-join source from the already-authorized rendered root. */
jazz_engine_CapErr JE_CLO(flat_join_contribution_membership_graph)(groove_GraphBuilder *visible_root, const jazz_engine_JoinContribution *contribution,
    const jazz_engine_ResolvedSource *contribution_source, const jz_btree *nodes, const jz_btree *resolved_sources,
    jazz_engine_LoweringContext *request, const jz_btree *route_fields, groove_GraphBuilder **out) {
    jazz_engine_RelationInputPlan plan;
    jazz_engine_CapErr err = analyze_contribution_input(&contribution->input, nodes, &plan);
    if (err) { groove_gb_drop(visible_root); return err; }
    jazz_engine_LoweredRelationInput lowered;
    jazz_engine_UrErr ue = JE_GL(lower_relation_input_for_contributor)(&plan, resolved_sources, request, &lowered);
    if (ue) { JE_PLAN(RelationInputPlan__drop)(&plan); groove_gb_drop(visible_root); return jazz_engine__ur_into_cap(ue); }
    const jazz_engine_PredicateExpr *m = &contribution->membership;
    if (m->tag != JAZZ_ENGINE_PRED_COMPARE || m->op != JAZZ_ENGINE_CMP_EQ) {
        err = JE_RES(CapabilityReport__operator)(jz_str_from("flat join contribution membership must be equality"));
        goto fail;
    }
    jazz_engine_TypedOutputField visible_key;
    jz_str relation_key;
    jazz_engine_UrErr a = flat_join_visible_field(&m->u.pair.left, nodes, &visible_key);
    jazz_engine_UrErr b = JE_GL(lower_relation_key_ref)(&m->u.pair.right, &plan, &lowered, request, &relation_key);
    if (a || b) {
        if (!a) tof_drop(&visible_key);
        if (!b) jz_str_drop(&relation_key);
        jazz_engine__ur_free(a);
        jazz_engine__ur_free(b);
        ue = flat_join_visible_field(&m->u.pair.right, nodes, &visible_key);
        if (ue) { err = jazz_engine__ur_into_cap(ue); goto fail; }
        ue = JE_GL(lower_relation_key_ref)(&m->u.pair.left, &plan, &lowered, request, &relation_key);
        if (ue) { tof_drop(&visible_key); err = jazz_engine__ur_into_cap(ue); goto fail; }
    }
    JE_PLAN(RelationInputPlan__drop)(&plan);
    groove_GraphBuilder *relation_graph = lowered.graph;
    lowered.graph = NULL;
    if (jz_btree_contains(&lowered.nullable_fields, &relation_key)) {
        const u32 *depth = jz_btree_get(&lowered.nullable_field_depths, &relation_key);
        relation_graph = JE_GL(unwrap_nullable_join_key)(relation_graph, jz_str_clone(&relation_key), depth ? *depth : 1);
    }
    JE_GL(LoweredRelationInput__drop)(&lowered);
    /* Public projections retain authored nullability: unwrap on this private frontier only. */
    const groove_ValueType *vt = &visible_key.ty;
    groove_GraphBuilder *visible_graph = unwrap_nullable_layers(visible_root, jz_str_clone(&visible_key.name), &vt);
    *out = GB(project_fields)(GB(join)(visible_graph, relation_graph, jazz_engine__strvec_one(jz_str_clone(&visible_key.name)),
                                       jazz_engine__strvec_one(relation_key)),
                              JE_CLO(project_join_contribution_fields_with_root_routes)(contribution_source, route_fields));
    tof_drop(&visible_key);
    return NULL;
fail:
    JE_PLAN(RelationInputPlan__drop)(&plan);
    JE_GL(LoweredRelationInput__drop)(&lowered);
    groove_gb_drop(visible_root);
    return err;
}

