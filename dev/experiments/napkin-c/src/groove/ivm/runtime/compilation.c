/* groove::ivm::runtime::compilation: hash-consed GraphBuilder compilation
 * into executable IVM nodes. */
#include "mod.h"

#define CMP(name) groove__ivm__runtime__compilation__IvmRuntime__##name
#define RP(name) groove__ivm__runtime__record_projection__##name
#define INIT_NODE(self, n) groove__ivm__runtime__graph_lifecycle__IvmRuntime__initialize_node_runtime((self), (n))
#define VMOVE(dst, src) memcpy(&(dst), &(src), sizeof(dst))
#define FIELDS(d) groove__records__RecordDescriptor__fields((d), NULL)

typedef JZ_VEC(groove_ProjectionExpr) ProjectionExprVecB;

static void str_vec_drop(groove_StrVec *v) { jz_vec_drop(v, sizeof(jz_str), groove_ivm_rt_str_drop_v); }
static void plan_expr_drop_v(void *p) { groove_PlanExpr_drop(p); }
static void plan_vec_drop(groove_PlanExprVec *v) { jz_vec_drop(v, sizeof(groove_PlanExpr), plan_expr_drop_v); }
static void u32_vec_drop(groove_U32Vec *v) { jz_vec_drop(v, sizeof(u32), NULL); }

static groove_NodeDurability ephemeral(void) { groove_NodeDurability d; memset(&d, 0, sizeof d); d.tag = groove_NodeDurability_Ephemeral; return d; }
static groove_OpType op_of(u8 tag) { groove_OpType op; memset(&op, 0, sizeof op); op.tag = tag; return op; }

static groove_NodeId dedup(groove_IvmRuntime *self, groove_OpType op, const groove_NodeId *inputs, u32 n, u8 out_tag, groove_RecordDescriptor out, groove_NodeDurability durability) {
    groove_NodeDescriptor d; memset(&d, 0, sizeof d);
    d.operator_ = op;
    jz_vec_extend(&d.inputs, inputs, n, sizeof(groove_NodeId));
    d.output.tag = out_tag;
    d.output.records = out;
    return groove__ivm__graph__IvmGraph__dedup_node(self->graph, d, durability);
}
static groove_NodeId dedup_records(groove_IvmRuntime *self, groove_OpType op, const groove_NodeId *inputs, u32 n, groove_RecordDescriptor out) {
    groove_NodeId node = dedup(self, op, inputs, n, groove_NodeOutput_Records, out, ephemeral());
    INIT_NODE(self, node);
    return node;
}
static groove_CompiledNode compiled_node(groove_RecordDescriptor output, groove_NodeId node, bool has_ro, groove_NodeId ro) {
    groove_CompiledNode c = { output, node, has_ro, ro }; return c;
}
static groove_CompiledNode compiled_inherit(groove_RecordDescriptor output, groove_NodeId node, const groove_CompiledNode *from) {
    return compiled_node(output, node, from->has_root_ordering_node, from->root_ordering_node);
}
static groove_PlanExpr plan_field(jz_str name) { groove_PlanExpr p; memset(&p, 0, sizeof p); p.tag = groove_PlanExpr_Field; p.field = name; return p; }
static groove_FieldRef resolved(u32 idx) { groove_FieldRef f; memset(&f, 0, sizeof f); f.tag = groove_FieldRef_Resolved; f.idx = idx; return f; }
static int u32_cmp(const void *a, const void *b, void *ctx) { (void)ctx; u32 x = *(const u32 *)a, y = *(const u32 *)b; return x < y ? -1 : x > y; }
static void sort_dedup_u32(groove_U32Vec *v) {
    jz_sort_unstable(v->ptr, v->len, sizeof(u32), u32_cmp, NULL);
    u32 w = 0;
    for (u32 i = 0; i < v->len; i++) if (w == 0 || v->ptr[w - 1] != v->ptr[i]) v->ptr[w++] = v->ptr[i];
    v->len = w;
}
static bool bsearch_u32(const groove_U32Vec *v, u32 key, u32 *idx) { return jz_bsearch(v->ptr, v->len, sizeof(u32), &key, u32_cmp, NULL, idx); }

static jz_err resolve_refs(groove_RecordDescriptor d, const groove_FieldRef *refs, u32 n, groove_U32Vec *out) {
    groove_U32Vec v = {0};
    for (u32 i = 0; i < n; i++) {
        u32 idx; jz_err e = RP(resolve_field_ref)(d, &refs[i], &idx);
        if (JZ_IS_ERR(e)) { u32_vec_drop(&v); return e; }
        JZ_VEC_PUSH(v, idx);
    }
    *out = v;
    return JZ_OK;
}
static jz_err names_at(groove_RecordDescriptor d, const u32 *idx, u32 n, groove_StrVec *out) {
    groove_StrVec v = {0};
    for (u32 i = 0; i < n; i++) {
        jz_str s; jz_err e = RP(field_name_at)(d, idx[i], &s);
        if (JZ_IS_ERR(e)) { str_vec_drop(&v); return e; }
        JZ_VEC_PUSH(v, s);
    }
    *out = v;
    return JZ_OK;
}
static jz_err plan_fields(groove_RecordDescriptor d, const groove_FieldRef *refs, u32 n, groove_PlanExprVec *out) {
    groove_PlanExprVec v = {0};
    for (u32 i = 0; i < n; i++) {
        jz_str s; jz_err e = RP(field_ref_name)(d, &refs[i], &s);
        if (JZ_IS_ERR(e)) { plan_vec_drop(&v); return e; }
        JZ_VEC_PUSH(v, plan_field(s));
    }
    *out = v;
    return JZ_OK;
}
static void plan_names(const groove_PlanExprVec *k, groove_StrVec *out) { groove__ivm__runtime__evaluator__plan_expr_names(k->ptr, k->len, out); }

static groove_NodeId add_arrangement_node(groove_IvmRuntime *self, groove_NodeId input, groove_RecordDescriptor records, groove_StrVec fields, groove_ValueComparison comparison) {
    self->logical_nodes_requested += 1;
    groove_OpType op = op_of(groove_Op_Arrange);
    VMOVE(op.v.arrange.fields, fields);
    op.v.arrange.comparison = comparison;
    groove_NodeId node = dedup(self, op, &input, 1, groove_NodeOutput_Arrangement, records, ephemeral());
    INIT_NODE(self, node);
    return node;
}

/* Consumer-local column pruning; the source graph stays intact. */
static jz_err compile_selected_columns(groove_IvmRuntime *self, groove_CompiledNode input, const u32 *selected, u32 n, groove_CompiledNode *out) {
    if (n == groove__records__RecordDescriptor__len(input.output)) { *out = input; return JZ_OK; }
    const groove_DescriptorField *fields = FIELDS(input.output);
    jz_str *names = calloc(n ? n : 1, sizeof(jz_str));
    groove_ValueType *types = calloc(n ? n : 1, sizeof(groove_ValueType));
    for (u32 i = 0; i < n; i++) {
        names[i] = (jz_str){0};
        jz_str_appendf(&names[i], "__narrow_field_%u", selected[i]);
        groove_ValueType_clone(&types[i], &fields[selected[i]].value_type);
    }
    groove_RecordDescriptor output = groove__records__RecordDescriptor__new_named(names, types, n);
    groove_OpType op = op_of(groove_Op_MapProject);
    for (u32 i = 0; i < n; i++) {
        groove_ProjectionExpr e; memset(&e, 0, sizeof e);
        e.expression.tag = groove_ProjectExpr_Field;
        e.expression.field = resolved(selected[i]);
        e.has_output_name = true; e.output_name = jz_str_clone(&names[i]);
        e.has_identity = true; e.output_identity.tag = groove_FieldIdentity_Name; e.output_identity.name = names[i];
        JZ_VEC_PUSH(op.v.map_project.expressions, e);
        groove_UsizePair m = { 0, selected[i] };
        JZ_VEC_PUSH(op.v.map_project.mapping, m);
        groove_ValueType_drop(&types[i]);
    }
    free(names); free(types);
    groove_NodeId node = dedup_records(self, op, &input.node, 1, output);
    *out = compiled_inherit(output, node, &input);
    return JZ_OK;
}

/* Semi/anti joins observe right-key multiplicities, not right payloads. */
static jz_err compile_existence_input(groove_IvmRuntime *self, groove_CompiledNode input, const groove_FieldRef *keys, u32 nkeys, groove_CompiledNode *out, groove_PlanExprVec *keys_out) {
    groove_U32Vec indices;
    JZ_TRY(resolve_refs(input.output, keys, nkeys, &indices));
    groove_U32Vec selected = {0};
    jz_vec_extend(&selected, indices.ptr, indices.len, sizeof(u32));
    sort_dedup_u32(&selected);
    groove_CompiledNode narrowed;
    jz_err e = compile_selected_columns(self, input, selected.ptr, selected.len, &narrowed);
    groove_PlanExprVec k = {0};
    for (u32 i = 0; i < indices.len && !JZ_IS_ERR(e); i++) {
        u32 pos;
        if (!bsearch_u32(&selected, indices.ptr[i], &pos)) jz_panic("selected key");
        groove_FieldRef r = resolved(pos);
        jz_str name;
        e = RP(field_ref_name)(narrowed.output, &r, &name);
        if (!JZ_IS_ERR(e)) JZ_VEC_PUSH(k, plan_field(name));
    }
    u32_vec_drop(&indices); u32_vec_drop(&selected);
    if (JZ_IS_ERR(e)) { plan_vec_drop(&k); return e; }
    *out = narrowed; *keys_out = k;
    return JZ_OK;
}

static groove_FieldRef *projection_source_ref(groove_ProjectExpr *expression) {
    switch (expression->tag) {
    case groove_ProjectExpr_Field: case groove_ProjectExpr_Nullable: case groove_ProjectExpr_NullableFlat:
    case groove_ProjectExpr_RecordField: case groove_ProjectExpr_EnumTagRemap: case groove_ProjectExpr_EnumRemap:
    case groove_ProjectExpr_RecursiveEnumRemap:
        return &expression->field;
    default: /* Literal, TypedLiteral, Null, TemplateArgument */
        return NULL;
    }
}

typedef struct JoinSide { groove_RecordDescriptor descriptor; groove_NodeId arrangement; groove_PlanExprVec key; } JoinSide;

/* The compiled Join inputs are arrangements: prune their record inputs, then
 * construct consumer-specific arrangements; never mutate a shared one. */
static jz_err compile_side(groove_IvmRuntime *self, groove_NodeId arrangement, groove_RecordDescriptor output, const groove_U32Vec *fields, const groove_U32Vec *keys, groove_ValueComparison comparison, JoinSide *out) {
    const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(self->graph, arrangement);
    if (!gn) return GROOVE_IVM_ERR_N(GROOVE_IVM_GraphNodeNotFound, arrangement);
    groove_NodeId source = gn->descriptor.inputs.ptr[0];
    groove_CompiledNode compiled;
    JZ_TRY(compile_selected_columns(self, compiled_node(output, source, false, 0), fields->ptr, fields->len, &compiled));
    groove_PlanExprVec key = {0};
    for (u32 i = 0; i < keys->len; i++) {
        u32 pos;
        if (!bsearch_u32(fields, keys->ptr[i], &pos)) jz_panic("join key retained");
        groove_FieldRef r = resolved(pos); jz_str name;
        jz_err e = RP(field_ref_name)(compiled.output, &r, &name);
        if (JZ_IS_ERR(e)) { plan_vec_drop(&key); return e; }
        JZ_VEC_PUSH(key, plan_field(name));
    }
    groove_StrVec names = {0}; plan_names(&key, &names);
    out->descriptor = compiled.output;
    out->arrangement = add_arrangement_node(self, compiled.node, compiled.output, names, comparison);
    out->key = key;
    return JZ_OK;
}

static jz_err resolve_plan_names(groove_RecordDescriptor d, const groove_PlanExprVec *keys, groove_U32Vec *out) {
    groove_StrVec names = {0}; plan_names(keys, &names);
    groove_U32Vec v = {0};
    jz_err e = JZ_OK;
    JZ_VEC_FOR(jz_str, name, names) {
        groove_FieldRef r; memset(&r, 0, sizeof r); r.tag = groove_FieldRef_Name; r.name = *name;
        u32 idx; e = RP(resolve_field_ref)(d, &r, &idx);
        if (JZ_IS_ERR(e)) break;
        JZ_VEC_PUSH(v, idx);
    }
    str_vec_drop(&names);
    if (JZ_IS_ERR(e)) { u32_vec_drop(&v); return e; }
    *out = v;
    return JZ_OK;
}

/* Project(Join) needs only consumer fields plus matching keys on each input.
 * Bind expressions against the original descriptor first. */
static jz_err narrow_projected_join(groove_IvmRuntime *self, groove_NodeId input, ProjectionExprVecB *expressions, groove_NodeId *out) {
    const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(self->graph, input);
    if (!gn) return GROOVE_IVM_ERR_N(GROOVE_IVM_GraphNodeNotFound, input);
    *out = input;
    if (gn->descriptor.operator_.tag != groove_Op_Join) return JZ_OK;
    const groove_JoinOp *j = &gn->descriptor.operator_.v.join;
    if (j->has_residual || j->kind != groove_JoinOpKind_Inner) return JZ_OK;
    /* descriptor.clone(): the graph may grow below. */
    groove_JoinOpKind kind = j->kind; groove_ValueComparison comparison = j->comparison;
    groove_RecordDescriptor left_descriptor = j->left_descriptor, right_descriptor = j->right_descriptor;
    groove_PlanExprVec left_key_src = {0}, right_key_src = {0};
    jz_vec_clone(&left_key_src, &j->left_key, sizeof(groove_PlanExpr), (void (*)(void *, const void *))groove_PlanExpr_clone);
    jz_vec_clone(&right_key_src, &j->right_key, sizeof(groove_PlanExpr), (void (*)(void *, const void *))groove_PlanExpr_clone);
    groove_NodeId in0 = gn->descriptor.inputs.ptr[0], in1 = gn->descriptor.inputs.ptr[1];
    groove_RecordDescriptor original = gn->descriptor.output.records;
    u32 left_len = groove__records__RecordDescriptor__len(left_descriptor);

    groove_UsizePairVec input_mapping = {0};
    groove_U32Vec left_fields = {0}, right_fields = {0}, left_keys = {0}, right_keys = {0};
    groove_UsizePairVec output_mapping = {0};
    JoinSide l = {0}, r = {0};
    jz_err e = groove__ivm__runtime__join__join_output_mapping(left_descriptor, right_descriptor, original, &input_mapping);
    if (JZ_IS_ERR(e)) goto done;
    JZ_VEC_FOR(groove_ProjectionExpr, expression, *expressions) {
        groove_FieldRef *field = projection_source_ref(&expression->expression);
        if (!field) continue;
        u32 idx; e = RP(resolve_field_ref)(original, field, &idx);
        if (JZ_IS_ERR(e)) goto done;
        groove_UsizePair m = input_mapping.ptr[idx];
        if (m.a == 0) JZ_VEC_PUSH(left_fields, m.b); else if (m.a == 1) JZ_VEC_PUSH(right_fields, m.b);
    }
    e = resolve_plan_names(left_descriptor, &left_key_src, &left_keys); if (JZ_IS_ERR(e)) goto done;
    e = resolve_plan_names(right_descriptor, &right_key_src, &right_keys); if (JZ_IS_ERR(e)) goto done;
    jz_vec_extend(&left_fields, left_keys.ptr, left_keys.len, sizeof(u32));
    jz_vec_extend(&right_fields, right_keys.ptr, right_keys.len, sizeof(u32));
    sort_dedup_u32(&left_fields); sort_dedup_u32(&right_fields);
    if (left_fields.len == left_len && right_fields.len == groove__records__RecordDescriptor__len(right_descriptor)) goto done;
    e = compile_side(self, in0, left_descriptor, &left_fields, &left_keys, comparison, &l); if (JZ_IS_ERR(e)) goto done;
    e = compile_side(self, in1, right_descriptor, &right_fields, &right_keys, comparison, &r); if (JZ_IS_ERR(e)) goto done;
    groove_RecordDescriptor output = groove__ivm__runtime__key_encoding__join_descriptor(l.descriptor, r.descriptor);
    e = groove__ivm__runtime__join__join_output_mapping(l.descriptor, r.descriptor, output, &output_mapping);
    if (JZ_IS_ERR(e)) goto done;
    JZ_VEC_FOR(groove_ProjectionExpr, expression, *expressions) {
        groove_FieldRef *field = projection_source_ref(&expression->expression);
        if (!field) continue;
        u32 idx; e = RP(resolve_field_ref)(original, field, &idx);
        if (JZ_IS_ERR(e)) goto done;
        groove_UsizePair m = input_mapping.ptr[idx];
        const groove_U32Vec *retained = m.a == 0 ? &left_fields : &right_fields;
        u32 physical;
        if (!bsearch_u32(retained, m.b, &physical)) jz_panic("consumer field retained");
        u32 position = 0; bool found = false;
        for (u32 p = 0; p < output_mapping.len; p++)
            if (output_mapping.ptr[p].a == m.a && output_mapping.ptr[p].b == physical) { position = p; found = true; break; }
        if (!found) jz_panic("output position");
        groove_FieldRef_drop(field);
        *field = resolved(position);
    }
    groove_OpType op = op_of(groove_Op_Join);
    op.v.join.kind = kind;
    op.v.join.left_descriptor = l.descriptor; op.v.join.right_descriptor = r.descriptor;
    op.v.join.left_key = l.key; op.v.join.right_key = r.key;
    l.key = (groove_PlanExprVec){0}; r.key = (groove_PlanExprVec){0};
    op.v.join.has_residual = false;
    op.v.join.comparison = comparison;
    groove_NodeId in[2] = { l.arrangement, r.arrangement };
    *out = dedup_records(self, op, in, 2, output);
done:
    plan_vec_drop(&left_key_src); plan_vec_drop(&right_key_src);
    plan_vec_drop(&l.key); plan_vec_drop(&r.key);
    jz_vec_drop(&input_mapping, sizeof(groove_UsizePair), NULL);
    jz_vec_drop(&output_mapping, sizeof(groove_UsizePair), NULL);
    u32_vec_drop(&left_fields); u32_vec_drop(&right_fields); u32_vec_drop(&left_keys); u32_vec_drop(&right_keys);
    return e;
}

/* ---------- compilation drivers ---------- */
static usize graph_builder_key(const groove_GraphBuilder *g) { return (usize)(uintptr_t)g; }

static jz_err add_dedup_graph_cached(groove_IvmRuntime *self, const groove_GraphBuilder *graph, jz_hmap *output_memo, jz_hmap *compiled_memo, groove_CompiledNode *out);

static bool step_has_template_arguments(const groove_GraphBuilder *b) {
    if (b->tag == groove_GB_Filter) return groove__ivm__op_types__PredicateExpr__has_template_arguments(&b->predicate);
    if (b->tag == groove_GB_Project) {
        JZ_VEC_FOR(groove_ProjectField, f, b->fields) if (f->expression.tag == groove_ProjectExpr_TemplateArgument) return true;
    }
    return false;
}

static jz_err add_dedup_graph_inner(groove_IvmRuntime *self, const groove_GraphBuilder *graph, bool template_, groove_CompiledNode *out) {
    JZ_TRY(RP(validate_collect_by_terminality)(graph));
    jz_hmap output_memo, compiled_memo;
    jz_hmap_init(&output_memo, &groove_ivm_rt_USIZE_DESC_MAP);
    jz_hmap_init(&compiled_memo, &groove_ivm_rt_USIZE_COMPILED_MAP);
    groove_CompilationStepVec order = {0};
    groove__ivm__runtime__compilation_cache__compilation_order(graph, &order);
    jz_err e = JZ_OK;
    JZ_VEC_FOR(groove_CompilationStep, step, order) {
        const groove_GraphBuilder *builder = step->graph;
        if (!template_ && step_has_template_arguments(builder)) { e = GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator); break; }
        usize key = graph_builder_key(builder);
        groove_CompiledNode compiled; groove_RecordDescriptor inferred; u64 logical_nodes;
        if (step->owner && groove__ivm__runtime__compilation_cache__CompilationCache__get(&self->compilation_cache, step->owner, &compiled_memo, &output_memo, self->graph, &compiled, &inferred, &logical_nodes)) {
            self->logical_nodes_requested += logical_nodes;
            jz_hmap_insert(&output_memo, &key, &inferred, NULL);
            jz_hmap_insert(&compiled_memo, &key, &compiled, NULL);
            continue;
        }
        /* Inputs have already been inferred and compiled: source ownership and
         * schema checks run before a parent's reuse decision. */
        e = groove__ivm__runtime__subscriptions__IvmRuntime__infer_builder_output_uncached(self, builder, &output_memo, &inferred);
        if (JZ_IS_ERR(e)) break;
        jz_hmap_insert(&output_memo, &key, &inferred, NULL);
        u64 before = self->logical_nodes_requested;
        e = add_dedup_graph_cached(self, builder, &output_memo, &compiled_memo, &compiled);
        if (JZ_IS_ERR(e)) break;
        if (step->owner)
            groove__ivm__runtime__compilation_cache__CompilationCache__insert(&self->compilation_cache, step->owner, &compiled_memo, &output_memo, compiled, inferred, self->logical_nodes_requested - before);
    }
    jz_vec_drop(&order, sizeof(groove_CompilationStep), NULL);
    if (!JZ_IS_ERR(e)) {
        usize key = graph_builder_key(graph);
        if (!jz_hmap_remove(&compiled_memo, &key, NULL, out)) e = GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
    }
    jz_hmap_drop(&output_memo); jz_hmap_drop(&compiled_memo);
    return e;
}

jz_err CMP(add_dedup_graph)(groove_IvmRuntime *self, const groove_GraphBuilder *g, groove_CompiledNode *out) { return add_dedup_graph_inner(self, g, false, out); }
/* Only the isolated template compiler accepts unbound scalar arguments. */
jz_err CMP(add_dedup_template_graph)(groove_IvmRuntime *self, const groove_GraphBuilder *g, groove_CompiledNode *out) { return add_dedup_graph_inner(self, g, true, out); }

static jz_err add_dedup_source_graph(groove_IvmRuntime *self, const groove_GraphBuilder *graph, groove_RecordDescriptor inferred, jz_hmap *om, jz_hmap *cm, groove_CompiledNode *out);
static jz_err add_dedup_ordering_graph(groove_IvmRuntime *self, const groove_GraphBuilder *graph, groove_RecordDescriptor inferred, jz_hmap *om, jz_hmap *cm, groove_CompiledNode *out);
static jz_err add_dedup_unary_graph(groove_IvmRuntime *self, const groove_GraphBuilder *graph, groove_RecordDescriptor inferred, jz_hmap *om, jz_hmap *cm, groove_CompiledNode *out);
static jz_err add_dedup_join_graph(groove_IvmRuntime *self, const groove_GraphBuilder *graph, groove_RecordDescriptor inferred, jz_hmap *om, jz_hmap *cm, groove_CompiledNode *out);
static jz_err add_dedup_lookup_graph(groove_IvmRuntime *self, const groove_GraphBuilder *graph, groove_RecordDescriptor inferred, jz_hmap *om, jz_hmap *cm, groove_CompiledNode *out);

static jz_err add_dedup_graph_cached(groove_IvmRuntime *self, const groove_GraphBuilder *graph, jz_hmap *output_memo, jz_hmap *compiled_memo, groove_CompiledNode *out) {
    usize key = graph_builder_key(graph);
    const groove_CompiledNode *hit = jz_hmap_get(compiled_memo, &key);
    if (hit) { *out = *hit; return JZ_OK; }
    groove_RecordDescriptor inferred;
    JZ_TRY(groove__ivm__runtime__subscriptions__IvmRuntime__infer_builder_output_cached(self, graph, output_memo, &inferred));
    groove_CompiledNode compiled;
    switch (graph->tag) {
    case groove_GB_TypedTemplate: {
        u32 n = graph->inputs.len;
        groove_CompiledNode *inputs = calloc(n ? n : 1, sizeof *inputs);
        for (u32 i = 0; i < n; i++) {
            usize k = graph_builder_key(graph->inputs.ptr[i]);
            const groove_CompiledNode *c = jz_hmap_get(compiled_memo, &k);
            if (!c) { free(inputs); return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator); }
            inputs[i] = *c;
        }
        jz_err e = groove__ivm__runtime__typed_template__IvmRuntime__install_typed_template(self, graph->program, inputs, n, graph->inputs.ptr, graph->inputs.len,
            graph->predicates.ptr, graph->predicates.len, graph->scalars, graph->scalars_len, &compiled);
        free(inputs);
        JZ_TRY(e);
        break;
    }
    case groove_GB_TemplateInput:
        if (!graph->input) return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
        JZ_TRY(add_dedup_graph_cached(self, graph->input, output_memo, compiled_memo, &compiled));
        if (compiled.output != graph->output) return GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch);
        break;
    case groove_GB_Table: case groove_GB_InlineRecords: case groove_GB_InputSource: case groove_GB_Index:
    case groove_GB_FrontierSource: case groove_GB_BindingSource: case groove_GB_Recursive:
    case groove_GB_RecursiveStepWitness: case groove_GB_CollectBy:
        JZ_TRY(add_dedup_source_graph(self, graph, inferred, output_memo, compiled_memo, &compiled));
        break;
    case groove_GB_ArgMaxBy: case groove_GB_ArgMinBy: case groove_GB_TopBy:
        JZ_TRY(add_dedup_ordering_graph(self, graph, inferred, output_memo, compiled_memo, &compiled));
        break;
    case groove_GB_Aggregate: case groove_GB_Filter: case groove_GB_Project: case groove_GB_StreamingChecksum:
    case groove_GB_UnwrapNullable: case groove_GB_Unnest: case groove_GB_VariantProject: case groove_GB_Union:
        JZ_TRY(add_dedup_unary_graph(self, graph, inferred, output_memo, compiled_memo, &compiled));
        break;
    case groove_GB_Join: case groove_GB_SemiJoin: case groove_GB_AntiJoin:
        JZ_TRY(add_dedup_join_graph(self, graph, inferred, output_memo, compiled_memo, &compiled));
        break;
    case groove_GB_Lookup:
        JZ_TRY(add_dedup_lookup_graph(self, graph, inferred, output_memo, compiled_memo, &compiled));
        break;
    default:
        JZ_UNREACHABLE();
    }
    jz_hmap_insert(compiled_memo, &key, &compiled, NULL);
    *out = compiled;
    return JZ_OK;
}

static void table_source_from_builder(const groove_GraphBuilder *g, groove_TableSourceOp *t) {
    memset(t, 0, sizeof *t);
    t->table = jz_str_clone(&g->table);
    t->has_scan = g->has_scan;
    if (g->has_scan) groove_StaticScanSpec_clone(&t->scan, &g->scan);
    t->has_vp = g->has_variant_projection;
    if (g->has_variant_projection) { t->variant_projection.tag = groove_VPT_Named; t->variant_projection.name = jz_str_clone(&g->variant_projection); }
}

/* A keyed lookup: one node over its key relation that probes the table by
 * each live key. */
__attribute__((noinline))
static jz_err add_dedup_lookup_graph(groove_IvmRuntime *self, const groove_GraphBuilder *graph, groove_RecordDescriptor inferred_output, jz_hmap *om, jz_hmap *cm, groove_CompiledNode *out) {
    if (graph->tag != groove_GB_Lookup) return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
    u32 prefix_len = 0;
    if (graph->has_scan) {
        if (graph->scan.tag == groove_Scan_Prefix || graph->scan.tag == groove_Scan_Point) prefix_len = graph->scan.a.len;
        else return GROOVE_IVM_ERR_C(GROOVE_IVM_UnsupportedLookup, "a lookup's static scan must be a point or prefix");
    }
    if (graph->keys_on.len == 0) return GROOVE_IVM_ERR_C(GROOVE_IVM_UnsupportedLookup, "a lookup needs at least one key field");
    groove_CompiledNode compiled_keys;
    JZ_TRY(add_dedup_graph_cached(self, graph->keys, om, cm, &compiled_keys));
    groove_RecordDescriptor key_descriptor = compiled_keys.output;
    groove_U32Vec key_fields;
    JZ_TRY(resolve_refs(key_descriptor, graph->keys_on.ptr, graph->keys_on.len, &key_fields));
    const groove_TableSchema *table_schema = groove__schema__DatabaseSchema__table(&self->schema, graph->table.ptr, graph->table.len);
    if (!table_schema) { u32_vec_drop(&key_fields); return GROOVE_IVM_ERR_S(GROOVE_IVM_TableNotFound, &graph->table); }
    groove_U32Vec primary_key;
    jz_err e = groove__ivm__runtime__key_encoding__primary_key_field_indices(table_schema, inferred_output, &primary_key);
    if (JZ_IS_ERR(e)) { u32_vec_drop(&key_fields); return e; }
    if (primary_key.len < prefix_len + key_fields.len) {
        e = GROOVE_IVM_ERR_C(GROOVE_IVM_UnsupportedLookup, "a lookup's prefix and keys exceed the table's primary key");
    } else {
        /* The probe is the storage key encoding of the key values, so each
         * key field must have the type of the primary-key column it names. */
        const groove_DescriptorField *kf = FIELDS(key_descriptor), *of = FIELDS(inferred_output);
        for (u32 i = 0; i < key_fields.len; i++) {
            if (!groove_ValueType_eq(&kf[key_fields.ptr[i]].value_type, &of[primary_key.ptr[prefix_len + i]].value_type)) {
                e = GROOVE_IVM_ERR_C(GROOVE_IVM_UnsupportedLookup, "a lookup key field must have its primary-key column's type");
                break;
            }
        }
    }
    u32_vec_drop(&primary_key);
    if (JZ_IS_ERR(e)) { u32_vec_drop(&key_fields); return e; }
    groove_OpType op = op_of(groove_Op_Lookup);
    table_source_from_builder(graph, &op.v.lookup.source);
    op.v.lookup.key_descriptor = key_descriptor;
    VMOVE(op.v.lookup.key_fields, key_fields);
    groove_NodeId node = dedup_records(self, op, &compiled_keys.node, 1, inferred_output);
    *out = compiled_node(inferred_output, node, false, 0);
    return JZ_OK;
}

static jz_err index_source_op(groove_IvmRuntime *self, const groove_TableSchema *table, const groove_IndexSchema *index, const groove_GraphBuilder *g, groove_IndexSourceOp *out);
static jz_err add_collect_by_graph(groove_IvmRuntime *self, const groove_GraphBuilder *input, const groove_CollectByBuilder *collect, groove_RecordDescriptor output, jz_hmap *om, jz_hmap *cm, groove_CompiledNode *out);

static const groove_IndexSchema *find_index(const groove_TableSchema *t, const jz_str *name) {
    JZ_VEC_FOR(groove_IndexSchema, i, t->indices) if (jz_str_eq(&i->name, name)) return i;
    return NULL;
}

__attribute__((noinline))
static jz_err add_dedup_source_graph(groove_IvmRuntime *self, const groove_GraphBuilder *graph, groove_RecordDescriptor inferred_output, jz_hmap *om, jz_hmap *cm, groove_CompiledNode *out) {
    switch (graph->tag) {
    case groove_GB_Table: {
        groove_OpType op = op_of(groove_Op_TableSource);
        table_source_from_builder(graph, &op.v.table_source);
        groove_NodeId node = dedup_records(self, op, NULL, 0, inferred_output);
        *out = compiled_node(inferred_output, node, false, 0);
        return JZ_OK;
    }
    case groove_GB_InlineRecords: {
        if (!groove__records__RecordDescriptor__registry_compatible_with(inferred_output, graph->output)) return GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch);
        groove_OpType op = op_of(groove_Op_InlineRecords);
        JZ_VEC_FOR(jz_bytes, r, graph->records) { jz_bytes c = jz_bytes_clone(r); JZ_VEC_PUSH(op.v.inline_records.records, c); }
        groove_NodeId node = dedup_records(self, op, NULL, 0, graph->output);
        *out = compiled_node(graph->output, node, false, 0);
        return JZ_OK;
    }
    case groove_GB_InputSource: {
        if (!groove__ivm__graph__InputSourceId__belongs_to(graph->id, self->input_source_runtime_namespace)) return GROOVE_IVM_ERR(GROOVE_IVM_ForeignInputSource);
        if (!groove__records__RecordDescriptor__registry_compatible_with(inferred_output, graph->output)) return GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch);
        groove_BindingSourceKey key = groove__ivm__graph__InputSourceId__binding_key(graph->id);
        const groove_BindingSourceState *source = groove__ivm__runtime__subscriptions__BindingSources__get(&self->binding_sources, &key);
        if (!source) { groove_BindingSourceKey_drop(&key); return GROOVE_IVM_ERR(GROOVE_IVM_InputSourceRetired); }
        if (source->descriptor != graph->output) {
            groove_BindingSourceKey_drop(&key);
            jz_str name = groove__ivm__graph__InputSourceId__diagnostic_name(graph->id);
            jz_err e = GROOVE_IVM_ERR_S(GROOVE_IVM_BindingSourceDescriptorMismatch, &name);
            jz_str_drop(&name);
            return e;
        }
        /* A mutable input uses the same runtime delta and hydration machinery
         * as a prepared binding source. */
        groove_OpType op = op_of(groove_Op_BindingSource);
        op.v.binding_source.key = key;
        groove_NodeId node = dedup_records(self, op, NULL, 0, graph->output);
        *out = compiled_node(graph->output, node, false, 0);
        return JZ_OK;
    }
    case groove_GB_Index: {
        const groove_TableSchema *table = groove__schema__DatabaseSchema__table(&self->schema, graph->table.ptr, graph->table.len);
        if (!table) return GROOVE_IVM_ERR_S(GROOVE_IVM_TableNotFound, &graph->table);
        const groove_IndexSchema *index = find_index(table, &graph->index);
        if (!index) return GROOVE_IVM_ERR_S(GROOVE_IVM_IndexNotFound, &graph->index);
        groove_OpType op = op_of(groove_Op_IndexSource);
        JZ_TRY(index_source_op(self, table, index, graph, &op.v.index_source));
        groove_NodeId node = dedup_records(self, op, NULL, 0, inferred_output);
        *out = compiled_node(inferred_output, node, false, 0);
        return JZ_OK;
    }
    case groove_GB_FrontierSource: {
        groove_OpType op = op_of(groove_Op_FrontierSource);
        op.v.frontier_source.binding.name = jz_str_clone(&graph->binding.name);
        groove_NodeId node = dedup_records(self, op, NULL, 0, graph->output);
        *out = compiled_node(inferred_output, node, false, 0);
        return JZ_OK;
    }
    case groove_GB_BindingSource: {
        groove_OpType op = op_of(groove_Op_BindingSource);
        op.v.binding_source.key.tag = groove_BindingSourceKey_Prepared;
        op.v.binding_source.key.prepared = jz_str_clone(&graph->shape);
        groove_NodeId node = dedup_records(self, op, NULL, 0, graph->output);
        *out = compiled_node(inferred_output, node, false, 0);
        return JZ_OK;
    }
    case groove_GB_Recursive: {
        if (groove__ivm__runtime__evaluator__builder_contains_recursive(graph->seed) ||
            groove__ivm__runtime__evaluator__builder_contains_recursive(graph->step) ||
            (graph->step_witness && groove__ivm__runtime__evaluator__builder_contains_recursive(graph->step_witness)))
            return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedNestedRecursion);
        groove_CompiledNode seed, step, witness;
        JZ_TRY(add_dedup_graph_cached(self, graph->seed, om, cm, &seed));
        JZ_TRY(add_dedup_graph_cached(self, graph->step, om, cm, &step));
        if (graph->step_witness) JZ_TRY(add_dedup_graph_cached(self, graph->step_witness, om, cm, &witness));
        if (!groove__records__RecordDescriptor__registry_compatible_with(seed.output, step.output) ||
            !groove__records__RecordDescriptor__registry_compatible_with(seed.output, inferred_output))
            return GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch);
        groove_OpType op = op_of(groove_Op_Recursive);
        groove_StrVec read_tables;
        JZ_TRY(groove__ivm__runtime__recursion__recursive_read_tables(self->graph, seed.node, step.node, graph->step_witness ? &witness.node : NULL, &read_tables));
        op.v.recursive.frontier.name = jz_str_clone(&graph->frontier.name);
        op.v.recursive.max_iters = graph->max_iters;
        op.v.recursive.truncate_at_max_iters = graph->truncate_at_max_iters;
        op.v.recursive.has_step_witness_output = graph->step_witness != NULL;
        if (graph->step_witness) op.v.recursive.step_witness_output = witness.output;
        VMOVE(op.v.recursive.read_tables, read_tables);
        groove_NodeId inputs[3] = { seed.node, step.node, graph->step_witness ? witness.node : 0 };
        groove_NodeId node = dedup_records(self, op, inputs, graph->step_witness ? 3 : 2, inferred_output);
        *out = compiled_node(inferred_output, node, false, 0);
        return JZ_OK;
    }
    case groove_GB_RecursiveStepWitness: {
        const groove_GraphBuilder *recursive = graph->recursive;
        if (recursive->tag != groove_GB_Recursive || !recursive->step_witness) return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
        groove_CompiledNode compiled_recursive;
        JZ_TRY(add_dedup_graph_cached(self, recursive, om, cm, &compiled_recursive));
        const groove_GraphNode *rn = groove__ivm__graph__IvmGraph__node(self->graph, compiled_recursive.node);
        if (!rn) return GROOVE_IVM_ERR_N(GROOVE_IVM_GraphNodeNotFound, compiled_recursive.node);
        if (rn->descriptor.inputs.len != 3) return GROOVE_IVM_ERR_N(GROOVE_IVM_GraphInputArityMismatch, compiled_recursive.node);
        groove_NodeId witness = rn->descriptor.inputs.ptr[2];
        const groove_GraphNode *wn = groove__ivm__graph__IvmGraph__node(self->graph, witness);
        if (!wn) return GROOVE_IVM_ERR_N(GROOVE_IVM_GraphNodeNotFound, witness);
        groove_RecordDescriptor output = wn->descriptor.output.records;
        groove_NodeId node = dedup_records(self, op_of(groove_Op_RecursiveStepWitness), &compiled_recursive.node, 1, output);
        *out = compiled_node(output, node, false, 0);
        return JZ_OK;
    }
    case groove_GB_CollectBy:
        return add_collect_by_graph(self, graph->input, graph->collect, inferred_output, om, cm, out);
    default:
        jz_panic("dispatcher routes only source graph builders here");
    }
}

static groove_StrVec str_vec_clone(const groove_StrVec *s) {
    groove_StrVec v = {0};
    JZ_VEC_FOR(jz_str, x, *s) { jz_str c = jz_str_clone(x); JZ_VEC_PUSH(v, c); }
    return v;
}

/* ArgMaxBy and ArgMinBy are two identical match arms in Rust. */
static jz_err compile_arg_by(groove_IvmRuntime *self, const groove_GraphBuilder *graph, groove_RecordDescriptor output, jz_hmap *om, jz_hmap *cm, bool max, groove_CompiledNode *out) {
    groove_CompiledNode compiled_input;
    JZ_TRY(add_dedup_graph_cached(self, graph->input, om, cm, &compiled_input));
    groove_U32Vec group = {0}, order = {0}, comparison = {0};
    groove_StrVec group_names = {0}, order_names = {0};
    jz_err e = resolve_refs(output, graph->group_cols.ptr, graph->group_cols.len, &group);
    if (!JZ_IS_ERR(e)) e = resolve_refs(output, graph->order_cols.ptr, graph->order_cols.len, &order);
    if (JZ_IS_ERR(e)) goto fail;
    if (graph->input->tag == groove_GB_Table) {
        const jz_str *table = &graph->input->table;
        const groove_TableSchema *table_schema = groove__schema__DatabaseSchema__table(&self->schema, table->ptr, table->len);
        if (!table_schema) { e = GROOVE_IVM_ERR_S(GROOVE_IVM_TableNotFound, table); goto fail; }
        if (!table_schema->has_primary_key) { e = GROOVE_IVM_ERR_S(GROOVE_IVM_MissingPrimaryKey, table); goto fail; }
        JZ_VEC_FOR(groove_PrimaryKeyColumn, column, table_schema->primary_key.columns) {
            u32 idx;
            if (!groove__records__RecordDescriptor__field_index(output, column->column.ptr, column->column.len, &idx)) {
                e = GROOVE_IVM_ERR_S(GROOVE_IVM_GraphFieldNotFound, &column->column); goto fail;
            }
            JZ_VEC_PUSH(comparison, idx);
        }
        e = groove__ivm__runtime__evaluator__validate_arg_by_primary_key_indices(max ? "arg_max_by" : "arg_min_by", table_schema,
            (const usize *)group.ptr, group.len, (const usize *)order.ptr, order.len, (const usize *)comparison.ptr, comparison.len);
        if (JZ_IS_ERR(e)) goto fail;
    } else {
        jz_vec_extend(&comparison, group.ptr, group.len, sizeof(u32));
        jz_vec_extend(&comparison, order.ptr, order.len, sizeof(u32));
    }
    e = names_at(output, group.ptr, group.len, &group_names); if (JZ_IS_ERR(e)) goto fail;
    e = names_at(output, order.ptr, order.len, &order_names); if (JZ_IS_ERR(e)) goto fail;
    groove_StrVec arrangement_fields = str_vec_clone(&group_names);
    groove_NodeId arrangement = add_arrangement_node(self, compiled_input.node, output, arrangement_fields, groove_ValueComparison_Exact);
    groove_OpType op = op_of(max ? groove_Op_ArgMaxBy : groove_Op_ArgMinBy);
    VMOVE(op.v.arg_by.group_fields, group_names);
    VMOVE(op.v.arg_by.order_fields, order_names);
    VMOVE(op.v.arg_by.group_field_indices, group);
    VMOVE(op.v.arg_by.comparison_field_indices, comparison);
    u32_vec_drop(&order);
    groove_NodeId node = dedup_records(self, op, &arrangement, 1, output);
    *out = compiled_inherit(output, node, &compiled_input);
    return JZ_OK;
fail:
    u32_vec_drop(&group); u32_vec_drop(&order); u32_vec_drop(&comparison);
    str_vec_drop(&group_names); str_vec_drop(&order_names);
    return e;
}


__attribute__((noinline))
static jz_err add_dedup_ordering_graph(groove_IvmRuntime *self, const groove_GraphBuilder *graph, groove_RecordDescriptor output, jz_hmap *om, jz_hmap *cm, groove_CompiledNode *out) {
    if (graph->tag == groove_GB_ArgMaxBy) return compile_arg_by(self, graph, output, om, cm, true, out);
    if (graph->tag == groove_GB_ArgMinBy) return compile_arg_by(self, graph, output, om, cm, false, out);
    if (graph->tag != groove_GB_TopBy) jz_panic("dispatcher routes only ordering graph builders here");
    groove_CompiledNode compiled_input;
    JZ_TRY(add_dedup_graph_cached(self, graph->input, om, cm, &compiled_input));
    groove_U32Vec group = {0}, order = {0}, tie = {0};
    groove_StrVec group_names = {0}, tie_names = {0};
    JZ_VEC(groove_TopByOrderField) order_fields = {0};
    jz_err e = resolve_refs(output, graph->group_cols.ptr, graph->group_cols.len, &group);
    if (JZ_IS_ERR(e)) goto fail;
    JZ_VEC_FOR(groove_TopByOrder, o, graph->top_order_cols) {
        u32 idx; e = RP(resolve_field_ref)(output, &o->field, &idx);
        if (JZ_IS_ERR(e)) goto fail;
        JZ_VEC_PUSH(order, idx);
    }
    e = resolve_refs(output, graph->tie_cols.ptr, graph->tie_cols.len, &tie);
    if (JZ_IS_ERR(e)) goto fail;
    u32 nfields = groove__records__RecordDescriptor__len(output);
    const groove_DescriptorField *fields = FIELDS(output);
    for (u32 pass = 0; pass < 2; pass++) {
        const groove_U32Vec *v = pass == 0 ? &order : &tie;
        for (u32 i = 0; i < v->len; i++) {
            u32 field_idx = v->ptr[i];
            if (field_idx >= nfields) { e = groove__ivm__runtime__IvmRuntimeError__with_usize(GROOVE_IVM_GraphFieldIndexOutOfBounds, field_idx); goto fail; }
            if (!groove__records__ValueType__collect_by_ordered_scalar(&fields[field_idx].value_type)) {
                jz_str name; e = RP(field_name_at)(output, field_idx, &name);
                if (JZ_IS_ERR(e)) goto fail;
                jz_str msg = {0}; jz_str_appendf(&msg, "sort field %.*s must be an orderable scalar", (int)name.len, name.ptr);
                e = GROOVE_IVM_ERR_S(GROOVE_IVM_InvalidTopBy, &msg);
                jz_str_drop(&msg); jz_str_drop(&name);
                goto fail;
            }
        }
    }
    e = names_at(output, group.ptr, group.len, &group_names); if (JZ_IS_ERR(e)) goto fail;
    for (u32 i = 0; i < order.len; i++) {
        groove_TopByOrderField f; e = RP(field_name_at)(output, order.ptr[i], &f.field);
        if (JZ_IS_ERR(e)) goto fail;
        f.direction = graph->top_order_cols.ptr[i].direction;
        JZ_VEC_PUSH(order_fields, f);
    }
    e = names_at(output, tie.ptr, tie.len, &tie_names); if (JZ_IS_ERR(e)) goto fail;
    groove_OpType op = op_of(groove_Op_TopBy);
    jz_vec_extend(&op.v.top_by.sort_field_indices, order.ptr, order.len, sizeof(u32));
    jz_vec_extend(&op.v.top_by.sort_field_indices, tie.ptr, tie.len, sizeof(u32));
    JZ_VEC_FOR(groove_TopByOrder, o, graph->top_order_cols) { u8 d = (u8)o->direction; JZ_VEC_PUSH(op.v.top_by.sort_directions, d); }
    for (u32 i = 0; i < tie.len; i++) { u8 d = groove_TopByDirection_Asc; JZ_VEC_PUSH(op.v.top_by.sort_directions, d); }
    groove_NodeId arrangement = add_arrangement_node(self, compiled_input.node, output, str_vec_clone(&group_names), groove_ValueComparison_Exact);
    VMOVE(op.v.top_by.group_fields, group_names);
    VMOVE(op.v.top_by.group_field_indices, group);
    VMOVE(op.v.top_by.order_fields, order_fields);
    VMOVE(op.v.top_by.tie_fields, tie_names);
    op.v.top_by.offset = graph->offset;
    op.v.top_by.limit = graph->limit;
    u32_vec_drop(&order); u32_vec_drop(&tie);
    groove_NodeId node = dedup_records(self, op, &arrangement, 1, output);
    *out = compiled_node(output, node, true, node);
    return JZ_OK;
fail:
    u32_vec_drop(&group); u32_vec_drop(&order); u32_vec_drop(&tie);
    str_vec_drop(&group_names); str_vec_drop(&tie_names);
    JZ_VEC_FOR(groove_TopByOrderField, f, order_fields) jz_str_drop(&f->field);
    jz_vec_drop(&order_fields, sizeof(groove_TopByOrderField), NULL);
    return e;
}

static bool all_field_exprs(const groove_ProjectionExpr *e, u32 n) {
    for (u32 i = 0; i < n; i++) if (e[i].expression.tag != groove_ProjectExpr_Field) return false;
    return true;
}
static void projection_expr_drop_v(void *p) { groove_ProjectionExpr_drop(p); }

static jz_err compile_project(groove_IvmRuntime *self, const groove_GraphBuilder *graph, groove_RecordDescriptor output, jz_hmap *om, jz_hmap *cm, groove_CompiledNode *out) {
    groove_CompiledNode compiled_input;
    JZ_TRY(add_dedup_graph_cached(self, graph->input, om, cm, &compiled_input));
    groove_NodeId input_node = compiled_input.node;
    groove_ProjectionPlan *plan;
    JZ_TRY(groove__ivm__runtime__typed_projection__IvmRuntime__projection_plan(self, compiled_input.output, graph->fields.ptr, graph->fields.len, &plan));
    const groove_ProjectionExprVec *plan_exprs;
    jz_err e = groove__ivm__runtime__typed_projection__ProjectionPlan__expressions(plan, &plan_exprs);
    if (JZ_IS_ERR(e)) { jz_rc_release(plan); return e; }
    ProjectionExprVecB expressions = {0};
    jz_vec_clone(&expressions, plan_exprs, sizeof(groove_ProjectionExpr), (void (*)(void *, const void *))groove_ProjectionExpr_clone);
    jz_rc_release(plan);
    /* Compose only total field selections; never cross enum conversions,
     * constants, filters, joins, winner selection or other semantic ops. */
    while (all_field_exprs(expressions.ptr, expressions.len)) {
        const groove_GraphNode *parent = groove__ivm__graph__IvmGraph__node(self->graph, input_node);
        if (!parent) { e = GROOVE_IVM_ERR_N(GROOVE_IVM_GraphNodeNotFound, input_node); goto fail; }
        if (parent->descriptor.operator_.tag != groove_Op_MapProject) break;
        const groove_MapProjectOp *pp = &parent->descriptor.operator_.v.map_project;
        if (pp->expressions.len == 0 || !all_field_exprs(pp->expressions.ptr, pp->expressions.len)) break;
        JZ_VEC_FOR(groove_ProjectionExpr, expr, expressions) {
            u32 index;
            e = RP(resolve_field_ref)(parent->descriptor.output.records, &expr->expression.field, &index);
            if (JZ_IS_ERR(e)) goto fail;
            groove_ProjectExpr next; groove_ProjectExpr_clone(&next, &pp->expressions.ptr[index].expression);
            groove_ProjectExpr_drop(&expr->expression);
            expr->expression = next;
        }
        input_node = parent->descriptor.inputs.ptr[0];
    }
    e = narrow_projected_join(self, input_node, &expressions, &input_node);
    if (JZ_IS_ERR(e)) goto fail;
    const groove_GraphNode *src = groove__ivm__graph__IvmGraph__node(self->graph, input_node);
    if (!src) { e = GROOVE_IVM_ERR_N(GROOVE_IVM_GraphNodeNotFound, input_node); goto fail; }
    groove_RecordDescriptor source_output = src->descriptor.output.records;
    groove_OpType op = op_of(groove_Op_MapProject);
    JZ_VEC_FOR(groove_ProjectionExpr, expr, expressions) {
        if (expr->expression.tag != groove_ProjectExpr_Field) continue;
        u32 idx; e = RP(resolve_field_ref)(source_output, &expr->expression.field, &idx);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&op.v.map_project.mapping, sizeof(groove_UsizePair), NULL); goto fail; }
        groove_UsizePair m = { 0, idx };
        JZ_VEC_PUSH(op.v.map_project.mapping, m);
    }
    VMOVE(op.v.map_project.expressions, expressions);
    groove_NodeId node = dedup_records(self, op, &input_node, 1, output);
    *out = compiled_inherit(output, node, &compiled_input);
    return JZ_OK;
fail:
    jz_vec_drop(&expressions, sizeof(groove_ProjectionExpr), projection_expr_drop_v);
    return e;
}

__attribute__((noinline))
static jz_err add_dedup_unary_graph(groove_IvmRuntime *self, const groove_GraphBuilder *graph, groove_RecordDescriptor output, jz_hmap *om, jz_hmap *cm, groove_CompiledNode *out) {
    groove_CompiledNode ci;
    switch (graph->tag) {
    case groove_GB_Aggregate: {
        JZ_TRY(add_dedup_graph_cached(self, graph->input, om, cm, &ci));
        groove_RecordDescriptor input_output = ci.output;
        groove_U32Vec group;
        JZ_TRY(resolve_refs(input_output, graph->group_cols.ptr, graph->group_cols.len, &group));
        groove_PlanExprVec group_key = {0};
        JZ_VEC(groove_AggregateExpr) aggregates = {0};
        jz_err e = JZ_OK;
        JZ_VEC_FOR(u32, field, group) {
            groove_FieldRef carrier; memset(&carrier, 0, sizeof carrier);
            carrier.tag = groove_FieldRef_StoredName;
            e = RP(field_name_at)(input_output, *field, &carrier.name);
            if (JZ_IS_ERR(e)) break;
            u32 idx; e = RP(resolve_field_ref)(input_output, &carrier, &idx);
            if (!JZ_IS_ERR(e) && idx != *field) {
                jz_str name = groove__ivm__graph__FieldRef__display_name(&carrier);
                e = GROOVE_IVM_ERR_S(GROOVE_IVM_AmbiguousGraphFieldReference, &name);
                jz_str_drop(&name);
            }
            /* Aggregate evaluation consumes PlanExpr names: only use a
             * spelling that resolves back to this group slot. */
            jz_str name;
            if (!JZ_IS_ERR(e)) e = RP(field_ref_name)(input_output, &carrier, &name);
            groove_FieldRef_drop(&carrier);
            if (JZ_IS_ERR(e)) break;
            JZ_VEC_PUSH(group_key, plan_field(name));
        }
        if (!JZ_IS_ERR(e)) {
            JZ_VEC_FOR(groove_AggregateExpr, aggregate, graph->aggregates) {
                groove_AggregateExpr resolved_agg;
                e = groove__ivm__runtime__aggregate__resolve_aggregate_expr(input_output, aggregate, &resolved_agg);
                if (JZ_IS_ERR(e)) break;
                JZ_VEC_PUSH(aggregates, resolved_agg);
            }
        }
        if (JZ_IS_ERR(e)) {
            u32_vec_drop(&group); plan_vec_drop(&group_key);
            jz_vec_drop(&aggregates, sizeof(groove_AggregateExpr), NULL); /* no AggregateExpr drop mirror */
            return e;
        }
        groove_StrVec names = {0}; plan_names(&group_key, &names);
        groove_NodeId arrangement = add_arrangement_node(self, ci.node, input_output, names, groove_ValueComparison_Exact);
        groove_OpType op = op_of(groove_Op_Aggregate);
        op.v.aggregate.group_key = group_key;
        VMOVE(op.v.aggregate.group_field_indices, group);
        VMOVE(op.v.aggregate.aggregates, aggregates);
        groove_NodeId node = dedup_records(self, op, &arrangement, 1, output);
        *out = compiled_inherit(output, node, &ci);
        return JZ_OK;
    }
    case groove_GB_Filter: {
        JZ_TRY(add_dedup_graph_cached(self, graph->input, om, cm, &ci));
        groove_OpType op = op_of(groove_Op_Filter);
        groove_PredicateExpr_clone(&op.v.filter.predicate, &graph->predicate);
        op.v.filter.comparison = graph->comparison;
        groove_NodeId node = dedup_records(self, op, &ci.node, 1, output);
        *out = compiled_inherit(output, node, &ci);
        return JZ_OK;
    }
    case groove_GB_Project:
        return compile_project(self, graph, output, om, cm, out);
    case groove_GB_StreamingChecksum: {
        if (graph->window_bytes == 0 || graph->max_bytes_per_turn == 0) return GROOVE_IVM_ERR(GROOVE_IVM_InvalidStreamingChecksumBudget);
        JZ_TRY(add_dedup_graph_cached(self, graph->input, om, cm, &ci));
        groove_OpType op = op_of(groove_Op_StreamingChecksum);
        JZ_TRY(RP(resolve_field_ref)(ci.output, &graph->field, &op.v.streaming_checksum.field_idx));
        JZ_TRY(RP(field_ref_name)(ci.output, &graph->field, &op.v.streaming_checksum.field));
        op.v.streaming_checksum.output_field = jz_str_clone(&graph->output_field);
        op.v.streaming_checksum.window_bytes = graph->window_bytes;
        op.v.streaming_checksum.max_bytes_per_turn = graph->max_bytes_per_turn;
        groove_NodeId node = dedup_records(self, op, &ci.node, 1, output);
        *out = compiled_inherit(output, node, &ci);
        return JZ_OK;
    }
    case groove_GB_UnwrapNullable: {
        JZ_TRY(add_dedup_graph_cached(self, graph->input, om, cm, &ci));
        groove_OpType op = op_of(groove_Op_UnwrapNullable);
        JZ_TRY(RP(resolve_field_ref)(ci.output, &graph->field, &op.v.unwrap_nullable.field_idx));
        JZ_TRY(RP(field_ref_name)(ci.output, &graph->field, &op.v.unwrap_nullable.field));
        groove_NodeId node = dedup_records(self, op, &ci.node, 1, output);
        *out = compiled_inherit(output, node, &ci);
        return JZ_OK;
    }
    case groove_GB_Unnest: {
        JZ_TRY(add_dedup_graph_cached(self, graph->input, om, cm, &ci));
        groove_OpType op = op_of(groove_Op_Unnest);
        JZ_TRY(RP(resolve_field_ref)(ci.output, &graph->field, &op.v.unnest.array_field_idx));
        JZ_TRY(RP(field_ref_name)(ci.output, &graph->field, &op.v.unnest.array_field));
        op.v.unnest.element_field = jz_str_clone(&graph->element_field);
        groove_NodeId node = dedup_records(self, op, &ci.node, 1, output);
        *out = compiled_inherit(output, node, &ci);
        return JZ_OK;
    }
    case groove_GB_VariantProject: {
        JZ_TRY(add_dedup_graph_cached(self, graph->input, om, cm, &ci));
        u32 field_idx;
        JZ_TRY(RP(resolve_field_ref)(ci.output, &graph->field, &field_idx));
        const groove_ValueType *vt = &FIELDS(ci.output)[field_idx].value_type;
        if (vt->tag != groove_ValueType_Enum) {
            jz_str name; JZ_TRY(RP(field_ref_name)(ci.output, &graph->field, &name));
            jz_err e = GROOVE_IVM_ERR_S(GROOVE_IVM_VariantProjectFieldTypeMismatch, &name);
            jz_str_drop(&name);
            return e;
        }
        u32 tag;
        JZ_TRY(groove__records__values__EnumSchema__tag(vt->v.enm, graph->case_name.ptr, graph->case_name.len, &tag));
        groove_OpType op = op_of(groove_Op_VariantProject);
        JZ_TRY(RP(field_ref_name)(ci.output, &graph->field, &op.v.variant_project.field));
        op.v.variant_project.field_idx = field_idx;
        op.v.variant_project.tag = tag;
        groove_NodeId node = dedup_records(self, op, &ci.node, 1, output);
        *out = compiled_inherit(output, node, &ci);
        return JZ_OK;
    }
    case groove_GB_Union: {
        groove_NodeIdVec input_nodes = {0};
        bool has_public = false; groove_NodeId public_root_ordering = 0;
        JZ_VEC_FOR(groove_GraphBuilder *, input, graph->inputs) {
            jz_err e = add_dedup_graph_cached(self, *input, om, cm, &ci);
            if (JZ_IS_ERR(e)) { jz_vec_drop(&input_nodes, sizeof(groove_NodeId), NULL); return e; }
            /* Structured lowering places the public/root anchor first. */
            if (input_nodes.len == 0) { has_public = ci.has_root_ordering_node; public_root_ordering = ci.root_ordering_node; }
            if (!groove__records__RecordDescriptor__registry_compatible_with(output, ci.output)) {
                jz_vec_drop(&input_nodes, sizeof(groove_NodeId), NULL);
                return GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch);
            }
            JZ_VEC_PUSH(input_nodes, ci.node);
        }
        groove_NodeId node = dedup_records(self, op_of(groove_Op_Union), input_nodes.ptr, input_nodes.len, output);
        jz_vec_drop(&input_nodes, sizeof(groove_NodeId), NULL);
        *out = compiled_node(output, node, has_public, public_root_ordering);
        return JZ_OK;
    }
    default:
        jz_panic("dispatcher routes only unary graph builders here");
    }
}

static groove_CompiledNode join_root_ordering(groove_RecordDescriptor output, groove_NodeId node, const groove_CompiledNode *l, const groove_CompiledNode *r) {
    /* Prefer the public (left) side so a nested TopBy cannot become root ordering. */
    if (l->has_root_ordering_node) return compiled_node(output, node, true, l->root_ordering_node);
    return compiled_node(output, node, r->has_root_ordering_node, r->root_ordering_node);
}

__attribute__((noinline))
static jz_err add_dedup_join_graph(groove_IvmRuntime *self, const groove_GraphBuilder *graph, groove_RecordDescriptor output, jz_hmap *om, jz_hmap *cm, groove_CompiledNode *out) {
    if (graph->tag != groove_GB_Join && graph->tag != groove_GB_SemiJoin && graph->tag != groove_GB_AntiJoin)
        jz_panic("dispatcher routes only join graph builders here");
    groove_CompiledNode compiled_left, compiled_right;
    JZ_TRY(add_dedup_graph_cached(self, graph->left, om, cm, &compiled_left));
    JZ_TRY(add_dedup_graph_cached(self, graph->right, om, cm, &compiled_right));
    groove_PlanExprVec left_key = {0}, right_key = {0};
    if (graph->tag == groove_GB_Join) {
        JZ_TRY(plan_fields(compiled_right.output, graph->right_on.ptr, graph->right_on.len, &right_key));
    } else {
        groove_CompiledNode existence;
        JZ_TRY(compile_existence_input(self, compiled_right, graph->right_on.ptr, graph->right_on.len, &existence, &right_key));
        compiled_right = existence;
    }
    jz_err e = plan_fields(compiled_left.output, graph->left_on.ptr, graph->left_on.len, &left_key);
    if (JZ_IS_ERR(e)) { plan_vec_drop(&right_key); return e; }
    groove_StrVec ln = {0}, rn = {0};
    plan_names(&left_key, &ln); plan_names(&right_key, &rn);
    groove_NodeId arrangements[2];
    arrangements[0] = add_arrangement_node(self, compiled_left.node, compiled_left.output, ln, graph->comparison);
    arrangements[1] = add_arrangement_node(self, compiled_right.node, compiled_right.output, rn, graph->comparison);
    groove_OpType op = op_of(graph->tag == groove_GB_Join ? groove_Op_Join : graph->tag == groove_GB_SemiJoin ? groove_Op_SemiJoin : groove_Op_AntiJoin);
    op.v.join.kind = groove_JoinOpKind_Inner;
    op.v.join.left_key = left_key; op.v.join.right_key = right_key;
    op.v.join.left_descriptor = compiled_left.output; op.v.join.right_descriptor = compiled_right.output;
    op.v.join.has_residual = false;
    op.v.join.comparison = graph->comparison;
    groove_NodeId node = dedup_records(self, op, arrangements, 2, output);
    *out = join_root_ordering(output, node, &compiled_left, &compiled_right);
    return JZ_OK;
}

static void projection_vec_drop(groove_CollectByProjectionVec *v) {
    JZ_VEC_FOR(groove_CollectByProjection, p, *v) { jz_str_drop(&p->field); jz_str_drop(&p->output_name); }
    jz_vec_drop(v, sizeof(groove_CollectByProjection), NULL);
}

static jz_err order_field_list(groove_RecordDescriptor d, const groove_U32Vec *idx, const groove_TopByOrder *cols, u32 ncols, void *out_vec) {
    JZ_VEC(groove_TopByOrderField) *out = out_vec;
    for (u32 i = 0; i < idx->len; i++) {
        groove_TopByOrderField f;
        JZ_TRY(RP(field_name_at)(d, idx->ptr[i], &f.field));
        f.direction = i < ncols ? cols[i].direction : groove_TopByDirection_Asc;
        JZ_VEC_PUSH(*out, f);
    }
    return JZ_OK;
}

static jz_err add_collect_by_graph(groove_IvmRuntime *self, const groove_GraphBuilder *input, const groove_CollectByBuilder *collect, groove_RecordDescriptor output, jz_hmap *om, jz_hmap *cm, groove_CompiledNode *out) {
    groove_CompiledNode compiled_input;
    JZ_TRY(add_dedup_graph_cached(self, input, om, cm, &compiled_input));
    groove_RecordDescriptor input_output = compiled_input.output;
    groove_U32Vec group = {0}, order = {0}, tie = {0}, occurrence = {0};
    groove_CollectByProjectionVec parent_fields = {0}, child_fields = {0}, tuple_fields = {0};
    groove_CollectBySlotVec slots = {0};
    groove_StrVec group_fields = {0};
    groove_CollectByOp *cb = calloc(1, sizeof *cb);
    jz_err e = resolve_refs(input_output, collect->group_cols.ptr, collect->group_cols.len, &group);
    if (!JZ_IS_ERR(e)) e = RP(collect_by_projections)(input_output, collect->parent_fields.ptr, collect->parent_fields.len, &parent_fields);
    if (JZ_IS_ERR(e)) goto fail;
    if ((collect->mode == groove_CollectByMode_Collect || collect->mode == groove_CollectByMode_Root) &&
        (collect->slots.len > 0 || collect->mode == groove_CollectByMode_Root)) {
        if (parent_fields.len == 0) { e = GROOVE_IVM_ERR_C(GROOVE_IVM_InvalidCollectBy, "tree collect requires a parent projection"); goto fail; }
        e = RP(validate_collect_by_key_types)(input_output, group.ptr, group.len); if (JZ_IS_ERR(e)) goto fail;
        e = RP(collect_by_slots)(input_output, collect->slots.ptr, collect->slots.len, (const usize *)group.ptr, group.len, 1, &slots); if (JZ_IS_ERR(e)) goto fail;
        if (collect->order_cols.len == 0) jz_vec_extend(&order, group.ptr, group.len, sizeof(u32));
        else JZ_VEC_FOR(groove_TopByOrder, o, collect->order_cols) {
            u32 idx; e = RP(resolve_field_ref)(input_output, &o->field, &idx); if (JZ_IS_ERR(e)) goto fail;
            JZ_VEC_PUSH(order, idx);
        }
        if (collect->tie_cols.len == 0) jz_vec_extend(&tie, group.ptr, group.len, sizeof(u32));
        else { e = resolve_refs(input_output, collect->tie_cols.ptr, collect->tie_cols.len, &tie); if (JZ_IS_ERR(e)) goto fail; }
        e = RP(validate_collect_by_key_types)(input_output, order.ptr, order.len); if (JZ_IS_ERR(e)) goto fail;
        e = RP(validate_collect_by_key_types)(input_output, tie.ptr, tie.len); if (JZ_IS_ERR(e)) goto fail;
        e = names_at(input_output, group.ptr, group.len, &group_fields); if (JZ_IS_ERR(e)) goto fail;
        cb->mode = collect->mode;
        e = order_field_list(input_output, &order, collect->order_cols.ptr, collect->order_cols.len, &cb->order_fields); if (JZ_IS_ERR(e)) goto fail;
        groove_StrVec tie_names = {0};
        e = names_at(input_output, tie.ptr, tie.len, &tie_names); if (JZ_IS_ERR(e)) goto fail;
        VMOVE(cb->tie_fields, tie_names);
        jz_vec_extend(&cb->sort_field_indices, order.ptr, order.len, sizeof(u32));
        jz_vec_extend(&cb->sort_field_indices, tie.ptr, tie.len, sizeof(u32));
        for (u32 i = 0; i < order.len; i++) { u8 d = (u8)(i < collect->order_cols.len ? collect->order_cols.ptr[i].direction : groove_TopByDirection_Asc); JZ_VEC_PUSH(cb->sort_directions, d); }
        for (u32 i = 0; i < tie.len; i++) { u8 d = groove_TopByDirection_Asc; JZ_VEC_PUSH(cb->sort_directions, d); }
        groove_NodeId arrangement = add_arrangement_node(self, compiled_input.node, input_output, str_vec_clone(&group_fields), groove_ValueComparison_Exact);
        VMOVE(cb->group_fields, group_fields);
        VMOVE(cb->group_field_indices, group);
        VMOVE(cb->parent_fields, parent_fields);
        cb->child_descriptor = groove__records__RecordDescriptor__empty();
        cb->collection_field = (jz_str){0};
        cb->collection_field_index = 0;
        VMOVE(cb->slots, slots);
        cb->offset = collect->offset; cb->limit = collect->limit;
        u32_vec_drop(&order); u32_vec_drop(&tie);
        groove_OpType op = op_of(groove_Op_CollectBy);
        op.v.collect_by = cb;
        groove_NodeId node = dedup_records(self, op, &arrangement, 1, output);
        *out = compiled_inherit(output, node, &compiled_input);
        return JZ_OK;
    }
    e = RP(collect_by_projections)(input_output, collect->child_fields.ptr, collect->child_fields.len, &child_fields); if (JZ_IS_ERR(e)) goto fail;
    e = RP(collect_by_projections)(input_output, collect->tuple_fields.ptr, collect->tuple_fields.len, &tuple_fields); if (JZ_IS_ERR(e)) goto fail;
    e = resolve_refs(input_output, collect->occurrence_id_cols.ptr, collect->occurrence_id_cols.len, &occurrence); if (JZ_IS_ERR(e)) goto fail;
    /* Parent values must be determined by the group. */
    if (collect->mode == groove_CollectByMode_Collect) {
        JZ_VEC_FOR(groove_CollectByProjection, f, parent_fields) {
            bool in_group = false;
            JZ_VEC_FOR(u32, g, group) if (*g == f->field_idx) { in_group = true; break; }
            if (!in_group) { e = GROOVE_IVM_ERR_C(GROOVE_IVM_InvalidCollectBy, "parent projection fields must be grouping fields"); goto fail; }
        }
    }
    e = RP(validate_collect_by_key_types)(input_output, group.ptr, group.len); if (JZ_IS_ERR(e)) goto fail;
    e = RP(validate_collect_by_key_types)(input_output, occurrence.ptr, occurrence.len); if (JZ_IS_ERR(e)) goto fail;
    JZ_VEC_FOR(groove_TopByOrder, o, collect->order_cols) {
        u32 idx; e = RP(resolve_field_ref)(input_output, &o->field, &idx); if (JZ_IS_ERR(e)) goto fail;
        JZ_VEC_PUSH(order, idx);
    }
    e = resolve_refs(input_output, collect->tie_cols.ptr, collect->tie_cols.len, &tie); if (JZ_IS_ERR(e)) goto fail;
    if (order.len == 0 || tie.len == 0) { e = GROOVE_IVM_ERR_C(GROOVE_IVM_InvalidCollectBy, "order and tie fields must both be complete and non-empty"); goto fail; }
    e = RP(validate_collect_by_key_types)(input_output, order.ptr, order.len); if (JZ_IS_ERR(e)) goto fail;
    e = RP(validate_collect_by_key_types)(input_output, tie.ptr, tie.len); if (JZ_IS_ERR(e)) goto fail;
    e = names_at(input_output, group.ptr, group.len, &group_fields); if (JZ_IS_ERR(e)) goto fail;
    e = order_field_list(input_output, &order, collect->order_cols.ptr, collect->order_cols.len, &cb->order_fields); if (JZ_IS_ERR(e)) goto fail;
    groove_StrVec tie_names = {0};
    e = names_at(input_output, tie.ptr, tie.len, &tie_names); if (JZ_IS_ERR(e)) goto fail;
    VMOVE(cb->tie_fields, tie_names);
    jz_vec_extend(&cb->sort_field_indices, order.ptr, order.len, sizeof(u32));
    jz_vec_extend(&cb->sort_field_indices, tie.ptr, tie.len, sizeof(u32));
    JZ_VEC_FOR(groove_TopByOrder, o, collect->order_cols) { u8 d = (u8)o->direction; JZ_VEC_PUSH(cb->sort_directions, d); }
    for (u32 i = 0; i < tie.len; i++) { u8 d = groove_TopByDirection_Asc; JZ_VEC_PUSH(cb->sort_directions, d); }
    {
        u32 n = child_fields.len;
        jz_str *names = calloc(n ? n : 1, sizeof(jz_str));
        groove_ValueType *types = calloc(n ? n : 1, sizeof(groove_ValueType));
        const groove_DescriptorField *fields = FIELDS(input_output);
        for (u32 i = 0; i < n; i++) { names[i] = jz_str_clone(&child_fields.ptr[i].output_name); groove_ValueType_clone(&types[i], &fields[child_fields.ptr[i].field_idx].value_type); }
        cb->child_descriptor = groove__records__RecordDescriptor__new_named(names, types, n);
        for (u32 i = 0; i < n; i++) { jz_str_drop(&names[i]); groove_ValueType_drop(&types[i]); }
        free(names); free(types);
    }
    if (collect->mode == groove_CollectByMode_Expand && (tuple_fields.len == 0 || occurrence.len == 0)) {
        e = GROOVE_IVM_ERR_C(GROOVE_IVM_InvalidCollectBy, "expand mode requires a tuple projection and ordered occurrence-id fields"); goto fail;
    }
    groove_StrVec occurrence_names = {0};
    e = names_at(input_output, occurrence.ptr, occurrence.len, &occurrence_names); if (JZ_IS_ERR(e)) goto fail;
    cb->collection_field_index = parent_fields.len;
    groove_NodeId arrangement = add_arrangement_node(self, compiled_input.node, input_output, str_vec_clone(&group_fields), groove_ValueComparison_Exact);
    cb->mode = collect->mode;
    VMOVE(cb->group_fields, group_fields);
    VMOVE(cb->group_field_indices, group);
    VMOVE(cb->parent_fields, parent_fields);
    VMOVE(cb->child_fields, child_fields);
    cb->collection_field = jz_str_clone(&collect->collection_field);
    VMOVE(cb->tuple_fields, tuple_fields);
    VMOVE(cb->occurrence_id_fields, occurrence_names);
    VMOVE(cb->occurrence_id_field_indices, occurrence);
    cb->offset = collect->offset; cb->limit = collect->limit;
    u32_vec_drop(&order); u32_vec_drop(&tie);
    groove_OpType op = op_of(groove_Op_CollectBy);
    op.v.collect_by = cb;
    groove_NodeId node = dedup_records(self, op, &arrangement, 1, output);
    /* CollectBy changes representation, not the identity/order of public
     * roots selected upstream. */
    *out = compiled_inherit(output, node, &compiled_input);
    return JZ_OK;
fail:
    u32_vec_drop(&group); u32_vec_drop(&order); u32_vec_drop(&tie); u32_vec_drop(&occurrence);
    projection_vec_drop(&parent_fields); projection_vec_drop(&child_fields); projection_vec_drop(&tuple_fields);
    str_vec_drop(&group_fields);
    jz_vec_drop(&slots, sizeof(groove_CollectBySlot), NULL);
    groove_OpType dropped = op_of(groove_Op_CollectBy); dropped.v.collect_by = cb; groove_OpType_drop(&dropped);
    return e;
}

/* ---------- schema indices ---------- */
static jz_err resolve_columns(groove_RecordDescriptor d, const jz_str *cols, u32 n, groove_U32Vec *out) {
    groove_U32Vec v = {0};
    for (u32 i = 0; i < n; i++) {
        u32 idx;
        if (!RP(resolve_field_name)(d, cols[i].ptr, cols[i].len, &idx)) { u32_vec_drop(&v); return GROOVE_IVM_ERR_S(GROOVE_IVM_GraphFieldNotFound, &cols[i]); }
        JZ_VEC_PUSH(v, idx);
    }
    *out = v;
    return JZ_OK;
}
static bool index_has_column(const groove_IndexSchema *index, const jz_str *column) {
    JZ_VEC_FOR(jz_str, c, index->columns) if (jz_str_eq(c, column)) return true;
    return false;
}
/* Primary-key columns already in the index key are not repeated in the
 * durable entry's primary-key suffix (or unique value). */
static jz_err suffix_value_fields(const groove_TableSchema *table, const groove_IndexSchema *index, groove_RecordDescriptor d, groove_U32Vec *fields, groove_StrVec *names) {
    if (!table->has_primary_key) return GROOVE_IVM_ERR_S(GROOVE_IVM_MissingPrimaryKey, &table->name);
    groove_U32Vec v = {0}; groove_StrVec n = {0};
    JZ_VEC_FOR(groove_PrimaryKeyColumn, column, table->primary_key.columns) {
        if (index_has_column(index, &column->column)) continue;
        u32 idx;
        if (!RP(resolve_field_name)(d, column->column.ptr, column->column.len, &idx)) {
            u32_vec_drop(&v); str_vec_drop(&n);
            return GROOVE_IVM_ERR_S(GROOVE_IVM_GraphFieldNotFound, &column->column);
        }
        JZ_VEC_PUSH(v, idx);
        if (names) { jz_str c = jz_str_clone(&column->column); JZ_VEC_PUSH(n, c); }
    }
    *fields = v;
    if (names) *names = n;
    return JZ_OK;
}

static jz_err table_index_descriptor(const groove_TableSchema *table, const groove_IndexSchema *index, groove_RecordDescriptor *d, bool *has_vp, groove_VariantProjectionTarget *vp) {
    if (groove__schema__TableSchema__has_variants(table)) {
        JZ_TRY(RP(schema_index_input_descriptor)(table, index, d));
        *has_vp = true; vp->tag = groove_VPT_SchemaIndex; vp->name = jz_str_clone(&index->name);
    } else {
        *d = groove__schema__TableSchema__record_schema(table);
        *has_vp = false;
    }
    return JZ_OK;
}

static jz_err add_dedup_index_by_from_input(groove_IvmRuntime *self, const groove_TableSchema *table, const groove_IndexSchema *index, groove_NodeId input,
                                            groove_RecordDescriptor table_descriptor, const groove_StaticScanSpec *scan, groove_CompiledNode *out) {
    groove_RecordDescriptor index_descriptor = RP(index_record_descriptor)();
    groove_U32Vec key_fields, primary_key_fields; groove_StrVec suffix_columns;
    JZ_TRY(resolve_columns(table_descriptor, index->columns.ptr, index->columns.len, &key_fields));
    jz_err e = suffix_value_fields(table, index, table_descriptor, &primary_key_fields, &suffix_columns);
    if (JZ_IS_ERR(e)) { u32_vec_drop(&key_fields); return e; }
    bool covers = suffix_columns.len == 0;
    groove_OpType op = op_of(groove_Op_IndexBy);
    JZ_VEC_FOR(jz_str, column, index->columns) JZ_VEC_PUSH(op.v.index_by.key_expressions, plan_field(jz_str_clone(column)));
    JZ_VEC_FOR(jz_str, column, suffix_columns) JZ_VEC_PUSH(op.v.index_by.value_expressions, plan_field(*column));
    jz_vec_drop(&suffix_columns, sizeof(jz_str), NULL);
    op.v.index_by.has_explicit_index = true;
    groove_IndexSchema_clone(&op.v.index_by.explicit_index, index);
    VMOVE(op.v.index_by.key_fields, key_fields);
    VMOVE(op.v.index_by.value_fields, primary_key_fields);
    op.v.index_by.unique = index->unique || covers;
    op.v.index_by.append_value_to_key = !index->unique && !covers;
    op.v.index_by.store_value = index->unique && !covers;
    op.v.index_by.has_scan = scan != NULL;
    if (scan) groove_StaticScanSpec_clone(&op.v.index_by.scan, scan);
    groove_NodeId node = dedup_records(self, op, &input, 1, index_descriptor);
    *out = compiled_node(index_descriptor, node, false, 0);
    return JZ_OK;
}

jz_err CMP(add_dedup_schema_index)(groove_IvmRuntime *self, const groove_TableSchema *table, const groove_IndexSchema *index, groove_NodeId *out) {
    self->logical_nodes_requested += 3;
    groove_RecordDescriptor table_descriptor; bool has_vp; groove_VariantProjectionTarget vp;
    JZ_TRY(table_index_descriptor(table, index, &table_descriptor, &has_vp, &vp));
    groove_OpType src = op_of(groove_Op_TableSource);
    src.v.table_source.table = jz_str_clone(&table->name);
    src.v.table_source.has_scan = false;
    src.v.table_source.has_vp = has_vp;
    if (has_vp) src.v.table_source.variant_projection = vp;
    groove_NodeId input = dedup_records(self, src, NULL, 0, table_descriptor);

    groove_CompiledNode index_by;
    JZ_TRY(add_dedup_index_by_from_input(self, table, index, input, table_descriptor, NULL, &index_by));

    u32 index_id = groove__ivm__runtime__IvmRuntime__index_storage_id(self, table->name.ptr, table->name.len, index);
    groove_DurableStorage storage;
    storage.column_family = jz_str_from("indices");
    storage.key_prefix = groove__ivm__runtime__index_ids__durable_index_key_prefix(index_id);
    storage.name = (jz_str){0};
    jz_str_appendf(&storage.name, "%.*s.%.*s", (int)table->name.len, table->name.ptr, (int)index->name.len, index->name.ptr);
    groove_OpType p = op_of(groove_Op_Persist);
    p.v.persist.name = jz_str_clone(&index->name);
    groove_DurableStorage_clone(&p.v.persist.storage, &storage);
    u32 zero = 0; JZ_VEC_PUSH(p.v.persist.key_fields, zero);
    p.v.persist.unique = index->unique;
    groove_NodeDurability durable; memset(&durable, 0, sizeof durable);
    durable.tag = groove_NodeDurability_Durable; durable.storage = storage;
    groove_NodeId persist = dedup(self, p, &index_by.node, 1, groove_NodeOutput_Records, index_by.output, durable);
    groove_Retainer retainer; retainer.tag = groove_Retainer_DurableSchemaObject; retainer.name = (jz_str){0};
    jz_str_appendf(&retainer.name, "%.*s.%.*s", (int)table->name.len, table->name.ptr, (int)index->name.len, index->name.ptr);
    groove__ivm__runtime__graph_lifecycle__IvmRuntime__add_retainer(self, persist, retainer);
    INIT_NODE(self, persist);
    if (out) *out = persist;
    return JZ_OK;
}

static bool index_column_covered(const groove_TableSchema *table, const groove_IndexSchema *index, const jz_str *column) {
    if (index_has_column(index, column)) return true;
    if (!table->has_primary_key) return false;
    JZ_VEC_FOR(groove_PrimaryKeyColumn, part, table->primary_key.columns) if (jz_str_eq(&part->column, column)) return true;
    return false;
}

static jz_err index_source_op(groove_IvmRuntime *self, const groove_TableSchema *table, const groove_IndexSchema *index, const groove_GraphBuilder *g, groove_IndexSourceOp *out) {
    JZ_VEC_FOR(groove_IndexIntersection, intersection, g->intersections)
        if (!find_index(table, &intersection->index)) return GROOVE_IVM_ERR_S(GROOVE_IVM_IndexNotFound, &intersection->index);
    if (g->has_candidate_filter) {
        const groove_IndexCandidateFilter *filter = &g->candidate_filter;
        if (!g->has_row_projection || g->intersections.len > 0 || !g->has_scan || g->scan.tag != groove_Scan_Prefix || filter->scan.tag != groove_Scan_Prefix)
            return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedIndexCandidateFilter);
        const groove_TableSchema *candidate_table = groove__schema__DatabaseSchema__table(&self->schema, filter->table.ptr, filter->table.len);
        if (!candidate_table) return GROOVE_IVM_ERR_S(GROOVE_IVM_TableNotFound, &filter->table);
        const groove_IndexSchema *candidate_index = find_index(candidate_table, &filter->index);
        if (!candidate_index) return GROOVE_IVM_ERR_S(GROOVE_IVM_IndexNotFound, &filter->index);
        if (!index_column_covered(table, index, &filter->source_column)) return GROOVE_IVM_ERR_S(GROOVE_IVM_GraphFieldNotFound, &filter->source_column);
        if (!index_column_covered(candidate_table, candidate_index, &filter->candidate_column)) return GROOVE_IVM_ERR_S(GROOVE_IVM_GraphFieldNotFound, &filter->candidate_column);
    }
    groove_RecordDescriptor table_descriptor; bool has_vp; groove_VariantProjectionTarget vp;
    JZ_TRY(table_index_descriptor(table, index, &table_descriptor, &has_vp, &vp));
    groove_U32Vec key_fields, value_fields;
    JZ_TRY(resolve_columns(table_descriptor, index->columns.ptr, index->columns.len, &key_fields));
    jz_err e = suffix_value_fields(table, index, table_descriptor, &value_fields, NULL);
    if (JZ_IS_ERR(e)) { u32_vec_drop(&key_fields); return e; }
    bool covers = value_fields.len == 0;
    groove_IndexSourceDurableIds *ids = calloc(1, sizeof *ids);
    ids->index = groove__ivm__runtime__IvmRuntime__index_storage_id(self, table->name.ptr, table->name.len, index);
    JZ_VEC_FOR(groove_IndexIntersection, intersection, g->intersections) {
        const groove_IndexSchema *i = find_index(table, &intersection->index);
        if (!i) { free(ids); u32_vec_drop(&key_fields); u32_vec_drop(&value_fields); return GROOVE_IVM_ERR_S(GROOVE_IVM_IndexNotFound, &intersection->index); }
        u32 id = groove__ivm__runtime__IvmRuntime__index_storage_id(self, table->name.ptr, table->name.len, i);
        JZ_VEC_PUSH(ids->intersections, id);
    }
    if (g->has_candidate_filter) {
        const groove_IndexCandidateFilter *filter = &g->candidate_filter;
        const groove_TableSchema *ct = groove__schema__DatabaseSchema__table(&self->schema, filter->table.ptr, filter->table.len);
        const groove_IndexSchema *candidate = ct ? find_index(ct, &filter->index) : NULL;
        if (!candidate) { jz_vec_drop(&ids->intersections, sizeof(u32), NULL); free(ids); u32_vec_drop(&key_fields); u32_vec_drop(&value_fields); return GROOVE_IVM_ERR_S(GROOVE_IVM_IndexNotFound, &filter->index); }
        ids->has_candidate = true;
        ids->candidate = groove__ivm__runtime__IvmRuntime__index_storage_id(self, filter->table.ptr, filter->table.len, candidate);
    }
    memset(out, 0, sizeof *out);
    out->table = jz_str_clone(&table->name);
    out->index = jz_str_clone(&index->name);
    out->durable_ids = ids;
    JZ_VEC_FOR(groove_IndexIntersection, intersection, g->intersections) {
        groove_IndexIntersection c; c.index = jz_str_clone(&intersection->index); groove_StaticScanSpec_clone(&c.scan, &intersection->scan);
        JZ_VEC_PUSH(out->intersections, c);
    }
    out->has_candidate_filter = g->has_candidate_filter;
    if (g->has_candidate_filter) {
        const groove_IndexCandidateFilter *f = &g->candidate_filter;
        out->candidate_filter.table = jz_str_clone(&f->table);
        out->candidate_filter.index = jz_str_clone(&f->index);
        groove_StaticScanSpec_clone(&out->candidate_filter.scan, &f->scan);
        out->candidate_filter.source_column = jz_str_clone(&f->source_column);
        out->candidate_filter.candidate_column = jz_str_clone(&f->candidate_column);
    }
    out->input_descriptor = table_descriptor;
    out->has_vp = has_vp; if (has_vp) out->variant_projection = vp;
    out->has_row_projection = g->has_row_projection;
    if (g->has_row_projection) { out->row_projection.tag = groove_VPT_Named; out->row_projection.name = jz_str_clone(&g->row_projection); }
    VMOVE(out->key_fields, key_fields);
    VMOVE(out->value_fields, value_fields);
    out->unique = index->unique || covers;
    out->append_value_to_key = !index->unique && !covers;
    out->store_value = index->unique && !covers;
    out->has_scan = g->has_scan;
    if (g->has_scan) groove_StaticScanSpec_clone(&out->scan, &g->scan);
    return JZ_OK;
}
