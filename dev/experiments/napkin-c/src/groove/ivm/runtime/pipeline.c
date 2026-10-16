/* groove::ivm::runtime::pipeline: physical unary tasks. Only private
 * stateless edges lose their batch boundary. */
#include "mod.h"

bool groove__ivm__runtime__pipeline__supports_node(const groove_IvmGraph *graph, groove_NodeId id) {
    const groove_GraphNode *node = groove__ivm__graph__IvmGraph__node(graph, id);
    if (!node) return false;
    if (node->descriptor.inputs.len != 1) return false;
    groove_NodeId input = node->descriptor.inputs.ptr[0];
    switch (node->descriptor.operator_.tag) {
    case groove_Op_MapProject: return true;
    case groove_Op_Filter: {
        const groove_GraphNode *in = groove__ivm__graph__IvmGraph__node(graph, input);
        if (!in) return false;
        groove_RecordDescriptor descriptor = in->descriptor.output.records;
        jz_btree fields; jz_btree_init(&fields, &groove_ivm_rt_STR_SET);
        groove__ivm__runtime__subscriptions__PredicateExpr__referenced_fields(&node->descriptor.operator_.v.filter.predicate, &fields);
        bool all = true;
        jz_str *field; void *unit;
        JZ_BTREE_FOR(&fields, field, unit) {
            u32 index;
            if (!groove__ivm__runtime__record_projection__resolve_field_name(descriptor, field->ptr, field->len, &index) ||
                groove__records__ValueType__may_contain_stored_scalar(&groove__records__RecordDescriptor__field(descriptor, index)->value_type)) {
                all = false;
                break;
            }
        }
        jz_btree_drop(&fields);
        return all;
    }
    default: return false;
    }
}

typedef enum { Stage_VirtualProject, Stage_RoutedFilter, Stage_Materialize, Stage_Filter, Stage_Project } StageTag;
typedef struct Stage {
    u8 tag;
    groove_FilterOp filter;          /* RoutedFilter / Filter */
    groove_FieldRoutes routes;       /* RoutedFilter / Materialize */
    groove_RecordDescriptor descriptor; /* Filter */
    groove_MapProjectOp project; groove_RecordDescriptor input, output;
    groove_PreparedProjection *prepared; bool omit_unrepresentable; /* Project */
} Stage;

struct groove_PreparedPipeline {
    groove_NodeId *nodes; u32 nodes_len;
    groove_NodeId input;
    groove_RecordDescriptor output;
    JZ_VEC(Stage) stages;
};

typedef struct Span { u32 start, end; i64 weight; } Span;
struct groove_PendingPipeline {
    groove_PreparedPipeline *plan;
    groove_NodeMemoLookup lookup;
    groove_RecordDeltas *input;
    u32 next, stage, location;
    jz_bytes scratch[2];
    jz_bytes output;
    JZ_VEC(Span) spans;
    groove_RecordDeltaVec borrowed;
    bool has_error; u32 error_stage; jz_err error;
};

static void stage_drop(void *p) {
    Stage *s = p;
    switch (s->tag) {
    case Stage_RoutedFilter: groove_PredicateExpr_drop(&s->filter.predicate); groove_FieldRoutes_drop(&s->routes); break;
    case Stage_Materialize: groove_FieldRoutes_drop(&s->routes); break;
    case Stage_Filter: groove_PredicateExpr_drop(&s->filter.predicate); break;
    case Stage_Project: groove_MapProjectOp_drop(&s->project); if (s->prepared) jz_rc_release(s->prepared); break;
    default: break;
    }
}
static void prepared_pipeline_drop(void *p) {
    groove_PreparedPipeline *pp = p;
    jz_free(pp->nodes);
    jz_vec_drop(&pp->stages, sizeof(Stage), stage_drop);
}

void groove_PendingPipeline_drop_v(void *p) {
    groove_PendingPipeline *pp = p;
    jz_rc_release(pp->plan);
    jz_rc_release(pp->input);
    jz_bytes_drop(&pp->scratch[0]); jz_bytes_drop(&pp->scratch[1]); jz_bytes_drop(&pp->output);
    jz_vec_drop(&pp->spans, sizeof(Span), NULL);
    jz_vec_drop(&pp->borrowed, sizeof(groove_RecordDelta), groove_RecordDelta_drop);
    if (pp->has_error) jz_err_drop(&pp->error);
}
const jz_hmap_desc groove_ivm_rt_PENDING_PIPELINE_MAP = { sizeof(groove_NodeId), sizeof(groove_PendingPipeline), groove_NodeId_hash, groove_NodeId_eq, NULL, groove_PendingPipeline_drop_v };

static jz_slice location_raw(const groove_PendingPipeline *self, const groove_RecordDelta *delta, u32 location) {
    switch (location) {
    case 0: return jz_bytes_slice(&delta->record);
    case 1: return jz_bytes_slice(&self->scratch[0]);
    default: return jz_bytes_slice(&self->scratch[1]);
    }
}

static void set_error(groove_PendingPipeline *self, u32 stage_index, jz_err e) {
    /* Earlier stages outrank later ones, regardless of row. */
    if (self->has_error) jz_err_drop(&self->error);
    self->has_error = true;
    self->error_stage = stage_index;
    self->error = e;
    self->output.len = 0;
    self->spans.len = 0;
    jz_vec_truncate(&self->borrowed, 0, sizeof(groove_RecordDelta), groove_RecordDelta_drop);
}

/* Returns false when the budget ran out mid-row. */
static bool pending_row(groove_PendingPipeline *self, u32 index, u32 *budget) {
    const groove_RecordDelta *delta = &self->input->deltas.ptr[index];
    /* 0 = source, 1/2 = reusable scratch. Byte-preserving projections keep the
     * current location; only the descriptor changes in the next stage. */
    u32 location = self->location;
    const groove_PreparedPipeline *plan = self->plan;
    for (u32 stage_index = self->stage; stage_index < plan->stages.len; stage_index++) {
        const Stage *stage = &plan->stages.ptr[stage_index];
        if (self->has_error && stage_index >= self->error_stage) return true;
        if (*budget == 0) { self->location = location; return false; }
        *budget -= 1;
        self->stage = stage_index + 1;
        jz_slice raw = location_raw(self, delta, location);
        bool last = stage_index + 1 == plan->stages.len;
        jz_err e = JZ_OK;
        bool keep = true;
        switch (stage->tag) {
        case Stage_VirtualProject: continue;
        case Stage_RoutedFilter: {
            groove_RoutedRecord rr = groove__ivm__runtime__pipeline__fields__FieldRoutes__record(&stage->routes, raw);
            groove_PredicateRecord pr = { &groove__ivm__runtime__pipeline__fields__RoutedRecord__PredicateRecord__VT, &rr };
            e = groove__ivm__runtime__subscriptions__PredicateExpr__matches(&stage->filter.predicate, pr, stage->filter.comparison, &keep);
            break;
        }
        case Stage_Materialize: {
            if (stage->routes.reuses_input) continue;
            if (last) {
                u32 s, en;
                e = groove__ivm__runtime__pipeline__fields__FieldRoutes__append(&stage->routes, raw, &self->output, &s, &en);
                if (!JZ_IS_ERR(e)) { Span sp = { s, en, delta->weight }; JZ_VEC_PUSH(self->spans, sp); return true; }
            } else {
                jz_bytes *destination = location == 1 ? &self->scratch[1] : &self->scratch[0];
                jz_bytes tmp = {0};
                if (location != 0) tmp = jz_bytes_from(raw.ptr, raw.len), raw = jz_bytes_slice(&tmp);
                destination->len = 0;
                u32 s, en;
                e = groove__ivm__runtime__pipeline__fields__FieldRoutes__append(&stage->routes, raw, destination, &s, &en);
                jz_bytes_drop(&tmp);
                location = location == 1 ? 2 : 1;
            }
            break;
        }
        case Stage_Filter: {
            groove_BorrowedRecord b = groove__records__BorrowedRecord__new(raw, stage->descriptor);
            groove_PredicateRecord pr = { &groove__ivm__runtime__key_encoding__BorrowedRecord__PredicateRecord__VT, &b };
            e = groove__ivm__runtime__subscriptions__PredicateExpr__matches(&stage->filter.predicate, pr, stage->filter.comparison, &keep);
            break;
        }
        case Stage_Project: {
            if (stage->prepared && stage->prepared->reuses_input) continue;
            bool some; u32 s, en;
            if (last) {
                u32 start = self->output.len;
                e = groove__ivm__runtime__operator_updates__NodeState__project_row_into(&stage->project, stage->output, stage->input, raw,
                    stage->prepared, stage->omit_unrepresentable, &self->output, &some, &s, &en);
                if (!JZ_IS_ERR(e)) {
                    if (some) { Span sp = { s, en, delta->weight }; JZ_VEC_PUSH(self->spans, sp); }
                    else self->output.len = start;
                    return true;
                }
            } else {
                jz_bytes *destination = location == 1 ? &self->scratch[1] : &self->scratch[0];
                jz_bytes tmp = {0};
                if (location != 0) tmp = jz_bytes_from(raw.ptr, raw.len), raw = jz_bytes_slice(&tmp);
                destination->len = 0;
                e = groove__ivm__runtime__operator_updates__NodeState__project_row_into(&stage->project, stage->output, stage->input, raw,
                    stage->prepared, stage->omit_unrepresentable, destination, &some, &s, &en);
                jz_bytes_drop(&tmp);
                location = location == 1 ? 2 : 1;
                keep = some;
            }
            break;
        }
        }
        if (JZ_IS_ERR(e)) { set_error(self, stage_index, e); return true; }
        if (!keep) return true;
    }
    if (location == 0) {
        groove_RecordDelta copy;
        groove_RecordDelta_clone(&copy, delta);
        JZ_VEC_PUSH(self->borrowed, copy);
        return true;
    }
    jz_slice raw = location_raw(self, delta, location);
    u32 start = self->output.len;
    jz_bytes_push(&self->output, raw.ptr, raw.len);
    Span sp = { start, self->output.len, delta->weight };
    JZ_VEC_PUSH(self->spans, sp);
    return true;
}

static jz_err pending_finish(groove_PendingPipeline *self, groove_NodeMemoLookup *lookup, groove_RecordDeltas *out) {
    if (self->has_error) {
        jz_err e = self->error;
        self->has_error = false;
        groove_PendingPipeline_drop_v(self);
        return e;
    }
    jz_bytes bytes = groove__ivm__runtime__freeze_batch_buffer(self->output);
    self->output = (jz_bytes){0};
    out->descriptor = self->plan->output;
    if (self->spans.len == 0) {
        out->deltas = self->borrowed;
        self->borrowed = (groove_RecordDeltaVec){0};
    } else {
        memset(&out->deltas, 0, sizeof(out->deltas));
        jz_vec_reserve(&out->deltas, self->spans.len, sizeof(groove_RecordDelta));
        for (u32 i = 0; i < self->spans.len; i++) {
            Span *sp = &self->spans.ptr[i];
            groove_RecordDelta d = { jz_bytes_from(bytes.ptr + sp->start, sp->end - sp->start), sp->weight };
            JZ_VEC_PUSH(out->deltas, d);
        }
    }
    jz_bytes_drop(&bytes);
    *lookup = self->lookup;
    groove_PendingPipeline_drop_v(self);
    return JZ_OK;
}

static bool expression_omits(const groove_MapProjectOp *project) {
    for (u32 i = 0; i < project->expressions.len; i++) {
        const groove_ProjectExpr *e = &project->expressions.ptr[i].expression;
        if (e->tag == groove_ProjectExpr_RecursiveEnumRemap && e->omit_unrepresentable) return true;
    }
    return false;
}

static groove_PreparedPipeline *prepared_pipeline(groove_TickEvaluator *te, const groove_NodeId *nodes, u32 n) {
    if (n == 0) return NULL;
    groove_NodeId tail = nodes[n - 1];
    groove_NodeRuntimeMeta *meta = jz_hmap_get(te->node_meta, &tail);
    if (meta && meta->pipeline && meta->pipeline->nodes_len == n && memcmp(meta->pipeline->nodes, nodes, n * sizeof(groove_NodeId)) == 0)
        return jz_rc_clone(meta->pipeline);
    const groove_GraphNode *first = groove__ivm__graph__IvmGraph__node(te->graph, nodes[0]);
    if (!first) return NULL;
    groove_NodeId input = first->descriptor.inputs.ptr[0];
    const groove_GraphNode *in = groove__ivm__graph__IvmGraph__node(te->graph, input);
    if (!in) return NULL;
    JZ_VEC(Stage) stages = {0};
    jz_vec_reserve(&stages, n, sizeof(Stage));
    groove_RecordDescriptor descriptor = in->descriptor.output.records;
    groove_FieldRoutes routes;
    groove__ivm__runtime__pipeline__fields__FieldRoutes__identity(descriptor, &routes);
    bool virtual_rows = false;
    for (u32 i = 0; i < n; i++) {
        groove_NodeId node = nodes[i];
        const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(te->graph, node);
        if (!gn) goto fail;
        groove_RecordDescriptor output = gn->descriptor.output.records;
        Stage stage;
        memset(&stage, 0, sizeof(stage));
        const groove_OpType *op = &gn->descriptor.operator_;
        if (op->tag == groove_Op_Filter && virtual_rows) {
            stage.tag = Stage_RoutedFilter;
            groove_PredicateExpr_clone(&stage.filter.predicate, &op->v.filter.predicate);
            stage.filter.comparison = op->v.filter.comparison;
            groove_FieldRoutes_clone(&stage.routes, &routes);
        } else if (op->tag == groove_Op_Filter) {
            stage.tag = Stage_Filter;
            groove_PredicateExpr_clone(&stage.filter.predicate, &op->v.filter.predicate);
            stage.filter.comparison = op->v.filter.comparison;
            stage.descriptor = descriptor;
        } else if (op->tag == groove_Op_MapProject) {
            const groove_MapProjectOp *project = &op->v.map_project;
            groove_PreparedProjection *prepared = NULL;
            jz_err e = groove__ivm__runtime__evaluator__TickEvaluator__raw_projection_fields(te, node, project, descriptor, output, &prepared);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); goto fail; }
            groove_FieldRoutes composed;
            if (prepared && groove__ivm__runtime__pipeline__fields__FieldRoutes__compose(&routes, output, prepared, &composed)) {
                groove_FieldRoutes_drop(&routes);
                routes = composed;
                virtual_rows = true;
                descriptor = output;
                jz_rc_release(prepared);
                Stage vp; memset(&vp, 0, sizeof(vp)); vp.tag = Stage_VirtualProject;
                JZ_VEC_PUSH(stages, vp);
                continue;
            }
            if (virtual_rows) {
                Stage m; memset(&m, 0, sizeof(m)); m.tag = Stage_Materialize; m.routes = routes;
                JZ_VEC_PUSH(stages, m);
            } else {
                groove_FieldRoutes_drop(&routes);
            }
            groove__ivm__runtime__pipeline__fields__FieldRoutes__identity(output, &routes);
            virtual_rows = false;
            stage.tag = Stage_Project;
            groove_MapProjectOp_clone(&stage.project, project);
            stage.input = descriptor;
            stage.output = output;
            /* If preparation itself fails, run the ordinary evaluator to
             * preserve empty-input and upstream error precedence. */
            stage.prepared = prepared;
            stage.omit_unrepresentable = expression_omits(project);
        } else {
            goto fail;
        }
        JZ_VEC_PUSH(stages, stage);
        descriptor = output;
    }
    if (virtual_rows) {
        Stage m; memset(&m, 0, sizeof(m)); m.tag = Stage_Materialize; m.routes = routes;
        JZ_VEC_PUSH(stages, m);
    } else {
        groove_FieldRoutes_drop(&routes);
    }
    groove_PreparedPipeline *plan = jz_rc_new(sizeof(groove_PreparedPipeline), prepared_pipeline_drop);
    plan->nodes = jz_memdup(nodes, n * sizeof(groove_NodeId));
    plan->nodes_len = n;
    plan->input = input;
    plan->output = descriptor;
    plan->stages.ptr = stages.ptr; plan->stages.len = stages.len; plan->stages.cap = stages.cap;
    groove_NodeRuntimeMeta *m = groove_ivm_rt_node_meta_entry(te->node_meta, tail);
    if (m->pipeline) jz_rc_release(m->pipeline);
    m->pipeline = jz_rc_clone(plan);
    return plan;
fail:
    groove_FieldRoutes_drop(&routes);
    jz_vec_drop(&stages, sizeof(Stage), stage_drop);
    return NULL;
}

bool groove__ivm__runtime__pipeline__TickEvaluator__poll_pipeline(groove_TickEvaluator *te, const groove_NodeId *nodes, u32 nnodes, jz_hmap *pending,
    groove_FrameInputs frame_inputs, jz_err *err, groove_RecordDeltas **out) {
    if (nnodes == 0) jz_panic("nonempty pipeline");
    groove_NodeId tail = nodes[nnodes - 1];
    *err = JZ_OK;
    if (!jz_hmap_get(pending, &tail)) {
        groove_NodeRuntimeMeta *meta = jz_hmap_get(te->node_meta, &tail);
        if (!meta || !meta->input_signature) {
            /* Contracting the queue must not turn metadata preparation back
             * into recursive traversal of an arbitrarily deep chain. */
            for (u32 i = 0; i < nnodes; i++) {
                groove_NodeInputSignature *sig;
                jz_err e = groove__ivm__runtime__evaluator__TickEvaluator__input_signature(te, nodes[i], &sig);
                if (JZ_IS_ERR(e)) { *err = e; return true; }
                jz_rc_release(sig);
            }
        }
        groove_NodeMemoLookup lookup;
        jz_err e = groove__ivm__runtime__evaluator__TickEvaluator__prepare_memo_lookup(te, tail, &lookup);
        if (JZ_IS_ERR(e)) { *err = e; return true; }
        groove_RecordDeltas *records = NULL;
        e = groove__ivm__runtime__evaluator__TickEvaluator__cached_node_records(te, &lookup, &records);
        if (JZ_IS_ERR(e)) { *err = e; return true; }
        if (records) { *out = records; return true; }
        if (te->context.sub_tick > 1 && !lookup.depends_on_context)
            return groove__ivm__runtime__evaluator__TickEvaluator__compute_node_poll(te, tail, lookup, err, out);
        groove_PreparedPipeline *plan = prepared_pipeline(te, nodes, nnodes);
        if (!plan) return groove__ivm__runtime__evaluator__TickEvaluator__compute_node_poll(te, tail, lookup, err, out);
        bool some = false;
        JZ_VEC(groove_RecordDeltas *) inputs = {0};
        e = groove__ivm__runtime__evaluator__TickEvaluator__resolve_register_inputs(te, frame_inputs, &some, (void *)&inputs);
        if (JZ_IS_ERR(e)) { jz_rc_release(plan); *err = e; return true; }
        groove_RecordDeltas *input = NULL;
        if (some && inputs.len) input = inputs.ptr[--inputs.len];
        for (u32 i = 0; i < inputs.len; i++) jz_rc_release(inputs.ptr[i]);
        jz_vec_drop(&inputs, sizeof(void *), NULL);
        if (!input) {
            groove_NodeMemoLookup key;
            e = groove__ivm__runtime__evaluator__TickEvaluator__prepare_memo_lookup(te, plan->input, &key);
            if (!JZ_IS_ERR(e)) e = groove__ivm__runtime__evaluator__TickEvaluator__cached_node_records(te, &key, &input);
            if (JZ_IS_ERR(e)) { jz_rc_release(plan); *err = e; return true; }
            if (!input) { jz_rc_release(plan); return groove__ivm__runtime__evaluator__TickEvaluator__compute_node_poll(te, tail, lookup, err, out); }
        }
        if (te->context.eval_mode == groove_EvalMode_Hydrate) {
            te->metrics->hydration_memo_computes += 1;
            jz_hmap_insert(&te->metrics->hydration_memo_computed_nodes, &tail, NULL, NULL);
        }
        groove_PendingPipeline pp;
        memset(&pp, 0, sizeof(pp));
        pp.plan = plan; pp.lookup = lookup; pp.input = input;
        jz_hmap_insert(pending, &tail, &pp, NULL);
    }
    groove_PendingPipeline *batch = jz_hmap_get(pending, &tail);
    if (!batch) jz_panic("prepared pipeline");
    u32 budget = 256;
    while (batch->next < batch->input->deltas.len && budget > 0) {
        if (!pending_row(batch, batch->next, &budget)) break;
        batch->next += 1;
        batch->stage = 0;
        batch->location = 0;
        if (batch->has_error && batch->error_stage == 0) break;
    }
    if (batch->next < batch->input->deltas.len && (!batch->has_error || batch->error_stage != 0)) return false;
    groove_PendingPipeline done;
    if (!jz_hmap_remove(pending, &tail, NULL, &done)) jz_panic("completed pipeline");
    groove_NodeMemoLookup lookup;
    groove_RecordDeltas output;
    jz_err e = pending_finish(&done, &lookup, &output);
    if (JZ_IS_ERR(e)) { *err = e; return true; }
    *out = groove__ivm__runtime__evaluator__TickEvaluator__memoize_result(te, &lookup, output);
    return true;
}
