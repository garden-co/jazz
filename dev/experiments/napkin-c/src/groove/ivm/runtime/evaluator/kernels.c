/* groove::ivm::runtime::evaluator::kernels — synchronous batch kernels.
 * Inputs have been resolved by the execution driver; no kernel asks the graph
 * to execute a predecessor. */
#include "jz_rt.h"
#include "groove/ivm/runtime/evaluator.h"
#include "groove/ivm/runtime/record_projection.h"

#define TE(name) groove__ivm__runtime__evaluator__TickEvaluator__##name

static int cmp_usize(const void *a, const void *b, void *ctx) { (void)ctx; usize x = *(const usize *)a, y = *(const usize *)b; return x < y ? -1 : x > y; }
static int cmp_usize2(const void *a, const void *b) { return cmp_usize(a, b, NULL); }
static void sort_dedup(groove_UsizeVec *v) {
    jz_sort_unstable(v->ptr, v->len, sizeof(usize), cmp_usize, NULL);
    jz_vec_dedup(v, sizeof(usize), cmp_usize2, NULL);
}

static jz_err first_input(groove_RecordDeltas *const *inputs, u32 n, groove_NodeId node, groove_RecordDeltas **out) {
    if (n == 0) return groove_err_input_missing(node);
    *out = jz_rc_clone(inputs[0]);
    return JZ_OK;
}

static jz_err arg_by_kernel(groove_TickEvaluator *e, groove_NodeId node, const groove_ArgByOp *op, u32 direction, groove_RecordDescriptor output_desc, groove_RecordDeltas *const *inputs, u32 n, groove_RecordDeltas *out) {
    groove_RecordDeltas *input = NULL, *materialized = NULL;
    JZ_TRY(first_input(inputs, n, node, &input));
    jz_err err = TE(materialize_indirect_field_indices)(e, input, op->comparison_field_indices.ptr, op->comparison_field_indices.len, &materialized);
    jz_rc_release(input);
    JZ_TRY(err);
    groove_ArgBySpec spec = { op->group_field_indices.ptr, op->group_field_indices.len, op->comparison_field_indices.ptr, op->comparison_field_indices.len, direction };
    err = TE(update_arg_by)(e, node, spec, output_desc, materialized, out);
    jz_rc_release(materialized);
    return err;
}

typedef jz_err (*ThresholdJoinFn)(groove_TickEvaluator *, groove_NodeId, const groove_JoinOp *, groove_RecordDescriptor, groove_NodeId, groove_NodeId, groove_RecordDeltas *, groove_RecordDeltas *, groove_RecordDeltas *);

static jz_err threshold_join_kernel(groove_TickEvaluator *e, groove_NodeId node, const groove_GraphNode *gn, const groove_JoinOp *join, groove_RecordDescriptor output_desc, groove_RecordDeltas *const *inputs, ThresholdJoinFn update, groove_RecordDeltas *out) {
    if (gn->descriptor.inputs.len != 2) return groove_err_arity(node);
    jz_btree left_fields, right_fields;
    groove__ivm__runtime__evaluator__plan_expr_fields(join->left_key.ptr, join->left_key.len, &left_fields);
    groove__ivm__runtime__evaluator__plan_expr_fields(join->right_key.ptr, join->right_key.len, &right_fields);
    groove_RecordDeltas *left = NULL, *right = NULL;
    jz_err err = TE(materialize_indirect_fields)(e, inputs[0], &left_fields, &left);
    if (!err.code) err = TE(materialize_indirect_fields)(e, inputs[1], &right_fields, &right);
    jz_btree_drop(&left_fields);
    jz_btree_drop(&right_fields);
    if (!err.code) err = update(e, node, join, output_desc, gn->descriptor.inputs.ptr[0], gn->descriptor.inputs.ptr[1], left, right, out);
    if (left) jz_rc_release(left);
    if (right) jz_rc_release(right);
    return err;
}

jz_err groove__ivm__runtime__evaluator__kernels__TickEvaluator__compute_batch(groove_TickEvaluator *e, groove_NodeId node, groove_RecordDeltas *const *inputs, u32 n, groove_RecordDeltas *out) {
    const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(e->graph, node);
    if (!gn) return groove_err_node_not_found(node);
    groove_RecordDescriptor output_desc = groove_NodeOutput_records(gn->descriptor.output);
    const groove_OpType *op = &gn->descriptor.op;
    bool hydrate_inputs = e->context.eval_mode == GROOVE_EVAL_HYDRATE && e->evaluation_inputs != NULL;
    groove_RecordDeltas *input = NULL, *materialized = NULL;
    jz_err err = JZ_OK;
    switch (op->tag) {
    case GROOVE_OP_TABLE_SOURCE:
        if (hydrate_inputs)
            return groove__ivm__runtime__operator_updates__NodeState__update_table_source_from_inputs(&op->v.table_source, e->schema, e->variant_projections, output_desc, e->evaluation_inputs, out);
        return groove__ivm__runtime__operator_updates__NodeState__update_table_source(&op->v.table_source, e->schema, e->variant_projections, output_desc, e->table_deltas, e->n_table_deltas, out);
    case GROOVE_OP_INDEX_SOURCE:
        if (hydrate_inputs)
            return groove__ivm__runtime__operator_updates__NodeState__update_index_source_from_inputs(&op->v.index_source, e->schema, e->variant_projections, output_desc, e->evaluation_inputs, out);
        return groove_err_unsupported_operator();
    case GROOVE_OP_INLINE_RECORDS:
        *out = groove_RecordDeltas_empty(output_desc);
        if (e->context.eval_mode == GROOVE_EVAL_HYDRATE) {
            for (u32 i = 0; i < op->v.inline_records.records.len; i++) {
                groove_RecordDelta d = { jz_bytes_clone(&op->v.inline_records.records.ptr[i]), 1 };
                jz_vec_push(&out->deltas, &d, sizeof(d));
            }
        }
        return JZ_OK;
    case GROOVE_OP_BINDING_SOURCE:
        return groove__ivm__runtime__operator_updates__NodeState__update_binding_source(&op->v.binding_source, output_desc, e->binding_deltas, e->n_binding_deltas, e->binding_snapshots, e->context.arrangement_update_mode, out);
    case GROOVE_OP_ARRANGE: {
        const groove_ArrangeOp *spec = &op->v.arrange;
        if (gn->descriptor.inputs.len != 1) return groove_err_arity(node);
        input = jz_rc_clone(inputs[0]);
        if (TE(arrangement_needs_index)(e, node)) {
            jz_btree fields; jz_btree_init(&fields, &groove_ivm_rt_a_desc_str_set);
            for (u32 i = 0; i < spec->fields.len; i++) {
                if (jz_btree_contains(&fields, &spec->fields.ptr[i])) continue;
                jz_str c = jz_str_clone(&spec->fields.ptr[i]);
                jz_btree_insert(&fields, &c, NULL, NULL);
            }
            err = TE(materialize_indirect_fields)(e, input, &fields, &materialized);
            jz_btree_drop(&fields);
            jz_rc_release(input);
            JZ_TRY(err);
            groove_ArrangementKey key;
            err = TE(arrangement_key)(e, node, output_desc, spec->fields.ptr, spec->fields.len, spec->comparison, &key);
            if (err.code) { jz_rc_release(materialized); return err; }
            groove_SubTick stamp = TE(arrangement_sub_tick)(e, &key);
            groove_AsOfArrangement state;
            groove_ArrangementKey rk;
            if (!jz_hmap_remove(e->arrangement_states, &key, &rk, &state)) {
                state.value = groove__ivm__runtime__join__ArrangementState__default();
                state.as_of.has = false;
            }
            groove_JoinInput snapshot = { materialized->deltas.ptr, materialized->deltas.len, materialized };
            err = groove__ivm__runtime__join__prepare_arrangement(&state, output_desc, spec->fields.ptr, spec->fields.len, spec->comparison, snapshot, stamp, e->context.arrangement_update_mode);
            if (err.code) { groove__ivm__runtime__join__ArrangementState__drop(state.value); jz_rc_release(materialized); return err; }
            TE(insert_arrangement)(e, key, state);
            groove__ivm__runtime__subscriptions__RecordDeltas__clone(out, materialized);
            jz_rc_release(materialized);
            return JZ_OK;
        }
        groove__ivm__runtime__subscriptions__RecordDeltas__clone(out, input);
        jz_rc_release(input);
        return JZ_OK;
    }
    case GROOVE_OP_FRONTIER_SOURCE:
        return TE(frontier_source)(e, &op->v.frontier_source, output_desc, out);
    case GROOVE_OP_FILTER:
        JZ_TRY(first_input(inputs, n, node, &input));
        err = TE(compute_filter)(e, node, &op->v.filter, output_desc, input, out);
        jz_rc_release(input);
        return err;
    case GROOVE_OP_MAP_PROJECT:
        JZ_TRY(first_input(inputs, n, node, &input));
        err = TE(compute_projection)(e, node, &op->v.map_project, output_desc, input, out);
        jz_rc_release(input);
        return err;
    case GROOVE_OP_UNWRAP_NULLABLE:
        JZ_TRY(first_input(inputs, n, node, &input));
        err = groove__ivm__runtime__operator_updates__NodeState__update_unwrap_nullable(&op->v.unwrap_nullable, output_desc, input, out);
        jz_rc_release(input);
        return err;
    case GROOVE_OP_UNNEST:
        JZ_TRY(first_input(inputs, n, node, &input));
        err = groove__ivm__runtime__operator_updates__NodeState__update_unnest(&op->v.unnest, output_desc, input, out);
        jz_rc_release(input);
        return err;
    case GROOVE_OP_VARIANT_PROJECT:
        JZ_TRY(first_input(inputs, n, node, &input));
        err = groove__ivm__runtime__operator_updates__NodeState__update_variant_project(&op->v.variant_project, output_desc, input, out);
        jz_rc_release(input);
        return err;
    case GROOVE_OP_ARG_MAX_BY:
        return arg_by_kernel(e, node, &op->v.arg_by, GROOVE_ARGBY_MAX, output_desc, inputs, n, out);
    case GROOVE_OP_ARG_MIN_BY:
        return arg_by_kernel(e, node, &op->v.arg_by, GROOVE_ARGBY_MIN, output_desc, inputs, n, out);
    case GROOVE_OP_TOP_BY: {
        const groove_TopByOp *top_by = &op->v.top_by;
        JZ_TRY(first_input(inputs, n, node, &input));
        groove_UsizeVec fields = {0};
        jz_vec_extend(&fields, top_by->group_field_indices.ptr, top_by->group_field_indices.len, sizeof(usize));
        jz_vec_extend(&fields, top_by->sort_field_indices.ptr, top_by->sort_field_indices.len, sizeof(usize));
        sort_dedup(&fields);
        err = TE(materialize_indirect_field_indices)(e, input, fields.ptr, fields.len, &materialized);
        jz_vec_drop(&fields, sizeof(usize), NULL);
        jz_rc_release(input);
        JZ_TRY(err);
        err = TE(update_top_by)(e, node, top_by, output_desc, materialized, out);
        jz_rc_release(materialized);
        return err;
    }
    case GROOVE_OP_COLLECT_BY: {
        const groove_CollectByOp *cb = op->v.collect_by;
        groove_RecordDeltas *canonical = NULL;
        JZ_TRY(first_input(inputs, n, node, &canonical));
        groove_UsizeVec read = {0};
        groove__ivm__op_types__CollectByOp__read_field_indices(cb, &read);
        err = TE(materialize_indirect_field_indices)(e, canonical, read.ptr, read.len, &materialized);
        jz_vec_drop(&read, sizeof(usize), NULL);
        if (!err.code) {
            err = TE(update_collect_by)(e, node, cb, output_desc, materialized, canonical, out);
            jz_rc_release(materialized);
        }
        jz_rc_release(canonical);
        return err;
    }
    case GROOVE_OP_AGGREGATE: {
        const groove_AggregateOp *agg = &op->v.aggregate;
        JZ_TRY(first_input(inputs, n, node, &input));
        groove_RecordDeltas *canonical = jz_rc_clone(input);
        /* COUNT(*) without grouping observes only row weights; retain indirect
         * columns and issue no chunk requests. */
        bool needs_values = agg->group_key.len != 0;
        for (u32 i = 0; !needs_values && i < agg->aggregates.len; i++) {
            const groove_AggregateExpr *x = &agg->aggregates.ptr[i];
            needs_values = x->function != GROOVE_AGG_COUNT || x->has_expression || x->distinct;
        }
        if (needs_values) {
            groove_UsizeVec fields = {0};
            jz_vec_extend(&fields, agg->group_field_indices.ptr, agg->group_field_indices.len, sizeof(usize));
            groove_PlanExprVec exprs = {0};
            for (u32 i = 0; i < agg->aggregates.len; i++)
                if (agg->aggregates.ptr[i].has_expression) jz_vec_push(&exprs, &agg->aggregates.ptr[i].expression, sizeof(groove_PlanExpr)); /* borrowed */
            jz_btree expr_fields;
            groove__ivm__runtime__evaluator__plan_expr_fields(exprs.ptr, exprs.len, &expr_fields);
            jz_vec_drop(&exprs, sizeof(groove_PlanExpr), NULL);
            jz_str *field; void *unused;
            JZ_BTREE_FOR(&expr_fields, field, unused) {
                usize idx;
                if (!groove__ivm__runtime__record_projection__resolve_field_name(input->descriptor, field->ptr, field->len, &idx)) {
                    err = groove_err_field_not_found(field->ptr, field->len);
                    break;
                }
                jz_vec_push(&fields, &idx, sizeof(idx));
            }
            jz_btree_drop(&expr_fields);
            if (!err.code) {
                sort_dedup(&fields);
                err = TE(materialize_indirect_field_indices)(e, input, fields.ptr, fields.len, &materialized);
            }
            jz_vec_drop(&fields, sizeof(usize), NULL);
            jz_rc_release(input);
            if (err.code) { jz_rc_release(canonical); return err; }
            input = materialized;
        }
        err = TE(update_aggregate)(e, node, agg, output_desc, input, canonical, out);
        jz_rc_release(input);
        jz_rc_release(canonical);
        return err;
    }
    case GROOVE_OP_INDEX_BY: {
        const groove_IndexByOp *index_by = &op->v.index_by;
        JZ_TRY(first_input(inputs, n, node, &input));
        groove_UsizeVec fields = {0};
        jz_vec_extend(&fields, index_by->key_fields.ptr, index_by->key_fields.len, sizeof(usize));
        if (index_by->append_value_to_key) jz_vec_extend(&fields, index_by->value_fields.ptr, index_by->value_fields.len, sizeof(usize));
        sort_dedup(&fields);
        err = TE(materialize_indirect_field_indices)(e, input, fields.ptr, fields.len, &materialized);
        jz_vec_drop(&fields, sizeof(usize), NULL);
        jz_rc_release(input);
        JZ_TRY(err);
        /* GROOVE_TRACE_INDEX_BY (std::env) is never set on wasm32-unknown-unknown:
         * the stderr trace is omitted in this port. */
        err = groove__ivm__runtime__operator_updates__NodeState__update_index_by(index_by, output_desc, materialized, out);
        jz_rc_release(materialized);
        return err;
    }
    case GROOVE_OP_UNION:
        return groove__ivm__runtime__operator_updates__NodeState__update_union(output_desc, (groove_RecordDeltas **)inputs, n, out);
    case GROOVE_OP_JOIN:
        if (gn->descriptor.inputs.len != 2) return groove_err_arity(node);
        /* Arrange already materialized join keys. */
        return TE(update_join)(e, node, &op->v.join, output_desc, gn->descriptor.inputs.ptr[0], gn->descriptor.inputs.ptr[1], inputs[0], inputs[1], out);
    case GROOVE_OP_SEMI_JOIN:
        return threshold_join_kernel(e, node, gn, &op->v.join, output_desc, inputs, TE(update_semi_join), out);
    case GROOVE_OP_ANTI_JOIN:
        return threshold_join_kernel(e, node, gn, &op->v.join, output_desc, inputs, TE(update_anti_join), out);
    case GROOVE_OP_LOOKUP:
        JZ_TRY(first_input(inputs, n, node, &input));
        err = groove__ivm__runtime__evaluator__lookup__TickEvaluator__update_lookup(e, node, &op->v.lookup, output_desc, input, out);
        jz_rc_release(input);
        return err;
    case GROOVE_OP_RECURSIVE_STEP_WITNESS:
        if (gn->descriptor.inputs.len != 1) return groove_err_arity(node);
        /* Drive the owner first; its side state is the only source here. */
        return TE(update_recursive_step_witness)(e, gn->descriptor.inputs.ptr[0], output_desc, out);
    case GROOVE_OP_PERSIST:
        /* Durable writes are driven outside this frame by tick_durable_nodes. */
        return groove_err_unsupported_operator();
    default:
        return groove_err_unsupported_operator();
    }
}
