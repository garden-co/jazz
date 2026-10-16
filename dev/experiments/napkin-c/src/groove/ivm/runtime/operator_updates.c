/* groove::ivm::runtime::operator_updates: stateless source and unary-operator
 * delta application. */
#include "mod.h"

/* ---------- NodeRuntimeMeta ---------- */
static const jz_hmap_desc RETAINER_SET = { sizeof(groove_Retainer), 0, groove_Retainer_hash, groove_Retainer_eq, groove_Retainer_drop_v, NULL };

void groove__ivm__runtime__operator_updates__NodeRuntimeMeta__default(groove_NodeRuntimeMeta *m) {
    memset(m, 0, sizeof *m);
    jz_hmap_init(&m->retainers, &RETAINER_SET);
    m->depends_on_context = -1;
    m->has_hydration_state_ancestor = -1;
}

static void opt_rc_release(void *p) { if (p) jz_rc_release(p); }
static void *opt_rc_clone(void *p) { return p ? jz_rc_clone(p) : NULL; }

void groove_NodeRuntimeMeta_drop(void *p) {
    groove_NodeRuntimeMeta *m = p;
    jz_hmap_drop(&m->retainers);
    opt_rc_release(m->readiness_frontier);
    opt_rc_release(m->terminal_lineage);
    opt_rc_release(m->input_signature);
    if (m->has_raw_projection_fields) opt_rc_release(m->raw_projection_fields);
    opt_rc_release(m->pipeline);
    opt_rc_release(m->join_left_fields);
    opt_rc_release(m->join_right_fields);
    opt_rc_release(m->join_output);
    opt_rc_release(m->aggregate_group_fields);
}

void groove_NodeRuntimeMeta_clone(void *dst, const void *src) {
    groove_NodeRuntimeMeta *d = dst; const groove_NodeRuntimeMeta *s = src;
    *d = *s;
    jz_hmap_init(&d->retainers, &RETAINER_SET);
    groove_Retainer *r; void *unused;
    JZ_HMAP_FOR(&s->retainers, r, unused) {
        groove_Retainer c; groove_Retainer_clone(&c, r);
        jz_hmap_insert(&d->retainers, &c, NULL, NULL);
    }
    d->readiness_frontier = opt_rc_clone(s->readiness_frontier);
    d->terminal_lineage = opt_rc_clone(s->terminal_lineage);
    d->input_signature = opt_rc_clone(s->input_signature);
    d->raw_projection_fields = s->has_raw_projection_fields ? opt_rc_clone(s->raw_projection_fields) : NULL;
    d->pipeline = opt_rc_clone(s->pipeline);
    d->join_left_fields = opt_rc_clone(s->join_left_fields);
    d->join_right_fields = opt_rc_clone(s->join_right_fields);
    d->join_output = opt_rc_clone(s->join_output);
    d->aggregate_group_fields = opt_rc_clone(s->aggregate_group_fields);
}

const jz_hmap_desc groove_ivm_rt_NODE_META_MAP = { sizeof(groove_NodeId), sizeof(groove_NodeRuntimeMeta), groove_NodeId_hash, groove_NodeId_eq, NULL, groove_NodeRuntimeMeta_drop };

groove_NodeRuntimeMeta *groove_ivm_rt_node_meta_entry(jz_hmap *node_meta, groove_NodeId node) {
    bool inserted;
    groove_NodeRuntimeMeta *m = jz_hmap_entry(node_meta, &node, &inserted);
    if (inserted) groove__ivm__runtime__operator_updates__NodeRuntimeMeta__default(m);
    return m;
}

/* ---------- helpers ---------- */
static void deltas_init(groove_RecordDeltas *out, groove_RecordDescriptor d) {
    out->descriptor = d;
    out->deltas.ptr = NULL; out->deltas.len = 0; out->deltas.cap = 0;
}
static void push_delta_clone(groove_RecordDeltaVec *v, const groove_RecordDelta *d) {
    groove_RecordDelta c; groove_RecordDelta_clone(&c, d);
    JZ_VEC_PUSH(*v, c);
}
static void table_delta_drop(void *p) {
    groove_TableDelta *t = p;
    jz_str_drop(&t->table);
    jz_vec_drop(&t->deltas, sizeof(groove_RecordDelta), groove_RecordDelta_drop);
}
static jz_str target_name(const groove_VariantProjectionTarget *t) {
    return t ? jz_str_clone(groove__ivm__runtime__record_projection__variant_projection_target_name(t)) : jz_str_from("");
}

/* ---------- NodeState: storage requests ---------- */
jz_err groove__ivm__runtime__operator_updates__NodeState__table_source_request(const groove_TableSourceOp *input, bool *some, groove_StorageRequestKey *out) {
    memset(out, 0, sizeof *out);
    *some = true;
    const groove_StaticScanSpec *scan = input->has_scan ? &input->scan : NULL;
    if (!scan) {
        out->tag = groove_SRK_ScanPrefix;
        out->family = jz_str_clone(&input->table);
        return JZ_OK;
    }
    groove_StaticScanBounds bounds;
    JZ_TRY(groove__ivm__runtime__record_projection__scan_bounds(scan, &bounds));
    if (bounds.tag == groove_StaticScanBounds_Prefix) {
        usize max_items;
        if (groove__ivm__runtime__record_projection__scan_max_items(scan, &max_items)) {
            out->tag = groove_SRK_ScanPrefixLimit;
            out->family = jz_str_clone(&input->table);
            out->a = bounds.a;
            out->max_items = (u32)max_items;
            out->reversed = groove__ivm__runtime__record_projection__scan_reversed(scan);
        } else {
            out->tag = groove_SRK_ScanPrefix;
            out->family = jz_str_clone(&input->table);
            out->a = bounds.a;
        }
        jz_bytes_drop(&bounds.b);
        return JZ_OK;
    }
    if (jz_bytes_cmp(&bounds.a, &bounds.b) < 0) {
        out->tag = groove_SRK_ScanRange;
        out->family = jz_str_clone(&input->table);
        out->a = bounds.a;
        out->b = bounds.b;
        return JZ_OK;
    }
    groove__ivm__runtime__record_projection__StaticScanBounds__drop(&bounds);
    *some = false;
    return JZ_OK;
}

static jz_err prefix_bounds(u32 index_id, const groove_StaticScanSpec *scan, u32 err_code, jz_bytes *prefix) {
    groove_StaticScanBounds b;
    JZ_TRY(groove__ivm__runtime__record_projection__persisted_index_scan_bounds(index_id, scan, &b));
    if (b.tag != groove_StaticScanBounds_Prefix) {
        groove__ivm__runtime__record_projection__StaticScanBounds__drop(&b);
        return GROOVE_IVM_ERR(err_code);
    }
    jz_bytes_drop(&b.b);
    *prefix = b.a;
    return JZ_OK;
}

jz_err groove__ivm__runtime__operator_updates__NodeState__index_source_request(const groove_IndexSourceOp *input, bool *some, groove_StorageRequestKey *out) {
    memset(out, 0, sizeof *out);
    *some = true;
    const groove_StaticScanSpec *scan = input->has_scan ? &input->scan : NULL;
    if (input->has_candidate_filter && !input->has_row_projection)
        return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedIndexCandidateFilter);
    if (input->has_row_projection) {
        if (input->has_candidate_filter) {
            const groove_IndexCandidateFilter *filter = &input->candidate_filter;
            if (input->intersections.len != 0) return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedIndexCandidateFilter);
            jz_bytes prefix, candidate_prefix;
            JZ_TRY(prefix_bounds(input->durable_ids->index, scan, GROOVE_IVM_UnsupportedIndexCandidateFilter, &prefix));
            if (!input->durable_ids->has_candidate) {
                jz_bytes_drop(&prefix);
                return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedIndexCandidateFilter);
            }
            jz_err e = prefix_bounds(input->durable_ids->candidate, &filter->scan, GROOVE_IVM_UnsupportedIndexCandidateFilter, &candidate_prefix);
            if (JZ_IS_ERR(e)) { jz_bytes_drop(&prefix); return e; }
            out->tag = groove_SRK_IndexedRowsCandidateFilter;
            out->family = jz_str_clone(&input->table);
            out->index = jz_str_clone(&input->index);
            out->a = prefix;
            out->candidate_table = jz_str_clone(&filter->table);
            out->candidate_index = jz_str_clone(&filter->index);
            out->candidate_prefix = candidate_prefix;
            out->source_column = jz_str_clone(&filter->source_column);
            out->candidate_column = jz_str_clone(&filter->candidate_column);
            return JZ_OK;
        }
        if (input->intersections.len != 0) {
            jz_bytes prefix;
            JZ_TRY(prefix_bounds(input->durable_ids->index, scan, GROOVE_IVM_UnsupportedIndexIntersectionScan, &prefix));
            JZ_VEC(groove_IndexPrefixPair) intersections = {0};
            u32 n = input->intersections.len < input->durable_ids->intersections.len ? input->intersections.len : input->durable_ids->intersections.len;
            for (u32 i = 0; i < n; i++) {
                const groove_IndexIntersection *ix = &input->intersections.ptr[i];
                jz_bytes p;
                jz_err e = prefix_bounds(input->durable_ids->intersections.ptr[i], &ix->scan, GROOVE_IVM_UnsupportedIndexIntersectionScan, &p);
                if (JZ_IS_ERR(e)) {
                    JZ_VEC_FOR(groove_IndexPrefixPair, it, intersections) { jz_str_drop(&it->index); jz_bytes_drop(&it->prefix); }
                    jz_vec_drop(&intersections, sizeof(groove_IndexPrefixPair), NULL);
                    jz_bytes_drop(&prefix);
                    return e;
                }
                groove_IndexPrefixPair pair = { jz_str_clone(&ix->index), p };
                JZ_VEC_PUSH(intersections, pair);
            }
            out->tag = groove_SRK_IndexedRowsIntersection;
            out->family = jz_str_clone(&input->table);
            out->index = jz_str_clone(&input->index);
            out->a = prefix;
            out->intersections.ptr = intersections.ptr;
            out->intersections.len = intersections.len;
            out->intersections.cap = intersections.cap;
            return JZ_OK;
        }
        usize max_items;
        bool has_max = groove__ivm__runtime__record_projection__scan_max_items(scan, &max_items);
        groove_StaticScanBounds b;
        JZ_TRY(groove__ivm__runtime__record_projection__persisted_index_scan_bounds(input->durable_ids->index, scan, &b));
        out->family = jz_str_clone(&input->table);
        out->index = jz_str_clone(&input->index);
        if (b.tag == groove_StaticScanBounds_Prefix) {
            jz_bytes_drop(&b.b);
            out->a = b.a;
            if (has_max) {
                out->tag = groove_SRK_IndexedRowsPrefixLimit;
                out->max_items = (u32)max_items;
                out->reversed = groove__ivm__runtime__record_projection__scan_reversed(scan);
            } else {
                out->tag = groove_SRK_IndexedRowsPrefix;
            }
        } else {
            out->tag = groove_SRK_IndexedRowsRange;
            out->a = b.a;
            out->b = b.b;
        }
        return JZ_OK;
    }
    groove_StaticScanBounds b;
    JZ_TRY(groove__ivm__runtime__record_projection__persisted_index_scan_bounds(input->durable_ids->index, scan, &b));
    if (b.tag == groove_StaticScanBounds_Prefix) {
        jz_bytes_drop(&b.b);
        usize max_items;
        out->family = jz_str_from("indices");
        out->a = b.a;
        if (groove__ivm__runtime__record_projection__scan_max_items(scan, &max_items)) {
            out->tag = groove_SRK_ScanPrefixLimit;
            out->max_items = (u32)max_items;
            out->reversed = groove__ivm__runtime__record_projection__scan_reversed(scan);
        } else {
            out->tag = groove_SRK_ScanPrefix;
        }
        return JZ_OK;
    }
    if (jz_bytes_cmp(&b.a, &b.b) < 0) {
        out->tag = groove_SRK_ScanRange;
        out->family = jz_str_from("indices");
        out->a = b.a;
        out->b = b.b;
        return JZ_OK;
    }
    groove__ivm__runtime__record_projection__StaticScanBounds__drop(&b);
    *some = false;
    return JZ_OK;
}

/* Resolve each variant layout once for the batch, before projecting rows. */
static u64 u32_hash(const void *k) { return jz_hash_u64(*(const u32 *)k); }
static bool u32_eq(const void *a, const void *b) { return *(const u32 *)a == *(const u32 *)b; }
/* values are moved out on success, dropped by grouped_drop on error */
static const jz_hmap_desc U32_TABLE_DELTA_MAP = { sizeof(u32), sizeof(groove_TableDelta), u32_hash, u32_eq, NULL, NULL };
static void grouped_drop(jz_hmap *m) {
    u32 *k; groove_TableDelta *v;
    JZ_HMAP_FOR(m, k, v) table_delta_drop(v);
    jz_hmap_drop(m);
}

jz_err groove__ivm__runtime__operator_updates__NodeState__group_source_rows(const groove_TableSchema *table, const groove_KvPairVec *rows, groove_TableDeltaVec *out) {
    jz_hmap grouped; jz_hmap_init(&grouped, &U32_TABLE_DELTA_MAP);
    JZ_VEC_FOR(const groove_KvPair, kv, *rows) {
        u32 variant_tag; jz_slice payload;
        jz_err e = groove__records__split_variant_record(jz_bytes_slice(&kv->value), &variant_tag, &payload);
        if (JZ_IS_ERR(e)) { grouped_drop(&grouped); return e; }
        groove_TableDelta *group = jz_hmap_get(&grouped, &variant_tag);
        if (!group) {
            groove_RecordDescriptor descriptor;
            if (!groove__schema__TableSchema__record_schema_for_variant(table, variant_tag, &descriptor)) {
                grouped_drop(&grouped);
                return groove__ivm__runtime__IvmRuntimeError__table_target_version(GROOVE_IVM_UnknownTableVariant, &table->name, "", 0, variant_tag);
            }
            bool inserted;
            group = jz_hmap_entry(&grouped, &variant_tag, &inserted);
            group->table = jz_str_clone(&table->name);
            group->variant_tag = variant_tag;
            group->descriptor = descriptor;
            group->deltas.ptr = NULL; group->deltas.len = 0; group->deltas.cap = 0;
        }
        groove_RecordDelta d = { jz_bytes_from(payload.ptr, payload.len), 1 };
        JZ_VEC_PUSH(group->deltas, d);
    }
    out->ptr = NULL; out->len = 0; out->cap = 0;
    u32 *k; groove_TableDelta *v;
    JZ_HMAP_FOR(&grouped, k, v) JZ_VEC_PUSH(*out, *v);
    jz_hmap_drop(&grouped); /* values moved out */
    return JZ_OK;
}

jz_err groove__ivm__runtime__operator_updates__NodeState__update_table_source_from_inputs(const groove_TableSourceOp *input, const groove_DatabaseSchema *schema,
    const jz_hmap *vp, groove_RecordDescriptor output_desc, groove_EvaluationInputs *inputs, groove_RecordDeltas *out) {
    bool some; groove_StorageRequestKey request;
    JZ_TRY(groove__ivm__runtime__operator_updates__NodeState__table_source_request(input, &some, &request));
    if (!some) { deltas_init(out, output_desc); return JZ_OK; }
    const groove_TableSchema *table_schema = groove__schema__DatabaseSchema__table(schema, input->table.ptr, input->table.len);
    if (!table_schema) { groove_StorageRequestKey_drop(&request); return GROOVE_IVM_ERR_S(GROOVE_IVM_TableNotFound, &input->table); }
    const groove_KvPairVec *rows;
    JZ_TRY(groove__ivm__runtime__evaluation_session__EvaluationInputs__rows(inputs, request, &rows));
    groove_TableDeltaVec table_deltas;
    JZ_TRY(groove__ivm__runtime__operator_updates__NodeState__group_source_rows(table_schema, rows, &table_deltas));
    jz_err e = groove__ivm__runtime__operator_updates__NodeState__update_table_source(input, schema, vp, output_desc, table_deltas.ptr, table_deltas.len, out);
    jz_vec_drop(&table_deltas, sizeof(groove_TableDelta), table_delta_drop);
    return e;
}

jz_err groove__ivm__runtime__operator_updates__NodeState__update_index_source_from_inputs(const groove_IndexSourceOp *input, const groove_DatabaseSchema *schema,
    const jz_hmap *vp, groove_RecordDescriptor output_desc, groove_EvaluationInputs *inputs, groove_RecordDeltas *out) {
    bool some; groove_StorageRequestKey request;
    JZ_TRY(groove__ivm__runtime__operator_updates__NodeState__index_source_request(input, &some, &request));
    if (!some) { deltas_init(out, output_desc); return JZ_OK; }
    const groove_KvPairVec *rows;
    JZ_TRY(groove__ivm__runtime__evaluation_session__EvaluationInputs__rows(inputs, request, &rows));
    if (input->has_row_projection) {
        const groove_TableSchema *table_schema = groove__schema__DatabaseSchema__table(schema, input->table.ptr, input->table.len);
        if (!table_schema) return GROOVE_IVM_ERR_S(GROOVE_IVM_TableNotFound, &input->table);
        groove_TableDeltaVec table_deltas;
        JZ_TRY(groove__ivm__runtime__operator_updates__NodeState__group_source_rows(table_schema, rows, &table_deltas));
        groove_TableSourceOp op; memset(&op, 0, sizeof op);
        op.table = input->table;
        op.has_vp = true;
        op.variant_projection = input->row_projection;
        jz_err e = groove__ivm__runtime__operator_updates__NodeState__update_table_source(&op, schema, vp, output_desc, table_deltas.ptr, table_deltas.len, out);
        jz_vec_drop(&table_deltas, sizeof(groove_TableDelta), table_delta_drop);
        return e;
    }
    jz_bytes prefix = groove__ivm__runtime__index_ids__durable_index_key_prefix(input->durable_ids->index);
    u32 prefix_len = prefix.len;
    jz_bytes_drop(&prefix);
    deltas_init(out, output_desc);
    JZ_VEC_FOR(const groove_KvPair, kv, *rows) {
        groove_RecordDelta d;
        jz_err e = groove__ivm__runtime__persist__index_record_from_storage(prefix_len, jz_bytes_slice(&kv->key), jz_bytes_clone(&kv->value), &d.record);
        if (JZ_IS_ERR(e)) { groove_RecordDeltas_drop(out); return e; }
        d.weight = 1;
        JZ_VEC_PUSH(out->deltas, d);
    }
    return JZ_OK;
}

jz_err groove__ivm__runtime__operator_updates__NodeState__update_table_source(const groove_TableSourceOp *input, const groove_DatabaseSchema *schema,
    const jz_hmap *variant_projections, groove_RecordDescriptor output_desc, const groove_TableDelta *table_deltas, u32 ntd, groove_RecordDeltas *out) {
    const groove_TableSchema *table_schema = groove__schema__DatabaseSchema__table(schema, input->table.ptr, input->table.len);
    if (!table_schema) return GROOVE_IVM_ERR_S(GROOVE_IVM_TableNotFound, &input->table);
    groove_U32Vec primary_key_fields = {0};
    if (input->has_scan)
        JZ_TRY(groove__ivm__runtime__key_encoding__primary_key_field_indices(table_schema, output_desc, &primary_key_fields));
    const groove_VariantProjection *projection = NULL;
    jz_err err = JZ_OK;
    if (input->has_vp) {
        groove_VariantProjectionKey key = { input->table, input->variant_projection };
        projection = jz_hmap_get(variant_projections, &key);
        if (!projection) {
            jz_str t = target_name(&input->variant_projection);
            err = groove__ivm__runtime__IvmRuntimeError__table_target(GROOVE_IVM_VariantProjectionNotFound, &input->table, t.ptr, t.len);
            jz_str_drop(&t);
            goto done;
        }
    }
    deltas_init(out, output_desc);
    for (u32 i = 0; i < ntd; i++) {
        const groove_TableDelta *delta = &table_deltas[i];
        if (!jz_str_eq(&delta->table, &input->table)) continue;
        groove_RecordDeltas projected; bool have_projected = false;
        const groove_RecordDelta *source; u32 nsource;
        if (projection) {
            if (!groove__records__RecordDescriptor__registry_compatible_with(projection->output, output_desc)) { err = GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch); goto fail; }
            const groove_VariantProjectionCase *c = jz_hmap_get(&projection->cases, &delta->variant_tag);
            if (!c) {
                jz_str t = target_name(input->has_vp ? &input->variant_projection : NULL);
                err = groove__ivm__runtime__IvmRuntimeError__table_target_version(GROOVE_IVM_VariantProjectionCaseNotFound, &input->table, t.ptr, t.len, delta->variant_tag);
                jz_str_drop(&t);
                goto fail;
            }
            if (!groove__records__RecordDescriptor__registry_compatible_with(groove__ivm__runtime__VariantProjectionCase__source(c), delta->descriptor)) {
                jz_str t = target_name(input->has_vp ? &input->variant_projection : NULL);
                err = groove__ivm__runtime__IvmRuntimeError__table_target_version(GROOVE_IVM_VariantProjectionSourceMismatch, &input->table, t.ptr, t.len, delta->variant_tag);
                jz_str_drop(&t);
                goto fail;
            }
            groove_RecordDeltas in = { delta->descriptor, { delta->deltas.ptr, delta->deltas.len, delta->deltas.cap } };
            switch (c->tag) {
            case groove_VPC_Project:
                err = groove__ivm__runtime__operator_updates__NodeState__update_map_project(&c->project, output_desc, &in, c->raw_projection, c->omit_unrepresentable_enum_rows, &projected);
                break;
            case groove_VPC_Enum:
                err = groove__ivm__runtime__operator_updates__NodeState__update_variant_enum_project(c->enum_tag, c->payload, &c->project, output_desc, &in, c->raw_projection, &projected);
                break;
            default:
                continue;
            }
            if (JZ_IS_ERR(err)) goto fail;
            have_projected = true;
            source = projected.deltas.ptr; nsource = projected.deltas.len;
        } else {
            if (!groove__records__RecordDescriptor__registry_compatible_with(delta->descriptor, output_desc)) { err = GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch); goto fail; }
            source = delta->deltas.ptr; nsource = delta->deltas.len;
        }
        for (u32 j = 0; j < nsource; j++) {
            const groove_RecordDelta *rd = &source[j];
            if (input->has_scan) {
                jz_bytes key;
                err = groove__ivm__runtime__key_encoding__primary_key_value_bytes(output_desc, jz_bytes_slice(&rd->record), primary_key_fields.ptr, primary_key_fields.len, &key);
                if (JZ_IS_ERR(err)) { if (have_projected) groove_RecordDeltas_drop(&projected); goto fail; }
                bool matches;
                err = groove__ivm__runtime__record_projection__key_matches_static_scan(key.ptr, key.len, &input->scan, &matches);
                jz_bytes_drop(&key);
                if (JZ_IS_ERR(err)) { if (have_projected) groove_RecordDeltas_drop(&projected); goto fail; }
                if (!matches) continue;
            }
            push_delta_clone(&out->deltas, rd);
        }
        if (have_projected) groove_RecordDeltas_drop(&projected);
    }
    goto done;
fail:
    groove_RecordDeltas_drop(out);
done:
    jz_vec_drop(&primary_key_fields, sizeof(u32), NULL);
    return err;
}

static void index_by_from_source(const groove_IndexSourceOp *input, groove_IndexByOp *index_by) {
    memset(index_by, 0, sizeof *index_by);
    index_by->key_fields.ptr = input->key_fields.ptr; index_by->key_fields.len = input->key_fields.len; index_by->key_fields.cap = input->key_fields.cap;
    index_by->value_fields.ptr = input->value_fields.ptr; index_by->value_fields.len = input->value_fields.len; index_by->value_fields.cap = input->value_fields.cap;
    index_by->unique = input->unique;
    index_by->append_value_to_key = input->append_value_to_key;
    index_by->store_value = input->store_value;
    index_by->has_scan = input->has_scan;
    index_by->scan = input->scan; /* borrowed for the call */
}

jz_err groove__ivm__runtime__operator_updates__NodeState__update_index_source(const groove_IndexSourceOp *input, const groove_DatabaseSchema *schema,
    const jz_hmap *variant_projections, groove_RecordDescriptor output_desc, const groove_TableDelta *table_deltas, u32 ntd,
    const groove_Storage *storage, u32 eval_mode, groove_RecordDeltas *out) {
    const groove_StaticScanSpec *scan = input->has_scan ? &input->scan : NULL;
    if (eval_mode == groove_EvalMode_Hydrate) {
        if (!storage) return GROOVE_IVM_ERR(GROOVE_IVM_StorageUnavailable);
        usize max_items;
        bool has_max = groove__ivm__runtime__record_projection__scan_max_items(scan, &max_items);
        groove_ScanDirection direction = groove__ivm__runtime__record_projection__scan_reversed(scan) ? groove_ScanDirection_Reverse : groove_ScanDirection_Forward;
        groove_StaticScanBounds b;
        JZ_TRY(groove__ivm__runtime__record_projection__persisted_index_scan_bounds(input->durable_ids->index, scan, &b));
        groove_ScanRequest req; memset(&req, 0, sizeof req);
        req.cf = jz_str_from("indices");
        req.direction = direction;
        req.has_max_items = has_max; req.max_items = (u32)max_items;
        if (b.tag == groove_StaticScanBounds_Prefix) {
            jz_bytes_drop(&b.b);
            req.bounds.tag = groove_ScanBounds_Prefix; req.bounds.start = b.a;
        } else {
            if (jz_bytes_cmp(&b.a, &b.b) >= 0) {
                groove__ivm__runtime__record_projection__StaticScanBounds__drop(&b);
                jz_str_drop(&req.cf);
                deltas_init(out, output_desc);
                return JZ_OK;
            }
            req.bounds.tag = groove_ScanBounds_Range; req.bounds.start = b.a; req.bounds.end = b.b;
        }
        groove_KvPairVec batch = {0};
        jz_err e = storage->vt->scan(storage->self, &req, &batch);
        groove_ScanRequest_drop(&req);
        JZ_TRY(e);
        jz_bytes prefix = groove__ivm__runtime__index_ids__durable_index_key_prefix(input->durable_ids->index);
        u32 prefix_len = prefix.len;
        jz_bytes_drop(&prefix);
        deltas_init(out, output_desc);
        JZ_VEC_FOR(groove_KvPair, kv, batch) {
            groove_RecordDelta d; d.weight = 1;
            e = groove__ivm__runtime__persist__index_record_from_storage(prefix_len, jz_bytes_slice(&kv->key), kv->value, &d.record);
            kv->value.ptr = NULL; kv->value.len = 0; kv->value.cap = 0;
            if (JZ_IS_ERR(e)) { groove_KvPairVec_drop(&batch); groove_RecordDeltas_drop(out); return e; }
            JZ_VEC_PUSH(out->deltas, d);
        }
        groove_KvPairVec_drop(&batch);
        return JZ_OK;
    }

    groove_IndexByOp index_by;
    index_by_from_source(input, &index_by);
    if (input->has_row_projection) {
        groove_RecordDeltaVec matching = {0};
        for (u32 i = 0; i < ntd; i++) {
            const groove_TableDelta *td = &table_deltas[i];
            if (!jz_str_eq(&td->table, &input->table)) continue;
            JZ_VEC_FOR(const groove_RecordDelta, rd, td->deltas) {
                groove_TableDelta one = { td->table, td->variant_tag, td->descriptor, { (groove_RecordDelta *)rd, 1, 1 } };
                groove_TableSourceOp op; memset(&op, 0, sizeof op);
                op.table = input->table;
                op.has_vp = input->has_vp; op.variant_projection = input->variant_projection;
                groove_RecordDeltas index_input;
                jz_err e = groove__ivm__runtime__operator_updates__NodeState__update_table_source(&op, schema, variant_projections, input->input_descriptor, &one, 1, &index_input);
                if (JZ_IS_ERR(e)) { jz_vec_drop(&matching, sizeof(groove_RecordDelta), groove_RecordDelta_drop); return e; }
                groove_RecordDeltaVec indexed = {0};
                e = groove__ivm__runtime__record_projection__apply_index_by(&index_by, input->input_descriptor, index_input.deltas.ptr, index_input.deltas.len, &indexed);
                groove_RecordDeltas_drop(&index_input);
                if (JZ_IS_ERR(e)) { jz_vec_drop(&matching, sizeof(groove_RecordDelta), groove_RecordDelta_drop); return e; }
                bool empty = indexed.len == 0;
                jz_vec_drop(&indexed, sizeof(groove_RecordDelta), groove_RecordDelta_drop);
                if (empty) continue;
                op.has_vp = true; op.variant_projection = input->row_projection;
                groove_RecordDeltas rows;
                e = groove__ivm__runtime__operator_updates__NodeState__update_table_source(&op, schema, variant_projections, output_desc, &one, 1, &rows);
                if (JZ_IS_ERR(e)) { jz_vec_drop(&matching, sizeof(groove_RecordDelta), groove_RecordDelta_drop); return e; }
                jz_vec_extend(&matching, rows.deltas.ptr, rows.deltas.len, sizeof(groove_RecordDelta));
                jz_vec_drop(&rows.deltas, sizeof(groove_RecordDelta), NULL);
            }
        }
        out->descriptor = output_desc;
        out->deltas.ptr = matching.ptr; out->deltas.len = matching.len; out->deltas.cap = matching.cap;
        return JZ_OK;
    }

    groove_TableSourceOp op; memset(&op, 0, sizeof op);
    op.table = input->table;
    op.has_vp = input->has_vp; op.variant_projection = input->variant_projection;
    groove_RecordDeltas source;
    JZ_TRY(groove__ivm__runtime__operator_updates__NodeState__update_table_source(&op, schema, variant_projections, input->input_descriptor, table_deltas, ntd, &source));
    groove_RecordDeltaVec deltas = {0};
    jz_err e = groove__ivm__runtime__record_projection__apply_index_by(&index_by, input->input_descriptor, source.deltas.ptr, source.deltas.len, &deltas);
    groove_RecordDeltas_drop(&source);
    JZ_TRY(e);
    out->descriptor = output_desc;
    out->deltas.ptr = deltas.ptr; out->deltas.len = deltas.len; out->deltas.cap = deltas.cap;
    return JZ_OK;
}

jz_err groove__ivm__runtime__operator_updates__NodeState__update_binding_source(const groove_BindingSourceOp *input, groove_RecordDescriptor output_desc,
    const groove_BindingDelta *binding_deltas, u32 nbd, const jz_hmap *binding_snapshots, u32 mode, groove_RecordDeltas *out) {
    if (mode == groove_ArrangementUpdateMode_Replace) {
        const groove_RecordDeltas *snapshot = jz_hmap_get(binding_snapshots, &input->key);
        if (!snapshot) { deltas_init(out, output_desc); return JZ_OK; }
        return groove__ivm__runtime__key_encoding__project_binding_source_deltas(snapshot, output_desc, out);
    }
    deltas_init(out, output_desc);
    for (u32 i = 0; i < nbd; i++) {
        const groove_BindingDelta *delta = &binding_deltas[i];
        if (!groove_BindingSourceKey_eq(&delta->key, &input->key)) continue;
        groove_RecordDeltas in = { delta->descriptor, { delta->deltas.ptr, delta->deltas.len, delta->deltas.cap } };
        groove_RecordDeltas projected;
        jz_err e = groove__ivm__runtime__key_encoding__project_binding_source_deltas(&in, output_desc, &projected);
        if (JZ_IS_ERR(e)) { groove_RecordDeltas_drop(out); return e; }
        jz_vec_extend(&out->deltas, projected.deltas.ptr, projected.deltas.len, sizeof(groove_RecordDelta));
        jz_vec_drop(&projected.deltas, sizeof(groove_RecordDelta), NULL);
    }
    return JZ_OK;
}

jz_err groove__ivm__runtime__operator_updates__NodeState__update_filter(const groove_FilterOp *filter, groove_RecordDescriptor output_desc, const groove_RecordDeltas *input, groove_RecordDeltas *out) {
    deltas_init(out, output_desc);
    JZ_VEC_FOR(const groove_RecordDelta, delta, input->deltas) {
        groove_BorrowedRecord rec = groove_RecordDelta_borrowed(delta, input->descriptor);
        groove_PredicateRecord pr = { &groove__ivm__runtime__key_encoding__BorrowedRecord__PredicateRecord__VT, &rec };
        bool matches;
        jz_err e = groove__ivm__runtime__subscriptions__PredicateExpr__matches(&filter->predicate, pr, filter->comparison, &matches);
        if (JZ_IS_ERR(e)) { groove_RecordDeltas_drop(out); return e; }
        if (matches) push_delta_clone(&out->deltas, delta);
    }
    return JZ_OK;
}

jz_err groove__ivm__runtime__operator_updates__NodeState__update_map_project(const groove_MapProjectOp *project, groove_RecordDescriptor output_desc,
    const groove_RecordDeltas *input, const groove_PreparedProjection *raw_projection, bool omit, groove_RecordDeltas *out) {
    return groove__ivm__runtime__operator_updates__NodeState__update_map_project_slice(project, output_desc, input->descriptor,
        input->deltas.ptr, input->deltas.len, raw_projection, omit, out);
}

typedef struct Span { u32 start, end; i64 weight; } Span;

/* Slice each span out of the frozen batch buffer (Bytes::slice is a copy here). */
static void deltas_from_spans(jz_bytes buffer, const Span *spans, u32 n, groove_RecordDescriptor output_desc, groove_RecordDeltas *out) {
    jz_bytes output = groove__ivm__runtime__freeze_batch_buffer(buffer);
    deltas_init(out, output_desc);
    jz_vec_reserve(&out->deltas, n, sizeof(groove_RecordDelta));
    for (u32 i = 0; i < n; i++) {
        groove_RecordDelta d = { jz_bytes_from(output.ptr + spans[i].start, spans[i].end - spans[i].start), spans[i].weight };
        JZ_VEC_PUSH(out->deltas, d);
    }
    jz_bytes_drop(&output);
}

jz_err groove__ivm__runtime__operator_updates__NodeState__update_map_project_slice(const groove_MapProjectOp *project, groove_RecordDescriptor output_desc,
    groove_RecordDescriptor input_desc, const groove_RecordDelta *input, u32 n, const groove_PreparedProjection *raw_projection, bool omit, groove_RecordDeltas *out) {
    if (raw_projection && raw_projection->reuses_input) {
        deltas_init(out, output_desc);
        for (u32 i = 0; i < n; i++) push_delta_clone(&out->deltas, &input[i]);
        return JZ_OK;
    }
    if (!omit) {
        JZ_VEC_FOR(const groove_ProjectionExpr, e, project->expressions) {
            if (e->expression.tag == groove_ProjectExpr_RecursiveEnumRemap && e->expression.omit_unrepresentable) { omit = true; break; }
        }
    }
    u32 estimated_output_bytes = 0;
    for (u32 i = 0; i < n; i++) estimated_output_bytes += input[i].record.len;
    jz_bytes output = {0};
    jz_vec_reserve(&output, estimated_output_bytes, 1);
    JZ_VEC(Span) spans = {0};
    jz_vec_reserve(&spans, n, sizeof(Span));
    for (u32 i = 0; i < n; i++) {
        bool some; u32 start, end;
        jz_err e = groove__ivm__runtime__operator_updates__NodeState__project_row_into(project, output_desc, input_desc, jz_bytes_slice(&input[i].record),
            raw_projection, omit, &output, &some, &start, &end);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&output); jz_vec_drop(&spans, sizeof(Span), NULL); return e; }
        if (some) { Span s = { start, end, input[i].weight }; JZ_VEC_PUSH(spans, s); }
    }
    deltas_from_spans(output, spans.ptr, spans.len, output_desc, out);
    jz_vec_drop(&spans, sizeof(Span), NULL);
    return JZ_OK;
}

typedef struct EvalFieldCtx {
    const groove_MapProjectOp *project; groove_RecordDescriptor output_desc, input_desc; jz_slice raw;
} EvalFieldCtx;

static jz_err eval_projection_field(u32 index, jz_bytes *output, void *p) {
    EvalFieldCtx *c = p;
    groove_Value value;
    JZ_TRY(groove__ivm__runtime__record_projection__project_field_value(&c->project->expressions.ptr[index], index, c->output_desc, c->input_desc,
        c->raw.ptr, c->raw.len, &value));
    jz_bytes encoded;
    JZ_TRY(groove__ivm__runtime__record_projection__encode_projection_field_value(c->output_desc, index, value, &encoded));
    jz_bytes_push(output, encoded.ptr, encoded.len);
    jz_bytes_drop(&encoded);
    return JZ_OK;
}

static bool is_unrepresentable_enum(jz_err e) {
    return e.code == GROOVE_IVM_EnumTagProjectionAbsent || e.code == GROOVE_IVM_EnumProjectionAbsent;
}

jz_err groove__ivm__runtime__operator_updates__NodeState__project_row_into(const groove_MapProjectOp *project, groove_RecordDescriptor output_desc,
    groove_RecordDescriptor input_desc, jz_slice raw, const groove_PreparedProjection *raw_projection, bool omit,
    jz_bytes *output, bool *some, u32 *start, u32 *end) {
    *some = false;
    if (raw_projection) {
        u32 begin = output->len;
        EvalFieldCtx ctx = { project, output_desc, input_desc, raw };
        jz_err e = groove__records__RecordDescriptor__project_raw_fields_into(output_desc, input_desc, raw, raw_projection->fields.ptr, raw_projection->fields.len,
            output, eval_projection_field, &ctx, start, end);
        if (JZ_IS_ERR(e)) {
            if (is_unrepresentable_enum(e) && omit) {
                jz_err_drop(&e);
                output->len = begin;
                return JZ_OK;
            }
            return e;
        }
        *some = true;
        return JZ_OK;
    }
    u32 begin = output->len;
    jz_bytes record;
    jz_err e = groove__ivm__runtime__record_projection__project_record(project->expressions.ptr, project->expressions.len,
        (const usize (*)[2])project->mapping.ptr, project->mapping.len, output_desc, input_desc, raw.ptr, raw.len, &record);
    if (JZ_IS_ERR(e)) {
        if (is_unrepresentable_enum(e) && omit) { jz_err_drop(&e); return JZ_OK; }
        return e;
    }
    jz_bytes_push(output, record.ptr, record.len);
    jz_bytes_drop(&record);
    *start = begin;
    *end = output->len;
    *some = true;
    return JZ_OK;
}

jz_err groove__ivm__runtime__operator_updates__NodeState__update_variant_enum_project(u32 tag, groove_RecordDescriptor payload_desc, const groove_MapProjectOp *project,
    groove_RecordDescriptor output_desc, const groove_RecordDeltas *input, const groove_PreparedProjection *raw_projection, groove_RecordDeltas *out) {
    groove_RecordDeltas payloads;
    JZ_TRY(groove__ivm__runtime__operator_updates__NodeState__update_map_project(project, payload_desc, input, raw_projection, false, &payloads));
    deltas_init(out, output_desc);
    jz_vec_reserve(&out->deltas, payloads.deltas.len, sizeof(groove_RecordDelta));
    JZ_VEC_FOR(groove_RecordDelta, payload, payloads.deltas) {
        groove_Value value; memset(&value, 0, sizeof value);
        value.tag = groove_Value_Enum;
        value.v.enm.tag = tag;
        value.v.enm.record.raw = jz_bytes_clone(&payload->record);
        value.v.enm.record.descriptor = payload_desc;
        groove_RecordDelta d; d.weight = payload->weight;
        jz_err e = groove__records__RecordDescriptor__create(output_desc, &value, 1, &d.record);
        groove_Value_drop(&value);
        if (JZ_IS_ERR(e)) { groove_RecordDeltas_drop(&payloads); groove_RecordDeltas_drop(out); return e; }
        JZ_VEC_PUSH(out->deltas, d);
    }
    groove_RecordDeltas_drop(&payloads);
    return JZ_OK;
}

jz_err groove__ivm__runtime__operator_updates__NodeState__update_variant_project(const groove_VariantProjectOp *vp, groove_RecordDescriptor output_desc,
    const groove_RecordDeltas *input, groove_RecordDeltas *out) {
    deltas_init(out, output_desc);
    JZ_VEC_FOR(const groove_RecordDelta, delta, input->deltas) {
        groove_BorrowedRecord rec = groove_RecordDelta_borrowed(delta, input->descriptor);
        groove_Value value;
        jz_err e = groove__records__BorrowedRecord__get_idx(&rec, vp->field_idx, &value);
        if (JZ_IS_ERR(e)) { groove_RecordDeltas_drop(out); return e; }
        if (value.tag != groove_Value_Enum) {
            groove_Value_drop(&value);
            groove_RecordDeltas_drop(out);
            return groove__ivm__runtime__IvmRuntimeError__with_str(GROOVE_IVM_VariantProjectFieldTypeMismatch, vp->field.ptr, vp->field.len);
        }
        if (value.v.enm.tag == vp->tag) {
            if (value.v.enm.record.descriptor != output_desc) {
                groove_Value_drop(&value);
                groove_RecordDeltas_drop(out);
                return groove__ivm__runtime__IvmRuntimeError__with_str(GROOVE_IVM_VariantProjectPayloadMismatch, vp->field.ptr, vp->field.len);
            }
            groove_RecordDelta d = { jz_bytes_clone(&value.v.enm.record.raw), delta->weight };
            JZ_VEC_PUSH(out->deltas, d);
        }
        groove_Value_drop(&value);
    }
    return JZ_OK;
}

jz_err groove__ivm__runtime__operator_updates__NodeState__update_unwrap_nullable(const groove_UnwrapNullableOp *unwrap, groove_RecordDescriptor output_desc,
    const groove_RecordDeltas *input, groove_RecordDeltas *out) {
    u32 estimated_output_bytes = 0;
    JZ_VEC_FOR(const groove_RecordDelta, d, input->deltas) estimated_output_bytes += d->record.len;
    jz_bytes output = {0};
    jz_vec_reserve(&output, estimated_output_bytes, 1);
    JZ_VEC(Span) spans = {0};
    groove_RawProjectionScratch *scratch = groove__records__RawProjectionScratch__default();
    JZ_VEC_FOR(const groove_RecordDelta, delta, input->deltas) {
        bool some; u32 start, end;
        jz_err e = groove__records__RecordDescriptor__unwrap_nullable_field_into(output_desc, input->descriptor, jz_bytes_slice(&delta->record),
            unwrap->field_idx, &output, scratch, &some, &start, &end);
        if (JZ_IS_ERR(e)) {
            groove_RawProjectionScratch_drop(scratch);
            jz_bytes_drop(&output);
            jz_vec_drop(&spans, sizeof(Span), NULL);
            return e; /* IvmRuntimeError::RecordEncoding (transparent) */
        }
        if (some) { Span s = { start, end, delta->weight }; JZ_VEC_PUSH(spans, s); }
    }
    groove_RawProjectionScratch_drop(scratch);
    deltas_from_spans(output, spans.ptr, spans.len, output_desc, out);
    jz_vec_drop(&spans, sizeof(Span), NULL);
    return JZ_OK;
}

jz_err groove__ivm__runtime__operator_updates__NodeState__update_unnest(const groove_UnnestOp *unnest, groove_RecordDescriptor output_desc,
    const groove_RecordDeltas *input, groove_RecordDeltas *out) {
    deltas_init(out, output_desc);
    JZ_VEC_FOR(const groove_RecordDelta, delta, input->deltas) {
        groove_BorrowedRecord rec = groove_RecordDelta_borrowed(delta, input->descriptor);
        groove_ValueVec values;
        jz_err e = groove__records__BorrowedRecord__to_values(&rec, &values);
        if (JZ_IS_ERR(e)) { groove_RecordDeltas_drop(out); return e; }
        if (unnest->array_field_idx >= values.len) {
            jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop_v);
            groove_RecordDeltas_drop(out);
            return groove__ivm__runtime__IvmRuntimeError__with_usize(GROOVE_IVM_GraphFieldIndexOutOfBounds, unnest->array_field_idx);
        }
        const groove_Value *value = &values.ptr[unnest->array_field_idx];
        if (value->tag != groove_Value_Array) {
            jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop_v);
            groove_RecordDeltas_drop(out);
            return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
        }
        JZ_VEC_FOR(const groove_Value, element, value->v.vals) {
            groove_ValueVec output_values = {0};
            jz_vec_clone(&output_values, &values, sizeof(groove_Value), (void (*)(void *, const void *))groove_Value_clone);
            groove_Value ec; groove_Value_clone(&ec, element);
            JZ_VEC_PUSH(output_values, ec);
            groove_RecordDelta d; d.weight = delta->weight;
            e = groove__records__RecordDescriptor__create(output_desc, output_values.ptr, output_values.len, &d.record);
            jz_vec_drop(&output_values, sizeof(groove_Value), groove_Value_drop_v);
            if (JZ_IS_ERR(e)) { jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop_v); groove_RecordDeltas_drop(out); return e; }
            JZ_VEC_PUSH(out->deltas, d);
        }
        jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop_v);
    }
    return JZ_OK;
}

jz_err groove__ivm__runtime__operator_updates__NodeState__update_union(groove_RecordDescriptor output_desc, groove_RecordDeltas **inputs, u32 n, groove_RecordDeltas *out) {
    deltas_init(out, output_desc);
    for (u32 i = 0; i < n; i++) {
        groove_RecordDeltas *input = inputs[i];
        if (input->deltas.len == 0) { jz_rc_release(input); continue; }
        if (!groove__records__RecordDescriptor__registry_compatible_with(output_desc, input->descriptor)) {
            for (u32 j = i; j < n; j++) jz_rc_release(inputs[j]);
            groove_RecordDeltas_drop(out);
            return GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch);
        }
        JZ_VEC_FOR(const groove_RecordDelta, d, input->deltas) push_delta_clone(&out->deltas, d);
        jz_rc_release(input);
    }
    return JZ_OK;
}

jz_err groove__ivm__runtime__operator_updates__NodeState__update_index_by(const groove_IndexByOp *index_by, groove_RecordDescriptor output_desc,
    const groove_RecordDeltas *input, groove_RecordDeltas *out) {
    groove_RecordDeltaVec deltas = {0};
    JZ_TRY(groove__ivm__runtime__record_projection__apply_index_by(index_by, input->descriptor, input->deltas.ptr, input->deltas.len, &deltas));
    out->descriptor = output_desc;
    out->deltas.ptr = deltas.ptr; out->deltas.len = deltas.len; out->deltas.cap = deltas.cap;
    return JZ_OK;
}
