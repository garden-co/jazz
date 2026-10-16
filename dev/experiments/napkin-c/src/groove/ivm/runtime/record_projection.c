/* groove::ivm::runtime::record_projection — record descriptors, projection,
 * enum remapping, root ordering and operator shape validation. */
#include "record_projection.h"

#define RP(name) groove__ivm__runtime__record_projection__##name

static const groove_GraphNode *graph_node(const groove_IvmGraph *g, groove_NodeId id) {
    return groove__ivm__graph__IvmGraph__node(g, id);
}
static usize desc_len(groove_RecordDescriptor d) { return d->fields.len; }
static bool usize_contains(const groove_UsizeVec *v, usize x) {
    for (u32 i = 0; i < v->len; i++) if (v->ptr[i] == x) return true;
    return false;
}

/* FxHashSet<&[u8]> / FxHashMap<&[u8], usize> */
static u64 slice_hash(const void *k) { const jz_slice *s = k; return jz_hash_bytes(s->ptr, s->len); }
static bool slice_eq(const void *a, const void *b) { const jz_slice *x = a, *y = b; return x->len == y->len && memcmp(x->ptr, y->ptr, x->len) == 0; }
static const jz_hmap_desc slice_set_desc = { sizeof(jz_slice), 0, slice_hash, slice_eq, NULL, NULL };
static const jz_hmap_desc slice_usize_desc = { sizeof(jz_slice), sizeof(usize), slice_hash, slice_eq, NULL, NULL };
static u64 usize_hash(const void *k) { return jz_hash_u64(*(const usize *)k); }
static bool usize_eq(const void *a, const void *b) { return *(const usize *)a == *(const usize *)b; }
static const jz_hmap_desc usize_bool_desc = { sizeof(usize), sizeof(bool), usize_hash, usize_eq, NULL, NULL };
static u64 str_hash(const void *k) { return jz_str_hash(k); }
static bool str_eq(const void *a, const void *b) { return jz_str_eq(a, b); }
static const jz_hmap_desc str_set_desc = { sizeof(jz_str), 0, str_hash, str_eq, groove_ivm_rt_a_drop_str, NULL };

void RP(RootIdentity__drop)(groove_RootIdentity *r) {
    jz_vec_drop(&r->fields, sizeof(usize), NULL);
    jz_vec_drop(&r->chain, sizeof(groove_NodeId), NULL);
}

static void indexed_key_drop(void *p) { jz_bytes_drop(&((groove_IndexedKey *)p)->key); }
void RP(GroupChanges__drop)(groove_GroupChanges *c) {
    jz_vec_drop(&c->removed, sizeof(groove_IndexedKey), indexed_key_drop);
    jz_vec_drop(&c->added, sizeof(groove_IndexedKey), indexed_key_drop);
}

void RP(StaticScanBounds__drop)(groove_StaticScanBounds *b) {
    jz_bytes_drop(&b->prefix);
    if (b->tag == GROOVE_SSB_RANGE) jz_bytes_drop(&b->end);
}

/* Keep demand planning and terminal publication on the same classification. */
jz_err RP(output_is_structured_collect_by)(const groove_IvmGraph *g, groove_NodeId node, bool *out) {
    const groove_GraphNode *n = graph_node(g, node);
    if (!n) return groove_err_node_not_found(node);
    *out = n->descriptor.op.tag == GROOVE_OP_COLLECT_BY &&
           (n->descriptor.op.v.collect_by->mode == GROOVE_CBM_COLLECT || n->descriptor.op.v.collect_by->mode == GROOVE_CBM_ROOT);
    return JZ_OK;
}

/* Row identity of a TopBy's output: its group fields plus its tie fields. */
void RP(top_by_identity_fields)(const groove_TopByOp *top_by, usize width, groove_UsizeVec *out) {
    u32 order_len = top_by->order_fields.len;
    u32 start = order_len < top_by->sort_field_indices.len ? order_len : top_by->sort_field_indices.len;
    const usize *tie = top_by->sort_field_indices.ptr + start;
    u32 tie_len = top_by->sort_field_indices.len - start;
    groove_UsizeVec fields = {0};
    jz_vec_extend(&fields, top_by->group_field_indices.ptr, top_by->group_field_indices.len, sizeof(usize));
    groove_UsizeVec identity = {0};
    if (tie_len == 0) {
        for (usize i = 0; i < width; i++) JZ_VEC_PUSH(identity, i);
    } else {
        jz_vec_extend(&identity, tie, tie_len, sizeof(usize));
    }
    for (u32 i = 0; i < identity.len; i++) {
        if (!usize_contains(&fields, identity.ptr[i])) JZ_VEC_PUSH(fields, identity.ptr[i]);
    }
    jz_vec_drop(&identity, sizeof(usize), NULL);
    *out = fields;
}

jz_err RP(output_consumes_root_positions)(const groove_IvmGraph *g, groove_NodeId output, groove_NodeId ordering, bool *out) {
    bool structured;
    JZ_TRY(RP(output_is_structured_collect_by)(g, output, &structured));
    if (structured) { *out = false; return JZ_OK; }
    bool some = false; groove_RootIdentity id;
    JZ_TRY(RP(root_identity_fields)(g, output, ordering, &some, &id));
    if (some) RP(RootIdentity__drop)(&id);
    *out = some;
    return JZ_OK;
}

static jz_err chain_filters_within_groups(const groove_IvmGraph *g, const groove_NodeId *chain, u32 n, const groove_TopByOp *top_by, usize width, bool *out);
static jz_err map_through_projections(const groove_IvmGraph *g, const groove_NodeId *chain, u32 n, groove_UsizeVec fields, bool *some, groove_UsizeVec *out);

/* Terminal key fields of a plain output ordered by `ordering` (#3290). */
jz_err RP(root_identity_fields)(const groove_IvmGraph *g, groove_NodeId output, groove_NodeId ordering, bool *some, groove_RootIdentity *out) {
    *some = false;
    groove_NodeIdVec chain = {0};
    groove_NodeId node = output;
    jz_err e = JZ_OK;
    while (node.v != ordering.v) {
        const groove_GraphNode *current = graph_node(g, node);
        if (!current) { e = groove_err_node_not_found(node); goto fail; }
        if (current->descriptor.inputs.len != 1) goto none;
        groove_NodeId input = current->descriptor.inputs.ptr[0];
        switch (current->descriptor.op.tag) {
        case GROOVE_OP_MAP_PROJECT: case GROOVE_OP_FILTER: break;
        default: goto none;
        }
        JZ_VEC_PUSH(chain, node);
        node = input;
    }
    const groove_GraphNode *top_node = graph_node(g, ordering);
    if (!top_node) { e = groove_err_node_not_found(ordering); goto fail; }
    usize width = desc_len(groove_NodeOutput_records(top_node->descriptor.output));
    if (top_node->descriptor.op.tag != GROOVE_OP_TOP_BY) goto none;
    const groove_TopByOp *top_by = &top_node->descriptor.op.v.top_by;
    /* chain.reverse() */
    for (u32 i = 0, j = chain.len ? chain.len - 1 : 0; i < j; i++, j--) {
        groove_NodeId t = chain.ptr[i]; chain.ptr[i] = chain.ptr[j]; chain.ptr[j] = t;
    }
    bool filtered;
    e = chain_filters_within_groups(g, chain.ptr, chain.len, top_by, width, &filtered);
    if (JZ_IS_ERR(e)) goto fail;
    usize group_len = top_by->group_field_indices.len;
    groove_UsizeVec identity;
    RP(top_by_identity_fields)(top_by, width, &identity);
    bool mapped_some; groove_UsizeVec mapped;
    e = map_through_projections(g, chain.ptr, chain.len, identity, &mapped_some, &mapped);
    if (JZ_IS_ERR(e)) goto fail;
    if (mapped_some) {
        out->fields = mapped; out->group_len = group_len; out->chain = chain;
        out->filtered = filtered; out->projected = false;
        *some = true;
        return JZ_OK;
    }
    groove_UsizeVec groups = {0};
    jz_vec_extend(&groups, top_by->group_field_indices.ptr, top_by->group_field_indices.len, sizeof(usize));
    groove_UsizeVec fields;
    e = map_through_projections(g, chain.ptr, chain.len, groups, &mapped_some, &fields);
    if (JZ_IS_ERR(e)) goto fail;
    if (!mapped_some) goto none;
    const groove_GraphNode *out_node = graph_node(g, output);
    if (!out_node) { jz_vec_drop(&fields, sizeof(usize), NULL); e = groove_err_node_not_found(output); goto fail; }
    usize output_width = desc_len(groove_NodeOutput_records(out_node->descriptor.output));
    for (usize index = 0; index < output_width; index++) {
        if (!usize_contains(&fields, index)) JZ_VEC_PUSH(fields, index);
    }
    out->fields = fields; out->group_len = group_len; out->chain = chain;
    out->filtered = filtered; out->projected = true;
    *some = true;
    return JZ_OK;
none:
    jz_vec_drop(&chain, sizeof(groove_NodeId), NULL);
    return JZ_OK;
fail:
    jz_vec_drop(&chain, sizeof(groove_NodeId), NULL);
    return e;
}

/* Whether a filter in `chain` can drop some rows of a group but keep others. */
static jz_err chain_filters_within_groups(const groove_IvmGraph *g, const groove_NodeId *chain, u32 n, const groove_TopByOp *top_by, usize width, bool *out) {
    /* For each field of the current record, the TopBy field it copies (-1 = None). */
    JZ_VEC(isize) sources = {0};
    for (usize i = 0; i < width; i++) { isize v = (isize)i; jz_vec_push(&sources, &v, sizeof(isize)); }
    jz_err e = JZ_OK;
    *out = false;
    for (u32 c = 0; c < n; c++) {
        groove_NodeId id = chain[c];
        const groove_GraphNode *current = graph_node(g, id);
        if (!current) { e = groove_err_node_not_found(id); goto done; }
        groove_NodeId input = current->descriptor.inputs.ptr[0];
        const groove_GraphNode *in_node = graph_node(g, input);
        if (!in_node) { e = groove_err_node_not_found(input); goto done; }
        groove_RecordDescriptor input_output = groove_NodeOutput_records(in_node->descriptor.output);
        switch (current->descriptor.op.tag) {
        case GROOVE_OP_FILTER: {
            jz_btree referenced; jz_btree_init(&referenced, &groove_ivm_rt_a_desc_str_set);
            groove__ivm__runtime__subscriptions__PredicateExpr__referenced_fields(&current->descriptor.op.v.filter.predicate, &referenced);
            bool within_group = true;
            jz_str *field; void *unused;
            JZ_BTREE_FOR(&referenced, field, unused) {
                usize index;
                bool ok = RP(resolve_field_name)(input_output, field->ptr, field->len, &index)
                    && index < sources.len && sources.ptr[index] >= 0
                    && usize_contains(&top_by->group_field_indices, (usize)sources.ptr[index]);
                if (!ok) { within_group = false; break; }
            }
            jz_btree_drop(&referenced);
            if (!within_group) { *out = true; goto done; }
            break;
        }
        case GROOVE_OP_MAP_PROJECT: {
            const groove_MapProjectOp *project = &current->descriptor.op.v.map_project;
            JZ_VEC(isize) next = {0};
            for (u32 i = 0; i < project->expressions.len; i++) {
                const groove_ProjectExpr *expr = &project->expressions.ptr[i].expression;
                isize v = -1;
                if (expr->tag == GROOVE_PX_FIELD) {
                    usize index;
                    jz_err r = RP(resolve_field_ref)(input_output, &expr->source, &index);
                    if (!JZ_IS_ERR(r)) { if (index < sources.len) v = sources.ptr[index]; }
                    else jz_err_drop(&r);
                }
                jz_vec_push(&next, &v, sizeof(isize));
            }
            jz_vec_drop(&sources, sizeof(isize), NULL);
            sources.ptr = next.ptr; sources.len = next.len; sources.cap = next.cap;
            break;
        }
        default:
            e = groove_err_unsupported_operator();
            goto done;
        }
    }
done:
    jz_vec_drop(&sources, sizeof(isize), NULL);
    return e;
}

/* Positions of TopBy output `fields` in the output of `chain`, or None when a
 * projection does not copy one of them. Consumes `fields`. */
static jz_err map_through_projections(const groove_IvmGraph *g, const groove_NodeId *chain, u32 n, groove_UsizeVec fields, bool *some, groove_UsizeVec *out) {
    *some = false;
    for (u32 c = 0; c < n; c++) {
        groove_NodeId id = chain[c];
        const groove_GraphNode *current = graph_node(g, id);
        if (!current) { jz_vec_drop(&fields, sizeof(usize), NULL); return groove_err_node_not_found(id); }
        if (current->descriptor.op.tag != GROOVE_OP_MAP_PROJECT) continue;
        const groove_MapProjectOp *project = &current->descriptor.op.v.map_project;
        groove_NodeId input = current->descriptor.inputs.ptr[0];
        const groove_GraphNode *in_node = graph_node(g, input);
        if (!in_node) { jz_vec_drop(&fields, sizeof(usize), NULL); return groove_err_node_not_found(input); }
        groove_RecordDescriptor input_output = groove_NodeOutput_records(in_node->descriptor.output);
        groove_UsizeVec mapped = {0};
        jz_vec_reserve(&mapped, fields.len, sizeof(usize));
        for (u32 f = 0; f < fields.len; f++) {
            isize position = -1;
            for (u32 i = 0; i < project->expressions.len; i++) {
                const groove_ProjectExpr *expr = &project->expressions.ptr[i].expression;
                if (expr->tag != GROOVE_PX_FIELD) continue;
                usize index;
                jz_err r = RP(resolve_field_ref)(input_output, &expr->source, &index);
                if (JZ_IS_ERR(r)) { jz_err_drop(&r); continue; }
                if (index == fields.ptr[f]) { position = (isize)i; break; }
            }
            if (position < 0) {
                jz_vec_drop(&mapped, sizeof(usize), NULL);
                jz_vec_drop(&fields, sizeof(usize), NULL);
                return JZ_OK;
            }
            usize p = (usize)position;
            JZ_VEC_PUSH(mapped, p);
        }
        jz_vec_drop(&fields, sizeof(usize), NULL);
        fields = mapped;
    }
    *some = true;
    *out = fields;
    return JZ_OK;
}

/* Runs a TopBy window record through `chain`. */
jz_err RP(window_record_reaches_output)(const groove_IvmGraph *g, const groove_NodeId *chain, u32 n, groove_RecordDescriptor descriptor, const jz_bytes *record, u32 *out) {
    groove_RecordDeltas current = groove_RecordDeltas_empty(descriptor);
    groove_RecordDelta first = { jz_bytes_clone(record), 1 };
    JZ_VEC_PUSH(current.deltas, first);
    bool unknown = false;
    jz_err e = JZ_OK;
    for (u32 c = 0; c < n; c++) {
        groove_NodeId id = chain[c];
        const groove_GraphNode *node = graph_node(g, id);
        if (!node) { e = groove_err_node_not_found(id); goto done; }
        if (current.deltas.len == 0) { *out = GROOVE_WR_NO; goto done; }
        const groove_RecordDelta *delta = &current.deltas.ptr[0];
        switch (node->descriptor.op.tag) {
        case GROOVE_OP_FILTER: {
            const groove_FilterOp *filter = &node->descriptor.op.v.filter;
            jz_btree referenced; jz_btree_init(&referenced, &groove_ivm_rt_a_desc_str_set);
            groove__ivm__runtime__subscriptions__PredicateExpr__referenced_fields(&filter->predicate, &referenced);
            groove_UsizeVec indices = {0};
            jz_str *field; void *unused;
            JZ_BTREE_FOR(&referenced, field, unused) {
                usize index;
                if (!RP(resolve_field_name)(current.descriptor, field->ptr, field->len, &index)) {
                    e = groove_err_field_not_found(field->ptr, field->len);
                    break;
                }
                JZ_VEC_PUSH(indices, index);
            }
            jz_btree_drop(&referenced);
            if (JZ_IS_ERR(e)) { jz_vec_drop(&indices, sizeof(usize), NULL); goto done; }
            bool indirect;
            e = groove__records__RecordDescriptor__fields_contain_indirect_values(current.descriptor, delta->record.ptr, delta->record.len, indices.ptr, indices.len, &indirect);
            jz_vec_drop(&indices, sizeof(usize), NULL);
            if (JZ_IS_ERR(e)) goto done;
            if (indirect) {
                unknown = true;
            } else {
                bool matches;
                e = groove__ivm__runtime__subscriptions__PredicateExpr__matches_borrowed(&filter->predicate, groove_RecordDelta_borrowed(delta, current.descriptor), filter->comparison, &matches);
                if (JZ_IS_ERR(e)) goto done;
                if (!matches) { *out = GROOVE_WR_NO; goto done; }
            }
            break;
        }
        case GROOVE_OP_MAP_PROJECT: {
            groove_RecordDeltas next;
            e = groove__ivm__runtime__operator_updates__NodeState__update_map_project(&node->descriptor.op.v.map_project, groove_NodeOutput_records(node->descriptor.output), &current, NULL, false, &next);
            if (JZ_IS_ERR(e)) goto done;
            groove__ivm__runtime__subscriptions__RecordDeltas__drop(&current);
            current = next;
            break;
        }
        default:
            e = groove_err_unsupported_operator();
            goto done;
        }
    }
    if (current.deltas.len == 0) *out = GROOVE_WR_NO;
    else if (unknown) *out = GROOVE_WR_UNKNOWN;
    else *out = GROOVE_WR_YES;
done:
    groove__ivm__runtime__subscriptions__RecordDeltas__drop(&current);
    return e;
}

/* A TopBy window record carried through the projections of `chain`. */
jz_err RP(project_window_record)(const groove_IvmGraph *g, const groove_NodeId *chain, u32 n, groove_RecordDescriptor descriptor, const u8 *record, u32 len, bool *some, groove_RecordDescriptor *out_desc, jz_bytes *out) {
    groove_RecordDeltas current = groove_RecordDeltas_empty(descriptor);
    groove_RecordDelta first = { jz_bytes_from(record, len), 1 };
    JZ_VEC_PUSH(current.deltas, first);
    for (u32 c = 0; c < n; c++) {
        groove_NodeId id = chain[c];
        const groove_GraphNode *node = graph_node(g, id);
        if (!node) { groove__ivm__runtime__subscriptions__RecordDeltas__drop(&current); return groove_err_node_not_found(id); }
        switch (node->descriptor.op.tag) {
        case GROOVE_OP_FILTER: break;
        case GROOVE_OP_MAP_PROJECT: {
            groove_RecordDeltas next;
            jz_err e = groove__ivm__runtime__operator_updates__NodeState__update_map_project(&node->descriptor.op.v.map_project, groove_NodeOutput_records(node->descriptor.output), &current, NULL, false, &next);
            groove__ivm__runtime__subscriptions__RecordDeltas__drop(&current);
            if (JZ_IS_ERR(e)) return e;
            current = next;
            break;
        }
        default:
            groove__ivm__runtime__subscriptions__RecordDeltas__drop(&current);
            return groove_err_unsupported_operator();
        }
    }
    if (current.deltas.len == 0) {
        *some = false;
    } else {
        *some = true;
        *out_desc = current.descriptor;
        *out = current.deltas.ptr[0].record;
        current.deltas.ptr[0].record = (jz_bytes){0};
    }
    groove__ivm__runtime__subscriptions__RecordDeltas__drop(&current);
    return JZ_OK;
}

/* ---- root ordering ---- */

/* WindowRoot<'a> { Unchanged(&[u8]), Keyed(Vec<u8>) } */
typedef struct WindowRoot { bool keyed; jz_slice unchanged; jz_bytes key; } WindowRoot;
static bool window_root_eq(const WindowRoot *a, const WindowRoot *b) {
    if (a->keyed != b->keyed) return false;
    if (a->keyed) return jz_bytes_eq(&a->key, &b->key);
    return slice_eq(&a->unchanged, &b->unchanged);
}
static void window_root_drop(void *p) { WindowRoot *r = p; if (r->keyed) jz_bytes_drop(&r->key); }

static void window_records(const groove_WindowedRecord *w, u32 n, jz_hmap *out) {
    jz_hmap_init(out, &slice_set_desc);
    for (u32 i = 0; i < n; i++) {
        jz_slice s = jz_bytes_slice(&w[i].record);
        jz_hmap_insert(out, &s, NULL, NULL);
    }
}

static jz_err apply_group_window_ordering_by_scan(const groove_WindowedRecord *before, u32 nb, const groove_WindowedRecord *after, u32 na, groove_RootKeyOf key_of, groove_RecordDescriptor root_descriptor, groove_TerminalDeltas *terminal);

/* Collect (index, key_of(record)) for rows of `window` absent from `other`. */
static jz_err changed_rows(const groove_WindowedRecord *w, u32 n, const jz_hmap *other, groove_RootKeyOf key_of, groove_IndexedKeyVec *out) {
    groove_IndexedKeyVec v = {0};
    for (u32 i = 0; i < n; i++) {
        jz_slice s = jz_bytes_slice(&w[i].record);
        if (jz_hmap_get(other, &s)) continue;
        groove_IndexedKey k = { i, {0} };
        jz_err e = key_of.call(key_of.ctx, s.ptr, s.len, &k.key);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&v, sizeof(groove_IndexedKey), indexed_key_drop); return e; }
        JZ_VEC_PUSH(v, k);
    }
    *out = v;
    return JZ_OK;
}

/* Root ordering for one group's window (#3290). */
jz_err RP(apply_group_window_ordering)(const groove_WindowedRecord *before, u32 nb, const groove_WindowedRecord *after, u32 na, groove_RootKeyOf key_of, groove_RecordDescriptor root_descriptor, groove_TerminalDeltas *terminal) {
    jz_hmap before_records, after_records;
    window_records(before, nb, &before_records);
    window_records(after, na, &after_records);
    groove_GroupChanges changes = {0};
    jz_err e = changed_rows(before, nb, &after_records, key_of, &changes.removed);
    if (!JZ_IS_ERR(e)) e = changed_rows(after, na, &before_records, key_of, &changes.added);
    jz_hmap_drop(&before_records);
    jz_hmap_drop(&after_records);
    if (JZ_IS_ERR(e)) { RP(GroupChanges__drop)(&changes); return e; }
    bool applied = RP(apply_changed_group_ordering)(&changes, root_descriptor, terminal);
    RP(GroupChanges__drop)(&changes);
    if (applied) return JZ_OK;
    return apply_group_window_ordering_by_scan(before, nb, after, na, key_of, root_descriptor, terminal);
}

/* A changed row currently in the consumer's list. */
typedef struct FloatingRoot { jz_slice key; usize gap; bool settled; } FloatingRoot;
typedef JZ_VEC(FloatingRoot) FloatingVec;

static isize floating_position(const FloatingVec *f, jz_slice key) {
    for (u32 i = 0; i < f->len; i++) if (slice_eq(&f->ptr[i].key, &key)) return (isize)i;
    return -1;
}
static usize gap_of(const groove_IndexedKeyVec *added, usize slot) { return added->ptr[slot].index - slot; }
/* The list slot directly after after-row `slot`'s nearest settled predecessor. */
static usize placement(const FloatingVec *floating, const bool *settled, const groove_IndexedKeyVec *added, usize slot, usize *gap_out) {
    usize gap = gap_of(added, slot);
    usize previous = slot;
    while (previous > 0) {
        previous -= 1;
        if (gap_of(added, previous) != gap) break;
        if (settled[previous]) {
            isize at = floating_position(floating, jz_bytes_slice(&added->ptr[previous].key));
            if (at < 0) jz_panic("a settled row is in the list");
            *gap_out = gap;
            return (usize)at + 1;
        }
    }
    /* partition_point(|root| root.gap < gap) */
    u32 lo = 0, hi = floating->len;
    while (lo < hi) {
        u32 mid = lo + (hi - lo) / 2;
        if (floating->ptr[mid].gap < gap) lo = mid + 1; else hi = mid;
    }
    *gap_out = gap;
    return lo;
}

/* Places the changed rows of one group with O(changed^2) work (#3505). */
bool RP(apply_changed_group_ordering)(const groove_GroupChanges *changes, groove_RecordDescriptor root_descriptor, groove_TerminalDeltas *terminal) {
    jz_hmap target; jz_hmap_init(&target, &slice_usize_desc);
    jz_hmap departed; jz_hmap_init(&departed, &slice_set_desc);
    bool result = false;
    FloatingVec floating = {0};
    bool *settled = NULL;
    for (u32 slot = 0; slot < changes->added.len; slot++) {
        jz_slice k = jz_bytes_slice(&changes->added.ptr[slot].key);
        usize s = slot;
        if (jz_hmap_insert(&target, &k, &s, NULL)) goto out;
    }
    for (u32 i = 0; i < changes->removed.len; i++) {
        jz_slice k = jz_bytes_slice(&changes->removed.ptr[i].key);
        if (jz_hmap_get(&departed, &k)) goto out;
        jz_hmap_insert(&departed, &k, NULL, NULL);
    }
    if (target.len == 0 && departed.len == 0) { result = true; goto out; }
#define CHANGED(sl) (jz_hmap_get(&target, &(sl)) != NULL || jz_hmap_get(&departed, &(sl)) != NULL)
    for (u32 i = 0; i < terminal->operations.len; i++) {
        const groove_TerminalOperation *op = &terminal->operations.ptr[i];
        jz_slice rk = jz_bytes_slice(&op->root_key);
        if (op->path.len == 0 && CHANGED(rk) && op->edit.tag == GROOVE_TE_INSERT) {
            jz_slice k = jz_bytes_slice(&op->edit.key);
            if (!jz_hmap_get(&target, &k)) goto out;
        }
    }
    const groove_IndexedKeyVec *added = &changes->added;
    for (u32 rank = 0; rank < changes->removed.len; rank++) {
        FloatingRoot r = { jz_bytes_slice(&changes->removed.ptr[rank].key), changes->removed.ptr[rank].index - rank, false };
        jz_vec_push(&floating, &r, sizeof(FloatingRoot));
    }
    settled = jz_zalloc(added->len ? added->len : 1);
    u32 n_ops = terminal->operations.len;
    for (u32 i = 0; i < n_ops; i++) {
        groove_TerminalOperation *op = &terminal->operations.ptr[i];
        jz_slice rk = jz_bytes_slice(&op->root_key);
        if (op->path.len != 0 || !CHANGED(rk)) continue;
        switch (op->edit.tag) {
        case GROOVE_TE_INSERT: {
            jz_slice k = jz_bytes_slice(&op->edit.key);
            usize slot = *(usize *)jz_hmap_get(&target, &k);
            isize existing = floating_position(&floating, k);
            if (existing >= 0) jz_vec_remove(&floating, (u32)existing, NULL, sizeof(FloatingRoot));
            settled[slot] = false;
            usize gap;
            usize at = placement(&floating, settled, added, slot, &gap);
            FloatingRoot r = { jz_bytes_slice(&added->ptr[slot].key), gap, true };
            jz_vec_insert(&floating, (u32)at, &r, sizeof(FloatingRoot));
            settled[slot] = true;
            op->edit.index = gap + at;
            break;
        }
        case GROOVE_TE_REMOVE: {
            jz_slice k = jz_bytes_slice(&op->edit.key);
            isize existing = floating_position(&floating, k);
            if (existing >= 0) jz_vec_remove(&floating, (u32)existing, NULL, sizeof(FloatingRoot));
            usize *slot = jz_hmap_get(&target, &k);
            if (slot) settled[*slot] = false;
            break;
        }
        case GROOVE_TE_UPDATE: case GROOVE_TE_MOVE: break;
        }
    }
#undef CHANGED
    for (u32 slot = 0; slot < added->len; slot++) {
        if (settled[slot]) continue;
        jz_slice key = jz_bytes_slice(&added->ptr[slot].key);
        isize cur = floating_position(&floating, key);
        if (cur < 0) continue;
        usize current = (usize)cur;
        usize gap = gap_of(added, slot);
        usize ignored;
        usize after_predecessor = placement(&floating, settled, added, slot, &ignored);
        bool in_place = floating.ptr[current].gap == gap && current >= after_predecessor;
        if (in_place) {
            for (usize j = after_predecessor; j < current; j++) {
                if (floating.ptr[j].settled) { in_place = false; break; }
            }
        }
        if (in_place) {
            floating.ptr[current].settled = true;
            settled[slot] = true;
            continue;
        }
        jz_vec_remove(&floating, (u32)current, NULL, sizeof(FloatingRoot));
        usize at = placement(&floating, settled, added, slot, &gap);
        FloatingRoot r = { key, gap, true };
        jz_vec_insert(&floating, (u32)at, &r, sizeof(FloatingRoot));
        settled[slot] = true;
        groove_TerminalOperation op;
        memset(&op, 0, sizeof op);
        op.root_descriptor = root_descriptor;
        op.root_key = jz_bytes_from(key.ptr, key.len);
        op.edit.tag = GROOVE_TE_MOVE;
        op.edit.key = jz_bytes_from(key.ptr, key.len);
        op.edit.index = gap + at;
        jz_vec_push(&terminal->operations, &op, sizeof op);
    }
    result = true;
out:
    jz_vec_drop(&floating, sizeof(FloatingRoot), NULL);
    if (settled) jz_free(settled);
    jz_hmap_drop(&target);
    jz_hmap_drop(&departed);
    return result;
}

static jz_err window_root(jz_slice record, const jz_hmap *other, groove_RootKeyOf key_of, WindowRoot *out) {
    if (jz_hmap_get(other, &record)) {
        out->keyed = false; out->unchanged = record; out->key = (jz_bytes){0};
        return JZ_OK;
    }
    out->keyed = true;
    return key_of.call(key_of.ctx, record.ptr, record.len, &out->key);
}

typedef JZ_VEC(WindowRoot) WindowRootVec;
static isize root_position(const WindowRootVec *v, const WindowRoot *r) {
    for (u32 i = 0; i < v->len; i++) if (window_root_eq(&v->ptr[i], r)) return (isize)i;
    return -1;
}

/* The pre-#3505 placement: walks both complete windows and repairs the list. */
static jz_err apply_group_window_ordering_by_scan(const groove_WindowedRecord *before, u32 nb, const groove_WindowedRecord *after, u32 na, groove_RootKeyOf key_of, groove_RecordDescriptor root_descriptor, groove_TerminalDeltas *terminal) {
    jz_hmap before_records, after_records;
    window_records(before, nb, &before_records);
    window_records(after, na, &after_records);
    WindowRootVec current = {0}, desired = {0};
    JZ_VEC(jz_bytes) group_keys = {0};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < nb; i++) {
        WindowRoot r;
        e = window_root(jz_bytes_slice(&before[i].record), &after_records, key_of, &r);
        if (JZ_IS_ERR(e)) goto done;
        jz_vec_push(&current, &r, sizeof r);
    }
    for (u32 i = 0; i < na; i++) {
        WindowRoot r;
        e = window_root(jz_bytes_slice(&after[i].record), &before_records, key_of, &r);
        if (JZ_IS_ERR(e)) goto done;
        jz_vec_push(&desired, &r, sizeof r);
    }
    /* Only changed rows can carry an edit; they are exactly the keyed roots. */
    for (u32 i = 0; i < current.len; i++) if (current.ptr[i].keyed) { jz_bytes k = jz_bytes_clone(&current.ptr[i].key); JZ_VEC_PUSH(group_keys, k); }
    for (u32 i = 0; i < desired.len; i++) if (desired.ptr[i].keyed) { jz_bytes k = jz_bytes_clone(&desired.ptr[i].key); JZ_VEC_PUSH(group_keys, k); }
    for (u32 i = 0; i < terminal->operations.len; i++) {
        groove_TerminalOperation *op = &terminal->operations.ptr[i];
        if (op->path.len != 0) continue;
        bool in_group = false;
        for (u32 k = 0; k < group_keys.len; k++) if (jz_bytes_eq(&group_keys.ptr[k], &op->root_key)) { in_group = true; break; }
        if (!in_group) continue;
        switch (op->edit.tag) {
        case GROOVE_TE_INSERT: {
            WindowRoot root = { true, {0}, jz_bytes_clone(&op->edit.key) };
            isize actual = root_position(&desired, &root);
            if (actual >= 0) op->edit.index = (usize)actual;
            isize existing = root_position(&current, &root);
            if (existing >= 0) { WindowRoot old; jz_vec_remove(&current, (u32)existing, &old, sizeof old); window_root_drop(&old); }
            usize at = op->edit.index < current.len ? op->edit.index : current.len;
            jz_vec_insert(&current, (u32)at, &root, sizeof root);
            break;
        }
        case GROOVE_TE_REMOVE: {
            WindowRoot root = { true, {0}, op->edit.key };
            isize existing = root_position(&current, &root);
            if (existing >= 0) { WindowRoot old; jz_vec_remove(&current, (u32)existing, &old, sizeof old); window_root_drop(&old); }
            break;
        }
        case GROOVE_TE_UPDATE: case GROOVE_TE_MOVE: break;
        }
    }
    for (u32 after_index = 0; after_index < desired.len; after_index++) {
        const WindowRoot *root = &desired.ptr[after_index];
        if (after_index < current.len && window_root_eq(&current.ptr[after_index], root)) continue;
        isize existing = root_position(&current, root);
        if (existing < 0) continue;
        WindowRoot moved;
        jz_vec_remove(&current, (u32)existing, &moved, sizeof moved);
        u32 at = after_index < current.len ? after_index : current.len;
        jz_vec_insert(&current, at, &moved, sizeof moved);
        jz_bytes key;
        if (root->keyed) key = jz_bytes_clone(&root->key);
        else {
            e = key_of.call(key_of.ctx, root->unchanged.ptr, root->unchanged.len, &key);
            if (JZ_IS_ERR(e)) goto done;
        }
        groove_TerminalOperation op;
        memset(&op, 0, sizeof op);
        op.root_descriptor = root_descriptor;
        op.root_key = jz_bytes_clone(&key);
        op.edit.tag = GROOVE_TE_MOVE;
        op.edit.key = key;
        op.edit.index = after_index;
        jz_vec_push(&terminal->operations, &op, sizeof op);
    }
done:
    jz_vec_drop(&current, sizeof(WindowRoot), window_root_drop);
    jz_vec_drop(&desired, sizeof(WindowRoot), window_root_drop);
    jz_vec_drop(&group_keys, sizeof(jz_bytes), groove_ivm_rt_a_drop_bytes);
    jz_hmap_drop(&before_records);
    jz_hmap_drop(&after_records);
    return e;
}

/* ---- descriptors ---- */

static void value_type_clone(groove_ValueType *dst, const groove_ValueType *src) { groove__records__values__ValueType__clone(dst, src); }
static groove_ValueType value_type_boxed(u32 tag, groove_ValueType inner) {
    groove_ValueType t; t.tag = tag; t.v.boxed = jz_memdup(&inner, sizeof inner); return t;
}
static groove_ValueType value_type_record(groove_RecordDescriptor d) {
    groove_ValueType t; t.tag = GROOVE_VT_RECORD; t.v.record = jz_memdup(&d, sizeof d); return t;
}
static groove_DescriptorField descriptor_field_new(jz_str name, groove_ValueType vt) {
    groove_DescriptorField f; memset(&f, 0, sizeof f);
    f.has_name = true; f.name = name; f.value_type = vt;
    return f;
}
static void descriptor_field_drop(void *p) { groove__records__DescriptorField__drop(p); }
static void named_type_drop(void *p) { groove_NamedType *n = p; jz_str_drop(&n->name); groove__records__values__ValueType__drop(&n->value_type); }

jz_err RP(project_descriptor)(groove_RecordDescriptor input, const groove_ProjectField *fields, u32 n, groove_RecordDescriptor *out) {
    JZ_VEC(groove_DescriptorField) result = {0};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < n; i++) {
        const groove_ProjectField *pf = &fields[i];
        const groove_ProjectExpr *x = &pf->expression;
        groove_ValueType vt;
        usize index;
        switch (x->tag) {
        case GROOVE_PX_RECORD_FIELD: {
            e = RP(resolve_field_ref)(input, &x->source, &index);
            if (JZ_IS_ERR(e)) goto fail;
            value_type_clone(&vt, &input->fields.ptr[index].value_type);
            for (u32 p = 0; p < x->path.len; p++) {
                if (vt.tag != GROOVE_VT_RECORD) { groove__records__values__ValueType__drop(&vt); e = groove_err_unsupported_operator(); goto fail; }
                groove_RecordDescriptor rec = *vt.v.record;
                usize ri;
                if (!groove__records__RecordDescriptor__field_index(rec, x->path.ptr[p].ptr, x->path.ptr[p].len, &ri)) {
                    groove__records__values__ValueType__drop(&vt); e = groove_err_unsupported_operator(); goto fail;
                }
                groove_ValueType next; value_type_clone(&next, &rec->fields.ptr[ri].value_type);
                groove__records__values__ValueType__drop(&vt);
                vt = next;
            }
            break;
        }
        case GROOVE_PX_FIELD:
        case GROOVE_PX_ENUM_TAG_REMAP:
        case GROOVE_PX_ENUM_REMAP:
            e = RP(resolve_field_ref)(input, &x->source, &index);
            if (JZ_IS_ERR(e)) goto fail;
            if (index >= input->fields.len) { e = groove_err_field_oob(index); goto fail; }
            value_type_clone(&vt, &input->fields.ptr[index].value_type);
            break;
        case GROOVE_PX_LITERAL:
            if (!groove__ivm__op_types__LiteralValue__value_type(&x->value, &vt)) { e = groove_err_unsupported_operator(); goto fail; }
            break;
        case GROOVE_PX_TYPED_LITERAL: case GROOVE_PX_TEMPLATE_ARGUMENT: case GROOVE_PX_NULL:
        case GROOVE_PX_RECURSIVE_ENUM_REMAP:
            value_type_clone(&vt, &x->value_type);
            break;
        case GROOVE_PX_NULLABLE: {
            e = RP(resolve_field_ref)(input, &x->source, &index);
            if (JZ_IS_ERR(e)) goto fail;
            if (index >= input->fields.len) { e = groove_err_field_oob(index); goto fail; }
            groove_ValueType inner; value_type_clone(&inner, &input->fields.ptr[index].value_type);
            vt = value_type_boxed(GROOVE_VT_NULLABLE, inner);
            break;
        }
        case GROOVE_PX_NULLABLE_FLAT: {
            e = RP(resolve_field_ref)(input, &x->source, &index);
            if (JZ_IS_ERR(e)) goto fail;
            if (index >= input->fields.len) { e = groove_err_field_oob(index); goto fail; }
            groove_ValueType inner; value_type_clone(&inner, &input->fields.ptr[index].value_type);
            vt = inner.tag == GROOVE_VT_NULLABLE ? inner : value_type_boxed(GROOVE_VT_NULLABLE, inner);
            break;
        }
        default: JZ_UNREACHABLE();
        }
        groove_DescriptorField f = descriptor_field_new(jz_str_clone(&pf->output_name), vt);
        f.has_identity = true;
        groove__records__FieldIdentity__clone(&f.identity, &pf->output_identity);
        jz_vec_push(&result, &f, sizeof f);
    }
    *out = groove__records__RecordDescriptor__new_with_fields(result.ptr, result.len);
    jz_free(result.ptr);
    return JZ_OK;
fail:
    jz_vec_drop(&result, sizeof(groove_DescriptorField), descriptor_field_drop);
    return e;
}

static jz_err collect_by_field_value_type(groove_RecordDescriptor input, usize index, const groove_CollectByField *field, groove_ValueType *out) {
    groove_ValueType vt; value_type_clone(&vt, &input->fields.ptr[index].value_type);
    if (!field->unwrap_nullable) { *out = vt; return JZ_OK; }
    if (vt.tag == GROOVE_VT_NULLABLE) {
        groove_ValueType *inner = vt.v.boxed;
        *out = *inner;
        jz_free(inner);
    } else {
        *out = vt;
    }
    return JZ_OK;
}

typedef JZ_VEC(groove_NamedType) NamedVec;

static jz_err push_collect_fields(groove_RecordDescriptor input, const groove_CollectByField *fields, u32 n, jz_hmap *names, const jz_str *reserved, const char *dup_msg, NamedVec *output) {
    for (u32 i = 0; i < n; i++) {
        const groove_CollectByField *field = &fields[i];
        jz_str name = jz_str_clone(&field->output_name);
        bool dup = jz_hmap_insert(names, &name, NULL, NULL);
        if (dup || (reserved && jz_str_eq(&field->output_name, reserved))) return groove_err_invalid_collect_by(dup_msg);
        usize index;
        JZ_TRY(RP(resolve_field_ref)(input, &field->field, &index));
        groove_NamedType nt = { jz_str_clone(&field->output_name), {0} };
        jz_err e = collect_by_field_value_type(input, index, field, &nt.value_type);
        if (JZ_IS_ERR(e)) { jz_str_drop(&nt.name); return e; }
        jz_vec_push(output, &nt, sizeof nt);
    }
    return JZ_OK;
}

jz_err RP(collect_by_descriptor)(groove_RecordDescriptor input, const groove_CollectByField *parent, u32 np, const groove_CollectByField *child, u32 nc, const jz_str *collection_field, groove_RecordDescriptor *out) {
    if (np == 0 || nc == 0 || collection_field->len == 0)
        return groove_err_invalid_collect_by("parent projection, child projection, and collection slot are required");
    jz_hmap names; jz_hmap_init(&names, &str_set_desc);
    NamedVec output = {0}, childv = {0};
    jz_vec_reserve(&output, np + 1, sizeof(groove_NamedType));
    jz_err e = push_collect_fields(input, parent, np, &names, collection_field, "output field names must be unique", &output);
    if (JZ_IS_ERR(e)) goto fail;
    jz_hmap child_names; jz_hmap_init(&child_names, &str_set_desc);
    e = push_collect_fields(input, child, nc, &child_names, NULL, "child output field names must be unique", &childv);
    jz_hmap_drop(&child_names);
    if (JZ_IS_ERR(e)) goto fail;
    groove_RecordDescriptor child_desc = groove__records__RecordDescriptor__new(childv.ptr, childv.len);
    jz_free(childv.ptr); childv = (NamedVec){0};
    groove_NamedType coll = { jz_str_clone(collection_field), value_type_boxed(GROOVE_VT_ARRAY, value_type_record(child_desc)) };
    jz_vec_push(&output, &coll, sizeof coll);
    *out = groove__records__RecordDescriptor__new(output.ptr, output.len);
    jz_free(output.ptr);
    jz_hmap_drop(&names);
    return JZ_OK;
fail:
    jz_hmap_drop(&names);
    jz_vec_drop(&output, sizeof(groove_NamedType), named_type_drop);
    jz_vec_drop(&childv, sizeof(groove_NamedType), named_type_drop);
    return e;
}

jz_err RP(collect_by_root_descriptor)(groove_RecordDescriptor input, const groove_CollectByField *parent, u32 np, groove_RecordDescriptor *out) {
    if (np == 0) return groove_err_invalid_collect_by("root collect requires a parent projection");
    JZ_VEC(groove_DescriptorField) fields = {0};
    for (u32 i = 0; i < np; i++) {
        usize index;
        jz_err e = RP(resolve_field_ref)(input, &parent[i].field, &index);
        groove_ValueType vt;
        if (!JZ_IS_ERR(e)) e = collect_by_field_value_type(input, index, &parent[i], &vt);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&fields, sizeof(groove_DescriptorField), descriptor_field_drop); return e; }
        groove_DescriptorField f = descriptor_field_new(jz_str_clone(&parent[i].output_name), vt);
        jz_vec_push(&fields, &f, sizeof f);
    }
    *out = groove__records__RecordDescriptor__new_with_fields(fields.ptr, fields.len);
    jz_free(fields.ptr);
    return JZ_OK;
}

#define MAX_COLLECT_BY_TREE_DEPTH 16

static jz_err collect_by_slot_descriptor(groove_RecordDescriptor input, const groove_CollectBySlotBuilder *slot, usize depth, groove_RecordDescriptor *out) {
    if (depth > MAX_COLLECT_BY_TREE_DEPTH)
        return jz_err_fmt(GROOVE_IVM_ERR_INVALID_COLLECT_BY, "invalid collect_by descriptor: tree collect depth exceeds MAX_COLLECT_BY_TREE_DEPTH (%d)", MAX_COLLECT_BY_TREE_DEPTH);
    if (slot->group_cols.len == 0 || slot->child_fields.len == 0 || slot->collection_field.len == 0 ||
        slot->order_cols.len == 0 || slot->tie_cols.len == 0)
        return groove_err_invalid_collect_by("each tree collection slot requires group, child, name, order, and tie fields");
    jz_hmap names; jz_hmap_init(&names, &str_set_desc);
    NamedVec output = {0};
    jz_vec_reserve(&output, slot->child_fields.len + slot->slots.len, sizeof(groove_NamedType));
    jz_err e = push_collect_fields(input, slot->child_fields.ptr, slot->child_fields.len, &names, NULL, "child output field names must be unique", &output);
    if (JZ_IS_ERR(e)) goto fail;
    for (u32 i = 0; i < slot->slots.len; i++) {
        const groove_CollectBySlotBuilder *nested = &slot->slots.ptr[i];
        jz_str name = jz_str_clone(&nested->collection_field);
        if (jz_hmap_insert(&names, &name, NULL, NULL)) { e = groove_err_invalid_collect_by("child output field names must be unique"); goto fail; }
        groove_RecordDescriptor nd;
        e = collect_by_slot_descriptor(input, nested, depth + 1, &nd);
        if (JZ_IS_ERR(e)) goto fail;
        groove_NamedType nt = { jz_str_clone(&nested->collection_field), value_type_boxed(GROOVE_VT_ARRAY, value_type_record(nd)) };
        jz_vec_push(&output, &nt, sizeof nt);
    }
    *out = groove__records__RecordDescriptor__new(output.ptr, output.len);
    jz_free(output.ptr);
    jz_hmap_drop(&names);
    return JZ_OK;
fail:
    jz_hmap_drop(&names);
    jz_vec_drop(&output, sizeof(groove_NamedType), named_type_drop);
    return e;
}

jz_err RP(collect_by_tree_descriptor)(groove_RecordDescriptor input, const groove_CollectByField *parent, u32 np, const groove_CollectBySlotBuilder *slots, u32 ns, groove_RecordDescriptor *out) {
    if (np == 0 || ns == 0)
        return groove_err_invalid_collect_by("tree collect requires a parent projection and at least one collection slot");
    jz_hmap names; jz_hmap_init(&names, &str_set_desc);
    NamedVec output = {0};
    jz_vec_reserve(&output, np + ns, sizeof(groove_NamedType));
    jz_err e = push_collect_fields(input, parent, np, &names, NULL, "output field names must be unique", &output);
    if (JZ_IS_ERR(e)) goto fail;
    for (u32 i = 0; i < ns; i++) {
        jz_str name = jz_str_clone(&slots[i].collection_field);
        if (jz_hmap_insert(&names, &name, NULL, NULL)) { e = groove_err_invalid_collect_by("output field names must be unique"); goto fail; }
        groove_RecordDescriptor sd;
        e = collect_by_slot_descriptor(input, &slots[i], 1, &sd);
        if (JZ_IS_ERR(e)) goto fail;
        groove_NamedType nt = { jz_str_clone(&slots[i].collection_field), value_type_boxed(GROOVE_VT_ARRAY, value_type_record(sd)) };
        jz_vec_push(&output, &nt, sizeof nt);
    }
    *out = groove__records__RecordDescriptor__new(output.ptr, output.len);
    jz_free(output.ptr);
    jz_hmap_drop(&names);
    return JZ_OK;
fail:
    jz_hmap_drop(&names);
    jz_vec_drop(&output, sizeof(groove_NamedType), named_type_drop);
    return e;
}

static void collect_by_projection_drop(void *p) { groove_CollectByProjection *c = p; jz_str_drop(&c->field); jz_str_drop(&c->output_name); }
static void order_field_drop(void *p) { jz_str_drop(&((groove_TopByOrderField *)p)->field); }
static void collect_by_slot_drop(void *p) {
    groove_CollectBySlot *s = p;
    jz_vec_drop(&s->group_fields, sizeof(jz_str), groove_ivm_rt_a_drop_str);
    jz_vec_drop(&s->group_field_indices, sizeof(usize), NULL);
    jz_vec_drop(&s->child_fields, sizeof(groove_CollectByProjection), collect_by_projection_drop);
    jz_str_drop(&s->collection_field);
    jz_vec_drop(&s->slots, sizeof(groove_CollectBySlot), collect_by_slot_drop);
    jz_vec_drop(&s->order_fields, sizeof(groove_TopByOrderField), order_field_drop);
    jz_vec_drop(&s->tie_fields, sizeof(jz_str), groove_ivm_rt_a_drop_str);
    jz_vec_drop(&s->sort_field_indices, sizeof(usize), NULL);
    jz_vec_drop(&s->sort_directions, sizeof(u8), NULL);
}

static jz_err resolve_refs(groove_RecordDescriptor input, const groove_FieldRef *refs, u32 n, groove_UsizeVec *out) {
    groove_UsizeVec v = {0};
    for (u32 i = 0; i < n; i++) {
        usize idx;
        jz_err e = RP(resolve_field_ref)(input, &refs[i], &idx);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&v, sizeof(usize), NULL); return e; }
        JZ_VEC_PUSH(v, idx);
    }
    *out = v;
    return JZ_OK;
}
static jz_err names_at(groove_RecordDescriptor input, const usize *idx, u32 n, groove_StrVec *out) {
    groove_StrVec v = {0};
    for (u32 i = 0; i < n; i++) {
        jz_str s;
        jz_err e = RP(field_name_at)(input, idx[i], &s);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&v, sizeof(jz_str), groove_ivm_rt_a_drop_str); return e; }
        JZ_VEC_PUSH(v, s);
    }
    *out = v;
    return JZ_OK;
}

jz_err RP(collect_by_slots)(groove_RecordDescriptor input, const groove_CollectBySlotBuilder *builders, u32 nb, const usize *owner_indices, u32 nowner, usize depth, groove_CollectBySlotVec *out) {
    if (depth > MAX_COLLECT_BY_TREE_DEPTH)
        return jz_err_fmt(GROOVE_IVM_ERR_INVALID_COLLECT_BY, "invalid collect_by descriptor: tree collect depth exceeds MAX_COLLECT_BY_TREE_DEPTH (%d)", MAX_COLLECT_BY_TREE_DEPTH);
    jz_hmap names; jz_hmap_init(&names, &str_set_desc);
    groove_CollectBySlotVec result = {0};
    jz_err e = JZ_OK;
    groove_UsizeVec group = {0}, order = {0}, tie = {0}, owners = {0};
    groove_CollectByProjectionVec child_fields = {0};
    for (u32 slot_index = 0; slot_index < nb; slot_index++) {
        const groove_CollectBySlotBuilder *b = &builders[slot_index];
        group = (groove_UsizeVec){0}; order = (groove_UsizeVec){0}; tie = (groove_UsizeVec){0}; owners = (groove_UsizeVec){0};
        child_fields.ptr = NULL; child_fields.len = child_fields.cap = 0;
        jz_str name = jz_str_clone(&b->collection_field);
        if (jz_hmap_insert(&names, &name, NULL, NULL)) { e = groove_err_invalid_collect_by("sibling collection slot names must be unique"); goto fail; }
        e = resolve_refs(input, b->group_cols.ptr, b->group_cols.len, &group);
        if (JZ_IS_ERR(e)) goto fail;
        bool unavailable = group.len == 0;
        for (u32 i = 0; i < group.len && !unavailable; i++) {
            bool found = false;
            for (u32 j = 0; j < nowner; j++) if (owner_indices[j] == group.ptr[i]) { found = true; break; }
            if (!found) unavailable = true;
        }
        if (unavailable) { e = groove_err_invalid_collect_by("a slot group must be available on its owning record"); goto fail; }
        e = RP(validate_collect_by_key_types)(input, group.ptr, group.len);
        if (JZ_IS_ERR(e)) goto fail;
        e = RP(collect_by_projections)(input, b->child_fields.ptr, b->child_fields.len, (void *)&child_fields);
        if (JZ_IS_ERR(e)) goto fail;
        for (u32 i = 0; i < b->order_cols.len; i++) {
            usize idx;
            e = RP(resolve_field_ref)(input, &b->order_cols.ptr[i].field, &idx);
            if (JZ_IS_ERR(e)) goto fail;
            JZ_VEC_PUSH(order, idx);
        }
        e = resolve_refs(input, b->tie_cols.ptr, b->tie_cols.len, &tie);
        if (JZ_IS_ERR(e)) goto fail;
        bool has_presence = false; usize presence = 0;
        if (b->has_presence_col) {
            e = RP(resolve_field_ref)(input, &b->presence_col, &presence);
            if (JZ_IS_ERR(e)) goto fail;
            has_presence = true;
        }
        if (has_presence && input->fields.ptr[presence].value_type.tag != GROOVE_VT_BOOL) {
            e = groove_err_invalid_collect_by("a tree collection presence field must be boolean"); goto fail;
        }
        if (order.len == 0 || tie.len == 0) { e = groove_err_invalid_collect_by("order and tie fields must both be complete and non-empty"); goto fail; }
        e = RP(validate_collect_by_key_types)(input, order.ptr, order.len);
        if (JZ_IS_ERR(e)) goto fail;
        e = RP(validate_collect_by_key_types)(input, tie.ptr, tie.len);
        if (JZ_IS_ERR(e)) goto fail;
        bool has_ref = false; usize ref_index = 0;
        if (b->has_reference_array_col) {
            e = RP(resolve_field_ref)(input, &b->reference_array_col, &ref_index);
            if (JZ_IS_ERR(e)) goto fail;
            has_ref = true;
            const groove_ValueType *ty = groove__records__values__ValueType__non_nullable(&input->fields.ptr[ref_index].value_type);
            if (!groove__records__values__ValueType__is_array_fk(ty)) { e = groove_err_invalid_collect_by("reference sequence must be a UUID array"); goto fail; }
        }
        groove_RecordDescriptor child_descriptor;
        e = collect_by_slot_descriptor(input, b, depth, &child_descriptor);
        if (JZ_IS_ERR(e)) goto fail;
        if (b->reference_order && !has_ref) { e = groove_err_invalid_collect_by("reference ordering requires a reference array"); goto fail; }
        if (has_ref && !(child_descriptor->fields.len > 0 && child_descriptor->fields.ptr[0].value_type.tag == GROOVE_VT_UUID)) {
            e = groove_err_invalid_collect_by("a reference collection must project its child UUID first"); goto fail;
        }
        for (u32 i = 0; i < child_fields.len; i++) JZ_VEC_PUSH(owners, child_fields.ptr[i].field_idx);
        for (u32 i = 0; i < b->owner_key_cols.len; i++) {
            usize ok;
            e = RP(resolve_field_ref)(input, &b->owner_key_cols.ptr[i], &ok);
            if (JZ_IS_ERR(e)) goto fail;
            if (!usize_contains(&group, ok)) { e = groove_err_invalid_collect_by("a slot owner key must also be a grouping field"); goto fail; }
            if (!usize_contains(&owners, ok)) JZ_VEC_PUSH(owners, ok);
        }
        groove_CollectBySlot slot; memset(&slot, 0, sizeof slot);
        e = RP(collect_by_slots)(input, b->slots.ptr, b->slots.len, owners.ptr, owners.len, depth + 1, (void *)&slot.slots);
        if (JZ_IS_ERR(e)) goto fail;
        e = names_at(input, group.ptr, group.len, &slot.group_fields);
        if (JZ_IS_ERR(e)) { collect_by_slot_drop(&slot); goto fail; }
        slot.group_field_indices = group; group = (groove_UsizeVec){0};
        slot.child_fields.ptr = child_fields.ptr; slot.child_fields.len = child_fields.len; slot.child_fields.cap = child_fields.cap;
        child_fields.ptr = NULL; child_fields.len = child_fields.cap = 0;
        slot.child_descriptor = child_descriptor;
        slot.collection_field = jz_str_clone(&b->collection_field);
        slot.collection_field_index = b->child_fields.len + slot_index;
        for (u32 i = 0; i < b->order_cols.len; i++) {
            groove_TopByOrderField of;
            e = RP(field_name_at)(input, order.ptr[i], &of.field);
            if (JZ_IS_ERR(e)) { collect_by_slot_drop(&slot); goto fail; }
            of.direction = b->order_cols.ptr[i].direction;
            jz_vec_push(&slot.order_fields, &of, sizeof of);
        }
        e = names_at(input, tie.ptr, tie.len, &slot.tie_fields);
        if (JZ_IS_ERR(e)) { collect_by_slot_drop(&slot); goto fail; }
        slot.has_presence_field_index = has_presence; slot.presence_field_index = presence;
        slot.has_reference_array_field_index = has_ref; slot.reference_array_field_index = ref_index;
        slot.reference_order = b->reference_order;
        jz_vec_extend(&slot.sort_field_indices, order.ptr, order.len, sizeof(usize));
        jz_vec_extend(&slot.sort_field_indices, tie.ptr, tie.len, sizeof(usize));
        for (u32 i = 0; i < b->order_cols.len; i++) { u8 d = (u8)b->order_cols.ptr[i].direction; JZ_VEC_PUSH(slot.sort_directions, d); }
        for (u32 i = 0; i < tie.len; i++) { u8 d = GROOVE_DIR_ASC; JZ_VEC_PUSH(slot.sort_directions, d); }
        slot.offset = b->offset;
        slot.limit = b->limit;
        jz_vec_push(&result, &slot, sizeof slot);
        jz_vec_drop(&order, sizeof(usize), NULL);
        jz_vec_drop(&tie, sizeof(usize), NULL);
        jz_vec_drop(&owners, sizeof(usize), NULL);
    }
    jz_hmap_drop(&names);
    out->ptr = result.ptr; out->len = result.len; out->cap = result.cap;
    return JZ_OK;
fail:
    jz_vec_drop(&group, sizeof(usize), NULL);
    jz_vec_drop(&order, sizeof(usize), NULL);
    jz_vec_drop(&tie, sizeof(usize), NULL);
    jz_vec_drop(&owners, sizeof(usize), NULL);
    jz_vec_drop(&child_fields, sizeof(groove_CollectByProjection), collect_by_projection_drop);
    jz_vec_drop(&result, sizeof(groove_CollectBySlot), collect_by_slot_drop);
    jz_hmap_drop(&names);
    return e;
}

jz_err RP(collect_by_expand_descriptor)(groove_RecordDescriptor input, const groove_CollectByField *tuple, u32 n, groove_RecordDescriptor *out) {
    if (n == 0) return groove_err_invalid_collect_by("expand mode requires a non-empty tuple projection");
    jz_hmap names; jz_hmap_init(&names, &str_set_desc);
    NamedVec output = {0};
    jz_err e = push_collect_fields(input, tuple, n, &names, NULL, "expand tuple output field names must be unique", &output);
    jz_hmap_drop(&names);
    if (JZ_IS_ERR(e)) { jz_vec_drop(&output, sizeof(groove_NamedType), named_type_drop); return e; }
    *out = groove__records__RecordDescriptor__new(output.ptr, output.len);
    jz_free(output.ptr);
    return JZ_OK;
}

jz_err RP(collect_by_projections)(groove_RecordDescriptor input, const groove_CollectByField *fields, u32 n, groove_CollectByProjectionVec *out) {
    groove_CollectByProjectionVec v = {0};
    for (u32 i = 0; i < n; i++) {
        groove_CollectByProjection p;
        jz_err e = RP(field_ref_name)(input, &fields[i].field, &p.field);
        if (!JZ_IS_ERR(e)) {
            e = RP(resolve_field_ref)(input, &fields[i].field, &p.field_idx);
            if (JZ_IS_ERR(e)) jz_str_drop(&p.field);
        }
        if (JZ_IS_ERR(e)) { jz_vec_drop(&v, sizeof(groove_CollectByProjection), collect_by_projection_drop); return e; }
        p.output_name = jz_str_clone(&fields[i].output_name);
        p.unwrap_nullable = fields[i].unwrap_nullable;
        jz_vec_push(&v, &p, sizeof p);
    }
    out->ptr = v.ptr; out->len = v.len; out->cap = v.cap;
    return JZ_OK;
}

jz_err RP(validate_collect_by_key_types)(groove_RecordDescriptor input, const usize *indices, u32 n) {
    for (u32 i = 0; i < n; i++) {
        usize index = indices[i];
        if (index >= input->fields.len) return groove_err_field_oob(index);
        const groove_ValueType *vt = &input->fields.ptr[index].value_type;
        bool scalar = groove__records__collect_by_ordered_scalar(vt);
        if (groove__records__values__ValueType__contains_record(vt) || !scalar)
            return groove_err_invalid_collect_by("group, order, and tie fields must be scalar ordered values without records");
    }
    return JZ_OK;
}

/* seen: HashMap<usize (node address), bool> */
static bool contains_child(const jz_hmap *seen, const groove_GraphBuilder *const *children, u32 n) {
    for (u32 i = 0; i < n; i++) {
        usize key = (usize)children[i];
        bool *v = jz_hmap_get(seen, &key);
        if (v && *v) return true;
    }
    return false;
}

jz_err RP(validate_collect_by_terminality)(const groove_GraphBuilder *graph) {
    jz_hmap contains_collect; jz_hmap_init(&contains_collect, &usize_bool_desc);
    groove_GraphBuilderRefVec order = {0};
    groove__ivm__graph__GraphBuilder__postorder(graph, &order);
    jz_err e = JZ_OK;
    for (u32 i = 0; i < order.len; i++) {
        const groove_GraphBuilder *node = order.ptr[i];
        bool children_contain_collect = false;
        const groove_GraphBuilder *kids[3];
        switch (node->tag) {
        case GROOVE_GB_TYPED_TEMPLATE:
        case GROOVE_GB_UNION:
            children_contain_collect = contains_child(&contains_collect, (const groove_GraphBuilder *const *)node->inputs.ptr, node->inputs.len);
            break;
        case GROOVE_GB_TEMPLATE_INPUT:
            if (node->input) { kids[0] = node->input; children_contain_collect = contains_child(&contains_collect, kids, 1); }
            break;
        case GROOVE_GB_FILTER: case GROOVE_GB_PROJECT: case GROOVE_GB_STREAMING_CHECKSUM:
        case GROOVE_GB_UNWRAP_NULLABLE: case GROOVE_GB_UNNEST: case GROOVE_GB_VARIANT_PROJECT:
        case GROOVE_GB_ARG_MAX_BY: case GROOVE_GB_ARG_MIN_BY: case GROOVE_GB_TOP_BY:
        case GROOVE_GB_COLLECT_BY: case GROOVE_GB_AGGREGATE: case GROOVE_GB_LOOKUP:
        case GROOVE_GB_RECURSIVE_STEP_WITNESS:
            kids[0] = node->input;
            children_contain_collect = contains_child(&contains_collect, kids, 1);
            break;
        case GROOVE_GB_JOIN: case GROOVE_GB_SEMI_JOIN: case GROOVE_GB_ANTI_JOIN:
            kids[0] = node->left; kids[1] = node->right;
            children_contain_collect = contains_child(&contains_collect, kids, 2);
            break;
        case GROOVE_GB_RECURSIVE:
            kids[0] = node->seed; kids[1] = node->step;
            children_contain_collect = contains_child(&contains_collect, kids, 2);
            if (!children_contain_collect && node->step_witness) {
                kids[0] = node->step_witness;
                children_contain_collect = contains_child(&contains_collect, kids, 1);
            }
            break;
        case GROOVE_GB_TABLE: case GROOVE_GB_INLINE_RECORDS: case GROOVE_GB_INPUT_SOURCE:
        case GROOVE_GB_INDEX: case GROOVE_GB_FRONTIER_SOURCE: case GROOVE_GB_BINDING_SOURCE:
            children_contain_collect = false;
            break;
        }
        if (children_contain_collect) {
            e = jz_err_new(GROOVE_IVM_ERR_COLLECT_BY_MUST_BE_TERMINAL, "collect_by is terminal-only and cannot feed another graph node");
            break;
        }
        usize key = (usize)node;
        bool v = node->tag == GROOVE_GB_COLLECT_BY ||
                 (node->tag == GROOVE_GB_TYPED_TEMPLATE && groove__ivm__template__TypedGraphTemplate__terminal(node->program));
        jz_hmap_insert(&contains_collect, &key, &v, NULL);
    }
    jz_free(order.ptr);
    jz_hmap_drop(&contains_collect);
    return e;
}

jz_err RP(project_field_expr)(groove_RecordDescriptor input, const groove_ProjectField *field, groove_ProjectExpr *out) {
    groove_ProjectExpr expression;
    groove__ivm__graph__ProjectExpr__clone(&expression, &field->expression);
    switch (expression.tag) {
    case GROOVE_PX_FIELD: case GROOVE_PX_RECORD_FIELD: case GROOVE_PX_NULLABLE: case GROOVE_PX_NULLABLE_FLAT:
    case GROOVE_PX_ENUM_TAG_REMAP: case GROOVE_PX_ENUM_REMAP: case GROOVE_PX_RECURSIVE_ENUM_REMAP: {
        usize index;
        jz_err e = RP(resolve_field_ref)(input, &expression.source, &index);
        if (JZ_IS_ERR(e)) { groove__ivm__graph__ProjectExpr__drop(&expression); return e; }
        if (expression.source.tag != GROOVE_FR_RESOLVED) jz_str_drop(&expression.source.name);
        expression.source.tag = GROOVE_FR_RESOLVED;
        expression.source.index = index;
        break;
    }
    case GROOVE_PX_LITERAL: case GROOVE_PX_TYPED_LITERAL: case GROOVE_PX_NULL: case GROOVE_PX_TEMPLATE_ARGUMENT:
        break;
    }
    *out = expression;
    return JZ_OK;
}

static void value_vec_drop(groove_ValueVec *v) { jz_vec_drop(v, sizeof(groove_Value), (void (*)(void *))groove__records__values__Value__drop); }

jz_err RP(project_record)(const groove_ProjectionExpr *exprs, u32 n, const usize (*mapping)[2], u32 nmap, groove_RecordDescriptor output_desc, groove_RecordDescriptor input_desc, const u8 *record, u32 len, jz_bytes *out) {
    if (RP(projection_uses_raw_copy)(exprs, n, nmap, output_desc)) {
        jz_slice rec = { record, len };
        return groove__records__RecordDescriptor__project_record_raw(output_desc, &input_desc, &rec, 1, mapping, nmap, out);
    }
    groove_ValueVec values = {0};
    jz_vec_reserve(&values, n, sizeof(groove_Value));
    for (u32 i = 0; i < n; i++) {
        groove_Value v;
        jz_err e = RP(project_field_value)(&exprs[i], i, output_desc, input_desc, record, len, &v);
        if (JZ_IS_ERR(e)) { value_vec_drop(&values); return e; }
        jz_vec_push(&values, &v, sizeof v);
    }
    jz_err e = groove__records__RecordDescriptor__create(output_desc, values.ptr, values.len, out);
    value_vec_drop(&values);
    return e;
}

static groove_Value value_nullable(groove_Value *inner) {
    groove_Value v; v.tag = GROOVE_V_NULLABLE; v.v.nullable = inner ? jz_memdup(inner, sizeof *inner) : NULL; return v;
}

jz_err RP(project_field_value)(const groove_ProjectionExpr *expr, usize output_idx, groove_RecordDescriptor output_desc, groove_RecordDescriptor input_desc, const u8 *record, u32 len, groove_Value *out) {
    groove_BorrowedRecord input = { record, len, input_desc };
    const groove_ProjectExpr *x = &expr->expression;
    usize source_idx;
    groove_Value value;
#define RESOLVED(field, dst) do { \
        JZ_TRY(RP(resolve_field_ref)(input_desc, (field), &source_idx)); \
        JZ_TRY(groove__records__BorrowedRecord__get_idx(&input, source_idx, (dst))); } while (0)
    switch (x->tag) {
    case GROOVE_PX_TEMPLATE_ARGUMENT:
        return groove_err_unsupported_operator();
    case GROOVE_PX_FIELD:
        RESOLVED(&x->source, out);
        return JZ_OK;
    case GROOVE_PX_RECORD_FIELD:
        RESOLVED(&x->source, &value);
        for (u32 p = 0; p < x->path.len; p++) {
            if (value.tag != GROOVE_V_RECORD) { groove__records__values__Value__drop(&value); return groove_err_unsupported_operator(); }
            groove_Value next;
            jz_err e = groove__records__OwnedRecord__get(&value.v.record, &x->path.ptr[p], &next);
            groove__records__values__Value__drop(&value);
            if (JZ_IS_ERR(e)) return e;
            value = next;
        }
        *out = value;
        return JZ_OK;
    case GROOVE_PX_LITERAL: case GROOVE_PX_TYPED_LITERAL:
        groove__ivm__op_types__LiteralValue__to_value(&x->value, out);
        return JZ_OK;
    case GROOVE_PX_NULL:
        *out = value_nullable(NULL);
        return JZ_OK;
    case GROOVE_PX_NULLABLE:
        RESOLVED(&x->source, &value);
        *out = value_nullable(&value);
        return JZ_OK;
    case GROOVE_PX_NULLABLE_FLAT:
        RESOLVED(&x->source, &value);
        *out = value.tag == GROOVE_V_NULLABLE ? value : value_nullable(&value);
        return JZ_OK;
    case GROOVE_PX_ENUM_TAG_REMAP:
        RESOLVED(&x->source, &value);
        return RP(remap_enum_tag)(value, x->tags.ptr, x->tags.len, out);
    case GROOVE_PX_ENUM_REMAP:
        RESOLVED(&x->source, &value);
        return RP(remap_enum)(value, x->tags.ptr, x->tags.len, out);
    case GROOVE_PX_RECURSIVE_ENUM_REMAP: {
        JZ_TRY(RP(resolve_field_ref)(input_desc, &x->source, &source_idx));
        usize resolved_idx = source_idx;
        RESOLVED(&x->source, &value);
        return RP(remap_recursive_enum_value)(value, &input_desc->fields.ptr[resolved_idx].value_type,
            &output_desc->fields.ptr[output_idx].value_type, &x->remaps, "root", 4, out);
    }
    }
#undef RESOLVED
    JZ_UNREACHABLE();
}

jz_err RP(remap_enum_tag)(groove_Value value, const i32 *tags, u32 n, groove_Value *out) {
    switch (value.tag) {
    case GROOVE_V_ENUM_TAG: {
        u8 tag = value.v.enum_tag;
        if (tag < n && tags[tag] >= 0) { out->tag = GROOVE_V_ENUM_TAG; out->v.enum_tag = (u8)tags[tag]; return JZ_OK; }
        return jz_err_fmt(GROOVE_IVM_ERR_ENUM_TAG_PROJECTION_ABSENT, "enum tag %u is absent from this projection target", (unsigned)tag);
    }
    case GROOVE_V_NULLABLE:
        if (!value.v.nullable) { *out = value_nullable(NULL); return JZ_OK; }
        {
            groove_Value inner = *value.v.nullable; jz_free(value.v.nullable);
            groove_Value mapped;
            JZ_TRY(RP(remap_enum_tag)(inner, tags, n, &mapped));
            *out = value_nullable(&mapped);
            return JZ_OK;
        }
    default:
        groove__records__values__Value__drop(&value);
        return jz_err_new(GROOVE_IVM_ERR_ENUM_TAG_PROJECTION_NON_ENUM, "enum tag projection requires an enum value");
    }
}

jz_err RP(remap_enum)(groove_Value value, const i32 *tags, u32 n, groove_Value *out) {
    switch (value.tag) {
    case GROOVE_V_ENUM: {
        u32 tag = value.v.enm.tag;
        if (tag >= n || tags[tag] < 0) {
            groove__records__values__Value__drop(&value);
            return jz_err_fmt(GROOVE_IVM_ERR_ENUM_PROJECTION_ABSENT, "payload enum tag %u is absent from this projection target", tag);
        }
        out->tag = GROOVE_V_ENUM;
        out->v.enm.tag = (u32)tags[tag];
        out->v.enm.record = value.v.enm.record;
        return JZ_OK;
    }
    case GROOVE_V_NULLABLE:
        if (!value.v.nullable) { *out = value_nullable(NULL); return JZ_OK; }
        {
            groove_Value inner = *value.v.nullable; jz_free(value.v.nullable);
            groove_Value mapped;
            JZ_TRY(RP(remap_enum)(inner, tags, n, &mapped));
            *out = value_nullable(&mapped);
            return JZ_OK;
        }
    default:
        groove__records__values__Value__drop(&value);
        return jz_err_new(GROOVE_IVM_ERR_ENUM_PROJECTION_NON_ENUM, "payload enum projection requires an enum value");
    }
}

static jz_err recursive_mismatch(const char *path, u32 len) {
    return jz_err_fmt(GROOVE_IVM_ERR_RECURSIVE_ENUM_PROJECTION_DESCRIPTOR_MISMATCH, "recursive enum projection descriptor mismatch at %.*s", (int)len, path);
}

static bool remap_path_with_enum_occurrence_below(const groove_RecursiveEnumRemaps *remaps, const jz_str *path) {
    jz_str below = jz_str_clone(path);
    jz_str_push_char(&below, '/');
    const jz_btree *maps[3] = { &remaps->scalar, &remaps->payload, &remaps->payload_children };
    bool found = false;
    for (int m = 0; m < 3 && !found; m++) {
        jz_str *candidate; void *unused;
        JZ_BTREE_FOR(maps[m], candidate, unused) {
            if (jz_str_eq(candidate, path) ||
                (candidate->len >= below.len && memcmp(candidate->ptr, below.ptr, below.len) == 0)) { found = true; break; }
        }
    }
    jz_str_drop(&below);
    return found;
}

/* tags vector stored in the remap maps: JZ_VEC(i32) with -1 = None */
typedef JZ_VEC(i32) I32Vec;
typedef struct OptStr { bool some; jz_str s; } OptStr;
typedef JZ_VEC(OptStr) OptStrVec;

static void *remap_lookup(const jz_btree *map, const char *path, u32 len) {
    jz_str key = { (char *)path, len, len };
    return jz_btree_get(map, &key);
}

jz_err RP(remap_recursive_enum_value)(groove_Value value, const groove_ValueType *source, const groove_ValueType *target, const groove_RecursiveEnumRemaps *remaps, const char *path, u32 path_len, groove_Value *out) {
    jz_err e = JZ_OK;
    if (value.tag == GROOVE_V_ENUM_TAG && source->tag == GROOVE_VT_ENUM_TAG && target->tag == GROOVE_VT_ENUM_TAG) {
        u8 tag = value.v.enum_tag;
        const I32Vec *tags = remap_lookup(&remaps->scalar, path, path_len);
        if (tags && tag < tags->len && tags->ptr[tag] >= 0) { out->tag = GROOVE_V_ENUM_TAG; out->v.enum_tag = (u8)tags->ptr[tag]; return JZ_OK; }
        return jz_err_fmt(GROOVE_IVM_ERR_ENUM_TAG_PROJECTION_ABSENT, "enum tag %u is absent from this projection target", (unsigned)tag);
    }
    if (value.tag == GROOVE_V_NULLABLE && !value.v.nullable && source->tag == GROOVE_VT_NULLABLE && target->tag == GROOVE_VT_NULLABLE) {
        *out = value_nullable(NULL);
        return JZ_OK;
    }
    if (value.tag == GROOVE_V_NULLABLE && value.v.nullable && source->tag == GROOVE_VT_NULLABLE && target->tag == GROOVE_VT_NULLABLE) {
        jz_str base = jz_str_from_slice(jz_slice_of(path, path_len));
        jz_str nullable_path = jz_format("%.*s/nullable", (int)path_len, path);
        bool use_nullable = remap_path_with_enum_occurrence_below(remaps, &nullable_path);
        groove_Value inner = *value.v.nullable; jz_free(value.v.nullable);
        groove_Value mapped;
        const jz_str *child = use_nullable ? &nullable_path : &base;
        e = RP(remap_recursive_enum_value)(inner, source->v.boxed, target->v.boxed, remaps, child->ptr, child->len, &mapped);
        jz_str_drop(&nullable_path); jz_str_drop(&base);
        if (JZ_IS_ERR(e)) return e;
        *out = value_nullable(&mapped);
        return JZ_OK;
    }
    if (value.tag == GROOVE_V_ARRAY && source->tag == GROOVE_VT_ARRAY && target->tag == GROOVE_VT_ARRAY) {
        groove_ValueVec result = {0};
        jz_str child = jz_format("%.*s/array", (int)path_len, path);
        for (u32 i = 0; i < value.v.list.len; i++) {
            groove_Value mapped;
            e = RP(remap_recursive_enum_value)(value.v.list.ptr[i], source->v.boxed, target->v.boxed, remaps, child.ptr, child.len, &mapped);
            if (JZ_IS_ERR(e)) {
                for (u32 j = i + 1; j < value.v.list.len; j++) groove__records__values__Value__drop(&value.v.list.ptr[j]);
                break;
            }
            jz_vec_push(&result, &mapped, sizeof mapped);
        }
        jz_free(value.v.list.ptr);
        jz_str_drop(&child);
        if (JZ_IS_ERR(e)) { value_vec_drop(&result); return e; }
        out->tag = GROOVE_V_ARRAY; out->v.list.ptr = result.ptr; out->v.list.len = result.len; out->v.list.cap = result.cap;
        return JZ_OK;
    }
    if (value.tag == GROOVE_V_TUPLE && source->tag == GROOVE_VT_TUPLE && target->tag == GROOVE_VT_TUPLE &&
        value.v.list.len == source->v.tuple.len && source->v.tuple.len == target->v.tuple.len) {
        groove_ValueVec result = {0};
        for (u32 i = 0; i < value.v.list.len; i++) {
            jz_str child = jz_format("%.*s/tuple/%u", (int)path_len, path, i);
            groove_Value mapped;
            e = RP(remap_recursive_enum_value)(value.v.list.ptr[i], &source->v.tuple.ptr[i], &target->v.tuple.ptr[i], remaps, child.ptr, child.len, &mapped);
            jz_str_drop(&child);
            if (JZ_IS_ERR(e)) {
                for (u32 j = i + 1; j < value.v.list.len; j++) groove__records__values__Value__drop(&value.v.list.ptr[j]);
                break;
            }
            jz_vec_push(&result, &mapped, sizeof mapped);
        }
        jz_free(value.v.list.ptr);
        if (JZ_IS_ERR(e)) { value_vec_drop(&result); return e; }
        out->tag = GROOVE_V_TUPLE; out->v.list.ptr = result.ptr; out->v.list.len = result.len; out->v.list.cap = result.cap;
        return JZ_OK;
    }
    if (value.tag == GROOVE_V_RECORD && source->tag == GROOVE_VT_RECORD && target->tag == GROOVE_VT_RECORD &&
        (*source->v.record)->fields.len == (*target->v.record)->fields.len) {
        groove_RecordDescriptor sd = *source->v.record, td = *target->v.record;
        groove_ValueVec values;
        e = groove__records__OwnedRecord__to_values(&value.v.record, &values);
        groove__records__values__Value__drop(&value);
        if (JZ_IS_ERR(e)) return e;
        groove_ValueVec mapped_values = {0};
        for (u32 i = 0; i < values.len && i < sd->fields.len; i++) {
            const groove_DescriptorField *sf = &sd->fields.ptr[i], *tf = &td->fields.ptr[i];
            if (!sf->has_name) { e = recursive_mismatch(path, path_len); break; }
            jz_str child = jz_format("%.*s/record/%.*s", (int)path_len, path, (int)sf->name.len, sf->name.ptr);
            groove_Value mapped;
            groove_Value item = values.ptr[i]; values.ptr[i].tag = GROOVE_V_BOOL;
            e = RP(remap_recursive_enum_value)(item, &sf->value_type, &tf->value_type, remaps, child.ptr, child.len, &mapped);
            jz_str_drop(&child);
            if (JZ_IS_ERR(e)) break;
            jz_vec_push(&mapped_values, &mapped, sizeof mapped);
        }
        value_vec_drop(&values);
        if (JZ_IS_ERR(e)) { value_vec_drop(&mapped_values); return e; }
        jz_bytes raw;
        e = groove__records__RecordDescriptor__create(td, mapped_values.ptr, mapped_values.len, &raw);
        value_vec_drop(&mapped_values);
        if (JZ_IS_ERR(e)) return e;
        out->tag = GROOVE_V_RECORD; out->v.record.raw = raw; out->v.record.descriptor = td;
        return JZ_OK;
    }
    if (value.tag == GROOVE_V_ENUM && source->tag == GROOVE_VT_ENUM && target->tag == GROOVE_VT_ENUM) {
        u32 source_tag = value.v.enm.tag;
        const I32Vec *tags = remap_lookup(&remaps->payload, path, path_len);
        if (!(tags && source_tag < tags->len && tags->ptr[source_tag] >= 0)) {
            groove__records__values__Value__drop(&value);
            return jz_err_fmt(GROOVE_IVM_ERR_ENUM_PROJECTION_ABSENT, "payload enum tag %u is absent from this projection target", source_tag);
        }
        u32 target_tag = (u32)tags->ptr[source_tag];
        const groove_EnumCase *source_case, *target_case;
        e = groove__records__values__EnumSchema__case(source->v.enum_schema, source_tag, &source_case);
        if (!JZ_IS_ERR(e)) e = groove__records__values__EnumSchema__case(target->v.enum_schema, target_tag, &target_case);
        if (JZ_IS_ERR(e)) { groove__records__values__Value__drop(&value); return e; }
        if (source_case->payload->fields.len != target_case->payload->fields.len) {
            groove__records__values__Value__drop(&value);
            return recursive_mismatch(path, path_len);
        }
        const OptStrVec *children = remap_lookup(&remaps->payload_children, path, path_len);
        const jz_str *semantic_child_root = NULL;
        if (children && source_tag < children->len && children->ptr[source_tag].some) semantic_child_root = &children->ptr[source_tag].s;
        jz_str child_root = semantic_child_root ? jz_str_clone(semantic_child_root)
                                                : jz_format("%.*s/case/%u", (int)path_len, path, source_tag);
        groove_ValueVec values;
        e = groove__records__OwnedRecord__to_values(&value.v.enm.record, &values);
        groove__records__values__Value__drop(&value);
        if (JZ_IS_ERR(e)) { jz_str_drop(&child_root); return e; }
        groove_ValueVec mapped_values = {0};
        groove_RecordDescriptor sp = source_case->payload, tp = target_case->payload;
        for (u32 i = 0; i < values.len && i < sp->fields.len; i++) {
            const groove_DescriptorField *sf = &sp->fields.ptr[i], *tf = &tp->fields.ptr[i];
            if (!sf->has_name) { e = recursive_mismatch(path, path_len); break; }
            jz_str child = semantic_child_root
                ? jz_format("%.*s/record/%.*s", (int)child_root.len, child_root.ptr, (int)sf->name.len, sf->name.ptr)
                : jz_format("%.*s/%.*s", (int)child_root.len, child_root.ptr, (int)sf->name.len, sf->name.ptr);
            groove_Value item = values.ptr[i]; values.ptr[i].tag = GROOVE_V_BOOL;
            groove_Value mapped;
            e = RP(remap_recursive_enum_value)(item, &sf->value_type, &tf->value_type, remaps, child.ptr, child.len, &mapped);
            jz_str_drop(&child);
            if (JZ_IS_ERR(e)) break;
            jz_vec_push(&mapped_values, &mapped, sizeof mapped);
        }
        jz_str_drop(&child_root);
        value_vec_drop(&values);
        if (JZ_IS_ERR(e)) { value_vec_drop(&mapped_values); return e; }
        jz_bytes raw;
        e = groove__records__RecordDescriptor__create(tp, mapped_values.ptr, mapped_values.len, &raw);
        value_vec_drop(&mapped_values);
        if (JZ_IS_ERR(e)) return e;
        out->tag = GROOVE_V_ENUM; out->v.enm.tag = target_tag; out->v.enm.record.raw = raw; out->v.enm.record.descriptor = tp;
        return JZ_OK;
    }
    if (groove__records__values__ValueType__eq(source, target)) { *out = value; return JZ_OK; }
    groove__records__values__Value__drop(&value);
    return recursive_mismatch(path, path_len);
}

bool RP(projection_uses_raw_copy)(const groove_ProjectionExpr *exprs, u32 n, u32 nmap, groove_RecordDescriptor output_desc) {
    if (n == 0) return nmap == output_desc->fields.len;
    if (!(n == output_desc->fields.len && nmap == output_desc->fields.len)) return false;
    for (u32 i = 0; i < n; i++) if (exprs[i].expression.tag != GROOVE_PX_FIELD) return false;
    return true;
}

static jz_err validate_copy(groove_RecordDescriptor output_desc, const groove_ValueType *source_type, usize output_idx) {
    const groove_ValueType *expected = &output_desc->fields.ptr[output_idx].value_type;
    if (!groove__records__values__ValueType__eq(source_type, expected)) {
        jz_str dbg = {0};
        groove__records__values__ValueType__debug(&dbg, expected);
        jz_err e = jz_err_fmt(GROOVE_RECORDS_ERR_TYPE_MISMATCH, "value does not match type %.*s", (int)dbg.len, dbg.ptr);
        jz_str_drop(&dbg);
        return e;
    }
    return JZ_OK;
}

static groove_RawProjectionField prepared_constant_field(groove_RecordDescriptor output, usize index, groove_Value value) {
    groove_RawProjectionField f;
    jz_bytes bytes;
    jz_err e = groove__records__encode_single_field_value(&value, &output->fields.ptr[index].value_type, &bytes);
    groove__records__values__Value__drop(&value);
    if (JZ_IS_ERR(e)) { f.tag = GROOVE_RPF_ERROR; f.v.error = e; }
    else { f.tag = GROOVE_RPF_ENCODED; f.v.bytes = bytes; }
    return f;
}

static void raw_field_drop(void *p) {
    groove_RawProjectionField *f = p;
    if (f->tag == GROOVE_RPF_NESTED) jz_vec_drop(&f->v.path, sizeof(groove_DescIdx), NULL);
    else if (f->tag == GROOVE_RPF_ENCODED) jz_bytes_drop(&f->v.bytes);
    else if (f->tag == GROOVE_RPF_ERROR) jz_err_drop(&f->v.error);
}

jz_err RP(raw_projection_fields)(const groove_MapProjectOp *project, groove_RecordDescriptor input_desc, groove_RecordDescriptor output_desc, groove_PreparedProjection **out) {
    *out = NULL;
    JZ_VEC(groove_RawProjectionField) fields = {0};
    jz_err e = JZ_OK;
    if (project->expressions.len == 0) {
        if (project->mapping.len != output_desc->fields.len) return JZ_OK;
        for (u32 output_idx = 0; output_idx < project->mapping.len; output_idx++) {
            usize source = project->mapping.ptr[output_idx].v[0], source_idx = project->mapping.ptr[output_idx].v[1];
            if (source != 0) { e = groove_err_unsupported_operator(); goto fail; }
            if (source_idx >= input_desc->fields.len) { e = groove_err_field_oob(source_idx); goto fail; }
            e = validate_copy(output_desc, &input_desc->fields.ptr[source_idx].value_type, output_idx);
            if (JZ_IS_ERR(e)) goto fail;
            groove_RawProjectionField f; f.tag = GROOVE_RPF_COPY; f.v.source_idx = source_idx;
            jz_vec_push(&fields, &f, sizeof f);
        }
        *out = groove__records__PreparedProjection__new(input_desc, output_desc, fields.ptr, fields.len);
        jz_free(fields.ptr);
        return JZ_OK;
    }
    if (project->expressions.len != output_desc->fields.len) return JZ_OK;
    for (u32 output_idx = 0; output_idx < project->expressions.len; output_idx++) {
        const groove_ProjectExpr *x = &project->expressions.ptr[output_idx].expression;
        groove_RawProjectionField f;
        usize source_idx;
        switch (x->tag) {
        case GROOVE_PX_TEMPLATE_ARGUMENT:
            e = groove_err_unsupported_operator(); goto fail;
        case GROOVE_PX_FIELD:
            e = RP(resolve_field_ref)(input_desc, &x->source, &source_idx);
            if (JZ_IS_ERR(e)) goto fail;
            e = validate_copy(output_desc, &input_desc->fields.ptr[source_idx].value_type, output_idx);
            if (JZ_IS_ERR(e)) goto fail;
            f.tag = GROOVE_RPF_COPY; f.v.source_idx = source_idx;
            break;
        case GROOVE_PX_NULLABLE: case GROOVE_PX_NULLABLE_FLAT: {
            e = RP(resolve_field_ref)(input_desc, &x->source, &source_idx);
            if (JZ_IS_ERR(e)) goto fail;
            const groove_ValueType *source_type = &input_desc->fields.ptr[source_idx].value_type;
            if (x->tag == GROOVE_PX_NULLABLE_FLAT && source_type->tag == GROOVE_VT_NULLABLE) {
                e = validate_copy(output_desc, source_type, output_idx);
                if (JZ_IS_ERR(e)) goto fail;
                f.tag = GROOVE_RPF_COPY; f.v.source_idx = source_idx;
            } else {
                groove_ValueType inner; value_type_clone(&inner, source_type);
                groove_ValueType wrapped = value_type_boxed(GROOVE_VT_NULLABLE, inner);
                e = validate_copy(output_desc, &wrapped, output_idx);
                groove__records__values__ValueType__drop(&wrapped);
                if (JZ_IS_ERR(e)) goto fail;
                f.tag = GROOVE_RPF_WRAP_NULLABLE; f.v.source_idx = source_idx;
            }
            break;
        }
        case GROOVE_PX_RECORD_FIELD: {
            e = RP(resolve_field_ref)(input_desc, &x->source, &source_idx);
            if (JZ_IS_ERR(e)) goto fail;
            f.tag = GROOVE_RPF_NESTED;
            memset(&f.v.path, 0, sizeof f.v.path);
            groove_DescIdx step = { input_desc, source_idx };
            jz_vec_push(&f.v.path, &step, sizeof step);
            const groove_ValueType *vt = &input_desc->fields.ptr[source_idx].value_type;
            for (u32 p = 0; p < x->path.len; p++) {
                if (vt->tag != GROOVE_VT_RECORD) { raw_field_drop(&f); e = groove_err_unsupported_operator(); goto fail; }
                groove_RecordDescriptor d = *vt->v.record;
                usize index;
                if (!groove__records__RecordDescriptor__field_index(d, x->path.ptr[p].ptr, x->path.ptr[p].len, &index)) {
                    raw_field_drop(&f); e = groove_err_unsupported_operator(); goto fail;
                }
                groove_DescIdx s2 = { d, index };
                jz_vec_push(&f.v.path, &s2, sizeof s2);
                vt = &d->fields.ptr[index].value_type;
            }
            e = validate_copy(output_desc, vt, output_idx);
            if (JZ_IS_ERR(e)) { raw_field_drop(&f); goto fail; }
            break;
        }
        case GROOVE_PX_NULL:
            f = prepared_constant_field(output_desc, output_idx, value_nullable(NULL));
            break;
        case GROOVE_PX_LITERAL: case GROOVE_PX_TYPED_LITERAL: {
            groove_Value v;
            groove__ivm__op_types__LiteralValue__to_value(&x->value, &v);
            f = prepared_constant_field(output_desc, output_idx, v);
            break;
        }
        case GROOVE_PX_ENUM_TAG_REMAP: case GROOVE_PX_ENUM_REMAP: case GROOVE_PX_RECURSIVE_ENUM_REMAP:
            f.tag = GROOVE_RPF_EVALUATE;
            break;
        default: JZ_UNREACHABLE();
        }
        jz_vec_push(&fields, &f, sizeof f);
    }
    *out = groove__records__PreparedProjection__new(input_desc, output_desc, fields.ptr, fields.len);
    jz_free(fields.ptr);
    return JZ_OK;
fail:
    jz_vec_drop(&fields, sizeof(groove_RawProjectionField), raw_field_drop);
    return e;
}

jz_err RP(encode_projection_field_value)(groove_RecordDescriptor output_desc, usize field_idx, groove_Value value, jz_bytes *out) {
    if (field_idx >= output_desc->fields.len) { groove__records__values__Value__drop(&value); return groove_err_field_oob(field_idx); }
    jz_err e = groove__records__encode_single_field_value(&value, &output_desc->fields.ptr[field_idx].value_type, out);
    groove__records__values__Value__drop(&value);
    return e;
}

static bool stored_name_position(groove_RecordDescriptor d, const jz_str *name, usize *out) {
    for (u32 i = 0; i < d->fields.len; i++) {
        const groove_DescriptorField *f = &d->fields.ptr[i];
        if (f->has_name && jz_str_eq(&f->name, name)) { *out = i; return true; }
    }
    return false;
}

jz_err RP(resolve_field_ref)(groove_RecordDescriptor d, const groove_FieldRef *field, usize *out) {
    switch (field->tag) {
    case GROOVE_FR_NAME:
        if (RP(resolve_field_name)(d, field->name.ptr, field->name.len, out)) return JZ_OK;
        return groove_err_field_not_found(field->name.ptr, field->name.len);
    case GROOVE_FR_STORED_NAME:
        if (stored_name_position(d, &field->name, out)) return JZ_OK;
        return groove_err_field_not_found(field->name.ptr, field->name.len);
    case GROOVE_FR_RESOLVED:
        if (field->index < d->fields.len) { *out = field->index; return JZ_OK; }
        return groove_err_field_oob(field->index);
    }
    JZ_UNREACHABLE();
}

bool RP(resolve_field_name)(groove_RecordDescriptor d, const char *name, u32 len, usize *out) {
    if (groove__records__RecordDescriptor__field_index(d, name, len, out)) return true;
    jz_str s = { (char *)name, len, len };
    return stored_name_position(d, &s, out);
}

jz_err RP(field_ref_name)(groove_RecordDescriptor d, const groove_FieldRef *field, jz_str *out) {
    switch (field->tag) {
    case GROOVE_FR_NAME:
        *out = jz_str_clone(&field->name);
        return JZ_OK;
    case GROOVE_FR_STORED_NAME: {
        usize index;
        JZ_TRY(RP(resolve_field_ref)(d, field, &index));
        const groove_DescriptorField *resolved = &d->fields.ptr[index];
        usize found;
        if (resolved->has_name && RP(resolve_field_name)(d, resolved->name.ptr, resolved->name.len, &found) && found == index) {
            *out = jz_str_clone(&resolved->name);
            return JZ_OK;
        }
        u32 llen;
        const char *logical = groove__records__DescriptorField__logical_name(resolved, &llen);
        if (logical && RP(resolve_field_name)(d, logical, llen, &found) && found == index) {
            *out = jz_str_from_slice(jz_slice_of(logical, llen));
            return JZ_OK;
        }
        jz_str dn = groove__ivm__graph__FieldRef__display_name(field);
        jz_err e = jz_err_fmt(GROOVE_IVM_ERR_AMBIGUOUS_GRAPH_FIELD_REFERENCE, "exact graph field reference cannot be represented unambiguously by name: %.*s", (int)dn.len, dn.ptr);
        jz_str_drop(&dn);
        return e;
    }
    case GROOVE_FR_RESOLVED:
        return RP(field_name_at)(d, field->index, out);
    }
    JZ_UNREACHABLE();
}

jz_err RP(field_name_at)(groove_RecordDescriptor d, usize index, jz_str *out) {
    if (index >= d->fields.len) return groove_err_field_oob(index);
    const groove_DescriptorField *f = &d->fields.ptr[index];
    if (!f->has_name) return jz_err_fmt(GROOVE_IVM_ERR_GRAPH_FIELD_NOT_FOUND, "graph field not found: #%zu", index);
    *out = jz_str_clone(&f->name);
    return JZ_OK;
}

static groove_DescriptorField descriptor_field_clone_with_type(const groove_DescriptorField *f, groove_ValueType vt) {
    groove_DescriptorField r; memset(&r, 0, sizeof r);
    r.has_name = f->has_name; if (f->has_name) r.name = jz_str_clone(&f->name);
    r.has_identity = f->has_identity; if (f->has_identity) groove__records__FieldIdentity__clone(&r.identity, &f->identity);
    r.value_type = vt;
    return r;
}

jz_err RP(unwrap_nullable_descriptor)(groove_RecordDescriptor input, usize field_idx, groove_RecordDescriptor *out) {
    JZ_VEC(groove_DescriptorField) fields = {0};
    for (u32 idx = 0; idx < input->fields.len; idx++) {
        const groove_DescriptorField *f = &input->fields.ptr[idx];
        groove_ValueType vt;
        if (idx == field_idx && f->value_type.tag == GROOVE_VT_NULLABLE) value_type_clone(&vt, f->value_type.v.boxed);
        else value_type_clone(&vt, &f->value_type);
        groove_DescriptorField nf = descriptor_field_clone_with_type(f, vt);
        jz_vec_push(&fields, &nf, sizeof nf);
    }
    *out = groove__records__RecordDescriptor__new_with_fields(fields.ptr, fields.len);
    jz_free(fields.ptr);
    return JZ_OK;
}

jz_err RP(unnest_descriptor)(groove_RecordDescriptor input, usize array_field_idx, const jz_str *element_field, groove_RecordDescriptor *out) {
    if (array_field_idx >= input->fields.len) return groove_err_field_oob(array_field_idx);
    const groove_DescriptorField *array_field = &input->fields.ptr[array_field_idx];
    if (array_field->value_type.tag != GROOVE_VT_ARRAY) return groove_err_unsupported_operator();
    JZ_VEC(groove_DescriptorField) fields = {0};
    for (u32 idx = 0; idx < input->fields.len; idx++) {
        groove_ValueType vt; value_type_clone(&vt, &input->fields.ptr[idx].value_type);
        groove_DescriptorField nf = descriptor_field_clone_with_type(&input->fields.ptr[idx], vt);
        jz_vec_push(&fields, &nf, sizeof nf);
    }
    groove_ValueType et; value_type_clone(&et, array_field->value_type.v.boxed);
    groove_DescriptorField ef = descriptor_field_new(jz_str_clone(element_field), et);
    jz_vec_push(&fields, &ef, sizeof ef);
    *out = groove__records__RecordDescriptor__new_with_fields(fields.ptr, fields.len);
    jz_free(fields.ptr);
    return JZ_OK;
}

jz_err RP(variant_project_descriptor)(groove_RecordDescriptor input, const groove_FieldRef *field, const jz_str *case_name, groove_RecordDescriptor *out) {
    usize field_idx;
    JZ_TRY(RP(resolve_field_ref)(input, field, &field_idx));
    if (field_idx >= input->fields.len) return groove_err_field_oob(field_idx);
    const groove_DescriptorField *enum_field = &input->fields.ptr[field_idx];
    if (enum_field->value_type.tag != GROOVE_VT_ENUM) {
        jz_str name;
        JZ_TRY(RP(field_ref_name)(input, field, &name));
        jz_err e = jz_err_fmt(GROOVE_IVM_ERR_VARIANT_PROJECT_FIELD_TYPE_MISMATCH, "enum match field is not an enum: %.*s", (int)name.len, name.ptr);
        jz_str_drop(&name);
        return e;
    }
    u32 tag;
    JZ_TRY(groove__records__values__EnumSchema__tag(enum_field->value_type.v.enum_schema, case_name, &tag));
    const groove_EnumCase *c;
    JZ_TRY(groove__records__values__EnumSchema__case(enum_field->value_type.v.enum_schema, tag, &c));
    *out = c->payload;
    return JZ_OK;
}

static jz_err aggregate_expr_value_type(groove_RecordDescriptor input, const groove_AggregateExpr *aggregate, groove_ValueType *out) {
    if (!aggregate->has_expression) return groove_err_unsupported_operator();
    const groove_PlanExpr *x = &aggregate->expression;
    switch (x->tag) {
    case GROOVE_PE_FIELD: case GROOVE_PE_NULLABLE: case GROOVE_PE_NULLABLE_FLAT: case GROOVE_PE_ENUM_TAG_REMAP:
    case GROOVE_PE_ENUM_REMAP: case GROOVE_PE_RECURSIVE_ENUM_REMAP: {
        usize field_idx;
        if (!RP(resolve_field_name)(input, x->field.ptr, x->field.len, &field_idx)) return groove_err_field_not_found(x->field.ptr, x->field.len);
        if (field_idx >= input->fields.len) return groove_err_field_oob(field_idx);
        value_type_clone(out, &input->fields.ptr[field_idx].value_type);
        return JZ_OK;
    }
    case GROOVE_PE_LITERAL:
        if (!groove__ivm__op_types__LiteralValue__value_type(&x->literal, out)) return groove_err_unsupported_operator();
        return JZ_OK;
    case GROOVE_PE_NULL:
        value_type_clone(out, &x->null_type);
        return JZ_OK;
    }
    JZ_UNREACHABLE();
}

static const groove_ValueType *non_nullable_type(const groove_ValueType *vt) {
    return vt->tag == GROOVE_VT_NULLABLE ? vt->v.boxed : vt;
}
/* Used for converting non-nullable column types to nullable column types when aggregating. */
static groove_ValueType nullable_type(const groove_ValueType *vt) {
    groove_ValueType inner; value_type_clone(&inner, non_nullable_type(vt));
    return value_type_boxed(GROOVE_VT_NULLABLE, inner);
}

static jz_err aggregate_output_type(groove_RecordDescriptor input, const groove_AggregateExpr *aggregate, groove_ValueType *out) {
    switch (aggregate->function) {
    case GROOVE_AGG_COUNT: out->tag = GROOVE_VT_U64; return JZ_OK;
    case GROOVE_AGG_AVG: { groove_ValueType f; f.tag = GROOVE_VT_F64; *out = value_type_boxed(GROOVE_VT_NULLABLE, f); return JZ_OK; }
    case GROOVE_AGG_SUM: {
        groove_ValueType vt;
        JZ_TRY(aggregate_expr_value_type(input, aggregate, &vt));
        switch (non_nullable_type(&vt)->tag) {
        case GROOVE_VT_U8: case GROOVE_VT_U16: case GROOVE_VT_U32: case GROOVE_VT_U64:
        case GROOVE_VT_I32: case GROOVE_VT_I64: case GROOVE_VT_F64:
            *out = nullable_type(&vt);
            groove__records__values__ValueType__drop(&vt);
            return JZ_OK;
        default:
            groove__records__values__ValueType__drop(&vt);
            return groove_err_unsupported_operator();
        }
    }
    case GROOVE_AGG_MIN: case GROOVE_AGG_MAX: {
        groove_ValueType vt;
        JZ_TRY(aggregate_expr_value_type(input, aggregate, &vt));
        *out = nullable_type(&vt);
        groove__records__values__ValueType__drop(&vt);
        return JZ_OK;
    }
    }
    JZ_UNREACHABLE();
}

jz_err RP(aggregate_descriptor)(groove_RecordDescriptor input, const groove_FieldRef *group_cols, u32 ng, const groove_AggregateExpr *aggs, u32 na, groove_RecordDescriptor *out) {
    JZ_VEC(groove_DescriptorField) fields = {0};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < ng; i++) {
        usize field_idx;
        e = RP(resolve_field_ref)(input, &group_cols[i], &field_idx);
        if (JZ_IS_ERR(e)) goto fail;
        if (field_idx >= input->fields.len) { e = groove_err_field_oob(field_idx); goto fail; }
        const groove_DescriptorField *field = &input->fields.ptr[field_idx];
        jz_str name;
        e = RP(field_name_at)(input, field_idx, &name);
        if (JZ_IS_ERR(e)) goto fail;
        groove_ValueType vt; value_type_clone(&vt, &field->value_type);
        groove_DescriptorField f = descriptor_field_new(name, vt);
        f.has_identity = true;
        if (field->has_identity) {
            groove__records__FieldIdentity__clone(&f.identity, &field->identity);
        } else {
            memset(&f.identity, 0, sizeof f.identity);
            if (field->has_name) f.identity.name = jz_str_clone(&field->name);
            else {
                jz_err r = RP(field_ref_name)(input, &group_cols[i], &f.identity.name);
                if (JZ_IS_ERR(r)) jz_unwrap_failed();
            }
        }
        jz_vec_push(&fields, &f, sizeof f);
    }
    for (u32 index = 0; index < na; index++) {
        const groove_AggregateExpr *aggregate = &aggs[index];
        jz_str name = aggregate->has_output_name ? jz_str_clone(&aggregate->output_name) : jz_format("aggregate_%u", index);
        groove_ValueType vt;
        e = aggregate_output_type(input, aggregate, &vt);
        if (JZ_IS_ERR(e)) { jz_str_drop(&name); goto fail; }
        groove_DescriptorField f = descriptor_field_new(jz_str_clone(&name), vt);
        f.has_identity = true;
        if (aggregate->has_output_identity) { groove__records__FieldIdentity__clone(&f.identity, &aggregate->output_identity); jz_str_drop(&name); }
        else { memset(&f.identity, 0, sizeof f.identity); f.identity.name = name; }
        jz_vec_push(&fields, &f, sizeof f);
    }
    *out = groove__records__RecordDescriptor__new_with_fields(fields.ptr, fields.len);
    jz_free(fields.ptr);
    return JZ_OK;
fail:
    jz_vec_drop(&fields, sizeof(groove_DescriptorField), descriptor_field_drop);
    return e;
}

groove_RecordDescriptor RP(index_record_descriptor)(void) {
    static groove_RecordDescriptor descriptor;
    if (!descriptor) {
        groove_NamedType fields[2];
        fields[0].name = jz_str_from("key"); fields[0].value_type.tag = GROOVE_VT_BYTES;
        fields[1].name = jz_str_from("value"); fields[1].value_type.tag = GROOVE_VT_BYTES;
        descriptor = groove__records__RecordDescriptor__new(fields, 2);
    }
    return descriptor;
}

const jz_str *RP(variant_projection_target_name)(const groove_VariantProjectionTarget *t) {
    switch (t->tag) {
    case GROOVE_VPT_NAMED: case GROOVE_VPT_SCHEMA_INDEX: return &t->name;
    }
    JZ_UNREACHABLE();
}

jz_err RP(schema_index_input_fields)(const groove_TableSchema *table, const groove_IndexSchema *index, groove_StrVec *out) {
    if (!table->has_primary_key)
        return jz_err_fmt(GROOVE_IVM_ERR_MISSING_PRIMARY_KEY, "table has no primary key: %.*s", (int)table->name.len, table->name.ptr);
    groove_StrVec fields = {0};
    u32 total = index->columns.len + table->primary_key.columns.len;
    for (u32 i = 0; i < total; i++) {
        const jz_str *field = i < index->columns.len ? &index->columns.ptr[i] : &table->primary_key.columns.ptr[i - index->columns.len].column;
        bool known = false;
        for (u32 c = 0; c < table->columns.len; c++) if (jz_str_eq(&table->columns.ptr[c].name, field)) { known = true; break; }
        if (!known) {
            jz_vec_drop(&fields, sizeof(jz_str), groove_ivm_rt_a_drop_str);
            return groove_err_field_not_found(field->ptr, field->len);
        }
        bool dup = false;
        for (u32 f = 0; f < fields.len; f++) if (jz_str_eq(&fields.ptr[f], field)) { dup = true; break; }
        if (!dup) { jz_str c = jz_str_clone(field); JZ_VEC_PUSH(fields, c); }
    }
    *out = fields;
    return JZ_OK;
}

jz_err RP(schema_index_input_descriptor)(const groove_TableSchema *table, const groove_IndexSchema *index, groove_RecordDescriptor *out) {
    groove_RecordDescriptor catalogue = groove__schema__TableSchema__record_schema(table);
    groove_StrVec names;
    JZ_TRY(RP(schema_index_input_fields)(table, index, &names));
    JZ_VEC(groove_ProjectField) fields = {0};
    for (u32 i = 0; i < names.len; i++) {
        groove_ProjectField pf;
        groove__ivm__graph__ProjectField__named(&pf, names.ptr[i]);
        jz_vec_push(&fields, &pf, sizeof pf);
    }
    jz_free(names.ptr);
    jz_err e = RP(project_descriptor)(catalogue, fields.ptr, fields.len, out);
    jz_vec_drop(&fields, sizeof(groove_ProjectField), (void (*)(void *))groove__ivm__graph__ProjectField__drop);
    return e;
}

static jz_err index_key_fields_are_scalar(const groove_IndexByOp *ib, groove_RecordDescriptor input, bool *out) {
    for (u32 i = 0; i < ib->key_fields.len; i++) {
        usize idx = ib->key_fields.ptr[i];
        if (idx >= input->fields.len) return groove_err_field_oob(idx);
        const groove_ValueType *vt = &input->fields.ptr[idx].value_type;
        if (vt->tag == GROOVE_VT_ARRAY) { *out = false; return JZ_OK; }
        if (vt->tag == GROOVE_VT_NULLABLE && vt->v.boxed->tag == GROOVE_VT_ARRAY) { *out = false; return JZ_OK; }
    }
    *out = true;
    return JZ_OK;
}

static jz_err scalar_index_key(const groove_IndexByOp *ib, groove_RecordDescriptor input, const u8 *record, u32 len, jz_bytes *out) {
    jz_bytes key = {0};
    for (u32 i = 0; i < ib->key_fields.len; i++) {
        jz_err e = groove__ivm__runtime__key_encoding__encode_record_field_key_part(&key, input, record, len, ib->key_fields.ptr[i]);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&key); return e; }
    }
    if (ib->append_value_to_key) {
        jz_bytes value;
        jz_err e = groove__ivm__runtime__key_encoding__primary_key_value_bytes(input, record, len, ib->value_fields.ptr, ib->value_fields.len, &value);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&key); return e; }
        jz_bytes_push_u8(&key, 0xff);
        jz_bytes_push(&key, value.ptr, value.len);
        jz_bytes_drop(&value);
    }
    *out = key;
    return JZ_OK;
}

typedef JZ_VEC(jz_bytes) BytesVec;
static void bytes_vec_drop(BytesVec *v) { jz_vec_drop(v, sizeof(jz_bytes), groove_ivm_rt_a_drop_bytes); }

static jz_err index_keys(const groove_IndexByOp *ib, groove_RecordDescriptor input, const u8 *record, u32 len, BytesVec *out) {
    BytesVec keys = {0};
    jz_bytes empty = {0};
    JZ_VEC_PUSH(keys, empty);
    jz_hmap seen; jz_hmap_init(&seen, &groove_ivm_rt_a_hdesc_bytes_set);
    jz_err e = JZ_OK;
    for (u32 f = 0; f < ib->key_fields.len; f++) {
        BytesVec parts = {0};
        e = groove__ivm__runtime__key_encoding__record_field_key_parts(input, record, len, ib->key_fields.ptr[f], (void *)&parts);
        if (JZ_IS_ERR(e)) goto fail;
        if (parts.len == 0) {
            bytes_vec_drop(&parts);
            bytes_vec_drop(&keys);
            jz_hmap_drop(&seen);
            *out = (BytesVec){0};
            return JZ_OK;
        }
        BytesVec next_keys = {0};
        jz_vec_reserve(&next_keys, keys.len * parts.len, sizeof(jz_bytes));
        for (u32 k = 0; k < keys.len; k++) {
            for (u32 p = 0; p < parts.len; p++) {
                jz_bytes next = jz_bytes_clone(&keys.ptr[k]);
                jz_bytes_push(&next, parts.ptr[p].ptr, parts.ptr[p].len);
                jz_bytes copy = jz_bytes_clone(&next);
                if (!jz_hmap_insert(&seen, &copy, NULL, NULL)) JZ_VEC_PUSH(next_keys, next);
                else jz_bytes_drop(&next);
            }
        }
        bytes_vec_drop(&parts);
        bytes_vec_drop(&keys);
        keys = next_keys;
        jz_hmap_clear(&seen);
    }
    if (ib->append_value_to_key) {
        jz_bytes value;
        e = groove__ivm__runtime__key_encoding__primary_key_value_bytes(input, record, len, ib->value_fields.ptr, ib->value_fields.len, &value);
        if (JZ_IS_ERR(e)) goto fail;
        for (u32 k = 0; k < keys.len; k++) {
            jz_bytes_push_u8(&keys.ptr[k], 0xff);
            jz_bytes_push(&keys.ptr[k], value.ptr, value.len);
        }
        jz_bytes_drop(&value);
    }
    jz_hmap_drop(&seen);
    *out = keys;
    return JZ_OK;
fail:
    bytes_vec_drop(&keys);
    jz_hmap_drop(&seen);
    return e;
}

static jz_err index_delta(jz_bytes key, const jz_bytes *value, i64 weight, groove_RecordDeltaVec *out) {
    groove_Value vals[2];
    vals[0].tag = GROOVE_V_BYTES; vals[0].v.bytes = key;
    vals[1].tag = GROOVE_V_BYTES; vals[1].v.bytes = jz_bytes_clone(value);
    groove_RecordDelta d = { {0}, weight };
    jz_err e = groove__records__RecordDescriptor__create(RP(index_record_descriptor)(), vals, 2, &d.record);
    jz_bytes_drop(&vals[0].v.bytes);
    jz_bytes_drop(&vals[1].v.bytes);
    if (JZ_IS_ERR(e)) return e;
    jz_vec_push(out, &d, sizeof d);
    return JZ_OK;
}

jz_err RP(apply_index_by)(const groove_IndexByOp *ib, groove_RecordDescriptor input, const groove_RecordDelta *deltas, u32 n, groove_RecordDeltaVec *out) {
    groove_RecordDeltaVec result = {0};
    bool scalar_key_fields;
    jz_err e = index_key_fields_are_scalar(ib, input, &scalar_key_fields);
    if (JZ_IS_ERR(e)) return e;
    for (u32 i = 0; i < n; i++) {
        const groove_RecordDelta *delta = &deltas[i];
        jz_bytes value = {0};
        if (ib->store_value) {
            e = groove__ivm__runtime__key_encoding__primary_key_value_bytes(input, delta->record.ptr, delta->record.len, ib->value_fields.ptr, ib->value_fields.len, &value);
            if (JZ_IS_ERR(e)) goto fail;
        }
        if (scalar_key_fields) {
            jz_bytes key;
            e = scalar_index_key(ib, input, delta->record.ptr, delta->record.len, &key);
            if (JZ_IS_ERR(e)) { jz_bytes_drop(&value); goto fail; }
            if (ib->has_scan) {
                bool matches;
                e = RP(key_matches_static_scan)(key.ptr, key.len, &ib->scan, &matches);
                if (JZ_IS_ERR(e) || !matches) { jz_bytes_drop(&key); jz_bytes_drop(&value); if (JZ_IS_ERR(e)) goto fail; continue; }
            }
            e = index_delta(key, &value, delta->weight, &result);
            jz_bytes_drop(&value);
            if (JZ_IS_ERR(e)) goto fail;
            continue;
        }
        BytesVec keys;
        e = index_keys(ib, input, delta->record.ptr, delta->record.len, &keys);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&value); goto fail; }
        for (u32 k = 0; k < keys.len; k++) {
            if (ib->has_scan) {
                bool matches;
                e = RP(key_matches_static_scan)(keys.ptr[k].ptr, keys.ptr[k].len, &ib->scan, &matches);
                if (JZ_IS_ERR(e)) break;
                if (!matches) continue;
            }
            e = index_delta(keys.ptr[k], &value, delta->weight, &result);
            keys.ptr[k] = (jz_bytes){0};
            if (JZ_IS_ERR(e)) break;
        }
        bytes_vec_drop(&keys);
        jz_bytes_drop(&value);
        if (JZ_IS_ERR(e)) goto fail;
    }
    *out = result;
    return JZ_OK;
fail:
    jz_vec_drop(&result, sizeof(groove_RecordDelta), groove__ivm__runtime__subscriptions__RecordDelta__drop);
    return e;
}

static jz_err static_scan_key(const groove_LiteralVec *values, jz_bytes *out) {
    jz_bytes key = {0};
    for (u32 i = 0; i < values->len; i++) {
        groove_Value v;
        groove__ivm__op_types__LiteralValue__to_value(&values->ptr[i], &v);
        jz_err e = groove__ivm__runtime__key_encoding__encode_key_part(&key, &v);
        groove__records__values__Value__drop(&v);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&key); return e; }
    }
    *out = key;
    return JZ_OK;
}

jz_err RP(scan_bounds)(const groove_StaticScanSpec *scan, groove_StaticScanBounds *out) {
    switch (scan->tag) {
    case GROOVE_SCAN_POINT: case GROOVE_SCAN_PREFIX: case GROOVE_SCAN_PREFIX_LIMIT: case GROOVE_SCAN_REVERSE_PREFIX_LIMIT:
        out->tag = GROOVE_SSB_PREFIX;
        out->end = (jz_bytes){0};
        return static_scan_key(&scan->prefix, &out->prefix);
    case GROOVE_SCAN_RANGE:
        out->tag = GROOVE_SSB_RANGE;
        JZ_TRY(static_scan_key(&scan->prefix, &out->prefix));
        {
            jz_err e = static_scan_key(&scan->end, &out->end);
            if (JZ_IS_ERR(e)) { jz_bytes_drop(&out->prefix); return e; }
        }
        return JZ_OK;
    }
    JZ_UNREACHABLE();
}

bool RP(scan_max_items)(const groove_StaticScanSpec *scan, usize *out) {
    if (scan && (scan->tag == GROOVE_SCAN_PREFIX_LIMIT || scan->tag == GROOVE_SCAN_REVERSE_PREFIX_LIMIT)) { *out = scan->max_items; return true; }
    return false;
}

bool RP(scan_reversed)(const groove_StaticScanSpec *scan) {
    return scan && scan->tag == GROOVE_SCAN_REVERSE_PREFIX_LIMIT;
}

jz_err RP(key_matches_static_scan)(const u8 *key, u32 len, const groove_StaticScanSpec *scan, bool *out) {
    groove_StaticScanBounds b;
    JZ_TRY(RP(scan_bounds)(scan, &b));
    jz_slice k = { key, len };
    if (b.tag == GROOVE_SSB_PREFIX) {
        *out = len >= b.prefix.len && memcmp(key, b.prefix.ptr, b.prefix.len) == 0;
    } else {
        *out = jz_slice_cmp(jz_bytes_slice(&b.prefix), k) <= 0 && jz_slice_cmp(k, jz_bytes_slice(&b.end)) < 0;
    }
    RP(StaticScanBounds__drop)(&b);
    return JZ_OK;
}

jz_err RP(persisted_index_scan_bounds)(u32 index_id, const groove_StaticScanSpec *scan, groove_StaticScanBounds *out) {
    /* The ordered part encodings are prefix-free and order-preserving on their
     * own, so the logical key follows the numeric index id directly. */
    jz_bytes base;
    groove__ivm__runtime__index_ids__durable_index_key_prefix(index_id, &base);
    if (!scan) {
        out->tag = GROOVE_SSB_PREFIX; out->prefix = base; out->end = (jz_bytes){0};
        return JZ_OK;
    }
    jz_bytes logical;
    switch (scan->tag) {
    case GROOVE_SCAN_POINT: case GROOVE_SCAN_PREFIX: case GROOVE_SCAN_PREFIX_LIMIT: case GROOVE_SCAN_REVERSE_PREFIX_LIMIT: {
        jz_err e = static_scan_key(&scan->prefix, &logical);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&base); return e; }
        out->tag = GROOVE_SSB_PREFIX;
        out->prefix = jz_bytes_clone(&base);
        jz_bytes_push(&out->prefix, logical.ptr, logical.len);
        out->end = (jz_bytes){0};
        jz_bytes_drop(&logical);
        break;
    }
    case GROOVE_SCAN_RANGE: {
        jz_err e = static_scan_key(&scan->prefix, &logical);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&base); return e; }
        jz_bytes end;
        e = static_scan_key(&scan->end, &end);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&logical); jz_bytes_drop(&base); return e; }
        out->tag = GROOVE_SSB_RANGE;
        out->prefix = jz_bytes_clone(&base); jz_bytes_push(&out->prefix, logical.ptr, logical.len);
        out->end = jz_bytes_clone(&base); jz_bytes_push(&out->end, end.ptr, end.len);
        jz_bytes_drop(&logical); jz_bytes_drop(&end);
        break;
    }
    }
    jz_bytes_drop(&base);
    return JZ_OK;
}
