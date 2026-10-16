/* groove::ivm::runtime::evaluator::lookup — keyed lookup: a table source
 * probed by the keys of its only input. Hydration reads one storage prefix
 * per distinct live key; a tick probes only keys that become live, retracts
 * the rows of keys that stop being live, and applies the table's own deltas to
 * keys that stay live. */
#include "jz_rt.h"
#include "groove/ivm/runtime/evaluator.h"
#include "groove/ivm/runtime/record_projection.h"

#define LK(name) groove__ivm__runtime__evaluator__lookup__##name
#define TE(name) groove__ivm__runtime__evaluator__TickEvaluator__##name

/* rows: Vec<u8> -> BTreeMap<Vec<u8>, Bytes> */
static void bytes_val_drop(void *p) { jz_bytes_drop(p); }
static const jz_btree_desc desc_pk_rows = { sizeof(jz_bytes), sizeof(jz_bytes), jz_cmp_bytes, groove_ivm_rt_a_drop_bytes, bytes_val_drop };
static void btree_val_drop(void *p) { jz_btree_drop(p); }
static const jz_hmap_desc hdesc_keys = { sizeof(jz_bytes), sizeof(i64), groove_ivm_rt_a_hash_bytes, groove_ivm_rt_a_eq_bytes, groove_ivm_rt_a_drop_bytes, NULL };
static const jz_hmap_desc hdesc_rows = { sizeof(jz_bytes), sizeof(jz_btree), groove_ivm_rt_a_hash_bytes, groove_ivm_rt_a_eq_bytes, groove_ivm_rt_a_drop_bytes, btree_val_drop };

static void contents_init(groove_LookupStateContents *c) {
    jz_hmap_init(&c->keys, &hdesc_keys);
    jz_hmap_init(&c->rows, &hdesc_rows);
}
static void contents_drop(void *p) {
    groove_LookupStateContents *c = p;
    jz_hmap_drop(&c->keys);
    jz_hmap_drop(&c->rows);
}
static void contents_clone(void *dst, const void *src) {
    groove_LookupStateContents *d = dst; const groove_LookupStateContents *s = src;
    contents_init(d);
    jz_bytes *k; void *v;
    JZ_HMAP_FOR(&s->keys, k, v) { jz_bytes kc = jz_bytes_clone(k); jz_hmap_insert(&d->keys, &kc, v, NULL); }
    JZ_HMAP_FOR(&s->rows, k, v) {
        jz_bytes kc = jz_bytes_clone(k);
        jz_btree rows; jz_btree_clone(&rows, v, groove_ivm_rt_a_clone_bytes, groove_ivm_rt_a_clone_bytes);
        jz_hmap_insert(&d->rows, &kc, &rows, NULL);
    }
}

void LK(LookupState__default)(groove_LookupState *s) {
    s->rc = jz_rc_new(sizeof(groove_LookupStateContents), contents_drop);
    contents_init(s->rc);
}
void LK(LookupState__drop)(groove_LookupState *s) {
    if (s->rc) jz_rc_release(s->rc);
    s->rc = NULL;
}

static bool LK(LookupStateContents__live)(const groove_LookupStateContents *c, const jz_bytes *key) {
    const i64 *count = jz_hmap_get(&c->keys, key);
    return count && *count > 0;
}

/* Encode the given fields of a record as storage key parts. */
static jz_err LK(encoded_key)(groove_RecordDescriptor d, const u8 *record, u32 len, const usize *fields, u32 n, jz_bytes *out) {
    jz_bytes key = {0};
    for (u32 i = 0; i < n; i++) {
        jz_err err = groove__ivm__runtime__key_encoding__encode_record_field_key_part(&key, d, record, len, fields[i]);
        if (err.code) { jz_bytes_drop(&key); return err; }
    }
    *out = key;
    return JZ_OK;
}

/* Where a lookup's probe key sits inside the table's primary key. */
typedef struct LookupLayout { jz_bytes prefix; groove_UsizeVec primary_key; groove_UsizeVec key_columns; } LookupLayout;
static void layout_drop(LookupLayout *l) {
    jz_bytes_drop(&l->prefix);
    jz_vec_drop(&l->primary_key, sizeof(usize), NULL);
    jz_vec_drop(&l->key_columns, sizeof(usize), NULL);
}

static jz_err LK(TickEvaluator__lookup_layout)(const groove_TickEvaluator *e, const groove_LookupOp *lookup, groove_RecordDescriptor output, LookupLayout *out) {
    const groove_TableSchema *table = groove__schema__DatabaseSchema__table(e->schema, lookup->source.table.ptr, lookup->source.table.len);
    if (!table) return groove_err_table_not_found(&lookup->source.table);
    memset(out, 0, sizeof(*out));
    JZ_TRY(groove__ivm__runtime__key_encoding__primary_key_field_indices(table, output, &out->primary_key));
    usize prefix_len = 0;
    if (lookup->source.has_scan) {
        u32 tag = lookup->source.scan.tag;
        if (tag == GROOVE_SCAN_PREFIX || tag == GROOVE_SCAN_POINT) prefix_len = lookup->source.scan.prefix.len;
        else { layout_drop(out); return groove_err_unsupported_lookup("a lookup's static scan must be a point or prefix"); }
        groove_StaticScanBounds bounds;
        jz_err err = groove__ivm__runtime__record_projection__scan_bounds(&lookup->source.scan, &bounds);
        if (err.code) { layout_drop(out); return err; }
        if (bounds.tag != GROOVE_SSB_PREFIX) jz_panic("internal error: entered unreachable code: point and prefix scans have prefix bounds");
        out->prefix = bounds.prefix;
        bounds.prefix.ptr = NULL; bounds.prefix.len = bounds.prefix.cap = 0;
        groove__ivm__runtime__record_projection__StaticScanBounds__drop(&bounds);
    }
    if (prefix_len + lookup->key_fields.len > out->primary_key.len) {
        layout_drop(out);
        return groove_err_unsupported_lookup("a lookup's prefix and keys exceed the table's primary key");
    }
    jz_vec_extend(&out->key_columns, out->primary_key.ptr + prefix_len, lookup->key_fields.len, sizeof(usize));
    return JZ_OK;
}

static groove_StorageRequestKey probe_request(const groove_LookupOp *lookup, const LookupLayout *layout, const jz_bytes *key) {
    groove_StorageRequestKey r;
    memset(&r, 0, sizeof(r));
    r.tag = GROOVE_SRK_SCAN_PREFIX;
    r.family = jz_str_clone(&lookup->source.table);
    r.prefix = jz_bytes_clone(&layout->prefix);
    jz_bytes_push(&r.prefix, key->ptr, key->len);
    return r;
}

static void table_delta_drop(void *p) { groove__ivm__runtime__subscriptions__TableDelta__drop(p); }

/* Read every row of `keys` from storage through the evaluation session.
 * Returns EvaluationBlocked (and registers every missing probe at once) until
 * all of them are loaded. `probed` is ProbedRows = rows-shaped hmap. */
static jz_err LK(TickEvaluator__probe_lookup_keys)(groove_TickEvaluator *e, const groove_LookupOp *lookup, groove_RecordDescriptor output, const LookupLayout *layout, const jz_bytes *keys, u32 n, jz_hmap *probed) {
    jz_hmap_init(probed, &hdesc_rows);
    groove_EvaluationInputs *inputs = e->evaluation_inputs;
    if (!inputs) return groove_err_unsupported_lookup("a lookup reads storage and cannot run in a recursive step or durable node");
    bool blocked = false;
    for (u32 i = 0; i < n; i++) {
        const groove_KvPairVec *rows;
        jz_err err = groove__ivm__runtime__evaluation_session__EvaluationInputs__rows(inputs, probe_request(lookup, layout, &keys[i]), &rows);
        if (err.code == GROOVE_IVM_ERR_EVALUATION_BLOCKED) { jz_err_drop(&err); blocked = true; }
        else if (err.code) return err;
    }
    if (blocked) return groove_err_evaluation_blocked();
    const groove_TableSchema *table = groove__schema__DatabaseSchema__table(e->schema, lookup->source.table.ptr, lookup->source.table.len);
    if (!table) return groove_err_table_not_found(&lookup->source.table);
    /* The probe prefix already bounds the rows; project them like the table
     * source would. */
    groove_TableSourceOp source = lookup->source;
    source.has_scan = false;
    for (u32 i = 0; i < n; i++) {
        const groove_KvPairVec *stored;
        JZ_TRY(groove__ivm__runtime__evaluation_session__EvaluationInputs__rows(inputs, probe_request(lookup, layout, &keys[i]), &stored));
        groove_TableDeltaVec grouped = {0};
        JZ_TRY(groove__ivm__runtime__operator_updates__NodeState__group_source_rows(table, stored, (void *)&grouped));
        groove_RecordDeltas projected = groove_RecordDeltas_empty(output);
        jz_err err = groove__ivm__runtime__operator_updates__NodeState__update_table_source(&source, e->schema, e->variant_projections, output, grouped.ptr, grouped.len, &projected);
        jz_vec_drop(&grouped, sizeof(groove_TableDelta), table_delta_drop);
        JZ_TRY(err);
        jz_bytes k = jz_bytes_clone(&keys[i]);
        bool inserted = false;
        jz_btree *rows = jz_hmap_entry(probed, &k, &inserted);
        if (inserted) jz_btree_init(rows, &desc_pk_rows); else jz_bytes_drop(&k);
        for (u32 j = 0; j < projected.deltas.len; j++) {
            groove_RecordDelta *delta = &projected.deltas.ptr[j];
            jz_bytes primary_key = {0};
            err = LK(encoded_key)(output, delta->record.ptr, delta->record.len, layout->primary_key.ptr, layout->primary_key.len, &primary_key);
            if (err.code) break;
            jz_bytes record = delta->record;
            delta->record.ptr = NULL; delta->record.len = delta->record.cap = 0;
            jz_btree_insert(rows, &primary_key, &record, NULL);
        }
        groove__ivm__runtime__subscriptions__RecordDeltas__drop(&projected);
        JZ_TRY(err);
    }
    return JZ_OK;
}

static int cmp_bytes_key(const void *a, const void *b, void *ctx) { return jz_cmp_bytes(a, b, ctx); }
static const jz_btree_desc desc_key_deltas = { sizeof(jz_bytes), sizeof(i64), cmp_bytes_key, groove_ivm_rt_a_drop_bytes, NULL };

typedef struct LookupChange { i64 weight; jz_bytes key; jz_bytes primary_key; jz_bytes record; } LookupChange;
static void change_drop(void *p) { LookupChange *c = p; jz_bytes_drop(&c->key); jz_bytes_drop(&c->primary_key); jz_bytes_drop(&c->record); }
static int change_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    bool x = ((const LookupChange *)a)->weight > 0, y = ((const LookupChange *)b)->weight > 0;
    return (int)x - (int)y;
}
static bool keep_nonempty(const void *k, void *v, void *ctx) { (void)k; (void)ctx; return ((jz_btree *)v)->len != 0; }
static void contents_rc_clone(void *dst, const void *src) { contents_clone(dst, src); }

static bool bytes_list_contains(const JZ_VEC(jz_bytes) *list, const jz_bytes *key) {
    for (u32 i = 0; i < list->len; i++) if (jz_bytes_eq(&list->ptr[i], key)) return true;
    return false;
}

jz_err groove__ivm__runtime__evaluator__lookup__TickEvaluator__update_lookup(groove_TickEvaluator *e, groove_NodeId node, const groove_LookupOp *lookup, groove_RecordDescriptor output_desc, groove_RecordDeltas *input_arc, groove_RecordDeltas *out) {
    if (e->context.scope != groove__ivm__runtime__state__ScopeId__root())
        return groove_err_unsupported_lookup("a lookup cannot run in a recursive step");
    LookupLayout layout;
    JZ_TRY(LK(TickEvaluator__lookup_layout)(e, lookup, output_desc, &layout));
    groove_RecordDeltas *input = NULL;
    jz_err err = TE(materialize_indirect_field_indices)(e, input_arc, lookup->key_fields.ptr, lookup->key_fields.len, &input);
    if (err.code) { layout_drop(&layout); return err; }
    jz_btree key_deltas; jz_btree_init(&key_deltas, &desc_key_deltas);
    JZ_VEC(jz_bytes) entering = {0}, leaving = {0};
    JZ_VEC(LookupChange) changes = {0};
    jz_hmap probed; jz_hmap_init(&probed, &hdesc_rows);
    groove_RecordDeltas table_rows = groove_RecordDeltas_empty(output_desc);
    groove_RecordDeltaVec output = {0};
    for (u32 i = 0; i < input->deltas.len; i++) {
        const groove_RecordDelta *delta = &input->deltas.ptr[i];
        jz_bytes key = {0};
        err = LK(encoded_key)(input->descriptor, delta->record.ptr, delta->record.len, lookup->key_fields.ptr, lookup->key_fields.len, &key);
        if (err.code) goto done;
        bool inserted = false;
        i64 *w = jz_btree_entry(&key_deltas, &key, &inserted);
        if (inserted) *w = 0; else jz_bytes_drop(&key);
        *w += delta->weight;
    }
    groove_OperatorStateKey operator_key = TE(operator_key)(e, node);
    groove_SubTick sub_tick = { e->current_tick, 0 };
    bool replace = e->context.arrangement_update_mode == GROOVE_AUM_REPLACE;
    groove_LookupStateContents empty; contents_init(&empty);
    const groove_OperatorState *existing = jz_hmap_get(e->operator_states, &operator_key);
    if (existing && existing->tag != GROOVE_OS_LOOKUP) { contents_drop(&empty); err = groove_err_state_mismatch(node); goto done; }
    if (existing) {
        const groove_AsOfStamp *st = &existing->v.lookup.as_of;
        if (st->has && groove_SubTick_cmp(st->at, sub_tick) > 0) { contents_drop(&empty); err = groove_ivm_rt_a_err_out_of_order(st->at, sub_tick, false); goto done; }
        if (!replace && st->has && groove_SubTick_cmp(st->at, sub_tick) == 0) {
            contents_drop(&empty);
            *out = groove_RecordDeltas_empty(output_desc);
            goto done;
        }
    }
    /* Hydration supplies the complete key relation; a rebuild starts from
     * nothing and reads current storage for every live key. */
    const groove_LookupStateContents *before = existing && !replace ? existing->v.lookup.value.rc : &empty;
    jz_bytes *kp; i64 *dp;
    JZ_BTREE_FOR(&key_deltas, kp, dp) {
        const i64 *oldp = jz_hmap_get(&before->keys, kp);
        i64 old = oldp ? *oldp : 0, now = old + *dp;
        if (old <= 0 && now > 0) JZ_VEC_PUSH(entering, jz_bytes_clone(kp));
        else if (old > 0 && now <= 0) JZ_VEC_PUSH(leaving, jz_bytes_clone(kp));
    }
    contents_drop(&empty);
    /* Read every entering key before touching state: a blocked probe re-polls
     * this node with the same inputs. */
    jz_hmap_drop(&probed);
    err = LK(TickEvaluator__probe_lookup_keys)(e, lookup, output_desc, &layout, entering.ptr, entering.len, &probed);
    if (err.code) goto done;
    /* The table's own changes, under the static prefix, for keys that stay live. */
    if (!replace) {
        err = groove__ivm__runtime__operator_updates__NodeState__update_table_source(&lookup->source, e->schema, e->variant_projections, output_desc, e->table_deltas, e->n_table_deltas, &table_rows);
        if (err.code) goto done;
    }
    groove_OperatorState operator;
    groove_OperatorStateKey rk;
    bool had = jz_hmap_remove(e->operator_states, &operator_key, &rk, &operator);
    if (!had || replace) {
        if (had) groove__ivm__runtime__evaluator__OperatorState__drop(&operator);
        memset(&operator, 0, sizeof(operator));
        operator.tag = GROOVE_OS_LOOKUP;
        LK(LookupState__default)(&operator.v.lookup.value);
    }
    if (operator.tag != GROOVE_OS_LOOKUP) { groove__ivm__runtime__evaluator__OperatorState__drop(&operator); err = groove_err_state_mismatch(node); goto done; }
    groove_AsOfLookup *state = &operator.v.lookup;
    {
        const groove_LookupStateContents *current = state->value.rc;
        for (u32 i = 0; i < table_rows.deltas.len; i++) {
            const groove_RecordDelta *delta = &table_rows.deltas.ptr[i];
            LookupChange c = { delta->weight, {0}, {0}, {0} };
            err = LK(encoded_key)(output_desc, delta->record.ptr, delta->record.len, layout.key_columns.ptr, layout.key_columns.len, &c.key);
            if (err.code) break;
            if (!LK(LookupStateContents__live)(current, &c.key) || bytes_list_contains((const void *)&leaving, &c.key)) { jz_bytes_drop(&c.key); continue; }
            err = LK(encoded_key)(output_desc, delta->record.ptr, delta->record.len, layout.primary_key.ptr, layout.primary_key.len, &c.primary_key);
            if (err.code) { jz_bytes_drop(&c.key); break; }
            c.record = jz_bytes_clone(&delta->record);
            jz_vec_push(&changes, &c, sizeof(c));
        }
        if (err.code) { groove__ivm__runtime__evaluator__OperatorState__drop(&operator); goto done; }
    }
    if (!(leaving.len == 0 && changes.len == 0 && probed.len == 0 && key_deltas.len == 0)) {
        groove_LookupStateContents *contents = jz_rc_make_mut((void **)&state->value.rc, sizeof(groove_LookupStateContents), contents_rc_clone);
        for (u32 i = 0; i < leaving.len; i++) {
            jz_bytes rkey; jz_btree rows;
            if (jz_hmap_remove(&contents->rows, &leaving.ptr[i], &rkey, &rows)) {
                jz_bytes pk, record;
                while (jz_btree_pop_first(&rows, &pk, &record)) {
                    jz_bytes_drop(&pk);
                    groove_RecordDelta d = { record, -1 };
                    jz_vec_push(&output, &d, sizeof(d));
                }
                jz_btree_drop(&rows);
                jz_bytes_drop(&rkey);
            }
        }
        /* Apply table changes per primary key, retractions first. A row is
         * removed only if it is the stored row and inserted only if its key is
         * absent, so a probe that already saw a later write converges. */
        jz_sort(changes.ptr, changes.len, sizeof(LookupChange), change_cmp, NULL);
        for (u32 i = 0; i < changes.len; i++) {
            LookupChange *c = &changes.ptr[i];
            bool inserted = false;
            jz_btree *rows = jz_hmap_entry(&contents->rows, &c->key, &inserted);
            if (inserted) { jz_btree_init(rows, &desc_pk_rows); c->key.ptr = NULL; c->key.len = c->key.cap = 0; }
            if (c->weight < 0) {
                const jz_bytes *stored = jz_btree_get(rows, &c->primary_key);
                if (stored && jz_bytes_eq(stored, &c->record)) {
                    jz_bytes ok, ov;
                    jz_btree_remove(rows, &c->primary_key, &ok, &ov);
                    jz_bytes_drop(&ok); jz_bytes_drop(&ov);
                    groove_RecordDelta d = { c->record, -1 };
                    c->record.ptr = NULL; c->record.len = c->record.cap = 0;
                    jz_vec_push(&output, &d, sizeof(d));
                }
            } else if (c->weight > 0 && !jz_btree_contains(rows, &c->primary_key)) {
                jz_bytes rec = jz_bytes_clone(&c->record);
                jz_btree_insert(rows, &c->primary_key, &rec, NULL);
                c->primary_key.ptr = NULL; c->primary_key.len = c->primary_key.cap = 0;
                groove_RecordDelta d = { c->record, 1 };
                c->record.ptr = NULL; c->record.len = c->record.cap = 0;
                jz_vec_push(&output, &d, sizeof(d));
            }
        }
        jz_bytes *pkey; jz_btree *prow;
        JZ_HMAP_FOR(&probed, pkey, prow) {
            jz_bytes *rk2; jz_bytes *rv;
            JZ_BTREE_FOR(prow, rk2, rv) { groove_RecordDelta d = { jz_bytes_clone(rv), 1 }; jz_vec_push(&output, &d, sizeof(d)); }
        }
        {
            jz_hmap_iter it; jz_hmap_iter_begin(&it, &probed);
            JZ_VEC(jz_bytes) pkeys = {0};
            JZ_HMAP_FOR(&probed, pkey, prow) JZ_VEC_PUSH(pkeys, jz_bytes_clone(pkey));
            for (u32 i = 0; i < pkeys.len; i++) {
                jz_bytes k; jz_btree rows;
                jz_hmap_remove(&probed, &pkeys.ptr[i], &k, &rows);
                jz_hmap_insert(&contents->rows, &k, &rows, NULL);
            }
            jz_vec_drop(&pkeys, sizeof(jz_bytes), groove_ivm_rt_a_drop_bytes);
        }
        jz_bytes key; i64 delta;
        while (jz_btree_pop_first(&key_deltas, &key, &delta)) {
            bool inserted = false;
            jz_bytes kc = jz_bytes_clone(&key);
            i64 *count = jz_hmap_entry(&contents->keys, &kc, &inserted);
            if (inserted) *count = 0; else jz_bytes_drop(&kc);
            *count += delta;
            if (*count == 0) jz_hmap_remove(&contents->keys, &key, NULL, NULL);
            if (!LK(LookupStateContents__live)(contents, &key)) jz_hmap_remove(&contents->rows, &key, NULL, NULL);
            jz_bytes_drop(&key);
        }
        jz_hmap_retain(&contents->rows, keep_nonempty, NULL);
    }
    err = groove__ivm__runtime__state__AsOf__mark_forward_as_of(&state->as_of, sub_tick, false);
    if (err.code) { groove__ivm__runtime__evaluator__OperatorState__drop(&operator); goto done; }
    jz_hmap_insert(e->operator_states, &operator_key, &operator, NULL);
    out->descriptor = output_desc; out->deltas = output;
    output.ptr = NULL; output.len = output.cap = 0;
done:
    jz_vec_drop(&output, sizeof(groove_RecordDelta), groove__ivm__runtime__subscriptions__RecordDelta__drop);
    groove__ivm__runtime__subscriptions__RecordDeltas__drop(&table_rows);
    jz_vec_drop(&changes, sizeof(LookupChange), change_drop);
    jz_hmap_drop(&probed);
    jz_vec_drop(&entering, sizeof(jz_bytes), groove_ivm_rt_a_drop_bytes);
    jz_vec_drop(&leaving, sizeof(jz_bytes), groove_ivm_rt_a_drop_bytes);
    jz_btree_drop(&key_deltas);
    jz_rc_release(input);
    layout_drop(&layout);
    return err;
}
