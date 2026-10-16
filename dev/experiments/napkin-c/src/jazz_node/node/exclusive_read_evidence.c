/* Port of crates/jazz/layers/node/src/node/exclusive_read_evidence.rs
 *
 * `jazz.exclusive-read-evidence.v1`: an exclusive transaction's snapshot and
 * read sets, kept in `jazz_transactions` slots 5-8 while its fate is pending.
 * Each slot holds the Groove typed-record v1 bytes of one fixed descriptor
 * with a leading `format_v1 = 1`; nested query and binding bytes reuse the
 * pinned native query Postcard codec and canonical `jazz-binding-v0` bytes. */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

#define FORMAT_V1 1

static groove_ValueType vt(u32 tag) { groove_ValueType t = { tag }; return t; }
static groove_ValueType vt_record(groove_RecordDescriptor *d) { groove_ValueType t = { GROOVE_VT_RECORD }; t.v.record = d; return t; }

static groove_Value v_u8(u8 x) { groove_Value v = { GROOVE_V_U8 }; v.v.u = x; return v; }
static groove_Value v_u64(u64 x) { groove_Value v = { GROOVE_V_U64 }; v.v.u = x; return v; }
static groove_Value v_uuid(jz_uuid x) { groove_Value v = { GROOVE_V_UUID }; v.v.uuid = x; return v; }
static groove_Value v_string(jz_str s) { groove_Value v = { GROOVE_V_STRING }; v.v.s = s; return v; }
static groove_Value v_bytes(jz_bytes b) { groove_Value v = { GROOVE_V_BYTES }; v.v.bytes = b; return v; }
static groove_Value v_array(groove_ValueVec items) { groove_Value v = { GROOVE_V_ARRAY }; v.v.vals = items; return v; }
static void value_drop_v(void *p) { groove__records__values__Value__drop(p); }
static void values_drop(groove_ValueVec *v) { jz_vec_drop(v, sizeof(groove_Value), value_drop_v); }
static void value_vec_drop_v(void *p) { values_drop(p); }
JZ_NVEC(jzn_value_rows, groove_ValueVec);
static void rows_drop(jzn_value_rows *r) { jz_vec_drop(r, sizeof(groove_ValueVec), value_vec_drop_v); }

static groove_RecordDescriptor *dot_descriptor(void) {
    static const char *const names[] = { "time", "node" };
    groove_ValueType tys[] = { vt(GROOVE_VT_U64), vt(GROOVE_VT_UUID) };
    return groove__records__RecordDescriptor__new(names, tys, 2);
}

static groove_RecordDescriptor *row_read_descriptor(void) {
    static const char *const names[] = { "table", "row_uuid", "version_time", "version_node" };
    groove_ValueType tys[] = { vt(GROOVE_VT_STRING), vt(GROOVE_VT_UUID), vt(GROOVE_VT_U64), vt(GROOVE_VT_UUID) };
    return groove__records__RecordDescriptor__new(names, tys, 4);
}

static groove_RecordDescriptor *absent_read_descriptor(void) {
    static const char *const names[] = { "table", "row_uuid" };
    groove_ValueType tys[] = { vt(GROOVE_VT_STRING), vt(GROOVE_VT_UUID) };
    return groove__records__RecordDescriptor__new(names, tys, 2);
}

static groove_RecordDescriptor *predicate_read_descriptor(void) {
    static const char *const names[] = { "table", "shape_id", "query", "binding_id", "bindings" };
    groove_ValueType tys[] = { vt(GROOVE_VT_STRING), vt(GROOVE_VT_UUID), vt(GROOVE_VT_BYTES), vt(GROOVE_VT_UUID), vt(GROOVE_VT_BYTES) };
    return groove__records__RecordDescriptor__new(names, tys, 5);
}

/* `jazz_exclusive_base_snapshot_v1` */
groove_RecordDescriptor *jazz_node__node__exclusive_read_evidence__base_snapshot_descriptor(void) {
    static const char *const names[] = { "format_v1", "owner", "global_base", "local_base", "dots" };
    groove_ValueType tys[] = {
        vt(GROOVE_VT_U8), vt(GROOVE_VT_UUID), vt(GROOVE_VT_U64), vt(GROOVE_VT_U64),
        groove__records__values__ValueType__array_of(vt_record(dot_descriptor())),
    };
    return groove__records__RecordDescriptor__new(names, tys, 5);
}

static groove_RecordDescriptor *reads_descriptor(groove_RecordDescriptor *item) {
    static const char *const names[] = { "format_v1", "reads" };
    groove_ValueType tys[] = { vt(GROOVE_VT_U8), groove__records__values__ValueType__array_of(vt_record(item)) };
    return groove__records__RecordDescriptor__new(names, tys, 2);
}

/* `jazz_exclusive_row_reads_v1` */
groove_RecordDescriptor *jazz_node__node__exclusive_read_evidence__row_reads_descriptor(void) { return reads_descriptor(row_read_descriptor()); }
/* `jazz_exclusive_absent_reads_v1` */
groove_RecordDescriptor *jazz_node__node__exclusive_read_evidence__absent_reads_descriptor(void) { return reads_descriptor(absent_read_descriptor()); }
/* `jazz_exclusive_predicate_reads_v1` */
groove_RecordDescriptor *jazz_node__node__exclusive_read_evidence__predicate_reads_descriptor(void) { return reads_descriptor(predicate_read_descriptor()); }

static jz_err nested(const groove_RecordDescriptor *descriptor, const groove_ValueVec *values, groove_Value *out) {
    jz_bytes raw = { 0 };
    JZ_TRY(groove__records__RecordDescriptor__create(descriptor, values->ptr, values->len, &raw));
    out->tag = GROOVE_V_RECORD;
    out->v.record = groove__records__OwnedRecord__new(raw, groove__records__RecordDescriptor__clone_box(descriptor));
    return JZ_OK;
}

/* Value::Nullable(bytes.map(|bytes| Box::new(Value::Bytes(bytes)))) */
static groove_Value slot(const jz_bytes *bytes) {
    groove_Value v = { GROOVE_V_NULLABLE };
    v.v.nullable = NULL;
    if (bytes) {
        v.v.nullable = JZ_NEW(groove_Value);
        *v.v.nullable = v_bytes(*bytes);
    }
    return v;
}

static jz_err encode_base_snapshot(const jazz_model_Snapshot *snapshot, jz_bytes *out) {
    groove_RecordDescriptor *dot = dot_descriptor();
    groove_ValueVec dots = { 0 };
    jz_err e = JZ_OK;
    for (u32 i = 0; i < snapshot->dots.len; i++) {
        const jazz_model_TxId *dot_id = &snapshot->dots.ptr[i];
        groove_ValueVec fields = { 0 };
        JZ_VEC_PUSH(fields, v_u64(dot_id->time));
        JZ_VEC_PUSH(fields, v_uuid(dot_id->node));
        groove_Value item;
        e = nested(dot, &fields, &item);
        values_drop(&fields);
        if (e.code) goto out;
        JZ_VEC_PUSH(dots, item);
    }
    {
        groove_RecordDescriptor *d = jazz_node__node__exclusive_read_evidence__base_snapshot_descriptor();
        groove_Value vals[5] = {
            v_u8(FORMAT_V1), v_uuid(snapshot->owner), v_u64(snapshot->global_base),
            v_u64(snapshot->local_base), v_array(dots),
        };
        e = groove__records__RecordDescriptor__create(d, vals, 5, out);
        groove__records__RecordDescriptor__drop_box(d);
        groove__records__RecordDescriptor__drop_box(dot);
        values_drop(&dots);
        return e;
    }
out:
    groove__records__RecordDescriptor__drop_box(dot);
    values_drop(&dots);
    return e;
}

/* Consumes `descriptor`, `item` and `reads`. */
jz_err jazz_node__node__exclusive_read_evidence__encode_reads(groove_RecordDescriptor *descriptor, groove_RecordDescriptor *item, jzn_value_rows *reads, jz_bytes *out) {
    groove_ValueVec nested_reads = { 0 };
    jz_err e = JZ_OK;
    for (u32 i = 0; i < reads->len; i++) {
        groove_Value v;
        e = nested(item, &reads->ptr[i], &v);
        if (e.code) goto done;
        JZ_VEC_PUSH(nested_reads, v);
    }
    {
        groove_Value vals[2] = { v_u8(FORMAT_V1), v_array(nested_reads) };
        e = groove__records__RecordDescriptor__create(descriptor, vals, 2, out);
    }
done:
    values_drop(&nested_reads);
    rows_drop(reads);
    groove__records__RecordDescriptor__drop_box(descriptor);
    groove__records__RecordDescriptor__drop_box(item);
    return e;
}

static jz_err encode_row_reads(const jazz_model_RowReadVec *reads, jz_bytes *out) {
    jzn_value_rows rows = { 0 };
    for (u32 i = 0; i < reads->len; i++) {
        const jazz_model_RowRead *read = &reads->ptr[i];
        groove_ValueVec row = { 0 };
        JZ_VEC_PUSH(row, v_string(jz_str_clone(&read->table)));
        JZ_VEC_PUSH(row, v_uuid(read->row));
        JZ_VEC_PUSH(row, v_u64(read->proof.time));
        JZ_VEC_PUSH(row, v_uuid(read->proof.node));
        JZ_VEC_PUSH(rows, row);
    }
    return jazz_node__node__exclusive_read_evidence__encode_reads(
        jazz_node__node__exclusive_read_evidence__row_reads_descriptor(), row_read_descriptor(), &rows, out);
}

static jz_err encode_absent_reads(const jazz_model_AbsentReadVec *reads, jz_bytes *out) {
    jzn_value_rows rows = { 0 };
    for (u32 i = 0; i < reads->len; i++) {
        const struct jazz_model_AbsentRead *read = &reads->ptr[i];
        groove_ValueVec row = { 0 };
        JZ_VEC_PUSH(row, v_string(jz_str_clone(&read->table)));
        JZ_VEC_PUSH(row, v_uuid(read->row_uuid));
        JZ_VEC_PUSH(rows, row);
    }
    return jazz_node__node__exclusive_read_evidence__encode_reads(
        jazz_node__node__exclusive_read_evidence__absent_reads_descriptor(), absent_read_descriptor(), &rows, out);
}

jz_err jazz_node__node__exclusive_read_evidence__decode_query(jz_slice bytes, struct jazz_model_Query **out);
static jz_err decode_binding_values_check(jz_slice bytes) {
    jz_btree values;
    jz_err e = jazz_model__query__binding_values_from_canonical_bytes(bytes, &values);
    if (e.code) return e;
    jz_btree_drop(&values);
    return JZ_OK;
}

/* `*some = false` when a predicate read cannot be represented in v1 (for
 * example a record-valued binding, which `jazz-binding-v0` cannot be decoded
 * back from). The caller then stores no evidence at all. */
static jz_err encode_predicate_reads(const jazz_model_PredicateReadVec *reads, bool *some, jz_bytes *out) {
    jzn_value_rows items = { 0 };
    *some = false;
    for (u32 i = 0; i < reads->len; i++) {
        const struct jazz_model_PredicateRead *read = &reads->ptr[i];
        jz_bytes query = { 0 };
        jz_pc_w w = { &query };
        jazz_model__query__Query__Serialize__serialize(&w, read->shape);
        jz_bytes bindings = { 0 };
        jz_err e = jazz_model__query__canonical_binding_bytes_for_values(&read->binding_values, &bindings);
        if (e.code) { jz_err_drop(&e); jz_bytes_drop(&query); rows_drop(&items); return JZ_OK; }
        jz_err e1 = decode_binding_values_check(jz_bytes_slice(&bindings));
        struct jazz_model_Query *q = NULL;
        jz_err e2 = e1.code ? JZ_OK : jazz_node__node__exclusive_read_evidence__decode_query(jz_bytes_slice(&query), &q);
        if (q) jazz_model__query__Query__drop(q);
        if (e1.code || e2.code) {
            jz_err_drop(&e1); jz_err_drop(&e2);
            jz_bytes_drop(&query); jz_bytes_drop(&bindings); rows_drop(&items);
            return JZ_OK;
        }
        groove_ValueVec row = { 0 };
        JZ_VEC_PUSH(row, v_string(jz_str_clone(&read->table)));
        JZ_VEC_PUSH(row, v_uuid(read->shape_id));
        JZ_VEC_PUSH(row, v_bytes(query));
        JZ_VEC_PUSH(row, v_uuid(read->binding_id));
        JZ_VEC_PUSH(row, v_bytes(bindings));
        JZ_VEC_PUSH(items, row);
    }
    JZ_TRY(jazz_node__node__exclusive_read_evidence__encode_reads(
        jazz_node__node__exclusive_read_evidence__predicate_reads_descriptor(), predicate_read_descriptor(), &items, out));
    *some = true;
    return JZ_OK;
}

static void none4(groove_Value out[4]) { for (int i = 0; i < 4; i++) out[i] = slot(NULL); }

/* Storage values for slots 5-8 of a `jazz_transactions` row.
 *
 * Evidence is kept only for an exclusive transaction whose fate is still
 * pending: that is the only time a stored unit may be retransmitted for
 * validation. It is all-or-nothing; any component that v1 cannot represent
 * leaves every slot null, which fails closed exactly as before this format. */
jz_err jazz_node__node__exclusive_read_evidence__evidence_slot_values(const jazz_model_Transaction *tx, bool pending, groove_Value out[4]) {
    if (!pending || jazz_model__tx__Transaction__kind(tx) != JAZZ_TXKIND_EXCLUSIVE) { none4(out); return JZ_OK; }
    const jazz_model_Snapshot *snapshot = jazz_model__tx__Transaction__base_snapshot(tx);
    if (!snapshot) { none4(out); return JZ_OK; }
    const jazz_model_PredicateReadVec *pred_set = jazz_model__tx__Transaction__predicate_read_set(tx);
    const jazz_model_RowReadVec *row_set = jazz_model__tx__Transaction__row_read_set(tx);
    const jazz_model_AbsentReadVec *absent_set = jazz_model__tx__Transaction__absent_read_set(tx);
    jz_bytes predicate = { 0 }, base = { 0 }, rows = { 0 }, absent = { 0 };
    bool has_predicate = false;
    if (pred_set) {
        bool some;
        JZ_TRY(encode_predicate_reads(pred_set, &some, &predicate));
        if (!some) { none4(out); return JZ_OK; }
        has_predicate = true;
    }
    jz_err e = encode_base_snapshot(snapshot, &base);
    if (e.code) { jz_bytes_drop(&predicate); return e; }
    if (row_set) {
        e = encode_row_reads(row_set, &rows);
        if (e.code) { jz_bytes_drop(&predicate); jz_bytes_drop(&base); return e; }
    }
    if (absent_set) {
        e = encode_absent_reads(absent_set, &absent);
        if (e.code) { jz_bytes_drop(&predicate); jz_bytes_drop(&base); jz_bytes_drop(&rows); return e; }
    }
    /* Store only evidence the decoder reads back as exactly this transaction's
     * evidence. A relay persists downstream units it did not author, so a
     * malformed unit (for example a binding id that does not match its
     * bindings) must not become a row every later read rejects. Anything
     * that does not round-trip is stored as no evidence and fails closed. */
    jz_slice sb = jz_bytes_slice(&base), sr = jz_bytes_slice(&rows), sa = jz_bytes_slice(&absent), sp = jz_bytes_slice(&predicate);
    jazz_node_StoredEvidence decoded;
    bool round_trips = false;
    jz_err de = jazz_node__node__exclusive_read_evidence__decode_evidence_slots(
        &sb, row_set ? &sr : NULL, absent_set ? &sa : NULL, has_predicate ? &sp : NULL, &decoded);
    if (!de.code) {
        round_trips = decoded.has_base_snapshot && jazz_model__tx__Snapshot__eq(&decoded.base_snapshot, snapshot)
            && (decoded.has_row_read_set == (row_set != NULL))
            && (!row_set || jazz_model__tx__RowReadVec__eq(&decoded.row_read_set, row_set))
            && (decoded.has_absent_read_set == (absent_set != NULL))
            && (!absent_set || jazz_model__tx__AbsentReadVec__eq(&decoded.absent_read_set, absent_set))
            && (decoded.has_predicate_read_set == (pred_set != NULL))
            && (!pred_set || jazz_model__tx__PredicateReadVec__eq(&decoded.predicate_read_set, pred_set));
        jazz_node__node__exclusive_read_evidence__StoredEvidence__drop(&decoded);
    } else {
        jz_err_drop(&de);
    }
    if (!round_trips) {
        jz_bytes_drop(&predicate); jz_bytes_drop(&base); jz_bytes_drop(&rows); jz_bytes_drop(&absent);
        none4(out);
        return JZ_OK;
    }
    out[0] = slot(&base);
    out[1] = slot(row_set ? &rows : NULL);
    out[2] = slot(absent_set ? &absent : NULL);
    out[3] = slot(has_predicate ? &predicate : NULL);
    return JZ_OK;
}

static jz_err invalid(const char *message) { return JZN_INVALID_STORED(message); }

jz_err jazz_node__node__exclusive_read_evidence__canonical_values(const groove_RecordDescriptor *descriptor, jz_slice bytes, const char *what, groove_ValueVec *out) {
    groove_ValueVec values = { 0 };
    jz_err e = groove__records__Record__to_values(bytes, descriptor, &values);
    if (e.code) { jz_err_drop(&e); return invalid(what); }
    jz_bytes re = { 0 };
    e = groove__records__RecordDescriptor__create(descriptor, values.ptr, values.len, &re);
    if (e.code) { jz_err_drop(&e); values_drop(&values); return invalid(what); }
    bool same = jz_slice_cmp(jz_bytes_slice(&re), bytes) == 0;
    jz_bytes_drop(&re);
    if (!same) { values_drop(&values); return invalid(what); }
    *out = values;
    return JZ_OK;
}

jz_err jazz_node__node__exclusive_read_evidence__versioned_reads(const groove_RecordDescriptor *descriptor, jz_slice bytes, const char *what, jzn_value_rows *out) {
    groove_ValueVec values;
    JZ_TRY(jazz_node__node__exclusive_read_evidence__canonical_values(descriptor, bytes, what, &values));
    if (values.len != 2 || values.ptr[0].tag != GROOVE_V_U8 || values.ptr[0].v.u != FORMAT_V1 || values.ptr[1].tag != GROOVE_V_ARRAY) {
        values_drop(&values);
        return invalid(what);
    }
    const groove_ValueVec *reads = &values.ptr[1].v.vals;
    jzn_value_rows rows = { 0 };
    for (u32 i = 0; i < reads->len; i++) {
        const groove_Value *read = &reads->ptr[i];
        groove_ValueVec fields = { 0 };
        jz_err e = read->tag == GROOVE_V_RECORD ? groove__records__OwnedRecord__to_values(read->v.record, &fields) : invalid(what);
        if (e.code) {
            if (read->tag == GROOVE_V_RECORD) { jz_err_drop(&e); e = invalid(what); }
            rows_drop(&rows); values_drop(&values);
            return e;
        }
        JZ_VEC_PUSH(rows, fields);
    }
    values_drop(&values);
    *out = rows;
    return JZ_OK;
}

jz_err jazz_node__node__exclusive_read_evidence__decode_query(jz_slice bytes, struct jazz_model_Query **out) {
    const char *what = "noncanonical exclusive predicate query";
    jz_pc_r r = { bytes.ptr, bytes.ptr + bytes.len, 0 };
    struct jazz_model_Query *query = NULL;
    jz_err e = jazz_model__query__Query__Deserialize__deserialize(&r, &query);
    if (e.code) { jz_err_drop(&e); return invalid(what); }
    bool rest_empty = r.p == r.end;
    jz_bytes again = { 0 };
    jz_pc_w w = { &again };
    jazz_model__query__Query__Serialize__serialize(&w, query);
    bool same = jz_slice_cmp(jz_bytes_slice(&again), bytes) == 0;
    jz_bytes_drop(&again);
    if (!rest_empty || !same) { jazz_model__query__Query__drop(query); return invalid(what); }
    *out = query;
    return JZ_OK;
}

static jz_err decode_base_snapshot(jz_slice bytes, jazz_model_Snapshot *out) {
    const char *what = "invalid exclusive base snapshot v1";
    groove_RecordDescriptor *d = jazz_node__node__exclusive_read_evidence__base_snapshot_descriptor();
    groove_ValueVec values;
    jz_err e = jazz_node__node__exclusive_read_evidence__canonical_values(d, bytes, what, &values);
    groove__records__RecordDescriptor__drop_box(d);
    if (e.code) return e;
    const groove_Value *v = values.ptr;
    if (values.len != 5 || v[0].tag != GROOVE_V_U8 || v[0].v.u != FORMAT_V1 || v[1].tag != GROOVE_V_UUID
        || v[2].tag != GROOVE_V_U64 || v[3].tag != GROOVE_V_U64 || v[4].tag != GROOVE_V_ARRAY) {
        values_drop(&values);
        return invalid(what);
    }
    jazz_model_TxIdVec dots = { 0 };
    for (u32 i = 0; i < v[4].v.vals.len; i++) {
        const groove_Value *dot = &v[4].v.vals.ptr[i];
        if (dot->tag != GROOVE_V_RECORD) goto bad;
        groove_ValueVec fields = { 0 };
        jz_err fe = groove__records__OwnedRecord__to_values(dot->v.record, &fields);
        if (fe.code) { jz_err_drop(&fe); goto bad; }
        if (fields.len == 2 && fields.ptr[0].tag == GROOVE_V_U64 && fields.ptr[1].tag == GROOVE_V_UUID) {
            jazz_model_TxId id = { fields.ptr[0].v.u, fields.ptr[1].v.uuid };
            JZ_VEC_PUSH(dots, id);
            values_drop(&fields);
        } else {
            values_drop(&fields);
            goto bad;
        }
    }
    {
        jz_err se = jazz_model__tx__Snapshot__exclusive_base(v[1].v.uuid, v[2].v.u, v[3].v.u, dots, out);
        values_drop(&values);
        if (se.code) { jz_err_drop(&se); return invalid(what); }
        return JZ_OK;
    }
bad:
    jz_vec_drop(&dots, sizeof(jazz_model_TxId), NULL);
    values_drop(&values);
    return invalid(what);
}

static jz_err decode_row_reads(jz_slice bytes, jazz_model_RowReadVec *out) {
    const char *what = "invalid exclusive row reads v1";
    groove_RecordDescriptor *d = jazz_node__node__exclusive_read_evidence__row_reads_descriptor();
    jzn_value_rows reads;
    jz_err e = jazz_node__node__exclusive_read_evidence__versioned_reads(d, bytes, what, &reads);
    groove__records__RecordDescriptor__drop_box(d);
    if (e.code) return e;
    jazz_model_RowReadVec result = { 0 };
    for (u32 i = 0; i < reads.len; i++) {
        const groove_Value *r = reads.ptr[i].ptr;
        if (reads.ptr[i].len == 4 && r[0].tag == GROOVE_V_STRING && r[1].tag == GROOVE_V_UUID && r[2].tag == GROOVE_V_U64 && r[3].tag == GROOVE_V_UUID) {
            jazz_model_RowRead rr = { jz_str_clone(&r[0].v.s), r[1].v.uuid, { r[2].v.u, r[3].v.uuid } };
            JZ_VEC_PUSH(result, rr);
        } else {
            jz_vec_drop(&result, sizeof(jazz_model_RowRead), jazz_model__tx__RowRead__drop);
            rows_drop(&reads);
            return invalid(what);
        }
    }
    rows_drop(&reads);
    *out = result;
    return JZ_OK;
}

static jz_err decode_absent_reads(jz_slice bytes, jazz_model_AbsentReadVec *out) {
    const char *what = "invalid exclusive absent reads v1";
    groove_RecordDescriptor *d = jazz_node__node__exclusive_read_evidence__absent_reads_descriptor();
    jzn_value_rows reads;
    jz_err e = jazz_node__node__exclusive_read_evidence__versioned_reads(d, bytes, what, &reads);
    groove__records__RecordDescriptor__drop_box(d);
    if (e.code) return e;
    jazz_model_AbsentReadVec result = { 0 };
    for (u32 i = 0; i < reads.len; i++) {
        const groove_Value *r = reads.ptr[i].ptr;
        if (reads.ptr[i].len == 2 && r[0].tag == GROOVE_V_STRING && r[1].tag == GROOVE_V_UUID) {
            struct jazz_model_AbsentRead ar = { jz_str_clone(&r[0].v.s), r[1].v.uuid };
            JZ_VEC_PUSH(result, ar);
        } else {
            jz_vec_drop(&result, sizeof(struct jazz_model_AbsentRead), jazz_model__tx__AbsentRead__drop);
            rows_drop(&reads);
            return invalid(what);
        }
    }
    rows_drop(&reads);
    *out = result;
    return JZ_OK;
}

static jz_err decode_predicate_reads(jz_slice bytes, jazz_model_PredicateReadVec *out) {
    const char *what = "invalid exclusive predicate reads v1";
    groove_RecordDescriptor *d = jazz_node__node__exclusive_read_evidence__predicate_reads_descriptor();
    jzn_value_rows reads;
    jz_err e = jazz_node__node__exclusive_read_evidence__versioned_reads(d, bytes, what, &reads);
    groove__records__RecordDescriptor__drop_box(d);
    if (e.code) return e;
    jazz_model_PredicateReadVec result = { 0 };
    for (u32 i = 0; i < reads.len; i++) {
        const groove_Value *r = reads.ptr[i].ptr;
        if (!(reads.ptr[i].len == 5 && r[0].tag == GROOVE_V_STRING && r[1].tag == GROOVE_V_UUID && r[2].tag == GROOVE_V_BYTES
              && r[3].tag == GROOVE_V_UUID && r[4].tag == GROOVE_V_BYTES)) {
            e = invalid(what);
            goto fail;
        }
        jz_btree binding_values;
        e = jazz_model__query__binding_values_from_canonical_bytes(jz_bytes_slice(&r[4].v.bytes), &binding_values);
        if (e.code) { jz_err_drop(&e); e = invalid(what); goto fail; }
        jz_uuid derived = jz_uuid_v5(&jazz_model__query__QUERY_NAMESPACE, r[4].v.bytes.ptr, r[4].v.bytes.len);
        if (!jz_uuid_eq(&derived, &r[3].v.uuid)) {
            jz_btree_drop(&binding_values);
            e = invalid("exclusive predicate binding id mismatch");
            goto fail;
        }
        struct jazz_model_Query *shape = NULL;
        e = jazz_node__node__exclusive_read_evidence__decode_query(jz_bytes_slice(&r[2].v.bytes), &shape);
        if (e.code) { jz_btree_drop(&binding_values); goto fail; }
        struct jazz_model_PredicateRead pr = { jz_str_clone(&r[0].v.s), r[1].v.uuid, shape, r[3].v.uuid, binding_values };
        JZ_VEC_PUSH(result, pr);
    }
    rows_drop(&reads);
    *out = result;
    return JZ_OK;
fail:
    jz_vec_drop(&result, sizeof(struct jazz_model_PredicateRead), jazz_model__tx__PredicateRead__drop);
    rows_drop(&reads);
    return e;
}

void jazz_node__node__exclusive_read_evidence__StoredEvidence__drop(jazz_node_StoredEvidence *s) {
    if (s->has_base_snapshot) jazz_model__tx__Snapshot__drop(&s->base_snapshot);
    if (s->has_row_read_set) jz_vec_drop(&s->row_read_set, sizeof(jazz_model_RowRead), jazz_model__tx__RowRead__drop);
    if (s->has_absent_read_set) jz_vec_drop(&s->absent_read_set, sizeof(struct jazz_model_AbsentRead), jazz_model__tx__AbsentRead__drop);
    if (s->has_predicate_read_set) jz_vec_drop(&s->predicate_read_set, sizeof(struct jazz_model_PredicateRead), jazz_model__tx__PredicateRead__drop);
}

/* Decoded slots 5-8. A null slot (NULL pointer) is valid and means "no evidence". */
jz_err jazz_node__node__exclusive_read_evidence__decode_evidence_slots(const jz_slice *base_snapshot, const jz_slice *row_read_set,
                                                                     const jz_slice *absent_read_set, const jz_slice *predicate_read_set,
                                                                     jazz_node_StoredEvidence *out) {
    if (!base_snapshot && (row_read_set || absent_read_set || predicate_read_set))
        return invalid("exclusive read sets stored without a base snapshot");
    jazz_node_StoredEvidence ev;
    memset(&ev, 0, sizeof ev);
    jz_err e = JZ_OK;
    if (base_snapshot) { e = decode_base_snapshot(*base_snapshot, &ev.base_snapshot); if (e.code) goto fail; ev.has_base_snapshot = true; }
    if (row_read_set) { e = decode_row_reads(*row_read_set, &ev.row_read_set); if (e.code) goto fail; ev.has_row_read_set = true; }
    if (absent_read_set) { e = decode_absent_reads(*absent_read_set, &ev.absent_read_set); if (e.code) goto fail; ev.has_absent_read_set = true; }
    if (predicate_read_set) { e = decode_predicate_reads(*predicate_read_set, &ev.predicate_read_set); if (e.code) goto fail; ev.has_predicate_read_set = true; }
    *out = ev;
    return JZ_OK;
fail:
    jazz_node__node__exclusive_read_evidence__StoredEvidence__drop(&ev);
    return e;
}
