/* groove::ivm::runtime::terminal: structured terminal snapshot ordering and
 * incremental-tree differencing. */
#include "mod.h"

static const usize FIELD0[1] = { 0 };

static void path_drop(groove_TerminalPath *p) {
    for (u32 i = 0; i < p->len; i++) { jz_str_drop(&p->ptr[i].collection); jz_bytes_drop(&p->ptr[i].key); }
    jz_vec_drop(p, sizeof(groove_TerminalPathSegment), NULL);
}
static groove_TerminalPath path_clone(const groove_TerminalPath *p) {
    groove_TerminalPath out = {0};
    jz_vec_reserve(&out, p->len, sizeof(groove_TerminalPathSegment));
    for (u32 i = 0; i < p->len; i++) {
        groove_TerminalPathSegment s = { p->ptr[i].tag, jz_str_clone(&p->ptr[i].collection), jz_bytes_clone(&p->ptr[i].key) };
        JZ_VEC_PUSH(out, s);
    }
    return out;
}

void groove_TerminalOperation_drop(void *p) {
    groove_TerminalOperation *op = p;
    jz_bytes_drop(&op->root_key);
    path_drop(&op->path);
    jz_bytes_drop(&op->edit.key);
    jz_bytes_drop(&op->edit.value);
}
void groove_TerminalDeltas_drop(groove_TerminalDeltas *d) { jz_vec_drop(&d->operations, sizeof(groove_TerminalOperation), groove_TerminalOperation_drop); }

/* Terminal child occurrence key v1: the first copy retains its typed row key;
 * subsequent copies append 0xff and a nonzero u64 big-endian repeat ordinal. */
jz_bytes groove__ivm__runtime__terminal__terminal_occurrence_key(jz_bytes row_key, u64 occurrence) {
    if (occurrence != 0) {
        jz_bytes_push_u8(&row_key, 0xff);
        u8 be[8];
        for (int i = 0; i < 8; i++) be[i] = (u8)(occurrence >> (56 - 8 * i));
        jz_bytes_push(&row_key, be, 8);
    }
    return row_key;
}

bool groove__ivm__runtime__terminal__TerminalDeltas__is_empty(const groove_TerminalDeltas *d) { return d->operations.len == 0; }

static void push_op(groove_TerminalOperationVec *ops, groove_RecordDescriptor root_descriptor, jz_slice root_key, groove_TerminalPath path, groove_TerminalEdit edit) {
    groove_TerminalOperation op = { root_descriptor, jz_bytes_from(root_key.ptr, root_key.len), path, edit };
    JZ_VEC_PUSH(*ops, op);
}

static void owned_record_drop_v(void *r) { groove_OwnedRecord_drop(r); }
static const jz_btree_desc KEY_RECORD_MAP = { sizeof(jz_bytes), sizeof(groove_OwnedRecord), jz_cmp_bytes, groove_ivm_rt_bytes_drop_v, owned_record_drop_v };
typedef struct IndexedRecord { u32 index; groove_OwnedRecord record; } IndexedRecord;
static void indexed_record_drop_v(void *r) { groove_OwnedRecord_drop(&((IndexedRecord *)r)->record); }
static const jz_btree_desc KEY_INDEXED_MAP = { sizeof(jz_bytes), sizeof(IndexedRecord), jz_cmp_bytes, groove_ivm_rt_bytes_drop_v, indexed_record_drop_v };
static const jz_btree_desc KEY_U64_MAP = { sizeof(jz_bytes), sizeof(u64), jz_cmp_bytes, groove_ivm_rt_bytes_drop_v, NULL };

static jz_err diff_terminal_collection(jz_slice root_key, groove_RecordDescriptor root_descriptor, groove_TerminalPath path,
    const groove_ValueVec *before_values, const groove_ValueVec *after_values, groove_TerminalOperationVec *operations);

static jz_err diff_terminal_record(jz_slice root_key, groove_RecordDescriptor root_descriptor, groove_TerminalPath path,
    const groove_OwnedRecord *before, const groove_OwnedRecord *after, groove_TerminalOperationVec *operations) {
    groove_ValueVec before_values = {0}, after_values = {0};
    groove_BorrowedRecord bb = groove__records__BorrowedRecord__new(jz_bytes_slice(&before->raw), before->descriptor);
    groove_BorrowedRecord ab = groove__records__BorrowedRecord__new(jz_bytes_slice(&after->raw), after->descriptor);
    jz_err e = groove__records__BorrowedRecord__to_values(&bb, &before_values);
    if (!JZ_IS_ERR(e)) e = groove__records__BorrowedRecord__to_values(&ab, &after_values);
    bool scalar_changed = false;
    u32 n = before_values.len < after_values.len ? before_values.len : after_values.len;
    for (u32 index = 0; index < n && !JZ_IS_ERR(e); index++) {
        if (index >= groove__records__RecordDescriptor__len(after->descriptor)) {
            e = groove__ivm__runtime__IvmRuntimeError__with_usize(GROOVE_IVM_GraphFieldIndexOutOfBounds, index);
            break;
        }
        const groove_DescriptorField *df = groove__records__RecordDescriptor__field(after->descriptor, index);
        const groove_Value *bv = &before_values.ptr[index], *av = &after_values.ptr[index];
        if (bv->tag == groove_Value_Array && av->tag == groove_Value_Array && df->value_type.tag == groove_ValueType_Array &&
            df->value_type.v.inner->tag == groove_ValueType_Record) {
            if (!df->has_name) { e = GROOVE_IVM_ERR_C(GROOVE_IVM_GraphFieldNotFound, "<unnamed>"); break; }
            groove_TerminalPath child_path = path_clone(&path);
            groove_TerminalPathSegment seg = { groove_TerminalPathSegment_Collection, jz_str_clone(&df->name), {0} };
            JZ_VEC_PUSH(child_path, seg);
            e = diff_terminal_collection(root_key, root_descriptor, child_path, &bv->v.vals, &av->v.vals, operations);
        } else if (!groove_Value_eq(bv, av)) {
            scalar_changed = true;
        }
    }
    jz_vec_drop(&before_values, sizeof(groove_Value), groove_Value_drop_v);
    jz_vec_drop(&after_values, sizeof(groove_Value), groove_Value_drop_v);
    if (JZ_IS_ERR(e)) { path_drop(&path); return e; }
    if (scalar_changed) {
        jz_bytes key;
        if (path.len && path.ptr[path.len - 1].tag == groove_TerminalPathSegment_Key) key = jz_bytes_clone(&path.ptr[path.len - 1].key);
        else if (path.len == 0) key = jz_bytes_from(root_key.ptr, root_key.len); /* a root update addresses its root */
        else {
            e = groove__ivm__runtime__windows__encoded_record_key_part(after->descriptor, (after->raw).ptr, (after->raw).len, FIELD0, 1, &key);
            if (JZ_IS_ERR(e)) { path_drop(&path); return e; }
        }
        groove_TerminalEdit edit = { groove_TerminalEdit_Update, 0, key, jz_bytes_clone(&after->raw) };
        push_op(operations, root_descriptor, root_key, path, edit);
        return JZ_OK;
    }
    path_drop(&path);
    return JZ_OK;
}

static jz_err collection_children(const groove_ValueVec *values, jz_btree *children) {
    jz_btree_init(children, &KEY_INDEXED_MAP);
    jz_btree occurrences; jz_btree_init(&occurrences, &KEY_U64_MAP);
    jz_err e = JZ_OK;
    for (u32 index = 0; index < values->len; index++) {
        const groove_Value *value = &values->ptr[index];
        if (value->tag != groove_Value_Record) {
            e = GROOVE_IVM_ERR_C(GROOVE_IVM_InvalidCollectBy, "structured terminal arrays must contain records");
            break;
        }
        const groove_OwnedRecord *record = &value->v.record;
        jz_bytes row_key;
        e = groove__ivm__runtime__windows__encoded_record_key_part(record->descriptor, (record->raw).ptr, (record->raw).len, FIELD0, 1, &row_key);
        if (JZ_IS_ERR(e)) break;
        jz_bytes rk = jz_bytes_clone(&row_key);
        bool inserted;
        u64 *occurrence = jz_btree_entry(&occurrences, &rk, &inserted);
        if (!inserted) jz_bytes_drop(&rk);
        jz_bytes key = groove__ivm__runtime__terminal__terminal_occurrence_key(row_key, *occurrence);
        *occurrence += 1;
        IndexedRecord ir = { index, {{0}, 0} };
        groove_OwnedRecord_clone(&ir.record, record);
        if (jz_btree_insert(children, &key, &ir, NULL)) { e = GROOVE_IVM_ERR(GROOVE_IVM_DuplicateCollectByOccurrenceId); break; }
    }
    jz_btree_drop(&occurrences);
    if (JZ_IS_ERR(e)) jz_btree_drop(children);
    return e;
}

static void union_keys(jz_btree *keys, const jz_btree *a, const jz_btree *b) {
    jz_btree_init(keys, &groove_ivm_rt_BYTES_SET);
    const jz_btree *srcs[2] = { a, b };
    for (int s = 0; s < 2; s++) {
        jz_bytes *k; void *v;
        JZ_BTREE_FOR(srcs[s], k, v) {
            if (!jz_btree_contains(keys, k)) { jz_bytes c = jz_bytes_clone(k); jz_btree_insert(keys, &c, NULL, NULL); }
        }
    }
}

static jz_err diff_terminal_collection(jz_slice root_key, groove_RecordDescriptor root_descriptor, groove_TerminalPath path,
    const groove_ValueVec *before_values, const groove_ValueVec *after_values, groove_TerminalOperationVec *operations) {
    jz_btree before, after;
    jz_err e = collection_children(before_values, &before);
    if (JZ_IS_ERR(e)) { path_drop(&path); return e; }
    e = collection_children(after_values, &after);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&before); path_drop(&path); return e; }
    jz_btree keys;
    union_keys(&keys, &before, &after);
    jz_bytes *key; void *unit;
    JZ_BTREE_FOR(&keys, key, unit) {
        IndexedRecord *b = jz_btree_get(&before, key), *a = jz_btree_get(&after, key);
        if (!b && a) {
            groove_TerminalEdit edit = { groove_TerminalEdit_Insert, a->index, jz_bytes_clone(key), jz_bytes_clone(&a->record.raw) };
            push_op(operations, root_descriptor, root_key, path_clone(&path), edit);
        } else if (b && !a) {
            groove_TerminalEdit edit = { groove_TerminalEdit_Remove, 0, jz_bytes_clone(key), {0} };
            push_op(operations, root_descriptor, root_key, path_clone(&path), edit);
        } else if (b && a) {
            if (b->index != a->index) {
                groove_TerminalEdit edit = { groove_TerminalEdit_Move, a->index, jz_bytes_clone(key), {0} };
                push_op(operations, root_descriptor, root_key, path_clone(&path), edit);
            }
            groove_TerminalPath descendant_path = path_clone(&path);
            groove_TerminalPathSegment seg = { groove_TerminalPathSegment_Key, {0}, jz_bytes_clone(key) };
            JZ_VEC_PUSH(descendant_path, seg);
            e = diff_terminal_record(root_key, root_descriptor, descendant_path, &b->record, &a->record, operations);
            if (JZ_IS_ERR(e)) break;
        } else {
            jz_panic("child key came from before or after");
        }
    }
    jz_btree_drop(&keys); jz_btree_drop(&before); jz_btree_drop(&after);
    path_drop(&path);
    return e;
}

static jz_err terminal_deltas_with_keys(const groove_RecordDeltas *deltas, groove_BytesVec *keys_in, groove_TerminalDeltas *out) {
    jz_btree before, after;
    jz_btree_init(&before, &KEY_RECORD_MAP);
    jz_btree_init(&after, &KEY_RECORD_MAP);
    for (u32 i = 0; i < deltas->deltas.len && i < keys_in->len; i++) {
        const groove_RecordDelta *delta = &deltas->deltas.ptr[i];
        jz_bytes key = keys_in->ptr[i];
        keys_in->ptr[i] = (jz_bytes){0};
        groove_OwnedRecord record = { jz_bytes_clone(&delta->record), deltas->descriptor };
        if (delta->weight < 0) jz_btree_insert(&before, &key, &record, NULL);
        else if (delta->weight > 0) jz_btree_insert(&after, &key, &record, NULL);
        else { jz_bytes_drop(&key); groove_OwnedRecord_drop(&record); }
    }
    jz_vec_drop(keys_in, sizeof(jz_bytes), groove_ivm_rt_bytes_drop_v);
    jz_btree keys;
    union_keys(&keys, &before, &after);
    memset(out, 0, sizeof(*out));
    jz_err e = JZ_OK;
    jz_bytes *key; void *unit;
    JZ_BTREE_FOR(&keys, key, unit) {
        groove_OwnedRecord *b = jz_btree_get(&before, key), *a = jz_btree_get(&after, key);
        groove_TerminalPath empty = {0};
        if (!b && a) {
            groove_TerminalEdit edit = { groove_TerminalEdit_Insert, 0, jz_bytes_clone(key), jz_bytes_clone(&a->raw) };
            push_op(&out->operations, deltas->descriptor, jz_bytes_slice(key), empty, edit);
        } else if (b && !a) {
            groove_TerminalEdit edit = { groove_TerminalEdit_Remove, 0, jz_bytes_clone(key), {0} };
            push_op(&out->operations, deltas->descriptor, jz_bytes_slice(key), empty, edit);
        } else if (b && a) {
            e = diff_terminal_record(jz_bytes_slice(key), deltas->descriptor, empty, b, a, &out->operations);
            if (JZ_IS_ERR(e)) break;
        } else {
            jz_panic("terminal key came from before or after");
        }
    }
    jz_btree_drop(&keys); jz_btree_drop(&before); jz_btree_drop(&after);
    if (JZ_IS_ERR(e)) groove_TerminalDeltas_drop(out);
    return e;
}

jz_err groove__ivm__runtime__terminal__terminal_deltas_from_record_deltas(const groove_RecordDeltas *deltas, groove_TerminalDeltas *out) {
    groove_BytesVec keys = {0};
    for (u32 i = 0; i < deltas->deltas.len; i++) {
        jz_bytes key;
        jz_err e = groove__ivm__runtime__windows__encoded_record_key_part(deltas->descriptor, (deltas->deltas.ptr[i].record).ptr, (deltas->deltas.ptr[i].record).len, FIELD0, 1, &key);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&keys, sizeof(jz_bytes), groove_ivm_rt_bytes_drop_v); return e; }
        JZ_VEC_PUSH(keys, key);
    }
    return terminal_deltas_with_keys(deltas, &keys, out);
}

/* Terminal edits of a TopBy output whose root keys are the identity
 * key_fields of `physical`, the same deltas before indirect values were
 * materialized for publication. */
jz_err groove__ivm__runtime__terminal__terminal_deltas_keyed_by_identity(const groove_RecordDeltas *deltas, const groove_RecordDeltas *physical, const u32 *key_fields, u32 n, groove_TerminalDeltas *out) {
    if (physical->deltas.len != deltas->deltas.len) return GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch);
    groove_BytesVec keys = {0};
    for (u32 i = 0; i < physical->deltas.len; i++) {
        jz_bytes key;
        jz_err e = groove__ivm__runtime__windows__encoded_identity_key_part(physical->descriptor, (physical->deltas.ptr[i].record).ptr, (physical->deltas.ptr[i].record).len, (const usize *)key_fields, n, &key);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&keys, sizeof(jz_bytes), groove_ivm_rt_bytes_drop_v); return e; }
        JZ_VEC_PUSH(keys, key);
    }
    return terminal_deltas_with_keys(deltas, &keys, out);
}

typedef struct KeyedDelta { u32 position; jz_bytes key; groove_RecordDelta delta; } KeyedDelta;
static int keyed_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const KeyedDelta *x = a, *y = b;
    if (x->position != y->position) return x->position < y->position ? -1 : 1;
    return jz_bytes_cmp(&x->key, &y->key);
}
static const jz_btree_desc KEY_POS_MAP = { sizeof(jz_bytes), sizeof(u32), jz_cmp_bytes, groove_ivm_rt_bytes_drop_v, NULL };

jz_err groove__ivm__runtime__terminal__order_terminal_snapshot(groove_RecordDeltas *terminal, const groove_RecordDeltas *ordering) {
    jz_btree positions; jz_btree_init(&positions, &KEY_POS_MAP);
    u32 index = 0;
    for (u32 i = 0; i < ordering->deltas.len; i++) {
        const groove_RecordDelta *delta = &ordering->deltas.ptr[i];
        if (delta->weight <= 0) continue;
        jz_bytes key;
        jz_err e = groove__ivm__runtime__windows__encoded_record_key_part(ordering->descriptor, (delta->record).ptr, (delta->record).len, FIELD0, 1, &key);
        if (JZ_IS_ERR(e)) { jz_btree_drop(&positions); return e; }
        bool inserted;
        u32 *slot = jz_btree_entry(&positions, &key, &inserted);
        if (inserted) *slot = index; else jz_bytes_drop(&key);
        index++;
    }
    JZ_VEC(KeyedDelta) keyed = {0};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < terminal->deltas.len; i++) {
        groove_RecordDelta delta = terminal->deltas.ptr[i];
        KeyedDelta kd = { UINT32_MAX, {0}, delta };
        if (!JZ_IS_ERR(e)) e = groove__ivm__runtime__windows__encoded_record_key_part(terminal->descriptor, (delta.record).ptr, (delta.record).len, FIELD0, 1, &kd.key);
        if (!JZ_IS_ERR(e)) {
            u32 *pos = jz_btree_get(&positions, &kd.key);
            if (pos) kd.position = *pos;
        }
        JZ_VEC_PUSH(keyed, kd);
    }
    terminal->deltas.len = 0;
    jz_btree_drop(&positions);
    if (JZ_IS_ERR(e)) {
        for (u32 i = 0; i < keyed.len; i++) { jz_bytes_drop(&keyed.ptr[i].key); groove_RecordDelta_drop(&keyed.ptr[i].delta); }
        jz_vec_drop(&keyed, sizeof(KeyedDelta), NULL);
        return e;
    }
    jz_sort(keyed.ptr, keyed.len, sizeof(KeyedDelta), keyed_cmp, NULL);
    for (u32 i = 0; i < keyed.len; i++) { jz_bytes_drop(&keyed.ptr[i].key); JZ_VEC_PUSH(terminal->deltas, keyed.ptr[i].delta); }
    jz_vec_drop(&keyed, sizeof(KeyedDelta), NULL);
    return JZ_OK;
}
