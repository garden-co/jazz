/* groove::ivm::runtime::persist: durable persist operator writes. */
#include "mod.h"

typedef struct PendingPersistKey {
    i64 weight;
    bool has_positive_record; jz_bytes positive_record;
    jz_hmap unique_record_deltas; /* Bytes -> i64 */
} PendingPersistKey;

static const jz_hmap_desc RECORD_WEIGHTS = { sizeof(jz_bytes), sizeof(i64), groove_ivm_rt_bytes_hash_v, groove_ivm_rt_bytes_eq_v, groove_ivm_rt_bytes_drop_v, NULL };
static void pending_drop(void *p) {
    PendingPersistKey *k = p;
    if (k->has_positive_record) jz_bytes_drop(&k->positive_record);
    if (k->unique_record_deltas.d) jz_hmap_drop(&k->unique_record_deltas);
}
static const jz_btree_desc PENDING = { sizeof(jz_bytes), sizeof(PendingPersistKey), jz_cmp_bytes, groove_ivm_rt_bytes_drop_v, pending_drop };

static PendingPersistKey *pending_entry(jz_btree *pending, jz_bytes key) {
    bool inserted;
    PendingPersistKey *e = jz_btree_entry(pending, &key, &inserted);
    if (inserted) jz_hmap_init(&e->unique_record_deltas, &RECORD_WEIGHTS);
    else jz_bytes_drop(&key);
    return e;
}

static void add_pending_delta(PendingPersistKey *entry, const jz_bytes *record, i64 weight, bool unique) {
    if (weight == 0) return;
    entry->weight += weight;
    if (unique) {
        i64 *current = jz_hmap_get(&entry->unique_record_deltas, record);
        if (current) *current += weight;
        else { jz_bytes k = jz_bytes_clone(record); jz_hmap_insert(&entry->unique_record_deltas, &k, &weight, NULL); }
    } else if (weight > 0) {
        if (entry->has_positive_record) jz_bytes_drop(&entry->positive_record);
        entry->positive_record = jz_bytes_clone(record);
        entry->has_positive_record = true;
    }
}

static groove_OwnedWriteOperation set_op(const groove_DurableStorage *d, jz_bytes key, jz_bytes value) {
    groove_OwnedWriteOperation op = { groove_WriteOp_Set, jz_str_clone(&d->column_family), key, value };
    return op;
}
static groove_OwnedWriteOperation delete_op(const groove_DurableStorage *d, jz_bytes key) {
    groove_OwnedWriteOperation op = { groove_WriteOp_Delete, jz_str_clone(&d->column_family), key, {0} };
    return op;
}

static jz_err get_raw(groove_Storage storage, const groove_DurableStorage *d, jz_slice key, bool *found, jz_bytes *out) {
    return storage.vt->get(storage.self, &d->column_family, key, found, out);
}

static jz_err resolve_unique_owner(groove_Storage storage, const groove_DurableStorage *d, jz_slice key, PendingPersistKey *entry, bool *has_owner, jz_bytes *owner) {
    bool found = false;
    jz_bytes durable_owner = {0};
    JZ_TRY(get_raw(storage, d, key, &found, &durable_owner));
    bool durable_owner_survives = false;
    if (found) {
        i64 w = 0;
        if (jz_hmap_remove(&entry->unique_record_deltas, &durable_owner, NULL, &w)) {}
        durable_owner_survives = w >= 0;
    }
    *has_owner = found && durable_owner_survives;
    if (*has_owner) *owner = durable_owner; else if (found) jz_bytes_drop(&durable_owner);
    jz_bytes *record; i64 *delta;
    JZ_HMAP_FOR(&entry->unique_record_deltas, record, delta) {
        if (*delta <= 0) continue;
        if (*has_owner) {
            jz_bytes_drop(owner);
            *has_owner = false;
            return groove__ivm__runtime__IvmRuntimeError__with_str(GROOVE_IVM_UniqueIndexViolation, d->name.ptr, d->name.len);
        }
        *owner = jz_bytes_clone(record);
        *has_owner = true;
    }
    return JZ_OK;
}

static jz_err unique_pending_operations(groove_Storage storage, const groove_DurableStorage *d, jz_btree *pending, groove_WriteOpVec *operations) {
    jz_vec_reserve(operations, pending->len, sizeof(groove_OwnedWriteOperation));
    jz_bytes key; PendingPersistKey entry;
    while (jz_btree_pop_first(pending, &key, &entry)) {
        bool has; jz_bytes record;
        jz_err e = resolve_unique_owner(storage, d, jz_bytes_slice(&key), &entry, &has, &record);
        pending_drop(&entry);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&key); return e; }
        if (has) JZ_VEC_PUSH(*operations, set_op(d, key, record));
        else JZ_VEC_PUSH(*operations, delete_op(d, key));
    }
    return JZ_OK;
}

/* Write the consolidated durable state of every pending key after `leading`,
 * in one atomic storage write. */
static jz_err write_pending(groove_Storage storage, const groove_DurableStorage *d, bool unique, jz_btree *pending, groove_WriteOpVec leading) {
    groove_WriteOpVec operations = leading;
    jz_err e = JZ_OK;
    if (unique) {
        e = unique_pending_operations(storage, d, pending, &operations);
        if (!JZ_IS_ERR(e)) e = storage.vt->write_batch(storage.self, &operations);
        jz_btree_drop(pending);
        jz_vec_drop(&operations, sizeof(groove_OwnedWriteOperation), groove_OwnedWriteOperation_drop);
        return e;
    }
    /* `pending` is already ordered. Consume it directly into owned operations. */
    jz_vec_reserve(&operations, pending->len, sizeof(groove_OwnedWriteOperation));
    jz_bytes key; PendingPersistKey entry;
    while (!JZ_IS_ERR(e) && jz_btree_pop_first(pending, &key, &entry)) {
        if (entry.weight > 0) {
            if (!entry.has_positive_record) { e = GROOVE_IVM_ERR(GROOVE_IVM_PersistRecordMismatch); jz_bytes_drop(&key); }
            else { JZ_VEC_PUSH(operations, set_op(d, key, entry.positive_record)); entry.has_positive_record = false; }
        } else if (entry.weight < 0) {
            JZ_VEC_PUSH(operations, delete_op(d, key));
        } else if (entry.has_positive_record) {
            bool found = false; jz_bytes existing = {0};
            e = get_raw(storage, d, jz_bytes_slice(&key), &found, &existing);
            if (found) jz_bytes_drop(&existing);
            if (!JZ_IS_ERR(e) && found) { JZ_VEC_PUSH(operations, set_op(d, key, entry.positive_record)); entry.has_positive_record = false; }
            else jz_bytes_drop(&key);
        } else {
            jz_bytes_drop(&key);
        }
        pending_drop(&entry);
    }
    if (!JZ_IS_ERR(e)) e = storage.vt->write_batch(storage.self, &operations);
    jz_btree_drop(pending);
    jz_vec_drop(&operations, sizeof(groove_OwnedWriteOperation), groove_OwnedWriteOperation_drop);
    return e;
}

/* Persist IndexBy records as compact durable index entries. */
static jz_err apply_index_persist_delta(groove_Storage storage, const groove_DurableStorage *d, bool unique, const groove_RecordDeltas *delta, groove_WriteOpVec leading) {
    jz_btree pending; jz_btree_init(&pending, &PENDING);
    for (u32 i = 0; i < delta->deltas.len; i++) {
        const groove_RecordDelta *rd = &delta->deltas.ptr[i];
        if (rd->weight == 0) continue;
        groove_BorrowedRecord record = groove_RecordDelta_borrowed(rd, delta->descriptor);
        jz_slice logical_key, value;
        jz_err e = groove__records__BorrowedRecord__get_bytes(&record, 0, &logical_key);
        if (!JZ_IS_ERR(e)) e = groove__records__BorrowedRecord__get_bytes(&record, 1, &value);
        if (JZ_IS_ERR(e)) {
            jz_btree_drop(&pending);
            jz_vec_drop(&leading, sizeof(groove_OwnedWriteOperation), groove_OwnedWriteOperation_drop);
            return e;
        }
        jz_bytes key = jz_bytes_clone(&d->key_prefix);
        jz_bytes_push(&key, logical_key.ptr, logical_key.len);
        jz_bytes v = jz_bytes_from(value.ptr, value.len);
        add_pending_delta(pending_entry(&pending, key), &v, rd->weight, unique);
        jz_bytes_drop(&v);
    }
    return write_pending(storage, d, unique, &pending, leading);
}

typedef struct FieldCtx { groove_RecordDescriptor d; jz_slice record; const u32 *key_fields; } FieldCtx;
static jz_err field_value_at(u32 i, groove_Value *out, void *c) {
    FieldCtx *f = c;
    return groove__ivm__runtime__key_encoding__named_field_value(f->d, f->record, f->key_fields[i], out);
}
static jz_err encode_part(jz_bytes *key, const groove_Value *v, void *ctx) { (void)ctx; return groove__ivm__runtime__key_encoding__encode_key_part(key, v); }

static jz_err persist_record_keys(groove_RecordDescriptor d, jz_slice record, const u32 *key_fields, u32 n, const groove_DurableStorage *durable, groove_BytesVec *out) {
    FieldCtx ctx = { d, record, key_fields };
    return groove__ivm__runtime__key_encoding__composite_keys(jz_bytes_clone(&durable->key_prefix), n, field_value_at, &ctx, encode_part, NULL, out);
}

jz_err groove__ivm__runtime__persist__apply_persist_delta(groove_Storage storage, const groove_DurableStorage *durable, const u32 *key_fields, u32 nkeys, bool unique, const groove_RecordDeltas *delta) {
    groove_WriteOpVec leading = {0};
    return groove__ivm__runtime__persist__apply_persist_delta_with(storage, durable, key_fields, nkeys, unique, delta, leading);
}

/* Like apply_persist_delta, and submit `leading` in the same atomic write. */
jz_err groove__ivm__runtime__persist__apply_persist_delta_with(groove_Storage storage, const groove_DurableStorage *durable, const u32 *key_fields, u32 nkeys, bool unique, const groove_RecordDeltas *delta, groove_WriteOpVec leading) {
    if (nkeys == 1 && key_fields[0] == 0 && delta->descriptor == groove__ivm__runtime__record_projection__index_record_descriptor())
        return apply_index_persist_delta(storage, durable, unique, delta, leading);
    /* Multiple deltas in one tick may touch the same durable key. Consolidate
     * by persisted key before writing. */
    jz_btree pending; jz_btree_init(&pending, &PENDING);
    for (u32 i = 0; i < delta->deltas.len; i++) {
        const groove_RecordDelta *rd = &delta->deltas.ptr[i];
        groove_BytesVec keys;
        jz_err e = persist_record_keys(delta->descriptor, jz_bytes_slice(&rd->record), key_fields, nkeys, durable, &keys);
        if (JZ_IS_ERR(e)) {
            jz_btree_drop(&pending);
            jz_vec_drop(&leading, sizeof(groove_OwnedWriteOperation), groove_OwnedWriteOperation_drop);
            return e;
        }
        for (u32 k = 0; k < keys.len; k++) {
            if (rd->weight == 0) { jz_bytes_drop(&keys.ptr[k]); continue; }
            add_pending_delta(pending_entry(&pending, keys.ptr[k]), &rd->record, rd->weight, unique);
        }
        jz_vec_drop(&keys, sizeof(jz_bytes), NULL);
    }
    return write_pending(storage, durable, unique, &pending, leading);
}

/* Rebuild the in-memory IndexBy record of one durable index entry from its
 * storage key (minus the id prefix) and value. */
jz_err groove__ivm__runtime__persist__index_record_from_storage(u32 prefix_len, jz_slice key, jz_bytes value, jz_bytes *out) {
    if (prefix_len > key.len) {
        jz_bytes_drop(&value);
        return GROOVE_IVM_ERR_C(GROOVE_IVM_InvalidPersistedIndex, "index key shorter than its id prefix");
    }
    groove_Value values[2];
    values[0].tag = groove_Value_Bytes; values[0].v.bytes = jz_bytes_from(key.ptr + prefix_len, key.len - prefix_len);
    values[1].tag = groove_Value_Bytes; values[1].v.bytes = value;
    jz_err e = groove__records__RecordDescriptor__create(groove__ivm__runtime__record_projection__index_record_descriptor(), values, 2, out);
    groove_Value_drop(&values[0]); groove_Value_drop(&values[1]);
    return e;
}
