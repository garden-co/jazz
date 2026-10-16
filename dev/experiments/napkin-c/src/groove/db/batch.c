/* db/batch.c: port of crates/groove/src/db/batch.rs.
 * Mutable collection of table writes committed atomically at storage level.
 * (StagedDatabaseBatch is #[cfg(test)] and not ported.) */
#include "mod.h"

/* ======================= DatabaseBatch ======================= */
static void str_drop_v(void *p) { jz_str_drop(p); }
static void pending_drop_vec(groove_PendingTableWriteVec *v) { jz_vec_drop(v, sizeof(groove_PendingTableWrite), groove__db__PendingTableWrite__drop); }

void groove__db__batch__DatabaseBatch__default(groove_DatabaseBatch *b) {
    memset(b, 0, sizeof *b);
    jz_btree_init(&b->exact_keys, &groove__db__EXACT_KEYS_DESC);
    jz_btree_init(&b->written_tables, &groove__db__STR_SET_DESC);
    b->notification_timing = GROOVE_NOTIFY_IMMEDIATE;
}
void groove__db__batch__DatabaseBatch__drop(groove_DatabaseBatch *b) {
    jz_vec_drop(&b->operations, sizeof(groove_BatchOperation), groove__db__batch__BatchOperation__drop);
    if (b->exact_owner) jz_rc_release(b->exact_owner);
    jz_btree_drop(&b->exact_keys);
    if (b->txn_operations) jz_rc_release(b->txn_operations);
    if (b->prepared.owner) jz_rc_release(b->prepared.owner);
    pending_drop_vec(&b->prepared.writes);
    jz_vec_drop(&b->accepted_large_values, sizeof(groove_StagedLargeValueId), NULL);
    jz_btree_drop(&b->written_tables);
}
/* RefCell<StagedWriteState>: created on first use. */
groove_StagedWriteState *groove__db__batch__DatabaseBatch__txn_operations(groove_DatabaseBatch *b) {
    if (!b->txn_operations) b->txn_operations = groove__storage__StagedWriteState__default();
    return b->txn_operations;
}

/* Stage an immutable record, or accept its identical existing bytes. A conflict
 * poisons this batch, never the database. */
jz_err groove__db__batch__DatabaseBatch__ensure_exact(groove_DatabaseBatch *self, const groove_Database *database, jz_str table, groove_PrimaryKeyValue key,
                                                      groove_RawRecordInput record, u8 *out) {
    jz_err e = groove__db__commit__Database__ensure_not_poisoned(database);
    if (!e.code) e = groove__db__batch__DatabaseBatch__check_exact_base(self, database);
    if (e.code) {
        jz_str_drop(&table);
        groove__db__batch__PrimaryKeyValue__drop(&key);
        groove__db__batch__RawRecordInput__drop(&record);
        return e;
    }
    self->has_exact_base = true;
    self->exact_base_owner = (u32)(uintptr_t)database->immutable_batch_owner;
    self->exact_base_publication = database->next_publication_id;
    if (self->exact_owner) jz_rc_release(self->exact_owner);
    self->exact_owner = jz_rc_clone(database->immutable_batch_owner);
    groove_BatchOperation operation;
    memset(&operation, 0, sizeof operation);
    operation.tag = GROOVE_BATCH_INSERT_RAW_FRESH;
    operation.table = jz_str_clone(&table);
    operation.key = key;
    operation.raw = record;
    groove_PendingTableWrite pending;
    e = groove__db__commit__Database__pending_write_from_operation(database, &operation, &pending);
    if (e.code) {
        jz_str_drop(&table);
        groove__db__batch__BatchOperation__drop(&operation);
        return e;
    }
    jz_bytes expected;
    if (!groove__db__PendingTableWrite__stored_record(&pending, &expected)) jz_panic("immutable insert has bytes");
    e = groove__db__commit__Database__ensure_batch_storage_txn(database, self);
    u8 comparison = GROOVE_VALUE_CMP_ABSENT;
    if (!e.code) {
        groove_StagedWriteOverlay *resident = groove__db__facade__Database__resident_storage(database);
        groove_StagedWriteOverlay *overlay = groove__storage__StagedWriteOverlay__new(groove__storage__StagedWriteOverlay__as_dyn(resident),
                                                                                       groove__db__batch__DatabaseBatch__txn_operations(self));
        groove_MeteredStorage storage = groove__db__MeteredStorage__new(groove__storage__StagedWriteOverlay__as_dyn(overlay), database->storage_read_metrics);
        e = groove__storage__OrderedKvStorage__compare_value(groove__db__MeteredStorage__as_dyn(&storage), jz_str_clone(&table), jz_bytes_clone(&pending.key),
                                                             jz_bytes_clone(&expected), &comparison);
        groove__db__MeteredStorage__drop(&storage);
        groove__storage__StagedWriteOverlay__release(overlay);
        groove__storage__StagedWriteOverlay__release(resident);
    }
    if (e.code) {
        jz_str_drop(&table);
        jz_bytes_drop(&expected);
        groove__db__PendingTableWrite__drop(&pending);
        groove__db__batch__BatchOperation__drop(&operation);
        return e;
    }
    if (comparison != GROOVE_VALUE_CMP_DIFFERENT) {
        groove_ExactKey coordinate = { jz_str_clone(&table), jz_bytes_clone(&pending.key) };
        jz_bytes old;
        if (jz_btree_insert(&self->exact_keys, &coordinate, &expected, &old)) {
            jz_bytes_drop(&old);
            jz_str_drop(&coordinate.table);
            jz_bytes_drop(&coordinate.key);
        }
    } else {
        jz_bytes_drop(&expected);
    }
    jz_str_drop(&table);
    switch (comparison) {
    case GROOVE_VALUE_CMP_ABSENT: {
        groove__db__batch__DatabaseBatch__push_operation(self, operation);
        /* The conflict check already prepared this immutable insert. Keep the
         * exact same preparation for overlay reads and commit. */
        groove_OwnedWriteOperation staged;
        e = groove__db__commit__Database__owned_storage_operation_for_pending(database, &pending, &staged);
        if (e.code) { groove__db__PendingTableWrite__drop(&pending); return e; }
        groove__storage__StagedWriteState__stage(groove__db__batch__DatabaseBatch__txn_operations(self), staged);
        JZ_VEC_PUSH(self->prepared.writes, pending);
        self->txn_indexed_operations = self->operations.len;
        *out = GROOVE_ENSURE_EXACT_INSERTED;
        return JZ_OK;
    }
    case GROOVE_VALUE_CMP_IDENTICAL:
        groove__db__PendingTableWrite__drop(&pending);
        groove__db__batch__BatchOperation__drop(&operation);
        *out = GROOVE_ENSURE_EXACT_ALREADY_IDENTICAL;
        return JZ_OK;
    default:
        groove__db__PendingTableWrite__drop(&pending);
        groove__db__batch__BatchOperation__drop(&operation);
        self->exact_conflict = true;
        *out = GROOVE_ENSURE_EXACT_CONFLICT;
        return JZ_OK;
    }
}

jz_err groove__db__batch__DatabaseBatch__check_exact_base(const groove_DatabaseBatch *b, const groove_Database *db) {
    if (b->exact_conflict) return groove__db__Error__unit(GROOVE_DB_ImmutableBatchConflict);
    if (b->has_exact_base && (b->exact_base_owner != (u32)(uintptr_t)db->immutable_batch_owner || b->exact_base_publication != db->next_publication_id))
        return groove__db__Error__unit(GROOVE_DB_StaleImmutableBatch);
    return JZ_OK;
}
void groove__db__batch__DatabaseBatch__deliver_notifications(groove_DatabaseBatch *b, u8 timing) { b->notification_timing = timing; }
void groove__db__batch__DatabaseBatch__reserve(groove_DatabaseBatch *b, u32 additional) { jz_vec_reserve(&b->operations, additional, sizeof(groove_BatchOperation)); }
/* Atomically consume a Groove staging root with this physical-record batch. */
void groove__db__batch__DatabaseBatch__accept_large_value(groove_DatabaseBatch *b, groove_StagedLargeValueId id) {
    for (u32 i = 0; i < b->accepted_large_values.len; i++)
        if (memcmp(b->accepted_large_values.ptr[i].b, id.b, 16) == 0) return;
    JZ_VEC_PUSH(b->accepted_large_values, id);
}
static groove_BatchOperation op_new(u8 tag, jz_str table) {
    groove_BatchOperation op;
    memset(&op, 0, sizeof op);
    op.tag = tag;
    op.table = table;
    return op;
}
void groove__db__batch__DatabaseBatch__insert(groove_DatabaseBatch *b, jz_str table, groove_RecordInput record) {
    groove_BatchOperation op = op_new(GROOVE_BATCH_INSERT, table);
    op.record = record;
    groove__db__batch__DatabaseBatch__push_operation(b, op);
}
void groove__db__batch__DatabaseBatch__insert_raw(groove_DatabaseBatch *b, jz_str table, groove_PrimaryKeyValue key, groove_RawRecordInput record) {
    groove_BatchOperation op = op_new(GROOVE_BATCH_INSERT_RAW, table);
    op.key = key;
    op.raw = record;
    groove__db__batch__DatabaseBatch__push_operation(b, op);
}
/* Stage a raw insert whose caller has already proven that the key is absent
 * (unsafe in Rust: a false proof omits the previous record's negative delta). */
void groove__db__batch__DatabaseBatch__insert_raw_fresh(groove_DatabaseBatch *b, jz_str table, groove_PrimaryKeyValue key, groove_RawRecordInput record) {
    groove_BatchOperation op = op_new(GROOVE_BATCH_INSERT_RAW_FRESH, table);
    op.key = key;
    op.raw = record;
    groove__db__batch__DatabaseBatch__push_operation(b, op);
}
void groove__db__batch__DatabaseBatch__update(groove_DatabaseBatch *b, jz_str table, groove_RecordInput record) {
    groove_BatchOperation op = op_new(GROOVE_BATCH_UPDATE, table);
    op.record = record;
    groove__db__batch__DatabaseBatch__push_operation(b, op);
}
void groove__db__batch__DatabaseBatch__update_raw(groove_DatabaseBatch *b, jz_str table, groove_PrimaryKeyValue key, groove_RawRecordInput record) {
    groove_BatchOperation op = op_new(GROOVE_BATCH_UPDATE_RAW, table);
    op.key = key;
    op.raw = record;
    groove__db__batch__DatabaseBatch__push_operation(b, op);
}
void groove__db__batch__DatabaseBatch__delete(groove_DatabaseBatch *b, jz_str table, groove_PrimaryKeyValue key) {
    groove_BatchOperation op = op_new(GROOVE_BATCH_DELETE, table);
    op.key = key;
    groove__db__batch__DatabaseBatch__push_operation(b, op);
}
bool groove__db__batch__DatabaseBatch__is_empty(const groove_DatabaseBatch *b) { return b->operations.len == 0; }
void groove__db__batch__DatabaseBatch__push_operation(groove_DatabaseBatch *b, groove_BatchOperation op) {
    const jz_str *table = groove__db__batch__BatchOperation__table(&op);
    if (!jz_btree_contains(&b->written_tables, table)) {
        jz_str owned = jz_str_clone(table);
        jz_btree_insert(&b->written_tables, &owned, NULL, NULL);
    }
    JZ_VEC_PUSH(b->operations, op);
}
/* Whether any operation of this batch writes `table`. */
bool groove__db__batch__DatabaseBatch__writes_table(const groove_DatabaseBatch *b, const char *table, u32 n) {
    jz_str key = { (char *)table, n, 0 };
    return jz_btree_contains(&b->written_tables, &key);
}

/* ======================= inputs / operations ======================= */
void groove__db__batch__RecordInput__drop(groove_RecordInput *r) {
    if (r->tag == GROOVE_RECORD_INPUT_VALUES) groove__records__values__ValueVec__drop(&r->values);
    else groove__records__VariantRecord__drop(&r->record);
}
void groove__db__batch__RawRecordInput__drop(groove_RawRecordInput *r) {
    if (r->tag == GROOVE_RAW_INPUT_PAYLOAD) jz_bytes_drop(&r->payload);
    else groove__records__VariantRecord__drop(&r->record);
}
const jz_str *groove__db__batch__BatchOperation__table(const groove_BatchOperation *op) { return &op->table; }
void groove__db__batch__BatchOperation__drop(void *p) {
    groove_BatchOperation *op = p;
    jz_str_drop(&op->table);
    switch (op->tag) {
    case GROOVE_BATCH_INSERT: case GROOVE_BATCH_UPDATE: groove__db__batch__RecordInput__drop(&op->record); break;
    case GROOVE_BATCH_DELETE: groove__db__batch__PrimaryKeyValue__drop(&op->key); break;
    default: groove__db__batch__PrimaryKeyValue__drop(&op->key); groove__db__batch__RawRecordInput__drop(&op->raw); break;
    }
}

/* ======================= PrimaryKeyValue ======================= */
void groove__db__batch__PrimaryKeyValue__Clone__clone(groove_PrimaryKeyValue *dst, const groove_PrimaryKeyValue *src) {
    *dst = *src;
    switch (src->tag) {
    case GROOVE_PKV_STRING: dst->u.s = jz_str_clone(&src->u.s); break;
    case GROOVE_PKV_BYTES: dst->u.bytes = jz_bytes_clone(&src->u.bytes); break;
    case GROOVE_PKV_COMPOSITE:
        memset(&dst->u.composite, 0, sizeof dst->u.composite);
        for (u32 i = 0; i < src->u.composite.len; i++) {
            groove_PrimaryKeyValue v;
            groove__db__batch__PrimaryKeyValue__Clone__clone(&v, &src->u.composite.ptr[i]);
            JZ_VEC_PUSH(dst->u.composite, v);
        }
        break;
    default: break;
    }
}
void groove__db__batch__PrimaryKeyValue__drop(groove_PrimaryKeyValue *v) {
    switch (v->tag) {
    case GROOVE_PKV_STRING: jz_str_drop(&v->u.s); break;
    case GROOVE_PKV_BYTES: jz_bytes_drop(&v->u.bytes); break;
    case GROOVE_PKV_COMPOSITE:
        for (u32 i = 0; i < v->u.composite.len; i++) groove__db__batch__PrimaryKeyValue__drop(&v->u.composite.ptr[i]);
        jz_vec_drop(&v->u.composite, sizeof(groove_PrimaryKeyValue), NULL);
        break;
    default: break;
    }
}
static void encode_expect(jz_bytes *bytes, groove_Value value) {
    jz_err e = groove__db__encoding__encode_primary_key_part(bytes, &value);
    if (e.code) jz_panic("PrimaryKeyValue only contains encodable primary-key values");
    groove__records__values__Value__drop(&value);
}
/* Encode this logical primary key exactly as Groove stores it. */
jz_bytes groove__db__batch__PrimaryKeyValue__into_bytes(groove_PrimaryKeyValue self) {
    jz_bytes bytes = {0};
    groove_Value v;
    memset(&v, 0, sizeof v);
    switch (self.tag) {
    case GROOVE_PKV_U8: v.tag = GROOVE_V_U8; v.u.u8_ = self.u.u8_; encode_expect(&bytes, v); break;
    case GROOVE_PKV_U16: v.tag = GROOVE_V_U16; v.u.u16_ = self.u.u16_; encode_expect(&bytes, v); break;
    case GROOVE_PKV_U32: v.tag = GROOVE_V_U32; v.u.u32_ = self.u.u32_; encode_expect(&bytes, v); break;
    case GROOVE_PKV_U64: v.tag = GROOVE_V_U64; v.u.u64_ = self.u.u64_; encode_expect(&bytes, v); break;
    case GROOVE_PKV_BOOL: v.tag = GROOVE_V_BOOL; v.u.b = self.u.b; encode_expect(&bytes, v); break;
    case GROOVE_PKV_STRING: v.tag = GROOVE_V_STRING; v.u.s = self.u.s; encode_expect(&bytes, v); break;
    case GROOVE_PKV_BYTES: v.tag = GROOVE_V_BYTES; v.u.bytes = self.u.bytes; encode_expect(&bytes, v); break;
    case GROOVE_PKV_UUID: v.tag = GROOVE_V_UUID; v.u.uuid = self.u.uuid; encode_expect(&bytes, v); break;
    case GROOVE_PKV_COMPOSITE:
        for (u32 i = 0; i < self.u.composite.len; i++) {
            jz_bytes part = groove__db__batch__PrimaryKeyValue__into_bytes(self.u.composite.ptr[i]);
            jz_bytes_push(&bytes, part.ptr, part.len);
            jz_bytes_drop(&part);
        }
        jz_vec_drop(&self.u.composite, sizeof(groove_PrimaryKeyValue), NULL);
        break;
    }
    return bytes;
}
