/* db/storage_helpers.c: port of crates/groove/src/db/storage_helpers.rs.
 * Direct record stores, commit/read metrics, the metered storage wrapper,
 * encoded key/value entries, storage-destination classification and
 * table-delta computation for pending writes. */
#include "mod.h"

/* ======================= DirectRecordStore ======================= */
void groove__db__DirectRecordStore__drop(groove_DirectRecordStore *s) { jz_str_drop(&s->name); }
/* Return the schema-declared column family name backing this direct store. */
const jz_str *groove__db__DirectRecordStore__name(const groove_DirectRecordStore *s) { return &s->name; }
const groove_RecordDescriptor *groove__db__DirectRecordStore__key_descriptor(const groove_DirectRecordStore *s) { return &s->key; }
const groove_RecordDescriptor *groove__db__DirectRecordStore__value_descriptor(const groove_DirectRecordStore *s) { return &s->value; }

static groove_DynStorage drs_storage(const groove_DirectRecordStore *s) { return groove__storage__LayoutStorage__as_dyn(s->storage); }

jz_err groove__db__DirectRecordStore__set(const groove_DirectRecordStore *s, const groove_Value *key, u32 kn, const groove_Value *value, u32 vn) {
    jz_bytes k, record;
    JZ_TRY(groove__db__DirectRecordStore__key_bytes(s, key, kn, &k));
    jz_err e = groove__records__RecordDescriptor__create(s->value, value, vn, &record);
    if (e.code) { jz_bytes_drop(&k); return e; }
    groove_WriteOpVec ops = {0};
    groove_OwnedWriteOperation op = { GROOVE_WRITE_OP_SET, jz_str_clone(&s->name), k, record };
    JZ_VEC_PUSH(ops, op);
    return groove__storage__OrderedKvStorage__write_many(drs_storage(s), ops);
}
groove_RecordStore groove__db__DirectRecordStore__record_store(const groove_DirectRecordStore *s) {
    return groove__storage__RecordStore__new(drs_storage(s), (const char *)s->name.ptr, s->name.len, s->value);
}
jz_err groove__db__DirectRecordStore__get(const groove_DirectRecordStore *s, const groove_Value *key, u32 kn, bool *found, groove_OwnedRecord *out) {
    jz_bytes k;
    JZ_TRY(groove__db__DirectRecordStore__key_bytes(s, key, kn, &k));
    groove_RecordStore store = groove__db__DirectRecordStore__record_store(s);
    jz_bytes raw;
    jz_err e = groove__storage__RecordStore__get_raw(&store, k.ptr, k.len, found, &raw);
    jz_bytes_drop(&k);
    if (e.code) return e;
    if (*found) *out = groove__records__OwnedRecord__new(raw, s->value);
    return JZ_OK;
}
jz_err groove__db__DirectRecordStore__delete(const groove_DirectRecordStore *s, const groove_Value *key, u32 kn) {
    jz_bytes k;
    JZ_TRY(groove__db__DirectRecordStore__key_bytes(s, key, kn, &k));
    groove_WriteOpVec ops = {0};
    groove_OwnedWriteOperation op = { GROOVE_WRITE_OP_DELETE, jz_str_clone(&s->name), k, {0} };
    JZ_VEC_PUSH(ops, op);
    return groove__storage__OrderedKvStorage__write_many(drs_storage(s), ops);
}
static void owned_record_drop_v(void *p) { groove__records__OwnedRecord__drop(p); }
static void entry_drop_v(void *p) {
    groove_DirectRecordStoreEntry *e = p;
    groove__records__values__ValueVec__drop(&e->key);
    groove__records__OwnedRecord__drop(&e->value);
}
/* rows -> Vec<Record> (values only) */
static void rows_to_records(const groove_DirectRecordStore *s, groove_KvPairVec rows, groove_OwnedRecordVec *out) {
    memset(out, 0, sizeof *out);
    for (u32 i = 0; i < rows.len; i++) {
        jz_bytes_drop(&rows.ptr[i].key);
        JZ_VEC_PUSH(*out, groove__records__OwnedRecord__new(rows.ptr[i].value, s->value));
    }
    jz_vec_drop(&rows, sizeof(groove_KvPair), NULL);
}
static jz_err rows_to_entries(const groove_DirectRecordStore *s, groove_KvPairVec rows, groove_DirectRecordStoreEntryVec *out) {
    groove_DirectRecordStoreEntryVec entries = {0};
    for (u32 i = 0; i < rows.len; i++) {
        groove_ValueVec key;
        jz_err e = groove__db__DirectRecordStore__decode_key(s, rows.ptr[i].key.ptr, rows.ptr[i].key.len, &key);
        if (e.code) {
            jz_vec_drop(&entries, sizeof(groove_DirectRecordStoreEntry), entry_drop_v);
            for (u32 j = i; j < rows.len; j++) groove__storage__KvPair__drop(&rows.ptr[j]);
            jz_vec_drop(&rows, sizeof(groove_KvPair), NULL);
            return e;
        }
        jz_bytes_drop(&rows.ptr[i].key);
        groove_DirectRecordStoreEntry entry = { key, groove__records__OwnedRecord__new(rows.ptr[i].value, s->value) };
        JZ_VEC_PUSH(entries, entry);
    }
    jz_vec_drop(&rows, sizeof(groove_KvPair), NULL);
    *out = entries;
    return JZ_OK;
}
static jz_err drs_range_rows(const groove_DirectRecordStore *s, const groove_Value *start, u32 sn, const groove_Value *end, u32 en, groove_KvPairVec *rows) {
    jz_bytes a, b;
    JZ_TRY(groove__db__DirectRecordStore__key_prefix_bytes(s, start, sn, &a));
    jz_err e = groove__db__DirectRecordStore__key_prefix_bytes(s, end, en, &b);
    if (e.code) { jz_bytes_drop(&a); return e; }
    groove_RecordStore store = groove__db__DirectRecordStore__record_store(s);
    e = groove__storage__RecordStore__range(&store, a.ptr, a.len, b.ptr, b.len, rows);
    jz_bytes_drop(&a);
    jz_bytes_drop(&b);
    return e;
}
static jz_err drs_prefix_rows(const groove_DirectRecordStore *s, const groove_Value *prefix, u32 n, groove_KvPairVec *rows) {
    jz_bytes p;
    JZ_TRY(groove__db__DirectRecordStore__key_prefix_bytes(s, prefix, n, &p));
    groove_RecordStore store = groove__db__DirectRecordStore__record_store(s);
    jz_err e = groove__storage__RecordStore__prefix(&store, p.ptr, p.len, rows);
    jz_bytes_drop(&p);
    return e;
}
jz_err groove__db__DirectRecordStore__range(const groove_DirectRecordStore *s, const groove_Value *start, u32 sn, const groove_Value *end, u32 en, groove_OwnedRecordVec *out) {
    groove_KvPairVec rows;
    JZ_TRY(drs_range_rows(s, start, sn, end, en, &rows));
    rows_to_records(s, rows, out);
    return JZ_OK;
}
jz_err groove__db__DirectRecordStore__range_entries(const groove_DirectRecordStore *s, const groove_Value *start, u32 sn, const groove_Value *end, u32 en,
                                                    groove_DirectRecordStoreEntryVec *out) {
    groove_KvPairVec rows;
    JZ_TRY(drs_range_rows(s, start, sn, end, en, &rows));
    return rows_to_entries(s, rows, out);
}
jz_err groove__db__DirectRecordStore__prefix(const groove_DirectRecordStore *s, const groove_Value *prefix, u32 n, groove_OwnedRecordVec *out) {
    groove_KvPairVec rows;
    JZ_TRY(drs_prefix_rows(s, prefix, n, &rows));
    rows_to_records(s, rows, out);
    return JZ_OK;
}
jz_err groove__db__DirectRecordStore__prefix_entries(const groove_DirectRecordStore *s, const groove_Value *prefix, u32 n, groove_DirectRecordStoreEntryVec *out) {
    groove_KvPairVec rows;
    JZ_TRY(drs_prefix_rows(s, prefix, n, &rows));
    return rows_to_entries(s, rows, out);
}
jz_err groove__db__DirectRecordStore__write_many(const groove_DirectRecordStore *s, const groove_DirectRecordStoreWrite *ops, u32 n) {
    groove_WriteOpVec encoded;
    JZ_TRY(groove__db__DirectRecordStore__encode_writes(s, ops, n, &encoded));
    return groove__storage__OrderedKvStorage__write_many(drs_storage(s), encoded);
}
jz_err groove__db__DirectRecordStore__encode_writes(const groove_DirectRecordStore *s, const groove_DirectRecordStoreWrite *ops, u32 n, groove_WriteOpVec *out) {
    groove_WriteOpVec encoded = {0};
    jz_vec_reserve(&encoded, n, sizeof(groove_OwnedWriteOperation));
    for (u32 i = 0; i < n; i++) {
        const groove_DirectRecordStoreWrite *op = &ops[i];
        jz_bytes key;
        jz_err e = groove__db__DirectRecordStore__key_bytes(s, op->key.ptr, op->key.len, &key);
        if (e.code) { groove__db__WriteOpVec__drop(&encoded); return e; }
        if (op->tag == GROOVE_DIRECT_WRITE_SET) {
            jz_bytes value;
            e = groove__records__RecordDescriptor__create(s->value, op->value.ptr, op->value.len, &value);
            if (e.code) { jz_bytes_drop(&key); groove__db__WriteOpVec__drop(&encoded); return e; }
            groove_OwnedWriteOperation w = { GROOVE_WRITE_OP_SET, jz_str_clone(&s->name), key, value };
            JZ_VEC_PUSH(encoded, w);
        } else {
            groove_OwnedWriteOperation w = { GROOVE_WRITE_OP_DELETE, jz_str_clone(&s->name), key, {0} };
            JZ_VEC_PUSH(encoded, w);
        }
    }
    *out = encoded;
    return JZ_OK;
}
jz_err groove__db__DirectRecordStore__key_bytes(const groove_DirectRecordStore *s, const groove_Value *values, u32 n, jz_bytes *out) {
    u32 nf;
    groove__records__RecordDescriptor__fields(s->key, &nf);
    if (n != nf) return groove__records__Error__arity_mismatch(nf, n);
    return groove__db__DirectRecordStore__key_prefix_bytes(s, values, n, out);
}
jz_err groove__db__DirectRecordStore__key_prefix_bytes(const groove_DirectRecordStore *s, const groove_Value *values, u32 n, jz_bytes *out) {
    u32 nf;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(s->key, &nf);
    if (n > nf) return groove__records__Error__arity_mismatch(nf, n);
    jz_bytes bytes = {0};
    for (u32 i = 0; i < n; i++) {
        jz_err e = groove__records__ensure_value_type(&values[i], &fields[i].value_type);
        if (!e.code) e = groove__db__encoding__encode_primary_key_part(&bytes, &values[i]);
        if (e.code) { jz_bytes_drop(&bytes); return e; }
    }
    *out = bytes;
    return JZ_OK;
}
jz_err groove__db__DirectRecordStore__decode_key(const groove_DirectRecordStore *s, const u8 *key, u32 n, groove_ValueVec *out) {
    jz_slice remaining = jz_slice_of(key, n);
    u32 nf;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(s->key, &nf);
    groove_ValueVec values = {0};
    jz_vec_reserve(&values, nf, sizeof(groove_Value));
    for (u32 i = 0; i < nf; i++) {
        groove_Value v;
        jz_err e = groove__db__encoding__decode_primary_key_part(&remaining, &fields[i].value_type, &v);
        if (e.code) { groove__records__values__ValueVec__drop(&values); return e; }
        JZ_VEC_PUSH(values, v);
    }
    if (remaining.len != 0) {
        groove__records__values__ValueVec__drop(&values);
        return groove__db__Error__str(GROOVE_DB_InvalidDirectRecordStoreKey, (const char *)s->name.ptr, s->name.len);
    }
    *out = values;
    return JZ_OK;
}

/* ======================= PreparedShape ======================= */
static void query_parameter_drop_v(void *p) {
    groove_QueryParameter *q = p;
    jz_str_drop(&q->name);
    groove__records__values__ValueType__drop(&q->value_type);
}
void groove__db__PreparedShape__drop(groove_PreparedShape *s) { jz_vec_drop(&s->parameters, sizeof(groove_QueryParameter), query_parameter_drop_v); }
groove_PreparedShapeId groove__db__PreparedShape__id(const groove_PreparedShape *s) { return s->id; }
const groove_QueryParameter *groove__db__PreparedShape__parameters(const groove_PreparedShape *s, u32 *len) { *len = s->parameters.len; return s->parameters.ptr; }
const groove_RecordDescriptor *groove__db__PreparedShape__output(const groove_PreparedShape *s) { return &s->output; }

/* ======================= CommitMetrics ======================= */
void groove__db__CommitMetrics__Clone__clone(groove_CommitMetrics *dst, const groove_CommitMetrics *src) {
    *dst = *src;
    dst->tick = src->tick ? groove__ivm__runtime__TickMetrics__clone(src->tick) : NULL;
}
void groove__db__CommitMetrics__drop(groove_CommitMetrics *m) { if (m->tick) groove__ivm__runtime__TickMetrics__drop(m->tick); m->tick = NULL; }

/* ======================= write metrics ======================= */
groove_StorageWriteMetrics groove__db__StorageWriteMetrics__from_operations(const groove_WriteOperation *ops, u32 n, const groove_IndexIdRegistry *index_ids) {
    groove_StorageWriteMetrics metrics;
    memset(&metrics, 0, sizeof metrics);
    for (u32 i = 0; i < n; i++) groove__db__StorageWriteMetrics__record(&metrics, &ops[i], index_ids);
    return metrics;
}
void groove__db__StorageWriteMetrics__record(groove_StorageWriteMetrics *m, const groove_WriteOperation *op, const groove_IndexIdRegistry *index_ids) {
    u32 bytes = groove__db__write_operation_bytes(op);
    groove__db__StorageWriteBucket__record(&m->total, bytes);
    groove_StorageWriteBucket *b;
    switch (groove__db__storage_write_destination(op, index_ids)) {
    case GROOVE_WRITE_DEST_HISTORY_ROWS: b = &m->history_rows; break;
    case GROOVE_WRITE_DEST_HISTORY_INDEXES: b = &m->history_indexes; break;
    case GROOVE_WRITE_DEST_GLOBAL_CURRENT_ROWS: b = &m->global_current_rows; break;
    case GROOVE_WRITE_DEST_GLOBAL_CURRENT_INDEXES: b = &m->global_current_indexes; break;
    case GROOVE_WRITE_DEST_REGISTER_GLOBAL_CURRENT_ROWS: b = &m->register_global_current_rows; break;
    case GROOVE_WRITE_DEST_GLOBAL_CHANGES_ROWS: b = &m->global_changes_rows; break;
    case GROOVE_WRITE_DEST_GLOBAL_CHANGES_INDEXES: b = &m->global_changes_indexes; break;
    case GROOVE_WRITE_DEST_TRANSACTIONS_ROWS: b = &m->transactions_rows; break;
    case GROOVE_WRITE_DEST_TRANSACTIONS_INDEXES: b = &m->transactions_indexes; break;
    default: b = &m->other; break;
    }
    groove__db__StorageWriteBucket__record(b, bytes);
}
void groove__db__StorageWriteBucket__record(groove_StorageWriteBucket *b, u32 bytes) { b->count += 1; b->bytes += bytes; }

/* ======================= read metrics ======================= */
void groove__db__StorageReadMetrics__record_destination(groove_StorageReadMetrics *m, u8 destination, u32 reads, u32 ranges) {
    groove__db__StorageReadBucket__record(&m->total, reads, ranges);
    groove_StorageReadBucket *b;
    switch (destination) {
    case GROOVE_READ_DEST_HISTORY_ROWS: b = &m->history_rows; break;
    case GROOVE_READ_DEST_HISTORY_INDEXES: b = &m->history_indexes; break;
    case GROOVE_READ_DEST_AHEAD_CURRENT_ROWS: b = &m->ahead_current_rows; break;
    case GROOVE_READ_DEST_GLOBAL_CURRENT_ROWS: b = &m->global_current_rows; break;
    case GROOVE_READ_DEST_GLOBAL_CURRENT_INDEXES: b = &m->global_current_indexes; break;
    case GROOVE_READ_DEST_REGISTER_GLOBAL_CURRENT_ROWS: b = &m->register_global_current_rows; break;
    case GROOVE_READ_DEST_GLOBAL_CHANGES_ROWS: b = &m->global_changes_rows; break;
    case GROOVE_READ_DEST_GLOBAL_CHANGES_INDEXES: b = &m->global_changes_indexes; break;
    case GROOVE_READ_DEST_TRANSACTIONS_ROWS: b = &m->transactions_rows; break;
    case GROOVE_READ_DEST_TRANSACTIONS_INDEXES: b = &m->transactions_indexes; break;
    default: b = &m->other; break;
    }
    groove__db__StorageReadBucket__record(b, reads, ranges);
}
void groove__db__StorageReadBucket__record(groove_StorageReadBucket *b, u32 reads, u32 ranges) { b->reads += reads; b->ranges += ranges; }

static void read_metrics_sink_drop(void *p) { groove_ReadMetricsSink *s = p; jz_rc_release(s->index_ids); }
groove_ReadMetricsSink *groove__db__ReadMetricsSink__new(groove_SharedIndexIds index_ids) {
    groove_ReadMetricsSink *s = jz_rc_new(sizeof *s, read_metrics_sink_drop);
    memset(&s->metrics, 0, sizeof s->metrics);
    s->index_ids = index_ids;
    return s;
}
static u8 sink_destination(const groove_ReadMetricsSink *s, const char *cf, u32 cfn, const u8 *key, u32 n) {
    return groove__db__storage_read_destination(cf, cfn, key, n, s->index_ids);
}
void groove__db__ReadMetricsSink__record_point(groove_ReadMetricsSink *s, const char *cf, u32 cfn, const u8 *key, u32 n) {
    groove__db__StorageReadMetrics__record_destination(&s->metrics, sink_destination(s, cf, cfn, key, n), 1, 1);
}
void groove__db__ReadMetricsSink__record_range(groove_ReadMetricsSink *s, const char *cf, u32 cfn, const u8 *key, u32 n) {
    groove__db__StorageReadMetrics__record_destination(&s->metrics, sink_destination(s, cf, cfn, key, n), 0, 1);
}
void groove__db__ReadMetricsSink__record_range_row(groove_ReadMetricsSink *s, const char *cf, u32 cfn, const u8 *key, u32 n) {
    groove__db__StorageReadMetrics__record_destination(&s->metrics, sink_destination(s, cf, cfn, key, n), 1, 0);
}

/* ======================= MeteredStorage ======================= */
groove_MeteredStorage groove__db__MeteredStorage__new(groove_DynStorage storage, groove_ReadMetricsSink *metrics) {
    groove_MeteredStorage m = { storage, NULL, metrics, false };
    return m;
}
static void metered_payload_drop(void *p) { groove__db__MeteredStorage__drop(p); }
groove_MeteredStorage *groove__db__MeteredStorage__new_owned(void *storage_rc, groove_DynStorage storage, groove_ReadMetricsSink *metrics) {
    groove_MeteredStorage *m = jz_rc_new(sizeof *m, metered_payload_drop);
    m->storage = storage;
    m->storage_rc = storage_rc;
    m->metrics = metrics;
    m->metrics_owned = true;
    return m;
}
void groove__db__MeteredStorage__drop(groove_MeteredStorage *m) {
    if (m->storage_rc) jz_rc_release(m->storage_rc);
    if (m->metrics_owned) jz_rc_release(m->metrics);
    m->storage_rc = NULL;
    m->metrics_owned = false;
}

typedef struct MeteredStorageCursor { groove_StorageCursor inner; jz_str column_family; groove_ReadMetricsSink *metrics; } MeteredStorageCursor;
static jz_err metered_cursor_next_batch(void *selfp, bool *some, groove_KvPairVec *out) {
    MeteredStorageCursor *self = selfp;
    JZ_TRY(groove__storage__StorageCursor__next_batch(&self->inner, some, out));
    if (*some)
        for (u32 i = 0; i < out->len; i++)
            groove__db__ReadMetricsSink__record_range_row(self->metrics, (const char *)self->column_family.ptr, self->column_family.len, out->ptr[i].key.ptr, out->ptr[i].key.len);
    return JZ_OK;
}
static void metered_cursor_drop(void *selfp) {
    MeteredStorageCursor *self = selfp;
    groove__storage__StorageCursor__drop(&self->inner);
    jz_str_drop(&self->column_family);
    jz_free(self);
}
static const groove_StorageCursorVt METERED_CURSOR_VT = { metered_cursor_next_batch, metered_cursor_drop };

#define MS(selfp) ((const groove_MeteredStorage *)(selfp))
#define CFP(cf) (const char *)(cf).ptr, (cf).len
static bool ms_permits_eager_read_retry(const void *self) { return groove__storage__OrderedKvStorage__permits_eager_read_retry(MS(self)->storage); }
static jz_err ms_scan(const void *self, groove_ScanRequest request, groove_StorageCursor *out) {
    jz_str cf = jz_str_clone(&request.cf);
    /* Prefix(prefix) | Range { start, .. } */
    groove__db__ReadMetricsSink__record_range(MS(self)->metrics, CFP(cf), request.prefix_or_start.ptr, request.prefix_or_start.len);
    groove_StorageCursor inner;
    jz_err e = groove__storage__OrderedKvStorage__scan(MS(self)->storage, request, &inner);
    if (e.code) { jz_str_drop(&cf); return e; }
    MeteredStorageCursor *c = JZ_NEW(MeteredStorageCursor);
    c->inner = inner;
    c->column_family = cf;
    c->metrics = MS(self)->metrics;
    out->self = c;
    out->vt = &METERED_CURSOR_VT;
    return JZ_OK;
}
static jz_err ms_close(const void *self) { return groove__storage__OrderedKvStorage__close(MS(self)->storage); }
static jz_err ms_set_write_flush_cadence(const void *self, u32 every) { return groove__storage__OrderedKvStorage__set_write_flush_cadence(MS(self)->storage, every); }
static jz_err ms_flush_write_boundary(const void *self) { return groove__storage__OrderedKvStorage__flush_write_boundary(MS(self)->storage); }
static jz_err ms_approximate_class_bytes(const void *self, jz_str cf, bool *some, u64 *out) { return groove__storage__OrderedKvStorage__approximate_class_bytes(MS(self)->storage, cf, some, out); }
static bool ms_column_family_names(const void *self, groove_StrVec *out) { return groove__storage__OrderedKvStorage__column_family_names(MS(self)->storage, out); }
static jz_err ms_compare_value(const void *self, jz_str cf, jz_bytes key, jz_bytes expected, u8 *out) {
    groove__db__ReadMetricsSink__record_point(MS(self)->metrics, CFP(cf), key.ptr, key.len);
    return groove__storage__OrderedKvStorage__compare_value(MS(self)->storage, cf, key, expected, out);
}
static jz_err ms_get(const void *self, jz_str cf, jz_bytes key, bool *found, jz_bytes *out) {
    groove__db__ReadMetricsSink__record_point(MS(self)->metrics, CFP(cf), key.ptr, key.len);
    return groove__storage__OrderedKvStorage__get(MS(self)->storage, cf, key, found, out);
}
static jz_err ms_put_if_absent(const void *self, jz_str cf, jz_bytes key, jz_bytes value, bool *found, jz_bytes *existing) {
    groove__db__ReadMetricsSink__record_point(MS(self)->metrics, CFP(cf), key.ptr, key.len);
    return groove__storage__OrderedKvStorage__put_if_absent(MS(self)->storage, cf, key, value, found, existing);
}
static jz_err ms_compare_and_delete(const void *self, jz_str cf, jz_bytes key, jz_bytes expected, bool *removed) {
    groove__db__ReadMetricsSink__record_point(MS(self)->metrics, CFP(cf), key.ptr, key.len);
    return groove__storage__OrderedKvStorage__compare_and_delete(MS(self)->storage, cf, key, expected, removed);
}
static jz_err ms_set(const void *self, jz_str cf, jz_bytes key, jz_bytes value) { return groove__storage__OrderedKvStorage__set(MS(self)->storage, cf, key, value); }
static jz_err ms_delete(const void *self, jz_str cf, jz_bytes key) { return groove__storage__OrderedKvStorage__delete(MS(self)->storage, cf, key); }
static jz_err ms_last_with_prefix(const void *self, jz_str cf, jz_bytes prefix, bool *some, groove_KvPair *out) {
    groove__db__ReadMetricsSink__record_range(MS(self)->metrics, CFP(cf), prefix.ptr, prefix.len);
    jz_str cf2 = jz_str_clone(&cf);
    jz_err e = groove__storage__OrderedKvStorage__last_with_prefix(MS(self)->storage, cf, prefix, some, out);
    if (!e.code && *some) groove__db__ReadMetricsSink__record_range_row(MS(self)->metrics, CFP(cf2), out->key.ptr, out->key.len);
    jz_str_drop(&cf2);
    return e;
}
static jz_err ms_last_with_prefix_before_or_at(const void *self, jz_str cf, jz_bytes prefix, jz_bytes upper, bool *some, groove_KvPair *out) {
    groove__db__ReadMetricsSink__record_range(MS(self)->metrics, CFP(cf), prefix.ptr, prefix.len);
    jz_str cf2 = jz_str_clone(&cf);
    jz_err e = groove__storage__OrderedKvStorage__last_with_prefix_before_or_at(MS(self)->storage, cf, prefix, upper, some, out);
    if (!e.code && *some) groove__db__ReadMetricsSink__record_range_row(MS(self)->metrics, CFP(cf2), out->key.ptr, out->key.len);
    jz_str_drop(&cf2);
    return e;
}
static jz_err ms_write_many(const void *self, groove_WriteOpVec operations) { return groove__storage__OrderedKvStorage__write_many(MS(self)->storage, operations); }
static const groove_OrderedKvStorageVt METERED_STORAGE_VT = {
    .permits_eager_read_retry = ms_permits_eager_read_retry,
    .get = ms_get,
    .compare_value = ms_compare_value,
    .put_if_absent = ms_put_if_absent,
    .compare_and_delete = ms_compare_and_delete,
    .set = ms_set,
    .delete_ = ms_delete,
    .close = ms_close,
    .set_write_flush_cadence = ms_set_write_flush_cadence,
    .flush_write_boundary = ms_flush_write_boundary,
    .approximate_class_bytes = ms_approximate_class_bytes,
    .scan = ms_scan,
    .last_with_prefix = ms_last_with_prefix,
    .last_with_prefix_before_or_at = ms_last_with_prefix_before_or_at,
    .write_many = ms_write_many,
    .write_many_outcome = NULL,          /* trait default */
    .write_many_borrowed_outcome = NULL, /* trait default */
    .column_family_names = ms_column_family_names,
};
groove_DynStorage groove__db__MeteredStorage__as_dyn(const groove_MeteredStorage *m) {
    groove_DynStorage d = { m, &METERED_STORAGE_VT };
    return d;
}

/* ======================= EncodedKeyValue ======================= */
groove_EncodedKeyValue groove__db__EncodedKeyValue__new(jz_bytes key, jz_bytes value, groove_RecordDescriptor descriptor) {
    groove_EncodedKeyValue e = { key, { 0, groove__records__OwnedRecord__new(value, descriptor) } };
    return e;
}
groove_EncodedKeyValue groove__db__EncodedKeyValue__from_variant(jz_bytes key, groove_VariantRecord record) {
    groove_EncodedKeyValue e = { key, record };
    return e;
}
jz_slice groove__db__EncodedKeyValue__key(const groove_EncodedKeyValue *e) { return jz_bytes_slice(&e->key); }
jz_slice groove__db__EncodedKeyValue__raw(const groove_EncodedKeyValue *e) { return jz_bytes_slice(&e->record.record.raw); }
u32 groove__db__EncodedKeyValue__variant_tag(const groove_EncodedKeyValue *e) { return e->record.variant_tag; }
const groove_VariantRecord *groove__db__EncodedKeyValue__variant_record(const groove_EncodedKeyValue *e) { return &e->record; }
void groove__db__EncodedKeyValue__into_parts(groove_EncodedKeyValue e, jz_bytes *key, jz_bytes *value) { *key = e.key; *value = e.record.record.raw; }
void groove__db__EncodedKeyValue__into_variant_parts(groove_EncodedKeyValue e, jz_bytes *key, groove_VariantRecord *record) { *key = e.key; *record = e.record; }
groove_BorrowedRecord groove__db__EncodedKeyValue__record(const groove_EncodedKeyValue *e) {
    groove_BorrowedRecord r = { jz_bytes_slice(&e->record.record.raw), e->record.record.descriptor };
    return r;
}
groove_OwnedRecord groove__db__EncodedKeyValue__owned_record(groove_EncodedKeyValue e) { jz_bytes_drop(&e.key); return e.record.record; }
void groove__db__EncodedKeyValue__drop(void *p) {
    groove_EncodedKeyValue *e = p;
    jz_bytes_drop(&e->key);
    groove__records__VariantRecord__drop(&e->record);
}
void groove__db__EncodedKeyValueVec__drop(groove_EncodedKeyValueVec *v) { jz_vec_drop(v, sizeof(groove_EncodedKeyValue), groove__db__EncodedKeyValue__drop); }

/* ======================= storage destinations ======================= */
u32 groove__db__write_operation_bytes(const groove_WriteOperation *op) {
    return op->tag == GROOVE_WRITE_OP_SET ? op->key.len + op->value.len : op->key.len;
}
static bool s_eq(const char *s, u32 n, const char *lit) { u32 m = (u32)strlen(lit); return n == m && memcmp(s, lit, m) == 0; }
static bool s_starts(const char *s, u32 n, const char *lit) { u32 m = (u32)strlen(lit); return n >= m && memcmp(s, lit, m) == 0; }
static bool s_ends(const char *s, u32 n, const char *lit) { u32 m = (u32)strlen(lit); return n >= m && memcmp(s + n - m, lit, m) == 0; }
static bool s_contains(const char *s, u32 n, const char *lit) {
    u32 m = (u32)strlen(lit);
    if (m > n) return false;
    for (u32 i = 0; i + m <= n; i++) if (memcmp(s + i, lit, m) == 0) return true;
    return false;
}
u8 groove__db__storage_write_destination(const groove_WriteOperation *op, const groove_IndexIdRegistry *index_ids) {
    if (s_eq((const char *)op->cf.ptr, op->cf.len, "indices")) return groove__db__storage_index_write_destination(op->key.ptr, op->key.len, index_ids);
    return groove__db__storage_table_write_destination((const char *)op->cf.ptr, op->cf.len);
}
u8 groove__db__storage_table_write_destination(const char *t, u32 n) {
    if (s_eq(t, n, "jazz_global_changes")) return GROOVE_WRITE_DEST_GLOBAL_CHANGES_ROWS;
    if (s_eq(t, n, "jazz_transactions")) return GROOVE_WRITE_DEST_TRANSACTIONS_ROWS;
    if (s_starts(t, n, "jazz_") && s_ends(t, n, "_register_global_current") && !s_contains(t, n, "_ahead_current"))
        return GROOVE_WRITE_DEST_REGISTER_GLOBAL_CURRENT_ROWS;
    if (s_starts(t, n, "jazz_") && s_ends(t, n, "_global_current") && !s_contains(t, n, "_register_global_current") && !s_contains(t, n, "_ahead_current"))
        return GROOVE_WRITE_DEST_GLOBAL_CURRENT_ROWS;
    if (s_starts(t, n, "jazz_") && s_ends(t, n, "_history")) return GROOVE_WRITE_DEST_HISTORY_ROWS;
    return GROOVE_WRITE_DEST_OTHER;
}
u8 groove__db__storage_index_write_destination(const u8 *key, u32 n, const groove_IndexIdRegistry *index_ids) {
    jz_slice table, index;
    if (!groove__db__durable_index_table_and_name(key, n, index_ids, &table, &index)) return GROOVE_WRITE_DEST_OTHER;
    const char *t = (const char *)table.ptr, *i = (const char *)index.ptr;
    u32 tn = table.len, in = index.len;
    if (s_eq(t, tn, "jazz_global_changes") && (s_eq(i, in, "by_global_time") || s_eq(i, in, "by_table_global_time")))
        return GROOVE_WRITE_DEST_GLOBAL_CHANGES_INDEXES;
    if (s_eq(t, tn, "jazz_transactions") && s_eq(i, in, "by_global_time")) return GROOVE_WRITE_DEST_TRANSACTIONS_INDEXES;
    if (s_starts(t, tn, "jazz_") && s_ends(t, tn, "_global_current") && !s_contains(t, tn, "_register_global_current") &&
        (s_starts(i, in, "by_app_") || s_starts(i, in, "by_physical_app_") || s_starts(i, in, "by_physical_composite_")))
        return GROOVE_WRITE_DEST_GLOBAL_CURRENT_INDEXES;
    if (s_starts(t, tn, "jazz_") && s_ends(t, tn, "_history") && s_eq(i, in, "by_tx")) return GROOVE_WRITE_DEST_HISTORY_INDEXES;
    return GROOVE_WRITE_DEST_OTHER;
}
u8 groove__db__storage_read_destination(const char *cf, u32 cfn, const u8 *key, u32 n, const groove_IndexIdRegistry *index_ids) {
    if (s_eq(cf, cfn, "indices")) return groove__db__storage_index_read_destination(key, n, index_ids);
    return groove__db__storage_table_read_destination(cf, cfn);
}
u8 groove__db__storage_table_read_destination(const char *t, u32 n) {
    if (s_starts(t, n, "jazz_") && (s_ends(t, n, "_ahead_current") || s_ends(t, n, "_register_ahead_current"))) return GROOVE_READ_DEST_AHEAD_CURRENT_ROWS;
    switch (groove__db__storage_table_write_destination(t, n)) {
    case GROOVE_WRITE_DEST_HISTORY_ROWS: return GROOVE_READ_DEST_HISTORY_ROWS;
    case GROOVE_WRITE_DEST_GLOBAL_CURRENT_ROWS: return GROOVE_READ_DEST_GLOBAL_CURRENT_ROWS;
    case GROOVE_WRITE_DEST_REGISTER_GLOBAL_CURRENT_ROWS: return GROOVE_READ_DEST_REGISTER_GLOBAL_CURRENT_ROWS;
    case GROOVE_WRITE_DEST_GLOBAL_CHANGES_ROWS: return GROOVE_READ_DEST_GLOBAL_CHANGES_ROWS;
    case GROOVE_WRITE_DEST_TRANSACTIONS_ROWS: return GROOVE_READ_DEST_TRANSACTIONS_ROWS;
    default: return GROOVE_READ_DEST_OTHER;
    }
}
u8 groove__db__storage_index_read_destination(const u8 *key, u32 n, const groove_IndexIdRegistry *index_ids) {
    switch (groove__db__storage_index_write_destination(key, n, index_ids)) {
    case GROOVE_WRITE_DEST_HISTORY_INDEXES: return GROOVE_READ_DEST_HISTORY_INDEXES;
    case GROOVE_WRITE_DEST_GLOBAL_CURRENT_INDEXES: return GROOVE_READ_DEST_GLOBAL_CURRENT_INDEXES;
    case GROOVE_WRITE_DEST_GLOBAL_CHANGES_INDEXES: return GROOVE_READ_DEST_GLOBAL_CHANGES_INDEXES;
    case GROOVE_WRITE_DEST_TRANSACTIONS_INDEXES: return GROOVE_READ_DEST_TRANSACTIONS_INDEXES;
    default: return GROOVE_READ_DEST_OTHER;
    }
}
/* The table and index a durable index key (or scan prefix) belongs to, via its
 * numeric id. */
bool groove__db__durable_index_table_and_name(const u8 *key, u32 n, const groove_IndexIdRegistry *index_ids, jz_slice *table, jz_slice *index) {
    u32 id;
    jz_slice logical;
    if (!groove__ivm__runtime__split_durable_index_key(key, n, &id, &logical)) return false;
    return groove__ivm__runtime__IndexIdRegistry__names(index_ids, id, table, index);
}

/* ======================= PendingTableWrite ======================= */
void groove__db__PendingTableWrite__drop(void *p) {
    groove_PendingTableWrite *w = p;
    jz_str_drop(&w->table);
    jz_bytes_drop(&w->key);
    if (w->tag == GROOVE_PENDING_SET) jz_bytes_drop(&w->record);
}
bool groove__db__PendingTableWrite__stored_record(const groove_PendingTableWrite *w, jz_bytes *out) {
    if (w->tag != GROOVE_PENDING_SET) return false;
    groove__records__encode_variant_record(w->variant_tag, w->record.ptr, w->record.len, out);
    return true;
}

/* ---- compute_table_deltas ---- */
typedef struct OverlayKey { jz_slice table; jz_slice key; } OverlayKey;
typedef struct OverlayVal { bool some; u32 variant_tag; jz_slice payload; } OverlayVal;
static u64 overlay_hash(const void *k) {
    const OverlayKey *o = k;
    return jz_hash_combine(jz_hash_bytes(o->table.ptr, o->table.len), jz_hash_bytes(o->key.ptr, o->key.len));
}
static bool overlay_eq(const void *a, const void *b) {
    const OverlayKey *x = a, *y = b;
    return jz_slice_cmp(x->table, y->table) == 0 && jz_slice_cmp(x->key, y->key) == 0;
}
static const jz_hmap_desc OVERLAY_DESC = { sizeof(OverlayKey), sizeof(OverlayVal), overlay_hash, overlay_eq, NULL, NULL };

typedef struct GroupKey { jz_slice table; u32 variant_tag; groove_RecordDescriptor descriptor; } GroupKey;
static u64 group_hash(const void *k) {
    const GroupKey *g = k;
    return jz_hash_combine(jz_hash_combine(jz_hash_bytes(g->table.ptr, g->table.len), g->variant_tag), (u64)(uintptr_t)g->descriptor);
}
static bool group_eq(const void *a, const void *b) {
    const GroupKey *x = a, *y = b;
    return x->variant_tag == y->variant_tag && x->descriptor == y->descriptor && jz_slice_cmp(x->table, y->table) == 0;
}
static u64 bytes_hash(const void *k) { const jz_bytes *b = k; return jz_hash_bytes(b->ptr, b->len); }
static bool bytes_eq(const void *a, const void *b) { return jz_bytes_eq(a, b); }
static void bytes_drop_v(void *p) { jz_bytes_drop(p); }
static const jz_hmap_desc RECORDS_DESC = { sizeof(jz_bytes), sizeof(i64), bytes_hash, bytes_eq, bytes_drop_v, NULL };
static void hmap_drop_v(void *p) { jz_hmap_drop(p); }
static const jz_hmap_desc BY_TABLE_DESC = { sizeof(GroupKey), sizeof(jz_hmap), group_hash, group_eq, NULL, hmap_drop_v };
typedef struct DescKey { jz_slice table; u32 variant_tag; } DescKey;
static u64 desc_hash(const void *k) { const DescKey *d = k; return jz_hash_combine(jz_hash_bytes(d->table.ptr, d->table.len), d->variant_tag); }
static bool desc_eq(const void *a, const void *b) { const DescKey *x = a, *y = b; return x->variant_tag == y->variant_tag && jz_slice_cmp(x->table, y->table) == 0; }
static const jz_hmap_desc OLD_DESCRIPTORS_DESC = { sizeof(DescKey), sizeof(groove_RecordDescriptor), desc_hash, desc_eq, NULL, NULL };

static void by_table_add(jz_hmap *by_table, jz_slice table, u32 variant_tag, groove_RecordDescriptor descriptor, const u8 *payload, u32 n, i64 delta) {
    GroupKey gk = { table, variant_tag, descriptor };
    bool inserted;
    jz_hmap *records = jz_hmap_entry(by_table, &gk, &inserted);
    if (inserted) jz_hmap_init(records, &RECORDS_DESC);
    jz_bytes key = jz_bytes_from(payload, n);
    i64 *weight = jz_hmap_entry(records, &key, &inserted);
    if (inserted) *weight = 0;
    else jz_bytes_drop(&key);
    *weight += delta;
}

jz_err groove__db__compute_table_deltas(const groove_PendingTableWrite *writes, const groove_RecordStore *stores, u32 n, const groove_DatabaseSchema *schema, groove_TableDeltaVec *out) {
    /* Reads see earlier writes in the same batch through this overlay. */
    jz_hmap overlay, by_table, old_descriptors;
    jz_hmap_init(&overlay, &OVERLAY_DESC);
    jz_hmap_init(&by_table, &BY_TABLE_DESC);
    jz_hmap_init(&old_descriptors, &OLD_DESCRIPTORS_DESC);
    jz_err e = JZ_OK;
    for (u32 i = 0; i < n && !e.code; i++) {
        const groove_PendingTableWrite *write = &writes[i];
        OverlayKey overlay_key = { jz_str_slice(&write->table), jz_bytes_slice(&write->key) };
        bool has_current = false;
        u32 current_tag = 0;
        jz_slice current_payload = {0};
        jz_bytes stored_current = {0};
        bool have_stored = false;
        OverlayVal *seen = jz_hmap_get(&overlay, &overlay_key);
        if (seen) {
            has_current = seen->some;
            current_tag = seen->variant_tag;
            current_payload = seen->payload;
        } else if (write->tag == GROOVE_PENDING_SET && write->mode == GROOVE_WRITE_MODE_INSERT_FRESH) {
            has_current = false;
        } else {
            if ((e = groove__storage__RecordStore__get_raw(&stores[i], write->key.ptr, write->key.len, &have_stored, &stored_current)).code) break;
            if (have_stored) {
                if ((e = groove__records__split_variant_record(stored_current.ptr, stored_current.len, &current_tag, &current_payload)).code) {
                    jz_bytes_drop(&stored_current);
                    break;
                }
                has_current = true;
            }
        }
        if (write->tag == GROOVE_PENDING_SET && write->mode == GROOVE_WRITE_MODE_INSERT && has_current) {
            e = groove__db__Error__duplicate_primary_key((const char *)write->table.ptr, write->table.len, write->key.ptr, write->key.len);
        } else {
            const groove_TableSchema *table_schema = groove__schema__DatabaseSchema__table(schema, (const char *)write->table.ptr, write->table.len);
            if (!table_schema) {
                e = groove__db__Error__str(GROOVE_DB_TableNotFound, (const char *)write->table.ptr, write->table.len);
            } else {
                if (has_current) {
                    DescKey dk = { overlay_key.table, current_tag };
                    groove_RecordDescriptor descriptor;
                    groove_RecordDescriptor *cached = jz_hmap_get(&old_descriptors, &dk);
                    if (cached) {
                        descriptor = *cached;
                    } else if (groove__schema__TableSchema__record_schema_for_variant(table_schema, current_tag, &descriptor)) {
                        jz_hmap_insert(&old_descriptors, &dk, &descriptor, NULL);
                    } else {
                        e = groove__db__Error__table_version(GROOVE_DB_UnknownTableVariant, (const char *)table_schema->name.ptr, table_schema->name.len, current_tag);
                    }
                    if (!e.code) by_table_add(&by_table, overlay_key.table, current_tag, descriptor, current_payload.ptr, current_payload.len, -1);
                }
                if (!e.code && write->tag == GROOVE_PENDING_SET)
                    by_table_add(&by_table, overlay_key.table, write->variant_tag, write->descriptor, write->record.ptr, write->record.len, +1);
                if (!e.code) {
                    OverlayVal next = { write->tag == GROOVE_PENDING_SET, write->variant_tag, write->tag == GROOVE_PENDING_SET ? jz_bytes_slice(&write->record) : jz_slice_of(NULL, 0) };
                    jz_hmap_insert(&overlay, &overlay_key, &next, NULL);
                }
            }
        }
        if (have_stored) jz_bytes_drop(&stored_current);
    }
    if (!e.code) {
        groove_TableDeltaVec result = {0};
        GroupKey *gk;
        jz_hmap *records;
        JZ_HMAP_FOR(&by_table, gk, records) {
            groove_TableDelta delta;
            memset(&delta, 0, sizeof delta);
            jz_bytes *record;
            i64 *weight;
            JZ_HMAP_FOR(records, record, weight) {
                if (*weight != 0) {
                    groove_RecordDelta d = { jz_bytes_clone(record), *weight };
                    JZ_VEC_PUSH(delta.deltas, d);
                }
            }
            if (delta.deltas.len) {
                delta.table = jz_str_from_slice(gk->table);
                delta.variant_tag = gk->variant_tag;
                delta.descriptor = gk->descriptor;
                JZ_VEC_PUSH(result, delta);
            }
        }
        *out = result;
    }
    jz_hmap_drop(&overlay);
    jz_hmap_drop(&by_table);
    jz_hmap_drop(&old_descriptors);
    return e;
}
