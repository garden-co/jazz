/* db/primary_storage.c: port of crates/groove/src/db/primary_storage.rs.
 * Primary-key and declared-index reads over the applied resident state. */
#include "mod.h"

/* How many key columns a probe must supply. */
enum { KEY_ARITY_EXACT, KEY_ARITY_PREFIX };
static bool KeyArity__admits(u8 self, u32 expected, u32 actual) { return self == KEY_ARITY_EXACT ? actual == expected : actual <= expected; }
static jz_err KeyArity__check_index(u8 self, const char *index, u32 in, u32 expected, u32 actual) {
    if (KeyArity__admits(self, expected, actual)) return JZ_OK;
    return groove__db__Error__arity(GROOVE_DB_IndexKeyArity, index, in, expected, actual);
}
/* Check `values` against the table's primary key and encode them. */
static jz_err KeyArity__encode_primary_key(u8 self, const char *table, u32 tn, const groove_TableSchema *table_schema, const groove_PrimaryKey *primary_key,
                                           const groove_Value *values, u32 n, jz_bytes *out) {
    if (!KeyArity__admits(self, primary_key->columns.len, n)) return groove__db__Error__arity(GROOVE_DB_PrimaryKeyArity, table, tn, primary_key->columns.len, n);
    jz_bytes key = {0};
    u32 m = n < primary_key->columns.len ? n : primary_key->columns.len;
    for (u32 i = 0; i < m; i++) {
        jz_err e = groove__db__encoding__ensure_primary_key_value_type(table_schema, &primary_key->columns.ptr[i], &values[i]);
        if (!e.code) e = groove__db__encoding__encode_primary_key_part(&key, &values[i]);
        if (e.code) { jz_bytes_drop(&key); return e; }
    }
    *out = key;
    return JZ_OK;
}

static jz_err primary_key_schema(const groove_Database *db, const char *table, u32 tn, const groove_TableSchema **ts, const groove_PrimaryKey **pk) {
    JZ_TRY(groove__db__commit__Database__table(db, table, tn, ts));
    if (!(*ts)->has_primary_key) return groove__db__Error__str(GROOVE_DB_MissingPrimaryKey, table, tn);
    *pk = &(*ts)->primary_key;
    return JZ_OK;
}

/* resident_storage() + MeteredStorage::new(&resident, &metrics), optionally
 * under a StagedWriteOverlay of a batch's txn operations. */
typedef struct Reader { groove_StagedWriteOverlay *resident; groove_StagedWriteOverlay *overlay; groove_MeteredStorage metered; } Reader;
static void reader_open(Reader *r, const groove_Database *db, groove_DatabaseBatch *batch) {
    r->resident = groove__db__facade__Database__resident_storage(db);
    r->overlay = NULL;
    groove_DynStorage base = groove__storage__StagedWriteOverlay__as_dyn(r->resident);
    if (batch) {
        r->overlay = groove__storage__StagedWriteOverlay__new(base, groove__db__batch__DatabaseBatch__txn_operations(batch));
        base = groove__storage__StagedWriteOverlay__as_dyn(r->overlay);
    }
    r->metered = groove__db__MeteredStorage__new(base, db->storage_read_metrics);
}
static groove_DynStorage reader_dyn(const Reader *r) { return groove__db__MeteredStorage__as_dyn(&r->metered); }
static void reader_close(Reader *r) {
    groove__db__MeteredStorage__drop(&r->metered);
    if (r->overlay) groove__storage__StagedWriteOverlay__release(r->overlay);
    groove__storage__StagedWriteOverlay__release(r->resident);
}

static void variant_record_drop_v(void *p) { groove__records__VariantRecord__drop(p); }
static void into_variant_records(groove_EncodedKeyValueVec raw, groove_VariantRecordVec *out) {
    memset(out, 0, sizeof *out);
    for (u32 i = 0; i < raw.len; i++) {
        jz_bytes key;
        groove_VariantRecord record;
        groove__db__EncodedKeyValue__into_variant_parts(raw.ptr[i], &key, &record);
        jz_bytes_drop(&key);
        JZ_VEC_PUSH(*out, record);
    }
    jz_vec_drop(&raw, sizeof(groove_EncodedKeyValue), NULL);
}
static jz_err decode_rows(const groove_Database *db, const groove_TableSchema *table_schema, groove_KvPairVec rows, groove_EncodedKeyValueVec *out) {
    groove_EncodedKeyValueVec entries = {0};
    jz_err e = JZ_OK;
    u32 i = 0;
    for (; i < rows.len; i++) {
        groove_EncodedKeyValue entry;
        e = groove__db__encoding__Database__decode_stored_key_value(db, table_schema, rows.ptr[i].key, rows.ptr[i].value, &entry);
        if (e.code) { i++; break; }
        JZ_VEC_PUSH(entries, entry);
    }
    for (; i < rows.len; i++) groove__storage__KvPair__drop(&rows.ptr[i]);
    jz_vec_drop(&rows, sizeof(groove_KvPair), NULL);
    if (e.code) { groove__db__EncodedKeyValueVec__drop(&entries); return e; }
    *out = entries;
    return JZ_OK;
}

/* Whether this physical table contains any stored record in the current
 * applied resident state. At most one logical storage entry is requested. */
jz_err groove__db__primary_storage__Database__table_has_stored_rows(const groove_Database *db, const char *table, u32 tn, bool *out) {
    const groove_TableSchema *ts;
    JZ_TRY(groove__db__commit__Database__table(db, table, tn, &ts));
    Reader r;
    reader_open(&r, db, NULL);
    groove_ScanRequest request = groove__storage__ScanRequest__prefix(jz_str_from_slice(jz_slice_of(table, tn)), (jz_bytes){0});
    groove__storage__ScanRequest__with_max_items(&request, 1);
    groove_StorageCursor cursor;
    jz_err e = groove__storage__OrderedKvStorage__scan(reader_dyn(&r), request, &cursor);
    *out = false;
    if (!e.code) {
        /* Backends may return an empty batch without having reached EOF. */
        for (;;) {
            bool some;
            groove_KvPairVec rows;
            if ((e = groove__storage__StorageCursor__next_batch(&cursor, &some, &rows)).code || !some) break;
            bool non_empty = rows.len != 0;
            jz_vec_drop(&rows, sizeof(groove_KvPair), groove__storage__KvPair__drop);
            if (non_empty) { *out = true; break; }
        }
        groove__storage__StorageCursor__drop(&cursor);
    }
    reader_close(&r);
    return e;
}

/* How many entries of `table`'s index `index_name` start with `prefix`,
 * counting no further than `max_items`. */
jz_err groove__db__primary_storage__Database__index_prefix_entries_up_to(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                                         const groove_Value *prefix, u32 n, u32 max_items, u32 *out) {
    if (max_items == 0) { *out = 0; return JZ_OK; }
    const groove_IndexSchema *index;
    JZ_TRY(groove__db__primary_storage__Database__index(db, table, tn, index_name, inn, &index));
    if (n > index->columns.len) return groove__db__Error__arity(GROOVE_DB_IndexKeyArity, index_name, inn, index->columns.len, n);
    /* The same bytes as an IVM index prefix scan of these values. */
    jz_bytes storage_prefix;
    groove__ivm__runtime__durable_index_key_prefix(groove__ivm__runtime__IvmRuntime__index_storage_id(db->ivm_runtime, table, tn, index), &storage_prefix);
    for (u32 i = 0; i < n; i++) {
        jz_err e = groove__ivm__runtime__encode_key_part(&storage_prefix, &prefix[i]);
        if (e.code) { jz_bytes_drop(&storage_prefix); return e; }
    }
    Reader r;
    reader_open(&r, db, NULL);
    groove_ScanRequest request = groove__storage__ScanRequest__prefix(jz_str_from("indices"), storage_prefix);
    groove__storage__ScanRequest__with_max_items(&request, max_items);
    groove_StorageCursor cursor;
    jz_err e = groove__storage__OrderedKvStorage__scan(reader_dyn(&r), request, &cursor);
    u32 entries = 0;
    if (!e.code) {
        while (entries < max_items) {
            bool some;
            groove_KvPairVec rows;
            if ((e = groove__storage__StorageCursor__next_batch(&cursor, &some, &rows)).code || !some) break;
            entries += rows.len;
            jz_vec_drop(&rows, sizeof(groove_KvPair), groove__storage__KvPair__drop);
        }
        groove__storage__StorageCursor__drop(&cursor);
    }
    reader_close(&r);
    JZ_TRY(e);
    *out = entries < max_items ? entries : max_items;
    return JZ_OK;
}

/* Return decoded records whose explicit schema index exactly matches the key. */
jz_err groove__db__primary_storage__Database__index_get(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                        const groove_Value *key, u32 n, groove_VariantRecordVec *out) {
    const groove_IndexSchema *index;
    JZ_TRY(groove__db__primary_storage__Database__index(db, table, tn, index_name, inn, &index));
    JZ_TRY(KeyArity__check_index(KEY_ARITY_EXACT, index_name, inn, index->columns.len, n));
    return groove__db__primary_storage__Database__index_scan(db, table, tn, index_name, inn, key, n, out);
}
/* Return decoded records whose explicit schema index starts with the prefix. */
jz_err groove__db__primary_storage__Database__index_scan(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                         const groove_Value *prefix, u32 n, groove_VariantRecordVec *out) {
    const groove_IndexSchema *index;
    JZ_TRY(groove__db__primary_storage__Database__index(db, table, tn, index_name, inn, &index));
    JZ_TRY(KeyArity__check_index(KEY_ARITY_PREFIX, index_name, inn, index->columns.len, n));
    groove_EncodedKeyValueVec raw_entries;
    JZ_TRY(groove__db__primary_storage__Database__index_scan_raw(db, table, tn, index_name, inn, prefix, n, &raw_entries));
    return groove__db__primary_storage__Database__decode_index_records(db, table, tn, index_name, inn, raw_entries, out);
}
/* Return decoded records whose explicit schema index is in the supplied
 * logical index-key range. */
jz_err groove__db__primary_storage__Database__index_scan_range(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                               const groove_Value *start, u32 sn, const groove_Value *end, u32 en, groove_VariantRecordVec *out) {
    const groove_IndexSchema *index;
    JZ_TRY(groove__db__primary_storage__Database__index(db, table, tn, index_name, inn, &index));
    JZ_TRY(KeyArity__check_index(KEY_ARITY_PREFIX, index_name, inn, index->columns.len, sn));
    JZ_TRY(KeyArity__check_index(KEY_ARITY_PREFIX, index_name, inn, index->columns.len, en));
    groove_EncodedKeyValueVec raw_entries;
    JZ_TRY(groove__db__primary_storage__Database__index_scan_range_raw(db, table, tn, index_name, inn, start, sn, end, en, &raw_entries));
    return groove__db__primary_storage__Database__decode_index_records(db, table, tn, index_name, inn, raw_entries, out);
}
jz_err groove__db__primary_storage__Database__decode_index_records(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                                   groove_EncodedKeyValueVec raw_entries, groove_VariantRecordVec *out) {
    const groove_TableSchema *ts;
    jz_err e = groove__db__commit__Database__table(db, table, tn, &ts);
    if (e.code) { groove__db__EncodedKeyValueVec__drop(&raw_entries); return e; }
    (void)index_name;
    into_variant_records(raw_entries, out);
    return JZ_OK;
}

/* Return decoded records whose primary key starts with the supplied prefix. */
jz_err groove__db__primary_storage__Database__primary_key_scan(const groove_Database *db, const char *table, u32 tn, const groove_Value *prefix, u32 n, groove_VariantRecordVec *out) {
    Reader r;
    reader_open(&r, db, NULL);
    jz_err e = groove__db__primary_storage__Database__primary_key_scan_with_storage(db, reader_dyn(&r), table, tn, prefix, n, out);
    reader_close(&r);
    return e;
}
jz_err groove__db__primary_storage__Database__primary_key_scan_with_storage(const groove_Database *db, groove_DynStorage storage, const char *table, u32 tn,
                                                                            const groove_Value *prefix, u32 n, groove_VariantRecordVec *out) {
    groove_EncodedKeyValueVec raw;
    JZ_TRY(groove__db__primary_storage__Database__primary_key_scan_raw_with_storage(db, storage, table, tn, prefix, n, &raw));
    into_variant_records(raw, out);
    return JZ_OK;
}
/* Return encoded records whose primary key starts with the supplied prefix. */
jz_err groove__db__primary_storage__Database__primary_key_scan_raw(const groove_Database *db, const char *table, u32 tn, const groove_Value *prefix, u32 n, groove_EncodedKeyValueVec *out) {
    Reader r;
    reader_open(&r, db, NULL);
    jz_err e = groove__db__primary_storage__Database__primary_key_scan_raw_with_storage(db, reader_dyn(&r), table, tn, prefix, n, out);
    reader_close(&r);
    return e;
}
/* Return encoded primary-key records while also observing writes already
 * staged in `batch`. */
jz_err groove__db__primary_storage__Database__primary_key_scan_raw_in_batch(const groove_Database *db, groove_DatabaseBatch *batch, const char *table, u32 tn,
                                                                            const groove_Value *prefix, u32 n, groove_EncodedKeyValueVec *out) {
    JZ_TRY(groove__db__commit__Database__ensure_batch_storage_txn(db, batch));
    Reader r;
    reader_open(&r, db, batch);
    jz_err e = groove__db__primary_storage__Database__primary_key_scan_raw_with_storage(db, reader_dyn(&r), table, tn, prefix, n, out);
    reader_close(&r);
    return e;
}
/* Return one encoded record by its full primary key. */
jz_err groove__db__primary_storage__Database__primary_key_get_raw(const groove_Database *db, const char *table, u32 tn, const groove_Value *key, u32 n, bool *found,
                                                                  groove_EncodedKeyValue *out) {
    Reader r;
    reader_open(&r, db, NULL);
    jz_err e = groove__db__primary_storage__Database__primary_key_get_raw_with_storage(db, reader_dyn(&r), table, tn, key, n, found, out);
    reader_close(&r);
    return e;
}
/* Return one schema-bound record by its complete primary key. */
jz_err groove__db__primary_storage__Database__primary_key_get(const groove_Database *db, const char *table, u32 tn, const groove_Value *key, u32 n, bool *found,
                                                              groove_VariantRecord *out) {
    groove_EncodedKeyValue entry;
    JZ_TRY(groove__db__primary_storage__Database__primary_key_get_raw(db, table, tn, key, n, found, &entry));
    if (*found) {
        jz_bytes k;
        groove__db__EncodedKeyValue__into_variant_parts(entry, &k, out);
        jz_bytes_drop(&k);
    }
    return JZ_OK;
}
static jz_err get_and_decode(const groove_Database *db, groove_DynStorage storage, const char *table, u32 tn, const groove_TableSchema *table_schema,
                             groove_RecordDescriptor descriptor, jz_bytes encoded_key, bool *found, groove_EncodedKeyValue *out) {
    groove_RecordStore store = groove__storage__RecordStore__new(storage, table, tn, descriptor);
    jz_bytes value;
    jz_err e = groove__storage__RecordStore__get_raw(&store, encoded_key.ptr, encoded_key.len, found, &value);
    if (e.code || !*found) { jz_bytes_drop(&encoded_key); return e; }
    return groove__db__encoding__Database__decode_stored_key_value(db, table_schema, encoded_key, value, out);
}
/* Return one encoded primary-key record while also observing writes already
 * staged in `batch`. */
jz_err groove__db__primary_storage__Database__primary_key_get_raw_in_batch(const groove_Database *db, groove_DatabaseBatch *batch, const char *table, u32 tn,
                                                                           const groove_Value *key, u32 n, bool *found, groove_EncodedKeyValue *out) {
    /* Staging the batch only serves reads that can observe it: a table this
     * batch never writes reads exactly as resident storage. */
    bool batch_writes_table = groove__db__batch__DatabaseBatch__writes_table(batch, table, tn);
    if (batch_writes_table) JZ_TRY(groove__db__commit__Database__ensure_batch_storage_txn(db, batch));
    const groove_TableSchema *table_schema;
    const groove_PrimaryKey *primary_key;
    JZ_TRY(primary_key_schema(db, table, tn, &table_schema, &primary_key));
    groove_RecordDescriptor descriptor;
    JZ_TRY(groove__db__commit__Database__table_storage_descriptor(db, table, tn, &descriptor));
    jz_bytes encoded_key;
    JZ_TRY(KeyArity__encode_primary_key(KEY_ARITY_EXACT, table, tn, table_schema, primary_key, key, n, &encoded_key));
    bool staged_contains_key = batch_writes_table &&
                               groove__storage__StagedWriteState__contains_key(groove__db__batch__DatabaseBatch__txn_operations(batch), table, tn, encoded_key.ptr, encoded_key.len);
    Reader r;
    reader_open(&r, db, staged_contains_key ? batch : NULL);
    jz_err e = get_and_decode(db, reader_dyn(&r), table, tn, table_schema, descriptor, encoded_key, found, out);
    reader_close(&r);
    return e;
}
jz_err groove__db__primary_storage__Database__primary_key_get_raw_with_storage(const groove_Database *db, groove_DynStorage storage, const char *table, u32 tn,
                                                                               const groove_Value *key, u32 n, bool *found, groove_EncodedKeyValue *out) {
    const groove_TableSchema *table_schema;
    const groove_PrimaryKey *primary_key;
    JZ_TRY(primary_key_schema(db, table, tn, &table_schema, &primary_key));
    groove_RecordDescriptor descriptor;
    JZ_TRY(groove__db__commit__Database__table_storage_descriptor(db, table, tn, &descriptor));
    jz_bytes encoded_key;
    JZ_TRY(KeyArity__encode_primary_key(KEY_ARITY_EXACT, table, tn, table_schema, primary_key, key, n, &encoded_key));
    return get_and_decode(db, storage, table, tn, table_schema, descriptor, encoded_key, found, out);
}
jz_err groove__db__primary_storage__Database__primary_key_scan_raw_with_storage(const groove_Database *db, groove_DynStorage storage, const char *table, u32 tn,
                                                                                const groove_Value *prefix, u32 n, groove_EncodedKeyValueVec *out) {
    const groove_TableSchema *table_schema;
    const groove_PrimaryKey *primary_key;
    JZ_TRY(primary_key_schema(db, table, tn, &table_schema, &primary_key));
    groove_RecordDescriptor descriptor;
    JZ_TRY(groove__db__commit__Database__table_storage_descriptor(db, table, tn, &descriptor));
    jz_bytes key_prefix;
    JZ_TRY(KeyArity__encode_primary_key(KEY_ARITY_PREFIX, table, tn, table_schema, primary_key, prefix, n, &key_prefix));
    groove_RecordStore store = groove__storage__RecordStore__new(storage, table, tn, descriptor);
    groove_KvPairVec rows;
    jz_err e = groove__storage__RecordStore__prefix(&store, key_prefix.ptr, key_prefix.len, &rows);
    jz_bytes_drop(&key_prefix);
    JZ_TRY(e);
    return decode_rows(db, table_schema, rows, out);
}
static jz_err decode_optional_row(const groove_Database *db, const groove_TableSchema *table_schema, bool some, groove_KvPair row, bool *found, groove_EncodedKeyValue *out) {
    *found = some;
    if (!some) return JZ_OK;
    return groove__db__encoding__Database__decode_stored_key_value(db, table_schema, row.key, row.value, out);
}
jz_err groove__db__primary_storage__Database__primary_key_last_raw_with_storage(const groove_Database *db, groove_DynStorage storage, const char *table, u32 tn,
                                                                                const groove_Value *prefix, u32 n, bool *found, groove_EncodedKeyValue *out) {
    const groove_TableSchema *table_schema;
    const groove_PrimaryKey *primary_key;
    JZ_TRY(primary_key_schema(db, table, tn, &table_schema, &primary_key));
    groove_RecordDescriptor descriptor;
    JZ_TRY(groove__db__commit__Database__table_storage_descriptor(db, table, tn, &descriptor));
    jz_bytes key_prefix;
    JZ_TRY(KeyArity__encode_primary_key(KEY_ARITY_PREFIX, table, tn, table_schema, primary_key, prefix, n, &key_prefix));
    groove_RecordStore store = groove__storage__RecordStore__new(storage, table, tn, descriptor);
    bool some;
    groove_KvPair row;
    jz_err e = groove__storage__RecordStore__last_with_prefix(&store, key_prefix.ptr, key_prefix.len, &some, &row);
    jz_bytes_drop(&key_prefix);
    JZ_TRY(e);
    return decode_optional_row(db, table_schema, some, row, found, out);
}
/* Return encoded records for an explicit primary-key logical range. */
jz_err groove__db__primary_storage__Database__primary_key_scan_range_raw(const groove_Database *db, const char *table, u32 tn, const groove_Value *start, u32 sn,
                                                                         const groove_Value *end, u32 en, groove_EncodedKeyValueVec *out);
static jz_err primary_key_scan_range_raw_with_storage(const groove_Database *db, groove_DynStorage storage, const char *table, u32 tn, const groove_Value *start, u32 sn,
                                                      const groove_Value *end, u32 en, groove_EncodedKeyValueVec *out) {
    const groove_TableSchema *table_schema;
    const groove_PrimaryKey *primary_key;
    JZ_TRY(primary_key_schema(db, table, tn, &table_schema, &primary_key));
    groove_RecordDescriptor descriptor;
    JZ_TRY(groove__db__commit__Database__table_storage_descriptor(db, table, tn, &descriptor));
    jz_bytes start_key, end_key;
    JZ_TRY(KeyArity__encode_primary_key(KEY_ARITY_EXACT, table, tn, table_schema, primary_key, start, sn, &start_key));
    jz_err e = KeyArity__encode_primary_key(KEY_ARITY_EXACT, table, tn, table_schema, primary_key, end, en, &end_key);
    if (e.code) { jz_bytes_drop(&start_key); return e; }
    groove_RecordStore store = groove__storage__RecordStore__new(storage, table, tn, descriptor);
    groove_KvPairVec rows;
    e = groove__storage__RecordStore__range(&store, start_key.ptr, start_key.len, end_key.ptr, end_key.len, &rows);
    jz_bytes_drop(&start_key);
    jz_bytes_drop(&end_key);
    JZ_TRY(e);
    return decode_rows(db, table_schema, rows, out);
}
jz_err groove__db__primary_storage__Database__primary_key_scan_range_raw(const groove_Database *db, const char *table, u32 tn, const groove_Value *start, u32 sn,
                                                                         const groove_Value *end, u32 en, groove_EncodedKeyValueVec *out) {
    Reader r;
    reader_open(&r, db, NULL);
    jz_err e = primary_key_scan_range_raw_with_storage(db, reader_dyn(&r), table, tn, start, sn, end, en, out);
    reader_close(&r);
    return e;
}
jz_err groove__db__primary_storage__Database__primary_key_scan_range_raw_in_batch(const groove_Database *db, groove_DatabaseBatch *batch, const char *table, u32 tn,
                                                                                  const groove_Value *start, u32 sn, const groove_Value *end, u32 en, groove_EncodedKeyValueVec *out) {
    JZ_TRY(groove__db__commit__Database__ensure_batch_storage_txn(db, batch));
    Reader r;
    reader_open(&r, db, batch);
    jz_err e = primary_key_scan_range_raw_with_storage(db, reader_dyn(&r), table, tn, start, sn, end, en, out);
    reader_close(&r);
    return e;
}
/* Return the last encoded record whose primary key starts with `prefix`. */
jz_err groove__db__primary_storage__Database__primary_key_last_raw(const groove_Database *db, const char *table, u32 tn, const groove_Value *prefix, u32 n, bool *found,
                                                                   groove_EncodedKeyValue *out) {
    Reader r;
    reader_open(&r, db, NULL);
    jz_err e = groove__db__primary_storage__Database__primary_key_last_raw_with_storage(db, reader_dyn(&r), table, tn, prefix, n, found, out);
    reader_close(&r);
    return e;
}
/* Return the last encoded record whose primary key starts with `prefix` and
 * whose full primary key is less than or equal to `upper`. */
jz_err groove__db__primary_storage__Database__primary_key_last_before_or_at_raw(const groove_Database *db, const char *table, u32 tn, const groove_Value *prefix, u32 n,
                                                                                const groove_Value *upper, u32 un, bool *found, groove_EncodedKeyValue *out) {
    const groove_TableSchema *table_schema;
    const groove_PrimaryKey *primary_key;
    JZ_TRY(primary_key_schema(db, table, tn, &table_schema, &primary_key));
    groove_RecordDescriptor descriptor;
    JZ_TRY(groove__db__commit__Database__table_storage_descriptor(db, table, tn, &descriptor));
    jz_bytes key_prefix, upper_key;
    JZ_TRY(KeyArity__encode_primary_key(KEY_ARITY_PREFIX, table, tn, table_schema, primary_key, prefix, n, &key_prefix));
    jz_err e = KeyArity__encode_primary_key(KEY_ARITY_EXACT, table, tn, table_schema, primary_key, upper, un, &upper_key);
    if (e.code) { jz_bytes_drop(&key_prefix); return e; }
    if (!(upper_key.len >= key_prefix.len && memcmp(upper_key.ptr, key_prefix.ptr, key_prefix.len) == 0)) {
        jz_bytes_drop(&key_prefix);
        jz_bytes_drop(&upper_key);
        *found = false;
        return JZ_OK;
    }
    Reader r;
    reader_open(&r, db, NULL);
    groove_RecordStore store = groove__storage__RecordStore__new(reader_dyn(&r), table, tn, descriptor);
    bool some;
    groove_KvPair row;
    e = groove__storage__RecordStore__last_with_prefix_before_or_at(&store, key_prefix.ptr, key_prefix.len, upper_key.ptr, upper_key.len, &some, &row);
    jz_bytes_drop(&key_prefix);
    jz_bytes_drop(&upper_key);
    if (!e.code) e = decode_optional_row(db, table_schema, some, row, found, out);
    reader_close(&r);
    return e;
}

/* Return encoded records whose explicit schema index exactly matches the key. */
jz_err groove__db__primary_storage__Database__index_get_raw(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                            const groove_Value *key, u32 n, groove_EncodedKeyValueVec *out) {
    const groove_IndexSchema *index;
    JZ_TRY(groove__db__primary_storage__Database__index(db, table, tn, index_name, inn, &index));
    JZ_TRY(KeyArity__check_index(KEY_ARITY_EXACT, index_name, inn, index->columns.len, n));
    return groove__db__primary_storage__Database__index_scan_raw(db, table, tn, index_name, inn, key, n, out);
}
/* Return encoded records whose explicit schema index starts with the prefix. */
jz_err groove__db__primary_storage__Database__index_scan_raw(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                             const groove_Value *prefix, u32 n, groove_EncodedKeyValueVec *out) {
    Reader r;
    reader_open(&r, db, NULL);
    jz_err e = groove__db__primary_storage__Database__index_scan_raw_with_storage(db, reader_dyn(&r), table, tn, index_name, inn, prefix, n, out);
    reader_close(&r);
    return e;
}
jz_err groove__db__primary_storage__Database__index_scan_raw_with_storage(const groove_Database *db, groove_DynStorage storage, const char *table, u32 tn,
                                                                          const char *index_name, u32 inn, const groove_Value *prefix, u32 n, groove_EncodedKeyValueVec *out) {
    const groove_IndexSchema *index;
    JZ_TRY(groove__db__primary_storage__Database__index(db, table, tn, index_name, inn, &index));
    JZ_TRY(KeyArity__check_index(KEY_ARITY_PREFIX, index_name, inn, index->columns.len, n));
    jz_bytes storage_prefix;
    JZ_TRY(groove__db__primary_storage__Database__persisted_index_scan_prefix(db, table, tn, index_name, inn, prefix, n, &storage_prefix));
    groove_RecordStore store = groove__db__facade__Database__durable_indices_store_with_storage(db, storage, groove__db__encoding__index_record_descriptor());
    groove_KvPairVec raw_entries;
    jz_err e = groove__storage__RecordStore__prefix(&store, storage_prefix.ptr, storage_prefix.len, &raw_entries);
    jz_bytes_drop(&storage_prefix);
    JZ_TRY(e);
    return groove__db__primary_storage__Database__decode_raw_index_entries_with_storage(db, storage, table, tn, index_name, inn, raw_entries, out);
}
/* Return the last encoded record whose explicit schema index starts with the prefix. */
jz_err groove__db__primary_storage__Database__index_last_raw(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                             const groove_Value *prefix, u32 n, bool *found, groove_EncodedKeyValue *out) {
    const groove_IndexSchema *index;
    JZ_TRY(groove__db__primary_storage__Database__index(db, table, tn, index_name, inn, &index));
    JZ_TRY(KeyArity__check_index(KEY_ARITY_PREFIX, index_name, inn, index->columns.len, n));
    jz_bytes storage_prefix;
    JZ_TRY(groove__db__primary_storage__Database__persisted_index_scan_prefix(db, table, tn, index_name, inn, prefix, n, &storage_prefix));
    Reader r;
    reader_open(&r, db, NULL);
    groove_RecordStore store = groove__db__facade__Database__durable_indices_store_with_storage(db, reader_dyn(&r), groove__db__encoding__index_record_descriptor());
    bool some;
    groove_KvPair raw_entry;
    jz_err e = groove__storage__RecordStore__last_with_prefix(&store, storage_prefix.ptr, storage_prefix.len, &some, &raw_entry);
    jz_bytes_drop(&storage_prefix);
    *found = false;
    if (!e.code && some) {
        groove_KvPairVec one = {0};
        JZ_VEC_PUSH(one, raw_entry);
        groove_EncodedKeyValueVec decoded;
        e = groove__db__primary_storage__Database__decode_raw_index_entries_with_storage(db, reader_dyn(&r), table, tn, index_name, inn, one, &decoded);
        if (!e.code) {
            if (decoded.len) {
                *found = true;
                *out = decoded.ptr[0];
                for (u32 i = 1; i < decoded.len; i++) groove__db__EncodedKeyValue__drop(&decoded.ptr[i]);
                jz_vec_drop(&decoded, sizeof(groove_EncodedKeyValue), NULL);
            } else {
                groove__db__EncodedKeyValueVec__drop(&decoded);
            }
        }
    }
    reader_close(&r);
    return e;
}
/* Return encoded records for an explicit schema index logical-key range. */
jz_err groove__db__primary_storage__Database__index_scan_range_raw(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                                   const groove_Value *start, u32 sn, const groove_Value *end, u32 en, groove_EncodedKeyValueVec *out) {
    const groove_IndexSchema *index;
    JZ_TRY(groove__db__primary_storage__Database__index(db, table, tn, index_name, inn, &index));
    JZ_TRY(KeyArity__check_index(KEY_ARITY_PREFIX, index_name, inn, index->columns.len, sn));
    JZ_TRY(KeyArity__check_index(KEY_ARITY_PREFIX, index_name, inn, index->columns.len, en));
    jz_bytes s, t;
    JZ_TRY(groove__db__primary_storage__Database__persisted_index_scan_prefix(db, table, tn, index_name, inn, start, sn, &s));
    jz_err e = groove__db__primary_storage__Database__persisted_index_scan_prefix(db, table, tn, index_name, inn, end, en, &t);
    if (e.code) { jz_bytes_drop(&s); return e; }
    Reader r;
    reader_open(&r, db, NULL);
    groove_RecordStore store = groove__db__facade__Database__durable_indices_store_with_storage(db, reader_dyn(&r), groove__db__encoding__index_record_descriptor());
    groove_KvPairVec raw_entries;
    e = groove__storage__RecordStore__range(&store, s.ptr, s.len, t.ptr, t.len, &raw_entries);
    jz_bytes_drop(&s);
    jz_bytes_drop(&t);
    if (!e.code) e = groove__db__primary_storage__Database__decode_raw_index_entries_with_storage(db, reader_dyn(&r), table, tn, index_name, inn, raw_entries, out);
    reader_close(&r);
    return e;
}

jz_err groove__db__primary_storage__Database__decode_raw_index_entries_with_storage(const groove_Database *db, groove_DynStorage storage, const char *table, u32 tn,
                                                                                    const char *index_name, u32 inn, groove_KvPairVec raw_entries, groove_EncodedKeyValueVec *out) {
    jz_err e;
    const groove_TableSchema *table_schema;
    groove_RecordDescriptor storage_descriptor;
    const groove_IndexSchema *index;
    groove_EncodedKeyValueVec records = {0};
    u32 i = 0;
    if ((e = groove__db__commit__Database__table(db, table, tn, &table_schema)).code) goto done;
    if ((e = groove__db__commit__Database__table_storage_descriptor(db, table, tn, &storage_descriptor)).code) goto done;
    groove_RecordStore store = groove__storage__RecordStore__new(storage, table, tn, storage_descriptor);
    if ((e = groove__db__primary_storage__Database__index(db, table, tn, index_name, inn, &index)).code) goto done;
    for (; i < raw_entries.len; i++) {
        groove_KvPair *entry = &raw_entries.ptr[i];
        jz_bytes primary_key;
        if ((e = groove__db__encoding__persisted_index_primary_key(table_schema, index_name, inn, index, entry->key.ptr, entry->key.len, entry->value.ptr, entry->value.len,
                                                                   &primary_key)).code) goto done;
        bool found;
        jz_bytes record;
        if ((e = groove__storage__RecordStore__get_raw(&store, primary_key.ptr, primary_key.len, &found, &record)).code) { jz_bytes_drop(&primary_key); goto done; }
        if (found) {
            groove_EncodedKeyValue decoded;
            if ((e = groove__db__encoding__Database__decode_stored_key_value(db, table_schema, primary_key, record, &decoded)).code) goto done;
            JZ_VEC_PUSH(records, decoded);
        } else {
            jz_bytes_drop(&primary_key);
            if (table_schema->has_primary_key) { e = groove__db__Error__str(GROOVE_DB_InvalidPersistedIndex, index_name, inn); goto done; }
        }
    }
done:
    jz_vec_drop(&raw_entries, sizeof(groove_KvPair), groove__storage__KvPair__drop);
    if (e.code) { groove__db__EncodedKeyValueVec__drop(&records); return e; }
    *out = records;
    return JZ_OK;
}

jz_err groove__db__primary_storage__Database__index(const groove_Database *db, const char *table, u32 tn, const char *index, u32 in, const groove_IndexSchema **out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    const groove_IndexSchema *i = groove__ivm__runtime__IvmRuntime__index(db->ivm_runtime, table, tn, index, in);
    if (!i) return groove__db__Error__pair(GROOVE_DB_IndexNotFound, table, tn, index, in);
    *out = i;
    return JZ_OK;
}
jz_err groove__db__primary_storage__Database__direct_record_store_schema(const groove_Database *db, const char *store, u32 n, const groove_DirectRecordStoreSchema **out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    const groove_DirectRecordStoreSchema *s = groove__ivm__runtime__IvmRuntime__direct_record_store(db->ivm_runtime, store, n);
    if (!s) return groove__db__Error__str(GROOVE_DB_DirectRecordStoreNotFound, store, n);
    *out = s;
    return JZ_OK;
}
jz_err groove__db__primary_storage__Database__persisted_index_scan_prefix(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                                          const groove_Value *prefix, u32 n, jz_bytes *out) {
    const groove_TableSchema *table_schema;
    JZ_TRY(groove__db__commit__Database__table(db, table, tn, &table_schema));
    const groove_IndexSchema *index;
    JZ_TRY(groove__db__primary_storage__Database__index(db, table, tn, index_name, inn, &index));
    jz_bytes logical_key = {0};
    u32 m = n < index->columns.len ? n : index->columns.len;
    for (u32 i = 0; i < m; i++) {
        const jz_str *column_name = &index->columns.ptr[i];
        const groove_ColumnSchema *column = NULL;
        for (u32 c = 0; c < table_schema->columns.len; c++)
            if (jz_str_eq(&table_schema->columns.ptr[c].name, column_name)) { column = &table_schema->columns.ptr[c]; break; }
        jz_err e;
        if (!column) e = groove__db__Error__invalid_persisted_index("index %.*s references unknown column %.*s", (int)inn, index_name, GROOVE_DB_S(*column_name));
        else e = groove__db__encoding__encode_index_prefix_part(&logical_key, &prefix[i], &column->column_type);
        if (e.code) { jz_bytes_drop(&logical_key); return e; }
    }
    /* Durable entries are the index id followed directly by the ordered,
     * prefix-free part encodings, so a logical prefix stays a byte prefix. */
    jz_bytes storage_prefix;
    groove__ivm__runtime__durable_index_key_prefix(groove__ivm__runtime__IvmRuntime__index_storage_id(db->ivm_runtime, table, tn, index), &storage_prefix);
    jz_bytes_push(&storage_prefix, logical_key.ptr, logical_key.len);
    jz_bytes_drop(&logical_key);
    *out = storage_prefix;
    return JZ_OK;
}
