/* groove::storage (crates/groove/src/storage/mod.rs), non-test part.
 * The forwarding `impl OrderedKvStorage for Rc<S>` / `for &S` collapse to the
 * same groove_DynStorage handle in C and therefore have no separate body. */
#include "jz_rt.h"
#include "mod.h"

/* ======================================================================
 * Error
 * ====================================================================== */
jz_err groove__storage__Error__column_family_not_found(jz_slice cf) {
    return jz_err_fmt(GROOVE_STORAGE_E_ColumnFamilyNotFound, "column family not found: %.*s", (int)cf.len, (const char *)cf.ptr);
}
jz_err groove__storage__Error__invalid_storage_layout(const char *msg) {
    return jz_err_fmt(GROOVE_STORAGE_E_InvalidStorageLayout, "invalid storage layout: %s", msg);
}
static jz_err invalid_storage_layout_owned(jz_str msg) {
    jz_err e = jz_err_fmt(GROOVE_STORAGE_E_InvalidStorageLayout, "invalid storage layout: %.*s", (int)msg.len, msg.ptr);
    jz_str_drop(&msg);
    return e;
}
jz_err groove__storage__Error__invalid_storage_key(const char *msg) {
    return jz_err_fmt(GROOVE_STORAGE_E_InvalidStorageKey, "invalid storage key: %s", msg);
}
jz_err groove__storage__Error__conditional_mutation_in_transaction(void) {
    return jz_err_new(GROOVE_STORAGE_E_ConditionalMutationInTransaction, "conditional mutations require a direct storage boundary");
}
jz_err groove__storage__Error__idb_generation_contention(usize retries) {
    return jz_err_fmt(GROOVE_STORAGE_E_IdbGenerationContention,
                      "IndexedDB storage remained contended after %zu generation-conflict retries", retries);
}
jz_err groove__storage__Error__backend(const char *backend, jz_slice message) {
    return jz_err_fmt(GROOVE_STORAGE_E_Backend, "%s storage error: %.*s", backend, (int)message.len, (const char *)message.ptr);
}
/* {:?} of Vec<String> */
static void debug_str_list(jz_str *out, const jz_str *items, u32 n) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < n; i++) {
        if (i) jz_str_push(out, ", ", 2);
        jz_str_push_char(out, '"');
        for (u32 j = 0; j < items[i].len; j++) {
            char c = items[i].ptr[j];
            if (c == '"' || c == '\\') jz_str_push_char(out, '\\');
            jz_str_push_char(out, c);
        }
        jz_str_push_char(out, '"');
    }
    jz_str_push_char(out, ']');
}
jz_err groove__storage__Error__unsupported_storage_codecs(u16 epoch, const jz_str *missing, u32 nmissing, const jz_str *unknown, u32 nunknown) {
    jz_str m = jz_format("unsupported storage format: this epoch-%u root lacks codec families ", (unsigned)epoch);
    debug_str_list(&m, missing, nmissing);
    jz_str_push_cstr(&m, " required by this build and declares ");
    debug_str_list(&m, unknown, nunknown);
    jz_str_push_cstr(&m, " that this build does not read");
    jz_err e = jz_err_fmt(GROOVE_STORAGE_E_UnsupportedStorageCodecs, "%.*s", (int)m.len, m.ptr);
    jz_str_drop(&m);
    return e;
}
jz_err groove__storage__Error__From_records__from(jz_err e) {
    if (!e.code) return e;
    e = jz_err_context(e, "record error");
    e.code = GROOVE_STORAGE_E_Record;
    return e;
}
bool groove__storage__Error__is_column_family_not_found(const jz_err *e) {
    return e->code == GROOVE_STORAGE_E_ColumnFamilyNotFound;
}

/* ======================================================================
 * small slice helpers
 * ====================================================================== */
static jz_slice cstr_slice(const char *s) { return jz_slice_of(s, (u32)strlen(s)); }
static bool slice_eq(jz_slice a, jz_slice b) { return a.len == b.len && (a.len == 0 || memcmp(a.ptr, b.ptr, a.len) == 0); }
static bool slice_eq_cstr(jz_slice a, const char *b) { return slice_eq(a, cstr_slice(b)); }
static bool starts_with(jz_slice s, const char *p) {
    u32 n = (u32)strlen(p);
    return s.len >= n && memcmp(s.ptr, p, n) == 0;
}
static bool ends_with(jz_slice s, const char *p) {
    u32 n = (u32)strlen(p);
    return s.len >= n && memcmp(s.ptr + s.len - n, p, n) == 0;
}
static bool contains_str(jz_slice s, const char *p) {
    u32 n = (u32)strlen(p);
    if (n == 0) return true;
    for (u32 i = 0; i + n <= s.len; i++)
        if (memcmp(s.ptr + i, p, n) == 0) return true;
    return false;
}
static bool bytes_starts_with(jz_slice s, jz_slice p) { return s.len >= p.len && (p.len == 0 || memcmp(s.ptr, p.ptr, p.len) == 0); }
/* borrowed jz_bytes view of a slice for btree lookups (never dropped) */
static jz_bytes bytes_view(jz_slice s) { jz_bytes b = {(u8 *)s.ptr, s.len, 0}; return b; }
static jz_str str_view(jz_slice s) { jz_str b = {(char *)s.ptr, s.len, 0}; return b; }
static jz_bytes slice_to_bytes(jz_slice s) { return jz_bytes_from(s.ptr, s.len); }
static jz_str slice_to_str(jz_slice s) { return jz_str_from_slice(s); }

/* ======================================================================
 * names
 * ====================================================================== */
bool groove__storage__prefix_successor(jz_slice prefix, jz_bytes *out) {
    return groove__storage__key_codec__increment_bytes(prefix, out);
}

jz_err groove__storage__validate_physical_storage_name(jz_slice name) {
    if (name.len > GROOVE_MAX_APPLICATION_STORAGE_NAME_BYTES) {
        return invalid_storage_layout_owned(jz_format("storage name exceeds the %u-byte limit: %u bytes",
                                                      (unsigned)GROOVE_MAX_APPLICATION_STORAGE_NAME_BYTES, (unsigned)name.len));
    }
    if (name.len && memchr(name.ptr, '\0', name.len)) {
        return groove__storage__Error__invalid_storage_layout("storage name contains an embedded NUL");
    }
    return JZ_OK;
}

jz_err groove__storage__validate_physical_storage_names(const jz_str *names, u32 n) {
    for (u32 i = 0; i < n; i++) JZ_TRY(groove__storage__validate_physical_storage_name(jz_str_slice(&names[i])));
    return JZ_OK;
}

jz_err groove__storage__validate_physical_storage_names_cstr(const char *const *names, u32 n) {
    for (u32 i = 0; i < n; i++) JZ_TRY(groove__storage__validate_physical_storage_name(cstr_slice(names[i])));
    return JZ_OK;
}

jz_err groove__storage__validate_application_storage_name(jz_slice name) {
    JZ_TRY(groove__storage__validate_physical_storage_name(name));
    if (starts_with(name, "__groove_") || slice_eq_cstr(name, "indices") || slice_eq_cstr(name, "default")) {
        jz_str m = jz_format("application storage name is reserved by Groove: \"%.*s\"", (int)name.len, (const char *)name.ptr);
        return invalid_storage_layout_owned(m);
    }
    return JZ_OK;
}

/* ======================================================================
 * ScanBounds / ScanRequest
 * ====================================================================== */
void groove__storage__ScanBounds__drop(groove_ScanBounds *b) {
    jz_bytes_drop(&b->start);
    jz_bytes_drop(&b->end);
}
void groove__storage__ScanBounds__Clone__clone(groove_ScanBounds *dst, const groove_ScanBounds *src) {
    dst->tag = src->tag;
    dst->start = jz_bytes_clone(&src->start);
    dst->end = src->tag == GROOVE_SCAN_RANGE ? jz_bytes_clone(&src->end) : (jz_bytes){0};
}
bool groove__storage__ScanBounds__is_empty_range(const groove_ScanBounds *b) {
    return b->tag == GROOVE_SCAN_RANGE && jz_bytes_cmp(&b->start, &b->end) >= 0;
}

groove_ScanRequest groove__storage__ScanRequest__prefix(jz_str cf, jz_bytes prefix) {
    groove_ScanRequest r = {0};
    r.cf = cf;
    r.bounds.tag = GROOVE_SCAN_PREFIX;
    r.bounds.start = prefix;
    r.direction = GROOVE_SCAN_FORWARD;
    r.has_max_items = false;
    return r;
}
groove_ScanRequest groove__storage__ScanRequest__range(jz_str cf, jz_bytes start, jz_bytes end) {
    groove_ScanRequest r = {0};
    r.cf = cf;
    r.bounds.tag = GROOVE_SCAN_RANGE;
    r.bounds.start = start;
    r.bounds.end = end;
    r.direction = GROOVE_SCAN_FORWARD;
    return r;
}
groove_ScanRequest groove__storage__ScanRequest__with_max_items(groove_ScanRequest r, usize max_items) {
    r.has_max_items = true;
    r.max_items = max_items;
    return r;
}
groove_ScanRequest groove__storage__ScanRequest__reversed(groove_ScanRequest r) {
    r.direction = GROOVE_SCAN_REVERSE;
    return r;
}
void groove__storage__ScanRequest__Clone__clone(groove_ScanRequest *dst, const groove_ScanRequest *src) {
    *dst = *src;
    dst->cf = jz_str_clone(&src->cf);
    groove__storage__ScanBounds__Clone__clone(&dst->bounds, &src->bounds);
}
void groove__storage__ScanRequest__drop(groove_ScanRequest *r) {
    jz_str_drop(&r->cf);
    groove__storage__ScanBounds__drop(&r->bounds);
}

void groove__storage__KeyValue__drop(void *p) {
    groove_KeyValue *kv = (groove_KeyValue *)p;
    jz_bytes_drop(&kv->key);
    jz_bytes_drop(&kv->value);
}
void groove__storage__KeyValue__clone(void *dst, const void *src) {
    const groove_KeyValue *s = (const groove_KeyValue *)src;
    groove_KeyValue *d = (groove_KeyValue *)dst;
    d->key = jz_bytes_clone(&s->key);
    d->value = jz_bytes_clone(&s->value);
}
void groove__storage__KeyValueVec__drop(groove_KeyValueVec *v) { jz_vec_drop(v, sizeof(groove_KeyValue), groove__storage__KeyValue__drop); }

static void str_drop_v(void *p) { jz_str_drop((jz_str *)p); }
static void bytes_drop_v(void *p) { jz_bytes_drop((jz_bytes *)p); }
static void str_clone_v(void *d, const void *s) { *(jz_str *)d = jz_str_clone((const jz_str *)s); }
static void bytes_clone_v(void *d, const void *s) { *(jz_bytes *)d = jz_bytes_clone((const jz_bytes *)s); }
void groove__storage__StrVec__drop(groove_StrVec *v) { jz_vec_drop(v, sizeof(jz_str), str_drop_v); }

/* ======================================================================
 * WriteOperation / OwnedWriteOperation / WriteManyOutcome
 * ====================================================================== */
void groove__storage__OwnedWriteOperation__drop(void *p) {
    groove_OwnedWriteOperation *op = (groove_OwnedWriteOperation *)p;
    jz_str_drop(&op->cf);
    jz_bytes_drop(&op->key);
    jz_bytes_drop(&op->value);
}
void groove__storage__OwnedWriteOperation__clone(void *dst, const void *src) {
    const groove_OwnedWriteOperation *s = (const groove_OwnedWriteOperation *)src;
    groove_OwnedWriteOperation *d = (groove_OwnedWriteOperation *)dst;
    d->tag = s->tag;
    d->cf = jz_str_clone(&s->cf);
    d->key = jz_bytes_clone(&s->key);
    d->value = s->tag == GROOVE_WRITE_SET ? jz_bytes_clone(&s->value) : (jz_bytes){0};
}
void groove__storage__OwnedWriteOperationVec__drop(groove_OwnedWriteOperationVec *v) {
    jz_vec_drop(v, sizeof(groove_OwnedWriteOperation), groove__storage__OwnedWriteOperation__drop);
}
bool groove__storage__OwnedWriteOperation__PartialEq__eq(const groove_OwnedWriteOperation *a, const groove_OwnedWriteOperation *b) {
    if (a->tag != b->tag || !jz_str_eq(&a->cf, &b->cf) || !jz_bytes_eq(&a->key, &b->key)) return false;
    return a->tag == GROOVE_WRITE_DELETE || jz_bytes_eq(&a->value, &b->value);
}
groove_WriteOperation groove__storage__WriteOperation__set(jz_slice cf, jz_slice key, jz_slice value) {
    groove_WriteOperation op = {GROOVE_WRITE_SET, cf, key, value};
    return op;
}
groove_WriteOperation groove__storage__WriteOperation__delete(jz_slice cf, jz_slice key) {
    groove_WriteOperation op = {GROOVE_WRITE_DELETE, cf, key, {0}};
    return op;
}
groove_WriteOperation groove__storage__OwnedWriteOperation__as_write_operation(const groove_OwnedWriteOperation *op) {
    if (op->tag == GROOVE_WRITE_SET)
        return groove__storage__WriteOperation__set(jz_str_slice(&op->cf), jz_bytes_slice(&op->key), jz_bytes_slice(&op->value));
    return groove__storage__WriteOperation__delete(jz_str_slice(&op->cf), jz_bytes_slice(&op->key));
}
groove_OwnedWriteOperation groove__storage__OwnedWriteOperation__From__from(const groove_WriteOperation *op) {
    groove_OwnedWriteOperation o = {0};
    o.tag = op->tag;
    o.cf = slice_to_str(op->cf);
    o.key = slice_to_bytes(op->key);
    if (op->tag == GROOVE_WRITE_SET) o.value = slice_to_bytes(op->value);
    return o;
}
static jz_slice owned_op_cf(const groove_OwnedWriteOperation *op) { return jz_str_slice(&op->cf); }
static jz_slice owned_op_key(const groove_OwnedWriteOperation *op) { return jz_bytes_slice(&op->key); }

bool groove__storage__WriteManyOutcome__is_committed(const groove_WriteManyOutcome *o) { return o->tag == GROOVE_WMO_COMMITTED; }
const jz_err *groove__storage__WriteManyOutcome__error(const groove_WriteManyOutcome *o) {
    return o->tag == GROOVE_WMO_COMMITTED ? NULL : &o->error;
}
bool groove__storage__WriteManyOutcome__may_have_committed(const groove_WriteManyOutcome *o) { return o->tag != GROOVE_WMO_UNCOMMITTED; }
static groove_WriteManyOutcome wmo(u32 tag, jz_err e) { groove_WriteManyOutcome o = {tag, e}; return o; }

/* ======================================================================
 * StorageCursor, ReadyStorageCursor, collect_scan
 * ====================================================================== */
jz_err groove__storage__StorageCursor__next_batch(groove_StorageScan *scan, bool *some, groove_KeyValueVec *out) {
    return scan->vt->next_batch(scan->self, some, out);
}
void groove__storage__StorageScan__drop(groove_StorageScan *scan) {
    if (scan->vt && scan->vt->drop) scan->vt->drop(scan->self);
    scan->self = NULL;
    scan->vt = NULL;
}

typedef struct ReadyStorageCursor { groove_KeyValueVec values; u32 pos; } ReadyStorageCursor;
static jz_err ready_cursor_next_batch(void *self, bool *some, groove_KeyValueVec *out) {
    ReadyStorageCursor *c = (ReadyStorageCursor *)self;
    groove_KeyValueVec batch = {0};
    while (c->pos < c->values.len && batch.len < 256) {
        jz_vec_push(&batch, &c->values.ptr[c->pos], sizeof(groove_KeyValue));
        c->pos++;
    }
    *some = batch.len != 0;
    if (*some) *out = batch;
    else jz_vec_drop(&batch, sizeof(groove_KeyValue), NULL);
    return JZ_OK;
}
static void ready_cursor_drop(void *self) {
    ReadyStorageCursor *c = (ReadyStorageCursor *)self;
    for (u32 i = c->pos; i < c->values.len; i++) groove__storage__KeyValue__drop(&c->values.ptr[i]);
    jz_vec_drop(&c->values, sizeof(groove_KeyValue), NULL);
    jz_free(c);
}
static const groove_StorageCursorVtbl READY_CURSOR_VTBL = {ready_cursor_next_batch, ready_cursor_drop};

groove_StorageScan groove__storage__ReadyStorageCursor__new(groove_KeyValueVec values) {
    ReadyStorageCursor *c = JZ_NEW(ReadyStorageCursor);
    c->values = values;
    c->pos = 0;
    groove_StorageScan s = {c, &READY_CURSOR_VTBL};
    return s;
}

jz_err groove__storage__collect_scan(groove_StorageScan scan, groove_KeyValueVec *out) {
    groove_KeyValueVec values = {0};
    for (;;) {
        bool some = false;
        groove_KeyValueVec batch = {0};
        jz_err e = groove__storage__StorageCursor__next_batch(&scan, &some, &batch);
        if (e.code) {
            groove__storage__KeyValueVec__drop(&values);
            groove__storage__StorageScan__drop(&scan);
            return e;
        }
        if (!some) break;
        jz_vec_extend(&values, batch.ptr, batch.len, sizeof(groove_KeyValue));
        jz_vec_drop(&batch, sizeof(groove_KeyValue), NULL);
    }
    groove__storage__StorageScan__drop(&scan);
    *out = values;
    return JZ_OK;
}

/* ======================================================================
 * OrderedKvStorage dispatch (+ trait defaults)
 * ====================================================================== */
bool groove__storage__OrderedKvStorage__permits_eager_read_retry(const groove_DynStorage *s) {
    return s->vt->permits_eager_read_retry ? s->vt->permits_eager_read_retry(s->self) : false;
}
jz_err groove__storage__OrderedKvStorage__get(const groove_DynStorage *s, jz_slice cf, jz_slice key, bool *some, jz_bytes *out) {
    return s->vt->get(s->self, cf, key, some, out);
}
jz_err groove__storage__OrderedKvStorage__compare_value(const groove_DynStorage *s, jz_slice cf, jz_slice key, jz_slice expected, groove_ValueComparison *out) {
    if (s->vt->compare_value) return s->vt->compare_value(s->self, cf, key, expected, out);
    bool some = false;
    jz_bytes value = {0};
    JZ_TRY(groove__storage__OrderedKvStorage__get(s, cf, key, &some, &value));
    if (!some) *out = GROOVE_VALUE_ABSENT;
    else if (jz_slice_cmp(jz_bytes_slice(&value), expected) == 0) *out = GROOVE_VALUE_IDENTICAL;
    else *out = GROOVE_VALUE_DIFFERENT;
    jz_bytes_drop(&value);
    return JZ_OK;
}
jz_err groove__storage__OrderedKvStorage__put_if_absent(const groove_DynStorage *s, jz_slice cf, jz_slice key, jz_bytes value, bool *some, jz_bytes *existing) {
    return s->vt->put_if_absent(s->self, cf, key, value, some, existing);
}
jz_err groove__storage__OrderedKvStorage__compare_and_delete(const groove_DynStorage *s, jz_slice cf, jz_slice key, jz_slice expected, bool *removed) {
    return s->vt->compare_and_delete(s->self, cf, key, expected, removed);
}
jz_err groove__storage__OrderedKvStorage__set(const groove_DynStorage *s, jz_slice cf, jz_slice key, jz_bytes value) {
    return s->vt->set(s->self, cf, key, value);
}
jz_err groove__storage__OrderedKvStorage__delete(const groove_DynStorage *s, jz_slice cf, jz_slice key) {
    return s->vt->delete_(s->self, cf, key);
}
jz_err groove__storage__OrderedKvStorage__close(const groove_DynStorage *s) {
    return s->vt->close ? s->vt->close(s->self) : JZ_OK;
}
jz_err groove__storage__OrderedKvStorage__set_write_flush_cadence(const groove_DynStorage *s, usize every) {
    return s->vt->set_write_flush_cadence ? s->vt->set_write_flush_cadence(s->self, every) : JZ_OK;
}
jz_err groove__storage__OrderedKvStorage__flush_write_boundary(const groove_DynStorage *s) {
    return s->vt->flush_write_boundary ? s->vt->flush_write_boundary(s->self) : JZ_OK;
}
usize groove__storage__OrderedKvStorage__cache_token(const groove_DynStorage *s) { return (usize)(uintptr_t)s->self; }
jz_err groove__storage__OrderedKvStorage__approximate_class_bytes(const groove_DynStorage *s, jz_slice cf, bool *some, u64 *out) {
    if (s->vt->approximate_class_bytes) return s->vt->approximate_class_bytes(s->self, cf, some, out);
    *some = false;
    return JZ_OK;
}
jz_err groove__storage__OrderedKvStorage__scan(const groove_DynStorage *s, groove_ScanRequest request, groove_StorageScan *out) {
    return s->vt->scan(s->self, request, out);
}
jz_err groove__storage__OrderedKvStorage__last_with_prefix(const groove_DynStorage *s, jz_slice cf, jz_slice prefix, bool *some, groove_KeyValue *out) {
    if (s->vt->last_with_prefix) return s->vt->last_with_prefix(s->self, cf, prefix, some, out);
    groove_ScanRequest req = groove__storage__ScanRequest__with_max_items(
        groove__storage__ScanRequest__reversed(groove__storage__ScanRequest__prefix(slice_to_str(cf), slice_to_bytes(prefix))), 1);
    groove_StorageScan scan;
    JZ_TRY(groove__storage__OrderedKvStorage__scan(s, req, &scan));
    groove_KeyValueVec values;
    JZ_TRY(groove__storage__collect_scan(scan, &values));
    *some = values.len > 0;
    if (*some) {
        *out = values.ptr[0];
        for (u32 i = 1; i < values.len; i++) groove__storage__KeyValue__drop(&values.ptr[i]);
    }
    jz_vec_drop(&values, sizeof(groove_KeyValue), NULL);
    return JZ_OK;
}
jz_err groove__storage__OrderedKvStorage__last_with_prefix_before_or_at(const groove_DynStorage *s, jz_slice cf, jz_slice prefix, jz_slice upper, bool *some, groove_KeyValue *out) {
    if (s->vt->last_with_prefix_before_or_at) return s->vt->last_with_prefix_before_or_at(s->self, cf, prefix, upper, some, out);
    groove_StorageScan scan;
    JZ_TRY(groove__storage__OrderedKvStorage__scan(s, groove__storage__ScanRequest__prefix(slice_to_str(cf), slice_to_bytes(prefix)), &scan));
    groove_KeyValueVec values;
    JZ_TRY(groove__storage__collect_scan(scan, &values));
    /* .take_while(|(key, _)| key <= &upper).last() */
    u32 taken = 0;
    while (taken < values.len && jz_slice_cmp(jz_bytes_slice(&values.ptr[taken].key), upper) <= 0) taken++;
    *some = taken > 0;
    for (u32 i = 0; i < values.len; i++) {
        if (*some && i == taken - 1) *out = values.ptr[i];
        else groove__storage__KeyValue__drop(&values.ptr[i]);
    }
    jz_vec_drop(&values, sizeof(groove_KeyValue), NULL);
    return JZ_OK;
}
jz_err groove__storage__OrderedKvStorage__write_many(const groove_DynStorage *s, groove_OwnedWriteOperationVec operations) {
    return s->vt->write_many(s->self, operations);
}
groove_WriteManyOutcome groove__storage__OrderedKvStorage__write_many_outcome(const groove_DynStorage *s, groove_OwnedWriteOperationVec operations) {
    if (s->vt->write_many_outcome) return s->vt->write_many_outcome(s->self, operations);
    jz_err e = groove__storage__OrderedKvStorage__write_many(s, operations);
    if (!e.code) return wmo(GROOVE_WMO_COMMITTED, JZ_OK);
    return wmo(GROOVE_WMO_POSSIBLY_COMMITTED, e);
}
groove_WriteManyOutcome groove__storage__OrderedKvStorage__write_many_borrowed_outcome(const groove_DynStorage *s, groove_WriteOperationVec operations) {
    if (s->vt->write_many_borrowed_outcome) return s->vt->write_many_borrowed_outcome(s->self, operations);
    groove_OwnedWriteOperationVec owned = {0};
    jz_vec_reserve(&owned, operations.len, sizeof(groove_OwnedWriteOperation));
    for (u32 i = 0; i < operations.len; i++) JZ_VEC_PUSH(owned, groove__storage__OwnedWriteOperation__From__from(&operations.ptr[i]));
    jz_vec_drop(&operations, sizeof(groove_WriteOperation), NULL);
    return groove__storage__OrderedKvStorage__write_many_outcome(s, owned);
}
bool groove__storage__OrderedKvStorage__column_family_names(const groove_DynStorage *s, groove_StrVec *out) {
    return s->vt->column_family_names ? s->vt->column_family_names(s->self, out) : false;
}
jz_err groove__storage__OrderedKvStorage__range(const groove_DynStorage *s, jz_slice cf, jz_slice start, jz_slice end, groove_KeyValueVec *out) {
    if (s->vt->range) return s->vt->range(s->self, cf, start, end, out);
    groove_StorageScan scan;
    JZ_TRY(groove__storage__OrderedKvStorage__scan(s, groove__storage__ScanRequest__range(slice_to_str(cf), slice_to_bytes(start), slice_to_bytes(end)), &scan));
    return groove__storage__collect_scan(scan, out);
}
jz_err groove__storage__OrderedKvStorage__prefix(const groove_DynStorage *s, jz_slice cf, jz_slice prefix, groove_KeyValueVec *out) {
    if (s->vt->prefix) return s->vt->prefix(s->self, cf, prefix, out);
    groove_StorageScan scan;
    JZ_TRY(groove__storage__OrderedKvStorage__scan(s, groove__storage__ScanRequest__prefix(slice_to_str(cf), slice_to_bytes(prefix)), &scan));
    return groove__storage__collect_scan(scan, out);
}

/* ======================================================================
 * OwnedStorage
 * ====================================================================== */
static void owned_storage_rc_drop(void *p) {
    groove_DynStorage *d = (groove_DynStorage *)p;
    if (d->vt && d->vt->drop) d->vt->drop(d->self);
}
groove_OwnedStorage groove__storage__OwnedStorage__new(groove_DynStorage storage) {
    groove_DynStorage *rc = (groove_DynStorage *)jz_rc_new(sizeof(groove_DynStorage), owned_storage_rc_drop);
    *rc = storage;
    groove_OwnedStorage s = {rc};
    return s;
}
groove_OwnedStorage groove__storage__OwnedStorage__Clone__clone(const groove_OwnedStorage *s) {
    groove_OwnedStorage c = {(groove_DynStorage *)jz_rc_clone(s->rc)};
    return c;
}
void groove__storage__OwnedStorage__drop(groove_OwnedStorage *s) {
    jz_rc_release(s->rc);
    s->rc = NULL;
}
const groove_DynStorage *groove__storage__OwnedStorage__as_ref(const groove_OwnedStorage *s) { return s->rc; }
jz_err groove__storage__OwnedStorage__get(const groove_OwnedStorage *s, jz_slice cf, jz_slice key, bool *some, jz_bytes *out) {
    groove_OwnedStorage storage = groove__storage__OwnedStorage__Clone__clone(s);
    jz_err e = groove__storage__OrderedKvStorage__get(storage.rc, cf, key, some, out);
    groove__storage__OwnedStorage__drop(&storage);
    return e;
}
jz_err groove__storage__OwnedStorage__scan(const groove_OwnedStorage *s, groove_ScanRequest request, groove_KeyValueVec *out) {
    groove_OwnedStorage storage = groove__storage__OwnedStorage__Clone__clone(s);
    groove_StorageScan scan;
    jz_err e = groove__storage__OrderedKvStorage__scan(storage.rc, request, &scan);
    if (!e.code) e = groove__storage__collect_scan(scan, out);
    groove__storage__OwnedStorage__drop(&storage);
    return e;
}

/* ======================================================================
 * StorageLayout
 * ====================================================================== */
#define CLASS_HISTORY_CF "__groove_class_history"
#define CLASS_REGISTER_CF "__groove_class_register"
#define CLASS_GLOBAL_CURRENT_CF "__groove_class_global_current"
#define CLASS_AHEAD_CURRENT_CF "__groove_class_ahead_current"
#define CLASS_CHANGES_CF "__groove_class_changes"
#define CLASS_INDICES_CF "__groove_class_indices"
#define CLASS_META_CF "__groove_class_meta"
#define CLASS_LAYOUT_MARKER_KEY "groove-storage-layout"
#define CLASS_LAYOUT_MARKER_VALUE "class-cf-v2"

static bool is_jazz_history_table(jz_slice name) { return starts_with(name, "jazz_") && ends_with(name, "_history"); }
static bool is_jazz_register_table(jz_slice name) {
    return starts_with(name, "jazz_") && ends_with(name, "_register") && !ends_with(name, "_register_global_current") &&
           !ends_with(name, "_register_ahead_current");
}
static bool is_jazz_global_current_table(jz_slice name) {
    return starts_with(name, "jazz_") && (ends_with(name, "_global_current") || ends_with(name, "_register_global_current")) &&
           !contains_str(name, "_ahead_current");
}
static bool is_jazz_ahead_current_table(jz_slice name) {
    return starts_with(name, "jazz_") && (ends_with(name, "_ahead_current") || ends_with(name, "_register_ahead_current"));
}
static const char *jazz_physical_class(jz_slice logical_cf) {
    if (is_jazz_history_table(logical_cf)) return CLASS_HISTORY_CF;
    if (is_jazz_register_table(logical_cf)) return CLASS_REGISTER_CF;
    if (is_jazz_global_current_table(logical_cf)) return CLASS_GLOBAL_CURRENT_CF;
    if (is_jazz_ahead_current_table(logical_cf)) return CLASS_AHEAD_CURRENT_CF;
    if (slice_eq_cstr(logical_cf, "jazz_global_changes")) return CLASS_CHANGES_CF;
    /* The only family in the indices class; its keys are stored without the
     * length-framed family name. */
    if (slice_eq_cstr(logical_cf, "indices")) return CLASS_INDICES_CF;
    if (starts_with(logical_cf, "jazz_")) return CLASS_META_CF;
    return NULL;
}

groove_StorageLayout groove__storage__StorageLayout__jazz_class_v2(void) { return GROOVE_LAYOUT_JAZZ_CLASS_V2; }

static const jz_btree_desc STR_SET_DESC = {sizeof(jz_str), 0, jz_cmp_str, str_drop_v, NULL};

groove_StrVec groove__storage__StorageLayout__physical_column_families(groove_StorageLayout layout, const jz_slice *logical, u32 n) {
    jz_btree names;
    jz_btree_init(&names, &STR_SET_DESC);
    if (layout == GROOVE_LAYOUT_JAZZ_CLASS_V2) {
        jz_str meta = jz_str_from(CLASS_META_CF);
        if (jz_btree_contains(&names, &meta)) jz_str_drop(&meta);
        else jz_btree_insert(&names, &meta, NULL, NULL);
    }
    for (u32 i = 0; i < n; i++) {
        jz_slice physical = logical[i];
        if (layout == GROOVE_LAYOUT_JAZZ_CLASS_V2) {
            const char *cls = jazz_physical_class(logical[i]);
            if (cls) physical = cstr_slice(cls);
        }
        jz_str view = str_view(physical);
        if (!jz_btree_contains(&names, &view)) {
            jz_str owned = slice_to_str(physical);
            jz_btree_insert(&names, &owned, NULL, NULL);
        }
    }
    groove_StrVec out = {0};
    jz_str tmp;
    while (jz_btree_pop_first(&names, &tmp, NULL)) JZ_VEC_PUSH(out, tmp);
    jz_btree_drop(&names);
    return out;
}

typedef struct PhysicalCf { jz_slice physical_cf; bool has_logical_prefix; jz_slice logical_prefix; } PhysicalCf;

static jz_err storage_layout_map_cf(groove_StorageLayout layout, jz_slice logical_cf, PhysicalCf *out) {
    /* Validate before framing so no invalid logical CF acquires a durable spelling. */
    JZ_TRY(groove__storage__validate_physical_storage_name(logical_cf));
    out->physical_cf = logical_cf;
    out->has_logical_prefix = false;
    out->logical_prefix = (jz_slice){0};
    if (layout == GROOVE_LAYOUT_JAZZ_CLASS_V2) {
        const char *physical_cf = jazz_physical_class(logical_cf);
        if (physical_cf) {
            out->physical_cf = cstr_slice(physical_cf);
            /* The indices class is stored unframed. */
            if (strcmp(physical_cf, CLASS_INDICES_CF) != 0) {
                out->has_logical_prefix = true;
                out->logical_prefix = logical_cf;
            }
        }
    }
    return JZ_OK;
}

static bool storage_layout_validates_marker(groove_StorageLayout layout) { return layout == GROOVE_LAYOUT_JAZZ_CLASS_V2; }

/* ======================================================================
 * MappedStorageCursor
 * ====================================================================== */
typedef struct MappedStorageCursor { groove_StorageScan inner; usize strip_len; } MappedStorageCursor;
static jz_err mapped_cursor_next_batch(void *self, bool *some, groove_KeyValueVec *out) {
    MappedStorageCursor *c = (MappedStorageCursor *)self;
    groove_KeyValueVec batch = {0};
    bool has = false;
    JZ_TRY(groove__storage__StorageCursor__next_batch(&c->inner, &has, &batch));
    if (!has) {
        *some = false;
        return JZ_OK;
    }
    groove_KeyValueVec mapped = {0};
    jz_vec_reserve(&mapped, batch.len, sizeof(groove_KeyValue));
    for (u32 i = 0; i < batch.len; i++) {
        groove_KeyValue *kv = &batch.ptr[i];
        if (kv->key.len < c->strip_len) {
            for (u32 j = i; j < batch.len; j++) groove__storage__KeyValue__drop(&batch.ptr[j]);
            jz_vec_drop(&batch, sizeof(groove_KeyValue), NULL);
            groove__storage__KeyValueVec__drop(&mapped);
            return groove__storage__Error__invalid_storage_key("physical layout key shorter than logical prefix");
        }
        groove_KeyValue m = {jz_bytes_from(kv->key.ptr + c->strip_len, kv->key.len - (u32)c->strip_len), kv->value};
        jz_bytes_drop(&kv->key);
        JZ_VEC_PUSH(mapped, m);
    }
    jz_vec_drop(&batch, sizeof(groove_KeyValue), NULL);
    *some = true;
    *out = mapped;
    return JZ_OK;
}
static void mapped_cursor_drop(void *self) {
    MappedStorageCursor *c = (MappedStorageCursor *)self;
    groove__storage__StorageScan__drop(&c->inner);
    jz_free(c);
}
static const groove_StorageCursorVtbl MAPPED_CURSOR_VTBL = {mapped_cursor_next_batch, mapped_cursor_drop};

/* ======================================================================
 * BoxedStorage / ErasedReopenableStorage
 * ====================================================================== */
groove_BoxedStorage groove__storage__BoxedStorage__new(groove_DynStorage storage) {
    groove_BoxedStorage b = {storage};
    return b;
}
/* BoxedStorage's OrderedKvStorage impl forwards every method to `inner`. */
groove_DynStorage groove__storage__BoxedStorage__as_dyn(groove_BoxedStorage *b) { return b->inner; }
void groove__storage__BoxedStorage__drop(groove_BoxedStorage *b) {
    if (b->inner.vt && b->inner.vt->drop) b->inner.vt->drop(b->inner.self);
    b->inner.self = NULL;
    b->inner.vt = NULL;
}
jz_err groove__storage__ErasedReopenableStorage__reopen_boxed(groove_DynStorage self, groove_StrVec column_families, groove_DynStorage *out) {
    JZ_ASSERT(self.vt->reopen != NULL, "storage is not reopenable");
    return self.vt->reopen(self.self, column_families, out);
}
jz_err groove__storage__BoxedStorage__ReopenableStorage__reopen(groove_BoxedStorage self, groove_StrVec column_families, groove_BoxedStorage *out) {
    groove_DynStorage inner;
    JZ_TRY(groove__storage__ErasedReopenableStorage__reopen_boxed(self.inner, column_families, &inner));
    out->inner = inner;
    return JZ_OK;
}

/* ======================================================================
 * LayoutStorage
 * ====================================================================== */
static jz_err layout_ensure_layout_marker(const groove_LayoutStorage *s);

jz_err groove__storage__LayoutStorage__new(groove_DynStorage inner, groove_StorageLayout layout, groove_LayoutStorage **out) {
    return groove__storage__LayoutStorage__new_boxed(groove__storage__BoxedStorage__new(inner), layout, out);
}

static void layout_rc_drop(void *p) { groove__storage__LayoutStorage__drop((groove_LayoutStorage *)p); }

jz_err groove__storage__LayoutStorage__new_boxed(groove_BoxedStorage inner, groove_StorageLayout layout, groove_LayoutStorage **out) {
    groove_LayoutStorage storage = {inner, layout};
    jz_err e = layout_ensure_layout_marker(&storage);
    if (e.code) {
        groove__storage__BoxedStorage__drop(&storage.inner);
        return e;
    }
    groove_LayoutStorage *rc = (groove_LayoutStorage *)jz_rc_new(sizeof(groove_LayoutStorage), layout_rc_drop);
    *rc = storage;
    *out = rc;
    return JZ_OK;
}

static int slice_cmp_v(const void *a, const void *b, void *ctx) { return jz_slice_cmp(*(const jz_slice *)a, *(const jz_slice *)b); }
static const jz_btree_desc SLICE_SET_DESC = {sizeof(jz_slice), 0, slice_cmp_v, NULL, NULL};

jz_err groove__storage__LayoutStorage__approximate_class_bytes_for_logical_families(const groove_LayoutStorage *s, const jz_slice *logical, u32 n, bool *some, u64 *out) {
    jz_btree physical_families;
    jz_btree_init(&physical_families, &SLICE_SET_DESC);
    for (u32 i = 0; i < n; i++) {
        PhysicalCf m;
        jz_err e = storage_layout_map_cf(s->layout, logical[i], &m);
        if (e.code) {
            jz_btree_drop(&physical_families);
            return e;
        }
        jz_btree_insert(&physical_families, &m.physical_cf, NULL, NULL);
    }
    u64 bytes = 0;
    jz_slice *physical_cf;
    void *unused;
    JZ_BTREE_FOR(&physical_families, physical_cf, unused) {
        bool has = false;
        u64 class_bytes = 0;
        jz_err e = groove__storage__OrderedKvStorage__approximate_class_bytes(&s->inner.inner, *physical_cf, &has, &class_bytes);
        if (e.code) {
            jz_btree_drop(&physical_families);
            return e;
        }
        if (!has) {
            jz_btree_drop(&physical_families);
            *some = false;
            return JZ_OK;
        }
        bytes = bytes + class_bytes < bytes ? UINT64_MAX : bytes + class_bytes;
    }
    jz_btree_drop(&physical_families);
    *some = true;
    *out = bytes;
    return JZ_OK;
}

groove_BoxedStorage groove__storage__LayoutStorage__into_inner(groove_LayoutStorage *s) {
    groove_BoxedStorage b = s->inner;
    s->inner.inner.self = NULL;
    s->inner.inner.vt = NULL;
    return b;
}
void groove__storage__LayoutStorage__drop(groove_LayoutStorage *s) { groove__storage__BoxedStorage__drop(&s->inner); }

static jz_err layout_has_class_data_or_legacy_layout(const groove_LayoutStorage *s, bool *out) {
    const groove_DynStorage *inner = &s->inner.inner;
    if (storage_layout_validates_marker(s->layout)) {
        /* An unenumerable adapter cannot distinguish a fresh class layout from
         * a legacy logical-CF store; refuse rather than make that durable. */
        groove_StrVec names = {0};
        if (!groove__storage__OrderedKvStorage__column_family_names(inner, &names)) {
            return groove__storage__Error__invalid_storage_layout("class-CF v2 requires enumerable physical column families");
        }
        bool any = false;
        for (u32 i = 0; i < names.len && !any; i++) any = jazz_physical_class(jz_str_slice(&names.ptr[i])) != NULL;
        groove__storage__StrVec__drop(&names);
        if (any) {
            *out = true;
            return JZ_OK;
        }
    }
    static const char *const CFS[] = {CLASS_HISTORY_CF, CLASS_REGISTER_CF, CLASS_GLOBAL_CURRENT_CF, CLASS_AHEAD_CURRENT_CF,
                                      CLASS_CHANGES_CF, CLASS_INDICES_CF, CLASS_META_CF};
    for (u32 i = 0; i < sizeof(CFS) / sizeof(CFS[0]); i++) {
        bool some = false;
        groove_KeyValue kv;
        jz_err e = groove__storage__OrderedKvStorage__last_with_prefix(inner, cstr_slice(CFS[i]), (jz_slice){0}, &some, &kv);
        if (!e.code && some) {
            groove__storage__KeyValue__drop(&kv);
            *out = true;
            return JZ_OK;
        }
        if (e.code) {
            if (groove__storage__Error__is_column_family_not_found(&e)) {
                jz_err_drop(&e);
                continue;
            }
            return e;
        }
    }
    *out = false;
    return JZ_OK;
}

static jz_err layout_ensure_layout_marker(const groove_LayoutStorage *s) {
    if (!storage_layout_validates_marker(s->layout)) return JZ_OK;
    const groove_DynStorage *inner = &s->inner.inner;
    bool some = false;
    jz_bytes value = {0};
    JZ_TRY(groove__storage__OrderedKvStorage__get(inner, cstr_slice(CLASS_META_CF), JZ_LIT(CLASS_LAYOUT_MARKER_KEY), &some, &value));
    if (some) {
        bool ok = jz_slice_cmp(jz_bytes_slice(&value), JZ_LIT(CLASS_LAYOUT_MARKER_VALUE)) == 0;
        jz_bytes_drop(&value);
        if (ok) return JZ_OK;
        return groove__storage__Error__invalid_storage_layout("unsupported class-CF storage layout marker");
    }
    bool has = false;
    JZ_TRY(layout_has_class_data_or_legacy_layout(s, &has));
    if (has) return groove__storage__Error__invalid_storage_layout("missing class-CF storage layout marker in non-empty store");
    return groove__storage__OrderedKvStorage__set(inner, cstr_slice(CLASS_META_CF), JZ_LIT(CLASS_LAYOUT_MARKER_KEY),
                                                  jz_bytes_from((const u8 *)CLASS_LAYOUT_MARKER_VALUE, sizeof(CLASS_LAYOUT_MARKER_VALUE) - 1));
}

/* physical_key: (physical cf, framed key) */
static jz_err layout_physical_key(const groove_LayoutStorage *s, jz_slice cf, jz_slice key, jz_str *pcf, jz_bytes *pkey) {
    PhysicalCf mapping;
    JZ_TRY(storage_layout_map_cf(s->layout, cf, &mapping));
    *pcf = slice_to_str(mapping.physical_cf);
    if (!mapping.has_logical_prefix) {
        *pkey = slice_to_bytes(key);
        return JZ_OK;
    }
    jz_bytes physical_key = {0};
    u32 len = mapping.logical_prefix.len;
    u8 be[4] = {(u8)(len >> 24), (u8)(len >> 16), (u8)(len >> 8), (u8)len};
    jz_bytes_push(&physical_key, be, 4);
    jz_bytes_push(&physical_key, mapping.logical_prefix.ptr, mapping.logical_prefix.len);
    jz_bytes_push(&physical_key, key.ptr, key.len);
    *pkey = physical_key;
    return JZ_OK;
}

static jz_err layout_physical_prefix(const groove_LayoutStorage *s, jz_slice cf, jz_slice prefix, jz_str *pcf, jz_bytes *pprefix, usize *strip_len) {
    PhysicalCf mapping;
    JZ_TRY(storage_layout_map_cf(s->layout, cf, &mapping));
    *pcf = slice_to_str(mapping.physical_cf);
    if (!mapping.has_logical_prefix) {
        *pprefix = slice_to_bytes(prefix);
        *strip_len = 0;
        return JZ_OK;
    }
    jz_bytes physical_prefix = {0};
    u32 len = mapping.logical_prefix.len;
    u8 be[4] = {(u8)(len >> 24), (u8)(len >> 16), (u8)(len >> 8), (u8)len};
    jz_bytes_push(&physical_prefix, be, 4);
    jz_bytes_push(&physical_prefix, mapping.logical_prefix.ptr, mapping.logical_prefix.len);
    jz_bytes_push(&physical_prefix, prefix.ptr, prefix.len);
    *pprefix = physical_prefix;
    *strip_len = 4 + (usize)len;
    return JZ_OK;
}

/* consumes operations */
static jz_err layout_physical_operations(const groove_LayoutStorage *s, groove_OwnedWriteOperationVec operations, groove_OwnedWriteOperationVec *out) {
    groove_OwnedWriteOperationVec mapped = {0};
    jz_vec_reserve(&mapped, operations.len, sizeof(groove_OwnedWriteOperation));
    for (u32 i = 0; i < operations.len; i++) {
        groove_OwnedWriteOperation *op = &operations.ptr[i];
        groove_OwnedWriteOperation m = {op->tag, {0}, {0}, {0}};
        jz_err e = layout_physical_key(s, owned_op_cf(op), owned_op_key(op), &m.cf, &m.key);
        if (e.code) {
            groove__storage__OwnedWriteOperationVec__drop(&operations);
            groove__storage__OwnedWriteOperationVec__drop(&mapped);
            return e;
        }
        if (op->tag == GROOVE_WRITE_SET) {
            m.value = op->value;
            op->value = (jz_bytes){0};
        }
        JZ_VEC_PUSH(mapped, m);
    }
    groove__storage__OwnedWriteOperationVec__drop(&operations);
    *out = mapped;
    return JZ_OK;
}

#define LS(self) ((groove_LayoutStorage *)(self))
#define LS_INNER(self) (&LS(self)->inner.inner)

static bool layout_permits_eager_read_retry(void *self) { return groove__storage__OrderedKvStorage__permits_eager_read_retry(LS_INNER(self)); }
static jz_err layout_compare_value(void *self, jz_slice cf, jz_slice key, jz_slice expected, groove_ValueComparison *out) {
    jz_str pcf;
    jz_bytes pkey;
    JZ_TRY(layout_physical_key(LS(self), cf, key, &pcf, &pkey));
    jz_err e = groove__storage__OrderedKvStorage__compare_value(LS_INNER(self), jz_str_slice(&pcf), jz_bytes_slice(&pkey), expected, out);
    jz_str_drop(&pcf);
    jz_bytes_drop(&pkey);
    return e;
}
static jz_err layout_get(void *self, jz_slice cf, jz_slice key, bool *some, jz_bytes *out) {
    jz_str pcf;
    jz_bytes pkey;
    JZ_TRY(layout_physical_key(LS(self), cf, key, &pcf, &pkey));
    jz_err e = groove__storage__OrderedKvStorage__get(LS_INNER(self), jz_str_slice(&pcf), jz_bytes_slice(&pkey), some, out);
    jz_str_drop(&pcf);
    jz_bytes_drop(&pkey);
    return e;
}
static jz_err layout_put_if_absent(void *self, jz_slice cf, jz_slice key, jz_bytes value, bool *some, jz_bytes *existing) {
    jz_str pcf;
    jz_bytes pkey;
    jz_err e = layout_physical_key(LS(self), cf, key, &pcf, &pkey);
    if (e.code) {
        jz_bytes_drop(&value);
        return e;
    }
    e = groove__storage__OrderedKvStorage__put_if_absent(LS_INNER(self), jz_str_slice(&pcf), jz_bytes_slice(&pkey), value, some, existing);
    jz_str_drop(&pcf);
    jz_bytes_drop(&pkey);
    return e;
}
static jz_err layout_compare_and_delete(void *self, jz_slice cf, jz_slice key, jz_slice expected, bool *removed) {
    jz_str pcf;
    jz_bytes pkey;
    JZ_TRY(layout_physical_key(LS(self), cf, key, &pcf, &pkey));
    jz_err e = groove__storage__OrderedKvStorage__compare_and_delete(LS_INNER(self), jz_str_slice(&pcf), jz_bytes_slice(&pkey), expected, removed);
    jz_str_drop(&pcf);
    jz_bytes_drop(&pkey);
    return e;
}
static jz_err layout_set(void *self, jz_slice cf, jz_slice key, jz_bytes value) {
    jz_str pcf;
    jz_bytes pkey;
    jz_err e = layout_physical_key(LS(self), cf, key, &pcf, &pkey);
    if (e.code) {
        jz_bytes_drop(&value);
        return e;
    }
    e = groove__storage__OrderedKvStorage__set(LS_INNER(self), jz_str_slice(&pcf), jz_bytes_slice(&pkey), value);
    jz_str_drop(&pcf);
    jz_bytes_drop(&pkey);
    return e;
}
static jz_err layout_delete(void *self, jz_slice cf, jz_slice key) {
    jz_str pcf;
    jz_bytes pkey;
    JZ_TRY(layout_physical_key(LS(self), cf, key, &pcf, &pkey));
    jz_err e = groove__storage__OrderedKvStorage__delete(LS_INNER(self), jz_str_slice(&pcf), jz_bytes_slice(&pkey));
    jz_str_drop(&pcf);
    jz_bytes_drop(&pkey);
    return e;
}
static jz_err layout_close(void *self) { return groove__storage__OrderedKvStorage__close(LS_INNER(self)); }
static jz_err layout_set_write_flush_cadence(void *self, usize every) { return groove__storage__OrderedKvStorage__set_write_flush_cadence(LS_INNER(self), every); }
static jz_err layout_flush_write_boundary(void *self) { return groove__storage__OrderedKvStorage__flush_write_boundary(LS_INNER(self)); }

static jz_err layout_scan(void *self, groove_ScanRequest request, groove_StorageScan *out) {
    jz_str physical_cf = {0};
    groove_ScanBounds physical_bounds = {0};
    usize strip_len = 0;
    jz_err e;
    if (request.bounds.tag == GROOVE_SCAN_PREFIX) {
        e = layout_physical_prefix(LS(self), jz_str_slice(&request.cf), jz_bytes_slice(&request.bounds.start), &physical_cf,
                                   &physical_bounds.start, &strip_len);
        physical_bounds.tag = GROOVE_SCAN_PREFIX;
    } else {
        e = layout_physical_prefix(LS(self), jz_str_slice(&request.cf), jz_bytes_slice(&request.bounds.start), &physical_cf,
                                   &physical_bounds.start, &strip_len);
        if (!e.code) {
            jz_str unused_cf;
            usize unused_strip;
            e = layout_physical_prefix(LS(self), jz_str_slice(&request.cf), jz_bytes_slice(&request.bounds.end), &unused_cf,
                                       &physical_bounds.end, &unused_strip);
            if (!e.code) jz_str_drop(&unused_cf);
            else {
                jz_str_drop(&physical_cf);
                jz_bytes_drop(&physical_bounds.start);
            }
        }
        physical_bounds.tag = GROOVE_SCAN_RANGE;
    }
    groove_ScanDirection direction = request.direction;
    bool has_max = request.has_max_items;
    usize max_items = request.max_items;
    groove__storage__ScanRequest__drop(&request);
    if (e.code) return e;
    groove_ScanRequest inner_req = {physical_cf, physical_bounds, direction, has_max, max_items};
    groove_StorageScan inner;
    JZ_TRY(groove__storage__OrderedKvStorage__scan(LS_INNER(self), inner_req, &inner));
    MappedStorageCursor *c = JZ_NEW(MappedStorageCursor);
    c->inner = inner;
    c->strip_len = strip_len;
    out->self = c;
    out->vt = &MAPPED_CURSOR_VTBL;
    return JZ_OK;
}

static jz_err layout_last_with_prefix(void *self, jz_slice cf, jz_slice prefix, bool *some, groove_KeyValue *out) {
    jz_str pcf;
    jz_bytes pprefix;
    usize strip_len;
    JZ_TRY(layout_physical_prefix(LS(self), cf, prefix, &pcf, &pprefix, &strip_len));
    groove_StorageScan scan;
    JZ_TRY(groove__storage__OrderedKvStorage__scan(
        LS_INNER(self), groove__storage__ScanRequest__with_max_items(groove__storage__ScanRequest__reversed(groove__storage__ScanRequest__prefix(pcf, pprefix)), 1),
        &scan));
    groove_KeyValueVec values;
    JZ_TRY(groove__storage__collect_scan(scan, &values));
    *some = values.len > 0;
    if (*some) {
        groove_KeyValue *kv = &values.ptr[0];
        JZ_ASSERT(kv->key.len >= strip_len, "range start index out of range for slice");
        out->key = jz_bytes_from(kv->key.ptr + strip_len, kv->key.len - (u32)strip_len);
        out->value = kv->value;
        jz_bytes_drop(&kv->key);
        for (u32 i = 1; i < values.len; i++) groove__storage__KeyValue__drop(&values.ptr[i]);
    }
    jz_vec_drop(&values, sizeof(groove_KeyValue), NULL);
    return JZ_OK;
}

static jz_err layout_last_with_prefix_before_or_at(void *self, jz_slice cf, jz_slice prefix, jz_slice upper, bool *some, groove_KeyValue *out) {
    jz_str pcf;
    jz_bytes pprefix;
    usize strip_len;
    JZ_TRY(layout_physical_prefix(LS(self), cf, prefix, &pcf, &pprefix, &strip_len));
    jz_str ucf;
    jz_bytes physical_upper;
    usize ustrip;
    jz_err e = layout_physical_prefix(LS(self), cf, upper, &ucf, &physical_upper, &ustrip);
    if (e.code) {
        jz_str_drop(&pcf);
        jz_bytes_drop(&pprefix);
        return e;
    }
    jz_str_drop(&ucf);
    groove_StorageScan scan;
    e = groove__storage__OrderedKvStorage__scan(LS_INNER(self), groove__storage__ScanRequest__prefix(pcf, pprefix), &scan);
    groove_KeyValueVec values = {0};
    if (!e.code) e = groove__storage__collect_scan(scan, &values);
    if (e.code) {
        jz_bytes_drop(&physical_upper);
        return e;
    }
    /* .rfind(|(key, _)| key <= &physical_upper) */
    i64 found = -1;
    for (i64 i = (i64)values.len - 1; i >= 0; i--) {
        if (jz_bytes_cmp(&values.ptr[i].key, &physical_upper) <= 0) {
            found = i;
            break;
        }
    }
    *some = found >= 0;
    for (u32 i = 0; i < values.len; i++) {
        groove_KeyValue *kv = &values.ptr[i];
        if ((i64)i == found) {
            JZ_ASSERT(kv->key.len >= strip_len, "range start index out of range for slice");
            out->key = jz_bytes_from(kv->key.ptr + strip_len, kv->key.len - (u32)strip_len);
            out->value = kv->value;
            jz_bytes_drop(&kv->key);
        } else {
            groove__storage__KeyValue__drop(kv);
        }
    }
    jz_vec_drop(&values, sizeof(groove_KeyValue), NULL);
    jz_bytes_drop(&physical_upper);
    return JZ_OK;
}

static jz_err layout_write_many(void *self, groove_OwnedWriteOperationVec operations) {
    groove_OwnedWriteOperationVec physical;
    JZ_TRY(layout_physical_operations(LS(self), operations, &physical));
    return groove__storage__OrderedKvStorage__write_many(LS_INNER(self), physical);
}
static groove_WriteManyOutcome layout_write_many_outcome(void *self, groove_OwnedWriteOperationVec operations) {
    groove_OwnedWriteOperationVec physical;
    jz_err e = layout_physical_operations(LS(self), operations, &physical);
    if (e.code) return wmo(GROOVE_WMO_UNCOMMITTED, e);
    return groove__storage__OrderedKvStorage__write_many_outcome(LS_INNER(self), physical);
}
typedef struct MappedKey { jz_str cf; jz_bytes key; } MappedKey;
static groove_WriteManyOutcome layout_write_many_borrowed_outcome(void *self, groove_WriteOperationVec operations) {
    /* Complete all fallible mapping before entering the backend. */
    JZ_VEC(MappedKey) mapped = {0};
    jz_vec_reserve(&mapped, operations.len, sizeof(MappedKey));
    for (u32 i = 0; i < operations.len; i++) {
        MappedKey m;
        jz_err e = layout_physical_key(LS(self), operations.ptr[i].cf, operations.ptr[i].key, &m.cf, &m.key);
        if (e.code) {
            for (u32 j = 0; j < mapped.len; j++) {
                jz_str_drop(&mapped.ptr[j].cf);
                jz_bytes_drop(&mapped.ptr[j].key);
            }
            jz_vec_drop(&mapped, sizeof(MappedKey), NULL);
            jz_vec_drop(&operations, sizeof(groove_WriteOperation), NULL);
            return wmo(GROOVE_WMO_UNCOMMITTED, e);
        }
        JZ_VEC_PUSH(mapped, m);
    }
    groove_WriteOperationVec physical = {0};
    jz_vec_reserve(&physical, operations.len, sizeof(groove_WriteOperation));
    for (u32 i = 0; i < operations.len; i++) {
        groove_WriteOperation *op = &operations.ptr[i];
        jz_slice cf = jz_str_slice(&mapped.ptr[i].cf), key = jz_bytes_slice(&mapped.ptr[i].key);
        if (op->tag == GROOVE_WRITE_SET) JZ_VEC_PUSH(physical, groove__storage__WriteOperation__set(cf, key, op->value));
        else JZ_VEC_PUSH(physical, groove__storage__WriteOperation__delete(cf, key));
    }
    groove_WriteManyOutcome outcome = groove__storage__OrderedKvStorage__write_many_borrowed_outcome(LS_INNER(self), physical);
    for (u32 j = 0; j < mapped.len; j++) {
        jz_str_drop(&mapped.ptr[j].cf);
        jz_bytes_drop(&mapped.ptr[j].key);
    }
    jz_vec_drop(&mapped, sizeof(MappedKey), NULL);
    jz_vec_drop(&operations, sizeof(groove_WriteOperation), NULL);
    return outcome;
}
static bool layout_column_family_names(void *self, groove_StrVec *out) { return groove__storage__OrderedKvStorage__column_family_names(LS_INNER(self), out); }
static jz_err layout_approximate_class_bytes(void *self, jz_slice cf, bool *some, u64 *out) {
    PhysicalCf m;
    JZ_TRY(storage_layout_map_cf(LS(self)->layout, cf, &m));
    return groove__storage__OrderedKvStorage__approximate_class_bytes(LS_INNER(self), m.physical_cf, some, out);
}
/* Box<LayoutStorage> / Rc<LayoutStorage> as dyn: release the rc payload */
static void layout_drop_boxed(void *self) { jz_rc_release(self); }

const groove_OrderedKvStorageVtbl groove__storage__LayoutStorage__OrderedKvStorage__VTBL = {
    .permits_eager_read_retry = layout_permits_eager_read_retry,
    .get = layout_get,
    .compare_value = layout_compare_value,
    .put_if_absent = layout_put_if_absent,
    .compare_and_delete = layout_compare_and_delete,
    .set = layout_set,
    .delete_ = layout_delete,
    .close = layout_close,
    .set_write_flush_cadence = layout_set_write_flush_cadence,
    .flush_write_boundary = layout_flush_write_boundary,
    .approximate_class_bytes = layout_approximate_class_bytes,
    .scan = layout_scan,
    .last_with_prefix = layout_last_with_prefix,
    .last_with_prefix_before_or_at = layout_last_with_prefix_before_or_at,
    .write_many = layout_write_many,
    .write_many_outcome = layout_write_many_outcome,
    .write_many_borrowed_outcome = layout_write_many_borrowed_outcome,
    .column_family_names = layout_column_family_names,
    .range = NULL,
    .prefix = NULL,
    .reopen = NULL,
    .drop = layout_drop_boxed,
};
/* Borrowed view: drop is not invoked through this handle by borrowers. */
groove_DynStorage groove__storage__LayoutStorage__as_dyn(const groove_LayoutStorage *s) {
    groove_DynStorage d = {(void *)s, &groove__storage__LayoutStorage__OrderedKvStorage__VTBL};
    return d;
}

/* ======================================================================
 * RecordStore
 * ====================================================================== */
groove_RecordStore groove__storage__RecordStore__new(const groove_DynStorage *storage, jz_slice column_family, const groove_RecordDescriptor *descriptor) {
    groove_RecordStore r = {storage, column_family, descriptor};
    return r;
}
const groove_RecordDescriptor *groove__storage__RecordStore__descriptor(const groove_RecordStore *r) { return r->descriptor; }
jz_slice groove__storage__RecordStore__column_family(const groove_RecordStore *r) { return r->column_family; }
jz_err groove__storage__RecordStore__get_raw(const groove_RecordStore *r, jz_slice key, bool *some, jz_bytes *out) {
    return groove__storage__OrderedKvStorage__get(r->storage, r->column_family, key, some, out);
}
jz_err groove__storage__RecordStore__get(const groove_RecordStore *r, jz_slice key, bool *some, groove_Record *out) {
    jz_bytes raw;
    JZ_TRY(groove__storage__RecordStore__get_raw(r, key, some, &raw));
    if (*some) *out = groove__records__RecordDescriptor__bind_owned(r->descriptor, raw);
    return JZ_OK;
}
jz_err groove__storage__RecordStore__range(const groove_RecordStore *r, jz_slice start, jz_slice end, groove_KeyValueVec *out) {
    return groove__storage__OrderedKvStorage__range(r->storage, r->column_family, start, end, out);
}
jz_err groove__storage__RecordStore__prefix(const groove_RecordStore *r, jz_slice prefix, groove_KeyValueVec *out) {
    return groove__storage__OrderedKvStorage__prefix(r->storage, r->column_family, prefix, out);
}
jz_err groove__storage__RecordStore__range_reverse(const groove_RecordStore *r, jz_slice start, jz_slice end, groove_KeyValueVec *out) {
    groove_KeyValueVec records;
    JZ_TRY(groove__storage__RecordStore__range(r, start, end, &records));
    for (u32 i = 0, j = records.len ? records.len - 1 : 0; i < j; i++, j--) {
        groove_KeyValue t = records.ptr[i];
        records.ptr[i] = records.ptr[j];
        records.ptr[j] = t;
    }
    *out = records;
    return JZ_OK;
}
jz_err groove__storage__RecordStore__scan(const groove_RecordStore *r, groove_ScanBounds bounds, groove_StorageScan *out) {
    groove_ScanRequest req = {slice_to_str(r->column_family), bounds, GROOVE_SCAN_FORWARD, false, 0};
    return groove__storage__OrderedKvStorage__scan(r->storage, req, out);
}
jz_err groove__storage__RecordStore__last_with_prefix(const groove_RecordStore *r, jz_slice prefix, bool *some, groove_KeyValue *out) {
    return groove__storage__OrderedKvStorage__last_with_prefix(r->storage, r->column_family, prefix, some, out);
}
jz_err groove__storage__RecordStore__last_with_prefix_before_or_at(const groove_RecordStore *r, jz_slice prefix, jz_slice upper, bool *some, groove_KeyValue *out) {
    return groove__storage__OrderedKvStorage__last_with_prefix_before_or_at(r->storage, r->column_family, prefix, upper, some, out);
}
jz_err groove__storage__RecordStore__put_if_absent(const groove_RecordStore *r, jz_slice key, jz_slice record, bool *some, groove_Record *out) {
    jz_bytes existing;
    JZ_TRY(groove__storage__OrderedKvStorage__put_if_absent(r->storage, r->column_family, key, slice_to_bytes(record), some, &existing));
    if (*some) *out = groove__records__RecordDescriptor__bind_owned(r->descriptor, existing);
    return JZ_OK;
}
groove_OwnedWriteOperation groove__storage__RecordStore__set(const groove_RecordStore *r, jz_slice key, jz_slice record) {
    groove_OwnedWriteOperation op = {GROOVE_WRITE_SET, slice_to_str(r->column_family), slice_to_bytes(key), slice_to_bytes(record)};
    return op;
}
groove_OwnedWriteOperation groove__storage__RecordStore__delete(const groove_RecordStore *r, jz_slice key) {
    groove_OwnedWriteOperation op = {GROOVE_WRITE_DELETE, slice_to_str(r->column_family), slice_to_bytes(key), {0}};
    return op;
}
jz_err groove__storage__RecordStore__write_many(const groove_RecordStore *r, groove_OwnedWriteOperationVec operations) {
    return groove__storage__OrderedKvStorage__write_many(r->storage, operations);
}

/* ======================================================================
 * StagedWriteState
 * ====================================================================== */
#define STAGED_POINT_READS_BEFORE_INDEX 16u
#define STAGED_OPS_BEFORE_POINT_INDEX 64u

static void btree_drop_v(void *p) { jz_btree_drop((jz_btree *)p); }
static const jz_btree_desc KEY_INDEX_DESC = {sizeof(jz_bytes), sizeof(usize), jz_cmp_bytes, bytes_drop_v, NULL};
static const jz_btree_desc CF_INDEX_DESC = {sizeof(jz_str), sizeof(jz_btree), jz_cmp_str, str_drop_v, btree_drop_v};

static void op_block_drop(void *p) {
    groove_OpBlock *b = (groove_OpBlock *)p;
    groove__storage__OwnedWriteOperationVec__drop(&b->ops);
}
static void op_block_release_v(void *p) { jz_rc_release(*(groove_OpBlock **)p); }
static void op_block_clone_v(void *d, const void *s) { *(groove_OpBlock **)d = (groove_OpBlock *)jz_rc_clone(*(groove_OpBlock *const *)s); }
void groove__storage__OpBlockVec__drop(groove_OpBlockVec *v) { jz_vec_drop(v, sizeof(groove_OpBlock *), op_block_release_v); }

void groove__storage__StagedWriteState__Default__default(groove_StagedWriteState *out) {
    memset(out, 0, sizeof(*out));
    out->has_latest_by_cf_key = false;
}
void groove__storage__StagedWriteState__drop(groove_StagedWriteState *s) {
    groove__storage__OpBlockVec__drop(&s->operations);
    jz_vec_drop(&s->operation_ends, sizeof(usize), NULL);
    if (s->has_latest_by_cf_key) jz_btree_drop(&s->latest_by_cf_key);
    s->has_latest_by_cf_key = false;
}
static void key_index_clone_v(void *d, const void *s) { jz_btree_clone((jz_btree *)d, (const jz_btree *)s, bytes_clone_v, NULL); }
void groove__storage__StagedWriteState__Clone__clone(groove_StagedWriteState *dst, const groove_StagedWriteState *src) {
    memset(dst, 0, sizeof(*dst));
    jz_vec_clone(&dst->operations, &src->operations, sizeof(groove_OpBlock *), op_block_clone_v);
    jz_vec_clone(&dst->operation_ends, &src->operation_ends, sizeof(usize), NULL);
    dst->has_latest_by_cf_key = src->has_latest_by_cf_key;
    if (src->has_latest_by_cf_key) jz_btree_clone(&dst->latest_by_cf_key, &src->latest_by_cf_key, str_clone_v, key_index_clone_v);
    dst->point_reads_without_index = src->point_reads_without_index;
}
bool groove__storage__StagedWriteState__is_empty(const groove_StagedWriteState *s) { return s->operations.len == 0; }
static usize staged_operation_count(const groove_StagedWriteState *s) {
    return s->operation_ends.len ? s->operation_ends.ptr[s->operation_ends.len - 1] : 0;
}
const groove_OwnedWriteOperation *groove__storage__StagedWriteState__operation(const groove_StagedWriteState *s, usize index) {
    /* partition_point(|end| *end <= index) */
    u32 lo = 0, hi = s->operation_ends.len;
    while (lo < hi) {
        u32 mid = lo + (hi - lo) / 2;
        if (s->operation_ends.ptr[mid] <= index) lo = mid + 1;
        else hi = mid;
    }
    u32 block = lo;
    usize start = block == 0 ? 0 : s->operation_ends.ptr[block - 1];
    JZ_ASSERT(block < s->operations.len, "index out of bounds");
    groove_OpBlock *b = s->operations.ptr[block];
    JZ_ASSERT(index - start < b->ops.len, "index out of bounds");
    return &b->ops.ptr[index - start];
}

/* latest_by_cf_key.entry(cf).or_default().insert(key, index) */
static void key_index_insert(jz_btree *by_cf, jz_slice cf, jz_slice key, usize index) {
    jz_str cf_view = str_view(cf);
    jz_btree *by_key = (jz_btree *)jz_btree_get(by_cf, &cf_view);
    if (!by_key) {
        jz_str owned = slice_to_str(cf);
        jz_btree fresh;
        jz_btree_init(&fresh, &KEY_INDEX_DESC);
        jz_btree_insert(by_cf, &owned, &fresh, NULL);
        by_key = (jz_btree *)jz_btree_get(by_cf, &cf_view);
    }
    jz_bytes key_view = bytes_view(key);
    usize *slot = (usize *)jz_btree_get(by_key, &key_view);
    if (slot) {
        *slot = index;
    } else {
        jz_bytes owned = slice_to_bytes(key);
        jz_btree_insert(by_key, &owned, &index, NULL);
    }
}

void groove__storage__StagedWriteState__stage(groove_StagedWriteState *s, groove_OwnedWriteOperation operation) {
    usize index = staged_operation_count(s);
    if (s->has_latest_by_cf_key) key_index_insert(&s->latest_by_cf_key, owned_op_cf(&operation), owned_op_key(&operation), index);
    groove_OpBlock *tail = s->operations.len ? s->operations.ptr[s->operations.len - 1] : NULL;
    if (tail && jz_rc_strong(tail) == 1) { /* Rc::get_mut */
        jz_vec_push(&tail->ops, &operation, sizeof(groove_OwnedWriteOperation));
        JZ_ASSERT(s->operation_ends.len > 0, "tail end");
        s->operation_ends.ptr[s->operation_ends.len - 1] = index + 1;
    } else {
        groove_OpBlock *block = (groove_OpBlock *)jz_rc_new(sizeof(groove_OpBlock), op_block_drop);
        jz_vec_push(&block->ops, &operation, sizeof(groove_OwnedWriteOperation));
        JZ_VEC_PUSH(s->operations, block);
        usize end = index + 1;
        JZ_VEC_PUSH(s->operation_ends, end);
    }
}

void groove__storage__StagedWriteState__extend(groove_StagedWriteState *s, groove_OwnedWriteOperationVec operations) {
    for (u32 i = 0; i < operations.len; i++) groove__storage__StagedWriteState__stage(s, operations.ptr[i]);
    jz_vec_drop(&operations, sizeof(groove_OwnedWriteOperation), NULL);
}

groove_OpBlockVec groove__storage__StagedWriteState__snapshot(const groove_StagedWriteState *s) {
    groove_OpBlockVec v = {0};
    jz_vec_clone(&v, &s->operations, sizeof(groove_OpBlock *), op_block_clone_v);
    return v;
}

void groove__storage__StagedWriteState__extend_shared(groove_StagedWriteState *s, groove_OpBlockVec snapshot) {
    for (u32 i = 0; i < snapshot.len; i++) {
        groove_OpBlock *block = snapshot.ptr[i];
        if (block->ops.len == 0) {
            jz_rc_release(block);
            continue;
        }
        usize start = staged_operation_count(s);
        if (s->has_latest_by_cf_key) {
            for (u32 offset = 0; offset < block->ops.len; offset++) {
                const groove_OwnedWriteOperation *op = &block->ops.ptr[offset];
                key_index_insert(&s->latest_by_cf_key, owned_op_cf(op), owned_op_key(op), start + offset);
            }
        }
        usize end = start + block->ops.len;
        JZ_VEC_PUSH(s->operation_ends, end);
        JZ_VEC_PUSH(s->operations, block);
    }
    jz_vec_drop(&snapshot, sizeof(groove_OpBlock *), NULL);
}

groove_OwnedWriteOperationVec groove__storage__StagedWriteState__into_operations(groove_StagedWriteState *s) {
    groove_OwnedWriteOperationVec out = {0};
    for (u32 i = 0; i < s->operations.len; i++) {
        groove_OpBlock *block = s->operations.ptr[i];
        if (jz_rc_strong(block) == 1) { /* Rc::try_unwrap */
            jz_vec_extend(&out, block->ops.ptr, block->ops.len, sizeof(groove_OwnedWriteOperation));
            jz_vec_drop(&block->ops, sizeof(groove_OwnedWriteOperation), NULL);
            block->ops = (groove_OwnedWriteOperationVec){0};
        } else {
            for (u32 j = 0; j < block->ops.len; j++) {
                groove_OwnedWriteOperation c;
                groove__storage__OwnedWriteOperation__clone(&c, &block->ops.ptr[j]);
                JZ_VEC_PUSH(out, c);
            }
        }
    }
    groove__storage__StagedWriteState__drop(s);
    groove__storage__StagedWriteState__Default__default(s);
    return out;
}

void groove__storage__StagedWriteState__ensure_key_index(groove_StagedWriteState *s) {
    if (s->has_latest_by_cf_key) return;
    jz_btree latest_by_cf_key;
    jz_btree_init(&latest_by_cf_key, &CF_INDEX_DESC);
    usize index = 0;
    for (u32 b = 0; b < s->operations.len; b++) {
        groove_OpBlock *block = s->operations.ptr[b];
        for (u32 i = 0; i < block->ops.len; i++, index++)
            key_index_insert(&latest_by_cf_key, owned_op_cf(&block->ops.ptr[i]), owned_op_key(&block->ops.ptr[i]), index);
    }
    s->latest_by_cf_key = latest_by_cf_key;
    s->has_latest_by_cf_key = true;
}

bool groove__storage__StagedWriteState__latest_index(groove_StagedWriteState *s, jz_slice cf, jz_slice key, usize *out) {
    if (!s->has_latest_by_cf_key) {
        usize count = staged_operation_count(s);
        if (count < STAGED_OPS_BEFORE_POINT_INDEX && s->point_reads_without_index < STAGED_POINT_READS_BEFORE_INDEX) {
            s->point_reads_without_index += 1;
            /* operations().rev().position(..) */
            for (usize i = count; i-- > 0;) {
                const groove_OwnedWriteOperation *op = groove__storage__StagedWriteState__operation(s, i);
                if (slice_eq(owned_op_cf(op), cf) && slice_eq(owned_op_key(op), key)) {
                    *out = i;
                    return true;
                }
            }
            return false;
        }
        groove__storage__StagedWriteState__ensure_key_index(s);
    }
    jz_str cf_view = str_view(cf);
    jz_btree *by_key = (jz_btree *)jz_btree_get(&s->latest_by_cf_key, &cf_view);
    if (!by_key) return false;
    jz_bytes key_view = bytes_view(key);
    usize *idx = (usize *)jz_btree_get(by_key, &key_view);
    if (!idx) return false;
    *out = *idx;
    return true;
}

void groove__storage__StagedWriteState__scan_snapshot(groove_StagedWriteState *s, jz_slice cf, const groove_ScanBounds *bounds, jz_deque *out) {
    jz_deque_init(out, sizeof(groove_StagedEntry));
    if (s->operations.len == 0 || groove__storage__ScanBounds__is_empty_range(bounds)) return;
    groove__storage__StagedWriteState__ensure_key_index(s);
    jz_str cf_view = str_view(cf);
    jz_btree *by_key = (jz_btree *)jz_btree_get(&s->latest_by_cf_key, &cf_view);
    if (!by_key) return;
    jz_bytes end = {0};
    bool has_end;
    if (bounds->tag == GROOVE_SCAN_PREFIX) {
        has_end = groove__storage__prefix_successor(jz_bytes_slice(&bounds->start), &end);
    } else {
        end = jz_bytes_clone(&bounds->end);
        has_end = true;
    }
    jz_btree_iter it;
    jz_btree_range(&it, by_key, &bounds->start, true, has_end ? &end : NULL, false);
    jz_bytes *key;
    usize *index;
    while (jz_btree_next(&it, (void **)&key, (void **)&index)) {
        const groove_OwnedWriteOperation *op = groove__storage__StagedWriteState__operation(s, *index);
        groove_StagedEntry entry = {jz_bytes_clone(key), false, {0}};
        if (op->tag == GROOVE_WRITE_SET) {
            entry.has_value = true;
            entry.value = jz_bytes_clone(&op->value);
        }
        jz_deque_push_back(out, &entry);
    }
    jz_bytes_drop(&end);
}

bool groove__storage__StagedWriteState__contains_key(groove_StagedWriteState *s, jz_slice cf, jz_slice key) {
    usize idx;
    return groove__storage__StagedWriteState__latest_index(s, cf, key, &idx);
}

void groove__storage__StagedWriteState__From__from(groove_OwnedWriteOperationVec operations, groove_StagedWriteState *out) {
    groove__storage__StagedWriteState__Default__default(out);
    groove__storage__StagedWriteState__extend(out, operations);
}

/* ======================================================================
 * OverlayScanCursor
 * ====================================================================== */
static void staged_entry_drop(void *p) {
    groove_StagedEntry *e = (groove_StagedEntry *)p;
    jz_bytes_drop(&e->key);
    jz_bytes_drop(&e->value);
}

typedef struct OverlayScanCursor {
    groove_StorageScan base;
    jz_deque staged;       /* groove_StagedEntry */
    jz_deque base_entries; /* groove_KeyValue */
    bool base_done;
    groove_ScanDirection direction;
    bool has_remaining;
    usize remaining;
} OverlayScanCursor;

static jz_err overlay_cursor_next_entry(OverlayScanCursor *c, bool *some, groove_KeyValue *out) {
    for (;;) {
        while (c->base_entries.len == 0 && !c->base_done) {
            bool has = false;
            groove_KeyValueVec entries = {0};
            JZ_TRY(groove__storage__StorageCursor__next_batch(&c->base, &has, &entries));
            if (has) {
                for (u32 i = 0; i < entries.len; i++) jz_deque_push_back(&c->base_entries, &entries.ptr[i]);
                jz_vec_drop(&entries, sizeof(groove_KeyValue), NULL);
            } else {
                c->base_done = true;
            }
        }
        groove_KeyValue *base_front = c->base_entries.len ? (groove_KeyValue *)jz_deque_at(&c->base_entries, 0) : NULL;
        groove_StagedEntry *staged_front = c->staged.len ? (groove_StagedEntry *)jz_deque_at(&c->staged, 0) : NULL;
        bool take_base, take_staged;
        if (!base_front && !staged_front) {
            *some = false;
            return JZ_OK;
        } else if (base_front && !staged_front) {
            take_base = true;
            take_staged = false;
        } else if (!base_front && staged_front) {
            take_base = false;
            take_staged = true;
        } else if (jz_bytes_eq(&base_front->key, &staged_front->key)) {
            take_base = true;
            take_staged = true;
        } else {
            int cmp = jz_bytes_cmp(&staged_front->key, &base_front->key);
            bool staged_first = c->direction == GROOVE_SCAN_FORWARD ? cmp < 0 : cmp > 0;
            take_base = !staged_first;
            take_staged = staged_first;
        }
        groove_KeyValue base_entry;
        bool has_base = take_base && jz_deque_pop_front(&c->base_entries, &base_entry);
        groove_StagedEntry staged_entry;
        bool has_staged = take_staged && jz_deque_pop_front(&c->staged, &staged_entry);
        if (has_staged) {
            if (has_base) groove__storage__KeyValue__drop(&base_entry);
            if (staged_entry.has_value) {
                out->key = staged_entry.key;
                out->value = staged_entry.value;
                *some = true;
                return JZ_OK;
            }
            staged_entry_drop(&staged_entry);
        } else if (has_base) {
            *out = base_entry;
            *some = true;
            return JZ_OK;
        }
    }
}

static jz_err overlay_cursor_next_batch(void *self, bool *some, groove_KeyValueVec *out) {
    OverlayScanCursor *c = (OverlayScanCursor *)self;
    usize batch_len;
    if (!c->has_remaining) {
        batch_len = 256;
    } else if (c->remaining > 0) {
        batch_len = c->remaining < 256 ? c->remaining : 256;
    } else {
        *some = false;
        return JZ_OK;
    }
    /* Short and empty scans should not reserve a full page. */
    groove_KeyValueVec values = {0};
    while (values.len < batch_len) {
        bool has = false;
        groove_KeyValue entry;
        jz_err e = overlay_cursor_next_entry(c, &has, &entry);
        if (e.code) {
            groove__storage__KeyValueVec__drop(&values);
            return e;
        }
        if (!has) break;
        JZ_VEC_PUSH(values, entry);
        if (c->has_remaining) c->remaining -= 1;
    }
    *some = values.len != 0;
    if (*some) *out = values;
    else jz_vec_drop(&values, sizeof(groove_KeyValue), NULL);
    return JZ_OK;
}
static void overlay_cursor_drop(void *self) {
    OverlayScanCursor *c = (OverlayScanCursor *)self;
    groove__storage__StorageScan__drop(&c->base);
    jz_deque_drop(&c->staged, staged_entry_drop);
    jz_deque_drop(&c->base_entries, groove__storage__KeyValue__drop);
    jz_free(c);
}
static const groove_StorageCursorVtbl OVERLAY_CURSOR_VTBL = {overlay_cursor_next_batch, overlay_cursor_drop};

static jz_err overlay_scan(const groove_DynStorage *base, groove_StagedWriteState *staged_writes, groove_ScanRequest request, groove_StorageScan *out) {
    /* Capture only the final value per key at scan creation. */
    jz_deque staged;
    groove__storage__StagedWriteState__scan_snapshot(staged_writes, jz_str_slice(&request.cf), &request.bounds, &staged);
    /* `limit + removals` is a hard physical ceiling and enough base entries
     * to fill the requested logical result when they exist. */
    bool has_physical_max = request.has_max_items;
    usize physical_max_items = 0;
    if (request.has_max_items) {
        usize final_removals = 0;
        for (u32 i = 0; i < staged.len; i++)
            if (!((groove_StagedEntry *)jz_deque_at(&staged, i))->has_value) final_removals++;
        physical_max_items = request.max_items + final_removals < request.max_items ? SIZE_MAX : request.max_items + final_removals;
    }
    groove_ScanRequest base_req;
    groove__storage__ScanRequest__Clone__clone(&base_req, &request);
    base_req.has_max_items = has_physical_max;
    base_req.max_items = physical_max_items;
    groove_StorageScan base_scan;
    jz_err e = groove__storage__OrderedKvStorage__scan(base, base_req, &base_scan);
    if (e.code) {
        jz_deque_drop(&staged, staged_entry_drop);
        groove__storage__ScanRequest__drop(&request);
        return e;
    }
    if (request.direction == GROOVE_SCAN_REVERSE) {
        /* staged.make_contiguous().reverse() */
        jz_deque reversed;
        jz_deque_init(&reversed, sizeof(groove_StagedEntry));
        groove_StagedEntry entry;
        while (jz_deque_pop_back(&staged, &entry)) jz_deque_push_back(&reversed, &entry);
        jz_deque_drop(&staged, NULL);
        staged = reversed;
    }
    OverlayScanCursor *c = JZ_NEW(OverlayScanCursor);
    c->base = base_scan;
    c->staged = staged;
    jz_deque_init(&c->base_entries, sizeof(groove_KeyValue));
    c->base_done = false;
    c->direction = request.direction;
    c->has_remaining = request.has_max_items;
    c->remaining = request.max_items;
    groove__storage__ScanRequest__drop(&request);
    out->self = c;
    out->vt = &OVERLAY_CURSOR_VTBL;
    return JZ_OK;
}

/* ======================================================================
 * StagedWriteOverlay
 * ====================================================================== */
groove_StagedWriteOverlay groove__storage__StagedWriteOverlay__new(groove_DynStorage base, groove_StagedWriteState *staged_writes) {
    groove_StagedWriteOverlay o = {base, NULL, staged_writes, false};
    return o;
}
static void staged_state_rc_drop(void *p) { groove__storage__StagedWriteState__drop((groove_StagedWriteState *)p); }
groove_StagedWriteOverlay groove__storage__StagedWriteOverlay__new_owned(groove_DynStorage base, void *base_rc, groove_StagedWriteState *staged_writes_rc) {
    groove_StagedWriteOverlay o = {base, base_rc, staged_writes_rc, true};
    return o;
}
void groove__storage__StagedWriteOverlay__drop(groove_StagedWriteOverlay *o) {
    if (o->base_rc) jz_rc_release(o->base_rc);
    if (o->staged_owned) jz_rc_release(o->staged_writes);
    o->base_rc = NULL;
    o->staged_writes = NULL;
}

groove_StagedPointValue groove__storage__StagedWriteOverlay__staged_point_value(const groove_StagedWriteOverlay *o, jz_slice cf, jz_slice key) {
    groove_StagedPointValue v = {GROOVE_STAGED_MISS, {0}};
    groove_StagedWriteState *staged_writes = o->staged_writes;
    if (groove__storage__StagedWriteState__is_empty(staged_writes)) return v;
    usize index;
    if (!groove__storage__StagedWriteState__latest_index(staged_writes, cf, key, &index)) return v;
    const groove_OwnedWriteOperation *op = groove__storage__StagedWriteState__operation(staged_writes, index);
    if (op->tag == GROOVE_WRITE_SET) {
        v.tag = GROOVE_STAGED_SET;
        v.value = jz_bytes_clone(&op->value);
    } else {
        v.tag = GROOVE_STAGED_DELETE;
    }
    return v;
}
void groove__storage__StagedWriteOverlay__stage(const groove_StagedWriteOverlay *o, groove_OwnedWriteOperation operation) {
    groove__storage__StagedWriteState__stage(o->staged_writes, operation);
}
void groove__storage__StagedWriteOverlay__drain_into(const groove_StagedWriteOverlay *o, groove_OwnedWriteOperationVec *target) {
    /* std::mem::take */
    groove_StagedWriteState state = *o->staged_writes;
    groove__storage__StagedWriteState__Default__default(o->staged_writes);
    groove_OwnedWriteOperationVec ops = groove__storage__StagedWriteState__into_operations(&state);
    jz_vec_extend(target, ops.ptr, ops.len, sizeof(groove_OwnedWriteOperation));
    jz_vec_drop(&ops, sizeof(groove_OwnedWriteOperation), NULL);
}

#define OV(self) ((groove_StagedWriteOverlay *)(self))

static jz_err overlay_compare_value(void *self, jz_slice cf, jz_slice key, jz_slice expected, groove_ValueComparison *out) {
    groove_StagedWriteState *staged = OV(self)->staged_writes;
    usize index;
    if (groove__storage__StagedWriteState__latest_index(staged, cf, key, &index)) {
        const groove_OwnedWriteOperation *op = groove__storage__StagedWriteState__operation(staged, index);
        if (op->tag == GROOVE_WRITE_DELETE) *out = GROOVE_VALUE_ABSENT;
        else if (jz_slice_cmp(jz_bytes_slice(&op->value), expected) == 0) *out = GROOVE_VALUE_IDENTICAL;
        else *out = GROOVE_VALUE_DIFFERENT;
        return JZ_OK;
    }
    return groove__storage__OrderedKvStorage__compare_value(&OV(self)->base, cf, key, expected, out);
}
static bool overlay_permits_eager_read_retry(void *self) { return groove__storage__OrderedKvStorage__permits_eager_read_retry(&OV(self)->base); }
static jz_err txn_put_if_absent(void *self, jz_slice cf, jz_slice key, jz_bytes value, bool *some, jz_bytes *existing) {
    jz_bytes_drop(&value);
    return groove__storage__Error__conditional_mutation_in_transaction();
}
static jz_err txn_compare_and_delete(void *self, jz_slice cf, jz_slice key, jz_slice expected, bool *removed) {
    return groove__storage__Error__conditional_mutation_in_transaction();
}
static jz_err overlay_scan_m(void *self, groove_ScanRequest request, groove_StorageScan *out) {
    return overlay_scan(&OV(self)->base, OV(self)->staged_writes, request, out);
}
static jz_err overlay_get(void *self, jz_slice cf, jz_slice key, bool *some, jz_bytes *out) {
    groove_StagedPointValue v = groove__storage__StagedWriteOverlay__staged_point_value(OV(self), cf, key);
    switch (v.tag) {
    case GROOVE_STAGED_MISS: return groove__storage__OrderedKvStorage__get(&OV(self)->base, cf, key, some, out);
    case GROOVE_STAGED_SET:
        *some = true;
        *out = v.value;
        return JZ_OK;
    default: *some = false; return JZ_OK;
    }
}
static jz_err overlay_set(void *self, jz_slice cf, jz_slice key, jz_bytes value) {
    groove_OwnedWriteOperation op = {GROOVE_WRITE_SET, slice_to_str(cf), slice_to_bytes(key), value};
    groove__storage__StagedWriteOverlay__stage(OV(self), op);
    return JZ_OK;
}
static jz_err overlay_delete(void *self, jz_slice cf, jz_slice key) {
    groove_OwnedWriteOperation op = {GROOVE_WRITE_DELETE, slice_to_str(cf), slice_to_bytes(key), {0}};
    groove__storage__StagedWriteOverlay__stage(OV(self), op);
    return JZ_OK;
}
static jz_err overlay_write_many(void *self, groove_OwnedWriteOperationVec operations) {
    groove__storage__StagedWriteState__extend(OV(self)->staged_writes, operations);
    return JZ_OK;
}
static jz_err overlay_approximate_class_bytes(void *self, jz_slice cf, bool *some, u64 *out) {
    return groove__storage__OrderedKvStorage__approximate_class_bytes(&OV(self)->base, cf, some, out);
}
static bool overlay_column_family_names(void *self, groove_StrVec *out) { return groove__storage__OrderedKvStorage__column_family_names(&OV(self)->base, out); }
static void overlay_drop_boxed(void *self) { groove__storage__StagedWriteOverlay__drop(OV(self)); }

const groove_OrderedKvStorageVtbl groove__storage__StagedWriteOverlay__OrderedKvStorage__VTBL = {
    .permits_eager_read_retry = overlay_permits_eager_read_retry,
    .get = overlay_get,
    .compare_value = overlay_compare_value,
    .put_if_absent = txn_put_if_absent,
    .compare_and_delete = txn_compare_and_delete,
    .set = overlay_set,
    .delete_ = overlay_delete,
    .scan = overlay_scan_m,
    .write_many = overlay_write_many,
    .approximate_class_bytes = overlay_approximate_class_bytes,
    .column_family_names = overlay_column_family_names,
    .drop = overlay_drop_boxed,
};
groove_DynStorage groove__storage__StagedWriteOverlay__as_dyn(groove_StagedWriteOverlay *o) {
    groove_DynStorage d = {o, &groove__storage__StagedWriteOverlay__OrderedKvStorage__VTBL};
    return d;
}

/* ======================================================================
 * StorageTransaction
 * ====================================================================== */
void groove__storage__StorageTransaction__new(groove_DynStorage base, groove_StorageTransaction *out) {
    out->base = base;
    groove__storage__StagedWriteState__Default__default(&out->staged_writes);
}
void groove__storage__OrderedKvStorage__begin_txn(groove_DynStorage base, groove_StorageTransaction *out) {
    groove__storage__StorageTransaction__new(base, out);
}
jz_err groove__storage__StorageTransaction__commit(groove_StorageTransaction *t) {
    groove_OwnedWriteOperationVec operations = groove__storage__StagedWriteState__into_operations(&t->staged_writes);
    return groove__storage__OrderedKvStorage__write_many(&t->base, operations);
}
bool groove__storage__StorageTransaction__is_empty(const groove_StorageTransaction *t) {
    return groove__storage__StagedWriteState__is_empty(&t->staged_writes);
}
void groove__storage__StorageTransaction__stage_owned_operations(groove_StorageTransaction *t, groove_OwnedWriteOperationVec operations) {
    groove__storage__StagedWriteState__extend(&t->staged_writes, operations);
}
void groove__storage__StorageTransaction__drop(groove_StorageTransaction *t) { groove__storage__StagedWriteState__drop(&t->staged_writes); }

#define TX(self) ((groove_StorageTransaction *)(self))
static bool txn_permits_eager_read_retry(void *self) { return groove__storage__OrderedKvStorage__permits_eager_read_retry(&TX(self)->base); }
static jz_err txn_scan(void *self, groove_ScanRequest request, groove_StorageScan *out) {
    return overlay_scan(&TX(self)->base, &TX(self)->staged_writes, request, out);
}
static jz_err txn_get(void *self, jz_slice cf, jz_slice key, bool *some, jz_bytes *out) {
    groove_StagedWriteOverlay o = groove__storage__StagedWriteOverlay__new(TX(self)->base, &TX(self)->staged_writes);
    return overlay_get(&o, cf, key, some, out);
}
static jz_err txn_set(void *self, jz_slice cf, jz_slice key, jz_bytes value) {
    groove_OwnedWriteOperation op = {GROOVE_WRITE_SET, slice_to_str(cf), slice_to_bytes(key), value};
    groove__storage__StagedWriteState__stage(&TX(self)->staged_writes, op);
    return JZ_OK;
}
static jz_err txn_delete(void *self, jz_slice cf, jz_slice key) {
    groove_OwnedWriteOperation op = {GROOVE_WRITE_DELETE, slice_to_str(cf), slice_to_bytes(key), {0}};
    groove__storage__StagedWriteState__stage(&TX(self)->staged_writes, op);
    return JZ_OK;
}
static jz_err txn_write_many(void *self, groove_OwnedWriteOperationVec operations) {
    groove__storage__StagedWriteState__extend(&TX(self)->staged_writes, operations);
    return JZ_OK;
}
static jz_err txn_approximate_class_bytes(void *self, jz_slice cf, bool *some, u64 *out) {
    return groove__storage__OrderedKvStorage__approximate_class_bytes(&TX(self)->base, cf, some, out);
}
static bool txn_column_family_names(void *self, groove_StrVec *out) { return groove__storage__OrderedKvStorage__column_family_names(&TX(self)->base, out); }
static void txn_drop_boxed(void *self) { groove__storage__StorageTransaction__drop(TX(self)); }

const groove_OrderedKvStorageVtbl groove__storage__StorageTransaction__OrderedKvStorage__VTBL = {
    .permits_eager_read_retry = txn_permits_eager_read_retry,
    .get = txn_get,
    .compare_value = NULL, /* trait default over txn_get */
    .put_if_absent = txn_put_if_absent,
    .compare_and_delete = txn_compare_and_delete,
    .set = txn_set,
    .delete_ = txn_delete,
    .scan = txn_scan,
    .write_many = txn_write_many,
    .approximate_class_bytes = txn_approximate_class_bytes,
    .column_family_names = txn_column_family_names,
    .drop = txn_drop_boxed,
};
groove_DynStorage groove__storage__StorageTransaction__as_dyn(groove_StorageTransaction *t) {
    groove_DynStorage d = {t, &groove__storage__StorageTransaction__OrderedKvStorage__VTBL};
    return d;
}
