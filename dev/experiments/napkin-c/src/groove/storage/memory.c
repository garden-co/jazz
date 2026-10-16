/* groove::storage::memory (crates/groove/src/storage/memory.rs), non-test part.
 * The #[cfg(test)] scan_entries_materialized counter is omitted. */
#include "jz_rt.h"
#include "mod.h"

#define MEMORY_STORAGE_SNAPSHOT_VERSION 1u
static const u8 MEMORY_STORAGE_SNAPSHOT_MAGIC[4] = {'G', 'M', 'S', '1'};

static void str_drop_v(void *p) { jz_str_drop((jz_str *)p); }
static void bytes_drop_v(void *p) { jz_bytes_drop((jz_bytes *)p); }
static void btree_drop_v(void *p) { jz_btree_drop((jz_btree *)p); }
static void bytes_clone_v(void *d, const void *s) { *(jz_bytes *)d = jz_bytes_clone((const jz_bytes *)s); }
static void str_clone_v(void *d, const void *s) { *(jz_str *)d = jz_str_clone((const jz_str *)s); }

/* BTreeMap<Vec<u8>, Vec<u8>> */
static const jz_btree_desc ENTRIES_DESC = {sizeof(jz_bytes), sizeof(jz_bytes), jz_cmp_bytes, bytes_drop_v, bytes_drop_v};
/* BTreeMap<String, BTreeMap<..>> */
static const jz_btree_desc FAMILIES_DESC = {sizeof(jz_str), sizeof(jz_btree), jz_cmp_str, str_drop_v, btree_drop_v};
static void entries_clone_v(void *d, const void *s) { jz_btree_clone((jz_btree *)d, (const jz_btree *)s, bytes_clone_v, bytes_clone_v); }

static jz_bytes bytes_view(jz_slice s) { jz_bytes b = {(u8 *)s.ptr, s.len, 0}; return b; }
static jz_str str_view(jz_slice s) { jz_str b = {(char *)s.ptr, s.len, 0}; return b; }

/* ======================================================================
 * MemoryStorageSnapshotError
 * ====================================================================== */
static jz_err snap_invalid(const char *what) {
    return jz_err_fmt(GROOVE_MEMSNAP_E_Invalid, "memory storage snapshot is invalid: %s", what);
}
static jz_err snap_too_large(const char *field) {
    return jz_err_fmt(GROOVE_MEMSNAP_E_TooLarge, "memory storage snapshot %s exceeds v1 bounds", field);
}
static jz_err snap_unsupported_version(u16 found, u16 expected) {
    return jz_err_fmt(GROOVE_MEMSNAP_E_UnsupportedVersion, "unsupported memory storage snapshot version %u; expected %u", (unsigned)found,
                      (unsigned)expected);
}
/* #[from] Error */
static jz_err snap_invalid_column_family(jz_err e) {
    if (!e.code) return e;
    e = jz_err_context(e, "memory storage snapshot has an invalid physical column-family name");
    e.code = GROOVE_MEMSNAP_E_InvalidColumnFamily;
    return e;
}

/* ======================================================================
 * snapshot codec
 * ====================================================================== */
typedef struct MemoryStorageSnapshot { u16 version; jz_btree column_families; } MemoryStorageSnapshot;

static jz_err put_snapshot_count(jz_bytes *output, usize value, const char *field) {
    if (value > UINT32_MAX) return snap_too_large(field);
    u32 v = (u32)value;
    u8 be[4] = {(u8)(v >> 24), (u8)(v >> 16), (u8)(v >> 8), (u8)v};
    jz_bytes_push(output, be, 4);
    return JZ_OK;
}
static jz_err put_snapshot_u16(jz_bytes *output, usize value, const char *field) {
    if (value > UINT16_MAX) return snap_too_large(field);
    u8 be[2] = {(u8)(value >> 8), (u8)value};
    jz_bytes_push(output, be, 2);
    return JZ_OK;
}

static jz_err encode_snapshot_into(const MemoryStorageSnapshot *snapshot, jz_bytes *bytes) {
    jz_bytes_push(bytes, MEMORY_STORAGE_SNAPSHOT_MAGIC, 4);
    u8 ver[2] = {(u8)(snapshot->version >> 8), (u8)snapshot->version};
    jz_bytes_push(bytes, ver, 2);
    JZ_TRY(put_snapshot_count(bytes, snapshot->column_families.len, "family count"));
    jz_str *name;
    jz_btree *entries;
    JZ_BTREE_FOR(&snapshot->column_families, name, entries) {
        JZ_TRY(put_snapshot_u16(bytes, name->len, "family name"));
        jz_bytes_push(bytes, (const u8 *)name->ptr, name->len);
        JZ_TRY(put_snapshot_count(bytes, entries->len, "entry count"));
        jz_bytes *key, *value;
        JZ_BTREE_FOR(entries, key, value) {
            JZ_TRY(put_snapshot_count(bytes, key->len, "key"));
            jz_bytes_push(bytes, key->ptr, key->len);
            JZ_TRY(put_snapshot_count(bytes, value->len, "value"));
            jz_bytes_push(bytes, value->ptr, value->len);
        }
    }
    return JZ_OK;
}
static jz_err encode_snapshot(const MemoryStorageSnapshot *snapshot, jz_bytes *out) {
    jz_bytes bytes = {0};
    jz_err e = encode_snapshot_into(snapshot, &bytes);
    if (e.code) {
        jz_bytes_drop(&bytes);
        return e;
    }
    *out = bytes;
    return JZ_OK;
}

typedef struct SnapIn { const u8 *p; u32 len; } SnapIn;
static jz_err take_snapshot(SnapIn *input, u32 count, const u8 **out) {
    if (input->len < count) return snap_invalid("truncated bytes");
    *out = input->p;
    input->p += count;
    input->len -= count;
    return JZ_OK;
}
static jz_err take_snapshot_u16(SnapIn *input, u16 *out) {
    const u8 *b;
    JZ_TRY(take_snapshot(input, 2, &b));
    *out = (u16)(((u16)b[0] << 8) | b[1]);
    return JZ_OK;
}
static jz_err take_snapshot_u32(SnapIn *input, u32 *out) {
    const u8 *b;
    JZ_TRY(take_snapshot(input, 4, &b));
    *out = ((u32)b[0] << 24) | ((u32)b[1] << 16) | ((u32)b[2] << 8) | b[3];
    return JZ_OK;
}

static jz_err decode_snapshot_into(jz_slice bytes, MemoryStorageSnapshot *snapshot) {
    SnapIn input = {bytes.ptr, bytes.len};
    const u8 *magic;
    JZ_TRY(take_snapshot(&input, 4, &magic));
    if (memcmp(magic, MEMORY_STORAGE_SNAPSHOT_MAGIC, 4) != 0) return snap_invalid("magic is invalid");
    JZ_TRY(take_snapshot_u16(&input, &snapshot->version));
    u32 family_count;
    JZ_TRY(take_snapshot_u32(&input, &family_count));
    for (u32 f = 0; f < family_count; f++) {
        u16 name_len;
        JZ_TRY(take_snapshot_u16(&input, &name_len));
        const u8 *name_p;
        JZ_TRY(take_snapshot(&input, name_len, &name_p));
        if (!jz_utf8_valid(name_p, name_len)) return snap_invalid("family name is not UTF-8");
        jz_str name = jz_str_from_slice(jz_slice_of(name_p, name_len));
        u32 entry_count;
        jz_err e = take_snapshot_u32(&input, &entry_count);
        if (e.code) {
            jz_str_drop(&name);
            return e;
        }
        jz_btree entries;
        jz_btree_init(&entries, &ENTRIES_DESC);
        for (u32 i = 0; i < entry_count && !e.code; i++) {
            u32 key_len, value_len;
            const u8 *kp, *vp;
            e = take_snapshot_u32(&input, &key_len);
            if (!e.code) e = take_snapshot(&input, key_len, &kp);
            if (!e.code) e = take_snapshot_u32(&input, &value_len);
            if (!e.code) e = take_snapshot(&input, value_len, &vp);
            if (e.code) break;
            jz_bytes key = jz_bytes_from(kp, key_len), value = jz_bytes_from(vp, value_len);
            jz_bytes *slot = (jz_bytes *)jz_btree_get(&entries, &key);
            if (slot) {
                jz_bytes_drop(slot);
                *slot = value;
                jz_bytes_drop(&key);
            } else {
                jz_btree_insert(&entries, &key, &value, NULL);
            }
        }
        if (e.code) {
            jz_str_drop(&name);
            jz_btree_drop(&entries);
            return e;
        }
        jz_btree *slot = (jz_btree *)jz_btree_get(&snapshot->column_families, &name);
        if (slot) {
            jz_btree_drop(slot);
            *slot = entries;
            jz_str_drop(&name);
        } else {
            jz_btree_insert(&snapshot->column_families, &name, &entries, NULL);
        }
    }
    if (input.len != 0) return snap_invalid("trailing bytes");
    jz_bytes reencoded;
    JZ_TRY(encode_snapshot(snapshot, &reencoded));
    bool same = reencoded.len == bytes.len && memcmp(reencoded.ptr, bytes.ptr, bytes.len) == 0;
    jz_bytes_drop(&reencoded);
    if (!same) return snap_invalid("noncanonical bytes");
    return JZ_OK;
}
static jz_err decode_snapshot(jz_slice bytes, MemoryStorageSnapshot *out) {
    out->version = 0;
    jz_btree_init(&out->column_families, &FAMILIES_DESC);
    jz_err e = decode_snapshot_into(bytes, out);
    if (e.code) jz_btree_drop(&out->column_families);
    return e;
}

/* ======================================================================
 * MemoryStorage
 * ====================================================================== */
static void families_rc_drop(void *p) { jz_btree_drop(&((groove_MemoryColumnFamilies *)p)->families); }

static groove_MemoryStorage memory_storage_default(void) {
    groove_MemoryStorage s;
    s.inner = (groove_MemoryColumnFamilies *)jz_rc_new(sizeof(groove_MemoryColumnFamilies), families_rc_drop);
    jz_btree_init(&s.inner->families, &FAMILIES_DESC);
    return s;
}

static void ensure_column_family(const groove_MemoryStorage *s, jz_slice cf) {
    jz_str view = str_view(cf);
    if (jz_btree_contains(&s->inner->families, &view)) return;
    jz_str name = jz_str_from_slice(cf);
    jz_btree entries;
    jz_btree_init(&entries, &ENTRIES_DESC);
    jz_btree_insert(&s->inner->families, &name, &entries, NULL);
}

jz_err groove__storage__memory__MemoryStorage__new(const char *const *column_families, u32 n, groove_MemoryStorage *out) {
    JZ_TRY(groove__storage__validate_physical_storage_names_cstr(column_families, n));
    groove_MemoryStorage storage = memory_storage_default();
    for (u32 i = 0; i < n; i++) ensure_column_family(&storage, jz_slice_of(column_families[i], (u32)strlen(column_families[i])));
    *out = storage;
    return JZ_OK;
}
groove_MemoryStorage groove__storage__memory__MemoryStorage__Clone__clone(const groove_MemoryStorage *s) {
    groove_MemoryStorage c = {(groove_MemoryColumnFamilies *)jz_rc_clone(s->inner)};
    return c;
}
void groove__storage__memory__MemoryStorage__drop(groove_MemoryStorage *s) {
    jz_rc_release(s->inner);
    s->inner = NULL;
}

/* with_cf: borrow one family, or ColumnFamilyNotFound */
static jz_err with_cf(const groove_MemoryStorage *s, jz_slice cf, jz_btree **out) {
    jz_str view = str_view(cf);
    jz_btree *values = (jz_btree *)jz_btree_get(&s->inner->families, &view);
    if (!values) return groove__storage__Error__column_family_not_found(cf);
    *out = values;
    return JZ_OK;
}

jz_err groove__storage__memory__MemoryStorage__export_snapshot(const groove_MemoryStorage *s, jz_bytes *out) {
    MemoryStorageSnapshot snapshot;
    snapshot.version = MEMORY_STORAGE_SNAPSHOT_VERSION;
    jz_btree_clone(&snapshot.column_families, &s->inner->families, str_clone_v, entries_clone_v);
    jz_err e = encode_snapshot(&snapshot, out);
    jz_btree_drop(&snapshot.column_families);
    return e;
}

jz_err groove__storage__memory__MemoryStorage__import_snapshot(const groove_MemoryStorage *s, jz_slice bytes) {
    MemoryStorageSnapshot snapshot;
    JZ_TRY(decode_snapshot(bytes, &snapshot));
    if (snapshot.version != MEMORY_STORAGE_SNAPSHOT_VERSION) {
        jz_btree_drop(&snapshot.column_families);
        return snap_unsupported_version(snapshot.version, MEMORY_STORAGE_SNAPSHOT_VERSION);
    }
    jz_str *name;
    jz_btree *entries;
    JZ_BTREE_FOR(&snapshot.column_families, name, entries) {
        jz_err e = groove__storage__validate_physical_storage_name(jz_str_slice(name));
        if (e.code) {
            jz_btree_drop(&snapshot.column_families);
            return snap_invalid_column_family(e);
        }
    }
    jz_btree_drop(&s->inner->families);
    s->inner->families = snapshot.column_families;
    return JZ_OK;
}

/* ======================================================================
 * MemoryStorageCursor
 * ====================================================================== */
typedef struct MemoryStorageCursor {
    groove_MemoryStorage storage;
    jz_str cf;
    groove_ScanBounds bounds;
    groove_ScanDirection direction;
    bool has_remaining;
    usize remaining;
    bool has_last_key;
    jz_bytes last_key;
    bool done;
} MemoryStorageCursor;

static bool memory_cursor_batch_limit(const MemoryStorageCursor *c, usize *out) {
    if (!c->has_remaining) {
        *out = 256;
        return true;
    }
    if (c->remaining == 0) return false;
    *out = c->remaining < 256 ? c->remaining : 256;
    return true;
}

static jz_err memory_cursor_next_values(MemoryStorageCursor *c, usize limit, groove_KeyValueVec *out) {
    jz_btree *values;
    JZ_TRY(with_cf(&c->storage, jz_str_slice(&c->cf), &values));
    /* Every key in the scan's bounds is in range (a prefix scan ends at the
     * prefix successor). Batches resume strictly past the last key returned. */
    const jz_bytes *lower = &c->bounds.start;
    bool lower_incl = true;
    jz_bytes succ = {0};
    const jz_bytes *upper = NULL;
    bool upper_incl = false;
    if (c->bounds.tag == GROOVE_SCAN_PREFIX) {
        if (groove__storage__prefix_successor(jz_bytes_slice(&c->bounds.start), &succ)) upper = &succ;
    } else {
        upper = &c->bounds.end;
    }
    if (c->has_last_key) {
        if (c->direction == GROOVE_SCAN_FORWARD) {
            lower = &c->last_key;
            lower_incl = false;
        } else {
            upper = &c->last_key;
            upper_incl = false;
        }
    }
    groove_KeyValueVec batch = {0};
    jz_btree_iter it;
    jz_btree_range(&it, values, lower, lower_incl, upper, upper_incl);
    jz_bytes *k, *v;
    if (c->direction == GROOVE_SCAN_FORWARD) {
        while (batch.len < limit && jz_btree_next(&it, (void **)&k, (void **)&v)) {
            groove_KeyValue kv = {jz_bytes_clone(k), jz_bytes_clone(v)};
            JZ_VEC_PUSH(batch, kv);
        }
    } else {
        /* range.rev().take(limit) */
        JZ_VEC(jz_bytes *) ks = {0};
        JZ_VEC(jz_bytes *) vs = {0};
        while (jz_btree_next(&it, (void **)&k, (void **)&v)) {
            JZ_VEC_PUSH(ks, k);
            JZ_VEC_PUSH(vs, v);
        }
        for (u32 i = ks.len; i-- > 0 && batch.len < limit;) {
            groove_KeyValue kv = {jz_bytes_clone(ks.ptr[i]), jz_bytes_clone(vs.ptr[i])};
            JZ_VEC_PUSH(batch, kv);
        }
        jz_vec_drop(&ks, sizeof(jz_bytes *), NULL);
        jz_vec_drop(&vs, sizeof(jz_bytes *), NULL);
    }
    jz_bytes_drop(&succ);
    if (batch.len) {
        jz_bytes_drop(&c->last_key);
        c->last_key = jz_bytes_clone(&batch.ptr[batch.len - 1].key);
        c->has_last_key = true;
    } else {
        c->done = true;
    }
    *out = batch;
    return JZ_OK;
}

static jz_err memory_cursor_next_batch(void *self, bool *some, groove_KeyValueVec *out) {
    MemoryStorageCursor *c = (MemoryStorageCursor *)self;
    if (c->done) {
        *some = false;
        return JZ_OK;
    }
    usize limit;
    if (!memory_cursor_batch_limit(c, &limit)) {
        c->done = true;
        *some = false;
        return JZ_OK;
    }
    groove_KeyValueVec values;
    JZ_TRY(memory_cursor_next_values(c, limit, &values));
    if (values.len == 0) {
        jz_vec_drop(&values, sizeof(groove_KeyValue), NULL);
        *some = false;
        return JZ_OK;
    }
    if (c->has_remaining) c->remaining -= values.len;
    *some = true;
    *out = values;
    return JZ_OK;
}
static void memory_cursor_drop(void *self) {
    MemoryStorageCursor *c = (MemoryStorageCursor *)self;
    groove__storage__memory__MemoryStorage__drop(&c->storage);
    jz_str_drop(&c->cf);
    groove__storage__ScanBounds__drop(&c->bounds);
    jz_bytes_drop(&c->last_key);
    jz_free(c);
}
static const groove_StorageCursorVtbl MEMORY_CURSOR_VTBL = {memory_cursor_next_batch, memory_cursor_drop};

/* ======================================================================
 * impl OrderedKvStorage for MemoryStorage
 * ====================================================================== */
#define MS(self) ((groove_MemoryStorage *)(self))

static jz_err mem_compare_value(void *self, jz_slice cf, jz_slice key, jz_slice expected, groove_ValueComparison *out) {
    jz_btree *values;
    JZ_TRY(with_cf(MS(self), cf, &values));
    jz_bytes kv = bytes_view(key);
    jz_bytes *value = (jz_bytes *)jz_btree_get(values, &kv);
    if (!value) *out = GROOVE_VALUE_ABSENT;
    else if (jz_slice_cmp(jz_bytes_slice(value), expected) == 0) *out = GROOVE_VALUE_IDENTICAL;
    else *out = GROOVE_VALUE_DIFFERENT;
    return JZ_OK;
}
static bool mem_permits_eager_read_retry(void *self) { return true; }
static jz_err mem_get(void *self, jz_slice cf, jz_slice key, bool *some, jz_bytes *out) {
    jz_btree *values;
    JZ_TRY(with_cf(MS(self), cf, &values));
    jz_bytes kv = bytes_view(key);
    jz_bytes *value = (jz_bytes *)jz_btree_get(values, &kv);
    *some = value != NULL;
    if (value) *out = jz_bytes_clone(value);
    return JZ_OK;
}
static jz_err mem_put_if_absent(void *self, jz_slice cf, jz_slice key, jz_bytes value, bool *some, jz_bytes *existing) {
    jz_btree *values;
    jz_err e = with_cf(MS(self), cf, &values);
    if (e.code) {
        jz_bytes_drop(&value);
        return e;
    }
    jz_bytes kv = bytes_view(key);
    jz_bytes *current = (jz_bytes *)jz_btree_get(values, &kv);
    if (current) {
        *some = true;
        *existing = jz_bytes_clone(current);
        jz_bytes_drop(&value);
        return JZ_OK;
    }
    jz_bytes owned_key = jz_bytes_from(key.ptr, key.len);
    jz_btree_insert(values, &owned_key, &value, NULL);
    *some = false;
    return JZ_OK;
}
static jz_err mem_compare_and_delete(void *self, jz_slice cf, jz_slice key, jz_slice expected, bool *removed) {
    jz_btree *values;
    JZ_TRY(with_cf(MS(self), cf, &values));
    jz_bytes kv = bytes_view(key);
    jz_bytes *current = (jz_bytes *)jz_btree_get(values, &kv);
    if (!current || jz_slice_cmp(jz_bytes_slice(current), expected) != 0) {
        *removed = false;
        return JZ_OK;
    }
    jz_bytes ok, ov;
    jz_btree_remove(values, &kv, &ok, &ov);
    jz_bytes_drop(&ok);
    jz_bytes_drop(&ov);
    *removed = true;
    return JZ_OK;
}
static jz_err mem_approximate_class_bytes(void *self, jz_slice cf, bool *some, u64 *out) {
    jz_btree *values;
    JZ_TRY(with_cf(MS(self), cf, &values));
    u64 sum = 0;
    jz_bytes *k, *v;
    JZ_BTREE_FOR(values, k, v) sum += (u64)k->len + (u64)v->len;
    *some = true;
    *out = sum;
    return JZ_OK;
}
static void entries_put(jz_btree *values, jz_slice key, jz_bytes value) {
    jz_bytes kv = bytes_view(key);
    jz_bytes *slot = (jz_bytes *)jz_btree_get(values, &kv);
    if (slot) {
        jz_bytes_drop(slot);
        *slot = value;
    } else {
        jz_bytes owned_key = jz_bytes_from(key.ptr, key.len);
        jz_btree_insert(values, &owned_key, &value, NULL);
    }
}
static void entries_remove(jz_btree *values, jz_slice key) {
    jz_bytes kv = bytes_view(key);
    jz_bytes ok, ov;
    if (jz_btree_remove(values, &kv, &ok, &ov)) {
        jz_bytes_drop(&ok);
        jz_bytes_drop(&ov);
    }
}
static jz_err mem_set(void *self, jz_slice cf, jz_slice key, jz_bytes value) {
    jz_btree *values;
    jz_err e = with_cf(MS(self), cf, &values);
    if (e.code) {
        jz_bytes_drop(&value);
        return e;
    }
    entries_put(values, key, value);
    return JZ_OK;
}
static jz_err mem_delete(void *self, jz_slice cf, jz_slice key) {
    jz_btree *values;
    JZ_TRY(with_cf(MS(self), cf, &values));
    entries_remove(values, key);
    return JZ_OK;
}
static jz_err mem_scan(void *self, groove_ScanRequest request, groove_StorageScan *out) {
    /* Validate eagerly so even a zero-item request reports a missing family. */
    jz_btree *values;
    jz_err e = with_cf(MS(self), jz_str_slice(&request.cf), &values);
    if (e.code) {
        groove__storage__ScanRequest__drop(&request);
        return e;
    }
    if (groove__storage__ScanBounds__is_empty_range(&request.bounds)) {
        groove__storage__ScanRequest__drop(&request);
        *out = groove__storage__ReadyStorageCursor__new((groove_KeyValueVec){0});
        return JZ_OK;
    }
    MemoryStorageCursor *c = JZ_NEW(MemoryStorageCursor);
    c->storage = groove__storage__memory__MemoryStorage__Clone__clone(MS(self));
    c->cf = request.cf;
    c->bounds = request.bounds;
    c->direction = request.direction;
    c->has_remaining = request.has_max_items;
    c->remaining = request.max_items;
    c->has_last_key = false;
    c->done = false;
    out->self = c;
    out->vt = &MEMORY_CURSOR_VTBL;
    return JZ_OK;
}
static jz_err mem_last_with_prefix(void *self, jz_slice cf, jz_slice prefix, bool *some, groove_KeyValue *out) {
    jz_btree *values;
    JZ_TRY(with_cf(MS(self), cf, &values));
    jz_bytes lo = bytes_view(prefix);
    jz_bytes upper = {0};
    bool has_upper = groove__storage__prefix_successor(prefix, &upper);
    jz_btree_iter it;
    jz_btree_range(&it, values, &lo, true, has_upper ? &upper : NULL, false);
    jz_bytes *k, *v, *lk = NULL, *lv = NULL;
    while (jz_btree_next(&it, (void **)&k, (void **)&v)) {
        lk = k;
        lv = v;
    }
    jz_bytes_drop(&upper);
    *some = lk != NULL;
    if (lk) {
        out->key = jz_bytes_clone(lk);
        out->value = jz_bytes_clone(lv);
    }
    return JZ_OK;
}
static jz_err mem_last_with_prefix_before_or_at(void *self, jz_slice cf, jz_slice prefix, jz_slice upper, bool *some, groove_KeyValue *out) {
    jz_btree *values;
    JZ_TRY(with_cf(MS(self), cf, &values));
    jz_bytes lo = bytes_view(prefix), hi = bytes_view(upper);
    /* values.range(prefix..=upper).rev().find(|(key, _)| key.starts_with(&prefix)) */
    jz_btree_iter it;
    jz_btree_range(&it, values, &lo, true, &hi, true);
    jz_bytes *k, *v, *fk = NULL, *fv = NULL;
    while (jz_btree_next(&it, (void **)&k, (void **)&v)) {
        if (k->len >= prefix.len && (prefix.len == 0 || memcmp(k->ptr, prefix.ptr, prefix.len) == 0)) {
            fk = k;
            fv = v;
        }
    }
    *some = fk != NULL;
    if (fk) {
        out->key = jz_bytes_clone(fk);
        out->value = jz_bytes_clone(fv);
    }
    return JZ_OK;
}

/* planned: BTreeMap<(String, Vec<u8>), Option<Vec<u8>>> */
typedef struct PlannedKey { jz_str cf; jz_bytes key; } PlannedKey;
typedef struct PlannedVal { bool some; jz_bytes value; } PlannedVal;
static int planned_key_cmp(const void *a, const void *b, void *ctx) {
    const PlannedKey *x = (const PlannedKey *)a, *y = (const PlannedKey *)b;
    int c = jz_str_cmp(&x->cf, &y->cf);
    return c ? c : jz_bytes_cmp(&x->key, &y->key);
}
static void planned_key_drop(void *p) {
    PlannedKey *k = (PlannedKey *)p;
    jz_str_drop(&k->cf);
    jz_bytes_drop(&k->key);
}
static void planned_val_drop(void *p) { jz_bytes_drop(&((PlannedVal *)p)->value); }
static const jz_btree_desc PLANNED_DESC = {sizeof(PlannedKey), sizeof(PlannedVal), planned_key_cmp, planned_key_drop, planned_val_drop};

static jz_err mem_write_many(void *self, groove_OwnedWriteOperationVec operations) {
    groove_MemoryStorage *s = MS(self);
    /* Validate and evaluate against a per-key prospective overlay before
     * changing the store: batch atomicity and read-your-prior-operation. */
    jz_btree planned;
    jz_btree_init(&planned, &PLANNED_DESC);
    for (u32 i = 0; i < operations.len; i++) {
        groove_OwnedWriteOperation *op = &operations.ptr[i];
        if (!jz_btree_contains(&s->inner->families, &op->cf)) {
            jz_err e = groove__storage__Error__column_family_not_found(jz_str_slice(&op->cf));
            jz_btree_drop(&planned);
            groove__storage__OwnedWriteOperationVec__drop(&operations);
            return e;
        }
        PlannedKey k = {op->cf, op->key};
        PlannedVal v = {op->tag == GROOVE_WRITE_SET, op->tag == GROOVE_WRITE_SET ? op->value : (jz_bytes){0}};
        op->cf = (jz_str){0};
        op->key = (jz_bytes){0};
        op->value = (jz_bytes){0};
        PlannedVal *slot = (PlannedVal *)jz_btree_get(&planned, &k);
        if (slot) {
            planned_val_drop(slot);
            *slot = v;
            planned_key_drop(&k);
        } else {
            jz_btree_insert(&planned, &k, &v, NULL);
        }
    }
    groove__storage__OwnedWriteOperationVec__drop(&operations);
    PlannedKey k;
    PlannedVal v;
    while (jz_btree_pop_first(&planned, &k, &v)) {
        jz_btree *values = (jz_btree *)jz_btree_get(&s->inner->families, &k.cf);
        JZ_ASSERT(values != NULL, "column family was validated");
        if (v.some) {
            entries_put(values, jz_bytes_slice(&k.key), v.value);
        } else {
            entries_remove(values, jz_bytes_slice(&k.key));
        }
        planned_key_drop(&k);
    }
    jz_btree_drop(&planned);
    return JZ_OK;
}
static groove_WriteManyOutcome mem_write_many_outcome(void *self, groove_OwnedWriteOperationVec operations) {
    /* Memory validates every operation before changing its map, so an error
     * is proven not to have committed. */
    jz_err e = mem_write_many(self, operations);
    groove_WriteManyOutcome o = {e.code ? GROOVE_WMO_UNCOMMITTED : GROOVE_WMO_COMMITTED, e};
    return o;
}
static bool mem_column_family_names(void *self, groove_StrVec *out) {
    groove_StrVec names = {0};
    jz_str *name;
    jz_btree *entries;
    JZ_BTREE_FOR(&MS(self)->inner->families, name, entries) JZ_VEC_PUSH(names, jz_str_clone(name));
    *out = names;
    return true;
}

jz_err groove__storage__memory__MemoryStorage__ReopenableStorage__reopen(groove_MemoryStorage self, groove_StrVec column_families, groove_MemoryStorage *out) {
    jz_err e = groove__storage__validate_physical_storage_names(column_families.ptr, column_families.len);
    if (e.code) {
        groove__storage__StrVec__drop(&column_families);
        groove__storage__memory__MemoryStorage__drop(&self);
        return e;
    }
    for (u32 i = 0; i < column_families.len; i++) ensure_column_family(&self, jz_str_slice(&column_families.ptr[i]));
    groove__storage__StrVec__drop(&column_families);
    *out = self;
    return JZ_OK;
}

/* ErasedReopenableStorage::reopen_boxed for Box<MemoryStorage> */
static jz_err mem_reopen_boxed(void *self, groove_StrVec column_families, groove_DynStorage *out) {
    groove_MemoryStorage *boxed = MS(self);
    groove_MemoryStorage reopened;
    jz_err e = groove__storage__memory__MemoryStorage__ReopenableStorage__reopen(*boxed, column_families, &reopened);
    jz_free(boxed);
    JZ_TRY(e);
    *out = groove__storage__memory__MemoryStorage__into_dyn(reopened);
    return JZ_OK;
}
static void mem_drop_boxed(void *self) {
    groove__storage__memory__MemoryStorage__drop(MS(self));
    jz_free(self);
}

const groove_OrderedKvStorageVtbl groove__storage__memory__MemoryStorage__OrderedKvStorage__VTBL = {
    .permits_eager_read_retry = mem_permits_eager_read_retry,
    .get = mem_get,
    .compare_value = mem_compare_value,
    .put_if_absent = mem_put_if_absent,
    .compare_and_delete = mem_compare_and_delete,
    .set = mem_set,
    .delete_ = mem_delete,
    .approximate_class_bytes = mem_approximate_class_bytes,
    .scan = mem_scan,
    .last_with_prefix = mem_last_with_prefix,
    .last_with_prefix_before_or_at = mem_last_with_prefix_before_or_at,
    .write_many = mem_write_many,
    .write_many_outcome = mem_write_many_outcome,
    .column_family_names = mem_column_family_names,
    .reopen = mem_reopen_boxed,
    .drop = mem_drop_boxed,
};

groove_DynStorage groove__storage__memory__MemoryStorage__into_dyn(groove_MemoryStorage s) {
    groove_MemoryStorage *boxed = JZ_NEW(groove_MemoryStorage);
    *boxed = s;
    groove_DynStorage d = {boxed, &groove__storage__memory__MemoryStorage__OrderedKvStorage__VTBL};
    return d;
}
