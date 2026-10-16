/* crates/idb-tree/src/page.rs: the fixed IDBTree v1 page format */
#include "idb_tree/idb_tree.h"

static const u8 MAGIC[8] = {'I', 'D', 'B', 'T', 'R', 'E', 'E', 0};
#define FORMAT_VERSION 1u
#define HEADER_LEN IDB_PAGE_HEADER_LEN

#define PAGE_LEAF_TAG 0u
#define PAGE_INTERNAL_TAG 1u
#define PAGE_OVERFLOW_TAG 2u
#define VALUE_INLINE_TAG 0u
#define VALUE_OVERFLOW_TAG 1u
#define OPTION_NONE_TAG 0u
#define OPTION_SOME_TAG 1u

static jz_err str_err(const char *msg) { return jz_err_new(IDB_STRING_ERR, msg); }

/* ---- drop / clone glue ---- */
void idb_tree__page__ValueCell__drop(void *p) {
    idb_tree_ValueCell *v = (idb_tree_ValueCell *)p;
    if (v->tag == IDB_VALUE_INLINE) jz_bytes_drop(&v->inline_bytes);
}

void idb_tree__page__ValueCell__clone(void *dst, const void *src) {
    const idb_tree_ValueCell *s = (const idb_tree_ValueCell *)src;
    idb_tree_ValueCell *d = (idb_tree_ValueCell *)dst;
    *d = *s;
    if (s->tag == IDB_VALUE_INLINE) d->inline_bytes = jz_bytes_clone(&s->inline_bytes);
}

void idb_tree__page__LeafEntry__drop(void *p) {
    idb_tree_LeafEntry *e = (idb_tree_LeafEntry *)p;
    jz_bytes_drop(&e->key);
    idb_tree__page__ValueCell__drop(&e->value);
}

void idb_tree__page__LeafEntry__clone(void *dst, const void *src) {
    const idb_tree_LeafEntry *s = (const idb_tree_LeafEntry *)src;
    idb_tree_LeafEntry *d = (idb_tree_LeafEntry *)dst;
    d->key = jz_bytes_clone(&s->key);
    idb_tree__page__ValueCell__clone(&d->value, &s->value);
}

static void bytes_drop(void *p) { jz_bytes_drop((jz_bytes *)p); }
static void bytes_clone(void *dst, const void *src) { *(jz_bytes *)dst = jz_bytes_clone((const jz_bytes *)src); }

void idb_tree__page__Page__drop(void *p) {
    idb_tree_Page *page = (idb_tree_Page *)p;
    switch (page->tag) {
    case IDB_PAGE_LEAF: jz_vec_drop(&page->entries, sizeof(idb_tree_LeafEntry), idb_tree__page__LeafEntry__drop); break;
    case IDB_PAGE_INTERNAL:
        jz_vec_drop(&page->keys, sizeof(jz_bytes), bytes_drop);
        jz_vec_drop(&page->children, sizeof(idb_tree_PageId), NULL);
        break;
    case IDB_PAGE_OVERFLOW: jz_bytes_drop(&page->bytes); break;
    }
}

void idb_tree__page__Page__clone(void *dst, const void *src) {
    const idb_tree_Page *s = (const idb_tree_Page *)src;
    idb_tree_Page *d = (idb_tree_Page *)dst;
    memset(d, 0, sizeof *d);
    d->tag = s->tag;
    switch (s->tag) {
    case IDB_PAGE_LEAF: jz_vec_clone(&d->entries, &s->entries, sizeof(idb_tree_LeafEntry), idb_tree__page__LeafEntry__clone); break;
    case IDB_PAGE_INTERNAL:
        jz_vec_clone(&d->keys, &s->keys, sizeof(jz_bytes), bytes_clone);
        jz_vec_clone(&d->children, &s->children, sizeof(idb_tree_PageId), NULL);
        break;
    case IDB_PAGE_OVERFLOW:
        d->has_next = s->has_next;
        d->next = s->next;
        d->bytes = jz_bytes_clone(&s->bytes);
        break;
    }
}

void idb_tree__page__Page__leaf(idb_tree_Page *out) {
    memset(out, 0, sizeof *out);
    out->tag = IDB_PAGE_LEAF;
}

/* ---- exact v1 sizes ---- */
usize idb_tree__page__leaf_entry_len(const jz_bytes *key, const idb_tree_ValueCell *value) {
    usize n = 4 + key->len;
    switch (value->tag) {
    case IDB_VALUE_INLINE: n += 1 + 4 + value->inline_bytes.len; break;
    case IDB_VALUE_OVERFLOW: n += 1 + 8 + 8; break;
    }
    return n;
}

usize idb_tree__page__internal_key_len(const jz_bytes *key) { return 4 + key->len + 8; }

usize idb_tree__page__encoded_len(const idb_tree_Page *page) {
    switch (page->tag) {
    case IDB_PAGE_LEAF: {
        usize sum = 0;
        JZ_VEC_FOR(idb_tree_LeafEntry, e, page->entries) sum += idb_tree__page__leaf_entry_len(&e->key, &e->value);
        return IDB_LEAF_BASE_LEN + sum;
    }
    case IDB_PAGE_INTERNAL: {
        usize sum = 0;
        JZ_VEC_FOR(jz_bytes, k, page->keys) sum += 4 + k->len;
        return HEADER_LEN + 1 + 4 + sum + 4 + 8 * (usize)page->children.len;
    }
    case IDB_PAGE_OVERFLOW: return HEADER_LEN + 1 + (page->has_next ? 9 : 1) + 4 + page->bytes.len;
    }
    JZ_UNREACHABLE();
}

/* ---- writers ---- */
static void put_u8(jz_bytes *out, u8 v) { jz_bytes_push_u8(out, v); }
static void put_u32(jz_bytes *out, u32 v) {
    u8 b[4];
    for (int i = 0; i < 4; i++) b[i] = (u8)(v >> (8 * i));
    jz_bytes_push(out, b, 4);
}
static void put_u64(jz_bytes *out, u64 v) {
    u8 b[8];
    for (int i = 0; i < 8; i++) b[i] = (u8)(v >> (8 * i));
    jz_bytes_push(out, b, 8);
}
static jz_err put_len(jz_bytes *out, usize len, const char *name) {
    if ((u64)len > 0xffffffffull) return jz_err_fmt(IDB_STRING_ERR, "%s exceeds u32", name);
    put_u32(out, (u32)len);
    return JZ_OK;
}
static jz_err put_bytes(jz_bytes *out, const u8 *p, u32 n, const char *name) {
    JZ_TRY(put_len(out, n, name));
    jz_bytes_push(out, p, n);
    return JZ_OK;
}
static jz_err put_byte_strings(jz_bytes *out, const idb_tree_KeyVec *values, const char *name) {
    JZ_TRY(put_len(out, values->len, name));
    JZ_VEC_FOR(jz_bytes, v, *values) JZ_TRY(put_bytes(out, v->ptr, v->len, name));
    return JZ_OK;
}

/* validate_strictly_increasing over a strided array of jz_bytes keys */
static jz_err validate_strictly_increasing(const void *base, u32 n, usize stride, const char *name) {
    const jz_bytes *previous = NULL;
    for (u32 i = 0; i < n; i++) {
        const jz_bytes *value = (const jz_bytes *)((const u8 *)base + (usize)i * stride);
        if (previous && jz_bytes_cmp(previous, value) >= 0) return jz_err_fmt(IDB_STRING_ERR, "%s must be strictly increasing", name);
        previous = value;
    }
    return JZ_OK;
}

static u64 pid_hash(const void *k) { return jz_hash_u64(*(const u64 *)k); }
static bool pid_eq(const void *a, const void *b) { return *(const u64 *)a == *(const u64 *)b; }
static const jz_hmap_desc PID_SET = {sizeof(u64), 0, pid_hash, pid_eq, NULL, NULL};

static jz_err idb_tree__page__validate_internal_shape(const idb_tree_KeyVec *keys, const idb_tree_PageIdVec *children) {
    JZ_TRY(validate_strictly_increasing(keys->ptr, keys->len, sizeof(jz_bytes), "internal keys"));
    if (keys->len == 0xffffffffu) return str_err("internal key count overflow");
    u32 expected_children = keys->len + 1;
    if (children->len != expected_children)
        return jz_err_fmt(IDB_STRING_ERR, "internal page has %u keys but %u children", keys->len, children->len);
    jz_hmap seen;
    jz_hmap_init(&seen, &PID_SET);
    bool shared = false;
    for (u32 i = 0; i < children->len; i++) {
        bool inserted;
        jz_hmap_entry(&seen, &children->ptr[i], &inserted);
        if (!inserted) {
            shared = true;
            break;
        }
    }
    jz_hmap_drop(&seen);
    if (shared) return str_err("internal page has a shared child");
    return JZ_OK;
}

static jz_err idb_tree__page__encode_value_cell(jz_bytes *out, const idb_tree_ValueCell *value) {
    switch (value->tag) {
    case IDB_VALUE_INLINE:
        put_u8(out, VALUE_INLINE_TAG);
        return put_bytes(out, value->inline_bytes.ptr, value->inline_bytes.len, "inline value");
    case IDB_VALUE_OVERFLOW:
        if (value->len == 0) return str_err("overflow value length must not be zero");
        put_u8(out, VALUE_OVERFLOW_TAG);
        put_u64(out, value->head);
        put_u64(out, value->len);
        return JZ_OK;
    }
    JZ_UNREACHABLE();
}

jz_err idb_tree__page__encode_page(const idb_tree_Page *page, jz_bytes *encoded) {
    jz_bytes payload = {0};
    jz_err e = JZ_OK;
    switch (page->tag) {
    case IDB_PAGE_LEAF:
        e = validate_strictly_increasing(page->entries.ptr, page->entries.len, sizeof(idb_tree_LeafEntry), "leaf keys");
        if (e.code) goto fail;
        put_u8(&payload, PAGE_LEAF_TAG);
        if ((e = put_len(&payload, page->entries.len, "leaf entry count")).code) goto fail;
        JZ_VEC_FOR(idb_tree_LeafEntry, entry, page->entries) {
            if ((e = put_bytes(&payload, entry->key.ptr, entry->key.len, "leaf key")).code) goto fail;
            if ((e = idb_tree__page__encode_value_cell(&payload, &entry->value)).code) goto fail;
        }
        break;
    case IDB_PAGE_INTERNAL:
        if ((e = idb_tree__page__validate_internal_shape(&page->keys, &page->children)).code) goto fail;
        put_u8(&payload, PAGE_INTERNAL_TAG);
        if ((e = put_byte_strings(&payload, &page->keys, "internal keys")).code) goto fail;
        if ((e = put_len(&payload, page->children.len, "internal child count")).code) goto fail;
        JZ_VEC_FOR(idb_tree_PageId, child, page->children) put_u64(&payload, *child);
        break;
    case IDB_PAGE_OVERFLOW:
        if (page->bytes.len == 0) {
            e = str_err("overflow page bytes must not be empty");
            goto fail;
        }
        put_u8(&payload, PAGE_OVERFLOW_TAG);
        if (!page->has_next) {
            put_u8(&payload, OPTION_NONE_TAG);
        } else {
            put_u8(&payload, OPTION_SOME_TAG);
            put_u64(&payload, page->next);
        }
        if ((e = put_bytes(&payload, page->bytes.ptr, page->bytes.len, "overflow page bytes")).code) goto fail;
        break;
    }
    memset(encoded, 0, sizeof *encoded);
    jz_vec_reserve(encoded, HEADER_LEN + payload.len, 1);
    jz_bytes_push(encoded, MAGIC, sizeof MAGIC);
    jz_bytes_push_u8(encoded, FORMAT_VERSION);
    put_u64(encoded, xxhash_rust__xxh3__xxh3_64(payload.ptr, payload.len));
    jz_bytes_push(encoded, payload.ptr, payload.len);
    jz_bytes_drop(&payload);
    return JZ_OK;
fail:
    jz_bytes_drop(&payload);
    return e;
}

/* ---- Decoder ---- */
typedef struct idb_tree_Decoder { const u8 *p; u32 remaining; } idb_tree_Decoder;

static jz_err idb_tree__page__Decoder__take(idb_tree_Decoder *d, u32 len, const char *name, const u8 **out) {
    if (len > d->remaining) return jz_err_fmt(IDB_STRING_ERR, "truncated %s", name);
    *out = d->p;
    d->p += len;
    d->remaining -= len;
    return JZ_OK;
}

static jz_err idb_tree__page__Decoder__u8(idb_tree_Decoder *d, const char *name, u8 *out) {
    const u8 *p;
    JZ_TRY(idb_tree__page__Decoder__take(d, 1, name, &p));
    *out = p[0];
    return JZ_OK;
}

static jz_err idb_tree__page__Decoder__u32(idb_tree_Decoder *d, const char *name, u32 *out) {
    const u8 *p;
    JZ_TRY(idb_tree__page__Decoder__take(d, 4, name, &p));
    *out = (u32)p[0] | (u32)p[1] << 8 | (u32)p[2] << 16 | (u32)p[3] << 24;
    return JZ_OK;
}

static jz_err idb_tree__page__Decoder__u64(idb_tree_Decoder *d, const char *name, u64 *out) {
    const u8 *p;
    JZ_TRY(idb_tree__page__Decoder__take(d, 8, name, &p));
    u64 v = 0;
    for (int i = 7; i >= 0; i--) v = v << 8 | p[i];
    *out = v;
    return JZ_OK;
}

static jz_err idb_tree__page__Decoder__count(idb_tree_Decoder *d, const char *name, u32 minimum_item_bytes, u32 *out) {
    u32 count;
    JZ_TRY(idb_tree__page__Decoder__u32(d, name, &count));
    /* usize::try_from(u32) cannot fail on wasm32; the Rust error arm is kept for parity */
    if (sizeof(usize) < 4) return jz_err_fmt(IDB_STRING_ERR, "%s exceeds this architecture", name);
    if (count > d->remaining / minimum_item_bytes) return jz_err_fmt(IDB_STRING_ERR, "%s exceeds the remaining payload", name);
    *out = count;
    return JZ_OK;
}

static jz_err idb_tree__page__Decoder__bytes(idb_tree_Decoder *d, const char *name, jz_bytes *out) {
    jz_str len_name = jz_format("%s length", name);
    u32 len;
    jz_str_push_char(&len_name, '\0');
    jz_err e = idb_tree__page__Decoder__u32(d, len_name.ptr, &len);
    jz_str_drop(&len_name);
    JZ_TRY(e);
    if (sizeof(usize) < 4) return jz_err_fmt(IDB_STRING_ERR, "%s length exceeds this architecture", name);
    const u8 *p;
    JZ_TRY(idb_tree__page__Decoder__take(d, len, name, &p));
    *out = jz_bytes_from(p, len);
    return JZ_OK;
}

static jz_err idb_tree__page__Decoder__byte_strings(idb_tree_Decoder *d, const char *name, idb_tree_KeyVec *out) {
    jz_str count_name = jz_format("%s count", name);
    u32 count;
    jz_str_push_char(&count_name, '\0');
    jz_err e = idb_tree__page__Decoder__count(d, count_name.ptr, 4, &count);
    jz_str_drop(&count_name);
    JZ_TRY(e);
    memset(out, 0, sizeof *out);
    jz_vec_reserve(out, count, sizeof(jz_bytes));
    for (u32 i = 0; i < count; i++) {
        jz_bytes v;
        e = idb_tree__page__Decoder__bytes(d, name, &v);
        if (e.code) {
            jz_vec_drop(out, sizeof(jz_bytes), bytes_drop);
            return e;
        }
        jz_vec_push(out, &v, sizeof v);
    }
    return JZ_OK;
}

static jz_err idb_tree__page__Decoder__finish(idb_tree_Decoder *d) {
    if (d->remaining == 0) return JZ_OK;
    return str_err("trailing page payload bytes");
}

static jz_err idb_tree__page__decode_value_cell(idb_tree_Decoder *d, idb_tree_ValueCell *out) {
    u8 tag;
    JZ_TRY(idb_tree__page__Decoder__u8(d, "value-cell tag", &tag));
    memset(out, 0, sizeof *out);
    switch (tag) {
    case VALUE_INLINE_TAG:
        out->tag = IDB_VALUE_INLINE;
        return idb_tree__page__Decoder__bytes(d, "inline value", &out->inline_bytes);
    case VALUE_OVERFLOW_TAG: {
        u64 head, encoded_len;
        JZ_TRY(idb_tree__page__Decoder__u64(d, "overflow value head", &head));
        JZ_TRY(idb_tree__page__Decoder__u64(d, "overflow value length", &encoded_len));
        if (encoded_len == 0) return str_err("overflow value length must not be zero");
        out->tag = IDB_VALUE_OVERFLOW;
        out->head = head;
        out->len = encoded_len;
        return JZ_OK;
    }
    default: return jz_err_fmt(IDB_STRING_ERR, "unknown value-cell tag %u", (unsigned)tag);
    }
}

jz_err idb_tree__page__decode_page(const u8 *bytes, u32 len, idb_tree_Page *page) {
    if (len < HEADER_LEN || memcmp(bytes, MAGIC, sizeof MAGIC) != 0) return str_err("bad magic");
    if (bytes[sizeof MAGIC] != FORMAT_VERSION) return jz_err_fmt(IDB_STRING_ERR, "unsupported format version %u", (unsigned)bytes[sizeof MAGIC]);
    u32 checksum_offset = sizeof MAGIC + 1;
    u64 expected = 0;
    for (int i = 7; i >= 0; i--) expected = expected << 8 | bytes[checksum_offset + i];
    const u8 *payload = bytes + HEADER_LEN;
    u32 payload_len = len - HEADER_LEN;
    if (xxhash_rust__xxh3__xxh3_64(payload, payload_len) != expected) return str_err("checksum mismatch");

    idb_tree_Decoder d = {payload, payload_len};
    memset(page, 0, sizeof *page);
    u8 tag;
    JZ_TRY(idb_tree__page__Decoder__u8(&d, "page tag", &tag));
    jz_err e = JZ_OK;
    switch (tag) {
    case PAGE_LEAF_TAG: {
        page->tag = IDB_PAGE_LEAF;
        u32 count;
        JZ_TRY(idb_tree__page__Decoder__count(&d, "leaf entry count", 9, &count));
        jz_vec_reserve(&page->entries, count, sizeof(idb_tree_LeafEntry));
        for (u32 i = 0; i < count; i++) {
            idb_tree_LeafEntry entry;
            if ((e = idb_tree__page__Decoder__bytes(&d, "leaf key", &entry.key)).code) goto fail;
            if ((e = idb_tree__page__decode_value_cell(&d, &entry.value)).code) {
                jz_bytes_drop(&entry.key);
                goto fail;
            }
            jz_vec_push(&page->entries, &entry, sizeof entry);
        }
        e = validate_strictly_increasing(page->entries.ptr, page->entries.len, sizeof(idb_tree_LeafEntry), "leaf keys");
        if (e.code) goto fail;
        break;
    }
    case PAGE_INTERNAL_TAG: {
        page->tag = IDB_PAGE_INTERNAL;
        if ((e = idb_tree__page__Decoder__byte_strings(&d, "internal keys", &page->keys)).code) goto fail;
        u32 child_count;
        if ((e = idb_tree__page__Decoder__count(&d, "internal child count", 8, &child_count)).code) goto fail;
        jz_vec_reserve(&page->children, child_count, sizeof(idb_tree_PageId));
        for (u32 i = 0; i < child_count; i++) {
            u64 child;
            if ((e = idb_tree__page__Decoder__u64(&d, "internal child page ID", &child)).code) goto fail;
            jz_vec_push(&page->children, &child, sizeof child);
        }
        if ((e = idb_tree__page__validate_internal_shape(&page->keys, &page->children)).code) goto fail;
        break;
    }
    case PAGE_OVERFLOW_TAG: {
        page->tag = IDB_PAGE_OVERFLOW;
        u8 next_tag;
        if ((e = idb_tree__page__Decoder__u8(&d, "overflow next tag", &next_tag)).code) goto fail;
        switch (next_tag) {
        case OPTION_NONE_TAG: page->has_next = false; break;
        case OPTION_SOME_TAG:
            page->has_next = true;
            if ((e = idb_tree__page__Decoder__u64(&d, "overflow next page ID", &page->next)).code) goto fail;
            break;
        default: e = jz_err_fmt(IDB_STRING_ERR, "unknown overflow next tag %u", (unsigned)next_tag); goto fail;
        }
        if ((e = idb_tree__page__Decoder__bytes(&d, "overflow page bytes", &page->bytes)).code) goto fail;
        if (page->bytes.len == 0) {
            e = str_err("overflow page bytes must not be empty");
            goto fail;
        }
        break;
    }
    default: return jz_err_fmt(IDB_STRING_ERR, "unknown page tag %u", (unsigned)tag);
    }
    if ((e = idb_tree__page__Decoder__finish(&d)).code) goto fail;
    return JZ_OK;
fail:
    idb_tree__page__Page__drop(page);
    return e;
}
