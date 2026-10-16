/* crates/idb-tree/src/web.rs: wasm adapter over the TypeScript IndexedDB page store */
#include "idb_tree/idb_tree.h"
#include "jazz_wasm/wbg.h"

/* #[wasm_bindgen] extern "C" { type IndexedDbPageStoreHandle; ... } */
JZ_IMPORT("idbps_claimTreeOwnership") u32 idbps_claimTreeOwnership(jz_js self, f64 *out, jz_js *err);
JZ_IMPORT("idbps_releaseTreeOwnership") void idbps_releaseTreeOwnership(jz_js self, f64 token);
JZ_IMPORT("idbps_isTreeOwnershipActive") u32 idbps_isTreeOwnershipActive(jz_js self, f64 token, u32 *out, jz_js *err);
JZ_IMPORT("idbps_canReclaimObsoletePages") u32 idbps_canReclaimObsoletePages(jz_js self);
JZ_IMPORT("idbps_metadata") jz_js idbps_metadata(jz_js self);
JZ_IMPORT("idbps_readPage") jz_js idbps_readPage(jz_js self, f64 page_id);
JZ_IMPORT("idbps_readPages") jz_js idbps_readPages(jz_js self, jz_js page_ids);
JZ_IMPORT("idbps_commitPages") jz_js idbps_commitPages(jz_js self, f64 expected_generation, u32 page_size, f64 root_page_id,
                                                       f64 next_page_id, jz_js page_ids, jz_js page_bytes, jz_js deleted_page_ids,
                                                       jz_js tree_token);

struct idb_tree_IndexedDbPageStore {
    jz_js handle;
    bool has_tree_token; /* Cell<Option<f64>> */
    f64 tree_token;
};

static const idb_tree_PageStoreVtbl IDB_PAGE_STORE_VT;

static idb_tree_IndexedDbPageStore *web_store_new(jz_js handle) {
    idb_tree_IndexedDbPageStore *s = JZ_NEW(idb_tree_IndexedDbPageStore);
    s->handle = handle;
    s->has_tree_token = false;
    return s;
}

idb_tree_PageStore idb_tree__web__IndexedDbPageStore__from_js(jz_js handle) {
    idb_tree_PageStore ps = {&IDB_PAGE_STORE_VT, web_store_new(handle)};
    return ps;
}

static void *idb_tree__web__IndexedDbPageStore__Clone__clone(void *self) {
    idb_tree_IndexedDbPageStore *s = (idb_tree_IndexedDbPageStore *)self;
    idb_tree_IndexedDbPageStore *c = web_store_new(wbg_clone_ref(s->handle));
    c->has_tree_token = s->has_tree_token;
    c->tree_token = s->tree_token;
    return c;
}

static void idb_tree__web__IndexedDbPageStore__drop(void *self) {
    idb_tree_IndexedDbPageStore *s = (idb_tree_IndexedDbPageStore *)self;
    jz_js_drop(s->handle);
    jz_free(s);
}

static jz_err idb_tree__web__js_error(jz_js value) {
    jz_err e;
    if (wbg_is_error(value)) {
        char *p;
        u32 n;
        wbg_error_message(value, &p, &n);
        jz_str s = {p, n, n};
        e = jz_err_fmt(IDB_STRING_ERR, "%.*s", (int)n, p);
        jz_str_drop(&s);
    } else {
        char *p;
        u32 n;
        wbg_debug_string(value, &p, &n); /* format!("{value:?}") */
        jz_str s = {p, n, n};
        e = jz_err_fmt(IDB_STRING_ERR, "%.*s", (int)n, p);
        jz_str_drop(&s);
    }
    jz_js_drop(value);
    return e;
}

static bool idb_tree__web__IndexedDbPageStore__has_method(const idb_tree_IndexedDbPageStore *s, const char *name) {
    jz_js key = wbg_str(name), value;
    u32 threw = wbg_reflect_get(s->handle, key, &value);
    jz_js_drop(key);
    if (threw) {
        jz_js_drop(value);
        return false;
    }
    bool is_fn = wbg_is_function(value) != 0;
    jz_js_drop(value);
    return is_fn;
}

static bool idb_tree__web__IndexedDbPageStore__supports_tree_ownership(const idb_tree_IndexedDbPageStore *s) {
    static const char *const names[3] = {"claimTreeOwnership", "releaseTreeOwnership", "isTreeOwnershipActive"};
    for (u32 i = 0; i < 3; i++)
        if (!idb_tree__web__IndexedDbPageStore__has_method(s, names[i])) return false;
    return true;
}

static bool tree_ownership_active(jz_js handle, f64 token) {
    u32 active;
    jz_js err;
    if (idbps_isTreeOwnershipActive(handle, token, &active, &err)) {
        jz_js_drop(err);
        return false; /* unwrap_or(false) */
    }
    return active != 0;
}

typedef struct web_ownership_ctx { jz_js live_handle; jz_js release_handle; f64 token; } web_ownership_ctx;
static bool web_ownership_is_live(void *ctx) {
    web_ownership_ctx *c = (web_ownership_ctx *)ctx;
    return tree_ownership_active(c->live_handle, c->token);
}
static void web_ownership_release(void *ctx) {
    web_ownership_ctx *c = (web_ownership_ctx *)ctx;
    idbps_releaseTreeOwnership(c->release_handle, c->token);
}
static void web_ownership_ctx_drop(void *ctx) {
    web_ownership_ctx *c = (web_ownership_ctx *)ctx;
    jz_js_drop(c->live_handle);
    jz_js_drop(c->release_handle);
    jz_free(c);
}

static jz_err idb_tree__web__IndexedDbPageStore__PageStore__claim_tree_ownership(void *self, idb_tree_TreeOwnership *out) {
    idb_tree_IndexedDbPageStore *s = (idb_tree_IndexedDbPageStore *)self;
    if (!idb_tree__web__IndexedDbPageStore__supports_tree_ownership(s)) {
        idb_tree__store__TreeOwnership__Default__default(out);
        return JZ_OK;
    }
    f64 token;
    jz_js err;
    if (idbps_claimTreeOwnership(s->handle, &token, &err)) return idb_tree__web__js_error(err);
    s->has_tree_token = true;
    s->tree_token = token;
    web_ownership_ctx *c = JZ_NEW(web_ownership_ctx);
    c->live_handle = wbg_clone_ref(s->handle);
    c->release_handle = wbg_clone_ref(s->handle);
    c->token = token;
    idb_tree__store__TreeOwnership__revocable(out, web_ownership_is_live, web_ownership_release, c, web_ownership_ctx_drop);
    return JZ_OK;
}

static bool idb_tree__web__IndexedDbPageStore__PageStore__can_reclaim_obsolete_pages(void *self) {
    idb_tree_IndexedDbPageStore *s = (idb_tree_IndexedDbPageStore *)self;
    if (!s->has_tree_token) return false;
    return tree_ownership_active(s->handle, s->tree_token) && idbps_canReclaimObsoletePages(s->handle) != 0;
}

static jz_err idb_tree__web__number_to_page_id(bool has, f64 value, const char *name, u64 *out) {
    if (!has) return jz_err_fmt(IDB_STRING_ERR, "%s is not a number", name);
    if (!__builtin_isfinite(value) || value < 0.0 || value != __builtin_trunc(value) || value > 9007199254740991.0)
        return jz_err_fmt(IDB_STRING_ERR, "%s is not a safe non-negative integer", name);
    *out = (u64)value;
    return JZ_OK;
}

static jz_err idb_tree__web__page_id_to_f64(u64 value, f64 *out) {
    if (value > 9007199254740991ull)
        return jz_err_new(IDB_STRING_ERR, "IDBTree page id exceeds JavaScript's safe integer range");
    *out = (f64)value;
    return JZ_OK;
}

static jz_err reflect_field(jz_js value, const char *name, jz_js *out) {
    jz_js key = wbg_str(name);
    u32 threw = wbg_reflect_get(value, key, out);
    jz_js_drop(key);
    if (threw) return idb_tree__web__js_error(*out);
    return JZ_OK;
}

static jz_err idb_tree__web__integer_field(jz_js value, const char *name, u64 *out) {
    jz_js field;
    JZ_TRY(reflect_field(value, name, &field));
    f64 v = 0;
    bool has = wbg_as_f64(field, &v) != 0;
    jz_js_drop(field);
    return idb_tree__web__number_to_page_id(has, v, name, out);
}

static jz_err idb_tree__web__nullable_integer_field(jz_js value, const char *name, bool *some, u64 *out) {
    jz_js field;
    JZ_TRY(reflect_field(value, name, &field));
    if (wbg_is_nullish(field)) {
        jz_js_drop(field);
        *some = false;
        return JZ_OK;
    }
    f64 v = 0;
    bool has = wbg_as_f64(field, &v) != 0;
    jz_js_drop(field);
    *some = true;
    return idb_tree__web__number_to_page_id(has, v, name, out);
}

static jz_err idb_tree__web__metadata_from_js(jz_js value, idb_tree_Metadata *out) {
    u64 page_size;
    JZ_TRY(idb_tree__web__integer_field(value, "pageSize", &page_size));
    if (page_size > (u64)(usize)-1) return jz_err_new(IDB_STRING_ERR, "pageSize does not fit usize");
    out->page_size = (usize)page_size;
    JZ_TRY(idb_tree__web__integer_field(value, "generation", &out->generation));
    JZ_TRY(idb_tree__web__nullable_integer_field(value, "rootPageId", &out->has_root_page_id, &out->root_page_id));
    JZ_TRY(idb_tree__web__integer_field(value, "nextPageId", &out->next_page_id));
    return JZ_OK;
}

static jz_err idb_tree__web__IndexedDbPageStore__PageStore__load_metadata(void *self, bool *some, idb_tree_Metadata *out) {
    idb_tree_IndexedDbPageStore *s = (idb_tree_IndexedDbPageStore *)self;
    jz_js value;
    if (wbg_await(idbps_metadata(s->handle), &value)) return idb_tree__web__js_error(value);
    if (wbg_is_nullish(value)) {
        jz_js_drop(value);
        *some = false;
        return JZ_OK;
    }
    jz_err e = idb_tree__web__metadata_from_js(value, out);
    jz_js_drop(value);
    *some = !e.code;
    return e;
}

static jz_err idb_tree__web__IndexedDbPageStore__PageStore__read_page(void *self, idb_tree_PageId page_id, idb_tree_PageSlot *out) {
    idb_tree_IndexedDbPageStore *s = (idb_tree_IndexedDbPageStore *)self;
    f64 id;
    JZ_TRY(idb_tree__web__page_id_to_f64(page_id, &id));
    jz_js value;
    if (wbg_await(idbps_readPage(s->handle, id), &value)) return idb_tree__web__js_error(value);
    if (wbg_is_nullish(value)) {
        jz_js_drop(value);
        out->some = false;
        return JZ_OK;
    }
    out->some = true;
    wbg_uint8array_to_vec(value, &out->bytes);
    jz_js_drop(value);
    return JZ_OK;
}

static jz_err idb_tree__web__IndexedDbPageStore__PageStore__read_pages(void *self, const idb_tree_PageId *page_ids, u32 n,
                                                                       idb_tree_PageSlotVec *out) {
    idb_tree_IndexedDbPageStore *s = (idb_tree_IndexedDbPageStore *)self;
    memset(out, 0, sizeof *out);
    if (!idb_tree__web__IndexedDbPageStore__has_method(s, "readPages")) {
        jz_vec_reserve(out, n, sizeof(idb_tree_PageSlot));
        for (u32 i = 0; i < n; i++) {
            idb_tree_PageSlot slot;
            jz_err e = idb_tree__web__IndexedDbPageStore__PageStore__read_page(self, page_ids[i], &slot);
            if (e.code) {
                jz_vec_drop(out, sizeof(idb_tree_PageSlot), idb_tree__store__PageSlot__drop);
                return e;
            }
            jz_vec_push(out, &slot, sizeof slot);
        }
        return JZ_OK;
    }
    jz_js ids = wbg_array_new();
    for (u32 i = 0; i < n; i++) {
        f64 id;
        jz_err e = idb_tree__web__page_id_to_f64(page_ids[i], &id);
        if (e.code) {
            jz_js_drop(ids);
            return e;
        }
        jz_js v = wbg_number_new(id);
        wbg_array_push(ids, v);
        jz_js_drop(v);
    }
    jz_js value;
    u32 rejected = wbg_await(idbps_readPages(s->handle, ids), &value);
    jz_js_drop(ids);
    if (rejected) return idb_tree__web__js_error(value);
    if (!wbg_is_array(value)) {
        jz_js_drop(value);
        return jz_err_new(IDB_STRING_ERR, "readPages did not return an array");
    }
    u32 len = wbg_array_length(value);
    jz_vec_reserve(out, len, sizeof(idb_tree_PageSlot));
    for (u32 i = 0; i < len; i++) {
        jz_js page = wbg_array_get(value, i);
        idb_tree_PageSlot slot;
        if (wbg_is_nullish(page)) {
            slot.some = false;
        } else {
            slot.some = true;
            wbg_uint8array_to_vec(page, &slot.bytes);
        }
        jz_js_drop(page);
        jz_vec_push(out, &slot, sizeof slot);
    }
    jz_js_drop(value);
    return JZ_OK;
}

static jz_err idb_tree__web__IndexedDbPageStore__PageStore__commit(void *self, const idb_tree_Commit *commit, idb_tree_Metadata *out) {
    idb_tree_IndexedDbPageStore *s = (idb_tree_IndexedDbPageStore *)self;
    jz_js page_ids = wbg_array_new();
    jz_js page_bytes = wbg_array_new();
    jz_js deleted_page_ids = wbg_array_new();
    jz_err e = JZ_OK;
    f64 id;
    for (u32 i = 0; i < commit->pages.len; i++) {
        if ((e = idb_tree__web__page_id_to_f64(commit->pages.ptr[i].id, &id)).code) goto out;
        jz_js v = wbg_number_new(id);
        wbg_array_push(page_ids, v);
        jz_js_drop(v);
        jz_js b = wbg_uint8array_from(commit->pages.ptr[i].bytes.ptr, commit->pages.ptr[i].bytes.len);
        wbg_array_push(page_bytes, b);
        jz_js_drop(b);
    }
    for (u32 i = 0; i < commit->deleted_page_ids.len; i++) {
        if ((e = idb_tree__web__page_id_to_f64(commit->deleted_page_ids.ptr[i], &id)).code) goto out;
        jz_js v = wbg_number_new(id);
        wbg_array_push(deleted_page_ids, v);
        jz_js_drop(v);
    }
    {
        f64 root_page_id = -1.0;
        if (commit->metadata.has_root_page_id && (e = idb_tree__web__page_id_to_f64(commit->metadata.root_page_id, &root_page_id)).code)
            goto out;
        f64 expected_generation, next_page_id;
        if ((e = idb_tree__web__page_id_to_f64(commit->expected_generation, &expected_generation)).code) goto out;
        if ((u64)commit->metadata.page_size > 0xffffffffull) {
            e = jz_err_new(IDB_STRING_ERR, "page size does not fit u32");
            goto out;
        }
        if ((e = idb_tree__web__page_id_to_f64(commit->metadata.next_page_id, &next_page_id)).code) goto out;
        jz_js token = s->has_tree_token ? wbg_number_new(s->tree_token) : WBG_UNDEFINED;
        jz_js promise = idbps_commitPages(s->handle, expected_generation, (u32)commit->metadata.page_size, root_page_id, next_page_id,
                                          page_ids, page_bytes, deleted_page_ids, token);
        jz_js value;
        u32 rejected = wbg_await(promise, &value);
        if (rejected) {
            e = idb_tree__web__js_error(value);
            goto out;
        }
        e = idb_tree__web__metadata_from_js(value, out);
        jz_js_drop(value);
    }
out:
    jz_js_drop(page_ids);
    jz_js_drop(page_bytes);
    jz_js_drop(deleted_page_ids);
    return e;
}

static const idb_tree_PageStoreVtbl IDB_PAGE_STORE_VT = {
    idb_tree__web__IndexedDbPageStore__PageStore__claim_tree_ownership,
    idb_tree__web__IndexedDbPageStore__PageStore__can_reclaim_obsolete_pages,
    idb_tree__web__IndexedDbPageStore__PageStore__load_metadata,
    idb_tree__web__IndexedDbPageStore__PageStore__read_page,
    idb_tree__web__IndexedDbPageStore__PageStore__read_pages,
    idb_tree__web__IndexedDbPageStore__PageStore__commit,
    idb_tree__web__IndexedDbPageStore__Clone__clone,
    idb_tree__web__IndexedDbPageStore__drop,
};
