/* swb.c: the serde_wasm_bindgen conversions crates/jazz-wasm/src/lib.rs relies
 * on (from_value::<T> / to_value(&T)), written out as explicit decoders and
 * encoders of the same shapes the derived serde impls produce.
 *
 * Deserialize: the JsValue is first walked into the serde data model
 * (jz_json: undefined/null -> unit, number -> integer when it is a safe
 * integer, BigInt -> u64, Uint8Array -> byte seq, Array -> seq,
 * Map/Object -> map), then the derived Deserialize impl of each type runs
 * over it. serde_wasm_bindgen is not human-readable, so Uuid is bytes.
 * Serialize: objects are built directly (serialize_maps_as_objects), Option
 * None is undefined, u64 is a Number and errors above 2^53 - 1, bytes become
 * a Uint8Array and unit variants become strings. */
#include <stdarg.h>
#include <stdio.h>
#include "jazz_wasm/jazz_wasm.h"

#define MAX_SAFE_INTEGER 9007199254740991.0

jz_err swb__error(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    char buf[512];
    vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    return jz_err_new(SWB_ERR, buf);
}

/* ------------------------------------------------------------------ */
/* JsValue -> serde data model                                         */
/* ------------------------------------------------------------------ */
static jz_err swb_number(f64 n, jz_json **out) {
    /* deserialize_any on a Number: integral and safe -> visit_i64/u64, else f64 */
    if (n == (f64)(i64)n && n >= -MAX_SAFE_INTEGER && n <= MAX_SAFE_INTEGER) {
        *out = n >= 0 ? jz_json_u64((u64)n) : jz_json_i64((i64)n);
    } else {
        *out = jz_json_f64(n);
    }
    return JZ_OK;
}

static jz_err swb_from_value_depth(jz_js v, u32 depth, jz_json **out);

static jz_err swb_object_to_map(jz_js obj, u32 depth, jz_json **out) {
    jz_js keys = wbg_object_keys(obj);
    u32 n = wbg_array_length(keys);
    jz_json *map = jz_json_obj();
    for (u32 i = 0; i < n; i++) {
        jz_js key = wbg_array_get(keys, i);
        jz_str key_str = {0};
        if (!wbg_take_string(key, &key_str)) {
            jz_js_drop(key);
            jz_js_drop(keys);
            jz_json_free(map);
            return swb__error("invalid type: object key, expected a string");
        }
        jz_js val = 0;
        if (wbg_reflect_get(obj, key, &val)) {
            jz_js_drop(key);
            jz_js_drop(keys);
            jz_str_drop(&key_str);
            jz_json_free(map);
            return wbg_err_from_js(val);
        }
        jz_js_drop(key);
        jz_json *child = NULL;
        jz_err e = swb_from_value_depth(val, depth + 1, &child);
        jz_js_drop(val);
        if (e.code) {
            jz_str_drop(&key_str);
            jz_js_drop(keys);
            jz_json_free(map);
            return e;
        }
        /* serde_json's map: last write wins */
        jz_json *old = NULL;
        if (jz_btree_insert(&map->v.obj, &key_str, &child, &old)) {
            jz_str_drop(&key_str);
            jz_json_free(old);
        }
    }
    jz_js_drop(keys);
    *out = map;
    return JZ_OK;
}

static jz_err swb_map_to_map(jz_js m, u32 depth, jz_json **out) {
    jz_js entries = wbg_map_entries(m);
    u32 n = wbg_array_length(entries);
    jz_json *map = jz_json_obj();
    for (u32 i = 0; i < n; i++) {
        jz_js pair = wbg_array_get(entries, i);
        jz_js key = wbg_array_get(pair, 0);
        jz_js val = wbg_array_get(pair, 1);
        jz_js_drop(pair);
        jz_str key_str = {0};
        bool is_str = wbg_take_string(key, &key_str);
        jz_js_drop(key);
        if (!is_str) {
            jz_js_drop(val);
            jz_js_drop(entries);
            jz_json_free(map);
            return swb__error("invalid type: map key, expected a string");
        }
        jz_json *child = NULL;
        jz_err e = swb_from_value_depth(val, depth + 1, &child);
        jz_js_drop(val);
        if (e.code) {
            jz_str_drop(&key_str);
            jz_js_drop(entries);
            jz_json_free(map);
            return e;
        }
        jz_json *old = NULL;
        if (jz_btree_insert(&map->v.obj, &key_str, &child, &old)) {
            jz_str_drop(&key_str);
            jz_json_free(old);
        }
    }
    jz_js_drop(entries);
    *out = map;
    return JZ_OK;
}

static jz_err swb_from_value_depth(jz_js v, u32 depth, jz_json **out) {
    if (depth > 128) return swb__error("recursion limit exceeded");
    switch (wbg_typeof(v)) {
    case WBG_T_UNDEFINED:
    case WBG_T_NULL:
        *out = jz_json_null();
        return JZ_OK;
    case WBG_T_BOOL:
        *out = jz_json_bool(wbg_as_bool(v) == 1);
        return JZ_OK;
    case WBG_T_NUMBER: {
        f64 n = 0;
        wbg_as_f64(v, &n);
        return swb_number(n, out);
    }
    case WBG_T_BIGINT: {
        u64 u = 0;
        if (!wbg_bigint_to_u64(v, &u)) return swb__error("invalid value: BigInt, expected a 64-bit integer");
        *out = jz_json_u64(u);
        return JZ_OK;
    }
    case WBG_T_STRING: {
        jz_str s = {0};
        wbg_take_string(v, &s);
        *out = jz_json_str_owned(s);
        return JZ_OK;
    }
    case WBG_T_UINT8ARRAY: {
        /* deserialize_any on a Uint8Array visits bytes; the data model keeps
         * them as a byte seq so Vec<u8> / Uuid / serde_json decoders see one shape */
        jz_bytes b = {0};
        wbg_uint8array_to_vec(v, &b);
        jz_json *arr = jz_json_arr();
        for (u32 i = 0; i < b.len; i++) jz_json_arr_push(arr, jz_json_u64(b.ptr[i]));
        jz_bytes_drop(&b);
        *out = arr;
        return JZ_OK;
    }
    case WBG_T_ARRAY: {
        u32 n = wbg_array_length(v);
        jz_json *arr = jz_json_arr();
        for (u32 i = 0; i < n; i++) {
            jz_js item = wbg_array_get(v, i);
            jz_json *child = NULL;
            jz_err e = swb_from_value_depth(item, depth + 1, &child);
            jz_js_drop(item);
            if (e.code) {
                jz_json_free(arr);
                return e;
            }
            jz_json_arr_push(arr, child);
        }
        *out = arr;
        return JZ_OK;
    }
    case WBG_T_MAP:
        return swb_map_to_map(v, depth, out);
    case WBG_T_OBJECT:
        return swb_object_to_map(v, depth, out);
    case WBG_T_FUNCTION:
    default: {
        char *p = NULL;
        u32 n = 0;
        wbg_debug_string(v, &p, &n);
        jz_err e = swb__error("invalid type: %.*s, expected any value", (int)n, p ? p : "");
        jz_free(p);
        return e;
    }
    }
}

jz_err swb__from_value(jz_js v, jz_json **out) { return swb_from_value_depth(v, 0, out); }

/* ------------------------------------------------------------------ */
/* Deserialize helpers                                                 */
/* ------------------------------------------------------------------ */
static const char *swb_kind_name(const jz_json *v) {
    switch (v->kind) {
    case JZ_JSON_NULL: return "unit value";
    case JZ_JSON_BOOL: return "boolean";
    case JZ_JSON_U64:
    case JZ_JSON_I64: return "integer";
    case JZ_JSON_F64: return "floating point";
    case JZ_JSON_STR: return "string";
    case JZ_JSON_ARR: return "sequence";
    case JZ_JSON_OBJ: return "map";
    }
    return "value";
}

static jz_err swb_invalid_type(const jz_json *v, const char *expected) {
    return swb__error("invalid type: %s, expected %s", swb_kind_name(v), expected);
}

static jz_err swb_expect_struct(const jz_json *v, const char *name) {
    if (v->kind != JZ_JSON_OBJ) return swb__error("invalid type: %s, expected struct %s", swb_kind_name(v), name);
    return JZ_OK;
}

static jz_err swb_field(const jz_json *obj, const char *key, const jz_json **out) {
    const jz_json *f = jz_json_obj_get(obj, key);
    if (!f) return swb__error("missing field `%s`", key);
    *out = f;
    return JZ_OK;
}

static jz_err swb_u64(const jz_json *v, u64 *out) {
    switch (v->kind) {
    case JZ_JSON_U64:
        *out = v->v.u;
        return JZ_OK;
    case JZ_JSON_I64:
        if (v->v.i < 0) return swb__error("invalid value: integer `%lld`, expected u64", (long long)v->v.i);
        *out = (u64)v->v.i;
        return JZ_OK;
    case JZ_JSON_F64:
        return swb__error("invalid type: floating point `%g`, expected u64", v->v.f);
    default:
        return swb_invalid_type(v, "u64");
    }
}

static jz_err swb_u64_field(const jz_json *obj, const char *key, u64 *out) {
    const jz_json *f = NULL;
    JZ_TRY(swb_field(obj, key, &f));
    return swb_u64(f, out);
}

static jz_err swb_string(const jz_json *v, jz_str *out) {
    if (v->kind != JZ_JSON_STR) return swb_invalid_type(v, "a string");
    *out = jz_str_clone(&v->v.s);
    return JZ_OK;
}

static jz_err swb_string_field(const jz_json *obj, const char *key, jz_str *out) {
    const jz_json *f = NULL;
    JZ_TRY(swb_field(obj, key, &f));
    return swb_string(f, out);
}

/* Vec<u8>: a byte buffer or a sequence of u8 */
static jz_err swb_bytes(const jz_json *v, jz_bytes *out) {
    if (v->kind != JZ_JSON_ARR) return swb_invalid_type(v, "a sequence");
    memset(out, 0, sizeof *out);
    for (u32 i = 0; i < v->v.arr.len; i++) {
        u64 b = 0;
        jz_err e = swb_u64(v->v.arr.ptr[i], &b);
        if (!e.code && b > 255) e = swb__error("invalid value: integer `%llu`, expected u8", (unsigned long long)b);
        if (e.code) {
            jz_bytes_drop(out);
            return e;
        }
        jz_bytes_push_u8(out, (u8)b);
    }
    return JZ_OK;
}

/* Uuid (non-human-readable): 16 bytes */
static jz_err swb_uuid(const jz_json *v, jz_uuid *out) {
    jz_bytes b = {0};
    JZ_TRY(swb_bytes(v, &b));
    if (b.len != 16) {
        jz_err e = swb__error("invalid length %u, expected 16 bytes", b.len);
        jz_bytes_drop(&b);
        return e;
    }
    memcpy(out->b, b.ptr, 16);
    jz_bytes_drop(&b);
    return JZ_OK;
}

static jz_err swb_uuid_field(const jz_json *obj, const char *key, jz_uuid *out) {
    const jz_json *f = NULL;
    JZ_TRY(swb_field(obj, key, &f));
    return swb_uuid(f, out);
}

/* externally tagged enum: "Variant" (unit) or { "Variant": payload } */
static jz_err swb_variant(const jz_json *v, const char *enum_name, const char *const *names, u32 n, u32 *idx, const jz_json **payload) {
    const jz_str *tag = NULL;
    *payload = NULL;
    if (v->kind == JZ_JSON_STR) {
        tag = &v->v.s;
    } else if (v->kind == JZ_JSON_OBJ) {
        if (v->v.obj.len != 1) return swb__error("invalid type: map, expected enum %s with a single key", enum_name);
        void *k = NULL, *val = NULL;
        jz_btree_first(&v->v.obj, &k, &val);
        tag = (const jz_str *)k;
        *payload = *(jz_json **)val;
    } else {
        return swb__error("invalid type: %s, expected enum %s", swb_kind_name(v), enum_name);
    }
    for (u32 i = 0; i < n; i++) {
        if (jz_str_eq_cstr(tag, names[i])) {
            *idx = i;
            return JZ_OK;
        }
    }
    jz_str expected = {0};
    for (u32 i = 0; i < n; i++) jz_str_appendf(&expected, "%s`%s`", i ? ", " : "", names[i]);
    jz_err e = swb__error("unknown variant `%.*s`, expected one of %.*s", (int)tag->len, tag->ptr, (int)expected.len, expected.ptr);
    jz_str_drop(&expected);
    return e;
}

/* internally tagged enum: { "kind": "variant", ...fields } */
static jz_err swb_internal_tag(const jz_json *v, const char *tag_key, const char *enum_name, const char *const *names, u32 n, u32 *idx) {
    if (v->kind != JZ_JSON_OBJ) return swb__error("invalid type: %s, expected internally tagged enum %s", swb_kind_name(v), enum_name);
    const jz_json *tag = jz_json_obj_get(v, tag_key);
    if (!tag) return swb__error("missing field `%s`", tag_key);
    if (tag->kind != JZ_JSON_STR) return swb_invalid_type(tag, "variant identifier");
    for (u32 i = 0; i < n; i++) {
        if (jz_str_eq_cstr(&tag->v.s, names[i])) {
            *idx = i;
            return JZ_OK;
        }
    }
    jz_str expected = {0};
    for (u32 i = 0; i < n; i++) jz_str_appendf(&expected, "%s`%s`", i ? ", " : "", names[i]);
    jz_err e = swb__error("unknown variant `%.*s`, expected one of %.*s", (int)tag->v.s.len, tag->v.s.ptr, (int)expected.len, expected.ptr);
    jz_str_drop(&expected);
    return e;
}

/* ------------------------------------------------------------------ */
/* Typed Deserialize impls                                             */
/* ------------------------------------------------------------------ */
jz_err swb__TxId__deserialize(const jz_json *v, jazz_model_TxId *out) {
    JZ_TRY(swb_expect_struct(v, "TxId"));
    JZ_TRY(swb_u64_field(v, "time", &out->time));
    return swb_uuid_field(v, "node", &out->node);
}

/* BranchSelector { values: BTreeMap<String, BranchColumnValue(Vec<u8>)> } */
jz_err swb__BranchSelector__deserialize(const jz_json *v, jazz_model_BranchSelector *out) {
    JZ_TRY(swb_expect_struct(v, "BranchSelector"));
    const jz_json *values = NULL;
    JZ_TRY(swb_field(v, "values", &values));
    if (values->kind != JZ_JSON_OBJ) return swb_invalid_type(values, "a map");
    jz_btree_init(&out->values, &jazz_model__branch__STRING_BYTES_BTREE);
    jz_str *k = NULL;
    jz_json **val = NULL;
    JZ_BTREE_FOR(&values->v.obj, k, val) {
        jz_bytes bytes = {0};
        jz_err e = swb_bytes(*val, &bytes);
        if (e.code) {
            jz_btree_drop(&out->values);
            return e;
        }
        jz_str key = jz_str_clone(k);
        jz_btree_insert(&out->values, &key, &bytes, NULL);
    }
    return JZ_OK;
}

static void swb_snapshot_drop(jazz_model_SnapshotRef *s) { jz_vec_drop(&s->dots, sizeof(jazz_model_TxId), NULL); }

/* SnapshotRef { owner, global_base, local_base, #[serde(default)] dots } */
jz_err swb__SnapshotRef__deserialize(const jz_json *v, jazz_model_SnapshotRef *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(swb_expect_struct(v, "SnapshotRef"));
    JZ_TRY(swb_uuid_field(v, "owner", &out->owner));
    JZ_TRY(swb_u64_field(v, "global_base", &out->global_base));
    JZ_TRY(swb_u64_field(v, "local_base", &out->local_base));
    const jz_json *dots = jz_json_obj_get(v, "dots");
    if (!dots) return JZ_OK;
    if (dots->kind != JZ_JSON_ARR) return swb_invalid_type(dots, "a sequence");
    for (u32 i = 0; i < dots->v.arr.len; i++) {
        jazz_model_TxId tx;
        jz_err e = swb__TxId__deserialize(dots->v.arr.ptr[i], &tx);
        if (e.code) {
            swb_snapshot_drop(out);
            return e;
        }
        jz_vec_push(&out->dots, &tx, sizeof tx);
    }
    return JZ_OK;
}

static const char *const BRANCH_VIEW_BASE_VARIANTS[] = {"Current", "Snapshot"};

/* enum BranchViewBase { Current(BranchSelector), Snapshot { branch, snapshot } } */
jz_err swb__BranchViewBase__deserialize(const jz_json *v, jazz_model_BranchViewBase *out) {
    memset(out, 0, sizeof *out);
    u32 idx = 0;
    const jz_json *payload = NULL;
    JZ_TRY(swb_variant(v, "BranchViewBase", BRANCH_VIEW_BASE_VARIANTS, 2, &idx, &payload));
    if (!payload) return swb__error("invalid type: unit variant, expected %s", idx == 0 ? "newtype variant" : "struct variant");
    out->tag = idx;
    if (idx == JAZZ_BASE_CURRENT) return swb__BranchSelector__deserialize(payload, &out->branch);
    JZ_TRY(swb_expect_struct(payload, "variant BranchViewBase::Snapshot"));
    const jz_json *branch = NULL, *snapshot = NULL;
    JZ_TRY(swb_field(payload, "branch", &branch));
    JZ_TRY(swb_field(payload, "snapshot", &snapshot));
    JZ_TRY(swb__BranchSelector__deserialize(branch, &out->branch));
    jz_err e = swb__SnapshotRef__deserialize(snapshot, &out->snapshot);
    if (e.code) jazz_model__model__branch__BranchSelector__drop(&out->branch);
    return e;
}

static const char *const READ_VIEW_SOURCE_VARIANTS[] = {"Current", "BranchView", "Snapshot"};

/* ReadViewSpec { source: ReadViewSourceSpec } */
jz_err swb__ReadViewSpec__deserialize(const jz_json *v, jazz_protocol_ReadViewSpec *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(swb_expect_struct(v, "ReadViewSpec"));
    const jz_json *source = NULL;
    JZ_TRY(swb_field(v, "source", &source));
    u32 idx = 0;
    const jz_json *payload = NULL;
    JZ_TRY(swb_variant(source, "ReadViewSourceSpec", READ_VIEW_SOURCE_VARIANTS, 3, &idx, &payload));
    out->source_tag = idx;
    if (idx == JAZZ_RVS_CURRENT) {
        if (payload && payload->kind != JZ_JSON_NULL) return swb_invalid_type(payload, "unit variant");
        return JZ_OK;
    }
    if (!payload) return swb__error("invalid type: unit variant, expected struct variant");
    if (idx == JAZZ_RVS_BRANCH_VIEW) {
        JZ_TRY(swb_expect_struct(payload, "variant ReadViewSourceSpec::BranchView"));
        const jz_json *head = NULL;
        JZ_TRY(swb_field(payload, "head", &head));
        JZ_TRY(swb__BranchSelector__deserialize(head, &out->head));
        const jz_json *base = jz_json_obj_get(payload, "base"); /* Option: missing / null = None */
        if (base && base->kind != JZ_JSON_NULL) {
            jz_err e = swb__BranchViewBase__deserialize(base, &out->base);
            if (e.code) {
                jazz_model__model__branch__BranchSelector__drop(&out->head);
                return e;
            }
            out->has_base = true;
        }
        return JZ_OK;
    }
    JZ_TRY(swb_expect_struct(payload, "variant ReadViewSourceSpec::Snapshot"));
    const jz_json *snapshot = NULL;
    JZ_TRY(swb_field(payload, "snapshot", &snapshot));
    return swb__SnapshotRef__deserialize(snapshot, &out->snapshot);
}

static const char *const LARGE_VALUE_PAGE_VARIANTS[] = {"bytes", "text_utf16", "text_utf8"};
static const char *const LARGE_VALUE_UPDATE_VARIANTS[] = {"splice", "json_set"};

static jz_err swb_large_value_page(const jz_json *v, jazz_db_LargeValueUpdatePage *out) {
    u32 idx = 0;
    JZ_TRY(swb_internal_tag(v, "kind", "LargeValueUpdatePage", LARGE_VALUE_PAGE_VARIANTS, 3, &idx));
    out->tag = idx;
    JZ_TRY(swb_u64_field(v, "from", &out->from));
    return swb_u64_field(v, "to", &out->to);
}

static void swb_splice_drop(void *p) { jz_bytes_drop(&((jazz_db_LargeValueUpdateSplice *)p)->insert); }
static void swb_json_set_edit_drop(void *p) {
    jazz_db_JsonSetEdit *e = (jazz_db_JsonSetEdit *)p;
    jz_str_drop(&e->at);
    if (e->value) jz_json_free(e->value);
}
static void swb_large_value_update_drop(void *p) {
    jazz_db_LargeValueUpdate *u = (jazz_db_LargeValueUpdate *)p;
    jz_str_drop(&u->column);
    jz_vec_drop(&u->splices, sizeof(jazz_db_LargeValueUpdateSplice), swb_splice_drop);
    jz_vec_drop(&u->edits, sizeof(jazz_db_JsonSetEdit), swb_json_set_edit_drop);
}

void swb__LargeValueUpdateVec__drop(jazz_db_LargeValueUpdateVec *v) {
    jz_vec_drop(v, sizeof(jazz_db_LargeValueUpdate), swb_large_value_update_drop);
}

static jz_err swb_large_value_update(const jz_json *v, jazz_db_LargeValueUpdate *out) {
    memset(out, 0, sizeof *out);
    u32 idx = 0;
    JZ_TRY(swb_internal_tag(v, "kind", "LargeValueUpdate", LARGE_VALUE_UPDATE_VARIANTS, 2, &idx));
    out->tag = idx;
    jz_err e = swb_string_field(v, "column", &out->column);
    if (e.code) return e;
    if (idx == JAZZ_LVU_SPLICE) {
        const jz_json *within = NULL, *splices = NULL;
        if ((e = swb_field(v, "within", &within)).code) goto fail;
        if ((e = swb_field(v, "splices", &splices)).code) goto fail;
        if ((e = swb_large_value_page(within, &out->within)).code) goto fail;
        if (splices->kind != JZ_JSON_ARR) {
            e = swb_invalid_type(splices, "a sequence");
            goto fail;
        }
        for (u32 i = 0; i < splices->v.arr.len; i++) {
            const jz_json *s = splices->v.arr.ptr[i];
            jazz_db_LargeValueUpdateSplice splice = {0};
            if ((e = swb_expect_struct(s, "LargeValueUpdateSplice")).code) goto fail;
            if ((e = swb_u64_field(s, "at", &splice.at)).code) goto fail;
            if ((e = swb_u64_field(s, "delete", &splice.delete_)).code) goto fail;
            const jz_json *insert = NULL;
            if ((e = swb_field(s, "insert", &insert)).code) goto fail;
            if ((e = swb_bytes(insert, &splice.insert)).code) goto fail;
            jz_vec_push(&out->splices, &splice, sizeof splice);
        }
        return JZ_OK;
    }
    const jz_json *edits = NULL;
    if ((e = swb_field(v, "edits", &edits)).code) goto fail;
    if (edits->kind != JZ_JSON_ARR) {
        e = swb_invalid_type(edits, "a sequence");
        goto fail;
    }
    for (u32 i = 0; i < edits->v.arr.len; i++) {
        const jz_json *s = edits->v.arr.ptr[i];
        jazz_db_JsonSetEdit edit = {0};
        if ((e = swb_expect_struct(s, "JsonSetEdit")).code) goto fail;
        if ((e = swb_string_field(s, "at", &edit.at)).code) goto fail;
        const jz_json *value = NULL;
        if ((e = swb_field(s, "value", &value)).code) {
            jz_str_drop(&edit.at);
            goto fail;
        }
        edit.value = jz_json_clone(value);
        jz_vec_push(&out->edits, &edit, sizeof edit);
    }
    return JZ_OK;
fail:
    swb_large_value_update_drop(out);
    return e;
}

jz_err swb__LargeValueUpdateVec__deserialize(const jz_json *v, jazz_db_LargeValueUpdateVec *out) {
    memset(out, 0, sizeof *out);
    if (v->kind != JZ_JSON_ARR) return swb_invalid_type(v, "a sequence");
    for (u32 i = 0; i < v->v.arr.len; i++) {
        jazz_db_LargeValueUpdate update;
        jz_err e = swb_large_value_update(v->v.arr.ptr[i], &update);
        if (e.code) {
            swb__LargeValueUpdateVec__drop(out);
            return e;
        }
        jz_vec_push(out, &update, sizeof update);
    }
    return JZ_OK;
}

/* from_value::<T>(js) */
#define SWB_FROM_VALUE(T, fn)                         \
    do {                                              \
        jz_json *data = NULL;                         \
        JZ_TRY(swb__from_value(v, &data));            \
        jz_err e_ = fn(data, out);                    \
        jz_json_free(data);                           \
        return e_;                                    \
    } while (0)

jz_err swb__from_value_as_ReadViewSpec(jz_js v, jazz_protocol_ReadViewSpec *out) { SWB_FROM_VALUE(ReadViewSpec, swb__ReadViewSpec__deserialize); }
jz_err swb__from_value_as_BranchSelector(jz_js v, jazz_model_BranchSelector *out) { SWB_FROM_VALUE(BranchSelector, swb__BranchSelector__deserialize); }
jz_err swb__from_value_as_BranchViewBase(jz_js v, jazz_model_BranchViewBase *out) { SWB_FROM_VALUE(BranchViewBase, swb__BranchViewBase__deserialize); }
jz_err swb__from_value_as_LargeValueUpdateVec(jz_js v, jazz_db_LargeValueUpdateVec *out) {
    SWB_FROM_VALUE(LargeValueUpdateVec, swb__LargeValueUpdateVec__deserialize);
}

/* ------------------------------------------------------------------ */
/* Serialize impls                                                     */
/* ------------------------------------------------------------------ */
static void swb_set(jz_js obj, const char *key, jz_js val) {
    jz_js k = wbg_str(key);
    jz_js err = 0;
    wbg_reflect_set(obj, k, val, &err); /* fresh ordinary object: cannot throw */
    jz_js_drop(k);
    jz_js_drop(val);
}

static jz_err swb_ser_u64(u64 v, jz_js *out) {
    if ((f64)v > MAX_SAFE_INTEGER) return swb__error("Number %llu is outside of safe integer range", (unsigned long long)v);
    *out = wbg_number_new((f64)v);
    return JZ_OK;
}

static jz_js swb_ser_uuid(const jz_uuid *u) { return wbg_uint8array_from(u->b, 16); }

static jz_err swb_ser_txid(const jazz_model_TxId *tx, jz_js *out) {
    jz_js time = 0;
    JZ_TRY(swb_ser_u64(tx->time, &time));
    jz_js obj = wbg_object_new();
    swb_set(obj, "time", time);
    swb_set(obj, "node", swb_ser_uuid(&tx->node));
    *out = obj;
    return JZ_OK;
}

/* single-key object for a non-unit externally tagged variant */
static jz_js swb_tagged(const char *variant, jz_js payload) {
    jz_js obj = wbg_object_new();
    swb_set(obj, variant, payload);
    return obj;
}

static const char *const REJECTION_REASON_NAMES[] = {"ClientClockTooFarAhead", "AuthorizationDenied", "ExclusiveConflict",
                                                     "CausalityViolation", "Cascade", "MalformedCommit"};
static const char *const DURABILITY_TIER_NAMES[] = {"None", "Local", "Global"};

static jz_err swb_ser_rejection_reason(const jazz_model_RejectionReason *r, jz_js *out) {
    switch (r->tag) {
    case JAZZ_REJ_CASCADE: {
        jz_js root = 0;
        JZ_TRY(swb_ser_txid(&r->root, &root));
        jz_js inner = wbg_object_new();
        swb_set(inner, "root", root);
        *out = swb_tagged("Cascade", inner);
        return JZ_OK;
    }
    case JAZZ_REJ_MALFORMED_COMMIT:
        *out = swb_tagged("MalformedCommit", wbg_string_new(r->message.ptr, r->message.len));
        return JZ_OK;
    default:
        *out = wbg_str(REJECTION_REASON_NAMES[r->tag]);
        return JZ_OK;
    }
}

/* WriteState { fate: Fate, global_time: Option<GlobalTime>, durability } */
jz_err swb__WriteState__serialize(const jazz_db_WriteState *s, jz_js *out) {
    jz_js fate = 0;
    switch (s->fate.tag) {
    case JAZZ_FATE_PENDING: fate = wbg_str("Pending"); break;
    case JAZZ_FATE_ACCEPTED: fate = wbg_str("Accepted"); break;
    default: {
        jz_js reason = 0;
        JZ_TRY(swb_ser_rejection_reason(&s->fate.rejected, &reason));
        fate = swb_tagged("Rejected", reason);
        break;
    }
    }
    jz_js global_time = WBG_UNDEFINED;
    if (s->has_global_time) {
        jz_err e = swb_ser_u64(s->global_time, &global_time);
        if (e.code) {
            jz_js_drop(fate);
            return e;
        }
    }
    jz_js obj = wbg_object_new();
    swb_set(obj, "fate", fate);
    swb_set(obj, "global_time", global_time);
    swb_set(obj, "durability", wbg_str(DURABILITY_TIER_NAMES[s->durability]));
    *out = obj;
    return JZ_OK;
}

/* TransactionId serializes as its 16-byte array (a tuple of u8) */
static jz_js swb_ser_transaction_id(const jazz_model_TransactionId *t) {
    jz_js arr = wbg_array_new_with_length(16);
    for (u32 i = 0; i < 16; i++) wbg_array_set(arr, i, wbg_number_new((f64)t->b[i]));
    return arr;
}

/* MutationErrorEvent (camelCase) { code, reason, transaction: LocalTransactionRecord } */
jz_err swb__MutationErrorEvent__serialize(const jazz_db_MutationErrorEvent *e, jz_js *out) {
    jz_js settlement = wbg_object_new();
    swb_set(settlement, "kind", wbg_str("rejected"));
    swb_set(settlement, "transactionId", swb_ser_transaction_id(&e->transaction.latest_settlement.transaction_id));
    swb_set(settlement, "code", wbg_string_new(e->transaction.latest_settlement.code.ptr, e->transaction.latest_settlement.code.len));
    swb_set(settlement, "reason", wbg_string_new(e->transaction.latest_settlement.reason.ptr, e->transaction.latest_settlement.reason.len));

    jz_js transaction = wbg_object_new();
    swb_set(transaction, "transactionId", swb_ser_transaction_id(&e->transaction.transaction_id));
    swb_set(transaction, "kind", wbg_str(e->transaction.kind == JAZZ_TXKIND_MERGEABLE ? "mergeable" : "exclusive"));
    swb_set(transaction, "sealed", wbg_bool(e->transaction.sealed));
    swb_set(transaction, "latestSettlement", settlement);

    jz_js obj = wbg_object_new();
    swb_set(obj, "code", wbg_string_new(e->code.ptr, e->code.len));
    swb_set(obj, "reason", wbg_string_new(e->reason.ptr, e->reason.len));
    swb_set(obj, "transaction", transaction);
    *out = obj;
    return JZ_OK;
}
