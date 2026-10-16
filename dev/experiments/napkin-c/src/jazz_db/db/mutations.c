/* mutations.c: port of crates/jazz/layers/db/src/db/mutations.rs
 * Row insertion, update, deletion, restoration, and authorization.
 *
 * async fns are synchronous; `node.lock().await` is a lock/unlock pair on the
 * shared node mutex; boxed futures handed to the owner queue are
 * jazz_db_LocalFuture values that complete on their first poll. */
#include "jz_rt.h"
#include "mutations.h"

#define DB_FN(name) jazz_db__db__mutations__Db__##name
#define NODE_FN(name) jazz_db__db__node_runtime__Node__##name
#define NS_FN(name) jazz_node__node__NodeState__##name
typedef jazz_db_Db DB;

#define DB_LOCK(db) jazz_db_node_lock((db)->node->node)
#define DB_UNLOCK(db) jazz_db_node_unlock((db)->node->node)
#define TRY_NODE_UNLOCK(db, expr) do { jz_err _ne = (expr); if (_ne.code) { DB_UNLOCK(db); return jazz_node__node__api_error__Error__From__from(_ne); } } while (0)
#define TRY_UNLOCK(db, expr) do { jz_err _ue = (expr); if (_ue.code) { DB_UNLOCK(db); return _ue; } } while (0)

/* ---------------- serde-derived impls ---------------- */

void jazz_db__db__mutations__LargeValueUpdateSplice__drop(void *p) {
    jazz_db_LargeValueUpdateSplice *s = p;
    jz_bytes_drop(&s->insert);
}
void jazz_db__db__mutations__LargeValueUpdateSplice__clone(void *dst, const void *src) {
    const jazz_db_LargeValueUpdateSplice *s = src;
    jazz_db_LargeValueUpdateSplice *d = dst;
    d->at = s->at;
    d->delete_ = s->delete_;
    d->insert = jz_bytes_clone(&s->insert);
}
void jazz_db__db__mutations__JsonSetEdit__drop(void *p) {
    jazz_db_JsonSetEdit *e = p;
    jz_str_drop(&e->at);
    if (e->value) jz_json_free(e->value);
}
void jazz_db__db__mutations__LargeValueUpdate__drop(void *p) {
    jazz_db_LargeValueUpdate *u = p;
    jz_str_drop(&u->column);
    jz_vec_drop(&u->splices, sizeof(jazz_db_LargeValueUpdateSplice), jazz_db__db__mutations__LargeValueUpdateSplice__drop);
    jz_vec_drop(&u->edits, sizeof(jazz_db_JsonSetEdit), jazz_db__db__mutations__JsonSetEdit__drop);
}

static const char *const LVP_KIND_NAMES[] = {"bytes", "text_utf16", "text_utf8"};
static const char *const LVU_KIND_NAMES[] = {"splice", "json_set"};

/* __FieldVisitor::visit_str for an internally tagged enum's "kind" */
static jz_err visit_variant_tag(const jz_json *obj, const char *const *names, u32 n, const char *expecting, u32 *idx) {
    const jz_json *kind = jz_json_obj_get(obj, "kind");
    if (!kind) return jz_err_fmt(JZ_JSON_ERR_MISSING_FIELD, "missing field `kind`");
    const jz_str *s = jz_json_as_str(kind);
    if (!s) return jz_err_fmt(JZ_JSON_ERR_TYPE, "invalid type: expected %s", expecting);
    for (u32 i = 0; i < n; ++i)
        if (jz_str_eq_cstr(s, names[i])) { *idx = i; return JZ_OK; }
    jz_str got = jz_str_clone(s);
    jz_str_push_char(&got, 0);
    jz_err e = jz_err_fmt(JZ_JSON_ERR_UNKNOWN_VARIANT, "unknown variant `%s`, expected %s", got.ptr, expecting);
    jz_str_drop(&got);
    return e;
}

jz_err jazz_db__db__mutations__LargeValueUpdatePage__Deserialize__deserialize(const jz_json *v, jazz_db_LargeValueUpdatePage *out) {
    JZ_TRY(jz_json_expect_obj(v, "internally tagged enum LargeValueUpdatePage"));
    u32 idx;
    JZ_TRY(visit_variant_tag(v, LVP_KIND_NAMES, 3, "one of `bytes`, `text_utf16`, `text_utf8`", &idx));
    out->tag = idx;
    JZ_TRY(jz_json_field_u64(v, "from", &out->from));
    JZ_TRY(jz_json_field_u64(v, "to", &out->to));
    return JZ_OK;
}

static jz_err deserialize_byte_vec(const jz_json *v, const char *field, jz_bytes *out) {
    const jz_json *f = jz_json_obj_get(v, field);
    if (!f) return jz_err_fmt(JZ_JSON_ERR_MISSING_FIELD, "missing field `%s`", field);
    if (f->kind != JZ_JSON_ARR) return jz_err_fmt(JZ_JSON_ERR_TYPE, "invalid type: expected a sequence");
    *out = (jz_bytes){0};
    for (u32 i = 0; i < f->v.arr.len; ++i) {
        u64 b;
        if (!jz_json_as_u64(f->v.arr.ptr[i], &b) || b > 255) {
            jz_bytes_drop(out);
            return jz_err_fmt(JZ_JSON_ERR_TYPE, "invalid value: expected u8");
        }
        jz_bytes_push_u8(out, (u8)b);
    }
    return JZ_OK;
}

jz_err jazz_db__db__mutations__LargeValueUpdateSplice__Deserialize__deserialize(const jz_json *v, jazz_db_LargeValueUpdateSplice *out) {
    JZ_TRY(jz_json_expect_obj(v, "struct LargeValueUpdateSplice"));
    JZ_TRY(jz_json_field_u64(v, "at", &out->at));
    JZ_TRY(jz_json_field_u64(v, "delete", &out->delete_));
    return deserialize_byte_vec(v, "insert", &out->insert);
}

jz_err jazz_db__db__mutations__JsonSetEdit__Deserialize__deserialize(const jz_json *v, jazz_db_JsonSetEdit *out) {
    JZ_TRY(jz_json_expect_obj(v, "struct JsonSetEdit"));
    JZ_TRY(jz_json_field_str(v, "at", &out->at));
    const jz_json *value = jz_json_obj_get(v, "value");
    if (!value) {
        jz_str_drop(&out->at);
        return jz_err_fmt(JZ_JSON_ERR_MISSING_FIELD, "missing field `value`");
    }
    out->value = jz_json_clone(value);
    return JZ_OK;
}

jz_err jazz_db__db__mutations__LargeValueUpdate__Deserialize__deserialize(const jz_json *v, jazz_db_LargeValueUpdate *out) {
    memset(out, 0, sizeof(*out));
    JZ_TRY(jz_json_expect_obj(v, "internally tagged enum LargeValueUpdate"));
    u32 idx;
    JZ_TRY(visit_variant_tag(v, LVU_KIND_NAMES, 2, "one of `splice`, `json_set`", &idx));
    out->tag = idx;
    JZ_TRY(jz_json_field_str(v, "column", &out->column));
    if (idx == JAZZ_LVU_SPLICE) {
        const jz_json *within = jz_json_obj_get(v, "within");
        if (!within) { jazz_db__db__mutations__LargeValueUpdate__drop(out); return jz_err_fmt(JZ_JSON_ERR_MISSING_FIELD, "missing field `within`"); }
        jz_err e = jazz_db__db__mutations__LargeValueUpdatePage__Deserialize__deserialize(within, &out->within);
        if (e.code) { jazz_db__db__mutations__LargeValueUpdate__drop(out); return e; }
        const jz_json *splices = jz_json_obj_get(v, "splices");
        if (!splices) { jazz_db__db__mutations__LargeValueUpdate__drop(out); return jz_err_fmt(JZ_JSON_ERR_MISSING_FIELD, "missing field `splices`"); }
        if (splices->kind != JZ_JSON_ARR) { jazz_db__db__mutations__LargeValueUpdate__drop(out); return jz_err_fmt(JZ_JSON_ERR_TYPE, "invalid type: expected a sequence"); }
        for (u32 i = 0; i < splices->v.arr.len; ++i) {
            jazz_db_LargeValueUpdateSplice s;
            e = jazz_db__db__mutations__LargeValueUpdateSplice__Deserialize__deserialize(splices->v.arr.ptr[i], &s);
            if (e.code) { jazz_db__db__mutations__LargeValueUpdate__drop(out); return e; }
            jz_vec_push(&out->splices, &s, sizeof s);
        }
    } else {
        const jz_json *edits = jz_json_obj_get(v, "edits");
        if (!edits) { jazz_db__db__mutations__LargeValueUpdate__drop(out); return jz_err_fmt(JZ_JSON_ERR_MISSING_FIELD, "missing field `edits`"); }
        if (edits->kind != JZ_JSON_ARR) { jazz_db__db__mutations__LargeValueUpdate__drop(out); return jz_err_fmt(JZ_JSON_ERR_TYPE, "invalid type: expected a sequence"); }
        for (u32 i = 0; i < edits->v.arr.len; ++i) {
            jazz_db_JsonSetEdit ed;
            jz_err e = jazz_db__db__mutations__JsonSetEdit__Deserialize__deserialize(edits->v.arr.ptr[i], &ed);
            if (e.code) { jazz_db__db__mutations__LargeValueUpdate__drop(out); return e; }
            jz_vec_push(&out->edits, &ed, sizeof ed);
        }
    }
    return JZ_OK;
}

/* ---------------- free helpers ---------------- */

#define MAX_PHYSICAL_TIMESTAMP_MS JAZZ_TYPES_HLC_MAX_PHYSICAL_MS

jz_err jazz_db__db__mutations__validate_updated_at_ms(bool has, u64 updated_at_ms) {
    if (has && updated_at_ms > MAX_PHYSICAL_TIMESTAMP_MS) {
        return jz_err_fmt(JAZZ_EC_WRITE_REJECTED,
                          "updated_at_ms %llu exceeds the packed-HLC physical millisecond range",
                          (unsigned long long)updated_at_ms);
    }
    return JZ_OK;
}

groove_StagedLargeValueId jazz_db__db__mutations__StreamingValueUpload__cleanup_id(jazz_db_StreamingValueUpload *self) {
    if (self->preparation) {
        groove__large_values__PushStreamingPreparation__drop(self->preparation);
        self->preparation = NULL;
    }
    if (self->emitted) {
        jz_vec_drop(self->emitted, sizeof(groove_StagedChunk), groove__large_values__StagedChunk__drop);
        jz_rc_release(self->emitted);
        self->emitted = NULL;
    }
    return self->id;
}

static void streaming_value_upload_drop(jazz_db_StreamingValueUpload *u) {
    (void)jazz_db__db__mutations__StreamingValueUpload__cleanup_id(u);
}

static jz_err large_value_cell_type_error(jz_slice table, jz_slice column) {
    return jz_err_fmt(JAZZ_EC_SCHEMA, "%.*s.%.*s is not a bytes or string cell",
                      (int)table.len, (const char *)table.ptr, (int)column.len, (const char *)column.ptr);
}

static jz_err checked_usize(u64 value, const char *what, usize *out) {
    if (value > (u64)SIZE_MAX) return jz_err_fmt(JAZZ_EC_QUERY, "%s exceeds addressable memory", what);
    *out = (usize)value;
    return JZ_OK;
}

/* UTF-8 decode one code point length and its UTF-16 width */
static u32 utf8_char_len(u8 lead) {
    if (lead < 0x80) return 1;
    if ((lead & 0xE0) == 0xC0) return 2;
    if ((lead & 0xF0) == 0xE0) return 3;
    return 4;
}
static u64 utf16_count(const u8 *p, u32 n) {
    u64 total = 0;
    for (u32 i = 0; i < n;) {
        u32 l = utf8_char_len(p[i]);
        total += l == 4 ? 2 : 1;
        i += l;
    }
    return total;
}

/* owned_utf16_range -> returns the byte range [start, end) */
static jz_err owned_utf16_range(const u8 *text, u32 len, u64 from, u64 to, u32 *start_out, u32 *end_out) {
    u64 total = utf16_count(text, len);
    if (from > to || to > total) return jz_err_new(JAZZ_EC_QUERY, "UTF-16 range is out of bounds");
    /* byte_at closure */
    bool found_start = false, found_end = false;
    u32 start = 0, end = 0;
    if (from == total) { start = len; found_start = true; }
    if (to == total) { end = len; found_end = true; }
    u64 offset = 0;
    for (u32 i = 0; i < len && (!found_start || !found_end);) {
        u32 l = utf8_char_len(text[i]);
        if (!found_start && offset == from) { start = i; found_start = true; }
        if (!found_end && offset == to) { end = i; found_end = true; }
        offset += l == 4 ? 2 : 1;
        i += l;
    }
    if (!found_start) return jz_err_new(JAZZ_EC_QUERY, "UTF-16 range starts inside a surrogate pair");
    if (!found_end) return jz_err_new(JAZZ_EC_QUERY, "UTF-16 range ends inside a surrogate pair");
    *start_out = start;
    *end_out = end;
    return JZ_OK;
}

static bool is_utf16_coordinate_lowering_error(const jz_err *error) {
    jz_str message = {0};
    jazz_node__node__Error__display(&message, error);
    jz_err probe = {error->code, &message};
    bool hit = jazz_db_err_msg_contains(&probe, "MalformedScalar") || jazz_db_err_msg_contains(&probe, "InvalidUtf8") ||
               jazz_db_err_msg_contains(&probe, "malformed physical scalar encoding");
    jz_str_drop(&message);
    return hit;
}

/* Map a UTF-16 coordinate lowering failure to the splice error callers see. */
static jz_err utf16_splice_byte(jz_err result) {
    if (!result.code) return result;
    if (is_utf16_coordinate_lowering_error(&result)) {
        jz_err_drop(&result);
        return jz_err_new(JAZZ_EC_QUERY, "UTF-16 splice splits a surrogate pair");
    }
    return jazz_node__node__api_error__Error__From__from(result);
}

static jz_err splice_owned_bytes(jz_bytes *bytes, u64 offset, u64 delete_length, const jz_bytes *insert) {
    usize start, end;
    JZ_TRY(checked_usize(offset, "splice offset", &start));
    if (offset + delete_length < offset) return jz_err_new(JAZZ_EC_QUERY, "splice range overflows");
    u64 end_u64 = offset + delete_length;
    JZ_TRY(checked_usize(end_u64, "splice end", &end));
    if (start > end || end > bytes->len) return jz_err_new(JAZZ_EC_QUERY, "splice range is out of bounds");
    /* bytes.splice(start..end, insert) */
    u32 tail = bytes->len - (u32)end;
    u32 new_len = (u32)start + insert->len + tail;
    if (new_len > bytes->cap) {
        bytes->ptr = jz_realloc(bytes->ptr, new_len);
        bytes->cap = new_len;
    }
    memmove(bytes->ptr + start + insert->len, bytes->ptr + end, tail);
    if (insert->len) memcpy(bytes->ptr + start, insert->ptr, insert->len);
    bytes->len = new_len;
    return JZ_OK;
}

static jz_err checked_range(u64 from, u64 to, u64 length, const char *coordinate) {
    if (from > to || to > length) return jz_err_fmt(JAZZ_EC_QUERY, "%s page is out of bounds", coordinate);
    return JZ_OK;
}

/* RFC 6901's `array-index` grammar rejects leading zeroes except for the
 * single index `0`. */
static jz_err json_pointer_array_index(const jz_str *token, u32 *out) {
    if (token->len == 1 && token->ptr[0] == '0') { *out = 0; return JZ_OK; }
    bool ok = token->len > 0 && token->ptr[0] >= '1' && token->ptr[0] <= '9';
    for (u32 i = 0; ok && i < token->len; ++i)
        if (token->ptr[i] < '0' || token->ptr[i] > '9') ok = false;
    if (!ok) return jz_err_new(JAZZ_EC_QUERY, "JSON array pointer token is not an index");
    u64 v = 0;
    for (u32 i = 0; i < token->len; ++i) {
        u64 next = v * 10 + (u64)(token->ptr[i] - '0');
        if (next < v || next > 0xFFFFFFFFull) return jz_err_new(JAZZ_EC_QUERY, "JSON array pointer token is not an index");
        v = next;
    }
    *out = (u32)v;
    return JZ_OK;
}

static jz_err json_set_step(jz_json *current, const jz_str *token, jz_json ***slot) {
    if (current->kind == JZ_JSON_OBJ) {
        jz_json **child = jz_btree_get(&current->v.obj, token);
        if (!child) return jz_err_new(JAZZ_EC_QUERY, "JSON set path does not exist");
        *slot = child;
        return JZ_OK;
    }
    if (current->kind == JZ_JSON_ARR) {
        u32 index;
        JZ_TRY(json_pointer_array_index(token, &index));
        if (index >= current->v.arr.len) return jz_err_new(JAZZ_EC_QUERY, "JSON set path does not exist");
        *slot = &current->v.arr.ptr[index];
        return JZ_OK;
    }
    return jz_err_new(JAZZ_EC_QUERY, "JSON set path has a scalar parent");
}

static void str_vec_drop(void *v) { jz_vec_drop(v, sizeof(jz_str), (void (*)(void *))jz_str_drop); }

jz_err jazz_db__db__mutations__apply_json_set(jz_json **value, jz_slice pointer, jz_json *replacement) {
    if (pointer.len == 0) {
        jz_json_free(*value);
        *value = replacement;
        return JZ_OK;
    }
    if (pointer.ptr[0] != '/') {
        jz_json_free(replacement);
        return jz_err_new(JAZZ_EC_QUERY, "JSON pointer must be empty or begin with '/'");
    }
    JZ_VEC(jz_str) tokens = {0};
    u32 i = 1;
    for (;;) {
        jz_str decoded = {0};
        while (i < pointer.len && pointer.ptr[i] != '/') {
            char c = (char)pointer.ptr[i++];
            if (c != '~') { jz_str_push_char(&decoded, c); continue; }
            char next = i < pointer.len ? (char)pointer.ptr[i++] : 0;
            if (next == '0') jz_str_push_char(&decoded, '~');
            else if (next == '1') jz_str_push_char(&decoded, '/');
            else {
                jz_str_drop(&decoded);
                str_vec_drop(&tokens);
                jz_json_free(replacement);
                return jz_err_new(JAZZ_EC_QUERY, "JSON pointer has an invalid escape");
            }
        }
        jz_vec_push(&tokens, &decoded, sizeof decoded);
        if (i >= pointer.len) break;
        ++i; /* skip '/' */
    }
    if (tokens.len == 0) {
        str_vec_drop(&tokens);
        jz_json_free(replacement);
        return jz_err_new(JAZZ_EC_QUERY, "JSON pointer is missing a target");
    }
    jz_json *current = *value;
    for (u32 t = 0; t + 1 < tokens.len; ++t) {
        jz_json **slot;
        jz_err e = json_set_step(current, &tokens.ptr[t], &slot);
        if (e.code) { str_vec_drop(&tokens); jz_json_free(replacement); return e; }
        current = *slot;
    }
    jz_json **target;
    jz_err e = json_set_step(current, &tokens.ptr[tokens.len - 1], &target);
    str_vec_drop(&tokens);
    if (e.code) { jz_json_free(replacement); return e; }
    jz_json_free(*target);
    *target = replacement;
    return JZ_OK;
}

static bool unwrap_present_nullable(groove_Value *value) {
    if (value->tag == GROOVE_V_NULLABLE && value->v.nullable) {
        groove_Value *inner = value->v.nullable;
        *value = *inner;
        jz_free(inner);
        return true;
    }
    return false;
}

static void preserve_nullable(groove_Value *value, bool nullable) {
    if (!nullable) return;
    groove_Value *boxed = JZ_NEW(groove_Value);
    *boxed = *value;
    value->tag = GROOVE_V_NULLABLE;
    value->v.nullable = boxed;
}

static jz_err apply_inline_splices(groove_Value *value, groove_LargeValueKind expected_kind, const jazz_db_LargeValueUpdatePage *page,
                                   const jazz_db_LargeValueUpdateSplice *splices, u32 n_splices) {
    bool nullable = unwrap_present_nullable(value);
    jz_bytes bytes;
    bool is_text;
    if (value->tag == GROOVE_V_BYTES) { bytes = value->v.bytes; is_text = false; }
    else if (value->tag == GROOVE_V_STRING) { bytes = (jz_bytes){(u8 *)value->v.s.ptr, value->v.s.len, value->v.s.cap}; is_text = true; }
    else {
        groove__records__Value__drop(value);
        return jz_err_new(JAZZ_EC_SCHEMA, "partial splice requires a bytes or string cell");
    }
    value->tag = GROOVE_V_U8; /* moved out */
    jz_err err = JZ_OK;
    groove_LargeValueKind actual_kind = is_text ? GROOVE_LV_STRING : GROOVE_LV_BYTES;
    if (actual_kind != expected_kind) { err = jz_err_new(JAZZ_EC_SCHEMA, "stored scalar kind does not match its column schema"); goto fail; }
    u64 page_start, page_length;
    if (page->tag == JAZZ_LVP_BYTES && !is_text) {
        if ((err = checked_range(page->from, page->to, bytes.len, "byte")).code) goto fail;
        page_start = page->from; page_length = page->to - page->from;
    } else if (page->tag == JAZZ_LVP_TEXT_UTF8 && is_text) {
        if ((err = checked_range(page->from, page->to, bytes.len, "UTF-8")).code) goto fail;
        if (!jz_utf8_valid(bytes.ptr, bytes.len)) { err = jz_err_new(JAZZ_EC_SCHEMA, "stored text is not valid UTF-8"); goto fail; }
        usize start, end;
        if ((err = checked_usize(page->from, "UTF-8 page start", &start)).code) goto fail;
        if ((err = checked_usize(page->to, "UTF-8 page end", &end)).code) goto fail;
        bool sb = start == bytes.len || (start < bytes.len && (bytes.ptr[start] & 0xC0) != 0x80);
        bool eb = end == bytes.len || (end < bytes.len && (bytes.ptr[end] & 0xC0) != 0x80);
        if (!sb || !eb) { err = jz_err_new(JAZZ_EC_QUERY, "UTF-8 page splits a code point"); goto fail; }
        page_start = page->from; page_length = page->to - page->from;
    } else if (page->tag == JAZZ_LVP_TEXT_UTF16 && is_text) {
        if (!jz_utf8_valid(bytes.ptr, bytes.len)) { err = jz_err_new(JAZZ_EC_SCHEMA, "stored text is not valid UTF-8"); goto fail; }
        u64 total = utf16_count(bytes.ptr, bytes.len);
        if ((err = checked_range(page->from, page->to, total, "UTF-16")).code) goto fail;
        u32 s0, e0;
        if ((err = owned_utf16_range(bytes.ptr, bytes.len, 0, page->from, &s0, &e0)).code) goto fail;
        if ((err = owned_utf16_range(bytes.ptr, bytes.len, 0, page->to, &s0, &e0)).code) goto fail;
        /* Keep this coordinate in UTF-16. Each later sequential splice is
         * resolved against the text produced by its predecessors. */
        page_start = page->from; page_length = page->to - page->from;
    } else if (page->tag == JAZZ_LVP_BYTES) {
        err = large_value_cell_type_error(JZ_LIT("row"), JZ_LIT("column"));
        goto fail;
    } else {
        err = jz_err_new(JAZZ_EC_SCHEMA, "text splice requires a string cell");
        goto fail;
    }
    for (u32 i = 0; i < n_splices; ++i) {
        const jazz_db_LargeValueUpdateSplice *splice = &splices[i];
        if (splice->at + splice->delete_ < splice->at) { err = jz_err_new(JAZZ_EC_QUERY, "splice range overflows"); goto fail; }
        u64 end = splice->at + splice->delete_;
        if (splice->at > page_length || end > page_length) { err = jz_err_new(JAZZ_EC_QUERY, "splice deletion leaves the selected page"); goto fail; }
        u64 absolute, delete_bytes, inserted_units;
        if (page->tag == JAZZ_LVP_BYTES || page->tag == JAZZ_LVP_TEXT_UTF8) {
            if (page_start + splice->at < page_start) { err = jz_err_new(JAZZ_EC_QUERY, "splice offset overflows"); goto fail; }
            absolute = page_start + splice->at;
            delete_bytes = splice->delete_;
            inserted_units = splice->insert.len;
        } else {
            if (!jz_utf8_valid(bytes.ptr, bytes.len)) { err = jz_err_new(JAZZ_EC_SCHEMA, "stored text is not valid UTF-8"); goto fail; }
            if (page_start + splice->at < page_start) { err = jz_err_new(JAZZ_EC_QUERY, "UTF-16 splice offset overflows"); goto fail; }
            u64 start_utf16 = page_start + splice->at;
            if (start_utf16 + splice->delete_ < start_utf16) { err = jz_err_new(JAZZ_EC_QUERY, "UTF-16 splice range overflows"); goto fail; }
            u64 end_utf16 = start_utf16 + splice->delete_;
            u32 s0, a_end, b_end;
            if ((err = owned_utf16_range(bytes.ptr, bytes.len, 0, start_utf16, &s0, &a_end)).code) goto fail;
            if ((err = owned_utf16_range(bytes.ptr, bytes.len, 0, end_utf16, &s0, &b_end)).code) goto fail;
            if (!jz_utf8_valid(splice->insert.ptr, splice->insert.len)) { err = jz_err_new(JAZZ_EC_QUERY, "text splice insert is not valid UTF-8"); goto fail; }
            absolute = a_end;
            delete_bytes = (u64)b_end - absolute;
            inserted_units = utf16_count(splice->insert.ptr, splice->insert.len);
        }
        if ((err = splice_owned_bytes(&bytes, absolute, delete_bytes, &splice->insert)).code) goto fail;
        if (is_text && !jz_utf8_valid(bytes.ptr, bytes.len)) { err = jz_err_new(JAZZ_EC_QUERY, "text splice is not valid UTF-8"); goto fail; }
        if (page_length < splice->delete_ || page_length - splice->delete_ + inserted_units < page_length - splice->delete_) {
            err = jz_err_new(JAZZ_EC_QUERY, "splice page length overflows");
            goto fail;
        }
        page_length = page_length - splice->delete_ + inserted_units;
    }
    if (is_text) {
        if (!jz_utf8_valid(bytes.ptr, bytes.len)) { err = jz_err_new(JAZZ_EC_QUERY, "text splice is not valid UTF-8"); goto fail; }
        value->tag = GROOVE_V_STRING;
        value->v.s = (jz_str){(char *)bytes.ptr, bytes.len, bytes.cap};
    } else {
        value->tag = GROOVE_V_BYTES;
        value->v.bytes = bytes;
    }
    preserve_nullable(value, nullable);
    return JZ_OK;
fail:
    jz_bytes_drop(&bytes);
    return err;
}

/* ---------------- LargeValueEdit = (Value, Option<StagedLargeValue>, Vec<StagedLargeValueId>) ---------------- */
typedef JZ_VEC(groove_StagedLargeValueId) StagedIdVec;
typedef struct LargeValueEdit {
    groove_Value next;
    bool has_staged;
    groove_StagedLargeValue staged;
    StagedIdVec obsolete;
} LargeValueEdit;

/* ---------------- Db impl ---------------- */

void DB_FN(queued_write_handle)(const DB *self, jazz_types_RowUuid row, jazz_model_TxId tx_id, jazz_db_QueuedMutationStatus *status,
                                jazz_db_QueuedMutationAlias *alias, jazz_db_WriteHandle *out) {
    out->node = self->node->node; /* Rc::downgrade */
    out->row_uuid = row;
    out->tx_id = tx_id;
    out->local_tier = JAZZ_DUR_NONE;
    out->queued_status = status;
    out->queued_alias = alias;
}

/* Validate a queued write's provenance time and pin it (defaulting to now). */
static jz_err queued_now_ms(const DB *self, bool *has_updated_at_ms, u64 *updated_at_ms, u64 *out) {
    JZ_TRY(jazz_db__db__mutations__validate_updated_at_ms(*has_updated_at_ms, *updated_at_ms));
    u64 now_ms = *has_updated_at_ms ? *updated_at_ms : DB_FN(next_now_ms)(self);
    *has_updated_at_ms = true;
    *updated_at_ms = now_ms;
    *out = now_ms;
    return JZ_OK;
}

/* Queued write closures: `move |db| async move { db.<op>(...).await }` */
typedef enum { QW_INSERT, QW_UPSERT, QW_DELETE, QW_RESTORE, QW_LARGE_VALUE_UPDATE } QueuedWriteKind;
typedef struct QueuedWrite {
    QueuedWriteKind kind;
    DB *db;
    jz_str table;
    jazz_types_RowUuid row;
    jz_btree cells;
    bool has_cells;
    jazz_db_InsertOptions insert_options;
    jazz_db_UpdateOptions update_options;
    jazz_db_RestoreOptions restore_options;
    jazz_db_LargeValueUpdateVec mutations;
    u64 now_ms;
    jazz_model_TxId tx_id; /* enqueue_write's reserved id */
} QueuedWrite;

static jz_err queued_write_run(QueuedWrite *w, jazz_db_WriteHandle *write) {
    jz_slice table = jz_str_slice(&w->table);
    switch (w->kind) {
    case QW_INSERT:
        w->has_cells = false;
        return DB_FN(insert)(w->db, table, w->cells, w->insert_options, write);
    case QW_UPSERT:
        w->has_cells = false;
        return jazz_db__db__mutations__Db__upsert_with_insert_validation(w->db, table, w->row, w->cells, w->update_options, true, write);
    case QW_DELETE:
        return DB_FN(delete)(w->db, table, w->row, w->update_options, write);
    case QW_RESTORE: {
        bool has = w->has_cells;
        w->has_cells = false;
        return DB_FN(restore)(w->db, table, w->row, has, w->cells, w->restore_options, write);
    }
    case QW_LARGE_VALUE_UPDATE: {
        w->has_cells = false;
        jazz_db_LargeValueUpdateVec mutations = w->mutations;
        w->mutations = (jazz_db_LargeValueUpdateVec){0};
        return DB_FN(update_with_large_value_mutations_at_ms)(w->db, table, w->row, w->cells, mutations, w->now_ms, write);
    }
    }
    JZ_UNREACHABLE();
}

/* enqueue_write's Box::pin(async move { let write = write.await?; debug_assert_eq!(..); Ok(()) }) */
static bool queued_write_poll(void *self, const jz_waker *cx, void *out) {
    (void)cx;
    QueuedWrite *w = self;
    jazz_db_WriteHandle write;
    jz_err e = queued_write_run(w, &write);
    if (!e.code) {
        jazz_model_TxId actual = jazz_db__db__WriteHandle__mergeable_tx_id(&write);
        (void)actual; /* debug_assert_eq!(write.mergeable_tx_id(), tx_id) */
        jazz_db__db__WriteHandle__drop(&write);
    }
    *(jz_err *)out = e;
    return true;
}

static void queued_write_drop(void *self) {
    QueuedWrite *w = self;
    jz_str_drop(&w->table);
    if (w->has_cells) jz_btree_drop(&w->cells);
    jz_vec_drop(&w->mutations, sizeof(jazz_db_LargeValueUpdate), jazz_db__db__mutations__LargeValueUpdate__drop);
    if (w->db) jazz_db__db__Db__drop(w->db);
    jz_free(w);
}

/* Reserve a queued write's final transaction id at `now_ms` and queue
 * `write`, run on a facade bound to that id, behind earlier owner work. */
static jz_err enqueue_write(const DB *self, jazz_types_RowUuid row, u64 now_ms, QueuedWrite *write, jazz_db_WriteHandle *out) {
    jazz_model_TxId tx_id;
    jz_err e = jazz_db__db__lifecycle__Db__reserve_transaction_id_at_ms(self, now_ms, &tx_id);
    if (e.code) { queued_write_drop(write); return e; }
    write->db = jazz_db__db__lifecycle__Db__clone_for_reserved_transaction(self, tx_id);
    write->tx_id = tx_id;
    jazz_db_QueuedMutationFuture future = {queued_write_poll, queued_write_drop, write};
    jazz_db_QueuedMutationStatus *status = NODE_FN(enqueue_mutation)(self->node, tx_id, future);
    DB_FN(queued_write_handle)(self, row, tx_id, status, NULL, out);
    return JZ_OK;
}

static QueuedWrite *queued_write_new(QueuedWriteKind kind, jz_str table, jazz_types_RowUuid row) {
    QueuedWrite *w = JZ_NEW(QueuedWrite);
    w->kind = kind;
    w->table = table;
    w->row = row;
    return w;
}

/* Synchronously reserve identity and enqueue an owner-retained insert. */
jz_err DB_FN(enqueue_insert)(const DB *self, jz_str table, jz_btree cells, jazz_db_InsertOptions options, jazz_db_WriteHandle *out) {
    /* Reject a bad provenance time before consuming a row id. */
    jz_err e = jazz_db__db__mutations__validate_updated_at_ms(options.has_updated_at_ms, options.updated_at_ms);
    if (e.code) { jz_str_drop(&table); jz_btree_drop(&cells); return e; }
    jazz_types_RowUuid row = options.has_row_id ? options.row_id : self->row_id_source->vt->next_row_id(self->row_id_source->self);
    u64 now_ms;
    e = queued_now_ms(self, &options.has_updated_at_ms, &options.updated_at_ms, &now_ms);
    if (e.code) { jz_str_drop(&table); jz_btree_drop(&cells); return e; }
    options.has_row_id = true;
    options.row_id = row;
    QueuedWrite *w = queued_write_new(QW_INSERT, table, row);
    w->cells = cells;
    w->has_cells = true;
    w->insert_options = options;
    return enqueue_write(self, row, now_ms, w, out);
}

/* enqueue_update's future */
typedef struct QueuedUpdate {
    DB *db;
    jz_str table;
    jazz_types_RowUuid row;
    jz_btree patch;
    bool has_patch;
    jazz_db_UpdateOptions options;
    bool empty_patch;
    jazz_model_TxId tx_id;
    jazz_db_QueuedMutationAlias *update_alias;
} QueuedUpdate;

static bool queued_update_poll(void *self, const jz_waker *cx, void *out) {
    (void)cx;
    QueuedUpdate *u = self;
    jazz_db_WriteHandle write;
    u->has_patch = false;
    jz_err e = DB_FN(update)(u->db, jz_str_slice(&u->table), u->row, u->patch, u->options, &write);
    if (e.code) { *(jz_err *)out = e; return true; }
    jazz_model_TxId actual_tx_id = jazz_db__db__WriteHandle__mergeable_tx_id(&write);
    if (u->empty_patch && !jazz_model_TxId_eq(actual_tx_id, u->tx_id)) {
        /* Empty root patches deliberately validate visibility and reuse the
         * current row transaction. The returned write handle retains that
         * existing transaction as its bounded completion target. */
        u->update_alias->has = true;
        u->update_alias->tx_id = actual_tx_id;
    }
    jazz_db__db__WriteHandle__drop(&write);
    *(jz_err *)out = JZ_OK;
    return true;
}

static void queued_update_drop(void *self) {
    QueuedUpdate *u = self;
    jz_str_drop(&u->table);
    if (u->has_patch) jz_btree_drop(&u->patch);
    jz_rc_release(u->update_alias);
    jazz_db__db__Db__drop(u->db);
    jz_free(u);
}

jz_err DB_FN(enqueue_update)(const DB *self, jz_str table, jazz_types_RowUuid row, jz_btree patch, jazz_db_UpdateOptions options, jazz_db_WriteHandle *out) {
    u64 now_ms;
    jz_err e = queued_now_ms(self, &options.has_updated_at_ms, &options.updated_at_ms, &now_ms);
    if (!e.code) {
        jazz_model_TxId tx_id;
        e = jazz_db__db__lifecycle__Db__reserve_transaction_id_at_ms(self, now_ms, &tx_id);
        if (!e.code) {
            QueuedUpdate *u = JZ_NEW(QueuedUpdate);
            u->db = jazz_db__db__lifecycle__Db__clone_for_reserved_transaction(self, tx_id);
            u->table = table;
            u->row = row;
            u->empty_patch = patch.len == 0;
            u->patch = patch;
            u->has_patch = true;
            u->options = options;
            u->tx_id = tx_id;
            jazz_db_QueuedMutationAlias *alias = jz_rc_new(sizeof(jazz_db_QueuedMutationAlias), NULL);
            u->update_alias = jz_rc_clone(alias);
            jazz_db_QueuedMutationFuture future = {queued_update_poll, queued_update_drop, u};
            jazz_db_QueuedMutationStatus *status = NODE_FN(enqueue_mutation)(self->node, tx_id, future);
            DB_FN(queued_write_handle)(self, row, tx_id, status, alias, out);
            return JZ_OK;
        }
    }
    jz_str_drop(&table);
    jz_btree_drop(&patch);
    return e;
}

jz_err DB_FN(enqueue_upsert)(const DB *self, jz_str table, jazz_types_RowUuid row, jz_btree cells, jazz_db_UpsertOptions options, jazz_db_WriteHandle *out) {
    u64 now_ms;
    jz_err e = queued_now_ms(self, &options.has_updated_at_ms, &options.updated_at_ms, &now_ms);
    if (e.code) { jz_str_drop(&table); jz_btree_drop(&cells); return e; }
    QueuedWrite *w = queued_write_new(QW_UPSERT, table, row);
    w->cells = cells;
    w->has_cells = true;
    w->update_options = options;
    return enqueue_write(self, row, now_ms, w, out);
}

jz_err DB_FN(enqueue_delete)(const DB *self, jz_str table, jazz_types_RowUuid row, jazz_db_DeleteOptions options, jazz_db_WriteHandle *out) {
    u64 now_ms;
    jz_err e = queued_now_ms(self, &options.has_updated_at_ms, &options.updated_at_ms, &now_ms);
    if (e.code) { jz_str_drop(&table); return e; }
    QueuedWrite *w = queued_write_new(QW_DELETE, table, row);
    w->update_options = options;
    return enqueue_write(self, row, now_ms, w, out);
}

jz_err DB_FN(enqueue_restore)(const DB *self, jz_str table, jazz_types_RowUuid row, bool has_cells, jz_btree cells, jazz_db_RestoreOptions options, jazz_db_WriteHandle *out) {
    u64 now_ms;
    jz_err e = queued_now_ms(self, &options.has_updated_at_ms, &options.updated_at_ms, &now_ms);
    if (e.code) { jz_str_drop(&table); if (has_cells) jz_btree_drop(&cells); return e; }
    QueuedWrite *w = queued_write_new(QW_RESTORE, table, row);
    w->has_cells = has_cells;
    w->cells = cells;
    w->restore_options = options;
    return enqueue_write(self, row, now_ms, w, out);
}

jz_err DB_FN(enqueue_large_value_update)(const DB *self, jz_str table, jazz_types_RowUuid row, jz_btree patch, jazz_db_LargeValueUpdateVec mutations,
                                         bool has_updated_at_ms, u64 updated_at_ms, jazz_db_WriteHandle *out) {
    u64 now_ms;
    jz_err e = queued_now_ms(self, &has_updated_at_ms, &updated_at_ms, &now_ms);
    if (e.code) {
        jz_str_drop(&table);
        jz_btree_drop(&patch);
        jz_vec_drop(&mutations, sizeof(jazz_db_LargeValueUpdate), jazz_db__db__mutations__LargeValueUpdate__drop);
        return e;
    }
    QueuedWrite *w = queued_write_new(QW_LARGE_VALUE_UPDATE, table, row);
    w->cells = patch;
    w->has_cells = true;
    w->mutations = mutations;
    w->now_ms = now_ms;
    return enqueue_write(self, row, now_ms, w, out);
}

/* ---------------- RowCells helpers ---------------- */
static void str_clone_fn(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone((const jz_str *)src); }
static void cells_init(jz_btree *t) { jz_btree_init(t, &jazz_db_str_value_desc); }
static void str_set_init(jz_btree *t) { jz_btree_init(t, &jazz_db_str_set_desc); }
static jz_btree cells_clone(const jz_btree *src) {
    jz_btree out;
    jz_btree_clone(&out, src, str_clone_fn, groove__records__Value__clone);
    return out;
}
/* cells.extend(patch) */
static void cells_extend(jz_btree *dst, jz_btree *src) {
    jz_str k;
    groove_Value v;
    while (jz_btree_pop_first(src, &k, &v)) {
        if (jz_btree_contains(dst, &k)) {
            jz_btree_insert(dst, &k, &v, NULL);
            jz_str_drop(&k);
        } else {
            jz_btree_insert(dst, &k, &v, NULL);
        }
    }
    jz_btree_drop(src);
}
/* patch.keys().cloned().collect::<BTreeSet<String>>() */
static jz_btree cell_keys(const jz_btree *cells) {
    jz_btree out;
    str_set_init(&out);
    jz_str *k;
    groove_Value *v;
    JZ_BTREE_FOR(cells, k, v) {
        jz_str key = jz_str_clone(k);
        jz_btree_insert(&out, &key, NULL, NULL);
    }
    return out;
}
static bool cells_contains(const jz_btree *t, jz_slice key) {
    jz_str k = {(char *)key.ptr, key.len, key.len};
    return jz_btree_contains(t, &k);
}
static const groove_Value *cells_get(const jz_btree *t, jz_slice key) {
    jz_str k = {(char *)key.ptr, key.len, key.len};
    return jz_btree_get(t, &k);
}

/* ---------------- forward declarations of private Db fns ---------------- */
typedef struct MergedCells { jz_btree cells; bool has_parent; jazz_model_TxId parent; jz_btree authored_columns; } MergedCells;
static jz_err merge_existing_cells(const DB *self, jz_slice table, jazz_types_RowUuid row, jz_btree patch, MergedCells *out);
static jz_err merge_existing_cells_for_client_identity(const DB *self, jz_slice table, jazz_types_RowUuid row, jz_btree patch, jazz_types_AuthorSubject identity, MergedCells *out);
static jz_err merge_existing_cells_for_identity(const DB *self, jz_slice table, jazz_types_RowUuid row, jz_btree patch, jazz_types_AuthorSubject identity, MergedCells *out);
static jz_err commit_mergeable_batch(const DB *self, jazz_node_NodeState *node, jazz_node_MergeableCommit **commits, u32 n, jazz_node_PublishedTransaction **out);
static jz_err finish_published_write(const DB *self, jazz_types_RowUuid row, jazz_node_PublishedTransaction *published, jazz_db_WriteHandle *out);
static jz_err large_value_column_kind(const DB *self, jz_slice table, jz_slice column, groove_LargeValueKind *kind, bool *nullable);
static jz_err validate_streaming_column(const DB *self, jz_slice table, const jz_btree *cells, jz_slice column, groove_LargeValueKind *kind, bool *nullable);
static jz_err reject_attributed_branch_target(const DB *self, jazz_db_WriteIdentity identity, bool targets_branch);
static jz_err ensure_row_absent(const DB *self, jz_slice table, jazz_types_RowUuid row);
static jz_err ensure_exact_branch_row_absent(const DB *self, jz_slice table, const jazz_protocol_BranchSelector *branch, jazz_types_RowUuid row);
static jz_err ensure_row_deleted(const DB *self, jz_slice table, jazz_types_RowUuid row);
static jz_err row_layer_parents(const DB *self, jz_slice table, jazz_types_RowUuid row, void *content, void *deletion);
static jz_err update_target_missing(const DB *self, jz_slice table, jazz_types_RowUuid row, jz_err *out);
static jz_err local_row_for_client_identity(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_types_AuthorSubject identity, bool *has, jazz_node_CurrentRow *out);
static jz_err upsert_target_for_client_identity(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_types_AuthorSubject identity, bool *exists);
static jz_err upsert_target_for_trusted_identity(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_types_AuthorSubject identity, bool *exists);
static jz_err no_op_update_handle(const DB *self, jz_slice table, jazz_types_RowUuid row, bool has_existing, jazz_node_CurrentRow *existing, jazz_db_WriteHandle *out);
typedef JZ_VEC(jazz_model_TxId) TxIdVec;
static jz_err write_mergeable_at_ms_with_authorship_in_branch(const DB *self, jazz_types_AuthorSubject made_by, bool has_permission_subject,
    jazz_types_AuthorSubject permission_subject, jz_slice table, jazz_types_RowUuid row, jz_btree cells, TxIdVec parents, bool has_deletion,
    jazz_model_DeletionEvent deletion, bool has_authored_columns, jz_btree authored_columns, u64 now_ms, jazz_protocol_BranchSelector branch,
    bool has_verified_inherited_cells, jz_btree verified_inherited_cells, jazz_db_WriteHandle *out);

static void merged_cells_drop(MergedCells *m) { jz_btree_drop(&m->cells); jz_btree_drop(&m->authored_columns); }
static void staged_value_vec_drop(void *v) { jz_vec_drop(v, sizeof(jazz_db_NamedStaged), NULL); }

/* ---------------- large-value updates ---------------- */

/* Validate one descriptor against the row's `current` cells and apply it. */
static jz_err apply_large_value_splices(const DB *self, groove_Value value, groove_LargeValueKind expected_kind, const jazz_db_LargeValueUpdatePage *page,
                                        const jazz_db_LargeValueUpdateSplice *splices, u32 n_splices, LargeValueEdit *out);
static jz_err apply_large_value_json_set(const DB *self, groove_Value value, groove_LargeValueKind expected_kind, const jazz_db_JsonSetEdit *edits, u32 n_edits, LargeValueEdit *out);

static jz_err apply_large_value_update(const DB *self, jz_slice table, jazz_db_LargeValueUpdate *mutation, const jz_btree *current, jz_btree *touched,
                                       const jz_btree *patched /* is_patched closure */, jz_str *column_out, LargeValueEdit *out) {
    jz_str column = jz_str_clone(&mutation->column);
    if (jz_btree_contains(touched, &column)) {
        jz_str_drop(&column);
        return jz_err_new(JAZZ_EC_QUERY, "a large-value field may have one descriptor per update");
    }
    jz_str touched_key = jz_str_clone(&column);
    jz_btree_insert(touched, &touched_key, NULL, NULL);
    if (jz_btree_contains(patched, &column)) {
        jz_str_drop(&column);
        return jz_err_new(JAZZ_EC_QUERY, "a large-value field cannot be both patched and partially updated");
    }
    const groove_Value *found = jz_btree_get(current, &column);
    if (!found) {
        jz_err e = jz_err_fmt(JAZZ_EC_SCHEMA, "unknown column %.*s.%.*s", (int)table.len, (const char *)table.ptr, (int)column.len, column.ptr);
        jz_str_drop(&column);
        return e;
    }
    groove_Value value;
    groove__records__Value__clone(&value, found);
    groove_LargeValueKind column_kind;
    bool nullable_unused;
    jz_err e = large_value_column_kind(self, table, jz_str_slice(&column), &column_kind, &nullable_unused);
    if (e.code) { groove__records__Value__drop(&value); jz_str_drop(&column); return e; }
    if (mutation->tag == JAZZ_LVU_SPLICE)
        e = apply_large_value_splices(self, value, column_kind, &mutation->within, mutation->splices.ptr, mutation->splices.len, out);
    else
        e = apply_large_value_json_set(self, value, column_kind, mutation->edits.ptr, mutation->edits.len, out);
    if (e.code) { jz_str_drop(&column); return e; }
    *column_out = column;
    return JZ_OK;
}

static jz_err apply_large_value_splices(const DB *self, groove_Value value, groove_LargeValueKind expected_kind, const jazz_db_LargeValueUpdatePage *page,
                                        const jazz_db_LargeValueUpdateSplice *splices, u32 n_splices, LargeValueEdit *out) {
    memset(out, 0, sizeof(*out));
    bool nullable = unwrap_present_nullable(&value);
    if (!(expected_kind == GROOVE_LV_BYTES || expected_kind == GROOVE_LV_STRING)) {
        groove__records__Value__drop(&value);
        return jz_err_new(JAZZ_EC_SCHEMA, "partial splice requires a bytes or string column");
    }
    if (value.tag != GROOVE_V_LARGE) {
        preserve_nullable(&value, nullable);
        JZ_TRY(apply_inline_splices(&value, expected_kind, page, splices, n_splices));
        out->next = value;
        return JZ_OK;
    }
    groove_LargeValueRef current = *value.v.large;
    jz_free(value.v.large);
    if (current.kind != expected_kind) return jz_err_new(JAZZ_EC_SCHEMA, "stored large-value kind does not match its column schema");
    bool text = expected_kind == GROOVE_LV_STRING;
    u64 page_start, page_length;
    if (page->tag == JAZZ_LVP_BYTES && !text) {
        JZ_TRY(checked_range(page->from, page->to, current.byte_length, "byte"));
        page_start = page->from; page_length = page->to - page->from;
    } else if (page->tag == JAZZ_LVP_TEXT_UTF8 && text) {
        JZ_TRY(checked_range(page->from, page->to, current.byte_length, "UTF-8"));
        /* The edit primitive validates both endpoints before it admits a
         * text tail. Probe the page endpoints even for an empty splice list
         * so invalid selected pages never become no-ops. */
        u64 boundaries[2] = {page->from, page->to};
        for (u32 b = 0; b < 2; ++b) {
            u64 boundary = boundaries[b];
            if (boundary < current.byte_length) {
                jz_bytes byte = {0};
                jazz_node_NodeState *node = DB_LOCK(self);
                jz_err e = NS_FN(read_large_value_range)(node, &current, boundary, boundary + 1, &byte);
                DB_UNLOCK(self);
                if (e.code) return jazz_node__node__api_error__Error__From__from(e);
                bool continuation = byte.len > 0 && (byte.ptr[0] & 0xC0) == 0x80;
                jz_bytes_drop(&byte);
                if (continuation) return jz_err_new(JAZZ_EC_QUERY, "UTF-8 page splits a code point");
            }
        }
        page_start = page->from; page_length = page->to - page->from;
    } else if (page->tag == JAZZ_LVP_TEXT_UTF16 && text) {
        if (!current.has_utf16_length) return jz_err_new(JAZZ_EC_SCHEMA, "text descriptor lacks UTF-16 metrics");
        JZ_TRY(checked_range(page->from, page->to, current.utf16_length, "UTF-16"));
        page_start = page->from; page_length = page->to - page->from;
    } else if (page->tag == JAZZ_LVP_BYTES) {
        return jz_err_new(JAZZ_EC_SCHEMA, "byte splice requires a bytes cell");
    } else {
        return jz_err_new(JAZZ_EC_SCHEMA, "text splice requires a string cell");
    }
    StagedIdVec prior_claims = {0};
    bool has_final = false;
    groove_StagedLargeValue final_staged;
    jz_err e = JZ_OK;
    for (u32 i = 0; i < n_splices; ++i) {
        const jazz_db_LargeValueUpdateSplice *splice = &splices[i];
        if (splice->at + splice->delete_ < splice->at) { e = jz_err_new(JAZZ_EC_QUERY, "splice range overflows"); goto fail; }
        u64 end = splice->at + splice->delete_;
        if (splice->at > page_length || end > page_length) { e = jz_err_new(JAZZ_EC_QUERY, "splice deletion leaves the selected page"); goto fail; }
        u64 offset, delete_length, inserted_units;
        if (page->tag == JAZZ_LVP_BYTES || page->tag == JAZZ_LVP_TEXT_UTF8) {
            if (page_start + splice->at < page_start) { e = jz_err_new(JAZZ_EC_QUERY, "splice offset overflows"); goto fail; }
            offset = page_start + splice->at;
            delete_length = splice->delete_;
            inserted_units = splice->insert.len;
        } else {
            if (page_start + splice->at < page_start) { e = jz_err_new(JAZZ_EC_QUERY, "UTF-16 splice offset overflows"); goto fail; }
            u64 offset_utf16 = page_start + splice->at;
            if (offset_utf16 + splice->delete_ < offset_utf16) { e = jz_err_new(JAZZ_EC_QUERY, "UTF-16 splice range overflows"); goto fail; }
            u64 end_utf16 = offset_utf16 + splice->delete_;
            jazz_node_NodeState *node = DB_LOCK(self);
            u64 end_byte;
            e = utf16_splice_byte(NS_FN(large_text_utf16_offset_to_byte)(node, &current, offset_utf16, &offset));
            if (!e.code) e = utf16_splice_byte(NS_FN(large_text_utf16_offset_to_byte)(node, &current, end_utf16, &end_byte));
            DB_UNLOCK(self);
            if (e.code) goto fail;
            if (!jz_utf8_valid(splice->insert.ptr, splice->insert.len)) { e = jz_err_new(JAZZ_EC_QUERY, "text splice insert is not valid UTF-8"); goto fail; }
            delete_length = end_byte - offset;
            inserted_units = utf16_count(splice->insert.ptr, splice->insert.len);
        }
        jazz_node_NodeState *node;
        if ((e = NODE_FN(lock_for_large_value_staging)(self->node, &node)).code) goto fail;
        groove_StagedLargeValue staged;
        jz_err se = NS_FN(edit_and_stage_large_value)(node, current, offset, delete_length, jz_bytes_clone(&splice->insert), &staged);
        DB_UNLOCK(self);
        if (se.code) { e = jazz_node__node__api_error__Error__From__from(se); goto fail; }
        if (has_final) jz_vec_push(&prior_claims, &final_staged.id, sizeof(final_staged.id));
        final_staged = staged;
        has_final = true;
        current = final_staged.value_ref;
        if (page_length < splice->delete_ || page_length - splice->delete_ + inserted_units < page_length - splice->delete_) {
            e = jz_err_new(JAZZ_EC_QUERY, "splice page length overflows");
            goto fail;
        }
        page_length = page_length - splice->delete_ + inserted_units;
    }
    out->next.tag = GROOVE_V_LARGE;
    out->next.v.large = jz_memdup(&current, sizeof current);
    preserve_nullable(&out->next, nullable);
    out->has_staged = has_final;
    if (has_final) out->staged = final_staged;
    out->obsolete = prior_claims;
    return JZ_OK;
fail:
    jz_vec_drop(&prior_claims, sizeof(groove_StagedLargeValueId), NULL);
    return e;
}

static jz_err apply_large_value_json_set(const DB *self, groove_Value value, groove_LargeValueKind expected_kind, const jazz_db_JsonSetEdit *edits, u32 n_edits, LargeValueEdit *out) {
    memset(out, 0, sizeof(*out));
    if (expected_kind != GROOVE_LV_JSON) {
        groove__records__Value__drop(&value);
        return jz_err_new(JAZZ_EC_SCHEMA, "JSON set requires a JSON column");
    }
    bool nullable = unwrap_present_nullable(&value);
    jz_bytes source = {0};
    groove_LargeValueRef *large = NULL;
    if (value.tag == GROOVE_V_STRING) {
        source = (jz_bytes){(u8 *)value.v.s.ptr, value.v.s.len, value.v.s.cap};
    } else if (value.tag == GROOVE_V_LARGE && value.v.large->kind == GROOVE_LV_JSON) {
        large = value.v.large;
        jazz_node_NodeState *node = DB_LOCK(self);
        jz_err e = NS_FN(read_large_value_range)(node, large, 0, large->byte_length, &source);
        DB_UNLOCK(self);
        if (e.code) { jz_free(large); return jazz_node__node__api_error__Error__From__from(e); }
    } else {
        groove__records__Value__drop(&value);
        return jz_err_new(JAZZ_EC_SCHEMA, "JSON set requires a JSON cell");
    }
    jz_json *json = NULL;
    jz_err pe = jz_json_parse(jz_bytes_slice(&source), &json);
    jz_bytes_drop(&source);
    if (pe.code) {
        jz_err_drop(&pe);
        if (large) jz_free(large);
        return jz_err_new(JAZZ_EC_QUERY, "stored JSON is invalid");
    }
    for (u32 i = 0; i < n_edits; ++i) {
        jz_err e = jazz_db__db__mutations__apply_json_set(&json, jz_str_slice(&edits[i].at), jz_json_clone(edits[i].value));
        if (e.code) { jz_json_free(json); if (large) jz_free(large); return e; }
    }
    jz_str replacement = {0};
    jz_json_write(json, &replacement);
    jz_json_free(json);
    if (!large) {
        out->next.tag = GROOVE_V_STRING;
        out->next.v.s = replacement;
        preserve_nullable(&out->next, nullable);
        return JZ_OK;
    }
    jazz_node_NodeState *node;
    jz_err e = NODE_FN(lock_for_large_value_staging)(self->node, &node);
    if (e.code) { jz_str_drop(&replacement); jz_free(large); return e; }
    groove_StagedLargeValue staged;
    jz_bytes bytes = {(u8 *)replacement.ptr, replacement.len, replacement.cap};
    e = NS_FN(edit_and_stage_large_value)(node, *large, 0, large->byte_length, bytes, &staged);
    DB_UNLOCK(self);
    jz_free(large);
    if (e.code) return jazz_node__node__api_error__Error__From__from(e);
    out->next.tag = GROOVE_V_LARGE;
    out->next.v.large = jz_memdup(&staged.value_ref, sizeof staged.value_ref);
    preserve_nullable(&out->next, nullable);
    out->has_staged = true;
    out->staged = staged;
    return JZ_OK;
}

static void evict_staged_large_values(const DB *self, const groove_StagedLargeValueId *ids, u32 n) {
    /* Settle earlier local publications first: while one is resident,
     * Groove defers eviction and the root would wait for the staging TTL. */
    jazz_node_NodeState *node;
    jz_err e = NODE_FN(lock_for_large_value_staging)(self->node, &node);
    if (e.code) { jz_err_drop(&e); return; }
    for (u32 i = 0; i < n; ++i) {
        jz_err r = NS_FN(evict_staged_large_value)(node, ids[i]);
        jz_err_drop(&r);
    }
    DB_UNLOCK(self);
}

static jz_err update_with_large_value_mutations_at_ms_option(const DB *self, jz_slice table, jazz_types_RowUuid row, jz_btree patch,
                                                             jazz_db_LargeValueUpdateVec mutations, bool has_now_ms, u64 now_ms, jazz_db_WriteHandle *out) {
    if (mutations.len == 0) {
        jz_vec_drop(&mutations, sizeof(jazz_db_LargeValueUpdate), jazz_db__db__mutations__LargeValueUpdate__drop);
        jazz_db_UpdateOptions options;
        memset(&options, 0, sizeof options);
        options.has_updated_at_ms = has_now_ms;
        options.updated_at_ms = now_ms;
        return DB_FN(update)(self, table, row, patch, options, out);
    }
    MergedCells merged;
    jz_err e = merge_existing_cells(self, table, row, patch, &merged);
    if (e.code) { jz_vec_drop(&mutations, sizeof(jazz_db_LargeValueUpdate), jazz_db__db__mutations__LargeValueUpdate__drop); return e; }
    JZ_VEC(jazz_db_NamedStaged) staged = {0};
    StagedIdVec cleanup = {0};
    jazz_node_PublishedTransaction *published = NULL;
    jz_err result = JZ_OK;
    {
        jz_btree touched;
        str_set_init(&touched);
        for (u32 i = 0; i < mutations.len; ++i) {
            jz_str column;
            LargeValueEdit edit;
            result = apply_large_value_update(self, table, &mutations.ptr[i], &merged.cells, &touched, &merged.authored_columns, &column, &edit);
            if (result.code) break;
            jz_vec_extend(&cleanup, edit.obsolete.ptr, edit.obsolete.len, sizeof(groove_StagedLargeValueId));
            jz_vec_drop(&edit.obsolete, sizeof(groove_StagedLargeValueId), NULL);
            if (edit.has_staged) {
                jz_vec_push(&cleanup, &edit.staged.id, sizeof(edit.staged.id));
                jazz_db_NamedStaged named = {jz_str_clone(&column), edit.staged};
                jz_vec_push(&staged, &named, sizeof named);
            }
            jz_str key = jz_str_clone(&column);
            jz_btree_insert(&merged.cells, &key, &edit.next, NULL);
            jz_btree_insert(&merged.authored_columns, &column, NULL, NULL);
        }
        jz_btree_drop(&touched);
        if (!result.code) {
            jazz_node_MergeableCommit *commit = jazz_node__node__MergeableCommit__new(table, row, has_now_ms ? now_ms : DB_FN(next_now_ms)(self));
            jazz_node__node__MergeableCommit__made_by(commit, self->identity.author);
            jazz_node__node__MergeableCommit__cells(commit, merged.cells);
            jazz_node__node__MergeableCommit__authored_columns(commit, merged.authored_columns);
            cells_init(&merged.cells);
            str_set_init(&merged.authored_columns);
            jazz_node_NodeState *node;
            result = NODE_FN(lock_for_large_value_staging)(self->node, &node);
            if (!result.code) {
                jz_err se = staged.len == 0 ? NS_FN(seal_inherited_large_values)(node, commit, self->schema_version_id, true)
                                            : NS_FN(seal_large_value_updates)(node, commit, staged.ptr, staged.len, self->schema_version_id);
                if (se.code) result = jazz_node__node__api_error__Error__From__from(se);
                else result = commit_mergeable_batch(self, node, &commit, 1, &published);
                DB_UNLOCK(self);
            } else {
                jazz_node__node__MergeableCommit__drop(commit);
            }
        }
    }
    /* Every intermediate descriptor has been restaged by its successor;
     * retaining its claim would merely postpone collectible bytes. The final
     * claim is consumed by `seal_large_value_updates` on success. */
    if (!result.code) {
        u32 kept = 0;
        for (u32 i = 0; i < cleanup.len; ++i) {
            bool consumed = false;
            for (u32 j = 0; j < staged.len; ++j)
                if (memcmp(&staged.ptr[j].staged.id, &cleanup.ptr[i], sizeof(groove_StagedLargeValueId)) == 0) consumed = true;
            if (!consumed) cleanup.ptr[kept++] = cleanup.ptr[i];
        }
        cleanup.len = kept;
    }
    for (u32 i = 0; i < cleanup.len; ++i) {
        jazz_node_NodeState *node = DB_LOCK(self);
        jz_err r = NS_FN(evict_staged_large_value)(node, cleanup.ptr[i]);
        DB_UNLOCK(self);
        jz_err_drop(&r);
    }
    jz_vec_drop(&cleanup, sizeof(groove_StagedLargeValueId), NULL);
    for (u32 j = 0; j < staged.len; ++j) jz_str_drop(&staged.ptr[j].column);
    staged_value_vec_drop(&staged);
    merged_cells_drop(&merged);
    jz_vec_drop(&mutations, sizeof(jazz_db_LargeValueUpdate), jazz_db__db__mutations__LargeValueUpdate__drop);
    JZ_TRY(result);
    return finish_published_write(self, row, published, out);
}

jz_err DB_FN(update_with_large_value_mutations_at_ms)(const DB *self, jz_slice table, jazz_types_RowUuid row, jz_btree patch,
                                                      jazz_db_LargeValueUpdateVec mutations, u64 now_ms, jazz_db_WriteHandle *out) {
    jz_err e = jazz_db__db__lifecycle__Db__ensure_mutation_operation_admitted(self);
    if (e.code) {
        jz_btree_drop(&patch);
        jz_vec_drop(&mutations, sizeof(jazz_db_LargeValueUpdate), jazz_db__db__mutations__LargeValueUpdate__drop);
        return e;
    }
    return update_with_large_value_mutations_at_ms_option(self, table, row, patch, mutations, true, now_ms, out);
}

/* Stage typed partial large-value descriptors inside an open transaction (#2087). */
jz_err DB_FN(stage_transaction_large_value_update)(const DB *self, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row,
                                                   jz_btree patch, jazz_db_LargeValueUpdateVec mutations, bool has_updated_at_ms, u64 updated_at_ms) {
    u64 now_ms = has_updated_at_ms ? updated_at_ms : DB_FN(next_now_ms)(self);
    jz_btree authored = patch;
    jz_btree target;
    bool exclusive = false, has_target = false;
    jz_err result = jazz_db__db__transactions__Db__transaction_is_exclusive(self, tx_id, &exclusive);
    if (!result.code) result = jazz_db__db__transactions__Db__transaction_target_for_write(self, tx_id, table, row, "UPDATE", false, &has_target, &target);
    if (result.code) {
        jz_btree_drop(&authored);
        jz_vec_drop(&mutations, sizeof(jazz_db_LargeValueUpdate), jazz_db__db__mutations__LargeValueUpdate__drop);
        return result;
    }
    if (!has_target) jz_panic("a required transaction write target is present");
    jz_btree staged; /* String -> StagedLargeValue */
    static const jz_btree_desc staged_desc = {sizeof(jz_str), sizeof(groove_StagedLargeValue), jz_cmp_str, (void (*)(void *))jz_str_drop, NULL};
    jz_btree_init(&staged, &staged_desc);
    {
        jz_btree touched;
        str_set_init(&touched);
        for (u32 i = 0; i < mutations.len && !result.code; ++i) {
            jz_str column;
            LargeValueEdit edit;
            result = apply_large_value_update(self, table, &mutations.ptr[i], &target, &touched, &authored, &column, &edit);
            if (result.code) break;
            evict_staged_large_values(self, edit.obsolete.ptr, edit.obsolete.len);
            jz_vec_drop(&edit.obsolete, sizeof(groove_StagedLargeValueId), NULL);
            /* An inline result that outgrew the inline limit is lowered now
             * rather than at commit, so a later splice in this transaction
             * stays local to the chunks it touches. */
            if (!edit.has_staged) {
                const jazz_node_TableSchema *schema;
                result = DB_FN(table_schema)(self, table, &schema);
                if (result.code) { groove__records__Value__drop(&edit.next); jz_str_drop(&column); break; }
                u8 semantic_kind = JAZZ_LV_SEM_NOT_LARGE;
                for (u32 c = 0; c < schema->columns.len; ++c)
                    if (jz_str_eq(&schema->columns.ptr[c].name, &column)) { semantic_kind = schema->columns.ptr[c].large_value_kind; break; }
                jazz_node_NodeState *node;
                result = NODE_FN(lock_for_large_value_staging)(self->node, &node);
                if (result.code) { groove__records__Value__drop(&edit.next); jz_str_drop(&column); break; }
                jz_err se = NS_FN(prepare_and_stage_large_scalar)(node, &edit.next, semantic_kind, &edit.has_staged, &edit.staged);
                DB_UNLOCK(self);
                if (se.code) { result = jazz_node__node__api_error__Error__From__from(se); groove__records__Value__drop(&edit.next); jz_str_drop(&column); break; }
            }
            if (edit.has_staged) {
                jz_str key = jz_str_clone(&column);
                jz_btree_insert(&staged, &key, &edit.staged, NULL);
            } else {
                const groove_Value *existing = jz_btree_get(&target, &column);
                if (existing && groove__records__Value__eq(existing, &edit.next)) {
                    /* An empty splice list or an identical JSON value changes
                     * nothing; leave the column unauthored. */
                    groove__records__Value__drop(&edit.next);
                    jz_str_drop(&column);
                    continue;
                }
            }
            jz_btree_insert(&authored, &column, &edit.next, NULL);
        }
        jz_btree_drop(&touched);
        if (!result.code && authored.len != 0) {
            jazz_node_NodeState *node;
            result = jazz_db__db__transactions__Db__lock_for_transaction_operation(self, tx_id, &node);
            if (!result.code) {
                jz_btree staged_clone;
                jz_btree_clone(&staged_clone, &staged, str_clone_fn, NULL);
                jz_err ne;
                if (exclusive) {
                    jz_btree cells = cells_clone(&target);
                    cells_extend(&cells, &authored);
                    cells_init(&authored);
                    ne = NS_FN(tx_write_in_schema_with_staged_large_values)(node, tx_id, self->schema_version_id, table, row, cells, NULL, true, now_ms, staged_clone);
                } else {
                    jazz_protocol_BranchSelector branch;
                    jazz_protocol__protocol__BranchSelector__default(&branch);
                    ne = NS_FN(tx_patch_mergeable_with_staged_large_values)(node, tx_id, self->schema_version_id, table, row, authored, true, now_ms, branch, false, staged_clone);
                    cells_init(&authored);
                }
                DB_UNLOCK(self);
                if (ne.code) result = jazz_node__node__api_error__Error__From__from(ne);
            }
        }
    }
    if (result.code) {
        /* The transaction never took ownership of these roots. */
        JZ_VEC(groove_StagedLargeValueId) ids = {0};
        jz_str *k;
        groove_StagedLargeValue *v;
        JZ_BTREE_FOR(&staged, k, v) jz_vec_push(&ids, &v->id, sizeof v->id);
        evict_staged_large_values(self, ids.ptr, ids.len);
        jz_vec_drop(&ids, sizeof(groove_StagedLargeValueId), NULL);
    }
    jz_btree_drop(&staged);
    jz_btree_drop(&authored);
    jz_btree_drop(&target);
    jz_vec_drop(&mutations, sizeof(jazz_db_LargeValueUpdate), jazz_db__db__mutations__LargeValueUpdate__drop);
    return result;
}

/* ---------------- insert / streaming ---------------- */

jz_err DB_FN(insert)(const DB *self, jz_slice table, jz_btree cells, jazz_db_InsertOptions options, jazz_db_WriteHandle *out) {
    jz_err e = jazz_db__db__lifecycle__Db__ensure_mutation_operation_admitted(self);
    if (!e.code) e = reject_attributed_branch_target(self, options.identity, options.target.is_branch);
    if (!e.code) e = jazz_db__db__mutations__validate_updated_at_ms(options.has_updated_at_ms, options.updated_at_ms);
    if (e.code) { jz_btree_drop(&cells); return e; }
    bool supplied_row_id = options.has_row_id;
    jazz_types_RowUuid row = options.has_row_id ? options.row_id : self->row_id_source->vt->next_row_id(self->row_id_source->self);
    jazz_db_ResolvedWriteIdentity resolved;
    e = DB_FN(resolve_write_identity)(self, options.identity, &resolved);
    if (e.code) { jz_btree_drop(&cells); return e; }
    jazz_protocol_BranchSelector branch;
    jazz_db__db__ExactWriteTarget__branch(&options.target, &branch);
    if (supplied_row_id) {
        e = !options.target.is_branch ? ensure_row_absent(self, table, row) : ensure_exact_branch_row_absent(self, table, &branch, row);
        if (e.code) { jz_btree_drop(&cells); jazz_protocol__protocol__BranchSelector__drop(&branch); return e; }
    }
    jz_btree none;
    return write_mergeable_at_ms_with_authorship_in_branch(self, resolved.made_by, resolved.has_permission_subject, resolved.permission_subject, table, row,
                                                           cells, (TxIdVec){0}, false, JAZZ_DELETION_DELETED, false, none,
                                                           options.has_updated_at_ms ? options.updated_at_ms : DB_FN(next_now_ms)(self), branch, false, none, out);
}

/* Box::new(move |chunk| { emitted_for_stage.borrow_mut().push(chunk); Ok(()) }) */
static jz_err streaming_emit_chunk(void *ctx, groove_StagedChunk chunk) {
    jz_vec_push(ctx, &chunk, sizeof chunk);
    return JZ_OK;
}
static void streaming_emit_drop(void *ctx) { jz_rc_release(ctx); }

/* Begin a resumable push preparation without retaining the logical value. */
jz_err DB_FN(begin_streaming_value_upload)(const DB *self, jz_slice table, const jz_btree *cells, jz_slice column, jazz_db_StreamingValueUpload *out) {
    JZ_TRY(jazz_db__db__lifecycle__Db__ensure_mutation_operation_admitted(self));
    groove_LargeValueKind kind;
    bool nullable;
    JZ_TRY(validate_streaming_column(self, table, cells, column, &kind, &nullable));
    JZ_VEC(groove_StagedChunk) *emitted = jz_rc_new(sizeof(*emitted), NULL);
    groove_ChunkSink sink = {streaming_emit_chunk, jz_rc_clone(emitted), streaming_emit_drop};
    groove_PushStreamingPreparation *preparation = groove__large_values__PushStreamingPreparation__new(kind, sink);
    jz_uuid id = jz_uuid_v4();
    memcpy(out->id.b, id.b, 16);
    out->kind = kind;
    out->initialized = false;
    out->preparation = preparation;
    out->emitted = (void *)emitted;
    return JZ_OK;
}

static void take_emitted(jazz_db_StreamingValueUpload *upload, JZ_VEC(groove_StagedChunk) *chunks) {
    *chunks = *(__typeof__(chunks))upload->emitted;
    memset(upload->emitted, 0, sizeof(*chunks));
}

/* Feed one host chunk and durably stage every tree node finalized by it. */
jz_err DB_FN(push_streaming_value_upload)(const DB *self, jazz_db_StreamingValueUpload *upload, const u8 *bytes, u32 n) {
    JZ_TRY(jazz_db__db__lifecycle__Db__ensure_mutation_operation_admitted(self));
    bool initialized_now = !upload->initialized;
    if (!upload->initialized) {
        jazz_node_NodeState *node;
        JZ_TRY(NODE_FN(lock_for_large_value_staging)(self->node, &node));
        TRY_NODE_UNLOCK(self, NS_FN(begin_streaming_large_value_upload)(node, upload->id, upload->kind));
        DB_UNLOCK(self);
        upload->initialized = true;
    }
    if (!upload->preparation) return jz_err_new(JAZZ_EC_SCHEMA, "streaming upload is closed");
    jz_err push_result = groove__large_values__PushStreamingPreparation__push(upload->preparation, bytes, n);
    if (push_result.code) {
        groove__large_values__PushStreamingPreparation__drop(upload->preparation);
        upload->preparation = NULL;
        jazz_node_NodeState *node;
        jz_err e = NODE_FN(lock_for_large_value_staging)(self->node, &node);
        if (e.code) { jz_err_drop(&push_result); return e; }
        e = NS_FN(evict_pending_large_value_upload)(node, upload->id);
        DB_UNLOCK(self);
        if (e.code) { jz_err_drop(&push_result); return jazz_node__node__api_error__Error__From__from(e); }
        return jazz_node__node__api_error__Error__From__from(jazz_node__node__Error__From_groove_large_values_Error__from(push_result));
    }
    JZ_VEC(groove_StagedChunk) chunks;
    take_emitted(upload, (void *)&chunks);
    /* The begin operation already persisted the journal under the same node
     * lock. Avoid immediately re-admitting an empty first flush; every
     * subsequent push, including an empty one, proves liveness. */
    if (initialized_now && chunks.len == 0) {
        jz_vec_drop(&chunks, sizeof(groove_StagedChunk), NULL);
        return JZ_OK;
    }
    jz_err stage_result;
    {
        jazz_node_NodeState *node;
        jz_err e = NODE_FN(lock_for_large_value_staging)(self->node, &node);
        if (e.code) { jz_vec_drop(&chunks, sizeof(groove_StagedChunk), groove__large_values__StagedChunk__drop); return e; }
        stage_result = NS_FN(stage_large_value_chunk_batch)(node, upload->id, upload->kind, (void *)&chunks);
        DB_UNLOCK(self);
    }
    if (stage_result.code) {
        groove__large_values__PushStreamingPreparation__drop(upload->preparation);
        upload->preparation = NULL;
        jazz_node_NodeState *node;
        jz_err e = NODE_FN(lock_for_large_value_staging)(self->node, &node);
        if (!e.code) {
            jz_err r = NS_FN(evict_pending_large_value_upload)(node, upload->id);
            jz_err_drop(&r);
            DB_UNLOCK(self);
        } else {
            jz_err_drop(&e);
        }
        return jazz_node__node__api_error__Error__From__from(stage_result);
    }
    return JZ_OK;
}

/* Abort a resumable upload and release its persisted pending retainers. */
jz_err DB_FN(abort_streaming_value_upload)(const DB *self, jazz_db_StreamingValueUpload upload) {
    jz_err e = jazz_db__db__lifecycle__Db__ensure_mutation_operation_admitted(self);
    if (e.code) { streaming_value_upload_drop(&upload); return e; }
    groove_StagedLargeValueId id = jazz_db__db__mutations__StreamingValueUpload__cleanup_id(&upload);
    jazz_node_NodeState *node;
    JZ_TRY(NODE_FN(lock_for_large_value_staging)(self->node, &node));
    TRY_NODE_UNLOCK(self, NS_FN(evict_pending_large_value_upload)(node, id));
    DB_UNLOCK(self);
    return JZ_OK;
}

/* publish_in_writer_order closure state */
typedef struct StreamingPublish {
    DB *db;
    jazz_db_StreamingMutationKind mutation;
    jz_str table_name, column_name;
    jazz_types_RowUuid row;
    jz_btree cells;
    groove_StagedLargeValue staged;
    bool nullable;
    jazz_types_AuthorSubject made_by;
    bool has_permission_subject;
    jazz_types_AuthorSubject permission_subject;
    u64 now_ms;
    bool has_head;
    jazz_protocol_BranchSelector head;
    jazz_protocol_BranchViewBase *base;
    jazz_db_Oneshot *sender;
    bool consumed;
} StreamingPublish;
typedef struct WriteResult { jz_err err; jazz_db_WriteHandle write; } WriteResult;

static jz_err publish_streaming_value_with_id(const DB *self, jazz_db_StreamingMutationKind mutation, jz_slice table, jazz_types_RowUuid row, jz_btree cells,
                                              jz_slice column, groove_StagedLargeValue staged, bool nullable, jazz_types_AuthorSubject made_by,
                                              bool has_permission_subject, jazz_types_AuthorSubject permission_subject, bool has_now_ms, u64 now_ms,
                                              const jazz_protocol_BranchSelector *head, jazz_protocol_BranchViewBase *base, jazz_db_WriteHandle *out);

static bool streaming_publish_poll(void *self, const jz_waker *cx, void *out) {
    (void)cx;
    StreamingPublish *p = self;
    WriteResult result;
    p->consumed = true;
    result.err = publish_streaming_value_with_id(p->db, p->mutation, jz_str_slice(&p->table_name), p->row, p->cells, jz_str_slice(&p->column_name), p->staged,
                                                 p->nullable, p->made_by, p->has_permission_subject, p->permission_subject, true, p->now_ms,
                                                 p->has_head ? &p->head : NULL, p->base, &result.write);
    /* let outcome = result.as_ref().map(|_| ()).map_err(Clone::clone); */
    jz_err outcome = result.err.code ? jazz_db_err_clone(&result.err) : JZ_OK;
    if (!jazz_db__oneshot__send(p->sender, &result)) {
        if (result.err.code) jz_err_drop(&result.err); else jazz_db__db__WriteHandle__drop(&result.write);
    }
    *(jz_err *)out = outcome;
    return true;
}
static void streaming_publish_drop(void *self) {
    StreamingPublish *p = self;
    if (!p->consumed) jz_btree_drop(&p->cells);
    jz_str_drop(&p->table_name);
    jz_str_drop(&p->column_name);
    if (p->has_head) jazz_protocol__protocol__BranchSelector__drop(&p->head);
    jazz_db__oneshot__drop_sender(p->sender);
    jazz_db__db__Db__drop(p->db);
    jz_free(p);
}

/* Publish a write that was not queued when it was made (a finished streamed
 * upload) in the writer's order (`INV-HIST-23`). */
static jz_err publish_in_writer_order(const DB *self, u64 now_ms, StreamingPublish *publish, jazz_db_WriteHandle *out) {
    jazz_model_TxId tx_id;
    jz_err e = jazz_db__db__lifecycle__Db__reserve_transaction_id_at_ms(self, now_ms, &tx_id);
    if (e.code) { streaming_publish_drop(publish); return e; }
    publish->db = jazz_db__db__lifecycle__Db__clone_for_reserved_transaction(self, tx_id);
    jazz_db_Oneshot *receiver = jazz_db__oneshot__channel(sizeof(WriteResult));
    publish->sender = receiver;
    receiver->refs++;
    NODE_FN(enqueue_reserved_write)(self->node, tx_id, (jazz_db_QueuedMutationFuture){streaming_publish_poll, streaming_publish_drop, publish});
    for (;;) {
        WriteResult result;
        if (jazz_db__oneshot__try_recv(receiver, &result)) {
            jazz_db__oneshot__drop_receiver(receiver);
            if (result.err.code) return result.err;
            *out = result.write;
            return JZ_OK;
        }
        if (jazz_db__oneshot__is_canceled(receiver)) {
            jazz_db__oneshot__drop_receiver(receiver);
            return jz_err_new(JAZZ_EC_WRITE_REJECTED, "database closed before the write was published");
        }
        u32 queued = NODE_FN(queued_mutation_count)(self->node);
        NODE_FN(poll_queued_mutation_once)(self->node);
        if (NODE_FN(queued_mutation_count)(self->node) >= queued) {
            /* The queue's front operation waits; its wake drives the queue
             * again and the publication then wakes this wait. In the
             * synchronous port the host turn runs ready tasks before retrying. */
            jz_task_run_ready();
        }
    }
}

/* Finish a resumable upload and publish its ordinary row mutation. */
jz_err DB_FN(finish_streaming_value_upload)(const DB *self, jazz_db_StreamingValueUpload upload, jazz_db_StreamingMutationKind mutation, jz_slice table,
                                            jazz_types_RowUuid row, jz_btree cells, jz_slice column, jazz_db_WriteIdentity identity, bool has_now_ms,
                                            u64 now_ms, const jazz_protocol_BranchSelector *head, jazz_protocol_BranchViewBase *base, jazz_db_WriteHandle *out) {
    jz_err e = jazz_db__db__lifecycle__Db__ensure_mutation_operation_admitted(self);
    if (e.code) { streaming_value_upload_drop(&upload); jz_btree_drop(&cells); return e; }
    if ((e = reject_attributed_branch_target(self, identity, head != NULL || base != NULL)).code) {
        jz_btree_drop(&cells);
        JZ_TRY(DB_FN(abort_streaming_value_upload)(self, upload));
        return e;
    }
    jazz_db_ResolvedWriteIdentity resolved;
    if ((e = DB_FN(resolve_write_identity)(self, identity, &resolved)).code) {
        jz_btree_drop(&cells);
        JZ_TRY(DB_FN(abort_streaming_value_upload)(self, upload));
        return e;
    }
    if (!upload.initialized) {
        jazz_node_NodeState *node;
        e = NODE_FN(lock_for_large_value_staging)(self->node, &node);
        if (!e.code) {
            jz_err ne = NS_FN(begin_streaming_large_value_upload)(node, upload.id, upload.kind);
            DB_UNLOCK(self);
            if (ne.code) e = jazz_node__node__api_error__Error__From__from(ne);
        }
        if (e.code) { jz_btree_drop(&cells); streaming_value_upload_drop(&upload); return e; }
        upload.initialized = true;
    }
    groove_PushStreamingPreparation *preparation = upload.preparation;
    upload.preparation = NULL;
    if (!preparation) { jz_btree_drop(&cells); streaming_value_upload_drop(&upload); return jz_err_new(JAZZ_EC_SCHEMA, "streaming upload is closed"); }
    groove_LargeValueRef value_ref;
    jz_err fe = groove__large_values__PushStreamingPreparation__finish(preparation, &value_ref);
    groove__large_values__PushStreamingPreparation__drop(preparation);
    if (fe.code) {
        jazz_node_NodeState *node;
        jz_err le = NODE_FN(lock_for_large_value_staging)(self->node, &node);
        if (!le.code) { jz_err r = NS_FN(evict_pending_large_value_upload)(node, upload.id); jz_err_drop(&r); DB_UNLOCK(self); }
        else jz_err_drop(&le);
        jz_btree_drop(&cells);
        streaming_value_upload_drop(&upload);
        return jazz_node__node__api_error__Error__From__from(jazz_node__node__Error__From_groove_large_values_Error__from(fe));
    }
    JZ_VEC(groove_StagedChunk) chunks;
    take_emitted(&upload, (void *)&chunks);
    groove_StagedLargeValue staged;
    jz_err staged_result;
    {
        jazz_node_NodeState *node;
        e = NODE_FN(lock_for_large_value_staging)(self->node, &node);
        if (e.code) { jz_btree_drop(&cells); jz_vec_drop(&chunks, sizeof(groove_StagedChunk), groove__large_values__StagedChunk__drop); streaming_value_upload_drop(&upload); return e; }
        staged_result = NS_FN(stage_large_value_chunk_batch)(node, upload.id, upload.kind, (void *)&chunks);
        if (!staged_result.code) staged_result = NS_FN(finalize_large_value_upload)(node, upload.id, value_ref, &staged);
        DB_UNLOCK(self);
    }
    if (staged_result.code) {
        /* Cleanup is best-effort here so the terminal operation reports its
         * original staging/finalization failure. */
        jazz_node_NodeState *node;
        jz_err le = NODE_FN(lock_for_large_value_staging)(self->node, &node);
        if (!le.code) { jz_err r = NS_FN(evict_pending_large_value_upload)(node, upload.id); jz_err_drop(&r); DB_UNLOCK(self); }
        else jz_err_drop(&le);
        jz_btree_drop(&cells);
        streaming_value_upload_drop(&upload);
        return jazz_node__node__api_error__Error__From__from(staged_result);
    }
    streaming_value_upload_drop(&upload);
    groove_LargeValueKind expected_kind;
    bool nullable;
    e = validate_streaming_column(self, table, &cells, column, &expected_kind, &nullable);
    if (e.code || expected_kind != staged.value_ref.kind) {
        jazz_node_NodeState *node = DB_LOCK(self);
        jz_err r = NS_FN(evict_staged_large_value)(node, staged.id);
        DB_UNLOCK(self);
        jz_err_drop(&r);
        jz_btree_drop(&cells);
        return e.code ? e : large_value_cell_type_error(table, column);
    }
    groove_StagedLargeValueId staged_id = staged.id;
    /* The row write takes its place in the writer's order now, behind every
     * write already queued on this owner (`INV-HIST-23`). */
    u64 publish_now_ms = has_now_ms ? now_ms : DB_FN(next_now_ms)(self);
    StreamingPublish *p = JZ_NEW(StreamingPublish);
    p->mutation = mutation;
    p->table_name = jz_str_from_slice(table);
    p->column_name = jz_str_from_slice(column);
    p->row = row;
    p->cells = cells;
    p->staged = staged;
    p->nullable = nullable;
    p->made_by = resolved.made_by;
    p->has_permission_subject = resolved.has_permission_subject;
    p->permission_subject = resolved.permission_subject;
    p->now_ms = publish_now_ms;
    p->has_head = head != NULL;
    if (head) jazz_protocol__protocol__BranchSelector__clone(&p->head, head);
    p->base = base;
    jz_err published = publish_in_writer_order(self, publish_now_ms, p, out);
    if (published.code) {
        /* Finalization transfers the pending journal into a staged root, but
         * publication is still fallible. Do not make cleanup mask the
         * caller-visible admission error. */
        jazz_node_NodeState *node = DB_LOCK(self);
        jz_err r = NS_FN(evict_staged_large_value)(node, staged_id);
        DB_UNLOCK(self);
        jz_err_drop(&r);
    }
    return published;
}

static jz_err validate_streaming_column(const DB *self, jz_slice table, const jz_btree *cells, jz_slice column, groove_LargeValueKind *kind, bool *nullable) {
    if (cells_contains(cells, column)) {
        return jz_err_fmt(JAZZ_EC_SCHEMA, "streamed column %.*s.%.*s was also supplied in row cells",
                          (int)table.len, (const char *)table.ptr, (int)column.len, (const char *)column.ptr);
    }
    return large_value_column_kind(self, table, column, kind, nullable);
}

/* Resolve the physical large-value kind solely from the declared column. */
static jz_err large_value_column_kind(const DB *self, jz_slice table, jz_slice column, groove_LargeValueKind *kind, bool *nullable) {
    const jazz_node_TableSchema *table_schema;
    JZ_TRY(DB_FN(table_schema)(self, table, &table_schema));
    const jazz_node_ColumnSchema *col = NULL;
    for (u32 i = 0; i < table_schema->columns.len; ++i) {
        const jz_str *name = &table_schema->columns.ptr[i].name;
        if (name->len == column.len && memcmp(name->ptr, column.ptr, column.len) == 0) { col = &table_schema->columns.ptr[i]; break; }
    }
    if (!col) {
        return jz_err_fmt(JAZZ_EC_SCHEMA, "unknown streamed column %.*s.%.*s",
                          (int)table.len, (const char *)table.ptr, (int)column.len, (const char *)column.ptr);
    }
    const groove_ValueType *column_type = &col->column_type;
    *nullable = false;
    if (column_type->tag == GROOVE_VT_NULLABLE) { column_type = column_type->inner; *nullable = true; }
    if (col->large_value_kind == JAZZ_LV_SEM_BYTES && column_type->tag == GROOVE_VT_BYTES) *kind = GROOVE_LV_BYTES;
    else if (col->large_value_kind == JAZZ_LV_SEM_STRING && column_type->tag == GROOVE_VT_STRING) *kind = GROOVE_LV_STRING;
    else if (col->large_value_kind == JAZZ_LV_SEM_JSON && column_type->tag == GROOVE_VT_STRING) *kind = GROOVE_LV_JSON;
    else return large_value_cell_type_error(table, jz_str_slice(&col->name));
    return JZ_OK;
}

/* Shared branch-head merge used by publish_streaming_value_with_id (Update/Upsert arms). */
typedef struct BranchMerge { jz_btree cells; TxIdVec parents; bool has_authored; jz_btree authored; bool inserting; } BranchMerge;

static jz_err publish_streaming_value_with_id(const DB *self, jazz_db_StreamingMutationKind mutation, jz_slice table, jazz_types_RowUuid row, jz_btree cells,
                                              jz_slice column, groove_StagedLargeValue staged, bool nullable, jazz_types_AuthorSubject made_by,
                                              bool has_permission_subject, jazz_types_AuthorSubject permission_subject, bool has_now_ms, u64 now_ms,
                                              const jazz_protocol_BranchSelector *head, jazz_protocol_BranchViewBase *base, jazz_db_WriteHandle *out) {
    jazz_protocol_BranchSelector branch;
    if (head) jazz_protocol__protocol__BranchSelector__clone(&branch, head);
    else jazz_protocol__protocol__BranchSelector__default(&branch);
    BranchMerge m = {0};
    jz_err e = JZ_OK;
    switch (mutation) {
    case JAZZ_SMK_INSERT:
        e = head ? ensure_exact_branch_row_absent(self, table, &branch, row) : ensure_row_absent(self, table, row);
        if (e.code) goto fail_cells;
        m.cells = cells;
        m.inserting = true;
        break;
    case JAZZ_SMK_UPDATE:
    case JAZZ_SMK_UPSERT: {
        bool upsert = mutation == JAZZ_SMK_UPSERT;
        jz_btree authored = cell_keys(&cells);
        jz_str col = jz_str_from_slice(column);
        jz_btree_insert(&authored, &col, NULL, NULL);
        if (head) {
            bool has_visible = false;
            jz_btree visible_to_session;
            if (has_permission_subject) {
                e = DB_FN(visible_branch_view_cells_for_identity)(self, table, head, base, row, permission_subject, &has_visible, &visible_to_session);
                if (e.code) { jz_btree_drop(&authored); goto fail_cells; }
                if (!upsert && !has_visible) { jz_btree_drop(&authored); e = jazz_db__db__read_for_write_denied("UPDATE", table); goto fail_cells; }
            }
            jazz_node_NodeState *node = DB_LOCK(self);
            bool has_current;
            jz_btree current;
            jz_err ne = NS_FN(visible_current_cells_in_branch)(node, table, head, row, &has_current, &current);
            if (ne.code) { DB_UNLOCK(self); if (has_visible) jz_btree_drop(&visible_to_session); jz_btree_drop(&authored); e = jazz_node__node__api_error__Error__From__from(ne); goto fail_cells; }
            if (has_current) {
                bool has_parent;
                jazz_model_TxId parent;
                ne = NS_FN(local_content_winner_tx_id_in_branch)(node, table, head, row, &has_parent, &parent);
                DB_UNLOCK(self);
                if (ne.code) { jz_btree_drop(&current); if (has_visible) jz_btree_drop(&visible_to_session); jz_btree_drop(&authored); e = jazz_node__node__api_error__Error__From__from(ne); goto fail_cells; }
                if (upsert && has_permission_subject && !has_visible) {
                    jz_btree_drop(&current); jz_btree_drop(&authored);
                    e = jazz_db__db__read_for_write_denied("UPSERT", table);
                    goto fail_cells;
                }
                if (has_visible) { jz_btree_drop(&current); current = visible_to_session; }
                cells_extend(&current, &cells);
                m.cells = current;
                if (has_parent) jz_vec_push(&m.parents, &parent, sizeof parent);
                m.has_authored = true; m.authored = authored;
                m.inserting = false;
            } else {
                bool has_inherited;
                jz_btree inherited;
                ne = NS_FN(visible_current_cells_in_branch_view)(node, table, head, base, row, &has_inherited, &inherited);
                DB_UNLOCK(self);
                if (ne.code) { if (has_visible) jz_btree_drop(&visible_to_session); jz_btree_drop(&authored); e = jazz_node__node__api_error__Error__From__from(ne); goto fail_cells; }
                if (!has_inherited) {
                    if (has_visible) jz_btree_drop(&visible_to_session);
                    if (!upsert) {
                        jz_btree_drop(&authored);
                        char id[37];
                        jz_uuid_to_string(&row, id);
                        e = jz_err_fmt(JAZZ_EC_NOT_OBSERVED, "row is not visible in branch view: %s", id);
                        goto fail_cells;
                    }
                    jz_btree_drop(&authored);
                    m.cells = cells;
                    m.inserting = true;
                } else {
                    if (upsert && has_permission_subject && !has_visible) {
                        jz_btree_drop(&inherited); jz_btree_drop(&authored);
                        e = jazz_db__db__read_for_write_denied("UPSERT", table);
                        goto fail_cells;
                    }
                    if (has_visible) { jz_btree_drop(&inherited); inherited = visible_to_session; }
                    cells_extend(&inherited, &cells);
                    m.cells = inherited;
                    m.has_authored = true; m.authored = authored;
                    m.inserting = true;
                }
            }
        } else {
            bool exists = true;
            if (upsert) {
                e = DB_FN(ensure_row_not_deleted)(self, table, row);
                if (!e.code) e = has_permission_subject ? upsert_target_for_trusted_identity(self, table, row, permission_subject, &exists)
                                                        : upsert_target_for_client_identity(self, table, row, self->identity.author, &exists);
                if (e.code) { jz_btree_drop(&authored); goto fail_cells; }
            }
            if (exists) {
                MergedCells merged;
                e = has_permission_subject ? merge_existing_cells_for_identity(self, table, row, cells, permission_subject, &merged)
                                           : merge_existing_cells(self, table, row, cells, &merged);
                if (e.code) { jz_btree_drop(&authored); goto fail; }
                jz_btree_drop(&merged.authored_columns);
                m.cells = merged.cells;
                if (merged.has_parent) jz_vec_push(&m.parents, &merged.parent, sizeof merged.parent);
                m.has_authored = true; m.authored = authored;
                m.inserting = false;
            } else {
                jz_btree_drop(&authored);
                m.cells = cells;
                m.inserting = true;
            }
        }
        break;
    }
    }
    jz_vec_drop(&m.parents, sizeof(jazz_model_TxId), NULL);
    if (m.inserting) {
        e = DB_FN(apply_insert_defaults)(self, table, &m.cells);
        if (e.code) { jz_btree_drop(&m.cells); if (m.has_authored) jz_btree_drop(&m.authored); goto fail; }
    }
    jazz_node_MergeableCommit *commit = jazz_node__node__MergeableCommit__new(table, row, has_now_ms ? now_ms : DB_FN(next_now_ms)(self));
    jazz_node__node__MergeableCommit__branch(commit, branch);
    jazz_node__node__MergeableCommit__made_by(commit, made_by);
    jazz_node__node__MergeableCommit__cells(commit, m.cells);
    if (m.has_authored) jazz_node__node__MergeableCommit__authored_columns(commit, m.authored);
    if (has_permission_subject) jazz_node__node__MergeableCommit__permission_subject(commit, permission_subject);
    jazz_node_PublishedTransaction *published;
    {
        jazz_node_NodeState *node;
        if (jazz_node__node__MergeableCommit__needs_large_value_staging(commit)) {
            e = NODE_FN(lock_for_large_value_staging)(self->node, &node);
            if (e.code) { jazz_node__node__MergeableCommit__drop(commit); return e; }
        } else {
            node = DB_LOCK(self);
        }
        jz_err ne = NS_FN(seal_inherited_large_values)(node, commit, self->schema_version_id, true);
        if (ne.code) { DB_UNLOCK(self); jazz_node__node__MergeableCommit__drop(commit); return jazz_node__node__api_error__Error__From__from(ne); }
        jazz_node__node__MergeableCommit__staged_large_cell(commit, column, staged, nullable);
        e = commit_mergeable_batch(self, node, &commit, 1, &published);
        DB_UNLOCK(self);
        if (e.code) return e;
    }
    return finish_published_write(self, row, published, out);
fail_cells:
    jz_btree_drop(&cells);
fail:
    jazz_protocol__protocol__BranchSelector__drop(&branch);
    return e;
}

/* Advise whether an insert may be allowed: a client-local replica never
 * turns a local policy evaluation into an allow/deny result. */
jz_err DB_FN(can_insert)(const DB *self, jz_slice table, jz_btree cells, jazz_protocol_PermissionAdvice *out) {
    (void)self; (void)table;
    jz_btree_drop(&cells);
    *out = JAZZ_ADVICE_UNKNOWN;
    return JZ_OK;
}

/* ---------------- update / upsert / delete ---------------- */

/* Update one row, optionally through a branch view. */
jz_err DB_FN(update)(const DB *self, jz_slice table, jazz_types_RowUuid row, jz_btree patch, jazz_db_UpdateOptions options, jazz_db_WriteHandle *out) {
    jz_err e = jazz_db__db__lifecycle__Db__ensure_mutation_operation_admitted(self);
    if (!e.code) e = reject_attributed_branch_target(self, options.identity, options.target.is_branch_view);
    if (!e.code) e = jazz_db__db__mutations__validate_updated_at_ms(options.has_updated_at_ms, options.updated_at_ms);
    jazz_db_ResolvedWriteIdentity r;
    if (!e.code) e = DB_FN(resolve_write_identity)(self, options.identity, &r);
    if (e.code) { jz_btree_drop(&patch); return e; }
    u64 now_ms = options.has_updated_at_ms ? options.updated_at_ms : DB_FN(next_now_ms)(self);
    jz_btree none;
    if (!options.target.is_branch_view) {
        if (patch.len == 0) {
            jz_btree_drop(&patch);
            JZ_TRY(DB_FN(ensure_row_not_deleted)(self, table, row));
            bool has_existing;
            jazz_node_CurrentRow existing;
            if (options.identity.tag == JAZZ_WID_SESSION)
                e = DB_FN(local_row_for_trusted_identity)(self, table, row, options.identity.author, &has_existing, &existing);
            else
                e = local_row_for_client_identity(self, table, row, r.has_permission_subject ? r.permission_subject : r.made_by, &has_existing, &existing);
            JZ_TRY(e);
            return no_op_update_handle(self, table, row, has_existing, &existing, out);
        }
        MergedCells merged;
        if (options.identity.tag == JAZZ_WID_SESSION) e = merge_existing_cells_for_identity(self, table, row, patch, options.identity.author, &merged);
        else e = merge_existing_cells(self, table, row, patch, &merged);
        JZ_TRY(e);
        TxIdVec parents = {0};
        if (merged.has_parent) jz_vec_push(&parents, &merged.parent, sizeof merged.parent);
        jazz_protocol_BranchSelector branch;
        jazz_protocol__protocol__BranchSelector__default(&branch);
        return write_mergeable_at_ms_with_authorship_in_branch(self, r.made_by, r.has_permission_subject, r.permission_subject, table, row, merged.cells,
                                                               parents, false, JAZZ_DELETION_DELETED, true, merged.authored_columns, now_ms, branch, false, none, out);
    }
    jazz_protocol_BranchSelector *head = &options.target.head;
    jazz_protocol_BranchViewBase *base = options.target.base;
    if (patch.len == 0) {
        jz_btree_drop(&patch);
        return jz_err_new(JAZZ_EC_SCHEMA, "branch update requires at least one authored column");
    }
    bool has_visible = false;
    jz_btree visible_to_session;
    if (options.identity.tag == JAZZ_WID_SESSION) {
        e = DB_FN(visible_branch_view_cells_for_identity)(self, table, head, base, row, options.identity.author, &has_visible, &visible_to_session);
        if (!e.code && !has_visible) e = jazz_db__db__read_for_write_denied("UPDATE", table);
        if (e.code) { jz_btree_drop(&patch); return e; }
    }
    bool has_local;
    jz_btree local;
    jazz_node_NodeState *node = DB_LOCK(self);
    jz_err ne = NS_FN(visible_current_cells_in_branch)(node, table, head, row, &has_local, &local);
    DB_UNLOCK(self);
    if (ne.code) { jz_btree_drop(&patch); if (has_visible) jz_btree_drop(&visible_to_session); return jazz_node__node__api_error__Error__From__from(ne); }
    jz_btree cells;
    TxIdVec parents = {0};
    bool has_authored = false, has_verified = false;
    jz_btree authored_columns, verified_inherited_cells;
    if (has_local) {
        bool has_parent;
        jazz_model_TxId parent;
        node = DB_LOCK(self);
        ne = NS_FN(local_content_winner_tx_id_in_branch)(node, table, head, row, &has_parent, &parent);
        DB_UNLOCK(self);
        if (ne.code) { jz_btree_drop(&patch); jz_btree_drop(&local); if (has_visible) jz_btree_drop(&visible_to_session); return jazz_node__node__api_error__Error__From__from(ne); }
        if (has_visible) { jz_btree_drop(&local); cells = visible_to_session; } else cells = local;
        if (has_parent) jz_vec_push(&parents, &parent, sizeof parent);
        has_authored = true;
        authored_columns = cell_keys(&patch);
    } else {
        bool has_inherited;
        jz_btree inherited;
        node = DB_LOCK(self);
        ne = NS_FN(visible_current_cells_in_branch_view)(node, table, head, base, row, &has_inherited, &inherited);
        DB_UNLOCK(self);
        if (ne.code) { jz_btree_drop(&patch); if (has_visible) jz_btree_drop(&visible_to_session); return jazz_node__node__api_error__Error__From__from(ne); }
        if (!has_inherited) {
            jz_btree_drop(&patch);
            if (has_visible) jz_btree_drop(&visible_to_session);
            char id[37];
            jz_uuid_to_string(&row, id);
            return jz_err_fmt(JAZZ_EC_NOT_OBSERVED, "row is not visible in branch view: %s", id);
        }
        if (has_visible) { jz_btree_drop(&inherited); cells = visible_to_session; } else cells = inherited;
        /* This complete cell map came from the locally observed branch base.
         * Retain it as engine-only provenance for unchanged large descriptors. */
        has_verified = true;
        verified_inherited_cells = cells_clone(&cells);
    }
    cells_extend(&cells, &patch);
    jazz_protocol_BranchSelector branch;
    jazz_protocol__protocol__BranchSelector__clone(&branch, head);
    return write_mergeable_at_ms_with_authorship_in_branch(self, r.made_by, r.has_permission_subject, r.permission_subject, table, row, cells, parents, false,
                                                           JAZZ_DELETION_DELETED, has_authored, authored_columns, now_ms, branch, has_verified,
                                                           verified_inherited_cells, out);
}

/* Insert or update one row through a single implementation. */
jz_err DB_FN(upsert)(const DB *self, jz_slice table, jazz_types_RowUuid row, jz_btree cells, jazz_db_UpsertOptions options, jazz_db_WriteHandle *out) {
    return DB_FN(upsert_with_insert_validation)(self, table, row, cells, options, false, out);
}

jz_err DB_FN(upsert_with_insert_validation)(const DB *self, jz_slice table, jazz_types_RowUuid row, jz_btree cells, jazz_db_UpsertOptions options,
                                            bool require_complete_insert, jazz_db_WriteHandle *out) {
    jz_err e = jazz_db__db__lifecycle__Db__ensure_mutation_operation_admitted(self);
    if (!e.code) e = reject_attributed_branch_target(self, options.identity, options.target.is_branch_view);
    if (!e.code) e = jazz_db__db__mutations__validate_updated_at_ms(options.has_updated_at_ms, options.updated_at_ms);
    jazz_db_ResolvedWriteIdentity r;
    if (!e.code) e = DB_FN(resolve_write_identity)(self, options.identity, &r);
    if (e.code) { jz_btree_drop(&cells); return e; }
    u64 now_ms = options.has_updated_at_ms ? options.updated_at_ms : DB_FN(next_now_ms)(self);
    jazz_protocol_BranchSelector branch;
    jz_btree out_cells;
    TxIdVec parents = {0};
    bool has_authored = false, has_verified = false;
    jz_btree authored_columns, verified_inherited_cells;
    bool session = options.identity.tag == JAZZ_WID_SESSION;
    if (!options.target.is_branch_view) {
        e = DB_FN(ensure_row_not_deleted)(self, table, row);
        bool exists = false;
        if (!e.code) e = session ? upsert_target_for_trusted_identity(self, table, row, options.identity.author, &exists)
                                 : upsert_target_for_client_identity(self, table, row, r.has_permission_subject ? r.permission_subject : r.made_by, &exists);
        if (e.code) { jz_btree_drop(&cells); return e; }
        if (exists) {
            MergedCells merged;
            e = session ? merge_existing_cells_for_identity(self, table, row, cells, options.identity.author, &merged)
                        : merge_existing_cells(self, table, row, cells, &merged);
            JZ_TRY(e);
            out_cells = merged.cells;
            if (merged.has_parent) jz_vec_push(&parents, &merged.parent, sizeof merged.parent);
            has_authored = true;
            authored_columns = merged.authored_columns;
        } else {
            out_cells = cells;
        }
        jazz_protocol__protocol__BranchSelector__default(&branch);
    } else {
        jazz_protocol_BranchSelector *head = &options.target.head;
        jazz_protocol_BranchViewBase *base = options.target.base;
        e = DB_FN(ensure_branch_view_row_not_deleted)(self, table, head, base, row);
        if (e.code) { jz_btree_drop(&cells); return e; }
        bool has_visible = false;
        jz_btree visible_to_session;
        if (session) {
            e = DB_FN(visible_branch_view_cells_for_identity)(self, table, head, base, row, options.identity.author, &has_visible, &visible_to_session);
            if (e.code) { jz_btree_drop(&cells); return e; }
        }
        jz_btree authored = cell_keys(&cells);
        jazz_node_NodeState *node = DB_LOCK(self);
        bool has_current;
        jz_btree current;
        jz_err ne = NS_FN(visible_current_cells_in_branch)(node, table, head, row, &has_current, &current);
        if (ne.code) {
            DB_UNLOCK(self);
            jz_btree_drop(&cells); jz_btree_drop(&authored); if (has_visible) jz_btree_drop(&visible_to_session);
            return jazz_node__node__api_error__Error__From__from(ne);
        }
        if (has_current) {
            if (cells.len == 0 || (session && !has_visible)) {
                DB_UNLOCK(self);
                e = cells.len == 0 ? jz_err_new(JAZZ_EC_SCHEMA, "branch upsert update requires at least one authored column")
                                   : jazz_db__db__read_for_write_denied("UPSERT", table);
                jz_btree_drop(&cells); jz_btree_drop(&authored); jz_btree_drop(&current); if (has_visible) jz_btree_drop(&visible_to_session);
                return e;
            }
            if (has_visible) { jz_btree_drop(&current); current = visible_to_session; }
            bool has_parent;
            jazz_model_TxId parent;
            ne = NS_FN(local_content_winner_tx_id_in_branch)(node, table, head, row, &has_parent, &parent);
            DB_UNLOCK(self);
            if (ne.code) { jz_btree_drop(&cells); jz_btree_drop(&authored); jz_btree_drop(&current); return jazz_node__node__api_error__Error__From__from(ne); }
            cells_extend(&current, &cells);
            out_cells = current;
            if (has_parent) jz_vec_push(&parents, &parent, sizeof parent);
            has_authored = true;
            authored_columns = authored;
        } else {
            bool has_inherited;
            jz_btree inherited;
            ne = NS_FN(visible_current_cells_in_branch_view)(node, table, head, base, row, &has_inherited, &inherited);
            DB_UNLOCK(self);
            if (ne.code) { jz_btree_drop(&cells); jz_btree_drop(&authored); if (has_visible) jz_btree_drop(&visible_to_session); return jazz_node__node__api_error__Error__From__from(ne); }
            if (has_inherited) {
                if (session && !has_visible) {
                    jz_btree_drop(&cells); jz_btree_drop(&authored); jz_btree_drop(&inherited);
                    return jazz_db__db__read_for_write_denied("UPSERT", table);
                }
                has_verified = true;
                verified_inherited_cells = cells_clone(&inherited);
                if (has_visible) { jz_btree_drop(&inherited); inherited = visible_to_session; }
                cells_extend(&inherited, &cells);
                out_cells = inherited;
                has_authored = true;
                authored_columns = authored;
            } else {
                jz_btree_drop(&authored);
                if (has_visible) jz_btree_drop(&visible_to_session);
                out_cells = cells;
            }
        }
        jazz_protocol__protocol__BranchSelector__clone(&branch, head);
    }
    /* Bindings submit an upsert patch without synchronously inspecting the
     * current row. Once the core has chosen an insertion, enforce required
     * fields here rather than relying on a JavaScript preflight query. */
    if (require_complete_insert && parents.len == 0) {
        e = DB_FN(validate_complete_insert)(self, table, &out_cells);
        if (e.code) {
            jz_btree_drop(&out_cells);
            if (has_authored) jz_btree_drop(&authored_columns);
            if (has_verified) jz_btree_drop(&verified_inherited_cells);
            jazz_protocol__protocol__BranchSelector__drop(&branch);
            return e;
        }
    }
    return write_mergeable_at_ms_with_authorship_in_branch(self, r.made_by, r.has_permission_subject, r.permission_subject, table, row, out_cells, parents,
                                                           false, JAZZ_DELETION_DELETED, has_authored, authored_columns, now_ms, branch, has_verified,
                                                           verified_inherited_cells, out);
}

/* Soft-delete one row, optionally through a branch view. */
jz_err DB_FN(delete)(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_db_DeleteOptions options, jazz_db_WriteHandle *out) {
    JZ_TRY(jazz_db__db__lifecycle__Db__ensure_mutation_operation_admitted(self));
    JZ_TRY(reject_attributed_branch_target(self, options.identity, options.target.is_branch_view));
    JZ_TRY(jazz_db__db__mutations__validate_updated_at_ms(options.has_updated_at_ms, options.updated_at_ms));
    jazz_db_ResolvedWriteIdentity r;
    JZ_TRY(DB_FN(resolve_write_identity)(self, options.identity, &r));
    u64 now_ms = options.has_updated_at_ms ? options.updated_at_ms : DB_FN(next_now_ms)(self);
    jazz_protocol_BranchSelector branch;
    TxIdVec parents = {0};
    if (!options.target.is_branch_view) {
        JZ_TRY(DB_FN(ensure_row_not_deleted)(self, table, row));
        TxIdVec content = {0};
        JZ_TRY(row_layer_parents(self, table, row, (void *)&content, (void *)&parents));
        jz_vec_drop(&content, sizeof(jazz_model_TxId), NULL);
        jazz_protocol__protocol__BranchSelector__default(&branch);
    } else {
        jazz_protocol_BranchSelector *head = &options.target.head;
        jazz_node_NodeState *node = DB_LOCK(self);
        bool has_local;
        jz_btree local;
        TRY_NODE_UNLOCK(self, NS_FN(visible_current_cells_in_branch)(node, table, head, row, &has_local, &local));
        bool has_deletion;
        jazz_model_TxId deletion;
        if (has_local) {
            jz_btree_drop(&local);
            /* A deletion is a write to the independent deletion register.
             * Content state proves visibility, but must not become a history
             * parent of that register. */
            TRY_NODE_UNLOCK(self, NS_FN(local_deletion_winner_tx_id_in_branch)(node, table, head, row, &has_deletion, &deletion));
        } else {
            bool has_view;
            jz_btree view;
            TRY_NODE_UNLOCK(self, NS_FN(visible_current_cells_in_branch_view)(node, table, head, options.target.base, row, &has_view, &view));
            if (!has_view) {
                DB_UNLOCK(self);
                char id[37];
                jz_uuid_to_string(&row, id);
                return jz_err_fmt(JAZZ_EC_NOT_OBSERVED, "row is not visible in branch view: %s", id);
            }
            jz_btree_drop(&view);
            TRY_NODE_UNLOCK(self, NS_FN(local_deletion_winner_tx_id_in_branch)(node, table, head, row, &has_deletion, &deletion));
        }
        DB_UNLOCK(self);
        if (has_deletion) jz_vec_push(&parents, &deletion, sizeof deletion);
        jazz_protocol__protocol__BranchSelector__clone(&branch, head);
    }
    jz_btree empty, none;
    cells_init(&empty);
    return write_mergeable_at_ms_with_authorship_in_branch(self, r.made_by, r.has_permission_subject, r.permission_subject, table, row, empty, parents, true,
                                                           JAZZ_DELETION_DELETED, false, none, now_ms, branch, false, none, out);
}

jz_err DB_FN(can_read)(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_protocol_PermissionAdvice *out) {
    (void)self; (void)table; (void)row;
    *out = JAZZ_ADVICE_UNKNOWN;
    return JZ_OK;
}

/* Evaluate a read for the serving path without disclosing data. */
jz_err DB_FN(authorize_read_for_identity)(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_types_AuthorSubject author, jazz_protocol_PermissionAdvice *out) {
    const jazz_node_TableSchema *schema;
    JZ_TRY(DB_FN(table_schema)(self, table, &schema));
    /* crate::local_executor::block_on(node.borrow_mut().dry_run_read_current_allows(..)) */
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(self->node->node);
    bool allowed;
    jz_err e = NS_FN(dry_run_read_current_allows)(node, table, row, author, &allowed);
    DB_UNLOCK(self);
    if (e.code) return jazz_node__node__api_error__Error__From__from(e);
    *out = allowed ? JAZZ_ADVICE_ALLOWED : JAZZ_ADVICE_DENIED;
    return JZ_OK;
}

jz_err DB_FN(can_update)(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_protocol_PermissionAdvice *out) {
    (void)self; (void)table; (void)row;
    *out = JAZZ_ADVICE_UNKNOWN;
    return JZ_OK;
}

/* Owner operation for set_identity_claims */
typedef struct SetClaimsOp { jazz_db_Node *runtime; jazz_types_AuthorSubject identity; jz_btree claims; bool consumed; } SetClaimsOp;
static bool set_claims_poll(void *self, const jz_waker *cx, void *out) {
    (void)cx;
    SetClaimsOp *op = self;
    jazz_node_NodeState *node = jazz_db_node_lock(op->runtime->node);
    u64 previous_revision = NS_FN(session_claim_revision)(node, op->identity);
    NS_FN(set_session_claims)(node, op->identity, op->claims);
    op->consumed = true;
    bool changed = NS_FN(session_claim_revision)(node, op->identity) != previous_revision;
    jazz_db_node_unlock(op->runtime->node);
    if (changed) NODE_FN(schedule_tick)(op->runtime, JAZZ_TICK_DEFERRED);
    *(jz_err *)out = JZ_OK;
    return true;
}
static void set_claims_drop(void *self) {
    SetClaimsOp *op = self;
    if (!op->consumed) jz_btree_drop(&op->claims);
    jz_free(op);
}

/* Attach process-local auth claims for `identity`, in admission order. */
void DB_FN(set_identity_claims)(const DB *self, jazz_types_AuthorSubject identity, jz_btree claims) {
    if (NODE_FN(owner_queue_is_quiescent)(self->node)) {
        jazz_node_NodeState *node = jazz_db_node_try_lock(self->node->node);
        if (node) {
            u64 previous_revision = NS_FN(session_claim_revision)(node, identity);
            NS_FN(set_session_claims)(node, identity, claims);
            bool changed = NS_FN(session_claim_revision)(node, identity) != previous_revision;
            DB_UNLOCK(self);
            if (changed) NODE_FN(schedule_tick)(self->node, JAZZ_TICK_DEFERRED);
            return;
        }
    }
    SetClaimsOp *op = JZ_NEW(SetClaimsOp);
    op->runtime = self->node;
    op->identity = identity;
    op->claims = claims;
    NODE_FN(enqueue_owner_operation)(self->node, (jazz_db_QueuedMutationFuture){set_claims_poll, set_claims_drop, op});
}

/* Attach claims without synchronously reentering a suspended storage operation. */
void DB_FN(set_identity_claims_async)(const DB *self, jazz_types_AuthorSubject identity, jz_btree claims) {
    jazz_node_NodeState *node = DB_LOCK(self);
    u64 previous_revision = NS_FN(session_claim_revision)(node, identity);
    NS_FN(set_session_claims)(node, identity, claims);
    bool changed = NS_FN(session_claim_revision)(node, identity) != previous_revision;
    DB_UNLOCK(self);
    if (changed) NODE_FN(schedule_tick)(self->node, JAZZ_TICK_DEFERRED);
}

jz_err DB_FN(can_delete)(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_protocol_PermissionAdvice *out) {
    (void)self; (void)table; (void)row;
    *out = JAZZ_ADVICE_UNKNOWN;
    return JZ_OK;
}

/* Restore a deleted row through one root-or-branch implementation. */
jz_err DB_FN(restore)(const DB *self, jz_slice table, jazz_types_RowUuid row, bool has_cells, jz_btree cells, jazz_db_RestoreOptions options, jazz_db_WriteHandle *out) {
    jz_err e = jazz_db__db__lifecycle__Db__ensure_mutation_operation_admitted(self);
    if (!e.code) e = reject_attributed_branch_target(self, options.identity, options.target.is_branch);
    if (!e.code) e = jazz_db__db__mutations__validate_updated_at_ms(options.has_updated_at_ms, options.updated_at_ms);
    jazz_db_ResolvedWriteIdentity r;
    if (!e.code) e = DB_FN(resolve_write_identity)(self, options.identity, &r);
    if (e.code) { if (has_cells) jz_btree_drop(&cells); return e; }
    jazz_protocol_BranchSelector branch;
    jazz_db__db__ExactWriteTarget__branch(&options.target, &branch);
    u64 now_ms = options.has_updated_at_ms ? options.updated_at_ms : DB_FN(next_now_ms)(self);
    if (has_cells && (e = DB_FN(apply_insert_defaults)(self, table, &cells)).code) {
        jz_btree_drop(&cells);
        jazz_protocol__protocol__BranchSelector__drop(&branch);
        return e;
    }
    TxIdVec content_parents = {0}, deletion_parents = {0};
    if (!options.target.is_branch) {
        e = ensure_row_deleted(self, table, row);
        if (!e.code) e = row_layer_parents(self, table, row, (void *)&content_parents, (void *)&deletion_parents);
    } else {
        jazz_node_NodeState *node = DB_LOCK(self);
        bool has_deletion, has_content;
        jazz_model_TxId deletion, content;
        jz_err ne = NS_FN(local_deletion_winner_tx_id_in_branch)(node, table, &branch, row, &has_deletion, &deletion);
        if (!ne.code && !has_deletion) {
            char id[37];
            jz_uuid_to_string(&row, id);
            e = jz_err_fmt(JAZZ_EC_NOT_OBSERVED, "branch deletion not observed: %s", id);
        } else if (!ne.code) {
            ne = NS_FN(local_content_winner_tx_id_in_branch)(node, table, &branch, row, &has_content, &content);
            if (!ne.code) {
                if (has_content) jz_vec_push(&content_parents, &content, sizeof content);
                jz_vec_push(&deletion_parents, &deletion, sizeof deletion);
            }
        }
        DB_UNLOCK(self);
        if (ne.code) e = jazz_node__node__api_error__Error__From__from(ne);
    }
    jz_vec_drop(&content_parents, sizeof(jazz_model_TxId), NULL);
    jz_vec_drop(&deletion_parents, sizeof(jazz_model_TxId), NULL);
    if (e.code) {
        if (has_cells) jz_btree_drop(&cells);
        jazz_protocol__protocol__BranchSelector__drop(&branch);
        return e;
    }
    jazz_node_MergeableCommit *commits[2];
    u32 n = 0;
    if (has_cells) {
        jazz_node_MergeableCommit *c = jazz_node__node__MergeableCommit__new(table, row, now_ms);
        jazz_protocol_BranchSelector b;
        jazz_protocol__protocol__BranchSelector__clone(&b, &branch);
        jazz_node__node__MergeableCommit__branch(c, b);
        jazz_node__node__MergeableCommit__made_by(c, r.made_by);
        jazz_node__node__MergeableCommit__cells(c, cells);
        if (r.has_permission_subject) jazz_node__node__MergeableCommit__permission_subject(c, r.permission_subject);
        commits[n++] = c;
    }
    {
        jazz_node_MergeableCommit *c = jazz_node__node__MergeableCommit__new(table, row, now_ms);
        jazz_node__node__MergeableCommit__branch(c, branch);
        jazz_node__node__MergeableCommit__made_by(c, r.made_by);
        jz_btree empty;
        cells_init(&empty);
        jazz_node__node__MergeableCommit__cells(c, empty);
        jazz_node__node__MergeableCommit__deletion(c, JAZZ_DELETION_RESTORED);
        if (r.has_permission_subject) jazz_node__node__MergeableCommit__permission_subject(c, r.permission_subject);
        commits[n++] = c;
    }
    jazz_node_PublishedTransaction *published;
    {
        bool staging = false;
        for (u32 i = 0; i < n; ++i) staging |= jazz_node__node__MergeableCommit__needs_large_value_staging(commits[i]);
        jazz_node_NodeState *node;
        if (staging) {
            e = NODE_FN(lock_for_large_value_staging)(self->node, &node);
            if (e.code) { for (u32 i = 0; i < n; ++i) jazz_node__node__MergeableCommit__drop(commits[i]); return e; }
        } else {
            node = DB_LOCK(self);
        }
        e = commit_mergeable_batch(self, node, commits, n, &published);
        DB_UNLOCK(self);
        JZ_TRY(e);
    }
    return finish_published_write(self, row, published, out);
}

static jz_err write_mergeable_at_ms_with_authorship_in_branch(const DB *self, jazz_types_AuthorSubject made_by, bool has_permission_subject,
    jazz_types_AuthorSubject permission_subject, jz_slice table, jazz_types_RowUuid row, jz_btree cells, TxIdVec parents, bool has_deletion,
    jazz_model_DeletionEvent deletion, bool has_authored_columns, jz_btree authored_columns, u64 now_ms, jazz_protocol_BranchSelector branch,
    bool has_verified_inherited_cells, jz_btree verified_inherited_cells, jazz_db_WriteHandle *out) {
    const char *operation = has_deletion && deletion == JAZZ_DELETION_DELETED ? "DELETE" : parents.len == 0 ? "INSERT" : "UPDATE";
    jz_vec_drop(&parents, sizeof(jazz_model_TxId), NULL);
    jz_err e = JZ_OK;
    if (strcmp(operation, "INSERT") == 0) e = DB_FN(apply_insert_defaults)(self, table, &cells);
    if (e.code) {
        jz_btree_drop(&cells);
        if (has_authored_columns) jz_btree_drop(&authored_columns);
        if (has_verified_inherited_cells) jz_btree_drop(&verified_inherited_cells);
        jazz_protocol__protocol__BranchSelector__drop(&branch);
        return e;
    }
    bool allow_inherited_descriptors = true;
    if (has_authored_columns) {
        const jazz_node_TableSchema *schema;
        jz_err se = DB_FN(table_schema)(self, table, &schema);
        if (!se.code) {
            bool all = true;
            for (u32 i = 0; i < schema->columns.len; ++i)
                if (!jz_btree_contains(&authored_columns, &schema->columns.ptr[i].name)) { all = false; break; }
            allow_inherited_descriptors = !all;
        } else {
            jz_err_drop(&se);
        }
    }
    jazz_node_MergeableCommit *commit = jazz_node__node__MergeableCommit__new(table, row, now_ms);
    jazz_node__node__MergeableCommit__branch(commit, branch);
    jazz_node__node__MergeableCommit__made_by(commit, made_by);
    jazz_node__node__MergeableCommit__cells(commit, cells);
    if (has_authored_columns) jazz_node__node__MergeableCommit__authored_columns(commit, authored_columns);
    if (has_verified_inherited_cells) {
        jazz_node__node__MergeableCommit__verified_inherited_large_cells(commit, &verified_inherited_cells);
        jz_btree_drop(&verified_inherited_cells);
    }
    if (has_permission_subject) jazz_node__node__MergeableCommit__permission_subject(commit, permission_subject);
    if (has_deletion) jazz_node__node__MergeableCommit__deletion(commit, deletion);
    /* Db is an untrusted client: structurally valid writes are staged and
     * sent optimistically. A serving authority assigns the policy fate. */
    jazz_node_PublishedTransaction *published;
    {
        jazz_node_NodeState *node;
        if (jazz_node__node__MergeableCommit__needs_large_value_staging(commit)) {
            e = NODE_FN(lock_for_large_value_staging)(self->node, &node);
            if (e.code) { jazz_node__node__MergeableCommit__drop(commit); return e; }
        } else {
            node = DB_LOCK(self);
        }
        jz_err ne = NS_FN(seal_inherited_large_values)(node, commit, self->schema_version_id, allow_inherited_descriptors);
        if (ne.code) { DB_UNLOCK(self); jazz_node__node__MergeableCommit__drop(commit); return jazz_node__node__api_error__Error__From__from(ne); }
        e = commit_mergeable_batch(self, node, &commit, 1, &published);
        DB_UNLOCK(self);
        JZ_TRY(e);
    }
    return finish_published_write(self, row, published, out);
}

/* Commit `commits` as one local transaction, into this handle's reserved
 * transaction slot when it has one. */
static jz_err commit_mergeable_batch(const DB *self, jazz_node_NodeState *node, jazz_node_MergeableCommit **commits, u32 n, jazz_node_PublishedTransaction **out) {
    if (self->has_reserved_tx_id) {
        JZ_TRY_NODE(NS_FN(commit_mergeable_many_in_schema_at)(node, self->schema_version_id, commits, n, self->reserved_tx_id, out));
    } else {
        JZ_TRY_NODE(NS_FN(commit_mergeable_many_in_schema)(node, self->schema_version_id, commits, n, out));
    }
    return JZ_OK;
}

static jz_err finish_published_write(const DB *self, jazz_types_RowUuid row, jazz_node_PublishedTransaction *published, jazz_db_WriteHandle *out) {
    jazz_model_TxId tx_id = jazz_node__node__PublishedTransaction__tx_id(published);
    jazz_model_DurabilityTier local_tier;
    jazz_protocol_SyncMessage none;
    memset(&none, 0, sizeof none);
    JZ_TRY(DB_FN(settle_local_publication)(self, published, false, none, &local_tier));
    out->node = self->node->node;
    out->row_uuid = row;
    out->tx_id = tx_id;
    out->local_tier = (u8)local_tier;
    out->queued_status = NULL;
    out->queued_alias = NULL;
    return JZ_OK;
}

/* Hand a published transaction to deferred local persistence, or settle its
 * publication now and queue its upload. */
jz_err DB_FN(settle_local_publication)(const DB *self, jazz_node_PublishedTransaction *published, bool has_unit, jazz_protocol_SyncMessage unit,
                                       jazz_model_DurabilityTier *out) {
    if (self->node->defer_local_persistence) {
        JZ_TRY(DB_FN(finish_deferred_local_publication)(self, published, has_unit, unit));
        *out = JAZZ_DUR_NONE;
        return JZ_OK;
    }
    jazz_model_TxId tx_id = jazz_node__node__PublishedTransaction__tx_id(published);
    jazz_node_PublicationOutcome outcome;
    jazz_node__node__PublicationOutcome__published(&outcome, NULL, 0, published);
    jz_err e = DB_FN(finish_publication_outcome)(self, outcome);
    if (e.code) { if (has_unit) jazz_protocol__protocol__SyncMessage__drop(&unit); return e; }
    if (!has_unit) return DB_FN(finalize_local_commit)(self, tx_id, out);
    NODE_FN(queue_pending_upload)(self->node, tx_id, true, unit);
    *out = JAZZ_DUR_NONE;
    return JZ_OK;
}

static void hset_insert_tables(jz_hmap *set, const JZ_VEC(jz_str) *tables) {
    for (u32 i = 0; i < tables->len; ++i) {
        jz_str t = jz_str_clone(&tables->ptr[i]);
        if (jz_hmap_insert(set, &t, NULL, NULL)) {}
    }
}

/* Transfer a published ordered slot into node ownership before resident
 * subscription refresh can suspend, fail, or be cancelled. */
jz_err DB_FN(finish_deferred_local_publication)(const DB *self, jazz_node_PublishedTransaction *published, bool has_unit, jazz_protocol_SyncMessage upload_unit) {
    jz_hmap changed_tables;
    jz_hmap_init(&changed_tables, &jazz_db_str_hset_desc);
    hset_insert_tables(&changed_tables, (const void *)jazz_node__node__PublishedTransaction__changed_tables(published));
    NODE_FN(queue_local_publication)(self->node, published, has_unit, upload_unit);
    jz_err e = jazz_db__db__lifecycle__Db__refresh_subscriptions_for_tables(self, &changed_tables);
    if (e.code) {
        jazz_db__db__peer_connection__route_subscription_refresh_failure(self->node->subscriptions, &e);
        jz_err_drop(&e);
    }
    jz_hmap_drop(&changed_tables);
    return JZ_OK;
}

jz_err DB_FN(finish_publication_outcome)(const DB *self, jazz_node_PublicationOutcome outcome) {
    for (;;) {
        if (outcome.publications.len != 0) {
            jz_hmap changed_tables;
            jz_hmap_init(&changed_tables, &jazz_db_str_hset_desc);
            for (u32 i = 0; i < outcome.publications.len; ++i)
                hset_insert_tables(&changed_tables, (const void *)jazz_node__node__PublishedTransaction__changed_tables(outcome.publications.ptr[i]));
            typedef struct { jazz_model_TxId tx_id; jz_err persistence; } Persisted;
            JZ_VEC(Persisted) persisted = {0};
            for (u32 i = 0; i < outcome.publications.len; ++i) {
                Persisted p = {jazz_node__node__PublishedTransaction__tx_id(outcome.publications.ptr[i]),
                               jazz_node__node__PublishedTransaction__persist(outcome.publications.ptr[i])};
                jz_vec_push(&persisted, &p, sizeof p);
            }
            jazz_node_NodeState *node = DB_LOCK(self);
            jz_err e = JZ_OK;
            for (u32 i = 0; i < persisted.len && !e.code; ++i)
                e = NS_FN(settle_published_transaction)(node, persisted.ptr[i].tx_id, persisted.ptr[i].persistence);
            DB_UNLOCK(self);
            jz_vec_drop(&persisted, sizeof(Persisted), NULL);
            if (e.code) {
                jz_hmap_drop(&changed_tables);
                jazz_node__node__PublicationOutcome__drop(&outcome);
                return jazz_node__node__api_error__Error__From__from(e);
            }
            e = jazz_db__db__lifecycle__Db__refresh_subscriptions_for_tables(self, &changed_tables);
            if (e.code) {
                jazz_db__db__peer_connection__route_subscription_refresh_failure(self->node->subscriptions, &e);
                jz_err_drop(&e);
            }
            jz_hmap_drop(&changed_tables);
        }
        jazz_protocol_SyncMessage message;
        if (!jz_deque_pop_front(&outcome.post_settlement_work, &message)) break;
        jz_vec_drop(&outcome.publications, sizeof(jazz_node_PublishedTransaction *), jazz_node__node__PublishedTransaction__drop);
        jazz_node_CommitUnitIngestContext ctx = {jazz_types__ids__AuthorSubject__SYSTEM, JAZZ_TRUST_TRUSTED_BACKEND, false, false};
        jazz_node_PublicationOutcome next;
        jazz_node_NodeState *node = DB_LOCK(self);
        jz_err e = NS_FN(apply_sync_message_with_ingest_context)(node, message, &ctx, &next);
        DB_UNLOCK(self);
        if (e.code) { jazz_node__node__PublicationOutcome__drop(&outcome); return jazz_node__node__api_error__Error__From__from(e); }
        outcome.publications = next.publications;
        next.publications = (__typeof__(next.publications)){0};
        jazz_protocol_SyncMessage more;
        while (jz_deque_pop_front(&next.post_settlement_work, &more)) jz_deque_push_back(&outcome.post_settlement_work, &more);
        jazz_node__node__PublicationOutcome__drop(&next);
    }
    jazz_node__node__PublicationOutcome__drop(&outcome);
    return JZ_OK;
}

jz_err DB_FN(resolve_write_identity)(const DB *self, jazz_db_WriteIdentity identity, jazz_db_ResolvedWriteIdentity *out) {
    switch (identity.tag) {
    case JAZZ_WID_DATABASE:
        out->made_by = self->identity.author;
        out->has_permission_subject = false;
        return JZ_OK;
    case JAZZ_WID_SESSION:
        out->made_by = identity.author;
        out->has_permission_subject = true;
        out->permission_subject = identity.author;
        return JZ_OK;
    default:
        if (jazz_types_AuthorSubject_eq(identity.author, self->identity.author) || self->backend_attribution) {
            out->made_by = identity.author;
            out->has_permission_subject = true;
            out->permission_subject = self->identity.author;
            return JZ_OK;
        }
        return jz_err_new(JAZZ_EC_WRITE_REJECTED, "attribution requires a trusted serving node");
    }
}

static jz_err reject_attributed_branch_target(const DB *self, jazz_db_WriteIdentity identity, bool targets_branch) {
    (void)self;
    if (identity.tag == JAZZ_WID_ATTRIBUTION && targets_branch)
        return jz_err_new(JAZZ_EC_WRITE_REJECTED, "backend-attributed writes do not support branch targets");
    return JZ_OK;
}

jz_err DB_FN(check_catalogue_admin)(const DB *self) {
    if (jazz_types_AuthorSubject_eq(self->identity.author, jazz_types__ids__AuthorSubject__SYSTEM)) return JZ_OK;
    return jz_err_new(JAZZ_EC_PROTOCOL, "catalogue updates require a serving Node");
}

/* Client writes stay pending at this runtime's authored durability until peer
 * durability or fate updates arrive over a connection. */
jz_err DB_FN(finalize_local_commit)(const DB *self, jazz_model_TxId tx_id, jazz_model_DurabilityTier *out) {
    jazz_protocol_SyncMessage none;
    memset(&none, 0, sizeof none);
    NODE_FN(queue_pending_upload)(self->node, tx_id, false, none);
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(self->node->node);
    *out = NS_FN(authored_commit_durability)(node);
    DB_UNLOCK(self);
    return JZ_OK;
}

u64 DB_FN(next_now_ms)(const DB *self) { return jazz_db__db__WriteClock__next_now_ms(self->next_now_ms); }

jz_err DB_FN(ensure_open_schema_admitted)(const DB *self) {
    if (!self->requires_open_schema_admission) return JZ_OK;
    const jazz_db_OpenSchemaAdmission *admission = self->node->open_schema_admission;
    if (admission->has && admission->pending.has_result) return jazz_db_err_clone(&admission->pending.result);
    return jazz_db__db__pending_open_schema_error();
}

jz_err DB_FN(await_open_schema_for_coverage)(const DB *self, jazz_model_DurabilityTier coverage_tier) {
    if (!self->requires_open_schema_admission) return JZ_OK;
    if (coverage_tier < JAZZ_DUR_GLOBAL) return DB_FN(ensure_open_schema_admitted)(self);
    jazz_db_SharedResultFuture *wait;
    {
        const jazz_db_OpenSchemaAdmission *state = self->node->open_schema_admission;
        if (!state->has) jz_panic("pending owner has admission state");
        if (state->pending.has_result) return jazz_db_err_clone(&state->pending.result);
        wait = jazz_db__db__SharedResultFuture__clone(state->pending.wait);
    }
    JZ_TRY(jazz_db__db__SharedResultFuture__await(wait));
    return DB_FN(ensure_open_schema_admitted)(self);
}

jz_err DB_FN(current_write_schema_for_query)(const DB *self, jazz_node_JazzSchema **schema_out, jazz_types_SchemaVersionId *out) {
    JZ_TRY(DB_FN(ensure_open_schema_admitted)(self));
    if (self->schema_view_is_fixed) {
        *schema_out = jazz_node__schema__JazzSchema__clone(self->schema);
        *out = self->schema_version_id;
        return JZ_OK;
    }
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(self->node->node);
    jazz_node_CurrentWriteSchema current;
    TRY_NODE_UNLOCK(self, NS_FN(current_write_schema)(node, &current));
    if (memcmp(&current.schema, &self->schema_version_id, sizeof current.schema) == 0) {
        DB_UNLOCK(self);
        *schema_out = jazz_node__schema__JazzSchema__clone(self->schema);
        *out = self->schema_version_id;
        return JZ_OK;
    }
    const jazz_node_JazzSchema *schema = NS_FN(catalogue_schema)(node, current.schema);
    DB_UNLOCK(self);
    if (!schema) {
        jz_str dbg = {0};
        jazz_types__ids__SchemaVersionId__debug(&dbg, &current.schema);
        jz_err e = jz_err_fmt(JAZZ_EC_SCHEMA, "current write schema %.*s is missing from catalogue", (int)dbg.len, dbg.ptr);
        jz_str_drop(&dbg);
        return e;
    }
    *schema_out = jazz_node__schema__JazzSchema__clone(schema);
    *out = current.schema;
    return JZ_OK;
}

jz_err DB_FN(table_schema)(const DB *self, jz_slice table, const jazz_node_TableSchema **out) {
    for (u32 i = 0; i < self->schema->tables.len; ++i) {
        const jz_str *name = &self->schema->tables.ptr[i].name;
        if (name->len == table.len && memcmp(name->ptr, table.ptr, table.len) == 0) { *out = &self->schema->tables.ptr[i]; return JZ_OK; }
    }
    return jz_err_fmt(JAZZ_EC_SCHEMA, "unknown table %.*s", (int)table.len, (const char *)table.ptr);
}

/* Typed binding insertions must supply every required column without a schema default. */
jz_err DB_FN(validate_complete_insert)(const DB *self, jz_slice table, const jz_btree *cells) {
    const jazz_node_TableSchema *schema;
    JZ_TRY(DB_FN(table_schema)(self, table, &schema));
    for (u32 i = 0; i < schema->columns.len; ++i) {
        const jazz_node_ColumnSchema *column = &schema->columns.ptr[i];
        bool in_branch_by = false;
        for (u32 j = 0; j < schema->branch_by.len; ++j)
            if (jz_str_eq(&schema->branch_by.ptr[j], &column->name)) in_branch_by = true;
        if (!in_branch_by && !jz_btree_contains(cells, &column->name) && !column->default_value && column->column_type.tag != GROOVE_COLTYPE_NULLABLE) {
            return jz_err_fmt(JAZZ_EC_WRITE_REJECTED, "missing required field `%.*s` on table `%.*s`", (int)column->name.len, column->name.ptr,
                              (int)table.len, (const char *)table.ptr);
        }
    }
    return JZ_OK;
}

jz_err DB_FN(apply_insert_defaults)(const DB *self, jz_slice table, jz_btree *cells) {
    const jazz_node_TableSchema *table_schema;
    JZ_TRY(DB_FN(table_schema)(self, table, &table_schema));
    for (u32 i = 0; i < table_schema->columns.len; ++i) {
        const jazz_node_ColumnSchema *column = &table_schema->columns.ptr[i];
        if (!jz_btree_contains(cells, &column->name) && column->default_value) {
            jz_str key = jz_str_clone(&column->name);
            groove_Value v = jazz_db__db__default_cell_for_column_type(&column->column_type, column->default_value);
            jz_btree_insert(cells, &key, &v, NULL);
        }
    }
    if (cells->len == 0) {
        /* A content version must carry at least one cell: author the null of
         * every nullable column explicitly. */
        for (u32 i = 0; i < table_schema->columns.len; ++i) {
            const jazz_node_ColumnSchema *column = &table_schema->columns.ptr[i];
            groove_ColumnType storage = jazz_node__schema__storage_column_type(column);
            if (storage.tag == GROOVE_COLTYPE_NULLABLE) {
                jz_str key = jz_str_clone(&column->name);
                groove_Value v;
                memset(&v, 0, sizeof v);
                v.tag = GROOVE_V_NULLABLE;
                v.v.nullable = NULL;
                jz_btree_insert(cells, &key, &v, NULL);
            }
        }
        /* Nullable JSON's published storage type cannot carry SQL null yet (#2733, #3007). */
        if (cells->len == 0 && table_schema->columns.len != 0) {
            bool all_json = true;
            for (u32 i = 0; i < table_schema->columns.len; ++i) {
                const jazz_node_ColumnSchema *column = &table_schema->columns.ptr[i];
                if (!(column->large_value_kind == JAZZ_LV_SEM_JSON && column->column_type.tag == GROOVE_COLTYPE_NULLABLE)) { all_json = false; break; }
            }
            if (all_json) {
                return jz_err_fmt(JAZZ_EC_SCHEMA,
                                  "inserting a row with every column omitted is not supported yet for table `%.*s`: its optional columns are all JSON, "
                                  "which cannot store null until #3007 lands; set at least one column",
                                  (int)table.len, (const char *)table.ptr);
            }
        }
    }
    return JZ_OK;
}

/* ---------------- row targets and preimages ---------------- */

static jz_err upsert_target_for_client_identity(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_types_AuthorSubject identity, bool *exists) {
    bool has;
    jazz_node_CurrentRow target;
    JZ_TRY(local_row_for_client_identity(self, table, row, identity, &has, &target));
    if (has) { jazz_node__node__CurrentRow__drop(&target); *exists = true; return JZ_OK; }
    /* A policy-filtered point read cannot by itself distinguish an absent row
     * from an existing row hidden from this identity. */
    bool has_current;
    jazz_node_CurrentRow current;
    JZ_TRY(DB_FN(local_current_row)(self, table, row, &has_current, &current));
    if (!has_current) { *exists = false; return JZ_OK; }
    jazz_node__node__CurrentRow__drop(&current);
    const jazz_node_TableSchema *schema;
    JZ_TRY(DB_FN(table_schema)(self, table, &schema));
    if (jazz_types_AuthorSubject_eq(identity, jazz_types__ids__AuthorSubject__SYSTEM) || !schema->read_policy) { *exists = false; return JZ_OK; }
    return jazz_db__db__read_for_write_denied("UPSERT", table);
}

static jz_err upsert_target_for_trusted_identity(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_types_AuthorSubject identity, bool *exists) {
    bool has;
    jazz_node_CurrentRow target;
    JZ_TRY(DB_FN(local_row_for_trusted_identity)(self, table, row, identity, &has, &target));
    if (has) { jazz_node__node__CurrentRow__drop(&target); *exists = true; return JZ_OK; }
    /* Trusted serving evaluates the identity's real read policy before
     * merging an existing row. A hidden existing row must not be treated as
     * an insert target. */
    bool has_current;
    jazz_node_CurrentRow current;
    JZ_TRY(DB_FN(local_current_row)(self, table, row, &has_current, &current));
    if (!has_current) { *exists = false; return JZ_OK; }
    jazz_node__node__CurrentRow__drop(&current);
    if (jazz_types_AuthorSubject_eq(identity, jazz_types__ids__AuthorSubject__SYSTEM)) { *exists = false; return JZ_OK; }
    return jazz_db__db__read_for_write_denied("UPSERT", table);
}

/* Read one locally-current row by primary key without evaluating a table query. */
jz_err DB_FN(local_current_row)(const DB *self, jz_slice table, jazz_types_RowUuid row, bool *has, jazz_node_CurrentRow *out) {
    JZ_TRY(DB_FN(ensure_open_schema_admitted)(self));
    const jazz_node_TableSchema *schema;
    JZ_TRY(DB_FN(table_schema)(self, table, &schema));
    jazz_node_NodeState *node = DB_LOCK(self);
    TRY_NODE_UNLOCK(self, NS_FN(local_current_row)(node, table, row, has, out));
    DB_UNLOCK(self);
    return JZ_OK;
}

static jz_err object_exists_error(const char *what, jazz_types_RowUuid row) {
    char id[37];
    jz_uuid_to_string(&row, id);
    return jz_err_fmt(JAZZ_EC_WRITE_REJECTED, "encoding error: %s: %s", what, id);
}

static jz_err ensure_row_absent(const DB *self, jz_slice table, jazz_types_RowUuid row) {
    const jazz_node_TableSchema *schema;
    JZ_TRY(DB_FN(table_schema)(self, table, &schema));
    bool has_content, has_deletion;
    jazz_model_TxId content, deletion;
    {
        jazz_node_NodeState *node = DB_LOCK(self);
        TRY_NODE_UNLOCK(self, NS_FN(local_content_winner_tx_id_in_schema)(node, self->schema_version_id, table, row, &has_content, &content));
        TRY_NODE_UNLOCK(self, NS_FN(local_deletion_winner_tx_id_in_schema)(node, self->schema_version_id, table, row, &has_deletion, &deletion));
        DB_UNLOCK(self);
    }
    if (has_deletion) return jazz_db__db__row_already_deleted(row);
    if (has_content) return object_exists_error("object already exists", row);
    return JZ_OK;
}

static jz_err ensure_exact_branch_row_absent(const DB *self, jz_slice table, const jazz_protocol_BranchSelector *branch, jazz_types_RowUuid row) {
    const jazz_node_TableSchema *schema;
    JZ_TRY(DB_FN(table_schema)(self, table, &schema));
    jazz_node_NodeState *node = DB_LOCK(self);
    bool has_content, has_deletion;
    jazz_model_TxId content, deletion;
    TRY_NODE_UNLOCK(self, NS_FN(local_content_winner_tx_id_in_branch)(node, table, branch, row, &has_content, &content));
    TRY_NODE_UNLOCK(self, NS_FN(local_deletion_winner_tx_id_in_branch)(node, table, branch, row, &has_deletion, &deletion));
    DB_UNLOCK(self);
    if (has_deletion) return jazz_db__db__row_already_deleted(row);
    if (has_content) return object_exists_error("branch-local row already exists", row);
    return JZ_OK;
}

static jz_err ensure_row_deleted(const DB *self, jz_slice table, jazz_types_RowUuid row) {
    const jazz_node_TableSchema *schema;
    JZ_TRY(DB_FN(table_schema)(self, table, &schema));
    bool deleted;
    jazz_model_TxId winner;
    jazz_node_NodeState *node = DB_LOCK(self);
    TRY_NODE_UNLOCK(self, NS_FN(local_deletion_winner_tx_id_in_schema)(node, self->schema_version_id, table, row, &deleted, &winner));
    DB_UNLOCK(self);
    if (deleted) return JZ_OK;
    char id[37];
    jz_uuid_to_string(&row, id);
    return jz_err_fmt(JAZZ_EC_WRITE_REJECTED, "row not deleted: %s", id);
}

jz_err DB_FN(ensure_row_not_deleted)(const DB *self, jz_slice table, jazz_types_RowUuid row) {
    const jazz_node_TableSchema *schema;
    JZ_TRY(DB_FN(table_schema)(self, table, &schema));
    jazz_node_NodeState *node = DB_LOCK(self);
    /* A retained deletion register may have been superseded by a restore. */
    bool has_deletion;
    jazz_model_TxId winner;
    TRY_NODE_UNLOCK(self, NS_FN(local_deletion_winner_tx_id_in_schema)(node, self->schema_version_id, table, row, &has_deletion, &winner));
    bool deleted = false;
    if (has_deletion) {
        bool has_current;
        jazz_node_CurrentRow current;
        TRY_NODE_UNLOCK(self, NS_FN(local_current_row_in_schema)(node, table, row, self->schema_version_id, &has_current, &current));
        if (has_current) jazz_node__node__CurrentRow__drop(&current);
        deleted = !has_current;
    }
    DB_UNLOCK(self);
    if (deleted) return jazz_db__db__row_already_deleted(row);
    return JZ_OK;
}

/* Decide, without applying anything, the admission failures of a queued
 * mutation that NAPI and WASM also report synchronously. */
jz_err DB_FN(precheck_mutation_admission)(const DB *self, jz_slice table) {
    JZ_TRY(jazz_db__db__lifecycle__Db__ensure_mutation_operation_admitted(self));
    const jazz_node_TableSchema *schema;
    JZ_TRY(DB_FN(table_schema)(self, table, &schema));
    return JZ_OK;
}

jz_err DB_FN(ensure_branch_view_row_not_deleted)(const DB *self, jz_slice table, const jazz_protocol_BranchSelector *head, const jazz_protocol_BranchViewBase *base,
                                                 jazz_types_RowUuid row) {
    const jazz_node_TableSchema *schema;
    JZ_TRY(DB_FN(table_schema)(self, table, &schema));
    jazz_node_NodeState *node = DB_LOCK(self);
    bool has_deletion_winner;
    jazz_model_TxId winner;
    TRY_NODE_UNLOCK(self, NS_FN(local_deletion_winner_tx_id_in_branch)(node, table, head, row, &has_deletion_winner, &winner));
    bool tombstone_hidden = false;
    if (has_deletion_winner) {
        bool has_view;
        jz_btree view;
        TRY_NODE_UNLOCK(self, NS_FN(visible_current_cells_in_branch_view)(node, table, head, base, row, &has_view, &view));
        if (has_view) jz_btree_drop(&view);
        tombstone_hidden = !has_view;
    }
    DB_UNLOCK(self);
    if (tombstone_hidden) return jazz_db__db__row_already_deleted(row);
    return JZ_OK;
}

static jz_err row_layer_parents(const DB *self, jz_slice table, jazz_types_RowUuid row, void *content, void *deletion) {
    jazz_node_NodeState *node = DB_LOCK(self);
    bool has;
    jazz_model_TxId tx;
    TRY_NODE_UNLOCK(self, NS_FN(local_content_winner_tx_id_in_schema)(node, self->schema_version_id, table, row, &has, &tx));
    if (has) jz_vec_push(content, &tx, sizeof tx);
    TRY_NODE_UNLOCK(self, NS_FN(local_deletion_winner_tx_id_in_schema)(node, self->schema_version_id, table, row, &has, &tx));
    if (has) jz_vec_push(deletion, &tx, sizeof tx);
    DB_UNLOCK(self);
    return JZ_OK;
}

/* Why an UPDATE found no preimage (#3661). */
static jz_err update_target_missing(const DB *self, jz_slice table, jazz_types_RowUuid row, jz_err *out) {
    bool has;
    jazz_node_CurrentRow current;
    JZ_TRY(DB_FN(local_current_row)(self, table, row, &has, &current));
    if (!has) {
        *out = jazz_db__db__update_target_not_loaded("UPDATE", table, row);
    } else {
        jazz_node__node__CurrentRow__drop(&current);
        *out = jazz_db__db__read_for_write_denied("UPDATE", table);
    }
    return JZ_OK;
}

static jz_err prepare_table_query(const DB *self, jz_slice table, jazz_db_PreparedQuery *out) {
    jazz_model_Query *q = jazz_model__query__Query__from_table(table);
    jz_err e = jazz_db__db__reads__Db__prepare_query(self, q, out);
    jazz_model__query__Query__drop(q);
    return e;
}

static jz_err first_row(jazz_node_CurrentRowVec *rows, bool *has, jazz_node_CurrentRow *out) {
    *has = rows->len > 0;
    if (*has) {
        jz_vec_remove(rows, 0, out, sizeof(jazz_node_CurrentRow));
    }
    jz_vec_drop(rows, sizeof(jazz_node_CurrentRow), jazz_node__node__CurrentRow__drop);
    return JZ_OK;
}

static jz_err local_row_for_client_identity(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_types_AuthorSubject identity, bool *has, jazz_node_CurrentRow *out) {
    /* A policy-free table cannot hide the preimage from this client. */
    const jazz_node_TableSchema *schema;
    JZ_TRY(DB_FN(table_schema)(self, table, &schema));
    if (!schema->read_policy) return DB_FN(local_current_row)(self, table, row, has, out);
    jazz_db_PreparedQuery query;
    JZ_TRY(prepare_table_query(self, table, &query));
    jazz_node_CurrentRowVec rows = {0};
    jazz_node_NodeState *node = DB_LOCK(self);
    jz_err e = NS_FN(query_rows_for_client_physical_row)(node, query.shape, query.binding, JAZZ_DUR_LOCAL, identity, row, &rows);
    DB_UNLOCK(self);
    jazz_db__db__PreparedQuery__drop(&query);
    if (e.code) return jazz_node__node__api_error__Error__From__from(e);
    return first_row(&rows, has, out);
}

jz_err DB_FN(local_row_for_trusted_identity)(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_types_AuthorSubject identity, bool *has, jazz_node_CurrentRow *out) {
    jazz_db_PreparedQuery query;
    JZ_TRY(prepare_table_query(self, table, &query));
    jazz_node_CurrentRowVec rows = {0};
    jazz_node_NodeState *node = DB_LOCK(self);
    jz_err e = NS_FN(query_rows_for_link_physical_row)(node, query.shape, query.binding, JAZZ_DUR_LOCAL, identity, row, &rows);
    DB_UNLOCK(self);
    jazz_db__db__PreparedQuery__drop(&query);
    if (e.code) return jazz_node__node__api_error__Error__From__from(e);
    return first_row(&rows, has, out);
}

jz_err DB_FN(visible_branch_view_cells_for_identity)(const DB *self, jz_slice table, const jazz_protocol_BranchSelector *head, const jazz_protocol_BranchViewBase *base,
                                                     jazz_types_RowUuid row, jazz_types_AuthorSubject identity, bool *has, jz_btree *out) {
    const jazz_node_TableSchema *schema_ref;
    JZ_TRY(DB_FN(table_schema)(self, table, &schema_ref));
    jazz_node_TableSchema table_schema;
    jazz_node__schema__TableSchema__clone(&table_schema, schema_ref);
    jazz_db_PreparedQuery query;
    jz_err e = prepare_table_query(self, table, &query);
    if (e.code) { jazz_node__schema__TableSchema__drop(&table_schema); return e; }
    jazz_db_ReadOpts *opts = jazz_db__db__ReadOpts__local_only();
    jazz_protocol_BranchSelector h;
    jazz_protocol__protocol__BranchSelector__clone(&h, head);
    jazz_db__db__ReadOpts__branch_view(opts, h, base ? jazz_protocol__protocol__BranchViewBase__clone(base) : NULL);
    jazz_node_CurrentRowVec rows = {0};
    e = jazz_db__db__reads__Db__all_for_identity(self, &query, opts, identity, &rows);
    jazz_db__db__PreparedQuery__drop(&query);
    if (e.code) { jazz_node__schema__TableSchema__drop(&table_schema); return e; }
    *has = false;
    for (u32 i = 0; i < rows.len; ++i) {
        jazz_types_RowUuid id = jazz_node__node__CurrentRow__row_uuid(&rows.ptr[i]);
        if (!jz_uuid_eq(&id, &row)) continue;
        *has = true;
        cells_init(out);
        for (u32 c = 0; c < table_schema.columns.len; ++c) {
            const jz_str *name = &table_schema.columns.ptr[c].name;
            groove_Value value;
            if (jazz_node__node__CurrentRow__cell(&rows.ptr[i], &table_schema, jz_str_slice(name), &value)) {
                jz_str key = jz_str_clone(name);
                jz_btree_insert(out, &key, &value, NULL);
            }
        }
        break;
    }
    jz_vec_drop(&rows, sizeof(jazz_node_CurrentRow), jazz_node__node__CurrentRow__drop);
    jazz_node__schema__TableSchema__drop(&table_schema);
    return JZ_OK;
}

/* Handle for an empty root update: the write is a no-op that reports the
 * transaction of the row's current version. */
static jz_err no_op_update_handle(const DB *self, jz_slice table, jazz_types_RowUuid row, bool has_existing, jazz_node_CurrentRow *existing, jazz_db_WriteHandle *out) {
    if (!has_existing) {
        jz_err missing;
        JZ_TRY(update_target_missing(self, table, row, &missing));
        return missing;
    }
    jazz_model_TxId tx_id;
    jazz_node_NodeState *node = DB_LOCK(self);
    bool found = NS_FN(current_row_tx_id)(node, existing, &tx_id);
    DB_UNLOCK(self);
    jazz_node__node__CurrentRow__drop(existing);
    if (!found) return jz_err_new(JAZZ_EC_NOT_OBSERVED, "current row has no transaction");
    jazz_db_WriteState state;
    JZ_TRY(jazz_db__db__lifecycle__Db__write_state(self, tx_id, &state));
    out->node = self->node->node;
    out->row_uuid = row;
    out->tx_id = tx_id;
    out->local_tier = state.durability;
    out->queued_status = NULL;
    out->queued_alias = NULL;
    return JZ_OK;
}

static jz_err merge_existing_cells(const DB *self, jz_slice table, jazz_types_RowUuid row, jz_btree patch, MergedCells *out) {
    return merge_existing_cells_for_client_identity(self, table, row, patch, self->identity.author, out);
}

static jz_err merge_existing_cells_for_client_identity(const DB *self, jz_slice table, jazz_types_RowUuid row, jz_btree patch, jazz_types_AuthorSubject identity,
                                                       MergedCells *out) {
    const jazz_node_TableSchema *table_schema;
    jz_err e = DB_FN(table_schema)(self, table, &table_schema);
    if (!e.code) e = DB_FN(ensure_row_not_deleted)(self, table, row);
    if (e.code) { jz_btree_drop(&patch); return e; }
    bool is_partial = false;
    for (u32 i = 0; i < table_schema->columns.len; ++i)
        if (!jz_btree_contains(&patch, &table_schema->columns.ptr[i].name)) { is_partial = true; break; }
    if (is_partial && (jazz_types_AuthorSubject_eq(identity, jazz_types__ids__AuthorSubject__SYSTEM) || !table_schema->read_policy)) {
        /* SYSTEM and policy-free tables are unconditionally visible; preserve
         * indirect descriptors by reading the physical winner. */
        bool has;
        jazz_node_NodeState *node = DB_LOCK(self);
        jz_err ne = NS_FN(current_physical_cells_and_winner_in_schema)(node, self->schema_version_id, table, row, &has, &out->cells, &out->parent);
        DB_UNLOCK(self);
        if (ne.code) { jz_btree_drop(&patch); return jazz_node__node__api_error__Error__From__from(ne); }
        if (!has) { jz_btree_drop(&patch); return jazz_db__db__update_target_not_loaded("partial UPDATE", table, row); }
        out->has_parent = true;
        out->authored_columns = cell_keys(&patch);
        cells_extend(&out->cells, &patch);
        return JZ_OK;
    }
    bool has_existing;
    jazz_node_CurrentRow existing;
    e = local_row_for_client_identity(self, table, row, identity, &has_existing, &existing);
    if (e.code) { jz_btree_drop(&patch); return e; }
    if (!has_existing) {
        jz_btree_drop(&patch);
        jz_err missing;
        JZ_TRY(update_target_missing(self, table, row, &missing));
        return missing;
    }
    jazz_node_NodeState *node = DB_LOCK(self);
    bool has_cells;
    jz_err ne = NS_FN(current_physical_cells_in_schema)(node, self->schema_version_id, table, row, &has_cells, &out->cells);
    if (ne.code) { DB_UNLOCK(self); jazz_node__node__CurrentRow__drop(&existing); jz_btree_drop(&patch); return jazz_node__node__api_error__Error__From__from(ne); }
    if (!has_cells) { DB_UNLOCK(self); jazz_node__node__CurrentRow__drop(&existing); jz_btree_drop(&patch); return jazz_db__db__read_for_write_denied("UPDATE", table); }
    out->has_parent = NS_FN(current_row_tx_id)(node, &existing, &out->parent);
    DB_UNLOCK(self);
    jazz_node__node__CurrentRow__drop(&existing);
    out->authored_columns = cell_keys(&patch);
    cells_extend(&out->cells, &patch);
    return JZ_OK;
}

static jz_err merge_existing_cells_for_identity(const DB *self, jz_slice table, jazz_types_RowUuid row, jz_btree patch, jazz_types_AuthorSubject identity, MergedCells *out) {
    jz_err e = DB_FN(ensure_row_not_deleted)(self, table, row);
    jazz_protocol_PermissionAdvice advice;
    if (!e.code) e = DB_FN(authorize_read_for_identity)(self, table, row, identity, &advice);
    if (e.code) { jz_btree_drop(&patch); return e; }
    if (advice != JAZZ_ADVICE_ALLOWED) {
        jz_btree_drop(&patch);
        jz_err missing;
        JZ_TRY(update_target_missing(self, table, row, &missing));
        return missing;
    }
    jz_btree authored_columns = cell_keys(&patch);
    /* A complete replacement still requires read permission, proved by the
     * point dry-run above, but it does not consume any preimage cells. */
    const jazz_node_TableSchema *schema;
    e = DB_FN(table_schema)(self, table, &schema);
    if (e.code) { jz_btree_drop(&patch); jz_btree_drop(&authored_columns); return e; }
    bool complete = true;
    for (u32 i = 0; i < schema->columns.len; ++i)
        if (!jz_btree_contains(&patch, &schema->columns.ptr[i].name)) { complete = false; break; }
    if (complete) {
        bool has;
        jazz_node_CurrentRow existing;
        e = DB_FN(local_current_row)(self, table, row, &has, &existing);
        if (e.code) { jz_btree_drop(&patch); jz_btree_drop(&authored_columns); return e; }
        out->has_parent = false;
        if (has) {
            jazz_node_NodeState *node = DB_LOCK(self);
            out->has_parent = NS_FN(current_row_tx_id)(node, &existing, &out->parent);
            DB_UNLOCK(self);
            jazz_node__node__CurrentRow__drop(&existing);
        }
        out->cells = patch;
        out->authored_columns = authored_columns;
        return JZ_OK;
    }
    bool has;
    jazz_node_NodeState *node = DB_LOCK(self);
    jz_err ne = NS_FN(current_physical_cells_and_winner_in_schema)(node, self->schema_version_id, table, row, &has, &out->cells, &out->parent);
    DB_UNLOCK(self);
    if (ne.code) { jz_btree_drop(&patch); jz_btree_drop(&authored_columns); return jazz_node__node__api_error__Error__From__from(ne); }
    if (!has) { jz_btree_drop(&patch); jz_btree_drop(&authored_columns); return jazz_db__db__update_target_not_loaded("partial UPDATE", table, row); }
    out->has_parent = true;
    cells_extend(&out->cells, &patch);
    out->authored_columns = authored_columns;
    return JZ_OK;
}
