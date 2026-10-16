/* groove::large_values (crates/groove/src/large_values.rs)
 * Canonical indirect representation for large logical scalar values.
 * async fns are synchronous here; IvmRuntimeError::{LargeValue,...} are
 * transparent, so large_values::Error codes pass through unchanged. */
#include "jz_rt.h"
#include "large_values.h"
#include "large_values/json_syntax.h"
#include "chunks.h"
#include "records/values.h"
#include "records/mod.h"
#include "_ext_groove-storage.h"

typedef groove_LargeValueRef LVRef;
typedef groove_NodeMetrics Metrics;

#define LVK_BYTES GROOVE_LVK_BYTES
#define LVK_STRING GROOVE_LVK_STRING
#define LVK_JSON GROOVE_LVK_JSON
#define ERR(c) groove__large_values__Error__new(GROOVE_LV_E_##c)
#define IS_BLOCKED(e) groove__ivm__runtime__IvmRuntimeError__is_evaluation_blocked(&(e))

/* ------------------------------------------------------------------ Error */
static const char *const LV_ERROR_MESSAGES[] = {
    "large string is not valid UTF-8",
    "large JSON is not valid JSON",
    "unsupported large-value format version %u",
    "chunk object hash does not match its authenticated reference",
    "malformed large-value node",
    "large-value metric overflow",
    "large-value descriptor metrics or logical hash are dishonest",
    "large-value tree contains a cycle or exceeds its depth bound",
    "large-value logical traversal exceeded its deterministic work limit",
    "large-value physical traversal exceeded its distinct-node limit",
    "malformed physical scalar encoding",
    "indirect scalar requires interruptible evaluation",
};

jz_err groove__large_values__Error__new(u32 code) {
    JZ_ASSERT(code > GROOVE_LV_E_BASE && code < GROOVE_LV_E_END && code != GROOVE_LV_E_UnsupportedFormat, "large_values::Error");
    return jz_err_new(code, LV_ERROR_MESSAGES[code - GROOVE_LV_E_BASE - 1]);
}

jz_err groove__large_values__Error__unsupported_format(u8 version) {
    return jz_err_fmt(GROOVE_LV_E_UnsupportedFormat, LV_ERROR_MESSAGES[GROOVE_LV_E_UnsupportedFormat - GROOVE_LV_E_BASE - 1], (unsigned)version);
}

static const char *const LV_ERROR_VARIANTS[] = {
    "InvalidUtf8", "InvalidJson", "UnsupportedFormat", "ObjectHashMismatch", "MalformedNode", "MetricOverflow",
    "DescriptorMismatch", "InvalidTree", "TraversalWorkLimitExceeded", "PhysicalTraversalNodeLimitExceeded",
    "MalformedScalar", "RequiresEvaluation",
};

/* #[derive(Debug)] for Error */
void groove__large_values__Error__Debug__fmt(jz_str *out, const jz_err *e) {
    if (!groove__large_values__is_error(e->code)) { jz_str_push_cstr(out, "?"); return; }
    jz_str_push_cstr(out, LV_ERROR_VARIANTS[e->code - GROOVE_LV_E_BASE - 1]);
    if (e->code == GROOVE_LV_E_UnsupportedFormat && e->msg) {
        /* UnsupportedFormat(n): recover n from the Display text */
        const char *m = e->msg->ptr;
        u32 i = e->msg->len;
        while (i > 0 && m[i - 1] >= '0' && m[i - 1] <= '9') i--;
        jz_str_push_char(out, '(');
        jz_str_push(out, m + i, e->msg->len - i);
        jz_str_push_char(out, ')');
    }
}

/* ReachabilityError { LargeValue(transparent) | Chunk(transparent) }: codes pass through. */

/* ------------------------------------------------------------------ constants */
#define FORMAT_V1 GROOVE_LV_FORMAT_V1
#define FORMAT_VERSION GROOVE_LV_FORMAT_VERSION
#define LEAF_MIN_BYTES GROOVE_LV_LEAF_MIN_BYTES
#define LEAF_TARGET_BYTES GROOVE_LV_LEAF_TARGET_BYTES
#define LEAF_MAX_BYTES GROOVE_LV_LEAF_MAX_BYTES
#define MAX_ENCODED_NODE_BYTES GROOVE_LV_MAX_ENCODED_NODE_BYTES
#define BRANCH_MIN_CHILDREN GROOVE_LV_BRANCH_MIN_CHILDREN
#define BRANCH_TARGET_CHILDREN GROOVE_LV_BRANCH_TARGET_CHILDREN
#define BRANCH_MAX_CHILDREN GROOVE_LV_BRANCH_MAX_CHILDREN
#define MAX_TREE_DEPTH GROOVE_LV_MAX_TREE_DEPTH
#define MAX_LOGICAL_TRAVERSAL_STEPS GROOVE_LV_MAX_LOGICAL_TRAVERSAL_STEPS
#define MAX_PHYSICAL_TRAVERSAL_NODES GROOVE_LV_MAX_PHYSICAL_TRAVERSAL_NODES
#define MAX_EDIT_COUNT GROOVE_LV_MAX_EDIT_COUNT
#define MAX_EDIT_TAIL_BYTES GROOVE_LV_MAX_EDIT_TAIL_BYTES

/* ------------------------------------------------------------------ forward decls */
typedef groove_BuiltNode BuiltNode;
typedef JZ_VEC(BuiltNode) BuiltNodeVec;
typedef struct Range { u32 start, end; } Range;
typedef JZ_VEC(Range) RangeVec;
typedef groove_Locator (*LocatorFn)(void *ctx, groove_ContentHash hash);
typedef groove_Locator (*LocateFn)(void *ctx, groove_ContentHash hash, bool *fresh);
typedef struct TreeStager { u8 kind; LocateFn locate; void *locate_ctx; groove_StagedChunkVec staged; } TreeStager;
typedef enum { RANGE_UNIT_BYTES, RANGE_UNIT_UTF16 } RangeUnit;

static jz_err validate_descriptor(const LVRef *value);
static jz_err validate_descriptor_v1(const LVRef *value);
static jz_err validate_descriptor_shape(const LVRef *value);
static jz_err validate_descriptor_shape_v1(const LVRef *value);
static jz_err decode_descriptor_payload_v1(u8 kind, const u8 *payload, u32 n, LVRef *out);
static jz_err encode_node_v1(const groove_ChunkNode *node, jz_bytes *out);
static jz_err decode_canonical_node_v1(const u8 *encoded, u32 n, groove_ChunkNode *out);
static u8 node_format(const groove_ChunkNode *node);
static u8 node_kind(const groove_ChunkNode *node);
static jz_err node_metrics(u8 kind, const groove_ChunkNode *node, Metrics *out);
static groove_ContentHash node_logical_hash(const groove_ChunkNode *node);
static jz_err metrics(u8 kind, const u8 *p, u32 n, Metrics *out);
static jz_err add_metrics(Metrics left, Metrics right, Metrics *out);
static u64 roll(u64 hash, u8 byte);
static u64 roll_child(u64 hash, u8 kind, const BuiltNode *node);
static bool leaf_cut(u64 hash, u32 length);
static bool branch_cut(u64 hash, u32 count);
static jz_err seal_node(const groove_ChunkNode *node, Metrics m, LocateFn locate, void *ctx, BuiltNode *built, bool *has_chunk, groove_StagedChunk *chunk);
static jz_err branch_over(u8 kind, const BuiltNode *group, u32 n, groove_ChunkNode *node, Metrics *m);
static groove_ChunkNode leaf_node(u8 kind, jz_bytes bytes);
static void built_into_value_ref(const BuiltNode *b, u8 kind, LVRef *out);
static jz_err load_authenticated_node_attempt(u8 format_version, u8 kind, const groove_NodeRef *node_ref, groove_ContentHash expected, groove_EvaluationInputs *inputs, groove_ChunkNode *out);
static jz_err decode_node_for_format(u8 format_version, u8 kind, groove_ContentHash expected, const u8 *p, u32 n, groove_ChunkNode *out);
static jz_err decode_object_verified_node_for_format(u8 format_version, u8 kind, const u8 *p, u32 n, groove_ChunkNode *out);
static jz_err validate_logical(u8 kind, const u8 *p, u32 n);
static jz_err check_format(u8 format);
static jz_err base_length(u64 final_length, const groove_ReplaceEditVec *edits, RangeUnit unit, u64 *out);
static jz_err utf16_length_for_byte_range_attempt(const LVRef *value, u64 start, u64 end, groove_EvaluationInputs *inputs, u64 *out);
static jz_err tail_base_descriptor(const LVRef *value, LVRef *out);
static jz_err leaf_ranges(u8 kind, const u8 *p, u32 n, RangeVec *out);
static jz_err prepare_with_locator(u8 kind, const u8 *p, u32 n, LocatorFn locator_for, void *ctx, groove_PreparedLargeValue *out);
jz_err groove__large_values__byte_range_attempt(const LVRef *value, u64 start, u64 end, groove_EvaluationInputs *inputs, jz_bytes *out);
jz_err groove__large_values__decode_node(u8 kind, groove_ContentHash expected, const u8 *p, u32 n, groove_ChunkNode *out);
jz_err groove__large_values__encode_node(const groove_ChunkNode *node, jz_bytes *out);
groove_ContentHash groove__large_values__object_hash(const u8 *p, u32 n);
jz_err groove__large_values__encode_node_ref(const groove_NodeRef *node_ref, jz_bytes *out);

/* ------------------------------------------------------------------ small helpers */
static bool metrics_eq(Metrics a, Metrics b) {
    return a.byte_length == b.byte_length && a.has_utf16 == b.has_utf16 && (!a.has_utf16 || a.utf16_length == b.utf16_length);
}
static bool hash_eq(const groove_ContentHash *a, const groove_ContentHash *b) { return memcmp(a->b, b->b, 32) == 0; }
static int node_ref_cmp(const void *a, const void *b, void *ctx) { (void)ctx; return memcmp(a, b, sizeof(groove_NodeRef)); }
static int content_hash_cmp(const void *a, const void *b, void *ctx) { (void)ctx; return memcmp(a, b, 32); }
static bool node_ref_eq(const groove_NodeRef *a, const groove_NodeRef *b) { return memcmp(a, b, sizeof *a) == 0; }
static bool checked_add_u64(u64 a, u64 b, u64 *out) { return !__builtin_add_overflow(a, b, out); }
static bool checked_sub_u64(u64 a, u64 b, u64 *out) { return !__builtin_sub_overflow(a, b, out); }

/* std::str::from_utf8: returns 0 ok, 1 incomplete trailing sequence
 * (error_len() == None), 2 invalid sequence; *valid_up_to as Utf8Error. */
static int utf8_scan(const u8 *p, u32 n, u32 *valid_up_to) {
    u32 i = 0;
    while (i < n) {
        u8 c = p[i];
        u32 need;
        u32 cp_min;
        if (c < 0x80) { i++; continue; }
        else if ((c & 0xe0) == 0xc0) { need = 1; cp_min = 0x80; }
        else if ((c & 0xf0) == 0xe0) { need = 2; cp_min = 0x800; }
        else if ((c & 0xf8) == 0xf0 && c <= 0xf4) { need = 3; cp_min = 0x10000; }
        else { *valid_up_to = i; return 2; }
        u32 cp = c & (0x3f >> need);
        for (u32 k = 1; k <= need; k++) {
            if (i + k >= n) {
                /* incomplete only if every present byte is a valid prefix */
                *valid_up_to = i;
                return 1;
            }
            u8 cc = p[i + k];
            if ((cc & 0xc0) != 0x80) { *valid_up_to = i; return 2; }
            if (k == 1) {
                if (need == 2 && c == 0xe0 && cc < 0xa0) { *valid_up_to = i; return 2; }
                if (need == 2 && c == 0xed && cc > 0x9f) { *valid_up_to = i; return 2; }
                if (need == 3 && c == 0xf0 && cc < 0x90) { *valid_up_to = i; return 2; }
                if (need == 3 && c == 0xf4 && cc > 0x8f) { *valid_up_to = i; return 2; }
            }
            cp = (cp << 6) | (cc & 0x3f);
        }
        if (cp < cp_min) { *valid_up_to = i; return 2; }
        i += need + 1;
    }
    *valid_up_to = n;
    return 0;
}
static bool utf8_ok(const u8 *p, u32 n) { u32 v; return utf8_scan(p, n, &v) == 0; }

/* text.encode_utf16().count() for valid UTF-8 */
static u64 utf16_count(const u8 *p, u32 n) {
    u64 count = 0;
    for (u32 i = 0; i < n; i++) {
        if ((p[i] & 0xc0) != 0x80) count += 1;
        if ((p[i] & 0xf8) == 0xf0) count += 1;
    }
    return count;
}

/* fn utf16_length_of(text: &str) -> Result<u64, Error> */
static jz_err utf16_length_of(const u8 *p, u32 n, u64 *out) {
    *out = utf16_count(p, n);
    return JZ_OK;
}

/* ------------------------------------------------------------------ derives */
void groove__large_values__ReplaceEdit__drop(groove_ReplaceEdit *e) { jz_bytes_drop(&e->insert_bytes); }
static void replace_edit_drop_v(void *e) { groove__large_values__ReplaceEdit__drop(e); }
void groove__large_values__ReplaceEdit__Clone__clone(groove_ReplaceEdit *dst, const groove_ReplaceEdit *src) {
    *dst = *src;
    dst->insert_bytes = jz_bytes_clone(&src->insert_bytes);
}
static void replace_edit_clone_v(void *dst, const void *src) { groove__large_values__ReplaceEdit__Clone__clone(dst, src); }
bool groove__large_values__ReplaceEdit__PartialEq__eq(const groove_ReplaceEdit *a, const groove_ReplaceEdit *b) {
    return a->offset == b->offset && a->delete_length == b->delete_length && jz_bytes_eq(&a->insert_bytes, &b->insert_bytes) &&
           a->utf16_offset == b->utf16_offset && a->delete_utf16_length == b->delete_utf16_length &&
           a->insert_utf16_length == b->insert_utf16_length;
}

void groove__large_values__LargeValueRef__drop(LVRef *r) { jz_vec_drop(&r->edit_tail, sizeof(groove_ReplaceEdit), replace_edit_drop_v); }
void groove__large_values__LargeValueRef__Clone__clone(LVRef *dst, const LVRef *src) {
    *dst = *src;
    memset(&dst->edit_tail, 0, sizeof dst->edit_tail);
    jz_vec_clone(&dst->edit_tail, &src->edit_tail, sizeof(groove_ReplaceEdit), replace_edit_clone_v);
}
bool groove__large_values__LargeValueRef__PartialEq__eq(const LVRef *a, const LVRef *b) {
    if (a->kind != b->kind || a->format_version != b->format_version || !hash_eq(&a->logical_hash, &b->logical_hash) ||
        !node_ref_eq(&a->root, &b->root) || a->byte_length != b->byte_length || a->has_utf16_length != b->has_utf16_length ||
        (a->has_utf16_length && a->utf16_length != b->utf16_length) || a->edit_tail.len != b->edit_tail.len)
        return false;
    for (u32 i = 0; i < a->edit_tail.len; i++)
        if (!groove__large_values__ReplaceEdit__PartialEq__eq(&a->edit_tail.ptr[i], &b->edit_tail.ptr[i])) return false;
    return true;
}

void groove__large_values__ChunkNode__drop(groove_ChunkNode *n) {
    jz_bytes_drop(&n->bytes);
    jz_vec_drop(&n->children, sizeof(groove_BranchChild), NULL);
}

void groove__large_values__StoredScalar__drop(groove_StoredScalar *s) {
    if (s->tag == GROOVE_STORED_PRIMITIVE) jz_bytes_drop(&s->primitive);
    else groove__large_values__LargeValueRef__drop(&s->chunked);
}

static const char *const KIND_NAMES[] = {"Bytes", "String", "Json"};
void groove__large_values__LargeValueKind__Debug__fmt(jz_str *out, u8 kind) { jz_str_push_cstr(out, KIND_NAMES[kind]); }

static void debug_u8_array(jz_str *out, const u8 *b, u32 n) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < n; i++) jz_str_appendf(out, i ? ", %u" : "%u", (unsigned)b[i]);
    jz_str_push_char(out, ']');
}
void groove__large_values__ContentHash__Debug__fmt(jz_str *out, const groove_ContentHash *h) {
    jz_str_push_cstr(out, "ContentHash(");
    debug_u8_array(out, h->b, 32);
    jz_str_push_char(out, ')');
}
void groove__large_values__Locator__Debug__fmt(jz_str *out, const groove_Locator *l) {
    jz_str_push_cstr(out, "Locator(");
    debug_u8_array(out, l->b, 32);
    jz_str_push_char(out, ')');
}
void groove__large_values__NodeRef__Debug__fmt(jz_str *out, const groove_NodeRef *r) {
    jz_str_push_cstr(out, "NodeRef { object_hash: ");
    groove__large_values__ContentHash__Debug__fmt(out, &r->object_hash);
    jz_str_push_cstr(out, ", locator: ");
    groove__large_values__Locator__Debug__fmt(out, &r->locator);
    jz_str_push_cstr(out, " }");
}
static void debug_opt_u64(jz_str *out, bool has, u64 v) {
    if (has) jz_str_appendf(out, "Some(%llu)", (unsigned long long)v);
    else jz_str_push_cstr(out, "None");
}
void groove__large_values__LargeValueRef__Debug__fmt(jz_str *out, const LVRef *r) {
    jz_str_push_cstr(out, "LargeValueRef { kind: ");
    groove__large_values__LargeValueKind__Debug__fmt(out, r->kind);
    jz_str_appendf(out, ", format_version: %u, logical_hash: ", (unsigned)r->format_version);
    groove__large_values__ContentHash__Debug__fmt(out, &r->logical_hash);
    jz_str_push_cstr(out, ", root: ");
    groove__large_values__NodeRef__Debug__fmt(out, &r->root);
    jz_str_appendf(out, ", byte_length: %llu, utf16_length: ", (unsigned long long)r->byte_length);
    debug_opt_u64(out, r->has_utf16_length, r->utf16_length);
    jz_str_push_cstr(out, ", edit_tail: [");
    for (u32 i = 0; i < r->edit_tail.len; i++) {
        const groove_ReplaceEdit *e = &r->edit_tail.ptr[i];
        jz_str_appendf(out, "%sReplaceEdit { offset: %llu, delete_length: %llu, insert_bytes: ", i ? ", " : "",
                       (unsigned long long)e->offset, (unsigned long long)e->delete_length);
        debug_u8_array(out, e->insert_bytes.ptr, e->insert_bytes.len);
        jz_str_appendf(out, ", utf16_offset: %llu, delete_utf16_length: %llu, insert_utf16_length: %llu }",
                       (unsigned long long)e->utf16_offset, (unsigned long long)e->delete_utf16_length,
                       (unsigned long long)e->insert_utf16_length);
    }
    jz_str_push_cstr(out, "] }");
}

/* ------------------------------------------------------------------ serde */
/* ContentHash([u8;32]) newtype: postcard tuple of 32 u8; JSON array of 32 numbers */
void groove__large_values__ContentHash__Serialize__serialize(jz_pc_w *w, const groove_ContentHash *h) { jz_pc_raw(w, h->b, 32); }
jz_err groove__large_values__ContentHash__Deserialize__deserialize(jz_pc_r *r, groove_ContentHash *out) { return jz_pc_r_raw(r, out->b, 32); }
static void json_write_u8s(jz_json_w *w, const u8 *b, u32 n) {
    jz_jw_begin_arr(w);
    for (u32 i = 0; i < n; i++) jz_jw_u64(w, b[i]);
    jz_jw_end_arr(w);
}
static jz_err json_read_u8s(const jz_json *j, jz_bytes *out, const char *what) {
    if (!j || j->kind != JZ_JSON_ARR) return jz_err_fmt(JZ_JSON_ERR_TYPE, "invalid type: expected %s", what);
    jz_bytes b = {0};
    for (u32 i = 0; i < j->v.arr.len; i++) {
        u64 v;
        if (!jz_json_as_u64(j->v.arr.ptr[i], &v) || v > 255) {
            jz_bytes_drop(&b);
            return jz_err_fmt(JZ_JSON_ERR_TYPE, "invalid value: expected u8");
        }
        jz_bytes_push_u8(&b, (u8)v);
    }
    *out = b;
    return JZ_OK;
}
void groove__large_values__ContentHash__Serialize__serialize_json(jz_json_w *w, const groove_ContentHash *h) { json_write_u8s(w, h->b, 32); }
jz_err groove__large_values__ContentHash__Deserialize__deserialize_json(const jz_json *j, groove_ContentHash *out) {
    jz_bytes b;
    JZ_TRY(json_read_u8s(j, &b, "an array of length 32"));
    if (b.len != 32) {
        u32 len = b.len;
        jz_bytes_drop(&b);
        return jz_err_fmt(JZ_JSON_ERR_TYPE, "invalid length %u, expected an array of length 32", len);
    }
    memcpy(out->b, b.ptr, 32);
    jz_bytes_drop(&b);
    return JZ_OK;
}

/* impl Serialize for Locator: self.0.to_vec().serialize() */
void groove__large_values__Locator__Serialize__serialize(jz_pc_w *w, const groove_Locator *l) { jz_pc_bytes(w, l->b, GROOVE_LV_LOCATOR_BYTES); }
static jz_err locator_from_vec(jz_bytes bytes, groove_Locator *out, u32 code) {
    u32 length = bytes.len;
    if (length != GROOVE_LV_LOCATOR_BYTES) {
        jz_bytes_drop(&bytes);
        return jz_err_fmt(code, "chunk locator must be exactly %u bytes, got %u", GROOVE_LV_LOCATOR_BYTES, length);
    }
    memcpy(out->b, bytes.ptr, GROOVE_LV_LOCATOR_BYTES);
    jz_bytes_drop(&bytes);
    return JZ_OK;
}
/* impl Deserialize for Locator */
jz_err groove__large_values__Locator__Deserialize__deserialize(jz_pc_r *r, groove_Locator *out) {
    jz_bytes bytes;
    JZ_TRY(jz_pc_r_bytes(r, &bytes));
    return locator_from_vec(bytes, out, JZ_PC_BAD_VARIANT);
}
void groove__large_values__Locator__Serialize__serialize_json(jz_json_w *w, const groove_Locator *l) { json_write_u8s(w, l->b, 32); }
jz_err groove__large_values__Locator__Deserialize__deserialize_json(const jz_json *j, groove_Locator *out) {
    jz_bytes bytes;
    JZ_TRY(json_read_u8s(j, &bytes, "a sequence"));
    return locator_from_vec(bytes, out, JZ_JSON_ERR_TYPE);
}

void groove__large_values__NodeRef__Serialize__serialize(jz_pc_w *w, const groove_NodeRef *r) {
    groove__large_values__ContentHash__Serialize__serialize(w, &r->object_hash);
    groove__large_values__Locator__Serialize__serialize(w, &r->locator);
}
jz_err groove__large_values__NodeRef__Deserialize__deserialize(jz_pc_r *r, groove_NodeRef *out) {
    JZ_TRY(groove__large_values__ContentHash__Deserialize__deserialize(r, &out->object_hash));
    return groove__large_values__Locator__Deserialize__deserialize(r, &out->locator);
}
void groove__large_values__NodeRef__Serialize__serialize_json(jz_json_w *w, const groove_NodeRef *r) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "object_hash");
    groove__large_values__ContentHash__Serialize__serialize_json(w, &r->object_hash);
    jz_jw_key(w, "locator");
    groove__large_values__Locator__Serialize__serialize_json(w, &r->locator);
    jz_jw_end_obj(w);
}
static jz_err json_field(const jz_json *obj, const char *key, const jz_json **out) {
    *out = jz_json_obj_get(obj, key);
    if (!*out) return jz_err_fmt(JZ_JSON_ERR_MISSING_FIELD, "missing field `%s`", key);
    return JZ_OK;
}
jz_err groove__large_values__NodeRef__Deserialize__deserialize_json(const jz_json *j, groove_NodeRef *out) {
    const jz_json *f;
    JZ_TRY(jz_json_expect_obj(j, "struct NodeRef"));
    JZ_TRY(json_field(j, "object_hash", &f));
    JZ_TRY(groove__large_values__ContentHash__Deserialize__deserialize_json(f, &out->object_hash));
    JZ_TRY(json_field(j, "locator", &f));
    return groove__large_values__Locator__Deserialize__deserialize_json(f, &out->locator);
}

static void replace_edit_serialize(jz_pc_w *w, const groove_ReplaceEdit *e) {
    jz_pc_varint(w, e->offset);
    jz_pc_varint(w, e->delete_length);
    jz_pc_bytes(w, e->insert_bytes.ptr, e->insert_bytes.len);
    jz_pc_varint(w, e->utf16_offset);
    jz_pc_varint(w, e->delete_utf16_length);
    jz_pc_varint(w, e->insert_utf16_length);
}
static jz_err replace_edit_deserialize(jz_pc_r *r, groove_ReplaceEdit *e) {
    memset(e, 0, sizeof *e);
    JZ_TRY(jz_pc_r_varint(r, &e->offset));
    JZ_TRY(jz_pc_r_varint(r, &e->delete_length));
    JZ_TRY(jz_pc_r_bytes(r, &e->insert_bytes));
    jz_err err = jz_pc_r_varint(r, &e->utf16_offset);
    if (!err.code) err = jz_pc_r_varint(r, &e->delete_utf16_length);
    if (!err.code) err = jz_pc_r_varint(r, &e->insert_utf16_length);
    if (err.code) jz_bytes_drop(&e->insert_bytes);
    return err;
}

void groove__large_values__LargeValueRef__Serialize__serialize(jz_pc_w *w, const LVRef *r) {
    jz_pc_variant(w, r->kind);
    jz_pc_u8(w, r->format_version);
    groove__large_values__ContentHash__Serialize__serialize(w, &r->logical_hash);
    groove__large_values__NodeRef__Serialize__serialize(w, &r->root);
    jz_pc_varint(w, r->byte_length);
    jz_pc_option(w, r->has_utf16_length);
    if (r->has_utf16_length) jz_pc_varint(w, r->utf16_length);
    jz_pc_seq(w, r->edit_tail.len);
    for (u32 i = 0; i < r->edit_tail.len; i++) replace_edit_serialize(w, &r->edit_tail.ptr[i]);
}

jz_err groove__large_values__LargeValueRef__Deserialize__deserialize(jz_pc_r *r, LVRef *out) {
    memset(out, 0, sizeof *out);
    u32 kind;
    JZ_TRY(jz_pc_r_enter(r));
    jz_err e = jz_pc_r_variant(r, &kind, 3);
    out->kind = (u8)kind;
    if (!e.code) e = jz_pc_r_u8(r, &out->format_version);
    if (!e.code) e = groove__large_values__ContentHash__Deserialize__deserialize(r, &out->logical_hash);
    if (!e.code) e = groove__large_values__NodeRef__Deserialize__deserialize(r, &out->root);
    if (!e.code) e = jz_pc_r_varint(r, &out->byte_length);
    if (!e.code) e = jz_pc_r_option(r, &out->has_utf16_length);
    if (!e.code && out->has_utf16_length) e = jz_pc_r_varint(r, &out->utf16_length);
    u32 n = 0;
    if (!e.code) e = jz_pc_r_seq(r, &n);
    for (u32 i = 0; !e.code && i < n; i++) {
        groove_ReplaceEdit edit;
        e = replace_edit_deserialize(r, &edit);
        if (!e.code) JZ_VEC_PUSH(out->edit_tail, edit);
    }
    jz_pc_r_leave(r);
    if (e.code) groove__large_values__LargeValueRef__drop(out);
    return e;
}

void groove__large_values__LargeValueRef__Serialize__serialize_json(jz_json_w *w, const LVRef *r) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "kind");
    groove__large_values__LargeValueKind__Serialize__serialize_json(w, r->kind);
    jz_jw_key(w, "format_version");
    jz_jw_u64(w, r->format_version);
    jz_jw_key(w, "logical_hash");
    groove__large_values__ContentHash__Serialize__serialize_json(w, &r->logical_hash);
    jz_jw_key(w, "root");
    groove__large_values__NodeRef__Serialize__serialize_json(w, &r->root);
    jz_jw_key(w, "byte_length");
    jz_jw_u64(w, r->byte_length);
    jz_jw_key(w, "utf16_length");
    if (r->has_utf16_length) jz_jw_u64(w, r->utf16_length);
    else jz_jw_null(w);
    jz_jw_key(w, "edit_tail");
    jz_jw_begin_arr(w);
    for (u32 i = 0; i < r->edit_tail.len; i++) {
        const groove_ReplaceEdit *e = &r->edit_tail.ptr[i];
        jz_jw_begin_obj(w);
        jz_jw_key(w, "offset"); jz_jw_u64(w, e->offset);
        jz_jw_key(w, "delete_length"); jz_jw_u64(w, e->delete_length);
        jz_jw_key(w, "insert_bytes"); json_write_u8s(w, e->insert_bytes.ptr, e->insert_bytes.len);
        jz_jw_key(w, "utf16_offset"); jz_jw_u64(w, e->utf16_offset);
        jz_jw_key(w, "delete_utf16_length"); jz_jw_u64(w, e->delete_utf16_length);
        jz_jw_key(w, "insert_utf16_length"); jz_jw_u64(w, e->insert_utf16_length);
        jz_jw_end_obj(w);
    }
    jz_jw_end_arr(w);
    jz_jw_end_obj(w);
}

static jz_err json_u64_field(const jz_json *obj, const char *key, u64 *out) { return jz_json_field_u64(obj, key, out); }

jz_err groove__large_values__LargeValueRef__Deserialize__deserialize_json(const jz_json *j, LVRef *out) {
    memset(out, 0, sizeof *out);
    const jz_json *f;
    u64 v;
    JZ_TRY(jz_json_expect_obj(j, "struct LargeValueRef"));
    JZ_TRY(json_field(j, "kind", &f));
    JZ_TRY(groove__large_values__LargeValueKind__Deserialize__deserialize_json(f, &out->kind));
    JZ_TRY(json_u64_field(j, "format_version", &v));
    if (v > 255) return jz_err_fmt(JZ_JSON_ERR_TYPE, "invalid value: integer `%llu`, expected u8", (unsigned long long)v);
    out->format_version = (u8)v;
    JZ_TRY(json_field(j, "logical_hash", &f));
    JZ_TRY(groove__large_values__ContentHash__Deserialize__deserialize_json(f, &out->logical_hash));
    JZ_TRY(json_field(j, "root", &f));
    JZ_TRY(groove__large_values__NodeRef__Deserialize__deserialize_json(f, &out->root));
    JZ_TRY(json_u64_field(j, "byte_length", &out->byte_length));
    f = jz_json_obj_get(j, "utf16_length"); /* Option: missing == None */
    if (f && f->kind != JZ_JSON_NULL) {
        if (!jz_json_as_u64(f, &out->utf16_length)) return jz_err_fmt(JZ_JSON_ERR_TYPE, "invalid type: expected u64");
        out->has_utf16_length = true;
    }
    JZ_TRY(json_field(j, "edit_tail", &f));
    if (f->kind != JZ_JSON_ARR) return jz_err_fmt(JZ_JSON_ERR_TYPE, "invalid type: expected a sequence");
    for (u32 i = 0; i < f->v.arr.len; i++) {
        const jz_json *ej = f->v.arr.ptr[i];
        groove_ReplaceEdit e = {0};
        const jz_json *ib;
        jz_err err = jz_json_expect_obj(ej, "struct ReplaceEdit");
        if (!err.code) err = json_u64_field(ej, "offset", &e.offset);
        if (!err.code) err = json_u64_field(ej, "delete_length", &e.delete_length);
        if (!err.code) err = json_field(ej, "insert_bytes", &ib);
        if (!err.code) err = json_read_u8s(ib, &e.insert_bytes, "a sequence");
        if (!err.code) err = json_u64_field(ej, "utf16_offset", &e.utf16_offset);
        if (!err.code) err = json_u64_field(ej, "delete_utf16_length", &e.delete_utf16_length);
        if (!err.code) err = json_u64_field(ej, "insert_utf16_length", &e.insert_utf16_length);
        if (err.code) {
            groove__large_values__ReplaceEdit__drop(&e);
            groove__large_values__LargeValueRef__drop(out);
            return err;
        }
        JZ_VEC_PUSH(out->edit_tail, e);
    }
    return JZ_OK;
}

/* ------------------------------------------------------------------ LargeValueFormat */
typedef enum { LARGE_VALUE_FORMAT_V1 } LargeValueFormat;

static jz_err format_from_version(u8 version, LargeValueFormat *out) {
    switch (version) {
    case FORMAT_V1: *out = LARGE_VALUE_FORMAT_V1; return JZ_OK;
    default: return groove__large_values__Error__unsupported_format(version);
    }
}
static u8 format_version(LargeValueFormat f) {
    switch (f) {
    case LARGE_VALUE_FORMAT_V1: default: return FORMAT_V1;
    }
}
static jz_err format_validate_descriptor_shape(LargeValueFormat f, const LVRef *value) {
    switch (f) {
    case LARGE_VALUE_FORMAT_V1: default: return validate_descriptor_shape_v1(value);
    }
}
static jz_err format_validate_descriptor(LargeValueFormat f, const LVRef *value) {
    switch (f) {
    case LARGE_VALUE_FORMAT_V1: default: return validate_descriptor_v1(value);
    }
}
static jz_err format_decode_descriptor_payload(LargeValueFormat f, u8 kind, const u8 *payload, u32 n, LVRef *out) {
    switch (f) {
    case LARGE_VALUE_FORMAT_V1: default: return decode_descriptor_payload_v1(kind, payload, n, out);
    }
}
static jz_err format_encode_node(LargeValueFormat f, const groove_ChunkNode *node, jz_bytes *out) {
    switch (f) {
    case LARGE_VALUE_FORMAT_V1: default: return encode_node_v1(node, out);
    }
}
static jz_err format_decode_node(LargeValueFormat f, const u8 *encoded, u32 n, groove_ChunkNode *out) {
    switch (f) {
    case LARGE_VALUE_FORMAT_V1: default: JZ_TRY(decode_canonical_node_v1(encoded, n, out)); break;
    }
    if (node_format(out) != format_version(f)) {
        u8 found = node_format(out);
        groove__large_values__ChunkNode__drop(out);
        return groove__large_values__Error__unsupported_format(found);
    }
    return JZ_OK;
}

/* ------------------------------------------------------------------ Locator */
/* Locator::random(): allocate a fresh random 256-bit retrieval capability */
groove_Locator groove__large_values__Locator__random(void) {
    groove_Locator locator;
    jz_random_bytes(locator.b, GROOVE_LV_LOCATOR_BYTES);
    return locator;
}
const u8 *groove__large_values__Locator__as_bytes(const groove_Locator *l) { return l->b; }

groove_ValueType groove__large_values__physical_storage_value_type(groove_LargeValueKind kind) {
    return groove__records__values__ValueType__stored_scalar(kind);
}

/* ------------------------------------------------------------------ StreamingChecksum / LargeValueCursor */
jz_err groove__large_values__LargeValueCursor__new(LVRef value, u32 window_bytes, groove_LargeValueCursor *out) {
    jz_err e = validate_descriptor(&value);
    if (e.code) { groove__large_values__LargeValueRef__drop(&value); return e; }
    if (window_bytes == 0) { groove__large_values__LargeValueRef__drop(&value); return ERR(MalformedScalar); }
    out->value = value;
    out->offset = 0;
    out->window_bytes = window_bytes;
    return JZ_OK;
}
u64 groove__large_values__LargeValueCursor__offset(const groove_LargeValueCursor *c) { return c->offset; }
u64 groove__large_values__LargeValueCursor__remaining_bytes(const groove_LargeValueCursor *c) { return c->value.byte_length - c->offset; }
const LVRef *groove__large_values__LargeValueCursor__value(const groove_LargeValueCursor *c) { return &c->value; }
bool groove__large_values__LargeValueCursor__next_range(const groove_LargeValueCursor *c, u64 *start, u64 *end) {
    if (c->offset == c->value.byte_length) return false;
    u64 e;
    if (!checked_add_u64(c->offset, c->window_bytes, &e)) e = UINT64_MAX; /* saturating_add */
    *start = c->offset;
    *end = e < c->value.byte_length ? e : c->value.byte_length;
    return true;
}
void groove__large_values__LargeValueCursor__advance_to(groove_LargeValueCursor *c, u64 offset) { c->offset = offset; }
void groove__large_values__LargeValueCursor__drop(groove_LargeValueCursor *c) { groove__large_values__LargeValueRef__drop(&c->value); }

jz_err groove__large_values__StreamingChecksum__new(LVRef value, u32 window_bytes, u32 max_bytes_per_turn, groove_StreamingChecksum *out) {
    if (max_bytes_per_turn == 0) {
        groove__large_values__LargeValueRef__drop(&value);
        return ERR(MalformedScalar);
    }
    memset(out, 0, sizeof *out);
    JZ_TRY(groove__large_values__LargeValueCursor__new(value, window_bytes < max_bytes_per_turn ? window_bytes : max_bytes_per_turn, &out->cursor));
    out->hasher = jz_blake3_new();
    out->max_bytes_per_turn = max_bytes_per_turn;
    out->bytes_this_turn = 0;
    return JZ_OK;
}
const groove_LargeValueCursor *groove__large_values__StreamingChecksum__cursor(const groove_StreamingChecksum *s) { return &s->cursor; }
groove_StreamingExecutionStats groove__large_values__StreamingChecksum__stats(const groove_StreamingChecksum *s) { return s->stats; }

/* Commit one complete cursor window. *yield is true when the caller must
 * cooperatively yield before consuming more. */
jz_err groove__large_values__StreamingChecksum__consume_window(groove_StreamingChecksum *s, const u8 *bytes, u32 n, bool *yield) {
    u64 start, end;
    if (!groove__large_values__LargeValueCursor__next_range(&s->cursor, &start, &end)) return ERR(MalformedScalar);
    if ((u64)n != end - start) return ERR(MalformedScalar);
    jz_blake3_update(s->hasher, bytes, n);
    groove__large_values__LargeValueCursor__advance_to(&s->cursor, end);
    if (!checked_add_u64(s->stats.logical_bytes_consumed, n, &s->stats.logical_bytes_consumed)) return ERR(MetricOverflow);
    if (!checked_add_u64(s->stats.windows_consumed, 1, &s->stats.windows_consumed)) return ERR(MetricOverflow);
    if (__builtin_add_overflow(s->bytes_this_turn, n, &s->bytes_this_turn)) return ERR(MetricOverflow);
    *yield = s->bytes_this_turn >= s->max_bytes_per_turn && groove__large_values__LargeValueCursor__remaining_bytes(&s->cursor) != 0;
    return JZ_OK;
}

jz_err groove__large_values__StreamingChecksum__record_yield(groove_StreamingChecksum *s) {
    if (!checked_add_u64(s->stats.cooperative_yields, 1, &s->stats.cooperative_yields)) return ERR(MetricOverflow);
    s->bytes_this_turn = 0;
    return JZ_OK;
}

void groove__large_values__StreamingChecksum__drop(groove_StreamingChecksum *s) {
    groove__large_values__LargeValueCursor__drop(&s->cursor);
    if (s->hasher) jz_blake3_free(s->hasher);
    s->hasher = NULL;
}

/* consumes self */
jz_err groove__large_values__StreamingChecksum__finish(groove_StreamingChecksum *s, groove_ContentHash *hash, groove_StreamingExecutionStats *stats) {
    jz_err e = JZ_OK;
    if (groove__large_values__LargeValueCursor__remaining_bytes(&s->cursor) != 0) {
        e = ERR(RequiresEvaluation);
    } else {
        jz_blake3_finalize(s->hasher, hash->b);
        *stats = s->stats;
    }
    groove__large_values__StreamingChecksum__drop(s);
    return e;
}

/* ------------------------------------------------------------------ validate_staged_chunk_batch */
static const jz_btree_desc LOCATOR_HASH_MAP = {sizeof(groove_Locator), sizeof(groove_ContentHash), content_hash_cmp, NULL, NULL};

jz_err groove__large_values__validate_staged_chunk_batch(u8 kind, const groove_StagedChunk *chunks, u32 n) {
    jz_btree locator_hashes;
    jz_btree_init(&locator_hashes, &LOCATOR_HASH_MAP);
    jz_err e = JZ_OK;
    for (u32 i = 0; i < n && !e.code; i++) {
        const groove_StagedChunk *chunk = &chunks[i];
        groove_ContentHash existing;
        if (jz_btree_insert(&locator_hashes, &chunk->node_ref.locator, &chunk->node_ref.object_hash, &existing) &&
            !hash_eq(&existing, &chunk->node_ref.object_hash)) {
            e = ERR(ObjectHashMismatch);
            break;
        }
        /* decode_node also checks the encoded-size and object-hash bounds. */
        groove_ChunkNode node;
        e = groove__large_values__decode_node(kind, chunk->node_ref.object_hash, chunk->encoded.ptr, chunk->encoded.len, &node);
        if (!e.code) groove__large_values__ChunkNode__drop(&node);
    }
    jz_btree_drop(&locator_hashes);
    return e;
}

/* ------------------------------------------------------------------ LogicalValueValidator */
jz_err groove__large_values__LogicalValueValidator__new(const LVRef *value, groove_LogicalValueValidator *out) {
    JZ_TRY(validate_descriptor(value));
    memset(out, 0, sizeof *out);
    out->kind = value->kind;
    out->utf16_length = 0;
    out->has_json = value->kind == LVK_JSON;
    if (out->has_json) groove__large_values__json_syntax__StreamingJsonValidator__new(&out->json);
    return JZ_OK;
}

void groove__large_values__LogicalValueValidator__drop(groove_LogicalValueValidator *v) {
    jz_bytes_drop(&v->utf8_tail);
    if (v->has_json) groove__large_values__json_syntax__StreamingJsonValidator__drop(&v->json);
    v->has_json = false;
}

jz_err groove__large_values__LogicalValueValidator__push(groove_LogicalValueValidator *v, const u8 *bytes, u32 n) {
    if (v->kind == LVK_BYTES) return JZ_OK;
    jz_bytes_push(&v->utf8_tail, bytes, n);
    u32 valid;
    int status = utf8_scan(v->utf8_tail.ptr, v->utf8_tail.len, &valid);
    u64 len16;
    if (status == 0) {
        JZ_TRY(utf16_length_of(v->utf8_tail.ptr, v->utf8_tail.len, &len16));
        if (!checked_add_u64(v->utf16_length, len16, &v->utf16_length)) return ERR(MetricOverflow);
        if (v->has_json && !groove__large_values__json_syntax__StreamingJsonValidator__push(&v->json, v->utf8_tail.ptr, v->utf8_tail.len))
            return ERR(InvalidJson);
        v->utf8_tail.len = 0;
    } else {
        if (status == 2) return ERR(InvalidUtf8); /* error.error_len().is_some() */
        JZ_TRY(utf16_length_of(v->utf8_tail.ptr, valid, &len16));
        if (!checked_add_u64(v->utf16_length, len16, &v->utf16_length)) return ERR(MetricOverflow);
        if (v->has_json && !groove__large_values__json_syntax__StreamingJsonValidator__push(&v->json, v->utf8_tail.ptr, valid))
            return ERR(InvalidJson);
        /* utf8_tail.drain(..valid) */
        memmove(v->utf8_tail.ptr, v->utf8_tail.ptr + valid, v->utf8_tail.len - valid);
        v->utf8_tail.len -= valid;
    }
    return JZ_OK;
}

/* consumes self */
jz_err groove__large_values__LogicalValueValidator__finish(groove_LogicalValueValidator *v, const LVRef *value) {
    jz_err e = JZ_OK;
    if (v->utf8_tail.len != 0) {
        e = ERR(InvalidUtf8);
    } else if (v->has_json) {
        v->has_json = false;
        if (!groove__large_values__json_syntax__StreamingJsonValidator__finish(&v->json)) e = ERR(InvalidJson);
    }
    if (!e.code && value->kind != LVK_BYTES && !(value->has_utf16_length && value->utf16_length == v->utf16_length))
        e = ERR(DescriptorMismatch);
    groove__large_values__LogicalValueValidator__drop(v);
    return e;
}

/* ------------------------------------------------------------------ PushStreamingPreparation / StreamingTreeBuilder */
static void streaming_level_drop_v(void *p) { jz_vec_drop(&((groove_StreamingLevel *)p)->pending, sizeof(BuiltNode), NULL); }

static void streaming_tree_builder_new(groove_StreamingTreeBuilder *b, u8 kind, groove_LocatorForFn locator_for, void *locator_ctx, groove_StageFn stage, void *stage_ctx) {
    memset(b, 0, sizeof *b);
    b->kind = kind;
    b->locator_for = locator_for;
    b->locator_ctx = locator_ctx;
    b->stage = stage;
    b->stage_ctx = stage_ctx;
}

static void streaming_tree_builder_drop(groove_StreamingTreeBuilder *b) {
    jz_bytes_drop(&b->leaf);
    jz_vec_drop(&b->levels, sizeof(groove_StreamingLevel), streaming_level_drop_v);
}

static groove_Locator builder_locate(void *ctx, groove_ContentHash hash, bool *fresh) {
    groove_StreamingTreeBuilder *b = ctx;
    *fresh = true;
    return b->locator_for(b->locator_ctx, hash);
}

static jz_err builder_stage_node_to(groove_StreamingTreeBuilder *b, groove_ChunkNode *node, Metrics m, BuiltNode *out) {
    jz_err e = JZ_OK;
    Metrics actual;
    if (node_kind(node) != b->kind) { e = ERR(DescriptorMismatch); goto done; }
    e = node_metrics(b->kind, node, &actual);
    if (e.code) goto done;
    if (!metrics_eq(actual, m)) { e = ERR(DescriptorMismatch); goto done; }
    bool has_chunk;
    groove_StagedChunk chunk;
    e = seal_node(node, m, builder_locate, b, out, &has_chunk, &chunk);
    if (e.code) goto done;
    if (has_chunk) {
        e = b->stage(b->stage_ctx, chunk);
        if (e.code) goto done;
        if (!checked_add_u64(b->stats.staged_chunk_count, 1, &b->stats.staged_chunk_count)) e = ERR(MetricOverflow);
    }
done:
    groove__large_values__ChunkNode__drop(node);
    return e;
}

static jz_err builder_flush_group(groove_StreamingTreeBuilder *b, u32 level, BuiltNode *out) {
    BuiltNodeVec group;
    memcpy(&group, &b->levels.ptr[level].pending, sizeof group); /* mem::take */
    memset(&b->levels.ptr[level].pending, 0, sizeof group);
    b->levels.ptr[level].hash = 0;
    groove_ChunkNode chunk_node;
    Metrics m;
    jz_err e = branch_over(b->kind, group.ptr, group.len, &chunk_node, &m);
    jz_vec_drop(&group, sizeof(BuiltNode), NULL);
    JZ_TRY(e);
    return builder_stage_node_to(b, &chunk_node, m, out);
}

static jz_err builder_add_node(groove_StreamingTreeBuilder *b, u32 level, BuiltNode node) {
    for (;;) {
        if (level >= MAX_TREE_DEPTH) return ERR(MalformedNode);
        while (b->levels.len <= level) {
            groove_StreamingLevel fresh = {0};
            JZ_VEC_PUSH(b->levels, fresh);
        }
        groove_StreamingLevel *state = &b->levels.ptr[level];
        state->hash = roll_child(state->hash, b->kind, &node);
        JZ_VEC_PUSH(state->pending, node);
        state->total += 1;
        u32 count = state->pending.len;
        bool boundary = branch_cut(state->hash, count);
        u32 frontier = 0;
        for (u32 i = 0; i < b->levels.len; i++) frontier += b->levels.ptr[i].pending.len;
        if (frontier > b->stats.peak_frontier_nodes) b->stats.peak_frontier_nodes = frontier;
        if (!boundary && count < BRANCH_MAX_CHILDREN) return JZ_OK;
        JZ_TRY(builder_flush_group(b, level, &node));
        level += 1;
    }
}

static jz_err builder_emit_leaf(groove_StreamingTreeBuilder *b, jz_bytes bytes) {
    Metrics m;
    jz_err e = metrics(b->kind, bytes.ptr, bytes.len, &m);
    if (e.code) { jz_bytes_drop(&bytes); return e; }
    groove_ChunkNode leaf = leaf_node(b->kind, bytes);
    BuiltNode node;
    JZ_TRY(builder_stage_node_to(b, &leaf, m, &node));
    return builder_add_node(b, 0, node);
}

static jz_err builder_feed(groove_StreamingTreeBuilder *b, const u8 *bytes, u32 n) {
    if (!checked_add_u64(b->stats.input_bytes, n, &b->stats.input_bytes)) return ERR(MetricOverflow);
    jz_bytes_push(&b->leaf, bytes, n);
    if (b->leaf.len > b->stats.peak_leaf_buffer_bytes) b->stats.peak_leaf_buffer_bytes = b->leaf.len;
    for (;;) {
        bool has_boundary = false;
        u32 end = 0;
        u32 limit = b->leaf.len < LEAF_MAX_BYTES ? b->leaf.len : LEAF_MAX_BYTES;
        while (b->leaf_scan < limit) {
            b->leaf_hash = roll(b->leaf_hash, b->leaf.ptr[b->leaf_scan]);
            b->leaf_scan += 1;
            u32 length = b->leaf_scan;
            if (leaf_cut(b->leaf_hash, length) || length == LEAF_MAX_BYTES) {
                has_boundary = true;
                end = length;
                break;
            }
        }
        if (!has_boundary) break;
        if (b->kind != LVK_BYTES) {
            u32 valid;
            int status = utf8_scan(b->leaf.ptr, end, &valid);
            if (status == 1) end = valid;
            else if (status == 2) return ERR(InvalidUtf8);
            if (end == 0) break;
        }
        jz_bytes leaf = jz_bytes_from(b->leaf.ptr, end);
        memmove(b->leaf.ptr, b->leaf.ptr + end, b->leaf.len - end);
        b->leaf.len -= end;
        b->leaf_scan = 0;
        b->leaf_hash = 0;
        JZ_TRY(builder_emit_leaf(b, leaf));
    }
    return JZ_OK;
}

/* consumes self */
static jz_err builder_finish(groove_StreamingTreeBuilder *b, LVRef *out, groove_StreamingPrepareStats *stats) {
    jz_err e = JZ_OK;
    if (b->leaf.len != 0 || b->levels.len == 0) {
        jz_bytes leaf = b->leaf;
        memset(&b->leaf, 0, sizeof b->leaf);
        e = builder_emit_leaf(b, leaf);
        if (e.code) goto done;
    }
    for (u32 level = 0;; level++) {
        if (level >= b->levels.len) { e = ERR(MalformedNode); goto done; }
        if (b->levels.ptr[level].total == 1) {
            groove_StreamingLevel *state = &b->levels.ptr[level];
            if (state->pending.len == 0) { e = ERR(MalformedNode); goto done; }
            BuiltNode root = state->pending.ptr[--state->pending.len];
            if (b->kind == LVK_JSON) {
                /* JSON is validated on writes by the reader-facing facade. */
            }
            built_into_value_ref(&root, b->kind, out);
            *stats = b->stats;
            goto done;
        }
        if (b->levels.ptr[level].pending.len != 0) {
            BuiltNode node;
            e = builder_flush_group(b, level, &node);
            if (!e.code) e = builder_add_node(b, level + 1, node);
            if (e.code) goto done;
        }
    }
done:
    streaming_tree_builder_drop(b);
    return e;
}

static groove_Locator random_locator(void *ctx, groove_ContentHash hash) {
    (void)ctx; (void)hash;
    return groove__large_values__Locator__random();
}

static void push_streaming_new_with_locator(groove_PushStreamingPreparation *p, u8 kind, groove_LocatorForFn locator_for, void *locator_ctx, groove_StageFn stage, void *stage_ctx) {
    streaming_tree_builder_new(&p->builder, kind, locator_for, locator_ctx, stage, stage_ctx);
    p->has_json = kind == LVK_JSON;
    if (p->has_json) groove__large_values__json_syntax__StreamingJsonValidator__new(&p->json);
}

/* PushStreamingPreparation<fn(ContentHash) -> Locator, S>::new */
void groove__large_values__PushStreamingPreparation__new(groove_PushStreamingPreparation *p, u8 kind, groove_StageFn stage, void *stage_ctx) {
    push_streaming_new_with_locator(p, kind, random_locator, NULL, stage, stage_ctx);
}

jz_err groove__large_values__PushStreamingPreparation__push(groove_PushStreamingPreparation *p, const u8 *bytes, u32 n) {
    if (p->has_json && !groove__large_values__json_syntax__StreamingJsonValidator__push(&p->json, bytes, n)) return ERR(InvalidJson);
    return builder_feed(&p->builder, bytes, n);
}

/* consumes self */
jz_err groove__large_values__PushStreamingPreparation__finish(groove_PushStreamingPreparation *p, LVRef *out, groove_StreamingPrepareStats *stats) {
    if (p->has_json) {
        p->has_json = false;
        if (!groove__large_values__json_syntax__StreamingJsonValidator__finish(&p->json)) {
            streaming_tree_builder_drop(&p->builder);
            return ERR(InvalidJson);
        }
    }
    return builder_finish(&p->builder, out, stats);
}

void groove__large_values__PushStreamingPreparation__drop(groove_PushStreamingPreparation *p) {
    if (p->has_json) groove__large_values__json_syntax__StreamingJsonValidator__drop(&p->json);
    p->has_json = false;
    streaming_tree_builder_drop(&p->builder);
}

/* ------------------------------------------------------------------ zipper types (used by consolidation) */
typedef struct PathFrame { groove_NodeRef node_ref; groove_BranchChildVec children; u32 selected; } PathFrame;
typedef JZ_VEC(PathFrame) PathFrameVec;
typedef struct LocatedLeaf { groove_NodeRef node_ref; jz_bytes bytes; u64 start; PathFrameVec path; } LocatedLeaf;
typedef JZ_VEC(LocatedLeaf) LocatedLeafVec;
typedef jz_err (*ChooseFn)(void *ctx, const groove_BranchChild *children, u32 n, u32 *selected);

static void path_frame_drop_v(void *p) { jz_vec_drop(&((PathFrame *)p)->children, sizeof(groove_BranchChild), NULL); }
static void path_frame_clone_v(void *dst, const void *src) {
    const PathFrame *s = src;
    PathFrame *d = dst;
    *d = *s;
    memset(&d->children, 0, sizeof d->children);
    jz_vec_clone(&d->children, &s->children, sizeof(groove_BranchChild), NULL);
}
static void path_clone(PathFrameVec *dst, const PathFrameVec *src) {
    memset(dst, 0, sizeof *dst);
    jz_vec_clone(dst, src, sizeof(PathFrame), path_frame_clone_v);
}
static void path_drop(PathFrameVec *p) { jz_vec_drop(p, sizeof(PathFrame), path_frame_drop_v); }
static void located_leaf_drop(LocatedLeaf *l) { jz_bytes_drop(&l->bytes); path_drop(&l->path); }
static void located_leaf_drop_v(void *l) { located_leaf_drop(l); }

static jz_err descend_attempt(const LVRef *value, groove_NodeRef node_ref, groove_ContentHash expected_hash, PathFrameVec path, u64 max_depth, groove_EvaluationInputs *inputs, ChooseFn choose, void *choose_ctx, LocatedLeaf *out);
static jz_err locate_leaf_attempt(const LVRef *value, u64 offset, u64 base_len, groove_EvaluationInputs *inputs, LocatedLeaf *out);
static jz_err next_leaf_attempt(const LVRef *value, const LocatedLeaf *current, groove_EvaluationInputs *inputs, LocatedLeaf *out);
static void tree_stager_new(TreeStager *s, u8 kind, LocateFn locate, void *ctx);
static void tree_stager_drop(TreeStager *s);
static jz_err tree_stager_leaves(TreeStager *s, const u8 *p, u32 n, BuiltNodeVec *out);
static jz_err tree_stager_branches(TreeStager *s, const BuiltNode *level, u32 n, BuiltNodeVec *out);
static jz_err tree_stager_root(TreeStager *s, BuiltNodeVec level, u32 depth, u32 too_deep, BuiltNode *out);

/* reusing_stager: a stager that reuses the stored locator of every node already
 * in `existing` and names only new nodes with `fresh_locator`. */
typedef struct ReusingLocate { const jz_btree *existing; LocatorFn fresh_locator; void *fresh_ctx; } ReusingLocate;
static groove_Locator reusing_locate(void *ctx, groove_ContentHash hash, bool *fresh) {
    ReusingLocate *r = ctx;
    const groove_Locator *locator = jz_btree_get(r->existing, &hash);
    if (locator) { *fresh = false; return *locator; }
    *fresh = true;
    return r->fresh_locator(r->fresh_ctx, hash);
}
static void reusing_stager(TreeStager *s, u8 kind, ReusingLocate *ctx) { tree_stager_new(s, kind, reusing_locate, ctx); }

static const jz_btree_desc HASH_LOCATOR_MAP = {sizeof(groove_ContentHash), sizeof(groove_Locator), content_hash_cmp, NULL, NULL};

static groove_BranchChild built_to_child(const BuiltNode *n) {
    groove_BranchChild c = {n->node_ref, n->metrics, n->structural_hash};
    return c;
}
static BuiltNode child_to_built(const groove_BranchChild *c) {
    BuiltNode n = {c->node_ref, c->metrics, c->logical_hash};
    return n;
}

static jz_err finish_rebuilt_tree(const LVRef *value, BuiltNodeVec level, u32 depth, TreeStager *stager, groove_PreparedLargeValue *out);
static jz_err replace_tail_with_bounds_attempt(const LVRef *value, u64 offset, u64 delete_length, jz_bytes insert_bytes, groove_EvaluationInputs *inputs, bool enforce_tail_bounds, groove_TailOutcome *out);
jz_err groove__large_values__consolidate_single_edit_attempt(const LVRef *value, groove_EvaluationInputs *inputs, LocatorFn fresh_locator, void *fresh_ctx, groove_PreparedLargeValue *out);

void groove__large_values__PreparedLargeValue__drop(groove_PreparedLargeValue *p);
static void staged_chunk_drop_v(void *c) { jz_bytes_drop(&((groove_StagedChunk *)c)->encoded); }
void groove__large_values__PreparedLargeValue__drop(groove_PreparedLargeValue *p) {
    groove__large_values__LargeValueRef__drop(&p->value_ref);
    jz_vec_drop(&p->staged_chunks, sizeof(groove_StagedChunk), staged_chunk_drop_v);
}

/* ------------------------------------------------------------------ ConsolidationContinuation */
jz_err groove__large_values__ConsolidationContinuation__new(LVRef source, groove_ConsolidationContinuation *out) {
    jz_err e = validate_descriptor_shape(&source);
    if (e.code) { groove__large_values__LargeValueRef__drop(&source); return e; }
    memset(out, 0, sizeof *out);
    out->source = source;
    out->has_current = false;
    out->next_edit = 0;
    return JZ_OK;
}

void groove__large_values__ConsolidationContinuation__drop(groove_ConsolidationContinuation *c) {
    groove__large_values__LargeValueRef__drop(&c->source);
    if (c->has_current) groove__large_values__LargeValueRef__drop(&c->current);
    jz_vec_drop(&c->staged_chunks, sizeof(groove_StagedChunk), staged_chunk_drop_v);
}

/* -> Result<Option<PreparedLargeValue>, IvmRuntimeError> (*has_out) */
jz_err groove__large_values__ConsolidationContinuation__step(groove_ConsolidationContinuation *c, groove_EvaluationInputs *inputs, bool *has_out, groove_PreparedLargeValue *out) {
    *has_out = false;
    if (!c->has_current) {
        groove_ChunkNode node;
        JZ_TRY(load_authenticated_node_attempt(c->source.format_version, c->source.kind, &c->source.root, c->source.logical_hash, inputs, &node));
        Metrics m;
        jz_err e = node_metrics(c->source.kind, &node, &m);
        groove__large_values__ChunkNode__drop(&node);
        JZ_TRY(e);
        LVRef current = {0};
        current.kind = c->source.kind;
        current.format_version = c->source.format_version;
        current.logical_hash = c->source.logical_hash;
        current.root = c->source.root;
        current.byte_length = m.byte_length;
        current.has_utf16_length = m.has_utf16;
        current.utf16_length = m.utf16_length;
        c->current = current;
        c->has_current = true;
    }
    while (c->next_edit < c->source.edit_tail.len) {
        groove_ReplaceEdit edit;
        groove__large_values__ReplaceEdit__Clone__clone(&edit, &c->source.edit_tail.ptr[c->next_edit]);
        JZ_ASSERT(c->has_current, "initialized current base");
        groove_TailOutcome outcome;
        JZ_TRY(replace_tail_with_bounds_attempt(&c->current, edit.offset, edit.delete_length, edit.insert_bytes, inputs, false, &outcome));
        if (outcome.tag != GROOVE_TAIL_UPDATED) {
            groove__large_values__LargeValueRef__drop(&outcome.value);
            return ERR(MalformedScalar);
        }
        groove_PreparedLargeValue prepared;
        jz_err e = groove__large_values__consolidate_single_edit_attempt(&outcome.value, inputs, random_locator, NULL, &prepared);
        groove__large_values__LargeValueRef__drop(&outcome.value);
        JZ_TRY(e);
        for (u32 i = 0; i < prepared.staged_chunks.len; i++) {
            const groove_StagedChunk *chunk = &prepared.staged_chunks.ptr[i];
            groove_ChunkRequest request;
            memcpy(request.object_hash, chunk->node_ref.object_hash.b, 32);
            request.locator = chunk->node_ref.locator;
            groove__ivm__runtime__evaluation_session__EvaluationInputs__install_chunk(inputs, request, jz_bytes_clone(&chunk->encoded));
        }
        jz_vec_extend(&c->staged_chunks, prepared.staged_chunks.ptr, prepared.staged_chunks.len, sizeof(groove_StagedChunk));
        jz_vec_drop(&prepared.staged_chunks, sizeof(groove_StagedChunk), NULL);
        groove__large_values__LargeValueRef__drop(&c->current);
        c->current = prepared.value_ref;
        c->next_edit += 1;
    }
    if (!c->has_current) return ERR(MalformedScalar);
    LVRef current;
    groove__large_values__LargeValueRef__Clone__clone(&current, &c->current);
    if (current.byte_length != c->source.byte_length || current.has_utf16_length != c->source.has_utf16_length ||
        (current.has_utf16_length && current.utf16_length != c->source.utf16_length)) {
        groove__large_values__LargeValueRef__drop(&current);
        return ERR(DescriptorMismatch);
    }
    out->value_ref = current;
    out->staged_chunks = c->staged_chunks; /* mem::take */
    memset(&c->staged_chunks, 0, sizeof c->staged_chunks);
    *has_out = true;
    return JZ_OK;
}

/* ------------------------------------------------------------------ tail edits */
/* push_tail_edit: consumes `updated` and `edit` */
static jz_err push_tail_edit(const LVRef *value, LVRef updated, groove_ReplaceEdit edit, bool enforce_tail_bounds, groove_TailOutcome *out) {
    u64 next_tail_bytes = 0;
    for (u32 i = 0; i <= value->edit_tail.len; i++) {
        const groove_ReplaceEdit *e = i < value->edit_tail.len ? &value->edit_tail.ptr[i] : &edit;
        if (!checked_add_u64(next_tail_bytes, e->insert_bytes.len, &next_tail_bytes) || !checked_add_u64(next_tail_bytes, 24, &next_tail_bytes)) {
            groove__large_values__LargeValueRef__drop(&updated);
            groove__large_values__ReplaceEdit__drop(&edit);
            return ERR(MetricOverflow);
        }
    }
    bool over_bounds = value->edit_tail.len >= MAX_EDIT_COUNT || next_tail_bytes > MAX_EDIT_TAIL_BYTES;
    JZ_VEC_PUSH(updated.edit_tail, edit);
    out->tag = enforce_tail_bounds && over_bounds ? GROOVE_TAIL_CONSOLIDATION_REQUIRED : GROOVE_TAIL_UPDATED;
    out->value = updated;
    return JZ_OK;
}

/* Append bytes without reading the immutable base tree. Consumes insert_bytes. */
jz_err groove__large_values__append_tail(const LVRef *value, jz_bytes insert_bytes, groove_TailOutcome *out) {
    jz_err e = validate_descriptor(value);
    if (e.code) goto fail;
    switch (value->kind) {
    case LVK_BYTES: break;
    case LVK_STRING:
        if (!utf8_ok(insert_bytes.ptr, insert_bytes.len)) { e = ERR(InvalidUtf8); goto fail; }
        break;
    default:
        /* Appending a fragment cannot validate the resulting JSON. */
        e = ERR(InvalidJson);
        goto fail;
    }
    u64 inserted_length = insert_bytes.len;
    LVRef updated;
    groove__large_values__LargeValueRef__Clone__clone(&updated, value);
    if (!checked_add_u64(updated.byte_length, inserted_length, &updated.byte_length)) {
        groove__large_values__LargeValueRef__drop(&updated);
        e = ERR(MetricOverflow);
        goto fail;
    }
    if (updated.has_utf16_length) {
        if (!utf8_ok(insert_bytes.ptr, insert_bytes.len)) { groove__large_values__LargeValueRef__drop(&updated); e = ERR(InvalidUtf8); goto fail; }
        u64 inserted_utf16;
        utf16_length_of(insert_bytes.ptr, insert_bytes.len, &inserted_utf16);
        if (!checked_add_u64(updated.utf16_length, inserted_utf16, &updated.utf16_length)) {
            groove__large_values__LargeValueRef__drop(&updated);
            e = ERR(MetricOverflow);
            goto fail;
        }
    }
    groove_ReplaceEdit edit = {0};
    edit.offset = value->byte_length;
    edit.delete_length = 0;
    edit.insert_bytes = insert_bytes;
    edit.utf16_offset = value->has_utf16_length ? value->utf16_length : 0;
    edit.delete_utf16_length = 0;
    u64 diff = 0;
    if (updated.has_utf16_length && value->has_utf16_length && checked_sub_u64(updated.utf16_length, value->utf16_length, &diff))
        edit.insert_utf16_length = diff;
    else
        edit.insert_utf16_length = 0;
    return push_tail_edit(value, updated, edit, true, out);
fail:
    jz_bytes_drop(&insert_bytes);
    return e;
}

jz_err groove__large_values__replace_tail_attempt(const LVRef *value, u64 offset, u64 delete_length, jz_bytes insert_bytes, groove_EvaluationInputs *inputs, groove_TailOutcome *out) {
    return replace_tail_with_bounds_attempt(value, offset, delete_length, insert_bytes, inputs, true, out);
}

static jz_err replace_tail_with_bounds_attempt(const LVRef *value, u64 offset, u64 delete_length, jz_bytes insert_bytes, groove_EvaluationInputs *inputs, bool enforce_tail_bounds, groove_TailOutcome *out) {
    jz_err e = validate_descriptor(value);
    if (e.code) goto fail;
    u64 end;
    if (!checked_add_u64(offset, delete_length, &end)) { e = ERR(MetricOverflow); goto fail; }
    if (end > value->byte_length) { e = ERR(MalformedScalar); goto fail; }
    u64 inserted_length = insert_bytes.len;
    u64 next_length;
    if (!checked_sub_u64(value->byte_length, delete_length, &next_length) || !checked_add_u64(next_length, inserted_length, &next_length)) {
        e = ERR(MetricOverflow);
        goto fail;
    }
    bool has_next_utf16 = false;
    u64 next_utf16 = 0, utf16_offset = 0, delete_utf16_length = 0, insert_utf16_length = 0;
    switch (value->kind) {
    case LVK_BYTES: break;
    case LVK_STRING: {
        if (!utf8_ok(insert_bytes.ptr, insert_bytes.len)) { e = ERR(InvalidUtf8); goto fail; }
        u64 boundaries[2] = {offset, end};
        for (int b = 0; b < 2; b++) {
            u64 boundary = boundaries[b];
            if (boundary < value->byte_length) {
                jz_bytes byte;
                e = groove__large_values__byte_range_attempt(value, boundary, boundary + 1, inputs, &byte);
                if (e.code) goto fail;
                bool continuation = byte.len > 0 && (byte.ptr[0] & 0xc0) == 0x80;
                jz_bytes_drop(&byte);
                if (continuation) { e = ERR(InvalidUtf8); goto fail; }
            }
        }
        jz_bytes deleted;
        e = groove__large_values__byte_range_attempt(value, offset, end, inputs, &deleted);
        if (e.code) goto fail;
        if (!utf8_ok(deleted.ptr, deleted.len)) { jz_bytes_drop(&deleted); e = ERR(InvalidUtf8); goto fail; }
        e = utf16_length_for_byte_range_attempt(value, 0, offset, inputs, &utf16_offset);
        if (e.code) { jz_bytes_drop(&deleted); goto fail; }
        utf16_length_of(deleted.ptr, deleted.len, &delete_utf16_length);
        jz_bytes_drop(&deleted);
        utf16_length_of(insert_bytes.ptr, insert_bytes.len, &insert_utf16_length);
        if (!value->has_utf16_length) { e = ERR(MalformedScalar); goto fail; }
        u64 current = value->utf16_length;
        if (!checked_sub_u64(current, delete_utf16_length, &next_utf16) || !checked_add_u64(next_utf16, insert_utf16_length, &next_utf16)) {
            e = ERR(MetricOverflow);
            goto fail;
        }
        has_next_utf16 = true;
        break;
    }
    default: /* Json */
        if (offset != 0 || delete_length != value->byte_length) { e = ERR(InvalidJson); goto fail; }
        e = validate_logical(LVK_JSON, insert_bytes.ptr, insert_bytes.len);
        if (e.code) goto fail;
        if (!utf8_ok(insert_bytes.ptr, insert_bytes.len)) { e = ERR(InvalidUtf8); goto fail; }
        utf16_length_of(insert_bytes.ptr, insert_bytes.len, &insert_utf16_length);
        if (!value->has_utf16_length) { e = ERR(MalformedScalar); goto fail; }
        has_next_utf16 = true;
        next_utf16 = insert_utf16_length;
        utf16_offset = 0;
        delete_utf16_length = value->utf16_length;
        break;
    }
    LVRef updated;
    groove__large_values__LargeValueRef__Clone__clone(&updated, value);
    updated.byte_length = next_length;
    updated.has_utf16_length = has_next_utf16;
    updated.utf16_length = has_next_utf16 ? next_utf16 : 0;
    groove_ReplaceEdit edit = {offset, delete_length, insert_bytes, utf16_offset, delete_utf16_length, insert_utf16_length};
    return push_tail_edit(value, updated, edit, enforce_tail_bounds, out);
fail:
    jz_bytes_drop(&insert_bytes);
    return e;
}

/* ------------------------------------------------------------------ consolidation */
static jz_err choose_last(void *ctx, const groove_BranchChild *children, u32 n, u32 *selected) {
    (void)ctx; (void)children;
    if (n == 0) return ERR(MalformedNode);
    *selected = n - 1;
    return JZ_OK;
}

/* Consolidate an append-only tail by walking only the old tree's right spine. */
jz_err groove__large_values__consolidate_appends_attempt(const LVRef *value, groove_EvaluationInputs *inputs, LocatorFn fresh_locator, void *fresh_ctx, groove_PreparedLargeValue *out) {
    JZ_TRY(validate_descriptor_shape(value));
    u64 base_byte_length;
    JZ_TRY(base_length(value->byte_length, &value->edit_tail, RANGE_UNIT_BYTES, &base_byte_length));
    u64 cursor = base_byte_length;
    jz_bytes appended = {0};
    for (u32 i = 0; i < value->edit_tail.len; i++) {
        const groove_ReplaceEdit *edit = &value->edit_tail.ptr[i];
        if (edit->delete_length != 0 || edit->offset != cursor) { jz_bytes_drop(&appended); return ERR(MalformedScalar); }
        if (!checked_add_u64(cursor, edit->insert_bytes.len, &cursor)) { jz_bytes_drop(&appended); return ERR(MetricOverflow); }
        jz_bytes_push(&appended, edit->insert_bytes.ptr, edit->insert_bytes.len);
    }
    if (cursor != value->byte_length) { jz_bytes_drop(&appended); return ERR(DescriptorMismatch); }

    LocatedLeaf last_leaf;
    PathFrameVec empty = {0};
    jz_err e = descend_attempt(value, value->root, value->logical_hash, empty, MAX_TREE_DEPTH, inputs, choose_last, NULL, &last_leaf);
    if (e.code) { jz_bytes_drop(&appended); return e; }
    jz_btree existing;
    jz_btree_init(&existing, &HASH_LOCATOR_MAP);
    for (u32 i = 0; i < last_leaf.path.len; i++)
        jz_btree_insert(&existing, &last_leaf.path.ptr[i].node_ref.object_hash, &last_leaf.path.ptr[i].node_ref.locator, NULL);
    jz_btree_insert(&existing, &last_leaf.node_ref.object_hash, &last_leaf.node_ref.locator, NULL);
    u32 depth = last_leaf.path.len;

    jz_bytes logical_suffix = last_leaf.bytes;
    memset(&last_leaf.bytes, 0, sizeof last_leaf.bytes);
    jz_bytes_push(&logical_suffix, appended.ptr, appended.len);
    jz_bytes_drop(&appended);
    ReusingLocate rl = {&existing, fresh_locator, fresh_ctx};
    TreeStager stager;
    reusing_stager(&stager, value->kind, &rl);
    BuiltNodeVec replacement;
    e = tree_stager_leaves(&stager, logical_suffix.ptr, logical_suffix.len, &replacement);
    jz_bytes_drop(&logical_suffix);
    for (u32 f = last_leaf.path.len; !e.code && f > 0; f--) {
        PathFrame *frame = &last_leaf.path.ptr[f - 1];
        BuiltNodeVec rebuilt = {0};
        /* level.pop() */
        for (u32 i = 0; i + 1 < frame->children.len; i++) JZ_VEC_PUSH(rebuilt, child_to_built(&frame->children.ptr[i]));
        jz_vec_extend(&rebuilt, replacement.ptr, replacement.len, sizeof(BuiltNode));
        jz_vec_drop(&replacement, sizeof(BuiltNode), NULL);
        e = tree_stager_branches(&stager, rebuilt.ptr, rebuilt.len, &replacement);
        jz_vec_drop(&rebuilt, sizeof(BuiltNode), NULL);
    }
    located_leaf_drop(&last_leaf);
    if (!e.code) e = finish_rebuilt_tree(value, replacement, depth, &stager, out);
    tree_stager_drop(&stager);
    jz_btree_drop(&existing);
    return e;
}

/* Consolidate one replacement against an otherwise tail-free base. */
jz_err groove__large_values__consolidate_single_edit_attempt(const LVRef *value, groove_EvaluationInputs *inputs, LocatorFn fresh_locator, void *fresh_ctx, groove_PreparedLargeValue *out) {
    JZ_TRY(validate_descriptor_shape(value));
    if (value->edit_tail.len != 1) return ERR(MalformedScalar);
    const groove_ReplaceEdit *edit = &value->edit_tail.ptr[0];
    u64 base_len;
    JZ_TRY(base_length(value->byte_length, &value->edit_tail, RANGE_UNIT_BYTES, &base_len));
    u64 edit_end;
    if (!checked_add_u64(edit->offset, edit->delete_length, &edit_end)) return ERR(MetricOverflow);
    if (edit_end > base_len) return ERR(MalformedScalar);
    if (edit->offset == base_len) return groove__large_values__consolidate_appends_attempt(value, inputs, fresh_locator, fresh_ctx, out);

    LocatedLeafVec leaves = {0};
    LocatedLeaf first;
    JZ_TRY(locate_leaf_attempt(value, edit->offset, base_len, inputs, &first));
    JZ_VEC_PUSH(leaves, first);
    jz_bytes segment = {0};
    jz_err e = JZ_OK;
    for (;;) {
        LocatedLeaf *last = &leaves.ptr[leaves.len - 1];
        u64 covered_end;
        if (!checked_add_u64(last->start, last->bytes.len, &covered_end)) { e = ERR(MetricOverflow); goto out_leaves; }
        if (covered_end >= edit_end) {
            u64 segment_start = leaves.ptr[0].start;
            jz_bytes seg = {0};
            for (u32 i = 0; i < leaves.len; i++) jz_bytes_push(&seg, leaves.ptr[i].bytes.ptr, leaves.ptr[i].bytes.len);
            u64 local_start = edit->offset - segment_start;
            u64 local_end = edit_end - segment_start;
            if (local_start > UINT32_MAX || local_end > UINT32_MAX) { jz_bytes_drop(&seg); e = ERR(MetricOverflow); goto out_leaves; }
            /* segment.splice(local_start..local_end, insert_bytes) */
            jz_bytes spliced = {0};
            jz_bytes_push(&spliced, seg.ptr, (u32)local_start);
            jz_bytes_push(&spliced, edit->insert_bytes.ptr, edit->insert_bytes.len);
            jz_bytes_push(&spliced, seg.ptr + local_end, seg.len - (u32)local_end);
            jz_bytes_drop(&seg);
            RangeVec ranges;
            e = leaf_ranges(value->kind, spliced.ptr, spliced.len, &ranges);
            if (e.code) { jz_bytes_drop(&spliced); goto out_leaves; }
            bool resynchronized = false;
            if (ranges.len > 0) {
                Range r = ranges.ptr[ranges.len - 1];
                resynchronized = r.start > 0 && r.end - r.start == last->bytes.len &&
                                 memcmp(spliced.ptr + r.start, last->bytes.ptr, last->bytes.len) == 0;
            }
            jz_vec_drop(&ranges, sizeof(Range), NULL);
            if (resynchronized || covered_end == base_len) { segment = spliced; break; }
            jz_bytes_drop(&spliced);
        }
        LocatedLeaf next;
        e = next_leaf_attempt(value, last, inputs, &next);
        if (e.code) goto out_leaves;
        JZ_VEC_PUSH(leaves, next);
    }

    {
        const PathFrameVec *left_path = &leaves.ptr[0].path;
        const PathFrameVec *right_path = &leaves.ptr[leaves.len - 1].path;
        if (left_path->len != right_path->len) { e = ERR(InvalidTree); goto out_segment; }
        jz_btree existing;
        jz_btree_init(&existing, &HASH_LOCATOR_MAP);
        for (u32 i = 0; i < leaves.len; i++) {
            jz_btree_insert(&existing, &leaves.ptr[i].node_ref.object_hash, &leaves.ptr[i].node_ref.locator, NULL);
            for (u32 f = 0; f < leaves.ptr[i].path.len; f++)
                jz_btree_insert(&existing, &leaves.ptr[i].path.ptr[f].node_ref.object_hash, &leaves.ptr[i].path.ptr[f].node_ref.locator, NULL);
        }

        if (value->byte_length == 0) {
            if (segment.len != 0) e = ERR(DescriptorMismatch);
            else e = prepare_with_locator(value->kind, segment.ptr, segment.len, fresh_locator, fresh_ctx, out);
            jz_btree_drop(&existing);
            goto out_segment;
        }

        ReusingLocate rl = {&existing, fresh_locator, fresh_ctx};
        TreeStager stager;
        reusing_stager(&stager, value->kind, &rl);
        BuiltNodeVec replacement = {0};
        if (segment.len != 0) e = tree_stager_leaves(&stager, segment.ptr, segment.len, &replacement);

        for (u32 depth = left_path->len; !e.code && depth > 0; depth--) {
            const PathFrame *left = &left_path->ptr[depth - 1];
            const PathFrame *right = &right_path->ptr[depth - 1];
            BuiltNodeVec level = {0};
            for (u32 i = 0; i < left->selected; i++) JZ_VEC_PUSH(level, child_to_built(&left->children.ptr[i]));
            jz_vec_extend(&level, replacement.ptr, replacement.len, sizeof(BuiltNode));
            jz_vec_drop(&replacement, sizeof(BuiltNode), NULL);
            for (u32 i = right->selected + 1; i < right->children.len; i++) JZ_VEC_PUSH(level, child_to_built(&right->children.ptr[i]));
            bool higher_path_has_siblings = false;
            for (u32 d = 0; d + 1 < depth; d++) {
                if (left_path->ptr[d].selected > 0 || right_path->ptr[d].selected + 1 < right_path->ptr[d].children.len) {
                    higher_path_has_siblings = true;
                    break;
                }
            }
            if (level.len == 1 && !higher_path_has_siblings) {
                replacement = level;
            } else {
                e = tree_stager_branches(&stager, level.ptr, level.len, &replacement);
                jz_vec_drop(&level, sizeof(BuiltNode), NULL);
            }
        }
        if (!e.code) e = finish_rebuilt_tree(value, replacement, left_path->len, &stager, out);
        else jz_vec_drop(&replacement, sizeof(BuiltNode), NULL);
        tree_stager_drop(&stager);
        jz_btree_drop(&existing);
    }
out_segment:
    jz_bytes_drop(&segment);
out_leaves:
    jz_vec_drop(&leaves, sizeof(LocatedLeaf), located_leaf_drop_v);
    return e;
}

/* Stage parent levels over `level` (rebuilt at `depth`) until one root
 * remains, then check that root still describes `value`'s final contents.
 * Moves stager->staged into out on success. */
static jz_err finish_rebuilt_tree(const LVRef *value, BuiltNodeVec level, u32 depth, TreeStager *stager, groove_PreparedLargeValue *out) {
    BuiltNode root;
    JZ_TRY(tree_stager_root(stager, level, depth, GROOVE_LV_E_InvalidTree, &root));
    if (root.metrics.byte_length != value->byte_length || root.metrics.has_utf16 != value->has_utf16_length ||
        (root.metrics.has_utf16 && root.metrics.utf16_length != value->utf16_length))
        return ERR(DescriptorMismatch);
    built_into_value_ref(&root, value->kind, &out->value_ref);
    out->staged_chunks = stager->staged;
    memset(&stager->staged, 0, sizeof stager->staged);
    return JZ_OK;
}

/* ------------------------------------------------------------------ PhysicalTraversal */
typedef struct PhysicalTraversalEntry { groove_NodeRef node_ref; groove_NodeRefVec ancestors; } PhysicalTraversalEntry;
typedef struct PhysicalNodeState {
    groove_ContentHash expected_logical_hash;
    bool has_expected_metrics; Metrics expected_metrics;
    bool has_actual_metrics; Metrics actual_metrics;
} PhysicalNodeState;
typedef struct PhysicalTraversal {
    groove_NodeRef root;
    JZ_VEC(PhysicalTraversalEntry) pending;
    jz_btree nodes; /* NodeRef -> PhysicalNodeState */
    jz_btree edges; /* NodeRef -> Vec<NodeRef> */
    groove_PhysicalTraversalNodeBudget node_budget;
} PhysicalTraversal;

static void node_ref_vec_drop_v(void *v) { jz_vec_drop(v, sizeof(groove_NodeRef), NULL); }
static void entry_drop_v(void *e) { jz_vec_drop(&((PhysicalTraversalEntry *)e)->ancestors, sizeof(groove_NodeRef), NULL); }
static const jz_btree_desc NODE_STATE_MAP = {sizeof(groove_NodeRef), sizeof(PhysicalNodeState), node_ref_cmp, NULL, NULL};
static const jz_btree_desc NODE_EDGES_MAP = {sizeof(groove_NodeRef), sizeof(groove_NodeRefVec), node_ref_cmp, NULL, node_ref_vec_drop_v};
static const jz_btree_desc NODE_SET = {sizeof(groove_NodeRef), 0, node_ref_cmp, NULL, NULL};
static const jz_btree_desc NODE_DEPTH_MAP = {sizeof(groove_NodeRef), sizeof(u32), node_ref_cmp, NULL, NULL};
typedef struct EdgeClaim { groove_ContentHash hash; Metrics metrics; } EdgeClaim;
static const jz_btree_desc NODE_CLAIM_MAP = {sizeof(groove_NodeRef), sizeof(EdgeClaim), node_ref_cmp, NULL, NULL};

groove_PhysicalTraversalNodeBudget groove__large_values__PhysicalTraversalNodeBudget__new(void) {
    groove_PhysicalTraversalNodeBudget b = {MAX_PHYSICAL_TRAVERSAL_NODES};
    return b;
}
static groove_PhysicalTraversalNodeBudget budget_with_limit(u32 limit) { groove_PhysicalTraversalNodeBudget b = {limit}; return b; }
jz_err groove__large_values__PhysicalTraversalNodeBudget__consume(groove_PhysicalTraversalNodeBudget *b) {
    if (b->remaining < 1) return ERR(PhysicalTraversalNodeLimitExceeded);
    b->remaining -= 1;
    return JZ_OK;
}
jz_err groove__large_values__PhysicalTraversalNodeBudget__consume_many(groove_PhysicalTraversalNodeBudget *b, u32 count) {
    if (b->remaining < count) return ERR(PhysicalTraversalNodeLimitExceeded);
    b->remaining -= count;
    return JZ_OK;
}

static void traversal_drop(PhysicalTraversal *t) {
    jz_vec_drop(&t->pending, sizeof(PhysicalTraversalEntry), entry_drop_v);
    jz_btree_drop(&t->nodes);
    jz_btree_drop(&t->edges);
}

static jz_err traversal_new_with_node_limit(PhysicalTraversal *t, groove_NodeRef root, bool has_metrics, Metrics expected_metrics, groove_ContentHash expected_logical_hash, u32 max_nodes) {
    groove_PhysicalTraversalNodeBudget node_budget = budget_with_limit(max_nodes);
    JZ_TRY(groove__large_values__PhysicalTraversalNodeBudget__consume(&node_budget));
    memset(t, 0, sizeof *t);
    t->root = root;
    PhysicalTraversalEntry entry = {root, {0}};
    JZ_VEC_PUSH(t->pending, entry);
    jz_btree_init(&t->nodes, &NODE_STATE_MAP);
    PhysicalNodeState state = {expected_logical_hash, has_metrics, expected_metrics, false, {0}};
    jz_btree_insert(&t->nodes, &root, &state, NULL);
    jz_btree_init(&t->edges, &NODE_EDGES_MAP);
    t->node_budget = node_budget;
    return JZ_OK;
}

static void traversal_new(PhysicalTraversal *t, groove_NodeRef root, bool has_metrics, Metrics expected_metrics, groove_ContentHash expected_logical_hash) {
    jz_err e = traversal_new_with_node_limit(t, root, has_metrics, expected_metrics, expected_logical_hash, MAX_PHYSICAL_TRAVERSAL_NODES);
    if (e.code) jz_panic("the configured physical traversal node limit is non-zero");
}

/* Root metrics are only known up front when there is no edit tail. */
static void traversal_for_value(PhysicalTraversal *t, const LVRef *value) {
    Metrics root_metrics = {value->byte_length, value->has_utf16_length, value->utf16_length};
    traversal_new(t, value->root, value->edit_tail.len == 0, root_metrics, value->logical_hash);
}

static bool traversal_pop(PhysicalTraversal *t, PhysicalTraversalEntry *out) {
    if (!t->pending.len) return false;
    *out = t->pending.ptr[--t->pending.len];
    return true;
}

static jz_err traversal_validate_node(PhysicalTraversal *t, u8 kind, const groove_NodeRef *node_ref, const groove_ChunkNode *node) {
    PhysicalNodeState *state = jz_btree_get(&t->nodes, node_ref);
    if (!state) return ERR(InvalidTree);
    groove_ContentHash h = node_logical_hash(node);
    if (!hash_eq(&h, &state->expected_logical_hash)) return ERR(DescriptorMismatch);
    Metrics actual;
    JZ_TRY(node_metrics(kind, node, &actual));
    if (state->has_expected_metrics && !metrics_eq(state->expected_metrics, actual)) return ERR(DescriptorMismatch);
    state->has_actual_metrics = true;
    state->actual_metrics = actual;
    groove_NodeRefVec children = {0};
    if (node->tag == GROOVE_CHUNK_NODE_BRANCH) {
        /* collect::<BTreeSet<_>>() then into Vec: sorted, deduplicated */
        jz_btree set;
        jz_btree_init(&set, &NODE_SET);
        for (u32 i = 0; i < node->children.len; i++) jz_btree_insert(&set, &node->children.ptr[i].node_ref, NULL, NULL);
        groove_NodeRef *k; void *v;
        JZ_BTREE_FOR(&set, k, v) JZ_VEC_PUSH(children, *k);
        jz_btree_drop(&set);
    }
    jz_btree_insert(&t->edges, node_ref, &children, NULL);
    return JZ_OK;
}

static jz_err traversal_edge_claim(const PhysicalTraversal *t, const groove_NodeRef *node_ref, EdgeClaim *out) {
    const PhysicalNodeState *state = jz_btree_get(&t->nodes, node_ref);
    if (!state) return ERR(InvalidTree);
    if (!state->has_expected_metrics) return ERR(InvalidTree);
    out->hash = state->expected_logical_hash;
    out->metrics = state->expected_metrics;
    return JZ_OK;
}

static void traversal_seal_reused(PhysicalTraversal *t, const groove_NodeRef *node_ref) {
    groove_NodeRefVec empty = {0};
    jz_btree_insert(&t->edges, node_ref, &empty, NULL);
}

typedef JZ_VEC(const groove_NodeRef *) NodeRefPtrVec;
static int node_ref_ptr_cmp(const void *a, const void *b, void *ctx) { return node_ref_cmp(*(const groove_NodeRef *const *)a, *(const groove_NodeRef *const *)b, ctx); }
static const jz_btree_desc NODE_PTR_SET = {sizeof(const groove_NodeRef *), 0, node_ref_ptr_cmp, NULL, NULL};

static void max_depths_post_order(const groove_NodeRef *node_ref, const jz_btree *edges, jz_btree *seen, NodeRefPtrVec *order) {
    if (jz_btree_insert(seen, &node_ref, NULL, NULL)) return; /* !seen.insert() */
    const groove_NodeRefVec *children = jz_btree_get(edges, node_ref);
    if (children)
        for (u32 i = 0; i < children->len; i++) max_depths_post_order(&children->ptr[i], edges, seen, order);
    JZ_VEC_PUSH(*order, node_ref);
}

/* The longest root path to every node of the validated graph. */
static void traversal_max_depths(const PhysicalTraversal *t, jz_btree *depths) {
    NodeRefPtrVec order = {0};
    jz_btree seen;
    jz_btree_init(&seen, &NODE_PTR_SET);
    max_depths_post_order(&t->root, &t->edges, &seen, &order);
    jz_btree_drop(&seen);
    jz_btree_init(depths, &NODE_DEPTH_MAP);
    u32 zero = 0;
    jz_btree_insert(depths, &t->root, &zero, NULL);
    for (u32 i = order.len; i > 0; i--) {
        const groove_NodeRef *node_ref = order.ptr[i - 1];
        const u32 *d = jz_btree_get(depths, node_ref);
        u32 depth = d ? *d : 0;
        const groove_NodeRefVec *children = jz_btree_get(&t->edges, node_ref);
        if (!children) continue;
        for (u32 c = 0; c < children->len; c++) {
            bool inserted;
            u32 *entry = jz_btree_entry(depths, &children->ptr[c], &inserted);
            if (*entry < depth + 1) *entry = depth + 1;
        }
    }
    jz_vec_drop(&order, sizeof(const groove_NodeRef *), NULL);
}

static jz_err finish_height(const groove_NodeRef *node_ref, const jz_btree *edges, jz_btree *visiting, jz_btree *heights, u32 *out) {
    const u32 *h = jz_btree_get(heights, node_ref);
    if (h) { *out = *h; return JZ_OK; }
    if (jz_btree_insert(visiting, node_ref, NULL, NULL)) return ERR(InvalidTree);
    const groove_NodeRefVec *children = jz_btree_get(edges, node_ref);
    if (!children) return ERR(InvalidTree);
    u32 result = 0;
    for (u32 i = 0; i < children->len; i++) {
        u32 child_height;
        JZ_TRY(finish_height(&children->ptr[i], edges, visiting, heights, &child_height));
        if (child_height == UINT32_MAX) return ERR(InvalidTree);
        if (child_height + 1 > result) result = child_height + 1;
        if (result > MAX_TREE_DEPTH) return ERR(InvalidTree);
    }
    jz_btree_remove(visiting, node_ref, NULL, NULL);
    jz_btree_insert(heights, node_ref, &result, NULL);
    *out = result;
    return JZ_OK;
}

/* Verify the complete authenticated physical graph in O(V + E). */
static jz_err traversal_finish(const PhysicalTraversal *t) {
    jz_btree visiting, heights;
    jz_btree_init(&visiting, &NODE_SET);
    jz_btree_init(&heights, &NODE_DEPTH_MAP);
    u32 h;
    jz_err e = finish_height(&t->root, &t->edges, &visiting, &heights, &h);
    jz_btree_drop(&visiting);
    jz_btree_drop(&heights);
    return e;
}

static bool node_ref_vec_contains(const groove_NodeRefVec *v, const groove_NodeRef *r) {
    for (u32 i = 0; i < v->len; i++) if (node_ref_eq(&v->ptr[i], r)) return true;
    return false;
}

static jz_err traversal_discover_child(PhysicalTraversal *t, const PhysicalTraversalEntry *parent, const groove_BranchChild *child) {
    groove_NodeRefVec ancestors = {0};
    jz_vec_clone(&ancestors, &parent->ancestors, sizeof(groove_NodeRef), NULL);
    JZ_VEC_PUSH(ancestors, parent->node_ref);
    if (ancestors.len > MAX_TREE_DEPTH || node_ref_vec_contains(&ancestors, &child->node_ref)) {
        jz_vec_drop(&ancestors, sizeof(groove_NodeRef), NULL);
        return ERR(InvalidTree);
    }
    PhysicalNodeState *state = jz_btree_get(&t->nodes, &child->node_ref);
    if (state) {
        jz_vec_drop(&ancestors, sizeof(groove_NodeRef), NULL);
        if (!hash_eq(&state->expected_logical_hash, &child->logical_hash) ||
            (state->has_expected_metrics && !metrics_eq(state->expected_metrics, child->metrics)) ||
            (state->has_actual_metrics && !metrics_eq(state->actual_metrics, child->metrics)))
            return ERR(DescriptorMismatch);
        state->has_expected_metrics = true;
        state->expected_metrics = child->metrics;
    } else {
        jz_err e = groove__large_values__PhysicalTraversalNodeBudget__consume(&t->node_budget);
        if (e.code) { jz_vec_drop(&ancestors, sizeof(groove_NodeRef), NULL); return e; }
        PhysicalNodeState fresh = {child->logical_hash, true, child->metrics, false, {0}};
        jz_btree_insert(&t->nodes, &child->node_ref, &fresh, NULL);
        PhysicalTraversalEntry entry = {child->node_ref, ancestors};
        JZ_VEC_PUSH(t->pending, entry);
    }
    return JZ_OK;
}

static jz_err traversal_discover_children(PhysicalTraversal *t, const PhysicalTraversalEntry *parent, const groove_BranchChildVec *children) {
    for (u32 i = children->len; i > 0; i--) JZ_TRY(traversal_discover_child(t, parent, &children->ptr[i - 1]));
    return JZ_OK;
}

/* Authenticate `encoded` as `entry`'s node of `value`, then queue its children. */
static jz_err traversal_admit_encoded(PhysicalTraversal *t, const LVRef *value, const PhysicalTraversalEntry *entry, const u8 *encoded, u32 n) {
    groove_ChunkNode node;
    JZ_TRY(decode_node_for_format(value->format_version, value->kind, entry->node_ref.object_hash, encoded, n, &node));
    jz_err e = traversal_validate_node(t, value->kind, &entry->node_ref, &node);
    if (!e.code && node.tag == GROOVE_CHUNK_NODE_BRANCH) e = traversal_discover_children(t, entry, &node.children);
    groove__large_values__ChunkNode__drop(&node);
    return e;
}

/* ------------------------------------------------------------------ logical walk */
typedef struct WalkSpan { u64 start; u64 len; bool has_metrics; Metrics metrics; } WalkSpan;
static jz_err walk_span_end(const WalkSpan *s, u64 *out) {
    if (!checked_add_u64(s->start, s->len, out)) return ERR(MetricOverflow);
    return JZ_OK;
}

/* trait LogicalVisitor as a vtable; wants/branch NULL = default */
typedef struct LogicalVisitorVtbl {
    jz_err (*wants)(void *self, const WalkSpan *span, bool *out);
    jz_err (*leaf)(void *self, const WalkSpan *span, jz_bytes bytes); /* consumes bytes */
    jz_err (*branch)(void *self, const WalkSpan *span, const groove_BranchChild *children, u32 n);
    jz_err (*child_len)(void *self, const Metrics *metrics, u64 *out);
    bool exact_cover;
} LogicalVisitorVtbl;

typedef struct WalkPending { groove_NodeRef node_ref; groove_ContentHash expected_hash; WalkSpan span; u32 depth; } WalkPending;

static jz_err walk_logical(const LVRef *value, WalkSpan root, const LogicalVisitorVtbl *vt, void *visitor, groove_EvaluationInputs *inputs) {
    JZ_VEC(WalkPending) pending = {0};
    WalkPending first = {value->root, value->logical_hash, root, 0};
    JZ_VEC_PUSH(pending, first);
    u64 remaining = MAX_LOGICAL_TRAVERSAL_STEPS;
#define CHARGE(count) do { if (remaining < (u64)(count)) { e = ERR(TraversalWorkLimitExceeded); goto out; } remaining -= (count); } while (0)
    bool blocked = false;
    jz_err e = JZ_OK;
    while (pending.len) {
        WalkPending item = pending.ptr[--pending.len];
        CHARGE(1);
        if (item.depth > MAX_TREE_DEPTH) { e = ERR(InvalidTree); goto out; }
        bool wants = true;
        if (vt->wants) { e = vt->wants(visitor, &item.span, &wants); if (e.code) goto out; }
        if (!wants) continue;
        groove_ChunkNode node;
        e = load_authenticated_node_attempt(value->format_version, value->kind, &item.node_ref, item.expected_hash, inputs, &node);
        if (e.code && IS_BLOCKED(e)) {
            jz_err_drop(&e);
            e = JZ_OK;
            blocked = true;
            continue;
        }
        if (e.code) goto out;
        if (node.tag == GROOVE_CHUNK_NODE_LEAF) {
            jz_bytes bytes = node.bytes;
            memset(&node.bytes, 0, sizeof node.bytes);
            groove__large_values__ChunkNode__drop(&node);
            e = vt->leaf(visitor, &item.span, bytes);
            if (e.code) goto out;
        } else {
            if (vt->branch) {
                e = vt->branch(visitor, &item.span, node.children.ptr, node.children.len);
                if (e.code) { groove__large_values__ChunkNode__drop(&node); goto out; }
            }
            if (remaining < node.children.len) { groove__large_values__ChunkNode__drop(&node); e = ERR(TraversalWorkLimitExceeded); goto out; }
            remaining -= node.children.len;
            u32 first_idx = pending.len;
            u64 child_start = item.span.start;
            for (u32 i = 0; i < node.children.len; i++) {
                const groove_BranchChild *child = &node.children.ptr[i];
                u64 len;
                e = vt->child_len(visitor, &child->metrics, &len);
                if (e.code) { groove__large_values__ChunkNode__drop(&node); goto out; }
                WalkSpan child_span = {child_start, len, true, child->metrics};
                e = walk_span_end(&child_span, &child_start);
                if (e.code) { groove__large_values__ChunkNode__drop(&node); goto out; }
                WalkPending p = {child->node_ref, child->logical_hash, child_span, item.depth + 1};
                JZ_VEC_PUSH(pending, p);
            }
            groove__large_values__ChunkNode__drop(&node);
            if (vt->exact_cover) {
                u64 span_end;
                e = walk_span_end(&item.span, &span_end);
                if (e.code) goto out;
                if (child_start != span_end) { e = ERR(DescriptorMismatch); goto out; }
            }
            /* pending[first..].reverse() */
            for (u32 a = first_idx, b = pending.len; a + 1 < b; a++, b--) {
                WalkPending tmp = pending.ptr[a];
                pending.ptr[a] = pending.ptr[b - 1];
                pending.ptr[b - 1] = tmp;
            }
        }
    }
#undef CHARGE
    if (blocked) e = groove__ivm__runtime__IvmRuntimeError__evaluation_blocked();
out:
    jz_vec_drop(&pending, sizeof(WalkPending), NULL);
    return e;
}

/* ------------------------------------------------------------------ upload validation (async -> sync) */
static jz_err read_for_validation(const groove_LocalChunkReader *reader, const groove_NodeRef *node_ref, jz_bytes *out);

/* Authenticate the locally present prefix of a descriptor's tree and return
 * its current missing frontier. */
jz_err groove__large_values__missing_upload_frontier(const LVRef *value, const groove_LocalChunkReader *reader, u32 limit, groove_NodeRefVec *out) {
    JZ_TRY(check_format(value->format_version));
    PhysicalTraversal traversal;
    traversal_for_value(&traversal, value);
    groove_NodeRefVec missing = {0};
    jz_err e = JZ_OK;
    PhysicalTraversalEntry entry;
    while (traversal_pop(&traversal, &entry)) {
        jz_bytes encoded;
        e = groove__chunks__LocalChunkReader__get(reader, entry.node_ref.locator, entry.node_ref.object_hash, &encoded);
        if (e.code == GROOVE_CHUNK_STORAGE_E_Unavailable) {
            jz_err_drop(&e);
            e = JZ_OK;
            JZ_VEC_PUSH(missing, entry.node_ref);
            entry_drop_v(&entry);
            if (missing.len >= (limit > 1 ? limit : 1)) break;
            continue;
        }
        if (e.code) {
            e = groove__chunks__ChunkError__From__from(e);
            entry_drop_v(&entry);
            break;
        }
        e = traversal_admit_encoded(&traversal, value, &entry, encoded.ptr, encoded.len);
        jz_bytes_drop(&encoded);
        entry_drop_v(&entry);
        if (e.code) break;
    }
    if (!e.code && missing.len == 0) e = traversal_finish(&traversal);
    traversal_drop(&traversal);
    if (e.code) { jz_vec_drop(&missing, sizeof(groove_NodeRef), NULL); return e; }
    *out = missing;
    return JZ_OK;
}

/* Verify the complete local tree of an upload immediately before issuing its
 * staging receipt. `uploaded_chunks` is a BTreeSet<NodeRef>. */
jz_err groove__large_values__validate_finalized_upload(const LVRef *value, const groove_LocalChunkReader *reader, const jz_btree *uploaded_chunks, bool descriptor_was_bound_before_completion) {
    JZ_TRY(validate_descriptor(value));
    PhysicalTraversal traversal;
    traversal_for_value(&traversal, value);
    jz_err e = JZ_OK;
    PhysicalTraversalEntry entry;
    while (traversal_pop(&traversal, &entry)) {
        if (!descriptor_was_bound_before_completion && !jz_btree_contains(uploaded_chunks, &entry.node_ref)) {
            entry_drop_v(&entry);
            e = ERR(DescriptorMismatch);
            break;
        }
        jz_bytes encoded;
        e = read_for_validation(reader, &entry.node_ref, &encoded);
        if (!e.code) {
            e = traversal_admit_encoded(&traversal, value, &entry, encoded.ptr, encoded.len);
            jz_bytes_drop(&encoded);
        }
        entry_drop_v(&entry);
        if (e.code) break;
    }
    if (!e.code) e = traversal_finish(&traversal);
    traversal_drop(&traversal);
    return e;
}

typedef enum { REUSE_PROVEN, REUSE_NOT_REUSED, REUSE_UNPROVABLE } ReuseProof;

static jz_err prove_reused_edge(jz_btree *claims, const jz_btree *derived_depths, const groove_NodeRef *node_ref, EdgeClaim base_claim, u32 base_depth, ReuseProof *out) {
    const EdgeClaim *expected = jz_btree_get(claims, node_ref);
    if (!expected) { *out = REUSE_NOT_REUSED; return JZ_OK; }
    if (!hash_eq(&expected->hash, &base_claim.hash) || !metrics_eq(expected->metrics, base_claim.metrics)) return ERR(DescriptorMismatch);
    const u32 *depth = jz_btree_get(derived_depths, node_ref);
    if (!depth || *depth > base_depth) { *out = REUSE_UNPROVABLE; return JZ_OK; }
    jz_btree_remove(claims, node_ref, NULL, NULL);
    *out = REUSE_PROVEN;
    return JZ_OK;
}

static Metrics tree_metrics(const LVRef *tree) {
    Metrics m = {tree->byte_length, tree->has_utf16_length, tree->utf16_length};
    return m;
}

/* Validate a descriptor Groove derived from `base` by a local edit.
 * *ok = false when reuse cannot be proven within the traversal budget. */
jz_err groove__large_values__validate_derived_upload(const LVRef *value, const LVRef *base, const groove_LocalChunkReader *reader, const jz_btree *staged_chunks, bool *ok) {
    LVRef tree, base_tree;
    JZ_TRY(tail_base_descriptor(value, &tree));
    jz_err e = tail_base_descriptor(base, &base_tree);
    if (e.code) { groove__large_values__LargeValueRef__drop(&tree); return e; }
    PhysicalTraversal traversal;
    bool have_traversal = false;
    jz_btree claims, depths, visited;
    bool have_maps = false;
    JZ_VEC(struct { groove_NodeRef node_ref; u32 depth; }) pending = {0};
    *ok = false;
    if (value->kind != base->kind || value->format_version != base->format_version) { e = ERR(DescriptorMismatch); goto out; }
    /* Authenticate the base edge against the base root node itself. */
    jz_bytes encoded_base_root;
    e = read_for_validation(reader, &base->root, &encoded_base_root);
    if (e.code) goto out;
    groove_ChunkNode base_root;
    e = decode_node_for_format(base->format_version, base->kind, base->root.object_hash, encoded_base_root.ptr, encoded_base_root.len, &base_root);
    jz_bytes_drop(&encoded_base_root);
    if (e.code) goto out;
    {
        groove_ContentHash h = node_logical_hash(&base_root);
        Metrics m;
        e = node_metrics(base->kind, &base_root, &m);
        groove__large_values__ChunkNode__drop(&base_root);
        if (e.code) goto out;
        if (!hash_eq(&h, &base_tree.logical_hash) || !metrics_eq(m, tree_metrics(&base_tree))) { e = ERR(DescriptorMismatch); goto out; }
    }
    traversal_new(&traversal, value->root, true, tree_metrics(&tree), value->logical_hash);
    have_traversal = true;
    jz_btree_init(&claims, &NODE_CLAIM_MAP);
    jz_btree_init(&visited, &NODE_SET);
    jz_btree_init(&depths, &NODE_DEPTH_MAP);
    have_maps = true;
    PhysicalTraversalEntry entry;
    while (traversal_pop(&traversal, &entry)) {
        if (!jz_btree_contains(staged_chunks, &entry.node_ref)) {
            EdgeClaim claim;
            e = traversal_edge_claim(&traversal, &entry.node_ref, &claim);
            if (!e.code) {
                jz_btree_insert(&claims, &entry.node_ref, &claim, NULL);
                traversal_seal_reused(&traversal, &entry.node_ref);
            }
            entry_drop_v(&entry);
            if (e.code) goto out;
            continue;
        }
        jz_bytes encoded;
        e = read_for_validation(reader, &entry.node_ref, &encoded);
        if (!e.code) {
            e = traversal_admit_encoded(&traversal, value, &entry, encoded.ptr, encoded.len);
            jz_bytes_drop(&encoded);
        }
        entry_drop_v(&entry);
        if (e.code) goto out;
    }
    e = traversal_finish(&traversal);
    if (e.code) goto out;
    jz_btree_drop(&depths);
    traversal_max_depths(&traversal, &depths);

    /* The base descriptor is the edge into the base root, at depth zero. */
    ReuseProof proof;
    EdgeClaim base_claim = {base->logical_hash, tree_metrics(&base_tree)};
    e = prove_reused_edge(&claims, &depths, &base->root, base_claim, 0, &proof);
    if (e.code) goto out;
    if (proof == REUSE_NOT_REUSED) {
        __typeof__(*pending.ptr) p = {base->root, 0};
        JZ_VEC_PUSH(pending, p);
    } else if (proof == REUSE_UNPROVABLE) {
        goto out; /* Ok(false) */
    }
    while (claims.len != 0) {
        if (!pending.len) goto out; /* Ok(false) */
        __typeof__(*pending.ptr) item = pending.ptr[--pending.len];
        if (jz_btree_insert(&visited, &item.node_ref, NULL, NULL)) continue;
        if (visited.len > MAX_PHYSICAL_TRAVERSAL_NODES) goto out; /* Ok(false) */
        jz_bytes encoded;
        e = read_for_validation(reader, &item.node_ref, &encoded);
        if (e.code) goto out;
        groove_ChunkNode node;
        e = decode_node_for_format(base->format_version, base->kind, item.node_ref.object_hash, encoded.ptr, encoded.len, &node);
        jz_bytes_drop(&encoded);
        if (e.code) goto out;
        if (node.tag == GROOVE_CHUNK_NODE_BRANCH) {
            for (u32 i = node.children.len; i > 0; i--) {
                const groove_BranchChild *child = &node.children.ptr[i - 1];
                EdgeClaim claim = {child->logical_hash, child->metrics};
                e = prove_reused_edge(&claims, &depths, &child->node_ref, claim, item.depth + 1, &proof);
                if (e.code) { groove__large_values__ChunkNode__drop(&node); goto out; }
                if (proof == REUSE_NOT_REUSED) {
                    __typeof__(*pending.ptr) p = {child->node_ref, item.depth + 1};
                    JZ_VEC_PUSH(pending, p);
                } else if (proof == REUSE_UNPROVABLE) {
                    groove__large_values__ChunkNode__drop(&node);
                    goto out; /* Ok(false) */
                }
            }
        }
        groove__large_values__ChunkNode__drop(&node);
    }
    *ok = true;
out:
    if (have_traversal) traversal_drop(&traversal);
    if (have_maps) { jz_btree_drop(&claims); jz_btree_drop(&depths); jz_btree_drop(&visited); }
    jz_vec_drop(&pending, sizeof(*pending.ptr), NULL);
    groove__large_values__LargeValueRef__drop(&tree);
    groove__large_values__LargeValueRef__drop(&base_tree);
    return e;
}

/* Read one node back from local storage while validating an upload. */
static jz_err read_for_validation(const groove_LocalChunkReader *reader, const groove_NodeRef *node_ref, jz_bytes *out) {
    jz_err e = groove__chunks__LocalChunkReader__get(reader, node_ref->locator, node_ref->object_hash, out);
    if (e.code) return groove__chunks__ChunkError__From__from(e);
    groove__large_values__record_finalize_validation_bytes(out->len);
    return JZ_OK;
}

/* Test-only receipt in Rust (cfg(any(test, feature = "test"))); a no-op here. */
void groove__large_values__record_finalize_validation_bytes(u32 bytes) { (void)bytes; }

/* ------------------------------------------------------------------ BuiltNode / staging */
static void built_into_value_ref(const BuiltNode *b, u8 kind, LVRef *out) {
    memset(out, 0, sizeof *out);
    out->kind = kind;
    out->format_version = FORMAT_VERSION;
    out->logical_hash = b->structural_hash;
    out->root = b->node_ref;
    out->byte_length = b->metrics.byte_length;
    out->has_utf16_length = b->metrics.has_utf16;
    out->utf16_length = b->metrics.utf16_length;
}

/* The current-format branch over `group`, with its summed metrics. */
static jz_err branch_over(u8 kind, const BuiltNode *group, u32 n, groove_ChunkNode *node, Metrics *m) {
    if (n == 0) return ERR(MalformedNode);
    Metrics acc = group[0].metrics;
    for (u32 i = 1; i < n; i++) JZ_TRY(add_metrics(acc, group[i].metrics, &acc));
    memset(node, 0, sizeof *node);
    node->tag = GROOVE_CHUNK_NODE_BRANCH;
    node->format = FORMAT_VERSION;
    node->kind = kind;
    for (u32 i = 0; i < n; i++) JZ_VEC_PUSH(node->children, built_to_child(&group[i]));
    *m = acc;
    return JZ_OK;
}

/* Encode `node` and name it with the locator `locate` picks for its object hash. */
static jz_err seal_node(const groove_ChunkNode *node, Metrics m, LocateFn locate, void *ctx, BuiltNode *built, bool *has_chunk, groove_StagedChunk *chunk) {
    groove_ContentHash structural_hash = node_logical_hash(node);
    jz_bytes encoded;
    JZ_TRY(groove__large_values__encode_node(node, &encoded));
    groove_ContentHash object_hash = groove__large_values__object_hash(encoded.ptr, encoded.len);
    bool fresh;
    groove_Locator locator = locate(ctx, object_hash, &fresh);
    groove_NodeRef node_ref = {object_hash, locator};
    *has_chunk = fresh;
    if (fresh) {
        chunk->node_ref = node_ref;
        chunk->encoded = encoded;
    } else {
        jz_bytes_drop(&encoded);
    }
    built->node_ref = node_ref;
    built->metrics = m;
    built->structural_hash = structural_hash;
    return JZ_OK;
}

static groove_Locator fresh_locate(void *ctx, groove_ContentHash hash, bool *fresh) {
    void **c = ctx; /* {LocatorFn, ctx} */
    *fresh = true;
    return ((LocatorFn)c[0])(c[1], hash);
}

/* Construct the canonical tree, assigning a fresh opaque locator to each new node. */
static jz_err prepare_with_locator(u8 kind, const u8 *p, u32 n, LocatorFn locator_for, void *ctx, groove_PreparedLargeValue *out) {
    JZ_TRY(validate_logical(kind, p, n));
    void *closure[2] = {(void *)locator_for, ctx};
    TreeStager stager;
    tree_stager_new(&stager, kind, fresh_locate, closure);
    BuiltNodeVec leaves;
    jz_err e = tree_stager_leaves(&stager, p, n, &leaves);
    BuiltNode root;
    if (!e.code) e = tree_stager_root(&stager, leaves, 0, GROOVE_LV_E_MalformedNode, &root);
    if (e.code) { tree_stager_drop(&stager); return e; }
    built_into_value_ref(&root, kind, &out->value_ref);
    out->staged_chunks = stager.staged;
    return JZ_OK;
}

/* Construct a canonical tree with fresh random capabilities for every new node. */
jz_err groove__large_values__prepare(u8 kind, const u8 *p, u32 n, groove_PreparedLargeValue *out) {
    return prepare_with_locator(kind, p, n, random_locator, NULL, out);
}

/* ------------------------------------------------------------------ durable record schemas */
typedef struct DurableLargeValueRecordSchema { JZ_VEC(u16) field_ids; groove_RecordDescriptor descriptor; } DurableLargeValueRecordSchema;
typedef struct DurableField { u16 id; const char *name; groove_ValueType value_type; } DurableField;

static int u16_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    u16 x = *(const u16 *)a, y = *(const u16 *)b;
    return x < y ? -1 : x > y;
}
static const jz_btree_desc FIELD_MAP = {sizeof(u16), sizeof(groove_Value), u16_cmp, NULL, groove__records__values__Value__drop_v};

static int durable_field_cmp(const void *a, const void *b, void *ctx) { return u16_cmp(&((const DurableField *)a)->id, &((const DurableField *)b)->id, ctx); }

/* Construct a fixed engine-owned record layout from permanent, one-based
 * positional field IDs. Consumes the value types. */
static void durable_large_value_record_descriptor(DurableField *fields, u32 n, DurableLargeValueRecordSchema *out) {
    jz_sort(fields, n, sizeof(DurableField), durable_field_cmp, NULL);
    JZ_ASSERT(n != 0, "large-value record must have at least one field");
    for (u32 i = 0; i < n; i++)
        JZ_ASSERT(fields[i].id == i + 1, "large-value record field IDs must be contiguous starting at one");
    memset(out, 0, sizeof *out);
    jz_str names[8];
    groove_ValueType types[8];
    JZ_ASSERT(n <= 8, "large-value record field count");
    for (u32 i = 0; i < n; i++) {
        JZ_VEC_PUSH(out->field_ids, fields[i].id);
        names[i] = jz_format("f%04u_%s", (unsigned)fields[i].id, fields[i].name);
        types[i] = fields[i].value_type;
    }
    out->descriptor = groove__records__RecordDescriptor__new(names, types, n);
    for (u32 i = 0; i < n; i++) jz_str_drop(&names[i]);
}

typedef struct IdValue { u16 id; groove_Value value; } IdValue;

/* ordered_values: consumes the input values */
static jz_err durable_ordered_values(const DurableLargeValueRecordSchema *s, IdValue *input, u32 n, groove_ValueVec *out) {
    jz_btree values;
    jz_btree_init(&values, &FIELD_MAP);
    jz_err e = JZ_OK;
    for (u32 i = 0; i < n; i++) {
        groove_Value old;
        if (jz_btree_insert(&values, &input[i].id, &input[i].value, &old)) {
            groove__records__values__Value__drop(&old);
            if (!e.code) e = ERR(MalformedScalar);
        }
    }
    groove_ValueVec ordered = {0};
    if (!e.code) {
        jz_vec_reserve(&ordered, s->field_ids.len, sizeof(groove_Value));
        for (u32 i = 0; i < s->field_ids.len; i++) {
            groove_Value v;
            if (!jz_btree_remove(&values, &s->field_ids.ptr[i], NULL, &v)) { e = ERR(MalformedScalar); break; }
            JZ_VEC_PUSH(ordered, v);
        }
    }
    if (!e.code && values.len != 0) e = ERR(MalformedScalar);
    jz_btree_drop(&values);
    if (e.code) { groove__records__values__ValueVec__drop(&ordered); return e; }
    *out = ordered;
    return JZ_OK;
}

static jz_err durable_decode_values(const DurableLargeValueRecordSchema *s, const groove_Value *values, u32 n, jz_btree *out) {
    if (n != s->field_ids.len) return ERR(MalformedScalar);
    jz_btree_init(out, &FIELD_MAP);
    for (u32 i = 0; i < n; i++) {
        groove_Value v;
        groove__records__values__Value__Clone__clone(&v, &values[i]);
        jz_btree_insert(out, &s->field_ids.ptr[i], &v, NULL);
    }
    return JZ_OK;
}

static jz_err take_durable_large_value_field(jz_btree *fields, u16 id, groove_Value *out) {
    if (!jz_btree_remove(fields, &id, NULL, out)) return ERR(MalformedScalar);
    return JZ_OK;
}
static jz_err take_u8(jz_btree *fields, u16 id, u8 *out) {
    groove_Value v;
    JZ_TRY(take_durable_large_value_field(fields, id, &v));
    bool ok = v.tag == GROOVE_V_U8;
    if (ok) *out = v.u.u8_;
    groove__records__values__Value__drop(&v);
    return ok ? JZ_OK : ERR(MalformedScalar);
}
static jz_err take_u64(jz_btree *fields, u16 id, u64 *out) {
    groove_Value v;
    JZ_TRY(take_durable_large_value_field(fields, id, &v));
    bool ok = v.tag == GROOVE_V_U64;
    if (ok) *out = v.u.u64_;
    groove__records__values__Value__drop(&v);
    return ok ? JZ_OK : ERR(MalformedScalar);
}
static jz_err raw_bytes(const groove_Value *value, u8 out[32]) {
    if (value->tag != GROOVE_V_BYTES) return ERR(MalformedScalar);
    if (value->u.bytes.len != 32) return ERR(MalformedScalar);
    memcpy(out, value->u.bytes.ptr, 32);
    return JZ_OK;
}
static jz_err take_raw32(jz_btree *fields, u16 id, u8 out[32]) {
    groove_Value v;
    JZ_TRY(take_durable_large_value_field(fields, id, &v));
    jz_err e = raw_bytes(&v, out);
    groove__records__values__Value__drop(&v);
    if (e.code) { jz_err_drop(&e); return ERR(MalformedScalar); }
    return JZ_OK;
}
static jz_err take_bytes(jz_btree *fields, u16 id, jz_bytes *out) {
    groove_Value v;
    JZ_TRY(take_durable_large_value_field(fields, id, &v));
    if (v.tag != GROOVE_V_BYTES) { groove__records__values__Value__drop(&v); return ERR(MalformedScalar); }
    *out = v.u.bytes;
    return JZ_OK;
}

#define NODE_REF_OBJECT_HASH_FIELD 1
#define NODE_REF_LOCATOR_FIELD 2
#define PRIMITIVE_VALUE_FIELD 1
#define EDIT_OFFSET_FIELD 1
#define EDIT_DELETE_LENGTH_FIELD 2
#define EDIT_INSERT_BYTES_FIELD 3
#define EDIT_UTF16_OFFSET_FIELD 4
#define EDIT_DELETE_UTF16_LENGTH_FIELD 5
#define LARGE_VALUE_REF_FORMAT_VERSION_FIELD 1
#define LARGE_VALUE_REF_LOGICAL_HASH_FIELD 2
#define LARGE_VALUE_REF_ROOT_FIELD 3
#define LARGE_VALUE_REF_BYTE_LENGTH_FIELD 4
#define LARGE_VALUE_REF_EDIT_TAIL_FIELD 5
#define LARGE_VALUE_REF_UTF16_LENGTH_FIELD 6
#define LARGE_VALUE_REF_BYTES_TAG 0
#define LARGE_VALUE_REF_STRING_TAG 1
#define LARGE_VALUE_REF_JSON_TAG 2

#define VT_RAW_BYTES() groove__records__values__ValueType__raw_bytes()
#define VT_RAW_STRING() groove__records__values__ValueType__raw_string()
#define VT_SCALAR(t) groove__records__values__ValueType__scalar(t)
static groove_ValueType vt_record(groove_RecordDescriptor d) { groove_ValueType t; memset(&t, 0, sizeof t); t.tag = GROOVE_VT_RECORD; t.u.record = d; return t; }
static groove_ValueType vt_enum(const groove_EnumSchema *schema) {
    groove_ValueType t;
    memset(&t, 0, sizeof t);
    t.tag = GROOVE_VT_ENUM;
    t.u.enum_ = JZ_NEW(groove_EnumSchema);
    groove__records__values__EnumSchema__Clone__clone(t.u.enum_, schema);
    return t;
}
static groove_Value v_u8(u8 x) { groove_Value v; memset(&v, 0, sizeof v); v.tag = GROOVE_V_U8; v.u.u8_ = x; return v; }
static groove_Value v_u64(u64 x) { return groove__records__values__Value__from_u64(x); }
static groove_Value v_bytes(const u8 *p, u32 n) { return groove__records__values__Value__from_bytes(p, n); }
static groove_Value v_record(groove_OwnedRecord r) { groove_Value v; memset(&v, 0, sizeof v); v.tag = GROOVE_V_RECORD; v.u.record = r; return v; }
static groove_Value v_enum(groove_EnumValue ev) { groove_Value v; memset(&v, 0, sizeof v); v.tag = GROOVE_V_ENUM; v.u.enum_ = ev; return v; }
static groove_Value v_array(groove_ValueVec items) { groove_Value v; memset(&v, 0, sizeof v); v.tag = GROOVE_V_ARRAY; v.u.items = items; return v; }

static const DurableLargeValueRecordSchema *node_ref_record_schema(void) {
    static DurableLargeValueRecordSchema SCHEMA;
    static bool init;
    if (!init) {
        DurableField fields[2] = {
            {NODE_REF_OBJECT_HASH_FIELD, "object_hash", VT_RAW_BYTES()},
            {NODE_REF_LOCATOR_FIELD, "locator", VT_RAW_BYTES()},
        };
        durable_large_value_record_descriptor(fields, 2, &SCHEMA);
        init = true;
    }
    return &SCHEMA;
}
groove_RecordDescriptor groove__large_values__node_ref_descriptor(void) { return node_ref_record_schema()->descriptor; }

groove_Value groove__large_values__node_ref_value(const groove_NodeRef *node_ref) {
    groove_RecordDescriptor descriptor = groove__large_values__node_ref_descriptor();
    jz_bytes encoded;
    jz_err e = groove__large_values__encode_node_ref(node_ref, &encoded);
    if (e.code) jz_panic("NodeRef fields always match their physical descriptor");
    return v_record(groove__records__OwnedRecord__new(encoded, descriptor));
}

static jz_err node_ref_from_values(const groove_Value *values, u32 n, groove_NodeRef *out) {
    jz_btree fields;
    JZ_TRY(durable_decode_values(node_ref_record_schema(), values, n, &fields));
    jz_err e = take_raw32(&fields, NODE_REF_OBJECT_HASH_FIELD, out->object_hash.b);
    if (!e.code) e = take_raw32(&fields, NODE_REF_LOCATOR_FIELD, out->locator.b);
    jz_btree_drop(&fields);
    return e;
}

jz_err groove__large_values__node_ref_from_value(const groove_Value *value, groove_NodeRef *out) {
    if (value->tag != GROOVE_V_RECORD) return ERR(MalformedScalar);
    groove_ValueVec values;
    jz_err e = groove__records__OwnedRecord__to_values(&value->u.record, &values);
    if (e.code) { jz_err_drop(&e); return ERR(MalformedScalar); }
    e = node_ref_from_values(values.ptr, values.len, out);
    groove__records__values__ValueVec__drop(&values);
    return e;
}

/* Encode a physical chunk identity through the canonical Groove record. */
jz_err groove__large_values__encode_node_ref(const groove_NodeRef *node_ref, jz_bytes *out) {
    IdValue input[2] = {
        {NODE_REF_OBJECT_HASH_FIELD, v_bytes(node_ref->object_hash.b, 32)},
        {NODE_REF_LOCATOR_FIELD, v_bytes(node_ref->locator.b, 32)},
    };
    groove_ValueVec values;
    JZ_TRY(durable_ordered_values(node_ref_record_schema(), input, 2, &values));
    jz_err e = groove__records__RecordDescriptor__create(groove__large_values__node_ref_descriptor(), values.ptr, values.len, out);
    groove__records__values__ValueVec__drop(&values);
    if (e.code) { jz_err_drop(&e); return ERR(MalformedScalar); }
    return JZ_OK;
}

/* Decode and canonically validate a physical chunk identity. */
jz_err groove__large_values__decode_node_ref(const u8 *encoded, u32 n, groove_NodeRef *out) {
    groove_RecordDescriptor descriptor = groove__large_values__node_ref_descriptor();
    groove_BorrowedRecord bound = groove__records__RecordDescriptor__bind(descriptor, encoded, n);
    groove_ValueVec values;
    jz_err e = groove__records__BorrowedRecord__to_values(&bound, &values);
    if (e.code) { jz_err_drop(&e); return ERR(MalformedScalar); }
    e = node_ref_from_values(values.ptr, values.len, out);
    groove__records__values__ValueVec__drop(&values);
    JZ_TRY(e);
    jz_bytes canonical;
    JZ_TRY(groove__large_values__encode_node_ref(out, &canonical));
    bool same = canonical.len == n && memcmp(canonical.ptr, encoded, n) == 0;
    jz_bytes_drop(&canonical);
    return same ? JZ_OK : ERR(MalformedScalar);
}

static const DurableLargeValueRecordSchema *primitive_payload_schema(u8 kind) {
    static DurableLargeValueRecordSchema SCHEMAS[3];
    static bool init[3];
    if (!init[kind]) {
        DurableField fields[1] = {{PRIMITIVE_VALUE_FIELD, "value", kind == LVK_BYTES ? VT_RAW_BYTES() : VT_RAW_STRING()}};
        durable_large_value_record_descriptor(fields, 1, &SCHEMAS[kind]);
        init[kind] = true;
    }
    return &SCHEMAS[kind];
}

static const DurableLargeValueRecordSchema *large_value_edit_schema(u8 kind) {
    static DurableLargeValueRecordSchema BYTES, TEXT;
    static bool init_bytes, init_text;
    DurableLargeValueRecordSchema *schema = kind == LVK_BYTES ? &BYTES : &TEXT;
    bool *init = kind == LVK_BYTES ? &init_bytes : &init_text;
    if (!*init) {
        DurableField fields[5] = {
            {EDIT_OFFSET_FIELD, "offset", VT_SCALAR(GROOVE_VT_U64)},
            {EDIT_DELETE_LENGTH_FIELD, "delete_length", VT_SCALAR(GROOVE_VT_U64)},
            {EDIT_INSERT_BYTES_FIELD, "insert_bytes", VT_RAW_BYTES()},
        };
        u32 n = 3;
        if (kind != LVK_BYTES) {
            fields[n++] = (DurableField){EDIT_UTF16_OFFSET_FIELD, "utf16_offset", VT_SCALAR(GROOVE_VT_U64)};
            fields[n++] = (DurableField){EDIT_DELETE_UTF16_LENGTH_FIELD, "delete_utf16_length", VT_SCALAR(GROOVE_VT_U64)};
        }
        durable_large_value_record_descriptor(fields, n, schema);
        *init = true;
    }
    return schema;
}
static groove_RecordDescriptor large_value_edit_descriptor(u8 kind) { return large_value_edit_schema(kind)->descriptor; }

static const DurableLargeValueRecordSchema *large_value_ref_payload_schema(u8 kind) {
    static DurableLargeValueRecordSchema BYTES, TEXT;
    static bool init_bytes, init_text;
    DurableLargeValueRecordSchema *schema = kind == LVK_BYTES ? &BYTES : &TEXT;
    bool *init = kind == LVK_BYTES ? &init_bytes : &init_text;
    if (!*init) {
        DurableField fields[6] = {
            {LARGE_VALUE_REF_FORMAT_VERSION_FIELD, "format_version", VT_SCALAR(GROOVE_VT_U8)},
            {LARGE_VALUE_REF_LOGICAL_HASH_FIELD, "logical_hash", VT_RAW_BYTES()},
            {LARGE_VALUE_REF_ROOT_FIELD, "root", vt_record(groove__large_values__node_ref_descriptor())},
            {LARGE_VALUE_REF_BYTE_LENGTH_FIELD, "byte_length", VT_SCALAR(GROOVE_VT_U64)},
            {LARGE_VALUE_REF_EDIT_TAIL_FIELD, "edit_tail", groove__records__values__ValueType__array_of(vt_record(large_value_edit_descriptor(kind)))},
        };
        u32 n = 5;
        if (kind != LVK_BYTES) fields[n++] = (DurableField){LARGE_VALUE_REF_UTF16_LENGTH_FIELD, "utf16_length", VT_SCALAR(GROOVE_VT_U64)};
        durable_large_value_record_descriptor(fields, n, schema);
        *init = true;
    }
    return schema;
}
static groove_RecordDescriptor large_value_ref_payload_descriptor(u8 kind) { return large_value_ref_payload_schema(kind)->descriptor; }

static void build_stored_scalar_schema(u8 kind, groove_EnumSchema *out) {
    static const char *const NAMES[3] = {"groove.internal.stored_scalar.bytes", "groove.internal.stored_scalar.string", "groove.internal.stored_scalar.json"};
    groove_RecordDescriptor primitive = primitive_payload_schema(kind)->descriptor;
    groove_RecordDescriptor chunked = large_value_ref_payload_descriptor(kind);
    /* Tags 0 and 1 are reused (SPEC 9, "Scalar tag history"). */
    groove_EnumCaseVec cases = {0};
    JZ_VEC_PUSH(cases, groove__records__values__EnumCase__new(jz_str_from("Primitive"), primitive));
    JZ_VEC_PUSH(cases, groove__records__values__EnumCase__new(jz_str_from("Chunked"), chunked));
    jz_err e = groove__records__values__EnumSchema__new(jz_str_from(NAMES[kind]), cases, out);
    if (e.code) jz_panic("fixed internal stored-scalar enum schema is valid");
}

static const groove_EnumSchema *stored_scalar_schema(u8 kind) {
    static groove_EnumSchema SCHEMAS[3];
    static bool init[3];
    if (!init[kind]) { build_stored_scalar_schema(kind, &SCHEMAS[kind]); init[kind] = true; }
    return &SCHEMAS[kind];
}

static const groove_ValueType *stored_scalar_value_type(u8 kind) {
    static groove_ValueType TYPES[3];
    static bool init[3];
    if (!init[kind]) { TYPES[kind] = vt_enum(stored_scalar_schema(kind)); init[kind] = true; }
    return &TYPES[kind];
}

static const groove_EnumSchema *large_value_ref_schema(void) {
    static groove_EnumSchema SCHEMA;
    static bool init;
    if (!init) {
        struct { u8 tag; const char *name; u8 kind; } cases[3] = {
            {LARGE_VALUE_REF_BYTES_TAG, "Bytes", LVK_BYTES},
            {LARGE_VALUE_REF_STRING_TAG, "String", LVK_STRING},
            {LARGE_VALUE_REF_JSON_TAG, "Json", LVK_JSON},
        };
        groove_EnumCaseVec v = {0};
        for (u32 i = 0; i < 3; i++) {
            JZ_ASSERT(cases[i].tag == i, "large-value-ref case tags");
            JZ_VEC_PUSH(v, groove__records__values__EnumCase__new(jz_str_from(cases[i].name), large_value_ref_payload_descriptor(cases[i].kind)));
        }
        jz_err e = groove__records__values__EnumSchema__new(jz_str_from("groove.internal.large_value_ref"), v, &SCHEMA);
        if (e.code) jz_panic("fixed internal large-value-ref enum schema is valid");
        init = true;
    }
    return &SCHEMA;
}

static u8 large_value_kind_tag(u8 kind) {
    switch (kind) {
    case LVK_BYTES: return LARGE_VALUE_REF_BYTES_TAG;
    case LVK_STRING: return LARGE_VALUE_REF_STRING_TAG;
    default: return LARGE_VALUE_REF_JSON_TAG;
    }
}
static jz_err large_value_kind_from_tag(u8 tag, u8 *out) {
    switch (tag) {
    case LARGE_VALUE_REF_BYTES_TAG: *out = LVK_BYTES; return JZ_OK;
    case LARGE_VALUE_REF_STRING_TAG: *out = LVK_STRING; return JZ_OK;
    case LARGE_VALUE_REF_JSON_TAG: *out = LVK_JSON; return JZ_OK;
    default: return ERR(MalformedScalar);
    }
}

/* chunked_values(value) -> Vec<Value> */
static void chunked_values(const LVRef *value, groove_ValueVec *out) {
    u8 kind = value->kind;
    groove_ValueVec edits = {0};
    for (u32 i = 0; i < value->edit_tail.len; i++) {
        const groove_ReplaceEdit *edit_value = &value->edit_tail.ptr[i];
        IdValue fields[5] = {
            {EDIT_OFFSET_FIELD, v_u64(edit_value->offset)},
            {EDIT_DELETE_LENGTH_FIELD, v_u64(edit_value->delete_length)},
            {EDIT_INSERT_BYTES_FIELD, v_bytes(edit_value->insert_bytes.ptr, edit_value->insert_bytes.len)},
        };
        u32 n = 3;
        if (kind != LVK_BYTES) {
            fields[n++] = (IdValue){EDIT_UTF16_OFFSET_FIELD, v_u64(edit_value->utf16_offset)};
            fields[n++] = (IdValue){EDIT_DELETE_UTF16_LENGTH_FIELD, v_u64(edit_value->delete_utf16_length)};
        }
        groove_ValueVec ordered;
        jz_err e = durable_ordered_values(large_value_edit_schema(kind), fields, n, &ordered);
        if (e.code) jz_panic("all internal edit fields are present");
        jz_bytes encoded;
        e = groove__records__RecordDescriptor__create(large_value_edit_descriptor(kind), ordered.ptr, ordered.len, &encoded);
        groove__records__values__ValueVec__drop(&ordered);
        if (e.code) jz_panic("internal edit record");
        JZ_VEC_PUSH(edits, v_record(groove__records__OwnedRecord__new(encoded, large_value_edit_descriptor(kind))));
    }
    IdValue fields[6] = {
        {LARGE_VALUE_REF_FORMAT_VERSION_FIELD, v_u8(value->format_version)},
        {LARGE_VALUE_REF_LOGICAL_HASH_FIELD, v_bytes(value->logical_hash.b, 32)},
        {LARGE_VALUE_REF_ROOT_FIELD, groove__large_values__node_ref_value(&value->root)},
        {LARGE_VALUE_REF_BYTE_LENGTH_FIELD, v_u64(value->byte_length)},
        {LARGE_VALUE_REF_EDIT_TAIL_FIELD, v_array(edits)},
    };
    u32 n = 5;
    if (kind != LVK_BYTES) {
        if (!value->has_utf16_length) jz_panic("validated text descriptor has a UTF-16 length");
        fields[n++] = (IdValue){LARGE_VALUE_REF_UTF16_LENGTH_FIELD, v_u64(value->utf16_length)};
    }
    jz_err e = durable_ordered_values(large_value_ref_payload_schema(kind), fields, n, out);
    if (e.code) jz_panic("all internal large-value reference fields are present");
}

static jz_err decode_chunked_values(u8 kind, const groove_Value *values, u32 n, LVRef *out) {
    jz_btree fields;
    JZ_TRY(durable_decode_values(large_value_ref_payload_schema(kind), values, n, &fields));
    LVRef value;
    memset(&value, 0, sizeof value);
    groove_Value root;
    bool have_root = false;
    groove_Value edits;
    bool have_edits = false;
    jz_err e = take_u8(&fields, LARGE_VALUE_REF_FORMAT_VERSION_FIELD, &value.format_version);
    /* Select the joint descriptor/edit/scalar contract before decoding the nested root or tail. */
    LargeValueFormat format = LARGE_VALUE_FORMAT_V1;
    if (!e.code) e = format_from_version(value.format_version, &format);
    if (!e.code) e = take_raw32(&fields, LARGE_VALUE_REF_LOGICAL_HASH_FIELD, value.logical_hash.b);
    if (!e.code) { e = take_durable_large_value_field(&fields, LARGE_VALUE_REF_ROOT_FIELD, &root); have_root = !e.code; }
    if (!e.code) e = take_u64(&fields, LARGE_VALUE_REF_BYTE_LENGTH_FIELD, &value.byte_length);
    if (!e.code && kind != LVK_BYTES) {
        e = take_u64(&fields, LARGE_VALUE_REF_UTF16_LENGTH_FIELD, &value.utf16_length);
        value.has_utf16_length = !e.code;
    }
    if (!e.code) {
        e = take_durable_large_value_field(&fields, LARGE_VALUE_REF_EDIT_TAIL_FIELD, &edits);
        have_edits = !e.code;
        if (!e.code && edits.tag != GROOVE_V_ARRAY) e = ERR(MalformedScalar);
    }
    for (u32 i = 0; !e.code && i < edits.u.items.len; i++) {
        const groove_Value *ev = &edits.u.items.ptr[i];
        if (ev->tag != GROOVE_V_RECORD) { e = ERR(MalformedScalar); break; }
        groove_ValueVec evalues;
        e = groove__records__OwnedRecord__to_values(&ev->u.record, &evalues);
        if (e.code) { jz_err_drop(&e); e = ERR(MalformedScalar); break; }
        jz_btree efields;
        e = durable_decode_values(large_value_edit_schema(kind), evalues.ptr, evalues.len, &efields);
        groove__records__values__ValueVec__drop(&evalues);
        if (e.code) break;
        groove_ReplaceEdit edit = {0};
        e = take_bytes(&efields, EDIT_INSERT_BYTES_FIELD, &edit.insert_bytes);
        /* Byte edits store no UTF-16 coordinates; the inserted UTF-16 length is derived. */
        if (!e.code && kind != LVK_BYTES) {
            e = take_u64(&efields, EDIT_UTF16_OFFSET_FIELD, &edit.utf16_offset);
            if (!e.code) e = take_u64(&efields, EDIT_DELETE_UTF16_LENGTH_FIELD, &edit.delete_utf16_length);
        }
        if (!e.code && kind != LVK_BYTES) {
            if (!utf8_ok(edit.insert_bytes.ptr, edit.insert_bytes.len)) e = ERR(InvalidUtf8);
            else edit.insert_utf16_length = utf16_count(edit.insert_bytes.ptr, edit.insert_bytes.len);
        }
        if (!e.code) e = take_u64(&efields, EDIT_OFFSET_FIELD, &edit.offset);
        if (!e.code) e = take_u64(&efields, EDIT_DELETE_LENGTH_FIELD, &edit.delete_length);
        jz_btree_drop(&efields);
        if (e.code) { groove__large_values__ReplaceEdit__drop(&edit); break; }
        JZ_VEC_PUSH(value.edit_tail, edit);
    }
    value.kind = kind;
    if (!e.code) e = groove__large_values__node_ref_from_value(&root, &value.root);
    if (!e.code) e = format_validate_descriptor(format, &value);
    if (have_root) groove__records__values__Value__drop(&root);
    if (have_edits) groove__records__values__Value__drop(&edits);
    jz_btree_drop(&fields);
    if (e.code) { groove__large_values__LargeValueRef__drop(&value); return e; }
    *out = value;
    return JZ_OK;
}

/* ------------------------------------------------------------------ stored scalar codecs */
static jz_err encode_primitive_payload(u8 kind, const u8 *p, u32 n, jz_bytes *out) {
    JZ_TRY(validate_logical(kind, p, n));
    *out = groove__records__encode_variant_record(0, p, n);
    return JZ_OK;
}

static bool bytes_eq_slice(const jz_bytes *a, const u8 *p, u32 n) { return a->len == n && memcmp(a->ptr, p, n) == 0; }

/* Encode an enum value of `schema` through the ordinary single-field codec. */
static jz_err encode_enum_value(u32 tag, const groove_EnumSchema *schema, const groove_ValueType *type, const groove_ValueVec *values, u32 code, jz_bytes *out) {
    const groove_EnumCase *c;
    jz_err e = groove__records__values__EnumSchema__case(schema, tag, &c);
    if (e.code) { jz_err_drop(&e); return groove__large_values__Error__new(code); }
    groove_EnumValue ev;
    e = groove__records__values__EnumValue__create(tag, c->payload, values->ptr, values->len, &ev);
    if (e.code) { jz_err_drop(&e); return groove__large_values__Error__new(code); }
    groove_Value v = v_enum(ev);
    e = groove__records__encode_single_field_value(&v, type, out);
    groove__records__values__Value__drop(&v);
    if (e.code) { jz_err_drop(&e); return groove__large_values__Error__new(code); }
    return JZ_OK;
}

/* The physical scalar encoder shared by public writes and raw canonicality checks. */
static jz_err encode_stored_scalar_canonical(u8 kind, const groove_StoredScalar *value, jz_bytes *out) {
    const groove_EnumSchema *schema = stored_scalar_schema(kind);
    if (value->tag == GROOVE_STORED_PRIMITIVE) return encode_primitive_payload(kind, value->primitive.ptr, value->primitive.len, out);
    if (value->chunked.kind != kind) return ERR(DescriptorMismatch);
    JZ_TRY(validate_descriptor(&value->chunked));
    groove_ValueVec values;
    chunked_values(&value->chunked, &values);
    jz_err e = encode_enum_value(1, schema, stored_scalar_value_type(kind), &values, GROOVE_LV_E_MalformedScalar, out);
    groove__records__values__ValueVec__drop(&values);
    return e;
}

jz_err groove__large_values__encode_stored_scalar(groove_LargeValueKind kind, const groove_StoredScalar *value, jz_bytes *out) {
    return encode_stored_scalar_canonical(kind, value, out);
}

jz_err groove__large_values__encode_primitive_stored_scalar_into(groove_LargeValueKind kind, const u8 *p, u32 n, jz_bytes *output) {
    JZ_TRY(validate_logical(kind, p, n));
    groove__records__append_variant_record(output, 0, p, n);
    return JZ_OK;
}

static jz_err decode_primitive_payload(u8 kind, const u8 *payload, u32 n, jz_bytes *out) {
    JZ_TRY(validate_logical(kind, payload, n));
    *out = jz_bytes_from(payload, n);
    return JZ_OK;
}

/* Read precisely the V1-independent selector: the payload's first byte. */
static jz_err preflight_descriptor_format(const u8 *payload, u32 n, LargeValueFormat *out) {
    if (n == 0) return ERR(MalformedScalar);
    return format_from_version(payload[0], out);
}

jz_err groove__large_values__decode_stored_scalar(groove_LargeValueKind kind, const u8 *encoded, u32 n, groove_StoredScalar *out) {
    u32 tag;
    jz_slice payload;
    jz_err e = groove__records__split_variant_record(encoded, n, &tag, &payload);
    if (e.code) { jz_err_drop(&e); return ERR(MalformedScalar); }
    groove_StoredScalar decoded;
    memset(&decoded, 0, sizeof decoded);
    switch (tag) {
    case 0:
        /* The primitive arm is one raw field; logical validation covers the payload. */
        decoded.tag = GROOVE_STORED_PRIMITIVE;
        JZ_TRY(decode_primitive_payload(kind, payload.ptr, payload.len, &decoded.primitive));
        *out = decoded;
        return JZ_OK;
    case 1: {
        /* An unknown format must win before the V1 ValueTypes inspect the payload. */
        LargeValueFormat format;
        JZ_TRY(preflight_descriptor_format(payload.ptr, payload.len, &format));
        decoded.tag = GROOVE_STORED_CHUNKED;
        JZ_TRY(format_decode_descriptor_payload(format, kind, payload.ptr, payload.len, &decoded.chunked));
        break;
    }
    default:
        return ERR(MalformedScalar);
    }
    jz_bytes canonical;
    e = encode_stored_scalar_canonical(kind, &decoded, &canonical);
    if (!e.code) {
        if (!bytes_eq_slice(&canonical, encoded, n)) e = ERR(MalformedScalar);
        jz_bytes_drop(&canonical);
    }
    if (e.code) { groove__large_values__StoredScalar__drop(&decoded); return e; }
    *out = decoded;
    return JZ_OK;
}

jz_err groove__large_values__inline_scalar_bytes(groove_LargeValueKind kind, const u8 *encoded, u32 n, jz_slice *out) {
    u32 tag;
    jz_slice payload;
    jz_err e = groove__records__split_variant_record(encoded, n, &tag, &payload);
    if (e.code) { jz_err_drop(&e); return ERR(MalformedScalar); }
    switch (tag) {
    case 0:
        /* Primitive has exactly one field, slot 1: its canonical record is the payload itself. */
        e = validate_logical(kind, payload.ptr, payload.len);
        if (e.code) { jz_err_drop(&e); return ERR(MalformedScalar); }
        *out = payload;
        return JZ_OK;
    case 1: {
        /* Validate the complete descriptor before reporting that materialization is needed. */
        groove_StoredScalar s;
        JZ_TRY(groove__large_values__decode_stored_scalar(kind, encoded, n, &s));
        groove__large_values__StoredScalar__drop(&s);
        return ERR(RequiresEvaluation);
    }
    default:
        return ERR(MalformedScalar);
    }
}

/* ------------------------------------------------------------------ large value ref codec */
jz_err groove__large_values__encode_large_value_ref(const LVRef *value, jz_bytes *out) {
    JZ_TRY(validate_descriptor(value));
    u32 tag = large_value_kind_tag(value->kind);
    const groove_EnumSchema *schema = large_value_ref_schema();
    groove_ValueVec values;
    chunked_values(value, &values);
    groove_ValueType type = vt_enum(schema);
    jz_err e = encode_enum_value(tag, schema, &type, &values, GROOVE_LV_E_MalformedScalar, out);
    groove__records__values__ValueType__drop(&type);
    groove__records__values__ValueVec__drop(&values);
    return e;
}

jz_err groove__large_values__decode_large_value_ref(const u8 *encoded, u32 n, LVRef *out) {
    u32 tag;
    jz_slice payload;
    jz_err e = groove__records__split_variant_record(encoded, n, &tag, &payload);
    if (e.code) { jz_err_drop(&e); return ERR(MalformedScalar); }
    if (tag > 255) return ERR(MalformedScalar);
    u8 kind;
    JZ_TRY(large_value_kind_from_tag((u8)tag, &kind));
    /* No V1 nested layout is decoded until the outer descriptor selected a format. */
    LargeValueFormat format;
    JZ_TRY(preflight_descriptor_format(payload.ptr, payload.len, &format));
    LVRef decoded;
    JZ_TRY(format_decode_descriptor_payload(format, kind, payload.ptr, payload.len, &decoded));
    jz_bytes canonical;
    e = groove__large_values__encode_large_value_ref(&decoded, &canonical);
    if (!e.code) {
        if (!bytes_eq_slice(&canonical, encoded, n)) e = ERR(MalformedScalar);
        jz_bytes_drop(&canonical);
    }
    if (e.code) { groove__large_values__LargeValueRef__drop(&decoded); return e; }
    *out = decoded;
    return JZ_OK;
}

static jz_err decode_descriptor_payload_v1(u8 kind, const u8 *payload, u32 n, LVRef *out) {
    const DurableLargeValueRecordSchema *schema = large_value_ref_payload_schema(kind);
    groove_BorrowedRecord bound = groove__records__RecordDescriptor__bind(schema->descriptor, payload, n);
    groove_ValueVec values;
    jz_err e = groove__records__BorrowedRecord__to_values(&bound, &values);
    if (e.code) { jz_err_drop(&e); return ERR(MalformedScalar); }
    jz_bytes canonical;
    e = groove__records__RecordDescriptor__create(schema->descriptor, values.ptr, values.len, &canonical);
    if (e.code) { jz_err_drop(&e); groove__records__values__ValueVec__drop(&values); return ERR(MalformedScalar); }
    bool same = bytes_eq_slice(&canonical, payload, n);
    jz_bytes_drop(&canonical);
    if (!same) { groove__records__values__ValueVec__drop(&values); return ERR(MalformedScalar); }
    e = decode_chunked_values(kind, values.ptr, values.len, out);
    groove__records__values__ValueVec__drop(&values);
    return e;
}

/* ------------------------------------------------------------------ descriptor validation */
static jz_err validate_descriptor(const LVRef *value) {
    LargeValueFormat f;
    JZ_TRY(format_from_version(value->format_version, &f));
    return format_validate_descriptor(f, value);
}

static jz_err validate_descriptor_tail_bounds(const LVRef *value) {
    if (value->edit_tail.len > MAX_EDIT_COUNT) return ERR(MalformedScalar);
    u64 tail_bytes = 0;
    for (u32 i = 0; i < value->edit_tail.len; i++) {
        if (!checked_add_u64(tail_bytes, value->edit_tail.ptr[i].insert_bytes.len, &tail_bytes) || !checked_add_u64(tail_bytes, 24, &tail_bytes))
            return ERR(MetricOverflow);
        if (tail_bytes > MAX_EDIT_TAIL_BYTES) return ERR(MalformedScalar);
    }
    return JZ_OK;
}

static jz_err validate_descriptor_v1(const LVRef *value) {
    JZ_TRY(validate_descriptor_shape_v1(value));
    return validate_descriptor_tail_bounds(value);
}

static jz_err validate_descriptor_shape(const LVRef *value) {
    LargeValueFormat f;
    JZ_TRY(format_from_version(value->format_version, &f));
    return format_validate_descriptor_shape(f, value);
}

static jz_err validate_descriptor_shape_v1(const LVRef *value) {
    u64 byte_length = value->byte_length;
    bool has_utf16 = value->has_utf16_length;
    u64 utf16_length = value->utf16_length;
    for (u32 i = value->edit_tail.len; i > 0; i--) {
        const groove_ReplaceEdit *edit = &value->edit_tail.ptr[i - 1];
        u64 inserted = edit->insert_bytes.len;
        u64 end;
        if (!checked_add_u64(edit->offset, inserted, &end)) return ERR(MetricOverflow);
        if (end > byte_length) return ERR(MalformedScalar);
        if (!checked_sub_u64(byte_length, inserted, &byte_length) || !checked_add_u64(byte_length, edit->delete_length, &byte_length))
            return ERR(MetricOverflow);
        if (value->kind == LVK_BYTES) {
            if (edit->utf16_offset != 0 || edit->delete_utf16_length != 0 || edit->insert_utf16_length != 0) return ERR(MalformedScalar);
        } else {
            if (!utf8_ok(edit->insert_bytes.ptr, edit->insert_bytes.len)) return ERR(InvalidUtf8);
            u64 actual_insert = utf16_count(edit->insert_bytes.ptr, edit->insert_bytes.len);
            if (actual_insert != edit->insert_utf16_length) return ERR(MalformedScalar);
            if (!has_utf16) return ERR(MalformedScalar);
            u64 current = utf16_length;
            u64 e16;
            if (!checked_add_u64(edit->utf16_offset, edit->insert_utf16_length, &e16)) return ERR(MetricOverflow);
            if (e16 > current) return ERR(MalformedScalar);
            if (!checked_sub_u64(current, edit->insert_utf16_length, &utf16_length) || !checked_add_u64(utf16_length, edit->delete_utf16_length, &utf16_length))
                return ERR(MetricOverflow);
            has_utf16 = true;
        }
    }
    if (value->kind == LVK_BYTES && value->has_utf16_length) return ERR(MalformedScalar);
    if (value->kind != LVK_BYTES && !value->has_utf16_length) return ERR(MalformedScalar);
    return JZ_OK;
}

/* Recover the tail-free descriptor of `value`'s immutable base tree. */
static jz_err tail_base_descriptor(const LVRef *value, LVRef *out) {
    JZ_TRY(validate_descriptor(value));
    LVRef replay;
    groove__large_values__LargeValueRef__Clone__clone(&replay, value);
    jz_err e = JZ_OK;
    for (u32 i = value->edit_tail.len; i > 0 && !e.code; i--) {
        const groove_ReplaceEdit *edit = &value->edit_tail.ptr[i - 1];
        if (!checked_sub_u64(replay.byte_length, edit->insert_bytes.len, &replay.byte_length) ||
            !checked_add_u64(replay.byte_length, edit->delete_length, &replay.byte_length)) {
            e = ERR(MetricOverflow);
            break;
        }
        if (replay.kind == LVK_BYTES) {
            replay.has_utf16_length = false;
            replay.utf16_length = 0;
        } else {
            if (!replay.has_utf16_length) { e = ERR(MalformedScalar); break; }
            if (!checked_sub_u64(replay.utf16_length, edit->insert_utf16_length, &replay.utf16_length) ||
                !checked_add_u64(replay.utf16_length, edit->delete_utf16_length, &replay.utf16_length)) {
                e = ERR(MetricOverflow);
                break;
            }
        }
    }
    jz_vec_truncate(&replay.edit_tail, 0, sizeof(groove_ReplaceEdit), replace_edit_drop_v);
    if (!e.code) e = validate_descriptor(&replay);
    if (e.code) { groove__large_values__LargeValueRef__drop(&replay); return e; }
    *out = replay;
    return JZ_OK;
}

/* Reconstruct and replay an untrusted descriptor's tail against its base tree. */
jz_err groove__large_values__validate_edit_tail_attempt(const LVRef *value, groove_EvaluationInputs *inputs) {
    LVRef replay;
    JZ_TRY(tail_base_descriptor(value, &replay));
    jz_err e = JZ_OK;
    for (u32 i = 0; i < value->edit_tail.len; i++) {
        const groove_ReplaceEdit *expected = &value->edit_tail.ptr[i];
        groove_TailOutcome outcome;
        e = replace_tail_with_bounds_attempt(&replay, expected->offset, expected->delete_length, jz_bytes_clone(&expected->insert_bytes), inputs, false, &outcome);
        if (e.code) break;
        LVRef next = outcome.value; /* Updated(next) | ConsolidationRequired(next) */
        bool last_matches = next.edit_tail.len > 0 && groove__large_values__ReplaceEdit__PartialEq__eq(&next.edit_tail.ptr[next.edit_tail.len - 1], expected);
        if (!last_matches) { groove__large_values__LargeValueRef__drop(&next); e = ERR(DescriptorMismatch); break; }
        groove__large_values__LargeValueRef__drop(&replay);
        replay = next;
    }
    if (!e.code && !groove__large_values__LargeValueRef__PartialEq__eq(&replay, value)) e = ERR(DescriptorMismatch);
    groove__large_values__LargeValueRef__drop(&replay);
    return e;
}

/* ------------------------------------------------------------------ TreeStager */
static void tree_stager_new(TreeStager *s, u8 kind, LocateFn locate, void *ctx) {
    s->kind = kind;
    s->locate = locate;
    s->locate_ctx = ctx;
    memset(&s->staged, 0, sizeof s->staged);
}
static void tree_stager_drop(TreeStager *s) { jz_vec_drop(&s->staged, sizeof(groove_StagedChunk), staged_chunk_drop_v); }

static jz_err validate_untyped_node_structure(const groove_ChunkNode *node);

/* consumes node */
static jz_err tree_stager_node(TreeStager *s, groove_ChunkNode *node, BuiltNode *out) {
    jz_err e = validate_untyped_node_structure(node);
    Metrics m;
    if (!e.code) e = node_metrics(s->kind, node, &m);
    if (!e.code) {
        bool has_chunk;
        groove_StagedChunk chunk;
        e = seal_node(node, m, s->locate, s->locate_ctx, out, &has_chunk, &chunk);
        if (!e.code && has_chunk) JZ_VEC_PUSH(s->staged, chunk);
    }
    groove__large_values__ChunkNode__drop(node);
    return e;
}

static jz_err tree_stager_leaves(TreeStager *s, const u8 *p, u32 n, BuiltNodeVec *out) {
    RangeVec ranges;
    JZ_TRY(leaf_ranges(s->kind, p, n, &ranges));
    BuiltNodeVec built = {0};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < ranges.len && !e.code; i++) {
        groove_ChunkNode leaf = leaf_node(s->kind, jz_bytes_from(p + ranges.ptr[i].start, ranges.ptr[i].end - ranges.ptr[i].start));
        BuiltNode b;
        e = tree_stager_node(s, &leaf, &b);
        if (!e.code) JZ_VEC_PUSH(built, b);
    }
    jz_vec_drop(&ranges, sizeof(Range), NULL);
    if (e.code) { jz_vec_drop(&built, sizeof(BuiltNode), NULL); return e; }
    *out = built;
    return JZ_OK;
}

static jz_err branch_ranges(u8 kind, const BuiltNode *level, u32 n, RangeVec *out);

static jz_err tree_stager_branches(TreeStager *s, const BuiltNode *level, u32 n, BuiltNodeVec *out) {
    RangeVec ranges;
    JZ_TRY(branch_ranges(s->kind, level, n, &ranges));
    BuiltNodeVec built = {0};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < ranges.len && !e.code; i++) {
        groove_ChunkNode node;
        memset(&node, 0, sizeof node);
        node.tag = GROOVE_CHUNK_NODE_BRANCH;
        node.format = FORMAT_VERSION;
        node.kind = s->kind;
        for (u32 k = ranges.ptr[i].start; k < ranges.ptr[i].end; k++) JZ_VEC_PUSH(node.children, built_to_child(&level[k]));
        BuiltNode b;
        e = tree_stager_node(s, &node, &b);
        if (!e.code) JZ_VEC_PUSH(built, b);
    }
    jz_vec_drop(&ranges, sizeof(Range), NULL);
    if (e.code) { jz_vec_drop(&built, sizeof(BuiltNode), NULL); return e; }
    *out = built;
    return JZ_OK;
}

/* Stage parent levels over `level`, built at `depth`, until one root remains;
 * a tree deeper than MAX_TREE_DEPTH fails with `too_deep`. Consumes level. */
static jz_err tree_stager_root(TreeStager *s, BuiltNodeVec level, u32 depth, u32 too_deep, BuiltNode *out) {
    while (level.len > 1) {
        depth += 1;
        if (depth > MAX_TREE_DEPTH) { jz_vec_drop(&level, sizeof(BuiltNode), NULL); return groove__large_values__Error__new(too_deep); }
        BuiltNodeVec next;
        jz_err e = tree_stager_branches(s, level.ptr, level.len, &next);
        jz_vec_drop(&level, sizeof(BuiltNode), NULL);
        JZ_TRY(e);
        level = next;
    }
    if (level.len == 0) { jz_vec_drop(&level, sizeof(BuiltNode), NULL); return ERR(MalformedNode); }
    *out = level.ptr[level.len - 1];
    jz_vec_drop(&level, sizeof(BuiltNode), NULL);
    return JZ_OK;
}

static groove_ChunkNode leaf_node(u8 kind, jz_bytes bytes) {
    groove_ChunkNode n;
    memset(&n, 0, sizeof n);
    n.tag = GROOVE_CHUNK_NODE_LEAF;
    n.format = FORMAT_VERSION;
    n.kind = kind;
    n.bytes = bytes;
    return n;
}

/* ------------------------------------------------------------------ authenticated node reads */
static jz_err load_authenticated_node_attempt(u8 format_version, u8 kind, const groove_NodeRef *node_ref, groove_ContentHash expected_logical_hash, groove_EvaluationInputs *inputs, groove_ChunkNode *out) {
    groove_ChunkRequest request;
    memcpy(request.object_hash, node_ref->object_hash.b, 32);
    request.locator = node_ref->locator;
    const groove_LoadedChunk *chunk;
    JZ_TRY(groove__ivm__runtime__evaluation_session__EvaluationInputs__loaded_chunk(inputs, request, &chunk));
    const jz_bytes *bytes = groove__ivm__runtime__evaluation_session__LoadedChunk__bytes(chunk);
    if (groove__ivm__runtime__evaluation_session__LoadedChunk__verifies_object_hash(chunk, node_ref->object_hash.b))
        JZ_TRY(decode_object_verified_node_for_format(format_version, kind, bytes->ptr, bytes->len, out));
    else
        JZ_TRY(decode_node_for_format(format_version, kind, node_ref->object_hash, bytes->ptr, bytes->len, out));
    groove_ContentHash h = node_logical_hash(out);
    if (!hash_eq(&h, &expected_logical_hash)) {
        groove__large_values__ChunkNode__drop(out);
        return ERR(DescriptorMismatch);
    }
    return JZ_OK;
}

/* Descend from `node_ref` to one leaf, letting `choose` pick a child of each
 * branch and recording the branches on `path`. Consumes path. */
static jz_err descend_attempt(const LVRef *value, groove_NodeRef node_ref, groove_ContentHash expected_hash, PathFrameVec path, u64 max_depth, groove_EvaluationInputs *inputs, ChooseFn choose, void *choose_ctx, LocatedLeaf *out) {
    for (;;) {
        if (path.len > max_depth) { path_drop(&path); return ERR(InvalidTree); }
        groove_ChunkNode node;
        jz_err e = load_authenticated_node_attempt(value->format_version, value->kind, &node_ref, expected_hash, inputs, &node);
        if (e.code) { path_drop(&path); return e; }
        if (node.tag == GROOVE_CHUNK_NODE_LEAF) {
            out->node_ref = node_ref;
            out->bytes = node.bytes;
            out->start = 0;
            out->path = path;
            jz_vec_drop(&node.children, sizeof(groove_BranchChild), NULL);
            return JZ_OK;
        }
        u32 selected;
        e = choose(choose_ctx, node.children.ptr, node.children.len, &selected);
        if (e.code) { groove__large_values__ChunkNode__drop(&node); path_drop(&path); return e; }
        const groove_BranchChild *child = &node.children.ptr[selected];
        groove_NodeRef parent = node_ref;
        node_ref = child->node_ref;
        expected_hash = child->logical_hash;
        PathFrame frame = {parent, node.children, selected};
        jz_bytes_drop(&node.bytes);
        JZ_VEC_PUSH(path, frame);
    }
}

typedef struct LocateCtx { u64 offset; u64 start; } LocateCtx;
static jz_err choose_by_offset(void *ctx, const groove_BranchChild *children, u32 n, u32 *selected) {
    LocateCtx *c = ctx;
    for (u32 i = 0; i < n; i++) {
        u64 end;
        if (!checked_add_u64(c->start, children[i].metrics.byte_length, &end)) end = UINT64_MAX; /* saturating_add */
        if (c->offset < end) { *selected = i; return JZ_OK; }
        c->start = end;
    }
    return ERR(DescriptorMismatch);
}

static jz_err locate_leaf_attempt(const LVRef *value, u64 offset, u64 base_len, groove_EvaluationInputs *inputs, LocatedLeaf *out) {
    if (offset >= base_len) return ERR(MalformedScalar);
    LocateCtx ctx = {offset, 0};
    PathFrameVec empty = {0};
    JZ_TRY(descend_attempt(value, value->root, value->logical_hash, empty, MAX_TREE_DEPTH, inputs, choose_by_offset, &ctx, out));
    if (offset >= ctx.start + out->bytes.len) { located_leaf_drop(out); return ERR(DescriptorMismatch); }
    out->start = ctx.start;
    return JZ_OK;
}

static jz_err choose_first(void *ctx, const groove_BranchChild *children, u32 n, u32 *selected) {
    (void)ctx; (void)children;
    if (n == 0) return ERR(MalformedNode);
    *selected = 0;
    return JZ_OK;
}

static jz_err next_leaf_attempt(const LVRef *value, const LocatedLeaf *current, groove_EvaluationInputs *inputs, LocatedLeaf *out) {
    PathFrameVec path;
    path_clone(&path, &current->path);
    groove_NodeRef child_ref;
    groove_ContentHash child_hash;
    for (;;) {
        if (!path.len) { path_drop(&path); return ERR(DescriptorMismatch); }
        PathFrame *frame = &path.ptr[path.len - 1];
        if (frame->selected + 1 < frame->children.len) {
            frame->selected += 1;
            child_ref = frame->children.ptr[frame->selected].node_ref;
            child_hash = frame->children.ptr[frame->selected].logical_hash;
            break;
        }
        path_frame_drop_v(frame);
        path.len -= 1;
    }
    u64 start;
    if (!checked_add_u64(current->start, current->bytes.len, &start)) { path_drop(&path); return ERR(MetricOverflow); }
    JZ_TRY(descend_attempt(value, child_ref, child_hash, path, UINT64_MAX, inputs, choose_first, NULL, out));
    out->start = start;
    return JZ_OK;
}

/* ------------------------------------------------------------------ node codecs */
jz_err groove__large_values__decode_node(u8 kind, groove_ContentHash expected_hash, const u8 *p, u32 n, groove_ChunkNode *out) {
    return decode_node_for_format(FORMAT_VERSION, kind, expected_hash, p, n, out);
}

static bool hash_matches(const u8 *p, u32 n, const groove_ContentHash *expected) {
    groove_ContentHash h = groove__large_values__object_hash(p, n);
    return hash_eq(&h, expected);
}

/* Decode a node through the exact format selected by its owner descriptor. */
static jz_err decode_node_for_format(u8 format_version, u8 kind, groove_ContentHash expected_hash, const u8 *p, u32 n, groove_ChunkNode *out) {
    if (n > MAX_ENCODED_NODE_BYTES) return ERR(MalformedNode);
    if (!hash_matches(p, n, &expected_hash)) return ERR(ObjectHashMismatch);
    return decode_object_verified_node_for_format(format_version, kind, p, n, out);
}

static jz_err preflight_node_format(const u8 *encoded, u32 n, LargeValueFormat *out);

/* The bytes have already passed object-hash verification. */
static jz_err decode_object_verified_node_for_format(u8 format_version, u8 kind, const u8 *p, u32 n, groove_ChunkNode *out) {
    if (n > MAX_ENCODED_NODE_BYTES) return ERR(MalformedNode);
    /* Select both contracts before V1 binds any variable node field. */
    LargeValueFormat descriptor_format, nf;
    JZ_TRY(format_from_version(format_version, &descriptor_format));
    JZ_TRY(preflight_node_format(p, n, &nf));
    if (descriptor_format != nf) return ERR(DescriptorMismatch);
    JZ_TRY(format_decode_node(descriptor_format, p, n, out));
    if (out->kind != kind) {
        groove__large_values__ChunkNode__drop(out);
        return ERR(DescriptorMismatch);
    }
    return JZ_OK;
}

jz_err groove__large_values__decode_node_untyped_authenticated(groove_ContentHash expected_hash, const u8 *p, u32 n, groove_ChunkNode *out) {
    /* Keep the resource ceiling ahead of authentication. */
    if (n > MAX_ENCODED_NODE_BYTES) return ERR(MalformedNode);
    if (!hash_matches(p, n, &expected_hash)) return ERR(ObjectHashMismatch);
    LargeValueFormat f;
    JZ_TRY(preflight_node_format(p, n, &f));
    return format_decode_node(f, p, n, out);
}

jz_err groove__large_values__decode_authenticated_node(groove_ContentHash expected_hash, const u8 *p, u32 n, groove_ChunkNode *out) {
    return groove__large_values__decode_node_untyped_authenticated(expected_hash, p, n, out);
}

/* Encode a chunk node using Groove's ordinary canonical enum/record algebra. */
jz_err groove__large_values__encode_node(const groove_ChunkNode *node, jz_bytes *out) {
    LargeValueFormat f;
    JZ_TRY(format_from_version(node_format(node), &f));
    return format_encode_node(f, node, out);
}

static groove_RecordDescriptor chunk_node_child_schema(u8 kind) {
    const char *names[5] = {"object_hash", "locator", "byte_length", "logical_hash", "utf16_length"};
    groove_ValueType types[5] = {VT_RAW_BYTES(), VT_RAW_BYTES(), VT_SCALAR(GROOVE_VT_U64), VT_RAW_BYTES(), VT_SCALAR(GROOVE_VT_U64)};
    return groove__records__RecordDescriptor__new_cstr(names, types, kind != LVK_BYTES ? 5 : 4);
}

static void chunk_node_schema(u8 kind, groove_EnumSchema *out) {
    const char *leaf_names[3] = {"format", "kind", "bytes"};
    groove_ValueType leaf_types[3] = {VT_SCALAR(GROOVE_VT_U8), VT_SCALAR(GROOVE_VT_U8), VT_RAW_BYTES()};
    const char *branch_names[3] = {"format", "kind", "children"};
    groove_ValueType branch_types[3] = {VT_SCALAR(GROOVE_VT_U8), VT_SCALAR(GROOVE_VT_U8),
                                        groove__records__values__ValueType__array_of(vt_record(chunk_node_child_schema(kind)))};
    groove_EnumCaseVec cases = {0};
    JZ_VEC_PUSH(cases, groove__records__values__EnumCase__new(jz_str_from("Leaf"), groove__records__RecordDescriptor__new_cstr(leaf_names, leaf_types, 3)));
    JZ_VEC_PUSH(cases, groove__records__values__EnumCase__new(jz_str_from("Branch"), groove__records__RecordDescriptor__new_cstr(branch_names, branch_types, 3)));
    jz_err e = groove__records__values__EnumSchema__new(jz_str_from("groove.internal.large_value.chunk_node"), cases, out);
    if (e.code) jz_panic("fixed chunk-node enum schema is valid");
}

/* The frozen v1 node codec. */
static jz_err encode_node_v1(const groove_ChunkNode *node, jz_bytes *out) {
    groove_EnumSchema schema;
    chunk_node_schema(node_kind(node), &schema);
    groove_ValueVec fields = {0};
    jz_err e = JZ_OK;
    u32 tag;
    if (node->tag == GROOVE_CHUNK_NODE_LEAF) {
        tag = 0;
        JZ_VEC_PUSH(fields, v_u8(node->format));
        JZ_VEC_PUSH(fields, v_u8(large_value_kind_tag(node->kind)));
        JZ_VEC_PUSH(fields, v_bytes(node->bytes.ptr, node->bytes.len));
    } else {
        tag = 1;
        groove_RecordDescriptor child_schema = chunk_node_child_schema(node->kind);
        groove_ValueVec children = {0};
        for (u32 i = 0; i < node->children.len && !e.code; i++) {
            const groove_BranchChild *child = &node->children.ptr[i];
            groove_Value cf[5] = {
                v_bytes(child->node_ref.object_hash.b, 32), v_bytes(child->node_ref.locator.b, 32),
                v_u64(child->metrics.byte_length), v_bytes(child->logical_hash.b, 32),
            };
            u32 n = 4;
            if (node->kind == LVK_BYTES && !child->metrics.has_utf16) {
            } else if (node->kind != LVK_BYTES && child->metrics.has_utf16) {
                cf[n++] = v_u64(child->metrics.utf16_length);
            } else {
                e = ERR(MalformedNode);
            }
            if (!e.code) {
                jz_bytes bytes;
                jz_err ce = groove__records__RecordDescriptor__create(child_schema, cf, n, &bytes);
                if (ce.code) { jz_err_drop(&ce); e = ERR(MalformedNode); }
                else JZ_VEC_PUSH(children, v_record(groove__records__OwnedRecord__new(bytes, child_schema)));
            }
            for (u32 k = 0; k < n; k++) groove__records__values__Value__drop(&cf[k]);
        }
        JZ_VEC_PUSH(fields, v_u8(node->format));
        JZ_VEC_PUSH(fields, v_u8(large_value_kind_tag(node->kind)));
        JZ_VEC_PUSH(fields, v_array(children));
    }
    if (!e.code) {
        groove_ValueType type = vt_enum(&schema);
        e = encode_enum_value(tag, &schema, &type, &fields, GROOVE_LV_E_MalformedNode, out);
        groove__records__values__ValueType__drop(&type);
    }
    groove__records__values__ValueVec__drop(&fields);
    groove__records__values__EnumSchema__drop(&schema);
    return e;
}

static jz_err preflight_node_bounds(const u8 *encoded, u32 n, const groove_EnumSchema *schema) {
    u32 tag;
    jz_slice payload;
    jz_err e = groove__records__split_variant_record(encoded, n, &tag, &payload);
    if (e.code) { jz_err_drop(&e); return ERR(MalformedNode); }
    if (tag == 1) {
        const groove_EnumCase *c;
        e = groove__records__values__EnumSchema__case(schema, 1, &c);
        if (e.code) { jz_err_drop(&e); return ERR(MalformedNode); }
        groove_Span span;
        e = groove__records__RecordDescriptor__field_span(c->payload, payload.ptr, payload.len, 2, &span);
        if (e.code) { jz_err_drop(&e); return ERR(MalformedNode); }
        if (span.end - span.start < 4) return ERR(MalformedNode);
        const u8 *array = payload.ptr + span.start;
        u32 count = (u32)array[0] | ((u32)array[1] << 8) | ((u32)array[2] << 16) | ((u32)array[3] << 24);
        if (count > BRANCH_MAX_CHILDREN) return ERR(MalformedNode);
    }
    return JZ_OK;
}

/* Read precisely the self-describing node selector. */
static jz_err preflight_node_format(const u8 *encoded, u32 n, LargeValueFormat *out) {
    u32 tag;
    jz_slice payload;
    jz_err e = groove__records__split_variant_record(encoded, n, &tag, &payload);
    if (e.code) { jz_err_drop(&e); return ERR(MalformedNode); }
    if (tag != 0 && tag != 1) return ERR(MalformedNode);
    if (payload.len == 0) return ERR(MalformedNode);
    return format_from_version(payload.ptr[0], out);
}

static jz_err kind_from_tag_node(u8 tag, u8 *out) {
    jz_err e = large_value_kind_from_tag(tag, out);
    if (e.code) { jz_err_drop(&e); return ERR(MalformedNode); }
    return JZ_OK;
}
static jz_err raw_bytes_node(const groove_Value *v, u8 out[32]) {
    jz_err e = raw_bytes(v, out);
    if (e.code) { jz_err_drop(&e); return ERR(MalformedNode); }
    return JZ_OK;
}

/* Decode the authenticated chunk payload representation. Exact canonical
 * re-encoding rejects any alternate or trailing representation. */
static jz_err decode_canonical_node_v1(const u8 *encoded, u32 n, groove_ChunkNode *out) {
    if (n > MAX_ENCODED_NODE_BYTES) return ERR(MalformedNode);
    u32 tag;
    jz_slice payload;
    jz_err e = groove__records__split_variant_record(encoded, n, &tag, &payload);
    if (e.code) { jz_err_drop(&e); return ERR(MalformedNode); }
    if (tag == 0) {
        if (payload.len < 2) return ERR(MalformedNode);
        u8 format = payload.ptr[0];
        u8 kind;
        JZ_TRY(kind_from_tag_node(payload.ptr[1], &kind));
        u32 blen = payload.len - 2;
        if (blen > LEAF_MAX_BYTES) return ERR(MalformedNode);
        /* The canonical re-encoding is this header followed by the unchanged payload. */
        u8 canonical_header[3] = {0, format, large_value_kind_tag(kind)};
        if (n < 3 || memcmp(encoded, canonical_header, 3) != 0 || n - 3 != blen || memcmp(encoded + 3, payload.ptr + 2, blen) != 0)
            return ERR(MalformedNode);
        groove_ChunkNode node = leaf_node(kind, jz_bytes_from(payload.ptr + 2, blen));
        node.format = format;
        e = validate_untyped_node_structure(&node);
        if (e.code) { groove__large_values__ChunkNode__drop(&node); return e; }
        *out = node;
        return JZ_OK;
    }
    /* The fixed kind byte selects the child layout before binding any children. */
    if (payload.len < 2) return ERR(MalformedNode);
    u8 kind;
    JZ_TRY(kind_from_tag_node(payload.ptr[1], &kind));
    groove_EnumSchema schema;
    chunk_node_schema(kind, &schema);
    e = preflight_node_bounds(encoded, n, &schema);
    if (e.code) { groove__records__values__EnumSchema__drop(&schema); return e; }
    groove_ValueType type;
    memset(&type, 0, sizeof type);
    type.tag = GROOVE_VT_ENUM;
    type.u.enum_ = JZ_NEW(groove_EnumSchema);
    *type.u.enum_ = schema; /* Box::new(schema) */
    groove_Value value;
    e = groove__records__decode_single_field_value(encoded, n, &type, &value);
    groove__records__values__ValueType__drop(&type);
    if (e.code) { jz_err_drop(&e); return ERR(MalformedNode); }
    if (value.tag != GROOVE_V_ENUM) { groove__records__values__Value__drop(&value); return ERR(MalformedNode); }
    groove_ValueVec fields;
    e = groove__records__OwnedRecord__to_values(&value.u.enum_.record, &fields);
    if (e.code) { jz_err_drop(&e); groove__records__values__Value__drop(&value); return ERR(MalformedNode); }
    groove_ChunkNode node;
    memset(&node, 0, sizeof node);
    u32 vtag = value.u.enum_.tag;
    e = JZ_OK;
    if (vtag == 0 && fields.len == 3 && fields.ptr[0].tag == GROOVE_V_U8 && fields.ptr[1].tag == GROOVE_V_U8 && fields.ptr[2].tag == GROOVE_V_BYTES) {
        node.tag = GROOVE_CHUNK_NODE_LEAF;
        node.format = fields.ptr[0].u.u8_;
        e = kind_from_tag_node(fields.ptr[1].u.u8_, &node.kind);
        if (!e.code) node.bytes = jz_bytes_clone(&fields.ptr[2].u.bytes);
    } else if (vtag == 1 && fields.len == 3 && fields.ptr[0].tag == GROOVE_V_U8 && fields.ptr[1].tag == GROOVE_V_U8 && fields.ptr[2].tag == GROOVE_V_ARRAY) {
        node.tag = GROOVE_CHUNK_NODE_BRANCH;
        node.format = fields.ptr[0].u.u8_;
        node.kind = kind;
        const groove_ValueVec *children = &fields.ptr[2].u.items;
        for (u32 i = 0; i < children->len && !e.code; i++) {
            const groove_Value *child = &children->ptr[i];
            if (child->tag != GROOVE_V_RECORD) { e = ERR(MalformedNode); break; }
            groove_ValueVec cf;
            e = groove__records__OwnedRecord__to_values(&child->u.record, &cf);
            if (e.code) { jz_err_drop(&e); e = ERR(MalformedNode); break; }
            groove_BranchChild bc;
            memset(&bc, 0, sizeof bc);
            bool shape_ok;
            if (kind == LVK_BYTES) {
                shape_ok = cf.len == 4 && cf.ptr[2].tag == GROOVE_V_U64;
            } else {
                shape_ok = cf.len == 5 && cf.ptr[2].tag == GROOVE_V_U64 && cf.ptr[4].tag == GROOVE_V_U64;
                if (shape_ok) { bc.metrics.has_utf16 = true; bc.metrics.utf16_length = cf.ptr[4].u.u64_; }
            }
            if (!shape_ok) e = ERR(MalformedNode);
            if (!e.code) {
                bc.metrics.byte_length = cf.ptr[2].u.u64_;
                e = raw_bytes_node(&cf.ptr[0], bc.node_ref.object_hash.b);
                if (!e.code) e = raw_bytes_node(&cf.ptr[1], bc.node_ref.locator.b);
                if (!e.code) e = raw_bytes_node(&cf.ptr[3], bc.logical_hash.b);
            }
            groove__records__values__ValueVec__drop(&cf);
            if (!e.code) JZ_VEC_PUSH(node.children, bc);
        }
    } else {
        e = ERR(MalformedNode);
    }
    groove__records__values__ValueVec__drop(&fields);
    groove__records__values__Value__drop(&value);
    if (!e.code) {
        jz_bytes canonical;
        e = encode_node_v1(&node, &canonical);
        if (!e.code) {
            if (!bytes_eq_slice(&canonical, encoded, n)) e = ERR(MalformedNode);
            jz_bytes_drop(&canonical);
        }
    }
    if (!e.code) e = validate_untyped_node_structure(&node);
    if (e.code) { groove__large_values__ChunkNode__drop(&node); return e; }
    *out = node;
    return JZ_OK;
}

static jz_err validate_untyped_node_structure(const groove_ChunkNode *node) {
    if (node->tag == GROOVE_CHUNK_NODE_LEAF) {
        JZ_TRY(check_format(node->format));
        if (node->bytes.len > LEAF_MAX_BYTES) return ERR(MalformedNode);
        if (node->kind != LVK_BYTES && !utf8_ok(node->bytes.ptr, node->bytes.len)) return ERR(InvalidUtf8);
        return JZ_OK;
    }
    JZ_TRY(check_format(node->format));
    if (node->children.len == 0 || node->children.len > BRANCH_MAX_CHILDREN) return ERR(MalformedNode);
    for (u32 i = 0; i < node->children.len; i++)
        if (node->children.ptr[i].metrics.byte_length == 0) return ERR(MalformedNode);
    Metrics acc = node->children.ptr[0].metrics;
    for (u32 i = 1; i < node->children.len; i++) JZ_TRY(add_metrics(acc, node->children.ptr[i].metrics, &acc));
    for (u32 i = 0; i < node->children.len; i++) {
        bool has = node->children.ptr[i].metrics.has_utf16;
        if (node->kind == LVK_BYTES && has) return ERR(MalformedNode);
        if (node->kind != LVK_BYTES && !has) return ERR(MalformedNode);
    }
    return JZ_OK;
}

/* ------------------------------------------------------------------ hashing */
static groove_ContentHash hash_domain(const char *domain, const u8 *bytes, u32 n);

groove_ContentHash groove__large_values__object_hash(const u8 *p, u32 n) { return hash_domain("groove-large-object-v1", p, n); }

static groove_ContentHash kind_format_hash_mask(u8 format, u8 kind) {
    u8 b[2] = {format, large_value_kind_tag(kind)};
    return hash_domain("groove-large-logical-kind-format-v1", b, 2);
}
static groove_ContentHash bind_grouping_hash(u8 format, u8 kind, groove_ContentHash grouping_hash) {
    groove_ContentHash mask = kind_format_hash_mask(format, kind);
    groove_ContentHash out;
    for (u32 i = 0; i < 32; i++) out.b[i] = grouping_hash.b[i] ^ mask.b[i];
    return out;
}
/* XOR is its own inverse. */
static groove_ContentHash grouping_hash_from_logical(u8 format, u8 kind, groove_ContentHash logical_hash) {
    return bind_grouping_hash(format, kind, logical_hash);
}

static void put_u64_le(jz_bytes *b, u64 v) {
    u8 t[8];
    for (int i = 0; i < 8; i++) t[i] = (u8)(v >> (8 * i));
    jz_bytes_push(b, t, 8);
}

/* Content-defined grouping deliberately remains stable across semantic kinds and format bumps. */
static groove_ContentHash node_grouping_hash(const groove_ChunkNode *node) {
    if (node->tag == GROOVE_CHUNK_NODE_LEAF) return hash_domain("groove-large-leaf-logical-v1", node->bytes.ptr, node->bytes.len);
    jz_bytes descriptor = {0};
    jz_vec_reserve(&descriptor, node->children.len * 48, 1);
    for (u32 i = 0; i < node->children.len; i++) {
        const groove_BranchChild *child = &node->children.ptr[i];
        groove_ContentHash g = grouping_hash_from_logical(node->format, node->kind, child->logical_hash);
        jz_bytes_push(&descriptor, g.b, 32);
        put_u64_le(&descriptor, child->metrics.byte_length);
        put_u64_le(&descriptor, child->metrics.has_utf16 ? child->metrics.utf16_length : UINT64_MAX);
    }
    groove_ContentHash h = hash_domain("groove-large-branch-logical-v1", descriptor.ptr, descriptor.len);
    jz_bytes_drop(&descriptor);
    return h;
}

static groove_ContentHash node_logical_hash(const groove_ChunkNode *node) {
    return bind_grouping_hash(node->format, node->kind, node_grouping_hash(node));
}

static jz_err node_metrics(u8 kind, const groove_ChunkNode *node, Metrics *out) {
    if (node_kind(node) != kind) return ERR(DescriptorMismatch);
    if (node->tag == GROOVE_CHUNK_NODE_LEAF) return metrics(kind, node->bytes.ptr, node->bytes.len, out);
    if (node->children.len == 0) return ERR(MalformedNode);
    Metrics acc = node->children.ptr[0].metrics;
    for (u32 i = 1; i < node->children.len; i++) JZ_TRY(add_metrics(acc, node->children.ptr[i].metrics, &acc));
    *out = acc;
    return JZ_OK;
}

static u8 node_format(const groove_ChunkNode *node) { return node->format; }
static u8 node_kind(const groove_ChunkNode *node) { return node->kind; }

/* ------------------------------------------------------------------ materialization */
/* Materialize: collects every leaf, checking each node against its claimed
 * metrics. Children are never positioned, so their walk length is zero. */
typedef struct Materialize { u8 kind; JZ_VEC(jz_bytes) leaves; } Materialize;
static jz_err materialize_leaf(void *self, const WalkSpan *span, jz_bytes bytes) {
    Materialize *m = self;
    if (span->has_metrics) {
        Metrics actual;
        jz_err e = metrics(m->kind, bytes.ptr, bytes.len, &actual);
        bool ok = !e.code && metrics_eq(actual, span->metrics);
        jz_err_drop(&e);
        if (!ok) { jz_bytes_drop(&bytes); return ERR(DescriptorMismatch); }
    }
    JZ_VEC_PUSH(m->leaves, bytes);
    return JZ_OK;
}
static jz_err materialize_branch(void *self, const WalkSpan *span, const groove_BranchChild *children, u32 n) {
    (void)self;
    if (span->has_metrics) {
        if (n == 0) return ERR(MalformedNode);
        Metrics acc = children[0].metrics;
        for (u32 i = 1; i < n; i++) JZ_TRY(add_metrics(acc, children[i].metrics, &acc));
        if (!metrics_eq(acc, span->metrics)) return ERR(DescriptorMismatch);
    }
    return JZ_OK;
}
static jz_err materialize_child_len(void *self, const Metrics *m, u64 *out) { (void)self; (void)m; *out = 0; return JZ_OK; }
static const LogicalVisitorVtbl MATERIALIZE_VTBL = {NULL, materialize_leaf, materialize_branch, materialize_child_len, false};

static void bytes_drop_v(void *b) { jz_bytes_drop(b); }

static jz_err apply_edits(jz_bytes *bytes, const groove_ReplaceEditVec *edits) {
    for (u32 i = 0; i < edits->len; i++) {
        const groove_ReplaceEdit *edit = &edits->ptr[i];
        if (edit->offset > UINT32_MAX || edit->delete_length > UINT32_MAX) return ERR(MetricOverflow);
        u64 end;
        if (!checked_add_u64(edit->offset, edit->delete_length, &end)) return ERR(MetricOverflow);
        if (end > bytes->len) return ERR(MalformedNode);
        jz_bytes spliced = {0};
        jz_vec_reserve(&spliced, bytes->len - (u32)edit->delete_length + edit->insert_bytes.len, 1);
        jz_bytes_push(&spliced, bytes->ptr, (u32)edit->offset);
        jz_bytes_push(&spliced, edit->insert_bytes.ptr, edit->insert_bytes.len);
        jz_bytes_push(&spliced, bytes->ptr + end, bytes->len - (u32)end);
        jz_bytes_drop(bytes);
        *bytes = spliced;
    }
    return JZ_OK;
}

/* Attempt to materialize one logical value using the evaluator-owned request set. */
jz_err groove__large_values__materialize_attempt(const LVRef *value, groove_EvaluationInputs *inputs, jz_bytes *out) {
    JZ_TRY(check_format(value->format_version));
    WalkSpan root = {0, 0, value->edit_tail.len == 0, {value->byte_length, value->has_utf16_length, value->utf16_length}};
    Materialize leaves = {value->kind, {0}};
    jz_err e = walk_logical(value, root, &MATERIALIZE_VTBL, &leaves, inputs);
    if (e.code) { jz_vec_drop(&leaves.leaves, sizeof(jz_bytes), bytes_drop_v); return e; }
    if (value->byte_length > UINT32_MAX) { jz_vec_drop(&leaves.leaves, sizeof(jz_bytes), bytes_drop_v); return ERR(MetricOverflow); }
    jz_bytes bytes = {0};
    jz_vec_reserve(&bytes, (u32)value->byte_length, 1);
    for (u32 i = 0; i < leaves.leaves.len; i++) jz_bytes_push(&bytes, leaves.leaves.ptr[i].ptr, leaves.leaves.ptr[i].len);
    jz_vec_drop(&leaves.leaves, sizeof(jz_bytes), bytes_drop_v);
    e = apply_edits(&bytes, &value->edit_tail);
    if (!e.code) e = validate_logical(value->kind, bytes.ptr, bytes.len);
    Metrics actual;
    if (!e.code) e = metrics(value->kind, bytes.ptr, bytes.len, &actual);
    if (!e.code && (actual.byte_length != value->byte_length || actual.has_utf16 != value->has_utf16_length ||
                    (actual.has_utf16 && actual.utf16_length != value->utf16_length)))
        e = ERR(DescriptorMismatch);
    if (e.code) { jz_bytes_drop(&bytes); return e; }
    *out = bytes;
    return JZ_OK;
}

static jz_err materialize_value_attempt(groove_Value *value, groove_EvaluationInputs *inputs, bool *blocked, bool *changed);

/* Cow<'a, [u8]>: *owned = true with out filled when rebuilt, else the caller keeps `raw`.
 * fields == NULL materializes every field. */
jz_err groove__large_values__materialize_record_borrowed_attempt(groove_RecordDescriptor descriptor, const u8 *raw, u32 n, const u32 *fields, u32 nfields, groove_EvaluationInputs *inputs, bool *owned, jz_bytes *out) {
    *owned = false;
    bool contains_indirect;
    JZ_TRY(groove__records__RecordDescriptor__fields_contain_indirect_values(descriptor, raw, n, fields, nfields, &contains_indirect));
    if (!contains_indirect) return JZ_OK;
    groove_BorrowedRecord bound = groove__records__RecordDescriptor__bind(descriptor, raw, n);
    groove_ValueVec values;
    JZ_TRY(groove__records__BorrowedRecord__to_values(&bound, &values));
    bool blocked = false, changed = false;
    jz_err e = JZ_OK;
    for (u32 index = 0; index < values.len; index++) {
        if (fields) {
            bool selected = false;
            for (u32 k = 0; k < nfields; k++) if (fields[k] == index) { selected = true; break; }
            if (!selected) continue;
        }
        bool c;
        e = materialize_value_attempt(&values.ptr[index], inputs, &blocked, &c);
        if (e.code) break;
        changed |= c;
    }
    if (!e.code && blocked) e = groove__ivm__runtime__IvmRuntimeError__evaluation_blocked();
    if (!e.code && changed) {
        e = groove__records__RecordDescriptor__create(descriptor, values.ptr, values.len, out);
        if (!e.code) *owned = true;
    }
    groove__records__values__ValueVec__drop(&values);
    return e;
}

jz_err groove__large_values__materialize_record_attempt(groove_RecordDescriptor descriptor, const u8 *raw, u32 n, groove_EvaluationInputs *inputs, jz_bytes *out) {
    bool owned;
    JZ_TRY(groove__large_values__materialize_record_borrowed_attempt(descriptor, raw, n, NULL, 0, inputs, &owned, out));
    if (!owned) *out = jz_bytes_from(raw, n); /* Cow::into_owned */
    return JZ_OK;
}

/* Materialize only the logical fields an operator will inspect. */
jz_err groove__large_values__materialize_record_fields_attempt(groove_RecordDescriptor descriptor, const u8 *raw, u32 n, const u32 *field_indices, u32 nfields, groove_EvaluationInputs *inputs, jz_bytes *out) {
    bool contains;
    JZ_TRY(groove__records__RecordDescriptor__fields_contain_indirect_values(descriptor, raw, n, field_indices, nfields, &contains));
    if (!contains) { *out = jz_bytes_from(raw, n); return JZ_OK; }
    groove_BorrowedRecord bound = groove__records__RecordDescriptor__bind(descriptor, raw, n);
    groove_ValueVec values;
    JZ_TRY(groove__records__BorrowedRecord__to_values(&bound, &values));
    bool blocked = false, changed = false;
    jz_err e = JZ_OK;
    for (u32 k = 0; k < nfields; k++) {
        u32 index = field_indices[k];
        if (index >= values.len) {
            u32 len;
            groove__records__RecordDescriptor__fields(descriptor, &len);
            e = groove__records__Error__field_index_out_of_bounds(index, len);
            break;
        }
        bool c;
        e = materialize_value_attempt(&values.ptr[index], inputs, &blocked, &c);
        if (e.code) break;
        changed |= c;
    }
    if (!e.code && blocked) e = groove__ivm__runtime__IvmRuntimeError__evaluation_blocked();
    if (!e.code) {
        if (changed) e = groove__records__RecordDescriptor__create(descriptor, values.ptr, values.len, out);
        else *out = jz_bytes_from(raw, n);
    }
    groove__records__values__ValueVec__drop(&values);
    return e;
}

static jz_err materialize_value_attempt(groove_Value *value, groove_EvaluationInputs *inputs, bool *blocked, bool *changed) {
    *changed = false;
    switch (value->tag) {
    case GROOVE_V_LARGE: {
        groove_LargeValueRef *large = value->u.large;
        jz_bytes bytes;
        jz_err e = groove__large_values__materialize_attempt(large, inputs, &bytes);
        if (e.code) {
            if (IS_BLOCKED(e)) { jz_err_drop(&e); *blocked = true; return JZ_OK; }
            return e;
        }
        groove_Value next;
        memset(&next, 0, sizeof next);
        if (large->kind == LVK_BYTES) {
            next.tag = GROOVE_V_BYTES;
            next.u.bytes = bytes;
        } else {
            /* Groove currently exposes JSON source through its string scalar type. */
            if (!utf8_ok(bytes.ptr, bytes.len)) { jz_bytes_drop(&bytes); return ERR(InvalidUtf8); }
            next.tag = GROOVE_V_STRING;
            memcpy(&next.u.s, &bytes, sizeof bytes);
        }
        groove__records__values__Value__drop(value);
        *value = next;
        *changed = true;
        return JZ_OK;
    }
    case GROOVE_V_TUPLE:
    case GROOVE_V_ARRAY: {
        bool any = false;
        for (u32 i = 0; i < value->u.items.len; i++) {
            bool c;
            JZ_TRY(materialize_value_attempt(&value->u.items.ptr[i], inputs, blocked, &c));
            any |= c;
        }
        *changed = any;
        return JZ_OK;
    }
    case GROOVE_V_NULLABLE:
        if (value->u.nullable) return materialize_value_attempt(value->u.nullable, inputs, blocked, changed);
        return JZ_OK;
    case GROOVE_V_RECORD:
    case GROOVE_V_ENUM: {
        groove_OwnedRecord *record = value->tag == GROOVE_V_RECORD ? &value->u.record : &value->u.enum_.record;
        groove_RecordDescriptor descriptor = record->descriptor;
        jz_bytes raw;
        jz_err e = groove__large_values__materialize_record_attempt(descriptor, record->raw.ptr, record->raw.len, inputs, &raw);
        if (e.code) {
            if (IS_BLOCKED(e)) { jz_err_drop(&e); *blocked = true; return JZ_OK; }
            return e;
        }
        *changed = !jz_bytes_eq(&raw, &record->raw);
        groove__records__OwnedRecord__drop(record);
        *record = groove__records__OwnedRecord__new(raw, descriptor); /* EnumValue::new(tag, ..) keeps tag */
        return JZ_OK;
    }
    default:
        return JZ_OK;
    }
}

/* ------------------------------------------------------------------ range reads */
typedef struct RangePiece { bool inserted; u64 start, end; jz_bytes bytes; } RangePiece;
typedef JZ_VEC(RangePiece) RangePieceVec;
static void range_piece_drop_v(void *p) { jz_bytes_drop(&((RangePiece *)p)->bytes); }

/* RangeUnit::edit_span -> (offset, inserted length, deleted length) */
static void range_unit_edit_span(RangeUnit unit, const groove_ReplaceEdit *edit, u64 *offset, u64 *inserted, u64 *deleted) {
    if (unit == RANGE_UNIT_BYTES) { *offset = edit->offset; *inserted = edit->insert_bytes.len; *deleted = edit->delete_length; }
    else { *offset = edit->utf16_offset; *inserted = edit->insert_utf16_length; *deleted = edit->delete_utf16_length; }
}
static jz_err range_unit_child_length(RangeUnit unit, const Metrics *m, u64 *out) {
    if (unit == RANGE_UNIT_BYTES) { *out = m->byte_length; return JZ_OK; }
    if (!m->has_utf16) return ERR(DescriptorMismatch);
    *out = m->utf16_length;
    return JZ_OK;
}
static jz_err slice_utf16(const u8 *bytes, u32 n, u64 start, u64 end, jz_bytes *out);
static jz_err range_unit_slice(RangeUnit unit, const u8 *bytes, u32 n, u64 start, u64 end, jz_bytes *out) {
    if (unit == RANGE_UNIT_BYTES) {
        if (start > UINT32_MAX || end > UINT32_MAX) return ERR(MetricOverflow);
        if (start > end || end > n) jz_panic("range end index out of range for slice");
        *out = jz_bytes_from(bytes + start, (u32)(end - start));
        return JZ_OK;
    }
    return slice_utf16(bytes, n, start, end, out);
}

static jz_err map_final_range_to_base(u64 start, u64 end, const groove_ReplaceEditVec *edits, RangeUnit unit, RangePieceVec *out);
static jz_err base_range_attempt(const LVRef *value, u64 base_len, u64 start, u64 end, RangeUnit unit, groove_EvaluationInputs *inputs, jz_bytes *out);

typedef jz_err (*ReadPieceFn)(void *ctx, RangePiece *piece, u64 base_length); /* piece is consumed */

/* Map a final range onto the base tree and read each piece in order. A
 * blocked piece wins only after every piece has been tried. */
static jz_err read_pieces(const LVRef *value, u64 start, u64 end, u64 length, RangeUnit unit, ReadPieceFn read, void *ctx) {
    RangePieceVec pieces;
    JZ_TRY(map_final_range_to_base(start, end, &value->edit_tail, unit, &pieces));
    u64 base_len;
    jz_err e = base_length(length, &value->edit_tail, unit, &base_len);
    bool blocked = false;
    u32 i = 0;
    for (; !e.code && i < pieces.len; i++) {
        RangePiece piece = pieces.ptr[i];
        memset(&pieces.ptr[i].bytes, 0, sizeof piece.bytes);
        e = read(ctx, &piece, base_len);
        jz_bytes_drop(&piece.bytes);
        if (e.code && IS_BLOCKED(e)) { jz_err_drop(&e); e = JZ_OK; blocked = true; }
    }
    jz_vec_drop(&pieces, sizeof(RangePiece), range_piece_drop_v);
    JZ_TRY(e);
    if (blocked) return groove__ivm__runtime__IvmRuntimeError__evaluation_blocked();
    return JZ_OK;
}

typedef struct RangeCtx { const LVRef *value; RangeUnit unit; groove_EvaluationInputs *inputs; jz_bytes output; } RangeCtx;
static jz_err range_read_piece(void *ctx, RangePiece *piece, u64 base_len) {
    RangeCtx *c = ctx;
    if (piece->inserted) {
        jz_bytes_push(&c->output, piece->bytes.ptr, piece->bytes.len);
        return JZ_OK;
    }
    jz_bytes got;
    JZ_TRY(base_range_attempt(c->value, base_len, piece->start, piece->end, c->unit, c->inputs, &got));
    jz_bytes_push(&c->output, got.ptr, got.len);
    jz_bytes_drop(&got);
    return JZ_OK;
}

static jz_err range_attempt(const LVRef *value, u64 start, u64 end, RangeUnit unit, groove_EvaluationInputs *inputs, jz_bytes *out) {
    u64 length;
    if (unit == RANGE_UNIT_BYTES) length = value->byte_length;
    else {
        if (!value->has_utf16_length) return ERR(MalformedScalar);
        length = value->utf16_length;
    }
    if (start > end || end > length) return ERR(MalformedScalar);
    RangeCtx ctx = {value, unit, inputs, {0}};
    jz_err e = read_pieces(value, start, end, length, unit, range_read_piece, &ctx);
    if (e.code) { jz_bytes_drop(&ctx.output); return e; }
    *out = ctx.output;
    return JZ_OK;
}

/* Read a byte range from the final logical value. */
jz_err groove__large_values__byte_range_attempt(const LVRef *value, u64 start, u64 end, groove_EvaluationInputs *inputs, jz_bytes *out) {
    return range_attempt(value, start, end, RANGE_UNIT_BYTES, inputs, out);
}
/* byte_range_attempt in UTF-16 code-unit coordinates. */
jz_err groove__large_values__utf16_range_attempt(const LVRef *value, u64 start, u64 end, groove_EvaluationInputs *inputs, jz_bytes *out) {
    return range_attempt(value, start, end, RANGE_UNIT_UTF16, inputs, out);
}

static jz_err utf8_boundary_at_or_before_attempt(const LVRef *value, u64 offset, groove_EvaluationInputs *inputs, u64 *out);
static jz_err utf8_next_boundary_attempt(const LVRef *value, u64 boundary, groove_EvaluationInputs *inputs, u64 *out);

/* Resolve a final logical UTF-16 boundary to its final logical byte offset. */
jz_err groove__large_values__utf16_offset_to_byte_attempt(const LVRef *value, u64 offset, groove_EvaluationInputs *inputs, u64 *out) {
    if (!value->has_utf16_length) return ERR(MalformedScalar);
    u64 total = value->utf16_length;
    if (offset > total) return ERR(MalformedScalar);
    if (offset == 0) { *out = 0; return JZ_OK; }
    if (offset == total) { *out = value->byte_length; return JZ_OK; }
    u64 low = 0, high = value->byte_length;
    while (low < high) {
        u64 midpoint = low + (high - low) / 2;
        u64 boundary, units;
        JZ_TRY(utf8_boundary_at_or_before_attempt(value, midpoint, inputs, &boundary));
        JZ_TRY(utf16_length_for_byte_range_attempt(value, 0, boundary, inputs, &units));
        if (units < offset) JZ_TRY(utf8_next_boundary_attempt(value, boundary, inputs, &low));
        else high = boundary;
    }
    u64 units;
    JZ_TRY(utf16_length_for_byte_range_attempt(value, 0, low, inputs, &units));
    if (units != offset) return ERR(MalformedScalar);
    *out = low;
    return JZ_OK;
}

static jz_err utf8_boundary_at_or_before_attempt(const LVRef *value, u64 offset, groove_EvaluationInputs *inputs, u64 *out) {
    if (offset == 0 || offset == value->byte_length) { *out = offset; return JZ_OK; }
    u64 start = offset >= 3 ? offset - 3 : 0;
    u64 end = offset == UINT64_MAX ? UINT64_MAX : offset + 1;
    jz_bytes bytes;
    JZ_TRY(groove__large_values__byte_range_attempt(value, start, end, inputs, &bytes));
    if (bytes.len == 0) { jz_bytes_drop(&bytes); return ERR(MalformedScalar); }
    u32 index = bytes.len - 1;
    while ((bytes.ptr[index] & 0xc0) == 0x80) {
        if (index == 0) { jz_bytes_drop(&bytes); return ERR(MalformedScalar); }
        index -= 1;
    }
    jz_bytes_drop(&bytes);
    if (!checked_add_u64(start, index, out)) return ERR(MetricOverflow);
    return JZ_OK;
}

static jz_err utf8_next_boundary_attempt(const LVRef *value, u64 boundary, groove_EvaluationInputs *inputs, u64 *out) {
    if (boundary >= value->byte_length) { *out = value->byte_length; return JZ_OK; }
    u64 end = boundary > UINT64_MAX - 4 ? UINT64_MAX : boundary + 4;
    if (end > value->byte_length) end = value->byte_length;
    jz_bytes bytes;
    JZ_TRY(groove__large_values__byte_range_attempt(value, boundary, end, inputs, &bytes));
    if (bytes.len == 0) { jz_bytes_drop(&bytes); return ERR(MalformedScalar); }
    u8 first = bytes.ptr[0];
    u32 have = bytes.len;
    jz_bytes_drop(&bytes);
    u64 width;
    if (first <= 0x7f) width = 1;
    else if (first >= 0xc2 && first <= 0xdf) width = 2;
    else if (first >= 0xe0 && first <= 0xef) width = 3;
    else if (first >= 0xf0 && first <= 0xf4) width = 4;
    else return ERR(InvalidUtf8);
    u64 next;
    if (!checked_add_u64(boundary, width, &next)) return ERR(MetricOverflow);
    if (next > value->byte_length || width > have) return ERR(InvalidUtf8);
    *out = next;
    return JZ_OK;
}

static jz_err slice_utf16(const u8 *bytes, u32 n, u64 rstart, u64 rend, jz_bytes *out) {
    if (!utf8_ok(bytes, n)) return ERR(InvalidUtf8);
    u64 units = 0;
    bool has_start = false, has_end = false;
    u32 start = 0, end = 0;
    u32 byte = 0;
    for (;;) {
        /* char_indices() chained with (text.len(), '\0') */
        bool at_end = byte >= n;
        if (units == rstart) { has_start = true; start = byte; }
        if (units == rend) { has_end = true; end = byte; break; }
        u32 width = 1;
        if (!at_end) {
            u8 c = bytes[byte];
            width = c < 0x80 ? 1 : (c & 0xe0) == 0xc0 ? 2 : (c & 0xf0) == 0xe0 ? 3 : 4;
            if (!checked_add_u64(units, width == 4 ? 2 : 1, &units)) return ERR(MetricOverflow);
        }
        if ((units > rstart && !has_start) || units > rend) return ERR(MalformedScalar);
        if (at_end) break;
        byte += width;
    }
    if (!has_start || !has_end) return ERR(MalformedScalar);
    *out = jz_bytes_from(bytes + start, end - start);
    return JZ_OK;
}

/* Count UTF-16 units in a byte-coordinate range without hydrating complete subtrees. */
static jz_err base_utf16_length_for_byte_range_attempt(const LVRef *value, u64 base_len, u64 start, u64 end, groove_EvaluationInputs *inputs, u64 *out);

typedef struct Utf16TotalCtx { const LVRef *value; groove_EvaluationInputs *inputs; u64 total; } Utf16TotalCtx;
static jz_err utf16_total_piece(void *ctx, RangePiece *piece, u64 base_len) {
    Utf16TotalCtx *c = ctx;
    u64 length;
    if (piece->inserted) {
        if (!utf8_ok(piece->bytes.ptr, piece->bytes.len)) return ERR(InvalidUtf8);
        JZ_TRY(utf16_length_of(piece->bytes.ptr, piece->bytes.len, &length));
    } else {
        JZ_TRY(base_utf16_length_for_byte_range_attempt(c->value, base_len, piece->start, piece->end, c->inputs, &length));
    }
    if (!checked_add_u64(c->total, length, &c->total)) return ERR(MetricOverflow);
    return JZ_OK;
}

static jz_err utf16_length_for_byte_range_attempt(const LVRef *value, u64 start, u64 end, groove_EvaluationInputs *inputs, u64 *out) {
    if (value->kind == LVK_BYTES || start > end || end > value->byte_length) return ERR(MalformedScalar);
    Utf16TotalCtx ctx = {value, inputs, 0};
    JZ_TRY(read_pieces(value, start, end, value->byte_length, RANGE_UNIT_BYTES, utf16_total_piece, &ctx));
    *out = ctx.total;
    return JZ_OK;
}

/* Utf16Count: sums the UTF-16 metric of fully covered subtrees and counts the
 * covered part of each boundary leaf. Spans are in bytes. */
typedef struct Utf16Count { u64 start, end; u64 total; } Utf16Count;
static jz_err utf16_count_add(Utf16Count *c, u64 units) {
    if (!checked_add_u64(c->total, units, &c->total)) return ERR(MetricOverflow);
    return JZ_OK;
}
static jz_err utf16_count_wants(void *self, const WalkSpan *span, bool *out) {
    Utf16Count *c = self;
    u64 start = span->start, end;
    JZ_TRY(walk_span_end(span, &end));
    if (c->start >= end || c->end <= start) { *out = false; return JZ_OK; }
    if (c->start <= start && end <= c->end) {
        if (!(span->has_metrics && span->metrics.has_utf16)) return ERR(DescriptorMismatch);
        JZ_TRY(utf16_count_add(c, span->metrics.utf16_length));
        *out = false;
        return JZ_OK;
    }
    *out = true;
    return JZ_OK;
}
static jz_err utf16_count_leaf(void *self, const WalkSpan *span, jz_bytes bytes) {
    Utf16Count *c = self;
    u64 span_end;
    jz_err e = walk_span_end(span, &span_end);
    if (e.code) { jz_bytes_drop(&bytes); return e; }
    u64 a = (c->start > span->start ? c->start : span->start) - span->start;
    u64 b = (c->end < span_end ? c->end : span_end) - span->start;
    if (a > UINT32_MAX || b > UINT32_MAX) { jz_bytes_drop(&bytes); return ERR(MetricOverflow); }
    if (a > b || b > bytes.len) jz_panic("range end index out of range for slice");
    if (!utf8_ok(bytes.ptr + a, (u32)(b - a))) { jz_bytes_drop(&bytes); return ERR(InvalidUtf8); }
    u64 units;
    utf16_length_of(bytes.ptr + a, (u32)(b - a), &units);
    jz_bytes_drop(&bytes);
    return utf16_count_add(c, units);
}
static jz_err utf16_count_child_len(void *self, const Metrics *m, u64 *out) {
    (void)self;
    if (!m->has_utf16) return ERR(DescriptorMismatch);
    *out = m->byte_length;
    return JZ_OK;
}
static const LogicalVisitorVtbl UTF16_COUNT_VTBL = {utf16_count_wants, utf16_count_leaf, NULL, utf16_count_child_len, false};

static jz_err base_utf16_length_for_byte_range_attempt(const LVRef *value, u64 base_len, u64 start, u64 end, groove_EvaluationInputs *inputs, u64 *out) {
    if (end > base_len) return ERR(MalformedScalar);
    if (!value->has_utf16_length) return ERR(MalformedScalar);
    u64 base_utf16 = value->utf16_length;
    for (u32 i = value->edit_tail.len; i > 0; i--) {
        const groove_ReplaceEdit *edit = &value->edit_tail.ptr[i - 1];
        if (!checked_sub_u64(base_utf16, edit->insert_utf16_length, &base_utf16) || !checked_add_u64(base_utf16, edit->delete_utf16_length, &base_utf16))
            return ERR(MetricOverflow);
    }
    WalkSpan root = {0, base_len, true, {base_len, true, base_utf16}};
    Utf16Count count = {start, end, 0};
    JZ_TRY(walk_logical(value, root, &UTF16_COUNT_VTBL, &count, inputs));
    *out = count.total;
    return JZ_OK;
}

/* ------------------------------------------------------------------ inline comparison */
/* -> std::cmp::Ordering as -1/0/1 */
jz_err groove__large_values__compare_inline_attempt(const LVRef *value, const u8 *inline_, u32 n, groove_EvaluationInputs *inputs, int *out) {
    JZ_TRY(validate_descriptor(value));
    if ((value->kind == LVK_STRING || value->kind == LVK_JSON) && !utf8_ok(inline_, n)) return ERR(InvalidUtf8);
    u64 common = value->byte_length < n ? value->byte_length : n;
    u64 offset = 0;
    while (offset < common) {
        u64 end = offset > UINT64_MAX - LEAF_MIN_BYTES ? UINT64_MAX : offset + LEAF_MIN_BYTES;
        if (end > common) end = common;
        jz_bytes indirect;
        JZ_TRY(groove__large_values__byte_range_attempt(value, offset, end, inputs, &indirect));
        int ord = jz_slice_cmp(jz_bytes_slice(&indirect), jz_slice_of(inline_ + offset, (u32)(end - offset)));
        jz_bytes_drop(&indirect);
        if (ord != 0) { *out = ord < 0 ? -1 : 1; return JZ_OK; }
        offset = end;
    }
    *out = value->byte_length < n ? -1 : value->byte_length > n;
    return JZ_OK;
}

/* ------------------------------------------------------------------ JSON pointer prefix */
static u32 skip_json_ws(const u8 *source, u32 n, u32 position) {
    while (position < n && (source[position] == ' ' || source[position] == '\n' || source[position] == '\r' || source[position] == '\t')) position++;
    return position;
}

static jz_err decode_json_pointer_component(const char *p, u32 n, jz_str *out) {
    jz_str decoded = {0};
    jz_vec_reserve(&decoded, n, 1);
    for (u32 i = 0; i < n; i++) {
        if (p[i] != '~') { jz_str_push_char(&decoded, p[i]); continue; }
        if (i + 1 < n && p[i + 1] == '0') jz_str_push_char(&decoded, '~');
        else if (i + 1 < n && p[i + 1] == '1') jz_str_push_char(&decoded, '/');
        else { jz_str_drop(&decoded); return ERR(InvalidJson); }
        i++;
    }
    *out = decoded;
    return JZ_OK;
}

/* str::parse::<usize>() followed by the round-trip `component == index.to_string()` check */
static bool parse_canonical_index(const jz_str *s, u32 *out) {
    u32 i = 0;
    if (s->len > 0 && s->ptr[0] == '+') i = 1;
    if (i >= s->len) return false;
    u64 v = 0;
    for (; i < s->len; i++) {
        if (s->ptr[i] < '0' || s->ptr[i] > '9') return false;
        v = v * 10 + (u64)(s->ptr[i] - '0');
        if (v > UINT32_MAX) return false;
    }
    *out = (u32)v;
    jz_str canonical = jz_format("%u", (unsigned)v);
    bool same = jz_str_eq(&canonical, s);
    jz_str_drop(&canonical);
    return same;
}

/* serde_json::StreamDeserializer framing of one value starting at source[0]:
 * 0 = complete (*end set), 1 = EOF inside the value, 2 = syntax error. */
static int json_value_extent(const u8 *source, u32 n, u32 *end) {
    u32 depth = 0, i = 0;
    for (;;) {
        if (i >= n) return 1;
        u8 c = source[i];
        if (c == '"') {
            i++;
            for (;;) {
                if (i >= n) return 1;
                if (source[i] == '\\') { i += 2; continue; }
                if (source[i] == '"') { i++; break; }
                i++;
            }
        } else if (c == '{' || c == '[') {
            depth++;
            i++;
        } else if (c == '}' || c == ']') {
            if (depth == 0) return 2;
            depth--;
            i++;
        } else if (c == '-' || (c >= '0' && c <= '9')) {
            while (i < n && (source[i] == '-' || source[i] == '+' || source[i] == '.' || source[i] == 'e' || source[i] == 'E' ||
                             (source[i] >= '0' && source[i] <= '9')))
                i++;
            if (i >= n && depth != 0) return 1;
        } else if (c == 't' || c == 'f' || c == 'n') {
            const char *lit = c == 't' ? "true" : c == 'f' ? "false" : "null";
            u32 len = (u32)strlen(lit);
            for (u32 k = 0; k < len; k++) {
                if (i + k >= n) return 1;
                if (source[i + k] != (u8)lit[k]) return 2;
            }
            i += len;
        } else if (depth > 0 && (c == ',' || c == ':' || c == ' ' || c == '\n' || c == '\r' || c == '\t')) {
            i++;
        } else {
            return 2;
        }
        if (depth == 0) { *end = i; return 0; }
    }
}

/* Resolve pointers whose first component is an array index from a source prefix. */
jz_err groove__large_values__json_pointer_prefix(const u8 *source, u32 n, const char *pointer, u32 plen, groove_JsonPointerPrefix *out) {
    memset(out, 0, sizeof *out);
    if (plen == 0) { out->tag = GROOVE_JSON_POINTER_REQUIRES_FULL_DOCUMENT; return JZ_OK; }
    if (pointer[0] != '/') return ERR(InvalidJson);
    /* split('/') */
    JZ_VEC(jz_str) components = {0};
    jz_err e = JZ_OK;
    u32 seg = 1;
    for (u32 i = 1; i <= plen; i++) {
        if (i == plen || pointer[i] == '/') {
            jz_str c;
            e = decode_json_pointer_component(pointer + seg, i - seg, &c);
            if (e.code) break;
            JZ_VEC_PUSH(components, c);
            seg = i + 1;
        }
    }
    u32 target_index = 0;
    if (!e.code && !parse_canonical_index(&components.ptr[0], &target_index)) {
        out->tag = GROOVE_JSON_POINTER_REQUIRES_FULL_DOCUMENT;
        goto done;
    }
    if (e.code) goto done;
    u32 position = skip_json_ws(source, n, 0);
    if (position >= n || source[position] != '[') { out->tag = GROOVE_JSON_POINTER_REQUIRES_FULL_DOCUMENT; goto done; }
    position += 1;
    for (u32 index = 0; index <= target_index; index++) {
        position = skip_json_ws(source, n, position);
        if (position >= n) { out->tag = GROOVE_JSON_POINTER_NEED_MORE; goto done; }
        if (source[position] == ']') { out->tag = GROOVE_JSON_POINTER_FOUND; goto done; }
        u32 extent;
        int status = json_value_extent(source + position, n - position, &extent);
        if (status == 1) { out->tag = GROOVE_JSON_POINTER_NEED_MORE; goto done; }
        if (status == 2) { e = ERR(InvalidJson); goto done; }
        jz_json *value = NULL;
        e = jz_json_parse(jz_slice_of(source + position, extent), &value);
        if (e.code) { jz_err_drop(&e); e = ERR(InvalidJson); goto done; }
        position += extent;
        if (index == target_index) {
            const jz_json *selected = value;
            for (u32 c = 1; c < components.len && selected; c++) {
                const jz_str *component = &components.ptr[c];
                if (selected->kind == JZ_JSON_OBJ) {
                    jz_str key = jz_str_clone(component);
                    jz_str_push_char(&key, 0);
                    selected = jz_json_obj_get(selected, key.ptr);
                    jz_str_drop(&key);
                } else if (selected->kind == JZ_JSON_ARR) {
                    u32 k;
                    selected = parse_canonical_index(component, &k) && k < selected->v.arr.len ? selected->v.arr.ptr[k] : NULL;
                } else {
                    selected = NULL;
                }
            }
            out->tag = GROOVE_JSON_POINTER_FOUND;
            out->found = selected ? jz_json_clone(selected) : NULL;
            jz_json_free(value);
            goto done;
        }
        jz_json_free(value);
        position = skip_json_ws(source, n, position);
        if (position >= n) { out->tag = GROOVE_JSON_POINTER_NEED_MORE; goto done; }
        if (source[position] == ',') position += 1;
        else if (source[position] == ']') { out->tag = GROOVE_JSON_POINTER_FOUND; goto done; }
        else { e = ERR(InvalidJson); goto done; }
    }
    e = ERR(InvalidJson);
done:
    for (u32 i = 0; i < components.len; i++) jz_str_drop(&components.ptr[i]);
    jz_vec_drop(&components, sizeof(jz_str), NULL);
    return e;
}

/* ------------------------------------------------------------------ tail coordinate mapping */
static jz_err base_length(u64 final_length, const groove_ReplaceEditVec *edits, RangeUnit unit, u64 *out) {
    u64 length = final_length;
    for (u32 i = edits->len; i > 0; i--) {
        u64 offset, inserted, deleted;
        range_unit_edit_span(unit, &edits->ptr[i - 1], &offset, &inserted, &deleted);
        if (!checked_sub_u64(length, inserted, &length) || !checked_add_u64(length, deleted, &length)) return ERR(MetricOverflow);
    }
    *out = length;
    return JZ_OK;
}

static jz_err map_final_range_to_base(u64 rstart, u64 rend, const groove_ReplaceEditVec *edits, RangeUnit unit, RangePieceVec *out) {
    RangePieceVec pieces = {0};
    RangePiece base = {false, rstart, rend, {0}};
    JZ_VEC_PUSH(pieces, base);
    jz_err e = JZ_OK;
    for (u32 i = edits->len; i > 0 && !e.code; i--) {
        const groove_ReplaceEdit *edit = &edits->ptr[i - 1];
        u64 offset, inserted, deleted;
        range_unit_edit_span(unit, edit, &offset, &inserted, &deleted);
        u64 inserted_end;
        if (!checked_add_u64(offset, inserted, &inserted_end)) { e = ERR(MetricOverflow); break; }
        RangePieceVec previous = {0};
        for (u32 p = 0; p < pieces.len; p++) {
            RangePiece piece = pieces.ptr[p];
            memset(&pieces.ptr[p].bytes, 0, sizeof piece.bytes);
            if (piece.inserted || e.code) {
                if (e.code) jz_bytes_drop(&piece.bytes);
                else JZ_VEC_PUSH(previous, piece);
                continue;
            }
            if (piece.start < offset) {
                RangePiece r = {false, piece.start, piece.end < offset ? piece.end : offset, {0}};
                JZ_VEC_PUSH(previous, r);
            }
            u64 overlap_start = piece.start > offset ? piece.start : offset;
            u64 overlap_end = piece.end < inserted_end ? piece.end : inserted_end;
            if (overlap_start < overlap_end) {
                RangePiece r = {true, 0, 0, {0}};
                e = range_unit_slice(unit, edit->insert_bytes.ptr, edit->insert_bytes.len, overlap_start - offset, overlap_end - offset, &r.bytes);
                if (e.code) continue;
                JZ_VEC_PUSH(previous, r);
            }
            if (piece.end > inserted_end) {
                u64 start = piece.start > inserted_end ? piece.start : inserted_end;
                u64 ms, me;
                if (!checked_sub_u64(start, inserted, &ms) || !checked_add_u64(ms, deleted, &ms) ||
                    !checked_sub_u64(piece.end, inserted, &me) || !checked_add_u64(me, deleted, &me)) {
                    e = ERR(MetricOverflow);
                    continue;
                }
                RangePiece r = {false, ms, me, {0}};
                JZ_VEC_PUSH(previous, r);
            }
        }
        jz_vec_drop(&pieces, sizeof(RangePiece), range_piece_drop_v);
        pieces = previous;
    }
    if (e.code) { jz_vec_drop(&pieces, sizeof(RangePiece), range_piece_drop_v); return e; }
    *out = pieces;
    return JZ_OK;
}

/* RangeRead: appends the in-range part of each intersecting leaf, in order. */
typedef struct RangeRead { u64 start, end; RangeUnit unit; jz_bytes out; } RangeRead;
static jz_err range_read_wants(void *self, const WalkSpan *span, bool *out) {
    RangeRead *r = self;
    u64 end;
    JZ_TRY(walk_span_end(span, &end));
    *out = r->start < end && r->end > span->start;
    return JZ_OK;
}
static jz_err range_read_leaf(void *self, const WalkSpan *span, jz_bytes bytes) {
    RangeRead *r = self;
    jz_err e = JZ_OK;
    /* UTF-16 slicing fails on its own past the end of the leaf. */
    if (r->unit == RANGE_UNIT_BYTES && (u64)bytes.len != span->len) e = ERR(DescriptorMismatch);
    u64 start = span->start, end = 0;
    if (!e.code) e = walk_span_end(span, &end);
    jz_bytes part;
    if (!e.code) {
        u64 a = (r->start > start ? r->start : start) - start;
        u64 b = (r->end < end ? r->end : end) - start;
        e = range_unit_slice(r->unit, bytes.ptr, bytes.len, a, b, &part);
    }
    jz_bytes_drop(&bytes);
    JZ_TRY(e);
    jz_bytes_push(&r->out, part.ptr, part.len);
    jz_bytes_drop(&part);
    return JZ_OK;
}
static jz_err range_read_child_len(void *self, const Metrics *m, u64 *out) { return range_unit_child_length(((RangeRead *)self)->unit, m, out); }
static const LogicalVisitorVtbl RANGE_READ_VTBL = {range_read_wants, range_read_leaf, NULL, range_read_child_len, true};

static jz_err base_range_attempt(const LVRef *value, u64 base_len, u64 start, u64 end, RangeUnit unit, groove_EvaluationInputs *inputs, jz_bytes *out) {
    if (end > base_len) return ERR(MalformedScalar);
    memset(out, 0, sizeof *out);
    if (start >= end) return JZ_OK; /* range.is_empty() */
    WalkSpan root = {0, base_len, false, {0}};
    RangeRead read = {start, end, unit, {0}};
    jz_err e = walk_logical(value, root, &RANGE_READ_VTBL, &read, inputs);
    if (e.code) { jz_bytes_drop(&read.out); return e; }
    *out = read.out;
    return JZ_OK;
}

/* ------------------------------------------------------------------ primitives */
static groove_ContentHash hash_domain(const char *domain, const u8 *bytes, u32 n) {
    u32 dlen = (u32)strlen(domain);
    jz_blake3_hasher *hasher = jz_blake3_new();
    u8 len_le[8];
    for (int i = 0; i < 8; i++) len_le[i] = (u8)((u64)dlen >> (8 * i));
    jz_blake3_update(hasher, len_le, 8);
    jz_blake3_update(hasher, (const u8 *)domain, dlen);
    jz_blake3_update(hasher, bytes, n);
    groove_ContentHash out;
    jz_blake3_finalize(hasher, out.b);
    jz_blake3_free(hasher);
    return out;
}

static jz_err check_format(u8 format) {
    if (format == FORMAT_VERSION) return JZ_OK;
    return groove__large_values__Error__unsupported_format(format);
}

static jz_err validate_logical(u8 kind, const u8 *p, u32 n) {
    switch (kind) {
    case LVK_BYTES: return JZ_OK;
    case LVK_STRING: return utf8_ok(p, n) ? JZ_OK : ERR(InvalidUtf8);
    default: {
        if (!utf8_ok(p, n)) return ERR(InvalidUtf8);
        jz_json *v = NULL;
        jz_err e = jz_json_parse(jz_slice_of(p, n), &v); /* serde_json::from_str::<Value> */
        if (e.code) { jz_err_drop(&e); return ERR(InvalidJson); }
        jz_json_free(v);
        return JZ_OK;
    }
    }
}

static jz_err metrics(u8 kind, const u8 *p, u32 n, Metrics *out) {
    out->byte_length = n;
    if (kind == LVK_BYTES) {
        out->has_utf16 = false;
        out->utf16_length = 0;
    } else {
        if (!utf8_ok(p, n)) return ERR(InvalidUtf8);
        out->has_utf16 = true;
        out->utf16_length = utf16_count(p, n);
    }
    return JZ_OK;
}

static jz_err add_metrics(Metrics left, Metrics right, Metrics *out) {
    Metrics m;
    if (!checked_add_u64(left.byte_length, right.byte_length, &m.byte_length)) return ERR(MetricOverflow);
    if (!left.has_utf16 && !right.has_utf16) { m.has_utf16 = false; m.utf16_length = 0; }
    else if (left.has_utf16 && right.has_utf16) {
        m.has_utf16 = true;
        if (!checked_add_u64(left.utf16_length, right.utf16_length, &m.utf16_length)) return ERR(MetricOverflow);
    } else return ERR(MalformedNode);
    *out = m;
    return JZ_OK;
}

/* cdc_ranges<T>: one copy over a type-erased item accessor. */
typedef u64 (*CdcRollFn)(void *ctx, u64 hash, u32 index);
typedef bool (*CdcCutFn)(u64 hash, u32 length);
typedef jz_err (*CdcSettleFn)(void *ctx, u32 start, u32 end, u32 *out);

static jz_err cdc_ranges(u32 len, u32 max, CdcRollFn roll_fn, CdcCutFn cut, CdcSettleFn settle, void *ctx, RangeVec *out) {
    RangeVec ranges = {0};
    u32 start = 0;
    while (start < len) {
        u32 hard_end = start + max < len ? start + max : len;
        u64 hash = 0;
        u32 end = hard_end;
        for (u32 offset = 0; start + offset < hard_end; offset++) {
            hash = roll_fn(ctx, hash, start + offset);
            if (cut(hash, offset + 1)) { end = start + offset + 1; break; }
        }
        jz_err e = settle(ctx, start, end, &end);
        if (e.code) { jz_vec_drop(&ranges, sizeof(Range), NULL); return e; }
        Range r = {start, end};
        JZ_VEC_PUSH(ranges, r);
        start = end;
    }
    *out = ranges;
    return JZ_OK;
}

typedef struct LeafCdc { const u8 *bytes; u32 n; bool text; } LeafCdc;
static u64 leaf_roll(void *ctx, u64 hash, u32 i) { return roll(hash, ((LeafCdc *)ctx)->bytes[i]); }
static bool leaf_cut_fn(u64 hash, u32 length) { return leaf_cut(hash, length); }
static jz_err leaf_settle(void *ctx, u32 start, u32 end, u32 *out) {
    LeafCdc *c = ctx;
    if (c->text) {
        while (end > start && end < c->n && (c->bytes[end] & 0xc0) == 0x80) end--; /* !text.is_char_boundary(end) */
        if (end == start) return ERR(MalformedNode);
    }
    *out = end;
    return JZ_OK;
}

static jz_err leaf_ranges(u8 kind, const u8 *p, u32 n, RangeVec *out) {
    if (n == 0) {
        /* The empty value still has one canonical empty leaf. */
        RangeVec v = {0};
        Range r = {0, 0};
        JZ_VEC_PUSH(v, r);
        *out = v;
        return JZ_OK;
    }
    bool text = kind != LVK_BYTES;
    if (text && !utf8_ok(p, n)) return ERR(InvalidUtf8);
    LeafCdc ctx = {p, n, text};
    return cdc_ranges(n, LEAF_MAX_BYTES, leaf_roll, leaf_cut_fn, leaf_settle, &ctx, out);
}

typedef struct BranchCdc { const BuiltNode *nodes; u8 kind; } BranchCdc;
static u64 branch_roll(void *ctx, u64 hash, u32 i) { BranchCdc *c = ctx; return roll_child(hash, c->kind, &c->nodes[i]); }
static bool branch_cut_fn(u64 hash, u32 count) { return branch_cut(hash, count); }
static jz_err branch_settle(void *ctx, u32 start, u32 end, u32 *out) { (void)ctx; (void)start; *out = end; return JZ_OK; }

static jz_err branch_ranges(u8 kind, const BuiltNode *nodes, u32 n, RangeVec *out) {
    BranchCdc ctx = {nodes, kind};
    return cdc_ranges(n, BRANCH_MAX_CHILDREN, branch_roll, branch_cut_fn, branch_settle, &ctx, out);
}

static u64 gear(u8 byte);

/* One step of the V1 gear rolling hash. */
static u64 roll(u64 hash, u8 byte) { return (hash << 1) + gear(byte); }

/* Roll one child's kind-bound grouping hash into a branch boundary hash. */
static u64 roll_child(u64 hash, u8 kind, const BuiltNode *node) {
    groove_ContentHash g = grouping_hash_from_logical(FORMAT_VERSION, kind, node->structural_hash);
    for (u32 i = 0; i < 32; i++) hash = roll(hash, g.b[i]);
    return hash;
}

static bool leaf_cut(u64 hash, u32 length) {
    u64 mask = length < LEAF_TARGET_BYTES ? GROOVE_LV_LEAF_SHORT_MASK : GROOVE_LV_LEAF_LONG_MASK;
    return length >= LEAF_MIN_BYTES && (hash & mask) == 0;
}

static bool branch_cut(u64 hash, u32 count) {
    u64 mask = count < BRANCH_TARGET_CHILDREN ? GROOVE_LV_BRANCH_SHORT_MASK : GROOVE_LV_BRANCH_LONG_MASK;
    return count >= BRANCH_MIN_CHILDREN && (hash & mask) == 0;
}

/* V1's gear table is generated by this frozen SplitMix64 parameterization. */
static u64 gear(u8 byte) {
    const u64 V1_FASTCDC_GEAR_ADD = 0x9e3779b97f4a7c15ull;
    const u64 V1_FASTCDC_GEAR_MUL_1 = 0xbf58476d1ce4e5b9ull;
    const u64 V1_FASTCDC_GEAR_MUL_2 = 0x94d049bb133111ebull;
    u64 value = (u64)byte + V1_FASTCDC_GEAR_ADD;
    value = (value ^ (value >> 30)) * V1_FASTCDC_GEAR_MUL_1;
    value = (value ^ (value >> 27)) * V1_FASTCDC_GEAR_MUL_2;
    return value ^ (value >> 31);
}
