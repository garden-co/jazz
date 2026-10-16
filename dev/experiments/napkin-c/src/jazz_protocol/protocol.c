/* jazz_protocol::protocol (crates/jazz/layers/protocol/src/protocol.rs)
 *
 * Written in parts:
 *   1. shared helpers, canonical byte writers, policy keys, read-view keys
 *   2. VersionRecord, the JVRR v2 wire-row codec and its descriptor proof cache
 *   3. version bundles / runs / carriers and SyncMessage validation
 *   4. derived Clone/Eq/Ord/Debug/Drop of result-member and program-fact entries
 *   5. schema versions, physical identity manifests, lineage publications, lenses
 *   6. postcard Serialize/Deserialize of SyncMessage and every nested type
 */
#include "jazz_protocol/protocol.h"
#include "jazz_protocol/wire.h"

/* =====================================================================
 * Part 1: helpers
 * ===================================================================== */
static void str_drop_v(void *p) { jz_str_drop((jz_str *)p); }
static void str_clone_v(void *d, const void *s) { *(jz_str *)d = jz_str_clone((const jz_str *)s); }
static void bytes_drop_v(void *p) { jz_bytes_drop((jz_bytes *)p); }
static void bytes_clone_v(void *d, const void *s) { *(jz_bytes *)d = jz_bytes_clone((const jz_bytes *)s); }
static void value_clone_v(void *d, const void *s) { groove__records__values__Value__Clone__clone((groove_Value *)d, (const groove_Value *)s); }
static void uuid_vec_drop_v(void *p) { jz_vec_drop(p, sizeof(jz_uuid), NULL); }
static void uuid_vec_clone_v(void *d, const void *s) { jz_vec_clone(d, s, sizeof(jz_uuid), NULL); }
static int cmp_uuid_v(const void *a, const void *b, void *ctx) { (void)ctx; return jz_uuid_cmp(a, b); }
static bool value_eq_v(const void *a, const void *b) { return groove__records__values__Value__PartialEq__eq(a, b); }

const jz_btree_desc jazz_protocol__protocol__DESC_STR_SET = { sizeof(jz_str), 0, jz_cmp_str, str_drop_v, NULL };
static const jz_btree_desc DESC_UUID_SET = { sizeof(jz_uuid), 0, cmp_uuid_v, NULL, NULL };
static const jz_btree_desc DESC_USIZE_SET = { sizeof(u32), 0, jz_cmp_u32, NULL, NULL };
static const jz_btree_desc DESC_STR_USIZE = { sizeof(jz_str), sizeof(u32), jz_cmp_str, str_drop_v, NULL };
static const jz_btree_desc DESC_STR_UUIDVEC = { sizeof(jz_str), sizeof(jazz_protocol_UuidVec), jz_cmp_str, str_drop_v, uuid_vec_drop_v };
static const jz_btree_desc DESC_STR_OPTSTR = { sizeof(jz_str), sizeof(jazz_protocol_OptStr), jz_cmp_str, str_drop_v, NULL };

static inline jz_slice istr(jazz_protocol_InternStr s) { return jz_str_slice(s); }
static inline bool slice_eq(jz_slice a, jz_slice b) { return a.len == b.len && memcmp(a.ptr, b.ptr, a.len) == 0; }
static inline bool slice_eq_cstr(jz_slice a, const char *b) { u32 n = (u32)strlen(b); return a.len == n && memcmp(a.ptr, b, n) == 0; }
static inline int u64_cmp(u64 a, u64 b) { return a < b ? -1 : a > b; }
static inline int bool_cmp(bool a, bool b) { return (int)a - (int)b; }
static jz_err msg_err(const char *m) { return jz_err_new(JAZZ_PROTO_ERR_MSG, m); }
static jz_err display_err(jz_err inner) {
    /* map_err(|error| error.to_string()) */
    jz_str s = {0};
    groove__records__Error__Display__fmt(&s, &inner);
    jz_err e = jz_err_fmt(JAZZ_PROTO_ERR_MSG, "%.*s", (int)s.len, s.ptr);
    jz_str_drop(&s);
    jz_err_drop(&inner);
    return e;
}
static jz_err serde_custom(jz_err inner) {
    /* serde::{ser,de}::Error::custom(inner) */
    if (inner.msg) {
        jz_err e = jz_err_fmt(JAZZ_PROTO_ERR_SERDE_CUSTOM, "%.*s", (int)inner.msg->len, inner.msg->ptr);
        jz_err_drop(&inner);
        return e;
    }
    inner.code = JAZZ_PROTO_ERR_SERDE_CUSTOM;
    return inner;
}

static void push_le16(jz_bytes *b, u16 v) { jz_bytes_push(b, (const u8 *)&v, 2); }
static void push_le32(jz_bytes *b, u32 v) { jz_bytes_push(b, (const u8 *)&v, 4); }
static void push_le64(jz_bytes *b, u64 v) { jz_bytes_push(b, (const u8 *)&v, 8); }

static void put_len(jz_bytes *bytes, usize len) {
    if (len > 0xffffffffu) jz_panic("canonical lens component exceeds u32");
    push_le32(bytes, (u32)len);
}
static void put_bytes(jz_bytes *bytes, const u8 *p, u32 n) {
    put_len(bytes, n);
    jz_bytes_push(bytes, p, n);
}
static void put_str(jz_bytes *bytes, jz_slice s) { put_bytes(bytes, s.ptr, s.len); }
static void put_values(jz_bytes *bytes, const groove_ValueVec *values);

void jazz_protocol__protocol__put_value(jz_bytes *bytes, const groove_Value *value) {
    switch (value->tag) {
    case GROOVE_V_U8: jz_bytes_push_u8(bytes, 0); jz_bytes_push_u8(bytes, value->u.u8_); break;
    case GROOVE_V_U16: jz_bytes_push_u8(bytes, 1); push_le16(bytes, value->u.u16_); break;
    case GROOVE_V_U32: jz_bytes_push_u8(bytes, 2); push_le32(bytes, value->u.u32_); break;
    case GROOVE_V_U64: jz_bytes_push_u8(bytes, 3); push_le64(bytes, value->u.u64_); break;
    case GROOVE_V_I32: jz_bytes_push_u8(bytes, 14); push_le32(bytes, (u32)value->u.i32_); break;
    case GROOVE_V_I64: jz_bytes_push_u8(bytes, 13); push_le64(bytes, (u64)value->u.i64_); break;
    case GROOVE_V_F64: { u64 bits; memcpy(&bits, &value->u.f64_, 8); jz_bytes_push_u8(bytes, 4); push_le64(bytes, bits); break; }
    case GROOVE_V_BOOL: jz_bytes_push_u8(bytes, 5); jz_bytes_push_u8(bytes, value->u.b ? 1 : 0); break;
    case GROOVE_V_STRING: jz_bytes_push_u8(bytes, 6); put_str(bytes, jz_str_slice(&value->u.s)); break;
    case GROOVE_V_BYTES: jz_bytes_push_u8(bytes, 7); put_bytes(bytes, value->u.bytes.ptr, value->u.bytes.len); break;
    case GROOVE_V_UUID: jz_bytes_push_u8(bytes, 8); jz_bytes_push(bytes, value->u.uuid.b, 16); break;
    case GROOVE_V_ENUM_TAG: jz_bytes_push_u8(bytes, 9); jz_bytes_push_u8(bytes, value->u.enum_tag); break;
    case GROOVE_V_TUPLE: jz_bytes_push_u8(bytes, 10); put_values(bytes, &value->u.items); break;
    case GROOVE_V_ARRAY: jz_bytes_push_u8(bytes, 11); put_values(bytes, &value->u.items); break;
    case GROOVE_V_NULLABLE:
        jz_bytes_push_u8(bytes, 12);
        if (value->u.nullable) { jz_bytes_push_u8(bytes, 1); jazz_protocol__protocol__put_value(bytes, value->u.nullable); }
        else jz_bytes_push_u8(bytes, 0);
        break;
    case GROOVE_V_RECORD: {
        /* Policy comparison keys retain the complete portable record. */
        jz_bytes desc = {0};
        jz_err e = groove__records__values__encode_record_descriptor(value->u.record.descriptor, &desc);
        if (e.code) jz_panic("admitted policy record descriptor has canonical encoding");
        jz_bytes_push_u8(bytes, 16);
        put_bytes(bytes, desc.ptr, desc.len);
        put_bytes(bytes, value->u.record.raw.ptr, value->u.record.raw.len);
        jz_bytes_drop(&desc);
        break;
    }
    case GROOVE_V_ENUM:
        jz_panic("union-valued values are an internal Groove representation, not a Jazz protocol value");
    case GROOVE_V_U48:
        jz_panic("U48 is an engine-owned stamp width, not a Jazz protocol value");
    case GROOVE_V_LARGE: {
        jz_bytes enc = {0};
        jz_bytes_push_u8(bytes, 15);
        jz_err e = groove__large_values__encode_stored_scalar_chunked(
            groove__large_values__LargeValueRef__kind(value->u.large), value->u.large, &enc);
        if (e.code) jz_panic("admitted large descriptor has canonical encoding");
        put_bytes(bytes, enc.ptr, enc.len);
        jz_bytes_drop(&enc);
        break;
    }
    default: JZ_UNREACHABLE();
    }
}
static void put_values(jz_bytes *bytes, const groove_ValueVec *values) {
    put_len(bytes, values->len);
    for (u32 i = 0; i < values->len; i++) jazz_protocol__protocol__put_value(bytes, &values->ptr[i]);
}

void jazz_protocol__protocol__put_branch_selector(jz_bytes *bytes, const jazz_model_BranchSelector *selector) {
    put_len(bytes, selector->values.len);
    jz_str *name; jz_bytes *value;
    JZ_BTREE_FOR(&selector->values, name, value) {
        put_str(bytes, jz_str_slice(name));
        put_bytes(bytes, value->ptr, value->len);
    }
}

void jazz_protocol__protocol__put_snapshot_ref(jz_bytes *bytes, const jazz_model_SnapshotRef *snapshot) {
    jz_bytes_push(bytes, snapshot->owner.b, 16);
    push_le64(bytes, snapshot->global_base);
    push_le64(bytes, snapshot->local_base);
    /* Dots are a frontier set: JRVK commits to the sorted unique TxId set. */
    u32 n = snapshot->dots.len;
    jazz_model_TxId *dots = jz_memdup(snapshot->dots.ptr, (usize)n * sizeof(jazz_model_TxId));
    jz_sort(dots, n, sizeof(jazz_model_TxId), jazz_model__tx__TxId__cmp, NULL);
    u32 uniq = 0;
    for (u32 i = 0; i < n; i++)
        if (uniq == 0 || jazz_model__tx__TxId__cmp(&dots[uniq - 1], &dots[i], NULL) != 0) dots[uniq++] = dots[i];
    put_len(bytes, uniq);
    for (u32 i = 0; i < uniq; i++) {
        push_le64(bytes, dots[i].time);
        jz_bytes_push(bytes, dots[i].node.b, 16);
    }
    jz_free(dots);
}

/* ---------------- CanonicalPolicyClaims / PolicyBindingKey ---------------- */
void jazz_protocol__protocol__CanonicalPolicyClaims__new(jazz_protocol_CanonicalPolicyClaims *out, jazz_protocol_ValueMap claims) {
    jz_bytes key = {0};
    jz_str *name; groove_Value *value;
    JZ_BTREE_FOR(&claims, name, value) {
        put_str(&key, jz_str_slice(name));
        jazz_protocol__protocol__put_value(&key, value);
    }
    out->claims = claims;
    out->comparison_key = key;
}
bool jazz_protocol__protocol__CanonicalPolicyClaims__PartialEq__eq(const jazz_protocol_CanonicalPolicyClaims *a, const jazz_protocol_CanonicalPolicyClaims *b) {
    return jz_bytes_eq(&a->comparison_key, &b->comparison_key);
}
static int CanonicalPolicyClaims_cmp(const jazz_protocol_CanonicalPolicyClaims *a, const jazz_protocol_CanonicalPolicyClaims *b) {
    return jz_bytes_cmp(&a->comparison_key, &b->comparison_key);
}
static void CanonicalPolicyClaims_clone(jazz_protocol_CanonicalPolicyClaims *dst, const jazz_protocol_CanonicalPolicyClaims *src) {
    jz_btree_clone(&dst->claims, &src->claims, str_clone_v, value_clone_v);
    dst->comparison_key = jz_bytes_clone(&src->comparison_key);
}
static void CanonicalPolicyClaims_drop(jazz_protocol_CanonicalPolicyClaims *c) {
    jz_btree_drop(&c->claims);
    jz_bytes_drop(&c->comparison_key);
}
static void value_map_clone(jazz_protocol_ValueMap *dst, const jazz_protocol_ValueMap *src) {
    jz_btree_clone(dst, src, str_clone_v, value_clone_v);
}

void jazz_protocol__protocol__PolicyBindingKey__from_delegated_session(jazz_protocol_PolicyBindingKey *out, const jazz_protocol_DelegatedSessionBinding *session) {
    jazz_protocol_ValueMap claims;
    value_map_clone(&claims, &session->claims);
    out->identity = session->identity;
    jazz_protocol__protocol__CanonicalPolicyClaims__new(&out->canonical_claims, claims);
}
void jazz_protocol__protocol__PolicyBindingKey__from_canonical_parts(jazz_protocol_PolicyBindingKey *out, jazz_types_AuthorSubject identity, jazz_protocol_ValueMap claims) {
    out->identity = identity;
    jazz_protocol__protocol__CanonicalPolicyClaims__new(&out->canonical_claims, claims);
}
void jazz_protocol__protocol__PolicyBindingKey__Clone__clone(jazz_protocol_PolicyBindingKey *dst, const jazz_protocol_PolicyBindingKey *src) {
    dst->identity = src->identity;
    CanonicalPolicyClaims_clone(&dst->canonical_claims, &src->canonical_claims);
}
void jazz_protocol__protocol__PolicyBindingKey__drop(jazz_protocol_PolicyBindingKey *k) { CanonicalPolicyClaims_drop(&k->canonical_claims); }
int jazz_protocol__protocol__PolicyBindingKey__Ord__cmp(const jazz_protocol_PolicyBindingKey *a, const jazz_protocol_PolicyBindingKey *b) {
    int c = jazz_types__ids__AuthorSubject__Ord__cmp(&a->identity, &b->identity);
    return c ? c : CanonicalPolicyClaims_cmp(&a->canonical_claims, &b->canonical_claims);
}
static bool PolicyBindingKey_eq(const jazz_protocol_PolicyBindingKey *a, const jazz_protocol_PolicyBindingKey *b) {
    return jazz_types_AuthorSubject_eq(&a->identity, &b->identity) &&
           jazz_protocol__protocol__CanonicalPolicyClaims__PartialEq__eq(&a->canonical_claims, &b->canonical_claims);
}

jz_err jazz_protocol__protocol__PolicyBindingKey__directory_value(const jazz_protocol_PolicyBindingKey *k, groove_Value *out) {
    jz_btree empty, derived;
    jz_btree_init(&empty, &jazz_model__btree_desc_str_value);
    jazz_model__model__policy_claims__canonical_policy_binding_claims(&k->identity, &empty, &derived);
    jazz_protocol_ValueMap claims;
    value_map_clone(&claims, &k->canonical_claims.claims);
    u8 presence = 0;
    u32 bit = 0;
    /* Bits use the eight derived names' canonical UTF-8 ordering. */
    jz_str *name; groove_Value *expected;
    JZ_BTREE_FOR(&derived, name, expected) {
        groove_Value *have = jz_btree_get(&claims, name);
        if (have && groove__records__values__Value__PartialEq__eq(have, expected)) {
            jz_str k_out; groove_Value v_out;
            jz_btree_remove(&claims, name, &k_out, &v_out);
            jz_str_drop(&k_out);
            groove__records__values__Value__drop(&v_out);
            presence |= (u8)(1u << bit);
        }
        bit++;
    }
    jz_btree_drop(&derived);
    groove_Value claims_value;
    jz_err e = jazz_model__model__policy_directory__policy_binding_directory_claims_value(&claims, &claims_value);
    jz_btree_drop(&claims);
    JZ_TRY(e);
    return jazz_model__model__policy_directory__policy_directory_payload(presence, claims_value, out);
}

jz_err jazz_protocol__protocol__PolicyBindingKey__from_directory_value(jazz_types_AuthorSubject identity, groove_Value value, jazz_protocol_PolicyBindingKey *out) {
    jz_err e;
    if (value.tag != GROOVE_V_RECORD) {
        groove__records__values__Value__drop(&value);
        return msg_err("policy directory payload must be record");
    }
    groove_OwnedRecord *record = &value.u.record;
    if (record->descriptor != jazz_model__model__policy_directory__policy_directory_descriptor()) {
        groove__records__values__Value__drop(&value);
        return msg_err("policy directory descriptor mismatch");
    }
    groove_Value presence_v;
    e = groove__records__OwnedRecord__get_idx(record, 0, &presence_v);
    if (e.code) { groove__records__values__Value__drop(&value); return display_err(e); }
    if (presence_v.tag != GROOVE_V_U8) {
        groove__records__values__Value__drop(&presence_v);
        groove__records__values__Value__drop(&value);
        return msg_err("policy directory presence must be u8");
    }
    u8 presence = presence_v.u.u8_;
    groove_Value inner;
    e = groove__records__OwnedRecord__get_idx(record, 1, &inner);
    groove__records__values__Value__drop(&value);
    if (e.code) return display_err(e);
    jazz_protocol_ValueMap claims;
    JZ_TRY(jazz_model__model__policy_directory__policy_binding_directory_claims_from_value(inner, &claims));
    jz_btree empty, derived;
    jz_btree_init(&empty, &jazz_model__btree_desc_str_value);
    jazz_model__model__policy_claims__canonical_policy_binding_claims(&identity, &empty, &derived);
    u32 bit = 0;
    jz_str *name; groove_Value *expected;
    JZ_BTREE_FOR(&derived, name, expected) {
        if (presence & (1u << bit)) {
            jz_str k = jz_str_clone(name);
            groove_Value v;
            groove__records__values__Value__Clone__clone(&v, expected);
            groove_Value old;
            if (jz_btree_insert(&claims, &k, &v, &old)) {
                groove__records__values__Value__drop(&old);
                jz_btree_drop(&derived);
                jz_btree_drop(&claims);
                return msg_err("policy directory contains a redundant identity-derived claim");
            }
        }
        bit++;
    }
    jz_btree_drop(&derived);
    jazz_protocol__protocol__PolicyBindingKey__from_canonical_parts(out, identity, claims);
    return JZ_OK;
}

void jazz_protocol__protocol__PolicyBindingKey__directory_digest(const jazz_protocol_PolicyBindingKey *k, u8 out[32]) {
    jz_bytes exact = {0};
    put_str(&exact, jazz_types__ids__AuthorSubject__canonical(&k->identity));
    jz_bytes_push(&exact, k->canonical_claims.comparison_key.ptr, k->canonical_claims.comparison_key.len);
    /* blake3::derive_key(context, material) */
    static const char ctx[] = "jazz.authority-policy-binding-directory.v1";
    jz_blake3_hasher *h = jz_blake3_new();
    jz_blake3_update(h, (const u8 *)ctx, sizeof(ctx) - 1);
    jz_blake3_update(h, exact.ptr, exact.len);
    jz_blake3_finalize(h, out);
    jz_blake3_free(h);
    jz_bytes_drop(&exact);
}

void jazz_protocol__protocol__AuthorityResultKey__Clone__clone(jazz_protocol_AuthorityResultKey *dst, const jazz_protocol_AuthorityResultKey *src) {
    dst->binding_view = src->binding_view;
    dst->has_policy_binding = src->has_policy_binding;
    if (src->has_policy_binding) jazz_protocol__protocol__PolicyBindingKey__Clone__clone(&dst->policy_binding, &src->policy_binding);
}

/* ---------------- Debug for keys ---------------- */
void jazz_protocol__protocol__ReadViewKey__Debug__fmt(jz_str *out, const jazz_protocol_ReadViewKey *k) {
    char u[37]; jz_uuid_to_string(&k->id, u);
    jz_str_appendf(out, "ReadViewKey { id: %s }", u);
}
static void key3_debug(jz_str *out, const char *name, const jazz_protocol_SubscriptionKey *k) {
    char a[37], b[37];
    jz_uuid_to_string(&k->shape_id.u, a);
    jz_uuid_to_string(&k->binding_id.u, b);
    jz_str_appendf(out, "%s { shape_id: ShapeId(%s), binding_id: BindingId(%s), read_view: ", name, a, b);
    jazz_protocol__protocol__ReadViewKey__Debug__fmt(out, &k->read_view);
    jz_str_push_cstr(out, " }");
}
void jazz_protocol__protocol__SubscriptionKey__Debug__fmt(jz_str *out, const jazz_protocol_SubscriptionKey *k) { key3_debug(out, "SubscriptionKey", k); }
void jazz_protocol__protocol__BindingViewKey__Debug__fmt(jz_str *out, const jazz_protocol_BindingViewKey *k) { key3_debug(out, "BindingViewKey", k); }
static void PolicyBindingKey_debug(jz_str *out, const jazz_protocol_PolicyBindingKey *k) {
    jz_str_push_cstr(out, "PolicyBindingKey { identity: ");
    jazz_types__ids__AuthorSubject__Debug__fmt(out, &k->identity);
    jz_str_push_cstr(out, ", canonical_claims: CanonicalPolicyClaims { claims: {");
    bool first = true;
    jz_str *name; groove_Value *value;
    JZ_BTREE_FOR(&k->canonical_claims.claims, name, value) {
        jz_str_appendf(out, "%s\"%.*s\": ", first ? "" : ", ", (int)name->len, name->ptr);
        groove__records__values__Value__Debug__fmt(out, value);
        first = false;
    }
    jz_str_appendf(out, "}, comparison_key: [%u bytes] } }", k->canonical_claims.comparison_key.len);
}
void jazz_protocol__protocol__AuthorityResultKey__Debug__fmt(jz_str *out, const jazz_protocol_AuthorityResultKey *k) {
    jz_str_push_cstr(out, "AuthorityResultKey { binding_view: ");
    jazz_protocol__protocol__BindingViewKey__Debug__fmt(out, &k->binding_view);
    jz_str_push_cstr(out, ", policy_binding: ");
    if (k->has_policy_binding) { jz_str_push_cstr(out, "Some("); PolicyBindingKey_debug(out, &k->policy_binding); jz_str_push_cstr(out, ")"); }
    else jz_str_push_cstr(out, "None");
    jz_str_push_cstr(out, " }");
}

/* ---------------- RegisterShapeOptions / ReadViewKey ---------------- */
static const jz_uuid READ_VIEW_NAMESPACE = {{0x1a,0x87,0xcf,0x70,0xf8,0xf0,0x5a,0xe7,0xa5,0x74,0x1f,0x9b,0x5e,0x45,0x17,0xf1}};
static const jz_uuid MIGRATION_LENS_NAMESPACE = {{0x5d,0x13,0xf9,0xcb,0x8a,0x10,0x5e,0x0f,0x9a,0x58,0xe5,0x66,0x30,0xa1,0xdc,0x22}};
static const jz_uuid SCHEMA_LINEAGE_PUBLICATION_NAMESPACE = {{0xa1,0xb3,0xff,0x15,0x93,0x58,0x52,0xe0,0xba,0xa8,0xf3,0x84,0xb1,0xd5,0xdb,0x1c}};

static void ReadViewSourceSpec_clone(jazz_protocol_ReadViewSourceSpec *dst, const jazz_protocol_ReadViewSourceSpec *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    switch (src->tag) {
    case JAZZ_RVS_CURRENT: break;
    case JAZZ_RVS_BRANCH_VIEW:
        jazz_model__model__branch__BranchSelector__Clone__clone(&dst->head, &src->head);
        dst->has_base = src->has_base;
        if (src->has_base) jazz_model__model__branch__BranchViewBase__Clone__clone(&dst->base, &src->base);
        break;
    case JAZZ_RVS_SNAPSHOT: jazz_model__model__branch__SnapshotRef__Clone__clone(&dst->snapshot, &src->snapshot); break;
    }
}
static void ReadViewSourceSpec_drop(jazz_protocol_ReadViewSourceSpec *s) {
    switch (s->tag) {
    case JAZZ_RVS_BRANCH_VIEW:
        jazz_model__model__branch__BranchSelector__drop(&s->head);
        if (s->has_base) jazz_model__model__branch__BranchViewBase__drop(&s->base);
        break;
    case JAZZ_RVS_SNAPSHOT: jazz_model__model__branch__SnapshotRef__drop(&s->snapshot); break;
    default: break;
    }
}
static int ReadViewSourceSpec_cmp(const jazz_protocol_ReadViewSourceSpec *a, const jazz_protocol_ReadViewSourceSpec *b) {
    if (a->tag != b->tag) return a->tag < b->tag ? -1 : 1;
    switch (a->tag) {
    case JAZZ_RVS_BRANCH_VIEW: {
        int c = jazz_model__model__branch__BranchSelector__Ord__cmp(&a->head, &b->head);
        if (c) return c;
        if (a->has_base != b->has_base) return bool_cmp(a->has_base, b->has_base);
        return a->has_base ? jazz_model__model__branch__BranchViewBase__Ord__cmp(&a->base, &b->base) : 0;
    }
    case JAZZ_RVS_SNAPSHOT: return jazz_model__model__branch__SnapshotRef__Ord__cmp(&a->snapshot, &b->snapshot);
    default: return 0;
    }
}
void jazz_protocol__protocol__RegisterShapeOptions__default(jazz_protocol_RegisterShapeOptions *out) {
    memset(out, 0, sizeof *out);
    out->tier = JAZZ_DUR_GLOBAL; /* default_register_shape_tier */
    out->read_view.source.tag = JAZZ_RVS_CURRENT;
    out->binding_source = JAZZ_BINDING_ORDINARY;
}
void jazz_protocol__protocol__RegisterShapeOptions__Clone__clone(jazz_protocol_RegisterShapeOptions *dst, const jazz_protocol_RegisterShapeOptions *src) {
    dst->tier = src->tier;
    ReadViewSourceSpec_clone(&dst->read_view.source, &src->read_view.source);
    dst->binding_source = src->binding_source;
}
void jazz_protocol__protocol__RegisterShapeOptions__drop(jazz_protocol_RegisterShapeOptions *o) { ReadViewSourceSpec_drop(&o->read_view.source); }
static int RegisterShapeOptions_cmp(const jazz_protocol_RegisterShapeOptions *a, const jazz_protocol_RegisterShapeOptions *b) {
    if (a->tier != b->tier) return a->tier < b->tier ? -1 : 1;
    int c = ReadViewSourceSpec_cmp(&a->read_view.source, &b->read_view.source);
    if (c) return c;
    return a->binding_source == b->binding_source ? 0 : (a->binding_source < b->binding_source ? -1 : 1);
}
bool jazz_protocol__protocol__ReadViewSpec__is_default(const jazz_protocol_ReadViewSpec *s) {
    return s->source.tag == JAZZ_RVS_CURRENT;
}
static void put_read_view_source(jz_bytes *bytes, const jazz_protocol_ReadViewSourceSpec *source) {
    switch (source->tag) {
    case JAZZ_RVS_CURRENT: jz_bytes_push_u8(bytes, 0); break;
    case JAZZ_RVS_BRANCH_VIEW:
        jz_bytes_push_u8(bytes, 1);
        jazz_protocol__protocol__put_branch_selector(bytes, &source->head);
        if (!source->has_base) jz_bytes_push_u8(bytes, 0);
        else if (source->base.tag == JAZZ_BVB_CURRENT) {
            jz_bytes_push_u8(bytes, 1);
            jazz_protocol__protocol__put_branch_selector(bytes, &source->base.branch);
        } else {
            jz_bytes_push_u8(bytes, 2);
            jazz_protocol__protocol__put_branch_selector(bytes, &source->base.branch);
            jazz_protocol__protocol__put_snapshot_ref(bytes, &source->base.snapshot);
        }
        break;
    case JAZZ_RVS_SNAPSHOT:
        jz_bytes_push_u8(bytes, 2);
        jazz_protocol__protocol__put_snapshot_ref(bytes, &source->snapshot);
        break;
    }
}
static jz_bytes canonical_register_shape_options_v2_bytes(const jazz_protocol_RegisterShapeOptions *options) {
    jz_bytes bytes = {0};
    jz_bytes_push(&bytes, (const u8 *)"JRVK", 4);
    jz_bytes_push_u8(&bytes, 2); /* READ_VIEW_KEY_CODEC_VERSION */
    switch (options->tier) {
    case JAZZ_DUR_NONE: jz_bytes_push_u8(&bytes, 0); break;
    case JAZZ_DUR_LOCAL: jz_bytes_push_u8(&bytes, 1); break;
    default: jz_bytes_push_u8(&bytes, 2); break;
    }
    jz_bytes_push_u8(&bytes, options->binding_source == JAZZ_BINDING_ORDINARY ? 0 : 1);
    put_read_view_source(&bytes, &options->read_view.source);
    return bytes;
}
jazz_protocol_ReadViewKey jazz_protocol__protocol__RegisterShapeOptions__read_view_key(const jazz_protocol_RegisterShapeOptions *opts) {
    /* ReadViewKey::from_register_shape_options: canonical() clones and canonicalizes (no-op) */
    jazz_protocol_RegisterShapeOptions canonical, def;
    jazz_protocol__protocol__RegisterShapeOptions__Clone__clone(&canonical, opts);
    jazz_protocol__protocol__RegisterShapeOptions__default(&def);
    jazz_protocol_ReadViewKey key = {{{0}}};
    if (RegisterShapeOptions_cmp(&canonical, &def) != 0) {
        jz_bytes bytes = canonical_register_shape_options_v2_bytes(&canonical);
        key.id = jz_uuid_v5(&READ_VIEW_NAMESPACE, bytes.ptr, bytes.len);
        jz_bytes_drop(&bytes);
    }
    jazz_protocol__protocol__RegisterShapeOptions__drop(&canonical);
    return key;
}

/* CoverageKey derives Clone/Eq/Ord */
void jazz_protocol__protocol__CoverageKey__Clone__clone(jazz_protocol_CoverageKey *dst, const jazz_protocol_CoverageKey *src) {
    dst->shape_id = src->shape_id;
    dst->binding_id = src->binding_id;
    jazz_protocol__protocol__RegisterShapeOptions__Clone__clone(&dst->opts, &src->opts);
    dst->has_policy_binding = src->has_policy_binding;
    if (src->has_policy_binding) jazz_protocol__protocol__PolicyBindingKey__Clone__clone(&dst->policy_binding, &src->policy_binding);
}
int jazz_protocol__protocol__CoverageKey__Ord__cmp(const jazz_protocol_CoverageKey *a, const jazz_protocol_CoverageKey *b) {
    int c = jz_uuid_cmp(&a->shape_id.u, &b->shape_id.u);
    if (c) return c;
    if ((c = jz_uuid_cmp(&a->binding_id.u, &b->binding_id.u))) return c;
    if ((c = RegisterShapeOptions_cmp(&a->opts, &b->opts))) return c;
    if (a->has_policy_binding != b->has_policy_binding) return bool_cmp(a->has_policy_binding, b->has_policy_binding);
    return a->has_policy_binding ? jazz_protocol__protocol__PolicyBindingKey__Ord__cmp(&a->policy_binding, &b->policy_binding) : 0;
}
bool jazz_protocol__protocol__CoverageKey__PartialEq__eq(const jazz_protocol_CoverageKey *a, const jazz_protocol_CoverageKey *b) {
    if (!jz_uuid_eq(&a->shape_id.u, &b->shape_id.u) || !jz_uuid_eq(&a->binding_id.u, &b->binding_id.u)) return false;
    if (RegisterShapeOptions_cmp(&a->opts, &b->opts) != 0) return false;
    if (a->has_policy_binding != b->has_policy_binding) return false;
    return !a->has_policy_binding || PolicyBindingKey_eq(&a->policy_binding, &b->policy_binding);
}
void jazz_protocol__protocol__CoverageKey__drop(jazz_protocol_CoverageKey *k) {
    jazz_protocol__protocol__RegisterShapeOptions__drop(&k->opts);
    if (k->has_policy_binding) jazz_protocol__protocol__PolicyBindingKey__drop(&k->policy_binding);
}

/* ---------------- ShapeAst ---------------- */
void jazz_protocol__protocol__ShapeAst__from_validated(jazz_protocol_ShapeAst *out, const jazz_model_ValidatedQuery *shape) {
    out->version = 0; /* ShapeAst::VERSION */
    out->schema_version = jazz_model__query__validation__ValidatedQuery__schema_version(shape);
    out->body_tag = JAZZ_SHAPE_QUERY;
    out->query = jazz_model__query__ast__Query__Clone__clone(jazz_model__query__validation__ValidatedQuery__query(shape));
    out->relation = NULL;
}
void jazz_protocol__protocol__ShapeAst__drop(jazz_protocol_ShapeAst *a) {
    if (a->body_tag == JAZZ_SHAPE_QUERY) jazz_model__query__ast__Query__drop(a->query);
    else jazz_model__query__ast__RelationQuery__drop(a->relation);
}
static void ShapeAst_clone(jazz_protocol_ShapeAst *dst, const jazz_protocol_ShapeAst *src) {
    *dst = *src;
    if (src->body_tag == JAZZ_SHAPE_QUERY) dst->query = jazz_model__query__ast__Query__Clone__clone(src->query);
    else dst->relation = jazz_model__query__ast__RelationQuery__Clone__clone(src->relation);
}

/* ---------------- SupportingRowsUpdate ---------------- */
void jazz_protocol__protocol__SupportingRowsUpdate__snapshot(jazz_protocol_SupportingRowsUpdate *out, jazz_protocol_SupportingRowVec rows) {
    memset(out, 0, sizeof *out);
    out->tag = JAZZ_SRU_SNAPSHOT;
    jz_uuid r = jz_uuid_v4();
    memcpy(out->revision, r.b, 16);
    out->added = rows;
}

/* =====================================================================
 * Part 2: VersionRecord and version_record_wire_row (JVRR v2)
 * ===================================================================== */
#define JVRR_MAGIC "JVRR\x02"
#define JVRR_MAGIC_LEN 5u
#define MAX_DESCRIPTOR_PROOFS 256u
#define MAX_DESCRIPTOR_PROOF_BYTES (64u * 1024u)
#define MAX_DESCRIPTOR_PROOF_CACHE_BYTES (1024u * 1024u)

/* Arc<[u8]> */
typedef struct ArcBytes { u32 len; u8 data[]; } ArcBytes;
static ArcBytes *arc_bytes_new(const u8 *p, u32 n) {
    ArcBytes *a = jz_rc_new(sizeof(ArcBytes) + n, NULL);
    a->len = n;
    memcpy(a->data, p, n);
    return a;
}
typedef struct DescriptorProof { groove_RecordDescriptor source, canonical; ArcBytes *encoded; } DescriptorProof;
typedef struct ProofEntry { u64 id; DescriptorProof proof; } ProofEntry;
/* by_encoded key: borrowed view of the Arc contents (owner kept for drop) */
typedef struct EncodedKey { const u8 *ptr; u32 len; ArcBytes *owner; } EncodedKey;
static u64 desc_ptr_hash(const void *k) { return jz_hash_u64((u64)(uintptr_t)*(const groove_RecordDescriptor *)k); }
static bool desc_ptr_eq(const void *a, const void *b) { return *(const groove_RecordDescriptor *)a == *(const groove_RecordDescriptor *)b; }
static u64 enc_key_hash(const void *k) { const EncodedKey *e = k; return jz_hash_bytes(e->ptr, e->len); }
static bool enc_key_eq(const void *a, const void *b) {
    const EncodedKey *x = a, *y = b;
    return x->len == y->len && memcmp(x->ptr, y->ptr, x->len) == 0;
}
static void enc_key_drop(void *k) { EncodedKey *e = k; if (e->owner) jz_rc_release(e->owner); }
static const jz_hmap_desc BY_SOURCE_DESC = { sizeof(groove_RecordDescriptor), sizeof(u64), desc_ptr_hash, desc_ptr_eq, NULL, NULL };
static const jz_hmap_desc BY_ENCODED_DESC = { sizeof(EncodedKey), sizeof(u64), enc_key_hash, enc_key_eq, enc_key_drop, NULL };

typedef struct DescriptorProofCache {
    bool init;
    jz_deque entries; /* ProofEntry */
    jz_hmap by_source, by_encoded;
    u64 next_id;
    usize bytes;
} DescriptorProofCache;
/* thread_local! DESCRIPTOR_PROOFS */
static DescriptorProofCache DESCRIPTOR_PROOFS;

static DescriptorProofCache *proofs(void) {
    DescriptorProofCache *c = &DESCRIPTOR_PROOFS;
    if (!c->init) {
        jz_deque_init(&c->entries, sizeof(ProofEntry));
        jz_hmap_init(&c->by_source, &BY_SOURCE_DESC);
        jz_hmap_init(&c->by_encoded, &BY_ENCODED_DESC);
        c->init = true;
    }
    return c;
}
static const DescriptorProof *DescriptorProofCache_get(const DescriptorProofCache *c, u64 id) {
    if (c->entries.len == 0) return NULL;
    u64 front = ((const ProofEntry *)jz_deque_at(&c->entries, 0))->id;
    if (id < front) return NULL;
    u64 index = id - front;
    if (index >= c->entries.len) return NULL;
    return &((const ProofEntry *)jz_deque_at(&c->entries, (u32)index))->proof;
}
static bool DescriptorProofCache_by_source(const DescriptorProofCache *c, groove_RecordDescriptor d, DescriptorProof *out) {
    const u64 *id = jz_hmap_get(&c->by_source, &d);
    if (!id) return false;
    const DescriptorProof *p = DescriptorProofCache_get(c, *id);
    if (!p) return false;
    *out = *p;
    out->encoded = jz_rc_clone(p->encoded);
    return true;
}
static bool DescriptorProofCache_by_encoded(const DescriptorProofCache *c, jz_slice encoded, DescriptorProof *out) {
    /* Consecutive rows usually share a descriptor: compare the newest proof first. */
    if (c->entries.len) {
        const ProofEntry *newest = jz_deque_at(&c->entries, c->entries.len - 1);
        if (newest->proof.encoded->len == encoded.len && memcmp(newest->proof.encoded->data, encoded.ptr, encoded.len) == 0) {
            *out = newest->proof;
            out->encoded = jz_rc_clone(newest->proof.encoded);
            return true;
        }
    }
    EncodedKey key = { encoded.ptr, encoded.len, NULL };
    const u64 *id = jz_hmap_get(&c->by_encoded, &key);
    if (!id) return false;
    const DescriptorProof *p = DescriptorProofCache_get(c, *id);
    if (!p) return false;
    *out = *p;
    out->encoded = jz_rc_clone(p->encoded);
    return true;
}
static void jazz_protocol__protocol__version_record_wire_row__DescriptorProofCache__remember(DescriptorProofCache *c, DescriptorProof proof) {
    /* This is a retention limit, never an input acceptance limit. */
    if (proof.encoded->len > MAX_DESCRIPTOR_PROOF_BYTES) { jz_rc_release(proof.encoded); return; }
    while (c->entries.len >= MAX_DESCRIPTOR_PROOFS || c->bytes + proof.encoded->len > MAX_DESCRIPTOR_PROOF_CACHE_BYTES) {
        ProofEntry evicted;
        if (!jz_deque_pop_front(&c->entries, &evicted)) jz_panic("nonempty bounded cache");
        c->bytes -= evicted.proof.encoded->len;
        const u64 *src_id = jz_hmap_get(&c->by_source, &evicted.proof.source);
        if (src_id && *src_id == evicted.id) jz_hmap_remove(&c->by_source, &evicted.proof.source, NULL, NULL);
        EncodedKey key = { evicted.proof.encoded->data, evicted.proof.encoded->len, NULL };
        const u64 *enc_id = jz_hmap_get(&c->by_encoded, &key);
        if (enc_id && *enc_id == evicted.id) {
            EncodedKey old;
            jz_hmap_remove(&c->by_encoded, &key, &old, NULL);
            enc_key_drop(&old);
        }
        jz_rc_release(evicted.proof.encoded);
    }
    u64 id = c->next_id++;
    c->bytes += proof.encoded->len;
    jz_hmap_insert(&c->by_source, &proof.source, &id, NULL);
    EncodedKey key = { proof.encoded->data, proof.encoded->len, jz_rc_clone(proof.encoded) };
    EncodedKey old_key; u64 old_val;
    if (jz_hmap_insert(&c->by_encoded, &key, &id, &old_val)) {
        /* key equal to an existing one: the map kept the old key; release ours */
        (void)old_key;
        jz_rc_release(key.owner);
    }
    ProofEntry e = { id, proof };
    jz_deque_push_back(&c->entries, &e);
}
static jz_err descriptor_for_encode(groove_RecordDescriptor descriptor, DescriptorProof *out) {
    if (DescriptorProofCache_by_source(proofs(), descriptor, out)) return JZ_OK;
    jz_bytes encoded = {0};
    JZ_TRY(groove__records__values__encode_persisted_record_descriptor(descriptor, &encoded));
    groove_RecordDescriptor canonical;
    jz_err e = groove__records__values__decode_persisted_record_descriptor(jz_bytes_slice(&encoded), &canonical);
    if (e.code) { jz_bytes_drop(&encoded); return e; }
    DescriptorProof proof = { descriptor, canonical, arc_bytes_new(encoded.ptr, encoded.len) };
    jz_bytes_drop(&encoded);
    *out = proof;
    out->encoded = jz_rc_clone(proof.encoded);
    jazz_protocol__protocol__version_record_wire_row__DescriptorProofCache__remember(proofs(), proof);
    return JZ_OK;
}
static jz_err descriptor_for_decode(jz_slice encoded, DescriptorProof *out) {
    if (DescriptorProofCache_by_encoded(proofs(), encoded, out)) return JZ_OK;
    groove_RecordDescriptor canonical;
    JZ_TRY(groove__records__values__decode_persisted_record_descriptor(encoded, &canonical));
    DescriptorProof proof = { canonical, canonical, arc_bytes_new(encoded.ptr, encoded.len) };
    *out = proof;
    out->encoded = jz_rc_clone(proof.encoded);
    jazz_protocol__protocol__version_record_wire_row__DescriptorProofCache__remember(proofs(), proof);
    return JZ_OK;
}
jz_err jazz_protocol__protocol__version_record_wire_row__encode(const groove_OwnedRecord *record, jz_bytes *out) {
    DescriptorProof proof;
    JZ_TRY(descriptor_for_encode(record->descriptor, &proof));
    u32 length = proof.encoded->len;
    jz_bytes bytes = {0};
    jz_vec_reserve(&bytes, JVRR_MAGIC_LEN + 4 + length + record->raw.len, 1);
    jz_bytes_push(&bytes, (const u8 *)JVRR_MAGIC, JVRR_MAGIC_LEN);
    push_le32(&bytes, length);
    jz_bytes_push(&bytes, proof.encoded->data, length);
    jz_bytes_push(&bytes, record->raw.ptr, record->raw.len);
    jz_rc_release(proof.encoded);
    *out = bytes;
    return JZ_OK;
}
jz_err jazz_protocol__protocol__version_record_wire_row__decode(jz_slice bytes, groove_OwnedRecord *out) {
    jz_err invalid = { GROOVE_REC_ERR_NON_CANONICAL_RECORD, NULL };
    if (bytes.len < JVRR_MAGIC_LEN || memcmp(bytes.ptr, JVRR_MAGIC, JVRR_MAGIC_LEN) != 0) return invalid;
    if (bytes.len < 9) return invalid;
    u32 length; memcpy(&length, bytes.ptr + 5, 4);
    u64 end = 9ull + length;
    if (end > bytes.len) return invalid;
    DescriptorProof proof;
    JZ_TRY(descriptor_for_decode(jz_slice_of(bytes.ptr + 9, length), &proof));
    groove_RecordDescriptor descriptor = proof.canonical;
    jz_rc_release(proof.encoded);
    jz_bytes raw = jz_bytes_from(bytes.ptr + end, bytes.len - (u32)end);
    *out = groove__records__OwnedRecord__new(raw, descriptor);
    return JZ_OK;
}
/* serialize_with: serialize_bytes(&encode(record)?) */
static jz_err wire_row_serialize(jz_pc_w *w, const groove_OwnedRecord *record) {
    jz_bytes bytes;
    jz_err e = jazz_protocol__protocol__version_record_wire_row__encode(record, &bytes);
    if (e.code) return serde_custom(e);
    jz_pc_bytes(w, bytes.ptr, bytes.len);
    jz_bytes_drop(&bytes);
    return JZ_OK;
}
/* deserialize_with: RowVisitor::visit_bytes (postcard always hands borrowed bytes) */
static jz_err wire_row_deserialize(jz_pc_r *r, groove_OwnedRecord *out) {
    jz_slice bytes;
    JZ_TRY(jz_pc_r_bytes_ref(r, &bytes));
    jz_err e = jazz_protocol__protocol__version_record_wire_row__decode(bytes, out);
    if (e.code) return serde_custom(e);
    return JZ_OK;
}

/* ---------------- deletion / nullable helpers ---------------- */
static bool deletion_from_value(groove_Value value, bool *some, u32 *out) {
    bool ok = true;
    if (value.tag != GROOVE_V_NULLABLE) ok = false;
    else if (!value.u.nullable) *some = false;
    else if (value.u.nullable->tag == GROOVE_V_ENUM_TAG && value.u.nullable->u.enum_tag == 0) { *some = true; *out = JAZZ_DELETION_DELETED; }
    else if (value.u.nullable->tag == GROOVE_V_ENUM_TAG && value.u.nullable->u.enum_tag == 1) { *some = true; *out = JAZZ_DELETION_RESTORED; }
    else ok = false; /* Err("deletion") */
    groove__records__values__Value__drop(&value);
    return ok;
}
/* NullableValue::nullable_value: Ok(None) / Ok(Some) / Err("nullable value expected") */
static bool nullable_value(groove_Value value, bool *some, groove_Value *out) {
    if (value.tag != GROOVE_V_NULLABLE) { groove__records__values__Value__drop(&value); return false; }
    if (!value.u.nullable) { *some = false; return true; }
    *some = true;
    *out = *value.u.nullable;
    jz_free(value.u.nullable);
    return true;
}

/* ---------------- VersionRecord ---------------- */
jz_slice jazz_protocol__protocol__VersionRecord__table(const jazz_protocol_VersionRecord *v) { return istr(v->table); }

static jz_err malformed(const jazz_protocol_VersionRecord *v) {
    jz_slice t = istr(v->table);
    return jz_err_fmt(JAZZ_PROTO_ERR_MALFORMED_VERSION_RECORD, "version for %.*s has malformed record bytes", (int)t.len, t.ptr);
}
static bool is_user_cell_field(const groove_DescriptorField *field) {
    u32 plen = (u32)strlen(jazz_model__schema__APP_COLUMN_PREFIX);
    return field->has_name && field->name.len >= plen && memcmp(field->name.ptr, jazz_model__schema__APP_COLUMN_PREFIX, plen) == 0;
}
jz_err jazz_protocol__protocol__VersionRecord__validate_receipt(const jazz_protocol_VersionRecord *self) {
    static const char *const fixed_names[6] = { "row_uuid", "created_by", "created_at", "updated_by", "updated_at", "_deletion" };
    u32 nfields;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(self->record.descriptor, &nfields);
    if (nfields < 6) return malformed(self);
    for (u32 i = 0; i < 6; i++)
        if (!fields[i].has_name || !jz_str_eq_cstr(&fields[i].name, fixed_names[i])) return malformed(self);
    for (u32 i = JAZZ_WRR_USER_CELLS; i < nfields; i++)
        if (!is_user_cell_field(&fields[i])) return malformed(self);
    groove_BorrowedRecord borrowed = groove_OwnedRecord_borrowed(&self->record);
    jz_uuid row_uuid;
    jz_err e = groove__records__BorrowedRecord__get_uuid(&borrowed, JAZZ_WRR_ROW_UUID, &row_uuid);
    if (e.code) { jz_err_drop(&e); return malformed(self); }
    static const u32 author_fields[2] = { JAZZ_WRR_CREATED_BY, JAZZ_WRR_UPDATED_BY };
    for (u32 i = 0; i < 2; i++) {
        groove_BorrowedRecord sub;
        e = groove__records__BorrowedRecord__get_record(&borrowed, author_fields[i], &sub);
        if (e.code) { jz_err_drop(&e); return malformed(self); }
        jazz_types_RowAuthor author;
        e = jazz_types__ids__RowAuthor__from_record(sub, &author);
        if (e.code) { jz_err_drop(&e); return malformed(self); }
    }
    u64 at;
    e = groove__records__BorrowedRecord__get_u64(&borrowed, JAZZ_WRR_CREATED_AT, &at);
    if (e.code) { jz_err_drop(&e); return malformed(self); }
    e = groove__records__BorrowedRecord__get_u64(&borrowed, JAZZ_WRR_UPDATED_AT, &at);
    if (e.code) { jz_err_drop(&e); return malformed(self); }
    groove_Value dv;
    e = groove__records__BorrowedRecord__get_idx(&borrowed, JAZZ_WRR_DELETION, &dv);
    if (e.code) { jz_err_drop(&e); return malformed(self); }
    bool some; u32 del;
    if (!deletion_from_value(dv, &some, &del)) return malformed(self);
    /* typed = WireRowRecord::new(record.clone()); typed.cell(index)? for each user cell */
    groove_OwnedRecord typed;
    groove__records__OwnedRecord__Clone__clone(&typed, &self->record);
    for (u32 index = 0; index < nfields - JAZZ_WRR_USER_CELLS; index++) {
        groove_Value cell;
        e = groove__records__OwnedRecord__get_idx(&typed, JAZZ_WRR_USER_CELLS + index, &cell);
        if (e.code) { jz_err_drop(&e); groove__records__OwnedRecord__drop(&typed); return malformed(self); }
        groove__records__values__Value__drop(&cell);
    }
    groove__records__OwnedRecord__drop(&typed);
    /* Decode every descriptor field, then reproduce the record. */
    groove_Value *values = jz_alloc((usize)nfields * sizeof(groove_Value));
    u32 got = 0;
    jz_err result = JZ_OK;
    for (; got < nfields; got++) {
        e = groove__records__BorrowedRecord__get_idx(&borrowed, got, &values[got]);
        if (e.code) { jz_err_drop(&e); result = malformed(self); break; }
    }
    if (!result.code) {
        jz_bytes canonical = {0};
        e = groove__records__RecordDescriptor__create(self->record.descriptor, values, nfields, &canonical);
        if (e.code) { jz_err_drop(&e); result = malformed(self); }
        else if (!jz_bytes_eq(&canonical, &self->record.raw)) result = malformed(self);
        jz_bytes_drop(&canonical);
    }
    for (u32 i = 0; i < got; i++) groove__records__values__Value__drop(&values[i]);
    jz_free(values);
    (void)row_uuid;
    return result;
}

void jazz_protocol__protocol__VersionRecord__new(jazz_protocol_VersionRecord *out, jz_str table, jazz_types_SchemaVersionId sv, groove_OwnedRecord record) {
    memset(out, 0, sizeof *out);
    out->table = groove__Intern__new_str(table);
    out->schema_version = sv;
    out->record = record;
    jz_btree_init(&out->authored_columns, &jazz_protocol__protocol__DESC_STR_SET);
}

jz_err jazz_protocol__protocol__VersionRecord__encode(const jazz_model_TableSchema *table, jazz_types_SchemaVersionId schema_version,
    jazz_types_RowUuid row_uuid, jazz_types_AuthorSubject created_by, u64 created_at_ms, jazz_types_AuthorSubject updated_by,
    u64 updated_at_ms, const groove_Value *const *cells_by_position, u32 n_cells, bool has_deletion, u32 deletion,
    jazz_protocol_VersionRecord *out) {
    /* This path is for data birth only; stored rows project to wire bytes without decoding. */
    groove_RecordDescriptor descriptor = jazz_model__schema__TableSchema__wire_record_descriptor(table);
    jazz_types_RowAuthor created, updated;
    jz_err e = jazz_types__ids__RowAuthor__from_persisted_subject(created_by, &created);
    if (e.code) { jz_err_drop(&e); return (jz_err){ GROOVE_REC_ERR_NON_CANONICAL_RECORD, NULL }; }
    e = jazz_types__ids__RowAuthor__from_persisted_subject(updated_by, &updated);
    if (e.code) { jz_err_drop(&e); return (jz_err){ GROOVE_REC_ERR_NON_CANONICAL_RECORD, NULL }; }
    u32 n = JAZZ_WRR_USER_CELLS + table->columns.len;
    groove_Value *values = jz_zalloc((usize)n * sizeof(groove_Value));
    values[0].tag = GROOVE_V_UUID; values[0].u.uuid = row_uuid;
    values[1] = jazz_types__ids__RowAuthor__to_value(created);
    values[2].tag = GROOVE_V_U64; values[2].u.u64_ = created_at_ms;
    values[3] = jazz_types__ids__RowAuthor__to_value(updated);
    values[4].tag = GROOVE_V_U64; values[4].u.u64_ = updated_at_ms;
    values[5].tag = GROOVE_V_NULLABLE;
    if (has_deletion) {
        groove_Value *tag = JZ_NEW(groove_Value);
        tag->tag = GROOVE_V_ENUM_TAG;
        tag->u.enum_tag = deletion == JAZZ_DELETION_DELETED ? 0 : 1;
        values[5].u.nullable = tag;
    }
    for (u32 idx = 0; idx < table->columns.len; idx++) {
        groove_Value *v = &values[JAZZ_WRR_USER_CELLS + idx];
        v->tag = GROOVE_V_NULLABLE;
        if (idx < n_cells && cells_by_position[idx]) {
            v->u.nullable = jz_alloc(sizeof(groove_Value));
            groove__records__values__Value__Clone__clone(v->u.nullable, cells_by_position[idx]);
        }
    }
    jz_bytes raw = {0};
    e = groove__records__RecordDescriptor__create(descriptor, values, n, &raw);
    for (u32 i = 0; i < n; i++) groove__records__values__Value__drop(&values[i]);
    jz_free(values);
    JZ_TRY(e);
    jazz_protocol__protocol__VersionRecord__new(out, jz_str_clone(&table->name), schema_version, groove__records__OwnedRecord__new(raw, descriptor));
    return JZ_OK;
}

jazz_types_RowUuid jazz_protocol__protocol__VersionRecord__row_uuid(const jazz_protocol_VersionRecord *v) {
    groove_BorrowedRecord b = groove_OwnedRecord_borrowed(&v->record);
    jz_uuid u;
    if (groove__records__BorrowedRecord__get_uuid(&b, JAZZ_WRR_ROW_UUID, &u).code) jz_panic("valid wire row uuid");
    return u;
}
bool jazz_protocol__protocol__VersionRecord__deletion(const jazz_protocol_VersionRecord *v, u32 *out) {
    groove_BorrowedRecord b = groove_OwnedRecord_borrowed(&v->record);
    groove_Value value;
    if (groove__records__BorrowedRecord__get_idx(&b, JAZZ_WRR_DELETION, &value).code) jz_panic("valid wire deletion");
    bool some = false;
    if (!deletion_from_value(value, &some, out)) jz_panic("valid wire deletion");
    return some;
}
bool jazz_protocol__protocol__VersionRecord__deletes_row(const jazz_protocol_VersionRecord *v) {
    u32 d;
    if (!jazz_protocol__protocol__VersionRecord__deletion(v, &d) || d != JAZZ_DELETION_DELETED) return false;
    if (!v->has_authored_columns) return true;
    jz_str key = { (char *)"_deletion", 9, 0 };
    return jz_btree_contains(&v->authored_columns, &key);
}
static jazz_types_AuthorSubject author_at(const jazz_protocol_VersionRecord *v, u32 idx, const char *valid, const char *canonical) {
    groove_BorrowedRecord b = groove_OwnedRecord_borrowed(&v->record), sub;
    if (groove__records__BorrowedRecord__get_record(&b, idx, &sub).code) jz_panic(valid);
    jazz_types_RowAuthor a;
    if (jazz_types__ids__RowAuthor__from_record(sub, &a).code) jz_panic(canonical);
    return jazz_types__ids__RowAuthor__as_author_subject(a);
}
jazz_types_AuthorSubject jazz_protocol__protocol__VersionRecord__created_by(const jazz_protocol_VersionRecord *v) {
    return author_at(v, JAZZ_WRR_CREATED_BY, "valid wire created_by", "canonical wire created_by");
}
jazz_types_AuthorSubject jazz_protocol__protocol__VersionRecord__updated_by(const jazz_protocol_VersionRecord *v) {
    return author_at(v, JAZZ_WRR_UPDATED_BY, "valid wire updated_by", "canonical wire updated_by");
}
u64 jazz_protocol__protocol__VersionRecord__created_at_ms(const jazz_protocol_VersionRecord *v) {
    groove_BorrowedRecord b = groove_OwnedRecord_borrowed(&v->record);
    u64 x;
    if (groove__records__BorrowedRecord__get_u64(&b, JAZZ_WRR_CREATED_AT, &x).code) jz_panic("valid wire created_at_ms");
    return x;
}
u64 jazz_protocol__protocol__VersionRecord__updated_at_ms(const jazz_protocol_VersionRecord *v) {
    groove_BorrowedRecord b = groove_OwnedRecord_borrowed(&v->record);
    u64 x;
    if (groove__records__BorrowedRecord__get_u64(&b, JAZZ_WRR_UPDATED_AT, &x).code) jz_panic("valid wire updated_at_ms");
    return x;
}
bool jazz_protocol__protocol__VersionRecord__cell_at(const jazz_protocol_VersionRecord *v, u32 pos, groove_Value *out) {
    groove_BorrowedRecord b = groove_OwnedRecord_borrowed(&v->record);
    groove_Value value;
    if (groove__records__BorrowedRecord__get_idx(&b, JAZZ_WRR_USER_CELLS + pos, &value).code) jz_panic("valid wire cell");
    bool some;
    if (!nullable_value(value, &some, out)) jz_panic("valid nullable wire cell");
    return some;
}
bool jazz_protocol__protocol__VersionRecord__optional_cell_at(const jazz_protocol_VersionRecord *v, u32 pos, groove_Value *out) {
    u32 field = JAZZ_WRR_USER_CELLS + pos, n;
    groove__records__RecordDescriptor__fields(v->record.descriptor, &n);
    if (field >= n) return false;
    groove_BorrowedRecord b = groove_OwnedRecord_borrowed(&v->record);
    groove_Value value;
    jz_err e = groove__records__BorrowedRecord__get_idx(&b, field, &value);
    if (e.code) { jz_err_drop(&e); return false; }
    bool some;
    if (!nullable_value(value, &some, out)) return false;
    return some;
}
u32 jazz_protocol__protocol__VersionRecord__application_cell_count(const jazz_protocol_VersionRecord *v) {
    u32 n;
    groove__records__RecordDescriptor__fields(v->record.descriptor, &n);
    return n > JAZZ_WRR_USER_CELLS ? n - JAZZ_WRR_USER_CELLS : 0;
}
void jazz_protocol__protocol__VersionRecord__with_authored_columns(jazz_protocol_VersionRecord *v, bool some, jazz_protocol_StrSet cols) {
    jz_btree_drop(&v->authored_columns);
    v->has_authored_columns = some;
    if (some) v->authored_columns = cols;
    else jz_btree_init(&v->authored_columns, &jazz_protocol__protocol__DESC_STR_SET);
}
void jazz_protocol__protocol__VersionRecord__Clone__clone(jazz_protocol_VersionRecord *dst, const jazz_protocol_VersionRecord *src) {
    dst->table = src->table;
    dst->schema_version = src->schema_version;
    jazz_model__model__branch__BranchKey__Clone__clone(&dst->branch_key, &src->branch_key);
    groove__records__OwnedRecord__Clone__clone(&dst->record, &src->record);
    dst->has_authored_columns = src->has_authored_columns;
    jz_btree_clone(&dst->authored_columns, &src->authored_columns, str_clone_v, NULL);
    dst->base = src->base;
    dst->counter_signs = jz_bytes_clone(&src->counter_signs);
}
static void VersionRecord_clone_v(void *d, const void *s) { jazz_protocol__protocol__VersionRecord__Clone__clone(d, s); }
void jazz_protocol__protocol__VersionRecord__drop(jazz_protocol_VersionRecord *v) {
    jazz_model__model__branch__BranchKey__drop(&v->branch_key);
    groove__records__OwnedRecord__drop(&v->record);
    jz_btree_drop(&v->authored_columns);
    jz_bytes_drop(&v->counter_signs);
}
static void VersionRecord_drop_v(void *v) { jazz_protocol__protocol__VersionRecord__drop(v); }

static int str_set_cmp(const jz_btree *a, const jz_btree *b) {
    jz_btree_iter ia, ib;
    jz_btree_iter_begin(&ia, a);
    jz_btree_iter_begin(&ib, b);
    for (;;) {
        jz_str *ka, *kb; void *va, *vb;
        bool ha = jz_btree_next(&ia, (void **)&ka, &va), hb = jz_btree_next(&ib, (void **)&kb, &vb);
        if (!ha || !hb) return bool_cmp(ha, hb);
        int c = jz_str_cmp(ka, kb);
        if (c) return c;
    }
}
static int opt_txid_cmp(bool ha, const jazz_model_TxId *a, bool hb, const jazz_model_TxId *b) {
    if (ha != hb) return bool_cmp(ha, hb);
    return ha ? jazz_model__tx__TxId__cmp(a, b, NULL) : 0;
}
static int RowBase_cmp(const jazz_protocol_RowBase *a, const jazz_protocol_RowBase *b) {
    if (a->has_seq != b->has_seq) return bool_cmp(a->has_seq, b->has_seq);
    if (a->has_seq && a->seq != b->seq) return u64_cmp(a->seq, b->seq);
    return opt_txid_cmp(a->has_pending, &a->pending, b->has_pending, &b->pending);
}
int jazz_protocol__protocol__VersionRecord__Ord__cmp(const jazz_protocol_VersionRecord *a, const jazz_protocol_VersionRecord *b) {
    int c = jz_slice_cmp(istr(a->table), istr(b->table));
    if (c) return c;
    if ((c = jz_uuid_cmp(&a->schema_version, &b->schema_version))) return c;
    if ((c = jazz_model__model__branch__BranchKey__Ord__cmp(&a->branch_key, &b->branch_key))) return c;
    if ((c = jz_bytes_cmp(&a->record.raw, &b->record.raw))) return c;
    if (a->has_authored_columns != b->has_authored_columns) return bool_cmp(a->has_authored_columns, b->has_authored_columns);
    if (a->has_authored_columns && (c = str_set_cmp(&a->authored_columns, &b->authored_columns))) return c;
    if ((c = RowBase_cmp(&a->base, &b->base))) return c;
    return jz_bytes_cmp(&a->counter_signs, &b->counter_signs);
}
bool jazz_protocol__protocol__VersionRecord__PartialEq__eq(const jazz_protocol_VersionRecord *a, const jazz_protocol_VersionRecord *b) {
    return a->table == b->table && jz_uuid_eq(&a->schema_version, &b->schema_version) &&
           jazz_model__model__branch__BranchKey__Ord__cmp(&a->branch_key, &b->branch_key) == 0 &&
           a->record.descriptor == b->record.descriptor && jz_bytes_eq(&a->record.raw, &b->record.raw) &&
           a->has_authored_columns == b->has_authored_columns &&
           (!a->has_authored_columns || str_set_cmp(&a->authored_columns, &b->authored_columns) == 0) &&
           RowBase_cmp(&a->base, &b->base) == 0 && jz_bytes_eq(&a->counter_signs, &b->counter_signs);
}
static bool version_vec_eq(const jazz_protocol_VersionRecordVec *a, const jazz_protocol_VersionRecordVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!jazz_protocol__protocol__VersionRecord__PartialEq__eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}

jz_err jazz_protocol__protocol__validate_version_records(const jazz_protocol_VersionRecord *versions, u32 n) {
    for (u32 i = 0; i < n; i++) JZ_TRY(jazz_protocol__protocol__VersionRecord__validate_receipt(&versions[i]));
    return JZ_OK;
}

/* =====================================================================
 * Part 3: version bundles, runs, carriers; SyncMessage validation
 * ===================================================================== */
static void version_vec_clone(jazz_protocol_VersionRecordVec *dst, const jazz_protocol_VersionRecordVec *src) {
    jz_vec_clone(dst, src, sizeof(jazz_protocol_VersionRecord), VersionRecord_clone_v);
}
static void version_vec_drop(jazz_protocol_VersionRecordVec *v) { jz_vec_drop(v, sizeof(jazz_protocol_VersionRecord), VersionRecord_drop_v); }

void jazz_protocol__protocol__VersionBundle__Clone__clone(jazz_protocol_VersionBundle *dst, const jazz_protocol_VersionBundle *src) {
    jazz_model__tx__Transaction__Clone__clone(&dst->tx, &src->tx);
    version_vec_clone(&dst->versions, &src->versions);
    dst->scope = src->scope;
    jazz_model__tx__Fate__Clone__clone(&dst->fate, &src->fate);
    dst->has_global_time = src->has_global_time;
    dst->global_time = src->global_time;
    dst->durability = src->durability;
}
static void VersionBundle_clone_v(void *d, const void *s) { jazz_protocol__protocol__VersionBundle__Clone__clone(d, s); }
void jazz_protocol__protocol__VersionBundle__drop(jazz_protocol_VersionBundle *b) {
    jazz_model__tx__Transaction__drop(&b->tx);
    version_vec_drop(&b->versions);
    jazz_model__tx__Fate__drop(&b->fate);
}
static void VersionBundle_drop_v(void *b) { jazz_protocol__protocol__VersionBundle__drop(b); }
static bool VersionBundle_eq(const jazz_protocol_VersionBundle *a, const jazz_protocol_VersionBundle *b) {
    return jazz_model__tx__Transaction__PartialEq__eq(&a->tx, &b->tx) && version_vec_eq(&a->versions, &b->versions) &&
           a->scope == b->scope && jazz_model__tx__Fate__PartialEq__eq(&a->fate, &b->fate) &&
           a->has_global_time == b->has_global_time && (!a->has_global_time || a->global_time == b->global_time) &&
           a->durability == b->durability;
}
static void VersionBundleRef_to_owned_bundle(const jazz_protocol_VersionBundleRef *r, jazz_protocol_VersionBundle *out) {
    jazz_model__tx__Transaction__Clone__clone(&out->tx, r->tx);
    jazz_protocol_VersionRecordVec src = { (jazz_protocol_VersionRecord *)r->versions, r->n_versions, r->n_versions };
    version_vec_clone(&out->versions, &src);
    out->scope = r->scope;
    jazz_model__tx__Fate__Clone__clone(&out->fate, r->fate);
    out->has_global_time = r->has_global_time;
    out->global_time = r->global_time;
    out->durability = r->durability;
}
static jazz_protocol_VersionBundleRef VersionBundle_as_ref(const jazz_protocol_VersionBundle *b) {
    jazz_protocol_VersionBundleRef r = { &b->tx, b->versions.ptr, b->versions.len, b->scope, &b->fate, b->has_global_time, b->global_time, b->durability };
    return r;
}

static void RunOverride_clone(void *d_, const void *s_) {
    jazz_protocol_VersionBundleRunOverride *d = d_; const jazz_protocol_VersionBundleRunOverride *s = s_;
    *d = *s;
    if (s->has_tx) jazz_model__tx__Transaction__Clone__clone(&d->tx, &s->tx);
    if (s->has_fate) jazz_model__tx__Fate__Clone__clone(&d->fate, &s->fate);
}
static void RunOverride_drop(void *p) {
    jazz_protocol_VersionBundleRunOverride *o = p;
    if (o->has_tx) jazz_model__tx__Transaction__drop(&o->tx);
    if (o->has_fate) jazz_model__tx__Fate__drop(&o->fate);
}
static bool RunOverride_eq(const jazz_protocol_VersionBundleRunOverride *a, const jazz_protocol_VersionBundleRunOverride *b) {
    if (a->body_index != b->body_index || a->has_tx != b->has_tx || a->has_scope != b->has_scope ||
        a->has_fate != b->has_fate || a->has_global_time != b->has_global_time || a->has_durability != b->has_durability) return false;
    if (a->has_tx && !jazz_model__tx__Transaction__PartialEq__eq(&a->tx, &b->tx)) return false;
    if (a->has_scope && a->scope != b->scope) return false;
    if (a->has_fate && !jazz_model__tx__Fate__PartialEq__eq(&a->fate, &b->fate)) return false;
    if (a->has_global_time && (a->global_time_some != b->global_time_some || (a->global_time_some && a->global_time != b->global_time))) return false;
    return !a->has_durability || a->durability == b->durability;
}
static void RunBody_clone(void *d, const void *s) { version_vec_clone(&((jazz_protocol_VersionBundleRunBody *)d)->versions, &((const jazz_protocol_VersionBundleRunBody *)s)->versions); }
static void RunBody_drop(void *p) { version_vec_drop(&((jazz_protocol_VersionBundleRunBody *)p)->versions); }
static void VersionBundleRun_clone(jazz_protocol_VersionBundleRun *d, const jazz_protocol_VersionBundleRun *s) {
    const jazz_protocol_VersionBundleRunHeader *h = &s->header;
    d->header = *h;
    jazz_model__tx__Transaction__Clone__clone(&d->header.tx, &h->tx);
    jazz_model__tx__Fate__Clone__clone(&d->header.fate, &h->fate);
    jz_vec_clone(&d->bodies, &s->bodies, sizeof(jazz_protocol_VersionBundleRunBody), RunBody_clone);
    jz_vec_clone(&d->overrides, &s->overrides, sizeof(jazz_protocol_VersionBundleRunOverride), RunOverride_clone);
}
static void VersionBundleRun_drop(jazz_protocol_VersionBundleRun *r) {
    jazz_model__tx__Transaction__drop(&r->header.tx);
    jazz_model__tx__Fate__drop(&r->header.fate);
    jz_vec_drop(&r->bodies, sizeof(jazz_protocol_VersionBundleRunBody), RunBody_drop);
    jz_vec_drop(&r->overrides, sizeof(jazz_protocol_VersionBundleRunOverride), RunOverride_drop);
}
static bool VersionBundleRun_eq(const jazz_protocol_VersionBundleRun *a, const jazz_protocol_VersionBundleRun *b) {
    const jazz_protocol_VersionBundleRunHeader *x = &a->header, *y = &b->header;
    if (x->has_table != y->has_table || (x->has_table && x->table != y->table)) return false;
    if (!jazz_model__tx__Transaction__PartialEq__eq(&x->tx, &y->tx) || x->scope != y->scope || x->body_count != y->body_count ||
        !jazz_model__tx__Fate__PartialEq__eq(&x->fate, &y->fate) || x->has_global_time != y->has_global_time ||
        (x->has_global_time && x->global_time != y->global_time) || x->durability != y->durability) return false;
    if (a->bodies.len != b->bodies.len || a->overrides.len != b->overrides.len) return false;
    for (u32 i = 0; i < a->bodies.len; i++) if (!version_vec_eq(&a->bodies.ptr[i].versions, &b->bodies.ptr[i].versions)) return false;
    for (u32 i = 0; i < a->overrides.len; i++) if (!RunOverride_eq(&a->overrides.ptr[i], &b->overrides.ptr[i])) return false;
    return true;
}
void jazz_protocol__protocol__VersionCarrier__Clone__clone(jazz_protocol_VersionCarrier *dst, const jazz_protocol_VersionCarrier *src) {
    dst->tag = src->tag;
    if (src->tag == JAZZ_CARRIER_BUNDLE) jazz_protocol__protocol__VersionBundle__Clone__clone(&dst->u.bundle, &src->u.bundle);
    else VersionBundleRun_clone(&dst->u.run, &src->u.run);
}
static void VersionCarrier_clone_v(void *d, const void *s) { jazz_protocol__protocol__VersionCarrier__Clone__clone(d, s); }
void jazz_protocol__protocol__VersionCarrier__drop(jazz_protocol_VersionCarrier *c) {
    if (c->tag == JAZZ_CARRIER_BUNDLE) jazz_protocol__protocol__VersionBundle__drop(&c->u.bundle);
    else VersionBundleRun_drop(&c->u.run);
}
static void VersionCarrier_drop_v(void *c) { jazz_protocol__protocol__VersionCarrier__drop(c); }
static bool VersionCarrier_eq(const jazz_protocol_VersionCarrier *a, const jazz_protocol_VersionCarrier *b) {
    if (a->tag != b->tag) return false;
    return a->tag == JAZZ_CARRIER_BUNDLE ? VersionBundle_eq(&a->u.bundle, &b->u.bundle) : VersionBundleRun_eq(&a->u.run, &b->u.run);
}
static void carriers_clone(jazz_protocol_VersionCarrierVec *d, const jazz_protocol_VersionCarrierVec *s) {
    jz_vec_clone(d, s, sizeof(jazz_protocol_VersionCarrier), VersionCarrier_clone_v);
}
static void carriers_drop(jazz_protocol_VersionCarrierVec *v) { jz_vec_drop(v, sizeof(jazz_protocol_VersionCarrier), VersionCarrier_drop_v); }
static bool carriers_eq(const jazz_protocol_VersionCarrierVec *a, const jazz_protocol_VersionCarrierVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!VersionCarrier_eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}

static bool RunOverride_has_overrides(const jazz_protocol_VersionBundleRunOverride *o) {
    return o->has_tx || o->has_scope || o->has_fate || o->has_global_time || o->has_durability;
}

jz_err jazz_protocol__protocol__VersionBundleRun__validate(const jazz_protocol_VersionBundleRun *run) {
    u32 declared = run->header.body_count;
    u32 actual = run->bodies.len;
    if (declared == 0) return jz_err_new(JAZZ_PROTO_ERR_EMPTY_RUN, "version-bundle run has no bodies");
    if (declared != actual)
        return jz_err_fmt(JAZZ_PROTO_ERR_BODY_COUNT_MISMATCH, "version-bundle run body_count %u did not match %u bodies", declared, actual);
    jz_btree seen;
    jz_btree_init(&seen, &DESC_USIZE_SET);
    for (u32 i = 0; i < run->overrides.len; i++) {
        u32 index = run->overrides.ptr[i].body_index;
        if (index >= declared) {
            jz_btree_drop(&seen);
            return jz_err_fmt(JAZZ_PROTO_ERR_OVERRIDE_INDEX_OUT_OF_RANGE, "version-bundle run override index %u out of range for %u bodies", index, declared);
        }
        if (jz_btree_insert(&seen, &index, NULL, NULL)) {
            jz_btree_drop(&seen);
            return jz_err_fmt(JAZZ_PROTO_ERR_DUPLICATE_OVERRIDE, "version-bundle run has duplicate override for body %u", index);
        }
    }
    jz_btree_drop(&seen);
    if (run->header.has_table) {
        jz_slice table = istr(run->header.table);
        for (u32 b = 0; b < run->bodies.len; b++) {
            const jazz_protocol_VersionRecordVec *vs = &run->bodies.ptr[b].versions;
            for (u32 i = 0; i < vs->len; i++) {
                jz_slice vt = istr(vs->ptr[i].table);
                if (!slice_eq(vt, table))
                    return jz_err_fmt(JAZZ_PROTO_ERR_TABLE_MISMATCH, "version-bundle run table context %.*s did not match body table %.*s",
                                      (int)table.len, table.ptr, (int)vt.len, vt.ptr);
            }
        }
    }
    return JZ_OK;
}

/* overrides BTreeMap<usize, &Override>: built as a dense index table (bodies are validated) */
static const jazz_protocol_VersionBundleRunOverride **override_index(const jazz_protocol_VersionBundleRun *run) {
    jz_btree map;
    static const jz_btree_desc DESC_IDX_PTR = { sizeof(u32), sizeof(void *), jz_cmp_u32, NULL, NULL };
    jz_btree_init(&map, &DESC_IDX_PTR);
    for (u32 i = 0; i < run->overrides.len; i++) {
        u32 idx = run->overrides.ptr[i].body_index;
        const void *p = &run->overrides.ptr[i];
        jz_btree_insert(&map, &idx, &p, NULL);
    }
    const jazz_protocol_VersionBundleRunOverride **out = jz_zalloc((usize)run->bodies.len * sizeof(void *) + 1);
    u32 *k; const jazz_protocol_VersionBundleRunOverride **v;
    JZ_BTREE_FOR(&map, k, v) out[*k] = *v;
    jz_btree_drop(&map);
    return out;
}
jz_err jazz_protocol__protocol__VersionBundleRun__expand(const jazz_protocol_VersionBundleRun *run, jazz_protocol_VersionBundleVec *out) {
    JZ_TRY(jazz_protocol__protocol__VersionBundleRun__validate(run));
    const jazz_protocol_VersionBundleRunOverride **ov = override_index(run);
    jazz_protocol_VersionBundleVec bundles = {0};
    jz_vec_reserve(&bundles, run->bodies.len, sizeof(jazz_protocol_VersionBundle));
    for (u32 index = 0; index < run->bodies.len; index++) {
        const jazz_protocol_VersionBundleRunOverride *o = ov[index];
        const jazz_protocol_VersionBundleRunHeader *h = &run->header;
        jazz_protocol_VersionBundle *b = jz_vec_push_slot(&bundles, sizeof *b);
        jazz_model__tx__Transaction__Clone__clone(&b->tx, (o && o->has_tx) ? &o->tx : &h->tx);
        version_vec_clone(&b->versions, &run->bodies.ptr[index].versions);
        b->scope = (o && o->has_scope) ? o->scope : h->scope;
        jazz_model__tx__Fate__Clone__clone(&b->fate, (o && o->has_fate) ? &o->fate : &h->fate);
        if (o && o->has_global_time) { b->has_global_time = o->global_time_some; b->global_time = o->global_time; }
        else { b->has_global_time = h->has_global_time; b->global_time = h->global_time; }
        b->durability = (o && o->has_durability) ? o->durability : h->durability;
    }
    jz_free(ov);
    *out = bundles;
    return JZ_OK;
}
jz_err jazz_protocol__protocol__VersionBundleRun__bundle_refs(const jazz_protocol_VersionBundleRun *run, jazz_protocol_VersionBundleRefVec *out) {
    JZ_TRY(jazz_protocol__protocol__VersionBundleRun__validate(run));
    const jazz_protocol_VersionBundleRunOverride **ov = override_index(run);
    jazz_protocol_VersionBundleRefVec refs = {0};
    jz_vec_reserve(&refs, run->bodies.len, sizeof(jazz_protocol_VersionBundleRef));
    for (u32 index = 0; index < run->bodies.len; index++) {
        const jazz_protocol_VersionBundleRunOverride *o = ov[index];
        const jazz_protocol_VersionBundleRunHeader *h = &run->header;
        jazz_protocol_VersionBundleRef *r = jz_vec_push_slot(&refs, sizeof *r);
        r->tx = (o && o->has_tx) ? &o->tx : &h->tx;
        r->versions = run->bodies.ptr[index].versions.ptr;
        r->n_versions = run->bodies.ptr[index].versions.len;
        r->scope = (o && o->has_scope) ? o->scope : h->scope;
        r->fate = (o && o->has_fate) ? &o->fate : &h->fate;
        if (o && o->has_global_time) { r->has_global_time = o->global_time_some; r->global_time = o->global_time; }
        else { r->has_global_time = h->has_global_time; r->global_time = h->global_time; }
        r->durability = (o && o->has_durability) ? o->durability : h->durability;
    }
    jz_free(ov);
    *out = refs;
    return JZ_OK;
}

static bool common_run_table(const jazz_protocol_VersionBundleVec *bundles, jazz_protocol_InternStr *out) {
    bool have = false;
    jz_slice table = {0};
    for (u32 b = 0; b < bundles->len; b++) {
        const jazz_protocol_VersionRecordVec *vs = &bundles->ptr[b].versions;
        for (u32 i = 0; i < vs->len; i++) {
            jz_slice t = istr(vs->ptr[i].table);
            if (!have) { table = t; have = true; }
            else if (!slice_eq(table, t)) return false;
        }
    }
    if (!have) return false;
    *out = groove__Intern__new_str(jz_str_from_slice(table));
    return true;
}
jz_err jazz_protocol__protocol__VersionBundleRun__from_adjacent_singleton_bundles(jazz_protocol_VersionBundleVec bundles, jazz_protocol_VersionBundleRun *out) {
    if (bundles.len == 0) { jz_vec_drop(&bundles, sizeof(jazz_protocol_VersionBundle), VersionBundle_drop_v); return jz_err_new(JAZZ_PROTO_ERR_EMPTY_RUN, "version-bundle run has no bodies"); }
    jazz_protocol_VersionBundleRun run;
    memset(&run, 0, sizeof run);
    run.header.has_table = common_run_table(&bundles, &run.header.table);
    const jazz_protocol_VersionBundle *first = &bundles.ptr[0];
    for (u32 index = 0; index < bundles.len; index++) {
        const jazz_protocol_VersionBundle *bundle = &bundles.ptr[index];
        jazz_protocol_VersionBundleRunOverride o;
        memset(&o, 0, sizeof o);
        o.body_index = index;
        if (!jazz_model__tx__Transaction__PartialEq__eq(&bundle->tx, &first->tx)) { o.has_tx = true; jazz_model__tx__Transaction__Clone__clone(&o.tx, &bundle->tx); }
        if (bundle->scope != first->scope) { o.has_scope = true; o.scope = bundle->scope; }
        if (!jazz_model__tx__Fate__PartialEq__eq(&bundle->fate, &first->fate)) { o.has_fate = true; jazz_model__tx__Fate__Clone__clone(&o.fate, &bundle->fate); }
        if (bundle->has_global_time != first->has_global_time || (bundle->has_global_time && bundle->global_time != first->global_time)) {
            o.has_global_time = true; o.global_time_some = bundle->has_global_time; o.global_time = bundle->global_time;
        }
        if (bundle->durability != first->durability) { o.has_durability = true; o.durability = bundle->durability; }
        if (RunOverride_has_overrides(&o)) jz_vec_push(&run.overrides, &o, sizeof o);
    }
    jazz_model__tx__Transaction__Clone__clone(&run.header.tx, &first->tx);
    run.header.scope = first->scope;
    run.header.body_count = bundles.len;
    jazz_model__tx__Fate__Clone__clone(&run.header.fate, &first->fate);
    run.header.has_global_time = first->has_global_time;
    run.header.global_time = first->global_time;
    run.header.durability = first->durability;
    jz_vec_reserve(&run.bodies, bundles.len, sizeof(jazz_protocol_VersionBundleRunBody));
    for (u32 i = 0; i < bundles.len; i++) {
        jazz_protocol_VersionBundle *b = &bundles.ptr[i];
        jazz_protocol_VersionBundleRunBody body = { { b->versions.ptr, b->versions.len, b->versions.cap } };
        jz_vec_push(&run.bodies, &body, sizeof body);
        jazz_model__tx__Transaction__drop(&b->tx);
        jazz_model__tx__Fate__drop(&b->fate);
    }
    jz_free(bundles.ptr);
    jz_err e = jazz_protocol__protocol__VersionBundleRun__validate(&run);
    if (e.code) { VersionBundleRun_drop(&run); return e; }
    *out = run;
    return JZ_OK;
}

jz_err jazz_protocol__protocol__VersionCarrier__expand(const jazz_protocol_VersionCarrier *c, jazz_protocol_VersionBundleVec *out) {
    if (c->tag == JAZZ_CARRIER_BUNDLE) {
        jazz_protocol_VersionBundleVec v = {0};
        jazz_protocol_VersionBundle *slot = jz_vec_push_slot(&v, sizeof *slot);
        jazz_protocol__protocol__VersionBundle__Clone__clone(slot, &c->u.bundle);
        *out = v;
        return JZ_OK;
    }
    return jazz_protocol__protocol__VersionBundleRun__expand(&c->u.run, out);
}
jz_err jazz_protocol__protocol__VersionCarrier__bundle_refs(const jazz_protocol_VersionCarrier *c, jazz_protocol_VersionBundleRefVec *out) {
    if (c->tag == JAZZ_CARRIER_BUNDLE) {
        jazz_protocol_VersionBundleRefVec v = {0};
        jazz_protocol_VersionBundleRef r = VersionBundle_as_ref(&c->u.bundle);
        jz_vec_push(&v, &r, sizeof r);
        *out = v;
        return JZ_OK;
    }
    return jazz_protocol__protocol__VersionBundleRun__bundle_refs(&c->u.run, out);
}
static bool force_singleton_version_carriers(void) { return jazz_types__debug_env__force_singleton_version_carriers(); }
jz_err jazz_protocol__protocol__build_version_carriers_from_singletons(jazz_protocol_VersionBundleVec bundles, jazz_protocol_VersionCarrierVec *out) {
    jazz_protocol_VersionCarrierVec carriers = {0};
    if (force_singleton_version_carriers() || bundles.len <= 1) {
        jz_vec_reserve(&carriers, bundles.len, sizeof(jazz_protocol_VersionCarrier));
        for (u32 i = 0; i < bundles.len; i++) {
            jazz_protocol_VersionCarrier *c = jz_vec_push_slot(&carriers, sizeof *c);
            c->tag = JAZZ_CARRIER_BUNDLE;
            c->u.bundle = bundles.ptr[i];
        }
        jz_free(bundles.ptr);
        *out = carriers;
        return JZ_OK;
    }
    jazz_protocol_VersionCarrier c;
    c.tag = JAZZ_CARRIER_RUN;
    JZ_TRY(jazz_protocol__protocol__VersionBundleRun__from_adjacent_singleton_bundles(bundles, &c.u.run));
    jz_vec_push(&carriers, &c, sizeof c);
    *out = carriers;
    return JZ_OK;
}
jz_err jazz_protocol__protocol__expand_version_carriers(const jazz_protocol_VersionCarrier *carriers, u32 n, jazz_protocol_VersionBundleVec *out) {
    jazz_protocol_VersionBundleVec bundles = {0};
    for (u32 i = 0; i < n; i++) {
        jazz_protocol_VersionBundleVec part;
        jz_err e = jazz_protocol__protocol__VersionCarrier__expand(&carriers[i], &part);
        if (e.code) { jz_vec_drop(&bundles, sizeof(jazz_protocol_VersionBundle), VersionBundle_drop_v); return e; }
        jz_vec_extend(&bundles, part.ptr, part.len, sizeof(jazz_protocol_VersionBundle));
        jz_free(part.ptr);
    }
    *out = bundles;
    return JZ_OK;
}

/* ---------------- SyncMessage helpers / validation ---------------- */
const jazz_protocol_VersionRecord *jazz_protocol__protocol__SyncMessage__uploaded_versions(const jazz_protocol_SyncMessage *m, u32 *n) {
    if (m->tag == JAZZ_SM_COMMIT_UNIT) { *n = m->u.commit_unit.versions.len; return m->u.commit_unit.versions.ptr; }
    *n = 0;
    return NULL;
}
u64 jazz_protocol__protocol__SyncMessage__required_wire_features(const jazz_protocol_SyncMessage *m) {
    switch (m->tag) {
    case JAZZ_SM_AUTH_SCOPE_INTENT: case JAZZ_SM_AUTH_SCOPE_VIEW: case JAZZ_SM_AUTH_SCOPE_AGGREGATE_RECEIPT:
    case JAZZ_SM_AUTH_SCOPE_UNAVAILABLE: case JAZZ_SM_AUTH_SCOPE_DECISION:
        return JAZZ_FEATURE_AUTHORIZATION_SCOPE_VIEWS;
    case JAZZ_SM_CHUNK_REQUEST_BATCH: case JAZZ_SM_CHUNK_RESPONSE_BATCH: case JAZZ_SM_CHUNK_UPLOAD_START:
    case JAZZ_SM_CHUNK_UPLOAD_NODES: case JAZZ_SM_CHUNK_UPLOAD_RESULT:
        return JAZZ_FEATURE_AUXILIARY_CHUNKS;
    default: return JAZZ_FEATURE_NONE;
    }
}
static const jazz_protocol_ViewUpdatePayload *carried_view_update(const jazz_protocol_SyncMessage *m) {
    switch (m->tag) {
    case JAZZ_SM_VIEW_UPDATE: case JAZZ_SM_VIEW_UPDATE_PART: return &m->u.view_update;
    case JAZZ_SM_AUTH_SCOPE_VIEW: return &m->u.auth_scope_view.view;
    default: return NULL;
    }
}
static jz_err validate_version_carrier_runs(const jazz_protocol_VersionCarrierVec *cs) {
    for (u32 i = 0; i < cs->len; i++)
        if (cs->ptr[i].tag == JAZZ_CARRIER_RUN) JZ_TRY(jazz_protocol__protocol__VersionBundleRun__validate(&cs->ptr[i].u.run));
    return JZ_OK;
}
static jz_err validate_carriers_and_records(const jazz_protocol_VersionCarrierVec *cs) {
    JZ_TRY(validate_version_carrier_runs(cs));
    for (u32 i = 0; i < cs->len; i++) {
        jazz_protocol_VersionBundleRefVec refs;
        JZ_TRY(jazz_protocol__protocol__VersionCarrier__bundle_refs(&cs->ptr[i], &refs));
        for (u32 j = 0; j < refs.len; j++) {
            jz_err e = jazz_protocol__protocol__validate_version_records(refs.ptr[j].versions, refs.ptr[j].n_versions);
            if (e.code) { jz_vec_drop(&refs, sizeof(jazz_protocol_VersionBundleRef), NULL); return e; }
        }
        jz_vec_drop(&refs, sizeof(jazz_protocol_VersionBundleRef), NULL);
    }
    return JZ_OK;
}
jz_err jazz_protocol__protocol__SyncMessage__validate_version_carriers(const jazz_protocol_SyncMessage *m) {
    if (m->tag == JAZZ_SM_COMMIT_UNIT)
        return jazz_protocol__protocol__validate_version_records(m->u.commit_unit.versions.ptr, m->u.commit_unit.versions.len);
    if (m->tag == JAZZ_SM_CURRENT_ROWS_RECEIPT) return validate_carriers_and_records(&m->u.current_rows_receipt.version_carriers);
    const jazz_protocol_ViewUpdatePayload *view = carried_view_update(m);
    return view ? validate_carriers_and_records(&view->version_carriers) : JZ_OK;
}
static bool is_zero16(const u8 *b) { for (u32 i = 0; i < 16; i++) if (b[i]) return false; return true; }
static int SupportingRow_cmp_v(const void *a, const void *b, void *ctx) { (void)ctx; return jazz_protocol__protocol__SupportingRow__Ord__cmp(a, b); }
jz_err jazz_protocol__protocol__SyncMessage__validate_wire_contract(const jazz_protocol_SyncMessage *m) {
    /* WireContractError::VersionCarrier(error) displays the inner error unchanged */
    JZ_TRY(jazz_protocol__protocol__SyncMessage__validate_version_carriers(m));
    const jazz_protocol_ViewUpdatePayload *view = carried_view_update(m);
    if (!view) return JZ_OK;
    const jazz_protocol_SupportingRowsUpdate *s = &view->supporting_rows;
    bool bad = is_zero16(s->revision);
    if (s->tag == JAZZ_SRU_DELTA)
        bad = bad || is_zero16(s->predecessor) ||
              (memcmp(s->predecessor, s->revision, 16) == 0 && (s->added.len != 0 || s->removed.len != 0));
    if (s->tag == JAZZ_SRU_CATCH_UP)
        bad = bad || is_zero16(s->predecessor) || memcmp(s->predecessor, s->revision, 16) == 0;
    if (bad) return jz_err_new(JAZZ_PROTO_ERR_INVALID_SUPPORTING_REVISION, "supporting transition revision is invalid");
    /* identities: BTreeSet<&SupportingRow> */
    static const jz_btree_desc DESC_ROWREF_SET = { sizeof(jazz_protocol_SupportingRow), 0, SupportingRow_cmp_v, NULL, NULL };
    jz_btree identities;
    jz_btree_init(&identities, &DESC_ROWREF_SET);
    const jazz_protocol_SupportingRowVec *lists[2] = { &s->added, s->tag == JAZZ_SRU_SNAPSHOT ? NULL : &s->removed };
    for (u32 l = 0; l < 2; l++) {
        if (!lists[l]) continue;
        for (u32 i = 0; i < lists[l]->len; i++) {
            const jazz_protocol_SupportingRow *row = &lists[l]->ptr[i];
            if (!jazz_protocol__protocol__SupportingRow__is_wire_valid(row)) {
                jz_btree_drop(&identities);
                return jz_err_new(JAZZ_PROTO_ERR_INVALID_SUPPORTING_ROW, "supporting row reference is invalid");
            }
            if (jz_btree_contains(&identities, row)) {
                jz_btree_drop(&identities);
                return jz_err_new(JAZZ_PROTO_ERR_DUPLICATE_SUPPORTING_ROW, "supporting snapshot duplicates an exact physical row version");
            }
            jz_btree_insert(&identities, row, NULL, NULL); /* shallow copy, no key drop */
        }
    }
    jz_btree_drop(&identities);
    return JZ_OK;
}

/* =====================================================================
 * Part 4: derived Clone / PartialEq / Ord / Debug / Drop of result rows,
 * supporting rows and program-fact entries
 * ===================================================================== */
static void ob_clone(jazz_protocol_OptBytes *d, const jazz_protocol_OptBytes *s) { d->some = s->some; d->v = s->some ? jz_bytes_clone(&s->v) : (jz_bytes){0}; }
static void ob_drop(jazz_protocol_OptBytes *o) { if (o->some) jz_bytes_drop(&o->v); }
static int ob_cmp(const jazz_protocol_OptBytes *a, const jazz_protocol_OptBytes *b) {
    if (a->some != b->some) return bool_cmp(a->some, b->some);
    return a->some ? jz_bytes_cmp(&a->v, &b->v) : 0;
}
static int otx_cmp(const jazz_protocol_OptTxId *a, const jazz_protocol_OptTxId *b) { return opt_txid_cmp(a->some, &a->v, b->some, &b->v); }
static void os_clone(jazz_protocol_OptStr *d, const jazz_protocol_OptStr *s) { d->some = s->some; d->v = s->some ? jz_str_clone(&s->v) : (jz_str){0}; }
static void os_drop(jazz_protocol_OptStr *o) { if (o->some) jz_str_drop(&o->v); }
static int os_cmp(const jazz_protocol_OptStr *a, const jazz_protocol_OptStr *b) {
    if (a->some != b->some) return bool_cmp(a->some, b->some);
    return a->some ? jz_str_cmp(&a->v, &b->v) : 0;
}
static int oe_cmp(const jazz_protocol_OptU32Enum *a, const jazz_protocol_OptU32Enum *b) {
    if (a->some != b->some) return bool_cmp(a->some, b->some);
    return a->some ? u64_cmp(a->v, b->v) : 0;
}
static int ouuid_cmp(bool ha, const jz_uuid *a, bool hb, const jz_uuid *b) {
    if (ha != hb) return bool_cmp(ha, hb);
    return ha ? jz_uuid_cmp(a, b) : 0;
}
static int istr_cmp(jazz_protocol_InternStr a, jazz_protocol_InternStr b) { return a == b ? 0 : jz_slice_cmp(istr(a), istr(b)); }
static void debug_opt_bytes(jz_str *out, const jazz_protocol_OptBytes *o) {
    if (!o->some) { jz_str_push_cstr(out, "None"); return; }
    jz_str_push_cstr(out, "Some([");
    for (u32 i = 0; i < o->v.len; i++) jz_str_appendf(out, i ? ", %u" : "%u", o->v.ptr[i]);
    jz_str_push_cstr(out, "])");
}
static void debug_opt_tx(jz_str *out, const jazz_protocol_OptTxId *o) {
    if (!o->some) { jz_str_push_cstr(out, "None"); return; }
    jz_str_push_cstr(out, "Some("); jazz_model__tx__TxId__Debug__fmt(out, &o->v); jz_str_push_cstr(out, ")");
}
static void debug_opt_uuid(jz_str *out, const char *name, bool some, const jz_uuid *u) {
    if (!some) { jz_str_push_cstr(out, "None"); return; }
    jz_str_push_cstr(out, "Some("); jazz_types__ids__uuid_id__Debug__fmt(out, name, u); jz_str_push_cstr(out, ")");
}
static void debug_istr(jz_str *out, jazz_protocol_InternStr s) { jz_str_appendf(out, "\"%.*s\"", (int)s->len, s->ptr); }

/* ---------------- RowVersionRefEntry ---------------- */
void jazz_protocol__protocol__RowVersionRefEntry__Clone__clone(jazz_protocol_RowVersionRefEntry *d, const jazz_protocol_RowVersionRefEntry *s) {
    d->tx = s->tx;
    d->has_schema_version = s->has_schema_version; d->schema_version = s->schema_version;
    d->layer = s->layer;
    d->batch = s->batch;
    ob_clone(&d->branch_or_prefix, &s->branch_or_prefix);
    ob_clone(&d->row_digest, &s->row_digest);
}
void jazz_protocol__protocol__RowVersionRefEntry__drop(jazz_protocol_RowVersionRefEntry *e) { ob_drop(&e->branch_or_prefix); ob_drop(&e->row_digest); }
int jazz_protocol__protocol__RowVersionRefEntry__Ord__cmp(const jazz_protocol_RowVersionRefEntry *a, const jazz_protocol_RowVersionRefEntry *b) {
    int c = jazz_model__tx__TxId__cmp(&a->tx, &b->tx, NULL);
    if (c) return c;
    if ((c = ouuid_cmp(a->has_schema_version, &a->schema_version, b->has_schema_version, &b->schema_version))) return c;
    if (a->layer != b->layer) return u64_cmp(a->layer, b->layer);
    if ((c = otx_cmp(&a->batch, &b->batch))) return c;
    if ((c = ob_cmp(&a->branch_or_prefix, &b->branch_or_prefix))) return c;
    return ob_cmp(&a->row_digest, &b->row_digest);
}
void jazz_protocol__protocol__ResultRowLayer__Debug__fmt(jz_str *out, u32 layer) {
    static const char *const names[3] = { "Content", "Deletion", "ContentOrDeletion" };
    jz_str_push_cstr(out, names[layer]);
}
void jazz_protocol__protocol__RowVersionRefEntry__Debug__fmt(jz_str *out, const jazz_protocol_RowVersionRefEntry *e) {
    jz_str_push_cstr(out, "RowVersionRefEntry { tx: ");
    jazz_model__tx__TxId__Debug__fmt(out, &e->tx);
    jz_str_push_cstr(out, ", schema_version: ");
    debug_opt_uuid(out, "SchemaVersionId", e->has_schema_version, &e->schema_version);
    jz_str_push_cstr(out, ", layer: ");
    jazz_protocol__protocol__ResultRowLayer__Debug__fmt(out, e->layer);
    jz_str_push_cstr(out, ", batch: ");
    debug_opt_tx(out, &e->batch);
    jz_str_push_cstr(out, ", branch_or_prefix: ");
    debug_opt_bytes(out, &e->branch_or_prefix);
    jz_str_push_cstr(out, ", row_digest: ");
    debug_opt_bytes(out, &e->row_digest);
    jz_str_push_cstr(out, " }");
}
static bool RowVersionRefEntry_eq(const jazz_protocol_RowVersionRefEntry *a, const jazz_protocol_RowVersionRefEntry *b) {
    return jazz_protocol__protocol__RowVersionRefEntry__Ord__cmp(a, b) == 0;
}

/* ---------------- SupportingRow ---------------- */
bool jazz_protocol__protocol__SupportingRow__is_wire_valid(const jazz_protocol_SupportingRow *r) {
    return r->version_table->len != 0 && r->version.layer != JAZZ_LAYER_CONTENT_OR_DELETION;
}
int jazz_protocol__protocol__SupportingRow__Ord__cmp(const jazz_protocol_SupportingRow *a, const jazz_protocol_SupportingRow *b) {
    int c = jz_uuid_cmp(&a->physical_table, &b->physical_table);
    if (c) return c;
    if ((c = istr_cmp(a->version_table, b->version_table))) return c;
    if ((c = jz_uuid_cmp(&a->row, &b->row))) return c;
    return jazz_protocol__protocol__RowVersionRefEntry__Ord__cmp(&a->version, &b->version);
}
bool jazz_protocol__protocol__SupportingRow__PartialEq__eq(const jazz_protocol_SupportingRow *a, const jazz_protocol_SupportingRow *b) {
    return jz_uuid_eq(&a->physical_table, &b->physical_table) && a->version_table == b->version_table &&
           jz_uuid_eq(&a->row, &b->row) && RowVersionRefEntry_eq(&a->version, &b->version);
}
static void SupportingRow_clone_v(void *d_, const void *s_) {
    jazz_protocol_SupportingRow *d = d_; const jazz_protocol_SupportingRow *s = s_;
    d->physical_table = s->physical_table; d->version_table = s->version_table; d->row = s->row;
    jazz_protocol__protocol__RowVersionRefEntry__Clone__clone(&d->version, &s->version);
}
static void SupportingRow_drop_v(void *p) { jazz_protocol__protocol__RowVersionRefEntry__drop(&((jazz_protocol_SupportingRow *)p)->version); }
static bool supporting_rows_eq(const jazz_protocol_SupportingRowVec *a, const jazz_protocol_SupportingRowVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!jazz_protocol__protocol__SupportingRow__PartialEq__eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
static void SupportingRowsUpdate_clone(jazz_protocol_SupportingRowsUpdate *d, const jazz_protocol_SupportingRowsUpdate *s) {
    d->tag = s->tag;
    memcpy(d->predecessor, s->predecessor, 16);
    memcpy(d->revision, s->revision, 16);
    jz_vec_clone(&d->added, &s->added, sizeof(jazz_protocol_SupportingRow), SupportingRow_clone_v);
    jz_vec_clone(&d->removed, &s->removed, sizeof(jazz_protocol_SupportingRow), SupportingRow_clone_v);
}
static void SupportingRowsUpdate_drop(jazz_protocol_SupportingRowsUpdate *s) {
    jz_vec_drop(&s->added, sizeof(jazz_protocol_SupportingRow), SupportingRow_drop_v);
    jz_vec_drop(&s->removed, sizeof(jazz_protocol_SupportingRow), SupportingRow_drop_v);
}
static bool SupportingRowsUpdate_eq(const jazz_protocol_SupportingRowsUpdate *a, const jazz_protocol_SupportingRowsUpdate *b) {
    if (a->tag != b->tag || memcmp(a->revision, b->revision, 16) != 0) return false;
    if (a->tag != JAZZ_SRU_SNAPSHOT && memcmp(a->predecessor, b->predecessor, 16) != 0) return false;
    return supporting_rows_eq(&a->added, &b->added) && supporting_rows_eq(&a->removed, &b->removed);
}

/* ---------------- PeerPayloadInventory / ViewUpdatePayload ---------------- */
static void PeerPayloadInventory_clone(jazz_protocol_PeerPayloadInventory *d, const jazz_protocol_PeerPayloadInventory *s) {
    jz_vec_clone(&d->complete_tx_payloads, &s->complete_tx_payloads, sizeof(jazz_model_TxId), NULL);
    d->has_authorization_progress = s->has_authorization_progress;
    d->authorization_progress = s->authorization_progress;
    d->opening_pending = s->opening_pending;
}
static bool PeerPayloadInventory_eq(const jazz_protocol_PeerPayloadInventory *a, const jazz_protocol_PeerPayloadInventory *b) {
    if (a->complete_tx_payloads.len != b->complete_tx_payloads.len) return false;
    for (u32 i = 0; i < a->complete_tx_payloads.len; i++)
        if (!jazz_model__tx__TxId__PartialEq__eq(&a->complete_tx_payloads.ptr[i], &b->complete_tx_payloads.ptr[i])) return false;
    return a->has_authorization_progress == b->has_authorization_progress &&
           (!a->has_authorization_progress || a->authorization_progress == b->authorization_progress) &&
           a->opening_pending == b->opening_pending;
}
void jazz_protocol__protocol__ViewUpdatePayload__Clone__clone(jazz_protocol_ViewUpdatePayload *d, const jazz_protocol_ViewUpdatePayload *s) {
    d->subscription = s->subscription;
    d->settled_through = s->settled_through;
    carriers_clone(&d->version_carriers, &s->version_carriers);
    PeerPayloadInventory_clone(&d->peer_payload_inventory, &s->peer_payload_inventory);
    SupportingRowsUpdate_clone(&d->supporting_rows, &s->supporting_rows);
}
bool jazz_protocol__protocol__ViewUpdatePayload__PartialEq__eq(const jazz_protocol_ViewUpdatePayload *a, const jazz_protocol_ViewUpdatePayload *b) {
    return memcmp(&a->subscription, &b->subscription, sizeof a->subscription) == 0 && a->settled_through == b->settled_through &&
           carriers_eq(&a->version_carriers, &b->version_carriers) &&
           PeerPayloadInventory_eq(&a->peer_payload_inventory, &b->peer_payload_inventory) &&
           SupportingRowsUpdate_eq(&a->supporting_rows, &b->supporting_rows);
}
void jazz_protocol__protocol__ViewUpdatePayload__drop(jazz_protocol_ViewUpdatePayload *v) {
    carriers_drop(&v->version_carriers);
    jz_vec_drop(&v->peer_payload_inventory.complete_tx_payloads, sizeof(jazz_model_TxId), NULL);
    SupportingRowsUpdate_drop(&v->supporting_rows);
}

/* ---------------- KnownStateDeclaration ---------------- */
bool jazz_protocol__protocol__KnownStateDeclaration__PartialEq__eq(const jazz_protocol_KnownStateDeclaration *a, const jazz_protocol_KnownStateDeclaration *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_KSD_FAST: return a->completeness == b->completeness && a->position == b->position;
    case JAZZ_KSD_FAST_WITH_AUTH:
        return a->completeness == b->completeness && a->position == b->position && a->authorization_progress == b->authorization_progress;
    case JAZZ_KSD_RESERVED2: JZ_UNREACHABLE(); /* uninhabited */
    default:
        return a->position == b->position && a->has_authorization_progress == b->has_authorization_progress &&
               (!a->has_authorization_progress || a->authorization_progress == b->authorization_progress) &&
               memcmp(a->supporting_revision, b->supporting_revision, 16) == 0;
    }
}
void jazz_protocol__protocol__KnownStateCompleteness__Debug__fmt(jz_str *out, u32 c) { (void)c; jz_str_push_cstr(out, "FastCurrentMembership"); }

/* ---------------- PermissionAdviceAction ---------------- */
void jazz_protocol__protocol__PermissionAdviceAction__Clone__clone(jazz_protocol_PermissionAdviceAction *d, const jazz_protocol_PermissionAdviceAction *s) {
    d->tag = s->tag;
    d->table = jz_str_clone(&s->table);
    d->row = s->row;
    if (s->tag == JAZZ_PAA_INSERT || s->tag == JAZZ_PAA_UPDATE) value_map_clone(&d->cells, &s->cells);
    else jz_btree_init(&d->cells, &jazz_model__btree_desc_str_value);
}
static void PermissionAdviceAction_drop(jazz_protocol_PermissionAdviceAction *a) { jz_str_drop(&a->table); jz_btree_drop(&a->cells); }

/* ---------------- SubscribeRejectReason Debug ---------------- */
void jazz_protocol__protocol__SubscribeServerFailureCode__Debug__fmt(jz_str *out, u32 code) {
    static const char *const names[6] = { "TableNotFound", "SchemaResolution", "QueryValidation", "QueryLowering", "PolicyEvaluation", "Internal" };
    jz_str_push_cstr(out, names[code]);
}
void jazz_protocol__protocol__SubscribeRejectReason__Debug__fmt(jz_str *out, const jazz_protocol_SubscribeRejectReason *r) {
    switch (r->tag) {
    case JAZZ_SRR_UNSUPPORTED_SHAPE_CAPABILITY:
        jz_str_appendf(out, "UnsupportedShapeCapability { detail: \"%.*s\" }", (int)r->text.len, r->text.ptr); break;
    case JAZZ_SRR_PENDING_CATALOGUE: jz_str_push_cstr(out, "ShapeRegistrationPendingCatalogueAdmission"); break;
    case JAZZ_SRR_SERVER_FAILURE:
        jz_str_push_cstr(out, "ServerFailure { code: ");
        jazz_protocol__protocol__SubscribeServerFailureCode__Debug__fmt(out, r->code);
        jz_str_push_cstr(out, " }");
        break;
    default:
        jz_str_appendf(out, "InvalidAuthoritySourceClosure { transition: \"%.*s\" }", (int)r->text.len, r->text.ptr); break;
    }
}

/* ---------------- ResultRowSource ---------------- */
static void ResultRowSource_clone_v(void *d, const void *s) { jazz_protocol__protocol__ResultRowSource__Clone__clone(d, s); }
static void ResultRowSource_drop_v(void *s) { jazz_protocol__protocol__ResultRowSource__drop(s); }
void jazz_protocol__protocol__ResultRowSource__Clone__clone(jazz_protocol_ResultRowSource *d, const jazz_protocol_ResultRowSource *s) {
    d->tag = s->tag;
    switch (s->tag) {
    case JAZZ_RRS_CURRENT: break;
    case JAZZ_RRS_SNAPSHOT: jazz_model__model__branch__SnapshotRef__Clone__clone(&d->u.snapshot, &s->u.snapshot); break;
    case JAZZ_RRS_HISTORY_CUT: d->u.global_time = s->u.global_time; break;
    case JAZZ_RRS_MERGE: jz_vec_clone(&d->u.inputs, &s->u.inputs, sizeof(jazz_protocol_ResultRowSource), ResultRowSource_clone_v); break;
    case JAZZ_RRS_LENS_PROJECTION:
        d->u.lens.schema_version = s->u.lens.schema_version;
        d->u.lens.base = jz_alloc(sizeof(jazz_protocol_ResultRowSource));
        jazz_protocol__protocol__ResultRowSource__Clone__clone(d->u.lens.base, s->u.lens.base);
        break;
    case JAZZ_RRS_OVERLAY:
        d->u.overlay.tx = s->u.overlay.tx;
        d->u.overlay.base = jz_alloc(sizeof(jazz_protocol_ResultRowSource));
        jazz_protocol__protocol__ResultRowSource__Clone__clone(d->u.overlay.base, s->u.overlay.base);
        break;
    }
}
void jazz_protocol__protocol__ResultRowSource__drop(jazz_protocol_ResultRowSource *s) {
    switch (s->tag) {
    case JAZZ_RRS_SNAPSHOT: jazz_model__model__branch__SnapshotRef__drop(&s->u.snapshot); break;
    case JAZZ_RRS_MERGE: jz_vec_drop(&s->u.inputs, sizeof(jazz_protocol_ResultRowSource), ResultRowSource_drop_v); break;
    case JAZZ_RRS_LENS_PROJECTION: jazz_protocol__protocol__ResultRowSource__drop(s->u.lens.base); jz_free(s->u.lens.base); break;
    case JAZZ_RRS_OVERLAY: jazz_protocol__protocol__ResultRowSource__drop(s->u.overlay.base); jz_free(s->u.overlay.base); break;
    default: break;
    }
}
int jazz_protocol__protocol__ResultRowSource__Ord__cmp(const jazz_protocol_ResultRowSource *a, const jazz_protocol_ResultRowSource *b) {
    if (a->tag != b->tag) return a->tag < b->tag ? -1 : 1;
    int c;
    switch (a->tag) {
    case JAZZ_RRS_SNAPSHOT: return jazz_model__model__branch__SnapshotRef__Ord__cmp(&a->u.snapshot, &b->u.snapshot);
    case JAZZ_RRS_HISTORY_CUT: return u64_cmp(a->u.global_time, b->u.global_time);
    case JAZZ_RRS_MERGE: {
        u32 n = a->u.inputs.len < b->u.inputs.len ? a->u.inputs.len : b->u.inputs.len;
        for (u32 i = 0; i < n; i++)
            if ((c = jazz_protocol__protocol__ResultRowSource__Ord__cmp(&a->u.inputs.ptr[i], &b->u.inputs.ptr[i]))) return c;
        return u64_cmp(a->u.inputs.len, b->u.inputs.len);
    }
    case JAZZ_RRS_LENS_PROJECTION:
        if ((c = jz_uuid_cmp(&a->u.lens.schema_version, &b->u.lens.schema_version))) return c;
        return jazz_protocol__protocol__ResultRowSource__Ord__cmp(a->u.lens.base, b->u.lens.base);
    case JAZZ_RRS_OVERLAY:
        if ((c = jazz_model__tx__TxId__cmp(&a->u.overlay.tx, &b->u.overlay.tx, NULL))) return c;
        return jazz_protocol__protocol__ResultRowSource__Ord__cmp(a->u.overlay.base, b->u.overlay.base);
    default: return 0;
    }
}
bool jazz_protocol__protocol__ResultRowSource__PartialEq__eq(const jazz_protocol_ResultRowSource *a, const jazz_protocol_ResultRowSource *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_RRS_SNAPSHOT: return jazz_model__model__branch__SnapshotRef__Ord__cmp(&a->u.snapshot, &b->u.snapshot) == 0;
    case JAZZ_RRS_HISTORY_CUT: return a->u.global_time == b->u.global_time;
    case JAZZ_RRS_MERGE:
        if (a->u.inputs.len != b->u.inputs.len) return false;
        for (u32 i = 0; i < a->u.inputs.len; i++)
            if (!jazz_protocol__protocol__ResultRowSource__PartialEq__eq(&a->u.inputs.ptr[i], &b->u.inputs.ptr[i])) return false;
        return true;
    case JAZZ_RRS_LENS_PROJECTION:
        return jz_uuid_eq(&a->u.lens.schema_version, &b->u.lens.schema_version) &&
               jazz_protocol__protocol__ResultRowSource__PartialEq__eq(a->u.lens.base, b->u.lens.base);
    case JAZZ_RRS_OVERLAY:
        return jazz_model__tx__TxId__PartialEq__eq(&a->u.overlay.tx, &b->u.overlay.tx) &&
               jazz_protocol__protocol__ResultRowSource__PartialEq__eq(a->u.overlay.base, b->u.overlay.base);
    default: return true;
    }
}
void jazz_protocol__protocol__ResultRowSource__Debug__fmt(jz_str *out, const jazz_protocol_ResultRowSource *s) {
    switch (s->tag) {
    case JAZZ_RRS_CURRENT: jz_str_push_cstr(out, "Current"); break;
    case JAZZ_RRS_SNAPSHOT:
        jz_str_push_cstr(out, "Snapshot { snapshot: ");
        jazz_model__model__branch__SnapshotRef__Debug__fmt(out, &s->u.snapshot);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_RRS_HISTORY_CUT:
        jz_str_push_cstr(out, "HistoryCut { global_time: ");
        jazz_types__time__GlobalTime__Debug__fmt(out, s->u.global_time);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_RRS_MERGE:
        jz_str_push_cstr(out, "Merge { inputs: [");
        for (u32 i = 0; i < s->u.inputs.len; i++) {
            if (i) jz_str_push_cstr(out, ", ");
            jazz_protocol__protocol__ResultRowSource__Debug__fmt(out, &s->u.inputs.ptr[i]);
        }
        jz_str_push_cstr(out, "] }");
        break;
    case JAZZ_RRS_LENS_PROJECTION:
        jz_str_push_cstr(out, "LensProjection { schema_version: ");
        jazz_types__ids__uuid_id__Debug__fmt(out, "SchemaVersionId", &s->u.lens.schema_version);
        jz_str_push_cstr(out, ", base: ");
        jazz_protocol__protocol__ResultRowSource__Debug__fmt(out, s->u.lens.base);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_RRS_OVERLAY:
        jz_str_push_cstr(out, "Overlay { tx: ");
        jazz_model__tx__TxId__Debug__fmt(out, &s->u.overlay.tx);
        jz_str_push_cstr(out, ", base: ");
        jazz_protocol__protocol__ResultRowSource__Debug__fmt(out, s->u.overlay.base);
        jz_str_push_cstr(out, " }");
        break;
    }
}

/* ---------------- RealRowMemberEntry ---------------- */
void jazz_protocol__protocol__RealRowMemberEntry__Clone__clone(jazz_protocol_RealRowMemberEntry *d, const jazz_protocol_RealRowMemberEntry *s) {
    d->table = s->table;
    d->row_uuid = s->row_uuid;
    d->has_occurrence_id = s->has_occurrence_id;
    if (s->has_occurrence_id) jazz_types__object__OutputOccurrenceId__Clone__clone(&d->occurrence_id, &s->occurrence_id);
    d->content_tx = s->content_tx;
    d->layer = s->layer;
    d->deletion_tx = s->deletion_tx;
    jazz_protocol__protocol__ResultRowSource__Clone__clone(&d->source, &s->source);
    d->read_view = s->read_view;
    d->has_schema_version = s->has_schema_version; d->schema_version = s->schema_version;
    ob_clone(&d->branch_or_prefix, &s->branch_or_prefix);
    ob_clone(&d->row_digest, &s->row_digest);
    d->batch = s->batch;
    d->has_settle_position = s->has_settle_position; d->settle_position = s->settle_position;
}
void jazz_protocol__protocol__RealRowMemberEntry__drop(jazz_protocol_RealRowMemberEntry *e) {
    if (e->has_occurrence_id) jazz_types__object__OutputOccurrenceId__drop(&e->occurrence_id);
    jazz_protocol__protocol__ResultRowSource__drop(&e->source);
    ob_drop(&e->branch_or_prefix);
    ob_drop(&e->row_digest);
}
int jazz_protocol__protocol__RealRowMemberEntry__Ord__cmp(const jazz_protocol_RealRowMemberEntry *a, const jazz_protocol_RealRowMemberEntry *b) {
    int c = istr_cmp(a->table, b->table);
    if (c) return c;
    if ((c = jz_uuid_cmp(&a->row_uuid, &b->row_uuid))) return c;
    if (a->has_occurrence_id != b->has_occurrence_id) return bool_cmp(a->has_occurrence_id, b->has_occurrence_id);
    if (a->has_occurrence_id && (c = jazz_types__object__OutputOccurrenceId__Ord__cmp(&a->occurrence_id, &b->occurrence_id))) return c;
    if ((c = otx_cmp(&a->content_tx, &b->content_tx))) return c;
    if (a->layer != b->layer) return u64_cmp(a->layer, b->layer);
    if ((c = otx_cmp(&a->deletion_tx, &b->deletion_tx))) return c;
    if ((c = jazz_protocol__protocol__ResultRowSource__Ord__cmp(&a->source, &b->source))) return c;
    if ((c = jz_uuid_cmp(&a->read_view.id, &b->read_view.id))) return c;
    if ((c = ouuid_cmp(a->has_schema_version, &a->schema_version, b->has_schema_version, &b->schema_version))) return c;
    if ((c = ob_cmp(&a->branch_or_prefix, &b->branch_or_prefix))) return c;
    if ((c = ob_cmp(&a->row_digest, &b->row_digest))) return c;
    if ((c = otx_cmp(&a->batch, &b->batch))) return c;
    if (a->has_settle_position != b->has_settle_position) return bool_cmp(a->has_settle_position, b->has_settle_position);
    return a->has_settle_position ? u64_cmp(a->settle_position, b->settle_position) : 0;
}
bool jazz_protocol__protocol__RealRowMemberEntry__PartialEq__eq(const jazz_protocol_RealRowMemberEntry *a, const jazz_protocol_RealRowMemberEntry *b) {
    if (a->table != b->table || !jz_uuid_eq(&a->row_uuid, &b->row_uuid) || a->has_occurrence_id != b->has_occurrence_id) return false;
    if (a->has_occurrence_id && !jazz_types__object__OutputOccurrenceId__PartialEq__eq(&a->occurrence_id, &b->occurrence_id)) return false;
    if (otx_cmp(&a->content_tx, &b->content_tx) || a->layer != b->layer || otx_cmp(&a->deletion_tx, &b->deletion_tx)) return false;
    if (!jazz_protocol__protocol__ResultRowSource__PartialEq__eq(&a->source, &b->source)) return false;
    return jz_uuid_eq(&a->read_view.id, &b->read_view.id) &&
           !ouuid_cmp(a->has_schema_version, &a->schema_version, b->has_schema_version, &b->schema_version) &&
           !ob_cmp(&a->branch_or_prefix, &b->branch_or_prefix) && !ob_cmp(&a->row_digest, &b->row_digest) &&
           !otx_cmp(&a->batch, &b->batch) && a->has_settle_position == b->has_settle_position &&
           (!a->has_settle_position || a->settle_position == b->settle_position);
}
void jazz_protocol__protocol__RealRowMemberEntry__Debug__fmt(jz_str *out, const jazz_protocol_RealRowMemberEntry *e) {
    jz_str_push_cstr(out, "RealRowMemberEntry { table: ");
    debug_istr(out, e->table);
    jz_str_push_cstr(out, ", row_uuid: ");
    jazz_types__ids__uuid_id__Debug__fmt(out, "RowUuid", &e->row_uuid);
    jz_str_push_cstr(out, ", occurrence_id: ");
    if (e->has_occurrence_id) { jz_str_push_cstr(out, "Some("); jazz_types__object__OutputOccurrenceId__Debug__fmt(out, &e->occurrence_id); jz_str_push_cstr(out, ")"); }
    else jz_str_push_cstr(out, "None");
    jz_str_push_cstr(out, ", content_tx: "); debug_opt_tx(out, &e->content_tx);
    jz_str_push_cstr(out, ", layer: "); jazz_protocol__protocol__ResultRowLayer__Debug__fmt(out, e->layer);
    jz_str_push_cstr(out, ", deletion_tx: "); debug_opt_tx(out, &e->deletion_tx);
    jz_str_push_cstr(out, ", source: "); jazz_protocol__protocol__ResultRowSource__Debug__fmt(out, &e->source);
    jz_str_push_cstr(out, ", read_view: "); jazz_protocol__protocol__ReadViewKey__Debug__fmt(out, &e->read_view);
    jz_str_push_cstr(out, ", schema_version: "); debug_opt_uuid(out, "SchemaVersionId", e->has_schema_version, &e->schema_version);
    jz_str_push_cstr(out, ", branch_or_prefix: "); debug_opt_bytes(out, &e->branch_or_prefix);
    jz_str_push_cstr(out, ", row_digest: "); debug_opt_bytes(out, &e->row_digest);
    jz_str_push_cstr(out, ", batch: "); debug_opt_tx(out, &e->batch);
    jz_str_push_cstr(out, ", settle_position: ");
    if (e->has_settle_position) { jz_str_push_cstr(out, "Some("); jazz_types__time__GlobalTime__Debug__fmt(out, e->settle_position); jz_str_push_cstr(out, ")"); }
    else jz_str_push_cstr(out, "None");
    jz_str_push_cstr(out, " }");
}
void jazz_protocol__protocol__RealRowMemberEntry__current_content(jazz_protocol_RealRowMemberEntry *out, jazz_protocol_ResultRowEntry row) {
    memset(out, 0, sizeof *out);
    out->table = row.table;
    out->row_uuid = row.row;
    out->has_occurrence_id = true;
    jazz_types__object__OutputOccurrenceId__single_source(&out->occurrence_id, jazz_types__object__ObjectId__from_uuid(&row.row));
    out->content_tx.some = true; out->content_tx.v = row.tx;
    out->layer = JAZZ_LAYER_CONTENT;
    out->source.tag = JAZZ_RRS_CURRENT;
}
void jazz_protocol__protocol__RealRowMemberEntry__output_occurrence_id(const jazz_protocol_RealRowMemberEntry *e, jazz_types_OutputOccurrenceId *out) {
    if (e->has_occurrence_id) jazz_types__object__OutputOccurrenceId__Clone__clone(out, &e->occurrence_id);
    else jazz_types__object__OutputOccurrenceId__single_source(out, jazz_types__object__ObjectId__from_uuid(&e->row_uuid));
}
static bool RealRowMemberEntry_row_projection(const jazz_protocol_RealRowMemberEntry *e, jazz_protocol_ResultRowEntry *out) {
    if (!e->content_tx.some) return false;
    out->table = e->table; out->row = e->row_uuid; out->tx = e->content_tx.v;
    return true;
}

/* ---------------- ResultMemberEntry ---------------- */
void jazz_protocol__protocol__ResultMemberEntry__Clone__clone(jazz_protocol_ResultMemberEntry *d, const jazz_protocol_ResultMemberEntry *s) {
    d->tag = s->tag;
    switch (s->tag) {
    case JAZZ_RME_ROW: jazz_protocol__protocol__RealRowMemberEntry__Clone__clone(&d->u.row, &s->u.row); break;
    case JAZZ_RME_SYNTHETIC:
        d->u.synthetic.table = jz_str_clone(&s->u.synthetic.table);
        d->u.synthetic.row = jz_bytes_clone(&s->u.synthetic.row);
        d->u.synthetic.replacement = jz_bytes_clone(&s->u.synthetic.replacement);
        break;
    case JAZZ_RME_PATH_TUPLE:
        d->u.path_tuple = s->u.path_tuple;
        d->u.path_tuple.path = jz_str_clone(&s->u.path_tuple.path);
        ob_clone(&d->u.path_tuple.edge_id, &s->u.path_tuple.edge_id);
        d->u.path_tuple.revision = jz_bytes_clone(&s->u.path_tuple.revision);
        break;
    case JAZZ_RME_TYPED_ROW:
        jazz_protocol__protocol__RealRowMemberEntry__Clone__clone(&d->u.typed.row, &s->u.typed.row);
        jazz_types__object__OutputOccurrenceId__Clone__clone(&d->u.typed.occurrence_key.occ, &s->u.typed.occurrence_key.occ);
        break;
    }
}
void jazz_protocol__protocol__ResultMemberEntry__drop(jazz_protocol_ResultMemberEntry *e) {
    switch (e->tag) {
    case JAZZ_RME_ROW: jazz_protocol__protocol__RealRowMemberEntry__drop(&e->u.row); break;
    case JAZZ_RME_SYNTHETIC:
        jz_str_drop(&e->u.synthetic.table); jz_bytes_drop(&e->u.synthetic.row); jz_bytes_drop(&e->u.synthetic.replacement); break;
    case JAZZ_RME_PATH_TUPLE:
        jz_str_drop(&e->u.path_tuple.path); ob_drop(&e->u.path_tuple.edge_id); jz_bytes_drop(&e->u.path_tuple.revision); break;
    case JAZZ_RME_TYPED_ROW:
        jazz_protocol__protocol__RealRowMemberEntry__drop(&e->u.typed.row);
        jazz_types__object__OutputOccurrenceId__drop(&e->u.typed.occurrence_key.occ);
        break;
    }
}
int jazz_protocol__protocol__ResultMemberEntry__Ord__cmp(const jazz_protocol_ResultMemberEntry *a, const jazz_protocol_ResultMemberEntry *b) {
    if (a->tag != b->tag) return a->tag < b->tag ? -1 : 1;
    int c;
    switch (a->tag) {
    case JAZZ_RME_ROW: return jazz_protocol__protocol__RealRowMemberEntry__Ord__cmp(&a->u.row, &b->u.row);
    case JAZZ_RME_SYNTHETIC:
        if ((c = jz_str_cmp(&a->u.synthetic.table, &b->u.synthetic.table))) return c;
        if ((c = jz_bytes_cmp(&a->u.synthetic.row, &b->u.synthetic.row))) return c;
        return jz_bytes_cmp(&a->u.synthetic.replacement, &b->u.synthetic.replacement);
    case JAZZ_RME_PATH_TUPLE: {
        const __typeof__(a->u.path_tuple) *x = &a->u.path_tuple, *y = &b->u.path_tuple;
        if ((c = jz_str_cmp(&x->path, &y->path))) return c;
        if ((c = istr_cmp(x->source_table, y->source_table))) return c;
        if ((c = jz_uuid_cmp(&x->source_row, &y->source_row))) return c;
        if ((c = istr_cmp(x->target_table, y->target_table))) return c;
        if ((c = jz_uuid_cmp(&x->target_row, &y->target_row))) return c;
        if ((c = ob_cmp(&x->edge_id, &y->edge_id))) return c;
        return jz_bytes_cmp(&x->revision, &y->revision);
    }
    default:
        if ((c = jazz_protocol__protocol__RealRowMemberEntry__Ord__cmp(&a->u.typed.row, &b->u.typed.row))) return c;
        return jazz_types__object__OutputOccurrenceId__Ord__cmp(&a->u.typed.occurrence_key.occ, &b->u.typed.occurrence_key.occ);
    }
}
bool jazz_protocol__protocol__ResultMemberEntry__PartialEq__eq(const jazz_protocol_ResultMemberEntry *a, const jazz_protocol_ResultMemberEntry *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_RME_ROW: return jazz_protocol__protocol__RealRowMemberEntry__PartialEq__eq(&a->u.row, &b->u.row);
    case JAZZ_RME_TYPED_ROW:
        return jazz_protocol__protocol__RealRowMemberEntry__PartialEq__eq(&a->u.typed.row, &b->u.typed.row) &&
               jazz_types__object__OutputOccurrenceId__PartialEq__eq(&a->u.typed.occurrence_key.occ, &b->u.typed.occurrence_key.occ);
    default: return jazz_protocol__protocol__ResultMemberEntry__Ord__cmp(a, b) == 0;
    }
}
void jazz_protocol__protocol__ResultMemberEntry__Debug__fmt(jz_str *out, const jazz_protocol_ResultMemberEntry *e) {
    switch (e->tag) {
    case JAZZ_RME_ROW:
        jz_str_push_cstr(out, "Row("); jazz_protocol__protocol__RealRowMemberEntry__Debug__fmt(out, &e->u.row); jz_str_push_cstr(out, ")"); break;
    case JAZZ_RME_SYNTHETIC:
        jz_str_appendf(out, "Synthetic { table: \"%.*s\", row: [%u bytes], replacement: SyntheticReplacementToken(..) }",
                       (int)e->u.synthetic.table.len, e->u.synthetic.table.ptr, e->u.synthetic.row.len);
        break;
    case JAZZ_RME_PATH_TUPLE: {
        const __typeof__(e->u.path_tuple) *p = &e->u.path_tuple;
        jz_str_appendf(out, "PathTuple { path: \"%.*s\", source_table: ", (int)p->path.len, p->path.ptr);
        debug_istr(out, p->source_table);
        jz_str_push_cstr(out, ", source_row: "); jazz_types__ids__uuid_id__Debug__fmt(out, "RowUuid", &p->source_row);
        jz_str_push_cstr(out, ", target_table: "); debug_istr(out, p->target_table);
        jz_str_push_cstr(out, ", target_row: "); jazz_types__ids__uuid_id__Debug__fmt(out, "RowUuid", &p->target_row);
        jz_str_push_cstr(out, ", edge_id: "); debug_opt_bytes(out, &p->edge_id);
        jz_str_appendf(out, ", revision: [%u bytes] }", p->revision.len);
        break;
    }
    case JAZZ_RME_TYPED_ROW:
        jz_str_push_cstr(out, "TypedRow { row: ");
        jazz_protocol__protocol__RealRowMemberEntry__Debug__fmt(out, &e->u.typed.row);
        jz_str_push_cstr(out, ", occurrence_key: ResultKey(");
        jazz_types__object__OutputOccurrenceId__Debug__fmt(out, &e->u.typed.occurrence_key.occ);
        jz_str_push_cstr(out, ") }");
        break;
    }
}
bool jazz_protocol__protocol__ResultMemberEntry__as_row(const jazz_protocol_ResultMemberEntry *e, jazz_protocol_ResultRowEntry *out) {
    switch (e->tag) {
    case JAZZ_RME_ROW: return RealRowMemberEntry_row_projection(&e->u.row, out);
    case JAZZ_RME_TYPED_ROW: return RealRowMemberEntry_row_projection(&e->u.typed.row, out);
    default: return false;
    }
}
bool jazz_protocol__protocol__ResultMemberEntry__output_occurrence_id(const jazz_protocol_ResultMemberEntry *e, jazz_types_OutputOccurrenceId *out) {
    switch (e->tag) {
    case JAZZ_RME_TYPED_ROW: jazz_types__object__OutputOccurrenceId__Clone__clone(out, &e->u.typed.occurrence_key.occ); return true;
    case JAZZ_RME_ROW: jazz_protocol__protocol__RealRowMemberEntry__output_occurrence_id(&e->u.row, out); return true;
    default: return false;
    }
}
void jazz_protocol__protocol__ResultMemberEntry__From_RealRowMemberEntry__from(jazz_protocol_ResultMemberEntry *out, jazz_protocol_RealRowMemberEntry entry) {
    if (entry.has_occurrence_id && jazz_types__object__OutputOccurrenceId__has_typed_discriminators(&entry.occurrence_id)) {
        out->tag = JAZZ_RME_TYPED_ROW;
        jazz_types__object__OutputOccurrenceId__Clone__clone(&out->u.typed.occurrence_key.occ, &entry.occurrence_id);
        out->u.typed.row = entry;
    } else {
        out->tag = JAZZ_RME_ROW;
        out->u.row = entry;
    }
}
static void opt_member_clone(jazz_protocol_OptMember *d, const jazz_protocol_OptMember *s) {
    d->v = NULL;
    if (s->v) { d->v = jz_alloc(sizeof *d->v); jazz_protocol__protocol__ResultMemberEntry__Clone__clone(d->v, s->v); }
}
static void opt_member_drop(jazz_protocol_OptMember *o) { if (o->v) { jazz_protocol__protocol__ResultMemberEntry__drop(o->v); jz_free(o->v); } }
static int opt_member_cmp(const jazz_protocol_OptMember *a, const jazz_protocol_OptMember *b) {
    if (!a->v || !b->v) return bool_cmp(a->v != NULL, b->v != NULL);
    return jazz_protocol__protocol__ResultMemberEntry__Ord__cmp(a->v, b->v);
}

/* ---------------- ProgramSourceId ---------------- */
static void ProgramSourceRole_clone_v(void *d_, const void *s_) {
    jazz_protocol_ProgramSourceRole *d = d_; const jazz_protocol_ProgramSourceRole *s = s_;
    d->tag = s->tag; d->name = s->tag == JAZZ_PSR_ROOT ? (jz_str){0} : jz_str_clone(&s->name);
}
static void ProgramSourceRole_drop_v(void *p) { jazz_protocol_ProgramSourceRole *r = p; if (r->tag != JAZZ_PSR_ROOT) jz_str_drop(&r->name); }
static void ProgramSourceId_clone(jazz_protocol_ProgramSourceId *d, const jazz_protocol_ProgramSourceId *s) {
    d->table = s->table;
    jz_vec_clone(&d->path, &s->path, sizeof(jazz_protocol_ProgramSourceRole), ProgramSourceRole_clone_v);
}
static void ProgramSourceId_drop(jazz_protocol_ProgramSourceId *s) { jz_vec_drop(&s->path, sizeof(jazz_protocol_ProgramSourceRole), ProgramSourceRole_drop_v); }
static int ProgramSourceId_cmp(const jazz_protocol_ProgramSourceId *a, const jazz_protocol_ProgramSourceId *b) {
    int c = istr_cmp(a->table, b->table);
    if (c) return c;
    u32 n = a->path.len < b->path.len ? a->path.len : b->path.len;
    for (u32 i = 0; i < n; i++) {
        const jazz_protocol_ProgramSourceRole *x = &a->path.ptr[i], *y = &b->path.ptr[i];
        if (x->tag != y->tag) return x->tag < y->tag ? -1 : 1;
        if (x->tag != JAZZ_PSR_ROOT && (c = jz_str_cmp(&x->name, &y->name))) return c;
    }
    return u64_cmp(a->path.len, b->path.len);
}
bool jazz_protocol__protocol__ProgramSourceId__is_wire_valid(const jazz_protocol_ProgramSourceId *s) {
    if (s->table->len == 0 || s->path.len == 0 || s->path.len > JAZZ_MAX_PROGRAM_SOURCE_PATH_SEGMENTS) return false;
    for (u32 i = 0; i < s->path.len; i++)
        if (s->path.ptr[i].tag != JAZZ_PSR_ROOT && s->path.ptr[i].name.len == 0) return false;
    return true;
}
bool jazz_protocol__protocol__CoveredInputEntry__is_wire_valid(const jazz_protocol_CoveredInputEntry *e) {
    return jazz_protocol__protocol__ProgramSourceId__is_wire_valid(&e->source) && e->version_table->len != 0;
}
void jazz_protocol__protocol__ProgramSourceId__Debug__fmt(jz_str *out, const jazz_protocol_ProgramSourceId *s) {
    static const char *const roles[6] = { "Root", "Alias", "RecursiveSeed", "RecursiveStep", "CorrelatedChild", "Policy" };
    jz_str_push_cstr(out, "ProgramSourceId { table: ");
    debug_istr(out, s->table);
    jz_str_push_cstr(out, ", path: [");
    for (u32 i = 0; i < s->path.len; i++) {
        const jazz_protocol_ProgramSourceRole *r = &s->path.ptr[i];
        if (i) jz_str_push_cstr(out, ", ");
        if (r->tag == JAZZ_PSR_ROOT) jz_str_push_cstr(out, "Root");
        else jz_str_appendf(out, "%s(\"%.*s\")", roles[r->tag], (int)r->name.len, r->name.ptr);
    }
    jz_str_push_cstr(out, "] }");
}
void jazz_protocol__protocol__PolicyDecisionOutcomeEntry__Debug__fmt(jz_str *out, const jazz_protocol_PolicyDecisionOutcomeEntry *e) {
    switch (e->tag) {
    case JAZZ_PDO_ALLOWED: jz_str_push_cstr(out, "Allowed"); break;
    case JAZZ_PDO_DENIED: jz_str_push_cstr(out, "Denied"); break;
    case JAZZ_PDO_INDETERMINATE_REQUIRES_INPUT:
        jz_str_appendf(out, "IndeterminateRequiresInput { input: \"%.*s\" }", (int)e->s.len, e->s.ptr); break;
    default:
        jz_str_appendf(out, "RequiresCoverage { scope: \"%.*s\", frontier: [%u bytes] }", (int)e->s.len, e->s.ptr, e->frontier.len); break;
    }
}
void jazz_protocol__protocol__PredicateOutputSetRoleEntry__Debug__fmt(jz_str *out, u32 role) { jz_str_push_cstr(out, role == JAZZ_POSR_BASE ? "Base" : "Now"); }
void jazz_protocol__protocol__ResultMemberPayloadEntry__Debug__fmt(jz_str *out, const jazz_protocol_ResultMemberPayloadEntry *e) {
    jz_str_push_cstr(out, "ResultMemberPayloadEntry { member: ");
    jazz_protocol__protocol__ResultMemberEntry__Debug__fmt(out, &e->member);
    jz_str_appendf(out, ", descriptor: [%u bytes], record: [%u bytes] }", e->descriptor.len, e->record.len);
}

/* ---------------- ProgramFactEntry ---------------- */
static void RelationEdgeEntry_clone(jazz_protocol_RelationEdgeEntry *d, const jazz_protocol_RelationEdgeEntry *s) {
    *d = *s;
    d->path = jz_str_clone(&s->path);
    if (s->has_source_version) jazz_protocol__protocol__RowVersionRefEntry__Clone__clone(&d->source_version, &s->source_version);
    if (s->has_target_version) jazz_protocol__protocol__RowVersionRefEntry__Clone__clone(&d->target_version, &s->target_version);
    ob_clone(&d->edge_id, &s->edge_id);
    ob_clone(&d->branch, &s->branch);
    ob_clone(&d->order, &s->order);
}
static void RelationEdgeEntry_drop(jazz_protocol_RelationEdgeEntry *e) {
    jz_str_drop(&e->path);
    if (e->has_source_version) jazz_protocol__protocol__RowVersionRefEntry__drop(&e->source_version);
    if (e->has_target_version) jazz_protocol__protocol__RowVersionRefEntry__drop(&e->target_version);
    ob_drop(&e->edge_id); ob_drop(&e->branch); ob_drop(&e->order);
}
static int opt_rvre_cmp(bool ha, const jazz_protocol_RowVersionRefEntry *a, bool hb, const jazz_protocol_RowVersionRefEntry *b) {
    if (ha != hb) return bool_cmp(ha, hb);
    return ha ? jazz_protocol__protocol__RowVersionRefEntry__Ord__cmp(a, b) : 0;
}
static int RelationEdgeEntry_cmp(const jazz_protocol_RelationEdgeEntry *a, const jazz_protocol_RelationEdgeEntry *b) {
    int c;
    if ((c = jz_str_cmp(&a->path, &b->path))) return c;
    if ((c = istr_cmp(a->source_table, b->source_table))) return c;
    if ((c = jz_uuid_cmp(&a->source_row, &b->source_row))) return c;
    if ((c = istr_cmp(a->target_table, b->target_table))) return c;
    if ((c = jz_uuid_cmp(&a->target_row, &b->target_row))) return c;
    if ((c = oe_cmp(&a->kind, &b->kind))) return c;
    if ((c = opt_rvre_cmp(a->has_source_version, &a->source_version, b->has_source_version, &b->source_version))) return c;
    if ((c = opt_rvre_cmp(a->has_target_version, &a->target_version, b->has_target_version, &b->target_version))) return c;
    if (a->has_depth != b->has_depth) return bool_cmp(a->has_depth, b->has_depth);
    if (a->has_depth && a->depth != b->depth) return u64_cmp(a->depth, b->depth);
    if ((c = ob_cmp(&a->edge_id, &b->edge_id))) return c;
    if ((c = ob_cmp(&a->branch, &b->branch))) return c;
    if ((c = oe_cmp(&a->role, &b->role))) return c;
    if ((c = ob_cmp(&a->order, &b->order))) return c;
    return oe_cmp(&a->hole_state, &b->hole_state);
}

void jazz_protocol__protocol__ProgramFactEntry__Clone__clone(jazz_protocol_ProgramFactEntry *d, const jazz_protocol_ProgramFactEntry *s) {
    d->tag = s->tag;
    switch (s->tag) {
    case JAZZ_PFE_RESULT_PAYLOAD:
        jazz_protocol__protocol__ResultMemberEntry__Clone__clone(&d->u.result_payload.member, &s->u.result_payload.member);
        d->u.result_payload.descriptor = jz_bytes_clone(&s->u.result_payload.descriptor);
        d->u.result_payload.record = jz_bytes_clone(&s->u.result_payload.record);
        break;
    case JAZZ_PFE_RELATION_EDGE: RelationEdgeEntry_clone(&d->u.relation_edge, &s->u.relation_edge); break;
    case JAZZ_PFE_PATH_CORRELATION_COVERAGE:
        d->u.path_correlation = s->u.path_correlation;
        d->u.path_correlation.path = jz_str_clone(&s->u.path_correlation.path);
        d->u.path_correlation.correlation_key = jz_bytes_clone(&s->u.path_correlation.correlation_key);
        break;
    case JAZZ_PFE_PROGRAM_SOURCE_COVERAGE:
        ProgramSourceId_clone(&d->u.program_source.source, &s->u.program_source.source);
        d->u.program_source.complete = s->u.program_source.complete;
        break;
    case JAZZ_PFE_READ_FRONTIER_SETTLED:
        d->u.read_frontier.scope = jz_str_clone(&s->u.read_frontier.scope);
        d->u.read_frontier.tier = s->u.read_frontier.tier;
        os_clone(&d->u.read_frontier.stream, &s->u.read_frontier.stream);
        d->u.read_frontier.frontier = jz_bytes_clone(&s->u.read_frontier.frontier);
        break;
    case JAZZ_PFE_COMPLETE_TX_PAYLOAD_COVERAGE:
        d->u.complete_tx = s->u.complete_tx;
        d->u.complete_tx.payload_digest = jz_bytes_clone(&s->u.complete_tx.payload_digest);
        break;
    case JAZZ_PFE_VIEW_COMPLETE_EXCLUSIVE_COVERAGE:
        d->u.view_complete.tx = s->u.view_complete.tx;
        d->u.view_complete.scope = jz_str_clone(&s->u.view_complete.scope);
        opt_member_clone(&d->u.view_complete.result, &s->u.view_complete.result);
        d->u.view_complete.tier = s->u.view_complete.tier;
        d->u.view_complete.covered_members_digest = jz_bytes_clone(&s->u.view_complete.covered_members_digest);
        break;
    case JAZZ_PFE_POLICY_DECISION:
        d->u.policy_decision.decision = jz_bytes_clone(&s->u.policy_decision.decision);
        d->u.policy_decision.outcome.tag = s->u.policy_decision.outcome.tag;
        d->u.policy_decision.outcome.s = jz_str_clone(&s->u.policy_decision.outcome.s);
        d->u.policy_decision.outcome.frontier = jz_bytes_clone(&s->u.policy_decision.outcome.frontier);
        os_clone(&d->u.policy_decision.reason, &s->u.policy_decision.reason);
        break;
    case JAZZ_PFE_VERSION_WITNESS:
        d->u.version_witness.role = jz_str_clone(&s->u.version_witness.role);
        jazz_protocol__protocol__RowVersionRefEntry__Clone__clone(&d->u.version_witness.version, &s->u.version_witness.version);
        opt_member_clone(&d->u.version_witness.member, &s->u.version_witness.member);
        break;
    case JAZZ_PFE_COVERED_INPUT:
        ProgramSourceId_clone(&d->u.covered_input.source, &s->u.covered_input.source);
        d->u.covered_input.version_table = s->u.covered_input.version_table;
        d->u.covered_input.source_row = s->u.covered_input.source_row;
        jazz_protocol__protocol__RowVersionRefEntry__Clone__clone(&d->u.covered_input.version, &s->u.covered_input.version);
        break;
    case JAZZ_PFE_POLICY_WITNESS:
        jazz_protocol__protocol__ResultMemberEntry__Clone__clone(&d->u.policy_witness.protected_, &s->u.policy_witness.protected_);
        d->u.policy_witness.policy_path = jz_str_clone(&s->u.policy_witness.policy_path);
        jazz_protocol__protocol__RowVersionRefEntry__Clone__clone(&d->u.policy_witness.witness, &s->u.policy_witness.witness);
        d->u.policy_witness.edge_kind = s->u.policy_witness.edge_kind;
        break;
    case JAZZ_PFE_CONTRIBUTING_MEMBERS:
        jazz_protocol__protocol__ResultMemberEntry__Clone__clone(&d->u.contributing.result, &s->u.contributing.result);
        jazz_protocol__protocol__ResultMemberEntry__Clone__clone(&d->u.contributing.contributor, &s->u.contributing.contributor);
        d->u.contributing.batch = s->u.contributing.batch;
        os_clone(&d->u.contributing.role, &s->u.contributing.role);
        break;
    case JAZZ_PFE_PREDICATE_READ:
        d->u.predicate_read = s->u.predicate_read;
        d->u.predicate_read.predicate = jz_bytes_clone(&s->u.predicate_read.predicate);
        d->u.predicate_read.frontier = jz_bytes_clone(&s->u.predicate_read.frontier);
        break;
    case JAZZ_PFE_PREDICATE_OUTPUT_SET:
        d->u.predicate_output = s->u.predicate_output;
        jazz_protocol__protocol__RowVersionRefEntry__Clone__clone(&d->u.predicate_output.version, &s->u.predicate_output.version);
        break;
    case JAZZ_PFE_POINT_READ:
        d->u.point_read = s->u.point_read;
        if (s->u.point_read.has_version) jazz_protocol__protocol__RowVersionRefEntry__Clone__clone(&d->u.point_read.version, &s->u.point_read.version);
        break;
    }
}
void jazz_protocol__protocol__ProgramFactEntry__drop(jazz_protocol_ProgramFactEntry *e) {
    switch (e->tag) {
    case JAZZ_PFE_RESULT_PAYLOAD:
        jazz_protocol__protocol__ResultMemberEntry__drop(&e->u.result_payload.member);
        jz_bytes_drop(&e->u.result_payload.descriptor); jz_bytes_drop(&e->u.result_payload.record); break;
    case JAZZ_PFE_RELATION_EDGE: RelationEdgeEntry_drop(&e->u.relation_edge); break;
    case JAZZ_PFE_PATH_CORRELATION_COVERAGE: jz_str_drop(&e->u.path_correlation.path); jz_bytes_drop(&e->u.path_correlation.correlation_key); break;
    case JAZZ_PFE_PROGRAM_SOURCE_COVERAGE: ProgramSourceId_drop(&e->u.program_source.source); break;
    case JAZZ_PFE_READ_FRONTIER_SETTLED:
        jz_str_drop(&e->u.read_frontier.scope); os_drop(&e->u.read_frontier.stream); jz_bytes_drop(&e->u.read_frontier.frontier); break;
    case JAZZ_PFE_COMPLETE_TX_PAYLOAD_COVERAGE: jz_bytes_drop(&e->u.complete_tx.payload_digest); break;
    case JAZZ_PFE_VIEW_COMPLETE_EXCLUSIVE_COVERAGE:
        jz_str_drop(&e->u.view_complete.scope); opt_member_drop(&e->u.view_complete.result);
        jz_bytes_drop(&e->u.view_complete.covered_members_digest); break;
    case JAZZ_PFE_POLICY_DECISION:
        jz_bytes_drop(&e->u.policy_decision.decision); jz_str_drop(&e->u.policy_decision.outcome.s);
        jz_bytes_drop(&e->u.policy_decision.outcome.frontier); os_drop(&e->u.policy_decision.reason); break;
    case JAZZ_PFE_VERSION_WITNESS:
        jz_str_drop(&e->u.version_witness.role); jazz_protocol__protocol__RowVersionRefEntry__drop(&e->u.version_witness.version);
        opt_member_drop(&e->u.version_witness.member); break;
    case JAZZ_PFE_COVERED_INPUT:
        ProgramSourceId_drop(&e->u.covered_input.source); jazz_protocol__protocol__RowVersionRefEntry__drop(&e->u.covered_input.version); break;
    case JAZZ_PFE_POLICY_WITNESS:
        jazz_protocol__protocol__ResultMemberEntry__drop(&e->u.policy_witness.protected_); jz_str_drop(&e->u.policy_witness.policy_path);
        jazz_protocol__protocol__RowVersionRefEntry__drop(&e->u.policy_witness.witness); break;
    case JAZZ_PFE_CONTRIBUTING_MEMBERS:
        jazz_protocol__protocol__ResultMemberEntry__drop(&e->u.contributing.result);
        jazz_protocol__protocol__ResultMemberEntry__drop(&e->u.contributing.contributor); os_drop(&e->u.contributing.role); break;
    case JAZZ_PFE_PREDICATE_READ: jz_bytes_drop(&e->u.predicate_read.predicate); jz_bytes_drop(&e->u.predicate_read.frontier); break;
    case JAZZ_PFE_PREDICATE_OUTPUT_SET: jazz_protocol__protocol__RowVersionRefEntry__drop(&e->u.predicate_output.version); break;
    case JAZZ_PFE_POINT_READ: if (e->u.point_read.has_version) jazz_protocol__protocol__RowVersionRefEntry__drop(&e->u.point_read.version); break;
    }
}
int jazz_protocol__protocol__ProgramFactEntry__Ord__cmp(const jazz_protocol_ProgramFactEntry *a, const jazz_protocol_ProgramFactEntry *b) {
    if (a->tag != b->tag) return a->tag < b->tag ? -1 : 1;
    int c;
    switch (a->tag) {
    case JAZZ_PFE_RESULT_PAYLOAD: {
        const jazz_protocol_ResultMemberPayloadEntry *x = &a->u.result_payload, *y = &b->u.result_payload;
        if ((c = jazz_protocol__protocol__ResultMemberEntry__Ord__cmp(&x->member, &y->member))) return c;
        if ((c = jz_bytes_cmp(&x->descriptor, &y->descriptor))) return c;
        return jz_bytes_cmp(&x->record, &y->record);
    }
    case JAZZ_PFE_RELATION_EDGE: return RelationEdgeEntry_cmp(&a->u.relation_edge, &b->u.relation_edge);
    case JAZZ_PFE_PATH_CORRELATION_COVERAGE: {
        const jazz_protocol_PathCorrelationCoverageEntry *x = &a->u.path_correlation, *y = &b->u.path_correlation;
        if ((c = jz_str_cmp(&x->path, &y->path))) return c;
        if ((c = istr_cmp(x->source_table, y->source_table))) return c;
        if ((c = jz_uuid_cmp(&x->source_row, &y->source_row))) return c;
        if ((c = jz_bytes_cmp(&x->correlation_key, &y->correlation_key))) return c;
        return bool_cmp(x->complete, y->complete);
    }
    case JAZZ_PFE_PROGRAM_SOURCE_COVERAGE:
        if ((c = ProgramSourceId_cmp(&a->u.program_source.source, &b->u.program_source.source))) return c;
        return bool_cmp(a->u.program_source.complete, b->u.program_source.complete);
    case JAZZ_PFE_READ_FRONTIER_SETTLED: {
        const jazz_protocol_ReadFrontierSettledEntry *x = &a->u.read_frontier, *y = &b->u.read_frontier;
        if ((c = jz_str_cmp(&x->scope, &y->scope))) return c;
        if (x->tier != y->tier) return u64_cmp(x->tier, y->tier);
        if ((c = os_cmp(&x->stream, &y->stream))) return c;
        return jz_bytes_cmp(&x->frontier, &y->frontier);
    }
    case JAZZ_PFE_COMPLETE_TX_PAYLOAD_COVERAGE: {
        const jazz_protocol_CompleteTxPayloadCoverageEntry *x = &a->u.complete_tx, *y = &b->u.complete_tx;
        if ((c = jazz_model__tx__TxId__cmp(&x->tx, &y->tx, NULL))) return c;
        if (x->tier != y->tier) return u64_cmp(x->tier, y->tier);
        return jz_bytes_cmp(&x->payload_digest, &y->payload_digest);
    }
    case JAZZ_PFE_VIEW_COMPLETE_EXCLUSIVE_COVERAGE: {
        const jazz_protocol_ViewCompleteExclusiveCoverageEntry *x = &a->u.view_complete, *y = &b->u.view_complete;
        if ((c = jazz_model__tx__TxId__cmp(&x->tx, &y->tx, NULL))) return c;
        if ((c = jz_str_cmp(&x->scope, &y->scope))) return c;
        if ((c = opt_member_cmp(&x->result, &y->result))) return c;
        if (x->tier != y->tier) return u64_cmp(x->tier, y->tier);
        return jz_bytes_cmp(&x->covered_members_digest, &y->covered_members_digest);
    }
    case JAZZ_PFE_POLICY_DECISION: {
        const jazz_protocol_PolicyDecisionEntry *x = &a->u.policy_decision, *y = &b->u.policy_decision;
        if ((c = jz_bytes_cmp(&x->decision, &y->decision))) return c;
        if (x->outcome.tag != y->outcome.tag) return u64_cmp(x->outcome.tag, y->outcome.tag);
        if (x->outcome.tag >= JAZZ_PDO_INDETERMINATE_REQUIRES_INPUT && (c = jz_str_cmp(&x->outcome.s, &y->outcome.s))) return c;
        if (x->outcome.tag == JAZZ_PDO_REQUIRES_COVERAGE && (c = jz_bytes_cmp(&x->outcome.frontier, &y->outcome.frontier))) return c;
        return os_cmp(&x->reason, &y->reason);
    }
    case JAZZ_PFE_VERSION_WITNESS: {
        const jazz_protocol_VersionWitnessEntry *x = &a->u.version_witness, *y = &b->u.version_witness;
        if ((c = jz_str_cmp(&x->role, &y->role))) return c;
        if ((c = jazz_protocol__protocol__RowVersionRefEntry__Ord__cmp(&x->version, &y->version))) return c;
        return opt_member_cmp(&x->member, &y->member);
    }
    case JAZZ_PFE_COVERED_INPUT: {
        const jazz_protocol_CoveredInputEntry *x = &a->u.covered_input, *y = &b->u.covered_input;
        if ((c = ProgramSourceId_cmp(&x->source, &y->source))) return c;
        if ((c = istr_cmp(x->version_table, y->version_table))) return c;
        if ((c = jz_uuid_cmp(&x->source_row, &y->source_row))) return c;
        return jazz_protocol__protocol__RowVersionRefEntry__Ord__cmp(&x->version, &y->version);
    }
    case JAZZ_PFE_POLICY_WITNESS: {
        const jazz_protocol_PolicyWitnessEntry *x = &a->u.policy_witness, *y = &b->u.policy_witness;
        if ((c = jazz_protocol__protocol__ResultMemberEntry__Ord__cmp(&x->protected_, &y->protected_))) return c;
        if ((c = jz_str_cmp(&x->policy_path, &y->policy_path))) return c;
        if ((c = jazz_protocol__protocol__RowVersionRefEntry__Ord__cmp(&x->witness, &y->witness))) return c;
        return oe_cmp(&x->edge_kind, &y->edge_kind);
    }
    case JAZZ_PFE_CONTRIBUTING_MEMBERS: {
        const jazz_protocol_ContributingMembersEntry *x = &a->u.contributing, *y = &b->u.contributing;
        if ((c = jazz_protocol__protocol__ResultMemberEntry__Ord__cmp(&x->result, &y->result))) return c;
        if ((c = jazz_protocol__protocol__ResultMemberEntry__Ord__cmp(&x->contributor, &y->contributor))) return c;
        if ((c = otx_cmp(&x->batch, &y->batch))) return c;
        return os_cmp(&x->role, &y->role);
    }
    case JAZZ_PFE_PREDICATE_READ: {
        const jazz_protocol_PredicateReadEntry *x = &a->u.predicate_read, *y = &b->u.predicate_read;
        if (x->role != y->role) return u64_cmp(x->role, y->role);
        if ((c = jz_uuid_cmp(&x->shape_id.u, &y->shape_id.u))) return c;
        if ((c = jz_uuid_cmp(&x->binding_id.u, &y->binding_id.u))) return c;
        if ((c = jz_bytes_cmp(&x->predicate, &y->predicate))) return c;
        return jz_bytes_cmp(&x->frontier, &y->frontier);
    }
    case JAZZ_PFE_PREDICATE_OUTPUT_SET: {
        const jazz_protocol_PredicateOutputSetEntry *x = &a->u.predicate_output, *y = &b->u.predicate_output;
        if (x->role != y->role) return u64_cmp(x->role, y->role);
        if ((c = istr_cmp(x->table, y->table))) return c;
        if ((c = jz_uuid_cmp(&x->row, &y->row))) return c;
        if ((c = jazz_protocol__protocol__RowVersionRefEntry__Ord__cmp(&x->version, &y->version))) return c;
        if ((c = jz_uuid_cmp(&x->shape_id.u, &y->shape_id.u))) return c;
        return jz_uuid_cmp(&x->binding_id.u, &y->binding_id.u);
    }
    default: {
        const jazz_protocol_PointReadEntry *x = &a->u.point_read, *y = &b->u.point_read;
        if ((c = bool_cmp(x->present, y->present))) return c;
        if ((c = istr_cmp(x->table, y->table))) return c;
        if ((c = jz_uuid_cmp(&x->row, &y->row))) return c;
        if ((c = opt_rvre_cmp(x->has_version, &x->version, y->has_version, &y->version))) return c;
        if ((c = jz_uuid_cmp(&x->shape_id.u, &y->shape_id.u))) return c;
        return jz_uuid_cmp(&x->binding_id.u, &y->binding_id.u);
    }
    }
}
bool jazz_protocol__protocol__ProgramFactEntry__is_peer_source_closure_fact(const jazz_protocol_ProgramFactEntry *e) {
    return e->tag == JAZZ_PFE_PROGRAM_SOURCE_COVERAGE || e->tag == JAZZ_PFE_COVERED_INPUT;
}

/* =====================================================================
 * Part 5: schema versions, physical identity manifests, lineage, lenses
 * ===================================================================== */
#define CATALOGUE_SCHEMA_VERSION 2u

void jazz_protocol__protocol__SchemaVersion__new(jazz_protocol_SchemaVersion *out, jazz_model_JazzSchema schema) {
    out->id = jazz_model__schema__JazzSchema__version_id(&schema);
    out->schema = schema;
}
void jazz_protocol__protocol__SchemaVersion__drop(jazz_protocol_SchemaVersion *s) { jazz_model__schema__JazzSchema__drop(&s->schema); }
static void SchemaVersion_clone(jazz_protocol_SchemaVersion *d, const jazz_protocol_SchemaVersion *s) {
    d->id = s->id;
    jazz_model__schema__JazzSchema__Clone__clone(&d->schema, &s->schema);
}
static bool SchemaVersion_eq(const jazz_protocol_SchemaVersion *a, const jazz_protocol_SchemaVersion *b) {
    return jz_uuid_eq(&a->id, &b->id) && jazz_model__schema__JazzSchema__PartialEq__eq(&a->schema, &b->schema);
}
jz_err jazz_protocol__protocol__durable_public_schema_json(const jazz_model_JazzSchema *schema, jz_bytes *out) {
    jz_str json = {0};
    jz_err e = jazz_model__model__public_schema__PublicSchema__Serialize__to_json(schema->public_schema, &json);
    if (e.code) {
        /* map_err(|error| error.to_string()) */
        jz_err m = e.msg ? jz_err_fmt(JAZZ_PROTO_ERR_MSG, "%.*s", (int)e.msg->len, e.msg->ptr) : msg_err("serde_json error");
        jz_err_drop(&e);
        return m;
    }
    out->ptr = (u8 *)json.ptr; out->len = json.len; out->cap = json.cap;
    return JZ_OK;
}
jz_err jazz_protocol__protocol__canonical_catalogue_schema_bytes(const jazz_protocol_SchemaVersion *schema, jz_bytes *out) {
    jz_bytes public_schema;
    JZ_TRY(jazz_protocol__protocol__durable_public_schema_json(&schema->schema, &public_schema));
    if (public_schema.len > 0xffffffffu) { jz_bytes_drop(&public_schema); return msg_err("catalogue public schema payload too large"); }
    jz_bytes payload = {0};
    jz_vec_reserve(&payload, 1 + 16 + 4 + public_schema.len, 1);
    jz_bytes_push_u8(&payload, CATALOGUE_SCHEMA_VERSION);
    jz_bytes_push(&payload, schema->id.b, 16);
    push_le32(&payload, public_schema.len);
    jz_bytes_push(&payload, public_schema.ptr, public_schema.len);
    jz_bytes_drop(&public_schema);
    *out = payload;
    return JZ_OK;
}
/* Serialize via SchemaVersionWire { id, public_schema_json } */
jz_err jazz_protocol__protocol__SchemaVersion__Serialize__serialize(jz_pc_w *w, const jazz_protocol_SchemaVersion *s) {
    jz_bytes json;
    jz_err e = jazz_protocol__protocol__durable_public_schema_json(&s->schema, &json);
    if (e.code) return serde_custom(e);
    jz_pc_uuid(w, &s->id);
    jz_pc_bytes(w, json.ptr, json.len);
    jz_bytes_drop(&json);
    return JZ_OK;
}
jz_err jazz_protocol__protocol__SchemaVersion__Deserialize__deserialize(jz_pc_r *r, jazz_protocol_SchemaVersion *out) {
    jz_uuid id;
    JZ_TRY(jz_pc_r_uuid(r, &id));
    jz_bytes json;
    JZ_TRY(jz_pc_r_bytes(r, &json));
    jz_err e = jazz_model__model__public_schema_convert__decode_public_schema_json(jz_bytes_slice(&json), &out->schema);
    jz_bytes_drop(&json);
    if (e.code) return serde_custom(e);
    /* Identity validation stays at catalogue admission/open. */
    out->id = id;
    return JZ_OK;
}

/* ---------------- PhysicalIdentityManifest ---------------- */
static void PhysicalColumnIdentity_drop_v(void *p) { jz_btree_drop(&((jazz_protocol_PhysicalColumnIdentity *)p)->enum_variants); }
static void PhysicalTableIdentity_drop_v(void *p) { jz_btree_drop(&((jazz_protocol_PhysicalTableIdentity *)p)->columns); }
static const jz_btree_desc DESC_COLUMN_IDENTITIES = { sizeof(jz_str), sizeof(jazz_protocol_PhysicalColumnIdentity), jz_cmp_str, str_drop_v, PhysicalColumnIdentity_drop_v };
static const jz_btree_desc DESC_TABLE_IDENTITIES = { sizeof(jz_str), sizeof(jazz_protocol_PhysicalTableIdentity), jz_cmp_str, str_drop_v, PhysicalTableIdentity_drop_v };
static void PhysicalColumnIdentity_clone_v(void *d_, const void *s_) {
    jazz_protocol_PhysicalColumnIdentity *d = d_; const jazz_protocol_PhysicalColumnIdentity *s = s_;
    d->id = s->id;
    jz_btree_clone(&d->enum_variants, &s->enum_variants, str_clone_v, uuid_vec_clone_v);
}
static void PhysicalTableIdentity_clone_v(void *d_, const void *s_) {
    jazz_protocol_PhysicalTableIdentity *d = d_; const jazz_protocol_PhysicalTableIdentity *s = s_;
    d->id = s->id;
    jz_btree_clone(&d->columns, &s->columns, str_clone_v, PhysicalColumnIdentity_clone_v);
}
void jazz_protocol__protocol__PhysicalIdentityManifest__drop(jazz_protocol_PhysicalIdentityManifest *m) { jz_btree_drop(&m->tables); }
void jazz_protocol__protocol__PhysicalIdentityManifest__Clone__clone(jazz_protocol_PhysicalIdentityManifest *d, const jazz_protocol_PhysicalIdentityManifest *s) {
    jz_btree_clone(&d->tables, &s->tables, str_clone_v, PhysicalTableIdentity_clone_v);
}
static bool uuid_vec_eq_v(const void *a_, const void *b_) {
    const jazz_protocol_UuidVec *a = a_, *b = b_;
    return a->len == b->len && memcmp(a->ptr, b->ptr, (usize)a->len * 16) == 0;
}
static bool column_identity_eq_v(const void *a_, const void *b_) {
    const jazz_protocol_PhysicalColumnIdentity *a = a_, *b = b_;
    return jz_uuid_eq(&a->id, &b->id) && jz_btree_eq(&a->enum_variants, &b->enum_variants, uuid_vec_eq_v);
}
static bool table_identity_eq_v(const void *a_, const void *b_) {
    const jazz_protocol_PhysicalTableIdentity *a = a_, *b = b_;
    return jz_uuid_eq(&a->id, &b->id) && jz_btree_eq(&a->columns, &b->columns, column_identity_eq_v);
}
bool jazz_protocol__protocol__PhysicalIdentityManifest__PartialEq__eq(const jazz_protocol_PhysicalIdentityManifest *a, const jazz_protocol_PhysicalIdentityManifest *b) {
    return jz_btree_eq(&a->tables, &b->tables, table_identity_eq_v);
}
static jz_bytes PhysicalIdentityManifest_canonical_bytes(const jazz_protocol_PhysicalIdentityManifest *m) {
    jz_bytes bytes = {0};
    put_len(&bytes, m->tables.len);
    jz_str *tname; jazz_protocol_PhysicalTableIdentity *table;
    JZ_BTREE_FOR(&m->tables, tname, table) {
        put_str(&bytes, jz_str_slice(tname));
        jz_bytes_push(&bytes, table->id.b, 16);
        put_len(&bytes, table->columns.len);
        jz_str *cname; jazz_protocol_PhysicalColumnIdentity *column;
        JZ_BTREE_FOR(&table->columns, cname, column) {
            put_str(&bytes, jz_str_slice(cname));
            jz_bytes_push(&bytes, column->id.b, 16);
            put_len(&bytes, column->enum_variants.len);
            jz_str *path; jazz_protocol_UuidVec *variants;
            JZ_BTREE_FOR(&column->enum_variants, path, variants) {
                put_str(&bytes, jz_str_slice(path));
                put_len(&bytes, variants->len);
                for (u32 i = 0; i < variants->len; i++) jz_bytes_push(&bytes, variants->ptr[i].b, 16);
            }
        }
    }
    return bytes;
}

static void collect_enum_variant_ids_record(groove_RecordDescriptor record, jz_slice path, jz_btree *output);
static void jazz_protocol__protocol__collect_enum_variant_ids(const groove_ValueType *vt, jz_slice path, jz_btree *output) {
    switch (vt->tag) {
    case GROOVE_VT_ENUM_TAG: {
        jazz_protocol_UuidVec ids = {0};
        for (u32 i = 0; i < vt->u.enum_tag->variants.len; i++) JZ_VEC_PUSH(ids, jz_uuid_v4());
        jz_str key = jz_str_from_slice(path);
        jazz_protocol_UuidVec old;
        if (jz_btree_insert(output, &key, &ids, &old)) jz_vec_drop(&old, sizeof(jz_uuid), NULL);
        break;
    }
    case GROOVE_VT_ENUM: {
        const groove_EnumSchema *schema = vt->u.enum_schema;
        jazz_protocol_UuidVec ids = {0};
        for (u32 i = 0; i < schema->cases.len; i++) JZ_VEC_PUSH(ids, jz_uuid_v4());
        jz_str key = jz_str_from_slice(path);
        jazz_protocol_UuidVec old;
        if (jz_btree_insert(output, &key, &ids, &old)) jz_vec_drop(&old, sizeof(jz_uuid), NULL);
        for (u32 ordinal = 0; ordinal < schema->cases.len; ordinal++) {
            jz_str sub = jz_format("%.*s/case/%u", (int)path.len, path.ptr, ordinal);
            collect_enum_variant_ids_record(schema->cases.ptr[ordinal].payload, jz_str_slice(&sub), output);
            jz_str_drop(&sub);
        }
        break;
    }
    case GROOVE_VT_NULLABLE: case GROOVE_VT_ARRAY: {
        jz_str sub = jz_format("%.*s/%s", (int)path.len, path.ptr, vt->tag == GROOVE_VT_NULLABLE ? "nullable" : "array");
        jazz_protocol__protocol__collect_enum_variant_ids(vt->u.inner, jz_str_slice(&sub), output);
        jz_str_drop(&sub);
        break;
    }
    case GROOVE_VT_TUPLE:
        for (u32 index = 0; index < vt->u.tuple.len; index++) {
            jz_str sub = jz_format("%.*s/tuple/%u", (int)path.len, path.ptr, index);
            jazz_protocol__protocol__collect_enum_variant_ids(&vt->u.tuple.ptr[index], jz_str_slice(&sub), output);
            jz_str_drop(&sub);
        }
        break;
    case GROOVE_VT_RECORD: collect_enum_variant_ids_record(*vt->u.record, path, output); break;
    default: break;
    }
}
static void collect_enum_variant_ids_record(groove_RecordDescriptor record, jz_slice path, jz_btree *output) {
    u32 n;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(record, &n);
    for (u32 i = 0; i < n; i++) {
        if (!fields[i].has_name) continue;
        jz_str sub = jz_format("%.*s/record/%.*s", (int)path.len, path.ptr, (int)fields[i].name.len, fields[i].name.ptr);
        jazz_protocol__protocol__collect_enum_variant_ids(&fields[i].value_type, jz_str_slice(&sub), output);
        jz_str_drop(&sub);
    }
}
static void collect_enum_variant_counts_record(groove_RecordDescriptor record, jz_slice path, jz_btree *output);
static void jazz_protocol__protocol__collect_enum_variant_counts(const groove_ValueType *vt, jz_slice path, jz_btree *output) {
    switch (vt->tag) {
    case GROOVE_VT_ENUM_TAG: {
        jz_str key = jz_str_from_slice(path);
        u32 count = vt->u.enum_tag->variants.len;
        if (jz_btree_insert(output, &key, &count, NULL)) {}
        break;
    }
    case GROOVE_VT_ENUM: {
        const groove_EnumSchema *schema = vt->u.enum_schema;
        jz_str key = jz_str_from_slice(path);
        u32 count = schema->cases.len;
        jz_btree_insert(output, &key, &count, NULL);
        for (u32 ordinal = 0; ordinal < schema->cases.len; ordinal++) {
            jz_str sub = jz_format("%.*s/case/%u", (int)path.len, path.ptr, ordinal);
            collect_enum_variant_counts_record(schema->cases.ptr[ordinal].payload, jz_str_slice(&sub), output);
            jz_str_drop(&sub);
        }
        break;
    }
    case GROOVE_VT_NULLABLE: case GROOVE_VT_ARRAY: {
        jz_str sub = jz_format("%.*s/%s", (int)path.len, path.ptr, vt->tag == GROOVE_VT_NULLABLE ? "nullable" : "array");
        jazz_protocol__protocol__collect_enum_variant_counts(vt->u.inner, jz_str_slice(&sub), output);
        jz_str_drop(&sub);
        break;
    }
    case GROOVE_VT_TUPLE:
        for (u32 index = 0; index < vt->u.tuple.len; index++) {
            jz_str sub = jz_format("%.*s/tuple/%u", (int)path.len, path.ptr, index);
            jazz_protocol__protocol__collect_enum_variant_counts(&vt->u.tuple.ptr[index], jz_str_slice(&sub), output);
            jz_str_drop(&sub);
        }
        break;
    case GROOVE_VT_RECORD: collect_enum_variant_counts_record(*vt->u.record, path, output); break;
    default: break;
    }
}
static void collect_enum_variant_counts_record(groove_RecordDescriptor record, jz_slice path, jz_btree *output) {
    u32 n;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(record, &n);
    for (u32 i = 0; i < n; i++) {
        if (!fields[i].has_name) continue;
        jz_str sub = jz_format("%.*s/record/%.*s", (int)path.len, path.ptr, (int)fields[i].name.len, fields[i].name.ptr);
        jazz_protocol__protocol__collect_enum_variant_counts(&fields[i].value_type, jz_str_slice(&sub), output);
        jz_str_drop(&sub);
    }
}

void jazz_protocol__protocol__PhysicalIdentityManifest__allocate(jazz_protocol_PhysicalIdentityManifest *out, const jazz_model_JazzSchema *schema) {
    jz_btree_init(&out->tables, &DESC_TABLE_IDENTITIES);
    for (u32 t = 0; t < schema->tables.len; t++) {
        const jazz_model_TableSchema *table = &schema->tables.ptr[t];
        jazz_protocol_PhysicalTableIdentity ti;
        jz_btree_init(&ti.columns, &DESC_COLUMN_IDENTITIES);
        for (u32 c = 0; c < table->columns.len; c++) {
            const jazz_model_ColumnSchema *column = &table->columns.ptr[c];
            jazz_protocol_PhysicalColumnIdentity ci;
            jz_btree_init(&ci.enum_variants, &DESC_STR_UUIDVEC);
            jazz_protocol__protocol__collect_enum_variant_ids(&column->column_type, JZ_LIT("root"), &ci.enum_variants);
            ci.id = jz_uuid_v4();
            jz_str name = jz_str_clone(&column->name);
            jazz_protocol_PhysicalColumnIdentity old;
            if (jz_btree_insert(&ti.columns, &name, &ci, &old)) PhysicalColumnIdentity_drop_v(&old);
        }
        ti.id = jz_uuid_v4();
        jz_str name = jz_str_clone(&table->name);
        jazz_protocol_PhysicalTableIdentity old;
        if (jz_btree_insert(&out->tables, &name, &ti, &old)) PhysicalTableIdentity_drop_v(&old);
    }
}

static bool uuid_set_insert(jz_btree *set, const jz_uuid *u) { return !jz_btree_insert(set, u, NULL, NULL); }
jz_err jazz_protocol__protocol__PhysicalIdentityManifest__validate_for_schema(const jazz_protocol_PhysicalIdentityManifest *m, const jazz_model_JazzSchema *schema) {
    if (m->tables.len != schema->tables.len) return msg_err("physical identity manifest table coverage mismatch");
    jz_btree seen;
    jz_btree_init(&seen, &DESC_UUID_SET);
    jz_err err = JZ_OK;
    jz_uuid nil = {{0}};
    for (u32 t = 0; t < schema->tables.len && !err.code; t++) {
        const jazz_model_TableSchema *table = &schema->tables.ptr[t];
        const jazz_protocol_PhysicalTableIdentity *ti = jz_btree_get(&m->tables, &table->name);
        if (!ti) { err = msg_err("physical identity manifest table missing"); break; }
        if (jz_uuid_eq(&ti->id, &nil) || !uuid_set_insert(&seen, &ti->id)) { err = msg_err("physical identity manifest UUID collision"); break; }
        if (ti->columns.len != table->columns.len) { err = msg_err("physical identity manifest column coverage mismatch"); break; }
        for (u32 c = 0; c < table->columns.len && !err.code; c++) {
            const jazz_model_ColumnSchema *column = &table->columns.ptr[c];
            const jazz_protocol_PhysicalColumnIdentity *ci = jz_btree_get(&ti->columns, &column->name);
            if (!ci) { err = msg_err("physical identity manifest column missing"); break; }
            if (jz_uuid_eq(&ci->id, &nil) || !uuid_set_insert(&seen, &ci->id)) { err = msg_err("physical identity manifest UUID collision"); break; }
            jz_btree occurrences;
            jz_btree_init(&occurrences, &DESC_STR_USIZE);
            jazz_protocol__protocol__collect_enum_variant_counts(&column->column_type, JZ_LIT("root"), &occurrences);
            if (ci->enum_variants.len != occurrences.len) err = msg_err("physical identity manifest enum coverage mismatch");
            jz_str *path; u32 *count;
            if (!err.code) JZ_BTREE_FOR(&occurrences, path, count) {
                const jazz_protocol_UuidVec *variants = jz_btree_get(&ci->enum_variants, path);
                if (!variants) { err = msg_err("physical identity manifest enum occurrence missing"); break; }
                if (variants->len != *count) { err = msg_err("physical identity manifest enum case coverage mismatch"); break; }
                for (u32 v = 0; v < variants->len; v++)
                    if (jz_uuid_eq(&variants->ptr[v], &nil) || !uuid_set_insert(&seen, &variants->ptr[v])) {
                        err = msg_err("physical identity manifest UUID collision");
                        break;
                    }
                if (err.code) break;
            }
            jz_btree_drop(&occurrences);
        }
    }
    jz_btree_drop(&seen);
    return err;
}
static void PhysicalIdentityManifest_all_identity_uuids(const jazz_protocol_PhysicalIdentityManifest *m, jz_btree *uuids) {
    jz_str *tn; jazz_protocol_PhysicalTableIdentity *table;
    JZ_BTREE_FOR(&m->tables, tn, table) {
        jz_btree_insert(uuids, &table->id, NULL, NULL);
        jz_str *cn; jazz_protocol_PhysicalColumnIdentity *column;
        JZ_BTREE_FOR(&table->columns, cn, column) {
            jz_btree_insert(uuids, &column->id, NULL, NULL);
            jz_str *path; jazz_protocol_UuidVec *variants;
            JZ_BTREE_FOR(&column->enum_variants, path, variants)
                for (u32 i = 0; i < variants->len; i++) jz_btree_insert(uuids, &variants->ptr[i], NULL, NULL);
        }
    }
}
static const jazz_model_TableSchema *find_table(const jazz_model_JazzSchema *s, const jz_str *name) {
    for (u32 i = 0; i < s->tables.len; i++) if (jz_str_eq(&s->tables.ptr[i].name, name)) return &s->tables.ptr[i];
    return NULL;
}
/* target_name_by_source: BTreeMap<String, Option<String>> */
static void inherited_uuids_fail(jz_btree *map, jz_err *err, const char *m) { (void)map; *err = msg_err(m); }
static jz_err jazz_protocol__protocol__PhysicalIdentityManifest__inherited_uuids(const jazz_protocol_PhysicalIdentityManifest *self,
    const jazz_model_JazzSchema *source, const jazz_protocol_PhysicalIdentityManifest *target_manifest,
    const jazz_model_JazzSchema *target, const jazz_protocol_MigrationLens *lens, jz_btree *inherited) {
    jz_err err = JZ_OK;
    for (u32 l = 0; l < lens->table_lenses.len && !err.code; l++) {
        const jazz_protocol_TableLens *tl = &lens->table_lenses.ptr[l];
        const jazz_protocol_PhysicalTableIdentity *st = jz_btree_get(&self->tables, &tl->source_table);
        if (!st) return msg_err("physical identity source table missing");
        const jazz_protocol_PhysicalTableIdentity *tt = jz_btree_get(&target_manifest->tables, &tl->target_table);
        if (!tt) return msg_err("physical identity target table missing");
        if (!jz_uuid_eq(&st->id, &tt->id)) return msg_err("physical table identity changed across lineage");
        jz_btree_insert(inherited, &st->id, NULL, NULL);
        const jazz_model_TableSchema *sts = find_table(source, &tl->source_table);
        if (!sts) return msg_err("physical identity source table schema missing");
        const jazz_model_TableSchema *tts = find_table(target, &tl->target_table);
        if (!tts) return msg_err("physical identity target table schema missing");
        jz_btree names;
        jz_btree_init(&names, &DESC_STR_OPTSTR);
        jz_str *cn; jazz_protocol_PhysicalColumnIdentity *ci;
        JZ_BTREE_FOR(&st->columns, cn, ci) {
            jz_str k = jz_str_clone(cn);
            jazz_protocol_OptStr v = { true, jz_str_clone(cn) };
            jz_btree_insert(&names, &k, &v, NULL);
        }
        for (u32 o = 0; o < tl->ops.len; o++) {
            const jazz_protocol_LensOp *op = &tl->ops.ptr[o];
            jz_str *k; jazz_protocol_OptStr *tn;
            switch (op->tag) {
            case JAZZ_LENS_RENAME_COLUMN:
                JZ_BTREE_FOR(&names, k, tn)
                    if (tn->some && jz_str_eq(&tn->v, &op->a)) { jz_str_drop(&tn->v); tn->v = jz_str_clone(&op->b); break; }
                break;
            case JAZZ_LENS_COPY_COLUMN: case JAZZ_LENS_ADD_COLUMN: {
                const jz_str *to = op->tag == JAZZ_LENS_COPY_COLUMN ? &op->b : &op->a;
                JZ_BTREE_FOR(&names, k, tn)
                    if (tn->some && jz_str_eq(&tn->v, to)) { jz_str_drop(&tn->v); tn->some = false; }
                break;
            }
            case JAZZ_LENS_DROP_COLUMN:
                JZ_BTREE_FOR(&names, k, tn)
                    if (tn->some && jz_str_eq(&tn->v, &op->a)) { jz_str_drop(&tn->v); tn->some = false; }
                break;
            default: break; /* RenameTable / TransformColumn / RejectSourceDelta */
            }
        }
        jz_str *source_name; jazz_protocol_OptStr *target_name;
        JZ_BTREE_FOR(&names, source_name, target_name) {
            if (!target_name->some) continue;
            const jazz_protocol_PhysicalColumnIdentity *sc = jz_btree_get(&st->columns, source_name);
            if (!sc) jz_panic("index out of bounds");
            const jazz_protocol_PhysicalColumnIdentity *tc = jz_btree_get(&tt->columns, &target_name->v);
            if (!tc) { inherited_uuids_fail(&names, &err, "physical identity inherited column missing"); break; }
            bool compatible = jazz_model__schema__physical_column_epoch_is_compatible(sts, jz_str_slice(source_name), tts, jz_str_slice(&target_name->v));
            if (compatible && !jz_uuid_eq(&sc->id, &tc->id)) { err = msg_err("physical compatible column identity changed across lineage"); break; }
            if (!compatible && jz_uuid_eq(&sc->id, &tc->id)) { err = msg_err("physical incompatible column epoch reused identity"); break; }
            if (!compatible) continue; /* a changed epoch is an entirely new physical subtree */
            jz_btree_insert(inherited, &sc->id, NULL, NULL);
            jz_str *path; jazz_protocol_UuidVec *sv;
            JZ_BTREE_FOR(&sc->enum_variants, path, sv) {
                const jazz_protocol_UuidVec *tv = jz_btree_get(&tc->enum_variants, path);
                if (!tv) continue;
                u32 shared = sv->len < tv->len ? sv->len : tv->len;
                if (memcmp(sv->ptr, tv->ptr, (usize)shared * 16) != 0) { err = msg_err("physical enum identity changed across lineage"); break; }
                for (u32 i = 0; i < shared; i++) jz_btree_insert(inherited, &sv->ptr[i], NULL, NULL);
            }
            if (err.code) break;
        }
        /* names: values are OptStr; drop owned strings */
        JZ_BTREE_FOR(&names, source_name, target_name) os_drop(target_name);
        jz_btree_drop(&names);
    }
    return err;
}

/* ---------------- SchemaLineagePublication ---------------- */
static void StrVec_clone(jazz_protocol_StrVec *d, const jazz_protocol_StrVec *s) { jz_vec_clone(d, s, sizeof(jz_str), str_clone_v); }
static void StrVec_drop(jazz_protocol_StrVec *v) { jz_vec_drop(v, sizeof(jz_str), str_drop_v); }
static int cmp_str_ptr(const void *a, const void *b, void *ctx) { (void)ctx; return jz_str_cmp(*(const jz_str *const *)a, *(const jz_str *const *)b); }
static int cmp_pred_source(const void *a, const void *b, void *ctx) {
    (void)ctx;
    return jz_uuid_cmp(&(*(const jazz_protocol_SchemaPredecessor *const *)a)->lens.source, &(*(const jazz_protocol_SchemaPredecessor *const *)b)->lens.source);
}
static void SchemaPredecessor_clone_v(void *d_, const void *s_) {
    jazz_protocol_SchemaPredecessor *d = d_; const jazz_protocol_SchemaPredecessor *s = s_;
    jazz_protocol__protocol__MigrationLens__Clone__clone(&d->lens, &s->lens);
    StrVec_clone(&d->new_tables, &s->new_tables);
    StrVec_clone(&d->dropped_tables, &s->dropped_tables);
}
static void SchemaPredecessor_drop_v(void *p) {
    jazz_protocol_SchemaPredecessor *s = p;
    jazz_protocol__protocol__MigrationLens__drop(&s->lens);
    StrVec_drop(&s->new_tables);
    StrVec_drop(&s->dropped_tables);
}
/* fn sorted(values: &[String]) -> Vec<&String> */
static const jz_str **jazz_protocol__protocol__SchemaPredecessor__eq__sorted(const jazz_protocol_StrVec *values) {
    const jz_str **v = jz_alloc((usize)values->len * sizeof(void *) + 1);
    for (u32 i = 0; i < values->len; i++) v[i] = &values->ptr[i];
    jz_sort((void *)v, values->len, sizeof(void *), cmp_str_ptr, NULL);
    return v;
}
static bool sorted_eq(const jazz_protocol_StrVec *a, const jazz_protocol_StrVec *b) {
    if (a->len != b->len) return false;
    const jz_str **x = jazz_protocol__protocol__SchemaPredecessor__eq__sorted(a), **y = jazz_protocol__protocol__SchemaPredecessor__eq__sorted(b);
    bool eq = true;
    for (u32 i = 0; i < a->len && eq; i++) eq = jz_str_eq(x[i], y[i]);
    jz_free(x); jz_free(y);
    return eq;
}
bool jazz_protocol__protocol__SchemaPredecessor__PartialEq__eq(const jazz_protocol_SchemaPredecessor *a, const jazz_protocol_SchemaPredecessor *b) {
    return jazz_protocol__protocol__MigrationLens__PartialEq__eq(&a->lens, &b->lens) &&
           sorted_eq(&a->new_tables, &b->new_tables) && sorted_eq(&a->dropped_tables, &b->dropped_tables);
}
void jazz_protocol__protocol__SchemaLineagePublication__Clone__clone(jazz_protocol_SchemaLineagePublication *d, const jazz_protocol_SchemaLineagePublication *s) {
    d->id = s->id;
    SchemaVersion_clone(&d->schema, &s->schema);
    jz_vec_clone(&d->predecessors, &s->predecessors, sizeof(jazz_protocol_SchemaPredecessor), SchemaPredecessor_clone_v);
    jazz_protocol__protocol__PhysicalIdentityManifest__Clone__clone(&d->physical_identities, &s->physical_identities);
}
void jazz_protocol__protocol__SchemaLineagePublication__drop(jazz_protocol_SchemaLineagePublication *p) {
    jazz_protocol__protocol__SchemaVersion__drop(&p->schema);
    jz_vec_drop(&p->predecessors, sizeof(jazz_protocol_SchemaPredecessor), SchemaPredecessor_drop_v);
    jazz_protocol__protocol__PhysicalIdentityManifest__drop(&p->physical_identities);
}
static const jazz_protocol_SchemaPredecessor **ordered_predecessors(const jazz_protocol_SchemaLineagePublication *p) {
    /* clone + sort_by_key(lens.source): stable sort of references (equal-length size profile) */
    const jazz_protocol_SchemaPredecessor **v = jz_alloc((usize)p->predecessors.len * sizeof(void *) + 1);
    for (u32 i = 0; i < p->predecessors.len; i++) v[i] = &p->predecessors.ptr[i];
    jz_sort((void *)v, p->predecessors.len, sizeof(void *), cmp_pred_source, NULL);
    return v;
}
bool jazz_protocol__protocol__SchemaLineagePublication__PartialEq__eq(const jazz_protocol_SchemaLineagePublication *a, const jazz_protocol_SchemaLineagePublication *b) {
    if (!jz_uuid_eq(&a->id, &b->id) || !SchemaVersion_eq(&a->schema, &b->schema) ||
        !jazz_protocol__protocol__PhysicalIdentityManifest__PartialEq__eq(&a->physical_identities, &b->physical_identities)) return false;
    if (a->predecessors.len != b->predecessors.len) return false;
    const jazz_protocol_SchemaPredecessor **x = ordered_predecessors(a), **y = ordered_predecessors(b);
    bool eq = true;
    for (u32 i = 0; i < a->predecessors.len && eq; i++) eq = jazz_protocol__protocol__SchemaPredecessor__PartialEq__eq(x[i], y[i]);
    jz_free(x); jz_free(y);
    return eq;
}
jz_err jazz_protocol__protocol__SchemaLineagePublication__validate_predecessor_identities(const jazz_protocol_SchemaLineagePublication *self,
    const jz_btree *sources, const jazz_protocol_PhysicalIdentityManifest *history, u32 n_history) {
    JZ_TRY(jazz_protocol__protocol__PhysicalIdentityManifest__validate_for_schema(&self->physical_identities, &self->schema.schema));
    jz_btree inherited, reserved;
    jz_btree_init(&inherited, &DESC_UUID_SET);
    jz_btree_init(&reserved, &DESC_UUID_SET);
    jz_err err = JZ_OK;
    for (u32 i = 0; i < self->predecessors.len && !err.code; i++) {
        const jazz_protocol_SchemaPredecessor *pred = &self->predecessors.ptr[i];
        const jazz_protocol_LineageSource *src = jz_btree_get(sources, &pred->lens.source);
        if (!src) { err = msg_err("schema predecessor is missing"); break; }
        err = jazz_protocol__protocol__PhysicalIdentityManifest__validate_for_schema(&src->manifest, &src->schema);
        if (err.code) break;
        err = jazz_protocol__protocol__PhysicalIdentityManifest__inherited_uuids(&src->manifest, &src->schema, &self->physical_identities,
                                                                               &self->schema.schema, &pred->lens, &inherited);
        if (err.code) break;
        PhysicalIdentityManifest_all_identity_uuids(&src->manifest, &reserved);
    }
    if (!err.code) {
        for (u32 i = 0; i < n_history; i++) PhysicalIdentityManifest_all_identity_uuids(&history[i], &reserved);
        jz_btree mine;
        jz_btree_init(&mine, &DESC_UUID_SET);
        PhysicalIdentityManifest_all_identity_uuids(&self->physical_identities, &mine);
        jz_uuid *u; void *unused;
        JZ_BTREE_FOR(&reserved, u, unused)
            if (jz_btree_contains(&mine, u) && !jz_btree_contains(&inherited, u)) { err = msg_err("physical retired identity reused across lineage"); break; }
        jz_btree_drop(&mine);
    }
    jz_btree_drop(&inherited);
    jz_btree_drop(&reserved);
    return err;
}
jazz_types_SchemaLineagePublicationId jazz_protocol__protocol__SchemaLineagePublication__content_id(const jazz_protocol_SchemaLineagePublication *self) {
    jz_bytes bytes = {0};
    put_str(&bytes, JZ_LIT("jazz-schema-lineage-publication-v2"));
    jz_bytes schema_bytes;
    jz_err e = jazz_protocol__protocol__canonical_catalogue_schema_bytes(&self->schema, &schema_bytes);
    if (e.code) jz_panic("schema publication has canonical catalogue schema bytes");
    put_bytes(&bytes, schema_bytes.ptr, schema_bytes.len);
    jz_bytes_drop(&schema_bytes);
    const jazz_protocol_SchemaPredecessor **preds = ordered_predecessors(self);
    put_len(&bytes, self->predecessors.len);
    for (u32 i = 0; i < self->predecessors.len; i++) {
        const jazz_protocol_SchemaPredecessor *p = preds[i];
        jz_bytes lens = jazz_protocol__protocol__canonical_lens_bytes(&p->lens);
        put_bytes(&bytes, lens.ptr, lens.len);
        jz_bytes_drop(&lens);
        const jazz_protocol_StrVec *lists[2] = { &p->new_tables, &p->dropped_tables };
        for (u32 l = 0; l < 2; l++) {
            const jz_str **sorted = jazz_protocol__protocol__SchemaPredecessor__eq__sorted(lists[l]);
            put_len(&bytes, lists[l]->len);
            for (u32 t = 0; t < lists[l]->len; t++) put_str(&bytes, jz_str_slice(sorted[t]));
            jz_free(sorted);
        }
    }
    jz_free(preds);
    jz_bytes manifest = PhysicalIdentityManifest_canonical_bytes(&self->physical_identities);
    put_bytes(&bytes, manifest.ptr, manifest.len);
    jz_bytes_drop(&manifest);
    jz_uuid id = jz_uuid_v5(&SCHEMA_LINEAGE_PUBLICATION_NAMESPACE, bytes.ptr, bytes.len);
    jz_bytes_drop(&bytes);
    return id;
}

/* ---------------- LensOp / TableLens / MigrationLens ---------------- */
static bool lens_op_has_value(u32 tag) { return tag == JAZZ_LENS_ADD_COLUMN || tag == JAZZ_LENS_DROP_COLUMN; }
static bool lens_op_has_b(u32 tag) { return tag != JAZZ_LENS_ADD_COLUMN && tag != JAZZ_LENS_DROP_COLUMN && tag != JAZZ_LENS_REJECT_SOURCE_DELTA; }
static void LensOp_clone_v(void *d_, const void *s_) {
    jazz_protocol_LensOp *d = d_; const jazz_protocol_LensOp *s = s_;
    memset(d, 0, sizeof *d);
    d->tag = s->tag;
    d->a = jz_str_clone(&s->a);
    if (lens_op_has_b(s->tag)) d->b = jz_str_clone(&s->b);
    if (lens_op_has_value(s->tag)) groove__records__values__Value__Clone__clone(&d->value, &s->value);
}
static void LensOp_drop_v(void *p) {
    jazz_protocol_LensOp *o = p;
    jz_str_drop(&o->a);
    if (lens_op_has_b(o->tag)) jz_str_drop(&o->b);
    if (lens_op_has_value(o->tag)) groove__records__values__Value__drop(&o->value);
}
static bool LensOp_eq(const jazz_protocol_LensOp *a, const jazz_protocol_LensOp *b) {
    if (a->tag != b->tag || !jz_str_eq(&a->a, &b->a)) return false;
    if (lens_op_has_b(a->tag) && !jz_str_eq(&a->b, &b->b)) return false;
    return !lens_op_has_value(a->tag) || groove__records__values__Value__PartialEq__eq(&a->value, &b->value);
}
static void TableLens_clone_v(void *d_, const void *s_) {
    jazz_protocol_TableLens *d = d_; const jazz_protocol_TableLens *s = s_;
    d->source_table = jz_str_clone(&s->source_table);
    d->target_table = jz_str_clone(&s->target_table);
    jz_vec_clone(&d->ops, &s->ops, sizeof(jazz_protocol_LensOp), LensOp_clone_v);
}
static void TableLens_drop_v(void *p) {
    jazz_protocol_TableLens *t = p;
    jz_str_drop(&t->source_table); jz_str_drop(&t->target_table);
    jz_vec_drop(&t->ops, sizeof(jazz_protocol_LensOp), LensOp_drop_v);
}
void jazz_protocol__protocol__MigrationLens__Clone__clone(jazz_protocol_MigrationLens *d, const jazz_protocol_MigrationLens *s) {
    d->id = s->id; d->source = s->source; d->target = s->target;
    jz_vec_clone(&d->table_lenses, &s->table_lenses, sizeof(jazz_protocol_TableLens), TableLens_clone_v);
}
void jazz_protocol__protocol__MigrationLens__drop(jazz_protocol_MigrationLens *l) { jz_vec_drop(&l->table_lenses, sizeof(jazz_protocol_TableLens), TableLens_drop_v); }
bool jazz_protocol__protocol__MigrationLens__PartialEq__eq(const jazz_protocol_MigrationLens *a, const jazz_protocol_MigrationLens *b) {
    if (!jz_uuid_eq(&a->id, &b->id) || !jz_uuid_eq(&a->source, &b->source) || !jz_uuid_eq(&a->target, &b->target)) return false;
    if (a->table_lenses.len != b->table_lenses.len) return false;
    for (u32 i = 0; i < a->table_lenses.len; i++) {
        const jazz_protocol_TableLens *x = &a->table_lenses.ptr[i], *y = &b->table_lenses.ptr[i];
        if (!jz_str_eq(&x->source_table, &y->source_table) || !jz_str_eq(&x->target_table, &y->target_table) || x->ops.len != y->ops.len) return false;
        for (u32 j = 0; j < x->ops.len; j++) if (!LensOp_eq(&x->ops.ptr[j], &y->ops.ptr[j])) return false;
    }
    return true;
}
static int cmp_table_lens(const void *a_, const void *b_, void *ctx) {
    (void)ctx;
    const jazz_protocol_TableLens *a = a_, *b = b_;
    int c = jz_str_cmp(&a->source_table, &b->source_table);
    return c ? c : jz_str_cmp(&a->target_table, &b->target_table);
}
static int cmp_table_lens_ptr(const void *a, const void *b, void *ctx) { return cmp_table_lens(*(const void *const *)a, *(const void *const *)b, ctx); }

/* validate_protocol_lens_defaults::valid */
static bool jazz_protocol__protocol__validate_protocol_lens_defaults__valid(const groove_Value *value) {
    switch (value->tag) {
    case GROOVE_V_LARGE: case GROOVE_V_RECORD: case GROOVE_V_ENUM: return false;
    case GROOVE_V_TUPLE: case GROOVE_V_ARRAY:
        for (u32 i = 0; i < value->u.items.len; i++)
            if (!jazz_protocol__protocol__validate_protocol_lens_defaults__valid(&value->u.items.ptr[i])) return false;
        return true;
    case GROOVE_V_NULLABLE: return !value->u.nullable || jazz_protocol__protocol__validate_protocol_lens_defaults__valid(value->u.nullable);
    default: return true;
    }
}
static jz_err validate_protocol_lens_defaults(const jazz_protocol_TableLensVec *table_lenses) {
    for (u32 t = 0; t < table_lenses->len; t++) {
        const jazz_protocol_TableLens *table = &table_lenses->ptr[t];
        for (u32 o = 0; o < table->ops.len; o++) {
            const jazz_protocol_LensOp *op = &table->ops.ptr[o];
            if (lens_op_has_value(op->tag) && !jazz_protocol__protocol__validate_protocol_lens_defaults__valid(&op->value))
                return msg_err("migration lens defaults cannot contain storage-only or descriptor-less values");
        }
    }
    return JZ_OK;
}
jz_err jazz_protocol__protocol__MigrationLens__new(jazz_types_SchemaVersionId source, jazz_types_SchemaVersionId target,
                                                  jazz_protocol_TableLensVec table_lenses, jazz_protocol_MigrationLens *out) {
    jz_sort(table_lenses.ptr, table_lenses.len, sizeof(jazz_protocol_TableLens), cmp_table_lens, NULL);
    jz_err e = validate_protocol_lens_defaults(&table_lenses);
    if (e.code) { jz_vec_drop(&table_lenses, sizeof(jazz_protocol_TableLens), TableLens_drop_v); return e; }
    static const jz_btree_desc DESC_STRREF_SET = { sizeof(jz_str), 0, jz_cmp_str, NULL, NULL };
    jz_btree sources, targets;
    jz_btree_init(&sources, &DESC_STRREF_SET);
    jz_btree_init(&targets, &DESC_STRREF_SET);
    for (u32 i = 0; i < table_lenses.len && !e.code; i++) {
        if (jz_btree_insert(&sources, &table_lenses.ptr[i].source_table, NULL, NULL) ||
            jz_btree_insert(&targets, &table_lenses.ptr[i].target_table, NULL, NULL))
            e = msg_err("migration lens table coordinates must be bijective");
    }
    jz_btree_drop(&sources);
    jz_btree_drop(&targets);
    if (e.code) { jz_vec_drop(&table_lenses, sizeof(jazz_protocol_TableLens), TableLens_drop_v); return e; }
    memset(&out->id, 0, sizeof out->id);
    out->source = source;
    out->target = target;
    out->table_lenses = table_lenses;
    out->id = jazz_protocol__protocol__MigrationLens__content_id(out);
    return JZ_OK;
}
jazz_types_MigrationLensId jazz_protocol__protocol__MigrationLens__content_id(const jazz_protocol_MigrationLens *l) {
    jz_bytes bytes = jazz_protocol__protocol__canonical_lens_bytes(l);
    jz_uuid id = jz_uuid_v5(&MIGRATION_LENS_NAMESPACE, bytes.ptr, bytes.len);
    jz_bytes_drop(&bytes);
    return id;
}
static void put_lens_op(jz_bytes *bytes, const jazz_protocol_LensOp *op) {
    jz_bytes_push_u8(bytes, (u8)op->tag); /* tags 0..6 match the canonical op bytes */
    put_str(bytes, jz_str_slice(&op->a));
    switch (op->tag) {
    case JAZZ_LENS_ADD_COLUMN: case JAZZ_LENS_DROP_COLUMN: jazz_protocol__protocol__put_value(bytes, &op->value); break;
    case JAZZ_LENS_REJECT_SOURCE_DELTA: break;
    default: put_str(bytes, jz_str_slice(&op->b)); break;
    }
}
jz_bytes jazz_protocol__protocol__canonical_lens_bytes(const jazz_protocol_MigrationLens *lens) {
    jz_bytes bytes = {0};
    put_str(&bytes, JZ_LIT("jazz-migration-lens-v1"));
    jz_bytes_push(&bytes, lens->source.b, 16);
    jz_bytes_push(&bytes, lens->target.b, 16);
    u32 n = lens->table_lenses.len;
    const jazz_protocol_TableLens **sorted = jz_alloc((usize)n * sizeof(void *) + 1);
    for (u32 i = 0; i < n; i++) sorted[i] = &lens->table_lenses.ptr[i];
    jz_sort((void *)sorted, n, sizeof(void *), cmp_table_lens_ptr, NULL);
    put_len(&bytes, n);
    for (u32 i = 0; i < n; i++) {
        const jazz_protocol_TableLens *tl = sorted[i];
        put_str(&bytes, jz_str_slice(&tl->source_table));
        put_str(&bytes, jz_str_slice(&tl->target_table));
        put_len(&bytes, tl->ops.len);
        for (u32 o = 0; o < tl->ops.len; o++) put_lens_op(&bytes, &tl->ops.ptr[o]);
    }
    jz_free(sorted);
    return bytes;
}

/* ---------------- ProtocolCodecCursor ---------------- */
#define PCC_MAX_COUNT 16384u
#define PCC_MAX_BYTES (16u * 1024u * 1024u)
#define PCC_MAX_TOTAL_BYTES (32u * 1024u * 1024u)
#define PCC_MAX_TOTAL_ITEMS 65536u
#define PCC_MAX_DEPTH 64u
typedef struct ProtocolCodecCursor { jz_slice bytes; u32 offset; u32 budget; u32 items; } ProtocolCodecCursor;
static jz_err pcc_take(ProtocolCodecCursor *c, u32 n, const u8 **out) {
    if (n > c->budget) return msg_err("protocol aggregate byte budget exceeds limit");
    u64 e = (u64)c->offset + n;
    if (e > c->bytes.len) return msg_err("truncated protocol codec");
    *out = c->bytes.ptr + c->offset;
    c->offset = (u32)e;
    c->budget -= n;
    return JZ_OK;
}
static jz_err pcc_u8(ProtocolCodecCursor *c, u8 *v) { const u8 *p; JZ_TRY(pcc_take(c, 1, &p)); *v = p[0]; return JZ_OK; }
static jz_err pcc_u32(ProtocolCodecCursor *c, u32 *v) { const u8 *p; JZ_TRY(pcc_take(c, 4, &p)); memcpy(v, p, 4); return JZ_OK; }
static jz_err pcc_u64(ProtocolCodecCursor *c, u64 *v) { const u8 *p; JZ_TRY(pcc_take(c, 8, &p)); memcpy(v, p, 8); return JZ_OK; }
static jz_err pcc_uuid(ProtocolCodecCursor *c, jz_uuid *v) { const u8 *p; JZ_TRY(pcc_take(c, 16, &p)); memcpy(v->b, p, 16); return JZ_OK; }
static jz_err pcc_count(ProtocolCodecCursor *c, u32 *out) {
    u32 count;
    JZ_TRY(pcc_u32(c, &count));
    if ((u64)c->items + count > 0xffffffffull) return msg_err("protocol aggregate item budget exceeds limit");
    c->items += count;
    if (count > PCC_MAX_COUNT || c->items > PCC_MAX_TOTAL_ITEMS) return msg_err("protocol collection count exceeds limit");
    *out = count;
    return JZ_OK;
}
static jz_err pcc_bytes(ProtocolCodecCursor *c, jz_bytes *out) {
    u32 n;
    JZ_TRY(pcc_u32(c, &n));
    if (n > PCC_MAX_BYTES) return msg_err("protocol byte length exceeds limit");
    const u8 *p;
    JZ_TRY(pcc_take(c, n, &p));
    *out = jz_bytes_from(p, n);
    return JZ_OK;
}
static jz_err jazz_protocol__protocol__ProtocolCodecCursor__string(ProtocolCodecCursor *c, jz_str *out) {
    jz_bytes b;
    JZ_TRY(pcc_bytes(c, &b));
    if (!jz_utf8_valid(b.ptr, b.len)) { jz_bytes_drop(&b); return msg_err("invalid protocol utf8"); }
    out->ptr = (char *)b.ptr; out->len = b.len; out->cap = b.cap;
    return JZ_OK;
}
static jz_err jazz_protocol__protocol__ProtocolCodecCursor__values_at(ProtocolCodecCursor *c, u32 depth, groove_ValueVec *out);
static jz_err jazz_protocol__protocol__ProtocolCodecCursor__value_at(ProtocolCodecCursor *c, u32 depth, groove_Value *out) {
    if (depth >= PCC_MAX_DEPTH) return msg_err("protocol value nesting exceeds limit");
    u8 tag;
    JZ_TRY(pcc_u8(c, &tag));
    memset(out, 0, sizeof *out);
    switch (tag) {
    case 0: out->tag = GROOVE_V_U8; return pcc_u8(c, &out->u.u8_);
    case 1: { const u8 *p; JZ_TRY(pcc_take(c, 2, &p)); out->tag = GROOVE_V_U16; memcpy(&out->u.u16_, p, 2); return JZ_OK; }
    case 2: out->tag = GROOVE_V_U32; return pcc_u32(c, &out->u.u32_);
    case 3: out->tag = GROOVE_V_U64; return pcc_u64(c, &out->u.u64_);
    case 4: { u64 bits; JZ_TRY(pcc_u64(c, &bits)); out->tag = GROOVE_V_F64; memcpy(&out->u.f64_, &bits, 8); return JZ_OK; }
    case 5: {
        u8 b; JZ_TRY(pcc_u8(c, &b));
        if (b > 1) return msg_err("invalid protocol bool");
        out->tag = GROOVE_V_BOOL; out->u.b = b == 1;
        return JZ_OK;
    }
    case 6: out->tag = GROOVE_V_STRING; return jazz_protocol__protocol__ProtocolCodecCursor__string(c, &out->u.s);
    case 7: out->tag = GROOVE_V_BYTES; return pcc_bytes(c, &out->u.bytes);
    case 8: out->tag = GROOVE_V_UUID; return pcc_uuid(c, &out->u.uuid);
    case 9: out->tag = GROOVE_V_ENUM_TAG; return pcc_u8(c, &out->u.enum_tag);
    case 10: out->tag = GROOVE_V_TUPLE; return jazz_protocol__protocol__ProtocolCodecCursor__values_at(c, depth + 1, &out->u.items);
    case 11: out->tag = GROOVE_V_ARRAY; return jazz_protocol__protocol__ProtocolCodecCursor__values_at(c, depth + 1, &out->u.items);
    case 12: {
        u8 some; JZ_TRY(pcc_u8(c, &some));
        out->tag = GROOVE_V_NULLABLE;
        if (some == 0) return JZ_OK;
        if (some != 1) return msg_err("invalid protocol nullable");
        groove_Value *inner = jz_alloc(sizeof *inner);
        jz_err e = jazz_protocol__protocol__ProtocolCodecCursor__value_at(c, depth + 1, inner);
        if (e.code) { jz_free(inner); return e; }
        out->u.nullable = inner;
        return JZ_OK;
    }
    case 13: { u64 v; JZ_TRY(pcc_u64(c, &v)); out->tag = GROOVE_V_I64; out->u.i64_ = (i64)v; return JZ_OK; }
    case 14: { u32 v; JZ_TRY(pcc_u32(c, &v)); out->tag = GROOVE_V_I32; out->u.i32_ = (i32)v; return JZ_OK; }
    case 15: return msg_err("large default values are not supported in migration lenses");
    default: return msg_err("invalid protocol value tag");
    }
}
static jz_err jazz_protocol__protocol__ProtocolCodecCursor__values_at(ProtocolCodecCursor *c, u32 depth, groove_ValueVec *out) {
    groove_ValueVec v = {0};
    u32 n;
    JZ_TRY(pcc_count(c, &n));
    for (u32 i = 0; i < n; i++) {
        groove_Value x;
        jz_err e = jazz_protocol__protocol__ProtocolCodecCursor__value_at(c, depth, &x);
        if (e.code) { jz_vec_drop(&v, sizeof(groove_Value), groove__records__values__Value__drop_v); return e; }
        jz_vec_push(&v, &x, sizeof x);
    }
    *out = v;
    return JZ_OK;
}
static jz_err pcc_lens_op(ProtocolCodecCursor *c, jazz_protocol_LensOp *op) {
    u8 tag;
    JZ_TRY(pcc_u8(c, &tag));
    if (tag > 6) return msg_err("invalid migration lens op");
    memset(op, 0, sizeof *op);
    op->tag = tag;
    JZ_TRY(jazz_protocol__protocol__ProtocolCodecCursor__string(c, &op->a));
    jz_err e = JZ_OK;
    if (lens_op_has_value(tag)) e = jazz_protocol__protocol__ProtocolCodecCursor__value_at(c, 0, &op->value);
    else if (lens_op_has_b(tag)) e = jazz_protocol__protocol__ProtocolCodecCursor__string(c, &op->b);
    if (e.code) { jz_str_drop(&op->a); return e; }
    return JZ_OK;
}
jz_err jazz_protocol__protocol__decode_canonical_lens_bytes(jz_slice bytes, jazz_protocol_MigrationLens *out) {
    ProtocolCodecCursor c = { bytes, 0, PCC_MAX_TOTAL_BYTES, 0 };
    jz_str marker;
    JZ_TRY(jazz_protocol__protocol__ProtocolCodecCursor__string(&c, &marker));
    bool ok = jz_str_eq_cstr(&marker, "jazz-migration-lens-v1");
    jz_str_drop(&marker);
    if (!ok) return msg_err("invalid migration lens marker");
    jz_uuid source, target;
    JZ_TRY(pcc_uuid(&c, &source));
    JZ_TRY(pcc_uuid(&c, &target));
    jazz_protocol_TableLensVec table_lenses = {0};
    u32 n;
    jz_err e = pcc_count(&c, &n);
    for (u32 i = 0; i < n && !e.code; i++) {
        jazz_protocol_TableLens tl;
        memset(&tl, 0, sizeof tl);
        if ((e = jazz_protocol__protocol__ProtocolCodecCursor__string(&c, &tl.source_table)).code) break;
        if ((e = jazz_protocol__protocol__ProtocolCodecCursor__string(&c, &tl.target_table)).code) { jz_str_drop(&tl.source_table); break; }
        u32 nops;
        e = pcc_count(&c, &nops);
        for (u32 o = 0; o < nops && !e.code; o++) {
            jazz_protocol_LensOp op;
            if (!(e = pcc_lens_op(&c, &op)).code) jz_vec_push(&tl.ops, &op, sizeof op);
        }
        jz_vec_push(&table_lenses, &tl, sizeof tl);
    }
    if (!e.code && c.offset != bytes.len) e = msg_err("trailing protocol codec bytes");
    if (e.code) { jz_vec_drop(&table_lenses, sizeof(jazz_protocol_TableLens), TableLens_drop_v); return e; }
    jazz_protocol_MigrationLens lens;
    e = jazz_protocol__protocol__MigrationLens__new(source, target, table_lenses, &lens);
    if (e.code) { jz_err_drop(&e); return msg_err("invalid migration lens"); }
    jz_bytes canonical = jazz_protocol__protocol__canonical_lens_bytes(&lens);
    bool same = canonical.len == bytes.len && memcmp(canonical.ptr, bytes.ptr, bytes.len) == 0;
    jz_bytes_drop(&canonical);
    if (!same) { jazz_protocol__protocol__MigrationLens__drop(&lens); return msg_err("noncanonical migration lens"); }
    *out = lens;
    return JZ_OK;
}
void jazz_protocol__protocol__MigrationLens__Serialize__serialize(jz_pc_w *w, const jazz_protocol_MigrationLens *l) {
    jz_bytes b = jazz_protocol__protocol__canonical_lens_bytes(l);
    jz_pc_bytes(w, b.ptr, b.len);
    jz_bytes_drop(&b);
}
jz_err jazz_protocol__protocol__MigrationLens__Deserialize__deserialize(jz_pc_r *r, jazz_protocol_MigrationLens *out) {
    jz_slice bytes;
    JZ_TRY(jz_pc_r_bytes_ref(r, &bytes));
    if (bytes.len > PCC_MAX_TOTAL_BYTES) return jz_err_new(JAZZ_PROTO_ERR_SERDE_CUSTOM, "migration lens blob exceeds aggregate byte budget");
    jz_err e = jazz_protocol__protocol__decode_canonical_lens_bytes(bytes, out);
    return e.code ? serde_custom(e) : JZ_OK;
}

/* =====================================================================
 * Part 6: postcard Serialize / Deserialize, Clone and Drop of SyncMessage
 * and every nested message type (serde derive, field order, variant index)
 * ===================================================================== */
#define TRYF(expr) do { if ((e = (expr)).code) goto fail; } while (0)

/* ---- small shared codecs ---- */
static void ser_intern(jz_pc_w *w, jazz_protocol_InternStr s) { jz_pc_str(w, s); }
static jz_err de_intern(jz_pc_r *r, jazz_protocol_InternStr *out) {
    jz_str s;
    JZ_TRY(jz_pc_r_str(r, &s));
    *out = groove__Intern__new_str(s);
    return JZ_OK;
}
static void ser_opt_u64(jz_pc_w *w, bool some, u64 v) { jz_pc_option(w, some); if (some) jz_pc_varint(w, v); }
static jz_err de_opt_u64(jz_pc_r *r, bool *some, u64 *v) { JZ_TRY(jz_pc_r_option(r, some)); return *some ? jz_pc_r_varint(r, v) : JZ_OK; }
static void ser_opt_uuid(jz_pc_w *w, bool some, const jz_uuid *v) { jz_pc_option(w, some); if (some) jz_pc_uuid(w, v); }
static jz_err de_opt_uuid(jz_pc_r *r, bool *some, jz_uuid *v) { JZ_TRY(jz_pc_r_option(r, some)); return *some ? jz_pc_r_uuid(r, v) : JZ_OK; }
static void ser_opt_tx(jz_pc_w *w, const jazz_protocol_OptTxId *o) { jz_pc_option(w, o->some); if (o->some) jazz_model__tx__TxId__Serialize__serialize(w, &o->v); }
static jz_err de_opt_tx(jz_pc_r *r, jazz_protocol_OptTxId *o) { JZ_TRY(jz_pc_r_option(r, &o->some)); return o->some ? jazz_model__tx__TxId__Deserialize__deserialize(r, &o->v) : JZ_OK; }
static void ser_opt_bytes(jz_pc_w *w, const jazz_protocol_OptBytes *o) { jz_pc_option(w, o->some); if (o->some) jz_pc_bytes(w, o->v.ptr, o->v.len); }
static jz_err de_opt_bytes(jz_pc_r *r, jazz_protocol_OptBytes *o) { JZ_TRY(jz_pc_r_option(r, &o->some)); return o->some ? jz_pc_r_bytes(r, &o->v) : JZ_OK; }
static jz_err de_enum(jz_pc_r *r, u32 *out, u32 n) { return jz_pc_r_variant(r, out, n); }
static jz_err de_u16(jz_pc_r *r, u16 *v) { return jz_pc_r_u16(r, v); }

static void ser_value_map(jz_pc_w *w, const jazz_protocol_ValueMap *m) {
    jz_pc_seq(w, m->len);
    jz_str *k; groove_Value *v;
    JZ_BTREE_FOR(m, k, v) { jz_pc_str(w, k); groove__records__values__Value__Serialize__serialize(w, v); }
}
static jz_err de_value_map(jz_pc_r *r, jazz_protocol_ValueMap *out) {
    jz_btree_init(out, &jazz_model__btree_desc_str_value);
    u32 n;
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jz_str k; groove_Value v;
        JZ_TRY(jz_pc_r_str(r, &k));
        jz_err e = groove__records__values__Value__Deserialize__deserialize(r, &v);
        if (e.code) { jz_str_drop(&k); return e; }
        groove_Value old;
        if (jz_btree_insert(out, &k, &v, &old)) groove__records__values__Value__drop(&old);
    }
    return JZ_OK;
}
static void ser_values(jz_pc_w *w, const groove_ValueVec *v) {
    jz_pc_seq(w, v->len);
    for (u32 i = 0; i < v->len; i++) groove__records__values__Value__Serialize__serialize(w, &v->ptr[i]);
}
static jz_err de_values(jz_pc_r *r, groove_ValueVec *out) {
    u32 n;
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        groove_Value *slot = jz_vec_push_slot(out, sizeof *slot);
        jz_err e = groove__records__values__Value__Deserialize__deserialize(r, slot);
        if (e.code) { out->len--; return e; }
    }
    return JZ_OK;
}
static void ser_author(jz_pc_w *w, const jazz_types_AuthorSubject *a) { jazz_types__ids__AuthorSubject__Serialize__serialize(w, a); }
static jz_err de_author(jz_pc_r *r, jazz_types_AuthorSubject *a) { return jazz_types__ids__AuthorSubject__Deserialize__deserialize(r, a); }

void jazz_protocol__protocol__SubscriptionKey__Serialize__serialize(jz_pc_w *w, const jazz_protocol_SubscriptionKey *k) {
    jz_pc_uuid(w, &k->shape_id.u);
    jz_pc_uuid(w, &k->binding_id.u);
    jz_pc_uuid(w, &k->read_view.id);
}
static jz_err de_subscription_key(jz_pc_r *r, jazz_protocol_SubscriptionKey *k) {
    JZ_TRY(jz_pc_r_uuid(r, &k->shape_id.u));
    JZ_TRY(jz_pc_r_uuid(r, &k->binding_id.u));
    return jz_pc_r_uuid(r, &k->read_view.id);
}
static void ser_request_id(jz_pc_w *w, const jazz_protocol_PermissionAdviceRequestId *id) { jz_pc_raw(w, id->b, 16); }
static jz_err de_request_id(jz_pc_r *r, jazz_protocol_PermissionAdviceRequestId *id) { return jz_pc_r_raw(r, id->b, 16); }

/* ---- DelegatedSessionBinding ---- */
static void DelegatedSession_clone(jazz_protocol_DelegatedSessionBinding *d, const jazz_protocol_DelegatedSessionBinding *s) {
    d->identity = s->identity;
    value_map_clone(&d->claims, &s->claims);
}
static void ser_delegated(jz_pc_w *w, bool some, const jazz_protocol_DelegatedSessionBinding *s) {
    jz_pc_option(w, some);
    if (some) { ser_author(w, &s->identity); ser_value_map(w, &s->claims); }
}
static jz_err de_delegated(jz_pc_r *r, bool *some, jazz_protocol_DelegatedSessionBinding *s) {
    JZ_TRY(jz_pc_r_option(r, some));
    if (!*some) { jz_btree_init(&s->claims, &jazz_model__btree_desc_str_value); return JZ_OK; }
    JZ_TRY(de_author(r, &s->identity));
    return de_value_map(r, &s->claims);
}

/* ---- PolicyBindingKey: CanonicalPolicyClaims serializes as its claims map ---- */
static void ser_policy_binding_key(jz_pc_w *w, const jazz_protocol_PolicyBindingKey *k) {
    ser_author(w, &k->identity);
    ser_value_map(w, &k->canonical_claims.claims);
}
static jz_err de_policy_binding_key(jz_pc_r *r, jazz_protocol_PolicyBindingKey *k) {
    JZ_TRY(de_author(r, &k->identity));
    jazz_protocol_ValueMap claims;
    jz_err e = de_value_map(r, &claims);
    if (e.code) { jz_btree_drop(&claims); return e; }
    jazz_protocol__protocol__CanonicalPolicyClaims__new(&k->canonical_claims, claims);
    return JZ_OK;
}

/* ---- RowBase / VersionRecord ---- */
static void ser_row_base(jz_pc_w *w, const jazz_protocol_RowBase *b) {
    ser_opt_u64(w, b->has_seq, b->seq);
    jz_pc_option(w, b->has_pending);
    if (b->has_pending) jazz_model__tx__TxId__Serialize__serialize(w, &b->pending);
}
static jz_err de_row_base(jz_pc_r *r, jazz_protocol_RowBase *b) {
    JZ_TRY(de_opt_u64(r, &b->has_seq, &b->seq));
    JZ_TRY(jz_pc_r_option(r, &b->has_pending));
    return b->has_pending ? jazz_model__tx__TxId__Deserialize__deserialize(r, &b->pending) : JZ_OK;
}
static jz_err ser_version_record(jz_pc_w *w, const jazz_protocol_VersionRecord *v) {
    ser_intern(w, v->table);
    jz_pc_uuid(w, &v->schema_version);
    jazz_model__model__branch__BranchKey__Serialize__serialize(w, &v->branch_key);
    JZ_TRY(wire_row_serialize(w, &v->record));
    jz_pc_option(w, v->has_authored_columns);
    if (v->has_authored_columns) {
        jz_pc_seq(w, v->authored_columns.len);
        jz_str *k; void *unused;
        JZ_BTREE_FOR(&v->authored_columns, k, unused) jz_pc_str(w, k);
    }
    ser_row_base(w, &v->base);
    jz_pc_bytes(w, v->counter_signs.ptr, v->counter_signs.len);
    return JZ_OK;
}
static jz_err de_version_record(jz_pc_r *r, jazz_protocol_VersionRecord *out) {
    jz_err e;
    memset(out, 0, sizeof *out);
    jz_btree_init(&out->authored_columns, &jazz_protocol__protocol__DESC_STR_SET);
    TRYF(de_intern(r, &out->table));
    TRYF(jz_pc_r_uuid(r, &out->schema_version));
    TRYF(jazz_model__model__branch__BranchKey__Deserialize__deserialize(r, &out->branch_key));
    TRYF(wire_row_deserialize(r, &out->record));
    TRYF(jz_pc_r_option(r, &out->has_authored_columns));
    if (out->has_authored_columns) {
        u32 n;
        TRYF(jz_pc_r_seq(r, &n));
        for (u32 i = 0; i < n; i++) {
            jz_str s;
            TRYF(jz_pc_r_str(r, &s));
            jz_str old;
            if (jz_btree_insert(&out->authored_columns, &s, NULL, &old)) {}
        }
    }
    TRYF(de_row_base(r, &out->base));
    TRYF(jz_pc_r_bytes(r, &out->counter_signs));
    return JZ_OK;
fail:
    jazz_protocol__protocol__VersionRecord__drop(out);
    return e;
}
static jz_err ser_versions(jz_pc_w *w, const jazz_protocol_VersionRecord *v, u32 n) {
    jz_pc_seq(w, n);
    for (u32 i = 0; i < n; i++) JZ_TRY(ser_version_record(w, &v[i]));
    return JZ_OK;
}
static jz_err de_versions(jz_pc_r *r, jazz_protocol_VersionRecordVec *out) {
    u32 n;
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_protocol_VersionRecord v;
        JZ_TRY(de_version_record(r, &v));
        jz_vec_push(out, &v, sizeof v);
    }
    return JZ_OK;
}

/* ---- bundles / runs / carriers ---- */
static void ser_opt_gt(jz_pc_w *w, bool some, jazz_types_GlobalTime t) { jz_pc_option(w, some); if (some) jazz_types__time__GlobalTime__Serialize__serialize(w, t); }
static jz_err de_opt_gt(jz_pc_r *r, bool *some, jazz_types_GlobalTime *t) { JZ_TRY(jz_pc_r_option(r, some)); return *some ? jazz_types__time__GlobalTime__Deserialize__deserialize(r, t) : JZ_OK; }
jz_err jazz_protocol__protocol__VersionBundle__Serialize__serialize(jz_pc_w *w, const jazz_protocol_VersionBundle *b) {
    jazz_model__tx__Transaction__Serialize__serialize(w, &b->tx);
    JZ_TRY(ser_versions(w, b->versions.ptr, b->versions.len));
    jz_pc_variant(w, b->scope);
    jazz_model__tx__Fate__Serialize__serialize(w, &b->fate);
    ser_opt_gt(w, b->has_global_time, b->global_time);
    jz_pc_variant(w, b->durability);
    return JZ_OK;
}
/* VersionBundleScope deserialize */
static jz_err jazz_protocol__protocol__VersionBundleScope__Deserialize__deserialize(jz_pc_r *r, u32 *out) { return de_enum(r, out, 2); }
static jz_err de_version_bundle(jz_pc_r *r, jazz_protocol_VersionBundle *b) {
    jz_err e;
    memset(b, 0, sizeof *b);
    TRYF(jazz_model__tx__Transaction__Deserialize__deserialize(r, &b->tx));
    TRYF(de_versions(r, &b->versions));
    TRYF(jazz_protocol__protocol__VersionBundleScope__Deserialize__deserialize(r, &b->scope));
    TRYF(jazz_model__tx__Fate__Deserialize__deserialize(r, &b->fate));
    TRYF(de_opt_gt(r, &b->has_global_time, &b->global_time));
    TRYF(de_enum(r, &b->durability, 3));
    return JZ_OK;
fail:
    jazz_protocol__protocol__VersionBundle__drop(b);
    return e;
}
static jz_err ser_run(jz_pc_w *w, const jazz_protocol_VersionBundleRun *run) {
    const jazz_protocol_VersionBundleRunHeader *h = &run->header;
    jz_pc_option(w, h->has_table);
    if (h->has_table) ser_intern(w, h->table);
    jazz_model__tx__Transaction__Serialize__serialize(w, &h->tx);
    jz_pc_variant(w, h->scope);
    jz_pc_varint(w, h->body_count);
    jazz_model__tx__Fate__Serialize__serialize(w, &h->fate);
    ser_opt_gt(w, h->has_global_time, h->global_time);
    jz_pc_variant(w, h->durability);
    jz_pc_seq(w, run->bodies.len);
    for (u32 i = 0; i < run->bodies.len; i++)
        JZ_TRY(ser_versions(w, run->bodies.ptr[i].versions.ptr, run->bodies.ptr[i].versions.len));
    jz_pc_seq(w, run->overrides.len);
    for (u32 i = 0; i < run->overrides.len; i++) {
        const jazz_protocol_VersionBundleRunOverride *o = &run->overrides.ptr[i];
        jz_pc_varint(w, o->body_index);
        jz_pc_option(w, o->has_tx);
        if (o->has_tx) jazz_model__tx__Transaction__Serialize__serialize(w, &o->tx);
        jz_pc_option(w, o->has_scope);
        if (o->has_scope) jz_pc_variant(w, o->scope);
        jz_pc_option(w, o->has_fate);
        if (o->has_fate) jazz_model__tx__Fate__Serialize__serialize(w, &o->fate);
        jz_pc_option(w, o->has_global_time);
        if (o->has_global_time) ser_opt_gt(w, o->global_time_some, o->global_time);
        jz_pc_option(w, o->has_durability);
        if (o->has_durability) jz_pc_variant(w, o->durability);
    }
    return JZ_OK;
}
static jz_err de_run(jz_pc_r *r, jazz_protocol_VersionBundleRun *run) {
    jz_err e;
    memset(run, 0, sizeof *run);
    jazz_protocol_VersionBundleRunHeader *h = &run->header;
    TRYF(jz_pc_r_option(r, &h->has_table));
    if (h->has_table) TRYF(de_intern(r, &h->table));
    TRYF(jazz_model__tx__Transaction__Deserialize__deserialize(r, &h->tx));
    TRYF(jazz_protocol__protocol__VersionBundleScope__Deserialize__deserialize(r, &h->scope));
    TRYF(jz_pc_r_u32(r, &h->body_count));
    TRYF(jazz_model__tx__Fate__Deserialize__deserialize(r, &h->fate));
    TRYF(de_opt_gt(r, &h->has_global_time, &h->global_time));
    TRYF(de_enum(r, &h->durability, 3));
    u32 n;
    TRYF(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_protocol_VersionBundleRunBody *body = jz_vec_push_slot(&run->bodies, sizeof *body);
        memset(body, 0, sizeof *body);
        TRYF(de_versions(r, &body->versions));
    }
    TRYF(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_protocol_VersionBundleRunOverride *o = jz_vec_push_slot(&run->overrides, sizeof *o);
        memset(o, 0, sizeof *o);
        TRYF(jz_pc_r_u32(r, &o->body_index));
        TRYF(jz_pc_r_option(r, &o->has_tx));
        if (o->has_tx && (e = jazz_model__tx__Transaction__Deserialize__deserialize(r, &o->tx)).code) { o->has_tx = false; goto fail; }
        TRYF(jz_pc_r_option(r, &o->has_scope));
        if (o->has_scope) TRYF(jazz_protocol__protocol__VersionBundleScope__Deserialize__deserialize(r, &o->scope));
        TRYF(jz_pc_r_option(r, &o->has_fate));
        if (o->has_fate && (e = jazz_model__tx__Fate__Deserialize__deserialize(r, &o->fate)).code) { o->has_fate = false; goto fail; }
        TRYF(jz_pc_r_option(r, &o->has_global_time));
        if (o->has_global_time) TRYF(de_opt_gt(r, &o->global_time_some, &o->global_time));
        TRYF(jz_pc_r_option(r, &o->has_durability));
        if (o->has_durability) TRYF(de_enum(r, &o->durability, 3));
    }
    return JZ_OK;
fail:
    VersionBundleRun_drop(run);
    return e;
}
static jz_err ser_carriers(jz_pc_w *w, const jazz_protocol_VersionCarrierVec *cs) {
    jz_pc_seq(w, cs->len);
    for (u32 i = 0; i < cs->len; i++) {
        jz_pc_variant(w, cs->ptr[i].tag);
        if (cs->ptr[i].tag == JAZZ_CARRIER_BUNDLE) JZ_TRY(jazz_protocol__protocol__VersionBundle__Serialize__serialize(w, &cs->ptr[i].u.bundle));
        else JZ_TRY(ser_run(w, &cs->ptr[i].u.run));
    }
    return JZ_OK;
}
static jz_err de_carriers(jz_pc_r *r, jazz_protocol_VersionCarrierVec *out) {
    u32 n;
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_protocol_VersionCarrier c;
        JZ_TRY(jz_pc_r_variant(r, &c.tag, 2));
        if (c.tag == JAZZ_CARRIER_BUNDLE) JZ_TRY(de_version_bundle(r, &c.u.bundle));
        else JZ_TRY(de_run(r, &c.u.run));
        jz_vec_push(out, &c, sizeof c);
    }
    return JZ_OK;
}

/* ---- supporting rows ---- */
static void ser_rvre(jz_pc_w *w, const jazz_protocol_RowVersionRefEntry *e) {
    jazz_model__tx__TxId__Serialize__serialize(w, &e->tx);
    ser_opt_uuid(w, e->has_schema_version, &e->schema_version);
    jz_pc_variant(w, e->layer);
    ser_opt_tx(w, &e->batch);
    ser_opt_bytes(w, &e->branch_or_prefix);
    ser_opt_bytes(w, &e->row_digest);
}
static jz_err de_rvre(jz_pc_r *r, jazz_protocol_RowVersionRefEntry *o) {
    JZ_TRY(jazz_model__tx__TxId__Deserialize__deserialize(r, &o->tx));
    JZ_TRY(de_opt_uuid(r, &o->has_schema_version, &o->schema_version));
    JZ_TRY(de_enum(r, &o->layer, 3));
    JZ_TRY(de_opt_tx(r, &o->batch));
    JZ_TRY(de_opt_bytes(r, &o->branch_or_prefix));
    return de_opt_bytes(r, &o->row_digest);
}
void jazz_protocol__protocol__SupportingRow__Serialize__serialize(jz_pc_w *w, const jazz_protocol_SupportingRow *row) {
    jz_pc_uuid(w, &row->physical_table);
    ser_intern(w, row->version_table);
    jz_pc_uuid(w, &row->row);
    ser_rvre(w, &row->version);
}
/* supporting_table_name: thread_local LAST intern handle */
static jazz_protocol_InternStr SUPPORTING_TABLE_LAST;
static jazz_protocol_InternStr jazz_protocol__protocol__supporting_table_name__intern(jz_slice name) {
    if (SUPPORTING_TABLE_LAST && slice_eq(istr(SUPPORTING_TABLE_LAST), name)) return SUPPORTING_TABLE_LAST;
    jazz_protocol_InternStr found = groove__Intern__from_ref_str(name);
    SUPPORTING_TABLE_LAST = found;
    return found;
}
static jz_err jazz_protocol__protocol__supporting_table_name__deserialize(jz_pc_r *r, jazz_protocol_InternStr *out) {
    jz_slice s;
    JZ_TRY(jz_pc_r_bytes_ref(r, &s));
    if (!jz_utf8_valid(s.ptr, s.len)) return jz_err_new(JZ_PC_BAD_UTF8, "invalid value: byte array, expected a string");
    *out = jazz_protocol__protocol__supporting_table_name__intern(s);
    return JZ_OK;
}
static jz_err de_supporting_row(jz_pc_r *r, jazz_protocol_SupportingRow *row) {
    memset(row, 0, sizeof *row);
    JZ_TRY(jz_pc_r_uuid(r, &row->physical_table));
    JZ_TRY(jazz_protocol__protocol__supporting_table_name__deserialize(r, &row->version_table));
    JZ_TRY(jz_pc_r_uuid(r, &row->row));
    jz_err e = de_rvre(r, &row->version);
    if (e.code) jazz_protocol__protocol__RowVersionRefEntry__drop(&row->version);
    return e;
}
static void ser_supporting_rows(jz_pc_w *w, const jazz_protocol_SupportingRowVec *v) {
    jz_pc_seq(w, v->len);
    for (u32 i = 0; i < v->len; i++) jazz_protocol__protocol__SupportingRow__Serialize__serialize(w, &v->ptr[i]);
}
static jz_err de_supporting_rows(jz_pc_r *r, jazz_protocol_SupportingRowVec *out) {
    u32 n;
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_protocol_SupportingRow row;
        JZ_TRY(de_supporting_row(r, &row));
        jz_vec_push(out, &row, sizeof row);
    }
    return JZ_OK;
}
static void ser_sru(jz_pc_w *w, const jazz_protocol_SupportingRowsUpdate *s) {
    jz_pc_variant(w, s->tag);
    if (s->tag != JAZZ_SRU_SNAPSHOT) jz_pc_raw(w, s->predecessor, 16);
    jz_pc_raw(w, s->revision, 16);
    ser_supporting_rows(w, &s->added);
    if (s->tag != JAZZ_SRU_SNAPSHOT) ser_supporting_rows(w, &s->removed);
}
static jz_err de_sru(jz_pc_r *r, jazz_protocol_SupportingRowsUpdate *s) {
    memset(s, 0, sizeof *s);
    JZ_TRY(jz_pc_r_variant(r, &s->tag, 3));
    if (s->tag != JAZZ_SRU_SNAPSHOT) JZ_TRY(jz_pc_r_raw(r, s->predecessor, 16));
    JZ_TRY(jz_pc_r_raw(r, s->revision, 16));
    JZ_TRY(de_supporting_rows(r, &s->added));
    if (s->tag != JAZZ_SRU_SNAPSHOT) JZ_TRY(de_supporting_rows(r, &s->removed));
    return JZ_OK;
}

/* ---- ViewUpdatePayload ---- */
jz_err jazz_protocol__protocol__ViewUpdatePayload__Serialize__serialize(jz_pc_w *w, const jazz_protocol_ViewUpdatePayload *v) {
    jazz_protocol__protocol__SubscriptionKey__Serialize__serialize(w, &v->subscription);
    jazz_types__time__GlobalTime__Serialize__serialize(w, v->settled_through);
    JZ_TRY(ser_carriers(w, &v->version_carriers));
    const jazz_protocol_PeerPayloadInventory *p = &v->peer_payload_inventory;
    jz_pc_seq(w, p->complete_tx_payloads.len);
    for (u32 i = 0; i < p->complete_tx_payloads.len; i++) jazz_model__tx__TxId__Serialize__serialize(w, &p->complete_tx_payloads.ptr[i]);
    ser_opt_u64(w, p->has_authorization_progress, p->authorization_progress);
    jz_pc_bool(w, p->opening_pending);
    ser_sru(w, &v->supporting_rows);
    return JZ_OK;
}
static jz_err de_view_update(jz_pc_r *r, jazz_protocol_ViewUpdatePayload *v) {
    jz_err e;
    memset(v, 0, sizeof *v);
    TRYF(de_subscription_key(r, &v->subscription));
    TRYF(jazz_types__time__GlobalTime__Deserialize__deserialize(r, &v->settled_through));
    TRYF(de_carriers(r, &v->version_carriers));
    jazz_protocol_PeerPayloadInventory *p = &v->peer_payload_inventory;
    u32 n;
    TRYF(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_model_TxId tx;
        TRYF(jazz_model__tx__TxId__Deserialize__deserialize(r, &tx));
        jz_vec_push(&p->complete_tx_payloads, &tx, sizeof tx);
    }
    TRYF(de_opt_u64(r, &p->has_authorization_progress, &p->authorization_progress));
    TRYF(jz_pc_r_bool(r, &p->opening_pending));
    TRYF(de_sru(r, &v->supporting_rows));
    return JZ_OK;
fail:
    jazz_protocol__protocol__ViewUpdatePayload__drop(v);
    return e;
}

/* ---- shapes ---- */
void jazz_protocol__protocol__ShapeAst__Serialize__serialize(jz_pc_w *w, const jazz_protocol_ShapeAst *a) {
    jz_pc_varint(w, a->version);
    jz_pc_uuid(w, &a->schema_version);
    jz_pc_variant(w, a->body_tag);
    if (a->body_tag == JAZZ_SHAPE_QUERY) jazz_model__query__ast__Query__Serialize__serialize(w, a->query);
    else jazz_model__query__ast__relation_query_wire__serialize_required(w, a->relation);
}
static jz_err de_shape_ast(jz_pc_r *r, jazz_protocol_ShapeAst *a) {
    memset(a, 0, sizeof *a);
    JZ_TRY(jz_pc_r_u32(r, &a->version));
    JZ_TRY(jz_pc_r_uuid(r, &a->schema_version));
    JZ_TRY(jz_pc_r_variant(r, &a->body_tag, 2));
    if (a->body_tag == JAZZ_SHAPE_QUERY) return jazz_model__query__ast__Query__Deserialize__deserialize(r, &a->query);
    return jazz_model__query__ast__relation_query_wire__deserialize_required(r, &a->relation);
}
static void ser_read_view_source(jz_pc_w *w, const jazz_protocol_ReadViewSourceSpec *s) {
    jz_pc_variant(w, s->tag);
    if (s->tag == JAZZ_RVS_BRANCH_VIEW) {
        jazz_model__model__branch__BranchSelector__Serialize__serialize(w, &s->head);
        jz_pc_option(w, s->has_base);
        if (s->has_base) jazz_model__model__branch__BranchViewBase__Serialize__serialize(w, &s->base);
    } else if (s->tag == JAZZ_RVS_SNAPSHOT) {
        jazz_model__model__branch__SnapshotRef__Serialize__serialize(w, &s->snapshot);
    }
}
/* <ReadViewSourceSpec as Deserialize>::__Visitor::visit_enum */
static jz_err jazz_protocol__protocol__ReadViewSourceSpec__Deserialize__visit_enum(jz_pc_r *r, jazz_protocol_ReadViewSourceSpec *s) {
    memset(s, 0, sizeof *s);
    JZ_TRY(jz_pc_r_variant(r, &s->tag, 3));
    if (s->tag == JAZZ_RVS_BRANCH_VIEW) {
        JZ_TRY(jazz_model__model__branch__BranchSelector__Deserialize__deserialize(r, &s->head));
        JZ_TRY(jz_pc_r_option(r, &s->has_base));
        if (s->has_base) JZ_TRY(jazz_model__model__branch__BranchViewBase__Deserialize__deserialize(r, &s->base));
    } else if (s->tag == JAZZ_RVS_SNAPSHOT) {
        JZ_TRY(jazz_model__model__branch__SnapshotRef__Deserialize__deserialize(r, &s->snapshot));
    }
    return JZ_OK;
}
void jazz_protocol__protocol__RegisterShapeOptions__Serialize__serialize(jz_pc_w *w, const jazz_protocol_RegisterShapeOptions *o) {
    jz_pc_variant(w, o->tier);
    ser_read_view_source(w, &o->read_view.source);
    jz_pc_variant(w, o->binding_source);
}
static jz_err de_register_shape_options(jz_pc_r *r, jazz_protocol_RegisterShapeOptions *o) {
    JZ_TRY(de_enum(r, &o->tier, 3));
    JZ_TRY(jazz_protocol__protocol__ReadViewSourceSpec__Deserialize__visit_enum(r, &o->read_view.source));
    return de_enum(r, &o->binding_source, 2);
}

/* ---- known state / subscribe ---- */
static void ser_known_state(jz_pc_w *w, const jazz_protocol_KnownStateDeclaration *k) {
    jz_pc_variant(w, k->tag);
    switch (k->tag) {
    case JAZZ_KSD_FAST: jz_pc_variant(w, k->completeness); jz_pc_varint(w, k->position); break;
    case JAZZ_KSD_FAST_WITH_AUTH: jz_pc_variant(w, k->completeness); jz_pc_varint(w, k->position); jz_pc_varint(w, k->authorization_progress); break;
    case JAZZ_KSD_RESERVED2: JZ_UNREACHABLE();
    default: jz_pc_varint(w, k->position); ser_opt_u64(w, k->has_authorization_progress, k->authorization_progress); jz_pc_raw(w, k->supporting_revision, 16); break;
    }
}
static jz_err de_known_state(jz_pc_r *r, jazz_protocol_KnownStateDeclaration *k) {
    memset(k, 0, sizeof *k);
    JZ_TRY(jz_pc_r_variant(r, &k->tag, 4));
    switch (k->tag) {
    case JAZZ_KSD_FAST:
        JZ_TRY(de_enum(r, &k->completeness, 1));
        return jazz_types__time__GlobalTime__Deserialize__deserialize(r, &k->position);
    case JAZZ_KSD_FAST_WITH_AUTH:
        JZ_TRY(de_enum(r, &k->completeness, 1));
        JZ_TRY(jazz_types__time__GlobalTime__Deserialize__deserialize(r, &k->position));
        k->has_authorization_progress = true;
        return jz_pc_r_varint(r, &k->authorization_progress);
    case JAZZ_KSD_RESERVED2: { u32 none; return jz_pc_r_variant(r, &none, 0); } /* ReservedWireMessage is uninhabited */
    default:
        JZ_TRY(jazz_types__time__GlobalTime__Deserialize__deserialize(r, &k->position));
        JZ_TRY(de_opt_u64(r, &k->has_authorization_progress, &k->authorization_progress));
        return jz_pc_r_raw(r, k->supporting_revision, 16);
    }
}
static void Subscribe_drop(jazz_protocol_Subscribe *s) {
    jz_vec_drop(&s->values, sizeof(groove_Value), groove__records__values__Value__drop_v);
    jz_btree_drop(&s->delegated_session.claims);
}
static void ser_subscribe(jz_pc_w *w, const jazz_protocol_Subscribe *s) {
    jz_pc_uuid(w, &s->shape_id.u);
    jazz_protocol__protocol__SubscriptionKey__Serialize__serialize(w, &s->subscription);
    ser_values(w, &s->values);
    jz_pc_option(w, s->has_known_state);
    if (s->has_known_state) ser_known_state(w, &s->known_state);
    ser_delegated(w, s->has_delegated_session, &s->delegated_session);
}
static jz_err de_subscribe(jz_pc_r *r, jazz_protocol_Subscribe *s) {
    jz_err e;
    memset(s, 0, sizeof *s);
    jz_btree_init(&s->delegated_session.claims, &jazz_model__btree_desc_str_value);
    TRYF(jz_pc_r_uuid(r, &s->shape_id.u));
    TRYF(de_subscription_key(r, &s->subscription));
    TRYF(de_values(r, &s->values));
    TRYF(jz_pc_r_option(r, &s->has_known_state));
    if (s->has_known_state) TRYF(de_known_state(r, &s->known_state));
    TRYF(de_delegated(r, &s->has_delegated_session, &s->delegated_session));
    return JZ_OK;
fail:
    Subscribe_drop(s);
    return e;
}
static void ser_reject_reason(jz_pc_w *w, const jazz_protocol_SubscribeRejectReason *r) {
    jz_pc_variant(w, r->tag);
    if (r->tag == JAZZ_SRR_UNSUPPORTED_SHAPE_CAPABILITY || r->tag == JAZZ_SRR_INVALID_AUTHORITY_SOURCE_CLOSURE) jz_pc_str(w, &r->text);
    else if (r->tag == JAZZ_SRR_SERVER_FAILURE) jz_pc_variant(w, r->code);
}
static jz_err de_reject_reason(jz_pc_r *r, jazz_protocol_SubscribeRejectReason *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_pc_r_variant(r, &out->tag, 4));
    if (out->tag == JAZZ_SRR_UNSUPPORTED_SHAPE_CAPABILITY || out->tag == JAZZ_SRR_INVALID_AUTHORITY_SOURCE_CLOSURE) return jz_pc_r_str(r, &out->text);
    if (out->tag == JAZZ_SRR_SERVER_FAILURE) return de_enum(r, &out->code, 6);
    return JZ_OK;
}

/* ---- schemas / catalogue ---- */
static void ser_strvec(jz_pc_w *w, const jazz_protocol_StrVec *v) { jz_pc_seq(w, v->len); for (u32 i = 0; i < v->len; i++) jz_pc_str(w, &v->ptr[i]); }
static jz_err de_strvec(jz_pc_r *r, jazz_protocol_StrVec *out) {
    u32 n;
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) { jz_str s; JZ_TRY(jz_pc_r_str(r, &s)); jz_vec_push(out, &s, sizeof s); }
    return JZ_OK;
}
static void ser_manifest(jz_pc_w *w, const jazz_protocol_PhysicalIdentityManifest *m) {
    jz_pc_seq(w, m->tables.len);
    jz_str *tn; jazz_protocol_PhysicalTableIdentity *t;
    JZ_BTREE_FOR(&m->tables, tn, t) {
        jz_pc_str(w, tn);
        jz_pc_uuid(w, &t->id);
        jz_pc_seq(w, t->columns.len);
        jz_str *cn; jazz_protocol_PhysicalColumnIdentity *c;
        JZ_BTREE_FOR(&t->columns, cn, c) {
            jz_pc_str(w, cn);
            jz_pc_uuid(w, &c->id);
            jz_pc_seq(w, c->enum_variants.len);
            jz_str *path; jazz_protocol_UuidVec *vs;
            JZ_BTREE_FOR(&c->enum_variants, path, vs) {
                jz_pc_str(w, path);
                jz_pc_seq(w, vs->len);
                for (u32 i = 0; i < vs->len; i++) jz_pc_uuid(w, &vs->ptr[i]);
            }
        }
    }
}
static jz_err de_manifest(jz_pc_r *r, jazz_protocol_PhysicalIdentityManifest *m) {
    jz_btree_init(&m->tables, &DESC_TABLE_IDENTITIES);
    u32 nt;
    JZ_TRY(jz_pc_r_seq(r, &nt));
    for (u32 i = 0; i < nt; i++) {
        jz_str tn;
        JZ_TRY(jz_pc_r_str(r, &tn));
        jazz_protocol_PhysicalTableIdentity t;
        jz_btree_init(&t.columns, &DESC_COLUMN_IDENTITIES);
        jz_err e = jz_pc_r_uuid(r, &t.id);
        u32 nc = 0;
        if (!e.code) e = jz_pc_r_seq(r, &nc);
        for (u32 j = 0; j < nc && !e.code; j++) {
            jz_str cn;
            if ((e = jz_pc_r_str(r, &cn)).code) break;
            jazz_protocol_PhysicalColumnIdentity c;
            jz_btree_init(&c.enum_variants, &DESC_STR_UUIDVEC);
            e = jz_pc_r_uuid(r, &c.id);
            u32 nv = 0;
            if (!e.code) e = jz_pc_r_seq(r, &nv);
            for (u32 k = 0; k < nv && !e.code; k++) {
                jz_str path;
                if ((e = jz_pc_r_str(r, &path)).code) break;
                jazz_protocol_UuidVec vs = {0};
                u32 nu = 0;
                e = jz_pc_r_seq(r, &nu);
                for (u32 x = 0; x < nu && !e.code; x++) { jz_uuid u; if (!(e = jz_pc_r_uuid(r, &u)).code) JZ_VEC_PUSH(vs, u); }
                jazz_protocol_UuidVec old;
                if (jz_btree_insert(&c.enum_variants, &path, &vs, &old)) jz_vec_drop(&old, sizeof(jz_uuid), NULL);
            }
            jazz_protocol_PhysicalColumnIdentity oldc;
            if (jz_btree_insert(&t.columns, &cn, &c, &oldc)) PhysicalColumnIdentity_drop_v(&oldc);
        }
        jazz_protocol_PhysicalTableIdentity oldt;
        if (jz_btree_insert(&m->tables, &tn, &t, &oldt)) PhysicalTableIdentity_drop_v(&oldt);
        JZ_TRY(e);
    }
    return JZ_OK;
}
jz_err jazz_protocol__protocol__SchemaLineagePublication__Serialize__serialize(jz_pc_w *w, const jazz_protocol_SchemaLineagePublication *p) {
    jz_pc_uuid(w, &p->id);
    JZ_TRY(jazz_protocol__protocol__SchemaVersion__Serialize__serialize(w, &p->schema));
    jz_pc_seq(w, p->predecessors.len);
    for (u32 i = 0; i < p->predecessors.len; i++) {
        const jazz_protocol_SchemaPredecessor *pred = &p->predecessors.ptr[i];
        jazz_protocol__protocol__MigrationLens__Serialize__serialize(w, &pred->lens);
        ser_strvec(w, &pred->new_tables);
        ser_strvec(w, &pred->dropped_tables);
    }
    ser_manifest(w, &p->physical_identities);
    return JZ_OK;
}
jz_err jazz_protocol__protocol__SchemaLineagePublication__Deserialize__deserialize(jz_pc_r *r, jazz_protocol_SchemaLineagePublication *p) {
    jz_err e;
    memset(p, 0, sizeof *p);
    jz_btree_init(&p->physical_identities.tables, &DESC_TABLE_IDENTITIES);
    TRYF(jz_pc_r_uuid(r, &p->id));
    TRYF(jazz_protocol__protocol__SchemaVersion__Deserialize__deserialize(r, &p->schema));
    u32 n;
    TRYF(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_protocol_SchemaPredecessor *pred = jz_vec_push_slot(&p->predecessors, sizeof *pred);
        memset(pred, 0, sizeof *pred);
        TRYF(jazz_protocol__protocol__MigrationLens__Deserialize__deserialize(r, &pred->lens));
        TRYF(de_strvec(r, &pred->new_tables));
        TRYF(de_strvec(r, &pred->dropped_tables));
    }
    jz_btree_drop(&p->physical_identities.tables);
    TRYF(de_manifest(r, &p->physical_identities));
    return JZ_OK;
fail:
    jazz_protocol__protocol__SchemaLineagePublication__drop(p);
    return e;
}
static void CatalogueSnapshot_drop(jazz_protocol_CatalogueSnapshot *s) {
    jazz_protocol__protocol__PhysicalIdentityManifest__drop(&s->genesis_physical_identities);
    for (u32 i = 0; i < s->schemas.len; i++) jazz_protocol__protocol__SchemaVersion__drop(&s->schemas.ptr[i]);
    jz_free(s->schemas.ptr);
    for (u32 i = 0; i < s->lineages.len; i++) jazz_protocol__protocol__SchemaLineagePublication__drop(&s->lineages.ptr[i].publication);
    jz_free(s->lineages.ptr);
}
static void CatalogueSnapshot_clone(jazz_protocol_CatalogueSnapshot *d, const jazz_protocol_CatalogueSnapshot *s) {
    jazz_protocol__protocol__PhysicalIdentityManifest__Clone__clone(&d->genesis_physical_identities, &s->genesis_physical_identities);
    memset(&d->schemas, 0, sizeof d->schemas);
    memset(&d->lineages, 0, sizeof d->lineages);
    for (u32 i = 0; i < s->schemas.len; i++) SchemaVersion_clone(jz_vec_push_slot(&d->schemas, sizeof(jazz_protocol_SchemaVersion)), &s->schemas.ptr[i]);
    for (u32 i = 0; i < s->lineages.len; i++) {
        jazz_protocol_SeqLineage *l = jz_vec_push_slot(&d->lineages, sizeof *l);
        l->seq = s->lineages.ptr[i].seq;
        jazz_protocol__protocol__SchemaLineagePublication__Clone__clone(&l->publication, &s->lineages.ptr[i].publication);
    }
    d->current_write_schema = s->current_write_schema;
}
static jz_err ser_catalogue_snapshot(jz_pc_w *w, const jazz_protocol_CatalogueSnapshot *s) {
    ser_manifest(w, &s->genesis_physical_identities);
    jz_pc_seq(w, s->schemas.len);
    for (u32 i = 0; i < s->schemas.len; i++) JZ_TRY(jazz_protocol__protocol__SchemaVersion__Serialize__serialize(w, &s->schemas.ptr[i]));
    jz_pc_seq(w, s->lineages.len);
    for (u32 i = 0; i < s->lineages.len; i++) {
        jz_pc_varint(w, s->lineages.ptr[i].seq);
        JZ_TRY(jazz_protocol__protocol__SchemaLineagePublication__Serialize__serialize(w, &s->lineages.ptr[i].publication));
    }
    jz_pc_varint(w, s->current_write_schema.revision);
    jz_pc_uuid(w, &s->current_write_schema.schema);
    return JZ_OK;
}
static jz_err de_catalogue_snapshot(jz_pc_r *r, jazz_protocol_CatalogueSnapshot *s) {
    jz_err e;
    memset(s, 0, sizeof *s);
    TRYF(de_manifest(r, &s->genesis_physical_identities));
    u32 n;
    TRYF(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_protocol_SchemaVersion sv;
        TRYF(jazz_protocol__protocol__SchemaVersion__Deserialize__deserialize(r, &sv));
        jz_vec_push(&s->schemas, &sv, sizeof sv);
    }
    TRYF(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_protocol_SeqLineage l;
        TRYF(jz_pc_r_varint(r, &l.seq));
        TRYF(jazz_protocol__protocol__SchemaLineagePublication__Deserialize__deserialize(r, &l.publication));
        jz_vec_push(&s->lineages, &l, sizeof l);
    }
    TRYF(jz_pc_r_varint(r, &s->current_write_schema.revision));
    TRYF(jz_pc_r_uuid(r, &s->current_write_schema.schema));
    return JZ_OK;
fail:
    CatalogueSnapshot_drop(s);
    return e;
}

/* ---- authorization scope ---- */
void jazz_protocol__protocol__AuthorizationSupportScopeKey__Serialize__serialize(jz_pc_w *w, const jazz_protocol_AuthorizationSupportScopeKey *k) {
    jz_pc_raw(w, k->support_shape_digest, 32);
    ser_author(w, &k->subject);
    jz_pc_raw(w, k->claims_digest, 32);
    jz_pc_raw(w, k->policy_digest, 32);
}
static jz_err de_scope_key(jz_pc_r *r, jazz_protocol_AuthorizationSupportScopeKey *k) {
    JZ_TRY(jz_pc_r_raw(r, k->support_shape_digest, 32));
    JZ_TRY(de_author(r, &k->subject));
    JZ_TRY(jz_pc_r_raw(r, k->claims_digest, 32));
    return jz_pc_r_raw(r, k->policy_digest, 32);
}
static void ser_scope_receipt(jz_pc_w *w, const jazz_protocol_AuthorizationScopeReceipt *rc) {
    jazz_protocol__protocol__AuthorizationSupportScopeKey__Serialize__serialize(w, &rc->key);
    jz_pc_raw(w, rc->authority, 16);
    ser_author(w, &rc->link);
    jz_pc_varint(w, rc->authority_epoch);
    jz_pc_varint(w, rc->claims_revision);
    jz_pc_varint(w, rc->policy_epoch);
    jz_pc_varint(w, rc->settled_through);
    jz_pc_varint(w, rc->authorization_progress);
}
static jz_err de_scope_receipt(jz_pc_r *r, jazz_protocol_AuthorizationScopeReceipt *rc) {
    JZ_TRY(de_scope_key(r, &rc->key));
    JZ_TRY(jz_pc_r_raw(r, rc->authority, 16));
    JZ_TRY(de_author(r, &rc->link));
    JZ_TRY(jz_pc_r_varint(r, &rc->authority_epoch));
    JZ_TRY(jz_pc_r_varint(r, &rc->claims_revision));
    JZ_TRY(jz_pc_r_varint(r, &rc->policy_epoch));
    JZ_TRY(jazz_types__time__GlobalTime__Deserialize__deserialize(r, &rc->settled_through));
    return jz_pc_r_varint(r, &rc->authorization_progress);
}
static void ser_action(jz_pc_w *w, const jazz_protocol_PermissionAdviceAction *a) {
    jz_pc_variant(w, a->tag);
    jz_pc_str(w, &a->table);
    switch (a->tag) {
    case JAZZ_PAA_INSERT: ser_value_map(w, &a->cells); break;
    case JAZZ_PAA_UPDATE: jz_pc_uuid(w, &a->row); ser_value_map(w, &a->cells); break;
    default: jz_pc_uuid(w, &a->row); break;
    }
}
static jz_err de_action(jz_pc_r *r, jazz_protocol_PermissionAdviceAction *a) {
    memset(a, 0, sizeof *a);
    jz_btree_init(&a->cells, &jazz_model__btree_desc_str_value);
    JZ_TRY(jz_pc_r_variant(r, &a->tag, 4));
    JZ_TRY(jz_pc_r_str(r, &a->table));
    switch (a->tag) {
    case JAZZ_PAA_INSERT: return de_value_map(r, &a->cells);
    case JAZZ_PAA_UPDATE: JZ_TRY(jz_pc_r_uuid(r, &a->row)); return de_value_map(r, &a->cells);
    default: return jz_pc_r_uuid(r, &a->row);
    }
}

/* ---- chunks ---- */
static void ser_chunk_request_batch(jz_pc_w *w, const jazz_protocol_ChunkRequestBatch *b) {
    jz_pc_seq(w, b->requests.len);
    for (u32 i = 0; i < b->requests.len; i++) {
        const jazz_protocol_ChunkRequestEntry *e = &b->requests.ptr[i];
        jz_pc_varint(w, e->request_id);
        groove__large_values__Locator__Serialize__serialize(w, &e->locator);
        jz_pc_raw(w, e->expected_hash, 32);
        jz_pc_u8(w, e->remaining_hops);
    }
}
/* deserialize_with = "deserialize_chunk_requests" (ChunkRequestsVisitor::visit_seq) */
static jz_err jazz_protocol__protocol__deserialize_chunk_requests(jz_pc_r *r, jazz_protocol_ChunkRequestBatch *b) {
    u32 declared;
    JZ_TRY(jz_pc_r_seq(r, &declared));
    if (declared > JAZZ_MAX_CHUNK_REQUEST_BATCH_ENTRIES)
        return jz_err_new(JAZZ_PROTO_ERR_SERDE_CUSTOM, "chunk request batch exceeds cardinality limit");
    jz_vec_reserve(&b->requests, declared < JAZZ_MAX_CHUNK_REQUEST_BATCH_ENTRIES ? declared : JAZZ_MAX_CHUNK_REQUEST_BATCH_ENTRIES, sizeof(jazz_protocol_ChunkRequestEntry));
    for (u32 i = 0; i < declared; i++) {
        jazz_protocol_ChunkRequestEntry e;
        JZ_TRY(jz_pc_r_varint(r, &e.request_id));
        JZ_TRY(groove__large_values__Locator__Deserialize__deserialize(r, &e.locator));
        JZ_TRY(jz_pc_r_raw(r, e.expected_hash, 32));
        JZ_TRY(jz_pc_r_u8(r, &e.remaining_hops));
        if (b->requests.len >= JAZZ_MAX_CHUNK_REQUEST_BATCH_ENTRIES)
            return jz_err_new(JAZZ_PROTO_ERR_SERDE_CUSTOM, "chunk request batch exceeds cardinality limit");
        jz_vec_push(&b->requests, &e, sizeof e);
    }
    return JZ_OK;
}
static void ChunkResponseEntry_drop_v(void *p) { jazz_protocol_ChunkResponseEntry *e = p; if (e->result.tag == JAZZ_CHUNK_FOUND) jz_bytes_drop(&e->result.found); }
static void ChunkResponseEntry_clone_v(void *d_, const void *s_) {
    jazz_protocol_ChunkResponseEntry *d = d_; const jazz_protocol_ChunkResponseEntry *s = s_;
    *d = *s;
    if (s->result.tag == JAZZ_CHUNK_FOUND) d->result.found = jz_bytes_clone(&s->result.found);
}
static void ser_chunk_response_batch(jz_pc_w *w, const jazz_protocol_ChunkResponseBatch *b) {
    jz_pc_seq(w, b->responses.len);
    for (u32 i = 0; i < b->responses.len; i++) {
        const jazz_protocol_ChunkResponseEntry *e = &b->responses.ptr[i];
        jz_pc_varint(w, e->request_id);
        jz_pc_variant(w, e->result.tag);
        if (e->result.tag == JAZZ_CHUNK_FOUND) jz_pc_bytes(w, e->result.found.ptr, e->result.found.len);
        else if (e->result.tag == JAZZ_CHUNK_RETRYABLE) jz_pc_varint(w, e->result.retry_after_ms);
    }
}
static jz_err de_chunk_response_batch(jz_pc_r *r, jazz_protocol_ChunkResponseBatch *b) {
    u32 n;
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_protocol_ChunkResponseEntry e;
        memset(&e, 0, sizeof e);
        JZ_TRY(jz_pc_r_varint(r, &e.request_id));
        JZ_TRY(jz_pc_r_variant(r, &e.result.tag, 3));
        if (e.result.tag == JAZZ_CHUNK_FOUND) JZ_TRY(jz_pc_r_bytes(r, &e.result.found));
        else if (e.result.tag == JAZZ_CHUNK_RETRYABLE) JZ_TRY(jz_pc_r_u32(r, &e.result.retry_after_ms));
        jz_vec_push(&b->responses, &e, sizeof e);
    }
    return JZ_OK;
}
static void StagedChunk_clone_v(void *d_, const void *s_) {
    groove_StagedChunk *d = d_; const groove_StagedChunk *s = s_;
    d->node = s->node; d->bytes = jz_bytes_clone(&s->bytes);
}
static void StagedChunk_drop_v(void *p) { jz_bytes_drop(&((groove_StagedChunk *)p)->bytes); }

/* ---- current rows ---- */
static void CurrentRowCoordinate_clone_v(void *d_, const void *s_) {
    jazz_protocol_CurrentRowCoordinate *d = d_; const jazz_protocol_CurrentRowCoordinate *s = s_;
    *d = *s; d->table = jz_str_clone(&s->table);
}
static void CurrentRowCoordinate_drop_v(void *p) { jz_str_drop(&((jazz_protocol_CurrentRowCoordinate *)p)->table); }
static void ser_row_coords(jz_pc_w *w, const jazz_protocol_CurrentRowCoordinateVec *v) {
    jz_pc_seq(w, v->len);
    for (u32 i = 0; i < v->len; i++) {
        jz_pc_uuid(w, &v->ptr[i].schema);
        jz_pc_str(w, &v->ptr[i].table);
        jz_pc_uuid(w, &v->ptr[i].physical_table);
        jz_pc_uuid(w, &v->ptr[i].row);
    }
}
static jz_err de_row_coords(jz_pc_r *r, jazz_protocol_CurrentRowCoordinateVec *out) {
    u32 n;
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_protocol_CurrentRowCoordinate c;
        JZ_TRY(jz_pc_r_uuid(r, &c.schema));
        JZ_TRY(jz_pc_r_str(r, &c.table));
        jz_err e = jz_pc_r_uuid(r, &c.physical_table);
        if (!e.code) e = jz_pc_r_uuid(r, &c.row);
        if (e.code) { jz_str_drop(&c.table); return e; }
        jz_vec_push(out, &c, sizeof c);
    }
    return JZ_OK;
}
static void CurrentRowsReceipt_drop(jazz_protocol_CurrentRowsReceipt *rc) {
    jz_vec_drop(&rc->rows, sizeof(jazz_protocol_CurrentRowCoordinate), CurrentRowCoordinate_drop_v);
    jz_vec_drop(&rc->outcomes, 1, NULL);
    jazz_protocol__protocol__PolicyBindingKey__drop(&rc->context);
    carriers_drop(&rc->version_carriers);
}
static jz_err ser_current_rows_receipt(jz_pc_w *w, const jazz_protocol_CurrentRowsReceipt *rc) {
    ser_request_id(w, &rc->request_id);
    ser_row_coords(w, &rc->rows);
    jz_pc_seq(w, rc->outcomes.len);
    for (u32 i = 0; i < rc->outcomes.len; i++) jz_pc_variant(w, rc->outcomes.ptr[i]);
    ser_policy_binding_key(w, &rc->context);
    jz_pc_uuid(w, &rc->core);
    jz_pc_varint(w, rc->core_epoch);
    jz_pc_varint(w, rc->claims_revision);
    jz_pc_varint(w, rc->policy_epoch);
    jz_pc_varint(w, rc->settled_through);
    jz_pc_varint(w, rc->authorization_progress);
    return ser_carriers(w, &rc->version_carriers);
}
static jz_err de_current_rows_receipt(jz_pc_r *r, jazz_protocol_CurrentRowsReceipt *rc) {
    jz_err e;
    memset(rc, 0, sizeof *rc);
    jz_btree_init(&rc->context.canonical_claims.claims, &jazz_model__btree_desc_str_value);
    TRYF(de_request_id(r, &rc->request_id));
    TRYF(de_row_coords(r, &rc->rows));
    u32 n;
    TRYF(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) { u32 o; TRYF(de_enum(r, &o, 3)); u8 b = (u8)o; jz_vec_push(&rc->outcomes, &b, 1); }
    jz_btree_drop(&rc->context.canonical_claims.claims);
    TRYF(de_policy_binding_key(r, &rc->context));
    TRYF(jz_pc_r_uuid(r, &rc->core));
    TRYF(jz_pc_r_varint(r, &rc->core_epoch));
    TRYF(jz_pc_r_varint(r, &rc->claims_revision));
    TRYF(jz_pc_r_varint(r, &rc->policy_epoch));
    TRYF(jazz_types__time__GlobalTime__Deserialize__deserialize(r, &rc->settled_through));
    TRYF(jz_pc_r_varint(r, &rc->authorization_progress));
    TRYF(de_carriers(r, &rc->version_carriers));
    return JZ_OK;
fail:
    CurrentRowsReceipt_drop(rc);
    return e;
}

/* ---- SyncMessage ---- */
jz_err jazz_protocol__protocol__SyncMessage__Serialize__serialize(jz_pc_w *w, const jazz_protocol_SyncMessage *m) {
    jz_pc_variant(w, m->tag);
    switch (m->tag) {
    case JAZZ_SM_CHUNK_REQUEST_BATCH: ser_chunk_request_batch(w, &m->u.chunk_request_batch); break;
    case JAZZ_SM_CHUNK_RESPONSE_BATCH: ser_chunk_response_batch(w, &m->u.chunk_response_batch); break;
    case JAZZ_SM_SESSION_CLAIMS: ser_author(w, &m->u.session_claims.identity); ser_value_map(w, &m->u.session_claims.claims); break;
    case JAZZ_SM_COMMIT_UNIT:
        jazz_model__tx__Transaction__Serialize__serialize(w, &m->u.commit_unit.tx);
        JZ_TRY(ser_versions(w, m->u.commit_unit.versions.ptr, m->u.commit_unit.versions.len));
        break;
    case JAZZ_SM_FATE_UPDATE:
        jazz_model__tx__TxId__Serialize__serialize(w, &m->u.fate_update.tx_id);
        jazz_model__tx__Fate__Serialize__serialize(w, &m->u.fate_update.fate);
        ser_opt_gt(w, m->u.fate_update.has_global_time, m->u.fate_update.global_time);
        jz_pc_option(w, m->u.fate_update.has_durability);
        if (m->u.fate_update.has_durability) jz_pc_variant(w, m->u.fate_update.durability);
        break;
    case JAZZ_SM_REGISTER_SHAPE:
        jz_pc_uuid(w, &m->u.register_shape.shape_id.u);
        jazz_protocol__protocol__ShapeAst__Serialize__serialize(w, &m->u.register_shape.ast);
        jazz_protocol__protocol__RegisterShapeOptions__Serialize__serialize(w, &m->u.register_shape.opts);
        break;
    case JAZZ_SM_SUBSCRIBE: ser_subscribe(w, &m->u.subscribe); break;
    case JAZZ_SM_SUBSCRIBE_REJECTED:
        jazz_protocol__protocol__SubscriptionKey__Serialize__serialize(w, &m->u.subscribe_rejected.subscription);
        ser_reject_reason(w, &m->u.subscribe_rejected.reason);
        break;
    case JAZZ_SM_UNSUBSCRIBE: jazz_protocol__protocol__SubscriptionKey__Serialize__serialize(w, &m->u.unsubscribe.subscription); break;
    case JAZZ_SM_PUBLISH_SCHEMA:
        ser_author(w, &m->u.publish_schema.author);
        JZ_TRY(jazz_protocol__protocol__SchemaVersion__Serialize__serialize(w, m->u.publish_schema.schema));
        break;
    case JAZZ_SM_PUBLISH_SCHEMA_WITH_LENS:
        ser_author(w, &m->u.publish_schema_with_lens.author);
        jz_pc_varint(w, m->u.publish_schema_with_lens.catalogue_seq);
        JZ_TRY(jazz_protocol__protocol__SchemaLineagePublication__Serialize__serialize(w, m->u.publish_schema_with_lens.publication));
        break;
    case JAZZ_SM_PUBLISH_LENS:
        ser_author(w, &m->u.publish_lens.author);
        jazz_protocol__protocol__MigrationLens__Serialize__serialize(w, &m->u.publish_lens.lens);
        break;
    case JAZZ_SM_CATALOGUE_ACK: {
        const jazz_protocol_CatalogueAck *a = &m->u.catalogue_ack;
        ser_opt_u64(w, a->has_revision, a->revision);
        ser_opt_uuid(w, a->has_schema, &a->schema);
        ser_opt_uuid(w, a->has_lens, &a->lens);
        jz_pc_bool(w, a->applied);
        break;
    }
    case JAZZ_SM_VIEW_UPDATE: case JAZZ_SM_VIEW_UPDATE_PART:
        JZ_TRY(jazz_protocol__protocol__ViewUpdatePayload__Serialize__serialize(w, &m->u.view_update));
        break;
    case JAZZ_SM_RESERVED14: case JAZZ_SM_RESERVED15: case JAZZ_SM_RESERVED29: JZ_UNREACHABLE();
    case JAZZ_SM_CATALOGUE_SNAPSHOT: JZ_TRY(ser_catalogue_snapshot(w, m->u.catalogue_snapshot)); break;
    case JAZZ_SM_AUTH_SCOPE_INTENT:
        ser_request_id(w, &m->u.auth_scope_intent.request_id);
        ser_action(w, &m->u.auth_scope_intent.action);
        ser_delegated(w, m->u.auth_scope_intent.has_delegated_session, &m->u.auth_scope_intent.delegated_session);
        break;
    case JAZZ_SM_AUTH_SCOPE_VIEW:
        ser_request_id(w, &m->u.auth_scope_view.request_id);
        jazz_protocol__protocol__AuthorizationSupportScopeKey__Serialize__serialize(w, &m->u.auth_scope_view.key);
        jz_pc_varint(w, m->u.auth_scope_view.clause_index);
        jz_pc_varint(w, m->u.auth_scope_view.clause_count);
        JZ_TRY(jazz_protocol__protocol__ViewUpdatePayload__Serialize__serialize(w, &m->u.auth_scope_view.view));
        break;
    case JAZZ_SM_AUTH_SCOPE_AGGREGATE_RECEIPT:
        ser_request_id(w, &m->u.auth_scope_receipt.request_id);
        ser_scope_receipt(w, &m->u.auth_scope_receipt.receipt);
        break;
    case JAZZ_SM_AUTH_SCOPE_UNAVAILABLE: case JAZZ_SM_CURRENT_ROWS_CANCEL: ser_request_id(w, &m->u.request_only.request_id); break;
    case JAZZ_SM_AUTH_SCOPE_DECISION:
        ser_request_id(w, &m->u.auth_scope_decision.request_id);
        jz_pc_variant(w, m->u.auth_scope_decision.advice);
        break;
    case JAZZ_SM_CHUNK_UPLOAD_START: groove__large_values__LargeValueRef__Serialize__serialize(w, m->u.chunk_upload_start.value_ref); break;
    case JAZZ_SM_CHUNK_UPLOAD_NODES:
        groove__large_values__LargeValueRef__Serialize__serialize(w, m->u.chunk_upload_nodes.value_ref);
        jz_pc_seq(w, m->u.chunk_upload_nodes.chunks.len);
        for (u32 i = 0; i < m->u.chunk_upload_nodes.chunks.len; i++)
            groove__large_values__StagedChunk__Serialize__serialize(w, &m->u.chunk_upload_nodes.chunks.ptr[i]);
        break;
    case JAZZ_SM_CHUNK_UPLOAD_RESULT: {
        const jazz_protocol_ChunkUploadResult *u = &m->u.chunk_upload_result;
        groove__large_values__LargeValueRef__Serialize__serialize(w, u->value_ref);
        jz_pc_variant(w, u->status.tag);
        if (u->status.tag == JAZZ_UPLOAD_NEED) {
            jz_pc_seq(w, u->status.need.len);
            for (u32 i = 0; i < u->status.need.len; i++) groove__large_values__NodeRef__Serialize__serialize(w, &u->status.need.ptr[i]);
        }
        break;
    }
    case JAZZ_SM_CURRENT_ROWS_REQUEST:
        ser_request_id(w, &m->u.current_rows_request.request_id);
        ser_row_coords(w, &m->u.current_rows_request.rows);
        ser_delegated(w, m->u.current_rows_request.has_delegated_session, &m->u.current_rows_request.delegated_session);
        break;
    case JAZZ_SM_CURRENT_ROWS_RECEIPT: JZ_TRY(ser_current_rows_receipt(w, &m->u.current_rows_receipt)); break;
    case JAZZ_SM_READ_BARRIER: jz_pc_varint(w, m->u.read_barrier.writes_through); break;
    default: JZ_UNREACHABLE();
    }
    return JZ_OK;
}

void jazz_protocol__protocol__SyncMessage__drop(jazz_protocol_SyncMessage *m) {
    switch (m->tag) {
    case JAZZ_SM_CHUNK_REQUEST_BATCH: jz_vec_drop(&m->u.chunk_request_batch.requests, sizeof(jazz_protocol_ChunkRequestEntry), NULL); break;
    case JAZZ_SM_CHUNK_RESPONSE_BATCH: jz_vec_drop(&m->u.chunk_response_batch.responses, sizeof(jazz_protocol_ChunkResponseEntry), ChunkResponseEntry_drop_v); break;
    case JAZZ_SM_SESSION_CLAIMS: jz_btree_drop(&m->u.session_claims.claims); break;
    case JAZZ_SM_COMMIT_UNIT: jazz_model__tx__Transaction__drop(&m->u.commit_unit.tx); version_vec_drop(&m->u.commit_unit.versions); break;
    case JAZZ_SM_FATE_UPDATE: jazz_model__tx__Fate__drop(&m->u.fate_update.fate); break;
    case JAZZ_SM_REGISTER_SHAPE:
        jazz_protocol__protocol__ShapeAst__drop(&m->u.register_shape.ast);
        jazz_protocol__protocol__RegisterShapeOptions__drop(&m->u.register_shape.opts);
        break;
    case JAZZ_SM_SUBSCRIBE: Subscribe_drop(&m->u.subscribe); break;
    case JAZZ_SM_SUBSCRIBE_REJECTED: jz_str_drop(&m->u.subscribe_rejected.reason.text); break;
    case JAZZ_SM_PUBLISH_SCHEMA: jazz_protocol__protocol__SchemaVersion__drop(m->u.publish_schema.schema); jz_free(m->u.publish_schema.schema); break;
    case JAZZ_SM_PUBLISH_SCHEMA_WITH_LENS:
        jazz_protocol__protocol__SchemaLineagePublication__drop(m->u.publish_schema_with_lens.publication);
        jz_free(m->u.publish_schema_with_lens.publication);
        break;
    case JAZZ_SM_PUBLISH_LENS: jazz_protocol__protocol__MigrationLens__drop(&m->u.publish_lens.lens); break;
    case JAZZ_SM_VIEW_UPDATE: case JAZZ_SM_VIEW_UPDATE_PART: jazz_protocol__protocol__ViewUpdatePayload__drop(&m->u.view_update); break;
    case JAZZ_SM_CATALOGUE_SNAPSHOT: CatalogueSnapshot_drop(m->u.catalogue_snapshot); jz_free(m->u.catalogue_snapshot); break;
    case JAZZ_SM_AUTH_SCOPE_INTENT:
        PermissionAdviceAction_drop(&m->u.auth_scope_intent.action);
        jz_btree_drop(&m->u.auth_scope_intent.delegated_session.claims);
        break;
    case JAZZ_SM_AUTH_SCOPE_VIEW: jazz_protocol__protocol__ViewUpdatePayload__drop(&m->u.auth_scope_view.view); break;
    case JAZZ_SM_CHUNK_UPLOAD_START: groove__large_values__LargeValueRef__drop(m->u.chunk_upload_start.value_ref); break;
    case JAZZ_SM_CHUNK_UPLOAD_NODES:
        groove__large_values__LargeValueRef__drop(m->u.chunk_upload_nodes.value_ref);
        jz_vec_drop(&m->u.chunk_upload_nodes.chunks, sizeof(groove_StagedChunk), StagedChunk_drop_v);
        break;
    case JAZZ_SM_CHUNK_UPLOAD_RESULT:
        groove__large_values__LargeValueRef__drop(m->u.chunk_upload_result.value_ref);
        jz_vec_drop(&m->u.chunk_upload_result.status.need, sizeof(groove_NodeRef), NULL);
        break;
    case JAZZ_SM_CURRENT_ROWS_REQUEST:
        jz_vec_drop(&m->u.current_rows_request.rows, sizeof(jazz_protocol_CurrentRowCoordinate), CurrentRowCoordinate_drop_v);
        jz_btree_drop(&m->u.current_rows_request.delegated_session.claims);
        break;
    case JAZZ_SM_CURRENT_ROWS_RECEIPT: CurrentRowsReceipt_drop(&m->u.current_rows_receipt); break;
    default: break;
    }
}
void jazz_protocol__protocol__SyncMessage__drop_v(void *m) { jazz_protocol__protocol__SyncMessage__drop(m); }

void jazz_protocol__protocol__SyncMessage__Clone__clone(jazz_protocol_SyncMessage *d, const jazz_protocol_SyncMessage *s) {
    memset(d, 0, sizeof *d);
    d->tag = s->tag;
    switch (s->tag) {
    case JAZZ_SM_CHUNK_REQUEST_BATCH: jz_vec_clone(&d->u.chunk_request_batch.requests, &s->u.chunk_request_batch.requests, sizeof(jazz_protocol_ChunkRequestEntry), NULL); break;
    case JAZZ_SM_CHUNK_RESPONSE_BATCH: jz_vec_clone(&d->u.chunk_response_batch.responses, &s->u.chunk_response_batch.responses, sizeof(jazz_protocol_ChunkResponseEntry), ChunkResponseEntry_clone_v); break;
    case JAZZ_SM_SESSION_CLAIMS:
        d->u.session_claims.identity = s->u.session_claims.identity;
        value_map_clone(&d->u.session_claims.claims, &s->u.session_claims.claims);
        break;
    case JAZZ_SM_COMMIT_UNIT:
        jazz_model__tx__Transaction__Clone__clone(&d->u.commit_unit.tx, &s->u.commit_unit.tx);
        version_vec_clone(&d->u.commit_unit.versions, &s->u.commit_unit.versions);
        break;
    case JAZZ_SM_FATE_UPDATE:
        d->u.fate_update = s->u.fate_update;
        jazz_model__tx__Fate__Clone__clone(&d->u.fate_update.fate, &s->u.fate_update.fate);
        break;
    case JAZZ_SM_REGISTER_SHAPE:
        d->u.register_shape.shape_id = s->u.register_shape.shape_id;
        ShapeAst_clone(&d->u.register_shape.ast, &s->u.register_shape.ast);
        jazz_protocol__protocol__RegisterShapeOptions__Clone__clone(&d->u.register_shape.opts, &s->u.register_shape.opts);
        break;
    case JAZZ_SM_SUBSCRIBE: {
        const jazz_protocol_Subscribe *x = &s->u.subscribe;
        jazz_protocol_Subscribe *y = &d->u.subscribe;
        *y = *x;
        jz_vec_clone(&y->values, &x->values, sizeof(groove_Value), value_clone_v);
        DelegatedSession_clone(&y->delegated_session, &x->delegated_session);
        break;
    }
    case JAZZ_SM_SUBSCRIBE_REJECTED:
        d->u.subscribe_rejected = s->u.subscribe_rejected;
        d->u.subscribe_rejected.reason.text = jz_str_clone(&s->u.subscribe_rejected.reason.text);
        break;
    case JAZZ_SM_UNSUBSCRIBE: d->u.unsubscribe = s->u.unsubscribe; break;
    case JAZZ_SM_PUBLISH_SCHEMA:
        d->u.publish_schema.author = s->u.publish_schema.author;
        d->u.publish_schema.schema = jz_alloc(sizeof(jazz_protocol_SchemaVersion));
        SchemaVersion_clone(d->u.publish_schema.schema, s->u.publish_schema.schema);
        break;
    case JAZZ_SM_PUBLISH_SCHEMA_WITH_LENS:
        d->u.publish_schema_with_lens.author = s->u.publish_schema_with_lens.author;
        d->u.publish_schema_with_lens.catalogue_seq = s->u.publish_schema_with_lens.catalogue_seq;
        d->u.publish_schema_with_lens.publication = jz_alloc(sizeof(jazz_protocol_SchemaLineagePublication));
        jazz_protocol__protocol__SchemaLineagePublication__Clone__clone(d->u.publish_schema_with_lens.publication, s->u.publish_schema_with_lens.publication);
        break;
    case JAZZ_SM_PUBLISH_LENS:
        d->u.publish_lens.author = s->u.publish_lens.author;
        jazz_protocol__protocol__MigrationLens__Clone__clone(&d->u.publish_lens.lens, &s->u.publish_lens.lens);
        break;
    case JAZZ_SM_CATALOGUE_ACK: d->u.catalogue_ack = s->u.catalogue_ack; break;
    case JAZZ_SM_VIEW_UPDATE: case JAZZ_SM_VIEW_UPDATE_PART:
        jazz_protocol__protocol__ViewUpdatePayload__Clone__clone(&d->u.view_update, &s->u.view_update);
        break;
    case JAZZ_SM_RESERVED14: case JAZZ_SM_RESERVED15: case JAZZ_SM_RESERVED29: JZ_UNREACHABLE();
    case JAZZ_SM_CATALOGUE_SNAPSHOT:
        d->u.catalogue_snapshot = jz_alloc(sizeof(jazz_protocol_CatalogueSnapshot));
        CatalogueSnapshot_clone(d->u.catalogue_snapshot, s->u.catalogue_snapshot);
        break;
    case JAZZ_SM_AUTH_SCOPE_INTENT:
        d->u.auth_scope_intent.request_id = s->u.auth_scope_intent.request_id;
        jazz_protocol__protocol__PermissionAdviceAction__Clone__clone(&d->u.auth_scope_intent.action, &s->u.auth_scope_intent.action);
        d->u.auth_scope_intent.has_delegated_session = s->u.auth_scope_intent.has_delegated_session;
        DelegatedSession_clone(&d->u.auth_scope_intent.delegated_session, &s->u.auth_scope_intent.delegated_session);
        break;
    case JAZZ_SM_AUTH_SCOPE_VIEW:
        d->u.auth_scope_view = s->u.auth_scope_view;
        jazz_protocol__protocol__ViewUpdatePayload__Clone__clone(&d->u.auth_scope_view.view, &s->u.auth_scope_view.view);
        break;
    case JAZZ_SM_AUTH_SCOPE_AGGREGATE_RECEIPT: d->u.auth_scope_receipt = s->u.auth_scope_receipt; break;
    case JAZZ_SM_AUTH_SCOPE_UNAVAILABLE: case JAZZ_SM_CURRENT_ROWS_CANCEL: d->u.request_only = s->u.request_only; break;
    case JAZZ_SM_AUTH_SCOPE_DECISION: d->u.auth_scope_decision = s->u.auth_scope_decision; break;
    case JAZZ_SM_CHUNK_UPLOAD_START: d->u.chunk_upload_start.value_ref = groove__large_values__LargeValueRef__clone(s->u.chunk_upload_start.value_ref); break;
    case JAZZ_SM_CHUNK_UPLOAD_NODES:
        d->u.chunk_upload_nodes.value_ref = groove__large_values__LargeValueRef__clone(s->u.chunk_upload_nodes.value_ref);
        jz_vec_clone(&d->u.chunk_upload_nodes.chunks, &s->u.chunk_upload_nodes.chunks, sizeof(groove_StagedChunk), StagedChunk_clone_v);
        break;
    case JAZZ_SM_CHUNK_UPLOAD_RESULT:
        d->u.chunk_upload_result.value_ref = groove__large_values__LargeValueRef__clone(s->u.chunk_upload_result.value_ref);
        d->u.chunk_upload_result.status.tag = s->u.chunk_upload_result.status.tag;
        jz_vec_clone(&d->u.chunk_upload_result.status.need, &s->u.chunk_upload_result.status.need, sizeof(groove_NodeRef), NULL);
        break;
    case JAZZ_SM_CURRENT_ROWS_REQUEST:
        d->u.current_rows_request.request_id = s->u.current_rows_request.request_id;
        jz_vec_clone(&d->u.current_rows_request.rows, &s->u.current_rows_request.rows, sizeof(jazz_protocol_CurrentRowCoordinate), CurrentRowCoordinate_clone_v);
        d->u.current_rows_request.has_delegated_session = s->u.current_rows_request.has_delegated_session;
        DelegatedSession_clone(&d->u.current_rows_request.delegated_session, &s->u.current_rows_request.delegated_session);
        break;
    case JAZZ_SM_CURRENT_ROWS_RECEIPT: {
        const jazz_protocol_CurrentRowsReceipt *x = &s->u.current_rows_receipt;
        jazz_protocol_CurrentRowsReceipt *y = &d->u.current_rows_receipt;
        *y = *x;
        jz_vec_clone(&y->rows, &x->rows, sizeof(jazz_protocol_CurrentRowCoordinate), CurrentRowCoordinate_clone_v);
        jz_vec_clone(&y->outcomes, &x->outcomes, 1, NULL);
        jazz_protocol__protocol__PolicyBindingKey__Clone__clone(&y->context, &x->context);
        carriers_clone(&y->version_carriers, &x->version_carriers);
        break;
    }
    case JAZZ_SM_READ_BARRIER: d->u.read_barrier = s->u.read_barrier; break;
    }
}

jz_err jazz_protocol__protocol__SyncMessage__Deserialize__deserialize(jz_pc_r *r, jazz_protocol_SyncMessage *m) {
    jz_err e;
    memset(m, 0, sizeof *m);
    JZ_TRY(jz_pc_r_enter(r));
    TRYF(jz_pc_r_variant(r, &m->tag, JAZZ_SM__COUNT));
    switch (m->tag) {
    case JAZZ_SM_CHUNK_REQUEST_BATCH: TRYF(jazz_protocol__protocol__deserialize_chunk_requests(r, &m->u.chunk_request_batch)); break;
    case JAZZ_SM_CHUNK_RESPONSE_BATCH: TRYF(de_chunk_response_batch(r, &m->u.chunk_response_batch)); break;
    case JAZZ_SM_SESSION_CLAIMS:
        jz_btree_init(&m->u.session_claims.claims, &jazz_model__btree_desc_str_value);
        TRYF(de_author(r, &m->u.session_claims.identity));
        jz_btree_drop(&m->u.session_claims.claims);
        TRYF(de_value_map(r, &m->u.session_claims.claims));
        break;
    case JAZZ_SM_COMMIT_UNIT:
        TRYF(jazz_model__tx__Transaction__Deserialize__deserialize(r, &m->u.commit_unit.tx));
        TRYF(de_versions(r, &m->u.commit_unit.versions));
        break;
    case JAZZ_SM_FATE_UPDATE:
        TRYF(jazz_model__tx__TxId__Deserialize__deserialize(r, &m->u.fate_update.tx_id));
        TRYF(jazz_model__tx__Fate__Deserialize__deserialize(r, &m->u.fate_update.fate));
        TRYF(de_opt_gt(r, &m->u.fate_update.has_global_time, &m->u.fate_update.global_time));
        TRYF(jz_pc_r_option(r, &m->u.fate_update.has_durability));
        if (m->u.fate_update.has_durability) TRYF(de_enum(r, &m->u.fate_update.durability, 3));
        break;
    case JAZZ_SM_REGISTER_SHAPE:
        TRYF(jz_pc_r_uuid(r, &m->u.register_shape.shape_id.u));
        TRYF(de_shape_ast(r, &m->u.register_shape.ast));
        TRYF(de_register_shape_options(r, &m->u.register_shape.opts));
        break;
    case JAZZ_SM_SUBSCRIBE: TRYF(de_subscribe(r, &m->u.subscribe)); break;
    case JAZZ_SM_SUBSCRIBE_REJECTED:
        TRYF(de_subscription_key(r, &m->u.subscribe_rejected.subscription));
        TRYF(de_reject_reason(r, &m->u.subscribe_rejected.reason));
        break;
    case JAZZ_SM_UNSUBSCRIBE: TRYF(de_subscription_key(r, &m->u.unsubscribe.subscription)); break;
    case JAZZ_SM_PUBLISH_SCHEMA: {
        TRYF(de_author(r, &m->u.publish_schema.author));
        jazz_protocol_SchemaVersion *sv = jz_alloc(sizeof *sv);
        if ((e = jazz_protocol__protocol__SchemaVersion__Deserialize__deserialize(r, sv)).code) { jz_free(sv); m->tag = JAZZ_SM_READ_BARRIER; goto fail; }
        m->u.publish_schema.schema = sv;
        break;
    }
    case JAZZ_SM_PUBLISH_SCHEMA_WITH_LENS: {
        TRYF(de_author(r, &m->u.publish_schema_with_lens.author));
        if ((e = jz_pc_r_varint(r, &m->u.publish_schema_with_lens.catalogue_seq)).code) { m->tag = JAZZ_SM_READ_BARRIER; goto fail; }
        jazz_protocol_SchemaLineagePublication *p = jz_alloc(sizeof *p);
        if ((e = jazz_protocol__protocol__SchemaLineagePublication__Deserialize__deserialize(r, p)).code) { jz_free(p); m->tag = JAZZ_SM_READ_BARRIER; goto fail; }
        m->u.publish_schema_with_lens.publication = p;
        break;
    }
    case JAZZ_SM_PUBLISH_LENS:
        TRYF(de_author(r, &m->u.publish_lens.author));
        TRYF(jazz_protocol__protocol__MigrationLens__Deserialize__deserialize(r, &m->u.publish_lens.lens));
        break;
    case JAZZ_SM_CATALOGUE_ACK: {
        jazz_protocol_CatalogueAck *a = &m->u.catalogue_ack;
        TRYF(de_opt_u64(r, &a->has_revision, &a->revision));
        TRYF(de_opt_uuid(r, &a->has_schema, &a->schema));
        TRYF(de_opt_uuid(r, &a->has_lens, &a->lens));
        TRYF(jz_pc_r_bool(r, &a->applied));
        break;
    }
    case JAZZ_SM_VIEW_UPDATE: case JAZZ_SM_VIEW_UPDATE_PART: TRYF(de_view_update(r, &m->u.view_update)); break;
    case JAZZ_SM_RESERVED14: case JAZZ_SM_RESERVED15: case JAZZ_SM_RESERVED29: {
        u32 none;
        m->tag = JAZZ_SM_READ_BARRIER;
        TRYF(jz_pc_r_variant(r, &none, 0)); /* ReservedWireMessage has no variants */
        break;
    }
    case JAZZ_SM_CATALOGUE_SNAPSHOT: {
        jazz_protocol_CatalogueSnapshot *s = jz_alloc(sizeof *s);
        if ((e = de_catalogue_snapshot(r, s)).code) { jz_free(s); m->tag = JAZZ_SM_READ_BARRIER; goto fail; }
        m->u.catalogue_snapshot = s;
        break;
    }
    case JAZZ_SM_AUTH_SCOPE_INTENT:
        jz_btree_init(&m->u.auth_scope_intent.delegated_session.claims, &jazz_model__btree_desc_str_value);
        TRYF(de_request_id(r, &m->u.auth_scope_intent.request_id));
        TRYF(de_action(r, &m->u.auth_scope_intent.action));
        TRYF(de_delegated(r, &m->u.auth_scope_intent.has_delegated_session, &m->u.auth_scope_intent.delegated_session));
        break;
    case JAZZ_SM_AUTH_SCOPE_VIEW:
        TRYF(de_request_id(r, &m->u.auth_scope_view.request_id));
        TRYF(de_scope_key(r, &m->u.auth_scope_view.key));
        TRYF(de_u16(r, &m->u.auth_scope_view.clause_index));
        TRYF(de_u16(r, &m->u.auth_scope_view.clause_count));
        TRYF(de_view_update(r, &m->u.auth_scope_view.view));
        break;
    case JAZZ_SM_AUTH_SCOPE_AGGREGATE_RECEIPT:
        TRYF(de_request_id(r, &m->u.auth_scope_receipt.request_id));
        TRYF(de_scope_receipt(r, &m->u.auth_scope_receipt.receipt));
        break;
    case JAZZ_SM_AUTH_SCOPE_UNAVAILABLE: case JAZZ_SM_CURRENT_ROWS_CANCEL: TRYF(de_request_id(r, &m->u.request_only.request_id)); break;
    case JAZZ_SM_AUTH_SCOPE_DECISION:
        TRYF(de_request_id(r, &m->u.auth_scope_decision.request_id));
        TRYF(de_enum(r, &m->u.auth_scope_decision.advice, 3));
        break;
    case JAZZ_SM_CHUNK_UPLOAD_START:
        if ((e = groove__large_values__LargeValueRef__Deserialize__deserialize(r, &m->u.chunk_upload_start.value_ref)).code) { m->tag = JAZZ_SM_READ_BARRIER; goto fail; }
        break;
    case JAZZ_SM_CHUNK_UPLOAD_NODES: {
        if ((e = groove__large_values__LargeValueRef__Deserialize__deserialize(r, &m->u.chunk_upload_nodes.value_ref)).code) { m->tag = JAZZ_SM_READ_BARRIER; goto fail; }
        u32 n;
        TRYF(jz_pc_r_seq(r, &n));
        for (u32 i = 0; i < n; i++) {
            groove_StagedChunk c;
            TRYF(groove__large_values__StagedChunk__Deserialize__deserialize(r, &c));
            jz_vec_push(&m->u.chunk_upload_nodes.chunks, &c, sizeof c);
        }
        break;
    }
    case JAZZ_SM_CHUNK_UPLOAD_RESULT: {
        jazz_protocol_ChunkUploadResult *u = &m->u.chunk_upload_result;
        if ((e = groove__large_values__LargeValueRef__Deserialize__deserialize(r, &u->value_ref)).code) { m->tag = JAZZ_SM_READ_BARRIER; goto fail; }
        TRYF(jz_pc_r_variant(r, &u->status.tag, 4));
        if (u->status.tag == JAZZ_UPLOAD_NEED) {
            u32 n;
            TRYF(jz_pc_r_seq(r, &n));
            for (u32 i = 0; i < n; i++) {
                groove_NodeRef node;
                TRYF(groove__large_values__NodeRef__Deserialize__deserialize(r, &node));
                jz_vec_push(&u->status.need, &node, sizeof node);
            }
        }
        break;
    }
    case JAZZ_SM_CURRENT_ROWS_REQUEST:
        jz_btree_init(&m->u.current_rows_request.delegated_session.claims, &jazz_model__btree_desc_str_value);
        TRYF(de_request_id(r, &m->u.current_rows_request.request_id));
        TRYF(de_row_coords(r, &m->u.current_rows_request.rows));
        TRYF(de_delegated(r, &m->u.current_rows_request.has_delegated_session, &m->u.current_rows_request.delegated_session));
        break;
    case JAZZ_SM_CURRENT_ROWS_RECEIPT:
        if ((e = de_current_rows_receipt(r, &m->u.current_rows_receipt)).code) { m->tag = JAZZ_SM_READ_BARRIER; goto fail; }
        break;
    case JAZZ_SM_READ_BARRIER: TRYF(jz_pc_r_varint(r, &m->u.read_barrier.writes_through)); break;
    }
    jz_pc_r_leave(r);
    return JZ_OK;
fail:
    jz_pc_r_leave(r);
    jazz_protocol__protocol__SyncMessage__drop(m);
    return e;
}

/* codec vtable for the single C copy of decode_postcard_exact / encodes_exactly */
static jz_err sync_message_decode_v(jz_pc_r *r, void *out) { return jazz_protocol__protocol__SyncMessage__Deserialize__deserialize(r, out); }
static void sync_message_encode_v(jz_pc_w *w, const void *v) {
    /* encodes_exactly only re-encodes values it just decoded; encoding them cannot fail */
    jz_err e = jazz_protocol__protocol__SyncMessage__Serialize__serialize(w, v);
    if (e.code) jz_err_drop(&e);
}
const jazz_types_PcCodec jazz_protocol__protocol__SYNC_MESSAGE_CODEC = { sync_message_decode_v, sync_message_encode_v, jazz_protocol__protocol__SyncMessage__drop_v };
