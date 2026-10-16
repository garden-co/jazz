/* model/branch.rs: branch coordinates and snapshot references. */
#include "jazz_model/jazz_model.h"

#define B(x) jazz_model__model__branch__##x
#define GV(x) groove__records__values__Value__##x
#define GVT(x) groove__records__values__ValueType__##x

#define BRANCH_COLUMN_CODEC_VERSION 1
#define BRANCH_COLUMN_ENUM_TAG 8
#define BRANCH_KEY_CODEC_VERSION 1
/* BRANCH_COLUMN_SCALARS: each type's codec tag is its index */
static const u8 BRANCH_COLUMN_SCALARS[8] = {
    GROOVE_VT_U8, GROOVE_VT_U16, GROOVE_VT_U32, GROOVE_VT_U64, GROOVE_VT_I32, GROOVE_VT_I64, GROOVE_VT_STRING, GROOVE_VT_UUID,
};

/* ---------------------------------------------------------------- BranchCodecError */
static jz_err invalid_envelope(void) { return jz_err_new(JAZZ_MODEL_ERR_BRANCH_CODEC_ENVELOPE, "invalid branch codec envelope"); }
static jz_err unsupported_type(void) { return jz_err_new(JAZZ_MODEL_ERR_BRANCH_CODEC_UNSUPPORTED, "unsupported branch column type"); }
static jz_err type_mismatch(void) {
    return jz_err_new(JAZZ_MODEL_ERR_BRANCH_CODEC_MISMATCH, "branch column encoding does not match its declared type");
}
static jz_err groove_error(jz_err e) {
    jz_str msg = {0};
    groove__records__Error__Display__fmt(&msg, &e);
    jz_err r = jz_err_fmt(JAZZ_MODEL_ERR_BRANCH_CODEC_GROOVE, "invalid Groove branch column payload: %.*s", (int)msg.len, msg.ptr);
    jz_str_drop(&msg);
    jz_err_drop(&e);
    return r;
}

void B(BranchCodecError__Display__fmt)(jz_str *out, const jz_err *e) {
    if (e->msg) jz_str_push(out, e->msg->ptr, e->msg->len);
}

void B(BranchCodecError__Debug__fmt)(jz_str *out, const jz_err *e) {
    switch (e->code) {
    case JAZZ_MODEL_ERR_BRANCH_CODEC_ENVELOPE: jz_str_push_cstr(out, "InvalidEnvelope"); break;
    case JAZZ_MODEL_ERR_BRANCH_CODEC_UNSUPPORTED: jz_str_push_cstr(out, "UnsupportedType"); break;
    case JAZZ_MODEL_ERR_BRANCH_CODEC_MISMATCH: jz_str_push_cstr(out, "TypeMismatch"); break;
    default:
        jz_str_push_cstr(out, "Groove(");
        if (e->msg) jz_str_push(out, e->msg->ptr, e->msg->len);
        jz_str_push_char(out, ')');
        break;
    }
}

/* ---------------------------------------------------------------- BranchColumnValue */
static bool jazz_model__model__branch__branch_column_tag(const groove_ValueType *t, u8 *out) {
    if (t->tag == GROOVE_VT_ENUM_TAG) { *out = BRANCH_COLUMN_ENUM_TAG; return true; }
    for (u8 i = 0; i < 8; i++)
        if (BRANCH_COLUMN_SCALARS[i] == t->tag) { *out = i; return true; }
    return false;
}

static bool jazz_model__model__branch__branch_column_type(u8 tag, groove_ValueType *out) {
    if (tag >= 8) return false;
    *out = GVT(scalar)(BRANCH_COLUMN_SCALARS[tag]);
    return true;
}

static groove_RecordDescriptor value_descriptor(const groove_ValueType *t) {
    jz_str name = jz_str_from("value");
    groove_ValueType vt;
    GVT(Clone__clone)(&vt, t);
    return groove__records__RecordDescriptor__new(&name, &vt, 1);
}

jz_err B(BranchColumnValue__encode_typed)(const groove_Value *value, const groove_ValueType *t, jazz_model_BranchColumnValue *out) {
    u8 tag;
    if (!jazz_model__model__branch__branch_column_tag(t, &tag)) return unsupported_type();
    groove_RecordDescriptor d = value_descriptor(t);
    jz_bytes payload = {0};
    jz_err e = groove__records__RecordDescriptor__create(d, value, 1, &payload);
    if (e.code) return groove_error(e);
    jz_bytes bytes = {0};
    jz_vec_reserve(&bytes, 2 + payload.len, 1);
    jz_bytes_push_u8(&bytes, BRANCH_COLUMN_CODEC_VERSION);
    jz_bytes_push_u8(&bytes, tag);
    jz_bytes_push(&bytes, payload.ptr, payload.len);
    jz_bytes_drop(&payload);
    out->bytes = bytes;
    return JZ_OK;
}

jazz_model_BranchColumnValue B(BranchColumnValue__From_Value__from)(groove_Value value) {
    jazz_model_BranchColumnValue out = {{0}};
    u8 vt;
    switch (value.tag) {
    case GROOVE_V_U8: vt = GROOVE_VT_U8; break;
    case GROOVE_V_U16: vt = GROOVE_VT_U16; break;
    case GROOVE_V_U32: vt = GROOVE_VT_U32; break;
    case GROOVE_V_U64: vt = GROOVE_VT_U64; break;
    case GROOVE_V_I32: vt = GROOVE_VT_I32; break;
    case GROOVE_V_I64: vt = GROOVE_VT_I64; break;
    case GROOVE_V_STRING: vt = GROOVE_VT_STRING; break;
    case GROOVE_V_UUID: vt = GROOVE_VT_UUID; break;
    case GROOVE_V_ENUM_TAG:
        jz_bytes_push_u8(&out.bytes, BRANCH_COLUMN_CODEC_VERSION);
        jz_bytes_push_u8(&out.bytes, BRANCH_COLUMN_ENUM_TAG);
        jz_bytes_push_u8(&out.bytes, value.u.enum_tag);
        return out;
    default:
        /* infallible constructor: schema projection rejects this unknown tag later */
        GV(drop)(&value);
        jz_bytes_push_u8(&out.bytes, BRANCH_COLUMN_CODEC_VERSION);
        jz_bytes_push_u8(&out.bytes, 0xff);
        return out;
    }
    groove_ValueType t = GVT(scalar)(vt);
    jz_err e = B(BranchColumnValue__encode_typed)(&value, &t, &out);
    GV(drop)(&value);
    if (e.code) jz_panic("inferred branch selector type accepts its value");
    return out;
}

static jz_err jazz_model__model__branch__BranchColumnValue__envelope(const jazz_model_BranchColumnValue *v, u8 *tag, jz_slice *payload) {
    if (v->bytes.len < 2 || v->bytes.ptr[0] != BRANCH_COLUMN_CODEC_VERSION) return invalid_envelope();
    *tag = v->bytes.ptr[1];
    *payload = jz_slice_of(v->bytes.ptr + 2, v->bytes.len - 2);
    return JZ_OK;
}

jz_err B(BranchColumnValue__decode)(const jazz_model_BranchColumnValue *v, groove_Value *out) {
    u8 tag;
    jz_slice payload;
    JZ_TRY(jazz_model__model__branch__BranchColumnValue__envelope(v, &tag, &payload));
    if (tag == BRANCH_COLUMN_ENUM_TAG) {
        if (payload.len != 1) return invalid_envelope();
        memset(out, 0, sizeof *out);
        out->tag = GROOVE_V_ENUM_TAG;
        out->u.enum_tag = payload.ptr[0];
        return JZ_OK;
    }
    groove_ValueType t;
    if (!jazz_model__model__branch__branch_column_type(tag, &t)) return invalid_envelope();
    return B(BranchColumnValue__decode_as)(v, &t, out);
}

jz_err B(BranchColumnValue__decode_as)(const jazz_model_BranchColumnValue *v, const groove_ValueType *t, groove_Value *out) {
    u8 tag, expected;
    jz_slice payload;
    JZ_TRY(jazz_model__model__branch__BranchColumnValue__envelope(v, &tag, &payload));
    if (!jazz_model__model__branch__branch_column_tag(t, &expected) || expected != tag) return type_mismatch();
    groove_RecordDescriptor d = value_descriptor(t);
    groove_Value value;
    jz_err e = groove__records__RecordDescriptor__get_idx(d, payload, 0, &value);
    if (e.code) return groove_error(e);
    jz_bytes again = {0};
    e = groove__records__RecordDescriptor__create(d, &value, 1, &again);
    if (e.code) { GV(drop)(&value); return groove_error(e); }
    bool same = again.len == payload.len && memcmp(again.ptr, payload.ptr, payload.len) == 0;
    jz_bytes_drop(&again);
    if (!same) { GV(drop)(&value); return invalid_envelope(); }
    *out = value;
    return JZ_OK;
}

void B(BranchColumnValue__Serialize__serialize)(jz_pc_w *w, const jazz_model_BranchColumnValue *v) {
    /* Vec<u8> through serde: a sequence of u8 */
    jz_pc_seq(w, v->bytes.len);
    jz_pc_raw(w, v->bytes.ptr, v->bytes.len);
}

jz_err B(BranchColumnValue__Deserialize__deserialize)(jz_pc_r *r, jazz_model_BranchColumnValue *out) {
    u32 n;
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        u8 b;
        jz_err e = jz_pc_r_u8(r, &b);
        if (e.code) { jz_bytes_drop(&out->bytes); return e; }
        jz_bytes_push_u8(&out->bytes, b);
    }
    return JZ_OK;
}

static void bcv_drop_v(void *v) { jz_bytes_drop(&((jazz_model_BranchColumnValue *)v)->bytes); }
static void bcv_clone_v(void *dst, const void *src) {
    ((jazz_model_BranchColumnValue *)dst)->bytes = jz_bytes_clone(&((const jazz_model_BranchColumnValue *)src)->bytes);
}
static bool bcv_eq_v(const void *a, const void *b) {
    return jz_bytes_eq(&((const jazz_model_BranchColumnValue *)a)->bytes, &((const jazz_model_BranchColumnValue *)b)->bytes);
}

/* ---------------------------------------------------------------- BranchSelector */
const jz_btree_desc jazz_model__model__branch__SELECTOR_VALUES_DESC = {
    sizeof(jz_str), sizeof(jazz_model_BranchColumnValue), jz_cmp_str, jazz_model__str_drop_v, bcv_drop_v,
};

void B(BranchSelector__new)(jazz_model_BranchSelector *s) { jz_btree_init(&s->values, &jazz_model__model__branch__SELECTOR_VALUES_DESC); }
void B(BranchSelector__drop)(jazz_model_BranchSelector *s) { jz_btree_drop(&s->values); }
void B(BranchSelector__Clone__clone)(jazz_model_BranchSelector *dst, const jazz_model_BranchSelector *src) {
    jz_btree_clone(&dst->values, &src->values, jazz_model__str_clone_v, bcv_clone_v);
}
bool B(BranchSelector__PartialEq__eq)(const jazz_model_BranchSelector *a, const jazz_model_BranchSelector *b) {
    return jz_btree_eq(&a->values, &b->values, bcv_eq_v);
}

int B(BranchSelector__Ord__cmp)(const jazz_model_BranchSelector *a, const jazz_model_BranchSelector *b) {
    jz_btree_iter ia, ib;
    jz_str *ka, *kb;
    jazz_model_BranchColumnValue *va, *vb;
    jz_btree_iter_begin(&ia, &a->values);
    jz_btree_iter_begin(&ib, &b->values);
    for (;;) {
        bool ha = jz_btree_next(&ia, (void **)&ka, (void **)&va);
        bool hb = jz_btree_next(&ib, (void **)&kb, (void **)&vb);
        if (!ha || !hb) return ha == hb ? 0 : ha ? 1 : -1;
        int c = jz_str_cmp(ka, kb);
        if (c) return c;
        c = jz_bytes_cmp(&va->bytes, &vb->bytes);
        if (c) return c;
    }
}

void B(BranchSelector__Serialize__serialize)(jz_pc_w *w, const jazz_model_BranchSelector *s) {
    jz_str *k;
    jazz_model_BranchColumnValue *v;
    jz_pc_seq(w, s->values.len);
    JZ_BTREE_FOR(&s->values, k, v) {
        jz_pc_str(w, k);
        B(BranchColumnValue__Serialize__serialize)(w, v);
    }
}

jz_err B(BranchSelector__Deserialize__deserialize)(jz_pc_r *r, jazz_model_BranchSelector *out) {
    u32 n;
    B(BranchSelector__new)(out);
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jz_str k;
        jazz_model_BranchColumnValue v;
        jz_err e = jz_pc_r_str(r, &k);
        if (!e.code) {
            e = B(BranchColumnValue__Deserialize__deserialize)(r, &v);
            if (e.code) jz_str_drop(&k);
        }
        if (e.code) { B(BranchSelector__drop)(out); return e; }
        jz_btree_insert(&out->values, &k, &v, NULL);
    }
    return JZ_OK;
}

static void bytes_debug(jz_str *out, const jz_bytes *b) {
    jz_str_push_cstr(out, "BranchColumnValue([");
    for (u32 i = 0; i < b->len; i++) jz_str_appendf(out, i ? ", %u" : "%u", b->ptr[i]);
    jz_str_push_cstr(out, "])");
}

void B(BranchSelector__Debug__fmt)(jz_str *out, const jazz_model_BranchSelector *s) {
    jz_str *k;
    jazz_model_BranchColumnValue *v;
    bool first = true;
    jz_str_push_cstr(out, "BranchSelector { values: {");
    JZ_BTREE_FOR(&s->values, k, v) {
        if (!first) jz_str_push_cstr(out, ", ");
        first = false;
        jazz_model__debug_str(out, k->ptr, k->len);
        jz_str_push_cstr(out, ": ");
        bytes_debug(out, &v->bytes);
    }
    jz_str_push_cstr(out, "} }");
}

/* ---------------------------------------------------------------- BranchKey */
static void entry_drop_v(void *e) {
    jazz_model_BranchKeyEntry *x = e;
    jz_str_drop(&x->name);
    jz_bytes_drop(&x->value.bytes);
}
static void entry_clone_v(void *dst, const void *src) {
    jazz_model_BranchKeyEntry *d = dst;
    const jazz_model_BranchKeyEntry *s = src;
    d->name = jz_str_clone(&s->name);
    d->value.bytes = jz_bytes_clone(&s->value.bytes);
}

void B(BranchKey__drop)(jazz_model_BranchKey *k) { jz_vec_drop(&k->values, sizeof(jazz_model_BranchKeyEntry), entry_drop_v); }
void B(BranchKey__drop_v)(void *k) { B(BranchKey__drop)((jazz_model_BranchKey *)k); }
void B(BranchKey__Clone__clone)(jazz_model_BranchKey *dst, const jazz_model_BranchKey *src) {
    jz_vec_clone(&dst->values, &src->values, sizeof(jazz_model_BranchKeyEntry), entry_clone_v);
}
void B(BranchKey__Clone__clone_v)(void *dst, const void *src) { B(BranchKey__Clone__clone)(dst, src); }

int B(BranchKey__Ord__cmp)(const void *pa, const void *pb, void *ctx) {
    (void)ctx;
    const jazz_model_BranchKey *a = pa, *b = pb;
    u32 n = a->values.len < b->values.len ? a->values.len : b->values.len;
    for (u32 i = 0; i < n; i++) {
        int c = jz_str_cmp(&a->values.ptr[i].name, &b->values.ptr[i].name);
        if (c) return c;
        c = jz_bytes_cmp(&a->values.ptr[i].value.bytes, &b->values.ptr[i].value.bytes);
        if (c) return c;
    }
    return a->values.len < b->values.len ? -1 : a->values.len > b->values.len ? 1 : 0;
}

bool B(BranchKey__PartialEq__eq)(const jazz_model_BranchKey *a, const jazz_model_BranchKey *b) {
    return B(BranchKey__Ord__cmp)(a, b, NULL) == 0;
}

u64 B(BranchKey__Hash__hash)(const void *pk) {
    const jazz_model_BranchKey *k = pk;
    u64 h = jz_hash_u64(k->values.len);
    for (u32 i = 0; i < k->values.len; i++) {
        h = jz_hash_combine(h, jz_str_hash(&k->values.ptr[i].name));
        h = jz_hash_combine(h, jz_hash_bytes(k->values.ptr[i].value.bytes.ptr, k->values.ptr[i].value.bytes.len));
    }
    return h;
}

void B(BranchKey__Debug__fmt)(jz_str *out, const jazz_model_BranchKey *k) {
    jz_str_push_cstr(out, "BranchKey { values: [");
    for (u32 i = 0; i < k->values.len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        jz_str_push_char(out, '(');
        jazz_model__debug_str(out, k->values.ptr[i].name.ptr, k->values.ptr[i].name.len);
        jz_str_push_cstr(out, ", ");
        bytes_debug(out, &k->values.ptr[i].value.bytes);
        jz_str_push_char(out, ')');
    }
    jz_str_push_cstr(out, "] }");
}

const jz_btree_desc jazz_model__model__branch__BRANCH_KEY_SET_DESC = {
    sizeof(jazz_model_BranchKey), 0, jazz_model__model__branch__BranchKey__Ord__cmp, jazz_model__model__branch__BranchKey__drop_v, NULL,
};

static void jazz_model__model__branch__put_branch_component_len(jz_bytes *bytes, u32 len) {
    u8 le[4] = {(u8)len, (u8)(len >> 8), (u8)(len >> 16), (u8)(len >> 24)};
    jz_bytes_push(bytes, le, 4);
}

static jz_err jazz_model__model__branch__take_branch_component(jz_slice *cursor, u32 len, jz_slice *out) {
    if (len > cursor->len) return invalid_envelope();
    *out = jz_slice_of(cursor->ptr, len);
    cursor->ptr += len;
    cursor->len -= len;
    return JZ_OK;
}

static jz_err jazz_model__model__branch__take_branch_component_len(jz_slice *cursor, u32 *out) {
    jz_slice enc;
    JZ_TRY(jazz_model__model__branch__take_branch_component(cursor, 4, &enc));
    *out = (u32)enc.ptr[0] | (u32)enc.ptr[1] << 8 | (u32)enc.ptr[2] << 16 | (u32)enc.ptr[3] << 24;
    return JZ_OK;
}

bool B(BranchKey__is_canonical)(const jazz_model_BranchKey *k) {
    for (u32 i = 1; i < k->values.len; i++)
        if (jz_str_cmp(&k->values.ptr[i - 1].name, &k->values.ptr[i].name) >= 0) return false;
    for (u32 i = 0; i < k->values.len; i++) {
        if (k->values.ptr[i].name.len == 0) return false;
        groove_Value v;
        jz_err e = B(BranchColumnValue__decode)(&k->values.ptr[i].value, &v);
        if (e.code) { jz_err_drop(&e); return false; }
        GV(drop)(&v);
    }
    return true;
}

jz_err B(BranchKey__try_canonical_bytes)(const jazz_model_BranchKey *k, jz_bytes *out) {
    if (!B(BranchKey__is_canonical)(k)) return invalid_envelope();
    jz_bytes bytes = {0};
    jz_bytes_push_u8(&bytes, BRANCH_KEY_CODEC_VERSION);
    jazz_model__model__branch__put_branch_component_len(&bytes, k->values.len);
    for (u32 i = 0; i < k->values.len; i++) {
        const jazz_model_BranchKeyEntry *e = &k->values.ptr[i];
        jazz_model__model__branch__put_branch_component_len(&bytes, e->name.len);
        jz_bytes_push(&bytes, (const u8 *)e->name.ptr, e->name.len);
        jazz_model__model__branch__put_branch_component_len(&bytes, e->value.bytes.len);
        jz_bytes_push(&bytes, e->value.bytes.ptr, e->value.bytes.len);
    }
    *out = bytes;
    return JZ_OK;
}

jz_bytes B(BranchKey__canonical_bytes)(const jazz_model_BranchKey *k) {
    jz_bytes out;
    jz_err e = B(BranchKey__try_canonical_bytes)(k, &out);
    if (e.code) jz_panic("branch keys must be canonical before serialization");
    return out;
}

jz_err B(BranchKey__from_canonical_bytes)(jz_slice bytes, jazz_model_BranchKey *out) {
    memset(out, 0, sizeof *out);
    if (bytes.len < 1 || bytes.ptr[0] != BRANCH_KEY_CODEC_VERSION) return invalid_envelope();
    jz_slice cursor = jz_slice_of(bytes.ptr + 1, bytes.len - 1);
    u32 count;
    JZ_TRY(jazz_model__model__branch__take_branch_component_len(&cursor, &count));
    if (count > cursor.len / 8) return invalid_envelope();
    jz_vec_reserve(&out->values, count, sizeof(jazz_model_BranchKeyEntry));
    jz_err e = JZ_OK;
    for (u32 i = 0; i < count; i++) {
        u32 name_len, value_len;
        jz_slice name, value;
        if ((e = jazz_model__model__branch__take_branch_component_len(&cursor, &name_len)).code) break;
        if ((e = jazz_model__model__branch__take_branch_component(&cursor, name_len, &name)).code) break;
        if (!jz_utf8_valid(name.ptr, name.len) || name.len == 0) { e = invalid_envelope(); break; }
        if ((e = jazz_model__model__branch__take_branch_component_len(&cursor, &value_len)).code) break;
        if ((e = jazz_model__model__branch__take_branch_component(&cursor, value_len, &value)).code) break;
        jazz_model_BranchKeyEntry entry;
        entry.name = jz_str_from_slice(name);
        entry.value.bytes = jz_bytes_from(value.ptr, value.len);
        groove_Value decoded;
        if ((e = B(BranchColumnValue__decode)(&entry.value, &decoded)).code) { entry_drop_v(&entry); break; }
        GV(drop)(&decoded);
        if (out->values.len && jz_str_cmp(&out->values.ptr[out->values.len - 1].name, &entry.name) >= 0) {
            entry_drop_v(&entry);
            e = invalid_envelope();
            break;
        }
        JZ_VEC_PUSH(out->values, entry);
    }
    if (!e.code && cursor.len != 0) e = invalid_envelope();
    if (e.code) B(BranchKey__drop)(out);
    return e;
}

void B(BranchKey__Serialize__serialize)(jz_pc_w *w, const jazz_model_BranchKey *k) {
    jz_pc_seq(w, k->values.len);
    for (u32 i = 0; i < k->values.len; i++) {
        jz_pc_str(w, &k->values.ptr[i].name);
        B(BranchColumnValue__Serialize__serialize)(w, &k->values.ptr[i].value);
    }
}

jz_err B(BranchKey__Deserialize__deserialize)(jz_pc_r *r, jazz_model_BranchKey *out) {
    u32 n;
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_model_BranchKeyEntry e;
        jz_err err = jz_pc_r_str(r, &e.name);
        if (!err.code) {
            err = B(BranchColumnValue__Deserialize__deserialize)(r, &e.value);
            if (err.code) jz_str_drop(&e.name);
        }
        if (err.code) { B(BranchKey__drop)(out); return err; }
        JZ_VEC_PUSH(out->values, e);
    }
    return JZ_OK;
}

/* ---------------------------------------------------------------- SnapshotRef */
void B(SnapshotRef__drop)(jazz_model_SnapshotRef *s) { jz_vec_drop(&s->dots, sizeof(jazz_model_TxId), NULL); }
void B(SnapshotRef__Clone__clone)(jazz_model_SnapshotRef *dst, const jazz_model_SnapshotRef *src) {
    *dst = *src;
    jz_vec_clone(&dst->dots, &src->dots, sizeof(jazz_model_TxId), NULL);
}
int B(SnapshotRef__Ord__cmp)(const jazz_model_SnapshotRef *a, const jazz_model_SnapshotRef *b) {
    int c = jz_uuid_cmp(&a->owner, &b->owner);
    if (c) return c;
    if (a->global_base != b->global_base) return a->global_base < b->global_base ? -1 : 1;
    if (a->local_base != b->local_base) return a->local_base < b->local_base ? -1 : 1;
    u32 n = a->dots.len < b->dots.len ? a->dots.len : b->dots.len;
    for (u32 i = 0; i < n; i++) {
        c = jazz_model__tx__TxId__Ord__cmp(&a->dots.ptr[i], &b->dots.ptr[i], NULL);
        if (c) return c;
    }
    return a->dots.len < b->dots.len ? -1 : a->dots.len > b->dots.len ? 1 : 0;
}
bool B(SnapshotRef__PartialEq__eq)(const jazz_model_SnapshotRef *a, const jazz_model_SnapshotRef *b) {
    return B(SnapshotRef__Ord__cmp)(a, b) == 0;
}

void B(SnapshotRef__Serialize__serialize)(jz_pc_w *w, const jazz_model_SnapshotRef *s) {
    jz_pc_uuid(w, &s->owner);
    jazz_types__time__GlobalTime__Serialize__serialize(w, s->global_base);
    jazz_types__time__TxTime__Serialize__serialize(w, s->local_base);
    jz_pc_seq(w, s->dots.len);
    for (u32 i = 0; i < s->dots.len; i++) jazz_model__tx__TxId__Serialize__serialize(w, &s->dots.ptr[i]);
}

jz_err B(SnapshotRef__Deserialize__deserialize)(jz_pc_r *r, jazz_model_SnapshotRef *out) {
    u32 n;
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_pc_r_uuid(r, &out->owner));
    JZ_TRY(jazz_types__time__GlobalTime__Deserialize__deserialize(r, &out->global_base));
    JZ_TRY(jazz_types__time__TxTime__Deserialize__deserialize(r, &out->local_base));
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_model_TxId id;
        jz_err e = jazz_model__tx__TxId__Deserialize__deserialize(r, &id);
        if (e.code) { B(SnapshotRef__drop)(out); return e; }
        JZ_VEC_PUSH(out->dots, id);
    }
    return JZ_OK;
}

/* ---------------------------------------------------------------- BranchViewBase */
void B(BranchViewBase__drop)(jazz_model_BranchViewBase *b) {
    B(BranchSelector__drop)(&b->branch);
    if (b->tag == JAZZ_BVB_SNAPSHOT) B(SnapshotRef__drop)(&b->snapshot);
}

void B(BranchViewBase__Clone__clone)(jazz_model_BranchViewBase *dst, const jazz_model_BranchViewBase *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    B(BranchSelector__Clone__clone)(&dst->branch, &src->branch);
    if (src->tag == JAZZ_BVB_SNAPSHOT) B(SnapshotRef__Clone__clone)(&dst->snapshot, &src->snapshot);
}

bool B(BranchViewBase__PartialEq__eq)(const jazz_model_BranchViewBase *a, const jazz_model_BranchViewBase *b) {
    if (a->tag != b->tag) return false;
    if (!B(BranchSelector__PartialEq__eq)(&a->branch, &b->branch)) return false;
    return a->tag == JAZZ_BVB_CURRENT || B(SnapshotRef__PartialEq__eq)(&a->snapshot, &b->snapshot);
}

void B(BranchViewBase__Serialize__serialize)(jz_pc_w *w, const jazz_model_BranchViewBase *b) {
    jz_pc_variant(w, b->tag);
    B(BranchSelector__Serialize__serialize)(w, &b->branch);
    if (b->tag == JAZZ_BVB_SNAPSHOT) B(SnapshotRef__Serialize__serialize)(w, &b->snapshot);
}

jz_err B(BranchViewBase__Deserialize__deserialize)(jz_pc_r *r, jazz_model_BranchViewBase *out) {
    u32 idx;
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_pc_r_variant(r, &idx, 2));
    out->tag = (u8)idx;
    JZ_TRY(B(BranchSelector__Deserialize__deserialize)(r, &out->branch));
    if (idx == JAZZ_BVB_SNAPSHOT) {
        jz_err e = B(SnapshotRef__Deserialize__deserialize)(r, &out->snapshot);
        if (e.code) { B(BranchSelector__drop)(&out->branch); return e; }
    }
    return JZ_OK;
}
