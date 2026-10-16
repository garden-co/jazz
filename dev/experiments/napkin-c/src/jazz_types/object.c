/* crates/jazz/layers/types/src/object.rs */
#include "jazz_types.h"

static jz_interner *jazz_types__object__UUID_POOL;
static jz_interner *jazz_types__object__BRANCH_NAME_POOL;

static u64 uuid_hash(const void *p) { return jz_hash_bytes(p, 16); }
static bool uuid_eq(const void *a, const void *b) { return memcmp(a, b, 16) == 0; }

/* ---------------- ObjectId(Intern<Uuid>) ---------------- */
jazz_types_ObjectId jazz_types__object__ObjectId__from_uuid(const jz_uuid *u) {
    return (jazz_types_ObjectId)jz_intern(&jazz_types__object__UUID_POOL, u, sizeof *u, uuid_hash, uuid_eq);
}

jazz_types_ObjectId jazz_types__object__ObjectId__new(void) {
    u8 rnd[10];
    jz_random_bytes(rnd, sizeof rnd);
    jz_uuid u = jz_uuid_v7(jz_now_ms(), rnd); /* Uuid::now_v7() */
    return jazz_types__object__ObjectId__from_uuid(&u);
}

int jazz_types__object__ObjectId__Ord__cmp(jazz_types_ObjectId a, jazz_types_ObjectId b) {
    return jz_uuid_cmp(a, b);
}

void jazz_types__object__ObjectId__Display__fmt(jz_str *out, jazz_types_ObjectId id) {
    char buf[37];
    jz_uuid_to_string(id, buf);
    jz_str_push(out, buf, 36);
}

void jazz_types__object__ObjectId__Debug__fmt(jz_str *out, jazz_types_ObjectId id) {
    jz_str_push_cstr(out, "ObjectId(");
    jazz_types__object__ObjectId__Display__fmt(out, id);
    jz_str_push_char(out, ')');
}

void jazz_types__object__ObjectId__Serialize__serialize(jz_pc_w *w, jazz_types_ObjectId id) { jz_pc_uuid(w, id); }

jz_err jazz_types__object__ObjectId__Deserialize__deserialize(jz_pc_r *r, jazz_types_ObjectId *out) {
    jz_uuid u;
    JZ_TRY(jz_pc_r_uuid(r, &u));
    *out = jazz_types__object__ObjectId__from_uuid(&u);
    return JZ_OK;
}

/* ---------------- OutputOccurrenceId ---------------- */
bool jazz_types__object__OutputOccurrenceId__has_typed_discriminators(const jazz_types_OutputOccurrenceId *o) {
    return o->union_arms.len != 0;
}

const jazz_types_UnionArm *jazz_types__object__OutputOccurrenceId__union_arms(const jazz_types_OutputOccurrenceId *o, u32 *n) {
    *n = o->union_arms.len;
    return o->union_arms.ptr;
}

const jazz_types_ObjectId *jazz_types__object__OutputOccurrenceId__joined_sources(const jazz_types_OutputOccurrenceId *o, u32 *n) {
    *n = o->joined.len;
    return o->joined.ptr;
}

void jazz_types__object__OutputOccurrenceId__new(jazz_types_OutputOccurrenceId *out, jazz_types_ObjectId root,
                                                 const jazz_types_ObjectId *joined, u32 n) {
    memset(out, 0, sizeof *out);
    out->root = root;
    jz_vec_extend(&out->joined, joined, n, sizeof *joined);
}

void jazz_types__object__OutputOccurrenceId__single_source(jazz_types_OutputOccurrenceId *out, jazz_types_ObjectId root) {
    memset(out, 0, sizeof *out);
    out->root = root;
}

static int union_arm_position_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    u32 x = ((const jazz_types_UnionArm *)a)->position, y = ((const jazz_types_UnionArm *)b)->position;
    return x < y ? -1 : x > y;
}

static void union_arm_drop(void *p) { jz_str_drop(&((jazz_types_UnionArm *)p)->label); }

bool jazz_types__object__OutputOccurrenceId__with_union_arms(jazz_types_OutputOccurrenceId *out, jazz_types_ObjectId root,
                                                             const jazz_types_ObjectId *joined, u32 n,
                                                             jazz_types_UnionArm *arms_moved, u32 narms) {
    jazz_types_OutputOccurrenceId o;
    memset(&o, 0, sizeof o);
    o.root = root;
    jz_vec_extend(&o.joined, joined, n, sizeof *joined);
    jz_vec_extend(&o.union_arms, arms_moved, narms, sizeof *arms_moved);
    /* sort_by_key(position): stable */
    jz_sort(o.union_arms.ptr, o.union_arms.len, sizeof *o.union_arms.ptr, union_arm_position_cmp, NULL);
    bool valid = true;
    for (u32 i = 0; i < o.union_arms.len; i++) {
        const jazz_types_UnionArm *a = &o.union_arms.ptr[i];
        if (!(a->position <= o.joined.len && a->label.len != 0)) { valid = false; break; }
    }
    if (valid) {
        for (u32 i = 1; i < o.union_arms.len; i++)
            if (o.union_arms.ptr[i - 1].position == o.union_arms.ptr[i].position) { valid = false; break; }
    }
    if (!valid) {
        jazz_types__object__OutputOccurrenceId__drop(&o);
        return false;
    }
    *out = o;
    return true;
}

static void push_be32(jz_bytes *b, u32 v) {
    u8 t[4] = { (u8)(v >> 24), (u8)(v >> 16), (u8)(v >> 8), (u8)v };
    jz_bytes_push(b, t, 4);
}

static jz_bytes jazz_types__object__OutputOccurrenceId__typed_canonical_bytes(const jazz_types_OutputOccurrenceId *o) {
    jz_bytes bytes = {0};
    jz_bytes_push(&bytes, o->root->b, 16);
    push_be32(&bytes, o->joined.len);
    for (u32 i = 0; i < o->joined.len; i++) jz_bytes_push(&bytes, o->joined.ptr[i]->b, 16);
    push_be32(&bytes, o->union_arms.len);
    for (u32 i = 0; i < o->union_arms.len; i++) {
        const jazz_types_UnionArm *a = &o->union_arms.ptr[i];
        push_be32(&bytes, a->position);
        push_be32(&bytes, a->label.len);
        jz_bytes_push(&bytes, (const u8 *)a->label.ptr, a->label.len);
    }
    return bytes;
}

jz_bytes jazz_types__object__OutputOccurrenceId__canonical_bytes(const jazz_types_OutputOccurrenceId *o) {
    if (o->union_arms.len != 0) return jazz_types__object__OutputOccurrenceId__typed_canonical_bytes(o);
    jz_bytes bytes = {0};
    jz_vec_reserve(&bytes, (o->joined.len + 1) * 16, 1);
    jz_bytes_push(&bytes, o->root->b, 16);
    for (u32 i = 0; i < o->joined.len; i++) jz_bytes_push(&bytes, o->joined.ptr[i]->b, 16);
    return bytes;
}

static void union_arm_clone(void *dst, const void *src) {
    const jazz_types_UnionArm *s = src;
    jazz_types_UnionArm *d = dst;
    d->position = s->position;
    d->label = jz_str_clone(&s->label);
}

void jazz_types__object__OutputOccurrenceId__Clone__clone(jazz_types_OutputOccurrenceId *dst, const jazz_types_OutputOccurrenceId *src) {
    dst->root = src->root;
    jz_vec_clone(&dst->joined, &src->joined, sizeof *src->joined.ptr, NULL);
    jz_vec_clone(&dst->union_arms, &src->union_arms, sizeof *src->union_arms.ptr, union_arm_clone);
}

void jazz_types__object__OutputOccurrenceId__drop(jazz_types_OutputOccurrenceId *o) {
    jz_vec_drop(&o->joined, sizeof *o->joined.ptr, NULL);
    jz_vec_drop(&o->union_arms, sizeof *o->union_arms.ptr, union_arm_drop);
}

bool jazz_types__object__OutputOccurrenceId__PartialEq__eq(const jazz_types_OutputOccurrenceId *a, const jazz_types_OutputOccurrenceId *b) {
    if (a->root != b->root || a->joined.len != b->joined.len || a->union_arms.len != b->union_arms.len) return false;
    for (u32 i = 0; i < a->joined.len; i++) if (a->joined.ptr[i] != b->joined.ptr[i]) return false;
    for (u32 i = 0; i < a->union_arms.len; i++) {
        if (a->union_arms.ptr[i].position != b->union_arms.ptr[i].position) return false;
        if (!jz_str_eq(&a->union_arms.ptr[i].label, &b->union_arms.ptr[i].label)) return false;
    }
    return true;
}

/* derived Ord: root, joined (lexicographic), union_arms (lexicographic) */
int jazz_types__object__OutputOccurrenceId__Ord__cmp(const jazz_types_OutputOccurrenceId *a, const jazz_types_OutputOccurrenceId *b) {
    int c = jazz_types__object__ObjectId__Ord__cmp(a->root, b->root);
    if (c) return c;
    u32 n = a->joined.len < b->joined.len ? a->joined.len : b->joined.len;
    for (u32 i = 0; i < n; i++) {
        c = jazz_types__object__ObjectId__Ord__cmp(a->joined.ptr[i], b->joined.ptr[i]);
        if (c) return c;
    }
    if (a->joined.len != b->joined.len) return a->joined.len < b->joined.len ? -1 : 1;
    n = a->union_arms.len < b->union_arms.len ? a->union_arms.len : b->union_arms.len;
    for (u32 i = 0; i < n; i++) {
        const jazz_types_UnionArm *x = &a->union_arms.ptr[i], *y = &b->union_arms.ptr[i];
        if (x->position != y->position) return x->position < y->position ? -1 : 1;
        c = jz_str_cmp(&x->label, &y->label);
        if (c) return c;
    }
    if (a->union_arms.len != b->union_arms.len) return a->union_arms.len < b->union_arms.len ? -1 : 1;
    return 0;
}

void jazz_types__object__OutputOccurrenceId__Debug__fmt(jz_str *out, const jazz_types_OutputOccurrenceId *o) {
    jz_str_push_cstr(out, "OutputOccurrenceId { root: ");
    jazz_types__object__ObjectId__Debug__fmt(out, o->root);
    jz_str_push_cstr(out, ", joined: [");
    for (u32 i = 0; i < o->joined.len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        jazz_types__object__ObjectId__Debug__fmt(out, o->joined.ptr[i]);
    }
    jz_str_push_cstr(out, "], union_arms: [");
    for (u32 i = 0; i < o->union_arms.len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        jz_str_appendf(out, "(%u, \"%.*s\")", o->union_arms.ptr[i].position, (int)o->union_arms.ptr[i].label.len,
                       o->union_arms.ptr[i].label.ptr);
    }
    jz_str_push_cstr(out, "] }");
}

/* serde: (root, joined); union_arms is #[serde(skip)] */
void jazz_types__object__OutputOccurrenceId__Serialize__serialize(jz_pc_w *w, const jazz_types_OutputOccurrenceId *o) {
    jazz_types__object__ObjectId__Serialize__serialize(w, o->root);
    jz_pc_seq(w, o->joined.len);
    for (u32 i = 0; i < o->joined.len; i++) jazz_types__object__ObjectId__Serialize__serialize(w, o->joined.ptr[i]);
}

jz_err jazz_types__object__OutputOccurrenceId__Deserialize__deserialize(jz_pc_r *r, jazz_types_OutputOccurrenceId *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jazz_types__object__ObjectId__Deserialize__deserialize(r, &out->root));
    u32 n;
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_types_ObjectId id;
        jz_err e = jazz_types__object__ObjectId__Deserialize__deserialize(r, &id);
        if (e.code) { jazz_types__object__OutputOccurrenceId__drop(out); return e; }
        JZ_VEC_PUSH(out->joined, id);
    }
    return JZ_OK;
}

/* PartialEq<ObjectId> for OutputOccurrenceId */
bool jazz_types__object__OutputOccurrenceId__eq_object_id(const jazz_types_OutputOccurrenceId *o, jazz_types_ObjectId id) {
    return o->joined.len == 0 && o->union_arms.len == 0 && o->root == id;
}

/* ---------------- ResultKey ---------------- */
#define RESULT_KEY_WIRE_VERSION 1

void jazz_types__object__ResultKey__Serialize__serialize(jz_pc_w *w, const jazz_types_ResultKey *k) {
    jz_bytes identity = jazz_types__object__OutputOccurrenceId__typed_canonical_bytes(&k->occ);
    jz_bytes encoded = {0};
    jz_vec_reserve(&encoded, identity.len + 1, 1);
    jz_bytes_push_u8(&encoded, RESULT_KEY_WIRE_VERSION);
    jz_bytes_push(&encoded, identity.ptr, identity.len);
    jz_pc_bytes(w, encoded.ptr, encoded.len);
    jz_bytes_drop(&encoded);
    jz_bytes_drop(&identity);
}

static bool take(jz_slice identity, u32 *cursor, u32 len, const u8 **out) {
    u32 end = *cursor + len;
    if (end < *cursor || end > identity.len) return false;
    *out = identity.ptr + *cursor;
    *cursor = end;
    return true;
}
static bool take_be32(jz_slice identity, u32 *cursor, u32 *out) {
    const u8 *p;
    if (!take(identity, cursor, 4, &p)) return false;
    *out = ((u32)p[0] << 24) | ((u32)p[1] << 16) | ((u32)p[2] << 8) | p[3];
    return true;
}

static bool jazz_types__object__decode_typed_result_key(jz_slice identity, jazz_types_ResultKey *out) {
    enum { MAX_JOINED_COMPONENTS = 256, MAX_DISCRIMINATOR_BYTES = 4 * 1024 };
    u32 cursor = 0;
    const u8 *p;
    if (!take(identity, &cursor, 16, &p)) return false;
    jazz_types_ObjectId root = jazz_types__object__ObjectId__from_uuid((const jz_uuid *)p);
    u32 joined_count;
    if (!take_be32(identity, &cursor, &joined_count)) return false;
    if (joined_count > MAX_JOINED_COMPONENTS) return false;
    u64 need = (u64)joined_count * 16 + 4;
    if ((u64)(identity.len - cursor) < need) return false;
    JZ_VEC(jazz_types_ObjectId) joined = {0};
    JZ_VEC(jazz_types_UnionArm) arms = {0};
    jz_vec_reserve(&joined, joined_count, sizeof(jazz_types_ObjectId));
    for (u32 i = 0; i < joined_count; i++) {
        if (!take(identity, &cursor, 16, &p)) goto fail;
        jazz_types_ObjectId id = jazz_types__object__ObjectId__from_uuid((const jz_uuid *)p);
        JZ_VEC_PUSH(joined, id);
    }
    u32 discriminator_count;
    if (!take_be32(identity, &cursor, &discriminator_count)) goto fail;
    if (discriminator_count > joined_count + 1) goto fail;
    jz_vec_reserve(&arms, discriminator_count, sizeof(jazz_types_UnionArm));
    bool has_previous = false;
    u32 previous_position = 0;
    for (u32 i = 0; i < discriminator_count; i++) {
        u32 position, len;
        if (!take_be32(identity, &cursor, &position)) goto fail;
        if (!take_be32(identity, &cursor, &len)) goto fail;
        if (len == 0 || len > MAX_DISCRIMINATOR_BYTES || len > identity.len - cursor) goto fail;
        if (position > joined_count || (has_previous && position <= previous_position)) goto fail;
        if (!take(identity, &cursor, len, &p)) goto fail;
        if (!jz_utf8_valid(p, len)) goto fail;
        jazz_types_UnionArm arm = { position, jz_str_from_slice(jz_slice_of(p, len)) };
        previous_position = position;
        has_previous = true;
        JZ_VEC_PUSH(arms, arm);
    }
    if (cursor != identity.len) goto fail;
    {
        bool ok = jazz_types__object__OutputOccurrenceId__with_union_arms(&out->occ, root, joined.ptr, joined.len, arms.ptr, arms.len);
        jz_vec_drop(&joined, sizeof(jazz_types_ObjectId), NULL);
        if (!ok) { arms.len = 0; }
        jz_vec_drop(&arms, sizeof(jazz_types_UnionArm), NULL); /* elements moved */
        return ok;
    }
fail:
    jz_vec_drop(&joined, sizeof(jazz_types_ObjectId), NULL);
    jz_vec_drop(&arms, sizeof(jazz_types_UnionArm), union_arm_drop);
    return false;
}

jz_err jazz_types__object__ResultKey__Deserialize__deserialize(jz_pc_r *r, jazz_types_ResultKey *out) {
    jz_bytes encoded;
    JZ_TRY(jz_pc_r_bytes(r, &encoded)); /* serde_bytes::ByteBuf */
    jz_err e = JZ_OK;
    if (encoded.len == 0) {
        e = jz_err_new(JAZZ_TYPES_ERR_SERDE_CUSTOM, "unsupported ResultKey version");
    } else if (encoded.ptr[0] != RESULT_KEY_WIRE_VERSION) {
        e = jz_err_new(JAZZ_TYPES_ERR_SERDE_CUSTOM, "unsupported or malformed ResultKey identity");
    } else if (!jazz_types__object__decode_typed_result_key(jz_slice_of(encoded.ptr + 1, encoded.len - 1), out)) {
        e = jz_err_new(JAZZ_TYPES_ERR_SERDE_CUSTOM, "malformed ResultKey V1 identity");
    }
    jz_bytes_drop(&encoded);
    return e;
}

bool jazz_types__object__ResultKey__row_id(const jazz_types_ResultKey *k, jazz_types_ObjectId *out) {
    if (k->occ.joined.len != 0) return false;
    *out = k->occ.root;
    return true;
}

/* ---------------- BranchName(Intern<String>) ---------------- */
static u64 str_hash(const void *p) { return jz_str_hash((const jz_str *)p); }
static bool str_eq(const void *a, const void *b) { return jz_str_eq((const jz_str *)a, (const jz_str *)b); }

jazz_types_BranchName jazz_types__object__BranchName__new(jz_str name) {
    bool inserted;
    jazz_types_BranchName n = jz_intern_ex(&jazz_types__object__BRANCH_NAME_POOL, &name, sizeof name, str_hash, str_eq, &inserted);
    if (!inserted) jz_str_drop(&name);
    return n;
}

void jazz_types__object__BranchName__Serialize__serialize(jz_pc_w *w, jazz_types_BranchName n) { jz_pc_str(w, n); }

jz_err jazz_types__object__BranchName__Deserialize__deserialize(jz_pc_r *r, jazz_types_BranchName *out) {
    jz_str s;
    JZ_TRY(jz_pc_r_str(r, &s));
    *out = jazz_types__object__BranchName__new(s);
    return JZ_OK;
}

void jazz_types__object__BranchName__Display__fmt(jz_str *out, jazz_types_BranchName n) { jz_str_push(out, n->ptr, n->len); }
