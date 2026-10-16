/* query/validation.rs: schema validation, parameter inference and binding of query shapes. */
#include "jazz_model/jazz_model.h"
#include <stdarg.h>
#include <stdio.h>

#define Q(x) jazz_model__query__##x
#define VT(x) groove__records__values__ValueType__##x
#define VDROP(v, N) jz_vec_drop(&(v), sizeof(*(v).ptr), Q(N##__drop_v))
#define SL(s) jz_str_slice(&(s))
#define CS(c) jz_slice_of((c), (u32)strlen(c))
#define SLF(s) (int)(s).len, (const char *)(s).ptr /* %.*s of a jz_slice */
#define STRF(s) (int)(s).len, (s).ptr          /* %.*s of a jz_str */

/* ---------------------------------------------------------------- QueryError */
jz_err Q(QueryError__new)(u32 variant, const char *fmt, ...) {
    char buf[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    return jz_err_new(JAZZ_MODEL_ERR_QUERY + variant, buf);
}
bool Q(QueryError__is)(const jz_err *e, u32 variant) { return e->code == JAZZ_MODEL_ERR_QUERY + variant; }

static jz_err mismatch(void) { return Q(QueryError__new)(JAZZ_QE_OPERAND_TYPE_MISMATCH, "operand type mismatch"); }
static jz_err unknown_column(jz_slice table, jz_slice column) {
    return Q(QueryError__new)(JAZZ_QE_UNKNOWN_COLUMN, "unknown column %.*s.%.*s", SLF(table), SLF(column));
}
static jz_err not_ref_compatible(jz_slice join_table, jz_slice column, jz_slice target_table) {
    return Q(QueryError__new)(JAZZ_QE_JOIN_NOT_REF_COMPATIBLE, "join column %.*s.%.*s does not reference %.*s",
                              SLF(join_table), SLF(column), SLF(target_table));
}
static jz_err unsupported_relation(const char *msg) {
    return Q(QueryError__new)(JAZZ_QE_UNSUPPORTED_RELATION_QUERY, "unsupported relation query: %s", msg);
}
static jz_err flat_join_unsupported(const char *feature) {
    return Q(QueryError__new)(JAZZ_QE_UNSUPPORTED_FLAT_JOIN_COMBINATION, "flat join cannot be combined with %s", feature);
}
static jz_err param_type_conflict(jz_slice param) {
    return Q(QueryError__new)(JAZZ_QE_PARAM_TYPE_CONFLICT, "parameter %.*s inferred with incompatible type", SLF(param));
}

static const groove_ValueType VT_UUID = {GROOVE_VT_UUID};
static const groove_ValueType VT_U64 = {GROOVE_VT_U64};
static const groove_ValueType VT_BOOL = {GROOVE_VT_BOOL};
static const groove_ValueType VT_STRING = {GROOVE_VT_STRING};
static bool vt_eq(const groove_ValueType *a, const groove_ValueType *b) { return VT(PartialEq__eq)(a, b); }
static groove_ValueType vt_clone(const groove_ValueType *t) { groove_ValueType d; VT(Clone__clone)(&d, t); return d; }
static const groove_ValueType *non_null_column_type(const groove_ValueType *t) { return t->tag == GROOVE_VT_NULLABLE ? t->u.inner : t; }

static void param_types_drop_v(void *t) { VT(drop)(t); }
const jz_btree_desc jazz_model__query__PARAM_TYPES_DESC = {sizeof(jz_str), sizeof(groove_ValueType), jz_cmp_str, jazz_model__str_drop_v, param_types_drop_v};
static void value_drop_v(void *v) { groove__records__values__Value__drop(v); }
static const jz_btree_desc STR_VALUE_DESC = {sizeof(jz_str), sizeof(groove_Value), jz_cmp_str, jazz_model__str_drop_v, value_drop_v};

/* ---------------------------------------------------------------- ValidatedQuery / Binding */
void Q(ValidatedQuery__drop)(jazz_model_ValidatedQuery *v) {
    Q(Query__drop)(&v->query);
    jz_btree_drop(&v->params);
    jz_vec_drop(&v->canonical, 1, NULL);
}
void Q(ValidatedQuery__Clone__clone)(jazz_model_ValidatedQuery *d, const jazz_model_ValidatedQuery *s) {
    Q(Query__Clone__clone)(&d->query, &s->query);
    d->schema_version = s->schema_version;
    jz_btree_clone(&d->params, &s->params, jazz_model__str_clone_v, VT(Clone__clone_v));
    jz_vec_clone(&d->canonical, &s->canonical, 1, NULL);
    d->shape_id = s->shape_id;
}

static jz_err validate_binding_values(const jz_btree *params, jz_btree values, jazz_model_Binding *out) {
    const jz_str *required;
    const groove_ValueType *expected;
    jz_err e = JZ_OK;
    JZ_BTREE_FOR(params, required, expected) {
        if (!jz_btree_contains(&values, required)) {
            e = Q(QueryError__new)(JAZZ_QE_MISSING_PARAM, "missing parameter %.*s", STRF(*required));
            goto fail;
        }
    }
    const jz_str *name;
    const groove_Value *value;
    JZ_BTREE_FOR(&values, name, value) {
        const groove_ValueType *ty = jz_btree_get(params, name);
        if (!ty) { e = Q(QueryError__new)(JAZZ_QE_UNKNOWN_PARAM, "unknown parameter %.*s", STRF(*name)); goto fail; }
        if (!Q(value_matches_type)(value, ty)) {
            e = Q(QueryError__new)(JAZZ_QE_PARAM_TYPE_MISMATCH, "parameter %.*s has wrong type", STRF(*name));
            goto fail;
        }
    }
    jz_bytes canonical = {0};
    e = Q(canonical_binding_bytes)(&values, &canonical);
    if (JZ_IS_ERR(e)) goto fail;
    out->values = values;
    out->canonical = canonical;
    return JZ_OK;
fail:
    jz_btree_drop(&values);
    return e;
}

jz_err Q(ValidatedQuery__bind)(const jazz_model_ValidatedQuery *v, jz_btree values, jazz_model_Binding *out) {
    return validate_binding_values(&v->params, values, out);
}
void Q(Binding__drop)(jazz_model_Binding *b) { jz_btree_drop(&b->values); jz_vec_drop(&b->canonical, 1, NULL); }
jazz_model_BindingId Q(Binding__binding_id)(const jazz_model_Binding *b) {
    return (jazz_model_BindingId){jz_uuid_v5(&jazz_model__query__QUERY_NAMESPACE, b->canonical.ptr, b->canonical.len)};
}

/* Canonical `jazz-binding-v0` bytes for binding values: the exact preimage of their BindingId. */
jz_err Q(canonical_binding_bytes_for_values)(const jz_btree *values, jz_bytes *out) { return Q(canonical_binding_bytes)(values, out); }

/* ---- binding_values_from_canonical_bytes::Reader */
typedef struct Reader { const u8 *p; u32 n; } Reader;
static jz_err reader_take(Reader *r, u32 len, const u8 **out) {
    if (r->n < len) return mismatch();
    *out = r->p;
    r->p += len; r->n -= len;
    return JZ_OK;
}
static jz_err reader_byte(Reader *r, u8 *out) { const u8 *p; JZ_TRY(reader_take(r, 1, &p)); *out = p[0]; return JZ_OK; }
static jz_err reader_be(Reader *r, u32 width, u64 *out) {
    const u8 *p;
    JZ_TRY(reader_take(r, width, &p));
    u64 v = 0;
    for (u32 i = 0; i < width; i++) v = (v << 8) | p[i];
    *out = v;
    return JZ_OK;
}
static jz_err reader_len(Reader *r, u32 *out) {
    u64 len;
    JZ_TRY(reader_be(r, 8, &len));
    /* Every encoded element occupies at least one byte, so a length beyond the remaining input is malformed. */
    if (len > r->n) return mismatch();
    *out = (u32)len;
    return JZ_OK;
}
static jz_err reader_bytes(Reader *r, jz_bytes *out) {
    u32 len;
    const u8 *p;
    JZ_TRY(reader_len(r, &len));
    JZ_TRY(reader_take(r, len, &p));
    *out = (jz_bytes){0};
    jz_vec_extend(out, p, len, 1);
    return JZ_OK;
}
static bool utf8_valid(const u8 *s, u32 n) {
    for (u32 i = 0; i < n;) {
        u8 c = s[i];
        u32 len = c < 0x80 ? 1 : (c >> 5) == 6 ? 2 : (c >> 4) == 14 ? 3 : (c >> 3) == 30 ? 4 : 0;
        if (!len || i + len > n) return false;
        for (u32 j = 1; j < len; j++) if ((s[i + j] & 0xc0) != 0x80) return false;
        i += len;
    }
    return true;
}
static jz_err reader_string(Reader *r, jz_str *out) {
    jz_bytes b;
    JZ_TRY(reader_bytes(r, &b));
    if (!utf8_valid(b.ptr, b.len)) { jz_vec_drop(&b, 1, NULL); return mismatch(); }
    *out = (jz_str){(char *)b.ptr, b.len, b.cap};
    return JZ_OK;
}
static groove_Value gv(u8 tag) { groove_Value v; memset(&v, 0, sizeof(v)); v.tag = tag; return v; }
static jz_err reader_value(Reader *r, u32 depth, groove_Value *out) {
    if (depth > 64) return mismatch();
    u8 tag, b;
    u64 x;
    JZ_TRY(reader_byte(r, &tag));
    switch (tag) {
    case 1: JZ_TRY(reader_byte(r, &b)); *out = gv(GROOVE_V_U8); out->u.u8_ = b; return JZ_OK;
    case 2: JZ_TRY(reader_be(r, 2, &x)); *out = gv(GROOVE_V_U16); out->u.u16_ = (u16)x; return JZ_OK;
    case 3: JZ_TRY(reader_be(r, 4, &x)); *out = gv(GROOVE_V_U32); out->u.u32_ = (u32)x; return JZ_OK;
    case 4: JZ_TRY(reader_be(r, 8, &x)); *out = gv(GROOVE_V_U64); out->u.u64_ = x; return JZ_OK;
    case 5: JZ_TRY(reader_be(r, 8, &x)); *out = gv(GROOVE_V_F64); memcpy(&out->u.f64_, &x, 8); return JZ_OK;
    case 6:
        JZ_TRY(reader_byte(r, &b));
        if (b > 1) return mismatch();
        *out = gv(GROOVE_V_BOOL); out->u.b = b == 1;
        return JZ_OK;
    case 7: *out = gv(GROOVE_V_STRING); return reader_string(r, &out->u.s);
    case 8: *out = gv(GROOVE_V_BYTES); return reader_bytes(r, &out->u.bytes);
    case 9: {
        const u8 *p;
        JZ_TRY(reader_take(r, 16, &p));
        *out = gv(GROOVE_V_UUID); memcpy(out->u.uuid.b, p, 16);
        return JZ_OK;
    }
    case 10: JZ_TRY(reader_byte(r, &b)); *out = gv(GROOVE_V_ENUM_TAG); out->u.enum_tag = b; return JZ_OK;
    case 11:
    case 12: {
        u32 len;
        JZ_TRY(reader_len(r, &len));
        *out = gv(tag == 11 ? GROOVE_V_TUPLE : GROOVE_V_ARRAY);
        for (u32 i = 0; i < len; i++) {
            groove_Value item;
            jz_err e = reader_value(r, depth + 1, &item);
            if (JZ_IS_ERR(e)) { groove__records__values__Value__drop(out); return e; }
            JZ_VEC_PUSH(out->u.items, item);
        }
        return JZ_OK;
    }
    case 13:
        JZ_TRY(reader_byte(r, &b));
        *out = gv(GROOVE_V_NULLABLE);
        if (b == 0) return JZ_OK;
        if (b != 1) return mismatch();
        out->u.nullable = JZ_NEW(groove_Value);
        {
            jz_err e = reader_value(r, depth + 1, out->u.nullable);
            if (JZ_IS_ERR(e)) { jz_free(out->u.nullable); out->u.nullable = NULL; }
            return e;
        }
    case 14: JZ_TRY(reader_be(r, 8, &x)); *out = gv(GROOVE_V_I64); out->u.i64_ = (i64)x; return JZ_OK;
    case 15: JZ_TRY(reader_be(r, 4, &x)); *out = gv(GROOVE_V_I32); out->u.i32_ = (i32)(u32)x; return JZ_OK;
    default: return mismatch();
    }
}

/* Decode canonical `jazz-binding-v0` bytes back into binding values (tags 1-15). The result is only
 * returned when re-encoding reproduces the input exactly. */
jz_err Q(binding_values_from_canonical_bytes)(jz_slice bytes, jz_btree *out) {
    Reader r = {bytes.ptr, bytes.len};
    const u8 *head;
    JZ_TRY(reader_take(&r, 15, &head));
    if (memcmp(head, "jazz-binding-v0", 15) != 0) return mismatch();
    u32 count;
    JZ_TRY(reader_len(&r, &count));
    jz_btree values;
    jz_btree_init(&values, &STR_VALUE_DESC);
    jz_err e = JZ_OK;
    for (u32 i = 0; i < count; i++) {
        jz_str name;
        e = reader_string(&r, &name);
        if (JZ_IS_ERR(e)) goto fail;
        groove_Value value;
        e = reader_value(&r, 0, &value);
        if (JZ_IS_ERR(e)) { jz_str_drop(&name); goto fail; }
        groove_Value old;
        if (jz_btree_insert(&values, &name, &value, &old)) {
            groove__records__values__Value__drop(&old);
            e = mismatch();
            goto fail;
        }
    }
    if (r.n != 0) { e = mismatch(); goto fail; }
    jz_bytes canonical = {0};
    e = Q(canonical_binding_bytes)(&values, &canonical);
    if (JZ_IS_ERR(e)) goto fail;
    bool same = jz_slice_cmp(jz_bytes_slice(&canonical), bytes) == 0;
    jz_vec_drop(&canonical, 1, NULL);
    if (!same) { e = mismatch(); goto fail; }
    *out = values;
    return JZ_OK;
fail:
    jz_btree_drop(&values);
    return e;
}

jz_err Q(binding_id_for_values)(const jz_btree *values, jazz_model_BindingId *out) {
    jz_bytes canonical = {0};
    JZ_TRY(Q(canonical_binding_bytes)(values, &canonical));
    out->id = jz_uuid_v5(&jazz_model__query__QUERY_NAMESPACE, canonical.ptr, canonical.len);
    jz_vec_drop(&canonical, 1, NULL);
    return JZ_OK;
}

/* ---------------------------------------------------------------- schema lookups */
jz_err Q(schema_table)(const jazz_model_RuntimeSchema *schema, jz_slice name, const jazz_model_TableSchema **out) {
    JZ_VEC_FOR(const jazz_model_TableSchema, t, schema->tables) {
        if (jz_slice_cmp(SL(t->name), name) == 0) { *out = t; return JZ_OK; }
    }
    return Q(QueryError__new)(JAZZ_QE_UNKNOWN_TABLE, "unknown table %.*s", SLF(name));
}
static jz_err column_schema(const jazz_model_TableSchema *table, jz_slice column, const jazz_model_ColumnSchema **out) {
    JZ_VEC_FOR(const jazz_model_ColumnSchema, c, table->columns) {
        if (jz_slice_cmp(SL(c->name), column) == 0) { *out = c; return JZ_OK; }
    }
    return unknown_column(SL(table->name), column);
}
static jz_err column_type(const jazz_model_TableSchema *table, jz_slice column, const groove_ValueType **out) {
    const jazz_model_ColumnSchema *c;
    JZ_TRY(column_schema(table, column, &c));
    *out = &c->column_type;
    return JZ_OK;
}
static bool slice_is(jz_slice s, const char *c) { return s.len == strlen(c) && memcmp(s.ptr, c, s.len) == 0; }
static bool is_permission_introspection_magic_column(jz_slice column) { return slice_is(column, "$canRead"); }
static jz_err executable_magic_column_type(jz_slice column, const groove_ValueType **out) {
    const groove_ValueType *ty = jazz_types__ids__AuthorSubject__metadata_type(column);
    if (ty) { *out = ty; return JZ_OK; }
    if (is_permission_introspection_magic_column(column))
        return Q(QueryError__new)(JAZZ_QE_UNSUPPORTED_MAGIC_COLUMN,
                                  "unsupported query magic column %.*s: permission introspection columns are not executable yet", SLF(column));
    *out = (slice_is(column, "$createdAt") || slice_is(column, "$updatedAt")) ? &VT_U64 : NULL;
    return JZ_OK;
}
jz_err Q(planner_column_type)(const jazz_model_TableSchema *table, jz_slice column, const groove_ValueType **out) {
    if (slice_is(column, "id")) { *out = &VT_UUID; return JZ_OK; }
    JZ_TRY(executable_magic_column_type(column, out));
    if (*out) return JZ_OK;
    return column_type(table, column, out);
}
#define PCT(table, column, out) Q(planner_column_type)((table), (column), (out))

/* ---------------------------------------------------------------- type relations */
static bool column_types_comparable(const groove_ValueType *l, const groove_ValueType *r) {
    l = non_null_column_type(l);
    r = non_null_column_type(r);
    return vt_eq(l, r) || (l->tag == GROOVE_VT_ENUM_TAG && r->tag == GROOVE_VT_U8) || (l->tag == GROOVE_VT_U8 && r->tag == GROOVE_VT_ENUM_TAG);
}
static bool is_orderable(const groove_ValueType *t) {
    switch (non_null_column_type(t)->tag) {
    case GROOVE_VT_U8: case GROOVE_VT_U16: case GROOVE_VT_U32: case GROOVE_VT_U64: case GROOVE_VT_I32: case GROOVE_VT_I64:
    case GROOVE_VT_F64: case GROOVE_VT_UUID: case GROOVE_VT_STRING: return true;
    default: return false;
    }
}
/* Unwrap nullability like is_orderable: SUM/AVG over a nullable column is the canonical SQL case. */
static bool is_numeric(const groove_ValueType *t) {
    switch (non_null_column_type(t)->tag) {
    case GROOVE_VT_U8: case GROOVE_VT_U16: case GROOVE_VT_U32: case GROOVE_VT_U64: case GROOVE_VT_I32: case GROOVE_VT_I64:
    case GROOVE_VT_F64: return true;
    default: return false;
    }
}
static bool array_element_type_compatible(const groove_ValueType *array, const groove_ValueType *scalar) {
    const groove_ValueType *a = non_null_column_type(array);
    if (a->tag != GROOVE_VT_ARRAY) return false;
    return non_null_column_type(scalar)->tag != GROOVE_VT_ARRAY && column_types_comparable(a->u.inner, scalar);
}
static bool flat_join_key_types_compatible(const groove_ValueType *l, const groove_ValueType *r) {
    return column_types_comparable(l, r) || array_element_type_compatible(l, r) || array_element_type_compatible(r, l);
}
static bool in_operand_types_compatible(const groove_ValueType *l, const groove_ValueType *r) {
    if (column_types_comparable(l, r)) return true;
    l = non_null_column_type(l);
    r = non_null_column_type(r);
    return l->tag == GROOVE_VT_ENUM_TAG && (r->tag == GROOVE_VT_STRING || r->tag == GROOVE_VT_UUID);
}
/* Array-subquery correlation expands array lookup keys on either side. */
static bool array_correlation_types_compatible(const groove_ValueType *parent, const groove_ValueType *child) {
    if (in_operand_types_compatible(parent, child)) return true;
    const groove_ValueType *p = non_null_column_type(parent);
    bool forward = p->tag == GROOVE_VT_ARRAY && column_types_comparable(p->u.inner, child);
    return forward || array_element_type_compatible(child, parent);
}
static bool in_literal_value_coercible_value(const groove_ValueType *left, const groove_Value *value) {
    const groove_ValueType *l = non_null_column_type(left);
    switch (l->tag) {
    case GROOVE_VT_STRING: return value->tag == GROOVE_V_UUID;
    case GROOVE_VT_ENUM_TAG: return value->tag == GROOVE_V_STRING || value->tag == GROOVE_V_UUID;
    case GROOVE_VT_ARRAY:
        if (value->tag != GROOVE_V_ARRAY) return false;
        JZ_VEC_FOR(const groove_Value, it, value->u.items) if (!in_literal_value_coercible_value(l->u.inner, it)) return false;
        return true;
    default: return false;
    }
}
static bool in_literal_value_coercible(const groove_ValueType *left, const jazz_model_Operand *value) {
    return value->tag == JAZZ_OPERAND_LITERAL && in_literal_value_coercible_value(left, &value->literal);
}
static jz_err in_candidate_type_mismatch_error(const jazz_model_Operand *left, const groove_ValueType *column_type,
                                               const groove_ValueType *candidate_type) {
    if (left->tag != JAZZ_OPERAND_COLUMN) return mismatch();
    jz_str ct = {0}, cand = {0};
    VT(Debug__fmt)(&ct, column_type);
    VT(Debug__fmt)(&cand, candidate_type);
    jz_err e = Q(QueryError__new)(JAZZ_QE_IN_CANDIDATE_TYPE_MISMATCH,
                                  "in candidate for column %.*s has type %.*s, but the column has type %.*s",
                                  STRF(left->name), STRF(cand), STRF(ct));
    jz_str_drop(&ct); jz_str_drop(&cand);
    return e;
}
static bool coerce_integer_literal_for_type(jazz_model_Operand *operand, const groove_ValueType *target) {
    target = non_null_column_type(target);
    if (operand->tag != JAZZ_OPERAND_LITERAL) return false;
    groove_Value *v = &operand->literal;
    if (v->tag == GROOVE_V_I32 && target->tag == GROOVE_VT_I64) {
        i64 x = v->u.i32_;
        v->tag = GROOVE_V_I64; v->u.i64_ = x;
        return true;
    }
    if (v->tag == GROOVE_V_I64 && target->tag == GROOVE_VT_I32 && v->u.i64_ >= INT32_MIN && v->u.i64_ <= INT32_MAX) {
        i32 x = (i32)v->u.i64_;
        v->tag = GROOVE_V_I32; v->u.i32_ = x;
        return true;
    }
    return false;
}

/* Option<ColumnType> owned */
typedef struct OptType { bool some; groove_ValueType t; } OptType;
static void opt_type_drop(OptType *o) { if (o->some) VT(drop)(&o->t); o->some = false; }

static jz_err claim_type(jz_slice name, OptType *out) {
    out->some = false;
    if (name.len >= 4 && memcmp(name.ptr, "user", 4) == 0) {
        jz_str key = jz_format("$createdBy%.*s", (int)(name.len - 4), (const char *)name.ptr + 4);
        const groove_ValueType *ty = jazz_types__ids__AuthorSubject__metadata_type(SL(key));
        jz_str_drop(&key);
        if (ty) { out->some = true; out->t = vt_clone(ty); return JZ_OK; }
    }
    if (slice_is(name, "team")) { out->some = true; out->t = VT_UUID; }
    else if (slice_is(name, "isAdmin")) { out->some = true; out->t = VT_BOOL; }
    return JZ_OK;
}

static jz_err operand_type(const jazz_model_TableSchema *table, const jazz_model_Operand *operand, const jz_btree *params, OptType *out) {
    out->some = false;
    switch (operand->tag) {
    case JAZZ_OPERAND_COLUMN: {
        const groove_ValueType *t;
        JZ_TRY(PCT(table, SL(operand->name), &t));
        out->some = true; out->t = vt_clone(t);
        return JZ_OK;
    }
    case JAZZ_OPERAND_LITERAL:
        JZ_TRY(Q(value_type)(&operand->literal, &out->t));
        out->some = true;
        return JZ_OK;
    case JAZZ_OPERAND_PARAM: {
        const groove_ValueType *t = params ? jz_btree_get(params, &operand->name) : NULL;
        if (t) { out->some = true; out->t = vt_clone(t); }
        return JZ_OK;
    }
    default: return claim_type(SL(operand->name), out);
    }
}

static jz_err claim_type_mismatch_error(const jazz_model_Operand *claim, const groove_ValueType *claim_ty,
                                        const jazz_model_Operand *other, const groove_ValueType *other_ty, bool *some) {
    *some = false;
    if (claim->tag != JAZZ_OPERAND_CLAIM || other->tag != JAZZ_OPERAND_COLUMN) return JZ_OK;
    *some = true;
    /* column_type_name: format!("{column_type:?}"); the message then Debug-quotes those Strings */
    jz_str ct = {0}, ot = {0}, ctq = {0}, otq = {0};
    VT(Debug__fmt)(&ct, claim_ty);
    VT(Debug__fmt)(&ot, other_ty);
    jazz_model__debug_str(&ctq, ct.ptr, ct.len);
    jazz_model__debug_str(&otq, ot.ptr, ot.len);
    jz_err e = Q(QueryError__new)(JAZZ_QE_CLAIM_TYPE_MISMATCH, "claim %.*s has type %.*s, but column %.*s has type %.*s",
                                  STRF(claim->name), STRF(ctq), STRF(other->name), STRF(otq));
    jz_str_drop(&ct); jz_str_drop(&ot); jz_str_drop(&ctq); jz_str_drop(&otq);
    return e;
}

static jz_err check_inferred_param(const jazz_model_Operand *operand, const groove_ValueType *expected, const jz_btree *params) {
    if (operand->tag != JAZZ_OPERAND_PARAM) return JZ_OK;
    const groove_ValueType *existing = jz_btree_get(params, &operand->name);
    if (!existing) return mismatch();
    if (!vt_eq(existing, expected)) return param_type_conflict(SL(operand->name));
    return JZ_OK;
}

/* ---------------------------------------------------------------- ParameterDomain */
typedef struct ParameterFact { u32 descriptor; bool required; } ParameterFact;
enum { TERM_PARAM, TERM_KNOWN, TERM_DYNAMIC };
typedef struct ParameterTerm { u8 tag; u32 param; ParameterFact fact; } ParameterTerm;
enum { DESC_FIXED, DESC_ARRAY };
/* Array keeps the member live: copying a provisional nullable member would make aliases stale. */
typedef struct ParameterDescriptor { u8 tag; groove_ValueType fixed; ParameterTerm member; } ParameterDescriptor;
enum { REL_COMPARE, REL_IN, REL_CONTAINS };
typedef JZ_VEC(u32) U32Vec;
typedef struct ParameterRequirement {
    bool has_fact; ParameterFact fact;
    U32Vec dependents, constructed_dependents;
    u64 notified, needle_seen, haystack_seen, fallback_deferred;
} ParameterRequirement;
typedef struct ParameterDependency {
    u8 relationship;
    ParameterTerm left, right;
    bool literal;
    bool has_member; ParameterTerm member;
    bool constructor_queued;
} ParameterDependency;
/* VecDeque<usize> */
typedef struct Deque { u32 *ptr; u32 len, cap; u32 head; } Deque;
static void dq_push_back(Deque *d, u32 x) { JZ_VEC_PUSH(*d, x); }
static void dq_push_front(Deque *d, u32 x) {
    if (d->head > 0) d->ptr[--d->head] = x;
    else jz_vec_insert(d, 0, &x, sizeof(u32));
}
static bool dq_pop_front(Deque *d, u32 *out) {
    if (d->head >= d->len) { d->head = 0; d->len = 0; return false; }
    *out = d->ptr[d->head++];
    return true;
}
static void dq_drop(Deque *d) { jz_vec_drop(d, sizeof(u32), NULL); }

typedef struct ParameterDomain {
    jz_btree names; /* String -> usize */
    JZ_VEC(ParameterRequirement) requirements;
    JZ_VEC(ParameterDescriptor) descriptors;
    JZ_VEC(ParameterDependency) dependencies;
    Deque pending;
    JZ_VEC(bool) queued;
    U32Vec affected;
    u64 notification;
    U32Vec literal_dependencies, contains_dependencies;
    Deque constructors;
    U32Vec needle_search, haystack_search;
    u64 needle_generation, haystack_generation;
} ParameterDomain;
static const jz_btree_desc STR_INDEX_DESC = {sizeof(jz_str), sizeof(u32), jz_cmp_str, jazz_model__str_drop_v, NULL};

static void domain_init(ParameterDomain *d) {
    memset(d, 0, sizeof(*d));
    jz_btree_init(&d->names, &STR_INDEX_DESC);
}
static void requirement_drop_v(void *p) {
    ParameterRequirement *r = p;
    jz_vec_drop(&r->dependents, sizeof(u32), NULL);
    jz_vec_drop(&r->constructed_dependents, sizeof(u32), NULL);
}
static void descriptor_drop_v(void *p) { ParameterDescriptor *d = p; if (d->tag == DESC_FIXED) VT(drop)(&d->fixed); }
static void domain_drop(ParameterDomain *d) {
    jz_btree_drop(&d->names);
    jz_vec_drop(&d->requirements, sizeof(ParameterRequirement), requirement_drop_v);
    jz_vec_drop(&d->descriptors, sizeof(ParameterDescriptor), descriptor_drop_v);
    jz_vec_drop(&d->dependencies, sizeof(ParameterDependency), NULL);
    dq_drop(&d->pending);
    jz_vec_drop(&d->queued, sizeof(bool), NULL);
    jz_vec_drop(&d->affected, sizeof(u32), NULL);
    jz_vec_drop(&d->literal_dependencies, sizeof(u32), NULL);
    jz_vec_drop(&d->contains_dependencies, sizeof(u32), NULL);
    dq_drop(&d->constructors);
    jz_vec_drop(&d->needle_search, sizeof(u32), NULL);
    jz_vec_drop(&d->haystack_search, sizeof(u32), NULL);
}

#define REQ(d, i) (&(d)->requirements.ptr[(i)])
#define DEP(d, i) (&(d)->dependencies.ptr[(i)])
#define DESC(d, i) (&(d)->descriptors.ptr[(i)])

static ParameterTerm term_param(u32 index) { ParameterTerm t = {TERM_PARAM, index, {0, false}}; return t; }

static ParameterTerm domain_parameter(ParameterDomain *d, jz_slice name) {
    jz_str key = jz_str_from_slice(name);
    const u32 *found = jz_btree_get(&d->names, &key);
    if (found) { jz_str_drop(&key); return term_param(*found); }
    u32 index = d->requirements.len;
    jz_btree_insert(&d->names, &key, &index, NULL);
    ParameterRequirement *r = JAZZ_VEC_SLOT(d->requirements);
    (void)r; /* fact None, empty dependents, counters 0 */
    return term_param(index);
}

static ParameterTerm domain_known(ParameterDomain *d, groove_ValueType ty) {
    bool required = true;
    groove_ValueType base = ty;
    if (ty.tag == GROOVE_VT_NULLABLE) {
        base = *ty.u.inner;
        jz_free(ty.u.inner);
        required = false;
    }
    u32 descriptor = d->descriptors.len;
    ParameterDescriptor *desc = JAZZ_VEC_SLOT(d->descriptors);
    desc->tag = DESC_FIXED;
    desc->fixed = base;
    ParameterTerm t = {TERM_KNOWN, 0, {descriptor, required}};
    return t;
}

static jz_err domain_term(ParameterDomain *d, const jazz_model_TableSchema *table, const jazz_model_Operand *operand, ParameterTerm *out) {
    switch (operand->tag) {
    case JAZZ_OPERAND_PARAM: *out = domain_parameter(d, SL(operand->name)); return JZ_OK;
    case JAZZ_OPERAND_COLUMN: {
        const groove_ValueType *t;
        JZ_TRY(PCT(table, SL(operand->name), &t));
        *out = domain_known(d, vt_clone(t));
        return JZ_OK;
    }
    case JAZZ_OPERAND_LITERAL: {
        groove_ValueType t;
        JZ_TRY(Q(value_type)(&operand->literal, &t));
        *out = domain_known(d, t);
        return JZ_OK;
    }
    default: {
        OptType t;
        JZ_TRY(claim_type(SL(operand->name), &t));
        if (t.some) *out = domain_known(d, t.t);
        else out->tag = TERM_DYNAMIC;
        return JZ_OK;
    }
    }
}

static jz_err domain_relationship(ParameterDomain *d, const jazz_model_TableSchema *table, const jazz_model_Operand *left,
                                  const jazz_model_Operand *right, u8 relationship) {
    /* Empty vectors/maps allocate nothing on the ordinary no-param path. */
    if (left->tag != JAZZ_OPERAND_PARAM && right->tag != JAZZ_OPERAND_PARAM) return JZ_OK;
    ParameterTerm lt, rt;
    JZ_TRY(domain_term(d, table, left, &lt));
    JZ_TRY(domain_term(d, table, right, &rt));
    u32 index = d->dependencies.len;
    ParameterTerm terms[2] = {lt, rt};
    for (u32 i = 0; i < 2; i++)
        if (terms[i].tag == TERM_PARAM) JZ_VEC_PUSH(REQ(d, terms[i].param)->dependents, index);
    bool literal = left->tag == JAZZ_OPERAND_LITERAL || right->tag == JAZZ_OPERAND_LITERAL;
    if (literal) JZ_VEC_PUSH(d->literal_dependencies, index);
    if (relationship == REL_CONTAINS) JZ_VEC_PUSH(d->contains_dependencies, index);
    ParameterDependency *dep = JAZZ_VEC_SLOT(d->dependencies);
    dep->relationship = relationship;
    dep->left = lt;
    dep->right = rt;
    dep->literal = literal;
    bool f = false;
    JZ_VEC_PUSH(d->queued, f);
    return JZ_OK;
}

static bool domain_fact(const ParameterDomain *d, ParameterTerm term, ParameterFact *out) {
    switch (term.tag) {
    case TERM_PARAM:
        if (!REQ(d, term.param)->has_fact) return false;
        *out = REQ(d, term.param)->fact;
        return true;
    case TERM_KNOWN: *out = term.fact; return true;
    default: return false;
    }
}

static bool descriptor_matches_type(const ParameterDomain *d, u32 descriptor, const groove_ValueType *ty);
static bool matches_type(const ParameterDomain *d, ParameterFact fact, const groove_ValueType *ty) {
    bool required = ty->tag != GROOVE_VT_NULLABLE;
    const groove_ValueType *base = required ? ty : ty->u.inner;
    return fact.required == required && descriptor_matches_type(d, fact.descriptor, base);
}
static bool descriptor_matches_type(const ParameterDomain *d, u32 descriptor, const groove_ValueType *ty) {
    const ParameterDescriptor *desc = DESC(d, descriptor);
    if (desc->tag == DESC_FIXED) return vt_eq(&desc->fixed, ty);
    if (ty->tag != GROOVE_VT_ARRAY) return false;
    ParameterFact f;
    return domain_fact(d, desc->member, &f) && matches_type(d, f, ty->u.inner);
}
static bool same_descriptor(const ParameterDomain *d, u32 left, u32 right) {
    if (left == right) return true;
    const ParameterDescriptor *l = DESC(d, left), *r = DESC(d, right);
    if (l->tag == DESC_FIXED) return descriptor_matches_type(d, right, &l->fixed);
    if (r->tag == DESC_FIXED) return descriptor_matches_type(d, left, &r->fixed);
    ParameterFact lf, rf;
    if (!domain_fact(d, l->member, &lf) || !domain_fact(d, r->member, &rf)) return false;
    return lf.required == rf.required && same_descriptor(d, lf.descriptor, rf.descriptor);
}
static bool depends_on(const ParameterDomain *d, u32 descriptor, u32 param) {
    const ParameterDescriptor *desc = DESC(d, descriptor);
    if (desc->tag == DESC_FIXED) return false;
    switch (desc->member.tag) {
    case TERM_PARAM: {
        u32 member = desc->member.param;
        return member == param || (REQ(d, member)->has_fact && depends_on(d, REQ(d, member)->fact.descriptor, param));
    }
    case TERM_KNOWN: return depends_on(d, desc->member.fact.descriptor, param);
    default: return false;
    }
}
static void domain_enqueue(ParameterDomain *d, u32 dependency) {
    if (!d->queued.ptr[dependency]) {
        d->queued.ptr[dependency] = true;
        dq_push_back(&d->pending, dependency);
    }
}
static void register_constructor_consumer(ParameterDomain *d, u32 descriptor, u32 consumer) {
    const ParameterDescriptor *desc = DESC(d, descriptor);
    if (desc->tag != DESC_ARRAY) return;
    if (desc->member.tag == TERM_PARAM) JZ_VEC_PUSH(REQ(d, desc->member.param)->constructed_dependents, consumer);
    else if (desc->member.tag == TERM_KNOWN) register_constructor_consumer(d, desc->member.fact.descriptor, consumer);
}

static jz_err domain_require(ParameterDomain *d, ParameterTerm term, ParameterFact fact) {
    if (term.tag != TERM_PARAM) return JZ_OK;
    u32 index = term.param;
    ParameterRequirement *r = REQ(d, index);
    bool changed;
    if (!r->has_fact) {
        if (depends_on(d, fact.descriptor, index)) return mismatch();
        r->has_fact = true;
        r->fact = fact;
        register_constructor_consumer(d, fact.descriptor, index);
        changed = true;
    } else if (same_descriptor(d, r->fact.descriptor, fact.descriptor) && fact.required && !r->fact.required) {
        REQ(d, index)->fact.required = true;
        changed = true;
    } else {
        /* Heterogeneous comparability is not transitive; keep independently anchored descriptors distinct. */
        changed = false;
    }
    if (changed) {
        /* A constructor's member can change without its descriptor index changing.
         * Notify all consumers of that descriptor, transitively. */
        d->notification += 1;
        JZ_VEC_PUSH(d->affected, index);
        while (d->affected.len) {
            u32 param = d->affected.ptr[--d->affected.len];
            if (REQ(d, param)->notified == d->notification) continue;
            REQ(d, param)->notified = d->notification;
            for (u32 off = 0; off < REQ(d, param)->dependents.len; off++) domain_enqueue(d, REQ(d, param)->dependents.ptr[off]);
            jz_vec_extend(&d->affected, REQ(d, param)->constructed_dependents.ptr, REQ(d, param)->constructed_dependents.len, sizeof(u32));
        }
    }
    return JZ_OK;
}

static bool unresolved_alias(const ParameterDomain *d, u32 dependency, u32 param, u32 *out) {
    const ParameterDependency *dep = DEP(d, dependency);
    if (dep->literal || dep->relationship == REL_CONTAINS) return false;
    if (dep->left.tag != TERM_PARAM || dep->right.tag != TERM_PARAM) return false;
    u32 other = dep->left.param == param ? dep->right.param : dep->left.param;
    if (REQ(d, other)->has_fact) return false;
    *out = other;
    return true;
}
static bool ready_constructor(const ParameterDomain *d, u32 dependency) {
    const ParameterDependency *dep = DEP(d, dependency);
    ParameterFact f;
    return dep->relationship == REL_CONTAINS && dep->left.tag == TERM_PARAM && !domain_fact(d, dep->left, &f) &&
           domain_fact(d, dep->right, &f);
}
static bool contains_haystack_of(const ParameterDomain *d, u32 index, u32 param, u32 *haystack) {
    const ParameterDependency *dep = DEP(d, index);
    if (dep->relationship == REL_CONTAINS && dep->right.tag == TERM_PARAM && dep->right.param == param && dep->left.tag == TERM_PARAM &&
        !REQ(d, dep->left.param)->has_fact) {
        *haystack = dep->left.param;
        return true;
    }
    return false;
}

static bool ready_forward_provider(ParameterDomain *d, u32 needle, u32 *out) {
    /* Mark this unknown alias component first: its own constructors cannot supply a forward
     * expectation to themselves. This is dependency lookahead. */
    d->needle_generation += 1;
    d->haystack_generation += 1;
    d->needle_search.len = 0;
    d->haystack_search.len = 0;
    JZ_VEC_PUSH(d->needle_search, needle);
    while (d->needle_search.len) {
        u32 param = d->needle_search.ptr[--d->needle_search.len];
        if (REQ(d, param)->needle_seen == d->needle_generation) continue;
        REQ(d, param)->needle_seen = d->needle_generation;
        for (u32 off = 0; off < REQ(d, param)->dependents.len; off++) {
            u32 index = REQ(d, param)->dependents.ptr[off], other, haystack;
            if (unresolved_alias(d, index, param, &other)) JZ_VEC_PUSH(d->needle_search, other);
            if (contains_haystack_of(d, index, param, &haystack)) JZ_VEC_PUSH(d->haystack_search, haystack);
        }
    }
    /* Follow forward providers transitively, including unknown aliases between them. */
    while (d->haystack_search.len) {
        u32 param = d->haystack_search.ptr[--d->haystack_search.len];
        if (REQ(d, param)->needle_seen == d->needle_generation || REQ(d, param)->haystack_seen == d->haystack_generation) continue;
        REQ(d, param)->haystack_seen = d->haystack_generation;
        for (u32 off = 0; off < REQ(d, param)->dependents.len; off++) {
            u32 index = REQ(d, param)->dependents.ptr[off], other, haystack;
            if (unresolved_alias(d, index, param, &other)) JZ_VEC_PUSH(d->haystack_search, other);
            const ParameterDependency *dep = DEP(d, index);
            if (dep->left.tag == TERM_PARAM && dep->left.param == param && ready_constructor(d, index)) { *out = index; return true; }
            if (contains_haystack_of(d, index, param, &haystack)) JZ_VEC_PUSH(d->haystack_search, haystack);
        }
    }
    return false;
}

static void enqueue_constructor(ParameterDomain *d, u32 index) {
    if (ready_constructor(d, index) && !DEP(d, index)->constructor_queued) {
        DEP(d, index)->constructor_queued = true;
        dq_push_back(&d->constructors, index);
    }
}

static jz_err settle_dependency(ParameterDomain *d, u32 index, u8 fallbacks);
static jz_err drain_pending(ParameterDomain *d, bool literals, bool constructors) {
    u32 index;
    while (dq_pop_front(&d->pending, &index)) {
        d->queued.ptr[index] = false;
        JZ_TRY(settle_dependency(d, index, literals ? 1 : 0));
        if (constructors) enqueue_constructor(d, index);
    }
    return JZ_OK;
}

static jz_err settle_dependency(ParameterDomain *d, u32 index, u8 fallbacks) {
    ParameterDependency dep = *DEP(d, index);
    if (dep.literal && fallbacks == 0) return JZ_OK;
    ParameterFact lf, rf;
    bool hl, hr;
    if (dep.relationship == REL_COMPARE || dep.relationship == REL_IN) {
        hl = domain_fact(d, dep.left, &lf);
        hr = domain_fact(d, dep.right, &rf);
        if (hl && !hr) JZ_TRY(domain_require(d, dep.right, lf));
        else if (!hl && hr) JZ_TRY(domain_require(d, dep.left, rf));
        else if (hl && hr && !dep.literal && same_descriptor(d, lf.descriptor, rf.descriptor)) {
            JZ_TRY(domain_require(d, dep.left, rf));
            JZ_TRY(domain_require(d, dep.right, lf));
        }
        return JZ_OK;
    }
    /* Contains */
    ParameterFact haystack;
    if (domain_fact(d, dep.left, &haystack)) {
        bool has_member = DEP(d, index)->has_member;
        ParameterTerm member = DEP(d, index)->member;
        if (!has_member) {
            const ParameterDescriptor *desc = DESC(d, haystack.descriptor);
            if (desc->tag == DESC_FIXED && desc->fixed.tag == GROOVE_VT_STRING) {
                member = domain_known(d, VT_STRING); has_member = true;
            } else if (desc->tag == DESC_FIXED && desc->fixed.tag == GROOVE_VT_ARRAY) {
                groove_ValueType ty = vt_clone(desc->fixed.u.inner);
                member = domain_known(d, ty); has_member = true;
            } else if (desc->tag == DESC_ARRAY) {
                member = desc->member; has_member = true;
            }
            DEP(d, index)->has_member = has_member;
            DEP(d, index)->member = member;
        }
        if (has_member) {
            /* Only inferred constructors reconcile their needles' requiredness. A schema
             * Array<Nullable<T>> is fixed and must still reject a separately required needle. */
            ParameterFact mf;
            if (DESC(d, haystack.descriptor)->tag == DESC_ARRAY && domain_fact(d, member, &mf) && domain_fact(d, dep.right, &rf) &&
                same_descriptor(d, mf.descriptor, rf.descriptor))
                JZ_TRY(domain_require(d, member, rf));
            if (domain_fact(d, member, &mf)) JZ_TRY(domain_require(d, dep.right, mf));
        }
    } else if (fallbacks == 2 && domain_fact(d, dep.right, &rf) && dep.left.tag == TERM_PARAM) {
        u32 param = dep.left.param;
        u64 epoch = d->notification + 1;
        if (REQ(d, param)->fallback_deferred == epoch) return JZ_OK;
        u32 provider;
        if (ready_forward_provider(d, param, &provider)) {
            REQ(d, param)->fallback_deferred = epoch;
            /* Run the provider immediately instead of letting each blocked consumer rescan its adjacency. */
            DEP(d, provider)->constructor_queued = true;
            dq_push_front(&d->constructors, provider);
            return JZ_OK;
        }
        u32 descriptor = d->descriptors.len;
        ParameterDescriptor *desc = JAZZ_VEC_SLOT(d->descriptors);
        desc->tag = DESC_ARRAY;
        desc->member = dep.right;
        ParameterFact f = {descriptor, true};
        JZ_TRY(domain_require(d, dep.left, f));
    }
    return JZ_OK;
}

static jz_err domain_materialize(const ParameterDomain *d, ParameterFact fact, groove_ValueType *out) {
    const ParameterDescriptor *desc = DESC(d, fact.descriptor);
    groove_ValueType base;
    if (desc->tag == DESC_FIXED) {
        base = vt_clone(&desc->fixed);
    } else {
        ParameterFact mf;
        if (!domain_fact(d, desc->member, &mf)) return mismatch();
        groove_ValueType member;
        JZ_TRY(domain_materialize(d, mf, &member));
        memset(&base, 0, sizeof(base));
        base.tag = GROOVE_VT_ARRAY;
        base.u.inner = JZ_NEW(groove_ValueType);
        *base.u.inner = member;
    }
    *out = fact.required ? base : VT(nullable)(base);
    return JZ_OK;
}

static jz_err domain_finish(ParameterDomain *d, jz_btree *out) {
    /* Seed typed context once. Later phases awaken only deferred opportunities. */
    for (u32 i = 0; i < d->dependencies.len; i++) domain_enqueue(d, i);
    JZ_TRY(drain_pending(d, false, false));
    for (u32 off = 0; off < d->literal_dependencies.len; off++) domain_enqueue(d, d->literal_dependencies.ptr[off]);
    JZ_TRY(drain_pending(d, true, false));
    for (u32 off = 0; off < d->contains_dependencies.len; off++) enqueue_constructor(d, d->contains_dependencies.ptr[off]);
    u32 index;
    while (dq_pop_front(&d->constructors, &index)) {
        DEP(d, index)->constructor_queued = false;
        if (ready_constructor(d, index)) {
            JZ_TRY(settle_dependency(d, index, 2));
            /* Exhaust newly available forward context before the next backward choice. */
            JZ_TRY(drain_pending(d, true, true));
        }
    }
    jz_btree_init(out, &jazz_model__query__PARAM_TYPES_DESC);
    const jz_str *name;
    const u32 *idx;
    JZ_BTREE_FOR(&d->names, name, idx) {
        jz_err e = JZ_OK;
        groove_ValueType ty;
        if (!REQ(d, *idx)->has_fact) e = mismatch();
        else e = domain_materialize(d, REQ(d, *idx)->fact, &ty);
        if (JZ_IS_ERR(e)) { jz_btree_drop(out); return e; }
        jz_str k = jz_str_clone(name);
        jz_btree_insert(out, &k, &ty, NULL);
    }
    return JZ_OK;
}

/* ParameterPhase::{Prepare(&mut ParameterDomain), Check(&BTreeMap<String, ColumnType>)} */
typedef struct ParameterPhase { ParameterDomain *domain; const jz_btree *params; } ParameterPhase;

/* ---------------------------------------------------------------- predicates */
static jz_err validate_predicate(const jazz_model_TableSchema *table, jazz_model_Predicate *p, ParameterPhase *phase);
static jz_err validate_predicates(const jazz_model_TableSchema *table, jazz_model_PredicateVec *ps, ParameterPhase *phase) {
    JZ_VEC_FOR(jazz_model_Predicate, p, *ps) JZ_TRY(validate_predicate(table, p, phase));
    return JZ_OK;
}

static void replace_with_false(jazz_model_Predicate *p) {
    Q(Predicate__drop)(p);
    *p = Q(any_of)((jazz_model_PredicateVec){0});
}

static jz_err validate_comparable_operands(const jazz_model_TableSchema *table, jazz_model_Operand *left, jazz_model_Operand *right,
                                           const jz_btree *params, groove_ValueType *out) {
    /* An empty array literal has no element type of its own. Permit it only when directly paired with
     * a schema-resolved array column. */
    const jz_str *empty_array_column = NULL;
    if (left->tag == JAZZ_OPERAND_LITERAL && left->literal.tag == GROOVE_V_ARRAY && left->literal.u.items.len == 0 && right->tag == JAZZ_OPERAND_COLUMN)
        empty_array_column = &right->name;
    else if (right->tag == JAZZ_OPERAND_LITERAL && right->literal.tag == GROOVE_V_ARRAY && right->literal.u.items.len == 0 && left->tag == JAZZ_OPERAND_COLUMN)
        empty_array_column = &left->name;
    if (empty_array_column) {
        const groove_ValueType *ct;
        JZ_TRY(PCT(table, SL(*empty_array_column), &ct));
        if (ct->tag == GROOVE_VT_ARRAY || (ct->tag == GROOVE_VT_NULLABLE && ct->u.inner->tag == GROOVE_VT_ARRAY)) {
            *out = vt_clone(ct);
            return JZ_OK;
        }
    }
    OptType lt, rt;
    JZ_TRY(operand_type(table, left, params, &lt));
    jz_err e = operand_type(table, right, params, &rt);
    if (JZ_IS_ERR(e)) { opt_type_drop(&lt); return e; }
    if (lt.some && rt.some && !column_types_comparable(&lt.t, &rt.t)) {
        bool coerced = false;
        if (left->tag == JAZZ_OPERAND_LITERAL && right->tag != JAZZ_OPERAND_LITERAL) coerced = coerce_integer_literal_for_type(left, &rt.t);
        else if (right->tag == JAZZ_OPERAND_LITERAL && left->tag != JAZZ_OPERAND_LITERAL) coerced = coerce_integer_literal_for_type(right, &lt.t);
        if (coerced) {
            opt_type_drop(&lt); opt_type_drop(&rt);
            JZ_TRY(operand_type(table, left, params, &lt));
            e = operand_type(table, right, params, &rt);
            if (JZ_IS_ERR(e)) { opt_type_drop(&lt); return e; }
        }
    }
    if (lt.some && rt.some) {
        if (!column_types_comparable(&lt.t, &rt.t)) {
            bool some = false;
            e = claim_type_mismatch_error(left, &lt.t, right, &rt.t, &some);
            if (!some) e = claim_type_mismatch_error(right, &rt.t, left, &lt.t, &some);
            if (!some) e = mismatch();
            opt_type_drop(&lt); opt_type_drop(&rt);
            return e;
        }
        opt_type_drop(&rt);
        *out = lt.t;
        return JZ_OK;
    }
    if (lt.some) {
        e = check_inferred_param(right, &lt.t, params);
        if (JZ_IS_ERR(e)) { opt_type_drop(&lt); return e; }
        *out = lt.t;
        return JZ_OK;
    }
    if (rt.some) {
        e = check_inferred_param(left, &rt.t, params);
        if (JZ_IS_ERR(e)) { opt_type_drop(&rt); return e; }
        *out = rt.t;
        return JZ_OK;
    }
    return mismatch();
}

static jz_err validate_in_check(const jazz_model_TableSchema *table, jazz_model_Predicate *p, const jz_btree *params) {
    OptType lt;
    JZ_TRY(operand_type(table, &p->left, params, &lt));
    if (p->values.len == 0) { opt_type_drop(&lt); replace_with_false(p); return JZ_OK; }
    jz_err e = JZ_OK;
    JZ_VEC_FOR(jazz_model_Operand, value, p->values) {
        OptType vt;
        e = operand_type(table, value, params, &vt);
        if (JZ_IS_ERR(e)) break;
        if (lt.some && vt.some && !in_operand_types_compatible(&lt.t, &vt.t) && p->left.tag != JAZZ_OPERAND_LITERAL &&
            coerce_integer_literal_for_type(value, &lt.t)) {
            opt_type_drop(&vt);
            e = operand_type(table, value, params, &vt);
            if (JZ_IS_ERR(e)) break;
        }
        if (lt.some && vt.some) {
            if (!in_operand_types_compatible(&lt.t, &vt.t) && !in_literal_value_coercible(&lt.t, value))
                e = in_candidate_type_mismatch_error(&p->left, &lt.t, &vt.t);
        } else if (lt.some) {
            e = check_inferred_param(value, &lt.t, params);
        } else if (vt.some) {
            e = check_inferred_param(&p->left, &vt.t, params);
        } else {
            e = mismatch();
        }
        opt_type_drop(&vt);
        if (JZ_IS_ERR(e)) break;
    }
    opt_type_drop(&lt);
    return e;
}

static jz_err validate_contains_check(const jazz_model_TableSchema *table, jazz_model_Predicate *p, const jz_btree *params) {
    OptType lt, rt;
    JZ_TRY(operand_type(table, &p->left, params, &lt));
    jz_err e = operand_type(table, &p->right, params, &rt);
    if (JZ_IS_ERR(e)) { opt_type_drop(&lt); return e; }
    const groove_ValueType *expected = NULL;
    if (!lt.some) {
        if (rt.some) {
            groove_ValueType arr;
            memset(&arr, 0, sizeof(arr));
            arr.tag = GROOVE_VT_ARRAY;
            arr.u.inner = JZ_NEW(groove_ValueType);
            *arr.u.inner = rt.t;
            e = check_inferred_param(&p->left, &arr, params);
            VT(drop)(&arr);
            return e;
        }
        return mismatch();
    }
    const groove_ValueType *haystack = non_null_column_type(&lt.t);
    if (haystack->tag == GROOVE_VT_STRING) expected = &VT_STRING;
    else if (haystack->tag == GROOVE_VT_ARRAY) expected = haystack->u.inner;
    else { opt_type_drop(&lt); opt_type_drop(&rt); return mismatch(); }
    if (rt.some && !vt_eq(&rt.t, expected)) {
        jz_str es = {0}, as = {0};
        VT(Debug__fmt)(&es, expected);
        VT(Debug__fmt)(&as, &rt.t);
        e = Q(QueryError__new)(JAZZ_QE_CONTAINS_NEEDLE_TYPE_MISMATCH, "contains needle has type %.*s; expected %.*s", STRF(as), STRF(es));
        jz_str_drop(&es); jz_str_drop(&as);
    } else if (!rt.some) {
        e = check_inferred_param(&p->right, expected, params);
    }
    opt_type_drop(&lt); opt_type_drop(&rt);
    return e;
}

static jz_err validate_predicate(const jazz_model_TableSchema *table, jazz_model_Predicate *p, ParameterPhase *phase) {
    switch (p->tag) {
    case JAZZ_PRED_ALL: case JAZZ_PRED_ANY: return validate_predicates(table, &p->children, phase);
    case JAZZ_PRED_NOT: return validate_predicate(table, p->inner, phase);
    case JAZZ_PRED_ENUM_MATCH: {
        const groove_ValueType *ct;
        JZ_TRY(PCT(table, SL(p->column), &ct));
        const groove_ValueType *nn = non_null_column_type(ct);
        if (nn->tag != GROOVE_VT_ENUM) return mismatch();
        u32 tag;
        jz_err e = groove__records__values__EnumSchema__tag(nn->u.enum_, p->case_.ptr, p->case_.len, &tag);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); return mismatch(); }
        const groove_EnumCase *enum_case;
        e = groove__records__values__EnumSchema__case(nn->u.enum_, tag, &enum_case);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); return mismatch(); }
        u32 n = 0;
        const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(enum_case->payload, &n);
        jazz_model_ColumnSchemaVec columns = {0};
        for (u32 i = 0; i < n; i++) {
            jazz_model_ColumnSchema c = jazz_model__schema__ColumnSchema__new(fields[i].has_name ? jz_str_clone(&fields[i].name) : (jz_str){0},
                                                                              vt_clone(&fields[i].value_type));
            JZ_VEC_PUSH(columns, c);
        }
        jazz_model_TableSchema payload_table = jazz_model__schema__TableSchema__new(jz_str_from("__enum_payload"), columns);
        e = validate_predicate(&payload_table, p->inner, phase);
        jazz_model__schema__TableSchema__drop(&payload_table);
        return e;
    }
    default: break;
    }
    if (phase->domain) {
        ParameterDomain *d = phase->domain;
        switch (p->tag) {
        case JAZZ_PRED_EQ: case JAZZ_PRED_NE: case JAZZ_PRED_GT: case JAZZ_PRED_GTE: case JAZZ_PRED_LT: case JAZZ_PRED_LTE:
            return domain_relationship(d, table, &p->left, &p->right, REL_COMPARE);
        case JAZZ_PRED_IN:
            if (p->values.len == 0) {
                OptType t;
                JZ_TRY(operand_type(table, &p->left, NULL, &t));
                opt_type_drop(&t);
                replace_with_false(p);
                return JZ_OK;
            }
            JZ_VEC_FOR(const jazz_model_Operand, right, p->values) JZ_TRY(domain_relationship(d, table, &p->left, right, REL_IN));
            return JZ_OK;
        case JAZZ_PRED_CONTAINS: return domain_relationship(d, table, &p->left, &p->right, REL_CONTAINS);
        case JAZZ_PRED_IS_NULL:
            if (p->left.tag == JAZZ_OPERAND_PARAM) domain_parameter(d, SL(p->left.name));
            return JZ_OK;
        default: JZ_UNREACHABLE(); /* recursive predicates handled by the shared traversal */
        }
    }
    const jz_btree *params = phase->params;
    groove_ValueType ct;
    switch (p->tag) {
    case JAZZ_PRED_EQ: case JAZZ_PRED_NE:
        JZ_TRY(validate_comparable_operands(table, &p->left, &p->right, params, &ct));
        VT(drop)(&ct);
        return JZ_OK;
    case JAZZ_PRED_IN: return validate_in_check(table, p, params);
    case JAZZ_PRED_GT: case JAZZ_PRED_GTE: case JAZZ_PRED_LT: case JAZZ_PRED_LTE: {
        JZ_TRY(validate_comparable_operands(table, &p->left, &p->right, params, &ct));
        bool ok = is_orderable(&ct);
        VT(drop)(&ct);
        return ok ? JZ_OK : mismatch();
    }
    case JAZZ_PRED_CONTAINS: return validate_contains_check(table, p, params);
    case JAZZ_PRED_IS_NULL: {
        /* Claims are dynamically typed and become literals when binding the session. */
        if (p->left.tag == JAZZ_OPERAND_CLAIM || p->left.tag == JAZZ_OPERAND_LITERAL) return JZ_OK;
        OptType t;
        JZ_TRY(operand_type(table, &p->left, params, &t));
        bool ok = t.some && t.t.tag == GROOVE_VT_NULLABLE;
        opt_type_drop(&t);
        return ok ? JZ_OK : mismatch();
    }
    default: JZ_UNREACHABLE();
    }
}

/* ---------------------------------------------------------------- joins / reachability / includes */
static jz_err validate_join(const jazz_model_RuntimeSchema *schema, const jazz_model_TableSchema *root, jz_slice root_table,
                            jazz_model_JoinVia *join, ParameterPhase *phase);

/* Validates a join's own filters, then its nested joins. */
static jz_err validate_join_conditions(const jazz_model_RuntimeSchema *schema, const jazz_model_TableSchema *join_table,
                                       jazz_model_JoinVia *join, ParameterPhase *phase) {
    JZ_TRY(validate_predicates(join_table, &join->filters, phase));
    JZ_VEC_FOR(jazz_model_JoinVia, nested, join->nested_joins) JZ_TRY(validate_join(schema, join_table, SL(join->table), nested, phase));
    return JZ_OK;
}

static const jz_str *ref_target(const jazz_model_TableSchema *t, const jz_str *column) { return jz_btree_get(&t->references, column); }

static jz_err validate_join(const jazz_model_RuntimeSchema *schema, const jazz_model_TableSchema *root, jz_slice root_table,
                            jazz_model_JoinVia *join, ParameterPhase *phase) {
    const jazz_model_TableSchema *join_table;
    const groove_ValueType *t1, *t2;
    JZ_TRY(Q(schema_table)(schema, SL(join->table), &join_table));
    if (join->target == JAZZ_JOIN_TARGET_UNCORRELATED) {
        if (join->on_column.len || join->source_column.some || join->source_lookup || join->correlated_filters.len)
            return not_ref_compatible(SL(join->table), SL(join->on_column), CS("uncorrelated existence (no join keys)"));
        return validate_join_conditions(schema, join_table, join, phase);
    }
    if (join->target == JAZZ_JOIN_TARGET_COLUMN) JZ_TRY(PCT(join_table, SL(join->on_column), &t1));
    else if (!jz_str_eq_cstr(&join->on_column, "id")) return unknown_column(SL(join->table), SL(join->on_column));

    /* Explicit non-reference column equalities are ordinary existence joins. */
    if (join->target == JAZZ_JOIN_TARGET_COLUMN && !join->source_lookup && join->source_column.some &&
        !ref_target(root, &join->source_column.s) && !ref_target(join_table, &join->on_column)) {
        const groove_ValueType *source_type, *target_type;
        JZ_TRY(PCT(root, SL(join->source_column.s), &source_type));
        JZ_TRY(PCT(join_table, SL(join->on_column), &target_type));
        u8 st = non_null_column_type(source_type)->tag, tt = non_null_column_type(target_type)->tag;
        if (st != GROOVE_VT_UUID && st != GROOVE_VT_ARRAY && tt != GROOVE_VT_UUID && tt != GROOVE_VT_ARRAY) {
            if (!column_types_comparable(source_type, target_type)) return mismatch();
            JZ_VEC_FOR(const jazz_model_JoinCorrelation, c, join->correlated_filters) {
                JZ_TRY(PCT(root, SL(c->source_column), &t1));
                JZ_TRY(PCT(join_table, SL(c->join_column), &t2));
                if (!column_types_comparable(t1, t2)) return mismatch();
            }
            return validate_join_conditions(schema, join_table, join, phase);
        }
    }
    const jz_str *target_table;
    if (join->source_lookup) {
        const jazz_model_JoinSourceLookup *lookup = join->source_lookup;
        const jazz_model_TableSchema *lookup_table;
        JZ_TRY(PCT(root, SL(lookup->row_id_source_column), &t1));
        JZ_TRY(Q(schema_table)(schema, SL(lookup->table), &lookup_table));
        const jz_str *target = ref_target(root, &lookup->row_id_source_column);
        if (!target || !jz_str_eq(target, &lookup->table))
            return not_ref_compatible(root_table, SL(lookup->row_id_source_column), SL(lookup->table));
        JZ_TRY(PCT(lookup_table, SL(lookup->value_column), &t1));
        if (!join->source_column.some || !jz_str_eq(&join->source_column.s, &lookup->value_column))
            return not_ref_compatible(SL(lookup->table), SL(lookup->value_column), CS("join source column"));
        if (jz_str_eq_cstr(&lookup->value_column, "id")) {
            target_table = &lookup->table;
        } else {
            target_table = ref_target(lookup_table, &lookup->value_column);
            if (!target_table) return not_ref_compatible(SL(lookup->table), SL(lookup->value_column), CS("referenced table"));
        }
    } else if (join->source_column.some) {
        if (jz_str_eq_cstr(&join->source_column.s, "id")) {
            target_table = NULL; /* root_table */
        } else {
            JZ_TRY(PCT(root, SL(join->source_column.s), &t1));
            target_table = ref_target(root, &join->source_column.s);
            if (!target_table) return not_ref_compatible(root_table, SL(join->source_column.s), CS("referenced table"));
        }
    } else {
        target_table = NULL;
    }
    jz_slice target = target_table ? SL(*target_table) : root_table;
    /* A correlation is an equality: a NULL side simply never matches. */
    JZ_VEC_FOR(const jazz_model_JoinCorrelation, c, join->correlated_filters) {
        JZ_TRY(PCT(root, SL(c->source_column), &t1));
        JZ_TRY(PCT(join_table, SL(c->join_column), &t2));
        if (!column_types_comparable(t1, t2)) return mismatch();
    }
    if (join->target == JAZZ_JOIN_TARGET_COLUMN) {
        const jz_str *ref = ref_target(join_table, &join->on_column);
        bool ok = ref ? jz_slice_cmp(SL(*ref), target) == 0
                      : jz_str_eq_cstr(&join->on_column, "id") && jz_slice_cmp(SL(join->table), target) == 0;
        if (!ok) return not_ref_compatible(SL(join->table), SL(join->on_column), target);
    } else if (jz_slice_cmp(SL(join->table), target) != 0) {
        return not_ref_compatible(SL(join->table), SL(join->on_column), target);
    }
    return validate_join_conditions(schema, join_table, join, phase);
}

static jz_err validate_reachable(const jazz_model_RuntimeSchema *schema, const jazz_model_TableSchema *root, jazz_model_ReachableVia *r,
                                 ParameterPhase *phase) {
    const jazz_model_TableSchema *access;
    const groove_ValueType *t;
    JZ_TRY(Q(schema_table)(schema, SL(r->access_table), &access));
    JZ_TRY(PCT(access, SL(r->access_row_column), &t));
    JZ_TRY(PCT(access, SL(r->access_team_column), &t));
    const groove_ValueType *root_key_type = &VT_UUID;
    if (jz_str_eq_cstr(&r->access_row_column, "id")) {
        if (!jz_str_eq(&access->name, &root->name)) return not_ref_compatible(SL(r->access_table), SL(r->access_row_column), SL(root->name));
    } else if (jz_str_eq(&access->name, &root->name)) {
        JZ_TRY(PCT(access, SL(r->access_row_column), &t));
        if (!column_types_comparable(t, root_key_type)) return not_ref_compatible(SL(r->access_table), SL(r->access_row_column), SL(root->name));
    } else {
        const jz_str *target = ref_target(access, &r->access_row_column);
        if (!target || !jz_str_eq(target, &root->name)) return not_ref_compatible(SL(r->access_table), SL(r->access_row_column), SL(root->name));
        JZ_TRY(PCT(access, SL(r->access_row_column), &t));
        if (!column_types_comparable(t, root_key_type)) return mismatch();
    }
    const jz_str *team_table;
    switch (r->access_team_target) {
    case JAZZ_JOIN_TARGET_UNCORRELATED:
        return not_ref_compatible(SL(r->access_table), SL(r->access_team_column), CS("reachability requires a team reference"));
    case JAZZ_JOIN_TARGET_COLUMN:
        team_table = ref_target(access, &r->access_team_column);
        if (!team_table) return not_ref_compatible(SL(r->access_table), SL(r->access_team_column), CS("referenced table"));
        break;
    default:
        if (!jz_str_eq_cstr(&r->access_team_column, "id"))
            return not_ref_compatible(SL(r->access_table), SL(r->access_team_column), SL(r->access_table));
        team_table = &access->name;
        break;
    }
    const jazz_model_TableSchema *edge;
    JZ_TRY(Q(schema_table)(schema, SL(r->edge_table), &edge));
    const jz_str *edge_columns[2] = {&r->edge_member_column, &r->edge_parent_column};
    for (u32 i = 0; i < 2; i++) {
        const jz_str *column = edge_columns[i];
        JZ_TRY(PCT(edge, SL(*column), &t));
        if (jz_str_eq_cstr(column, "id") && jz_str_eq(&edge->name, team_table)) continue;
        const jz_str *target = ref_target(edge, column);
        if (!target || !jz_str_eq(target, team_table)) return not_ref_compatible(SL(r->edge_table), SL(*column), SL(*team_table));
    }
    if (r->seed) {
        jazz_model_ReachableSeed *seed = r->seed;
        const jazz_model_TableSchema *seed_table;
        JZ_TRY(Q(schema_table)(schema, SL(seed->table), &seed_table));
        JZ_TRY(PCT(seed_table, SL(seed->team_column), &t));
        if (!vt_eq(t, &VT_UUID)) return mismatch();
        if (seed->user_column.some) JZ_TRY(PCT(seed_table, SL(seed->user_column.s), &t));
        bool projects_team;
        if (jz_str_eq_cstr(&seed->team_column, "id")) projects_team = jz_str_eq(&seed_table->name, team_table);
        else { const jz_str *target = ref_target(seed_table, &seed->team_column); projects_team = target && jz_str_eq(target, team_table); }
        if (!projects_team) return not_ref_compatible(SL(seed->table), SL(seed->team_column), SL(*team_table));
        JZ_TRY(validate_predicates(seed_table, &seed->filters, phase));
    } else if (phase->domain) {
        if (r->from.tag == JAZZ_OPERAND_PARAM) {
            ParameterTerm parameter = domain_parameter(phase->domain, SL(r->from.name));
            ParameterTerm known = domain_known(phase->domain, VT_UUID);
            JZ_TRY(domain_require(phase->domain, parameter, known.fact));
        }
    } else {
        OptType ft;
        JZ_TRY(operand_type(root, &r->from, phase->params, &ft));
        jz_err e = JZ_OK;
        if (!ft.some) e = check_inferred_param(&r->from, &VT_UUID, phase->params);
        else if (ft.t.tag != GROOVE_VT_UUID) e = mismatch();
        opt_type_drop(&ft);
        JZ_TRY(e);
    }
    JZ_TRY(validate_predicates(access, &r->access_filters, phase));
    return validate_predicates(edge, &r->edge_filters, phase);
}

static jz_err validate_inherits(const jazz_model_TableSchema *root, const jazz_model_InheritsVia *inherits) {
    const groove_ValueType *t;
    JZ_TRY(PCT(root, SL(inherits->parent_column), &t));
    if (!ref_target(root, &inherits->parent_column)) return not_ref_compatible(SL(root->name), SL(inherits->parent_column), CS("referenced table"));
    return JZ_OK;
}

static jz_err validate_select_column(const jazz_model_TableSchema *table, jz_slice column) {
    if (slice_is(column, "id")) return JZ_OK;
    const groove_ValueType *t;
    JZ_TRY(executable_magic_column_type(column, &t));
    if (t) return JZ_OK;
    if (column.len && column.ptr[0] == '$') return unknown_column(SL(table->name), column);
    return column_type(table, column, &t);
}

static jz_err validate_include(const jazz_model_RuntimeSchema *schema, const jazz_model_TableSchema *root, const jz_str *path) {
    const jazz_model_TableSchema *current = root;
    u32 start = 0;
    for (u32 i = 0; i <= path->len; i++) {
        if (i < path->len && path->ptr[i] != '.') continue;
        jz_slice segment = jz_slice_of(path->ptr + start, i - start);
        start = i + 1;
        const groove_ValueType *t;
        JZ_TRY(column_type(current, segment, &t));
        jz_str key = jz_str_from_slice(segment);
        const jz_str *target = jz_btree_get(&current->references, &key);
        jz_str_drop(&key);
        if (!target) return Q(QueryError__new)(JAZZ_QE_BAD_INCLUDE_PATH, "bad include path %.*s", STRF(*path));
        JZ_TRY(Q(schema_table)(schema, SL(*target), &current));
    }
    return JZ_OK;
}

static jz_err reject_author_ordering(const jazz_model_OrderByVec *order_by) {
    JZ_VEC_FOR(const jazz_model_OrderBy, o, *order_by) {
        if (jz_str_eq_cstr(&o->column, "$createdBy") || jz_str_eq_cstr(&o->column, "$updatedBy"))
            return Q(QueryError__new)(JAZZ_QE_UNSUPPORTED_AUTHOR_ORDERING, "ordering by author provenance column %.*s is unsupported", STRF(o->column));
    }
    return JZ_OK;
}

static jz_err bad_include_path(const jz_str *path) { return Q(QueryError__new)(JAZZ_QE_BAD_INCLUDE_PATH, "bad include path %.*s", STRF(*path)); }

static jz_err validate_array_subquery(const jazz_model_RuntimeSchema *schema, const jazz_model_TableSchema *parent, jazz_model_ArraySubquery *s,
                                      ParameterPhase *phase, const jz_str *relation_path) {
    const jazz_model_TableSchema *child;
    const groove_ValueType *pt, *ct;
    JZ_TRY(Q(schema_table)(schema, SL(s->table), &child));
    JZ_TRY(PCT(parent, SL(s->outer_column), &pt));
    JZ_TRY(PCT(child, SL(s->inner_column), &ct));
    if (!array_correlation_types_compatible(pt, ct)) return mismatch();
    JZ_TRY(validate_predicates(child, &s->filters, phase));
    if (s->has_select) JZ_VEC_FOR(const jz_str, c, s->select) JZ_TRY(validate_select_column(child, SL(*c)));
    JZ_TRY(reject_author_ordering(&s->order_by));
    JZ_VEC_FOR(const jazz_model_OrderBy, o, s->order_by) JZ_TRY(PCT(child, SL(o->column), &ct));
    jz_btree names;
    jz_btree_init(&names, &jazz_model__STR_SET_DESC);
    jz_err e = JZ_OK;
    JZ_VEC_FOR(jazz_model_ArraySubquery, nested, s->nested_arrays) {
        jz_str k = jz_str_clone(&nested->column_name);
        if (nested->column_name.len == 0 || jz_btree_insert(&names, &k, NULL, NULL)) { e = bad_include_path(&nested->column_name); break; }
        jz_str nested_path = jz_format("%.*s.%.*s", STRF(*relation_path), STRF(nested->column_name));
        e = validate_array_subquery(schema, child, nested, phase, &nested_path);
        jz_str_drop(&nested_path);
        if (JZ_IS_ERR(e)) break;
    }
    jz_btree_drop(&names);
    return e;
}

static jz_err validate_array_subqueries(const jazz_model_RuntimeSchema *schema, const jazz_model_TableSchema *parent,
                                        jazz_model_ArraySubqueryVec *subqueries, ParameterPhase *phase) {
    jz_btree names;
    jz_btree_init(&names, &jazz_model__STR_SET_DESC);
    jz_err e = JZ_OK;
    JZ_VEC_FOR(const jazz_model_ArraySubquery, s, *subqueries) {
        jz_str k = jz_str_clone(&s->column_name);
        if (s->column_name.len == 0 || jz_btree_insert(&names, &k, NULL, NULL)) { e = bad_include_path(&s->column_name); break; }
    }
    jz_btree_drop(&names);
    JZ_TRY(e);
    JZ_VEC_FOR(jazz_model_ArraySubquery, s, *subqueries) {
        jz_str relation_path = jz_str_clone(&s->column_name);
        e = validate_array_subquery(schema, parent, s, phase, &relation_path);
        jz_str_drop(&relation_path);
        JZ_TRY(e);
    }
    return JZ_OK;
}

static jz_err validate_aggregate(const jazz_model_TableSchema *table, const jazz_model_AggregateQuery *aggregate) {
    const groove_ValueType *t;
    if (aggregate->group_by.some) JZ_TRY(PCT(table, SL(aggregate->group_by.s), &t));
    JZ_VEC_FOR(const jazz_model_Aggregate, a, aggregate->aggregates) {
        if (a->alias.len >= 17 && memcmp(a->alias.ptr, "__jazz_aggregate_", 17) == 0)
            return Q(QueryError__new)(JAZZ_QE_RESERVED_AGGREGATE_ALIAS, "aggregate alias %.*s uses a reserved compiler namespace", STRF(a->alias));
        switch (a->function) {
        case JAZZ_AGG_COUNT:
            if (a->column.some) JZ_TRY(column_type(table, SL(a->column.s), &t));
            break;
        case JAZZ_AGG_SUM: case JAZZ_AGG_AVG:
            if (!a->column.some) return mismatch();
            JZ_TRY(column_type(table, SL(a->column.s), &t));
            if (!is_numeric(t)) return mismatch();
            break;
        default: /* Min | Max */
            if (!a->column.some) return mismatch();
            JZ_TRY(column_type(table, SL(a->column.s), &t));
            if (!is_orderable(t)) return mismatch();
            break;
        }
    }
    /* Keep all existing aggregate errors ahead of output-name collisions. */
    jz_btree names;
    jz_btree_init(&names, &jazz_model__STR_SET_DESC);
    if (aggregate->group_by.some) { jz_str k = jz_str_clone(&aggregate->group_by.s); jz_btree_insert(&names, &k, NULL, NULL); }
    jz_err e = JZ_OK;
    JZ_VEC_FOR(const jazz_model_Aggregate, a, aggregate->aggregates) {
        jz_str k = jz_str_clone(&a->alias);
        if (jz_btree_insert(&names, &k, NULL, NULL)) {
            e = Q(QueryError__new)(JAZZ_QE_AGGREGATE_OUTPUT_NAME_COLLISION, "aggregate output name %.*s is used more than once", STRF(a->alias));
            break;
        }
    }
    jz_btree_drop(&names);
    return e;
}

static jz_err validate_aggregate_order_by(const jz_str *table, const jazz_model_AggregateQuery *aggregate, const jazz_model_OrderByVec *order_by) {
    JZ_VEC_FOR(const jazz_model_OrderBy, o, *order_by) {
        bool is_group_by = aggregate->group_by.some && jz_str_eq(&aggregate->group_by.s, &o->column);
        bool is_aggregate = false;
        JZ_VEC_FOR(const jazz_model_Aggregate, a, aggregate->aggregates) if (jz_str_eq(&a->alias, &o->column)) { is_aggregate = true; break; }
        if (!is_group_by && !is_aggregate) {
            jz_str t = jz_format("%.*s_aggregate", STRF(*table));
            jz_err e = unknown_column(SL(t), SL(o->column));
            jz_str_drop(&t);
            return e;
        }
    }
    return JZ_OK;
}

/* ---------------------------------------------------------------- flat joins */
static void ptr_drop_v(void *p) { (void)p; }
static const jz_btree_desc STR_TABLE_REF_DESC = {sizeof(jz_str), sizeof(void *), jz_cmp_str, jazz_model__str_drop_v, ptr_drop_v};

static jz_str flat_join_source_name(const jz_str *table, const jazz_model_OptStr *alias) {
    return alias->some ? jz_str_clone(&alias->s) : jz_str_clone(table);
}

static jz_err flat_join_qualified_field(jz_slice field, jz_slice *scope, jz_slice *rest) {
    for (u32 i = 0; i + 1 < field.len; i++) {
        if (field.ptr[i] == '.' && field.ptr[i + 1] == '$') {
            *scope = jz_slice_of(field.ptr, i);
            *rest = jz_slice_of(field.ptr + i + 1, field.len - i - 1);
            return JZ_OK;
        }
    }
    for (u32 i = field.len; i-- > 0;) {
        if (field.ptr[i] == '.') {
            *scope = jz_slice_of(field.ptr, i);
            *rest = jz_slice_of(field.ptr + i + 1, field.len - i - 1);
            return JZ_OK;
        }
    }
    return unknown_column(CS("flat join qualified source"), field);
}

static jz_err flat_join_column_type(const jazz_model_TableSchema *table, jz_slice column, const groove_ValueType **out) {
    if (slice_is(column, "_id")) { *out = &VT_UUID; return JZ_OK; }
    return PCT(table, column, out);
}

static jz_err flat_join_source_tables(const jazz_model_RuntimeSchema *schema, const jz_str *root_table, const jazz_model_FlatJoin *fj, jz_btree *sources) {
    jz_btree_init(sources, &STR_TABLE_REF_DESC);
    const jazz_model_TableSchema *t;
    jz_err e = Q(schema_table)(schema, SL(*root_table), &t);
    if (JZ_IS_ERR(e)) goto fail;
    jz_str root_name = flat_join_source_name(root_table, &fj->root_alias);
    jz_btree_insert(sources, &root_name, &t, NULL);
    JZ_VEC_FOR(const jazz_model_FlatJoinSource, s, fj->sources) {
        jz_str name = flat_join_source_name(&s->table, &s->alias);
        e = Q(schema_table)(schema, SL(s->table), &t);
        if (JZ_IS_ERR(e)) { jz_str_drop(&name); goto fail; }
        jz_str k = jz_str_clone(&name);
        if (jz_btree_insert(sources, &k, &t, NULL)) {
            e = unknown_column(CS("flat join duplicate source"), SL(name));
            jz_str_drop(&name);
            goto fail;
        }
        jz_str_drop(&name);
    }
    return JZ_OK;
fail:
    jz_btree_drop(sources);
    return e;
}

static jz_err flat_join_filter_schema(const jz_btree *sources, jazz_model_TableSchema *out) {
    static const char *const MAGIC[] = {"$createdBy", "$updatedBy", "$createdAt", "$updatedAt"};
    jazz_model_ColumnSchemaVec columns = {0};
    const jz_str *scope;
    const jazz_model_TableSchema *const *table;
    JZ_BTREE_FOR(sources, scope, table) {
        /* Both public identity spellings resolve to the physical row UUID. */
        jazz_model_ColumnSchema c = jazz_model__schema__ColumnSchema__new(jz_format("%.*s._id", STRF(*scope)), VT_UUID);
        JZ_VEC_PUSH(columns, c);
        c = jazz_model__schema__ColumnSchema__new(jz_format("%.*s.id", STRF(*scope)), VT_UUID);
        JZ_VEC_PUSH(columns, c);
        for (u32 i = 0; i < 4; i++) {
            const groove_ValueType *t;
            jz_err e = executable_magic_column_type(CS(MAGIC[i]), &t);
            if (JZ_IS_ERR(e)) { jz_vec_drop(&columns, sizeof(jazz_model_ColumnSchema), NULL); return e; }
            JZ_ASSERT(t, "listed flat-join magic columns are executable");
            c = jazz_model__schema__ColumnSchema__new(jz_format("%.*s.%s", STRF(*scope), MAGIC[i]), vt_clone(t));
            JZ_VEC_PUSH(columns, c);
        }
        JZ_VEC_FOR(const jazz_model_ColumnSchema, col, (*table)->columns) {
            c = jazz_model__schema__ColumnSchema__new(jz_format("%.*s.%.*s", STRF(*scope), STRF(col->name)), vt_clone(&col->column_type));
            JZ_VEC_PUSH(columns, c);
        }
    }
    *out = jazz_model__schema__TableSchema__new(jz_str_from("flat join"), columns);
    return JZ_OK;
}

typedef jz_err (*RewriteFn)(void *ctx, jz_slice column, jz_str *out);
static jz_err rewrite_operand(jazz_model_Operand *operand, RewriteFn rewrite, void *ctx) {
    if (operand->tag != JAZZ_OPERAND_COLUMN) return JZ_OK;
    jz_str next;
    JZ_TRY(rewrite(ctx, SL(operand->name), &next));
    jz_str_drop(&operand->name);
    operand->name = next;
    return JZ_OK;
}
static jz_err rewrite_flat_join_predicate_columns(jazz_model_Predicate *p, RewriteFn rewrite, void *ctx) {
    switch (p->tag) {
    case JAZZ_PRED_ALL: case JAZZ_PRED_ANY:
        JZ_VEC_FOR(jazz_model_Predicate, c, p->children) JZ_TRY(rewrite_flat_join_predicate_columns(c, rewrite, ctx));
        return JZ_OK;
    case JAZZ_PRED_NOT: return rewrite_flat_join_predicate_columns(p->inner, rewrite, ctx);
    case JAZZ_PRED_IN:
        JZ_TRY(rewrite_operand(&p->left, rewrite, ctx));
        JZ_VEC_FOR(jazz_model_Operand, o, p->values) JZ_TRY(rewrite_operand(o, rewrite, ctx));
        return JZ_OK;
    case JAZZ_PRED_ENUM_MATCH: {
        jz_str next;
        JZ_TRY(rewrite(ctx, SL(p->column), &next));
        jz_str_drop(&p->column);
        p->column = next;
        return JZ_OK;
    }
    case JAZZ_PRED_IS_NULL: return rewrite_operand(&p->left, rewrite, ctx);
    default: /* Eq | Ne | Gt | Gte | Lt | Lte | Contains */
        JZ_TRY(rewrite_operand(&p->left, rewrite, ctx));
        return rewrite_operand(&p->right, rewrite, ctx);
    }
}

static jz_err qualify_flat_join_column(void *ctx, jz_slice column, jz_str *out) {
    const jz_btree *sources = ctx;
    jz_slice scope, field;
    const groove_ValueType *t;
    jz_err e = flat_join_qualified_field(column, &scope, &field);
    if (!JZ_IS_ERR(e)) {
        jz_str key = jz_str_from_slice(scope);
        const jazz_model_TableSchema *const *table = jz_btree_get(sources, &key);
        jz_str_drop(&key);
        if (!table) return unknown_column(CS("flat join source"), scope);
        JZ_TRY(flat_join_column_type(*table, field, &t));
        *out = jz_str_from_slice(column);
        return JZ_OK;
    }
    jz_err_drop(&e);
    const jz_str *match = NULL;
    const jz_str *name;
    const jazz_model_TableSchema *const *table;
    JZ_BTREE_FOR(sources, name, table) {
        jz_err te = flat_join_column_type(*table, column, &t);
        if (JZ_IS_ERR(te)) { jz_err_drop(&te); continue; }
        if (match) return unknown_column(CS("ambiguous flat join column; qualify its source"), column);
        match = name;
    }
    if (!match) return unknown_column(CS("flat join"), column);
    *out = jz_format("%.*s.%.*s", STRF(*match), SLF(column));
    return JZ_OK;
}

static jz_err qualify_flat_join_predicate(jazz_model_Predicate *p, const jz_btree *sources) {
    return rewrite_flat_join_predicate_columns(p, qualify_flat_join_column, (void *)sources);
}

static jz_err collect_source(void *ctx, jz_slice column, jz_str *out) {
    jz_btree *sources = ctx;
    jz_slice scope, field;
    JZ_TRY(flat_join_qualified_field(column, &scope, &field));
    jz_str k = jz_str_from_slice(scope);
    jz_btree_insert(sources, &k, NULL, NULL);
    *out = jz_str_from_slice(column);
    return JZ_OK;
}

jz_err Q(flat_join_predicate_sources)(const jazz_model_Predicate *predicate, jz_btree *out) {
    jazz_model_Predicate p;
    Q(Predicate__Clone__clone)(&p, predicate);
    jz_btree_init(out, &jazz_model__STR_SET_DESC);
    jz_err e = rewrite_flat_join_predicate_columns(&p, collect_source, out);
    Q(Predicate__drop)(&p);
    if (JZ_IS_ERR(e)) jz_btree_drop(out);
    return e;
}

static jz_err unqualify_column(void *ctx, jz_slice column, jz_str *out) {
    const jz_slice *expected_scope = ctx;
    jz_slice scope, field;
    JZ_TRY(flat_join_qualified_field(column, &scope, &field));
    if (jz_slice_cmp(scope, *expected_scope) != 0) return flat_join_unsupported("filters spanning multiple sources");
    *out = jz_str_from_slice(field);
    return JZ_OK;
}

jz_err Q(unqualify_flat_join_predicate)(const jazz_model_Predicate *predicate, jz_slice expected_scope, jazz_model_Predicate *out) {
    Q(Predicate__Clone__clone)(out, predicate);
    jz_err e = rewrite_flat_join_predicate_columns(out, unqualify_column, &expected_scope);
    if (JZ_IS_ERR(e)) Q(Predicate__drop)(out);
    return e;
}

static jz_err validate_flat_join_filter_routing(const jazz_model_Predicate *p) {
    if (p->tag == JAZZ_PRED_ALL) {
        JZ_VEC_FOR(const jazz_model_Predicate, c, p->children) JZ_TRY(validate_flat_join_filter_routing(c));
        return JZ_OK;
    }
    jz_btree sources;
    JZ_TRY(Q(flat_join_predicate_sources)(p, &sources));
    u32 n = sources.len;
    jz_btree_drop(&sources);
    if (n > 1) return flat_join_unsupported("filters spanning multiple sources");
    return JZ_OK;
}

static jz_err validate_flat_join(const jazz_model_RuntimeSchema *schema, const jz_str *root_table, const jazz_model_FlatJoin *fj) {
    jz_btree sources;
    jz_btree_init(&sources, &jazz_model__schema__STR_STR_DESC);
    jz_str root_name = flat_join_source_name(root_table, &fj->root_alias);
    jz_str root_value = jz_str_clone(root_table);
    jz_btree_insert(&sources, &root_name, &root_value, NULL);
    jz_err e = JZ_OK;
    JZ_VEC_FOR(const jazz_model_FlatJoinSource, s, fj->sources) {
        jz_str name = flat_join_source_name(&s->table, &s->alias);
        const jazz_model_TableSchema *left_schema, *right_schema;
        jz_slice left_source, left_column, right_source, right_column;
        const groove_ValueType *lt, *rt;
        const jz_str *left_table;
        if (jz_btree_contains(&sources, &name)) { e = unknown_column(CS("flat join duplicate source"), SL(name)); goto next; }
        if (JZ_IS_ERR(e = Q(schema_table)(schema, SL(s->table), &right_schema))) goto next;
        if (JZ_IS_ERR(e = flat_join_qualified_field(SL(s->on.left), &left_source, &left_column))) goto next;
        if (JZ_IS_ERR(e = flat_join_qualified_field(SL(s->on.right), &right_source, &right_column))) goto next;
        {
            jz_str key = jz_str_from_slice(left_source);
            left_table = jz_btree_get(&sources, &key);
            jz_str_drop(&key);
        }
        if (!left_table) { e = unknown_column(CS("flat join accumulated source"), left_source); goto next; }
        if (jz_slice_cmp(right_source, SL(name)) != 0) { e = unknown_column(CS("flat join source"), right_source); goto next; }
        if (JZ_IS_ERR(e = Q(schema_table)(schema, SL(*left_table), &left_schema))) goto next;
        if (JZ_IS_ERR(e = flat_join_column_type(left_schema, left_column, &lt))) goto next;
        if (JZ_IS_ERR(e = flat_join_column_type(right_schema, right_column, &rt))) goto next;
        if (!flat_join_key_types_compatible(lt, rt)) { e = mismatch(); goto next; }
        {
            jz_str v = jz_str_clone(&s->table);
            jz_btree_insert(&sources, &name, &v, NULL);
            continue;
        }
    next:
        jz_str_drop(&name);
        break;
    }
    jz_btree_drop(&sources);
    return e;
}

/* ---------------------------------------------------------------- parameter contexts */
static jz_err validate_parameter_contexts(jazz_model_Query *q, const jazz_model_RuntimeSchema *schema, const jazz_model_TableSchema *root,
                                          const jazz_model_TableSchema *filter_schema, ParameterPhase *phase) {
    JZ_VEC_FOR(jazz_model_JoinVia, join, q->joins) JZ_TRY(validate_join(schema, root, SL(q->table), join, phase));
    JZ_TRY(validate_predicates(filter_schema, &q->filters, phase));
    JZ_VEC_FOR(jazz_model_ReachableVia, r, q->reachable) JZ_TRY(validate_reachable(schema, root, r, phase));
    JZ_VEC_FOR(jazz_model_PolicyBranch, branch, q->policy_branches) {
        JZ_TRY(validate_predicates(root, &branch->filters, phase));
        JZ_VEC_FOR(jazz_model_JoinVia, join, branch->joins) JZ_TRY(validate_join(schema, root, SL(q->table), join, phase));
        JZ_VEC_FOR(jazz_model_ReachableVia, r, branch->reachable) JZ_TRY(validate_reachable(schema, root, r, phase));
    }
    return validate_array_subqueries(schema, root, &q->array_subqueries, phase);
}

static jz_err prepare_parameter_domain(jazz_model_Query *q, const jazz_model_RuntimeSchema *schema, const jazz_model_TableSchema *root,
                                       const jazz_model_TableSchema *filter_schema, const jz_btree *initial, jz_btree *out) {
    ParameterDomain domain;
    domain_init(&domain);
    jz_err e = JZ_OK;
    const jz_str *name;
    const groove_ValueType *ty;
    JZ_BTREE_FOR(initial, name, ty) {
        ParameterTerm parameter = domain_parameter(&domain, SL(*name));
        ParameterTerm known = domain_known(&domain, vt_clone(ty));
        e = domain_require(&domain, parameter, known.fact);
        if (JZ_IS_ERR(e)) { domain_drop(&domain); return e; }
    }
    ParameterPhase prepare = {&domain, NULL};
    e = validate_parameter_contexts(q, schema, root, filter_schema, &prepare);
    if (!JZ_IS_ERR(e)) e = domain_finish(&domain, out);
    domain_drop(&domain);
    JZ_TRY(e);
    /* UNION arms remain independent and their merged domain is exact. Outer presentation must not
     * silently narrow an arm's published carrier. */
    JZ_BTREE_FOR(initial, name, ty) {
        const groove_ValueType *got = jz_btree_get(out, name);
        if (!got || !vt_eq(got, ty)) { jz_btree_drop(out); return param_type_conflict(SL(*name)); }
    }
    ParameterPhase check = {NULL, out};
    e = validate_parameter_contexts(q, schema, root, filter_schema, &check);
    if (JZ_IS_ERR(e)) jz_btree_drop(out);
    return e;
}

/* ---------------------------------------------------------------- relation envelopes */
enum { ROLE_RESULT, ROLE_UNION_ARM };

static bool is_full_identity_projection(const jazz_model_TableSchema *table, const jz_str *output_scope, const jazz_model_RelationProjectColumnVec *columns) {
    jz_btree projected;
    jz_btree_init(&projected, &jazz_model__STR_SET_DESC);
    bool ok = true;
    JZ_VEC_FOR(const jazz_model_RelationProjectColumn, c, *columns) {
        bool identity;
        if (c->expr.tag == JAZZ_KEYREF_ROW_ID) identity = c->expr.row_id == JAZZ_ROWID_CURRENT && jz_str_eq_cstr(&c->alias, "id");
        else identity = c->expr.column.scope.some && jz_str_eq(&c->expr.column.scope.s, output_scope) && jz_str_eq(&c->expr.column.column, &c->alias);
        if (!identity) { ok = false; break; }
        if (!jz_str_eq_cstr(&c->alias, "id")) { jz_str k = jz_str_clone(&c->alias); jz_btree_insert(&projected, &k, NULL, NULL); }
    }
    if (ok) {
        ok = projected.len == table->columns.len;
        JZ_VEC_FOR(const jazz_model_ColumnSchema, col, table->columns) if (ok && !jz_btree_contains(&projected, &col->name)) ok = false;
    }
    jz_btree_drop(&projected);
    return ok;
}

static jz_err reject_presentation_over_relation_projection(const jazz_model_Query *q) {
    if (q->array_subqueries.len)
        return unsupported_relation("include(...) is not supported on a relation query whose projection renames or narrows its output columns");
    if (q->has_select)
        return unsupported_relation("select(...) is not supported on a relation query whose projection renames or narrows its output columns");
    return JZ_OK;
}

static jz_err retain_relation_output_projection(const jazz_model_Query *q, const jazz_model_RelationQuery *relation,
                                                const jazz_model_TableSchema *output_table, u8 role, bool *out) {
    bool some = false;
    jazz_model_RelationProjectColumnVec present = {0};
    JZ_TRY(Q(relation_output_projection_if_present)(relation, &some, &present));
    VDROP(present, RelationProjectColumn);
    if (!some) { *out = false; return JZ_OK; }
    jz_str output_scope = {0};
    jazz_model_RelationProjectColumnVec columns = {0};
    JZ_TRY(Q(relation_output_projection)(relation, &output_scope, &columns));
    jz_err e = JZ_OK;
    if (role == ROLE_RESULT && is_full_identity_projection(output_table, &output_scope, &columns)) {
        *out = false;
    } else {
        e = reject_presentation_over_relation_projection(q);
        if (!JZ_IS_ERR(e)) e = Q(reject_reserved_relation_aliases)(&columns);
        *out = true;
    }
    jz_str_drop(&output_scope);
    VDROP(columns, RelationProjectColumn);
    return e;
}

static bool has_ordinary_clauses(const jazz_model_Query *q) {
    return q->filters.len || q->joins.len || q->flat_join || q->policy_branches.len || q->reachable.len || q->inherits.len ||
           q->includes.len || q->order_by.len || q->aggregate || q->has_limit || q->offset != 0;
}

/* Relation syntax owns row membership and its terminal ordering/window. */
static jz_err validate_retained_relation_outer_query(const jazz_model_Query *q) {
    if (has_ordinary_clauses(q)) return unsupported_relation("relation query cannot be combined with ordinary query clauses");
    return JZ_OK;
}

typedef struct ProjectionContract { jz_str alias; groove_ValueType ty; } ProjectionContract;
typedef JZ_VEC(ProjectionContract) ProjectionContractVec;
static void contract_drop_v(void *p) { ProjectionContract *c = p; jz_str_drop(&c->alias); VT(drop)(&c->ty); }
static bool contract_eq(const ProjectionContractVec *a, const ProjectionContractVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!jz_str_eq(&a->ptr[i].alias, &b->ptr[i].alias) || !vt_eq(&a->ptr[i].ty, &b->ptr[i].ty)) return false;
    return true;
}

static jz_err relation_projection_contract(const jazz_model_TableSchema *table, const jazz_model_RelationProjectColumnVec *columns,
                                           ProjectionContractVec *out) {
    memset(out, 0, sizeof(*out));
    JZ_VEC_FOR(const jazz_model_RelationProjectColumn, c, *columns) {
        groove_ValueType ty;
        if (c->expr.tag == JAZZ_KEYREF_COLUMN) {
            const groove_ValueType *t;
            jz_err e = PCT(table, SL(c->expr.column.column), &t);
            if (JZ_IS_ERR(e)) { jz_vec_drop(out, sizeof(ProjectionContract), contract_drop_v); return e; }
            ty = vt_clone(t);
        } else if (c->expr.row_id == JAZZ_ROWID_CURRENT) {
            ty = VT_UUID;
        } else {
            jz_vec_drop(out, sizeof(ProjectionContract), contract_drop_v);
            return unsupported_relation("outer/frontier row-id relation projections are not unified yet");
        }
        ProjectionContract *pc = JAZZ_VEC_SLOT(*out);
        pc->alias = jz_str_clone(&c->alias);
        pc->ty = ty;
    }
    return JZ_OK;
}

static jz_err validate_query_canonical_parts_as(const jazz_model_Query *q, const jazz_model_RuntimeSchema *schema, u8 role,
                                                jazz_model_Query *normalized, jz_btree *params, jz_bytes *canonical);

/* Validate every relation UNION arm through the established single-relation facade, then merge its
 * inferred parameter domain. */
static jz_err validate_retained_relation_union(const jazz_model_RelationQuery *relation, const jz_str *output_table,
                                               const jazz_model_RuntimeSchema *schema, jz_btree *params) {
    jazz_model_RelationUnionParts parts;
    if (!Q(relation_union_parts)(&relation->rel, &parts))
        return unsupported_relation("retained relation query must be a union with global terminal operators in builder order");
    jz_err e = JZ_OK;
    jz_btree labels;
    jz_btree_init(&labels, &jazz_model__STR_SET_DESC);
    bool has_output = false;
    ProjectionContractVec output_projection = {0};
    const jazz_model_TableSchema *output_table_schema;
    if (parts.inputs.len == 0) { e = unsupported_relation("union requires at least one input"); goto done; }
    if (JZ_IS_ERR(e = Q(schema_table)(schema, SL(*output_table), &output_table_schema))) goto done;
    /* Global UNION ordering runs over the union output rows, which are always rows of output_table. */
    if (parts.has_order_by) {
        JZ_VEC_FOR(const jazz_model_RelationOrderBy, term, parts.order_by) {
            if (term->column.scope.some && !jz_str_eq(&term->column.scope.s, output_table)) {
                e = unsupported_relation("union order_by must be scoped to the union output table");
                goto done;
            }
            jazz_model_OrderByVec one = {0};
            jazz_model_OrderBy ob = {jz_str_clone(&term->column.column), term->direction};
            JZ_VEC_PUSH(one, ob);
            e = reject_author_ordering(&one);
            VDROP(one, OrderBy);
            if (JZ_IS_ERR(e)) goto done;
            const groove_ValueType *t;
            if (JZ_IS_ERR(e = PCT(output_table_schema, SL(term->column.column), &t))) goto done;
        }
    }
    JZ_VEC_FOR(const jazz_model_RelationUnionArm, arm, parts.inputs) {
        bool bad = arm->label.len == 0 || arm->label.len > 4096 || memchr(arm->label.ptr, 0, arm->label.len);
        if (!bad) { jz_str k = jz_str_clone(&arm->label); bad = jz_btree_insert(&labels, &k, NULL, NULL); }
        if (bad) { e = unsupported_relation("union arm labels must be 1..=4096 bytes, NUL-free, and unique"); goto done; }
        jazz_model_RelationQuery arm_relation;
        Q(RelationExpr__Clone__clone)(&arm_relation.rel, &arm->input);
        jz_str scope = {0};
        jazz_model_RelationProjectColumnVec arm_columns = {0};
        e = Q(relation_output_projection)(&arm_relation, &scope, &arm_columns);
        jz_str_drop(&scope);
        ProjectionContractVec arm_contract = {0};
        if (!JZ_IS_ERR(e)) e = relation_projection_contract(output_table_schema, &arm_columns, &arm_contract);
        VDROP(arm_columns, RelationProjectColumn);
        if (!JZ_IS_ERR(e)) {
            if (has_output) {
                if (!contract_eq(&output_projection, &arm_contract)) e = unsupported_relation("union arms must use the same output projection");
                jz_vec_drop(&arm_contract, sizeof(ProjectionContract), contract_drop_v);
            } else {
                output_projection = arm_contract;
                has_output = true;
            }
        }
        jazz_model_Query arm_query;
        memset(&arm_query, 0, sizeof(arm_query));
        if (!JZ_IS_ERR(e)) e = Q(relation_query_to_query)(&arm_relation, &arm_query);
        if (JZ_IS_ERR(e)) { Q(RelationQuery__drop)(&arm_relation); goto done; }
        if (!jz_str_eq(&arm_query.table, output_table)) {
            Q(RelationQuery__drop)(&arm_relation);
            Q(Query__drop)(&arm_query);
            e = unsupported_relation("union inputs must output the same table");
            goto done;
        }
        arm_query.relation = JZ_NEW(jazz_model_RelationQuery);
        *arm_query.relation = arm_relation;
        jazz_model_Query normalized;
        jz_btree arm_params;
        jz_bytes canonical;
        e = validate_query_canonical_parts_as(&arm_query, schema, ROLE_UNION_ARM, &normalized, &arm_params, &canonical);
        Q(Query__drop)(&arm_query);
        if (JZ_IS_ERR(e)) goto done;
        Q(Query__drop)(&normalized);
        jz_vec_drop(&canonical, 1, NULL);
        const jz_str *name;
        const groove_ValueType *ty;
        JZ_BTREE_FOR(&arm_params, name, ty) {
            const groove_ValueType *existing = jz_btree_get(params, name);
            if (!existing) {
                jz_str k = jz_str_clone(name);
                groove_ValueType v = vt_clone(ty);
                jz_btree_insert(params, &k, &v, NULL);
            } else if (!vt_eq(existing, ty)) {
                e = param_type_conflict(SL(*name));
                break;
            }
        }
        jz_btree_drop(&arm_params);
        if (JZ_IS_ERR(e)) goto done;
    }
done:
    jz_btree_drop(&labels);
    jz_vec_drop(&output_projection, sizeof(ProjectionContract), contract_drop_v);
    Q(RelationUnionParts__drop)(&parts);
    return e;
}

/* ---------------------------------------------------------------- validate_query_canonical_parts_as */
static bool query_matches_generated(const jazz_model_Query *q, const jazz_model_Query *r) {
#define VEQ(a, b, N) JAZZ_VEC_EQ(&(a), &(b), Q(N##__PartialEq__eq))
    bool flat_eq = (!q->flat_join && !r->flat_join) || (q->flat_join && r->flat_join && Q(FlatJoin__PartialEq__eq)(q->flat_join, r->flat_join));
    bool agg_eq = (!q->aggregate && !r->aggregate) || (q->aggregate && r->aggregate && Q(AggregateQuery__PartialEq__eq)(q->aggregate, r->aggregate));
    return VEQ(q->filters, r->filters, Predicate) && VEQ(q->joins, r->joins, JoinVia) && flat_eq &&
           VEQ(q->policy_branches, r->policy_branches, PolicyBranch) && VEQ(q->reachable, r->reachable, ReachableVia) &&
           VEQ(q->inherits, r->inherits, InheritsVia) && VEQ(q->includes, r->includes, Include) && VEQ(q->order_by, r->order_by, OrderBy) &&
           agg_eq && q->has_limit == r->has_limit && (!q->has_limit || q->limit == r->limit) && q->offset == r->offset;
#undef VEQ
}

static jz_err validate_query_canonical_parts_as(const jazz_model_Query *q, const jazz_model_RuntimeSchema *schema, u8 role,
                                                jazz_model_Query *normalized, jz_btree *params_out, jz_bytes *canonical) {
    const jazz_model_TableSchema *root;
    JZ_TRY(Q(schema_table)(schema, SL(q->table), &root));
    jz_err e = JZ_OK;
    jz_btree params;
    jz_btree_init(&params, &jazz_model__query__PARAM_TYPES_DESC);
    jazz_model_Query resolved_query;
    Q(Query__Clone__clone)(&resolved_query, q);

    if (q->relation) {
        const jazz_model_RelationQuery *relation = q->relation;
        jazz_model_RelationUnionParts parts;
        bool is_union = Q(relation_union_parts)(&relation->rel, &parts);
        if (is_union) Q(RelationUnionParts__drop)(&parts);
        if (!is_union) {
            jazz_model_Query resolved;
            memset(&resolved, 0, sizeof(resolved));
            if (JZ_IS_ERR(e = Q(relation_query_to_query)(relation, &resolved))) goto fail;
            if (!jz_str_eq(&resolved.table, &q->table)) {
                Q(Query__drop)(&resolved);
                e = unsupported_relation("relation query output table does not match its Query envelope");
                goto fail;
            }
            /* A relation facade sent over the public wire carries no ordinary clauses; reject only
             * ordinary clauses that are neither absent nor canonical. */
            if (has_ordinary_clauses(q) && !query_matches_generated(q, &resolved)) {
                Q(Query__drop)(&resolved);
                e = unsupported_relation("relation query cannot be combined with ordinary query clauses");
                goto fail;
            }
            VDROP(resolved.array_subqueries, ArraySubquery);
            jz_vec_clone(&resolved.array_subqueries, &q->array_subqueries, sizeof(jazz_model_ArraySubquery), Q(ArraySubquery__Clone__clone_v));
            if (resolved.relation) { Q(RelationQuery__drop)(resolved.relation); jz_free(resolved.relation); resolved.relation = NULL; }
            jazz_model__StrVec__drop(&resolved.select);
            resolved.has_select = q->has_select;
            jazz_model__StrVec__clone(&resolved.select, &q->select);
            bool retain = false;
            e = retain_relation_output_projection(q, relation, root, role, &retain);
            jz_bytes ignored = {0};
            if (!JZ_IS_ERR(e)) e = validate_query_canonical_parts_as(&resolved, schema, ROLE_RESULT, normalized, params_out, &ignored);
            Q(Query__drop)(&resolved);
            jz_vec_drop(&ignored, 1, NULL);
            if (JZ_IS_ERR(e)) goto fail;
            if (retain) {
                normalized->relation = JZ_NEW(jazz_model_RelationQuery);
                Q(RelationQuery__Clone__clone)(normalized->relation, relation);
            }
            e = Q(canonical_query_bytes_for_schema)(normalized, schema, canonical);
            if (JZ_IS_ERR(e)) { Q(Query__drop)(normalized); jz_btree_drop(params_out); goto fail; }
            jz_btree_drop(&params);
            Q(Query__drop)(&resolved_query);
            return JZ_OK;
        }
        if (JZ_IS_ERR(e = validate_retained_relation_outer_query(q))) goto fail;
        if (JZ_IS_ERR(e = reject_presentation_over_relation_projection(q))) goto fail;
        if (JZ_IS_ERR(e = validate_retained_relation_union(relation, &q->table, schema, &params))) goto fail;
        jz_btree prepared;
        if (JZ_IS_ERR(e = prepare_parameter_domain(&resolved_query, schema, root, root, &params, &prepared))) goto fail;
        if (q->has_select) {
            JZ_VEC_FOR(const jz_str, c, q->select) {
                if (JZ_IS_ERR(e = validate_select_column(root, SL(*c)))) { jz_btree_drop(&prepared); goto fail; }
            }
        }
        Q(normalize_query)(&resolved_query, normalized);
        e = Q(canonical_query_bytes_for_schema)(normalized, schema, canonical);
        if (JZ_IS_ERR(e)) { Q(Query__drop)(normalized); jz_btree_drop(&prepared); goto fail; }
        *params_out = prepared;
        jz_btree_drop(&params);
        Q(Query__drop)(&resolved_query);
        return JZ_OK;
    }

    bool has_flat = false;
    jazz_model_TableSchema flat_filter_schema;
    if (q->flat_join) {
        const jazz_model_FlatJoin *fj = q->flat_join;
        struct { bool present; const char *feature; } unsupported[] = {
            {fj->sources.len == 0, "zero joined sources"},
            {q->joins.len != 0, "existential joins"},
            {q->policy_branches.len != 0, "policy branches"},
            {q->reachable.len != 0, "reachability"},
            {q->inherits.len != 0, "inherited-policy traversals"},
            {q->includes.len != 0, "includes"},
            {q->array_subqueries.len != 0, "array subqueries"},
            {q->has_select, "select projections"},
            {q->order_by.len != 0, "ordering"},
            {q->aggregate != NULL, "aggregates"},
            {q->has_limit, "limits"},
            {q->offset != 0, "offsets"},
        };
        for (u32 i = 0; i < sizeof(unsupported) / sizeof(unsupported[0]); i++) {
            if (unsupported[i].present) { e = flat_join_unsupported(unsupported[i].feature); goto fail; }
        }
        if (JZ_IS_ERR(e = validate_flat_join(schema, &q->table, fj))) goto fail;
        jz_btree sources;
        if (JZ_IS_ERR(e = flat_join_source_tables(schema, &q->table, fj, &sources))) goto fail;
        JZ_VEC_FOR(jazz_model_Predicate, p, resolved_query.filters) {
            if (JZ_IS_ERR(e = qualify_flat_join_predicate(p, &sources))) break;
            if (JZ_IS_ERR(e = validate_flat_join_filter_routing(p))) break;
        }
        if (!JZ_IS_ERR(e)) e = flat_join_filter_schema(&sources, &flat_filter_schema);
        jz_btree_drop(&sources);
        if (JZ_IS_ERR(e)) goto fail;
        has_flat = true;
    }
    jz_btree prepared;
    e = prepare_parameter_domain(&resolved_query, schema, root, has_flat ? &flat_filter_schema : root, &params, &prepared);
    if (has_flat) jazz_model__schema__TableSchema__drop(&flat_filter_schema);
    if (JZ_IS_ERR(e)) goto fail;
    JZ_VEC_FOR(const jazz_model_InheritsVia, i, q->inherits) if (JZ_IS_ERR(e = validate_inherits(root, i))) goto fail_prepared;
    JZ_VEC_FOR(const jazz_model_PolicyBranch, branch, resolved_query.policy_branches)
        JZ_VEC_FOR(const jazz_model_InheritsVia, i, branch->inherits) if (JZ_IS_ERR(e = validate_inherits(root, i))) goto fail_prepared;
    JZ_VEC_FOR(const jazz_model_Include, inc, q->includes) if (JZ_IS_ERR(e = validate_include(schema, root, &inc->path))) goto fail_prepared;
    if (q->has_select) JZ_VEC_FOR(const jz_str, c, q->select) if (JZ_IS_ERR(e = validate_select_column(root, SL(*c)))) goto fail_prepared;
    if (JZ_IS_ERR(e = reject_author_ordering(&q->order_by))) goto fail_prepared;
    if (q->aggregate) {
        if (JZ_IS_ERR(e = validate_aggregate(root, q->aggregate))) goto fail_prepared;
        if (JZ_IS_ERR(e = validate_aggregate_order_by(&q->table, q->aggregate, &q->order_by))) goto fail_prepared;
    } else {
        const groove_ValueType *t;
        JZ_VEC_FOR(const jazz_model_OrderBy, o, q->order_by) if (JZ_IS_ERR(e = PCT(root, SL(o->column), &t))) goto fail_prepared;
    }
    Q(normalize_query)(&resolved_query, normalized);
    e = Q(canonical_query_bytes_for_schema)(normalized, schema, canonical);
    if (JZ_IS_ERR(e)) { Q(Query__drop)(normalized); goto fail_prepared; }
    *params_out = prepared;
    jz_btree_drop(&params);
    Q(Query__drop)(&resolved_query);
    return JZ_OK;
fail_prepared:
    jz_btree_drop(&prepared);
fail:
    jz_btree_drop(&params);
    Q(Query__drop)(&resolved_query);
    return e;
}

/* ---------------------------------------------------------------- entry points */
static jz_err validate_query_with_schema_version_as(const jazz_model_Query *q, const jazz_model_RuntimeSchema *schema,
                                                    jazz_types_SchemaVersionId version, u8 role, jazz_model_ValidatedQuery *out) {
    jazz_model_Query normalized;
    jz_btree params;
    jz_bytes canonical;
    JZ_TRY(validate_query_canonical_parts_as(q, schema, role, &normalized, &params, &canonical));
    jz_bytes shape_identity = {0};
    jz_vec_extend(&shape_identity, canonical.ptr, canonical.len, 1);
    jz_vec_extend(&shape_identity, version.b, 16, 1);
    out->shape_id.id = jz_uuid_v5(&jazz_model__query__QUERY_NAMESPACE, shape_identity.ptr, shape_identity.len);
    jz_vec_drop(&shape_identity, 1, NULL);
    out->query = normalized;
    out->schema_version = version;
    out->params = params;
    out->canonical = canonical;
    return JZ_OK;
}

jz_err Q(validate_query_with_schema_version)(const jazz_model_Query *q, const jazz_model_RuntimeSchema *schema,
                                             jazz_types_SchemaVersionId version, jazz_model_ValidatedQuery *out) {
    return validate_query_with_schema_version_as(q, schema, version, ROLE_RESULT, out);
}
jz_err Q(validate_query)(const jazz_model_Query *q, const jazz_model_RuntimeSchema *schema, jazz_model_ValidatedQuery *out) {
    return Q(validate_query_with_schema_version)(q, schema, jazz_model__schema__RuntimeSchema__version_id(schema), out);
}
/* Validate one arm of a retained relation UNION. An arm always keeps its explicit projection. */
jz_err Q(validate_union_arm_with_schema_version)(const jazz_model_Query *q, const jazz_model_RuntimeSchema *schema,
                                                 jazz_types_SchemaVersionId version, jazz_model_ValidatedQuery *out) {
    return validate_query_with_schema_version_as(q, schema, version, ROLE_UNION_ARM, out);
}
