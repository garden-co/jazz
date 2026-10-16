/* model/public_api/relation_ir.rs: relation IR used by relation-backed policy
 * `exists(...)`, with externally tagged JSON serde. */
#include "jazz_model/jazz_model.h"

#define RI(x) jazz_model__model__public_api__relation_ir__##x
#define VALUE(x) jazz_model__model__public_api__types__value__Value__##x

static const char *const VALUE_REF_NAMES[] = {"Literal", "SessionRef", "OuterColumn", "RowId"};
static const char *const PCMP_NAMES[] = {"Eq", "Ne", "Lt", "Le", "Gt", "Ge"};
static const char *const PRED_NAMES[] = {"Cmp", "IsNull", "IsNotNull", "In", "Contains", "EnumMatch", "And", "Or", "Not", "True", "False"};
static const char *const ROW_ID_NAMES[] = {"Current", "Outer", "Frontier"};
static const char *const KEY_REF_NAMES[] = {"Column", "RowId"};
static const char *const JOIN_KIND_NAMES[] = {"Inner", "Left"};
static const char *const BOUND_NAMES[] = {"Fixpoint", "MaxDepth"};
static const char *const REL_NAMES[] = {"TableScan", "Filter", "Union", "Join", "Project", "Gather"};

/* ---------------------------------------------------------------- ColumnRef */
void RI(ColumnRef__drop)(jazz_model_RelColumnRef *c) { jazz_model__OptStr__drop(&c->scope); jz_str_drop(&c->column); }
void RI(ColumnRef__Clone__clone)(jazz_model_RelColumnRef *dst, const jazz_model_RelColumnRef *src) {
    dst->scope = jazz_model__OptStr__clone(&src->scope);
    dst->column = jz_str_clone(&src->column);
}
bool RI(ColumnRef__PartialEq__eq)(const jazz_model_RelColumnRef *a, const jazz_model_RelColumnRef *b) {
    return jazz_model__OptStr__eq(&a->scope, &b->scope) && jz_str_eq(&a->column, &b->column);
}
void RI(ColumnRef__Debug__fmt)(jz_str *out, const jazz_model_RelColumnRef *c) {
    jz_str_push_cstr(out, "ColumnRef { scope: ");
    if (c->scope.some) {
        jz_str_push_cstr(out, "Some(");
        jazz_model__debug_str(out, c->scope.s.ptr, c->scope.s.len);
        jz_str_push_char(out, ')');
    } else jz_str_push_cstr(out, "None");
    jz_str_push_cstr(out, ", column: ");
    jazz_model__debug_str(out, c->column.ptr, c->column.len);
    jz_str_push_cstr(out, " }");
}
jazz_model_RelColumnRef RI(ColumnRef__unscoped)(jz_str column) {
    jazz_model_RelColumnRef c;
    c.scope.some = false;
    c.scope.s = (jz_str){0};
    c.column = column;
    return c;
}

static void column_ref_serialize(jz_json_w *w, const jazz_model_RelColumnRef *c) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "scope");
    if (c->scope.some) jazz_model__json_w_str(w, &c->scope.s);
    else jz_jw_null(w);
    jz_jw_key(w, "column");
    jazz_model__json_w_str(w, &c->column);
    jz_jw_end_obj(w);
}

static jz_err column_ref_deserialize(const jz_json *j, jazz_model_RelColumnRef *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_json_expect_obj(j, "struct ColumnRef"));
    const jz_json *s = jz_json_obj_get(j, "scope");
    if (s && s->kind != JZ_JSON_NULL) {
        JZ_TRY(jazz_model__json_expect_str(s, &out->scope.s));
        out->scope.some = true;
    }
    jz_err e = jz_json_field_str(j, "column", &out->column);
    if (e.code) jazz_model__OptStr__drop(&out->scope);
    return e;
}

/* ---------------------------------------------------------------- ValueRef */
void RI(ValueRef__drop)(jazz_model_RelValueRef *v) {
    switch (v->tag) {
    case JAZZ_RVR_LITERAL: VALUE(drop)(&v->literal); break;
    case JAZZ_RVR_SESSION_REF: jazz_model__StrVec__drop(&v->session_ref); break;
    case JAZZ_RVR_OUTER_COLUMN: RI(ColumnRef__drop)(&v->outer_column); break;
    default: break;
    }
}
static void value_ref_drop_v(void *v) { RI(ValueRef__drop)((jazz_model_RelValueRef *)v); }

void RI(ValueRef__Clone__clone)(jazz_model_RelValueRef *dst, const jazz_model_RelValueRef *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    switch (src->tag) {
    case JAZZ_RVR_LITERAL: VALUE(Clone__clone)(&dst->literal, &src->literal); break;
    case JAZZ_RVR_SESSION_REF: jazz_model__StrVec__clone(&dst->session_ref, &src->session_ref); break;
    case JAZZ_RVR_OUTER_COLUMN: RI(ColumnRef__Clone__clone)(&dst->outer_column, &src->outer_column); break;
    default: dst->row_id = src->row_id; break;
    }
}
static void value_ref_clone_v(void *dst, const void *src) { RI(ValueRef__Clone__clone)(dst, src); }

bool RI(ValueRef__PartialEq__eq)(const jazz_model_RelValueRef *a, const jazz_model_RelValueRef *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_RVR_LITERAL: return VALUE(PartialEq__eq)(&a->literal, &b->literal);
    case JAZZ_RVR_SESSION_REF: return jazz_model__StrVec__eq(&a->session_ref, &b->session_ref);
    case JAZZ_RVR_OUTER_COLUMN: return RI(ColumnRef__PartialEq__eq)(&a->outer_column, &b->outer_column);
    default: return a->row_id == b->row_id;
    }
}

static void value_ref_debug(jz_str *out, const jazz_model_RelValueRef *v) {
    jz_str_push_cstr(out, VALUE_REF_NAMES[v->tag]);
    jz_str_push_char(out, '(');
    switch (v->tag) {
    case JAZZ_RVR_LITERAL: VALUE(Debug__fmt)(out, &v->literal); break;
    case JAZZ_RVR_SESSION_REF: jazz_model__StrVec__Debug__fmt(out, &v->session_ref); break;
    case JAZZ_RVR_OUTER_COLUMN: RI(ColumnRef__Debug__fmt)(out, &v->outer_column); break;
    default: jz_str_push_cstr(out, ROW_ID_NAMES[v->row_id]); break;
    }
    jz_str_push_char(out, ')');
}

static jz_err value_ref_serialize(jz_json_w *w, const jazz_model_RelValueRef *v) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, VALUE_REF_NAMES[v->tag]);
    switch (v->tag) {
    case JAZZ_RVR_LITERAL: JZ_TRY(VALUE(Serialize__serialize)(w, &v->literal)); break;
    case JAZZ_RVR_SESSION_REF: jazz_model__StrVec__serialize_json(w, &v->session_ref); break;
    case JAZZ_RVR_OUTER_COLUMN: column_ref_serialize(w, &v->outer_column); break;
    default: jazz_model__json_w_cstr(w, ROW_ID_NAMES[v->row_id]); break;
    }
    jz_jw_end_obj(w);
    return JZ_OK;
}

static jz_err row_id_deserialize(const jz_json *j, u8 *out) {
    u32 idx;
    JZ_TRY(jazz_model__json_unit_variant(j, ROW_ID_NAMES, 3, &idx));
    *out = (u8)idx;
    return JZ_OK;
}

static jz_err value_ref_deserialize(const jz_json *j, jazz_model_RelValueRef *out) {
    u32 idx;
    const jz_json *p;
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_json_variant(j, VALUE_REF_NAMES, 4, &idx, &p));
    if (!p) return jazz_model__serde_err("invalid type: unit variant, expected newtype variant");
    out->tag = (u8)idx;
    switch (idx) {
    case JAZZ_RVR_LITERAL: return VALUE(Deserialize__deserialize)(p, &out->literal);
    case JAZZ_RVR_SESSION_REF: return jazz_model__StrVec__deserialize_json(p, &out->session_ref);
    case JAZZ_RVR_OUTER_COLUMN: return column_ref_deserialize(p, &out->outer_column);
    default: return row_id_deserialize(p, &out->row_id);
    }
}

/* ---------------------------------------------------------------- PredicateExpr */
static void pred_drop_v(void *p) { RI(PredicateExpr__drop)((jazz_model_RelPredicateExpr *)p); }
static void pred_clone_v(void *dst, const void *src) { RI(PredicateExpr__Clone__clone)(dst, src); }

void RI(PredicateExpr__drop)(jazz_model_RelPredicateExpr *p) {
    switch (p->tag) {
    case JAZZ_RPE_CMP: case JAZZ_RPE_CONTAINS: RI(ColumnRef__drop)(&p->left); RI(ValueRef__drop)(&p->right); break;
    case JAZZ_RPE_IS_NULL: case JAZZ_RPE_IS_NOT_NULL: RI(ColumnRef__drop)(&p->left); break;
    case JAZZ_RPE_IN:
        RI(ColumnRef__drop)(&p->left);
        jz_vec_drop(&p->values, sizeof(jazz_model_RelValueRef), value_ref_drop_v);
        break;
    case JAZZ_RPE_ENUM_MATCH:
        RI(ColumnRef__drop)(&p->left);
        jz_str_drop(&p->case_);
        if (p->inner) { RI(PredicateExpr__drop)(p->inner); jz_free(p->inner); }
        break;
    case JAZZ_RPE_AND: case JAZZ_RPE_OR: jz_vec_drop(&p->items, sizeof(jazz_model_RelPredicateExpr), pred_drop_v); break;
    case JAZZ_RPE_NOT: if (p->inner) { RI(PredicateExpr__drop)(p->inner); jz_free(p->inner); } break;
    default: break;
    }
    memset(p, 0, sizeof *p);
    p->tag = JAZZ_RPE_TRUE;
}

static jazz_model_RelPredicateExpr *pred_box_clone(const jazz_model_RelPredicateExpr *p) {
    jazz_model_RelPredicateExpr *b = JZ_NEW(jazz_model_RelPredicateExpr);
    RI(PredicateExpr__Clone__clone)(b, p);
    return b;
}

void RI(PredicateExpr__Clone__clone)(jazz_model_RelPredicateExpr *dst, const jazz_model_RelPredicateExpr *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    dst->op = src->op;
    switch (src->tag) {
    case JAZZ_RPE_CMP: case JAZZ_RPE_CONTAINS:
        RI(ColumnRef__Clone__clone)(&dst->left, &src->left);
        RI(ValueRef__Clone__clone)(&dst->right, &src->right);
        break;
    case JAZZ_RPE_IS_NULL: case JAZZ_RPE_IS_NOT_NULL: RI(ColumnRef__Clone__clone)(&dst->left, &src->left); break;
    case JAZZ_RPE_IN:
        RI(ColumnRef__Clone__clone)(&dst->left, &src->left);
        jz_vec_clone(&dst->values, &src->values, sizeof(jazz_model_RelValueRef), value_ref_clone_v);
        break;
    case JAZZ_RPE_ENUM_MATCH:
        RI(ColumnRef__Clone__clone)(&dst->left, &src->left);
        dst->case_ = jz_str_clone(&src->case_);
        dst->inner = pred_box_clone(src->inner);
        break;
    case JAZZ_RPE_AND: case JAZZ_RPE_OR:
        jz_vec_clone(&dst->items, &src->items, sizeof(jazz_model_RelPredicateExpr), pred_clone_v);
        break;
    case JAZZ_RPE_NOT: dst->inner = pred_box_clone(src->inner); break;
    default: break;
    }
}

bool RI(PredicateExpr__PartialEq__eq)(const jazz_model_RelPredicateExpr *a, const jazz_model_RelPredicateExpr *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_RPE_CMP:
        return RI(ColumnRef__PartialEq__eq)(&a->left, &b->left) && a->op == b->op && RI(ValueRef__PartialEq__eq)(&a->right, &b->right);
    case JAZZ_RPE_CONTAINS:
        return RI(ColumnRef__PartialEq__eq)(&a->left, &b->left) && RI(ValueRef__PartialEq__eq)(&a->right, &b->right);
    case JAZZ_RPE_IS_NULL: case JAZZ_RPE_IS_NOT_NULL: return RI(ColumnRef__PartialEq__eq)(&a->left, &b->left);
    case JAZZ_RPE_IN:
        if (!RI(ColumnRef__PartialEq__eq)(&a->left, &b->left) || a->values.len != b->values.len) return false;
        for (u32 i = 0; i < a->values.len; i++)
            if (!RI(ValueRef__PartialEq__eq)(&a->values.ptr[i], &b->values.ptr[i])) return false;
        return true;
    case JAZZ_RPE_ENUM_MATCH:
        return RI(ColumnRef__PartialEq__eq)(&a->left, &b->left) && jz_str_eq(&a->case_, &b->case_) &&
               RI(PredicateExpr__PartialEq__eq)(a->inner, b->inner);
    case JAZZ_RPE_AND: case JAZZ_RPE_OR:
        if (a->items.len != b->items.len) return false;
        for (u32 i = 0; i < a->items.len; i++)
            if (!RI(PredicateExpr__PartialEq__eq)(&a->items.ptr[i], &b->items.ptr[i])) return false;
        return true;
    case JAZZ_RPE_NOT: return RI(PredicateExpr__PartialEq__eq)(a->inner, b->inner);
    default: return true;
    }
}

void RI(PredicateExpr__Debug__fmt)(jz_str *out, const jazz_model_RelPredicateExpr *p) {
    jz_str_push_cstr(out, PRED_NAMES[p->tag]);
    switch (p->tag) {
    case JAZZ_RPE_CMP:
        jz_str_push_cstr(out, " { left: ");
        RI(ColumnRef__Debug__fmt)(out, &p->left);
        jz_str_appendf(out, ", op: %s, right: ", PCMP_NAMES[p->op]);
        value_ref_debug(out, &p->right);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_RPE_IS_NULL: case JAZZ_RPE_IS_NOT_NULL:
        jz_str_push_cstr(out, " { column: ");
        RI(ColumnRef__Debug__fmt)(out, &p->left);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_RPE_IN:
        jz_str_push_cstr(out, " { left: ");
        RI(ColumnRef__Debug__fmt)(out, &p->left);
        jz_str_push_cstr(out, ", values: [");
        for (u32 i = 0; i < p->values.len; i++) {
            if (i) jz_str_push_cstr(out, ", ");
            value_ref_debug(out, &p->values.ptr[i]);
        }
        jz_str_push_cstr(out, "] }");
        break;
    case JAZZ_RPE_CONTAINS:
        jz_str_push_cstr(out, " { left: ");
        RI(ColumnRef__Debug__fmt)(out, &p->left);
        jz_str_push_cstr(out, ", right: ");
        value_ref_debug(out, &p->right);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_RPE_ENUM_MATCH:
        jz_str_push_cstr(out, " { column: ");
        RI(ColumnRef__Debug__fmt)(out, &p->left);
        jz_str_push_cstr(out, ", case: ");
        jazz_model__debug_str(out, p->case_.ptr, p->case_.len);
        jz_str_push_cstr(out, ", payload: ");
        RI(PredicateExpr__Debug__fmt)(out, p->inner);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_RPE_AND: case JAZZ_RPE_OR:
        jz_str_push_cstr(out, "([");
        for (u32 i = 0; i < p->items.len; i++) {
            if (i) jz_str_push_cstr(out, ", ");
            RI(PredicateExpr__Debug__fmt)(out, &p->items.ptr[i]);
        }
        jz_str_push_cstr(out, "])");
        break;
    case JAZZ_RPE_NOT:
        jz_str_push_char(out, '(');
        RI(PredicateExpr__Debug__fmt)(out, p->inner);
        jz_str_push_char(out, ')');
        break;
    default: break;
    }
}

jz_err RI(PredicateExpr__Serialize__serialize)(jz_json_w *w, const jazz_model_RelPredicateExpr *p) {
    if (p->tag == JAZZ_RPE_TRUE || p->tag == JAZZ_RPE_FALSE) {
        jazz_model__json_w_cstr(w, PRED_NAMES[p->tag]);
        return JZ_OK;
    }
    jz_jw_begin_obj(w);
    jz_jw_key(w, PRED_NAMES[p->tag]);
    switch (p->tag) {
    case JAZZ_RPE_CMP:
        jz_jw_begin_obj(w);
        jz_jw_key(w, "left"); column_ref_serialize(w, &p->left);
        jz_jw_key(w, "op"); jazz_model__json_w_cstr(w, PCMP_NAMES[p->op]);
        jz_jw_key(w, "right"); JZ_TRY(value_ref_serialize(w, &p->right));
        jz_jw_end_obj(w);
        break;
    case JAZZ_RPE_IS_NULL: case JAZZ_RPE_IS_NOT_NULL:
        jz_jw_begin_obj(w);
        jz_jw_key(w, "column"); column_ref_serialize(w, &p->left);
        jz_jw_end_obj(w);
        break;
    case JAZZ_RPE_IN:
        jz_jw_begin_obj(w);
        jz_jw_key(w, "left"); column_ref_serialize(w, &p->left);
        jz_jw_key(w, "values");
        jz_jw_begin_arr(w);
        for (u32 i = 0; i < p->values.len; i++) JZ_TRY(value_ref_serialize(w, &p->values.ptr[i]));
        jz_jw_end_arr(w);
        jz_jw_end_obj(w);
        break;
    case JAZZ_RPE_CONTAINS:
        jz_jw_begin_obj(w);
        jz_jw_key(w, "left"); column_ref_serialize(w, &p->left);
        jz_jw_key(w, "right"); JZ_TRY(value_ref_serialize(w, &p->right));
        jz_jw_end_obj(w);
        break;
    case JAZZ_RPE_ENUM_MATCH:
        jz_jw_begin_obj(w);
        jz_jw_key(w, "column"); column_ref_serialize(w, &p->left);
        jz_jw_key(w, "case"); jazz_model__json_w_str(w, &p->case_);
        jz_jw_key(w, "payload"); JZ_TRY(RI(PredicateExpr__Serialize__serialize)(w, p->inner));
        jz_jw_end_obj(w);
        break;
    case JAZZ_RPE_AND: case JAZZ_RPE_OR:
        jz_jw_begin_arr(w);
        for (u32 i = 0; i < p->items.len; i++) JZ_TRY(RI(PredicateExpr__Serialize__serialize)(w, &p->items.ptr[i]));
        jz_jw_end_arr(w);
        break;
    case JAZZ_RPE_NOT: JZ_TRY(RI(PredicateExpr__Serialize__serialize)(w, p->inner)); break;
    default: break;
    }
    jz_jw_end_obj(w);
    return JZ_OK;
}

static jz_err col_field(const jz_json *o, const char *name, jazz_model_RelColumnRef *out) {
    const jz_json *f = jz_json_obj_get(o, name);
    if (!f) return jazz_model__json_missing_field(name);
    return column_ref_deserialize(f, out);
}

static jz_err value_ref_field(const jz_json *o, const char *name, jazz_model_RelValueRef *out) {
    const jz_json *f = jz_json_obj_get(o, name);
    if (!f) return jazz_model__json_missing_field(name);
    return value_ref_deserialize(f, out);
}

static jz_err pred_box(const jz_json *f, const char *name, jazz_model_RelPredicateExpr **out) {
    if (!f) return jazz_model__json_missing_field(name);
    jazz_model_RelPredicateExpr *b = JZ_NEW(jazz_model_RelPredicateExpr);
    jz_err e = RI(PredicateExpr__Deserialize__deserialize)(f, b);
    if (e.code) { jz_free(b); return e; }
    *out = b;
    return JZ_OK;
}

static jz_err pred_fields(u32 idx, const jz_json *p, jazz_model_RelPredicateExpr *out) {
    u32 op;
    switch (idx) {
    case JAZZ_RPE_CMP: {
        JZ_TRY(jz_json_expect_obj(p, "struct variant PredicateExpr::Cmp"));
        JZ_TRY(col_field(p, "left", &out->left));
        const jz_json *o = jz_json_obj_get(p, "op");
        if (!o) return jazz_model__json_missing_field("op");
        JZ_TRY(jazz_model__json_unit_variant(o, PCMP_NAMES, 6, &op));
        out->op = (u8)op;
        return value_ref_field(p, "right", &out->right);
    }
    case JAZZ_RPE_IS_NULL: case JAZZ_RPE_IS_NOT_NULL:
        JZ_TRY(jz_json_expect_obj(p, "struct variant"));
        return col_field(p, "column", &out->left);
    case JAZZ_RPE_IN: {
        JZ_TRY(jz_json_expect_obj(p, "struct variant PredicateExpr::In"));
        JZ_TRY(col_field(p, "left", &out->left));
        const jz_json *vs = jz_json_obj_get(p, "values");
        if (!vs) return jazz_model__json_missing_field("values");
        JZ_TRY(jazz_model__json_expect_arr(vs, "a sequence"));
        for (u32 i = 0; i < vs->v.arr.len; i++) {
            jazz_model_RelValueRef v;
            JZ_TRY(value_ref_deserialize(vs->v.arr.ptr[i], &v));
            JZ_VEC_PUSH(out->values, v);
        }
        return JZ_OK;
    }
    case JAZZ_RPE_CONTAINS:
        JZ_TRY(jz_json_expect_obj(p, "struct variant PredicateExpr::Contains"));
        JZ_TRY(col_field(p, "left", &out->left));
        return value_ref_field(p, "right", &out->right);
    case JAZZ_RPE_ENUM_MATCH:
        JZ_TRY(jz_json_expect_obj(p, "struct variant PredicateExpr::EnumMatch"));
        JZ_TRY(col_field(p, "column", &out->left));
        JZ_TRY(jz_json_field_str(p, "case", &out->case_));
        return pred_box(jz_json_obj_get(p, "payload"), "payload", &out->inner);
    case JAZZ_RPE_AND: case JAZZ_RPE_OR:
        JZ_TRY(jazz_model__json_expect_arr(p, "a sequence"));
        for (u32 i = 0; i < p->v.arr.len; i++) {
            jazz_model_RelPredicateExpr x;
            JZ_TRY(RI(PredicateExpr__Deserialize__deserialize)(p->v.arr.ptr[i], &x));
            JZ_VEC_PUSH(out->items, x);
        }
        return JZ_OK;
    case JAZZ_RPE_NOT: return pred_box(p, "0", &out->inner);
    default: return JZ_OK;
    }
}

jz_err RI(PredicateExpr__Deserialize__deserialize)(const jz_json *j, jazz_model_RelPredicateExpr *out) {
    u32 idx;
    const jz_json *p;
    memset(out, 0, sizeof *out);
    out->tag = JAZZ_RPE_TRUE;
    JZ_TRY(jz_json_variant(j, PRED_NAMES, 11, &idx, &p));
    bool unit = idx == JAZZ_RPE_TRUE || idx == JAZZ_RPE_FALSE;
    if (!unit && !p) return jazz_model__serde_err("invalid type: unit variant, expected struct variant");
    out->tag = (u8)idx;
    if (unit) return JZ_OK;
    jz_err e = pred_fields(idx, p, out);
    if (e.code) RI(PredicateExpr__drop)(out);
    return e;
}

/* ---------------------------------------------------------------- KeyRef / ProjectExpr */
void RI(KeyRef__drop)(jazz_model_RelKeyRef *k) { if (k->tag == JAZZ_KEYREF_COLUMN) RI(ColumnRef__drop)(&k->column); }
static void key_ref_drop_v(void *k) { RI(KeyRef__drop)((jazz_model_RelKeyRef *)k); }
void RI(KeyRef__Clone__clone)(jazz_model_RelKeyRef *dst, const jazz_model_RelKeyRef *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    dst->row_id = src->row_id;
    if (src->tag == JAZZ_KEYREF_COLUMN) RI(ColumnRef__Clone__clone)(&dst->column, &src->column);
}
static void key_ref_clone_v(void *dst, const void *src) { RI(KeyRef__Clone__clone)(dst, src); }
bool RI(KeyRef__PartialEq__eq)(const jazz_model_RelKeyRef *a, const jazz_model_RelKeyRef *b) {
    if (a->tag != b->tag) return false;
    if (a->tag == JAZZ_KEYREF_COLUMN) return RI(ColumnRef__PartialEq__eq)(&a->column, &b->column);
    return a->row_id == b->row_id;
}
void RI(KeyRef__Debug__fmt)(jz_str *out, const jazz_model_RelKeyRef *k) {
    if (k->tag == JAZZ_KEYREF_COLUMN) {
        jz_str_push_cstr(out, "Column(");
        RI(ColumnRef__Debug__fmt)(out, &k->column);
        jz_str_push_char(out, ')');
    } else jz_str_appendf(out, "RowId(%s)", ROW_ID_NAMES[k->row_id]);
}

static void key_ref_serialize(jz_json_w *w, const jazz_model_RelKeyRef *k) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, KEY_REF_NAMES[k->tag]);
    if (k->tag == JAZZ_KEYREF_COLUMN) column_ref_serialize(w, &k->column);
    else jazz_model__json_w_cstr(w, ROW_ID_NAMES[k->row_id]);
    jz_jw_end_obj(w);
}

static jz_err key_ref_deserialize(const jz_json *j, jazz_model_RelKeyRef *out) {
    u32 idx;
    const jz_json *p;
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_json_variant(j, KEY_REF_NAMES, 2, &idx, &p));
    if (!p) return jazz_model__serde_err("invalid type: unit variant, expected newtype variant");
    out->tag = (u8)idx;
    if (idx == JAZZ_KEYREF_COLUMN) return column_ref_deserialize(p, &out->column);
    return row_id_deserialize(p, &out->row_id);
}

void RI(RecursionBound__Debug__fmt)(jz_str *out, const jazz_model_RelRecursionBound *b) {
    if (b->tag == JAZZ_BOUND_FIXPOINT) jz_str_push_cstr(out, "Fixpoint");
    else jz_str_appendf(out, "MaxDepth(%u)", b->max_depth);
}

/* ---------------------------------------------------------------- RelExpr */
static void union_arm_drop_v(void *a) {
    jazz_model_RelUnionArm *arm = a;
    jz_str_drop(&arm->label);
    RI(RelExpr__drop)(&arm->input);
}
static void union_arm_clone_v(void *dst, const void *src) {
    jazz_model_RelUnionArm *d = dst;
    const jazz_model_RelUnionArm *s = src;
    d->label = jz_str_clone(&s->label);
    RI(RelExpr__Clone__clone)(&d->input, &s->input);
}
static void project_column_drop_v(void *c) {
    jazz_model_RelProjectColumn *pc = c;
    jz_str_drop(&pc->alias);
    RI(KeyRef__drop)(&pc->expr);
}
static void project_column_clone_v(void *dst, const void *src) {
    jazz_model_RelProjectColumn *d = dst;
    const jazz_model_RelProjectColumn *s = src;
    d->alias = jz_str_clone(&s->alias);
    RI(KeyRef__Clone__clone)(&d->expr, &s->expr);
}
static void join_condition_drop_v(void *c) {
    jazz_model_RelJoinCondition *jc = c;
    RI(ColumnRef__drop)(&jc->left);
    RI(ColumnRef__drop)(&jc->right);
}
static void join_condition_clone_v(void *dst, const void *src) {
    jazz_model_RelJoinCondition *d = dst;
    const jazz_model_RelJoinCondition *s = src;
    RI(ColumnRef__Clone__clone)(&d->left, &s->left);
    RI(ColumnRef__Clone__clone)(&d->right, &s->right);
}

static void rel_box_drop(jazz_model_RelExpr **e) {
    if (*e) { RI(RelExpr__drop)(*e); jz_free(*e); *e = NULL; }
}

void RI(RelExpr__drop)(jazz_model_RelExpr *e) {
    switch (e->tag) {
    case JAZZ_REL_TABLE_SCAN: jazz_model__OptStr__drop(&e->alias); break;
    case JAZZ_REL_FILTER: rel_box_drop(&e->input); RI(PredicateExpr__drop)(&e->predicate); break;
    case JAZZ_REL_UNION: jz_vec_drop(&e->inputs, sizeof(jazz_model_RelUnionArm), union_arm_drop_v); break;
    case JAZZ_REL_JOIN:
        rel_box_drop(&e->input);
        rel_box_drop(&e->right);
        jz_vec_drop(&e->on, sizeof(jazz_model_RelJoinCondition), join_condition_drop_v);
        break;
    case JAZZ_REL_PROJECT:
        rel_box_drop(&e->input);
        jz_vec_drop(&e->columns, sizeof(jazz_model_RelProjectColumn), project_column_drop_v);
        break;
    case JAZZ_REL_GATHER:
        rel_box_drop(&e->input);
        rel_box_drop(&e->right);
        RI(KeyRef__drop)(&e->frontier_key);
        jz_vec_drop(&e->dedupe_key, sizeof(jazz_model_RelKeyRef), key_ref_drop_v);
        break;
    }
    memset(e, 0, sizeof *e);
}

static jazz_model_RelExpr *rel_box_clone(const jazz_model_RelExpr *e) {
    jazz_model_RelExpr *b = JZ_NEW(jazz_model_RelExpr);
    RI(RelExpr__Clone__clone)(b, e);
    return b;
}

void RI(RelExpr__Clone__clone)(jazz_model_RelExpr *dst, const jazz_model_RelExpr *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    switch (src->tag) {
    case JAZZ_REL_TABLE_SCAN:
        dst->table = src->table;
        dst->alias = jazz_model__OptStr__clone(&src->alias);
        break;
    case JAZZ_REL_FILTER:
        dst->input = rel_box_clone(src->input);
        RI(PredicateExpr__Clone__clone)(&dst->predicate, &src->predicate);
        break;
    case JAZZ_REL_UNION: jz_vec_clone(&dst->inputs, &src->inputs, sizeof(jazz_model_RelUnionArm), union_arm_clone_v); break;
    case JAZZ_REL_JOIN:
        dst->input = rel_box_clone(src->input);
        dst->right = rel_box_clone(src->right);
        jz_vec_clone(&dst->on, &src->on, sizeof(jazz_model_RelJoinCondition), join_condition_clone_v);
        dst->join_kind = src->join_kind;
        break;
    case JAZZ_REL_PROJECT:
        dst->input = rel_box_clone(src->input);
        jz_vec_clone(&dst->columns, &src->columns, sizeof(jazz_model_RelProjectColumn), project_column_clone_v);
        break;
    case JAZZ_REL_GATHER:
        dst->input = rel_box_clone(src->input);
        dst->right = rel_box_clone(src->right);
        RI(KeyRef__Clone__clone)(&dst->frontier_key, &src->frontier_key);
        dst->bound = src->bound;
        jz_vec_clone(&dst->dedupe_key, &src->dedupe_key, sizeof(jazz_model_RelKeyRef), key_ref_clone_v);
        break;
    }
}

bool RI(RelExpr__PartialEq__eq)(const jazz_model_RelExpr *a, const jazz_model_RelExpr *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_REL_TABLE_SCAN: return a->table == b->table && jazz_model__OptStr__eq(&a->alias, &b->alias);
    case JAZZ_REL_FILTER:
        return RI(RelExpr__PartialEq__eq)(a->input, b->input) && RI(PredicateExpr__PartialEq__eq)(&a->predicate, &b->predicate);
    case JAZZ_REL_UNION:
        if (a->inputs.len != b->inputs.len) return false;
        for (u32 i = 0; i < a->inputs.len; i++)
            if (!jz_str_eq(&a->inputs.ptr[i].label, &b->inputs.ptr[i].label) ||
                !RI(RelExpr__PartialEq__eq)(&a->inputs.ptr[i].input, &b->inputs.ptr[i].input))
                return false;
        return true;
    case JAZZ_REL_JOIN:
        if (!RI(RelExpr__PartialEq__eq)(a->input, b->input) || !RI(RelExpr__PartialEq__eq)(a->right, b->right)) return false;
        if (a->on.len != b->on.len || a->join_kind != b->join_kind) return false;
        for (u32 i = 0; i < a->on.len; i++)
            if (!RI(ColumnRef__PartialEq__eq)(&a->on.ptr[i].left, &b->on.ptr[i].left) ||
                !RI(ColumnRef__PartialEq__eq)(&a->on.ptr[i].right, &b->on.ptr[i].right))
                return false;
        return true;
    case JAZZ_REL_PROJECT:
        if (!RI(RelExpr__PartialEq__eq)(a->input, b->input) || a->columns.len != b->columns.len) return false;
        for (u32 i = 0; i < a->columns.len; i++)
            if (!jz_str_eq(&a->columns.ptr[i].alias, &b->columns.ptr[i].alias) ||
                !RI(KeyRef__PartialEq__eq)(&a->columns.ptr[i].expr, &b->columns.ptr[i].expr))
                return false;
        return true;
    case JAZZ_REL_GATHER:
        if (!RI(RelExpr__PartialEq__eq)(a->input, b->input) || !RI(RelExpr__PartialEq__eq)(a->right, b->right)) return false;
        if (!RI(KeyRef__PartialEq__eq)(&a->frontier_key, &b->frontier_key)) return false;
        if (a->bound.tag != b->bound.tag || (a->bound.tag == JAZZ_BOUND_MAX_DEPTH && a->bound.max_depth != b->bound.max_depth)) return false;
        if (a->dedupe_key.len != b->dedupe_key.len) return false;
        for (u32 i = 0; i < a->dedupe_key.len; i++)
            if (!RI(KeyRef__PartialEq__eq)(&a->dedupe_key.ptr[i], &b->dedupe_key.ptr[i])) return false;
        return true;
    }
    return false;
}

jz_err RI(RelExpr__Serialize__serialize)(jz_json_w *w, const jazz_model_RelExpr *e) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, REL_NAMES[e->tag]);
    jz_jw_begin_obj(w);
    switch (e->tag) {
    case JAZZ_REL_TABLE_SCAN:
        jz_jw_key(w, "table"); jazz_model__json_w_str(w, e->table);
        if (e->alias.some) { jz_jw_key(w, "alias"); jazz_model__json_w_str(w, &e->alias.s); }
        break;
    case JAZZ_REL_FILTER:
        jz_jw_key(w, "input"); JZ_TRY(RI(RelExpr__Serialize__serialize)(w, e->input));
        jz_jw_key(w, "predicate"); JZ_TRY(RI(PredicateExpr__Serialize__serialize)(w, &e->predicate));
        break;
    case JAZZ_REL_UNION:
        jz_jw_key(w, "inputs");
        jz_jw_begin_arr(w);
        for (u32 i = 0; i < e->inputs.len; i++) {
            jz_jw_begin_obj(w);
            jz_jw_key(w, "label"); jazz_model__json_w_str(w, &e->inputs.ptr[i].label);
            jz_jw_key(w, "input"); JZ_TRY(RI(RelExpr__Serialize__serialize)(w, &e->inputs.ptr[i].input));
            jz_jw_end_obj(w);
        }
        jz_jw_end_arr(w);
        break;
    case JAZZ_REL_JOIN:
        jz_jw_key(w, "left"); JZ_TRY(RI(RelExpr__Serialize__serialize)(w, e->input));
        jz_jw_key(w, "right"); JZ_TRY(RI(RelExpr__Serialize__serialize)(w, e->right));
        jz_jw_key(w, "on");
        jz_jw_begin_arr(w);
        for (u32 i = 0; i < e->on.len; i++) {
            jz_jw_begin_obj(w);
            jz_jw_key(w, "left"); column_ref_serialize(w, &e->on.ptr[i].left);
            jz_jw_key(w, "right"); column_ref_serialize(w, &e->on.ptr[i].right);
            jz_jw_end_obj(w);
        }
        jz_jw_end_arr(w);
        jz_jw_key(w, "join_kind"); jazz_model__json_w_cstr(w, JOIN_KIND_NAMES[e->join_kind]);
        break;
    case JAZZ_REL_PROJECT:
        jz_jw_key(w, "input"); JZ_TRY(RI(RelExpr__Serialize__serialize)(w, e->input));
        jz_jw_key(w, "columns");
        jz_jw_begin_arr(w);
        for (u32 i = 0; i < e->columns.len; i++) {
            jz_jw_begin_obj(w);
            jz_jw_key(w, "alias"); jazz_model__json_w_str(w, &e->columns.ptr[i].alias);
            jz_jw_key(w, "expr"); key_ref_serialize(w, &e->columns.ptr[i].expr);
            jz_jw_end_obj(w);
        }
        jz_jw_end_arr(w);
        break;
    case JAZZ_REL_GATHER:
        jz_jw_key(w, "seed"); JZ_TRY(RI(RelExpr__Serialize__serialize)(w, e->input));
        jz_jw_key(w, "step"); JZ_TRY(RI(RelExpr__Serialize__serialize)(w, e->right));
        jz_jw_key(w, "frontier_key"); key_ref_serialize(w, &e->frontier_key);
        jz_jw_key(w, "bound");
        if (e->bound.tag == JAZZ_BOUND_FIXPOINT) jazz_model__json_w_cstr(w, "Fixpoint");
        else { jz_jw_begin_obj(w); jz_jw_key(w, "MaxDepth"); jz_jw_u64(w, e->bound.max_depth); jz_jw_end_obj(w); }
        jz_jw_key(w, "dedupe_key");
        jz_jw_begin_arr(w);
        for (u32 i = 0; i < e->dedupe_key.len; i++) key_ref_serialize(w, &e->dedupe_key.ptr[i]);
        jz_jw_end_arr(w);
        break;
    }
    jz_jw_end_obj(w);
    jz_jw_end_obj(w);
    return JZ_OK;
}

static jz_err rel_box(const jz_json *o, const char *name, jazz_model_RelExpr **out) {
    const jz_json *f = jz_json_obj_get(o, name);
    if (!f) return jazz_model__json_missing_field(name);
    jazz_model_RelExpr *b = JZ_NEW(jazz_model_RelExpr);
    jz_err e = RI(RelExpr__Deserialize__deserialize)(f, b);
    if (e.code) { jz_free(b); return e; }
    *out = b;
    return JZ_OK;
}

static jz_err recursion_bound_deserialize(const jz_json *j, jazz_model_RelRecursionBound *out) {
    u32 idx;
    const jz_json *p;
    JZ_TRY(jz_json_variant(j, BOUND_NAMES, 2, &idx, &p));
    out->tag = (u8)idx;
    out->max_depth = 0;
    if (idx == JAZZ_BOUND_MAX_DEPTH) {
        u64 d;
        if (!p) return jazz_model__serde_err("invalid type: unit variant, expected newtype variant");
        JZ_TRY(jazz_model__json_expect_u64(p, &d));
        out->max_depth = (u32)d;
    }
    return JZ_OK;
}

static jz_err rel_fields(u32 idx, const jz_json *p, jazz_model_RelExpr *out) {
    const jz_json *f;
    JZ_TRY(jz_json_expect_obj(p, "struct variant RelExpr"));
    switch (idx) {
    case JAZZ_REL_TABLE_SCAN: {
        jz_str table;
        JZ_TRY(jz_json_field_str(p, "table", &table));
        out->table = jazz_model__model__public_api__types__schema__TableName__new(table);
        f = jz_json_obj_get(p, "alias");
        if (f && f->kind != JZ_JSON_NULL) {
            JZ_TRY(jazz_model__json_expect_str(f, &out->alias.s));
            out->alias.some = true;
        }
        return JZ_OK;
    }
    case JAZZ_REL_FILTER:
        JZ_TRY(rel_box(p, "input", &out->input));
        f = jz_json_obj_get(p, "predicate");
        if (!f) return jazz_model__json_missing_field("predicate");
        return RI(PredicateExpr__Deserialize__deserialize)(f, &out->predicate);
    case JAZZ_REL_UNION:
        f = jz_json_obj_get(p, "inputs");
        if (!f) return jazz_model__json_missing_field("inputs");
        JZ_TRY(jazz_model__json_expect_arr(f, "a sequence"));
        for (u32 i = 0; i < f->v.arr.len; i++) {
            const jz_json *a = f->v.arr.ptr[i];
            jazz_model_RelUnionArm arm;
            memset(&arm, 0, sizeof arm);
            JZ_TRY(jz_json_expect_obj(a, "struct UnionArm"));
            JZ_TRY(jz_json_field_str(a, "label", &arm.label));
            const jz_json *in = jz_json_obj_get(a, "input");
            if (!in) { jz_str_drop(&arm.label); return jazz_model__json_missing_field("input"); }
            jz_err e = RI(RelExpr__Deserialize__deserialize)(in, &arm.input);
            if (e.code) { jz_str_drop(&arm.label); return e; }
            JZ_VEC_PUSH(out->inputs, arm);
        }
        return JZ_OK;
    case JAZZ_REL_JOIN: {
        u32 kind;
        JZ_TRY(rel_box(p, "left", &out->input));
        JZ_TRY(rel_box(p, "right", &out->right));
        f = jz_json_obj_get(p, "on");
        if (!f) return jazz_model__json_missing_field("on");
        JZ_TRY(jazz_model__json_expect_arr(f, "a sequence"));
        for (u32 i = 0; i < f->v.arr.len; i++) {
            jazz_model_RelJoinCondition jc;
            memset(&jc, 0, sizeof jc);
            JZ_TRY(jz_json_expect_obj(f->v.arr.ptr[i], "struct JoinCondition"));
            JZ_TRY(col_field(f->v.arr.ptr[i], "left", &jc.left));
            jz_err e = col_field(f->v.arr.ptr[i], "right", &jc.right);
            if (e.code) { RI(ColumnRef__drop)(&jc.left); return e; }
            JZ_VEC_PUSH(out->on, jc);
        }
        f = jz_json_obj_get(p, "join_kind");
        if (!f) return jazz_model__json_missing_field("join_kind");
        JZ_TRY(jazz_model__json_unit_variant(f, JOIN_KIND_NAMES, 2, &kind));
        out->join_kind = (u8)kind;
        return JZ_OK;
    }
    case JAZZ_REL_PROJECT:
        JZ_TRY(rel_box(p, "input", &out->input));
        f = jz_json_obj_get(p, "columns");
        if (!f) return jazz_model__json_missing_field("columns");
        JZ_TRY(jazz_model__json_expect_arr(f, "a sequence"));
        for (u32 i = 0; i < f->v.arr.len; i++) {
            const jz_json *c = f->v.arr.ptr[i];
            jazz_model_RelProjectColumn pc;
            memset(&pc, 0, sizeof pc);
            JZ_TRY(jz_json_expect_obj(c, "struct ProjectColumn"));
            JZ_TRY(jz_json_field_str(c, "alias", &pc.alias));
            const jz_json *x = jz_json_obj_get(c, "expr");
            jz_err e = x ? key_ref_deserialize(x, &pc.expr) : jazz_model__json_missing_field("expr");
            if (e.code) { jz_str_drop(&pc.alias); return e; }
            JZ_VEC_PUSH(out->columns, pc);
        }
        return JZ_OK;
    case JAZZ_REL_GATHER:
        JZ_TRY(rel_box(p, "seed", &out->input));
        JZ_TRY(rel_box(p, "step", &out->right));
        f = jz_json_obj_get(p, "frontier_key");
        if (!f) return jazz_model__json_missing_field("frontier_key");
        JZ_TRY(key_ref_deserialize(f, &out->frontier_key));
        f = jz_json_obj_get(p, "bound");
        if (!f) return jazz_model__json_missing_field("bound");
        JZ_TRY(recursion_bound_deserialize(f, &out->bound));
        f = jz_json_obj_get(p, "dedupe_key");
        if (!f) return jazz_model__json_missing_field("dedupe_key");
        JZ_TRY(jazz_model__json_expect_arr(f, "a sequence"));
        for (u32 i = 0; i < f->v.arr.len; i++) {
            jazz_model_RelKeyRef k;
            JZ_TRY(key_ref_deserialize(f->v.arr.ptr[i], &k));
            JZ_VEC_PUSH(out->dedupe_key, k);
        }
        return JZ_OK;
    }
    return JZ_OK;
}

jz_err RI(RelExpr__Deserialize__deserialize)(const jz_json *j, jazz_model_RelExpr *out) {
    u32 idx;
    const jz_json *p;
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_json_variant(j, REL_NAMES, 6, &idx, &p));
    if (!p) return jazz_model__serde_err("invalid type: unit variant, expected struct variant");
    out->tag = (u8)idx;
    jz_err e = rel_fields(idx, p, out);
    if (e.code) RI(RelExpr__drop)(out);
    return e;
}
