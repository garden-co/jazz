/* model/public_api/policy.rs: row-level policy expressions (CmpOp, PolicyValue,
 * Operation, PolicyExpr) with their internally tagged JSON serde. */
#include "jazz_model/jazz_model.h"

#define PP(x) jazz_model__model__public_api__policy__##x
#define VALUE(x) jazz_model__model__public_api__types__value__Value__##x
#define REL(x) jazz_model__model__public_api__relation_ir__##x

static const char *const CMP_OP_NAMES[] = {"Eq", "Ne", "Lt", "Le", "Gt", "Ge"};
static const char *const OPERATION_NAMES[] = {"Select", "Insert", "Update", "Delete"};
static const char *const POLICY_VALUE_NAMES[] = {"Literal", "SessionRef"};
static const char *const POLICY_EXPR_NAMES[JAZZ_PE_COUNT] = {
    "Cmp", "SessionCmp", "IsNull", "SessionIsNull", "IsNotNull", "SessionIsNotNull", "Contains",
    "SessionContains", "In", "InList", "SessionInList", "Exists", "ExistsRel", "Inherits",
    "InheritsReferencing", "And", "Or", "Not", "True", "False",
};

void PP(CmpOp__Debug__fmt)(jz_str *out, u8 op) { jz_str_push_cstr(out, CMP_OP_NAMES[op]); }
void PP(Operation__Debug__fmt)(jz_str *out, u8 op) { jz_str_push_cstr(out, OPERATION_NAMES[op]); }

void PP(Operation__Display__fmt)(jz_str *out, u8 op) {
    switch (op) {
    case JAZZ_OP_SELECT: jz_str_push_cstr(out, "SELECT"); break;
    case JAZZ_OP_INSERT: jz_str_push_cstr(out, "INSERT"); break;
    case JAZZ_OP_UPDATE: jz_str_push_cstr(out, "UPDATE"); break;
    case JAZZ_OP_DELETE: jz_str_push_cstr(out, "DELETE"); break;
    }
}

/* ---------------------------------------------------------------- PolicyValue */
void PP(PolicyValue__drop)(jazz_model_PolicyValue *v) {
    if (v->tag == JAZZ_POLVAL_LITERAL) VALUE(drop)(&v->literal);
    else jazz_model__StrVec__drop(&v->path);
}
static void policy_value_drop_v(void *v) { PP(PolicyValue__drop)((jazz_model_PolicyValue *)v); }

void PP(PolicyValue__Clone__clone)(jazz_model_PolicyValue *dst, const jazz_model_PolicyValue *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    if (src->tag == JAZZ_POLVAL_LITERAL) VALUE(Clone__clone)(&dst->literal, &src->literal);
    else jazz_model__StrVec__clone(&dst->path, &src->path);
}
static void policy_value_clone_v(void *dst, const void *src) { PP(PolicyValue__Clone__clone)(dst, src); }

bool PP(PolicyValue__PartialEq__eq)(const jazz_model_PolicyValue *a, const jazz_model_PolicyValue *b) {
    if (a->tag != b->tag) return false;
    if (a->tag == JAZZ_POLVAL_LITERAL) return VALUE(PartialEq__eq)(&a->literal, &b->literal);
    return jazz_model__StrVec__eq(&a->path, &b->path);
}

static void policy_value_debug(jz_str *out, const jazz_model_PolicyValue *v) {
    if (v->tag == JAZZ_POLVAL_LITERAL) {
        jz_str_push_cstr(out, "Literal(");
        VALUE(Debug__fmt)(out, &v->literal);
    } else {
        jz_str_push_cstr(out, "SessionRef(");
        jazz_model__StrVec__Debug__fmt(out, &v->path);
    }
    jz_str_push_char(out, ')');
}

jz_err PP(PolicyValue__Serialize__serialize)(jz_json_w *w, const jazz_model_PolicyValue *v) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "type");
    jazz_model__json_w_cstr(w, POLICY_VALUE_NAMES[v->tag]);
    if (v->tag == JAZZ_POLVAL_LITERAL) {
        jz_jw_key(w, "value");
        JZ_TRY(VALUE(Serialize__serialize)(w, &v->literal));
    } else {
        jz_jw_key(w, "path");
        jazz_model__StrVec__serialize_json(w, &v->path);
    }
    jz_jw_end_obj(w);
    return JZ_OK;
}

/* Internally tagged enums also accept the sequence form `["Variant", field0, ...]`. */
static jz_err tagged_variant(const jz_json *j, const char *const *names, u32 n, u32 *idx, bool *seq) {
    if (j && j->kind == JZ_JSON_ARR) {
        *seq = true;
        if (j->v.arr.len == 0) return jazz_model__serde_err("invalid length 0, expected tag");
        const jz_json *tag = j->v.arr.ptr[0];
        if (tag->kind != JZ_JSON_STR) return jazz_model__serde_err("invalid type: expected variant identifier");
        for (u32 i = 0; i < n; i++)
            if (jz_str_eq_cstr(&tag->v.s, names[i])) { *idx = i; return JZ_OK; }
        return jazz_model__json_unknown_variant(&tag->v.s, names, n);
    }
    *seq = false;
    return jazz_model__json_internal_tag(j, "type", names, n, idx);
}

static const jz_json *tagged_field(const jz_json *j, bool seq, u32 pos, const char *name) {
    if (seq) return pos + 1 < j->v.arr.len ? j->v.arr.ptr[pos + 1] : NULL;
    return jz_json_obj_get(j, name);
}

static jz_err required(const jz_json *f, const char *name) {
    return f ? JZ_OK : jazz_model__json_missing_field(name);
}

/* one_field_payload! adapters: the newtype payload is a one-field struct. */
static const jz_json *one_field_payload(const jz_json *j, bool seq, const char *field) {
    if (seq) {
        /* sequence form carries the payload itself */
        return j->v.arr.len > 1 ? j->v.arr.ptr[1] : NULL;
    }
    return jz_json_obj_get(j, field);
}

jz_err PP(PolicyValue__Deserialize__deserialize)(const jz_json *j, jazz_model_PolicyValue *out) {
    u32 idx;
    bool seq;
    memset(out, 0, sizeof *out);
    JZ_TRY(tagged_variant(j, POLICY_VALUE_NAMES, 2, &idx, &seq));
    out->tag = (u8)idx;
    if (idx == JAZZ_POLVAL_LITERAL) {
        const jz_json *f = one_field_payload(j, seq, "value");
        JZ_TRY(required(f, "value"));
        return VALUE(Deserialize__deserialize)(f, &out->literal);
    }
    const jz_json *f = one_field_payload(j, seq, "path");
    JZ_TRY(required(f, "path"));
    return jazz_model__StrVec__deserialize_json(f, &out->path);
}

/* ---------------------------------------------------------------- PolicyExpr */
jazz_model_PolicyExpr PP(PolicyExpr__simple)(u8 tag) {
    jazz_model_PolicyExpr e;
    memset(&e, 0, sizeof e);
    e.tag = tag;
    return e;
}

void PP(PolicyExpr__drop)(jazz_model_PolicyExpr *e) {
    switch (e->tag) {
    case JAZZ_PE_CMP: jz_str_drop(&e->column); PP(PolicyValue__drop)(&e->pvalue); break;
    case JAZZ_PE_SESSION_CMP: jazz_model__StrVec__drop(&e->path); VALUE(drop)(&e->value); break;
    case JAZZ_PE_IS_NULL: case JAZZ_PE_IS_NOT_NULL: jz_str_drop(&e->column); break;
    case JAZZ_PE_SESSION_IS_NULL: case JAZZ_PE_SESSION_IS_NOT_NULL: jazz_model__StrVec__drop(&e->path); break;
    case JAZZ_PE_CONTAINS: jz_str_drop(&e->column); PP(PolicyValue__drop)(&e->pvalue); break;
    case JAZZ_PE_SESSION_CONTAINS: jazz_model__StrVec__drop(&e->path); VALUE(drop)(&e->value); break;
    case JAZZ_PE_IN: jz_str_drop(&e->column); jazz_model__StrVec__drop(&e->path); break;
    case JAZZ_PE_IN_LIST:
        jz_str_drop(&e->column);
        jz_vec_drop(&e->pvalues, sizeof(jazz_model_PolicyValue), policy_value_drop_v);
        break;
    case JAZZ_PE_SESSION_IN_LIST:
        jazz_model__StrVec__drop(&e->path);
        jazz_model__model__public_api__types__value__ValueVec__drop(&e->values);
        break;
    case JAZZ_PE_EXISTS:
        jz_str_drop(&e->table);
        if (e->inner) { PP(PolicyExpr__drop)(e->inner); jz_free(e->inner); }
        break;
    case JAZZ_PE_EXISTS_REL: REL(RelExpr__drop)(&e->rel); break;
    case JAZZ_PE_INHERITS: jz_str_drop(&e->column); break;
    case JAZZ_PE_INHERITS_REFERENCING: jz_str_drop(&e->table); jz_str_drop(&e->column); break;
    case JAZZ_PE_AND: case JAZZ_PE_OR:
        jz_vec_drop(&e->exprs, sizeof(jazz_model_PolicyExpr), PP(PolicyExpr__drop_v));
        break;
    case JAZZ_PE_NOT: if (e->inner) { PP(PolicyExpr__drop)(e->inner); jz_free(e->inner); } break;
    default: break;
    }
    memset(e, 0, sizeof *e);
    e->tag = JAZZ_PE_TRUE;
}
void PP(PolicyExpr__drop_v)(void *e) { PP(PolicyExpr__drop)((jazz_model_PolicyExpr *)e); }

static void policy_expr_clone_v(void *dst, const void *src) { PP(PolicyExpr__Clone__clone)(dst, src); }

static jazz_model_PolicyExpr *box_clone(const jazz_model_PolicyExpr *e) {
    jazz_model_PolicyExpr *b = JZ_NEW(jazz_model_PolicyExpr);
    PP(PolicyExpr__Clone__clone)(b, e);
    return b;
}

void PP(PolicyExpr__Clone__clone)(jazz_model_PolicyExpr *dst, const jazz_model_PolicyExpr *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    dst->op = src->op;
    dst->has_max_depth = src->has_max_depth;
    dst->max_depth = src->max_depth;
    switch (src->tag) {
    case JAZZ_PE_CMP: case JAZZ_PE_CONTAINS:
        dst->column = jz_str_clone(&src->column);
        PP(PolicyValue__Clone__clone)(&dst->pvalue, &src->pvalue);
        break;
    case JAZZ_PE_SESSION_CMP: case JAZZ_PE_SESSION_CONTAINS:
        jazz_model__StrVec__clone(&dst->path, &src->path);
        VALUE(Clone__clone)(&dst->value, &src->value);
        break;
    case JAZZ_PE_IS_NULL: case JAZZ_PE_IS_NOT_NULL: case JAZZ_PE_INHERITS:
        dst->column = jz_str_clone(&src->column);
        break;
    case JAZZ_PE_SESSION_IS_NULL: case JAZZ_PE_SESSION_IS_NOT_NULL:
        jazz_model__StrVec__clone(&dst->path, &src->path);
        break;
    case JAZZ_PE_IN:
        dst->column = jz_str_clone(&src->column);
        jazz_model__StrVec__clone(&dst->path, &src->path);
        break;
    case JAZZ_PE_IN_LIST:
        dst->column = jz_str_clone(&src->column);
        jz_vec_clone(&dst->pvalues, &src->pvalues, sizeof(jazz_model_PolicyValue), policy_value_clone_v);
        break;
    case JAZZ_PE_SESSION_IN_LIST:
        jazz_model__StrVec__clone(&dst->path, &src->path);
        jazz_model__model__public_api__types__value__ValueVec__clone(&dst->values, &src->values);
        break;
    case JAZZ_PE_EXISTS:
        dst->table = jz_str_clone(&src->table);
        dst->inner = box_clone(src->inner);
        break;
    case JAZZ_PE_EXISTS_REL: REL(RelExpr__Clone__clone)(&dst->rel, &src->rel); break;
    case JAZZ_PE_INHERITS_REFERENCING:
        dst->table = jz_str_clone(&src->table);
        dst->column = jz_str_clone(&src->column);
        break;
    case JAZZ_PE_AND: case JAZZ_PE_OR:
        jz_vec_clone(&dst->exprs, &src->exprs, sizeof(jazz_model_PolicyExpr), policy_expr_clone_v);
        break;
    case JAZZ_PE_NOT: dst->inner = box_clone(src->inner); break;
    default: break;
    }
}

static bool policy_value_vec_eq(const jazz_model_PolicyValueVec *a, const jazz_model_PolicyValueVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!PP(PolicyValue__PartialEq__eq)(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}

static bool policy_expr_vec_eq(const jazz_model_PolicyExprVec *a, const jazz_model_PolicyExprVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!PP(PolicyExpr__PartialEq__eq)(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}

bool PP(PolicyExpr__PartialEq__eq)(const jazz_model_PolicyExpr *a, const jazz_model_PolicyExpr *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_PE_CMP:
        return jz_str_eq(&a->column, &b->column) && a->op == b->op && PP(PolicyValue__PartialEq__eq)(&a->pvalue, &b->pvalue);
    case JAZZ_PE_SESSION_CMP:
        return jazz_model__StrVec__eq(&a->path, &b->path) && a->op == b->op && VALUE(PartialEq__eq)(&a->value, &b->value);
    case JAZZ_PE_IS_NULL: case JAZZ_PE_IS_NOT_NULL: return jz_str_eq(&a->column, &b->column);
    case JAZZ_PE_SESSION_IS_NULL: case JAZZ_PE_SESSION_IS_NOT_NULL: return jazz_model__StrVec__eq(&a->path, &b->path);
    case JAZZ_PE_CONTAINS:
        return jz_str_eq(&a->column, &b->column) && PP(PolicyValue__PartialEq__eq)(&a->pvalue, &b->pvalue);
    case JAZZ_PE_SESSION_CONTAINS:
        return jazz_model__StrVec__eq(&a->path, &b->path) && VALUE(PartialEq__eq)(&a->value, &b->value);
    case JAZZ_PE_IN: return jz_str_eq(&a->column, &b->column) && jazz_model__StrVec__eq(&a->path, &b->path);
    case JAZZ_PE_IN_LIST: return jz_str_eq(&a->column, &b->column) && policy_value_vec_eq(&a->pvalues, &b->pvalues);
    case JAZZ_PE_SESSION_IN_LIST:
        return jazz_model__StrVec__eq(&a->path, &b->path) &&
               jazz_model__model__public_api__types__value__ValueVec__eq(&a->values, &b->values);
    case JAZZ_PE_EXISTS: return jz_str_eq(&a->table, &b->table) && PP(PolicyExpr__PartialEq__eq)(a->inner, b->inner);
    case JAZZ_PE_EXISTS_REL: return REL(RelExpr__PartialEq__eq)(&a->rel, &b->rel);
    case JAZZ_PE_INHERITS:
        return a->op == b->op && jz_str_eq(&a->column, &b->column) && a->has_max_depth == b->has_max_depth &&
               (!a->has_max_depth || a->max_depth == b->max_depth);
    case JAZZ_PE_INHERITS_REFERENCING:
        return a->op == b->op && jz_str_eq(&a->table, &b->table) && jz_str_eq(&a->column, &b->column) &&
               a->has_max_depth == b->has_max_depth && (!a->has_max_depth || a->max_depth == b->max_depth);
    case JAZZ_PE_AND: case JAZZ_PE_OR: return policy_expr_vec_eq(&a->exprs, &b->exprs);
    case JAZZ_PE_NOT: return PP(PolicyExpr__PartialEq__eq)(a->inner, b->inner);
    default: return true;
    }
}

static void debug_field_str(jz_str *out, const char *name, const jz_str *s) {
    jz_str_appendf(out, "%s: ", name);
    jazz_model__debug_str(out, s->ptr, s->len);
}

static void debug_max_depth(jz_str *out, const jazz_model_PolicyExpr *e) {
    if (e->has_max_depth) jz_str_appendf(out, ", max_depth: Some(%u) }", e->max_depth);
    else jz_str_push_cstr(out, ", max_depth: None }");
}

void PP(PolicyExpr__Debug__fmt)(jz_str *out, const jazz_model_PolicyExpr *e) {
    jz_str_push_cstr(out, POLICY_EXPR_NAMES[e->tag]);
    switch (e->tag) {
    case JAZZ_PE_CMP:
        jz_str_push_cstr(out, " { ");
        debug_field_str(out, "column", &e->column);
        jz_str_appendf(out, ", op: %s, value: ", CMP_OP_NAMES[e->op]);
        policy_value_debug(out, &e->pvalue);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_PE_SESSION_CMP:
        jz_str_push_cstr(out, " { path: ");
        jazz_model__StrVec__Debug__fmt(out, &e->path);
        jz_str_appendf(out, ", op: %s, value: ", CMP_OP_NAMES[e->op]);
        VALUE(Debug__fmt)(out, &e->value);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_PE_IS_NULL: case JAZZ_PE_IS_NOT_NULL:
        jz_str_push_cstr(out, " { ");
        debug_field_str(out, "column", &e->column);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_PE_SESSION_IS_NULL: case JAZZ_PE_SESSION_IS_NOT_NULL:
        jz_str_push_cstr(out, " { path: ");
        jazz_model__StrVec__Debug__fmt(out, &e->path);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_PE_CONTAINS:
        jz_str_push_cstr(out, " { ");
        debug_field_str(out, "column", &e->column);
        jz_str_push_cstr(out, ", value: ");
        policy_value_debug(out, &e->pvalue);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_PE_SESSION_CONTAINS:
        jz_str_push_cstr(out, " { path: ");
        jazz_model__StrVec__Debug__fmt(out, &e->path);
        jz_str_push_cstr(out, ", value: ");
        VALUE(Debug__fmt)(out, &e->value);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_PE_IN:
        jz_str_push_cstr(out, " { ");
        debug_field_str(out, "column", &e->column);
        jz_str_push_cstr(out, ", session_path: ");
        jazz_model__StrVec__Debug__fmt(out, &e->path);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_PE_IN_LIST:
        jz_str_push_cstr(out, " { ");
        debug_field_str(out, "column", &e->column);
        jz_str_push_cstr(out, ", values: [");
        for (u32 i = 0; i < e->pvalues.len; i++) {
            if (i) jz_str_push_cstr(out, ", ");
            policy_value_debug(out, &e->pvalues.ptr[i]);
        }
        jz_str_push_cstr(out, "] }");
        break;
    case JAZZ_PE_SESSION_IN_LIST:
        jz_str_push_cstr(out, " { path: ");
        jazz_model__StrVec__Debug__fmt(out, &e->path);
        jz_str_push_cstr(out, ", values: [");
        for (u32 i = 0; i < e->values.len; i++) {
            if (i) jz_str_push_cstr(out, ", ");
            VALUE(Debug__fmt)(out, &e->values.ptr[i]);
        }
        jz_str_push_cstr(out, "] }");
        break;
    case JAZZ_PE_EXISTS:
        jz_str_push_cstr(out, " { ");
        debug_field_str(out, "table", &e->table);
        jz_str_push_cstr(out, ", condition: ");
        PP(PolicyExpr__Debug__fmt)(out, e->inner);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_PE_EXISTS_REL:
        jz_str_push_cstr(out, " { rel: .. }");
        break;
    case JAZZ_PE_INHERITS:
        jz_str_appendf(out, " { operation: %s, ", OPERATION_NAMES[e->op]);
        debug_field_str(out, "via_column", &e->column);
        debug_max_depth(out, e);
        break;
    case JAZZ_PE_INHERITS_REFERENCING:
        jz_str_appendf(out, " { operation: %s, ", OPERATION_NAMES[e->op]);
        debug_field_str(out, "source_table", &e->table);
        jz_str_push_cstr(out, ", ");
        debug_field_str(out, "via_column", &e->column);
        debug_max_depth(out, e);
        break;
    case JAZZ_PE_AND: case JAZZ_PE_OR:
        jz_str_push_cstr(out, "([");
        for (u32 i = 0; i < e->exprs.len; i++) {
            if (i) jz_str_push_cstr(out, ", ");
            PP(PolicyExpr__Debug__fmt)(out, &e->exprs.ptr[i]);
        }
        jz_str_push_cstr(out, "])");
        break;
    case JAZZ_PE_NOT:
        jz_str_push_char(out, '(');
        PP(PolicyExpr__Debug__fmt)(out, e->inner);
        jz_str_push_char(out, ')');
        break;
    default: break;
    }
}

static void write_max_depth(jz_json_w *w, const jazz_model_PolicyExpr *e) {
    jz_jw_key(w, "max_depth");
    if (e->has_max_depth) jz_jw_u64(w, e->max_depth);
    else jz_jw_null(w);
}

jz_err PP(PolicyExpr__Serialize__serialize)(jz_json_w *w, const jazz_model_PolicyExpr *e) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "type");
    jazz_model__json_w_cstr(w, POLICY_EXPR_NAMES[e->tag]);
    switch (e->tag) {
    case JAZZ_PE_CMP:
        jz_jw_key(w, "column"); jazz_model__json_w_str(w, &e->column);
        jz_jw_key(w, "op"); jazz_model__json_w_cstr(w, CMP_OP_NAMES[e->op]);
        jz_jw_key(w, "value"); JZ_TRY(PP(PolicyValue__Serialize__serialize)(w, &e->pvalue));
        break;
    case JAZZ_PE_SESSION_CMP:
        jz_jw_key(w, "path"); jazz_model__StrVec__serialize_json(w, &e->path);
        jz_jw_key(w, "op"); jazz_model__json_w_cstr(w, CMP_OP_NAMES[e->op]);
        jz_jw_key(w, "value"); JZ_TRY(VALUE(Serialize__serialize)(w, &e->value));
        break;
    case JAZZ_PE_IS_NULL: case JAZZ_PE_IS_NOT_NULL:
        jz_jw_key(w, "column"); jazz_model__json_w_str(w, &e->column);
        break;
    case JAZZ_PE_SESSION_IS_NULL: case JAZZ_PE_SESSION_IS_NOT_NULL:
        jz_jw_key(w, "path"); jazz_model__StrVec__serialize_json(w, &e->path);
        break;
    case JAZZ_PE_CONTAINS:
        jz_jw_key(w, "column"); jazz_model__json_w_str(w, &e->column);
        jz_jw_key(w, "value"); JZ_TRY(PP(PolicyValue__Serialize__serialize)(w, &e->pvalue));
        break;
    case JAZZ_PE_SESSION_CONTAINS:
        jz_jw_key(w, "path"); jazz_model__StrVec__serialize_json(w, &e->path);
        jz_jw_key(w, "value"); JZ_TRY(VALUE(Serialize__serialize)(w, &e->value));
        break;
    case JAZZ_PE_IN:
        jz_jw_key(w, "column"); jazz_model__json_w_str(w, &e->column);
        jz_jw_key(w, "session_path"); jazz_model__StrVec__serialize_json(w, &e->path);
        break;
    case JAZZ_PE_IN_LIST:
        jz_jw_key(w, "column"); jazz_model__json_w_str(w, &e->column);
        jz_jw_key(w, "values");
        jz_jw_begin_arr(w);
        for (u32 i = 0; i < e->pvalues.len; i++) JZ_TRY(PP(PolicyValue__Serialize__serialize)(w, &e->pvalues.ptr[i]));
        jz_jw_end_arr(w);
        break;
    case JAZZ_PE_SESSION_IN_LIST:
        jz_jw_key(w, "path"); jazz_model__StrVec__serialize_json(w, &e->path);
        jz_jw_key(w, "values");
        jz_jw_begin_arr(w);
        for (u32 i = 0; i < e->values.len; i++) JZ_TRY(VALUE(Serialize__serialize)(w, &e->values.ptr[i]));
        jz_jw_end_arr(w);
        break;
    case JAZZ_PE_EXISTS:
        jz_jw_key(w, "table"); jazz_model__json_w_str(w, &e->table);
        jz_jw_key(w, "condition"); JZ_TRY(PP(PolicyExpr__Serialize__serialize)(w, e->inner));
        break;
    case JAZZ_PE_EXISTS_REL:
        jz_jw_key(w, "rel"); JZ_TRY(REL(RelExpr__Serialize__serialize)(w, &e->rel));
        break;
    case JAZZ_PE_INHERITS:
        jz_jw_key(w, "operation"); jazz_model__json_w_cstr(w, OPERATION_NAMES[e->op]);
        jz_jw_key(w, "via_column"); jazz_model__json_w_str(w, &e->column);
        write_max_depth(w, e);
        break;
    case JAZZ_PE_INHERITS_REFERENCING:
        jz_jw_key(w, "operation"); jazz_model__json_w_cstr(w, OPERATION_NAMES[e->op]);
        jz_jw_key(w, "source_table"); jazz_model__json_w_str(w, &e->table);
        jz_jw_key(w, "via_column"); jazz_model__json_w_str(w, &e->column);
        write_max_depth(w, e);
        break;
    case JAZZ_PE_AND: case JAZZ_PE_OR:
        jz_jw_key(w, "exprs");
        jz_jw_begin_arr(w);
        for (u32 i = 0; i < e->exprs.len; i++) JZ_TRY(PP(PolicyExpr__Serialize__serialize)(w, &e->exprs.ptr[i]));
        jz_jw_end_arr(w);
        break;
    case JAZZ_PE_NOT:
        jz_jw_key(w, "expr"); JZ_TRY(PP(PolicyExpr__Serialize__serialize)(w, e->inner));
        break;
    default: break;
    }
    jz_jw_end_obj(w);
    return JZ_OK;
}

static jz_err str_field(const jz_json *j, bool seq, u32 pos, const char *name, jz_str *out) {
    const jz_json *f = tagged_field(j, seq, pos, name);
    JZ_TRY(required(f, name));
    return jazz_model__json_expect_str(f, out);
}

static jz_err path_field(const jz_json *j, bool seq, u32 pos, const char *name, jazz_model_StrVec *out) {
    const jz_json *f = tagged_field(j, seq, pos, name);
    JZ_TRY(required(f, name));
    return jazz_model__StrVec__deserialize_json(f, out);
}

static jz_err unit_field(const jz_json *j, bool seq, u32 pos, const char *name, const char *const *names, u32 n, u8 *out) {
    u32 idx;
    const jz_json *f = tagged_field(j, seq, pos, name);
    JZ_TRY(required(f, name));
    JZ_TRY(jazz_model__json_unit_variant(f, names, n, &idx));
    *out = (u8)idx;
    return JZ_OK;
}

static jz_err max_depth_field(const jz_json *j, bool seq, u32 pos, jazz_model_PolicyExpr *e) {
    const jz_json *f = tagged_field(j, seq, pos, "max_depth");
    if (!f || f->kind == JZ_JSON_NULL) return JZ_OK;
    u64 d;
    JZ_TRY(jazz_model__json_expect_u64(f, &d));
    if (d > UINT32_MAX) return jazz_model__serde_err("invalid value: integer `%llu`, expected usize", (unsigned long long)d);
    e->has_max_depth = true;
    e->max_depth = (u32)d;
    return JZ_OK;
}

static jz_err boxed_expr(const jz_json *f, const char *name, jazz_model_PolicyExpr **out) {
    JZ_TRY(required(f, name));
    jazz_model_PolicyExpr *b = JZ_NEW(jazz_model_PolicyExpr);
    jz_err e = PP(PolicyExpr__Deserialize__deserialize)(f, b);
    if (e.code) { jz_free(b); return e; }
    *out = b;
    return JZ_OK;
}

static jz_err expr_vec(const jz_json *f, jazz_model_PolicyExprVec *out) {
    JZ_TRY(required(f, "exprs"));
    if (f->kind != JZ_JSON_ARR) {
        if (f->kind == JZ_JSON_U64 || f->kind == JZ_JSON_I64)
            return jazz_model__serde_err("invalid type: integer `%lld`, expected a sequence", (long long)f->v.i);
        return jazz_model__serde_err("invalid type: expected a sequence");
    }
    for (u32 i = 0; i < f->v.arr.len; i++) {
        jazz_model_PolicyExpr x;
        JZ_TRY(PP(PolicyExpr__Deserialize__deserialize)(f->v.arr.ptr[i], &x));
        JZ_VEC_PUSH(*out, x);
    }
    return JZ_OK;
}

static jz_err policy_expr_fields(const jz_json *j, bool seq, u32 idx, jazz_model_PolicyExpr *out) {
    const jz_json *f;
    switch (idx) {
    case JAZZ_PE_CMP:
        JZ_TRY(str_field(j, seq, 0, "column", &out->column));
        JZ_TRY(unit_field(j, seq, 1, "op", CMP_OP_NAMES, 6, &out->op));
        f = tagged_field(j, seq, 2, "value");
        JZ_TRY(required(f, "value"));
        return PP(PolicyValue__Deserialize__deserialize)(f, &out->pvalue);
    case JAZZ_PE_SESSION_CMP:
        JZ_TRY(path_field(j, seq, 0, "path", &out->path));
        JZ_TRY(unit_field(j, seq, 1, "op", CMP_OP_NAMES, 6, &out->op));
        f = tagged_field(j, seq, 2, "value");
        JZ_TRY(required(f, "value"));
        return VALUE(Deserialize__deserialize)(f, &out->value);
    case JAZZ_PE_IS_NULL: case JAZZ_PE_IS_NOT_NULL:
        return str_field(j, seq, 0, "column", &out->column);
    case JAZZ_PE_SESSION_IS_NULL: case JAZZ_PE_SESSION_IS_NOT_NULL:
        return path_field(j, seq, 0, "path", &out->path);
    case JAZZ_PE_CONTAINS:
        JZ_TRY(str_field(j, seq, 0, "column", &out->column));
        f = tagged_field(j, seq, 1, "value");
        JZ_TRY(required(f, "value"));
        return PP(PolicyValue__Deserialize__deserialize)(f, &out->pvalue);
    case JAZZ_PE_SESSION_CONTAINS:
        JZ_TRY(path_field(j, seq, 0, "path", &out->path));
        f = tagged_field(j, seq, 1, "value");
        JZ_TRY(required(f, "value"));
        return VALUE(Deserialize__deserialize)(f, &out->value);
    case JAZZ_PE_IN:
        JZ_TRY(str_field(j, seq, 0, "column", &out->column));
        return path_field(j, seq, 1, "session_path", &out->path);
    case JAZZ_PE_IN_LIST:
        JZ_TRY(str_field(j, seq, 0, "column", &out->column));
        f = tagged_field(j, seq, 1, "values");
        JZ_TRY(required(f, "values"));
        JZ_TRY(jazz_model__json_expect_arr(f, "a sequence"));
        for (u32 i = 0; i < f->v.arr.len; i++) {
            jazz_model_PolicyValue pv;
            JZ_TRY(PP(PolicyValue__Deserialize__deserialize)(f->v.arr.ptr[i], &pv));
            JZ_VEC_PUSH(out->pvalues, pv);
        }
        return JZ_OK;
    case JAZZ_PE_SESSION_IN_LIST:
        JZ_TRY(path_field(j, seq, 0, "path", &out->path));
        f = tagged_field(j, seq, 1, "values");
        JZ_TRY(required(f, "values"));
        JZ_TRY(jazz_model__json_expect_arr(f, "a sequence"));
        for (u32 i = 0; i < f->v.arr.len; i++) {
            jazz_model_public_Value v;
            JZ_TRY(VALUE(Deserialize__deserialize)(f->v.arr.ptr[i], &v));
            JZ_VEC_PUSH(out->values, v);
        }
        return JZ_OK;
    case JAZZ_PE_EXISTS:
        JZ_TRY(str_field(j, seq, 0, "table", &out->table));
        return boxed_expr(tagged_field(j, seq, 1, "condition"), "condition", &out->inner);
    case JAZZ_PE_EXISTS_REL:
        f = tagged_field(j, seq, 0, "rel");
        JZ_TRY(required(f, "rel"));
        return REL(RelExpr__Deserialize__deserialize)(f, &out->rel);
    case JAZZ_PE_INHERITS:
        JZ_TRY(unit_field(j, seq, 0, "operation", OPERATION_NAMES, 4, &out->op));
        JZ_TRY(str_field(j, seq, 1, "via_column", &out->column));
        return max_depth_field(j, seq, 2, out);
    case JAZZ_PE_INHERITS_REFERENCING:
        JZ_TRY(unit_field(j, seq, 0, "operation", OPERATION_NAMES, 4, &out->op));
        JZ_TRY(str_field(j, seq, 1, "source_table", &out->table));
        JZ_TRY(str_field(j, seq, 2, "via_column", &out->column));
        return max_depth_field(j, seq, 3, out);
    case JAZZ_PE_AND: case JAZZ_PE_OR:
        return expr_vec(one_field_payload(j, seq, "exprs"), &out->exprs);
    case JAZZ_PE_NOT:
        return boxed_expr(one_field_payload(j, seq, "expr"), "expr", &out->inner);
    default: return JZ_OK;
    }
}

jz_err PP(PolicyExpr__Deserialize__deserialize)(const jz_json *j, jazz_model_PolicyExpr *out) {
    u32 idx;
    bool seq;
    *out = PP(PolicyExpr__simple)(JAZZ_PE_TRUE);
    JZ_TRY(tagged_variant(j, POLICY_EXPR_NAMES, JAZZ_PE_COUNT, &idx, &seq));
    out->tag = (u8)idx;
    jz_err e = policy_expr_fields(j, seq, idx, out);
    if (e.code) PP(PolicyExpr__drop)(out);
    return e;
}

/* ---------------------------------------------------------------- combinators */
jazz_model_PolicyExpr PP(PolicyExpr__and)(jazz_model_PolicyExprVec exprs) {
    if (exprs.len == 0) {
        jz_vec_drop(&exprs, sizeof(jazz_model_PolicyExpr), NULL);
        return PP(PolicyExpr__simple)(JAZZ_PE_TRUE);
    }
    if (exprs.len == 1) {
        jazz_model_PolicyExpr only = exprs.ptr[0];
        jz_vec_drop(&exprs, sizeof(jazz_model_PolicyExpr), NULL);
        return only;
    }
    jazz_model_PolicyExpr e = PP(PolicyExpr__simple)(JAZZ_PE_AND);
    e.exprs = exprs;
    return e;
}

jazz_model_PolicyExpr PP(PolicyExpr__or)(jazz_model_PolicyExprVec exprs) {
    if (exprs.len == 0) {
        jz_vec_drop(&exprs, sizeof(jazz_model_PolicyExpr), NULL);
        return PP(PolicyExpr__simple)(JAZZ_PE_FALSE);
    }
    if (exprs.len == 1) {
        jazz_model_PolicyExpr only = exprs.ptr[0];
        jz_vec_drop(&exprs, sizeof(jazz_model_PolicyExpr), NULL);
        return only;
    }
    jazz_model_PolicyExpr e = PP(PolicyExpr__simple)(JAZZ_PE_OR);
    e.exprs = exprs;
    return e;
}

jazz_model_PolicyExpr PP(PolicyExpr__not)(jazz_model_PolicyExpr expr) {
    jazz_model_PolicyExpr e = PP(PolicyExpr__simple)(JAZZ_PE_NOT);
    e.inner = JZ_NEW(jazz_model_PolicyExpr);
    *e.inner = expr;
    return e;
}
