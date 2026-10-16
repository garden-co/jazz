/* model/public_api/types/schema.rs: public schema vocabulary (TableName,
 * ColumnType, ColumnDescriptor, RowDescriptor, TableSchema, Schema). */
#include "jazz_model/jazz_model.h"

#define M(x) jazz_model__model__public_api__types__schema__##x
#define VALUE(x) jazz_model__model__public_api__types__value__Value__##x
#define POL(x) jazz_model__model__public_api__types__policy__##x

/* ---------------------------------------------------------------- interned names */
static jz_interner *jazz_model__model__public_api__types__schema__NAME_POOL;

static u64 name_hash(const void *p) { return jz_str_hash((const jz_str *)p); }
static bool name_eq(const void *a, const void *b) { return jz_str_eq((const jz_str *)a, (const jz_str *)b); }

static const jz_str *intern_name(jz_str name) {
    bool inserted = false;
    const jz_str *r = jz_intern_ex(&jazz_model__model__public_api__types__schema__NAME_POOL, &name, sizeof name, name_hash, name_eq, &inserted);
    if (!inserted) jz_str_drop(&name);
    return r;
}

jazz_model_TableName M(TableName__new)(jz_str name) { return intern_name(name); }
jazz_model_TableName M(TableName__from)(const char *p, u32 n) { return intern_name(jz_str_from_slice(jz_slice_of(p, n))); }
jazz_model_ColumnName M(ColumnName__new)(jz_str name) { return intern_name(name); }
jazz_model_ColumnName M(ColumnName__from)(const char *p, u32 n) { return intern_name(jz_str_from_slice(jz_slice_of(p, n))); }

int M(TableName__Ord__cmp)(const void *a, const void *b, void *ctx) {
    (void)ctx;
    return jz_str_cmp(*(const jz_str *const *)a, *(const jz_str *const *)b);
}

void M(TableName__Debug__fmt)(jz_str *out, jazz_model_TableName n) {
    jz_str_push_cstr(out, "TableName(");
    jazz_model__debug_str(out, n->ptr, n->len);
    jz_str_push_char(out, ')');
}

void M(ColumnName__Debug__fmt)(jz_str *out, jazz_model_ColumnName n) {
    jz_str_push_cstr(out, "ColumnName(");
    jazz_model__debug_str(out, n->ptr, n->len);
    jz_str_push_char(out, ')');
}

void M(ColumnNameVec__drop)(jazz_model_ColumnNameVec *v) { jz_vec_drop(v, sizeof(jazz_model_ColumnName), NULL); }
static void column_name_vec_drop_v(void *v) { M(ColumnNameVec__drop)((jazz_model_ColumnNameVec *)v); }
static void column_name_vec_clone_v(void *dst, const void *src) { jz_vec_clone(dst, src, sizeof(jazz_model_ColumnName), NULL); }
void M(ColumnNameVecVec__drop)(jazz_model_ColumnNameVecVec *v) { jz_vec_drop(v, sizeof(jazz_model_ColumnNameVec), column_name_vec_drop_v); }

static bool column_name_vec_eq(const jazz_model_ColumnNameVec *a, const jazz_model_ColumnNameVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!jz_str_eq(a->ptr[i], b->ptr[i])) return false;
    return true;
}

static void column_name_vec_serialize(jz_json_w *w, const jazz_model_ColumnNameVec *v) {
    jz_jw_begin_arr(w);
    for (u32 i = 0; i < v->len; i++) jazz_model__json_w_str(w, v->ptr[i]);
    jz_jw_end_arr(w);
}

static jz_err column_name_vec_deserialize(const jz_json *j, jazz_model_ColumnNameVec *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jazz_model__json_expect_arr(j, "a sequence"));
    for (u32 i = 0; i < j->v.arr.len; i++) {
        jz_str s;
        jz_err e = jazz_model__json_expect_str(j->v.arr.ptr[i], &s);
        if (e.code) { M(ColumnNameVec__drop)(out); return e; }
        JZ_VEC_PUSH(*out, M(ColumnName__new)(s));
    }
    return JZ_OK;
}

/* ---------------------------------------------------------------- ColumnType */
static const char *const COLUMN_TYPE_NAMES[JAZZ_CT_COUNT] = {
    "Integer", "BigInt", "Boolean", "Text", "Enum", "ScalarEnum", "CatalogueEnumPayload", "EnumPayload",
    "Timestamp", "Double", "Uuid", "TransactionId", "Bytea", "Json", "Array", "Row",
};

jazz_model_public_ColumnType M(ColumnType__simple)(u8 tag) {
    jazz_model_public_ColumnType t;
    memset(&t, 0, sizeof t);
    t.tag = tag;
    return t;
}

static void enum_case_drop_v(void *c) { M(EnumCaseDescriptor__drop)((jazz_model_EnumCaseDescriptor *)c); }
static void column_descriptor_drop_v(void *c) { M(ColumnDescriptor__drop)((jazz_model_ColumnDescriptor *)c); }
static void column_descriptor_clone_v(void *dst, const void *src) { M(ColumnDescriptor__Clone__clone)(dst, src); }

void M(EnumCaseDescriptor__drop)(jazz_model_EnumCaseDescriptor *c) {
    jz_str_drop(&c->name);
    jz_vec_drop(&c->fields, sizeof(jazz_model_ColumnDescriptor), column_descriptor_drop_v);
}

static void enum_case_clone_v(void *dst, const void *src) {
    jazz_model_EnumCaseDescriptor *d = dst;
    const jazz_model_EnumCaseDescriptor *s = src;
    d->name = jz_str_clone(&s->name);
    jz_vec_clone(&d->fields, &s->fields, sizeof(jazz_model_ColumnDescriptor), column_descriptor_clone_v);
}

static bool column_descriptor_vec_eq(const jazz_model_ColumnDescriptorVec *a, const jazz_model_ColumnDescriptorVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!M(ColumnDescriptor__PartialEq__eq)(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}

static bool enum_cases_eq(const jazz_model_EnumCaseDescriptorVec *a, const jazz_model_EnumCaseDescriptorVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) {
        if (!jz_str_eq(&a->ptr[i].name, &b->ptr[i].name)) return false;
        if (!column_descriptor_vec_eq(&a->ptr[i].fields, &b->ptr[i].fields)) return false;
    }
    return true;
}

void M(ColumnType__drop)(jazz_model_public_ColumnType *t) {
    switch (t->tag) {
    case JAZZ_CT_ENUM: jazz_model__StrVec__drop(&t->variants); break;
    case JAZZ_CT_SCALAR_ENUM: jz_str_drop(&t->name); jazz_model__StrVec__drop(&t->variants); break;
    case JAZZ_CT_CATALOGUE_ENUM_PAYLOAD:
        jz_str_drop(&t->name);
        jz_vec_drop(&t->cases, sizeof(jazz_model_EnumCaseDescriptor), enum_case_drop_v);
        break;
    case JAZZ_CT_ENUM_PAYLOAD: jz_vec_drop(&t->cases, sizeof(jazz_model_EnumCaseDescriptor), enum_case_drop_v); break;
    case JAZZ_CT_JSON: if (t->schema) jz_json_free(t->schema); break;
    case JAZZ_CT_ARRAY: if (t->element) { M(ColumnType__drop)(t->element); jz_free(t->element); } break;
    case JAZZ_CT_ROW: if (t->columns) { M(RowDescriptor__drop)(t->columns); jz_free(t->columns); } break;
    default: break;
    }
    t->tag = JAZZ_CT_INTEGER;
}

void M(ColumnType__Clone__clone)(jazz_model_public_ColumnType *dst, const jazz_model_public_ColumnType *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    switch (src->tag) {
    case JAZZ_CT_ENUM: jazz_model__StrVec__clone(&dst->variants, &src->variants); break;
    case JAZZ_CT_SCALAR_ENUM:
        dst->name = jz_str_clone(&src->name);
        jazz_model__StrVec__clone(&dst->variants, &src->variants);
        break;
    case JAZZ_CT_CATALOGUE_ENUM_PAYLOAD:
        dst->name = jz_str_clone(&src->name);
        jz_vec_clone(&dst->cases, &src->cases, sizeof(jazz_model_EnumCaseDescriptor), enum_case_clone_v);
        break;
    case JAZZ_CT_ENUM_PAYLOAD:
        jz_vec_clone(&dst->cases, &src->cases, sizeof(jazz_model_EnumCaseDescriptor), enum_case_clone_v);
        break;
    case JAZZ_CT_JSON: dst->schema = src->schema ? jz_json_clone(src->schema) : NULL; break;
    case JAZZ_CT_ARRAY:
        dst->element = JZ_NEW(jazz_model_public_ColumnType);
        M(ColumnType__Clone__clone)(dst->element, src->element);
        break;
    case JAZZ_CT_ROW:
        dst->columns = JZ_NEW(jazz_model_RowDescriptor);
        M(RowDescriptor__Clone__clone)(dst->columns, src->columns);
        break;
    default: break;
    }
}

bool M(ColumnType__PartialEq__eq)(const jazz_model_public_ColumnType *a, const jazz_model_public_ColumnType *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_CT_ENUM: return jazz_model__StrVec__eq(&a->variants, &b->variants);
    case JAZZ_CT_SCALAR_ENUM: return jz_str_eq(&a->name, &b->name) && jazz_model__StrVec__eq(&a->variants, &b->variants);
    case JAZZ_CT_CATALOGUE_ENUM_PAYLOAD: return jz_str_eq(&a->name, &b->name) && enum_cases_eq(&a->cases, &b->cases);
    case JAZZ_CT_ENUM_PAYLOAD: return enum_cases_eq(&a->cases, &b->cases);
    case JAZZ_CT_JSON:
        if (!a->schema || !b->schema) return a->schema == b->schema;
        return jz_json_eq(a->schema, b->schema);
    case JAZZ_CT_ARRAY: return M(ColumnType__PartialEq__eq)(a->element, b->element);
    case JAZZ_CT_ROW: return M(RowDescriptor__PartialEq__eq)(a->columns, b->columns);
    default: return true;
    }
}

const jazz_model_public_ColumnType *M(ColumnType__element_type)(const jazz_model_public_ColumnType *t) {
    return t->tag == JAZZ_CT_ARRAY ? t->element : NULL;
}

const jazz_model_RowDescriptor *M(ColumnType__row_descriptor)(const jazz_model_public_ColumnType *t) {
    return t->tag == JAZZ_CT_ROW ? t->columns : NULL;
}

static void column_descriptor_debug(jz_str *out, const jazz_model_ColumnDescriptor *c);

static void enum_cases_debug(jz_str *out, const jazz_model_EnumCaseDescriptorVec *cases) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < cases->len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        jz_str_push_cstr(out, "EnumCaseDescriptor { name: ");
        jazz_model__debug_str(out, cases->ptr[i].name.ptr, cases->ptr[i].name.len);
        jz_str_push_cstr(out, ", fields: [");
        for (u32 f = 0; f < cases->ptr[i].fields.len; f++) {
            if (f) jz_str_push_cstr(out, ", ");
            column_descriptor_debug(out, &cases->ptr[i].fields.ptr[f]);
        }
        jz_str_push_cstr(out, "] }");
    }
    jz_str_push_char(out, ']');
}

void M(ColumnType__Debug__fmt)(jz_str *out, const jazz_model_public_ColumnType *t) {
    jz_str_push_cstr(out, COLUMN_TYPE_NAMES[t->tag]);
    switch (t->tag) {
    case JAZZ_CT_ENUM:
        jz_str_push_cstr(out, " { variants: ");
        jazz_model__StrVec__Debug__fmt(out, &t->variants);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_CT_SCALAR_ENUM:
        jz_str_push_cstr(out, " { name: ");
        jazz_model__debug_str(out, t->name.ptr, t->name.len);
        jz_str_push_cstr(out, ", variants: ");
        jazz_model__StrVec__Debug__fmt(out, &t->variants);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_CT_CATALOGUE_ENUM_PAYLOAD:
        jz_str_push_cstr(out, " { name: ");
        jazz_model__debug_str(out, t->name.ptr, t->name.len);
        jz_str_push_cstr(out, ", cases: ");
        enum_cases_debug(out, &t->cases);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_CT_ENUM_PAYLOAD:
        jz_str_push_cstr(out, " { cases: ");
        enum_cases_debug(out, &t->cases);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_CT_JSON:
        jz_str_push_cstr(out, " { schema: ");
        if (t->schema) {
            jz_str_push_cstr(out, "Some(");
            jz_json_write(t->schema, out);
            jz_str_push_char(out, ')');
        } else jz_str_push_cstr(out, "None");
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_CT_ARRAY:
        jz_str_push_cstr(out, " { element: ");
        M(ColumnType__Debug__fmt)(out, t->element);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_CT_ROW:
        jz_str_push_cstr(out, " { columns: RowDescriptor { columns: [");
        for (u32 i = 0; i < t->columns->columns.len; i++) {
            if (i) jz_str_push_cstr(out, ", ");
            column_descriptor_debug(out, &t->columns->columns.ptr[i]);
        }
        jz_str_push_cstr(out, "] } }");
        break;
    default: break;
    }
}

static void enum_cases_serialize(jz_json_w *w, const jazz_model_EnumCaseDescriptorVec *cases) {
    jz_jw_begin_arr(w);
    for (u32 i = 0; i < cases->len; i++) {
        jz_jw_begin_obj(w);
        jz_jw_key(w, "name");
        jazz_model__json_w_str(w, &cases->ptr[i].name);
        jz_jw_key(w, "fields");
        jz_jw_begin_arr(w);
        for (u32 f = 0; f < cases->ptr[i].fields.len; f++)
            M(ColumnDescriptor__Serialize__serialize)(w, &cases->ptr[i].fields.ptr[f]);
        jz_jw_end_arr(w);
        jz_jw_end_obj(w);
    }
    jz_jw_end_arr(w);
}

static void row_descriptor_serialize(jz_json_w *w, const jazz_model_RowDescriptor *r) {
    jz_jw_begin_arr(w);
    for (u32 i = 0; i < r->columns.len; i++) M(ColumnDescriptor__Serialize__serialize)(w, &r->columns.ptr[i]);
    jz_jw_end_arr(w);
}

void M(ColumnType__Serialize__serialize)(jz_json_w *w, const jazz_model_public_ColumnType *t) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "type");
    jazz_model__json_w_cstr(w, COLUMN_TYPE_NAMES[t->tag]);
    switch (t->tag) {
    case JAZZ_CT_ENUM:
        jz_jw_key(w, "variants");
        jazz_model__StrVec__serialize_json(w, &t->variants);
        break;
    case JAZZ_CT_SCALAR_ENUM:
        jz_jw_key(w, "name");
        jazz_model__json_w_str(w, &t->name);
        jz_jw_key(w, "variants");
        jazz_model__StrVec__serialize_json(w, &t->variants);
        break;
    case JAZZ_CT_CATALOGUE_ENUM_PAYLOAD:
        jz_jw_key(w, "name");
        jazz_model__json_w_str(w, &t->name);
        jz_jw_key(w, "cases");
        enum_cases_serialize(w, &t->cases);
        break;
    case JAZZ_CT_ENUM_PAYLOAD:
        jz_jw_key(w, "cases");
        enum_cases_serialize(w, &t->cases);
        break;
    case JAZZ_CT_JSON:
        if (t->schema) {
            jz_str tmp = {0};
            jz_jw_key(w, "schema");
            /* nested serde_json::Value: write its text through the streaming writer's buffer */
            jz_json_write(t->schema, &tmp);
            jz_jw_null(w);
            w->out->len -= 4;
            jz_str_push(w->out, tmp.ptr, tmp.len);
            jz_str_drop(&tmp);
        }
        break;
    case JAZZ_CT_ARRAY:
        jz_jw_key(w, "element");
        M(ColumnType__Serialize__serialize)(w, t->element);
        break;
    case JAZZ_CT_ROW:
        jz_jw_key(w, "columns");
        row_descriptor_serialize(w, t->columns);
        break;
    default: break;
    }
    jz_jw_end_obj(w);
}

static jz_err enum_cases_deserialize(const jz_json *j, jazz_model_EnumCaseDescriptorVec *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jazz_model__json_expect_arr(j, "a sequence"));
    for (u32 i = 0; i < j->v.arr.len; i++) {
        const jz_json *c = j->v.arr.ptr[i];
        jazz_model_EnumCaseDescriptor ec;
        memset(&ec, 0, sizeof ec);
        jz_err e = jz_json_expect_obj(c, "struct EnumCaseDescriptor");
        if (!e.code) e = jz_json_field_str(c, "name", &ec.name);
        if (!e.code) {
            const jz_json *fields = jz_json_obj_get(c, "fields");
            if (!fields) e = jazz_model__json_missing_field("fields");
            else e = jazz_model__json_expect_arr(fields, "a sequence");
            for (u32 f = 0; !e.code && f < fields->v.arr.len; f++) {
                jazz_model_ColumnDescriptor cd;
                e = M(ColumnDescriptor__Deserialize__deserialize)(fields->v.arr.ptr[f], &cd);
                if (!e.code) JZ_VEC_PUSH(ec.fields, cd);
            }
        }
        if (e.code) {
            M(EnumCaseDescriptor__drop)(&ec);
            jz_vec_drop(out, sizeof(jazz_model_EnumCaseDescriptor), enum_case_drop_v);
            return e;
        }
        JZ_VEC_PUSH(*out, ec);
    }
    return JZ_OK;
}

static jz_err row_descriptor_deserialize(const jz_json *j, jazz_model_RowDescriptor *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jazz_model__json_expect_arr(j, "a sequence"));
    for (u32 i = 0; i < j->v.arr.len; i++) {
        jazz_model_ColumnDescriptor cd;
        jz_err e = M(ColumnDescriptor__Deserialize__deserialize)(j->v.arr.ptr[i], &cd);
        if (e.code) { M(RowDescriptor__drop)(out); return e; }
        JZ_VEC_PUSH(out->columns, cd);
    }
    return JZ_OK;
}

static jz_err column_type_variant_fields(u32 idx, const jz_json *j) {
    static const char *const unit[] = {"type"};
    static const char *const en[] = {"type", "variants"};
    static const char *const se[] = {"type", "name", "variants"};
    static const char *const cep[] = {"type", "name", "cases"};
    static const char *const ep[] = {"type", "cases"};
    static const char *const js[] = {"type", "schema"};
    static const char *const ar[] = {"type", "element"};
    static const char *const rw[] = {"type", "columns"};
    switch (idx) {
    case JAZZ_CT_ENUM: return jazz_model__json_deny_unknown_fields(j, en, 2);
    case JAZZ_CT_SCALAR_ENUM: return jazz_model__json_deny_unknown_fields(j, se, 3);
    case JAZZ_CT_CATALOGUE_ENUM_PAYLOAD: return jazz_model__json_deny_unknown_fields(j, cep, 3);
    case JAZZ_CT_ENUM_PAYLOAD: return jazz_model__json_deny_unknown_fields(j, ep, 2);
    case JAZZ_CT_JSON: return jazz_model__json_deny_unknown_fields(j, js, 2);
    case JAZZ_CT_ARRAY: return jazz_model__json_deny_unknown_fields(j, ar, 2);
    case JAZZ_CT_ROW: return jazz_model__json_deny_unknown_fields(j, rw, 2);
    default: return jazz_model__json_deny_unknown_fields(j, unit, 1);
    }
}

jz_err M(ColumnType__Deserialize__deserialize)(const jz_json *j, jazz_model_public_ColumnType *out) {
    u32 idx;
    memset(out, 0, sizeof *out);
    JZ_TRY(jazz_model__json_internal_tag(j, "type", COLUMN_TYPE_NAMES, JAZZ_CT_COUNT, &idx));
    JZ_TRY(column_type_variant_fields(idx, j));
    out->tag = (u8)idx;
    jz_err e = JZ_OK;
    const jz_json *f;
    switch (idx) {
    case JAZZ_CT_ENUM:
        f = jz_json_obj_get(j, "variants");
        e = f ? jazz_model__StrVec__deserialize_json(f, &out->variants) : jazz_model__json_missing_field("variants");
        break;
    case JAZZ_CT_SCALAR_ENUM:
        e = jz_json_field_str(j, "name", &out->name);
        if (!e.code) {
            f = jz_json_obj_get(j, "variants");
            e = f ? jazz_model__StrVec__deserialize_json(f, &out->variants) : jazz_model__json_missing_field("variants");
        }
        break;
    case JAZZ_CT_CATALOGUE_ENUM_PAYLOAD:
        e = jz_json_field_str(j, "name", &out->name);
        if (!e.code) {
            f = jz_json_obj_get(j, "cases");
            e = f ? enum_cases_deserialize(f, &out->cases) : jazz_model__json_missing_field("cases");
        }
        break;
    case JAZZ_CT_ENUM_PAYLOAD:
        f = jz_json_obj_get(j, "cases");
        e = f ? enum_cases_deserialize(f, &out->cases) : jazz_model__json_missing_field("cases");
        break;
    case JAZZ_CT_JSON:
        f = jz_json_obj_get(j, "schema");
        if (f && f->kind != JZ_JSON_NULL) out->schema = jz_json_clone(f);
        break;
    case JAZZ_CT_ARRAY:
        f = jz_json_obj_get(j, "element");
        if (!f) { e = jazz_model__json_missing_field("element"); break; }
        out->element = JZ_NEW(jazz_model_public_ColumnType);
        e = M(ColumnType__Deserialize__deserialize)(f, out->element);
        break;
    case JAZZ_CT_ROW:
        f = jz_json_obj_get(j, "columns");
        if (!f) { e = jazz_model__json_missing_field("columns"); break; }
        out->columns = JZ_NEW(jazz_model_RowDescriptor);
        e = row_descriptor_deserialize(f, out->columns);
        break;
    default: break;
    }
    if (e.code) M(ColumnType__drop)(out);
    return e;
}

/* ---------------------------------------------------------------- ColumnDescriptor */
static const char *const MERGE_STRATEGY_NAMES[] = {"Counter", "GSet"};

jazz_model_ColumnDescriptor M(ColumnDescriptor__new)(jazz_model_ColumnName name, jazz_model_public_ColumnType t) {
    jazz_model_ColumnDescriptor c;
    memset(&c, 0, sizeof c);
    c.name = name;
    c.column_type = t;
    return c;
}

void M(ColumnDescriptor__drop)(jazz_model_ColumnDescriptor *c) {
    M(ColumnType__drop)(&c->column_type);
    if (c->default_) { VALUE(drop)(c->default_); jz_free(c->default_); c->default_ = NULL; }
}

void M(ColumnDescriptor__Clone__clone)(jazz_model_ColumnDescriptor *dst, const jazz_model_ColumnDescriptor *src) {
    *dst = *src;
    M(ColumnType__Clone__clone)(&dst->column_type, &src->column_type);
    if (src->default_) {
        dst->default_ = JZ_NEW(jazz_model_public_Value);
        VALUE(Clone__clone)(dst->default_, src->default_);
    }
}

bool M(ColumnDescriptor__PartialEq__eq)(const jazz_model_ColumnDescriptor *a, const jazz_model_ColumnDescriptor *b) {
    if (a->name != b->name) return false;
    if (!M(ColumnType__PartialEq__eq)(&a->column_type, &b->column_type)) return false;
    if (a->nullable != b->nullable) return false;
    if (a->references != b->references) return false;
    if ((a->default_ == NULL) != (b->default_ == NULL)) return false;
    if (a->default_ && !VALUE(PartialEq__eq)(a->default_, b->default_)) return false;
    if (a->has_merge_strategy != b->has_merge_strategy) return false;
    return !a->has_merge_strategy || a->merge_strategy == b->merge_strategy;
}

static void column_descriptor_debug(jz_str *out, const jazz_model_ColumnDescriptor *c) {
    jz_str_push_cstr(out, "ColumnDescriptor { name: ");
    M(ColumnName__Debug__fmt)(out, c->name);
    jz_str_push_cstr(out, ", column_type: ");
    M(ColumnType__Debug__fmt)(out, &c->column_type);
    jz_str_appendf(out, ", nullable: %s, references: ", c->nullable ? "true" : "false");
    if (c->references) {
        jz_str_push_cstr(out, "Some(");
        M(TableName__Debug__fmt)(out, c->references);
        jz_str_push_char(out, ')');
    } else jz_str_push_cstr(out, "None");
    jz_str_push_cstr(out, ", default: ");
    if (c->default_) {
        jz_str_push_cstr(out, "Some(");
        VALUE(Debug__fmt)(out, c->default_);
        jz_str_push_char(out, ')');
    } else jz_str_push_cstr(out, "None");
    jz_str_push_cstr(out, ", merge_strategy: ");
    if (c->has_merge_strategy) jz_str_appendf(out, "Some(%s)", MERGE_STRATEGY_NAMES[c->merge_strategy]);
    else jz_str_push_cstr(out, "None");
    jz_str_push_cstr(out, " }");
}

jz_err M(ColumnDescriptor__validate_merge_strategy)(const jazz_model_ColumnDescriptor *c) {
    if (!c->has_merge_strategy) return JZ_OK;
    if (c->merge_strategy == JAZZ_MERGE_COUNTER) {
        if (c->nullable || !(c->column_type.tag == JAZZ_CT_INTEGER || c->column_type.tag == JAZZ_CT_BIGINT))
            return jz_err_new(JAZZ_MODEL_ERR_STRING,
                              "Counter merge strategy is only supported on non-nullable INTEGER or BIGINT columns");
        return JZ_OK;
    }
    if (c->nullable || c->column_type.tag != JAZZ_CT_ARRAY) {
        jz_str dbg = {0};
        M(ColumnType__Debug__fmt)(&dbg, &c->column_type);
        jz_err e = jz_err_fmt(JAZZ_MODEL_ERR_STRING,
                              "g-set merge strategy is only supported on non-nullable ARRAY columns, got %.*s (%.*s, nullable=%s)",
                              (int)c->name->len, c->name->ptr, (int)dbg.len, dbg.ptr, c->nullable ? "true" : "false");
        jz_str_drop(&dbg);
        return e;
    }
    return JZ_OK;
}

void M(ColumnDescriptor__Serialize__serialize)(jz_json_w *w, const jazz_model_ColumnDescriptor *c) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "name");
    jazz_model__json_w_str(w, c->name);
    jz_jw_key(w, "column_type");
    M(ColumnType__Serialize__serialize)(w, &c->column_type);
    jz_jw_key(w, "nullable");
    jz_jw_bool(w, c->nullable);
    if (c->references) {
        jz_jw_key(w, "references");
        jazz_model__json_w_str(w, c->references);
    }
    if (c->default_) {
        jz_jw_key(w, "default");
        (void)VALUE(Serialize__serialize)(w, c->default_);
    }
    if (c->has_merge_strategy) {
        jz_jw_key(w, "merge_strategy");
        jazz_model__json_w_unit_variant(w, MERGE_STRATEGY_NAMES[c->merge_strategy]);
    }
    jz_jw_end_obj(w);
}

jz_err M(ColumnDescriptor__Deserialize__deserialize)(const jz_json *j, jazz_model_ColumnDescriptor *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_json_expect_obj(j, "struct ColumnDescriptor"));
    jz_str name;
    JZ_TRY(jz_json_field_str(j, "name", &name));
    out->name = M(ColumnName__new)(name);
    const jz_json *ct = jz_json_obj_get(j, "column_type");
    if (!ct) return jazz_model__json_missing_field("column_type");
    JZ_TRY(M(ColumnType__Deserialize__deserialize)(ct, &out->column_type));
    jz_err e = jz_json_field_bool(j, "nullable", &out->nullable);
    const jz_json *f;
    if (!e.code && (f = jz_json_obj_get(j, "references")) && f->kind != JZ_JSON_NULL) {
        jz_str r;
        e = jazz_model__json_expect_str(f, &r);
        if (!e.code) out->references = M(TableName__new)(r);
    }
    if (!e.code && (f = jz_json_obj_get(j, "default")) && f->kind != JZ_JSON_NULL) {
        out->default_ = JZ_NEW(jazz_model_public_Value);
        e = VALUE(Deserialize__deserialize)(f, out->default_);
        if (e.code) { jz_free(out->default_); out->default_ = NULL; }
    }
    if (!e.code && (f = jz_json_obj_get(j, "merge_strategy")) && f->kind != JZ_JSON_NULL) {
        u32 idx;
        e = jazz_model__json_unit_variant(f, MERGE_STRATEGY_NAMES, 2, &idx);
        if (!e.code) { out->has_merge_strategy = true; out->merge_strategy = (u8)idx; }
    }
    if (e.code) M(ColumnDescriptor__drop)(out);
    return e;
}

/* ---------------------------------------------------------------- RowDescriptor */
void M(RowDescriptor__drop)(jazz_model_RowDescriptor *r) {
    jz_vec_drop(&r->columns, sizeof(jazz_model_ColumnDescriptor), column_descriptor_drop_v);
}

void M(RowDescriptor__Clone__clone)(jazz_model_RowDescriptor *dst, const jazz_model_RowDescriptor *src) {
    jz_vec_clone(&dst->columns, &src->columns, sizeof(jazz_model_ColumnDescriptor), column_descriptor_clone_v);
}

bool M(RowDescriptor__PartialEq__eq)(const jazz_model_RowDescriptor *a, const jazz_model_RowDescriptor *b) {
    return column_descriptor_vec_eq(&a->columns, &b->columns);
}

bool M(RowDescriptor__column_index)(const jazz_model_RowDescriptor *r, jz_slice name, u32 *out) {
    for (u32 i = 0; i < r->columns.len; i++)
        if (jazz_model__slice_eq(jz_str_slice(r->columns.ptr[i].name), name)) { *out = i; return true; }
    return false;
}

const jazz_model_ColumnDescriptor *M(RowDescriptor__column)(const jazz_model_RowDescriptor *r, jz_slice name) {
    for (u32 i = 0; i < r->columns.len; i++)
        if (jazz_model__slice_eq(jz_str_slice(r->columns.ptr[i].name), name)) return &r->columns.ptr[i];
    return NULL;
}

/* ---------------------------------------------------------------- TableSchema */
static int slice_vec_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const jazz_model_SliceVec *l = a, *r = b;
    u32 n = l->len < r->len ? l->len : r->len;
    for (u32 i = 0; i < n; i++) {
        int c = jz_slice_cmp(l->ptr[i], r->ptr[i]);
        if (c) return c;
    }
    return l->len < r->len ? -1 : l->len > r->len ? 1 : 0;
}

void M(SliceVecVec__drop)(jazz_model_SliceVecVec *v) {
    for (u32 i = 0; i < v->len; i++) jz_vec_drop(&v->ptr[i], sizeof(jz_slice), NULL);
    jz_vec_drop(v, sizeof(jazz_model_SliceVec), NULL);
}

void M(canonical_composite_index_order)(const jazz_model_ColumnNameVecVec *indexes, jazz_model_SliceVecVec *out) {
    memset(out, 0, sizeof *out);
    for (u32 i = 0; i < indexes->len; i++) {
        jazz_model_SliceVec cols = {0};
        for (u32 c = 0; c < indexes->ptr[i].len; c++) JZ_VEC_PUSH(cols, jz_str_slice(indexes->ptr[i].ptr[c]));
        JZ_VEC_PUSH(*out, cols);
    }
    jz_sort_unstable(out->ptr, out->len, sizeof(jazz_model_SliceVec), slice_vec_cmp, NULL);
}

static int column_name_vec_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const jazz_model_ColumnNameVec *l = a, *r = b;
    u32 n = l->len < r->len ? l->len : r->len;
    for (u32 i = 0; i < n; i++) {
        int c = jz_str_cmp(l->ptr[i], r->ptr[i]);
        if (c) return c;
    }
    return l->len < r->len ? -1 : l->len > r->len ? 1 : 0;
}

static void jazz_model__model__public_api__types__schema__canonicalize_composite_indexes(jazz_model_ColumnNameVecVec *indexes) {
    jz_sort_unstable(indexes->ptr, indexes->len, sizeof(jazz_model_ColumnNameVec), column_name_vec_cmp, NULL);
}

static jz_err jazz_model__model__public_api__types__schema__deserialize_composite_indexes_canonically(const jz_json *j, jazz_model_ColumnNameVecVec *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jazz_model__json_expect_arr(j, "a sequence"));
    for (u32 i = 0; i < j->v.arr.len; i++) {
        jazz_model_ColumnNameVec cols;
        jz_err e = column_name_vec_deserialize(j->v.arr.ptr[i], &cols);
        if (e.code) { M(ColumnNameVecVec__drop)(out); return e; }
        JZ_VEC_PUSH(*out, cols);
    }
    jazz_model__model__public_api__types__schema__canonicalize_composite_indexes(out);
    return JZ_OK;
}

static void jazz_model__model__public_api__types__schema__serialize_composite_indexes_canonically(jz_json_w *w, const jazz_model_ColumnNameVecVec *indexes) {
    jazz_model_SliceVecVec sorted;
    M(canonical_composite_index_order)(indexes, &sorted);
    jz_jw_begin_arr(w);
    for (u32 i = 0; i < sorted.len; i++) {
        jz_jw_begin_arr(w);
        for (u32 c = 0; c < sorted.ptr[i].len; c++) jz_jw_str(w, (const char *)sorted.ptr[i].ptr[c].ptr, sorted.ptr[i].ptr[c].len);
        jz_jw_end_arr(w);
    }
    jz_jw_end_arr(w);
    M(SliceVecVec__drop)(&sorted);
}

bool M(table_policies_are_default)(const jazz_model_TablePolicies *p) {
    jazz_model_TablePolicies def;
    memset(&def, 0, sizeof def);
    return POL(TablePolicies__PartialEq__eq)(p, &def);
}

jazz_model_public_TableSchema M(TableSchema__new)(jazz_model_RowDescriptor columns) {
    jazz_model_public_TableSchema t;
    memset(&t, 0, sizeof t);
    t.columns = columns;
    return t;
}

void M(TableSchema__drop)(jazz_model_public_TableSchema *t) {
    M(RowDescriptor__drop)(&t->columns);
    M(ColumnNameVec__drop)(&t->indexed_columns);
    M(ColumnNameVecVec__drop)(&t->composite_indexes);
    POL(TablePolicies__drop)(&t->policies);
    M(ColumnNameVec__drop)(&t->branch_by);
}
void M(TableSchema__drop_v)(void *t) { M(TableSchema__drop)((jazz_model_public_TableSchema *)t); }

void M(TableSchema__Clone__clone)(jazz_model_public_TableSchema *dst, const jazz_model_public_TableSchema *src) {
    M(RowDescriptor__Clone__clone)(&dst->columns, &src->columns);
    dst->has_indexed_columns = src->has_indexed_columns;
    jz_vec_clone(&dst->indexed_columns, &src->indexed_columns, sizeof(jazz_model_ColumnName), NULL);
    jz_vec_clone(&dst->composite_indexes, &src->composite_indexes, sizeof(jazz_model_ColumnNameVec), column_name_vec_clone_v);
    POL(TablePolicies__Clone__clone)(&dst->policies, &src->policies);
    jz_vec_clone(&dst->branch_by, &src->branch_by, sizeof(jazz_model_ColumnName), NULL);
}
void M(TableSchema__Clone__clone_v)(void *dst, const void *src) { M(TableSchema__Clone__clone)(dst, src); }

static bool slice_vec_vec_eq(const jazz_model_SliceVecVec *a, const jazz_model_SliceVecVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (slice_vec_cmp(&a->ptr[i], &b->ptr[i], NULL) != 0) return false;
    return true;
}

bool M(TableSchema__PartialEq__eq)(const jazz_model_public_TableSchema *a, const jazz_model_public_TableSchema *b) {
    if (!M(RowDescriptor__PartialEq__eq)(&a->columns, &b->columns)) return false;
    if (a->has_indexed_columns != b->has_indexed_columns) return false;
    if (a->has_indexed_columns && !column_name_vec_eq(&a->indexed_columns, &b->indexed_columns)) return false;
    jazz_model_SliceVecVec ca, cb;
    M(canonical_composite_index_order)(&a->composite_indexes, &ca);
    M(canonical_composite_index_order)(&b->composite_indexes, &cb);
    bool eq = slice_vec_vec_eq(&ca, &cb);
    M(SliceVecVec__drop)(&ca);
    M(SliceVecVec__drop)(&cb);
    if (!eq) return false;
    if (!POL(TablePolicies__PartialEq__eq)(&a->policies, &b->policies)) return false;
    return column_name_vec_eq(&a->branch_by, &b->branch_by);
}
bool M(TableSchema__PartialEq__eq_v)(const void *a, const void *b) { return M(TableSchema__PartialEq__eq)(a, b); }

bool M(TableSchema__is_indexed_column)(const jazz_model_public_TableSchema *t, jz_slice column) {
    if (jazz_model__slice_eq_cstr(column, "_id") || jazz_model__slice_eq_cstr(column, "_id_deleted")) return true;
    const jazz_model_ColumnDescriptor *d = M(RowDescriptor__column)(&t->columns, column);
    if (d && d->column_type.tag == JAZZ_CT_BYTEA) return false;
    if (!t->has_indexed_columns) return true;
    for (u32 i = 0; i < t->indexed_columns.len; i++)
        if (jazz_model__slice_eq(jz_str_slice(t->indexed_columns.ptr[i]), column)) return true;
    return false;
}

void M(TableSchema__Serialize__serialize)(jz_json_w *w, const jazz_model_public_TableSchema *t) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "columns");
    row_descriptor_serialize(w, &t->columns);
    if (t->has_indexed_columns) {
        jz_jw_key(w, "indexed_columns");
        column_name_vec_serialize(w, &t->indexed_columns);
    }
    if (t->composite_indexes.len) {
        jz_jw_key(w, "composite_indexes");
        jazz_model__model__public_api__types__schema__serialize_composite_indexes_canonically(w, &t->composite_indexes);
    }
    if (!M(table_policies_are_default)(&t->policies)) {
        jz_jw_key(w, "policies");
        POL(TablePolicies__Serialize__serialize)(w, &t->policies);
    }
    if (t->branch_by.len) {
        jz_jw_key(w, "branchBy");
        column_name_vec_serialize(w, &t->branch_by);
    }
    jz_jw_end_obj(w);
}

jz_err M(TableSchema__Deserialize__deserialize)(const jz_json *j, jazz_model_public_TableSchema *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_json_expect_obj(j, "struct TableSchema"));
    const jz_json *f = jz_json_obj_get(j, "columns");
    if (!f) return jazz_model__json_missing_field("columns");
    JZ_TRY(row_descriptor_deserialize(f, &out->columns));
    jz_err e = JZ_OK;
    if ((f = jz_json_obj_get(j, "indexed_columns")) && f->kind != JZ_JSON_NULL) {
        out->has_indexed_columns = true;
        e = column_name_vec_deserialize(f, &out->indexed_columns);
    }
    if (!e.code && (f = jz_json_obj_get(j, "composite_indexes")))
        e = jazz_model__model__public_api__types__schema__deserialize_composite_indexes_canonically(f, &out->composite_indexes);
    if (!e.code && (f = jz_json_obj_get(j, "policies")))
        e = POL(TablePolicies__Deserialize__deserialize)(f, &out->policies);
    if (!e.code && (f = jz_json_obj_get(j, "branchBy")))
        e = column_name_vec_deserialize(f, &out->branch_by);
    if (e.code) M(TableSchema__drop)(out);
    return e;
}

/* ---------------------------------------------------------------- Schema */
const jz_btree_desc jazz_model__PUBLIC_SCHEMA_TABLES_DESC = {
    sizeof(jazz_model_TableName), sizeof(jazz_model_public_TableSchema),
    jazz_model__model__public_api__types__schema__TableName__Ord__cmp, NULL,
    jazz_model__model__public_api__types__schema__TableSchema__drop_v,
};

void M(Schema__new)(jazz_model_public_Schema *s) { jz_btree_init(&s->tables, &jazz_model__PUBLIC_SCHEMA_TABLES_DESC); }
void M(Schema__drop)(jazz_model_public_Schema *s) { jz_btree_drop(&s->tables); }

static void table_name_clone_v(void *dst, const void *src) { *(jazz_model_TableName *)dst = *(const jazz_model_TableName *)src; }

void M(Schema__Clone__clone)(jazz_model_public_Schema *dst, const jazz_model_public_Schema *src) {
    jz_btree_clone(&dst->tables, &src->tables, table_name_clone_v, M(TableSchema__Clone__clone_v));
}

bool M(Schema__PartialEq__eq)(const jazz_model_public_Schema *a, const jazz_model_public_Schema *b) {
    return jz_btree_eq(&a->tables, &b->tables, M(TableSchema__PartialEq__eq_v));
}

void M(Schema__Serialize__serialize)(jz_json_w *w, const jazz_model_public_Schema *s) {
    jazz_model_TableName *k;
    jazz_model_public_TableSchema *v;
    jz_jw_begin_obj(w);
    jz_jw_key(w, "tables");
    jz_jw_begin_obj(w);
    JZ_BTREE_FOR(&s->tables, k, v) {
        char buf[256];
        const jz_str *name = *k;
        /* object keys go through the writer's key path */
        if (name->len < sizeof buf) {
            memcpy(buf, name->ptr, name->len);
            buf[name->len] = 0;
            jz_jw_key(w, buf);
        } else {
            jz_str tmp = jz_str_clone(name);
            jz_str_push_char(&tmp, 0);
            jz_jw_key(w, tmp.ptr);
            jz_str_drop(&tmp);
        }
        M(TableSchema__Serialize__serialize)(w, v);
    }
    jz_jw_end_obj(w);
    jz_jw_end_obj(w);
}

jz_err M(Schema__Deserialize__deserialize)(const jz_json *j, jazz_model_public_Schema *out) {
    static const char *const fields[] = {"tables"};
    M(Schema__new)(out);
    JZ_TRY(jz_json_expect_obj(j, "struct Schema"));
    JZ_TRY(jazz_model__json_deny_unknown_fields(j, fields, 1));
    const jz_json *tables = jz_json_obj_get(j, "tables");
    if (!tables) return jazz_model__json_missing_field("tables");
    JZ_TRY(jz_json_expect_obj(tables, "a map"));
    jz_str *k;
    jz_json **v;
    JZ_BTREE_FOR(&tables->v.obj, k, v) {
        jazz_model_public_TableSchema t;
        jz_err e = M(TableSchema__Deserialize__deserialize)(*v, &t);
        if (e.code) { M(Schema__drop)(out); return e; }
        jazz_model_TableName name = M(TableName__new)(jz_str_clone(k));
        jz_btree_insert(&out->tables, &name, &t, NULL);
    }
    return JZ_OK;
}
