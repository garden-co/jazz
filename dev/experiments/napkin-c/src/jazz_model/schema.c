/* schema.rs: Jazz schema metadata and lowering to groove storage schemas.
 * Test-only / `testing`-feature items (RuntimeSchema::new + validated,
 * runtime_mut_for_testing, new_with_branch_columns, with_* builders, Policy)
 * are not ported. */
#include <string.h>
#include "jazz_model/jazz_model.h"

#define SC(x) jazz_model__schema__##x
#define VT(tag) groove__records__values__ValueType__scalar(tag)
#define NULLABLE(t) groove__records__values__ValueType__nullable(t)
#define ARRAY_OF(t) groove__records__values__ValueType__array_of(t)

/* SCHEMA_VERSION_NAMESPACE = uuid!("61b9ef21-3195-50e8-87fc-2aa83a6f74e3") */
const jz_uuid jazz_model__schema__SCHEMA_VERSION_NAMESPACE = {{0x61, 0xb9, 0xef, 0x21, 0x31, 0x95, 0x50, 0xe8,
                                                               0x87, 0xfc, 0x2a, 0xa8, 0x3a, 0x6f, 0x74, 0xe3}};

/* ---------------------------------------------------------------- collection descs */
static void strvec_drop_v(void *v) { jazz_model__StrVec__drop((jazz_model_StrVec *)v); }
static void strvec_clone_v(void *d, const void *s) { jazz_model__StrVec__clone((jazz_model_StrVec *)d, (const jazz_model_StrVec *)s); }
static int strvec_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const jazz_model_StrVec *x = a, *y = b;
    for (u32 i = 0; i < x->len && i < y->len; i++) {
        int c = jz_str_cmp(&x->ptr[i], &y->ptr[i]);
        if (c) return c;
    }
    return x->len < y->len ? -1 : x->len > y->len;
}
static bool str_eq_v(const void *a, const void *b) { return jz_str_eq((const jz_str *)a, (const jz_str *)b); }
static bool u8_eq_v(const void *a, const void *b) { return *(const u8 *)a == *(const u8 *)b; }
static void u8_clone_v(void *d, const void *s) { *(u8 *)d = *(const u8 *)s; }

const jz_btree_desc jazz_model__schema__STR_STR_DESC = {sizeof(jz_str), sizeof(jz_str), jz_cmp_str, jazz_model__str_drop_v, jazz_model__str_drop_v};
const jz_btree_desc jazz_model__schema__STRVEC_SET_DESC = {sizeof(jazz_model_StrVec), 0, strvec_cmp, strvec_drop_v, NULL};
const jz_btree_desc jazz_model__schema__STR_MERGE_DESC = {sizeof(jz_str), sizeof(u8), jz_cmp_str, jazz_model__str_drop_v, NULL};

/* ---------------------------------------------------------------- small groove builders */
static groove_ColumnSchema column(const char *name, groove_ValueType t) {
    return groove__schema__ColumnSchema__new(jz_str_from(name), t);
}
static groove_ColumnSchema column_owned(jz_str name, groove_ValueType t) { return groove__schema__ColumnSchema__new(name, t); }
static groove_RecordDescriptor desc_of(const char *const *names, groove_ValueType *types, u32 n) {
    jz_str buf[16];
    JZ_ASSERT(n <= 16, "descriptor arity");
    for (u32 i = 0; i < n; i++) buf[i] = jz_str_from(names[i]);
    return groove__records__RecordDescriptor__new(buf, types, n);
}
#define DESC(names, ...) ({ groove_ValueType _t[] = {__VA_ARGS__}; desc_of(names, _t, (u32)(sizeof(_t) / sizeof(_t[0]))); })
static groove_ValueType record_type(groove_RecordDescriptor d) {
    groove_ValueType t;
    memset(&t, 0, sizeof t);
    t.tag = GROOVE_VT_RECORD;
    t.u.record = d;
    return t;
}
static groove_ValueType enum_type(const char *name, groove_EnumCaseVec cases, const char *expect) {
    groove_ValueType t;
    memset(&t, 0, sizeof t);
    t.tag = GROOVE_VT_ENUM;
    t.u.enum_ = JZ_NEW(groove_EnumSchema);
    if (groove__records__values__EnumSchema__new(jz_str_from(name), cases, t.u.enum_).code) jz_panic(expect);
    return t;
}
static groove_RecordDescriptor empty_desc(void) { return groove__records__RecordDescriptor__new(NULL, NULL, 0); }
static groove_TableSchema table_new(const char *name, groove_ColumnSchemaVec *cols) {
    groove_TableSchema t = groove__schema__TableSchema__new(jz_str_from(name), cols->ptr, cols->len);
    jz_free(cols->ptr);
    return t;
}
static groove_TableSchema table_new_owned(jz_str name, groove_ColumnSchemaVec *cols) {
    groove_TableSchema t = groove__schema__TableSchema__new(name, cols->ptr, cols->len);
    jz_free(cols->ptr);
    return t;
}
static groove_PrimaryKeyColumn pk_int(const char *c) { return groove__schema__PrimaryKeyColumn__integer(jz_str_from(c), GROOVE_PKT_U64); }
static groove_PrimaryKeyColumn pk_uuid(const char *c) { return groove__schema__PrimaryKeyColumn__uuid(jz_str_from(c)); }
static groove_PrimaryKeyColumn pk_bytes(const char *c) { return groove__schema__PrimaryKeyColumn__bytes(jz_str_from(c)); }
#define PK_COMPOSITE(...) ({ groove_PrimaryKeyColumn _c[] = {__VA_ARGS__}; groove__schema__PrimaryKey__composite(_c, (u32)(sizeof(_c) / sizeof(_c[0]))); })

/* ================================================================ ColumnSchema */
void SC(ColumnSchema__drop)(jazz_model_ColumnSchema *c) {
    jz_str_drop(&c->name);
    groove__records__values__ValueType__drop(&c->column_type);
    if (c->has_default) groove__records__values__Value__drop(&c->default_);
    c->has_default = false;
}
static void column_schema_drop_v(void *c) { SC(ColumnSchema__drop)((jazz_model_ColumnSchema *)c); }
void SC(ColumnSchema__Clone__clone)(jazz_model_ColumnSchema *d, const jazz_model_ColumnSchema *s) {
    d->name = jz_str_clone(&s->name);
    groove__records__values__ValueType__Clone__clone(&d->column_type, &s->column_type);
    d->large_value_kind = s->large_value_kind;
    d->has_default = s->has_default;
    if (s->has_default) groove__records__values__Value__Clone__clone(&d->default_, &s->default_);
}
static void column_schema_clone_v(void *d, const void *s) { SC(ColumnSchema__Clone__clone)((jazz_model_ColumnSchema *)d, (const jazz_model_ColumnSchema *)s); }
bool SC(ColumnSchema__PartialEq__eq)(const jazz_model_ColumnSchema *a, const jazz_model_ColumnSchema *b) {
    return jz_str_eq(&a->name, &b->name) && groove__records__values__ValueType__PartialEq__eq(&a->column_type, &b->column_type) &&
           a->large_value_kind == b->large_value_kind && a->has_default == b->has_default &&
           (!a->has_default || groove__records__values__Value__PartialEq__eq(&a->default_, &b->default_));
}

static bool descriptor_contains_internal(groove_RecordDescriptor d) {
    u32 n;
    const groove_DescriptorField *f = groove__records__RecordDescriptor__fields(d, &n);
    for (u32 i = 0; i < n; i++)
        if (SC(contains_internal_storage_type)(&f[i].value_type)) return true;
    return false;
}
bool SC(contains_internal_storage_type)(const groove_ValueType *t) {
    if (groove__records__values__ValueType__is_internal_storage_type(t)) return true;
    switch (t->tag) {
    case GROOVE_VT_NULLABLE:
    case GROOVE_VT_ARRAY: return SC(contains_internal_storage_type)(t->u.inner);
    case GROOVE_VT_TUPLE:
        JZ_VEC_FOR(const groove_ValueType, m, t->u.tuple) if (SC(contains_internal_storage_type)(m)) return true;
        return false;
    case GROOVE_VT_RECORD: return descriptor_contains_internal(t->u.record);
    case GROOVE_VT_ENUM:
        JZ_VEC_FOR(const groove_EnumCase, c, t->u.enum_->cases) if (descriptor_contains_internal(c->payload)) return true;
        return false;
    default: return false;
    }
}
u8 SC(large_value_kind_for_type)(const groove_ValueType *t) {
    switch (t->tag) {
    case GROOVE_VT_STRING: return JAZZ_LVK_STRING;
    case GROOVE_VT_BYTES: return JAZZ_LVK_BYTES;
    case GROOVE_VT_NULLABLE: return SC(large_value_kind_for_type)(t->u.inner);
    default: return JAZZ_LVK_NOT_LARGE;
    }
}

jazz_model_ColumnSchema SC(ColumnSchema__new)(jz_str name, groove_ValueType column_type) {
    JZ_ASSERT(!SC(contains_internal_storage_type)(&column_type),
              "raw and stored-scalar value types are physical-only and cannot be declared in a Jazz schema");
    jazz_model_ColumnSchema c;
    memset(&c, 0, sizeof c);
    c.name = name;
    c.large_value_kind = SC(large_value_kind_for_type)(&column_type);
    c.column_type = column_type;
    return c;
}
void SC(ColumnSchema__with_default)(jazz_model_ColumnSchema *c, groove_Value value) {
    if (c->has_default) groove__records__values__Value__drop(&c->default_);
    c->has_default = true;
    c->default_ = value;
}
bool SC(ColumnSchema__is_nullable_json)(const jazz_model_ColumnSchema *c) {
    return c->large_value_kind == JAZZ_LVK_JSON && c->column_type.tag == GROOVE_VT_NULLABLE;
}
groove_Value SC(ColumnSchema__storage_value)(const jazz_model_ColumnSchema *c, groove_Value value) {
    if (value.tag == GROOVE_V_NULLABLE && value.u.nullable && SC(ColumnSchema__is_nullable_json)(c)) {
        groove_Value inner = *value.u.nullable;
        jz_free(value.u.nullable);
        return inner;
    }
    return value;
}
jazz_model_ColumnSchema SC(ColumnSchema__From_groove__from)(groove_ColumnSchema column) {
    jazz_model_ColumnSchema c;
    memset(&c, 0, sizeof c);
    c.name = column.name;
    c.large_value_kind = SC(large_value_kind_for_type)(&column.column_type);
    c.column_type = column.column_type;
    return c;
}
groove_ValueType SC(storage_column_type)(const jazz_model_ColumnSchema *c) {
    if (c->large_value_kind == JAZZ_LVK_JSON) return groove__large_values__physical_storage_value_type(GROOVE_LVK_JSON);
    groove_ValueType t;
    groove__records__values__ValueType__Clone__clone(&t, &c->column_type);
    return t;
}
bool SC(registered_column_transform)(jz_slice key, jazz_model_ColumnTransformSemantics *out) {
    if (jazz_model__slice_eq_cstr(key, "identity") || jazz_model__slice_eq_cstr(key, "jazz.identity")) {
        *out = (jazz_model_ColumnTransformSemantics){true, true};
        return true;
    }
    return false;
}

/* ================================================================ WritePolicies */
static void opt_query_drop(jazz_model_Query **q) {
    if (*q) { jazz_model__query__Query__drop(*q); jz_free(*q); *q = NULL; }
}
static jazz_model_Query *opt_query_clone(const jazz_model_Query *q) {
    if (!q) return NULL;
    jazz_model_Query *c = jazz_model__query__Query__box_new();
    jazz_model__query__Query__Clone__clone(c, q);
    return c;
}
static bool opt_query_eq(const jazz_model_Query *a, const jazz_model_Query *b) {
    if (!a || !b) return a == b;
    return jazz_model__query__Query__PartialEq__eq(a, b);
}
void SC(WritePolicies__drop)(jazz_model_WritePolicies *p) {
    opt_query_drop(&p->insert_check);
    opt_query_drop(&p->update_using);
    opt_query_drop(&p->update_check);
    opt_query_drop(&p->delete_using);
}
void SC(WritePolicies__Clone__clone)(jazz_model_WritePolicies *d, const jazz_model_WritePolicies *s) {
    d->insert_check = opt_query_clone(s->insert_check);
    d->update_using = opt_query_clone(s->update_using);
    d->update_check = opt_query_clone(s->update_check);
    d->delete_using = opt_query_clone(s->delete_using);
}
bool SC(WritePolicies__PartialEq__eq)(const jazz_model_WritePolicies *a, const jazz_model_WritePolicies *b) {
    return opt_query_eq(a->insert_check, b->insert_check) && opt_query_eq(a->update_using, b->update_using) &&
           opt_query_eq(a->update_check, b->update_check) && opt_query_eq(a->delete_using, b->delete_using);
}
jazz_model_WritePolicies SC(WritePolicies__legacy)(jazz_model_Query *policy) {
    jazz_model_WritePolicies p;
    p.insert_check = opt_query_clone(policy);
    p.update_using = opt_query_clone(policy);
    p.update_check = opt_query_clone(policy);
    p.delete_using = policy;
    return p;
}
u32 SC(WritePolicies__iter)(const jazz_model_WritePolicies *p, const char *labels[4], const jazz_model_Query *policies[4]) {
    const char *const all_labels[4] = {"insert_check", "update_using", "update_check", "delete_using"};
    const jazz_model_Query *all[4] = {p->insert_check, p->update_using, p->update_check, p->delete_using};
    u32 n = 0;
    for (u32 i = 0; i < 4; i++)
        if (all[i]) { labels[n] = all_labels[i]; policies[n] = all[i]; n++; }
    return n;
}
jazz_model_Query *SC(WritePolicies__any)(const jazz_model_WritePolicies *p) {
    if (p->insert_check) return opt_query_clone(p->insert_check);
    if (p->update_check) return opt_query_clone(p->update_check);
    if (p->update_using) return opt_query_clone(p->update_using);
    return opt_query_clone(p->delete_using);
}
bool SC(WritePolicies__has_any)(const jazz_model_WritePolicies *p) {
    return p->insert_check || p->update_using || p->update_check || p->delete_using;
}

/* ================================================================ TableSchema */
void SC(TableSchema__drop)(jazz_model_TableSchema *t) {
    jz_str_drop(&t->name);
    jz_vec_drop(&t->columns, sizeof(jazz_model_ColumnSchema), column_schema_drop_v);
    jazz_model__StrVec__drop(&t->branch_by);
    jz_btree_drop(&t->references);
    opt_query_drop(&t->read_policy);
    SC(WritePolicies__drop)(&t->write_policies);
    jz_btree_drop(&t->indexed_columns);
    jz_btree_drop(&t->composite_indexes);
    jz_btree_drop(&t->merge_strategies);
}
void SC(TableSchema__drop_v)(void *t) { SC(TableSchema__drop)((jazz_model_TableSchema *)t); }
void SC(TableSchema__Clone__clone)(jazz_model_TableSchema *d, const jazz_model_TableSchema *s) {
    d->name = jz_str_clone(&s->name);
    jz_vec_clone(&d->columns, &s->columns, sizeof(jazz_model_ColumnSchema), column_schema_clone_v);
    jazz_model__StrVec__clone(&d->branch_by, &s->branch_by);
    jz_btree_clone(&d->references, &s->references, jazz_model__str_clone_v, jazz_model__str_clone_v);
    d->read_policy = opt_query_clone(s->read_policy);
    SC(WritePolicies__Clone__clone)(&d->write_policies, &s->write_policies);
    jz_btree_clone(&d->indexed_columns, &s->indexed_columns, jazz_model__str_clone_v, NULL);
    jz_btree_clone(&d->composite_indexes, &s->composite_indexes, strvec_clone_v, NULL);
    jz_btree_clone(&d->merge_strategies, &s->merge_strategies, jazz_model__str_clone_v, u8_clone_v);
}
void SC(TableSchema__Clone__clone_v)(void *d, const void *s) { SC(TableSchema__Clone__clone)((jazz_model_TableSchema *)d, (const jazz_model_TableSchema *)s); }
bool SC(TableSchema__PartialEq__eq)(const jazz_model_TableSchema *a, const jazz_model_TableSchema *b) {
    return jz_str_eq(&a->name, &b->name) && JAZZ_VEC_EQ(&a->columns, &b->columns, SC(ColumnSchema__PartialEq__eq)) &&
           jazz_model__StrVec__eq(&a->branch_by, &b->branch_by) && jz_btree_eq(&a->references, &b->references, str_eq_v) &&
           opt_query_eq(a->read_policy, b->read_policy) && SC(WritePolicies__PartialEq__eq)(&a->write_policies, &b->write_policies) &&
           jz_btree_eq(&a->indexed_columns, &b->indexed_columns, NULL) &&
           jz_btree_eq(&a->composite_indexes, &b->composite_indexes, NULL) &&
           jz_btree_eq(&a->merge_strategies, &b->merge_strategies, u8_eq_v);
}
jazz_model_TableSchema SC(TableSchema__new)(jz_str name, jazz_model_ColumnSchemaVec columns) {
    jazz_model_TableSchema t;
    memset(&t, 0, sizeof t);
    t.name = name;
    t.columns = columns;
    jz_btree_init(&t.references, &jazz_model__schema__STR_STR_DESC);
    jz_btree_init(&t.indexed_columns, &jazz_model__STR_SET_DESC);
    jz_btree_init(&t.composite_indexes, &jazz_model__schema__STRVEC_SET_DESC);
    jz_btree_init(&t.merge_strategies, &jazz_model__schema__STR_MERGE_DESC);
    return t;
}
bool SC(TableSchema__has_any_policy)(const jazz_model_TableSchema *t) {
    return t->read_policy || SC(WritePolicies__has_any)(&t->write_policies);
}
void SC(TableSchema__with_branch_column)(jazz_model_TableSchema *t, jz_str column) { JZ_VEC_PUSH(t->branch_by, column); }
u8 SC(TableSchema__merge_strategy)(const jazz_model_TableSchema *t, jz_slice column) {
    jz_str key = {(char *)column.ptr, column.len, 0};
    const u8 *s = jz_btree_get(&t->merge_strategies, &key);
    return s ? *s : JAZZ_MERGE_STRATEGY_LWW;
}
const jazz_model_ColumnSchema *SC(TableSchema__column)(const jazz_model_TableSchema *t, jz_slice name) {
    JZ_VEC_FOR(const jazz_model_ColumnSchema, c, t->columns)
        if (jazz_model__slice_eq(jz_str_slice(&c->name), name)) return c;
    return NULL;
}

/* ---------------------------------------------------------------- storage columns */
static groove_ValueType storage_enum(const char *name, const char *const *variants, u32 n) {
    groove_StrVec vs = {0};
    for (u32 i = 0; i < n; i++) JZ_VEC_PUSH(vs, jz_str_from(variants[i]));
    groove_ValueType t;
    memset(&t, 0, sizeof t);
    t.tag = GROOVE_VT_ENUM_TAG;
    if (groove__records__values__ScalarEnumSchema__new(jz_str_from(name), vs, &t.u.enum_tag).code) jz_panic("valid enum schema");
    return t;
}
static groove_ValueType tx_kind_column(void) {
    static const char *const v[] = {"mergeable", "exclusive"};
    return storage_enum("jazz_tx_kind", v, 2);
}
static groove_ValueType fate_column(void) {
    static const char *const v[] = {"pending", "accepted", "rejected"};
    return storage_enum("jazz_fate", v, 3);
}
static groove_ValueType deletion_column(void) {
    static const char *const v[] = {"deleted", "restored"};
    groove_ValueType t = storage_enum("jazz_deletion", v, 2);
    groove__records__values__ScalarEnumSchema__with_system_registry(&t.u.enum_tag, groove__records__values__SystemVariantRegistry__deletion_state());
    return t;
}
static groove_ValueType rejection_reason_column(void) { return jazz_model__tx__RejectionReason__storage_value_type(); }
static groove_ValueType durability_column(void) {
    static const char *const v[] = {"none", "local", "global"};
    return storage_enum("jazz_durability", v, 3);
}
static groove_ValueType tx_id_column(void) {
    groove_ValueType t;
    memset(&t, 0, sizeof t);
    t.tag = GROOVE_VT_TUPLE;
    JZ_VEC_PUSH(t.u.tuple, VT(GROOVE_VT_U64));
    JZ_VEC_PUSH(t.u.tuple, VT(GROOVE_VT_UUID));
    return t;
}

jz_str SC(app_storage_column_name)(jz_slice column) {
    return jz_format("%s%.*s", JAZZ_APP_COLUMN_PREFIX, (int)column.len, (const char *)column.ptr);
}
jz_str SC(global_current_index_name)(jz_slice column) {
    return jz_format("by_app_%.*s", (int)column.len, (const char *)column.ptr);
}
jz_str SC(global_current_composite_index_name)(const jazz_model_StrVec *columns) {
    jz_str out = jz_str_from("by_composite_");
    for (u32 i = 0; i < columns->len; i++) {
        if (i) jz_str_push_char(&out, '_');
        jz_str_appendf(&out, "%u_%.*s", columns->ptr[i].len, (int)columns->ptr[i].len, columns->ptr[i].ptr);
    }
    return out;
}

static void push_app_columns(const jazz_model_TableSchema *t, groove_ColumnSchemaVec *cols) {
    JZ_VEC_FOR(const jazz_model_ColumnSchema, uc, t->columns)
        JZ_VEC_PUSH(*cols, column_owned(SC(app_storage_column_name)(jz_str_slice(&uc->name)), NULLABLE(SC(storage_column_type)(uc))));
}

groove_TableSchema SC(TableSchema__rejected_versions_storage_table)(const jazz_model_TableSchema *t) {
    groove_ColumnSchemaVec cols = {0};
    JZ_VEC_PUSH(cols, column("tx_time", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(cols, column("tx_node_id", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(cols, column("row_uuid", VT(GROOVE_VT_UUID)));
    JZ_VEC_PUSH(cols, column("_deletion", NULLABLE(deletion_column())));
    push_app_columns(t, &cols);
    groove_TableSchema out = table_new_owned(jz_format("jazz_%.*s_rejected_versions", (int)t->name.len, t->name.ptr), &cols);
    groove__schema__TableSchema__with_primary_key(&out, PK_COMPOSITE(pk_int("tx_time"), pk_int("tx_node_id"), pk_uuid("row_uuid")));
    return out;
}

static groove_TableSchema history_storage_table_named(const jazz_model_TableSchema *t, jz_str name) {
    groove_ColumnSchemaVec cols = {0};
    JZ_VEC_PUSH(cols, column("branch_key", VT(GROOVE_VT_BYTES)));
    JZ_VEC_PUSH(cols, column("row_uuid", VT(GROOVE_VT_UUID)));
    JZ_VEC_PUSH(cols, column("tx_time", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(cols, column("tx_node_id", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(cols, column("schema_version", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(cols, column("created_by", jazz_types__ids__RowAuthor__value_type()));
    JZ_VEC_PUSH(cols, column("created_at", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(cols, column("updated_by", NULLABLE(jazz_types__ids__RowAuthor__value_type())));
    JZ_VEC_PUSH(cols, column("updated_at", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(cols, column("_deletion", NULLABLE(deletion_column())));
    push_app_columns(t, &cols);
    JZ_VEC_PUSH(cols, column("authored_columns", NULLABLE(ARRAY_OF(VT(GROOVE_VT_U64)))));
    JZ_VEC_PUSH(cols, column(JAZZ_COUNTER_SIGNS_FIELD, VT(GROOVE_VT_BYTES)));
    JZ_VEC_PUSH(cols, column(JAZZ_SEQ_FIELD, VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(cols, column(JAZZ_BASE_SEQ_FIELD, NULLABLE(VT(GROOVE_VT_U64))));
    JZ_VEC_PUSH(cols, column(JAZZ_BASE_PENDING_FIELD, NULLABLE(tx_id_column())));
    groove_TableSchema out = table_new_owned(name, &cols);
    groove__schema__TableSchema__with_primary_key(
        &out, PK_COMPOSITE(pk_bytes("branch_key"), pk_uuid("row_uuid"), pk_int(JAZZ_SEQ_FIELD), pk_int("tx_time"), pk_int("tx_node_id")));
    return out;
}
groove_TableSchema SC(TableSchema__history_storage_table)(const jazz_model_TableSchema *t) {
    return history_storage_table_named(t, jz_format("jazz_%.*s_history", (int)t->name.len, t->name.ptr));
}
groove_TableSchema SC(TableSchema__authored_history_storage_table)(const jazz_model_TableSchema *t) {
    return SC(TableSchema__history_storage_table)(t);
}

static void push_current_columns(const jazz_model_TableSchema *t, groove_ColumnSchemaVec *cols) {
    JZ_VEC_PUSH(*cols, column("branch_key", VT(GROOVE_VT_BYTES)));
    JZ_VEC_PUSH(*cols, column("row_uuid", VT(GROOVE_VT_UUID)));
    JZ_VEC_PUSH(*cols, column("tx_time", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(*cols, column("tx_node_id", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(*cols, column("schema_version", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(*cols, column("created_by", jazz_types__ids__RowAuthor__value_type()));
    JZ_VEC_PUSH(*cols, column("created_at", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(*cols, column("updated_by", jazz_types__ids__RowAuthor__value_type()));
    JZ_VEC_PUSH(*cols, column("updated_at", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(*cols, column("global_time", NULLABLE(VT(GROOVE_VT_U64))));
    JZ_VEC_PUSH(*cols, column("_deletion", NULLABLE(deletion_column())));
    push_app_columns(t, cols);
    JZ_VEC_PUSH(*cols, column("authored_columns", NULLABLE(ARRAY_OF(VT(GROOVE_VT_U64)))));
}

groove_TableSchema SC(TableSchema__global_current_storage_table)(const jazz_model_TableSchema *t) {
    jz_btree indexed_columns;
    SC(TableSchema__global_current_indexed_columns)(t, &indexed_columns);
    groove_ColumnSchemaVec cols = {0};
    push_current_columns(t, &cols);
    groove_TableSchema content = table_new_owned(jz_format("jazz_%.*s_global_current", (int)t->name.len, t->name.ptr), &cols);
    groove__schema__TableSchema__with_primary_key(&content, PK_COMPOSITE(pk_bytes("branch_key"), pk_uuid("row_uuid")));
    jz_str *indexed;
    void *unit;
    JZ_BTREE_FOR(&indexed_columns, indexed, unit) {
        jz_str icols[2] = {jz_str_from("branch_key"), SC(app_storage_column_name)(jz_str_slice(indexed))};
        groove__schema__TableSchema__with_index(
            &content, groove__schema__IndexSchema__new(SC(global_current_index_name)(jz_str_slice(indexed)), icols, 2));
    }
    jazz_model_StrVec *columns;
    JZ_BTREE_FOR(&t->composite_indexes, columns, unit) {
        JZ_VEC(jz_str) icols = {0};
        JZ_VEC_PUSH(icols, jz_str_from("branch_key"));
        JZ_VEC_FOR(const jz_str, c, *columns) JZ_VEC_PUSH(icols, SC(app_storage_column_name)(jz_str_slice(c)));
        groove__schema__TableSchema__with_index(
            &content, groove__schema__IndexSchema__new(SC(global_current_composite_index_name)(columns), icols.ptr, icols.len));
        jz_free(icols.ptr);
    }
    jz_str seq_cols[3] = {jz_str_from("branch_key"), jz_str_from("global_time"), jz_str_from("row_uuid")};
    groove__schema__TableSchema__with_index(&content, groove__schema__IndexSchema__new(jz_str_from(JAZZ_GLOBAL_CURRENT_BY_SEQ_INDEX), seq_cols, 3));
    jz_btree_drop(&indexed_columns);
    return content;
}

groove_TableSchema SC(TableSchema__ahead_current_storage_table)(const jazz_model_TableSchema *t) {
    groove_ColumnSchemaVec cols = {0};
    push_current_columns(t, &cols);
    groove_TableSchema out = table_new_owned(jz_format("jazz_%.*s_ahead_current", (int)t->name.len, t->name.ptr), &cols);
    groove__schema__TableSchema__with_primary_key(&out, PK_COMPOSITE(pk_bytes("branch_key"), pk_uuid("row_uuid")));
    return out;
}

void SC(TableSchema__global_current_indexed_columns)(const jazz_model_TableSchema *t, jz_btree *out) {
    jz_btree_init(out, &jazz_model__STR_SET_DESC);
    jz_str *k;
    void *v;
    JZ_BTREE_FOR(&t->references, k, v) {
        jz_str c = jz_str_clone(k);
        if (jz_btree_insert(out, &c, NULL, NULL)) jz_str_drop(&c);
    }
    JZ_BTREE_FOR(&t->indexed_columns, k, v) {
        jz_str c = jz_str_clone(k);
        if (jz_btree_insert(out, &c, NULL, NULL)) jz_str_drop(&c);
    }
}

groove_RecordDescriptor SC(TableSchema__wire_record_descriptor)(const jazz_model_TableSchema *t) {
    u32 n = 6 + t->columns.len;
    jz_str *names = jz_alloc(sizeof(jz_str) * n);
    groove_ValueType *types = jz_alloc(sizeof(groove_ValueType) * n);
    names[0] = jz_str_from("row_uuid"); types[0] = VT(GROOVE_VT_UUID);
    names[1] = jz_str_from("created_by"); types[1] = jazz_types__ids__RowAuthor__value_type();
    names[2] = jz_str_from("created_at"); types[2] = VT(GROOVE_VT_U64);
    names[3] = jz_str_from("updated_by"); types[3] = jazz_types__ids__RowAuthor__value_type();
    names[4] = jz_str_from("updated_at"); types[4] = VT(GROOVE_VT_U64);
    names[5] = jz_str_from("_deletion"); types[5] = NULLABLE(deletion_column());
    for (u32 i = 0; i < t->columns.len; i++) {
        names[6 + i] = SC(app_storage_column_name)(jz_str_slice(&t->columns.ptr[i].name));
        types[6 + i] = NULLABLE(SC(storage_column_type)(&t->columns.ptr[i]));
    }
    groove_RecordDescriptor d = groove__records__RecordDescriptor__new(names, types, n);
    jz_free(names);
    jz_free(types);
    return d;
}

/* ---------------------------------------------------------------- fixed descriptors */
groove_RecordDescriptor SC(subscription_watermark_key_descriptor)(void) {
    static const char *const n[] = {"shape_id", "binding_id", "read_view_id", "policy_scope", "policy_binding_digest"};
    return DESC(n, VT(GROOVE_VT_UUID), VT(GROOVE_VT_UUID), VT(GROOVE_VT_UUID), VT(GROOVE_VT_U8), VT(GROOVE_VT_BYTES));
}
groove_RecordDescriptor SC(subscription_watermark_value_descriptor)(void) {
    static const char *const n[] = {"format_v1", "settled_through", "supporting_revision"};
    return DESC(n, VT(GROOVE_VT_U8), VT(GROOVE_VT_U64), VT(GROOVE_VT_BYTES));
}
groove_RecordDescriptor SC(local_availability_record_descriptor)(void) {
    static const char *const n[] = {"format_v1", "unavailable", "core", "core_epoch", "claims_revision", "policy_epoch", "settled_through", "evaluation_seq"};
    return DESC(n, VT(GROOVE_VT_U8), VT(GROOVE_VT_BOOL), VT(GROOVE_VT_UUID), VT(GROOVE_VT_U64), VT(GROOVE_VT_U64),
                VT(GROOVE_VT_U64), VT(GROOVE_VT_U64), VT(GROOVE_VT_U64));
}

/* ---------------------------------------------------------------- fixed system tables */
static groove_TableSchema schema_versions_table(void) {
    groove_ColumnSchemaVec cols = {0};
    JZ_VEC_PUSH(cols, column("id", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(cols, column("uuid", VT(GROOVE_VT_UUID)));
    JZ_VEC_PUSH(cols, column("physical_mapping", VT(GROOVE_VT_BYTES)));
    groove_TableSchema t = table_new("jazz_schema_versions", &cols);
    groove__schema__TableSchema__with_primary_key(&t, groove__schema__PrimaryKey__new(jz_str_from("id"), GROOVE_PKT_U64));
    return t;
}
static groove_TableSchema catalogue_table(void) {
    groove_ColumnSchemaVec cols = {0};
    JZ_VEC_PUSH(cols, column("kind", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(cols, column("id", VT(GROOVE_VT_UUID)));
    JZ_VEC_PUSH(cols, column("payload", VT(GROOVE_VT_BYTES)));
    groove_TableSchema t = table_new("jazz_catalogue", &cols);
    groove__schema__TableSchema__with_primary_key(&t, PK_COMPOSITE(pk_int("kind"), pk_uuid("id")));
    return t;
}
static groove_TableSchema nodes_table(void) {
    groove_ColumnSchemaVec cols = {0};
    JZ_VEC_PUSH(cols, column("id", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(cols, column("uuid", VT(GROOVE_VT_UUID)));
    groove_TableSchema t = table_new("jazz_nodes", &cols);
    groove__schema__TableSchema__with_primary_key(&t, groove__schema__PrimaryKey__new(jz_str_from("id"), GROOVE_PKT_U64));
    return t;
}

static groove_ValueType contribution_component_column(void) {
    static const char *const op[] = {"physical_column_id", "identity"};
    static const char *const col[] = {"physical_column_id"};
    groove_EnumCaseVec cases = {0};
    JZ_VEC_PUSH(cases, groove__records__values__EnumCase__new(jz_str_from("column"), DESC(col, VT(GROOVE_VT_U64))));
    JZ_VEC_PUSH(cases, groove__records__values__EnumCase__new(jz_str_from("operation"), DESC(op, VT(GROOVE_VT_U64), VT(GROOVE_VT_BYTES))));
    JZ_VEC_PUSH(cases, groove__records__values__EnumCase__new(jz_str_from("register"), empty_desc()));
    return enum_type("jazz_contribution_component", cases, "valid contribution component enum");
}
static groove_ValueType contribution_coordinate_column(void) {
    static const char *const n[] = {"branch_key", "physical_table_id", "row_uuid", "layer", "component"};
    static const char *const layers[] = {"content", "deletion"};
    return record_type(DESC(n, VT(GROOVE_VT_BYTES), VT(GROOVE_VT_U64), VT(GROOVE_VT_UUID),
                            storage_enum("jazz_contribution_layer", layers, 2), contribution_component_column()));
}
static groove_ValueType contribution_dot_column(void) {
    static const char *const n[] = {"tx_time", "tx_node", "coordinate"};
    return record_type(DESC(n, VT(GROOVE_VT_U64), VT(GROOVE_VT_UUID), contribution_coordinate_column()));
}
static groove_ValueType branch_view_copy_base_enum(void) {
    static const char *const cur[] = {"branch"};
    static const char *const snap[] = {"branch", "owner", "global_base", "local_base", "dots"};
    static const char *const dot[] = {"time", "node"};
    groove_EnumCaseVec cases = {0};
    JZ_VEC_PUSH(cases, groove__records__values__EnumCase__new(jz_str_from("current"), DESC(cur, VT(GROOVE_VT_BYTES))));
    JZ_VEC_PUSH(cases, groove__records__values__EnumCase__new(
                           jz_str_from("snapshot"),
                           DESC(snap, VT(GROOVE_VT_BYTES), VT(GROOVE_VT_UUID), VT(GROOVE_VT_U64), VT(GROOVE_VT_U64),
                                ARRAY_OF(record_type(DESC(dot, VT(GROOVE_VT_U64), VT(GROOVE_VT_UUID)))))));
    return enum_type("jazz_branch_view_copy_base_v1", cases, "valid branch-view copy base enum");
}
static groove_ValueType branch_write_operation_enum(void) {
    static const char *const vuc[] = {"evidence_index"};
    groove_EnumCaseVec cases = {0};
    JZ_VEC_PUSH(cases, groove__records__values__EnumCase__new(jz_str_from("exact_head_insert"), empty_desc()));
    JZ_VEC_PUSH(cases, groove__records__values__EnumCase__new(jz_str_from("exact_head_update"), empty_desc()));
    JZ_VEC_PUSH(cases, groove__records__values__EnumCase__new(jz_str_from("view_update_copy"), DESC(vuc, VT(GROOVE_VT_U32))));
    return enum_type("jazz_branch_write_operation_v1", cases, "valid branch write operation enum");
}
static groove_ValueType contribution_merge_column(void) {
    static const char *const top[] = {"source", "target", "substitutions", "branch_view_copy_v1", "branch_write_intent_v1"};
    static const char *const sub[] = {"target", "sources"};
    static const char *const bvc[] = {"version", "head", "base", "table", "row_uuid", "source_time", "source_node"};
    static const char *const bwi[] = {"version", "physical_table_id", "authored_schema", "row_uuid", "head", "operation"};
    groove_ValueType substitutions = ARRAY_OF(record_type(DESC(sub, contribution_coordinate_column(), ARRAY_OF(contribution_dot_column()))));
    groove_ValueType copies = ARRAY_OF(record_type(DESC(bvc, VT(GROOVE_VT_U8), VT(GROOVE_VT_BYTES), branch_view_copy_base_enum(),
                                                        VT(GROOVE_VT_STRING), VT(GROOVE_VT_UUID), VT(GROOVE_VT_U64), VT(GROOVE_VT_UUID))));
    groove_ValueType intents = ARRAY_OF(record_type(DESC(bwi, VT(GROOVE_VT_U8), VT(GROOVE_VT_U64), VT(GROOVE_VT_UUID),
                                                         VT(GROOVE_VT_UUID), VT(GROOVE_VT_BYTES), branch_write_operation_enum())));
    return NULLABLE(record_type(DESC(top, VT(GROOVE_VT_BYTES), VT(GROOVE_VT_BYTES), substitutions, copies, intents)));
}

/* Bound physical type used by the transaction codec. */
groove_ValueType SC(contribution_merge_storage_type)(void) {
    groove_ColumnSchemaVec cols = {0};
    JZ_VEC_PUSH(cols, column("contribution_merge", contribution_merge_column()));
    groove_TableSchema t = table_new("jazz_transactions", &cols);
    JZ_ASSERT(t.columns.len > 0, "contribution merge storage column");
    groove_ValueType out = t.columns.ptr[0].column_type;
    memset(&t.columns.ptr[0].column_type, 0, sizeof(groove_ValueType)); /* moved out */
    t.columns.ptr[0].column_type.tag = GROOVE_VT_U8;
    groove__schema__TableSchema__drop(&t);
    return out;
}

static groove_TableSchema transactions_table(void) {
    groove_ColumnSchemaVec cols = {0};
    JZ_VEC_PUSH(cols, column("time", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(cols, column("node_id", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(cols, column("kind", tx_kind_column()));
    JZ_VEC_PUSH(cols, column("n_total_writes", VT(GROOVE_VT_U32)));
    JZ_VEC_PUSH(cols, column("made_by", jazz_types__ids__RowAuthor__value_type()));
    JZ_VEC_PUSH(cols, column("base_snapshot", NULLABLE(VT(GROOVE_VT_BYTES))));
    JZ_VEC_PUSH(cols, column("row_read_set", NULLABLE(VT(GROOVE_VT_BYTES))));
    JZ_VEC_PUSH(cols, column("absent_read_set", NULLABLE(VT(GROOVE_VT_BYTES))));
    JZ_VEC_PUSH(cols, column("predicate_read_set", NULLABLE(VT(GROOVE_VT_BYTES))));
    JZ_VEC_PUSH(cols, column("user_metadata", NULLABLE(VT(GROOVE_VT_STRING))));
    JZ_VEC_PUSH(cols, column("contribution_merge", contribution_merge_column()));
    JZ_VEC_PUSH(cols, column("permission_subject", NULLABLE(jazz_types__ids__AuthorSubject__value_type())));
    JZ_VEC_PUSH(cols, column("is_partial", VT(GROOVE_VT_BOOL)));
    JZ_VEC_PUSH(cols, column("fate", fate_column()));
    JZ_VEC_PUSH(cols, column("global_time", NULLABLE(VT(GROOVE_VT_U64))));
    JZ_VEC_PUSH(cols, column("rejection_reason", NULLABLE(rejection_reason_column())));
    JZ_VEC_PUSH(cols, column("durability", durability_column()));
    groove_TableSchema t = table_new("jazz_transactions", &cols);
    groove__schema__TableSchema__with_primary_key(&t, PK_COMPOSITE(pk_int("time"), pk_int("node_id")));
    jz_str idx = jz_str_from("global_time");
    groove__schema__TableSchema__with_index(&t, groove__schema__IndexSchema__new(jz_str_from("by_global_time"), &idx, 1));
    return t;
}
static groove_ValueType touched_rows_column(void) {
    static const char *const n[] = {"physical_table_id", "branch_key", "row_uuids"};
    return ARRAY_OF(record_type(DESC(n, VT(GROOVE_VT_U64), VT(GROOVE_VT_BYTES), ARRAY_OF(VT(GROOVE_VT_UUID)))));
}
static groove_TableSchema tx_touched_rows_table(void) {
    groove_ColumnSchemaVec cols = {0};
    JZ_VEC_PUSH(cols, column("tx_time", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(cols, column("tx_node_id", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(cols, column("touched_rows", touched_rows_column()));
    groove_TableSchema t = table_new("jazz_tx_touched_rows", &cols);
    groove__schema__TableSchema__with_primary_key(&t, PK_COMPOSITE(pk_int("tx_time"), pk_int("tx_node_id")));
    return t;
}
static groove_TableSchema rejected_transactions_table(void) {
    groove_ColumnSchemaVec cols = {0};
    JZ_VEC_PUSH(cols, column("time", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(cols, column("node_id", VT(GROOVE_VT_U64)));
    JZ_VEC_PUSH(cols, column("kind", tx_kind_column()));
    JZ_VEC_PUSH(cols, column("made_by", jazz_types__ids__RowAuthor__value_type()));
    JZ_VEC_PUSH(cols, column("rejection_reason", rejection_reason_column()));
    JZ_VEC_PUSH(cols, column("user_metadata", NULLABLE(VT(GROOVE_VT_STRING))));
    groove_TableSchema t = table_new("jazz_rejected_transactions", &cols);
    groove__schema__TableSchema__with_primary_key(&t, PK_COMPOSITE(pk_int("time"), pk_int("node_id")));
    return t;
}

/* ================================================================ RuntimeSchema */
void SC(RuntimeSchema__drop)(jazz_model_RuntimeSchema *s) { jz_vec_drop(&s->tables, sizeof(jazz_model_TableSchema), SC(TableSchema__drop_v)); }
void SC(RuntimeSchema__Clone__clone)(jazz_model_RuntimeSchema *d, const jazz_model_RuntimeSchema *s) {
    jz_vec_clone(&d->tables, &s->tables, sizeof(jazz_model_TableSchema), SC(TableSchema__Clone__clone_v));
}
bool SC(RuntimeSchema__PartialEq__eq)(const jazz_model_RuntimeSchema *a, const jazz_model_RuntimeSchema *b) {
    return JAZZ_VEC_EQ(&a->tables, &b->tables, SC(TableSchema__PartialEq__eq));
}
const jazz_model_TableSchema *SC(RuntimeSchema__table)(const jazz_model_RuntimeSchema *s, jz_slice name) {
    JZ_VEC_FOR(const jazz_model_TableSchema, t, s->tables)
        if (jazz_model__slice_eq(jz_str_slice(&t->name), name)) return t;
    return NULL;
}

static jz_err string_err(jz_str msg) {
    jz_err e = jz_err_fmt(JAZZ_MODEL_ERR_STRING, "%.*s", (int)msg.len, msg.ptr);
    jz_str_drop(&msg);
    return e;
}
#define TNAME(t) (int)(t)->name.len, (t)->name.ptr
#define SNAME(s) (int)(s)->len, (s)->ptr

/* RecordDescriptor::new([("value", column_type.clone())]).create(&[value]) */
static bool value_fits_column(const groove_Value *value, const groove_ValueType *t) {
    jz_str name = jz_str_from("value");
    groove_ValueType tc;
    groove__records__values__ValueType__Clone__clone(&tc, t);
    groove_RecordDescriptor d = groove__records__RecordDescriptor__new(&name, &tc, 1);
    jz_bytes raw = {0};
    jz_err e = groove__records__RecordDescriptor__create(d, value, 1, &raw);
    jz_bytes_drop(&raw);
    if (e.code) { jz_err_drop(&e); return false; }
    return true;
}
static int branch_entry_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    return jz_str_cmp(&((const jazz_model_BranchKeyEntry *)a)->name, &((const jazz_model_BranchKeyEntry *)b)->name);
}

jz_err SC(RuntimeSchema__project_branch_selector)(const jazz_model_RuntimeSchema *s, const jazz_model_TableSchema *table,
                                                  const jazz_model_BranchSelector *selector, jazz_model_BranchKey *key_out,
                                                  jz_btree *cells_out) {
    (void)s;
    if (selector->values.len != table->branch_by.len)
        return string_err(jz_format("branch selector for %.*s must provide exactly %u values", TNAME(table), table->branch_by.len));
    jazz_model_BranchKey key = {{0}};
    jz_btree cells;
    jz_btree_init(&cells, &jazz_model__STR_GVALUE_DESC);
    jz_err err = JZ_OK;
    JZ_VEC_FOR(const jz_str, column_name, table->branch_by) {
        const jazz_model_ColumnSchema *column = SC(TableSchema__column)(table, jz_str_slice(column_name));
        if (!column) {
            jz_str dbg = {0};
            jazz_model__debug_str(&dbg, column_name->ptr, column_name->len);
            err = string_err(jz_format("unknown branch column %.*s on %.*s", SNAME(&dbg), TNAME(table)));
            jz_str_drop(&dbg);
            break;
        }
        const jazz_model_BranchColumnValue *encoded = jz_btree_get(&selector->values, column_name);
        if (!encoded) { err = string_err(jz_format("missing branch column %.*s", SNAME(column_name))); break; }
        groove_Value value;
        jz_err e = jazz_model__model__branch__BranchColumnValue__decode(encoded, &value);
        if (e.code) {
            jz_err_drop(&e);
            err = string_err(jz_format("invalid branch column %.*s encoding", SNAME(column_name)));
            break;
        }
        jazz_model_BranchColumnValue typed;
        if (!value_fits_column(&value, &column->column_type) ||
            jazz_model__model__branch__BranchColumnValue__encode_typed(&value, &column->column_type, &typed).code) {
            groove__records__values__Value__drop(&value);
            err = string_err(jz_format("invalid branch column %.*s value", SNAME(column_name)));
            break;
        }
        jazz_model_BranchKeyEntry entry = {jz_str_clone(column_name), typed};
        JZ_VEC_PUSH(key.values, entry);
        jz_str cell_key = jz_str_clone(column_name);
        jz_btree_insert(&cells, &cell_key, &value, NULL);
    }
    if (err.code) {
        jazz_model__model__branch__BranchKey__drop(&key);
        jz_btree_drop(&cells);
        return err;
    }
    jz_sort(key.values.ptr, key.values.len, sizeof(jazz_model_BranchKeyEntry), branch_entry_cmp, NULL);
    *key_out = key;
    *cells_out = cells;
    return JZ_OK;
}

jz_err SC(RuntimeSchema__project_branch_view_selector)(const jazz_model_RuntimeSchema *s, const jazz_model_TableSchema *table,
                                                       const jazz_model_BranchSelector *selector, jazz_model_BranchKey *key_out,
                                                       jz_btree *cells_out) {
    jz_btree schema_branch_columns, selector_columns;
    jz_btree_init(&schema_branch_columns, &jazz_model__STR_SET_DESC);
    jz_btree_init(&selector_columns, &jazz_model__STR_SET_DESC);
    JZ_VEC_FOR(const jazz_model_TableSchema, t, s->tables)
        JZ_VEC_FOR(const jz_str, c, t->branch_by) {
            jz_str k = jz_str_clone(c);
            if (jz_btree_insert(&schema_branch_columns, &k, NULL, NULL)) jz_str_drop(&k);
        }
    jz_str *k;
    void *v;
    JZ_BTREE_FOR(&selector->values, k, v) {
        jz_str c = jz_str_clone(k);
        jz_btree_insert(&selector_columns, &c, NULL, NULL);
    }
    bool same = jz_btree_eq(&selector_columns, &schema_branch_columns, NULL);
    jz_btree_drop(&schema_branch_columns);
    jz_btree_drop(&selector_columns);
    if (!same) return jz_err_new(JAZZ_MODEL_ERR_STRING, "branch view selector must provide every schema branch column exactly once");
    jazz_model_BranchSelector projected;
    jazz_model__model__branch__BranchSelector__new(&projected);
    JZ_VEC_FOR(const jz_str, column, table->branch_by) {
        const jazz_model_BranchColumnValue *val = jz_btree_get(&selector->values, column);
        if (!val) jz_panic("selector.values[column]");
        jz_str key = jz_str_clone(column);
        jazz_model_BranchColumnValue copy = {jz_bytes_clone(&val->bytes)};
        if (jz_btree_insert(&projected.values, &key, &copy, NULL)) jz_str_drop(&key);
    }
    jz_err e = SC(RuntimeSchema__project_branch_selector)(s, table, &projected, key_out, cells_out);
    jazz_model__model__branch__BranchSelector__drop(&projected);
    return e;
}

jz_err SC(RuntimeSchema__validate_authored_branch_key)(const jazz_model_RuntimeSchema *s, const jazz_model_TableSchema *table,
                                                       const jazz_model_BranchKey *key, jz_btree *cells_out) {
    (void)s;
    return SC(RuntimeSchema__validate_branch_key_for_table)(table, key, cells_out);
}

jz_err SC(RuntimeSchema__decode_persisted_branch_key)(const jazz_model_TableSchema *table, jz_slice bytes, jazz_model_BranchKey *out) {
    jazz_model_BranchKey key;
    jz_err e = jazz_model__model__branch__BranchKey__from_canonical_bytes(bytes, &key);
    if (e.code) {
        jz_err_drop(&e);
        return string_err(jz_format("invalid persisted branch key for %.*s", TNAME(table)));
    }
    jz_btree cells;
    e = SC(RuntimeSchema__validate_branch_key_for_table)(table, &key, &cells);
    if (e.code) { jazz_model__model__branch__BranchKey__drop(&key); return e; }
    jz_btree_drop(&cells);
    *out = key;
    return JZ_OK;
}

static int str_ptr_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    return jz_str_cmp(*(const jz_str *const *)a, *(const jz_str *const *)b);
}

jz_err SC(RuntimeSchema__validate_branch_key_for_table)(const jazz_model_TableSchema *table, const jazz_model_BranchKey *key, jz_btree *cells_out) {
    u32 n = table->branch_by.len;
    const jz_str **bindings = jz_alloc(sizeof(jz_str *) * (n ? n : 1));
    for (u32 i = 0; i < n; i++) bindings[i] = &table->branch_by.ptr[i];
    jz_sort(bindings, n, sizeof(jz_str *), str_ptr_cmp, NULL);
    jz_err err = JZ_OK;
    jz_btree cells;
    jz_btree_init(&cells, &jazz_model__STR_GVALUE_DESC);
    if (key->values.len != n) {
        err = string_err(jz_format("branch key for %.*s must contain exactly %u values", TNAME(table), n));
        goto done;
    }
    for (u32 i = 0; i < n; i++) {
        const jz_str *column_name = bindings[i];
        const jazz_model_BranchKeyEntry *entry = &key->values.ptr[i];
        if (!jz_str_eq(&entry->name, column_name)) {
            err = string_err(jz_format("branch key for %.*s is not in canonical column order", TNAME(table)));
            goto done;
        }
        const jazz_model_ColumnSchema *column = SC(TableSchema__column)(table, jz_str_slice(column_name));
        if (!column) { err = string_err(jz_format("unknown branch column on %.*s", TNAME(table))); goto done; }
        groove_Value value;
        jz_err e = jazz_model__model__branch__BranchColumnValue__decode_as(&entry->value, &column->column_type, &value);
        if (e.code) {
            jz_err_drop(&e);
            err = string_err(jz_format("invalid branch column %.*s encoding", SNAME(column_name)));
            goto done;
        }
        jazz_model_BranchColumnValue reencoded;
        e = jazz_model__model__branch__BranchColumnValue__encode_typed(&value, &column->column_type, &reencoded);
        if (e.code) {
            jz_err_drop(&e);
            groove__records__values__Value__drop(&value);
            err = string_err(jz_format("invalid branch column %.*s value", SNAME(column_name)));
            goto done;
        }
        bool canonical = jz_bytes_eq(&reencoded.bytes, &entry->value.bytes);
        jz_bytes_drop(&reencoded.bytes);
        if (!canonical) {
            groove__records__values__Value__drop(&value);
            err = string_err(jz_format("non-canonical branch column %.*s encoding", SNAME(column_name)));
            goto done;
        }
        if (!value_fits_column(&value, &column->column_type)) {
            groove__records__values__Value__drop(&value);
            err = string_err(jz_format("invalid branch column %.*s value", SNAME(column_name)));
            goto done;
        }
        jz_str k = jz_str_clone(column_name);
        jz_btree_insert(&cells, &k, &value, NULL);
    }
done:
    jz_free(bindings);
    if (err.code) { jz_btree_drop(&cells); return err; }
    *cells_out = cells;
    return JZ_OK;
}

void SC(RuntimeSchema__storage_tables)(const jazz_model_RuntimeSchema *s, groove_TableSchemaVec *out) {
    (void)s;
    memset(out, 0, sizeof(*out));
    JZ_VEC_PUSH(*out, nodes_table());
    JZ_VEC_PUSH(*out, schema_versions_table());
    JZ_VEC_PUSH(*out, catalogue_table());
    JZ_VEC_PUSH(*out, transactions_table());
    JZ_VEC_PUSH(*out, tx_touched_rows_table());
    JZ_VEC_PUSH(*out, rejected_transactions_table());
}
void SC(RuntimeSchema__catalogue_meta_storage_tables)(const jazz_model_RuntimeSchema *s, groove_TableSchemaVec *out) {
    (void)s;
    memset(out, 0, sizeof(*out));
    JZ_VEC_PUSH(*out, nodes_table());
    JZ_VEC_PUSH(*out, schema_versions_table());
    JZ_VEC_PUSH(*out, catalogue_table());
}

static groove_DatabaseSchema with_jazz_direct_record_stores(groove_DatabaseSchema schema) {
    static const char *const digest[] = {"policy_binding_digest"};
    static const char *const subject_claims[] = {"subject", "claims_v1"};
    static const char *const availability_key[] = {"policy_binding_digest", "global_table", "row"};
    static const char *const superseded_key[] = {"physical_table_id", "branch_key", "row", "node"};
    static const char *const superseded_value[] = {"superseded_through"};
    groove__schema__DatabaseSchema__with_direct_record_store(
        &schema, groove__schema__DirectRecordStoreSchema__new(
                     jz_str_from(JAZZ_AUTHORITY_POLICY_BINDINGS_STORE), DESC(digest, VT(GROOVE_VT_BYTES)),
                     DESC(subject_claims, VT(GROOVE_VT_STRING),
                          jazz_model__model__policy_directory__policy_binding_directory_claims_value_type())));
    groove__schema__DatabaseSchema__with_direct_record_store(
        &schema, groove__schema__DirectRecordStoreSchema__new(jz_str_from(JAZZ_SUBSCRIPTION_WATERMARKS_STORE),
                                                              SC(subscription_watermark_key_descriptor)(),
                                                              SC(subscription_watermark_value_descriptor)()));
    groove__schema__DatabaseSchema__with_direct_record_store(
        &schema, groove__schema__DirectRecordStoreSchema__new(
                     jz_str_from(JAZZ_LOCAL_ROW_AVAILABILITY_STORE),
                     DESC(availability_key, VT(GROOVE_VT_BYTES), VT(GROOVE_VT_UUID), VT(GROOVE_VT_UUID)),
                     SC(local_availability_record_descriptor)()));
    groove__schema__DatabaseSchema__with_direct_record_store(
        &schema, groove__schema__DirectRecordStoreSchema__new(
                     jz_str_from(JAZZ_SUPERSEDED_WRITES_STORE),
                     DESC(superseded_key, VT(GROOVE_VT_U64), VT(GROOVE_VT_BYTES), VT(GROOVE_VT_UUID), VT(GROOVE_VT_UUID)),
                     DESC(superseded_value, VT(GROOVE_VT_U64))));
    return schema;
}

groove_DatabaseSchema SC(RuntimeSchema__lower_to_groove)(const jazz_model_RuntimeSchema *s) {
    groove_TableSchemaVec tables;
    SC(RuntimeSchema__storage_tables)(s, &tables);
    return with_jazz_direct_record_stores(groove__schema__DatabaseSchema__new(tables));
}
groove_DatabaseSchema SC(RuntimeSchema__lower_catalogue_meta_to_groove)(const jazz_model_RuntimeSchema *s) {
    groove_TableSchemaVec tables;
    SC(RuntimeSchema__catalogue_meta_storage_tables)(s, &tables);
    return with_jazz_direct_record_stores(groove__schema__DatabaseSchema__new(tables));
}

void SC(RuntimeSchema__column_families)(const jazz_model_RuntimeSchema *s, jazz_model_StrVec *out) {
    groove_DatabaseSchema lowered = SC(RuntimeSchema__lower_to_groove)(s);
    groove_SliceVec logical = {0};
    groove__schema__DatabaseSchema__column_families(&lowered, &logical);
    JZ_VEC_PUSH(logical, JZ_LIT("indices"));
    /* the shared physical row classes stay available for the empty bootstrap schema */
    JZ_VEC_PUSH(logical, JZ_LIT("jazz_physical_history"));
    JZ_VEC_PUSH(logical, JZ_LIT("jazz_physical_global_current"));
    JZ_VEC_PUSH(logical, JZ_LIT("jazz_physical_ahead_current"));
    memset(out, 0, sizeof(*out));
    groove__storage__StorageLayout__physical_column_families(GROOVE_STORAGE_LAYOUT_JAZZ_CLASS_V2, logical.ptr, logical.len, (void *)out);
    jz_free(logical.ptr);
    groove__schema__DatabaseSchema__drop(&lowered);
}
void SC(RuntimeSchema__physical_column_families)(const jazz_model_RuntimeSchema *s, jazz_model_StrVec *out) {
    SC(RuntimeSchema__column_families)(s, out);
}

/* ---------------------------------------------------------------- canonical schema bytes */
#define SCHEMA_ID_DOMAIN_V1 "jazz-schema-v1-large-value-kinds"
#define SCHEMA_ID_DOMAIN_V2_COMPOSITE_INDEXES "jazz-schema-v2-composite-indexes"

static void put_u64(jz_bytes *b, u64 v) {
    u8 be[8];
    for (int i = 0; i < 8; i++) be[i] = (u8)(v >> (56 - 8 * i));
    jz_bytes_push(b, be, 8);
}
static void put_bytes(jz_bytes *b, const u8 *p, u32 n) {
    put_u64(b, n);
    jz_bytes_push(b, p, n);
}
static void put_str(jz_bytes *b, const jz_str *s) { put_bytes(b, (const u8 *)s->ptr, s->len); }
static void put_cstr(jz_bytes *b, const char *s) { put_bytes(b, (const u8 *)s, (u32)strlen(s)); }
static void put_merge_strategy(jz_bytes *b, u8 strategy) { jz_bytes_push_u8(b, strategy); /* Lww 0, Counter 1, GSet 2 */ }

static void put_column_type(jz_bytes *b, const groove_ValueType *t);
static void put_fields(jz_bytes *b, groove_RecordDescriptor d) {
    u32 n;
    const groove_DescriptorField *f = groove__records__RecordDescriptor__fields(d, &n);
    put_u64(b, n);
    for (u32 i = 0; i < n; i++) {
        if (f[i].has_name) { jz_bytes_push_u8(b, 1); put_str(b, &f[i].name); }
        else jz_bytes_push_u8(b, 0);
        put_column_type(b, &f[i].value_type);
    }
}
static void put_column_type(jz_bytes *b, const groove_ValueType *t) {
    switch (t->tag) {
    case GROOVE_VT_U8: jz_bytes_push_u8(b, 0); break;
    case GROOVE_VT_U16: jz_bytes_push_u8(b, 1); break;
    case GROOVE_VT_U32: jz_bytes_push_u8(b, 2); break;
    case GROOVE_VT_U64: jz_bytes_push_u8(b, 3); break;
    case GROOVE_VT_I32: jz_bytes_push_u8(b, 4); break;
    case GROOVE_VT_I64: jz_bytes_push_u8(b, 5); break;
    case GROOVE_VT_F64: jz_bytes_push_u8(b, 6); break;
    case GROOVE_VT_BOOL: jz_bytes_push_u8(b, 7); break;
    case GROOVE_VT_STRING: jz_bytes_push_u8(b, 8); break;
    case GROOVE_VT_BYTES: jz_bytes_push_u8(b, 9); break;
    case GROOVE_VT_UUID: jz_bytes_push_u8(b, 10); break;
    case GROOVE_VT_ENUM_TAG:
        jz_bytes_push_u8(b, 11);
        put_str(b, &t->u.enum_tag.name);
        put_u64(b, t->u.enum_tag.variants.len);
        JZ_VEC_FOR(const jz_str, v, t->u.enum_tag.variants) put_str(b, v);
        break;
    case GROOVE_VT_TUPLE:
        jz_bytes_push_u8(b, 12);
        put_u64(b, t->u.tuple.len);
        JZ_VEC_FOR(const groove_ValueType, m, t->u.tuple) put_column_type(b, m);
        break;
    case GROOVE_VT_ARRAY: jz_bytes_push_u8(b, 13); put_column_type(b, t->u.inner); break;
    case GROOVE_VT_NULLABLE: jz_bytes_push_u8(b, 14); put_column_type(b, t->u.inner); break;
    case GROOVE_VT_RECORD: jz_bytes_push_u8(b, 15); put_fields(b, t->u.record); break;
    case GROOVE_VT_ENUM:
        jz_bytes_push_u8(b, 16);
        put_str(b, &t->u.enum_->name);
        put_u64(b, t->u.enum_->cases.len);
        JZ_VEC_FOR(const groove_EnumCase, c, t->u.enum_->cases) {
            put_str(b, &c->name);
            put_fields(b, c->payload);
        }
        break;
    default: jz_panic("raw stored-scalar backing types cannot appear in a Jazz schema");
    }
}
static int table_ptr_name_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    return jz_str_cmp(&(*(const jazz_model_TableSchema *const *)a)->name, &(*(const jazz_model_TableSchema *const *)b)->name);
}
static jz_bytes canonical_schema_bytes(const jazz_model_RuntimeSchema *schema) {
    bool with_composite_indexes = false;
    JZ_VEC_FOR(const jazz_model_TableSchema, t, schema->tables) if (t->composite_indexes.len) with_composite_indexes = true;
    jz_bytes bytes = {0};
    put_cstr(&bytes, with_composite_indexes ? SCHEMA_ID_DOMAIN_V2_COMPOSITE_INDEXES : SCHEMA_ID_DOMAIN_V1);
    u32 nt = schema->tables.len;
    const jazz_model_TableSchema **tables = jz_alloc(sizeof(void *) * (nt ? nt : 1));
    for (u32 i = 0; i < nt; i++) tables[i] = &schema->tables.ptr[i];
    jz_sort(tables, nt, sizeof(void *), table_ptr_name_cmp, NULL);
    put_u64(&bytes, nt);
    for (u32 i = 0; i < nt; i++) {
        const jazz_model_TableSchema *table = tables[i];
        put_str(&bytes, &table->name);
        put_u64(&bytes, table->columns.len);
        JZ_VEC_FOR(const jazz_model_ColumnSchema, c, table->columns) {
            put_str(&bytes, &c->name);
            put_column_type(&bytes, &c->column_type);
            jz_bytes_push_u8(&bytes, c->large_value_kind); /* NotLarge 0, Bytes 1, String 2, Json 3 */
            put_merge_strategy(&bytes, SC(TableSchema__merge_strategy)(table, jz_str_slice(&c->name)));
        }
        put_u64(&bytes, table->references.len);
        jz_str *k, *v;
        JZ_BTREE_FOR(&table->references, k, v) {
            put_str(&bytes, k);
            put_str(&bytes, v);
        }
        u32 nb = table->branch_by.len;
        const jz_str **branch_by = jz_alloc(sizeof(void *) * (nb ? nb : 1));
        for (u32 j = 0; j < nb; j++) branch_by[j] = &table->branch_by.ptr[j];
        jz_sort(branch_by, nb, sizeof(void *), str_ptr_cmp, NULL);
        put_u64(&bytes, nb);
        for (u32 j = 0; j < nb; j++) put_str(&bytes, branch_by[j]);
        jz_free(branch_by);
        if (with_composite_indexes) {
            put_u64(&bytes, table->composite_indexes.len);
            jazz_model_StrVec *cols;
            void *unit;
            JZ_BTREE_FOR(&table->composite_indexes, cols, unit) {
                put_u64(&bytes, cols->len);
                JZ_VEC_FOR(const jz_str, c, *cols) put_str(&bytes, c);
            }
        }
    }
    jz_free(tables);
    return bytes;
}
jz_bytes SC(RuntimeSchema__canonical_bytes)(const jazz_model_RuntimeSchema *s) { return canonical_schema_bytes(s); }
jazz_types_SchemaVersionId SC(RuntimeSchema__version_id)(const jazz_model_RuntimeSchema *s) {
    jz_bytes bytes = canonical_schema_bytes(s);
    jz_uuid id = jz_uuid_v5(&jazz_model__schema__SCHEMA_VERSION_NAMESPACE, bytes.ptr, bytes.len);
    jz_bytes_drop(&bytes);
    return id;
}

/* ---------------------------------------------------------------- physical epoch compatibility */
static bool physical_record_epoch_is_compatible(groove_RecordDescriptor source, groove_RecordDescriptor target) {
    u32 ns, nt;
    const groove_DescriptorField *a = groove__records__RecordDescriptor__fields(source, &ns);
    const groove_DescriptorField *b = groove__records__RecordDescriptor__fields(target, &nt);
    if (ns != nt) return false;
    for (u32 i = 0; i < ns; i++) {
        if (a[i].has_name != b[i].has_name || (a[i].has_name && !jz_str_eq(&a[i].name, &b[i].name))) return false;
        if (!SC(physical_value_epoch_is_compatible)(&a[i].value_type, &b[i].value_type)) return false;
    }
    return true;
}
bool SC(physical_value_epoch_is_compatible)(const groove_ValueType *source, const groove_ValueType *target) {
    if (source->tag == target->tag) {
        switch (source->tag) {
        case GROOVE_VT_ENUM_TAG: {
            const groove_StrVec *l = &source->u.enum_tag.variants, *r = &target->u.enum_tag.variants;
            if (r->len < l->len) return false;
            for (u32 i = 0; i < l->len; i++) if (!jz_str_eq(&l->ptr[i], &r->ptr[i])) return false;
            return true;
        }
        case GROOVE_VT_ENUM: {
            const groove_EnumCaseVec *l = &source->u.enum_->cases, *r = &target->u.enum_->cases;
            if (r->len < l->len) return false;
            for (u32 i = 0; i < l->len; i++)
                if (!jz_str_eq(&l->ptr[i].name, &r->ptr[i].name) || !physical_record_epoch_is_compatible(l->ptr[i].payload, r->ptr[i].payload))
                    return false;
            return true;
        }
        case GROOVE_VT_TUPLE:
            if (source->u.tuple.len != target->u.tuple.len) return false;
            for (u32 i = 0; i < source->u.tuple.len; i++)
                if (!SC(physical_value_epoch_is_compatible)(&source->u.tuple.ptr[i], &target->u.tuple.ptr[i])) return false;
            return true;
        case GROOVE_VT_ARRAY:
        case GROOVE_VT_NULLABLE: return SC(physical_value_epoch_is_compatible)(source->u.inner, target->u.inner);
        case GROOVE_VT_RECORD: return physical_record_epoch_is_compatible(source->u.record, target->u.record);
        default: break;
        }
    }
    return groove__records__values__ValueType__PartialEq__eq(source, target);
}
bool SC(physical_column_epoch_is_compatible)(const jazz_model_TableSchema *source_table, jz_slice source_column_name,
                                             const jazz_model_TableSchema *target_table, jz_slice target_column_name) {
    const jazz_model_ColumnSchema *source = SC(TableSchema__column)(source_table, source_column_name);
    if (!source) return false;
    const jazz_model_ColumnSchema *target = SC(TableSchema__column)(target_table, target_column_name);
    if (!target) return false;
    return SC(physical_value_epoch_is_compatible)(&source->column_type, &target->column_type) &&
           SC(TableSchema__merge_strategy)(source_table, source_column_name) == SC(TableSchema__merge_strategy)(target_table, target_column_name);
}

/* ================================================================ JazzSchema */
#define PS(x) jazz_model__model__public_api__types__schema__##x
void SC(JazzSchema__drop)(jazz_model_JazzSchema *s) {
    PS(Schema__drop)(&s->public_schema);
    SC(RuntimeSchema__drop)(&s->runtime);
}
void SC(JazzSchema__Clone__clone)(jazz_model_JazzSchema *d, const jazz_model_JazzSchema *s) {
    PS(Schema__Clone__clone)(&d->public_schema, &s->public_schema);
    SC(RuntimeSchema__Clone__clone)(&d->runtime, &s->runtime);
}
bool SC(JazzSchema__PartialEq__eq)(const jazz_model_JazzSchema *a, const jazz_model_JazzSchema *b) {
    return SC(RuntimeSchema__PartialEq__eq)(&a->runtime, &b->runtime);
}
jz_err SC(JazzSchema__new)(const jazz_model_public_Schema *schema, jazz_model_JazzSchema *out) {
    return jazz_model__model__public_schema_convert__convert_public_schema(schema, out);
}
void SC(JazzSchema__empty)(jazz_model_JazzSchema *out) {
    jazz_model_public_Schema empty;
    PS(Schema__new)(&empty);
    jz_err e = SC(JazzSchema__new)(&empty, out);
    PS(Schema__drop)(&empty);
    if (e.code) jz_panic("empty public schema always compiles");
}
static void reset_public_policies(jazz_model_public_Schema *s) {
    jazz_model_TableName *name;
    jazz_model_public_TableSchema *table;
    JZ_BTREE_FOR(&s->tables, name, table) {
        jazz_model__model__public_api__types__policy__TablePolicies__drop(&table->policies);
        memset(&table->policies, 0, sizeof(table->policies));
    }
}
void SC(JazzSchema__without_permissions)(const jazz_model_JazzSchema *s, jazz_model_JazzSchema *out) {
    SC(JazzSchema__Clone__clone)(out, s);
    reset_public_policies(&out->public_schema);
    JZ_VEC_FOR(jazz_model_TableSchema, t, out->runtime.tables) {
        opt_query_drop(&t->read_policy);
        SC(WritePolicies__drop)(&t->write_policies);
    }
}
static jazz_model_RuntimeSchema lineage_normalize(const jazz_model_JazzSchema *schema) {
    jazz_model_RuntimeSchema runtime;
    SC(RuntimeSchema__Clone__clone)(&runtime, &schema->runtime);
    JZ_VEC_FOR(jazz_model_TableSchema, t, runtime.tables) {
        opt_query_drop(&t->read_policy);
        SC(WritePolicies__drop)(&t->write_policies);
        jz_btree_clear(&t->indexed_columns);
        JZ_VEC_FOR(jazz_model_ColumnSchema, c, t->columns) {
            bool is_branch = false;
            JZ_VEC_FOR(const jz_str, b, t->branch_by) if (jz_str_eq(b, &c->name)) is_branch = true;
            if (!is_branch && c->has_default) {
                groove__records__values__Value__drop(&c->default_);
                c->has_default = false;
            }
        }
    }
    return runtime;
}
bool SC(JazzSchema__has_same_lineage_structure)(const jazz_model_JazzSchema *a, const jazz_model_JazzSchema *b) {
    jazz_model_RuntimeSchema x = lineage_normalize(a), y = lineage_normalize(b);
    bool same = SC(RuntimeSchema__PartialEq__eq)(&x, &y);
    SC(RuntimeSchema__drop)(&x);
    SC(RuntimeSchema__drop)(&y);
    return same;
}
void SC(JazzSchema__with_permissions_from)(const jazz_model_JazzSchema *s, const jazz_model_JazzSchema *selected, jazz_model_JazzSchema *out) {
    jazz_types_SchemaVersionId a = SC(RuntimeSchema__version_id)(&s->runtime), b = SC(RuntimeSchema__version_id)(&selected->runtime);
    JZ_ASSERT(jz_uuid_eq(&a, &b), "assertion `left == right` failed");
    SC(JazzSchema__Clone__clone)(out, s);
    jazz_model_TableName *name;
    jazz_model_public_TableSchema *table;
    JZ_BTREE_FOR(&out->public_schema.tables, name, table) {
        const jazz_model_public_TableSchema *other = jz_btree_get(&selected->public_schema.tables, name);
        if (!other) jz_panic("same schema tables");
        jazz_model__model__public_api__types__policy__TablePolicies__drop(&table->policies);
        jazz_model__model__public_api__types__policy__TablePolicies__Clone__clone(&table->policies, &other->policies);
    }
    JZ_VEC_FOR(jazz_model_TableSchema, t, out->runtime.tables) {
        const jazz_model_TableSchema *other = SC(RuntimeSchema__table)(&selected->runtime, jz_str_slice(&t->name));
        if (!other) jz_panic("same schema tables");
        opt_query_drop(&t->read_policy);
        t->read_policy = opt_query_clone(other->read_policy);
        SC(WritePolicies__drop)(&t->write_policies);
        SC(WritePolicies__Clone__clone)(&t->write_policies, &other->write_policies);
    }
}
const jazz_model_public_Schema *SC(JazzSchema__public_schema)(const jazz_model_JazzSchema *s) { return &s->public_schema; }
const jazz_model_TableSchemaVec *SC(JazzSchema__tables)(const jazz_model_JazzSchema *s) { return &s->runtime.tables; }
jazz_model_JazzSchema SC(JazzSchema__from_runtime)(jazz_model_public_Schema public_schema, jazz_model_RuntimeSchema runtime) {
    jazz_model_JazzSchema s = {public_schema, runtime};
    return s;
}
const jazz_model_RuntimeSchema *SC(JazzSchema__runtime)(const jazz_model_JazzSchema *s) { return &s->runtime; }
