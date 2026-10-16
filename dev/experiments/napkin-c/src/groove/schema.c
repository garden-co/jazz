/* groove::schema (crates/groove/src/schema.rs), non-test part.
 * Derived Clone / PartialEq / Drop are written out; serde derives are not
 * shipped in the browser build and are omitted. The #[cfg(test)]
 * VARIANT_DESCRIPTOR_BUILDS counter is omitted. */
#include "jz_rt.h"
#include "schema.h"
#include "_ext_groove-storage.h"

static void str_drop_v(void *p) { jz_str_drop((jz_str *)p); }
static void str_clone_v(void *d, const void *s) { *(jz_str *)d = jz_str_clone((const jz_str *)s); }
static void strvec_drop(groove_StrVec *v) { jz_vec_drop(v, sizeof(jz_str), str_drop_v); }
static void strvec_clone(groove_StrVec *dst, const groove_StrVec *src) { jz_vec_clone(dst, src, sizeof(jz_str), str_clone_v); }
static bool strvec_eq(const groove_StrVec *a, const groove_StrVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!jz_str_eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
static bool str_eq_slice(const jz_str *s, const char *p, u32 n) { return s->len == n && (n == 0 || memcmp(s->ptr, p, n) == 0); }

static void vt_clone(groove_ValueType *d, const groove_ValueType *s) { groove__records__values__ValueType__Clone__clone(d, s); }
static void vt_drop(groove_ValueType *t) { groove__records__values__ValueType__drop(t); }
static bool vt_eq(const groove_ValueType *a, const groove_ValueType *b) { return groove__records__values__ValueType__PartialEq__eq(a, b); }

/* assert! / assert_eq! panics */
static void assert_failed(const char *msg) {
    jz_str m = jz_format("assertion `left == right` failed: %s", msg);
    jz_panic(m.ptr ? m.ptr : msg);
}

/* ======================================================================
 * ColumnSchema
 * ====================================================================== */
groove_ColumnSchema groove__schema__ColumnSchema__new(jz_str name, groove_ColumnType column_type) {
    groove_ColumnSchema c = {name, column_type};
    return c;
}
bool groove__schema__ColumnSchema__PartialEq__eq(const groove_ColumnSchema *a, const groove_ColumnSchema *b) {
    return jz_str_eq(&a->name, &b->name) && vt_eq(&a->column_type, &b->column_type);
}
void groove__schema__ColumnSchema__Clone__clone(groove_ColumnSchema *dst, const groove_ColumnSchema *src) {
    dst->name = jz_str_clone(&src->name);
    vt_clone(&dst->column_type, &src->column_type);
}
static void column_clone_v(void *d, const void *s) { groove__schema__ColumnSchema__Clone__clone((groove_ColumnSchema *)d, (const groove_ColumnSchema *)s); }
void groove__schema__ColumnSchema__drop(void *p) {
    groove_ColumnSchema *c = (groove_ColumnSchema *)p;
    jz_str_drop(&c->name);
    vt_drop(&c->column_type);
}

/* ======================================================================
 * PrimaryKey / PrimaryKeyColumn / PrimaryKeyType / IntegerKeyType
 * ====================================================================== */
groove_ColumnType groove__schema__IntegerKeyType__column_type(u8 integer_type) {
    switch (integer_type) {
    case GROOVE_INT_KEY_U8: return groove__records__values__ValueType__scalar(GROOVE_VT_U8);
    case GROOVE_INT_KEY_U16: return groove__records__values__ValueType__scalar(GROOVE_VT_U16);
    case GROOVE_INT_KEY_U32: return groove__records__values__ValueType__scalar(GROOVE_VT_U32);
    default: return groove__records__values__ValueType__scalar(GROOVE_VT_U64);
    }
}
groove_ColumnType groove__schema__PrimaryKeyType__column_type(const groove_PrimaryKeyType *t) {
    switch (t->tag) {
    case GROOVE_PK_INTEGER: return groove__schema__IntegerKeyType__column_type(t->integer);
    case GROOVE_PK_BOOL: return groove__records__values__ValueType__scalar(GROOVE_VT_BOOL);
    case GROOVE_PK_STRING: return groove__records__values__ValueType__scalar(GROOVE_VT_STRING);
    case GROOVE_PK_BYTES: return groove__records__values__ValueType__scalar(GROOVE_VT_BYTES);
    default: return groove__records__values__ValueType__scalar(GROOVE_VT_UUID);
    }
}
groove_PrimaryKeyColumn groove__schema__PrimaryKeyColumn__new(jz_str column, groove_PrimaryKeyType key_type) {
    groove_PrimaryKeyColumn c = {column, key_type};
    return c;
}
groove_PrimaryKeyColumn groove__schema__PrimaryKeyColumn__integer(jz_str column, u8 integer_type) {
    groove_PrimaryKeyType t = {GROOVE_PK_INTEGER, integer_type};
    return groove__schema__PrimaryKeyColumn__new(column, t);
}
groove_PrimaryKeyColumn groove__schema__PrimaryKeyColumn__bytes(jz_str column) {
    groove_PrimaryKeyType t = {GROOVE_PK_BYTES, 0};
    return groove__schema__PrimaryKeyColumn__new(column, t);
}
groove_PrimaryKeyColumn groove__schema__PrimaryKeyColumn__uuid(jz_str column) {
    groove_PrimaryKeyType t = {GROOVE_PK_UUID, 0};
    return groove__schema__PrimaryKeyColumn__new(column, t);
}
groove_PrimaryKey groove__schema__PrimaryKey__new(jz_str column, u8 integer_type) {
    groove_PrimaryKey pk = {{0}, true};
    JZ_VEC_PUSH(pk.columns, groove__schema__PrimaryKeyColumn__integer(column, integer_type));
    return pk;
}
groove_PrimaryKey groove__schema__PrimaryKey__composite(groove_PrimaryKeyColumnVec columns) {
    groove_PrimaryKey pk = {columns, false};
    return pk;
}
groove_PrimaryKey groove__schema__PrimaryKey__user_supplied(groove_PrimaryKey self) {
    self.generated = false;
    return self;
}
static void pk_column_drop_v(void *p) { jz_str_drop(&((groove_PrimaryKeyColumn *)p)->column); }
static void pk_column_clone_v(void *d, const void *s) {
    const groove_PrimaryKeyColumn *src = (const groove_PrimaryKeyColumn *)s;
    groove_PrimaryKeyColumn *dst = (groove_PrimaryKeyColumn *)d;
    dst->column = jz_str_clone(&src->column);
    dst->key_type = src->key_type;
}
static void primary_key_drop(groove_PrimaryKey *pk) { jz_vec_drop(&pk->columns, sizeof(groove_PrimaryKeyColumn), pk_column_drop_v); }
static bool primary_key_eq(const groove_PrimaryKey *a, const groove_PrimaryKey *b) {
    if (a->generated != b->generated || a->columns.len != b->columns.len) return false;
    for (u32 i = 0; i < a->columns.len; i++) {
        const groove_PrimaryKeyColumn *x = &a->columns.ptr[i], *y = &b->columns.ptr[i];
        if (!jz_str_eq(&x->column, &y->column) || x->key_type.tag != y->key_type.tag) return false;
        if (x->key_type.tag == GROOVE_PK_INTEGER && x->key_type.integer != y->key_type.integer) return false;
    }
    return true;
}

/* ======================================================================
 * IndexSchema / ForeignKey
 * ====================================================================== */
groove_IndexSchema groove__schema__IndexSchema__new(jz_str name, groove_StrVec columns) {
    groove_IndexSchema i = {name, columns, false};
    return i;
}
groove_IndexSchema groove__schema__IndexSchema__unique(groove_IndexSchema self) {
    self.unique = true;
    return self;
}
static void index_drop_v(void *p) {
    groove_IndexSchema *i = (groove_IndexSchema *)p;
    jz_str_drop(&i->name);
    strvec_drop(&i->columns);
}
static void index_clone_v(void *d, const void *s) {
    const groove_IndexSchema *src = (const groove_IndexSchema *)s;
    groove_IndexSchema *dst = (groove_IndexSchema *)d;
    dst->name = jz_str_clone(&src->name);
    strvec_clone(&dst->columns, &src->columns);
    dst->unique = src->unique;
}
static bool index_eq(const groove_IndexSchema *a, const groove_IndexSchema *b) {
    return jz_str_eq(&a->name, &b->name) && strvec_eq(&a->columns, &b->columns) && a->unique == b->unique;
}

groove_ForeignKey groove__schema__ForeignKey__new(jz_str name, groove_StrVec columns, jz_str referenced_table, groove_StrVec referenced_columns) {
    groove_ForeignKey f = {name, columns, referenced_table, referenced_columns};
    return f;
}
static void foreign_key_drop_v(void *p) {
    groove_ForeignKey *f = (groove_ForeignKey *)p;
    jz_str_drop(&f->name);
    strvec_drop(&f->columns);
    jz_str_drop(&f->referenced_table);
    strvec_drop(&f->referenced_columns);
}
static void foreign_key_clone_v(void *d, const void *s) {
    const groove_ForeignKey *src = (const groove_ForeignKey *)s;
    groove_ForeignKey *dst = (groove_ForeignKey *)d;
    dst->name = jz_str_clone(&src->name);
    strvec_clone(&dst->columns, &src->columns);
    dst->referenced_table = jz_str_clone(&src->referenced_table);
    strvec_clone(&dst->referenced_columns, &src->referenced_columns);
}
static bool foreign_key_eq(const groove_ForeignKey *a, const groove_ForeignKey *b) {
    return jz_str_eq(&a->name, &b->name) && strvec_eq(&a->columns, &b->columns) && jz_str_eq(&a->referenced_table, &b->referenced_table) &&
           strvec_eq(&a->referenced_columns, &b->referenced_columns);
}

/* ======================================================================
 * TableVariant / TableVariantField
 * ====================================================================== */
groove_TableVariantField groove__schema__TableVariantField__local(jz_str name, groove_ValueType value_type) {
    groove_TableVariantField f = {name, value_type, false, {0}};
    return f;
}
groove_TableVariantField groove__schema__TableVariantField__shared(jz_str name, groove_ValueType value_type, jz_str shared_column) {
    groove_TableVariantField f = {name, value_type, true, shared_column};
    return f;
}
static void variant_field_drop_v(void *p) {
    groove_TableVariantField *f = (groove_TableVariantField *)p;
    jz_str_drop(&f->name);
    vt_drop(&f->value_type);
    jz_str_drop(&f->shared_column);
}
static void variant_field_clone_v(void *d, const void *s) {
    const groove_TableVariantField *src = (const groove_TableVariantField *)s;
    groove_TableVariantField *dst = (groove_TableVariantField *)d;
    dst->name = jz_str_clone(&src->name);
    vt_clone(&dst->value_type, &src->value_type);
    dst->has_shared_column = src->has_shared_column;
    dst->shared_column = src->has_shared_column ? jz_str_clone(&src->shared_column) : (jz_str){0};
}
static bool variant_field_eq(const groove_TableVariantField *a, const groove_TableVariantField *b) {
    if (!jz_str_eq(&a->name, &b->name) || !vt_eq(&a->value_type, &b->value_type) || a->has_shared_column != b->has_shared_column) return false;
    return !a->has_shared_column || jz_str_eq(&a->shared_column, &b->shared_column);
}

groove_TableVariant groove__schema__TableVariant__new(u32 tag, groove_StrVec fields) {
    groove_TableVariant v = {tag, fields, {0}};
    return v;
}
groove_TableVariant groove__schema__TableVariant__with_payload(u32 tag, groove_TableVariantFieldVec fields) {
    groove_TableVariant v = {tag, {0}, fields};
    return v;
}
const jz_str *groove__schema__TableVariant__payload_name_for_shared(const groove_TableVariant *v, const jz_str *shared) {
    if (v->payload_fields.len == 0) {
        for (u32 i = 0; i < v->fields.len; i++)
            if (jz_str_eq(&v->fields.ptr[i], shared)) return &v->fields.ptr[i];
        return NULL;
    }
    for (u32 i = 0; i < v->payload_fields.len; i++) {
        const groove_TableVariantField *f = &v->payload_fields.ptr[i];
        if (f->has_shared_column && jz_str_eq(&f->shared_column, shared)) return &f->name;
    }
    return NULL;
}
void groove__schema__TableVariant__Clone__clone(groove_TableVariant *dst, const groove_TableVariant *src) {
    dst->tag = src->tag;
    strvec_clone(&dst->fields, &src->fields);
    jz_vec_clone(&dst->payload_fields, &src->payload_fields, sizeof(groove_TableVariantField), variant_field_clone_v);
}
static void variant_clone_v(void *d, const void *s) { groove__schema__TableVariant__Clone__clone((groove_TableVariant *)d, (const groove_TableVariant *)s); }
void groove__schema__TableVariant__drop(groove_TableVariant *v) {
    strvec_drop(&v->fields);
    jz_vec_drop(&v->payload_fields, sizeof(groove_TableVariantField), variant_field_drop_v);
}
static void variant_drop_v(void *p) { groove__schema__TableVariant__drop((groove_TableVariant *)p); }
static bool variant_eq(const groove_TableVariant *a, const groove_TableVariant *b) {
    if (a->tag != b->tag || !strvec_eq(&a->fields, &b->fields) || a->payload_fields.len != b->payload_fields.len) return false;
    for (u32 i = 0; i < a->payload_fields.len; i++)
        if (!variant_field_eq(&a->payload_fields.ptr[i], &b->payload_fields.ptr[i])) return false;
    return true;
}

/* ======================================================================
 * TableSchema
 * ====================================================================== */
static void registry_clone_v(void *d, const void *s) {
    groove__records__values__VariantRegistry__Clone__clone((groove_VariantRegistry *)d, (const groove_VariantRegistry *)s);
}
static void u64_clone_v(void *d, const void *s) { *(u64 *)d = *(const u64 *)s; }

static groove_TableSchema table_schema_new_inner(jz_str name, groove_ColumnSchemaVec columns, bool preserve_bound_registries) {
    for (u32 i = 0; i < columns.len; i++) {
        groove_ColumnSchema *column = &columns.ptr[i];
        jz_str path = jz_format("table/%.*s/column/%.*s", (int)name.len, name.ptr, (int)column->name.len, column->name.ptr);
        if (preserve_bound_registries) groove__records__values__ValueType__stamp_variant_registries(&column->column_type, path.ptr, path.len);
        else groove__records__values__ValueType__rebind_variant_registries(&column->column_type, path.ptr, path.len);
        jz_str_drop(&path);
    }
    jz_btree value_variant_registries;
    jz_btree_init(&value_variant_registries, &groove__records__values__VARIANT_REGISTRY_MAP);
    for (u32 i = 0; i < columns.len; i++) {
        groove_ColumnSchema *column = &columns.ptr[i];
        jz_btree registries;
        jz_btree_init(&registries, &groove__records__values__VARIANT_REGISTRY_MAP);
        groove__records__values__ValueType__collect_variant_registries(&column->column_type, &registries);
        if (registries.len != groove__records__values__ValueType__variant_registry_occurrence_count(&column->column_type)) {
            jz_str m = jz_format("variant registry identity collision within column %.*s", (int)column->name.len, column->name.ptr);
            assert_failed(m.ptr);
        }
        u64 id;
        groove_VariantRegistry registry;
        while (jz_btree_pop_first(&registries, &id, &registry)) {
            if (jz_btree_contains(&value_variant_registries, &id)) jz_panic("variant registry identity collision across columns");
            jz_btree_insert(&value_variant_registries, &id, &registry, NULL);
        }
        jz_btree_drop(&registries);
    }
    groove_TableSchema t;
    memset(&t, 0, sizeof(t));
    jz_str row_path = jz_format("table/%.*s/row", (int)name.len, name.ptr);
    t.variant_registry_id = groove__records__values__variant_registry_id_for_path(row_path.ptr, row_path.len);
    jz_str_drop(&row_path);
    t.value_variant_registries = value_variant_registries;
    t.name = name;
    t.columns = columns;
    t.has_primary_key = false;
    return t;
}

groove_TableSchema groove__schema__TableSchema__new(jz_str name, groove_ColumnSchemaVec columns) { return table_schema_new_inner(name, columns, false); }
groove_TableSchema groove__schema__TableSchema__new_with_bound_registries(jz_str name, groove_ColumnSchemaVec columns) {
    return table_schema_new_inner(name, columns, true);
}
groove_TableSchema groove__schema__TableSchema__with_primary_key(groove_TableSchema self, groove_PrimaryKey primary_key) {
    if (self.has_primary_key) primary_key_drop(&self.primary_key);
    self.has_primary_key = true;
    self.primary_key = primary_key;
    return self;
}
groove_TableSchema groove__schema__TableSchema__with_index(groove_TableSchema self, groove_IndexSchema index) {
    JZ_VEC_PUSH(self.indices, index);
    return self;
}
groove_TableSchema groove__schema__TableSchema__with_foreign_key(groove_TableSchema self, groove_ForeignKey foreign_key) {
    JZ_VEC_PUSH(self.foreign_keys, foreign_key);
    return self;
}
groove_TableSchema groove__schema__TableSchema__with_variant(groove_TableSchema self, u32 tag, groove_StrVec fields) {
    JZ_VEC_PUSH(self.variants, groove__schema__TableVariant__new(tag, fields));
    return self;
}
groove_TableSchema groove__schema__TableSchema__with_variant_payload(groove_TableSchema self, u32 tag, groove_TableVariantFieldVec fields) {
    for (u32 i = 0; i < fields.len; i++) {
        groove_TableVariantField *field = &fields.ptr[i];
        jz_str path = jz_format("%llu/case/%u/field/%.*s", (unsigned long long)self.variant_registry_id, (unsigned)tag, (int)field->name.len, field->name.ptr);
        if (field->has_shared_column) groove__records__values__ValueType__stamp_variant_registries(&field->value_type, path.ptr, path.len);
        else groove__records__values__ValueType__rebind_variant_registries(&field->value_type, path.ptr, path.len);
        jz_str_drop(&path);
    }
    for (u32 i = 0; i < fields.len; i++) {
        groove_TableVariantField *field = &fields.ptr[i];
        jz_btree registries;
        jz_btree_init(&registries, &groove__records__values__VARIANT_REGISTRY_MAP);
        groove__records__values__ValueType__collect_variant_registries(&field->value_type, &registries);
        if (registries.len != groove__records__values__ValueType__variant_registry_occurrence_count(&field->value_type)) {
            jz_str m = jz_format("variant registry identity collision within payload field %.*s", (int)field->name.len, field->name.ptr);
            assert_failed(m.ptr);
        }
        u64 id;
        groove_VariantRegistry registry;
        while (jz_btree_pop_first(&registries, &id, &registry)) {
            groove_VariantRegistry *existing = (groove_VariantRegistry *)jz_btree_get(&self.value_variant_registries, &id);
            if (existing) {
                if (!groove__records__values__VariantRegistry__PartialEq__eq(existing, &registry)) assert_failed("variant registry definition collision");
                groove__records__values__VariantRegistry__drop(&registry);
            } else {
                jz_btree_insert(&self.value_variant_registries, &id, &registry, NULL);
            }
        }
        jz_btree_drop(&registries);
    }
    JZ_VEC_PUSH(self.variants, groove__schema__TableVariant__with_payload(tag, fields));
    return self;
}
bool groove__schema__TableSchema__has_variants(const groove_TableSchema *t) { return t->variants.len != 0; }
const groove_TableVariant *groove__schema__TableSchema__variant(const groove_TableSchema *t, u32 tag) {
    for (u32 i = 0; i < t->variants.len; i++)
        if (t->variants.ptr[i].tag == tag) return &t->variants.ptr[i];
    return NULL;
}

bool groove__schema__TableSchema__record_schema_for_variant(const groove_TableSchema *t, u32 tag, groove_RecordDescriptor *out) {
    if (t->variants.len == 0) {
        *out = groove__schema__TableSchema__record_schema(t);
        return true;
    }
    const groove_TableVariant *variant = groove__schema__TableSchema__variant(t, tag);
    if (!variant) return false;
    groove_StrVec names = {0};
    JZ_VEC(groove_ValueType) types = {0};
    if (variant->payload_fields.len == 0) {
        for (u32 i = 0; i < variant->fields.len; i++) {
            const groove_ColumnSchema *column = NULL;
            for (u32 c = 0; c < t->columns.len; c++)
                if (jz_str_eq(&t->columns.ptr[c].name, &variant->fields.ptr[i])) {
                    column = &t->columns.ptr[c];
                    break;
                }
            if (!column) { /* collect::<Option<Vec<_>>>()? */
                strvec_drop(&names);
                jz_vec_drop(&types, sizeof(groove_ValueType), groove__records__values__ValueType__drop_v);
                return false;
            }
            JZ_VEC_PUSH(names, jz_str_clone(&column->name));
            groove_ValueType vt;
            vt_clone(&vt, &column->column_type);
            JZ_VEC_PUSH(types, vt);
        }
    } else {
        for (u32 i = 0; i < variant->payload_fields.len; i++) {
            const groove_TableVariantField *field = &variant->payload_fields.ptr[i];
            JZ_VEC_PUSH(names, jz_str_clone(&field->name));
            groove_ValueType vt;
            vt_clone(&vt, &field->value_type);
            JZ_VEC_PUSH(types, vt);
        }
    }
    *out = groove__records__RecordDescriptor__new(names.ptr, types.ptr, names.len);
    strvec_drop(&names);
    jz_vec_drop(&types, sizeof(groove_ValueType), NULL); /* moved into the descriptor */
    return true;
}

groove_RecordDescriptor groove__schema__TableSchema__record_schema(const groove_TableSchema *t) {
    groove_StrVec names = {0};
    JZ_VEC(groove_ValueType) types = {0};
    for (u32 i = 0; i < t->columns.len; i++) {
        JZ_VEC_PUSH(names, jz_str_clone(&t->columns.ptr[i].name));
        groove_ValueType vt;
        vt_clone(&vt, &t->columns.ptr[i].column_type);
        JZ_VEC_PUSH(types, vt);
    }
    groove_RecordDescriptor d = groove__records__RecordDescriptor__new(names.ptr, types.ptr, names.len);
    strvec_drop(&names);
    jz_vec_drop(&types, sizeof(groove_ValueType), NULL);
    return d;
}

void groove__schema__TableSchema__Clone__clone(groove_TableSchema *dst, const groove_TableSchema *src) {
    dst->name = jz_str_clone(&src->name);
    dst->variant_registry_id = src->variant_registry_id;
    jz_btree_clone(&dst->value_variant_registries, &src->value_variant_registries, u64_clone_v, registry_clone_v);
    jz_vec_clone(&dst->columns, &src->columns, sizeof(groove_ColumnSchema), column_clone_v);
    dst->has_primary_key = src->has_primary_key;
    if (src->has_primary_key) {
        dst->primary_key.generated = src->primary_key.generated;
        jz_vec_clone(&dst->primary_key.columns, &src->primary_key.columns, sizeof(groove_PrimaryKeyColumn), pk_column_clone_v);
    } else {
        memset(&dst->primary_key, 0, sizeof(dst->primary_key));
    }
    jz_vec_clone(&dst->indices, &src->indices, sizeof(groove_IndexSchema), index_clone_v);
    jz_vec_clone(&dst->foreign_keys, &src->foreign_keys, sizeof(groove_ForeignKey), foreign_key_clone_v);
    jz_vec_clone(&dst->variants, &src->variants, sizeof(groove_TableVariant), variant_clone_v);
}
static void table_clone_v(void *d, const void *s) { groove__schema__TableSchema__Clone__clone((groove_TableSchema *)d, (const groove_TableSchema *)s); }

void groove__schema__TableSchema__drop(groove_TableSchema *t) {
    jz_str_drop(&t->name);
    jz_btree_drop(&t->value_variant_registries);
    jz_vec_drop(&t->columns, sizeof(groove_ColumnSchema), groove__schema__ColumnSchema__drop);
    if (t->has_primary_key) primary_key_drop(&t->primary_key);
    jz_vec_drop(&t->indices, sizeof(groove_IndexSchema), index_drop_v);
    jz_vec_drop(&t->foreign_keys, sizeof(groove_ForeignKey), foreign_key_drop_v);
    jz_vec_drop(&t->variants, sizeof(groove_TableVariant), variant_drop_v);
}
void groove__schema__TableSchema__drop_v(void *t) { groove__schema__TableSchema__drop((groove_TableSchema *)t); }

static bool registry_value_eq(const void *a, const void *b) {
    return groove__records__values__VariantRegistry__PartialEq__eq((const groove_VariantRegistry *)a, (const groove_VariantRegistry *)b);
}
bool groove__schema__TableSchema__PartialEq__eq(const groove_TableSchema *a, const groove_TableSchema *b) {
    if (!jz_str_eq(&a->name, &b->name) || a->variant_registry_id != b->variant_registry_id) return false;
    if (!jz_btree_eq(&a->value_variant_registries, &b->value_variant_registries, registry_value_eq)) return false;
    if (a->columns.len != b->columns.len) return false;
    for (u32 i = 0; i < a->columns.len; i++)
        if (!groove__schema__ColumnSchema__PartialEq__eq(&a->columns.ptr[i], &b->columns.ptr[i])) return false;
    if (a->has_primary_key != b->has_primary_key) return false;
    if (a->has_primary_key && !primary_key_eq(&a->primary_key, &b->primary_key)) return false;
    if (a->indices.len != b->indices.len || a->foreign_keys.len != b->foreign_keys.len || a->variants.len != b->variants.len) return false;
    for (u32 i = 0; i < a->indices.len; i++)
        if (!index_eq(&a->indices.ptr[i], &b->indices.ptr[i])) return false;
    for (u32 i = 0; i < a->foreign_keys.len; i++)
        if (!foreign_key_eq(&a->foreign_keys.ptr[i], &b->foreign_keys.ptr[i])) return false;
    for (u32 i = 0; i < a->variants.len; i++)
        if (!variant_eq(&a->variants.ptr[i], &b->variants.ptr[i])) return false;
    return true;
}

/* ======================================================================
 * DirectRecordStoreSchema
 * ====================================================================== */
static groove_KeyFieldVec descriptor_fields(groove_RecordDescriptor descriptor) {
    groove_KeyFieldVec out = {0};
    u32 n = 0;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(descriptor, &n);
    for (u32 i = 0; i < n; i++) {
        JZ_ASSERT(fields[i].has_name, "direct store fields are named");
        groove_KeyField f;
        f.name = jz_str_clone(&fields[i].name);
        vt_clone(&f.value_type, &fields[i].value_type);
        JZ_VEC_PUSH(out, f);
    }
    return out;
}
static void key_field_drop_v(void *p) {
    groove_KeyField *f = (groove_KeyField *)p;
    jz_str_drop(&f->name);
    vt_drop(&f->value_type);
}
static void key_field_clone_v(void *d, const void *s) {
    const groove_KeyField *src = (const groove_KeyField *)s;
    groove_KeyField *dst = (groove_KeyField *)d;
    dst->name = jz_str_clone(&src->name);
    vt_clone(&dst->value_type, &src->value_type);
}
static bool key_fields_eq(const groove_KeyFieldVec *a, const groove_KeyFieldVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!jz_str_eq(&a->ptr[i].name, &b->ptr[i].name) || !vt_eq(&a->ptr[i].value_type, &b->ptr[i].value_type)) return false;
    return true;
}

groove_DirectRecordStoreSchema groove__schema__DirectRecordStoreSchema__new(jz_str name, groove_RecordDescriptor key, groove_RecordDescriptor value) {
    groove_DirectRecordStoreSchema s = {name, descriptor_fields(key), descriptor_fields(value)};
    return s;
}
static groove_RecordDescriptor descriptor_from_key_fields(const groove_KeyFieldVec *fields) {
    groove_StrVec names = {0};
    JZ_VEC(groove_ValueType) types = {0};
    for (u32 i = 0; i < fields->len; i++) {
        JZ_VEC_PUSH(names, jz_str_clone(&fields->ptr[i].name));
        groove_ValueType vt;
        vt_clone(&vt, &fields->ptr[i].value_type);
        JZ_VEC_PUSH(types, vt);
    }
    groove_RecordDescriptor d = groove__records__RecordDescriptor__new(names.ptr, types.ptr, names.len);
    strvec_drop(&names);
    jz_vec_drop(&types, sizeof(groove_ValueType), NULL);
    return d;
}
groove_RecordDescriptor groove__schema__DirectRecordStoreSchema__key_descriptor(const groove_DirectRecordStoreSchema *s) { return descriptor_from_key_fields(&s->key); }
groove_RecordDescriptor groove__schema__DirectRecordStoreSchema__value_descriptor(const groove_DirectRecordStoreSchema *s) { return descriptor_from_key_fields(&s->value); }
void groove__schema__DirectRecordStoreSchema__Clone__clone(void *d, const void *s) {
    const groove_DirectRecordStoreSchema *src = (const groove_DirectRecordStoreSchema *)s;
    groove_DirectRecordStoreSchema *dst = (groove_DirectRecordStoreSchema *)d;
    dst->name = jz_str_clone(&src->name);
    jz_vec_clone(&dst->key, &src->key, sizeof(groove_KeyField), key_field_clone_v);
    jz_vec_clone(&dst->value, &src->value, sizeof(groove_KeyField), key_field_clone_v);
}
void groove__schema__DirectRecordStoreSchema__drop(void *p) {
    groove_DirectRecordStoreSchema *s = (groove_DirectRecordStoreSchema *)p;
    jz_str_drop(&s->name);
    jz_vec_drop(&s->key, sizeof(groove_KeyField), key_field_drop_v);
    jz_vec_drop(&s->value, sizeof(groove_KeyField), key_field_drop_v);
}

/* ======================================================================
 * DatabaseSchema
 * ====================================================================== */
groove_DatabaseSchema groove__schema__DatabaseSchema__new(groove_TableSchemaVec tables) {
    groove_DatabaseSchema s = {tables, {0}};
    return s;
}
groove_DatabaseSchema groove__schema__DatabaseSchema__with_direct_record_store(groove_DatabaseSchema self, groove_DirectRecordStoreSchema store) {
    JZ_VEC_PUSH(self.direct_record_stores, store);
    return self;
}
const groove_TableSchema *groove__schema__DatabaseSchema__table(const groove_DatabaseSchema *s, const char *name, u32 len) {
    for (u32 i = 0; i < s->tables.len; i++)
        if (str_eq_slice(&s->tables.ptr[i].name, name, len)) return &s->tables.ptr[i];
    return NULL;
}
const groove_DirectRecordStoreSchema *groove__schema__DatabaseSchema__direct_record_store(const groove_DatabaseSchema *s, const char *name, u32 len) {
    for (u32 i = 0; i < s->direct_record_stores.len; i++)
        if (str_eq_slice(&s->direct_record_stores.ptr[i].name, name, len)) return &s->direct_record_stores.ptr[i];
    return NULL;
}
groove_SliceVec groove__schema__DatabaseSchema__column_families(const groove_DatabaseSchema *s) {
    bool has_indices = false;
    for (u32 i = 0; i < s->tables.len && !has_indices; i++) has_indices = s->tables.ptr[i].indices.len != 0;
    groove_SliceVec out = {0};
    for (u32 i = 0; i < s->tables.len; i++) JZ_VEC_PUSH(out, jz_str_slice(&s->tables.ptr[i].name));
    for (u32 i = 0; i < s->direct_record_stores.len; i++) JZ_VEC_PUSH(out, jz_str_slice(&s->direct_record_stores.ptr[i].name));
    if (has_indices) JZ_VEC_PUSH(out, JZ_LIT("indices"));
    JZ_VEC_PUSH(out, jz_slice_of(groove__db__LARGE_VALUE_METADATA_CF, (u32)strlen(groove__db__LARGE_VALUE_METADATA_CF)));
    return out;
}
void groove__schema__DatabaseSchema__Clone__clone(groove_DatabaseSchema *dst, const groove_DatabaseSchema *src) {
    jz_vec_clone(&dst->tables, &src->tables, sizeof(groove_TableSchema), table_clone_v);
    jz_vec_clone(&dst->direct_record_stores, &src->direct_record_stores, sizeof(groove_DirectRecordStoreSchema), groove__schema__DirectRecordStoreSchema__Clone__clone);
}
void groove__schema__DatabaseSchema__drop(groove_DatabaseSchema *s) {
    jz_vec_drop(&s->tables, sizeof(groove_TableSchema), groove__schema__TableSchema__drop_v);
    jz_vec_drop(&s->direct_record_stores, sizeof(groove_DirectRecordStoreSchema), groove__schema__DirectRecordStoreSchema__drop);
}
bool groove__schema__DatabaseSchema__PartialEq__eq(const groove_DatabaseSchema *a, const groove_DatabaseSchema *b) {
    if (a->tables.len != b->tables.len || a->direct_record_stores.len != b->direct_record_stores.len) return false;
    for (u32 i = 0; i < a->tables.len; i++)
        if (!groove__schema__TableSchema__PartialEq__eq(&a->tables.ptr[i], &b->tables.ptr[i])) return false;
    for (u32 i = 0; i < a->direct_record_stores.len; i++) {
        const groove_DirectRecordStoreSchema *x = &a->direct_record_stores.ptr[i], *y = &b->direct_record_stores.ptr[i];
        if (!jz_str_eq(&x->name, &y->name) || !key_fields_eq(&x->key, &y->key) || !key_fields_eq(&x->value, &y->value)) return false;
    }
    return true;
}
