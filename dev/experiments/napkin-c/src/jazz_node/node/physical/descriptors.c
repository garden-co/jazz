/* physical/descriptors.c: jazz_node::node::physical (physical/descriptors.rs).
 * Physical descriptors, enum value remapping and physical table lowering. */
#include "../physical.h"
#include "../codec.h"

static void jzn_str_drop_v(void *p) { jz_str_drop((jz_str *)p); }
static void jzn_str_clone_v(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone((const jz_str *)src); }

static const groove_ColumnSchema *jzn_find_column(const groove_TableSchema *t, const jz_str *name) {
    for (u32 i = 0; i < t->columns.len; i++)
        if (jz_str_eq(&t->columns.ptr[i].name, name)) return &t->columns.ptr[i];
    return NULL;
}

jz_err jazz_node__node__physical__widened_projection_descriptor(groove_RecordDescriptor logical, const jz_str *physical_names, u32 n,
                                                               const groove_TableSchema *physical_table, groove_RecordDescriptor *out) {
    const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(logical);
    if (fields->len != n) return JZN_ISV("physical projection descriptor width mismatch");
    groove_NamedValueTypeVec widened = {0};
    for (u32 i = 0; i < n; i++) {
        const groove_ColumnSchema *physical = jzn_find_column(physical_table, &physical_names[i]);
        if (!physical) {
            jz_vec_drop(&widened, sizeof(groove_NamedValueType), groove_NamedValueType_drop);
            return JZN_ISV("physical projection column missing");
        }
        if (!fields->ptr[i].has_name) {
            jz_vec_drop(&widened, sizeof(groove_NamedValueType), groove_NamedValueType_drop);
            return JZN_ISV("physical projection logical field unnamed");
        }
        groove_NamedValueType entry;
        entry.name = jz_str_clone(&fields->ptr[i].name);
        entry.value_type = jazz_node__node__physical__widen_projection_value_type(&fields->ptr[i].value_type, &physical->column_type);
        JZ_VEC_PUSH(widened, entry);
    }
    *out = groove__records__RecordDescriptor__new(widened);
    return JZ_OK;
}

/* Write-side counterpart: physical field names and physical value types. */
jz_err jazz_node__node__physical__physical_write_descriptor(groove_RecordDescriptor logical, const jz_str *physical_names, u32 n,
                                                           const groove_TableSchema *physical_table, groove_RecordDescriptor *out) {
    const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(logical);
    if (fields->len != n) return JZN_ISV("physical write descriptor width mismatch");
    groove_NamedValueTypeVec physical_fields = {0};
    for (u32 i = 0; i < n; i++) {
        const groove_ColumnSchema *physical = jzn_find_column(physical_table, &physical_names[i]);
        if (!physical) {
            jz_vec_drop(&physical_fields, sizeof(groove_NamedValueType), groove_NamedValueType_drop);
            return JZN_ISV("physical write column missing");
        }
        groove_NamedValueType entry;
        entry.name = jz_str_clone(&physical_names[i]);
        groove_ValueType_clone(&entry.value_type, &physical->column_type);
        JZ_VEC_PUSH(physical_fields, entry);
    }
    *out = groove__records__RecordDescriptor__new(physical_fields);
    return JZ_OK;
}

static jz_err jzn_remap_values(groove_ValueVec *values, const groove_DescriptorFieldVec *authored, const groove_DescriptorFieldVec *physical,
                               const jazz_node_EnumOccurrenceRemaps *remaps, const jz_str *child_root, bool record_infix,
                               const char *unnamed, groove_ValueVec *out) {
    groove_ValueVec result = {0};
    u32 n = values->len;
    if (authored->len < n) n = authored->len;
    if (physical->len < n) n = physical->len;
    jz_err err = JZ_OK;
    u32 i = 0;
    for (; i < n; i++) {
        if (!authored->ptr[i].has_name) { err = JZN_ISV(unnamed); break; }
        jz_str path = record_infix ? jz_format("%.*s/record/%.*s", JZN_S(*child_root), JZN_S(authored->ptr[i].name))
                                   : jz_format("%.*s/%.*s", JZN_S(*child_root), JZN_S(authored->ptr[i].name));
        groove_Value v;
        err = jazz_node__node__physical__remap_nested_enum_value(values->ptr[i], &authored->ptr[i].value_type,
                                                                 &physical->ptr[i].value_type, remaps, &path, &v);
        jz_str_drop(&path);
        if (JZ_IS_ERR(err)) { i++; break; }
        JZ_VEC_PUSH(result, v);
    }
    /* values that were not consumed are dropped */
    for (; i < values->len; i++) groove_Value_drop(&values->ptr[i]);
    jz_free(values->ptr);
    if (JZ_IS_ERR(err)) {
        jz_vec_drop(&result, sizeof(groove_Value), groove_Value_drop);
        return err;
    }
    *out = result;
    return JZ_OK;
}

/* Re-tag every enum occurrence of an authored value into its physical registry
 * (or the reverse): consumes `value`. */
jz_err jazz_node__node__physical__remap_nested_enum_value(groove_Value value, const groove_ValueType *authored,
                                                         const groove_ValueType *physical, const jazz_node_EnumOccurrenceRemaps *remaps,
                                                         const jz_str *path, groove_Value *out) {
    if (value.tag == GROOVE_V_ENUM_TAG && authored->tag == GROOVE_VT_ENUM_TAG && physical->tag == GROOVE_VT_ENUM_TAG) {
        const groove_OptU8Vec *tags = jz_btree_get(&remaps->scalar, path);
        if (!tags || value.v.enum_tag >= tags->len || tags->ptr[value.v.enum_tag] == GROOVE_OPT_U8_NONE)
            return JZN_ISV("nested scalar enum tag absent from physical mapping");
        out->tag = GROOVE_V_ENUM_TAG;
        out->v.enum_tag = (u8)tags->ptr[value.v.enum_tag];
        return JZ_OK;
    }
    if (value.tag == GROOVE_V_NULLABLE && authored->tag == GROOVE_VT_NULLABLE && physical->tag == GROOVE_VT_NULLABLE) {
        if (!value.v.nullable) { *out = groove_value_null(); return JZ_OK; }
        groove_Value inner = *value.v.nullable;
        jz_free(value.v.nullable);
        jz_str child = jz_format("%.*s/nullable", JZN_S(*path));
        groove_Value mapped;
        jz_err e = jazz_node__node__physical__remap_nested_enum_value(inner, authored->v.inner, physical->v.inner, remaps, &child, &mapped);
        jz_str_drop(&child);
        JZ_TRY(e);
        *out = groove_value_some(mapped);
        return JZ_OK;
    }
    if (value.tag == GROOVE_V_ARRAY && authored->tag == GROOVE_VT_ARRAY && physical->tag == GROOVE_VT_ARRAY) {
        jz_str child = jz_format("%.*s/array", JZN_S(*path));
        groove_ValueVec result = {0};
        jz_err err = JZ_OK;
        u32 i = 0;
        for (; i < value.v.vals.len; i++) {
            groove_Value mapped;
            err = jazz_node__node__physical__remap_nested_enum_value(value.v.vals.ptr[i], authored->v.inner, physical->v.inner,
                                                                     remaps, &child, &mapped);
            if (JZ_IS_ERR(err)) { i++; break; }
            JZ_VEC_PUSH(result, mapped);
        }
        for (; i < value.v.vals.len; i++) groove_Value_drop(&value.v.vals.ptr[i]);
        jz_free(value.v.vals.ptr);
        jz_str_drop(&child);
        if (JZ_IS_ERR(err)) { jz_vec_drop(&result, sizeof(groove_Value), groove_Value_drop); return err; }
        out->tag = GROOVE_V_ARRAY;
        out->v.vals = result;
        return JZ_OK;
    }
    if (value.tag == GROOVE_V_TUPLE && authored->tag == GROOVE_VT_TUPLE && physical->tag == GROOVE_VT_TUPLE &&
        authored->v.tuple.len == physical->v.tuple.len && value.v.vals.len == authored->v.tuple.len) {
        groove_ValueVec result = {0};
        jz_err err = JZ_OK;
        u32 i = 0;
        for (; i < value.v.vals.len; i++) {
            jz_str child = jz_format("%.*s/tuple/%u", JZN_S(*path), i);
            groove_Value mapped;
            err = jazz_node__node__physical__remap_nested_enum_value(value.v.vals.ptr[i], &authored->v.tuple.ptr[i],
                                                                     &physical->v.tuple.ptr[i], remaps, &child, &mapped);
            jz_str_drop(&child);
            if (JZ_IS_ERR(err)) { i++; break; }
            JZ_VEC_PUSH(result, mapped);
        }
        for (; i < value.v.vals.len; i++) groove_Value_drop(&value.v.vals.ptr[i]);
        jz_free(value.v.vals.ptr);
        if (JZ_IS_ERR(err)) { jz_vec_drop(&result, sizeof(groove_Value), groove_Value_drop); return err; }
        out->tag = GROOVE_V_TUPLE;
        out->v.vals = result;
        return JZ_OK;
    }
    if (value.tag == GROOVE_V_RECORD && authored->tag == GROOVE_VT_RECORD && physical->tag == GROOVE_VT_RECORD) {
        const groove_DescriptorFieldVec *af = groove__records__RecordDescriptor__fields(*authored->v.record);
        const groove_DescriptorFieldVec *pf = groove__records__RecordDescriptor__fields(*physical->v.record);
        if (af->len == pf->len) {
            groove_ValueVec values;
            jz_err e = groove__records__OwnedRecord__to_values(&value.v.record, &values);
            groove_OwnedRecord_drop(&value.v.record);
            JZ_TRY(e);
            groove_ValueVec mapped;
            JZ_TRY(jzn_remap_values(&values, af, pf, remaps, path, true, "nested record enum field unnamed", &mapped));
            jz_bytes raw;
            e = groove__records__RecordDescriptor__create(*physical->v.record, mapped.ptr, mapped.len, &raw);
            jz_vec_drop(&mapped, sizeof(groove_Value), groove_Value_drop);
            if (JZ_IS_ERR(e)) return jz_err_context(e, "record");
            out->tag = GROOVE_V_RECORD;
            out->v.record.raw = raw;
            out->v.record.descriptor = *physical->v.record;
            return JZ_OK;
        }
    }
    if (value.tag == GROOVE_V_ENUM && authored->tag == GROOVE_VT_ENUM && physical->tag == GROOVE_VT_ENUM) {
        u32 authored_tag = value.v.en.tag;
        const groove_OptU32Vec *tags = jz_btree_get(&remaps->payload, path);
        if (!tags || authored_tag >= tags->len || tags->ptr[authored_tag] < 0) {
            groove_Value_drop(&value);
            return JZN_ISV("nested payload enum tag absent from physical mapping");
        }
        u32 physical_tag = (u32)tags->ptr[authored_tag];
        const groove_EnumCase *authored_case, *physical_case;
        jz_err e = groove__records__values__EnumSchema__case(authored->v.enm, authored_tag, &authored_case);
        if (!JZ_IS_ERR(e)) e = groove__records__values__EnumSchema__case(physical->v.enm, physical_tag, &physical_case);
        if (JZ_IS_ERR(e)) { groove_Value_drop(&value); return e; }
        const groove_DescriptorFieldVec *af = groove__records__RecordDescriptor__fields(authored_case->payload);
        const groove_DescriptorFieldVec *pf = groove__records__RecordDescriptor__fields(physical_case->payload);
        if (af->len != pf->len) { groove_Value_drop(&value); return JZN_ISV("nested payload enum payload width changed"); }
        const groove_OptStrVec *children = jz_btree_get(&remaps->payload_children, path);
        const jz_str *semantic_child_root = NULL;
        if (children && authored_tag < children->len && children->ptr[authored_tag].ptr) semantic_child_root = &children->ptr[authored_tag];
        jz_str child_root = semantic_child_root ? jz_str_clone(semantic_child_root)
                                                : jz_format("%.*s/case/%u", JZN_S(*path), authored_tag);
        groove_ValueVec values;
        e = groove__records__OwnedRecord__to_values(&value.v.en.record, &values);
        groove_OwnedRecord_drop(&value.v.en.record);
        if (JZ_IS_ERR(e)) { jz_str_drop(&child_root); return e; }
        groove_ValueVec mapped;
        e = jzn_remap_values(&values, af, pf, remaps, &child_root, semantic_child_root != NULL, "nested payload enum field unnamed", &mapped);
        jz_str_drop(&child_root);
        JZ_TRY(e);
        jz_bytes raw;
        e = groove__records__RecordDescriptor__create(physical_case->payload, mapped.ptr, mapped.len, &raw);
        jz_vec_drop(&mapped, sizeof(groove_Value), groove_Value_drop);
        JZ_TRY(e);
        out->tag = GROOVE_V_ENUM;
        out->v.en.tag = physical_tag;
        out->v.en.record.raw = raw;
        out->v.en.record.descriptor = physical_case->payload;
        return JZ_OK;
    }
    if (groove_ValueType_eq(authored, physical)) {
        *out = value;
        return JZ_OK;
    }
    groove_Value_drop(&value);
    return JZN_ISV("nested enum remap descriptor mismatch");
}

groove_ValueType jazz_node__node__physical__widen_projection_value_type(const groove_ValueType *logical, const groove_ValueType *physical) {
    groove_ValueType out;
    /* Large JSON's durable cell is the internal stored-scalar enum. */
    if (groove__records__values__ValueType__is_internal_storage_type(physical)) {
        groove_ValueType_clone(&out, physical);
        return out;
    }
    /* Projection exposes the target schema's declaration-local enum descriptor. */
    if ((logical->tag == GROOVE_VT_ENUM_TAG && physical->tag == GROOVE_VT_ENUM_TAG) ||
        (logical->tag == GROOVE_VT_ENUM && physical->tag == GROOVE_VT_ENUM)) {
        groove_ValueType_clone(&out, logical);
        return out;
    }
    if (physical->tag == GROOVE_VT_NULLABLE && logical->tag != GROOVE_VT_NULLABLE)
        return jazz_node__node__physical__widen_projection_value_type(logical, physical->v.inner);
    if ((logical->tag == GROOVE_VT_NULLABLE && physical->tag == GROOVE_VT_NULLABLE) ||
        (logical->tag == GROOVE_VT_ARRAY && physical->tag == GROOVE_VT_ARRAY))
        return groove_vt_box(logical->tag, jazz_node__node__physical__widen_projection_value_type(logical->v.inner, physical->v.inner));
    if (logical->tag == GROOVE_VT_TUPLE && physical->tag == GROOVE_VT_TUPLE && logical->v.tuple.len == physical->v.tuple.len) {
        groove_ValueTypeVec values = {0};
        for (u32 i = 0; i < logical->v.tuple.len; i++) {
            groove_ValueType w = jazz_node__node__physical__widen_projection_value_type(&logical->v.tuple.ptr[i], &physical->v.tuple.ptr[i]);
            JZ_VEC_PUSH(values, w);
        }
        out.tag = GROOVE_VT_TUPLE;
        out.v.tuple = values;
        return out;
    }
    groove_ValueType_clone(&out, logical);
    return out;
}

/* ------------------------------------------------------------------ */
/* physical_version_storage_tables                                      */
/* ------------------------------------------------------------------ */
typedef struct jzn_lineage_entry {
    jazz_types_SchemaVersionId schema_version;
    const jazz_model_TableSchema *table;
    const jazz_node_TablePhysicalMapping *mapping;
    const jazz_protocol_PhysicalTableIdentity *identity;
} jzn_lineage_entry;
typedef JZ_VEC(jzn_lineage_entry) jzn_lineage_vec;
static void jzn_lineage_vec_drop(void *v) { jz_vec_drop(v, sizeof(jzn_lineage_entry), NULL); }
static const jz_btree_desc jzn_desc_u64_lineage = { sizeof(u64), sizeof(jzn_lineage_vec), jz_cmp_u64, NULL, jzn_lineage_vec_drop };

typedef struct jzn_col_path { u64 column; jz_str path; } jzn_col_path;
static int jzn_col_path_cmp(const void *a, const void *b, void *ctx) {
    const jzn_col_path *l = a, *r = b;
    if (l->column != r->column) return l->column < r->column ? -1 : 1;
    return jz_str_cmp(&l->path, &r->path);
}
static void jzn_col_path_drop(void *p) { jz_str_drop(&((jzn_col_path *)p)->path); }
static void jzn_btree_drop_v(void *p) { jz_btree_drop((jz_btree *)p); }
static const jz_btree_desc jzn_desc_colpath_set = { sizeof(jzn_col_path), sizeof(jz_btree), jzn_col_path_cmp, jzn_col_path_drop, jzn_btree_drop_v };
static const jz_btree_desc jzn_desc_colpath_vec = { sizeof(jzn_col_path), sizeof(jz_vec), jzn_col_path_cmp, jzn_col_path_drop, jzn_pod_vec_drop };
static const jz_btree_desc jzn_desc_u64_set_map = { sizeof(u64), sizeof(jz_btree), jz_cmp_u64, NULL, jzn_btree_drop_v };

typedef struct jzn_col_path_case { u64 column; jz_str path; jazz_node_GlobalEnumCaseId identity; } jzn_col_path_case;
static int jzn_col_path_case_cmp(const void *a, const void *b, void *ctx) {
    const jzn_col_path_case *l = a, *r = b;
    if (l->column != r->column) return l->column < r->column ? -1 : 1;
    int c = jz_str_cmp(&l->path, &r->path);
    if (c) return c;
    return jazz_node_GlobalEnumCaseId_cmp(&l->identity, &r->identity, ctx);
}
static void jzn_col_path_case_drop(void *p) { jz_str_drop(&((jzn_col_path_case *)p)->path); }
static const jz_btree_desc jzn_desc_colpathcase_desc = { sizeof(jzn_col_path_case), sizeof(groove_RecordDescriptor), jzn_col_path_case_cmp, jzn_col_path_case_drop, NULL };

typedef struct jzn_col_case { u64 column; jazz_node_GlobalEnumCaseId identity; } jzn_col_case;
static int jzn_col_case_cmp(const void *a, const void *b, void *ctx) {
    const jzn_col_case *l = a, *r = b;
    if (l->column != r->column) return l->column < r->column ? -1 : 1;
    return jazz_node_GlobalEnumCaseId_cmp(&l->identity, &r->identity, ctx);
}
static const jz_btree_desc jzn_desc_colcase_desc = { sizeof(jzn_col_case), sizeof(groove_RecordDescriptor), jzn_col_case_cmp, NULL, NULL };
static void jzn_vt_drop_v(void *p) { groove_ValueType_drop(p); }
static const jz_btree_desc jzn_desc_u64_valuetype = { sizeof(u64), sizeof(groove_ValueType), jz_cmp_u64, NULL, jzn_vt_drop_v };
typedef struct jzn_ids_key { JZ_VEC(u64) ids; } jzn_ids_key;
static int jzn_ids_key_cmp(const void *a, const void *b, void *ctx) {
    const jzn_ids_key *l = a, *r = b;
    u32 n = l->ids.len < r->ids.len ? l->ids.len : r->ids.len;
    for (u32 i = 0; i < n; i++)
        if (l->ids.ptr[i] != r->ids.ptr[i]) return l->ids.ptr[i] < r->ids.ptr[i] ? -1 : 1;
    return (l->ids.len > r->ids.len) - (l->ids.len < r->ids.len);
}
static void jzn_ids_key_drop(void *p) { jz_vec_drop(p, sizeof(u64), NULL); }
static const jz_btree_desc jzn_desc_ids_set = { sizeof(jzn_ids_key), 0, jzn_ids_key_cmp, jzn_ids_key_drop, NULL };

typedef struct jzn_case_sort_ctx { const jz_btree *aliases; bool scalar; } jzn_case_sort_ctx;
static int jzn_case_sort(const void *a, const void *b, void *ctx) {
    jzn_case_sort_ctx *c = ctx;
    return c->scalar ? jazz_node__node__physical__compare_scalar_enum_cases(c->aliases, a, b)
                     : jazz_node__node__physical__compare_global_enum_cases(c->aliases, a, b);
}
/* set (BTreeSet<case>) -> sorted Vec<case> */
static void jzn_set_to_sorted_vec(const jz_btree *set, usize elem, const jz_btree *aliases, bool scalar, jz_vec *out) {
    jz_vec v = {0};
    jz_vec_reserve(&v, set->len, elem);
    void *k, *unused;
    JZ_BTREE_FOR(set, k, unused) jz_vec_push(&v, k, elem);
    jzn_case_sort_ctx ctx = { aliases, scalar };
    jz_sort(v.ptr, v.len, elem, jzn_case_sort, &ctx);
    *out = v;
}

static const jazz_protocol_PhysicalColumnIdentity *jzn_identity_column(const jazz_protocol_PhysicalTableIdentity *t, const jz_str *name) {
    const jazz_protocol_PhysicalColumnIdentity *c = jz_btree_get(&t->columns, name);
    if (!c) jz_panic("table_identity.columns[&column.name]");
    return c;
}
static const jz_vec *jzn_root_variants(const jazz_protocol_PhysicalColumnIdentity *c) {
    jz_str root = jz_str_from("root");
    const jz_vec *v = jz_btree_get(&c->enum_variants, &root);
    jz_str_drop(&root);
    if (!v) jz_panic("enum_variants[\"root\"]");
    return v;
}

/* filter (column, path) -> cases into path -> cases for one column (cloned) */
static void jzn_filter_registries(const jz_btree *src, u64 column_id, usize elem, jz_btree *out) {
    jz_btree_init(out, &jzn_desc_str_podvec);
    jzn_col_path *k;
    jz_vec *cases;
    JZ_BTREE_FOR(src, k, cases) {
        if (k->column != column_id) continue;
        jz_str path = jz_str_clone(&k->path);
        jz_vec copy = {0};
        jz_vec_clone(&copy, cases, elem, NULL);
        jz_btree_insert(out, &path, &copy, NULL);
    }
}
static void jzn_filter_layouts(const jz_btree *src, u64 column_id, jz_btree *out) {
    jz_btree_init(out, &jzn_desc_pathcase_desc);
    jzn_col_path_case *k;
    groove_RecordDescriptor *layout;
    JZ_BTREE_FOR(src, k, layout) {
        if (k->column != column_id) continue;
        jazz_node_PathCaseKey key = { jz_str_clone(&k->path), k->identity };
        jz_btree_insert(out, &key, layout, NULL);
    }
}
static bool jzn_any_column(const jz_btree *src, u64 column_id) {
    jzn_col_path *k;
    void *v;
    JZ_BTREE_FOR(src, k, v) if (k->column == column_id) return true;
    return false;
}
static jz_str jzn_registry_path(u64 column_id) { return jz_format("physical-column/%llu", (unsigned long long)column_id); }

static jz_err jzn_payload_storage_type(u64 column_id, const jz_btree *payload_enum_registries, const jz_btree *payload_enum_layouts,
                                       const jz_btree *nested_scalar, const jz_btree *nested_payload, const jz_btree *nested_layouts,
                                       groove_ValueType *out) {
    const jazz_node_GlobalEnumCaseIdVec *cases = jz_btree_get(payload_enum_registries, &column_id);
    if (!cases) return JZN_ISV("physical payload enum registry missing");
    groove_EnumCaseVec lowered = {0};
    jz_err err = JZ_OK;
    for (u32 i = 0; i < cases->len; i++) {
        const jazz_node_GlobalEnumCaseId *identity = &cases->ptr[i];
        jzn_col_case key = { column_id, *identity };
        const groove_RecordDescriptor *payload = jz_btree_get(payload_enum_layouts, &key);
        if (!payload) { err = JZN_ISV("physical payload enum layout missing"); break; }
        jz_btree scalar_registries, payload_registries, payload_layouts;
        jzn_filter_registries(nested_scalar, column_id, sizeof(jazz_node_GlobalScalarEnumCaseId), &scalar_registries);
        jzn_filter_registries(nested_payload, column_id, sizeof(jazz_node_GlobalEnumCaseId), &payload_registries);
        jzn_filter_layouts(nested_layouts, column_id, &payload_layouts);
        groove_ValueType record = groove_vt_record(*payload);
        jz_str root = jz_str_from("root");
        jz_str path = jazz_node__node__global_case_path(&root, identity);
        groove_ValueType lowered_record;
        err = jazz_node__node__physical__physical_nested_enum_value_type(&record, &path, &scalar_registries, &payload_registries,
                                                                        &payload_layouts, column_id, &lowered_record);
        jz_str_drop(&root);
        jz_str_drop(&path);
        groove_ValueType_drop(&record);
        jz_btree_drop(&scalar_registries);
        jz_btree_drop(&payload_registries);
        jz_btree_drop(&payload_layouts);
        if (JZ_IS_ERR(err)) break;
        if (lowered_record.tag != GROOVE_VT_RECORD) jz_panic("record lowering preserves payload shape");
        groove_EnumCase c;
        c.name = jazz_node__node__physical__physical_enum_case_name(identity);
        c.payload = *lowered_record.v.record;
        jz_free(lowered_record.v.record);
        JZ_VEC_PUSH(lowered, c);
    }
    if (JZ_IS_ERR(err)) {
        jz_vec_drop(&lowered, sizeof(groove_EnumCase), groove_EnumCase_drop);
        return err;
    }
    groove_EnumSchema *schema = JZ_NEW(groove_EnumSchema);
    jz_err e = groove__records__values__EnumSchema__new(jz_format("physical-column-%llu", (unsigned long long)column_id), lowered, schema);
    if (JZ_IS_ERR(e)) {
        jz_err_drop(&e);
        jz_free(schema);
        return JZN_ISV("invalid physical payload enum registry");
    }
    jz_str rp = jzn_registry_path(column_id);
    groove__records__values__EnumSchema__with_registry_id(schema, groove__records__values__variant_registry_id_for_path(&rp));
    jz_str_drop(&rp);
    groove_ValueType t;
    t.tag = GROOVE_VT_ENUM;
    t.v.enm = schema;
    *out = groove__records__values__ValueType__nullable(t);
    return JZ_OK;
}

static void jzn_push_physical_user_columns(groove_ColumnSchemaVec *columns, const jz_btree *physical_columns) {
    u64 *column_id;
    groove_ValueType *column_type;
    JZ_BTREE_FOR(physical_columns, column_id, column_type) {
        groove_ColumnSchema c;
        c.name = jazz_node__node__physical__physical_user_column_field(*column_id);
        groove_ValueType_clone(&c.column_type, column_type);
        JZ_VEC_PUSH(*columns, c);
    }
}
static void jzn_push_columns_range(groove_ColumnSchemaVec *dst, const groove_ColumnSchemaVec *src, u32 from, u32 to) {
    for (u32 i = from; i < to && i < src->len; i++) {
        groove_ColumnSchema c;
        groove_ColumnSchema_clone(&c, &src->ptr[i]);
        JZ_VEC_PUSH(*dst, c);
    }
}
static groove_IndexSchema jzn_branch_index(jz_str name, u64 column_id) {
    jz_str cols[2] = { jz_str_from("branch_key"), jazz_node__node__physical__physical_user_column_field(column_id) };
    return groove__schema__IndexSchema__new(name, cols, 2);
}
static groove_IndexSchema jzn_composite_index(const jzn_ids_key *ids) {
    jzn_StrVec cols = {0};
    jz_str bk = jz_str_from("branch_key");
    JZ_VEC_PUSH(cols, bk);
    for (u32 i = 0; i < ids->ids.len; i++) {
        jz_str f = jazz_node__node__physical__physical_user_column_field(ids->ids.ptr[i]);
        JZ_VEC_PUSH(cols, f);
    }
    groove_IndexSchema index = groove__schema__IndexSchema__new(
        jazz_node__node__physical__physical_current_composite_index_name(ids->ids.ptr, ids->ids.len), cols.ptr, cols.len);
    jz_free(cols.ptr);
    return index;
}

jz_err jazz_node__node__physical__variant_payload_fields_for_names(const groove_TableSchema *table, const jz_str *names, u32 n,
                                                                  groove_TableVariantFieldVec *out);

static jz_err jzn_build_lineage_tables(u64 table_id, const jzn_lineage_vec *variants, const jz_btree *aliases, groove_TableSchemaVec *tables);

jz_err jazz_node__node__physical__physical_version_storage_tables(const jz_btree *catalogue_schemas, const jz_btree *schema_version_aliases,
                                                                 const jz_btree *physical_mappings, groove_TableSchemaVec *out) {
    jz_btree lineages; /* PhysicalTableId -> Vec<(schema, table, mapping, identity)> */
    jz_btree_init(&lineages, &jzn_desc_u64_lineage);
    jz_err err = JZ_OK;
    jz_uuid *schema_version;
    jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(physical_mappings, schema_version, mapping) {
        const jazz_model_SchemaVersion *schema = jz_btree_get(catalogue_schemas, schema_version);
        if (!schema) { err = JZN_ISV("physical mapping schema payload missing"); goto done; }
        jz_str *logical_table;
        jazz_node_TablePhysicalMapping *table_mapping;
        JZ_BTREE_FOR(&mapping->tables, logical_table, table_mapping) {
            const jazz_protocol_PhysicalTableIdentity *table_identity = jz_btree_get(&mapping->identities.tables, logical_table);
            if (!table_identity) { err = JZN_ISV("physical table identity missing"); goto done; }
            const jazz_model_TableSchema *table = NULL;
            for (u32 i = 0; i < schema->schema.tables.len; i++)
                if (jz_str_eq(&schema->schema.tables.ptr[i].name, logical_table)) { table = &schema->schema.tables.ptr[i]; break; }
            if (!table) { err = JZN_ISV("physical mapping logical table missing"); goto done; }
            bool inserted;
            jzn_lineage_vec *entries = jz_btree_entry(&lineages, &table_mapping->table_id, &inserted);
            jzn_lineage_entry e = { *schema_version, table, table_mapping, table_identity };
            JZ_VEC_PUSH(*entries, e);
        }
    }
    groove_TableSchemaVec tables = {0};
    jz_vec_reserve(&tables, lineages.len * 7, sizeof(groove_TableSchema));
    u64 *table_id;
    jzn_lineage_vec *variants;
    JZ_BTREE_FOR(&lineages, table_id, variants) {
        err = jzn_build_lineage_tables(*table_id, variants, schema_version_aliases, &tables);
        if (JZ_IS_ERR(err)) {
            jz_vec_drop(&tables, sizeof(groove_TableSchema), groove_TableSchema_drop);
            goto done;
        }
    }
    *out = tables;
done:
    jz_btree_drop(&lineages);
    return err;
}

static jz_err jzn_build_lineage_tables(u64 table_id, const jzn_lineage_vec *variants, const jz_btree *aliases, groove_TableSchemaVec *tables) {
    if (variants->len == 0) return JZN_ISV("physical history lineage empty");
    jz_err err = JZ_OK;
    const jazz_model_TableSchema *template_table = variants->ptr[0].table;
    groove_TableSchema template = jazz_model__schema__TableSchema__history_storage_table(template_table);
    groove_ColumnSchemaVec system_columns = {0}, trailing_history_columns = {0};
    jzn_push_columns_range(&system_columns, &template.columns, 0, JZN_HISTORY_USER_CELLS);
    jzn_push_columns_range(&trailing_history_columns, &template.columns, JZN_HISTORY_USER_CELLS + template_table->columns.len,
                           template.columns.len);

    /* the containers below are all released at `out:` */
    jz_btree scalar_sets, scalar_enum_registries, nested_scalar_sets, nested_scalar_enum_registries;
    jz_btree nested_payload_sets, nested_payload_layouts, nested_payload_enum_registries;
    jz_btree payload_sets, payload_enum_layouts, payload_enum_registries, physical_columns;
    jz_btree_init(&scalar_sets, &jzn_desc_u64_set_map);
    jz_btree_init(&scalar_enum_registries, &jzn_desc_u64_podvec);
    jz_btree_init(&nested_scalar_sets, &jzn_desc_colpath_set);
    jz_btree_init(&nested_scalar_enum_registries, &jzn_desc_colpath_vec);
    jz_btree_init(&nested_payload_sets, &jzn_desc_colpath_set);
    jz_btree_init(&nested_payload_layouts, &jzn_desc_colpathcase_desc);
    jz_btree_init(&nested_payload_enum_registries, &jzn_desc_colpath_vec);
    jz_btree_init(&payload_sets, &jzn_desc_u64_set_map);
    jz_btree_init(&payload_enum_layouts, &jzn_desc_colcase_desc);
    jz_btree_init(&payload_enum_registries, &jzn_desc_u64_podvec);
    jz_btree_init(&physical_columns, &jzn_desc_u64_valuetype);
    groove_TableSchema physical = {0}, physical_global = {0}, physical_ahead = {0}, rejected = {0};
    bool built = false;

    /* 1. union of permanent scalar case identities per column */
    for (u32 v = 0; v < variants->len; v++) {
        const jzn_lineage_entry *ent = &variants->ptr[v];
        for (u32 c = 0; c < ent->table->columns.len; c++) {
            const jazz_model_ColumnSchema *column = &ent->table->columns.ptr[c];
            if (column->column_type.tag != GROOVE_VT_ENUM_TAG) continue;
            const u64 *column_id = jz_btree_get(&ent->mapping->columns, &column->name);
            if (!column_id) { err = JZN_ISV("physical scalar enum column mapping missing"); goto out; }
            bool inserted;
            jz_btree *set = jz_btree_entry(&scalar_sets, column_id, &inserted);
            if (inserted) jz_btree_init(set, &jzn_desc_gse_set);
            const jazz_node_GlobalScalarEnumCaseIdVec *cases = jz_btree_get(&ent->mapping->scalar_enum_cases, column_id);
            if (cases) {
                for (u32 i = 0; i < cases->len; i++) jz_btree_insert(set, &cases->ptr[i], NULL, NULL);
            } else {
                /* Provisional bootstrap: the deterministic spelling hydration will persist. */
                const jz_vec *ids = jzn_root_variants(jzn_identity_column(ent->identity, &column->name));
                u32 n = column->column_type.v.enum_tag.variants.len < ids->len ? column->column_type.v.enum_tag.variants.len : ids->len;
                for (u32 ordinal = 0; ordinal < n; ordinal++) {
                    if (ordinal > 255) { err = JZN_ISV("scalar enum tag exhausted"); goto out; }
                    jazz_node_GlobalScalarEnumCaseId id = { ((const jz_uuid *)ids->ptr)[ordinal], ent->schema_version, (u8)ordinal };
                    jz_btree_insert(set, &id, NULL, NULL);
                }
            }
        }
    }
    {
        u64 *column_id;
        jz_btree *set;
        JZ_BTREE_FOR(&scalar_sets, column_id, set) {
            jz_vec cases;
            jzn_set_to_sorted_vec(set, sizeof(jazz_node_GlobalScalarEnumCaseId), aliases, true, &cases);
            jz_btree_insert(&scalar_enum_registries, column_id, &cases, NULL);
        }
    }

    /* 2. nested scalar occurrences (seeded from the authored descriptor at bootstrap) */
    for (u32 v = 0; v < variants->len; v++) {
        const jzn_lineage_entry *ent = &variants->ptr[v];
        jz_btree bootstrap_paths;
        jz_btree_clone(&bootstrap_paths, &ent->mapping->nested_scalar_enum_cases, NULL, NULL);
        jz_btree_drop(&bootstrap_paths);
        jz_btree_init(&bootstrap_paths, &jzn_desc_u64_strpodvec_map);
        {
            u64 *cid;
            jz_btree *paths;
            JZ_BTREE_FOR(&ent->mapping->nested_scalar_enum_cases, cid, paths) {
                jz_btree copy;
                jz_btree_init(&copy, &jzn_desc_str_podvec);
                jz_str *p;
                jz_vec *cs;
                JZ_BTREE_FOR(paths, p, cs) {
                    jz_str key = jz_str_clone(p);
                    jz_vec val = {0};
                    jz_vec_clone(&val, cs, sizeof(jazz_node_GlobalScalarEnumCaseId), NULL);
                    jz_btree_insert(&copy, &key, &val, NULL);
                }
                jz_btree_insert(&bootstrap_paths, cid, &copy, NULL);
            }
        }
        if (bootstrap_paths.len == 0) {
            for (u32 c = 0; c < ent->table->columns.len; c++) {
                const jazz_model_ColumnSchema *column = &ent->table->columns.ptr[c];
                if (column->column_type.tag == GROOVE_VT_ENUM_TAG) continue;
                const u64 *column_id = jz_btree_get(&ent->mapping->columns, &column->name);
                if (!column_id) { jz_btree_drop(&bootstrap_paths); err = JZN_ISV("physical nested scalar enum column mapping missing"); goto out; }
                bool inserted;
                jz_btree *slot = jz_btree_entry(&bootstrap_paths, column_id, &inserted);
                if (inserted) jz_btree_init(slot, &jzn_desc_str_podvec);
                err = jazz_node__node__hydrate_nested_scalar_enum_cases(&column->column_type,
                                                                       &jzn_identity_column(ent->identity, &column->name)->enum_variants,
                                                                       ent->schema_version, "root", "root", slot);
                if (JZ_IS_ERR(err)) { jz_btree_drop(&bootstrap_paths); goto out; }
            }
        }
        u64 *cid;
        jz_btree *paths;
        JZ_BTREE_FOR(&bootstrap_paths, cid, paths) {
            jz_str *p;
            jazz_node_GlobalScalarEnumCaseIdVec *cs;
            JZ_BTREE_FOR(paths, p, cs) {
                jzn_col_path key = { *cid, jz_str_clone(p) };
                bool inserted;
                jz_btree *set = jz_btree_entry(&nested_scalar_sets, &key, &inserted);
                if (inserted) jz_btree_init(set, &jzn_desc_gse_set);
                else jz_str_drop(&key.path);
                for (u32 i = 0; i < cs->len; i++) jz_btree_insert(set, &cs->ptr[i], NULL, NULL);
            }
        }
        jz_btree_drop(&bootstrap_paths);
    }
    {
        jzn_col_path *k;
        jz_btree *set;
        JZ_BTREE_FOR(&nested_scalar_sets, k, set) {
            jz_vec cases;
            jzn_set_to_sorted_vec(set, sizeof(jazz_node_GlobalScalarEnumCaseId), aliases, true, &cases);
            jzn_col_path key = { k->column, jz_str_clone(&k->path) };
            jz_btree_insert(&nested_scalar_enum_registries, &key, &cases, NULL);
        }
    }

    /* 3. nested payload occurrences + layouts */
    for (u32 v = 0; v < variants->len; v++) {
        const jzn_lineage_entry *ent = &variants->ptr[v];
        for (u32 c = 0; c < ent->table->columns.len; c++) {
            const jazz_model_ColumnSchema *column = &ent->table->columns.ptr[c];
            const u64 *column_id = jz_btree_get(&ent->mapping->columns, &column->name);
            if (!column_id) { err = JZN_ISV("physical nested payload enum column mapping missing"); goto out; }
            jz_btree bootstrap_paths;
            jz_btree_init(&bootstrap_paths, &jzn_desc_u64_strpodvec_map);
            {
                u64 *cid;
                jz_btree *paths;
                JZ_BTREE_FOR(&ent->mapping->nested_payload_enum_cases, cid, paths) {
                    jz_btree copy;
                    jz_btree_init(&copy, &jzn_desc_str_podvec);
                    jz_str *p;
                    jz_vec *cs;
                    JZ_BTREE_FOR(paths, p, cs) {
                        jz_str key = jz_str_clone(p);
                        jz_vec val = {0};
                        jz_vec_clone(&val, cs, sizeof(jazz_node_GlobalEnumCaseId), NULL);
                        jz_btree_insert(&copy, &key, &val, NULL);
                    }
                    jz_btree_insert(&bootstrap_paths, cid, &copy, NULL);
                }
            }
            if (bootstrap_paths.len == 0) {
                bool inserted;
                jz_btree *slot = jz_btree_entry(&bootstrap_paths, column_id, &inserted);
                if (inserted) jz_btree_init(slot, &jzn_desc_str_podvec);
                err = jazz_node__node__hydrate_nested_payload_enum_cases(&column->column_type,
                                                                        &jzn_identity_column(ent->identity, &column->name)->enum_variants,
                                                                        ent->schema_version, "root", "root", slot);
                if (JZ_IS_ERR(err)) { jz_btree_drop(&bootstrap_paths); goto out; }
            }
            const jz_btree *paths = jz_btree_get(&bootstrap_paths, column_id);
            if (!paths) { jz_btree_drop(&bootstrap_paths); continue; }
            jz_str *p;
            jazz_node_GlobalEnumCaseIdVec *cs;
            JZ_BTREE_FOR(paths, p, cs) {
                jzn_col_path key = { *column_id, jz_str_clone(p) };
                bool inserted;
                jz_btree *set = jz_btree_entry(&nested_payload_sets, &key, &inserted);
                if (inserted) jz_btree_init(set, &jzn_desc_ge_set);
                else jz_str_drop(&key.path);
                for (u32 i = 0; i < cs->len; i++) jz_btree_insert(set, &cs->ptr[i], NULL, NULL);
            }
            jz_btree layouts;
            jz_btree_init(&layouts, &jzn_desc_pathcase_desc);
            jz_str root = jz_str_from("root");
            err = jazz_node__node__physical__collect_nested_payload_enum_layouts(&column->column_type, &root, paths, &layouts);
            jz_str_drop(&root);
            jz_btree_drop(&bootstrap_paths);
            if (JZ_IS_ERR(err)) { jz_btree_drop(&layouts); goto out; }
            jazz_node_PathCaseKey *lk;
            groove_RecordDescriptor *layout;
            JZ_BTREE_FOR(&layouts, lk, layout) {
                jzn_col_path_case key = { *column_id, jz_str_clone(&lk->path), lk->identity };
                bool inserted;
                groove_RecordDescriptor *slot = jz_btree_entry(&nested_payload_layouts, &key, &inserted);
                if (inserted) {
                    *slot = *layout;
                } else {
                    jz_str_drop(&key.path);
                    if (!groove_RecordDescriptor_eq(*slot, *layout)) {
                        jz_btree_drop(&layouts);
                        err = JZN_ISV("same nested payload enum identity has incompatible layout");
                        goto out;
                    }
                }
            }
            jz_btree_drop(&layouts);
        }
    }
    {
        jzn_col_path *k;
        jz_btree *set;
        JZ_BTREE_FOR(&nested_payload_sets, k, set) {
            jz_vec cases;
            jzn_set_to_sorted_vec(set, sizeof(jazz_node_GlobalEnumCaseId), aliases, false, &cases);
            jzn_col_path key = { k->column, jz_str_clone(&k->path) };
            jz_btree_insert(&nested_payload_enum_registries, &key, &cases, NULL);
        }
    }

    /* 4. direct payload enum registries + layouts */
    for (u32 v = 0; v < variants->len; v++) {
        const jzn_lineage_entry *ent = &variants->ptr[v];
        for (u32 c = 0; c < ent->table->columns.len; c++) {
            const jazz_model_ColumnSchema *column = &ent->table->columns.ptr[c];
            if (column->column_type.tag != GROOVE_VT_ENUM) continue;
            const groove_EnumSchema *enum_schema = column->column_type.v.enm;
            const u64 *column_id = jz_btree_get(&ent->mapping->columns, &column->name);
            if (!column_id) { err = JZN_ISV("physical payload enum column mapping missing"); goto out; }
            jazz_node_GlobalEnumCaseIdVec identities = {0};
            const jazz_node_GlobalEnumCaseIdVec *mapped = jz_btree_get(&ent->mapping->payload_enum_cases, column_id);
            if (mapped) {
                jz_vec_clone(&identities, mapped, sizeof(jazz_node_GlobalEnumCaseId), NULL);
            } else {
                const jz_vec *ids = jzn_root_variants(jzn_identity_column(ent->identity, &column->name));
                u32 n = enum_schema->cases.len < ids->len ? enum_schema->cases.len : ids->len;
                for (u32 ordinal = 0; ordinal < n; ordinal++) {
                    jazz_node_GlobalEnumCaseId id = { ((const jz_uuid *)ids->ptr)[ordinal], ent->schema_version, ordinal };
                    JZ_VEC_PUSH(identities, id);
                }
            }
            if (identities.len != enum_schema->cases.len) {
                jz_vec_drop(&identities, sizeof(jazz_node_GlobalEnumCaseId), NULL);
                err = JZN_ISV("payload enum identity mapping width mismatch");
                goto out;
            }
            const jz_btree *nested = jz_btree_get(&ent->mapping->nested_payload_enum_cases, column_id);
            if (nested) {
                jz_str root = jz_str_from("root");
                const jazz_node_GlobalEnumCaseIdVec *nested_root = jz_btree_get(nested, &root);
                jz_str_drop(&root);
                if (nested_root && (nested_root->len != identities.len ||
                                    (identities.len && memcmp(nested_root->ptr, identities.ptr, identities.len * sizeof(jazz_node_GlobalEnumCaseId)) != 0))) {
                    jz_vec_drop(&identities, sizeof(jazz_node_GlobalEnumCaseId), NULL);
                    err = JZN_ISV("direct and nested payload enum identity mappings diverged");
                    goto out;
                }
            }
            for (u32 i = 0; i < identities.len; i++) {
                jzn_col_case key = { *column_id, identities.ptr[i] };
                bool inserted;
                groove_RecordDescriptor *slot = jz_btree_entry(&payload_enum_layouts, &key, &inserted);
                if (inserted) {
                    *slot = enum_schema->cases.ptr[i].payload;
                } else if (!groove_RecordDescriptor_eq(*slot, enum_schema->cases.ptr[i].payload)) {
                    jz_vec_drop(&identities, sizeof(jazz_node_GlobalEnumCaseId), NULL);
                    err = JZN_ISV("same payload enum identity has incompatible layout");
                    goto out;
                }
            }
            bool inserted;
            jz_btree *set = jz_btree_entry(&payload_sets, column_id, &inserted);
            if (inserted) jz_btree_init(set, &jzn_desc_ge_set);
            for (u32 i = 0; i < identities.len; i++) jz_btree_insert(set, &identities.ptr[i], NULL, NULL);
            jz_vec_drop(&identities, sizeof(jazz_node_GlobalEnumCaseId), NULL);
        }
    }
    {
        u64 *column_id;
        jz_btree *set;
        JZ_BTREE_FOR(&payload_sets, column_id, set) {
            jz_vec cases;
            jzn_set_to_sorted_vec(set, sizeof(jazz_node_GlobalEnumCaseId), aliases, false, &cases);
            jz_btree_insert(&payload_enum_registries, column_id, &cases, NULL);
        }
    }

    /* 5. one merged physical value type per physical column */
    for (u32 v = 0; v < variants->len; v++) {
        const jzn_lineage_entry *ent = &variants->ptr[v];
        for (u32 c = 0; c < ent->table->columns.len; c++) {
            const jazz_model_ColumnSchema *column = &ent->table->columns.ptr[c];
            const u64 *column_id_p = jz_btree_get(&ent->mapping->columns, &column->name);
            if (!column_id_p) { err = JZN_ISV("physical history column mapping missing"); goto out; }
            u64 column_id = *column_id_p;
            groove_ValueType storage_type;
            if (column->column_type.tag == GROOVE_VT_ENUM_TAG) {
                const jazz_node_GlobalScalarEnumCaseIdVec *cases = jz_btree_get(&scalar_enum_registries, &column_id);
                if (!cases) { err = JZN_ISV("physical scalar enum registry missing"); goto out; }
                groove_ValueType t;
                t.tag = GROOVE_VT_ENUM_TAG;
                err = jazz_node__node__physical__physical_scalar_enum_schema(column_id, cases->ptr, cases->len, &t.v.enum_tag);
                if (JZ_IS_ERR(err)) goto out;
                storage_type = groove__records__values__ValueType__nullable(t);
            } else if (column->column_type.tag == GROOVE_VT_ENUM) {
                err = jzn_payload_storage_type(column_id, &payload_enum_registries, &payload_enum_layouts, &nested_scalar_enum_registries,
                                               &nested_payload_enum_registries, &nested_payload_layouts, &storage_type);
                if (JZ_IS_ERR(err)) goto out;
            } else if (jzn_any_column(&nested_scalar_enum_registries, column_id) || jzn_any_column(&nested_payload_enum_registries, column_id)) {
                jz_btree scalar_registries, payload_registries, payload_layouts;
                jzn_filter_registries(&nested_scalar_enum_registries, column_id, sizeof(jazz_node_GlobalScalarEnumCaseId), &scalar_registries);
                jzn_filter_registries(&nested_payload_enum_registries, column_id, sizeof(jazz_node_GlobalEnumCaseId), &payload_registries);
                jzn_filter_layouts(&nested_payload_layouts, column_id, &payload_layouts);
                jz_str root = jz_str_from("root");
                groove_ValueType t;
                err = jazz_node__node__physical__physical_nested_enum_value_type(&column->column_type, &root, &scalar_registries,
                                                                                &payload_registries, &payload_layouts, column_id, &t);
                jz_str_drop(&root);
                jz_btree_drop(&scalar_registries);
                jz_btree_drop(&payload_registries);
                jz_btree_drop(&payload_layouts);
                if (JZ_IS_ERR(err)) goto out;
                storage_type = groove__records__values__ValueType__nullable(t);
            } else {
                jz_str rp = jzn_registry_path(column_id);
                storage_type = groove__records__values__ValueType__rebind_variant_registries(
                    groove__records__values__ValueType__nullable(jazz_node__node__physical__physical_storage_value_type(column)), &rp);
                jz_str_drop(&rp);
            }
            groove_ValueType *existing = jz_btree_get(&physical_columns, &column_id);
            if (existing) {
                groove_ValueType merged;
                err = jazz_node__node__physical__merge_physical_value_type(existing, &storage_type, &merged);
                groove_ValueType_drop(&storage_type);
                if (JZ_IS_ERR(err)) goto out;
                groove_ValueType_drop(existing);
                *existing = merged;
            } else {
                jz_btree_insert(&physical_columns, &column_id, &storage_type, NULL);
            }
        }
    }

    /* 6. the four physical tables of this lineage */
    {
        groove_ColumnSchemaVec columns = {0};
        jzn_push_columns_range(&columns, &system_columns, 0, system_columns.len);
        jzn_push_physical_user_columns(&columns, &physical_columns);
        jzn_push_columns_range(&columns, &trailing_history_columns, 0, trailing_history_columns.len);
        groove__schema__TableSchema__new_with_bound_registries(&physical, jazz_node__node__physical__physical_history_table_name(table_id), columns);
        physical.primary_key = template.primary_key ? groove_PrimaryKey_clone(template.primary_key) : NULL;
        jz_vec_clone(&physical.indices, &template.indices, sizeof(groove_IndexSchema), groove_IndexSchema_clone);
    }
    groove_TableSchema logical_global = jazz_model__schema__TableSchema__global_current_storage_table(template_table);
    u32 cur_user_end = JZN_GCUR_USER_CELLS + template_table->columns.len;
    {
        groove_ColumnSchemaVec columns = {0};
        jzn_push_columns_range(&columns, &logical_global.columns, 0, JZN_GCUR_USER_CELLS);
        jzn_push_physical_user_columns(&columns, &physical_columns);
        jzn_push_columns_range(&columns, &logical_global.columns, cur_user_end, logical_global.columns.len);
        groove__schema__TableSchema__new_with_bound_registries(&physical_global, jazz_node__node__physical__physical_global_current_table_name(table_id), columns);
        physical_global.primary_key = logical_global.primary_key ? groove_PrimaryKey_clone(logical_global.primary_key) : NULL;
        jz_str cols[3] = { jz_str_from("branch_key"), jz_str_from("global_time"), jz_str_from("row_uuid") };
        groove__schema__TableSchema__with_index(&physical_global,
            groove__schema__IndexSchema__new(jz_str_from(JAZZ_SCHEMA_GLOBAL_CURRENT_BY_SEQ_INDEX), cols, 3));
    }
    jz_btree indexed_columns, composite_indexes;
    jz_btree_init(&indexed_columns, &jzn_desc_u64_set);
    jz_btree_init(&composite_indexes, &jzn_desc_ids_set);
    for (u32 v = 0; v < variants->len; v++) {
        const jzn_lineage_entry *ent = &variants->ptr[v];
        jzn_StrVec indexed = {0};
        jazz_model__schema__TableSchema__global_current_indexed_columns(ent->table, (void *)&indexed);
        for (u32 i = 0; i < indexed.len; i++) {
            const u64 *id = jz_btree_get(&ent->mapping->columns, &indexed.ptr[i]);
            if (id) jz_btree_insert(&indexed_columns, id, NULL, NULL);
        }
        jzn_str_vec_drop(&indexed);
        jzn_StrVec *columns;
        void *unused;
        JZ_BTREE_FOR(&ent->table->composite_indexes, columns, unused) {
            jzn_ids_key key = {0};
            bool all = true;
            for (u32 i = 0; i < columns->len; i++) {
                const u64 *id = jz_btree_get(&ent->mapping->columns, &columns->ptr[i]);
                if (!id) { all = false; break; }
                JZ_VEC_PUSH(key.ids, *id);
            }
            if (all) jz_btree_insert(&composite_indexes, &key, NULL, NULL);
            else jzn_ids_key_drop(&key);
        }
    }
    {
        u64 *column_id;
        void *unused;
        JZ_BTREE_FOR(&indexed_columns, column_id, unused)
            groove__schema__TableSchema__with_index(&physical_global,
                jzn_branch_index(jazz_node__node__physical__physical_current_index_name(*column_id), *column_id));
        jzn_ids_key *ids;
        JZ_BTREE_FOR(&composite_indexes, ids, unused) groove__schema__TableSchema__with_index(&physical_global, jzn_composite_index(ids));
    }
    groove_TableSchema logical_ahead = jazz_model__schema__TableSchema__ahead_current_storage_table(template_table);
    {
        groove_ColumnSchemaVec columns = {0};
        jzn_push_columns_range(&columns, &logical_global.columns, 0, JZN_GCUR_USER_CELLS);
        jzn_push_physical_user_columns(&columns, &physical_columns);
        jzn_push_columns_range(&columns, &logical_global.columns, cur_user_end, logical_global.columns.len);
        groove__schema__TableSchema__new_with_bound_registries(&physical_ahead, jazz_node__node__physical__physical_ahead_current_table_name(table_id), columns);
        physical_ahead.primary_key = logical_ahead.primary_key ? groove_PrimaryKey_clone(logical_ahead.primary_key) : NULL;
        jz_vec_clone(&physical_ahead.indices, &logical_ahead.indices, sizeof(groove_IndexSchema), groove_IndexSchema_clone);
        u64 *column_id;
        void *unused;
        JZ_BTREE_FOR(&indexed_columns, column_id, unused)
            groove__schema__TableSchema__with_index(&physical_ahead,
                jzn_branch_index(jazz_node__node__physical__physical_current_index_name(*column_id), *column_id));
        jzn_ids_key *ids;
        JZ_BTREE_FOR(&composite_indexes, ids, unused) groove__schema__TableSchema__with_index(&physical_ahead, jzn_composite_index(ids));
    }
    jz_btree_drop(&indexed_columns);
    jz_btree_drop(&composite_indexes);
    groove_TableSchema_drop(&logical_global);
    groove_TableSchema_drop(&logical_ahead);
    {
        groove_TableSchema rejected_template = jazz_model__schema__TableSchema__rejected_versions_storage_table(template_table);
        groove_ColumnSchemaVec columns = {0};
        jzn_push_columns_range(&columns, &rejected_template.columns, 0, JZN_REJVER_USER_CELLS);
        jzn_push_physical_user_columns(&columns, &physical_columns);
        groove__schema__TableSchema__new_with_bound_registries(&rejected, jazz_node__node__physical__physical_rejected_versions_table_name(table_id), columns);
        rejected.primary_key = rejected_template.primary_key ? groove_PrimaryKey_clone(rejected_template.primary_key) : NULL;
        groove_TableSchema_drop(&rejected_template);
    }
    built = true;

    /* 7. variant payload layouts by tag */
    {
        jz_btree layouts_by_tag, current_layouts_by_tag, rejected_layouts_by_tag;
        jz_btree_init(&layouts_by_tag, &jzn_desc_u32_strvec);
        jz_btree_init(&current_layouts_by_tag, &jzn_desc_u32_strvec);
        jz_btree_init(&rejected_layouts_by_tag, &jzn_desc_u32_strvec);
        for (u32 v = 0; v < variants->len && !JZ_IS_ERR(err); v++) {
            const jzn_lineage_entry *ent = &variants->ptr[v];
            const u64 *alias = jz_btree_get(aliases, &ent->schema_version);
            if (!alias) { err = JZN_ISV("physical history schema alias missing"); break; }
            u32 ncases = ent->mapping->variant_cases.len ? ent->mapping->variant_cases.len : 1;
            for (u32 ci = 0; ci < ncases; ci++) {
                u32 tag;
                const jz_btree *fields = NULL;
                if (ent->mapping->variant_cases.len == 0) {
                    err = jazz_node__node__physical__groove_variant_tag(*alias, &tag);
                    if (JZ_IS_ERR(err)) break;
                } else {
                    tag = ent->mapping->variant_cases.ptr[ci].tag;
                    fields = &ent->mapping->variant_cases.ptr[ci].fields;
                }
                jzn_StrVec history, current, rejected_names;
                err = jazz_node__node__physical__physical_history_field_names_for_case(ent->table, ent->mapping, fields, &history);
                if (JZ_IS_ERR(err)) break;
                if (jz_btree_insert(&layouts_by_tag, &tag, &history, NULL)) { err = JZN_ISV("physical table variant tag collision"); break; }
                err = jazz_node__node__physical__physical_current_field_names_for_case(ent->table, ent->mapping, fields, &current);
                if (JZ_IS_ERR(err)) break;
                if (jz_btree_insert(&current_layouts_by_tag, &tag, &current, NULL)) { err = JZN_ISV("physical table variant tag collision"); break; }
                err = jazz_node__node__physical__physical_rejected_version_field_names_for_case(ent->table, ent->mapping, fields, &rejected_names);
                if (JZ_IS_ERR(err)) break;
                if (jz_btree_insert(&rejected_layouts_by_tag, &tag, &rejected_names, NULL)) { err = JZN_ISV("physical table variant tag collision"); break; }
            }
        }
        u32 *tag;
        jzn_StrVec *fields;
        if (!JZ_IS_ERR(err)) JZ_BTREE_FOR(&layouts_by_tag, tag, fields) {
            groove_TableVariantFieldVec payload;
            err = jazz_node__node__physical__variant_payload_fields_for_names(&physical, fields->ptr, fields->len, &payload);
            if (JZ_IS_ERR(err)) break;
            groove__schema__TableSchema__with_variant_payload(&physical, *tag, payload);
        }
        if (!JZ_IS_ERR(err)) JZ_BTREE_FOR(&current_layouts_by_tag, tag, fields) {
            groove_TableVariantFieldVec global_payload, ahead_payload;
            err = jazz_node__node__physical__variant_payload_fields_for_names(&physical_global, fields->ptr, fields->len, &global_payload);
            if (JZ_IS_ERR(err)) break;
            groove__schema__TableSchema__with_variant_payload(&physical_global, *tag, global_payload);
            err = jazz_node__node__physical__variant_payload_fields_for_names(&physical_ahead, fields->ptr, fields->len, &ahead_payload);
            if (JZ_IS_ERR(err)) break;
            groove__schema__TableSchema__with_variant_payload(&physical_ahead, *tag, ahead_payload);
        }
        if (!JZ_IS_ERR(err)) JZ_BTREE_FOR(&rejected_layouts_by_tag, tag, fields) {
            groove_TableVariantFieldVec payload;
            err = jazz_node__node__physical__variant_payload_fields_for_names(&rejected, fields->ptr, fields->len, &payload);
            if (JZ_IS_ERR(err)) break;
            groove__schema__TableSchema__with_variant_payload(&rejected, *tag, payload);
        }
        jz_btree_drop(&layouts_by_tag);
        jz_btree_drop(&current_layouts_by_tag);
        jz_btree_drop(&rejected_layouts_by_tag);
    }
    if (!JZ_IS_ERR(err)) {
        JZ_VEC_PUSH(*tables, physical);
        JZ_VEC_PUSH(*tables, physical_global);
        JZ_VEC_PUSH(*tables, physical_ahead);
        JZ_VEC_PUSH(*tables, rejected);
        built = false;
    }
out:
    if (built) {
        groove_TableSchema_drop(&physical);
        groove_TableSchema_drop(&physical_global);
        groove_TableSchema_drop(&physical_ahead);
        groove_TableSchema_drop(&rejected);
    }
    jz_vec_drop(&system_columns, sizeof(groove_ColumnSchema), groove_ColumnSchema_drop);
    jz_vec_drop(&trailing_history_columns, sizeof(groove_ColumnSchema), groove_ColumnSchema_drop);
    groove_TableSchema_drop(&template);
    jz_btree_drop(&scalar_sets);
    jz_btree_drop(&scalar_enum_registries);
    jz_btree_drop(&nested_scalar_sets);
    jz_btree_drop(&nested_scalar_enum_registries);
    jz_btree_drop(&nested_payload_sets);
    jz_btree_drop(&nested_payload_layouts);
    jz_btree_drop(&nested_payload_enum_registries);
    jz_btree_drop(&payload_sets);
    jz_btree_drop(&payload_enum_layouts);
    jz_btree_drop(&payload_enum_registries);
    jz_btree_drop(&physical_columns);
    return err;
}

/* JSON is logically a String but physically an internal stored scalar. */
groove_ValueType jazz_node__node__physical__physical_storage_value_type(const jazz_model_ColumnSchema *column) {
    if (column->large_value_kind == JAZZ_LARGE_KIND_JSON)
        return groove__large_values__physical_storage_value_type(GROOVE_LARGE_VALUE_KIND_JSON);
    groove_ValueType t;
    groove_ValueType_clone(&t, &column->column_type);
    return t;
}

jz_err jazz_node__node__physical__variant_payload_fields_for_names(const groove_TableSchema *table, const jz_str *names, u32 n,
                                                                  groove_TableVariantFieldVec *out) {
    groove_TableVariantFieldVec fields = {0};
    for (u32 i = 0; i < n; i++) {
        const groove_ColumnSchema *column = jzn_find_column(table, &names[i]);
        if (!column) {
            for (u32 j = 0; j < fields.len; j++) {
                jz_str_drop(&fields.ptr[j].name);
                jz_str_drop(&fields.ptr[j].shared_column);
                groove_ValueType_drop(&fields.ptr[j].value_type);
            }
            jz_free(fields.ptr);
            return JZN_ISV("physical variant shared column missing");
        }
        groove_ValueType t;
        groove_ValueType_clone(&t, &column->column_type);
        groove_TableVariantField f = groove__schema__TableVariantField__shared(jz_str_clone(&names[i]), t, jz_str_clone(&names[i]));
        JZ_VEC_PUSH(fields, f);
    }
    *out = fields;
    return JZ_OK;
}

static jz_err jazz_node__node__physical__merge_physical_record_descriptor(groove_RecordDescriptor existing, groove_RecordDescriptor incoming,
                                                                         groove_RecordDescriptor *out) {
    const groove_DescriptorFieldVec *l = groove__records__RecordDescriptor__fields(existing);
    const groove_DescriptorFieldVec *r = groove__records__RecordDescriptor__fields(incoming);
    if (l->len != r->len) return JZN_ISV("physical variant payload descriptor width changed");
    groove_NamedValueTypeVec fields = {0};
    jz_vec_reserve(&fields, l->len, sizeof(groove_NamedValueType));
    for (u32 i = 0; i < l->len; i++) {
        const groove_DescriptorField *left = &l->ptr[i], *right = &r->ptr[i];
        if (left->has_name != right->has_name || (left->has_name && !jz_str_eq(&left->name, &right->name))) {
            jz_vec_drop(&fields, sizeof(groove_NamedValueType), groove_NamedValueType_drop);
            return JZN_ISV("physical variant payload field identity changed");
        }
        if (!left->has_name) {
            jz_vec_drop(&fields, sizeof(groove_NamedValueType), groove_NamedValueType_drop);
            return JZN_ISV("physical variant payload field unnamed");
        }
        groove_NamedValueType entry;
        jz_err e = jazz_node__node__physical__merge_physical_value_type(&left->value_type, &right->value_type, &entry.value_type);
        if (JZ_IS_ERR(e)) {
            jz_vec_drop(&fields, sizeof(groove_NamedValueType), groove_NamedValueType_drop);
            return e;
        }
        entry.name = jz_str_clone(&left->name);
        JZ_VEC_PUSH(fields, entry);
    }
    *out = groove__records__RecordDescriptor__new(fields);
    return JZ_OK;
}

/* Registry identity, rather than structural equality, is authoritative for
 * enums; the older declaration must be an exact prefix of the newer one. */
jz_err jazz_node__node__physical__merge_physical_value_type(const groove_ValueType *existing, const groove_ValueType *incoming,
                                                           groove_ValueType *out) {
    if (existing->tag == GROOVE_VT_ENUM_TAG && incoming->tag == GROOVE_VT_ENUM_TAG &&
        groove__records__values__ScalarEnumSchema__registry_id(&existing->v.enum_tag) ==
            groove__records__values__ScalarEnumSchema__registry_id(&incoming->v.enum_tag)) {
        const groove_ScalarEnumSchema *left = &existing->v.enum_tag, *right = &incoming->v.enum_tag;
        jzn_StrVec variants = {0};
        jzn_str_vec_clone(&variants, &left->variants);
        u32 established = variants.len;
        for (u32 i = 0; i < right->variants.len; i++) {
            bool present = false;
            for (u32 j = 0; j < established; j++)
                if (jz_str_eq(&variants.ptr[j], &right->variants.ptr[i])) { present = true; break; }
            if (!present) {
                jz_str v = jz_str_clone(&right->variants.ptr[i]);
                JZ_VEC_PUSH(variants, v);
            }
        }
        out->tag = GROOVE_VT_ENUM_TAG;
        jz_err e = groove__records__values__ScalarEnumSchema__new(jz_str_clone(&left->name), variants.ptr, variants.len, &out->v.enum_tag);
        jz_free(variants.ptr);
        if (JZ_IS_ERR(e)) {
            jz_err_drop(&e);
            return JZN_ISV("invalid physical enum registry");
        }
        groove__records__values__ScalarEnumSchema__with_registry_id(&out->v.enum_tag,
                                                                   groove__records__values__ScalarEnumSchema__registry_id(left));
        return JZ_OK;
    }
    if (existing->tag == GROOVE_VT_ENUM && incoming->tag == GROOVE_VT_ENUM && existing->v.enm->registry_id == incoming->v.enm->registry_id) {
        const groove_EnumSchema *left = existing->v.enm, *right = incoming->v.enm;
        groove_EnumCaseVec cases = {0};
        jz_vec_clone(&cases, &left->cases, sizeof(groove_EnumCase), groove_EnumCase_clone);
        for (u32 i = 0; i < right->cases.len; i++) {
            const groove_EnumCase *incoming_case = &right->cases.ptr[i];
            groove_EnumCase *existing_case = NULL;
            for (u32 j = 0; j < cases.len; j++)
                if (jz_str_eq(&cases.ptr[j].name, &incoming_case->name)) { existing_case = &cases.ptr[j]; break; }
            if (existing_case) {
                groove_RecordDescriptor merged;
                jz_err e = jazz_node__node__physical__merge_physical_record_descriptor(existing_case->payload, incoming_case->payload, &merged);
                if (JZ_IS_ERR(e)) {
                    jz_vec_drop(&cases, sizeof(groove_EnumCase), groove_EnumCase_drop);
                    return e;
                }
                existing_case->payload = merged;
            } else {
                groove_EnumCase c;
                groove_EnumCase_clone(&c, incoming_case);
                JZ_VEC_PUSH(cases, c);
            }
        }
        groove_EnumSchema *schema = JZ_NEW(groove_EnumSchema);
        jz_err e = groove__records__values__EnumSchema__new(jz_str_clone(&right->name), cases, schema);
        if (JZ_IS_ERR(e)) {
            jz_err_drop(&e);
            jz_free(schema);
            return JZN_ISV("invalid physical enum registry");
        }
        groove__records__values__EnumSchema__with_registry_id(schema, left->registry_id);
        out->tag = GROOVE_VT_ENUM;
        out->v.enm = schema;
        return JZ_OK;
    }
    if (existing->tag == GROOVE_VT_TUPLE && incoming->tag == GROOVE_VT_TUPLE && existing->v.tuple.len == incoming->v.tuple.len) {
        groove_ValueTypeVec values = {0};
        for (u32 i = 0; i < existing->v.tuple.len; i++) {
            groove_ValueType m;
            jz_err e = jazz_node__node__physical__merge_physical_value_type(&existing->v.tuple.ptr[i], &incoming->v.tuple.ptr[i], &m);
            if (JZ_IS_ERR(e)) {
                jz_vec_drop(&values, sizeof(groove_ValueType), groove_ValueType_drop);
                return e;
            }
            JZ_VEC_PUSH(values, m);
        }
        out->tag = GROOVE_VT_TUPLE;
        out->v.tuple = values;
        return JZ_OK;
    }
    if ((existing->tag == GROOVE_VT_ARRAY && incoming->tag == GROOVE_VT_ARRAY) ||
        (existing->tag == GROOVE_VT_NULLABLE && incoming->tag == GROOVE_VT_NULLABLE)) {
        groove_ValueType inner;
        JZ_TRY(jazz_node__node__physical__merge_physical_value_type(existing->v.inner, incoming->v.inner, &inner));
        *out = groove_vt_box(existing->tag, inner);
        return JZ_OK;
    }
    if (existing->tag == GROOVE_VT_RECORD && incoming->tag == GROOVE_VT_RECORD) {
        groove_RecordDescriptor merged;
        JZ_TRY(jazz_node__node__physical__merge_physical_record_descriptor(*existing->v.record, *incoming->v.record, &merged));
        *out = groove_vt_record(merged);
        return JZ_OK;
    }
    if (groove_ValueType_eq(existing, incoming)) {
        groove_ValueType_clone(out, existing);
        return JZ_OK;
    }
    return JZN_ISV("physical history column type mismatch");
}

static jz_err jazz_node__node__physical__physical_descriptor_with_enum_registries(const jazz_model_TableSchema *table,
                                                                                 groove_RecordDescriptor logical, jzn_StrVec physical_names,
                                                                                 const jazz_node_TablePhysicalMapping *mapping,
                                                                                 groove_RecordDescriptor *out);

jz_err jazz_node__node__physical__physical_current_descriptor(const jazz_model_TableSchema *table, const jazz_node_TablePhysicalMapping *mapping,
                                                             groove_RecordDescriptor *out) {
    groove_TableSchema storage = jazz_model__schema__TableSchema__global_current_storage_table(table);
    groove_RecordDescriptor logical_descriptor = groove__schema__TableSchema__record_schema(&storage);
    groove_TableSchema_drop(&storage);
    jzn_StrVec physical_names;
    JZ_TRY(jazz_node__node__physical__physical_current_field_names(table, mapping, &physical_names));
    if (groove__records__RecordDescriptor__fields(logical_descriptor)->len != physical_names.len) {
        jzn_str_vec_drop(&physical_names);
        return JZN_ISV("physical current descriptor width mismatch");
    }
    return jazz_node__node__physical__physical_descriptor_with_enum_registries(table, logical_descriptor, physical_names, mapping, out);
}

jz_err jazz_node__node__physical__physical_rejected_version_descriptor(const jazz_model_TableSchema *table,
                                                                      const jazz_node_TablePhysicalMapping *mapping, groove_RecordDescriptor *out) {
    groove_TableSchema storage = jazz_model__schema__TableSchema__rejected_versions_storage_table(table);
    groove_RecordDescriptor logical_descriptor = groove__schema__TableSchema__record_schema(&storage);
    groove_TableSchema_drop(&storage);
    jzn_StrVec physical_names;
    JZ_TRY(jazz_node__node__physical__physical_rejected_version_field_names(table, mapping, &physical_names));
    if (groove__records__RecordDescriptor__fields(logical_descriptor)->len != physical_names.len) {
        jzn_str_vec_drop(&physical_names);
        return JZN_ISV("physical rejected-version descriptor width mismatch");
    }
    return jazz_node__node__physical__physical_descriptor_with_enum_registries(table, logical_descriptor, physical_names, mapping, out);
}

static bool jazz_node__node__physical__physical_descriptor_with_enum_registries__physical_user_slot(const jz_str *name, u64 *out) {
    if (name->len <= 5 || memcmp(name->ptr, "_app_", 5) != 0) return false;
    u64 v = 0;
    for (u32 i = 5; i < name->len; i++) {
        u8 d = (u8)name->ptr[i];
        if (d < '0' || d > '9') return false;
        if (v > (UINT64_MAX - (d - '0')) / 10) return false;
        v = v * 10 + (d - '0');
    }
    *out = v;
    return true;
}

static jz_err jazz_node__node__physical__physical_descriptor_with_enum_registries(const jazz_model_TableSchema *table,
                                                                                 groove_RecordDescriptor logical, jzn_StrVec physical_names,
                                                                                 const jazz_node_TablePhysicalMapping *mapping,
                                                                                 groove_RecordDescriptor *out) {
    const groove_DescriptorFieldVec *lf = groove__records__RecordDescriptor__fields(logical);
    groove_DescriptorFieldVec fields = {0};
    jz_err err = JZ_OK;
    u32 n = physical_names.len < lf->len ? physical_names.len : lf->len;
    u32 i = 0;
    for (; i < n; i++) {
        jz_str name = physical_names.ptr[i];
        const groove_DescriptorField *field = &lf->ptr[i];
        groove_DescriptorField df;
        memset(&df, 0, sizeof df);
        u64 id;
        bool slot = jazz_node__node__physical__physical_descriptor_with_enum_registries__physical_user_slot(&name, &id);
        if (slot) {
            jz_slice logical_name;
            if (groove__records__DescriptorField__logical_name(field, &logical_name) &&
                !(logical_name.len == name.len && memcmp(logical_name.ptr, name.ptr, name.len) == 0)) {
                df.identity.tag = GROOVE_FI_NAMED_SLOT;
                df.identity.name = jz_str_from_slice(logical_name);
                df.identity.slot = id;
            } else {
                df.identity.tag = GROOVE_FI_SLOT;
                df.identity.slot = id;
            }
            const jazz_node_GlobalScalarEnumCaseIdVec *cases = jz_btree_get(&mapping->scalar_enum_cases, &id);
            const jazz_model_ColumnSchema *column = NULL;
            if (!cases) {
                jz_str *cname; u64 *candidate;
                const jz_str *found = NULL;
                JZ_BTREE_FOR(&mapping->columns, cname, candidate) if (*candidate == id) { found = cname; break; }
                if (found)
                    for (u32 c = 0; c < table->columns.len; c++)
                        if (jz_str_eq(&table->columns.ptr[c].name, found)) { column = &table->columns.ptr[c]; break; }
            }
            if (cases) {
                groove_ValueType t;
                t.tag = GROOVE_VT_ENUM_TAG;
                err = jazz_node__node__physical__physical_scalar_enum_schema(id, cases->ptr, cases->len, &t.v.enum_tag);
                if (JZ_IS_ERR(err)) { jz_str_drop(&name); i++; break; }
                df.value_type = groove__records__values__ValueType__nullable(t);
            } else if (column) {
                df.value_type = groove__records__values__ValueType__nullable(jazz_node__node__physical__physical_storage_value_type(column));
            } else {
                jz_str rp = jzn_registry_path(id);
                if (field->value_type.tag == GROOVE_VT_NULLABLE) {
                    groove_ValueType inner;
                    groove_ValueType_clone(&inner, field->value_type.v.inner);
                    df.value_type = groove_vt_box(GROOVE_VT_NULLABLE, groove__records__values__ValueType__rebind_variant_registries(inner, &rp));
                } else {
                    groove_ValueType t;
                    groove_ValueType_clone(&t, &field->value_type);
                    df.value_type = groove__records__values__ValueType__rebind_variant_registries(t, &rp);
                }
                jz_str_drop(&rp);
            }
        } else {
            groove_FieldIdentity_clone(&df.identity, &field->identity);
            groove_ValueType_clone(&df.value_type, &field->value_type);
        }
        df.has_name = true;
        df.name = name;
        JZ_VEC_PUSH(fields, df);
    }
    for (; i < physical_names.len; i++) jz_str_drop(&physical_names.ptr[i]);
    jz_free(physical_names.ptr);
    if (JZ_IS_ERR(err)) {
        jz_vec_drop(&fields, sizeof(groove_DescriptorField), groove_DescriptorField_drop);
        return err;
    }
    *out = groove__records__RecordDescriptor__new_with_fields(fields);
    return JZ_OK;
}

/* Naming a projection does not construct storage tables: fixed prefix from the
 * record declaration, user fields in schema order with physical identities. */
static jz_err jazz_node__node__physical__physical_row_field_names(const jazz_model_TableSchema *table, const jazz_node_TablePhysicalMapping *mapping,
                                                                 const jz_btree *present, const char *const *prefix, u32 nprefix,
                                                                 const char *const *suffix, u32 nsuffix, const char *missing_mapping,
                                                                 jzn_StrVec *out) {
    jzn_StrVec fields = {0};
    jz_vec_reserve(&fields, nprefix + table->columns.len + nsuffix, sizeof(jz_str));
    for (u32 i = 0; i < nprefix; i++) {
        jz_str s = jz_str_from(prefix[i]);
        JZ_VEC_PUSH(fields, s);
    }
    for (u32 c = 0; c < table->columns.len; c++) {
        const jazz_model_ColumnSchema *column = &table->columns.ptr[c];
        if (present && !jz_btree_contains(present, &column->name)) continue;
        const u64 *column_id = jz_btree_get(&mapping->columns, &column->name);
        if (!column_id) {
            jzn_str_vec_drop(&fields);
            return JZN_ISV(missing_mapping);
        }
        jz_str f = jazz_node__node__physical__physical_user_column_field(*column_id);
        JZ_VEC_PUSH(fields, f);
    }
    for (u32 i = 0; i < nsuffix; i++) {
        jz_str s = jz_str_from(suffix[i]);
        JZ_VEC_PUSH(fields, s);
    }
    *out = fields;
    return JZ_OK;
}

static const char *const jzn_history_suffix[] = {
    "authored_columns", JAZZ_SCHEMA_COUNTER_SIGNS_FIELD, JAZZ_SCHEMA_SEQ_FIELD, JAZZ_SCHEMA_BASE_SEQ_FIELD, JAZZ_SCHEMA_BASE_PENDING_FIELD,
};
static const char *const jzn_current_suffix[] = { "authored_columns" };

jz_err jazz_node__node__physical__physical_history_field_names(const jazz_model_TableSchema *t, const jazz_node_TablePhysicalMapping *m, jzn_StrVec *out) {
    return jazz_node__node__physical__physical_history_field_names_for_case(t, m, NULL, out);
}
jz_err jazz_node__node__physical__physical_history_field_names_for_case(const jazz_model_TableSchema *t, const jazz_node_TablePhysicalMapping *m,
                                                                       const jz_btree *present, jzn_StrVec *out) {
    return jazz_node__node__physical__physical_row_field_names(t, m, present, jzn_HistoryRowRecord_PREFIX_FIELD_NAMES, JZN_HISTORY_USER_CELLS,
                                                               jzn_history_suffix, 5, "physical history column mapping missing", out);
}
jz_err jazz_node__node__physical__physical_current_field_names(const jazz_model_TableSchema *t, const jazz_node_TablePhysicalMapping *m, jzn_StrVec *out) {
    return jazz_node__node__physical__physical_current_field_names_for_case(t, m, NULL, out);
}
jz_err jazz_node__node__physical__physical_current_field_names_for_case(const jazz_model_TableSchema *t, const jazz_node_TablePhysicalMapping *m,
                                                                       const jz_btree *present, jzn_StrVec *out) {
    return jazz_node__node__physical__physical_row_field_names(t, m, present, jzn_GlobalCurrentRowRecord_PREFIX_FIELD_NAMES, JZN_GCUR_USER_CELLS,
                                                               jzn_current_suffix, 1, "physical current column mapping missing", out);
}
jz_err jazz_node__node__physical__physical_rejected_version_field_names(const jazz_model_TableSchema *t, const jazz_node_TablePhysicalMapping *m, jzn_StrVec *out) {
    return jazz_node__node__physical__physical_rejected_version_field_names_for_case(t, m, NULL, out);
}
jz_err jazz_node__node__physical__physical_rejected_version_field_names_for_case(const jazz_model_TableSchema *t, const jazz_node_TablePhysicalMapping *m,
                                                                                const jz_btree *present, jzn_StrVec *out) {
    return jazz_node__node__physical__physical_row_field_names(t, m, present, jzn_RejectedVersionRowRecord_PREFIX_FIELD_NAMES, JZN_REJVER_USER_CELLS,
                                                               NULL, 0, "physical rejected-version column mapping missing", out);
}
