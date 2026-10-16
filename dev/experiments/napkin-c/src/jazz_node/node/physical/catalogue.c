/* physical/catalogue.c: jazz_node::node::physical (physical/catalogue.rs).
 * Durable physical mapping types, enum registry lowering and physical names. */
#include "../physical.h"
#include "../codec.h"

/* Lower Jazz's durable schema alias into Groove's table-local union-case space. */
jz_err jazz_node__node__physical__groove_variant_tag(jazz_types_SchemaVersionAlias alias, u32 *out) {
    if (alias > UINT32_MAX) return JZN_ISV("physical table variant tag exhausted");
    *out = (u32)alias;
    return JZ_OK;
}

static bool jzn_nested_nonempty(const jz_btree *nested, jazz_types_PhysicalColumnId column_id) {
    const jz_btree *paths = jz_btree_get(nested, &column_id);
    return paths && paths->len != 0;
}

bool jazz_node__node__physical__physical_mapping_has_enum_boundary(const jazz_node_TablePhysicalMapping *mapping,
                                                                  jazz_types_PhysicalColumnId column_id) {
    return jz_btree_contains(&mapping->scalar_enum_cases, &column_id) ||
           jz_btree_contains(&mapping->payload_enum_cases, &column_id) ||
           jzn_nested_nonempty(&mapping->nested_scalar_enum_cases, column_id) ||
           jzn_nested_nonempty(&mapping->nested_payload_enum_cases, column_id);
}

bool jazz_node__node__physical__value_type_has_enum_boundary(const groove_ValueType *value_type) {
    switch (value_type->tag) {
    case GROOVE_VT_ENUM_TAG:
    case GROOVE_VT_ENUM:
        return true;
    case GROOVE_VT_NULLABLE:
    case GROOVE_VT_ARRAY:
        return jazz_node__node__physical__value_type_has_enum_boundary(value_type->v.inner);
    case GROOVE_VT_TUPLE:
        for (u32 i = 0; i < value_type->v.tuple.len; i++)
            if (jazz_node__node__physical__value_type_has_enum_boundary(&value_type->v.tuple.ptr[i])) return true;
        return false;
    case GROOVE_VT_RECORD: {
        const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(*value_type->v.record);
        for (u32 i = 0; i < fields->len; i++)
            if (jazz_node__node__physical__value_type_has_enum_boundary(&fields->ptr[i].value_type)) return true;
        return false;
    }
    default:
        return false;
    }
}

/* entry(path.to_owned()).or_insert(value): value was evaluated eagerly */
static void jzn_or_insert_vec(jz_btree *map, const jz_str *path, void *vec, usize elem, void (*drop)(void *)) {
    if (jz_btree_contains(map, path)) {
        jz_vec_drop(vec, elem, drop);
        return;
    }
    jz_str key = jz_str_clone(path);
    jz_btree_insert(map, &key, vec, NULL);
}
static void jzn_str_drop_v(void *p) { jz_str_drop((jz_str *)p); }

/* Bootstrap mappings have no durable per-column registry entries yet: copy
 * the authored layout by ordinal. */
jz_err jazz_node__node__physical__bootstrap_copy_enum_remaps(const groove_ValueType *source, const groove_ValueType *target,
                                                            const jz_str *path, jazz_node_EnumOccurrenceRemaps *remaps) {
    if (source->tag != target->tag) return JZ_OK;
    switch (source->tag) {
    case GROOVE_VT_ENUM_TAG: {
        groove_OptU8Vec tags = {0};
        for (u32 ordinal = 0; ordinal < source->v.enum_tag.variants.len; ordinal++) {
            u16 tag = GROOVE_OPT_U8_NONE;
            if (ordinal < target->v.enum_tag.variants.len) {
                if (ordinal > 255) {
                    jz_vec_drop(&tags, sizeof(u16), NULL);
                    return JZN_ISV("bootstrap copied scalar enum tag exhausted");
                }
                tag = (u16)ordinal;
            }
            JZ_VEC_PUSH(tags, tag);
        }
        jzn_or_insert_vec(&remaps->scalar, path, &tags, sizeof(u16), NULL);
        break;
    }
    case GROOVE_VT_ENUM: {
        const groove_EnumSchema *s = source->v.enm, *t = target->v.enm;
        groove_OptU32Vec tags = {0};
        for (u32 ordinal = 0; ordinal < s->cases.len; ordinal++) {
            i64 tag = ordinal < t->cases.len ? (i64)ordinal : -1;
            JZ_VEC_PUSH(tags, tag);
        }
        jzn_or_insert_vec(&remaps->payload, path, &tags, sizeof(i64), NULL);
        groove_OptStrVec children = {0};
        for (u32 ordinal = 0; ordinal < s->cases.len; ordinal++) {
            jz_str child = {0};
            if (ordinal < t->cases.len) child = jz_format("%.*s/case/bootstrap/%u", JZN_S(*path), ordinal);
            JZ_VEC_PUSH(children, child);
        }
        jzn_or_insert_vec(&remaps->payload_children, path, &children, sizeof(jz_str), jzn_str_drop_v);
        u32 n = s->cases.len < t->cases.len ? s->cases.len : t->cases.len;
        for (u32 ordinal = 0; ordinal < n; ordinal++) {
            groove_ValueType sv = groove_vt_record(s->cases.ptr[ordinal].payload);
            groove_ValueType tv = groove_vt_record(t->cases.ptr[ordinal].payload);
            jz_str child = jz_format("%.*s/case/bootstrap/%u", JZN_S(*path), ordinal);
            jz_err e = jazz_node__node__physical__bootstrap_copy_enum_remaps(&sv, &tv, &child, remaps);
            jz_str_drop(&child);
            groove_ValueType_drop(&sv);
            groove_ValueType_drop(&tv);
            JZ_TRY(e);
        }
        break;
    }
    case GROOVE_VT_NULLABLE:
    case GROOVE_VT_ARRAY: {
        jz_str child = jz_format("%.*s/%s", JZN_S(*path), source->tag == GROOVE_VT_NULLABLE ? "nullable" : "array");
        jz_err e = jazz_node__node__physical__bootstrap_copy_enum_remaps(source->v.inner, target->v.inner, &child, remaps);
        jz_str_drop(&child);
        JZ_TRY(e);
        break;
    }
    case GROOVE_VT_TUPLE: {
        u32 n = source->v.tuple.len < target->v.tuple.len ? source->v.tuple.len : target->v.tuple.len;
        for (u32 index = 0; index < n; index++) {
            jz_str child = jz_format("%.*s/tuple/%u", JZN_S(*path), index);
            jz_err e = jazz_node__node__physical__bootstrap_copy_enum_remaps(&source->v.tuple.ptr[index],
                                                                            &target->v.tuple.ptr[index], &child, remaps);
            jz_str_drop(&child);
            JZ_TRY(e);
        }
        break;
    }
    case GROOVE_VT_RECORD: {
        const groove_DescriptorFieldVec *sf = groove__records__RecordDescriptor__fields(*source->v.record);
        const groove_DescriptorFieldVec *tf = groove__records__RecordDescriptor__fields(*target->v.record);
        u32 n = sf->len < tf->len ? sf->len : tf->len;
        for (u32 i = 0; i < n; i++) {
            if (!sf->ptr[i].has_name) return JZN_ISV("bootstrap copied enum record field unnamed");
            jz_str child = jz_format("%.*s/record/%.*s", JZN_S(*path), JZN_S(sf->ptr[i].name));
            jz_err e = jazz_node__node__physical__bootstrap_copy_enum_remaps(&sf->ptr[i].value_type, &tf->ptr[i].value_type,
                                                                            &child, remaps);
            jz_str_drop(&child);
            JZ_TRY(e);
        }
        break;
    }
    default:
        break;
    }
    return JZ_OK;
}

/* Physical case names are interned spellings of the global identity. */
static jz_str jzn_case_name(const jz_uuid *id) {
    static const char hex[] = "0123456789abcdef";
    char simple[32];
    for (int i = 0; i < 16; i++) {
        simple[2 * i] = hex[id->b[i] >> 4];
        simple[2 * i + 1] = hex[id->b[i] & 15];
    }
    return jz_format("case-%.*s", 32, simple);
}
jz_str jazz_node__node__physical__physical_scalar_enum_case_name(const jazz_node_GlobalScalarEnumCaseId *c) { return jzn_case_name(&c->id); }
jz_str jazz_node__node__physical__physical_enum_case_name(const jazz_node_GlobalEnumCaseId *c) { return jzn_case_name(&c->id); }

/* Option<&SchemaVersionAlias> ordering: None < Some */
static int jzn_cmp_alias_opt(const jz_btree *aliases, const jz_uuid *l, const jz_uuid *r) {
    const u64 *a = jz_btree_get(aliases, l), *b = jz_btree_get(aliases, r);
    if (!a || !b) return (a != NULL) - (b != NULL);
    return (*a > *b) - (*a < *b);
}

int jazz_node__node__physical__compare_scalar_enum_cases(const jz_btree *aliases, const jazz_node_GlobalScalarEnumCaseId *left,
                                                       const jazz_node_GlobalScalarEnumCaseId *right) {
    int c = jzn_cmp_alias_opt(aliases, &left->introducing_schema, &right->introducing_schema);
    if (c) return c;
    c = (left->introducing_ordinal > right->introducing_ordinal) - (left->introducing_ordinal < right->introducing_ordinal);
    if (c) return c;
    return jz_uuid_cmp(&left->id, &right->id);
}

/* A case's introducing schema alias is its durable introduction position;
 * ordinal orders only cases introduced by the same schema; UUID breaks ties. */
int jazz_node__node__physical__compare_global_enum_cases(const jz_btree *aliases, const jazz_node_GlobalEnumCaseId *left,
                                                       const jazz_node_GlobalEnumCaseId *right) {
    int c = jzn_cmp_alias_opt(aliases, &left->introducing_schema, &right->introducing_schema);
    if (c) return c;
    c = (left->introducing_ordinal > right->introducing_ordinal) - (left->introducing_ordinal < right->introducing_ordinal);
    if (c) return c;
    return jz_uuid_cmp(&left->id, &right->id);
}

jz_err jazz_node__node__physical__physical_scalar_enum_schema(jazz_types_PhysicalColumnId column_id,
                                                             const jazz_node_GlobalScalarEnumCaseId *cases, u32 n,
                                                             groove_ScalarEnumSchema *out) {
    jz_str *names = jz_alloc((usize)(n ? n : 1) * sizeof(jz_str));
    for (u32 i = 0; i < n; i++) names[i] = jazz_node__node__physical__physical_scalar_enum_case_name(&cases[i]);
    jz_err e = groove__records__values__ScalarEnumSchema__new(jz_format("physical-column-%llu", (unsigned long long)column_id),
                                                             names, n, out);
    jz_free(names);
    if (JZ_IS_ERR(e)) {
        jz_err_drop(&e);
        return JZN_ISV("invalid physical scalar enum registry");
    }
    jz_str path = jz_format("physical-column/%llu/nullable", (unsigned long long)column_id);
    groove__records__values__ScalarEnumSchema__with_registry_id(out, groove__records__values__variant_registry_id_for_path(&path));
    jz_str_drop(&path);
    return JZ_OK;
}

/* Lower a recursively nested authored value into the durable physical enum
 * registries for one user column. */
jz_err jazz_node__node__physical__physical_nested_enum_value_type(const groove_ValueType *value_type, const jz_str *path,
                                                                 const jz_btree *scalar_registries, const jz_btree *payload_registries,
                                                                 const jz_btree *payload_layouts, jazz_types_PhysicalColumnId column_id,
                                                                 groove_ValueType *out) {
    switch (value_type->tag) {
    case GROOVE_VT_ENUM_TAG: {
        const jazz_node_GlobalScalarEnumCaseIdVec *cases = jz_btree_get(scalar_registries, path);
        if (!cases) return JZN_ISV("physical nested scalar enum registry missing");
        out->tag = GROOVE_VT_ENUM_TAG;
        return jazz_node__node__physical__physical_scalar_enum_schema(column_id, cases->ptr, cases->len, &out->v.enum_tag);
    }
    case GROOVE_VT_NULLABLE:
    case GROOVE_VT_ARRAY: {
        jz_str child = jz_format("%.*s/%s", JZN_S(*path), value_type->tag == GROOVE_VT_NULLABLE ? "nullable" : "array");
        groove_ValueType inner;
        jz_err e = jazz_node__node__physical__physical_nested_enum_value_type(value_type->v.inner, &child, scalar_registries,
                                                                             payload_registries, payload_layouts, column_id, &inner);
        jz_str_drop(&child);
        JZ_TRY(e);
        *out = groove_vt_box(value_type->tag, inner);
        return JZ_OK;
    }
    case GROOVE_VT_TUPLE: {
        groove_ValueTypeVec values = {0};
        for (u32 index = 0; index < value_type->v.tuple.len; index++) {
            jz_str child = jz_format("%.*s/tuple/%u", JZN_S(*path), index);
            groove_ValueType v;
            jz_err e = jazz_node__node__physical__physical_nested_enum_value_type(&value_type->v.tuple.ptr[index], &child,
                                                                                 scalar_registries, payload_registries,
                                                                                 payload_layouts, column_id, &v);
            jz_str_drop(&child);
            if (JZ_IS_ERR(e)) {
                jz_vec_drop(&values, sizeof(groove_ValueType), groove_ValueType_drop);
                return e;
            }
            JZ_VEC_PUSH(values, v);
        }
        out->tag = GROOVE_VT_TUPLE;
        out->v.tuple = values;
        return JZ_OK;
    }
    case GROOVE_VT_RECORD: {
        const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(*value_type->v.record);
        groove_NamedValueTypeVec lowered = {0};
        for (u32 i = 0; i < fields->len; i++) {
            const groove_DescriptorField *field = &fields->ptr[i];
            if (!field->has_name) {
                jz_vec_drop(&lowered, sizeof(groove_NamedValueType), groove_NamedValueType_drop);
                return JZN_ISV("nested scalar enum record field unnamed");
            }
            jz_str child = jz_format("%.*s/record/%.*s", JZN_S(*path), JZN_S(field->name));
            groove_NamedValueType entry;
            jz_err e = jazz_node__node__physical__physical_nested_enum_value_type(&field->value_type, &child, scalar_registries,
                                                                                 payload_registries, payload_layouts, column_id,
                                                                                 &entry.value_type);
            jz_str_drop(&child);
            if (JZ_IS_ERR(e)) {
                jz_vec_drop(&lowered, sizeof(groove_NamedValueType), groove_NamedValueType_drop);
                return e;
            }
            entry.name = jz_str_clone(&field->name);
            JZ_VEC_PUSH(lowered, entry);
        }
        *out = groove_vt_record(groove__records__RecordDescriptor__new(lowered));
        return JZ_OK;
    }
    case GROOVE_VT_ENUM: {
        const jazz_node_GlobalEnumCaseIdVec *cases = jz_btree_get(payload_registries, path);
        if (!cases) return JZN_ISV("physical nested payload enum registry missing");
        groove_EnumCaseVec lowered = {0};
        for (u32 i = 0; i < cases->len; i++) {
            const jazz_node_GlobalEnumCaseId *identity = &cases->ptr[i];
            jazz_node_PathCaseKey key = { *path, *identity };
            const groove_RecordDescriptor *payload = jz_btree_get(payload_layouts, &key);
            if (!payload) {
                jz_vec_drop(&lowered, sizeof(groove_EnumCase), groove_EnumCase_drop);
                return JZN_ISV("physical nested payload enum layout missing");
            }
            groove_ValueType record = groove_vt_record(*payload);
            jz_str child = jazz_node__node__global_case_path(path, identity);
            groove_ValueType lowered_record;
            jz_err e = jazz_node__node__physical__physical_nested_enum_value_type(&record, &child, scalar_registries,
                                                                                 payload_registries, payload_layouts, column_id,
                                                                                 &lowered_record);
            jz_str_drop(&child);
            groove_ValueType_drop(&record);
            if (JZ_IS_ERR(e)) {
                jz_vec_drop(&lowered, sizeof(groove_EnumCase), groove_EnumCase_drop);
                return e;
            }
            if (lowered_record.tag != GROOVE_VT_RECORD) jz_panic("record lowering preserves record shape");
            groove_EnumCase c;
            c.name = jazz_node__node__physical__physical_enum_case_name(identity);
            c.payload = *lowered_record.v.record;
            jz_free(lowered_record.v.record);
            JZ_VEC_PUSH(lowered, c);
        }
        groove_EnumSchema *schema = JZ_NEW(groove_EnumSchema);
        jz_err e = groove__records__values__EnumSchema__new(
            jz_format("physical-column-%llu-%.*s", (unsigned long long)column_id, JZN_S(*path)), lowered, schema);
        if (JZ_IS_ERR(e)) {
            jz_err_drop(&e);
            jz_free(schema);
            return JZN_ISV("invalid physical nested payload enum registry");
        }
        jz_str registry_path = jz_format("physical-column/%llu/%.*s", (unsigned long long)column_id, JZN_S(*path));
        groove__records__values__EnumSchema__with_registry_id(schema, groove__records__values__variant_registry_id_for_path(&registry_path));
        jz_str_drop(&registry_path);
        out->tag = GROOVE_VT_ENUM;
        out->v.enm = schema;
        return JZ_OK;
    }
    default:
        groove_ValueType_clone(out, value_type);
        return JZ_OK;
    }
}

/* Collect the authored payload layout for each durable nested payload case;
 * the same identity must always describe one layout. */
jz_err jazz_node__node__physical__collect_nested_payload_enum_layouts(const groove_ValueType *value_type, const jz_str *path,
                                                                     const jz_btree *identities, jz_btree *output) {
    switch (value_type->tag) {
    case GROOVE_VT_NULLABLE:
    case GROOVE_VT_ARRAY: {
        jz_str child = jz_format("%.*s/%s", JZN_S(*path), value_type->tag == GROOVE_VT_NULLABLE ? "nullable" : "array");
        jz_err e = jazz_node__node__physical__collect_nested_payload_enum_layouts(value_type->v.inner, &child, identities, output);
        jz_str_drop(&child);
        return e;
    }
    case GROOVE_VT_TUPLE:
        for (u32 index = 0; index < value_type->v.tuple.len; index++) {
            jz_str child = jz_format("%.*s/tuple/%u", JZN_S(*path), index);
            jz_err e = jazz_node__node__physical__collect_nested_payload_enum_layouts(&value_type->v.tuple.ptr[index], &child,
                                                                                     identities, output);
            jz_str_drop(&child);
            JZ_TRY(e);
        }
        return JZ_OK;
    case GROOVE_VT_RECORD: {
        const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(*value_type->v.record);
        for (u32 i = 0; i < fields->len; i++) {
            if (!fields->ptr[i].has_name) return JZN_ISV("nested payload enum record field unnamed");
            jz_str child = jz_format("%.*s/record/%.*s", JZN_S(*path), JZN_S(fields->ptr[i].name));
            jz_err e = jazz_node__node__physical__collect_nested_payload_enum_layouts(&fields->ptr[i].value_type, &child,
                                                                                     identities, output);
            jz_str_drop(&child);
            JZ_TRY(e);
        }
        return JZ_OK;
    }
    case GROOVE_VT_ENUM: {
        const groove_EnumSchema *schema = value_type->v.enm;
        const jazz_node_GlobalEnumCaseIdVec *cases = jz_btree_get(identities, path);
        if (!cases) return JZN_ISV("nested payload enum identity mapping missing");
        if (cases->len != schema->cases.len) return JZN_ISV("nested payload enum identity mapping width mismatch");
        for (u32 i = 0; i < cases->len; i++) {
            const jazz_node_GlobalEnumCaseId *identity = &cases->ptr[i];
            const groove_EnumCase *c = &schema->cases.ptr[i];
            jazz_node_PathCaseKey key = { jz_str_clone(path), *identity };
            bool inserted;
            groove_RecordDescriptor *slot = jz_btree_entry(output, &key, &inserted);
            if (inserted) {
                *slot = c->payload;
            } else {
                jz_str_drop(&key.path);
                if (!groove_RecordDescriptor_eq(*slot, c->payload))
                    return JZN_ISV("same nested payload enum identity has incompatible layout");
            }
            groove_ValueType record = groove_vt_record(c->payload);
            jz_str child = jazz_node__node__global_case_path(path, identity);
            jz_err e = jazz_node__node__physical__collect_nested_payload_enum_layouts(&record, &child, identities, output);
            jz_str_drop(&child);
            groove_ValueType_drop(&record);
            JZ_TRY(e);
        }
        return JZ_OK;
    }
    default:
        return JZ_OK;
    }
}

jz_str jazz_node__node__physical__physical_history_table_name(jazz_types_PhysicalTableId table_id) {
    return jz_format("jazz_physical_%llu_history", (unsigned long long)table_id);
}

/* Split `jazz_physical_{id}_{suffix}`; only the exact constructor spelling is
 * accepted (unsigned decimal, no sign/padding). Allocation-free. */
bool jazz_node__node__physical__split_physical_table_name(jz_slice name, jazz_types_PhysicalTableId *id, jz_slice *suffix) {
    static const char prefix[] = "jazz_physical_";
    const u32 plen = sizeof(prefix) - 1;
    if (name.len < plen || memcmp(name.ptr, prefix, plen) != 0) return false;
    const u8 *digits = name.ptr + plen;
    u32 rest = name.len - plen, ndigits = 0;
    while (ndigits < rest && digits[ndigits] != '_') ndigits++;
    if (ndigits == rest) return false; /* split_once('_') */
    if (ndigits == 0 || !(digits[0] >= '0' && digits[0] <= '9') || (ndigits > 1 && digits[0] == '0')) return false;
    u64 value = 0;
    for (u32 i = 0; i < ndigits; i++) {
        u8 d = digits[i];
        if (d < '0' || d > '9') return false;
        if (value > (UINT64_MAX - (d - '0')) / 10) return false;
        value = value * 10 + (u64)(d - '0');
    }
    *id = value;
    suffix->ptr = digits + ndigits + 1;
    suffix->len = rest - ndigits - 1;
    return true;
}

bool jazz_node__node__physical__physical_version_table_id(jz_slice name, jazz_types_PhysicalTableId *out) {
    jz_slice suffix;
    if (!jazz_node__node__physical__split_physical_table_name(name, out, &suffix)) return false;
    return suffix.len == 7 && memcmp(suffix.ptr, "history", 7) == 0;
}

jz_str jazz_node__node__physical__physical_global_current_table_name(jazz_types_PhysicalTableId table_id) {
    return jz_format("jazz_physical_%llu_global_current", (unsigned long long)table_id);
}
jz_str jazz_node__node__physical__physical_ahead_current_table_name(jazz_types_PhysicalTableId table_id) {
    return jz_format("jazz_physical_%llu_ahead_current", (unsigned long long)table_id);
}
jz_str jazz_node__node__physical__physical_rejected_versions_table_name(jazz_types_PhysicalTableId table_id) {
    return jz_format("jazz_physical_%llu_rejected_versions", (unsigned long long)table_id);
}
/* Durable key/catalogue vocabulary: never rename. */
jz_str jazz_node__node__physical__physical_current_index_name(jazz_types_PhysicalColumnId column_id) {
    return jz_format("by_physical_app_v1_%llu", (unsigned long long)column_id);
}
jz_str jazz_node__node__physical__physical_current_composite_index_name(const jazz_types_PhysicalColumnId *column_ids, u32 n) {
    jz_str out = jz_str_from("by_physical_composite_v1_");
    for (u32 i = 0; i < n; i++) {
        if (i) jz_str_push_char(&out, '_');
        jz_str_appendf(&out, "%llu", (unsigned long long)column_ids[i]);
    }
    return out;
}
jz_str jazz_node__node__physical__physical_user_column_field(jazz_types_PhysicalColumnId column_id) {
    return jz_format("_app_%llu", (unsigned long long)column_id);
}
jz_str jazz_node__node__physical__physical_history_projection_target(jazz_types_SchemaVersionAlias schema_alias, const jz_str *logical_table) {
    return jz_format("schema_%llu_%.*s_history", (unsigned long long)schema_alias, JZN_S(*logical_table));
}
jz_str jazz_node__node__physical__physical_current_projection_target(jazz_types_SchemaVersionAlias schema_alias, const jz_str *logical_table) {
    return jz_format("schema_%llu_%.*s_current", (unsigned long long)schema_alias, JZN_S(*logical_table));
}
