/* physical/storage.c: jazz_node::node::physical (physical/storage.rs).
 * Authored <-> physical column translation at the storage boundary, the
 * cached per-(schema, table, target) physical write plan, and the history /
 * current / rejected-version encoders built from it. */
#include "../physical.h"
#include "../codec.h"

static void jzn_str_drop_v(void *p) { jz_str_drop((jz_str *)p); }

static const jazz_node_TablePhysicalMapping *jzn_mapping(const jazz_node_NodeState *s, jazz_types_SchemaVersionId v, const jz_str *table) {
    const jazz_node_SchemaPhysicalMapping *m = jz_btree_get(&s->catalogue.physical_mappings, &v);
    if (!m) return NULL;
    return jz_btree_get(&m->tables, table);
}

jz_err jazz_node__node__physical__NodeState__authored_column_ids_for_names(const jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *table, const jz_btree *columns /* NULL = None */, bool *some, jz_btree *out) {
    *some = false;
    if (!columns) return JZ_OK;
    const jazz_node_TablePhysicalMapping *mapping = jzn_mapping(s, v, table);
    if (!mapping) return JZN_ISV("authored columns physical table mapping missing");
    jz_btree ids;
    jz_btree_init(&ids, &jzn_desc_u64_set);
    const jz_str *column; const void *unused;
    JZ_BTREE_FOR(columns, column, unused) {
        u64 id;
        if (jz_str_eq_cstr(column, JZN_DELETION_COLUMN_NAME)) {
            id = JZN_DELETION_COLUMN_ID;
        } else {
            const u64 *found = jz_btree_get(&mapping->columns, column);
            if (!found) { jz_btree_drop(&ids); return JZN_ISV("authored column physical mapping missing"); }
            id = *found;
        }
        jz_btree_insert(&ids, &id, NULL, NULL);
    }
    *some = true;
    *out = ids;
    return JZ_OK;
}

jz_err jazz_node__node__physical__NodeState__authored_column_names_for_ids(const jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *table, const jz_btree *columns /* NULL = None */, bool *some, jz_btree *out) {
    *some = false;
    if (!columns) return JZ_OK;
    const jazz_node_TablePhysicalMapping *mapping = jzn_mapping(s, v, table);
    if (!mapping) return JZN_ISV("authored columns physical table mapping missing");
    jz_btree names_by_id;
    jz_btree_init(&names_by_id, &jzn_desc_u64_str);
    const jz_str *name; const u64 *id;
    JZ_BTREE_FOR(&mapping->columns, name, id) {
        jz_str copy = jz_str_clone(name);
        jz_str old;
        if (jz_btree_insert(&names_by_id, id, &copy, &old)) {
            jz_str_drop(&old);
            jz_btree_drop(&names_by_id);
            return JZN_ISV("physical table maps multiple authored columns to one id");
        }
    }
    jz_btree names;
    jz_btree_init(&names, &jzn_desc_str_set);
    const u64 *column; const void *unused;
    JZ_BTREE_FOR(columns, column, unused) {
        jz_str n;
        if (*column == JZN_DELETION_COLUMN_ID) {
            n = jz_str_from(JZN_DELETION_COLUMN_NAME);
        } else {
            const jz_str *found = jz_btree_get(&names_by_id, column);
            if (!found) {
                jz_btree_drop(&names);
                jz_btree_drop(&names_by_id);
                return JZN_ISV("stored authored column id is absent from its schema mapping");
            }
            n = jz_str_clone(found);
        }
        if (jz_btree_contains(&names, &n)) jz_str_drop(&n);
        else jz_btree_insert(&names, &n, NULL, NULL);
    }
    jz_btree_drop(&names_by_id);
    *some = true;
    *out = names;
    return JZ_OK;
}

/* The authored spelling of a physical user cell of `table` under
 * `schema_version`: physical enum tags re-tagged to that schema version's
 * authored tags. A cell without an enum boundary is returned unchanged. */
jz_err jazz_node__node__physical__NodeState__physical_cell_to_authored(jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *table, u32 column, groove_Value value, groove_Value *out) {
    jazz_node_PreparedPhysicalWritePlan *plan;
    jz_err err = jazz_node__node__physical__NodeState__prepared_physical_write_plan(s, v, table, JZN_PWT_HISTORY, &plan);
    if (JZ_IS_ERR(err)) { groove_Value_drop(&value); return err; }
    u32 index = 0;
    bool found = false;
    for (; index < plan->write_fields.len; index++) {
        const jazz_node_PhysicalWriteField *f = &plan->write_fields.ptr[index];
        if (f->tag == JZN_PWF_ENUM && f->column == column) { found = true; break; }
    }
    if (!found) { jz_rc_release(plan); *out = value; return JZ_OK; }
    const groove_ValueType *physical_type = &groove__records__RecordDescriptor__fields(plan->physical_descriptor)->ptr[index].value_type;
    const groove_ValueType *authored_type = &plan->source_table->columns.ptr[column].column_type;
    const jazz_node_TablePhysicalMapping *mapping = jzn_mapping(s, v, table);
    if (!mapping) { err = JZN_ISV("enum cell physical table mapping missing"); goto fail; }
    const u64 *column_id = jz_btree_get(&mapping->columns, &plan->source_table->columns.ptr[column].name);
    if (!column_id) { err = JZN_ISV("enum cell physical column mapping missing"); goto fail; }
    jazz_node_EnumOccurrenceRemaps remaps;
    err = jazz_node__node__physical__NodeState__physical_to_authored_enum_remaps(s, mapping, *column_id, &remaps);
    if (JZ_IS_ERR(err)) goto fail;
    if (value.tag == GROOVE_V_NULLABLE && value.v.nullable && physical_type->tag == GROOVE_VT_NULLABLE) {
        groove_Value inner = *value.v.nullable;
        jz_free(value.v.nullable);
        jz_str root = jz_str_from("root");
        groove_Value remapped;
        /* physical -> authored: the physical type is the "authored" side of the remap */
        err = jazz_node__node__physical__remap_nested_enum_value(inner, physical_type->v.inner, authored_type, &remaps, &root, &remapped);
        jz_str_drop(&root);
        groove_RecursiveEnumRemaps_drop(&remaps);
        jz_rc_release(plan);
        if (JZ_IS_ERR(err)) return err;
        *out = groove_value_some(remapped);
        return JZ_OK;
    }
    groove_RecursiveEnumRemaps_drop(&remaps);
    jz_rc_release(plan);
    *out = value;
    return JZ_OK;
fail:
    jz_rc_release(plan);
    groove_Value_drop(&value);
    return err;
}

/* Translate a logical contribution table at the local storage boundary.
 * Physical ids are node-local and never cross the transaction/wire boundary. */
jz_err jazz_node__node__physical__NodeState__contribution_table_id_for_storage(const jazz_node_NodeState *s, const jz_str *table,
                                                                            jazz_types_PhysicalTableId *out) {
    const jazz_node_TablePhysicalMapping *current = jzn_mapping(s, s->catalogue.local_schema_version_id, table);
    if (current) { *out = current->table_id; return JZ_OK; }
    jz_btree ids;
    jz_btree_init(&ids, &jzn_desc_u64_set);
    const jz_uuid *schema; const jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(&s->catalogue.physical_mappings, schema, mapping) {
        const jazz_node_TablePhysicalMapping *t = jz_btree_get(&mapping->tables, table);
        if (t) jz_btree_insert(&ids, &t->table_id, NULL, NULL);
    }
    jz_err err = JZ_OK;
    if (ids.len == 0) err = JZN_ISV("contribution physical table mapping missing");
    else if (ids.len == 1) { u64 *k; void *v; jz_btree_first(&ids, (void **)&k, &v); *out = *k; }
    else err = JZN_ISV("contribution logical table maps to multiple physical ids");
    jz_btree_drop(&ids);
    return err;
}

/* Resolve an on-disk contribution table id to the active logical name,
 * falling back to an unambiguous retained spelling during recovery. */
jz_err jazz_node__node__physical__NodeState__contribution_table_name_for_storage(const jazz_node_NodeState *s, jazz_types_PhysicalTableId table,
                                                                              jz_str *out) {
    const jazz_node_SchemaPhysicalMapping *local = jz_btree_get(&s->catalogue.physical_mappings, &s->catalogue.local_schema_version_id);
    const jz_str *name; const jazz_node_TablePhysicalMapping *candidate;
    if (local) {
        JZ_BTREE_FOR(&local->tables, name, candidate) {
            if (candidate->table_id == table) { *out = jz_str_clone(name); return JZ_OK; }
        }
    }
    jz_btree names;
    jz_btree_init(&names, &jzn_desc_str_set);
    const jz_uuid *schema; const jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(&s->catalogue.physical_mappings, schema, mapping) {
        JZ_BTREE_FOR(&mapping->tables, name, candidate) {
            if (candidate->table_id != table || jz_btree_contains(&names, name)) continue;
            jz_str copy = jz_str_clone(name);
            jz_btree_insert(&names, &copy, NULL, NULL);
        }
    }
    jz_err err = JZ_OK;
    if (names.len == 0) err = JZN_ISV("stored contribution physical table id is absent from the catalogue");
    else if (names.len == 1) jz_btree_pop_first(&names, out, NULL);
    else err = JZN_ISV("stored contribution physical table id maps to multiple logical names");
    jz_btree_drop(&names);
    return err;
}

/* Translate a logical contribution coordinate at the local storage boundary. */
jz_err jazz_node__node__physical__NodeState__contribution_column_id_for_storage(const jazz_node_NodeState *s, const jz_str *table,
                                                                             const jz_str *column, jazz_types_PhysicalColumnId *out) {
    const jazz_node_TablePhysicalMapping *current = jzn_mapping(s, s->catalogue.local_schema_version_id, table);
    const u64 *cid = current ? jz_btree_get(&current->columns, column) : NULL;
    if (cid) { *out = *cid; return JZ_OK; }
    jz_btree ids;
    jz_btree_init(&ids, &jzn_desc_u64_set);
    const jz_uuid *schema; const jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(&s->catalogue.physical_mappings, schema, mapping) {
        const jazz_node_TablePhysicalMapping *t = jz_btree_get(&mapping->tables, table);
        const u64 *id = t ? jz_btree_get(&t->columns, column) : NULL;
        if (id) jz_btree_insert(&ids, id, NULL, NULL);
    }
    jz_err err = JZ_OK;
    if (ids.len == 0) err = JZN_ISV("contribution physical column mapping missing");
    else if (ids.len == 1) { u64 *k; void *v; jz_btree_first(&ids, (void **)&k, &v); *out = *k; }
    else err = JZN_ISV("contribution logical column maps to multiple physical ids");
    jz_btree_drop(&ids);
    return err;
}

/* Resolve an on-disk contribution column id into the logical name, preferring
 * the active schema so a retained id follows a compatible rename. */
jz_err jazz_node__node__physical__NodeState__contribution_column_name_for_storage(const jazz_node_NodeState *s, const jz_str *table,
                                                                               jazz_types_PhysicalColumnId column, jz_str *out) {
    const jazz_node_TablePhysicalMapping *current = jzn_mapping(s, s->catalogue.local_schema_version_id, table);
    const jz_str *name; const u64 *id;
    if (current) {
        JZ_BTREE_FOR(&current->columns, name, id) {
            if (*id == column) { *out = jz_str_clone(name); return JZ_OK; }
        }
    }
    jz_btree names;
    jz_btree_init(&names, &jzn_desc_str_set);
    const jz_uuid *schema; const jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(&s->catalogue.physical_mappings, schema, mapping) {
        const jazz_node_TablePhysicalMapping *t = jz_btree_get(&mapping->tables, table);
        if (!t) continue;
        JZ_BTREE_FOR(&t->columns, name, id) {
            if (*id != column || jz_btree_contains(&names, name)) continue;
            jz_str copy = jz_str_clone(name);
            jz_btree_insert(&names, &copy, NULL, NULL);
        }
    }
    jz_err err = JZ_OK;
    if (names.len == 0) err = JZN_ISV("stored contribution physical column id is absent from its table mapping");
    else if (names.len == 1) jz_btree_pop_first(&names, out, NULL);
    else err = JZN_ISV("stored contribution physical column id maps to multiple logical names");
    jz_btree_drop(&names);
    return err;
}

/* closures passed to the codec */
static jz_err jzn_resolve_table_id(void *ctx, const jz_str *table, u64 *out) {
    return jazz_node__node__physical__NodeState__contribution_table_id_for_storage(ctx, table, out);
}
static jz_err jzn_resolve_column_id(void *ctx, const jz_str *table, const jz_str *column, u64 *out) {
    return jazz_node__node__physical__NodeState__contribution_column_id_for_storage(ctx, table, column, out);
}
static jz_err jzn_resolve_table_name(void *ctx, u64 table, jz_str *out) {
    return jazz_node__node__physical__NodeState__contribution_table_name_for_storage(ctx, table, out);
}
static jz_err jzn_resolve_column_name(void *ctx, const jz_str *table, u64 column, jz_str *out) {
    return jazz_node__node__physical__NodeState__contribution_column_name_for_storage(ctx, table, column, out);
}

jz_err jazz_node__node__physical__NodeState__contribution_merge_storage_value(const jazz_node_NodeState *s,
    const jazz_model_ContributionMergeProvenance *provenance, groove_Value *out) {
    jzn_contribution_id_resolver r = { (void *)s, jzn_resolve_table_id, jzn_resolve_column_id };
    return jazz_node__node__codec__contribution_merge_storage_value(provenance, &r, out);
}

jz_err jazz_node__node__physical__NodeState__contribution_merge_from_storage_record(const jazz_node_NodeState *s,
    groove_OwnedRecord record, jazz_model_ContributionMergeProvenance *out) {
    jzn_contribution_name_resolver r = { (void *)s, jzn_resolve_table_name, jzn_resolve_column_name };
    return jazz_node__node__codec__contribution_merge_from_storage_record(record, &r, out);
}

jz_err jazz_node__node__physical__NodeState__authored_columns_for_version(jazz_node_NodeState *s, const jazz_node_VersionRow *version,
                                                                       bool *some, jz_btree *out) {
    jazz_types_SchemaVersionId schema_version;
    if (!jazz_node__node__state__catalogue__NodeState__schema_version_for_alias(s, jazz_node__node__codec__VersionRow__schema_version_alias(version), &schema_version))
        return JZN_ISV("authored columns schema version alias missing");
    bool has_ids = false;
    jz_btree ids;
    JZ_TRY(jazz_node__node__codec__VersionRow__authored_column_ids(version, &has_ids, &ids));
    jz_err err = jazz_node__node__physical__NodeState__authored_column_names_for_ids(s, schema_version,
        jazz_node__node__codec__VersionRow__table(version), has_ids ? &ids : NULL, some, out);
    if (has_ids) jz_btree_drop(&ids);
    return err;
}

/* ---- the write plan cache: SchemaVersionId -> String -> PhysicalWriteTarget -> Arc<Plan> ---- */
static void jzn_plan_release_v(void *p) { jz_rc_release(*(void **)p); }
static void jzn_btree_drop_v(void *p) { jz_btree_drop((jz_btree *)p); }
static const jz_btree_desc jzn_desc_target_plan = { sizeof(u32), sizeof(void *), jz_cmp_u32, NULL, jzn_plan_release_v };
static const jz_btree_desc jzn_desc_str_target_map = { sizeof(jz_str), sizeof(jz_btree), jz_cmp_str, jzn_str_drop_v, jzn_btree_drop_v };
const jz_btree_desc jzn_desc_uuid_plan_map = { sizeof(jz_uuid), sizeof(jz_btree), jz_cmp_uuid, NULL, jzn_btree_drop_v };

static void jzn_table_schema_rc_drop(void *p) { jazz_model_TableSchema_drop(p); }

static bool jzn_remaps_has_cases(const jazz_node_EnumOccurrenceRemaps *r) { return r->scalar.len != 0 || r->payload.len != 0; }

static jz_err jazz_node__node__physical__NodeState__prepare_authored_enum_cells_for_physical(const jazz_node_NodeState *s,
    const jazz_model_TableSchema *source_table, const jazz_node_TablePhysicalMapping *source_mapping, jazz_node_EnumOccurrenceRemapsVec *out);

jz_err jazz_node__node__physical__NodeState__prepared_physical_write_plan(jazz_node_NodeState *s, jazz_types_SchemaVersionId schema_version,
    const jz_str *logical_table, jazz_node_PhysicalWriteTarget target, jazz_node_PreparedPhysicalWritePlan **out) {
    const jz_btree *tables = jz_btree_get(&s->catalogue.physical_write_plan_cache, &schema_version);
    const jz_btree *targets = tables ? jz_btree_get(tables, logical_table) : NULL;
    u32 target_key = (u32)target;
    void *const *cached = targets ? jz_btree_get(targets, &target_key) : NULL;
    if (cached) { *out = jz_rc_clone(*cached); return JZ_OK; }

    jazz_model_TableSchema *source_table = jz_rc_new(sizeof(jazz_model_TableSchema), jzn_table_schema_rc_drop);
    jz_err err = jazz_node__node__state__read_payload__NodeState__table_in_schema(s, logical_table, schema_version, source_table);
    if (JZ_IS_ERR(err)) { memset(source_table, 0, sizeof *source_table); jz_rc_release(source_table); return err; }
    const jazz_node_TablePhysicalMapping *sm = jzn_mapping(s, schema_version, logical_table);
    if (!sm) { jz_rc_release(source_table); return JZN_ISV("physical write table mapping missing"); }
    jazz_node_TablePhysicalMapping source_mapping;
    jazz_node_TablePhysicalMapping_clone(&source_mapping, sm);
    jz_str storage_table = {0};
    jzn_StrVec physical_names = {0};
    jazz_node_EnumOccurrenceRemapsVec enum_remaps = {0};
    groove_TableSchema physical_table;
    bool have_physical = false;
    switch (target) {
    case JZN_PWT_HISTORY:
        err = jazz_node__node__physical__physical_history_storage_table(&s->catalogue.physical_mappings, schema_version, logical_table, &storage_table);
        break;
    case JZN_PWT_GLOBAL_CURRENT:
        err = jazz_node__node__physical__physical_current_storage_table(&s->catalogue.physical_mappings, schema_version, logical_table, JZN_PCC_GLOBAL, &storage_table);
        break;
    case JZN_PWT_AHEAD_CURRENT:
        err = jazz_node__node__physical__physical_current_storage_table(&s->catalogue.physical_mappings, schema_version, logical_table, JZN_PCC_AHEAD, &storage_table);
        break;
    }
    if (JZ_IS_ERR(err)) goto fail;
    const groove_TableSchema *found;
    err = groove__db__schema_admission__Database__table_schema(jzn_db(s), &storage_table, &found);
    if (JZ_IS_ERR(err)) goto fail;
    groove_TableSchema_clone(&physical_table, found);
    have_physical = true;
    groove_TableSchema lowered = target == JZN_PWT_HISTORY ? jazz_model__schema__TableSchema__history_storage_table(source_table)
                               : target == JZN_PWT_GLOBAL_CURRENT ? jazz_model__schema__TableSchema__global_current_storage_table(source_table)
                               : jazz_model__schema__TableSchema__ahead_current_storage_table(source_table);
    groove_RecordDescriptor logical_descriptor = groove__schema__TableSchema__record_schema(&lowered);
    groove_TableSchema_drop(&lowered);
    err = target == JZN_PWT_HISTORY ? jazz_node__node__physical__physical_history_field_names(source_table, &source_mapping, &physical_names)
                                    : jazz_node__node__physical__physical_current_field_names(source_table, &source_mapping, &physical_names);
    if (JZ_IS_ERR(err)) goto fail;
    groove_RecordDescriptor physical_descriptor;
    err = jazz_node__node__physical__physical_write_descriptor(logical_descriptor, physical_names.ptr, physical_names.len, &physical_table, &physical_descriptor);
    if (JZ_IS_ERR(err)) goto fail;
    groove_TableSchema history = jazz_model__schema__TableSchema__history_storage_table(source_table);
    groove_RecordDescriptor history_descriptor = groove__schema__TableSchema__record_schema(&history);
    groove_TableSchema_drop(&history);
    err = jazz_node__node__physical__NodeState__prepare_authored_enum_cells_for_physical(s, source_table, &source_mapping, &enum_remaps);
    if (JZ_IS_ERR(err)) goto fail;
    bool current = target != JZN_PWT_HISTORY;
    u32 user_cells = current ? JZN_GCUR_USER_CELLS : JZN_HISTORY_USER_CELLS;
    const groove_DescriptorFieldVec *pfields = groove__records__RecordDescriptor__fields(physical_descriptor);
    const groove_DescriptorFieldVec *hfields = groove__records__RecordDescriptor__fields(history_descriptor);
    jazz_node_PreparedPhysicalWritePlan *plan = jz_rc_new(sizeof(jazz_node_PreparedPhysicalWritePlan), jazz_node_PreparedPhysicalWritePlan_drop);
    memset(plan, 0, sizeof *plan);
    for (u32 index = 0; index < pfields->len; index++) {
        jazz_node_PhysicalWriteField wf = { JZN_PWF_COPY, 0, 0 };
        if (!current && index == JZN_HISTORY_FIELD_UPDATED_BY_IDX) { wf.tag = JZN_PWF_HISTORY_UPDATED_BY; goto push; }
        if (current) {
            switch (index) {
            case JZN_GCUR_FIELD_CREATED_AT_IDX: wf.tag = JZN_PWF_CREATED_AT_MILLIS; goto push;
            case JZN_GCUR_FIELD_UPDATED_BY_IDX: wf.tag = JZN_PWF_UPDATED_BY; goto push;
            case JZN_GCUR_FIELD_UPDATED_AT_IDX: wf.tag = JZN_PWF_UPDATED_AT_MILLIS; goto push;
            case JZN_GCUR_FIELD_GLOBAL_TIME_IDX: wf.tag = JZN_PWF_GLOBAL_TIME; goto push;
            default: break;
            }
        }
        /* A current row has `global_time` where history has none, and ends at
         * `authored_columns`, before history's counter_signs / merge fields. */
        {
            u32 source = current && index > JZN_GCUR_FIELD_GLOBAL_TIME_IDX ? index - 1 : index;
            wf.source = source;
            if (index >= user_cells) {
                u32 column = index - user_cells;
                if (column < enum_remaps.len && jzn_remaps_has_cases(&enum_remaps.ptr[column])) {
                    wf.tag = JZN_PWF_ENUM;
                    wf.column = column;
                    goto push;
                }
            }
            JZ_ASSERT(source < hfields->len, "index out of bounds");
            wf.tag = groove_ValueType_eq(&hfields->ptr[source].value_type, &pfields->ptr[index].value_type) ? JZN_PWF_COPY : JZN_PWF_DECODE;
        }
    push:
        JZ_VEC_PUSH(plan->write_fields, wf);
    }
    plan->storage_table = storage_table;
    plan->source_table = source_table;
    plan->logical_descriptor = logical_descriptor;
    plan->physical_descriptor = physical_descriptor;
    plan->history_descriptor = history_descriptor;
    plan->enum_remaps = enum_remaps;
    {
        bool inserted;
        jz_btree *by_table = jz_btree_entry(&s->catalogue.physical_write_plan_cache, &schema_version, &inserted);
        if (inserted) jz_btree_init(by_table, &jzn_desc_str_target_map);
        jz_btree *by_target = jz_btree_get(by_table, logical_table);
        if (!by_target) {
            jz_str key = jz_str_clone(logical_table);
            jz_btree fresh;
            jz_btree_init(&fresh, &jzn_desc_target_plan);
            jz_btree_insert(by_table, &key, &fresh, NULL);
            by_target = jz_btree_get(by_table, logical_table);
        }
        void *cached_plan = jz_rc_clone(plan);
        void *old;
        if (jz_btree_insert(by_target, &target_key, &cached_plan, &old)) jz_rc_release(old);
    }
    jzn_str_vec_drop(&physical_names);
    groove_TableSchema_drop(&physical_table);
    jazz_node_TablePhysicalMapping_drop(&source_mapping);
    *out = plan;
    return JZ_OK;
fail:
    jz_vec_drop(&enum_remaps, sizeof(jazz_node_EnumOccurrenceRemaps), groove_RecursiveEnumRemaps_drop);
    jzn_str_vec_drop(&physical_names);
    if (have_physical) groove_TableSchema_drop(&physical_table);
    jz_str_drop(&storage_table);
    jazz_node_TablePhysicalMapping_drop(&source_mapping);
    jz_rc_release(source_table);
    return err;
}

jz_err jazz_node__node__physical__NodeState__version_storage_table_for_row(jazz_node_NodeState *s, const jazz_node_VersionRow *version,
                                                                        groove_InternStr *out) {
    jazz_types_SchemaVersionId schema_version;
    if (!jazz_node__node__state__catalogue__NodeState__schema_version_for_alias(s, jazz_node__node__codec__VersionRow__schema_version_alias(version), &schema_version))
        return JZN_ISV("stored row schema version alias missing while resolving storage table");
    jz_str table;
    JZ_TRY(jazz_node__node__physical__physical_history_storage_table(&s->catalogue.physical_mappings, schema_version,
                                                                    jazz_node__node__codec__VersionRow__table(version), &table));
    *out = groove__Intern__new_str(table);
    return JZ_OK;
}

jz_err jazz_node__node__physical__NodeState__version_storage_primary_key(const jazz_node_NodeState *s, const jazz_node_VersionRow *version,
                                                                      jazz_node_PrimaryKeyValue *out) {
    (void)s;
    return jazz_node__node__codec__history_primary_key(version, out);
}

/* For each authored case identity, its position among the physical cases
 * (None when the physical registry lacks it). Shared by the four registry
 * kinds; Rust repeats the closure four times. */
static jz_err jzn_scalar_positions(const jazz_node_GlobalScalarEnumCaseIdVec *authored, const jazz_node_GlobalScalarEnumCaseIdVec *physical,
                                   const char *exhausted, groove_OptU8Vec *out) {
    groove_OptU8Vec tags = {0};
    for (u32 i = 0; i < authored->len; i++) {
        u16 tag = GROOVE_OPT_U8_NONE;
        for (u32 j = 0; j < physical->len; j++) {
            if (jazz_node_GlobalScalarEnumCaseId_cmp(&physical->ptr[j], &authored->ptr[i], NULL) == 0 &&
                physical->ptr[j].introducing_ordinal == authored->ptr[i].introducing_ordinal &&
                jz_uuid_eq(&physical->ptr[j].introducing_schema, &authored->ptr[i].introducing_schema)) {
                if (j > 0xff) { jz_vec_drop(&tags, sizeof(u16), NULL); return JZN_ISV(exhausted); }
                tag = (u16)j;
                break;
            }
        }
        JZ_VEC_PUSH(tags, tag);
    }
    *out = tags;
    return JZ_OK;
}

static jz_err jzn_payload_positions(const jazz_node_GlobalEnumCaseIdVec *authored, const jazz_node_GlobalEnumCaseIdVec *physical,
                                    const char *exhausted, groove_OptU32Vec *out) {
    groove_OptU32Vec tags = {0};
    for (u32 i = 0; i < authored->len; i++) {
        i64 tag = -1;
        for (u32 j = 0; j < physical->len; j++) {
            if (jazz_node_GlobalEnumCaseId_cmp(&physical->ptr[j], &authored->ptr[i], NULL) == 0 &&
                physical->ptr[j].introducing_ordinal == authored->ptr[i].introducing_ordinal &&
                jz_uuid_eq(&physical->ptr[j].introducing_schema, &authored->ptr[i].introducing_schema)) {
                if ((u64)j > 0xffffffffull) { jz_vec_drop(&tags, sizeof(i64), NULL); return JZN_ISV(exhausted); }
                tag = (i64)j;
                break;
            }
        }
        JZ_VEC_PUSH(tags, tag);
    }
    *out = tags;
    return JZ_OK;
}

static void jzn_payload_children(const jazz_node_GlobalEnumCaseIdVec *authored, const jz_str *path, groove_OptStrVec *out) {
    groove_OptStrVec children = {0};
    for (u32 i = 0; i < authored->len; i++) JZ_VEC_PUSH(children, jazz_node__node__global_case_path(path, &authored->ptr[i]));
    *out = children;
}

static void jzn_insert_replacing(jz_btree *map, jz_str key, void *val, usize elem, void (*drop)(void *)) {
    u8 old[sizeof(jz_vec)];
    if (jz_btree_insert(map, &key, val, old)) jz_vec_drop(old, elem, drop);
}

/* Prepare authored-to-physical enum identities once per catalogue write plan.
 * Local ordinals cannot be copied across concurrently introduced cases; the
 * same prepared remaps serve history and current encoders. */
static jz_err jazz_node__node__physical__NodeState__prepare_authored_enum_cells_for_physical(const jazz_node_NodeState *s,
    const jazz_model_TableSchema *source_table, const jazz_node_TablePhysicalMapping *source_mapping, jazz_node_EnumOccurrenceRemapsVec *out) {
    jazz_node_EnumOccurrenceRemapsVec plans = {0};
    jz_vec_reserve(&plans, source_table->columns.len, sizeof(jazz_node_EnumOccurrenceRemaps));
    jz_err err = JZ_OK;
    jz_str root = jz_str_from("root");
    for (u32 c = 0; c < source_table->columns.len && !JZ_IS_ERR(err); c++) {
        const jazz_model_ColumnSchema *column = &source_table->columns.ptr[c];
        const u64 *cid = jz_btree_get(&source_mapping->columns, &column->name);
        if (!cid) { err = JZN_ISV("physical enum write column mapping missing"); break; }
        u64 column_id = *cid;
        jazz_node_EnumOccurrenceRemaps remaps;
        groove_RecursiveEnumRemaps_init(&remaps);
        const jazz_node_GlobalScalarEnumCaseIdVec *scalar_cases = jz_btree_get(&source_mapping->scalar_enum_cases, &column_id);
        if (scalar_cases) {
            jazz_node_GlobalScalarEnumCaseIdVec physical_cases = {0};
            groove_OptU8Vec tags;
            err = jazz_node__node__physical__NodeState__physical_scalar_enum_cases(s, source_mapping->table_id, column_id, &physical_cases);
            if (!JZ_IS_ERR(err)) err = jzn_scalar_positions(scalar_cases, &physical_cases, "physical scalar enum tag exhausted", &tags);
            if (!JZ_IS_ERR(err)) jzn_insert_replacing(&remaps.scalar, jz_str_clone(&root), &tags, sizeof(u16), NULL);
            jz_vec_drop(&physical_cases, sizeof(jazz_node_GlobalScalarEnumCaseId), NULL);
        }
        const jazz_node_GlobalEnumCaseIdVec *payload_cases = JZ_IS_ERR(err) ? NULL : jz_btree_get(&source_mapping->payload_enum_cases, &column_id);
        if (payload_cases) {
            jazz_node_GlobalEnumCaseIdVec physical_cases = {0};
            groove_OptU32Vec tags;
            err = jazz_node__node__physical__NodeState__physical_payload_enum_cases(s, source_mapping->table_id, column_id, &physical_cases);
            if (!JZ_IS_ERR(err)) err = jzn_payload_positions(payload_cases, &physical_cases, "physical payload enum tag exhausted", &tags);
            if (!JZ_IS_ERR(err)) {
                jzn_insert_replacing(&remaps.payload, jz_str_clone(&root), &tags, sizeof(i64), NULL);
                groove_OptStrVec children;
                jzn_payload_children(payload_cases, &root, &children);
                jzn_insert_replacing(&remaps.payload_children, jz_str_clone(&root), &children, sizeof(jz_str), jzn_str_drop_v);
            }
            jz_vec_drop(&physical_cases, sizeof(jazz_node_GlobalEnumCaseId), NULL);
        }
        const jz_btree *nested_scalar = JZ_IS_ERR(err) ? NULL : jz_btree_get(&source_mapping->nested_scalar_enum_cases, &column_id);
        if (nested_scalar) {
            const jz_str *path; const jazz_node_GlobalScalarEnumCaseIdVec *authored_cases;
            JZ_BTREE_FOR(nested_scalar, path, authored_cases) {
                jazz_node_GlobalScalarEnumCaseIdVec physical_cases = {0};
                groove_OptU8Vec tags;
                err = jazz_node__node__physical__NodeState__physical_nested_scalar_enum_cases(s, source_mapping->table_id, column_id, path, &physical_cases);
                if (!JZ_IS_ERR(err)) err = jzn_scalar_positions(authored_cases, &physical_cases, "physical nested scalar enum tag exhausted", &tags);
                jz_vec_drop(&physical_cases, sizeof(jazz_node_GlobalScalarEnumCaseId), NULL);
                if (JZ_IS_ERR(err)) break;
                jzn_insert_replacing(&remaps.scalar, jz_str_clone(path), &tags, sizeof(u16), NULL);
            }
        }
        const jz_btree *nested_payload = JZ_IS_ERR(err) ? NULL : jz_btree_get(&source_mapping->nested_payload_enum_cases, &column_id);
        if (nested_payload) {
            const jz_str *path; const jazz_node_GlobalEnumCaseIdVec *authored_cases;
            JZ_BTREE_FOR(nested_payload, path, authored_cases) {
                jazz_node_GlobalEnumCaseIdVec physical_cases = {0};
                groove_OptU32Vec tags;
                err = jazz_node__node__physical__NodeState__physical_nested_payload_enum_cases(s, source_mapping->table_id, column_id, path, &physical_cases);
                if (!JZ_IS_ERR(err)) err = jzn_payload_positions(authored_cases, &physical_cases, "physical nested payload enum tag exhausted", &tags);
                jz_vec_drop(&physical_cases, sizeof(jazz_node_GlobalEnumCaseId), NULL);
                if (JZ_IS_ERR(err)) break;
                jzn_insert_replacing(&remaps.payload, jz_str_clone(path), &tags, sizeof(i64), NULL);
                groove_OptStrVec children;
                jzn_payload_children(authored_cases, path, &children);
                jzn_insert_replacing(&remaps.payload_children, jz_str_clone(path), &children, sizeof(jz_str), jzn_str_drop_v);
            }
        }
        if (JZ_IS_ERR(err)) { groove_RecursiveEnumRemaps_drop(&remaps); break; }
        JZ_VEC_PUSH(plans, remaps);
    }
    jz_str_drop(&root);
    if (JZ_IS_ERR(err)) {
        jz_vec_drop(&plans, sizeof(jazz_node_EnumOccurrenceRemaps), groove_RecursiveEnumRemaps_drop);
        return err;
    }
    *out = plans;
    return JZ_OK;
}

/* Encode `version` for its history table. `tx_author` is the made_by of the
 * transaction that wrote it: the image stores `updated_by` only when it
 * differs, so a read can always fill the author back in. */
jz_err jazz_node__node__physical__NodeState__version_storage_write_binding(jazz_node_NodeState *s, const jazz_node_VersionRow *version,
    jazz_types_AuthorSubject tx_author, groove_InternStr *table_out, groove_ValidatedVariantRecord *record_out) {
    jazz_types_SchemaVersionId schema_version;
    if (!jazz_node__node__state__catalogue__NodeState__schema_version_for_alias(s, jazz_node__node__codec__VersionRow__schema_version_alias(version), &schema_version))
        return JZN_ISV("stored row schema version alias missing while preparing storage write");
    jazz_node_PreparedPhysicalWritePlan *plan;
    JZ_TRY(jazz_node__node__physical__NodeState__prepared_physical_write_plan(s, schema_version, jazz_node__node__codec__VersionRow__table(version),
                                                                            JZN_PWT_HISTORY, &plan));
    /* Every history write goes through this binding; list the row in its
     * transaction record when the batch is flushed. */
    jazz_types_PhysicalTableId table_id;
    jz_err err = jazz_node__node__physical__NodeState__physical_table_id_for_schema(s, schema_version, jazz_node__node__codec__VersionRow__table(version), &table_id);
    if (!JZ_IS_ERR(err)) {
        jazz_node__node__state__commit__NodeState__mark_tx_touched_row(s, table_id, version);
        groove_ValidatedVariantRecord record;
        err = jazz_node__node__physical__NodeState__encode_physical_version_record(s, plan, version, false, 0, true, tx_author, &record);
        if (!JZ_IS_ERR(err)) {
            *table_out = groove__Intern__new_str(jz_str_clone(&plan->storage_table));
            *record_out = record;
        }
    }
    jz_rc_release(plan);
    return err;
}

/* the `create_with_encoded_fields` closure state */
typedef struct jzn_encode_ctx {
    const jazz_node_PreparedPhysicalWritePlan *plan;
    const jazz_node_VersionRow *version;
    groove_BorrowedRecord input;
    bool matching_layout;
    bool has_global_time;
    jazz_types_GlobalTime global_time;
    bool has_author;
    jazz_types_AuthorSubject tx_author;
} jzn_encode_ctx;

static jz_err jzn_history_updated_by(const jzn_encode_ctx *c, groove_Value *out) {
    if (!c->has_author) return JZN_ISV("a history image is written with its transaction author");
    jazz_types_AuthorSubject updated_by = jazz_node__node__codec__VersionRow__updated_by(c->version);
    groove_Value a, b;
    JZ_TRY(jazz_node__node__codec__row_author_value(updated_by, &a));
    jz_err err = jazz_node__node__codec__row_author_value(c->tx_author, &b);
    if (JZ_IS_ERR(err)) { groove_Value_drop(&a); return err; }
    bool same = groove_Value_eq(&a, &b);
    groove_Value_drop(&a);
    groove_Value_drop(&b);
    if (same) { *out = groove_value_null(); return JZ_OK; }
    return jazz_node__node__codec__history_updated_by_value(updated_by, out);
}

static jz_err jzn_encode_field(void *ctx, u32 index, jz_bytes *output) {
    const jzn_encode_ctx *c = ctx;
    const jazz_node_PreparedPhysicalWritePlan *plan = c->plan;
    const jazz_node_PhysicalWriteField *wf = &plan->write_fields.ptr[index];
    groove_Value value;
    switch (wf->tag) {
    case JZN_PWF_COPY:
        if (c->matching_layout) {
            u32 start, end;
            JZ_TRY(groove__records__RecordDescriptor__field_span(*c->input.descriptor, c->input.raw, wf->source, &start, &end));
            jz_bytes_push(output, c->input.raw.ptr + start, end - start);
            return JZ_OK;
        }
        /* fall through */
    case JZN_PWF_DECODE:
        JZ_TRY(groove__records__BorrowedRecord__get_idx(&c->input, wf->source, &value));
        break;
    case JZN_PWF_CREATED_AT_MILLIS:
        value = groove_value_u64(jazz_types__time__TxTime__physical_ms(jazz_node__node__codec__VersionRow__created_at(c->version)));
        break;
    case JZN_PWF_UPDATED_AT_MILLIS:
        value = groove_value_u64(jazz_types__time__TxTime__physical_ms(jazz_node__node__codec__VersionRow__updated_at(c->version)));
        break;
    case JZN_PWF_UPDATED_BY:
        JZ_TRY(jazz_node__node__codec__row_author_value(jazz_node__node__codec__VersionRow__updated_by(c->version), &value));
        break;
    case JZN_PWF_HISTORY_UPDATED_BY:
        JZ_TRY(jzn_history_updated_by(c, &value));
        break;
    case JZN_PWF_GLOBAL_TIME:
        value = c->has_global_time ? groove_value_some(groove_value_u64(c->global_time)) : groove_value_null();
        break;
    case JZN_PWF_ENUM: {
        groove_Value v;
        JZ_TRY(groove__records__BorrowedRecord__get_idx(&c->input, wf->source, &v));
        const groove_ValueType *physical = &groove__records__RecordDescriptor__fields(plan->physical_descriptor)->ptr[index].value_type;
        if (v.tag == GROOVE_V_NULLABLE && v.v.nullable && physical->tag == GROOVE_VT_NULLABLE) {
            groove_Value inner = *v.v.nullable;
            jz_free(v.v.nullable);
            jz_str root = jz_str_from("root");
            groove_Value remapped;
            jz_err err = jazz_node__node__physical__remap_nested_enum_value(inner, &plan->source_table->columns.ptr[wf->column].column_type,
                                                                            physical->v.inner, &plan->enum_remaps.ptr[wf->column], &root, &remapped);
            jz_str_drop(&root);
            if (JZ_IS_ERR(err)) return err;
            value = groove_value_some(remapped);
        } else {
            value = v;
        }
        break;
    }
    default: JZ_UNREACHABLE();
    }
    jz_err err = groove__records__RecordDescriptor__encode_field_into(plan->physical_descriptor, index, &value, output);
    groove_Value_drop(&value);
    return err;
}

/* Reuse encoded history fields; only current timestamps and enum tags need
 * representation changes. */
jz_err jazz_node__node__physical__NodeState__encode_physical_version_record(const jazz_node_NodeState *s, const jazz_node_PreparedPhysicalWritePlan *plan,
    const jazz_node_VersionRow *version, bool has_global_time, jazz_types_GlobalTime global_time, bool has_author, jazz_types_AuthorSubject tx_author,
    groove_ValidatedVariantRecord *out) {
    (void)s;
    jzn_encode_ctx c;
    c.plan = plan;
    c.version = version;
    c.input = groove_OwnedRecord_borrowed(&version->record);
    c.matching_layout = groove_RecordDescriptor_eq(*c.input.descriptor, plan->history_descriptor);
    c.has_global_time = has_global_time;
    c.global_time = global_time;
    c.has_author = has_author;
    c.tx_author = tx_author;
    u32 tag;
    JZ_TRY(jazz_node__node__physical__groove_variant_tag(jazz_node__node__codec__VersionRow__schema_version_alias(version), &tag));
    return groove__records__ValidatedVariantRecord__create_with_encoded_fields(tag, plan->physical_descriptor, c.input.raw.len + 16,
                                                                              jzn_encode_field, &c, out);
}

jz_err jazz_node__node__physical__NodeState__rejected_version_storage_write_binding(const jazz_node_NodeState *s, const jazz_node_VersionRow *version,
    const groove_OwnedRecord *logical_record, groove_InternStr *table_out, groove_VariantRecord *record_out) {
    jazz_types_SchemaVersionId schema_version;
    if (!jazz_node__node__state__catalogue__NodeState__schema_version_for_alias(s, jazz_node__node__codec__VersionRow__schema_version_alias(version), &schema_version))
        return JZN_ISV("rejected row schema version alias missing");
    jazz_node_PhysicalHistoryBinding binding;
    JZ_TRY(jazz_node__node__physical__physical_rejected_version_binding(&s->catalogue.catalogue_schemas, &s->catalogue.physical_mappings,
                                                                       schema_version, jazz_node__node__codec__VersionRow__table(version), &binding));
    u32 tag;
    jz_err err = jazz_node__node__physical__groove_variant_tag(jazz_node__node__codec__VersionRow__schema_version_alias(version), &tag);
    if (JZ_IS_ERR(err)) { jz_str_drop(&binding.storage_table); return err; }
    record_out->variant_tag = tag;
    record_out->record.raw = jz_bytes_clone(&logical_record->raw);
    record_out->record.descriptor = binding.descriptor;
    *table_out = groove__Intern__new_str(binding.storage_table);
    return JZ_OK;
}
