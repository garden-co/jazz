/* physical/projections.c: jazz_node::node::physical (physical/projections.rs).
 * Physical table resolution, source graphs and the registration of every
 * physical -> logical variant projection (history, current, query-local enum
 * targets and raw current-winner targets). Rust's async fns are synchronous
 * here (the storage boundary is synchronous in the port). */
#include "../physical.h"
#include "../codec.h"

static void jzn_str_drop_v(void *p) { jz_str_drop((jz_str *)p); }

/* ---- small shared helpers ---- */
static const jazz_node_TablePhysicalMapping *jzn_mapping(const jazz_node_NodeState *s, jazz_types_SchemaVersionId v, const jz_str *table) {
    const jazz_node_SchemaPhysicalMapping *m = jz_btree_get(&s->catalogue.physical_mappings, &v);
    if (!m) return NULL;
    return jz_btree_get(&m->tables, table);
}

static jz_err jzn_alias(const jazz_node_NodeState *s, jazz_types_SchemaVersionId v, const char *missing, jazz_types_SchemaVersionAlias *out) {
    const jazz_types_SchemaVersionAlias *a = jz_btree_get(&s->catalogue.schema_version_aliases, &v);
    if (!a) return JZN_ISV(missing);
    *out = *a;
    return JZ_OK;
}

static jz_err jzn_descriptor_names(groove_RecordDescriptor d, jzn_StrVec *out) {
    const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(d);
    jzn_StrVec names = {0};
    for (u32 i = 0; i < fields->len; i++) {
        if (!fields->ptr[i].has_name) {
            jzn_str_vec_drop(&names);
            return JZN_ISV("physical current winner field unnamed");
        }
        JZ_VEC_PUSH(names, jz_str_clone(&fields->ptr[i].name));
    }
    *out = names;
    return JZ_OK;
}

static bool jzn_str_vec_eq(const jzn_StrVec *a, const jzn_StrVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!jz_str_eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}

/* (schema_version, logical_table, mapping) triples cloned out of the catalogue. */
typedef struct jzn_src {
    jazz_types_SchemaVersionId schema;
    jz_str table;
    jazz_node_TablePhysicalMapping mapping;
} jzn_src;
typedef JZ_VEC(jzn_src) jzn_src_vec;
static void jzn_src_drop(void *p) {
    jzn_src *s = p;
    jz_str_drop(&s->table);
    jazz_node_TablePhysicalMapping_drop(&s->mapping);
}
static void jzn_src_vec_drop(jzn_src_vec *v) { jz_vec_drop(v, sizeof(jzn_src), jzn_src_drop); }

static void jzn_collect_sources(const jazz_node_NodeState *s, bool filter, jazz_types_PhysicalTableId table_id, jzn_src_vec *out) {
    jzn_src_vec v = {0};
    const jz_uuid *schema;
    const jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(&s->catalogue.physical_mappings, schema, mapping) {
        const jz_str *name;
        const jazz_node_TablePhysicalMapping *table;
        JZ_BTREE_FOR(&mapping->tables, name, table) {
            if (filter && table->table_id != table_id) continue;
            jzn_src e;
            e.schema = *schema;
            e.table = jz_str_clone(name);
            jazz_node_TablePhysicalMapping_clone(&e.mapping, table);
            JZ_VEC_PUSH(v, e);
        }
    }
    *out = v;
}

/* `variant_cases.is_empty() ? [(groove_variant_tag(alias), None)] : cases` */
typedef struct jzn_case { u32 tag; const jz_btree *present; } jzn_case;
typedef JZ_VEC(jzn_case) jzn_case_vec;
static jz_err jzn_cases(const jzn_src *src, jazz_types_SchemaVersionAlias alias, jzn_case_vec *out) {
    jzn_case_vec cases = {0};
    if (src->mapping.variant_cases.len == 0) {
        jzn_case c;
        JZ_TRY(jazz_node__node__physical__groove_variant_tag(alias, &c.tag));
        c.present = NULL;
        JZ_VEC_PUSH(cases, c);
    } else {
        for (u32 i = 0; i < src->mapping.variant_cases.len; i++) {
            jzn_case c = { src->mapping.variant_cases.ptr[i].tag, &src->mapping.variant_cases.ptr[i].fields };
            JZ_VEC_PUSH(cases, c);
        }
    }
    *out = cases;
    return JZ_OK;
}

static jz_err jzn_cases_for(const jazz_node_NodeState *s, const jzn_src *src, const char *alias_missing, jzn_case_vec *out) {
    jazz_types_SchemaVersionAlias alias;
    JZ_TRY(jzn_alias(s, src->schema, alias_missing, &alias));
    return jzn_cases(src, alias, out);
}

/* forward decls of the private projection-case builders */
static jz_err jzn_content_projection_case(jazz_node_NodeState *s, jazz_types_SchemaVersionId source_schema, const jz_str *source_table_name,
    const jazz_node_TablePhysicalMapping *source_mapping, jazz_types_SchemaVersionId target_schema, const jz_str *target_table_name,
    jazz_node_ContentProjectionShape shape, const jz_btree *present, const jz_btree *required_enum_columns, bool *some, groove_ProjectFieldVec *out);

/* ======================= table id resolution ======================= */

jz_err jazz_node__node__physical__NodeState__physical_table_id_for_schema(jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
                                                                       const jz_str *logical_table, jazz_types_PhysicalTableId *out) {
    const jazz_model_TableSchema *t;
    JZ_TRY(jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, logical_table, v, &t));
    const jazz_node_TablePhysicalMapping *m = jzn_mapping(s, v, logical_table);
    if (!m) return JZN_ISV("physical table mapping missing");
    *out = m->table_id;
    return JZ_OK;
}

/* The table id of a per-table physical publication name, exactly the inverse
 * of the `physical_*_table_name` spellings consulted by targeted refresh. */
static bool jazz_node__node__physical__physical_table_id_for_publication_name(jz_slice name, u64 *out) {
    static const char *const SUFFIXES[4] = { "history", "global_current", "ahead_current", "rejected_versions" };
    jazz_types_PhysicalTableId id;
    jz_slice suffix;
    if (!jazz_node__node__physical__split_physical_table_name(name, &id, &suffix)) return false;
    for (u32 i = 0; i < 4; i++) {
        u32 n = (u32)strlen(SUFFIXES[i]);
        if (suffix.len == n && memcmp(suffix.ptr, SUFFIXES[i], n) == 0) { *out = id; return true; }
    }
    return false;
}

static u64 jzn_hash_str_v(const void *k) { return jz_str_hash((const jz_str *)k); }
static bool jzn_eq_str_v(const void *a, const void *b) { return jz_str_eq((const jz_str *)a, (const jz_str *)b); }
static u64 jzn_hash_u64_v(const void *k) { return jz_hash_u64(*(const u64 *)k); }
static bool jzn_eq_u64_v(const void *a, const void *b) { return *(const u64 *)a == *(const u64 *)b; }
const jz_hmap_desc jzn_hdesc_str_set = { sizeof(jz_str), 0, jzn_hash_str_v, jzn_eq_str_v, jzn_str_drop_v, NULL };
static const jz_hmap_desc jzn_hdesc_u64_set = { sizeof(u64), 0, jzn_hash_u64_v, jzn_eq_u64_v, NULL, NULL };

/* Resolve storage publication names to logical tables across every known
 * schema mapping. `changed_tables` and `out` are HashSet<String>. */
void jazz_node__node__physical__NodeState__logical_table_names_for_storage_tables(const jazz_node_NodeState *s,
                                                                                const jz_hmap *changed_tables, jz_hmap *out) {
    jz_hmap changed_table_ids;
    jz_hmap_init(&changed_table_ids, &jzn_hdesc_u64_set);
    const jz_str *name;
    void *unused;
    JZ_HMAP_FOR(changed_tables, name, unused) {
        u64 id;
        if (jazz_node__node__physical__physical_table_id_for_publication_name(jz_str_slice(name), &id))
            jz_hmap_insert(&changed_table_ids, &id, NULL, NULL);
    }
    jz_hmap_init(out, &jzn_hdesc_str_set);
    const jz_uuid *schema;
    const jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(&s->catalogue.physical_mappings, schema, mapping) {
        const jz_str *logical_table;
        const jazz_node_TablePhysicalMapping *table;
        JZ_BTREE_FOR(&mapping->tables, logical_table, table) {
            bool changed = jz_hmap_get(&changed_table_ids, &table->table_id) != NULL;
            if (changed) {
                jz_str copy = jz_str_clone(logical_table);
                bool replaced = jz_hmap_insert(out, &copy, NULL, NULL);
                (void)replaced;
            }
        }
    }
    jz_hmap_drop(&changed_table_ids);
}

jz_err jazz_node__node__physical__NodeState__physical_table_id_for_version(jazz_node_NodeState *s, const jazz_node_VersionRow *version,
                                                                        jazz_types_PhysicalTableId *out) {
    jazz_types_SchemaVersionId schema_version;
    if (!jazz_node__node__state__catalogue__NodeState__schema_version_for_alias(s, jazz_node__node__codec__VersionRow__schema_version_alias(version), &schema_version))
        return JZN_ISV("stored row schema version alias missing while resolving physical table");
    return jazz_node__node__physical__NodeState__physical_table_id_for_schema(s, schema_version, jazz_node__node__codec__VersionRow__table(version), out);
}

jz_err jazz_node__node__physical__NodeState__physical_current_table_for_schema(jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
                                                                            const jz_str *logical_table, jazz_node_PhysicalCurrentClass cls, jz_str *out) {
    jazz_types_PhysicalTableId table_id;
    JZ_TRY(jazz_node__node__physical__NodeState__physical_table_id_for_schema(s, v, logical_table, &table_id));
    *out = cls == JZN_PCC_GLOBAL ? jazz_node__node__physical__physical_global_current_table_name(table_id)
                                 : jazz_node__node__physical__physical_ahead_current_table_name(table_id);
    return JZ_OK;
}

/* ======================= source graphs ======================= */

jz_err jazz_node__node__physical__NodeState__physical_current_source_graph(const jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
                                                                        const jz_str *logical_table, jazz_node_PhysicalCurrentClass cls, groove_GraphBuilder **out) {
    jazz_types_SchemaVersionAlias alias;
    JZ_TRY(jzn_alias(s, v, "physical current source schema alias missing", &alias));
    jz_str storage_table;
    JZ_TRY(jazz_node__node__physical__physical_current_source_table(&s->catalogue.catalogue_schemas, &s->catalogue.physical_mappings,
                                                                   v, logical_table, cls, &storage_table));
    *out = groove__db__query__GraphBuilder__variant_source_scan(storage_table,
        jazz_node__node__physical__physical_current_projection_target(alias, logical_table),
        jazz_node__node__physical__shared_branch_scan(NULL));
    return JZ_OK;
}

/* System fields of the physical current descriptor of one table + the
 * mapping, shared by the marker graphs. */
static jz_err jzn_marker_fields(jazz_node_NodeState *s, jazz_types_SchemaVersionId v, const jz_str *logical_table,
                                jazz_node_TablePhysicalMapping *mapping_out, jzn_StrVec *fields_out) {
    const jazz_node_TablePhysicalMapping *m = jzn_mapping(s, v, logical_table);
    if (!m) return JZN_ISV("physical current marker mapping missing");
    jazz_node_TablePhysicalMapping mapping;
    jazz_node_TablePhysicalMapping_clone(&mapping, m);
    const jazz_model_TableSchema *table;
    jz_err err = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, logical_table, v, &table);
    groove_RecordDescriptor d;
    if (!JZ_IS_ERR(err)) err = jazz_node__node__physical__physical_current_descriptor(table, &mapping, &d);
    if (!JZ_IS_ERR(err)) err = jzn_descriptor_names(d, fields_out);
    if (JZ_IS_ERR(err)) { jazz_node_TablePhysicalMapping_drop(&mapping); return err; }
    *mapping_out = mapping;
    return JZ_OK;
}

/* Read raw row images of one branch through the physical winner projection,
 * optionally under a `scan` below the branch key. Deletion markers need only
 * system fields, so they never pass through the read schema's enum lens. */
jz_err jazz_node__node__physical__NodeState__physical_current_marker_source_graph(jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *logical_table, jazz_node_PhysicalCurrentClass cls, const jazz_model_BranchKey *branch_key,
    const groove_StaticScanSpec *scan, groove_GraphBuilder **out) {
    jazz_node_TablePhysicalMapping mapping;
    jzn_StrVec physical_fields;
    JZ_TRY(jzn_marker_fields(s, v, logical_table, &mapping, &physical_fields));
    jz_str storage_table;
    jz_err err = jazz_node__node__physical__physical_current_source_table(&s->catalogue.catalogue_schemas, &s->catalogue.physical_mappings,
                                                                         v, logical_table, cls, &storage_table);
    if (!JZ_IS_ERR(err))
        *out = groove__db__query__GraphBuilder__variant_source_scan(storage_table,
            jazz_node__node__physical__physical_current_winner_projection_target(mapping.table_id, physical_fields.ptr, physical_fields.len),
            jazz_node__node__physical__branch_scan(branch_key, scan));
    jzn_str_vec_drop(&physical_fields);
    jazz_node_TablePhysicalMapping_drop(&mapping);
    return err;
}

/* The global current table and its system-field winner projection target. */
static jz_err jazz_node__node__physical__NodeState__physical_global_marker_target(jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
                                                                                const jz_str *logical_table, jz_str *table_out, jz_str *target_out) {
    jazz_node_TablePhysicalMapping mapping;
    jzn_StrVec physical_fields;
    JZ_TRY(jzn_marker_fields(s, v, logical_table, &mapping, &physical_fields));
    *table_out = jazz_node__node__physical__physical_global_current_table_name(mapping.table_id);
    *target_out = jazz_node__node__physical__physical_current_winner_projection_target(mapping.table_id, physical_fields.ptr, physical_fields.len);
    jzn_str_vec_drop(&physical_fields);
    jazz_node_TablePhysicalMapping_drop(&mapping);
    return JZ_OK;
}

/* Read the global winners that a capped composite index scan names. */
jz_err jazz_node__node__physical__NodeState__physical_global_marker_index_graph(jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *logical_table, jz_str index, groove_StaticScanSpec scan, groove_GraphBuilder **out) {
    jz_str table, target;
    jz_err err = jazz_node__node__physical__NodeState__physical_global_marker_target(s, v, logical_table, &table, &target);
    if (JZ_IS_ERR(err)) { jz_str_drop(&index); groove_StaticScanSpec_drop(&scan); return err; }
    *out = groove__db__query__GraphBuilder__variant_index_scan(table, index, target, scan);
    return JZ_OK;
}

/* Read the global winners that a capped primary-key scan names. */
jz_err jazz_node__node__physical__NodeState__physical_global_marker_page_graph(jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *logical_table, groove_StaticScanSpec scan, groove_GraphBuilder **out) {
    jz_str table, target;
    jz_err err = jazz_node__node__physical__NodeState__physical_global_marker_target(s, v, logical_table, &table, &target);
    if (JZ_IS_ERR(err)) { groove_StaticScanSpec_drop(&scan); return err; }
    *out = groove__db__query__GraphBuilder__variant_source_scan(table, target, scan);
    return JZ_OK;
}

jz_err jazz_node__node__physical__NodeState__physical_current_source_graph_with_projection_target(const jazz_node_NodeState *s,
    jazz_types_SchemaVersionId v, const jz_str *logical_table, jazz_node_PhysicalCurrentClass cls, jz_str projection_target, groove_GraphBuilder **out) {
    jz_str storage_table;
    jz_err err = jazz_node__node__physical__physical_current_source_table(&s->catalogue.catalogue_schemas, &s->catalogue.physical_mappings,
                                                                         v, logical_table, cls, &storage_table);
    if (JZ_IS_ERR(err)) { jz_str_drop(&projection_target); return err; }
    *out = groove__db__query__GraphBuilder__variant_source_scan(storage_table, projection_target, jazz_node__node__physical__shared_branch_scan(NULL));
    return JZ_OK;
}

/* Read one physical current class of one branch key, optionally under a scan. */
jz_err jazz_node__node__physical__NodeState__physical_current_branch_source_graph_with_projection_target(const jazz_node_NodeState *s,
    jazz_types_SchemaVersionId v, const jz_str *logical_table, jazz_node_PhysicalCurrentClass cls, jz_str projection_target,
    const jazz_model_BranchKey *branch_key, const groove_StaticScanSpec *scan, groove_GraphBuilder **out) {
    jz_str storage_table;
    jz_err err = jazz_node__node__physical__physical_current_source_table(&s->catalogue.catalogue_schemas, &s->catalogue.physical_mappings,
                                                                         v, logical_table, cls, &storage_table);
    if (JZ_IS_ERR(err)) { jz_str_drop(&projection_target); return err; }
    *out = groove__db__query__GraphBuilder__variant_source_scan(storage_table, projection_target, jazz_node__node__physical__branch_scan(branch_key, scan));
    return JZ_OK;
}

jz_err jazz_node__node__physical__NodeState__physical_current_source_scan_graph(const jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *logical_table, jazz_node_PhysicalCurrentClass cls, groove_StaticScanSpec scan, groove_GraphBuilder **out) {
    jazz_types_SchemaVersionAlias alias;
    jz_err err = jzn_alias(s, v, "physical current source schema alias missing", &alias);
    jz_str storage_table;
    if (!JZ_IS_ERR(err))
        err = jazz_node__node__physical__physical_current_source_table(&s->catalogue.catalogue_schemas, &s->catalogue.physical_mappings,
                                                                      v, logical_table, cls, &storage_table);
    if (JZ_IS_ERR(err)) { groove_StaticScanSpec_drop(&scan); return err; }
    *out = groove__db__query__GraphBuilder__variant_source_scan(storage_table,
        jazz_node__node__physical__physical_current_projection_target(alias, logical_table),
        jazz_node__node__physical__shared_branch_scan(&scan));
    return JZ_OK;
}

jz_err jazz_node__node__physical__NodeState__physical_current_source_scan_graph_with_projection_target(const jazz_node_NodeState *s,
    jazz_types_SchemaVersionId v, const jz_str *logical_table, jazz_node_PhysicalCurrentClass cls, jz_str projection_target,
    groove_StaticScanSpec scan, groove_GraphBuilder **out) {
    jz_str storage_table;
    jz_err err = jazz_node__node__physical__physical_current_source_table(&s->catalogue.catalogue_schemas, &s->catalogue.physical_mappings,
                                                                         v, logical_table, cls, &storage_table);
    if (JZ_IS_ERR(err)) { jz_str_drop(&projection_target); groove_StaticScanSpec_drop(&scan); return err; }
    *out = groove__db__query__GraphBuilder__variant_source_scan(storage_table, projection_target, jazz_node__node__physical__shared_branch_scan(&scan));
    return JZ_OK;
}

jz_err jazz_node__node__physical__NodeState__logical_table_for_physical_alias(const jazz_node_NodeState *s, jazz_types_PhysicalTableId table_id,
                                                                           jazz_types_SchemaVersionAlias alias, jz_str *out) {
    jazz_types_SchemaVersionId schema_version;
    if (!jazz_node__node__state__catalogue__NodeState__schema_version_for_alias(s, alias, &schema_version))
        return JZN_ISV("physical row schema version alias missing");
    const jazz_node_SchemaPhysicalMapping *mapping = jz_btree_get(&s->catalogue.physical_mappings, &schema_version);
    if (mapping) {
        const jz_str *logical_table;
        const jazz_node_TablePhysicalMapping *m;
        JZ_BTREE_FOR(&mapping->tables, logical_table, m) {
            if (m->table_id == table_id) { *out = jz_str_clone(logical_table); return JZ_OK; }
        }
    }
    return JZN_ISV("physical row logical table mapping missing");
}

void jazz_node__node__physical__NodeState__physical_table_ids(const jazz_node_NodeState *s, jz_btree *out) {
    jz_btree_init(out, &jzn_desc_u64_set);
    const jz_uuid *schema;
    const jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(&s->catalogue.physical_mappings, schema, mapping) {
        const jz_str *name;
        const jazz_node_TablePhysicalMapping *table;
        JZ_BTREE_FOR(&mapping->tables, name, table) jz_btree_insert(out, &table->table_id, NULL, NULL);
    }
}

jz_err jazz_node__node__physical__NodeState__physical_history_source_graph(const jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
                                                                        const jz_str *logical_table, groove_GraphBuilder **out) {
    jazz_types_SchemaVersionAlias alias;
    JZ_TRY(jzn_alias(s, v, "physical history source schema alias missing", &alias));
    jz_str storage_table;
    JZ_TRY(jazz_node__node__physical__physical_history_source_table(&s->catalogue.catalogue_schemas, &s->catalogue.schema_version_aliases,
                                                                   &s->catalogue.physical_mappings, v, logical_table, &storage_table));
    *out = groove__db__query__GraphBuilder__variant_source(storage_table, jazz_node__node__physical__physical_history_projection_target(alias, logical_table));
    return JZ_OK;
}

/* ======================= projection registration ======================= */

static jz_err jazz_node__node__physical__NodeState__register_physical_history_variant_projections(jazz_node_NodeState *s) {
    jzn_src_vec targets;
    jzn_collect_sources(s, false, 0, &targets);
    jz_err err = JZ_OK;
    for (u32 t = 0; t < targets.len && !JZ_IS_ERR(err); t++) {
        const jzn_src *target = &targets.ptr[t];
        jazz_types_SchemaVersionAlias target_alias;
        err = jzn_alias(s, target->schema, "physical projection target schema alias missing", &target_alias);
        if (JZ_IS_ERR(err)) break;
        const jazz_model_TableSchema *target_table;
        err = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, &target->table, target->schema, &target_table);
        if (JZ_IS_ERR(err)) break;
        jz_str projection_target = jazz_node__node__physical__physical_history_projection_target(target_alias, &target->table);
        groove_TableSchema history = jazz_model__schema__TableSchema__history_storage_table(target_table);
        groove_RecordDescriptor logical_output = groove__schema__TableSchema__record_schema(&history);
        groove_TableSchema_drop(&history);
        jzn_StrVec physical_names = {0};
        err = jazz_node__node__physical__physical_history_field_names(target_table, &target->mapping, &physical_names);
        jzn_src_vec sources = {0};
        if (!JZ_IS_ERR(err)) jzn_collect_sources(s, true, target->mapping.table_id, &sources);
        jz_str storage_table = jazz_node__node__physical__physical_history_table_name(target->mapping.table_id);
        const groove_TableSchema *physical = NULL;
        groove_RecordDescriptor output;
        if (!JZ_IS_ERR(err)) err = groove__db__schema_admission__Database__table_schema(jzn_db(s), &storage_table, &physical);
        if (!JZ_IS_ERR(err)) err = jazz_node__node__physical__widened_projection_descriptor(logical_output, physical_names.ptr, physical_names.len, physical, &output);
        if (!JZ_IS_ERR(err)) err = groove__db__schema_admission__Database__define_variant_projection(jzn_db(s), &storage_table, &projection_target, output);
        for (u32 i = 0; i < sources.len && !JZ_IS_ERR(err); i++) {
            const jzn_src *src = &sources.ptr[i];
            jzn_case_vec cases = {0};
            err = jzn_cases_for(s, src, "physical projection source schema alias missing", &cases);
            for (u32 c = 0; c < cases.len && !JZ_IS_ERR(err); c++) {
                bool some = false;
                groove_ProjectFieldVec fields = {0};
                err = jzn_content_projection_case(s, src->schema, &src->table, &src->mapping, target->schema, &target->table,
                                                  JZN_CPS_HISTORY, cases.ptr[c].present, NULL, &some, &fields);
                if (JZ_IS_ERR(err)) break;
                if (!some) {
                    err = groove__db__schema_admission__Database__register_variant_ignore_case(jzn_db(s), &storage_table, &projection_target, cases.ptr[c].tag);
                    continue;
                }
                err = groove__db__schema_admission__Database__register_variant_case(jzn_db(s), &storage_table, &projection_target, cases.ptr[c].tag, fields);
            }
            jz_vec_drop(&cases, sizeof(jzn_case), NULL);
        }
        jz_str_drop(&storage_table);
        jzn_src_vec_drop(&sources);
        jzn_str_vec_drop(&physical_names);
        jz_str_drop(&projection_target);
    }
    jzn_src_vec_drop(&targets);
    return err;
}

static jz_err jazz_node__node__physical__NodeState__register_physical_current_variant_projections(jazz_node_NodeState *s) {
    jzn_src_vec targets;
    jzn_collect_sources(s, false, 0, &targets);
    jz_err err = JZ_OK;
    for (u32 t = 0; t < targets.len && !JZ_IS_ERR(err); t++) {
        const jzn_src *target = &targets.ptr[t];
        jazz_types_SchemaVersionAlias target_alias;
        err = jzn_alias(s, target->schema, "physical current projection target schema alias missing", &target_alias);
        if (JZ_IS_ERR(err)) break;
        jazz_model_TableSchema target_table;
        err = jazz_node__node__state__read_payload__NodeState__table_in_schema(s, &target->table, target->schema, &target_table);
        if (JZ_IS_ERR(err)) break;
        jz_str projection_target = jazz_node__node__physical__physical_current_projection_target(target_alias, &target->table);
        jz_str storage_tables[2] = {
            jazz_node__node__physical__physical_global_current_table_name(target->mapping.table_id),
            jazz_node__node__physical__physical_ahead_current_table_name(target->mapping.table_id),
        };
        for (u32 k = 0; k < 2 && !JZ_IS_ERR(err); k++) {
            groove_TableSchema gc = jazz_model__schema__TableSchema__global_current_storage_table(&target_table);
            groove_RecordDescriptor logical_output = groove__schema__TableSchema__record_schema(&gc);
            groove_TableSchema_drop(&gc);
            jzn_StrVec physical_names = {0};
            err = jazz_node__node__physical__physical_current_field_names(&target_table, &target->mapping, &physical_names);
            const groove_TableSchema *physical = NULL;
            groove_RecordDescriptor output;
            if (!JZ_IS_ERR(err)) err = groove__db__schema_admission__Database__table_schema(jzn_db(s), &storage_tables[k], &physical);
            if (!JZ_IS_ERR(err)) err = jazz_node__node__physical__widened_projection_descriptor(logical_output, physical_names.ptr, physical_names.len, physical, &output);
            if (!JZ_IS_ERR(err)) err = groove__db__schema_admission__Database__define_variant_projection(jzn_db(s), &storage_tables[k], &projection_target, output);
            jzn_str_vec_drop(&physical_names);
        }
        jzn_src_vec sources = {0};
        if (!JZ_IS_ERR(err)) jzn_collect_sources(s, true, target->mapping.table_id, &sources);
        for (u32 i = 0; i < sources.len && !JZ_IS_ERR(err); i++) {
            const jzn_src *src = &sources.ptr[i];
            jzn_case_vec cases = {0};
            err = jzn_cases_for(s, src, "physical current projection source schema alias missing", &cases);
            for (u32 c = 0; c < cases.len && !JZ_IS_ERR(err); c++) {
                bool some = false;
                groove_ProjectFieldVec fields = {0};
                err = jzn_content_projection_case(s, src->schema, &src->table, &src->mapping, target->schema, &target->table,
                                                  JZN_CPS_CURRENT, cases.ptr[c].present, NULL, &some, &fields);
                for (u32 k = 0; k < 2 && !JZ_IS_ERR(err); k++) {
                    if (some) {
                        groove_ProjectFieldVec copy;
                        groove_ProjectFieldVec_clone(&copy, &fields);
                        err = groove__db__schema_admission__Database__register_variant_case(jzn_db(s), &storage_tables[k], &projection_target, cases.ptr[c].tag, copy);
                    } else {
                        err = groove__db__schema_admission__Database__register_variant_ignore_case(jzn_db(s), &storage_tables[k], &projection_target, cases.ptr[c].tag);
                    }
                }
                if (some) groove_ProjectFieldVec_drop(&fields);
            }
            jz_vec_drop(&cases, sizeof(jzn_case), NULL);
        }
        jzn_src_vec_drop(&sources);
        jz_str_drop(&storage_tables[0]);
        jz_str_drop(&storage_tables[1]);
        jz_str_drop(&projection_target);
        jazz_model_TableSchema_drop(&target_table);
    }
    jzn_src_vec_drop(&targets);
    return err;
}

static jz_err jzn_required_enum_columns(const jazz_model_TableSchema *t, const jazz_node_TablePhysicalMapping *m,
                                        const jz_btree *required_fields, bool require_boundary, jz_btree *out) {
    jz_btree_init(out, &jzn_desc_u64_set);
    for (u32 i = 0; i < t->columns.len; i++) {
        const jazz_model_ColumnSchema *column = &t->columns.ptr[i];
        if (!jz_btree_contains(required_fields, &column->name)) continue;
        const u64 *column_id = jz_btree_get(&m->columns, &column->name);
        if (!column_id) continue;
        if (require_boundary) {
            bool has_enum_boundary = jazz_node__node__physical__physical_mapping_has_enum_boundary(m, *column_id) ||
                                     jazz_node__node__physical__value_type_has_enum_boundary(&column->column_type);
            if (!has_enum_boundary) continue;
        }
        jz_btree_insert(out, column_id, NULL, NULL);
    }
    return JZ_OK;
}

/* Register (or refresh) a current-source projection that decodes only the
 * enum columns required by one query source. The fixed output shape retains
 * the ordinary logical row descriptor; enum values outside the requirement
 * closure are typed nulls and never expose a physical tag. */
jz_err jazz_node__node__physical__NodeState__ensure_physical_current_projection_for_enum_columns(jazz_node_NodeState *s,
    jazz_types_SchemaVersionId target_schema, const jz_str *target_table_name, const jz_btree *required_fields, jz_str *out) {
    jazz_types_SchemaVersionAlias target_alias;
    JZ_TRY(jzn_alias(s, target_schema, "physical current projection target schema alias missing", &target_alias));
    jazz_model_TableSchema target_table;
    JZ_TRY(jazz_node__node__state__read_payload__NodeState__table_in_schema(s, target_table_name, target_schema, &target_table));
    const jazz_node_TablePhysicalMapping *tm = jzn_mapping(s, target_schema, target_table_name);
    if (!tm) { jazz_model_TableSchema_drop(&target_table); return JZN_ISV("target enum physical mapping missing"); }
    jazz_node_TablePhysicalMapping target_mapping;
    jazz_node_TablePhysicalMapping_clone(&target_mapping, tm);
    jz_btree required_enum_columns;
    jzn_required_enum_columns(&target_table, &target_mapping, required_fields, true, &required_enum_columns);
    bool target_has_any_enum_boundary = false;
    for (u32 i = 0; i < target_table.columns.len; i++) {
        const jazz_model_ColumnSchema *column = &target_table.columns.ptr[i];
        const u64 *column_id = jz_btree_get(&target_mapping.columns, &column->name);
        if ((column_id && jazz_node__node__physical__physical_mapping_has_enum_boundary(&target_mapping, *column_id)) ||
            jazz_node__node__physical__value_type_has_enum_boundary(&column->column_type)) {
            target_has_any_enum_boundary = true;
            break;
        }
    }
    jz_err err = JZ_OK;
    /* A schema with no enum boundary can use its durable target directly. For
     * an enum-bearing table, an empty requirement set is itself a query-local
     * compatibility boundary: unrequested enum cells must be typed-null. */
    if (required_enum_columns.len == 0 && !target_has_any_enum_boundary) {
        *out = jazz_node__node__physical__physical_current_projection_target(target_alias, target_table_name);
        goto done;
    }
    jz_str projection_target = jazz_node__node__physical__physical_current_projection_target_for_enum_columns(target_alias, target_table_name, &required_enum_columns);
    jz_str storage_tables[2] = {
        jazz_node__node__physical__physical_global_current_table_name(target_mapping.table_id),
        jazz_node__node__physical__physical_ahead_current_table_name(target_mapping.table_id),
    };
    for (u32 k = 0; k < 2 && !JZ_IS_ERR(err); k++) {
        groove_TableSchema gc = jazz_model__schema__TableSchema__global_current_storage_table(&target_table);
        /* The query-local target is the semantic read boundary: it exposes the
         * authored descriptor itself. */
        groove_RecordDescriptor output = groove__schema__TableSchema__record_schema(&gc);
        groove_TableSchema_drop(&gc);
        err = groove__db__schema_admission__Database__define_variant_projection(jzn_db(s), &storage_tables[k], &projection_target, output);
    }
    jzn_src_vec sources = {0};
    if (!JZ_IS_ERR(err)) jzn_collect_sources(s, true, target_mapping.table_id, &sources);
    for (u32 i = 0; i < sources.len && !JZ_IS_ERR(err); i++) {
        const jzn_src *src = &sources.ptr[i];
        jzn_case_vec cases = {0};
        err = jzn_cases_for(s, src, "physical current projection source schema alias missing", &cases);
        for (u32 c = 0; c < cases.len && !JZ_IS_ERR(err); c++) {
            bool some = false;
            groove_ProjectFieldVec fields = {0};
            err = jzn_content_projection_case(s, src->schema, &src->table, &src->mapping, target_schema, target_table_name,
                                              JZN_CPS_CURRENT, cases.ptr[c].present, &required_enum_columns, &some, &fields);
            for (u32 k = 0; k < 2 && !JZ_IS_ERR(err); k++) {
                if (some) {
                    groove_ProjectFieldVec copy;
                    groove_ProjectFieldVec_clone(&copy, &fields);
                    err = groove__db__schema_admission__Database__register_variant_projection_case_omitting_unrepresentable_enums(
                        jzn_db(s), &storage_tables[k], &projection_target, cases.ptr[c].tag, copy);
                } else {
                    err = groove__db__schema_admission__Database__register_variant_ignore_case(jzn_db(s), &storage_tables[k], &projection_target, cases.ptr[c].tag);
                }
            }
            if (some) groove_ProjectFieldVec_drop(&fields);
        }
        jz_vec_drop(&cases, sizeof(jzn_case), NULL);
    }
    jzn_src_vec_drop(&sources);
    jz_str_drop(&storage_tables[0]);
    jz_str_drop(&storage_tables[1]);
    if (JZ_IS_ERR(err)) jz_str_drop(&projection_target);
    else *out = projection_target;
done:
    jz_btree_drop(&required_enum_columns);
    jazz_node_TablePhysicalMapping_drop(&target_mapping);
    jazz_model_TableSchema_drop(&target_table);
    return err;
}

/* ---- winner projection cache: SchemaVersionId -> String -> (target, fields) ---- */
typedef struct jzn_winner_prepared { jz_str target; jzn_StrVec fields; } jzn_winner_prepared;
static void jzn_winner_prepared_drop(void *p) {
    jzn_winner_prepared *w = p;
    jz_str_drop(&w->target);
    jzn_str_vec_drop(&w->fields);
}
static void jzn_btree_drop_v(void *p) { jz_btree_drop((jz_btree *)p); }
static const jz_btree_desc jzn_desc_str_winner = { sizeof(jz_str), sizeof(jzn_winner_prepared), jz_cmp_str, jzn_str_drop_v, jzn_winner_prepared_drop };
const jz_btree_desc jzn_desc_uuid_winner_map = { sizeof(jz_uuid), sizeof(jz_btree), jz_cmp_uuid, NULL, jzn_btree_drop_v };

static jz_err jazz_node__node__physical__NodeState__lens_current_cell(jazz_node_NodeState *s, jazz_types_SchemaVersionId source_schema,
    const jz_str *source_table_name, const jazz_node_TablePhysicalMapping *source_mapping, const jz_btree *available,
    jazz_types_SchemaVersionId target_schema, const jz_str *target_table_name, const jz_str *target_column,
    bool *some, jazz_node_CurrentWinnerCellProjection *out);

/* Resolve a missing target user field through the migration path before the
 * Global/Ahead arg-max. Yields a projection field, not only a literal:
 * CopyColumn chains must read their actual source physical field. */
static jz_err jazz_node__node__physical__NodeState__lens_projection_for_missing_current_field(jazz_node_NodeState *s,
    jazz_types_SchemaVersionId source_schema, const jz_str *source_table_name, const jazz_node_TablePhysicalMapping *source_mapping,
    const jz_btree *available, jazz_types_SchemaVersionId target_schema, const jz_str *target_table_name,
    const jazz_node_TablePhysicalMapping *target_mapping, const jz_str *target_column, jz_str output_name, groove_ValueType output_type,
    groove_ProjectField **out /* NULL result = None */) {
    bool some = false;
    jazz_node_CurrentWinnerCellProjection cell;
    jz_err err = jazz_node__node__physical__NodeState__lens_current_cell(s, source_schema, source_table_name, source_mapping, available,
                                                                         target_schema, target_table_name, target_column, &some, &cell);
    *out = NULL;
    if (JZ_IS_ERR(err)) goto fail;
    if (!some || cell.tag == JZN_CWCP_NULL) {
        if (some) jazz_node_CurrentWinnerCellProjection_drop(&cell);
        goto fail; /* None, not an error */
    }
    if (cell.tag == JZN_CWCP_FIELD) {
        const u64 *target_column_id = jz_btree_get(&target_mapping->columns, target_column);
        if (!target_column_id) { err = JZN_ISV("target current winner column mapping missing"); goto fail_cell; }
        const jazz_model_TableSchema *target_table;
        err = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, target_table_name, target_schema, &target_table);
        if (JZ_IS_ERR(err)) goto fail_cell;
        const groove_ValueType *tct = NULL;
        for (u32 i = 0; i < target_table->columns.len; i++)
            if (jz_str_eq(&target_table->columns.ptr[i].name, target_column)) { tct = &target_table->columns.ptr[i].column_type; break; }
        if (!tct) { err = JZN_ISV("target current winner column schema missing"); goto fail_cell; }
        groove_ValueType target_column_type;
        groove_ValueType_clone(&target_column_type, tct);
        if (cell.column_id == *target_column_id ||
            (!jazz_node__node__physical__physical_mapping_has_enum_boundary(source_mapping, cell.column_id) &&
             !jazz_node__node__physical__physical_mapping_has_enum_boundary(target_mapping, *target_column_id) &&
             !jazz_node__node__physical__value_type_has_enum_boundary(&cell.column_type) &&
             !jazz_node__node__physical__value_type_has_enum_boundary(&target_column_type))) {
            *out = groove__ivm__op_types__ProjectField__renamed(cell.name, output_name);
            cell.name = (jz_str){0};
            groove_ValueType_drop(&output_type);
        } else {
            jazz_node_EnumOccurrenceRemaps remaps;
            err = jazz_node__node__physical__NodeState__physical_copy_enum_remaps(s, source_mapping, cell.column_id, target_mapping, *target_column_id,
                                                                               &cell.column_type, &target_column_type, &remaps);
            if (JZ_IS_ERR(err)) { groove_ValueType_drop(&target_column_type); goto fail_cell; }
            *out = groove__ivm__op_types__ProjectField__recursive_enum_remap(cell.name, output_name, output_type, remaps);
            cell.name = (jz_str){0};
        }
        groove_ValueType_drop(&target_column_type);
        jazz_node_CurrentWinnerCellProjection_drop(&cell);
        return JZ_OK;
    }
    /* Literal(default) */
    {
        groove_Value def = cell.literal;
        cell.literal = groove_value_null();
        if (output_type.tag == GROOVE_VT_NULLABLE) {
            /* A current-winner cell has one outer nullable layer for
             * source-field presence; a nullable lens default is its inner
             * value, so it is wrapped too. */
            def = groove_value_some(def);
        }
        *out = groove__ivm__op_types__ProjectField__literal_typed(output_name, def, output_type);
        jazz_node_CurrentWinnerCellProjection_drop(&cell);
        return JZ_OK;
    }
fail_cell:
    jazz_node_CurrentWinnerCellProjection_drop(&cell);
fail:
    jz_str_drop(&output_name);
    groove_ValueType_drop(&output_type);
    return err;
}

/* Lens op application shared by both cell maps (Rust repeats the match in
 * lens_current_cell and physical_content_projection_case). */
typedef void (*jzn_make_literal_fn)(void *dst, const groove_Value *def);
static void jzn_apply_lens_ops(jz_btree *cells, const jazz_node_CompiledLensPath *path, usize val_size,
                               void (*vclone)(void *, const void *), jzn_make_literal_fn make_literal) {
    u8 tmp[256];
    JZ_ASSERT(val_size <= sizeof tmp, "cell projection too large");
    for (u32 i = 0; i < path->ops.len; i++) {
        const jazz_node_CompiledLensOp *op = &path->ops.ptr[i];
        switch (op->tag) {
        case JAZZ_LENS_OP_RENAME: {
            jz_str old_key;
            if (jz_btree_remove(cells, &op->a, &old_key, tmp)) {
                jz_str_drop(&old_key);
                jz_str to = jz_str_clone(&op->b);
                jz_btree_insert(cells, &to, tmp, NULL);
            }
            break;
        }
        case JAZZ_LENS_OP_COPY: {
            const void *value = jz_btree_get(cells, &op->a);
            if (value) {
                vclone(tmp, value);
                jz_str to = jz_str_clone(&op->b);
                jz_btree_insert(cells, &to, tmp, NULL);
            }
            break;
        }
        case JAZZ_LENS_OP_ADD: {
            if (!jz_btree_contains(cells, &op->a)) {
                make_literal(tmp, &op->default_);
                jz_str column = jz_str_clone(&op->a);
                jz_btree_insert(cells, &column, tmp, NULL);
            }
            break;
        }
        case JAZZ_LENS_OP_DROP: {
            jz_str old_key;
            if (jz_btree_remove(cells, &op->a, &old_key, tmp)) {
                jz_str_drop(&old_key);
                cells->d->val_drop(tmp);
            }
            break;
        }
        default: JZ_UNREACHABLE();
        }
    }
}

static void jzn_cwcp_literal(void *dst, const groove_Value *def) {
    jazz_node_CurrentWinnerCellProjection *c = dst;
    memset(c, 0, sizeof *c);
    c->tag = JZN_CWCP_LITERAL;
    groove_Value_clone(&c->literal, def);
}

/* Where a target current-winner user column comes from in one source variant
 * along the migration lens path; None without a lens path. */
static jz_err jazz_node__node__physical__NodeState__lens_current_cell(jazz_node_NodeState *s, jazz_types_SchemaVersionId source_schema,
    const jz_str *source_table_name, const jazz_node_TablePhysicalMapping *source_mapping, const jz_btree *available,
    jazz_types_SchemaVersionId target_schema, const jz_str *target_table_name, const jz_str *target_column,
    bool *some, jazz_node_CurrentWinnerCellProjection *out) {
    *some = false;
    jazz_model_TableSchema source_table;
    JZ_TRY(jazz_node__node__state__read_payload__NodeState__table_in_schema(s, source_table_name, source_schema, &source_table));
    jz_btree cells;
    jz_btree_init(&cells, &jzn_desc_str_cwcp);
    for (u32 i = 0; i < source_table.columns.len; i++) {
        const jazz_model_ColumnSchema *column = &source_table.columns.ptr[i];
        jazz_node_CurrentWinnerCellProjection projection;
        memset(&projection, 0, sizeof projection);
        projection.tag = JZN_CWCP_NULL;
        const u64 *column_id = jz_btree_get(&source_mapping->columns, &column->name);
        if (column_id) {
            jz_str name = jazz_node__node__physical__physical_user_column_field(*column_id);
            if (jz_btree_contains(available, &name)) {
                projection.tag = JZN_CWCP_FIELD;
                projection.name = name;
                projection.column_id = *column_id;
                groove_ValueType_clone(&projection.column_type, &column->column_type);
            } else {
                jz_str_drop(&name);
            }
        }
        jz_str key = jz_str_clone(&column->name);
        jz_btree_insert(&cells, &key, &projection, NULL);
    }
    bool has_path = false;
    jazz_node_CompiledLensPath path;
    jz_err err = jazz_node__node__state__read_payload__NodeState__compiled_lens_path(s, source_schema, target_schema, source_table_name, &has_path, &path);
    if (!JZ_IS_ERR(err) && has_path) {
        if (jz_str_eq(&path.target_table, target_table_name)) {
            jzn_apply_lens_ops(&cells, &path, sizeof(jazz_node_CurrentWinnerCellProjection), jazz_node_CurrentWinnerCellProjection_clone, jzn_cwcp_literal);
            jz_str key;
            if (jz_btree_remove(&cells, target_column, &key, out)) { jz_str_drop(&key); *some = true; }
        }
        jazz_node_CompiledLensPath_drop(&path);
    }
    jz_btree_drop(&cells);
    jazz_model_TableSchema_drop(&source_table);
    return err;
}

/* Register the common physical descriptor used to choose the latest
 * Global/Ahead version before a query-local old-schema projection can omit an
 * unrepresentable enum case. */
jz_err jazz_node__node__physical__NodeState__ensure_physical_current_winner_projection(jazz_node_NodeState *s,
    jazz_types_SchemaVersionId target_schema, const jz_str *target_table_name, jz_str *target_out, jzn_StrVec *fields_out) {
    const jz_btree *cached_targets = jz_btree_get(&s->catalogue.physical_current_winner_projections, &target_schema);
    if (cached_targets) {
        const jzn_winner_prepared *prepared = jz_btree_get(cached_targets, target_table_name);
        if (prepared) {
            *target_out = jz_str_clone(&prepared->target);
            jzn_str_vec_clone(fields_out, &prepared->fields);
            return JZ_OK;
        }
    }
    const jazz_node_TablePhysicalMapping *tm = jzn_mapping(s, target_schema, target_table_name);
    if (!tm) return JZN_ISV("target current winner physical mapping missing");
    jazz_node_TablePhysicalMapping target_mapping;
    jazz_node_TablePhysicalMapping_clone(&target_mapping, tm);
    jz_str storage_tables[2] = {
        jazz_node__node__physical__physical_global_current_table_name(target_mapping.table_id),
        jazz_node__node__physical__physical_ahead_current_table_name(target_mapping.table_id),
    };
    jz_str projection_target = {0};
    jzn_StrVec physical_fields = {0};
    bool has_output_fields = false;
    jzn_StrVec output_fields = {0};
    jz_btree target_columns_by_physical_field;
    jz_btree_init(&target_columns_by_physical_field, &jzn_desc_str_str);
    jzn_src_vec sources = {0};
    groove_RecordDescriptor authored_output, output;
    const jazz_model_TableSchema *target_table;
    jz_err err = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, target_table_name, target_schema, &target_table);
    if (!JZ_IS_ERR(err)) err = jazz_node__node__physical__physical_current_descriptor(target_table, &target_mapping, &authored_output);
    if (!JZ_IS_ERR(err)) err = jzn_descriptor_names(authored_output, &physical_fields);
    /* Winner selection operates on raw physical data: keep the target layout
     * fixed, but take enum registries from the actual evolved storage table. */
    const groove_TableSchema *physical = NULL;
    if (!JZ_IS_ERR(err)) err = groove__db__schema_admission__Database__table_schema(jzn_db(s), &storage_tables[0], &physical);
    if (!JZ_IS_ERR(err)) err = jazz_node__node__physical__physical_write_descriptor(authored_output, physical_fields.ptr, physical_fields.len, physical, &output);
    if (JZ_IS_ERR(err)) goto done;
    projection_target = jazz_node__node__physical__physical_current_winner_projection_target(target_mapping.table_id, physical_fields.ptr, physical_fields.len);
    for (u32 k = 0; k < 2 && !JZ_IS_ERR(err); k++) {
        jzn_StrVec fields;
        err = jzn_descriptor_names(output, &fields);
        if (JZ_IS_ERR(err)) break;
        if (has_output_fields) {
            bool same = jzn_str_vec_eq(&output_fields, &fields);
            jzn_str_vec_drop(&fields);
            if (!same) { err = JZN_ISV("physical current winner descriptors disagree"); break; }
        } else {
            output_fields = fields;
            has_output_fields = true;
        }
        err = groove__db__schema_admission__Database__define_variant_projection(jzn_db(s), &storage_tables[k], &projection_target, output);
    }
    if (JZ_IS_ERR(err)) goto done;

    jzn_collect_sources(s, true, target_mapping.table_id, &sources);
    for (u32 i = 0; i < sources.len && !JZ_IS_ERR(err); i++) {
        const jzn_src *src = &sources.ptr[i];
        jazz_types_SchemaVersionAlias source_alias;
        err = jzn_alias(s, src->schema, "physical current winner source schema alias missing", &source_alias);
        if (JZ_IS_ERR(err)) break;
        jazz_model_TableSchema source_table;
        err = jazz_node__node__state__read_payload__NodeState__table_in_schema(s, &src->table, src->schema, &source_table);
        if (JZ_IS_ERR(err)) break;
        jz_btree_clear(&target_columns_by_physical_field);
        {
            const jz_str *column; const u64 *id;
            JZ_BTREE_FOR(&target_mapping.columns, column, id) {
                jz_str k = jazz_node__node__physical__physical_user_column_field(*id);
                jz_str v = jz_str_clone(column);
                jz_btree_insert(&target_columns_by_physical_field, &k, &v, NULL);
            }
        }
        jzn_case_vec cases = {0};
        err = jzn_cases(src, source_alias, &cases);
        for (u32 c = 0; c < cases.len && !JZ_IS_ERR(err); c++) {
            jzn_StrVec names = {0};
            err = jazz_node__node__physical__physical_current_field_names_for_case(&source_table, &src->mapping, cases.ptr[c].present, &names);
            if (JZ_IS_ERR(err)) break;
            jz_btree available;
            jz_btree_init(&available, &jzn_desc_str_set);
            for (u32 n = 0; n < names.len; n++) jz_btree_insert(&available, &names.ptr[n], NULL, NULL);
            jz_vec_drop(&names, sizeof(jz_str), NULL); /* moved into the set */
            const groove_DescriptorFieldVec *ofields = groove__records__RecordDescriptor__fields(output);
            for (u32 k = 0; k < 2 && !JZ_IS_ERR(err); k++) {
                groove_ProjectFieldVec fields = {0};
                for (u32 f = 0; f < ofields->len; f++) {
                    const groove_DescriptorField *field = &ofields->ptr[f];
                    if (!field->has_name) { err = JZN_ISV("physical current winner field unnamed"); break; }
                    jz_str name = jz_str_clone(&field->name);
                    const jz_str *column;
                    groove_ProjectField *pf = NULL;
                    if (jz_btree_contains(&available, &name)) {
                        pf = groove__ivm__op_types__ProjectField__named(name);
                    } else if ((column = jz_btree_get(&target_columns_by_physical_field, &name)) != NULL) {
                        /* Only mapped user columns can be absent because of a
                         * lens; witness and system fields remain raw. */
                        groove_ValueType vt;
                        groove_ValueType_clone(&vt, &field->value_type);
                        jz_str name_copy = jz_str_clone(&name);
                        err = jazz_node__node__physical__NodeState__lens_projection_for_missing_current_field(s, src->schema, &src->table,
                            &src->mapping, &available, target_schema, target_table_name, &target_mapping, column, name_copy, vt, &pf);
                        if (JZ_IS_ERR(err)) { jz_str_drop(&name); break; }
                        if (!pf) {
                            groove_ValueType vt2;
                            groove_ValueType_clone(&vt2, &field->value_type);
                            pf = groove__ivm__op_types__ProjectField__literal_typed(name, groove_value_null(), vt2);
                        } else {
                            jz_str_drop(&name);
                        }
                    } else if (field->value_type.tag == GROOVE_VT_NULLABLE) {
                        groove_ValueType vt;
                        groove_ValueType_clone(&vt, &field->value_type);
                        pf = groove__ivm__op_types__ProjectField__literal_typed(name, groove_value_null(), vt);
                    } else {
                        jz_str_drop(&name);
                        err = JZN_ISV("physical current winner source misses required field");
                        break;
                    }
                    JZ_VEC_PUSH(fields, pf);
                }
                if (JZ_IS_ERR(err)) { groove_ProjectFieldVec_drop(&fields); break; }
                err = groove__db__schema_admission__Database__refresh_variant_case_for_registry_evolution(jzn_db(s), &storage_tables[k],
                                                                                                        &projection_target, cases.ptr[c].tag, fields);
            }
            jz_btree_drop(&available);
        }
        jz_vec_drop(&cases, sizeof(jzn_case), NULL);
        jazz_model_TableSchema_drop(&source_table);
    }
    if (JZ_IS_ERR(err)) goto done;
    {
        /* Publish only after every source variant and both storage layers
         * have been registered: a failed registration must not become a hit. */
        jzn_winner_prepared prepared;
        prepared.target = jz_str_clone(&projection_target);
        if (has_output_fields) jzn_str_vec_clone(&prepared.fields, &output_fields);
        else memset(&prepared.fields, 0, sizeof prepared.fields);
        bool inserted;
        jz_btree *targets = jz_btree_entry(&s->catalogue.physical_current_winner_projections, &target_schema, &inserted);
        if (inserted) jz_btree_init(targets, &jzn_desc_str_winner);
        jz_str key = jz_str_clone(target_table_name);
        jzn_winner_prepared old;
        if (jz_btree_insert(targets, &key, &prepared, &old)) jzn_winner_prepared_drop(&old);
        *target_out = projection_target;
        projection_target = (jz_str){0};
        *fields_out = output_fields;
        output_fields = (jzn_StrVec){0};
    }
done:
    jzn_src_vec_drop(&sources);
    jz_btree_drop(&target_columns_by_physical_field);
    jzn_str_vec_drop(&output_fields);
    jzn_str_vec_drop(&physical_fields);
    jz_str_drop(&projection_target);
    jz_str_drop(&storage_tables[0]);
    jz_str_drop(&storage_tables[1]);
    jazz_node_TablePhysicalMapping_drop(&target_mapping);
    return err;
}

/* Build the logical query-local projection placed after physical current
 * winner selection. Its enum remaps are non-total row omissions. */
jz_err jazz_node__node__physical__NodeState__physical_current_post_winner_projection_fields(jazz_node_NodeState *s,
    jazz_types_SchemaVersionId target_schema, const jz_str *target_table_name, const jz_btree *required_fields, groove_ProjectFieldVec *out) {
    const jazz_node_TablePhysicalMapping *tm = jzn_mapping(s, target_schema, target_table_name);
    if (!tm) return JZN_ISV("target post-winner physical mapping missing");
    jazz_node_TablePhysicalMapping target_mapping;
    jazz_node_TablePhysicalMapping_clone(&target_mapping, tm);
    const jazz_model_TableSchema *target_table;
    jz_err err = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, target_table_name, target_schema, &target_table);
    if (JZ_IS_ERR(err)) { jazz_node_TablePhysicalMapping_drop(&target_mapping); return err; }
    jz_btree required_enum_columns;
    jzn_required_enum_columns(target_table, &target_mapping, required_fields, false, &required_enum_columns);
    bool some = false;
    err = jzn_content_projection_case(s, target_schema, target_table_name, &target_mapping, target_schema, target_table_name,
                                      JZN_CPS_CURRENT, NULL, &required_enum_columns, &some, out);
    if (!JZ_IS_ERR(err) && !some) err = JZN_ISV("post-winner current projection unexpectedly lacks fields");
    jz_btree_drop(&required_enum_columns);
    jazz_node_TablePhysicalMapping_drop(&target_mapping);
    return err;
}

static jz_err jazz_node__node__physical__NodeState__register_physical_current_winner_projections(jazz_node_NodeState *s);

static jz_err jazz_node__node__physical__NodeState__synchronize_physical_version_tables_inner(jazz_node_NodeState *s) {
    groove_TableSchemaVec desired_tables = {0};
    JZ_TRY(jazz_node__node__physical__physical_version_storage_tables(&s->catalogue.catalogue_schemas, &s->catalogue.schema_version_aliases,
                                                                     &s->catalogue.physical_mappings, &desired_tables));
    jz_err err = JZ_OK;
    u32 i = 0;
    for (; i < desired_tables.len; i++) {
        groove_TableSchema *desired = &desired_tables.ptr[i];
        const groove_TableSchema *found = NULL;
        err = groove__db__schema_admission__Database__table_schema(jzn_db(s), &desired->name, &found);
        if (JZ_IS_ERR(err)) {
            if (err.code != GROOVE_DB_ERR_TABLE_NOT_FOUND) break;
            jz_err_drop(&err);
            err = groove__db__schema_admission__Database__register_table(jzn_db(s), *desired);
            memset(desired, 0, sizeof *desired); /* moved */
            if (JZ_IS_ERR(err)) break;
            continue;
        }
        groove_TableSchema existing;
        groove_TableSchema_clone(&existing, found);
        err = groove__db__schema_admission__Database__evolve_table_variant_registries(jzn_db(s), &desired->name, &desired->columns);
        groove_ColumnSchemaVec added_columns = {0};
        if (!JZ_IS_ERR(err)) {
            for (u32 c = 0; c < desired->columns.len; c++) {
                bool all_differ = true;
                for (u32 e = 0; e < existing.columns.len; e++)
                    if (jz_str_eq(&existing.columns.ptr[e].name, &desired->columns.ptr[c].name)) { all_differ = false; break; }
                if (!all_differ) continue;
                groove_ColumnSchema col;
                groove_ColumnSchema_clone(&col, &desired->columns.ptr[c]);
                JZ_VEC_PUSH(added_columns, col);
            }
        }
        for (u32 v = 0; v < desired->variants.len && !JZ_IS_ERR(err); v++) {
            groove_TableVariant *schema_version = &desired->variants.ptr[v];
            if (groove__schema__TableSchema__variant(&existing, schema_version->tag)) continue;
            groove_ColumnSchemaVec cols;
            jz_vec_clone(&cols, &added_columns, sizeof(groove_ColumnSchema), groove_ColumnSchema_clone);
            err = groove__db__schema_admission__Database__register_table_variant_with_columns(jzn_db(s), &desired->name, cols, *schema_version);
            memset(schema_version, 0, sizeof *schema_version); /* moved */
        }
        for (u32 x = 0; x < desired->indices.len && !JZ_IS_ERR(err); x++) {
            groove_IndexSchema *index = &desired->indices.ptr[x];
            bool present = false;
            for (u32 e = 0; e < existing.indices.len; e++)
                if (jz_str_eq(&existing.indices.ptr[e].name, &index->name)) { present = true; break; }
            if (present) continue;
            err = groove__db__schema_admission__Database__register_table_index(jzn_db(s), &desired->name, *index);
            memset(index, 0, sizeof *index); /* moved */
        }
        jz_vec_drop(&added_columns, sizeof(groove_ColumnSchema), groove_ColumnSchema_drop);
        groove_TableSchema_drop(&existing);
        if (JZ_IS_ERR(err)) break;
    }
    jz_vec_drop(&desired_tables, sizeof(groove_TableSchema), groove_TableSchema_drop);
    if (JZ_IS_ERR(err)) return err;
    JZ_TRY(jazz_node__node__physical__NodeState__register_physical_history_variant_projections(s));
    JZ_TRY(jazz_node__node__physical__NodeState__register_physical_current_variant_projections(s));
    return jazz_node__node__physical__NodeState__register_physical_current_winner_projections(s);
}

jz_err jazz_node__node__physical__NodeState__synchronize_physical_version_tables(jazz_node_NodeState *s) {
    /* The registry can evolve without changing a logical schema key: rebuild
     * all successful metadata with the new registry. */
    jz_btree_clear(&s->catalogue.physical_current_winner_projections);
    /* A physical schema is a coupled registry; do not leave a prefix behind
     * if any later member fails validation. */
    groove_RuntimeRegistryCheckpoint *checkpoint = groove__db__schema_admission__Database__runtime_registry_checkpoint(jzn_db(s));
    jz_err result = jazz_node__node__physical__NodeState__synchronize_physical_version_tables_inner(s);
    if (JZ_IS_ERR(result)) {
        groove__db__schema_admission__Database__restore_runtime_registry(jzn_db(s), checkpoint);
        /* None of the earlier successes describe the restored registry. */
        jz_btree_clear(&s->catalogue.physical_current_winner_projections);
    } else {
        groove_RuntimeRegistryCheckpoint_drop(checkpoint);
    }
    return result;
}

typedef struct jzn_schema_table { jz_uuid schema; jz_str table; } jzn_schema_table;
static int jzn_schema_table_cmp(const void *a, const void *b, void *ctx) {
    const jzn_schema_table *x = a, *y = b;
    int c = jz_cmp_uuid(&x->schema, &y->schema, ctx);
    return c ? c : jz_str_cmp(&x->table, &y->table);
}
static void jzn_schema_table_drop(void *p) { jz_str_drop(&((jzn_schema_table *)p)->table); }
static const jz_btree_desc jzn_desc_schema_table_set = { sizeof(jzn_schema_table), 0, jzn_schema_table_cmp, jzn_schema_table_drop, NULL };

/* Keep the raw Global/Ahead winner targets live as the physical enum
 * registries evolve. */
static jz_err jazz_node__node__physical__NodeState__register_physical_current_winner_projections(jazz_node_NodeState *s) {
    jz_btree targets;
    jz_btree_init(&targets, &jzn_desc_schema_table_set);
    const jz_uuid *schema;
    const jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(&s->catalogue.physical_mappings, schema, mapping) {
        const jz_str *name; const void *unused;
        JZ_BTREE_FOR(&mapping->tables, name, unused) {
            jzn_schema_table k = { *schema, jz_str_clone(name) };
            jz_btree_insert(&targets, &k, NULL, NULL);
        }
    }
    jz_err err = JZ_OK;
    const jzn_schema_table *t; const void *unused;
    JZ_BTREE_FOR(&targets, t, unused) {
        jz_str target; jzn_StrVec fields;
        err = jazz_node__node__physical__NodeState__ensure_physical_current_winner_projection(s, t->schema, &t->table, &target, &fields);
        if (JZ_IS_ERR(err)) break;
        jz_str_drop(&target);
        jzn_str_vec_drop(&fields);
    }
    jz_btree_drop(&targets);
    return err;
}

/* ======================= content projection cases ======================= */

/* history/current wrappers (Rust: physical_history_projection_case[_for_enum_columns],
 * physical_current_projection_case[_for_enum_columns]) */
jz_err jazz_node__node__physical__NodeState__physical_history_projection_case_for_enum_columns(jazz_node_NodeState *s,
    jazz_types_SchemaVersionId source_schema, const jz_str *source_table_name, const jazz_node_TablePhysicalMapping *source_mapping,
    jazz_types_SchemaVersionId target_schema, const jz_str *target_table_name, const jz_btree *present, const jz_btree *required,
    bool *some, groove_ProjectFieldVec *out) {
    return jzn_content_projection_case(s, source_schema, source_table_name, source_mapping, target_schema, target_table_name,
                                       JZN_CPS_HISTORY, present, required, some, out);
}

jz_err jazz_node__node__physical__NodeState__physical_current_projection_case_for_enum_columns(jazz_node_NodeState *s,
    jazz_types_SchemaVersionId source_schema, const jz_str *source_table_name, const jazz_node_TablePhysicalMapping *source_mapping,
    jazz_types_SchemaVersionId target_schema, const jz_str *target_table_name, const jz_btree *present, const jz_btree *required,
    bool *some, groove_ProjectFieldVec *out) {
    return jzn_content_projection_case(s, source_schema, source_table_name, source_mapping, target_schema, target_table_name,
                                       JZN_CPS_CURRENT, present, required, some, out);
}

/* local `enum CellProjection { Field(String), Missing, Literal(Value) }` */
enum { JZN_CELL_FIELD, JZN_CELL_MISSING, JZN_CELL_LITERAL };
typedef struct jzn_cell { u32 tag; jz_str field; groove_Value literal; } jzn_cell;
static void jzn_cell_drop(void *p) {
    jzn_cell *c = p;
    if (c->tag == JZN_CELL_FIELD) jz_str_drop(&c->field);
    else if (c->tag == JZN_CELL_LITERAL) groove_Value_drop(&c->literal);
}
static void jzn_cell_clone(void *dst, const void *src) {
    jzn_cell *d = dst; const jzn_cell *s = src;
    d->tag = s->tag;
    if (s->tag == JZN_CELL_FIELD) d->field = jz_str_clone(&s->field);
    else if (s->tag == JZN_CELL_LITERAL) groove_Value_clone(&d->literal, &s->literal);
}
static void jzn_cell_literal(void *dst, const groove_Value *def) {
    jzn_cell *c = dst;
    c->tag = JZN_CELL_LITERAL;
    groove_Value_clone(&c->literal, def);
}
static const jz_btree_desc jzn_desc_str_cell = { sizeof(jz_str), sizeof(jzn_cell), jz_cmp_str, jzn_str_drop_v, jzn_cell_drop };

/* bootstrap identity remaps for a direct enum column before durable registry hydration */
static void jzn_bootstrap_identity_remaps(const groove_ValueType *t, jazz_node_EnumOccurrenceRemaps *out) {
    groove_RecursiveEnumRemaps_init(out);
    jz_str root = jz_str_from("root");
    if (t->tag == GROOVE_VT_ENUM_TAG) {
        groove_OptU8Vec tags = {0};
        for (u32 tag = 0; tag < t->v.enum_tag.variants.len; tag++)
            JZ_VEC_PUSH(tags, (u16)(tag <= 0xff ? tag : GROOVE_OPT_U8_NONE));
        jz_btree_insert(&out->scalar, &root, &tags, NULL);
    } else if (t->tag == GROOVE_VT_ENUM) {
        groove_OptU32Vec tags = {0};
        groove_OptStrVec children = {0};
        for (u32 tag = 0; tag < t->v.enm->cases.len; tag++) {
            JZ_VEC_PUSH(tags, (i64)tag);
            JZ_VEC_PUSH(children, jz_format("root/case/bootstrap/%u", tag));
        }
        jz_btree_insert(&out->payload, &root, &tags, NULL);
        jz_str root2 = jz_str_from("root");
        jz_btree_insert(&out->payload_children, &root2, &children, NULL);
    } else {
        JZ_ASSERT(false, "direct enum checked above");
    }
}

static jz_err jzn_content_projection_case(jazz_node_NodeState *s, jazz_types_SchemaVersionId source_schema, const jz_str *source_table_name,
    const jazz_node_TablePhysicalMapping *source_mapping, jazz_types_SchemaVersionId target_schema, const jz_str *target_table_name,
    jazz_node_ContentProjectionShape shape, const jz_btree *present, const jz_btree *required_enum_columns, bool *some, groove_ProjectFieldVec *out) {
    *some = false;
    jazz_model_TableSchema source_table, target_table;
    JZ_TRY(jazz_node__node__state__read_payload__NodeState__table_in_schema(s, source_table_name, source_schema, &source_table));
    jz_err err = jazz_node__node__state__read_payload__NodeState__table_in_schema(s, target_table_name, target_schema, &target_table);
    if (JZ_IS_ERR(err)) { jazz_model_TableSchema_drop(&source_table); return err; }
    jz_btree cells;
    jz_btree_init(&cells, &jzn_desc_str_cell);
    groove_TableSchema target_storage;
    memset(&target_storage, 0, sizeof target_storage);
    bool have_storage = false;
    jzn_StrVec physical_names = {0};
    jz_str physical_storage = {0};
    groove_ProjectFieldVec fields = {0};
    bool result_some = false;

    for (u32 i = 0; i < source_table.columns.len; i++) {
        const jazz_model_ColumnSchema *column = &source_table.columns.ptr[i];
        if (present && !jz_btree_contains(present, &column->name)) continue;
        const u64 *column_id = jz_btree_get(&source_mapping->columns, &column->name);
        if (!column_id) { err = JZN_ISV("physical projection column mapping missing"); goto done; }
        jzn_cell cell = { JZN_CELL_FIELD, jazz_node__node__physical__physical_user_column_field(*column_id), {0} };
        jz_str key = jz_str_clone(&column->name);
        jz_btree_insert(&cells, &key, &cell, NULL);
    }

    if (!jz_uuid_eq(&source_schema, &target_schema) || !jz_str_eq(source_table_name, target_table_name)) {
        bool has_path = false;
        jazz_node_CompiledLensPath path;
        err = jazz_node__node__state__read_payload__NodeState__compiled_lens_path(s, source_schema, target_schema, source_table_name, &has_path, &path);
        if (JZ_IS_ERR(err) || !has_path) goto done;
        if (!jz_str_eq(&path.target_table, target_table_name)) { jazz_node_CompiledLensPath_drop(&path); goto done; }
        jzn_apply_lens_ops(&cells, &path, sizeof(jzn_cell), jzn_cell_clone, jzn_cell_literal);
        jazz_node_CompiledLensPath_drop(&path);
    }
    target_storage = shape == JZN_CPS_HISTORY ? jazz_model__schema__TableSchema__history_storage_table(&target_table)
                                              : jazz_model__schema__TableSchema__global_current_storage_table(&target_table);
    have_storage = true;
    groove_RecordDescriptor target_record = groove__schema__TableSchema__record_schema(&target_storage);
    u32 user_cells = shape == JZN_CPS_HISTORY ? JZN_HISTORY_USER_CELLS : JZN_GCUR_USER_CELLS;
    const jazz_node_TablePhysicalMapping *target_mapping = jzn_mapping(s, target_schema, target_table_name);
    if (!target_mapping) { err = JZN_ISV("target enum physical mapping missing"); goto done; }
    err = shape == JZN_CPS_HISTORY ? jazz_node__node__physical__physical_history_field_names(&target_table, target_mapping, &physical_names)
                                   : jazz_node__node__physical__physical_current_field_names(&target_table, target_mapping, &physical_names);
    if (JZ_IS_ERR(err)) goto done;
    physical_storage = shape == JZN_CPS_HISTORY ? jazz_node__node__physical__physical_history_table_name(target_mapping->table_id)
                                                : jazz_node__node__physical__physical_global_current_table_name(target_mapping->table_id);
    groove_RecordDescriptor projection_output;
    if (required_enum_columns) {
        /* Query-local enum targets are authored-descriptor boundaries. */
        projection_output = shape == JZN_CPS_HISTORY ? jazz_node__node__physical__authored_history_projection_descriptor(&target_table)
                                                     : target_record;
    } else {
        const groove_TableSchema *physical = NULL;
        err = groove__db__schema_admission__Database__table_schema(jzn_db(s), &physical_storage, &physical);
        if (!JZ_IS_ERR(err))
            err = jazz_node__node__physical__widened_projection_descriptor(target_record, physical_names.ptr, physical_names.len, physical, &projection_output);
        if (JZ_IS_ERR(err)) goto done;
    }
    const groove_DescriptorFieldVec *record_fields = groove__records__RecordDescriptor__fields(target_record);
    for (u32 i = 0; i < user_cells && i < record_fields->len; i++) {
        JZ_ASSERT(record_fields->ptr[i].has_name, "Jazz history system fields are named");
        JZ_VEC_PUSH(fields, groove__ivm__op_types__ProjectField__named(jz_str_clone(&record_fields->ptr[i].name)));
    }
    for (u32 i = 0; i < target_table.columns.len; i++) {
        const jazz_model_ColumnSchema *column = &target_table.columns.ptr[i];
        jz_str output = jazz_node__node__query_engine__user_column_field(&column->name);
        jzn_cell projection;
        jz_str removed_key;
        if (jz_btree_remove(&cells, &column->name, &removed_key, &projection)) {
            jz_str_drop(&removed_key);
        } else if (present) {
            projection.tag = JZN_CELL_MISSING;
        } else {
            jz_str_drop(&output);
            goto done; /* Ok(None) */
        }
        switch (projection.tag) {
        case JZN_CELL_FIELD: {
            const u64 *cid = jz_btree_get(&target_mapping->columns, &column->name);
            if (!cid) {
                jz_str_drop(&output); jzn_cell_drop(&projection);
                err = JZN_ISV("target enum physical column mapping missing");
                goto done;
            }
            u64 column_id = *cid;
            bool has_enum_boundary = jazz_node__node__physical__physical_mapping_has_enum_boundary(target_mapping, column_id);
            bool direct_enum = column->column_type.tag == GROOVE_VT_ENUM_TAG || column->column_type.tag == GROOVE_VT_ENUM;
            if (has_enum_boundary || direct_enum) {
                if (required_enum_columns && !jz_btree_contains(required_enum_columns, &column_id)) {
                    /* This source does not semantically consume the cell: do
                     * not decode an unknown physical case, and do not let the
                     * physical tag cross the boundary. */
                    groove_ValueType inner;
                    groove_ValueType_clone(&inner, &column->column_type);
                    JZ_VEC_PUSH(fields, groove__ivm__op_types__ProjectField__literal_typed(output, groove_value_null(),
                                                                                          groove_vt_box(GROOVE_VT_NULLABLE, inner)));
                    jzn_cell_drop(&projection);
                    continue;
                }
                jazz_node_EnumOccurrenceRemaps remaps;
                if (has_enum_boundary) {
                    err = jazz_node__node__physical__NodeState__physical_to_authored_enum_remaps(s, target_mapping, column_id, &remaps);
                    if (JZ_IS_ERR(err)) { jz_str_drop(&output); jzn_cell_drop(&projection); goto done; }
                } else {
                    /* Initial table construction precedes durable registry
                     * hydration: an explicit identity map in authored order. */
                    jzn_bootstrap_identity_remaps(&column->column_type, &remaps);
                }
                jz_str field_name = jazz_node__node__query_engine__user_column_field(&column->name);
                u32 index;
                const groove_DescriptorFieldVec *pfields = groove__records__RecordDescriptor__fields(projection_output);
                bool found = groove__records__RecordDescriptor__field_index(projection_output, jz_str_slice(&field_name), &index) && index < pfields->len;
                jz_str_drop(&field_name);
                if (!found) {
                    groove_RecursiveEnumRemaps_drop(&remaps);
                    jz_str_drop(&output); jzn_cell_drop(&projection);
                    err = JZN_ISV("target enum projection output field missing");
                    goto done;
                }
                groove_ValueType target;
                groove_ValueType_clone(&target, &pfields->ptr[index].value_type);
                JZ_VEC_PUSH(fields, required_enum_columns
                    ? groove__ivm__op_types__ProjectField__recursive_enum_remap_omitting_unrepresentable(projection.field, output, target, remaps)
                    : groove__ivm__op_types__ProjectField__recursive_enum_remap(projection.field, output, target, remaps));
            } else {
                JZ_VEC_PUSH(fields, groove__ivm__op_types__ProjectField__renamed(projection.field, output));
            }
            break;
        }
        case JZN_CELL_MISSING: {
            groove_ValueType inner;
            groove_ValueType_clone(&inner, &column->column_type);
            JZ_VEC_PUSH(fields, groove__ivm__op_types__ProjectField__literal_typed(output, groove_value_null(),
                                                                                  groove_vt_box(GROOVE_VT_NULLABLE, inner)));
            break;
        }
        case JZN_CELL_LITERAL: {
            groove_ValueType inner;
            groove_ValueType_clone(&inner, &column->column_type);
            JZ_VEC_PUSH(fields, groove__ivm__op_types__ProjectField__literal_typed(output, groove_value_some(projection.literal),
                                                                                  groove_vt_box(GROOVE_VT_NULLABLE, inner)));
            break;
        }
        }
    }
    {
        const groove_DescriptorFieldVec *all = groove__records__RecordDescriptor__fields(groove__schema__TableSchema__record_schema(&target_storage));
        for (u32 i = user_cells + target_table.columns.len; i < all->len; i++) {
            JZ_ASSERT(all->ptr[i].has_name, "Jazz trailing storage fields are named");
            JZ_VEC_PUSH(fields, groove__ivm__op_types__ProjectField__named(jz_str_clone(&all->ptr[i].name)));
        }
    }
    result_some = true;
done:
    if (result_some) { *some = true; *out = fields; }
    else groove_ProjectFieldVec_drop(&fields);
    jz_str_drop(&physical_storage);
    jzn_str_vec_drop(&physical_names);
    if (have_storage) groove_TableSchema_drop(&target_storage);
    jz_btree_drop(&cells);
    jazz_model_TableSchema_drop(&target_table);
    jazz_model_TableSchema_drop(&source_table);
    return err;
}

/* ======================= scans ======================= */

groove_StaticScanSpec jazz_node__node__physical__shared_branch_scan(const groove_StaticScanSpec *scan) {
    jazz_model_BranchKey k;
    jazz_model_BranchKey_default(&k);
    groove_StaticScanSpec out = jazz_node__node__physical__branch_scan(&k, scan);
    jazz_model_BranchKey_drop(&k);
    return out;
}

/* Prefix every scan bound with the canonical branch key. `scan` is moved. */
groove_StaticScanSpec jazz_node__node__physical__branch_scan(const jazz_model_BranchKey *branch_key, const groove_StaticScanSpec *scan) {
    groove_Value bytes;
    bytes.tag = GROOVE_V_BYTES;
    jazz_model__model__branch__BranchKey__canonical_bytes(branch_key, &bytes.v.bytes);
    groove_LiteralValue branch = groove__ivm__op_types__LiteralValue__from_value(bytes);
    groove_StaticScanSpec out;
    memset(&out, 0, sizeof out);
    if (!scan) {
        out.tag = GROOVE_SCAN_PREFIX;
        JZ_VEC_PUSH(out.a, branch);
        return out;
    }
    out = *scan; /* moved */
    {
        groove_LiteralValue b;
        groove_LiteralValue_clone(&b, &branch);
        jz_vec_insert(&out.a, 0, &b, sizeof b);
    }
    if (out.tag == GROOVE_SCAN_RANGE) {
        groove_LiteralValue b;
        groove_LiteralValue_clone(&b, &branch);
        jz_vec_insert(&out.b, 0, &b, sizeof b);
    }
    groove_LiteralValue_drop(&branch);
    return out;
}
