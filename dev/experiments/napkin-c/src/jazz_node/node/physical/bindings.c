/* physical/bindings.c: jazz_node::node::physical (physical/bindings.rs).
 * Physical mapping allocation, registry validation and storage-table binding. */
#include "../physical.h"
#include "../codec.h"

static void jzn_str_clone_v(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone((const jz_str *)src); }

jz_err jazz_node__node__physical__allocate_provisional_physical_mapping(const jazz_model_JazzSchema *schema,
                                                                       jazz_protocol_PhysicalIdentityManifest identities,
                                                                       u64 *next_table_id, u64 *next_column_id,
                                                                       jazz_node_SchemaPhysicalMapping *out) {
    const char *invalid = jazz_protocol__protocol__PhysicalIdentityManifest__validate_for_schema(&identities, schema);
    if (invalid) {
        jazz_protocol_PhysicalIdentityManifest_drop(&identities);
        return jz_err_fmt(JZN_E_INVALID_CATALOGUE_UPDATE, "invalid catalogue update: %s", invalid);
    }
    jz_btree tables;
    jz_btree_init(&tables, &jzn_desc_str_table_mapping);
    jz_err err = JZ_OK;
    for (u32 t = 0; t < schema->tables.len; t++) {
        const jazz_model_TableSchema *table = &schema->tables.ptr[t];
        jazz_types_PhysicalTableId table_id = *next_table_id;
        if (*next_table_id == UINT64_MAX) { err = JZN_ISV("physical table id exhausted"); goto fail; }
        *next_table_id += 1;
        jazz_node_TablePhysicalMapping mapping;
        jazz_node_TablePhysicalMapping_init(&mapping, table_id);
        for (u32 c = 0; c < table->columns.len; c++) {
            jazz_types_PhysicalColumnId column_id = *next_column_id;
            if (*next_column_id == UINT64_MAX) {
                jazz_node_TablePhysicalMapping_drop(&mapping);
                err = JZN_ISV("physical column id exhausted");
                goto fail;
            }
            *next_column_id += 1;
            jz_str name = jz_str_clone(&table->columns.ptr[c].name);
            jz_btree_insert(&mapping.columns, &name, &column_id, NULL);
        }
        jz_str name = jz_str_clone(&table->name);
        jz_btree_insert(&tables, &name, &mapping, NULL);
    }
    out->identities = identities;
    out->tables = tables;
    return JZ_OK;
fail:
    jz_btree_drop(&tables);
    jazz_protocol_PhysicalIdentityManifest_drop(&identities);
    return err;
}

static const jazz_node_TablePhysicalMapping *jzn_lookup_table(const jz_btree *mappings, const jazz_types_SchemaVersionId *v,
                                                             const jz_str *table) {
    const jazz_node_SchemaPhysicalMapping *mapping = jz_btree_get(mappings, v);
    return mapping ? jz_btree_get(&mapping->tables, table) : NULL;
}

/* is `fields` a subset of the keys of `columns`? */
static bool jzn_fields_subset_of_columns(const jz_btree *fields, const jz_btree *columns) {
    jz_str *field;
    void *unused;
    JZ_BTREE_FOR(fields, field, unused) {
        if (!jz_btree_contains(columns, field)) return false;
    }
    return true;
}

/* Allocate and retain the single hidden Groove row case for one Jazz layout. */
jz_err jazz_node__node__physical__allocate_physical_variant_cases(jz_btree *mappings, const jz_btree *aliases,
                                                                 jazz_types_SchemaVersionId schema_version,
                                                                 const jz_str *logical_table, jz_btree fields,
                                                                 jazz_node_PhysicalVariantCaseVec *out) {
    jz_err err = JZ_OK;
    const jazz_node_TablePhysicalMapping *target = jzn_lookup_table(mappings, &schema_version, logical_table);
    if (!target) { err = JZN_ISV("variant-case target physical mapping missing"); goto fail; }
    jazz_types_PhysicalTableId table_id = target->table_id;
    /* target_columns = target.columns.keys().cloned().collect::<BTreeSet<_>>() */
    jz_btree target_columns;
    jz_btree_init(&target_columns, &jzn_desc_str_set);
    {
        jz_str *k; u64 *v;
        JZ_BTREE_FOR(&target->columns, k, v) {
            jz_str key = jz_str_clone(k);
            jz_btree_insert(&target_columns, &key, NULL, NULL);
        }
    }
    bool subset = jzn_fields_subset_of_columns(&fields, &target_columns);
    jz_btree_drop(&target_columns);
    if (!subset) { err = JZN_ISV("physical table variant contains an unknown field"); goto fail; }
    if (target->variant_cases.len > 0) {
        const jazz_node_PhysicalVariantCase *existing = &target->variant_cases.ptr[0];
        if (target->variant_cases.len != 1 || !jz_btree_eq(&existing->fields, &fields, NULL)) {
            err = JZN_ISV("physical table variant case definition changed");
            goto fail;
        }
        jz_btree_drop(&fields);
        jz_vec_clone(out, &target->variant_cases, sizeof(jazz_node_PhysicalVariantCase), jazz_node_PhysicalVariantCase_clone);
        return JZ_OK;
    }

    jz_btree used;
    jz_btree_init(&used, &jzn_desc_u32_uuid);
    {
        jz_uuid *candidate_schema;
        jazz_node_SchemaPhysicalMapping *mapping;
        JZ_BTREE_FOR(mappings, candidate_schema, mapping) {
            const jazz_node_TablePhysicalMapping *table = NULL;
            jz_str *name;
            jazz_node_TablePhysicalMapping *candidate;
            JZ_BTREE_FOR(&mapping->tables, name, candidate) {
                if (candidate->table_id == table_id) { table = candidate; break; }
            }
            if (!table) continue;
            if (table->variant_cases.len == 0) {
                /* The target mapping is still provisional: its alias has never
                 * been written as a row tag, and is replaced below. */
                if (jz_uuid_eq(candidate_schema, &schema_version)) continue;
                const u64 *alias = jz_btree_get(aliases, candidate_schema);
                if (!alias) { err = JZN_ISV("variant-case schema alias missing"); goto fail_used; }
                u32 tag;
                err = jazz_node__node__physical__groove_variant_tag(*alias, &tag);
                if (JZ_IS_ERR(err)) goto fail_used;
                if (jz_btree_insert(&used, &tag, candidate_schema, NULL)) {
                    err = JZN_ISV("physical table variant tag collision");
                    goto fail_used;
                }
            } else if (table->variant_cases.len != 1 ||
                       jz_btree_insert(&used, &table->variant_cases.ptr[0].tag, candidate_schema, NULL)) {
                err = JZN_ISV("physical table variant tag collision");
                goto fail_used;
            }
        }
    }
    const u64 *alias = jz_btree_get(aliases, &schema_version);
    if (!alias) { err = JZN_ISV("variant-case schema alias missing"); goto fail_used; }
    u32 tag;
    err = jazz_node__node__physical__groove_variant_tag(*alias, &tag);
    if (JZ_IS_ERR(err)) goto fail_used;
    if (jz_btree_contains(&used, &tag)) { err = JZN_ISV("physical table variant tag collision"); goto fail_used; }
    jz_btree_drop(&used);

    jazz_node_PhysicalVariantCaseVec allocated = {0};
    jazz_node_PhysicalVariantCase vc = { tag, fields };
    JZ_VEC_PUSH(allocated, vc);
    jazz_node_SchemaPhysicalMapping *mapping = jz_btree_get(mappings, &schema_version);
    jazz_node_TablePhysicalMapping *slot = mapping ? jz_btree_get(&mapping->tables, logical_table) : NULL;
    if (!slot) {
        jz_vec_drop(&allocated, sizeof(jazz_node_PhysicalVariantCase), jazz_node_PhysicalVariantCase_drop);
        return JZN_ISV("variant-case target physical mapping missing");
    }
    jz_vec_drop(&slot->variant_cases, sizeof(jazz_node_PhysicalVariantCase), jazz_node_PhysicalVariantCase_drop);
    jz_vec_clone(&slot->variant_cases, &allocated, sizeof(jazz_node_PhysicalVariantCase), jazz_node_PhysicalVariantCase_clone);
    *out = allocated;
    return JZ_OK;
fail_used:
    jz_btree_drop(&used);
fail:
    jz_btree_drop(&fields);
    return err;
}

static void jzn_u32_uuid_map_drop(void *m) { jz_btree_drop((jz_btree *)m); }
static const jz_btree_desc jzn_desc_u64_tagmap = { sizeof(u64), sizeof(jz_btree), jz_cmp_u64, NULL, jzn_u32_uuid_map_drop };

jz_err jazz_node__node__physical__validate_physical_variant_cases(const jz_btree *mappings, const jz_btree *aliases) {
    jz_btree by_table; /* PhysicalTableId -> BTreeMap<u32, SchemaVersionId> */
    jz_btree_init(&by_table, &jzn_desc_u64_tagmap);
    jz_err err = JZ_OK;
    jz_uuid *schema_version;
    jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(mappings, schema_version, mapping) {
        jz_str *name;
        jazz_node_TablePhysicalMapping *table;
        JZ_BTREE_FOR(&mapping->tables, name, table) {
            jz_btree column_ids;
            jz_btree_init(&column_ids, &jzn_desc_u64_set);
            bool dup = false;
            jz_str *cname; u64 *cid;
            JZ_BTREE_FOR(&table->columns, cname, cid) {
                if (jz_btree_insert(&column_ids, cid, NULL, NULL)) { dup = true; break; }
            }
            jz_btree_drop(&column_ids);
            if (dup) { err = JZN_ISV("physical table maps multiple columns to one id"); goto done; }
            u32 tag;
            if (table->variant_cases.len == 0) {
                const u64 *alias = jz_btree_get(aliases, schema_version);
                if (!alias) { err = JZN_ISV("variant-case schema alias missing"); goto done; }
                err = jazz_node__node__physical__groove_variant_tag(*alias, &tag);
                if (JZ_IS_ERR(err)) goto done;
            } else {
                if (table->variant_cases.len != 1) { err = JZN_ISV("physical table layout has multiple row cases"); goto done; }
                tag = table->variant_cases.ptr[0].tag;
            }
            bool inserted;
            jz_btree *tags = jz_btree_entry(&by_table, &table->table_id, &inserted);
            if (inserted) jz_btree_init(tags, &jzn_desc_u32_uuid);
            if (jz_btree_insert(tags, &tag, schema_version, NULL)) { err = JZN_ISV("physical table variant tag collision"); goto done; }
        }
    }
done:
    jz_btree_drop(&by_table);
    return err;
}

/* forward decls for the registry validators */
static jz_err jzn_validate_enum_registries(const jz_btree *columns, const jz_btree *registries, const jz_btree *aliases);
static jz_err jzn_validate_nested_enum_registries(const jz_btree *columns, const jz_btree *registries, const jz_btree *aliases);
static jz_err jzn_validate_payload_enum_registries(const jz_btree *columns, const jz_btree *registries, const jz_btree *aliases);
static jz_err jzn_validate_nested_payload_enum_registries(const jz_btree *columns, const jz_btree *registries, const jz_btree *aliases);

/* Validate physical mapping references after all local schema aliases are recovered. */
jz_err jazz_node__node__physical__validate_physical_mapping_registries(const jz_btree *mappings, const jz_btree *aliases) {
    jz_uuid *k;
    jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(mappings, k, mapping) {
        jz_btree table_ids;
        jz_btree_init(&table_ids, &jzn_desc_u64_set);
        jz_err err = JZ_OK;
        jz_str *name;
        jazz_node_TablePhysicalMapping *table;
        JZ_BTREE_FOR(&mapping->tables, name, table) {
            if (jz_btree_insert(&table_ids, &table->table_id, NULL, NULL)) {
                err = JZN_ISV("physical mapping aliases multiple tables to one id");
                break;
            }
            jz_btree columns;
            jz_btree_init(&columns, &jzn_desc_u64_set);
            jz_str *cname; u64 *cid;
            JZ_BTREE_FOR(&table->columns, cname, cid) jz_btree_insert(&columns, cid, NULL, NULL);
            if (columns.len != table->columns.len) {
                err = JZN_ISV("physical table maps multiple columns to one id");
            }
            for (u32 c = 0; !JZ_IS_ERR(err) && c < table->variant_cases.len; c++) {
                jz_str *field; void *unused;
                JZ_BTREE_FOR(&table->variant_cases.ptr[c].fields, field, unused) {
                    if (!jz_btree_contains(&table->columns, field)) {
                        err = JZN_ISV("physical table variant contains an unknown field");
                        break;
                    }
                }
            }
            if (!JZ_IS_ERR(err)) err = jzn_validate_enum_registries(&columns, &table->scalar_enum_cases, aliases);
            if (!JZ_IS_ERR(err)) err = jzn_validate_payload_enum_registries(&columns, &table->payload_enum_cases, aliases);
            if (!JZ_IS_ERR(err)) err = jzn_validate_nested_enum_registries(&columns, &table->nested_scalar_enum_cases, aliases);
            if (!JZ_IS_ERR(err)) err = jzn_validate_nested_payload_enum_registries(&columns, &table->nested_payload_enum_cases, aliases);
            jz_btree_drop(&columns);
            if (JZ_IS_ERR(err)) break;
        }
        jz_btree_drop(&table_ids);
        JZ_TRY(err);
    }
    return JZ_OK;
}

/* ---- case provenance: one copy over both case kinds (Rust has two
 * monomorphic copies of the same body) ---- */
typedef struct jzn_case_kind {
    usize elem;
    u32 (*ordinal)(const void *c);
    const char *unknown_schema, *disagrees, *not_introduction, *missing_alias;
} jzn_case_kind;
static u32 jzn_gse_ordinal(const void *c) { return ((const jazz_node_GlobalScalarEnumCaseId *)c)->introducing_ordinal; }
static u32 jzn_ge_ordinal(const void *c) { return ((const jazz_node_GlobalEnumCaseId *)c)->introducing_ordinal; }
static const jzn_case_kind jzn_scalar_kind = {
    sizeof(jazz_node_GlobalScalarEnumCaseId), jzn_gse_ordinal,
    "scalar enum registry case provenance references an unknown schema",
    "scalar enum registry case provenance disagrees with authority identities",
    "scalar enum registry case provenance does not name its introduction",
    "scalar enum registry provenance schema alias is missing",
};
static const jzn_case_kind jzn_payload_kind = {
    sizeof(jazz_node_GlobalEnumCaseId), jzn_ge_ordinal,
    "payload enum registry case provenance references an unknown schema",
    "payload enum registry case provenance disagrees with authority identities",
    "payload enum registry case provenance does not name its introduction",
    "payload enum registry provenance schema alias is missing",
};

/* origin.tables.iter().any(|(name, table)| table.table_id == table_id &&
 *   table.columns.iter().any(|(cname, cid)| cid == column_id &&
 *     identities.tables[name].columns[cname].enum_variants.values().any(pred))) */
static bool jzn_authority_has_case(const jazz_node_SchemaPhysicalMapping *origin, jazz_types_PhysicalTableId table_id,
                                   jazz_types_PhysicalColumnId column_id, const jz_uuid *case_id, bool at_ordinal, u32 ordinal) {
    jz_str *table_name;
    jazz_node_TablePhysicalMapping *table;
    JZ_BTREE_FOR(&origin->tables, table_name, table) {
        if (table->table_id != table_id) continue;
        jz_str *column_name; u64 *candidate_column;
        JZ_BTREE_FOR(&table->columns, column_name, candidate_column) {
            if (*candidate_column != column_id) continue;
            const jazz_protocol_PhysicalTableIdentity *identity_table = jz_btree_get(&origin->identities.tables, table_name);
            if (!identity_table) continue;
            const jazz_protocol_PhysicalColumnIdentity *identity_column = jz_btree_get(&identity_table->columns, column_name);
            if (!identity_column) continue;
            jz_str *path;
            JZ_VEC(jz_uuid) *variants;
            JZ_BTREE_FOR(&identity_column->enum_variants, path, variants) {
                if (at_ordinal) {
                    if (ordinal < variants->len && jz_uuid_eq(&variants->ptr[ordinal], case_id)) return true;
                } else {
                    for (u32 i = 0; i < variants->len; i++)
                        if (jz_uuid_eq(&variants->ptr[i], case_id)) return true;
                }
            }
        }
    }
    return false;
}

static jz_err jzn_validate_case_provenance_for_column(const jzn_case_kind *kind, const jz_btree *mappings, const jz_btree *aliases,
                                                     jazz_types_PhysicalTableId table_id, jazz_types_PhysicalColumnId column_id,
                                                     const jz_vec *cases) {
    for (u32 i = 0; i < cases->len; i++) {
        /* both case structs start with (id, introducing_schema) */
        const jazz_node_GlobalEnumCaseId *head = (const void *)((const u8 *)cases->ptr + (usize)i * kind->elem);
        const void *c = head;
        const jazz_node_SchemaPhysicalMapping *origin = jz_btree_get(mappings, &head->introducing_schema);
        if (!origin) return JZN_ISV(kind->unknown_schema);
        u32 ordinal = kind->ordinal(c);
        if (!jzn_authority_has_case(origin, table_id, column_id, &head->id, true, ordinal)) return JZN_ISV(kind->disagrees);
        /* earliest authority by alias */
        const jz_uuid *earliest = NULL;
        u64 earliest_alias = 0;
        jz_uuid *schema;
        jazz_node_SchemaPhysicalMapping *candidate;
        JZ_BTREE_FOR(mappings, schema, candidate) {
            if (!jzn_authority_has_case(candidate, table_id, column_id, &head->id, false, 0)) continue;
            const u64 *alias = jz_btree_get(aliases, schema);
            if (!alias) jz_panic("aliases[schema]");
            if (!earliest || *alias < earliest_alias) {
                earliest = schema;
                earliest_alias = *alias;
            }
        }
        if (!earliest) return JZN_ISV(kind->disagrees);
        if (!jz_uuid_eq(earliest, &head->introducing_schema)) return JZN_ISV(kind->not_introduction);
    }
    return JZ_OK;
}

static jz_err jzn_validate_case_provenance(const jzn_case_kind *kind, const jz_btree *mappings, const jz_btree *aliases, bool scalar) {
    jz_uuid *schema;
    jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(mappings, schema, mapping) {
        if (!jz_btree_contains(aliases, schema)) return JZN_ISV(kind->missing_alias);
    }
    JZ_BTREE_FOR(mappings, schema, mapping) {
        jz_str *name;
        jazz_node_TablePhysicalMapping *table;
        JZ_BTREE_FOR(&mapping->tables, name, table) {
            const jz_btree *root = scalar ? &table->scalar_enum_cases : &table->payload_enum_cases;
            const jz_btree *nested = scalar ? &table->nested_scalar_enum_cases : &table->nested_payload_enum_cases;
            u64 *column_id;
            jz_vec *cases;
            JZ_BTREE_FOR(root, column_id, cases) {
                JZ_TRY(jzn_validate_case_provenance_for_column(kind, mappings, aliases, table->table_id, *column_id, cases));
            }
            jz_btree *paths;
            JZ_BTREE_FOR(nested, column_id, paths) {
                jz_str *path;
                JZ_BTREE_FOR(paths, path, cases) {
                    JZ_TRY(jzn_validate_case_provenance_for_column(kind, mappings, aliases, table->table_id, *column_id, cases));
                }
            }
        }
    }
    return JZ_OK;
}

/* Payload enum registry order is physical storage identity; its authored
 * coordinates must name the authority manifest that minted the UUID. */
jz_err jazz_node__node__physical__validate_payload_enum_case_provenance(const jz_btree *mappings, const jz_btree *aliases) {
    return jzn_validate_case_provenance(&jzn_payload_kind, mappings, aliases, false);
}
jz_err jazz_node__node__physical__validate_scalar_enum_case_provenance(const jz_btree *mappings, const jz_btree *aliases) {
    return jzn_validate_case_provenance(&jzn_scalar_kind, mappings, aliases, true);
}

typedef struct jzn_case_order_ctx { const jz_btree *aliases; bool scalar; } jzn_case_order_ctx;

static jz_err jzn_validate_cases(const jz_vec *cases, const jz_btree *aliases, bool scalar) {
    usize elem = scalar ? sizeof(jazz_node_GlobalScalarEnumCaseId) : sizeof(jazz_node_GlobalEnumCaseId);
    jz_btree seen;
    jz_btree_init(&seen, scalar ? &jzn_desc_gse_set : &jzn_desc_ge_set);
    for (u32 i = 0; i < cases->len; i++) {
        const u8 *c = (const u8 *)cases->ptr + (usize)i * elem;
        static const jz_uuid nil;
        if (jz_uuid_eq((const jz_uuid *)c, &nil)) {
            jz_btree_drop(&seen);
            return JZN_ISV("physical enum registry contains a nil global identity");
        }
        if (jz_btree_insert(&seen, c, NULL, NULL)) {
            jz_btree_drop(&seen);
            return JZN_ISV("physical enum registry repeats a case identity");
        }
    }
    jz_btree_drop(&seen);
    for (u32 i = 0; i + 1 < cases->len; i++) {
        const u8 *a = (const u8 *)cases->ptr + (usize)i * elem, *b = a + elem;
        int ord = scalar ? jazz_node__node__physical__compare_scalar_enum_cases(aliases, (const void *)a, (const void *)b)
                         : jazz_node__node__physical__compare_global_enum_cases(aliases, (const void *)a, (const void *)b);
        if (ord > 0) return JZN_ISV("physical enum registry has non-canonical case order");
    }
    return JZ_OK;
}

static jz_err jzn_validate_root_registries(const jz_btree *columns, const jz_btree *registries, const jz_btree *aliases, bool scalar) {
    u64 *column;
    jz_vec *cases;
    JZ_BTREE_FOR(registries, column, cases) {
        if (!jz_btree_contains(columns, column)) return JZN_ISV("physical enum registry references an unknown column");
        JZ_TRY(jzn_validate_cases(cases, aliases, scalar));
    }
    return JZ_OK;
}
static jz_err jzn_validate_nested_registries(const jz_btree *columns, const jz_btree *registries, const jz_btree *aliases, bool scalar) {
    u64 *column;
    jz_btree *paths;
    JZ_BTREE_FOR(registries, column, paths) {
        if (!jz_btree_contains(columns, column)) return JZN_ISV("physical enum registry references an unknown column");
        jz_str *path;
        jz_vec *cases;
        JZ_BTREE_FOR(paths, path, cases) JZ_TRY(jzn_validate_cases(cases, aliases, scalar));
    }
    return JZ_OK;
}
static jz_err jzn_validate_enum_registries(const jz_btree *columns, const jz_btree *registries, const jz_btree *aliases) {
    return jzn_validate_root_registries(columns, registries, aliases, true);
}
static jz_err jzn_validate_nested_enum_registries(const jz_btree *columns, const jz_btree *registries, const jz_btree *aliases) {
    return jzn_validate_nested_registries(columns, registries, aliases, true);
}
static jz_err jzn_validate_payload_enum_registries(const jz_btree *columns, const jz_btree *registries, const jz_btree *aliases) {
    return jzn_validate_root_registries(columns, registries, aliases, false);
}
static jz_err jzn_validate_nested_payload_enum_registries(const jz_btree *columns, const jz_btree *registries, const jz_btree *aliases) {
    return jzn_validate_nested_registries(columns, registries, aliases, false);
}

static bool jzn_schema_has_table(const jazz_model_SchemaVersion *schema, const jz_str *logical_table) {
    for (u32 i = 0; i < schema->schema.tables.len; i++)
        if (jz_str_eq(&schema->schema.tables.ptr[i].name, logical_table)) return true;
    return false;
}

/* Resolve the physical history table a registered logical table scans. */
jz_err jazz_node__node__physical__physical_history_source_table(const jz_btree *catalogue_schemas, const jz_btree *aliases,
                                                               const jz_btree *mappings, jazz_types_SchemaVersionId v,
                                                               const jz_str *logical_table, jz_str *out) {
    const jazz_model_SchemaVersion *schema = jz_btree_get(catalogue_schemas, &v);
    if (!schema) return JZN_ISV("physical history schema missing");
    if (!jzn_schema_has_table(schema, logical_table)) return jzn_table_not_found(logical_table->ptr, logical_table->len);
    if (!jz_btree_contains(aliases, &v)) return JZN_ISV("physical history schema alias missing");
    return jazz_node__node__physical__physical_history_storage_table(mappings, v, logical_table, out);
}

jz_err jazz_node__node__physical__physical_history_storage_table(const jz_btree *mappings, jazz_types_SchemaVersionId v,
                                                                const jz_str *logical_table, jz_str *out) {
    const jazz_node_TablePhysicalMapping *mapping = jzn_lookup_table(mappings, &v, logical_table);
    if (!mapping) return JZN_ISV("physical history table mapping missing");
    *out = jazz_node__node__physical__physical_history_table_name(mapping->table_id);
    return JZ_OK;
}

jz_err jazz_node__node__physical__physical_current_source_table(const jz_btree *catalogue_schemas, const jz_btree *mappings,
                                                               jazz_types_SchemaVersionId v, const jz_str *logical_table,
                                                               jazz_node_PhysicalCurrentClass cls, jz_str *out) {
    const jazz_model_SchemaVersion *schema = jz_btree_get(catalogue_schemas, &v);
    if (!schema) return JZN_ISV("physical current schema missing");
    if (!jzn_schema_has_table(schema, logical_table)) return jzn_table_not_found(logical_table->ptr, logical_table->len);
    return jazz_node__node__physical__physical_current_storage_table(mappings, v, logical_table, cls, out);
}

jz_err jazz_node__node__physical__physical_current_storage_table(const jz_btree *mappings, jazz_types_SchemaVersionId v,
                                                                const jz_str *logical_table, jazz_node_PhysicalCurrentClass cls,
                                                                jz_str *out) {
    const jazz_node_TablePhysicalMapping *mapping = jzn_lookup_table(mappings, &v, logical_table);
    if (!mapping) return JZN_ISV("physical current table mapping missing");
    switch (cls) {
    case JZN_PCC_GLOBAL: *out = jazz_node__node__physical__physical_global_current_table_name(mapping->table_id); break;
    case JZN_PCC_AHEAD: *out = jazz_node__node__physical__physical_ahead_current_table_name(mapping->table_id); break;
    }
    return JZ_OK;
}

jz_err jazz_node__node__physical__physical_rejected_version_binding(const jz_btree *catalogue_schemas, const jz_btree *mappings,
                                                                   jazz_types_SchemaVersionId v, const jz_str *logical_table,
                                                                   jazz_node_PhysicalHistoryBinding *out) {
    const jazz_model_SchemaVersion *schema = jz_btree_get(catalogue_schemas, &v);
    if (!schema) return JZN_ISV("physical rejected-version schema missing");
    const jazz_model_TableSchema *table = NULL;
    for (u32 i = 0; i < schema->schema.tables.len; i++)
        if (jz_str_eq(&schema->schema.tables.ptr[i].name, logical_table)) { table = &schema->schema.tables.ptr[i]; break; }
    if (!table) return jzn_table_not_found(logical_table->ptr, logical_table->len);
    const jazz_node_TablePhysicalMapping *mapping = jzn_lookup_table(mappings, &v, logical_table);
    if (!mapping) return JZN_ISV("physical rejected-version table mapping missing");
    groove_RecordDescriptor descriptor;
    JZ_TRY(jazz_node__node__physical__physical_rejected_version_descriptor(table, mapping, &descriptor));
    out->storage_table = jazz_node__node__physical__physical_rejected_versions_table_name(mapping->table_id);
    out->descriptor = descriptor;
    return JZ_OK;
}
