/* Port of crates/jazz/layers/node/src/node/catalogue_ingest.rs
 * Trusted catalogue snapshot ingestion and planning.
 * (validate_deployment_snapshot is cfg(feature = "runtime") and absent from the
 * browser build; the testing failpoint is cfg(any(test, feature = "testing")).) */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

typedef struct jazz_node_PlannedCatalogueSnapshot {
    jazz_node_SchemaCatalogue catalogue;
    JZ_VEC(jazz_node_StagedSchemaLineage) activated_lineages;
} jazz_node_PlannedCatalogueSnapshot;

static jz_err invalid_update(const char *m) { return JZN_INVALID_CATALOGUE(m); }

/* GlobalPhysicalEnumVariantId -> GlobalPhysicalEnumVariantId */
static const jz_btree_desc desc_uuid_uuid = { sizeof(jz_uuid), sizeof(jz_uuid), jz_cmp_uuid, NULL, NULL };
/* String -> String */
static void str_drop_v(void *p) { jz_str_drop(p); }
static const jz_btree_desc desc_str_str = { sizeof(jz_str), sizeof(jz_str), jz_cmp_str, str_drop_v, str_drop_v };

/* Returns whether a trusted catalogue replacement changes the *live* Groove
 * runtime's effective input.  The catalogue is deliberately much larger than
 * that input: it also retains historical authority schemas, lenses, and
 * permanent identity manifests for validation and history decoding.  Learning
 * any of those facts must not tear down subscriptions that are compiled only
 * against this node's active local/read schema and its local physical layout.
 *
 * The active schema alias and mapping remain part of the runtime contract:
 * Groove records use the alias as their variant discriminator, while table,
 * column, and enum registries in `tables` determine their physical layout.
 * `identities` are global catalogue evidence, not a Groove layout input. */
bool jazz_node__node__catalogue_ingest__active_runtime_layouts_equal(const jazz_node_SchemaCatalogue *left, const jazz_node_SchemaCatalogue *right) {
    if (!jz_uuid_eq(&left->local_schema_version_id, &right->local_schema_version_id)
        || left->has_local_schema_version_alias != right->has_local_schema_version_alias
        || (left->has_local_schema_version_alias && left->local_schema_version_alias != right->local_schema_version_alias)
        || !jazz_model__schema__JazzSchema__eq(left->schema, right->schema))
        return false;
    jazz_types_SchemaVersionId active = left->local_schema_version_id;
    const u64 *la = jz_btree_get(&left->schema_version_aliases, &active);
    const u64 *ra = jz_btree_get(&right->schema_version_aliases, &active);
    if ((la == NULL) != (ra == NULL) || (la && *la != *ra)) return false;
    const jazz_node_SchemaPhysicalMapping *lm = jz_btree_get(&left->physical_mappings, &active);
    const jazz_node_SchemaPhysicalMapping *rm = jz_btree_get(&right->physical_mappings, &active);
    return lm && rm && jz_btree_eq(&lm->tables, &rm->tables, jazz_node__node__physical__TablePhysicalMapping__eq_v);
}

jz_err jazz_node__node__catalogue_ingest__next_schema_version_alias_in_catalogue(const jazz_node_SchemaCatalogue *catalogue, jazz_types_SchemaVersionAlias *out) {
    u64 max = 0;
    const jz_uuid *k;
    const u64 *alias;
    JZ_BTREE_FOR(&catalogue->schema_version_aliases, k, alias) { (void)k; if (*alias > max) max = *alias; }
    if (max == UINT64_MAX) return JZN_INVALID_STORED("schema version alias exhausted");
    *out = max + 1;
    return JZ_OK;
}

jz_err jazz_node__node__catalogue_ingest__rebind_mapping_enum_cases(jazz_node_SchemaPhysicalMapping *mapping, const jz_btree *replacements);

/* Adopt the authority's UUIDs without changing local enum tags. A node can
 * have persisted rows before its first trusted snapshot, so replacing only
 * the manifest leaves those rows' registries referring to provisional UUIDs.
 * The two manifests describe the same authored schema: their occurrence and
 * ordinal coordinates identify the corresponding cases, including UUIDs in
 * nested payload paths. Registry order and introduction coordinates stay put.
 * Consumes `identities`; `out` is GlobalPhysicalEnumVariantId -> id. */
jz_err jazz_node__node__catalogue_ingest__rebind_provisional_mapping_identities(jazz_node_SchemaPhysicalMapping *mapping,
                                                                               jazz_protocol_PhysicalIdentityManifest *identities, jz_btree *out) {
    jz_btree_init(out, &desc_uuid_uuid);
    if (jazz_protocol__protocol__PhysicalIdentityManifest__eq(mapping->identities, identities)) {
        jazz_protocol__protocol__PhysicalIdentityManifest__drop(identities);
        return JZ_OK;
    }
    jz_btree replacements, origins;
    jz_btree_init(&replacements, &desc_uuid_uuid);
    jz_btree_init(&origins, &desc_uuid_uuid);
    jz_err e = JZ_OK;
    const jz_str *table_name;
    const jazz_protocol_PhysicalTableIdentity *table;
    JZ_BTREE_FOR(&mapping->identities->tables, table_name, table) {
        const jazz_protocol_PhysicalTableIdentity *authority_table = jz_btree_get(&identities->tables, table_name);
        if (!authority_table) { e = invalid_update("snapshot identity table missing"); goto fail; }
        const jz_str *column_name;
        const jazz_protocol_PhysicalColumnIdentity *column;
        JZ_BTREE_FOR(&table->columns, column_name, column) {
            const jazz_protocol_PhysicalColumnIdentity *authority_column = jz_btree_get(&authority_table->columns, column_name);
            if (!authority_column) { e = invalid_update("snapshot identity column missing"); goto fail; }
            const jz_str *path;
            const jzn_uuidvec *cases;
            JZ_BTREE_FOR(&column->enum_variants, path, cases) {
                const jzn_uuidvec *authority_cases = jz_btree_get(&authority_column->enum_variants, path);
                if (!authority_cases) { e = invalid_update("snapshot enum identity occurrence missing"); goto fail; }
                if (cases->len != authority_cases->len) { e = invalid_update("snapshot enum identity occurrence width differs"); goto fail; }
                for (u32 i = 0; i < cases->len; i++) {
                    jz_uuid old = cases->ptr[i], nw = authority_cases->ptr[i], prev;
                    bool ambiguous = false;
                    if (jz_btree_insert(&replacements, &old, &nw, &prev) && !jz_uuid_eq(&prev, &nw)) ambiguous = true;
                    if (jz_btree_insert(&origins, &nw, &old, &prev) && !jz_uuid_eq(&prev, &old)) ambiguous = true;
                    if (ambiguous) { e = invalid_update("snapshot enum identity rebind is ambiguous"); goto fail; }
                }
            }
        }
    }
    {
        const jz_uuid *old;
        const jz_uuid *nw;
        JZ_BTREE_FOR(&replacements, old, nw) {
            if (!jz_uuid_eq(old, nw) && jz_btree_contains(&replacements, nw)) {
                e = invalid_update("snapshot enum identity rebind overlaps provisional identities");
                goto fail;
            }
        }
    }
    e = jazz_node__node__catalogue_ingest__rebind_mapping_enum_cases(mapping, &replacements);
    if (e.code) goto fail;
    jazz_protocol__protocol__PhysicalIdentityManifest__drop(mapping->identities);
    mapping->identities = identities;
    jz_btree_drop(&origins);
    jz_btree_drop(out);
    *out = replacements;
    return JZ_OK;
fail:
    jz_btree_drop(&replacements);
    jz_btree_drop(&origins);
    jazz_protocol__protocol__PhysicalIdentityManifest__drop(identities);
    return e;
}

/* uuid.simple().to_string(): 32 lowercase hex digits */
static jz_str uuid_simple(const jz_uuid *u) {
    static const char hex[] = "0123456789abcdef";
    char buf[32];
    for (int i = 0; i < 16; i++) { buf[2 * i] = hex[u->b[i] >> 4]; buf[2 * i + 1] = hex[u->b[i] & 15]; }
    return jz_str_from_slice(jz_slice_of(buf, 32));
}

/* path.split('/') with each component after a "case" component replaced. */
static jz_str rebind_path(const jz_btree *path_replacements, const jz_str *path) {
    jz_str out = { 0 };
    jz_slice previous = { (const u8 *)"", 0 };
    u32 start = 0;
    bool first = true;
    for (u32 i = 0; i <= path->len; i++) {
        if (i < path->len && path->ptr[i] != '/') continue;
        jz_slice component = jz_slice_of(path->ptr + start, i - start);
        jz_slice rebound = component;
        if (previous.len == 4 && memcmp(previous.ptr, "case", 4) == 0) {
            jz_str key = jz_str_from_slice(component);
            const jz_str *r = jz_btree_get(path_replacements, &key);
            if (r) rebound = jz_str_slice(r);
            jz_str_drop(&key);
        }
        if (!first) jz_str_push_char(&out, '/');
        jz_str_push(&out, (const char *)rebound.ptr, rebound.len);
        first = false;
        previous = component;
        start = i + 1;
    }
    return out;
}

static void rebind_scalar_cases(jazz_node_GlobalScalarEnumCaseIdVec *cases, const jz_btree *replacements) {
    for (u32 i = 0; i < cases->len; i++) {
        const jz_uuid *id = jz_btree_get(replacements, &cases->ptr[i].id);
        if (id) cases->ptr[i].id = *id;
    }
}
static void rebind_payload_cases(jazz_node_GlobalEnumCaseIdVec *cases, const jz_btree *replacements) {
    for (u32 i = 0; i < cases->len; i++) {
        const jz_uuid *id = jz_btree_get(replacements, &cases->ptr[i].id);
        if (id) cases->ptr[i].id = *id;
    }
}

/* nested map: String path -> Vec<case>; `payload` selects the case type. */
static jz_err rebind_nested_paths(jz_btree *paths, const jz_btree *replacements, const jz_btree *path_replacements, bool payload) {
    jz_btree previous = *paths;
    jz_btree_init(paths, previous.d);
    jz_str path;
    jazz_node_GlobalScalarEnumCaseIdVec scalar_cases;
    jazz_node_GlobalEnumCaseIdVec payload_cases;
    void *val = payload ? (void *)&payload_cases : (void *)&scalar_cases;
    while (jz_btree_pop_first(&previous, &path, val)) {
        if (payload) rebind_payload_cases(&payload_cases, replacements);
        else rebind_scalar_cases(&scalar_cases, replacements);
        jz_str rebound = rebind_path(path_replacements, &path);
        jz_str_drop(&path);
        if (jz_btree_insert(paths, &rebound, val, NULL)) {
            jz_btree_drop(&previous);
            return invalid_update("snapshot enum occurrence rebind collides");
        }
    }
    jz_btree_drop(&previous);
    return JZ_OK;
}

jz_err jazz_node__node__catalogue_ingest__rebind_mapping_enum_cases(jazz_node_SchemaPhysicalMapping *mapping, const jz_btree *replacements) {
    if (replacements->len == 0) return JZ_OK;
    jz_btree path_replacements;
    jz_btree_init(&path_replacements, &desc_str_str);
    {
        const jz_uuid *old, *nw;
        JZ_BTREE_FOR(replacements, old, nw) {
            jz_str k = uuid_simple(old), v = uuid_simple(nw);
            jz_btree_insert(&path_replacements, &k, &v, NULL);
        }
    }
    jz_err e = JZ_OK;
    const jz_str *tname;
    jazz_node_TablePhysicalMapping *table;
    JZ_BTREE_FOR(&mapping->tables, tname, table) {
        (void)tname;
        {
            const u64 *col; jazz_node_GlobalScalarEnumCaseIdVec *cases;
            JZ_BTREE_FOR(&table->scalar_enum_cases, col, cases) { (void)col; rebind_scalar_cases(cases, replacements); }
        }
        {
            const u64 *col; jazz_node_GlobalEnumCaseIdVec *cases;
            JZ_BTREE_FOR(&table->payload_enum_cases, col, cases) { (void)col; rebind_payload_cases(cases, replacements); }
        }
        {
            const u64 *col; jz_btree *paths;
            JZ_BTREE_FOR(&table->nested_scalar_enum_cases, col, paths) {
                (void)col;
                e = rebind_nested_paths(paths, replacements, &path_replacements, false);
                if (e.code) goto done;
            }
        }
        {
            const u64 *col; jz_btree *paths;
            JZ_BTREE_FOR(&table->nested_payload_enum_cases, col, paths) {
                (void)col;
                e = rebind_nested_paths(paths, replacements, &path_replacements, true);
                if (e.code) goto done;
            }
        }
    }
done:
    jz_btree_drop(&path_replacements);
    return e;
}

/* Roll a failed snapshot activation back to `previous` (and the runtime
 * registry to `checkpoint`), and refuse further catalogue activation. */
static jz_err fail_catalogue_activation(NS *self, jazz_node_SchemaCatalogue previous, groove_RuntimeRegistryCheckpoint *checkpoint) {
    jazz_node__node__SchemaCatalogue__drop(&self->catalogue);
    self->catalogue = previous;
    if (checkpoint) groove__db__schema_admission__Database__restore_runtime_registry(jzn_db(self), checkpoint);
    self->catalogue_activation_failed = true;
    return jz_err_new(JZN_E_CATALOGUE_ACTIVATION_FAILED, NULL);
}

static jz_err plan_trusted_catalogue_snapshot(const NS *self, jazz_protocol_CatalogueSnapshot snapshot, jazz_node_PlannedCatalogueSnapshot *out);

/* catalogue_genesis: the catalogue schema that is not a lineage target. */
static jz_err catalogue_genesis(const jazz_node_SchemaCatalogue *catalogue, jazz_types_SchemaVersionId *out) {
    const jz_uuid *schema;
    void *v;
    JZ_BTREE_FOR(&catalogue->catalogue_schemas, schema, v) {
        (void)v;
        if (!jz_btree_contains(&catalogue->active_lineages_by_target, schema)) { *out = *schema; return JZ_OK; }
    }
    return JZN_INVALID_STORED("catalogue genesis schema is missing");
}

static void clear_btree(jz_btree *t) { jz_btree_clear(t); }

jz_err jazz_node__node__catalogue_ingest__NodeState__apply_trusted_catalogue_snapshot(NS *self, jazz_protocol_CatalogueSnapshot snapshot, jazz_node_PublicationOutcome *out) {
    /* A rolled-back activation can retry while its old runtime is usable.
     * Failed persistence retains the catalogue-specific terminal error. */
    jz_err e = groove__db__facade__Database__ensure_usable(jzn_db(self));
    if (e.code) {
        if (self->catalogue_activation_failed) { jz_err_drop(&e); return jz_err_new(JZN_E_CATALOGUE_ACTIVATION_FAILED, NULL); }
        return e;
    }
    if (groove__db__facade__Database__has_unsettled_publications(jzn_db(self)))
        return jz_err_new(GROOVE_DB_E_UNSETTLED_PUBLICATIONS, "unsettled publications");
    bool bootstrap_uninitialized = self->catalogue_bootstrap_state == JZN_BOOTSTRAP_UNINITIALIZED;
    jazz_node_PlannedCatalogueSnapshot plan;
    JZ_TRY(plan_trusted_catalogue_snapshot(self, snapshot, &plan));
    jazz_types_SchemaVersionId planned_genesis, previous_genesis;
    e = catalogue_genesis(&plan.catalogue, &planned_genesis);
    if (e.code) goto drop_plan;
    bool runtime_semantics_changed = !jazz_node__node__catalogue_ingest__active_runtime_layouts_equal(&self->catalogue, &plan.catalogue);
    /* Global UUID adoption does not change local storage layouts, but live
     * query metadata captures those identities (including table/column/enum
     * manifests). Retire those plans and authority proofs after activation
     * while retaining the storage runtime and raw history subscriptions. */
    bool physical_identities_changed = false;
    {
        const jz_uuid *schema;
        const jazz_node_SchemaPhysicalMapping *previous;
        JZ_BTREE_FOR(&self->catalogue.physical_mappings, schema, previous) {
            const jazz_node_SchemaPhysicalMapping *next = jz_btree_get(&plan.catalogue.physical_mappings, schema);
            if (next && !jazz_protocol__protocol__PhysicalIdentityManifest__eq(previous->identities, next->identities)) { physical_identities_changed = true; break; }
        }
    }
    bool authorization_source_changed = !jazz_node__node__ActiveSchema__same_authorization_source(
        &self->catalogue.active_schema, &plan.catalogue.active_schema,
        jz_btree_get(&self->catalogue.physical_mappings, &self->catalogue.active_schema.schema),
        jz_btree_get(&plan.catalogue.physical_mappings, &plan.catalogue.active_schema.schema));
    jazz_node_SchemaCatalogue previous_catalogue = self->catalogue;
    jazz_node__node__SchemaCatalogue__Clone__clone(&self->catalogue, &plan.catalogue);
    e = catalogue_genesis(&previous_catalogue, &previous_genesis);
    if (e.code) { jazz_node__node__SchemaCatalogue__drop(&previous_catalogue); goto drop_plan; }
    /* Historical/import-only snapshot growth must not be visible through
     * Groove until the catalogue records which explain that registry are
     * durably activated.  A read-layout change takes the deliberately
     * separate rebuild path below. */
    groove_RuntimeRegistryCheckpoint *runtime_registry_checkpoint =
        !runtime_semantics_changed ? groove__db__schema_admission__Database__runtime_registry_checkpoint(jzn_db(self)) : NULL;
    /* Snapshot replay can install widened mappings after a persistent runtime
     * restart. Rebuild the live projection registry as part of that
     * semantic transition, not after client traffic begins. An identical
     * trusted prefix is idempotent and must retain maintained/query state. */
    if (runtime_semantics_changed) {
        jz_err re = jazz_node__node__NodeState__rebuild_database_slot(self);
        if (re.code) { jz_err_drop(&re); e = fail_catalogue_activation(self, previous_catalogue, NULL); goto drop_plan; }
    } else {
        jz_err se = jazz_node__node__physical__NodeState__synchronize_physical_version_tables(self);
        if (se.code) {
            /* A snapshot may extend historical or write-schema variants while
             * leaving the local read layout unchanged.  Their physical columns,
             * indices and projections must be available before authored history
             * or a later write-schema pointer can be admitted. */
            jz_err_drop(&se);
            e = fail_catalogue_activation(self, previous_catalogue, runtime_registry_checkpoint);
            goto drop_plan;
        }
    }

    groove_DatabaseBatch *batch = groove__db__facade__Database__open_batch(jzn_db(self));
    /* A snapshot's root is the authority's permanent genesis, rather than
     * its current write schema.  A pre-bootstrap receiver may have opened
     * against a descendant-shaped local schema; replace that provisional
     * marker atomically with the authority root before it can be recovered. */
    bool genesis_moved = !jz_uuid_eq(&previous_genesis, &planned_genesis);
    if (genesis_moved) jazz_node__node__delete_catalogue(batch, JZN_CAT_GENESIS, previous_genesis);
    if (bootstrap_uninitialized || self->catalogue_bootstrap_marker || genesis_moved) {
        /* This is intentionally in the same batch as every imported schema,
         * physical mapping, lineage activation, and write pointer. */
        jz_bytes empty = { 0 };
        jazz_node__node__put_catalogue(batch, JZN_CAT_GENESIS, planned_genesis, empty);
        jazz_node_CatalogueBootstrapReady ready = {
            planned_genesis, jazz_node__node__ActiveSchema__wire_pointer(&plan.catalogue.active_schema), plan.catalogue.active_catalogue_seq,
        };
        jazz_node__node__put_catalogue(batch, JZN_CAT_BOOTSTRAP_READY, ready.genesis, jazz_node__node__codec__encode_catalogue_bootstrap_ready(&ready));
    }
    /* Retain the immutable lineage receipts, then write the selected
     * structural payloads so first install and replay persist the same view. */
    for (u32 i = 0; i < plan.activated_lineages.len && !e.code; i++)
        e = jazz_node__node__NodeState__write_active_schema_lineage_to_batch(batch, &plan.activated_lineages.ptr[i]);
    if (!e.code) {
        const jz_uuid *k;
        jazz_protocol_SchemaVersion **schema;
        JZ_BTREE_FOR(&plan.catalogue.catalogue_schemas, k, schema) {
            (void)k;
            jazz_protocol_SchemaVersion **prev = jz_btree_get(&previous_catalogue.catalogue_schemas, &(*schema)->id);
            if (prev && jazz_protocol__protocol__SchemaVersion__eq(*prev, *schema)) continue;
            jz_bytes payload = { 0 };
            e = jazz_node__node__codec__encode_catalogue_schema(*schema, &payload);
            if (e.code) break;
            jazz_node__node__put_catalogue(batch, JZN_CAT_SCHEMA, (*schema)->id, payload);
        }
    }
    if (!e.code) {
        const jz_uuid *schema_version;
        const jazz_node_SchemaPhysicalMapping *mapping;
        JZ_BTREE_FOR(&plan.catalogue.physical_mappings, schema_version, mapping) {
            const u64 *alias_p = jz_btree_get(&plan.catalogue.schema_version_aliases, schema_version);
            if (!alias_p) jz_panic("schema version alias index");
            u64 alias = *alias_p;
            const jazz_node_SchemaPhysicalMapping *prev_mapping = jz_btree_get(&previous_catalogue.physical_mappings, schema_version);
            const u64 *prev_alias = jz_btree_get(&previous_catalogue.schema_version_aliases, schema_version);
            if (prev_mapping && jazz_node__node__physical__SchemaPhysicalMapping__eq(prev_mapping, mapping) && prev_alias && *prev_alias == alias) continue;
            e = jazz_node__node__NodeState__write_schema_version_mapping_to_batch(batch, alias, *schema_version, mapping);
            if (e.code) break;
        }
    }
    if (!e.code) e = jazz_node__node__NodeState__write_active_schema_to_batch(batch, &plan.catalogue.active_schema);
    if (e.code) { groove__db__batch__DatabaseBatch__drop(batch); jazz_node__node__SchemaCatalogue__drop(&previous_catalogue); goto drop_plan; }
    /* A catalogue-only batch writes no history rows, so it applies directly
     * rather than through `apply_node_batch`. */
    {
        groove_AppliedBatch *applied = NULL;
        jz_err pe = groove__db__commit__Database__apply_batch(jzn_db(self), batch, &applied);
        if (!pe.code) {
            groove_PersistOutcome *persisted = groove__db__commit__Database__persist_with_progress(jzn_db(self), applied);
            pe = groove__db__commit__Database__finish_persistence(jzn_db(self), persisted);
            groove__db__AppliedBatch__drop(applied);
        }
        if (pe.code) {
            jz_err_drop(&pe);
            e = fail_catalogue_activation(self, previous_catalogue, runtime_registry_checkpoint);
            goto drop_plan;
        }
    }
    jazz_node__node__SchemaCatalogue__drop(&previous_catalogue);

    clear_btree(&self->catalogue.staged_lineages);
    clear_btree(&self->catalogue.pending_lineages);
    clear_btree(&self->catalogue.lens_path_cache);
    clear_btree(&self->catalogue.compiled_lens_cache);
    clear_btree(&self->catalogue.physical_write_plan_cache);
    clear_btree(&self->catalogue.physical_current_winner_projections);
    clear_btree(&self->query.version_storage_sources_cache);
    clear_btree(&self->query.query_shape_cache);
    clear_btree(&self->query.compiled_query_program_cache);
    jazz_engine__query_engine__QueryProgramTemplateCache__clear(self->query.query_program_templates);
    {
        u32 elem = self->query.supported_query_program_requests.elem;
        jz_deque_drop(&self->query.supported_query_program_requests, jazz_node__node__query_eval__SupportedQueryProgram__drop);
        jz_deque_init(&self->query.supported_query_program_requests, elem);
    }
    clear_btree(&self->query.read_policy_authorization_request_cache);
    clear_btree(&self->query.policy_authorization_graph_cache);
    clear_btree(&self->query.policy_authorization_graph_replacements);
    /* A trusted snapshot is a complete replacement of the authority
     * catalogue. Once its registry and durable receipt have both crossed
     * the activation boundary, it also proves that a transient earlier
     * activation failure has been repaired. */
    self->catalogue_activation_failed = false;
    self->catalogue_bootstrap_state = JZN_BOOTSTRAP_READY;
    self->catalogue_bootstrap_marker |= bootstrap_uninitialized;
    if (physical_identities_changed) {
        jazz_node__node__NodeState__invalidate_subscription_scopes(self);
        self->physical_identity_generation = jazz_node__node__next_groove_runtime_token();
    }
    if (runtime_semantics_changed || authorization_source_changed) self->groove_runtime_token = jazz_node__node__next_groove_runtime_token();
    jazz_node_PublicationOutcome drained = { 0 };
    e = jazz_node__node__ingest__NodeState__drain_parked_commit_units(self, &drained);
    if (!e.code) e = jazz_node__node__ingest__NodeState__drain_parked_relay_commit_units(self);
    if (!e.code) e = jazz_node__node__query_eval__subscriptions__NodeState__drain_parked_shape_registrations(self);
    if (!e.code) {
        out->value = NULL;
        out->publications = drained.publications;
        out->post_settlement_work = drained.post_settlement_work;
    }
drop_plan:
    jazz_node__node__SchemaCatalogue__drop(&plan.catalogue);
    jz_vec_drop(&plan.activated_lineages, sizeof(jazz_node_StagedSchemaLineage), jazz_node__node__StagedSchemaLineage__drop);
    return e;
}

/* --- helpers for plan_trusted_catalogue_snapshot --- */
static void schema_version_ptr_drop(void *p) { jazz_protocol_SchemaVersion *v = *(jazz_protocol_SchemaVersion **)p; jazz_protocol__protocol__SchemaVersion__drop(v); jz_free(v); }
/* SchemaVersionId -> SchemaVersion* (owned) */
static const jz_btree_desc desc_schema_map = { sizeof(jz_uuid), sizeof(jazz_protocol_SchemaVersion *), jz_cmp_uuid, NULL, schema_version_ptr_drop };
static jazz_protocol_SchemaVersion *schema_box(const jazz_protocol_SchemaVersion *s) {
    jazz_protocol_SchemaVersion *b = JZ_NEW(jazz_protocol_SchemaVersion);
    jazz_protocol__protocol__SchemaVersion__clone(b, s);
    return b;
}
static void insert_schema(jz_btree *catalogue_schemas, const jazz_protocol_SchemaVersion *s) {
    jazz_protocol_SchemaVersion *b = schema_box(s);
    jazz_protocol_SchemaVersion *old;
    if (jz_btree_insert(catalogue_schemas, &b->id, &b, &old)) { jazz_protocol__protocol__SchemaVersion__drop(old); jz_free(old); }
}
static void insert_mapping(jz_btree *mappings, jazz_types_SchemaVersionId id, jazz_node_SchemaPhysicalMapping m) {
    jazz_node_SchemaPhysicalMapping old;
    if (jz_btree_insert(mappings, &id, &m, &old)) jazz_node__node__physical__SchemaPhysicalMapping__drop(&old);
}
static void insert_staged(jz_btree *t, jazz_types_SchemaVersionId id, const jazz_node_StagedSchemaLineage *staged) {
    jazz_node_StagedSchemaLineage c, old;
    jazz_node__node__StagedSchemaLineage__Clone__clone(&c, staged);
    if (jz_btree_insert(t, &id, &c, &old)) jazz_node__node__StagedSchemaLineage__drop(&old);
}
static int cmp_seq_pub(const void *a, const void *b, void *c) {
    (void)c;
    u64 x = ((const jazz_protocol_SeqPublication *)a)->catalogue_seq, y = ((const jazz_protocol_SeqPublication *)b)->catalogue_seq;
    return x < y ? -1 : x > y;
}
static int cmp_staged_seq(const void *a, const void *b, void *c) {
    (void)c;
    u64 x = ((const jazz_node_StagedSchemaLineage *)a)->catalogue_seq, y = ((const jazz_node_StagedSchemaLineage *)b)->catalogue_seq;
    return x < y ? -1 : x > y;
}
static int cmp_staged_seq_rev(const void *a, const void *b, void *c) { return -cmp_staged_seq(a, b, c); }
typedef JZ_VEC(jazz_node_StagedSchemaLineage) staged_vec;
static void staged_values(const jz_btree *t, staged_vec *out) {
    const jz_uuid *k;
    const jazz_node_StagedSchemaLineage *v;
    JZ_BTREE_FOR(t, k, v) {
        (void)k;
        jazz_node_StagedSchemaLineage c;
        jazz_node__node__StagedSchemaLineage__Clone__clone(&c, v);
        JZ_VEC_PUSH(*out, c);
    }
}

static jz_err plan_trusted_catalogue_snapshot(const NS *self, jazz_protocol_CatalogueSnapshot snapshot, jazz_node_PlannedCatalogueSnapshot *out) {
    if (self->catalogue.pending_lineages.len != 0 || self->catalogue.staged_lineages.len != 0)
        return invalid_update("trusted catalogue snapshot conflicts with pending catalogue work");

    jazz_protocol_PhysicalIdentityManifest *genesis_physical_identities = snapshot.genesis_physical_identities;
    jz_btree schemas, lineage_targets, genesis_rebindings;
    jz_btree_init(&schemas, &desc_schema_map);
    jz_btree_init(&lineage_targets, &jzn_desc_uuid_set);
    jz_btree_init(&genesis_rebindings, &desc_uuid_uuid);
    jazz_node_PlannedCatalogueSnapshot result;
    memset(&result, 0, sizeof result);
    jazz_node_SchemaCatalogue *planned = &result.catalogue;
    bool have_planned = false;
    jz_err e = JZ_OK;
    for (u32 i = 0; i < snapshot.schemas.len; i++) {
        jazz_protocol_SchemaVersion *schema = &snapshot.schemas.ptr[i];
        jazz_types_SchemaVersionId vid = jazz_model__schema__RuntimeSchema__version_id(schema->schema);
        if (!jz_uuid_eq(&schema->id, &vid)) { e = invalid_update("trusted catalogue snapshot schema id mismatch"); goto fail; }
        if (jz_btree_contains(&schemas, &schema->id)) { e = invalid_update("trusted catalogue snapshot repeats a schema id"); goto fail; }
        jazz_protocol_SchemaVersion *b = schema_box(schema);
        jz_btree_insert(&schemas, &b->id, &b, NULL);
    }

    jazz_protocol_SeqPublicationVec lineages = snapshot.lineages;
    jz_sort(lineages.ptr, lineages.len, sizeof(jazz_protocol_SeqPublication), cmp_seq_pub, NULL);
    for (u32 i = 0; i < lineages.len; i++) jz_btree_insert(&lineage_targets, &lineages.ptr[i].publication.schema.id, NULL, NULL);
    /* §10.2 has one durable genesis and every other schema enters through
     * its lineage-defining bundle. */
    jazz_types_SchemaVersionId genesis_id;
    u32 genesis_count = 0;
    {
        const jz_uuid *id; void *v;
        JZ_BTREE_FOR(&schemas, id, v) { (void)v; if (!jz_btree_contains(&lineage_targets, id)) { genesis_id = *id; genesis_count++; } }
    }
    if (genesis_count != 1) { e = invalid_update("trusted catalogue snapshot must contain exactly one genesis schema"); goto fail; }
    const jazz_protocol_SchemaVersion *bootstrap_genesis = NULL;
    if (self->catalogue_bootstrap_state == JZN_BOOTSTRAP_UNINITIALIZED) {
        jazz_protocol_SchemaVersion **g = jz_btree_get(&schemas, &genesis_id);
        if (!g) { e = invalid_update("trusted bootstrap snapshot omits genesis payload"); goto fail; }
        bootstrap_genesis = *g;
    }

    bool has_anchor = false;
    jazz_types_SchemaVersionAlias anchor_alias = 0;
    jazz_node_SchemaPhysicalMapping anchor_mapping;
    if (self->catalogue_bootstrap_state != JZN_BOOTSTRAP_UNINITIALIZED) {
        const u64 *a = jz_btree_get(&self->catalogue.schema_version_aliases, &self->catalogue.local_schema_version_id);
        const jazz_node_SchemaPhysicalMapping *m = jz_btree_get(&self->catalogue.physical_mappings, &self->catalogue.local_schema_version_id);
        if (a && m) { has_anchor = true; anchor_alias = *a; jazz_node__node__physical__SchemaPhysicalMapping__clone(&anchor_mapping, m); }
    }

    if (bootstrap_genesis) {
        u64 next_physical_table_id = 1, next_physical_column_id = 1;
        jazz_node_SchemaPhysicalMapping mapping;
        e = jazz_node__node__physical__allocate_provisional_physical_mapping(bootstrap_genesis->schema, jazz_protocol__protocol__PhysicalIdentityManifest__clone(genesis_physical_identities),
                                                                            &next_physical_table_id, &next_physical_column_id, &mapping);
        if (e.code) goto fail;
        jazz_types_SchemaVersionId gid = bootstrap_genesis->id;
        memset(planned, 0, sizeof *planned);
        planned->local_schema_version_id = gid;
        planned->has_local_schema_version_alias = true;
        planned->local_schema_version_alias = 1;
        planned->schema = jazz_model__schema__JazzSchema__clone(bootstrap_genesis->schema);
        jz_btree_init(&planned->schema_version_aliases, self->catalogue.schema_version_aliases.d);
        u64 one = 1;
        jz_btree_insert(&planned->schema_version_aliases, &gid, &one, NULL);
        jz_btree_init(&planned->catalogue_schemas, self->catalogue.catalogue_schemas.d);
        insert_schema(&planned->catalogue_schemas, bootstrap_genesis);
        jz_btree_init(&planned->catalogue_lenses, self->catalogue.catalogue_lenses.d);
        jz_btree_init(&planned->physical_mappings, self->catalogue.physical_mappings.d);
        insert_mapping(&planned->physical_mappings, gid, mapping);
        jz_btree_init(&planned->physical_current_winner_projections, self->catalogue.physical_current_winner_projections.d);
        jz_btree_init(&planned->staged_lineages, self->catalogue.staged_lineages.d);
        jz_btree_init(&planned->pending_lineages, self->catalogue.pending_lineages.d);
        jz_btree_init(&planned->active_lineages_by_target, self->catalogue.active_lineages_by_target.d);
        planned->active_catalogue_seq = 0;
        planned->next_physical_table_id = next_physical_table_id;
        planned->next_physical_column_id = next_physical_column_id;
        jz_btree_init(&planned->lens_path_cache, self->catalogue.lens_path_cache.d);
        jz_btree_init(&planned->compiled_lens_cache, self->catalogue.compiled_lens_cache.d);
        jz_btree_init(&planned->physical_write_plan_cache, self->catalogue.physical_write_plan_cache.d);
        jazz_protocol_CurrentWriteSchema pointer = { 0, gid };
        have_planned = true;
        e = jazz_node__node__ActiveSchema__new(pointer, jazz_model__schema__JazzSchema__clone(bootstrap_genesis->schema), &planned->active_schema);
        if (e.code) goto fail;
    } else {
        jazz_node__node__SchemaCatalogue__Clone__clone(planned, &self->catalogue);
        have_planned = true;
    }
    {
        const jz_uuid *id;
        jazz_protocol_SchemaVersion **schema;
        JZ_BTREE_FOR(&schemas, id, schema) {
            if (jz_btree_contains(&lineage_targets, id) || jz_btree_contains(&planned->catalogue_schemas, id)) continue;
            if (!jz_uuid_eq(id, &genesis_id)) { e = invalid_update("trusted catalogue snapshot schema has no identity publication"); goto fail; }
            jazz_node_SchemaPhysicalMapping mapping;
            e = jazz_node__node__physical__allocate_provisional_physical_mapping((*schema)->schema, jazz_protocol__protocol__PhysicalIdentityManifest__clone(genesis_physical_identities),
                                                                                &planned->next_physical_table_id, &planned->next_physical_column_id, &mapping);
            if (e.code) goto fail;
            jazz_types_SchemaVersionAlias alias;
            e = jazz_node__node__catalogue_ingest__next_schema_version_alias_in_catalogue(planned, &alias);
            if (e.code) { jazz_node__node__physical__SchemaPhysicalMapping__drop(&mapping); goto fail; }
            insert_schema(&planned->catalogue_schemas, *schema);
            insert_mapping(&planned->physical_mappings, *id, mapping);
            jz_btree_insert(&planned->schema_version_aliases, id, &alias, NULL);
        }
    }

    /* A pre-bootstrap local node may already have durable rows under its
     * opening schema.  Install the exact snapshot genesis manifest before
     * validating descendant publications. */
    {
        jazz_node_SchemaPhysicalMapping *gm = jz_btree_get(&planned->physical_mappings, &genesis_id);
        if (gm) {
            jz_btree_drop(&genesis_rebindings);
            e = jazz_node__node__catalogue_ingest__rebind_provisional_mapping_identities(gm, jazz_protocol__protocol__PhysicalIdentityManifest__clone(genesis_physical_identities),
                                                                                        &genesis_rebindings);
            if (e.code) { jz_btree_init(&genesis_rebindings, &desc_uuid_uuid); goto fail; }
        }
        const jz_uuid *schema;
        jazz_node_SchemaPhysicalMapping *mapping;
        JZ_BTREE_FOR(&planned->physical_mappings, schema, mapping) {
            if (jz_uuid_eq(schema, &genesis_id)) continue;
            e = jazz_node__node__catalogue_ingest__rebind_mapping_enum_cases(mapping, &genesis_rebindings);
            if (e.code) goto fail;
        }
    }

    for (u32 li = 0; li < lineages.len; li++) {
        u64 catalogue_seq = lineages.ptr[li].catalogue_seq;
        const jazz_protocol_SchemaLineagePublication *publication = &lineages.ptr[li].publication;
        jz_err ve = jazz_node__node__ingest__NodeState__validate_schema_lineage_publication(publication);
        if (ve.code) { jz_err_drop(&ve); e = invalid_update("trusted catalogue snapshot contains invalid lineage identity"); goto fail; }
        const jazz_node_StagedSchemaLineage *existing = jz_btree_get(&planned->active_lineages_by_target, &publication->schema.id);
        if (existing) {
            if (existing->catalogue_seq != catalogue_seq || !jazz_protocol__protocol__SchemaLineagePublication__eq(&existing->publication, publication)) {
                e = invalid_update("trusted catalogue snapshot lineage conflicts with catalogue");
                goto fail;
            }
            continue;
        }
        u64 expected = planned->active_catalogue_seq == UINT64_MAX ? UINT64_MAX : planned->active_catalogue_seq + 1;
        if (catalogue_seq != expected) { e = invalid_update("trusted catalogue snapshot lineage sequence is not contiguous"); goto fail; }
        jzn_ptrvec manifests = { 0 };
        {
            const jz_uuid *k; const jazz_node_SchemaPhysicalMapping *m;
            JZ_BTREE_FOR(&planned->physical_mappings, k, m) { (void)k; JZ_VEC_PUSH(manifests, (void *)jazz_protocol__protocol__PhysicalIdentityManifest__clone(m->identities)); }
        }
        e = jazz_node__node__NodeState__validate_publication_sources(publication, &planned->catalogue_schemas, &planned->physical_mappings, manifests);
        if (e.code) goto fail;
        jazz_node_SchemaPhysicalMapping fresh, mapping;
        e = jazz_node__node__physical__allocate_provisional_physical_mapping(publication->schema.schema, jazz_protocol__protocol__PhysicalIdentityManifest__clone(publication->physical_identities),
                                                                            &planned->next_physical_table_id, &planned->next_physical_column_id, &fresh);
        if (e.code) goto fail;
        e = jazz_node__node__NodeState__reconcile_publication_mapping(planned, publication, &fresh, &mapping);
        jazz_node__node__physical__SchemaPhysicalMapping__drop(&fresh);
        if (e.code) goto fail;
        jazz_node_StagedSchemaLineage staged;
        staged.catalogue_seq = catalogue_seq;
        jazz_protocol__protocol__SchemaLineagePublication__clone(&staged.publication, publication);
        staged.mapping = mapping;
        e = jazz_node__node__catalogue_ingest__next_schema_version_alias_in_catalogue(planned, &staged.alias);
        if (e.code) { jazz_node__node__StagedSchemaLineage__drop(&staged); goto fail; }
        insert_schema(&planned->catalogue_schemas, &publication->schema);
        for (u32 p = 0; p < publication->predecessors.len; p++) {
            const jazz_protocol_MigrationLens *lens = &publication->predecessors.ptr[p].lens;
            jazz_protocol_MigrationLens *c = JZ_NEW(jazz_protocol_MigrationLens);
            jazz_protocol__protocol__MigrationLens__clone(c, lens);
            jz_btree_insert(&planned->catalogue_lenses, &c->id, &c, NULL);
        }
        jz_btree_insert(&planned->schema_version_aliases, &publication->schema.id, &staged.alias, NULL);
        jazz_node_SchemaPhysicalMapping mc;
        jazz_node__node__physical__SchemaPhysicalMapping__clone(&mc, &staged.mapping);
        insert_mapping(&planned->physical_mappings, publication->schema.id, mc);
        insert_staged(&planned->active_lineages_by_target, publication->schema.id, &staged);
        planned->active_catalogue_seq = catalogue_seq;
        JZ_VEC_PUSH(result.activated_lineages, staged);
    }

    /* Schema identity deliberately excludes policy and physical-index
     * declarations. Apply the sender's final payloads after lineage so
     * agreeing same-id metadata updates use the ordinary trusted path. */
    {
        const jz_uuid *id;
        jazz_protocol_SchemaVersion **schema;
        JZ_BTREE_FOR(&schemas, id, schema) {
            if (!jz_btree_contains(&planned->catalogue_schemas, id)) { e = invalid_update("trusted catalogue snapshot schema has no lineage"); goto fail; }
            insert_schema(&planned->catalogue_schemas, *schema);
        }
    }

    /* Schema aliases and physical ids are node-local. Keep the local schema's
     * storage identity and reconcile the received lineage around it rather
     * than orphaning the pending rows by adopting a newly allocated alias/mapping. */
    if (has_anchor) {
        jazz_types_SchemaVersionId anchor = planned->local_schema_version_id;
        const jazz_node_SchemaPhysicalMapping *am = jz_btree_get(&planned->physical_mappings, &anchor);
        if (!am) { e = JZN_INVALID_STORED("snapshot authority mapping missing for local anchor"); goto fail_anchor; }
        jazz_protocol_PhysicalIdentityManifest *authority_identities = jazz_protocol__protocol__PhysicalIdentityManifest__clone(am->identities);
        jazz_protocol_SchemaVersion **asv = jz_btree_get(&planned->catalogue_schemas, &anchor);
        if (!asv) { jazz_protocol__protocol__PhysicalIdentityManifest__drop(authority_identities); e = JZN_INVALID_STORED("snapshot authority schema missing for local anchor"); goto fail_anchor; }
        jz_str msg = { 0 };
        jz_err vfe = jazz_protocol__protocol__PhysicalIdentityManifest__validate_for_schema(authority_identities, (*asv)->schema, &msg);
        if (vfe.code) {
            jz_err_drop(&vfe);
            jazz_protocol__protocol__PhysicalIdentityManifest__drop(authority_identities);
            e = jz_err_fmt(JZN_E_INVALID_CATALOGUE_UPDATE, "%.*s", (int)msg.len, msg.ptr);
            jz_str_drop(&msg);
            goto fail_anchor;
        }
        /* Preserve only below-semantic-layer aliases. The authority manifest
         * is the global identity source of truth. */
        jz_btree anchor_rebindings;
        e = jazz_node__node__catalogue_ingest__rebind_provisional_mapping_identities(&anchor_mapping, authority_identities, &anchor_rebindings);
        if (e.code) goto fail_anchor;
        {
            const jz_uuid *k; jazz_node_SchemaPhysicalMapping *m;
            JZ_BTREE_FOR(&planned->physical_mappings, k, m) {
                (void)k;
                e = jazz_node__node__catalogue_ingest__rebind_mapping_enum_cases(m, &anchor_rebindings);
                if (e.code) break;
            }
        }
        jz_btree_drop(&anchor_rebindings);
        if (e.code) goto fail_anchor;
        jz_btree_insert(&planned->schema_version_aliases, &anchor, &anchor_alias, NULL);
        insert_mapping(&planned->physical_mappings, anchor, anchor_mapping);
        has_anchor = false;

        jz_btree ancestors;
        jz_btree_init(&ancestors, &jzn_desc_uuid_set);
        jz_btree_insert(&ancestors, &anchor, NULL, NULL);
        staged_vec reverse_lineages = { 0 };
        staged_values(&planned->active_lineages_by_target, &reverse_lineages);
        jz_sort(reverse_lineages.ptr, reverse_lineages.len, sizeof(jazz_node_StagedSchemaLineage), cmp_staged_seq_rev, NULL);
        for (u32 i = 0; i < reverse_lineages.len && !e.code; i++) {
            const jazz_node_StagedSchemaLineage *lineage = &reverse_lineages.ptr[i];
            if (!jz_btree_contains(&ancestors, &lineage->publication.schema.id)) continue;
            for (u32 p = 0; p < lineage->publication.predecessors.len; p++) {
                const jazz_protocol_MigrationLens *lens = &lineage->publication.predecessors.ptr[p].lens;
                jazz_protocol_SchemaVersion **source_schema = jz_btree_get(&planned->catalogue_schemas, &lens->source);
                if (!source_schema) { e = JZN_INVALID_STORED("snapshot source schema missing during local mapping reconciliation"); break; }
                jazz_protocol_SchemaVersion **target_schema = jz_btree_get(&planned->catalogue_schemas, &lens->target);
                if (!target_schema) { e = JZN_INVALID_STORED("snapshot target schema missing during local mapping reconciliation"); break; }
                const jazz_node_SchemaPhysicalMapping *provisional_source = jz_btree_get(&planned->physical_mappings, &lens->source);
                if (!provisional_source) { e = JZN_INVALID_STORED("snapshot source mapping missing during local mapping reconciliation"); break; }
                const jazz_node_SchemaPhysicalMapping *target_mapping = jz_btree_get(&planned->physical_mappings, &lineage->publication.schema.id);
                if (!target_mapping) { e = JZN_INVALID_STORED("snapshot target mapping missing during local mapping reconciliation"); break; }
                jazz_node_SchemaPhysicalMapping source_mapping;
                e = jazz_node__node__NodeState__reconcile_source_physical_mapping_for_lens_payload(lens, *source_schema, *target_schema, provisional_source,
                                                                                                  target_mapping, &source_mapping);
                if (e.code) break;
                insert_mapping(&planned->physical_mappings, lens->source, source_mapping);
                jz_btree_insert(&ancestors, &lens->source, NULL, NULL);
            }
        }
        jz_vec_drop(&reverse_lineages, sizeof(jazz_node_StagedSchemaLineage), jazz_node__node__StagedSchemaLineage__drop);
        jz_btree_drop(&ancestors);
        if (e.code) goto fail;

        staged_vec ordered_lineages = { 0 };
        staged_values(&planned->active_lineages_by_target, &ordered_lineages);
        jz_sort(ordered_lineages.ptr, ordered_lineages.len, sizeof(jazz_node_StagedSchemaLineage), cmp_staged_seq, NULL);
        for (u32 i = 0; i < ordered_lineages.len && !e.code; i++) {
            const jazz_node_StagedSchemaLineage *lineage = &ordered_lineages.ptr[i];
            const jazz_node_SchemaPhysicalMapping *pt = jz_btree_get(&planned->physical_mappings, &lineage->publication.schema.id);
            if (!pt) { e = JZN_INVALID_STORED("snapshot target mapping missing during forward reconciliation"); break; }
            jazz_node_SchemaPhysicalMapping provisional_target, mapping;
            jazz_node__node__physical__SchemaPhysicalMapping__clone(&provisional_target, pt);
            e = jazz_node__node__NodeState__reconcile_publication_mapping(planned, &lineage->publication, &provisional_target, &mapping);
            jazz_node__node__physical__SchemaPhysicalMapping__drop(&provisional_target);
            if (!e.code) insert_mapping(&planned->physical_mappings, lineage->publication.schema.id, mapping);
        }
        jz_vec_drop(&ordered_lineages, sizeof(jazz_node_StagedSchemaLineage), jazz_node__node__StagedSchemaLineage__drop);
        if (e.code) goto fail;
        {
            const jz_uuid *k; jazz_node_StagedSchemaLineage *lineage;
            JZ_BTREE_FOR(&planned->active_lineages_by_target, k, lineage) {
                (void)k;
                lineage->alias = *(const u64 *)jz_btree_get(&planned->schema_version_aliases, &lineage->publication.schema.id);
                jazz_node__node__physical__SchemaPhysicalMapping__drop(&lineage->mapping);
                jazz_node__node__physical__SchemaPhysicalMapping__clone(&lineage->mapping, jz_btree_get(&planned->physical_mappings, &lineage->publication.schema.id));
            }
        }
        for (u32 i = 0; i < result.activated_lineages.len; i++) {
            jazz_node_StagedSchemaLineage *lineage = &result.activated_lineages.ptr[i];
            lineage->alias = *(const u64 *)jz_btree_get(&planned->schema_version_aliases, &lineage->publication.schema.id);
            jazz_node__node__physical__SchemaPhysicalMapping__drop(&lineage->mapping);
            jazz_node__node__physical__SchemaPhysicalMapping__clone(&lineage->mapping, jz_btree_get(&planned->physical_mappings, &lineage->publication.schema.id));
        }
    }

    {
        jazz_protocol_CurrentWriteSchema cws = snapshot.current_write_schema;
        jazz_protocol_CurrentWriteSchema planned_ptr = jazz_node__node__ActiveSchema__wire_pointer(&planned->active_schema);
        if (cws.revision < planned->active_schema.revision
            || (cws.revision == planned->active_schema.revision && (cws.revision != planned_ptr.revision || !jz_uuid_eq(&cws.schema, &planned_ptr.schema)))
            || !jz_btree_contains(&planned->catalogue_schemas, &cws.schema)) {
            e = invalid_update("trusted catalogue snapshot conflicts at write-schema revision");
            goto fail;
        }
        jazz_protocol_SchemaVersion **target = jz_btree_get(&planned->catalogue_schemas, &cws.schema);
        jazz_node_ActiveSchema active;
        e = jazz_node__node__ActiveSchema__new(cws, jazz_model__schema__JazzSchema__clone((*target)->schema), &active);
        if (e.code) goto fail;
        if (active.revision > 0 && active.revision == planned->active_schema.revision && !jazz_node__node__ActiveSchema__same_permissions(&active, &planned->active_schema)) {
            e = invalid_update("conflicting active schema revision");
            goto fail;
        }
        planned->active_schema = active;
    }
    if (planned->active_schema.revision > 0) {
        jazz_protocol_SchemaVersion structural = jazz_protocol__protocol__SchemaVersion__new(jazz_model__schema__JazzSchema__without_permissions(planned->active_schema.compiled));
        insert_schema(&planned->catalogue_schemas, &structural);
        jazz_protocol__protocol__SchemaVersion__drop(&structural);
    }
    /* `schema` is the active read-schema payload supplied when this node
     * was opened. Its policy metadata is intentionally outside the
     * structural schema id and may therefore be refreshed by a snapshot. */
    {
        jazz_protocol_SchemaVersion **local = jz_btree_get(&planned->catalogue_schemas, &planned->local_schema_version_id);
        if (!local) { e = invalid_update("trusted catalogue snapshot omits the active read schema"); goto fail; }
        planned->schema = jazz_model__schema__JazzSchema__clone((*local)->schema);
    }
    if (jz_uuid_eq(&planned->local_schema_version_id, &planned->active_schema.schema))
        planned->schema = jazz_model__schema__JazzSchema__clone(planned->active_schema.compiled);
    {
        const u64 *a = jz_btree_get(&planned->schema_version_aliases, &planned->local_schema_version_id);
        planned->has_local_schema_version_alias = a != NULL;
        planned->local_schema_version_alias = a ? *a : 0;
    }
    /* Validate the complete authority graph, including retained mappings. */
    {
        jz_btree authority_mappings;
        jz_btree_init(&authority_mappings, planned->physical_mappings.d);
        const jz_uuid *schema;
        const jazz_node_SchemaPhysicalMapping *mapping;
        JZ_BTREE_FOR(&planned->physical_mappings, schema, mapping) {
            if (!jz_btree_contains(&schemas, schema)) continue;
            jazz_node_SchemaPhysicalMapping c;
            jazz_node__node__physical__SchemaPhysicalMapping__clone(&c, mapping);
            jz_btree_insert(&authority_mappings, schema, &c, NULL);
        }
        e = jazz_node__node__NodeState__validate_durable_physical_identity_bindings(&schemas, &authority_mappings, &planned->staged_lineages,
                                                                                    &planned->active_lineages_by_target, &planned->pending_lineages, &genesis_id,
                                                                                    JZN_BOOTSTRAP_READY);
        jz_btree_drop(&authority_mappings);
        if (e.code) goto fail;
    }
    e = jazz_node__node__physical__validate_scalar_enum_case_provenance(&planned->physical_mappings, &planned->schema_version_aliases);
    if (e.code) goto fail;
    e = jazz_node__node__physical__validate_payload_enum_case_provenance(&planned->physical_mappings, &planned->schema_version_aliases);
    if (e.code) goto fail;
    jz_btree_drop(&schemas);
    jz_btree_drop(&lineage_targets);
    jz_btree_drop(&genesis_rebindings);
    *out = result;
    return JZ_OK;
fail_anchor:
    if (has_anchor) jazz_node__node__physical__SchemaPhysicalMapping__drop(&anchor_mapping);
fail:
    jz_btree_drop(&schemas);
    jz_btree_drop(&lineage_targets);
    jz_btree_drop(&genesis_rebindings);
    if (have_planned) jazz_node__node__SchemaCatalogue__drop(planned);
    jz_vec_drop(&result.activated_lineages, sizeof(jazz_node_StagedSchemaLineage), jazz_node__node__StagedSchemaLineage__drop);
    return e;
}
