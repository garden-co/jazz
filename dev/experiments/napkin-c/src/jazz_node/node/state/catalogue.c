/* Port of crates/jazz/layers/node/src/node/state/catalogue.rs
 * (include!d into node/mod.rs, so the Rust module is jazz_node::node).
 * Skipped: activate_schema_for_test (#[cfg(any(test, feature = "testing"))]).
 * validate_schema_activation is #[cfg(feature = "runtime")] (server) and is
 * kept under JZ_FEATURE_RUNTIME. */
#include "jz_rt.h"
#include "../../_ext_node-qe-b.h"
#include "../node_qe_b.h"

#define NS jazz_node_NodeState

/* ---- small clone/drop adapters for jz_btree_clone ---- */
static void mapping_vclone(void *dst, const void *src) { jazz_node_SchemaPhysicalMapping_clone(dst, src); }
static void alias_vclone(void *dst, const void *src) { memcpy(dst, src, sizeof(jazz_types_SchemaVersionAlias)); }
static void uuid_kclone(void *dst, const void *src) { memcpy(dst, src, sizeof(jz_uuid)); }
static void u64_kclone(void *dst, const void *src) { memcpy(dst, src, sizeof(u64)); }
static void str_kclone(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone(src); }
static void table_mapping_vclone(void *dst, const void *src) { jazz_node_PhysicalTableMapping_clone(dst, src); }
static void case_vec_vclone(void *dst, const void *src) { *(jazz_node_GlobalScalarEnumCaseIdVec *)dst = jazz_node_GlobalScalarEnumCaseIdVec_clone(src); }
static void nested_cases_vclone(void *dst, const void *src) { jz_btree_clone(dst, src, str_kclone, case_vec_vclone); }

static void put_catalogue(groove_DatabaseBatch *batch, u32 kind, jz_uuid id, jz_bytes payload);
static void delete_catalogue(groove_DatabaseBatch *batch, u32 kind, jz_uuid id);

/* Apply a node batch and wait until it is durably persisted. */
jz_err jazz_node__node__NodeState__apply_and_persist_node_batch(NS *self, groove_DatabaseBatch *batch)
{
    groove_AppliedBatch *applied;
    JZ_TRY(jazz_node__node__NodeState__apply_node_batch(self, batch, &applied));
    groove_PersistedBatch *persisted = groove__db__commit__Database__persist_with_progress(self->database, applied);
    groove__db__commit__AppliedBatch__drop(applied);
    jz_err e = groove__db__commit__Database__finish_persistence(self->database, persisted);
    return e.code ? jazz_node__node__Error_groove(e) : JZ_OK;
}

/* ---- parents / is_descendant (shared shape with lifecycle's history_for) ---- */
typedef struct ParentsCtx { jz_btree parents; /* SchemaVersionId -> jz_uuid_vec */ jazz_types_SchemaVersionId candidate; } ParentsCtx;
static void uuid_vec_drop_v(void *p) { jz_free(((jz_uuid_vec *)p)->ptr); }
static const jz_btree_desc PARENTS_DESC = { sizeof(jz_uuid), sizeof(jz_uuid_vec), jz_cmp_uuid, NULL, uuid_vec_drop_v };

static void parents_insert_publication(jz_btree *parents, const jazz_protocol_SchemaLineagePublication *publication)
{
    jz_uuid_vec sources = {0};
    JZ_VEC_FOR(const jazz_protocol_SchemaPredecessor, p, publication->predecessors) JZ_VEC_PUSH(sources, p->lens.source);
    jz_uuid_vec old;
    if (jz_btree_insert(parents, &publication->schema.id, &sources, &old)) jz_free(old.ptr);
}

bool jazz_node__node__lineage_is_descendant(const jz_btree *parents, jazz_types_SchemaVersionId candidate, jazz_types_SchemaVersionId schema)
{
    jz_uuid_vec remaining = {0};
    JZ_VEC_PUSH(remaining, schema);
    jz_btree visited; jz_btree_init(&visited, &jazz_node_UUID_SET_DESC);
    bool found = false;
    while (remaining.len && !found) {
        jz_uuid cursor = remaining.ptr[--remaining.len];
        bool inserted;
        jz_btree_entry(&visited, &cursor, &inserted);
        if (!inserted) continue;
        const jz_uuid_vec *sources = jz_btree_get(parents, &cursor);
        if (!sources) continue;
        JZ_VEC_FOR(const jz_uuid, s, *sources) if (jz_uuid_eq(s, &candidate)) { found = true; break; }
        if (!found) JZ_VEC_FOR(const jz_uuid, s, *sources) JZ_VEC_PUSH(remaining, *s);
    }
    jz_free(remaining.ptr);
    jz_btree_drop(&visited);
    return found;
}

/* Reservation history for admitting `candidate_schema`. Descendant parked
 * envelopes are excluded: they are allowed to inherit identities minted by
 * this candidate, even when they arrived out of order. Unrelated
 * parked/staged envelopes remain reservations and therefore cannot claim one
 * of its new UUIDs first. */
jazz_protocol_PhysicalIdentityManifestVec jazz_node__node__NodeState__physical_identity_history_for_candidate(const NS *self,
    jazz_types_SchemaVersionId candidate_schema, bool has_omit, jazz_types_SchemaLineagePublicationId omit)
{
    jz_btree parents; jz_btree_init(&parents, &PARENTS_DESC);
    const void *k; const jazz_node_StagedSchemaLineage *lineage; const jazz_node_PendingSchemaLineage *pending;
    JZ_BTREE_FOR(&self->catalogue.active_lineages_by_target, k, lineage) parents_insert_publication(&parents, &lineage->publication);
    JZ_BTREE_FOR(&self->catalogue.staged_lineages, k, lineage) parents_insert_publication(&parents, &lineage->publication);
    JZ_BTREE_FOR(&self->catalogue.pending_lineages, k, pending) parents_insert_publication(&parents, &pending->publication);

    jazz_protocol_PhysicalIdentityManifestVec out = {0};
    const jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(&self->catalogue.physical_mappings, k, mapping) {
        jazz_protocol_PhysicalIdentityManifest m; jazz_protocol_PhysicalIdentityManifest_clone(&m, &mapping->identities);
        JZ_VEC_PUSH(out, m);
    }
    JZ_BTREE_FOR(&self->catalogue.staged_lineages, k, lineage) {
        if ((has_omit && jz_uuid_eq(&lineage->publication.id, &omit))
            || jazz_node__node__lineage_is_descendant(&parents, candidate_schema, lineage->publication.schema.id)) continue;
        jazz_protocol_PhysicalIdentityManifest m; jazz_protocol_PhysicalIdentityManifest_clone(&m, &lineage->publication.physical_identities);
        JZ_VEC_PUSH(out, m);
    }
    JZ_BTREE_FOR(&self->catalogue.pending_lineages, k, pending) {
        if ((has_omit && jz_uuid_eq(&pending->publication.id, &omit))
            || jazz_node__node__lineage_is_descendant(&parents, candidate_schema, pending->publication.schema.id)) continue;
        jazz_protocol_PhysicalIdentityManifest m; jazz_protocol_PhysicalIdentityManifest_clone(&m, &pending->publication.physical_identities);
        JZ_VEC_PUSH(out, m);
    }
    jz_btree_drop(&parents);
    return out;
}

/* Single-predecessor convenience entry point. */
jz_err jazz_node__node__NodeState__author_schema_lineage_publication(const NS *self, jazz_protocol_SchemaVersion schema, jazz_protocol_MigrationLens lens,
    jz_str_vec new_tables, jz_str_vec dropped_tables, jazz_protocol_SchemaLineagePublication *out)
{
    jazz_protocol_SchemaPredecessorVec predecessors = {0};
    JZ_VEC_PUSH(predecessors, ((jazz_protocol_SchemaPredecessor){ lens, new_tables, dropped_tables }));
    return jazz_node__node__NodeState__author_schema_from_predecessors(self, schema, predecessors, out);
}

/* Author one immutable schema record containing every incoming migration. */
jz_err jazz_node__node__NodeState__author_schema_from_predecessors(const NS *self, jazz_protocol_SchemaVersion schema,
    jazz_protocol_SchemaPredecessorVec predecessors, jazz_protocol_SchemaLineagePublication *out)
{
    jz_btree sources;
    JZ_TRY(jazz_node__node__NodeState__predecessor_sources(&predecessors, &self->catalogue.catalogue_schemas, &self->catalogue.physical_mappings, &sources));
    jazz_protocol_PhysicalIdentityManifestVec history = jazz_node__node__NodeState__physical_identity_history_for_candidate(self, schema.id, false, (jz_uuid){{0}});
    const char *msg = jazz_protocol__protocol__SchemaLineagePublication__author_from_predecessors(schema, predecessors, &sources, history, out);
    jz_btree_drop(&sources);
    return msg ? JZN_INVALID_CATALOGUE(msg) : JZ_OK;
}

jz_err jazz_node__node__NodeState__predecessor_sources(const jazz_protocol_SchemaPredecessorVec *predecessors, const jz_btree *schemas, const jz_btree *mappings, jz_btree *out)
{
    jz_btree_init(out, &jazz_node_PREDECESSOR_SOURCES_DESC);
    JZ_VEC_FOR(const jazz_protocol_SchemaPredecessor, predecessor, *predecessors) {
        const jazz_protocol_SchemaVersion *source = jz_btree_get(schemas, &predecessor->lens.source);
        if (!source) { jz_btree_drop(out); return JZN_INVALID_CATALOGUE("schema predecessor is missing"); }
        const jazz_node_SchemaPhysicalMapping *mapping = jz_btree_get(mappings, &source->id);
        if (!mapping) { jz_btree_drop(out); return JZN_INVALID_CATALOGUE("schema predecessor identities are missing"); }
        jazz_node_PredecessorSource value;
        jazz_model_JazzSchema_clone(&value.schema, &source->schema);
        jazz_protocol_PhysicalIdentityManifest_clone(&value.identities, &mapping->identities);
        jazz_node_PredecessorSource old;
        if (jz_btree_insert(out, &source->id, &value, &old)) jazz_node_PredecessorSource_drop(&old);
    }
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__validate_publication_sources(const jazz_protocol_SchemaLineagePublication *publication, const jz_btree *schemas,
    const jz_btree *mappings, jazz_protocol_PhysicalIdentityManifestVec history)
{
    jz_btree sources;
    jz_err e = jazz_node__node__NodeState__predecessor_sources(&publication->predecessors, schemas, mappings, &sources);
    if (e.code) { jazz_protocol_PhysicalIdentityManifestVec_drop(&history); return e; }
    JZ_VEC_FOR(const jazz_protocol_SchemaPredecessor, predecessor, publication->predecessors) {
        const jazz_protocol_SchemaVersion *source = jz_btree_get(schemas, &predecessor->lens.source);
        JZ_ASSERT(source, "schemas[&predecessor.lens.source]");
        e = jazz_node__node__ingest__NodeState__validate_migration_lens_between(&predecessor->lens, source, &publication->schema);
        if (!e.code) e = jazz_node__node__ingest__NodeState__validate_lineage_table_partition(&source->schema, &publication->schema.schema,
            &predecessor->lens, &predecessor->new_tables, &predecessor->dropped_tables);
        if (e.code) { jz_btree_drop(&sources); jazz_protocol_PhysicalIdentityManifestVec_drop(&history); return e; }
    }
    const char *msg = jazz_protocol__protocol__SchemaLineagePublication__validate_predecessor_identities(publication, &sources, history);
    jz_btree_drop(&sources);
    return msg ? JZN_INVALID_CATALOGUE(msg) : JZ_OK;
}

static int predecessor_source_cmp(const void *a, const void *b, void *ctx)
{
    (void)ctx;
    const jazz_protocol_SchemaPredecessor *const *x = a, *const *y = b;
    return jz_uuid_cmp(&(*x)->lens.source, &(*y)->lens.source);
}

jz_err jazz_node__node__NodeState__reconcile_publication_mapping(const jazz_node_SchemaCatalogue *catalogue, const jazz_protocol_SchemaLineagePublication *publication,
    const jazz_node_SchemaPhysicalMapping *provisional, jazz_node_SchemaPhysicalMapping *out)
{
    jazz_node_SchemaPhysicalMapping mapping; jazz_node_SchemaPhysicalMapping_clone(&mapping, provisional);
    JZ_VEC(const jazz_protocol_SchemaPredecessor *) predecessors = {0};
    JZ_VEC_FOR(const jazz_protocol_SchemaPredecessor, p, publication->predecessors) JZ_VEC_PUSH(predecessors, p);
    jz_sort(predecessors.ptr, predecessors.len, sizeof(predecessors.ptr[0]), predecessor_source_cmp, NULL);
    for (u32 i = 0; i < predecessors.len; ++i) {
        jazz_node_SchemaPhysicalMapping next;
        jz_err e = jazz_node__node__NodeState__reconcile_physical_mapping_for_lens_payload_in_catalogue(catalogue, &predecessors.ptr[i]->lens,
            &publication->schema, &mapping, &next);
        jazz_node_SchemaPhysicalMapping_drop(&mapping);
        if (e.code) { jz_free(predecessors.ptr); return e; }
        mapping = next;
    }
    jz_free(predecessors.ptr);
    *out = mapping;
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__persist_catalogue_schema(NS *self, const jazz_protocol_SchemaVersion *schema)
{
    jz_bytes payload;
    JZ_TRY(jazz_node__node__codec__encode_catalogue_schema(schema, &payload));
    groove_DatabaseBatch *batch = groove__db__facade__Database__open_batch(self->database);
    put_catalogue(batch, JZN_CRK_SCHEMA, schema->id, payload);
    return jazz_node__node__NodeState__apply_and_persist_node_batch(self, batch);
}

/* identities["root"] for one column, panicking like `[..]` indexing */
static const jz_uuid_vec *root_variant_ids(const jazz_node_PhysicalTableIdentity *table_identities, const jz_str *column)
{
    const jazz_protocol_PhysicalColumnIdentity *column_identity = jz_btree_get(&table_identities->columns, column);
    JZ_ASSERT(column_identity, "table_identities.columns[&column.name]");
    jz_str root = jz_str_from("root");
    const jz_uuid_vec *ids = jz_btree_get(&column_identity->enum_variants, &root);
    jz_str_drop(&root);
    JZ_ASSERT(ids, "enum_variants[\"root\"]");
    return ids;
}
static const jz_btree *column_enum_variants(const jazz_node_PhysicalTableIdentity *table_identities, const jz_str *column)
{
    const jazz_protocol_PhysicalColumnIdentity *column_identity = jz_btree_get(&table_identities->columns, column);
    JZ_ASSERT(column_identity, "table_identities.columns[&column.name]");
    return &column_identity->enum_variants;
}

jz_err jazz_node__node__NodeState__hydrate_enum_case_mapping(NS *self, jazz_types_SchemaVersionId schema_version)
{
    const jazz_protocol_SchemaVersion *schema_entry = jz_btree_get(&self->catalogue.catalogue_schemas, &schema_version);
    if (!schema_entry) return JZN_INVALID_STORED("physical mapping schema payload missing");
    jazz_model_JazzSchema schema; jazz_model_JazzSchema_clone(&schema, &schema_entry->schema);
    jazz_node_SchemaPhysicalMapping *mapping = jz_btree_get(&self->catalogue.physical_mappings, &schema_version);
    jz_err e = JZ_OK;
    if (!mapping) { e = JZN_INVALID_STORED("physical mapping missing"); goto out; }
    JZ_VEC_FOR(const jazz_model_TableSchema, table, schema.tables) {
        const jazz_node_PhysicalTableIdentity *identities_ref = jz_btree_get(&mapping->identities.tables, &table->name);
        if (!identities_ref) { e = JZN_INVALID_STORED("physical table identities missing"); goto out; }
        jazz_node_PhysicalTableIdentity table_identities; jazz_node_PhysicalTableIdentity_clone(&table_identities, identities_ref);
        jazz_node_PhysicalTableMapping *physical = jz_btree_get(&mapping->tables, &table->name);
        if (!physical) { jazz_node_PhysicalTableIdentity_drop(&table_identities); continue; }
        JZ_VEC_FOR(const jazz_model_ColumnSchema, column, table->columns) {
            u32 variant_count;
            if (!groove__records__ValueType__as_enum_tag(&column->column_type, &variant_count)) continue;
            const jazz_types_PhysicalColumnId *id = jz_btree_get(&physical->columns, &column->name);
            if (!id) continue;
            const jz_uuid_vec *ids = root_variant_ids(&table_identities, &column->name);
            if (ids->len != variant_count) { e = JZN_INVALID_STORED("scalar enum authority identity width mismatch"); break; }
            bool inserted;
            jazz_node_GlobalScalarEnumCaseIdVec *slot = jz_btree_entry(&physical->scalar_enum_cases, id, &inserted);
            if ((e = jazz_node__node__install_enum_case_ids(slot, ids, schema_version)).code) break;
        }
        /* Each column writes only its own registry entries, so payload and
         * non-payload columns hydrate in one pass. */
        if (!e.code) JZ_VEC_FOR(const jazz_model_ColumnSchema, column, table->columns) {
            const jazz_types_PhysicalColumnId *id = jz_btree_get(&physical->columns, &column->name);
            if (!id) continue;
            const jz_btree *identities = column_enum_variants(&table_identities, &column->name);
            u32 case_count;
            if (groove__records__ValueType__as_enum(&column->column_type, &case_count)) {
                const jz_uuid_vec *ids = root_variant_ids(&table_identities, &column->name);
                if (ids->len != case_count) { e = JZN_INVALID_STORED("payload enum authority identity width mismatch"); break; }
                bool inserted;
                jazz_node_GlobalEnumCaseIdVec *slot = jz_btree_entry(&physical->payload_enum_cases, id, &inserted);
                if ((e = jazz_node__node__install_payload_enum_case_ids(slot, ids, schema_version)).code) break;
            }
            bool inserted;
            jz_btree *nested = jz_btree_entry(&physical->nested_scalar_enum_cases, id, &inserted);
            if (inserted) jz_btree_init(nested, &jazz_node_NESTED_CASES_BY_PATH_DESC);
            if ((e = jazz_node__node__hydrate_nested_scalar_enum_cases(&column->column_type, identities, schema_version, "root", "root", nested)).code) break;
            jz_btree *nested_payload = jz_btree_entry(&physical->nested_payload_enum_cases, id, &inserted);
            if (inserted) jz_btree_init(nested_payload, &jazz_node_NESTED_CASES_BY_PATH_DESC);
            if ((e = jazz_node__node__hydrate_nested_payload_enum_cases(&column->column_type, identities, schema_version, "root", "root", nested_payload)).code) break;
        }
        jazz_node_PhysicalTableIdentity_drop(&table_identities);
        if (e.code) goto out;
    }
out:
    jazz_model_JazzSchema_drop(&schema);
    return e;
}

jz_err jazz_node__node__NodeState__ensure_provisional_physical_mapping(NS *self, jazz_types_SchemaVersionId schema_version)
{
    if (jz_btree_contains(&self->catalogue.physical_mappings, &schema_version)
        && jz_btree_contains(&self->catalogue.schema_version_aliases, &schema_version))
        return jazz_node__node__NodeState__hydrate_enum_case_mapping(self, schema_version);
    jazz_node_SchemaPhysicalMapping mapping;
    const jazz_node_SchemaPhysicalMapping *existing = jz_btree_get(&self->catalogue.physical_mappings, &schema_version);
    if (existing) {
        jazz_node_SchemaPhysicalMapping_clone(&mapping, existing);
    } else {
        const jazz_protocol_SchemaVersion *entry = jz_btree_get(&self->catalogue.catalogue_schemas, &schema_version);
        if (!entry) return JZN_INVALID_STORED("physical mapping schema payload missing");
        jazz_model_JazzSchema schema; jazz_model_JazzSchema_clone(&schema, &entry->schema);
        jz_btree tables; jz_btree_init(&tables, &jazz_node_PHYSICAL_TABLE_MAPPINGS_DESC);
        JZ_VEC_FOR(const jazz_model_TableSchema, table, schema.tables) {
            jazz_types_PhysicalTableId table_id;
            jz_err e = jazz_node__node__NodeState__allocate_physical_table_id(self, &table_id);
            if (e.code) { jz_btree_drop(&tables); jazz_model_JazzSchema_drop(&schema); return e; }
            jazz_node_PhysicalTableMapping physical = { .table_id = table_id };
            jz_btree_init(&physical.columns, &jazz_node_COLUMN_IDS_DESC);
            jz_btree_init(&physical.scalar_enum_cases, &jazz_node_COLUMN_CASES_DESC);
            jz_btree_init(&physical.payload_enum_cases, &jazz_node_COLUMN_CASES_DESC);
            jz_btree_init(&physical.nested_scalar_enum_cases, &jazz_node_NESTED_COLUMN_CASES_DESC);
            jz_btree_init(&physical.nested_payload_enum_cases, &jazz_node_NESTED_COLUMN_CASES_DESC);
            JZ_VEC_FOR(const jazz_model_ColumnSchema, column, table->columns) {
                jazz_types_PhysicalColumnId column_id;
                e = jazz_node__node__NodeState__allocate_physical_column_id(self, &column_id);
                if (e.code) { jazz_node_PhysicalTableMapping_drop(&physical); jz_btree_drop(&tables); jazz_model_JazzSchema_drop(&schema); return e; }
                jz_str name = jz_str_clone(&column->name);
                jazz_types_PhysicalColumnId old;
                if (jz_btree_insert(&physical.columns, &name, &column_id, &old)) jz_str_drop(&name);
            }
            jz_str name = jz_str_clone(&table->name);
            jazz_node_PhysicalTableMapping old;
            if (jz_btree_insert(&tables, &name, &physical, &old)) { jz_str_drop(&name); jazz_node_PhysicalTableMapping_drop(&old); }
        }
        mapping.identities = jazz_protocol__protocol__PhysicalIdentityManifest__allocate(&schema);
        mapping.tables = tables;
        jazz_model_JazzSchema_drop(&schema);
    }
    jazz_types_SchemaVersionAlias alias;
    const jazz_types_SchemaVersionAlias *existing_alias = jz_btree_get(&self->catalogue.schema_version_aliases, &schema_version);
    if (existing_alias) {
        alias = *existing_alias;
    } else {
        u64 max = 0;
        const void *k; const jazz_types_SchemaVersionAlias *a;
        JZ_BTREE_FOR(&self->catalogue.schema_version_aliases, k, a) if (*a > max) max = *a;
        if (max == UINT64_MAX) { jazz_node_SchemaPhysicalMapping_drop(&mapping); return JZN_INVALID_STORED("schema version alias exhausted"); }
        alias = max + 1;
    }
    const jazz_protocol_SchemaVersion *entry = jz_btree_get(&self->catalogue.catalogue_schemas, &schema_version);
    if (!entry) { jazz_node_SchemaPhysicalMapping_drop(&mapping); return JZN_INVALID_STORED("physical mapping schema payload missing"); }
    jz_btree candidate_mappings; jz_btree_clone(&candidate_mappings, &self->catalogue.physical_mappings, uuid_kclone, mapping_vclone);
    jazz_node_SchemaPhysicalMapping replaced;
    if (jz_btree_insert(&candidate_mappings, &schema_version, &mapping, &replaced)) jazz_node_SchemaPhysicalMapping_drop(&replaced);
    jz_btree candidate_aliases; jz_btree_clone(&candidate_aliases, &self->catalogue.schema_version_aliases, uuid_kclone, alias_vclone);
    jz_btree_insert(&candidate_aliases, &schema_version, &alias, NULL);
    jz_err e = JZ_OK;
    JZ_VEC_FOR(const jazz_model_TableSchema, table, entry->schema.tables) {
        jz_btree fields; jz_btree_init(&fields, &jazz_node_STRING_SET_DESC);
        JZ_VEC_FOR(const jazz_model_ColumnSchema, column, table->columns) {
            jz_str name = jz_str_clone(&column->name);
            bool inserted; jz_btree_entry(&fields, &name, &inserted);
            if (!inserted) jz_str_drop(&name);
        }
        jazz_node_PhysicalVariantCaseVec cases;
        e = jazz_node__node__physical__allocate_physical_variant_cases(&candidate_mappings, &candidate_aliases, schema_version, &table->name, fields, &cases);
        if (e.code) break;
        jazz_node_PhysicalVariantCaseVec_drop(&cases);
    }
    jz_btree_drop(&candidate_aliases);
    if (e.code) { jz_btree_drop(&candidate_mappings); return e; }
    if (!jz_btree_remove(&candidate_mappings, &schema_version, NULL, &mapping)) {
        jz_btree_drop(&candidate_mappings);
        return JZN_INVALID_STORED("allocated physical mapping disappeared");
    }
    jz_btree_drop(&candidate_mappings);
    groove_DatabaseBatch *batch = groove__db__facade__Database__open_batch(self->database);
    e = jazz_node__node__NodeState__write_schema_version_mapping_to_batch(batch, alias, schema_version, &mapping);
    if (e.code) { groove__db__batch__DatabaseBatch__drop(batch); jazz_node_SchemaPhysicalMapping_drop(&mapping); return e; }
    e = jazz_node__node__NodeState__apply_and_persist_node_batch(self, batch);
    if (e.code) { jazz_node_SchemaPhysicalMapping_drop(&mapping); return e; }
    jz_btree_insert(&self->catalogue.schema_version_aliases, &schema_version, &alias, NULL);
    if (jz_uuid_eq(&schema_version, &self->catalogue.local_schema_version_id)) {
        self->catalogue.has_local_schema_version_alias = true;
        self->catalogue.local_schema_version_alias = alias;
    }
    if (jz_btree_insert(&self->catalogue.physical_mappings, &schema_version, &mapping, &replaced)) jazz_node_SchemaPhysicalMapping_drop(&replaced);
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__next_schema_version_alias(const NS *self, jazz_types_SchemaVersionAlias *out)
{
    u64 max = 0;
    const void *k; const jazz_types_SchemaVersionAlias *alias;
    JZ_BTREE_FOR(&self->catalogue.schema_version_aliases, k, alias) if (*alias > max) max = *alias;
    const jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(&self->catalogue.physical_mappings, k, mapping) {
        const jz_str *name; const jazz_node_PhysicalTableMapping *table;
        JZ_BTREE_FOR(&mapping->tables, name, table)
            JZ_VEC_FOR(const jazz_node_PhysicalVariantCase, c, table->variant_cases) if ((u64)c->tag > max) max = c->tag;
    }
    if (max == UINT64_MAX) return JZN_INVALID_STORED("schema version alias exhausted");
    *out = max + 1;
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__allocate_physical_table_id(NS *self, jazz_types_PhysicalTableId *out)
{
    jazz_types_PhysicalTableId id = self->catalogue.next_physical_table_id;
    if (self->catalogue.next_physical_table_id == UINT64_MAX) return JZN_INVALID_STORED("physical table id exhausted");
    self->catalogue.next_physical_table_id += 1;
    *out = id;
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__allocate_physical_column_id(NS *self, jazz_types_PhysicalColumnId *out)
{
    jazz_types_PhysicalColumnId id = self->catalogue.next_physical_column_id;
    if (self->catalogue.next_physical_column_id == UINT64_MAX) return JZN_INVALID_STORED("physical column id exhausted");
    self->catalogue.next_physical_column_id += 1;
    *out = id;
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__validate_pending_schema_lineage_physical_mapping(const NS *self, const jazz_node_PendingSchemaLineage *pending)
{
    JZ_VEC_FOR(const jazz_protocol_SchemaPredecessor, p, pending->publication.predecessors)
        if (!jz_btree_contains(&self->catalogue.catalogue_schemas, &p->lens.source)) return JZ_OK;
    /* Admission must not reserve durable catalogue state or consume local
     * physical ids for a lineage whose shared scalar registry cannot be
     * encoded. Allocate against copies; activation performs the identical
     * reconciliation after the pending envelope has passed this boundary. */
    u64 next_table = self->catalogue.next_physical_table_id;
    u64 next_column = self->catalogue.next_physical_column_id;
    jazz_protocol_PhysicalIdentityManifest identities;
    jazz_protocol_PhysicalIdentityManifest_clone(&identities, &pending->publication.physical_identities);
    jazz_node_SchemaPhysicalMapping provisional;
    JZ_TRY(jazz_node__node__physical__allocate_provisional_physical_mapping(&pending->publication.schema.schema, identities, &next_table, &next_column, &provisional));
    jazz_node_SchemaPhysicalMapping mapping;
    jz_err e = jazz_node__node__NodeState__reconcile_publication_mapping(&self->catalogue, &pending->publication, &provisional, &mapping);
    jazz_node_SchemaPhysicalMapping_drop(&provisional);
    if (e.code) return e;
    jz_btree candidate_mappings; jz_btree_clone(&candidate_mappings, &self->catalogue.physical_mappings, uuid_kclone, mapping_vclone);
    jazz_node_SchemaPhysicalMapping replaced;
    if (jz_btree_insert(&candidate_mappings, &pending->publication.schema.id, &mapping, &replaced)) jazz_node_SchemaPhysicalMapping_drop(&replaced);
    jz_btree candidate_aliases; jz_btree_clone(&candidate_aliases, &self->catalogue.schema_version_aliases, uuid_kclone, alias_vclone);
    jazz_types_SchemaVersionAlias next_alias;
    e = jazz_node__node__NodeState__next_schema_version_alias(self, &next_alias);
    if (!e.code) {
        jz_btree_insert(&candidate_aliases, &pending->publication.schema.id, &next_alias, NULL);
        e = jazz_node__node__physical__validate_scalar_enum_case_provenance(&candidate_mappings, &candidate_aliases);
        if (!e.code) e = jazz_node__node__physical__validate_payload_enum_case_provenance(&candidate_mappings, &candidate_aliases);
    }
    jz_btree_drop(&candidate_aliases);
    jz_btree_drop(&candidate_mappings);
    return e;
}

#define SCALAR_CASE_CAPACITY ((u32)UINT8_MAX + 1)

static void cases_extend(jz_btree *cases, const jazz_node_GlobalScalarEnumCaseIdVec *column_cases)
{
    JZ_VEC_FOR(const jazz_node_GlobalScalarEnumCaseId, c, *column_cases) {
        bool inserted; jz_btree_entry(cases, c, &inserted);
    }
}

jz_err jazz_node__node__NodeState__validate_candidate_scalar_enum_capacity(const jazz_node_SchemaCatalogue *catalogue, const jazz_node_SchemaPhysicalMapping *candidate)
{
    const jz_str *tname; const jazz_node_PhysicalTableMapping *candidate_table;
    JZ_BTREE_FOR(&candidate->tables, tname, candidate_table) {
        const jazz_types_PhysicalColumnId *column_id; const void *unused;
        JZ_BTREE_FOR(&candidate_table->scalar_enum_cases, column_id, unused) {
            jz_btree cases; jz_btree_init(&cases, &jazz_node_SCALAR_CASE_SET_DESC);
            /* catalogue.physical_mappings.values().chain(once(candidate)) */
            const void *k; const jazz_node_SchemaPhysicalMapping *mapping = NULL;
            jz_btree_iter it; jz_btree_iter_begin(&it, &catalogue->physical_mappings);
            bool chained = false;
            for (;;) {
                if (!chained) {
                    if (!jz_btree_next(&it, (void **)&k, (void **)&mapping)) { chained = true; mapping = candidate; }
                } else break;
                const jz_str *n2; const jazz_node_PhysicalTableMapping *table;
                JZ_BTREE_FOR(&mapping->tables, n2, table) {
                    if (table->table_id != candidate_table->table_id) continue;
                    const jazz_node_GlobalScalarEnumCaseIdVec *column_cases = jz_btree_get(&table->scalar_enum_cases, column_id);
                    if (column_cases) cases_extend(&cases, column_cases);
                }
                if (mapping == candidate && chained) break;
            }
            u32 n = cases.len;
            jz_btree_drop(&cases);
            if (n > SCALAR_CASE_CAPACITY) return JZN_INVALID_CATALOGUE("physical scalar enum registry exceeds u8 capacity");
        }
        const jz_btree *candidate_paths;
        JZ_BTREE_FOR(&candidate_table->nested_scalar_enum_cases, column_id, candidate_paths) {
            const jz_str *path; const void *unused2;
            JZ_BTREE_FOR(candidate_paths, path, unused2) {
                jz_btree cases; jz_btree_init(&cases, &jazz_node_SCALAR_CASE_SET_DESC);
                const void *k; const jazz_node_SchemaPhysicalMapping *mapping = NULL;
                jz_btree_iter it; jz_btree_iter_begin(&it, &catalogue->physical_mappings);
                bool chained = false;
                for (;;) {
                    if (!chained) {
                        if (!jz_btree_next(&it, (void **)&k, (void **)&mapping)) { chained = true; mapping = candidate; }
                    } else break;
                    const jz_str *n2; const jazz_node_PhysicalTableMapping *table;
                    JZ_BTREE_FOR(&mapping->tables, n2, table) {
                        if (table->table_id != candidate_table->table_id) continue;
                        const jz_btree *paths = jz_btree_get(&table->nested_scalar_enum_cases, column_id);
                        const jazz_node_GlobalScalarEnumCaseIdVec *column_cases = paths ? jz_btree_get(paths, path) : NULL;
                        if (column_cases) cases_extend(&cases, column_cases);
                    }
                    if (mapping == candidate && chained) break;
                }
                u32 n = cases.len;
                jz_btree_drop(&cases);
                if (n > SCALAR_CASE_CAPACITY) return JZN_INVALID_CATALOGUE("physical scalar enum registry exceeds u8 capacity");
            }
        }
    }
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__reconcile_physical_mapping_for_lens(NS *self, const jazz_protocol_MigrationLens *lens, jazz_node_SchemaPhysicalMapping *out)
{
    const jazz_node_SchemaPhysicalMapping *target_ref = jz_btree_get(&self->catalogue.physical_mappings, &lens->target);
    if (!target_ref) return JZN_INVALID_STORED("target physical mapping missing");
    const jazz_protocol_SchemaVersion *schema_ref = jz_btree_get(&self->catalogue.catalogue_schemas, &lens->target);
    if (!schema_ref) return JZN_INVALID_STORED("target physical mapping schema missing");
    jazz_node_SchemaPhysicalMapping target_mapping; jazz_node_SchemaPhysicalMapping_clone(&target_mapping, target_ref);
    jazz_protocol_SchemaVersion target_schema; jazz_protocol_SchemaVersion_clone(&target_schema, schema_ref);
    jz_err e = jazz_node__node__NodeState__reconcile_physical_mapping_for_lens_payload(self, lens, &target_schema, &target_mapping, out);
    jazz_node_SchemaPhysicalMapping_drop(&target_mapping);
    jazz_protocol_SchemaVersion_drop(&target_schema);
    return e;
}

jz_err jazz_node__node__NodeState__reconcile_physical_mapping_for_lens_payload(const NS *self, const jazz_protocol_MigrationLens *lens,
    const jazz_protocol_SchemaVersion *target_schema_version, const jazz_node_SchemaPhysicalMapping *provisional_target_mapping, jazz_node_SchemaPhysicalMapping *out)
{
    return jazz_node__node__NodeState__reconcile_physical_mapping_for_lens_payload_in_catalogue(&self->catalogue, lens, target_schema_version, provisional_target_mapping, out);
}

static const jazz_model_TableSchema *find_table(const jazz_model_JazzSchema *schema, const jz_str *name)
{
    JZ_VEC_FOR(const jazz_model_TableSchema, t, schema->tables) if (jz_str_eq(&t->name, name)) return t;
    return NULL;
}

/* column name -> (inherited id, Option<source name>) */
typedef struct InheritedColumn { jazz_types_PhysicalColumnId id; bool has_source; jz_str source_name; } InheritedColumn;
static void inherited_column_drop(void *p) { InheritedColumn *c = p; if (c->has_source) jz_str_drop(&c->source_name); }
static void str_drop_v(void *p) { jz_str_drop(p); }
static const jz_btree_desc INHERITED_COLUMNS_DESC = { sizeof(jz_str), sizeof(InheritedColumn), jz_cmp_str, str_drop_v, inherited_column_drop };

typedef struct RetainNames { const jz_btree *names; } RetainNames;
static bool keep_if_name_in(const void *k, void *v, void *ctx) { (void)v; return jz_btree_contains(((RetainNames *)ctx)->names, k); }
static bool keep_if_column_live(const void *k, void *v, void *ctx) { (void)v; return jz_btree_contains(ctx, k); }

static void extend_case_map(jz_btree *dst, const jz_btree *src, void (*vclone)(void *, const void *))
{
    /* BTreeMap::extend: later entries replace existing keys */
    jz_btree copy; jz_btree_clone(&copy, src, u64_kclone, vclone);
    jz_btree_append(dst, &copy);
    jz_btree_drop(&copy);
}

jz_err jazz_node__node__NodeState__reconcile_physical_mapping_for_lens_payload_in_catalogue(const jazz_node_SchemaCatalogue *catalogue,
    const jazz_protocol_MigrationLens *lens, const jazz_protocol_SchemaVersion *target_schema_version,
    const jazz_node_SchemaPhysicalMapping *provisional_target_mapping, jazz_node_SchemaPhysicalMapping *out)
{
    const jazz_node_SchemaPhysicalMapping *source_ref = jz_btree_get(&catalogue->physical_mappings, &lens->source);
    if (!source_ref) return JZN_INVALID_STORED("source physical mapping missing");
    jazz_node_SchemaPhysicalMapping source_mapping; jazz_node_SchemaPhysicalMapping_clone(&source_mapping, source_ref);
    jazz_node_SchemaPhysicalMapping target_mapping; jazz_node_SchemaPhysicalMapping_clone(&target_mapping, provisional_target_mapping);
    const jazz_protocol_SchemaVersion *source_schema_ref = jz_btree_get(&catalogue->catalogue_schemas, &lens->source);
    jz_err e = JZ_OK;
    if (!source_schema_ref) { e = JZN_INVALID_STORED("source physical mapping schema missing"); goto fail_early; }
    jazz_model_JazzSchema source_schema; jazz_model_JazzSchema_clone(&source_schema, &source_schema_ref->schema);
    const jazz_model_JazzSchema *target_schema = &target_schema_version->schema;
    JZ_VEC_FOR(const jazz_protocol_TableLens, table_lens, lens->table_lenses) {
        const jazz_node_PhysicalTableMapping *source_table = jz_btree_get(&source_mapping.tables, &table_lens->source_table);
        if (!source_table) { e = JZN_INVALID_STORED("source physical table mapping missing"); break; }
        const jazz_node_PhysicalTableMapping *pt_ref = jz_btree_get(&target_mapping.tables, &table_lens->target_table);
        if (!pt_ref) { e = JZN_INVALID_STORED("target physical table mapping missing"); break; }
        jazz_node_PhysicalTableMapping provisional_target_table; jazz_node_PhysicalTableMapping_clone(&provisional_target_table, pt_ref);
        const jazz_model_TableSchema *source_table_schema = find_table(&source_schema, &table_lens->source_table);
        const jazz_model_TableSchema *target_table_schema = find_table(target_schema, &table_lens->target_table);
        const jazz_node_PhysicalTableIdentity *ti_ref = jz_btree_get(&target_mapping.identities.tables, &table_lens->target_table);
        if (!source_table_schema) e = JZN_INVALID_STORED("source physical table schema missing");
        else if (!target_table_schema) e = JZN_INVALID_STORED("target physical table schema missing");
        else if (!ti_ref) e = JZN_INVALID_STORED("target physical table identities missing");
        if (e.code) { jazz_node_PhysicalTableMapping_drop(&provisional_target_table); break; }
        jazz_node_PhysicalTableIdentity target_table_identities; jazz_node_PhysicalTableIdentity_clone(&target_table_identities, ti_ref);

        jz_btree columns; jz_btree_init(&columns, &INHERITED_COLUMNS_DESC);
        {
            const jz_str *name; const jazz_types_PhysicalColumnId *id;
            JZ_BTREE_FOR(&source_table->columns, name, id) {
                jz_str key = jz_str_clone(name);
                InheritedColumn v = { *id, true, jz_str_clone(name) };
                jz_btree_insert(&columns, &key, &v, NULL);
            }
        }
        JZ_VEC_FOR(const jazz_protocol_LensOp, op, table_lens->ops) {
            switch (op->tag) {
            case JZ_LENSOP_RENAME_TABLE: case JZ_LENSOP_TRANSFORM_COLUMN: case JZ_LENSOP_REJECT_SOURCE_DELTA: break;
            case JZ_LENSOP_RENAME_COLUMN: {
                jz_str old_key; InheritedColumn column_id;
                if (jz_btree_remove(&columns, &op->from, &old_key, &column_id)) {
                    jz_str_drop(&old_key);
                    jz_str key = jz_str_clone(&op->to);
                    InheritedColumn replaced;
                    if (jz_btree_insert(&columns, &key, &column_id, &replaced)) { jz_str_drop(&key); inherited_column_drop(&replaced); }
                }
                break;
            }
            case JZ_LENSOP_COPY_COLUMN: case JZ_LENSOP_ADD_COLUMN: {
                const jz_str *to = op->tag == JZ_LENSOP_COPY_COLUMN ? &op->to : &op->column;
                const jazz_types_PhysicalColumnId *column_id = jz_btree_get(&provisional_target_table.columns, to);
                if (!column_id) { e = JZN_INVALID_STORED("target provisional physical column mapping missing"); break; }
                jz_str key = jz_str_clone(to);
                InheritedColumn v = { *column_id, false, {0} }, replaced;
                if (jz_btree_insert(&columns, &key, &v, &replaced)) { jz_str_drop(&key); inherited_column_drop(&replaced); }
                break;
            }
            case JZ_LENSOP_DROP_COLUMN: {
                jz_str old_key; InheritedColumn old;
                if (jz_btree_remove(&columns, &op->column, &old_key, &old)) { jz_str_drop(&old_key); inherited_column_drop(&old); }
                break;
            }
            }
            if (e.code) break;
        }
        jz_btree final_columns; jz_btree_init(&final_columns, &jazz_node_COLUMN_IDS_DESC);
        if (!e.code) {
            jz_btree target_column_names; jz_btree_init(&target_column_names, &jazz_node_STRING_SET_DESC);
            JZ_VEC_FOR(const jazz_model_ColumnSchema, column, target_table_schema->columns) {
                jz_str n = jz_str_clone(&column->name); bool ins; jz_btree_entry(&target_column_names, &n, &ins); if (!ins) jz_str_drop(&n);
            }
            RetainNames rn = { &target_column_names };
            jz_btree_retain(&columns, keep_if_name_in, &rn);
            jz_btree_drop(&target_column_names);
            JZ_VEC_FOR(const jazz_model_ColumnSchema, column, target_table_schema->columns) {
                if (jz_btree_contains(&columns, &column->name)) continue;
                const jazz_types_PhysicalColumnId *column_id = jz_btree_get(&provisional_target_table.columns, &column->name);
                if (!column_id) { e = JZN_INVALID_STORED("target provisional physical column mapping missing"); break; }
                jz_str key = jz_str_clone(&column->name);
                InheritedColumn v = { *column_id, false, {0} };
                jz_btree_insert(&columns, &key, &v, NULL);
            }
        }
        if (!e.code) {
            const jz_str *target_name; const InheritedColumn *ic;
            JZ_BTREE_FOR(&columns, target_name, ic) {
                jazz_types_PhysicalColumnId column_id;
                if (ic->has_source && jazz_model__schema__physical_column_epoch_is_compatible(source_table_schema, jz_str_slice(&ic->source_name), target_table_schema, jz_str_slice(target_name))) {
                    column_id = ic->id;
                } else {
                    const jazz_types_PhysicalColumnId *p = jz_btree_get(&provisional_target_table.columns, target_name);
                    if (!p) { e = JZN_INVALID_STORED("target provisional physical column mapping missing"); break; }
                    column_id = *p;
                }
                jz_str key = jz_str_clone(target_name);
                jz_btree_insert(&final_columns, &key, &column_id, NULL);
            }
        }
        jz_btree_drop(&columns);
        jz_btree scalar_enum_cases; jz_btree_init(&scalar_enum_cases, &jazz_node_COLUMN_CASES_DESC);
        jz_btree payload_enum_cases; jz_btree_init(&payload_enum_cases, &jazz_node_COLUMN_CASES_DESC);
        if (!e.code) JZ_VEC_FOR(const jazz_model_ColumnSchema, column, target_table_schema->columns) {
            const jazz_types_PhysicalColumnId *idp = jz_btree_get(&final_columns, &column->name);
            if (!idp) { e = JZN_INVALID_STORED("enum physical column missing"); break; }
            jazz_types_PhysicalColumnId id = *idp;
            u32 count;
            if (groove__records__ValueType__as_enum_tag(&column->column_type, &count)) {
                const jazz_node_GlobalScalarEnumCaseIdVec *existing = jz_btree_get(&source_table->scalar_enum_cases, &id);
                if (!existing) existing = jz_btree_get(&provisional_target_table.scalar_enum_cases, &id);
                jazz_node_GlobalScalarEnumCaseIdVec cases = existing ? jazz_node_GlobalScalarEnumCaseIdVec_clone(existing) : (jazz_node_GlobalScalarEnumCaseIdVec){0};
                if (cases.len > count) { jz_free(cases.ptr); e = JZN_INVALID_STORED("scalar enum registry changed non-additively"); break; }
                const jz_uuid_vec *ids = root_variant_ids(&target_table_identities, &column->name);
                if ((e = jazz_node__node__install_enum_case_ids(&cases, ids, target_schema_version->id)).code) { jz_free(cases.ptr); break; }
                jazz_node_GlobalScalarEnumCaseIdVec old;
                if (jz_btree_insert(&scalar_enum_cases, &id, &cases, &old)) jz_free(old.ptr);
            } else if (groove__records__ValueType__as_enum(&column->column_type, &count)) {
                const jazz_node_GlobalEnumCaseIdVec *existing = jz_btree_get(&source_table->payload_enum_cases, &id);
                if (!existing) existing = jz_btree_get(&provisional_target_table.payload_enum_cases, &id);
                jazz_node_GlobalEnumCaseIdVec cases = existing ? jazz_node_GlobalEnumCaseIdVec_clone(existing) : (jazz_node_GlobalEnumCaseIdVec){0};
                if (cases.len > count) { jz_free(cases.ptr); e = JZN_INVALID_STORED("payload enum registry changed non-additively"); break; }
                const jz_uuid_vec *ids = root_variant_ids(&target_table_identities, &column->name);
                if ((e = jazz_node__node__install_payload_enum_case_ids(&cases, ids, target_schema_version->id)).code) { jz_free(cases.ptr); break; }
                jazz_node_GlobalEnumCaseIdVec old;
                if (jz_btree_insert(&payload_enum_cases, &id, &cases, &old)) jz_free(old.ptr);
            }
        }
        jz_btree nested_scalar_enum_cases; jz_btree_clone(&nested_scalar_enum_cases, &provisional_target_table.nested_scalar_enum_cases, u64_kclone, nested_cases_vclone);
        extend_case_map(&nested_scalar_enum_cases, &source_table->nested_scalar_enum_cases, nested_cases_vclone);
        jz_btree nested_payload_enum_cases; jz_btree_clone(&nested_payload_enum_cases, &provisional_target_table.nested_payload_enum_cases, u64_kclone, nested_cases_vclone);
        extend_case_map(&nested_payload_enum_cases, &source_table->nested_payload_enum_cases, nested_cases_vclone);
        if (!e.code) JZ_VEC_FOR(const jazz_model_ColumnSchema, column, target_table_schema->columns) {
            const jazz_types_PhysicalColumnId *idp = jz_btree_get(&final_columns, &column->name);
            if (!idp) { e = JZN_INVALID_STORED("nested enum physical column missing"); break; }
            bool inserted;
            jz_btree *nested = jz_btree_entry(&nested_scalar_enum_cases, idp, &inserted);
            if (inserted) jz_btree_init(nested, &jazz_node_NESTED_CASES_BY_PATH_DESC);
            const jz_btree *identities = column_enum_variants(&target_table_identities, &column->name);
            if ((e = jazz_node__node__hydrate_nested_scalar_enum_cases(&column->column_type, identities, target_schema_version->id, "root", "root", nested)).code) break;
            jz_btree *nested_payload = jz_btree_entry(&nested_payload_enum_cases, idp, &inserted);
            if (inserted) jz_btree_init(nested_payload, &jazz_node_NESTED_CASES_BY_PATH_DESC);
            if ((e = jazz_node__node__hydrate_nested_payload_enum_cases(&column->column_type, identities, target_schema_version->id, "root", "root", nested_payload)).code) break;
        }
        /* Registry entries belong to a live physical column. Leaving an entry
         * behind for a dropped column makes otherwise identical cross-lens
         * mappings compare unequal solely because one path had an
         * intermediate column epoch. */
        jz_btree live_columns; jz_btree_init(&live_columns, &jazz_node_U64_SET_DESC);
        {
            const jz_str *n; const jazz_types_PhysicalColumnId *id;
            JZ_BTREE_FOR(&final_columns, n, id) jz_btree_insert(&live_columns, id, NULL, NULL);
        }
        jz_btree_retain(&scalar_enum_cases, keep_if_column_live, &live_columns);
        jz_btree_retain(&payload_enum_cases, keep_if_column_live, &live_columns);
        jz_btree_retain(&nested_scalar_enum_cases, keep_if_column_live, &live_columns);
        jz_btree_retain(&nested_payload_enum_cases, keep_if_column_live, &live_columns);
        jz_btree_drop(&live_columns);
        jazz_types_PhysicalTableId source_table_id = source_table->table_id;
        jazz_node_PhysicalTableIdentity_drop(&target_table_identities);
        jazz_node_PhysicalTableMapping_drop(&provisional_target_table);
        if (e.code) {
            jz_btree_drop(&final_columns); jz_btree_drop(&scalar_enum_cases); jz_btree_drop(&payload_enum_cases);
            jz_btree_drop(&nested_scalar_enum_cases); jz_btree_drop(&nested_payload_enum_cases);
            break;
        }
        jazz_node_PhysicalTableMapping table = {
            .table_id = source_table_id, .columns = final_columns, .variant_cases = {0},
            .scalar_enum_cases = scalar_enum_cases, .payload_enum_cases = payload_enum_cases,
            .nested_scalar_enum_cases = nested_scalar_enum_cases, .nested_payload_enum_cases = nested_payload_enum_cases,
        };
        jz_str key = jz_str_clone(&table_lens->target_table);
        jazz_node_PhysicalTableMapping replaced;
        if (jz_btree_insert(&target_mapping.tables, &key, &table, &replaced)) { jz_str_drop(&key); jazz_node_PhysicalTableMapping_drop(&replaced); }
    }
    jazz_model_JazzSchema_drop(&source_schema);
    if (!e.code) e = jazz_node__node__NodeState__validate_candidate_scalar_enum_capacity(catalogue, &target_mapping);
fail_early:
    jazz_node_SchemaPhysicalMapping_drop(&source_mapping);
    if (e.code) { jazz_node_SchemaPhysicalMapping_drop(&target_mapping); return e; }
    *out = target_mapping;
    return JZ_OK;
}

/* source column name -> Option<target name> */
typedef struct TargetName { bool has; jz_str name; } TargetName;
static void target_name_drop(void *p) { TargetName *t = p; if (t->has) jz_str_drop(&t->name); }
static const jz_btree_desc TARGET_NAME_DESC = { sizeof(jz_str), sizeof(TargetName), jz_cmp_str, str_drop_v, target_name_drop };

jz_err jazz_node__node__NodeState__reconcile_source_physical_mapping_for_lens_payload(const jazz_protocol_MigrationLens *lens,
    const jazz_protocol_SchemaVersion *source_schema_version, const jazz_protocol_SchemaVersion *target_schema_version,
    const jazz_node_SchemaPhysicalMapping *provisional_source_mapping, const jazz_node_SchemaPhysicalMapping *target_mapping, jazz_node_SchemaPhysicalMapping *out)
{
    jazz_node_SchemaPhysicalMapping source_mapping; jazz_node_SchemaPhysicalMapping_clone(&source_mapping, provisional_source_mapping);
    jz_err e = JZ_OK;
    JZ_VEC_FOR(const jazz_protocol_TableLens, table_lens, lens->table_lenses) {
        const jazz_node_PhysicalTableMapping *ps_ref = jz_btree_get(&source_mapping.tables, &table_lens->source_table);
        if (!ps_ref) { e = JZN_INVALID_STORED("source provisional physical table mapping missing"); break; }
        jazz_node_PhysicalTableMapping provisional_source_table; jazz_node_PhysicalTableMapping_clone(&provisional_source_table, ps_ref);
        const jazz_node_PhysicalTableMapping *target_table = jz_btree_get(&target_mapping->tables, &table_lens->target_table);
        const jazz_model_TableSchema *source_table_schema = find_table(&source_schema_version->schema, &table_lens->source_table);
        const jazz_model_TableSchema *target_table_schema = find_table(&target_schema_version->schema, &table_lens->target_table);
        if (!target_table) e = JZN_INVALID_STORED("target physical table mapping missing");
        else if (!source_table_schema) e = JZN_INVALID_STORED("source physical table schema missing");
        else if (!target_table_schema) e = JZN_INVALID_STORED("target physical table schema missing");
        if (e.code) { jazz_node_PhysicalTableMapping_drop(&provisional_source_table); break; }

        jz_btree target_name_by_source; jz_btree_init(&target_name_by_source, &TARGET_NAME_DESC);
        JZ_VEC_FOR(const jazz_model_ColumnSchema, column, source_table_schema->columns) {
            jz_str key = jz_str_clone(&column->name);
            TargetName v = { true, jz_str_clone(&column->name) }, old;
            if (jz_btree_insert(&target_name_by_source, &key, &v, &old)) { jz_str_drop(&key); target_name_drop(&old); }
        }
        JZ_VEC_FOR(const jazz_protocol_LensOp, op, table_lens->ops) {
            if (op->tag == JZ_LENSOP_RENAME_COLUMN || op->tag == JZ_LENSOP_DROP_COLUMN) {
                const jz_str *match = op->tag == JZ_LENSOP_RENAME_COLUMN ? &op->from : &op->column;
                const jz_str *k; TargetName *target_name;
                JZ_BTREE_FOR(&target_name_by_source, k, target_name) {
                    if (target_name->has && jz_str_eq(&target_name->name, match)) {
                        jz_str_drop(&target_name->name);
                        if (op->tag == JZ_LENSOP_RENAME_COLUMN) target_name->name = jz_str_clone(&op->to);
                        else { target_name->has = false; target_name->name = (jz_str){0}; }
                        break;
                    }
                }
            }
            /* RenameTable | CopyColumn | AddColumn | TransformColumn | RejectSourceDelta => {} */
        }
        jz_btree columns; jz_btree_init(&columns, &jazz_node_COLUMN_IDS_DESC);
        JZ_VEC_FOR(const jazz_model_ColumnSchema, source_column, source_table_schema->columns) {
            const jazz_types_PhysicalColumnId *provisional_id = jz_btree_get(&provisional_source_table.columns, &source_column->name);
            if (!provisional_id) { e = JZN_INVALID_STORED("source provisional physical column mapping missing"); break; }
            jazz_types_PhysicalColumnId column_id = *provisional_id;
            const TargetName *target_name = jz_btree_get(&target_name_by_source, &source_column->name);
            if (target_name && target_name->has
                && jazz_model__schema__physical_column_epoch_is_compatible(source_table_schema, jz_str_slice(&source_column->name), target_table_schema, jz_str_slice(&target_name->name))) {
                const jazz_types_PhysicalColumnId *t = jz_btree_get(&target_table->columns, &target_name->name);
                if (t) column_id = *t;
            }
            jz_str key = jz_str_clone(&source_column->name);
            jazz_types_PhysicalColumnId old;
            if (jz_btree_insert(&columns, &key, &column_id, &old)) jz_str_drop(&key);
        }
        jz_btree_drop(&target_name_by_source);
        if (e.code) { jz_btree_drop(&columns); jazz_node_PhysicalTableMapping_drop(&provisional_source_table); break; }
        jz_btree live_column_ids; jz_btree_init(&live_column_ids, &jazz_node_U64_SET_DESC);
        { const jz_str *n; const jazz_types_PhysicalColumnId *id; JZ_BTREE_FOR(&columns, n, id) jz_btree_insert(&live_column_ids, id, NULL, NULL); }
        jz_btree scalar_enum_cases; jz_btree_clone(&scalar_enum_cases, &provisional_source_table.scalar_enum_cases, u64_kclone, case_vec_vclone);
        extend_case_map(&scalar_enum_cases, &target_table->scalar_enum_cases, case_vec_vclone);
        jz_btree_retain(&scalar_enum_cases, keep_if_column_live, &live_column_ids);
        jz_btree payload_enum_cases; jz_btree_clone(&payload_enum_cases, &provisional_source_table.payload_enum_cases, u64_kclone, case_vec_vclone);
        extend_case_map(&payload_enum_cases, &target_table->payload_enum_cases, case_vec_vclone);
        jz_btree_retain(&payload_enum_cases, keep_if_column_live, &live_column_ids);
        jz_btree nested_scalar_enum_cases; jz_btree_clone(&nested_scalar_enum_cases, &provisional_source_table.nested_scalar_enum_cases, u64_kclone, nested_cases_vclone);
        extend_case_map(&nested_scalar_enum_cases, &target_table->nested_scalar_enum_cases, nested_cases_vclone);
        jz_btree_retain(&nested_scalar_enum_cases, keep_if_column_live, &live_column_ids);
        jz_btree nested_payload_enum_cases; jz_btree_clone(&nested_payload_enum_cases, &provisional_source_table.nested_payload_enum_cases, u64_kclone, nested_cases_vclone);
        extend_case_map(&nested_payload_enum_cases, &target_table->nested_payload_enum_cases, nested_cases_vclone);
        jz_btree_retain(&nested_payload_enum_cases, keep_if_column_live, &live_column_ids);
        jz_btree_drop(&live_column_ids);
        jazz_node_PhysicalTableMapping table = {
            .table_id = target_table->table_id, .columns = columns,
            .variant_cases = jazz_node_PhysicalVariantCaseVec_clone(&target_table->variant_cases),
            .scalar_enum_cases = scalar_enum_cases, .payload_enum_cases = payload_enum_cases,
            .nested_scalar_enum_cases = nested_scalar_enum_cases, .nested_payload_enum_cases = nested_payload_enum_cases,
        };
        jazz_node_PhysicalTableMapping_drop(&provisional_source_table);
        jz_str key = jz_str_clone(&table_lens->source_table);
        jazz_node_PhysicalTableMapping replaced;
        if (jz_btree_insert(&source_mapping.tables, &key, &table, &replaced)) { jz_str_drop(&key); jazz_node_PhysicalTableMapping_drop(&replaced); }
    }
    if (e.code) { jazz_node_SchemaPhysicalMapping_drop(&source_mapping); return e; }
    *out = source_mapping;
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__persist_catalogue_schema_lineage(NS *self, const jazz_node_StagedSchemaLineage *staged)
{
    jz_bytes payload;
    JZ_TRY(jazz_node__node__codec__encode_catalogue_staged_lineage(staged, &payload));
    groove_DatabaseBatch *batch = groove__db__facade__Database__open_batch(self->database);
    put_catalogue(batch, JZN_CRK_SCHEMA_LINEAGE_STAGED, staged->publication.id, payload);
    return jazz_node__node__NodeState__apply_and_persist_node_batch(self, batch);
}

jz_err jazz_node__node__NodeState__persist_pending_schema_lineage(NS *self, const jazz_node_PendingSchemaLineage *pending)
{
    JZ_TRY(jazz_node__node__NodeState__validate_pending_schema_lineage_physical_mapping(self, pending));
    jz_bytes payload;
    JZ_TRY(jazz_node__node__codec__encode_catalogue_pending_lineage(pending, &payload));
    groove_DatabaseBatch *batch = groove__db__facade__Database__open_batch(self->database);
    put_catalogue(batch, JZN_CRK_SCHEMA_LINEAGE_PENDING, pending->publication.id, payload);
    return jazz_node__node__NodeState__apply_and_persist_node_batch(self, batch);
}

jz_err jazz_node__node__NodeState__remove_pending_schema_lineage(NS *self, u64 catalogue_seq, jazz_types_SchemaLineagePublicationId publication_id)
{
    groove_DatabaseBatch *batch = groove__db__facade__Database__open_batch(self->database);
    delete_catalogue(batch, JZN_CRK_SCHEMA_LINEAGE_PENDING, publication_id);
    JZ_TRY(jazz_node__node__NodeState__apply_and_persist_node_batch(self, batch));
    jazz_node_PendingSchemaLineage removed;
    if (jz_btree_remove(&self->catalogue.pending_lineages, &catalogue_seq, NULL, &removed)) jazz_node_PendingSchemaLineage_drop(&removed);
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__write_active_schema_lineage_to_batch(groove_DatabaseBatch *batch, const jazz_node_StagedSchemaLineage *staged)
{
    const jazz_protocol_SchemaVersion *schema = &staged->publication.schema;
    /* The active receipt retains the canonical staged payload as its durable
     * witness. The pending envelope, however, is an obligation to activate:
     * leaving it behind after this batch commits would make reopen retain a
     * stale retry candidate beside an already-active lineage. Delete it in the
     * same batch that makes the schema, lens, mapping, and active receipt
     * visible, so an active recovery state never also carries a stale pending
     * obligation. */
    delete_catalogue(batch, JZN_CRK_SCHEMA_LINEAGE_PENDING, staged->publication.id);
    /* The active marker only carries an id and sequence. Persist its canonical
     * payload in the same durable activation batch so recovery can prove both
     * the prefix identity and its exact sequence. */
    jz_bytes payload;
    JZ_TRY(jazz_node__node__codec__encode_catalogue_staged_lineage(staged, &payload));
    put_catalogue(batch, JZN_CRK_SCHEMA_LINEAGE_STAGED, staged->publication.id, payload);
    JZ_TRY(jazz_node__node__codec__encode_catalogue_schema(schema, &payload));
    put_catalogue(batch, JZN_CRK_SCHEMA, schema->id, payload);
    JZ_VEC_FOR(const jazz_protocol_SchemaPredecessor, predecessor, staged->publication.predecessors) {
        const jazz_protocol_MigrationLens *lens = &predecessor->lens;
        jz_bytes lens_payload = {0};
        jazz_node__node__codec__encode_catalogue_lens(lens, &lens_payload);
        put_catalogue(batch, JZN_CRK_LENS, lens->id, lens_payload);
    }
    JZ_TRY(jazz_node__node__NodeState__write_schema_version_mapping_to_batch(batch, staged->alias, schema->id, &staged->mapping));
    jazz_node_SchemaLineageActivation active = { staged->publication.id, staged->catalogue_seq };
    jz_bytes active_payload = {0};
    jazz_node__node__codec__encode_catalogue_lineage_activation(active, &active_payload);
    put_catalogue(batch, JZN_CRK_SCHEMA_LINEAGE_ACTIVE, active.id, active_payload);
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__persist_catalogue_lens_with_physical_metadata(NS *self, const jazz_protocol_MigrationLens *lens, const jazz_node_SchemaPhysicalMapping *mapping)
{
    groove_DatabaseBatch *batch = groove__db__facade__Database__open_batch(self->database);
    jz_bytes payload = {0};
    jazz_node__node__codec__encode_catalogue_lens(lens, &payload);
    put_catalogue(batch, JZN_CRK_LENS, lens->id, payload);
    if (mapping) {
        const jazz_types_SchemaVersionAlias *alias = jz_btree_get(&self->catalogue.schema_version_aliases, &lens->target);
        if (!alias) { groove__db__batch__DatabaseBatch__drop(batch); return JZN_INVALID_STORED("physical mapping schema alias missing"); }
        jz_err e = jazz_node__node__NodeState__write_schema_version_mapping_to_batch(batch, *alias, lens->target, mapping);
        if (e.code) { groove__db__batch__DatabaseBatch__drop(batch); return e; }
    }
    return jazz_node__node__NodeState__apply_and_persist_node_batch(self, batch);
}

jz_err jazz_node__node__NodeState__write_schema_version_mapping_to_batch(groove_DatabaseBatch *batch, jazz_types_SchemaVersionAlias alias,
    jazz_types_SchemaVersionId schema_version, const jazz_node_SchemaPhysicalMapping *mapping)
{
    jz_bytes encoded;
    JZ_TRY(jazz_node__node__codec__encode_physical_mapping(mapping, &encoded));
    groove_ValueVec values = {0};
    JZ_VEC_PUSH(values, gv_u64(alias));
    JZ_VEC_PUSH(values, gv_uuid(schema_version));
    JZ_VEC_PUSH(values, gv_bytes(encoded));
    jz_str table = jz_str_from("jazz_schema_versions");
    groove__db__batch__DatabaseBatch__update(batch, &table, values);
    jz_str_drop(&table);
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__write_active_schema_to_batch(groove_DatabaseBatch *batch, const jazz_node_ActiveSchema *active)
{
    jz_bytes payload;
    JZ_TRY(jazz_node__node__codec__encode_active_schema(active, &payload));
    put_catalogue(batch, JZN_CRK_ACTIVE_SCHEMA, (jz_uuid){{0}} /* Uuid::nil() */, payload);
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__write_active_schema_bootstrap_to_batch(const NS *self, groove_DatabaseBatch *batch, const jazz_node_ActiveSchema *active)
{
    if (!self->catalogue_bootstrap_marker) return JZ_OK;
    jz_str table = jz_str_from("jazz_catalogue");
    groove_Value prefix = gv_u64(jazz_node__node__codec__CatalogueRecordKind__key(JZN_CRK_GENESIS));
    groove_RawRowVec raws = {0};
    jz_err e = groove__db__primary_storage__Database__primary_key_scan_raw(self->database, &table, &prefix, 1, &raws);
    jz_str_drop(&table);
    if (e.code) return jazz_node__node__Error_groove(e);
    if (raws.len == 0) { groove_RawRowVec_drop(&raws); return JZN_INVALID_STORED("catalogue genesis missing"); }
    jz_uuid genesis;
    e = groove__records__OwnedRecord__get_uuid(groove__db__RawRow__record(raws.ptr[0]), JZN_CATALOGUE_ROW_FIELD_ID_IDX, &genesis);
    groove_RawRowVec_drop(&raws);
    if (e.code) return jazz_node__node__Error_record(e);
    jazz_node_CatalogueBootstrapReady ready = {
        .genesis = genesis,
        .current_write_schema = jazz_node__node__ActiveSchema__wire_pointer(active),
        .active_catalogue_seq = self->catalogue.active_catalogue_seq,
    };
    jz_bytes payload = {0};
    jazz_node__node__codec__encode_catalogue_bootstrap_ready(&ready, &payload);
    put_catalogue(batch, JZN_CRK_BOOTSTRAP_READY, ready.genesis, payload);
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__persist_active_schema(NS *self, const jazz_node_ActiveSchema *active)
{
    groove_DatabaseBatch *batch = groove__db__facade__Database__open_batch(self->database);
    jz_err e = jazz_node__node__NodeState__write_active_schema_to_batch(batch, active);
    if (!e.code) e = jazz_node__node__NodeState__write_active_schema_bootstrap_to_batch(self, batch, active);
    if (e.code) { groove__db__batch__DatabaseBatch__drop(batch); return e; }
    /* Migrate a legacy policy-bearing catalogue payload on first activation. */
    jazz_protocol_SchemaVersion structural = jazz_protocol__protocol__SchemaVersion__new(jazz_model__schema__JazzSchema__without_permissions(&active->compiled));
    jz_bytes payload;
    e = jazz_node__node__codec__encode_catalogue_schema(&structural, &payload);
    if (e.code) { jazz_protocol_SchemaVersion_drop(&structural); groove__db__batch__DatabaseBatch__drop(batch); return e; }
    put_catalogue(batch, JZN_CRK_SCHEMA, structural.id, payload);
    jazz_protocol_SchemaVersion_drop(&structural);
    return jazz_node__node__NodeState__apply_and_persist_node_batch(self, batch);
}

jz_err jazz_node__node__NodeState__activate_schema(NS *self, u64 revision, jazz_model_JazzSchema compiled)
{
    jazz_node_ActiveSchema active;
    JZ_TRY(jazz_node__node__NodeState__prepare_active_schema(self, revision, compiled, &active));
    if (jazz_node_ActiveSchema_eq(&active, &self->catalogue.active_schema)) { jazz_node_ActiveSchema_drop(&active); return JZ_OK; }
    jz_err e = jazz_node__node__NodeState__persist_active_schema(self, &active);
    if (e.code) { jazz_node_ActiveSchema_drop(&active); return e; }
    jazz_node__node__NodeState__install_active_schema(self, active);
    return JZ_OK;
}

#ifdef JZ_FEATURE_RUNTIME
jz_err jazz_node__node__NodeState__validate_schema_activation(const NS *self, u64 revision, jazz_model_JazzSchema compiled)
{
    jazz_node_ActiveSchema active;
    JZ_TRY(jazz_node__node__NodeState__prepare_active_schema(self, revision, compiled, &active));
    jazz_node_ActiveSchema_drop(&active);
    return JZ_OK;
}
#endif

jz_err jazz_node__node__NodeState__prepare_active_schema(const NS *self, u64 revision, jazz_model_JazzSchema compiled, jazz_node_ActiveSchema *out)
{
    jz_err e = jazz_node__node__NodeState__require_catalogue_ready(self);
    if (!e.code && self->catalogue_activation_failed) e = JZN_CATALOGUE_ACTIVATION_FAILED();
    if (!e.code && revision == 0) e = JZN_INVALID_CATALOGUE("active schema revision must be positive");
    if (e.code) { jazz_model_JazzSchema_drop(&compiled); return e; }
    jazz_protocol_CurrentWriteSchema pointer = { revision, jazz_model__schema__JazzSchema__version_id(&compiled) };
    if (!jz_btree_contains(&self->catalogue.catalogue_schemas, &pointer.schema)) {
        jazz_model_JazzSchema_drop(&compiled);
        return JZN_INVALID_CATALOGUE("active schema requires admitted structural lineage");
    }
    jazz_node_LensStepVec path = {0};
    if (!jazz_node__node__NodeState__shortest_lens_path(self, self->catalogue.active_schema.schema, pointer.schema, &path)) {
        jazz_model_JazzSchema_drop(&compiled);
        return JZN_INVALID_CATALOGUE("active schema requires a complete migration path");
    }
    jz_free(path.ptr);
    jazz_node_ActiveSchema active;
    JZ_TRY(jazz_node__node__ActiveSchema__new(pointer, compiled, &active));
    if (revision < self->catalogue.active_schema.revision) {
        jazz_node_ActiveSchema_drop(&active);
        return JZN_INVALID_CATALOGUE("stale active schema revision");
    }
    if (revision == self->catalogue.active_schema.revision && !jazz_node_ActiveSchema_eq(&active, &self->catalogue.active_schema)) {
        jazz_node_ActiveSchema_drop(&active);
        return JZN_INVALID_CATALOGUE("conflicting active schema revision");
    }
    *out = active;
    return JZ_OK;
}

void jazz_node__node__NodeState__install_active_schema(NS *self, jazz_node_ActiveSchema active)
{
    bool authorization_source_changed = !jazz_node__node__ActiveSchema__same_authorization_source(&active, &self->catalogue.active_schema,
        jz_btree_get(&self->catalogue.physical_mappings, &active.schema),
        jz_btree_get(&self->catalogue.physical_mappings, &self->catalogue.active_schema.schema));
    jazz_protocol_SchemaVersion structural = jazz_protocol__protocol__SchemaVersion__new(jazz_model__schema__JazzSchema__without_permissions(&active.compiled));
    jazz_protocol_SchemaVersion replaced;
    if (jz_btree_insert(&self->catalogue.catalogue_schemas, &active.schema, &structural, &replaced)) jazz_protocol_SchemaVersion_drop(&replaced);
    if (jz_uuid_eq(&active.schema, &self->catalogue.local_schema_version_id)) {
        jazz_model_JazzSchema_drop(&self->catalogue.schema);
        jazz_model_JazzSchema_clone(&self->catalogue.schema, &active.compiled);
    }
    jazz_node_ActiveSchema_drop(&self->catalogue.active_schema);
    self->catalogue.active_schema = active;
    jz_btree_clear(&self->query.version_storage_sources_cache);
    jz_btree_clear(&self->query.compiled_query_program_cache);
    jazz_engine__query_engine__QueryProgramTemplateCache__clear(self->query.query_program_templates);
    jazz_node__node__QueryServing__clear_supported_query_program_requests(&self->query);
    jz_btree_clear(&self->query.read_policy_authorization_request_cache);
    jz_btree_clear(&self->query.policy_authorization_graph_cache);
    jz_btree_clear(&self->query.policy_authorization_graph_replacements);
    if (authorization_source_changed) self->groove_runtime_token = jazz_node__node__next_groove_runtime_token();
}

jz_err jazz_node__node__NodeState__ensure_node_alias(NS *self, jazz_types_NodeUuid node_uuid, jazz_types_NodeAlias *out)
{
    if (jz_uuid_eq(&node_uuid, &self->node_uuid) && self->has_self_node_alias) { *out = self->self_node_alias; return JZ_OK; }
    jazz_types_NodeAlias existing;
    if (jazz_node__node__NodeAliases__get(self->node_aliases, &node_uuid, &existing)) {
        if (jz_uuid_eq(&node_uuid, &self->node_uuid)) { self->has_self_node_alias = true; self->self_node_alias = existing; }
        *out = existing;
        return JZ_OK;
    }
    /* Clear before the first await, not after successful persistence: a
     * cancelled/failed writer must not leave an absence proof alive after its
     * alias may have entered resident or durable storage. */
    self->has_absent_node_alias = false;
    u64 max_alias = jazz_node__node__NodeAliases__max_alias(self->node_aliases);
    jz_str jazz_nodes = jz_str_from("jazz_nodes");
    groove_RawRowVec raws = {0};
    jz_err e = groove__db__primary_storage__Database__primary_key_scan_raw(self->database, &jazz_nodes, NULL, 0, &raws);
    if (e.code) { jz_str_drop(&jazz_nodes); return jazz_node__node__Error_groove(e); }
    for (u32 i = 0; i < raws.len; ++i) {
        const groove_OwnedRecord *record = groove__db__RawRow__record(raws.ptr[i]);
        u64 alias; jz_uuid uuid;
        if ((e = groove__records__OwnedRecord__get_u64(record, JZN_NODE_ALIAS_ROW_FIELD_ID_IDX, &alias)).code) break;
        if (alias > max_alias) max_alias = alias;
        if ((e = groove__records__OwnedRecord__get_uuid(record, JZN_NODE_ALIAS_ROW_FIELD_UUID_IDX, &uuid)).code) break;
        if (jz_uuid_eq(&uuid, &node_uuid)) {
            jazz_node__node__NodeAliases__insert(self->node_aliases, node_uuid, alias);
            if (jz_uuid_eq(&node_uuid, &self->node_uuid)) { self->has_self_node_alias = true; self->self_node_alias = alias; }
            groove_RawRowVec_drop(&raws);
            jz_str_drop(&jazz_nodes);
            *out = alias;
            return JZ_OK;
        }
    }
    groove_RawRowVec_drop(&raws);
    if (e.code) { jz_str_drop(&jazz_nodes); return jazz_node__node__Error_record(e); }
    if (max_alias == UINT64_MAX) { jz_str_drop(&jazz_nodes); return JZN_INVALID_STORED("node alias exhausted"); }
    jazz_types_NodeAlias alias = max_alias + 1;
    groove_DatabaseBatch *batch = groove__db__facade__Database__open_batch(self->database);
    groove_ValueVec values = {0};
    JZ_VEC_PUSH(values, gv_u64(alias));
    JZ_VEC_PUSH(values, gv_uuid(node_uuid));
    groove__db__batch__DatabaseBatch__insert(batch, &jazz_nodes, values);
    jz_str_drop(&jazz_nodes);
    JZ_TRY(jazz_node__node__NodeState__apply_and_persist_node_batch(self, batch));
    /* This mapping is a durable prerequisite for every later row that contains
     * the compact alias. Do not leave an in-memory alias behind if resident
     * application or ordered persistence fails: the caller may only observe it
     * after its catalogue row is durable. */
    jazz_node__node__NodeAliases__insert(self->node_aliases, node_uuid, alias);
    if (jz_uuid_eq(&node_uuid, &self->node_uuid)) { self->has_self_node_alias = true; self->self_node_alias = alias; }
    *out = alias;
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__ensure_schema_version_alias(NS *self, jazz_types_SchemaVersionId schema_version_id, jazz_types_SchemaVersionAlias *out)
{
    const jazz_types_SchemaVersionAlias *alias = jz_btree_get(&self->catalogue.schema_version_aliases, &schema_version_id);
    if (alias) {
        if (jz_uuid_eq(&schema_version_id, &self->catalogue.local_schema_version_id)) {
            self->catalogue.has_local_schema_version_alias = true;
            self->catalogue.local_schema_version_alias = *alias;
        }
        *out = *alias;
        return JZ_OK;
    }
    JZ_TRY(jazz_node__node__NodeState__ensure_provisional_physical_mapping(self, schema_version_id));
    alias = jz_btree_get(&self->catalogue.schema_version_aliases, &schema_version_id);
    if (!alias) return JZN_INVALID_STORED("schema version alias allocation failed");
    *out = *alias;
    return JZ_OK;
}

bool jazz_node__node__NodeState__schema_version_for_alias(const NS *self, jazz_types_SchemaVersionAlias alias, jazz_types_SchemaVersionId *out)
{
    const jazz_types_SchemaVersionId *id; const jazz_types_SchemaVersionAlias *candidate;
    JZ_BTREE_FOR(&self->catalogue.schema_version_aliases, id, candidate)
        if (*candidate == alias) { *out = *id; return true; }
    return false;
}

/* The compiled schema for `version`: the active schema (carrying its
 * permissions), the node's local API schema, or a catalogue entry. */
const jazz_model_JazzSchema *jazz_node__node__SchemaCatalogue__schema_for_version(const jazz_node_SchemaCatalogue *self, jazz_types_SchemaVersionId version)
{
    if (jz_uuid_eq(&version, &self->active_schema.schema)) return &self->active_schema.compiled;
    if (jz_uuid_eq(&version, &self->local_schema_version_id)) return &self->schema;
    const jazz_protocol_SchemaVersion *entry = jz_btree_get(&self->catalogue_schemas, &version);
    return entry ? &entry->schema : NULL;
}

/* Stage an upsert of one `jazz_catalogue` record. */
static void put_catalogue(groove_DatabaseBatch *batch, u32 kind, jz_uuid id, jz_bytes payload)
{
    groove_ValueVec values = {0};
    JZ_VEC_PUSH(values, gv_u64(jazz_node__node__codec__CatalogueRecordKind__key(kind)));
    JZ_VEC_PUSH(values, gv_uuid(id));
    JZ_VEC_PUSH(values, gv_bytes(payload));
    jz_str table = jz_str_from("jazz_catalogue");
    groove__db__batch__DatabaseBatch__update(batch, &table, values);
    jz_str_drop(&table);
}

/* Stage the deletion of one `jazz_catalogue` record. */
static void delete_catalogue(groove_DatabaseBatch *batch, u32 kind, jz_uuid id)
{
    jazz_node_PrimaryKeyValue key = { .tag = JZN_PKV_COMPOSITE };
    JZ_VEC_PUSH(key.parts, ((jazz_node_PrimaryKeyValue){ .tag = JZN_PKV_U64, .u64_ = jazz_node__node__codec__CatalogueRecordKind__key(kind) }));
    JZ_VEC_PUSH(key.parts, ((jazz_node_PrimaryKeyValue){ .tag = JZN_PKV_UUID, .uuid = id }));
    jz_str table = jz_str_from("jazz_catalogue");
    groove__db__batch__DatabaseBatch__delete(batch, &table, key);
    jz_str_drop(&table);
}
