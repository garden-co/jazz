/* Port of crates/jazz/layers/node/src/node/state/lifecycle.rs
 * (include!d into node/mod.rs, so the Rust module is jazz_node::node).
 * Async fns are synchronous; `T: ReopenableStorage` storage is erased to an
 * opaque BoxedStorage pointer exactly as the Rust constructors erase it.
 * Skipped (#[cfg(test)] / #[cfg(feature = "testing")]): pending_upload_count_for_test,
 * new_with_shared_test_catalogue, new_with_open_receipt_for_test, the
 * NodeOpenReceipt timing/receipt plumbing and the `genesis_identities`
 * fixture override in new_with_options_inner / open_catalogue_stage,
 * set_relay_authority_session_owner_for_test, set_test_provider_claims,
 * admit_test_session_claims, attach_large_cell_for_test, and the
 * delivery_diagnostics record in retire_authority_result_view.
 * select_durable_reopen_schema and enable_authoritative_scalar_exit_refresh
 * are #[cfg(feature = "runtime")] and kept under JZ_FEATURE_RUNTIME. */
#include "jz_rt.h"
#include "../../_ext_node-qe-b.h"
#include "../node_qe_b.h"

#define NS jazz_node_NodeState

static void uuid_kclone(void *dst, const void *src) { memcpy(dst, src, sizeof(jz_uuid)); }
static void u64_kclone(void *dst, const void *src) { memcpy(dst, src, sizeof(u64)); }
static void str_kclone(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone(src); }
static void value_vclone(void *dst, const void *src) { groove_Value_clone(dst, src); }
static void schema_version_vclone(void *dst, const void *src) { jazz_protocol_SchemaVersion_clone(dst, src); }
static void alias_vclone(void *dst, const void *src) { memcpy(dst, src, sizeof(jazz_types_SchemaVersionAlias)); }
static void mapping_vclone(void *dst, const void *src) { jazz_node_SchemaPhysicalMapping_clone(dst, src); }

static jz_err groove_err(jz_err e) { return e.code ? jazz_node__node__Error_groove(e) : e; }
static jz_err record_err(jz_err e) { return e.code ? jazz_node__node__Error_record(e) : e; }

/* ===================== ActiveSessionClaimsScope ===================== */

/* A cancellation-safe temporary claim scope for one node-locked operation.
 * The scope owns the mutable node borrow, so its drop runs before the node
 * lock is released. */
void jazz_node__node__ActiveSessionClaimsScope__drop(jazz_node_ActiveSessionClaimsScope *scope)
{
    NS *node = scope->node;
    if (node->has_active_session_claims) jz_btree_drop(&node->active_session_claims);
    node->has_active_session_claims = scope->has_previous;
    if (scope->has_previous) {
        node->active_session_identity = scope->previous_identity;
        node->active_session_claims = scope->previous_claims;
    }
    scope->has_previous = false;
}

/* ===================== constructors ===================== */

/* Read one `jazz_catalogue` row. */
static jz_err catalogue_row(const groove_RawRow *raw, u32 *kind, jz_uuid *id, jz_slice *payload)
{
    const groove_OwnedRecord *record = groove__db__RawRow__record(raw);
    u64 key;
    JZ_TRY(record_err(groove__records__OwnedRecord__get_u64(record, JZN_CATALOGUE_ROW_FIELD_KIND_IDX, &key)));
    JZ_TRY(jazz_node__node__codec__CatalogueRecordKind__from_key(key, kind));
    JZ_TRY(record_err(groove__records__OwnedRecord__get_uuid(record, JZN_CATALOGUE_ROW_FIELD_ID_IDX, id)));
    if (payload) JZ_TRY(record_err(groove__records__OwnedRecord__get_bytes(record, JZN_CATALOGUE_ROW_FIELD_PAYLOAD_IDX, payload)));
    return JZ_OK;
}

static jz_err open_meta_database(groove_DbSchema meta_schema, jazz_node_BoxedStorage *storage, groove_Database **out)
{
    return groove_err(groove__db__Database__new_with_storage_layout(meta_schema, storage, groove__db__StorageLayout__jazz_class_v2(), out));
}

/* Open or create a node over the supplied storage. */
jz_err jazz_node__node__NodeState__new(jazz_types_NodeUuid node_uuid, jazz_model_JazzSchema schema, jazz_node_BoxedStorage *storage, NS **out)
{
    return jazz_node__node__NodeState__new_with_history_complete(node_uuid, schema, storage, false, out);
}

/* Recover a partial client replica using a schema already admitted by its
 * durable catalogue. The requested schema remains a Db view only; this path
 * never publishes it or allocates its physical mapping. */
jz_err jazz_node__node__NodeState__new_client(jazz_types_NodeUuid node_uuid, jazz_model_JazzSchema requested_schema,
    jazz_node_BoxedStorage *storage, bool history_complete, NS **out)
{
    jazz_model_JazzSchema empty = jazz_model__schema__JazzSchema__empty();
    groove_Database *meta_database;
    jz_err e = open_meta_database(jazz_model__schema__JazzSchema__lower_catalogue_meta_to_groove(&empty), storage, &meta_database);
    jazz_model_JazzSchema_drop(&empty);
    if (e.code) { jazz_model_JazzSchema_drop(&requested_schema); return e; }
    jz_str catalogue_table = jz_str_from("jazz_catalogue");
    groove_Value requested_key[2] = {
        gv_u64(jazz_node__node__codec__CatalogueRecordKind__key(JZN_CRK_SCHEMA)),
        gv_uuid(jazz_model__schema__JazzSchema__version_id(&requested_schema)),
    };
    groove_RawRow *raw = NULL;
    jazz_model_JazzSchema schema;
    e = groove_err(groove__db__primary_storage__Database__primary_key_get_raw(meta_database, &catalogue_table, requested_key, 2, &raw));
    if (e.code) goto fail;
    if (raw) {
        groove__db__RawRow__drop(raw);
        schema = requested_schema;
    } else {
        groove_Value genesis_prefix = gv_u64(jazz_node__node__codec__CatalogueRecordKind__key(JZN_CRK_GENESIS));
        groove_RawRowVec genesis_rows;
        if ((e = groove_err(groove__db__primary_storage__Database__primary_key_scan_raw(meta_database, &catalogue_table, &genesis_prefix, 1, &genesis_rows))).code) goto fail;
        if (genesis_rows.len == 0) {
            schema = requested_schema;
        } else if (genesis_rows.len == 1) {
            jz_uuid id;
            e = record_err(groove__records__OwnedRecord__get_uuid(groove__db__RawRow__record(genesis_rows.ptr[0]), JZN_CATALOGUE_ROW_FIELD_ID_IDX, &id));
            groove_RawRowVec_drop(&genesis_rows);
            if (e.code) goto fail;
            groove_Value key[2] = { gv_u64(jazz_node__node__codec__CatalogueRecordKind__key(JZN_CRK_SCHEMA)), gv_uuid(id) };
            if ((e = groove_err(groove__db__primary_storage__Database__primary_key_get_raw(meta_database, &catalogue_table, key, 2, &raw))).code) goto fail;
            if (!raw) { e = JZN_INVALID_STORED("durable genesis schema is missing"); goto fail; }
            jz_slice payload;
            e = record_err(groove__records__OwnedRecord__get_bytes(groove__db__RawRow__record(raw), JZN_CATALOGUE_ROW_FIELD_PAYLOAD_IDX, &payload));
            jazz_protocol_SchemaVersion decoded;
            if (!e.code) e = jazz_node__node__codec__decode_catalogue_schema(payload, &decoded);
            groove__db__RawRow__drop(raw);
            if (e.code) goto fail;
            if (!jz_uuid_eq(&decoded.id, &id)) { jazz_protocol_SchemaVersion_drop(&decoded); e = JZN_INVALID_STORED("catalogue schema id mismatch"); goto fail; }
            schema = decoded.schema;
            jazz_model_JazzSchema_drop(&requested_schema);
        } else {
            groove_RawRowVec_drop(&genesis_rows);
            e = JZN_INVALID_STORED("duplicate catalogue genesis marker");
            goto fail;
        }
    }
    jz_str_drop(&catalogue_table);
    /* Ordinary recovery validates every catalogue record and physical mapping.
     * Discovery adds only a point read on an already-admitted schema open. */
    return jazz_node__node__NodeState__new_with_options_inner(node_uuid, schema, groove__db__Database__into_storage(meta_database),
        history_complete, JZN_CBS_READY, out);
fail:
    jz_str_drop(&catalogue_table);
    groove__db__Database__drop(meta_database);
    jazz_model_JazzSchema_drop(&requested_schema);
    return e;
}

/* Open a downstream runtime before it has received an authenticated authority
 * catalogue. Unlike `new`, this deliberately does not create a durable genesis
 * from `JazzSchema::empty()`. */
jz_err jazz_node__node__NodeState__new_catalogue_uninitialized(jazz_types_NodeUuid node_uuid, jazz_node_BoxedStorage *storage, NS **out)
{
    jazz_node_BoxedStorage *boxed; bool has_genesis; jazz_model_JazzSchema durable_genesis;
    JZ_TRY(jazz_node__node__NodeState__discover_durable_catalogue_genesis(storage, &boxed, &has_genesis, &durable_genesis));
    if (has_genesis) {
        /* A fresh process cannot assume the temporary empty schema: recover
         * the authority genesis first, then use the normal ready open path so
         * all physical layouts are reconstructed from the real lineage. */
        return jazz_node__node__NodeState__new_with_options_inner(node_uuid, durable_genesis, boxed, false, JZN_CBS_READY, out);
    }
    return jazz_node__node__NodeState__new_with_options_inner(node_uuid, jazz_model__schema__JazzSchema__empty(), boxed, false,
        JZN_CBS_UNINITIALIZED, out);
}

static const jz_btree_desc SCHEMA_VERSIONS_DESC_LOCAL = { sizeof(jz_uuid), sizeof(jazz_protocol_SchemaVersion), jz_cmp_uuid, NULL, jazz_protocol_SchemaVersion_drop };
static const jz_btree_desc UUID_U64_DESC = { sizeof(jz_uuid), sizeof(u64), jz_cmp_uuid, NULL, NULL };
static const jz_btree_desc STAGED_BY_ID_DESC = { sizeof(jz_uuid), sizeof(jazz_node_StagedSchemaLineage), jz_cmp_uuid, NULL, jazz_node_StagedSchemaLineage_drop };

/* Read only the fixed catalogue metadata layout to discover an already
 * durable authority genesis before choosing an application schema for a
 * fresh process. Never uses `JazzSchema::empty()` as a genesis candidate. */
jz_err jazz_node__node__NodeState__discover_durable_catalogue_genesis(jazz_node_BoxedStorage *storage,
    jazz_node_BoxedStorage **storage_out, bool *has_schema, jazz_model_JazzSchema *schema_out)
{
    *has_schema = false;
    jazz_model_JazzSchema bootstrap_schema = jazz_model__schema__JazzSchema__empty();
    /* Dynamic discovery must inspect the fixed history/branch/fate stores too:
     * an empty catalogue does not make an existing Jazz store safe to
     * repurpose as an uninitialized runtime. */
    groove_DbSchema meta_schema = jazz_model__schema__JazzSchema__lower_to_groove(&bootstrap_schema);
    jazz_model_JazzSchema_drop(&bootstrap_schema);
    groove_Database *meta_database;
    JZ_TRY(open_meta_database(meta_schema, storage, &meta_database));
    bool has_genesis = false; jazz_types_SchemaVersionId genesis = {{0}};
    jz_btree schemas; jz_btree_init(&schemas, &SCHEMA_VERSIONS_DESC_LOCAL);
    bool has_ready = false; jazz_node_CatalogueBootstrapReady bootstrap_ready;
    bool has_active_selection = false; jazz_node_ActiveSchema active_selection;
    jz_btree staged_lineages; jz_btree_init(&staged_lineages, &STAGED_BY_ID_DESC);
    jz_btree active_lineages; jz_btree_init(&active_lineages, &UUID_U64_DESC);
    jz_btree mapping_ids; jz_btree_init(&mapping_ids, &jazz_node_UUID_SET_DESC);
    jz_btree active_lineage_targets; jz_btree_init(&active_lineage_targets, &jazz_node_UUID_SET_DESC);
    jz_btree active_catalogue_sequences; jz_btree_init(&active_catalogue_sequences, &jazz_node_U64_SET_DESC);
    bool has_catalogue_residue = false;
    jz_str catalogue_table = jz_str_from("jazz_catalogue");
    groove_RawRowVec raws = {0};
    jz_err e = groove_err(groove__db__primary_storage__Database__primary_key_scan_raw(meta_database, &catalogue_table, NULL, 0, &raws));
    for (u32 i = 0; !e.code && i < raws.len; ++i) {
        has_catalogue_residue = true;
        u32 kind; jz_uuid id; jz_slice payload;
        if ((e = catalogue_row(raws.ptr[i], &kind, &id, &payload)).code) break;
        switch (kind) {
        case JZN_CRK_GENESIS:
            if (has_genesis) { e = JZN_INVALID_STORED("duplicate catalogue genesis marker"); break; }
            has_genesis = true; genesis = id;
            break;
        case JZN_CRK_ACTIVE_SCHEMA: {
            jz_uuid nil = {{0}};
            if (!jz_uuid_eq(&id, &nil) || has_active_selection) { e = JZN_INVALID_STORED("invalid active schema record identity"); break; }
            if ((e = jazz_node__node__codec__decode_active_schema(payload, &active_selection)).code) break;
            has_active_selection = true;
            break;
        }
        case JZN_CRK_SCHEMA: {
            jazz_protocol_SchemaVersion schema;
            if ((e = jazz_node__node__codec__decode_catalogue_schema(payload, &schema)).code) break;
            if (!jz_uuid_eq(&schema.id, &id) || jz_btree_contains(&schemas, &schema.id)) {
                jazz_protocol_SchemaVersion_drop(&schema); e = JZN_INVALID_STORED("catalogue schema id mismatch"); break;
            }
            jz_btree_insert(&schemas, &schema.id, &schema, NULL);
            break;
        }
        case JZN_CRK_SCHEMA_LINEAGE_ACTIVE: {
            jazz_node_SchemaLineageActivation active;
            if ((e = jazz_node__node__codec__decode_catalogue_lineage_activation(payload, &active)).code) break;
            if (!jz_uuid_eq(&active.id, &id) || active.catalogue_seq == 0 || jz_btree_insert(&active_lineages, &active.id, &active.catalogue_seq, NULL))
                e = JZN_INVALID_STORED("catalogue bootstrap active lineage marker is malformed");
            break;
        }
        case JZN_CRK_SCHEMA_LINEAGE_STAGED: {
            jazz_node_StagedSchemaLineage staged;
            if ((e = jazz_node__node__codec__decode_catalogue_staged_lineage(payload, &staged)).code) break;
            if (!jz_uuid_eq(&staged.publication.id, &id) || staged.catalogue_seq == 0 || jz_btree_contains(&staged_lineages, &staged.publication.id)) {
                jazz_node_StagedSchemaLineage_drop(&staged);
                e = JZN_INVALID_STORED("catalogue bootstrap staged lineage marker is malformed");
                break;
            }
            jz_btree_insert(&staged_lineages, &staged.publication.id, &staged, NULL);
            break;
        }
        case JZN_CRK_BOOTSTRAP_READY: {
            jazz_node_CatalogueBootstrapReady ready;
            if ((e = jazz_node__node__codec__decode_catalogue_bootstrap_ready(payload, &ready)).code) break;
            if (!jz_uuid_eq(&ready.genesis, &id) || has_ready) { e = JZN_INVALID_STORED("duplicate or malformed catalogue bootstrap marker"); break; }
            has_ready = true; bootstrap_ready = ready;
            break;
        }
        default: break;
        }
    }
    groove_RawRowVec_drop(&raws);
    if (e.code) goto out;
    {
        jz_str versions_table = jz_str_from("jazz_schema_versions");
        e = groove_err(groove__db__primary_storage__Database__primary_key_scan_raw(meta_database, &versions_table, NULL, 0, &raws));
        jz_str_drop(&versions_table);
        for (u32 i = 0; !e.code && i < raws.len; ++i) {
            jz_uuid schema_id;
            if ((e = record_err(groove__records__OwnedRecord__get_uuid(groove__db__RawRow__record(raws.ptr[i]), JZN_SCHEMA_VERSION_ALIAS_ROW_FIELD_UUID_IDX, &schema_id))).code) break;
            jz_btree_insert(&mapping_ids, &schema_id, NULL, NULL);
        }
        groove_RawRowVec_drop(&raws);
        if (e.code) goto out;
    }
    bool has_durable_pointer = has_active_selection;
    jazz_protocol_CurrentWriteSchema durable_pointer = {0};
    if (has_active_selection) durable_pointer = jazz_node__node__ActiveSchema__wire_pointer(&active_selection);
    bool has_non_catalogue_residue = false;
    static const char *const history_tables[2] = { "jazz_transactions", "jazz_rejected_transactions" };
    for (u32 t = 0; t < 2; ++t) {
        jz_str table = jz_str_from(history_tables[t]);
        e = groove_err(groove__db__primary_storage__Database__primary_key_scan_raw(meta_database, &table, NULL, 0, &raws));
        jz_str_drop(&table);
        if (e.code) goto out;
        bool non_empty = raws.len != 0;
        groove_RawRowVec_drop(&raws);
        if (non_empty) { has_non_catalogue_residue = true; break; }
    }
    bool has_residue = has_catalogue_residue || mapping_ids.len != 0 || has_durable_pointer || has_non_catalogue_residue;
    if (!has_ready) {
        if (!has_residue) goto out;
        e = has_non_catalogue_residue
            ? JZN_INVALID_STORED("dynamic catalogue state cannot initialize over durable history")
            : JZN_INVALID_STORED("dynamic catalogue state has no bootstrap completion marker");
        goto out;
    }
    {
        const jz_uuid *k; const jazz_node_StagedSchemaLineage *staged;
        JZ_BTREE_FOR(&staged_lineages, k, staged) {
            if ((e = jazz_node__node__NodeState__validate_durable_staged_lineage(staged, &schemas)).code) goto out;
            u64 sequence;
            if (jz_btree_remove(&active_lineages, &staged->publication.id, NULL, &sequence)) {
                if (sequence != staged->catalogue_seq) { e = JZN_INVALID_STORED("catalogue bootstrap active lineage marker conflicts with payload"); goto out; }
                /* The original publication retains its metadata. Later
                 * deployments may refresh defaults, indexes and grants, but
                 * must still belong to the same row structure. */
                const jazz_protocol_SchemaVersion *schema = jz_btree_get(&schemas, &staged->publication.schema.id);
                if (!schema || !jazz_model__schema__JazzSchema__has_same_lineage_structure(&schema->schema, &staged->publication.schema.schema)
                    || jz_btree_insert(&active_lineage_targets, &staged->publication.schema.id, NULL, NULL)) {
                    e = JZN_INVALID_STORED("catalogue bootstrap active lineage does not own its schema"); goto out;
                }
                if (jz_btree_insert(&active_catalogue_sequences, &sequence, NULL, NULL)) {
                    e = JZN_INVALID_STORED("catalogue bootstrap active lineage marker is malformed"); goto out;
                }
            } else if (jz_btree_contains(&schemas, &staged->publication.schema.id) || jz_btree_contains(&mapping_ids, &staged->publication.schema.id)) {
                e = JZN_INVALID_STORED("catalogue bootstrap inactive lineage has durable target state"); goto out;
            }
        }
    }
    if (active_lineages.len != 0) { e = JZN_INVALID_STORED("catalogue bootstrap active lineage is missing canonical payload"); goto out; }
    {
        jz_btree expected_schema_ids; jz_btree_init(&expected_schema_ids, &jazz_node_UUID_SET_DESC);
        jz_btree_insert(&expected_schema_ids, &bootstrap_ready.genesis, NULL, NULL);
        const jz_uuid *target; const void *unit;
        JZ_BTREE_FOR(&active_lineage_targets, target, unit) jz_btree_insert(&expected_schema_ids, target, NULL, NULL);
        jz_btree schema_ids; jz_btree_init(&schema_ids, &jazz_node_UUID_SET_DESC);
        const jz_uuid *sk; const void *sv;
        JZ_BTREE_FOR(&schemas, sk, sv) jz_btree_insert(&schema_ids, sk, NULL, NULL);
        bool contiguous = true;
        u64 expected = 1;
        const u64 *seq;
        JZ_BTREE_FOR(&active_catalogue_sequences, seq, unit) { if (*seq != expected) { contiguous = false; break; } ++expected; }
        if (contiguous && expected - 1 != bootstrap_ready.active_catalogue_seq) contiguous = false;
        bool mismatch = !has_genesis || !jz_uuid_eq(&genesis, &bootstrap_ready.genesis)
            || !has_durable_pointer || durable_pointer.revision != bootstrap_ready.current_write_schema.revision
            || !jz_uuid_eq(&durable_pointer.schema, &bootstrap_ready.current_write_schema.schema)
            || !jz_btree_contains(&schemas, &bootstrap_ready.genesis)
            || !jz_btree_eq(&schema_ids, &expected_schema_ids, NULL)
            || !jz_btree_eq(&mapping_ids, &expected_schema_ids, NULL)
            || (u64)active_catalogue_sequences.len != bootstrap_ready.active_catalogue_seq
            || !contiguous;
        jz_btree_drop(&schema_ids);
        jz_btree_drop(&expected_schema_ids);
        if (mismatch) { e = JZN_INVALID_STORED("catalogue bootstrap completion marker does not match durable catalogue"); goto out; }
    }
    {
        jazz_protocol_SchemaVersion genesis_schema;
        bool removed = jz_btree_remove(&schemas, &bootstrap_ready.genesis, NULL, &genesis_schema);
        JZ_ASSERT(removed, "validated durable genesis schema must exist");
        *schema_out = genesis_schema.schema;
        *has_schema = true;
    }
out:
    jz_str_drop(&catalogue_table);
    if (has_active_selection) jazz_node_ActiveSchema_drop(&active_selection);
    jz_btree_drop(&schemas);
    jz_btree_drop(&staged_lineages);
    jz_btree_drop(&active_lineages);
    jz_btree_drop(&mapping_ids);
    jz_btree_drop(&active_lineage_targets);
    jz_btree_drop(&active_catalogue_sequences);
    if (e.code) { groove__db__Database__drop(meta_database); return e; }
    *storage_out = groove__db__Database__into_storage(meta_database);
    return JZ_OK;
}

#ifdef JZ_FEATURE_RUNTIME
static const jz_btree_desc SCHEMAS_BY_ID_DESC_LOCAL = { sizeof(jz_uuid), sizeof(jazz_model_JazzSchema), jz_cmp_uuid, NULL, jazz_model_JazzSchema_drop };

/* Choose the schema a server runtime reopens an existing store with. Keep
 * `requested` when this store's catalogue already holds it, and on a fresh
 * store. Otherwise reopen with the store's own current schema: its active
 * selection, else its genesis. */
jz_err jazz_node__node__NodeState__select_durable_reopen_schema(jazz_node_BoxedStorage *storage, jazz_model_JazzSchema requested,
    jazz_node_BoxedStorage **storage_out, jazz_model_JazzSchema *schema_out)
{
    jazz_model_JazzSchema empty = jazz_model__schema__JazzSchema__empty();
    groove_Database *meta_database;
    jz_err e = open_meta_database(jazz_model__schema__JazzSchema__lower_catalogue_meta_to_groove(&empty), storage, &meta_database);
    jazz_model_JazzSchema_drop(&empty);
    if (e.code) { jazz_model_JazzSchema_drop(&requested); return e; }
    bool has_genesis = false, has_active = false; jz_uuid genesis = {{0}}, active = {{0}};
    jz_btree schemas; jz_btree_init(&schemas, &SCHEMAS_BY_ID_DESC_LOCAL);
    jz_str catalogue_table = jz_str_from("jazz_catalogue");
    groove_RawRowVec raws = {0};
    e = groove_err(groove__db__primary_storage__Database__primary_key_scan_raw(meta_database, &catalogue_table, NULL, 0, &raws));
    jz_str_drop(&catalogue_table);
    for (u32 i = 0; !e.code && i < raws.len; ++i) {
        u32 kind; jz_uuid id; jz_slice payload;
        if ((e = catalogue_row(raws.ptr[i], &kind, &id, &payload)).code) break;
        if (kind == JZN_CRK_GENESIS) { has_genesis = true; genesis = id; }
        else if (kind == JZN_CRK_ACTIVE_SCHEMA) {
            jazz_node_ActiveSchema selection;
            if ((e = jazz_node__node__codec__decode_active_schema(payload, &selection)).code) break;
            has_active = true; active = selection.schema;
            jazz_node_ActiveSchema_drop(&selection);
        } else if (kind == JZN_CRK_SCHEMA) {
            jazz_protocol_SchemaVersion schema;
            if ((e = jazz_node__node__codec__decode_catalogue_schema(payload, &schema)).code) break;
            jazz_model_JazzSchema old;
            if (jz_btree_insert(&schemas, &schema.id, &schema.schema, &old)) jazz_model_JazzSchema_drop(&old);
        }
    }
    groove_RawRowVec_drop(&raws);
    if (e.code) { jz_btree_drop(&schemas); groove__db__Database__drop(meta_database); jazz_model_JazzSchema_drop(&requested); return e; }
    *storage_out = groove__db__Database__into_storage(meta_database);
    jazz_types_SchemaVersionId requested_id = jazz_model__schema__JazzSchema__version_id(&requested);
    if (!has_genesis || jz_btree_contains(&schemas, &requested_id)) {
        jz_btree_drop(&schemas);
        *schema_out = requested;
        return JZ_OK;
    }
    /* The full open validates the recovered catalogue; an unusable candidate
     * here only falls through to that open's own error. */
    jazz_model_JazzSchema own;
    bool found = (has_active && jz_btree_remove(&schemas, &active, NULL, &own)) || jz_btree_remove(&schemas, &genesis, NULL, &own);
    jz_btree_drop(&schemas);
    if (found) { jazz_model_JazzSchema_drop(&requested); *schema_out = own; }
    else *schema_out = requested;
    return JZ_OK;
}
#endif

/* Open or create a node that is known to hold complete settled history. */
jz_err jazz_node__node__NodeState__new_history_complete(jazz_types_NodeUuid node_uuid, jazz_model_JazzSchema schema, jazz_node_BoxedStorage *storage, NS **out)
{
    return jazz_node__node__NodeState__new_with_history_complete(node_uuid, schema, storage, true, out);
}

/* Rebuild the groove layer over the same storage using the standard open path.
 * Consumes `self`. */
jz_err jazz_node__node__NodeState__reopen_in_place(NS *self, NS **out)
{
    jazz_types_NodeUuid node_uuid = self->node_uuid;
    u32 catalogue_bootstrap_state = self->catalogue_bootstrap_state;
    bool history_complete = self->history_complete;
    bool authoritative_scalar_exit_refresh = self->authoritative_scalar_exit_refresh;
    void *chunk_resolver = self->chunk_resolver; self->chunk_resolver = NULL;
    bool *detach_covered_chunk_waits = self->detach_covered_chunk_waits; self->detach_covered_chunk_waits = NULL;
    jazz_model_JazzSchema schema = self->catalogue.schema;
    self->catalogue.schema = jazz_model__schema__JazzSchema__empty();
    jazz_node_BoxedStorage *storage = groove__db__Database__into_storage(jazz_node__node__database_slot__DatabaseSlot__into_inner(self->database));
    self->database = NULL;
    jazz_node__node__NodeState__drop(self);
    NS *reopened;
    jz_err e;
    if (catalogue_bootstrap_state == JZN_CBS_UNINITIALIZED) {
        jazz_model_JazzSchema_drop(&schema);
        e = jazz_node__node__NodeState__new_catalogue_uninitialized(node_uuid, storage, &reopened);
    } else {
        e = jazz_node__node__NodeState__new_with_history_complete(node_uuid, schema, storage, history_complete, &reopened);
    }
    if (e.code) { groove__chunks__MissingChunkResolver__release(chunk_resolver); jz_rc_release(detach_covered_chunk_waits); return e; }
    groove__db__Database__set_missing_chunk_resolver(reopened->database, groove__chunks__MissingChunkResolver__clone(chunk_resolver));
    groove__chunks__LocalChunkReader__release(reopened->local_chunk_reader);
    reopened->local_chunk_reader = groove__db__Database__local_chunk_reader(reopened->database);
    groove__chunks__MissingChunkResolver__release(reopened->chunk_resolver);
    reopened->chunk_resolver = chunk_resolver;
    jz_rc_release(reopened->detach_covered_chunk_waits);
    reopened->detach_covered_chunk_waits = detach_covered_chunk_waits;
    groove__chunks__OwnedChunkProvider__release(reopened->content_runtime_provider);
    reopened->content_runtime_provider = groove__db__Database__owned_chunk_provider(reopened->database);
    reopened->authoritative_scalar_exit_refresh = authoritative_scalar_exit_refresh;
    *out = reopened;
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__new_with_history_complete(jazz_types_NodeUuid node_uuid, jazz_model_JazzSchema schema,
    jazz_node_BoxedStorage *storage, bool history_complete, NS **out)
{
    return jazz_node__node__NodeState__new_with_options(node_uuid, schema, storage, history_complete, out);
}

jz_err jazz_node__node__NodeState__new_with_options(jazz_types_NodeUuid node_uuid, jazz_model_JazzSchema schema,
    jazz_node_BoxedStorage *storage, bool history_complete, NS **out)
{
    return jazz_node__node__NodeState__new_with_options_inner(node_uuid, schema, storage, history_complete, JZN_CBS_READY, out);
}

static void init_query_serving(jazz_node_QueryServing *q)
{
    jz_btree_init(&q->watermark_restore_seen, &jazz_node_AUTHORITY_RESULT_KEY_SET_DESC);
    jz_btree_init(&q->persisted_watermarks, &jazz_node_NS_PERSISTED_WATERMARKS_DESC);
    q->watermarks_invalidated = false;
    jz_btree_init(&q->local_availability_records, &jazz_node_NS_LOCAL_AVAILABILITY_RECORDS_DESC);
    jz_btree_init(&q->local_availability_authorities, &jazz_node_NS_LOCAL_AVAILABILITY_AUTHORITIES_DESC);
    jz_btree_init(&q->local_unavailable_inputs, &jazz_node_NS_LOCAL_UNAVAILABLE_INPUTS_DESC);
    jz_btree_init(&q->query_shape_cache, &jazz_node_NS_QUERY_SHAPE_CACHE_DESC);
    jz_btree_init(&q->compiled_query_program_cache, &jazz_node_NS_COMPILED_QUERY_PROGRAM_CACHE_DESC);
    q->query_program_templates = jazz_engine__query_engine__QueryProgramTemplateCache__default();
    jz_deque_init(&q->supported_query_program_requests, jazz_node_SUPPORTED_QUERY_PROGRAM_SIZE);
    jz_btree_init(&q->read_policy_authorization_request_cache, &jazz_node_NS_READ_POLICY_AUTHORIZATION_REQUEST_CACHE_DESC);
    jz_btree_init(&q->policy_authorization_graph_cache, &jazz_node_NS_POLICY_AUTHORIZATION_GRAPH_CACHE_DESC);
    jz_btree_init(&q->policy_authorization_graph_replacements, &jazz_node_NS_POLICY_AUTHORIZATION_GRAPH_REPLACEMENTS_DESC);
    q->policy_proof_stack.ptr = NULL; q->policy_proof_stack.len = q->policy_proof_stack.cap = 0;
    jz_btree_init(&q->tx_versions_cache, &jazz_node_NS_TX_VERSIONS_CACHE_DESC);
    jz_deque_init(&q->tx_version_tables_cache_order, sizeof(jazz_model_TxId));
    jz_btree_init(&q->tx_version_tables_cache_order_set, &jazz_node_TX_ID_SET_DESC);
    jz_btree_init(&q->version_storage_sources_cache, &jazz_node_NS_VERSION_STORAGE_SOURCES_CACHE_DESC);
    jz_btree_init(&q->registered_shapes, &jazz_node_NS_REGISTERED_SHAPES_DESC);
    jz_btree_init(&q->registered_shape_options, &jazz_node_NS_REGISTERED_SHAPE_OPTIONS_DESC);
    jz_btree_init(&q->peer_shape_owners, &jazz_node_NS_PEER_SHAPE_OWNERS_DESC);
    jz_btree_init(&q->locally_registered_shapes, &jazz_node_UUID_SET_DESC);
    jz_btree_init(&q->outbound_shape_owners, &jazz_node_NS_OUTBOUND_SHAPE_OWNERS_DESC);
    jz_btree_init(&q->outbound_binding_owners, &jazz_node_NS_OUTBOUND_BINDING_OWNERS_DESC);
    jz_btree_init(&q->registered_bindings, &jazz_node_NS_REGISTERED_BINDINGS_DESC);
    jz_btree_init(&q->authority_results, &jazz_node_NS_AUTHORITY_RESULTS_DESC);
    jz_btree_init(&q->applied_view_update_generations, &jazz_node_AUTHORITY_RESULT_GENERATION_DESC);
    jz_btree_init(&q->retained_root_window_sources, &jazz_node_NS_RETAINED_ROOT_WINDOW_SOURCES_DESC);
}

static const jz_btree_desc STAGED_BY_SEQ_DESC_LOCAL = { sizeof(u64), sizeof(jazz_node_StagedSchemaLineage), jz_cmp_u64, NULL, jazz_node_StagedSchemaLineage_drop };

static bool any_predecessor_source_missing(const jazz_protocol_SchemaLineagePublication *p, const jz_btree *schemas)
{
    JZ_VEC_FOR(const jazz_protocol_SchemaPredecessor, pred, p->predecessors) if (!jz_btree_contains(schemas, &pred->lens.source)) return true;
    return false;
}

jz_err jazz_node__node__NodeState__new_with_options_inner(jazz_types_NodeUuid node_uuid, jazz_model_JazzSchema schema,
    jazz_node_BoxedStorage *storage, bool history_complete, u32 catalogue_bootstrap_state, NS **out)
{
    jazz_types_SchemaVersionId local_schema_version_id = jazz_model__schema__JazzSchema__version_id(&schema);
    jazz_model_JazzSchema schema_copy; jazz_model_JazzSchema_clone(&schema_copy, &schema);
    jazz_node_CatalogueOpenState open;
    jz_err e = jazz_node__node__NodeState__open_catalogue_stage(schema_copy, storage, catalogue_bootstrap_state, &open);
    if (e.code) { jazz_model_JazzSchema_drop(&schema); return e; }
    jz_btree registration_schemas, registration_aliases, registration_mappings;
    jz_btree_clone(&registration_schemas, &open.schemas, uuid_kclone, schema_version_vclone);
    jz_btree_clone(&registration_aliases, &open.schema_version_aliases, uuid_kclone, alias_vclone);
    jz_btree_clone(&registration_mappings, &open.physical_mappings, uuid_kclone, mapping_vclone);
    {
        const u64 *k; const jazz_node_StagedSchemaLineage *staged;
        JZ_BTREE_FOR(&open.staged_lineages, k, staged) {
            jazz_protocol_SchemaVersion sv; jazz_protocol_SchemaVersion_clone(&sv, &staged->publication.schema);
            jazz_protocol_SchemaVersion old_sv;
            if (jz_btree_insert(&registration_schemas, &sv.id, &sv, &old_sv)) jazz_protocol_SchemaVersion_drop(&old_sv);
            jz_btree_insert(&registration_aliases, &staged->publication.schema.id, &staged->alias, NULL);
            jazz_node_SchemaPhysicalMapping m; jazz_node_SchemaPhysicalMapping_clone(&m, &staged->mapping);
            jazz_node_SchemaPhysicalMapping old_m;
            if (jz_btree_insert(&registration_mappings, &staged->publication.schema.id, &m, &old_m)) jazz_node_SchemaPhysicalMapping_drop(&old_m);
        }
    }
    groove_Database *database;
    e = jazz_node__node__NodeState__open_full_database(&schema, &registration_schemas, &registration_aliases, &registration_mappings, open.storage, &database);
    jz_btree_drop(&registration_schemas); jz_btree_drop(&registration_aliases); jz_btree_drop(&registration_mappings);
    if (e.code) { jazz_model_JazzSchema_drop(&schema); return e; }
    for (;;) {
        u64 next = open.active_catalogue_seq == UINT64_MAX ? UINT64_MAX : open.active_catalogue_seq + 1;
        const jazz_node_StagedSchemaLineage *staged_ref = jz_btree_get(&open.staged_lineages, &next);
        if (!staged_ref) break;
        if (any_predecessor_source_missing(&staged_ref->publication, &open.schemas)) break;
        jazz_node_StagedSchemaLineage staged; jazz_node_StagedSchemaLineage_clone(&staged, staged_ref);
        groove_DatabaseBatch *batch = groove__db__facade__Database__open_batch(database);
        e = jazz_node__node__NodeState__write_active_schema_lineage_to_batch(batch, &staged);
        if (!e.code && open.catalogue_bootstrap_marker) {
            jazz_node_CatalogueBootstrapReady ready = {
                .genesis = local_schema_version_id,
                .current_write_schema = jazz_node__node__ActiveSchema__wire_pointer(&open.active_schema),
                .active_catalogue_seq = next,
            };
            jz_bytes payload;
            jazz_node__node__codec__encode_catalogue_bootstrap_ready(&ready, &payload);
            groove_ValueVec values = {0};
            JZ_VEC_PUSH(values, gv_u64(jazz_node__node__codec__CatalogueRecordKind__key(JZN_CRK_BOOTSTRAP_READY)));
            JZ_VEC_PUSH(values, gv_uuid(ready.genesis));
            JZ_VEC_PUSH(values, gv_bytes(payload));
            jz_str table = jz_str_from("jazz_catalogue");
            groove__db__batch__DatabaseBatch__update(batch, &table, values);
            jz_str_drop(&table);
        }
        groove_AppliedBatch *applied = NULL;
        if (!e.code) e = groove_err(groove__db__commit__Database__apply_batch(database, batch, &applied));
        else groove__db__batch__DatabaseBatch__drop(batch);
        if (!e.code) {
            groove_PersistedBatch *persisted = groove__db__commit__Database__persist_with_progress(database, applied);
            groove__db__commit__AppliedBatch__drop(applied);
            e = groove_err(groove__db__commit__Database__finish_persistence(database, persisted));
        }
        if (e.code) { jazz_node_StagedSchemaLineage_drop(&staged); groove__db__Database__drop(database); jazz_model_JazzSchema_drop(&schema); return e; }
        {
            jazz_protocol_SchemaVersion sv; jazz_protocol_SchemaVersion_clone(&sv, &staged.publication.schema);
            jazz_protocol_SchemaVersion old_sv;
            if (jz_btree_insert(&open.schemas, &sv.id, &sv, &old_sv)) jazz_protocol_SchemaVersion_drop(&old_sv);
        }
        JZ_VEC_FOR(const jazz_protocol_SchemaPredecessor, predecessor, staged.publication.predecessors) {
            jazz_protocol_MigrationLens lens; jazz_protocol_MigrationLens_clone(&lens, &predecessor->lens);
            jazz_protocol_MigrationLens old;
            if (jz_btree_insert(&open.lenses, &lens.id, &lens, &old)) jazz_protocol_MigrationLens_drop(&old);
        }
        jz_btree_insert(&open.schema_version_aliases, &staged.publication.schema.id, &staged.alias, NULL);
        {
            jazz_node_SchemaPhysicalMapping m; jazz_node_SchemaPhysicalMapping_clone(&m, &staged.mapping);
            jazz_node_SchemaPhysicalMapping old_m;
            if (jz_btree_insert(&open.physical_mappings, &staged.publication.schema.id, &m, &old_m)) jazz_node_SchemaPhysicalMapping_drop(&old_m);
        }
        jazz_types_SchemaVersionId target = staged.publication.schema.id;
        jazz_node_StagedSchemaLineage old_active;
        if (jz_btree_insert(&open.active_lineages_by_target, &target, &staged, &old_active)) jazz_node_StagedSchemaLineage_drop(&old_active);
        jz_btree_remove(&open.staged_lineages, &next, NULL, NULL);
        open.active_catalogue_seq = next;
    }
    const jazz_types_SchemaVersionAlias *local_alias = jz_btree_get(&open.schema_version_aliases, &local_schema_version_id);
    /* Schema identity excludes policy and physical-index metadata. On a
     * reopen, retain the durable payload previously learned from the
     * authority instead of reverting to the caller's structural schema. */
    jazz_model_JazzSchema active_schema;
    const jazz_protocol_SchemaVersion *known = jz_btree_get(&open.schemas, &local_schema_version_id);
    if (known) jazz_model_JazzSchema_clone(&active_schema, &known->schema);
    else jazz_model_JazzSchema_clone(&active_schema, &schema);
    if (jz_uuid_eq(&open.active_schema.schema, &local_schema_version_id)) {
        jazz_model_JazzSchema_drop(&active_schema);
        jazz_model_JazzSchema_clone(&active_schema, &open.active_schema.compiled);
    }
    void *chunk_resolver = groove__chunks__UnavailableChunkResolver__new();
    groove__db__Database__set_missing_chunk_resolver(database, groove__chunks__MissingChunkResolver__clone(chunk_resolver));
    void *content_runtime_provider = groove__db__Database__owned_chunk_provider(database);
    void *local_chunk_reader = groove__db__Database__local_chunk_reader(database);

    NS *node = jz_alloc(sizeof(NS));
    memset(node, 0, sizeof(NS));
    node->node_uuid = node_uuid;
    node->has_self_node_alias = false;
    jazz_node_SchemaCatalogue *c = &node->catalogue;
    c->local_schema_version_id = local_schema_version_id;
    c->has_local_schema_version_alias = local_alias != NULL;
    if (local_alias) c->local_schema_version_alias = *local_alias;
    c->schema = active_schema;
    c->schema_version_aliases = open.schema_version_aliases;
    c->catalogue_schemas = open.schemas;
    c->catalogue_lenses = open.lenses;
    c->physical_mappings = open.physical_mappings;
    jz_btree_init(&c->physical_current_winner_projections, &jazz_node_NS_PHYSICAL_CURRENT_WINNER_PROJECTIONS_DESC);
    c->staged_lineages = open.staged_lineages;
    c->pending_lineages = open.pending_lineages;
    c->active_lineages_by_target = open.active_lineages_by_target;
    c->active_catalogue_seq = open.active_catalogue_seq;
    c->next_physical_table_id = open.next_physical_table_id;
    c->next_physical_column_id = open.next_physical_column_id;
    jz_btree_init(&c->lens_path_cache, &jazz_node_LENS_PATH_CACHE_DESC);
    jz_btree_init(&c->compiled_lens_cache, &jazz_node_COMPILED_LENS_CACHE_DESC);
    jz_btree_init(&c->physical_write_plan_cache, &jazz_node_NS_PHYSICAL_WRITE_PLAN_CACHE_DESC);
    c->active_schema = open.active_schema;
    node->catalogue_bootstrap_state = catalogue_bootstrap_state;
    node->catalogue_bootstrap_marker = open.catalogue_bootstrap_marker;
    node->clock.tx_time = 0;
    node->clock.reservation_high_water = jz_rc_new(sizeof(jazz_types_TxTime), NULL);
    *node->clock.reservation_high_water = 0;
    node->clock.global_time_register = 0;
    jz_btree_init(&node->clock.locally_minted_global_times, &jazz_node_U64_SET_DESC);
    node->clock.committed_global_time = 0;
    jz_btree_init(&node->clock.applied_global_times_after_frontier, &jazz_node_U64_SET_DESC);
    jz_btree_init(&node->clock.frontier_dots, &jazz_node_NS_FRONTIER_DOTS_DESC);
    jazz_node_Parking_default(&node->parking);
    init_query_serving(&node->query);
    jz_btree_init(&node->open_tx.open_transactions, &jazz_node_NS_OPEN_TRANSACTIONS_DESC);
    jz_btree_init(&node->open_tx.closed_batches, &jazz_node_UUID_SET_DESC);
    jz_btree_init(&node->open_tx.local_permission_subjects, &jazz_node_NS_LOCAL_PERMISSION_SUBJECTS_DESC);
    node->open_tx.released_large_values.ptr = NULL; node->open_tx.released_large_values.len = node->open_tx.released_large_values.cap = 0;
    jz_btree_init(&node->open_tx.pending_foreign_transactions, &jazz_node_TX_ID_SET_DESC);
    jazz_node__node__RejectionTracking__default(&node->rejections);
    node->database = jazz_node__node__database_slot__DatabaseSlot__new(database);
    node->local_chunk_reader = local_chunk_reader;
    node->chunk_resolver = chunk_resolver;
    node->detach_covered_chunk_waits = jz_rc_new(sizeof(bool), NULL);
    *node->detach_covered_chunk_waits = false;
    node->large_value_staging_policy = jazz_node__node__LargeValueStagingPolicy__default();
    node->large_value_ingress.window_started_ms = 0; node->large_value_ingress.admitted_bytes = 0;
    node->content_runtime_provider = content_runtime_provider;
    node->groove_runtime_token = jazz_node__node__next_groove_runtime_token();
    node->physical_identity_generation = 0;
    node->next_authoritative_reset_generation = 1;
    node->history_complete = history_complete;
    node->authored_commit_durability = JZ_DURABILITY_LOCAL;
    node->authoritative_scalar_exit_refresh = false;
    jz_hmap_init(&node->client_local_literal_shapes, &jazz_node_NS_CLIENT_LOCAL_LITERAL_SHAPES_DESC);
    node->relay_authority_session_owner = NULL;
    jz_btree_init(&node->pending_persistence, &jazz_node_TX_ID_SET_DESC);
    node->node_aliases = jazz_node__node__NodeAliases__default();
    node->has_absent_node_alias = false;
    node->ahead_current_keys = jazz_node__node__AheadCurrentKeys__default();
    jz_btree_init(&node->tx_touched_dirty, &jazz_node_TX_TOUCHED_DIRTY_DESC);
    jz_btree_init(&node->history_tx_authors, &jazz_node_NS_HISTORY_TX_AUTHORS_DESC);
    jz_hmap_init(&node->history_tx_seqs, &jazz_node_NS_HISTORY_TX_SEQS_DESC);
    node->minting_global_time = false;
    memset(&node->sync_metrics, 0, sizeof(node->sync_metrics));
    node->query_engine_read_metrics = jazz_node__node__QueryEngineReadMetrics__default();
    jz_btree_init(&node->session_claims, &jazz_node_NS_SESSION_CLAIMS_DESC);
    jz_btree_init(&node->session_claim_revisions, &jazz_node_NS_SESSION_CLAIM_REVISIONS_DESC);
    node->has_active_session_claims = false;
    node->permissions_ready = true;
    node->catalogue_activation_failed = false;
    node->has_initial_sync_flush_cadence = false;
    node->initial_sync_flush_active = false;
    node->initial_sync_flush_completed = false;
    jazz_model_JazzSchema_drop(&schema);

    /* Every known schema version gets its provisional physical mapping. */
    JZ_VEC(jazz_types_SchemaVersionId) known_schema_versions = {0};
    { const jz_uuid *k; const void *v; JZ_BTREE_FOR(&node->catalogue.catalogue_schemas, k, v) JZ_VEC_PUSH(known_schema_versions, *k); }
    for (u32 i = 0; i < known_schema_versions.len && !e.code; ++i)
        e = jazz_node__node__NodeState__ensure_provisional_physical_mapping(node, known_schema_versions.ptr[i]);
    jz_free(known_schema_versions.ptr);
    if (!e.code) e = jazz_node__node__physical__NodeState__synchronize_physical_version_tables(node);
    if (!e.code) e = groove_err(groove__db__Database__ensure_declared_index_generation(node->database, 1));
    if (!e.code) e = jazz_node__node__ingest__NodeState__recover_pending_schema_lineages(node);
    if (!e.code) e = jazz_node__node__recovery__NodeState__recover_from_storage(node);
    if (!e.code) e = jazz_node__node__query_eval__local_availability_receipts__NodeState__recover_local_availability_records(node);
    if (!e.code) e = jazz_node__node__NodeState__rebuild_ahead_current_keys(node);
    jazz_types_NodeAlias self_node_alias;
    if (!e.code) e = jazz_node__node__NodeState__ensure_node_alias(node, node_uuid, &self_node_alias);
    if (!e.code) { node->has_self_node_alias = true; node->self_node_alias = self_node_alias; }
    jazz_types_SchemaVersionAlias schema_alias;
    if (!e.code) e = jazz_node__node__NodeState__ensure_schema_version_alias(node, local_schema_version_id, &schema_alias);
    if (e.code) { jazz_node__node__NodeState__drop(node); return e; }
    node->catalogue.has_local_schema_version_alias = true;
    node->catalogue.local_schema_version_alias = schema_alias;
    *out = node;
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__open_full_database(const jazz_model_JazzSchema *schema, const jz_btree *catalogue_schemas,
    const jz_btree *schema_version_aliases, const jz_btree *physical_mappings, jazz_node_BoxedStorage *storage, groove_Database **out)
{
    jazz_node__node__codec__debug_assert_lowered_layouts(schema);
    groove_DbSchema lowered = jazz_model__schema__JazzSchema__lower_to_groove(schema);
    groove_TableSchemaVec current_tables;
    jz_err e = jazz_node__node__physical__physical_version_storage_tables(catalogue_schemas, schema_version_aliases, physical_mappings, &current_tables);
    if (e.code) { groove_DbSchema_drop(&lowered); return e; }
    groove__db__DbSchema__extend_tables(&lowered, current_tables);
    groove_Database *database;
    JZ_TRY(groove_err(groove__db__Database__new_with_storage_layout(lowered, storage, groove__db__StorageLayout__jazz_class_v2(), &database)));
    /* Jazz publishes plain ordered results from membership and version deltas
     * and never reads their generic root positions; collecting them would
     * make every write to an ordered subscription proportional to its result
     * size (#2086). */
    groove__db__Database__set_plain_output_root_positions_enabled(database, false);
    *out = database;
    return JZ_OK;
}

/* ===================== accessors ===================== */

jazz_types_GlobalTime jazz_node__node__NodeState__committed_global_time(const NS *self) { return self->clock.committed_global_time; }

/* Stable identity of the authority issuing wire-level receipts. */
jazz_types_NodeUuid jazz_node__node__NodeState__node_uuid(const NS *self) { return self->node_uuid; }

/* Highest HLC value this runtime has observed or minted, including
 * identities allocated for transactions later rolled back. */
jazz_types_TxTime jazz_node__node__NodeState__tx_time_high_water(const NS *self)
{
    jazz_types_TxTime reserved = *self->clock.reservation_high_water;
    return self->clock.tx_time > reserved ? self->clock.tx_time : reserved;
}

jazz_types_TxTime *jazz_node__node__NodeState__tx_time_reservation_clock(const NS *self)
{
    return jz_rc_clone(self->clock.reservation_high_water);
}

/* Establish a durable-owner supplied lower bound before this runtime can mint
 * its first transaction identity. */
void jazz_node__node__NodeState__seed_tx_time_high_water(NS *self, jazz_types_TxTime high_water)
{
    jazz_node__node__NodeState__merge_tx_time(self, high_water);
}

jazz_model_DurabilityTier jazz_node__node__NodeState__authored_commit_durability(const NS *self) { return self->authored_commit_durability; }

void jazz_node__node__NodeState__set_non_durable_client(NS *self) { self->authored_commit_durability = JZ_DURABILITY_NONE; }

#ifdef JZ_FEATURE_RUNTIME
/* Enable only for a host that owns complete current policy inputs. */
void jazz_node__node__NodeState__enable_authoritative_scalar_exit_refresh(NS *self)
{
    if (!jazz_node__node__NodeState__client_relay_scope(self)) self->authoritative_scalar_exit_refresh = true;
}
#endif

/* Mark this process as the durable half of a browser client/worker relay. */
jz_err jazz_node__node__NodeState__configure_scope_isolated_client_relay(NS *self, jazz_node_ClientRelayScope *scope)
{
    if (self->relay_authority_session_owner
        && !jazz_node__node__relay_scope__ClientRelayScope__same_owner(self->relay_authority_session_owner, scope)) {
        jazz_node__node__relay_scope__ClientRelayScope__drop(scope);
        return jazz_node__node__Error_invalid_branch_key_str("a relay cannot be rebound to a different storage ownership scope");
    }
    self->authoritative_scalar_exit_refresh = false;
    if (self->relay_authority_session_owner) jazz_node__node__relay_scope__ClientRelayScope__drop(self->relay_authority_session_owner);
    self->relay_authority_session_owner = scope;
    return JZ_OK;
}

/* The immutable host-admitted scope carried by this relay. */
const jazz_node_ClientRelayScope *jazz_node__node__NodeState__client_relay_scope(const NS *self)
{
    return self->relay_authority_session_owner;
}

/* ===================== session claims ===================== */

static bool value_eq_v(const void *a, const void *b) { return groove_Value_eq(a, b); }

/* Attach process-local auth claims to an accepted subscriber identity. */
void jazz_node__node__NodeState__set_session_claims(NS *self, jazz_types_AuthorSubject identity, jz_btree claims)
{
    const jz_btree *existing = jz_btree_get(&self->session_claims, &identity);
    if (existing && jz_btree_eq(existing, &claims, value_eq_v)) { jz_btree_drop(&claims); return; }
    jz_btree old;
    if (jz_btree_insert(&self->session_claims, &identity, &claims, &old)) jz_btree_drop(&old);
    bool inserted;
    u64 *revision = jz_btree_entry(&self->session_claim_revisions, &identity, &inserted);
    if (inserted) *revision = 0;
    JZ_ASSERT(*revision != UINT64_MAX, "session claim revision overflow must stop authorization delivery");
    *revision += 1;
    jz_btree_clear(&self->query.read_policy_authorization_request_cache);
    jz_btree_clear(&self->query.policy_authorization_graph_cache);
    jz_btree_clear(&self->query.policy_authorization_graph_replacements);
}

/* Scope one node-locked operation to this subscriber's admitted claims.
 * Dropping the returned guard restores the prior scope. */
jazz_node_ActiveSessionClaimsScope jazz_node__node__NodeState__scoped_active_session_claims(NS *self, jazz_types_AuthorSubject identity, jz_btree claims)
{
    jazz_node_ActiveSessionClaimsScope scope = { .node = self, .has_previous = self->has_active_session_claims };
    if (scope.has_previous) { scope.previous_identity = self->active_session_identity; scope.previous_claims = self->active_session_claims; }
    self->has_active_session_claims = true;
    self->active_session_identity = identity;
    self->active_session_claims = claims;
    return scope;
}

jazz_node_ActiveSessionClaimsScope jazz_node__node__NodeState__scoped_optional_session_claims(NS *self, jazz_types_AuthorSubject identity,
    bool has_claims, jz_btree claims)
{
    jazz_node_ActiveSessionClaimsScope scope = { .node = self, .has_previous = self->has_active_session_claims };
    if (scope.has_previous) {
        scope.previous_identity = self->active_session_identity;
        jz_btree_clone(&scope.previous_claims, &self->active_session_claims, str_kclone, value_vclone);
    }
    if (has_claims) {
        if (self->has_active_session_claims) jz_btree_drop(&self->active_session_claims);
        self->has_active_session_claims = true;
        self->active_session_identity = identity;
        self->active_session_claims = claims;
    }
    return scope;
}

/* Return an opaque, domain-separated identity for the active session scope.
 * It is runtime-only and deliberately never formats raw claims. */
bool jazz_node__node__NodeState__active_session_claim_scope_key(const NS *self, jazz_types_AuthorSubject identity, jz_str *out)
{
    if (!self->has_active_session_claims || !jazz_types_AuthorSubject_eq(self->active_session_identity, identity)) return false;
    jz_bytes encoded = {0};
    jz_pc_w w = { &encoded };
    /* postcard::to_allocvec(&(identity, claims)) */
    jazz_types_AuthorSubject_pc_encode(&w, &identity);
    jz_pc_seq(&w, self->active_session_claims.len);
    const jz_str *name; const groove_Value *value;
    JZ_BTREE_FOR(&self->active_session_claims, name, value) { jz_pc_str(&w, name); groove_Value_pc_encode(&w, value); }
    jz_blake3_hasher *hasher = jz_blake3_new();
    static const char domain[] = "jazz/active-session-claim-scope/v1\\0";
    jz_blake3_update(hasher, (const u8 *)domain, sizeof(domain) - 1);
    jz_blake3_update(hasher, encoded.ptr, encoded.len);
    u8 digest[32];
    jz_blake3_finalize(hasher, digest);
    jz_blake3_free(hasher);
    jz_bytes_drop(&encoded);
    *out = (jz_str){0};
    jz_hex_encode(digest, 32, out);
    return true;
}

/* Return the revision of process-local claims for `identity`. */
u64 jazz_node__node__NodeState__session_claim_revision(const NS *self, jazz_types_AuthorSubject identity)
{
    const u64 *revision = jz_btree_get(&self->session_claim_revisions, &identity);
    return revision ? *revision : 0;
}

/* Return the current process-local claims together with their revisions. */
jazz_node_SessionClaimsRevisionVec jazz_node__node__NodeState__session_claims_with_revisions(const NS *self)
{
    jazz_node_SessionClaimsRevisionVec out = {0};
    const jazz_types_AuthorSubject *identity; const jz_btree *claims;
    JZ_BTREE_FOR(&self->session_claims, identity, claims) {
        jazz_node_SessionClaimsRevision entry = { .identity = *identity, .revision = jazz_node__node__NodeState__session_claim_revision(self, *identity) };
        jz_btree_clone(&entry.claims, claims, str_kclone, value_vclone);
        JZ_VEC_PUSH(out, entry);
    }
    return out;
}

/* Return the admitted claims for one directly terminated session. */
void jazz_node__node__NodeState__session_claims_for(const NS *self, jazz_types_AuthorSubject identity, jz_btree *out)
{
    const jz_btree *claims = jz_btree_get(&self->session_claims, &identity);
    if (claims) jz_btree_clone(out, claims, str_kclone, value_vclone);
    else jz_btree_init(out, &jazz_node_STRING_VALUE_MAP_DESC);
}

/* Gate session-scoped serving until an authority has installed its
 * permissions head. Local/offline nodes stay ready by default. */
void jazz_node__node__NodeState__set_permissions_ready(NS *self, bool ready) { self->permissions_ready = ready; }
bool jazz_node__node__NodeState__permissions_ready(const NS *self) { return self->permissions_ready; }

/* ===================== chunks ===================== */

/* Replace the policy-blind immutable content backend used by Groove. */
void jazz_node__node__NodeState__set_chunk_storage(NS *self, void *storage /* Rc<dyn ChunkStorage> */)
{
    groove__db__Database__set_chunk_storage(self->database, storage);
    groove__db__Database__set_missing_chunk_resolver(self->database, groove__chunks__MissingChunkResolver__clone(self->chunk_resolver));
    void *runtime_provider = groove__db__Database__owned_chunk_provider(self->database);
    void *reader = groove__db__Database__local_chunk_reader(self->database);
    groove__chunks__LocalChunkReader__refresh_from(self->local_chunk_reader, reader);
    groove__chunks__LocalChunkReader__release(reader);
    groove__chunks__OwnedChunkProvider__release(self->content_runtime_provider);
    self->content_runtime_provider = runtime_provider;
}

/* The flag `Node` flips when its host drops pending ticks. */
bool *jazz_node__node__NodeState__detach_covered_chunk_waits_handle(const NS *self) { return jz_rc_clone(self->detach_covered_chunk_waits); }

/* Whether covered receiver installs detach chunk-waiting evaluation. */
bool jazz_node__node__NodeState__detaches_covered_chunk_waits(const NS *self) { return *self->detach_covered_chunk_waits; }

/* Install Jazz's sync-plane fallback for chunks absent from local storage. */
void jazz_node__node__NodeState__set_missing_chunk_resolver(NS *self, void *resolver)
{
    groove__db__Database__set_missing_chunk_resolver(self->database, groove__chunks__MissingChunkResolver__clone(resolver));
    void *runtime_provider = groove__db__Database__owned_chunk_provider(self->database);
    groove__chunks__MissingChunkResolver__release(self->chunk_resolver);
    self->chunk_resolver = resolver;
    groove__chunks__OwnedChunkProvider__release(self->content_runtime_provider);
    self->content_runtime_provider = runtime_provider;
}

/* Consult Groove's local immutable storage without invoking the peer fallback. */
jz_err jazz_node__node__NodeState__local_chunk(const NS *self, groove_Locator locator, groove_ContentHash expected_hash, jz_bytes *out)
{
    return groove__chunks__LocalChunkReader__get(self->local_chunk_reader, locator, expected_hash, out);
}

void *jazz_node__node__NodeState__local_chunk_reader_handle(const NS *self) { return groove__chunks__LocalChunkReader__clone(self->local_chunk_reader); }

/* ===================== large values ===================== */

void jazz_node__node__NodeState__set_large_value_staging_policy(NS *self, jazz_node_LargeValueStagingPolicy policy) { self->large_value_staging_policy = policy; }

jz_err jazz_node__node__NodeState__enforce_large_value_staging_policy(NS *self, const groove_StagedLargeValue *newest)
{
    if (!jazz_node__node__NodeState__admit_large_value_ingress(self, newest->accounting.encoded_bytes)) {
        bool evicted;
        JZ_TRY(groove_err(groove__db__Database__evict_staged_large_value(self->database, newest->id, &evicted)));
        return JZN_LARGE_VALUE_INGRESS_RATE_LIMITED();
    }
    return JZ_OK;
}

bool jazz_node__node__NodeState__admit_large_value_ingress(NS *self, u64 encoded_bytes)
{
    u64 now_ms = jz_now_ms();
    u64 window_ms = self->large_value_staging_policy.window_ms > 1 ? self->large_value_staging_policy.window_ms : 1;
    if (self->large_value_ingress.window_started_ms == 0
        || (now_ms > self->large_value_ingress.window_started_ms ? now_ms - self->large_value_ingress.window_started_ms : 0) >= window_ms) {
        self->large_value_ingress.window_started_ms = now_ms;
        self->large_value_ingress.admitted_bytes = 0;
    }
    u64 admitted = self->large_value_ingress.admitted_bytes;
    u64 next = admitted + encoded_bytes < admitted ? UINT64_MAX : admitted + encoded_bytes;
    if (next > self->large_value_staging_policy.incoming_bytes_per_window) return false;
    self->large_value_ingress.admitted_bytes = next;
    return true;
}

jz_err jazz_node__node__NodeState__ensure_large_value_stages_current(NS *self, const jz_btree *ids)
{
    if (ids->len == 0) return JZ_OK;
    groove_StagedLargeValueVec staged_list;
    JZ_TRY(groove_err(groove__db__Database__staged_large_values(self->database, &staged_list)));
    jz_btree staged; jz_btree_init(&staged, &jazz_node_U64_SET_DESC);
    JZ_VEC_FOR(const groove_StagedLargeValue, s, staged_list) jz_btree_insert(&staged, &s->id, NULL, NULL);
    groove_StagedLargeValueVec_drop(&staged_list);
    jz_err e = JZ_OK;
    const groove_StagedLargeValueId *id; const void *unit;
    JZ_BTREE_FOR(ids, id, unit) if (!jz_btree_contains(&staged, id)) { e = JZN_LARGE_VALUE_STAGE_EXPIRED(); break; }
    jz_btree_drop(&staged);
    return e;
}

jz_err jazz_node__node__NodeState__current_staged_ids_for_descriptors(NS *self, const groove_LargeValueRefVec *descriptors,
    bool require_every_descriptor, groove_StagedLargeValueIdVec *out)
{
    *out = (groove_StagedLargeValueIdVec){0};
    if (descriptors->len == 0) return JZ_OK;
    groove_StagedLargeValueVec staged;
    JZ_TRY(groove_err(groove__db__Database__staged_large_values(self->database, &staged)));
    jz_err e = JZ_OK;
    JZ_VEC_FOR(groove_LargeValueRef *const, descriptor, *descriptors) {
        const groove_StagedLargeValue *receipt = NULL;
        JZ_VEC_FOR(const groove_StagedLargeValue, r, staged) if (groove_LargeValueRef_eq(r->value_ref, *descriptor)) { receipt = r; break; }
        if (receipt) JZ_VEC_PUSH(*out, receipt->id);
        else if (require_every_descriptor) { e = JZN_LARGE_VALUE_STAGE_EXPIRED(); break; }
    }
    groove_StagedLargeValueVec_drop(&staged);
    if (e.code) { jz_free(out->ptr); *out = (groove_StagedLargeValueIdVec){0}; }
    return e;
}

jz_err jazz_node__node__NodeState__require_staged_large_values_for_versions(NS *self, const jazz_protocol_VersionRecordVec *versions)
{
    groove_LargeValueRefVec descriptors = jazz_node__node__version_indirect_descriptors(versions);
    groove_StagedLargeValueIdVec ids;
    jz_err e = jazz_node__node__NodeState__current_staged_ids_for_descriptors(self, &descriptors, true, &ids);
    groove_LargeValueRefVec_drop(&descriptors);
    if (!e.code) jz_free(ids.ptr);
    return e;
}

/* Evict unpublished Groove staging roots older than the configured policy. */
jz_err jazz_node__node__NodeState__evict_expired_staged_large_values(NS *self, usize *out)
{
    u64 max_age_ms = self->large_value_staging_policy.max_age_ms;
    u64 now_ms = jz_now_ms();
    usize evicted = 0;
    groove_StagedLargeValueVec staged;
    JZ_TRY(groove_err(groove__db__Database__staged_large_values(self->database, &staged)));
    jz_err e = JZ_OK;
    JZ_VEC_FOR(const groove_StagedLargeValue, s, staged) {
        u64 age = now_ms > s->created_at_ms ? now_ms - s->created_at_ms : 0;
        if (age <= max_age_ms) continue;
        bool removed;
        if ((e = groove_err(groove__db__Database__evict_staged_large_value(self->database, s->id, &removed))).code) break;
        if (removed) ++evicted;
    }
    groove_StagedLargeValueVec_drop(&staged);
    if (e.code) return e;
    groove_PendingLargeValueUploadVec uploads;
    JZ_TRY(groove_err(groove__db__Database__pending_large_value_uploads(self->database, &uploads)));
    JZ_VEC_FOR(const groove_PendingLargeValueUpload, upload, uploads) {
        u64 age = now_ms > upload->created_at_ms ? now_ms - upload->created_at_ms : 0;
        if (age <= max_age_ms) continue;
        bool removed;
        if ((e = groove_err(groove__db__Database__evict_pending_large_value_upload(self->database, upload->id, &removed))).code) break;
        if (removed) ++evicted;
    }
    jz_free(uploads.ptr);
    if (e.code) return e;
    *out = evicted;
    return JZ_OK;
}

/* Ids of every unpublished Groove staging root, for leak assertions. */
jz_err jazz_node__node__NodeState__staged_large_value_ids(NS *self, groove_StagedLargeValueIdVec *out)
{
    groove_StagedLargeValueVec staged;
    JZ_TRY(groove_err(groove__db__Database__staged_large_values(self->database, &staged)));
    *out = (groove_StagedLargeValueIdVec){0};
    JZ_VEC_FOR(const groove_StagedLargeValue, s, staged) JZ_VEC_PUSH(*out, s->id);
    groove_StagedLargeValueVec_drop(&staged);
    return JZ_OK;
}

/* Evict an opaque Groove staging root selected by Jazz policy. */
jz_err jazz_node__node__NodeState__evict_staged_large_value(NS *self, groove_StagedLargeValueId id, bool *out)
{
    return groove_err(groove__db__Database__evict_staged_large_value(self->database, id, out));
}

jz_err jazz_node__node__NodeState__stage_large_value_chunk_batch(NS *self, groove_StagedLargeValueId upload_id, u32 kind, groove_StagedChunkVec chunks)
{
    u64 encoded_bytes = 0;
    JZ_VEC_FOR(const groove_StagedChunk, chunk, chunks) {
        u64 len = chunk->encoded.len;
        if (encoded_bytes + len < encoded_bytes) { groove_StagedChunkVec_drop(&chunks); return JZN_INVALID_STORED("large-value chunk batch accounting overflow"); }
        encoded_bytes += len;
    }
    if (!jazz_node__node__NodeState__admit_large_value_ingress(self, encoded_bytes)) {
        groove_StagedChunkVec_drop(&chunks);
        bool evicted;
        JZ_TRY(groove_err(groove__db__Database__evict_staged_large_value(self->database, upload_id, &evicted)));
        return JZN_LARGE_VALUE_INGRESS_RATE_LIMITED();
    }
    bool staged;
    JZ_TRY(groove_err(groove__db__Database__stage_large_value_chunk_batch_if_current(self->database, upload_id, kind, chunks, &staged)));
    if (!staged) return JZN_LARGE_VALUE_STAGE_EXPIRED();
    return JZ_OK;
}

/* Establish the one pending journal that later local stream operations must
 * continue. Only this initialization path may create it. */
jz_err jazz_node__node__NodeState__begin_streaming_large_value_upload(NS *self, groove_StagedLargeValueId upload_id, u32 kind)
{
    return groove_err(groove__db__Database__stage_large_value_chunk_batch(self->database, upload_id, kind, (groove_StagedChunkVec){0}));
}

jz_err jazz_node__node__NodeState__evict_pending_large_value_upload(NS *self, groove_StagedLargeValueId upload_id)
{
    bool removed;
    return groove_err(groove__db__Database__evict_pending_large_value_upload(self->database, upload_id, &removed));
}

jz_err jazz_node__node__NodeState__finalize_large_value_upload(NS *self, groove_StagedLargeValueId upload_id, groove_LargeValueRef *value_ref,
    groove_StagedLargeValue *out)
{
    /* Presence is the semantic boundary: maintenance may evict an old
     * journal, but wall-clock age alone cannot reject an active upload. */
    bool some;
    JZ_TRY(groove_err(groove__db__Database__finalize_large_value_upload_if_current(self->database, upload_id, value_ref, &some, out)));
    if (!some) return JZN_LARGE_VALUE_STAGE_EXPIRED();
    return JZ_OK;
}

/* Consolidate through Groove-owned storage. */
jz_err jazz_node__node__NodeState__consolidate_and_stage_large_value(NS *self, groove_LargeValueRef *value, groove_StagedLargeValue *out)
{
    JZ_TRY(groove_err(groove__db__Database__consolidate_and_stage_large_value(self->database, value, out)));
    jz_err e = jazz_node__node__NodeState__enforce_large_value_staging_policy(self, out);
    if (e.code) groove_LargeValueRef_drop(out->value_ref);
    return e;
}

/* Prepare and stage a logical append through Groove's bounded tail. */
jz_err jazz_node__node__NodeState__append_and_stage_large_value(NS *self, groove_LargeValueRef *value, jz_bytes bytes, groove_StagedLargeValue *out)
{
    JZ_TRY(groove_err(groove__db__Database__append_and_stage_large_value(self->database, value, bytes, out)));
    jz_err e = jazz_node__node__NodeState__enforce_large_value_staging_policy(self, out);
    if (e.code) groove_LargeValueRef_drop(out->value_ref);
    return e;
}

/* Prepare and stage a logical byte-coordinate splice through Groove. */
jz_err jazz_node__node__NodeState__edit_and_stage_large_value(NS *self, groove_LargeValueRef *value, u64 offset, u64 delete_length,
    jz_bytes insert_bytes, groove_StagedLargeValue *out)
{
    JZ_TRY(groove_err(groove__db__Database__edit_and_stage_large_value(self->database, value, offset, delete_length, insert_bytes, out)));
    jz_err e = jazz_node__node__NodeState__enforce_large_value_staging_policy(self, out);
    if (e.code) groove_LargeValueRef_drop(out->value_ref);
    return e;
}

jz_err jazz_node__node__NodeState__read_large_value_range(const NS *self, const groove_LargeValueRef *value, u64 start, u64 end, jz_bytes *out)
{
    return groove_err(groove__db__Database__read_large_value_range(self->database, value, start, end, out));
}

jz_err jazz_node__node__NodeState__read_large_text_utf16_range(const NS *self, const groove_LargeValueRef *value, u64 start, u64 end, jz_str *out)
{
    return groove_err(groove__db__Database__read_large_text_utf16_range(self->database, value, start, end, out));
}

/* Keep UTF-16-to-byte lowering inside Groove. */
jz_err jazz_node__node__NodeState__large_text_utf16_offset_to_byte(const NS *self, const groove_LargeValueRef *value, u64 offset, u64 *out)
{
    return groove_err(groove__db__Database__large_text_utf16_offset_to_byte(self->database, value, offset, out));
}

jz_err jazz_node__node__NodeState__read_large_json_pointer(const NS *self, const groove_LargeValueRef *value, const jz_str *pointer,
    bool *some, jz_json *out)
{
    return groove_err(groove__db__Database__read_large_json_pointer(self->database, value, pointer, some, out));
}

static jz_err materialize_large_value(const NS *self, const groove_LargeValueRef *value_ref, groove_Value *out);

/* Materialize physical indirect scalar arms before returning cells across a
 * public transaction read boundary. */
jz_err jazz_node__node__NodeState__hydrate_large_value_cells(const NS *self, jz_btree *cells)
{
    /* Transaction cells are application scalars today; binding records use
     * the descriptor-directed walker below instead. */
    const jz_str *k; groove_Value *value;
    JZ_BTREE_FOR(cells, k, value) {
        groove_Value *target = value->tag == GV_NULLABLE && value->v.nullable ? value->v.nullable : value;
        if (target->tag != GV_LARGE) continue;
        groove_Value hydrated;
        JZ_TRY(materialize_large_value(self, target->v.large, &hydrated));
        groove_Value_drop(target);
        *target = hydrated;
    }
    return JZ_OK;
}

static jz_err hydrate_record_values(const NS *self, groove_ValueVec *values, const groove_RecordDescriptor *descriptor);

jz_err jazz_node__node__NodeState__hydrate_current_rows(const NS *self, jazz_node_CurrentRow *rows, u32 n)
{
    for (u32 i = 0; i < n; ++i) {
        jazz_node_CurrentRow *row = &rows[i];
        const groove_RecordDescriptor *descriptor = row->record->descriptor;
        groove_BorrowedRecord borrowed = groove__records__record__OwnedRecord__borrowed(row->record);
        groove_ValueVec values;
        JZ_TRY(record_err(groove__records__record__BorrowedRecord__to_values(&borrowed, &values)));
        jz_err e = hydrate_record_values(self, &values, descriptor);
        jz_bytes encoded;
        if (!e.code) e = record_err(groove__records__descriptor__RecordDescriptor__create(descriptor, values.ptr, values.len, &encoded));
        groove_ValueVec_drop(&values);
        if (e.code) return e;
        groove_OwnedRecord *record = groove__records__OwnedRecord__new_arc(encoded, descriptor);
        groove_OwnedRecord_arc_release(row->record);
        row->record = record;
    }
    return JZ_OK;
}

/* Materialize physical indirect scalars in one encoded record before a
 * language binding exposes that record outside Jazz. */
jz_err jazz_node__node__NodeState__hydrate_encoded_record(const NS *self, const groove_RecordDescriptor *descriptor, jz_bytes *raw)
{
    groove_BorrowedRecord bound = groove__records__RecordDescriptor__bind(descriptor, jz_bytes_slice(raw));
    groove_ValueVec values;
    JZ_TRY(record_err(groove__records__record__BorrowedRecord__to_values(&bound, &values)));
    jz_err e = hydrate_record_values(self, &values, descriptor);
    jz_bytes encoded;
    if (!e.code) e = record_err(groove__records__descriptor__RecordDescriptor__create(descriptor, values.ptr, values.len, &encoded));
    groove_ValueVec_drop(&values);
    if (e.code) return e;
    jz_bytes_drop(raw);
    *raw = encoded;
    return JZ_OK;
}

static jz_err hydrate_value_for_binding(const NS *self, groove_Value *value, const groove_ValueType *value_type);

/* Hydrate every physical indirect scalar in a descriptor-owned record. Values
 * are decoded into a temporary tree and re-encoded only after the full walk
 * succeeds, so a retryable missing chunk leaves the event byte-identical. */
static jz_err hydrate_record_values(const NS *self, groove_ValueVec *values, const groove_RecordDescriptor *descriptor)
{
    u32 n = groove__records__descriptor__RecordDescriptor__field_count(descriptor);
    if (values->len != n) return JZN_INVALID_STORED("binding record has a descriptor/value arity mismatch");
    for (u32 i = 0; i < n; ++i)
        JZ_TRY(hydrate_value_for_binding(self, &values->ptr[i], groove__records__RecordDescriptor__field_type(descriptor, i)));
    return JZ_OK;
}

static jz_err replace_with_materialized(const NS *self, groove_Value *value)
{
    groove_Value hydrated;
    JZ_TRY(materialize_large_value(self, value->v.large, &hydrated));
    groove_Value_drop(value);
    *value = hydrated;
    return JZ_OK;
}

static jz_err rebuild_owned_record(const NS *self, groove_OwnedRecord *record, const groove_RecordDescriptor *descriptor)
{
    groove_BorrowedRecord borrowed = groove__records__record__OwnedRecord__borrowed(record);
    groove_ValueVec values;
    JZ_TRY(record_err(groove__records__record__BorrowedRecord__to_values(&borrowed, &values)));
    jz_err e = hydrate_record_values(self, &values, descriptor);
    jz_bytes encoded;
    if (!e.code) e = record_err(groove__records__descriptor__RecordDescriptor__create(descriptor, values.ptr, values.len, &encoded));
    groove_ValueVec_drop(&values);
    if (e.code) return e;
    jz_bytes_drop(&record->bytes);
    record->bytes = encoded;
    record->descriptor = (groove_RecordDescriptor *)descriptor;
    return JZ_OK;
}

static jz_err hydrate_value_for_binding(const NS *self, groove_Value *value, const groove_ValueType *value_type)
{
    switch (value_type->tag) {
    case GROOVE_VT_INTERNAL: {
        /* JSON remains string-shaped to callers, but query collectors retain
         * its distinct storage codec through the binding boundary. */
        groove_ValueType json = groove__large_values__physical_storage_value_type(GROOVE_LVK_JSON);
        bool is_json = groove_ValueType_eq(value_type, &json);
        groove_ValueType_drop(&json);
        if (!is_json) break;
        if (value->tag == GV_STRING) return JZ_OK;
        if (value->tag == GV_LARGE && groove__large_values__LargeValueRef__kind(value->v.large) == GROOVE_LVK_JSON)
            return replace_with_materialized(self, value);
        return JZN_INVALID_STORED("binding JSON value does not match its descriptor");
    }
    case GROOVE_VT_STRING:
        if (value->tag == GV_STRING) return JZ_OK;
        if (value->tag == GV_LARGE) {
            u32 kind = groove__large_values__LargeValueRef__kind(value->v.large);
            if (kind == GROOVE_LVK_STRING || kind == GROOVE_LVK_JSON) return replace_with_materialized(self, value);
        }
        return JZN_INVALID_STORED("binding string value does not match its descriptor");
    case GROOVE_VT_BYTES:
        if (value->tag == GV_BYTES) return JZ_OK;
        if (value->tag == GV_LARGE && groove__large_values__LargeValueRef__kind(value->v.large) == GROOVE_LVK_BYTES)
            return replace_with_materialized(self, value);
        return JZN_INVALID_STORED("binding bytes value does not match its descriptor");
    case GROOVE_VT_NULLABLE:
        if (value->tag != GV_NULLABLE) return JZN_INVALID_STORED("binding nullable value does not match its descriptor");
        if (!value->v.nullable) return JZ_OK;
        return hydrate_value_for_binding(self, value->v.nullable, groove__records__ValueType__inner(value_type));
    case GROOVE_VT_ARRAY: {
        if (value->tag != GV_ARRAY) return JZN_INVALID_STORED("binding array value does not match its descriptor");
        const groove_ValueType *element_type = groove__records__ValueType__inner(value_type);
        JZ_VEC_FOR(groove_Value, element, value->v.items) JZ_TRY(hydrate_value_for_binding(self, element, element_type));
        return JZ_OK;
    }
    case GROOVE_VT_TUPLE: {
        u32 n;
        const groove_ValueType *member_types = groove__records__ValueType__members(value_type, &n);
        if (value->tag != GV_TUPLE || value->v.items.len != n) return JZN_INVALID_STORED("binding tuple value does not match its descriptor");
        for (u32 i = 0; i < n; ++i) JZ_TRY(hydrate_value_for_binding(self, &value->v.items.ptr[i], &member_types[i]));
        return JZ_OK;
    }
    case GROOVE_VT_RECORD: {
        const groove_RecordDescriptor *descriptor = groove__records__ValueType__record_descriptor(value_type);
        groove_OwnedRecord *record = value->v.record;
        if (value->tag != GV_RECORD || !groove_RecordDescriptor_eq(record->descriptor, descriptor))
            return JZN_INVALID_STORED("binding nested record does not match its descriptor");
        return rebuild_owned_record(self, record, descriptor);
    }
    case GROOVE_VT_ENUM: {
        if (value->tag != GV_ENUM) return JZN_INVALID_STORED("binding enum value does not match its descriptor");
        u32 tag = value->v.enum_.tag;
        const groove_RecordDescriptor *payload;
        JZ_TRY(record_err(groove__records__EnumSchema__case_payload(groove__records__ValueType__enum_schema(value_type), tag, &payload)));
        groove_OwnedRecord *record = value->v.enum_.record;
        if (!groove_RecordDescriptor_eq(record->descriptor, payload))
            return JZN_INVALID_STORED("binding enum payload does not match its descriptor");
        return rebuild_owned_record(self, record, payload);
    }
    case GROOVE_VT_U8: if (value->tag == GV_U8) return JZ_OK; break;
    case GROOVE_VT_U16: if (value->tag == GV_U16) return JZ_OK; break;
    case GROOVE_VT_U32: if (value->tag == GV_U32) return JZ_OK; break;
    case GROOVE_VT_U64: if (value->tag == GV_U64) return JZ_OK; break;
    case GROOVE_VT_I32: if (value->tag == GV_I32) return JZ_OK; break;
    case GROOVE_VT_I64: if (value->tag == GV_I64) return JZ_OK; break;
    case GROOVE_VT_BOOL: if (value->tag == GV_BOOL) return JZ_OK; break;
    case GROOVE_VT_UUID: if (value->tag == GV_UUID) return JZ_OK; break;
    case GROOVE_VT_F64:
        if (value->tag == GV_F64 && value->v.f64_ == value->v.f64_) return JZ_OK;
        return JZN_INVALID_STORED("binding float value does not match its descriptor");
    case GROOVE_VT_ENUM_TAG: {
        const void *schema = groove__records__ValueType__enum_tag_schema(value_type);
        if (value->tag == GV_ENUM_TAG) return record_err(groove__records__EnumTagSchema__variant(schema, value->v.enum_tag));
        if (value->tag == GV_STRING) return record_err(groove__records__EnumTagSchema__discriminant(schema, &value->v.s));
        return JZN_INVALID_STORED("binding enum tag value does not match its descriptor");
    }
    default: break;
    }
    return JZN_INVALID_STORED("binding value does not match its descriptor");
}

static jz_err materialize_large_value(const NS *self, const groove_LargeValueRef *value_ref, groove_Value *out)
{
    jz_bytes bytes;
    JZ_TRY(groove_err(groove__db__Database__read_large_value_range(self->database, value_ref, 0,
        groove__large_values__LargeValueRef__byte_length(value_ref), &bytes)));
    switch (groove__large_values__LargeValueRef__kind(value_ref)) {
    case GROOVE_LVK_BYTES: *out = gv_bytes(bytes); return JZ_OK;
    default:
        if (!jz_utf8_valid(bytes.ptr, bytes.len)) { jz_bytes_drop(&bytes); return JZN_INVALID_STORED("large text is not valid UTF-8"); }
        *out = gv_str((jz_str){ (char *)bytes.ptr, bytes.len, bytes.cap });
        return JZ_OK;
    }
}

/* ===================== database rebuild ===================== */

jz_err jazz_node__node__NodeState__rebuild_database_slot(NS *self)
{
    /* Reopening the database refreshes Groove's physical table catalogue.
     * Parking is in-memory delivery state, not derivable from storage, so a
     * live refresh must retain it for the caller to drain afterwards. */
    jazz_node_Parking parking; jazz_node_Parking_clone(&parking, &self->parking);
    jz_err e = groove_err(groove__db__Database__prepare_for_storage_extraction(self->database));
    if (e.code) { jazz_node_Parking_drop(&parking); return e; }
    groove_DbSchema lowered = jazz_model__schema__JazzSchema__lower_to_groove(&self->catalogue.schema);
    groove_TableSchemaVec tables;
    e = jazz_node__node__physical__physical_version_storage_tables(&self->catalogue.catalogue_schemas, &self->catalogue.schema_version_aliases,
        &self->catalogue.physical_mappings, &tables);
    if (e.code) { groove_DbSchema_drop(&lowered); jazz_node_Parking_drop(&parking); return e; }
    groove__db__DbSchema__extend_tables(&lowered, tables);
    if ((e = groove_err(groove__db__Database__rebuild(self->database, lowered))).code) { jazz_node_Parking_drop(&parking); return e; }
    groove__db__Database__set_missing_chunk_resolver(self->database, groove__chunks__MissingChunkResolver__clone(self->chunk_resolver));
    groove__chunks__OwnedChunkProvider__release(self->content_runtime_provider);
    self->content_runtime_provider = groove__db__Database__owned_chunk_provider(self->database);
    /* Existing peer I/O pumps retain this local-only lookup service. */
    void *reader = groove__db__Database__local_chunk_reader(self->database);
    groove__chunks__LocalChunkReader__refresh_from(self->local_chunk_reader, reader);
    groove__chunks__LocalChunkReader__release(reader);
    /* A catalogue rebuild must register the same frozen derived indexes as
     * ordinary open before compiling live query graphs against them. */
    if ((e = jazz_node__node__physical__NodeState__synchronize_physical_version_tables(self)).code) { jazz_node_Parking_drop(&parking); return e; }
    self->groove_runtime_token = jazz_node__node__next_groove_runtime_token();
    jazz_node__node__NodeState__invalidate_runtime_handles_after_database_rebuild(self);
    jazz_node_Parking_drop(&self->parking);
    self->parking = parking;
    return JZ_OK;
}

/* A catalogue change rebuilds Groove's in-memory graph registry, but it does
 * not restart this node: drop handles and plans compiled against the old
 * registry while keeping recovered durable facts. */
void jazz_node__node__NodeState__invalidate_runtime_handles_after_database_rebuild(NS *self)
{
    jz_btree_clear(&self->query.query_shape_cache);
    jz_btree_clear(&self->query.compiled_query_program_cache);
    jazz_engine__query_engine__QueryProgramTemplateCache__clear(self->query.query_program_templates);
    { u32 el = self->query.supported_query_program_requests.elem;
      jz_deque_drop(&self->query.supported_query_program_requests, jazz_node_SupportedQueryProgram_drop);
      jz_deque_init(&self->query.supported_query_program_requests, el); }
    jz_btree_clear(&self->query.read_policy_authorization_request_cache);
    jz_btree_clear(&self->query.policy_authorization_graph_cache);
    jz_btree_clear(&self->query.policy_authorization_graph_replacements);
    jz_btree_clear(&self->query.tx_versions_cache);
    { u32 el = self->query.tx_version_tables_cache_order.elem;
      jz_deque_drop(&self->query.tx_version_tables_cache_order, NULL);
      jz_deque_init(&self->query.tx_version_tables_cache_order, el); }
    jz_btree_clear(&self->query.tx_version_tables_cache_order_set);
    jz_btree_clear(&self->query.version_storage_sources_cache);
    jazz_node__node__NodeState__invalidate_subscription_scopes(self);
}

/* A reset replaces receipt contents, not the receipt itself: the exact
 * policy-scoped lifecycle generation, opening marker, deferred-publication
 * bit and terminal queue must survive while new membership is installed. */
void jazz_node__node__NodeState__clear_settled_result_view(NS *self, jazz_protocol_AuthorityResultKey authority_result_key)
{
    jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, &authority_result_key);
    if (state) {
        state->has_supporting_revision = false;
        jz_btree_clear(&state->covered_input_versions);
        state->source_closure = (jazz_node_AuthoritySourceClosure){ .tag = JZN_ASC_PENDING };
        jazz_node_AuthoritySourceIncrementalVec_clear(&state->source_incrementals);
    }
    jz_btree_remove(&self->query.retained_root_window_sources, &authority_result_key, NULL, NULL);
    jazz_protocol_AuthorityResultKey_drop(&authority_result_key);
}

/* Retire a receipt whose exact usage-site ownership has ended. Distinct from
 * clear_settled_result_view: an unsubscribe or ownerless-recovery
 * invalidation must not leave a settled stamp for the next usage site. */
void jazz_node__node__NodeState__retire_authority_result_view(NS *self, jazz_protocol_AuthorityResultKey authority_result_key)
{
    jz_btree_remove(&self->query.authority_results, &authority_result_key, NULL, NULL);
    jz_btree_remove(&self->query.retained_root_window_sources, &authority_result_key, NULL, NULL);
    jazz_protocol_AuthorityResultKey_drop(&authority_result_key);
}

/* ===================== catalogue open stage ===================== */

static const jz_btree_desc LENSES_DESC_LOCAL = { sizeof(jz_uuid), sizeof(jazz_protocol_MigrationLens), jz_cmp_uuid, NULL, jazz_protocol_MigrationLens_drop };
static const jz_btree_desc PENDING_BY_SEQ_DESC_LOCAL = { sizeof(u64), sizeof(jazz_node_PendingSchemaLineage), jz_cmp_u64, NULL, jazz_node_PendingSchemaLineage_drop };
static const jz_btree_desc ACTIVATION_BY_ID_DESC = { sizeof(jz_uuid), sizeof(jazz_node_SchemaLineageActivation), jz_cmp_uuid, NULL, NULL };
static const jz_btree_desc U64_UUID_DESC = { sizeof(u64), sizeof(jz_uuid), jz_cmp_u64, NULL, NULL };
static const jz_btree_desc POINTER_BY_REVISION_DESC = { sizeof(u64), sizeof(jazz_protocol_CurrentWriteSchema), jz_cmp_u64, NULL, NULL };
static const jz_btree_desc ALIASES_DESC_LOCAL = { sizeof(jz_uuid), sizeof(jazz_types_SchemaVersionAlias), jz_cmp_uuid, NULL, NULL };
static const jz_btree_desc ALIAS_TO_SCHEMA_DESC = { sizeof(u64), sizeof(jz_uuid), jz_cmp_u64, NULL, NULL };
static const jz_btree_desc MAPPINGS_DESC_LOCAL = { sizeof(jz_uuid), sizeof(jazz_node_SchemaPhysicalMapping), jz_cmp_uuid, NULL, jazz_node_SchemaPhysicalMapping_drop };
static const jz_btree_desc STAGED_BY_TARGET_DESC_LOCAL = { sizeof(jz_uuid), sizeof(jazz_node_StagedSchemaLineage), jz_cmp_uuid, NULL, jazz_node_StagedSchemaLineage_drop };

static jz_err bump_successor(u64 id, u64 *next, const char *msg)
{
    if (id == UINT64_MAX) return JZN_INVALID_STORED(msg);
    if (id + 1 > *next) *next = id + 1;
    return JZ_OK;
}

static jz_err bump_from_mapping_tables(const jz_btree *tables, u64 *next_table_id, u64 *next_column_id)
{
    const jz_str *tk; const jazz_node_PhysicalTableMapping *table;
    JZ_BTREE_FOR(tables, tk, table) {
        JZ_TRY(bump_successor(table->table_id, next_table_id, "physical table id exhausted"));
        const jz_str *ck; const jazz_types_PhysicalColumnId *column_id;
        JZ_BTREE_FOR(&table->columns, ck, column_id) JZ_TRY(bump_successor(*column_id, next_column_id, "physical column id exhausted"));
    }
    return JZ_OK;
}

static void put_catalogue_value(groove_DatabaseBatch *batch, u32 kind, jz_uuid id, jz_bytes payload)
{
    groove_ValueVec values = {0};
    JZ_VEC_PUSH(values, gv_u64(jazz_node__node__codec__CatalogueRecordKind__key(kind)));
    JZ_VEC_PUSH(values, gv_uuid(id));
    JZ_VEC_PUSH(values, gv_bytes(payload));
    jz_str table = jz_str_from("jazz_catalogue");
    groove__db__batch__DatabaseBatch__update(batch, &table, values);
    jz_str_drop(&table);
}

jz_err jazz_node__node__NodeState__open_catalogue_stage(jazz_model_JazzSchema schema, jazz_node_BoxedStorage *storage,
    u32 catalogue_bootstrap_state, jazz_node_CatalogueOpenState *out)
{
    jazz_types_SchemaVersionId local_schema_version_id = jazz_model__schema__JazzSchema__version_id(&schema);
    groove_Database *meta_database;
    jz_err e = open_meta_database(jazz_model__schema__JazzSchema__lower_catalogue_meta_to_groove(&schema), storage, &meta_database);
    if (e.code) { jazz_model_JazzSchema_drop(&schema); return e; }
    bool has_recovered_active_schema = false; jazz_node_ActiveSchema recovered_active_schema;
    jz_btree catalogue_schemas; jz_btree_init(&catalogue_schemas, &SCHEMA_VERSIONS_DESC_LOCAL);
    jz_btree catalogue_lenses; jz_btree_init(&catalogue_lenses, &LENSES_DESC_LOCAL);
    jz_btree staged_lineages_by_id; jz_btree_init(&staged_lineages_by_id, &STAGED_BY_ID_DESC);
    jz_btree pending_lineages; jz_btree_init(&pending_lineages, &PENDING_BY_SEQ_DESC_LOCAL);
    jz_btree active_lineages_by_id; jz_btree_init(&active_lineages_by_id, &ACTIVATION_BY_ID_DESC);
    jz_btree pending_write_pointers; jz_btree_init(&pending_write_pointers, &POINTER_BY_REVISION_DESC);
    bool has_genesis_schema = false; jazz_types_SchemaVersionId genesis_schema = {{0}};
    bool has_bootstrap_ready = false;
    jz_btree staged_lineages; jz_btree_init(&staged_lineages, &STAGED_BY_SEQ_DESC_LOCAL);
    jz_btree active_lineages_by_target; jz_btree_init(&active_lineages_by_target, &STAGED_BY_TARGET_DESC_LOCAL);
    jz_btree lineage_targets; jz_btree_init(&lineage_targets, &jazz_node_UUID_SET_DESC);
    jz_btree active_lineages_by_seq; jz_btree_init(&active_lineages_by_seq, &U64_UUID_DESC);
    jz_btree schema_version_aliases; jz_btree_init(&schema_version_aliases, &ALIASES_DESC_LOCAL);
    jz_btree schema_version_for_alias; jz_btree_init(&schema_version_for_alias, &ALIAS_TO_SCHEMA_DESC);
    jz_btree physical_mappings; jz_btree_init(&physical_mappings, &MAPPINGS_DESC_LOCAL);
    jazz_node_ActiveSchema active_schema; bool has_active_schema = false;
    u64 active_catalogue_seq = 0, next_physical_table_id = 1, next_physical_column_id = 1;
    groove_RawRowVec raws = {0};
    jz_str catalogue_table = jz_str_from("jazz_catalogue");
    e = groove_err(groove__db__primary_storage__Database__primary_key_scan_raw(meta_database, &catalogue_table, NULL, 0, &raws));
    jz_str_drop(&catalogue_table);
    for (u32 i = 0; !e.code && i < raws.len; ++i) {
        u32 kind; jz_uuid id; jz_slice payload;
        if ((e = catalogue_row(raws.ptr[i], &kind, &id, &payload)).code) break;
        switch (kind) {
        case JZN_CRK_ACTIVE_SCHEMA: {
            jz_uuid nil = {{0}};
            if (!jz_uuid_eq(&id, &nil) || has_recovered_active_schema) { e = JZN_INVALID_STORED("invalid active schema record identity"); break; }
            if ((e = jazz_node__node__codec__decode_active_schema(payload, &recovered_active_schema)).code) break;
            has_recovered_active_schema = true;
            break;
        }
        case JZN_CRK_SCHEMA: {
            jazz_protocol_SchemaVersion schema_version;
            if ((e = jazz_node__node__codec__decode_catalogue_schema(payload, &schema_version)).code) break;
            jazz_types_SchemaVersionId content = jazz_model__schema__JazzSchema__version_id(&schema_version.schema);
            if (!jz_uuid_eq(&schema_version.id, &id) || !jz_uuid_eq(&schema_version.id, &content)) {
                jazz_protocol_SchemaVersion_drop(&schema_version);
                e = JZN_INVALID_STORED("catalogue schema id does not match schema payload"); break;
            }
            jazz_protocol_SchemaVersion old;
            if (jz_btree_insert(&catalogue_schemas, &schema_version.id, &schema_version, &old)) jazz_protocol_SchemaVersion_drop(&old);
            break;
        }
        case JZN_CRK_LENS: {
            jazz_protocol_MigrationLens lens;
            if ((e = jazz_node__node__codec__decode_catalogue_lens(payload, &lens)).code) break;
            jazz_types_MigrationLensId content = jazz_protocol__protocol__MigrationLens__content_id(&lens);
            if (!jz_uuid_eq(&lens.id, &id) || !jz_uuid_eq(&lens.id, &content)) {
                jazz_protocol_MigrationLens_drop(&lens);
                e = JZN_INVALID_STORED("catalogue lens id does not match lens payload"); break;
            }
            jazz_protocol_MigrationLens old;
            if (jz_btree_insert(&catalogue_lenses, &lens.id, &lens, &old)) jazz_protocol_MigrationLens_drop(&old);
            break;
        }
        case JZN_CRK_SCHEMA_LINEAGE_STAGED: {
            jazz_node_StagedSchemaLineage staged;
            if ((e = jazz_node__node__codec__decode_catalogue_staged_lineage(payload, &staged)).code) break;
            jazz_types_SchemaLineagePublicationId content = jazz_protocol__protocol__SchemaLineagePublication__content_id(&staged.publication);
            if (!jz_uuid_eq(&staged.publication.id, &id) || !jz_uuid_eq(&staged.publication.id, &content)) {
                jazz_node_StagedSchemaLineage_drop(&staged);
                e = JZN_INVALID_STORED("staged schema lineage id mismatch"); break;
            }
            jazz_node_StagedSchemaLineage old;
            if (jz_btree_insert(&staged_lineages_by_id, &staged.publication.id, &staged, &old)) jazz_node_StagedSchemaLineage_drop(&old);
            break;
        }
        case JZN_CRK_SCHEMA_LINEAGE_PENDING: {
            jazz_node_PendingSchemaLineage pending;
            if ((e = jazz_node__node__codec__decode_catalogue_pending_lineage(payload, &pending)).code) break;
            jazz_types_SchemaLineagePublicationId content = jazz_protocol__protocol__SchemaLineagePublication__content_id(&pending.publication);
            if (!jz_uuid_eq(&pending.publication.id, &id) || !jz_uuid_eq(&pending.publication.id, &content)
                || jz_btree_contains(&pending_lineages, &pending.catalogue_seq)) {
                jazz_node_PendingSchemaLineage_drop(&pending);
                e = JZN_INVALID_STORED("pending schema lineage identity conflict"); break;
            }
            jz_btree_insert(&pending_lineages, &pending.catalogue_seq, &pending, NULL);
            break;
        }
        case JZN_CRK_SCHEMA_LINEAGE_ACTIVE: {
            jazz_node_SchemaLineageActivation active;
            if ((e = jazz_node__node__codec__decode_catalogue_lineage_activation(payload, &active)).code) break;
            if (!jz_uuid_eq(&active.id, &id) || active.catalogue_seq == 0 || jz_btree_insert(&active_lineages_by_id, &active.id, &active, NULL))
                e = JZN_INVALID_STORED("active schema lineage id mismatch");
            break;
        }
        case JZN_CRK_WRITE_POINTER_PENDING: {
            jazz_protocol_CurrentWriteSchema pointer;
            if ((e = jazz_node__node__codec__decode_catalogue_write_pointer(payload, &pointer)).code) break;
            jz_uuid pointer_id = jazz_node__node__codec__catalogue_write_pointer_id(pointer);
            if (!jz_uuid_eq(&id, &pointer_id)) { e = JZN_INVALID_STORED("pending catalogue write-pointer id mismatch"); break; }
            if (jz_btree_insert(&pending_write_pointers, &pointer.revision, &pointer, NULL))
                e = JZN_INVALID_STORED("duplicate pending catalogue write-pointer revision");
            break;
        }
        case JZN_CRK_GENESIS:
            if (has_genesis_schema) { e = JZN_INVALID_STORED("duplicate catalogue genesis marker"); break; }
            has_genesis_schema = true; genesis_schema = id;
            break;
        case JZN_CRK_BOOTSTRAP_READY: {
            jazz_node_CatalogueBootstrapReady ready;
            if ((e = jazz_node__node__codec__decode_catalogue_bootstrap_ready(payload, &ready)).code) break;
            if (!jz_uuid_eq(&ready.genesis, &id) || has_bootstrap_ready) { e = JZN_INVALID_STORED("duplicate or malformed catalogue bootstrap marker"); break; }
            has_bootstrap_ready = true;
            break;
        }
        }
    }
    groove_RawRowVec_drop(&raws);
    if (e.code) goto fail;
    /* A standalone `PublishLens` survives independently of a lineage receipt:
     * validate it against the exact durable endpoints before it becomes
     * resident catalogue state. */
    {
        const jz_uuid *k; const jazz_protocol_MigrationLens *lens;
        JZ_BTREE_FOR(&catalogue_lenses, k, lens) {
            const jazz_protocol_SchemaVersion *source = jz_btree_get(&catalogue_schemas, &lens->source);
            if (!source) { e = JZN_INVALID_STORED("catalogue lens source schema is missing"); goto fail; }
            const jazz_protocol_SchemaVersion *target = jz_btree_get(&catalogue_schemas, &lens->target);
            if (!target) { e = JZN_INVALID_STORED("catalogue lens target schema is missing"); goto fail; }
            jz_err ve = jazz_node__node__ingest__NodeState__validate_migration_lens_between(lens, source, target);
            if (ve.code) { jz_err_drop(&ve); e = JZN_INVALID_STORED("catalogue lens violates trusted semantic invariants"); goto fail; }
        }
    }
    {
        const jz_uuid *k; const jazz_node_SchemaLineageActivation *active;
        JZ_BTREE_FOR(&active_lineages_by_id, k, active)
            if (jz_btree_insert(&active_lineages_by_seq, &active->catalogue_seq, &active->id, NULL)) { e = JZN_INVALID_STORED("duplicate active catalogue sequence"); goto fail; }
    }
    {
        jz_uuid id; jazz_node_StagedSchemaLineage staged;
        while (jz_btree_pop_first(&staged_lineages_by_id, &id, &staged)) {
            if ((e = jazz_node__node__NodeState__validate_durable_staged_lineage(&staged, &catalogue_schemas)).code) { jazz_node_StagedSchemaLineage_drop(&staged); goto fail; }
            if (staged.catalogue_seq == 0) { jazz_node_StagedSchemaLineage_drop(&staged); e = JZN_INVALID_STORED("staged schema lineage sequence must be nonzero"); goto fail; }
            if (jz_btree_insert(&lineage_targets, &staged.publication.schema.id, NULL, NULL)) {
                jazz_node_StagedSchemaLineage_drop(&staged); e = JZN_INVALID_STORED("duplicate durable schema lineage target"); goto fail;
            }
            jazz_node_SchemaLineageActivation active;
            if (jz_btree_remove(&active_lineages_by_id, &staged.publication.id, NULL, &active)) {
                if (active.catalogue_seq != staged.catalogue_seq) {
                    jazz_node_StagedSchemaLineage_drop(&staged); e = JZN_INVALID_STORED("active schema lineage payload conflicts with marker"); goto fail;
                }
                jazz_types_SchemaVersionId target = staged.publication.schema.id;
                jz_btree_insert(&active_lineages_by_target, &target, &staged, NULL);
            } else if (jz_btree_contains(&active_lineages_by_seq, &staged.catalogue_seq)) {
                jazz_node_StagedSchemaLineage_drop(&staged); e = JZN_INVALID_STORED("staged catalogue sequence conflicts with active lineage"); goto fail;
            } else if (jz_btree_contains(&staged_lineages, &staged.catalogue_seq)) {
                jazz_node_StagedSchemaLineage_drop(&staged); e = JZN_INVALID_STORED("duplicate staged catalogue sequence"); goto fail;
            } else {
                u64 seq = staged.catalogue_seq;
                jz_btree_insert(&staged_lineages, &seq, &staged, NULL);
            }
        }
    }
    if (active_lineages_by_id.len != 0) { e = JZN_INVALID_STORED("active schema lineage is missing canonical payload"); goto fail; }
    {
        u64 expected = 1; const u64 *actual; const jz_uuid *v;
        JZ_BTREE_FOR(&active_lineages_by_seq, actual, v) {
            if (*actual != expected) { e = JZN_INVALID_STORED("active catalogue sequences are not contiguous"); goto fail; }
            ++expected;
        }
        void *last_k, *last_v;
        if (jz_btree_last(&active_lineages_by_seq, &last_k, &last_v)) active_catalogue_seq = *(u64 *)last_k;
    }
    {
        jz_str versions_table = jz_str_from("jazz_schema_versions");
        e = groove_err(groove__db__primary_storage__Database__primary_key_scan_raw(meta_database, &versions_table, NULL, 0, &raws));
        jz_str_drop(&versions_table);
        for (u32 i = 0; !e.code && i < raws.len; ++i) {
            const groove_OwnedRecord *record = groove__db__RawRow__record(raws.ptr[i]);
            jz_slice payload; jz_uuid schema_version; u64 alias;
            if ((e = record_err(groove__records__OwnedRecord__get_bytes(record, JZN_SCHEMA_VERSION_ALIAS_ROW_FIELD_PHYSICAL_MAPPING_IDX, &payload))).code) break;
            jazz_node_SchemaPhysicalMapping mapping;
            if ((e = jazz_node__node__codec__decode_physical_mapping(payload, &mapping)).code) break;
            if ((e = record_err(groove__records__OwnedRecord__get_uuid(record, JZN_SCHEMA_VERSION_ALIAS_ROW_FIELD_UUID_IDX, &schema_version))).code
                || (e = record_err(groove__records__OwnedRecord__get_u64(record, JZN_SCHEMA_VERSION_ALIAS_ROW_FIELD_ID_IDX, &alias))).code) {
                jazz_node_SchemaPhysicalMapping_drop(&mapping); break;
            }
            /* Schema aliases become Groove variant tags in physical storage:
             * check the narrowing boundary on recovery. */
            u32 tag;
            if ((e = jazz_node__node__physical__groove_variant_tag(alias, &tag)).code) { jazz_node_SchemaPhysicalMapping_drop(&mapping); break; }
            const jazz_types_SchemaVersionAlias *existing = jz_btree_get(&schema_version_aliases, &schema_version);
            if (existing && *existing != alias) { jazz_node_SchemaPhysicalMapping_drop(&mapping); e = JZN_INVALID_STORED("schema version has conflicting durable aliases"); break; }
            const jz_uuid *existing_schema = jz_btree_get(&schema_version_for_alias, &alias);
            if (existing_schema && !jz_uuid_eq(existing_schema, &schema_version)) {
                jazz_node_SchemaPhysicalMapping_drop(&mapping); e = JZN_INVALID_STORED("schema alias maps to multiple durable schema versions"); break;
            }
            jz_btree_insert(&schema_version_aliases, &schema_version, &alias, NULL);
            jz_btree_insert(&schema_version_for_alias, &alias, &schema_version, NULL);
            jazz_node_SchemaPhysicalMapping old;
            if (jz_btree_insert(&physical_mappings, &schema_version, &mapping, &old)) jazz_node_SchemaPhysicalMapping_drop(&old);
        }
        groove_RawRowVec_drop(&raws);
        if (e.code) goto fail;
    }
    if ((e = jazz_node__node__physical__validate_physical_variant_cases(&physical_mappings, &schema_version_aliases)).code) goto fail;
    if ((e = jazz_node__node__physical__validate_physical_mapping_registries(&physical_mappings, &schema_version_aliases)).code) goto fail;
    {
        const jz_uuid *k; const jazz_node_SchemaPhysicalMapping *mapping;
        JZ_BTREE_FOR(&physical_mappings, k, mapping) if ((e = bump_from_mapping_tables(&mapping->tables, &next_physical_table_id, &next_physical_column_id)).code) goto fail;
        const u64 *sk; const jazz_node_StagedSchemaLineage *staged;
        JZ_BTREE_FOR(&staged_lineages, sk, staged) if ((e = bump_from_mapping_tables(&staged->mapping.tables, &next_physical_table_id, &next_physical_column_id)).code) goto fail;
    }
    if (has_genesis_schema && !jz_btree_contains(&catalogue_schemas, &local_schema_version_id)) { e = JZN_INVALID_STORED("opened schema is absent from the durable catalogue"); goto fail; }
    if (!has_genesis_schema && catalogue_schemas.len != 0) { e = JZN_INVALID_STORED("catalogue schemas exist without a genesis marker"); goto fail; }
    if (catalogue_bootstrap_state == JZN_CBS_UNINITIALIZED
        && (has_genesis_schema || catalogue_schemas.len || catalogue_lenses.len || physical_mappings.len || schema_version_aliases.len
            || active_catalogue_seq != 0 || staged_lineages.len || pending_lineages.len || active_lineages_by_target.len
            || pending_write_pointers.len || has_recovered_active_schema)) {
        e = JZN_INVALID_STORED("uninitialized catalogue open requires empty durable catalogue state"); goto fail;
    }
    if (has_recovered_active_schema) {
        if (!jz_btree_contains(&catalogue_schemas, &recovered_active_schema.schema)) { e = JZN_INVALID_STORED("active schema references an unknown structural schema"); goto fail; }
        active_schema = recovered_active_schema; has_recovered_active_schema = false;
    } else if (has_genesis_schema) {
        e = JZN_INVALID_STORED("catalogue active schema missing"); goto fail;
    } else {
        jazz_model_JazzSchema compiled; jazz_model_JazzSchema_clone(&compiled, &schema);
        if ((e = jazz_node__node__ActiveSchema__new((jazz_protocol_CurrentWriteSchema){ .revision = 0, .schema = local_schema_version_id }, compiled, &active_schema)).code) goto fail;
    }
    has_active_schema = true;
    bool had_current_schema = jz_btree_contains(&catalogue_schemas, &local_schema_version_id);
    if (!had_current_schema) {
        jazz_model_JazzSchema copy; jazz_model_JazzSchema_clone(&copy, &schema);
        jazz_protocol_SchemaVersion sv = jazz_protocol__protocol__SchemaVersion__new(copy);
        jz_btree_insert(&catalogue_schemas, &local_schema_version_id, &sv, NULL);
    }
    if (!jz_btree_contains(&physical_mappings, &local_schema_version_id) || !jz_btree_contains(&schema_version_aliases, &local_schema_version_id)) {
        jazz_node_SchemaPhysicalMapping mapping;
        const jazz_node_SchemaPhysicalMapping *existing = jz_btree_get(&physical_mappings, &local_schema_version_id);
        if (existing) jazz_node_SchemaPhysicalMapping_clone(&mapping, existing);
        else if ((e = jazz_node__node__physical__allocate_provisional_physical_mapping(&schema, jazz_protocol__protocol__PhysicalIdentityManifest__allocate(&schema),
                &next_physical_table_id, &next_physical_column_id, &mapping)).code) goto fail;
        jazz_types_SchemaVersionAlias alias;
        const jazz_types_SchemaVersionAlias *existing_alias = jz_btree_get(&schema_version_aliases, &local_schema_version_id);
        if (existing_alias) alias = *existing_alias;
        else {
            u64 max = 0; const jz_uuid *k; const jazz_types_SchemaVersionAlias *a;
            JZ_BTREE_FOR(&schema_version_aliases, k, a) if (*a > max) max = *a;
            if (max == UINT64_MAX) { jazz_node_SchemaPhysicalMapping_drop(&mapping); e = JZN_INVALID_STORED("schema version alias exhausted"); goto fail; }
            alias = max + 1;
        }
        if (catalogue_bootstrap_state == JZN_CBS_READY) {
            groove_DatabaseBatch *batch = groove__db__facade__Database__open_batch(meta_database);
            if (!has_genesis_schema) put_catalogue_value(batch, JZN_CRK_GENESIS, local_schema_version_id, (jz_bytes){0});
            if (!had_current_schema) {
                jazz_model_JazzSchema copy; jazz_model_JazzSchema_clone(&copy, &schema);
                jazz_protocol_SchemaVersion sv = jazz_protocol__protocol__SchemaVersion__new(copy);
                jz_bytes encoded;
                e = jazz_node__node__codec__encode_catalogue_schema(&sv, &encoded);
                jazz_protocol_SchemaVersion_drop(&sv);
                if (!e.code) put_catalogue_value(batch, JZN_CRK_SCHEMA, local_schema_version_id, encoded);
            }
            if (!e.code) e = jazz_node__node__NodeState__write_schema_version_mapping_to_batch(batch, alias, local_schema_version_id, &mapping);
            if (!e.code && !has_genesis_schema) e = jazz_node__node__NodeState__write_active_schema_to_batch(batch, &active_schema);
            groove_AppliedBatch *applied = NULL;
            if (!e.code) e = groove_err(groove__db__commit__Database__apply_batch(meta_database, batch, &applied));
            else groove__db__batch__DatabaseBatch__drop(batch);
            if (!e.code) {
                groove_PersistedBatch *persisted = groove__db__commit__Database__persist_with_progress(meta_database, applied);
                groove__db__commit__AppliedBatch__drop(applied);
                e = groove_err(groove__db__commit__Database__finish_persistence(meta_database, persisted));
            }
            if (e.code) { jazz_node_SchemaPhysicalMapping_drop(&mapping); goto fail; }
        }
        jz_btree_insert(&schema_version_aliases, &local_schema_version_id, &alias, NULL);
        jazz_node_SchemaPhysicalMapping old;
        if (jz_btree_insert(&physical_mappings, &local_schema_version_id, &mapping, &old)) jazz_node_SchemaPhysicalMapping_drop(&old);
    }
    if ((e = jazz_node__node__NodeState__validate_durable_physical_identity_bindings(&catalogue_schemas, &physical_mappings, &staged_lineages,
            &active_lineages_by_target, &pending_lineages, has_genesis_schema, genesis_schema, catalogue_bootstrap_state)).code) goto fail;
    /* Descriptor rebuilding sorts payload cases by their durable introduction
     * provenance: validate each stored provenance triple first. */
    if ((e = jazz_node__node__physical__validate_scalar_enum_case_provenance(&physical_mappings, &schema_version_aliases)).code) goto fail;
    if ((e = jazz_node__node__physical__validate_payload_enum_case_provenance(&physical_mappings, &schema_version_aliases)).code) goto fail;
    *out = (jazz_node_CatalogueOpenState){
        .storage = groove__db__Database__into_storage(meta_database),
        .schemas = catalogue_schemas, .lenses = catalogue_lenses, .schema_version_aliases = schema_version_aliases,
        .physical_mappings = physical_mappings, .staged_lineages = staged_lineages, .pending_lineages = pending_lineages,
        .active_lineages_by_target = active_lineages_by_target, .active_catalogue_seq = active_catalogue_seq,
        .next_physical_table_id = next_physical_table_id, .next_physical_column_id = next_physical_column_id,
        .catalogue_bootstrap_marker = has_bootstrap_ready, .active_schema = active_schema,
    };
    jz_btree_drop(&staged_lineages_by_id); jz_btree_drop(&active_lineages_by_id); jz_btree_drop(&pending_write_pointers);
    jz_btree_drop(&lineage_targets); jz_btree_drop(&active_lineages_by_seq); jz_btree_drop(&schema_version_for_alias);
    jazz_model_JazzSchema_drop(&schema);
    return JZ_OK;
fail:
    if (has_recovered_active_schema) jazz_node_ActiveSchema_drop(&recovered_active_schema);
    if (has_active_schema) jazz_node_ActiveSchema_drop(&active_schema);
    jz_btree_drop(&catalogue_schemas); jz_btree_drop(&catalogue_lenses); jz_btree_drop(&staged_lineages_by_id);
    jz_btree_drop(&pending_lineages); jz_btree_drop(&active_lineages_by_id); jz_btree_drop(&pending_write_pointers);
    jz_btree_drop(&staged_lineages); jz_btree_drop(&active_lineages_by_target); jz_btree_drop(&lineage_targets);
    jz_btree_drop(&active_lineages_by_seq); jz_btree_drop(&schema_version_aliases); jz_btree_drop(&schema_version_for_alias);
    jz_btree_drop(&physical_mappings);
    groove__db__Database__drop(meta_database);
    jazz_model_JazzSchema_drop(&schema);
    return e;
}

jz_err jazz_node__node__NodeState__validate_durable_staged_lineage(const jazz_node_StagedSchemaLineage *staged, const jz_btree *catalogue_schemas)
{
    jz_err e = jazz_node__node__ingest__NodeState__validate_schema_lineage_publication(&staged->publication);
    if (e.code) { jz_err_drop(&e); return JZN_INVALID_STORED("staged schema lineage violates trusted publication invariants"); }
    JZ_VEC_FOR(const jazz_protocol_SchemaPredecessor, predecessor, staged->publication.predecessors) {
        const jazz_protocol_SchemaVersion *source = jz_btree_get(catalogue_schemas, &predecessor->lens.source);
        if (!source) return JZN_INVALID_STORED("staged schema lineage source is missing");
        e = jazz_node__node__ingest__NodeState__validate_migration_lens_between(&predecessor->lens, source, &staged->publication.schema);
        if (e.code) { jz_err_drop(&e); return JZN_INVALID_STORED("staged schema lineage lens is invalid"); }
        e = jazz_node__node__ingest__NodeState__validate_lineage_table_partition(&source->schema, &staged->publication.schema.schema,
            &predecessor->lens, &predecessor->new_tables, &predecessor->dropped_tables);
        if (e.code) { jz_err_drop(&e); return JZN_INVALID_STORED("staged schema lineage table partition is invalid"); }
    }
    return JZ_OK;
}

/* ---- history_for closure of validate_durable_physical_identity_bindings ---- */
typedef struct HistoryCtx { const jz_btree *mappings, *staged, *active, *pending; jz_btree parents; } HistoryCtx;
static void uuid_vec_drop_v(void *p) { jz_free(((jz_uuid_vec *)p)->ptr); }
static const jz_btree_desc PARENTS_DESC_LOCAL = { sizeof(jz_uuid), sizeof(jz_uuid_vec), jz_cmp_uuid, NULL, uuid_vec_drop_v };

static void parents_insert(jz_btree *parents, const jazz_protocol_SchemaLineagePublication *p)
{
    jz_uuid_vec sources = {0};
    JZ_VEC_FOR(const jazz_protocol_SchemaPredecessor, pred, p->predecessors) JZ_VEC_PUSH(sources, pred->lens.source);
    jz_uuid_vec old;
    if (jz_btree_insert(parents, &p->schema.id, &sources, &old)) jz_free(old.ptr);
}

static bool is_descendant(const jz_btree *parents, jazz_types_SchemaVersionId candidate, jazz_types_SchemaVersionId schema)
{
    jz_uuid_vec remaining = {0};
    JZ_VEC_PUSH(remaining, schema);
    jz_btree visited; jz_btree_init(&visited, &jazz_node_UUID_SET_DESC);
    bool found = false;
    while (remaining.len && !found) {
        jz_uuid cursor = remaining.ptr[--remaining.len];
        if (jz_btree_insert(&visited, &cursor, NULL, NULL)) continue;
        const jz_uuid_vec *sources = jz_btree_get(parents, &cursor);
        if (!sources) continue;
        JZ_VEC_FOR(const jz_uuid, s, *sources) if (jz_uuid_eq(s, &candidate)) { found = true; break; }
        if (!found) JZ_VEC_FOR(const jz_uuid, s, *sources) JZ_VEC_PUSH(remaining, *s);
    }
    jz_free(remaining.ptr);
    jz_btree_drop(&visited);
    return found;
}

static jazz_protocol_PhysicalIdentityManifestVec history_for(const HistoryCtx *c, jazz_types_SchemaVersionId candidate, jazz_types_SchemaLineagePublicationId omit)
{
    jazz_protocol_PhysicalIdentityManifestVec out = {0};
    const void *k; const jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(c->mappings, k, mapping) {
        const jz_uuid *schema = k;
        if (jz_uuid_eq(schema, &candidate) || is_descendant(&c->parents, candidate, *schema)) continue;
        jazz_protocol_PhysicalIdentityManifest m; jazz_protocol_PhysicalIdentityManifest_clone(&m, &mapping->identities);
        JZ_VEC_PUSH(out, m);
    }
    const jazz_node_StagedSchemaLineage *lineage;
    JZ_BTREE_FOR(c->staged, k, lineage) {
        if (jz_uuid_eq(&lineage->publication.id, &omit) || is_descendant(&c->parents, candidate, lineage->publication.schema.id)) continue;
        jazz_protocol_PhysicalIdentityManifest m; jazz_protocol_PhysicalIdentityManifest_clone(&m, &lineage->publication.physical_identities);
        JZ_VEC_PUSH(out, m);
    }
    const jazz_node_PendingSchemaLineage *pending;
    JZ_BTREE_FOR(c->pending, k, pending) {
        if (jz_uuid_eq(&pending->publication.id, &omit) || is_descendant(&c->parents, candidate, pending->publication.schema.id)) continue;
        jazz_protocol_PhysicalIdentityManifest m; jazz_protocol_PhysicalIdentityManifest_clone(&m, &pending->publication.physical_identities);
        JZ_VEC_PUSH(out, m);
    }
    return out;
}

static const jz_btree_desc PUBLISHED_DESC = { sizeof(jz_uuid), sizeof(const jazz_protocol_SchemaLineagePublication *), jz_cmp_uuid, NULL, NULL };

/* Bind every recovered local alias mapping to the one authority manifest that
 * published its schema. The integer aliases are deliberately local; the UUID
 * manifest is not, so recovery re-checks the binding. */
jz_err jazz_node__node__NodeState__validate_durable_physical_identity_bindings(const jz_btree *schemas, const jz_btree *mappings,
    const jz_btree *staged, const jz_btree *active, const jz_btree *pending, bool has_genesis, jazz_types_SchemaVersionId genesis, u32 bootstrap_state)
{
    if (bootstrap_state == JZN_CBS_UNINITIALIZED) return JZ_OK;
    if (mappings->len != schemas->len) return JZN_INVALID_STORED("durable schema and physical mapping coverage differs");
    const jz_uuid *schema_id; const jazz_protocol_SchemaVersion *schema;
    JZ_BTREE_FOR(schemas, schema_id, schema) {
        const jazz_node_SchemaPhysicalMapping *mapping = jz_btree_get(mappings, schema_id);
        if (!mapping) return JZN_INVALID_STORED("durable schema physical mapping is missing");
        jz_err e = jazz_protocol__protocol__PhysicalIdentityManifest__validate_for_schema(&mapping->identities, &schema->schema);
        if (e.code) { jz_err_drop(&e); return JZN_INVALID_STORED("durable physical identity manifest is invalid"); }
    }
    jz_btree published; jz_btree_init(&published, &PUBLISHED_DESC);
    jz_btree sequences; jz_btree_init(&sequences, &UUID_U64_DESC);
    const jz_btree *lineage_maps[2] = { active, staged };
    jz_err e = JZ_OK;
    for (u32 m = 0; m < 2 && !e.code; ++m) {
        const void *k; const jazz_node_StagedSchemaLineage *lineage;
        JZ_BTREE_FOR(lineage_maps[m], k, lineage) {
            const jazz_protocol_SchemaLineagePublication *p = &lineage->publication;
            if (jz_btree_insert(&published, &p->schema.id, &p, NULL)) { e = JZN_INVALID_STORED("durable schema has multiple identity publications"); break; }
            jz_btree_insert(&sequences, &p->schema.id, &lineage->catalogue_seq, NULL);
        }
    }
    for (u32 m = 0; m < 2 && !e.code; ++m) {
        const void *k; const jazz_node_StagedSchemaLineage *lineage;
        JZ_BTREE_FOR(lineage_maps[m], k, lineage) {
            JZ_VEC_FOR(const jazz_protocol_SchemaPredecessor, predecessor, lineage->publication.predecessors) {
                const u64 *sequence = jz_btree_get(&sequences, &predecessor->lens.source);
                if (sequence && *sequence >= lineage->catalogue_seq) { e = JZN_INVALID_STORED("schema predecessor must precede its target"); break; }
            }
            if (e.code) break;
        }
    }
    jz_btree_drop(&sequences);
    if (e.code) { jz_btree_drop(&published); return e; }
    u32 n_roots = 0; jz_uuid root = {{0}};
    JZ_BTREE_FOR(schemas, schema_id, schema) if (!jz_btree_contains(&published, schema_id)) { root = *schema_id; ++n_roots; }
    if (n_roots != 1) { jz_btree_drop(&published); return JZN_INVALID_STORED("durable catalogue must have exactly one genesis schema"); }
    if (has_genesis && !jz_uuid_eq(&genesis, &root)) { jz_btree_drop(&published); return JZN_INVALID_STORED("durable genesis marker disagrees with lineage root"); }
    if (jz_btree_contains(&published, &root)) { jz_btree_drop(&published); return JZN_INVALID_STORED("durable genesis is incorrectly published as a lineage target"); }
    /* Parked descendants may legally retain an identity first minted by the
     * bundle now being recovered; unrelated parked/staged bundles remain
     * reserved. */
    HistoryCtx ctx = { mappings, staged, active, pending };
    jz_btree_init(&ctx.parents, &PARENTS_DESC_LOCAL);
    for (u32 m = 0; m < 2; ++m) {
        const void *k; const jazz_node_StagedSchemaLineage *lineage;
        JZ_BTREE_FOR(lineage_maps[m], k, lineage) parents_insert(&ctx.parents, &lineage->publication);
    }
    { const void *k; const jazz_node_PendingSchemaLineage *lineage; JZ_BTREE_FOR(pending, k, lineage) parents_insert(&ctx.parents, &lineage->publication); }
    const jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(mappings, schema_id, mapping) {
        if (jz_uuid_eq(schema_id, &root)) continue;
        const jazz_protocol_SchemaLineagePublication *const *publication = jz_btree_get(&published, schema_id);
        if (!publication) { e = JZN_INVALID_STORED("durable non-genesis schema has no identity publication"); break; }
        if (!jazz_protocol_PhysicalIdentityManifest_eq(&mapping->identities, &(*publication)->physical_identities)) {
            e = JZN_INVALID_STORED("durable mapping identities disagree with authority publication"); break;
        }
        jz_err ve = jazz_node__node__NodeState__validate_publication_sources(*publication, schemas, mappings,
            history_for(&ctx, (*publication)->schema.id, (*publication)->id));
        if (ve.code) { jz_err_drop(&ve); e = JZN_INVALID_STORED("durable identity publication evolution is invalid"); break; }
    }
    if (!e.code) {
        const void *k; const jazz_node_PendingSchemaLineage *lineage;
        JZ_BTREE_FOR(pending, k, lineage) {
            /* A pending lineage may be durably parked ahead of the publication
             * that introduces its source; its self-contained target manifest,
             * content id and structural bounds are authoritative already. */
            jz_err ve = jazz_node__node__ingest__NodeState__validate_schema_lineage_publication(&lineage->publication);
            if (ve.code) { jz_err_drop(&ve); e = JZN_INVALID_STORED("pending schema lineage violates trusted publication invariants"); break; }
            if (any_predecessor_source_missing(&lineage->publication, schemas)) continue;
            ve = jazz_node__node__NodeState__validate_publication_sources(&lineage->publication, schemas, mappings,
                history_for(&ctx, lineage->publication.schema.id, lineage->publication.id));
            if (ve.code) { jz_err_drop(&ve); e = JZN_INVALID_STORED("pending identity publication evolution is invalid"); break; }
        }
    }
    jz_btree_drop(&ctx.parents);
    jz_btree_drop(&published);
    return e;
}
