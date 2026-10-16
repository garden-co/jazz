/* catalogue.c: port of crates/jazz/layers/db/src/db/catalogue.rs
 * Catalogue schema and migration-lens publication APIs.
 * `validate_schema_activation` (runtime feature) and the `*_for_test`
 * fixture helpers (test/testing) are not ported. */
#include "jz_rt.h"
#include "../_ext_db-b-lc.h"

#define CAT_FN(n) jazz_db__db__catalogue__Db__##n
#define NS(n) jazz_node__node__NodeState__##n
#define MUT(n) jazz_db__db__mutations__Db__##n

typedef struct jazz_types_MigrationLensId { u8 b[32]; } jazz_types_MigrationLensId;

static jz_err ne(jz_err e) {
    return JZ_IS_ERR(e) ? jazz_node__node__api_error__Error__from_node_error(e) : e;
}

/* Allocate the immutable global physical identities for a proposed
 * descendant lineage. This only authors a payload. */
jz_err CAT_FN(author_schema_lineage_publication)(const jazz_db_Db *db, jazz_model_SchemaVersion *schema, jazz_protocol_MigrationLens *lens,
                                                 jazz_db_StrVec new_tables, jazz_db_StrVec dropped_tables,
                                                 jazz_node_SchemaLineagePublication **out) {
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(db->node->node, "author_schema_lineage_publication");
    jz_err e = ne(NS(author_schema_lineage_publication)(node, schema, lens, new_tables, dropped_tables, out));
    jazz_db__db__SharedNodeState__unlock(db->node->node);
    return e;
}

/* lock().await.apply_trusted_catalogue_message(message).await?; finish_publication_outcome(outcome).await */
static jz_err apply_and_finish(const jazz_db_Db *db, jazz_protocol_SyncMessage message, jazz_db_SyncMessageVec *out) {
    jazz_db_bPublicationOutcome outcome;
    jazz_node_NodeState *node = jazz_db__db__SharedNodeState__lock(db->node->node);
    jz_err e = ne(NS(apply_trusted_catalogue_message)(node, message, &outcome));
    jazz_db__db__SharedNodeState__unlock(db->node->node);
    JZ_TRY(e);
    jazz_db_SyncMessageVec responses = {0};
    e = jazz_db__db__PublicationOutcome__responses(&outcome, &responses);
    if (!JZ_IS_ERR(e)) e = MUT(finish_publication_outcome)(db, outcome);
    if (JZ_IS_ERR(e)) {
        jz_vec_drop(&responses, sizeof(jazz_protocol_SyncMessage), jazz_protocol__protocol__SyncMessage__drop);
        return e;
    }
    *out = responses;
    return JZ_OK;
}

/* Publish an immutable schema-version payload through the catalogue lane. */
jz_err CAT_FN(publish_schema)(const jazz_db_Db *db, jazz_model_SchemaVersion *schema, jazz_db_SyncMessageVec *out) {
    JZ_TRY(MUT(check_catalogue_admin)(db));
    jazz_protocol_SyncMessage message;
    jazz_protocol__protocol__SyncMessage__publish_schema(&message, db->identity.author, schema);
    return apply_and_finish(db, message, out);
}

/* Atomically publish a non-genesis schema and its lineage-defining lens. */
jz_err CAT_FN(publish_schema_with_lens)(const jazz_db_Db *db, u64 catalogue_seq, jazz_node_SchemaLineagePublication *publication,
                                        jazz_db_SyncMessageVec *out) {
    JZ_TRY(MUT(check_catalogue_admin)(db));
    jazz_protocol_SyncMessage message;
    jazz_protocol__protocol__SyncMessage__publish_schema_with_lens(&message, db->identity.author, catalogue_seq, publication);
    return apply_and_finish(db, message, out);
}

/* Publish an immutable migration lens through the catalogue lane. */
jz_err CAT_FN(publish_lens)(const jazz_db_Db *db, jazz_protocol_MigrationLens *lens, jazz_db_SyncMessageVec *out) {
    JZ_TRY(MUT(check_catalogue_admin)(db));
    jazz_protocol_SyncMessage message;
    jazz_protocol__protocol__SyncMessage__publish_lens(&message, db->identity.author, lens);
    return apply_and_finish(db, message, out);
}

/* Activate an admitted structural schema with its explicit permissions.
 * The revision covers both schema and permissions, including policy-only changes. */
jz_err CAT_FN(activate_schema)(const jazz_db_Db *db, u64 revision, jazz_model_JazzSchema schema, jz_hmap permissions) {
    jz_err e = MUT(check_catalogue_admin)(db);
    if (JZ_IS_ERR(e)) goto drop_inputs;
    jz_btree source;
    jazz_model__schema__JazzSchema__public_schema_clone(&schema, &source);
    {
        jz_str *name; jazz_model_PublicTable **table;
        JZ_BTREE_FOR(&source, name, table) {
            jazz_model_TablePolicies policies;
            const jazz_model_TablePolicies *found = jz_hmap_get(&permissions, name);
            if (found) jazz_model__public_schema__TablePolicies__clone(&policies, found);
            else jazz_model__public_schema__TablePolicies__default(&policies);
            jazz_model__public_schema__PublicTable__set_policies(*table, policies);
        }
    }
    {
        jz_str *name; jazz_model_TablePolicies *policies;
        JZ_HMAP_FOR(&permissions, name, policies) {
            if (!jz_btree_contains(&source, name)) {
                jz_btree_drop(&source);
                e = ne(jz_err_new(JAZZ_NODE_ERR_INVALID_CATALOGUE_UPDATE, "active schema permissions reference unknown table"));
                goto drop_inputs;
            }
        }
    }
    jazz_model_JazzSchema active;
    jz_err built = jazz_model__schema__JazzSchema__new(&source, &active);
    jz_btree_drop(&source);
    if (JZ_IS_ERR(built)) {
        e = jz_err_fmt(JAZZ_EC_SCHEMA, "invalid active schema permissions: %.*s",
                       built.msg ? (int)built.msg->len : 0, built.msg ? built.msg->ptr : "");
        goto drop_inputs;
    }
    {
        jazz_node_NodeState *node = jazz_db__db__SharedNodeState__lock(db->node->node);
        e = ne(NS(activate_schema)(node, revision, active));
        jazz_db__db__SharedNodeState__unlock(db->node->node);
    }
    if (JZ_IS_ERR(e)) goto drop_inputs;
    e = jazz_db__db__node_runtime__Node__set_permissions_ready(db->node, true);
drop_inputs:
    jazz_model__schema__JazzSchema__drop(&schema);
    jz_hmap_drop(&permissions);
    return e;
}

/* Set whether this authority may settle session-scoped reads and writes. */
jz_err CAT_FN(set_permissions_ready)(const jazz_db_Db *db, bool ready) {
    return jazz_db__db__node_runtime__Node__set_permissions_ready(db->node, ready);
}

/* Return the current write-schema pointer known to this database. */
jz_err CAT_FN(current_write_schema)(const jazz_db_Db *db, jazz_types_SchemaVersionId *out) {
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(db->node->node, "current_write_schema");
    jz_err e = ne(NS(current_write_schema)(node, out));
    jazz_db__db__SharedNodeState__unlock(db->node->node);
    return e;
}

/* Return a published schema-version payload known to this database. */
bool CAT_FN(catalogue_schema)(const jazz_db_Db *db, jazz_types_SchemaVersionId schema, jazz_model_JazzSchema *out) {
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(db->node->node, "catalogue_schema");
    const jazz_node_CatalogueSchemaEntry *entry = NS(catalogue_schema)(node, &schema);
    if (entry) jazz_model__schema__JazzSchema__clone(out, &entry->schema);
    jazz_db__db__SharedNodeState__unlock(db->node->node);
    return entry != NULL;
}

/* Highest contiguously activated authoritative catalogue position. */
u64 CAT_FN(active_catalogue_seq)(const jazz_db_Db *db) {
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(db->node->node, "active_catalogue_seq");
    u64 seq = NS(active_catalogue_seq)(node);
    jazz_db__db__SharedNodeState__unlock(db->node->node);
    return seq;
}

/* Return a published migration lens known to this database. */
bool CAT_FN(catalogue_lens)(const jazz_db_Db *db, jazz_types_MigrationLensId lens, jazz_protocol_MigrationLens **out) {
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(db->node->node, "catalogue_lens");
    const jazz_protocol_MigrationLens *found = NS(catalogue_lens)(node, &lens);
    if (found) jazz_protocol__protocol__MigrationLens__clone(out, found);
    jazz_db__db__SharedNodeState__unlock(db->node->node);
    return found != NULL;
}
