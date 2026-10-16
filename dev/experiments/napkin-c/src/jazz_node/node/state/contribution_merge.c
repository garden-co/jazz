/* Port of crates/jazz/layers/node/src/node/state/contribution_merge.rs
 * (include!d into node/mod.rs, so the Rust module is jazz_node::node). */
#include "jz_rt.h"
#include "../../_ext_node-qe-b.h"
#include "../node_qe_b.h"

/* The one durable admission boundary for contribution provenance. Every path
 * that can persist a transaction must pass through this before it can
 * allocate aliases, stage large values, or mutate a batch. */
jz_err jazz_node__node__NodeState__admit_contribution_merge_for_storage(const jazz_node_NodeState *self, const jazz_model_Transaction *tx, groove_Value *out)
{
    JZ_TRY(jazz_node__node__NodeState__validate_contribution_merge_operation_identities(self, tx));
    return jazz_node__node__physical__NodeState__contribution_merge_storage_value(self,
        tx->has_contribution_merge ? &tx->contribution_merge : NULL, out);
}

static const jazz_model_TableSchema *find_table(const jazz_model_JazzSchema *schema, const jz_str *name)
{
    JZ_VEC_FOR(const jazz_model_TableSchema, t, schema->tables)
        if (jz_str_eq(&t->name, name)) return t;
    return NULL;
}

/* Validate strategy-defined operation coordinates before a transaction can
 * become durable. Operation identity is not opaque provenance: its canonical
 * spelling is part of merge deduplication, so a received or recovered record
 * must be checked against the admitted schema rather than deferred until a
 * later contribution calculation happens to read it. */
jz_err jazz_node__node__NodeState__validate_contribution_merge_operation_identities(const jazz_node_NodeState *self, const jazz_model_Transaction *tx)
{
    if (!tx->has_contribution_merge) return JZ_OK;
    const jazz_model_ContributionMergeProvenance *provenance = &tx->contribution_merge;
    jz_err e = jazz_model__tx__ContributionMergeProvenance__validate(provenance);
    if (e.code) {
        jz_err_drop(&e);
        return JZN_INVALID_STORED("transaction contribution provenance must be canonical");
    }
    /* Validate every branch key structurally against its *authored* physical
     * table before any durable codec calls `canonical_bytes`. Raw input must
     * become a malformed rejection, never an encoder panic. */
    JZ_VEC_FOR(const jazz_model_BranchWriteIntent, intent, provenance->branch_write_intents) {
        const jazz_protocol_SchemaVersion *catalogue_schema = jz_btree_get(&self->catalogue.catalogue_schemas, &intent->authored_schema);
        if (!catalogue_schema) return JZN_INVALID_STORED("branch write intent schema is unknown");
        const jazz_node_SchemaPhysicalMapping *mapping = jz_btree_get(&self->catalogue.physical_mappings, &intent->authored_schema);
        if (!mapping) return JZN_INVALID_STORED("branch write intent physical mapping is missing");
        const jz_str *table_name = NULL;
        {
            const jz_str *name; const jazz_node_PhysicalTableMapping *table;
            JZ_BTREE_FOR(&mapping->tables, name, table)
                if (table->table_id == intent->physical_table_id) { table_name = name; break; }
        }
        if (!table_name) return JZN_INVALID_STORED("branch write intent table is unknown");
        const jazz_model_TableSchema *table = find_table(&catalogue_schema->schema, table_name);
        if (!table) return JZN_INVALID_STORED("branch write intent table schema is missing");
        e = jazz_model__schema__JazzSchema__validate_authored_branch_key(&catalogue_schema->schema, table, &intent->head);
        if (e.code) return jazz_node__node__Error_invalid_branch_key(e);
        if (intent->operation.tag == JZ_BWO_VIEW_UPDATE_COPY) {
            const jazz_model_BranchViewCopyEvidence *evidence = &intent->operation.evidence;
            if (!jz_str_eq(&evidence->table, table_name)
                || !jz_uuid_eq(&evidence->row_uuid, &intent->row_uuid)
                || !jazz_model_BranchKey_eq(&evidence->head, &intent->head))
                return JZN_INVALID_STORED("branch write copy evidence is not bound to its intent");
            e = jazz_model__schema__JazzSchema__validate_authored_branch_key(&catalogue_schema->schema, table, &evidence->head);
            if (e.code) return jazz_node__node__Error_invalid_branch_key(e);
            /* Current(base) => base; Snapshot { branch, .. } => branch */
            const jazz_model_BranchKey *base = &evidence->base.branch;
            e = jazz_model__schema__JazzSchema__validate_authored_branch_key(&catalogue_schema->schema, table, base);
            if (e.code) return jazz_node__node__Error_invalid_branch_key(e);
        }
    }
    /* Linear history has no cross-branch contribution calculator, so nothing
     * legitimately produces contribution substitutions. */
    if (provenance->substitutions.len != 0)
        return JZN_INVALID_STORED("contribution substitutions require the removed contribution calculator");
    return JZ_OK;
}
