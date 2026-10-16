/* Port of crates/jazz/layers/node/src/node/row_availability.rs */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

typedef jazz_protocol_CurrentRowCoordinate Coord;

bool jazz_node__node__row_availability__NodeState__can_mint_current_row_receipts(const NS *self) {
    return self->authoritative_scalar_exit_refresh && jazz_node__node__NodeState__client_relay_scope(self) == NULL;
}

jz_err jazz_node__node__row_availability__NodeState__current_row_coordinate(const NS *self, const jz_str *table, jazz_types_RowUuid row, Coord *out) {
    jazz_types_SchemaVersionId schema = self->catalogue.local_schema_version_id;
    const jazz_node_SchemaPhysicalMapping *mapping = jz_btree_get(&self->catalogue.physical_mappings, &schema);
    jazz_types_GlobalPhysicalTableId physical_table;
    if (!mapping || !jazz_protocol__protocol__PhysicalIdentityManifest__table_id(mapping->identities, table, &physical_table))
        return jz_err_fmt(JZN_E_TABLE_NOT_FOUND, "%.*s", (int)table->len, table->ptr);
    out->schema = schema;
    out->table = jz_str_clone(table);
    out->physical_table = physical_table;
    out->row = row;
    return JZ_OK;
}

static bool coordinate_matches(const NS *self, const Coord *coordinate) {
    Coord c;
    jz_err e = jazz_node__node__row_availability__NodeState__current_row_coordinate(self, &coordinate->table, coordinate->row, &c);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    bool eq = jazz_protocol__protocol__CurrentRowCoordinate__eq(&c, coordinate);
    jz_str_drop(&c.table);
    return eq;
}

static void carrier_drop_v(void *p) { jazz_protocol__protocol__VersionCarrier__drop(*(jazz_protocol_VersionCarrier **)p); }
static void vrow_drop_v(void *p) { jazz_node__node__codec__VersionRow__drop(*(jazz_node_VersionRow **)p); }

jz_err jazz_node__node__row_availability__NodeState__evaluate_current_rows(NS *self, const jazz_protocol_CurrentRowsRequest *request,
                                                                          jazz_protocol_PolicyBindingKey *context, u64 core_epoch, u64 progress,
                                                                          jazz_protocol_CurrentRowsReceipt *receipt) {
    memset(receipt, 0, sizeof *receipt);
    receipt->request_id = request->request_id;
    jz_vec_clone(&receipt->rows, &request->rows, sizeof(Coord), jazz_protocol__protocol__CurrentRowCoordinate__clone);
    for (u32 i = 0; i < request->rows.len; i++) JZ_VEC_PUSH(receipt->outcomes, JAZZ_CRO_UNKNOWN);
    receipt->context = jazz_protocol__protocol__PolicyBindingKey__clone(context);
    receipt->core = jazz_node__node__NodeState__node_uuid(self);
    receipt->core_epoch = core_epoch;
    jazz_types_AuthorSubject identity = *jazz_protocol__protocol__PolicyBindingKey__identity(context);
    receipt->claims_revision = jazz_node__node__NodeState__session_claim_revision(self, identity);
    receipt->policy_epoch = jazz_node__node__NodeState__active_catalogue_seq(self);
    receipt->settled_through = jazz_node__node__NodeState__committed_global_time(self);
    receipt->authorization_progress = progress;
    if (!jazz_node__node__row_availability__NodeState__can_mint_current_row_receipts(self) || !jazz_node__node__NodeState__permissions_ready(self))
        return JZ_OK;
    for (u32 index = 0; index < request->rows.len; index++) {
        const Coord *coordinate = &request->rows.ptr[index];
        if (!coordinate_matches(self, coordinate)) continue;
        const jazz_model_TableSchema *ts;
        JZ_TRY(jazz_node__node__NodeState__table(self, &coordinate->table, &ts));
        if (ts->branch_by.len != 0) continue;
        jazz_model_ValidatedQuery *shape; jazz_model_Binding *binding;
        JZ_TRY(jazz_node__node__views__NodeState__whole_table_shape_binding(self, &coordinate->table, &shape, &binding));
        jazz_node_CurrentRowVec rows = {0};
        JZ_TRY(jazz_node__node__query_eval__NodeState__query_readable_current_row_including_deleted(self, shape, binding, JAZZ_DUR_GLOBAL, identity, coordinate->row, &rows));
        bool found = false;
        for (u32 i = 0; i < rows.len; i++) {
            jazz_types_RowUuid r = jazz_node__node__CurrentRow__row_uuid(&rows.ptr[i]);
            if (jz_uuid_eq(&r, &coordinate->row)) { found = true; break; }
        }
        jz_vec_drop(&rows, sizeof(jazz_node_CurrentRow), jazz_node__node__CurrentRow__drop);
        if (!found) { receipt->outcomes.ptr[index] = JAZZ_CRO_CURRENT_UNAVAILABLE; continue; }
        jazz_types_PhysicalTableId table_id;
        JZ_TRY(jazz_node__node__physical__NodeState__physical_table_id_for_schema(self, coordinate->schema, &coordinate->table, &table_id));
        jazz_model_TxId tx_id;
        if (!jazz_node__node__global_state__NodeState__visible_global_tx_id_for_physical_table_now(self, table_id, coordinate->row, &tx_id)) continue;
        JZ_VEC(jazz_protocol_VersionCarrier *) carriers = {0};
        /* transactions = BTreeSet::from([tx_id]) */
        jazz_model_TxId transactions[1] = { tx_id };
        for (u32 t = 0; t < 1; t++) {
            jazz_model_TxId tx = transactions[t];
            jazz_node_VersionRowVec all = {0}, versions = {0};
            JZ_TRY(jazz_node__node__currency__NodeState__query_versions_for_tx(self, tx, &all));
            for (u32 i = 0; i < all.len; i++) {
                jazz_node_VersionRow *version = all.ptr[i];
                jazz_types_RowUuid r = jazz_node__node__codec__VersionRow__row_uuid(version);
                jazz_types_PhysicalTableId vt;
                bool keep = jz_uuid_eq(&r, &coordinate->row);
                if (keep) {
                    jz_err e = jazz_node__node__physical__NodeState__physical_table_id_for_version(self, version, &vt);
                    keep = !JZ_IS_ERR(e) && vt == table_id;
                    jz_err_drop(&e);
                }
                if (keep) JZ_VEC_PUSH(versions, version); else jazz_node__node__codec__VersionRow__drop(version);
            }
            jz_free(all.ptr);
            bool branched = false;
            for (u32 i = 0; i < versions.len; i++)
                if (!jazz_model__model__branch__BranchKey__values_is_empty(jazz_node__node__codec__VersionRow__branch_key(versions.ptr[i]))) branched = true;
            if (versions.len == 0 || branched) {
                jz_vec_drop(&versions, sizeof(void *), vrow_drop_v);
                jz_vec_truncate(&carriers, 0, sizeof(void *), carrier_drop_v);
                break;
            }
            bool have; jazz_node_StoredTransaction stored;
            jz_err e = jazz_node__node__currency__NodeState__query_transaction(self, tx, &have, &stored);
            if (JZ_IS_ERR(e)) { jz_vec_drop(&versions, sizeof(void *), vrow_drop_v); return e; }
            if (!have) { jz_vec_drop(&versions, sizeof(void *), vrow_drop_v); return jazz_node__node__Error__missing_transaction(&tx); }
            jazz_protocol_VersionBundle bundle;
            e = jazz_node__node__views__NodeState__version_bundle_for_maintained_view_versions_with_tx(self, &stored, versions.ptr, versions.len, &bundle);
            jazz_node__node__codec__StoredTransaction__drop(&stored);
            jz_vec_drop(&versions, sizeof(void *), vrow_drop_v);
            JZ_TRY(e);
            bundle.scope = JAZZ_BUNDLE_SCOPE_VIEW_SCOPED;
            JZ_VEC_PUSH(carriers, jazz_protocol__protocol__VersionCarrier__bundle(bundle));
        }
        if (carriers.len != 0) {
            receipt->outcomes.ptr[index] = JAZZ_CRO_READABLE;
            jz_vec_extend(&receipt->version_carriers, carriers.ptr, carriers.len, sizeof(void *));
        }
        jz_free(carriers.ptr);
    }
    return JZ_OK;
}

static jz_err invalid(void) { return JZN_MALFORMED_VIEW("current row receipt does not match requested coordinates"); }

jz_err jazz_node__node__row_availability__NodeState__ingest_current_rows_receipt(NS *self, const jazz_protocol_CurrentRowsReceipt *receipt) {
    if (receipt->rows.len != receipt->outcomes.len) return invalid();
    jazz_protocol_VersionBundleVec bundles = {0};
    jz_err e = jazz_protocol__protocol__expand_version_carriers(receipt->version_carriers.ptr, receipt->version_carriers.len, &bundles);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return invalid(); }
    jz_btree content_covered;
    jz_btree_init(&content_covered, &jzn_desc_u64_set);
    JZ_VEC(jazz_protocol_RowVersionRef) requests = {0};
    for (u32 b = 0; b < bundles.len; b++) {
        const jazz_protocol_VersionBundle *bundle = &bundles.ptr[b];
        if (bundle->scope != JAZZ_BUNDLE_SCOPE_VIEW_SCOPED || bundle->versions.len == 0 || !bundle->has_global_time ||
            bundle->global_time > receipt->settled_through) { e = invalid(); goto fail; }
        for (u32 v = 0; v < bundle->versions.len; v++) {
            const jazz_protocol_VersionRecord *version = bundle->versions.ptr[v];
            jazz_types_SchemaVersionId sv = jazz_protocol__protocol__VersionRecord__schema_version(version);
            const jazz_node_SchemaPhysicalMapping *mapping = jz_btree_get(&self->catalogue.physical_mappings, &sv);
            jazz_types_GlobalPhysicalTableId physical;
            if (!mapping || !jazz_protocol__protocol__PhysicalIdentityManifest__table_id(mapping->identities, jazz_protocol__protocol__VersionRecord__table(version), &physical)) { e = invalid(); goto fail; }
            jazz_types_RowUuid vr = jazz_protocol__protocol__VersionRecord__row_uuid(version);
            u64 index = 0; const Coord *coordinate = NULL;
            for (u32 i = 0; i < receipt->rows.len; i++)
                if (jz_uuid_eq(&receipt->rows.ptr[i].physical_table, &physical) && jz_uuid_eq(&receipt->rows.ptr[i].row, &vr)) { index = i; coordinate = &receipt->rows.ptr[i]; break; }
            if (!coordinate) { e = invalid(); goto fail; }
            if (receipt->outcomes.ptr[index] != JAZZ_CRO_READABLE ||
                !jazz_model__model__branch__BranchKey__values_is_empty(jazz_protocol__protocol__VersionRecord__branch_key(version)) ||
                !coordinate_matches(self, coordinate)) { e = invalid(); goto fail; }
            jz_btree_insert(&content_covered, &index, NULL, NULL);
            jazz_model_TxId tx = jazz_model__tx__Transaction__tx_id(bundle->tx);
            JZ_VEC_PUSH(requests, jazz_protocol__protocol__RowVersionRef__new(jz_str_clone(&coordinate->table), coordinate->row, tx));
        }
    }
    for (u32 i = 0; i < receipt->outcomes.len; i++) {
        u64 idx = i;
        if (receipt->outcomes.ptr[i] == JAZZ_CRO_READABLE && !jz_btree_contains(&content_covered, &idx)) { e = invalid(); goto fail; }
    }
    jz_btree_drop(&content_covered);
    e = jazz_node__node__NodeState__apply_row_version_payloads_for_requests(self, requests.ptr, requests.len, &bundles);
    for (u32 i = 0; i < requests.len; i++) jz_str_drop(&requests.ptr[i].table);
    jz_free(requests.ptr);
    return e;
fail:
    jz_btree_drop(&content_covered);
    for (u32 i = 0; i < requests.len; i++) jz_str_drop(&requests.ptr[i].table);
    jz_free(requests.ptr);
    jz_vec_drop(&bundles, sizeof(jazz_protocol_VersionBundle), jazz_protocol__protocol__VersionBundle__drop);
    return e;
}
