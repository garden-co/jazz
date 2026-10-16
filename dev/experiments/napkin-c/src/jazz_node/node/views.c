/* views.c: port of crates/jazz/layers/node/src/node/views.rs
 *
 * View-update construction for subscribers and sync peers. This module owns
 * current-row and query-result bundle assembly, closure expansion, settled
 * canonical binding-view result-set/completeness state, and deduplicated
 * version shipping; per-peer shipped state lives in peer.c, policy filtering
 * in policy.c, and query execution/planning in query_eval. It sits on the
 * node side of the protocol boundary and emits SyncMessage values.
 *
 * Napkin-port notes: async fns are synchronous; Option<VersionRow> is a
 * nullable owned VersionRow*; BTreeMap/BTreeSet are jz_btree with per-key
 * descriptors defined below. #[cfg(test)] helpers are not ported. */
#include "jazz_node/node/mod.h"

typedef jazz_protocol_VersionRecord VR;
typedef jazz_node_VersionRow VRow;
typedef jazz_model_TxId TxId;
typedef jazz_protocol_SupportingRow SRow;
typedef jazz_protocol_VersionBundle Bundle;
typedef jazz_protocol_VersionBundleRef BundleRef;

#define TXID(tx) jazz_model__tx__Transaction__tx_id(tx)
#define VROW_TABLE(v) jazz_node__node__codec__VersionRow__table(v)
#define VROW_ROW(v) jazz_node__node__codec__VersionRow__row_uuid(v)

/* ---------------- small owned-pointer helpers ---------------- */
static void vr_drop_v(void *p) { jazz_protocol__protocol__VersionRecord__drop(*(VR **)p); }
static void vrow_drop_v(void *p) { jazz_node__node__codec__VersionRow__drop(*(VRow **)p); }
static void vrow_vec_drop(jazz_node_VersionRowVec *v) { jz_vec_drop(v, sizeof(VRow *), vrow_drop_v); }
static void vrow_vec_drop_v(void *p) { vrow_vec_drop(p); }
static void vr_vec_drop(jazz_protocol_VersionRecordVec *v) { jz_vec_drop(v, sizeof(VR *), vr_drop_v); }
static void btree_drop_v(void *p) { jz_btree_drop(p); }
static void bundle_drop_v(void *p) { jazz_protocol__protocol__VersionBundle__drop(p); }
static void vrow_vec_clone(jazz_node_VersionRowVec *dst, const jazz_node_VersionRowVec *src) {
    *dst = (jazz_node_VersionRowVec){ 0 };
    for (u32 i = 0; i < src->len; i++) JZ_VEC_PUSH(*dst, jazz_node__node__codec__VersionRow__clone(src->ptr[i]));
}

static const jz_btree_desc desc_txid_set = { sizeof(TxId), 0, jzn_cmp_txid, NULL, NULL };
static const jz_btree_desc desc_tx_bundles = { sizeof(TxId), sizeof(Bundle), jzn_cmp_txid, NULL, bundle_drop_v };
static const jz_btree_desc desc_tx_versions = { sizeof(TxId), sizeof(jazz_node_VersionRowVec), jzn_cmp_txid, NULL, vrow_vec_drop_v };

/* (String, RowUuid) */
typedef struct { jz_str table; jazz_types_RowUuid row; } TableRow;
static int table_row_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const TableRow *x = a, *y = b;
    int c = jz_str_cmp(&x->table, &y->table);
    return c ? c : jz_uuid_cmp(&x->row, &y->row);
}
static void table_row_drop(void *p) { jz_str_drop(&((TableRow *)p)->table); }
static const jz_btree_desc desc_table_row_set = { sizeof(TableRow), 0, table_row_cmp, table_row_drop, NULL };
static const jz_btree_desc desc_tx_table_rows = { sizeof(TxId), sizeof(jz_btree), jzn_cmp_txid, NULL, btree_drop_v };
static bool table_row_insert(jz_btree *set, const jz_str *table, jazz_types_RowUuid row) {
    TableRow probe = { *table, row };
    if (jz_btree_contains(set, &probe)) return false;
    TableRow k = { jz_str_clone(table), row };
    jz_btree_insert(set, &k, NULL, NULL);
    return true;
}
static bool table_row_contains(const jz_btree *set, const jz_str *table, jazz_types_RowUuid row) {
    TableRow probe = { *table, row };
    return jz_btree_contains(set, &probe);
}
static jz_btree *tx_table_rows_entry(jz_btree *by_tx, TxId tx) {
    bool inserted = false;
    jz_btree *set = jz_btree_entry(by_tx, &tx, &inserted);
    if (inserted) jz_btree_init(set, &desc_table_row_set);
    return set;
}

/* (String, RowUuid, TxId) */
typedef struct { jz_str table; jazz_types_RowUuid row; TxId tx; } TableRowTx;
static int table_row_tx_cmp(const void *a, const void *b, void *ctx) {
    const TableRowTx *x = a, *y = b;
    int c = table_row_cmp(a, b, ctx);
    return c ? c : jazz_model_TxId_cmp(&x->tx, &y->tx);
}
static const jz_btree_desc desc_table_row_tx_set = { sizeof(TableRowTx), 0, table_row_tx_cmp, table_row_drop, NULL };
static void table_row_tx_insert(jz_btree *set, const jz_str *table, jazz_types_RowUuid row, TxId tx) {
    TableRowTx probe = { *table, row, tx };
    if (jz_btree_contains(set, &probe)) return;
    TableRowTx k = { jz_str_clone(table), row, tx };
    jz_btree_insert(set, &k, NULL, NULL);
}
static bool table_row_tx_contains(const jz_btree *set, const jz_str *table, jazz_types_RowUuid row, TxId tx) {
    TableRowTx probe = { *table, row, tx };
    return jz_btree_contains(set, &probe);
}

/* ResultRowEntry = (Intern<String>, RowUuid, TxId), borrowed from its member */
typedef struct { const jz_str *table; jazz_types_RowUuid row; TxId tx; } RowEntry;
typedef JZ_VEC(RowEntry) RowEntryVec;

/* ---------------- error helpers ---------------- */
static jz_err invalid_closure(jazz_protocol_SubscriptionKey subscription, const char *transition, u32 transition_len) {
    jz_str d = { 0 };
    jazz_protocol__protocol__SubscriptionKey__debug(&d, &subscription);
    jz_err e = jz_err_fmt(JZN_E_INVALID_AUTHORITY_SOURCE_CLOSURE, "%.*s: %.*s", (int)d.len, d.ptr, (int)transition_len, transition);
    jz_str_drop(&d);
    return e;
}
static jz_err invalid_closure_c(jazz_protocol_SubscriptionKey subscription, const char *transition) {
    return invalid_closure(subscription, transition, (u32)strlen(transition));
}
static bool err_is_stored(const jz_err *e, const char *msg) {
    return e->code == JZN_E_INVALID_STORED_VALUE && e->msg && jz_str_eq_cstr(e->msg, msg);
}
static bool err_is_unregistered_subscription(const jz_err *e) {
    return err_is_stored(e, "subscription referenced unregistered shape") || err_is_stored(e, "subscription referenced unregistered binding");
}

/* Preserve the usage-site handle while converting a structurally malformed
 * authority payload into the only client-visible source-closure diagnostic.
 * Operational storage, transaction, and compiler errors must continue to
 * fail the peer tick rather than being misreported as a terminal stream
 * error. */
static jz_err invalid_authority_source_closure_error(jazz_protocol_SubscriptionKey subscription, jz_err error) {
    if (error.code == JZN_E_MALFORMED_VIEW_UPDATE) {
        jz_str transition = jz_format("authority source-closure payload failed validation: %.*s",
                                      error.msg ? (int)error.msg->len : 0, error.msg ? error.msg->ptr : "");
        jz_err_drop(&error);
        jz_err out = invalid_closure(subscription, transition.ptr, transition.len);
        jz_str_drop(&transition);
        return out;
    }
    return error; /* InvalidAuthoritySourceClosure and everything else pass through */
}
#define TRY_CLOSURE(sub, expr) do { jz_err _e = (expr); if (_e.code) return invalid_authority_source_closure_error((sub), _e); } while (0)

/* Name one supporting version in a source-closure diagnostic: the table,
 * row and exact version, so a rejected transition says which coordinate
 * disagreed with the receiver's retained predecessor. */
static jz_str supporting_row_diagnostic(const SRow *row) {
    char uuid[37];
    jz_uuid_to_string(&row->row, uuid);
    jz_str tx = { 0 };
    jazz_model__tx__TxId__debug(&tx, &row->version.tx);
    static const char *layers[] = { "Content", "Deletion", "ContentOrDeletion" };
    jz_str out = jz_format("%.*s %s %s tx %.*s", (int)row->version_table->len, row->version_table->ptr, uuid,
                           layers[row->version.layer], (int)tx.len, tx.ptr);
    jz_str_drop(&tx);
    return out;
}

/* ---------------- (String, BranchKey, RowUuid, SchemaVersionId, bool) record key ---------------- */
typedef struct {
    jz_str table;
    jazz_model_BranchKey *branch;
    jazz_types_RowUuid row;
    jazz_types_SchemaVersionId schema_version;
    bool deletion;
} RecordKey;
static int record_key_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const RecordKey *x = a, *y = b;
    int c = jz_str_cmp(&x->table, &y->table); if (c) return c;
    c = jazz_model__model__branch__BranchKey__cmp(x->branch, y->branch); if (c) return c;
    c = jz_uuid_cmp(&x->row, &y->row); if (c) return c;
    c = jz_uuid_cmp(&x->schema_version, &y->schema_version); if (c) return c;
    return (int)x->deletion - (int)y->deletion;
}
static void record_key_drop(void *p) {
    RecordKey *k = p;
    jz_str_drop(&k->table);
    jazz_model__model__branch__BranchKey__drop(k->branch);
}
static const jz_btree_desc desc_record_map = { sizeof(RecordKey), sizeof(VR *), record_key_cmp, record_key_drop, vr_drop_v };
static const jz_btree_desc desc_record_ref_map = { sizeof(RecordKey), sizeof(const VR *), record_key_cmp, record_key_drop, NULL };

static RecordKey version_bundle_record_key(const VR *version) {
    RecordKey k;
    k.table = jz_str_clone(jazz_protocol__protocol__VersionRecord__table(version));
    k.branch = jazz_model__model__branch__BranchKey__clone(jazz_protocol__protocol__VersionRecord__branch_key(version));
    k.row = jazz_protocol__protocol__VersionRecord__row_uuid(version);
    k.schema_version = jazz_protocol__protocol__VersionRecord__schema_version(version);
    k.deletion = jazz_protocol__protocol__VersionRecord__deletion(version) != JZN_DELETION_NONE;
    return k;
}

/* (String, BranchKey, RowUuid) */
typedef struct { jz_str table; jazz_model_BranchKey *branch; jazz_types_RowUuid row; } ViewVersionKey;
static int view_version_key_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const ViewVersionKey *x = a, *y = b;
    int c = jz_str_cmp(&x->table, &y->table); if (c) return c;
    c = jazz_model__model__branch__BranchKey__cmp(x->branch, y->branch); if (c) return c;
    return jz_uuid_cmp(&x->row, &y->row);
}
static void view_version_key_drop(void *p) {
    ViewVersionKey *k = p;
    jz_str_drop(&k->table);
    jazz_model__model__branch__BranchKey__drop(k->branch);
}
static const jz_btree_desc desc_view_version_key_set = { sizeof(ViewVersionKey), 0, view_version_key_cmp, view_version_key_drop, NULL };
static void view_version_key_insert(jz_btree *set, const VR *version) {
    ViewVersionKey k = {
        jz_str_clone(jazz_protocol__protocol__VersionRecord__table(version)),
        jazz_model__model__branch__BranchKey__clone(jazz_protocol__protocol__VersionRecord__branch_key(version)),
        jazz_protocol__protocol__VersionRecord__row_uuid(version),
    };
    if (jz_btree_contains(set, &k)) { view_version_key_drop(&k); return; }
    jz_btree_insert(set, &k, NULL, NULL);
}

/* ---------------- free functions ---------------- */
static void apply_covered_input_closure_admission_delta(jazz_node_AuthorityResultState *state, bool cleared,
                                                        const SRow *adds, u32 n_adds, const SRow *removes, u32 n_removes) {
    if (cleared) jz_btree_clear(&state->covered_input_versions);
    for (u32 i = 0; i < n_removes; i++) {
        jazz_node_CoveredInputCoordinate c;
        jazz_node__node__CoveredInputCoordinate__From__from(&c, &removes[i]);
        jazz_node_CoveredInputCoordinate ok; SRow ov;
        if (jz_btree_remove(&state->covered_input_versions, &c, &ok, &ov)) {
            jazz_node__node__CoveredInputCoordinate__drop(&ok);
            jazz_protocol__protocol__SupportingRow__drop(&ov);
        }
        jazz_node__node__CoveredInputCoordinate__drop(&c);
    }
    for (u32 i = 0; i < n_adds; i++) {
        jazz_node_CoveredInputCoordinate c;
        jazz_node__node__CoveredInputCoordinate__From__from(&c, &adds[i]);
        SRow row; jazz_protocol__protocol__SupportingRow__clone(&row, &adds[i]);
        SRow old;
        if (jz_btree_insert(&state->covered_input_versions, &c, &row, &old)) {
            jazz_node__node__CoveredInputCoordinate__drop(&c); /* key kept */
            jazz_protocol__protocol__SupportingRow__drop(&old);
        }
    }
}

static bool maintained_view_tx_versions_contain_winner(const jazz_node_VersionRowVec *tx_versions, const VRow *winner) {
    jazz_types_RowUuid row = VROW_ROW(winner);
    for (u32 i = 0; i < tx_versions->len; i++) {
        const VRow *candidate = tx_versions->ptr[i];
        jazz_types_RowUuid c_row = VROW_ROW(candidate);
        if (jz_str_eq(VROW_TABLE(candidate), VROW_TABLE(winner)) && jz_uuid_eq(&c_row, &row)) return true;
    }
    return false;
}

static jz_err canonical_receiver_bundle(BundleRef bundle, Bundle *out) {
    TxId tx_id = TXID(bundle.tx);
    jz_btree versions; jz_btree_init(&versions, &desc_record_map);
    for (u32 i = 0; i < bundle.n_versions; i++) {
        const VR *version = bundle.versions[i];
        RecordKey key = version_bundle_record_key(version);
        VR **existing = jz_btree_get(&versions, &key);
        if (existing) {
            bool same = jazz_protocol__protocol__VersionRecord__eq(*existing, version);
            record_key_drop(&key);
            if (!same) { jz_btree_drop(&versions); return jazz_node__node__Error__conflicting_commit_unit(&tx_id); }
        } else {
            VR *copy = jazz_protocol__protocol__VersionRecord__clone(version);
            jz_btree_insert(&versions, &key, &copy, NULL);
        }
    }
    jazz_protocol_VersionRecordVec collected = { 0 };
    RecordKey k; VR *v;
    while (jz_btree_pop_first(&versions, &k, &v)) { record_key_drop(&k); JZ_VEC_PUSH(collected, v); }
    jz_btree_drop(&versions);
    if (jazz_model__tx__Transaction__n_total_writes(bundle.tx) != collected.len) {
        vr_vec_drop(&collected);
        return jazz_node__node__Error__conflicting_commit_unit(&tx_id);
    }
    jazz_protocol__protocol__VersionBundleRef__to_owned_bundle(bundle, out);
    jz_vec_drop(&out->versions, sizeof(VR *), vr_drop_v);
    out->versions.ptr = collected.ptr; out->versions.len = collected.len; out->versions.cap = collected.cap;
    /* A served view carries durable provenance, never the sender's local
     * policy capability. Normalize before merged-bundle identity checks too. */
    jazz_model_Transaction *redacted = jazz_node__node__codec__transaction_without_permission_subject(out->tx);
    jazz_model__tx__Transaction__drop(out->tx);
    out->tx = redacted;
    return JZ_OK;
}

static jz_err validate_version_subset(VR *const *subset, u32 n_subset, VR *const *complete, u32 n_complete, TxId tx_id) {
    jz_btree by_key; jz_btree_init(&by_key, &desc_record_ref_map);
    for (u32 i = 0; i < n_complete; i++) {
        RecordKey key = version_bundle_record_key(complete[i]);
        const VR *ref = complete[i];
        if (jz_btree_insert(&by_key, &key, &ref, NULL)) record_key_drop(&key);
    }
    jz_err e = JZ_OK;
    for (u32 i = 0; i < n_subset && !e.code; i++) {
        RecordKey key = version_bundle_record_key(subset[i]);
        const VR **found = jz_btree_get(&by_key, &key);
        if (!found || !jazz_protocol__protocol__VersionRecord__eq(*found, subset[i])) e = jazz_node__node__Error__conflicting_commit_unit(&tx_id);
        record_key_drop(&key);
    }
    jz_btree_drop(&by_key);
    return e;
}

static jz_err merge_receiver_version_bundle_ref(jz_btree *bundles /* TxId -> VersionBundle */, BundleRef bundle) {
    Bundle incoming;
    JZ_TRY(canonical_receiver_bundle(bundle, &incoming));
    TxId tx_id = TXID(bundle.tx);
    Bundle *existing = jz_btree_get(bundles, &tx_id);
    if (!existing) {
        jz_btree_insert(bundles, &tx_id, &incoming, NULL);
        return JZ_OK;
    }
    jazz_model_Transaction *existing_tx_identity = jazz_model__tx__Transaction__clone(existing->tx);
    jazz_model__tx__Transaction__set_n_total_writes(existing_tx_identity, 0);
    jazz_model_Transaction *incoming_tx_identity = jazz_model__tx__Transaction__clone(incoming.tx);
    jazz_model__tx__Transaction__set_n_total_writes(incoming_tx_identity, 0);
    bool conflict = !jazz_model__tx__Transaction__eq(existing_tx_identity, incoming_tx_identity)
        || !jazz_model__tx__Fate__eq(&existing->fate, &incoming.fate)
        || existing->has_global_time != incoming.has_global_time
        || (existing->has_global_time && existing->global_time != incoming.global_time)
        || existing->durability != incoming.durability;
    jazz_model__tx__Transaction__drop(existing_tx_identity);
    jazz_model__tx__Transaction__drop(incoming_tx_identity);
    jz_err e = JZ_OK;
    jz_btree seen; jz_btree_init(&seen, &desc_record_map);
    if (conflict) { e = jazz_node__node__Error__conflicting_commit_unit(&tx_id); goto out; }
    for (u32 i = 0; i < existing->versions.len; i++) {
        RecordKey key = version_bundle_record_key(existing->versions.ptr[i]);
        VR *copy = jazz_protocol__protocol__VersionRecord__clone(existing->versions.ptr[i]);
        VR *old;
        if (jz_btree_insert(&seen, &key, &copy, &old)) { record_key_drop(&key); jazz_protocol__protocol__VersionRecord__drop(old); }
    }
    for (u32 i = 0; i < incoming.versions.len; i++) {
        const VR *version = incoming.versions.ptr[i];
        RecordKey key = version_bundle_record_key(version);
        VR **found = jz_btree_get(&seen, &key);
        if (found) {
            bool same = jazz_protocol__protocol__VersionRecord__eq(*found, version);
            record_key_drop(&key);
            if (!same) { e = jazz_node__node__Error__conflicting_commit_unit(&tx_id); goto out; }
        } else {
            VR *copy = jazz_protocol__protocol__VersionRecord__clone(version);
            jz_btree_insert(&seen, &key, &copy, NULL);
        }
    }
    if (existing->scope == JAZZ_BUNDLE_SCOPE_COMPLETE && incoming.scope == JAZZ_BUNDLE_SCOPE_COMPLETE) {
        bool same = jazz_model__tx__Transaction__n_total_writes(existing->tx) == jazz_model__tx__Transaction__n_total_writes(incoming.tx)
            && existing->versions.len == incoming.versions.len;
        for (u32 i = 0; same && i < existing->versions.len; i++)
            same = jazz_protocol__protocol__VersionRecord__eq(existing->versions.ptr[i], incoming.versions.ptr[i]);
        if (!same) e = jazz_node__node__Error__conflicting_commit_unit(&tx_id);
    } else if (existing->scope == JAZZ_BUNDLE_SCOPE_COMPLETE && incoming.scope == JAZZ_BUNDLE_SCOPE_VIEW_SCOPED) {
        e = validate_version_subset(incoming.versions.ptr, incoming.versions.len, existing->versions.ptr, existing->versions.len, tx_id);
    } else if (existing->scope == JAZZ_BUNDLE_SCOPE_VIEW_SCOPED && incoming.scope == JAZZ_BUNDLE_SCOPE_COMPLETE) {
        e = validate_version_subset(existing->versions.ptr, existing->versions.len, incoming.versions.ptr, incoming.versions.len, tx_id);
        if (!e.code) {
            jazz_protocol__protocol__VersionBundle__drop(existing);
            *existing = incoming;
            incoming = (Bundle){ 0 };
        }
    } else {
        jz_vec_drop(&existing->versions, sizeof(VR *), vr_drop_v);
        RecordKey k; VR *v;
        while (jz_btree_pop_first(&seen, &k, &v)) { record_key_drop(&k); JZ_VEC_PUSH(existing->versions, v); }
        jazz_model__tx__Transaction__set_n_total_writes(existing->tx, existing->versions.len);
    }
out:
    jz_btree_drop(&seen);
    if (incoming.tx) jazz_protocol__protocol__VersionBundle__drop(&incoming);
    return e;
}

static jz_err version_bundle_refs_for_carriers(jazz_protocol_VersionCarrier *const *carriers, u32 n, jazz_protocol_VersionBundleRefVec *out) {
    *out = (jazz_protocol_VersionBundleRefVec){ 0 };
    jz_vec_reserve(out, n, sizeof(BundleRef));
    for (u32 i = 0; i < n; i++) {
        jazz_protocol_VersionBundleRefVec refs = { 0 };
        jz_err e = jazz_protocol__protocol__VersionCarrier__bundle_refs(carriers[i], &refs);
        if (e.code) {
            jz_err_drop(&e);
            jz_free(out->ptr); *out = (jazz_protocol_VersionBundleRefVec){ 0 };
            return JZN_MALFORMED_VIEW("malformed version-bundle run");
        }
        jz_vec_extend(out, refs.ptr, refs.len, sizeof(BundleRef));
        jz_free(refs.ptr);
    }
    return JZ_OK;
}

static jz_err content_row_members_for_bundle(const jazz_protocol_ResultMemberEntryVec *members, const char *context, RowEntryVec *out) {
    *out = (RowEntryVec){ 0 };
    for (u32 i = 0; i < members->len; i++) {
        const jazz_protocol_ResultMemberEntry *member = &members->ptr[i];
        RowEntry entry;
        if (!jazz_protocol__protocol__ResultMemberEntry__as_row_entry(member, &entry.table, &entry.row, &entry.tx)) {
            /* filter: only members whose as_row() is Some reach the map, so
             * the ok_or below is unreachable in practice. */
            const char *reason = context;
            if (member->tag == JAZZ_RME_SYNTHETIC) reason = "synthetic result members require typed payload facts before row bundle shipping";
            else if (member->tag == JAZZ_RME_PATH_TUPLE) reason = "path tuple result members require typed payload facts before row bundle shipping";
            (void)reason;
            continue;
        }
        JZ_VEC_PUSH(*out, entry);
    }
    return JZ_OK;
}

bool jazz_node__node__views__simple_scalar_exit_query(const jazz_model_Query *query) {
    return query->filters.len != 0 && jazz_node__node__views__single_table_scalar_query(query);
}

static bool scalar_operand(const jazz_model_Operand *operand) {
    if (operand->tag != JAZZ_OPERAND_COLUMN) return true;
    for (u32 i = 0; i < operand->name.len; i++) if (operand->name.ptr[i] == '.') return false;
    return !(operand->name.len > 0 && operand->name.ptr[0] == '$');
}
static bool scalar_predicate(const jazz_model_Predicate *filter) {
    switch (filter->tag) {
    case JAZZ_PRED_ALL: case JAZZ_PRED_ANY:
        for (u32 i = 0; i < filter->children.len; i++) if (!scalar_predicate(&filter->children.ptr[i])) return false;
        return true;
    case JAZZ_PRED_NOT: return scalar_predicate(filter->inner);
    case JAZZ_PRED_EQ: case JAZZ_PRED_NE: case JAZZ_PRED_GT: case JAZZ_PRED_GTE:
    case JAZZ_PRED_LT: case JAZZ_PRED_LTE: case JAZZ_PRED_CONTAINS:
        return scalar_operand(&filter->left) && scalar_operand(&filter->right);
    case JAZZ_PRED_IN:
        if (!scalar_operand(&filter->left)) return false;
        for (u32 i = 0; i < filter->values.len; i++) if (!scalar_operand(&filter->values.ptr[i])) return false;
        return true;
    case JAZZ_PRED_IS_NULL: return scalar_operand(&filter->left);
    case JAZZ_PRED_ENUM_MATCH: default: return false;
    }
}

/* One unprojected table with only scalar filters (possibly none): its
 * authority input rows are exactly its result roots. */
bool jazz_node__node__views__single_table_scalar_query(const jazz_model_Query *query) {
    for (u32 i = 0; i < query->filters.len; i++) if (!scalar_predicate(&query->filters.ptr[i])) return false;
    return query->joins.len == 0
        && query->flat_join == NULL
        && query->policy_branches.len == 0
        && query->reachable.len == 0
        && query->inherits.len == 0
        && query->includes.len == 0
        && query->array_subqueries.len == 0
        && !query->has_select
        && query->aggregate == NULL
        && query->relation == NULL
        && !query->has_limit
        && query->offset == 0;
}


/* ViewBundlePreflight { bundles, persisted_tx_ids } */
typedef struct {
    jz_btree bundles;          /* TxId -> VersionBundle */
    jz_btree persisted_tx_ids; /* set<TxId> */
} ViewBundlePreflight;
static void preflight_drop(ViewBundlePreflight *p) { jz_btree_drop(&p->bundles); jz_btree_drop(&p->persisted_tx_ids); }

/* Provenance of version rows selected for a maintained-view wire bundle.
 * Only exact immutable-store reads may skip witness canonicalization. */
typedef enum { MBVS_IVM_WITNESS, MBVS_EXACT_STORAGE } MaintainedBundleVersionSource;

static jz_err version_bundle_for_maintained_view_versions_with_tx_and_source(NS *self, const jazz_node_StoredTransaction *stored_tx,
    VRow *const *tx_versions, u32 n, MaintainedBundleVersionSource source, Bundle *out);

/* ---------------- NodeState: storage-backed winners ---------------- */

/* The row's current image from storage; with `deletion_only`, only an
 * image that carries a deletion event. */
static jz_err storage_backed_current_image(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_model_DurabilityTier tier,
                                           jazz_node_ViewEvaluationContext *context, bool deletion_only, VRow **out) {
    *out = NULL;
    jazz_types_PhysicalTableId table_id;
    JZ_TRY(jazz_node__node__physical__NodeState__physical_table_id_for_schema(self, self->catalogue.local_schema_version_id, table, &table_id));
    bool has_tx = false;
    TxId tx_id;
    switch (tier) {
    /* Global current storage already represents the settled winner. */
    case JAZZ_DUR_GLOBAL:
        has_tx = jazz_node__node__global_state__NodeState__visible_global_tx_id_for_physical_table_now(self, table_id, row_uuid, &tx_id);
        break;
    /* Local reads select the greatest global/ahead register winner. */
    case JAZZ_DUR_LOCAL:
        JZ_TRY(jazz_node__node__NodeState__local_deletion_winner_tx_id(self, table, row_uuid, &has_tx, &tx_id));
        break;
    /* No-tier reads do not have a settled maintained source. */
    case JAZZ_DUR_NONE: has_tx = false; break;
    }
    if (!has_tx) return JZ_OK;
    bool found = false;
    jazz_node_StoredTransaction stored_tx;
    JZ_TRY(jazz_node__node__policy__NodeState__query_transaction_memo(self, tx_id, context, &found, &stored_tx));
    if (!found) return jazz_node__node__Error__missing_transaction(&tx_id);
    jz_btree wanted_row; jz_btree_init(&wanted_row, &desc_table_row_set);
    table_row_insert(&wanted_row, table, row_uuid);
    jazz_node_VersionRowVec versions = { 0 };
    jz_err e = jazz_node__node__currency__NodeState__query_versions_for_tx_rows_by_alias(self, tx_id, stored_tx.node_alias, &wanted_row, &versions);
    jz_btree_drop(&wanted_row);
    jazz_node__node__codec__StoredTransaction__drop(&stored_tx);
    if (e.code) return e;
    for (u32 i = 0; i < versions.len; i++) {
        jazz_model_DeletionEvent ev;
        if (!deletion_only || jazz_node__node__codec__VersionRow__deletion(versions.ptr[i], &ev)) {
            *out = versions.ptr[i];
            versions.ptr[i] = NULL;
            break;
        }
    }
    for (u32 i = 0; i < versions.len; i++) if (versions.ptr[i]) jazz_node__node__codec__VersionRow__drop(versions.ptr[i]);
    jz_free(versions.ptr);
    return JZ_OK;
}

/* Recover the deletion-register winner currently visible at the
 * subscription tier for an optimized scalar root. Result membership tells
 * us that the row is visible, but not whether that visibility is due to a
 * later `Restored` register event; the receiver must learn that event to
 * keep its own register current for later ordinary reads. */
static jz_err storage_backed_maintained_deletion_winner(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_model_DurabilityTier tier,
                                                        jazz_node_ViewEvaluationContext *context, VRow **out) {
    return storage_backed_current_image(self, table, row_uuid, tier, context, true, out);
}

static jz_err stored_versions_by_key(NS *self, TxId tx_id, jz_btree *out /* RecordKey -> VR* */) {
    jz_btree_init(out, &desc_record_map);
    jazz_node_VersionRowVec stored_versions = { 0 };
    JZ_TRY(jazz_node__node__currency__NodeState__query_versions_for_tx(self, tx_id, &stored_versions));
    jz_err e = JZ_OK;
    for (u32 i = 0; i < stored_versions.len && !e.code; i++) {
        VR *version = NULL;
        e = jazz_node__node__NodeState__version_record_from_row(self, stored_versions.ptr[i], &version);
        if (e.code) break;
        RecordKey key = version_bundle_record_key(version);
        VR *old;
        if (jz_btree_insert(out, &key, &version, &old)) { record_key_drop(&key); jazz_protocol__protocol__VersionRecord__drop(old); }
    }
    vrow_vec_drop(&stored_versions);
    return e;
}

static jz_err preflight_view_bundle_conflicts(NS *self, const BundleRef *bundles, u32 n, ViewBundlePreflight *out) {
    jz_btree_init(&out->bundles, &desc_tx_bundles);
    jz_btree_init(&out->persisted_tx_ids, &desc_txid_set);
    jz_err e = JZ_OK;
    for (u32 i = 0; i < n; i++) {
        e = merge_receiver_version_bundle_ref(&out->bundles, bundles[i]);
        if (e.code) goto fail;
    }
    TxId *tx_id; Bundle *bundle;
    JZ_BTREE_FOR(&out->bundles, tx_id, bundle) {
        bool found = false;
        jazz_node_StoredTransaction stored;
        e = jazz_node__node__currency__NodeState__query_transaction(self, *tx_id, &found, &stored);
        if (e.code) goto fail;
        if (!found) continue;
        jz_btree_insert(&out->persisted_tx_ids, tx_id, NULL, NULL);
        jazz_model_Transaction *stored_identity0 = jazz_model__tx__Transaction__clone(stored.tx);
        jazz_model__tx__Transaction__set_n_total_writes(stored_identity0, 0);
        jazz_model_Transaction *stored_identity = jazz_node__node__codec__transaction_without_permission_subject(stored_identity0);
        jazz_model__tx__Transaction__drop(stored_identity0);
        jazz_model_Transaction *incoming_identity = jazz_model__tx__Transaction__clone(bundle->tx);
        jazz_model__tx__Transaction__set_n_total_writes(incoming_identity, 0);
        /* The author or relay of a pending exclusive transaction stores
         * its read evidence; view carriers never ship it. Compare the
         * payload, not the local-only evidence. */
        bool matches = jazz_node__node__codec__known_transaction_payload_matches(stored_identity, incoming_identity);
        jazz_model__tx__Transaction__drop(stored_identity);
        jazz_model__tx__Transaction__drop(incoming_identity);
        if (!matches) { jazz_node__node__codec__StoredTransaction__drop(&stored); e = jazz_node__node__Error__conflicting_commit_unit(tx_id); goto fail; }
        jz_btree stored_by_key;
        e = stored_versions_by_key(self, *tx_id, &stored_by_key);
        if (e.code) { jz_btree_drop(&stored_by_key); jazz_node__node__codec__StoredTransaction__drop(&stored); goto fail; }
        jz_btree incoming_by_key; jz_btree_init(&incoming_by_key, &desc_record_ref_map);
        for (u32 i = 0; i < bundle->versions.len; i++) {
            RecordKey key = version_bundle_record_key(bundle->versions.ptr[i]);
            const VR *ref = bundle->versions.ptr[i];
            const VR *old;
            if (jz_btree_insert(&incoming_by_key, &key, &ref, &old)) record_key_drop(&key);
        }
        /* An accepted, Core-sequenced bundle carries Core's post-images,
         * which replace a locally stored authored unit: this node's own
         * patch, or, on a client relay (a browser worker), the patch it
         * relayed upstream for its downstream originator. The relay may
         * apply the accepted fate before the post-image arrives, so a
         * copy at the same sequence position is still that patch. Any
         * other node holds only upstream post-images, where a differing
         * copy is a genuine conflict. */
        bool carries_post_images = bundle->fate.tag == JAZZ_FATE_ACCEPTED && bundle->has_global_time
            && (jz_uuid_eq(&tx_id->node, &self->node_uuid)
                || (jazz_node__node__NodeState__client_relay_scope(self) != NULL
                    && (!stored.has_global_time || stored.global_time == bundle->global_time)));
        RecordKey *key; const VR **incoming;
        JZ_BTREE_FOR(&incoming_by_key, key, incoming) {
            VR **existing = jz_btree_get(&stored_by_key, key);
            if (existing && !jazz_protocol__protocol__VersionRecord__eq(*existing, *incoming) && !carries_post_images) {
                e = jazz_node__node__Error__conflicting_commit_unit(tx_id);
                break;
            }
        }
        if (!e.code) {
            if (!stored.is_partial && bundle->scope == JAZZ_BUNDLE_SCOPE_COMPLETE) {
                bool keys_ne = stored_by_key.len != incoming_by_key.len;
                if (!keys_ne) {
                    jz_btree_iter a, b; void *ka, *va, *kb, *vb;
                    jz_btree_iter_begin(&a, &stored_by_key); jz_btree_iter_begin(&b, &incoming_by_key);
                    while (jz_btree_next(&a, &ka, &va) && jz_btree_next(&b, &kb, &vb))
                        if (record_key_cmp(ka, kb, NULL) != 0) { keys_ne = true; break; }
                }
                if (jazz_model__tx__Transaction__n_total_writes(stored.tx) != jazz_model__tx__Transaction__n_total_writes(bundle->tx)
                    || stored_by_key.len != incoming_by_key.len || keys_ne)
                    e = jazz_node__node__Error__conflicting_commit_unit(tx_id);
            } else if (!stored.is_partial && bundle->scope == JAZZ_BUNDLE_SCOPE_VIEW_SCOPED) {
                JZ_BTREE_FOR(&incoming_by_key, key, incoming)
                    if (!jz_btree_contains(&stored_by_key, key)) { e = jazz_node__node__Error__conflicting_commit_unit(tx_id); break; }
            } else if (stored.is_partial && bundle->scope == JAZZ_BUNDLE_SCOPE_COMPLETE) {
                VR **sv;
                JZ_BTREE_FOR(&stored_by_key, key, sv)
                    if (!jz_btree_contains(&incoming_by_key, key)) { e = jazz_node__node__Error__conflicting_commit_unit(tx_id); break; }
            } else {
                RecordKey k; VR *version;
                while (jz_btree_pop_first(&stored_by_key, &k, &version)) {
                    if (!jz_btree_contains(&incoming_by_key, &k)) JZ_VEC_PUSH(bundle->versions, version);
                    else jazz_protocol__protocol__VersionRecord__drop(version);
                    record_key_drop(&k);
                }
                jz_sort(bundle->versions.ptr, bundle->versions.len, sizeof(VR *), jazz_protocol__protocol__VersionRecord__cmp_ptr, NULL);
                jazz_model__tx__Transaction__set_n_total_writes(bundle->tx, bundle->versions.len);
            }
        }
        jz_btree_drop(&incoming_by_key);
        jz_btree_drop(&stored_by_key);
        jazz_node__node__codec__StoredTransaction__drop(&stored);
        if (e.code) goto fail;
    }
    return JZ_OK;
fail:
    preflight_drop(out);
    return e;
}

/* Subscribe to the raw history storage table. */
jz_err jazz_node__node__views__NodeState__subscribe_history(NS *self, const jz_str *table, groove_Subscription **out) {
    const jazz_model_TableSchema *t;
    JZ_TRY(jazz_node__node__NodeState__table(self, table, &t));
    jazz_types_SchemaVersionId schema_version = self->catalogue.local_schema_version_id;
    groove_GraphBuilder *source = NULL;
    JZ_TRY(jazz_node__node__physical__NodeState__physical_history_source_graph(self, schema_version, table, &source));
    return groove__db__facade__Database__subscribe_one_sink(self->database.database, source, out); /* map_err(Error::Groove) */
}

jz_err jazz_node__node__views__NodeState__scope_physical_table(const NS *self, jazz_types_SchemaVersionId schema, const jz_str *table,
                                                              jazz_types_GlobalPhysicalTableId *out) {
    const jazz_node_SchemaPhysicalMapping *mapping = jz_btree_get(&self->catalogue.physical_mappings, &schema);
    const jazz_protocol_PhysicalTableIdentity *identity = mapping ? jz_btree_get(&mapping->identities->tables, table) : NULL;
    if (!identity) return JZN_INVALID_STORED("scope physical table mapping missing");
    *out = identity->id;
    return JZ_OK;
}

jz_err jazz_node__node__views__NodeState__scope_logical_table(const NS *self, jazz_types_SchemaVersionId schema,
                                                             jazz_types_GlobalPhysicalTableId physical, groove_InternStr *out) {
    /* Publication validation guarantees physical IDs are unique within a
     * read schema. Names are compiler lookup metadata, never membership. */
    const jazz_node_SchemaPhysicalMapping *mapping = jz_btree_get(&self->catalogue.physical_mappings, &schema);
    if (mapping) {
        const jz_str *name; const jazz_protocol_PhysicalTableIdentity *table;
        JZ_BTREE_FOR(&mapping->identities->tables, name, table)
            if (jz_uuid_eq(&table->id, &physical)) { *out = name; return JZ_OK; }
    }
    return JZN_INVALID_STORED("scope physical table is absent from read schema");
}

/* ---------------- authority-result helpers ---------------- */
static jazz_node_AuthorityResultState *authority_state_entry(NS *self, const jazz_protocol_AuthorityResultKey *key) {
    jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, key);
    if (state) return state;
    jazz_protocol_AuthorityResultKey k;
    jazz_protocol__protocol__AuthorityResultKey__clone(&k, key);
    jazz_node_AuthorityResultState d;
    jazz_node__node__AuthorityResultState__default(&d);
    jz_btree_insert(&self->query.authority_results, &k, &d, NULL);
    return jz_btree_get(&self->query.authority_results, key);
}

static const jz_btree_desc desc_revisions = {
    sizeof(jazz_protocol_AuthorityResultKey), 16, jazz_protocol__protocol__AuthorityResultKey__cmp, jazz_protocol__protocol__AuthorityResultKey__drop, NULL
};
static const jz_btree_desc desc_coord_set = {
    sizeof(jazz_node_CoveredInputCoordinate), 0, jazz_node__node__CoveredInputCoordinate__cmp, jazz_node__node__CoveredInputCoordinate__drop, NULL
};
static const jz_btree_desc desc_coord_ref = {
    sizeof(jazz_node_CoveredInputCoordinate), sizeof(const SRow *), jazz_node__node__CoveredInputCoordinate__cmp, jazz_node__node__CoveredInputCoordinate__drop, NULL
};

static void sru_replace(jazz_node_ViewUpdateParts *update, jazz_protocol_SupportingRowsUpdate fresh) {
    if (update->wire_rows) jazz_protocol__protocol__SupportingRowsUpdate__drop(update->wire_rows);
    else update->wire_rows = JZ_NEW(jazz_protocol_SupportingRowsUpdate);
    *update->wire_rows = fresh;
}

/* Preserve the physical frontier from wire to receiver. The only optional
 * normalization is a fresh snapshot refreshing an already-live usage:
 * derive its net delta to keep application listeners incremental. */
static jz_err normalize_supporting_update(NS *self, jazz_node_ViewUpdateParts *update, jz_btree *revisions /* AuthorityResultKey -> [u8;16] */,
                                          bool *has_out, u8 out[16]) {
    *has_out = false;
    jazz_protocol_SupportingRowsUpdate *wire = update->wire_rows;
    if (!wire) return JZ_OK;
    static const char invalid_msg[] = "invalid supporting-set revision or predecessor";
    if (update->opening_pending) {
        if (wire->tag != JAZZ_SRU_SNAPSHOT || wire->added.len != 0) return invalid_closure_c(update->subscription, invalid_msg);
        update->reset_input_set = false;
        return JZ_OK;
    }
    jazz_protocol_AuthorityResultKey key;
    jz_err e = jazz_node__node__query_eval__subscriptions__NodeState__authority_result_key_for_subscription(self, update->subscription, &key);
    if (e.code) {
        if (err_is_unregistered_subscription(&e)) { jz_err_drop(&e); return JZ_OK; }
        return e;
    }
    u8 revision[16];
    memcpy(revision, wire->revision, 16);
    static const u8 zero[16] = { 0 };
    if (!memcmp(revision, zero, 16)) { e = invalid_closure_c(update->subscription, invalid_msg); goto out; }
    if (wire->tag == JAZZ_SRU_CATCH_UP) {
        /* Resolve the watermark catch-up against the set this receiver
         * installed as `predecessor` into the complete set it describes:
         * coordinates that moved are replaced or dropped, the rest are
         * held here already. From then on it is an ordinary snapshot. */
        const jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, &key);
        if (!state || jz_btree_contains(revisions, &key) || !state->has_supporting_revision
            || memcmp(state->supporting_revision, wire->predecessor, 16)) {
            e = invalid_closure_c(update->subscription, invalid_msg);
            goto out;
        }
        jz_btree moved; jz_btree_init(&moved, &desc_coord_set);
        for (u32 pass = 0; pass < 2; pass++) {
            const jazz_protocol_SupportingRowVec *rows = pass ? &wire->removed : &wire->added;
            for (u32 i = 0; i < rows->len; i++) {
                jazz_node_CoveredInputCoordinate c;
                jazz_node__node__CoveredInputCoordinate__From__from(&c, &rows->ptr[i]);
                if (jz_btree_insert(&moved, &c, NULL, NULL)) jazz_node__node__CoveredInputCoordinate__drop(&c);
            }
        }
        jazz_protocol_SupportingRowVec rows = { 0 };
        const jazz_node_CoveredInputCoordinate *coordinate; const SRow *row;
        JZ_BTREE_FOR(&state->covered_input_versions, coordinate, row) {
            if (jz_btree_contains(&moved, coordinate)) continue;
            SRow copy; jazz_protocol__protocol__SupportingRow__clone(&copy, row);
            JZ_VEC_PUSH(rows, copy);
        }
        jz_btree_drop(&moved);
        for (u32 i = 0; i < wire->added.len; i++) {
            SRow copy; jazz_protocol__protocol__SupportingRow__clone(&copy, &wire->added.ptr[i]);
            JZ_VEC_PUSH(rows, copy);
        }
        jazz_protocol_SupportingRowsUpdate snapshot = { 0 };
        snapshot.tag = JAZZ_SRU_SNAPSHOT;
        memcpy(snapshot.revision, revision, 16);
        snapshot.added = rows;
        sru_replace(update, snapshot);
        wire = update->wire_rows;
    }
    if (wire->tag == JAZZ_SRU_DELTA) {
        bool has_current = false; u8 current[16];
        const u8 *rev = jz_btree_get(revisions, &key);
        if (rev) { has_current = true; memcpy(current, rev, 16); }
        else {
            const jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, &key);
            if (state && state->has_supporting_revision) { has_current = true; memcpy(current, state->supporting_revision, 16); }
        }
        if (!has_current || memcmp(current, wire->predecessor, 16)
            || (!memcmp(wire->predecessor, revision, 16) && (wire->added.len != 0 || wire->removed.len != 0))) {
            e = invalid_closure_c(update->subscription, invalid_msg);
            goto out;
        }
    }
    update->reset_input_set = wire->tag == JAZZ_SRU_SNAPSHOT;
    const jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, &key);
    if (wire->tag == JAZZ_SRU_SNAPSHOT && !jz_btree_contains(revisions, &key) && state && state->live_settled
        && !state->has_pending_authoritative_reset && state->source_closure_tag == JZN_SOURCE_CLOSURE_CLAIMED
        && state->has_supporting_revision) {
        u8 predecessor[16];
        memcpy(predecessor, state->supporting_revision, 16);
        jz_btree incoming; jz_btree_init(&incoming, &desc_coord_ref);
        for (u32 i = 0; i < wire->added.len; i++) {
            const SRow *row = &wire->added.ptr[i];
            jazz_node_CoveredInputCoordinate c;
            jazz_node__node__CoveredInputCoordinate__From__from(&c, row);
            bool dup = jz_btree_contains(&incoming, &c);
            if (!jazz_protocol__protocol__SupportingRow__is_wire_valid(row) || dup) {
                jazz_node__node__CoveredInputCoordinate__drop(&c);
                jz_btree_drop(&incoming);
                e = invalid_closure_c(update->subscription, "invalid or duplicate supporting physical row coordinate");
                goto out;
            }
            jz_btree_insert(&incoming, &c, &row, NULL);
        }
        jazz_protocol_SupportingRowVec adds = { 0 }, removes = { 0 };
        const jazz_node_CoveredInputCoordinate *coordinate; const SRow **in_row;
        JZ_BTREE_FOR(&incoming, coordinate, in_row) {
            const SRow *retained = jz_btree_get(&state->covered_input_versions, coordinate);
            if (!retained || !jazz_protocol__protocol__SupportingRow__eq(retained, *in_row)) {
                SRow copy; jazz_protocol__protocol__SupportingRow__clone(&copy, *in_row);
                JZ_VEC_PUSH(adds, copy);
            }
        }
        const SRow *retained;
        JZ_BTREE_FOR(&state->covered_input_versions, coordinate, retained) {
            jazz_node_CoveredInputCoordinate c;
            jazz_node__node__CoveredInputCoordinate__From__from(&c, retained);
            const SRow **now = jz_btree_get(&incoming, &c);
            jazz_node__node__CoveredInputCoordinate__drop(&c);
            if (!now || !jazz_protocol__protocol__SupportingRow__eq(*now, retained)) {
                SRow copy; jazz_protocol__protocol__SupportingRow__clone(&copy, retained);
                JZ_VEC_PUSH(removes, copy);
            }
        }
        jz_btree_drop(&incoming);
        jazz_protocol_SupportingRowsUpdate delta = { 0 };
        delta.tag = JAZZ_SRU_DELTA;
        memcpy(delta.predecessor, predecessor, 16);
        memcpy(delta.revision, revision, 16);
        delta.added = adds;
        delta.removed = removes;
        sru_replace(update, delta);
        update->reset_input_set = false;
    }
    {
        jazz_protocol_AuthorityResultKey k;
        jazz_protocol__protocol__AuthorityResultKey__clone(&k, &key);
        u8 old[16];
        if (jz_btree_insert(revisions, &k, revision, old)) jazz_protocol__protocol__AuthorityResultKey__drop(&k);
    }
    *has_out = true;
    memcpy(out, revision, 16);
out:
    jazz_protocol__protocol__AuthorityResultKey__drop(&key);
    return e;
}
