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
static jz_err exact_storage_maintained_versions_are_unambiguous(NS *self, VRow *const *versions, u32 n, bool *out);

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

/* ---------------- view_update_for_maintained_result_members ---------------- */
static jz_err memo_tx(NS *self, TxId tx_id, jazz_node_ViewEvaluationContext *context, jazz_node_StoredTransaction *out) {
    bool found = false;
    JZ_TRY(jazz_node__node__policy__NodeState__query_transaction_memo(self, tx_id, context, &found, out));
    if (!found) return jazz_node__node__Error__missing_transaction(&tx_id);
    return JZ_OK;
}

static void vrow_opt_drop_v(void *p) { VRow *v = *(VRow **)p; if (v) jazz_node__node__codec__VersionRow__drop(v); }
static const jz_btree_desc desc_logical_tables = { sizeof(jazz_types_GlobalPhysicalTableId), sizeof(const jz_str *), jz_cmp_uuid, NULL, NULL };
static const jz_btree_desc desc_deletion_winners = { sizeof(TableRow), sizeof(VRow *), table_row_cmp, table_row_drop, vrow_opt_drop_v };
static int str_ptr_cmp(const void *a, const void *b, void *ctx) { (void)ctx; return jz_str_cmp(*(const jz_str *const *)a, *(const jz_str *const *)b); }
static const jz_btree_desc desc_str_ptr_set = { sizeof(const jz_str *), 0, str_ptr_cmp, NULL, NULL };

/* (table, row, version, missing-witness message) */
typedef struct { jz_str table; jazz_types_RowUuid row; VRow *version; const char *missing_witness; } Winner;
typedef JZ_VEC(Winner) WinnerVec;
static void winner_vec_drop_v(void *p) {
    WinnerVec *v = p;
    for (u32 i = 0; i < v->len; i++) { jz_str_drop(&v->ptr[i].table); jazz_node__node__codec__VersionRow__drop(v->ptr[i].version); }
    jz_free(v->ptr);
}
static const jz_btree_desc desc_winners_by_tx = { sizeof(TxId), sizeof(WinnerVec), jzn_cmp_txid, NULL, winner_vec_drop_v };

typedef struct { bool has_tx; TxId tx; bool is_member; } RemovedCandidate;
static const jz_btree_desc desc_removed_candidates = { sizeof(TableRow), sizeof(RemovedCandidate), table_row_cmp, table_row_drop, NULL };
static void removed_candidate_or_insert(jz_btree *m, const jz_str *table, jazz_types_RowUuid row, bool has_tx, TxId tx, bool is_member, bool merge_member) {
    TableRow probe = { *table, row };
    RemovedCandidate *entry = jz_btree_get(m, &probe);
    if (!entry) {
        TableRow k = { jz_str_clone(table), row };
        RemovedCandidate c = { has_tx, tx, is_member };
        jz_btree_insert(m, &k, &c, NULL);
        return;
    }
    if (merge_member) entry->is_member |= is_member;
}

static jazz_node_VersionRowVec *tx_versions_entry(jz_btree *cache, TxId tx_id, const jazz_node_MaintainedSubscriptionView *maintained_facts) {
    jazz_node_VersionRowVec *v = jz_btree_get(cache, &tx_id);
    if (v) return v;
    jazz_node_VersionRowVec fresh = jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__versions_by_tx(maintained_facts, tx_id);
    jz_btree_insert(cache, &tx_id, &fresh, NULL);
    return jz_btree_get(cache, &tx_id);
}
static void tx_versions_replace(jz_btree *cache, TxId tx_id, jazz_node_VersionRowVec fresh) {
    jazz_node_VersionRowVec old;
    if (jz_btree_insert(cache, &tx_id, &fresh, &old)) vrow_vec_drop(&old);
}

static jz_err fallback_versions_for(NS *self, TxId tx_id, const jazz_node_StoredTransaction *stored_tx, bool complete_exclusive_payloads,
                                    const jz_btree *wanted_rows, jazz_node_VersionRowVec *out) {
    *out = (jazz_node_VersionRowVec){ 0 };
    if (complete_exclusive_payloads && jazz_model__tx__Transaction__kind(stored_tx->tx) == JAZZ_TXKIND_EXCLUSIVE)
        return jazz_node__node__currency__NodeState__query_versions_for_tx(self, tx_id, out);
    return jazz_node__node__currency__NodeState__query_versions_for_tx_rows_by_alias(self, tx_id, stored_tx->node_alias, wanted_rows, out);
}

static jz_err deletion_winner_for(NS *self, const jazz_node_MaintainedSubscriptionView *maintained_facts, const jz_str *table, jazz_types_RowUuid row_uuid,
                                  bool allow_storage_witness_fallback, jazz_model_DurabilityTier tier, jazz_node_ViewEvaluationContext *context, VRow **out) {
    VRow *content = NULL, *retained_deletion_winner = NULL;
    jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__replacement_for(maintained_facts, table, row_uuid, &content, &retained_deletion_winner);
    if (content) jazz_node__node__codec__VersionRow__drop(content);
    if (retained_deletion_winner) { *out = retained_deletion_winner; return JZ_OK; }
    *out = NULL;
    if (allow_storage_witness_fallback) return storage_backed_maintained_deletion_winner(self, table, row_uuid, tier, context, out);
    return JZ_OK;
}

static bool tx_versions_touch_wanted(const jazz_node_VersionRowVec *tx_versions, const jz_btree *wanted_rows) {
    for (u32 i = 0; i < tx_versions->len; i++)
        if (table_row_contains(wanted_rows, VROW_TABLE(tx_versions->ptr[i]), VROW_ROW(tx_versions->ptr[i]))) return true;
    return false;
}

jz_err jazz_node__node__views__NodeState__view_update_for_maintained_result_members(NS *self, jazz_node_MaintainedViewBundleInputs inputs,
                                                                                   jazz_protocol_SyncMessage **out) {
    const jazz_node_MaintainedSubscriptionView *maintained_facts = inputs.maintained_facts;
    const jazz_model_ValidatedQuery *shape = inputs.shape;
    const jazz_model_Query *shape_query = jazz_model__query__ValidatedQuery__query(shape);
    jazz_model_DurabilityTier tier = inputs.tier;
    jazz_types_AuthorSubject identity = inputs.identity;
    bool complete_exclusive_payloads = inputs.complete_exclusive_payloads;
    /* The storage-backed maintained subset deliberately omits the
     * source-wide terminal witnesses. Its result-member identity is still
     * exact, so Stream B must retrieve that immutable payload from the
     * authoritative node store when shipping a newly admitted member.
     * Keep callers' explicit fallback flag for the legacy cases that
     * already use it. */
    bool uses_storage_backed = jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__uses_storage_backed_result_materialization(maintained_facts);
    bool allow_storage_witness_fallback = inputs.allow_storage_witness_fallback || uses_storage_backed;
    jazz_protocol_SupportingRowsUpdate supporting_update;
    if (inputs.supporting_update) { supporting_update = *inputs.supporting_update; jz_free(inputs.supporting_update); }
    else jazz_protocol__protocol__SupportingRowsUpdate__snapshot(&supporting_update,
             jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__supporting_rows_cloned(maintained_facts));

    jz_err e = JZ_OK;
    jz_btree logical_tables, tx_versions_cache, skipped_known_state_rows, wanted_add_rows_by_tx, resettled_rows, exit_candidates,
        emitted_versions, result_add_deletion_winners, result_add_coordinates, replacement_winners_by_tx, storage_deletion_txs,
        removed_row_candidates, joined_tables, supporting_now, cursor_left, catch_up_left, unreadable_left;
    jz_btree_init(&logical_tables, &desc_logical_tables);
    jz_btree_init(&tx_versions_cache, &desc_tx_versions);
    jz_btree_init(&skipped_known_state_rows, &desc_table_row_set);
    jz_btree_init(&wanted_add_rows_by_tx, &desc_tx_table_rows);
    jz_btree_init(&resettled_rows, &desc_table_row_tx_set);
    jz_btree_init(&exit_candidates, &desc_table_row_tx_set);
    jz_btree_init(&emitted_versions, &desc_txid_set);
    jz_btree_init(&result_add_deletion_winners, &desc_deletion_winners);
    jz_btree_init(&result_add_coordinates, &desc_table_row_tx_set);
    jz_btree_init(&replacement_winners_by_tx, &desc_winners_by_tx);
    jz_btree_init(&storage_deletion_txs, &desc_txid_set);
    jz_btree_init(&removed_row_candidates, &desc_removed_candidates);
    jz_btree_init(&joined_tables, &desc_str_ptr_set);
    jz_btree_init(&supporting_now, &desc_table_row_set);
    jz_btree_init(&cursor_left, &desc_table_row_set);
    jz_btree_init(&catch_up_left, &desc_table_row_set);
    jz_btree_init(&unreadable_left, &desc_table_row_set);
    jazz_node_ViewEvaluationContext context;
    jazz_node__node__policy__ViewEvaluationContext__default(&context);
    RowEntryVec row_result_adds = { 0 }, row_result_removes = { 0 };
    jazz_protocol_VersionBundleVec version_bundles = { 0 };
    jazz_model_TxIdVec peer_payload_inventory_refs = { 0 };
    jazz_model_ValidatedQuery *read_shape = NULL;
    jazz_model_Binding *read_binding = NULL;

    {
        const jz_str *name; const jazz_types_GlobalPhysicalTableId *id;
        JZ_BTREE_FOR(jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__physical_tables(maintained_facts), name, id)
            jz_btree_insert(&logical_tables, id, &name, NULL);
    }
    e = content_row_members_for_bundle(&inputs.result_member_adds, "real row result member is missing content transaction for bundle shipping", &row_result_adds);
    if (e.code) goto out;
    e = content_row_members_for_bundle(&inputs.result_member_removes,
                                       "real row result member removal is missing content transaction for replacement shipping", &row_result_removes);
    if (e.code) goto out;
    bool has_known_state_position = false;
    jazz_types_GlobalTime known_state_position = 0;
    if (inputs.known_state) {
        switch (inputs.known_state->tag) {
        case JAZZ_KSD_FAST: case JAZZ_KSD_FAST_WITH_AUTH: case JAZZ_KSD_WATERMARK:
            has_known_state_position = true;
            known_state_position = inputs.known_state->position;
            break;
        default: JZ_UNREACHABLE(); /* Reserved2(retired) is uninhabited */
        }
    }
    for (u32 i = 0; i < inputs.result_member_adds.len; i++) {
        const jazz_protocol_RealRowMemberEntry *row = jazz_protocol__protocol__ResultMemberEntry__as_real_row(&inputs.result_member_adds.ptr[i]);
        if (!row) continue;
        jazz_types_GlobalTime position;
        if (jazz_protocol__protocol__RealRowMemberEntry__settle_position(row, &position) && has_known_state_position && position <= known_state_position)
            table_row_insert(&skipped_known_state_rows, jazz_protocol__protocol__RealRowMemberEntry__table(row), jazz_protocol__protocol__RealRowMemberEntry__row_uuid(row));
    }
    for (u32 i = 0; i < supporting_update.added.len; i++) {
        const SRow *row = &supporting_update.added.ptr[i];
        const jz_str **table = jz_btree_get(&logical_tables, &row->physical_table);
        if (!table) { e = JZN_INVALID_STORED("support body has no compiled physical table"); goto out; }
        table_row_insert(tx_table_rows_entry(&wanted_add_rows_by_tx, row->version.tx), *table, row->row);
    }
    /* A member that leaves and re-enters naming the same content
     * transaction was re-settled: once Core sequences a unit, this node
     * stores Core's post-image in place of the authored patch the peer may
     * have received (always so for the downstream that authored it), and
     * the supporting-row identity, keyed by transaction, cannot show that.
     * Ship the settled payload again so the peer replaces its copy. */
    for (u32 i = 0; i < row_result_removes.len; i++)
        table_row_tx_insert(&resettled_rows, row_result_removes.ptr[i].table, row_result_removes.ptr[i].row, row_result_removes.ptr[i].tx);
    for (u32 i = 0; i < row_result_adds.len; i++) {
        const RowEntry *r = &row_result_adds.ptr[i];
        if (table_row_tx_contains(&resettled_rows, r->table, r->row, r->tx))
            table_row_insert(tx_table_rows_entry(&wanted_add_rows_by_tx, r->tx), r->table, r->row);
    }
    /* Scalar publication can retract only its covered root source;
     * the receiver derives result removal from that source delta. */
    for (u32 i = 0; i < row_result_removes.len; i++)
        table_row_tx_insert(&exit_candidates, row_result_removes.ptr[i].table, row_result_removes.ptr[i].row, row_result_removes.ptr[i].tx);
    for (u32 i = 0; i < supporting_update.removed.len; i++) {
        const SRow *row = &supporting_update.removed.ptr[i];
        const jz_str **table = jz_btree_get(&logical_tables, &row->physical_table);
        if (!table) continue;
        if (jz_str_eq(*table, &shape_query->table)) table_row_tx_insert(&exit_candidates, *table, row->row, row->version.tx);
    }
    /* A live scalar predicate exit retracts coverage, but a still-readable
     * successor must also refresh the receiver's local-first cache. Probe
     * only removed physical rows through ordinary serving authorization;
     * do not reopen the query's input relation or infer permission from its
     * previous membership. Keep non-default views and complex scopes on
     * their existing witness path until their replacement contract exists. */
    {
        jazz_types_SchemaVersionId shape_schema = jazz_model__query__ValidatedQuery__schema_version(shape);
        if (self->authoritative_scalar_exit_refresh && jazz_node__node__NodeState__client_relay_scope(self) == NULL
            && inputs.allow_authoritative_scalar_exit_refresh && exit_candidates.len != 0 && inputs.has_default_read_view
            && jz_uuid_eq(&shape_schema, &self->catalogue.local_schema_version_id)
            && jazz_node__node__views__simple_scalar_exit_query(shape_query)) {
            e = jazz_node__node__views__NodeState__whole_table_shape_binding(self, &shape_query->table, &read_shape, &read_binding);
            if (e.code) goto out;
            jz_btree added_rows; jz_btree_init(&added_rows, &desc_table_row_set);
            for (u32 i = 0; i < row_result_adds.len; i++) table_row_insert(&added_rows, row_result_adds.ptr[i].table, row_result_adds.ptr[i].row);
            const TableRowTx *candidate; void *unit;
            JZ_BTREE_FOR(&exit_candidates, candidate, unit) {
                (void)unit;
                if (!jz_str_eq(&candidate->table, &shape_query->table)) continue;
                VRow *content = NULL, *deletion = NULL;
                jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__replacement_for(maintained_facts, &candidate->table, candidate->row, &content, &deletion);
                bool has_replacement = content != NULL;
                if (content) jazz_node__node__codec__VersionRow__drop(content);
                if (deletion) jazz_node__node__codec__VersionRow__drop(deletion);
                if (has_replacement || table_row_contains(&added_rows, &candidate->table, candidate->row)) continue;
                jazz_node_CurrentRowVec rows = { 0 };
                e = jazz_node__node__query_eval__NodeState__query_rows_for_link_physical_row(self, read_shape, read_binding, tier, identity, candidate->row, &rows);
                if (e.code) break;
                const jazz_node_CurrentRow *row = NULL;
                for (u32 i = 0; i < rows.len && !row; i++) {
                    jazz_types_RowUuid r = jazz_node__node__CurrentRow__row_uuid(&rows.ptr[i]);
                    if (jz_uuid_eq(&r, &candidate->row)) row = &rows.ptr[i];
                }
                TxId tx_id;
                bool has_tx = row && jazz_node__node__NodeState__current_row_tx_id(self, row, &tx_id);
                jz_vec_drop(&rows, sizeof(jazz_node_CurrentRow), jazz_node__node__CurrentRow__drop);
                if (!has_tx || jazz_model_TxId_eq(&tx_id, &candidate->tx)) continue;
                jazz_node_StoredTransaction stored_tx;
                e = memo_tx(self, tx_id, &context, &stored_tx);
                if (e.code) break;
                jz_btree wanted; jz_btree_init(&wanted, &desc_table_row_set);
                table_row_insert(&wanted, &candidate->table, candidate->row);
                jazz_node_VersionRowVec versions = { 0 };
                e = jazz_node__node__currency__NodeState__query_versions_for_tx_rows_by_alias(self, tx_id, stored_tx.node_alias, &wanted, &versions);
                jazz_node__node__codec__StoredTransaction__drop(&stored_tx);
                if (e.code) { jz_btree_drop(&wanted); break; }
                /* Merge before bundling: an authorized sibling may already
                 * require this transaction, while other siblings remain private. */
                jazz_node_VersionRowVec *cached = tx_versions_entry(&tx_versions_cache, tx_id, maintained_facts);
                for (u32 i = 0; i < versions.len; i++) {
                    if (!jazz_node__node__codec__VersionRow__is_deleted(versions.ptr[i])) JZ_VEC_PUSH(*cached, versions.ptr[i]);
                    else jazz_node__node__codec__VersionRow__drop(versions.ptr[i]);
                }
                jz_free(versions.ptr);
                table_row_insert(tx_table_rows_entry(&wanted_add_rows_by_tx, tx_id), &candidate->table, candidate->row);
                jz_btree_drop(&wanted);
            }
            jz_btree_drop(&added_rows);
            if (e.code) goto out;
        }
    }
    {
        jazz_model_TxIdVec ids = { 0 };
        TxId *tx; jz_btree *rows;
        JZ_BTREE_FOR(&wanted_add_rows_by_tx, tx, rows) JZ_VEC_PUSH(ids, *tx);
        e = jazz_node__node__currency__NodeState__preload_transaction_memo(self, ids.ptr, ids.len, &context);
        jz_free(ids.ptr);
        if (e.code) goto out;
    }
    jz_vec_reserve(&version_bundles, row_result_adds.len, sizeof(Bundle));
    /* A current result member names its content version, but its visible
     * existence can also depend on a deletion-register `Restored` event.
     * Remember the winner while assembling its content bundle so that a
     * content and restore event committed in the *same* transaction are
     * shipped together.  The later replacement pass only emits a
     * different transaction; once this transaction is marked emitted it
     * cannot repair a missing same-transaction register witness. */
    /* Initial snapshots may add thousands of rows. Test membership by
     * coordinate instead of rescanning the entire result-add list for
     * each wanted native body. */
    for (u32 i = 0; i < row_result_adds.len; i++)
        table_row_tx_insert(&result_add_coordinates, row_result_adds.ptr[i].table, row_result_adds.ptr[i].row, row_result_adds.ptr[i].tx);
    {
        TxId *tx_idp; jz_btree *wanted_rows;
        JZ_BTREE_FOR(&wanted_add_rows_by_tx, tx_idp, wanted_rows) {
            TxId tx_id = *tx_idp;
            /* Scope membership and body availability are independent. A
             * resumed snapshot still declares every input, while rows the
             * receiver holds through its watermark ship no body. A receiver
             * that lost one asks again without known state. */
            if (has_known_state_position) {
                bool found = false;
                jazz_node_StoredTransaction tx;
                e = jazz_node__node__policy__NodeState__query_transaction_memo(self, tx_id, &context, &found, &tx);
                if (e.code) goto out;
                bool covered = found && tx.has_global_time && tx.global_time <= known_state_position;
                if (found) jazz_node__node__codec__StoredTransaction__drop(&tx);
                if (covered) continue;
            }
            if (jz_btree_contains(&inputs.peer_complete_tx_payloads, &tx_id)) {
                JZ_VEC_PUSH(peer_payload_inventory_refs, tx_id);
                continue;
            }
            if (jz_btree_insert(&emitted_versions, &tx_id, NULL, NULL)) continue;
            jazz_node_VersionRowVec *tx_versions = tx_versions_entry(&tx_versions_cache, tx_id, maintained_facts);
            /* These checks only need content-witness presence, not a selected
             * version. Index once rather than decoding row identities while
             * scanning the transaction twice for every requested row. Keep
             * the original version vector intact (including ordering and
             * multiple versions/layers for a coordinate). */
            jz_btree content_coordinates; jz_btree_init(&content_coordinates, &desc_table_row_set);
            for (u32 i = 0; i < tx_versions->len; i++)
                if (!jazz_node__node__codec__VersionRow__is_deleted(tx_versions->ptr[i]))
                    table_row_insert(&content_coordinates, VROW_TABLE(tx_versions->ptr[i]), VROW_ROW(tx_versions->ptr[i]));
            bool needs_storage_fallback = false;
            const TableRow *coordinate; void *unit;
            JZ_BTREE_FOR(wanted_rows, coordinate, unit) {
                (void)unit;
                if (table_row_contains(&content_coordinates, &coordinate->table, coordinate->row)) continue;
                VRow *content_winner = NULL, *deletion = NULL;
                jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__replacement_for(maintained_facts, &coordinate->table, coordinate->row,
                                                                                                         &content_winner, &deletion);
                if (deletion) jazz_node__node__codec__VersionRow__drop(deletion);
                if (content_winner) {
                    TxId winner_tx;
                    e = jazz_node__node__NodeState__version_tx_id(self, content_winner, &winner_tx);
                    if (e.code) { jazz_node__node__codec__VersionRow__drop(content_winner); break; }
                    if (jazz_model_TxId_eq(&winner_tx, &tx_id)) {
                        if (!jazz_node__node__codec__VersionRow__is_deleted(content_winner))
                            table_row_insert(&content_coordinates, VROW_TABLE(content_winner), VROW_ROW(content_winner));
                        JZ_VEC_PUSH(*tx_versions, content_winner);
                    } else {
                        jazz_node__node__codec__VersionRow__drop(content_winner);
                    }
                }
                if (!table_row_contains(&content_coordinates, &coordinate->table, coordinate->row)) needs_storage_fallback = true;
            }
            jz_btree_drop(&content_coordinates);
            if (e.code) goto out;
            if (needs_storage_fallback && allow_storage_witness_fallback) {
                jazz_node_StoredTransaction stored_tx;
                e = memo_tx(self, tx_id, &context, &stored_tx);
                if (e.code) goto out;
                jazz_node_VersionRowVec fallback_versions;
                e = fallback_versions_for(self, tx_id, &stored_tx, complete_exclusive_payloads, wanted_rows, &fallback_versions);
                jazz_node__node__codec__StoredTransaction__drop(&stored_tx);
                if (e.code) goto out;
                tx_versions_replace(&tx_versions_cache, tx_id, fallback_versions);
            }
            jazz_node_VersionRowVec same_transaction_deletion_winners = { 0 };
            JZ_BTREE_FOR(wanted_rows, coordinate, unit) {
                (void)unit;
                if (!table_row_tx_contains(&result_add_coordinates, &coordinate->table, coordinate->row, tx_id)) continue;
                VRow *deletion_winner = NULL;
                VRow **memo = jz_btree_get(&result_add_deletion_winners, coordinate);
                if (memo) {
                    deletion_winner = *memo ? jazz_node__node__codec__VersionRow__clone(*memo) : NULL;
                } else {
                    VRow *winner = NULL;
                    e = deletion_winner_for(self, maintained_facts, &coordinate->table, coordinate->row, allow_storage_witness_fallback, tier, &context, &winner);
                    if (e.code) break;
                    TableRow k = { jz_str_clone(&coordinate->table), coordinate->row };
                    VRow *stored = winner ? jazz_node__node__codec__VersionRow__clone(winner) : NULL;
                    jz_btree_insert(&result_add_deletion_winners, &k, &stored, NULL);
                    deletion_winner = winner;
                }
                if (deletion_winner) {
                    TxId winner_tx;
                    e = jazz_node__node__NodeState__version_tx_id(self, deletion_winner, &winner_tx);
                    if (e.code) { jazz_node__node__codec__VersionRow__drop(deletion_winner); break; }
                    if (jazz_model_TxId_eq(&winner_tx, &tx_id)) JZ_VEC_PUSH(same_transaction_deletion_winners, deletion_winner);
                    else jazz_node__node__codec__VersionRow__drop(deletion_winner);
                }
            }
            if (e.code) { vrow_vec_drop(&same_transaction_deletion_winners); goto out; }
            tx_versions = jz_btree_get(&tx_versions_cache, &tx_id);
            if (!tx_versions) jz_panic("tx versions cache entry must exist after fallback");
            for (u32 i = 0; i < same_transaction_deletion_winners.len; i++) {
                VRow *winner = same_transaction_deletion_winners.ptr[i];
                if (!maintained_view_tx_versions_contain_winner(tx_versions, winner)) JZ_VEC_PUSH(*tx_versions, winner);
                else jazz_node__node__codec__VersionRow__drop(winner);
            }
            jz_free(same_transaction_deletion_winners.ptr);
            if (tx_versions_touch_wanted(tx_versions, wanted_rows)) {
                jazz_node_StoredTransaction stored_tx;
                e = memo_tx(self, tx_id, &context, &stored_tx);
                if (e.code) goto out;
                bool exclusive = jazz_model__tx__Transaction__kind(stored_tx.tx) == JAZZ_TXKIND_EXCLUSIVE;
                /* A trusted writer link that explicitly requests complete
                 * exclusive payloads must receive the whole cross-table
                 * transaction, not one permanently redacted fragment per
                 * table subscription. Identity-scoped links remain bounded
                 * by their maintained policy witnesses even when the peer
                 * preference is enabled. */
                if (complete_exclusive_payloads && exclusive
                    && jazz_model__tx__Transaction__n_total_writes(stored_tx.tx) != tx_versions->len) {
                    jazz_node_VersionRowVec all = { 0 };
                    e = jazz_node__node__currency__NodeState__query_versions_for_tx(self, tx_id, &all);
                    if (e.code) { jazz_node__node__codec__StoredTransaction__drop(&stored_tx); goto out; }
                    vrow_vec_drop(tx_versions);
                    *tx_versions = all;
                }
                jazz_node_VersionRowVec filtered_tx_versions = { 0 };
                for (u32 i = 0; i < tx_versions->len; i++) {
                    const VRow *version = tx_versions->ptr[i];
                    if (!((complete_exclusive_payloads && exclusive) || table_row_contains(wanted_rows, VROW_TABLE(version), VROW_ROW(version)))) continue;
                    if (table_row_contains(&skipped_known_state_rows, VROW_TABLE(version), VROW_ROW(version))) continue;
                    JZ_VEC_PUSH(filtered_tx_versions, jazz_node__node__codec__VersionRow__clone(version));
                }
                if (filtered_tx_versions.len == 0) {
                    jz_free(filtered_tx_versions.ptr);
                    jazz_node__node__codec__StoredTransaction__drop(&stored_tx);
                    continue;
                }
                /* Storage-backed result membership loaded these exact
                 * immutable history records by `(table, row, tx)` above. Do
                 * not immediately reload them merely to canonicalize an IVM
                 * witness: unlike a graph-projected witness, these are
                 * already authored history rows. Keeping that distinction
                 * here preserves the cold current-row O(1) read receipt. */
                bool unambiguous = false;
                if (uses_storage_backed && needs_storage_fallback) {
                    e = exact_storage_maintained_versions_are_unambiguous(self, filtered_tx_versions.ptr, filtered_tx_versions.len, &unambiguous);
                    if (e.code) { vrow_vec_drop(&filtered_tx_versions); jazz_node__node__codec__StoredTransaction__drop(&stored_tx); goto out; }
                }
                Bundle bundle;
                e = version_bundle_for_maintained_view_versions_with_tx_and_source(self, &stored_tx, filtered_tx_versions.ptr, filtered_tx_versions.len,
                        (uses_storage_backed && needs_storage_fallback && unambiguous) ? MBVS_EXACT_STORAGE : MBVS_IVM_WITNESS, &bundle);
                vrow_vec_drop(&filtered_tx_versions);
                jazz_node__node__codec__StoredTransaction__drop(&stored_tx);
                if (e.code) goto out;
                JZ_VEC_PUSH(version_bundles, bundle);
                jazz_node__node__record_maintained_view_stream_b_add_bundle();
                continue;
            }
            e = JZN_ERR(JZN_E_MAINTAINED_VIEW_MISSING_BUNDLE_WITNESS, "add result row missing Stream B content witness");
            goto out;
        }
    }
    for (u32 i = 0; i < row_result_adds.len; i++) {
        const RowEntry *add = &row_result_adds.ptr[i];
        VRow *deletion_winner = NULL;
        TableRow probe = { *add->table, add->row };
        VRow **memo = jz_btree_get(&result_add_deletion_winners, &probe);
        if (memo) deletion_winner = *memo ? jazz_node__node__codec__VersionRow__clone(*memo) : NULL;
        else {
            e = deletion_winner_for(self, maintained_facts, add->table, add->row, allow_storage_witness_fallback, tier, &context, &deletion_winner);
            if (e.code) goto out;
        }
        if (!deletion_winner) continue;
        TxId tx_id;
        e = jazz_node__node__NodeState__version_tx_id(self, deletion_winner, &tx_id);
        if (e.code) { jazz_node__node__codec__VersionRow__drop(deletion_winner); goto out; }
        if (jazz_model_TxId_eq(&tx_id, &add->tx) || jz_btree_contains(&emitted_versions, &tx_id)) {
            jazz_node__node__codec__VersionRow__drop(deletion_winner);
            continue;
        }
        bool inserted = false;
        WinnerVec *winners = jz_btree_entry(&replacement_winners_by_tx, &tx_id, &inserted);
        Winner w = { jz_str_clone(add->table), add->row, deletion_winner, "add result row missing deletion replacement witness" };
        JZ_VEC_PUSH(*winners, w);
    }
    /* Rows that left the result or its covered sources. A deleted row
     * simply leaves; its deleted image is shipped so the reader's store
     * learns the deletion. */
    for (u32 i = 0; i < row_result_removes.len; i++)
        removed_candidate_or_insert(&removed_row_candidates, row_result_removes.ptr[i].table, row_result_removes.ptr[i].row, true,
                                        row_result_removes.ptr[i].tx, true, true);
    /* A catch-up's leaving rows name their current image, which
     * is exactly what must ship; the cursor scan below adds them. */
    if (supporting_update.tag != JAZZ_SRU_CATCH_UP) {
        for (u32 i = 0; i < supporting_update.removed.len; i++) {
            const SRow *row = &supporting_update.removed.ptr[i];
            const jz_str **table = jz_btree_get(&logical_tables, &row->physical_table);
            if (!table) continue;
            removed_candidate_or_insert(&removed_row_candidates, *table, row->row, true, row->version.tx, false, true);
        }
    }
    /* A joined occurrence leaves when one of its joined rows is deleted,
     * while its root row stays live. Nothing else names that joined row,
     * so offer it as a candidate: the storage probe below ships its
     * deleted image, and skips it when it is still live. */
    {
        const jazz_types_GlobalPhysicalTableId *id; const jz_str **table;
        JZ_BTREE_FOR(&logical_tables, id, table)
            if (!jz_str_eq(*table, &shape_query->table)) jz_btree_insert(&joined_tables, table, NULL, NULL);
    }
    if (joined_tables.len != 0) {
        for (u32 i = 0; i < inputs.result_member_removes.len; i++) {
            const jazz_protocol_RealRowMemberEntry *row = jazz_protocol__protocol__ResultMemberEntry__as_real_row(&inputs.result_member_removes.ptr[i]);
            if (!row) continue;
            const jazz_types_OutputOccurrenceId *occurrence = jazz_protocol__protocol__RealRowMemberEntry__occurrence_id(row);
            if (!occurrence) continue;
            u32 n_joined = 0;
            const jazz_types_SourceRef *joined = jazz_types__object__OutputOccurrenceId__joined_sources(occurrence, &n_joined);
            for (u32 j = 0; j < n_joined; j++) {
                jazz_types_RowUuid row_uuid = *jazz_types__object__SourceRef__uuid(joined, j);
                const jz_str **table; void *unit;
                TxId none = { 0 };
                JZ_BTREE_FOR(&joined_tables, table, unit) { (void)unit; removed_candidate_or_insert(&removed_row_candidates, *table, row_uuid, false, none, false, false); }
            }
        }
    }
    /* A reset from a known cursor cannot name the rows the reader holds,
     * so every source row that changed after the cursor is a candidate.
     * One that is not in the new set left it: like a catch-up's leaving
     * row, it ships its current image so the reader's copy stops matching. */
    for (u32 i = 0; i < supporting_update.added.len; i++) {
        const SRow *row = &supporting_update.added.ptr[i];
        const jz_str **table = jz_btree_get(&logical_tables, &row->physical_table);
        if (table) table_row_insert(&supporting_now, *table, row->row);
    }
    {
        bool has_position = has_known_state_position || inputs.has_leave_scan_after;
        jazz_types_GlobalTime position = has_known_state_position ? known_state_position : inputs.leave_scan_after;
        if ((supporting_update.tag == JAZZ_SRU_SNAPSHOT || supporting_update.tag == JAZZ_SRU_CATCH_UP) && has_position) {
            jz_btree tables; jz_btree_init(&tables, &desc_str_ptr_set);
            const jazz_types_GlobalPhysicalTableId *id; const jz_str **tp;
            JZ_BTREE_FOR(&logical_tables, id, tp) jz_btree_insert(&tables, tp, NULL, NULL);
            const jz_str **table; void *unit;
            JZ_BTREE_FOR(&tables, table, unit) {
                (void)unit;
                jz_btree changed;
                e = jazz_node__node__global_state__NodeState__global_rows_changed_after(self, *table, position, &changed);
                if (e.code) break;
                const jazz_types_RowUuid *row_uuid; void *u2;
                TxId none = { 0 };
                JZ_BTREE_FOR(&changed, row_uuid, u2) {
                    (void)u2;
                    if (!table_row_contains(&supporting_now, *table, *row_uuid)) table_row_insert(&cursor_left, *table, *row_uuid);
                    removed_candidate_or_insert(&removed_row_candidates, *table, *row_uuid, false, none, false, false);
                }
                jz_btree_drop(&changed);
            }
            jz_btree_drop(&tables);
            if (e.code) goto out;
        }
    }
    /* Rows leaving by catch-up ship their current image, deleted or not,
     * so the receiver's copy stops matching the query. */
    if (supporting_update.tag == JAZZ_SRU_CATCH_UP) {
        for (u32 i = 0; i < supporting_update.removed.len; i++) {
            const SRow *row = &supporting_update.removed.ptr[i];
            const jz_str **table = jz_btree_get(&logical_tables, &row->physical_table);
            if (table) table_row_insert(&catch_up_left, *table, row->row);
        }
    }
    {
        TableRow k; 
        while (jz_btree_pop_first(&cursor_left, &k, NULL)) {
            if (jz_btree_contains(&catch_up_left, &k)) table_row_drop(&k);
            else jz_btree_insert(&catch_up_left, &k, NULL, NULL);
        }
    }
    /* A leaving row's live image is disclosed only when this reader may
     * still read it; otherwise only a deletion ships. */
    {
        const TableRow *left; void *unit;
        JZ_BTREE_FOR(&catch_up_left, left, unit) {
            (void)unit;
            const jazz_model_TableSchema *t;
            e = jazz_node__node__NodeState__table(self, &left->table, &t);
            if (e.code) break;
            if (!t->read_policy || jazz_node__peer__read_policy_admits_every_row(t->read_policy)) continue;
            jazz_model_ValidatedQuery *rs = NULL; jazz_model_Binding *rb = NULL;
            e = jazz_node__node__views__NodeState__whole_table_shape_binding(self, &left->table, &rs, &rb);
            if (e.code) break;
            jazz_node_CurrentRowVec rows = { 0 };
            e = jazz_node__node__query_eval__NodeState__query_rows_for_link_physical_row(self, rs, rb, tier, identity, left->row, &rows);
            jazz_model__query__ValidatedQuery__drop(rs);
            jazz_model__query__Binding__drop(rb);
            if (e.code) break;
            bool readable = false;
            for (u32 i = 0; i < rows.len && !readable; i++) {
                jazz_types_RowUuid r = jazz_node__node__CurrentRow__row_uuid(&rows.ptr[i]);
                readable = jz_uuid_eq(&r, &left->row);
            }
            jz_vec_drop(&rows, sizeof(jazz_node_CurrentRow), jazz_node__node__CurrentRow__drop);
            if (!readable) table_row_insert(&unreadable_left, &left->table, left->row);
        }
        if (e.code) goto out;
        const TableRow *u; void *uu;
        JZ_BTREE_FOR(&unreadable_left, u, uu) {
            (void)uu;
            TableRow ok;
            if (jz_btree_remove(&catch_up_left, u, &ok, NULL)) table_row_drop(&ok);
        }
    }
    {
        const TableRow *candidate; const RemovedCandidate *info;
        JZ_BTREE_FOR(&removed_row_candidates, candidate, info) {
            const jz_str *entry_table = &candidate->table;
            VRow *content_winner = NULL, *deletion_winner = NULL;
            jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__replacement_for(maintained_facts, entry_table, candidate->row,
                                                                                                     &content_winner, &deletion_winner);
            if (content_winner && !info->is_member) { jazz_node__node__codec__VersionRow__drop(content_winner); content_winner = NULL; }
            if (!deletion_winner) {
                bool leaves = table_row_contains(&catch_up_left, entry_table, candidate->row);
                VRow *winner = NULL;
                e = storage_backed_current_image(self, entry_table, candidate->row, tier, &context, !leaves, &winner);
                if (e.code) { if (content_winner) jazz_node__node__codec__VersionRow__drop(content_winner); break; }
                if (winner) {
                    jazz_model_DeletionEvent ev;
                    bool deleted = jazz_node__node__codec__VersionRow__deletion(winner, &ev) && ev == JAZZ_DEL_DELETED;
                    if (!(leaves || deleted)) { jazz_node__node__codec__VersionRow__drop(winner); winner = NULL; }
                }
                if (winner) {
                    TxId wtx;
                    e = jazz_node__node__NodeState__version_tx_id(self, winner, &wtx);
                    if (e.code) { jazz_node__node__codec__VersionRow__drop(winner); if (content_winner) jazz_node__node__codec__VersionRow__drop(content_winner); break; }
                    jz_btree_insert(&storage_deletion_txs, &wtx, NULL, NULL);
                }
                deletion_winner = winner;
            }
            struct { VRow *version; const char *missing_witness; } pairs[2] = {
                { content_winner, "removed result row missing content replacement witness" },
                { deletion_winner, "removed result row missing deletion replacement witness" },
            };
            for (u32 p = 0; p < 2; p++) {
                VRow *version = pairs[p].version;
                if (!version) continue;
                TxId tx_id;
                if (!e.code) e = jazz_node__node__NodeState__version_tx_id(self, version, &tx_id);
                if (e.code || (info->has_tx && jazz_model_TxId_eq(&tx_id, &info->tx)) || jz_btree_contains(&emitted_versions, &tx_id)) {
                    jazz_node__node__codec__VersionRow__drop(version);
                    continue;
                }
                bool inserted = false;
                WinnerVec *winners = jz_btree_entry(&replacement_winners_by_tx, &tx_id, &inserted);
                Winner w = { jz_str_clone(entry_table), candidate->row, version, pairs[p].missing_witness };
                JZ_VEC_PUSH(*winners, w);
            }
            if (e.code) break;
        }
        if (e.code) goto out;
    }
    {
        TxId tx_id; WinnerVec winners;
        while (jz_btree_pop_first(&replacement_winners_by_tx, &tx_id, &winners)) {
            if (jz_btree_contains(&emitted_versions, &tx_id)) { winner_vec_drop_v(&winners); continue; }
            if (jz_btree_contains(&inputs.peer_complete_tx_payloads, &tx_id)) {
                jz_btree_insert(&emitted_versions, &tx_id, NULL, NULL);
                JZ_VEC_PUSH(peer_payload_inventory_refs, tx_id);
                jazz_node__node__record_maintained_view_removal_stream_bundle();
                winner_vec_drop_v(&winners);
                continue;
            }
            jz_btree wanted_rows; jz_btree_init(&wanted_rows, &desc_table_row_set);
            for (u32 i = 0; i < winners.len; i++) table_row_insert(&wanted_rows, &winners.ptr[i].table, winners.ptr[i].row);
            jazz_node_VersionRowVec *tx_versions = tx_versions_entry(&tx_versions_cache, tx_id, maintained_facts);
            const Winner *missing = NULL;
            for (u32 i = 0; i < winners.len && !missing; i++)
                if (!maintained_view_tx_versions_contain_winner(tx_versions, winners.ptr[i].version)) missing = &winners.ptr[i];
            jazz_node_StoredTransaction stored_tx = { 0 };
            if (missing) {
                if (!allow_storage_witness_fallback && !jz_btree_contains(&storage_deletion_txs, &tx_id)) {
                    e = JZN_ERR(JZN_E_MAINTAINED_VIEW_MISSING_BUNDLE_WITNESS, missing->missing_witness);
                    goto winners_fail;
                }
                e = memo_tx(self, tx_id, &context, &stored_tx);
                if (e.code) goto winners_fail;
                jazz_node_VersionRowVec fallback;
                e = fallback_versions_for(self, tx_id, &stored_tx, complete_exclusive_payloads, &wanted_rows, &fallback);
                jazz_node__node__codec__StoredTransaction__drop(&stored_tx);
                if (e.code) goto winners_fail;
                tx_versions_replace(&tx_versions_cache, tx_id, fallback);
            }
            e = memo_tx(self, tx_id, &context, &stored_tx);
            if (e.code) goto winners_fail;
            bool exclusive = jazz_model__tx__Transaction__kind(stored_tx.tx) == JAZZ_TXKIND_EXCLUSIVE;
            tx_versions = jz_btree_get(&tx_versions_cache, &tx_id);
            if (complete_exclusive_payloads && exclusive && (!tx_versions || jazz_model__tx__Transaction__n_total_writes(stored_tx.tx) != tx_versions->len)) {
                jazz_node_VersionRowVec all = { 0 };
                e = jazz_node__node__currency__NodeState__query_versions_for_tx(self, tx_id, &all);
                if (e.code) goto winners_tx_fail;
                tx_versions_replace(&tx_versions_cache, tx_id, all);
            }
            tx_versions = jz_btree_get(&tx_versions_cache, &tx_id);
            if (!tx_versions) jz_panic("tx versions cache entry must exist after fallback");
            for (u32 i = 0; i < winners.len; i++)
                if (!maintained_view_tx_versions_contain_winner(tx_versions, winners.ptr[i].version)) {
                    e = JZN_ERR(JZN_E_MAINTAINED_VIEW_MISSING_BUNDLE_WITNESS, winners.ptr[i].missing_witness);
                    goto winners_tx_fail;
                }
            jz_btree_insert(&emitted_versions, &tx_id, NULL, NULL);
            jazz_node_VersionRowVec bundled_versions = { 0 };
            if (complete_exclusive_payloads && exclusive) vrow_vec_clone(&bundled_versions, tx_versions);
            else for (u32 i = 0; i < tx_versions->len; i++)
                if (table_row_contains(&wanted_rows, VROW_TABLE(tx_versions->ptr[i]), VROW_ROW(tx_versions->ptr[i])))
                    JZ_VEC_PUSH(bundled_versions, jazz_node__node__codec__VersionRow__clone(tx_versions->ptr[i]));
            Bundle bundle;
            e = jazz_node__node__views__NodeState__version_bundle_for_maintained_view_versions_with_tx(self, &stored_tx, bundled_versions.ptr, bundled_versions.len, &bundle);
            vrow_vec_drop(&bundled_versions);
            if (e.code) goto winners_tx_fail;
            JZ_VEC_PUSH(version_bundles, bundle);
            jazz_node__node__record_maintained_view_removal_stream_bundle();
            jazz_node__node__codec__StoredTransaction__drop(&stored_tx);
            jz_btree_drop(&wanted_rows);
            winner_vec_drop_v(&winners);
            continue;
        winners_tx_fail:
            jazz_node__node__codec__StoredTransaction__drop(&stored_tx);
        winners_fail:
            jz_btree_drop(&wanted_rows);
            winner_vec_drop_v(&winners);
            goto out;
        }
    }
    for (u32 b = 0; b < version_bundles.len; b++) {
        Bundle *bundle = &version_bundles.ptr[b];
        if (jazz_model__tx__Transaction__kind(bundle->tx) != JAZZ_TXKIND_EXCLUSIVE) continue;
        if (complete_exclusive_payloads) continue;
        TxId btx = TXID(bundle->tx);
        const jz_btree *wanted_rows = jz_btree_get(&wanted_add_rows_by_tx, &btx);
        if (!wanted_rows) continue;
        /* Selection uses the query's logical names, while the canonical
         * wire bodies retain their authored names. Compare physical table
         * identities across renames without admitting same-UUID siblings. */
        typedef struct { jazz_types_PhysicalTableId table; jazz_types_RowUuid row; } PhysRow;
        JZ_VEC(PhysRow) wanted_physical_rows = { 0 };
        jazz_types_SchemaVersionId shape_schema = jazz_model__query__ValidatedQuery__schema_version(shape);
        const TableRow *wr; void *unit;
        JZ_BTREE_FOR(wanted_rows, wr, unit) {
            (void)unit;
            PhysRow pr = { 0, wr->row };
            e = jazz_node__node__physical__NodeState__physical_table_id_for_schema(self, shape_schema, &wr->table, &pr.table);
            if (e.code) break;
            JZ_VEC_PUSH(wanted_physical_rows, pr);
        }
        if (e.code) { jz_free(wanted_physical_rows.ptr); goto out; }
        jazz_protocol_VersionRecordVec scoped_versions = { 0 };
        jz_vec_reserve(&scoped_versions, bundle->versions.len, sizeof(VR *));
        for (u32 i = 0; i < bundle->versions.len; i++) {
            VR *version = bundle->versions.ptr[i];
            bool keep = jazz_protocol__protocol__VersionRecord__deletion(version) != JZN_DELETION_NONE;
            if (!keep) {
                jazz_types_PhysicalTableId pt;
                e = jazz_node__node__physical__NodeState__physical_table_id_for_schema(self, jazz_protocol__protocol__VersionRecord__schema_version(version),
                                                                                    jazz_protocol__protocol__VersionRecord__table(version), &pt);
                if (e.code) {
                    for (u32 j = i; j < bundle->versions.len; j++) jazz_protocol__protocol__VersionRecord__drop(bundle->versions.ptr[j]);
                    break;
                }
                jazz_types_RowUuid row = jazz_protocol__protocol__VersionRecord__row_uuid(version);
                for (u32 j = 0; j < wanted_physical_rows.len && !keep; j++)
                    keep = wanted_physical_rows.ptr[j].table == pt && jz_uuid_eq(&wanted_physical_rows.ptr[j].row, &row);
            }
            if (keep) JZ_VEC_PUSH(scoped_versions, version);
            else jazz_protocol__protocol__VersionRecord__drop(version);
        }
        jz_free(wanted_physical_rows.ptr);
        jz_free(bundle->versions.ptr);
        bundle->versions.ptr = scoped_versions.ptr; bundle->versions.len = scoped_versions.len; bundle->versions.cap = scoped_versions.cap;
        if (e.code) goto out;
        jazz_model__tx__Transaction__set_n_total_writes(bundle->tx, bundle->versions.len);
        bundle->scope = JAZZ_BUNDLE_SCOPE_VIEW_SCOPED;
    }
    {
        jazz_protocol_VersionCarrierPtrVec version_carriers = { 0 };
        e = jazz_protocol__protocol__build_version_carriers_from_singletons(version_bundles, &version_carriers);
        version_bundles = (jazz_protocol_VersionBundleVec){ 0 };
        if (e.code) { jz_err_drop(&e); e = JZN_INVALID_STORED("failed to build version-bundle run"); goto out; }
        jazz_protocol_ViewUpdatePayload payload;
        payload.subscription = inputs.subscription;
        payload.settled_through = inputs.settled_through;
        payload.version_carriers = version_carriers;
        payload.peer_payload_inventory.complete_tx_payloads = peer_payload_inventory_refs;
        payload.peer_payload_inventory.has_authorization_progress = false;
        payload.peer_payload_inventory.authorization_progress = 0;
        payload.peer_payload_inventory.opening_pending = false;
        payload.supporting_rows = supporting_update;
        peer_payload_inventory_refs = (jazz_model_TxIdVec){ 0 };
        supporting_update = (jazz_protocol_SupportingRowsUpdate){ 0 };
        *out = jazz_protocol__protocol__SyncMessage__ViewUpdate(payload);
    }
out:
    jazz_protocol__protocol__SupportingRowsUpdate__drop(&supporting_update);
    jz_btree_drop(&logical_tables); jz_btree_drop(&tx_versions_cache); jz_btree_drop(&skipped_known_state_rows);
    jz_btree_drop(&wanted_add_rows_by_tx); jz_btree_drop(&resettled_rows); jz_btree_drop(&exit_candidates);
    jz_btree_drop(&emitted_versions); jz_btree_drop(&result_add_deletion_winners); jz_btree_drop(&result_add_coordinates);
    jz_btree_drop(&replacement_winners_by_tx); jz_btree_drop(&storage_deletion_txs); jz_btree_drop(&removed_row_candidates);
    jz_btree_drop(&joined_tables); jz_btree_drop(&supporting_now); jz_btree_drop(&cursor_left);
    jz_btree_drop(&catch_up_left); jz_btree_drop(&unreadable_left);
    jazz_node__node__policy__ViewEvaluationContext__drop(&context);
    jz_free(row_result_adds.ptr); jz_free(row_result_removes.ptr);
    jz_vec_drop(&version_bundles, sizeof(Bundle), bundle_drop_v);
    jz_free(peer_payload_inventory_refs.ptr);
    if (read_shape) jazz_model__query__ValidatedQuery__drop(read_shape);
    if (read_binding) jazz_model__query__Binding__drop(read_binding);
    jz_btree_drop(&inputs.peer_complete_tx_payloads);
    jz_btree_drop(&inputs.previous_result_set);
    jz_vec_drop(&inputs.result_member_adds, sizeof(jazz_protocol_ResultMemberEntry), jazz_protocol__protocol__ResultMemberEntry__drop);
    jz_vec_drop(&inputs.result_member_removes, sizeof(jazz_protocol_ResultMemberEntry), jazz_protocol__protocol__ResultMemberEntry__drop);
    return e;
}

/* ---------------- receiver: apply view updates ---------------- */
static jz_err validate_received_view_update_global_time_durability(NS *self, const jazz_node_ViewUpdateParts *update) {
    (void)self;
    jazz_protocol_VersionBundleRefVec refs;
    JZ_TRY(version_bundle_refs_for_carriers(update->version_carriers.ptr, update->version_carriers.len, &refs));
    jz_err e = JZ_OK;
    for (u32 i = 0; i < refs.len && !e.code; i++)
        e = jazz_node__node__ingest__validate_received_view_bundle_global_time_durability(refs.ptr[i].has_global_time, refs.ptr[i].global_time, refs.ptr[i].durability);
    jz_free(refs.ptr);
    return e;
}

static int srow_cmp(const void *a, const void *b, void *ctx) { return jazz_protocol__protocol__SupportingRow__cmp(a, b, ctx); }
static const jz_btree_desc desc_srow_ref_set = { sizeof(const SRow *), 0, NULL, NULL, NULL };
static int srow_ptr_cmp(const void *a, const void *b, void *ctx) { return srow_cmp(*(const SRow *const *)a, *(const SRow *const *)b, ctx); }

/* Validate all row bundles carried by a receiver frame without changing
 * storage or in-memory receiver state. */
static jz_err validate_view_update_payloads(NS *self, const jazz_node_ViewUpdateParts *updates, u32 n) {
    jz_btree descriptors;
    jz_btree_init(&descriptors, &jazz_node__node__ingest__PREPARED_DESCRIPTORS_DESC);
    jz_btree_desc set_desc = desc_srow_ref_set;
    set_desc.cmp = srow_ptr_cmp;
    jz_err e = JZ_OK;
    for (u32 u = 0; u < n && !e.code; u++) {
        const jazz_node_ViewUpdateParts *update = &updates[u];
        u32 n_adds = 0, n_removes = 0;
        const SRow *adds = jazz_node__node__ViewUpdateParts__supporting_adds(update, &n_adds);
        const SRow *removes = jazz_node__node__ViewUpdateParts__supporting_removes(update, &n_removes);
        /* An opening-pending marker makes no source or body claim. It
         * must not mutate a prior closure while withholding settlement. */
        if (update->opening_pending && (n_adds != 0 || n_removes != 0 || update->version_carriers.len != 0
                                        || update->peer_complete_tx_payload_refs.len != 0)) {
            e = invalid_closure_c(update->subscription, "opening-pending marker carries source data");
            break;
        }
        /* INV-SYNC-36 has one peer result model: the authority sends an
         * exact covered source closure and the receiver derives its own
         * terminal. Result-member deltas and payload rows are authority
         * output, not receiver input; accepting either would revive the
         * retired snapshot/materialization bypass. */
        if (update->result_member_adds.len != 0 || update->result_member_removes.len != 0) {
            e = invalid_closure_c(update->subscription, "authority view update carries retired result members");
            break;
        }
        jz_btree added, removed;
        jz_btree_init(&added, &set_desc); jz_btree_init(&removed, &set_desc);
        for (u32 i = 0; i < n_adds; i++) { const SRow *r = &adds[i]; jz_btree_insert(&added, &r, NULL, NULL); }
        for (u32 i = 0; i < n_removes; i++) { const SRow *r = &removes[i]; jz_btree_insert(&removed, &r, NULL, NULL); }
        bool disjoint = true;
        const SRow **k; void *unit;
        JZ_BTREE_FOR(&added, k, unit) { (void)unit; if (jz_btree_contains(&removed, k)) { disjoint = false; break; } }
        bool wire_valid = true;
        for (u32 i = 0; i < n_adds && wire_valid; i++) wire_valid = jazz_protocol__protocol__SupportingRow__is_wire_valid(&adds[i]);
        for (u32 i = 0; i < n_removes && wire_valid; i++) wire_valid = jazz_protocol__protocol__SupportingRow__is_wire_valid(&removes[i]);
        bool bad = added.len != n_adds || removed.len != n_removes || !disjoint || !wire_valid;
        jz_btree_drop(&added); jz_btree_drop(&removed);
        if (bad) { e = invalid_closure_c(update->subscription, "invalid or duplicate supporting physical row version"); break; }
        jazz_protocol_VersionBundleRefVec refs;
        e = version_bundle_refs_for_carriers(update->version_carriers.ptr, update->version_carriers.len, &refs);
        if (e.code) break;
        for (u32 i = 0; i < refs.len && !e.code; i++) {
            /* This preflight runs before bulk-reset selection, alias
             * allocation, clock advancement, or receiver staging. The
             * shared transaction boundary keeps view payloads from being
             * a durable-ingress bypass for operation provenance. */
            groove_Value merge;
            e = jazz_node__node__NodeState__admit_contribution_merge_for_storage(self, refs.ptr[i].tx, &merge);
            if (e.code) break;
            groove__records__values__Value__drop(&merge);
            e = jazz_node__node__ingest__NodeState__validate_view_payload_versions_prepared(self, refs.ptr[i].versions, refs.ptr[i].n_versions, &descriptors);
        }
        jz_free(refs.ptr);
    }
    jz_btree_drop(&descriptors);
    return e;
}

/* Only an explicit physical snapshot replaces a scope. Metadata-only
 * opening/deferred-completion stamps never clear retained membership. */
static bool reset_replaces_authority_source_closure(bool reset, bool has_scope, bool opening_pending) {
    return reset && has_scope && !opening_pending;
}

static void compiled_tables_drop_v(void *p) { jz_btree_drop(p); }
static const jz_btree_desc desc_compiled_caches = {
    sizeof(jazz_protocol_AuthorityResultKey), sizeof(jz_btree), jazz_protocol__protocol__AuthorityResultKey__cmp,
    jazz_protocol__protocol__AuthorityResultKey__drop, compiled_tables_drop_v
};
/* (cleared, BTreeMap<CoveredInputCoordinate, Option<SupportingRow>>) */
typedef struct { bool cleared; jz_btree changes; } Overlay;
typedef struct { bool some; SRow row; } OptSRow;
static void opt_srow_drop_v(void *p) { OptSRow *o = p; if (o->some) jazz_protocol__protocol__SupportingRow__drop(&o->row); }
static const jz_btree_desc desc_overlay_changes = {
    sizeof(jazz_node_CoveredInputCoordinate), sizeof(OptSRow), jazz_node__node__CoveredInputCoordinate__cmp,
    jazz_node__node__CoveredInputCoordinate__drop, opt_srow_drop_v
};
static void overlay_drop_v(void *p) { jz_btree_drop(&((Overlay *)p)->changes); }
static const jz_btree_desc desc_overlays = {
    sizeof(jazz_protocol_AuthorityResultKey), sizeof(Overlay), jazz_protocol__protocol__AuthorityResultKey__cmp,
    jazz_protocol__protocol__AuthorityResultKey__drop, overlay_drop_v
};

/* Stage only changed coordinates; no full predecessor copy for deltas. */
static jz_err validate_covered_input_closure_admission(NS *self, const jazz_node_ViewUpdateParts *updates, u32 n,
                                                      jz_btree *caches /* AuthorityResultKey -> CompiledScopeTables */) {
    const char *receiver_role = jazz_node__node__NodeState__client_relay_scope(self) ? "client relay" : "client";
    jz_btree_init(caches, &desc_compiled_caches);
    jz_btree overlays; jz_btree_init(&overlays, &desc_overlays);
    jz_err e = JZ_OK;
    for (u32 u = 0; u < n && !e.code; u++) {
        const jazz_node_ViewUpdateParts *update = &updates[u];
        jazz_protocol_AuthorityResultKey key;
        jz_err ke = jazz_node__node__query_eval__subscriptions__NodeState__authority_result_key_for_subscription(self, update->subscription, &key);
        if (ke.code) { jz_err_drop(&ke); continue; }
        const jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, &key);
        const jz_btree *tables = NULL;
        if (state && state->has_compiled_covered_input_sources) tables = &state->compiled_covered_input_sources;
        else {
            if (!jz_btree_contains(caches, &key)) {
                jz_btree compiled;
                e = jazz_node__node__query_eval__NodeState__compiled_covered_input_sources_for_subscription(self, update->subscription, &compiled);
                if (e.code) { jazz_protocol__protocol__AuthorityResultKey__drop(&key); break; }
                jazz_protocol_AuthorityResultKey k; jazz_protocol__protocol__AuthorityResultKey__clone(&k, &key);
                jz_btree_insert(caches, &k, &compiled, NULL);
            }
            tables = jz_btree_get(caches, &key);
        }
        bool inserted = false;
        Overlay *overlay = jz_btree_get(&overlays, &key);
        if (!overlay) {
            Overlay fresh = { false };
            jz_btree_init(&fresh.changes, &desc_overlay_changes);
            jazz_protocol_AuthorityResultKey k; jazz_protocol__protocol__AuthorityResultKey__clone(&k, &key);
            jz_btree_insert(&overlays, &k, &fresh, NULL);
            overlay = jz_btree_get(&overlays, &key);
        }
        (void)inserted;
        jazz_protocol__protocol__AuthorityResultKey__drop(&key);
        if (reset_replaces_authority_source_closure(update->reset_input_set, update->wire_rows != NULL, update->opening_pending)) {
            overlay->cleared = true;
            jz_btree_clear(&overlay->changes);
        }
        for (u32 pass = 0; pass < 2 && !e.code; pass++) {
            bool adding = pass == 1;
            u32 n_rows = 0;
            const SRow *rows = adding ? jazz_node__node__ViewUpdateParts__supporting_adds(update, &n_rows)
                                      : jazz_node__node__ViewUpdateParts__supporting_removes(update, &n_rows);
            for (u32 i = 0; i < n_rows; i++) {
                const SRow *row = &rows[i];
                if (!jz_btree_contains(tables, &row->physical_table)) {
                    e = invalid_closure_c(update->subscription, "supporting physical table is outside compiled scope");
                    break;
                }
                jazz_node_CoveredInputCoordinate coordinate;
                jazz_node__node__CoveredInputCoordinate__From__from(&coordinate, row);
                const SRow *current = NULL;
                const OptSRow *changed = jz_btree_get(&overlay->changes, &coordinate);
                if (changed) current = changed->some ? &changed->row : NULL;
                else if (!overlay->cleared && state) current = jz_btree_get(&state->covered_input_versions, &coordinate);
                if (adding) {
                    if (current) {
                        jz_str retained = supporting_row_diagnostic(current), added = supporting_row_diagnostic(row);
                        jz_str msg = jz_format("scope addition duplicates a retained physical coordinate (receiver %s: retained %.*s, added %.*s)",
                                               receiver_role, (int)retained.len, retained.ptr, (int)added.len, added.ptr);
                        e = invalid_closure(update->subscription, msg.ptr, msg.len);
                        jz_str_drop(&retained); jz_str_drop(&added); jz_str_drop(&msg);
                        jazz_node__node__CoveredInputCoordinate__drop(&coordinate);
                        break;
                    }
                    OptSRow v = { true }; jazz_protocol__protocol__SupportingRow__clone(&v.row, row);
                    OptSRow old;
                    if (jz_btree_insert(&overlay->changes, &coordinate, &v, &old)) { jazz_node__node__CoveredInputCoordinate__drop(&coordinate); opt_srow_drop_v(&old); }
                } else {
                    if (!current || !jazz_protocol__protocol__SupportingRow__eq(current, row)) {
                        jz_str retained = current ? supporting_row_diagnostic(current) : jz_str_from("none"), removed = supporting_row_diagnostic(row);
                        jz_str msg = jz_format("scope removal is absent from exact predecessor (receiver %s: retained %.*s, removed %.*s)",
                                               receiver_role, (int)retained.len, retained.ptr, (int)removed.len, removed.ptr);
                        e = invalid_closure(update->subscription, msg.ptr, msg.len);
                        jz_str_drop(&retained); jz_str_drop(&removed); jz_str_drop(&msg);
                        jazz_node__node__CoveredInputCoordinate__drop(&coordinate);
                        break;
                    }
                    OptSRow v = { false }, old;
                    if (jz_btree_insert(&overlay->changes, &coordinate, &v, &old)) { jazz_node__node__CoveredInputCoordinate__drop(&coordinate); opt_srow_drop_v(&old); }
                }
            }
        }
    }
    jz_btree_drop(&overlays);
    if (e.code) jz_btree_drop(caches);
    return e;
}

static void install_compiled_covered_input_source_caches(NS *self, jz_btree *caches) {
    jazz_protocol_AuthorityResultKey key; jz_btree compiled;
    while (jz_btree_pop_first(caches, &key, &compiled)) {
        jazz_node_AuthorityResultState *state = authority_state_entry(self, &key);
        jz_btree_drop(&state->compiled_covered_input_sources);
        state->compiled_covered_input_sources = compiled;
        state->has_compiled_covered_input_sources = true;
        jazz_protocol__protocol__AuthorityResultKey__drop(&key);
    }
    jz_btree_drop(caches);
}

/* (TxId, String, RowUuid, Vec<u8>) */
typedef struct { TxId tx; jz_str table; jazz_types_RowUuid row; jz_bytes branch; } AdmittedVersion;
static int admitted_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const AdmittedVersion *x = a, *y = b;
    int c = jazz_model_TxId_cmp(&x->tx, &y->tx); if (c) return c;
    c = jz_str_cmp(&x->table, &y->table); if (c) return c;
    c = jz_uuid_cmp(&x->row, &y->row); if (c) return c;
    return jz_bytes_cmp(&x->branch, &y->branch);
}
static void admitted_drop(void *p) { AdmittedVersion *v = p; jz_str_drop(&v->table); jz_bytes_drop(&v->branch); }
static const jz_btree_desc desc_admitted = { sizeof(AdmittedVersion), 0, admitted_cmp, admitted_drop, NULL };
static void admitted_insert(jz_btree *set, TxId tx, const jz_str *table, jazz_types_RowUuid row, jz_bytes branch) {
    AdmittedVersion v = { tx, jz_str_clone(table), row, branch };
    if (jz_btree_insert(set, &v, NULL, NULL)) admitted_drop(&v);
}

static jz_err covered_input_has_resident_body(NS *self, const SRow *input, jazz_types_SchemaVersionId read_schema, bool *out) {
    *out = false;
    bool found = false;
    jazz_node_StoredTransaction stored;
    JZ_TRY(jazz_node__node__currency__NodeState__query_transaction(self, input->version.tx, &found, &stored));
    if (!found) return JZ_OK;
    jazz_node__node__codec__StoredTransaction__drop(&stored);
    VRow *version = NULL;
    JZ_TRY(jazz_node__node__views__NodeState__covered_input_version(self, input, read_schema, &version));
    *out = version != NULL;
    if (version) jazz_node__node__codec__VersionRow__drop(version);
    return JZ_OK;
}

static jz_err validate_covered_input_body_witnesses(NS *self, const jazz_node_ViewUpdateParts *updates, u32 n, const jz_btree *staged_bundles) {
    jz_btree referenced_complete_payloads; jz_btree_init(&referenced_complete_payloads, &desc_txid_set);
    for (u32 u = 0; u < n; u++)
        for (u32 i = 0; i < updates[u].peer_complete_tx_payload_refs.len; i++)
            jz_btree_insert(&referenced_complete_payloads, &updates[u].peer_complete_tx_payload_refs.ptr[i], NULL, NULL);
    jz_btree admitted_versions; jz_btree_init(&admitted_versions, &desc_admitted);
    const TxId *txp; const Bundle *bundle;
    JZ_BTREE_FOR(staged_bundles, txp, bundle) {
        for (u32 i = 0; i < bundle->versions.len; i++) {
            const VR *version = bundle->versions.ptr[i];
            jz_bytes branch;
            jazz_model__model__branch__BranchKey__canonical_bytes(jazz_protocol__protocol__VersionRecord__branch_key(version), &branch);
            admitted_insert(&admitted_versions, TXID(bundle->tx), jazz_protocol__protocol__VersionRecord__table(version),
                            jazz_protocol__protocol__VersionRecord__row_uuid(version), branch);
        }
    }
    jz_err e = JZ_OK;
    /* Complete-payload inventory references are the only valid exception
     * to a same-frame body: the receiver already admitted that complete
     * immutable transaction on this link. Re-read its stored bodies before
     * accepting a source coordinate, rather than trusting the sender's
     * declaration alone. */
    void *unit;
    JZ_BTREE_FOR(&referenced_complete_payloads, txp, unit) {
        (void)unit;
        jazz_node_VersionRowVec versions = { 0 };
        e = jazz_node__node__currency__NodeState__query_versions_for_tx(self, *txp, &versions);
        if (e.code) break;
        for (u32 i = 0; i < versions.len; i++) {
            jz_bytes branch;
            jazz_model__model__branch__BranchKey__canonical_bytes(jazz_node__node__codec__VersionRow__branch_key(versions.ptr[i]), &branch);
            admitted_insert(&admitted_versions, *txp, VROW_TABLE(versions.ptr[i]), VROW_ROW(versions.ptr[i]), branch);
        }
        vrow_vec_drop(&versions);
    }
    for (u32 u = 0; u < n && !e.code; u++) {
        const jazz_node_ViewUpdateParts *update = &updates[u];
        u32 n_adds = 0;
        const SRow *adds = jazz_node__node__ViewUpdateParts__supporting_adds(update, &n_adds);
        for (u32 i = 0; i < n_adds; i++) {
            const SRow *input = &adds[i];
            AdmittedVersion coordinate = { input->version.tx, *input->version_table, input->row,
                                           input->version.has_branch_or_prefix ? input->version.branch_or_prefix : (jz_bytes){ 0 } };
            bool staged_body_witness = jz_btree_contains(&admitted_versions, &coordinate);
            /* A known-state repair may have admitted this exact immutable
             * body in its own request/response frame before the later view
             * frame that consumes it. Use the same physical lookup, concrete
             * layer, and canonical-branch check as receiver materialization;
             * a merely matching tx/row name is not evidence. */
            bool resident_body_witness = false;
            if (!staged_body_witness) {
                jazz_model_ValidatedQuery *const *shape = jz_btree_get(&self->query.registered_shapes, &update->subscription.shape_id);
                if (!shape) { e = JZN_INVALID_STORED("covered input shape is not registered"); break; }
                jazz_types_SchemaVersionId read_schema = jazz_model__query__ValidatedQuery__schema_version(*shape);
                e = covered_input_has_resident_body(self, input, read_schema, &resident_body_witness);
                if (e.code) break;
            }
            bool batch_is_tx = input->version.has_batch && jazz_model_TxId_eq(&input->version.batch, &input->version.tx);
            if (!batch_is_tx || (!staged_body_witness && !resident_body_witness)) {
                e = invalid_closure_c(update->subscription, "covered input is not witnessed by admitted payload");
                break;
            }
        }
    }
    jz_btree_drop(&admitted_versions);
    jz_btree_drop(&referenced_complete_payloads);
    return e;
}

/* Resolve an exact immutable witness using the receiver source's physical
 * table, then verify its authored name. A rename changes the name, not
 * the physical table, row, layer, branch, or transaction being requested. */
jz_err jazz_node__node__views__NodeState__covered_input_version(NS *self, const SRow *input, jazz_types_SchemaVersionId read_schema, VRow **out) {
    *out = NULL;
    const jazz_types_NodeAlias *alias = jazz_node__node__node_aliases__NodeAliases__get(&self->node_aliases, &input->version.tx.node);
    if (!alias) return JZ_OK;
    jazz_types_NodeAlias tx_node_alias = *alias;
    /* Content and deletion witnesses name the same stored row image. */
    switch (input->version.layer) {
    case JAZZ_LAYER_CONTENT: case JAZZ_LAYER_DELETION: break;
    case JAZZ_LAYER_CONTENT_OR_DELETION: return JZN_INVALID_STORED("covered input must name one concrete version layer");
    }
    jazz_model_BranchKey *branch_key = NULL;
    if (input->version.has_branch_or_prefix) {
        jz_err be = jazz_model__model__branch__BranchKey__from_canonical_bytes(jz_bytes_slice(&input->version.branch_or_prefix), &branch_key);
        if (be.code) { jz_err_drop(&be); return JZN_INVALID_STORED("covered input branch witness is malformed"); }
    } else {
        branch_key = jazz_model__model__branch__BranchKey__default();
    }
    groove_InternStr logical;
    jz_err e = jazz_node__node__views__NodeState__scope_logical_table(self, read_schema, input->physical_table, &logical);
    VRow *version = NULL;
    if (!e.code) e = jazz_node__node__currency__NodeState__query_version_by_alias_in_branch(self, read_schema, logical, branch_key, input->row,
                                                                                          input->version.tx.time, tx_node_alias, &version);
    jazz_model__model__branch__BranchKey__drop(branch_key);
    if (e.code || !version) return e;
    if (!jz_str_eq(VROW_TABLE(version), input->version_table)) {
        jazz_node__node__codec__VersionRow__drop(version);
        return JZN_INVALID_STORED("authority covered input authored table disagrees with stored version");
    }
    jz_bytes stored_branch;
    jazz_model__model__branch__BranchKey__canonical_bytes(jazz_node__node__codec__VersionRow__branch_key(version), &stored_branch);
    jz_bytes witness = input->version.has_branch_or_prefix ? input->version.branch_or_prefix : (jz_bytes){ 0 };
    bool same = jz_bytes_eq(&stored_branch, &witness);
    jz_bytes_drop(&stored_branch);
    if (!same) {
        jazz_node__node__codec__VersionRow__drop(version);
        return JZN_INVALID_STORED("authority covered input branch witness disagrees with stored version");
    }
    *out = version;
    return JZ_OK;
}

/* Clone a bundle's records into an owned VersionRecordVec. */
static jazz_protocol_VersionRecordVec clone_versions(const Bundle *bundle) {
    jazz_protocol_VersionRecordVec v = { 0 };
    for (u32 i = 0; i < bundle->versions.len; i++) JZ_VEC_PUSH(v, jazz_protocol__protocol__VersionRecord__clone(bundle->versions.ptr[i]));
    return v;
}
static jazz_protocol_VersionRecordVec take_versions(Bundle *bundle) {
    jazz_protocol_VersionRecordVec v = { bundle->versions.ptr, bundle->versions.len, bundle->versions.cap };
    bundle->versions.ptr = NULL; bundle->versions.len = bundle->versions.cap = 0;
    return v;
}

static jz_err ingest_view_bundle(NS *self, Bundle bundle) {
    jz_err e = jazz_node__node__ingest__validate_received_view_bundle_global_time_durability(bundle.has_global_time, bundle.global_time, bundle.durability);
    if (e.code) goto out;
    /* View carriers may replay a transaction learned under a local
     * permission capability. They never transport that capability into a
     * receiving store or onward view publication. */
    {
        jazz_model_Transaction *redacted = jazz_node__node__codec__transaction_without_permission_subject(bundle.tx);
        jazz_model__tx__Transaction__drop(bundle.tx);
        bundle.tx = redacted;
    }
    if (jazz_model__tx__Transaction__n_total_writes(bundle.tx) != bundle.versions.len) {
        e = JZN_MALFORMED_VIEW("version bundle count does not match its declared scope payload");
        goto out;
    }
    TxId tx_id = TXID(bundle.tx);
    bool found = false;
    jazz_node_StoredTransaction stored;
    e = jazz_node__node__currency__NodeState__query_transaction(self, tx_id, &found, &stored);
    if (e.code) goto out;
    if (found) {
        jazz_model__tx__Transaction__set_n_total_writes(stored.tx, 0);
        jazz_model_Transaction *stored_identity = jazz_node__node__codec__transaction_without_permission_subject(stored.tx);
        jazz_model_Transaction *incoming_identity = jazz_model__tx__Transaction__clone(bundle.tx);
        jazz_model__tx__Transaction__set_n_total_writes(incoming_identity, 0);
        /* The author or relay of a pending exclusive transaction stores
         * its read evidence; view carriers never ship it. Compare the
         * payload, not the local-only evidence. */
        bool matches = jazz_node__node__codec__known_transaction_payload_matches(stored_identity, incoming_identity);
        jazz_model__tx__Transaction__drop(stored_identity);
        jazz_model__tx__Transaction__drop(incoming_identity);
        jazz_node__node__codec__StoredTransaction__drop(&stored);
        if (!matches) { e = jazz_node__node__Error__conflicting_commit_unit(&tx_id); goto out; }
    }
    if (jazz_model__tx__Transaction__kind(bundle.tx) != JAZZ_TXKIND_EXCLUSIVE) {
        jazz_model_Transaction *tx = bundle.tx; bundle.tx = NULL;
        jazz_protocol_VersionRecordVec versions = take_versions(&bundle);
        if (bundle.scope == JAZZ_BUNDLE_SCOPE_VIEW_SCOPED)
            return jazz_node__node__ingest__NodeState__ingest_view_scoped_transaction_with_current_indexes(self, tx, versions, bundle.fate,
                       bundle.has_global_time, bundle.global_time, bundle.durability);
        return jazz_node__node__ingest__NodeState__ingest_known_transaction(self, tx, versions, bundle.fate, bundle.has_global_time,
                                                                           bundle.global_time, bundle.durability);
    }
    if (bundle.scope == JAZZ_BUNDLE_SCOPE_VIEW_SCOPED) {
        bool has_stored = false;
        e = jazz_node__node__currency__NodeState__query_transaction(self, tx_id, &has_stored, &stored);
        if (e.code) goto out;
        /* A bulk reset installs its authorized fragment as locally current
         * so a relay can serve it onward. Further authorized siblings from
         * that same view-scoped transaction must extend that projection;
         * fragments first learned outside such a reset remain history-only. */
        bool extend_current_view = has_stored && stored.is_partial;
        if (has_stored) jazz_node__node__codec__StoredTransaction__drop(&stored);
        jz_btree known_keys; jz_btree_init(&known_keys, &desc_view_version_key_set);
        if (has_stored) {
            jz_btree stored_by_key;
            e = stored_versions_by_key(self, tx_id, &stored_by_key);
            RecordKey *k; VR **v;
            JZ_BTREE_FOR(&stored_by_key, k, v) view_version_key_insert(&known_keys, *v);
            jz_btree_drop(&stored_by_key);
            if (e.code) { jz_btree_drop(&known_keys); goto out; }
        }
        for (u32 i = 0; i < bundle.versions.len; i++) view_version_key_insert(&known_keys, bundle.versions.ptr[i]);
        jazz_model_Transaction *redacted_tx = bundle.tx; bundle.tx = NULL;
        jazz_model__tx__Transaction__set_n_total_writes(redacted_tx, known_keys.len);
        jz_btree_drop(&known_keys);
        jazz_protocol_VersionRecordVec versions = take_versions(&bundle);
        if (extend_current_view)
            return jazz_node__node__ingest__NodeState__ingest_view_scoped_transaction_with_current_indexes(self, redacted_tx, versions, bundle.fate,
                       bundle.has_global_time, bundle.global_time, bundle.durability);
        return jazz_node__node__ingest__NodeState__ingest_transaction_fragment_without_current_indexes(self, redacted_tx, versions, bundle.fate,
                   bundle.has_global_time, bundle.global_time, bundle.durability);
    }
    u32 complete_len = jazz_model__tx__Transaction__n_total_writes(bundle.tx);
    jazz_protocol_VersionRecordVec stored_versions = { 0 };
    {
        bool has_stored = false;
        e = jazz_node__node__currency__NodeState__query_transaction(self, tx_id, &has_stored, &stored);
        if (e.code) goto out;
        if (has_stored) {
            jazz_node__node__codec__StoredTransaction__drop(&stored);
            jazz_node_VersionRowVec rows = { 0 };
            e = jazz_node__node__currency__NodeState__query_versions_for_tx(self, tx_id, &rows);
            for (u32 i = 0; i < rows.len && !e.code; i++) {
                VR *v = NULL;
                e = jazz_node__node__NodeState__version_record_from_row(self, rows.ptr[i], &v);
                if (!e.code) JZ_VEC_PUSH(stored_versions, v);
            }
            vrow_vec_drop(&rows);
            if (e.code) { vr_vec_drop(&stored_versions); goto out; }
        }
    }
    jz_btree known_keys; jz_btree_init(&known_keys, &desc_view_version_key_set);
    for (u32 i = 0; i < stored_versions.len; i++) view_version_key_insert(&known_keys, stored_versions.ptr[i]);
    for (u32 i = 0; i < bundle.versions.len; i++) view_version_key_insert(&known_keys, bundle.versions.ptr[i]);
    u32 known = known_keys.len;
    jz_btree_drop(&known_keys);
    if (known > complete_len) { vr_vec_drop(&stored_versions); e = jazz_node__node__Error__conflicting_commit_unit(&tx_id); goto out; }
    if (known == complete_len) {
        jazz_protocol_VersionRecordVec complete_versions = { 0 };
        jz_btree complete_keys; jz_btree_init(&complete_keys, &desc_view_version_key_set);
        /* The delivered versions come first: for an accepted transaction
         * they are the authority's post-images, which replace this node's
         * own copy (for the originator, its patch or its local merge). */
        for (u32 pass = 0; pass < 2; pass++) {
            const jazz_protocol_VersionRecordVec *src = pass ? &stored_versions : (const jazz_protocol_VersionRecordVec *)&bundle.versions;
            for (u32 i = 0; i < src->len; i++) {
                u32 before = complete_keys.len;
                view_version_key_insert(&complete_keys, src->ptr[i]);
                if (complete_keys.len != before) JZ_VEC_PUSH(complete_versions, jazz_protocol__protocol__VersionRecord__clone(src->ptr[i]));
            }
        }
        jz_btree_drop(&complete_keys);
        vr_vec_drop(&stored_versions);
        jazz_model_Fate fate; jazz_model__tx__Fate__clone(&fate, &bundle.fate);
        jazz_model_Transaction *tx = bundle.tx; bundle.tx = NULL;
        e = jazz_node__node__ingest__NodeState__ingest_known_transaction(self, tx, complete_versions, fate, bundle.has_global_time, bundle.global_time, bundle.durability);
        goto out;
    }
    vr_vec_drop(&stored_versions);
    {
        jazz_model_Transaction *tx = bundle.tx; bundle.tx = NULL;
        jazz_protocol_VersionRecordVec versions = take_versions(&bundle);
        return jazz_node__node__ingest__NodeState__ingest_transaction_fragment_without_current_indexes(self, tx, versions, bundle.fate,
                   bundle.has_global_time, bundle.global_time, bundle.durability);
    }
out:
    if (bundle.tx) jazz_protocol__protocol__VersionBundle__drop(&bundle);
    else { jz_vec_drop(&bundle.versions, sizeof(VR *), vr_drop_v); jazz_model__tx__Fate__drop(&bundle.fate); }
    return e;
}

static jz_err stage_view_bundle(NS *self, groove_DatabaseBatch *batch, const Bundle *bundle, jz_btree *staged_tx_ids,
                                jazz_node_GlobalTimeTxVec *staged_global_times, jazz_node_VersionRowVec *staged_content_versions,
                                jazz_node_RejectedTransactionVec *staged_rejections, bool *staged) {
    *staged = true;
    JZ_TRY(jazz_node__node__ingest__validate_received_view_bundle_global_time_durability(bundle->has_global_time, bundle->global_time, bundle->durability));
    if (jazz_model__tx__Transaction__n_total_writes(bundle->tx) != bundle->versions.len)
        return JZN_MALFORMED_VIEW("version bundle count does not match its declared scope payload");
    TxId tx_id = TXID(bundle->tx);
    jazz_model_Fate fate;
    if (bundle->scope == JAZZ_BUNDLE_SCOPE_VIEW_SCOPED) {
        /* A view-scoped exclusive fragment may extend a projection already
         * installed by an earlier reset. Its redacted cardinality is
         * deliberately not a whole-transaction completeness claim, so do
         * not take the complete-exclusive fast path below merely because
         * the transaction itself is already known. */
        if (jz_btree_insert(staged_tx_ids, &tx_id, NULL, NULL)) return JZ_OK;
        jazz_model__tx__Fate__clone(&fate, &bundle->fate);
        return jazz_node__node__ingest__NodeState__stage_view_scoped_transaction_with_current_indexes(self, batch, jazz_model__tx__Transaction__clone(bundle->tx),
                   clone_versions(bundle), fate, bundle->has_global_time, bundle->global_time, bundle->durability,
                   staged_global_times, staged_content_versions, staged_rejections);
    }
    /* Known complete transactions take the fate/update path below, after
     * this batch commits. That path may publish independently and must not
     * invalidate exact-match preparation for preceding new transactions. */
    bool found = false;
    jazz_node_StoredTransaction stored;
    JZ_TRY(jazz_node__node__currency__NodeState__query_transaction(self, tx_id, &found, &stored));
    if (found) { jazz_node__node__codec__StoredTransaction__drop(&stored); *staged = false; return JZ_OK; }
    if (jazz_model__tx__Transaction__kind(bundle->tx) == JAZZ_TXKIND_EXCLUSIVE
        && bundle->versions.len != jazz_model__tx__Transaction__n_total_writes(bundle->tx)) {
        *staged = false;
        return JZ_OK;
    }
    if (jz_btree_insert(staged_tx_ids, &tx_id, NULL, NULL)) return JZ_OK;
    jazz_model__tx__Fate__clone(&fate, &bundle->fate);
    return jazz_node__node__ingest__NodeState__stage_known_transaction(self, batch, jazz_model__tx__Transaction__clone(bundle->tx), clone_versions(bundle),
               fate, bundle->has_global_time, bundle->global_time, bundle->durability, staged_global_times, staged_content_versions, staged_rejections);
}

static jz_err apply_view_update_inner(NS *self, jazz_node_ViewUpdateParts update, const jz_btree *preloaded_tx_ids /* NULL = None */,
                                      bool has_normalized_snapshot, const u8 normalized_snapshot[16]) {
    jazz_protocol_SubscriptionKey subscription = update.subscription;
    bool defer_settlement = update.defer_settlement, reset_input_set = update.reset_input_set, opening_pending = update.opening_pending;
    bool has_scope = update.wire_rows != NULL;
    jz_err e = JZ_OK;
    jazz_protocol_SupportingRowVec program_fact_adds = { 0 }, program_fact_removes = { 0 };
    jazz_protocol_VersionBundleRefVec version_bundle_refs = { 0 };
    ViewBundlePreflight preflight; bool has_preflight = false;
    jz_btree bulk_loaded_tx_ids; jz_btree_init(&bulk_loaded_tx_ids, &desc_txid_set);
    jazz_protocol_AuthorityResultKey authority_result_key; bool has_key = false;
    if (update.wire_rows) {
        jazz_protocol_SupportingRowsUpdate *wire = update.wire_rows;
        switch (wire->tag) {
        case JAZZ_SRU_SNAPSHOT: program_fact_adds = wire->added; wire->added = (jazz_protocol_SupportingRowVec){ 0 }; break;
        case JAZZ_SRU_DELTA:
            program_fact_adds = wire->added; program_fact_removes = wire->removed;
            wire->added = wire->removed = (jazz_protocol_SupportingRowVec){ 0 };
            break;
        /* Normalization turns a catch-up into an exact delta first. */
        case JAZZ_SRU_CATCH_UP:
            e = invalid_closure_c(subscription, "catch-up without an installed predecessor");
            goto out;
        }
    }
    e = version_bundle_refs_for_carriers(update.version_carriers.ptr, update.version_carriers.len, &version_bundle_refs);
    if (e.code) goto out;
    {
        jazz_protocol_BindingViewKey bvk;
        jz_err be = jazz_node__node__query_eval__subscriptions__NodeState__binding_view_key_for_subscription(self, subscription, &bvk);
        if (be.code) {
            if (err_is_unregistered_subscription(&be)) {
                /* Subscription teardown races in-flight traffic by design:
                 * unsubscribe is asynchronous, so per-subscription messages
                 * arriving after detach are normal protocol life, not
                 * corruption. The receiver cannot distinguish late-detached
                 * from never-registered keys, so both are benign drops. */
                jz_err_drop(&be);
                self->sync_metrics.dropped_detached_subscription_messages += 1;
                goto out;
            }
            e = be;
            goto out;
        }
    }
    /* Wire updates name a usage subscription only.  Resolve the complete
     * policy-scoped authority identity captured when that subscription was
     * admitted; never reconstruct or share it through BindingViewKey. */
    e = jazz_node__node__query_eval__subscriptions__NodeState__authority_result_key_for_subscription(self, subscription, &authority_result_key);
    if (e.code) goto out;
    has_key = true;
    /* Invalidate before any mutation or suspension. A failed frame can
     * alter provisional facts before advancing its generation, so a
     * generation comparison alone would not be a sufficient cache proof. */
    {
        jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, &authority_result_key);
        if (state) state->has_supporting_revision = false;
    }
    bool reset_cleared_shared_state = reset_replaces_authority_source_closure(reset_input_set, has_scope, opening_pending);
    bool has_reset_generation = false; u64 pending_authoritative_reset_generation = 0;
    if (reset_cleared_shared_state) {
        e = jazz_node__node__query_eval__subscriptions__NodeState__allocate_authoritative_reset_generation(self, &pending_authoritative_reset_generation);
        if (e.code) goto out;
        has_reset_generation = true;
    }
    if (!preloaded_tx_ids) {
        e = preflight_view_bundle_conflicts(self, version_bundle_refs.ptr, version_bundle_refs.len, &preflight);
        if (e.code) goto out;
        has_preflight = true;
    }
    if (preloaded_tx_ids) {
        jz_btree_drop(&bulk_loaded_tx_ids);
        jz_btree_clone(&bulk_loaded_tx_ids, preloaded_tx_ids, NULL, NULL);
    } else if (reset_input_set && update.peer_complete_tx_payload_refs.len == 0) {
        /* A reset with bundles is a snapshot for this subscription even
         * when other subscriptions already advanced the node watermark.
         * Empty reset stamps stay orthogonal below: with no bundles there
         * is no payload to bulk ingest and the stamp must not clear shared
         * state that is already more settled. */
        jazz_protocol_VersionBundleRefVec complete_refs = { 0 };
        const TxId *tx; const Bundle *bundle;
        JZ_BTREE_FOR(&preflight.bundles, tx, bundle)
            if (bundle->scope == JAZZ_BUNDLE_SCOPE_COMPLETE) JZ_VEC_PUSH(complete_refs, jazz_protocol__protocol__VersionBundle__as_ref(bundle));
        jz_btree_drop(&bulk_loaded_tx_ids);
        e = jazz_node__node__ingest__NodeState__ingest_reset_view_bundle_refs_in_bulk(self, complete_refs.ptr, complete_refs.len,
                                                                                     &preflight.persisted_tx_ids, &bulk_loaded_tx_ids);
        jz_free(complete_refs.ptr);
        if (e.code) { jz_btree_init(&bulk_loaded_tx_ids, &desc_txid_set); goto out; }
    }
    if (reset_input_set) authority_state_entry(self, &authority_result_key)->initial_hydration = true;
    authority_state_entry(self, &authority_result_key)->deferred_publication = defer_settlement;
    if (defer_settlement) {
        /* A newer deferred update supersedes any earlier complete
         * handoff for this *exact* authority receipt. Its membership may
         * be useful while the authority finishes the update, but neither
         * it nor a sibling policy scope may justify a known-state claim
         * until a non-deferred completion arrives. */
        authority_state_entry(self, &authority_result_key)->live_settled = false;
    }
    bool version_bundles_is_empty = version_bundle_refs.len == 0;
    if (has_preflight) {
        TxId tx; Bundle bundle;
        while (jz_btree_pop_first(&preflight.bundles, &tx, &bundle)) {
            if (jz_btree_contains(&bulk_loaded_tx_ids, &tx)) { jazz_protocol__protocol__VersionBundle__drop(&bundle); continue; }
            self->sync_metrics.receiver_per_bundle_ingests += 1;
            e = ingest_view_bundle(self, bundle);
            if (e.code) goto out;
        }
    }
    /* Retain the active peer payload inventory diagnostic independently
     * from the retired result-member witness validation. */
    for (u32 i = 0; i < update.peer_complete_tx_payload_refs.len; i++) {
        TxId tx_id = update.peer_complete_tx_payload_refs.ptr[i];
        if (jz_btree_contains(&bulk_loaded_tx_ids, &tx_id)) continue;
        bool found = false; jazz_node_StoredTransaction stored;
        e = jazz_node__node__currency__NodeState__query_transaction(self, tx_id, &found, &stored);
        if (e.code) goto out;
        if (found) jazz_node__node__codec__StoredTransaction__drop(&stored);
        else jazz_node__node__NodeState__record_peer_payload_inventory_missing_fallback(self);
    }
    if (reset_cleared_shared_state) {
        jazz_protocol_AuthorityResultKey k; jazz_protocol__protocol__AuthorityResultKey__clone(&k, &authority_result_key);
        jazz_node__node__NodeState__clear_settled_result_view(self, k);
    }
    if (reset_input_set) authority_state_entry(self, &authority_result_key);
    apply_covered_input_closure_admission_delta(authority_state_entry(self, &authority_result_key), reset_cleared_shared_state,
                                                program_fact_adds.ptr, program_fact_adds.len, program_fact_removes.ptr, program_fact_removes.len);
    if (!defer_settlement && !opening_pending) {
        jazz_node_AuthorityResultState *state = authority_state_entry(self, &authority_result_key);
        /* A complete inbound receipt makes this exact result live for
         * the current process even when it has no usable fast cursor.
         * `GlobalTime::default()` is the protocol's cursorless settlement
         * marker: it permits an exact known-state declaration but must
         * not claim a fast current-membership position. */
        state->live_settled = true;
        state->has_settled_through = update.settled_through != 0;
        state->settled_through = update.settled_through;
        /* A reset is an authoritative membership rebuild, including when
         * it carries retractions. The public subscription materializes its
         * replacement snapshot below, rather than attempting to apply a
         * removal after the reset has cleared the cached result set. */
        if (has_reset_generation) {
            state->has_pending_authoritative_reset = true;
            state->pending_authoritative_reset = pending_authoritative_reset_generation;
        }
    }
    /* An empty-bundle reset may finish initial hydration even when it names
     * peer payload refs: every covered input's body was already proven staged
     * or physically resident by `validate_covered_input_body_witnesses`, which
     * rejects the whole frame otherwise. This flag only ends the initial-sync
     * flush cadence; it does not gate visibility, known state or authorization.
     * Loosening witness validation would widen this shortcut. */
    {
        jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, &authority_result_key);
        if (state && state->initial_hydration && version_bundles_is_empty && !defer_settlement && !opening_pending)
            state->initial_hydration = false;
    }
    if (update.has_authorization_progress) {
        jazz_node_AuthorityResultState *state = authority_state_entry(self, &authority_result_key);
        state->has_authorization_progress = true;
        state->authorization_progress = update.authorization_progress;
    }
    authority_state_entry(self, &authority_result_key)->pending_opening = opening_pending;
    jazz_node_AuthorityResultState *state = authority_state_entry(self, &authority_result_key);
    state->applied_view_update_generation += 1; /* wrapping_add */
    /* In the current wire contract, a non-deferred reset is the single
     * frame that claims a complete replacement frontier.  Do not infer an
     * empty closure from an opened result container or an incremental
     * frame: receiver-local maintained graphs must wait for this marker.
     * The receiver validates the exact compiler-owned coverage set before
     * it installs the replacement, so this marker never makes a partial
     * closure publishable. */
    if (reset_input_set && !defer_settlement && !opening_pending) {
        state->source_closure_tag = JZN_SOURCE_CLOSURE_CLAIMED;
        state->source_closure_generation = state->applied_view_update_generation;
        for (u32 i = 0; i < state->source_incrementals.len; i++) {
            jz_btree_drop(&state->source_incrementals.ptr[i].adds);
            jz_btree_drop(&state->source_incrementals.ptr[i].removes);
        }
        state->source_incrementals.len = 0;
    } else if (!defer_settlement && !opening_pending) {
        /* Normal source updates retain their predecessor closure and are
         * applied as descriptor-bound runtime input deltas. They must not
         * be re-labelled as a complete reset merely because an authority
         * state map happens to contain the current closure. */
        static const jz_btree_desc desc_srow_set = { sizeof(SRow), 0, srow_cmp, jazz_protocol__protocol__SupportingRow__drop, NULL };
        jz_btree adds, removes;
        jz_btree_init(&adds, &desc_srow_set); jz_btree_init(&removes, &desc_srow_set);
        for (u32 i = 0; i < program_fact_adds.len; i++) {
            SRow c; jazz_protocol__protocol__SupportingRow__clone(&c, &program_fact_adds.ptr[i]);
            if (jz_btree_insert(&adds, &c, NULL, NULL)) jazz_protocol__protocol__SupportingRow__drop(&c);
        }
        for (u32 i = 0; i < program_fact_removes.len; i++) {
            SRow c; jazz_protocol__protocol__SupportingRow__clone(&c, &program_fact_removes.ptr[i]);
            if (jz_btree_insert(&removes, &c, NULL, NULL)) jazz_protocol__protocol__SupportingRow__drop(&c);
        }
        if (adds.len != 0 || removes.len != 0) {
            if (state->source_closure_tag != JZN_SOURCE_CLOSURE_CLAIMED) {
                jz_btree_drop(&adds); jz_btree_drop(&removes);
                e = JZN_INVALID_STORED("incremental source closure arrived before a reset closure");
                goto out;
            }
            u64 predecessor_generation = state->source_closure_generation;
            u64 generation = state->applied_view_update_generation;
            jazz_node_AuthoritySourceIncremental inc = { predecessor_generation, generation, adds, removes };
            JZ_VEC_PUSH(state->source_incrementals, inc);
            state->source_closure_tag = JZN_SOURCE_CLOSURE_CLAIMED;
            state->source_closure_generation = generation;
        } else {
            jz_btree_drop(&adds); jz_btree_drop(&removes);
        }
    }
    if (jazz_node__debug_env__covered_input_trace()) {
        char node[37];
        jz_uuid_to_string(&self->node_uuid, node);
        jz_str line = jz_format("JAZZ_COVERED_INPUT_TRACE stage=view_update_applied node=%s reset=%s deferred=%s generation=%llu facts=%u closure=%s",
                                node, reset_input_set ? "true" : "false", defer_settlement ? "true" : "false",
                                (unsigned long long)state->applied_view_update_generation, state->covered_input_versions.len,
                                state->source_closure_tag == JZN_SOURCE_CLOSURE_CLAIMED ? "Claimed" : "Pending");
        jazz_node__debug_env__eprintln(&line);
        jz_str_drop(&line);
    }
    /* Only this fully admitted in-process update establishes a predecessor.
     * Neither membership nor transport revisions survive process restart. */
    if (!defer_settlement && !opening_pending) {
        const jazz_node_AuthorityResultState *s = jz_btree_get(&self->query.authority_results, &authority_result_key);
        if (s && s->source_closure_tag == JZN_SOURCE_CLOSURE_CLAIMED) {
            bool has_settled = s->has_settled_through;
            jazz_types_GlobalTime settled_through = s->settled_through;
            /* Record the watermark before installing it in memory: a
             * cancelled write then leaves no in-process revision behind. */
            if (has_settled && has_normalized_snapshot) {
                const jazz_model_ValidatedQuery *shape = jazz_node__node__query_eval__NodeState__registered_shape(self, subscription.shape_id);
                if (shape && jazz_node__node__query_eval__subscription_watermarks__NodeState__watermark_catch_up_view(self, shape)) {
                    e = jazz_node__node__query_eval__subscription_watermarks__NodeState__persist_subscription_watermark(self, &authority_result_key,
                                                                                                                       settled_through, normalized_snapshot);
                    if (e.code) goto out;
                }
            }
            /* Only this exact successfully applied frame can install its
             * normalized predecessor. Legacy/deferred/opening frames do
             * not preserve an earlier frame's cache accidentally. */
            jazz_node_AuthorityResultState *m = jz_btree_get(&self->query.authority_results, &authority_result_key);
            if (m) {
                m->has_supporting_revision = has_normalized_snapshot;
                if (has_normalized_snapshot) memcpy(m->supporting_revision, normalized_snapshot, 16);
            }
        }
    }
out:
    jz_vec_drop(&program_fact_adds, sizeof(SRow), jazz_protocol__protocol__SupportingRow__drop);
    jz_vec_drop(&program_fact_removes, sizeof(SRow), jazz_protocol__protocol__SupportingRow__drop);
    jz_free(version_bundle_refs.ptr);
    if (has_preflight) preflight_drop(&preflight);
    jz_btree_drop(&bulk_loaded_tx_ids);
    if (has_key) jazz_protocol__protocol__AuthorityResultKey__drop(&authority_result_key);
    jazz_node__node__ViewUpdateParts__drop(&update);
    return e;
}

/* Apply a downstream current-row view update. */
jz_err jazz_node__node__views__NodeState__apply_view_update(NS *self, jazz_node_ViewUpdateParts update) {
    jz_btree revisions; jz_btree_init(&revisions, &desc_revisions);
    bool has_snapshot = false; u8 snapshot[16];
    jz_err e = normalize_supporting_update(self, &update, &revisions, &has_snapshot, snapshot);
    jz_btree_drop(&revisions);
    jazz_protocol_SubscriptionKey sub = update.subscription;
    jz_btree compiled_source_caches; jz_btree_init(&compiled_source_caches, &desc_compiled_caches);
    jazz_protocol_VersionBundleRefVec bundle_refs = { 0 };
    ViewBundlePreflight preflight; bool has_preflight = false;
    if (e.code) goto fail;
    e = validate_received_view_update_global_time_durability(self, &update);
    if (e.code) { e = invalid_authority_source_closure_error(sub, e); goto fail; }
    e = validate_view_update_payloads(self, &update, 1);
    if (e.code) { e = invalid_authority_source_closure_error(sub, e); goto fail; }
    jz_btree_drop(&compiled_source_caches);
    e = validate_covered_input_closure_admission(self, &update, 1, &compiled_source_caches);
    if (e.code) { jz_btree_init(&compiled_source_caches, &desc_compiled_caches); e = invalid_authority_source_closure_error(sub, e); goto fail; }
    e = version_bundle_refs_for_carriers(update.version_carriers.ptr, update.version_carriers.len, &bundle_refs);
    if (e.code) { e = invalid_authority_source_closure_error(sub, e); goto fail; }
    e = preflight_view_bundle_conflicts(self, bundle_refs.ptr, bundle_refs.len, &preflight);
    if (e.code) { e = invalid_authority_source_closure_error(sub, e); goto fail; }
    has_preflight = true;
    e = validate_covered_input_body_witnesses(self, &update, 1, &preflight.bundles);
    if (e.code) { e = invalid_authority_source_closure_error(sub, e); goto fail; }
    preflight_drop(&preflight);
    jz_free(bundle_refs.ptr);
    JZ_TRY(apply_view_update_inner(self, update, NULL, has_snapshot, snapshot));
    install_compiled_covered_input_source_caches(self, &compiled_source_caches);
    return JZ_OK;
fail:
    if (has_preflight) preflight_drop(&preflight);
    jz_free(bundle_refs.ptr);
    jz_btree_drop(&compiled_source_caches);
    jazz_node__node__ViewUpdateParts__drop(&update);
    return e;
}

typedef struct { bool some; u8 revision[16]; } OptRevision;

jz_err jazz_node__node__views__NodeState__apply_view_updates_in_batch(NS *self, jazz_node_ViewUpdatePartsVec updates) {
    jz_err e = JZ_OK;
    JZ_VEC(OptRevision) snapshots = { 0 };
    jz_btree compiled_source_caches; jz_btree_init(&compiled_source_caches, &desc_compiled_caches);
    jazz_protocol_VersionBundleRefVec all_bundle_refs = { 0 }, bulk_candidates = { 0 };
    jz_btree initial_hydration_authority_results; jz_btree_init(&initial_hydration_authority_results, &desc_revisions);
    ViewBundlePreflight preflight; bool has_preflight = false;
    jz_btree bulk_loaded_tx_ids; jz_btree_init(&bulk_loaded_tx_ids, &desc_txid_set);
    jz_btree receiver_batch_tx_ids; jz_btree_init(&receiver_batch_tx_ids, &desc_txid_set);
    jazz_node_GlobalTimeTxVec receiver_batch_global_times = { 0 };
    jazz_node_VersionRowVec receiver_batch_content_versions = { 0 };
    jazz_node_RejectedTransactionVec receiver_batch_rejections = { 0 };
    jazz_protocol_VersionBundleVec deferred_bundles = { 0 };
    groove_DatabaseBatch *receiver_batch = NULL;
    u32 consumed = 0; /* updates moved into apply_view_update_inner */
    {
        jz_btree prior_snapshots; jz_btree_init(&prior_snapshots, &desc_revisions);
        jz_vec_reserve(&snapshots, updates.len, sizeof(OptRevision));
        for (u32 i = 0; i < updates.len && !e.code; i++) {
            OptRevision r = { false };
            e = normalize_supporting_update(self, &updates.ptr[i], &prior_snapshots, &r.some, r.revision);
            JZ_VEC_PUSH(snapshots, r);
        }
        jz_btree_drop(&prior_snapshots);
        if (e.code) goto out;
    }
    if (updates.len == 0) goto out;
    for (u32 i = 0; i < updates.len; i++) {
        e = validate_received_view_update_global_time_durability(self, &updates.ptr[i]);
        if (e.code) { e = invalid_authority_source_closure_error(updates.ptr[i].subscription, e); goto out; }
    }
    /* A receiver tick is one atomic protocol frame. Validate every row
     * descriptor before the first reset can change flush cadence, or a
     * preceding valid bundle can advance clocks, allocate aliases, or
     * stage history before a later malformed bundle rejects the frame. */
    for (u32 i = 0; i < updates.len; i++) {
        e = validate_view_update_payloads(self, &updates.ptr[i], 1);
        if (e.code) { e = invalid_authority_source_closure_error(updates.ptr[i].subscription, e); goto out; }
    }
    jz_btree_drop(&compiled_source_caches);
    e = validate_covered_input_closure_admission(self, updates.ptr, updates.len, &compiled_source_caches);
    if (e.code) { jz_btree_init(&compiled_source_caches, &desc_compiled_caches); goto out; }
    {
        const jazz_protocol_AuthorityResultKey *key; const jazz_node_AuthorityResultState *state;
        JZ_BTREE_FOR(&self->query.authority_results, key, state) {
            if (!state->initial_hydration) continue;
            jazz_protocol_AuthorityResultKey k; jazz_protocol__protocol__AuthorityResultKey__clone(&k, key);
            u8 none[16] = { 0 };
            jz_btree_insert(&initial_hydration_authority_results, &k, none, NULL);
        }
    }
    for (u32 i = 0; i < updates.len; i++) {
        const jazz_node_ViewUpdateParts *update = &updates.ptr[i];
        jazz_protocol_VersionBundleRefVec version_bundle_refs;
        e = version_bundle_refs_for_carriers(update->version_carriers.ptr, update->version_carriers.len, &version_bundle_refs);
        if (e.code) goto out;
        jz_vec_extend(&all_bundle_refs, version_bundle_refs.ptr, version_bundle_refs.len, sizeof(BundleRef));
        jazz_protocol_AuthorityResultKey authority_result_key;
        jz_err ke = jazz_node__node__query_eval__subscriptions__NodeState__authority_result_key_for_subscription(self, update->subscription, &authority_result_key);
        if (ke.code) { jz_err_drop(&ke); jz_free(version_bundle_refs.ptr); continue; }
        if (update->reset_input_set && !jz_btree_contains(&initial_hydration_authority_results, &authority_result_key)) {
            jazz_protocol_AuthorityResultKey k; jazz_protocol__protocol__AuthorityResultKey__clone(&k, &authority_result_key);
            u8 none[16] = { 0 };
            jz_btree_insert(&initial_hydration_authority_results, &k, none, NULL);
        }
        bool in_initial_hydration = jz_btree_contains(&initial_hydration_authority_results, &authority_result_key);
        if (update->reset_input_set && update->peer_complete_tx_payload_refs.len == 0 && update->result_member_removes.len == 0)
            jz_vec_extend(&bulk_candidates, version_bundle_refs.ptr, version_bundle_refs.len, sizeof(BundleRef));
        if (in_initial_hydration && version_bundle_refs.len == 0 && (!update->reset_input_set || update->peer_complete_tx_payload_refs.len == 0)) {
            jazz_protocol_AuthorityResultKey ok;
            if (jz_btree_remove(&initial_hydration_authority_results, &authority_result_key, &ok, NULL)) jazz_protocol__protocol__AuthorityResultKey__drop(&ok);
        }
        jazz_protocol__protocol__AuthorityResultKey__drop(&authority_result_key);
        jz_free(version_bundle_refs.ptr);
    }
    e = preflight_view_bundle_conflicts(self, all_bundle_refs.ptr, all_bundle_refs.len, &preflight);
    if (e.code) goto out;
    has_preflight = true;
    /* A CoveredInput is not merely a selection hint: it names immutable
     * bytes the receiver will later install into a local runtime source.
     * Validate that each added coordinate is backed by either a body in
     * this atomic receiver frame or a previously admitted complete
     * payload explicitly referenced by its sender inventory. This closes
     * the old ResultMember witness boundary without accepting a new
     * source-input bypass. */
    e = validate_covered_input_body_witnesses(self, updates.ptr, updates.len, &preflight.bundles);
    if (e.code) goto out;
    {
        jz_btree bulk_candidate_tx_ids; jz_btree_init(&bulk_candidate_tx_ids, &desc_txid_set);
        for (u32 i = 0; i < bulk_candidates.len; i++) { TxId t = TXID(bulk_candidates.ptr[i].tx); jz_btree_insert(&bulk_candidate_tx_ids, &t, NULL, NULL); }
        jazz_protocol_VersionBundleRefVec bulk_candidate_refs = { 0 };
        const TxId *tx; const Bundle *bundle;
        JZ_BTREE_FOR(&preflight.bundles, tx, bundle)
            if (jz_btree_contains(&bulk_candidate_tx_ids, tx) && bundle->scope == JAZZ_BUNDLE_SCOPE_COMPLETE)
                JZ_VEC_PUSH(bulk_candidate_refs, jazz_protocol__protocol__VersionBundle__as_ref(bundle));
        jz_btree_drop(&bulk_loaded_tx_ids);
        e = jazz_node__node__ingest__NodeState__ingest_reset_view_bundle_refs_in_bulk(self, bulk_candidate_refs.ptr, bulk_candidate_refs.len,
                                                                                     &preflight.persisted_tx_ids, &bulk_loaded_tx_ids);
        jz_free(bulk_candidate_refs.ptr);
        jz_btree_drop(&bulk_candidate_tx_ids);
        if (e.code) { jz_btree_init(&bulk_loaded_tx_ids, &desc_txid_set); goto out; }
    }
    jz_btree *receiver_candidates = &preflight.bundles;
    {
        bool any_reset = false;
        for (u32 i = 0; i < updates.len; i++) any_reset |= updates.ptr[i].reset_input_set;
        if (any_reset) { e = jazz_node__node__NodeState__begin_initial_sync_flush_cadence(self); if (e.code) goto out; }
    }
    {
        const TxId *tx; void *unit;
        JZ_BTREE_FOR(&bulk_loaded_tx_ids, tx, unit) {
            (void)unit;
            Bundle removed;
            if (jz_btree_remove(receiver_candidates, tx, NULL, &removed)) jazz_protocol__protocol__VersionBundle__drop(&removed);
        }
    }
    /* Alias registration publishes metadata. Resolve every author, parent
     * and schema before preparing any immutable row in this shared batch. */
    {
        const TxId *tx; const Bundle *bundle;
        JZ_BTREE_FOR(receiver_candidates, tx, bundle) {
            jazz_types_NodeAlias alias;
            e = jazz_node__node__state__catalogue__NodeState__ensure_node_alias(self, tx->node, &alias);
            if (e.code) goto out;
            for (u32 i = 0; i < bundle->versions.len; i++) {
                jazz_types_SchemaVersionAlias sva;
                e = jazz_node__node__state__catalogue__NodeState__ensure_schema_version_alias(self, jazz_protocol__protocol__VersionRecord__schema_version(bundle->versions.ptr[i]), &sva);
                if (e.code) goto out;
            }
        }
    }
    receiver_batch = groove__db__facade__Database__open_batch(self->database.database);
    u64 receiver_batch_bundle_count = 0;
    {
        TxId tx; Bundle bundle;
        while (jz_btree_pop_first(receiver_candidates, &tx, &bundle)) {
            bool staged = false;
            e = stage_view_bundle(self, receiver_batch, &bundle, &receiver_batch_tx_ids, &receiver_batch_global_times,
                                  &receiver_batch_content_versions, &receiver_batch_rejections, &staged);
            if (e.code) { jazz_protocol__protocol__VersionBundle__drop(&bundle); goto out; }
            if (staged) { receiver_batch_bundle_count += 1; jazz_protocol__protocol__VersionBundle__drop(&bundle); }
            else JZ_VEC_PUSH(deferred_bundles, bundle);
        }
    }
    if (!groove__db__facade__DatabaseBatch__is_empty(receiver_batch)) {
        self->sync_metrics.receiver_bulk_ingest_commits += 1;
        self->sync_metrics.receiver_bulk_bundle_ingests += receiver_batch_bundle_count;
        groove_AppliedBatch *applied = NULL;
        groove_DatabaseBatch *batch = receiver_batch; receiver_batch = NULL;
        e = jazz_node__node__state__commit__NodeState__apply_node_batch(self, batch, &applied);
        if (e.code) goto out;
        groove_PersistOutcome *persisted = groove__db__commit__Database__persist_with_progress(self->database.database, applied);
        e = groove__db__commit__Database__finish_persistence(self->database.database, persisted);
        if (e.code) goto out;
        for (u32 i = 0; i < receiver_batch_rejections.len; i++) {
            TxId rtx = jazz_model__tx__RejectedTransaction__tx_id(receiver_batch_rejections.ptr[i]);
            jz_btree_insert(&self->rejections.rejected_transactions, &rtx, &receiver_batch_rejections.ptr[i], NULL);
        }
        receiver_batch_rejections.len = 0;
        const TxId *tx; void *unit;
        JZ_BTREE_FOR(&receiver_batch_tx_ids, tx, unit) { (void)unit; jazz_node__node__NodeState__invalidate_tx_version_tables_cache(self, *tx); }
        for (u32 i = 0; i < receiver_batch_global_times.len; i++) {
            jazz_types_GlobalTime newly; bool has_newly;
            e = jazz_node__node__open_tx__NodeState__record_applied_global_time(self, receiver_batch_global_times.ptr[i].gt, receiver_batch_global_times.ptr[i].tx,
                                                                              &newly, &has_newly);
            if (e.code) goto out;
        }
    }
    jz_btree preloaded_tx_ids;
    jz_btree_clone(&preloaded_tx_ids, &bulk_loaded_tx_ids, NULL, NULL);
    {
        const TxId *tx; void *unit;
        JZ_BTREE_FOR(&receiver_batch_tx_ids, tx, unit) { (void)unit; jz_btree_insert(&preloaded_tx_ids, tx, NULL, NULL); }
    }
    /* Supplying preloaded IDs bypasses per-update bundle ingestion below,
     * so deferred known transactions must explicitly apply their new fate. */
    for (u32 i = 0; i < deferred_bundles.len; i++) {
        TxId tx_id = TXID(deferred_bundles.ptr[i].tx);
        self->sync_metrics.receiver_per_bundle_ingests += 1;
        Bundle b = deferred_bundles.ptr[i];
        deferred_bundles.ptr[i] = (Bundle){ 0 };
        e = ingest_view_bundle(self, b);
        if (e.code) { jz_btree_drop(&preloaded_tx_ids); goto out; }
        jz_btree_insert(&preloaded_tx_ids, &tx_id, NULL, NULL);
    }
    /* Cross-subscription ordering within one receiver tick carries no
     * protocol semantics beyond per-link FIFO. Table writes are coalesced
     * above; per-subscription settled-state mutations still apply in
     * arrival order below. */
    for (u32 i = 0; i < updates.len; i++) {
        consumed = i + 1;
        e = apply_view_update_inner(self, updates.ptr[i], &preloaded_tx_ids, snapshots.ptr[i].some, snapshots.ptr[i].revision);
        if (e.code) break;
    }
    jz_btree_drop(&preloaded_tx_ids);
    if (e.code) goto out;
    install_compiled_covered_input_source_caches(self, &compiled_source_caches);
    jz_btree_init(&compiled_source_caches, &desc_compiled_caches);
    if (self->initial_sync_flush_active) {
        bool all_hydrated = true;
        const jazz_protocol_AuthorityResultKey *key; const jazz_node_AuthorityResultState *state;
        JZ_BTREE_FOR(&self->query.authority_results, key, state) if (state->initial_hydration) { all_hydrated = false; break; }
        if (all_hydrated) e = jazz_node__node__NodeState__finish_initial_sync_flush_cadence(self);
    }
out:
    for (u32 i = consumed; i < updates.len; i++) jazz_node__node__ViewUpdateParts__drop(&updates.ptr[i]);
    jz_free(updates.ptr);
    jz_free(snapshots.ptr);
    jz_btree_drop(&compiled_source_caches);
    jz_free(all_bundle_refs.ptr); jz_free(bulk_candidates.ptr);
    jz_btree_drop(&initial_hydration_authority_results);
    if (has_preflight) preflight_drop(&preflight);
    jz_btree_drop(&bulk_loaded_tx_ids);
    jz_btree_drop(&receiver_batch_tx_ids);
    jz_free(receiver_batch_global_times.ptr);
    vrow_vec_drop(&receiver_batch_content_versions);
    for (u32 i = 0; i < receiver_batch_rejections.len; i++) jz_free(receiver_batch_rejections.ptr[i]);
    jz_free(receiver_batch_rejections.ptr);
    for (u32 i = 0; i < deferred_bundles.len; i++) if (deferred_bundles.ptr[i].tx) jazz_protocol__protocol__VersionBundle__drop(&deferred_bundles.ptr[i]);
    jz_free(deferred_bundles.ptr);
    (void)receiver_batch;
    return e;
}

static const jz_btree_desc desc_tx_headers = { sizeof(TxId), sizeof(jazz_model_Transaction *), jzn_cmp_txid, NULL, NULL };

/* Preserve a discarded Pending carrier's transaction identity for its
 * already-registered fate observer, without publishing any row version or
 * accepting the discarded view's supporting set. A later ordinary carrier
 * extends this existing zero-body, view-scoped fragment. */
jz_err jazz_node__node__views__NodeState__remember_discarded_pending_view_transactions(NS *self, jazz_protocol_VersionCarrier *const *carriers, u32 n) {
    jz_btree headers; jz_btree_init(&headers, &desc_tx_headers);
    jz_err e = JZ_OK;
    for (u32 c = 0; c < n && !e.code; c++) {
        jazz_protocol_VersionBundleRefVec refs = { 0 };
        jz_err be = jazz_protocol__protocol__VersionCarrier__bundle_refs(carriers[c], &refs);
        if (be.code) { jz_err_drop(&be); e = JZN_MALFORMED_VIEW("malformed version-bundle run"); break; }
        for (u32 i = 0; i < refs.len; i++) {
            if (refs.ptr[i].fate->tag != JAZZ_FATE_PENDING) continue;
            jazz_model_Transaction *tx = jazz_node__node__codec__transaction_without_permission_subject(refs.ptr[i].tx);
            jazz_model__tx__Transaction__set_n_total_writes(tx, 0);
            groove_Value merge;
            e = jazz_node__node__NodeState__admit_contribution_merge_for_storage(self, tx, &merge);
            if (e.code) { jazz_model__tx__Transaction__drop(tx); break; }
            groove__records__values__Value__drop(&merge);
            TxId tx_id = TXID(tx);
            jazz_model_Transaction **previous = jz_btree_get(&headers, &tx_id);
            if (previous && !jazz_node__node__codec__known_transaction_payload_matches(*previous, tx)) {
                jazz_model__tx__Transaction__drop(tx);
                e = jazz_node__node__Error__conflicting_commit_unit(&tx_id);
                break;
            }
            jazz_model_Transaction *old;
            if (jz_btree_insert(&headers, &tx_id, &tx, &old)) jazz_model__tx__Transaction__drop(old);
        }
        jz_free(refs.ptr);
    }
    JZ_VEC(jazz_model_Transaction *) missing = { 0 };
    TxId tx_id; jazz_model_Transaction *tx;
    while (jz_btree_pop_first(&headers, &tx_id, &tx)) {
        if (e.code) { jazz_model__tx__Transaction__drop(tx); continue; }
        bool found = false; jazz_node_StoredTransaction stored;
        e = jazz_node__node__currency__NodeState__query_transaction(self, tx_id, &found, &stored);
        if (e.code) { jazz_model__tx__Transaction__drop(tx); continue; }
        if (found) {
            jazz_model_Transaction *identity = jazz_node__node__codec__transaction_without_permission_subject(stored.tx);
            jazz_model__tx__Transaction__set_n_total_writes(identity, 0);
            bool matches = jazz_node__node__codec__known_transaction_payload_matches(identity, tx);
            jazz_model__tx__Transaction__drop(identity);
            jazz_node__node__codec__StoredTransaction__drop(&stored);
            jazz_model__tx__Transaction__drop(tx);
            if (!matches) e = jazz_node__node__Error__conflicting_commit_unit(&tx_id);
        } else {
            JZ_VEC_PUSH(missing, tx);
        }
    }
    jz_btree_drop(&headers);
    for (u32 i = 0; i < missing.len; i++) {
        if (e.code) { jazz_model__tx__Transaction__drop(missing.ptr[i]); continue; }
        jazz_model_Fate pending = { JAZZ_FATE_PENDING, NULL };
        e = jazz_node__node__ingest__NodeState__ingest_transaction_fragment_without_current_indexes(self, missing.ptr[i], (jazz_protocol_VersionRecordVec){ 0 },
                                                                                                   pending, false, 0, JAZZ_DUR_LOCAL);
    }
    jz_free(missing.ptr);
    return e;
}

/* Persist immutable bodies carried by a stale authority link without
 * accepting that link's source closure as an authority receipt.
 *
 * A replacement upstream can have a later receipt selected while an old
 * transport still has a frame in flight. Its bodies are ordinary
 * immutable replication data and may be needed for the selected link's
 * later exact closure, but its covered-input facts must not mutate the
 * selected receiver frontier or derived terminal. */
jz_err jazz_node__node__views__NodeState__ingest_unselected_authority_view_bundles(NS *self, jazz_protocol_VersionCarrier *const *carriers, u32 n) {
    jazz_protocol_VersionBundleRefVec bundle_refs;
    JZ_TRY(version_bundle_refs_for_carriers(carriers, n, &bundle_refs));
    ViewBundlePreflight preflight;
    jz_err e = preflight_view_bundle_conflicts(self, bundle_refs.ptr, bundle_refs.len, &preflight);
    jz_free(bundle_refs.ptr);
    if (e.code) return e;
    TxId tx; Bundle bundle;
    while (jz_btree_pop_first(&preflight.bundles, &tx, &bundle)) {
        if (e.code) { jazz_protocol__protocol__VersionBundle__drop(&bundle); continue; }
        e = ingest_view_bundle(self, bundle);
    }
    preflight_drop(&preflight);
    return e;
}

/* ---------------- whole-table shape, bundle assembly ---------------- */
jz_err jazz_node__node__views__NodeState__whole_table_subscription_key(NS *self, const jz_str *table, jazz_protocol_SubscriptionKey *out) {
    jazz_model_ValidatedQuery *shape = NULL; jazz_model_Binding *binding = NULL;
    JZ_TRY(jazz_node__node__views__NodeState__whole_table_shape_binding(self, table, &shape, &binding));
    out->shape_id = jazz_model__query__ValidatedQuery__shape_id(shape);
    out->binding_id = jazz_model__query__Binding__binding_id(binding);
    out->read_view = (jazz_protocol_ReadViewKey){ 0 }; /* Default::default() */
    jazz_model__query__ValidatedQuery__drop(shape);
    jazz_model__query__Binding__drop(binding);
    return JZ_OK;
}

jz_err jazz_node__node__views__NodeState__whole_table_shape_binding(NS *self, const jz_str *table, jazz_model_ValidatedQuery **shape_out,
                                                                   jazz_model_Binding **binding_out) {
    const jazz_model_JazzSchema *schema;
    jazz_types_SchemaVersionId schema_version;
    const jazz_model_TableSchema *t;
    jz_err te = jazz_node__node__NodeState__table(self, table, &t);
    if (!te.code) {
        schema = self->catalogue.schema;
        schema_version = self->catalogue.local_schema_version_id;
    } else {
        jz_err_drop(&te);
        schema_version = self->catalogue.active_schema.schema;
        jazz_protocol_SchemaVersion *const *payload = jz_btree_get(&self->catalogue.catalogue_schemas, &schema_version);
        if (!payload) return JZN_INVALID_STORED("current write schema payload missing");
        schema = (*payload)->schema;
    }
    jazz_model_Query query = jazz_model__query__Query__from(jz_str_clone(table), (jazz_model_OptStr){ false });
    jazz_model_ValidatedQuery *shape = NULL;
    jz_err e = jazz_model__query__Query__validate_with_schema_version(&query, schema, schema_version, &shape);
    jazz_model__query__Query__drop_v(&query);
    if (e.code) return e;
    jz_btree params; jz_btree_init(&params, &jzn_desc_str_value);
    jazz_model_Binding *binding = NULL;
    e = jazz_model__query__ValidatedQuery__bind(shape, params, &binding);
    if (e.code) { jazz_model__query__ValidatedQuery__drop(shape); return e; }
    *shape_out = shape;
    *binding_out = binding;
    return JZ_OK;
}

jz_err jazz_node__node__views__NodeState__version_bundle_for_maintained_view_versions_with_tx(NS *self, const jazz_node_StoredTransaction *stored_tx,
                                                                                             jazz_node_VersionRow *const *tx_versions, u32 n, Bundle *out) {
    return version_bundle_for_maintained_view_versions_with_tx_and_source(self, stored_tx, tx_versions, n, MBVS_IVM_WITNESS, out);
}

/* (version_bundle_for_exact_storage_maintained_view_versions_with_tx is the
 * MBVS_EXACT_STORAGE call above, inlined at its single call site.)
 *
 * Whether these direct immutable-store reads can be serialized without
 * the normal maintained-witness canonicalization. Logical names can be
 * reused by different physical tables across schema history, so an exact
 * `(table, row, tx)` lookup alone is not enough: every row must resolve
 * to the sole physical table ever named by that logical label. Otherwise
 * use the ordinary fail-closed canonicalization path. */
static jz_err exact_storage_maintained_versions_are_unambiguous(NS *self, VRow *const *versions, u32 n, bool *out) {
    bool all_unambiguous = true;
    for (u32 i = 0; i < n; i++) {
        jazz_types_PhysicalTableId version_table_id;
        JZ_TRY(jazz_node__node__physical__NodeState__physical_table_id_for_version(self, versions[i], &version_table_id));
        u32 distinct = 0; bool contains = false;
        jazz_types_PhysicalTableId seen[64]; /* BTreeSet<PhysicalTableId>; schema history is small */
        const jazz_types_SchemaVersionId *sv; const jazz_node_SchemaPhysicalMapping *mapping;
        JZ_BTREE_FOR(&self->catalogue.physical_mappings, sv, mapping) {
            const jazz_node_TablePhysicalMapping *tm = jz_btree_get(&mapping->tables, VROW_TABLE(versions[i]));
            if (!tm) continue;
            bool dup = false;
            for (u32 j = 0; j < distinct && !dup; j++) dup = seen[j] == tm->table_id;
            if (!dup && distinct < 64) seen[distinct++] = tm->table_id;
            if (tm->table_id == version_table_id) contains = true;
        }
        all_unambiguous = all_unambiguous && distinct == 1 && contains;
    }
    *out = all_unambiguous;
    return JZ_OK;
}

static jz_err version_bundle_for_maintained_view_versions_with_tx_and_source(NS *self, const jazz_node_StoredTransaction *stored_tx,
    VRow *const *tx_versions, u32 n, MaintainedBundleVersionSource source, Bundle *out) {
    const jazz_model_Transaction *tx = stored_tx->tx;
    TxId tx_id = TXID(tx);
    u32 n_total_writes = jazz_model__tx__Transaction__n_total_writes(tx);
    /* A structured result can reach the same immutable version through
     * more than one retained fact (for example, a root's nested relation
     * and the relation's sender witness). A wire bundle describes a set of
     * row versions, not those traversal paths: canonicalize it before
     * deriving its declared view-scoped cardinality. */
    jz_btree versions; jz_btree_init(&versions, &desc_record_map);
    jz_err e = JZ_OK;
    for (u32 i = 0; i < n && !e.code; i++) {
        /* A maintained terminal may use a current-row source projected for
         * query evaluation, but a VersionRecord is replicated history, not
         * query output. Resolve its identity back to the stored authored
         * row before crossing the wire boundary (INV-DATA-16/18,
         * INV-SYNC-16, and C.3's byte-fidelity rule). */
        VRow *canonical = NULL;
        if (source == MBVS_EXACT_STORAGE) canonical = jazz_node__node__codec__VersionRow__clone(tx_versions[i]);
        else { e = jazz_node__node__views__NodeState__canonical_history_version_for_maintained_witness(self, tx_versions[i], &canonical); if (e.code) break; }
        VR *record = NULL;
        e = jazz_node__node__NodeState__version_record_from_row(self, canonical, &record);
        jazz_node__node__codec__VersionRow__drop(canonical);
        if (e.code) break;
        RecordKey key = version_bundle_record_key(record);
        VR **existing = jz_btree_get(&versions, &key);
        if (existing) {
            bool same = jazz_protocol__protocol__VersionRecord__eq(*existing, record);
            record_key_drop(&key);
            jazz_protocol__protocol__VersionRecord__drop(record);
            if (!same) e = jazz_node__node__Error__conflicting_commit_unit(&tx_id);
        } else {
            jz_btree_insert(&versions, &key, &record, NULL);
        }
    }
    if (e.code) { jz_btree_drop(&versions); return e; }
    memset(out, 0, sizeof *out);
    RecordKey k; VR *v;
    while (jz_btree_pop_first(&versions, &k, &v)) { record_key_drop(&k); JZ_VEC_PUSH(out->versions, v); }
    jz_btree_drop(&versions);
    /* A relay may only retain the selected fragment of an upstream
     * view-scoped bundle. Its stored cardinality then describes that
     * fragment, so matching it to the selected versions does not prove
     * that the complete transaction is known. */
    u32 scope = (!stored_tx->is_partial && n_total_writes == out->versions.len) ? JAZZ_BUNDLE_SCOPE_COMPLETE : JAZZ_BUNDLE_SCOPE_VIEW_SCOPED;
    const jz_str *meta = jazz_model__tx__Transaction__user_metadata_json(tx);
    jz_slice meta_slice = meta ? jz_str_slice(meta) : (jz_slice){ 0 };
    out->tx = jazz_model__tx__Transaction__new(
        tx_id, jazz_model__tx__Transaction__kind(tx),
        scope == JAZZ_BUNDLE_SCOPE_COMPLETE ? n_total_writes : out->versions.len,
        *jazz_model__tx__Transaction__made_by(tx),
        /* Policy capabilities are local authority state and never part of
         * a view or repair carrier. Durable made_by remains explicit. */
        false, jazz_types__ids__AuthorSubject__SYSTEM,
        /* Exclusive read evidence is kept only for retransmitting the
         * author's own pending unit; view carriers never expose it. */
        false, (jazz_model_Snapshot){ 0 }, NULL, NULL, NULL,
        meta ? &meta_slice : NULL, jazz_model__tx__Transaction__contribution_merge_clone(tx));
    out->scope = scope;
    jazz_model__tx__Fate__clone(&out->fate, &stored_tx->fate);
    out->has_global_time = stored_tx->has_global_time;
    out->global_time = stored_tx->global_time;
    out->durability = stored_tx->durability;
    return JZ_OK;
}

/* Return the stored, authored history row for a maintained witness.
 *
 * The maintained graph may evaluate a schema-compatible current source,
 * whereas a wire `VersionRecord` is always the complete immutable row
 * version under its authored schema. This producer-side normalization is
 * deliberately before serialization; receivers reject non-identical
 * duplicate row versions rather than repairing them. */
jz_err jazz_node__node__views__NodeState__canonical_history_version_for_maintained_witness(NS *self, const VRow *version, VRow **out) {
    *out = NULL;
    const jz_str *vtable = VROW_TABLE(version);
    jazz_types_RowUuid vrow = VROW_ROW(version);
    /* The maintained graph can call its projected result table by a name
     * that also existed in the authored schema.  Resolve that name once
     * through the active catalogue and require the same physical table
     * for every candidate below; a reused name is ambiguous and must fail
     * closed rather than selecting the first matching row key. */
    jz_btree logical_candidates; jz_btree_init(&logical_candidates, &jzn_desc_u64_set);
    {
        const jazz_types_SchemaVersionId *sv; const jazz_node_SchemaPhysicalMapping *mapping;
        JZ_BTREE_FOR(&self->catalogue.physical_mappings, sv, mapping) {
            const jazz_node_TablePhysicalMapping *tm = jz_btree_get(&mapping->tables, vtable);
            if (tm) jz_btree_insert(&logical_candidates, &tm->table_id, NULL, NULL);
        }
    }
    TxId witness_tx_id;
    jz_err e = jazz_node__node__NodeState__version_tx_id(self, version, &witness_tx_id);
    if (e.code) { jz_btree_drop(&logical_candidates); return e; }
    jz_btree physical_candidates;
    if (logical_candidates.len == 1) physical_candidates = logical_candidates;
    else {
        jz_btree_init(&physical_candidates, &jzn_desc_u64_set);
        jazz_node_VersionRowVec stored = { 0 };
        e = jazz_node__node__currency__NodeState__query_versions_for_tx(self, witness_tx_id, &stored);
        for (u32 i = 0; i < stored.len && !e.code; i++) {
            jazz_types_RowUuid r = VROW_ROW(stored.ptr[i]);
            if (!jz_uuid_eq(&r, &vrow) || !jazz_model__model__branch__BranchKey__eq(jazz_node__node__codec__VersionRow__branch_key(stored.ptr[i]),
                                                                                    jazz_node__node__codec__VersionRow__branch_key(version))) continue;
            jazz_types_PhysicalTableId table_id;
            jz_err pe = jazz_node__node__physical__NodeState__physical_table_id_for_version(self, stored.ptr[i], &table_id);
            if (pe.code) { jz_err_drop(&pe); continue; }
            if (jz_btree_contains(&logical_candidates, &table_id)) jz_btree_insert(&physical_candidates, &table_id, NULL, NULL);
        }
        vrow_vec_drop(&stored);
        jz_btree_drop(&logical_candidates);
        if (e.code) { jz_btree_drop(&physical_candidates); return e; }
    }
    if (physical_candidates.len != 1) {
        jz_btree_drop(&physical_candidates);
        return JZN_INVALID_STORED("maintained witness maps to zero or multiple physical tables");
    }
    jazz_types_PhysicalTableId projected_table_id;
    {
        void *k, *v;
        jz_btree_first(&physical_candidates, &k, &v);
        projected_table_id = *(jazz_types_PhysicalTableId *)k;
    }
    jz_btree_drop(&physical_candidates);
    jazz_types_SchemaVersionId authored_schema;
    if (!jazz_node__node__NodeState__schema_version_for_alias(self, jazz_node__node__codec__VersionRow__schema_version_alias(version), &authored_schema))
        return JZN_INVALID_STORED("maintained witness schema version alias must exist");
    const jazz_model_TableSchema *authored_table = NULL;
    e = jazz_node__node__NodeState__table_in_schema_ref(self, vtable, authored_schema, &authored_table);
    if (e.code) {
        if (e.code != JZN_E_TABLE_NOT_FOUND) return e;
        jz_err_drop(&e);
        /* A current-query source may have been projected through a
         * later schema, so its logical name need not exist under the
         * authored alias carried by its immutable history identity.
         * Resolve it through the catalogue's unambiguous physical
         * table id, then reload the actual stored row for the same
         * physical table, branch, row, transaction, and layer. This is the
         * same identity boundary used for repair payloads; reused or
         * unknown logical names fail closed before history is read. */
        jazz_node_VersionRowVec stored = { 0 };
        JZ_TRY(jazz_node__node__currency__NodeState__query_versions_for_tx(self, witness_tx_id, &stored));
        for (u32 i = 0; i < stored.len; i++) {
            jazz_types_RowUuid r = VROW_ROW(stored.ptr[i]);
            if (!jz_uuid_eq(&r, &vrow) || !jazz_model__model__branch__BranchKey__eq(jazz_node__node__codec__VersionRow__branch_key(stored.ptr[i]),
                                                                                    jazz_node__node__codec__VersionRow__branch_key(version))) continue;
            jazz_types_PhysicalTableId table_id;
            jz_err pe = jazz_node__node__physical__NodeState__physical_table_id_for_version(self, stored.ptr[i], &table_id);
            if (pe.code) { jz_err_drop(&pe); continue; }
            if (table_id == projected_table_id) {
                *out = stored.ptr[i];
                for (u32 j = 0; j < stored.len; j++) if (j != i) jazz_node__node__codec__VersionRow__drop(stored.ptr[j]);
                jz_free(stored.ptr);
                return JZ_OK;
            }
        }
        vrow_vec_drop(&stored);
        return JZN_ERR(JZN_E_MAINTAINED_VIEW_MISSING_BUNDLE_WITNESS, "maintained witness projection is missing its canonical history row");
    }
    /* A maintained witness is decoded from the current-query graph. Its
     * descriptor can be identical to history storage while selected-out
     * cells are still typed nulls, so first prefer its immutable stored
     * history identity even when the descriptors match. */
    jzn_strvec storage_tables = { 0 };
    JZ_TRY(jazz_node__node__currency__NodeState__version_storage_sources(self, vtable, &storage_tables));
    for (u32 i = 0; i < storage_tables.len && !e.code; i++) {
        VRow *canonical = NULL;
        e = jazz_node__node__currency__NodeState__query_version_by_alias_with_storage_in_schema(self, authored_schema, vtable, &storage_tables.ptr[i],
                jazz_node__node__codec__VersionRow__branch_key(version), vrow, jazz_node__node__codec__VersionRow__tx_time(version),
                jazz_node__node__codec__VersionRow__tx_node_alias(version), &canonical);
        if (e.code || !canonical) continue;
        /* The read projection may change the schema alias even when the
         * table layout is unchanged. This exact history key identifies the
         * authored version; never replace its identity with the projection. */
        jazz_types_PhysicalTableId table_id;
        e = jazz_node__node__physical__NodeState__physical_table_id_for_version(self, canonical, &table_id);
        if (!e.code && table_id == projected_table_id) { *out = canonical; break; }
        jazz_node__node__codec__VersionRow__drop(canonical);
    }
    jz_vec_drop(&storage_tables, sizeof(jz_str), jzn_str_drop_v);
    if (e.code || *out) return e;
    /* Some maintained rows are legitimate materialized/synthetic versions
     * (for example, synthesized merge output) and therefore have no persisted
     * history identity to reload. They may cross the boundary only when
     * their authored descriptor is complete. `authored_columns` lets us
     * distinguish such a row from a query projection whose selected-out
     * authored cells were replaced by typed nulls.
     * Only synthetic rows need a reconstructed descriptor. Ordinary rows
     * returned above already carry the immutable store's authored layout. */
    JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, vtable, authored_schema, &authored_table));
    const groove_RecordDescriptor *authored_descriptor = jazz_model__schema__TableSchema__history_storage_record_schema(authored_table);
    bool has_authored_layout = groove__records__RecordDescriptor__eq(
        groove__records__OwnedRecord__descriptor(jazz_node__node__codec__VersionRow__record(version)), authored_descriptor);
    bool has_complete_authored_payload = false;
    if (has_authored_layout) {
        jazz_model_DeletionEvent ev;
        if (jazz_node__node__codec__VersionRow__deletion(version, &ev)) has_complete_authored_payload = true;
        else {
            bool some = false; jz_btree authored;
            JZ_TRY(jazz_node__node__physical__NodeState__authored_columns_for_version(self, version, &some, &authored));
            if (!some) has_complete_authored_payload = true; /* Legacy complete rows predate authored-column metadata. */
            else {
                has_complete_authored_payload = true;
                const jz_str *column; void *unit;
                JZ_BTREE_FOR(&authored, column, unit) {
                    (void)unit;
                    if (jz_str_eq_cstr(column, JAZZ_DELETION_COLUMN_NAME)) continue;
                    bool cell_some = false; groove_Value value;
                    jz_err ce = jazz_node__node__codec__VersionRow__cell(version, authored_table, column, &cell_some, &value);
                    if (ce.code) { jz_err_drop(&ce); has_complete_authored_payload = false; break; }
                    if (cell_some) groove__records__values__Value__drop(&value);
                    else { has_complete_authored_payload = false; break; }
                }
                jz_btree_drop(&authored);
            }
        }
    }
    if (has_complete_authored_payload) { *out = jazz_node__node__codec__VersionRow__clone(version); return JZ_OK; }
    return JZN_ERR(JZN_E_MAINTAINED_VIEW_MISSING_BUNDLE_WITNESS, "maintained witness is missing its canonical history row");
}
