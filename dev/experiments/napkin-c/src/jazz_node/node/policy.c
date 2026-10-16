/* Port of crates/jazz/layers/node/src/node/policy.rs
 *
 * Write-policy admission and policy-pinned row projection. Policy predicates,
 * joins, inheritance, reachability, and alternatives execute through the query
 * program in `query_eval`; this module selects the operation clause, projects
 * old/candidate data into the pinned policy schema, and fail-closes write
 * ingest. It also retains the transaction memo used by view emission.
 *
 * `impl<S: OrderedKvStorage> NodeState<S>` becomes one C copy; async methods
 * are synchronous. Option<CurrentRow> is a (bool has, CurrentRow) pair. */
#include <stdio.h>
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

typedef jazz_protocol_VersionRecord VR;
typedef jazz_node_RowProvenance RowProvenance;
typedef jazz_node_CurrentRow CurrentRow;
#define QE_AUTHZ(name) jazz_node__node__query_eval__authorization__NodeState__##name
#define RS(name) jazz_node__node__query_eval__read_sources__##name

/* ---------------- ViewEvaluationContext ---------------- */

static void opt_stored_tx_drop(void *p) {
    jazz_node_OptStoredTransaction *o = p;
    if (o->some) jazz_node__node__codec__StoredTransaction__drop(&o->tx);
}
static const jz_btree_desc desc_tx_rows = { sizeof(jazz_model_TxId), sizeof(jazz_node_OptStoredTransaction), jzn_cmp_txid, NULL, opt_stored_tx_drop };

void jazz_node__node__policy__ViewEvaluationContext__default(jazz_node_ViewEvaluationContext *out) {
    jz_btree_init(&out->tx_rows, &desc_tx_rows);
}
void jazz_node__node__policy__ViewEvaluationContext__drop(jazz_node_ViewEvaluationContext *c) { jz_btree_drop(&c->tx_rows); }

/* ---------------- (SchemaVersionId, String) keys ---------------- */

static int cmp_table_key(const void *a, const void *b, void *c) {
    (void)c;
    const jazz_node_OverlayTableKey *x = a, *y = b;
    int r = jz_uuid_cmp(&x->schema, &y->schema);
    return r ? r : jz_slice_cmp(jz_str_slice(&x->table), jz_str_slice(&y->table));
}
static void table_key_drop(void *p) { jz_str_drop(&((jazz_node_OverlayTableKey *)p)->table); }
static void table_key_clone(void *d, const void *s) {
    const jazz_node_OverlayTableKey *src = s;
    jazz_node_OverlayTableKey *dst = d;
    dst->schema = src->schema;
    dst->table = jz_str_clone(&src->table);
}
/* BTreeSet<(SchemaVersionId, String)> */
static const jz_btree_desc desc_table_key_set = { sizeof(jazz_node_OverlayTableKey), 0, cmp_table_key, table_key_drop, NULL };
/* BTreeMap<(SchemaVersionId, String), usize> */
static const jz_btree_desc desc_table_key_count = { sizeof(jazz_node_OverlayTableKey), sizeof(usize), cmp_table_key, table_key_drop, NULL };
/* BTreeMap<(SchemaVersionId, String), bool> */
static const jz_btree_desc desc_table_key_bool = { sizeof(jazz_node_OverlayTableKey), sizeof(bool), cmp_table_key, table_key_drop, NULL };
/* OverlayTables = BTreeMap<(SchemaVersionId, String), Arc<TransactionOverlayTable>> */
static void overlay_table_release(void *p) { jz_rc_release(*(jazz_node_TransactionOverlayTable **)p); }
static void overlay_table_clone(void *d, const void *s) { *(jazz_node_TransactionOverlayTable **)d = jz_rc_clone(*(jazz_node_TransactionOverlayTable *const *)s); }
static const jz_btree_desc desc_overlay_tables = { sizeof(jazz_node_OverlayTableKey), sizeof(jazz_node_TransactionOverlayTable *), cmp_table_key, table_key_drop, overlay_table_release };
/* RowUuid -> Option<CurrentRow> (CurrentRow*, NULL = None) */
static void opt_row_drop(void *p) {
    CurrentRow *r = *(CurrentRow **)p;
    if (r) { jazz_node__node__CurrentRow__drop(r); jz_free(r); }
}
static const jz_btree_desc desc_overlay_rows = { sizeof(jz_uuid), sizeof(CurrentRow *), jz_cmp_uuid, NULL, opt_row_drop };
/* BTreeMap<(SchemaVersionId, String), BTreeMap<RowUuid, Option<CurrentRow>>> */
static void btree_drop_v(void *p) { jz_btree_drop(p); }
static const jz_btree_desc desc_rebuilt = { sizeof(jazz_node_OverlayTableKey), sizeof(jz_btree), cmp_table_key, table_key_drop, btree_drop_v };
/* BTreeSet<usize> */
static int cmp_usize(const void *a, const void *b, void *c) {
    (void)c;
    usize x = *(const usize *)a, y = *(const usize *)b;
    return x < y ? -1 : x > y;
}
static const jz_btree_desc desc_usize_set = { sizeof(usize), 0, cmp_usize, NULL, NULL };

static jazz_node_OverlayTableKey table_key(jazz_types_SchemaVersionId schema, const jz_str *table) {
    jazz_node_OverlayTableKey k = { schema, jz_str_clone(table) };
    return k;
}

/* ---------------- provenance helpers ---------------- */

static RowProvenance version_provenance(const VR *version) {
    RowProvenance p = {
        jazz_protocol__protocol__VersionRecord__created_by(version),
        jazz_protocol__protocol__VersionRecord__created_at_ms(version),
        jazz_protocol__protocol__VersionRecord__updated_by(version),
        jazz_protocol__protocol__VersionRecord__updated_at_ms(version),
    };
    return p;
}

/* `version`'s update stamped over a row whose creation is `previous`. */
static RowProvenance updated_provenance(RowProvenance previous, const VR *version) {
    RowProvenance p = {
        previous.created_by,
        previous.created_at,
        jazz_protocol__protocol__VersionRecord__updated_by(version),
        jazz_protocol__protocol__VersionRecord__updated_at_ms(version),
    };
    return p;
}

static RowProvenance stored_version_provenance(const jazz_node_VersionRow *version) {
    RowProvenance p = {
        jazz_node__node__codec__VersionRow__created_by(version),
        jazz_types__time__TxTime__physical_ms(jazz_node__node__codec__VersionRow__created_at(version)),
        jazz_node__node__codec__VersionRow__updated_by(version),
        jazz_types__time__TxTime__physical_ms(jazz_node__node__codec__VersionRow__updated_at(version)),
    };
    return p;
}

static jz_err reconstructed_policy_subject_row(const jazz_model_TableSchema *table, jazz_types_RowUuid row_uuid, const jz_btree *cells,
                                               const jazz_node_VersionRow *version, CurrentRow *out) {
    return jazz_node__node__codec__current_row_from_cells_with_explicit_provenance(
        table, row_uuid, cells, stored_version_provenance(version), true,
        jazz_node__node__codec__VersionRow__tx_time(version), jazz_node__node__codec__VersionRow__tx_node_alias(version), out);
}

/* A reconstructed candidate without retained row metadata cannot prove a
 * provenance ownership clause. Keep that case fail-closed instead of
 * mistaking the incoming writer for the historic creator. */
static RowProvenance unresolved_provenance(void) {
    RowProvenance p = { jazz_types__ids__AuthorSubject__SYSTEM, 0, jazz_types__ids__AuthorSubject__SYSTEM, 0 };
    return p;
}

/* previous.provenance()?.unwrap_or_else(unresolved_provenance) */
static jz_err provenance_or_unresolved(const CurrentRow *row, RowProvenance *out) {
    bool has = false;
    JZ_TRY(jazz_node__node__CurrentRow__provenance(row, &has, out));
    if (!has) *out = unresolved_provenance();
    return JZ_OK;
}

/* ---------------- candidate unit evidence ---------------- */

/* One row a candidate commit unit writes on the main branch, prepared as
 * write-policy evidence for the unit's other writes (`INV-RLS-9`). */
typedef struct CandidateEvidenceRow {
    jz_str authored_table; /* Logical table as the authored versions name it. */
    jazz_types_RowUuid row_uuid;
    jazz_types_SchemaVersionId policy_schema; /* Policy schema and table the row is projected into. */
    jz_str policy_table;
    /* The row after the transaction. `None` only for a restore whose row has
     * no committed content to bring back. */
    bool has_after; CurrentRow after;
    /* The committed row an update replaces. It stays evidence until the
     * update's own checks pass. */
    bool has_before; CurrentRow before;
} CandidateEvidenceRow;

static void candidate_evidence_row_drop(CandidateEvidenceRow *r) {
    jz_str_drop(&r->authored_table);
    jz_str_drop(&r->policy_table);
    if (r->has_after) jazz_node__node__CurrentRow__drop(&r->after);
    if (r->has_before) jazz_node__node__CurrentRow__drop(&r->before);
}

/* Whether this row is an update: its post-state replaces committed content
 * that ungrounded checks still read. */
static bool CandidateEvidenceRow__replaces_committed(const CandidateEvidenceRow *self) {
    return self->has_before && self->has_after && !jazz_node__node__CurrentRow__PartialEq__eq(&self->before, &self->after);
}

static jazz_node_OverlayTableKey CandidateEvidenceRow__table_key(const CandidateEvidenceRow *self) {
    return table_key(self->policy_schema, &self->policy_table);
}

/* What a check reads for this row: its post-transaction content once the row
 * is grounded, otherwise its committed content for an update and nothing for
 * an insert or restore. Returns NULL for None, else a heap clone. */
static CurrentRow *CandidateEvidenceRow__shown(const CandidateEvidenceRow *self, bool grounded) {
    if (!self->has_after) return NULL;
    const CurrentRow *src = grounded ? &self->after : (self->has_before ? &self->before : NULL);
    if (!src) return NULL;
    CurrentRow *out = JZ_NEW(CurrentRow);
    jazz_node__node__CurrentRow__clone(out, src);
    return out;
}

/* The rows of one candidate commit unit, in `(authored_table, row_uuid)` order. */
JZ_NVEC(CandidateEvidenceRowVec, CandidateEvidenceRow);
typedef struct CandidateUnitEvidence { CandidateEvidenceRowVec rows; } CandidateUnitEvidence;

static void candidate_unit_evidence_drop(CandidateUnitEvidence *e) {
    for (u32 i = 0; i < e->rows.len; i++) candidate_evidence_row_drop(&e->rows.ptr[i]);
    jz_free(e->rows.ptr);
    e->rows.ptr = NULL; e->rows.len = e->rows.cap = 0;
}

/* binary_search_by (row.authored_table, row.row_uuid).cmp(&(table, row_uuid)) */
static bool CandidateUnitEvidence__row_index(const CandidateUnitEvidence *self, const jz_str *table, jazz_types_RowUuid row_uuid, usize *out) {
    u32 lo = 0, hi = self->rows.len;
    while (lo < hi) {
        u32 mid = lo + (hi - lo) / 2;
        const CandidateEvidenceRow *row = &self->rows.ptr[mid];
        int c = jz_slice_cmp(jz_str_slice(&row->authored_table), jz_str_slice(table));
        if (!c) c = jz_uuid_cmp(&row->row_uuid, &row_uuid);
        if (!c) { *out = mid; return true; }
        if (c < 0) lo = mid + 1; else hi = mid;
    }
    return false;
}

/* Rebuild the shared overlay tables named by `changed` (every table when
 * NULL) for the given grounded rows. Rows the unit deletes are not in the
 * evidence at all: a committed row stays visible as it was, and a row the
 * unit both inserts and deletes never counts. */
static void CandidateUnitEvidence__rebuild_tables(const CandidateUnitEvidence *self, jz_btree *tables, const bool *grounded, const jz_btree *changed) {
    jz_btree rebuilt;
    jz_btree_init(&rebuilt, &desc_rebuilt);
    for (u32 index = 0; index < self->rows.len; index++) {
        const CandidateEvidenceRow *row = &self->rows.ptr[index];
        jazz_node_OverlayTableKey key = CandidateEvidenceRow__table_key(row);
        if (changed && !jz_btree_contains(changed, &key)) { table_key_drop(&key); continue; }
        bool inserted = false;
        jz_btree *rows = jz_btree_entry(&rebuilt, &key, &inserted);
        if (inserted) jz_btree_init(rows, &desc_overlay_rows);
        else table_key_drop(&key);
        CurrentRow *shown = CandidateEvidenceRow__shown(row, grounded[index]);
        CurrentRow *old = NULL;
        if (jz_btree_insert(rows, &row->row_uuid, &shown, &old)) opt_row_drop(&old);
    }
    jazz_node_OverlayTableKey key;
    jz_btree rows;
    while (jz_btree_pop_first(&rebuilt, &key, &rows)) {
        jazz_node_TransactionOverlayTable *table = RS(TransactionOverlayTable__new)(rows);
        jazz_node_TransactionOverlayTable *old = NULL;
        if (jz_btree_insert(tables, &key, &table, &old)) { table_key_drop(&key); jz_rc_release(old); }
    }
    jz_btree_drop(&rebuilt);
}

/* ---------------- monotonicity ---------------- */

static bool predicates_are_monotone(const jazz_model_PredicateVec *predicates);
static bool predicate_is_monotone(const jazz_model_Predicate *predicate);
static bool join_is_monotone(const jazz_model_JoinVia *join);
static bool reachable_is_monotone(const jazz_model_ReachableVia *reachable);
static bool relation_is_monotone(const jazz_model_RelationExpr *relation);
static bool relation_predicate_is_monotone(const jazz_model_RelationPredicate *predicate);

/* Whether a write-policy clause can only grant more as rows are added:
 * existential joins, inner relation joins, reachability, inheritance and
 * row-level filters. `NOT` anywhere, a non-inner relation join, aggregates
 * and limits or offsets count as non-monotone, conservatively. */
static bool write_policy_query_is_monotone(const jazz_model_Query *query) {
    if (query->aggregate != NULL || query->has_limit || query->offset != 0 || query->array_subqueries.len != 0) return false;
    if (!predicates_are_monotone(&query->filters)) return false;
    for (u32 i = 0; i < query->joins.len; i++) if (!join_is_monotone(&query->joins.ptr[i])) return false;
    for (u32 i = 0; i < query->reachable.len; i++) if (!reachable_is_monotone(&query->reachable.ptr[i])) return false;
    for (u32 b = 0; b < query->policy_branches.len; b++) {
        const jazz_model_PolicyBranch *branch = &query->policy_branches.ptr[b];
        if (!predicates_are_monotone(&branch->filters)) return false;
        for (u32 i = 0; i < branch->joins.len; i++) if (!join_is_monotone(&branch->joins.ptr[i])) return false;
        for (u32 i = 0; i < branch->reachable.len; i++) if (!reachable_is_monotone(&branch->reachable.ptr[i])) return false;
    }
    return query->relation == NULL || relation_is_monotone(&query->relation->rel);
}

static bool query_inherits(const jazz_model_Query *query) {
    if (query->inherits.len != 0) return true;
    for (u32 b = 0; b < query->policy_branches.len; b++)
        if (query->policy_branches.ptr[b].inherits.len != 0) return true;
    return false;
}

static bool predicates_are_monotone(const jazz_model_PredicateVec *predicates) {
    for (u32 i = 0; i < predicates->len; i++)
        if (!predicate_is_monotone(&predicates->ptr[i])) return false;
    return true;
}

static bool predicate_is_monotone(const jazz_model_Predicate *predicate) {
    switch (predicate->tag) {
    case JAZZ_PRED_NOT: return false;
    case JAZZ_PRED_ALL:
    case JAZZ_PRED_ANY: return predicates_are_monotone(&predicate->children);
    case JAZZ_PRED_ENUM_MATCH: return predicate_is_monotone(predicate->inner);
    default: return true;
    }
}

static bool join_is_monotone(const jazz_model_JoinVia *join) {
    if (!predicates_are_monotone(&join->filters)) return false;
    for (u32 i = 0; i < join->nested_joins.len; i++)
        if (!join_is_monotone(&join->nested_joins.ptr[i])) return false;
    return true;
}

static bool reachable_is_monotone(const jazz_model_ReachableVia *reachable) {
    return predicates_are_monotone(&reachable->access_filters) && predicates_are_monotone(&reachable->edge_filters);
}

static bool relation_is_monotone(const jazz_model_RelationExpr *relation) {
    switch (relation->tag) {
    case JAZZ_REXPR_TABLE_SCAN: return true;
    case JAZZ_REXPR_FILTER: return relation_is_monotone(relation->input) && relation_predicate_is_monotone(&relation->predicate);
    case JAZZ_REXPR_UNION:
        for (u32 i = 0; i < relation->inputs.len; i++)
            if (!relation_is_monotone(&relation->inputs.ptr[i].input)) return false;
        return true;
    case JAZZ_REXPR_JOIN:
        return relation->join_kind == JAZZ_JOIN_INNER && relation_is_monotone(relation->input) && relation_is_monotone(relation->right);
    case JAZZ_REXPR_PROJECT:
    case JAZZ_REXPR_DISTINCT:
    case JAZZ_REXPR_ORDER_BY: return relation_is_monotone(relation->input);
    case JAZZ_REXPR_GATHER: return relation_is_monotone(relation->input) && relation_is_monotone(relation->right);
    case JAZZ_REXPR_OFFSET:
    case JAZZ_REXPR_LIMIT:
    default: return false;
    }
}

static bool relation_predicate_is_monotone(const jazz_model_RelationPredicate *predicate) {
    switch (predicate->tag) {
    case JAZZ_RPE_NOT: return false;
    case JAZZ_RPE_AND:
    case JAZZ_RPE_OR:
        for (u32 i = 0; i < predicate->items.len; i++)
            if (!relation_predicate_is_monotone(&predicate->items.ptr[i])) return false;
        return true;
    case JAZZ_RPE_ENUM_MATCH: return relation_predicate_is_monotone(predicate->inner);
    default: return true;
    }
}

/* ---------------- UnitWritePolicyDecision ---------------- */

static void unsupported_transaction_policy_evidence(u32 *decision, jz_str *reason) {
    char buf[160];
    snprintf(buf, sizeof buf,
             "Reading more than %u rows of a transaction's own writes in its write-policy checks is not supported yet",
             (unsigned)JZN_MAX_TRANSACTION_POLICY_EVIDENCE_ROWS);
    *decision = JZN_UNIT_POLICY_UNSUPPORTED;
    *reason = jz_str_from(buf);
}

static void current_row_cells(const jazz_model_TableSchema *table, const CurrentRow *row, jz_btree *out) {
    jz_btree_init(out, &jzn_desc_str_value);
    for (u32 i = 0; i < table->columns.len; i++) {
        const jazz_model_ColumnSchema *column = &table->columns.ptr[i];
        groove_Value value;
        if (jazz_node__node__CurrentRow__cell(row, table, &column->name, &value)) {
            jz_str name = jz_str_clone(&column->name);
            groove_Value old;
            if (jz_btree_insert(out, &name, &value, &old)) { jz_str_drop(&name); groove__records__values__Value__drop(&old); }
        }
    }
}

/* cells.extend(other) (consumes `other`) */
static void cells_extend(jz_btree *cells, jz_btree *other) {
    jz_str k; groove_Value v;
    while (jz_btree_pop_first(other, &k, &v)) {
        groove_Value old;
        if (jz_btree_insert(cells, &k, &v, &old)) { jz_str_drop(&k); groove__records__values__Value__drop(&old); }
    }
    jz_btree_drop(other);
}

static void str_clone_v(void *d, const void *s) { *(jz_str *)d = jz_str_clone(s); }
static void cells_clone(jz_btree *dst, const jz_btree *src) {
    jz_btree_clone(dst, src, str_clone_v, groove__records__values__Value__clone);
}

/* (SchemaVersionId, TableSchema, BTreeMap<String, Value>) */
typedef struct policy_projection {
    jazz_types_SchemaVersionId schema;
    jazz_model_TableSchema table;
    jz_btree cells;
} policy_projection;
static void policy_projection_drop(policy_projection *p) {
    jazz_model__schema__TableSchema__drop(&p->table);
    jz_btree_drop(&p->cells);
}

static jz_err policy_projection_for_version_row(NS *self, const jazz_node_VersionRow *version, policy_projection *out);
static jz_err policy_projection_for_version_record(NS *self, const VR *version, policy_projection *out);
static jz_err translate_policy_cells(NS *self, jazz_types_SchemaVersionId source, const jz_str *table, jz_btree cells, policy_projection *out);
static jz_err policy_target_schema_for_source(NS *self, jazz_types_SchemaVersionId source, const jz_str *table, jazz_types_SchemaVersionId *out);
static jz_err source_reaches_write_policy_table(NS *self, jazz_types_SchemaVersionId source, jazz_types_SchemaVersionId target, const jz_str *table, bool *out);
static jz_err policy_delete_subject_row(NS *self, jazz_types_SchemaVersionId policy_schema_version, const jazz_model_TableSchema *table, const VR *version,
                                        const jazz_model_TxId *candidate_tx_id, const jazz_protocol_VersionRecordVec *candidate_versions, bool *has, CurrentRow *out);
static jz_err policy_previous_content_subject_row(NS *self, jazz_types_SchemaVersionId policy_schema_version, const jazz_model_TableSchema *table,
                                                  const VR *version, const jazz_model_TxId *candidate_tx_id, bool *has, CurrentRow *out);
static jz_err policy_local_current_subject_row(NS *self, const jazz_model_TableSchema *table, jazz_types_RowUuid row_uuid, bool *has, CurrentRow *out);
static jz_err write_policy_allows_version_record_for_view(NS *self, const VR *version, jazz_types_AuthorSubject author, const jazz_model_JazzSchema *exact_view,
                                                          const jazz_model_TxId *candidate_tx_id, const jazz_protocol_VersionRecordVec *candidate_versions,
                                                          const jazz_node_TransactionWriteOverlay *transaction_overlay, bool *out);
static bool policy_tables_are_directly_compatible(const jazz_model_TableSchema *source, const jazz_model_TableSchema *target);

static const jazz_protocol_VersionRecordVec EMPTY_VERSIONS = { 0 };

/* Build a typed row-preview for advisory policy evaluation without letting
 * durable attribution replace the selected policy capability. */
static jz_err durable_author_policy_preview(NS *self, jazz_node_MergeableCommit *commit, jazz_types_AuthorSubject *permission_subject) {
    *permission_subject = jazz_node__node__MergeableCommit__effective_permission_subject(commit);
    jazz_types_RowAuthor author;
    jz_err e = jazz_types__ids__RowAuthor__from_session(commit->made_by, self->node_uuid, &author);
    if (e.code) { jz_err_drop(&e); return jz_err_new(JZN_E_UNADMITTED_WRITE_AUTHOR, NULL); }
    commit->made_by = jazz_types__ids__RowAuthor__as_author_subject(author);
    return JZ_OK;
}

jz_err jazz_node__node__policy__NodeState__write_policy_allows_version_record(NS *self, const VR *version, jazz_types_AuthorSubject author,
                                                                             const jazz_model_TxId *candidate_tx_id,
                                                                             const jazz_protocol_VersionRecordVec *candidate_versions, bool *out) {
    jazz_node_TransactionWriteOverlay overlay = RS(TransactionWriteOverlay__Default__default)();
    jz_err e = write_policy_allows_version_record_for_view(self, version, author, NULL, candidate_tx_id, candidate_versions, &overlay, out);
    jazz_node_TransactionWriteOverlay_drop(&overlay);
    return e;
}

static jz_err unit_write_policies_monotone(NS *self, const jazz_protocol_VersionRecordVec *versions, bool **out);
static jz_err unit_write_policy_check(NS *self, const jazz_protocol_VersionRecordVec *versions, usize index, bool has_own, usize own_row,
                                      const CandidateUnitEvidence *evidence, const jazz_node_TransactionWriteOverlay *overlay,
                                      jazz_types_AuthorSubject author, jazz_model_TxId candidate_tx_id, jz_btree *reads, usize *spent,
                                      bool *some, bool *allowed);
static jz_err candidate_unit_evidence(NS *self, const jazz_protocol_VersionRecordVec *versions, jazz_types_AuthorSubject author,
                                      jazz_model_TxId candidate_tx_id, CandidateUnitEvidence *out);

/* Whether a passing check already saw everything the post-state could change
 * for it. A monotone policy can only lose a pass to an update grounded later;
 * any other policy to any ungrounded row. (the `saw_post_state` closure) */
static bool saw_post_state(const CandidateUnitEvidence *evidence, bool has_own, usize own, bool monotone, const jz_btree *reads, const bool *grounded,
                           const jz_btree *ungrounded_live, const jz_btree *ungrounded_updates) {
    jazz_node_OverlayTableKey *table; void *unit;
    JZ_BTREE_FOR(reads, table, unit) {
        (void)unit;
        const CandidateEvidenceRow *own_row = NULL;
        if (has_own && !grounded[own]) {
            const CandidateEvidenceRow *row = &evidence->rows.ptr[own];
            jazz_node_OverlayTableKey k = CandidateEvidenceRow__table_key(row);
            if (cmp_table_key(&k, table, NULL) == 0) own_row = row;
            table_key_drop(&k);
        }
        usize own_counted, count;
        const usize *c;
        if (monotone) {
            own_counted = own_row && CandidateEvidenceRow__replaces_committed(own_row);
            c = jz_btree_get(ungrounded_updates, table);
        } else {
            own_counted = own_row && own_row->has_after;
            c = jz_btree_get(ungrounded_live, table);
        }
        count = c ? *c : 0;
        if (count != own_counted) return false;
    }
    return true;
}

static void count_dec(jz_btree *counts, const jazz_node_OverlayTableKey *key) {
    usize *c = jz_btree_get(counts, key);
    if (c) *c -= 1;
}
static void count_inc(jz_btree *counts, jazz_node_OverlayTableKey key) {
    bool inserted = false;
    usize *c = jz_btree_entry(counts, &key, &inserted);
    if (!inserted) table_key_drop(&key);
    *c += 1;
}

static bool sets_disjoint(const jz_btree *a, const jz_btree *b) {
    void *k, *u;
    JZ_BTREE_FOR(a, k, u) { (void)u; if (jz_btree_contains(b, k)) return false; }
    return true;
}

static jz_btree *new_shared_tables(const jz_btree *tables) {
    jz_btree *shared = JZ_NEW(jz_btree);
    jz_btree_clone(shared, tables, table_key_clone, overlay_table_clone);
    return shared;
}

/* Decide every write policy of one commit unit (`INV-RLS-9`).
 *
 * A transaction reads its own writes, so its policy checks do too. Each
 * write's WITH CHECK clause (insert check, update check) reads committed
 * state overlaid with the unit's other inserts, restores and updates; the
 * unit's deletes are not overlaid, and the written row itself is the inline
 * candidate. A commit unit carries its writes as a canonical set, not in
 * write order, so the checks run to a fixpoint: a row's post-transaction
 * content becomes evidence once all of its own checks pass ("grounded"),
 * until then an update shows its committed content and an insert or restore
 * shows nothing, so writes cannot justify each other in a cycle. When the
 * fixpoint passes every write, each write that passed before all the unit
 * rows it reads were grounded is checked once more against the unit's full
 * post-state, and the unit is accepted only if those checks pass too. USING
 * clauses judge the rows the transaction acts on as committed. Other
 * transactions' writes are never evidence.
 *
 * Each round shares one overlay per table among its checks, and a failed
 * write is re-checked only when a table its policy reads gained grounded
 * rows. A unit whose checks would read more than
 * MAX_TRANSACTION_POLICY_EVIDENCE_ROWS overlaid rows in total is not
 * supported yet. */
jz_err jazz_node__node__policy__NodeState__commit_unit_write_policies_allow(NS *self, const jazz_protocol_VersionRecordVec *versions,
                                                                           jazz_types_AuthorSubject author, jazz_model_TxId candidate_tx_id,
                                                                           u32 *decision, jz_str *unsupported_reason) {
    CandidateUnitEvidence evidence = { { 0 } };
    JZ_TRY(candidate_unit_evidence(self, versions, author, candidate_tx_id, &evidence));
    if (evidence.rows.len == 0) {
        /* No write can see another unit row: committed state alone decides each write. */
        jazz_node_TransactionWriteOverlay accepted = RS(TransactionWriteOverlay__accepted_state)();
        for (u32 i = 0; i < versions->len; i++) {
            bool allowed = false;
            jz_err e = write_policy_allows_version_record_for_view(self, versions->ptr[i], author, NULL, &candidate_tx_id, versions, &accepted, &allowed);
            if (e.code) { jazz_node_TransactionWriteOverlay_drop(&accepted); return e; }
            if (!allowed) { jazz_node_TransactionWriteOverlay_drop(&accepted); *decision = JZN_UNIT_POLICY_DENIED; return JZ_OK; }
        }
        jazz_node_TransactionWriteOverlay_drop(&accepted);
        *decision = JZN_UNIT_POLICY_ALLOWED;
        return JZ_OK;
    }

    jz_err e = JZ_OK;
    u32 n = versions->len, nrows = evidence.rows.len;
    bool *has_own = jz_zalloc((n ? n : 1) * sizeof(bool));
    usize *own_row = jz_zalloc((n ? n : 1) * sizeof(usize));
    for (u32 i = 0; i < n; i++) {
        const VR *version = versions->ptr[i];
        if (jazz_protocol__protocol__VersionRecord__branch_key(version)->values.len == 0)
            has_own[i] = CandidateUnitEvidence__row_index(&evidence, jazz_protocol__protocol__VersionRecord__table(version),
                                                          jazz_protocol__protocol__VersionRecord__row_uuid(version), &own_row[i]);
    }
    /* Versions still to pass per row, and per table the rows not yet grounded
     * whose post-state differs from what an ungrounded row shows. */
    usize *unpassed = jz_zalloc((nrows) * sizeof(usize));
    for (u32 i = 0; i < n; i++) if (has_own[i]) unpassed[own_row[i]] += 1;
    /* `ungrounded_live`: rows whose post-state differs from what they show
     * while ungrounded (inserts, restores and updates). `ungrounded_updates`:
     * the subset that replaces committed content (updates), the only rows
     * whose grounding can take evidence away from a monotone policy. */
    jz_btree ungrounded_live, ungrounded_updates;
    jz_btree_init(&ungrounded_live, &desc_table_key_count);
    jz_btree_init(&ungrounded_updates, &desc_table_key_count);
    for (u32 r = 0; r < nrows; r++) {
        const CandidateEvidenceRow *row = &evidence.rows.ptr[r];
        if (row->has_after) count_inc(&ungrounded_live, CandidateEvidenceRow__table_key(row));
        if (CandidateEvidenceRow__replaces_committed(row)) count_inc(&ungrounded_updates, CandidateEvidenceRow__table_key(row));
    }
    bool *monotone = NULL;
    bool *grounded = jz_zalloc((nrows) * sizeof(bool));
    bool *post_state_checked = jz_zalloc((n ? n : 1) * sizeof(bool));
    jz_btree *reads = jz_zalloc((n ? n : 1) * sizeof(jz_btree));
    for (u32 i = 0; i < n; i++) jz_btree_init(&reads[i], &desc_table_key_set);
    /* The first round has nothing grounded, so every unit row shows what
     * committed state already shows: it runs on committed state alone, with
     * no overlaid rows and nothing charged to the evidence budget, and only
     * records which tables each policy reads. */
    jz_btree tables;
    jz_btree_init(&tables, &desc_overlay_tables);
    bool first_round = true;
    usize spent = 0;
    jz_btree failed;
    jz_btree_init(&failed, &desc_usize_set);
    JZ_NVEC(usize_vec, usize);
    usize_vec pending = { 0 };
    for (usize i = 0; i < n; i++) JZ_VEC_PUSH(pending, i);
    e = unit_write_policies_monotone(self, versions, &monotone);
    if (e.code) goto done;
    for (;;) {
        jazz_node_TransactionWriteOverlay overlay = RS(TransactionWriteOverlay__from_tables)(new_shared_tables(&tables));
        usize_vec newly_grounded = { 0 };
        for (u32 p = 0; p < pending.len; p++) {
            usize index = pending.ptr[p];
            bool some = false, allowed = false;
            e = unit_write_policy_check(self, versions, index, has_own[index], own_row[index], &evidence, &overlay, author, candidate_tx_id,
                                        &reads[index], &spent, &some, &allowed);
            if (e.code) break;
            if (!some) {
                unsupported_transaction_policy_evidence(decision, unsupported_reason);
                jazz_node_TransactionWriteOverlay_drop(&overlay);
                jz_free(newly_grounded.ptr);
                goto done;
            }
            if (allowed) {
                jz_btree_remove(&failed, &index, NULL, NULL);
                post_state_checked[index] = saw_post_state(&evidence, has_own[index], own_row[index], monotone[index], &reads[index], grounded,
                                                           &ungrounded_live, &ungrounded_updates);
                /* A row is grounded once every version the unit writes for
                 * it has passed; it becomes evidence next round. */
                if (has_own[index]) {
                    usize own = own_row[index];
                    unpassed[own] -= 1;
                    if (unpassed[own] == 0) JZ_VEC_PUSH(newly_grounded, own);
                }
            } else {
                jz_btree_insert(&failed, &index, NULL, NULL);
            }
        }
        jazz_node_TransactionWriteOverlay_drop(&overlay);
        if (e.code) { jz_free(newly_grounded.ptr); goto done; }
        if (failed.len == 0) { jz_free(newly_grounded.ptr); break; }
        jz_btree changed;
        jz_btree_init(&changed, &desc_table_key_set);
        for (u32 g = 0; g < newly_grounded.len; g++) {
            usize own = newly_grounded.ptr[g];
            grounded[own] = true;
            const CandidateEvidenceRow *row = &evidence.rows.ptr[own];
            jazz_node_OverlayTableKey key = CandidateEvidenceRow__table_key(row);
            if (row->has_after) count_dec(&ungrounded_live, &key);
            if (CandidateEvidenceRow__replaces_committed(row)) count_dec(&ungrounded_updates, &key);
            if (row->has_after) { if (!jz_btree_insert(&changed, &key, NULL, NULL)) continue; table_key_drop(&key); }
            else table_key_drop(&key);
        }
        jz_free(newly_grounded.ptr);
        /* Only a table that gained grounded rows can change a failed check's
         * evidence. Without one, every failure is final. */
        if (changed.len == 0) {
            jz_btree_drop(&changed);
            *decision = JZN_UNIT_POLICY_DENIED;
            goto done;
        }
        if (first_round) {
            CandidateUnitEvidence__rebuild_tables(&evidence, &tables, grounded, NULL);
            first_round = false;
        } else {
            CandidateUnitEvidence__rebuild_tables(&evidence, &tables, grounded, &changed);
        }
        pending.len = 0;
        usize *idx; void *u;
        JZ_BTREE_FOR(&failed, idx, u) { (void)u; if (!sets_disjoint(&reads[*idx], &changed)) JZ_VEC_PUSH(pending, *idx); }
        jz_btree_drop(&changed);
    }

    /* Every write passed, so every row is grounded and the tables now show
     * the unit's post-state. Re-check each write whose passing check read a
     * unit row that was not grounded yet. */
    {
        usize_vec final_pass = { 0 };
        for (usize i = 0; i < n; i++) if (!post_state_checked[i]) JZ_VEC_PUSH(final_pass, i);
        if (final_pass.len == 0) { *decision = JZN_UNIT_POLICY_ALLOWED; goto done; }
        for (u32 r = 0; r < nrows; r++) grounded[r] = true;
        CandidateUnitEvidence__rebuild_tables(&evidence, &tables, grounded, NULL);
        jazz_node_TransactionWriteOverlay overlay = RS(TransactionWriteOverlay__from_tables)(new_shared_tables(&tables));
        *decision = JZN_UNIT_POLICY_ALLOWED;
        for (u32 p = 0; p < final_pass.len; p++) {
            usize index = final_pass.ptr[p];
            bool some = false, allowed = false;
            e = unit_write_policy_check(self, versions, index, has_own[index], own_row[index], &evidence, &overlay, author, candidate_tx_id,
                                        &reads[index], &spent, &some, &allowed);
            if (e.code) break;
            if (!some) { unsupported_transaction_policy_evidence(decision, unsupported_reason); break; }
            if (!allowed) { *decision = JZN_UNIT_POLICY_DENIED; break; }
        }
        jazz_node_TransactionWriteOverlay_drop(&overlay);
        jz_free(final_pass.ptr);
    }

done:
    jz_free(pending.ptr);
    jz_btree_drop(&failed);
    jz_btree_drop(&tables);
    for (u32 i = 0; i < n; i++) jz_btree_drop(&reads[i]);
    jz_free(reads);
    jz_free(post_state_checked);
    jz_free(grounded);
    jz_free(monotone);
    jz_btree_drop(&ungrounded_live);
    jz_btree_drop(&ungrounded_updates);
    jz_free(unpassed);
    jz_free(own_row);
    jz_free(has_own);
    candidate_unit_evidence_drop(&evidence);
    return e;
}

/* Whether each version's WITH CHECK policies are monotone in the rows present
 * (`INV-RLS-9`), decided statically per policy table and cached per unit. A
 * table whose insert or update check uses a non-monotone construct, or
 * inherits through a schema that has one, is not. */
static jz_err unit_write_policies_monotone(NS *self, const jazz_protocol_VersionRecordVec *versions, bool **out) {
    jz_btree by_table;
    jz_btree_init(&by_table, &desc_table_key_bool);
    bool *monotone = jz_zalloc((versions->len ? versions->len : 1) * sizeof(bool));
    for (u32 i = 0; i < versions->len; i++) {
        const VR *version = versions->ptr[i];
        jazz_node_OverlayTableKey key = table_key(jazz_protocol__protocol__VersionRecord__schema_version(version),
                                                  jazz_protocol__protocol__VersionRecord__table(version));
        const bool *known_cached = jz_btree_get(&by_table, &key);
        if (known_cached) { monotone[i] = *known_cached; table_key_drop(&key); continue; }
        policy_projection proj;
        jz_err e = policy_projection_for_version_record(self, version, &proj);
        if (e.code) { table_key_drop(&key); jz_btree_drop(&by_table); jz_free(monotone); return e; }
        const jazz_model_Query *checks[2] = { proj.table.write_policies.insert_check, proj.table.write_policies.update_check };
        bool known = true;
        for (u32 c = 0; c < 2; c++) if (checks[c] && !write_policy_query_is_monotone(checks[c])) known = false;
        bool inherits = false;
        for (u32 c = 0; c < 2; c++) if (checks[c] && query_inherits(checks[c])) inherits = true;
        if (known && inherits) {
            /* An inherited clause evaluates another table's policy; accept it
             * as monotone only when every policy of the schema is. */
            const jazz_model_JazzSchema *schema = jazz_node__node__SchemaCatalogue__schema_for_version(&self->catalogue, proj.schema);
            known = schema != NULL;
            if (schema) {
                const jazz_model_TableSchemaVec *ts = jazz_model__schema__JazzSchema__tables(schema);
                for (u32 t = 0; known && t < ts->len; t++) {
                    const jazz_model_TableSchema *table = &ts->ptr[t];
                    const jazz_model_Query *all[5] = { table->read_policy, table->write_policies.insert_check, table->write_policies.update_using,
                                                       table->write_policies.update_check, table->write_policies.delete_using };
                    for (u32 q = 0; known && q < 5; q++) if (all[q] && !write_policy_query_is_monotone(all[q])) known = false;
                }
            }
        }
        policy_projection_drop(&proj);
        jz_btree_insert(&by_table, &key, &known, NULL);
        monotone[i] = known;
    }
    jz_btree_drop(&by_table);
    *out = monotone;
    return JZ_OK;
}

/* Arc<Mutex<BTreeSet<(SchemaVersionId, String)>>> payload drop */
static void overlay_reads_drop(void *p) { jz_btree_drop(&((jazz_node_OverlayReads *)p)->set); }

/* Check one write of a unit against a round's shared overlay, leaving its own
 * row out by lookup and recording the tables its policy reads. `*some` is
 * false once the unit's evidence budget is spent. */
static jz_err unit_write_policy_check(NS *self, const jazz_protocol_VersionRecordVec *versions, usize index, bool has_own, usize own_row,
                                      const CandidateUnitEvidence *evidence, const jazz_node_TransactionWriteOverlay *overlay_in,
                                      jazz_types_AuthorSubject author, jazz_model_TxId candidate_tx_id, jz_btree *reads, usize *spent,
                                      bool *some, bool *allowed) {
    *some = false;
    if (*spent > JZN_MAX_TRANSACTION_POLICY_EVIDENCE_ROWS) return JZ_OK;
    const VR *version = versions->ptr[index];
    jazz_node_OverlayReads *recorder = jz_rc_new(sizeof(jazz_node_OverlayReads), overlay_reads_drop);
    recorder->poisoned = false;
    jz_btree_init(&recorder->set, &desc_table_key_set);
    jazz_node_TransactionWriteOverlay overlay = RS(TransactionWriteOverlay__recording)(overlay_in, jz_rc_clone(recorder));
    if (has_own) {
        const CandidateEvidenceRow *row = &evidence->rows.ptr[own_row];
        jazz_node_TransactionWriteOverlay excluded = RS(TransactionWriteOverlay__excluding)(&overlay, row->policy_schema, jz_str_slice(&row->policy_table), row->row_uuid);
        jazz_node_TransactionWriteOverlay_drop(&overlay);
        overlay = excluded;
    }
    jz_err e = write_policy_allows_version_record_for_view(self, version, author, NULL, &candidate_tx_id, versions, &overlay, allowed);
    if (!e.code) {
        if (recorder->poisoned) e = JZN_INVALID_STORED("transaction overlay reads poisoned");
        else {
            jz_btree_drop(reads);
            *reads = recorder->set; /* std::mem::take */
            jz_btree_init(&recorder->set, &desc_table_key_set);
            *spent += RS(TransactionWriteOverlay__rows_in)(&overlay, reads);
            *some = true;
        }
    }
    jazz_node_TransactionWriteOverlay_drop(&overlay);
    jz_rc_release(recorder);
    return e;
}

/* (String, RowUuid) -> Vec<&VersionRecord> */
typedef struct { jz_str table; jazz_types_RowUuid row; } table_row_key;
static int cmp_table_row(const void *a, const void *b, void *c) {
    (void)c;
    const table_row_key *x = a, *y = b;
    int r = jz_slice_cmp(jz_str_slice(&x->table), jz_str_slice(&y->table));
    return r ? r : jz_uuid_cmp(&x->row, &y->row);
}
static void table_row_key_drop(void *p) { jz_str_drop(&((table_row_key *)p)->table); }
JZ_NVEC(vr_ref_vec, const VR *);
static void vr_ref_vec_drop(void *p) { jz_free(((vr_ref_vec *)p)->ptr); }
static const jz_btree_desc desc_by_row = { sizeof(table_row_key), sizeof(vr_ref_vec), cmp_table_row, table_row_key_drop, vr_ref_vec_drop };

/* Prepare the main-branch rows of a candidate unit as evidence for its other
 * writes. A unit that writes one row has nothing to overlay. A row the unit
 * deletes contributes nothing, so committed state shows it as committed (and
 * a row the unit both inserts and deletes not at all). */
static jz_err candidate_unit_evidence(NS *self, const jazz_protocol_VersionRecordVec *versions, jazz_types_AuthorSubject author,
                                      jazz_model_TxId candidate_tx_id, CandidateUnitEvidence *out) {
    out->rows.ptr = NULL; out->rows.len = out->rows.cap = 0;
    if (JZN_AUTHOR_IS_SYSTEM(author)) return JZ_OK;
    jz_btree by_row;
    jz_btree_init(&by_row, &desc_by_row);
    for (u32 i = 0; i < versions->len; i++) {
        const VR *version = versions->ptr[i];
        /* Branch-local writes are evaluated in their own branch view, which
         * this overlay does not model. */
        if (jazz_protocol__protocol__VersionRecord__branch_key(version)->values.len != 0) continue;
        table_row_key key = { jz_str_clone(jazz_protocol__protocol__VersionRecord__table(version)), jazz_protocol__protocol__VersionRecord__row_uuid(version) };
        bool inserted = false;
        vr_ref_vec *list = jz_btree_entry(&by_row, &key, &inserted);
        if (!inserted) table_row_key_drop(&key);
        JZ_VEC_PUSH(*list, version);
    }
    if (by_row.len < 2) { jz_btree_drop(&by_row); return JZ_OK; }
    jz_err e = JZ_OK;
    table_row_key *key; vr_ref_vec *row_versions;
    JZ_BTREE_FOR(&by_row, key, row_versions) {
        bool deleted = false;
        const VR *content = NULL;
        for (u32 i = 0; i < row_versions->len; i++) {
            u32 d = jazz_protocol__protocol__VersionRecord__deletion(row_versions->ptr[i]);
            if (d == JZN_DELETION_DELETED) deleted = true;
            if (d == JZN_DELETION_NONE && !content) content = row_versions->ptr[i];
        }
        if (deleted) continue;
        const VR *subject = content ? content : row_versions->ptr[0];
        policy_projection proj;
        e = policy_projection_for_version_record(self, subject, &proj);
        if (e.code) break;
        bool has_previous = false;
        CurrentRow previous;
        e = policy_previous_content_subject_row(self, proj.schema, &proj.table, subject, &candidate_tx_id, &has_previous, &previous);
        if (e.code) { policy_projection_drop(&proj); break; }
        CandidateEvidenceRow row = { 0 };
        if (content) {
            jz_btree after_cells;
            if (has_previous) current_row_cells(&proj.table, &previous, &after_cells);
            else jz_btree_init(&after_cells, &jzn_desc_str_value);
            cells_extend(&after_cells, &proj.cells);
            jz_btree_init(&proj.cells, &jzn_desc_str_value);
            RowProvenance provenance;
            if (has_previous) {
                RowProvenance prev;
                e = provenance_or_unresolved(&previous, &prev);
                provenance = updated_provenance(prev, content);
            } else {
                provenance = version_provenance(content);
            }
            if (!e.code) {
                jazz_types_TxTime t0 = { 0 };
                e = jazz_node__node__codec__current_row_from_cells_with_explicit_provenance(&proj.table, key->row, &after_cells, provenance, false, t0, 0, &row.after);
            }
            jz_btree_drop(&after_cells);
            if (e.code) {
                if (has_previous) jazz_node__node__CurrentRow__drop(&previous);
                policy_projection_drop(&proj);
                break;
            }
            row.has_after = true;
            row.has_before = has_previous;
            if (has_previous) row.before = previous;
        } else {
            /* A restore brings back the row's current content, which
             * committed state still reports as deleted. */
            row.has_after = has_previous;
            if (has_previous) row.after = previous;
            row.has_before = false;
        }
        row.authored_table = jz_str_clone(&key->table);
        row.row_uuid = key->row;
        row.policy_schema = proj.schema;
        row.policy_table = jz_str_clone(&proj.table.name);
        policy_projection_drop(&proj);
        JZ_VEC_PUSH(out->rows, row);
    }
    jz_btree_drop(&by_row);
    if (e.code) candidate_unit_evidence_drop(out);
    return e;
}

/* A session update/upsert of an existing row also requires that the fate
 * authority can read that previous row. This is deliberately decided at
 * authority admission, never while a client stages its mergeable
 * transaction: a replica can retain the target preimage without the private
 * support rows that make its read policy true. */
jz_err jazz_node__node__policy__NodeState__version_satisfies_read_for_write_visibility(NS *self, const VR *version, jazz_types_AuthorSubject author,
                                                                                      const jazz_model_TxId *candidate_tx_id, bool *out) {
    if (JZN_AUTHOR_IS_SYSTEM(author) || jazz_protocol__protocol__VersionRecord__deletes_row(version)) { *out = true; return JZ_OK; }
    policy_projection proj;
    JZ_TRY(policy_projection_for_version_record(self, version, &proj));
    bool has_previous = false;
    CurrentRow previous;
    jz_err e = policy_previous_content_subject_row(self, proj.schema, &proj.table, version, candidate_tx_id, &has_previous, &previous);
    if (e.code || !has_previous) {
        /* This is an INSERT (including an absent-target UPSERT), for which
         * INV-RLS-20 does not require prior read visibility. */
        policy_projection_drop(&proj);
        if (!e.code) *out = true;
        return e;
    }
    jazz_model_Query read_policy = jazz_node__node__query_eval__bindings__authorization_query_from_read_policy(&proj.table);
    jz_btree previous_cells;
    current_row_cells(&proj.table, &previous, &previous_cells);
    RowProvenance provenance;
    e = provenance_or_unresolved(&previous, &provenance);
    if (!e.code)
        e = QE_AUTHZ(read_policy_query_allows_candidate_with_provenance_for_schema)(self, proj.schema, &proj.table, &read_policy,
                                                                                  jazz_node__node__CurrentRow__row_uuid(&previous), &previous_cells,
                                                                                  author, provenance, out);
    jz_btree_drop(&previous_cells);
    jazz_model__query__Query__drop_v(&read_policy);
    jazz_node__node__CurrentRow__drop(&previous);
    policy_projection_drop(&proj);
    return e;
}

/* Prove read-for-write visibility of the logical branch-view source that was
 * copied into a first physical head overlay.
 *
 * A normal mergeable update obtains its prior row from the target's physical
 * history. A first branch overlay has intentionally no cross-branch parent,
 * so that lookup says "insert". Its separately versioned evidence lets the
 * authority resolve the inherited source and evaluate the ordinary read
 * policy against it without turning the source into a causal dependency or
 * exposing policy support to the client. */
jz_err jazz_node__node__policy__NodeState__branch_view_copy_satisfies_read_for_write_visibility(NS *self, const jazz_model_BranchViewCopyEvidence *evidence,
                                                                                               jazz_types_SchemaVersionId authored_schema,
                                                                                               jazz_types_AuthorSubject author,
                                                                                               const jazz_model_TxId *candidate_tx_id, bool *out) {
    if (JZN_AUTHOR_IS_SYSTEM(author)) { *out = true; return JZ_OK; }
    jazz_node_VersionRow *source_row = NULL;
    JZ_TRY(jazz_node__node__source_resolution__NodeState__resolve_branch_view_copy_evidence(self, evidence, authored_schema, candidate_tx_id, &source_row));
    if (!source_row) { *out = false; return JZ_OK; }
    VR *source = NULL;
    jz_err e = jazz_node__node__NodeState__version_record_from_row(self, source_row, &source);
    jazz_node__node__codec__VersionRow__drop(source_row);
    if (e.code) return e;
    policy_projection proj;
    e = policy_projection_for_version_record(self, source, &proj);
    if (!e.code) {
        jazz_model_Query read_policy = jazz_node__node__query_eval__bindings__authorization_query_from_read_policy(&proj.table);
        RowProvenance provenance = version_provenance(source);
        e = QE_AUTHZ(read_policy_query_allows_candidate_with_provenance_for_schema)(self, proj.schema, &proj.table, &read_policy,
                                                                                  jazz_protocol__protocol__VersionRecord__row_uuid(source), &proj.cells,
                                                                                  author, provenance, out);
        jazz_model__query__Query__drop_v(&read_policy);
        policy_projection_drop(&proj);
    }
    jazz_protocol__protocol__VersionRecord__drop(source);
    return e;
}

static jz_err write_policy_allows_version_record_for_view(NS *self, const VR *version, jazz_types_AuthorSubject author, const jazz_model_JazzSchema *exact_view,
                                                          const jazz_model_TxId *candidate_tx_id, const jazz_protocol_VersionRecordVec *candidate_versions,
                                                          const jazz_node_TransactionWriteOverlay *transaction_overlay, bool *out) {
    *out = false;
    if (JZN_AUTHOR_IS_SYSTEM(author)) { *out = true; return JZ_OK; }
    policy_projection proj;
    if (exact_view) {
        const jz_str *vtable = jazz_protocol__protocol__VersionRecord__table(version);
        const jazz_model_TableSchemaVec *ts = jazz_model__schema__JazzSchema__tables(exact_view);
        const jazz_model_TableSchema *found = NULL;
        for (u32 i = 0; i < ts->len; i++) if (jz_str_eq(&ts->ptr[i].name, vtable)) { found = &ts->ptr[i]; break; }
        if (!found) return jz_err_fmt(JZN_E_TABLE_NOT_FOUND, "%.*s", (int)vtable->len, vtable->ptr);
        jazz_model__schema__TableSchema__clone(&proj.table, found);
        jz_btree_init(&proj.cells, &jzn_desc_str_value);
        for (u32 idx = 0; idx < proj.table.columns.len; idx++) {
            groove_Value value;
            if (jazz_protocol__protocol__VersionRecord__optional_cell_at(version, idx, &value)) {
                jz_str name = jz_str_clone(&proj.table.columns.ptr[idx].name);
                jz_btree_insert(&proj.cells, &name, &value, NULL);
            }
        }
        proj.schema = jazz_protocol__protocol__VersionRecord__schema_version(version);
    } else {
        JZ_TRY(policy_projection_for_version_record(self, version, &proj));
    }
    jazz_types_SchemaVersionId policy_schema_version = proj.schema;
    const jazz_model_TableSchema *table = &proj.table;
    jazz_types_RowUuid row_uuid = jazz_protocol__protocol__VersionRecord__row_uuid(version);
    jz_err e = JZ_OK;
    /* Every user operation requires an explicit grant, including on a table
     * with no policies at all. SYSTEM is the explicit bypass above. */
    if (jazz_protocol__protocol__VersionRecord__deletes_row(version)) {
        const jazz_model_Query *policy = table->write_policies.delete_using;
        if (!policy) goto done;
        bool has_current = false;
        CurrentRow current;
        e = policy_delete_subject_row(self, policy_schema_version, table, version, candidate_tx_id, candidate_versions, &has_current, &current);
        if (e.code) goto done;
        if (!has_current) {
            /* An insert followed by a delete in one transaction is one row
             * image carrying both its content and the deletion. It must pass
             * the insert grant, then the delete grant against that same
             * content. */
            if (proj.cells.len == 0) goto done;
            const jazz_model_Query *insert_policy = table->write_policies.insert_check;
            if (!insert_policy) goto done;
            bool ok = false;
            e = QE_AUTHZ(write_policy_query_allows_candidate_with_provenance_for_schema)(self, policy_schema_version, table, insert_policy, row_uuid,
                                                                                       &proj.cells, author, true, version_provenance(version), &ok);
            if (e.code || !ok) goto done;
            jazz_types_TxTime t0 = { 0 };
            e = jazz_node__node__codec__current_row_from_cells_with_explicit_provenance(table, row_uuid, &proj.cells, version_provenance(version), false, t0, 0, &current);
            if (e.code) goto done;
        }
        jz_btree current_cells;
        current_row_cells(table, &current, &current_cells);
        RowProvenance provenance;
        e = provenance_or_unresolved(&current, &provenance);
        if (!e.code) {
            jazz_node_TransactionWriteOverlay committed = RS(TransactionWriteOverlay__committed_view)(transaction_overlay);
            e = QE_AUTHZ(write_policy_query_allows_candidate_over_transaction)(self, policy_schema_version, table, policy,
                                                                             jazz_node__node__CurrentRow__row_uuid(&current), &current_cells, author,
                                                                             false, provenance, &committed, out);
            jazz_node_TransactionWriteOverlay_drop(&committed);
        }
        jz_btree_drop(&current_cells);
        jazz_node__node__CurrentRow__drop(&current);
        goto done;
    }
    {
        bool has_previous = false;
        CurrentRow previous;
        e = policy_previous_content_subject_row(self, policy_schema_version, table, version, candidate_tx_id, &has_previous, &previous);
        if (e.code) goto done;
        if (has_previous) {
            jz_btree previous_cells;
            current_row_cells(table, &previous, &previous_cells);
            RowProvenance previous_provenance;
            e = provenance_or_unresolved(&previous, &previous_provenance);
            if (!e.code && table->write_policies.update_using) {
                bool ok = false;
                jazz_node_TransactionWriteOverlay committed = RS(TransactionWriteOverlay__committed_view)(transaction_overlay);
                e = QE_AUTHZ(write_policy_query_allows_candidate_over_transaction)(self, policy_schema_version, table, table->write_policies.update_using,
                                                                                 jazz_node__node__CurrentRow__row_uuid(&previous), &previous_cells, author,
                                                                                 false, previous_provenance, &committed, &ok);
                jazz_node_TransactionWriteOverlay_drop(&committed);
                if (!e.code && !ok) { *out = false; jz_btree_drop(&previous_cells); jazz_node__node__CurrentRow__drop(&previous); goto done; }
            }
            jazz_node__node__CurrentRow__drop(&previous);
            if (e.code) { jz_btree_drop(&previous_cells); goto done; }
            if (!table->write_policies.update_using && !table->write_policies.update_check) { jz_btree_drop(&previous_cells); *out = false; goto done; }
            const jazz_model_Query *policy = table->write_policies.update_check;
            if (!policy) { jz_btree_drop(&previous_cells); *out = true; goto done; }
            jz_btree effective_cells = previous_cells;
            jz_btree cells_copy;
            cells_clone(&cells_copy, &proj.cells);
            cells_extend(&effective_cells, &cells_copy);
            RowProvenance update_check_provenance = updated_provenance(previous_provenance, version);
            /* WITH CHECK judges the row the transaction leaves behind, so its
             * evidence includes the transaction's other writes (INV-RLS-9). */
            e = QE_AUTHZ(write_policy_query_allows_candidate_over_transaction)(self, policy_schema_version, table, policy, row_uuid, &effective_cells, author,
                                                                             false, update_check_provenance, transaction_overlay, out);
            jz_btree_drop(&effective_cells);
            goto done;
        }
    }
    {
        const jazz_model_Query *policy = table->write_policies.insert_check;
        if (!policy) { *out = false; goto done; }
        e = QE_AUTHZ(write_policy_query_allows_candidate_over_transaction)(self, policy_schema_version, table, policy, row_uuid, &proj.cells, author, true,
                                                                         version_provenance(version), transaction_overlay, out);
    }
done:
    policy_projection_drop(&proj);
    return e;
}

jz_err jazz_node__node__policy__NodeState__dry_run_insert_allows(NS *self, jazz_node_MergeableCommit commit, bool *out) {
    return jazz_node__node__policy__NodeState__dry_run_mergeable_write_allows_in_schema(self, self->catalogue.active_schema.schema, commit, out);
}

jz_err jazz_node__node__policy__NodeState__dry_run_mergeable_write_allows_in_schema(NS *self, jazz_types_SchemaVersionId write_schema_version,
                                                                                   jazz_node_MergeableCommit commit, bool *out) {
    jazz_types_AuthorSubject permission_subject;
    jz_err e = durable_author_policy_preview(self, &commit, &permission_subject);
    const jazz_model_TableSchema *table = NULL;
    if (!e.code) e = jazz_node__node__NodeState__table_in_schema_ref(self, &commit.table, write_schema_version, &table);
    VR *version = NULL;
    if (!e.code) e = jazz_node__node__codec__VersionRecordFromNode__from_commit(&commit, table, write_schema_version, &version);
    jazz_node__node__MergeableCommit__drop(&commit);
    if (e.code) return e;
    e = jazz_node__node__policy__NodeState__write_policy_allows_version_record(self, version, permission_subject, NULL, &EMPTY_VERSIONS, out);
    jazz_protocol__protocol__VersionRecord__drop(version);
    return e;
}

jz_err jazz_node__node__policy__NodeState__dry_run_read_current_allows(NS *self, jz_slice table_name, jazz_types_RowUuid row_uuid,
                                                                      jazz_types_AuthorSubject identity, bool *out) {
    return jazz_node__node__policy__NodeState__dry_run_read_current_allows_in_schema(self, table_name, row_uuid, self->catalogue.local_schema_version_id,
                                                                                    identity, false, out);
}

/* Evaluate a point-read policy in the schema that named the wire request.
 * Repair requests may use a projected table from a catalogue schema newer
 * than this node's base API schema. */
jz_err jazz_node__node__policy__NodeState__dry_run_read_current_allows_in_schema(NS *self, jz_slice table_name, jazz_types_RowUuid row_uuid,
                                                                                jazz_types_SchemaVersionId schema_version, jazz_types_AuthorSubject identity,
                                                                                bool include_deleted, bool *out) {
    const jazz_model_JazzSchema *schema = jazz_node__node__SchemaCatalogue__schema_for_version(&self->catalogue, schema_version);
    if (!schema) return JZN_INVALID_STORED("repair request schema is missing from catalogue");
    /* `id` resolves to a declared user column when a table has one, so an
     * internal physical-row probe must use the dedicated access-path API. */
    jazz_model_OptStr no_alias = { false, { 0 } };
    jazz_model_Query query = jazz_model__query__Query__from(jz_str_from_slice(table_name), no_alias);
    jazz_model_ValidatedQuery *shape = NULL;
    jz_err e = jazz_model__query__Query__validate_with_schema_version(&query, schema, schema_version, &shape);
    jazz_model__query__Query__drop_v(&query);
    if (e.code) return e;
    jz_btree params;
    jz_btree_init(&params, &jzn_desc_str_value);
    jazz_model_Binding *binding = NULL;
    e = jazz_model__query__ValidatedQuery__bind(shape, params, &binding);
    if (e.code) { jazz_model__query__ValidatedQuery__drop(shape); return e; }
    jazz_node_CurrentRowVec rows = { 0 };
    if (include_deleted) {
        /* Repair authorizes disclosure, not membership in the default
         * non-deleted query. The ordinary includeDeleted serving path still
         * enforces the current row policy against its content. */
        e = jazz_node__node__query_eval__NodeState__query_readable_current_row_including_deleted(self, shape, binding, JAZZ_DUR_LOCAL, identity, row_uuid, &rows);
    } else {
        /* Only row identity is inspected: keep large values physical. */
        e = jazz_node__node__query_eval__NodeState__query_row_visibility_for_link_physical_row(self, shape, binding, JAZZ_DUR_LOCAL, identity, row_uuid, &rows);
    }
    jazz_model__query__Binding__drop(binding);
    jazz_model__query__ValidatedQuery__drop(shape);
    if (e.code) return e;
    *out = false;
    for (u32 i = 0; i < rows.len; i++) {
        jazz_types_RowUuid r = jazz_node__node__CurrentRow__row_uuid(&rows.ptr[i]);
        if (jz_uuid_eq(&r, &row_uuid)) *out = true;
    }
    jz_vec_drop(&rows, sizeof(CurrentRow), jazz_node__node__CurrentRow__drop);
    return JZ_OK;
}

static jz_err dry_run_current_subject(NS *self, jz_slice table_name, jazz_types_RowUuid row_uuid, jazz_model_TableSchema *table, bool *has, CurrentRow *row) {
    jz_str name = jz_str_from_slice(table_name);
    const jazz_model_TableSchema *t = NULL;
    jz_err e = jazz_node__node__NodeState__table(self, &name, &t);
    jz_str_drop(&name);
    if (e.code) return e;
    jazz_model__schema__TableSchema__clone(table, t);
    e = policy_local_current_subject_row(self, table, row_uuid, has, row);
    if (e.code) jazz_model__schema__TableSchema__drop(table);
    return e;
}

jz_err jazz_node__node__policy__NodeState__dry_run_delete_current_allows(NS *self, jz_slice table_name, jazz_types_RowUuid row_uuid,
                                                                        jazz_types_AuthorSubject author, bool *out) {
    if (JZN_AUTHOR_IS_SYSTEM(author)) { *out = true; return JZ_OK; }
    jazz_model_TableSchema table;
    bool has = false;
    CurrentRow row;
    JZ_TRY(dry_run_current_subject(self, table_name, row_uuid, &table, &has, &row));
    jz_err e = JZ_OK;
    *out = false;
    if (has) {
        if (table.write_policies.delete_using)
            e = QE_AUTHZ(write_policy_query_allows_current_row)(self, table.write_policies.delete_using, jazz_node__node__CurrentRow__row_uuid(&row), author, out);
        jazz_node__node__CurrentRow__drop(&row);
    }
    jazz_model__schema__TableSchema__drop(&table);
    return e;
}

/* Whether `row_uuid` is a live local row of `table`, by point lookup of its
 * content and deletion winners (the lookup delete advice uses). */
jz_err jazz_node__node__policy__NodeState__local_current_row_exists(NS *self, jz_slice table_name, jazz_types_RowUuid row_uuid, bool *out) {
    jazz_model_TableSchema table;
    CurrentRow row;
    JZ_TRY(dry_run_current_subject(self, table_name, row_uuid, &table, out, &row));
    if (*out) jazz_node__node__CurrentRow__drop(&row);
    jazz_model__schema__TableSchema__drop(&table);
    return JZ_OK;
}

static bool version_row_is_deleted(const jazz_node_VersionRow *v) {
    jazz_model_DeletionEvent d;
    return jazz_node__node__codec__VersionRow__deletion(v, &d) && d == JAZZ_DEL_DELETED;
}

static jz_err policy_local_current_subject_row(NS *self, const jazz_model_TableSchema *table, jazz_types_RowUuid row_uuid, bool *has, CurrentRow *out) {
    *has = false;
    jazz_node_VersionRow *version = NULL;
    JZ_TRY(jazz_node__node__currency__NodeState__query_local_winner(self, &table->name, row_uuid, &version));
    if (!version) return JZ_OK;
    if (version_row_is_deleted(version)) { jazz_node__node__codec__VersionRow__drop(version); return JZ_OK; }
    policy_projection proj;
    jz_err e = policy_projection_for_version_row(self, version, &proj);
    if (!e.code) {
        if (jz_str_eq(&proj.table.name, &table->name)) {
            e = reconstructed_policy_subject_row(table, row_uuid, &proj.cells, version, out);
            if (!e.code) *has = true;
        }
        policy_projection_drop(&proj);
    }
    jazz_node__node__codec__VersionRow__drop(version);
    return e;
}

static jz_err policy_projection_for_version_row(NS *self, const jazz_node_VersionRow *version, policy_projection *out) {
    jazz_types_SchemaVersionId source_schema;
    if (!jazz_node__node__NodeState__schema_version_for_alias(self, jazz_node__node__codec__VersionRow__schema_version_alias(version), &source_schema))
        return JZN_INVALID_STORED("history schema version alias must exist");
    const jazz_model_TableSchema *source_table = NULL;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, jazz_node__node__codec__VersionRow__table(version), source_schema, &source_table));
    jz_btree cells;
    JZ_TRY(jazz_node__node__codec__VersionRow__cells(version, source_table, &cells));
    return translate_policy_cells(self, source_schema, jazz_node__node__codec__VersionRow__table(version), cells, out);
}

static jz_err policy_projection_for_version_record(NS *self, const VR *version, policy_projection *out) {
    jazz_types_SchemaVersionId source_schema = jazz_protocol__protocol__VersionRecord__schema_version(version);
    const jz_str *vtable = jazz_protocol__protocol__VersionRecord__table(version);
    const jazz_model_TableSchema *source_table = NULL;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, vtable, source_schema, &source_table));
    jz_btree cells;
    jz_btree_init(&cells, &jzn_desc_str_value);
    for (u32 idx = 0; idx < source_table->columns.len; idx++) {
        groove_Value value;
        if (jazz_protocol__protocol__VersionRecord__optional_cell_at(version, idx, &value)) {
            jz_str name = jz_str_clone(&source_table->columns.ptr[idx].name);
            groove_Value old;
            if (jz_btree_insert(&cells, &name, &value, &old)) { jz_str_drop(&name); groove__records__values__Value__drop(&old); }
        }
    }
    return translate_policy_cells(self, source_schema, vtable, cells, out);
}

/* Takes ownership of `cells`. */
static jz_err translate_policy_cells(NS *self, jazz_types_SchemaVersionId source, const jz_str *table, jz_btree cells, policy_projection *out) {
    /* Resolve the schema that owns the policy bundle, then project data
     * (including table identity) into it. The bundle itself stays unchanged. */
    jazz_types_SchemaVersionId target;
    jz_err e = policy_target_schema_for_source(self, source, table, &target);
    if (e.code) { jz_btree_drop(&cells); return e; }
    const jazz_model_TableSchema *t = NULL;
    if (jz_uuid_eq(&source, &target)) {
        e = jazz_node__node__NodeState__table_in_schema_ref(self, table, target, &t);
        if (e.code) { jz_btree_drop(&cells); return e; }
        out->schema = target;
        jazz_model__schema__TableSchema__clone(&out->table, t);
        out->cells = cells;
        return JZ_OK;
    }

    bool has_path = false;
    jazz_node_CompiledLensPath path;
    e = jazz_node__node__NodeState__compiled_lens_path(self, source, target, table, &has_path, &path);
    if (e.code) { jz_btree_drop(&cells); return e; }
    if (has_path) {
        jz_str translated_table;
        jazz_node__node__apply_compiled_lens_path(&path, &cells, &translated_table);
        jazz_node__node__CompiledLensPath__drop(&path);
        e = jazz_node__node__NodeState__table_in_schema_ref(self, &translated_table, target, &t);
        jz_str_drop(&translated_table);
        if (e.code) { jz_btree_drop(&cells); return e; }
        out->schema = target;
        jazz_model__schema__TableSchema__clone(&out->table, t);
        out->cells = cells;
        return JZ_OK;
    }

    const jazz_model_TableSchema *target_table = NULL, *source_table = NULL;
    e = jazz_node__node__NodeState__table_in_schema_ref(self, table, target, &target_table);
    if (!e.code) e = jazz_node__node__NodeState__table_in_schema_ref(self, table, source, &source_table);
    if (e.code) { jz_btree_drop(&cells); return e; }
    if (policy_tables_are_directly_compatible(source_table, target_table)) {
        out->schema = target;
        jazz_model__schema__TableSchema__clone(&out->table, target_table);
        out->cells = cells;
        return JZ_OK;
    }
    jz_btree_drop(&cells);
    return JZN_INVALID_CATALOGUE("lens chain is unknown");
}

static bool table_in_schema_ok(const NS *self, const jz_str *table, jazz_types_SchemaVersionId schema) {
    const jazz_model_TableSchema *t = NULL;
    jz_err e = jazz_node__node__NodeState__table_in_schema_ref(self, table, schema, &t);
    if (e.code) { jz_err_drop(&e); return false; }
    return true;
}

static bool policy_schema_resolves_query_sources(const NS *self, jazz_types_SchemaVersionId schema, const jazz_engine_NormalizedRowSetShape *shape) {
    void *node_id; const jazz_engine_RowSetExpr *node;
    JZ_BTREE_FOR(jazz_engine__query_engine__input__NormalizedRowSetShape__nodes(shape), node_id, node) {
        (void)node_id;
        if (node->tag == JAZZ_ENGINE_EXPR_SOURCE && !table_in_schema_ok(self, &node->u.source.source.table, schema)) return false;
    }
    const jazz_engine_SourceId *source; void *u;
    JZ_BTREE_FOR(jazz_engine__query_engine__input__NormalizedRowSetShape__auxiliary_sources(shape), source, u) {
        (void)u;
        if (!table_in_schema_ok(self, &source->table, schema)) return false;
    }
    return true;
}

jazz_types_SchemaVersionId jazz_node__node__policy__NodeState__read_policy_schema_for_table_name(const NS *self, const jz_str *table,
                                                                                                 jazz_types_SchemaVersionId query_schema,
                                                                                                 const jazz_engine_NormalizedRowSetShape *shape) {
    jazz_types_SchemaVersionId write_schema = self->catalogue.active_schema.schema;
    jazz_types_SchemaVersionId current_schema = self->catalogue.local_schema_version_id;
    if (table_in_schema_ok(self, table, write_schema) && policy_schema_resolves_query_sources(self, write_schema, shape))
        return write_schema;
    if (policy_schema_resolves_query_sources(self, current_schema, shape))
        /* Preserve the pinned current policy schema unless it predates a table
         * rename and cannot resolve every queried source. */
        return current_schema;
    return query_schema;
}

static jz_err policy_target_schema_for_source(NS *self, jazz_types_SchemaVersionId source, const jz_str *table, jazz_types_SchemaVersionId *out) {
    jazz_types_SchemaVersionId write_schema = self->catalogue.active_schema.schema;
    bool reaches = false;
    JZ_TRY(source_reaches_write_policy_table(self, source, write_schema, table, &reaches));
    if (reaches) { *out = write_schema; return JZ_OK; }
    JZ_TRY(source_reaches_write_policy_table(self, source, self->catalogue.local_schema_version_id, table, &reaches));
    if (reaches || table_in_schema_ok(self, table, self->catalogue.local_schema_version_id)) *out = self->catalogue.local_schema_version_id;
    else *out = source;
    return JZ_OK;
}

static jz_err source_reaches_write_policy_table(NS *self, jazz_types_SchemaVersionId source, jazz_types_SchemaVersionId target, const jz_str *table, bool *out) {
    if (jz_uuid_eq(&source, &target)) { *out = table_in_schema_ok(self, table, target); return JZ_OK; }
    bool has_path = false;
    jazz_node_CompiledLensPath path;
    JZ_TRY(jazz_node__node__NodeState__compiled_lens_path(self, source, target, table, &has_path, &path));
    if (has_path) {
        jz_btree cells;
        jz_btree_init(&cells, &jzn_desc_str_value);
        jz_str target_table;
        jazz_node__node__apply_compiled_lens_path(&path, &cells, &target_table);
        jazz_node__node__CompiledLensPath__drop(&path);
        jz_btree_drop(&cells);
        *out = table_in_schema_ok(self, &target_table, target);
        jz_str_drop(&target_table);
        return JZ_OK;
    }
    *out = table_in_schema_ok(self, table, target);
    return JZ_OK;
}

static jz_err policy_delete_subject_row(NS *self, jazz_types_SchemaVersionId policy_schema_version, const jazz_model_TableSchema *table, const VR *version,
                                        const jazz_model_TxId *candidate_tx_id, const jazz_protocol_VersionRecordVec *candidate_versions, bool *has, CurrentRow *out) {
    JZ_TRY(policy_previous_content_subject_row(self, policy_schema_version, table, version, candidate_tx_id, has, out));
    if (*has) return JZ_OK;
    /* An insert followed by a delete in one transaction has no prior content
     * row. Admission runs before persistence, so inspect the same incoming
     * unit for its content and real creation provenance. */
    jazz_types_RowUuid row_uuid = jazz_protocol__protocol__VersionRecord__row_uuid(version);
    for (u32 i = 0; i < candidate_versions->len; i++) {
        const VR *candidate = candidate_versions->ptr[i];
        jazz_types_RowUuid cr = jazz_protocol__protocol__VersionRecord__row_uuid(candidate);
        if (!jz_uuid_eq(&cr, &row_uuid) || jazz_protocol__protocol__VersionRecord__deletes_row(candidate) ||
            !jazz_model__model__branch__BranchKey__eq(jazz_protocol__protocol__VersionRecord__branch_key(candidate),
                                                     jazz_protocol__protocol__VersionRecord__branch_key(version)))
            continue;
        policy_projection proj;
        JZ_TRY(policy_projection_for_version_record(self, candidate, &proj));
        jz_err e = JZ_OK;
        bool same = jz_str_eq(&proj.table.name, &table->name);
        if (same) {
            e = jazz_node__node__codec__current_row_from_cells_with_explicit_provenance(table, row_uuid, &proj.cells, version_provenance(candidate),
                                                                                    false, (jazz_types_TxTime){ 0 }, 0, out);
            if (!e.code) *has = true;
        }
        policy_projection_drop(&proj);
        if (same || e.code) return e;
    }
    return JZ_OK;
}

static jz_err policy_previous_content_subject_row(NS *self, jazz_types_SchemaVersionId policy_schema_version, const jazz_model_TableSchema *table,
                                                  const VR *version, const jazz_model_TxId *candidate_tx_id, bool *has, CurrentRow *out) {
    (void)policy_schema_version;
    *has = false;
    const jz_str *vtable = jazz_protocol__protocol__VersionRecord__table(version);
    const jz_str *subject_table = table_in_schema_ok(self, vtable, self->catalogue.active_schema.schema) ? vtable : &table->name;
    const jazz_model_BranchKey *branch_key = jazz_protocol__protocol__VersionRecord__branch_key(version);
    jazz_types_RowUuid row_uuid = jazz_protocol__protocol__VersionRecord__row_uuid(version);
    jazz_node_VersionRow *local_previous = NULL;
    if (candidate_tx_id)
        JZ_TRY(jazz_node__node__currency__NodeState__query_local_winner_in_branch_excluding_tx(self, subject_table, branch_key, row_uuid, *candidate_tx_id, &local_previous));
    else
        JZ_TRY(jazz_node__node__currency__NodeState__query_local_winner_in_branch(self, subject_table, branch_key, row_uuid, &local_previous));
    jz_err e = JZ_OK;
    if (local_previous) {
        policy_projection proj;
        e = policy_projection_for_version_row(self, local_previous, &proj);
        if (!e.code) {
            if (jz_str_eq(&proj.table.name, &table->name)) {
                e = reconstructed_policy_subject_row(table, row_uuid, &proj.cells, local_previous, out);
                if (!e.code) *has = true;
            }
            policy_projection_drop(&proj);
        }
        jazz_node__node__codec__VersionRow__drop(local_previous);
        if (e.code || *has) return e;
    }

    jazz_node_VersionRow *current_version = NULL;
    JZ_TRY(jazz_node__node__currency__NodeState__query_global_winner_in_branch(self, subject_table, branch_key, row_uuid, &current_version));
    if (current_version) {
        jazz_model_TxId tx;
        e = jazz_node__node__NodeState__version_tx_id(self, current_version, &tx);
        if (!e.code && !(candidate_tx_id && jazz_model_TxId_eq(candidate_tx_id, &tx))) {
            policy_projection proj;
            e = policy_projection_for_version_row(self, current_version, &proj);
            if (!e.code) {
                if (jz_str_eq(&proj.table.name, &table->name)) {
                    e = reconstructed_policy_subject_row(table, row_uuid, &proj.cells, current_version, out);
                    if (!e.code) *has = true;
                }
                policy_projection_drop(&proj);
            }
        }
        jazz_node__node__codec__VersionRow__drop(current_version);
    }
    return e;
}

jz_err jazz_node__node__policy__NodeState__query_transaction_memo(NS *self, jazz_model_TxId tx_id, jazz_node_ViewEvaluationContext *context,
                                                                 bool *found, jazz_node_StoredTransaction *out) {
    if (!jz_btree_contains(&context->tx_rows, &tx_id)) {
        jazz_node_OptStoredTransaction entry = { false };
        JZ_TRY(jazz_node__node__currency__NodeState__query_transaction(self, tx_id, &entry.some, &entry.tx));
        jz_btree_insert(&context->tx_rows, &tx_id, &entry, NULL);
    }
    const jazz_node_OptStoredTransaction *memo = jz_btree_get(&context->tx_rows, &tx_id);
    if (!memo) jz_panic("tx row memo populated");
    *found = memo->some;
    if (memo->some) jazz_node__node__codec__StoredTransaction__clone(out, &memo->tx);
    return JZ_OK;
}

static bool policy_tables_are_directly_compatible(const jazz_model_TableSchema *source, const jazz_model_TableSchema *target) {
    if (!jz_str_eq(&source->name, &target->name) || source->columns.len != target->columns.len) return false;
    for (u32 i = 0; i < source->columns.len; i++) {
        const jazz_model_ColumnSchema *s = &source->columns.ptr[i], *t = &target->columns.ptr[i];
        if (!jz_str_eq(&s->name, &t->name) || !groove__records__values__ValueType__eq(&s->column_type, &t->column_type)) return false;
    }
    return true;
}
