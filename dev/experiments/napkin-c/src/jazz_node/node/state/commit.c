/* Port of crates/jazz/layers/node/src/node/state/commit.rs
 * (include!d into node/mod.rs, so the Rust module is jazz_node::node).
 * Async fns are synchronous here. Generic `PublicationOutcome<T>` carries an
 * erased `void *value`.
 * Skipped: rebuild_ahead_current_keys_with_receipt and the receipt counter
 * inside rebuild_ahead_current_keys_inner (#[cfg(feature = "testing")]). */
#include "jz_rt.h"
#include "../../_ext_node-qe-b.h"
#include "../node_qe_b.h"

#define NS jazz_node_NodeState

static void str_kclone(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone(src); }
static void value_vclone(void *dst, const void *src) { groove_Value_clone(dst, src); }

static void schema_commit_vec_drop(jazz_node_SchemaCommitVec *v)
{
    for (u32 i = 0; i < v->len; ++i) jazz_node__node__MergeableCommit__drop(&v->ptr[i].commit);
    jz_free(v->ptr); v->ptr = NULL; v->len = v->cap = 0;
}

static jazz_node_SchemaCommitVec with_schema(jazz_node_MergeableCommitVec commits, jazz_types_SchemaVersionId schema)
{
    jazz_node_SchemaCommitVec out = {0};
    jz_vec_reserve(&out, commits.len, sizeof(jazz_node_SchemaCommit));
    JZ_VEC_FOR(jazz_node_MergeableCommit, c, commits) JZ_VEC_PUSH(out, ((jazz_node_SchemaCommit){ schema, *c }));
    jz_free(commits.ptr);
    return out;
}

static void version_row_vec_clone(jazz_node_VersionRowVec *dst, const jazz_node_VersionRowVec *src)
{
    *dst = (jazz_node_VersionRowVec){0};
    jz_vec_reserve(dst, src->len, sizeof(jazz_node_VersionRow));
    JZ_VEC_FOR(const jazz_node_VersionRow, v, *src) { jazz_node_VersionRow c; jazz_node_VersionRow_clone(&c, v); JZ_VEC_PUSH(*dst, c); }
}

static void version_row_vec_drop(jazz_node_VersionRowVec *v)
{
    for (u32 i = 0; i < v->len; ++i) jazz_node_VersionRow_drop(&v->ptr[i]);
    jz_free(v->ptr); v->ptr = NULL; v->len = v->cap = 0;
}

static void string_keys_into_set(const jz_btree *map, jz_btree *set)
{
    jz_btree_init(set, &jazz_node_STRING_SET_DESC);
    const jz_str *k; const void *v;
    JZ_BTREE_FOR(map, k, v) { jz_str c = jz_str_clone(k); jz_btree_insert(set, &c, NULL, NULL); }
}

/* The shared validation loop of the commit_mergeable_many* entry points. */
static jz_err validate_mergeable_commits(const jazz_node_MergeableCommit *commits, u32 n, usize stride)
{
    if (n == 0) return JZN_INVALID_MERGEABLE("mergeable transaction requires at least one write");
    const jazz_node_MergeableCommit *first = commits;
    jazz_types_AuthorSubject first_subject = jazz_node__node__MergeableCommit__effective_permission_subject(first);
    for (u32 i = 0; i < n; ++i) {
        const jazz_node_MergeableCommit *commit = (const jazz_node_MergeableCommit *)((const u8 *)commits + i * stride);
        JZ_TRY(jazz_node__node__MergeableCommit__validate(commit));
        if (!jazz_types_AuthorSubject_eq(jazz_node__node__MergeableCommit__effective_permission_subject(commit), first_subject))
            return JZN_INVALID_MERGEABLE("mergeable transaction permission subjects must match");
    }
    return JZ_OK;
}

static jazz_node_SchemaCommitVec coalesce_same_row_commits(jazz_node_SchemaCommitVec commits);

/* Commit a local mergeable write and leave its fate pending. */
jz_err jazz_node__node__NodeState__commit_mergeable(NS *self, jazz_node_MergeableCommit commit, jazz_node_PublishedTransaction *out)
{
    jz_err e = jazz_node__node__MergeableCommit__validate(&commit);
    jazz_types_TxTime made_at;
    if (!e.code) e = jazz_node__node__NodeState__mint_tx_time(self, commit.now_ms, &made_at);
    if (e.code) { jazz_node__node__MergeableCommit__drop(&commit); return e; }
    return jazz_node__node__NodeState__commit_mergeable_at(self, commit, made_at, out);
}

/* Commit one local mergeable write under an admitted authored schema.
 * Client database handles retain the schema they were opened with even when
 * an authority later advances its separate current-write pointer. */
jz_err jazz_node__node__NodeState__commit_mergeable_in_schema(NS *self, jazz_types_SchemaVersionId schema_version,
    jazz_node_MergeableCommit commit, jazz_node_PublishedTransaction *out)
{
    jazz_node_MergeableCommitVec commits = {0};
    JZ_VEC_PUSH(commits, commit);
    return jazz_node__node__NodeState__commit_mergeable_many_in_schema(self, schema_version, commits, out);
}

/* Commit multiple local mergeable writes as one transaction. */
jz_err jazz_node__node__NodeState__commit_mergeable_many(NS *self, jazz_node_MergeableCommitVec commits, jazz_node_PublishedTransaction *out)
{
    jz_err e = validate_mergeable_commits(commits.ptr, commits.len, sizeof(jazz_node_MergeableCommit));
    jazz_types_TxTime made_at;
    if (!e.code) e = jazz_node__node__NodeState__mint_tx_time(self, commits.ptr[0].now_ms, &made_at);
    if (e.code) {
        JZ_VEC_FOR(jazz_node_MergeableCommit, c, commits) jazz_node__node__MergeableCommit__drop(c);
        jz_free(commits.ptr);
        return e;
    }
    return jazz_node__node__NodeState__commit_mergeable_many_at(self, commits, made_at, out);
}

static jz_err drop_commits_err(jazz_node_MergeableCommitVec *commits, jz_err e)
{
    JZ_VEC_FOR(jazz_node_MergeableCommit, c, *commits) jazz_node__node__MergeableCommit__drop(c);
    jz_free(commits->ptr);
    return e;
}

/* Commit local mergeable writes under one admitted authored schema. */
jz_err jazz_node__node__NodeState__commit_mergeable_many_in_schema(NS *self, jazz_types_SchemaVersionId schema_version,
    jazz_node_MergeableCommitVec commits, jazz_node_PublishedTransaction *out)
{
    jz_err e = jazz_node__node__NodeState__require_catalogue_ready(self);
    if (e.code) return drop_commits_err(&commits, e);
    if (!jz_btree_contains(&self->catalogue.catalogue_schemas, &schema_version))
        return drop_commits_err(&commits, JZN_INVALID_MERGEABLE("authored schema version is not admitted"));
    e = validate_mergeable_commits(commits.ptr, commits.len, sizeof(jazz_node_MergeableCommit));
    if (e.code) return drop_commits_err(&commits, e);
    jazz_types_TxTime made_at;
    e = jazz_node__node__NodeState__mint_tx_time(self, commits.ptr[0].now_ms, &made_at);
    if (e.code) return drop_commits_err(&commits, e);
    return jazz_node__node__NodeState__commit_mergeable_many_at_with_schema_versions(self, with_schema(commits, schema_version), made_at, out);
}

/* Commit prepared local writes under a transaction identity reserved by the
 * host before asynchronous preparation began. */
jz_err jazz_node__node__NodeState__commit_mergeable_many_in_schema_at(NS *self, jazz_types_SchemaVersionId schema_version,
    jazz_node_MergeableCommitVec commits, jazz_model_TxId reserved, jazz_node_PublishedTransaction *out)
{
    if (!jz_uuid_eq(&reserved.node, &self->node_uuid))
        return drop_commits_err(&commits, JZN_INVALID_MERGEABLE("reserved transaction belongs to another node"));
    jz_err e = jazz_node__node__NodeState__require_catalogue_ready(self);
    if (e.code) return drop_commits_err(&commits, e);
    if (!jz_btree_contains(&self->catalogue.catalogue_schemas, &schema_version))
        return drop_commits_err(&commits, JZN_INVALID_MERGEABLE("authored schema version is not admitted"));
    e = validate_mergeable_commits(commits.ptr, commits.len, sizeof(jazz_node_MergeableCommit));
    if (e.code) return drop_commits_err(&commits, e);
    jazz_node__node__NodeState__merge_tx_time(self, reserved.time);
    return jazz_node__node__NodeState__commit_mergeable_many_at_with_schema_versions(self, with_schema(commits, schema_version), reserved.time, out);
}

jz_err jazz_node__node__NodeState__commit_mergeable_at(NS *self, jazz_node_MergeableCommit commit, jazz_types_TxTime made_at, jazz_node_PublishedTransaction *out)
{
    jazz_node_MergeableCommitVec commits = {0};
    JZ_VEC_PUSH(commits, commit);
    return jazz_node__node__NodeState__commit_mergeable_many_at(self, commits, made_at, out);
}

jz_err jazz_node__node__NodeState__commit_mergeable_many_at(NS *self, jazz_node_MergeableCommitVec commits, jazz_types_TxTime made_at, jazz_node_PublishedTransaction *out)
{
    jz_err e = jazz_node__node__NodeState__require_catalogue_ready(self);
    if (e.code) return drop_commits_err(&commits, e);
    jazz_types_SchemaVersionId write_schema_version = self->catalogue.active_schema.schema;
    return jazz_node__node__NodeState__commit_mergeable_many_at_with_schema_versions(self, with_schema(commits, write_schema_version), made_at, out);
}

jz_err jazz_node__node__NodeState__commit_mergeable_many_at_with_schema_versions(NS *self, jazz_node_SchemaCommitVec commits,
    jazz_types_TxTime made_at, jazz_node_PublishedTransaction *out)
{
    return jazz_node__node__NodeState__commit_mergeable_many_at_with_schema_versions_and_provenance(self, commits, made_at, NULL, out);
}

/* Captures of the Rust `row_version` closure. */
typedef struct RowVersionCtx {
    const jazz_model_TableSchema *table_schema;
    const jz_str *table;
    const jazz_model_BranchKey *branch_key;
    jazz_types_RowUuid row_uuid;
    jazz_types_NodeAlias tx_node_alias;
    jazz_types_SchemaVersionAlias schema_version_alias;
    jazz_types_TxTime made_at;
    jazz_types_AuthorSubject created_by;
    jazz_types_TxTime created_at;
    jazz_types_AuthorSubject updated_by;
    jazz_types_TxTime provenance_at;
    bool has_authored_column_ids; const jz_btree *authored_column_ids;
    bool has_deletion; jazz_model_DeletionEvent deletion;
    jazz_protocol_RowBase base;
    bool foreign_schema; jazz_types_SchemaVersionId write_schema_version;
    const groove_RecordDescriptor *history_descriptor;
} RowVersionCtx;

static void u64_kclone(void *dst, const void *src) { memcpy(dst, src, sizeof(u64)); }

static jz_err row_version(const RowVersionCtx *c, jz_btree cells, jz_bytes counter_signs, jazz_node_VersionRow *out)
{
    jazz_node_VersionRowParts parts = {
        .table = jz_str_clone(c->table),
        .row_uuid = c->row_uuid,
        .tx_node_alias = c->tx_node_alias,
        .schema_version_alias = c->schema_version_alias,
        .tx_time = c->made_at,
        .created_by = c->created_by,
        .created_at = c->created_at,
        .updated_by = c->updated_by,
        .updated_at = c->provenance_at,
        .cells = cells,
        .has_deletion = c->has_deletion, .deletion = c->deletion,
        /* A pending write has no seq yet. */
        .seq = 0,
        .base = c->base,
        .counter_signs = counter_signs,
    };
    jazz_model_BranchKey_clone(&parts.branch_key, c->branch_key);
    parts.has_authored_columns = c->has_authored_column_ids;
    if (c->has_authored_column_ids) jz_btree_clone(&parts.authored_columns, c->authored_column_ids, u64_kclone, NULL);
    return jazz_node__node__codec__VersionRow__from_parts_with_schema_version(c->table_schema, parts,
        c->foreign_schema, c->write_schema_version, c->history_descriptor, out);
}

static bool intent_matches(const jazz_model_BranchWriteIntentVec *intents, jazz_types_PhysicalTableId table_id,
    jazz_types_SchemaVersionId schema_version, jazz_types_RowUuid row_uuid, const jazz_model_BranchKey *head)
{
    JZ_VEC_FOR(const jazz_model_BranchWriteIntent, intent, *intents)
        if (intent->physical_table_id == table_id && jz_uuid_eq(&intent->authored_schema, &schema_version)
            && jz_uuid_eq(&intent->row_uuid, &row_uuid) && jazz_model_BranchKey_eq(&intent->head, head)) return true;
    return false;
}

static const jazz_model_ColumnSchema *find_column(const jazz_model_TableSchema *t, const jz_str *name)
{
    JZ_VEC_FOR(const jazz_model_ColumnSchema, column, t->columns) if (jz_str_eq(&column->name, name)) return column;
    return NULL;
}

jz_err jazz_node__node__NodeState__commit_mergeable_many_at_with_schema_versions_and_provenance(NS *self, jazz_node_SchemaCommitVec commits,
    jazz_types_TxTime made_at, jazz_model_ContributionMergeProvenance *contribution_merge_in /* NULL = None, moved */, jazz_node_PublishedTransaction *out)
{
    jz_err e = JZ_OK;
    /* `*_at` is also used by trusted internal paths, so do not rely on the
     * public wrapper to validate a public provenance millisecond before any
     * staging or batch mutation begins. */
    JZ_VEC_FOR(const jazz_node_SchemaCommit, sc, commits)
        if ((e = jazz_node__node__MergeableCommit__validate(&sc->commit)).code) { schema_commit_vec_drop(&commits); return e; }
    /* One row has one image per transaction: a restore with replacement
     * content arrives as a content commit plus a `Restored` event. */
    commits = coalesce_same_row_commits(commits);
    /* Authorization is evaluated as the session/capability selected before
     * durable attribution is normalized. */
    jazz_types_AuthorSubject permission_subject = jazz_node__node__MergeableCommit__effective_permission_subject(&commits.ptr[0].commit);
    /* Persist a node-attributed system identity and reject an unadmitted
     * session rather than granting it synthetic ownership. */
    JZ_VEC_FOR(jazz_node_SchemaCommit, sc, commits) {
        jazz_types_RowAuthor author;
        jz_err ae = jazz_types__ids__RowAuthor__from_session(sc->commit.made_by, self->node_uuid, &author);
        if (ae.code) { jz_err_drop(&ae); schema_commit_vec_drop(&commits); return JZN_UNADMITTED_WRITE_AUTHOR(); }
        sc->commit.made_by = jazz_types__ids__RowAuthor__as_author_subject(&author);
    }
    /* Bindings send a present optional-JSON value wrapped as nullable. Lower
     * it to the bare stored scalar before anything validates or stores it. */
    JZ_VEC_FOR(jazz_node_SchemaCommit, sc, commits) {
        const jazz_model_TableSchema *table;
        if ((e = jazz_node__node__NodeState__table_in_schema_ref(self, &sc->commit.table, sc->schema, &table)).code) { schema_commit_vec_drop(&commits); return e; }
        const jz_str *name; groove_Value *value;
        JZ_BTREE_FOR(&sc->commit.cells, name, value) {
            const jazz_model_ColumnSchema *column = find_column(table, name);
            if (column) *value = jazz_model__schema__ColumnSchema__storage_value(column, *value);
        }
    }
    /* The lowest common local commit construction boundary: fill only exact
     * branch operations not already classified by a higher view-copy path. */
    jazz_model_ContributionMergeProvenance contribution_merge = {0};
    if (contribution_merge_in) contribution_merge = *contribution_merge_in;
    else { jazz_model_BranchKey_default(&contribution_merge.source); jazz_model_BranchKey_default(&contribution_merge.target); }
    JZ_VEC_FOR(const jazz_node_SchemaCommit, sc, commits) {
        const jazz_node_MergeableCommit *commit = &sc->commit;
        if (commit->branch.values.len == 0) continue;
        const jazz_protocol_SchemaVersion *sv = jz_btree_get(&self->catalogue.catalogue_schemas, &sc->schema);
        if (!sv) { e = JZN_INVALID_STORED("write schema is missing"); goto fail_cm; }
        const jazz_model_TableSchema *table;
        if ((e = jazz_node__node__NodeState__table_in_schema_ref(self, &commit->table, sc->schema, &table)).code) goto fail_cm;
        /* Project only through the target table. */
        jazz_model_BranchKey head; jz_btree head_cells;
        if ((e = jazz_model__schema__JazzSchema__project_branch_selector(&sv->schema, table, &commit->branch, &head, &head_cells)).code) {
            e = jazz_node__node__Error_invalid_branch_key(e); goto fail_cm;
        }
        jz_btree_drop(&head_cells);
        if ((e = jazz_model__schema__JazzSchema__validate_authored_branch_key(&sv->schema, table, &head)).code) {
            jazz_model_BranchKey_drop(&head); e = jazz_node__node__Error_invalid_branch_key(e); goto fail_cm;
        }
        jazz_types_PhysicalTableId table_id;
        if ((e = jazz_node__node__physical__NodeState__physical_table_id_for_schema(self, sc->schema, &commit->table, &table_id)).code) { jazz_model_BranchKey_drop(&head); goto fail_cm; }
        if (intent_matches(&contribution_merge.branch_write_intents, table_id, sc->schema, commit->row_uuid, &head)) { jazz_model_BranchKey_drop(&head); continue; }
        jazz_node_VersionRow *found = NULL;
        if ((e = jazz_node__node__currency__NodeState__query_local_winner_in_branch(self, &commit->table, &head, commit->row_uuid, &found)).code) { jazz_model_BranchKey_drop(&head); goto fail_cm; }
        bool exists = found != NULL;
        if (found) jazz_node__node__codec__VersionRow__free(found);
        if (!exists) {
            if ((e = jazz_node__node__currency__NodeState__query_global_winner_in_branch(self, &commit->table, &head, commit->row_uuid, &found)).code) { jazz_model_BranchKey_drop(&head); goto fail_cm; }
            exists = found != NULL;
            if (found) jazz_node__node__codec__VersionRow__free(found);
        }
        jazz_model_BranchWriteIntent intent = {
            .version = 1, .physical_table_id = table_id, .authored_schema = sc->schema, .row_uuid = commit->row_uuid, .head = head,
            .operation = { .tag = exists ? JZ_BWO_EXACT_HEAD_UPDATE : JZ_BWO_EXACT_HEAD_INSERT },
        };
        JZ_VEC_PUSH(contribution_merge.branch_write_intents, intent);
    }
    jz_sort(contribution_merge.branch_write_intents.ptr, contribution_merge.branch_write_intents.len,
        sizeof(jazz_model_BranchWriteIntent), jazz_model_BranchWriteIntent_sort_cmp, NULL);
    bool has_contribution_merge = !(contribution_merge.substitutions.len == 0 && contribution_merge.branch_view_copies.len == 0
        && contribution_merge.branch_write_intents.len == 0);
    if (!has_contribution_merge) jazz_model_ContributionMergeProvenance_drop(&contribution_merge);

    jazz_model_TxId tx_id = { made_at, self->node_uuid };
    jazz_types_AuthorSubject made_by = commits.ptr[0].commit.made_by;
    if ((u64)commits.len > 0xffffffffull) {
        if (has_contribution_merge) jazz_model_ContributionMergeProvenance_drop(&contribution_merge);
        schema_commit_vec_drop(&commits);
        return JZN_INVALID_MERGEABLE("transaction write count exceeds u32");
    }
    jazz_model_Transaction tx = {
        .tx_id = tx_id, .kind = JZ_TXKIND_MERGEABLE, .n_total_writes = commits.len, .made_by = made_by,
        .has_permission_subject = true, .permission_subject = permission_subject,
        .has_base_snapshot = false, .row_read_set = NULL, .absent_read_set = NULL, .predicate_read_set = NULL,
        .has_contribution_merge = has_contribution_merge, .contribution_merge = contribution_merge,
    };
    if (commits.ptr[0].commit.has_user_metadata_json) {
        tx.has_user_metadata_json = true;
        tx.user_metadata_json = jz_str_clone(&commits.ptr[0].commit.user_metadata_json);
    }
    groove_Value contribution_merge_value;
    jz_btree staged_ids; jz_btree_init(&staged_ids, &jazz_node_U64_SET_DESC);
    groove_DatabaseBatch *batch = NULL;
    jazz_node_VersionRowVec stored_versions = {0};
    if ((e = jazz_node__node__NodeState__admit_contribution_merge_for_storage(self, &tx, &contribution_merge_value)).code) goto fail_tx;
    if ((e = jazz_node__node__NodeState__prepare_and_stage_large_commit_values(self, &commits)).code) goto fail_tx;
    JZ_VEC_FOR(const jazz_node_SchemaCommit, sc, commits)
        JZ_VEC_FOR(const groove_StagedLargeValueId, id, sc->commit.staged_large_values) jz_btree_insert(&staged_ids, id, NULL, NULL);
    if ((e = jazz_node__node__NodeState__ensure_large_value_stages_current(self, &staged_ids)).code) goto fail_tx;
    jazz_types_NodeAlias tx_node_alias;
    if ((e = jazz_node__node__NodeState__ensure_node_alias(self, tx_id.node, &tx_node_alias)).code) goto fail_tx;
    batch = groove__db__facade__Database__open_batch(self->database);
    JZ_VEC_FOR(const jazz_node_SchemaCommit, sc, commits)
        JZ_VEC_FOR(const groove_StagedLargeValueId, id, sc->commit.staged_large_values) groove__db__batch__DatabaseBatch__accept_large_value(batch, *id);
    {
        jazz_model_Fate pending = { .tag = JZ_FATE_PENDING, .rejection = NULL };
        groove_ValueVec values;
        if ((e = jazz_node__node__codec__transaction_values(tx_node_alias, &tx, &pending, false, 0, self->authored_commit_durability,
                contribution_merge_value, &values)).code) goto fail_tx;
        jz_str jazz_transactions = jz_str_from("jazz_transactions");
        groove__db__batch__DatabaseBatch__insert(batch, &jazz_transactions, values);
        jz_str_drop(&jazz_transactions);
    }
    for (u32 i = 0; i < commits.len; ++i) {
        jazz_types_SchemaVersionId write_schema_version = commits.ptr[i].schema;
        jazz_node_MergeableCommit *commit = &commits.ptr[i].commit;
        jazz_types_TxTime provenance_at;
        jz_err te = jazz_types__time__TxTime__from_physical_ms(commit->now_ms, &provenance_at);
        if (te.code) { jz_err_drop(&te); e = JZN_INVALID_MERGEABLE("commit now_ms exceeds packed HLC physical-millisecond range"); goto fail_tx; }
        jazz_types_SchemaVersionAlias schema_version_alias;
        if ((e = jazz_node__node__NodeState__ensure_schema_version_alias(self, write_schema_version, &schema_version_alias)).code) goto fail_tx;
        jazz_model_TableSchema table_schema;
        if ((e = jazz_node__node__NodeState__table_in_schema(self, &commit->table, write_schema_version, &table_schema)).code) goto fail_tx;
        const jazz_protocol_SchemaVersion *sv = jz_btree_get(&self->catalogue.catalogue_schemas, &write_schema_version);
        if (!sv) { jazz_model_TableSchema_drop(&table_schema); e = JZN_INVALID_STORED("commit schema missing"); goto fail_tx; }
        jazz_model_BranchKey branch_key; jz_btree branch_cells;
        if ((e = jazz_model__schema__JazzSchema__project_branch_selector(&sv->schema, &table_schema, &commit->branch, &branch_key, &branch_cells)).code) {
            jazz_model_TableSchema_drop(&table_schema); e = jazz_node__node__Error_invalid_branch_key(e); goto fail_tx;
        }
        bool known_fresh_row = commit->known_fresh_row;
        jazz_node_VersionRow *previous_current = NULL;
        jz_btree cells = {0}, authored_columns = {0}, base_cells = {0}, image_cells = {0}, authored_column_ids = {0};
        bool has_image = false, has_authored_column_ids = false;
        jz_bytes counter_signs = {0};
        jazz_node_VersionRow overlay, stored;
        bool has_overlay = false;
        if (!known_fresh_row) {
            if ((e = jazz_node__node__currency__NodeState__query_local_winner_in_branch(self, &table_schema.name, &branch_key, commit->row_uuid, &previous_current)).code) goto fail_row;
            if (!previous_current
                && (e = jazz_node__node__currency__NodeState__query_global_winner_in_branch(self, &table_schema.name, &branch_key, commit->row_uuid, &previous_current)).code) goto fail_row;
        }
        jazz_types_AuthorSubject created_by = commit->made_by;
        jazz_types_TxTime created_at = provenance_at;
        if (previous_current) {
            created_by = jazz_node__node__codec__VersionRow__created_by(previous_current);
            created_at = jazz_node__node__codec__VersionRow__created_at(previous_current);
        }
        /* The image this write is made over (SPEC 4 §4.6, "Base of a write"):
         * the settled image's seq, and this node's own newest pending write to
         * the row. An insert, and a write over no image, have no base. */
        jazz_protocol_RowBase base = {0};
        if (previous_current) {
            if ((e = jazz_node__node__ingest__NodeState__newest_own_pending_write_in_batch(self, batch, &table_schema.name, &branch_key,
                    commit->row_uuid, &base.has_pending, &base.pending)).code) goto fail_row;
            if ((e = jazz_node__node__ingest__NodeState__global_current_seq_in_batch(self, batch, write_schema_version, &table_schema.name,
                    &branch_key, commit->row_uuid, &base.has_seq, &base.seq)).code) goto fail_row;
        }
        cells = commit->cells;
        jz_btree_init(&commit->cells, &jazz_node_STRING_VALUE_MAP_DESC);
        {
            const jz_str *column; const groove_Value *value;
            JZ_BTREE_FOR(&branch_cells, column, value) {
                const groove_Value *authored = jz_btree_get(&cells, column);
                if (authored && !groove_Value_eq(authored, value)) { e = JZN_INVALID_MERGEABLE("branch column does not match exact branch key"); goto fail_row; }
                jz_str k = jz_str_clone(column); groove_Value v; groove_Value_clone(&v, value);
                jz_btree_insert(&cells, &k, &v, NULL);
            }
        }
        if (commit->has_authored_columns) jz_btree_clone(&authored_columns, &commit->authored_columns, str_kclone, NULL);
        else string_keys_into_set(&cells, &authored_columns);
        /* The version is a complete row image: unauthored cells carry the
         * previous image forward, as does its deletion state unless this
         * commit deletes or restores the row. */
        bool has_deletion = false; jazz_model_DeletionEvent deletion = 0;
        if (commit->has_deletion) {
            jz_str d = jz_str_from(JZN_DELETION_COLUMN_NAME);
            jz_btree_insert(&authored_columns, &d, NULL, NULL);
            has_deletion = true; deletion = commit->deletion;
        } else if (previous_current) {
            has_deletion = jazz_node__node__codec__VersionRow__deletion(previous_current, &deletion);
        }
        jz_btree_init(&base_cells, &jazz_node_STRING_VALUE_MAP_DESC);
        if (previous_current) {
            jazz_types_SchemaVersionId previous_schema;
            if (!jazz_node__node__NodeState__schema_version_for_alias(self, jazz_node__node__codec__VersionRow__schema_version_alias(previous_current), &previous_schema)) {
                e = JZN_INVALID_STORED("previous row image schema alias must exist"); goto fail_row;
            }
            const jz_str *previous_table_name = jazz_node__node__codec__VersionRow__table(previous_current);
            jazz_model_TableSchema previous_table;
            if ((e = jazz_node__node__NodeState__table_in_schema(self, previous_table_name, previous_schema, &previous_table)).code) goto fail_row;
            jz_btree inherited;
            e = jazz_node__node__codec__VersionRow__cells(previous_current, &previous_table, &inherited);
            jazz_model_TableSchema_drop(&previous_table);
            if (e.code) goto fail_row;
            bool projected_some; jz_str projected = {0};
            if (jz_uuid_eq(&previous_schema, &write_schema_version)) { projected_some = true; projected = jz_str_clone(&table_schema.name); }
            else if ((e = jazz_node__node__source_resolution__NodeState__translate_cells(self, previous_schema, write_schema_version,
                    previous_table_name, &inherited, &projected_some, &projected)).code) { jz_btree_drop(&inherited); goto fail_row; }
            if (projected_some && jz_str_eq(&projected, &table_schema.name)) {
                jz_btree_drop(&base_cells);
                jz_btree_clone(&base_cells, &inherited, str_kclone, value_vclone);
                jz_str column; groove_Value value;
                while (jz_btree_pop_first(&inherited, &column, &value)) {
                    if (!jz_btree_contains(&authored_columns, &column) && !jz_btree_contains(&cells, &column))
                        jz_btree_insert(&cells, &column, &value, NULL);
                    else { jz_str_drop(&column); groove_Value_drop(&value); }
                }
            }
            if (projected_some) jz_str_drop(&projected);
            jz_btree_drop(&inherited);
        }
        /* Merge columns travel as ops over the image this write saw; the local
         * overlay shows the resulting image. */
        if ((e = jazz_node__node__merge_ops__split_merge_ops(&table_schema, &authored_columns, &base_cells, &cells,
                &has_image, &image_cells, &counter_signs)).code) goto fail_row;
        if ((e = jazz_node__node__physical__NodeState__authored_column_ids_for_names(self, write_schema_version, &table_schema.name,
                &authored_columns, &has_authored_column_ids, &authored_column_ids)).code) goto fail_row;
        jazz_node_PreparedPhysicalWritePlan *plan;
        if ((e = jazz_node__node__physical__NodeState__prepared_physical_write_plan(self, write_schema_version, &table_schema.name,
                JZN_PWT_HISTORY, &plan)).code) goto fail_row;
        RowVersionCtx rv = {
            .table_schema = &table_schema, .table = &commit->table, .branch_key = &branch_key, .row_uuid = commit->row_uuid,
            .tx_node_alias = tx_node_alias, .schema_version_alias = schema_version_alias, .made_at = made_at,
            .created_by = created_by, .created_at = created_at, .updated_by = commit->made_by, .provenance_at = provenance_at,
            .has_authored_column_ids = has_authored_column_ids, .authored_column_ids = &authored_column_ids,
            .has_deletion = has_deletion, .deletion = deletion, .base = base,
            .foreign_schema = !jz_uuid_eq(&write_schema_version, &self->catalogue.local_schema_version_id),
            .write_schema_version = write_schema_version,
            .history_descriptor = jazz_node__node__physical__PreparedPhysicalWritePlan__logical_descriptor(plan),
        };
        /* The overlay is the local image: it carries no ops, so no signs. */
        if (has_image) {
            jz_btree moved = image_cells; has_image = false;
            if ((e = row_version(&rv, moved, (jz_bytes){0}, &overlay)).code) goto fail_row;
            has_overlay = true;
        }
        {
            jz_btree moved = cells; jz_btree_init(&cells, &jazz_node_STRING_VALUE_MAP_DESC);
            jz_bytes signs = counter_signs; counter_signs = (jz_bytes){0};
            if ((e = row_version(&rv, moved, signs, &stored)).code) goto fail_row;
        }
        {
            groove_InternStr history_table; groove_ValidatedVariantRecord groove_record;
            if ((e = jazz_node__node__physical__NodeState__version_storage_write_binding(self, &stored, tx.made_by, &history_table, &groove_record)).code) {
                jazz_node_VersionRow_drop(&stored); goto fail_row;
            }
            jazz_node_PrimaryKeyValue key;
            if ((e = jazz_node__node__physical__NodeState__version_storage_primary_key(self, &stored, &key)).code) { jazz_node_VersionRow_drop(&stored); goto fail_row; }
            groove__db__batch__DatabaseBatch__insert_raw(batch, history_table.s, key, groove_record);
        }
        if ((e = jazz_node__node__ingest__NodeState__write_ahead_current_insert(self, batch, has_overlay ? &overlay : &stored)).code) {
            jazz_node_VersionRow_drop(&stored); goto fail_row;
        }
        JZ_VEC_PUSH(stored_versions, stored);
    fail_row:
        if (has_overlay) jazz_node_VersionRow_drop(&overlay);
        if (previous_current) jazz_node__node__codec__VersionRow__free(previous_current);
        if (cells.d) jz_btree_drop(&cells);
        if (authored_columns.d) jz_btree_drop(&authored_columns);
        if (base_cells.d) jz_btree_drop(&base_cells);
        if (has_image) jz_btree_drop(&image_cells);
        if (authored_column_ids.d) jz_btree_drop(&authored_column_ids);
        jz_bytes_drop(&counter_signs);
        jz_btree_drop(&branch_cells);
        jazz_model_BranchKey_drop(&branch_key);
        jazz_model_TableSchema_drop(&table_schema);
        if (e.code) goto fail_tx;
    }
    {
        groove_AppliedBatch *persistence;
        e = jazz_node__node__NodeState__apply_node_batch(self, batch, &persistence);
        batch = NULL;
        if (e.code) goto fail_tx;
        jazz_node_VersionRowVec cached; version_row_vec_clone(&cached, &stored_versions);
        jazz_node__node__NodeState__cache_tx_versions(self, tx_id, cached);
        if (!jazz_types_AuthorSubject_eq(permission_subject, made_by))
            jz_btree_insert(&self->open_tx.local_permission_subjects, &tx_id, &permission_subject, NULL);
        jz_btree_insert(&self->pending_persistence, &tx_id, NULL, NULL);
        *out = (jazz_node_PublishedTransaction){ tx_id, persistence };
    }
fail_tx:
    if (batch) groove__db__batch__DatabaseBatch__drop(batch);
    version_row_vec_drop(&stored_versions);
    jz_btree_drop(&staged_ids);
    jazz_model_Transaction_drop(&tx);
    schema_commit_vec_drop(&commits);
    return e;
fail_cm:
    jazz_model_ContributionMergeProvenance_drop(&contribution_merge);
    schema_commit_vec_drop(&commits);
    return e;
}

/* Lower oversized ordinary scalar cells through Groove and atomically stage
 * their immutable nodes before row publication begins. */
jz_err jazz_node__node__NodeState__prepare_and_stage_large_commit_values(NS *self, jazz_node_SchemaCommitVec *commits)
{
    JZ_VEC_FOR(jazz_node_SchemaCommit, sc, *commits) {
        jazz_node_MergeableCommit *commit = &sc->commit;
        const jazz_model_TableSchema *table_schema;
        JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, &commit->table, sc->schema, &table_schema));
        JZ_VEC(u32) semantic_kinds = {0};
        bool any_indirect = false;
        const jz_str *column; groove_Value *value;
        JZ_BTREE_FOR(&commit->cells, column, value) {
            const jazz_model_ColumnSchema *candidate = find_column(table_schema, column);
            JZ_VEC_PUSH(semantic_kinds, candidate ? candidate->large_value_kind : (u32)JZ_LVSK_NOT_LARGE);
            if (jazz_node__node__value_contains_indirect_descriptor(value)) any_indirect = true;
        }
        jz_btree inherited; bool has_inherited = false;
        if (any_indirect) {
            jz_err e = jazz_node__node__NodeState__current_physical_cells_in_branch_schema(self, sc->schema, &commit->table, &commit->branch,
                commit->row_uuid, &has_inherited, &inherited);
            if (e.code) { jz_free(semantic_kinds.ptr); return e; }
        }
        if (!has_inherited) jz_btree_init(&inherited, &jazz_node_STRING_VALUE_MAP_DESC);
        u32 i = 0;
        JZ_BTREE_FOR(&commit->cells, column, value) {
            u32 semantic_kind = semantic_kinds.ptr[i++];
            const groove_Value *prior = jz_btree_get(&inherited, column);
            if (jazz_node__node__value_contains_indirect_descriptor(value) && prior && groove_Value_eq(prior, value)) {
                jz_str c = jz_str_clone(column);
                if (jz_btree_insert(&commit->prepared_large_columns, &c, NULL, NULL)) {}
                continue;
            }
            bool staged_some; groove_StagedLargeValue staged;
            jz_err e = jazz_node__node__NodeState__prepare_and_stage_large_scalar(self, value, semantic_kind, &staged_some, &staged);
            if (e.code) { jz_btree_drop(&inherited); jz_free(semantic_kinds.ptr); return e; }
            if (!staged_some) continue;
            JZ_VEC_PUSH(commit->staged_large_values, staged.id);
            groove_LargeValueRef_drop(staged.value_ref);
        }
        jz_btree_drop(&inherited);
        jz_free(semantic_kinds.ptr);
    }
    return JZ_OK;
}

/* Lower one top-level scalar cell, preserving its nullable wrapper. Shared by
 * mergeable and exclusive publication so neither write path can leak an
 * oversized inline scalar onto the wire. */
jz_err jazz_node__node__NodeState__prepare_and_stage_large_scalar(NS *self, groove_Value *value, u32 semantic_kind,
    bool *some, groove_StagedLargeValue *out)
{
    *some = false;
    bool candidate = false, nullable = false; u32 kind = 0; jz_bytes bytes = {0};
    u32 string_kind = semantic_kind == JZ_LVSK_JSON ? GROOVE_LVK_JSON : GROOVE_LVK_STRING;
    const groove_Value *probe = value;
    if (value->tag == GV_NULLABLE && value->v.nullable) { probe = value->v.nullable; nullable = true; }
    if (probe->tag == GV_STRING && probe->v.s.len > GROOVE_INLINE_VALUE_MAX_BYTES) {
        candidate = true; kind = string_kind; bytes = jz_bytes_from((const u8 *)probe->v.s.ptr, probe->v.s.len);
    } else if (probe->tag == GV_BYTES && probe->v.bytes.len > GROOVE_INLINE_VALUE_MAX_BYTES) {
        candidate = true; kind = GROOVE_LVK_BYTES; bytes = jz_bytes_from(probe->v.bytes.ptr, probe->v.bytes.len);
    }
    if (!candidate) return JZ_OK;
    groove_StagedLargeValue staged;
    jz_err e = groove__db__large_values__Database__prepare_and_stage_large_value(self->database, kind, jz_bytes_slice(&bytes), &staged);
    jz_bytes_drop(&bytes);
    if (e.code) return jazz_node__node__Error_groove(e);
    e = jazz_node__node__NodeState__enforce_large_value_staging_policy(self, &staged);
    if (e.code) { groove_LargeValueRef_drop(staged.value_ref); return e; }
    groove_Value descriptor = gv_large(groove_LargeValueRef_clone(staged.value_ref));
    groove_Value_drop(value);
    if (nullable) {
        groove_Value *inner = jz_alloc(sizeof(groove_Value));
        *inner = descriptor;
        *value = gv_nullable(inner);
    } else {
        *value = descriptor;
    }
    *some = true;
    *out = staged;
    return JZ_OK;
}

/* Commit a local mergeable write and return its sync commit unit. */
jz_err jazz_node__node__NodeState__commit_mergeable_unit(NS *self, jazz_node_MergeableCommit commit,
    jazz_node_PublishedTransaction *published, jazz_protocol_SyncMessage **unit)
{
    JZ_TRY(jazz_node__node__NodeState__commit_mergeable(self, commit, published));
    return jazz_node__node__NodeState__resident_commit_unit(self, published->tx_id, unit);
}

static jz_err stored_transaction(NS *self, jazz_model_TxId tx_id, jazz_model_Transaction *out)
{
    bool found; jazz_node_StoredTransaction stored;
    JZ_TRY(jazz_node__node__currency__NodeState__query_transaction(self, tx_id, &found, &stored));
    if (!found) return jazz_node__node__Error_missing_transaction(tx_id);
    jazz_model_Transaction_clone(out, &stored.tx);
    jazz_node__node__codec__StoredTransaction__drop(&stored);
    return JZ_OK;
}

static jz_err version_records(NS *self, jazz_node_VersionRowVec rows, jazz_protocol_VersionRecordVec *out)
{
    *out = (jazz_protocol_VersionRecordVec){0};
    jz_err e = JZ_OK;
    JZ_VEC_FOR(const jazz_node_VersionRow, row, rows) {
        jazz_protocol_VersionRecord *record;
        if ((e = jazz_node__node__NodeState__version_record_from_row(self, row, &record)).code) break;
        JZ_VEC_PUSH(*out, record);
    }
    version_row_vec_drop(&rows);
    if (e.code) jazz_protocol_VersionRecordVec_drop(out);
    return e;
}

jz_err jazz_node__node__NodeState__resident_commit_unit(NS *self, jazz_model_TxId tx_id, jazz_protocol_SyncMessage **out)
{
    /* Use the transaction actually published by the common commit path.
     * Reconstructing an envelope here loses generated branch-write intent. */
    jazz_model_Transaction tx;
    JZ_TRY(stored_transaction(self, tx_id, &tx));
    jazz_node_VersionRowVec rows;
    bool cached = jazz_node__node__NodeState__cached_tx_versions(self, tx.tx_id, &rows);
    JZ_ASSERT(cached, "newly published transaction retains its resident versions");
    jazz_protocol_VersionRecordVec versions;
    jz_err e = version_records(self, rows, &versions);
    if (e.code) { jazz_model_Transaction_drop(&tx); return e; }
    *out = jazz_protocol__protocol__SyncMessage__commit_unit(tx, versions);
    return JZ_OK;
}

/* Settle a completed persistence receipt and release its storage boundary. */
jz_err jazz_node__node__NodeState__settle_published_transaction(NS *self, jazz_model_TxId tx_id, groove_PersistedBatch *persistence)
{
    jz_err e = groove__db__commit__Database__finish_persistence(self->database, persistence);
    if (e.code) return jazz_node__node__Error_groove(e);
    jz_btree_remove(&self->pending_persistence, &tx_id, NULL, NULL);
    return JZ_OK;
}

/* Persist and settle one resident transaction publication. */
jz_err jazz_node__node__NodeState__persist_and_settle_transaction(NS *self, jazz_node_PublishedTransaction published, jazz_model_TxId *out)
{
    jazz_model_TxId tx_id = published.tx_id;
    groove_PersistedBatch *persistence = groove__db__commit__Database__persist_with_progress(self->database, published.persistence);
    jz_err e = jazz_node__node__NodeState__settle_published_transaction(self, tx_id, persistence);
    groove__db__commit__AppliedBatch__drop(published.persistence);
    if (e.code) return e;
    *out = tx_id;
    return JZ_OK;
}

static void published_vec_drop(jazz_node_PublishedTransactionVec *v, u32 from)
{
    for (u32 i = from; i < v->len; ++i) groove__db__commit__AppliedBatch__drop(v->ptr[i].persistence);
    jz_free(v->ptr); v->ptr = NULL; v->len = v->cap = 0;
}

static void sync_message_free_v(void *p) { jazz_protocol_SyncMessage_free(*(jazz_protocol_SyncMessage **)p); }

/* Persist and settle every resident publication attached to an outcome. */
jz_err jazz_node__node__NodeState__persist_and_settle_outcome(NS *self, jazz_node_PublicationOutcome outcome, void **out)
{
    void *value = outcome.value;
    jazz_node_PublishedTransactionVec publications = outcome.publications;
    jz_deque work = outcome.post_settlement_work;
    jz_err e = JZ_OK;
    for (;;) {
        for (u32 i = 0; i < publications.len; ++i) {
            jazz_node_PublishedTransaction *publication = &publications.ptr[i];
            groove_PersistedBatch *persistence = groove__db__commit__Database__persist_with_progress(self->database, publication->persistence);
            e = jazz_node__node__NodeState__settle_published_transaction(self, publication->tx_id, persistence);
            groove__db__commit__AppliedBatch__drop(publication->persistence);
            if (e.code) { published_vec_drop(&publications, i + 1); goto done; }
        }
        published_vec_drop(&publications, publications.len);
        jazz_protocol_SyncMessage *message;
        if (!jz_deque_pop_front(&work, &message)) break;
        jazz_node_PublicationOutcome next;
        if ((e = jazz_node__node__ingest__catalogue__NodeState__apply_sync_message(self, message, &next)).code) goto done;
        /* Internal continuation work, not a reply to a remote sender: its
         * resident publications and continuations are lifecycle-significant;
         * its protocol response is not. */
        jazz_protocol_SyncMessageVec_free_boxed(next.value);
        publications = next.publications;
        jazz_protocol_SyncMessage *continuation;
        while (jz_deque_pop_front(&next.post_settlement_work, &continuation)) jz_deque_push_back(&work, &continuation);
        jz_deque_drop(&next.post_settlement_work, NULL);
    }
done:
    jz_deque_drop(&work, sync_message_free_v);
    if (e.code) return e;
    *out = value;
    return JZ_OK;
}

/* Rebuild the sync commit unit for an already-committed local transaction
 * from its stored versions. */
jz_err jazz_node__node__NodeState__commit_unit_for(NS *self, jazz_model_TxId tx_id, jazz_protocol_SyncMessage **out)
{
    jazz_model_Transaction tx;
    JZ_TRY(stored_transaction(self, tx_id, &tx));
    jazz_node_VersionRowVec rows;
    jz_err e = jazz_node__node__currency__NodeState__query_versions_for_tx(self, tx_id, &rows);
    jazz_protocol_VersionRecordVec versions;
    if (!e.code) e = version_records(self, rows, &versions);
    if (e.code) { jazz_model_Transaction_drop(&tx); return e; }
    *out = jazz_protocol__protocol__SyncMessage__commit_unit(tx, versions);
    return JZ_OK;
}

/* Open an exclusive transaction over the current snapshot. */
jz_err jazz_node__node__NodeState__visible_current_cells(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, bool *has, jz_btree *out)
{
    *has = false;
    jazz_node_CurrentRowVec rows;
    JZ_TRY(jazz_node__node__NodeState__current_rows(self, table, JZ_DURABILITY_LOCAL, &rows));
    JZ_VEC_FOR(const jazz_node_CurrentRow, row, rows) {
        jazz_types_RowUuid id = jazz_node__node__CurrentRow__row_uuid(row);
        if (!jz_uuid_eq(&id, &row_uuid)) continue;
        const jazz_model_TableSchema *table_schema;
        jz_err te = jazz_node__node__NodeState__table(self, table, &table_schema);
        JZ_ASSERT(!te.code, "table exists");
        jz_btree_init(out, &jazz_node_STRING_VALUE_MAP_DESC);
        JZ_VEC_FOR(const jazz_model_ColumnSchema, column, table_schema->columns) {
            groove_Value value;
            if (jazz_node__node__CurrentRow__cell(row, table_schema, &column->name, &value)) {
                jz_str k = jz_str_clone(&column->name);
                jz_btree_insert(out, &k, &value, NULL);
            }
        }
        *has = true;
        break;
    }
    JZ_VEC_FOR(jazz_node_CurrentRow, row, rows) jazz_node_CurrentRow_drop(row);
    jz_free(rows.ptr);
    return JZ_OK;
}

/* Read one exact branch-local row for mutation preparation. */
jz_err jazz_node__node__NodeState__visible_current_cells_in_branch(NS *self, const jz_str *table, const jazz_model_BranchSelector *branch,
    jazz_types_RowUuid row_uuid, bool *has, jz_btree *out)
{
    jazz_types_SchemaVersionId schema_version = self->catalogue.active_schema.schema;
    return jazz_node__node__NodeState__visible_current_physical_cells_in_branch_schema(self, schema_version, table, branch, row_uuid, has, out);
}

/* Read a branch-local winner projected into one registered schema while
 * retaining indirect scalar descriptors rather than hydrating them. */
jz_err jazz_node__node__NodeState__visible_current_physical_cells_in_branch_schema(NS *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, const jazz_model_BranchSelector *branch, jazz_types_RowUuid row_uuid, bool *has, jz_btree *out)
{
    jazz_model_TxId winner;
    return jazz_node__node__NodeState__visible_current_physical_cells_and_winner_in_branch_schema(self, schema_version, table, branch, row_uuid, has, out, &winner);
}

jz_err jazz_node__node__NodeState__visible_current_physical_cells_and_winner_in_branch_schema(NS *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, const jazz_model_BranchSelector *branch, jazz_types_RowUuid row_uuid, bool *has, jz_btree *cells_out, jazz_model_TxId *tx_out)
{
    *has = false;
    const jazz_model_TableSchema *table_schema;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, table, schema_version, &table_schema));
    const jazz_protocol_SchemaVersion *sv = jz_btree_get(&self->catalogue.catalogue_schemas, &schema_version);
    if (!sv) return JZN_INVALID_STORED("registered read schema missing");
    jazz_model_BranchKey branch_key; jz_btree branch_cells;
    jz_err e = jazz_model__schema__JazzSchema__project_branch_selector(&sv->schema, table_schema, branch, &branch_key, &branch_cells);
    if (e.code) return jazz_node__node__Error_invalid_branch_key(e);
    jz_btree_drop(&branch_cells);
    jazz_node_VersionRow *content = NULL;
    e = jazz_node__node__currency__NodeState__query_local_winner_in_branch(self, table, &branch_key, row_uuid, &content);
    if (!e.code && !content) e = jazz_node__node__currency__NodeState__query_global_winner_in_branch(self, table, &branch_key, row_uuid, &content);
    jazz_model_BranchKey_drop(&branch_key);
    if (e.code) return e;
    if (!content || jazz_node__node__codec__VersionRow__is_deleted(content)) {
        if (content) jazz_node__node__codec__VersionRow__free(content);
        return JZ_OK;
    }
    jazz_model_TxId content_tx;
    jazz_types_SchemaVersionId authored_schema;
    const jazz_model_TableSchema *authored_table;
    jz_btree cells = {0};
    bool projected_some = false; jz_str projected_table = {0};
    if ((e = jazz_node__node__NodeState__version_tx_id(self, content, &content_tx)).code) goto out;
    if (!jazz_node__node__NodeState__schema_version_for_alias(self, jazz_node__node__codec__VersionRow__schema_version_alias(content), &authored_schema)) {
        e = JZN_INVALID_STORED("current version schema alias must exist"); goto out;
    }
    if ((e = jazz_node__node__NodeState__table_in_schema_ref(self, jazz_node__node__codec__VersionRow__table(content), authored_schema, &authored_table)).code) goto out;
    if ((e = jazz_node__node__NodeState__materialized_cells_for_version(self, authored_table, content, &cells)).code) goto out;
    if ((e = jazz_node__node__source_resolution__NodeState__translate_cells(self, authored_schema, schema_version,
            jazz_node__node__codec__VersionRow__table(content), &cells, &projected_some, &projected_table)).code) goto out;
    if (!projected_some) goto out;
    if (!jz_str_eq(&projected_table, table)) { e = JZN_INVALID_STORED("current version projects to an unexpected table"); goto out; }
    *cells_out = cells; cells = (jz_btree){0};
    *tx_out = content_tx;
    *has = true;
out:
    if (projected_some) jz_str_drop(&projected_table);
    if (cells.d) jz_btree_drop(&cells);
    jazz_node__node__codec__VersionRow__free(content);
    return e;
}

/* Return the exact local content parent for a branch-local row. */
jz_err jazz_node__node__NodeState__local_content_winner_tx_id_in_branch(NS *self, const jz_str *table, const jazz_model_BranchSelector *branch,
    jazz_types_RowUuid row_uuid, bool *has, jazz_model_TxId *out)
{
    *has = false;
    jazz_node_VersionRow *version;
    JZ_TRY(jazz_node__node__NodeState__local_winner_in_branch_selector(self, table, branch, row_uuid, &version));
    if (!version) return JZ_OK;
    /* A deleted image is not visible content. */
    jz_err e = JZ_OK;
    if (!jazz_node__node__codec__VersionRow__is_deleted(version)) {
        e = jazz_node__node__NodeState__version_tx_id(self, version, out);
        *has = !e.code;
    }
    jazz_node__node__codec__VersionRow__free(version);
    return e;
}

/* Return the local winner of a branch-local row while it is deleted. */
jz_err jazz_node__node__NodeState__local_deletion_winner_tx_id_in_branch(NS *self, const jz_str *table, const jazz_model_BranchSelector *branch,
    jazz_types_RowUuid row_uuid, bool *has, jazz_model_TxId *out)
{
    *has = false;
    jazz_node_VersionRow *version;
    JZ_TRY(jazz_node__node__NodeState__local_winner_in_branch_selector(self, table, branch, row_uuid, &version));
    if (!version) return JZ_OK;
    jz_err e = JZ_OK;
    if (jazz_node__node__codec__VersionRow__is_deleted(version)) {
        e = jazz_node__node__NodeState__version_tx_id(self, version, out);
        *has = !e.code;
    }
    jazz_node__node__codec__VersionRow__free(version);
    return e;
}

jz_err jazz_node__node__NodeState__local_winner_in_branch_selector(NS *self, const jz_str *table, const jazz_model_BranchSelector *branch,
    jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out)
{
    *out = NULL;
    jazz_types_SchemaVersionId schema_version = self->catalogue.active_schema.schema;
    const jazz_model_TableSchema *table_schema;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, table, schema_version, &table_schema));
    const jazz_protocol_SchemaVersion *sv = jz_btree_get(&self->catalogue.catalogue_schemas, &schema_version);
    if (!sv) return JZN_INVALID_STORED("current write schema missing");
    jazz_model_BranchKey branch_key; jz_btree branch_cells;
    jz_err e = jazz_model__schema__JazzSchema__project_branch_selector(&sv->schema, table_schema, branch, &branch_key, &branch_cells);
    if (e.code) return jazz_node__node__Error_invalid_branch_key(e);
    jz_btree_drop(&branch_cells);
    e = jazz_node__node__currency__NodeState__query_local_view_winner_in_branch(self, table, &branch_key, row_uuid, out);
    jazz_model_BranchKey_drop(&branch_key);
    return e;
}

/* Return current rows at the requested durability tier. */
jz_err jazz_node__node__NodeState__current_rows(NS *self, const jz_str *table, jazz_model_DurabilityTier settled, jazz_node_CurrentRowVec *out)
{
    JZ_TRY(jazz_node__node__NodeState__require_catalogue_ready(self));
    jazz_model_Query query = jazz_model__query__Query__from_table(table);
    jazz_model_ValidatedQuery shape;
    jz_err e = jazz_model__query__Query__validate(&query, &self->catalogue.schema, &shape);
    jazz_model_Query_drop(&query);
    if (e.code) return jazz_node__node__Error_query(e);
    jz_btree params; jz_btree_init(&params, &jazz_node_STRING_VALUE_MAP_DESC);
    jazz_model_Binding binding;
    e = jazz_model__query__ValidatedQuery__bind(&shape, params, &binding);
    if (e.code) { jazz_model_ValidatedQuery_drop(&shape); return jazz_node__node__Error_query(e); }
    e = AUTHZ(NodeState__query_rows)(self, &shape, &binding, settled, out);
    jazz_model_Binding_drop(&binding);
    jazz_model_ValidatedQuery_drop(&shape);
    return e;
}

jz_err jazz_node__node__NodeState__local_content_winner_tx_id(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, bool *has, jazz_model_TxId *out)
{
    return jazz_node__node__NodeState__local_content_winner_tx_id_in_schema(self, self->catalogue.active_schema.schema, table, row_uuid, has, out);
}

/* The local image (the pending overlay, else the synced image) by key. */
static jz_err local_view_image(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **image)
{
    jazz_model_TableSchema table_schema;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema(self, table, schema_version, &table_schema));
    jazz_model_BranchKey branch_key; jazz_model_BranchKey_default(&branch_key);
    jz_err e = jazz_node__node__currency__NodeState__query_local_view_winner_in_branch(self, &table_schema.name, &branch_key, row_uuid, image);
    jazz_model_BranchKey_drop(&branch_key);
    jazz_model_TableSchema_drop(&table_schema);
    return e;
}

jz_err jazz_node__node__NodeState__local_content_winner_tx_id_in_schema(NS *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, jazz_types_RowUuid row_uuid, bool *has, jazz_model_TxId *out)
{
    *has = false;
    /* Only the winner's identity is needed: read the local image by key
     * rather than evaluating a query graph. A deleted image is not content. */
    jazz_node_VersionRow *image;
    JZ_TRY(local_view_image(self, schema_version, table, row_uuid, &image));
    if (!image) return JZ_OK;
    jz_err e = JZ_OK;
    if (!jazz_node__node__codec__VersionRow__is_deleted(image)) {
        e = jazz_node__node__NodeState__version_tx_id(self, image, out);
        *has = !e.code;
    }
    jazz_node__node__codec__VersionRow__free(image);
    return e;
}

jz_err jazz_node__node__NodeState__local_deletion_winner_tx_id(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, bool *has, jazz_model_TxId *out)
{
    return jazz_node__node__NodeState__local_deletion_winner_tx_id_in_schema(self, self->catalogue.active_schema.schema, table, row_uuid, has, out);
}

jz_err jazz_node__node__NodeState__local_deletion_winner_tx_id_in_schema(NS *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, jazz_types_RowUuid row_uuid, bool *has, jazz_model_TxId *out)
{
    *has = false;
    /* Deletion is stamped into the row image: the "deletion winner" is the
     * current image's transaction while that image is deleted. */
    jazz_node_VersionRow *image;
    JZ_TRY(local_view_image(self, schema_version, table, row_uuid, &image));
    if (!image) return JZ_OK;
    jz_err e = JZ_OK;
    jazz_model_DeletionEvent deletion;
    if (jazz_node__node__codec__VersionRow__deletion(image, &deletion) && deletion == JZ_DELETION_DELETED) {
        e = jazz_node__node__NodeState__version_tx_id(self, image, out);
        *has = !e.code;
    }
    jazz_node__node__codec__VersionRow__free(image);
    return e;
}

static jz_err rebuild_ahead_current_keys_inner(NS *self)
{
    jazz_node__node__AheadCurrentKeys__clear(self->ahead_current_keys);
    jz_btree physical_table_ids; jz_btree_init(&physical_table_ids, &jazz_node_U64_SET_DESC);
    const void *k; const jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(&self->catalogue.physical_mappings, k, mapping) {
        const void *tk; const jazz_node_PhysicalTableMapping *table;
        JZ_BTREE_FOR(&mapping->tables, tk, table) jz_btree_insert(&physical_table_ids, &table->table_id, NULL, NULL);
    }
    jz_err e = JZ_OK;
    const jazz_types_PhysicalTableId *table_id; const void *unit;
    JZ_BTREE_FOR(&physical_table_ids, table_id, unit) {
        jz_str content_table = jazz_node__node__physical__physical_ahead_current_table_name(*table_id);
        groove_RawRowVec content_rows;
        e = groove__db__primary_storage__Database__primary_key_scan_raw(self->database, &content_table, NULL, 0, &content_rows);
        jz_str_drop(&content_table);
        if (e.code) { e = jazz_node__node__Error_groove(e); break; }
        for (u32 i = 0; i < content_rows.len && !e.code; ++i) {
            const groove_RawRow *raw = content_rows.ptr[i];
            groove_BorrowedRecord record = groove__records__record__OwnedRecord__borrowed(groove__db__RawRow__record(raw));
            u64 tx_time, alias;
            if ((e = groove__records__record__BorrowedRecord__get_u64(&record, JZN_HISTORY_ROW_FIELD_TX_TIME_IDX, &tx_time)).code
                || (e = groove__records__record__BorrowedRecord__get_u64(&record, JZN_HISTORY_ROW_FIELD_TX_NODE_ID_IDX, &alias)).code) {
                e = jazz_node__node__Error_record(e); break;
            }
            jazz_types_NodeUuid node;
            if (!jazz_node__node__NodeState__node_for_alias(self, alias, &node)) { e = JZN_INVALID_STORED("overlay node alias must exist"); break; }
            jz_slice key = groove__db__RawRow__key(raw);
            jazz_node__node__AheadCurrentKeys__insert(self->ahead_current_keys, *table_id, jz_bytes_from(key.ptr, key.len),
                (jazz_model_TxId){ tx_time, node });
        }
        groove_RawRowVec_drop(&content_rows);
        if (e.code) break;
    }
    jz_btree_drop(&physical_table_ids);
    return e;
}

jz_err jazz_node__node__NodeState__rebuild_ahead_current_keys(NS *self)
{
    return rebuild_ahead_current_keys_inner(self);
}

/* Note that `version`'s history row is written in the open batch; the next
 * applied node batch adds it to the transaction's `jazz_tx_touched_rows`. */
void jazz_node__node__NodeState__mark_tx_touched_row(NS *self, jazz_types_PhysicalTableId table_id, const jazz_node_VersionRow *version)
{
    jazz_node_TxTouchedKey key = { jazz_node__node__codec__VersionRow__tx_time(version), jazz_node__node__codec__VersionRow__tx_node_alias(version) };
    bool inserted;
    jazz_node_TouchedRows *rows = jz_btree_entry(&self->tx_touched_dirty, &key, &inserted);
    if (inserted) jazz_node__node__codec__TouchedRows__default(rows);
    jazz_model_BranchKey branch_key = jazz_node__node__codec__VersionRow__branch_key(version);
    jazz_node__node__codec__TouchedRows__insert(rows, table_id, jazz_model__branch__BranchKey__canonical_bytes(&branch_key),
        jazz_node__node__codec__VersionRow__row_uuid(version));
    jazz_model_BranchKey_drop(&branch_key);
}

static jz_err flush_tx_touched_rows(NS *self, groove_DatabaseBatch *batch);

/* Apply a batch this node built. Every node batch is applied here, and each
 * one first adds the rows marked since the previous apply to their
 * transactions' `jazz_tx_touched_rows` lists, so a history row is listed no
 * later than the batch that writes it. The list is a superset; a listed row
 * that was never written is skipped on read. */
jz_err jazz_node__node__NodeState__apply_node_batch(NS *self, groove_DatabaseBatch *batch, groove_AppliedBatch **out)
{
    jz_err e = flush_tx_touched_rows(self, batch);
    if (e.code) { groove__db__batch__DatabaseBatch__drop(batch); return e; }
    e = groove__db__commit__Database__apply_batch(self->database, batch, out);
    return e.code ? jazz_node__node__Error_groove(e) : JZ_OK;
}

/* The history rows this node listed for a transaction. */
jz_err jazz_node__node__NodeState__load_tx_touched_rows(const NS *self, const groove_DatabaseBatch *batch /* NULL = None */,
    jazz_types_TxTime tx_time, jazz_types_NodeAlias node_alias, jazz_node_TouchedRows *out)
{
    groove_Value key[2] = { gv_u64(tx_time), gv_u64(node_alias) };
    jz_str table = jz_str_from(JZN_TX_TOUCHED_ROWS_TABLE);
    groove_RawRow *raw = NULL;
    jz_err e = batch
        ? groove__db__primary_storage__Database__primary_key_get_raw_in_batch(self->database, batch, &table, key, 2, &raw)
        : groove__db__primary_storage__Database__primary_key_get_raw(self->database, &table, key, 2, &raw);
    jz_str_drop(&table);
    if (e.code) return jazz_node__node__Error_groove(e);
    if (!raw) { jazz_node__node__codec__TouchedRows__default(out); return JZ_OK; }
    groove_BorrowedRecord record = groove__records__record__OwnedRecord__borrowed(groove__db__RawRow__record(raw));
    e = jazz_node__node__codec__TouchedRows__from_list_record(&record, out);
    groove__db__RawRow__drop(raw);
    return e;
}

/* Extend each marked transaction's list (as staged in this batch) with the
 * rows marked since the last flush. */
static jz_err flush_tx_touched_rows(NS *self, groove_DatabaseBatch *batch)
{
    if (self->tx_touched_dirty.len == 0) return JZ_OK;
    jz_btree dirty = self->tx_touched_dirty;
    jz_btree_init(&self->tx_touched_dirty, dirty.d);
    jz_err e = JZ_OK;
    const jazz_node_TxTouchedKey *key; const jazz_node_TouchedRows *rows;
    JZ_BTREE_FOR(&dirty, key, rows) {
        jazz_node_TouchedRows stored;
        if ((e = jazz_node__node__NodeState__load_tx_touched_rows(self, batch, key->tx_time, key->node_alias, &stored)).code) break;
        jazz_node_TouchedRows touched;
        jazz_node__node__codec__TouchedRows__clone(&touched, &stored);
        jazz_node__node__codec__TouchedRows__extend(&touched, rows);
        bool unchanged = jazz_node__node__codec__TouchedRows__eq(&touched, &stored);
        groove_ValueVec values;
        if (!unchanged) e = jazz_node__node__codec__TouchedRows__list_record_values(&touched, key->tx_time, key->node_alias, &values);
        jazz_node__node__codec__TouchedRows__drop(&touched);
        jazz_node__node__codec__TouchedRows__drop(&stored);
        if (e.code) break;
        if (unchanged) continue;
        jz_str table = jz_str_from(JZN_TX_TOUCHED_ROWS_TABLE);
        groove__db__batch__DatabaseBatch__update(batch, &table, values);
        jz_str_drop(&table);
    }
    jz_btree_drop(&dirty);
    return e;
}

bool jazz_node__node__NodeState__cached_tx_versions(const NS *self, jazz_model_TxId tx_id, jazz_node_VersionRowVec *out)
{
    const jazz_node_CachedTransactionVersions *cached = jz_btree_get(&self->query.tx_versions_cache, &tx_id);
    if (!cached) return false;
    version_row_vec_clone(out, &cached->versions);
    return true;
}

static void touch_tx_version_cache_entry(NS *self, jazz_model_TxId tx_id)
{
    if (!jz_btree_insert(&self->query.tx_version_tables_cache_order_set, &tx_id, NULL, NULL))
        jz_deque_push_back(&self->query.tx_version_tables_cache_order, &tx_id);
}

static void bound_tx_version_cache(NS *self)
{
    while (self->query.tx_versions_cache.len > JZN_TX_VERSION_TABLE_CACHE_MAX_ENTRIES) {
        jazz_model_TxId oldest;
        if (!jz_deque_pop_front(&self->query.tx_version_tables_cache_order, &oldest)) break;
        if (!jz_btree_remove(&self->query.tx_version_tables_cache_order_set, &oldest, NULL, NULL)) continue;
        jz_btree_remove(&self->query.tx_versions_cache, &oldest, NULL, NULL);
    }
}

void jazz_node__node__NodeState__cache_tx_versions(NS *self, jazz_model_TxId tx_id, jazz_node_VersionRowVec versions)
{
    touch_tx_version_cache_entry(self, tx_id);
    jazz_node_CachedTransactionVersions entry = jazz_node__node__CachedTransactionVersions__new(versions);
    jazz_node_CachedTransactionVersions old;
    if (jz_btree_insert(&self->query.tx_versions_cache, &tx_id, &entry, &old)) jazz_node_CachedTransactionVersions_drop(&old);
    bound_tx_version_cache(self);
}

void jazz_node__node__NodeState__invalidate_tx_version_tables_cache(NS *self, jazz_model_TxId tx_id)
{
    jz_btree_remove(&self->query.tx_versions_cache, &tx_id, NULL, NULL);
    jz_btree_remove(&self->query.tx_version_tables_cache_order_set, &tx_id, NULL, NULL);
}

static jazz_node_CurrentRow materialize_current_row(NS *self, const jazz_model_TableSchema *table, jazz_node_CurrentRow row)
{
    (void)self; (void)table;
    return row;
}

jz_err jazz_node__node__NodeState__current_row_from_materialized_version(NS *self, const jazz_model_TableSchema *table,
    const jazz_node_VersionRow *version, jazz_node_CurrentRow *out)
{
    (void)self;
    return jazz_node__node__codec__current_row_from_version_projection(table, version, out);
}

jz_err jazz_node__node__NodeState__materialized_cells_for_version(const NS *self, const jazz_model_TableSchema *table,
    const jazz_node_VersionRow *version, jz_btree *out)
{
    (void)self;
    return jazz_node__node__codec__VersionRow__cells(version, table, out);
}

jz_err jazz_node__node__NodeState__local_current_row(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, bool *has, jazz_node_CurrentRow *out)
{
    return jazz_node__node__NodeState__local_current_row_in_schema(self, table, row_uuid, self->catalogue.active_schema.schema, has, out);
}

/* Resolve an engine-owned indirect descriptor from the current physical row.
 * Callers must perform ordinary Jazz row authorization first; the descriptor
 * never crosses the public API boundary. */
jz_err jazz_node__node__NodeState__current_physical_cell_in_schema(NS *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, jazz_types_RowUuid row_uuid, const jz_str *column, bool *has, groove_Value *out)
{
    *has = false;
    bool has_cells; jz_btree cells;
    JZ_TRY(jazz_node__node__NodeState__current_physical_cells_in_schema(self, schema_version, table, row_uuid, &has_cells, &cells));
    if (!has_cells) return JZ_OK;
    jz_str key;
    *has = jz_btree_remove(&cells, column, &key, out);
    if (*has) jz_str_drop(&key);
    jz_btree_drop(&cells);
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__current_physical_cells_in_schema(NS *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, jazz_types_RowUuid row_uuid, bool *has, jz_btree *out)
{
    jazz_model_BranchSelector branch; jazz_model_BranchSelector_default(&branch);
    jz_err e = jazz_node__node__NodeState__current_physical_cells_in_branch_schema(self, schema_version, table, &branch, row_uuid, has, out);
    jazz_model_BranchSelector_drop(&branch);
    return e;
}

jz_err jazz_node__node__NodeState__current_physical_cells_and_winner_in_schema(NS *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, jazz_types_RowUuid row_uuid, bool *has, jz_btree *cells, jazz_model_TxId *winner)
{
    jazz_model_BranchSelector branch; jazz_model_BranchSelector_default(&branch);
    jz_err e = jazz_node__node__NodeState__visible_current_physical_cells_and_winner_in_branch_schema(self, schema_version, table, &branch,
        row_uuid, has, cells, winner);
    jazz_model_BranchSelector_drop(&branch);
    return e;
}

jz_err jazz_node__node__NodeState__current_physical_cells_in_branch_schema(NS *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, const jazz_model_BranchSelector *branch, jazz_types_RowUuid row_uuid, bool *has, jz_btree *out)
{
    return jazz_node__node__NodeState__visible_current_physical_cells_in_branch_schema(self, schema_version, table, branch, row_uuid, has, out);
}

/* Seal several independently staged large values into one ordinary row
 * commit: every indirect descriptor in the patch must be one of the freshly
 * staged targets, and all remaining descriptors must be inherited verbatim
 * from the observed row. */
jz_err jazz_node__node__NodeState__seal_large_value_updates(NS *self, jazz_node_MergeableCommit commit,
    const jazz_node_NamedStagedLargeValue *staged, u32 n_staged, jazz_types_SchemaVersionId schema_version, jazz_node_MergeableCommit *out)
{
    bool has_inherited; jz_btree inherited;
    jz_err e = jazz_node__node__NodeState__current_physical_cells_in_branch_schema(self, schema_version, &commit.table, &commit.branch,
        commit.row_uuid, &has_inherited, &inherited);
    if (!e.code && !has_inherited) e = JZN_INVALID_MERGEABLE("partial large-value update target is not observed");
    if (e.code) { jazz_node__node__MergeableCommit__drop(&commit); return e; }
    const jz_str *column; const groove_Value *value;
    JZ_BTREE_FOR(&commit.cells, column, value) {
        if (!jazz_node__node__value_contains_indirect_descriptor(value)) continue;
        const groove_LargeValueRef *target = NULL;
        for (u32 i = 0; i < n_staged; ++i) if (jz_str_eq(&staged[i].column, column)) { target = staged[i].staged.value_ref; break; }
        bool valid;
        if (target) {
            groove_LargeValueRefVec descriptors = {0};
            jazz_node__node__collect_indirect_descriptors(value, &descriptors);
            valid = descriptors.len == 1 && groove_LargeValueRef_eq(descriptors.ptr[0], target);
            groove_LargeValueRefVec_drop(&descriptors);
        } else {
            const groove_Value *prior = jz_btree_get(&inherited, column);
            valid = prior && groove_Value_eq(prior, value);
        }
        if (!valid) {
            jz_btree_drop(&inherited); jazz_node__node__MergeableCommit__drop(&commit);
            return JZN_INVALID_MERGEABLE("partial large-value update contains an unverified descriptor");
        }
        jz_str c = jz_str_clone(column);
        jz_btree_insert(&commit.prepared_large_columns, &c, NULL, NULL);
    }
    for (u32 i = 0; i < n_staged; ++i) JZ_VEC_PUSH(commit.staged_large_values, staged[i].staged.id);
    jz_btree_drop(&inherited);
    *out = commit;
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__seal_inherited_large_values(NS *self, jazz_node_MergeableCommit commit,
    jazz_types_SchemaVersionId schema_version, bool allow_inherited_descriptors, jazz_node_MergeableCommit *out)
{
    bool any = false;
    const jz_str *column; const groove_Value *value;
    JZ_BTREE_FOR(&commit.cells, column, value) if (jazz_node__node__value_contains_indirect_descriptor(value)) { any = true; break; }
    if (!any) { *out = commit; return JZ_OK; }
    if (!allow_inherited_descriptors) {
        jazz_node__node__MergeableCommit__drop(&commit);
        return JZN_INVALID_MERGEABLE("complete row replacement contains an unverified large-value descriptor");
    }
    bool has_inherited; jz_btree inherited;
    jz_err e = jazz_node__node__NodeState__current_physical_cells_in_branch_schema(self, schema_version, &commit.table, &commit.branch,
        commit.row_uuid, &has_inherited, &inherited);
    if (e.code) { jazz_node__node__MergeableCommit__drop(&commit); return e; }
    if (!has_inherited) jz_btree_init(&inherited, &jazz_node_STRING_VALUE_MAP_DESC);
    JZ_BTREE_FOR(&commit.cells, column, value) {
        if (!jazz_node__node__value_contains_indirect_descriptor(value)) continue;
        /* A branch-view write can carry an unchanged descriptor from a locally
         * read base snapshot while the target branch has no physical row yet;
         * that exact cell is marked privately. Every other descriptor still
         * has to equal the target branch's observed physical cell. */
        const groove_Value *prior = jz_btree_get(&inherited, column);
        if (!(prior && groove_Value_eq(prior, value)) && !jz_btree_contains(&commit.prepared_large_columns, column)) {
            jz_btree_drop(&inherited); jazz_node__node__MergeableCommit__drop(&commit);
            return JZN_INVALID_MERGEABLE("row update contains an unverified large-value descriptor");
        }
        jz_str c = jz_str_clone(column);
        if (jz_btree_insert(&commit.prepared_large_columns, &c, NULL, NULL)) {}
    }
    jz_btree_drop(&inherited);
    *out = commit;
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__local_current_row_in_schema(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid,
    jazz_types_SchemaVersionId schema_version, bool *has, jazz_node_CurrentRow *out)
{
    *has = false;
    jazz_model_TableSchema table_schema;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema(self, table, schema_version, &table_schema));
    bool found, deleted; jazz_node_CurrentRow row; jazz_types_TxTime tx_time; jazz_types_NodeUuid tx_node;
    jz_err e = jazz_node__node__NodeState__local_current_content_row_candidate(self, &table_schema, row_uuid, schema_version,
        &found, &row, &tx_time, &tx_node, &deleted);
    if (!e.code && found) {
        if (deleted) jazz_node_CurrentRow_drop(&row);
        else {
            row = materialize_current_row(self, &table_schema, row);
            /* Exact local reads bypass the query terminal's publication binder,
             * but native write merging consumes the same row ABI. */
            e = MAT(NodeState__bind_current_row_columns_in_schema)(self, schema_version, &row);
            if (e.code) jazz_node_CurrentRow_drop(&row);
            else { *out = row; *has = true; }
        }
    }
    jazz_model_TableSchema_drop(&table_schema);
    return e;
}

typedef struct ContentScanCtx { NS *self; jazz_types_SchemaVersionId schema_version; const jz_str *table; const groove_ValueVec *prefix; } ContentScanCtx;

static jz_err content_scan_layer(const jazz_node_LayerRef *r, void *vctx, jazz_node_Layer *out)
{
    ContentScanCtx *c = vctx;
    groove_StaticScanSpec scan = { .tag = jazz_node__node__layer_stack__LayerRef__is_local(r) ? GROOVE_SSS_PREFIX : GROOVE_SSS_POINT };
    JZ_VEC_FOR(const groove_Value, v, *c->prefix) { groove_Value copy; groove_Value_clone(&copy, v); JZ_VEC_PUSH(scan.values, copy); }
    groove_GraphBuilder *graph;
    JZ_TRY(jazz_node__node__physical__NodeState__physical_current_source_scan_graph(c->self, c->schema_version, c->table,
        jazz_node__node__layer_stack__LayerRef__physical_class(r), scan, &graph));
    *out = jazz_node__node__layer_stack__Layer__new(graph);
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__local_current_content_row_candidate(NS *self, const jazz_model_TableSchema *table, jazz_types_RowUuid row_uuid,
    jazz_types_SchemaVersionId schema_version, bool *found, jazz_node_CurrentRow *row_out, jazz_types_TxTime *tx_time, jazz_types_NodeUuid *tx_node, bool *deleted_out)
{
    *found = false;
    groove_ValueVec prefix = {0};
    JZ_VEC_PUSH(prefix, gv_uuid(row_uuid));
    /* The row's `[local, origin]` stack, both refs read at the row's key. */
    ContentScanCtx ctx = { self, schema_version, &table->name, &prefix };
    jazz_node_LayerVec layers;
    jz_err e = jazz_node__node__layer_stack__try_ref_layers(JZ_DURABILITY_LOCAL, content_scan_layer, &ctx, &layers);
    jz_free(prefix.ptr);
    if (e.code) return e;
    groove_RecordDeltas *result;
    e = groove__db__query__Database__query_graph(self->database, jazz_node__node__layer_stack__compose(layers), &result);
    if (e.code) return jazz_node__node__NodeState__malformed_current_query_error(&table->name, row_uuid, e);
    groove_RecordDeltasIter it = { result, 0 };
    groove_BorrowedRecord record; i64 weight; bool any = false;
    while (groove__ivm__RecordDeltas__next(&it, &record, &weight)) if (weight > 0) { any = true; break; }
    if (!any) { groove_RecordDeltas_drop(result); return JZ_OK; }
    const groove_RecordDescriptor *descriptor = groove__ivm__RecordDeltas__descriptor(result);
    bool deleted = false;
    u32 field;
    if (groove__records__descriptor__RecordDescriptor__field_index(descriptor, JZN_DELETION_COLUMN_NAME, &field)) {
        bool some; u8 tag;
        e = groove__records__BorrowedRecord__get_nullable_enum(&record, field, &some, &tag);
        if (e.code) {
            groove_RecordDeltas_drop(result);
            return jazz_node__node__NodeState__malformed_current_query_error(&table->name, row_uuid, groove__db__Error__record_encoding(e));
        }
        if (some) {
            jazz_model_DeletionEvent event;
            jz_err de = jazz_model__tx__deletion_event_from_value(gv_enum_tag(tag), &event);
            if (de.code) jz_err_drop(&de);
            else deleted = event == JZ_DELETION_DELETED;
        }
    }
    e = jazz_node__node__NodeState__current_record_sort_key(self, &table->name, row_uuid, &record, tx_time, tx_node);
    if (!e.code) e = jazz_node__node__decode_current_row(table, &record, row_out);
    groove_RecordDeltas_drop(result);
    if (e.code) return e;
    *deleted_out = deleted;
    *found = true;
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__current_record_sort_key(const NS *self, const jz_str *table, jazz_types_RowUuid row_uuid,
    const groove_BorrowedRecord *record, jazz_types_TxTime *tx_time, jazz_types_NodeUuid *tx_node)
{
    u64 time, alias;
    jz_err e = groove__records__record__BorrowedRecord__get_u64(record, JZN_GLOBAL_CURRENT_ROW_FIELD_TX_TIME_IDX, &time);
    if (e.code) return jazz_node__node__Error_malformed_current_row(table, row_uuid, e);
    e = groove__records__record__BorrowedRecord__get_u64(record, JZN_GLOBAL_CURRENT_ROW_FIELD_TX_NODE_ID_IDX, &alias);
    if (e.code) return jazz_node__node__Error_malformed_current_row(table, row_uuid, e);
    if (!jazz_node__node__NodeAliases__node_for_alias(self->node_aliases, alias, tx_node))
        return JZN_INVALID_STORED("current row references unknown node alias");
    *tx_time = time;
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__malformed_current_query_error(const jz_str *table, jazz_types_RowUuid row_uuid, jz_err error)
{
    jz_err source;
    if (!groove__db__Error__record_encoding_source(&error, &source)) return jazz_node__node__Error_groove(error);
    jz_err_drop(&error);
    return jazz_node__node__Error_malformed_current_row(table, row_uuid, source);
}

/* ---- coalesce_same_row_commits ---- */
typedef struct RowKey { jz_str table; jazz_types_RowUuid row_uuid; } RowKey;
typedef JZ_VEC(u32) IndexVec;
static int row_key_cmp(const void *a, const void *b, void *ctx)
{
    const RowKey *x = a, *y = b;
    int c = jz_cmp_str(&x->table, &y->table, ctx);
    return c ? c : jz_uuid_cmp(&x->row_uuid, &y->row_uuid);
}
static void row_key_drop(void *k) { jz_str_drop(&((RowKey *)k)->table); }
static void index_vec_drop(void *v) { jz_free(((IndexVec *)v)->ptr); }
static const jz_btree_desc BY_ROW_DESC = { sizeof(RowKey), sizeof(IndexVec), row_key_cmp, row_key_drop, index_vec_drop };

/* Merge commits addressing the same row of the same schema/branch into one
 * row-image commit, in authoring order: later cells and deletion events win,
 * authored column sets are unioned. */
static jazz_node_SchemaCommitVec coalesce_same_row_commits(jazz_node_SchemaCommitVec commits)
{
    if (commits.len < 2) return commits;
    jz_btree by_row; jz_btree_init(&by_row, &BY_ROW_DESC);
    jazz_node_SchemaCommitVec merged = {0};
    jz_vec_reserve(&merged, commits.len, sizeof(jazz_node_SchemaCommit));
    for (u32 c = 0; c < commits.len; ++c) {
        jazz_types_SchemaVersionId schema_version = commits.ptr[c].schema;
        jazz_node_MergeableCommit commit = commits.ptr[c].commit;
        RowKey key = { jz_str_clone(&commit.table), commit.row_uuid };
        bool inserted;
        IndexVec *candidates = jz_btree_entry(&by_row, &key, &inserted);
        if (!inserted) jz_str_drop(&key.table);
        bool found = false; u32 index = 0;
        JZ_VEC_FOR(const u32, i, *candidates) {
            const jazz_node_SchemaCommit *existing = &merged.ptr[*i];
            if (jz_uuid_eq(&existing->schema, &schema_version) && jazz_model_BranchSelector_eq(&existing->commit.branch, &commit.branch)) {
                found = true; index = *i; break;
            }
        }
        if (!found) {
            JZ_VEC_PUSH(*candidates, merged.len);
            JZ_VEC_PUSH(merged, ((jazz_node_SchemaCommit){ schema_version, commit }));
            continue;
        }
        jazz_node_MergeableCommit *base = &merged.ptr[index].commit;
        jz_btree base_authored;
        if (base->has_authored_columns) { base_authored = base->authored_columns; base->has_authored_columns = false; }
        else string_keys_into_set(&base->cells, &base_authored);
        jz_btree next_authored;
        if (commit.has_authored_columns) jz_btree_clone(&next_authored, &commit.authored_columns, str_kclone, NULL);
        else string_keys_into_set(&commit.cells, &next_authored);
        jz_str column;
        while (jz_btree_pop_first(&next_authored, &column, NULL)) {
            if (jz_btree_insert(&base_authored, &column, NULL, NULL)) {}
        }
        jz_btree_drop(&next_authored);
        base->has_authored_columns = true; base->authored_columns = base_authored;
        /* Content written after a delete in the same transaction resurrects
         * the row: the later write of the row wins. */
        bool resurrects = !commit.has_deletion && base->has_deletion && base->deletion == JZ_DELETION_DELETED;
        jz_btree_append(&base->cells, &commit.cells);
        if (resurrects) { base->has_deletion = true; base->deletion = JZ_DELETION_RESTORED; }
        else if (commit.has_deletion) { base->has_deletion = true; base->deletion = commit.deletion; }
        if (commit.now_ms > base->now_ms) base->now_ms = commit.now_ms;
        base->known_fresh_row = base->known_fresh_row && commit.known_fresh_row;
        jz_btree_append(&base->prepared_large_columns, &commit.prepared_large_columns);
        JZ_VEC_FOR(const groove_StagedLargeValueId, id, commit.staged_large_values) JZ_VEC_PUSH(base->staged_large_values, *id);
        if (commit.has_user_metadata_json) {
            if (base->has_user_metadata_json) jz_str_drop(&base->user_metadata_json);
            base->has_user_metadata_json = true; base->user_metadata_json = commit.user_metadata_json;
            commit.has_user_metadata_json = false;
        }
        jazz_node__node__MergeableCommit__drop(&commit);
    }
    jz_btree_drop(&by_row);
    jz_free(commits.ptr);
    return merged;
}
