/* maintained_subscription_view.c: receiver-local maintained view over Groove
 * multisink terminal output.
 * Rust: crates/jazz/layers/node/src/node/maintained_subscription_view.rs
 *
 * Skipped: everything under #[cfg(test)] (covered_input_for_version,
 * apply_decoded_deltas, structured_app_row(s), assert_* footprint oracles,
 * replacement_map_bytes, test counters), the `testing` feature
 * (compiled_authorization_mode, MaintainedTerminalSchemas::footprint) and the
 * cold-settle-attribution spans/conversions. Debug derives are not ported,
 * except where the covered-input trace prints them (rendered through the
 * mirrored protocol Debug helpers). */
#include "maintained_subscription_view.h"

typedef jazz_node_MaintainedSubscriptionView MSVT;
typedef jazz_protocol_ResultMemberEntry RME;
typedef jazz_protocol_ResultMemberPayloadEntry RMPE;
typedef jazz_protocol_ProgramFactEntry PFE;
typedef jazz_protocol_SupportingRow SROW;
typedef jazz_node_VersionRow VROW;

/* ======================= small helpers ======================= */
static int jzn_intern_cmp(groove_InternStr a, groove_InternStr b) { return a == b ? 0 : jz_str_cmp(a, b); }
static int jzn_bytes_cmp_v(const void *a, const void *b, void *ctx) { (void)ctx; return jz_bytes_cmp(a, b); }
static void jzn_bytes_drop_v(void *p) { jz_bytes_drop(p); }
static void jzn_str_drop_v(void *p) { jz_str_drop(p); }
static void jzn_btree_drop_v(void *p) { jz_btree_drop(p); }
static int jzn_i64_cmp(i64 a, i64 b) { return a < b ? -1 : a > b; }

static jzn_ArcBytes *jzn_arc_bytes_new(jz_slice raw) {
    jzn_ArcBytes *a = jz_rc_new(sizeof(jzn_ArcBytes), jzn_bytes_drop_v);
    *a = jz_bytes_from(raw.ptr, raw.len);
    return a;
}

static const jz_btree_desc jzn_desc_bytes_set = { sizeof(jz_bytes), 0, jzn_bytes_cmp_v, jzn_bytes_drop_v, NULL };
static const jz_btree_desc jzn_desc_bytes_i64 = { sizeof(jz_bytes), sizeof(i64), jzn_bytes_cmp_v, jzn_bytes_drop_v, NULL };
static const jz_btree_desc jzn_desc_bytes_rows = { sizeof(jz_bytes), sizeof(jz_btree), jzn_bytes_cmp_v, jzn_bytes_drop_v, jzn_btree_drop_v };
static const jz_btree_desc jzn_desc_bytes_uuid = { sizeof(jz_bytes), sizeof(jz_uuid), jzn_bytes_cmp_v, jzn_bytes_drop_v, NULL };
static const jz_btree_desc jzn_desc_bytes_terminal_record = { sizeof(jz_bytes), sizeof(jazz_node_TerminalRecordState *), jzn_bytes_cmp_v,
    jzn_bytes_drop_v, jazz_node__node__terminal_record__TerminalRecordState__drop_box };

/* witness_table_names: (String, SchemaVersionAlias) -> String */
typedef struct jzn_WitnessNameKey { jz_str table; jazz_types_SchemaVersionAlias alias; } jzn_WitnessNameKey;
static int jzn_witness_name_key_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const jzn_WitnessNameKey *x = a, *y = b;
    int c = jz_str_cmp(&x->table, &y->table);
    return c ? c : (x->alias < y->alias ? -1 : x->alias > y->alias);
}
static void jzn_witness_name_key_drop(void *p) { jz_str_drop(&((jzn_WitnessNameKey *)p)->table); }
const jz_btree_desc jzn_desc_witness_table_names = { sizeof(jzn_WitnessNameKey), sizeof(jz_str), jzn_witness_name_key_cmp,
    jzn_witness_name_key_drop, jzn_str_drop_v };

/* physical_tables: Intern<String> -> GlobalPhysicalTableId */
static int jzn_intern_cmp_v(const void *a, const void *b, void *ctx) {
    (void)ctx;
    return jzn_intern_cmp(*(const groove_InternStr *)a, *(const groove_InternStr *)b);
}
const jz_btree_desc jzn_desc_physical_tables = { sizeof(groove_InternStr), sizeof(jazz_types_GlobalPhysicalTableId), jzn_intern_cmp_v, NULL, NULL };

static const jz_btree_desc jzn_desc_rme_set = { sizeof(RME), 0, jazz_protocol__protocol__ResultMemberEntry__cmp,
    jazz_protocol__protocol__ResultMemberEntry__drop, NULL };
static const jz_btree_desc jzn_desc_rme_i64 = { sizeof(RME), sizeof(i64), jazz_protocol__protocol__ResultMemberEntry__cmp,
    jazz_protocol__protocol__ResultMemberEntry__drop, NULL };
static const jz_btree_desc jzn_desc_rme_payload = { sizeof(RME), sizeof(RMPE), jazz_protocol__protocol__ResultMemberEntry__cmp,
    jazz_protocol__protocol__ResultMemberEntry__drop, jazz_protocol__protocol__ResultMemberPayloadEntry__drop };
static const jz_btree_desc jzn_desc_pfe_i64 = { sizeof(PFE), sizeof(i64), jazz_protocol__protocol__ProgramFactEntry__cmp,
    jazz_protocol__protocol__ProgramFactEntry__drop, NULL };
const jz_btree_desc jzn_desc_selected_deletion_witnesses = { sizeof(SROW), sizeof(VROW), jazz_protocol__protocol__SupportingRow__cmp,
    jazz_protocol__protocol__SupportingRow__drop, jazz_node_VersionRow_drop };

static void jzn_rme_vec_drop(jazz_protocol_ResultMemberEntryVec *v) { jz_vec_drop(v, sizeof(RME), jazz_protocol__protocol__ResultMemberEntry__drop); }
static void jzn_pfe_vec_drop(jazz_protocol_ProgramFactEntryVec *v) { jz_vec_drop(v, sizeof(PFE), jazz_protocol__protocol__ProgramFactEntry__drop); }
static void jzn_pair_drop(void *p) {
    jazz_protocol_MemberPayloadPair *x = p;
    jazz_protocol__protocol__ResultMemberEntry__drop(&x->member);
    jazz_protocol__protocol__ResultMemberPayloadEntry__drop(&x->payload);
}
static void jzn_pair_vec_drop(jazz_protocol_MemberPayloadPairVec *v) { jz_vec_drop(v, sizeof(jazz_protocol_MemberPayloadPair), jzn_pair_drop); }
static RME jzn_rme_clone(const RME *m) { RME out; jazz_protocol__protocol__ResultMemberEntry__clone(&out, m); return out; }
static RMPE jzn_rmpe_clone(const RMPE *p) { RMPE out; jazz_protocol__protocol__ResultMemberPayloadEntry__clone(&out, p); return out; }
static PFE jzn_pfe_clone(const PFE *p) { PFE out; jazz_protocol__protocol__ProgramFactEntry__clone(&out, p); return out; }
static VROW jzn_vrow_clone(const VROW *v) { VROW out; jazz_node_VersionRow_clone(&out, v); return out; }

static bool jzn_trace(void) { return jazz_node__debug_env__covered_input_trace(); }

/* ======================= byte accounting ======================= */
static usize jzn_btree_map_bytes(usize len) { return len * 96; }
static usize jzn_btree_set_bytes(usize len) { return len * 64; }
static usize jzn_intern_string_bytes(groove_InternStr value) { return sizeof(groove_InternStr) + value->len; }
static usize jzn_vec_bytes(usize len, usize elem) { return sizeof(jz_vec) + len * elem; }
static usize jzn_option_vec_bytes(const jazz_protocol_OptBytes *v) { return v->some ? jzn_vec_bytes(v->v.len, 1) : 0; }

static usize jzn_result_member_entry_bytes(const RME *member) {
    usize extra = 0;
    switch (member->tag) {
    case JAZZ_RME_ROW:
    case JAZZ_RME_TYPED_ROW: {
        const jazz_protocol_RealRowMemberEntry *row = member->tag == JAZZ_RME_ROW ? &member->u.row : &member->u.typed.row;
        extra = jzn_intern_string_bytes(row->table) + jzn_option_vec_bytes(&row->branch_or_prefix) + jzn_option_vec_bytes(&row->row_digest);
        break;
    }
    case JAZZ_RME_SYNTHETIC:
        extra = member->u.synthetic.table.len + jzn_vec_bytes(member->u.synthetic.row.len, 1) + sizeof(member->u.synthetic.replacement);
        break;
    case JAZZ_RME_PATH_TUPLE:
        extra = member->u.path_tuple.path.len + jzn_intern_string_bytes(member->u.path_tuple.source_table) +
                jzn_intern_string_bytes(member->u.path_tuple.target_table) + jzn_option_vec_bytes(&member->u.path_tuple.edge_id) +
                jzn_vec_bytes(member->u.path_tuple.revision.len, 1);
        break;
    }
    return sizeof(*member) + extra;
}

static usize jzn_result_member_payload_entry_bytes(const RMPE *payload) {
    return sizeof(*payload) + jzn_result_member_entry_bytes(&payload->member) + jzn_vec_bytes(payload->descriptor.len, 1) +
           jzn_vec_bytes(payload->record.len, 1);
}

static usize jzn_version_identity_bytes(const jzn_VersionIdentity *identity) {
    return sizeof(*identity) + jzn_intern_string_bytes(identity->table) + identity->raw_record->len;
}
static usize jzn_version_sort_key_bytes(const jzn_VersionSortKey *sort_key) {
    return sizeof(*sort_key) + jzn_intern_string_bytes(sort_key->table) + sort_key->raw_record->len;
}
static usize jzn_replacement_key_bytes(const jzn_ReplacementKey *key) { return sizeof(*key) + jzn_intern_string_bytes(key->table); }
static usize jzn_version_row_bytes(const VROW *row) { return sizeof(*row) + jzn_intern_string_bytes(row->table) + row->record.raw.len; }
static usize jzn_weighted_version_bytes(const jzn_WeightedVersion *version) {
    return sizeof(*version) + sizeof(jzn_VersionPayload) + jzn_version_row_bytes(&version->payload->row) +
           jzn_version_sort_key_bytes(&version->payload->sort_key);
}

/* ======================= identities ======================= */
static u32 jzn_witness_layer_of(const VROW *row) {
    return jazz_node__node__codec__VersionRow__deletion(row) != JZN_DELETION_NONE ? JZN_WL_DELETION : JZN_WL_CONTENT;
}

static int jzn_version_identity_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const jzn_VersionIdentity *x = a, *y = b;
    int c = jzn_intern_cmp(x->table, y->table);
    if (c) return c;
    if (x->layer != y->layer) return x->layer < y->layer ? -1 : 1;
    return jz_bytes_cmp(x->raw_record, y->raw_record);
}
static void jzn_version_identity_drop(void *p) { jz_rc_release(((jzn_VersionIdentity *)p)->raw_record); }
static jzn_VersionIdentity jzn_version_identity_clone(const jzn_VersionIdentity *i) {
    jzn_VersionIdentity out = *i;
    out.raw_record = jz_rc_clone(i->raw_record);
    return out;
}

static int jzn_version_sort_key_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const jzn_VersionSortKey *x = a, *y = b;
    int c = jzn_intern_cmp(x->table, y->table);
    if (c) return c;
    c = jz_uuid_cmp(&x->row_uuid, &y->row_uuid);
    if (c) return c;
    if (x->layer != y->layer) return x->layer < y->layer ? -1 : 1;
    return jz_bytes_cmp(x->raw_record, y->raw_record);
}
static void jzn_version_sort_key_drop(void *p) { jz_rc_release(((jzn_VersionSortKey *)p)->raw_record); }

static int jzn_replacement_key_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const jzn_ReplacementKey *x = a, *y = b;
    int c = jzn_intern_cmp(x->table, y->table);
    if (c) return c;
    c = jz_uuid_cmp(&x->row_uuid, &y->row_uuid);
    if (c) return c;
    return x->layer == y->layer ? 0 : (x->layer < y->layer ? -1 : 1);
}

/* VersionIdentity::for_row */
static jzn_VersionIdentity jzn_version_identity_for_row(const VROW *row) {
    jzn_VersionIdentity id;
    id.table = row->table;
    id.layer = jzn_witness_layer_of(row);
    id.raw_record = jzn_arc_bytes_new(jz_bytes_slice(&row->record.raw));
    return id;
}
/* VersionSortKey::for_row */
static jzn_VersionSortKey jzn_version_sort_key_for_row(const VROW *row, const jzn_VersionIdentity *identity) {
    jzn_VersionSortKey k;
    k.table = row->table;
    k.row_uuid = jazz_node__node__codec__VersionRow__row_uuid(row);
    k.layer = jzn_witness_layer_of(row);
    k.raw_record = jz_rc_clone(identity->raw_record);
    return k;
}
/* ReplacementKey::for_row */
static jzn_ReplacementKey jzn_replacement_key_for_row(const VROW *row) {
    jzn_ReplacementKey k;
    k.table = row->table;
    k.row_uuid = jazz_node__node__codec__VersionRow__row_uuid(row);
    k.layer = jzn_witness_layer_of(row);
    return k;
}

/* ======================= VersionPayload / WeightedVersion ======================= */
static void jzn_version_payload_drop(void *p) {
    jzn_VersionPayload *v = p;
    jazz_node_VersionRow_drop(&v->row);
    jzn_version_sort_key_drop(&v->sort_key);
}

/* VersionPayload::prepare */
static jz_err jzn_version_payload_prepare(VROW row, const jzn_VersionIdentity *identity, const jazz_node_NodeAliases *node_aliases,
                                          jzn_VersionPayload **out) {
    jazz_types_TxId tx_id;
    if (!jazz_node__node__codec__version_tx_id_from_aliases(&row, node_aliases, &tx_id)) {
        jazz_node_VersionRow_drop(&row);
        return JZN_ISV("history tx node alias must exist");
    }
    jzn_VersionPayload *p = jz_rc_new(sizeof(jzn_VersionPayload), jzn_version_payload_drop);
    p->sort_key = jzn_version_sort_key_for_row(&row, identity);
    p->row = row;
    p->tx_id = tx_id;
    *out = p;
    return JZ_OK;
}

static void jzn_weighted_version_drop(void *p) { jz_rc_release(((jzn_WeightedVersion *)p)->payload); }
static void jzn_weighted_version_clone(void *dst, const void *src) {
    const jzn_WeightedVersion *s = src;
    jzn_WeightedVersion *d = dst;
    d->payload = jz_rc_clone(s->payload);
    d->weight = s->weight;
}

static const jz_btree_desc jzn_desc_sort_key_versions = { sizeof(jzn_VersionSortKey), sizeof(jzn_WeightedVersion), jzn_version_sort_key_cmp,
    jzn_version_sort_key_drop, jzn_weighted_version_drop };
static const jz_btree_desc jzn_desc_tx_rows = { sizeof(jazz_types_TxId), sizeof(jz_btree), jazz_types__ids__TxId__cmp, NULL, jzn_btree_drop_v };
static const jz_btree_desc jzn_desc_identity_versions = { sizeof(jzn_VersionIdentity), sizeof(jzn_WeightedVersion), jzn_version_identity_cmp,
    jzn_version_identity_drop, jzn_weighted_version_drop };
static const jz_btree_desc jzn_desc_replacement_rows = { sizeof(jzn_ReplacementKey), sizeof(jz_btree), jzn_replacement_key_cmp, NULL,
    jzn_btree_drop_v };

/* ======================= WeightedVersionIndex ======================= */
static void jzn_wvi_default(jzn_WeightedVersionIndex *x) {
    jz_btree_init(&x->by_tx, &jzn_desc_tx_rows);
    x->entry_count = 0;
    x->entry_bytes = 0;
}

static bool jzn_wvi_has_content_witness(const jzn_WeightedVersionIndex *x, jazz_types_TxId tx_id, groove_InternStr table, jazz_types_RowUuid row_uuid) {
    const jz_btree *rows = jz_btree_get(&x->by_tx, &tx_id);
    if (!rows) return false;
    /* Empty bytes are the inclusive lower bound of all complete record
     * identities at this prefix. Inspect only its first candidate; content
     * records sort before deletion records and only positive weights remain
     * in this index. */
    jzn_ArcBytes empty = { 0 };
    jzn_VersionSortKey lower = { table, row_uuid, JZN_WL_CONTENT, &empty };
    jz_btree_iter it;
    jz_btree_range(&it, rows, &lower, true, NULL, false);
    jzn_VersionSortKey *key;
    jzn_WeightedVersion *val;
    if (!jz_btree_next(&it, (void **)&key, (void **)&val)) return false;
    return jzn_intern_cmp(key->table, table) == 0 && jz_uuid_eq(&key->row_uuid, &row_uuid) && key->layer == JZN_WL_CONTENT;
}

static usize jzn_wvi_footprint_bytes(const jzn_WeightedVersionIndex *x) {
    return jzn_btree_map_bytes(x->by_tx.len) + jzn_btree_map_bytes(x->entry_count) + x->entry_bytes;
}

/* WeightedVersionIndex::apply_delta (consumes one payload reference) */
static void jzn_wvi_apply_delta(jzn_WeightedVersionIndex *x, jzn_VersionPayload *payload, i64 weight) {
    jazz_types_TxId tx_id = payload->tx_id;
    jz_btree *rows = jz_btree_get(&x->by_tx, &tx_id);
    if (!rows) {
        if (weight <= 0) {
            jz_rc_release(payload);
            return;
        }
        jz_btree fresh;
        jz_btree_init(&fresh, &jzn_desc_sort_key_versions);
        jz_btree_insert(&x->by_tx, &tx_id, &fresh, NULL);
        rows = jz_btree_get(&x->by_tx, &tx_id);
    }
    jzn_WeightedVersion *existing = jz_btree_get(rows, &payload->sort_key);
    if (existing) {
        i64 next = existing->weight + weight;
        x->entry_bytes -= jzn_weighted_version_bytes(existing);
        if (next > 0) {
            jzn_WeightedVersion version = { payload, next };
            x->entry_bytes += jzn_weighted_version_bytes(&version);
            jz_rc_release(existing->payload);
            *existing = version;
        } else {
            x->entry_bytes -= jzn_version_sort_key_bytes(&payload->sort_key);
            x->entry_count -= 1;
            jz_btree_remove(rows, &payload->sort_key, NULL, NULL);
            jz_rc_release(payload);
        }
    } else if (weight > 0) {
        jzn_VersionSortKey key = payload->sort_key;
        key.raw_record = jz_rc_clone(payload->sort_key.raw_record);
        jzn_WeightedVersion version = { payload, weight };
        x->entry_bytes += jzn_version_sort_key_bytes(&key) + jzn_weighted_version_bytes(&version);
        x->entry_count += 1;
        jz_btree_insert(rows, &key, &version, NULL);
    } else {
        jz_rc_release(payload);
    }
    if (rows->len == 0) jz_btree_remove(&x->by_tx, &tx_id, NULL, NULL);
}

/* WeightedVersionIndex::versions_by_tx (rows_by_tx cloned) */
static jazz_node_VersionRowVec jzn_wvi_versions_by_tx(const jzn_WeightedVersionIndex *x, jazz_types_TxId tx_id) {
    jazz_node_VersionRowVec out = { 0 };
    const jz_btree *rows = jz_btree_get(&x->by_tx, &tx_id);
    if (!rows) return out;
    jzn_VersionSortKey *k;
    jzn_WeightedVersion *v;
    JZ_BTREE_FOR(rows, k, v) {
        (void)k;
        JZ_VEC_PUSH(out, jzn_vrow_clone(&v->payload->row));
    }
    return out;
}

/* ======================= ReplacementIndex ======================= */
static void jzn_ri_default(jzn_ReplacementIndex *x) {
    jz_btree_init(&x->content_by_key, &jzn_desc_replacement_rows);
    jz_btree_init(&x->deletion_by_key, &jzn_desc_replacement_rows);
    x->entry_count = x->entry_bytes = x->key_bytes = 0;
}

static usize jzn_ri_footprint_bytes(const jzn_ReplacementIndex *x) {
    return jzn_btree_map_bytes(x->content_by_key.len + x->deletion_by_key.len) + jzn_btree_map_bytes(x->entry_count) + x->key_bytes +
           x->entry_bytes;
}

/* ReplacementIndex::apply_delta (consumes identity and one payload reference) */
static void jzn_ri_apply_delta(jzn_ReplacementIndex *x, jzn_ReplacementKey key, jzn_VersionIdentity identity, jzn_VersionPayload *payload, i64 weight) {
    jz_btree *by_key = key.layer == JZN_WL_CONTENT ? &x->content_by_key : &x->deletion_by_key;
    bool inserted;
    jz_btree *row_versions = jz_btree_entry(by_key, &key, &inserted);
    if (inserted) jz_btree_init(row_versions, &jzn_desc_identity_versions);
    bool was_empty = row_versions->len == 0;
    jzn_WeightedVersion *existing = jz_btree_get(row_versions, &identity);
    if (existing) {
        i64 next = existing->weight + weight;
        x->entry_bytes -= jzn_weighted_version_bytes(existing);
        if (next > 0) {
            jzn_WeightedVersion version = { payload, next };
            x->entry_bytes += jzn_weighted_version_bytes(&version);
            jz_rc_release(existing->payload);
            *existing = version;
        } else {
            x->entry_bytes -= jzn_version_identity_bytes(&identity);
            x->entry_count -= 1;
            jz_btree_remove(row_versions, &identity, NULL, NULL);
            jz_rc_release(payload);
        }
        jzn_version_identity_drop(&identity);
    } else if (weight > 0) {
        jzn_WeightedVersion version = { payload, weight };
        x->entry_bytes += jzn_version_identity_bytes(&identity) + jzn_weighted_version_bytes(&version);
        x->entry_count += 1;
        jz_btree_insert(row_versions, &identity, &version, NULL);
    } else {
        jz_rc_release(payload);
        jzn_version_identity_drop(&identity);
    }
    if (was_empty && row_versions->len != 0) {
        x->key_bytes += jzn_replacement_key_bytes(&key);
    } else if (!was_empty && row_versions->len == 0) {
        x->key_bytes -= jzn_replacement_key_bytes(&key);
    }
    if (row_versions->len == 0) jz_btree_remove(by_key, &key, NULL, NULL);
}

static bool jzn_ri_contains_key(const jzn_ReplacementIndex *x, const jzn_ReplacementKey *key) {
    return key->layer == JZN_WL_CONTENT ? jz_btree_contains(&x->content_by_key, key) : jz_btree_contains(&x->deletion_by_key, key);
}

/* replacement_winner: the positive version with the greatest tx_id (last on ties) */
static bool jzn_replacement_winner(const jz_btree *versions, VROW *out) {
    if (!versions) return false;
    const jzn_WeightedVersion *best = NULL;
    jzn_VersionIdentity *k;
    jzn_WeightedVersion *v;
    JZ_BTREE_FOR(versions, k, v) {
        (void)k;
        if (v->weight <= 0) continue;
        if (!best || jazz_types__ids__TxId__cmp(&v->payload->tx_id, &best->payload->tx_id, NULL) >= 0) best = v;
    }
    if (!best) return false;
    if (out) *out = jzn_vrow_clone(&best->payload->row);
    return true;
}

/* ReplacementIndex::replacement_for -> (content winner, deletion winner) */
static bool jzn_ri_replacement_for(const jzn_ReplacementIndex *x, const jz_str *table, jazz_types_RowUuid row_uuid, VROW *content,
                                   bool *has_deletion, VROW *deletion) {
    groove_InternStr t = groove__Intern__new_str(jz_str_clone(table));
    jzn_ReplacementKey ck = { t, row_uuid, JZN_WL_CONTENT };
    jzn_ReplacementKey dk = { t, row_uuid, JZN_WL_DELETION };
    const jz_btree *c = jz_btree_get(&x->content_by_key, &ck);
    const jz_btree *d = jz_btree_get(&x->deletion_by_key, &dk);
    bool hd = jzn_replacement_winner(d, deletion);
    if (has_deletion) *has_deletion = hd;
    return jzn_replacement_winner(c, content);
}

static usize jzn_ri_entry_count(const jzn_ReplacementIndex *x) { return x->entry_count; }

/* ======================= RetainedResultMap / RetainedResultMembers ======================= */
static void jzn_rrm_default(jzn_RetainedResultMap *m, bool payload_values) {
    jz_btree_init(&m->entries, payload_values ? &jzn_desc_rme_payload : &jzn_desc_rme_i64);
    m->entry_bytes = 0;
    m->positive_count = 0;
    m->payload_values = payload_values;
}
static usize jzn_rrm_value_bytes(const jzn_RetainedResultMap *m, const void *v) {
    return m->payload_values ? jzn_result_member_payload_entry_bytes(v) : sizeof(i64);
}
static usize jzn_rrm_value_positive(const jzn_RetainedResultMap *m, const void *v) {
    return m->payload_values ? 0 : (usize)(*(const i64 *)v > 0);
}

/* RetainedResultMap::insert: returns true and writes `old` (when non-NULL) if a value was replaced. */
static bool jzn_rrm_insert(jzn_RetainedResultMap *m, RME member, const void *value, void *old) {
    usize key_bytes = jzn_result_member_entry_bytes(&member);
    usize value_bytes = jzn_rrm_value_bytes(m, value);
    usize positive = jzn_rrm_value_positive(m, value);
    union { i64 w; RMPE p; } prev;
    bool replaced = jz_btree_insert(&m->entries, &member, value, &prev);
    if (replaced) {
        jazz_protocol__protocol__ResultMemberEntry__drop(&member); /* the existing equal key is kept */
        m->entry_bytes -= jzn_rrm_value_bytes(m, &prev);
        m->positive_count -= jzn_rrm_value_positive(m, &prev);
        if (old) {
            memcpy(old, &prev, m->payload_values ? sizeof(RMPE) : sizeof(i64));
        } else if (m->payload_values) {
            jazz_protocol__protocol__ResultMemberPayloadEntry__drop(&prev.p);
        }
    } else {
        /* Equal keys have equal modeled sizes: the model uses lengths, not
         * vector capacities or allocation addresses. */
        m->entry_bytes += key_bytes;
    }
    m->entry_bytes += value_bytes;
    m->positive_count += positive;
    return replaced;
}

/* RetainedResultMap::remove: returns true and writes the removed value (when non-NULL). */
static bool jzn_rrm_remove(jzn_RetainedResultMap *m, const RME *member, void *out) {
    RME key;
    union { i64 w; RMPE p; } value;
    if (!jz_btree_remove(&m->entries, member, &key, &value)) return false;
    m->entry_bytes -= jzn_result_member_entry_bytes(&key) + jzn_rrm_value_bytes(m, &value);
    m->positive_count -= jzn_rrm_value_positive(m, &value);
    jazz_protocol__protocol__ResultMemberEntry__drop(&key);
    if (out) {
        memcpy(out, &value, m->payload_values ? sizeof(RMPE) : sizeof(i64));
    } else if (m->payload_values) {
        jazz_protocol__protocol__ResultMemberPayloadEntry__drop(&value.p);
    }
    return true;
}

static usize jzn_rrm_footprint_bytes(const jzn_RetainedResultMap *m) { return jzn_btree_map_bytes(m->entries.len) + m->entry_bytes; }

static void jzn_members_default(jzn_RetainedResultMembers *m) {
    jz_btree_init(&m->entries, &jzn_desc_rme_set);
    m->entry_bytes = 0;
}
/* From<BTreeSet<ResultMemberEntry>> */
static void jzn_members_from(jzn_RetainedResultMembers *m, jz_btree entries) {
    m->entries = entries;
    m->entry_bytes = 0;
    RME *k;
    void *v;
    JZ_BTREE_FOR(&m->entries, k, v) {
        (void)v;
        m->entry_bytes += jzn_result_member_entry_bytes(k);
    }
}
/* RetainedResultMembers::insert (consumes member) */
static bool jzn_members_insert(jzn_RetainedResultMembers *m, RME member) {
    usize bytes = jzn_result_member_entry_bytes(&member);
    if (jz_btree_contains(&m->entries, &member)) {
        jazz_protocol__protocol__ResultMemberEntry__drop(&member);
        return false;
    }
    jz_btree_insert(&m->entries, &member, NULL, NULL);
    m->entry_bytes += bytes;
    return true;
}
static bool jzn_members_remove(jzn_RetainedResultMembers *m, const RME *member) {
    if (!jz_btree_remove(&m->entries, member, NULL, NULL)) return false;
    m->entry_bytes -= jzn_result_member_entry_bytes(member);
    return true;
}

static void jzn_journal_drop(jzn_WitnessGatedJournal *j) {
    jz_btree_drop(&j->changed);
    jz_btree_drop(&j->withheld);
}
static void jzn_drop_journal(MSVT *m) {
    if (m->has_witness_gated_journal) jzn_journal_drop(&m->witness_gated_journal);
    m->has_witness_gated_journal = false;
}
static void jzn_drop_unreconciled(MSVT *m) {
    if (m->has_unreconciled_result_members) jz_btree_drop(&m->unreconciled_result_members.entries);
    m->has_unreconciled_result_members = false;
}

/* ======================= MaintainedSubscriptionView: lifecycle ======================= */
void jazz_node_MaintainedSubscriptionView_default(MSVT *m) {
    memset(m, 0, sizeof *m);
    m->client_local_literal_token = NULL;
    jz_btree_init(&m->witness_table_names, &jzn_desc_witness_table_names);
    jzn_rrm_default(&m->result_weights, false);
    jzn_members_default(&m->published_result_members);
    m->has_unreconciled_result_members = false;
    m->has_witness_gated_journal = false;
    jzn_rrm_default(&m->result_payloads, true);
    jzn_rrm_default(&m->published_result_payloads, true);
    jz_btree_init(&m->structured_app_rows, &jzn_desc_bytes_rows);
    jz_btree_init(&m->structured_terminal_records, &jzn_desc_bytes_terminal_record);
    jz_btree_init(&m->structured_root_keys, &jzn_desc_bytes_uuid);
    m->has_structured_app_row_descriptor = false;
    m->retains_structured_app_rows = true;
    m->storage_backed_result_materialization = false;
    jz_btree_init(&m->inline_content_branch_keys, &jzn_desc_bytes_set);
    jz_btree_init(&m->program_fact_weights, &jzn_desc_pfe_i64);
    jazz_node__node__supporting_frontier__SupportingFrontier__default(&m->supporting);
    jz_btree_init(&m->physical_tables, &jzn_desc_physical_tables);
    jz_btree_init(&m->targeted_refresh_tables, &jzn_desc_str_set);
    m->targeted_refresh_uncertain = false;
    jz_btree_init(&m->selected_deletion_witnesses, &jzn_desc_selected_deletion_witnesses);
    jzn_wvi_default(&m->versions);
    jzn_ri_default(&m->replacements);
}

void jazz_node_MaintainedSubscriptionView_drop(MSVT *m) {
    if (m->client_local_literal_token) jz_rc_release(m->client_local_literal_token);
    jz_btree_drop(&m->witness_table_names);
    jz_btree_drop(&m->result_weights.entries);
    jz_btree_drop(&m->published_result_members.entries);
    jzn_drop_unreconciled(m);
    jzn_drop_journal(m);
    jz_btree_drop(&m->result_payloads.entries);
    jz_btree_drop(&m->published_result_payloads.entries);
    jz_btree_drop(&m->structured_app_rows);
    jz_btree_drop(&m->structured_terminal_records);
    jz_btree_drop(&m->structured_root_keys);
    jz_vec_drop(&m->structured_root_key_order, sizeof(jz_bytes), jzn_bytes_drop_v);
    jz_btree_drop(&m->inline_content_branch_keys);
    jz_btree_drop(&m->program_fact_weights);
    jazz_node__node__supporting_frontier__SupportingFrontier__drop(&m->supporting);
    jz_btree_drop(&m->physical_tables);
    jz_btree_drop(&m->targeted_refresh_tables);
    jz_btree_drop(&m->selected_deletion_witnesses);
    jz_btree_drop(&m->versions.by_tx);
    jz_btree_drop(&m->replacements.content_by_key);
    jz_btree_drop(&m->replacements.deletion_by_key);
}

void MSV(MaintainedSubscriptionView__hold_client_local_literal_token)(MSVT *m, void *token) {
    if (m->client_local_literal_token) jz_rc_release(m->client_local_literal_token);
    m->client_local_literal_token = token;
}

void MSV(MaintainedSubscriptionView__set_read_view)(MSVT *m, jazz_protocol_ReadViewKey read_view) { m->read_view = read_view; }

void MSV(MaintainedSubscriptionView__set_witness_table_names)(MSVT *m, jz_btree names) {
    jz_btree_drop(&m->witness_table_names);
    m->witness_table_names = names;
}

/* supporting_row_for_version */
static jz_err jzn_supporting_row_for_version(const MSVT *m, jazz_protocol_ProgramSourceId source, const VROW *row,
                                             const jazz_node_NodeAliases *node_aliases, SROW *out) {
    const jazz_types_GlobalPhysicalTableId *physical_table = jz_btree_get(&m->physical_tables, &source.table);
    jazz_protocol__protocol__ProgramSourceId__drop(&source);
    if (!physical_table) return JZN_ISV("support terminal has no physical table identity");
    jazz_types_TxId tx;
    if (!jazz_node__node__codec__version_tx_id_from_aliases(row, node_aliases, &tx)) return JZN_ISV("support input tx node alias must exist");
    jz_bytes branch = { 0 };
    jazz_model__model__branch__BranchKey__canonical_bytes(jazz_node__node__codec__VersionRow__branch_key(row), &branch);
    jzn_WitnessNameKey key = { *row->table, jazz_node__node__codec__VersionRow__schema_version_alias(row) };
    const jz_str *authored = jz_btree_get(&m->witness_table_names, &key);
    const jz_str *name = authored ? authored : row->table;
    memset(out, 0, sizeof *out);
    out->physical_table = *physical_table;
    out->version_table = groove__Intern__new_str(jz_str_clone(name));
    out->row = jazz_node__node__codec__VersionRow__row_uuid(row);
    out->version.tx = tx;
    out->version.has_schema_version = false;
    out->version.layer = jzn_witness_layer_of(row) == JZN_WL_CONTENT ? JAZZ_LAYER_CONTENT : JAZZ_LAYER_DELETION;
    out->version.batch.some = true;
    out->version.batch.v = tx;
    if (branch.len != 0) {
        out->version.branch_or_prefix.some = true;
        out->version.branch_or_prefix.v = branch;
    } else {
        jz_bytes_drop(&branch);
    }
    out->version.row_digest.some = false;
    return JZ_OK;
}

bool MSV(MaintainedSubscriptionView__uses_storage_backed_result_materialization)(const MSVT *m) { return m->storage_backed_result_materialization; }

void MSV(MaintainedSubscriptionView__enable_storage_backed_result_materialization)(MSVT *m) {
    m->storage_backed_result_materialization = true;
    jzn_drop_unreconciled(m);
    jzn_drop_journal(m);
}

void MSV(MaintainedSubscriptionView__enable_inline_content_branch_key)(MSVT *m, const jazz_model_BranchKey *branch_key) {
    jz_bytes bytes = { 0 };
    jazz_model__model__branch__BranchKey__canonical_bytes(branch_key, &bytes);
    if (jz_btree_contains(&m->inline_content_branch_keys, &bytes)) jz_bytes_drop(&bytes);
    else jz_btree_insert(&m->inline_content_branch_keys, &bytes, NULL, NULL);
    jzn_drop_journal(m);
}

/* ======================= MaintainedTerminalSchemas ======================= */
static void jzn_mtk_drop(void *p) {
    jzn_MaintainedTerminalKind *k = p;
    switch (k->tag) {
    case JZN_MTK_RESULT_CURRENT: jazz_engine_ResultMembershipSchema_drop(&k->u.result); break;
    case JZN_MTK_AGGREGATE_RESULT: jazz_engine_AggregateResultSchema_drop(&k->u.aggregate); break;
    case JZN_MTK_VERSION_CONTENT: case JZN_MTK_VERSION_DELETION: case JZN_MTK_REPLACEMENT_CONTENT:
    case JZN_MTK_REPLACEMENT_DELETION: case JZN_MTK_SHARED_CONTENT: case JZN_MTK_SHARED_DELETION:
        jazz_engine_VersionWitnessSchema_drop(&k->u.witness); break;
    case JZN_MTK_PROGRAM_SOURCE_COVERAGE: jazz_engine_ProgramSourceCoverageSchema_drop(&k->u.coverage); break;
    case JZN_MTK_RELATION_EDGE: jazz_engine_RelationEdgeSchema_drop(&k->u.relation); break;
    case JZN_MTK_ROOT_COLLECTOR_APP_ROWS:
        jazz_engine_AppRowSchema_drop(&k->u.root_collector.schema);
        jazz_node_TerminalRootLayout_drop(&k->u.root_collector.layout);
        break;
    case JZN_MTK_DIRECT_APP_ROWS: case JZN_MTK_AGGREGATE_APP_ROWS: jazz_engine_AppRowSchema_drop(&k->u.app_rows); break;
    }
}
static const jz_btree_desc jzn_desc_sinks = { sizeof(jz_str), sizeof(jzn_MaintainedTerminalKind), jz_cmp_str, jzn_str_drop_v, jzn_mtk_drop };

void jazz_node_MaintainedTerminalSchemas_drop(jazz_node_MaintainedTerminalSchemas *s) { jz_btree_drop(&s->sinks); }

static bool jzn_mtk_is_result_terminal(const jzn_MaintainedTerminalKind *k) {
    return k->tag == JZN_MTK_RESULT_CURRENT || k->tag == JZN_MTK_AGGREGATE_RESULT;
}
static bool jzn_mtk_requires_authoritative_membership_reconcile(const jzn_MaintainedTerminalKind *k) {
    return k->tag == JZN_MTK_VERSION_DELETION || k->tag == JZN_MTK_REPLACEMENT_DELETION || k->tag == JZN_MTK_SHARED_DELETION;
}

static const char *const jzn_mtk_names[] = {
    "ResultCurrent", "AggregateResult", "VersionContent", "VersionDeletion", "ReplacementContent", "ReplacementDeletion",
    "SharedContent", "SharedDeletion", "ProgramSourceCoverage", "RelationEdge", "RootCollectorAppRows", "DirectAppRows",
    "AggregateAppRows",
};

jz_err jazz_node__node__MaintainedTerminalSchemas__current_payload_schema(const jazz_node_MaintainedTerminalSchemas *s,
                                                                         const jazz_engine_ResultMembershipSchema **out) {
    jz_str *k;
    jzn_MaintainedTerminalKind *v;
    JZ_BTREE_FOR(&s->sinks, k, v) {
        (void)k;
        if (v->tag == JZN_MTK_RESULT_CURRENT) { *out = &v->u.result; return JZ_OK; }
    }
    return JZN_ISV("maintained result has no compiled payload schema");
}

jz_err jazz_node__node__MaintainedTerminalSchemas__direct_app_row_schema(const jazz_node_MaintainedTerminalSchemas *s,
                                                                        const jazz_engine_AppRowSchema **out) {
    jz_str *k;
    jzn_MaintainedTerminalKind *v;
    JZ_BTREE_FOR(&s->sinks, k, v) {
        (void)k;
        if (v->tag == JZN_MTK_DIRECT_APP_ROWS) { *out = &v->u.app_rows; return JZ_OK; }
    }
    return JZN_ISV("maintained direct result has no compiler-owned app-row schema");
}

jz_err jazz_node__node__MaintainedTerminalSchemas__aggregate_app_row_schema(const jazz_node_MaintainedTerminalSchemas *s,
                                                                           const jazz_engine_AppRowSchema **out) {
    jz_str *k;
    jzn_MaintainedTerminalKind *v;
    JZ_BTREE_FOR(&s->sinks, k, v) {
        (void)k;
        if (v->tag == JZN_MTK_AGGREGATE_APP_ROWS) { *out = &v->u.app_rows; return JZ_OK; }
    }
    return JZN_ISV("maintained aggregate has no compiler-owned app-row schema");
}

static jz_err jzn_mts_get(const jazz_node_MaintainedTerminalSchemas *s, const jz_str *sink, const jzn_MaintainedTerminalKind **out) {
    *out = jz_btree_get(&s->sinks, sink);
    if (!*out) return JZN_ISV("maintained view delta arrived for an unknown query-engine terminal");
    return JZ_OK;
}

const jazz_node_TerminalRootLayout *jazz_node__node__MaintainedTerminalSchemas__terminal_root_layout(const jazz_node_MaintainedTerminalSchemas *s) {
    jz_str *k;
    jzn_MaintainedTerminalKind *v;
    JZ_BTREE_FOR(&s->sinks, k, v) {
        (void)k;
        if (v->tag == JZN_MTK_ROOT_COLLECTOR_APP_ROWS) return &v->u.root_collector.layout;
    }
    return NULL;
}

bool jazz_node__node__MaintainedTerminalSchemas__has_root_collector(const jazz_node_MaintainedTerminalSchemas *s) {
    return jazz_node__node__MaintainedTerminalSchemas__terminal_root_layout(s) != NULL;
}

/* terminal_root_layout: the prepared publication layout of a compiler root collector. */
static u8 jzn_carrier_byte(u32 carrier) { return carrier == JAZZ_ENGINE_CARRIER_CURRENT_ROW ? 0 : 1; }

static void jzn_terminal_root_layout(const jazz_engine_AppRowSchema *rows, jazz_node_TerminalRootLayout *out) {
    /* The terminal exposes canonical nested schemas, while publication below
     * restores only public logical identities. Runtime allocation slots never
     * enter the terminal descriptor or its layout hash. */
    jz_bytes encoded = { 0 };
    jz_err e = groove__records__encode_persisted_record_descriptor(rows->descriptor, &encoded);
    if (JZ_IS_ERR(e)) jz_panic("compiler row schema is encodable");
    groove_RecordDescriptor canonical;
    e = groove__records__decode_persisted_record_descriptor(jz_bytes_slice(&encoded), &canonical);
    jz_bytes_drop(&encoded);
    if (JZ_IS_ERR(e)) jz_panic("compiler row schema is canonical");
    groove_DescriptorFieldVec descriptor_fields = { 0 };
    jz_vec_clone(&descriptor_fields, groove__records__RecordDescriptor__fields(canonical), sizeof(groove_DescriptorField), groove_DescriptorField_clone);

    const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(rows->descriptor);
    u32 root_key_slot = UINT32_MAX;
    for (u32 i = 0; i < fields->len; i++) {
        if (fields->ptr[i].has_name && jz_str_eq_cstr(&fields->ptr[i].name, "row_uuid")) { root_key_slot = i; break; }
    }
    if (root_key_slot == UINT32_MAX) jz_panic("structured app-row terminal has a row_uuid slot");

    /* Bind every public descriptor slot, including collector-owned trailing
     * arrays/records. */
    jazz_node_TerminalRootPublicFieldVec public_fields = { 0 };
    for (u32 slot = 0; slot < fields->len; slot++) {
        const groove_DescriptorField *field = &fields->ptr[slot];
        if (!field->has_name) continue;
        const jazz_engine_CurrentRowPublicationField *publication = jz_btree_get(&rows->publication_fields, &field->name);
        if (!publication) continue;
        /* Provenance is retained by the collector as a routing field, but a
         * selected magic column is public output. */
        bool selected_provenance = publication->tag == JZ_CRPF_RESULT_FIELD && publication->visibility == JZ_CRRV_PUBLIC_PROVENANCE;
        if (slot == root_key_slot || (jz_btree_contains(&rows->hidden_fields, &field->name) && !selected_provenance)) continue;
        jz_slice public_name;
        if (!jazz_engine__query_engine__publication__CurrentRowPublicationField__public_name(publication, &public_name)) continue;
        const u8 *carrier = jz_btree_get(&rows->field_carriers, &field->name);
        u8 c = carrier ? *carrier : rows->carrier;
        jazz_node_TerminalRootPublicField f;
        jazz_engine_CurrentRowPublicationField_clone(&f.publication, publication);
        f.name = jz_str_from_slice(public_name);
        f.descriptor_field_name = jz_str_clone(&field->name);
        f.slot = slot;
        f.carrier = c == JAZZ_ENGINE_CARRIER_CURRENT_ROW ? JZN_CARRIER_CURRENT_ROW : JZN_CARRIER_LOGICAL;
        JZ_VEC_PUSH(public_fields, f);
    }
    JZ_VEC_FOR(jazz_node_TerminalRootPublicField, f, public_fields) {
        if (f->slot < descriptor_fields.len) {
            groove_DescriptorField *df = &descriptor_fields.ptr[f->slot];
            jz_str_drop(&df->identity.name);
            df->identity.tag = GROOVE_FI_NAME;
            df->identity.name = jz_str_clone(&f->name);
        }
    }
    groove_RecordDescriptor root_descriptor = groove__records__RecordDescriptor__new_with_fields(descriptor_fields);

    jz_blake3_hasher *hasher = jz_blake3_new();
    jz_blake3_update(hasher, (const u8 *)"jazz terminal root publication v1", 33);
    const groove_DescriptorFieldVec *root_fields = groove__records__RecordDescriptor__fields(root_descriptor);
    groove_DescriptorFieldVec role_fields = { 0 };
    for (u32 slot = 0; slot < root_fields->len; slot++) {
        jz_str role_name;
        const jazz_node_TerminalRootPublicField *public = NULL;
        JZ_VEC_FOR(jazz_node_TerminalRootPublicField, f, public_fields) if (f->slot == slot) { public = f; break; }
        if (slot == root_key_slot) {
            role_name = jz_str_from("metadata/row_uuid");
        } else if (public) {
            const char *role = "source";
            if (public->publication.tag == JZ_CRPF_RESULT_FIELD) {
                switch (public->publication.visibility) {
                case JZ_CRRV_APPLICATION_CELL: role = "result"; break;
                case JZ_CRRV_PUBLIC_PROVENANCE: role = "provenance"; break;
                default: role = "hidden"; break;
                }
            }
            role_name = jz_format("%s/%u/%.*s", role, slot, (int)public->name.len, public->name.ptr);
        } else {
            /* Hidden routing fields affect byte layout, not public field identity. */
            role_name = jz_format("hidden/%u", slot);
        }
        groove_ValueType t;
        groove_ValueType_clone(&t, &root_fields->ptr[slot].value_type);
        groove_DescriptorField *df = jz_vec_push_slot(&role_fields, sizeof(groove_DescriptorField));
        groove__records__DescriptorField__new(df, role_name, t);
    }
    groove_RecordDescriptor role_descriptor = groove__records__RecordDescriptor__new_with_fields(role_fields);
    jz_bytes role_bytes = { 0 };
    e = groove__records__encode_persisted_record_descriptor(role_descriptor, &role_bytes);
    if (JZ_IS_ERR(e)) jz_panic("terminal role schemas contain valid Groove record descriptors");
    jz_blake3_update(hasher, role_bytes.ptr, role_bytes.len);
    jz_bytes_drop(&role_bytes);
    u64 slot64 = root_key_slot;
    jz_blake3_update(hasher, (const u8 *)&slot64, 8);
    u8 b = jzn_carrier_byte(rows->carrier);
    jz_blake3_update(hasher, &b, 1);
    b = rows->root_union_arm ? 1 : 0;
    jz_blake3_update(hasher, &b, 1);
    JZ_VEC_FOR(jazz_node_TerminalRootPublicField, f, public_fields) {
        u8 zero = 0;
        jz_blake3_update(hasher, (const u8 *)f->name.ptr, f->name.len);
        jz_blake3_update(hasher, &zero, 1);
        u64 s = f->slot;
        jz_blake3_update(hasher, (const u8 *)&s, 8);
        b = f->carrier == JZN_CARRIER_CURRENT_ROW ? 0 : 1;
        jz_blake3_update(hasher, &b, 1);
    }
    u8 digest[32];
    jz_blake3_finalize(hasher, digest);
    jz_blake3_free(hasher);
    out->id = jz_str_from("terminal:");
    jz_hex_encode(digest, 32, &out->id);
    out->root_descriptor = root_descriptor;
    out->root_key_slot = root_key_slot;
    if (!fields->ptr[root_key_slot].has_name) jz_panic("structured app-row row_uuid field is named");
    out->root_key_field_name = jz_str_clone(&fields->ptr[root_key_slot].name);
    out->public_fields = public_fields;
    out->carrier = rows->carrier == JAZZ_ENGINE_CARRIER_CURRENT_ROW ? JZN_CARRIER_CURRENT_ROW : JZN_CARRIER_LOGICAL;
    out->root_union_arm = rows->root_union_arm;
}

static bool jzn_has_row_uuid(groove_RecordDescriptor d) {
    u32 idx;
    return groove__records__RecordDescriptor__field_index(d, JZ_LIT("row_uuid"), &idx);
}

/* MaintainedTerminalSchemas::for_program */
static void jzn_mts_for_program(const jazz_engine_QueryProgram *program, jazz_node_MaintainedTerminalSchemas *out) {
    jz_btree_init(&out->sinks, &jzn_desc_sinks);
    JZ_VEC_FOR(const jazz_engine_LoweredTerminal, terminal, program->lowered.terminals) {
        jzn_MaintainedTerminalKind kind;
        memset(&kind, 0, sizeof kind);
        if (terminal->output.tag == JAZZ_ENGINE_OTS_APP_ROWS) {
            const jazz_engine_AppRowSchema *rows = &terminal->output.u.app_rows;
            switch (rows->terminal.tag) {
            case JAZZ_ENGINE_ART_ROOT_COLLECTOR:
                if (!jzn_has_row_uuid(rows->descriptor)) jz_panic("public root collector app-row terminal has no row_uuid");
                kind.tag = JZN_MTK_ROOT_COLLECTOR_APP_ROWS;
                jazz_engine_AppRowSchema_clone(&kind.u.root_collector.schema, rows);
                jzn_terminal_root_layout(rows, &kind.u.root_collector.layout);
                break;
            case JAZZ_ENGINE_ART_DIRECT:
                if (!jzn_has_row_uuid(rows->descriptor)) jz_panic("direct app-row terminal has no row_uuid");
                kind.tag = JZN_MTK_DIRECT_APP_ROWS;
                jazz_engine_AppRowSchema_clone(&kind.u.app_rows, rows);
                break;
            default:
                kind.tag = JZN_MTK_AGGREGATE_APP_ROWS;
                jazz_engine_AppRowSchema_clone(&kind.u.app_rows, rows);
                break;
            }
            jz_str sink = jz_str_clone(&terminal->sink);
            jzn_MaintainedTerminalKind old;
            if (jz_btree_insert(&out->sinks, &sink, &kind, &old)) { jzn_mtk_drop(&old); jz_str_drop(&sink); }
            continue;
        }
        const jazz_engine_ProgramFactOutput *fact = &terminal->output.u.fact;
        bool some = false;
        u32 key = fact->key.tag, term = fact->terminal, sch = fact->schema.tag;
        if (key == JAZZ_ENGINE_PFK_RESULT_MEMBERSHIP && term == JAZZ_ENGINE_PFT_PRIMARY && sch == JAZZ_ENGINE_PFS_RESULT_MEMBERSHIP) {
            kind.tag = JZN_MTK_RESULT_CURRENT;
            jazz_engine_ResultMembershipSchema_clone(&kind.u.result, &fact->schema.u.result_membership);
            some = true;
        } else if (key == JAZZ_ENGINE_PFK_RESULT_MEMBERSHIP && term == JAZZ_ENGINE_PFT_PRIMARY && sch == JAZZ_ENGINE_PFS_AGGREGATE_RESULT) {
            kind.tag = JZN_MTK_AGGREGATE_RESULT;
            jazz_engine_AggregateResultSchema_clone(&kind.u.aggregate, &fact->schema.u.aggregate_result);
            some = true;
        } else if (key == JAZZ_ENGINE_PFK_PROGRAM_SOURCE_COVERAGE && term == JAZZ_ENGINE_PFT_PRIMARY &&
                   sch == JAZZ_ENGINE_PFS_PROGRAM_SOURCE_COVERAGE) {
            kind.tag = JZN_MTK_PROGRAM_SOURCE_COVERAGE;
            jazz_engine_ProgramSourceCoverageSchema_clone(&kind.u.coverage, &fact->schema.u.program_source_coverage);
            some = true;
        } else if (key == JAZZ_ENGINE_PFK_RELATION_EDGES && term == JAZZ_ENGINE_PFT_PRIMARY && sch == JAZZ_ENGINE_PFS_RELATION_EDGES) {
            kind.tag = JZN_MTK_RELATION_EDGE;
            jazz_engine_RelationEdgeSchema_clone(&kind.u.relation, &fact->schema.u.relation_edges);
            some = true;
        } else if ((key == JAZZ_ENGINE_PFK_VERSION_WITNESSES && sch == JAZZ_ENGINE_PFS_VERSION_WITNESSES) ||
                   (key == JAZZ_ENGINE_PFK_REPLACEMENT_WITNESSES && sch == JAZZ_ENGINE_PFS_REPLACEMENT_WITNESSES)) {
            const jazz_engine_VersionWitnessSchemas *w = &fact->schema.u.witnesses;
            bool version = key == JAZZ_ENGINE_PFK_VERSION_WITNESSES;
            if (term == (version ? JAZZ_ENGINE_PFT_VERSION_WITNESS_DELETION : JAZZ_ENGINE_PFT_REPLACEMENT_WITNESS_DELETION) && w->has_deletion) {
                kind.tag = version ? JZN_MTK_VERSION_DELETION : JZN_MTK_REPLACEMENT_DELETION;
                jazz_engine_VersionWitnessSchema_clone(&kind.u.witness, &w->deletion);
                some = true;
            } else if (term == (version ? JAZZ_ENGINE_PFT_VERSION_WITNESS_CONTENT : JAZZ_ENGINE_PFT_REPLACEMENT_WITNESS_CONTENT) &&
                       w->has_content) {
                kind.tag = version ? JZN_MTK_VERSION_CONTENT : JZN_MTK_REPLACEMENT_CONTENT;
                jazz_engine_VersionWitnessSchema_clone(&kind.u.witness, &w->content);
                some = true;
            }
        }
        bool shared = false;
        void *sk;
        jz_str *sv;
        JZ_BTREE_FOR(&program->lowered.shared_witness_sinks, sk, sv) {
            (void)sk;
            if (jz_str_eq(sv, &terminal->sink)) { shared = true; break; }
        }
        if (shared && some) {
            if (kind.tag == JZN_MTK_VERSION_CONTENT) kind.tag = JZN_MTK_SHARED_CONTENT;
            else if (kind.tag == JZN_MTK_VERSION_DELETION) kind.tag = JZN_MTK_SHARED_DELETION;
        }
        if (some) {
            jz_str sink = jz_str_clone(&terminal->sink);
            jzn_MaintainedTerminalKind old;
            if (jz_btree_insert(&out->sinks, &sink, &kind, &old)) { jzn_mtk_drop(&old); jz_str_drop(&sink); }
        }
    }
}

void MSV(MaintainedSubscriptionView__terminal_schemas_for_program)(const jazz_engine_QueryProgram *program, jazz_node_MaintainedTerminalSchemas *out) {
    jzn_mts_for_program(program, out);
}

/* ======================= typed terminal record decoding ======================= */
/* field_idx / field_idx_in_descriptor */
static jz_err jzn_field_idx_in_descriptor(groove_RecordDescriptor descriptor, const jz_str *field, u32 *out) {
    if (!groove__records__RecordDescriptor__field_index(descriptor, jz_str_slice(field), out))
        return JZN_ISV("maintained view terminal missing field");
    return JZ_OK;
}
static jz_err jzn_field_idx(const groove_BorrowedRecord *record, const jz_str *field, u32 *out) {
    return jzn_field_idx_in_descriptor(*record->descriptor, field, out);
}
static jz_err jzn_field_idx_cstr(const groove_BorrowedRecord *record, const char *field, u32 *out) {
    jz_slice s = jz_slice_of(field, (u32)strlen(field));
    if (!groove__records__RecordDescriptor__field_index(*record->descriptor, s, out)) return JZN_ISV("maintained view terminal missing field");
    return JZ_OK;
}
static jz_err jzn_get_field(const groove_BorrowedRecord *record, const jz_str *field, groove_Value *out) {
    u32 idx;
    JZ_TRY(jzn_field_idx(record, field, &idx));
    return groove__records__BorrowedRecord__get_idx(record, idx, out);
}

/* record_string */
static jz_err jzn_record_string(const groove_BorrowedRecord *record, const jz_str *field, const char *not_string, jz_str *out) {
    groove_Value v;
    JZ_TRY(jzn_get_field(record, field, &v));
    if (v.tag != GROOVE_V_STRING) {
        groove_Value_drop(&v);
        return JZN_ISV(not_string);
    }
    *out = v.v.s;
    return JZ_OK;
}

/* record_u64_idx / record_u64 */
static jz_err jzn_record_u64_idx(const groove_BorrowedRecord *record, u32 idx, u64 *out) {
    groove_Value v;
    JZ_TRY(groove__records__BorrowedRecord__get_idx(record, idx, &v));
    if (v.tag != GROOVE_V_U64) {
        groove_Value_drop(&v);
        return JZN_ISV("field must be u64");
    }
    *out = v.v.u;
    return JZ_OK;
}
static jz_err jzn_record_u64(const groove_BorrowedRecord *record, const jz_str *field, u64 *out) {
    u32 idx;
    JZ_TRY(jzn_field_idx(record, field, &idx));
    return jzn_record_u64_idx(record, idx, out);
}

/* nullable_u64 */
static jz_err jzn_nullable_u64(const groove_BorrowedRecord *record, const jz_str *field, bool *some, u64 *out) {
    groove_Value v;
    JZ_TRY(jzn_get_field(record, field, &v));
    jz_err e = JZ_OK;
    *some = false;
    if (v.tag == GROOVE_V_NULLABLE) {
        if (v.v.nullable) {
            if (v.v.nullable->tag == GROOVE_V_U64) {
                *some = true;
                *out = v.v.nullable->v.u;
            } else {
                e = JZN_ISV("nullable field payload must be u64");
            }
        }
    } else if (v.tag == GROOVE_V_U64) {
        *some = true;
        *out = v.v.u;
    } else {
        e = JZN_ISV("field must be nullable u64");
    }
    groove_Value_drop(&v);
    return e;
}

/* tagged_deletion */
static jz_err jzn_tagged_deletion(groove_Value value, u32 *out) {
    if (value.tag != GROOVE_V_NULLABLE) {
        groove_Value_drop(&value);
        return JZN_ISV("tagged _deletion must be nullable");
    }
    if (!value.v.nullable) {
        *out = JZN_DELETION_NONE;
        return JZ_OK;
    }
    groove_Value inner = *value.v.nullable;
    jz_free(value.v.nullable);
    if (inner.tag == GROOVE_V_U8) {
        u8 discriminant = (u8)inner.v.u;
        inner.tag = GROOVE_V_ENUM_TAG;
        inner.v.enum_tag = discriminant;
    }
    return jazz_node__node__codec__deletion_event_from_value(inner, out);
}

/* validate_witness_event_kind */
static jz_err jzn_validate_witness_event_kind(const groove_BorrowedRecord *record, const char *expected) {
    u32 idx;
    JZ_TRY(jzn_field_idx_cstr(record, "event_kind", &idx));
    groove_Value v;
    JZ_TRY(groove__records__BorrowedRecord__get_idx(record, idx, &v));
    jz_err e = JZ_OK;
    if (v.tag == GROOVE_V_STRING) {
        if (!jz_str_eq_cstr(&v.v.s, expected)) e = JZN_ISV("maintained witness event kind did not match query-engine terminal schema");
    } else {
        e = JZN_ISV("maintained witness event kind must be string");
    }
    groove_Value_drop(&v);
    return e;
}

/* VersionDecodePlanCache = BTreeMap<(String, WitnessLayer), VersionDecodePlan> */
typedef struct jzn_PlanKey { jz_str table; u32 layer; } jzn_PlanKey;
static int jzn_plan_key_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const jzn_PlanKey *x = a, *y = b;
    int c = jz_str_cmp(&x->table, &y->table);
    return c ? c : (x->layer == y->layer ? 0 : (x->layer < y->layer ? -1 : 1));
}
static void jzn_plan_key_drop(void *p) { jz_str_drop(&((jzn_PlanKey *)p)->table); }
static void jzn_plan_drop(void *p) { jz_btree_drop(&((jzn_VersionDecodePlan *)p)->user_indices); }
static const jz_btree_desc jzn_desc_plan_cache = { sizeof(jzn_PlanKey), sizeof(jzn_VersionDecodePlan), jzn_plan_key_cmp, jzn_plan_key_drop,
    jzn_plan_drop };
static const jz_btree_desc jzn_desc_str_u32 = { sizeof(jz_str), sizeof(u32), jz_cmp_str, jzn_str_drop_v, NULL };

/* build_version_decode_plan */
static jz_err jzn_build_version_decode_plan(groove_RecordDescriptor terminal_descriptor, const jazz_engine_VersionWitnessSchema *schema,
                                            const jazz_model_TableSchema *table, u32 layer, jzn_VersionDecodePlan *out) {
    memset(out, 0, sizeof *out);
    groove_TableSchema history = jazz_model__schema__TableSchema__history_storage_table(table);
    out->descriptor = groove__schema__TableSchema__record_schema(&history);
    groove_TableSchema_drop(&history);
    if (schema->identity.branch_or_prefix_field.some) {
        out->has_branch_idx = true;
        JZ_TRY(jzn_field_idx_in_descriptor(terminal_descriptor, &schema->identity.branch_or_prefix_field.s, &out->branch_idx));
    }
    jz_btree_init(&out->user_indices, &jzn_desc_str_u32);
    if (layer == JZN_WL_CONTENT) {
        jz_str *column, *field;
        JZ_BTREE_FOR(&schema->user_fields, column, field) {
            u32 idx;
            jz_err e = jzn_field_idx_in_descriptor(terminal_descriptor, field, &idx);
            if (JZ_IS_ERR(e)) {
                jz_btree_drop(&out->user_indices);
                return e;
            }
            jz_str c = jz_str_clone(column);
            jz_btree_insert(&out->user_indices, &c, &idx, NULL);
        }
    }
    jz_err e = JZ_OK;
    if (!JZ_IS_ERR(e)) e = jzn_field_idx_in_descriptor(terminal_descriptor, &schema->identity.row_field, &out->row_idx);
    if (!JZ_IS_ERR(e)) e = jzn_field_idx_in_descriptor(terminal_descriptor, &schema->identity.tx_time_field, &out->tx_time_idx);
    if (!JZ_IS_ERR(e)) e = jzn_field_idx_in_descriptor(terminal_descriptor, &schema->identity.tx_node_field, &out->tx_node_idx);
    if (!JZ_IS_ERR(e)) e = jzn_field_idx_in_descriptor(terminal_descriptor, &schema->identity.schema_field, &out->schema_version_idx);
    if (!JZ_IS_ERR(e)) e = jzn_field_idx_in_descriptor(terminal_descriptor, &schema->created_by_field, &out->created_by_idx);
    if (!JZ_IS_ERR(e)) e = jzn_field_idx_in_descriptor(terminal_descriptor, &schema->created_at_field, &out->created_at_idx);
    if (!JZ_IS_ERR(e)) e = jzn_field_idx_in_descriptor(terminal_descriptor, &schema->updated_by_field, &out->updated_by_idx);
    if (!JZ_IS_ERR(e)) e = jzn_field_idx_in_descriptor(terminal_descriptor, &schema->updated_at_field, &out->updated_at_idx);
    if (!JZ_IS_ERR(e)) e = jzn_field_idx_in_descriptor(terminal_descriptor, &schema->authored_columns_field, &out->authored_columns_idx);
    if (JZ_IS_ERR(e)) jz_btree_drop(&out->user_indices);
    return e;
}

static jz_err jzn_witness_branch_key(const jazz_model_TableSchema *table, groove_Value value, jazz_model_BranchKey *out) {
    jz_err e = JZ_OK;
    if (value.tag == GROOVE_V_BYTES) {
        if (JZ_IS_ERR(jazz_model__schema__RuntimeSchema__decode_persisted_branch_key(table, jz_bytes_slice(&value.v.bytes), out)))
            e = JZN_ISV("maintained witness branch key is invalid");
    } else if (value.tag == GROOVE_V_NULLABLE && !value.v.nullable) {
        jazz_model_BranchKey_default(out);
    } else if (value.tag == GROOVE_V_NULLABLE) {
        if (value.v.nullable->tag == GROOVE_V_BYTES) {
            if (JZ_IS_ERR(jazz_model__schema__RuntimeSchema__decode_persisted_branch_key(table, jz_bytes_slice(&value.v.nullable->v.bytes), out)))
                e = JZN_ISV("maintained witness branch key is invalid");
        } else {
            e = JZN_ISV("branch key must be bytes");
        }
    } else {
        e = JZN_ISV("branch key must be bytes");
    }
    groove_Value_drop(&value);
    return e;
}

static jz_err jzn_witness_author(const groove_BorrowedRecord *record, u32 idx, jazz_types_AuthorSubject *out) {
    groove_BorrowedRecord sub;
    JZ_TRY(groove__records__BorrowedRecord__get_record(record, idx, &sub));
    jazz_types_RowAuthor author;
    jz_err e = jazz_types__ids__RowAuthor__from_record(&sub, &author);
    if (JZ_IS_ERR(e)) {
        jz_err_drop(&e);
        return jz_err_new(JZN_E_RECORD, "non-canonical record");
    }
    *out = jazz_types__ids__RowAuthor__as_author_subject(author);
    return JZ_OK;
}

static void jzn_parts_drop(jazz_node_VersionRowParts *p) {
    jz_str_drop(&p->table);
    jazz_model_BranchKey_drop(&p->branch_key);
    jz_btree_drop(&p->cells);
    if (p->authored_columns) {
        jz_btree_drop(p->authored_columns);
        jz_free(p->authored_columns);
    }
    jz_bytes_drop(&p->counter_signs);
}

/* create_with_encoded_fields closure of decode_typed_version_witness */
typedef struct jzn_WitnessEncodeCtx {
    const groove_BorrowedRecord *record;
    const jzn_VersionDecodePlan *plan;
    const jazz_model_TableSchema *table;
    const groove_ValueVec *values;
    u32 layer;
} jzn_WitnessEncodeCtx;

static jz_err jzn_witness_encode_field(void *vctx, u32 index, jz_bytes *output) {
    jzn_WitnessEncodeCtx *c = vctx;
    u32 user_cells = JZN_HISTORY_USER_CELLS;
    if (c->layer == JZN_WL_CONTENT && index >= user_cells && index < user_cells + c->table->columns.len) {
        const u32 *source = jz_btree_get(&c->plan->user_indices, &c->table->columns.ptr[index - user_cells].name);
        if (!source) jz_panic("user index planned for every column");
        u32 source_index = *source;
        const groove_DescriptorFieldVec *src_fields = groove__records__RecordDescriptor__fields(*c->record->descriptor);
        const groove_DescriptorFieldVec *dst_fields = groove__records__RecordDescriptor__fields(c->plan->descriptor);
        if (groove_ValueType_eq(&src_fields->ptr[source_index].value_type, &dst_fields->ptr[index].value_type)) {
            u32 start, end;
            JZ_TRY(groove__records__RecordDescriptor__field_span(*c->record->descriptor, c->record->raw, source_index, &start, &end));
            jz_bytes_push(output, c->record->raw.ptr + start, end - start);
            return JZ_OK;
        }
        groove_Value value;
        JZ_TRY(groove__records__BorrowedRecord__get_idx(c->record, source_index, &value));
        groove_Value probe, inner;
        groove_Value_clone(&probe, &value);
        bool some;
        jz_err e = jazz_node__node__codec__nullable_value(probe, &some, &inner);
        if (JZ_IS_ERR(e)) {
            groove_Value_drop(&value);
            return e;
        }
        if (some) groove_Value_drop(&inner);
        e = groove__records__RecordDescriptor__encode_field_into(c->plan->descriptor, index, &value, output);
        groove_Value_drop(&value);
        return e;
    }
    return groove__records__RecordDescriptor__encode_field_into(c->plan->descriptor, index, &c->values->ptr[index], output);
}

/* decode_typed_version_witness */
static jz_err jzn_decode_typed_version_witness(const groove_BorrowedRecord *record, const jazz_engine_VersionWitnessSchema *schema,
                                               const jzn_TableSchemas *tables, jz_btree *decode_plan_cache, VROW *out) {
    jz_str table_name;
    JZ_TRY(jzn_record_string(record, &schema->identity.table_field, "maintained witness table field must be string", &table_name));
    const jazz_model_TableSchema *table = jz_btree_get(tables, &table_name);
    jz_str_drop(&table_name);
    if (!table) return JZN_ISV("maintained witness table_name must exist");
    groove_Value deletion_value;
    JZ_TRY(jzn_get_field(record, &schema->deletion_field, &deletion_value));
    u32 deletion;
    JZ_TRY(jzn_tagged_deletion(deletion_value, &deletion));
    u32 layer = deletion != JZN_DELETION_NONE ? JZN_WL_DELETION : JZN_WL_CONTENT;
    jzn_PlanKey cache_key = { table->name, layer };
    if (!jz_btree_contains(decode_plan_cache, &cache_key)) {
        jzn_VersionDecodePlan plan;
        JZ_TRY(jzn_build_version_decode_plan(*record->descriptor, schema, table, layer, &plan));
        jzn_PlanKey owned = { jz_str_clone(&table->name), layer };
        jz_btree_insert(decode_plan_cache, &owned, &plan, NULL);
    }
    const jzn_VersionDecodePlan *plan = jz_btree_get(decode_plan_cache, &cache_key);
    if (!plan) jz_panic("version decode plan was just inserted");

    jazz_node_VersionRowParts parts;
    memset(&parts, 0, sizeof parts);
    jz_err e = JZ_OK;
    u64 tx_time;
    JZ_TRY(jzn_record_u64_idx(record, plan->tx_time_idx, &tx_time));
    if (plan->has_branch_idx) {
        groove_Value bv;
        JZ_TRY(groove__records__BorrowedRecord__get_idx(record, plan->branch_idx, &bv));
        JZ_TRY(jzn_witness_branch_key(table, bv, &parts.branch_key));
    } else {
        jazz_model_BranchKey_default(&parts.branch_key);
    }
    parts.table = jz_str_clone(&table->name);
    jz_btree_init(&parts.cells, &jzn_desc_str_value_pub);
    if (layer == JZN_WL_CONTENT) {
        groove_Value av, inner;
        bool some = false;
        e = groove__records__BorrowedRecord__get_idx(record, plan->authored_columns_idx, &av);
        if (!JZ_IS_ERR(e)) e = jazz_node__node__codec__nullable_value(av, &some, &inner);
        if (!JZ_IS_ERR(e) && some) {
            parts.authored_columns = jz_alloc(sizeof(jz_btree));
            e = jazz_node__node__codec__authored_column_ids_from_value(inner, parts.authored_columns);
        }
        if (JZ_IS_ERR(e)) goto fail;
    }
    e = groove__records__BorrowedRecord__get_uuid(record, plan->row_idx, &parts.row_uuid);
    if (!JZ_IS_ERR(e)) e = jzn_record_u64_idx(record, plan->tx_node_idx, &parts.tx_node_alias);
    if (!JZ_IS_ERR(e)) e = jzn_record_u64_idx(record, plan->schema_version_idx, &parts.schema_version_alias);
    if (JZ_IS_ERR(e)) goto fail;
    parts.tx_time = tx_time;
    e = jzn_witness_author(record, plan->created_by_idx, &parts.created_by);
    if (JZ_IS_ERR(e)) goto fail;
    /* Current-row provenance is public Unix milliseconds. Witness state needs
     * the corresponding history form only to identify/materialize the
     * authored version, whose provenance HLC always has counter zero. */
    u64 ms;
    e = jzn_record_u64_idx(record, plan->created_at_idx, &ms);
    if (JZ_IS_ERR(e)) goto fail;
    if (JZ_IS_ERR(jazz_types__time__TxTime__from_physical_ms(ms, &parts.created_at))) {
        e = JZN_ISV("maintained witness created_at_ms exceeds packed HLC range");
        goto fail;
    }
    e = jzn_witness_author(record, plan->updated_by_idx, &parts.updated_by);
    if (!JZ_IS_ERR(e)) e = jzn_record_u64_idx(record, plan->updated_at_idx, &ms);
    if (JZ_IS_ERR(e)) goto fail;
    if (JZ_IS_ERR(jazz_types__time__TxTime__from_physical_ms(ms, &parts.updated_at))) {
        e = JZN_ISV("maintained witness updated_at_ms exceeds packed HLC range");
        goto fail;
    }
    parts.deletion = deletion;
    /* A query witness identifies its image's write; it projects no merge fields. */
    parts.seq = 0;
    memset(&parts.base, 0, sizeof parts.base);
    groove_ValueVec values = { 0 };
    e = jazz_node__node__codec__history_values_from_parts(table, &parts, &values);
    if (JZ_IS_ERR(e)) goto fail;
    /* Query witnesses already contain encoded nullable user cells. Copy those
     * fields into the history layout instead of allocating a cells map. */
    jzn_WitnessEncodeCtx ctx = { record, plan, table, &values, layer };
    jz_bytes raw = { 0 };
    e = groove__records__RecordDescriptor__create_with_encoded_fields(plan->descriptor, record->raw.len, jzn_witness_encode_field, &ctx, &raw);
    jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop);
    if (JZ_IS_ERR(e)) goto fail;
    out->table = groove__Intern__new_str(parts.table);
    parts.table = (jz_str){ 0 };
    out->branch_key = parts.branch_key;
    jazz_model_BranchKey_default(&parts.branch_key);
    out->record.raw = raw;
    out->record.descriptor = plan->descriptor;
    jzn_parts_drop(&parts);
    e = jazz_node__node__codec__VersionRow__validate_canonical(out);
    if (JZ_IS_ERR(e)) jazz_node_VersionRow_drop(out);
    return e;
fail:
    jzn_parts_drop(&parts);
    return e;
}

/* branch_or_prefix discriminator of a result or relation edge row */
static jz_err jzn_branch_discriminator(const groove_BorrowedRecord *record, const jz_str *field, const char *bad, jazz_protocol_OptBytes *out) {
    groove_Value v;
    JZ_TRY(jzn_get_field(record, field, &v));
    jz_err e = JZ_OK;
    out->some = true;
    out->v = (jz_bytes){ 0 };
    if (v.tag == GROOVE_V_UUID) {
        out->v = jz_bytes_from(v.v.uuid.b, 16);
    } else if (v.tag == GROOVE_V_BYTES) {
        out->v = v.v.bytes;
        v.v.bytes = (jz_bytes){ 0 };
    } else if (v.tag == GROOVE_V_NULLABLE && v.v.nullable) {
        groove_Value *inner = v.v.nullable;
        if (inner->tag == GROOVE_V_UUID) {
            out->v = jz_bytes_from(inner->v.uuid.b, 16);
        } else if (inner->tag == GROOVE_V_BYTES) {
            out->v = inner->v.bytes;
            inner->v.bytes = (jz_bytes){ 0 };
        } else {
            e = JZN_ISV(bad);
        }
    } else if (v.tag == GROOVE_V_NULLABLE) {
        /* Nullable(None) -> empty */
    } else {
        e = JZN_ISV(bad);
    }
    groove_Value_drop(&v);
    if (JZ_IS_ERR(e)) out->some = false;
    return e;
}

/* table_name_from_versioned_ref */
static jz_err jzn_table_name_from_versioned_ref(const groove_BorrowedRecord *record, const jazz_engine_VersionedRowRefSchema *schema,
                                                const jzn_TableSchemas *tables, jz_str *out) {
    JZ_TRY(jzn_record_string(record, &schema->row.table_field, "relation edge table field must be string", out));
    if (!jz_btree_contains(tables, out)) {
        jz_str_drop(out);
        return JZN_ISV("relation edge table_name must exist");
    }
    return JZ_OK;
}

/* decode_relation_edge_version */
static jz_err jzn_decode_relation_edge_version(const groove_BorrowedRecord *record, const jazz_engine_VersionedRowRefSchema *schema,
                                               const jazz_node_NodeAliases *node_aliases, bool *has, jazz_protocol_RowVersionRefEntry *out) {
    *has = false;
    if (!schema->has_version || schema->version.tag != JAZZ_ENGINE_RMV_CONTENT) return JZ_OK;
    const jazz_engine_ContentVersionFields *version = &schema->version.content;
    u64 tx_time, alias;
    JZ_TRY(jzn_record_u64(record, &version->tx_time_field, &tx_time));
    JZ_TRY(jzn_record_u64(record, &version->tx_node_field, &alias));
    jazz_types_NodeUuid tx_node;
    if (!jazz_node__node__node_aliases__NodeAliases__node_for_alias(node_aliases, alias, &tx_node))
        return JZN_ISV("relation edge tx node alias must exist");
    memset(out, 0, sizeof *out);
    if (schema->branch_or_prefix_field.some) {
        JZ_TRY(jzn_branch_discriminator(record, &schema->branch_or_prefix_field.s, "relation edge branch discriminator must be UUID or bytes",
                                        &out->branch_or_prefix));
        if (out->branch_or_prefix.v.len == 0) {
            jz_bytes_drop(&out->branch_or_prefix.v);
            out->branch_or_prefix.some = false;
        }
    }
    out->tx.time = tx_time;
    out->tx.node = tx_node;
    out->has_schema_version = false;
    out->layer = JAZZ_LAYER_CONTENT;
    out->batch.some = false;
    out->row_digest.some = false;
    *has = true;
    return JZ_OK;
}

/* decode_typed_relation_edge */
static jz_err jzn_decode_typed_relation_edge(const groove_BorrowedRecord *record, const jazz_engine_RelationEdgeSchema *schema,
                                             const jzn_TableSchemas *tables, const jazz_node_NodeAliases *node_aliases,
                                             jazz_protocol_RelationEdgeEntry *out) {
    jz_str source_table, target_table, path;
    JZ_TRY(jzn_table_name_from_versioned_ref(record, &schema->source, tables, &source_table));
    jz_err e = jzn_table_name_from_versioned_ref(record, &schema->target, tables, &target_table);
    if (JZ_IS_ERR(e)) { jz_str_drop(&source_table); return e; }
    e = jzn_record_string(record, &schema->path_field, "relation edge path field must be string", &path);
    if (JZ_IS_ERR(e)) { jz_str_drop(&source_table); jz_str_drop(&target_table); return e; }
    memset(out, 0, sizeof *out);
    out->path = path;
    out->source_table = groove__Intern__new_str(source_table);
    out->target_table = groove__Intern__new_str(target_table);
    u32 idx;
    e = jzn_field_idx(record, &schema->source.row.row_field, &idx);
    if (!JZ_IS_ERR(e)) e = groove__records__BorrowedRecord__get_uuid(record, idx, &out->source_row);
    if (!JZ_IS_ERR(e)) e = jzn_field_idx(record, &schema->target.row.row_field, &idx);
    if (!JZ_IS_ERR(e)) e = groove__records__BorrowedRecord__get_uuid(record, idx, &out->target_row);
    out->kind.some = true;
    out->kind.v = JAZZ_EDGE_RELATION;
    if (!JZ_IS_ERR(e)) e = jzn_decode_relation_edge_version(record, &schema->source, node_aliases, &out->has_source_version, &out->source_version);
    if (!JZ_IS_ERR(e)) e = jzn_decode_relation_edge_version(record, &schema->target, node_aliases, &out->has_target_version, &out->target_version);
    out->has_depth = false;
    out->role.some = true;
    out->role.v = JAZZ_ROLE_TERMINAL;
    if (JZ_IS_ERR(e)) jz_str_drop(&out->path);
    return e;
}

/* ---- flat-join row digest ---- */
#define JZN_FLAT_JOIN_ROW_DIGEST_DOMAIN "jazz.flat-join-row-digest.v1"
static const u8 JZN_FLAT_JOIN_ROW_DIGEST_MAGIC[4] = { 'J', 'F', 'R', 'D' };
#define JZN_FLAT_JOIN_ROW_DIGEST_VERSION 1u

/* canonicalize_flat_join_payload_value (consumes value) */
static groove_Value jzn_canonicalize_flat_join_payload_value(groove_Value value, const groove_ValueType *target) {
    if (target->tag == GROOVE_VT_NULLABLE) {
        const groove_ValueType *inner = target->v.inner;
        if (value.tag == GROOVE_V_NULLABLE && !value.v.nullable) return groove_value_null();
        if (value.tag == GROOVE_V_NULLABLE) {
            groove_Value present = *value.v.nullable;
            jz_free(value.v.nullable);
            groove_Value v = jzn_canonicalize_flat_join_payload_value(present, inner);
            /* A relation carrier can add one nullable layer around a null
             * authored value. A `Nullable(T)` public field has only one such
             * layer, so collapse that extra present-null carrier. */
            if (v.tag == GROOVE_V_NULLABLE && !v.v.nullable && inner->tag != GROOVE_VT_NULLABLE) return v;
            return groove_value_some(v);
        }
        /* Runtime lowering may unwrap a nullable join key after proving it is present. */
        return groove_value_some(jzn_canonicalize_flat_join_payload_value(value, inner));
    }
    /* A runtime source can retain an optional carrier around a field whose
     * public projection is proven non-null. Only a present wrapper is
     * equivalent; `None` remains invalid and is rejected by `create`. */
    if (value.tag == GROOVE_V_NULLABLE && value.v.nullable) {
        groove_Value present = *value.v.nullable;
        jz_free(value.v.nullable);
        return jzn_canonicalize_flat_join_payload_value(present, target);
    }
    if (value.tag == GROOVE_V_ARRAY && target->tag == GROOVE_VT_ARRAY) {
        JZ_VEC_FOR(groove_Value, v, value.v.vals) *v = jzn_canonicalize_flat_join_payload_value(*v, target->v.inner);
        return value;
    }
    if (value.tag == GROOVE_V_TUPLE && target->tag == GROOVE_VT_TUPLE && value.v.vals.len == target->v.tuple.len) {
        for (u32 i = 0; i < value.v.vals.len; i++)
            value.v.vals.ptr[i] = jzn_canonicalize_flat_join_payload_value(value.v.vals.ptr[i], &target->v.tuple.ptr[i]);
        return value;
    }
    return value;
}

static void jzn_put_be32(jz_bytes *b, u32 v) {
    u8 x[4] = { (u8)(v >> 24), (u8)(v >> 16), (u8)(v >> 8), (u8)v };
    jz_bytes_push(b, x, 4);
}

/* flat_join_row_digest_preimage (consumes values) */
static jz_err jzn_flat_join_row_digest_preimage(const jazz_engine_TypedOutputFieldVec *fields, groove_ValueVec values, jz_bytes *out) {
    if (fields->len != values.len) {
        jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop);
        return JZN_ISV("flat joined result revision field/value arity disagrees");
    }
    groove_NamedValueTypeVec named = { 0 };
    for (u32 i = 0; i < fields->len; i++) {
        groove_NamedValueType n;
        n.name = jz_format("flat_join_payload_%u", i);
        groove_ValueType_clone(&n.value_type, &fields->ptr[i].ty);
        JZ_VEC_PUSH(named, n);
    }
    groove_RecordDescriptor descriptor = groove__records__RecordDescriptor__new(named);
    jz_bytes descriptor_bytes = { 0 }, record_bytes = { 0 };
    jz_err e = groove__records__encode_persisted_record_descriptor(descriptor, &descriptor_bytes);
    if (JZ_IS_ERR(e)) {
        jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop);
        return e;
    }
    /* Restore a nullable wrapper an inner join tightened, so the same logical
     * tuple gets one digest regardless of that execution detail. */
    for (u32 i = 0; i < values.len; i++) values.ptr[i] = jzn_canonicalize_flat_join_payload_value(values.ptr[i], &fields->ptr[i].ty);
    e = groove__records__RecordDescriptor__create(descriptor, values.ptr, values.len, &record_bytes);
    jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop);
    if (JZ_IS_ERR(e)) {
        jz_bytes_drop(&descriptor_bytes);
        return e;
    }
    /* u32::try_from always fits on wasm32 (u32 lengths) */
    jz_bytes bytes = { 0 };
    jz_vec_reserve(&bytes, 4 + 1 + 12 + descriptor_bytes.len + record_bytes.len, 1);
    jz_bytes_push(&bytes, JZN_FLAT_JOIN_ROW_DIGEST_MAGIC, 4);
    jz_bytes_push_u8(&bytes, JZN_FLAT_JOIN_ROW_DIGEST_VERSION);
    jzn_put_be32(&bytes, fields->len);
    jzn_put_be32(&bytes, descriptor_bytes.len);
    jz_bytes_push(&bytes, descriptor_bytes.ptr, descriptor_bytes.len);
    jzn_put_be32(&bytes, record_bytes.len);
    jz_bytes_push(&bytes, record_bytes.ptr, record_bytes.len);
    jz_bytes_drop(&descriptor_bytes);
    jz_bytes_drop(&record_bytes);
    *out = bytes;
    return JZ_OK;
}

/* flat_join_row_digest */
static jz_err jzn_flat_join_row_digest(const jazz_engine_TypedOutputFieldVec *fields, groove_ValueVec values, jz_bytes *out) {
    jz_bytes bytes;
    JZ_TRY(jzn_flat_join_row_digest_preimage(fields, values, &bytes));
    u8 key[32];
    jz_blake3_derive_key(JZN_FLAT_JOIN_ROW_DIGEST_DOMAIN, bytes.ptr, bytes.len, key);
    jz_bytes_drop(&bytes);
    *out = jz_bytes_from(key, 32);
    return JZ_OK;
}

/* decode_aggregate_result: the synthetic member/payload pair of an aggregate terminal, as a member of `table`. */
static jz_err jzn_decode_aggregate_result(const groove_BorrowedRecord *record, const jazz_engine_AggregateResultSchema *schema, jz_str table,
                                          jazz_node_DecodedMaintainedEvent *out) {
    jazz_node_PreparedAggregatePayload prepared;
    jz_err e = jazz_node__node__descriptor_roles__encode_aggregate_payload_record(record, schema, &prepared);
    if (JZ_IS_ERR(e)) {
        jz_str_drop(&table);
        return e;
    }
    memset(out, 0, sizeof *out);
    out->tag = JZN_DME_AGGREGATE_RESULT;
    out->member.tag = JAZZ_RME_SYNTHETIC;
    out->member.u.synthetic.table = table;
    out->member.u.synthetic.row = prepared.row_identity;
    out->member.u.synthetic.replacement = jazz_protocol__protocol__SyntheticReplacementToken__from_encoded_record(prepared.replacement_identity);
    out->payload.member = jzn_rme_clone(&out->member);
    out->payload.descriptor = prepared.descriptor;
    out->payload.record = prepared.record;
    jazz_engine_SyntheticResultMembershipSchema_clone(&out->synthetic, &schema->synthetic);
    JZ_VEC_FOR(const groove_DescriptorField, field, schema->value_fields) {
        if (!field->has_name) jz_panic("lowered aggregate output is named");
        JZ_VEC_PUSH(out->value_fields, jz_str_clone(&field->name));
    }
    return JZ_OK;
}

/* payload_plans: HashMap<RecordDescriptor, CurrentPayloadEncodePlan> */
static u64 jzn_descriptor_hash(const void *k) { return groove__records__RecordDescriptor__hash(*(const groove_RecordDescriptor *)k); }
static bool jzn_descriptor_eq(const void *a, const void *b) {
    return groove_RecordDescriptor_eq(*(const groove_RecordDescriptor *)a, *(const groove_RecordDescriptor *)b);
}
static const jz_hmap_desc jzn_desc_payload_plans = { sizeof(groove_RecordDescriptor), sizeof(jazz_node_CurrentPayloadEncodePlan), jzn_descriptor_hash,
    jzn_descriptor_eq, NULL, jazz_node__node__descriptor_roles__CurrentPayloadEncodePlan__drop };

typedef struct jzn_DecodeCtx {
    const jzn_TableSchemas *tables;
    const jazz_node_NodeAliases *node_aliases;
    jz_btree decode_plan_cache;
    jz_hmap payload_plans;
    jazz_protocol_ReadViewKey read_view;
} jzn_DecodeCtx;

/* MaintainedTerminalKind::ResultCurrent arm of decode_typed_terminal_record */
static jz_err jzn_decode_result_current(const groove_BorrowedRecord *record, const jazz_engine_ResultMembershipSchema *schema, jzn_DecodeCtx *ctx,
                                        jazz_node_DecodedMaintainedEvent *out) {
    jz_str table_name;
    JZ_TRY(jzn_record_string(record, &schema->table_field, "maintained result membership table field must be string", &table_name));
    const jazz_model_TableSchema *table = jz_btree_get(ctx->tables, &table_name);
    jz_str_drop(&table_name);
    if (!table) return JZN_ISV("maintained result membership table_name must exist");
    u32 idx;
    jazz_types_RowUuid row_uuid;
    JZ_TRY(jzn_field_idx(record, &schema->row_field, &idx));
    JZ_TRY(groove__records__BorrowedRecord__get_uuid(record, idx, &row_uuid));
    jzn_ObjectIdVec occurrence_ids = { 0 };
    JZ_VEC_FOR(const jz_str, field, schema->occurrence_id_fields) {
        jz_uuid u;
        jz_err e = jzn_field_idx(record, field, &idx);
        if (!JZ_IS_ERR(e)) e = groove__records__BorrowedRecord__get_uuid(record, idx, &u);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&occurrence_ids, sizeof(jazz_types_ObjectId), NULL); return e; }
        JZ_VEC_PUSH(occurrence_ids, jazz_types__object__ObjectId__from_uuid(u));
    }
    if (occurrence_ids.len == 0) return JZN_ISV("maintained result membership occurrence must include its root row");
    jazz_types_ObjectId root = occurrence_ids.ptr[0];
    jzn_ObjectIdVec joined = { 0 };
    jz_vec_extend(&joined, occurrence_ids.ptr + 1, occurrence_ids.len - 1, sizeof(jazz_types_ObjectId));
    jz_vec_drop(&occurrence_ids, sizeof(jazz_types_ObjectId), NULL);
    jzn_UnionArmVec union_arms = { 0 };
    usize *position;
    jz_str *field;
    jz_err e = JZ_OK;
    JZ_BTREE_FOR(&schema->occurrence_union_arm_fields, position, field) {
        groove_Value v;
        e = jzn_get_field(record, field, &v);
        if (JZ_IS_ERR(e)) break;
        if (v.tag != GROOVE_V_STRING || v.v.s.len == 0) {
            groove_Value_drop(&v);
            e = JZN_ISV("maintained result union arm must be a non-empty string");
            break;
        }
        jzn_UnionArm arm = { *position, v.v.s };
        JZ_VEC_PUSH(union_arms, arm);
    }
    if (JZ_IS_ERR(e)) {
        jz_vec_drop(&joined, sizeof(jazz_types_ObjectId), NULL);
        JZ_VEC_FOR(jzn_UnionArm, a, union_arms) jz_str_drop(&a->label);
        jz_vec_drop(&union_arms, sizeof(jzn_UnionArm), NULL);
        return e;
    }
    jazz_types_OutputOccurrenceId occurrence_id;
    if (!jazz_types__object__OutputOccurrenceId__with_union_arms(root, joined, union_arms, &occurrence_id))
        return JZN_ISV("maintained result union occurrence carrier is malformed");
    if (schema->version.tag == JAZZ_ENGINE_RMV_CONTENT_OR_DELETION)
        return JZN_ISV("maintained result membership does not support include-deleted schemas yet");
    const jz_str *tx_time_field = &schema->version.content.tx_time_field, *tx_node_field = &schema->version.content.tx_node_field;
    u64 tx_time, alias;
    JZ_TRY(jzn_record_u64(record, tx_time_field, &tx_time));
    JZ_TRY(jzn_record_u64(record, tx_node_field, &alias));
    jazz_types_NodeUuid tx_node;
    if (!jazz_node__node__node_aliases__NodeAliases__node_for_alias(ctx->node_aliases, alias, &tx_node))
        return JZN_ISV("result tx node alias must exist");
    bool has_settle = false;
    u64 settle = 0;
    if (schema->settle_position_field.some) JZ_TRY(jzn_nullable_u64(record, &schema->settle_position_field.s, &has_settle, &settle));
    bool has_digest = false;
    jz_bytes digest = { 0 };
    if (schema->payload_fields.len != 0) {
        groove_ValueVec values = { 0 };
        JZ_VEC_FOR(const jazz_engine_TypedOutputField, f, schema->payload_fields) {
            groove_Value v;
            e = jzn_get_field(record, &f->name, &v);
            if (JZ_IS_ERR(e)) { jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop); return e; }
            JZ_VEC_PUSH(values, v);
        }
        JZ_TRY(jzn_flat_join_row_digest(&schema->payload_fields, values, &digest));
        has_digest = true;
    }
    jazz_protocol_OptBytes branch_or_prefix = { 0 };
    if (schema->branch_or_prefix_field.some) {
        JZ_TRY(jzn_branch_discriminator(record, &schema->branch_or_prefix_field.s, "result branch discriminator must be UUID or bytes",
                                        &branch_or_prefix));
        /* The empty/shared branch has a non-empty postcard encoding. Keep its
         * historical `None` identity so ordinary result members and runtime
         * receipts do not churn. */
        jazz_model_BranchKey shared;
        jazz_model_BranchKey_default(&shared);
        jz_bytes shared_bytes = { 0 };
        jazz_model__model__branch__BranchKey__canonical_bytes(&shared, &shared_bytes);
        jazz_model_BranchKey_drop(&shared);
        if (branch_or_prefix.v.len == 0 || jz_bytes_eq(&branch_or_prefix.v, &shared_bytes)) {
            jz_bytes_drop(&branch_or_prefix.v);
            branch_or_prefix.some = false;
        }
        jz_bytes_drop(&shared_bytes);
    }
    jazz_protocol_RealRowMemberEntry member;
    jazz_protocol_ResultRowEntry rr = { groove__Intern__new_str(jz_str_clone(&table->name)), row_uuid, { tx_time, tx_node } };
    jazz_protocol__protocol__RealRowMemberEntry__current_content(&member, rr);
    jazz_protocol__protocol__RealRowMemberEntry__with_occurrence_id(&member, occurrence_id);
    jazz_protocol__protocol__RealRowMemberEntry__with_settle_position(&member, has_settle, settle);
    member.read_view = ctx->read_view;
    member.branch_or_prefix = branch_or_prefix;
    if (has_digest) jazz_protocol__protocol__RealRowMemberEntry__with_row_digest(&member, digest);
    memset(out, 0, sizeof *out);
    out->tag = JZN_DME_RESULT_CURRENT;
    jazz_protocol__protocol__ResultMemberEntry__from_real(&out->member, member);
    bool inserted;
    groove_RecordDescriptor d = *record->descriptor;
    jazz_node_CurrentPayloadEncodePlan *plan = jz_hmap_entry(&ctx->payload_plans, &d, &inserted);
    if (inserted) {
        e = jazz_node__node__descriptor_roles__CurrentPayloadEncodePlan__new(d, schema, plan);
        if (JZ_IS_ERR(e)) {
            jz_hmap_remove(&ctx->payload_plans, &d, NULL, NULL);
            jazz_protocol__protocol__ResultMemberEntry__drop(&out->member);
            return e;
        }
    }
    jz_bytes descriptor = { 0 }, row_bytes = { 0 };
    e = jazz_node__node__descriptor_roles__CurrentPayloadEncodePlan__encode(plan, record, &descriptor, &row_bytes);
    if (JZ_IS_ERR(e)) {
        jazz_protocol__protocol__ResultMemberEntry__drop(&out->member);
        return e;
    }
    out->payload.member = jzn_rme_clone(&out->member);
    out->payload.descriptor = descriptor;
    out->payload.record = row_bytes;
    return JZ_OK;
}

static jz_err jzn_decode_witness_event(const groove_BorrowedRecord *record, const jazz_engine_VersionWitnessSchema *schema, const char *expected,
                                       u32 tag, jzn_DecodeCtx *ctx, jazz_node_DecodedMaintainedEvent *out) {
    JZ_TRY(jzn_validate_witness_event_kind(record, expected));
    memset(out, 0, sizeof *out);
    JZ_TRY(jzn_decode_typed_version_witness(record, schema, ctx->tables, &ctx->decode_plan_cache, &out->row));
    out->tag = tag;
    jazz_protocol__protocol__ProgramSourceId__clone(&out->source, &schema->source);
    return JZ_OK;
}

/* decode_typed_terminal_record */
static jz_err jzn_decode_typed_terminal_record(const groove_BorrowedRecord *record, const jzn_MaintainedTerminalKind *kind, jzn_DecodeCtx *ctx,
                                               jazz_node_DecodedMaintainedEvent *out) {
    switch (kind->tag) {
    case JZN_MTK_SHARED_CONTENT:
        return jzn_decode_witness_event(record, &kind->u.witness, "version_content", JZN_DME_SHARED_VERSION, ctx, out);
    case JZN_MTK_SHARED_DELETION:
        return jzn_decode_witness_event(record, &kind->u.witness, "version_deletion", JZN_DME_SHARED_VERSION, ctx, out);
    case JZN_MTK_AGGREGATE_APP_ROWS:
        if (kind->u.app_rows.terminal.tag != JAZZ_ENGINE_ART_AGGREGATE) return JZN_ISV("aggregate terminal lost its lowered schema");
        return jzn_decode_aggregate_result(record, &kind->u.app_rows.terminal.aggregate, jz_str_from("aggregate_result"), out);
    case JZN_MTK_RESULT_CURRENT:
        return jzn_decode_result_current(record, &kind->u.result, ctx, out);
    case JZN_MTK_AGGREGATE_RESULT: {
        jz_str table;
        JZ_TRY(jzn_record_string(record, &kind->u.aggregate.synthetic.table_field, "aggregate result table field must be string", &table));
        return jzn_decode_aggregate_result(record, &kind->u.aggregate, table, out);
    }
    case JZN_MTK_PROGRAM_SOURCE_COVERAGE: {
        u32 idx;
        JZ_TRY(jzn_field_idx_cstr(record, "complete", &idx));
        groove_Value v;
        JZ_TRY(groove__records__BorrowedRecord__get_idx(record, idx, &v));
        if (v.tag != GROOVE_V_BOOL) {
            groove_Value_drop(&v);
            return JZN_ISV("program-source coverage complete field must be bool");
        }
        if (v.v.b != kind->u.coverage.complete) return JZN_ISV("program-source coverage terminal disagrees with compiled schema");
        memset(out, 0, sizeof *out);
        out->tag = JZN_DME_PROGRAM_SOURCE_COVERAGE;
        jazz_protocol__protocol__ProgramSourceId__clone(&out->coverage.source, &kind->u.coverage.source);
        out->coverage.complete = v.v.b;
        return JZ_OK;
    }
    case JZN_MTK_VERSION_CONTENT:
        return jzn_decode_witness_event(record, &kind->u.witness, "version_content", JZN_DME_VERSION_CONTENT, ctx, out);
    case JZN_MTK_VERSION_DELETION:
        return jzn_decode_witness_event(record, &kind->u.witness, "version_deletion", JZN_DME_VERSION_DELETION, ctx, out);
    case JZN_MTK_REPLACEMENT_CONTENT:
        return jzn_decode_witness_event(record, &kind->u.witness, "replacement_content", JZN_DME_REPLACEMENT_CONTENT, ctx, out);
    case JZN_MTK_REPLACEMENT_DELETION:
        return jzn_decode_witness_event(record, &kind->u.witness, "replacement_deletion", JZN_DME_REPLACEMENT_DELETION, ctx, out);
    case JZN_MTK_RELATION_EDGE:
        memset(out, 0, sizeof *out);
        out->tag = JZN_DME_RELATION_EDGE;
        return jzn_decode_typed_relation_edge(record, &kind->u.relation, ctx->tables, ctx->node_aliases, &out->edge);
    case JZN_MTK_ROOT_COLLECTOR_APP_ROWS:
    case JZN_MTK_DIRECT_APP_ROWS: {
        const jazz_engine_AppRowSchema *schema = kind->tag == JZN_MTK_DIRECT_APP_ROWS ? &kind->u.app_rows : &kind->u.root_collector.schema;
        u32 idx;
        JZ_TRY(jzn_field_idx_cstr(record, "row_uuid", &idx));
        memset(out, 0, sizeof *out);
        JZ_TRY(groove__records__BorrowedRecord__get_uuid(record, idx, &out->root));
        out->tag = JZN_DME_STRUCTURED_APP_ROW;
        out->record.raw = jz_bytes_from(record->raw.ptr, record->raw.len);
        out->record.descriptor = schema->descriptor;
        return JZ_OK;
    }
    }
    JZ_UNREACHABLE();
}

/* ======================= terminal layout rebinding ======================= */
static bool jzn_terminal_field_can_rebind_to_layout(const groove_ValueType *source, const groove_ValueType *target) {
    if (groove_ValueType_eq(source, target)) return true;
    if (target->tag == GROOVE_VT_NULLABLE && jzn_terminal_field_can_rebind_to_layout(source, target->v.inner)) return true;
    groove_NamedValueTypeVec a = { 0 }, b = { 0 };
    groove_NamedValueType na = { jz_str_from("value"), { 0 } }, nb = { jz_str_from("value"), { 0 } };
    groove_ValueType_clone(&na.value_type, source);
    groove_ValueType_clone(&nb.value_type, target);
    JZ_VEC_PUSH(a, na);
    JZ_VEC_PUSH(b, nb);
    groove_FieldPair pair = { 0, 0 };
    groove_RecordProjector *projector = NULL;
    jz_err e = groove__records__RecordProjector__new_registry_rebound(groove__records__RecordDescriptor__new(a), groove__records__RecordDescriptor__new(b),
                                                                     &pair, 1, &projector);
    if (JZ_IS_ERR(e)) {
        jz_err_drop(&e);
        return false;
    }
    groove_RecordProjector_drop(projector);
    return true;
}

static bool jzn_terminal_descriptor_can_rebind_to_layout(groove_RecordDescriptor source, groove_RecordDescriptor target) {
    const groove_DescriptorFieldVec *s = groove__records__RecordDescriptor__fields(source);
    const groove_DescriptorFieldVec *t = groove__records__RecordDescriptor__fields(target);
    if (s->len != t->len) return false;
    for (u32 i = 0; i < s->len; i++) {
        if (s->ptr[i].has_name != t->ptr[i].has_name) return false;
        if (s->ptr[i].has_name && !jz_str_eq(&s->ptr[i].name, &t->ptr[i].name)) return false;
        if (!jzn_terminal_field_can_rebind_to_layout(&s->ptr[i].value_type, &t->ptr[i].value_type)) return false;
    }
    return true;
}

static bool jzn_terminal_nested_collection_layout_agrees(const groove_TerminalOperation *operation, groove_RecordDescriptor target) {
    if (operation->path.len == 0) return true;
    const groove_TerminalPathSegment *first = &operation->path.ptr[0];
    if (first->tag != GROOVE_TPS_COLLECTION) return false;
    u32 source_index, target_index;
    if (!groove__records__RecordDescriptor__field_index(operation->root_descriptor, jz_str_slice(&first->name), &source_index) ||
        !groove__records__RecordDescriptor__field_index(target, jz_str_slice(&first->name), &target_index))
        return false;
    const groove_ValueType *source_type = &groove__records__RecordDescriptor__fields(operation->root_descriptor)->ptr[source_index].value_type;
    return source_type->tag == GROOVE_VT_ARRAY && source_type->v.inner->tag == GROOVE_VT_RECORD &&
           groove_ValueType_eq(source_type, &groove__records__RecordDescriptor__fields(target)->ptr[target_index].value_type);
}

static jz_err jzn_rebind_terminal_record_values(groove_RecordDescriptor source, groove_RecordDescriptor target, groove_ValueVec values,
                                                const char *encoding_failed, jz_bytes *out);

/* rebind_terminal_value (consumes value) */
static jz_err jzn_rebind_terminal_value(groove_Value value, const groove_ValueType *source, const groove_ValueType *target, groove_Value *out) {
    if (groove_ValueType_eq(source, target)) {
        *out = value;
        return JZ_OK;
    }
    if (target->tag == GROOVE_VT_NULLABLE && jzn_terminal_field_can_rebind_to_layout(source, target->v.inner)) {
        groove_Value inner;
        JZ_TRY(jzn_rebind_terminal_value(value, source, target->v.inner, &inner));
        *out = groove_value_some(inner);
        return JZ_OK;
    }
    if (!jzn_terminal_field_can_rebind_to_layout(source, target)) {
        groove_Value_drop(&value);
        return JZN_ISV("structured terminal root value disagrees with prepared layout");
    }
    if (value.tag == GROOVE_V_TUPLE && source->tag == GROOVE_VT_TUPLE && target->tag == GROOVE_VT_TUPLE) {
        u32 n = value.v.vals.len;
        if (source->v.tuple.len < n) n = source->v.tuple.len;
        if (target->v.tuple.len < n) n = target->v.tuple.len;
        for (u32 i = 0; i < n; i++) {
            groove_Value r;
            jz_err e = jzn_rebind_terminal_value(value.v.vals.ptr[i], &source->v.tuple.ptr[i], &target->v.tuple.ptr[i], &r);
            if (JZ_IS_ERR(e)) {
                value.v.vals.ptr[i] = groove_value_null();
                groove_Value_drop(&value);
                return e;
            }
            value.v.vals.ptr[i] = r;
        }
        jz_vec_truncate(&value.v.vals, n, sizeof(groove_Value), groove_Value_drop);
        *out = value;
        return JZ_OK;
    }
    if (value.tag == GROOVE_V_ARRAY && source->tag == GROOVE_VT_ARRAY && target->tag == GROOVE_VT_ARRAY) {
        for (u32 i = 0; i < value.v.vals.len; i++) {
            groove_Value r;
            jz_err e = jzn_rebind_terminal_value(value.v.vals.ptr[i], source->v.inner, target->v.inner, &r);
            if (JZ_IS_ERR(e)) {
                value.v.vals.ptr[i] = groove_value_null();
                groove_Value_drop(&value);
                return e;
            }
            value.v.vals.ptr[i] = r;
        }
        *out = value;
        return JZ_OK;
    }
    if (value.tag == GROOVE_V_NULLABLE && source->tag == GROOVE_VT_NULLABLE && target->tag == GROOVE_VT_NULLABLE) {
        if (!value.v.nullable) {
            *out = value;
            return JZ_OK;
        }
        groove_Value inner = *value.v.nullable, r;
        jz_free(value.v.nullable);
        JZ_TRY(jzn_rebind_terminal_value(inner, source->v.inner, target->v.inner, &r));
        *out = groove_value_some(r);
        return JZ_OK;
    }
    if (value.tag == GROOVE_V_RECORD && source->tag == GROOVE_VT_RECORD && target->tag == GROOVE_VT_RECORD) {
        groove_ValueVec values = { 0 };
        jz_err e = groove__records__OwnedRecord__to_values(&value.v.record, &values);
        groove_Value_drop(&value);
        if (JZ_IS_ERR(e)) {
            jz_err_drop(&e);
            return JZN_ISV("invalid structured terminal record");
        }
        jz_bytes raw;
        JZ_TRY(jzn_rebind_terminal_record_values(*source->v.record, *target->v.record, values, "structured terminal nested re-encoding failed", &raw));
        out->tag = GROOVE_V_RECORD;
        out->v.record.raw = raw;
        out->v.record.descriptor = *target->v.record;
        return JZ_OK;
    }
    if (value.tag == GROOVE_V_ENUM && source->tag == GROOVE_VT_ENUM && target->tag == GROOVE_VT_ENUM) {
        u32 tag = value.v.en.tag;
        const groove_EnumCase *source_case, *target_case;
        jz_err e = groove__records__values__EnumSchema__case(source->v.enm, tag, &source_case);
        if (JZ_IS_ERR(e)) {
            jz_err_drop(&e);
            groove_Value_drop(&value);
            return JZN_ISV("invalid structured terminal enum tag");
        }
        e = groove__records__values__EnumSchema__case(target->v.enm, tag, &target_case);
        if (JZ_IS_ERR(e)) {
            jz_err_drop(&e);
            groove_Value_drop(&value);
            return JZN_ISV("prepared terminal enum tag is absent");
        }
        groove_Value record = { GROOVE_V_RECORD, { 0 } };
        record.v.record = value.v.en.record;
        groove_ValueType st = groove_vt_record(source_case->payload), tt = groove_vt_record(target_case->payload);
        groove_Value rebound;
        e = jzn_rebind_terminal_value(record, &st, &tt, &rebound);
        groove_ValueType_drop(&st);
        groove_ValueType_drop(&tt);
        if (JZ_IS_ERR(e)) return e;
        if (rebound.tag != GROOVE_V_RECORD) jz_panic("record rebinding preserves record value");
        out->tag = GROOVE_V_ENUM;
        groove__records__EnumValue__new(&out->v.en, tag, rebound.v.record);
        return JZ_OK;
    }
    *out = value;
    return JZ_OK;
}

/* rebind_terminal_record_values: rebind each field value of a `source` record to `target`'s layout and encode it under `target`. */
static jz_err jzn_rebind_terminal_record_values(groove_RecordDescriptor source, groove_RecordDescriptor target, groove_ValueVec values,
                                                const char *encoding_failed, jz_bytes *out) {
    const groove_DescriptorFieldVec *s = groove__records__RecordDescriptor__fields(source);
    const groove_DescriptorFieldVec *t = groove__records__RecordDescriptor__fields(target);
    u32 n = values.len;
    if (s->len < n) n = s->len;
    if (t->len < n) n = t->len;
    groove_ValueVec rebound = { 0 };
    jz_err e = JZ_OK;
    u32 i = 0;
    for (; i < n; i++) {
        groove_Value r;
        e = jzn_rebind_terminal_value(values.ptr[i], &s->ptr[i].value_type, &t->ptr[i].value_type, &r);
        if (JZ_IS_ERR(e)) { i++; break; }
        JZ_VEC_PUSH(rebound, r);
    }
    for (; i < values.len; i++) groove_Value_drop(&values.ptr[i]);
    jz_free(values.ptr);
    if (!JZ_IS_ERR(e)) {
        e = groove__records__RecordDescriptor__create(target, rebound.ptr, rebound.len, out);
        if (JZ_IS_ERR(e)) {
            jz_err_drop(&e);
            e = JZN_ISV(encoding_failed);
        }
    }
    jz_vec_drop(&rebound, sizeof(groove_Value), groove_Value_drop);
    return e;
}

static jz_err jzn_reencode_terminal_root_record(groove_RecordDescriptor source, groove_RecordDescriptor target, jz_slice raw, jz_bytes *out) {
    groove_BorrowedRecord bound = { raw, &source };
    groove_ValueVec values = { 0 };
    jz_err e = groove__records__BorrowedRecord__to_values(&bound, &values);
    if (JZ_IS_ERR(e)) {
        jz_err_drop(&e);
        return JZN_ISV("invalid structured terminal record");
    }
    return jzn_rebind_terminal_record_values(source, target, values, "structured terminal root re-encoding failed", out);
}

/* rebind_terminal_operation_to_layout: rebind a runtime terminal operation to its early-bound prepared layout. */
static jz_err jzn_rebind_terminal_operation_to_layout(groove_TerminalOperation *operation, const jazz_node_TerminalRootLayout *layout) {
    if (groove_RecordDescriptor_eq(operation->root_descriptor, layout->root_descriptor)) return JZ_OK;
    if (jzn_trace()) {
        jz_str line = jz_str_from("JAZZ_COVERED_INPUT_TRACE terminal_descriptor_mismatch operation=");
        jz_str_push_cstr(&line, "<descriptor> layout=<descriptor>");
        jz_eprint(&line);
        jz_str_drop(&line);
    }
    if (!jzn_terminal_descriptor_can_rebind_to_layout(operation->root_descriptor, layout->root_descriptor) ||
        !jzn_terminal_nested_collection_layout_agrees(operation, layout->root_descriptor))
        return JZN_ISV("structured terminal operation descriptor disagrees with prepared root layout");
    if ((operation->edit.tag == GROOVE_TE_INSERT || operation->edit.tag == GROOVE_TE_UPDATE) && operation->path.len == 0) {
        jz_bytes reencoded;
        JZ_TRY(jzn_reencode_terminal_root_record(operation->root_descriptor, layout->root_descriptor, jz_bytes_slice(&operation->edit.value), &reencoded));
        jz_bytes_drop(&operation->edit.value);
        operation->edit.value = reencoded;
    }
    operation->root_descriptor = layout->root_descriptor;
    return JZ_OK;
}

/* ======================= structured app rows ======================= */
/* terminal_root_uuid_from_key: root collector keys begin with the root source
 * occurrence; for the single-root shape that is one ordered UUID scalar. */
static bool jzn_terminal_root_uuid_from_key(const jz_bytes *key, jazz_types_RowUuid *out) {
    if (key->len < 1 || key->ptr[0] != 10) return false;
    if (key->len < 17) return false;
    memcpy(out->b, key->ptr + 1, 16);
    return true;
}

/* structured_app_row_for_terminal_key */
static jz_err jzn_structured_app_row_for_terminal_key(const MSVT *m, const jz_bytes *terminal_key, bool *some, groove_OwnedRecord *out) {
    *some = false;
    jazz_node_TerminalRecordState *const *state = jz_btree_get(&m->structured_terminal_records, terminal_key);
    if (state) {
        jz_err e = jazz_node__node__terminal_record__TerminalRecordState__record(*state, out);
        if (JZ_IS_ERR(e)) {
            jz_err_drop(&e);
            return JZN_ISV("cannot encode retained terminal tree");
        }
        *some = true;
        return JZ_OK;
    }
    if (!m->has_structured_app_row_descriptor) return JZ_OK;
    const jz_btree *records = jz_btree_get(&m->structured_app_rows, terminal_key);
    if (!records) return JZ_OK;
    jz_bytes *raw;
    i64 *weight;
    JZ_BTREE_FOR(records, raw, weight) {
        if (*weight > 0) {
            out->raw = jz_bytes_clone(raw);
            out->descriptor = m->structured_app_row_descriptor;
            *some = true;
            break;
        }
    }
    return JZ_OK;
}

jz_err MSV(MaintainedSubscriptionView__structured_app_rows_by_terminal_key)(const MSVT *m, jazz_node_TerminalKeyRecordVec *out) {
    jazz_node_TerminalKeyRecordVec rows = { 0 };
    JZ_VEC_FOR(const jz_bytes, key, m->structured_root_key_order) {
        if (!jz_btree_contains(&m->structured_root_keys, key)) continue;
        bool some;
        groove_OwnedRecord record;
        jz_err e = jzn_structured_app_row_for_terminal_key(m, key, &some, &record);
        if (JZ_IS_ERR(e)) {
            JZ_VEC_FOR(jazz_node_TerminalKeyRecord, r, rows) { jz_bytes_drop(&r->terminal_key); groove_OwnedRecord_drop(&r->record); }
            jz_vec_drop(&rows, sizeof(jazz_node_TerminalKeyRecord), NULL);
            return e;
        }
        if (some) {
            jazz_node_TerminalKeyRecord r = { jz_bytes_clone(key), record };
            JZ_VEC_PUSH(rows, r);
        }
    }
    *out = rows;
    return JZ_OK;
}

const jz_btree *MSV(MaintainedSubscriptionView__decoded_terminal_records)(const MSVT *m) { return &m->structured_terminal_records; }

/* discard_structured_app_rows: release the app-row collector after a flat subscription's reset. */
void MSV(MaintainedSubscriptionView__discard_structured_app_rows)(MSVT *m) {
    jz_btree_clear(&m->structured_app_rows);
    jz_btree_clear(&m->structured_terminal_records);
    jz_btree_clear(&m->structured_root_keys);
    jz_vec_truncate(&m->structured_root_key_order, 0, sizeof(jz_bytes), jzn_bytes_drop_v);
    m->has_structured_app_row_descriptor = false;
    m->retains_structured_app_rows = false;
}

/* apply_structured_app_row_delta (consumes terminal_key and record) */
static void jzn_apply_structured_app_row_delta(MSVT *m, jz_bytes terminal_key, groove_OwnedRecord record, i64 weight) {
    m->has_structured_app_row_descriptor = true;
    m->structured_app_row_descriptor = record.descriptor;
    bool inserted;
    jz_bytes key_copy = jz_bytes_clone(&terminal_key);
    jz_btree *records = jz_btree_entry(&m->structured_app_rows, &key_copy, &inserted);
    if (inserted) jz_btree_init(records, &jzn_desc_bytes_i64);
    else jz_bytes_drop(&key_copy);
    const i64 *existing = jz_btree_get(records, &record.raw);
    i64 new_weight = (existing ? *existing : 0) + weight;
    if (new_weight == 0) {
        jz_btree_remove(records, &record.raw, NULL, NULL);
        jz_bytes_drop(&record.raw);
    } else if (existing) {
        *(i64 *)jz_btree_get(records, &record.raw) = new_weight;
        jz_bytes_drop(&record.raw);
    } else {
        jz_btree_insert(records, &record.raw, &new_weight, NULL);
    }
    if (records->len == 0) jz_btree_remove(&m->structured_app_rows, &terminal_key, NULL, NULL);
    jz_bytes_drop(&terminal_key);
}

/* retained_terminal_record: initialize from the retained pre-update row,
 * before a scalar payload with empty collection placeholders can replace it. */
static jz_err jzn_retained_terminal_record(MSVT *m, const jz_bytes *root_key, jazz_node_TerminalRecordState **out) {
    if (!jz_btree_contains(&m->structured_terminal_records, root_key)) {
        if (!m->has_structured_app_row_descriptor) return JZN_ISV("terminal update arrived before its root collector record");
        groove_RecordDescriptor descriptor = m->structured_app_row_descriptor;
        jz_btree *records = jz_btree_get(&m->structured_app_rows, root_key);
        if (!records) return JZN_ISV("terminal descendant operation addressed an absent retained root");
        const jz_bytes *raw = NULL;
        i64 weight = 0;
        u32 candidates = 0;
        jz_bytes *k;
        i64 *w;
        JZ_BTREE_FOR(records, k, w) {
            if (*w <= 0) continue;
            if (candidates++ == 0) { raw = k; weight = *w; }
        }
        if (candidates == 0) return JZN_ISV("terminal descendant operation addressed a non-positive retained root");
        if (candidates > 1) return JZN_ISV("terminal descendant operation addressed an ambiguous retained root");
        if (weight != 1) return JZN_ISV("collector terminal root has non-unit multiplicity");
        groove_OwnedRecord record = { jz_bytes_clone(raw), descriptor };
        jazz_node_TerminalRecordState *state;
        jz_err e = jazz_node__node__terminal_record__TerminalRecordState__new(record, &state);
        if (JZ_IS_ERR(e)) {
            jz_err_drop(&e);
            return JZN_ISV("invalid retained collector terminal record");
        }
        jz_btree_remove(&m->structured_app_rows, root_key, NULL, NULL);
        jz_bytes key = jz_bytes_clone(root_key);
        jz_btree_insert(&m->structured_terminal_records, &key, &state, NULL);
    }
    jazz_node_TerminalRecordState **slot = jz_btree_get(&m->structured_terminal_records, root_key);
    if (!slot) jz_panic("initialized above");
    *out = *slot;
    return JZ_OK;
}

static bool jzn_root_key_order_position(const MSVT *m, const jz_bytes *key, u32 *pos) {
    for (u32 i = 0; i < m->structured_root_key_order.len; i++)
        if (jz_bytes_eq(&m->structured_root_key_order.ptr[i], key)) { *pos = i; return true; }
    return false;
}
static void jzn_root_key_order_remove_all(MSVT *m, const jz_bytes *key) {
    u32 w = 0;
    for (u32 i = 0; i < m->structured_root_key_order.len; i++) {
        jz_bytes *k = &m->structured_root_key_order.ptr[i];
        if (jz_bytes_eq(k, key)) jz_bytes_drop(k);
        else m->structured_root_key_order.ptr[w++] = *k;
    }
    m->structured_root_key_order.len = w;
}
static void jzn_root_key_order_insert(MSVT *m, usize index, const jz_bytes *key) {
    u32 at = index < m->structured_root_key_order.len ? (u32)index : m->structured_root_key_order.len;
    jz_bytes k = jz_bytes_clone(key);
    jz_vec_insert(&m->structured_root_key_order, at, &k, sizeof(jz_bytes));
}

/* apply_structured_terminal_operation: fold root terminal edits into the
 * retained collector tree used by an initial/reset snapshot. */
static jz_err jzn_apply_structured_terminal_operation(MSVT *m, const groove_TerminalOperation *operation) {
    jazz_types_RowUuid root;
    /* A descendant edit has the root collector's key, but its value is a
     * nested element rather than an app-row record. */
    if (operation->path.len != 0) {
        const jazz_types_RowUuid *known = jz_btree_get(&m->structured_root_keys, &operation->root_key);
        if (!known && !jzn_terminal_root_uuid_from_key(&operation->root_key, &root))
            return JZN_ISV("terminal descendant operation addresses an unknown root key");
        if (!m->has_structured_app_row_descriptor) return JZN_ISV("terminal descendant operation arrived before its root collector record");
        if (!groove_RecordDescriptor_eq(m->structured_app_row_descriptor, operation->root_descriptor))
            return JZN_ISV("terminal descendant descriptor disagrees with retained collector layout");
        jazz_node_TerminalRecordState *state;
        JZ_TRY(jzn_retained_terminal_record(m, &operation->root_key, &state));
        jz_err e = jazz_node__node__terminal_record__TerminalRecordState__apply(state, operation);
        if (JZ_IS_ERR(e)) {
            jz_err_drop(&e);
            return JZN_ISV("invalid collector descendant terminal operation");
        }
        return JZ_OK;
    }
    bool root_was_present = jz_btree_contains(&m->structured_root_keys, &operation->root_key);
    if (operation->edit.tag == GROOVE_TE_INSERT || operation->edit.tag == GROOVE_TE_UPDATE) {
        u32 index;
        if (!groove__records__RecordDescriptor__field_index(operation->root_descriptor, JZ_LIT("row_uuid"), &index))
            return JZN_ISV("root collector terminal operation has no row_uuid");
        groove_BorrowedRecord borrowed = { jz_bytes_slice(&operation->edit.value), &operation->root_descriptor };
        JZ_TRY(groove__records__BorrowedRecord__get_uuid(&borrowed, index, &root));
        jz_bytes key = jz_bytes_clone(&operation->root_key);
        if (jz_btree_insert(&m->structured_root_keys, &key, &root, NULL)) jz_bytes_drop(&key);
    } else {
        const jazz_types_RowUuid *known = jz_btree_get(&m->structured_root_keys, &operation->root_key);
        if (!known && !jzn_terminal_root_uuid_from_key(&operation->root_key, &root))
            return JZN_ISV("root collector terminal edit addresses an unknown root key");
    }
    switch (operation->edit.tag) {
    case GROOVE_TE_INSERT: {
        jz_btree_remove(&m->structured_terminal_records, &operation->root_key, NULL, NULL);
        groove_OwnedRecord record = { jz_bytes_clone(&operation->edit.value), operation->root_descriptor };
        jz_btree_remove(&m->structured_app_rows, &operation->root_key, NULL, NULL);
        jzn_apply_structured_app_row_delta(m, jz_bytes_clone(&operation->root_key), record, 1);
        /* An insert may replace an existing occurrence. Fresh roots cannot be
         * in the order yet: scanning for each one would be quadratic. */
        if (root_was_present) jzn_root_key_order_remove_all(m, &operation->root_key);
        jzn_root_key_order_insert(m, operation->edit.index, &operation->root_key);
        break;
    }
    case GROOVE_TE_UPDATE: {
        groove_OwnedRecord record = { jz_bytes_clone(&operation->edit.value), operation->root_descriptor };
        jazz_node_TerminalRecordState *state;
        jz_err e = jzn_retained_terminal_record(m, &operation->root_key, &state);
        if (JZ_IS_ERR(e)) {
            groove_OwnedRecord_drop(&record);
            return e;
        }
        e = jazz_node__node__terminal_record__TerminalRecordState__update_record(state, record);
        if (JZ_IS_ERR(e)) {
            jz_err_drop(&e);
            return JZN_ISV("invalid collector terminal scalar update");
        }
        break;
    }
    case GROOVE_TE_REMOVE:
        jz_btree_remove(&m->structured_terminal_records, &operation->root_key, NULL, NULL);
        jz_btree_remove(&m->structured_root_keys, &operation->root_key, NULL, NULL);
        jzn_root_key_order_remove_all(m, &operation->root_key);
        jz_btree_remove(&m->structured_app_rows, &operation->root_key, NULL, NULL);
        break;
    case GROOVE_TE_MOVE: {
        u32 previous;
        if (!jzn_root_key_order_position(m, &operation->root_key, &previous))
            return JZN_ISV("root collector terminal move addresses an absent root key");
        jz_bytes removed;
        jz_vec_remove(&m->structured_root_key_order, previous, &removed, sizeof(jz_bytes));
        jz_bytes_drop(&removed);
        jzn_root_key_order_insert(m, operation->edit.index, &operation->root_key);
        break;
    }
    }
    return JZ_OK;
}

/* ======================= ResultTransitions ======================= */
void jazz_node_ResultTransitions_default(jazz_node_ResultTransitions *t) {
    memset(t, 0, sizeof *t);
    jz_btree_init(&t->authoritative_member_adds, &jzn_desc_rme_set);
}
void jazz_node_ResultTransitions_drop(void *p) {
    jazz_node_ResultTransitions *t = p;
    jz_btree_drop(&t->authoritative_member_adds);
    jzn_rme_vec_drop(&t->adds);
    jzn_rme_vec_drop(&t->removes);
    jzn_pair_vec_drop(&t->result_payload_adds);
    jzn_rme_vec_drop(&t->result_payload_removes);
    jzn_pfe_vec_drop(&t->program_fact_adds);
    jzn_pfe_vec_drop(&t->program_fact_removes);
    jz_vec_drop(&t->terminal_operations, sizeof(groove_TerminalOperation), groove_TerminalOperation_drop);
}
/* move all of src's elements onto the end of dst */
static void jzn_vec_append(void *dst, void *src, usize elem) {
    jz_vec *s = src;
    jz_vec_extend(dst, s->ptr, s->len, elem);
    jz_free(s->ptr);
    s->ptr = NULL;
    s->len = s->cap = 0;
}

/* ======================= net events ======================= */
enum { JZN_EI_RESULT = 0, JZN_EI_VERSION, JZN_EI_REPLACEMENT, JZN_EI_SHARED_VERSION, JZN_EI_PROGRAM_FACT, JZN_EI_STRUCTURED_APP_ROW };
/* EventIdentity */
typedef struct jzn_EventIdentity {
    u32 tag;
    RME *member;                         /* Rc<ResultMemberEntry> */
    jazz_protocol_ProgramSourceId source;
    jzn_ReplacementKey key;
    jzn_VersionIdentity identity;
    PFE *fact;                           /* Rc<ProgramFactEntry> */
    jazz_types_RowUuid root;
    jz_bytes raw;
} jzn_EventIdentity;
/* NetEvent (Result / AggregateResult share the Rc member and payload) */
enum { JZN_NE_RESULT = 0, JZN_NE_AGGREGATE_RESULT, JZN_NE_VERSION, JZN_NE_REPLACEMENT, JZN_NE_SHARED_VERSION, JZN_NE_PROGRAM_FACT,
       JZN_NE_STRUCTURED_APP_ROW };
typedef struct jzn_NetEvent {
    u32 tag;
    RME *member;
    RMPE payload;
    jazz_engine_SyntheticResultMembershipSchema synthetic;
    jazz_engine_StrVec value_fields;
    jazz_protocol_ProgramSourceId source;
    jzn_ReplacementKey key;
    jzn_VersionIdentity identity;
    VROW row;
    PFE *fact;
    jazz_types_RowUuid root;
    groove_OwnedRecord record;
} jzn_NetEvent;
typedef struct jzn_NetEntry { jzn_NetEvent event; i64 weight; } jzn_NetEntry;

static const char *const jzn_net_event_names[] = { "Result", "AggregateResult", "Version", "Replacement", "SharedVersion", "ProgramFact",
                                                   "StructuredAppRow" };

static int jzn_event_identity_cmp(const void *a, const void *b, void *ctx) {
    const jzn_EventIdentity *x = a, *y = b;
    if (x->tag != y->tag) return x->tag < y->tag ? -1 : 1;
    int c;
    switch (x->tag) {
    case JZN_EI_RESULT: return jazz_protocol__protocol__ResultMemberEntry__cmp(x->member, y->member, ctx);
    case JZN_EI_VERSION:
    case JZN_EI_SHARED_VERSION:
        c = jazz_protocol__protocol__ProgramSourceId__cmp(&x->source, &y->source, ctx);
        return c ? c : jzn_version_identity_cmp(&x->identity, &y->identity, ctx);
    case JZN_EI_REPLACEMENT:
        c = jazz_protocol__protocol__ProgramSourceId__cmp(&x->source, &y->source, ctx);
        if (c) return c;
        c = jzn_replacement_key_cmp(&x->key, &y->key, ctx);
        return c ? c : jzn_version_identity_cmp(&x->identity, &y->identity, ctx);
    case JZN_EI_PROGRAM_FACT: return jazz_protocol__protocol__ProgramFactEntry__cmp(x->fact, y->fact, ctx);
    default:
        c = jz_uuid_cmp(&x->root, &y->root);
        return c ? c : jz_bytes_cmp(&x->raw, &y->raw);
    }
}
static void jzn_event_identity_drop(void *p) {
    jzn_EventIdentity *i = p;
    switch (i->tag) {
    case JZN_EI_RESULT: jz_rc_release(i->member); break;
    case JZN_EI_VERSION:
    case JZN_EI_SHARED_VERSION:
    case JZN_EI_REPLACEMENT:
        jazz_protocol__protocol__ProgramSourceId__drop(&i->source);
        jzn_version_identity_drop(&i->identity);
        break;
    case JZN_EI_PROGRAM_FACT: jz_rc_release(i->fact); break;
    default: jz_bytes_drop(&i->raw); break;
    }
}
static void jzn_net_event_drop(jzn_NetEvent *e) {
    switch (e->tag) {
    case JZN_NE_RESULT:
    case JZN_NE_AGGREGATE_RESULT:
        jz_rc_release(e->member);
        jazz_protocol__protocol__ResultMemberPayloadEntry__drop(&e->payload);
        if (e->tag == JZN_NE_AGGREGATE_RESULT) {
            jazz_engine_SyntheticResultMembershipSchema_drop(&e->synthetic);
            jz_vec_drop(&e->value_fields, sizeof(jz_str), jzn_str_drop_v);
        }
        break;
    case JZN_NE_VERSION:
    case JZN_NE_REPLACEMENT:
    case JZN_NE_SHARED_VERSION:
        jazz_protocol__protocol__ProgramSourceId__drop(&e->source);
        jzn_version_identity_drop(&e->identity);
        jazz_node_VersionRow_drop(&e->row);
        break;
    case JZN_NE_PROGRAM_FACT: jz_rc_release(e->fact); break;
    default: groove_OwnedRecord_drop(&e->record); break;
    }
}
static void jzn_net_entry_drop(void *p) { jzn_net_event_drop(&((jzn_NetEntry *)p)->event); }
static const jz_btree_desc jzn_desc_net = { sizeof(jzn_EventIdentity), sizeof(jzn_NetEntry), jzn_event_identity_cmp, jzn_event_identity_drop,
    jzn_net_entry_drop };

static void jzn_rme_rc_drop(void *p) { jazz_protocol__protocol__ResultMemberEntry__drop(p); }
static void jzn_pfe_rc_drop(void *p) { jazz_protocol__protocol__ProgramFactEntry__drop(p); }
static RME *jzn_rc_member(RME member) {
    RME *rc = jz_rc_new(sizeof(RME), jzn_rme_rc_drop);
    *rc = member;
    return rc;
}
static PFE *jzn_rc_fact(PFE fact) {
    PFE *rc = jz_rc_new(sizeof(PFE), jzn_pfe_rc_drop);
    *rc = fact;
    return rc;
}
/* Rc::unwrap_or_clone */
static RME jzn_rc_member_take(RME *rc) {
    RME out = jzn_rme_clone(rc);
    jz_rc_release(rc);
    return out;
}
static PFE jzn_rc_fact_take(PFE *rc) {
    PFE out = jzn_pfe_clone(rc);
    jz_rc_release(rc);
    return out;
}

/* NetEvent::identity */
static jzn_EventIdentity jzn_net_event_identity(const jzn_NetEvent *e) {
    jzn_EventIdentity id;
    memset(&id, 0, sizeof id);
    switch (e->tag) {
    case JZN_NE_RESULT:
    case JZN_NE_AGGREGATE_RESULT:
        id.tag = JZN_EI_RESULT;
        id.member = jz_rc_clone(e->member);
        break;
    case JZN_NE_VERSION:
    case JZN_NE_SHARED_VERSION:
        id.tag = e->tag == JZN_NE_VERSION ? JZN_EI_VERSION : JZN_EI_SHARED_VERSION;
        jazz_protocol__protocol__ProgramSourceId__clone(&id.source, &e->source);
        id.identity = jzn_version_identity_clone(&e->identity);
        break;
    case JZN_NE_REPLACEMENT:
        id.tag = JZN_EI_REPLACEMENT;
        jazz_protocol__protocol__ProgramSourceId__clone(&id.source, &e->source);
        id.key = e->key;
        id.identity = jzn_version_identity_clone(&e->identity);
        break;
    case JZN_NE_PROGRAM_FACT:
        id.tag = JZN_EI_PROGRAM_FACT;
        id.fact = jz_rc_clone(e->fact);
        break;
    default:
        id.tag = JZN_EI_STRUCTURED_APP_ROW;
        id.root = e->root;
        id.raw = jz_bytes_clone(&e->record.raw);
        break;
    }
    return id;
}

/* DecodedMaintainedEvent -> NetEvent (consumes the decoded event) */
static jzn_NetEvent jzn_net_event_from_decoded(jazz_node_DecodedMaintainedEvent *ev) {
    jzn_NetEvent n;
    memset(&n, 0, sizeof n);
    switch (ev->tag) {
    case JZN_DME_RESULT_CURRENT:
        n.tag = JZN_NE_RESULT;
        n.member = jzn_rc_member(ev->member);
        n.payload = ev->payload;
        break;
    case JZN_DME_AGGREGATE_RESULT:
        n.tag = JZN_NE_AGGREGATE_RESULT;
        n.member = jzn_rc_member(ev->member);
        n.payload = ev->payload;
        n.synthetic = ev->synthetic;
        n.value_fields = ev->value_fields;
        break;
    case JZN_DME_VERSION_CONTENT:
    case JZN_DME_VERSION_DELETION:
        n.tag = JZN_NE_VERSION;
        n.identity = jzn_version_identity_for_row(&ev->row);
        n.source = ev->source;
        n.row = ev->row;
        break;
    case JZN_DME_REPLACEMENT_CONTENT:
    case JZN_DME_REPLACEMENT_DELETION:
        n.tag = JZN_NE_REPLACEMENT;
        n.identity = jzn_version_identity_for_row(&ev->row);
        n.key = jzn_replacement_key_for_row(&ev->row);
        n.source = ev->source;
        n.row = ev->row;
        break;
    case JZN_DME_SHARED_VERSION:
        n.tag = JZN_NE_SHARED_VERSION;
        n.identity = jzn_version_identity_for_row(&ev->row);
        n.source = ev->source;
        n.row = ev->row;
        break;
    case JZN_DME_PROGRAM_SOURCE_COVERAGE: {
        PFE fact;
        memset(&fact, 0, sizeof fact);
        fact.tag = JAZZ_PFE_PROGRAM_SOURCE_COVERAGE;
        fact.u.program_source = ev->coverage;
        n.tag = JZN_NE_PROGRAM_FACT;
        n.fact = jzn_rc_fact(fact);
        break;
    }
    case JZN_DME_RELATION_EDGE: {
        PFE fact;
        memset(&fact, 0, sizeof fact);
        fact.tag = JAZZ_PFE_RELATION_EDGE;
        fact.u.relation_edge = ev->edge;
        n.tag = JZN_NE_PROGRAM_FACT;
        n.fact = jzn_rc_fact(fact);
        break;
    }
    default:
        n.tag = JZN_NE_STRUCTURED_APP_ROW;
        n.root = ev->root;
        n.record = ev->record;
        break;
    }
    return n;
}

/* ======================= result membership ======================= */
static void jzn_mark_result_member_changed(MSVT *m, const RME *member) {
    if (m->has_unreconciled_result_members) jzn_members_insert(&m->unreconciled_result_members, jzn_rme_clone(member));
    if (m->has_witness_gated_journal && !jz_btree_contains(&m->witness_gated_journal.changed, member)) {
        RME c = jzn_rme_clone(member);
        jz_btree_insert(&m->witness_gated_journal.changed, &c, NULL, NULL);
    }
}

static i64 jzn_result_weight(const MSVT *m, const RME *member) {
    const i64 *w = jz_btree_get(&m->result_weights.entries, member);
    return w ? *w : 0;
}

/* apply_result_delta (consumes entry and payload) */
static void jzn_apply_result_delta(MSVT *m, RME entry, RMPE payload, i64 weight, jazz_node_ResultTransitions *t) {
    jzn_mark_result_member_changed(m, &entry);
    i64 old = jzn_result_weight(m, &entry);
    i64 next = old + weight;
    bool payload_kept = false;
    if (old <= 0 && next > 0) {
        JZ_VEC_PUSH(t->adds, jzn_rme_clone(&entry));
        const jazz_protocol_RealRowMemberEntry *row = jazz_protocol__protocol__ResultMemberEntry__as_real_row(&entry);
        if (row && row->row_digest.some) {
            jazz_protocol_MemberPayloadPair pair = { jzn_rme_clone(&entry), jzn_rmpe_clone(&payload) };
            JZ_VEC_PUSH(t->result_payload_adds, pair);
            jzn_rrm_insert(&m->result_payloads, jzn_rme_clone(&entry), &payload, NULL);
            payload_kept = true;
        }
    }
    if (!payload_kept) jazz_protocol__protocol__ResultMemberPayloadEntry__drop(&payload);
    if (old > 0 && next <= 0) {
        JZ_VEC_PUSH(t->removes, jzn_rme_clone(&entry));
        JZ_VEC_PUSH(t->result_payload_removes, jzn_rme_clone(&entry));
        jzn_rrm_remove(&m->result_payloads, &entry, NULL);
    }
    if (next == 0) {
        jzn_rrm_remove(&m->result_weights, &entry, NULL);
        jazz_protocol__protocol__ResultMemberEntry__drop(&entry);
    } else {
        jzn_rrm_insert(&m->result_weights, entry, &next, NULL);
    }
}

/* aggregate_payload_for_stable_member */
static bool jzn_aggregate_payload_for_stable_member(const MSVT *m, const RME *member, RME *old_member, RMPE *old_payload) {
    if (member->tag != JAZZ_RME_SYNTHETIC) return false;
    RME *candidate;
    RMPE *payload;
    JZ_BTREE_FOR(&m->result_payloads.entries, candidate, payload) {
        if (candidate->tag == JAZZ_RME_SYNTHETIC && jz_str_eq(&candidate->u.synthetic.table, &member->u.synthetic.table) &&
            jz_bytes_eq(&candidate->u.synthetic.row, &member->u.synthetic.row)) {
            *old_member = jzn_rme_clone(candidate);
            *old_payload = jzn_rmpe_clone(payload);
            return true;
        }
    }
    return false;
}

static void jzn_push_result_payload_fact(jazz_protocol_ProgramFactEntryVec *v, RMPE payload) {
    PFE fact;
    memset(&fact, 0, sizeof fact);
    fact.tag = JAZZ_PFE_RESULT_PAYLOAD;
    fact.u.result_payload = payload;
    JZ_VEC_PUSH(*v, fact);
}

/* apply_aggregate_result_delta (consumes member and payload) */
static jz_err jzn_apply_aggregate_result_delta(MSVT *m, RME member, RMPE payload, const jazz_engine_SyntheticResultMembershipSchema *synthetic,
                                               const jazz_engine_StrVec *value_fields, i64 weight, jazz_node_ResultTransitions *t) {
    (void)synthetic;
    (void)value_fields;
    jzn_mark_result_member_changed(m, &member);
    RME old_member;
    RMPE old_payload;
    bool has_old = jzn_aggregate_payload_for_stable_member(m, &member, &old_member, &old_payload);
    if (weight < 0) {
        /* Groove's aggregate operator emits complete before/after group rows.
         * A retraction removes only the payload it names. */
        if (has_old && jazz_protocol__protocol__ResultMemberEntry__eq(&old_member, &member)) {
            JZ_VEC_PUSH(t->removes, jzn_rme_clone(&member));
            jzn_rrm_remove(&m->result_weights, &member, NULL);
            RMPE existing;
            if (jzn_rrm_remove(&m->result_payloads, &member, &existing)) {
                JZ_VEC_PUSH(t->result_payload_removes, jzn_rme_clone(&member));
                jzn_push_result_payload_fact(&t->program_fact_removes, existing);
            }
        }
        if (has_old) {
            jazz_protocol__protocol__ResultMemberEntry__drop(&old_member);
            jazz_protocol__protocol__ResultMemberPayloadEntry__drop(&old_payload);
        }
        jazz_protocol__protocol__ResultMemberEntry__drop(&member);
        jazz_protocol__protocol__ResultMemberPayloadEntry__drop(&payload);
        return JZ_OK;
    }
    if (has_old && !jazz_protocol__protocol__ResultMemberEntry__eq(&old_member, &member)) {
        jzn_mark_result_member_changed(m, &old_member);
        JZ_VEC_PUSH(t->removes, jzn_rme_clone(&old_member));
        jzn_rrm_remove(&m->result_weights, &old_member, NULL);
        RMPE existing;
        if (jzn_rrm_remove(&m->result_payloads, &old_member, &existing)) {
            jazz_protocol__protocol__ResultMemberPayloadEntry__drop(&old_payload);
        } else {
            existing = old_payload;
        }
        JZ_VEC_PUSH(t->result_payload_removes, jzn_rme_clone(&old_member));
        jzn_push_result_payload_fact(&t->program_fact_removes, existing);
        jazz_protocol__protocol__ResultMemberEntry__drop(&old_member);
    } else if (has_old) {
        jazz_protocol__protocol__ResultMemberEntry__drop(&old_member);
        jazz_protocol__protocol__ResultMemberPayloadEntry__drop(&old_payload);
    }
    if (jzn_result_weight(m, &member) <= 0) JZ_VEC_PUSH(t->adds, jzn_rme_clone(&member));
    jzn_push_result_payload_fact(&t->program_fact_adds, jzn_rmpe_clone(&payload));
    jazz_protocol_MemberPayloadPair pair = { jzn_rme_clone(&member), jzn_rmpe_clone(&payload) };
    JZ_VEC_PUSH(t->result_payload_adds, pair);
    jzn_rrm_insert(&m->result_payloads, jzn_rme_clone(&member), &payload, NULL);
    i64 one = 1;
    jzn_rrm_insert(&m->result_weights, member, &one, NULL);
    return JZ_OK;
}

/* apply_program_fact_delta: Some(is_present) when presence flipped. */
static bool jzn_apply_program_fact_delta(MSVT *m, PFE fact, i64 weight, bool *is_present) {
    bool inserted;
    PFE key = jzn_pfe_clone(&fact);
    i64 *retained = jz_btree_entry(&m->program_fact_weights, &key, &inserted);
    if (!inserted) jazz_protocol__protocol__ProgramFactEntry__drop(&key);
    bool before = *retained > 0;
    *retained += weight;
    bool after = *retained > 0;
    if (*retained == 0) jz_btree_remove(&m->program_fact_weights, &fact, NULL, NULL);
    jazz_protocol__protocol__ProgramFactEntry__drop(&fact);
    *is_present = after;
    return before != after;
}

/* result_member_has_inline_content_source */
static bool jzn_result_member_has_inline_content_source(const MSVT *m, const RME *member) {
    const jazz_protocol_RealRowMemberEntry *row = jazz_protocol__protocol__ResultMemberEntry__as_real_row(member);
    return row && row->branch_or_prefix.some && jz_btree_contains(&m->inline_content_branch_keys, &row->branch_or_prefix.v);
}

static bool jzn_row_uuid_is(const VROW *version, const jazz_types_RowUuid *row) {
    jazz_types_RowUuid u = jazz_node__node__codec__VersionRow__row_uuid(version);
    return jz_uuid_eq(&u, row);
}

/* result_member_has_bundle_witness */
static bool jzn_result_member_has_bundle_witness(const MSVT *m, const RME *member, const jazz_node_NodeAliases *node_aliases) {
    jazz_protocol_ResultRowEntry row;
    /* Synthetic aggregate output is self-contained in its payload fact, so it
     * has no Stream B history-row witness. */
    if (!jazz_protocol__protocol__ResultMemberEntry__as_row(member, &row)) return true;
    /* The retained index already orders by table, row and layer within each transaction. */
    if (jzn_wvi_has_content_witness(&m->versions, row.tx, row.table, row.row)) return true;
    SROW *fact;
    VROW *version;
    JZ_BTREE_FOR(&m->selected_deletion_witnesses, fact, version) {
        if (jazz_types__ids__TxId__cmp(&fact->version.tx, &row.tx, NULL) == 0 && jz_str_eq(version->table, row.table) &&
            jzn_row_uuid_is(version, &row.row) &&
            !jazz_node__node__codec__VersionRow__is_deleted(version))
            return true;
    }
    VROW content;
    if (jzn_ri_replacement_for(&m->replacements, row.table, row.row, &content, NULL, NULL)) {
        jazz_types_TxId tx;
        bool same = jazz_node__node__codec__version_tx_id_from_aliases(&content, node_aliases, &tx) &&
                    jazz_types__ids__TxId__cmp(&tx, &row.tx, NULL) == 0;
        jazz_node_VersionRow_drop(&content);
        if (same) return true;
    }
    return false;
}

static void jzn_new_journal(MSVT *m, jz_btree withheld, const jazz_node_NodeAliases *node_aliases) {
    m->has_witness_gated_journal = true;
    jz_btree_init(&m->witness_gated_journal.changed, &jzn_desc_rme_set);
    m->witness_gated_journal.withheld = withheld;
    m->witness_gated_journal.alias_retargets = jazz_node__node__node_aliases__NodeAliases__retarget_count(node_aliases);
    m->witness_gated_journal.alias_count = jazz_node__node__node_aliases__NodeAliases__len(node_aliases);
}

/* reconcile_publishable_result_members -> (adds, removes, payload_adds, payload_removes) */
static void jzn_reconcile_publishable_result_members(MSVT *m, const jazz_node_NodeAliases *node_aliases, jazz_protocol_ResultMemberEntryVec *adds,
                                                     jazz_protocol_ResultMemberEntryVec *removes, jazz_protocol_MemberPayloadPairVec *payload_adds,
                                                     jazz_protocol_ResultMemberEntryVec *payload_removes) {
    memset(adds, 0, sizeof *adds);
    memset(removes, 0, sizeof *removes);
    if (m->has_unreconciled_result_members) {
        jzn_RetainedResultMembers candidates = m->unreconciled_result_members;
        m->has_unreconciled_result_members = false;
        JZ_ASSERT(m->storage_backed_result_materialization, "unreconciled members imply storage-backed materialization");
        RME member;
        while (jz_btree_pop_first(&candidates.entries, &member, NULL)) {
            bool present = jzn_result_weight(m, &member) > 0;
            bool published = jz_btree_contains(&m->published_result_members.entries, &member);
            if (!published && present) {
                jzn_members_insert(&m->published_result_members, jzn_rme_clone(&member));
                JZ_VEC_PUSH(*adds, member);
            } else if (published && !present) {
                jzn_members_remove(&m->published_result_members, &member);
                JZ_VEC_PUSH(*removes, member);
            } else {
                jazz_protocol__protocol__ResultMemberEntry__drop(&member);
            }
        }
        jz_btree_drop(&candidates.entries);
    } else {
        bool use_journal = false;
        jzn_WitnessGatedJournal journal;
        if (m->has_witness_gated_journal) {
            journal = m->witness_gated_journal;
            m->has_witness_gated_journal = false;
            use_journal = !m->storage_backed_result_materialization &&
                          journal.alias_retargets == jazz_node__node__node_aliases__NodeAliases__retarget_count(node_aliases) &&
                          journal.alias_count <= jazz_node__node__node_aliases__NodeAliases__len(node_aliases);
            if (!use_journal) jzn_journal_drop(&journal);
        }
        if (use_journal) {
            /* Visit in member order so adds and removes are ordered exactly as
             * the full reconcile's set differences below. */
            jz_btree changed = journal.changed;
            jz_btree_append(&changed, &journal.withheld);
            jz_btree_drop(&journal.withheld);
            jz_btree withheld;
            jz_btree_init(&withheld, &jzn_desc_rme_set);
            RME member;
            while (jz_btree_pop_first(&changed, &member, NULL)) {
                bool positive = jzn_result_weight(m, &member) > 0;
                bool publishable = positive && (jzn_result_member_has_inline_content_source(m, &member) ||
                                                jzn_result_member_has_bundle_witness(m, &member, node_aliases));
                bool published = jz_btree_contains(&m->published_result_members.entries, &member);
                if (!published && publishable) {
                    jzn_members_insert(&m->published_result_members, jzn_rme_clone(&member));
                    JZ_VEC_PUSH(*adds, member);
                } else if (published && !publishable) {
                    jzn_members_remove(&m->published_result_members, &member);
                    if (positive) {
                        RME c = jzn_rme_clone(&member);
                        jz_btree_insert(&withheld, &c, NULL, NULL);
                    }
                    JZ_VEC_PUSH(*removes, member);
                } else if (!published && !publishable && positive) {
                    jz_btree_insert(&withheld, &member, NULL, NULL);
                } else {
                    jazz_protocol__protocol__ResultMemberEntry__drop(&member);
                }
            }
            jz_btree_drop(&changed);
            jzn_new_journal(m, withheld, node_aliases);
        } else {
            jz_btree withheld, publishable;
            jz_btree_init(&withheld, &jzn_desc_rme_set);
            jz_btree_init(&publishable, &jzn_desc_rme_set);
            RME *member;
            i64 *weight;
            JZ_BTREE_FOR(&m->result_weights.entries, member, weight) {
                if (*weight <= 0) continue;
                bool ok = m->storage_backed_result_materialization || jzn_result_member_has_inline_content_source(m, member) ||
                          jzn_result_member_has_bundle_witness(m, member, node_aliases);
                RME c = jzn_rme_clone(member);
                if (ok) jz_btree_insert(&publishable, &c, NULL, NULL);
                else jz_btree_insert(&withheld, &c, NULL, NULL);
            }
            void *v;
            JZ_BTREE_FOR(&publishable, member, v) {
                (void)v;
                if (!jz_btree_contains(&m->published_result_members.entries, member)) JZ_VEC_PUSH(*adds, jzn_rme_clone(member));
            }
            JZ_BTREE_FOR(&m->published_result_members.entries, member, v) {
                (void)v;
                if (!jz_btree_contains(&publishable, member)) JZ_VEC_PUSH(*removes, jzn_rme_clone(member));
            }
            jz_btree_drop(&m->published_result_members.entries);
            jzn_members_from(&m->published_result_members, publishable);
            if (!m->storage_backed_result_materialization) jzn_new_journal(m, withheld, node_aliases);
            else jz_btree_drop(&withheld);
        }
    }
    if (m->storage_backed_result_materialization) {
        m->has_unreconciled_result_members = true;
        jzn_members_default(&m->unreconciled_result_members);
    }
    memset(payload_removes, 0, sizeof *payload_removes);
    memset(payload_adds, 0, sizeof *payload_adds);
    JZ_VEC_FOR(RME, member, *removes) {
        if (jz_btree_contains(&m->published_result_payloads.entries, member)) JZ_VEC_PUSH(*payload_removes, jzn_rme_clone(member));
    }
    JZ_VEC_FOR(RME, member, *adds) {
        const RMPE *payload = jz_btree_get(&m->result_payloads.entries, member);
        if (payload) {
            jazz_protocol_MemberPayloadPair pair = { jzn_rme_clone(member), jzn_rmpe_clone(payload) };
            JZ_VEC_PUSH(*payload_adds, pair);
        }
    }
    JZ_VEC_FOR(RME, member, *payload_removes) jzn_rrm_remove(&m->published_result_payloads, member, NULL);
    JZ_VEC_FOR(jazz_protocol_MemberPayloadPair, pair, *payload_adds) {
        RMPE p = jzn_rmpe_clone(&pair->payload);
        jzn_rrm_insert(&m->published_result_payloads, jzn_rme_clone(&pair->member), &p, NULL);
    }
}

static bool jzn_keep_non_row_pair(void *item, void *ctx) {
    (void)ctx;
    jazz_protocol_ResultRowEntry row;
    return !jazz_protocol__protocol__ResultMemberEntry__as_row(&((jazz_protocol_MemberPayloadPair *)item)->member, &row);
}
static bool jzn_keep_non_row_member(void *item, void *ctx) {
    (void)ctx;
    jazz_protocol_ResultRowEntry row;
    return !jazz_protocol__protocol__ResultMemberEntry__as_row(item, &row);
}

/* finalize_multisink_transitions: publish Stream A only across its Stream B witness boundary. */
static void jzn_finalize_multisink_transitions(MSVT *m, jazz_node_ResultTransitions *t, const jazz_node_NodeAliases *node_aliases) {
    /* Synthetic/path payloads are self-contained. Row-digest payloads are the
     * Stream-A half of a real-row membership and must cross the same witness
     * boundary as that membership. */
    jz_vec_retain(&t->result_payload_adds, sizeof(jazz_protocol_MemberPayloadPair), jzn_keep_non_row_pair, NULL, jzn_pair_drop);
    jz_vec_retain(&t->result_payload_removes, sizeof(RME), jzn_keep_non_row_member, NULL, jazz_protocol__protocol__ResultMemberEntry__drop);
    jazz_protocol_ResultMemberEntryVec adds, removes, payload_removes;
    jazz_protocol_MemberPayloadPairVec payload_adds;
    jzn_reconcile_publishable_result_members(m, node_aliases, &adds, &removes, &payload_adds, &payload_removes);
    jzn_rme_vec_drop(&t->adds);
    jzn_rme_vec_drop(&t->removes);
    t->adds = adds;
    t->removes = removes;
    jzn_vec_append(&t->result_payload_adds, &payload_adds, sizeof(jazz_protocol_MemberPayloadPair));
    jzn_vec_append(&t->result_payload_removes, &payload_removes, sizeof(RME));
}

/* ======================= applying decoded deltas ======================= */
static void jzn_trace_event(const jzn_NetEvent *event, i64 weight) {
    jz_str line = jz_format("JAZZ_COVERED_INPUT_TRACE stage=apply_decoded_event event=%s weight=%lld", jzn_net_event_names[event->tag],
                            (long long)weight);
    jz_eprint(&line);
    jz_str_drop(&line);
}

/* apply_decoded_delta_results over a fully decoded net-change accumulator.
 * No retained state changes until the complete input has decoded. */
static jz_err jzn_apply_net(MSVT *m, jz_btree *net, const jazz_node_NodeAliases *node_aliases, jazz_node_ResultTransitions *t) {
    jazz_node_ResultTransitions_default(t);
    jzn_EventIdentity identity;
    jzn_NetEntry entry;
    jz_err e = JZ_OK;
    while (jz_btree_pop_first(net, &identity, &entry)) {
        jzn_event_identity_drop(&identity);
        jzn_NetEvent *ev = &entry.event;
        i64 weight = entry.weight;
        if (weight == 0 || JZ_IS_ERR(e)) {
            jzn_net_event_drop(ev);
            continue;
        }
        if (jzn_trace()) jzn_trace_event(ev, weight);
        switch (ev->tag) {
        case JZN_NE_RESULT:
            jzn_apply_result_delta(m, jzn_rc_member_take(ev->member), ev->payload, weight, t);
            break;
        case JZN_NE_AGGREGATE_RESULT: {
            e = jzn_apply_aggregate_result_delta(m, jzn_rc_member_take(ev->member), ev->payload, &ev->synthetic, &ev->value_fields, weight, t);
            jazz_engine_SyntheticResultMembershipSchema_drop(&ev->synthetic);
            jz_vec_drop(&ev->value_fields, sizeof(jz_str), jzn_str_drop_v);
            break;
        }
        case JZN_NE_VERSION: {
            SROW covered_input;
            e = jzn_supporting_row_for_version(m, ev->source, &ev->row, node_aliases, &covered_input);
            if (JZ_IS_ERR(e)) { jzn_version_identity_drop(&ev->identity); jazz_node_VersionRow_drop(&ev->row); break; }
            jzn_VersionPayload *payload;
            e = jzn_version_payload_prepare(ev->row, &ev->identity, node_aliases, &payload);
            jzn_version_identity_drop(&ev->identity);
            if (JZ_IS_ERR(e)) { jazz_protocol__protocol__SupportingRow__drop(&covered_input); break; }
            if (weight < 0) jzn_drop_journal(m);
            jzn_wvi_apply_delta(&m->versions, payload, weight);
            jazz_node__node__supporting_frontier__SupportingFrontier__apply(&m->supporting, 0, covered_input, weight);
            t->supporting_changed = true;
            break;
        }
        case JZN_NE_REPLACEMENT: {
            SROW covered_input;
            e = jzn_supporting_row_for_version(m, ev->source, &ev->row, node_aliases, &covered_input);
            if (JZ_IS_ERR(e)) { jzn_version_identity_drop(&ev->identity); jazz_node_VersionRow_drop(&ev->row); break; }
            jzn_VersionPayload *payload;
            e = jzn_version_payload_prepare(ev->row, &ev->identity, node_aliases, &payload);
            if (JZ_IS_ERR(e)) { jzn_version_identity_drop(&ev->identity); jazz_protocol__protocol__SupportingRow__drop(&covered_input); break; }
            /* A new replacement for an already replaced row can change its
             * winner, retracting the previous winner's witness. */
            if (weight < 0 || jzn_ri_contains_key(&m->replacements, &ev->key)) jzn_drop_journal(m);
            jzn_ri_apply_delta(&m->replacements, ev->key, ev->identity, payload, weight);
            jazz_node__node__supporting_frontier__SupportingFrontier__apply(&m->supporting, 1, covered_input, weight);
            t->supporting_changed = true;
            break;
        }
        case JZN_NE_SHARED_VERSION: {
            SROW covered_input;
            e = jzn_supporting_row_for_version(m, ev->source, &ev->row, node_aliases, &covered_input);
            if (JZ_IS_ERR(e)) { jzn_version_identity_drop(&ev->identity); jazz_node_VersionRow_drop(&ev->row); break; }
            jzn_ReplacementKey key = jzn_replacement_key_for_row(&ev->row);
            jzn_VersionPayload *payload;
            e = jzn_version_payload_prepare(ev->row, &ev->identity, node_aliases, &payload);
            if (JZ_IS_ERR(e)) { jzn_version_identity_drop(&ev->identity); jazz_protocol__protocol__SupportingRow__drop(&covered_input); break; }
            if (weight < 0 || jzn_ri_contains_key(&m->replacements, &key)) jzn_drop_journal(m);
            jzn_wvi_apply_delta(&m->versions, jz_rc_clone(payload), weight);
            jzn_ri_apply_delta(&m->replacements, key, ev->identity, payload, weight);
            SROW second;
            jazz_protocol__protocol__SupportingRow__clone(&second, &covered_input);
            jazz_node__node__supporting_frontier__SupportingFrontier__apply(&m->supporting, 0, covered_input, weight);
            jazz_node__node__supporting_frontier__SupportingFrontier__apply(&m->supporting, 1, second, weight);
            t->supporting_changed = true;
            break;
        }
        case JZN_NE_PROGRAM_FACT: {
            PFE fact = jzn_rc_fact_take(ev->fact);
            if (fact.tag == JAZZ_PFE_PROGRAM_SOURCE_COVERAGE) {
                jazz_protocol__protocol__ProgramFactEntry__drop(&fact);
                break;
            }
            bool is_present;
            if (jzn_apply_program_fact_delta(m, jzn_pfe_clone(&fact), weight, &is_present)) {
                if (is_present) JZ_VEC_PUSH(t->program_fact_adds, fact);
                else JZ_VEC_PUSH(t->program_fact_removes, fact);
            } else {
                jazz_protocol__protocol__ProgramFactEntry__drop(&fact);
            }
            break;
        }
        default: /* StructuredAppRow */
            if (m->retains_structured_app_rows) {
                /* Direct app rows have no Groove positional terminal, so their
                 * public root remains their only identity. */
                jz_bytes terminal_key = jz_bytes_from(ev->root.b, 16);
                /* Direct-row keys enter `structured_root_keys` and the order
                 * together, so a fresh key map entry is a key not yet ordered. */
                jz_bytes map_key = jz_bytes_clone(&terminal_key);
                bool first_occurrence = !jz_btree_insert(&m->structured_root_keys, &map_key, &ev->root, NULL);
                if (!first_occurrence) jz_bytes_drop(&map_key);
                jzn_apply_structured_app_row_delta(m, jz_bytes_clone(&terminal_key), ev->record, weight);
                if (first_occurrence) JZ_VEC_PUSH(m->structured_root_key_order, terminal_key);
                else jz_bytes_drop(&terminal_key);
                JZ_ASSERT(m->structured_root_keys.len == m->structured_root_key_order.len,
                          "direct app-row key map and order must hold the same keys");
            } else {
                groove_OwnedRecord_drop(&ev->record);
            }
            break;
        }
    }
    if (JZ_IS_ERR(e)) jazz_node_ResultTransitions_drop(t);
    return e;
}

/* apply_typed_deltas */
jz_err MSV(MaintainedSubscriptionView__apply_typed_deltas)(MSVT *m, const jz_str *sink, const groove_RecordDeltas *deltas,
                                                           const jazz_node_MaintainedTerminalSchemas *schemas, const jzn_TableSchemas *tables,
                                                           const jazz_node_NodeAliases *node_aliases, jazz_node_ResultTransitions *out) {
    const jzn_MaintainedTerminalKind *kind;
    JZ_TRY(jzn_mts_get(schemas, sink, &kind));
    bool empty = groove__ivm__RecordDeltas__is_empty(deltas);
    bool observed_result_delta_batch = !empty && jzn_mtk_is_result_terminal(kind);
    /* Deletion witnesses are part of the membership proof: a current-row
     * anti-join can become empty solely because its deletion-register input
     * changed. */
    bool requires_reconcile = !empty && jzn_mtk_requires_authoritative_membership_reconcile(kind);
    jzn_DecodeCtx ctx;
    ctx.tables = tables;
    ctx.node_aliases = node_aliases;
    jz_btree_init(&ctx.decode_plan_cache, &jzn_desc_plan_cache);
    jz_hmap_init(&ctx.payload_plans, &jzn_desc_payload_plans);
    ctx.read_view = m->read_view;
    /* Decode into the net-change accumulator directly. */
    jz_btree net;
    jz_btree_init(&net, &jzn_desc_net);
    groove_RecordDeltasIter it = { deltas, 0 };
    groove_BorrowedRecord record;
    i64 weight;
    jz_err e = JZ_OK;
    while (groove__ivm__RecordDeltas__next(&it, &record, &weight)) {
        jazz_node_DecodedMaintainedEvent event;
        e = jzn_decode_typed_terminal_record(&record, kind, &ctx, &event);
        if (JZ_IS_ERR(e)) break;
        jzn_NetEvent net_event = jzn_net_event_from_decoded(&event);
        jzn_EventIdentity identity = jzn_net_event_identity(&net_event);
        jzn_NetEntry *existing = jz_btree_get(&net, &identity);
        if (existing) {
            existing->weight += weight;
            jzn_event_identity_drop(&identity);
            jzn_net_event_drop(&net_event);
        } else {
            jzn_NetEntry entry = { net_event, weight };
            jz_btree_insert(&net, &identity, &entry, NULL);
        }
    }
    jz_btree_drop(&ctx.decode_plan_cache);
    jz_hmap_drop(&ctx.payload_plans);
    if (JZ_IS_ERR(e)) {
        jz_btree_drop(&net);
        return e;
    }
    e = jzn_apply_net(m, &net, node_aliases, out);
    jz_btree_drop(&net);
    if (JZ_IS_ERR(e)) return e;
    if (observed_result_delta_batch) out->observed_result_delta_batches += 1;
    out->requires_authoritative_membership_reconcile = requires_reconcile;
    return JZ_OK;
}

static void jzn_trace_sink(const char *stage, const jz_str *sink, const jzn_MaintainedTerminalKind *kind, const char *what, u32 n) {
    jz_str line = jz_format("JAZZ_COVERED_INPUT_TRACE stage=%s sink=%.*s kind=%s %s=%u", stage, (int)sink->len, sink->ptr, jzn_mtk_names[kind->tag],
                            what, n);
    jz_eprint(&line);
    jz_str_drop(&line);
}

/* apply_multisink_deltas */
jz_err MSV(MaintainedSubscriptionView__apply_multisink_deltas)(MSVT *m, groove_MultisinkBatch deltas, const jazz_node_MaintainedTerminalSchemas *schemas,
                                                               const jzn_TableSchemas *tables, const jazz_node_NodeAliases *node_aliases,
                                                               jazz_node_ResultTransitions *out) {
    jazz_node_ResultTransitions t;
    jazz_node_ResultTransitions_default(&t);
    jz_err e = JZ_OK;
    /* A single IVM drain may touch the same source fact through more than one
     * terminal. Record only the first pre-state and final post-state for each
     * touched peer fact, then publish the net closure change. */
    for (u32 i = 0; i < deltas.terminal_sinks_len && !JZ_IS_ERR(e); i++) {
        const jz_str *sink;
        groove_TerminalOperationVec operations = { 0 };
        groove__ivm__MultisinkBatch__take_terminal_sink(&deltas, i, &sink, &operations);
        const jzn_MaintainedTerminalKind *kind;
        e = jzn_mts_get(schemas, sink, &kind);
        if (!JZ_IS_ERR(e) && jzn_trace() && operations.len != 0) jzn_trace_sink("terminal_operations", sink, kind, "operations", operations.len);
        if (!JZ_IS_ERR(e) && kind->tag == JZN_MTK_ROOT_COLLECTOR_APP_ROWS) {
            JZ_VEC_FOR(groove_TerminalOperation, op, operations) {
                e = jzn_rebind_terminal_operation_to_layout(op, &kind->u.root_collector.layout);
                if (JZ_IS_ERR(e)) break;
            }
            /* Keep operation order: a later full insertion starts a new tree
             * and must not inherit descendants from the removed root. */
            jz_btree removed_roots;
            jz_btree_init(&removed_roots, &jzn_desc_bytes_set);
            u32 k = 0;
            for (; k < operations.len && !JZ_IS_ERR(e); k++) {
                groove_TerminalOperation *op = &operations.ptr[k];
                if (op->path.len != 0 && jz_btree_contains(&removed_roots, &op->root_key)) {
                    /* Retractions beneath an already removed root have no target. */
                    groove_TerminalOperation_drop(op);
                    continue;
                }
                e = jzn_apply_structured_terminal_operation(m, op);
                if (JZ_IS_ERR(e)) break;
                if (op->path.len == 0) {
                    if (op->edit.tag == GROOVE_TE_REMOVE) {
                        jz_bytes key = jz_bytes_clone(&op->root_key);
                        if (jz_btree_insert(&removed_roots, &key, NULL, NULL)) jz_bytes_drop(&key);
                    } else if (op->edit.tag == GROOVE_TE_INSERT) {
                        jz_btree_remove(&removed_roots, &op->root_key, NULL, NULL);
                    }
                }
                JZ_VEC_PUSH(t.terminal_operations, *op);
            }
            for (; k < operations.len; k++) groove_TerminalOperation_drop(&operations.ptr[k]);
            jz_free(operations.ptr);
            jz_btree_drop(&removed_roots);
        } else {
            jz_vec_drop(&operations, sizeof(groove_TerminalOperation), groove_TerminalOperation_drop);
        }
    }
    for (u32 i = 0; i < deltas.sinks_len && !JZ_IS_ERR(e); i++) {
        const jz_str *sink;
        const groove_RecordDeltas *sink_deltas;
        groove__ivm__MultisinkBatch__sink(&deltas, i, &sink, &sink_deltas);
        const jzn_MaintainedTerminalKind *kind;
        e = jzn_mts_get(schemas, sink, &kind);
        if (JZ_IS_ERR(e)) break;
        if (jzn_trace() && !groove__ivm__RecordDeltas__is_empty(sink_deltas))
            jzn_trace_sink("terminal_sink", sink, kind, "records", groove__ivm__RecordDeltas__len(sink_deltas));
        /* The root collector's positional terminal is the sole owner of
         * retained app-row state; its ordinary sink delta is the same fact
         * stream without an occurrence key. */
        if (kind->tag == JZN_MTK_ROOT_COLLECTOR_APP_ROWS) continue;
        jazz_node_ResultTransitions d;
        e = MSV(MaintainedSubscriptionView__apply_typed_deltas)(m, sink, sink_deltas, schemas, tables, node_aliases, &d);
        if (JZ_IS_ERR(e)) break;
        t.supporting_changed |= d.supporting_changed;
        jzn_vec_append(&t.adds, &d.adds, sizeof(RME));
        jzn_vec_append(&t.removes, &d.removes, sizeof(RME));
        jzn_vec_append(&t.program_fact_adds, &d.program_fact_adds, sizeof(PFE));
        jzn_vec_append(&t.program_fact_removes, &d.program_fact_removes, sizeof(PFE));
        jzn_vec_append(&t.result_payload_adds, &d.result_payload_adds, sizeof(jazz_protocol_MemberPayloadPair));
        jzn_vec_append(&t.result_payload_removes, &d.result_payload_removes, sizeof(RME));
        t.observed_result_delta_batches += d.observed_result_delta_batches;
        t.requires_authoritative_membership_reconcile |= d.requires_authoritative_membership_reconcile;
        jazz_node_ResultTransitions_drop(&d);
    }
    groove_MultisinkBatch_drop(&deltas);
    if (JZ_IS_ERR(e)) {
        jazz_node_ResultTransitions_drop(&t);
        return e;
    }
    jzn_finalize_multisink_transitions(m, &t, node_aliases);
    if (jzn_trace() && (t.adds.len != 0 || t.program_fact_adds.len != 0 || t.program_fact_removes.len != 0)) {
        jz_str line = jz_format("JAZZ_COVERED_INPUT_TRACE stage=maintained_transition adds=%u removes=%u fact_adds=%u fact_removes=%u terminal_ops=%u",
                                t.adds.len, t.removes.len, t.program_fact_adds.len, t.program_fact_removes.len, t.terminal_operations.len);
        jz_eprint(&line);
        jz_str_drop(&line);
    }
    *out = t;
    return JZ_OK;
}

/* ======================= accessors ======================= */
static bool jzn_version_row_eq(const VROW *a, const VROW *b) {
    return jz_str_eq(a->table, b->table) && jazz_model_BranchKey_eq(&a->branch_key, &b->branch_key) &&
           groove_OwnedRecord_eq(&a->record, &b->record);
}
static bool jzn_version_row_eq_v(const void *a, const void *b) { return jzn_version_row_eq(a, b); }

jazz_node_VersionRowVec MSV(MaintainedSubscriptionView__versions_by_tx)(const MSVT *m, jazz_types_TxId tx_id) {
    jazz_node_VersionRowVec versions = jzn_wvi_versions_by_tx(&m->versions, tx_id);
    SROW *fact;
    VROW *version;
    JZ_BTREE_FOR(&m->selected_deletion_witnesses, fact, version) {
        if (jazz_types__ids__TxId__cmp(&fact->version.tx, &tx_id, NULL) != 0) continue;
        bool contained = false;
        JZ_VEC_FOR(VROW, v, versions) if (jzn_version_row_eq(v, version)) { contained = true; break; }
        if (!contained) JZ_VEC_PUSH(versions, jzn_vrow_clone(version));
    }
    return versions;
}

void MSV(MaintainedSubscriptionView__acknowledged_supporting_rows)(const MSVT *m, jzn_SupportingRowRefVec *out) {
    jazz_node__node__supporting_frontier__SupportingFrontier__acknowledged_rows(&m->supporting, out);
}

/* single_physical_table: the only physical table this view reads, when it reads exactly one. */
bool MSV(MaintainedSubscriptionView__single_physical_table)(const MSVT *m, const jz_str **table, jazz_types_GlobalPhysicalTableId *id) {
    if (m->physical_tables.len != 1) return false;
    groove_InternStr *k;
    jazz_types_GlobalPhysicalTableId *v;
    jz_btree_first(&m->physical_tables, (void **)&k, (void **)&v);
    *table = *k;
    *id = *v;
    return true;
}

void MSV(MaintainedSubscriptionView__supporting_rows)(const MSVT *m, jzn_SupportingRowRefVec *out) {
    jazz_node__node__supporting_frontier__SupportingFrontier__rows(&m->supporting, out);
}
bool MSV(MaintainedSubscriptionView__unpublished_supporting_delta)(const MSVT *m, jazz_protocol_SupportingRowVec *adds, jazz_protocol_SupportingRowVec *removes) {
    return jazz_node__node__supporting_frontier__SupportingFrontier__delta(&m->supporting, adds, removes);
}
void MSV(MaintainedSubscriptionView__forget_peer_source_closure_baseline)(MSVT *m) {
    jazz_node__node__supporting_frontier__SupportingFrontier__forget_predecessor(&m->supporting);
}
void MSV(MaintainedSubscriptionView__acknowledge_peer_source_closure)(MSVT *m) {
    jazz_node__node__supporting_frontier__SupportingFrontier__acknowledge(&m->supporting);
}

/* replace_selected_deletion_witnesses: selected-scope tombstones are another
 * contributor to the same physical frontier (consumes witnesses). */
bool MSV(MaintainedSubscriptionView__replace_selected_deletion_witnesses)(MSVT *m, jz_btree witnesses) {
    if (!jz_btree_eq(&m->selected_deletion_witnesses, &witnesses, jzn_version_row_eq_v)) jzn_drop_journal(m);
    bool changed = false;
    SROW *row;
    VROW *v;
    JZ_BTREE_FOR(&m->selected_deletion_witnesses, row, v) {
        (void)v;
        if (jz_btree_contains(&witnesses, row)) continue;
        SROW c;
        jazz_protocol__protocol__SupportingRow__clone(&c, row);
        jazz_node__node__supporting_frontier__SupportingFrontier__apply(&m->supporting, 3, c, -1);
        changed = true;
    }
    JZ_BTREE_FOR(&witnesses, row, v) {
        (void)v;
        if (jz_btree_contains(&m->selected_deletion_witnesses, row)) continue;
        SROW c;
        jazz_protocol__protocol__SupportingRow__clone(&c, row);
        jazz_node__node__supporting_frontier__SupportingFrontier__apply(&m->supporting, 3, c, 1);
        changed = true;
    }
    jz_btree_drop(&m->selected_deletion_witnesses);
    m->selected_deletion_witnesses = witnesses;
    return changed;
}

bool MSV(MaintainedSubscriptionView__replacement_for)(const MSVT *m, const jz_str *table, jazz_types_RowUuid row_uuid, VROW *content,
                                                      bool *has_deletion, VROW *deletion) {
    return jzn_ri_replacement_for(&m->replacements, table, row_uuid, content, has_deletion, deletion);
}

/* footprint: on-demand maintained subscription view index sizes. */
void MSV(MaintainedSubscriptionView__footprint)(const MSVT *m, jazz_node_MaintainedSubscriptionViewFootprint *out) {
    usize result_weights_bytes = jzn_rrm_footprint_bytes(&m->result_weights) +
                                 (m->has_unreconciled_result_members
                                      ? jzn_btree_set_bytes(m->unreconciled_result_members.entries.len) + m->unreconciled_result_members.entry_bytes
                                      : 0) +
                                 jzn_btree_map_bytes(m->published_result_members.entries.len) + m->published_result_members.entry_bytes;
    usize result_payloads_bytes = jzn_rrm_footprint_bytes(&m->result_payloads) + jzn_rrm_footprint_bytes(&m->published_result_payloads);
    usize supporting_frontier_bytes = jazz_node__node__supporting_frontier__SupportingFrontier__retained_bytes(&m->supporting);
    usize versions_bytes = jzn_wvi_footprint_bytes(&m->versions) + jzn_btree_map_bytes(m->selected_deletion_witnesses.len);
    SROW *srow;
    VROW *vrow;
    JZ_BTREE_FOR(&m->selected_deletion_witnesses, srow, vrow) {
        (void)srow;
        versions_bytes += jzn_version_row_bytes(vrow);
    }
    usize replacements_bytes = jzn_ri_footprint_bytes(&m->replacements);
    usize witness_table_names_bytes = jzn_btree_map_bytes(m->witness_table_names.len);
    jzn_WitnessNameKey *wk;
    jz_str *authored;
    JZ_BTREE_FOR(&m->witness_table_names, wk, authored) {
        witness_table_names_bytes += wk->table.len + authored->len + 2 * sizeof(jz_str) + sizeof(jazz_types_SchemaVersionAlias);
    }
    usize structured_app_rows_bytes = 0, structured_app_rows = 0;
    jz_bytes *key;
    jz_btree *records;
    JZ_BTREE_FOR(&m->structured_app_rows, key, records) {
        (void)key;
        jz_bytes *raw;
        i64 *w;
        JZ_BTREE_FOR(records, raw, w) {
            structured_app_rows_bytes += raw->len + sizeof(i64);
            if (*w > 0) structured_app_rows += 1;
        }
        structured_app_rows_bytes += jzn_btree_map_bytes(records->len);
    }
    structured_app_rows_bytes += jzn_btree_map_bytes(m->structured_app_rows.len) + jzn_btree_map_bytes(m->structured_terminal_records.len);
    jazz_node_TerminalRecordState **state;
    JZ_BTREE_FOR(&m->structured_terminal_records, key, state) {
        (void)key;
        structured_app_rows_bytes += jazz_node__node__terminal_record__TerminalRecordState__retained_bytes(*state);
    }
    out->result_rows = m->result_weights.positive_count;
    out->result_weights = m->result_weights.entries.len;
    out->result_payloads = m->result_payloads.entries.len;
    out->structured_app_rows = structured_app_rows + m->structured_terminal_records.len;
    out->version_identities = m->versions.entry_count + m->selected_deletion_witnesses.len;
    out->version_tx_entries = m->versions.entry_count;
    out->replacement_entries = jzn_ri_entry_count(&m->replacements);
    out->result_weights_bytes = result_weights_bytes;
    out->result_payloads_bytes = result_payloads_bytes;
    out->structured_app_rows_bytes = structured_app_rows_bytes;
    out->versions_bytes = versions_bytes;
    out->supporting_frontier_bytes = supporting_frontier_bytes;
    out->replacements_bytes = replacements_bytes;
    out->total_heap_bytes = result_weights_bytes + result_payloads_bytes + structured_app_rows_bytes + versions_bytes + supporting_frontier_bytes +
                            replacements_bytes + witness_table_names_bytes;
}

/* active_result_members: current positive result memberships. */
void MSV(MaintainedSubscriptionView__active_result_members)(const MSVT *m, jazz_protocol_ResultMemberEntryVec *out) {
    memset(out, 0, sizeof *out);
    RME *member;
    i64 *weight;
    JZ_BTREE_FOR(&m->result_weights.entries, member, weight) {
        if (*weight > 0) JZ_VEC_PUSH(*out, jzn_rme_clone(member));
    }
}

/* published_result_members: memberships that crossed the result/content witness boundary. */
const jz_btree *MSV(MaintainedSubscriptionView__published_result_members)(const MSVT *m) { return &m->published_result_members.entries; }
