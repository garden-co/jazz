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
