/* Port of crates/jazz/layers/node/src/node/query_eval/local_availability_receipts.rs
 * Local availability record v1 uses Groove's native typed-record codec. */
#include "jazz_node/node/node_qe_b.h"

static void jazz_node__node__query_eval__local_availability_receipts__LocalAvailabilityRecord__values(
    jazz_node_LocalAvailabilityRecord self, groove_Value out[8])
{
    jazz_node_LocalAvailabilityWatermark w = self.watermark;
    out[0] = gv_u8(1);
    out[1] = gv_bool(self.status == JZN_LRA_CURRENT_UNAVAILABLE);
    out[2] = gv_uuid(w.core);
    out[3] = gv_u64(w.core_epoch);
    out[4] = gv_u64(w.claims_revision);
    out[5] = gv_u64(w.policy_epoch);
    out[6] = gv_u64(w.settled_through);
    out[7] = gv_u64(w.authorization_progress);
}

static void value_vec_drop(groove_ValueVec *v) { jz_vec_drop(v, sizeof(groove_Value), groove_Value_drop); }

static jz_err jazz_node__node__query_eval__local_availability_receipts__LocalAvailabilityRecord__decode(
    const groove_OwnedRecord *record, jazz_node_LocalAvailabilityRecord *out)
{
    groove_BorrowedRecord br = groove__records__record__OwnedRecord__borrowed(record);
    groove_ValueVec values;
    JZ_TRY(groove__records__record__BorrowedRecord__to_values(&br, &values));
    groove_RecordDescriptor *d = jazz_model__schema__local_availability_record_descriptor();
    jz_bytes canonical;
    jz_err e = groove__records__descriptor__RecordDescriptor__create(d, values.ptr, values.len, &canonical);
    groove_RecordDescriptor_drop(d);
    if (JZ_IS_ERR(e)) { value_vec_drop(&values); return e; }
    bool same = jz_bytes_eq(&canonical, &record->bytes);
    jz_bytes_drop(&canonical);
    if (!same) { value_vec_drop(&values); return JZN_INVALID_STORED("noncanonical local availability record v1"); }
    const groove_Value *v = values.ptr;
    if (values.len != 8 || v[0].tag != GV_U8 || v[0].v.u8_ != 1 || v[1].tag != GV_BOOL || v[2].tag != GV_UUID ||
        v[3].tag != GV_U64 || v[4].tag != GV_U64 || v[5].tag != GV_U64 || v[6].tag != GV_U64 || v[7].tag != GV_U64) {
        value_vec_drop(&values);
        return JZN_INVALID_STORED("invalid local availability record v1");
    }
    if (v[7].v.u64_ == 0) {
        value_vec_drop(&values);
        return JZN_INVALID_STORED("local availability evaluation sequence must be positive");
    }
    out->status = v[1].v.b ? JZN_LRA_CURRENT_UNAVAILABLE : JZN_LRA_READABLE;
    out->watermark.core = v[2].v.uuid;
    out->watermark.core_epoch = v[3].v.u64_;
    out->watermark.claims_revision = v[4].v.u64_;
    out->watermark.policy_epoch = v[5].v.u64_;
    out->watermark.settled_through = v[6].v.u64_;
    out->watermark.authorization_progress = v[7].v.u64_;
    value_vec_drop(&values);
    return JZ_OK;
}

static bool record_eq(const jazz_node_LocalAvailabilityRecord *a, const jazz_node_LocalAvailabilityRecord *b)
{
    return a->status == b->status && jz_uuid_eq(&a->watermark.core, &b->watermark.core) &&
           a->watermark.core_epoch == b->watermark.core_epoch && a->watermark.claims_revision == b->watermark.claims_revision &&
           a->watermark.policy_epoch == b->watermark.policy_epoch && a->watermark.settled_through == b->watermark.settled_through &&
           a->watermark.authorization_progress == b->watermark.authorization_progress;
}

static jz_err jazz_node__node__query_eval__local_availability_receipts__LocalAvailabilityRecord__follows(
    jazz_node_LocalAvailabilityRecord self, jazz_node_LocalAvailabilityRecord prior, bool *out)
{
    jazz_node_LocalAvailabilityWatermark next = self.watermark, old = prior.watermark;
    *out = false;
    if (next.policy_epoch < old.policy_epoch || next.settled_through < old.settled_through) return JZ_OK;
    if (!jz_uuid_eq(&next.core, &old.core)) { *out = true; return JZ_OK; } /* Explicit route-owner admission is required separately. */
    if (next.core_epoch != old.core_epoch) { *out = true; return JZ_OK; }
    if (next.claims_revision < old.claims_revision || next.authorization_progress < old.authorization_progress) return JZ_OK;
    if (next.authorization_progress == old.authorization_progress) {
        if (!record_eq(&self, &prior)) return JZN_INVALID_STORED("conflicting local availability evaluation receipt");
        return JZ_OK;
    }
    *out = true;
    return JZ_OK;
}

/* The selected route owner calls this at admission, never from an uncorrelated reply. */
jz_err jazz_node__node__query_eval__local_availability_receipts__NodeState__activate_local_availability_authority(
    jazz_node_NodeState *self, jazz_protocol_PolicyBindingKey scope, jazz_types_NodeUuid core, u64 epoch)
{
    if (jazz_types_AuthorSubject_is_system(scope.identity)) {
        jazz_protocol_PolicyBindingKey_drop(&scope);
        return JZN_INVALID_STORED("SYSTEM does not own local availability exclusions");
    }
    jz_err e = jazz_node__node__query_eval__unavailable_inputs__NodeState__require_local_availability_context_capacity(self, &scope);
    if (JZ_IS_ERR(e)) { jazz_protocol_PolicyBindingKey_drop(&scope); return e; }
    jazz_node_CoreEpoch ce = { core, epoch };
    jazz_node_CoreEpoch old;
    if (jz_btree_insert(&self->query.local_availability_authorities, &scope, &ce, &old)) jazz_protocol_PolicyBindingKey_drop(&scope);
    return JZ_OK;
}

jz_err jazz_node__node__query_eval__local_availability_receipts__NodeState__apply_verified_local_row_availability(
    jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *scope, jazz_node_LocalAvailabilityWatermark watermark,
    const jazz_node_LocalAvailabilityOutcome *outcomes, u32 n, bool *out)
{
    return jazz_node__node__query_eval__local_availability_receipts__NodeState__apply_local_row_availability_after_persist(
        self, scope, watermark, outcomes, n, NULL, NULL, out);
}

typedef struct Coord { jazz_types_GlobalPhysicalTableId table; jazz_types_RowUuid row; } Coord;
static int coord_cmp(const void *a, const void *b, void *ctx)
{
    const Coord *l = a, *r = b;
    int c = jz_uuid_cmp(&l->table, &r->table);
    return c ? c : jz_uuid_cmp(&l->row, &r->row);
}
static const jz_btree_desc COORD_SET = { sizeof(Coord), 0, coord_cmp, NULL, NULL };
typedef struct Update { jazz_node_ScopeTableRowKey key; jazz_node_LocalAvailabilityRecord record; } Update;
static void update_drop(void *p) { Update *u = p; jazz_protocol_PolicyBindingKey_drop(&u->key.scope); }

jz_err jazz_node__node__query_eval__local_availability_receipts__NodeState__apply_local_row_availability_after_persist(
    jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *scope, jazz_node_LocalAvailabilityWatermark watermark,
    const jazz_node_LocalAvailabilityOutcome *outcomes, u32 n, void (*after_persist)(void *), void *after_ctx, bool *out)
{
    *out = false;
    const jazz_node_CoreEpoch *auth = jz_btree_get(&self->query.local_availability_authorities, scope);
    if (!auth || !jz_uuid_eq(&auth->core, &watermark.core) || auth->epoch != watermark.core_epoch) return JZ_OK;
    if (n > JZ_MAX_KNOWN_STATE_EXACT_REFS || watermark.authorization_progress == 0)
        return JZN_INVALID_STORED("invalid local availability receipt bounds");
    jz_btree seen;
    jz_btree_init(&seen, &COORD_SET);
    JZ_VEC(Update) updates = {0};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < n; i++) {
        Coord c = { outcomes[i].table, outcomes[i].row };
        if (jz_btree_contains(&seen, &c)) { e = JZN_INVALID_STORED("duplicate local availability row coordinate"); goto fail; }
        jz_btree_insert(&seen, &c, NULL, NULL);
        bool known = false;
        void *mk; jazz_node_SchemaPhysicalMapping *mapping;
        JZ_BTREE_FOR(&self->catalogue.physical_mappings, mk, mapping) {
            void *ik; jazz_node_PhysicalTableIdentity *ident;
            JZ_BTREE_FOR(&mapping->identities.tables, ik, ident) if (jz_uuid_eq(&ident->id, &c.table)) { known = true; break; }
            if (known) break;
        }
        if (!known) { e = JZN_INVALID_STORED("local availability receipt has unknown global table identity"); goto fail; }
        jazz_node_ScopeTableRowKey key = { *scope, c.table, c.row };
        jazz_node_LocalAvailabilityRecord record = { outcomes[i].status, watermark };
        const jazz_node_LocalAvailabilityRecord *prior = jz_btree_get(&self->query.local_availability_records, &key);
        if (prior) {
            bool follows;
            e = jazz_node__node__query_eval__local_availability_receipts__LocalAvailabilityRecord__follows(record, *prior, &follows);
            if (JZ_IS_ERR(e)) goto fail;
            if (!follows) continue;
        }
        Update u;
        jazz_protocol_PolicyBindingKey_clone(&u.key.scope, scope);
        u.key.table = c.table;
        u.key.row = c.row;
        u.record = record;
        jz_vec_push(&updates, &u, sizeof u);
    }
    jz_btree_drop(&seen);
    if (updates.len == 0) { jz_free(updates.ptr); return JZ_OK; }
    /* The exact directory comparison rejects digest collisions. */
    e = jazz_node__node__NodeState__persist_policy_binding_directory(self, scope);
    if (JZ_IS_ERR(e)) goto fail_updates;
    u8 digest[32];
    jazz_protocol__protocol__PolicyBindingKey__directory_digest(scope, digest);
    groove_DirectRecordStoreWriteVec writes = {0};
    for (u32 i = 0; i < updates.len; i++) {
        groove_DirectRecordStoreWrite w;
        memset(&w, 0, sizeof w);
        w.tag = GDRSW_SET;
        JZ_VEC_PUSH(w.key, gv_bytes(jz_bytes_from(digest, 32)));
        JZ_VEC_PUSH(w.key, gv_uuid(updates.ptr[i].key.table));
        JZ_VEC_PUSH(w.key, gv_uuid(updates.ptr[i].key.row));
        jz_vec_reserve(&w.value, 8, sizeof(groove_Value));
        jazz_node__node__query_eval__local_availability_receipts__LocalAvailabilityRecord__values(updates.ptr[i].record, w.value.ptr);
        w.value.len = 8;
        jz_vec_push(&writes, &w, sizeof w);
    }
    /* Arm before the write: cancellation of storage itself can have an ambiguous durable outcome. */
    groove_HostApplicationGuard application;
    e = groove__db__facade__Database__guard_host_application(self->database, &application);
    if (JZ_IS_ERR(e)) { jz_vec_drop(&writes, sizeof(groove_DirectRecordStoreWrite), groove_DirectRecordStoreWrite_drop); goto fail_updates; }
    groove_DirectRecordStore store;
    e = groove__db__facade__Database__direct_record_store(self->database, jazz_model__schema__LOCAL_ROW_AVAILABILITY_STORE, &store);
    if (!JZ_IS_ERR(e)) {
        e = groove__db__storage_helpers__DirectRecordStore__write_many(&store, writes.ptr, writes.len);
        groove_DirectRecordStore_drop(&store);
    }
    jz_vec_drop(&writes, sizeof(groove_DirectRecordStoreWrite), groove_DirectRecordStoreWrite_drop);
    if (JZ_IS_ERR(e)) { groove_HostApplicationGuard_drop(&application); goto fail_updates; }
    if (after_persist) after_persist(after_ctx);
    JZ_VEC(jazz_node_UnavailableRowChange) changes = {0};
    for (u32 i = 0; i < updates.len; i++) {
        jazz_node_UnavailableRowChange c = { updates.ptr[i].key.table, updates.ptr[i].key.row,
                                             updates.ptr[i].record.status == JZN_LRA_CURRENT_UNAVAILABLE };
        jz_vec_push(&changes, &c, sizeof c);
    }
    bool changed;
    e = jazz_node__node__query_eval__unavailable_inputs__NodeState__update_local_unavailable_rows(self, scope, changes.ptr, changes.len, &changed);
    jz_free(changes.ptr);
    if (JZ_IS_ERR(e)) { groove_HostApplicationGuard_drop(&application); goto fail_updates; }
    for (u32 i = 0; i < updates.len; i++) {
        jazz_node_LocalAvailabilityRecord old;
        if (jz_btree_insert(&self->query.local_availability_records, &updates.ptr[i].key, &updates.ptr[i].record, &old))
            jazz_protocol_PolicyBindingKey_drop(&updates.ptr[i].key.scope);
    }
    jz_free(updates.ptr);
    groove__db__HostApplicationGuard__complete(&application);
    *out = changed;
    return JZ_OK;

fail:
    jz_btree_drop(&seen);
fail_updates:
    jz_vec_drop(&updates, sizeof(Update), update_drop);
    return e;
}

typedef struct Digest { u8 b[32]; } Digest;
static int digest_cmp(const void *a, const void *b, void *ctx) { return memcmp(a, b, 32); }
static const jz_btree_desc DIGEST_POLICY_DESC = { sizeof(Digest), sizeof(jazz_protocol_PolicyBindingKey), digest_cmp, NULL, jazz_protocol_PolicyBindingKey_drop };
static int scope_table_row_cmp(const void *a, const void *b, void *ctx)
{
    const jazz_node_ScopeTableRowKey *l = a, *r = b;
    int c = jazz_protocol_PolicyBindingKey_cmp(&l->scope, &r->scope, ctx);
    if (c) return c;
    c = jz_uuid_cmp(&l->table, &r->table);
    return c ? c : jz_uuid_cmp(&l->row, &r->row);
}
static void scope_table_row_drop(void *p) { jazz_protocol_PolicyBindingKey_drop(&((jazz_node_ScopeTableRowKey *)p)->scope); }
static const jz_btree_desc RECOVERED_DESC = { sizeof(jazz_node_ScopeTableRowKey), sizeof(jazz_node_LocalAvailabilityRecord), scope_table_row_cmp, scope_table_row_drop, NULL };

jz_err jazz_node__node__query_eval__local_availability_receipts__NodeState__recover_local_availability_records(jazz_node_NodeState *self)
{
    groove_DirectRecordStore store, policy_store;
    JZ_TRY(groove__db__facade__Database__direct_record_store(self->database, jazz_model__schema__LOCAL_ROW_AVAILABILITY_STORE, &store));
    groove_DirectRecordEntryVec entries;
    jz_err e = groove__db__storage_helpers__DirectRecordStore__prefix_entries(&store, NULL, 0, &entries);
    groove_DirectRecordStore_drop(&store);
    if (JZ_IS_ERR(e)) return e;
    e = groove__db__facade__Database__direct_record_store(self->database, jazz_model__schema__AUTHORITY_POLICY_BINDINGS_STORE, &policy_store);
    if (JZ_IS_ERR(e)) { jz_vec_drop(&entries, sizeof(groove_DirectRecordEntry), groove_DirectRecordEntry_drop); return e; }
    jz_btree policies, recovered;
    jz_btree_init(&policies, &DIGEST_POLICY_DESC);
    jz_btree_init(&recovered, &RECOVERED_DESC);
    for (u32 i = 0; i < entries.len; i++) {
        const groove_DirectRecordEntry *entry = &entries.ptr[i];
        const groove_Value *k = entry->key.ptr;
        if (entry->key.len != 3 || k[0].tag != GV_BYTES || k[1].tag != GV_UUID || k[2].tag != GV_UUID) {
            e = JZN_INVALID_STORED("invalid local availability record key");
            goto done;
        }
        if (k[0].v.bytes.len != 32) { e = JZN_INVALID_STORED("invalid local availability policy digest"); goto done; }
        Digest digest;
        memcpy(digest.b, k[0].v.bytes.ptr, 32);
        if (!jz_btree_contains(&policies, &digest)) {
            groove_Value dk = gv_bytes(jz_bytes_from(digest.b, 32));
            bool found = false;
            groove_OwnedRecord policy;
            e = groove__db__storage_helpers__DirectRecordStore__get(&policy_store, &dk, 1, &found, &policy);
            groove_Value_drop(&dk);
            if (JZ_IS_ERR(e)) goto done;
            if (!found) { e = JZN_INVALID_STORED("local availability policy directory entry is missing"); goto done; }
            groove_BorrowedRecord pr = groove__records__record__OwnedRecord__borrowed(&policy);
            groove_Value subject;
            e = groove__records__record__BorrowedRecord__get_idx(&pr, 0, &subject);
            if (JZ_IS_ERR(e)) { groove_OwnedRecord_drop(&policy); goto done; }
            if (subject.tag != GV_STRING) {
                groove_Value_drop(&subject);
                groove_OwnedRecord_drop(&policy);
                e = JZN_INVALID_STORED("invalid local availability policy subject");
                goto done;
            }
            jazz_types_AuthorSubject identity;
            jz_err pe = jazz_types__ids__AuthorSubject__from_canonical(jz_str_slice(&subject.v.s), &identity);
            groove_Value_drop(&subject);
            if (JZ_IS_ERR(pe)) {
                jz_err_drop(&pe);
                groove_OwnedRecord_drop(&policy);
                e = JZN_INVALID_STORED("invalid local availability subject");
                goto done;
            }
            groove_Value claims;
            e = groove__records__record__BorrowedRecord__get_idx(&pr, 1, &claims);
            groove_OwnedRecord_drop(&policy);
            if (JZ_IS_ERR(e)) goto done;
            jazz_protocol_PolicyBindingKey key;
            pe = jazz_protocol__protocol__PolicyBindingKey__from_directory_value(identity, claims, &key);
            if (JZ_IS_ERR(pe)) {
                jz_err_drop(&pe);
                e = JZN_INVALID_STORED("invalid local availability policy claims");
                goto done;
            }
            u8 actual[32];
            jazz_protocol__protocol__PolicyBindingKey__directory_digest(&key, actual);
            if (jazz_types_AuthorSubject_is_system(key.identity) || memcmp(actual, digest.b, 32) != 0) {
                jazz_protocol_PolicyBindingKey_drop(&key);
                e = JZN_INVALID_STORED("local availability policy directory identity mismatch");
                goto done;
            }
            jz_btree_insert(&policies, &digest, &key, NULL);
        }
        jazz_node_LocalAvailabilityRecord record;
        e = jazz_node__node__query_eval__local_availability_receipts__LocalAvailabilityRecord__decode(&entry->value, &record);
        if (JZ_IS_ERR(e)) goto done;
        const jazz_protocol_PolicyBindingKey *policy = jz_btree_get(&policies, &digest);
        if (!policy) jz_panic("key not found in BTreeMap");
        jazz_node_ScopeTableRowKey rk;
        jazz_protocol_PolicyBindingKey_clone(&rk.scope, policy);
        rk.table = k[1].v.uuid;
        rk.row = k[2].v.uuid;
        jazz_node_LocalAvailabilityRecord old;
        if (jz_btree_insert(&recovered, &rk, &record, &old)) jazz_protocol_PolicyBindingKey_drop(&rk.scope);
    }
    /* Recovery publishes no prefix if a later record is malformed. */
    {
        jazz_node_ScopeTableRowKey *rk; jazz_node_LocalAvailabilityRecord *rec;
        JZ_BTREE_FOR(&recovered, rk, rec) {
            jazz_node_UnavailableRowChange c = { rk->table, rk->row, rec->status == JZN_LRA_CURRENT_UNAVAILABLE };
            bool changed;
            e = jazz_node__node__query_eval__unavailable_inputs__NodeState__update_local_unavailable_rows(self, &rk->scope, &c, 1, &changed);
            if (JZ_IS_ERR(e)) goto done;
        }
    }
    jz_btree_drop(&self->query.local_availability_records);
    self->query.local_availability_records = recovered;
    jz_btree_init(&recovered, &RECOVERED_DESC);
done:
    jz_btree_drop(&recovered);
    jz_btree_drop(&policies);
    groove_DirectRecordStore_drop(&policy_store);
    jz_vec_drop(&entries, sizeof(groove_DirectRecordEntry), groove_DirectRecordEntry_drop);
    return e;
}
