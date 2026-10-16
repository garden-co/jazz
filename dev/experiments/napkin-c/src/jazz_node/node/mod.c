/* Port of crates/jazz/layers/node/src/node/mod.rs (non-test, non-include! parts).
 * The include!("state/*.rs") bodies belong to another unit. */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

/* ---------------- shared map descriptors ---------------- */
void jzn_str_drop_v(void *p) { jz_str_drop((jz_str *)p); }
static void value_drop_v(void *p) { groove__records__values__Value__drop(p); }
int jzn_cmp_txid(const void *a, const void *b, void *ctx) { (void)ctx; return jazz_model_TxId_cmp(a, b); }
const jz_btree_desc jzn_desc_str_value = { sizeof(jz_str), sizeof(groove_Value), jz_cmp_str, jzn_str_drop_v, value_drop_v };
static const jz_btree_desc mod_desc_str_set = { sizeof(jz_str), 0, jz_cmp_str, jzn_str_drop_v, NULL };
const jz_btree_desc jzn_desc_txid_set = { sizeof(jazz_model_TxId), 0, jzn_cmp_txid, NULL, NULL };
static const jz_btree_desc mod_desc_u64_set = { sizeof(u64), 0, jz_cmp_u64, NULL, NULL };
const jz_btree_desc jzn_desc_uuid_u64 = { sizeof(jz_uuid), sizeof(u64), jz_cmp_uuid, NULL, NULL };
const jz_btree_desc jzn_desc_uuid_set = { sizeof(jz_uuid), 0, jz_cmp_uuid, NULL, NULL };
const jz_btree_desc jzn_desc_str_ptr = { sizeof(jz_str), sizeof(void *), jz_cmp_str, jzn_str_drop_v, NULL };
const jz_btree_desc jzn_desc_uuid_ptr = { sizeof(jz_uuid), sizeof(void *), jz_cmp_uuid, NULL, NULL };
const jz_btree_desc jzn_desc_txid_ptr = { sizeof(jazz_model_TxId), sizeof(void *), jzn_cmp_txid, NULL, NULL };

typedef JZ_VEC(jz_uuid) uuid_vec;
static void scalar_vec_drop(void *p) { jz_vec_drop(p, sizeof(jazz_node_GlobalScalarEnumCaseId), NULL); }
static void payload_vec_drop(void *p) { jz_vec_drop(p, sizeof(jazz_node_GlobalEnumCaseId), NULL); }
static const jz_btree_desc desc_str_scalar_cases = { sizeof(jz_str), sizeof(jazz_node_GlobalScalarEnumCaseIdVec), jz_cmp_str, jzn_str_drop_v, scalar_vec_drop };
static const jz_btree_desc desc_str_payload_cases = { sizeof(jz_str), sizeof(jazz_node_GlobalEnumCaseIdVec), jz_cmp_str, jzn_str_drop_v, payload_vec_drop };

/* ---------------- enum case identity hydration ---------------- */
static jz_err jazz_node__node__install_enum_case_ids(jazz_node_GlobalScalarEnumCaseIdVec *slot, const uuid_vec *ids,
                                                     jazz_types_SchemaVersionId introducing_schema) {
    jazz_node_GlobalScalarEnumCaseIdVec expected = {0};
    for (u32 ordinal = 0; ordinal < ids->len; ordinal++) {
        if (ordinal > 0xff) {
            jz_vec_drop(&expected, sizeof(*expected.ptr), NULL);
            return JZN_INVALID_STORED("scalar enum tag exhausted");
        }
        jazz_node_GlobalScalarEnumCaseId id = { ids->ptr[ordinal], introducing_schema, (u8)ordinal };
        JZ_VEC_PUSH(expected, id);
    }
    u32 shared = slot->len < expected.len ? slot->len : expected.len;
    bool mismatch = slot->len > expected.len;
    for (u32 i = 0; i < shared && !mismatch; i++)
        if (!jz_uuid_eq(&slot->ptr[i].id, &expected.ptr[i].id)) mismatch = true;
    if (mismatch) {
        jz_vec_drop(&expected, sizeof(*expected.ptr), NULL);
        return JZN_INVALID_STORED("enum registry conflicts with authority identities");
    }
    for (u32 i = slot->len; i < expected.len; i++) JZ_VEC_PUSH(*slot, expected.ptr[i]);
    jz_vec_drop(&expected, sizeof(*expected.ptr), NULL);
    return JZ_OK;
}

static jz_err jazz_node__node__install_payload_enum_case_ids(jazz_node_GlobalEnumCaseIdVec *slot, const uuid_vec *ids,
                                                             jazz_types_SchemaVersionId introducing_schema) {
    jazz_node_GlobalEnumCaseIdVec expected = {0};
    for (u32 ordinal = 0; ordinal < ids->len; ordinal++) {
        jazz_node_GlobalEnumCaseId id = { ids->ptr[ordinal], introducing_schema, ordinal };
        JZ_VEC_PUSH(expected, id);
    }
    u32 shared = slot->len < expected.len ? slot->len : expected.len;
    bool mismatch = slot->len > expected.len;
    for (u32 i = 0; i < shared && !mismatch; i++)
        if (!jz_uuid_eq(&slot->ptr[i].id, &expected.ptr[i].id)) mismatch = true;
    if (mismatch) {
        jz_vec_drop(&expected, sizeof(*expected.ptr), NULL);
        return JZN_INVALID_STORED("enum registry conflicts with authority identities");
    }
    for (u32 i = slot->len; i < expected.len; i++) JZ_VEC_PUSH(*slot, expected.ptr[i]);
    jz_vec_drop(&expected, sizeof(*expected.ptr), NULL);
    return JZ_OK;
}

static jz_str jazz_node__node__global_case_path(const jz_str *path, const jz_uuid *case_id) {
    char simple[37];
    jz_uuid_to_string(case_id, simple);
    /* `.simple()` drops the hyphens */
    char out[33]; u32 j = 0;
    for (u32 i = 0; i < 36; i++) if (simple[i] != '-') out[j++] = simple[i];
    out[j] = 0;
    return jz_format("%.*s/case/%s", (int)path->len, path->ptr, out);
}

static void *entry_or_default(jz_btree *m, const jz_str *path) {
    jz_str key = jz_str_clone(path);
    bool inserted;
    void *v = jz_btree_entry(m, &key, &inserted);
    if (!inserted) jz_str_drop(&key);
    return v;
}

static jz_err jazz_node__node__hydrate_nested_scalar_enum_cases(
    const groove_ValueType *value_type, const jz_btree *identities, jazz_types_SchemaVersionId introducing_schema,
    const jz_str *authored_path, const jz_str *path, jz_btree *output) {
    jz_err e = JZ_OK;
    switch (value_type->tag) {
    case GROOVE_VT_ENUM_TAG: {
        const uuid_vec *ids = jz_btree_get(identities, authored_path);
        if (!ids) return JZN_INVALID_STORED("nested scalar enum authority identities missing");
        if (ids->len != value_type->v.enum_tag->variants.len)
            return JZN_INVALID_STORED("nested scalar enum authority identity width mismatch");
        JZ_TRY(jazz_node__node__install_enum_case_ids(entry_or_default(output, path), ids, introducing_schema));
        break;
    }
    case GROOVE_VT_NULLABLE:
    case GROOVE_VT_ARRAY: {
        const char *seg = value_type->tag == GROOVE_VT_NULLABLE ? "nullable" : "array";
        jz_str ap = jz_format("%.*s/%s", (int)authored_path->len, authored_path->ptr, seg);
        jz_str pp = jz_format("%.*s/%s", (int)path->len, path->ptr, seg);
        e = jazz_node__node__hydrate_nested_scalar_enum_cases(value_type->v.inner, identities, introducing_schema, &ap, &pp, output);
        jz_str_drop(&ap); jz_str_drop(&pp);
        return e;
    }
    case GROOVE_VT_TUPLE:
        for (u32 index = 0; index < value_type->v.tuple.len; index++) {
            jz_str ap = jz_format("%.*s/tuple/%u", (int)authored_path->len, authored_path->ptr, index);
            jz_str pp = jz_format("%.*s/tuple/%u", (int)path->len, path->ptr, index);
            e = jazz_node__node__hydrate_nested_scalar_enum_cases(&value_type->v.tuple.ptr[index], identities, introducing_schema, &ap, &pp, output);
            jz_str_drop(&ap); jz_str_drop(&pp);
            JZ_TRY(e);
        }
        break;
    case GROOVE_VT_RECORD: {
        const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(value_type->v.record);
        for (u32 i = 0; i < fields->len; i++) {
            const groove_DescriptorField *field = &fields->ptr[i];
            if (!field->has_name) return JZN_INVALID_STORED("nested enum record field unnamed");
            jz_str ap = jz_format("%.*s/record/%.*s", (int)authored_path->len, authored_path->ptr, (int)field->name.len, field->name.ptr);
            jz_str pp = jz_format("%.*s/record/%.*s", (int)path->len, path->ptr, (int)field->name.len, field->name.ptr);
            e = jazz_node__node__hydrate_nested_scalar_enum_cases(&field->value_type, identities, introducing_schema, &ap, &pp, output);
            jz_str_drop(&ap); jz_str_drop(&pp);
            JZ_TRY(e);
        }
        break;
    }
    case GROOVE_VT_ENUM: {
        const groove_EnumSchema *schema = value_type->v.enm;
        for (u32 ordinal = 0; ordinal < schema->cases.len; ordinal++) {
            const uuid_vec *ids = jz_btree_get(identities, authored_path);
            if (!ids || ordinal >= ids->len)
                return JZN_INVALID_STORED("nested payload enum authority identity missing");
            jazz_node_GlobalEnumCaseId identity = { ids->ptr[ordinal], introducing_schema, ordinal };
            groove_ValueType rec = { GROOVE_VT_RECORD };
            rec.v.record = groove__records__RecordDescriptor__clone_box(groove__records__EnumCaseLayout__payload(schema->cases.ptr[ordinal]));
            jz_str ap = jz_format("%.*s/case/%u", (int)authored_path->len, authored_path->ptr, ordinal);
            jz_str pp = jazz_node__node__global_case_path(path, &identity.id);
            e = jazz_node__node__hydrate_nested_scalar_enum_cases(&rec, identities, introducing_schema, &ap, &pp, output);
            jz_str_drop(&ap); jz_str_drop(&pp);
            groove__records__values__ValueType__drop(&rec);
            JZ_TRY(e);
        }
        break;
    }
    default: break;
    }
    return JZ_OK;
}

static jz_err jazz_node__node__hydrate_nested_payload_enum_cases(
    const groove_ValueType *value_type, const jz_btree *identities, jazz_types_SchemaVersionId introducing_schema,
    const jz_str *authored_path, const jz_str *path, jz_btree *output) {
    jz_err e = JZ_OK;
    switch (value_type->tag) {
    case GROOVE_VT_NULLABLE:
    case GROOVE_VT_ARRAY: {
        const char *seg = value_type->tag == GROOVE_VT_NULLABLE ? "nullable" : "array";
        jz_str ap = jz_format("%.*s/%s", (int)authored_path->len, authored_path->ptr, seg);
        jz_str pp = jz_format("%.*s/%s", (int)path->len, path->ptr, seg);
        e = jazz_node__node__hydrate_nested_payload_enum_cases(value_type->v.inner, identities, introducing_schema, &ap, &pp, output);
        jz_str_drop(&ap); jz_str_drop(&pp);
        return e;
    }
    case GROOVE_VT_TUPLE:
        for (u32 index = 0; index < value_type->v.tuple.len; index++) {
            jz_str ap = jz_format("%.*s/tuple/%u", (int)authored_path->len, authored_path->ptr, index);
            jz_str pp = jz_format("%.*s/tuple/%u", (int)path->len, path->ptr, index);
            e = jazz_node__node__hydrate_nested_payload_enum_cases(&value_type->v.tuple.ptr[index], identities, introducing_schema, &ap, &pp, output);
            jz_str_drop(&ap); jz_str_drop(&pp);
            JZ_TRY(e);
        }
        break;
    case GROOVE_VT_RECORD: {
        const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(value_type->v.record);
        for (u32 i = 0; i < fields->len; i++) {
            const groove_DescriptorField *field = &fields->ptr[i];
            if (!field->has_name) return JZN_INVALID_STORED("nested enum record field unnamed");
            jz_str ap = jz_format("%.*s/record/%.*s", (int)authored_path->len, authored_path->ptr, (int)field->name.len, field->name.ptr);
            jz_str pp = jz_format("%.*s/record/%.*s", (int)path->len, path->ptr, (int)field->name.len, field->name.ptr);
            e = jazz_node__node__hydrate_nested_payload_enum_cases(&field->value_type, identities, introducing_schema, &ap, &pp, output);
            jz_str_drop(&ap); jz_str_drop(&pp);
            JZ_TRY(e);
        }
        break;
    }
    case GROOVE_VT_ENUM: {
        const groove_EnumSchema *schema = value_type->v.enm;
        const uuid_vec *ids = jz_btree_get(identities, authored_path);
        if (!ids) return JZN_INVALID_STORED("nested payload enum authority identities missing");
        if (ids->len != schema->cases.len)
            return JZN_INVALID_STORED("nested payload enum authority identity width mismatch");
        JZ_TRY(jazz_node__node__install_payload_enum_case_ids(entry_or_default(output, path), ids, introducing_schema));
        const jazz_node_GlobalEnumCaseIdVec *slot = jz_btree_get(output, path);
        if (!slot) jz_panic("map index out of range");
        jazz_node_GlobalEnumCaseIdVec cases = {0};
        jz_vec_extend(&cases, slot->ptr, slot->len, sizeof(*cases.ptr));
        u32 n = cases.len < schema->cases.len ? cases.len : schema->cases.len;
        for (u32 ordinal = 0; ordinal < n; ordinal++) {
            groove_ValueType rec = { GROOVE_VT_RECORD };
            rec.v.record = groove__records__RecordDescriptor__clone_box(groove__records__EnumCaseLayout__payload(schema->cases.ptr[ordinal]));
            jz_str ap = jz_format("%.*s/case/%u", (int)authored_path->len, authored_path->ptr, ordinal);
            jz_str pp = jazz_node__node__global_case_path(path, &cases.ptr[ordinal].id);
            e = jazz_node__node__hydrate_nested_payload_enum_cases(&rec, identities, introducing_schema, &ap, &pp, output);
            jz_str_drop(&ap); jz_str_drop(&pp);
            groove__records__values__ValueType__drop(&rec);
            if (JZ_IS_ERR(e)) break;
        }
        jz_vec_drop(&cases, sizeof(*cases.ptr), NULL);
        return e;
    }
    default: break;
    }
    return JZ_OK;
}

/* Non-static entry points used by the catalogue reconciler (state unit). */
jz_err jazz_node__node__hydrate_nested_scalar_enum_cases_pub(const groove_ValueType *t, const jz_btree *ids, jazz_types_SchemaVersionId s,
                                                              const jz_str *ap, const jz_str *p, jz_btree *out) {
    if (!out->d) jz_btree_init(out, &desc_str_scalar_cases);
    return jazz_node__node__hydrate_nested_scalar_enum_cases(t, ids, s, ap, p, out);
}
jz_err jazz_node__node__hydrate_nested_payload_enum_cases_pub(const groove_ValueType *t, const jz_btree *ids, jazz_types_SchemaVersionId s,
                                                               const jz_str *ap, const jz_str *p, jz_btree *out) {
    if (!out->d) jz_btree_init(out, &desc_str_payload_cases);
    return jazz_node__node__hydrate_nested_payload_enum_cases(t, ids, s, ap, p, out);
}

/* ---------------- AheadCurrentKeys ---------------- */
static u64 ahead_key_hash(const void *k) {
    const jazz_node_AheadKey *a = k;
    return jz_hash_combine(jz_hash_u64(a->table), jz_hash_bytes(a->key.ptr, a->key.len));
}
static bool ahead_key_eq(const void *a, const void *b) {
    const jazz_node_AheadKey *x = a, *y = b;
    return x->table == y->table && jz_bytes_eq(&x->key, &y->key);
}
static void ahead_key_drop(void *k) { jz_bytes_drop(&((jazz_node_AheadKey *)k)->key); }
static u64 u64_hash(const void *k) { return jz_hash_u64(*(const u64 *)k); }
static bool u64_eq(const void *a, const void *b) { return *(const u64 *)a == *(const u64 *)b; }
static u64 bytes_hash(const void *k) { const jz_bytes *b = k; return jz_hash_bytes(b->ptr, b->len); }
static bool bytes_eq(const void *a, const void *b) { return jz_bytes_eq(a, b); }
static void bytes_drop(void *p) { jz_bytes_drop(p); }
static const jz_hmap_desc desc_ahead_keys = { sizeof(jazz_node_AheadKey), sizeof(jazz_model_TxId), ahead_key_hash, ahead_key_eq, ahead_key_drop, NULL };
static const jz_hmap_desc desc_branch_rows = { sizeof(jz_bytes), sizeof(usize), bytes_hash, bytes_eq, bytes_drop, NULL };
static void branch_map_drop(void *p) { jz_hmap_drop(p); }
static const jz_hmap_desc desc_rows_per_branch = { sizeof(u64), sizeof(jz_hmap), u64_hash, u64_eq, NULL, branch_map_drop };

void jazz_node__node__AheadCurrentKeys__init(jazz_node_AheadCurrentKeys *k) {
    jz_hmap_init(&k->keys, &desc_ahead_keys);
    jz_hmap_init(&k->rows_per_branch, &desc_rows_per_branch);
}

const jazz_model_TxId *jazz_node__node__AheadCurrentKeys__get(const jazz_node_AheadCurrentKeys *k, const jazz_node_AheadKey *key) {
    return jz_hmap_get(&k->keys, key);
}

bool jazz_node__node__AheadCurrentKeys__contains_key(const jazz_node_AheadCurrentKeys *k, const jazz_node_AheadKey *key) {
    return jz_hmap_get(&k->keys, key) != NULL;
}

bool jazz_node__node__AheadCurrentKeys__insert(jazz_node_AheadCurrentKeys *k, jazz_node_AheadKey key, jazz_model_TxId tx, jazz_model_TxId *prev) {
    jazz_types_PhysicalTableId table_id = key.table;
    jz_slice branch_slice = jazz_node__node__codec__global_current_key_branch_prefix(jz_bytes_slice(&key.key));
    jz_bytes branch = jz_bytes_from(branch_slice.ptr, branch_slice.len);
    jazz_model_TxId old;
    bool had = jz_hmap_insert(&k->keys, &key, &tx, &old);
    if (!had) {
        bool inserted;
        jz_hmap *branches = jz_hmap_entry(&k->rows_per_branch, &table_id, &inserted);
        if (inserted) jz_hmap_init(branches, &desc_branch_rows);
        usize *rows = jz_hmap_entry(branches, &branch, &inserted);
        if (inserted) *rows = 0; else jz_bytes_drop(&branch);
        *rows += 1;
    } else {
        jz_bytes_drop(&branch);
        if (prev) *prev = old;
    }
    return had;
}

bool jazz_node__node__AheadCurrentKeys__remove(jazz_node_AheadCurrentKeys *k, const jazz_node_AheadKey *key, jazz_model_TxId *removed) {
    jazz_node_AheadKey okey; jazz_model_TxId oval;
    bool had = jz_hmap_remove(&k->keys, key, &okey, &oval);
    if (had) {
        jz_hmap *branches = jz_hmap_get(&k->rows_per_branch, &key->table);
        if (branches) {
            jz_slice branch = jazz_node__node__codec__global_current_key_branch_prefix(jz_bytes_slice(&key->key));
            jz_bytes probe = { (u8 *)branch.ptr, branch.len, 0 };
            usize *rows = jz_hmap_get(branches, &probe);
            if (rows) {
                *rows -= 1;
                if (*rows == 0) {
                    jz_bytes ok; usize ov;
                    jz_hmap_remove(branches, &probe, &ok, &ov);
                    jz_bytes_drop(&ok);
                }
            }
            if (branches->len == 0) {
                u64 tk; jz_hmap tv;
                jz_hmap_remove(&k->rows_per_branch, &key->table, &tk, &tv);
                jz_hmap_drop(&tv);
            }
        }
        ahead_key_drop(&okey);
        if (removed) *removed = oval;
    }
    return had;
}

void jazz_node__node__AheadCurrentKeys__clear(jazz_node_AheadCurrentKeys *k) {
    jz_hmap_clear(&k->keys);
    jz_hmap_clear(&k->rows_per_branch);
}

usize jazz_node__node__AheadCurrentKeys__branch_rows(const jazz_node_AheadCurrentKeys *k, jazz_types_PhysicalTableId t, const jazz_model_BranchKey *b) {
    const jz_hmap *branches = jz_hmap_get(&k->rows_per_branch, &t);
    if (!branches) return 0;
    jz_bytes prefix = {0};
    jazz_node__node__codec__global_current_branch_key_prefix(b, &prefix);
    const usize *rows = jz_hmap_get(branches, &prefix);
    usize n = rows ? *rows : 0;
    jz_bytes_drop(&prefix);
    return n;
}

/* ---------------- misc free fns ---------------- */
static u64 NEXT_GROOVE_RUNTIME_TOKEN = 1;

bool jazz_node__node__is_catalogue_mutation(const jazz_protocol_SyncMessage *m) {
    switch (jazz_protocol__protocol__SyncMessage__tag(m)) {
    case JAZZ_SM_CATALOGUE_SNAPSHOT:
    case JAZZ_SM_PUBLISH_SCHEMA:
    case JAZZ_SM_PUBLISH_SCHEMA_WITH_LENS:
    case JAZZ_SM_PUBLISH_LENS: return true;
    default: return false;
    }
}

u64 jazz_node__node__next_groove_runtime_token(void) { return NEXT_GROOVE_RUNTIME_TOKEN++; }

void jazz_node__node__record_maintained_view_stream_b_add_bundle(void) {}
void jazz_node__node__record_maintained_view_removal_stream_bundle(void) {}

/* ---------------- ActiveSchema ---------------- */
jazz_protocol_CurrentWriteSchema jazz_node__node__ActiveSchema__wire_pointer(const jazz_node_ActiveSchema *s) {
    jazz_protocol_CurrentWriteSchema w = { s->revision, s->schema };
    return w;
}

bool jazz_node__node__ActiveSchema__same_permissions(const jazz_node_ActiveSchema *a, const jazz_node_ActiveSchema *b) {
    const jazz_model_TableSchemaVec *ta = jazz_model__schema__JazzSchema__tables(a->compiled);
    const jazz_model_TableSchemaVec *tb = jazz_model__schema__JazzSchema__tables(b->compiled);
    if (ta->len != tb->len) return false;
    for (u32 i = 0; i < ta->len; i++) {
        const jazz_model_TableSchema *x = &ta->ptr[i], *y = &tb->ptr[i];
        if (!jz_str_eq(&x->name, &y->name)) return false;
        if ((x->read_policy == NULL) != (y->read_policy == NULL)) return false;
        if (x->read_policy && !jazz_model__query__Query__eq(x->read_policy, y->read_policy)) return false;
        const jazz_model_Query *xw[4] = { x->write_policies.insert_check, x->write_policies.update_using, x->write_policies.update_check, x->write_policies.delete_using };
        const jazz_model_Query *yw[4] = { y->write_policies.insert_check, y->write_policies.update_using, y->write_policies.update_check, y->write_policies.delete_using };
        for (u32 j = 0; j < 4; j++) {
            if ((xw[j] == NULL) != (yw[j] == NULL)) return false;
            if (xw[j] && !jazz_model__query__Query__eq(xw[j], yw[j])) return false;
        }
    }
    return true;
}

bool jazz_node__node__ActiveSchema__same_authorization_source(const jazz_node_ActiveSchema *self, const jazz_node_ActiveSchema *other,
                                                              const jazz_node_SchemaPhysicalMapping *source_mapping,
                                                              const jazz_node_SchemaPhysicalMapping *other_mapping) {
    if (!jazz_node__node__ActiveSchema__same_permissions(self, other)) return false;
    if (jz_uuid_eq(&self->schema, &other->schema)) return true;
    const jazz_model_TableSchemaVec *ta = jazz_model__schema__JazzSchema__tables(self->compiled);
    const jazz_model_TableSchemaVec *tb = jazz_model__schema__JazzSchema__tables(other->compiled);
    u32 n = ta->len < tb->len ? ta->len : tb->len;
    for (u32 i = 0; i < n; i++) {
        const jazz_model_TableSchema *table = &ta->ptr[i], *other_table = &tb->ptr[i];
        if (table->branch_by.len != 0 || other_table->branch_by.len != 0) return false;
        const jazz_node_TablePhysicalMapping *source = source_mapping ? jz_btree_get(&source_mapping->tables, &table->name) : NULL;
        const jazz_node_TablePhysicalMapping *target = other_mapping ? jz_btree_get(&other_mapping->tables, &table->name) : NULL;
        if (!source || !target) return false;
        if (source->table_id != target->table_id) return false;
        jazz_model_Query *allow = jazz_model__query__Query__from_table(&table->name);
        jazz_model_Query *deny = jazz_model__query__Query__filter_any_empty(jazz_model__query__Query__clone(allow));
        bool ok = true;
        if (table->read_policy)
            ok = jazz_model__query__Query__eq(table->read_policy, allow) || jazz_model__query__Query__eq(table->read_policy, deny);
        const jazz_model_Query *wp[4] = { table->write_policies.insert_check, table->write_policies.update_using, table->write_policies.update_check, table->write_policies.delete_using };
        for (u32 j = 0; ok && j < 4; j++) {
            const jazz_model_Query *p = wp[j];
            if (p) ok = jazz_model__query__Query__eq(p, allow) || jazz_model__query__Query__eq(p, deny);
        }
        jazz_model__query__Query__drop(allow);
        jazz_model__query__Query__drop(deny);
        if (!ok) return false;
    }
    return true;
}

jz_err jazz_node__node__ActiveSchema__new(jazz_protocol_CurrentWriteSchema pointer, jazz_model_JazzSchema *compiled, jazz_node_ActiveSchema *out) {
    jazz_types_SchemaVersionId v = jazz_model__schema__JazzSchema__version_id(compiled);
    if (!jz_uuid_eq(&v, &pointer.schema)) {
        jazz_model__schema__JazzSchema__drop(compiled);
        return JZN_INVALID_CATALOGUE("active schema permissions target mismatch");
    }
    out->schema = pointer.schema;
    out->revision = pointer.revision;
    out->compiled = compiled;
    return JZ_OK;
}

/* ---------------- Clock ---------------- */
static int gt_cmp(const void *a, const void *b, void *c) { return jz_cmp_u64(a, b, c); }
jz_err jazz_node__node__Clock__allocate_global_time(jazz_node_Clock *c, u64 now_ms, jazz_types_GlobalTime *out) {
    jazz_types_GlobalTime gt;
    JZ_TRY(jazz_types__time__GlobalTime__tick(c->global_time_register, now_ms, &gt));
    c->global_time_register = gt;
    jz_btree_insert(&c->locally_minted_global_times, &gt, NULL, NULL);
    *out = gt;
    return JZ_OK;
}

/* defined in state/read_payload.rs (other unit) */

jz_err jazz_node__node__NodeState__reserve_tx_time_after(NS *self, jazz_types_TxTime high_water) {
    jazz_types_TxTime next;
    JZ_TRY(jazz_types__time__TxTime__tick_after(high_water, &next));
    jazz_node__node__NodeState__merge_tx_time(self, next);
    return JZ_OK;
}

/* ---------------- RetainedRootWindowSource ---------------- */
typedef struct jazz_node_RetainedRootWindowSource {
    jazz_protocol_ProgramSourceId *source;
    jazz_model_ValidatedQuery *source_shape;
    u32 partition;   /* Global */
    void *order_by;  /* Vec<OrderBy> */
    u32 tie_break;   /* RootOccurrence */
    usize offset;
    bool has_limit; usize limit;
    jazz_model_Query *query_without_window;
} jazz_node_RetainedRootWindowSource;

const jazz_model_Query *jazz_model__query__ValidatedQuery__query(const jazz_model_ValidatedQuery *q);
jazz_model_ValidatedQuery *jazz_model__query__ValidatedQuery__clone(const jazz_model_ValidatedQuery *q);
void jazz_model__query__ValidatedQuery__drop(jazz_model_ValidatedQuery *q);
jazz_types_SchemaVersionId jazz_model__query__ValidatedQuery__schema_version(const jazz_model_ValidatedQuery *q);
bool jazz_model__query__ValidatedQuery__params_eq(const jazz_model_ValidatedQuery *a, const jazz_model_ValidatedQuery *b);
const jz_str *jazz_model__query__Query__table(const jazz_model_Query *q);
usize *jazz_model__query__Query__offset_mut(jazz_model_Query *q);
bool jazz_model__query__Query__limit(const jazz_model_Query *q, usize *out);
void jazz_model__query__Query__clear_limit(jazz_model_Query *q);
void *jazz_model__query__Query__order_by_clone(const jazz_model_Query *q);
bool jazz_model__query__OrderByVec__eq(const void *a, const void *b);
void jazz_model__query__OrderByVec__drop(void *v);
jazz_protocol_ProgramSourceId *jazz_protocol__protocol__ProgramSourceId__root(groove_InternStr table);
bool jazz_protocol__protocol__ProgramSourceId__eq(const jazz_protocol_ProgramSourceId *a, const jazz_protocol_ProgramSourceId *b);
void jazz_protocol__protocol__ProgramSourceId__drop(jazz_protocol_ProgramSourceId *p);

void jazz_node__node__RetainedRootWindowSource__for_shape(const jazz_model_ValidatedQuery *shape, jazz_node_RetainedRootWindowSource *out) {
    const jazz_model_Query *query = jazz_model__query__ValidatedQuery__query(shape);
    jazz_model_Query *without = jazz_model__query__Query__clone(query);
    *jazz_model__query__Query__offset_mut(without) = 0;
    jazz_model__query__Query__clear_limit(without);
    out->source = jazz_protocol__protocol__ProgramSourceId__root(groove__Intern__new_str(jz_str_clone(jazz_model__query__Query__table(query))));
    out->source_shape = jazz_model__query__ValidatedQuery__clone(shape);
    out->partition = 0;
    out->order_by = jazz_model__query__Query__order_by_clone(query);
    out->tie_break = 0;
    out->offset = *jazz_model__query__Query__offset_mut((jazz_model_Query *)query);
    out->has_limit = jazz_model__query__Query__limit(query, &out->limit);
    out->query_without_window = without;
}

void jazz_node__node__RetainedRootWindowSource__drop(jazz_node_RetainedRootWindowSource *s) {
    jazz_protocol__protocol__ProgramSourceId__drop(s->source);
    jazz_model__query__ValidatedQuery__drop(s->source_shape);
    jazz_model__query__OrderByVec__drop(s->order_by);
    jazz_model__query__Query__drop(s->query_without_window);
}

bool jazz_node__node__RetainedRootWindowSource__is_bounded(const jazz_node_RetainedRootWindowSource *s) {
    return s->offset != 0 || s->has_limit;
}

bool jazz_node__node__RetainedRootWindowSource__contains_target(const jazz_node_RetainedRootWindowSource *s, const jazz_model_ValidatedQuery *target) {
    jazz_node_RetainedRootWindowSource tw;
    jazz_node__node__RetainedRootWindowSource__for_shape(target, &tw);
    jazz_types_SchemaVersionId a = jazz_model__query__ValidatedQuery__schema_version(s->source_shape);
    jazz_types_SchemaVersionId b = jazz_model__query__ValidatedQuery__schema_version(target);
    bool ok = true;
    if (!jazz_protocol__protocol__ProgramSourceId__eq(s->source, tw.source) || s->partition != tw.partition ||
        !jazz_model__query__OrderByVec__eq(s->order_by, tw.order_by) || s->tie_break != tw.tie_break ||
        !jz_uuid_eq(&a, &b) || !jazz_model__query__ValidatedQuery__params_eq(s->source_shape, target) ||
        !jazz_model__query__Query__eq(s->query_without_window, tw.query_without_window) || s->offset > tw.offset)
        ok = false;
    if (ok) {
        bool has_se = s->has_limit, has_te = tw.has_limit;
        usize se = has_se ? (s->offset + s->limit < s->offset ? (usize)-1 : s->offset + s->limit) : 0;
        usize te = has_te ? (tw.offset + tw.limit < tw.offset ? (usize)-1 : tw.offset + tw.limit) : 0;
        if (has_se && has_te && te > se) ok = false;
        if (has_se && !has_te) ok = false;
    }
    jazz_node__node__RetainedRootWindowSource__drop(&tw);
    return ok;
}

void jazz_node__node__RetainedRootWindowSource__relative_window_for(const jazz_node_RetainedRootWindowSource *s, const jazz_model_ValidatedQuery *target,
                                                                    usize *offset, bool *has_limit, usize *limit) {
    const jazz_model_Query *q = jazz_model__query__ValidatedQuery__query(target);
    *offset = *jazz_model__query__Query__offset_mut((jazz_model_Query *)q) - s->offset;
    *has_limit = jazz_model__query__Query__limit(q, limit);
}

/* ---------------- CoveredInputCoordinate ---------------- */
/* struct in mod.h */

void jazz_node__node__CoveredInputCoordinate__From__from(jazz_node_CoveredInputCoordinate *out, const jazz_protocol_SupportingRow *input) {
    out->physical_table = input->physical_table;
    out->row = input->row;
    out->layer = input->version.layer;
    out->branch = input->version.has_branch_or_prefix ? jz_bytes_clone(&input->version.branch_or_prefix) : (jz_bytes){0};
}

int jazz_node__node__CoveredInputCoordinate__cmp(const void *a, const void *b, void *ctx) {
    const jazz_node_CoveredInputCoordinate *x = a, *y = b;
    int c = jz_uuid_cmp(&x->physical_table, &y->physical_table); if (c) return c;
    c = jz_uuid_cmp(&x->row, &y->row); if (c) return c;
    if (x->layer != y->layer) return x->layer < y->layer ? -1 : 1;
    return jz_bytes_cmp(&x->branch, &y->branch);
}

void jazz_node__node__CoveredInputCoordinate__drop(void *p) { jz_bytes_drop(&((jazz_node_CoveredInputCoordinate *)p)->branch); }
const jz_btree_desc jzn_desc_covered_inputs = { sizeof(jazz_node_CoveredInputCoordinate), sizeof(jazz_protocol_SupportingRow),
    jazz_node__node__CoveredInputCoordinate__cmp, jazz_node__node__CoveredInputCoordinate__drop, jazz_protocol__protocol__SupportingRow__drop };
const jz_btree_desc jzn_desc_compiled_scope_tables = { sizeof(jazz_types_GlobalPhysicalTableId), sizeof(groove_InternStr), jz_cmp_uuid, NULL, NULL };

/* #[derive(Default)] AuthorityResultState */
void jazz_node__node__AuthorityResultState__default(jazz_node_AuthorityResultState *out) {
    memset(out, 0, sizeof *out);
    out->source_closure_tag = JZN_SOURCE_CLOSURE_PENDING;
    jz_btree_init(&out->covered_input_versions, &jzn_desc_covered_inputs);
    jz_btree_init(&out->compiled_covered_input_sources, &jzn_desc_compiled_scope_tables);
}

void jazz_node__node__AuthorityResultState__drop(void *p) {
    jazz_node_AuthorityResultState *s = p;
    for (u32 i = 0; i < s->source_incrementals.len; i++) {
        jz_btree_drop(&s->source_incrementals.ptr[i].adds);
        jz_btree_drop(&s->source_incrementals.ptr[i].removes);
    }
    jz_free(s->source_incrementals.ptr);
    jz_btree_drop(&s->covered_input_versions);
    jz_btree_drop(&s->compiled_covered_input_sources);
}

/* ---------------- AuthoritySourceIncremental ---------------- */
static const jz_btree_desc desc_supporting_set = { sizeof(jazz_protocol_SupportingRow), 0, jazz_protocol__protocol__SupportingRow__cmp, jazz_protocol__protocol__SupportingRow__drop, NULL };

static bool sets_disjoint(const jz_btree *a, const jz_btree *b) {
    void *k, *v;
    JZ_BTREE_FOR(a, k, v) if (jz_btree_contains(b, k)) return false;
    return true;
}

jz_err jazz_node__node__AuthoritySourceIncremental__coalesce(bool has_previous, jazz_node_AuthoritySourceIncremental *previous,
                                                             u64 predecessor_generation, u64 generation,
                                                             jz_btree *adds, jz_btree *removes,
                                                             jazz_node_AuthoritySourceIncremental *out) {
    if (!sets_disjoint(adds, removes)) {
        jz_btree_drop(adds); jz_btree_drop(removes);
        return JZN_INVALID_STORED("covered input incremental delta overlaps additions and removals");
    }
    jazz_node_AuthoritySourceIncremental result;
    if (has_previous) {
        if (previous->generation != predecessor_generation) {
            jz_btree_drop(adds); jz_btree_drop(removes);
            return JZN_INVALID_STORED("covered input incremental delta does not continue its retained predecessor");
        }
        result = *previous;
    } else {
        result.predecessor_generation = predecessor_generation;
        result.generation = predecessor_generation;
        jz_btree_init(&result.adds, &desc_supporting_set);
        jz_btree_init(&result.removes, &desc_supporting_set);
    }
    jazz_protocol_SupportingRow fact;
    while (jz_btree_pop_first(adds, &fact, NULL)) {
        jazz_protocol_SupportingRow old;
        if (jz_btree_remove(&result.removes, &fact, &old, NULL)) {
            jazz_protocol__protocol__SupportingRow__drop(&old);
            jazz_protocol__protocol__SupportingRow__drop(&fact);
        } else if (jz_btree_insert(&result.adds, &fact, NULL, NULL)) {
            /* replaced: key dropped by btree */
        }
    }
    while (jz_btree_pop_first(removes, &fact, NULL)) {
        jazz_protocol_SupportingRow old;
        if (jz_btree_remove(&result.adds, &fact, &old, NULL)) {
            jazz_protocol__protocol__SupportingRow__drop(&old);
            jazz_protocol__protocol__SupportingRow__drop(&fact);
        } else {
            jz_btree_insert(&result.removes, &fact, NULL, NULL);
        }
    }
    jz_btree_drop(adds); jz_btree_drop(removes);
    result.generation = generation;
    *out = result;
    return JZ_OK;
}

static void supporting_row_clone_v(void *d, const void *s) { jazz_protocol__protocol__SupportingRow__clone(d, s); }

jz_err jazz_node__node__coalesce_authority_source_incrementals(const jazz_node_AuthoritySourceIncremental *frames, u32 nframes,
                                                               u64 predecessor_generation, u64 successor_generation,
                                                               jazz_node_AuthoritySourceIncremental *out) {
    u64 cursor = predecessor_generation;
    bool has_combined = false;
    jazz_node_AuthoritySourceIncremental combined;
    while (cursor != successor_generation) {
        const jazz_node_AuthoritySourceIncremental *frame = NULL;
        bool forked = false;
        for (u32 i = 0; i < nframes; i++) {
            if (frames[i].predecessor_generation != cursor) continue;
            if (frame) { forked = true; break; }
            frame = &frames[i];
        }
        if (!frame) {
            if (has_combined) { jz_btree_drop(&combined.adds); jz_btree_drop(&combined.removes); }
            return JZN_INVALID_STORED("covered input receiver missed an incremental predecessor; authority reset required");
        }
        if (forked || frame->generation <= cursor) {
            if (has_combined) { jz_btree_drop(&combined.adds); jz_btree_drop(&combined.removes); }
            return JZN_INVALID_STORED("covered input incremental chain is forked or nonmonotonic");
        }
        jz_btree adds, removes;
        jz_btree_clone(&adds, &frame->adds, supporting_row_clone_v, NULL);
        jz_btree_clone(&removes, &frame->removes, supporting_row_clone_v, NULL);
        jazz_node_AuthoritySourceIncremental next;
        JZ_TRY(jazz_node__node__AuthoritySourceIncremental__coalesce(has_combined, &combined, frame->predecessor_generation,
                                                                     frame->generation, &adds, &removes, &next));
        combined = next;
        has_combined = true;
        cursor = frame->generation;
    }
    if (!has_combined) return JZN_INVALID_STORED("covered input receiver has no incremental successor");
    *out = combined;
    return JZ_OK;
}

/* ---------------- CommitUnitIngestContext / Trust ---------------- */
static bool ctx_eq(const jazz_node_CommitUnitIngestContext *a, const jazz_node_CommitUnitIngestContext *b) {
    return jazz_types_AuthorSubject_eq(&a->identity, &b->identity) && a->trust == b->trust &&
           a->admitted_write_authorization == b->admitted_write_authorization &&
           a->version_receipts_validated == b->version_receipts_validated;
}

bool jazz_node__node__CommitUnitIngestContext__same_parked_authority(const jazz_node_CommitUnitIngestContext *existing,
                                                                     const jazz_node_CommitUnitIngestContext *resent) {
    if (existing && resent) {
        jazz_node_CommitUnitIngestContext merged = *existing;
        merged.version_receipts_validated = resent->version_receipts_validated;
        return ctx_eq(&merged, resent);
    }
    return existing == NULL && resent == NULL;
}

bool jazz_node__node__CommitUnitTrust__is_trusted(jazz_node_CommitUnitTrust t) {
    return t == JZN_TRUST_BACKEND || t == JZN_TRUST_AUTHORITY || t == JZN_TRUST_ADMIN;
}

/* ---------------- compiled lens paths ---------------- */
void jazz_node__node__CompiledLensOp__drop(void *p) {
    jazz_node_CompiledLensOp *op = p;
    jz_str_drop(&op->a); jz_str_drop(&op->b);
    if (op->tag == JZN_CLO_ADD) groove__records__values__Value__drop(&op->deflt);
}
void jazz_node__node__CompiledLensPath__drop(void *p) {
    jazz_node_CompiledLensPath *path = p;
    jz_str_drop(&path->target_table);
    jz_vec_drop(&path->ops, sizeof(jazz_node_CompiledLensOp), jazz_node__node__CompiledLensOp__drop);
}

void jazz_node__node__apply_compiled_lens_path(const jazz_node_CompiledLensPath *path, jz_btree *cells, jz_str *out_table) {
    for (u32 i = 0; i < path->ops.len; i++) {
        const jazz_node_CompiledLensOp *op = &path->ops.ptr[i];
        switch (op->tag) {
        case JZN_CLO_RENAME: {
            jz_str k; groove_Value value;
            if (jz_btree_remove(cells, &op->a, &k, &value)) {
                jz_str_drop(&k);
                jz_str to = jz_str_clone(&op->b);
                jz_btree_insert(cells, &to, &value, NULL);
            }
            break;
        }
        case JZN_CLO_COPY: {
            const groove_Value *v = jz_btree_get(cells, &op->a);
            if (v) {
                groove_Value value; groove__records__values__Value__clone(&value, v);
                jz_str to = jz_str_clone(&op->b);
                jz_btree_insert(cells, &to, &value, NULL);
            }
            break;
        }
        case JZN_CLO_ADD: {
            jz_str col = jz_str_clone(&op->a);
            bool inserted;
            groove_Value *slot = jz_btree_entry(cells, &col, &inserted);
            if (inserted) groove__records__values__Value__clone(slot, &op->deflt);
            else jz_str_drop(&col);
            break;
        }
        case JZN_CLO_DROP: {
            jz_str k; groove_Value value;
            if (jz_btree_remove(cells, &op->a, &k, &value)) { jz_str_drop(&k); groove__records__values__Value__drop(&value); }
            break;
        }
        }
    }
    *out_table = jz_str_clone(&path->target_table);
}

jz_err jazz_node__node__validate_registered_transform(const jz_str *transform) {
    const jazz_model_ColumnTransformSemantics *semantics = jazz_model__schema__registered_column_transform(transform);
    if (!semantics) return JZN_INVALID_CATALOGUE("transform column is not registered");
    if (!semantics->bijective || !semantics->canonical_equality_preserving)
        return JZN_INVALID_CATALOGUE("transform column is not bijective and canonical-preserving");
    return JZ_OK;
}

typedef JZ_VEC(jazz_node_CompiledLensOp) clo_vec;
static void push_op(clo_vec *v, jazz_node_CompiledLensOpTag tag, const jz_str *a, const jz_str *b, const groove_Value *d) {
    jazz_node_CompiledLensOp op = {0};
    op.tag = tag;
    op.a = jz_str_clone(a);
    if (b) op.b = jz_str_clone(b);
    if (d) groove__records__values__Value__clone(&op.deflt, d);
    JZ_VEC_PUSH(*v, op);
}

jz_err jazz_node__node__push_compiled_forward_lens_op(const jazz_protocol_LensOp *op, void *compiled_v) {
    clo_vec *compiled = compiled_v;
    switch (op->tag) {
    case JAZZ_LENSOP_RENAME_TABLE: break;
    case JAZZ_LENSOP_RENAME_COLUMN: push_op(compiled, JZN_CLO_RENAME, &op->a, &op->b, NULL); break;
    case JAZZ_LENSOP_COPY_COLUMN: push_op(compiled, JZN_CLO_COPY, &op->a, &op->b, NULL); break;
    case JAZZ_LENSOP_ADD_COLUMN: push_op(compiled, JZN_CLO_ADD, &op->a, NULL, &op->value); break;
    case JAZZ_LENSOP_DROP_COLUMN: push_op(compiled, JZN_CLO_DROP, &op->a, NULL, NULL); break;
    case JAZZ_LENSOP_TRANSFORM_COLUMN: JZ_TRY(jazz_node__node__validate_registered_transform(&op->b)); break;
    case JAZZ_LENSOP_REJECT_SOURCE_DELTA: return JZN_INVALID_CATALOGUE("lens op is not naturally mappable");
    }
    return JZ_OK;
}

jz_err jazz_node__node__push_compiled_reverse_lens_op(const jazz_protocol_LensOp *op, void *compiled_v) {
    clo_vec *compiled = compiled_v;
    switch (op->tag) {
    case JAZZ_LENSOP_RENAME_TABLE: break;
    case JAZZ_LENSOP_RENAME_COLUMN: push_op(compiled, JZN_CLO_RENAME, &op->b, &op->a, NULL); break;
    case JAZZ_LENSOP_COPY_COLUMN: push_op(compiled, JZN_CLO_DROP, &op->b, NULL, NULL); break;
    case JAZZ_LENSOP_ADD_COLUMN: push_op(compiled, JZN_CLO_DROP, &op->a, NULL, NULL); break;
    case JAZZ_LENSOP_DROP_COLUMN: push_op(compiled, JZN_CLO_ADD, &op->a, NULL, &op->value); break;
    case JAZZ_LENSOP_TRANSFORM_COLUMN: JZ_TRY(jazz_node__node__validate_registered_transform(&op->b)); break;
    case JAZZ_LENSOP_REJECT_SOURCE_DELTA: return JZN_INVALID_CATALOGUE("lens op is not naturally mappable");
    }
    return JZ_OK;
}

/* ---------------- CurrentRow ---------------- */
#define CURRENT_ROW_FIELD_ROW_UUID_IDX 0u
typedef jazz_engine_CurrentRowPublicationField pubfield;
typedef jazz_engine_CurrentRowPublicationFieldVec pubvec;
static void pubfield_drop_v(void *p) { jazz_engine__query_engine__publication__CurrentRowPublicationField__drop(p); }
static void pubvec_rc_drop(void *p) { jz_vec_drop(p, sizeof(pubfield), pubfield_drop_v); }
static void pubvec_clone(void *dst, const void *src) {
    jz_vec_clone(dst, src, sizeof(pubfield), jazz_engine__query_engine__publication__CurrentRowPublicationField__clone);
}
static void record_rc_drop(void *p) { (void)p; }

void jazz_node__node__CurrentRow__clone(void *dst, const void *src) {
    jazz_node_CurrentRow *d = dst; const jazz_node_CurrentRow *s = src;
    d->table = s->table;
    d->record = jz_rc_clone(s->record);
    d->deleted = s->deleted;
    d->publication_fields = jz_rc_clone(s->publication_fields);
}
void jazz_node__node__CurrentRow__drop(void *p) {
    jazz_node_CurrentRow *r = p;
    jz_rc_release(r->record);
    jz_rc_release(r->publication_fields);
}

/* #[derive(PartialEq)] on CurrentRow */
bool jazz_node__node__CurrentRow__PartialEq__eq(const jazz_node_CurrentRow *a, const jazz_node_CurrentRow *b) {
    if (!jz_str_eq(a->table, b->table)) return false;
    if (a->record != b->record && !groove__records__OwnedRecord__eq(a->record, b->record)) return false;
    if (a->deleted != b->deleted) return false;
    if (a->publication_fields == b->publication_fields) return true;
    if (a->publication_fields->len != b->publication_fields->len) return false;
    for (u32 i = 0; i < a->publication_fields->len; i++)
        if (!jazz_engine__query_engine__publication__CurrentRowPublicationField__PartialEq__eq(&a->publication_fields->ptr[i], &b->publication_fields->ptr[i])) return false;
    return true;
}

jazz_node_CurrentRow jazz_node__node__CurrentRow__new_with_publication_fields(jz_str table, groove_OwnedRecord *rec, pubvec fields) {
    if (fields.len != groove__records__RecordDescriptor__fields(groove__records__OwnedRecord__descriptor(rec))->len)
        jz_panic("publication fields must align with record slots");
    jazz_node_CurrentRow row;
    row.table = groove__Intern__new_str(table);
    groove_OwnedRecord **rc = jz_rc_new(sizeof(groove_OwnedRecord *), record_rc_drop);
    *rc = rec;
    row.record = rec;
    pubvec *pv = jz_rc_new(sizeof(pubvec), pubvec_rc_drop);
    *pv = fields;
    row.publication_fields = pv;
    row.deleted = false;
    return row;
}

static bool jazz_node__node__CurrentRow__physical_application_name(const groove_DescriptorField *field, jz_slice *out) {
    jz_slice name;
    if (!jazz_engine__query_engine__descriptor_public_name(field, &name)) return false;
    if (field->has_name && field->name.len == name.len && memcmp(field->name.ptr, name.ptr, name.len) == 0) {
        const u32 plen = sizeof(JAZZ_USER_COLUMN_PREFIX) - 1;
        if (name.len < plen || memcmp(name.ptr, JAZZ_USER_COLUMN_PREFIX, plen) != 0) return false;
        out->ptr = name.ptr + plen; out->len = name.len - plen;
        return true;
    }
    *out = name;
    return true;
}

jazz_node_CurrentRow jazz_node__node__CurrentRow__new_with_explicit_binding_fields_and_names(jz_str table, groove_OwnedRecord *rec,
                                                                                         jazz_engine_CurrentRowBindingRoleVec roles,
                                                                                         jzn_strptr_vec names) {
    const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(groove__records__OwnedRecord__descriptor(rec));
    if (roles.len != fields->len) jz_panic("native binding fields must align with their record descriptor");
    if (names.len != roles.len) jz_panic("native binding field names must align with their record descriptor");
    pubvec out = {0};
    for (u32 i = 0; i < fields->len; i++) {
        const groove_DescriptorField *field = &fields->ptr[i];
        if (!field->has_name) jz_panic("current row fields are named");
        const jz_str *name = &field->name;
        jz_str *public_name = names.ptr[i];
        pubfield f = {0};
        if (roles.ptr[i] == JAZZ_ROLE_PHYSICAL_COLUMN) {
            jz_slice phys;
            if (public_name) {
                f.tag = JAZZ_PUBF_UNRESOLVED_SOURCE_CELL; f.name = *public_name;
            } else if (jazz_node__node__CurrentRow__physical_application_name(field, &phys)) {
                f.tag = JAZZ_PUBF_UNRESOLVED_SOURCE_CELL; f.name = jz_str_from_slice(phys);
            } else {
                f.tag = JAZZ_PUBF_RESULT_FIELD; f.name = jz_str_clone(name);
                f.visibility = jazz_engine__query_engine__publication__CurrentRowResultVisibility__current_row_metadata(name);
            }
        } else {
            f.tag = JAZZ_PUBF_RESULT_FIELD;
            if (public_name) { f.visibility = JAZZ_VIS_APPLICATION_CELL; f.name = *public_name; }
            else { f.visibility = jazz_engine__query_engine__publication__CurrentRowResultVisibility__current_row_metadata(name); f.name = jz_str_clone(name); }
        }
        if (public_name) jz_free(public_name);
        JZ_VEC_PUSH(out, f);
    }
    jz_vec_drop(&roles, sizeof(*roles.ptr), NULL);
    jz_free(names.ptr);
    return jazz_node__node__CurrentRow__new_with_publication_fields(table, rec, out);
}

jazz_node_CurrentRow jazz_node__node__CurrentRow__new_with_explicit_binding_fields(jz_str table, groove_OwnedRecord *rec,
                                                                                 jazz_engine_CurrentRowBindingRoleVec roles) {
    const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(groove__records__OwnedRecord__descriptor(rec));
    jzn_strptr_vec names = {0};
    for (u32 i = 0; i < fields->len && i < roles.len; i++) {
        jz_str *n = NULL;
        if (roles.ptr[i] == JAZZ_ROLE_LOGICAL_FIELD) {
            jz_slice pn;
            if (jazz_engine__query_engine__descriptor_public_name(&fields->ptr[i], &pn)) {
                n = JZ_NEW(jz_str); *n = jz_str_from_slice(pn);
            }
        }
        JZ_VEC_PUSH(names, n);
    }
    return jazz_node__node__CurrentRow__new_with_explicit_binding_fields_and_names(table, rec, roles, names);
}

jazz_node_CurrentRow jazz_node__node__CurrentRow__new_with_binding_fields(jz_str table, groove_OwnedRecord *rec, jazz_engine_CurrentRowBindingRole role) {
    u32 n = groove__records__RecordDescriptor__fields(groove__records__OwnedRecord__descriptor(rec))->len;
    jazz_engine_CurrentRowBindingRoleVec roles = {0};
    for (u32 i = 0; i < n; i++) JZ_VEC_PUSH(roles, role);
    return jazz_node__node__CurrentRow__new_with_explicit_binding_fields(table, rec, roles);
}

jazz_node_CurrentRow jazz_node__node__CurrentRow__new(jz_str table, groove_OwnedRecord *rec) {
    return jazz_node__node__CurrentRow__new_with_binding_fields(table, rec, JAZZ_ROLE_PHYSICAL_COLUMN);
}

jazz_node_CurrentRow jazz_node__node__CurrentRow__into_deleted(jazz_node_CurrentRow r) { r.deleted = true; return r; }
bool jazz_node__node__CurrentRow__is_deleted(const jazz_node_CurrentRow *r) { return r->deleted; }

jazz_engine_CurrentRowBindingRoleVec jazz_node__node__CurrentRow__binding_fields(const jazz_node_CurrentRow *r) {
    jazz_engine_CurrentRowBindingRoleVec out = {0};
    for (u32 i = 0; i < r->publication_fields->len; i++)
        JZ_VEC_PUSH(out, jazz_engine__query_engine__publication__CurrentRowPublicationField__role(&r->publication_fields->ptr[i]));
    return out;
}

const pubvec *jazz_node__node__CurrentRow__publication_fields(const jazz_node_CurrentRow *r) { return r->publication_fields; }

void jazz_node__node__CurrentRow__binding_field_names(const jazz_node_CurrentRow *r, JZ_VEC(const jz_str *) *out) {
    for (u32 i = 0; i < r->publication_fields->len; i++) {
        const pubfield *f = &r->publication_fields->ptr[i];
        const jz_str *n = NULL;
        if (f->tag == JAZZ_PUBF_RESULT_FIELD && (f->visibility == JAZZ_VIS_APPLICATION_CELL || f->visibility == JAZZ_VIS_PUBLIC_PROVENANCE))
            n = &f->name;
        JZ_VEC_PUSH(*out, n);
    }
}

const jz_str *jazz_node__node__CurrentRow__table(const jazz_node_CurrentRow *r) { return r->table; }

jazz_types_RowUuid jazz_node__node__CurrentRow__row_uuid(const jazz_node_CurrentRow *r) {
    u32 idx;
    if (!groove__records__RecordDescriptor__field_index(groove__records__OwnedRecord__descriptor(r->record), "row_uuid", &idx))
        idx = CURRENT_ROW_FIELD_ROW_UUID_IDX;
    jz_uuid u;
    jz_err e = groove__records__BorrowedRecord__get_uuid(r->record, idx, &u);
    if (JZ_IS_ERR(e)) jz_panic("valid current row_uuid");
    return u;
}

static bool unwrap_nullable(groove_Value *value, groove_Value *out) {
    if (value->tag == GROOVE_V_NULLABLE) {
        if (!value->v.nullable) return false;
        *out = *value->v.nullable;
        jz_free(value->v.nullable);
        return true;
    }
    *out = *value;
    return true;
}

bool jazz_node__node__CurrentRow__cell_at(const jazz_node_CurrentRow *r, u32 pos, groove_Value *out) {
    u32 idx, user_cells = 0;
    if (groove__records__RecordDescriptor__field_index(groove__records__OwnedRecord__descriptor(r->record), "row_uuid", &idx))
        user_cells = idx + 1;
    groove_Value value;
    if (JZ_IS_ERR(groove__records__BorrowedRecord__get_idx(r->record, user_cells + pos, &value)))
        jz_panic("valid current user cell");
    return unwrap_nullable(&value, out);
}

static bool jazz_node__node__CurrentRow__application_column_index_by_name(const jazz_node_CurrentRow *r, const jz_str *column, u32 *out) {
    static const jazz_engine_CurrentRowBindingRole roles[2] = { JAZZ_ROLE_PHYSICAL_COLUMN, JAZZ_ROLE_LOGICAL_FIELD };
    for (u32 ri = 0; ri < 2; ri++) {
        for (u32 i = 0; i < r->publication_fields->len; i++) {
            const pubfield *f = &r->publication_fields->ptr[i];
            jz_slice name;
            if (jazz_engine__query_engine__publication__CurrentRowPublicationField__role(f) == roles[ri] &&
                jazz_engine__query_engine__publication__CurrentRowPublicationField__application_name(f, &name) &&
                name.len == column->len && memcmp(name.ptr, column->ptr, name.len) == 0) {
                *out = i;
                return true;
            }
        }
    }
    return false;
}

static const jazz_model_ColumnSchema *find_column(const jazz_model_TableSchema *t, const jz_str *column) {
    for (u32 i = 0; i < t->columns.len; i++) if (jz_str_eq(&t->columns.ptr[i].name, column)) return &t->columns.ptr[i];
    return NULL;
}

static bool jazz_node__node__CurrentRow__application_column_index(const jazz_node_CurrentRow *r, const jazz_model_TableSchema *t, const jz_str *column, u32 *out) {
    if (!find_column(t, column)) return false;
    return jazz_node__node__CurrentRow__application_column_index_by_name(r, column, out);
}

bool jazz_node__node__CurrentRow__cell(const jazz_node_CurrentRow *r, const jazz_model_TableSchema *t, const jz_str *column, groove_Value *out) {
    u32 idx;
    if (!jazz_node__node__CurrentRow__application_column_index(r, t, column, &idx)) return false;
    groove_Value value;
    jz_err e = groove__records__BorrowedRecord__get_idx(r->record, idx, &value);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    const jazz_model_ColumnSchema *declared = find_column(t, column);
    if (!declared) { groove__records__values__Value__drop(&value); return false; }
    if (declared->column_type.tag == GROOVE_VT_NULLABLE) {
        const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(groove__records__OwnedRecord__descriptor(r->record));
        if (idx < fields->len && groove__records__values__ValueType__eq(&fields->ptr[idx].value_type, &declared->column_type)) {
            *out = value;
            return true;
        }
    }
    return unwrap_nullable(&value, out);
}

bool jazz_node__node__CurrentRow__raw_field(const jazz_node_CurrentRow *r, const char *field, groove_Value *out) {
    u32 idx;
    if (!groove__records__RecordDescriptor__field_index(groove__records__OwnedRecord__descriptor(r->record), field, &idx)) return false;
    jz_err e = groove__records__BorrowedRecord__get_idx(r->record, idx, out);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    return true;
}

void jazz_node__node__CurrentRow__encoded_record(const jazz_node_CurrentRow *r, const groove_RecordDescriptor **d, jz_slice *raw) {
    *d = groove__records__OwnedRecord__descriptor(r->record);
    *raw = groove__records__OwnedRecord__raw(r->record);
}

static bool jazz_node__node__CurrentRow__provenance_field_index(const jazz_node_CurrentRow *r, const char *column, u32 *out) {
    const char *stored;
    if (!strcmp(column, "$createdBy")) stored = "created_by";
    else if (!strcmp(column, "$createdAt")) stored = "created_at";
    else if (!strcmp(column, "$updatedBy")) stored = "updated_by";
    else if (!strcmp(column, "$updatedAt")) stored = "updated_at";
    else return false;
    const groove_RecordDescriptor *d = groove__records__OwnedRecord__descriptor(r->record);
    if (groove__records__RecordDescriptor__field_index(d, column, out)) return true;
    u32 tmp;
    if (groove__records__RecordDescriptor__field_index(d, "schema_version", &tmp) &&
        groove__records__RecordDescriptor__field_index(d, "branch_key", &tmp))
        return groove__records__RecordDescriptor__field_index(d, stored, out);
    return false;
}

jz_err jazz_types__ids__RowAuthor__from_record(const groove_OwnedRecord *rec, jazz_types_RowAuthor *out);
groove_Value jazz_types__ids__RowAuthor__to_value(jazz_types_RowAuthor a);
groove_ValueType jazz_types__ids__RowAuthor__value_type(void);

static jz_err author_at(const groove_OwnedRecord *rec, u32 idx, jazz_types_AuthorSubject *out) {
    groove_OwnedRecord *nested;
    JZ_TRY(groove__records__BorrowedRecord__get_record(rec, idx, &nested));
    jazz_types_RowAuthor author;
    jz_err e = jazz_types__ids__RowAuthor__from_record(nested, &author);
    groove__records__OwnedRecord__drop(nested);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return jz_err_new(GROOVE_REC_ERR_NON_CANONICAL, "non-canonical record"); }
    *out = jazz_types__ids__RowAuthor__as_author_subject(author);
    return JZ_OK;
}

jz_err jazz_node__node__CurrentRow__provenance(const jazz_node_CurrentRow *r, bool *has, jazz_node_RowProvenance *out) {
    u32 cb, ca, ub, ua;
    *has = false;
    if (!jazz_node__node__CurrentRow__provenance_field_index(r, "$createdBy", &cb) ||
        !jazz_node__node__CurrentRow__provenance_field_index(r, "$createdAt", &ca) ||
        !jazz_node__node__CurrentRow__provenance_field_index(r, "$updatedBy", &ub) ||
        !jazz_node__node__CurrentRow__provenance_field_index(r, "$updatedAt", &ua))
        return JZ_OK;
    JZ_TRY(author_at(r->record, cb, &out->created_by));
    JZ_TRY(groove__records__BorrowedRecord__get_u64(r->record, ca, &out->created_at));
    JZ_TRY(author_at(r->record, ub, &out->updated_by));
    JZ_TRY(groove__records__BorrowedRecord__get_u64(r->record, ua, &out->updated_at));
    *has = true;
    return JZ_OK;
}

bool jazz_node__node__CurrentRow__projected_tx_alias(const jazz_node_CurrentRow *r, jazz_types_TxTime *t, jazz_types_NodeAlias *a) {
    const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(groove__records__OwnedRecord__descriptor(r->record));
    u32 stamp_idx = 0; bool found = false;
    for (u32 i = 0; i < fields->len; i++)
        if (fields->ptr[i].has_name && jz_str_eq_cstr(&fields->ptr[i].name, "tx_time")) { stamp_idx = i; found = true; break; }
    if (!found) return false;
    u32 alias_idx = stamp_idx + 1;
    if (alias_idx >= fields->len) return false;
    if (!fields->ptr[alias_idx].has_name || !jz_str_eq_cstr(&fields->ptr[alias_idx].name, "tx_node_id")) return false;
    u64 time, alias;
    jz_err e = groove__records__BorrowedRecord__get_u64(r->record, stamp_idx, &time);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    e = groove__records__BorrowedRecord__get_u64(r->record, alias_idx, &alias);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    if (time == 0 && alias == 0) return false;
    *t = time; *a = alias;
    return true;
}

static groove_Value v_u64(u64 x) { groove_Value v = { GROOVE_V_U64 }; v.v.u = x; return v; }
static groove_Value v_uuid(jz_uuid u) { groove_Value v = { GROOVE_V_UUID }; v.v.uuid = u; return v; }
static groove_Value v_nullable(groove_Value *inner /* moved, may be NULL */) {
    groove_Value v = { GROOVE_V_NULLABLE };
    if (inner) { v.v.nullable = jz_alloc(sizeof(groove_Value)); *v.v.nullable = *inner; } else v.v.nullable = NULL;
    return v;
}
static groove_ValueType vt_simple(u32 tag) { groove_ValueType t = { tag }; return t; }
static groove_ValueType vt_nullable(const groove_ValueType *inner) {
    groove_ValueType t = { GROOVE_VT_NULLABLE };
    t.v.inner = jz_alloc(sizeof(groove_ValueType));
    groove__records__values__ValueType__clone(t.v.inner, inner);
    return t;
}
static void push_field(groove_DescriptorFieldVec *v, const char *name, groove_ValueType ty) {
    groove_DescriptorField f;
    groove__records__DescriptorField__new(&f, jz_str_from(name), ty);
    JZ_VEC_PUSH(*v, f);
}

/* column_binding closure: either "by application name" (project) or an explicit vtable */
typedef bool (*column_binding_fn)(const jazz_node_CurrentRow *self, const jz_str *name, u32 *out);

static jz_str projected_public_name(const jazz_node_CurrentRow *self, column_binding_fn binding, const jz_str *column_name) {
    u32 index; jz_slice name;
    if (binding(self, column_name, &index) &&
        jazz_engine__query_engine__publication__CurrentRowPublicationField__application_name(&self->publication_fields->ptr[index], &name))
        return jz_str_from_slice(name);
    return jz_str_clone(column_name);
}

static jazz_engine_CurrentRowBindingRole binding_role_at(const jazz_node_CurrentRow *self, bool found, u32 index) {
    if (found && index < self->publication_fields->len)
        return jazz_engine__query_engine__publication__CurrentRowPublicationField__role(&self->publication_fields->ptr[index]);
    return JAZZ_ROLE_LOGICAL_FIELD;
}

static jz_err jazz_node__node__CurrentRow__project_with_column_binding(const jazz_node_CurrentRow *self, const jazz_model_TableSchema *table,
                                                                       const jz_str *columns, u32 ncols, column_binding_fn column_binding,
                                                                       jazz_node_CurrentRow *out) {
    jz_btree selected;
    jz_btree_init(&selected, &mod_desc_str_set);
    for (u32 i = 0; i < ncols; i++) { jz_str c = jz_str_clone(&columns[i]); if (jz_btree_insert(&selected, &c, NULL, NULL)) {} }
    JZ_VEC(const jazz_model_ColumnSchema *) projected_columns = {0};
    for (u32 i = 0; i < table->columns.len; i++)
        if (jz_btree_contains(&selected, &table->columns.ptr[i].name)) JZ_VEC_PUSH(projected_columns, &table->columns.ptr[i]);
    jz_btree_drop(&selected);

    groove_DescriptorFieldVec descriptor_fields = {0};
    push_field(&descriptor_fields, "row_uuid", vt_simple(GROOVE_VT_UUID));
    for (u32 i = 0; i < projected_columns.len; i++) {
        const jazz_model_ColumnSchema *column = projected_columns.ptr[i];
        jz_str public_name = projected_public_name(self, column_binding, &column->name);
        groove_DescriptorField f;
        groove__records__DescriptorField__new(&f, jazz_engine__query_engine__user_column_field(&column->name), vt_nullable(&column->column_type));
        groove__records__DescriptorField__with_identity_name(&f, public_name);
        JZ_VEC_PUSH(descriptor_fields, f);
    }
    groove_ValueVec values = {0};
    JZ_VEC_PUSH(values, v_uuid(jazz_node__node__CurrentRow__row_uuid(self)));
    u32 row_idx;
    bool has_row_idx = groove__records__RecordDescriptor__field_index(groove__records__OwnedRecord__descriptor(self->record), "row_uuid", &row_idx);
    jazz_engine_CurrentRowBindingRoleVec binding_fields = {0};
    JZ_VEC_PUSH(binding_fields, binding_role_at(self, has_row_idx, row_idx));
    jzn_strptr_vec binding_field_names = {0};
    jz_str *none = NULL;
    JZ_VEC_PUSH(binding_field_names, none);
    for (u32 i = 0; i < projected_columns.len; i++) {
        const jazz_model_ColumnSchema *column = projected_columns.ptr[i];
        jz_str public_name = projected_public_name(self, column_binding, &column->name);
        u32 idx;
        bool bound = column_binding(self, &column->name, &idx);
        bool has_cell = false;
        groove_Value cell;
        if (bound) {
            groove_Value raw;
            jz_err e = groove__records__BorrowedRecord__get_idx(self->record, idx, &raw);
            if (JZ_IS_ERR(e)) jz_err_drop(&e);
            else has_cell = unwrap_nullable(&raw, &cell);
        }
        groove_Value projected;
        if (column->column_type.tag == GROOVE_VT_NULLABLE) {
            if (has_cell && cell.tag == GROOVE_V_NULLABLE) projected = v_nullable(&cell);
            else if (has_cell) { groove_Value inner = v_nullable(&cell); projected = v_nullable(&inner); }
            else projected = v_nullable(NULL);
        } else {
            projected = v_nullable(has_cell ? &cell : NULL);
        }
        JZ_VEC_PUSH(values, projected);
        JZ_VEC_PUSH(binding_fields, binding_role_at(self, bound, idx));
        jz_str *pn = JZ_NEW(jz_str); *pn = public_name;
        JZ_VEC_PUSH(binding_field_names, pn);
    }
    jz_free(projected_columns.ptr);

    bool has_prov;
    jazz_node_RowProvenance provenance;
    jz_err e = jazz_node__node__CurrentRow__provenance(self, &has_prov, &provenance);
    if (JZ_IS_ERR(e)) goto fail;
    if (has_prov) {
        push_field(&descriptor_fields, "$createdBy", jazz_types__ids__RowAuthor__value_type());
        push_field(&descriptor_fields, "$createdAt", vt_simple(GROOVE_VT_U64));
        push_field(&descriptor_fields, "$updatedBy", jazz_types__ids__RowAuthor__value_type());
        push_field(&descriptor_fields, "$updatedAt", vt_simple(GROOVE_VT_U64));
        jazz_types_RowAuthor ra;
        e = jazz_types__ids__RowAuthor__from_persisted_subject(provenance.created_by, &ra);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); e = JZN_ERR(JZN_E_UNADMITTED_WRITE_AUTHOR, NULL); goto fail; }
        JZ_VEC_PUSH(values, jazz_types__ids__RowAuthor__to_value(ra));
        JZ_VEC_PUSH(values, v_u64(provenance.created_at));
        e = jazz_types__ids__RowAuthor__from_persisted_subject(provenance.updated_by, &ra);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); e = JZN_ERR(JZN_E_UNADMITTED_WRITE_AUTHOR, NULL); goto fail; }
        JZ_VEC_PUSH(values, jazz_types__ids__RowAuthor__to_value(ra));
        JZ_VEC_PUSH(values, v_u64(provenance.updated_at));
        static const char *const prov_cols[4] = { "$createdBy", "$createdAt", "$updatedBy", "$updatedAt" };
        for (u32 i = 0; i < 4; i++) {
            u32 idx;
            bool f = jazz_node__node__CurrentRow__provenance_field_index(self, prov_cols[i], &idx);
            JZ_VEC_PUSH(binding_fields, binding_role_at(self, f, idx));
            JZ_VEC_PUSH(binding_field_names, none);
        }
    }
    jazz_types_TxTime time; jazz_types_NodeAlias node;
    if (jazz_node__node__CurrentRow__projected_tx_alias(self, &time, &node)) {
        push_field(&descriptor_fields, "tx_time", vt_simple(GROOVE_VT_U64));
        push_field(&descriptor_fields, "tx_node_id", vt_simple(GROOVE_VT_U64));
        JZ_VEC_PUSH(values, v_u64(time));
        JZ_VEC_PUSH(values, v_u64(node));
        JZ_VEC_PUSH(binding_fields, JAZZ_ROLE_LOGICAL_FIELD);
        JZ_VEC_PUSH(binding_fields, JAZZ_ROLE_LOGICAL_FIELD);
        JZ_VEC_PUSH(binding_field_names, none);
        JZ_VEC_PUSH(binding_field_names, none);
    }
    groove_RecordDescriptor *descriptor = groove__records__RecordDescriptor__new_with_fields(descriptor_fields);
    jz_bytes raw;
    e = groove__records__RecordDescriptor__create(descriptor, values.ptr, values.len, &raw);
    jz_vec_drop(&values, sizeof(groove_Value), value_drop_v);
    if (JZ_IS_ERR(e)) { groove__records__RecordDescriptor__drop_box(descriptor); goto fail2; }
    jazz_node_CurrentRow projected = jazz_node__node__CurrentRow__new_with_explicit_binding_fields_and_names(
        jz_str_clone(&table->name), groove__records__OwnedRecord__new(raw, descriptor), binding_fields, binding_field_names);
    pubvec *fields = jz_rc_make_mut((void **)&projected.publication_fields, sizeof(pubvec), pubvec_clone);
    for (u32 i = 0; i < fields->len; i++) {
        jz_slice name;
        if (!jazz_engine__query_engine__publication__CurrentRowPublicationField__application_name(&fields->ptr[i], &name)) continue;
        jz_str n = jz_str_from_slice(name);
        u32 index;
        if (jazz_node__node__CurrentRow__application_column_index_by_name(self, &n, &index)) {
            jazz_engine__query_engine__publication__CurrentRowPublicationField__drop(&fields->ptr[i]);
            jazz_engine__query_engine__publication__CurrentRowPublicationField__clone(&fields->ptr[i], &self->publication_fields->ptr[index]);
        }
        jz_str_drop(&n);
    }
    *out = projected;
    return JZ_OK;
fail:
    jz_vec_drop(&values, sizeof(groove_Value), value_drop_v);
    jz_vec_drop(&descriptor_fields, sizeof(groove_DescriptorField), groove__records__DescriptorField__drop);
fail2:
    jz_vec_drop(&binding_fields, sizeof(*binding_fields.ptr), NULL);
    for (u32 i = 0; i < binding_field_names.len; i++) if (binding_field_names.ptr[i]) { jz_str_drop(binding_field_names.ptr[i]); jz_free(binding_field_names.ptr[i]); }
    jz_free(binding_field_names.ptr);
    return e;
}

jz_err jazz_node__node__CurrentRow__project(const jazz_node_CurrentRow *r, const jazz_model_TableSchema *t, const jz_str *cols, u32 ncols, jazz_node_CurrentRow *out) {
    return jazz_node__node__CurrentRow__project_with_column_binding(r, t, cols, ncols, jazz_node__node__CurrentRow__application_column_index_by_name, out);
}


jz_err jazz_node__node__CurrentRow__project_relation(const jazz_node_CurrentRow *self, const jazz_model_TableSchema *table,
                                                     const jazz_model_RelationProjectColumn *columns, u32 ncols, jazz_node_CurrentRow *out) {
    groove_DescriptorFieldVec descriptor_fields = {0};
    push_field(&descriptor_fields, "row_uuid", vt_simple(GROOVE_VT_UUID));
    groove_ValueVec values = {0};
    JZ_VEC_PUSH(values, v_uuid(jazz_node__node__CurrentRow__row_uuid(self)));
    pubvec publication_fields = {0};
    pubfield rf = { JAZZ_PUBF_RESULT_FIELD, 0, jz_str_from("row_uuid"), JAZZ_VIS_HIDDEN_METADATA };
    JZ_VEC_PUSH(publication_fields, rf);
    jz_err e = JZ_OK;
    for (u32 i = 0; i < ncols; i++) {
        const jazz_model_RelationProjectColumn *projection = &columns[i];
        groove_Value value; groove_ValueType field_type;
        if (projection->expr.tag == JAZZ_KEYREF_ROW_ID && projection->expr.row_id == JAZZ_ROWID_CURRENT) {
            value = v_uuid(jazz_node__node__CurrentRow__row_uuid(self)); field_type = vt_simple(GROOVE_VT_UUID);
        } else if (projection->expr.tag == JAZZ_KEYREF_COLUMN && jz_str_eq_cstr(&projection->expr.column.column, "id")) {
            value = v_uuid(jazz_node__node__CurrentRow__row_uuid(self)); field_type = vt_simple(GROOVE_VT_UUID);
        } else if (projection->expr.tag == JAZZ_KEYREF_COLUMN) {
            u32 column_position = 0; bool found = false;
            for (u32 c = 0; c < table->columns.len; c++)
                if (jz_str_eq(&table->columns.ptr[c].name, &projection->expr.column.column)) { column_position = c; found = true; break; }
            if (!found) { e = JZN_INVALID_STORED("relation output column is absent from the read schema"); goto fail; }
            groove__records__values__ValueType__clone(&field_type, &table->columns.ptr[column_position].column_type);
            if (!jazz_node__node__CurrentRow__cell_at(self, column_position, &value)) {
                if (field_type.tag == GROOVE_VT_NULLABLE) value = v_nullable(NULL);
                else {
                    groove__records__values__ValueType__drop(&field_type);
                    e = JZN_INVALID_STORED("relation output of a non-nullable column has no value");
                    goto fail;
                }
            }
        } else {
            e = JZN_INVALID_STORED("relation output contains an unsupported expression");
            goto fail;
        }
        JZ_VEC_PUSH(values, value);
        groove_DescriptorField f;
        groove__records__DescriptorField__new(&f, jz_str_clone(&projection->alias), field_type);
        groove__records__DescriptorField__with_identity_name(&f, jz_str_clone(&projection->alias));
        JZ_VEC_PUSH(descriptor_fields, f);
        pubfield pf = { JAZZ_PUBF_RESULT_FIELD, 0, jz_str_clone(&projection->alias), JAZZ_VIS_APPLICATION_CELL };
        JZ_VEC_PUSH(publication_fields, pf);
    }
    groove_RecordDescriptor *descriptor = groove__records__RecordDescriptor__new_with_fields(descriptor_fields);
    jz_bytes raw;
    e = groove__records__RecordDescriptor__create(descriptor, values.ptr, values.len, &raw);
    jz_vec_drop(&values, sizeof(groove_Value), value_drop_v);
    if (JZ_IS_ERR(e)) {
        groove__records__RecordDescriptor__drop_box(descriptor);
        jz_vec_drop(&publication_fields, sizeof(pubfield), pubfield_drop_v);
        return e;
    }
    *out = jazz_node__node__CurrentRow__new_with_publication_fields(jz_str_clone(&table->name), groove__records__OwnedRecord__new(raw, descriptor), publication_fields);
    return JZ_OK;
fail:
    jz_vec_drop(&values, sizeof(groove_Value), value_drop_v);
    jz_vec_drop(&descriptor_fields, sizeof(groove_DescriptorField), groove__records__DescriptorField__drop);
    jz_vec_drop(&publication_fields, sizeof(pubfield), pubfield_drop_v);
    return e;
}

/* canonical subscription cells: (name, postcard(Option<Value>)) sorted */
typedef struct sub_cell { jz_slice name; jz_bytes bytes; } sub_cell;
static int sub_cell_cmp(const void *a, const void *b, void *c) {
    const sub_cell *x = a, *y = b;
    int r = jz_slice_cmp(x->name, y->name);
    return r ? r : jz_bytes_cmp(&x->bytes, &y->bytes);
}
static void sub_cell_drop(void *p) { jz_bytes_drop(&((sub_cell *)p)->bytes); }
typedef JZ_VEC(sub_cell) sub_cells;

static bool jazz_node__node__CurrentRow__canonical_subscription_cells(const jazz_node_CurrentRow *r, sub_cells *out) {
    for (u32 idx = 0; idx < r->publication_fields->len; idx++) {
        jz_slice name;
        if (!jazz_engine__query_engine__publication__CurrentRowPublicationField__application_name(&r->publication_fields->ptr[idx], &name)) continue;
        groove_Value raw;
        jz_err e = groove__records__BorrowedRecord__get_idx(r->record, idx, &raw);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); continue; }
        groove_Value value;
        bool some = unwrap_nullable(&raw, &value);
        sub_cell c = { name, {0} };
        jz_pc_w w = { &c.bytes };
        jz_pc_option(&w, some);
        if (some) { groove__records__values__Value__serialize(&w, &value); groove__records__values__Value__drop(&value); }
        JZ_VEC_PUSH(*out, c);
    }
    jz_sort_unstable(out->ptr, out->len, sizeof(sub_cell), sub_cell_cmp, NULL);
    return true;
}

static bool jazz_node__node__CurrentRow__subscription_cells_equivalent(const jazz_node_CurrentRow *a, const jazz_node_CurrentRow *b) {
    sub_cells l = {0}, r = {0};
    bool ok = jazz_node__node__CurrentRow__canonical_subscription_cells(a, &l) && jazz_node__node__CurrentRow__canonical_subscription_cells(b, &r);
    if (ok) {
        ok = l.len == r.len;
        for (u32 i = 0; ok && i < l.len; i++) ok = sub_cell_cmp(&l.ptr[i], &r.ptr[i], NULL) == 0;
    }
    jz_vec_drop(&l, sizeof(sub_cell), sub_cell_drop);
    jz_vec_drop(&r, sizeof(sub_cell), sub_cell_drop);
    return ok;
}

static bool prov_eq(const jazz_node_RowProvenance *a, const jazz_node_RowProvenance *b) {
    return jazz_types_AuthorSubject_eq(&a->created_by, &b->created_by) && a->created_at == b->created_at &&
           jazz_types_AuthorSubject_eq(&a->updated_by, &b->updated_by) && a->updated_at == b->updated_at;
}

bool jazz_node__node__CurrentRow__subscription_equivalent(const jazz_node_CurrentRow *a, const jazz_node_CurrentRow *b) {
    bool ha, hb; jazz_node_RowProvenance pa, pb;
    jz_err ea = jazz_node__node__CurrentRow__provenance(a, &ha, &pa);
    jz_err eb = jazz_node__node__CurrentRow__provenance(b, &hb, &pb);
    bool provenance_matches = !JZ_IS_ERR(ea) && !JZ_IS_ERR(eb) && ha == hb && (!ha || prov_eq(&pa, &pb));
    jz_err_drop(&ea); jz_err_drop(&eb);
    jazz_types_RowUuid ra = jazz_node__node__CurrentRow__row_uuid(a), rb = jazz_node__node__CurrentRow__row_uuid(b);
    return a->table == b->table && jz_uuid_eq(&ra, &rb) && a->deleted == b->deleted &&
           jazz_node__node__CurrentRow__subscription_cells_equivalent(a, b) && provenance_matches;
}

bool jazz_node__node__CurrentRow__aggregate_payload_equivalent(const jazz_node_CurrentRow *a, const jazz_node_CurrentRow *b) {
    jazz_types_RowUuid ra = jazz_node__node__CurrentRow__row_uuid(a), rb = jazz_node__node__CurrentRow__row_uuid(b);
    return a->table == b->table && jz_uuid_eq(&ra, &rb) && a->deleted == b->deleted &&
           jazz_node__node__CurrentRow__subscription_cells_equivalent(a, b);
}

/* ---------------- MergeableCommit ---------------- */
bool jazz_node__node__value_needs_large_value_staging(const groove_Value *value) {
    switch (value->tag) {
    case GROOVE_V_STRING: return value->v.s.len > GROOVE_INLINE_VALUE_MAX_BYTES;
    case GROOVE_V_BYTES: return value->v.bytes.len > GROOVE_INLINE_VALUE_MAX_BYTES;
    case GROOVE_V_NULLABLE: return value->v.nullable && jazz_node__node__value_needs_large_value_staging(value->v.nullable);
    default: return false;
    }
}

bool jazz_node__node__MergeableCommit__needs_large_value_staging(const jazz_node_MergeableCommit *c) {
    void *k, *v;
    JZ_BTREE_FOR(&c->cells, k, v) if (jazz_node__node__value_needs_large_value_staging(v)) return true;
    return false;
}

void jazz_node__node__MergeableCommit__new(jazz_node_MergeableCommit *out, jz_str table, jazz_types_RowUuid row, u64 now_ms) {
    memset(out, 0, sizeof *out);
    out->table = table;
    out->row_uuid = row;
    out->branch = jazz_model__model__branch__BranchSelector__default();
    out->made_by.tag = JAZZ_AUTHOR_SYSTEM;
    out->now_ms = now_ms;
    jz_btree_init(&out->cells, &jzn_desc_str_value);
    jz_btree_init(&out->prepared_large_columns, &mod_desc_str_set);
}

void jazz_node__node__MergeableCommit__drop(void *p) {
    jazz_node_MergeableCommit *c = p;
    jz_str_drop(&c->table);
    if (c->branch) jazz_model__model__branch__BranchSelector__drop(c->branch);
    jz_btree_drop(&c->cells);
    if (c->has_authored_columns) jz_btree_drop(&c->authored_columns);
    if (c->has_user_metadata_json) jz_str_drop(&c->user_metadata_json);
    jz_btree_drop(&c->prepared_large_columns);
    jz_vec_drop(&c->staged_large_values, sizeof(groove_StagedLargeValueId), NULL);
}

void jazz_node__node__MergeableCommit__known_fresh_row(jazz_node_MergeableCommit *c) { c->known_fresh_row = true; }
void jazz_node__node__MergeableCommit__branch(jazz_node_MergeableCommit *c, jazz_model_BranchSelector *b) { c->branch = b; }
void jazz_node__node__MergeableCommit__made_by(jazz_node_MergeableCommit *c, jazz_types_AuthorSubject s) { c->made_by = s; }
void jazz_node__node__MergeableCommit__permission_subject(jazz_node_MergeableCommit *c, jazz_types_AuthorSubject s) { c->has_permission_subject = true; c->permission_subject = s; }

jazz_types_AuthorSubject jazz_node__node__MergeableCommit__effective_permission_subject(const jazz_node_MergeableCommit *c) {
    return c->has_permission_subject ? c->permission_subject : c->made_by;
}

void jazz_node__node__MergeableCommit__cells(jazz_node_MergeableCommit *c, jz_btree *cells /* moved: String -> Value */) {
    jz_btree_drop(&c->cells);
    jz_btree_init(&c->cells, &jzn_desc_str_value);
    jz_str k; groove_Value v;
    while (jz_btree_pop_first(cells, &k, &v)) jz_btree_insert(&c->cells, &k, &v, NULL);
    jz_btree_drop(cells);
}

void jazz_node__node__MergeableCommit__cell(jazz_node_MergeableCommit *c, jz_str column, groove_Value value) {
    jz_btree_insert(&c->cells, &column, &value, NULL);
}

void jazz_node__node__MergeableCommit__staged_large_cell(jazz_node_MergeableCommit *c, jz_str column, groove_StagedLargeValue staged, bool nullable) {
    groove_Value value = { GROOVE_V_LARGE };
    value.v.large = staged.value_ref;
    groove_Value cell = nullable ? v_nullable(&value) : value;
    jz_str k = jz_str_clone(&column);
    jz_btree_insert(&c->cells, &k, &cell, NULL);
    jz_btree_insert(&c->prepared_large_columns, &column, NULL, NULL);
    JZ_VEC_PUSH(c->staged_large_values, staged.id);
}

void jazz_node__node__MergeableCommit__verified_inherited_large_cells(jazz_node_MergeableCommit *c, const jz_btree *inherited) {
    jz_str *column; groove_Value *value;
    JZ_BTREE_FOR(inherited, column, value) {
        if (!jazz_node__node__value_contains_indirect_descriptor(value)) continue;
        const groove_Value *mine = jz_btree_get(&c->cells, column);
        if (mine && groove__records__values__Value__eq(mine, value)) {
            jz_str k = jz_str_clone(column);
            jz_btree_insert(&c->prepared_large_columns, &k, NULL, NULL);
        }
    }
}

void jazz_node__node__MergeableCommit__authored_columns(jazz_node_MergeableCommit *c, jz_btree columns) {
    if (c->has_authored_columns) jz_btree_drop(&c->authored_columns);
    c->has_authored_columns = true; c->authored_columns = columns;
}
void jazz_node__node__MergeableCommit__deletion(jazz_node_MergeableCommit *c, jazz_model_DeletionEvent d) { c->has_deletion = true; c->deletion = d; }
void jazz_node__node__MergeableCommit__user_metadata(jazz_node_MergeableCommit *c, jz_str json) {
    if (c->has_user_metadata_json) jz_str_drop(&c->user_metadata_json);
    c->has_user_metadata_json = true; c->user_metadata_json = json;
}

jz_err jazz_node__node__validate_mergeable_write_shape(bool cells_empty, bool deletion_present) {
    if (cells_empty == deletion_present) return JZ_OK;
    if (!cells_empty && deletion_present)
        return JZN_INVALID_MERGEABLE("content versions cannot also carry deletion-register events");
    return JZN_INVALID_MERGEABLE("mergeable commits must carry content cells or a deletion-register event");
}

jz_err jazz_node__node__MergeableCommit__validate(const jazz_node_MergeableCommit *c) {
    jazz_types_TxTime t;
    jz_err e = jazz_types__time__TxTime__from_physical_ms(c->now_ms, &t);
    if (JZ_IS_ERR(e)) {
        jz_err_drop(&e);
        return JZN_INVALID_MERGEABLE("commit now_ms exceeds packed HLC physical-millisecond range");
    }
    JZ_TRY(jazz_node__node__validate_mergeable_write_shape(c->cells.len == 0, c->has_deletion));
    jz_str *column; groove_Value *value;
    JZ_BTREE_FOR(&c->cells, column, value) {
        if (jazz_node__node__value_contains_indirect_descriptor(value) && !jz_btree_contains(&c->prepared_large_columns, column))
            return JZN_INVALID_MERGEABLE("callers must author logical scalar values, not physical large descriptors");
    }
    return JZ_OK;
}

bool jazz_node__node__value_contains_indirect_descriptor(const groove_Value *value) {
    switch (value->tag) {
    case GROOVE_V_LARGE: return true;
    case GROOVE_V_TUPLE:
    case GROOVE_V_ARRAY:
        for (u32 i = 0; i < value->v.vals.len; i++)
            if (jazz_node__node__value_contains_indirect_descriptor(&value->v.vals.ptr[i])) return true;
        return false;
    case GROOVE_V_NULLABLE: return value->v.nullable && jazz_node__node__value_contains_indirect_descriptor(value->v.nullable);
    case GROOVE_V_RECORD:
    case GROOVE_V_ENUM: {
        groove_ValueVec values = {0};
        groove_OwnedRecord *rec = value->tag == GROOVE_V_RECORD ? value->v.record : value->v.en.record;
        jz_err e = groove__records__OwnedRecord__to_values(rec, &values);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
        bool any = false;
        for (u32 i = 0; i < values.len && !any; i++) any = jazz_node__node__value_contains_indirect_descriptor(&values.ptr[i]);
        jz_vec_drop(&values, sizeof(groove_Value), value_drop_v);
        return any;
    }
    default: return false;
    }
}

typedef JZ_VEC(groove_LargeValueRef *) large_ref_vec;
void jazz_node__node__collect_indirect_descriptors(const groove_Value *value, void *descriptors_v) {
    large_ref_vec *descriptors = descriptors_v;
    switch (value->tag) {
    case GROOVE_V_LARGE: {
        for (u32 i = 0; i < descriptors->len; i++)
            if (groove__large_values__LargeValueRef__eq(descriptors->ptr[i], value->v.large)) return;
        JZ_VEC_PUSH(*descriptors, groove__large_values__LargeValueRef__clone(value->v.large));
        break;
    }
    case GROOVE_V_TUPLE:
    case GROOVE_V_ARRAY:
        for (u32 i = 0; i < value->v.vals.len; i++) jazz_node__node__collect_indirect_descriptors(&value->v.vals.ptr[i], descriptors);
        break;
    case GROOVE_V_NULLABLE:
        if (value->v.nullable) jazz_node__node__collect_indirect_descriptors(value->v.nullable, descriptors);
        break;
    case GROOVE_V_RECORD:
    case GROOVE_V_ENUM: {
        groove_ValueVec values = {0};
        groove_OwnedRecord *rec = value->tag == GROOVE_V_RECORD ? value->v.record : value->v.en.record;
        jz_err e = groove__records__OwnedRecord__to_values(rec, &values);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); break; }
        for (u32 i = 0; i < values.len; i++) jazz_node__node__collect_indirect_descriptors(&values.ptr[i], descriptors);
        jz_vec_drop(&values, sizeof(groove_Value), value_drop_v);
        break;
    }
    default: break;
    }
}

void jazz_node__node__version_indirect_descriptors(jazz_protocol_VersionRecord *const *versions, u32 n, void *out) {
    for (u32 i = 0; i < n; i++) {
        u32 cells = jazz_protocol__protocol__VersionRecord__application_cell_count(versions[i]);
        for (u32 position = 0; position < cells; position++) {
            groove_Value value;
            if (jazz_protocol__protocol__VersionRecord__cell_at(versions[i], position, &value)) {
                jazz_node__node__collect_indirect_descriptors(&value, out);
                groove__records__values__Value__drop(&value);
            }
        }
    }
}

/* ---------------- ViewUpdateParts / PublishedTransaction / PublicationOutcome ---------------- */
static void view_carrier_drop_v(void *p) { jazz_protocol__protocol__VersionCarrier__drop(*(jazz_protocol_VersionCarrier **)p); }
void jazz_node__node__ViewUpdateParts__drop(void *p) {
    jazz_node_ViewUpdateParts *u = p;
    if (u->wire_rows) { jazz_protocol__protocol__SupportingRowsUpdate__drop(u->wire_rows); jz_free(u->wire_rows); u->wire_rows = NULL; }
    jz_vec_drop(&u->version_carriers, sizeof(jazz_protocol_VersionCarrier *), view_carrier_drop_v);
    jz_free(u->peer_complete_tx_payload_refs.ptr);
    jz_vec_drop(&u->result_member_adds, sizeof(jazz_protocol_ResultMemberEntry), jazz_protocol__protocol__ResultMemberEntry__drop);
    jz_vec_drop(&u->result_member_removes, sizeof(jazz_protocol_ResultMemberEntry), jazz_protocol__protocol__ResultMemberEntry__drop);
}
const jazz_protocol_SupportingRow *jazz_node__node__ViewUpdateParts__supporting_adds(const jazz_node_ViewUpdateParts *p, u32 *n) {
    if (!p->wire_rows) { *n = 0; return NULL; }
    return jazz_protocol__protocol__SupportingRowsUpdate__added_rows(p->wire_rows, n);
}
const jazz_protocol_SupportingRow *jazz_node__node__ViewUpdateParts__supporting_removes(const jazz_node_ViewUpdateParts *p, u32 *n) {
    if (!p->wire_rows) { *n = 0; return NULL; }
    return jazz_protocol__protocol__SupportingRowsUpdate__removed_rows(p->wire_rows, n);
}

jazz_model_TxId jazz_node__node__PublishedTransaction__tx_id(const jazz_node_PublishedTransaction *p) { return p->tx_id; }
const jz_str *jazz_node__node__PublishedTransaction__changed_tables(const jazz_node_PublishedTransaction *p, u32 *n) {
    return groove__db__AppliedBatch__changed_tables(p->persistence, n);
}
groove_PersistedBatch *jazz_node__node__PublishedTransaction__persist(const jazz_node_PublishedTransaction *p) {
    groove_PersistedBatch *out;
    groove__db__AppliedBatch__persist(p->persistence, &out);
    return out;
}

void jazz_node__node__PublicationOutcome__append_outcome(jazz_node_PublicationOutcome *self, jazz_node_PublicationOutcome *other, usize elem) {
    jz_vec *sv = self->value, *ov = other->value;
    jz_vec_extend(sv, ov->ptr, ov->len, elem);
    ov->len = 0;
    jz_vec_extend(&self->publications, other->publications.ptr, other->publications.len, sizeof(jazz_node_PublishedTransaction));
    other->publications.len = 0;
    void *msg;
    while (jz_deque_pop_front(&other->post_settlement_work, &msg)) jz_deque_push_back(&self->post_settlement_work, &msg);
}

/* ---------------- PreparedQueryPlan ---------------- */
enum { JZN_PQP_GRAPH, JZN_PQP_PREPARED, JZN_PQP_PEER_MAINTAINED_MARKER };
typedef struct jazz_node_PreparedQueryPlan {
    u32 tag;
    groove_GraphBuilder *graph;
    groove_PreparedShapeId shape;
    void *params;   /* Vec<PreparedQueryParam> */
    void *output;   /* AppRowSchema */
} jazz_node_PreparedQueryPlan;
const void *jazz_node__node__PreparedQueryPlan__app_row_schema(const jazz_node_PreparedQueryPlan *p) {
    switch (p->tag) {
    case JZN_PQP_GRAPH:
    case JZN_PQP_PREPARED: return p->output;
    default: return NULL;
    }
}

/* ---------------- Error ---------------- */
bool jazz_node__node__is_unrepresentable_enum_projection(const jz_err *e) {
    return e->code == GROOVE_REC_ERR_INVALID_ENUM_DISCRIMINANT || e->code == GROOVE_REC_ERR_UNKNOWN_ENUM_TAG;
}

jz_err jazz_node__node__Error__missing_transaction(const jazz_model_TxId *tx) {
    jz_str d = {0}; jazz_model__tx__TxId__debug(&d, tx);
    jz_err e = jz_err_fmt(JZN_E_MISSING_TRANSACTION, "%.*s", (int)d.len, d.ptr);
    jz_str_drop(&d);
    return e;
}
jz_err jazz_node__node__Error__conflicting_commit_unit(const jazz_model_TxId *tx) {
    jz_str d = {0}; jazz_model__tx__TxId__debug(&d, tx);
    jz_err e = jz_err_fmt(JZN_E_CONFLICTING_COMMIT_UNIT, "%.*s", (int)d.len, d.ptr);
    jz_str_drop(&d);
    return e;
}

void jazz_node__node__Error__Display__fmt(const jz_err *e, jz_str *out) {
    const char *d = e->msg ? "" : "";
    int dl = e->msg ? (int)e->msg->len : 0;
    const char *dp = e->msg ? e->msg->ptr : d;
    switch (e->code) {
    case JZN_E_LARGE_VALUE_UPLOAD_CHUNK_UNAVAILABLE: jz_str_appendf(out, "large-value upstream upload cannot read local chunk (%.*s)", dl, dp); break;
    case JZN_E_LARGE_VALUE_INGRESS_RATE_LIMITED: jz_str_push_cstr(out, "large-value upload rate limit exceeded"); break;
    case JZN_E_LARGE_VALUE_STAGE_EXPIRED: jz_str_push_cstr(out, "large-value staging root expired; upload again"); break;
    case JZN_E_QUERY: jz_str_appendf(out, "%.*s", dl, dp); break;
    case JZN_E_QUERY_LOWERING: jz_str_appendf(out, "query lowering failed: %.*s", dl, dp); break;
    case JZN_E_QUERY_CAPABILITY: jz_str_appendf(out, "query capability unsupported: %.*s", dl, dp); break;
    case JZN_E_AUTHORIZATION_SUPPORT_MISSING_CLAIM: jz_str_appendf(out, "authorization support policy claim is not bound: %.*s", dl, dp); break;
    case JZN_E_POLICY_PROOF_CYCLE: jz_str_appendf(out, "PolicyProofCycle: %.*s", dl, dp); break;
    case JZN_E_TABLE_NOT_FOUND: jz_str_appendf(out, "table not found: %.*s", dl, dp); break;
    case JZN_E_UNSUPPORTED_COLUMN_TYPE: jz_str_appendf(out, "M1 only supports string user columns, got unsupported column: %.*s", dl, dp); break;
    case JZN_E_INVALID_MERGEABLE_COMMIT: jz_str_appendf(out, "invalid mergeable commit: %.*s", dl, dp); break;
    case JZN_E_UNADMITTED_WRITE_AUTHOR: jz_str_push_cstr(out, "durable writes require an admitted account author"); break;
    case JZN_E_INVALID_BRANCH_KEY: jz_str_appendf(out, "invalid branch key: %.*s", dl, dp); break;
    case JZN_E_UNSUPPORTED_EXCLUSIVE_READ: jz_str_appendf(out, "Reading %.*s is not supported in exclusive transactions yet", dl, dp); break;
    case JZN_E_TRANSACTION_CONFLICT: jz_str_push_cstr(out, "row visible parent changed since transaction write was staged"); break;
    case JZN_E_INVALID_STORED_VALUE: jz_str_appendf(out, "invalid stored value: %.*s", dl, dp); break;
    case JZN_E_INVALID_AUTHORITY_SOURCE_CLOSURE: jz_str_appendf(out, "invalid authority source closure for subscription %.*s", dl, dp); break;
    case JZN_E_MISSING_TRANSACTION: jz_str_appendf(out, "missing transaction: %.*s", dl, dp); break;
    case JZN_E_MALFORMED_VIEW_UPDATE: jz_str_appendf(out, "malformed view update: %.*s", dl, dp); break;
    case JZN_E_MAINTAINED_VIEW_MISSING_BUNDLE_WITNESS: jz_str_appendf(out, "maintained subscription view missing bundle witness: %.*s", dl, dp); break;
    case JZN_E_MISSING_OPEN_BATCH: jz_str_appendf(out, "missing open transaction: %.*s", dl, dp); break;
    case JZN_E_DUPLICATE_OPEN_BATCH: jz_str_appendf(out, "duplicate open batch id: %.*s", dl, dp); break;
    case JZN_E_OPEN_TRANSACTION_IDENTITY_MISMATCH: jz_str_push_cstr(out, "open transaction identity does not match its bound identity"); break;
    case JZN_E_NON_MONOTONE_STATE: jz_str_appendf(out, "non-monotone state update: %.*s", dl, dp); break;
    case JZN_E_CONFLICTING_COMMIT_UNIT: jz_str_appendf(out, "conflicting commit unit for transaction: %.*s", dl, dp); break;
    case JZN_E_CONFLICTING_FATE: jz_str_push_cstr(out, "conflicting fate transition"); break;
    case JZN_E_UNSUPPORTED_COMMIT_UNIT: jz_str_appendf(out, "unsupported commit unit: %.*s", dl, dp); break;
    case JZN_E_UNSUPPORTED_SYNC_MESSAGE: jz_str_appendf(out, "unsupported sync message: %.*s", dl, dp); break;
    case JZN_E_UNAUTHORIZED_CATALOGUE_UPDATE: jz_str_push_cstr(out, "unauthorized catalogue update"); break;
    case JZN_E_INVALID_CATALOGUE_UPDATE: jz_str_appendf(out, "invalid catalogue update: %.*s", dl, dp); break;
    case JZN_E_CATALOGUE_ACTIVATION_FAILED: jz_str_push_cstr(out, "catalogue activation failed; reopen required"); break;
    case JZN_E_CATALOGUE_UNINITIALIZED: jz_str_push_cstr(out, "catalogue bootstrap is not ready"); break;
    case JZN_E_HISTORICAL_READ_REQUIRES_SERVER: jz_str_push_cstr(out, "historical read requires server evaluation"); break;
    case JZN_E_AUTHORIZATION_DENIED: jz_str_push_cstr(out, "authorization denied"); break;
    case JZN_E_SUBSCRIPTION_CLOSED: jz_str_push_cstr(out, "prepared point-read subscription closed"); break;
    case JZN_E_MALFORMED_CURRENT_ROW: jz_str_appendf(out, "malformed current row in table %.*s", dl, dp); break;
    default: /* #[error(transparent)] */ jz_str_push(out, dp, (u32)dl); break;
    }
}

/* From<QueryError> for Error */
void jazz_model__query__QueryError__Display__fmt(const jazz_model_QueryError *e, jz_str *out);
jz_err jazz_node__node__Error__From_QueryError__from(jazz_model_QueryError *error) {
    jz_str s = {0};
    jazz_model__query__QueryError__Display__fmt(error, &s);
    jz_err e = jz_err_fmt(JZN_E_QUERY, "%.*s", (int)s.len, s.ptr);
    jz_str_drop(&s);
    return e;
}

/* ---------------- #[derive(Clone)] SchemaCatalogue / StagedSchemaLineage / PendingSchemaLineage ---------------- */
void jazz_node__node__StagedSchemaLineage__Clone__clone(void *dst, const void *src) {
    jazz_node_StagedSchemaLineage *d = dst;
    const jazz_node_StagedSchemaLineage *s = src;
    d->catalogue_seq = s->catalogue_seq;
    jazz_protocol__protocol__SchemaLineagePublication__clone(&d->publication, &s->publication);
    d->alias = s->alias;
    jazz_node__node__physical__SchemaPhysicalMapping__clone(&d->mapping, &s->mapping);
}
void jazz_node__node__StagedSchemaLineage__drop(void *p) {
    jazz_node_StagedSchemaLineage *l = p;
    jazz_protocol__protocol__SchemaLineagePublication__drop(&l->publication);
    jazz_node__node__physical__SchemaPhysicalMapping__drop(&l->mapping);
}
void jazz_node__node__PendingSchemaLineage__Clone__clone(void *dst, const void *src) {
    jazz_node_PendingSchemaLineage *d = dst;
    const jazz_node_PendingSchemaLineage *s = src;
    d->catalogue_seq = s->catalogue_seq;
    jazz_protocol__protocol__SchemaLineagePublication__clone(&d->publication, &s->publication);
}
void jazz_node__node__PendingSchemaLineage__drop(void *p) {
    jazz_protocol__protocol__SchemaLineagePublication__drop(&((jazz_node_PendingSchemaLineage *)p)->publication);
}
static void copy_pod(void *dst, const void *src, usize n) { memcpy(dst, src, n); }
static void k_uuid_clone(void *d, const void *s) { copy_pod(d, s, sizeof(jz_uuid)); }
static void k_u64_clone(void *d, const void *s) { copy_pod(d, s, sizeof(u64)); }
static void v_u64_clone(void *d, const void *s) { copy_pod(d, s, sizeof(u64)); }
static void v_schema_version_ptr_clone(void *d, const void *s) {
    jazz_protocol_SchemaVersion *c = JZ_NEW(jazz_protocol_SchemaVersion);
    jazz_protocol__protocol__SchemaVersion__clone(c, *(jazz_protocol_SchemaVersion *const *)s);
    *(jazz_protocol_SchemaVersion **)d = c;
}
static void v_lens_ptr_clone(void *d, const void *s) {
    jazz_protocol_MigrationLens *c = JZ_NEW(jazz_protocol_MigrationLens);
    jazz_protocol__protocol__MigrationLens__clone(c, *(jazz_protocol_MigrationLens *const *)s);
    *(jazz_protocol_MigrationLens **)d = c;
}
static void v_mapping_clone(void *d, const void *s) { jazz_node__node__physical__SchemaPhysicalMapping__clone(d, s); }

void jazz_node__node__SchemaCatalogue__Clone__clone(jazz_node_SchemaCatalogue *dst, const jazz_node_SchemaCatalogue *src) {
    *dst = *src;
    dst->schema = src->schema ? jazz_model__schema__JazzSchema__clone(src->schema) : NULL;
    jz_btree_clone(&dst->schema_version_aliases, &src->schema_version_aliases, k_uuid_clone, v_u64_clone);
    jz_btree_clone(&dst->catalogue_schemas, &src->catalogue_schemas, k_uuid_clone, v_schema_version_ptr_clone);
    jz_btree_clone(&dst->catalogue_lenses, &src->catalogue_lenses, k_uuid_clone, v_lens_ptr_clone);
    jz_btree_clone(&dst->physical_mappings, &src->physical_mappings, k_uuid_clone, v_mapping_clone);
    jz_btree_clone(&dst->physical_current_winner_projections, &src->physical_current_winner_projections, NULL, NULL);
    jz_btree_clone(&dst->staged_lineages, &src->staged_lineages, k_u64_clone, jazz_node__node__StagedSchemaLineage__Clone__clone);
    jz_btree_clone(&dst->pending_lineages, &src->pending_lineages, k_u64_clone, jazz_node__node__PendingSchemaLineage__Clone__clone);
    jz_btree_clone(&dst->active_lineages_by_target, &src->active_lineages_by_target, k_uuid_clone, jazz_node__node__StagedSchemaLineage__Clone__clone);
    jz_btree_clone(&dst->lens_path_cache, &src->lens_path_cache, NULL, NULL);
    jz_btree_clone(&dst->compiled_lens_cache, &src->compiled_lens_cache, NULL, NULL);
    jz_btree_clone(&dst->physical_write_plan_cache, &src->physical_write_plan_cache, NULL, NULL);
    dst->active_schema.compiled = jazz_model__schema__JazzSchema__clone(src->active_schema.compiled);
}

void jazz_node__node__SchemaCatalogue__drop(jazz_node_SchemaCatalogue *c) {
    jz_btree_drop(&c->schema_version_aliases);
    jz_btree_drop(&c->catalogue_schemas);
    jz_btree_drop(&c->catalogue_lenses);
    jz_btree_drop(&c->physical_mappings);
    jz_btree_drop(&c->physical_current_winner_projections);
    jz_btree_drop(&c->staged_lineages);
    jz_btree_drop(&c->pending_lineages);
    jz_btree_drop(&c->active_lineages_by_target);
    jz_btree_drop(&c->lens_path_cache);
    jz_btree_drop(&c->compiled_lens_cache);
    jz_btree_drop(&c->physical_write_plan_cache);
}
