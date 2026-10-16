/* Port of crates/jazz/layers/node/src/node/query_eval/unavailable_inputs.rs
 * Context-scoped mutable inputs for locally unavailable current rows. */
#include "jazz_node/node/node_qe_b.h"

const jz_btree_desc jazz_node_qeb_UUID_SET_DESC = { sizeof(jz_uuid), 0, jz_cmp_uuid, NULL, NULL };

void jazz_node_LocalUnavailableInput_init(jazz_node_LocalUnavailableInput *i)
{
    memset(i, 0, sizeof *i);
    jz_btree_init(&i->rows, &jazz_node_qeb_UUID_SET_DESC);
}

static groove_RecordDescriptor *jazz_node__node__query_eval__unavailable_inputs__descriptor(void)
{
    groove_FieldSpec f = { "row_uuid", { GVT_UUID, NULL } };
    return groove__records__descriptor__RecordDescriptor__new(&f, 1);
}

static bool jazz_node__node__query_eval__unavailable_inputs__policy_binding(const jazz_engine_PolicyContext *policy, jazz_protocol_PolicyBindingKey *out)
{
    switch (policy->tag) {
    case JZ_PC_IDENTITY: {
        jz_btree claims;
        jz_btree_clone(&claims, &policy->claims, jazz_node_qeb_str_clone, jazz_node_qeb_value_clone);
        out->identity = policy->permission_subject;
        out->canonical_claims = jazz_protocol__protocol__CanonicalPolicyClaims__new(claims);
        return true;
    }
    case JZ_PC_SYSTEM:
    case JZ_PC_AUTHORIZATION_SUBPLAN:
    default:
        return false;
    }
}

bool jazz_node__node__query_eval__unavailable_inputs__local_unavailable_policy_binding(
    const jazz_engine_QueryProgramRequest *request, jazz_protocol_PolicyBindingKey *out)
{
    if (request->authorization_mode == JZ_QAM_TRUSTED_SERVING) return false;
    return jazz_node__node__query_eval__unavailable_inputs__policy_binding(&request->policy, out);
}

bool jazz_node__node__query_eval__unavailable_inputs__is_current_app_source(const jazz_engine_SourceExpr *source)
{
    switch (source->tag) {
    case JZ_SE_VISIBLE_CURRENT:
        return source->data.tag == JZ_DS_CURRENT;
    case JZ_SE_SETTLED_BINDING_VIEW:
        return source->current_default;
    case JZ_SE_WITH_OVERLAYS:
        if (source->overlays.entries.len == 1 && source->overlays.entries.ptr[0].tag == JZ_OVL_PENDING_LOCAL)
            return jazz_node__node__query_eval__unavailable_inputs__is_current_app_source(source->input);
        return false;
    default:
        return false;
    }
}

/* Capture the effective app identity without formatting raw claims. */
bool jazz_node__node__query_eval__unavailable_inputs__NodeState__local_read_policy_binding(
    const jazz_node_NodeState *self, jazz_types_AuthorSubject identity, jazz_protocol_PolicyBindingKey *out)
{
    jazz_engine_PolicyContext ctx;
    jazz_node__node__query_eval__authorization__NodeState__query_program_policy_context(self, identity, &ctx);
    bool r = jazz_node__node__query_eval__unavailable_inputs__policy_binding(&ctx, out);
    jazz_engine_PolicyContext_drop(&ctx);
    return r;
}

static jazz_node_ScopeTableKey scope_key(const jazz_protocol_PolicyBindingKey *scope, jazz_types_GlobalPhysicalTableId table)
{
    jazz_node_ScopeTableKey k;
    k.scope = *scope; /* borrowed shallow copy for lookups */
    k.table = table;
    return k;
}
static jazz_node_ScopeTableKey scope_key_owned(const jazz_protocol_PolicyBindingKey *scope, jazz_types_GlobalPhysicalTableId table)
{
    jazz_node_ScopeTableKey k;
    jazz_protocol_PolicyBindingKey_clone(&k.scope, scope);
    k.table = table;
    return k;
}

bool jazz_node__node__query_eval__unavailable_inputs__NodeState__is_local_row_unavailable(
    const jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *scope, jazz_types_GlobalPhysicalTableId table, jazz_types_RowUuid row)
{
    jazz_node_ScopeTableKey k = scope_key(scope, table);
    const jazz_node_LocalUnavailableInput *input = jz_btree_get(&self->query.local_unavailable_inputs, &k);
    return input && jz_btree_contains(&input->rows, &row);
}

jz_err jazz_node__node__query_eval__unavailable_inputs__NodeState__local_availability_table_id(
    const jazz_node_NodeState *self, jazz_types_SchemaVersionId schema, const jz_str *table, jazz_types_GlobalPhysicalTableId *out)
{
    const jazz_node_SchemaPhysicalMapping *mapping = jz_btree_get(&self->catalogue.physical_mappings, &schema);
    const jazz_node_PhysicalTableIdentity *ident = mapping ? jz_btree_get(&mapping->identities.tables, table) : NULL;
    if (!ident) return JZN_INVALID_STORED("local availability source has no global table identity");
    *out = ident->id;
    return JZ_OK;
}

/* Runtime-only setup primitive. */
jz_err jazz_node__node__query_eval__unavailable_inputs__NodeState__set_local_row_unavailable(
    jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *scope, const jz_str *table, jazz_types_RowUuid row, bool unavailable, bool *out)
{
    if (jazz_types_AuthorSubject_is_system(scope->identity)) { *out = false; return JZ_OK; }
    jazz_node_UnavailableRowChange c;
    JZ_TRY(jazz_node__node__query_eval__unavailable_inputs__NodeState__local_availability_table_id(self, self->catalogue.local_schema_version_id, table, &c.table));
    c.row = row;
    c.unavailable = unavailable;
    return jazz_node__node__query_eval__unavailable_inputs__NodeState__update_local_unavailable_rows(self, scope, &c, 1, out);
}

typedef JZ_VEC(jz_bytes) BytesVec;
typedef struct AddsRemoves { BytesVec adds; BytesVec removes; } AddsRemoves;
static void bytes_drop(void *b) { jz_bytes_drop(b); }
static void adds_removes_drop(void *p)
{
    AddsRemoves *a = p;
    jz_vec_drop(&a->adds, sizeof(jz_bytes), bytes_drop);
    jz_vec_drop(&a->removes, sizeof(jz_bytes), bytes_drop);
}
static const jz_btree_desc CHANGES_DESC = { sizeof(jz_uuid), sizeof(AddsRemoves), jz_cmp_uuid, NULL, adds_removes_drop };

jz_err jazz_node__node__query_eval__unavailable_inputs__NodeState__update_local_unavailable_rows(
    jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *scope, const jazz_node_UnavailableRowChange *rows, u32 n, bool *out)
{
    jz_btree changes;
    jz_btree_init(&changes, &CHANGES_DESC);
    bool changed = false;
    jz_err e = JZ_OK;
    for (u32 i = 0; i < n; i++) {
        const jazz_node_UnavailableRowChange *r = &rows[i];
        jazz_node_ScopeTableKey k = scope_key(scope, r->table);
        const jazz_node_LocalUnavailableInput *input = jz_btree_get(&self->query.local_unavailable_inputs, &k);
        bool prior = input && jz_btree_contains(&input->rows, &r->row);
        if (prior == r->unavailable) continue;
        changed = true;
        if (input && input->has_id && input->runtime_token == self->groove_runtime_token) {
            groove_RecordDescriptor *d = jazz_node__node__query_eval__unavailable_inputs__descriptor();
            groove_Value v = gv_uuid(r->row);
            jz_bytes record;
            e = groove__records__descriptor__RecordDescriptor__create(d, &v, 1, &record);
            groove_RecordDescriptor_drop(d);
            if (JZ_IS_ERR(e)) { jz_btree_drop(&changes); return e; }
            bool ins;
            AddsRemoves *ar = jz_btree_entry(&changes, &r->table, &ins);
            if (r->unavailable) jz_vec_push(&ar->adds, &record, sizeof record);
            else jz_vec_push(&ar->removes, &record, sizeof record);
        }
    }
    JZ_VEC(groove_InputSourceDelta) deltas = {0};
    jz_uuid table; AddsRemoves ar;
    while (jz_btree_pop_first(&changes, &table, &ar)) {
        jazz_node_ScopeTableKey k = scope_key(scope, table);
        const jazz_node_LocalUnavailableInput *input = jz_btree_get(&self->query.local_unavailable_inputs, &k);
        if (!input) jz_panic("key not found in BTreeMap");
        if (!input->has_id) jz_panic("live input");
        groove_InputSourceDelta d;
        d.id = input->id;
        d.descriptor = jazz_node__node__query_eval__unavailable_inputs__descriptor();
        memcpy(&d.adds, &ar.adds, sizeof d.adds);
        memcpy(&d.removes, &ar.removes, sizeof d.removes);
        jz_vec_push(&deltas, &d, sizeof d);
    }
    jz_btree_drop(&changes);
    if (deltas.len) e = groove__db__query__Database__apply_input_source_deltas(self->database, deltas.ptr, deltas.len);
    jz_vec_drop(&deltas, sizeof(groove_InputSourceDelta), groove_InputSourceDelta_drop);
    if (JZ_IS_ERR(e)) return e; /* map_err(Error::Groove) */
    for (u32 i = 0; i < n; i++) {
        const jazz_node_UnavailableRowChange *r = &rows[i];
        jazz_node_ScopeTableKey k = scope_key_owned(scope, r->table);
        bool ins;
        jazz_node_LocalUnavailableInput *input = jz_btree_entry(&self->query.local_unavailable_inputs, &k, &ins);
        if (ins) jazz_node_LocalUnavailableInput_init(input);
        else jazz_protocol_PolicyBindingKey_drop(&k.scope);
        if (r->unavailable) jz_btree_insert(&input->rows, &r->row, NULL, NULL);
        else jz_btree_remove(&input->rows, &r->row, NULL, NULL);
    }
    *out = changed;
    return JZ_OK;
}

static int scope_ref_cmp(const void *a, const void *b, void *ctx)
{
    return jazz_protocol_PolicyBindingKey_cmp(*(const void *const *)a, *(const void *const *)b, ctx);
}
static const jz_btree_desc SCOPE_REF_SET = { sizeof(void *), 0, scope_ref_cmp, NULL, NULL };

jz_err jazz_node__node__query_eval__unavailable_inputs__NodeState__require_local_availability_context_capacity(
    const jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *scope)
{
    jz_btree live_scopes;
    jz_btree_init(&live_scopes, &SCOPE_REF_SET);
    jazz_node_ScopeTableKey *k; jazz_node_LocalUnavailableInput *input;
    JZ_BTREE_FOR(&self->query.local_unavailable_inputs, k, input) {
        if (input->has_id && input->runtime_token == self->groove_runtime_token) {
            const jazz_protocol_PolicyBindingKey *p = &k->scope;
            jz_btree_insert(&live_scopes, &p, NULL, NULL);
        }
    }
    jazz_protocol_PolicyBindingKey *ak; void *av;
    JZ_BTREE_FOR(&self->query.local_availability_authorities, ak, av) {
        const jazz_protocol_PolicyBindingKey *p = ak;
        jz_btree_insert(&live_scopes, &p, NULL, NULL);
    }
    bool contains = jz_btree_contains(&live_scopes, &scope);
    u32 len = live_scopes.len;
    jz_btree_drop(&live_scopes);
    if (!contains && len >= JZ_MAX_AUTHORIZATION_SCOPES)
        return jazz_node__node__Error_query_capability(jz_str_from(
            "local availability context capacity reached; retire inactive scope graphs before retrying"));
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__unavailable_inputs__NodeState__local_unavailable_input(
    jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *scope, jazz_types_GlobalPhysicalTableId table, groove_InputSourceId *out)
{
    jazz_node_ScopeTableKey key = scope_key(scope, table);
    const jazz_node_LocalUnavailableInput *input = jz_btree_get(&self->query.local_unavailable_inputs, &key);
    if (input && input->runtime_token == self->groove_runtime_token && input->has_id) {
        *out = input->id;
        return JZ_OK;
    }
    JZ_TRY(jazz_node__node__query_eval__unavailable_inputs__NodeState__require_local_availability_context_capacity(self, scope));
    BytesVec records = {0};
    if (input) {
        jz_uuid *row; void *unused;
        JZ_BTREE_FOR(&input->rows, row, unused) {
            groove_RecordDescriptor *d = jazz_node__node__query_eval__unavailable_inputs__descriptor();
            groove_Value v = gv_uuid(*row);
            jz_bytes rec;
            jz_err e = groove__records__descriptor__RecordDescriptor__create(d, &v, 1, &rec);
            groove_RecordDescriptor_drop(d);
            if (JZ_IS_ERR(e)) { jz_vec_drop(&records, sizeof(jz_bytes), bytes_drop); return e; }
            jz_vec_push(&records, &rec, sizeof rec);
        }
    }
    groove_InputSourceId id = groove__db__query__Database__allocate_input_source(self->database, jazz_node__node__query_eval__unavailable_inputs__descriptor());
    if (records.len) {
        groove_InputSourceReplacement r;
        r.id = id;
        r.descriptor = jazz_node__node__query_eval__unavailable_inputs__descriptor();
        memcpy(&r.records, &records, sizeof r.records);
        jz_err e = groove__db__query__Database__replace_input_sources(self->database, &r, 1);
        groove_InputSourceReplacement_drop(&r);
        if (JZ_IS_ERR(e)) return e;
    } else {
        jz_free(records.ptr);
    }
    jazz_node_ScopeTableKey owned = scope_key_owned(scope, table);
    bool ins;
    jazz_node_LocalUnavailableInput *slot = jz_btree_entry(&self->query.local_unavailable_inputs, &owned, &ins);
    if (ins) jazz_node_LocalUnavailableInput_init(slot);
    else jazz_protocol_PolicyBindingKey_drop(&owned.scope);
    slot->has_id = true;
    slot->id = id;
    slot->runtime_token = self->groove_runtime_token;
    *out = id;
    return JZ_OK;
}

/* Called only after this scope's query/subscription owners have stopped. */
jz_err jazz_node__node__query_eval__unavailable_inputs__NodeState__retire_local_availability_scope_inputs(
    jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *scope)
{
    JZ_VEC(groove_InputSourceId) ids = {0};
    jazz_node_ScopeTableKey *k; jazz_node_LocalUnavailableInput *input;
    JZ_BTREE_FOR(&self->query.local_unavailable_inputs, k, input) {
        if (jazz_protocol_PolicyBindingKey_eq(&k->scope, scope) && input->runtime_token == self->groove_runtime_token && input->has_id)
            jz_vec_push(&ids, &input->id, sizeof input->id);
    }
    if (ids.len) {
        jz_err e = groove__db__query__Database__retire_input_sources(self->database, ids.ptr, ids.len);
        jz_free(ids.ptr);
        if (JZ_IS_ERR(e)) return e;
    }
    JZ_BTREE_FOR(&self->query.local_unavailable_inputs, k, input) {
        if (jazz_protocol_PolicyBindingKey_eq(&k->scope, scope)) input->has_id = false;
    }
    jz_btree_clear(&self->query.query_shape_cache);
    jz_btree_clear(&self->query.compiled_query_program_cache);
    jazz_engine__query_engine__QueryProgramTemplateCache__clear(self->query.query_program_templates);
    { u32 el = self->query.supported_query_program_requests.elem;
      jz_deque_drop(&self->query.supported_query_program_requests, jazz_node_SupportedQueryProgram_drop);
      jz_deque_init(&self->query.supported_query_program_requests, el); }
    jz_btree_remove(&self->query.local_availability_authorities, scope, NULL, NULL);
    return JZ_OK;
}

static void rename_left_fields(const char *const *fields, u32 n, groove_ProjectField *out)
{
    for (u32 i = 0; i < n; i++) out[i] = groove__ivm__graph__ProjectField__renamed(jazz_engine__query_engine__fields__left_field(fields[i]), fields[i]);
}

jz_err jazz_node__node__query_eval__unavailable_inputs__NodeState__exclude_local_unavailable_graph(
    jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *scope, jazz_types_SchemaVersionId schema,
    const jazz_engine_SourceRequest *source, groove_GraphBuilder *graph, bool pending_ahead, groove_GraphBuilder **out)
{
    jazz_types_GlobalPhysicalTableId table;
    jz_err e = jazz_node__node__query_eval__unavailable_inputs__NodeState__local_availability_table_id(self, schema, &source->source.table, &table);
    if (JZ_IS_ERR(e)) { groove_GraphBuilder_drop(graph); return e; }
    groove_InputSourceId id;
    e = jazz_node__node__query_eval__unavailable_inputs__NodeState__local_unavailable_input(self, scope, table, &id);
    if (JZ_IS_ERR(e)) { groove_GraphBuilder_drop(graph); return e; }
    groove_GraphBuilder *unavailable = groove__ivm__graph__GraphBuilder__input_source(id, jazz_node__node__query_eval__unavailable_inputs__descriptor());
    static const char *const row_uuid[] = { "row_uuid" };
    if (pending_ahead) {
        /* Ahead can retain Core-confirmed versions until cleanup. Keep exact version keys. */
        static const char *const fields[] = { "row_uuid", "tx_time", "tx_node_id" };
        static const char *const tx_keys[] = { "tx_time", "tx_node_id" };
        static const char *const tx_cols[] = { "time", "node_id" };
        groove_PredicateExpr *parts[2];
        parts[0] = groove__ivm__graph__PredicateExpr__eq("fate", gv_enum_tag((u8)JZ_FATE_ACCEPTED));
        parts[1] = groove__ivm__graph__PredicateExpr__eq("durability", gv_enum_tag(jazz_model__tx__DurabilityTier__discriminant(JZ_DURABILITY_GLOBAL)));
        groove_PredicateExpr *pred = groove__ivm__graph__PredicateExpr__canonicalize(groove__ivm__graph__PredicateExpr__and(parts, 2));
        groove_GraphBuilder *txs = groove__ivm__graph__GraphBuilder__project(
            groove__ivm__graph__GraphBuilder__filter(groove__ivm__graph__GraphBuilder__table("jazz_transactions"), pred), tx_cols, 2);
        groove_ProjectField pf[3];
        rename_left_fields(fields, 3, pf);
        groove_GraphBuilder *settled = groove__ivm__graph__GraphBuilder__project_fields(
            groove__ivm__graph__GraphBuilder__join(groove_GraphBuilder_clone(graph), txs, tx_keys, 2, tx_cols, 2), pf, 3);
        rename_left_fields(fields, 3, pf);
        groove_GraphBuilder *blocked = groove__ivm__graph__GraphBuilder__project_fields(
            groove__ivm__graph__GraphBuilder__join(settled, unavailable, row_uuid, 1, row_uuid, 1), pf, 3);
        *out = groove__ivm__graph__GraphBuilder__anti_join(graph, blocked, fields, 3, fields, 3);
        return JZ_OK;
    }
    *out = groove__ivm__graph__GraphBuilder__anti_join(graph, unavailable, row_uuid, 1, row_uuid, 1);
    return JZ_OK;
}
