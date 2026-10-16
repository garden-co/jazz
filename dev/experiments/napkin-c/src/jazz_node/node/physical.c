/* physical.c: jazz_node::node::physical module root (physical.rs) plus
 * physical/names.rs (both are part of the one Rust module via include!), and
 * the shared container descriptors / drop / clone glue of the module's types.
 * Rust: crates/jazz/layers/node/src/node/physical.rs, physical/names.rs */
#include "physical.h"
#include "codec.h"

/* ------------------------------------------------------------------ */
/* drop / clone glue (Rust derives Clone + drop_in_place glue)          */
/* ------------------------------------------------------------------ */
static void jzn_str_drop_v(void *p) { jz_str_drop((jz_str *)p); }
static void jzn_str_clone_v(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone((const jz_str *)src); }
void jzn_pod_vec_drop(void *v) { jz_vec_drop(v, 1, NULL); }
void jzn_str_vec_drop(void *v) { jz_vec_drop(v, sizeof(jz_str), jzn_str_drop_v); }
void jzn_str_vec_clone(void *dst, const void *src) { jz_vec_clone(dst, src, sizeof(jz_str), jzn_str_clone_v); }
static void jzn_strpodvec_map_drop(void *m) { jz_btree_drop((jz_btree *)m); }
static void jzn_table_mapping_drop_v(void *p) { jazz_node_TablePhysicalMapping_drop(p); }
static void jzn_schema_mapping_drop_v(void *p) { jazz_node_SchemaPhysicalMapping_drop(p); }
static void jzn_cwcp_drop_v(void *p) { jazz_node_CurrentWinnerCellProjection_drop(p); }

int jazz_node_GlobalScalarEnumCaseId_cmp(const void *a, const void *b, void *ctx) {
    /* impl_enum_case_identity!: equality and order use only the UUID */
    return jz_uuid_cmp(&((const jazz_node_GlobalScalarEnumCaseId *)a)->id, &((const jazz_node_GlobalScalarEnumCaseId *)b)->id);
}
int jazz_node_GlobalEnumCaseId_cmp(const void *a, const void *b, void *ctx) {
    return jz_uuid_cmp(&((const jazz_node_GlobalEnumCaseId *)a)->id, &((const jazz_node_GlobalEnumCaseId *)b)->id);
}
static int jzn_pathcase_cmp(const void *a, const void *b, void *ctx) {
    const jazz_node_PathCaseKey *l = a, *r = b;
    int c = jz_str_cmp(&l->path, &r->path);
    if (c) return c;
    return jazz_node_GlobalEnumCaseId_cmp(&l->identity, &r->identity, ctx);
}
static void jzn_pathcase_drop(void *p) { jz_str_drop(&((jazz_node_PathCaseKey *)p)->path); }

const jz_btree_desc jzn_desc_str_set = { sizeof(jz_str), 0, jz_cmp_str, jzn_str_drop_v, NULL };
const jz_btree_desc jzn_desc_str_u64 = { sizeof(jz_str), sizeof(u64), jz_cmp_str, jzn_str_drop_v, NULL };
const jz_btree_desc jzn_desc_u64_set = { sizeof(u64), 0, jz_cmp_u64, NULL, NULL };
const jz_btree_desc jzn_desc_u64_podvec = { sizeof(u64), sizeof(jz_vec), jz_cmp_u64, NULL, jzn_pod_vec_drop };
const jz_btree_desc jzn_desc_str_podvec = { sizeof(jz_str), sizeof(jz_vec), jz_cmp_str, jzn_str_drop_v, jzn_pod_vec_drop };
const jz_btree_desc jzn_desc_u64_strpodvec_map = { sizeof(u64), sizeof(jz_btree), jz_cmp_u64, NULL, jzn_strpodvec_map_drop };
const jz_btree_desc jzn_desc_str_table_mapping = { sizeof(jz_str), sizeof(jazz_node_TablePhysicalMapping), jz_cmp_str, jzn_str_drop_v, jzn_table_mapping_drop_v };
const jz_btree_desc jzn_desc_uuid_schema_mapping = { sizeof(jz_uuid), sizeof(jazz_node_SchemaPhysicalMapping), jz_cmp_uuid, NULL, jzn_schema_mapping_drop_v };
const jz_btree_desc jzn_desc_gse_set = { sizeof(jazz_node_GlobalScalarEnumCaseId), 0, jazz_node_GlobalScalarEnumCaseId_cmp, NULL, NULL };
const jz_btree_desc jzn_desc_ge_set = { sizeof(jazz_node_GlobalEnumCaseId), 0, jazz_node_GlobalEnumCaseId_cmp, NULL, NULL };
const jz_btree_desc jzn_desc_u32_uuid = { sizeof(u32), sizeof(jz_uuid), jz_cmp_u32, NULL, NULL };
const jz_btree_desc jzn_desc_u32_strvec = { sizeof(u32), sizeof(jz_vec), jz_cmp_u32, NULL, jzn_str_vec_drop };
const jz_btree_desc jzn_desc_str_cwcp = { sizeof(jz_str), sizeof(jazz_node_CurrentWinnerCellProjection), jz_cmp_str, jzn_str_drop_v, jzn_cwcp_drop_v };
const jz_btree_desc jzn_desc_str_str = { sizeof(jz_str), sizeof(jz_str), jz_cmp_str, jzn_str_drop_v, jzn_str_drop_v };
const jz_btree_desc jzn_desc_u64_str = { sizeof(u64), sizeof(jz_str), jz_cmp_u64, NULL, jzn_str_drop_v };
static void jzn_desc_noop(void *p) { (void)p; }
const jz_btree_desc jzn_desc_pathcase_desc = { sizeof(jazz_node_PathCaseKey), sizeof(groove_RecordDescriptor), jzn_pathcase_cmp, jzn_pathcase_drop, NULL };

static void jzn_pod_vec_clone_sized(void *dst, const void *src, usize elem) { jz_vec_clone(dst, src, elem, NULL); }
static void jzn_gse_vec_clone(void *dst, const void *src) { jzn_pod_vec_clone_sized(dst, src, sizeof(jazz_node_GlobalScalarEnumCaseId)); }
static void jzn_ge_vec_clone(void *dst, const void *src) { jzn_pod_vec_clone_sized(dst, src, sizeof(jazz_node_GlobalEnumCaseId)); }
static void jzn_u64_copy(void *dst, const void *src) { *(u64 *)dst = *(const u64 *)src; }
static void jzn_gse_map_clone(void *dst, const void *src) { jz_btree_clone(dst, src, jzn_str_clone_v, jzn_gse_vec_clone); }
static void jzn_ge_map_clone(void *dst, const void *src) { jz_btree_clone(dst, src, jzn_str_clone_v, jzn_ge_vec_clone); }

void jazz_node_PhysicalVariantCase_clone(void *dst, const void *src) {
    const jazz_node_PhysicalVariantCase *s = src;
    jazz_node_PhysicalVariantCase *d = dst;
    d->tag = s->tag;
    jz_btree_clone(&d->fields, &s->fields, jzn_str_clone_v, NULL);
}
void jazz_node_PhysicalVariantCase_drop(void *p) { jz_btree_drop(&((jazz_node_PhysicalVariantCase *)p)->fields); }

void jazz_node_TablePhysicalMapping_init(jazz_node_TablePhysicalMapping *m, jazz_types_PhysicalTableId id) {
    memset(m, 0, sizeof *m);
    m->table_id = id;
    jz_btree_init(&m->columns, &jzn_desc_str_u64);
    jz_btree_init(&m->scalar_enum_cases, &jzn_desc_u64_podvec);
    jz_btree_init(&m->payload_enum_cases, &jzn_desc_u64_podvec);
    jz_btree_init(&m->nested_scalar_enum_cases, &jzn_desc_u64_strpodvec_map);
    jz_btree_init(&m->nested_payload_enum_cases, &jzn_desc_u64_strpodvec_map);
}
void jazz_node_TablePhysicalMapping_clone(void *dst, const void *src) {
    const jazz_node_TablePhysicalMapping *s = src;
    jazz_node_TablePhysicalMapping *d = dst;
    d->table_id = s->table_id;
    jz_btree_clone(&d->columns, &s->columns, jzn_str_clone_v, jzn_u64_copy);
    jz_vec_clone(&d->variant_cases, &s->variant_cases, sizeof(jazz_node_PhysicalVariantCase), jazz_node_PhysicalVariantCase_clone);
    jz_btree_clone(&d->scalar_enum_cases, &s->scalar_enum_cases, jzn_u64_copy, jzn_gse_vec_clone);
    jz_btree_clone(&d->payload_enum_cases, &s->payload_enum_cases, jzn_u64_copy, jzn_ge_vec_clone);
    jz_btree_clone(&d->nested_scalar_enum_cases, &s->nested_scalar_enum_cases, jzn_u64_copy, jzn_gse_map_clone);
    jz_btree_clone(&d->nested_payload_enum_cases, &s->nested_payload_enum_cases, jzn_u64_copy, jzn_ge_map_clone);
}
void jazz_node_TablePhysicalMapping_drop(void *p) {
    jazz_node_TablePhysicalMapping *m = p;
    jz_btree_drop(&m->columns);
    jz_vec_drop(&m->variant_cases, sizeof(jazz_node_PhysicalVariantCase), jazz_node_PhysicalVariantCase_drop);
    jz_btree_drop(&m->scalar_enum_cases);
    jz_btree_drop(&m->payload_enum_cases);
    jz_btree_drop(&m->nested_scalar_enum_cases);
    jz_btree_drop(&m->nested_payload_enum_cases);
}
void jazz_node_SchemaPhysicalMapping_clone(void *dst, const void *src) {
    const jazz_node_SchemaPhysicalMapping *s = src;
    jazz_node_SchemaPhysicalMapping *d = dst;
    jazz_protocol_PhysicalIdentityManifest_clone(&d->identities, &s->identities);
    jz_btree_clone(&d->tables, &s->tables, jzn_str_clone_v, jazz_node_TablePhysicalMapping_clone);
}
void jazz_node_SchemaPhysicalMapping_drop(void *p) {
    jazz_node_SchemaPhysicalMapping *m = p;
    jazz_protocol_PhysicalIdentityManifest_drop(&m->identities);
    jz_btree_drop(&m->tables);
}

static bool jzn_pod_vec_eq(const jz_vec *a, const jz_vec *b, usize elem) {
    return a->len == b->len && (a->len == 0 || memcmp(a->ptr, b->ptr, (usize)a->len * elem) == 0);
}
static bool jzn_gse_vec_eq(const void *a, const void *b) { return jzn_pod_vec_eq(a, b, sizeof(jazz_node_GlobalScalarEnumCaseId)); }
static bool jzn_ge_vec_eq(const void *a, const void *b) { return jzn_pod_vec_eq(a, b, sizeof(jazz_node_GlobalEnumCaseId)); }
static bool jzn_gse_map_eq(const void *a, const void *b) { return jz_btree_eq(a, b, jzn_gse_vec_eq); }
static bool jzn_ge_map_eq(const void *a, const void *b) { return jz_btree_eq(a, b, jzn_ge_vec_eq); }
static bool jzn_u64_eq(const void *a, const void *b) { return *(const u64 *)a == *(const u64 *)b; }
static bool jzn_table_mapping_eq(const void *pa, const void *pb) {
    const jazz_node_TablePhysicalMapping *a = pa, *b = pb;
    if (a->table_id != b->table_id || !jz_btree_eq(&a->columns, &b->columns, jzn_u64_eq)) return false;
    if (a->variant_cases.len != b->variant_cases.len) return false;
    for (u32 i = 0; i < a->variant_cases.len; i++) {
        if (a->variant_cases.ptr[i].tag != b->variant_cases.ptr[i].tag ||
            !jz_btree_eq(&a->variant_cases.ptr[i].fields, &b->variant_cases.ptr[i].fields, NULL))
            return false;
    }
    return jz_btree_eq(&a->scalar_enum_cases, &b->scalar_enum_cases, jzn_gse_vec_eq) &&
           jz_btree_eq(&a->payload_enum_cases, &b->payload_enum_cases, jzn_ge_vec_eq) &&
           jz_btree_eq(&a->nested_scalar_enum_cases, &b->nested_scalar_enum_cases, jzn_gse_map_eq) &&
           jz_btree_eq(&a->nested_payload_enum_cases, &b->nested_payload_enum_cases, jzn_ge_map_eq);
}
bool jazz_node_SchemaPhysicalMapping_eq(const jazz_node_SchemaPhysicalMapping *a, const jazz_node_SchemaPhysicalMapping *b) {
    return jazz_protocol_PhysicalIdentityManifest_eq(&a->identities, &b->identities) &&
           jz_btree_eq(&a->tables, &b->tables, jzn_table_mapping_eq);
}

void jazz_node_CurrentWinnerCellProjection_clone(void *dst, const void *src) {
    const jazz_node_CurrentWinnerCellProjection *s = src;
    jazz_node_CurrentWinnerCellProjection *d = dst;
    memset(d, 0, sizeof *d);
    d->tag = s->tag;
    switch (s->tag) {
    case JZN_CWCP_FIELD:
        d->name = jz_str_clone(&s->name);
        d->column_id = s->column_id;
        groove_ValueType_clone(&d->column_type, &s->column_type);
        break;
    case JZN_CWCP_LITERAL: groove_Value_clone(&d->literal, &s->literal); break;
    case JZN_CWCP_NULL: break;
    }
}
void jazz_node_CurrentWinnerCellProjection_drop(void *p) {
    jazz_node_CurrentWinnerCellProjection *c = p;
    switch (c->tag) {
    case JZN_CWCP_FIELD: jz_str_drop(&c->name); groove_ValueType_drop(&c->column_type); break;
    case JZN_CWCP_LITERAL: groove_Value_drop(&c->literal); break;
    case JZN_CWCP_NULL: break;
    }
}

void jazz_node_PreparedPhysicalWritePlan_drop(void *p) {
    jazz_node_PreparedPhysicalWritePlan *plan = p;
    jz_str_drop(&plan->storage_table);
    jz_rc_release(plan->source_table);
    jz_vec_drop(&plan->write_fields, sizeof(jazz_node_PhysicalWriteField), NULL);
    jz_vec_drop(&plan->enum_remaps, sizeof(jazz_node_EnumOccurrenceRemaps), groove_RecursiveEnumRemaps_drop);
}

/* ------------------------------------------------------------------ */
/* physical/names.rs                                                    */
/* ------------------------------------------------------------------ */

/* A schema-agnostic physical current-row target used only until the
 * Global/Ahead winner has been selected. */
jz_str jazz_node__node__physical__physical_current_winner_projection_target(jazz_types_PhysicalTableId table_id,
                                                                           const jz_str *physical_fields, u32 n) {
    jz_str joined = {0};
    for (u32 i = 0; i < n; i++) {
        if (i) jz_str_push_char(&joined, '_');
        jz_str_push(&joined, physical_fields[i].ptr, physical_fields[i].len);
    }
    jz_str out = jz_format("physical_%llu_current_winner_%.*s", (unsigned long long)table_id, JZN_S(joined));
    jz_str_drop(&joined);
    return out;
}

/* A query-local current-source target whose unneeded enum cells are typed nulls. */
jz_str jazz_node__node__physical__physical_current_projection_target_for_enum_columns(jazz_types_SchemaVersionAlias schema_alias,
                                                                                     const jz_str *logical_table,
                                                                                     const jz_btree *enum_columns) {
    jz_str base = jazz_node__node__physical__physical_current_projection_target(schema_alias, logical_table);
    jz_str suffix = {0};
    u64 *column;
    void *unused;
    bool first = true;
    JZ_BTREE_FOR(enum_columns, column, unused) {
        if (!first) jz_str_push_char(&suffix, '_');
        first = false;
        jz_str_appendf(&suffix, "%llu", (unsigned long long)*column);
    }
    jz_str out = jz_format("%.*s_enum_fields_%.*s", JZN_S(base), JZN_S(suffix));
    jz_str_drop(&base);
    jz_str_drop(&suffix);
    return out;
}

/* The history storage schema owns physical enum registry identities; a
 * query-local history source crosses into the reader's authored descriptor. */
groove_RecordDescriptor jazz_node__node__physical__authored_history_projection_descriptor(const jazz_model_TableSchema *table) {
    groove_TableSchema history = jazz_model__schema__TableSchema__history_storage_table(table);
    groove_RecordDescriptor record = groove__schema__TableSchema__record_schema(&history);
    const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(record);
    groove_NamedValueTypeVec out = {0};
    jz_vec_reserve(&out, fields->len, sizeof(groove_NamedValueType));
    for (u32 i = 0; i < fields->len; i++) {
        const groove_DescriptorField *field = &fields->ptr[i];
        if (!field->has_name) jz_panic("Jazz history storage fields are named");
        groove_NamedValueType entry;
        entry.name = jz_str_clone(&field->name);
        bool found = false;
        for (u32 c = 0; c < table->columns.len; c++) {
            const jazz_model_ColumnSchema *column = &table->columns.ptr[c];
            jz_str user = jazz_node__node__query_engine__user_column_field(&column->name);
            bool eq = jz_str_eq(&user, &entry.name);
            jz_str_drop(&user);
            if (eq) {
                groove_ValueType inner;
                groove_ValueType_clone(&inner, &column->column_type);
                entry.value_type = groove_vt_box(GROOVE_VT_NULLABLE, inner);
                found = true;
                break;
            }
        }
        if (!found) groove_ValueType_clone(&entry.value_type, &field->value_type);
        JZ_VEC_PUSH(out, entry);
    }
    groove_TableSchema_drop(&history);
    return groove__records__RecordDescriptor__new(out);
}
