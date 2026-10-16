/* graph.c: port of crates/groove/src/ivm/graph.rs.
 * GraphBuilder IR (+ derived Clone/PartialEq/Hash/Debug), NodeDescriptor
 * identity and validation, and the deduplicating IvmGraph. */
#include "jz_rt.h"
#include "../_ext_groove-ivm-db.h"
#include "op_types.h"
#include "graph.h"

/* ======================================================================
 * shared set/map descriptors
 * ====================================================================== */
static u64 nodeid_hash_v(const void *k) { return jz_hash_u64(*(const u64 *)k); }
static bool nodeid_eq_v(const void *a, const void *b) { return *(const u64 *)a == *(const u64 *)b; }
const jz_hmap_desc groove__ivm__NODESET_DESC = { sizeof(u64), 0, nodeid_hash_v, nodeid_eq_v, NULL, NULL };
static void nodeset_drop_v(void *p) { jz_hmap_drop((jz_hmap *)p); }
static u64 str_hash_v(const void *k) { return jz_str_hash((const jz_str *)k); }
static bool str_eq_v(const void *a, const void *b) { return jz_str_eq((const jz_str *)a, (const jz_str *)b); }
static void graph_node_drop_v(void *p) { groove__ivm__graph__GraphNode__drop((groove_GraphNode *)p); }
static const jz_hmap_desc NODES_DESC = { sizeof(u64), sizeof(groove_GraphNode), nodeid_hash_v, nodeid_eq_v, NULL, graph_node_drop_v };
static const jz_hmap_desc STR_SOURCES_DESC = { sizeof(jz_str), sizeof(jz_hmap), str_hash_v, str_eq_v, groove__ivm__jz_str_drop_v, nodeset_drop_v };
static const jz_hmap_desc BINDING_SOURCES_DESC = { sizeof(groove_BindingSourceKey), sizeof(jz_hmap),
    groove__ivm__op_types__BindingSourceKey__hash_v, groove__ivm__op_types__BindingSourceKey__eq_v,
    groove__ivm__op_types__BindingSourceKey__drop_v, nodeset_drop_v };

/* ======================================================================
 * FieldRef
 * ====================================================================== */
groove_FieldRef groove__ivm__graph__FieldRef__name(jz_str name) {
    groove_FieldRef f = { GROOVE_FIELDREF_NAME, name, 0 };
    return f;
}
groove_FieldRef groove__ivm__graph__FieldRef__resolved(u32 index) {
    groove_FieldRef f; memset(&f, 0, sizeof f);
    f.tag = GROOVE_FIELDREF_RESOLVED; f.index = index;
    return f;
}
groove_FieldRef groove__ivm__graph__FieldRef__stored_name(jz_str name) {
    groove_FieldRef f = { GROOVE_FIELDREF_STORED_NAME, name, 0 };
    return f;
}
jz_str groove__ivm__graph__FieldRef__display_name(const groove_FieldRef *f) {
    switch (f->tag) {
    case GROOVE_FIELDREF_NAME: return jz_str_clone(&f->name);
    case GROOVE_FIELDREF_STORED_NAME: return jz_format("stored:%.*s", (int)f->name.len, f->name.ptr);
    default: return jz_format("#%u", (unsigned)f->index);
    }
}

/* ======================================================================
 * vec helpers for builder payloads
 * ====================================================================== */
static void fieldref_clone_v(void *d, const void *s) { groove__ivm__graph__FieldRef__Clone__clone(d, s); }
static void fieldref_drop_v(void *p) { groove__ivm__graph__FieldRef__drop(p); }
void groove__ivm__graph__fieldrefvec_clone(groove_FieldRefVec *dst, const groove_FieldRefVec *src) { jz_vec_clone(dst, src, sizeof(groove_FieldRef), fieldref_clone_v); }
void groove__ivm__graph__fieldrefvec_drop(groove_FieldRefVec *v) { jz_vec_drop(v, sizeof(groove_FieldRef), fieldref_drop_v); }
/* `cols.into_iter().map(FieldRef::name).collect()` */
void groove__ivm__graph__fieldrefvec_from_names(groove_FieldRefVec *out, const jz_str *names, u32 n) {
    memset(out, 0, sizeof *out);
    jz_vec_reserve(out, n, sizeof(groove_FieldRef));
    for (u32 i = 0; i < n; i++) JZ_VEC_PUSH(*out, groove__ivm__graph__FieldRef__name(jz_str_clone(&names[i])));
}
static bool fieldrefvec_eq(const groove_FieldRefVec *a, const groove_FieldRefVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!groove__ivm__graph__FieldRef__PartialEq__eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
static void fieldrefvec_hash(const groove_FieldRefVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) groove__ivm__graph__FieldRef__Hash__hash(&v->ptr[i], h);
}
static void fieldrefvec_debug(jz_str *out, const groove_FieldRefVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) { if (i) jz_str_push_cstr(out, ", "); groove__ivm__graph__FieldRef__Debug__fmt(out, &v->ptr[i]); }
    jz_str_push_char(out, ']');
}
static void opt_fieldref_clone(bool has, const groove_FieldRef *s, groove_FieldRef *d) {
    if (has) groove__ivm__graph__FieldRef__Clone__clone(d, s); else memset(d, 0, sizeof *d);
}
static bool opt_fieldref_eq(bool ha, const groove_FieldRef *a, bool hb, const groove_FieldRef *b) {
    return ha == hb && (!ha || groove__ivm__graph__FieldRef__PartialEq__eq(a, b));
}
static void opt_fieldref_hash(groove_Hasher *h, bool has, const groove_FieldRef *f) {
    groove_hash_discr(h, has);
    if (has) groove__ivm__graph__FieldRef__Hash__hash(f, h);
}
static void opt_fieldref_debug(jz_str *out, bool has, const groove_FieldRef *f) {
    if (!has) { jz_str_push_cstr(out, "None"); return; }
    jz_str_push_cstr(out, "Some(");
    groove__ivm__graph__FieldRef__Debug__fmt(out, f);
    jz_str_push_char(out, ')');
}

/* TopByOrder */
static void topby_order_clone_v(void *d, const void *s) {
    const groove_TopByOrder *src = s; groove_TopByOrder *dst = d;
    groove__ivm__graph__FieldRef__Clone__clone(&dst->field, &src->field);
    dst->direction = src->direction;
}
static void topby_order_drop_v(void *p) { groove__ivm__graph__FieldRef__drop(&((groove_TopByOrder *)p)->field); }
static void topby_orders_clone(groove_TopByOrderVec *d, const groove_TopByOrderVec *s) { jz_vec_clone(d, s, sizeof(groove_TopByOrder), topby_order_clone_v); }
static void topby_orders_drop(groove_TopByOrderVec *v) { jz_vec_drop(v, sizeof(groove_TopByOrder), topby_order_drop_v); }
static bool topby_orders_eq(const groove_TopByOrderVec *a, const groove_TopByOrderVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!groove__ivm__graph__FieldRef__PartialEq__eq(&a->ptr[i].field, &b->ptr[i].field) || a->ptr[i].direction != b->ptr[i].direction) return false;
    return true;
}
static void topby_orders_hash(const groove_TopByOrderVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) {
        groove__ivm__graph__FieldRef__Hash__hash(&v->ptr[i].field, h);
        groove_hash_discr(h, v->ptr[i].direction);
    }
}
static void topby_orders_debug(jz_str *out, const groove_TopByOrderVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        groove_DebugStruct d = groove__fmt__debug_struct(out, "TopByOrder");
        groove__fmt__DebugStruct__field(&d, "field"); groove__ivm__graph__FieldRef__Debug__fmt(out, &v->ptr[i].field);
        groove__fmt__DebugStruct__field(&d, "direction"); groove__ivm__op_types__dbg_dir(out, v->ptr[i].direction);
        groove__fmt__DebugStruct__finish(&d);
    }
    jz_str_push_char(out, ']');
}

/* CollectByField */
static void cbfield_clone_v(void *d, const void *s) {
    const groove_CollectByField *src = s; groove_CollectByField *dst = d;
    groove__ivm__graph__FieldRef__Clone__clone(&dst->field, &src->field);
    dst->output_name = jz_str_clone(&src->output_name);
    dst->unwrap_nullable = src->unwrap_nullable;
}
static void cbfield_drop_v(void *p) {
    groove_CollectByField *f = p;
    groove__ivm__graph__FieldRef__drop(&f->field);
    jz_str_drop(&f->output_name);
}
static void cbfields_clone(groove_CollectByFieldVec *d, const groove_CollectByFieldVec *s) { jz_vec_clone(d, s, sizeof(groove_CollectByField), cbfield_clone_v); }
static void cbfields_drop(groove_CollectByFieldVec *v) { jz_vec_drop(v, sizeof(groove_CollectByField), cbfield_drop_v); }
static bool cbfields_eq(const groove_CollectByFieldVec *a, const groove_CollectByFieldVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) {
        if (!groove__ivm__graph__FieldRef__PartialEq__eq(&a->ptr[i].field, &b->ptr[i].field)) return false;
        if (!jz_str_eq(&a->ptr[i].output_name, &b->ptr[i].output_name)) return false;
        if (a->ptr[i].unwrap_nullable != b->ptr[i].unwrap_nullable) return false;
    }
    return true;
}
static void cbfields_hash(const groove_CollectByFieldVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) {
        groove__ivm__graph__FieldRef__Hash__hash(&v->ptr[i].field, h);
        groove_hash_str(h, &v->ptr[i].output_name);
        groove_hash_bool(h, v->ptr[i].unwrap_nullable);
    }
}
static void cbfields_debug(jz_str *out, const groove_CollectByFieldVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        groove_DebugStruct d = groove__fmt__debug_struct(out, "CollectByField");
        groove__fmt__DebugStruct__field(&d, "field"); groove__ivm__graph__FieldRef__Debug__fmt(out, &v->ptr[i].field);
        groove__fmt__DebugStruct__field(&d, "output_name"); groove__fmt__debug_str(out, &v->ptr[i].output_name);
        groove__fmt__DebugStruct__field(&d, "unwrap_nullable"); groove__ivm__op_types__dbg_bool(out, v->ptr[i].unwrap_nullable);
        groove__fmt__DebugStruct__finish(&d);
    }
    jz_str_push_char(out, ']');
}

/* ProjectField */
void groove__ivm__graph__ProjectField__Clone__clone(groove_ProjectField *dst, const groove_ProjectField *src) {
    groove__ivm__graph__ProjectExpr__Clone__clone(&dst->expression, &src->expression);
    dst->output_name = jz_str_clone(&src->output_name);
    groove__records__FieldIdentity__Clone__clone(&dst->output_identity, &src->output_identity);
}
void groove__ivm__graph__ProjectField__drop(groove_ProjectField *v) {
    groove__ivm__graph__ProjectExpr__drop(&v->expression);
    jz_str_drop(&v->output_name);
    groove__records__FieldIdentity__drop(&v->output_identity);
}
bool groove__ivm__graph__ProjectField__PartialEq__eq(const groove_ProjectField *a, const groove_ProjectField *b) {
    return groove__ivm__graph__ProjectExpr__PartialEq__eq(&a->expression, &b->expression) &&
           jz_str_eq(&a->output_name, &b->output_name) &&
           groove__records__FieldIdentity__PartialEq__eq(&a->output_identity, &b->output_identity);
}
static void project_field_clone_v(void *d, const void *s) { groove__ivm__graph__ProjectField__Clone__clone(d, s); }
static void project_field_drop_v(void *p) { groove__ivm__graph__ProjectField__drop(p); }
static bool project_fields_eq(const groove_ProjectFieldVec *a, const groove_ProjectFieldVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!groove__ivm__graph__ProjectField__PartialEq__eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
static void project_fields_hash(const groove_ProjectFieldVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) {
        groove__ivm__graph__ProjectExpr__Hash__hash(&v->ptr[i].expression, h);
        groove_hash_str(h, &v->ptr[i].output_name);
        groove__records__FieldIdentity__Hash__hash(&v->ptr[i].output_identity, h);
    }
}
static void project_fields_debug(jz_str *out, const groove_ProjectFieldVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        groove_DebugStruct d = groove__fmt__debug_struct(out, "ProjectField");
        groove__fmt__DebugStruct__field(&d, "expression"); groove__ivm__graph__ProjectExpr__Debug__fmt(out, &v->ptr[i].expression);
        groove__fmt__DebugStruct__field(&d, "output_name"); groove__fmt__debug_str(out, &v->ptr[i].output_name);
        groove__fmt__DebugStruct__field(&d, "output_identity"); groove__records__FieldIdentity__Debug__fmt(out, &v->ptr[i].output_identity);
        groove__fmt__DebugStruct__finish(&d);
    }
    jz_str_push_char(out, ']');
}

/* CollectBySlotBuilder (recursive) */
static void slot_builder_clone_v(void *d, const void *s) { groove__ivm__graph__CollectBySlotBuilder__Clone__clone(d, s); }
static void slot_builder_drop_v(void *p) { groove__ivm__graph__CollectBySlotBuilder__drop(p); }
void groove__ivm__graph__CollectBySlotBuilder__Clone__clone(groove_CollectBySlotBuilder *dst, const groove_CollectBySlotBuilder *src) {
    groove__ivm__graph__fieldrefvec_clone(&dst->group_cols, &src->group_cols);
    groove__ivm__graph__fieldrefvec_clone(&dst->owner_key_cols, &src->owner_key_cols);
    cbfields_clone(&dst->child_fields, &src->child_fields);
    dst->collection_field = jz_str_clone(&src->collection_field);
    jz_vec_clone(&dst->slots, &src->slots, sizeof(groove_CollectBySlotBuilder), slot_builder_clone_v);
    topby_orders_clone(&dst->order_cols, &src->order_cols);
    groove__ivm__graph__fieldrefvec_clone(&dst->tie_cols, &src->tie_cols);
    dst->has_presence_col = src->has_presence_col;
    opt_fieldref_clone(src->has_presence_col, &src->presence_col, &dst->presence_col);
    dst->has_reference_array_col = src->has_reference_array_col;
    opt_fieldref_clone(src->has_reference_array_col, &src->reference_array_col, &dst->reference_array_col);
    dst->reference_order = src->reference_order;
    dst->offset = src->offset;
    dst->limit = src->limit;
}
void groove__ivm__graph__CollectBySlotBuilder__drop(groove_CollectBySlotBuilder *v) {
    groove__ivm__graph__fieldrefvec_drop(&v->group_cols);
    groove__ivm__graph__fieldrefvec_drop(&v->owner_key_cols);
    cbfields_drop(&v->child_fields);
    jz_str_drop(&v->collection_field);
    jz_vec_drop(&v->slots, sizeof(groove_CollectBySlotBuilder), slot_builder_drop_v);
    topby_orders_drop(&v->order_cols);
    groove__ivm__graph__fieldrefvec_drop(&v->tie_cols);
    if (v->has_presence_col) groove__ivm__graph__FieldRef__drop(&v->presence_col);
    if (v->has_reference_array_col) groove__ivm__graph__FieldRef__drop(&v->reference_array_col);
}
static bool slot_builders_eq(const groove_CollectBySlotBuilderVec *a, const groove_CollectBySlotBuilderVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!groove__ivm__graph__CollectBySlotBuilder__PartialEq__eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
bool groove__ivm__graph__CollectBySlotBuilder__PartialEq__eq(const groove_CollectBySlotBuilder *a, const groove_CollectBySlotBuilder *b) {
    return fieldrefvec_eq(&a->group_cols, &b->group_cols) && fieldrefvec_eq(&a->owner_key_cols, &b->owner_key_cols) &&
           cbfields_eq(&a->child_fields, &b->child_fields) && jz_str_eq(&a->collection_field, &b->collection_field) &&
           slot_builders_eq(&a->slots, &b->slots) && topby_orders_eq(&a->order_cols, &b->order_cols) &&
           fieldrefvec_eq(&a->tie_cols, &b->tie_cols) &&
           opt_fieldref_eq(a->has_presence_col, &a->presence_col, b->has_presence_col, &b->presence_col) &&
           opt_fieldref_eq(a->has_reference_array_col, &a->reference_array_col, b->has_reference_array_col, &b->reference_array_col) &&
           a->reference_order == b->reference_order && a->offset == b->offset &&
           groove__ivm__op_types__limit_eq(&a->limit, &b->limit);
}
static void slot_builder_hash(const groove_CollectBySlotBuilder *v, groove_Hasher *h);
static void slot_builders_hash(const groove_CollectBySlotBuilderVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) slot_builder_hash(&v->ptr[i], h);
}
static void slot_builder_hash(const groove_CollectBySlotBuilder *v, groove_Hasher *h) {
    fieldrefvec_hash(&v->group_cols, h);
    fieldrefvec_hash(&v->owner_key_cols, h);
    cbfields_hash(&v->child_fields, h);
    groove_hash_str(h, &v->collection_field);
    slot_builders_hash(&v->slots, h);
    topby_orders_hash(&v->order_cols, h);
    fieldrefvec_hash(&v->tie_cols, h);
    opt_fieldref_hash(h, v->has_presence_col, &v->presence_col);
    opt_fieldref_hash(h, v->has_reference_array_col, &v->reference_array_col);
    groove_hash_bool(h, v->reference_order);
    groove_hash_u64(h, v->offset);
    groove__ivm__op_types__limit_hash(h, &v->limit);
}
static void slot_builder_debug(jz_str *out, const groove_CollectBySlotBuilder *v);
static void slot_builders_debug(jz_str *out, const groove_CollectBySlotBuilderVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) { if (i) jz_str_push_cstr(out, ", "); slot_builder_debug(out, &v->ptr[i]); }
    jz_str_push_char(out, ']');
}
static void slot_builder_debug(jz_str *out, const groove_CollectBySlotBuilder *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "CollectBySlotBuilder");
    groove__fmt__DebugStruct__field(&d, "group_cols"); fieldrefvec_debug(out, &v->group_cols);
    groove__fmt__DebugStruct__field(&d, "owner_key_cols"); fieldrefvec_debug(out, &v->owner_key_cols);
    groove__fmt__DebugStruct__field(&d, "child_fields"); cbfields_debug(out, &v->child_fields);
    groove__fmt__DebugStruct__field(&d, "collection_field"); groove__fmt__debug_str(out, &v->collection_field);
    groove__fmt__DebugStruct__field(&d, "slots"); slot_builders_debug(out, &v->slots);
    groove__fmt__DebugStruct__field(&d, "order_cols"); topby_orders_debug(out, &v->order_cols);
    groove__fmt__DebugStruct__field(&d, "tie_cols"); fieldrefvec_debug(out, &v->tie_cols);
    groove__fmt__DebugStruct__field(&d, "presence_col"); opt_fieldref_debug(out, v->has_presence_col, &v->presence_col);
    groove__fmt__DebugStruct__field(&d, "reference_array_col"); opt_fieldref_debug(out, v->has_reference_array_col, &v->reference_array_col);
    groove__fmt__DebugStruct__field(&d, "reference_order"); groove__ivm__op_types__dbg_bool(out, v->reference_order);
    groove__fmt__DebugStruct__field(&d, "offset"); jz_str_appendf(out, "%llu", (unsigned long long)v->offset);
    groove__fmt__DebugStruct__field(&d, "limit"); groove__ivm__op_types__TopByLimit__Debug__fmt(out, &v->limit);
    groove__fmt__DebugStruct__finish(&d);
}

/* CollectByBuilder */
void groove__ivm__graph__CollectByBuilder__Clone__clone(groove_CollectByBuilder *dst, const groove_CollectByBuilder *src) {
    dst->mode = src->mode;
    groove__ivm__graph__fieldrefvec_clone(&dst->group_cols, &src->group_cols);
    cbfields_clone(&dst->parent_fields, &src->parent_fields);
    cbfields_clone(&dst->child_fields, &src->child_fields);
    dst->collection_field = jz_str_clone(&src->collection_field);
    jz_vec_clone(&dst->slots, &src->slots, sizeof(groove_CollectBySlotBuilder), slot_builder_clone_v);
    cbfields_clone(&dst->tuple_fields, &src->tuple_fields);
    groove__ivm__graph__fieldrefvec_clone(&dst->occurrence_id_cols, &src->occurrence_id_cols);
    topby_orders_clone(&dst->order_cols, &src->order_cols);
    groove__ivm__graph__fieldrefvec_clone(&dst->tie_cols, &src->tie_cols);
    dst->offset = src->offset;
    dst->limit = src->limit;
}
void groove__ivm__graph__CollectByBuilder__drop(groove_CollectByBuilder *v) {
    groove__ivm__graph__fieldrefvec_drop(&v->group_cols);
    cbfields_drop(&v->parent_fields);
    cbfields_drop(&v->child_fields);
    jz_str_drop(&v->collection_field);
    jz_vec_drop(&v->slots, sizeof(groove_CollectBySlotBuilder), slot_builder_drop_v);
    cbfields_drop(&v->tuple_fields);
    groove__ivm__graph__fieldrefvec_drop(&v->occurrence_id_cols);
    topby_orders_drop(&v->order_cols);
    groove__ivm__graph__fieldrefvec_drop(&v->tie_cols);
}
bool groove__ivm__graph__CollectByBuilder__PartialEq__eq(const groove_CollectByBuilder *a, const groove_CollectByBuilder *b) {
    return a->mode == b->mode && fieldrefvec_eq(&a->group_cols, &b->group_cols) &&
           cbfields_eq(&a->parent_fields, &b->parent_fields) && cbfields_eq(&a->child_fields, &b->child_fields) &&
           jz_str_eq(&a->collection_field, &b->collection_field) && slot_builders_eq(&a->slots, &b->slots) &&
           cbfields_eq(&a->tuple_fields, &b->tuple_fields) && fieldrefvec_eq(&a->occurrence_id_cols, &b->occurrence_id_cols) &&
           topby_orders_eq(&a->order_cols, &b->order_cols) && fieldrefvec_eq(&a->tie_cols, &b->tie_cols) &&
           a->offset == b->offset && groove__ivm__op_types__limit_eq(&a->limit, &b->limit);
}
static void collect_builder_hash(const groove_CollectByBuilder *v, groove_Hasher *h) {
    groove_hash_discr(h, v->mode);
    fieldrefvec_hash(&v->group_cols, h);
    cbfields_hash(&v->parent_fields, h);
    cbfields_hash(&v->child_fields, h);
    groove_hash_str(h, &v->collection_field);
    slot_builders_hash(&v->slots, h);
    cbfields_hash(&v->tuple_fields, h);
    fieldrefvec_hash(&v->occurrence_id_cols, h);
    topby_orders_hash(&v->order_cols, h);
    fieldrefvec_hash(&v->tie_cols, h);
    groove_hash_u64(h, v->offset);
    groove__ivm__op_types__limit_hash(h, &v->limit);
}
static void collect_mode_debug(jz_str *out, u8 mode) {
    jz_str_push_cstr(out, mode == GROOVE_COLLECT_MODE_COLLECT ? "Collect" : mode == GROOVE_COLLECT_MODE_ROOT ? "Root" : "Expand");
}
static void collect_builder_debug(jz_str *out, const groove_CollectByBuilder *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "CollectByBuilder");
    groove__fmt__DebugStruct__field(&d, "mode"); collect_mode_debug(out, v->mode);
    groove__fmt__DebugStruct__field(&d, "group_cols"); fieldrefvec_debug(out, &v->group_cols);
    groove__fmt__DebugStruct__field(&d, "parent_fields"); cbfields_debug(out, &v->parent_fields);
    groove__fmt__DebugStruct__field(&d, "child_fields"); cbfields_debug(out, &v->child_fields);
    groove__fmt__DebugStruct__field(&d, "collection_field"); groove__fmt__debug_str(out, &v->collection_field);
    groove__fmt__DebugStruct__field(&d, "slots"); slot_builders_debug(out, &v->slots);
    groove__fmt__DebugStruct__field(&d, "tuple_fields"); cbfields_debug(out, &v->tuple_fields);
    groove__fmt__DebugStruct__field(&d, "occurrence_id_cols"); fieldrefvec_debug(out, &v->occurrence_id_cols);
    groove__fmt__DebugStruct__field(&d, "order_cols"); topby_orders_debug(out, &v->order_cols);
    groove__fmt__DebugStruct__field(&d, "tie_cols"); fieldrefvec_debug(out, &v->tie_cols);
    groove__fmt__DebugStruct__field(&d, "offset"); jz_str_appendf(out, "%llu", (unsigned long long)v->offset);
    groove__fmt__DebugStruct__field(&d, "limit"); groove__ivm__op_types__TopByLimit__Debug__fmt(out, &v->limit);
    groove__fmt__DebugStruct__finish(&d);
}

/* Vec<PredicateExpr> / Vec<AggregateExpr> / Vec<(String, StaticScanSpec)> */
static void predvec_drop(groove_PredicateVec *v) { jz_vec_drop(v, sizeof(groove_PredicateExpr), groove__ivm__op_types__PredicateExpr__drop_v); }
static bool predvec_eq(const groove_PredicateVec *a, const groove_PredicateVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!groove__ivm__op_types__PredicateExpr__PartialEq__eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
static void predvec_hash(const groove_PredicateVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) groove__ivm__op_types__PredicateExpr__Hash__hash(&v->ptr[i], h);
}
static void predvec_debug(jz_str *out, const groove_PredicateVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) { if (i) jz_str_push_cstr(out, ", "); groove__ivm__op_types__PredicateExpr__Debug__fmt(out, &v->ptr[i]); }
    jz_str_push_char(out, ']');
}
static bool aggvec_eq(const groove_AggregateExprVec *a, const groove_AggregateExprVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!groove__ivm__op_types__AggregateExpr__PartialEq__eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
static void aggvec_hash(const groove_AggregateExprVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) groove__ivm__op_types__AggregateExpr__Hash__hash(&v->ptr[i], h);
}
static void aggvec_debug(jz_str *out, const groove_AggregateExprVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) { if (i) jz_str_push_cstr(out, ", "); groove__ivm__op_types__AggregateExpr__Debug__fmt(out, &v->ptr[i]); }
    jz_str_push_char(out, ']');
}
static bool intersections_eq(const groove_ScanIntersectionVec *a, const groove_ScanIntersectionVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!jz_str_eq(&a->ptr[i].index, &b->ptr[i].index) || !groove__ivm__op_types__StaticScanSpec__PartialEq__eq(&a->ptr[i].scan, &b->ptr[i].scan)) return false;
    return true;
}
static void intersections_hash(const groove_ScanIntersectionVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) {
        groove_hash_str(h, &v->ptr[i].index);
        groove__ivm__op_types__StaticScanSpec__Hash__hash(&v->ptr[i].scan, h);
    }
}
static void intersections_debug(jz_str *out, const groove_ScanIntersectionVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        jz_str_push_char(out, '(');
        groove__fmt__debug_str(out, &v->ptr[i].index);
        jz_str_push_cstr(out, ", ");
        groove__ivm__op_types__StaticScanSpec__Debug__fmt(out, &v->ptr[i].scan);
        jz_str_push_char(out, ')');
    }
    jz_str_push_char(out, ']');
}
static void bytesvec_debug(jz_str *out, const groove_BytesVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) { if (i) jz_str_push_cstr(out, ", "); groove__fmt__debug_bytes(out, v->ptr[i].ptr, v->ptr[i].len); }
    jz_str_push_char(out, ']');
}
static void scalar_args_hash(const groove_ScalarArgSlice *s, groove_Hasher *h) {
    groove_hash_usize(h, s->len);
    for (u32 i = 0; i < s->len; i++) groove__ivm__template__arguments__TemplateScalarArgument__Hash__hash(&s->items[i], h);
}
static bool scalar_args_eq(const groove_ScalarArgSlice *a, const groove_ScalarArgSlice *b) {
    if (a == b) return true;
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!groove__ivm__template__arguments__TemplateScalarArgument__PartialEq__eq(&a->items[i], &b->items[i])) return false;
    return true;
}
static void input_source_id_debug(jz_str *out, const groove_InputSourceId *id) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "InputSourceId");
    groove__fmt__DebugStruct__field(&d, "runtime_namespace"); jz_str_appendf(out, "%llu", (unsigned long long)id->runtime_namespace);
    groove__fmt__DebugStruct__field(&d, "local"); jz_str_appendf(out, "%llu", (unsigned long long)id->local);
    groove__fmt__DebugStruct__finish(&d);
}

/* ======================================================================
 * Arc<GraphBuilder>
 * ====================================================================== */
void groove__ivm__graph__GraphBuilder__drop_v(void *v) { groove__ivm__graph__GraphBuilder__drop((groove_GraphBuilder *)v); }
groove_GraphArc groove__ivm__graph__arc_new(groove_GraphBuilder value) {
    groove_GraphArc a = jz_rc_new(sizeof(groove_GraphBuilder), groove__ivm__graph__GraphBuilder__drop_v);
    *a = value;
    return a;
}
groove_GraphArc groove__ivm__graph__arc_clone(groove_GraphArc a) { return (groove_GraphArc)jz_rc_clone(a); }
void groove__ivm__graph__arc_release(groove_GraphArc a) { if (a) jz_rc_release(a); }
static void arc_clone_v(void *d, const void *s) { *(groove_GraphArc *)d = groove__ivm__graph__arc_clone(*(groove_GraphArc const *)s); }
static void arc_release_v(void *p) { groove__ivm__graph__arc_release(*(groove_GraphArc *)p); }
void groove__ivm__graph__GraphArcVec__drop(groove_GraphArcVec *v) { jz_vec_drop(v, sizeof(groove_GraphArc), arc_release_v); }
static void arcvec_clone(groove_GraphArcVec *d, const groove_GraphArcVec *s) { jz_vec_clone(d, s, sizeof(groove_GraphArc), arc_clone_v); }
/* <Arc<T: Eq> as PartialEq>::eq: pointer fast path, then structural. */
static bool arc_eq(groove_GraphArc a, groove_GraphArc b) {
    return a == b || groove__ivm__graph__GraphBuilder__PartialEq__eq(a, b);
}
static bool opt_arc_eq(groove_GraphArc a, groove_GraphArc b) {
    if (!a || !b) return a == b;
    return arc_eq(a, b);
}
static bool arcvec_eq(const groove_GraphArcVec *a, const groove_GraphArcVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!arc_eq(a->ptr[i], b->ptr[i])) return false;
    return true;
}
static void arcvec_hash(const groove_GraphArcVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) groove__ivm__graph__GraphBuilder__Hash__hash(v->ptr[i], h);
}
static void arcvec_debug(jz_str *out, const groove_GraphArcVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) { if (i) jz_str_push_cstr(out, ", "); groove__ivm__graph__GraphBuilder__Debug__fmt(out, v->ptr[i]); }
    jz_str_push_char(out, ']');
}
static void opt_arc_debug(jz_str *out, groove_GraphArc a) {
    if (!a) { jz_str_push_cstr(out, "None"); return; }
    jz_str_push_cstr(out, "Some(");
    groove__ivm__graph__GraphBuilder__Debug__fmt(out, a);
    jz_str_push_char(out, ')');
}

/* ======================================================================
 * GraphBuilder: derived Clone
 * ====================================================================== */
void groove__ivm__graph__GraphBuilder__Clone__clone(groove_GraphBuilder *dst, const groove_GraphBuilder *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    switch (src->tag) {
    case GROOVE_GB_TYPED_TEMPLATE:
        dst->u.typed_template.program = jz_rc_clone(src->u.typed_template.program);
        arcvec_clone(&dst->u.typed_template.inputs, &src->u.typed_template.inputs);
        groove__ivm__op_types__predvec_clone(&dst->u.typed_template.predicates, &src->u.typed_template.predicates);
        dst->u.typed_template.scalars = jz_rc_clone(src->u.typed_template.scalars);
        break;
    case GROOVE_GB_TEMPLATE_INPUT:
        dst->u.template_input.slot = src->u.template_input.slot;
        dst->u.template_input.output = src->u.template_input.output;
        dst->u.template_input.input = src->u.template_input.input ? groove__ivm__graph__arc_clone(src->u.template_input.input) : NULL;
        break;
    case GROOVE_GB_TABLE:
        dst->u.table.table = jz_str_clone(&src->u.table.table);
        dst->u.table.has_scan = src->u.table.has_scan;
        groove__ivm__op_types__scan_opt_clone(src->u.table.has_scan, &src->u.table.scan, &dst->u.table.scan);
        dst->u.table.has_variant_projection = src->u.table.has_variant_projection;
        groove__ivm__op_types__opt_str_clone(src->u.table.has_variant_projection, &src->u.table.variant_projection, &dst->u.table.variant_projection);
        break;
    case GROOVE_GB_INLINE_RECORDS:
        dst->u.inline_records.output = src->u.inline_records.output;
        groove__ivm__bytesvec_clone(&dst->u.inline_records.records, &src->u.inline_records.records);
        break;
    case GROOVE_GB_INPUT_SOURCE:
        dst->u.input_source = src->u.input_source;
        break;
    case GROOVE_GB_INDEX:
        dst->u.index.table = jz_str_clone(&src->u.index.table);
        dst->u.index.index = jz_str_clone(&src->u.index.index);
        dst->u.index.has_scan = src->u.index.has_scan;
        groove__ivm__op_types__scan_opt_clone(src->u.index.has_scan, &src->u.index.scan, &dst->u.index.scan);
        jz_vec_clone(&dst->u.index.intersections, &src->u.index.intersections, sizeof(groove_ScanIntersection), groove__ivm__op_types__intersection_clone_v);
        dst->u.index.has_candidate_filter = src->u.index.has_candidate_filter;
        if (src->u.index.has_candidate_filter)
            groove__ivm__graph__IndexCandidateFilter__Clone__clone(&dst->u.index.candidate_filter, &src->u.index.candidate_filter);
        dst->u.index.has_row_projection = src->u.index.has_row_projection;
        groove__ivm__op_types__opt_str_clone(src->u.index.has_row_projection, &src->u.index.row_projection, &dst->u.index.row_projection);
        break;
    case GROOVE_GB_FRONTIER_SOURCE:
        dst->u.frontier_source.binding = jz_str_clone(&src->u.frontier_source.binding);
        dst->u.frontier_source.output = src->u.frontier_source.output;
        break;
    case GROOVE_GB_BINDING_SOURCE:
        dst->u.binding_source.shape = jz_str_clone(&src->u.binding_source.shape);
        dst->u.binding_source.output = src->u.binding_source.output;
        break;
    case GROOVE_GB_RECURSIVE:
        dst->u.recursive.seed = groove__ivm__graph__arc_clone(src->u.recursive.seed);
        dst->u.recursive.step = groove__ivm__graph__arc_clone(src->u.recursive.step);
        dst->u.recursive.step_witness = src->u.recursive.step_witness ? groove__ivm__graph__arc_clone(src->u.recursive.step_witness) : NULL;
        dst->u.recursive.frontier = jz_str_clone(&src->u.recursive.frontier);
        dst->u.recursive.max_iters = src->u.recursive.max_iters;
        dst->u.recursive.truncate_at_max_iters = src->u.recursive.truncate_at_max_iters;
        break;
    case GROOVE_GB_RECURSIVE_STEP_WITNESS:
        dst->u.recursive_step_witness.recursive = groove__ivm__graph__arc_clone(src->u.recursive_step_witness.recursive);
        break;
    case GROOVE_GB_FILTER:
        dst->u.filter.input = groove__ivm__graph__arc_clone(src->u.filter.input);
        groove__ivm__op_types__PredicateExpr__Clone__clone(&dst->u.filter.predicate, &src->u.filter.predicate);
        dst->u.filter.comparison = src->u.filter.comparison;
        break;
    case GROOVE_GB_UNWRAP_NULLABLE:
        dst->u.unwrap_nullable.input = groove__ivm__graph__arc_clone(src->u.unwrap_nullable.input);
        groove__ivm__graph__FieldRef__Clone__clone(&dst->u.unwrap_nullable.field, &src->u.unwrap_nullable.field);
        break;
    case GROOVE_GB_UNNEST:
        dst->u.unnest.input = groove__ivm__graph__arc_clone(src->u.unnest.input);
        groove__ivm__graph__FieldRef__Clone__clone(&dst->u.unnest.array_field, &src->u.unnest.array_field);
        dst->u.unnest.element_field = jz_str_clone(&src->u.unnest.element_field);
        break;
    case GROOVE_GB_VARIANT_PROJECT:
        dst->u.variant_project.input = groove__ivm__graph__arc_clone(src->u.variant_project.input);
        groove__ivm__graph__FieldRef__Clone__clone(&dst->u.variant_project.field, &src->u.variant_project.field);
        dst->u.variant_project.case_ = jz_str_clone(&src->u.variant_project.case_);
        break;
    case GROOVE_GB_PROJECT:
        dst->u.project.input = groove__ivm__graph__arc_clone(src->u.project.input);
        jz_vec_clone(&dst->u.project.fields, &src->u.project.fields, sizeof(groove_ProjectField), project_field_clone_v);
        break;
    case GROOVE_GB_STREAMING_CHECKSUM:
        dst->u.streaming_checksum.input = groove__ivm__graph__arc_clone(src->u.streaming_checksum.input);
        groove__ivm__graph__FieldRef__Clone__clone(&dst->u.streaming_checksum.field, &src->u.streaming_checksum.field);
        dst->u.streaming_checksum.output_field = jz_str_clone(&src->u.streaming_checksum.output_field);
        dst->u.streaming_checksum.window_bytes = src->u.streaming_checksum.window_bytes;
        dst->u.streaming_checksum.max_bytes_per_turn = src->u.streaming_checksum.max_bytes_per_turn;
        break;
    case GROOVE_GB_UNION:
        arcvec_clone(&dst->u.union_.inputs, &src->u.union_.inputs);
        break;
    case GROOVE_GB_JOIN: case GROOVE_GB_SEMI_JOIN: case GROOVE_GB_ANTI_JOIN:
        dst->u.join.left = groove__ivm__graph__arc_clone(src->u.join.left);
        dst->u.join.right = groove__ivm__graph__arc_clone(src->u.join.right);
        groove__ivm__graph__fieldrefvec_clone(&dst->u.join.left_on, &src->u.join.left_on);
        groove__ivm__graph__fieldrefvec_clone(&dst->u.join.right_on, &src->u.join.right_on);
        dst->u.join.comparison = src->u.join.comparison;
        break;
    case GROOVE_GB_LOOKUP:
        dst->u.lookup.keys = groove__ivm__graph__arc_clone(src->u.lookup.keys);
        groove__ivm__graph__fieldrefvec_clone(&dst->u.lookup.keys_on, &src->u.lookup.keys_on);
        dst->u.lookup.table = jz_str_clone(&src->u.lookup.table);
        dst->u.lookup.has_scan = src->u.lookup.has_scan;
        groove__ivm__op_types__scan_opt_clone(src->u.lookup.has_scan, &src->u.lookup.scan, &dst->u.lookup.scan);
        dst->u.lookup.has_variant_projection = src->u.lookup.has_variant_projection;
        groove__ivm__op_types__opt_str_clone(src->u.lookup.has_variant_projection, &src->u.lookup.variant_projection, &dst->u.lookup.variant_projection);
        break;
    case GROOVE_GB_ARG_MAX_BY: case GROOVE_GB_ARG_MIN_BY:
        dst->u.arg_by.input = groove__ivm__graph__arc_clone(src->u.arg_by.input);
        groove__ivm__graph__fieldrefvec_clone(&dst->u.arg_by.group_cols, &src->u.arg_by.group_cols);
        groove__ivm__graph__fieldrefvec_clone(&dst->u.arg_by.order_cols, &src->u.arg_by.order_cols);
        break;
    case GROOVE_GB_TOP_BY:
        dst->u.top_by.input = groove__ivm__graph__arc_clone(src->u.top_by.input);
        groove__ivm__graph__fieldrefvec_clone(&dst->u.top_by.group_cols, &src->u.top_by.group_cols);
        topby_orders_clone(&dst->u.top_by.order_cols, &src->u.top_by.order_cols);
        groove__ivm__graph__fieldrefvec_clone(&dst->u.top_by.tie_cols, &src->u.top_by.tie_cols);
        dst->u.top_by.offset = src->u.top_by.offset;
        dst->u.top_by.limit = src->u.top_by.limit;
        break;
    case GROOVE_GB_COLLECT_BY:
        dst->u.collect_by.input = groove__ivm__graph__arc_clone(src->u.collect_by.input);
        dst->u.collect_by.collect = JZ_NEW(groove_CollectByBuilder);
        groove__ivm__graph__CollectByBuilder__Clone__clone(dst->u.collect_by.collect, src->u.collect_by.collect);
        break;
    case GROOVE_GB_AGGREGATE:
        dst->u.aggregate.input = groove__ivm__graph__arc_clone(src->u.aggregate.input);
        groove__ivm__graph__fieldrefvec_clone(&dst->u.aggregate.group_cols, &src->u.aggregate.group_cols);
        jz_vec_clone(&dst->u.aggregate.aggregates, &src->u.aggregate.aggregates, sizeof(groove_AggregateExpr), groove__ivm__op_types__agg_expr_clone_v);
        break;
    }
}

/* drop_in_place<GraphBuilder> */
void groove__ivm__graph__GraphBuilder__drop(groove_GraphBuilder *v) {
    switch (v->tag) {
    case GROOVE_GB_TYPED_TEMPLATE:
        jz_rc_release(v->u.typed_template.program);
        groove__ivm__graph__GraphArcVec__drop(&v->u.typed_template.inputs);
        predvec_drop(&v->u.typed_template.predicates);
        jz_rc_release(v->u.typed_template.scalars);
        break;
    case GROOVE_GB_TEMPLATE_INPUT:
        groove__ivm__graph__arc_release(v->u.template_input.input);
        break;
    case GROOVE_GB_TABLE:
        jz_str_drop(&v->u.table.table);
        if (v->u.table.has_scan) groove__ivm__op_types__StaticScanSpec__drop(&v->u.table.scan);
        if (v->u.table.has_variant_projection) jz_str_drop(&v->u.table.variant_projection);
        break;
    case GROOVE_GB_INLINE_RECORDS:
        groove__ivm__bytesvec_drop(&v->u.inline_records.records);
        break;
    case GROOVE_GB_INPUT_SOURCE:
        break;
    case GROOVE_GB_INDEX:
        jz_str_drop(&v->u.index.table);
        jz_str_drop(&v->u.index.index);
        if (v->u.index.has_scan) groove__ivm__op_types__StaticScanSpec__drop(&v->u.index.scan);
        jz_vec_drop(&v->u.index.intersections, sizeof(groove_ScanIntersection), groove__ivm__op_types__intersection_drop_v);
        if (v->u.index.has_candidate_filter) groove__ivm__graph__IndexCandidateFilter__drop(&v->u.index.candidate_filter);
        if (v->u.index.has_row_projection) jz_str_drop(&v->u.index.row_projection);
        break;
    case GROOVE_GB_FRONTIER_SOURCE: jz_str_drop(&v->u.frontier_source.binding); break;
    case GROOVE_GB_BINDING_SOURCE: jz_str_drop(&v->u.binding_source.shape); break;
    case GROOVE_GB_RECURSIVE:
        groove__ivm__graph__arc_release(v->u.recursive.seed);
        groove__ivm__graph__arc_release(v->u.recursive.step);
        groove__ivm__graph__arc_release(v->u.recursive.step_witness);
        jz_str_drop(&v->u.recursive.frontier);
        break;
    case GROOVE_GB_RECURSIVE_STEP_WITNESS:
        groove__ivm__graph__arc_release(v->u.recursive_step_witness.recursive);
        break;
    case GROOVE_GB_FILTER:
        groove__ivm__graph__arc_release(v->u.filter.input);
        groove__ivm__op_types__PredicateExpr__drop(&v->u.filter.predicate);
        break;
    case GROOVE_GB_UNWRAP_NULLABLE:
        groove__ivm__graph__arc_release(v->u.unwrap_nullable.input);
        groove__ivm__graph__FieldRef__drop(&v->u.unwrap_nullable.field);
        break;
    case GROOVE_GB_UNNEST:
        groove__ivm__graph__arc_release(v->u.unnest.input);
        groove__ivm__graph__FieldRef__drop(&v->u.unnest.array_field);
        jz_str_drop(&v->u.unnest.element_field);
        break;
    case GROOVE_GB_VARIANT_PROJECT:
        groove__ivm__graph__arc_release(v->u.variant_project.input);
        groove__ivm__graph__FieldRef__drop(&v->u.variant_project.field);
        jz_str_drop(&v->u.variant_project.case_);
        break;
    case GROOVE_GB_PROJECT:
        groove__ivm__graph__arc_release(v->u.project.input);
        jz_vec_drop(&v->u.project.fields, sizeof(groove_ProjectField), project_field_drop_v);
        break;
    case GROOVE_GB_STREAMING_CHECKSUM:
        groove__ivm__graph__arc_release(v->u.streaming_checksum.input);
        groove__ivm__graph__FieldRef__drop(&v->u.streaming_checksum.field);
        jz_str_drop(&v->u.streaming_checksum.output_field);
        break;
    case GROOVE_GB_UNION:
        groove__ivm__graph__GraphArcVec__drop(&v->u.union_.inputs);
        break;
    case GROOVE_GB_JOIN: case GROOVE_GB_SEMI_JOIN: case GROOVE_GB_ANTI_JOIN:
        groove__ivm__graph__arc_release(v->u.join.left);
        groove__ivm__graph__arc_release(v->u.join.right);
        groove__ivm__graph__fieldrefvec_drop(&v->u.join.left_on);
        groove__ivm__graph__fieldrefvec_drop(&v->u.join.right_on);
        break;
    case GROOVE_GB_LOOKUP:
        groove__ivm__graph__arc_release(v->u.lookup.keys);
        groove__ivm__graph__fieldrefvec_drop(&v->u.lookup.keys_on);
        jz_str_drop(&v->u.lookup.table);
        if (v->u.lookup.has_scan) groove__ivm__op_types__StaticScanSpec__drop(&v->u.lookup.scan);
        if (v->u.lookup.has_variant_projection) jz_str_drop(&v->u.lookup.variant_projection);
        break;
    case GROOVE_GB_ARG_MAX_BY: case GROOVE_GB_ARG_MIN_BY:
        groove__ivm__graph__arc_release(v->u.arg_by.input);
        groove__ivm__graph__fieldrefvec_drop(&v->u.arg_by.group_cols);
        groove__ivm__graph__fieldrefvec_drop(&v->u.arg_by.order_cols);
        break;
    case GROOVE_GB_TOP_BY:
        groove__ivm__graph__arc_release(v->u.top_by.input);
        groove__ivm__graph__fieldrefvec_drop(&v->u.top_by.group_cols);
        topby_orders_drop(&v->u.top_by.order_cols);
        groove__ivm__graph__fieldrefvec_drop(&v->u.top_by.tie_cols);
        break;
    case GROOVE_GB_COLLECT_BY:
        groove__ivm__graph__arc_release(v->u.collect_by.input);
        groove__ivm__graph__CollectByBuilder__drop(v->u.collect_by.collect);
        jz_free(v->u.collect_by.collect);
        break;
    case GROOVE_GB_AGGREGATE:
        groove__ivm__graph__arc_release(v->u.aggregate.input);
        groove__ivm__graph__fieldrefvec_drop(&v->u.aggregate.group_cols);
        jz_vec_drop(&v->u.aggregate.aggregates, sizeof(groove_AggregateExpr), groove__ivm__op_types__agg_expr_drop_v);
        break;
    }
}

/* ======================================================================
 * GraphBuilder: derived PartialEq
 * ====================================================================== */
bool groove__ivm__graph__GraphBuilder__PartialEq__eq(const groove_GraphBuilder *a, const groove_GraphBuilder *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case GROOVE_GB_TYPED_TEMPLATE:
        return (a->u.typed_template.program == b->u.typed_template.program ||
                groove__ivm__template__TypedGraphTemplate__PartialEq__eq(a->u.typed_template.program, b->u.typed_template.program)) &&
               arcvec_eq(&a->u.typed_template.inputs, &b->u.typed_template.inputs) &&
               predvec_eq(&a->u.typed_template.predicates, &b->u.typed_template.predicates) &&
               scalar_args_eq(a->u.typed_template.scalars, b->u.typed_template.scalars);
    case GROOVE_GB_TEMPLATE_INPUT:
        return a->u.template_input.slot == b->u.template_input.slot &&
               a->u.template_input.output == b->u.template_input.output &&
               opt_arc_eq(a->u.template_input.input, b->u.template_input.input);
    case GROOVE_GB_TABLE:
        return jz_str_eq(&a->u.table.table, &b->u.table.table) &&
               groove__ivm__op_types__scan_opt_eq(a->u.table.has_scan, &a->u.table.scan, b->u.table.has_scan, &b->u.table.scan) &&
               groove__ivm__op_types__opt_str_eq(a->u.table.has_variant_projection, &a->u.table.variant_projection,
                                                 b->u.table.has_variant_projection, &b->u.table.variant_projection);
    case GROOVE_GB_INLINE_RECORDS:
        return a->u.inline_records.output == b->u.inline_records.output &&
               groove__ivm__bytesvec_eq(&a->u.inline_records.records, &b->u.inline_records.records);
    case GROOVE_GB_INPUT_SOURCE:
        return a->u.input_source.id.runtime_namespace == b->u.input_source.id.runtime_namespace &&
               a->u.input_source.id.local == b->u.input_source.id.local &&
               a->u.input_source.output == b->u.input_source.output;
    case GROOVE_GB_INDEX:
        return jz_str_eq(&a->u.index.table, &b->u.index.table) && jz_str_eq(&a->u.index.index, &b->u.index.index) &&
               groove__ivm__op_types__scan_opt_eq(a->u.index.has_scan, &a->u.index.scan, b->u.index.has_scan, &b->u.index.scan) &&
               intersections_eq(&a->u.index.intersections, &b->u.index.intersections) &&
               a->u.index.has_candidate_filter == b->u.index.has_candidate_filter &&
               (!a->u.index.has_candidate_filter ||
                groove__ivm__graph__IndexCandidateFilter__PartialEq__eq(&a->u.index.candidate_filter, &b->u.index.candidate_filter)) &&
               groove__ivm__op_types__opt_str_eq(a->u.index.has_row_projection, &a->u.index.row_projection,
                                                 b->u.index.has_row_projection, &b->u.index.row_projection);
    case GROOVE_GB_FRONTIER_SOURCE:
        return jz_str_eq(&a->u.frontier_source.binding, &b->u.frontier_source.binding) &&
               a->u.frontier_source.output == b->u.frontier_source.output;
    case GROOVE_GB_BINDING_SOURCE:
        return jz_str_eq(&a->u.binding_source.shape, &b->u.binding_source.shape) &&
               a->u.binding_source.output == b->u.binding_source.output;
    case GROOVE_GB_RECURSIVE:
        return arc_eq(a->u.recursive.seed, b->u.recursive.seed) && arc_eq(a->u.recursive.step, b->u.recursive.step) &&
               opt_arc_eq(a->u.recursive.step_witness, b->u.recursive.step_witness) &&
               jz_str_eq(&a->u.recursive.frontier, &b->u.recursive.frontier) &&
               a->u.recursive.max_iters == b->u.recursive.max_iters &&
               a->u.recursive.truncate_at_max_iters == b->u.recursive.truncate_at_max_iters;
    case GROOVE_GB_RECURSIVE_STEP_WITNESS:
        return arc_eq(a->u.recursive_step_witness.recursive, b->u.recursive_step_witness.recursive);
    case GROOVE_GB_FILTER:
        return arc_eq(a->u.filter.input, b->u.filter.input) &&
               groove__ivm__op_types__PredicateExpr__PartialEq__eq(&a->u.filter.predicate, &b->u.filter.predicate) &&
               a->u.filter.comparison == b->u.filter.comparison;
    case GROOVE_GB_UNWRAP_NULLABLE:
        return arc_eq(a->u.unwrap_nullable.input, b->u.unwrap_nullable.input) &&
               groove__ivm__graph__FieldRef__PartialEq__eq(&a->u.unwrap_nullable.field, &b->u.unwrap_nullable.field);
    case GROOVE_GB_UNNEST:
        return arc_eq(a->u.unnest.input, b->u.unnest.input) &&
               groove__ivm__graph__FieldRef__PartialEq__eq(&a->u.unnest.array_field, &b->u.unnest.array_field) &&
               jz_str_eq(&a->u.unnest.element_field, &b->u.unnest.element_field);
    case GROOVE_GB_VARIANT_PROJECT:
        return arc_eq(a->u.variant_project.input, b->u.variant_project.input) &&
               groove__ivm__graph__FieldRef__PartialEq__eq(&a->u.variant_project.field, &b->u.variant_project.field) &&
               jz_str_eq(&a->u.variant_project.case_, &b->u.variant_project.case_);
    case GROOVE_GB_PROJECT:
        return arc_eq(a->u.project.input, b->u.project.input) && project_fields_eq(&a->u.project.fields, &b->u.project.fields);
    case GROOVE_GB_STREAMING_CHECKSUM:
        return arc_eq(a->u.streaming_checksum.input, b->u.streaming_checksum.input) &&
               groove__ivm__graph__FieldRef__PartialEq__eq(&a->u.streaming_checksum.field, &b->u.streaming_checksum.field) &&
               jz_str_eq(&a->u.streaming_checksum.output_field, &b->u.streaming_checksum.output_field) &&
               a->u.streaming_checksum.window_bytes == b->u.streaming_checksum.window_bytes &&
               a->u.streaming_checksum.max_bytes_per_turn == b->u.streaming_checksum.max_bytes_per_turn;
    case GROOVE_GB_UNION:
        return arcvec_eq(&a->u.union_.inputs, &b->u.union_.inputs);
    case GROOVE_GB_JOIN: case GROOVE_GB_SEMI_JOIN: case GROOVE_GB_ANTI_JOIN:
        return arc_eq(a->u.join.left, b->u.join.left) && arc_eq(a->u.join.right, b->u.join.right) &&
               fieldrefvec_eq(&a->u.join.left_on, &b->u.join.left_on) && fieldrefvec_eq(&a->u.join.right_on, &b->u.join.right_on) &&
               a->u.join.comparison == b->u.join.comparison;
    case GROOVE_GB_LOOKUP:
        return arc_eq(a->u.lookup.keys, b->u.lookup.keys) && fieldrefvec_eq(&a->u.lookup.keys_on, &b->u.lookup.keys_on) &&
               jz_str_eq(&a->u.lookup.table, &b->u.lookup.table) &&
               groove__ivm__op_types__scan_opt_eq(a->u.lookup.has_scan, &a->u.lookup.scan, b->u.lookup.has_scan, &b->u.lookup.scan) &&
               groove__ivm__op_types__opt_str_eq(a->u.lookup.has_variant_projection, &a->u.lookup.variant_projection,
                                                 b->u.lookup.has_variant_projection, &b->u.lookup.variant_projection);
    case GROOVE_GB_ARG_MAX_BY: case GROOVE_GB_ARG_MIN_BY:
        return arc_eq(a->u.arg_by.input, b->u.arg_by.input) &&
               fieldrefvec_eq(&a->u.arg_by.group_cols, &b->u.arg_by.group_cols) &&
               fieldrefvec_eq(&a->u.arg_by.order_cols, &b->u.arg_by.order_cols);
    case GROOVE_GB_TOP_BY:
        return arc_eq(a->u.top_by.input, b->u.top_by.input) &&
               fieldrefvec_eq(&a->u.top_by.group_cols, &b->u.top_by.group_cols) &&
               topby_orders_eq(&a->u.top_by.order_cols, &b->u.top_by.order_cols) &&
               fieldrefvec_eq(&a->u.top_by.tie_cols, &b->u.top_by.tie_cols) &&
               a->u.top_by.offset == b->u.top_by.offset &&
               groove__ivm__op_types__limit_eq(&a->u.top_by.limit, &b->u.top_by.limit);
    case GROOVE_GB_COLLECT_BY:
        return arc_eq(a->u.collect_by.input, b->u.collect_by.input) &&
               groove__ivm__graph__CollectByBuilder__PartialEq__eq(a->u.collect_by.collect, b->u.collect_by.collect);
    case GROOVE_GB_AGGREGATE:
        return arc_eq(a->u.aggregate.input, b->u.aggregate.input) &&
               fieldrefvec_eq(&a->u.aggregate.group_cols, &b->u.aggregate.group_cols) &&
               aggvec_eq(&a->u.aggregate.aggregates, &b->u.aggregate.aggregates);
    }
    return false;
}

/* ======================================================================
 * GraphBuilder: derived Hash
 * ====================================================================== */
static void hash_descriptor(groove_Hasher *h, groove_RecordDescriptor d) { groove_hash_usize(h, (u32)(uintptr_t)d); }
void groove__ivm__graph__GraphBuilder__Hash__hash(const groove_GraphBuilder *v, groove_Hasher *h) {
    groove_hash_discr(h, v->tag);
    switch (v->tag) {
    case GROOVE_GB_TYPED_TEMPLATE:
        groove__ivm__template__TypedGraphTemplate__Hash__hash(v->u.typed_template.program, h);
        arcvec_hash(&v->u.typed_template.inputs, h);
        predvec_hash(&v->u.typed_template.predicates, h);
        scalar_args_hash(v->u.typed_template.scalars, h);
        break;
    case GROOVE_GB_TEMPLATE_INPUT:
        groove_hash_u32(h, v->u.template_input.slot);
        hash_descriptor(h, v->u.template_input.output);
        groove_hash_discr(h, v->u.template_input.input != NULL);
        if (v->u.template_input.input) groove__ivm__graph__GraphBuilder__Hash__hash(v->u.template_input.input, h);
        break;
    case GROOVE_GB_TABLE:
        groove_hash_str(h, &v->u.table.table);
        groove__ivm__op_types__scan_opt_hash(h, v->u.table.has_scan, &v->u.table.scan);
        groove__ivm__op_types__hash_opt_str(h, v->u.table.has_variant_projection, &v->u.table.variant_projection);
        break;
    case GROOVE_GB_INLINE_RECORDS:
        hash_descriptor(h, v->u.inline_records.output);
        groove__ivm__bytesvec_hash(&v->u.inline_records.records, h);
        break;
    case GROOVE_GB_INPUT_SOURCE:
        groove_hash_u64(h, v->u.input_source.id.runtime_namespace);
        groove_hash_u64(h, v->u.input_source.id.local);
        hash_descriptor(h, v->u.input_source.output);
        break;
    case GROOVE_GB_INDEX:
        groove_hash_str(h, &v->u.index.table);
        groove_hash_str(h, &v->u.index.index);
        groove__ivm__op_types__scan_opt_hash(h, v->u.index.has_scan, &v->u.index.scan);
        intersections_hash(&v->u.index.intersections, h);
        groove_hash_discr(h, v->u.index.has_candidate_filter);
        if (v->u.index.has_candidate_filter) groove__ivm__graph__IndexCandidateFilter__Hash__hash(&v->u.index.candidate_filter, h);
        groove__ivm__op_types__hash_opt_str(h, v->u.index.has_row_projection, &v->u.index.row_projection);
        break;
    case GROOVE_GB_FRONTIER_SOURCE:
        groove_hash_str(h, &v->u.frontier_source.binding);
        hash_descriptor(h, v->u.frontier_source.output);
        break;
    case GROOVE_GB_BINDING_SOURCE:
        groove_hash_str(h, &v->u.binding_source.shape);
        hash_descriptor(h, v->u.binding_source.output);
        break;
    case GROOVE_GB_RECURSIVE:
        groove__ivm__graph__GraphBuilder__Hash__hash(v->u.recursive.seed, h);
        groove__ivm__graph__GraphBuilder__Hash__hash(v->u.recursive.step, h);
        groove_hash_discr(h, v->u.recursive.step_witness != NULL);
        if (v->u.recursive.step_witness) groove__ivm__graph__GraphBuilder__Hash__hash(v->u.recursive.step_witness, h);
        groove_hash_str(h, &v->u.recursive.frontier);
        groove_hash_usize(h, v->u.recursive.max_iters);
        groove_hash_bool(h, v->u.recursive.truncate_at_max_iters);
        break;
    case GROOVE_GB_RECURSIVE_STEP_WITNESS:
        groove__ivm__graph__GraphBuilder__Hash__hash(v->u.recursive_step_witness.recursive, h);
        break;
    case GROOVE_GB_FILTER:
        groove__ivm__graph__GraphBuilder__Hash__hash(v->u.filter.input, h);
        groove__ivm__op_types__PredicateExpr__Hash__hash(&v->u.filter.predicate, h);
        groove_hash_discr(h, v->u.filter.comparison);
        break;
    case GROOVE_GB_UNWRAP_NULLABLE:
        groove__ivm__graph__GraphBuilder__Hash__hash(v->u.unwrap_nullable.input, h);
        groove__ivm__graph__FieldRef__Hash__hash(&v->u.unwrap_nullable.field, h);
        break;
    case GROOVE_GB_UNNEST:
        groove__ivm__graph__GraphBuilder__Hash__hash(v->u.unnest.input, h);
        groove__ivm__graph__FieldRef__Hash__hash(&v->u.unnest.array_field, h);
        groove_hash_str(h, &v->u.unnest.element_field);
        break;
    case GROOVE_GB_VARIANT_PROJECT:
        groove__ivm__graph__GraphBuilder__Hash__hash(v->u.variant_project.input, h);
        groove__ivm__graph__FieldRef__Hash__hash(&v->u.variant_project.field, h);
        groove_hash_str(h, &v->u.variant_project.case_);
        break;
    case GROOVE_GB_PROJECT:
        groove__ivm__graph__GraphBuilder__Hash__hash(v->u.project.input, h);
        project_fields_hash(&v->u.project.fields, h);
        break;
    case GROOVE_GB_STREAMING_CHECKSUM:
        groove__ivm__graph__GraphBuilder__Hash__hash(v->u.streaming_checksum.input, h);
        groove__ivm__graph__FieldRef__Hash__hash(&v->u.streaming_checksum.field, h);
        groove_hash_str(h, &v->u.streaming_checksum.output_field);
        groove_hash_usize(h, v->u.streaming_checksum.window_bytes);
        groove_hash_usize(h, v->u.streaming_checksum.max_bytes_per_turn);
        break;
    case GROOVE_GB_UNION:
        arcvec_hash(&v->u.union_.inputs, h);
        break;
    case GROOVE_GB_JOIN: case GROOVE_GB_SEMI_JOIN: case GROOVE_GB_ANTI_JOIN:
        groove__ivm__graph__GraphBuilder__Hash__hash(v->u.join.left, h);
        groove__ivm__graph__GraphBuilder__Hash__hash(v->u.join.right, h);
        fieldrefvec_hash(&v->u.join.left_on, h);
        fieldrefvec_hash(&v->u.join.right_on, h);
        groove_hash_discr(h, v->u.join.comparison);
        break;
    case GROOVE_GB_LOOKUP:
        groove__ivm__graph__GraphBuilder__Hash__hash(v->u.lookup.keys, h);
        fieldrefvec_hash(&v->u.lookup.keys_on, h);
        groove_hash_str(h, &v->u.lookup.table);
        groove__ivm__op_types__scan_opt_hash(h, v->u.lookup.has_scan, &v->u.lookup.scan);
        groove__ivm__op_types__hash_opt_str(h, v->u.lookup.has_variant_projection, &v->u.lookup.variant_projection);
        break;
    case GROOVE_GB_ARG_MAX_BY: case GROOVE_GB_ARG_MIN_BY:
        groove__ivm__graph__GraphBuilder__Hash__hash(v->u.arg_by.input, h);
        fieldrefvec_hash(&v->u.arg_by.group_cols, h);
        fieldrefvec_hash(&v->u.arg_by.order_cols, h);
        break;
    case GROOVE_GB_TOP_BY:
        groove__ivm__graph__GraphBuilder__Hash__hash(v->u.top_by.input, h);
        fieldrefvec_hash(&v->u.top_by.group_cols, h);
        topby_orders_hash(&v->u.top_by.order_cols, h);
        fieldrefvec_hash(&v->u.top_by.tie_cols, h);
        groove_hash_u64(h, v->u.top_by.offset);
        groove__ivm__op_types__limit_hash(h, &v->u.top_by.limit);
        break;
    case GROOVE_GB_COLLECT_BY:
        groove__ivm__graph__GraphBuilder__Hash__hash(v->u.collect_by.input, h);
        collect_builder_hash(v->u.collect_by.collect, h);
        break;
    case GROOVE_GB_AGGREGATE:
        groove__ivm__graph__GraphBuilder__Hash__hash(v->u.aggregate.input, h);
        fieldrefvec_hash(&v->u.aggregate.group_cols, h);
        aggvec_hash(&v->u.aggregate.aggregates, h);
        break;
    }
}

/* ======================================================================
 * GraphBuilder: derived Debug
 * ====================================================================== */
static void dbg_arc_field(groove_DebugStruct *d, const char *name, groove_GraphArc a) {
    groove__fmt__DebugStruct__field(d, name);
    groove__ivm__graph__GraphBuilder__Debug__fmt(d->out, a);
}
static void dbg_fieldref_field(groove_DebugStruct *d, const char *name, const groove_FieldRef *f) {
    groove__fmt__DebugStruct__field(d, name);
    groove__ivm__graph__FieldRef__Debug__fmt(d->out, f);
}
static void dbg_str_field(groove_DebugStruct *d, const char *name, const jz_str *s) {
    groove__fmt__DebugStruct__field(d, name);
    groove__fmt__debug_str(d->out, s);
}
static void dbg_descriptor_field(groove_DebugStruct *d, const char *name, groove_RecordDescriptor r) {
    groove__fmt__DebugStruct__field(d, name);
    groove__records__RecordDescriptor__Debug__fmt(d->out, r);
}
void groove__ivm__graph__GraphBuilder__Debug__fmt(jz_str *out, const groove_GraphBuilder *v) {
    static const char *const NAMES[] = {
        "TypedTemplate", "TemplateInput", "Table", "InlineRecords", "InputSource", "Index", "FrontierSource",
        "BindingSource", "Recursive", "RecursiveStepWitness", "Filter", "UnwrapNullable", "Unnest",
        "VariantProject", "Project", "StreamingChecksum", "Union", "Join", "SemiJoin", "AntiJoin", "Lookup",
        "ArgMaxBy", "ArgMinBy", "TopBy", "CollectBy", "Aggregate"
    };
    groove_DebugStruct d = groove__fmt__debug_struct(out, NAMES[v->tag]);
    switch (v->tag) {
    case GROOVE_GB_TYPED_TEMPLATE:
        groove__fmt__DebugStruct__field(&d, "program"); groove__ivm__template__TypedGraphTemplate__Debug__fmt(out, v->u.typed_template.program);
        groove__fmt__DebugStruct__field(&d, "inputs"); arcvec_debug(out, &v->u.typed_template.inputs);
        groove__fmt__DebugStruct__field(&d, "predicates"); predvec_debug(out, &v->u.typed_template.predicates);
        groove__fmt__DebugStruct__field(&d, "scalars");
        jz_str_push_char(out, '[');
        for (u32 i = 0; i < v->u.typed_template.scalars->len; i++) {
            if (i) jz_str_push_cstr(out, ", ");
            groove__ivm__template__arguments__TemplateScalarArgument__Debug__fmt(out, &v->u.typed_template.scalars->items[i]);
        }
        jz_str_push_char(out, ']');
        break;
    case GROOVE_GB_TEMPLATE_INPUT:
        groove__fmt__DebugStruct__field(&d, "slot"); jz_str_appendf(out, "%u", (unsigned)v->u.template_input.slot);
        dbg_descriptor_field(&d, "output", v->u.template_input.output);
        groove__fmt__DebugStruct__field(&d, "input"); opt_arc_debug(out, v->u.template_input.input);
        break;
    case GROOVE_GB_TABLE:
        dbg_str_field(&d, "table", &v->u.table.table);
        groove__fmt__DebugStruct__field(&d, "scan"); groove__ivm__op_types__scan_opt_debug(out, v->u.table.has_scan, &v->u.table.scan);
        groove__fmt__DebugStruct__field(&d, "variant_projection");
        groove__ivm__op_types__dbg_opt_str(out, v->u.table.has_variant_projection, &v->u.table.variant_projection);
        break;
    case GROOVE_GB_INLINE_RECORDS:
        dbg_descriptor_field(&d, "output", v->u.inline_records.output);
        groove__fmt__DebugStruct__field(&d, "records"); bytesvec_debug(out, &v->u.inline_records.records);
        break;
    case GROOVE_GB_INPUT_SOURCE:
        groove__fmt__DebugStruct__field(&d, "id"); input_source_id_debug(out, &v->u.input_source.id);
        dbg_descriptor_field(&d, "output", v->u.input_source.output);
        break;
    case GROOVE_GB_INDEX:
        dbg_str_field(&d, "table", &v->u.index.table);
        dbg_str_field(&d, "index", &v->u.index.index);
        groove__fmt__DebugStruct__field(&d, "scan"); groove__ivm__op_types__scan_opt_debug(out, v->u.index.has_scan, &v->u.index.scan);
        groove__fmt__DebugStruct__field(&d, "intersections"); intersections_debug(out, &v->u.index.intersections);
        groove__fmt__DebugStruct__field(&d, "candidate_filter");
        if (v->u.index.has_candidate_filter) {
            jz_str_push_cstr(out, "Some(");
            groove__ivm__graph__IndexCandidateFilter__Debug__fmt(out, &v->u.index.candidate_filter);
            jz_str_push_char(out, ')');
        } else jz_str_push_cstr(out, "None");
        groove__fmt__DebugStruct__field(&d, "row_projection");
        groove__ivm__op_types__dbg_opt_str(out, v->u.index.has_row_projection, &v->u.index.row_projection);
        break;
    case GROOVE_GB_FRONTIER_SOURCE:
        groove__fmt__DebugStruct__field(&d, "binding");
        groove__fmt__debug_tuple_begin(out, "FrontierName"); groove__fmt__debug_str(out, &v->u.frontier_source.binding); groove__fmt__debug_tuple_end(out);
        dbg_descriptor_field(&d, "output", v->u.frontier_source.output);
        break;
    case GROOVE_GB_BINDING_SOURCE:
        dbg_str_field(&d, "shape", &v->u.binding_source.shape);
        dbg_descriptor_field(&d, "output", v->u.binding_source.output);
        break;
    case GROOVE_GB_RECURSIVE:
        dbg_arc_field(&d, "seed", v->u.recursive.seed);
        dbg_arc_field(&d, "step", v->u.recursive.step);
        groove__fmt__DebugStruct__field(&d, "step_witness"); opt_arc_debug(out, v->u.recursive.step_witness);
        groove__fmt__DebugStruct__field(&d, "frontier");
        groove__fmt__debug_tuple_begin(out, "FrontierName"); groove__fmt__debug_str(out, &v->u.recursive.frontier); groove__fmt__debug_tuple_end(out);
        groove__fmt__DebugStruct__field(&d, "max_iters"); jz_str_appendf(out, "%u", (unsigned)v->u.recursive.max_iters);
        groove__fmt__DebugStruct__field(&d, "truncate_at_max_iters"); groove__ivm__op_types__dbg_bool(out, v->u.recursive.truncate_at_max_iters);
        break;
    case GROOVE_GB_RECURSIVE_STEP_WITNESS:
        dbg_arc_field(&d, "recursive", v->u.recursive_step_witness.recursive);
        break;
    case GROOVE_GB_FILTER:
        dbg_arc_field(&d, "input", v->u.filter.input);
        groove__fmt__DebugStruct__field(&d, "predicate"); groove__ivm__op_types__PredicateExpr__Debug__fmt(out, &v->u.filter.predicate);
        groove__fmt__DebugStruct__field(&d, "comparison"); groove__ivm__op_types__dbg_cmp(out, v->u.filter.comparison);
        break;
    case GROOVE_GB_UNWRAP_NULLABLE:
        dbg_arc_field(&d, "input", v->u.unwrap_nullable.input);
        dbg_fieldref_field(&d, "field", &v->u.unwrap_nullable.field);
        break;
    case GROOVE_GB_UNNEST:
        dbg_arc_field(&d, "input", v->u.unnest.input);
        dbg_fieldref_field(&d, "array_field", &v->u.unnest.array_field);
        dbg_str_field(&d, "element_field", &v->u.unnest.element_field);
        break;
    case GROOVE_GB_VARIANT_PROJECT:
        dbg_arc_field(&d, "input", v->u.variant_project.input);
        dbg_fieldref_field(&d, "field", &v->u.variant_project.field);
        dbg_str_field(&d, "case", &v->u.variant_project.case_);
        break;
    case GROOVE_GB_PROJECT:
        dbg_arc_field(&d, "input", v->u.project.input);
        groove__fmt__DebugStruct__field(&d, "fields"); project_fields_debug(out, &v->u.project.fields);
        break;
    case GROOVE_GB_STREAMING_CHECKSUM:
        dbg_arc_field(&d, "input", v->u.streaming_checksum.input);
        dbg_fieldref_field(&d, "field", &v->u.streaming_checksum.field);
        dbg_str_field(&d, "output_field", &v->u.streaming_checksum.output_field);
        groove__fmt__DebugStruct__field(&d, "window_bytes"); jz_str_appendf(out, "%u", (unsigned)v->u.streaming_checksum.window_bytes);
        groove__fmt__DebugStruct__field(&d, "max_bytes_per_turn"); jz_str_appendf(out, "%u", (unsigned)v->u.streaming_checksum.max_bytes_per_turn);
        break;
    case GROOVE_GB_UNION:
        groove__fmt__DebugStruct__field(&d, "inputs"); arcvec_debug(out, &v->u.union_.inputs);
        break;
    case GROOVE_GB_JOIN: case GROOVE_GB_SEMI_JOIN: case GROOVE_GB_ANTI_JOIN:
        dbg_arc_field(&d, "left", v->u.join.left);
        dbg_arc_field(&d, "right", v->u.join.right);
        groove__fmt__DebugStruct__field(&d, "left_on"); fieldrefvec_debug(out, &v->u.join.left_on);
        groove__fmt__DebugStruct__field(&d, "right_on"); fieldrefvec_debug(out, &v->u.join.right_on);
        groove__fmt__DebugStruct__field(&d, "comparison"); groove__ivm__op_types__dbg_cmp(out, v->u.join.comparison);
        break;
    case GROOVE_GB_LOOKUP:
        dbg_arc_field(&d, "keys", v->u.lookup.keys);
        groove__fmt__DebugStruct__field(&d, "keys_on"); fieldrefvec_debug(out, &v->u.lookup.keys_on);
        dbg_str_field(&d, "table", &v->u.lookup.table);
        groove__fmt__DebugStruct__field(&d, "scan"); groove__ivm__op_types__scan_opt_debug(out, v->u.lookup.has_scan, &v->u.lookup.scan);
        groove__fmt__DebugStruct__field(&d, "variant_projection");
        groove__ivm__op_types__dbg_opt_str(out, v->u.lookup.has_variant_projection, &v->u.lookup.variant_projection);
        break;
    case GROOVE_GB_ARG_MAX_BY: case GROOVE_GB_ARG_MIN_BY:
        dbg_arc_field(&d, "input", v->u.arg_by.input);
        groove__fmt__DebugStruct__field(&d, "group_cols"); fieldrefvec_debug(out, &v->u.arg_by.group_cols);
        groove__fmt__DebugStruct__field(&d, "order_cols"); fieldrefvec_debug(out, &v->u.arg_by.order_cols);
        break;
    case GROOVE_GB_TOP_BY:
        dbg_arc_field(&d, "input", v->u.top_by.input);
        groove__fmt__DebugStruct__field(&d, "group_cols"); fieldrefvec_debug(out, &v->u.top_by.group_cols);
        groove__fmt__DebugStruct__field(&d, "order_cols"); topby_orders_debug(out, &v->u.top_by.order_cols);
        groove__fmt__DebugStruct__field(&d, "tie_cols"); fieldrefvec_debug(out, &v->u.top_by.tie_cols);
        groove__fmt__DebugStruct__field(&d, "offset"); jz_str_appendf(out, "%llu", (unsigned long long)v->u.top_by.offset);
        groove__fmt__DebugStruct__field(&d, "limit"); groove__ivm__op_types__TopByLimit__Debug__fmt(out, &v->u.top_by.limit);
        break;
    case GROOVE_GB_COLLECT_BY:
        dbg_arc_field(&d, "input", v->u.collect_by.input);
        groove__fmt__DebugStruct__field(&d, "collect"); collect_builder_debug(out, v->u.collect_by.collect);
        break;
    case GROOVE_GB_AGGREGATE:
        dbg_arc_field(&d, "input", v->u.aggregate.input);
        groove__fmt__DebugStruct__field(&d, "group_cols"); fieldrefvec_debug(out, &v->u.aggregate.group_cols);
        groove__fmt__DebugStruct__field(&d, "aggregates"); aggvec_debug(out, &v->u.aggregate.aggregates);
        break;
    }
    groove__fmt__DebugStruct__finish(&d);
}

/* ======================================================================
 * InputSourceId
 * ====================================================================== */
groove_InputSourceId groove__ivm__graph__InputSourceId__new(u64 runtime_namespace, u64 local) {
    groove_InputSourceId id = { runtime_namespace, local };
    return id;
}
bool groove__ivm__graph__InputSourceId__belongs_to(groove_InputSourceId self, u64 runtime_namespace) {
    return self.runtime_namespace == runtime_namespace;
}
bool groove__ivm__graph__InputSourceId__was_allocated_by(groove_InputSourceId self, u64 runtime_namespace, u64 next_local) {
    return groove__ivm__graph__InputSourceId__belongs_to(self, runtime_namespace) && self.local < next_local;
}
groove_BindingSourceKey groove__ivm__graph__InputSourceId__binding_key(groove_InputSourceId self) {
    groove_BindingSourceKey k; memset(&k, 0, sizeof k);
    k.tag = GROOVE_BSK_RUNTIME_INPUT;
    k.runtime_namespace = self.runtime_namespace;
    k.local = self.local;
    return k;
}
jz_str groove__ivm__graph__InputSourceId__diagnostic_name(groove_InputSourceId self) {
    return jz_format("runtime input source %llu:%llu", (unsigned long long)self.runtime_namespace, (unsigned long long)self.local);
}

/* ======================================================================
 * CollectBySlotBuilder
 * ====================================================================== */
void groove__ivm__graph__CollectBySlotBuilder__new(groove_CollectBySlotBuilder *out, const jz_str *group_cols, u32 n_group,
                                                   groove_CollectByFieldVec child_fields, jz_str collection_field,
                                                   groove_CollectBySlotBuilderVec slots, groove_TopByOrderVec order_cols,
                                                   const jz_str *tie_cols, u32 n_tie, u64 offset, groove_TopByLimit limit) {
    memset(out, 0, sizeof *out);
    groove__ivm__graph__fieldrefvec_from_names(&out->group_cols, group_cols, n_group);
    out->child_fields = child_fields;
    out->collection_field = collection_field;
    out->slots = slots;
    out->order_cols = order_cols;
    groove__ivm__graph__fieldrefvec_from_names(&out->tie_cols, tie_cols, n_tie);
    out->has_presence_col = false;
    out->has_reference_array_col = false;
    out->reference_order = false;
    out->offset = offset;
    out->limit = limit;
}
void groove__ivm__graph__CollectBySlotBuilder__with_presence_col(groove_CollectBySlotBuilder *self, jz_str presence_col) {
    if (self->has_presence_col) groove__ivm__graph__FieldRef__drop(&self->presence_col);
    self->has_presence_col = true;
    self->presence_col = groove__ivm__graph__FieldRef__name(presence_col);
}
void groove__ivm__graph__CollectBySlotBuilder__with_owner_key_cols(groove_CollectBySlotBuilder *self, const jz_str *cols, u32 n) {
    groove__ivm__graph__fieldrefvec_drop(&self->owner_key_cols);
    groove__ivm__graph__fieldrefvec_from_names(&self->owner_key_cols, cols, n);
}

/* ======================================================================
 * GraphBuilder constructors
 * ====================================================================== */
static groove_GraphBuilder gb_new(u8 tag) {
    groove_GraphBuilder g; memset(&g, 0, sizeof g);
    g.tag = tag;
    return g;
}
static groove_GraphBuilder gb_table(jz_str table, bool has_scan, groove_StaticScanSpec *scan, bool has_vp, jz_str vp) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_TABLE);
    g.u.table.table = table;
    g.u.table.has_scan = has_scan;
    if (has_scan) g.u.table.scan = *scan;
    g.u.table.has_variant_projection = has_vp;
    g.u.table.variant_projection = vp;
    return g;
}
static const jz_str EMPTY_STR = { NULL, 0, 0 };
groove_GraphBuilder groove__ivm__graph__GraphBuilder__table(jz_str table) {
    return gb_table(table, false, NULL, false, EMPTY_STR);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__table_scan(jz_str table, groove_StaticScanSpec scan) {
    return gb_table(table, true, &scan, false, EMPTY_STR);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__variant_source(jz_str table, jz_str projection_target) {
    return gb_table(table, false, NULL, true, projection_target);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__variant_source_scan(jz_str table, jz_str projection_target, groove_StaticScanSpec scan) {
    return gb_table(table, true, &scan, true, projection_target);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__inline_records(groove_RecordDescriptor output, groove_BytesVec records) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_INLINE_RECORDS);
    g.u.inline_records.output = output;
    g.u.inline_records.records = records;
    return g;
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__input_source(groove_InputSourceId id, groove_RecordDescriptor output) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_INPUT_SOURCE);
    g.u.input_source.id = id;
    g.u.input_source.output = output;
    return g;
}
jz_err groove__ivm__graph__GraphBuilder__values(groove_RecordDescriptor output, const groove_ValueVec *rows, u32 n_rows, groove_GraphBuilder *out) {
    groove_BytesVec records = {0};
    for (u32 i = 0; i < n_rows; i++) {
        jz_bytes record;
        jz_err e = groove__records__RecordDescriptor__create(output, rows[i].ptr, rows[i].len, &record);
        if (e.code) { groove__ivm__bytesvec_drop(&records); return e; }
        JZ_VEC_PUSH(records, record);
    }
    *out = groove__ivm__graph__GraphBuilder__inline_records(output, records);
    return JZ_OK;
}
static groove_GraphBuilder gb_index(jz_str table, jz_str index, bool has_scan, groove_StaticScanSpec *scan,
                                    groove_ScanIntersectionVec intersections, bool has_cf, groove_IndexCandidateFilter *cf,
                                    bool has_rp, jz_str rp) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_INDEX);
    g.u.index.table = table;
    g.u.index.index = index;
    g.u.index.has_scan = has_scan;
    if (has_scan) g.u.index.scan = *scan;
    g.u.index.intersections = intersections;
    g.u.index.has_candidate_filter = has_cf;
    if (has_cf) g.u.index.candidate_filter = *cf;
    g.u.index.has_row_projection = has_rp;
    g.u.index.row_projection = rp;
    return g;
}
static const groove_ScanIntersectionVec NO_INTERSECTIONS = { NULL, 0, 0 };
groove_GraphBuilder groove__ivm__graph__GraphBuilder__index(jz_str table, jz_str index) {
    return gb_index(table, index, false, NULL, NO_INTERSECTIONS, false, NULL, false, EMPTY_STR);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__index_scan(jz_str table, jz_str index, groove_StaticScanSpec scan) {
    return gb_index(table, index, true, &scan, NO_INTERSECTIONS, false, NULL, false, EMPTY_STR);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__variant_index_scan(jz_str table, jz_str index, jz_str projection_target, groove_StaticScanSpec scan) {
    return gb_index(table, index, true, &scan, NO_INTERSECTIONS, false, NULL, true, projection_target);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__variant_index_intersection_scan(jz_str table, jz_str index, groove_StaticScanSpec scan,
                                                                                   groove_ScanIntersectionVec intersections, jz_str projection_target) {
    return gb_index(table, index, true, &scan, intersections, false, NULL, true, projection_target);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__variant_index_candidate_scan(jz_str table, jz_str index, groove_StaticScanSpec scan,
                                                                                 groove_IndexCandidateFilter candidate_filter, jz_str projection_target) {
    return gb_index(table, index, true, &scan, NO_INTERSECTIONS, true, &candidate_filter, true, projection_target);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__frontier_source(jz_str binding, groove_RecordDescriptor output) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_FRONTIER_SOURCE);
    g.u.frontier_source.binding = binding;
    g.u.frontier_source.output = output;
    return g;
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__binding_source(jz_str shape, groove_RecordDescriptor output) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_BINDING_SOURCE);
    g.u.binding_source.shape = shape;
    g.u.binding_source.output = output;
    return g;
}
static groove_GraphBuilder recursive_with_limit(groove_GraphBuilder seed, groove_GraphBuilder step, jz_str frontier,
                                                u32 max_iters, bool truncate_at_max_iters) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_RECURSIVE);
    g.u.recursive.seed = groove__ivm__graph__arc_new(seed);
    g.u.recursive.step = groove__ivm__graph__arc_new(step);
    g.u.recursive.step_witness = NULL;
    g.u.recursive.frontier = frontier;
    g.u.recursive.max_iters = max_iters;
    g.u.recursive.truncate_at_max_iters = truncate_at_max_iters;
    return g;
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__recursive(groove_GraphBuilder seed, groove_GraphBuilder step, jz_str frontier, u32 max_iters) {
    return recursive_with_limit(seed, step, frontier, max_iters, false);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__recursive_bounded(groove_GraphBuilder seed, groove_GraphBuilder step, jz_str frontier, u32 max_iters) {
    return recursive_with_limit(seed, step, frontier, max_iters, true);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__recursive_with_step_witness(groove_GraphBuilder seed, groove_GraphBuilder step,
                                                                                groove_GraphBuilder step_witness, jz_str frontier,
                                                                                u32 max_iters, bool truncate_at_max_iters) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_RECURSIVE);
    g.u.recursive.seed = groove__ivm__graph__arc_new(seed);
    g.u.recursive.step = groove__ivm__graph__arc_new(step);
    g.u.recursive.step_witness = groove__ivm__graph__arc_new(step_witness);
    g.u.recursive.frontier = frontier;
    g.u.recursive.max_iters = max_iters;
    g.u.recursive.truncate_at_max_iters = truncate_at_max_iters;
    return g;
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__recursive_step_witness(groove_GraphBuilder recursive) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_RECURSIVE_STEP_WITNESS);
    g.u.recursive_step_witness.recursive = groove__ivm__graph__arc_new(recursive);
    return g;
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__union(groove_GraphBuilder *inputs, u32 n) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_UNION);
    jz_vec_reserve(&g.u.union_.inputs, n, sizeof(groove_GraphArc));
    for (u32 i = 0; i < n; i++) JZ_VEC_PUSH(g.u.union_.inputs, groove__ivm__graph__arc_new(inputs[i]));
    return g;
}

/* ---- traversal ---- */
typedef struct { const groove_GraphBuilder *graph; bool visited; } postorder_frame;
typedef JZ_VEC(postorder_frame) postorder_stack;
static void po_push(postorder_stack *s, const groove_GraphBuilder *g) { postorder_frame f = { g, false }; JZ_VEC_PUSH(*s, f); }
void groove__ivm__graph__GraphBuilder__postorder(const groove_GraphBuilder *self, groove_GraphRefVec *ordered) {
    postorder_stack pending = {0};
    memset(ordered, 0, sizeof *ordered);
    po_push(&pending, self);
    while (pending.len) {
        postorder_frame top = pending.ptr[--pending.len];
        const groove_GraphBuilder *graph = top.graph;
        if (top.visited) {
            JZ_VEC_PUSH(*ordered, graph);
            continue;
        }
        postorder_frame again = { graph, true };
        JZ_VEC_PUSH(pending, again);
        switch (graph->tag) {
        case GROOVE_GB_TYPED_TEMPLATE:
            for (u32 i = 0; i < graph->u.typed_template.inputs.len; i++) po_push(&pending, graph->u.typed_template.inputs.ptr[i]);
            break;
        case GROOVE_GB_TEMPLATE_INPUT:
            if (graph->u.template_input.input) po_push(&pending, graph->u.template_input.input);
            break;
        case GROOVE_GB_FILTER: po_push(&pending, graph->u.filter.input); break;
        case GROOVE_GB_PROJECT: po_push(&pending, graph->u.project.input); break;
        case GROOVE_GB_STREAMING_CHECKSUM: po_push(&pending, graph->u.streaming_checksum.input); break;
        case GROOVE_GB_UNWRAP_NULLABLE: po_push(&pending, graph->u.unwrap_nullable.input); break;
        case GROOVE_GB_UNNEST: po_push(&pending, graph->u.unnest.input); break;
        case GROOVE_GB_VARIANT_PROJECT: po_push(&pending, graph->u.variant_project.input); break;
        case GROOVE_GB_ARG_MAX_BY: case GROOVE_GB_ARG_MIN_BY: po_push(&pending, graph->u.arg_by.input); break;
        case GROOVE_GB_TOP_BY: po_push(&pending, graph->u.top_by.input); break;
        case GROOVE_GB_COLLECT_BY: po_push(&pending, graph->u.collect_by.input); break;
        case GROOVE_GB_AGGREGATE: po_push(&pending, graph->u.aggregate.input); break;
        case GROOVE_GB_LOOKUP: po_push(&pending, graph->u.lookup.keys); break;
        case GROOVE_GB_UNION:
            for (u32 i = graph->u.union_.inputs.len; i-- > 0;) po_push(&pending, graph->u.union_.inputs.ptr[i]);
            break;
        case GROOVE_GB_JOIN: case GROOVE_GB_SEMI_JOIN: case GROOVE_GB_ANTI_JOIN:
            po_push(&pending, graph->u.join.right);
            po_push(&pending, graph->u.join.left);
            break;
        case GROOVE_GB_RECURSIVE:
            if (graph->u.recursive.step_witness) po_push(&pending, graph->u.recursive.step_witness);
            po_push(&pending, graph->u.recursive.step);
            po_push(&pending, graph->u.recursive.seed);
            break;
        case GROOVE_GB_RECURSIVE_STEP_WITNESS: po_push(&pending, graph->u.recursive_step_witness.recursive); break;
        case GROOVE_GB_TABLE: case GROOVE_GB_INLINE_RECORDS: case GROOVE_GB_INPUT_SOURCE: case GROOVE_GB_INDEX:
        case GROOVE_GB_FRONTIER_SOURCE: case GROOVE_GB_BINDING_SOURCE:
            break;
        }
    }
    jz_vec_drop(&pending, sizeof(postorder_frame), NULL);
}

/* The single input slot of a unary builder, or NULL. */
static groove_GraphArc *unary_input(groove_GraphBuilder *g) {
    switch (g->tag) {
    case GROOVE_GB_FILTER: return &g->u.filter.input;
    case GROOVE_GB_PROJECT: return &g->u.project.input;
    case GROOVE_GB_STREAMING_CHECKSUM: return &g->u.streaming_checksum.input;
    case GROOVE_GB_UNWRAP_NULLABLE: return &g->u.unwrap_nullable.input;
    case GROOVE_GB_UNNEST: return &g->u.unnest.input;
    case GROOVE_GB_VARIANT_PROJECT: return &g->u.variant_project.input;
    case GROOVE_GB_ARG_MAX_BY: case GROOVE_GB_ARG_MIN_BY: return &g->u.arg_by.input;
    case GROOVE_GB_TOP_BY: return &g->u.top_by.input;
    case GROOVE_GB_COLLECT_BY: return &g->u.collect_by.input;
    case GROOVE_GB_AGGREGATE: return &g->u.aggregate.input;
    case GROOVE_GB_LOOKUP: return &g->u.lookup.keys;
    default: return NULL;
    }
}

void groove__ivm__graph__GraphBuilder__visit_inputs(const groove_GraphBuilder *self, groove_GraphVisitFn visit, void *ctx) {
    groove_GraphArc *single = unary_input((groove_GraphBuilder *)self);
    if (single) { visit(single, ctx); return; }
    switch (self->tag) {
    case GROOVE_GB_TYPED_TEMPLATE:
        for (u32 i = 0; i < self->u.typed_template.inputs.len; i++) visit(&self->u.typed_template.inputs.ptr[i], ctx);
        break;
    case GROOVE_GB_TEMPLATE_INPUT:
        if (self->u.template_input.input) visit(&self->u.template_input.input, ctx);
        break;
    case GROOVE_GB_UNION:
        for (u32 i = 0; i < self->u.union_.inputs.len; i++) visit(&self->u.union_.inputs.ptr[i], ctx);
        break;
    case GROOVE_GB_JOIN: case GROOVE_GB_SEMI_JOIN: case GROOVE_GB_ANTI_JOIN:
        visit(&self->u.join.left, ctx);
        visit(&self->u.join.right, ctx);
        break;
    case GROOVE_GB_RECURSIVE:
        visit(&self->u.recursive.seed, ctx);
        visit(&self->u.recursive.step, ctx);
        if (self->u.recursive.step_witness) visit(&self->u.recursive.step_witness, ctx);
        break;
    case GROOVE_GB_RECURSIVE_STEP_WITNESS:
        visit(&self->u.recursive_step_witness.recursive, ctx);
        break;
    default:
        break;
    }
}

static void remap(groove_GraphArc *slot, groove_GraphMapFn map, void *ctx) {
    groove_GraphArc next = map(slot, ctx);
    groove__ivm__graph__arc_release(*slot);
    *slot = next;
}
void groove__ivm__graph__GraphBuilder__map_inputs(const groove_GraphBuilder *self, groove_GraphMapFn map, void *ctx, groove_GraphBuilder *graph) {
    groove__ivm__graph__GraphBuilder__Clone__clone(graph, self);
    groove_GraphArc *single = unary_input(graph);
    if (single) { remap(single, map, ctx); return; }
    switch (graph->tag) {
    case GROOVE_GB_TYPED_TEMPLATE:
        for (u32 i = 0; i < graph->u.typed_template.inputs.len; i++) remap(&graph->u.typed_template.inputs.ptr[i], map, ctx);
        break;
    case GROOVE_GB_TEMPLATE_INPUT:
        if (graph->u.template_input.input) remap(&graph->u.template_input.input, map, ctx);
        break;
    case GROOVE_GB_UNION:
        for (u32 i = 0; i < graph->u.union_.inputs.len; i++) remap(&graph->u.union_.inputs.ptr[i], map, ctx);
        break;
    case GROOVE_GB_JOIN: case GROOVE_GB_SEMI_JOIN: case GROOVE_GB_ANTI_JOIN:
        remap(&graph->u.join.left, map, ctx);
        remap(&graph->u.join.right, map, ctx);
        break;
    case GROOVE_GB_RECURSIVE:
        remap(&graph->u.recursive.seed, map, ctx);
        remap(&graph->u.recursive.step, map, ctx);
        if (graph->u.recursive.step_witness) remap(&graph->u.recursive.step_witness, map, ctx);
        break;
    case GROOVE_GB_RECURSIVE_STEP_WITNESS:
        remap(&graph->u.recursive_step_witness.recursive, map, ctx);
        break;
    default:
        break;
    }
}

/* ---- binary / grouped constructors ---- */
static groove_GraphBuilder gb_join(u8 tag, groove_GraphBuilder left, groove_GraphBuilder right,
                                   const jz_str *left_on, u32 nl, const jz_str *right_on, u32 nr, u8 comparison) {
    groove_GraphBuilder g = gb_new(tag);
    g.u.join.left = groove__ivm__graph__arc_new(left);
    g.u.join.right = groove__ivm__graph__arc_new(right);
    groove__ivm__graph__fieldrefvec_from_names(&g.u.join.left_on, left_on, nl);
    groove__ivm__graph__fieldrefvec_from_names(&g.u.join.right_on, right_on, nr);
    g.u.join.comparison = comparison;
    return g;
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__join(groove_GraphBuilder left, groove_GraphBuilder right, const jz_str *left_on, u32 nl, const jz_str *right_on, u32 nr) {
    return gb_join(GROOVE_GB_JOIN, left, right, left_on, nl, right_on, nr, GROOVE_CMP_EXACT);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__policy_join(groove_GraphBuilder left, groove_GraphBuilder right, const jz_str *left_on, u32 nl, const jz_str *right_on, u32 nr) {
    return gb_join(GROOVE_GB_JOIN, left, right, left_on, nl, right_on, nr, GROOVE_CMP_POLICY);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__semi_join(groove_GraphBuilder left, groove_GraphBuilder right, const jz_str *left_on, u32 nl, const jz_str *right_on, u32 nr) {
    return gb_join(GROOVE_GB_SEMI_JOIN, left, right, left_on, nl, right_on, nr, GROOVE_CMP_EXACT);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__anti_join(groove_GraphBuilder left, groove_GraphBuilder right, const jz_str *left_on, u32 nl, const jz_str *right_on, u32 nr) {
    return gb_join(GROOVE_GB_ANTI_JOIN, left, right, left_on, nl, right_on, nr, GROOVE_CMP_EXACT);
}
bool groove__ivm__graph__GraphBuilder__lookup(groove_GraphBuilder right, groove_GraphBuilder keys, const jz_str *keys_on, u32 n, groove_GraphBuilder *out) {
    if (right.tag != GROOVE_GB_TABLE) {
        groove__ivm__graph__GraphBuilder__drop(&right);
        groove__ivm__graph__GraphBuilder__drop(&keys);
        return false;
    }
    groove_GraphBuilder g = gb_new(GROOVE_GB_LOOKUP);
    g.u.lookup.keys = groove__ivm__graph__arc_new(keys);
    groove__ivm__graph__fieldrefvec_from_names(&g.u.lookup.keys_on, keys_on, n);
    g.u.lookup.table = right.u.table.table;
    g.u.lookup.has_scan = right.u.table.has_scan;
    g.u.lookup.scan = right.u.table.scan;
    g.u.lookup.has_variant_projection = right.u.table.has_variant_projection;
    g.u.lookup.variant_projection = right.u.table.variant_projection;
    *out = g;
    return true;
}
static groove_GraphBuilder gb_arg_by(u8 tag, groove_GraphBuilder input, const jz_str *group_cols, u32 ng, const jz_str *order_cols, u32 no) {
    groove_GraphBuilder g = gb_new(tag);
    g.u.arg_by.input = groove__ivm__graph__arc_new(input);
    groove__ivm__graph__fieldrefvec_from_names(&g.u.arg_by.group_cols, group_cols, ng);
    groove__ivm__graph__fieldrefvec_from_names(&g.u.arg_by.order_cols, order_cols, no);
    return g;
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__arg_max_by(groove_GraphBuilder input, const jz_str *group_cols, u32 ng, const jz_str *order_cols, u32 no) {
    return gb_arg_by(GROOVE_GB_ARG_MAX_BY, input, group_cols, ng, order_cols, no);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__arg_min_by(groove_GraphBuilder input, const jz_str *group_cols, u32 ng, const jz_str *order_cols, u32 no) {
    return gb_arg_by(GROOVE_GB_ARG_MIN_BY, input, group_cols, ng, order_cols, no);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__top_by(groove_GraphBuilder input, const jz_str *group_cols, u32 ng, groove_TopByOrderVec order_cols,
                                                            const jz_str *tie_cols, u32 nt, u64 offset, groove_TopByLimit limit) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_TOP_BY);
    g.u.top_by.input = groove__ivm__graph__arc_new(input);
    groove__ivm__graph__fieldrefvec_from_names(&g.u.top_by.group_cols, group_cols, ng);
    g.u.top_by.order_cols = order_cols;
    groove__ivm__graph__fieldrefvec_from_names(&g.u.top_by.tie_cols, tie_cols, nt);
    g.u.top_by.offset = offset;
    g.u.top_by.limit = limit;
    return g;
}
static groove_GraphBuilder gb_collect(groove_GraphBuilder input, u8 mode, const jz_str *group_cols, u32 ng,
                                      groove_CollectByFieldVec parent_fields, groove_CollectByFieldVec child_fields,
                                      jz_str collection_field, groove_CollectBySlotBuilderVec slots,
                                      groove_CollectByFieldVec tuple_fields, const jz_str *occurrence_id_cols, u32 no,
                                      groove_TopByOrderVec order_cols, const jz_str *tie_cols, u32 nt, u64 offset, groove_TopByLimit limit) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_COLLECT_BY);
    g.u.collect_by.input = groove__ivm__graph__arc_new(input);
    groove_CollectByBuilder *c = JZ_NEW(groove_CollectByBuilder);
    c->mode = mode;
    groove__ivm__graph__fieldrefvec_from_names(&c->group_cols, group_cols, ng);
    c->parent_fields = parent_fields;
    c->child_fields = child_fields;
    c->collection_field = collection_field;
    c->slots = slots;
    c->tuple_fields = tuple_fields;
    groove__ivm__graph__fieldrefvec_from_names(&c->occurrence_id_cols, occurrence_id_cols, no);
    c->order_cols = order_cols;
    groove__ivm__graph__fieldrefvec_from_names(&c->tie_cols, tie_cols, nt);
    c->offset = offset;
    c->limit = limit;
    g.u.collect_by.collect = c;
    return g;
}
static const groove_CollectByFieldVec NO_CB_FIELDS = { NULL, 0, 0 };
static const groove_CollectBySlotBuilderVec NO_SLOTS = { NULL, 0, 0 };
groove_GraphBuilder groove__ivm__graph__GraphBuilder__collect_by(groove_GraphBuilder input, const jz_str *group_cols, u32 ng, groove_CollectByFieldVec parent_fields,
                                                                groove_CollectByFieldVec child_fields, jz_str collection_field, groove_TopByOrderVec order_cols,
                                                                const jz_str *tie_cols, u32 nt, u64 offset, groove_TopByLimit limit) {
    return gb_collect(input, GROOVE_COLLECT_MODE_COLLECT, group_cols, ng, parent_fields, child_fields, collection_field, NO_SLOTS,
                      NO_CB_FIELDS, NULL, 0, order_cols, tie_cols, nt, offset, limit);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__collect_by_tree(groove_GraphBuilder input, const jz_str *group_cols, u32 ng,
                                                                     groove_CollectByFieldVec parent_fields, groove_CollectBySlotBuilderVec slots) {
    groove_TopByOrderVec no_order = {0};
    groove_TopByLimit unbounded = { GROOVE_TOPBY_UNBOUNDED, 0 };
    return groove__ivm__graph__GraphBuilder__collect_by_tree_ordered(input, group_cols, ng, parent_fields, slots, no_order, NULL, 0, 0, unbounded);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__collect_by_tree_ordered(groove_GraphBuilder input, const jz_str *group_cols, u32 ng,
                                                                             groove_CollectByFieldVec parent_fields, groove_CollectBySlotBuilderVec slots,
                                                                             groove_TopByOrderVec order_cols, const jz_str *tie_cols, u32 nt,
                                                                             u64 offset, groove_TopByLimit limit) {
    return gb_collect(input, GROOVE_COLLECT_MODE_COLLECT, group_cols, ng, parent_fields, NO_CB_FIELDS, jz_str_from(""), slots,
                      NO_CB_FIELDS, NULL, 0, order_cols, tie_cols, nt, offset, limit);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__collect_root_ordered(groove_GraphBuilder input, const jz_str *group_cols, u32 ng,
                                                                          groove_CollectByFieldVec parent_fields, groove_TopByOrderVec order_cols,
                                                                          const jz_str *tie_cols, u32 nt, u64 offset, groove_TopByLimit limit) {
    return gb_collect(input, GROOVE_COLLECT_MODE_ROOT, group_cols, ng, parent_fields, NO_CB_FIELDS, jz_str_from(""), NO_SLOTS,
                      NO_CB_FIELDS, NULL, 0, order_cols, tie_cols, nt, offset, limit);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__collect_by_expand(groove_GraphBuilder input, const jz_str *group_cols, u32 ng,
                                                                       groove_CollectByFieldVec tuple_fields, const jz_str *occurrence_id_cols, u32 no,
                                                                       groove_TopByOrderVec order_cols, const jz_str *tie_cols, u32 nt,
                                                                       u64 offset, groove_TopByLimit limit) {
    return gb_collect(input, GROOVE_COLLECT_MODE_EXPAND, group_cols, ng, NO_CB_FIELDS, NO_CB_FIELDS, jz_str_from(""), NO_SLOTS,
                      tuple_fields, occurrence_id_cols, no, order_cols, tie_cols, nt, offset, limit);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__expand_by(groove_GraphBuilder input, const jz_str *group_cols, u32 ng,
                                                               groove_CollectByFieldVec tuple_fields, const jz_str *occurrence_id_cols, u32 no,
                                                               groove_TopByOrderVec order_cols, const jz_str *tie_cols, u32 nt,
                                                               u64 offset, groove_TopByLimit limit) {
    return groove__ivm__graph__GraphBuilder__collect_by_expand(input, group_cols, ng, tuple_fields, occurrence_id_cols, no,
                                                               order_cols, tie_cols, nt, offset, limit);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__aggregate(groove_GraphBuilder input, const jz_str *group_cols, u32 ng, groove_AggregateExprVec aggregates) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_AGGREGATE);
    g.u.aggregate.input = groove__ivm__graph__arc_new(input);
    groove__ivm__graph__fieldrefvec_from_names(&g.u.aggregate.group_cols, group_cols, ng);
    g.u.aggregate.aggregates = aggregates;
    return g;
}
static groove_GraphBuilder gb_filter(groove_GraphBuilder self, groove_PredicateExpr predicate, u8 comparison) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_FILTER);
    g.u.filter.input = groove__ivm__graph__arc_new(self);
    g.u.filter.predicate = predicate;
    g.u.filter.comparison = comparison;
    return g;
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__filter(groove_GraphBuilder self, groove_PredicateExpr predicate) {
    return gb_filter(self, predicate, GROOVE_CMP_EXACT);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__policy_filter(groove_GraphBuilder self, groove_PredicateExpr predicate) {
    return gb_filter(self, predicate, GROOVE_CMP_POLICY);
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__unwrap_nullable(groove_GraphBuilder self, jz_str field) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_UNWRAP_NULLABLE);
    g.u.unwrap_nullable.input = groove__ivm__graph__arc_new(self);
    g.u.unwrap_nullable.field = groove__ivm__graph__FieldRef__name(field);
    return g;
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__unnest(groove_GraphBuilder self, jz_str array_field, jz_str element_field) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_UNNEST);
    g.u.unnest.input = groove__ivm__graph__arc_new(self);
    g.u.unnest.array_field = groove__ivm__graph__FieldRef__name(array_field);
    g.u.unnest.element_field = element_field;
    return g;
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__variant_project(groove_GraphBuilder self, jz_str field, jz_str case_) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_VARIANT_PROJECT);
    g.u.variant_project.input = groove__ivm__graph__arc_new(self);
    g.u.variant_project.field = groove__ivm__graph__FieldRef__name(field);
    g.u.variant_project.case_ = case_;
    return g;
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__project(groove_GraphBuilder self, const jz_str *fields, u32 n) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_PROJECT);
    g.u.project.input = groove__ivm__graph__arc_new(self);
    jz_vec_reserve(&g.u.project.fields, n, sizeof(groove_ProjectField));
    for (u32 i = 0; i < n; i++) JZ_VEC_PUSH(g.u.project.fields, groove__ivm__graph__ProjectField__named(jz_str_clone(&fields[i])));
    return g;
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__project_fields(groove_GraphBuilder self, groove_ProjectFieldVec fields) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_PROJECT);
    g.u.project.input = groove__ivm__graph__arc_new(self);
    g.u.project.fields = fields;
    return g;
}
groove_GraphBuilder groove__ivm__graph__GraphBuilder__streaming_checksum(groove_GraphBuilder self, jz_str field, jz_str output_field,
                                                                        u32 window_bytes, u32 max_bytes_per_turn) {
    groove_GraphBuilder g = gb_new(GROOVE_GB_STREAMING_CHECKSUM);
    g.u.streaming_checksum.input = groove__ivm__graph__arc_new(self);
    g.u.streaming_checksum.field = groove__ivm__graph__FieldRef__name(field);
    g.u.streaming_checksum.output_field = output_field;
    g.u.streaming_checksum.window_bytes = window_bytes;
    g.u.streaming_checksum.max_bytes_per_turn = max_bytes_per_turn;
    return g;
}

/* ======================================================================
 * ProjectField constructors
 * ====================================================================== */
static groove_FieldIdentity identity_name(const jz_str *name) {
    groove_FieldIdentity id; memset(&id, 0, sizeof id);
    id.tag = GROOVE_FIELD_ID_NAME;
    id.name = jz_str_clone(name);
    return id;
}
static groove_ProjectField pf_make(groove_ProjectExpr expression, jz_str output_name) {
    groove_ProjectField f;
    f.expression = expression;
    f.output_identity = identity_name(&output_name);
    f.output_name = output_name;
    return f;
}
static groove_ProjectExpr pexpr_field(u8 tag, groove_FieldRef field) {
    groove_ProjectExpr e; memset(&e, 0, sizeof e);
    e.tag = tag;
    e.u.field = field;
    return e;
}
groove_ProjectField groove__ivm__graph__ProjectField__record_field(jz_str source, const jz_str *path, u32 n, jz_str output_name) {
    groove_ProjectExpr e; memset(&e, 0, sizeof e);
    e.tag = GROOVE_PEXPR_RECORD_FIELD;
    e.u.record_field.source = groove__ivm__graph__FieldRef__name(source);
    jz_vec_reserve(&e.u.record_field.path, n, sizeof(jz_str));
    for (u32 i = 0; i < n; i++) JZ_VEC_PUSH(e.u.record_field.path, jz_str_clone(&path[i]));
    return pf_make(e, output_name);
}
groove_ProjectField groove__ivm__graph__ProjectField__named(jz_str name) {
    groove_ProjectField f;
    f.expression = pexpr_field(GROOVE_PEXPR_FIELD, groove__ivm__graph__FieldRef__name(jz_str_clone(&name)));
    f.output_identity = identity_name(&name);
    f.output_name = name;
    return f;
}
groove_ProjectField groove__ivm__graph__ProjectField__named_with_identity(jz_str name, groove_FieldIdentity output_identity) {
    groove_ProjectField f;
    f.expression = pexpr_field(GROOVE_PEXPR_FIELD, groove__ivm__graph__FieldRef__name(jz_str_clone(&name)));
    f.output_identity = output_identity;
    f.output_name = name;
    return f;
}
groove_ProjectField groove__ivm__graph__ProjectField__renamed(jz_str source_name, jz_str output_name) {
    return pf_make(pexpr_field(GROOVE_PEXPR_FIELD, groove__ivm__graph__FieldRef__name(source_name)), output_name);
}
groove_ProjectField groove__ivm__graph__ProjectField__renamed_resolved(u32 source_idx, jz_str output_name) {
    return pf_make(pexpr_field(GROOVE_PEXPR_FIELD, groove__ivm__graph__FieldRef__resolved(source_idx)), output_name);
}
groove_ProjectField groove__ivm__graph__ProjectField__nullable_flat_resolved(u32 source_idx, jz_str output_name) {
    return pf_make(pexpr_field(GROOVE_PEXPR_NULLABLE_FLAT, groove__ivm__graph__FieldRef__resolved(source_idx)), output_name);
}
groove_ProjectField groove__ivm__graph__ProjectField__renamed_with_identity(jz_str source_name, jz_str output_name, groove_FieldIdentity output_identity) {
    groove_ProjectField f;
    f.expression = pexpr_field(GROOVE_PEXPR_FIELD, groove__ivm__graph__FieldRef__name(source_name));
    f.output_name = output_name;
    f.output_identity = output_identity;
    return f;
}
static groove_ProjectExpr pexpr_literal(groove_LiteralValue value) {
    groove_ProjectExpr e; memset(&e, 0, sizeof e);
    e.tag = GROOVE_PEXPR_LITERAL;
    e.u.literal = value;
    return e;
}
groove_ProjectField groove__ivm__graph__ProjectField__literal(jz_str output_name, groove_LiteralValue value) {
    return pf_make(pexpr_literal(value), output_name);
}
groove_ProjectField groove__ivm__graph__ProjectField__literal_with_identity(jz_str output_name, groove_LiteralValue value, groove_FieldIdentity output_identity) {
    groove_ProjectField f;
    f.expression = pexpr_literal(value);
    f.output_name = output_name;
    f.output_identity = output_identity;
    return f;
}
groove_ProjectField groove__ivm__graph__ProjectField__literal_typed(jz_str output_name, groove_LiteralValue value, groove_ValueType value_type) {
    groove_ProjectExpr e; memset(&e, 0, sizeof e);
    e.tag = GROOVE_PEXPR_TYPED_LITERAL;
    e.u.typed_literal.value = value;
    e.u.typed_literal.value_type = value_type;
    return pf_make(e, output_name);
}
groove_ProjectField groove__ivm__graph__ProjectField__null(jz_str output_name) {
    groove_ValueType bytes; memset(&bytes, 0, sizeof bytes);
    bytes.tag = GROOVE_VT_BYTES;
    groove_ValueType t; memset(&t, 0, sizeof t);
    t.tag = GROOVE_VT_NULLABLE;
    t.u.inner = jz_memdup(&bytes, sizeof bytes);
    return groove__ivm__graph__ProjectField__null_typed(output_name, t);
}
groove_ProjectField groove__ivm__graph__ProjectField__null_typed(jz_str output_name, groove_ValueType value_type) {
    groove_ProjectExpr e; memset(&e, 0, sizeof e);
    e.tag = GROOVE_PEXPR_NULL;
    e.u.null_type = value_type;
    return pf_make(e, output_name);
}
groove_ProjectField groove__ivm__graph__ProjectField__nullable(jz_str source_name, jz_str output_name) {
    return pf_make(pexpr_field(GROOVE_PEXPR_NULLABLE, groove__ivm__graph__FieldRef__name(source_name)), output_name);
}
groove_ProjectField groove__ivm__graph__ProjectField__nullable_resolved(u32 source_idx, jz_str output_name) {
    return pf_make(pexpr_field(GROOVE_PEXPR_NULLABLE, groove__ivm__graph__FieldRef__resolved(source_idx)), output_name);
}
groove_ProjectField groove__ivm__graph__ProjectField__nullable_flat(jz_str source_name, jz_str output_name) {
    return pf_make(pexpr_field(GROOVE_PEXPR_NULLABLE_FLAT, groove__ivm__graph__FieldRef__name(source_name)), output_name);
}
groove_ProjectField groove__ivm__graph__ProjectField__enum_tag_remap(jz_str source_name, jz_str output_name, groove_OptU8Vec tags) {
    groove_ProjectExpr e; memset(&e, 0, sizeof e);
    e.tag = GROOVE_PEXPR_ENUM_TAG_REMAP;
    e.u.enum_tag_remap.source = groove__ivm__graph__FieldRef__name(source_name);
    e.u.enum_tag_remap.tags = tags;
    return pf_make(e, output_name);
}
groove_ProjectField groove__ivm__graph__ProjectField__enum_remap(jz_str source_name, jz_str output_name, groove_OptU32Vec tags) {
    groove_ProjectExpr e; memset(&e, 0, sizeof e);
    e.tag = GROOVE_PEXPR_ENUM_REMAP;
    e.u.enum_remap.source = groove__ivm__graph__FieldRef__name(source_name);
    e.u.enum_remap.tags = tags;
    return pf_make(e, output_name);
}
static groove_ProjectField pf_recursive_remap(jz_str source_name, jz_str output_name, groove_ValueType target,
                                              groove_RecursiveEnumRemaps remaps, bool omit_unrepresentable) {
    groove_ProjectExpr e; memset(&e, 0, sizeof e);
    e.tag = GROOVE_PEXPR_RECURSIVE_ENUM_REMAP;
    e.u.recursive.source = groove__ivm__graph__FieldRef__name(source_name);
    e.u.recursive.target = target;
    e.u.recursive.remaps = remaps;
    e.u.recursive.omit_unrepresentable = omit_unrepresentable;
    return pf_make(e, output_name);
}
groove_ProjectField groove__ivm__graph__ProjectField__recursive_enum_remap(jz_str source_name, jz_str output_name, groove_ValueType target, groove_RecursiveEnumRemaps remaps) {
    return pf_recursive_remap(source_name, output_name, target, remaps, false);
}
groove_ProjectField groove__ivm__graph__ProjectField__recursive_enum_remap_omitting_unrepresentable(jz_str source_name, jz_str output_name,
                                                                                                   groove_ValueType target, groove_RecursiveEnumRemaps remaps) {
    return pf_recursive_remap(source_name, output_name, target, remaps, true);
}
const groove_FieldRef *groove__ivm__graph__ProjectField__source(const groove_ProjectField *self) {
    switch (self->expression.tag) {
    case GROOVE_PEXPR_FIELD: case GROOVE_PEXPR_NULLABLE: case GROOVE_PEXPR_NULLABLE_FLAT: return &self->expression.u.field;
    case GROOVE_PEXPR_RECORD_FIELD: return &self->expression.u.record_field.source;
    case GROOVE_PEXPR_ENUM_TAG_REMAP: return &self->expression.u.enum_tag_remap.source;
    case GROOVE_PEXPR_ENUM_REMAP: return &self->expression.u.enum_remap.source;
    case GROOVE_PEXPR_RECURSIVE_ENUM_REMAP: return &self->expression.u.recursive.source;
    case GROOVE_PEXPR_LITERAL: case GROOVE_PEXPR_TYPED_LITERAL: case GROOVE_PEXPR_NULL: case GROOVE_PEXPR_TEMPLATE_ARGUMENT:
    default: return NULL;
    }
}

/* ======================================================================
 * CollectByField / TopByOrder constructors
 * ====================================================================== */
groove_CollectByField groove__ivm__graph__CollectByField__named(jz_str field) {
    groove_CollectByField f;
    f.field = groove__ivm__graph__FieldRef__name(jz_str_clone(&field));
    f.output_name = field;
    f.unwrap_nullable = false;
    return f;
}
groove_CollectByField groove__ivm__graph__CollectByField__renamed(jz_str field, jz_str output_name) {
    groove_CollectByField f = { groove__ivm__graph__FieldRef__name(field), output_name, false };
    return f;
}
groove_CollectByField groove__ivm__graph__CollectByField__renamed_unwrap_nullable(jz_str field, jz_str output_name) {
    groove_CollectByField f = { groove__ivm__graph__FieldRef__name(field), output_name, true };
    return f;
}
groove_TopByOrder groove__ivm__graph__TopByOrder__asc(jz_str field) {
    groove_TopByOrder o = { groove__ivm__graph__FieldRef__name(field), GROOVE_DIR_ASC };
    return o;
}
groove_TopByOrder groove__ivm__graph__TopByOrder__desc(jz_str field) {
    groove_TopByOrder o = { groove__ivm__graph__FieldRef__name(field), GROOVE_DIR_DESC };
    return o;
}

/* fn collect_projection_output_type(input, projection) -> Option<ValueType> */
static bool collect_projection_output_type(const groove_ValueType *input, const groove_CollectByProjection *projection, groove_ValueType *out) {
    if (!projection->unwrap_nullable) {
        groove__records__values__ValueType__Clone__clone(out, input);
        return true;
    }
    if (input->tag == GROOVE_VT_NULLABLE) groove__records__values__ValueType__Clone__clone(out, input->u.inner);
    else groove__records__values__ValueType__Clone__clone(out, input);
    return true;
}

/* ======================================================================
 * StableNodeHasher (FNV-1a over the derived Hash byte stream)
 * ====================================================================== */
typedef struct StableNodeHasher { u64 hash; } StableNodeHasher;
static void StableNodeHasher__Hasher__write(void *self, const u8 *bytes, u32 n) {
    StableNodeHasher *h = self;
    for (u32 i = 0; i < n; i++) {
        h->hash ^= (u64)bytes[i];
        h->hash *= 0x00000100000001b3ull;
    }
}
static StableNodeHasher StableNodeHasher__Default__default(void) {
    StableNodeHasher h = { 0xcbf29ce484222325ull };
    return h;
}

/* ======================================================================
 * ArrangeOp (derives)
 * ====================================================================== */
static void arrange_clone(groove_ArrangeOp *dst, const groove_ArrangeOp *src) {
    groove__ivm__strvec_clone(&dst->fields, &src->fields);
    dst->comparison = src->comparison;
}
static bool arrange_eq(const groove_ArrangeOp *a, const groove_ArrangeOp *b) {
    return groove__ivm__strvec_eq(&a->fields, &b->fields) && a->comparison == b->comparison;
}
static void arrange_hash(const groove_ArrangeOp *v, groove_Hasher *h) {
    groove__ivm__strvec_hash(&v->fields, h);
    groove_hash_discr(h, v->comparison);
}
static void arrange_debug(jz_str *out, const groove_ArrangeOp *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "ArrangeOp");
    groove__fmt__DebugStruct__field(&d, "fields"); groove__fmt__debug_str_list(out, v->fields.ptr, v->fields.len);
    groove__fmt__DebugStruct__field(&d, "comparison"); groove__ivm__op_types__dbg_cmp(out, v->comparison);
    groove__fmt__DebugStruct__finish(&d);
}

/* ======================================================================
 * OpType (derives)
 * ====================================================================== */
void groove__ivm__graph__OpType__Clone__clone(groove_OpType *dst, const groove_OpType *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    switch (src->tag) {
    case GROOVE_OP_TABLE_SOURCE: groove__ivm__op_types__TableSourceOp__Clone__clone(&dst->u.table_source, &src->u.table_source); break;
    case GROOVE_OP_INDEX_SOURCE: groove__ivm__op_types__IndexSourceOp__Clone__clone(&dst->u.index_source, &src->u.index_source); break;
    case GROOVE_OP_INLINE_RECORDS: groove__ivm__op_types__InlineRecordsOp__Clone__clone(&dst->u.inline_records, &src->u.inline_records); break;
    case GROOVE_OP_FRONTIER_SOURCE: dst->u.frontier_source.binding = jz_str_clone(&src->u.frontier_source.binding); break;
    case GROOVE_OP_BINDING_SOURCE: groove__ivm__op_types__BindingSourceKey__Clone__clone(&dst->u.binding_source.key, &src->u.binding_source.key); break;
    case GROOVE_OP_ARRANGE: arrange_clone(&dst->u.arrange, &src->u.arrange); break;
    case GROOVE_OP_ARG_MAX_BY: case GROOVE_OP_ARG_MIN_BY: groove__ivm__op_types__ArgMaxByOp__Clone__clone(&dst->u.arg_by, &src->u.arg_by); break;
    case GROOVE_OP_TOP_BY: groove__ivm__op_types__TopByOp__Clone__clone(&dst->u.top_by, &src->u.top_by); break;
    case GROOVE_OP_COLLECT_BY:
        dst->u.collect_by = JZ_NEW(groove_CollectByOp);
        groove__ivm__op_types__CollectByOp__Clone__clone(dst->u.collect_by, src->u.collect_by);
        break;
    case GROOVE_OP_RECURSIVE: groove__ivm__op_types__RecursiveOp__Clone__clone(&dst->u.recursive, &src->u.recursive); break;
    case GROOVE_OP_RECURSIVE_STEP_WITNESS: break;
    case GROOVE_OP_PERSIST: groove__ivm__op_types__PersistOp__Clone__clone(&dst->u.persist, &src->u.persist); break;
    case GROOVE_OP_FILTER: groove__ivm__op_types__FilterOp__Clone__clone(&dst->u.filter, &src->u.filter); break;
    case GROOVE_OP_MAP_PROJECT: groove__ivm__op_types__MapProjectOp__Clone__clone(&dst->u.map_project, &src->u.map_project); break;
    case GROOVE_OP_STREAMING_CHECKSUM: groove__ivm__op_types__StreamingChecksumOp__Clone__clone(&dst->u.streaming_checksum, &src->u.streaming_checksum); break;
    case GROOVE_OP_UNWRAP_NULLABLE: groove__ivm__op_types__UnwrapNullableOp__Clone__clone(&dst->u.unwrap_nullable, &src->u.unwrap_nullable); break;
    case GROOVE_OP_UNNEST: groove__ivm__op_types__UnnestOp__Clone__clone(&dst->u.unnest, &src->u.unnest); break;
    case GROOVE_OP_VARIANT_PROJECT: groove__ivm__op_types__VariantProjectOp__Clone__clone(&dst->u.variant_project, &src->u.variant_project); break;
    case GROOVE_OP_INDEX_BY: groove__ivm__op_types__IndexByOp__Clone__clone(&dst->u.index_by, &src->u.index_by); break;
    case GROOVE_OP_JOIN: case GROOVE_OP_SEMI_JOIN: case GROOVE_OP_ANTI_JOIN: groove__ivm__op_types__JoinOp__Clone__clone(&dst->u.join, &src->u.join); break;
    case GROOVE_OP_LOOKUP: groove__ivm__op_types__LookupOp__Clone__clone(&dst->u.lookup, &src->u.lookup); break;
    case GROOVE_OP_UNION: case GROOVE_OP_NEGATE: case GROOVE_OP_DISTINCT: break;
    case GROOVE_OP_AGGREGATE: groove__ivm__op_types__AggregateOp__Clone__clone(&dst->u.aggregate, &src->u.aggregate); break;
    }
}
void groove__ivm__graph__OpType__drop(groove_OpType *v) {
    switch (v->tag) {
    case GROOVE_OP_TABLE_SOURCE: groove__ivm__op_types__TableSourceOp__drop(&v->u.table_source); break;
    case GROOVE_OP_INDEX_SOURCE: groove__ivm__op_types__IndexSourceOp__drop(&v->u.index_source); break;
    case GROOVE_OP_INLINE_RECORDS: groove__ivm__op_types__InlineRecordsOp__drop(&v->u.inline_records); break;
    case GROOVE_OP_FRONTIER_SOURCE: jz_str_drop(&v->u.frontier_source.binding); break;
    case GROOVE_OP_BINDING_SOURCE: groove__ivm__op_types__BindingSourceKey__drop(&v->u.binding_source.key); break;
    case GROOVE_OP_ARRANGE: groove__ivm__strvec_drop(&v->u.arrange.fields); break;
    case GROOVE_OP_ARG_MAX_BY: case GROOVE_OP_ARG_MIN_BY: groove__ivm__op_types__ArgMaxByOp__drop(&v->u.arg_by); break;
    case GROOVE_OP_TOP_BY: groove__ivm__op_types__TopByOp__drop(&v->u.top_by); break;
    case GROOVE_OP_COLLECT_BY: groove__ivm__op_types__CollectByOp__drop(v->u.collect_by); jz_free(v->u.collect_by); break;
    case GROOVE_OP_RECURSIVE: groove__ivm__op_types__RecursiveOp__drop(&v->u.recursive); break;
    case GROOVE_OP_RECURSIVE_STEP_WITNESS: break;
    case GROOVE_OP_PERSIST: groove__ivm__op_types__PersistOp__drop(&v->u.persist); break;
    case GROOVE_OP_FILTER: groove__ivm__op_types__FilterOp__drop(&v->u.filter); break;
    case GROOVE_OP_MAP_PROJECT: groove__ivm__op_types__MapProjectOp__drop(&v->u.map_project); break;
    case GROOVE_OP_STREAMING_CHECKSUM: groove__ivm__op_types__StreamingChecksumOp__drop(&v->u.streaming_checksum); break;
    case GROOVE_OP_UNWRAP_NULLABLE: groove__ivm__op_types__UnwrapNullableOp__drop(&v->u.unwrap_nullable); break;
    case GROOVE_OP_UNNEST: groove__ivm__op_types__UnnestOp__drop(&v->u.unnest); break;
    case GROOVE_OP_VARIANT_PROJECT: groove__ivm__op_types__VariantProjectOp__drop(&v->u.variant_project); break;
    case GROOVE_OP_INDEX_BY: groove__ivm__op_types__IndexByOp__drop(&v->u.index_by); break;
    case GROOVE_OP_JOIN: case GROOVE_OP_SEMI_JOIN: case GROOVE_OP_ANTI_JOIN: groove__ivm__op_types__JoinOp__drop(&v->u.join); break;
    case GROOVE_OP_LOOKUP: groove__ivm__op_types__LookupOp__drop(&v->u.lookup); break;
    case GROOVE_OP_UNION: case GROOVE_OP_NEGATE: case GROOVE_OP_DISTINCT: break;
    case GROOVE_OP_AGGREGATE: groove__ivm__op_types__AggregateOp__drop(&v->u.aggregate); break;
    }
}
bool groove__ivm__graph__OpType__PartialEq__eq(const groove_OpType *a, const groove_OpType *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case GROOVE_OP_TABLE_SOURCE: return groove__ivm__op_types__TableSourceOp__PartialEq__eq(&a->u.table_source, &b->u.table_source);
    case GROOVE_OP_INDEX_SOURCE: return groove__ivm__op_types__IndexSourceOp__PartialEq__eq(&a->u.index_source, &b->u.index_source);
    case GROOVE_OP_INLINE_RECORDS: return groove__ivm__op_types__InlineRecordsOp__PartialEq__eq(&a->u.inline_records, &b->u.inline_records);
    case GROOVE_OP_FRONTIER_SOURCE: return jz_str_eq(&a->u.frontier_source.binding, &b->u.frontier_source.binding);
    case GROOVE_OP_BINDING_SOURCE: return groove__ivm__op_types__BindingSourceKey__PartialEq__eq(&a->u.binding_source.key, &b->u.binding_source.key);
    case GROOVE_OP_ARRANGE: return arrange_eq(&a->u.arrange, &b->u.arrange);
    case GROOVE_OP_ARG_MAX_BY: case GROOVE_OP_ARG_MIN_BY: return groove__ivm__op_types__ArgMaxByOp__PartialEq__eq(&a->u.arg_by, &b->u.arg_by);
    case GROOVE_OP_TOP_BY: return groove__ivm__op_types__TopByOp__PartialEq__eq(&a->u.top_by, &b->u.top_by);
    case GROOVE_OP_COLLECT_BY: return groove__ivm__op_types__CollectByOp__PartialEq__eq(a->u.collect_by, b->u.collect_by);
    case GROOVE_OP_RECURSIVE: return groove__ivm__op_types__RecursiveOp__PartialEq__eq(&a->u.recursive, &b->u.recursive);
    case GROOVE_OP_RECURSIVE_STEP_WITNESS: return true;
    case GROOVE_OP_PERSIST: return groove__ivm__op_types__PersistOp__PartialEq__eq(&a->u.persist, &b->u.persist);
    case GROOVE_OP_FILTER: return groove__ivm__op_types__FilterOp__PartialEq__eq(&a->u.filter, &b->u.filter);
    case GROOVE_OP_MAP_PROJECT: return groove__ivm__op_types__MapProjectOp__PartialEq__eq(&a->u.map_project, &b->u.map_project);
    case GROOVE_OP_STREAMING_CHECKSUM: return groove__ivm__op_types__StreamingChecksumOp__PartialEq__eq(&a->u.streaming_checksum, &b->u.streaming_checksum);
    case GROOVE_OP_UNWRAP_NULLABLE: return groove__ivm__op_types__UnwrapNullableOp__PartialEq__eq(&a->u.unwrap_nullable, &b->u.unwrap_nullable);
    case GROOVE_OP_UNNEST: return groove__ivm__op_types__UnnestOp__PartialEq__eq(&a->u.unnest, &b->u.unnest);
    case GROOVE_OP_VARIANT_PROJECT: return groove__ivm__op_types__VariantProjectOp__PartialEq__eq(&a->u.variant_project, &b->u.variant_project);
    case GROOVE_OP_INDEX_BY: return groove__ivm__op_types__IndexByOp__PartialEq__eq(&a->u.index_by, &b->u.index_by);
    case GROOVE_OP_JOIN: case GROOVE_OP_SEMI_JOIN: case GROOVE_OP_ANTI_JOIN: return groove__ivm__op_types__JoinOp__PartialEq__eq(&a->u.join, &b->u.join);
    case GROOVE_OP_LOOKUP: return groove__ivm__op_types__LookupOp__PartialEq__eq(&a->u.lookup, &b->u.lookup);
    case GROOVE_OP_UNION: case GROOVE_OP_NEGATE: case GROOVE_OP_DISTINCT: return true;
    case GROOVE_OP_AGGREGATE: return groove__ivm__op_types__AggregateOp__PartialEq__eq(&a->u.aggregate, &b->u.aggregate);
    }
    return false;
}
void groove__ivm__graph__OpType__Hash__hash(const groove_OpType *v, groove_Hasher *h) {
    groove_hash_discr(h, v->tag);
    switch (v->tag) {
    case GROOVE_OP_TABLE_SOURCE: groove__ivm__op_types__TableSourceOp__Hash__hash(&v->u.table_source, h); break;
    case GROOVE_OP_INDEX_SOURCE: groove__ivm__op_types__IndexSourceOp__Hash__hash(&v->u.index_source, h); break;
    case GROOVE_OP_INLINE_RECORDS: groove__ivm__op_types__InlineRecordsOp__Hash__hash(&v->u.inline_records, h); break;
    case GROOVE_OP_FRONTIER_SOURCE: groove_hash_str(h, &v->u.frontier_source.binding); break;
    case GROOVE_OP_BINDING_SOURCE: groove__ivm__op_types__BindingSourceKey__Hash__hash(&v->u.binding_source.key, h); break;
    case GROOVE_OP_ARRANGE: arrange_hash(&v->u.arrange, h); break;
    case GROOVE_OP_ARG_MAX_BY: case GROOVE_OP_ARG_MIN_BY: groove__ivm__op_types__ArgMaxByOp__Hash__hash(&v->u.arg_by, h); break;
    case GROOVE_OP_TOP_BY: groove__ivm__op_types__TopByOp__Hash__hash(&v->u.top_by, h); break;
    case GROOVE_OP_COLLECT_BY: groove__ivm__op_types__CollectByOp__Hash__hash(v->u.collect_by, h); break;
    case GROOVE_OP_RECURSIVE: groove__ivm__op_types__RecursiveOp__Hash__hash(&v->u.recursive, h); break;
    case GROOVE_OP_RECURSIVE_STEP_WITNESS: break;
    case GROOVE_OP_PERSIST: groove__ivm__op_types__PersistOp__Hash__hash(&v->u.persist, h); break;
    case GROOVE_OP_FILTER: groove__ivm__op_types__FilterOp__Hash__hash(&v->u.filter, h); break;
    case GROOVE_OP_MAP_PROJECT: groove__ivm__op_types__MapProjectOp__Hash__hash(&v->u.map_project, h); break;
    case GROOVE_OP_STREAMING_CHECKSUM: groove__ivm__op_types__StreamingChecksumOp__Hash__hash(&v->u.streaming_checksum, h); break;
    case GROOVE_OP_UNWRAP_NULLABLE: groove__ivm__op_types__UnwrapNullableOp__Hash__hash(&v->u.unwrap_nullable, h); break;
    case GROOVE_OP_UNNEST: groove__ivm__op_types__UnnestOp__Hash__hash(&v->u.unnest, h); break;
    case GROOVE_OP_VARIANT_PROJECT: groove__ivm__op_types__VariantProjectOp__Hash__hash(&v->u.variant_project, h); break;
    case GROOVE_OP_INDEX_BY: groove__ivm__op_types__IndexByOp__Hash__hash(&v->u.index_by, h); break;
    case GROOVE_OP_JOIN: case GROOVE_OP_SEMI_JOIN: case GROOVE_OP_ANTI_JOIN: groove__ivm__op_types__JoinOp__Hash__hash(&v->u.join, h); break;
    case GROOVE_OP_LOOKUP: groove__ivm__op_types__LookupOp__Hash__hash(&v->u.lookup, h); break;
    case GROOVE_OP_UNION: case GROOVE_OP_NEGATE: case GROOVE_OP_DISTINCT: break;
    case GROOVE_OP_AGGREGATE: groove__ivm__op_types__AggregateOp__Hash__hash(&v->u.aggregate, h); break;
    }
}
void groove__ivm__graph__OpType__Debug__fmt(jz_str *out, const groove_OpType *v) {
    static const char *const NAMES[] = {
        "TableSource", "IndexSource", "InlineRecords", "FrontierSource", "BindingSource", "Arrange", "ArgMaxBy",
        "ArgMinBy", "TopBy", "CollectBy", "Recursive", "RecursiveStepWitness", "Persist", "Filter", "MapProject",
        "StreamingChecksum", "UnwrapNullable", "Unnest", "VariantProject", "IndexBy", "Join", "SemiJoin", "AntiJoin",
        "Lookup", "Union", "Negate", "Distinct", "Aggregate"
    };
    if (v->tag == GROOVE_OP_UNION || v->tag == GROOVE_OP_NEGATE || v->tag == GROOVE_OP_DISTINCT) {
        jz_str_push_cstr(out, NAMES[v->tag]);
        return;
    }
    groove__fmt__debug_tuple_begin(out, NAMES[v->tag]);
    switch (v->tag) {
    case GROOVE_OP_TABLE_SOURCE: groove__ivm__op_types__TableSourceOp__Debug__fmt(out, &v->u.table_source); break;
    case GROOVE_OP_INDEX_SOURCE: groove__ivm__op_types__IndexSourceOp__Debug__fmt(out, &v->u.index_source); break;
    case GROOVE_OP_INLINE_RECORDS: groove__ivm__op_types__InlineRecordsOp__Debug__fmt(out, &v->u.inline_records); break;
    case GROOVE_OP_FRONTIER_SOURCE: {
        groove_DebugStruct d = groove__fmt__debug_struct(out, "FrontierSourceOp");
        groove__fmt__DebugStruct__field(&d, "binding");
        groove__fmt__debug_tuple_begin(out, "FrontierName"); groove__fmt__debug_str(out, &v->u.frontier_source.binding); groove__fmt__debug_tuple_end(out);
        groove__fmt__DebugStruct__finish(&d);
        break;
    }
    case GROOVE_OP_BINDING_SOURCE: {
        groove_DebugStruct d = groove__fmt__debug_struct(out, "BindingSourceOp");
        groove__fmt__DebugStruct__field(&d, "key"); groove__ivm__op_types__BindingSourceKey__Debug__fmt(out, &v->u.binding_source.key);
        groove__fmt__DebugStruct__finish(&d);
        break;
    }
    case GROOVE_OP_ARRANGE: arrange_debug(out, &v->u.arrange); break;
    case GROOVE_OP_ARG_MAX_BY: case GROOVE_OP_ARG_MIN_BY: groove__ivm__op_types__ArgMaxByOp__Debug__fmt(out, &v->u.arg_by); break;
    case GROOVE_OP_TOP_BY: groove__ivm__op_types__TopByOp__Debug__fmt(out, &v->u.top_by); break;
    case GROOVE_OP_COLLECT_BY: groove__ivm__op_types__CollectByOp__Debug__fmt(out, v->u.collect_by); break;
    case GROOVE_OP_RECURSIVE: groove__ivm__op_types__RecursiveOp__Debug__fmt(out, &v->u.recursive); break;
    case GROOVE_OP_RECURSIVE_STEP_WITNESS: jz_str_push_cstr(out, "RecursiveStepWitnessOp"); break;
    case GROOVE_OP_PERSIST: groove__ivm__op_types__PersistOp__Debug__fmt(out, &v->u.persist); break;
    case GROOVE_OP_FILTER: groove__ivm__op_types__FilterOp__Debug__fmt(out, &v->u.filter); break;
    case GROOVE_OP_MAP_PROJECT: groove__ivm__op_types__MapProjectOp__Debug__fmt(out, &v->u.map_project); break;
    case GROOVE_OP_STREAMING_CHECKSUM: groove__ivm__op_types__StreamingChecksumOp__Debug__fmt(out, &v->u.streaming_checksum); break;
    case GROOVE_OP_UNWRAP_NULLABLE: groove__ivm__op_types__UnwrapNullableOp__Debug__fmt(out, &v->u.unwrap_nullable); break;
    case GROOVE_OP_UNNEST: groove__ivm__op_types__UnnestOp__Debug__fmt(out, &v->u.unnest); break;
    case GROOVE_OP_VARIANT_PROJECT: groove__ivm__op_types__VariantProjectOp__Debug__fmt(out, &v->u.variant_project); break;
    case GROOVE_OP_INDEX_BY: groove__ivm__op_types__IndexByOp__Debug__fmt(out, &v->u.index_by); break;
    case GROOVE_OP_JOIN: case GROOVE_OP_SEMI_JOIN: case GROOVE_OP_ANTI_JOIN: groove__ivm__op_types__JoinOp__Debug__fmt(out, &v->u.join); break;
    case GROOVE_OP_LOOKUP: groove__ivm__op_types__LookupOp__Debug__fmt(out, &v->u.lookup); break;
    case GROOVE_OP_AGGREGATE: groove__ivm__op_types__AggregateOp__Debug__fmt(out, &v->u.aggregate); break;
    }
    groove__fmt__debug_tuple_end(out);
}

/* ======================================================================
 * NodeOutput / NodeDescriptor
 * ====================================================================== */
groove_RecordDescriptor groove__ivm__graph__NodeOutput__records(groove_NodeOutput self) {
    switch (self.tag) {
    case GROOVE_NODE_OUTPUT_RECORDS: return self.records;
    default: return self.records; /* Arrangement(arrangement) => arrangement.records */
    }
}
groove_NodeOutput groove__ivm__graph__NodeOutput__From_RecordDescriptor__from(groove_RecordDescriptor d) {
    groove_NodeOutput o = { GROOVE_NODE_OUTPUT_RECORDS, d };
    return o;
}
static bool node_output_eq(const groove_NodeOutput *a, const groove_NodeOutput *b) {
    return a->tag == b->tag && a->records == b->records;
}
static void node_output_hash(const groove_NodeOutput *o, groove_Hasher *h) {
    groove_hash_discr(h, o->tag);
    hash_descriptor(h, o->records);
}
static void node_output_debug(jz_str *out, const groove_NodeOutput *o) {
    if (o->tag == GROOVE_NODE_OUTPUT_RECORDS) {
        groove__fmt__debug_tuple_begin(out, "Records");
        groove__records__RecordDescriptor__Debug__fmt(out, o->records);
    } else {
        groove__fmt__debug_tuple_begin(out, "Arrangement");
        groove_DebugStruct d = groove__fmt__debug_struct(out, "ArrangementDescriptor");
        groove__fmt__DebugStruct__field(&d, "records"); groove__records__RecordDescriptor__Debug__fmt(out, o->records);
        groove__fmt__DebugStruct__finish(&d);
    }
    groove__fmt__debug_tuple_end(out);
}

groove_NodeDescriptor groove__ivm__graph__NodeDescriptor__new(groove_OpType op, const groove_NodeId *inputs, u32 n, groove_NodeOutput output) {
    groove_NodeDescriptor d;
    d.operator_ = op;
    memset(&d.inputs, 0, sizeof d.inputs);
    jz_vec_extend(&d.inputs, inputs, n, sizeof(groove_NodeId));
    d.output = output;
    return d;
}
groove_NodeId groove__ivm__graph__NodeDescriptor__node_id(const groove_NodeDescriptor *self) {
    return groove__ivm__graph__NodeDescriptor__node_id_of(&self->operator_, self->inputs.ptr, self->inputs.len, &self->output);
}
/* The identity of a descriptor with these fields, hashed exactly as the
 * derived `Hash` does (fields in declaration order), without owning one. */
groove_NodeId groove__ivm__graph__NodeDescriptor__node_id_of(const groove_OpType *op, const groove_NodeId *inputs, u32 n, const groove_NodeOutput *output) {
    StableNodeHasher state = StableNodeHasher__Default__default();
    groove_Hasher hasher = { StableNodeHasher__Hasher__write, &state };
    groove__ivm__graph__OpType__Hash__hash(op, &hasher);
    groove_hash_usize(&hasher, n);
    for (u32 i = 0; i < n; i++) groove_hash_u64(&hasher, inputs[i]);
    node_output_hash(output, &hasher);
    return state.hash;
}
void groove__ivm__graph__NodeDescriptor__Clone__clone(groove_NodeDescriptor *dst, const groove_NodeDescriptor *src) {
    groove__ivm__graph__OpType__Clone__clone(&dst->operator_, &src->operator_);
    jz_vec_clone(&dst->inputs, &src->inputs, sizeof(groove_NodeId), NULL);
    dst->output = src->output;
}
void groove__ivm__graph__NodeDescriptor__drop(groove_NodeDescriptor *v) {
    groove__ivm__graph__OpType__drop(&v->operator_);
    jz_vec_drop(&v->inputs, sizeof(groove_NodeId), NULL);
}
static bool nodeids_eq(const groove_NodeIdVec *a, const groove_NodeIdVec *b) {
    return a->len == b->len && (a->len == 0 || memcmp(a->ptr, b->ptr, a->len * sizeof(groove_NodeId)) == 0);
}
bool groove__ivm__graph__NodeDescriptor__PartialEq__eq(const groove_NodeDescriptor *a, const groove_NodeDescriptor *b) {
    return groove__ivm__graph__OpType__PartialEq__eq(&a->operator_, &b->operator_) &&
           nodeids_eq(&a->inputs, &b->inputs) && node_output_eq(&a->output, &b->output);
}
void groove__ivm__graph__NodeDescriptor__Debug__fmt(jz_str *out, const groove_NodeDescriptor *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "NodeDescriptor");
    groove__fmt__DebugStruct__field(&d, "operator"); groove__ivm__graph__OpType__Debug__fmt(out, &v->operator_);
    groove__fmt__DebugStruct__field(&d, "inputs");
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->inputs.len; i++) jz_str_appendf(out, i ? ", NodeId(%llu)" : "NodeId(%llu)", (unsigned long long)v->inputs.ptr[i]);
    jz_str_push_char(out, ']');
    groove__fmt__DebugStruct__field(&d, "output"); node_output_debug(out, &v->output);
    groove__fmt__DebugStruct__finish(&d);
}

/* ---- GraphValidationError constructors (Display text) ---- */
static jz_err err_invalid_node_output(void) { return jz_err_new(GROOVE_GRAPH_ERR_INVALID_NODE_OUTPUT, "operator has an incompatible typed output"); }
static jz_err err_field_oob(u32 index, u32 len) {
    return jz_err_fmt(GROOVE_GRAPH_ERR_FIELD_INDEX_OUT_OF_BOUNDS, "field index %u out of bounds for %u fields", (unsigned)index, (unsigned)len);
}
static jz_err err_output_mismatch(void) { return jz_err_new(GROOVE_GRAPH_ERR_OUTPUT_DESCRIPTOR_MISMATCH, "output descriptor mismatch"); }
static jz_err err_collect_mismatch(void) {
    return jz_err_new(GROOVE_GRAPH_ERR_COLLECT_BY_OUTPUT_DESCRIPTOR_MISMATCH,
                      "collect_by output descriptor does not match its projection and collection slot");
}
static jz_err err_collect_key_scalar(void) {
    return jz_err_new(GROOVE_GRAPH_ERR_COLLECT_BY_KEY_FIELD_MUST_BE_SCALAR,
                      "collect_by group, order, and tie fields must be scalar and non-record-valued");
}
static jz_err err_join_descriptor(void) { return jz_err_new(GROOVE_GRAPH_ERR_JOIN_INPUT_DESCRIPTOR_MISMATCH, "join input descriptors do not match"); }

static jz_err expect_arity(const groove_NodeIdVec *inputs, u32 expected) {
    if (inputs->len == expected) return JZ_OK;
    return jz_err_fmt(GROOVE_GRAPH_ERR_INPUT_ARITY_MISMATCH, "expected %u inputs, got %u", (unsigned)expected, (unsigned)inputs->len);
}
static jz_err expect_same_output(groove_RecordDescriptor expected, groove_RecordDescriptor actual) {
    if (groove__records__RecordDescriptor__registry_compatible_with(expected, actual)) return JZ_OK;
    return err_output_mismatch();
}
static jz_err expect_arrangement_inputs(const groove_NodeOutput *inputs, u32 n) {
    for (u32 i = 0; i < n; i++)
        if (inputs[i].tag != GROOVE_NODE_OUTPUT_ARRANGEMENT) return err_invalid_node_output();
    return JZ_OK;
}
static u32 nfields(groove_RecordDescriptor d) { u32 n; groove__records__RecordDescriptor__fields(d, &n); return n; }
static const groove_DescriptorField *fields_of(groove_RecordDescriptor d, u32 *n) { return groove__records__RecordDescriptor__fields(d, n); }
static jz_err check_indices(const u32 *idx, u32 n, u32 len) {
    for (u32 i = 0; i < n; i++) if (idx[i] >= len) return err_field_oob(idx[i], len);
    return JZ_OK;
}
static bool name_is(const groove_DescriptorField *f, const jz_str *name) {
    return f->has_name && jz_str_eq(&f->name, name);
}
/* output_field.name != Some(projection.output_name) || collect_projection_output_type(..) != Some(output_field.value_type) */
static bool projection_mismatch(const groove_DescriptorField *output_field, const groove_DescriptorField *input_field,
                                const groove_CollectByProjection *projection) {
    if (!name_is(output_field, &projection->output_name)) return true;
    groove_ValueType t;
    bool some = collect_projection_output_type(&input_field->value_type, projection, &t);
    bool ne = !some || !groove__records__values__ValueType__PartialEq__eq(&t, &output_field->value_type);
    if (some) groove__records__values__ValueType__drop(&t);
    return ne;
}
/* ValueType::Array(Box::new(ValueType::Record(Box::new(child)))) */
static void array_of_records(groove_ValueType *out, groove_RecordDescriptor child) {
    groove_ValueType record; memset(&record, 0, sizeof record);
    record.tag = GROOVE_VT_RECORD;
    record.u.record = jz_memdup(&child, sizeof child);
    memset(out, 0, sizeof *out);
    out->tag = GROOVE_VT_ARRAY;
    out->u.inner = jz_memdup(&record, sizeof record);
}
static bool type_is_array_of(const groove_ValueType *t, groove_RecordDescriptor child) {
    groove_ValueType expected;
    array_of_records(&expected, child);
    bool eq = groove__records__values__ValueType__PartialEq__eq(t, &expected);
    groove__records__values__ValueType__drop(&expected);
    return eq;
}
static bool key_field_not_scalar(const groove_ValueType *value_type) {
    bool scalar = groove__records__values__collect_by_ordered_scalar(value_type);
    return !scalar || groove__records__values__ValueType__contains_record(value_type);
}

static jz_err validate_collect_by(const groove_CollectByOp *c, groove_RecordDescriptor input, groove_RecordDescriptor output) {
    u32 in_len, out_len;
    const groove_DescriptorField *in_fields = fields_of(input, &in_len);
    const groove_DescriptorField *out_fields = fields_of(output, &out_len);
    u32 sort_n = c->sort_field_indices.len;
    if (c->mode == GROOVE_COLLECT_MODE_EXPAND) {
        if (c->tuple_fields.len == 0 || c->occurrence_id_field_indices.len == 0 ||
            sort_n != c->sort_directions.len || sort_n != c->order_fields.len + c->tie_fields.len ||
            c->order_fields.len == 0 || c->tie_fields.len == 0 || out_len != c->tuple_fields.len)
            return err_collect_mismatch();
        JZ_TRY(check_indices(c->group_field_indices.ptr, c->group_field_indices.len, in_len));
        JZ_TRY(check_indices(c->sort_field_indices.ptr, sort_n, in_len));
        JZ_TRY(check_indices(c->occurrence_id_field_indices.ptr, c->occurrence_id_field_indices.len, in_len));
        for (u32 i = 0; i < c->tuple_fields.len; i++)
            if (c->tuple_fields.ptr[i].field_idx >= in_len) return err_field_oob(c->tuple_fields.ptr[i].field_idx, in_len);
        for (u32 i = 0; i < out_len && i < c->tuple_fields.len; i++) {
            const groove_CollectByProjection *p = &c->tuple_fields.ptr[i];
            if (projection_mismatch(&out_fields[i], &in_fields[p->field_idx], p)) return err_collect_mismatch();
        }
        const groove_UsizeVec *lists[3] = { &c->group_field_indices, &c->sort_field_indices, &c->occurrence_id_field_indices };
        for (u32 l = 0; l < 3; l++)
            for (u32 i = 0; i < lists[l]->len; i++)
                if (key_field_not_scalar(&in_fields[lists[l]->ptr[i]].value_type)) return err_collect_key_scalar();
        return JZ_OK;
    }
    if (c->mode == GROOVE_COLLECT_MODE_ROOT) {
        if (c->slots.len != 0 || c->child_fields.len != 0 || c->collection_field.len != 0 ||
            out_len != c->parent_fields.len || sort_n != c->sort_directions.len ||
            sort_n != c->order_fields.len + c->tie_fields.len || c->order_fields.len == 0 || c->tie_fields.len == 0)
            return err_collect_mismatch();
        for (u32 i = 0; i < out_len && i < c->parent_fields.len; i++) {
            const groove_CollectByProjection *p = &c->parent_fields.ptr[i];
            if (p->field_idx >= in_len) return err_field_oob(p->field_idx, in_len);
            if (projection_mismatch(&out_fields[i], &in_fields[p->field_idx], p)) return err_collect_mismatch();
        }
        const groove_UsizeVec *lists[2] = { &c->group_field_indices, &c->sort_field_indices };
        for (u32 l = 0; l < 2; l++)
            for (u32 i = 0; i < lists[l]->len; i++) {
                u32 field_idx = lists[l]->ptr[i];
                if (field_idx >= in_len) return err_field_oob(field_idx, in_len);
                groove_ValueType value_type;
                groove__records__values__ValueType__Clone__clone(&value_type, &in_fields[field_idx].value_type);
                bool bad = key_field_not_scalar(&value_type);
                groove__records__values__ValueType__drop(&value_type);
                if (bad) return err_collect_key_scalar();
            }
        return JZ_OK;
    }
    if (c->slots.len != 0) {
        if (out_len != c->parent_fields.len + c->slots.len) return err_collect_mismatch();
        for (u32 i = 0; i < out_len && i < c->parent_fields.len; i++) {
            const groove_CollectByProjection *p = &c->parent_fields.ptr[i];
            if (p->field_idx >= in_len) return err_field_oob(p->field_idx, in_len);
            if (projection_mismatch(&out_fields[i], &in_fields[p->field_idx], p)) return err_collect_mismatch();
        }
        for (u32 i = 0; i + c->parent_fields.len < out_len && i < c->slots.len; i++) {
            const groove_DescriptorField *of = &out_fields[c->parent_fields.len + i];
            const groove_CollectBySlot *slot = &c->slots.ptr[i];
            if (!name_is(of, &slot->collection_field) || !type_is_array_of(&of->value_type, slot->child_descriptor))
                return err_collect_mismatch();
        }
        return JZ_OK;
    }
    if (c->collection_field_index >= out_len) return err_field_oob(c->collection_field_index, out_len);
    if (!type_is_array_of(&out_fields[c->collection_field_index].value_type, c->child_descriptor)) return err_collect_mismatch();
    if (out_len != c->parent_fields.len + 1 || !name_is(&out_fields[c->collection_field_index], &c->collection_field))
        return err_collect_mismatch();
    JZ_TRY(check_indices(c->group_field_indices.ptr, c->group_field_indices.len, in_len));
    JZ_TRY(check_indices(c->sort_field_indices.ptr, sort_n, in_len));
    for (u32 i = 0; i < c->parent_fields.len; i++)
        if (c->parent_fields.ptr[i].field_idx >= in_len) return err_field_oob(c->parent_fields.ptr[i].field_idx, in_len);
    for (u32 i = 0; i < c->child_fields.len; i++)
        if (c->child_fields.ptr[i].field_idx >= in_len) return err_field_oob(c->child_fields.ptr[i].field_idx, in_len);
    u32 child_len;
    const groove_DescriptorField *child_fields = fields_of(c->child_descriptor, &child_len);
    if (c->collection_field_index != c->parent_fields.len || child_len != c->child_fields.len ||
        sort_n != c->sort_directions.len || sort_n != c->order_fields.len + c->tie_fields.len ||
        c->order_fields.len == 0 || c->tie_fields.len == 0)
        return err_collect_mismatch();
    for (u32 i = 0; i < out_len && i < c->parent_fields.len; i++) {
        const groove_CollectByProjection *p = &c->parent_fields.ptr[i];
        if (projection_mismatch(&out_fields[i], &in_fields[p->field_idx], p)) return err_collect_mismatch();
    }
    for (u32 i = 0; i < child_len && i < c->child_fields.len; i++) {
        const groove_CollectByProjection *p = &c->child_fields.ptr[i];
        if (projection_mismatch(&child_fields[i], &in_fields[p->field_idx], p)) return err_collect_mismatch();
    }
    const groove_UsizeVec *lists[2] = { &c->group_field_indices, &c->sort_field_indices };
    for (u32 l = 0; l < 2; l++)
        for (u32 i = 0; i < lists[l]->len; i++)
            if (key_field_not_scalar(&in_fields[lists[l]->ptr[i]].value_type)) return err_collect_key_scalar();
    return JZ_OK;
}

jz_err groove__ivm__graph__NodeDescriptor__validate(const groove_NodeDescriptor *self, const groove_NodeOutput *typed_inputs, u32 n) {
    if (self->inputs.len != n)
        return jz_err_fmt(GROOVE_GRAPH_ERR_INPUT_DESCRIPTOR_ARITY_MISMATCH, "input count %u does not match descriptor count %u",
                          (unsigned)self->inputs.len, (unsigned)n);
    u8 op = self->operator_.tag;
    bool arrangement_consumer = op == GROOVE_OP_JOIN || op == GROOVE_OP_SEMI_JOIN || op == GROOVE_OP_ANTI_JOIN ||
                                op == GROOVE_OP_ARG_MAX_BY || op == GROOVE_OP_ARG_MIN_BY || op == GROOVE_OP_TOP_BY ||
                                op == GROOVE_OP_COLLECT_BY || op == GROOVE_OP_AGGREGATE;
    if (arrangement_consumer) {
        JZ_TRY(expect_arrangement_inputs(typed_inputs, n));
    } else {
        for (u32 i = 0; i < n; i++)
            if (typed_inputs[i].tag == GROOVE_NODE_OUTPUT_ARRANGEMENT) return err_invalid_node_output();
    }
    groove_RecordDescriptor input_outputs[n ? n : 1];
    for (u32 i = 0; i < n; i++) input_outputs[i] = groove__ivm__graph__NodeOutput__records(typed_inputs[i]);
    groove_RecordDescriptor output = groove__ivm__graph__NodeOutput__records(self->output);
    if (op != GROOVE_OP_ARRANGE && self->output.tag == GROOVE_NODE_OUTPUT_ARRANGEMENT) return err_invalid_node_output();
    const groove_OpType *o = &self->operator_;
    switch (op) {
    case GROOVE_OP_TABLE_SOURCE: case GROOVE_OP_INDEX_SOURCE: case GROOVE_OP_INLINE_RECORDS:
    case GROOVE_OP_FRONTIER_SOURCE: case GROOVE_OP_BINDING_SOURCE:
        return expect_arity(&self->inputs, 0);
    case GROOVE_OP_ARRANGE:
        JZ_TRY(expect_arity(&self->inputs, 1));
        if (typed_inputs[0].tag != GROOVE_NODE_OUTPUT_RECORDS || self->output.tag != GROOVE_NODE_OUTPUT_ARRANGEMENT)
            return err_invalid_node_output();
        return expect_same_output(output, input_outputs[0]);
    case GROOVE_OP_FILTER: case GROOVE_OP_DISTINCT: case GROOVE_OP_NEGATE:
        JZ_TRY(expect_arity(&self->inputs, 1));
        return expect_same_output(output, input_outputs[0]);
    case GROOVE_OP_ARG_MAX_BY: case GROOVE_OP_ARG_MIN_BY: {
        /* ArgMaxBy and ArgMinBy arms are identical (one C copy). */
        JZ_TRY(expect_arity(&self->inputs, 1));
        JZ_TRY(expect_arrangement_inputs(typed_inputs, n));
        JZ_TRY(expect_same_output(output, input_outputs[0]));
        u32 len = nfields(input_outputs[0]);
        JZ_TRY(check_indices(o->u.arg_by.group_field_indices.ptr, o->u.arg_by.group_field_indices.len, len));
        JZ_TRY(check_indices(o->u.arg_by.comparison_field_indices.ptr, o->u.arg_by.comparison_field_indices.len, len));
        return JZ_OK;
    }
    case GROOVE_OP_TOP_BY: {
        JZ_TRY(expect_arity(&self->inputs, 1));
        JZ_TRY(expect_arrangement_inputs(typed_inputs, n));
        JZ_TRY(expect_same_output(output, input_outputs[0]));
        u32 len;
        const groove_DescriptorField *fields = fields_of(input_outputs[0], &len);
        JZ_TRY(check_indices(o->u.top_by.group_field_indices.ptr, o->u.top_by.group_field_indices.len, len));
        JZ_TRY(check_indices(o->u.top_by.sort_field_indices.ptr, o->u.top_by.sort_field_indices.len, len));
        for (u32 i = 0; i < o->u.top_by.sort_field_indices.len; i++)
            if (!groove__records__values__collect_by_ordered_scalar(&fields[o->u.top_by.sort_field_indices.ptr[i]].value_type))
                return jz_err_new(GROOVE_GRAPH_ERR_TOP_BY_SORT_FIELD_MUST_BE_ORDERABLE, "top_by order and tie fields must be orderable scalar values");
        return JZ_OK;
    }
    case GROOVE_OP_COLLECT_BY:
        JZ_TRY(expect_arity(&self->inputs, 1));
        JZ_TRY(expect_arrangement_inputs(typed_inputs, n));
        return validate_collect_by(o->u.collect_by, input_outputs[0], output);
    case GROOVE_OP_UNWRAP_NULLABLE: {
        JZ_TRY(expect_arity(&self->inputs, 1));
        u32 len = nfields(input_outputs[0]);
        if (o->u.unwrap_nullable.field_idx >= len) return err_field_oob(o->u.unwrap_nullable.field_idx, len);
        return JZ_OK;
    }
    case GROOVE_OP_UNNEST: {
        JZ_TRY(expect_arity(&self->inputs, 1));
        u32 len = nfields(input_outputs[0]);
        if (o->u.unnest.array_field_idx >= len) return err_field_oob(o->u.unnest.array_field_idx, len);
        return JZ_OK;
    }
    case GROOVE_OP_VARIANT_PROJECT: {
        JZ_TRY(expect_arity(&self->inputs, 1));
        u32 len = nfields(input_outputs[0]);
        if (o->u.variant_project.field_idx >= len) return err_field_oob(o->u.variant_project.field_idx, len);
        return JZ_OK;
    }
    case GROOVE_OP_MAP_PROJECT: {
        JZ_TRY(expect_arity(&self->inputs, 1));
        u32 len = nfields(input_outputs[0]);
        for (u32 i = 0; i < o->u.map_project.mapping.len; i++)
            if (o->u.map_project.mapping.ptr[i].b >= len) return err_field_oob(o->u.map_project.mapping.ptr[i].b, len);
        u32 out_len = nfields(output);
        if (o->u.map_project.expressions.len != 0 && o->u.map_project.expressions.len != out_len)
            return jz_err_fmt(GROOVE_GRAPH_ERR_OUTPUT_FIELD_COUNT_MISMATCH, "expected %u output fields, got %u",
                              (unsigned)o->u.map_project.expressions.len, (unsigned)out_len);
        if (o->u.map_project.expressions.len == 0 && o->u.map_project.mapping.len != out_len)
            return jz_err_fmt(GROOVE_GRAPH_ERR_OUTPUT_FIELD_COUNT_MISMATCH, "expected %u output fields, got %u",
                              (unsigned)o->u.map_project.mapping.len, (unsigned)out_len);
        return JZ_OK;
    }
    case GROOVE_OP_STREAMING_CHECKSUM: {
        JZ_TRY(expect_arity(&self->inputs, 1));
        u32 len = nfields(input_outputs[0]);
        if (o->u.streaming_checksum.field_idx >= len) return err_field_oob(o->u.streaming_checksum.field_idx, len);
        if (o->u.streaming_checksum.window_bytes == 0 || o->u.streaming_checksum.max_bytes_per_turn == 0) return err_output_mismatch();
        return JZ_OK;
    }
    case GROOVE_OP_INDEX_BY: {
        JZ_TRY(expect_arity(&self->inputs, 1));
        u32 len = nfields(input_outputs[0]);
        JZ_TRY(check_indices(o->u.index_by.key_fields.ptr, o->u.index_by.key_fields.len, len));
        JZ_TRY(check_indices(o->u.index_by.value_fields.ptr, o->u.index_by.value_fields.len, len));
        return JZ_OK;
    }
    case GROOVE_OP_PERSIST:
        JZ_TRY(expect_arity(&self->inputs, 1));
        JZ_TRY(expect_same_output(output, input_outputs[0]));
        return check_indices(o->u.persist.key_fields.ptr, o->u.persist.key_fields.len, nfields(output));
    case GROOVE_OP_JOIN: case GROOVE_OP_SEMI_JOIN: case GROOVE_OP_ANTI_JOIN:
        JZ_TRY(expect_arity(&self->inputs, 2));
        JZ_TRY(expect_arrangement_inputs(typed_inputs, n));
        if (o->u.join.left_descriptor != input_outputs[0] || o->u.join.right_descriptor != input_outputs[1]) return err_join_descriptor();
        if (o->u.join.left_key.len != o->u.join.right_key.len)
            return jz_err_fmt(GROOVE_GRAPH_ERR_JOIN_KEY_ARITY_MISMATCH, "join key arity mismatch: %u vs %u",
                              (unsigned)o->u.join.left_key.len, (unsigned)o->u.join.right_key.len);
        return JZ_OK;
    case GROOVE_OP_UNION:
        if (self->inputs.len == 0) return JZ_OK;
        for (u32 i = 0; i < n; i++) JZ_TRY(expect_same_output(output, input_outputs[i]));
        return JZ_OK;
    case GROOVE_OP_AGGREGATE:
        JZ_TRY(expect_arity(&self->inputs, 1));
        JZ_TRY(expect_arrangement_inputs(typed_inputs, n));
        return check_indices(o->u.aggregate.group_field_indices.ptr, o->u.aggregate.group_field_indices.len, nfields(input_outputs[0]));
    case GROOVE_OP_RECURSIVE:
        return expect_arity(&self->inputs, o->u.recursive.has_step_witness_output ? 3 : 2);
    case GROOVE_OP_RECURSIVE_STEP_WITNESS:
        return expect_arity(&self->inputs, 1);
    case GROOVE_OP_LOOKUP:
        JZ_TRY(expect_arity(&self->inputs, 1));
        if (o->u.lookup.key_descriptor != input_outputs[0]) return err_join_descriptor();
        return check_indices(o->u.lookup.key_fields.ptr, o->u.lookup.key_fields.len, nfields(input_outputs[0]));
    }
    return JZ_OK;
}

void groove__ivm__graph__GraphValidationError__Debug__fmt(jz_str *out, const jz_err *e) {
    static const char *const NAMES[] = {
        "InvalidNodeOutput", "FieldIndexOutOfBounds", "InputArityMismatch", "InputDescriptorArityMismatch",
        "JoinInputDescriptorMismatch", "JoinKeyArityMismatch", "OutputDescriptorMismatch", "OutputFieldCountMismatch",
        "CollectByOutputDescriptorMismatch", "CollectByKeyFieldMustBeScalar", "TopBySortFieldMustBeOrderable",
        "CollectByInputIsTerminal"
    };
    u32 i = e->code - GROOVE_GRAPH_ERR_INVALID_NODE_OUTPUT;
    jz_str_push_cstr(out, i < 12 ? NAMES[i] : "GraphValidationError");
    if (e->msg) { jz_str_push_cstr(out, " { "); jz_str_push(out, e->msg->ptr, e->msg->len); jz_str_push_cstr(out, " }"); }
}

/* ======================================================================
 * NodeDurability / Retainer / GraphNode
 * ====================================================================== */
void groove__ivm__graph__NodeDurability__Clone__clone(groove_NodeDurability *dst, const groove_NodeDurability *src) {
    dst->tag = src->tag;
    if (src->tag == GROOVE_DURABILITY_DURABLE) groove__ivm__graph__DurableStorage__Clone__clone(&dst->storage, &src->storage);
    else memset(&dst->storage, 0, sizeof dst->storage);
}
void groove__ivm__graph__NodeDurability__drop(groove_NodeDurability *v) {
    if (v->tag == GROOVE_DURABILITY_DURABLE) groove__ivm__graph__DurableStorage__drop(&v->storage);
}
u64 groove__ivm__graph__Retainer__hash_v(const void *k) {
    const groove_Retainer *r = k;
    return jz_hash_combine(jz_hash_u64(r->tag), jz_str_hash(&r->name));
}
bool groove__ivm__graph__Retainer__eq_v(const void *a, const void *b) {
    const groove_Retainer *x = a, *y = b;
    return x->tag == y->tag && jz_str_eq(&x->name, &y->name);
}
void groove__ivm__graph__Retainer__drop_v(void *k) { jz_str_drop(&((groove_Retainer *)k)->name); }
void groove__ivm__graph__Retainer__Clone__clone(groove_Retainer *dst, const groove_Retainer *src) {
    dst->tag = src->tag;
    dst->name = jz_str_clone(&src->name);
}
void groove__ivm__graph__Retainer__Debug__fmt(jz_str *out, const groove_Retainer *v) {
    static const char *const NAMES[] = { "Subscription", "Hydration", "PreparedShape", "DurableSchemaObject" };
    groove__fmt__debug_tuple_begin(out, NAMES[v->tag]);
    groove__fmt__debug_str(out, &v->name);
    groove__fmt__debug_tuple_end(out);
}
const jz_hmap_desc groove__ivm__graph__RETAINER_SET_DESC = { sizeof(groove_Retainer), 0, groove__ivm__graph__Retainer__hash_v,
                                                             groove__ivm__graph__Retainer__eq_v, groove__ivm__graph__Retainer__drop_v, NULL };

static groove_GraphNode GraphNode__new(groove_NodeDescriptor descriptor, groove_NodeDurability durability) {
    groove_GraphNode node;
    node.id = groove__ivm__graph__NodeDescriptor__node_id(&descriptor);
    node.descriptor = descriptor;
    node.durability = durability;
    jz_hmap_init(&node.children, &groove__ivm__NODESET_DESC);
    return node;
}
bool groove__ivm__graph__GraphNode__is_durable(const groove_GraphNode *self) {
    return self->durability.tag == GROOVE_DURABILITY_DURABLE;
}
void groove__ivm__graph__GraphNode__drop(groove_GraphNode *self) {
    groove__ivm__graph__NodeDescriptor__drop(&self->descriptor);
    groove__ivm__graph__NodeDurability__drop(&self->durability);
    jz_hmap_drop(&self->children);
}

/* ======================================================================
 * IvmGraph
 * ====================================================================== */
const groove_RouteIndex *groove__ivm__graph__IvmGraph__routes(const groove_IvmGraph *self) { return &self->routes; }

void groove__ivm__graph__IvmGraph__add_route_barrier(groove_IvmGraph *self, groove_NodeId terminal, groove_NodeId barrier,
                                                     groove_UsizeVec field_indices, groove_ValueTypeVec field_types, jz_bytes key,
                                                     bool has_root_ordering_node, groove_NodeId root_ordering_node) {
    if (groove__ivm__routes__RouteIndex__add(&self->routes, terminal, barrier, field_indices, field_types, key,
                                             has_root_ordering_node, root_ordering_node))
        groove__ivm__activation__ActivationCache__clear(&self->activations);
}
void groove__ivm__graph__IvmGraph__release_route_barrier(groove_IvmGraph *self, groove_NodeId barrier) {
    if (groove__ivm__routes__RouteIndex__release(&self->routes, barrier))
        groove__ivm__activation__ActivationCache__clear(&self->activations);
}
void groove__ivm__graph__IvmGraph__downstream_through_routes(const groove_IvmGraph *self, const groove_NodeId *roots, u32 n, jz_hmap *reached) {
    jz_hmap_init(reached, &groove__ivm__NODESET_DESC);
    groove_NodeIdVec pending = {0};
    jz_vec_extend(&pending, roots, n, sizeof(groove_NodeId));
    while (pending.len) {
        groove_NodeId id = pending.ptr[--pending.len];
        if (jz_hmap_insert(reached, &id, NULL, NULL)) continue; /* already present */
        const groove_GraphNode *node = jz_hmap_get(&self->nodes, &id);
        if (node) {
            u64 *child; void *unused;
            JZ_HMAP_FOR(&node->children, child, unused) JZ_VEC_PUSH(pending, *child);
        }
    }
    jz_vec_drop(&pending, sizeof(groove_NodeId), NULL);
}
static void extend_from_set(groove_NodeIdVec *out, const jz_hmap *set) {
    u64 *id; void *unused;
    JZ_HMAP_FOR(set, id, unused) JZ_VEC_PUSH(*out, *id);
}
static void IvmGraph__source_nodes(const groove_IvmGraph *self, const jz_str *tables, u32 nt,
                                   const groove_BindingSourceKey *bindings, u32 nb, groove_NodeIdVec *out) {
    memset(out, 0, sizeof *out);
    for (u32 i = 0; i < nt; i++) {
        const jz_hmap *nodes = jz_hmap_get(&self->table_sources, &tables[i]);
        if (nodes) extend_from_set(out, nodes);
    }
    for (u32 i = 0; i < nb; i++) {
        const jz_hmap *nodes = jz_hmap_get(&self->binding_sources, &bindings[i]);
        if (nodes) extend_from_set(out, nodes);
        const jz_str *name = groove__ivm__op_types__BindingSourceKey__prepared_name(&bindings[i]);
        if (name) {
            const jz_hmap *frontier = jz_hmap_get(&self->frontier_sources, name);
            if (frontier) extend_from_set(out, frontier);
        }
    }
}
void groove__ivm__graph__IvmGraph__affected_nodes_through_routes(const groove_IvmGraph *self, const jz_str *tables, u32 nt,
                                                                 const groove_BindingSourceKey *bindings, u32 nb, jz_hmap *out) {
    groove_NodeIdVec sources;
    IvmGraph__source_nodes(self, tables, nt, bindings, nb, &sources);
    groove__ivm__graph__IvmGraph__downstream_through_routes(self, sources.ptr, sources.len, out);
    jz_vec_drop(&sources, sizeof(groove_NodeId), NULL);
}
bool groove__ivm__graph__IvmGraph__execution_layout(const groove_IvmGraph *self, const groove_NodeId *roots, u32 n,
                                                    groove_ExecutionLayout **out, groove_NodeId *err) {
    /* ExecutionLayoutCache sits behind a Mutex in Rust: interior mutability. */
    return groove__ivm__execution_layout__ExecutionLayoutCache__get((groove_ExecutionLayoutCache *)&self->execution_layouts, self, roots, n, out, err);
}
void groove__ivm__graph__IvmGraph__execution_layout_counters(const groove_IvmGraph *self, u64 *builds, u64 *hits) {
    groove__ivm__execution_layout__ExecutionLayoutCache__counters(&self->execution_layouts, builds, hits);
}
void groove__ivm__graph__IvmGraph__new(groove_IvmGraph *self) {
    groove__ivm__activation__ActivationCache__Default__default(&self->activations);
    groove__ivm__execution_layout__ExecutionLayoutCache__Default__default(&self->execution_layouts);
    jz_hmap_init(&self->nodes, &NODES_DESC);
    jz_hmap_init(&self->table_sources, &STR_SOURCES_DESC);
    jz_hmap_init(&self->binding_sources, &BINDING_SOURCES_DESC);
    jz_hmap_init(&self->frontier_sources, &STR_SOURCES_DESC);
    groove__ivm__routes__RouteIndex__Default__default(&self->routes);
    memset(&self->added_since_drain, 0, sizeof self->added_since_drain);
}
void groove__ivm__graph__IvmGraph__drop(groove_IvmGraph *self) {
    groove__ivm__activation__ActivationCache__drop(&self->activations);
    groove__ivm__execution_layout__ExecutionLayoutCache__drop(&self->execution_layouts);
    jz_hmap_drop(&self->nodes);
    jz_hmap_drop(&self->table_sources);
    jz_hmap_drop(&self->binding_sources);
    jz_hmap_drop(&self->frontier_sources);
    groove__ivm__routes__RouteIndex__drop(&self->routes);
    jz_vec_drop(&self->added_since_drain, sizeof(groove_NodeId), NULL);
}

_Noreturn static void collision_panic(const groove_NodeDescriptor *left, const groove_NodeDescriptor *right) {
    jz_str msg = jz_str_from("assertion `left == right` failed: IVM node id collision for incompatible descriptors\n  left: ");
    groove__ivm__graph__NodeDescriptor__Debug__fmt(&msg, left);
    jz_str_push_cstr(&msg, "\n right: ");
    groove__ivm__graph__NodeDescriptor__Debug__fmt(&msg, right);
    jz_str_push_char(&msg, '\0');
    jz_panic(msg.ptr);
}
_Noreturn static void invalid_descriptor_panic(jz_err e) {
    jz_str msg = jz_str_from("invalid IVM graph node descriptor: ");
    groove__ivm__graph__GraphValidationError__Debug__fmt(&msg, &e);
    jz_str_push_char(&msg, '\0');
    jz_panic(msg.ptr);
}
static groove_NodeId IvmGraph__insert_new_node(groove_IvmGraph *self, groove_NodeId id, groove_NodeDescriptor descriptor, groove_NodeDurability durability);

groove_NodeId groove__ivm__graph__IvmGraph__dedup_node(groove_IvmGraph *self, groove_NodeDescriptor descriptor, groove_NodeDurability durability) {
    groove_NodeId id = groove__ivm__graph__NodeDescriptor__node_id(&descriptor);
    const groove_GraphNode *existing = jz_hmap_get(&self->nodes, &id);
    if (existing) {
        /* An existing node was validated when inserted, against the same input
         * ids; node outputs are immutable per id. */
        if (!groove__ivm__graph__NodeDescriptor__PartialEq__eq(&existing->descriptor, &descriptor))
            collision_panic(&existing->descriptor, &descriptor);
        groove__ivm__graph__NodeDescriptor__drop(&descriptor);
        groove__ivm__graph__NodeDurability__drop(&durability);
        return id;
    }
    jz_err e = groove__ivm__graph__IvmGraph__validate_node(self, &descriptor);
    if (e.code) invalid_descriptor_panic(e);
    return IvmGraph__insert_new_node(self, id, descriptor, durability);
}
groove_NodeId groove__ivm__graph__IvmGraph__dedup_node_parts(groove_IvmGraph *self, const groove_OpType *op, groove_NodeIdVec inputs,
                                                             groove_NodeOutput output, groove_NodeDurability durability) {
    groove_NodeId id = groove__ivm__graph__NodeDescriptor__node_id_of(op, inputs.ptr, inputs.len, &output);
    const groove_GraphNode *existing = jz_hmap_get(&self->nodes, &id);
    if (existing) {
        if (!(groove__ivm__graph__OpType__PartialEq__eq(&existing->descriptor.operator_, op) &&
              nodeids_eq(&existing->descriptor.inputs, &inputs) && node_output_eq(&existing->descriptor.output, &output)))
            jz_panic("IVM node id collision for incompatible descriptors");
        jz_vec_drop(&inputs, sizeof(groove_NodeId), NULL);
        groove__ivm__graph__NodeDurability__drop(&durability);
        return id;
    }
    groove_NodeDescriptor descriptor;
    groove__ivm__graph__OpType__Clone__clone(&descriptor.operator_, op);
    descriptor.inputs = inputs;
    descriptor.output = output;
    jz_err e = groove__ivm__graph__IvmGraph__validate_node(self, &descriptor);
    if (e.code) invalid_descriptor_panic(e);
    return IvmGraph__insert_new_node(self, id, descriptor, durability);
}
static void add_source(jz_hmap *sources, const void *key, usize key_size, void (*key_clone)(void *, const void *),
                       void (*key_drop)(void *), groove_NodeId id) {
    u8 cloned[64];
    key_clone(cloned, key);
    bool inserted;
    jz_hmap *nodes = jz_hmap_entry(sources, cloned, &inserted);
    if (inserted) jz_hmap_init(nodes, &groove__ivm__NODESET_DESC);
    else key_drop(cloned);
    jz_hmap_insert(nodes, &id, NULL, NULL);
    (void)key_size;
}
static void bsk_clone_v(void *d, const void *s) { groove__ivm__op_types__BindingSourceKey__Clone__clone(d, s); }
static groove_NodeId IvmGraph__insert_new_node(groove_IvmGraph *self, groove_NodeId id, groove_NodeDescriptor descriptor, groove_NodeDurability durability) {
    JZ_VEC_PUSH(self->added_since_drain, id);
    /* A durable node changes whether every route barrier above it keeps
     * ordinary activation, including plans that stopped at such a barrier. */
    if (durability.tag == GROOVE_DURABILITY_DURABLE) groove__ivm__activation__ActivationCache__clear(&self->activations);
    else groove__ivm__activation__ActivationCache__added(&self->activations, descriptor.inputs.ptr, descriptor.inputs.len);
    for (u32 i = 0; i < descriptor.inputs.len; i++) {
        groove_GraphNode *input_node = jz_hmap_get(&self->nodes, &descriptor.inputs.ptr[i]);
        if (input_node) jz_hmap_insert(&input_node->children, &id, NULL, NULL);
    }
    const groove_OpType *op = &descriptor.operator_;
    switch (op->tag) {
    case GROOVE_OP_TABLE_SOURCE:
        add_source(&self->table_sources, &op->u.table_source.table, sizeof(jz_str), groove__ivm__jz_str_clone_v, groove__ivm__jz_str_drop_v, id);
        break;
    case GROOVE_OP_INDEX_SOURCE:
        add_source(&self->table_sources, &op->u.index_source.table, sizeof(jz_str), groove__ivm__jz_str_clone_v, groove__ivm__jz_str_drop_v, id);
        break;
    /* A lookup is also a source of its probed table. */
    case GROOVE_OP_LOOKUP:
        add_source(&self->table_sources, &op->u.lookup.source.table, sizeof(jz_str), groove__ivm__jz_str_clone_v, groove__ivm__jz_str_drop_v, id);
        break;
    case GROOVE_OP_BINDING_SOURCE:
        add_source(&self->binding_sources, &op->u.binding_source.key, sizeof(groove_BindingSourceKey), bsk_clone_v,
                   groove__ivm__op_types__BindingSourceKey__drop_v, id);
        break;
    case GROOVE_OP_FRONTIER_SOURCE:
        add_source(&self->frontier_sources, &op->u.frontier_source.binding, sizeof(jz_str), groove__ivm__jz_str_clone_v, groove__ivm__jz_str_drop_v, id);
        break;
    default:
        break;
    }
    groove_GraphNode node = GraphNode__new(descriptor, durability);
    groove_GraphNode old;
    if (jz_hmap_insert(&self->nodes, &id, &node, &old)) groove__ivm__graph__GraphNode__drop(&old);
    return id;
}

jz_err groove__ivm__graph__IvmGraph__validate_node(const groove_IvmGraph *self, const groove_NodeDescriptor *descriptor) {
    JZ_VEC(groove_NodeOutput) input_outputs = {0};
    for (u32 i = 0; i < descriptor->inputs.len; i++) {
        const groove_GraphNode *node = jz_hmap_get(&self->nodes, &descriptor->inputs.ptr[i]);
        if (node) JZ_VEC_PUSH(input_outputs, node->descriptor.output);
    }
    jz_err e = groove__ivm__graph__NodeDescriptor__validate(descriptor, input_outputs.ptr, input_outputs.len);
    jz_vec_drop(&input_outputs, sizeof(groove_NodeOutput), NULL);
    JZ_TRY(e);
    bool consumes_collect_by = false;
    for (u32 i = 0; i < descriptor->inputs.len && !consumes_collect_by; i++) {
        const groove_GraphNode *node = jz_hmap_get(&self->nodes, &descriptor->inputs.ptr[i]);
        consumes_collect_by = node && node->descriptor.operator_.tag == GROOVE_OP_COLLECT_BY;
    }
    if (consumes_collect_by)
        return jz_err_new(GROOVE_GRAPH_ERR_COLLECT_BY_INPUT_IS_TERMINAL, "collect_by is terminal-only and cannot be an input node");
    return JZ_OK;
}
const groove_GraphNode *groove__ivm__graph__IvmGraph__node(const groove_IvmGraph *self, groove_NodeId id) {
    return jz_hmap_get(&self->nodes, &id);
}
const jz_hmap *groove__ivm__graph__IvmGraph__nodes(const groove_IvmGraph *self) { return &self->nodes; }

bool groove__ivm__graph__IvmGraph__activation_plan(const groove_IvmGraph *self, const jz_str *tables, u32 nt,
                                                   const groove_BindingSourceKey *bindings, u32 nb,
                                                   groove_ActivationPlan **out, groove_NodeId *err) {
    groove_NodeIdVec sources;
    IvmGraph__source_nodes(self, tables, nt, bindings, nb, &sources);
    /* ActivationCache sits behind a Mutex in Rust: interior mutability. */
    return groove__ivm__activation__ActivationCache__get((groove_ActivationCache *)&self->activations, self, sources, out, err);
}
jz_hmap *groove__ivm__graph__IvmGraph__affected_nodes(const groove_IvmGraph *self, const jz_str *tables, u32 nt,
                                                     const groove_BindingSourceKey *bindings, u32 nb) {
    groove_ActivationPlan *plan; groove_NodeId err;
    if (!groove__ivm__graph__IvmGraph__activation_plan(self, tables, nt, bindings, nb, &plan, &err))
        jz_panic("graph source reachability is valid");
    jz_hmap *affected = jz_rc_clone(plan->affected);
    jz_rc_release(plan);
    return affected;
}
groove_NodeIdVec groove__ivm__graph__IvmGraph__take_added_nodes(groove_IvmGraph *self) {
    groove_NodeIdVec taken = self->added_since_drain;
    memset(&self->added_since_drain, 0, sizeof self->added_since_drain);
    return taken;
}
void groove__ivm__graph__IvmGraph__mark_ancestors(const groove_IvmGraph *self, groove_NodeId id, jz_hmap *retained) {
    if (jz_hmap_insert(retained, &id, NULL, NULL)) return; /* was already present */
    const groove_GraphNode *node = jz_hmap_get(&self->nodes, &id);
    if (node)
        for (u32 i = 0; i < node->descriptor.inputs.len; i++)
            groove__ivm__graph__IvmGraph__mark_ancestors(self, node->descriptor.inputs.ptr[i], retained);
}

/* fn remove_source_node<K>(sources, source, node): one copy over the erased map. */
static void remove_source_node(jz_hmap *sources, const void *source, groove_NodeId node) {
    jz_hmap *nodes = jz_hmap_get(sources, source);
    bool remove_source = false;
    if (nodes) {
        jz_hmap_remove(nodes, &node, NULL, NULL);
        remove_source = nodes->len == 0;
    }
    if (remove_source) jz_hmap_remove(sources, source, NULL, NULL);
}
void groove__ivm__graph__IvmGraph__remove_node(groove_IvmGraph *self, groove_NodeId id) {
    groove__ivm__activation__ActivationCache__removed(&self->activations, id);
    if (groove__ivm__routes__RouteIndex__remove(&self->routes, id)) groove__ivm__activation__ActivationCache__clear(&self->activations);
    groove_NodeId key;
    groove_GraphNode node;
    if (!jz_hmap_remove(&self->nodes, &id, &key, &node)) return;
    groove__ivm__execution_layout__ExecutionLayoutCache__invalidate(&self->execution_layouts, true, id);
    for (u32 i = 0; i < node.descriptor.inputs.len; i++) {
        groove_GraphNode *input_node = jz_hmap_get(&self->nodes, &node.descriptor.inputs.ptr[i]);
        if (input_node) jz_hmap_remove(&input_node->children, &id, NULL, NULL);
    }
    const groove_OpType *op = &node.descriptor.operator_;
    switch (op->tag) {
    case GROOVE_OP_TABLE_SOURCE: remove_source_node(&self->table_sources, &op->u.table_source.table, id); break;
    case GROOVE_OP_INDEX_SOURCE: remove_source_node(&self->table_sources, &op->u.index_source.table, id); break;
    case GROOVE_OP_LOOKUP: remove_source_node(&self->table_sources, &op->u.lookup.source.table, id); break;
    case GROOVE_OP_BINDING_SOURCE: remove_source_node(&self->binding_sources, &op->u.binding_source.key, id); break;
    case GROOVE_OP_FRONTIER_SOURCE: remove_source_node(&self->frontier_sources, &op->u.frontier_source.binding, id); break;
    default: break;
    }
    groove__ivm__graph__GraphNode__drop(&node);
}
