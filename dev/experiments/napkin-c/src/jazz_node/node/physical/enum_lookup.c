/* physical/enum_lookup.c: jazz_node::node::physical (physical/enum_lookup.rs).
 * The Rust `trait EnumCase` (implemented for GlobalScalarEnumCaseId and
 * GlobalEnumCaseId, so every generic method below was monomorphised twice)
 * becomes one vtable and one copy of each function. */
#include "../physical.h"
#include "../codec.h"

typedef struct jzn_EnumCaseVt {
    usize elem;
    u64 tag_max;
    const char *missing[2];
    const char *target_tag_exhausted[2];
    const char *copied_tag_exhausted[2];
    const jz_btree_desc *set_desc;
    const jz_btree *(*root)(const jazz_node_TablePhysicalMapping *t);
    const jz_btree *(*nested)(const jazz_node_TablePhysicalMapping *t);
    int (*compare)(const jz_btree *aliases, const void *l, const void *r);
    /* tags: Vec<Option<Tag>> as i64 (-1 = None); children: case pointers (NULL = None) */
    void (*install)(jazz_node_EnumOccurrenceRemaps *remaps, const jz_str *path, groove_OptU32Vec *tags, const void *const *children, u32 n);
    void (*after_authored_root)(jazz_node_EnumOccurrenceRemaps *remaps);
} jzn_EnumCaseVt;

static jz_err jazz_node__node__physical__enum_error(const char *const messages[2], const jz_str *path) {
    return JZN_ISV(messages[path != NULL]);
}

/* ---- GlobalScalarEnumCaseId impl ---- */
static const jz_btree *jzn_scalar_root(const jazz_node_TablePhysicalMapping *t) { return &t->scalar_enum_cases; }
static const jz_btree *jzn_scalar_nested(const jazz_node_TablePhysicalMapping *t) { return &t->nested_scalar_enum_cases; }
static int jzn_scalar_compare(const jz_btree *aliases, const void *l, const void *r) {
    return jazz_node__node__physical__compare_scalar_enum_cases(aliases, l, r);
}
static void jzn_str_drop_v(void *p) { jz_str_drop((jz_str *)p); }
static void jzn_scalar_install(jazz_node_EnumOccurrenceRemaps *remaps, const jz_str *path, groove_OptU32Vec *tags,
                               const void *const *children, u32 n) {
    groove_OptU8Vec out = {0};
    jz_vec_reserve(&out, tags->len, sizeof(u16));
    for (u32 i = 0; i < tags->len; i++) {
        u16 t = tags->ptr[i] < 0 ? GROOVE_OPT_U8_NONE : (u16)tags->ptr[i];
        JZ_VEC_PUSH(out, t);
    }
    jz_vec_drop(tags, sizeof(i64), NULL);
    jz_str key = jz_str_clone(path);
    jz_btree_insert(&remaps->scalar, &key, &out, NULL);
}
static void jzn_noop_after_root(jazz_node_EnumOccurrenceRemaps *remaps) { (void)remaps; }

/* ---- GlobalEnumCaseId impl ---- */
static const jz_btree *jzn_payload_root(const jazz_node_TablePhysicalMapping *t) { return &t->payload_enum_cases; }
static const jz_btree *jzn_payload_nested(const jazz_node_TablePhysicalMapping *t) { return &t->nested_payload_enum_cases; }
static int jzn_payload_compare(const jz_btree *aliases, const void *l, const void *r) {
    return jazz_node__node__physical__compare_global_enum_cases(aliases, l, r);
}
static void jzn_payload_install(jazz_node_EnumOccurrenceRemaps *remaps, const jz_str *path, groove_OptU32Vec *tags,
                                const void *const *children, u32 n) {
    groove_OptU32Vec out = *(groove_OptU32Vec *)tags;
    jz_str key = jz_str_clone(path);
    jz_btree_insert(&remaps->payload, &key, &out, NULL);
    groove_OptStrVec child_paths = {0};
    jz_vec_reserve(&child_paths, n, sizeof(jz_str));
    for (u32 i = 0; i < n; i++) {
        jz_str child = {0};
        if (children[i]) child = jazz_node__node__global_case_path(path, children[i]);
        JZ_VEC_PUSH(child_paths, child);
    }
    key = jz_str_clone(path);
    jz_btree_insert(&remaps->payload_children, &key, &child_paths, NULL);
}
static void jzn_opt_str_clone(void *dst, const void *src) {
    const jz_str *s = src;
    *(jz_str *)dst = s->ptr ? jz_str_clone(s) : (jz_str){0};
}
/* Payload values also receive a `root/nullable` alias for the current-table's
 * synthetic nullable cell carrier. */
static void jzn_payload_after_root(jazz_node_EnumOccurrenceRemaps *remaps) {
    jz_str root = jz_str_from("root");
    const groove_OptU32Vec *tags = jz_btree_get(&remaps->payload, &root);
    if (tags) {
        groove_OptU32Vec copy = {0};
        jz_vec_clone(&copy, tags, sizeof(i64), NULL);
        jz_str key = jz_str_from("root/nullable");
        jz_btree_insert(&remaps->payload, &key, &copy, NULL);
    }
    const groove_OptStrVec *children = jz_btree_get(&remaps->payload_children, &root);
    if (children) {
        groove_OptStrVec copy = {0};
        jz_vec_clone(&copy, children, sizeof(jz_str), jzn_opt_str_clone);
        jz_str key = jz_str_from("root/nullable");
        jz_btree_insert(&remaps->payload_children, &key, &copy, NULL);
    }
    jz_str_drop(&root);
}

static const jzn_EnumCaseVt jzn_scalar_case_vt = {
    sizeof(jazz_node_GlobalScalarEnumCaseId), 0xff,
    { "physical scalar enum registry identity mapping missing", "physical nested scalar enum registry identity mapping missing" },
    { "target scalar enum tag exhausted", "target nested scalar enum tag exhausted" },
    { "copied scalar enum tag exhausted", "copied nested scalar enum tag exhausted" },
    &jzn_desc_gse_set, jzn_scalar_root, jzn_scalar_nested, jzn_scalar_compare, jzn_scalar_install, jzn_noop_after_root,
};
static const jzn_EnumCaseVt jzn_payload_case_vt = {
    sizeof(jazz_node_GlobalEnumCaseId), 0xffffffffu,
    { "physical payload enum registry identity mapping missing", "physical nested payload enum registry identity mapping missing" },
    { "target payload enum tag exhausted", "target nested payload enum tag exhausted" },
    { "copied payload enum tag exhausted", "copied nested payload enum tag exhausted" },
    &jzn_desc_ge_set, jzn_payload_root, jzn_payload_nested, jzn_payload_compare, jzn_payload_install, jzn_payload_after_root,
};

/* EnumCase::occurrence: the registry at the root (NULL) or a nested path */
static const jz_vec *jazz_node__node__physical__EnumCase__occurrence(const jzn_EnumCaseVt *vt, const jazz_node_TablePhysicalMapping *table,
                                                                     jazz_types_PhysicalColumnId column_id, const jz_str *path) {
    if (!path) return jz_btree_get(vt->root(table), &column_id);
    const jz_btree *paths = jz_btree_get(vt->nested(table), &column_id);
    return paths ? jz_btree_get(paths, path) : NULL;
}

typedef struct jzn_sort_ctx { const jzn_EnumCaseVt *vt; const jz_btree *aliases; } jzn_sort_ctx;
static int jzn_sort_cases(const void *a, const void *b, void *ctx) {
    jzn_sort_ctx *c = ctx;
    return c->vt->compare(c->aliases, a, b);
}

/* The union of one enum occurrence's registry across every mapping of the
 * physical table, in durable introduction order. */
static jz_err jazz_node__node__physical__NodeState__physical_enum_cases(const jzn_EnumCaseVt *vt, const jazz_node_NodeState *s,
                                                                       jazz_types_PhysicalTableId table_id, jazz_types_PhysicalColumnId column_id,
                                                                       const jz_str *path, jz_vec *out) {
    jz_btree cases;
    jz_btree_init(&cases, vt->set_desc);
    jz_uuid *schema;
    jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(&s->catalogue.physical_mappings, schema, mapping) {
        jz_str *name;
        jazz_node_TablePhysicalMapping *table;
        JZ_BTREE_FOR(&mapping->tables, name, table) {
            if (table->table_id != table_id) continue;
            const jz_vec *column_cases = jazz_node__node__physical__EnumCase__occurrence(vt, table, column_id, path);
            if (!column_cases) continue;
            for (u32 i = 0; i < column_cases->len; i++)
                jz_btree_insert(&cases, (const u8 *)column_cases->ptr + (usize)i * vt->elem, NULL, NULL);
        }
    }
    if (cases.len == 0) {
        jz_btree_drop(&cases);
        return jazz_node__node__physical__enum_error(vt->missing, path);
    }
    jz_vec result = {0};
    jz_vec_reserve(&result, cases.len, vt->elem);
    void *k, *v;
    JZ_BTREE_FOR(&cases, k, v) jz_vec_push(&result, k, vt->elem);
    jz_btree_drop(&cases);
    jzn_sort_ctx ctx = { vt, &s->catalogue.schema_version_aliases };
    jz_sort(result.ptr, result.len, vt->elem, jzn_sort_cases, &ctx);
    *out = result;
    return JZ_OK;
}

/* The lineage union for an occurrence, or `fallback` when not yet durable. */
static void jazz_node__node__physical__NodeState__physical_enum_cases_or(const jzn_EnumCaseVt *vt, const jazz_node_NodeState *s,
                                                                        jazz_types_PhysicalTableId table_id, jazz_types_PhysicalColumnId column_id,
                                                                        const jz_str *path, const jz_vec *fallback, jz_vec *out) {
    jz_err e = jazz_node__node__physical__NodeState__physical_enum_cases(vt, s, table_id, column_id, path, out);
    if (JZ_IS_ERR(e)) {
        jz_err_drop(&e);
        memset(out, 0, sizeof *out);
        jz_vec_clone(out, fallback, vt->elem, NULL);
    }
}

jz_err jazz_node__node__physical__NodeState__physical_scalar_enum_cases(const jazz_node_NodeState *s, jazz_types_PhysicalTableId t,
                                                                       jazz_types_PhysicalColumnId c, jazz_node_GlobalScalarEnumCaseIdVec *out) {
    return jazz_node__node__physical__NodeState__physical_enum_cases(&jzn_scalar_case_vt, s, t, c, NULL, (jz_vec *)out);
}
jz_err jazz_node__node__physical__NodeState__physical_payload_enum_cases(const jazz_node_NodeState *s, jazz_types_PhysicalTableId t,
                                                                        jazz_types_PhysicalColumnId c, jazz_node_GlobalEnumCaseIdVec *out) {
    return jazz_node__node__physical__NodeState__physical_enum_cases(&jzn_payload_case_vt, s, t, c, NULL, (jz_vec *)out);
}
jz_err jazz_node__node__physical__NodeState__physical_nested_scalar_enum_cases(const jazz_node_NodeState *s, jazz_types_PhysicalTableId t,
                                                                              jazz_types_PhysicalColumnId c, const jz_str *path,
                                                                              jazz_node_GlobalScalarEnumCaseIdVec *out) {
    return jazz_node__node__physical__NodeState__physical_enum_cases(&jzn_scalar_case_vt, s, t, c, path, (jz_vec *)out);
}
jz_err jazz_node__node__physical__NodeState__physical_nested_payload_enum_cases(const jazz_node_NodeState *s, jazz_types_PhysicalTableId t,
                                                                               jazz_types_PhysicalColumnId c, const jz_str *path,
                                                                               jazz_node_GlobalEnumCaseIdVec *out) {
    return jazz_node__node__physical__NodeState__physical_enum_cases(&jzn_payload_case_vt, s, t, c, path, (jz_vec *)out);
}

/* iterate root-then-nested occurrences of one column: (path or NULL, cases) */
typedef struct jzn_occ { const jz_str *path; const jz_vec *cases; } jzn_occ;
typedef JZ_VEC(jzn_occ) jzn_occ_vec;
static void jzn_collect_occurrences(const jzn_EnumCaseVt *vt, const jazz_node_TablePhysicalMapping *mapping,
                                    jazz_types_PhysicalColumnId column_id, bool skip_empty_nested, jzn_occ_vec *out) {
    const jz_vec *root = jz_btree_get(vt->root(mapping), &column_id);
    if (root) {
        jzn_occ o = { NULL, root };
        JZ_VEC_PUSH(*out, o);
    }
    const jz_btree *nested = jz_btree_get(vt->nested(mapping), &column_id);
    if (nested) {
        jz_str *path;
        jz_vec *cases;
        JZ_BTREE_FOR(nested, path, cases) {
            if (skip_empty_nested && cases->len == 0) continue;
            jzn_occ o = { path, cases };
            JZ_VEC_PUSH(*out, o);
        }
    }
}

static jz_err jazz_node__node__physical__NodeState__install_authored_enum_remaps(const jzn_EnumCaseVt *vt, const jazz_node_NodeState *s,
                                                                                jazz_node_EnumOccurrenceRemaps *remaps,
                                                                                const jazz_node_TablePhysicalMapping *target_mapping,
                                                                                jazz_types_PhysicalColumnId column_id) {
    jzn_occ_vec occurrences = {0};
    jzn_collect_occurrences(vt, target_mapping, column_id, false, &occurrences);
    jz_str root_path = jz_str_from("root");
    jz_err err = JZ_OK;
    for (u32 o = 0; o < occurrences.len; o++) {
        const jz_str *path = occurrences.ptr[o].path;
        const jz_vec *target_cases = occurrences.ptr[o].cases;
        /* Bootstrap: before hydration the target's own registry is complete. */
        jz_vec physical_cases;
        jazz_node__node__physical__NodeState__physical_enum_cases_or(vt, s, target_mapping->table_id, column_id, path, target_cases,
                                                                    &physical_cases);
        groove_OptU32Vec tags = {0};
        const void **children = jz_alloc((usize)(physical_cases.len ? physical_cases.len : 1) * sizeof(void *));
        for (u32 i = 0; i < physical_cases.len; i++) {
            const u8 *identity = (const u8 *)physical_cases.ptr + (usize)i * vt->elem;
            children[i] = identity;
            i64 tag = -1;
            for (u32 j = 0; j < target_cases->len; j++) {
                const u8 *candidate = (const u8 *)target_cases->ptr + (usize)j * vt->elem;
                if (jz_uuid_eq((const jz_uuid *)candidate, (const jz_uuid *)identity)) {
                    if ((u64)j > vt->tag_max) err = jazz_node__node__physical__enum_error(vt->target_tag_exhausted, path);
                    tag = j;
                    break;
                }
            }
            if (JZ_IS_ERR(err)) break;
            JZ_VEC_PUSH(tags, tag);
        }
        if (JZ_IS_ERR(err)) {
            jz_vec_drop(&tags, sizeof(i64), NULL);
            jz_free(children);
            jz_vec_drop(&physical_cases, vt->elem, NULL);
            break;
        }
        vt->install(remaps, path ? path : &root_path, &tags, children, physical_cases.len);
        if (!path) vt->after_authored_root(remaps);
        jz_free(children);
        jz_vec_drop(&physical_cases, vt->elem, NULL);
    }
    jz_str_drop(&root_path);
    jz_vec_drop(&occurrences, sizeof(jzn_occ), NULL);
    return err;
}

/* Physical-to-authored side of the enum interning boundary for one user cell. */
jz_err jazz_node__node__physical__NodeState__physical_to_authored_enum_remaps(const jazz_node_NodeState *s,
                                                                             const jazz_node_TablePhysicalMapping *target_mapping,
                                                                             jazz_types_PhysicalColumnId column_id,
                                                                             jazz_node_EnumOccurrenceRemaps *out) {
    groove_RecursiveEnumRemaps_init(out);
    jz_err e = jazz_node__node__physical__NodeState__install_authored_enum_remaps(&jzn_scalar_case_vt, s, out, target_mapping, column_id);
    if (!JZ_IS_ERR(e)) e = jazz_node__node__physical__NodeState__install_authored_enum_remaps(&jzn_payload_case_vt, s, out, target_mapping, column_id);
    if (JZ_IS_ERR(e)) groove_RecursiveEnumRemaps_drop(out);
    return e;
}

static jz_err jazz_node__node__physical__NodeState__install_copied_enum_remaps(const jzn_EnumCaseVt *vt, const jazz_node_NodeState *s,
                                                                              jazz_node_EnumOccurrenceRemaps *remaps,
                                                                              const jazz_node_TablePhysicalMapping *source_mapping,
                                                                              jazz_types_PhysicalColumnId source_column_id,
                                                                              const jazz_node_TablePhysicalMapping *target_mapping,
                                                                              jazz_types_PhysicalColumnId target_column_id) {
    jzn_occ_vec occurrences = {0};
    jzn_collect_occurrences(vt, source_mapping, source_column_id, true, &occurrences);
    jz_str root_path = jz_str_from("root");
    jz_err err = JZ_OK;
    for (u32 o = 0; o < occurrences.len; o++) {
        const jz_str *path = occurrences.ptr[o].path;
        jz_vec source_cases, target_cases = {0};
        jazz_node__node__physical__NodeState__physical_enum_cases_or(vt, s, source_mapping->table_id, source_column_id, path,
                                                                    occurrences.ptr[o].cases, &source_cases);
        const jz_vec *fallback = jazz_node__node__physical__EnumCase__occurrence(vt, target_mapping, target_column_id, path);
        if (fallback)
            jazz_node__node__physical__NodeState__physical_enum_cases_or(vt, s, target_mapping->table_id, target_column_id, path,
                                                                        fallback, &target_cases);
        groove_OptU32Vec tags = {0};
        const void **children = jz_alloc((usize)(source_cases.len ? source_cases.len : 1) * sizeof(void *));
        for (u32 ordinal = 0; ordinal < source_cases.len; ordinal++) {
            i64 tag = -1;
            children[ordinal] = NULL;
            if (ordinal < target_cases.len) {
                if ((u64)ordinal > vt->tag_max) { err = jazz_node__node__physical__enum_error(vt->copied_tag_exhausted, path); break; }
                tag = ordinal;
                children[ordinal] = (const u8 *)target_cases.ptr + (usize)ordinal * vt->elem;
            }
            JZ_VEC_PUSH(tags, tag);
        }
        if (!JZ_IS_ERR(err)) vt->install(remaps, path ? path : &root_path, &tags, children, source_cases.len);
        else jz_vec_drop(&tags, sizeof(i64), NULL);
        jz_free(children);
        jz_vec_drop(&source_cases, vt->elem, NULL);
        jz_vec_drop(&target_cases, vt->elem, NULL);
        if (JZ_IS_ERR(err)) break;
    }
    jz_str_drop(&root_path);
    jz_vec_drop(&occurrences, sizeof(jzn_occ), NULL);
    return err;
}

/* Re-encode a source physical enum occurrence into a distinct copied column's
 * physical registry: each physical column owns an independent registry. */
jz_err jazz_node__node__physical__NodeState__physical_copy_enum_remaps(const jazz_node_NodeState *s,
                                                                      const jazz_node_TablePhysicalMapping *source_mapping,
                                                                      jazz_types_PhysicalColumnId source_column_id,
                                                                      const jazz_node_TablePhysicalMapping *target_mapping,
                                                                      jazz_types_PhysicalColumnId target_column_id,
                                                                      const groove_ValueType *source_column_type,
                                                                      const groove_ValueType *target_column_type,
                                                                      jazz_node_EnumOccurrenceRemaps *out) {
    groove_RecursiveEnumRemaps_init(out);
    jz_err e = jazz_node__node__physical__NodeState__install_copied_enum_remaps(&jzn_scalar_case_vt, s, out, source_mapping,
                                                                               source_column_id, target_mapping, target_column_id);
    if (!JZ_IS_ERR(e))
        e = jazz_node__node__physical__NodeState__install_copied_enum_remaps(&jzn_payload_case_vt, s, out, source_mapping,
                                                                            source_column_id, target_mapping, target_column_id);
    if (!JZ_IS_ERR(e)) {
        jz_str root = jz_str_from("root");
        e = jazz_node__node__physical__bootstrap_copy_enum_remaps(source_column_type, target_column_type, &root, out);
        jz_str_drop(&root);
    }
    if (JZ_IS_ERR(e)) groove_RecursiveEnumRemaps_drop(out);
    return e;
}
