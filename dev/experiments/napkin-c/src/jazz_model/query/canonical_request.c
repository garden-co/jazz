/* query/canonical_request.rs: canonical ordering and identity preimages for queries and bindings. */
#include "jazz_model/jazz_model.h"

#define Q(x) jazz_model__query__##x

/* ---------------------------------------------------------------- primitive writers */
static void push_byte(jz_bytes *b, u8 v) { JZ_VEC_PUSH(*b, v); }
static void put_raw(jz_bytes *b, const void *p, u32 n) { jz_vec_extend(b, p, n, 1); }
static void put_len(jz_bytes *b, u64 len) {
    u8 be[8];
    for (u32 i = 0; i < 8; i++) be[i] = (u8)(len >> (56 - 8 * i));
    put_raw(b, be, 8);
}
static void put_be(jz_bytes *b, u64 v, u32 width) {
    u8 be[8];
    for (u32 i = 0; i < width; i++) be[i] = (u8)(v >> (8 * (width - 1 - i)));
    put_raw(b, be, width);
}
static void put_bytes(jz_bytes *b, const u8 *p, u32 n) { put_len(b, n); put_raw(b, p, n); }
static void put_str(jz_bytes *b, const jz_str *s) { put_bytes(b, (const u8 *)s->ptr, s->len); }
static void put_key(jz_bytes *b, jz_bytes *key) { put_bytes(b, key->ptr, key->len); jz_vec_drop(key, 1, NULL); }
/* Writes a one-byte tag followed by `payload`. */
static void put_tagged(jz_bytes *b, u8 tag, const void *payload, u32 n) { push_byte(b, tag); put_raw(b, payload, n); }
static void put_opt_str(jz_bytes *b, const jazz_model_OptStr *v) {
    if (v->some) { push_byte(b, 1); put_str(b, &v->s); }
    else push_byte(b, 0);
}

typedef void (*KeyFn)(const void *item, jz_bytes *out);
/* A length-prefixed list of each item's canonical key. */
static void put_keys(jz_bytes *b, const void *items, u32 n, usize elem, KeyFn key) {
    put_len(b, n);
    for (u32 i = 0; i < n; i++) {
        jz_bytes k = {0};
        key((const u8 *)items + (usize)i * elem, &k);
        put_key(b, &k);
    }
}
#define PUT_KEYS(b, vec, fn) put_keys(b, (vec).ptr, (vec).len, sizeof(*(vec).ptr), (KeyFn)(fn))

static int bytes_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    return jz_slice_cmp(jz_bytes_slice(a), jz_bytes_slice(b));
}
/* A length-prefixed list of keys in sorted order, for operand lists whose order does not change meaning. */
static void put_sorted_keys(jz_bytes *b, jz_bytes *keys, u32 n) {
    jz_sort(keys, n, sizeof(jz_bytes), bytes_cmp, NULL);
    put_len(b, n);
    for (u32 i = 0; i < n; i++) put_key(b, &keys[i]);
}

/* ---------------------------------------------------------------- sort_by_key / dedup */
typedef struct KeyedIndex { jz_bytes key; u32 idx; } KeyedIndex;
static int keyed_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    return jz_slice_cmp(jz_bytes_slice(&((const KeyedIndex *)a)->key), jz_bytes_slice(&((const KeyedIndex *)b)->key));
}
/* slice::sort_by_key (stable) with a Vec<u8> key */
static void sort_by_key(void *base, u32 n, usize elem, KeyFn key) {
    if (n < 2) return;
    KeyedIndex *keys = jz_alloc(sizeof(KeyedIndex) * n);
    for (u32 i = 0; i < n; i++) {
        keys[i].key = (jz_bytes){0};
        key((const u8 *)base + (usize)i * elem, &keys[i].key);
        keys[i].idx = i;
    }
    jz_sort(keys, n, sizeof(KeyedIndex), keyed_cmp, NULL);
    u8 *tmp = jz_alloc(elem * n);
    for (u32 i = 0; i < n; i++) {
        memcpy(tmp + (usize)i * elem, (const u8 *)base + (usize)keys[i].idx * elem, elem);
        jz_vec_drop(&keys[i].key, 1, NULL);
    }
    memcpy(base, tmp, elem * n);
    jz_free(tmp);
    jz_free(keys);
}
#define SORT_BY_KEY(vec, fn) sort_by_key((vec).ptr, (vec).len, sizeof(*(vec).ptr), (KeyFn)(fn))

static int inherits_ne(const void *a, const void *b) { return !Q(InheritsVia__PartialEq__eq)(a, b); }
static int array_subquery_ne(const void *a, const void *b) { return !Q(ArraySubquery__PartialEq__eq)(a, b); }
static int include_ne(const void *a, const void *b) { return !Q(Include__PartialEq__eq)(a, b); }
static int str_ne(const void *a, const void *b) { return !jz_str_eq(a, b); }
static int str_cmp(const void *a, const void *b, void *ctx) { (void)ctx; return jz_str_cmp(a, b); }
/* #[derive(Ord)] Include { path, join_mode, require } */
static int include_cmp(const void *pa, const void *pb, void *ctx) {
    (void)ctx;
    const jazz_model_Include *a = pa, *b = pb;
    int c = jz_str_cmp(&a->path, &b->path);
    if (c) return c;
    if (a->join_mode != b->join_mode) return a->join_mode < b->join_mode ? -1 : 1;
    return (int)a->require - (int)b->require;
}
static void sort_dedup_strs(jazz_model_StrVec *v) {
    jz_sort(v->ptr, v->len, sizeof(jz_str), str_cmp, NULL);
    jz_vec_dedup(v, sizeof(jz_str), str_ne, jazz_model__str_drop_v);
}

/* ---------------------------------------------------------------- key functions */
static void canonical_operand_key(const jazz_model_Operand *operand, jz_bytes *b);
static void canonical_predicate_key(const jazz_model_Predicate *predicate, jz_bytes *b);
static void put_value(jz_bytes *b, const groove_Value *value);
static void put_column_type(jz_bytes *b, const groove_ValueType *ty);

static void put_operands(jz_bytes *b, u8 tag, const jazz_model_Operand *left, const jazz_model_Operand *right) {
    push_byte(b, tag);
    jz_bytes k = {0};
    canonical_operand_key(left, &k); put_key(b, &k);
    k = (jz_bytes){0};
    canonical_operand_key(right, &k); put_key(b, &k);
}
/* Like put_operands, but for symmetric comparisons whose operand order does not change meaning. */
static void put_unordered_operands(jz_bytes *b, u8 tag, const jazz_model_Operand *left, const jazz_model_Operand *right) {
    push_byte(b, tag);
    jz_bytes ops[2] = {{0}, {0}};
    canonical_operand_key(left, &ops[0]);
    canonical_operand_key(right, &ops[1]);
    if (jz_slice_cmp(jz_bytes_slice(&ops[0]), jz_bytes_slice(&ops[1])) > 0) { jz_bytes t = ops[0]; ops[0] = ops[1]; ops[1] = t; }
    put_key(b, &ops[0]);
    put_key(b, &ops[1]);
}

static void canonical_predicate_key(const jazz_model_Predicate *p, jz_bytes *b) {
    switch (p->tag) {
    case JAZZ_PRED_ALL:
    case JAZZ_PRED_ANY: {
        push_byte(b, p->tag == JAZZ_PRED_ALL ? 'A' : 'O');
        u32 n = p->children.len;
        jz_bytes *keys = jz_zalloc(sizeof(jz_bytes) * (n ? n : 1));
        for (u32 i = 0; i < n; i++) canonical_predicate_key(&p->children.ptr[i], &keys[i]);
        put_sorted_keys(b, keys, n);
        jz_free(keys);
        break;
    }
    case JAZZ_PRED_NOT: {
        push_byte(b, '!');
        jz_bytes k = {0};
        canonical_predicate_key(p->inner, &k);
        put_key(b, &k);
        break;
    }
    case JAZZ_PRED_EQ: put_unordered_operands(b, 'e', &p->left, &p->right); break;
    case JAZZ_PRED_NE: put_unordered_operands(b, 'n', &p->left, &p->right); break;
    case JAZZ_PRED_IN: {
        push_byte(b, 'i');
        jz_bytes k = {0};
        canonical_operand_key(&p->left, &k);
        put_key(b, &k);
        u32 n = p->values.len;
        jz_bytes *keys = jz_zalloc(sizeof(jz_bytes) * (n ? n : 1));
        for (u32 i = 0; i < n; i++) canonical_operand_key(&p->values.ptr[i], &keys[i]);
        put_sorted_keys(b, keys, n);
        jz_free(keys);
        break;
    }
    case JAZZ_PRED_GT: put_operands(b, 'g', &p->left, &p->right); break;
    case JAZZ_PRED_GTE: put_operands(b, 'G', &p->left, &p->right); break;
    case JAZZ_PRED_LT: put_operands(b, 't', &p->left, &p->right); break;
    case JAZZ_PRED_LTE: put_operands(b, 'T', &p->left, &p->right); break;
    case JAZZ_PRED_CONTAINS: put_operands(b, 'c', &p->left, &p->right); break;
    case JAZZ_PRED_IS_NULL: {
        push_byte(b, '0');
        jz_bytes k = {0};
        canonical_operand_key(&p->left, &k);
        put_key(b, &k);
        break;
    }
    case JAZZ_PRED_ENUM_MATCH: {
        push_byte(b, 'm');
        put_str(b, &p->column);
        put_str(b, &p->case_);
        jz_bytes k = {0};
        canonical_predicate_key(p->inner, &k);
        put_key(b, &k);
        break;
    }
    }
}

static void canonical_operand_key(const jazz_model_Operand *o, jz_bytes *b) {
    switch (o->tag) {
    case JAZZ_OPERAND_COLUMN: push_byte(b, 'c'); put_str(b, &o->name); break;
    case JAZZ_OPERAND_PARAM: push_byte(b, 'p'); put_str(b, &o->name); break;
    case JAZZ_OPERAND_CLAIM: push_byte(b, 'a'); put_str(b, &o->name); break;
    case JAZZ_OPERAND_LITERAL: push_byte(b, 'l'); put_value(b, &o->literal); break;
    }
}

static void canonical_join_correlation_key(const jazz_model_JoinCorrelation *c, jz_bytes *b) {
    put_str(b, &c->join_column);
    put_str(b, &c->source_column);
}

static void put_join_target(jz_bytes *b, u8 target) {
    if (target == JAZZ_JOIN_TARGET_ROW_ID) push_byte(b, 'r');
    else if (target == JAZZ_JOIN_TARGET_UNCORRELATED) push_byte(b, 'e');
}

static void canonical_join_key(const jazz_model_JoinVia *join, jz_bytes *b) {
    put_str(b, &join->table);
    put_str(b, &join->on_column);
    put_join_target(b, join->target);
    if (join->source_column.some) { push_byte(b, 's'); put_str(b, &join->source_column.s); }
    if (join->source_lookup) {
        push_byte(b, 'l');
        put_str(b, &join->source_lookup->table);
        put_str(b, &join->source_lookup->row_id_source_column);
        put_str(b, &join->source_lookup->value_column);
    }
    if (join->correlated_filters.len) { push_byte(b, 'c'); PUT_KEYS(b, join->correlated_filters, canonical_join_correlation_key); }
    if (join->nested_joins.len) { push_byte(b, 'j'); PUT_KEYS(b, join->nested_joins, canonical_join_key); }
    JZ_VEC_FOR(const jazz_model_Predicate, f, join->filters) {
        jz_bytes k = {0};
        canonical_predicate_key(f, &k);
        put_key(b, &k);
    }
}

static void canonical_inherits_key(const jazz_model_InheritsVia *i, jz_bytes *b) {
    static const u8 OPS[] = {'s', 'i', 'u', 'd'};
    put_str(b, &i->parent_column);
    push_byte(b, OPS[i->operation]);
    if (i->has_max_depth) { push_byte(b, 'd'); put_len(b, i->max_depth); }
    else push_byte(b, 'u');
}

static void Q(canonical_reachable_key_with_seed_type)(const jazz_model_ReachableVia *r, const groove_ValueType *seed_value_type, jz_bytes *b) {
    put_str(b, &r->access_table);
    put_str(b, &r->access_row_column);
    put_str(b, &r->access_team_column);
    put_join_target(b, r->access_team_target);
    jz_bytes k = {0};
    canonical_operand_key(&r->from, &k);
    put_key(b, &k);
    PUT_KEYS(b, r->access_filters, canonical_predicate_key);
    put_str(b, &r->edge_table);
    put_str(b, &r->edge_member_column);
    put_str(b, &r->edge_parent_column);
    if (r->bound.tag == JAZZ_BOUND_FIXPOINT) push_byte(b, 'f');
    else { push_byte(b, 'd'); put_len(b, r->bound.max_depth); }
    JZ_VEC_FOR(const jazz_model_Predicate, f, r->edge_filters) {
        k = (jz_bytes){0};
        canonical_predicate_key(f, &k);
        put_key(b, &k);
    }
    if (r->seed) {
        const jazz_model_ReachableSeed *seed = r->seed;
        push_byte(b, 's');
        put_str(b, &seed->table);
        if (seed->user_column.some && seed->user_claim.some) {
            push_byte(b, 'u');
            put_str(b, &seed->user_column.s);
            put_str(b, &seed->user_claim.s);
            if (seed_value_type) { push_byte(b, 't'); put_column_type(b, seed_value_type); }
        }
        put_str(b, &seed->team_column);
        PUT_KEYS(b, seed->filters, canonical_predicate_key);
    }
}

static void canonical_reachable_key(const jazz_model_ReachableVia *r, jz_bytes *b) {
    Q(canonical_reachable_key_with_seed_type)(r, NULL, b);
}

static jz_err canonical_reachable_key_for_schema(const jazz_model_ReachableVia *r, const jazz_model_RuntimeSchema *schema, jz_bytes *b) {
    const groove_ValueType *seed_value_type = NULL;
    if (r->seed && r->seed->user_column.some) {
        const jazz_model_TableSchema *table;
        JZ_TRY(Q(schema_table)(schema, jz_str_slice(&r->seed->table), &table));
        JZ_TRY(Q(planner_column_type)(table, jz_str_slice(&r->seed->user_column.s), &seed_value_type));
    }
    Q(canonical_reachable_key_with_seed_type)(r, seed_value_type, b);
    return JZ_OK;
}

static void canonical_policy_branch_key(const jazz_model_PolicyBranch *branch, jz_bytes *b) {
    PUT_KEYS(b, branch->filters, canonical_predicate_key);
    PUT_KEYS(b, branch->joins, canonical_join_key);
    PUT_KEYS(b, branch->reachable, canonical_reachable_key);
    PUT_KEYS(b, branch->inherits, canonical_inherits_key);
}

static jz_err try_put_reachable_keys(jz_bytes *b, const jazz_model_ReachableViaVec *items, const jazz_model_RuntimeSchema *schema) {
    put_len(b, items->len);
    JZ_VEC_FOR(const jazz_model_ReachableVia, r, *items) {
        jz_bytes k = {0};
        jz_err e = canonical_reachable_key_for_schema(r, schema, &k);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&k, 1, NULL); return e; }
        put_key(b, &k);
    }
    return JZ_OK;
}

static jz_err canonical_policy_branch_key_for_schema(const jazz_model_PolicyBranch *branch, const jazz_model_RuntimeSchema *schema, jz_bytes *b) {
    PUT_KEYS(b, branch->filters, canonical_predicate_key);
    PUT_KEYS(b, branch->joins, canonical_join_key);
    JZ_TRY(try_put_reachable_keys(b, &branch->reachable, schema));
    PUT_KEYS(b, branch->inherits, canonical_inherits_key);
    return JZ_OK;
}

static void Q(canonical_aggregate_key)(const jazz_model_Aggregate *a, jz_bytes *b) {
    static const u8 FNS[] = {'c', 's', 'a', 'n', 'x'};
    push_byte(b, FNS[a->function]);
    if (a->column.some) put_str(b, &a->column.s);
    put_str(b, &a->alias);
}

static void put_order_by(jz_bytes *b, const jazz_model_OrderByVec *order_by) {
    push_byte(b, 'o');
    put_len(b, order_by->len);
    JZ_VEC_FOR(const jazz_model_OrderBy, o, *order_by) {
        put_str(b, &o->column);
        push_byte(b, o->direction == JAZZ_ORDER_ASC ? 'a' : 'd');
    }
}
static void put_select(jz_bytes *b, const jazz_model_StrVec *select) {
    push_byte(b, 's');
    put_len(b, select->len);
    JZ_VEC_FOR(const jz_str, c, *select) put_str(b, c);
}

static void canonical_array_subquery_key(const jazz_model_ArraySubquery *s, jz_bytes *b) {
    static const u8 REQ[] = {'?', '+', '='};
    put_str(b, &s->column_name);
    put_str(b, &s->table);
    put_str(b, &s->inner_column);
    put_str(b, &s->outer_column);
    PUT_KEYS(b, s->filters, canonical_predicate_key);
    if (s->has_select) put_select(b, &s->select);
    if (s->order_by.len) put_order_by(b, &s->order_by);
    if (s->has_limit) { push_byte(b, 'l'); put_len(b, s->limit); }
    if (s->offset != 0) { push_byte(b, 'f'); put_len(b, s->offset); }
    push_byte(b, REQ[s->requirement]);
    if (s->nested_arrays.len) { push_byte(b, 'n'); PUT_KEYS(b, s->nested_arrays, canonical_array_subquery_key); }
}

static void canonical_flat_join_key(const jazz_model_FlatJoin *fj, jz_bytes *b) {
    put_opt_str(b, &fj->root_alias);
    put_len(b, fj->sources.len);
    JZ_VEC_FOR(const jazz_model_FlatJoinSource, s, fj->sources) {
        put_str(b, &s->table);
        put_opt_str(b, &s->alias);
        put_str(b, &s->on.left);
        put_str(b, &s->on.right);
    }
}

/* ---------------------------------------------------------------- normalize */
static void normalize_join(jazz_model_JoinVia *join) {
    SORT_BY_KEY(join->correlated_filters, canonical_join_correlation_key);
    JZ_VEC_FOR(jazz_model_JoinVia, nested, join->nested_joins) {
        SORT_BY_KEY(nested->filters, canonical_predicate_key);
        normalize_join(nested);
    }
    SORT_BY_KEY(join->nested_joins, canonical_join_key);
}

static void normalize_array_subquery(jazz_model_ArraySubquery *s) {
    SORT_BY_KEY(s->filters, canonical_predicate_key);
    if (s->has_select) sort_dedup_strs(&s->select);
    JZ_VEC_FOR(jazz_model_ArraySubquery, nested, s->nested_arrays) normalize_array_subquery(nested);
    SORT_BY_KEY(s->nested_arrays, canonical_array_subquery_key);
    jz_vec_dedup(&s->nested_arrays, sizeof(jazz_model_ArraySubquery), array_subquery_ne, Q(ArraySubquery__drop_v));
}

static void normalize_reachable(jazz_model_ReachableVia *r) {
    SORT_BY_KEY(r->access_filters, canonical_predicate_key);
    SORT_BY_KEY(r->edge_filters, canonical_predicate_key);
    if (r->seed) SORT_BY_KEY(r->seed->filters, canonical_predicate_key);
}

void Q(normalize_query)(const jazz_model_Query *src, jazz_model_Query *out) {
    Q(Query__Clone__clone)(out, src);
    jazz_model_Query *q = out;
    SORT_BY_KEY(q->filters, canonical_predicate_key);
    JZ_VEC_FOR(jazz_model_JoinVia, join, q->joins) {
        SORT_BY_KEY(join->filters, canonical_predicate_key);
        normalize_join(join);
    }
    SORT_BY_KEY(q->joins, canonical_join_key);
    JZ_VEC_FOR(jazz_model_PolicyBranch, branch, q->policy_branches) {
        SORT_BY_KEY(branch->filters, canonical_predicate_key);
        JZ_VEC_FOR(jazz_model_JoinVia, join, branch->joins) {
            SORT_BY_KEY(join->filters, canonical_predicate_key);
            normalize_join(join);
        }
        SORT_BY_KEY(branch->joins, canonical_join_key);
        JZ_VEC_FOR(jazz_model_ReachableVia, r, branch->reachable) normalize_reachable(r);
        SORT_BY_KEY(branch->reachable, canonical_reachable_key);
        SORT_BY_KEY(branch->inherits, canonical_inherits_key);
        jz_vec_dedup(&branch->inherits, sizeof(jazz_model_InheritsVia), inherits_ne, Q(InheritsVia__drop_v));
    }
    SORT_BY_KEY(q->policy_branches, canonical_policy_branch_key);
    JZ_VEC_FOR(jazz_model_ReachableVia, r, q->reachable) normalize_reachable(r);
    SORT_BY_KEY(q->reachable, canonical_reachable_key);
    SORT_BY_KEY(q->inherits, canonical_inherits_key);
    jz_vec_dedup(&q->inherits, sizeof(jazz_model_InheritsVia), inherits_ne, Q(InheritsVia__drop_v));
    jz_sort(q->includes.ptr, q->includes.len, sizeof(jazz_model_Include), include_cmp, NULL);
    jz_vec_dedup(&q->includes, sizeof(jazz_model_Include), include_ne, Q(Include__drop_v));
    JZ_VEC_FOR(jazz_model_ArraySubquery, s, q->array_subqueries) normalize_array_subquery(s);
    SORT_BY_KEY(q->array_subqueries, canonical_array_subquery_key);
    jz_vec_dedup(&q->array_subqueries, sizeof(jazz_model_ArraySubquery), array_subquery_ne, Q(ArraySubquery__drop_v));
    if (q->has_select) sort_dedup_strs(&q->select);
    if (q->aggregate) SORT_BY_KEY(q->aggregate->aggregates, Q(canonical_aggregate_key));
}

/* ---------------------------------------------------------------- relation identity */
static void put_json_canonical(const jz_json *value, jz_bytes *out) {
    switch (value->kind) {
    case JZ_JSON_NULL: push_byte(out, 'n'); break;
    case JZ_JSON_BOOL: push_byte(out, value->v.b ? 't' : 'f'); break;
    case JZ_JSON_U64: case JZ_JSON_I64: case JZ_JSON_F64: {
        push_byte(out, '#');
        jz_str text = {0};
        jz_json_write(value, &text); /* Number::to_string */
        put_str(out, &text);
        jz_str_drop(&text);
        break;
    }
    case JZ_JSON_STR: push_byte(out, 's'); put_str(out, &value->v.s); break;
    case JZ_JSON_ARR:
        push_byte(out, '[');
        put_len(out, value->v.arr.len);
        for (u32 i = 0; i < value->v.arr.len; i++) put_json_canonical(value->v.arr.ptr[i], out);
        break;
    case JZ_JSON_OBJ: {
        push_byte(out, '{');
        put_len(out, value->v.obj.len);
        /* entries sorted lexicographically by key: the object map iterates in key order */
        const jz_str *k;
        jz_json **v;
        JZ_BTREE_FOR(&value->v.obj, k, v) {
            put_str(out, k);
            put_json_canonical(*v, out);
        }
        break;
    }
    }
}

/* Relation query identity is an explicitly canonical JSON tree: object keys sort lexicographically
 * and scalar JSON values retain their exact content. */
static jz_err canonical_relation_query_key(const jazz_model_RelationQuery *query, jz_bytes *bytes) {
    /* serde_json::to_value(query) */
    jz_str text = {0};
    jz_json_w w = {&text, 0, {0}};
    Q(RelationQuery__Serialize__serialize_json)(&w, query);
    jz_json *value = NULL;
    jz_err e = jz_json_parse(jz_str_slice(&text), &value);
    jz_str_drop(&text);
    if (JZ_IS_ERR(e)) {
        jz_err q = Q(QueryError__new)(JAZZ_QE_UNSUPPORTED_RELATION_QUERY, "unsupported relation query: encode relation query: %.*s",
                                      e.msg ? (int)e.msg->len : 0, e.msg ? e.msg->ptr : "");
        jz_err_drop(&e);
        return q;
    }
    put_raw(bytes, "jazz-relation-v1", 16);
    put_json_canonical(value, bytes);
    jz_json_free(value);
    return JZ_OK;
}

jz_err Q(canonical_query_bytes_for_schema)(const jazz_model_Query *q, const jazz_model_RuntimeSchema *schema, jz_bytes *out) {
    jz_bytes b = {0};
    jz_err e = JZ_OK;
    put_raw(&b, "jazz-query-v0", 13);
    put_str(&b, &q->table);
    PUT_KEYS(&b, q->filters, canonical_predicate_key);
    PUT_KEYS(&b, q->joins, canonical_join_key);
    if (q->flat_join) {
        push_byte(&b, 'j');
        jz_bytes k = {0};
        canonical_flat_join_key(q->flat_join, &k);
        put_key(&b, &k);
    }
    if (q->policy_branches.len) {
        push_byte(&b, 'b');
        put_len(&b, q->policy_branches.len);
        JZ_VEC_FOR(const jazz_model_PolicyBranch, branch, q->policy_branches) {
            jz_bytes k = {0};
            e = canonical_policy_branch_key_for_schema(branch, schema, &k);
            if (JZ_IS_ERR(e)) { jz_vec_drop(&k, 1, NULL); goto fail; }
            put_key(&b, &k);
        }
    }
    if (q->reachable.len) {
        push_byte(&b, 'r');
        e = try_put_reachable_keys(&b, &q->reachable, schema);
        if (JZ_IS_ERR(e)) goto fail;
    }
    if (q->inherits.len) { push_byte(&b, 'i'); PUT_KEYS(&b, q->inherits, canonical_inherits_key); }
    put_len(&b, q->includes.len);
    JZ_VEC_FOR(const jazz_model_Include, inc, q->includes) {
        put_str(&b, &inc->path);
        push_byte(&b, inc->join_mode == JAZZ_JOIN_MODE_INNER ? 'i' : 'h');
        push_byte(&b, inc->require ? 1 : 0);
    }
    if (q->array_subqueries.len) { push_byte(&b, 'y'); PUT_KEYS(&b, q->array_subqueries, canonical_array_subquery_key); }
    if (q->has_select) put_select(&b, &q->select);
    if (q->order_by.len) put_order_by(&b, &q->order_by);
    if (q->aggregate) {
        push_byte(&b, 'a');
        PUT_KEYS(&b, q->aggregate->aggregates, Q(canonical_aggregate_key));
        put_opt_str(&b, &q->aggregate->group_by);
    }
    if (q->has_limit || q->offset != 0) {
        push_byte(&b, 'p');
        if (q->has_limit) { push_byte(&b, 1); put_len(&b, q->limit); }
        else push_byte(&b, 0);
        put_len(&b, q->offset);
    }
    if (q->relation) {
        push_byte(&b, 'u');
        jz_bytes k = {0};
        e = canonical_relation_query_key(q->relation, &k);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&k, 1, NULL); goto fail; }
        put_key(&b, &k);
    }
    *out = b;
    return JZ_OK;
fail:
    jz_vec_drop(&b, 1, NULL);
    return e;
}

/* ---------------------------------------------------------------- bindings */
static jz_err validate_public_column_type(const groove_ValueType *t);

static jz_err validate_descriptor_fields(groove_RecordDescriptor d) {
    u32 n = 0;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(d, &n);
    for (u32 i = 0; i < n; i++) JZ_TRY(validate_public_column_type(&fields[i].value_type));
    return JZ_OK;
}

static jz_err validate_public_column_type(const groove_ValueType *t) {
    switch (t->tag) {
    case GROOVE_VT_TUPLE:
        JZ_VEC_FOR(const groove_ValueType, it, t->u.tuple) JZ_TRY(validate_public_column_type(it));
        return JZ_OK;
    case GROOVE_VT_ARRAY: case GROOVE_VT_NULLABLE: return validate_public_column_type(t->u.inner);
    case GROOVE_VT_RECORD: return validate_descriptor_fields(t->u.record);
    case GROOVE_VT_ENUM:
        JZ_VEC_FOR(const groove_EnumCase, c, t->u.enum_->cases) JZ_TRY(validate_descriptor_fields(c->payload));
        return JZ_OK;
    case GROOVE_VT_INTERNAL: case GROOVE_VT_U48: return Q(QueryError__new)(JAZZ_QE_OPERAND_TYPE_MISMATCH, "operand type mismatch");
    default: return JZ_OK;
    }
}

static groove_ValueType record_type(groove_RecordDescriptor d) {
    groove_ValueType t;
    memset(&t, 0, sizeof(t));
    t.tag = GROOVE_VT_RECORD;
    t.u.record = d;
    return t;
}

/* Binding identities encode with the same tags as query literals, but only accept values a public caller can supply. */
jz_err Q(validate_binding_value)(const groove_Value *v) {
    switch (v->tag) {
    case GROOVE_V_TUPLE: case GROOVE_V_ARRAY:
        JZ_VEC_FOR(const groove_Value, it, v->u.items) JZ_TRY(Q(validate_binding_value)(it));
        return JZ_OK;
    case GROOVE_V_NULLABLE: return v->u.nullable ? Q(validate_binding_value)(v->u.nullable) : JZ_OK;
    case GROOVE_V_RECORD: {
        groove_ValueType t = record_type(v->u.record.descriptor);
        return validate_public_column_type(&t);
    }
    /* U48 is an engine-owned stamp width, never a public query binding. */
    case GROOVE_V_ENUM: case GROOVE_V_LARGE: case GROOVE_V_U48:
        return Q(QueryError__new)(JAZZ_QE_OPERAND_TYPE_MISMATCH, "operand type mismatch");
    default: return JZ_OK;
    }
}

jz_err Q(canonical_binding_bytes)(const jz_btree *values, jz_bytes *out) {
    jz_bytes b = {0};
    put_raw(&b, "jazz-binding-v0", 15);
    put_len(&b, values->len);
    const jz_str *name;
    const groove_Value *value;
    JZ_BTREE_FOR(values, name, value) {
        put_str(&b, name);
        jz_err e = Q(validate_binding_value)(value);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&b, 1, NULL); return e; }
        put_value(&b, value);
    }
    *out = b;
    return JZ_OK;
}

static groove_ValueType vt_scalar(u8 tag) { return groove__records__values__ValueType__scalar(tag); }
static groove_ValueType vt_boxed(u8 tag, groove_ValueType inner) {
    groove_ValueType t;
    memset(&t, 0, sizeof(t));
    t.tag = tag;
    t.u.inner = JZ_NEW(groove_ValueType);
    *t.u.inner = inner;
    return t;
}

jz_err Q(value_type)(const groove_Value *v, groove_ValueType *out) {
    switch (v->tag) {
    case GROOVE_V_U8: *out = vt_scalar(GROOVE_VT_U8); return JZ_OK;
    case GROOVE_V_U16: *out = vt_scalar(GROOVE_VT_U16); return JZ_OK;
    case GROOVE_V_U32: *out = vt_scalar(GROOVE_VT_U32); return JZ_OK;
    case GROOVE_V_U64: *out = vt_scalar(GROOVE_VT_U64); return JZ_OK;
    case GROOVE_V_I32: *out = vt_scalar(GROOVE_VT_I32); return JZ_OK;
    case GROOVE_V_I64: *out = vt_scalar(GROOVE_VT_I64); return JZ_OK;
    case GROOVE_V_F64: *out = vt_scalar(GROOVE_VT_F64); return JZ_OK;
    case GROOVE_V_BOOL: *out = vt_scalar(GROOVE_VT_BOOL); return JZ_OK;
    case GROOVE_V_STRING: *out = vt_scalar(GROOVE_VT_STRING); return JZ_OK;
    case GROOVE_V_BYTES: *out = vt_scalar(GROOVE_VT_BYTES); return JZ_OK;
    case GROOVE_V_UUID: *out = vt_scalar(GROOVE_VT_UUID); return JZ_OK;
    case GROOVE_V_ENUM_TAG: *out = vt_scalar(GROOVE_VT_U8); return JZ_OK;
    case GROOVE_V_TUPLE: {
        groove_ValueType t;
        memset(&t, 0, sizeof(t));
        t.tag = GROOVE_VT_TUPLE;
        JZ_VEC_FOR(const groove_Value, it, v->u.items) {
            groove_ValueType item;
            jz_err e = Q(value_type)(it, &item);
            if (JZ_IS_ERR(e)) { groove__records__values__ValueType__drop(&t); return e; }
            JZ_VEC_PUSH(t.u.tuple, item);
        }
        *out = t;
        return JZ_OK;
    }
    case GROOVE_V_ARRAY: {
        /* Infer the array type from its first element, but walk every element so an unsupported
         * nested value cannot reach the infallible canonical encoder. */
        groove_ValueType first;
        bool has_first = false;
        JZ_VEC_FOR(const groove_Value, it, v->u.items) {
            groove_ValueType t;
            jz_err e = Q(value_type)(it, &t);
            if (JZ_IS_ERR(e)) { if (has_first) groove__records__values__ValueType__drop(&first); return e; }
            if (!has_first) { first = t; has_first = true; }
            else groove__records__values__ValueType__drop(&t);
        }
        *out = vt_boxed(GROOVE_VT_ARRAY, has_first ? first : vt_scalar(GROOVE_VT_BYTES));
        return JZ_OK;
    }
    case GROOVE_V_NULLABLE: {
        if (!v->u.nullable) { *out = vt_boxed(GROOVE_VT_NULLABLE, vt_scalar(GROOVE_VT_BYTES)); return JZ_OK; }
        groove_ValueType inner;
        JZ_TRY(Q(value_type)(v->u.nullable, &inner));
        *out = vt_boxed(GROOVE_VT_NULLABLE, inner);
        return JZ_OK;
    }
    case GROOVE_V_RECORD: {
        groove_ValueType t = record_type(v->u.record.descriptor);
        JZ_TRY(validate_public_column_type(&t));
        *out = t;
        return JZ_OK;
    }
    default: /* Enum | Large | U48 */
        return Q(QueryError__new)(JAZZ_QE_OPERAND_TYPE_MISMATCH, "operand type mismatch");
    }
}

bool Q(value_matches_type)(const groove_Value *v, const groove_ValueType *t) {
    switch (v->tag) {
    case GROOVE_V_U8: return t->tag == GROOVE_VT_U8;
    case GROOVE_V_U16: return t->tag == GROOVE_VT_U16;
    case GROOVE_V_U32: return t->tag == GROOVE_VT_U32;
    case GROOVE_V_U64: return t->tag == GROOVE_VT_U64;
    case GROOVE_V_I32: return t->tag == GROOVE_VT_I32;
    case GROOVE_V_I64: return t->tag == GROOVE_VT_I64;
    case GROOVE_V_F64: return t->tag == GROOVE_VT_F64;
    case GROOVE_V_BOOL: return t->tag == GROOVE_VT_BOOL;
    case GROOVE_V_STRING: return t->tag == GROOVE_VT_STRING;
    case GROOVE_V_BYTES: return t->tag == GROOVE_VT_BYTES;
    case GROOVE_V_UUID: return t->tag == GROOVE_VT_UUID;
    case GROOVE_V_ENUM_TAG: return t->tag == GROOVE_VT_ENUM_TAG;
    case GROOVE_V_TUPLE:
        if (t->tag != GROOVE_VT_TUPLE || v->u.items.len != t->u.tuple.len) return false;
        for (u32 i = 0; i < v->u.items.len; i++)
            if (!Q(value_matches_type)(&v->u.items.ptr[i], &t->u.tuple.ptr[i])) return false;
        return true;
    case GROOVE_V_ARRAY:
        if (t->tag != GROOVE_VT_ARRAY) return false;
        JZ_VEC_FOR(const groove_Value, it, v->u.items) if (!Q(value_matches_type)(it, t->u.inner)) return false;
        return true;
    case GROOVE_V_NULLABLE:
        if (t->tag != GROOVE_VT_NULLABLE) return false;
        return !v->u.nullable || Q(value_matches_type)(v->u.nullable, t->u.inner);
    case GROOVE_V_RECORD: return t->tag == GROOVE_VT_RECORD && GROOVE_RECORD_DESCRIPTOR_EQ(v->u.record.descriptor, t->u.record);
    /* Indirect descriptors are engine-owned and cannot be supplied as a public query binding. */
    default: return false;
    }
}

/* ---------------------------------------------------------------- value / type encoders */
static void put_value(jz_bytes *b, const groove_Value *v) {
    switch (v->tag) {
    case GROOVE_V_U8: put_tagged(b, 1, &v->u.u8_, 1); break;
    case GROOVE_V_U16: push_byte(b, 2); put_be(b, v->u.u16_, 2); break;
    case GROOVE_V_U32: push_byte(b, 3); put_be(b, v->u.u32_, 4); break;
    case GROOVE_V_U64: push_byte(b, 4); put_be(b, v->u.u64_, 8); break;
    case GROOVE_V_I32: push_byte(b, 15); put_be(b, (u32)v->u.i32_, 4); break;
    case GROOVE_V_I64: push_byte(b, 14); put_be(b, (u64)v->u.i64_, 8); break;
    case GROOVE_V_F64: {
        u64 bits;
        memcpy(&bits, &v->u.f64_, 8);
        push_byte(b, 5); put_be(b, bits, 8);
        break;
    }
    case GROOVE_V_BOOL: { u8 x = v->u.b ? 1 : 0; put_tagged(b, 6, &x, 1); break; }
    case GROOVE_V_STRING: push_byte(b, 7); put_str(b, &v->u.s); break;
    case GROOVE_V_BYTES: push_byte(b, 8); put_bytes(b, v->u.bytes.ptr, v->u.bytes.len); break;
    case GROOVE_V_UUID: put_tagged(b, 9, v->u.uuid.b, 16); break;
    case GROOVE_V_ENUM_TAG: put_tagged(b, 10, &v->u.enum_tag, 1); break;
    case GROOVE_V_TUPLE:
    case GROOVE_V_ARRAY:
        push_byte(b, v->tag == GROOVE_V_TUPLE ? 11 : 12);
        put_len(b, v->u.items.len);
        JZ_VEC_FOR(const groove_Value, it, v->u.items) put_value(b, it);
        break;
    case GROOVE_V_NULLABLE:
        if (!v->u.nullable) { u8 z = 0; put_tagged(b, 13, &z, 1); }
        else { push_byte(b, 13); push_byte(b, 1); put_value(b, v->u.nullable); }
        break;
    case GROOVE_V_RECORD: {
        push_byte(b, 16);
        groove_ValueType t = record_type(v->u.record.descriptor);
        put_column_type(b, &t);
        put_bytes(b, v->u.record.raw.ptr, v->u.record.raw.len);
        break;
    }
    /* These Groove-native arms are rejected by value_type before a validated query reaches
     * canonicalization. Keep their fallback encodings total for defensive callers. */
    case GROOVE_V_ENUM: {
        push_byte(b, 17);
        put_be(b, v->u.enum_.tag, 4);
        groove_ValueType t = record_type(v->u.enum_.record.descriptor);
        put_column_type(b, &t);
        put_bytes(b, v->u.enum_.record.raw.ptr, v->u.enum_.record.raw.len);
        break;
    }
    case GROOVE_V_U48: push_byte(b, 19); put_be(b, v->u.u64_, 8); break;
    case GROOVE_V_LARGE: {
        const groove_LargeValueRefHead *l = (const groove_LargeValueRefHead *)v->u.large;
        push_byte(b, 18);
        push_byte(b, l->kind == GROOVE_LVK_BYTES ? 0 : l->kind == GROOVE_LVK_STRING ? 1 : 2);
        push_byte(b, l->format_version);
        put_raw(b, l->logical_hash, 32);
        break;
    }
    }
}

static void put_descriptor_type(jz_bytes *b, groove_RecordDescriptor d) {
    u32 n = 0;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(d, &n);
    push_byte(b, 15);
    put_len(b, n);
    for (u32 i = 0; i < n; i++) {
        const groove_DescriptorField *f = &fields[i];
        if (f->has_name) { push_byte(b, 1); put_str(b, &f->name); }
        else push_byte(b, 0);
        /* Physical slot identity is part of a record literal's type. */
        if (!f->has_identity) push_byte(b, 0);
        else if (f->identity.tag == GROOVE_FID_NAME) { push_byte(b, 1); put_str(b, &f->identity.name); }
        else if (f->identity.tag == GROOVE_FID_SLOT) { push_byte(b, 2); put_be(b, f->identity.slot, 8); }
        else { push_byte(b, 3); put_str(b, &f->identity.name); put_be(b, f->identity.slot, 8); }
        put_column_type(b, &f->value_type);
    }
}

static void put_column_type(jz_bytes *b, const groove_ValueType *t) {
    switch (t->tag) {
    case GROOVE_VT_U8: push_byte(b, 0); break;
    case GROOVE_VT_U16: push_byte(b, 1); break;
    case GROOVE_VT_U32: push_byte(b, 2); break;
    case GROOVE_VT_U64: push_byte(b, 3); break;
    case GROOVE_VT_I32: push_byte(b, 4); break;
    case GROOVE_VT_I64: push_byte(b, 5); break;
    case GROOVE_VT_F64: push_byte(b, 6); break;
    case GROOVE_VT_BOOL: push_byte(b, 7); break;
    case GROOVE_VT_STRING: push_byte(b, 8); break;
    case GROOVE_VT_BYTES: push_byte(b, 9); break;
    case GROOVE_VT_UUID: push_byte(b, 10); break;
    case GROOVE_VT_ENUM_TAG:
        push_byte(b, 11);
        put_str(b, &t->u.enum_tag.name);
        put_len(b, t->u.enum_tag.variants.len);
        JZ_VEC_FOR(const jz_str, v, t->u.enum_tag.variants) put_str(b, v);
        break;
    case GROOVE_VT_TUPLE:
        push_byte(b, 12);
        put_len(b, t->u.tuple.len);
        JZ_VEC_FOR(const groove_ValueType, it, t->u.tuple) put_column_type(b, it);
        break;
    case GROOVE_VT_ARRAY: push_byte(b, 13); put_column_type(b, t->u.inner); break;
    case GROOVE_VT_NULLABLE: push_byte(b, 14); put_column_type(b, t->u.inner); break;
    case GROOVE_VT_RECORD: put_descriptor_type(b, t->u.record); break;
    case GROOVE_VT_ENUM:
        push_byte(b, 16);
        put_be(b, t->u.enum_->registry_id, 8);
        put_str(b, &t->u.enum_->name);
        put_len(b, t->u.enum_->cases.len);
        JZ_VEC_FOR(const groove_EnumCase, c, t->u.enum_->cases) {
            put_str(b, &c->name);
            put_descriptor_type(b, c->payload);
        }
        break;
    /* Internal physical types cannot be supplied through the public query interface. */
    case GROOVE_VT_INTERNAL: push_byte(b, 17); break;
    case GROOVE_VT_U48: push_byte(b, 18); break;
    }
}
