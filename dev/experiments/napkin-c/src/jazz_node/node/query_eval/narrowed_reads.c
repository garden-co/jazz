/* Port of crates/jazz/layers/node/src/node/query_eval/narrowed_reads.rs
 * Narrowed exclusive reads of the sources a query reads beyond its root
 * (garden-co/jazz#3694). */
#include "jazz_node/node/node_qe_b.h"

typedef jazz_model_PredicateVec PredVec;
static void pred_vec_clone(PredVec *dst, const PredVec *src)
{
    memset(dst, 0, sizeof *dst);
    jz_vec_reserve(dst, src->len, sizeof(jazz_model_Predicate));
    for (u32 i = 0; i < src->len; i++) jazz_model_Predicate_clone(&dst->ptr[i], &src->ptr[i]);
    dst->len = src->len;
}
static void pred_vec_drop(PredVec *v) { jz_vec_drop(v, sizeof(jazz_model_Predicate), jazz_model_Predicate_drop); }
static void str_drop(void *s) { jz_str_drop(s); }

void jazz_node_NarrowedSourceRead_drop(void *p)
{
    jazz_node_NarrowedSourceRead *r = p;
    jazz_model_ValidatedQuery_drop(&r->shape);
    jazz_model_Binding_drop(&r->binding);
}
static const jz_btree_desc READS_DESC = { sizeof(jazz_engine_SourceId), sizeof(jazz_node_NarrowedSourceRead), jazz_engine_SourceId_cmp, jazz_engine_SourceId_drop, jazz_node_NarrowedSourceRead_drop };
static const jz_btree_desc UNSUPPORTED_DESC = { sizeof(jazz_engine_SourceId), sizeof(jz_str), jazz_engine_SourceId_cmp, jazz_engine_SourceId_drop, str_drop };
static const jz_btree_desc SOURCE_SET_DESC = { sizeof(jazz_engine_SourceId), 0, jazz_engine_SourceId_cmp, jazz_engine_SourceId_drop, NULL };
static int source_ref_cmp(const void *a, const void *b, void *ctx) { return jazz_engine_SourceId_cmp(*(void *const *)a, *(void *const *)b, ctx); }
static const jz_btree_desc SOURCE_REF_SET_DESC = { sizeof(void *), 0, source_ref_cmp, NULL, NULL };

/* NarrowedSources */
typedef struct NarrowedSources {
    jz_btree reads;        /* SourceId -> NarrowedSourceRead */
    jz_btree unsupported;  /* SourceId -> String */
    bool has_unsupported_query; jz_str unsupported_query;
} NarrowedSources;
static void narrowed_init(NarrowedSources *n)
{
    memset(n, 0, sizeof *n);
    jz_btree_init(&n->reads, &READS_DESC);
    jz_btree_init(&n->unsupported, &UNSUPPORTED_DESC);
}
static void narrowed_drop(NarrowedSources *n)
{
    jz_btree_drop(&n->reads);
    jz_btree_drop(&n->unsupported);
    if (n->has_unsupported_query) jz_str_drop(&n->unsupported_query);
}
static NarrowedSources jazz_node__node__query_eval__narrowed_reads__NarrowedSources__unsupported_query(jz_str pattern)
{
    NarrowedSources n;
    narrowed_init(&n);
    n.has_unsupported_query = true;
    n.unsupported_query = pattern;
    return n;
}

static jz_str jazz_node__node__query_eval__narrowed_reads__describe_source_path(const jazz_engine_SourceId *source)
{
    jz_str out = {0};
    for (u32 i = 0; i < source->path.components.len; i++) {
        const jazz_engine_SourceRole *c = &source->path.components.ptr[i];
        if (i) jz_str_push_char(&out, '/');
        switch (c->tag) {
        case JZ_SR_ROOT: jz_str_push_cstr(&out, "root"); break;
        case JZ_SR_ALIAS:
        case JZ_SR_RECURSIVE_SEED:
        case JZ_SR_RECURSIVE_STEP:
        case JZ_SR_CORRELATED_CHILD:
        case JZ_SR_POLICY: jz_str_push(&out, c->name.ptr, c->name.len); break;
        }
    }
    return out;
}

/* The read pattern of `source`, which has no narrowed read. */
static jz_str jazz_node__node__query_eval__narrowed_reads__NarrowedSources__unsupported_pattern(
    const NarrowedSources *self, const jazz_engine_SourceId *source, const jz_str *root)
{
    const jz_str *p = jz_btree_get(&self->unsupported, source);
    if (p) return jz_str_clone(p);
    if (self->has_unsupported_query) return jz_str_clone(&self->unsupported_query);
    jz_str path = jazz_node__node__query_eval__narrowed_reads__describe_source_path(source);
    jz_str out = jz_format("`%.*s` as source `%.*s` of a query on `%.*s`", (int)source->table.len, source->table.ptr,
                           (int)path.len, path.ptr, (int)root->len, root->ptr);
    jz_str_drop(&path);
    return out;
}

/* Correlation / Link / CorrelationParent */
typedef struct StrPair { jz_str child; jz_str parent; } StrPair;
static void str_pair_drop(void *p) { StrPair *s = p; jz_str_drop(&s->child); jz_str_drop(&s->parent); }
typedef struct Correlation { jz_str child_key; jz_str parent_key; JZ_VEC(StrPair) extra; bool membership; } Correlation;
typedef struct Link { jz_str parent_table; PredVec parent_filters; Correlation correlation; } Link;
typedef JZ_VEC(Link) LinkVec;
typedef struct CorrelationParent { jz_str table; PredVec filters; LinkVec links; } CorrelationParent;

static void correlation_clone(Correlation *d, const Correlation *s)
{
    d->child_key = jz_str_clone(&s->child_key);
    d->parent_key = jz_str_clone(&s->parent_key);
    memset(&d->extra, 0, sizeof d->extra);
    for (u32 i = 0; i < s->extra.len; i++) {
        StrPair p = { jz_str_clone(&s->extra.ptr[i].child), jz_str_clone(&s->extra.ptr[i].parent) };
        jz_vec_push(&d->extra, &p, sizeof p);
    }
    d->membership = s->membership;
}
static void correlation_drop(Correlation *c)
{
    jz_str_drop(&c->child_key);
    jz_str_drop(&c->parent_key);
    jz_vec_drop(&c->extra, sizeof(StrPair), str_pair_drop);
}
static void link_clone(Link *d, const Link *s)
{
    d->parent_table = jz_str_clone(&s->parent_table);
    pred_vec_clone(&d->parent_filters, &s->parent_filters);
    correlation_clone(&d->correlation, &s->correlation);
}
static void link_drop(void *p)
{
    Link *l = p;
    jz_str_drop(&l->parent_table);
    pred_vec_drop(&l->parent_filters);
    correlation_drop(&l->correlation);
}
static void parent_clone(CorrelationParent *d, const CorrelationParent *s)
{
    d->table = jz_str_clone(&s->table);
    pred_vec_clone(&d->filters, &s->filters);
    memset(&d->links, 0, sizeof d->links);
    for (u32 i = 0; i < s->links.len; i++) { Link l; link_clone(&l, &s->links.ptr[i]); jz_vec_push(&d->links, &l, sizeof l); }
}
static void parent_drop(CorrelationParent *p)
{
    jz_str_drop(&p->table);
    pred_vec_drop(&p->filters);
    jz_vec_drop(&p->links, sizeof(Link), link_drop);
}

/* This source under its own filters, restricted to rows that correlate with a
 * parent row passing the parent's own filters, up to the root. */
static bool jazz_node__node__query_eval__narrowed_reads__CorrelationParent__narrowed_read(const CorrelationParent *self, jazz_model_Query *out)
{
    jazz_model_Query query = jazz_model__query__builders__Query__from(self->table.ptr, self->table.len);
    pred_vec_drop(&query.filters);
    pred_vec_clone(&query.filters, &self->filters);
    bool any_membership = false;
    JZ_VEC_FOR(Link, l, self->links) if (l->correlation.membership) { any_membership = true; break; }
    if (any_membership) {
        bool has_up = false;
        jazz_model_ArraySubquery up;
        for (u32 i = self->links.len; i-- > 0;) {
            const Link *link = &self->links.ptr[i];
            const Correlation *c = &link->correlation;
            if (c->extra.len != 0) {
                if (has_up) jazz_model_ArraySubquery_drop(&up);
                jazz_model_Query_drop(&query);
                return false;
            }
            jazz_model_ArraySubquery hop = jazz_model__query__policy_and_request_contracts__ArraySubquery__new(
                "narrowed_parent", &link->parent_table, &c->parent_key, &c->child_key);
            pred_vec_drop(&hop.filters);
            pred_vec_clone(&hop.filters, &link->parent_filters);
            hop.requirement = JZ_ASR_AT_LEAST_ONE;
            if (has_up) jz_vec_push(&hop.nested_arrays, &up, sizeof up);
            up = hop;
            has_up = true;
        }
        if (has_up) jz_vec_push(&query.array_subqueries, &up, sizeof up);
    } else {
        bool has_up = false;
        jazz_model_JoinVia up;
        for (u32 i = self->links.len; i-- > 0;) {
            const Link *link = &self->links.ptr[i];
            const Correlation *c = &link->correlation;
            jazz_model_JoinVia j;
            memset(&j, 0, sizeof j);
            j.table = jz_str_clone(&link->parent_table);
            j.target = jz_str_eq_cstr(&c->parent_key, "id") ? JZ_JT_ROW_ID : JZ_JT_COLUMN;
            j.on_column = jz_str_clone(&c->parent_key);
            j.has_source_column = true;
            j.source_column = jz_str_clone(&c->child_key);
            j.source_lookup = NULL;
            for (u32 e = 0; e < c->extra.len; e++) {
                jazz_model_JoinCorrelation jc = { jz_str_clone(&c->extra.ptr[e].parent), jz_str_clone(&c->extra.ptr[e].child) };
                jz_vec_push(&j.correlated_filters, &jc, sizeof jc);
            }
            pred_vec_clone(&j.filters, &link->parent_filters);
            if (has_up) jz_vec_push(&j.nested_joins, &up, sizeof up);
            up = j;
            has_up = true;
        }
        if (has_up) jz_vec_push(&query.joins, &up, sizeof up);
    }
    *out = query;
    return true;
}

/* The narrowed read of a child source of this parent, and the parent the
 * child's own children correlate with. */
static bool jazz_node__node__query_eval__narrowed_reads__CorrelationParent__child(
    const CorrelationParent *self, const jz_str *table, PredVec filters, Correlation *correlation,
    jazz_model_Query *read, CorrelationParent *parent_out)
{
    LinkVec links = {0};
    if (correlation) {
        Link first;
        first.parent_table = jz_str_clone(&self->table);
        pred_vec_clone(&first.parent_filters, &self->filters);
        first.correlation = *correlation;
        jz_vec_push(&links, &first, sizeof first);
        for (u32 i = 0; i < self->links.len; i++) { Link l; link_clone(&l, &self->links.ptr[i]); jz_vec_push(&links, &l, sizeof l); }
    }
    parent_out->table = jz_str_clone(table);
    parent_out->filters = filters;
    parent_out->links = links;
    return jazz_node__node__query_eval__narrowed_reads__CorrelationParent__narrowed_read(parent_out, read);
}

/* `predicate` with every binding parameter replaced by its bound value. */
typedef struct BindCtx { const jz_btree *values; bool complete; } BindCtx;
static void bind_operand(jazz_model_Operand *operand, void *p)
{
    BindCtx *c = p;
    if (operand->tag != JZ_OPERAND_PARAM) return;
    const groove_Value *v = jz_btree_get(c->values, &operand->name);
    if (!v) { c->complete = false; return; }
    jazz_model_Operand_drop(operand);
    memset(operand, 0, sizeof *operand);
    operand->tag = JZ_OPERAND_LITERAL;
    groove_Value_clone(&operand->literal, v);
}
static bool jazz_node__node__query_eval__narrowed_reads__bind_predicate(const jazz_model_Predicate *predicate, const jz_btree *values, jazz_model_Predicate *out)
{
    jazz_model_Predicate bound;
    jazz_model_Predicate_clone(&bound, predicate);
    BindCtx c = { values, true };
    jazz_model__query__policy_and_request_contracts__Predicate__for_each_operand_mut(&bound, bind_operand, &c);
    if (!c.complete) { jazz_model_Predicate_drop(&bound); return false; }
    *out = bound;
    return true;
}
static bool jazz_node__node__query_eval__narrowed_reads__bind_predicates(const jazz_model_Predicate *preds, u32 n, const jz_btree *values, PredVec *out)
{
    PredVec v = {0};
    jz_vec_reserve(&v, n, sizeof(jazz_model_Predicate));
    for (u32 i = 0; i < n; i++) {
        if (!jazz_node__node__query_eval__narrowed_reads__bind_predicate(&preds[i], values, &v.ptr[i])) { pred_vec_drop(&v); return false; }
        v.len++;
    }
    *out = v;
    return true;
}

/* Whether `column_type` holds an array, whose correlation matches by membership. */
static bool jazz_node__node__query_eval__narrowed_reads__is_array(const groove_ColumnType *t)
{
    switch (t->tag) {
    case GCT_ARRAY: return true;
    case GCT_NULLABLE: return jazz_node__node__query_eval__narrowed_reads__is_array(t->inner);
    default: return false;
    }
}

typedef struct Candidate { jazz_engine_SourceId source; jazz_model_Query read; } Candidate;
typedef JZ_VEC(Candidate) CandidateVec;
static void candidate_drop(void *p) { Candidate *c = p; jazz_engine_SourceId_drop(&c->source); jazz_model_Query_drop(&c->read); }

/* Add the narrowed read of `source`, or note why it has none. */
static void jazz_node__node__query_eval__narrowed_reads__push_candidate(
    jazz_engine_SourceId source, bool has_read, jazz_model_Query *read, CandidateVec *candidates, jz_btree *unsupported)
{
    if (has_read) {
        Candidate c = { source, *read };
        jz_vec_push(candidates, &c, sizeof c);
        return;
    }
    jz_str pattern = jz_format("`%.*s` through a reference array together with additional join keys", (int)source.table.len, source.table.ptr);
    jz_str old;
    if (jz_btree_insert(unsupported, &source, &pattern, &old)) { jazz_engine_SourceId_drop(&source); jz_str_drop(&old); }
}

static bool jazz_node__node__query_eval__narrowed_reads__NodeState__column_is_array(
    const jazz_node_NodeState *self, const jz_str *table, const jz_str *column, jazz_types_SchemaVersionId schema_version)
{
    const jazz_model_TableSchema *t;
    jz_err e = jazz_node__node__NodeState__table_in_schema_ref(self, table, schema_version, &t);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    for (u32 i = 0; i < t->columns.len; i++)
        if (jz_str_eq(&t->columns.ptr[i].name, column))
            return jazz_node__node__query_eval__narrowed_reads__is_array(&t->columns.ptr[i].column_type);
    return false;
}

static Correlation jazz_node__node__query_eval__narrowed_reads__NodeState__correlation(
    const jazz_node_NodeState *self, const jz_str *child_table, const jz_str *child_key, const jz_str *parent_table,
    const jz_str *parent_key, jazz_types_SchemaVersionId schema_version)
{
    Correlation c;
    c.child_key = jz_str_clone(child_key);
    c.parent_key = jz_str_clone(parent_key);
    memset(&c.extra, 0, sizeof c.extra);
    c.membership = jazz_node__node__query_eval__narrowed_reads__NodeState__column_is_array(self, child_table, child_key, schema_version) ||
                   jazz_node__node__query_eval__narrowed_reads__NodeState__column_is_array(self, parent_table, parent_key, schema_version);
    return c;
}

static void insert_unsupported(jz_btree *unsupported, jazz_engine_SourceId source, jz_str pattern)
{
    jz_str old;
    if (jz_btree_insert(unsupported, &source, &pattern, &old)) { jazz_engine_SourceId_drop(&source); jz_str_drop(&old); }
}

/* Note `pattern` as the read of the join source at `path` and every source nested below it. */
static void jazz_node__node__query_eval__narrowed_reads__NodeState__decline_join(
    const jazz_model_JoinVia *join, const jz_str *path, const jz_str *pattern, jz_btree *unsupported)
{
    insert_unsupported(unsupported, jazz_node__node__query_eval__normalization__nested_join_source_id(join, path), jz_str_clone(pattern));
    for (u32 i = 0; i < join->nested_joins.len; i++) {
        jz_str nested = jz_format("%.*s:nested:%u", (int)path->len, path->ptr, i);
        jazz_node__node__query_eval__narrowed_reads__NodeState__decline_join(&join->nested_joins.ptr[i], &nested, pattern, unsupported);
        jz_str_drop(&nested);
    }
}

/* Narrow a JoinVia source at `path` and its nested joins. */
static void jazz_node__node__query_eval__narrowed_reads__NodeState__narrow_join(
    const jazz_node_NodeState *self, const CorrelationParent *parent, const jazz_model_JoinVia *join, const jz_str *path,
    jazz_types_SchemaVersionId schema_version, const jz_btree *values, CandidateVec *candidates, jz_btree *unsupported)
{
    /* A lookup join correlates through a third table, which a reverse join cannot express. */
    if (join->source_lookup) {
        jz_str pattern = jz_format("`%.*s` through a lookup join from `%.*s`", (int)join->table.len, join->table.ptr,
                                   (int)parent->table.len, parent->table.ptr);
        jazz_node__node__query_eval__narrowed_reads__NodeState__decline_join(join, path, &pattern, unsupported);
        jz_str_drop(&pattern);
        return;
    }
    PredVec filters;
    if (!jazz_node__node__query_eval__narrowed_reads__bind_predicates(join->filters.ptr, join->filters.len, values, &filters)) {
        jz_str pattern = jz_format("`%.*s` joined to `%.*s` with a filter whose parameter is not bound", (int)join->table.len,
                                   join->table.ptr, (int)parent->table.len, parent->table.ptr);
        jazz_node__node__query_eval__narrowed_reads__NodeState__decline_join(join, path, &pattern, unsupported);
        jz_str_drop(&pattern);
        return;
    }
    Correlation correlation;
    bool has_correlation = join->target != JZ_JT_UNCORRELATED;
    if (has_correlation) {
        jz_str id = jz_str_from("id");
        const jz_str *child_key = join->target == JZ_JT_ROW_ID ? &id : &join->on_column;
        const jz_str *parent_key = join->has_source_column ? &join->source_column : &id;
        correlation = jazz_node__node__query_eval__narrowed_reads__NodeState__correlation(self, &join->table, child_key, &parent->table, parent_key, schema_version);
        jz_str_drop(&id);
        for (u32 i = 0; i < join->correlated_filters.len; i++) {
            StrPair p = { jz_str_clone(&join->correlated_filters.ptr[i].join_column), jz_str_clone(&join->correlated_filters.ptr[i].source_column) };
            jz_vec_push(&correlation.extra, &p, sizeof p);
        }
    }
    jazz_model_Query read;
    CorrelationParent child;
    bool has_read = jazz_node__node__query_eval__narrowed_reads__CorrelationParent__child(parent, &join->table, filters, has_correlation ? &correlation : NULL, &read, &child);
    jazz_node__node__query_eval__narrowed_reads__push_candidate(jazz_node__node__query_eval__normalization__nested_join_source_id(join, path), has_read, &read, candidates, unsupported);
    for (u32 i = 0; i < join->nested_joins.len; i++) {
        jz_str nested = jz_format("%.*s:nested:%u", (int)path->len, path->ptr, i);
        jazz_node__node__query_eval__narrowed_reads__NodeState__narrow_join(self, &child, &join->nested_joins.ptr[i], &nested, schema_version, values, candidates, unsupported);
        jz_str_drop(&nested);
    }
    parent_drop(&child);
}

/* Narrow a correlated array source and its nested arrays. */
static void jazz_node__node__query_eval__narrowed_reads__NodeState__narrow_array_subquery(
    const jazz_node_NodeState *self, const CorrelationParent *parent, const jazz_engine_SourceId *owner,
    const jazz_model_ArraySubquery *subquery, const u32 *path, u32 npath, jazz_types_SchemaVersionId schema_version,
    const jz_btree *values, CandidateVec *candidates, jz_btree *unsupported)
{
    jazz_engine_SourceId source = jazz_node__node__query_eval__normalization__correlated_child_source_id(owner, subquery, path, npath);
    PredVec filters;
    if (!jazz_node__node__query_eval__narrowed_reads__bind_predicates(subquery->filters.ptr, subquery->filters.len, values, &filters)) {
        insert_unsupported(unsupported, source,
                           jz_format("`%.*s` related to `%.*s` with a filter whose parameter is not bound", (int)subquery->table.len,
                                     subquery->table.ptr, (int)parent->table.len, parent->table.ptr));
        return;
    }
    Correlation correlation = jazz_node__node__query_eval__narrowed_reads__NodeState__correlation(
        self, &subquery->table, &subquery->inner_column, &parent->table, &subquery->outer_column, schema_version);
    jazz_model_Query read;
    CorrelationParent child;
    bool has_read = jazz_node__node__query_eval__narrowed_reads__CorrelationParent__child(parent, &subquery->table, filters, &correlation, &read, &child);
    jazz_engine_SourceId copy;
    jazz_engine_SourceId_clone(&copy, &source);
    jazz_node__node__query_eval__narrowed_reads__push_candidate(copy, has_read, &read, candidates, unsupported);
    u32 *nested_path = jz_alloc(sizeof(u32) * (npath + 1));
    memcpy(nested_path, path, sizeof(u32) * npath);
    for (u32 i = 0; i < subquery->nested_arrays.len; i++) {
        nested_path[npath] = i;
        jazz_node__node__query_eval__narrowed_reads__NodeState__narrow_array_subquery(
            self, &child, &source, &subquery->nested_arrays.ptr[i], nested_path, npath + 1, schema_version, values, candidates, unsupported);
    }
    jz_free(nested_path);
    jazz_engine_SourceId_drop(&source);
    parent_drop(&child);
}

/* The narrowed read for each non-root source of `shape` that correlates back to its root. */
static jz_err jazz_node__node__query_eval__narrowed_reads__NodeState__narrowed_source_reads(
    const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, NarrowedSources *out)
{
    const jazz_model_Query *query = &shape->query;
    const jz_str *root_table = &query->table;
    int rl = (int)root_table->len;
    const char *rp = root_table->ptr;
    if (query->relation) {
        *out = jazz_node__node__query_eval__narrowed_reads__NarrowedSources__unsupported_query(
            jazz_model__query__relation_resolution__relation_union_parts(query->relation->rel)
                ? jz_format("a union of relations over `%.*s`", rl, rp)
                : jz_format("a relation over `%.*s` that projects selected columns", rl, rp));
        return JZ_OK;
    }
    if (query->flat_join) {
        jz_str tables = {0};
        for (u32 i = 0; i < query->flat_join->sources.len; i++) {
            if (i) jz_str_push_cstr(&tables, ", ");
            const jz_str *t = &query->flat_join->sources.ptr[i].table;
            jz_str_appendf(&tables, "`%.*s`", (int)t->len, t->ptr);
        }
        *out = jazz_node__node__query_eval__narrowed_reads__NarrowedSources__unsupported_query(
            jz_format("a flat join of `%.*s` with %.*s", rl, rp, (int)tables.len, tables.ptr));
        jz_str_drop(&tables);
        return JZ_OK;
    }
    if (query->policy_branches.len) {
        *out = jazz_node__node__query_eval__narrowed_reads__NarrowedSources__unsupported_query(jz_format("`%.*s` through policy branches", rl, rp));
        return JZ_OK;
    }
    if (query->reachable.len) {
        const jz_str *edge = &query->reachable.ptr[0].edge_table;
        *out = jazz_node__node__query_eval__narrowed_reads__NarrowedSources__unsupported_query(
            jz_format("`%.*s` through a recursive traversal of `%.*s`", rl, rp, (int)edge->len, edge->ptr));
        return JZ_OK;
    }
    if (query->inherits.len) {
        const jazz_model_InheritsVia *inherits = &query->inherits.ptr[0];
        const jazz_model_TableSchema *t;
        JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, root_table, shape->schema_version, &t));
        const jz_str *parent = jz_btree_get(&t->references, &inherits->parent_column);
        if (!parent) parent = &inherits->parent_column;
        const char *access = "read";
        switch (inherits->operation) {
        case JZ_INH_SELECT: access = "read"; break;
        case JZ_INH_INSERT: access = "insert"; break;
        case JZ_INH_UPDATE: access = "update"; break;
        case JZ_INH_DELETE: access = "delete"; break;
        }
        *out = jazz_node__node__query_eval__narrowed_reads__NarrowedSources__unsupported_query(
            jz_format("`%.*s` inheriting %s access from `%.*s`", rl, rp, access, (int)parent->len, parent->ptr));
        return JZ_OK;
    }
    const jz_btree *values = &binding->values;
    PredVec root_filters;
    if (!jazz_node__node__query_eval__narrowed_reads__bind_predicates(query->filters.ptr, query->filters.len, values, &root_filters)) {
        *out = jazz_node__node__query_eval__narrowed_reads__NarrowedSources__unsupported_query(
            jz_format("`%.*s` with a filter whose parameter is not bound", rl, rp));
        return JZ_OK;
    }
    CorrelationParent root = { jz_str_clone(&query->table), root_filters };
    memset(&root.links, 0, sizeof root.links);
    CandidateVec candidates = {0};
    NarrowedSources narrowed;
    narrowed_init(&narrowed);
    jz_err err = JZ_OK;

    jazz_types_SchemaVersionId schema_version = shape->schema_version;
    for (u32 i = 0; i < query->joins.len; i++) {
        jz_str path = jz_format("join_via:%u", i);
        jazz_node__node__query_eval__narrowed_reads__NodeState__narrow_join(self, &root, &query->joins.ptr[i], &path, schema_version, values, &candidates, &narrowed.unsupported);
        jz_str_drop(&path);
    }

    /* Implicit root references are sync payload: they need no read. */
    for (u32 include_index = 0; include_index < query->includes.len; include_index++) {
        const jz_str *ipath = &query->includes.ptr[include_index].path;
        CorrelationParent parent;
        parent_clone(&parent, &root);
        u32 start = 0, segment_index = 0;
        for (u32 pos = 0; pos <= ipath->len; pos++) {
            if (pos < ipath->len && ipath->ptr[pos] != '.') continue;
            jz_str segment = jz_str_from_slice(jz_slice_of(ipath->ptr + start, pos - start));
            start = pos + 1;
            const jazz_model_TableSchema *t;
            err = jazz_node__node__NodeState__table_in_schema_ref(self, &parent.table, schema_version, &t);
            if (JZ_IS_ERR(err)) { jz_str_drop(&segment); parent_drop(&parent); goto fail; }
            const jz_str *target_ref = jz_btree_get(&t->references, &segment);
            if (!target_ref) { jz_str_drop(&segment); break; }
            jz_str target = jz_str_clone(target_ref);
            jz_str id = jz_str_from("id");
            Correlation correlation = jazz_node__node__query_eval__narrowed_reads__NodeState__correlation(self, &target, &id, &parent.table, &segment, schema_version);
            jz_str_drop(&id);
            jz_str_drop(&segment);
            jazz_model_Query read;
            CorrelationParent child;
            PredVec no_filters = {0};
            bool has_read = jazz_node__node__query_eval__narrowed_reads__CorrelationParent__child(&parent, &target, no_filters, &correlation, &read, &child);
            jazz_node__node__query_eval__narrowed_reads__push_candidate(
                jazz_node__node__query_eval__normalization__include_auxiliary_source_id(target, include_index, segment_index),
                has_read, &read, &candidates, &narrowed.unsupported);
            parent_drop(&parent);
            parent = child;
            segment_index++;
        }
        parent_drop(&parent);
    }

    jazz_engine_SourceId root_source = jazz_node__node__query_eval__normalization__root_source_id(query->table.ptr, query->table.len);
    for (u32 i = 0; i < query->array_subqueries.len; i++) {
        jazz_node__node__query_eval__narrowed_reads__NodeState__narrow_array_subquery(
            self, &root, &root_source, &query->array_subqueries.ptr[i], &i, 1, schema_version, values, &candidates, &narrowed.unsupported);
    }
    jazz_engine_SourceId_drop(&root_source);

    const jazz_protocol_SchemaVersion *sv = jz_btree_get(&self->catalogue.catalogue_schemas, &schema_version);
    if (!sv) { err = JZN_INVALID_STORED("query schema version is unknown"); goto fail; }
    const jazz_model_JazzSchema *schema = &sv->schema;
    for (u32 i = 0; i < candidates.len; i++) {
        Candidate *c = &candidates.ptr[i];
        jazz_model_ValidatedQuery vq;
        jazz_model_Binding vb;
        jz_err e = jazz_model__query__builders__Query__validate(&c->read, schema, &vq);
        if (!JZ_IS_ERR(e)) {
            jz_btree empty;
            jz_btree_init(&empty, &jazz_node_qeb_STR_VALUE_DESC);
            e = jazz_model__query__validation__ValidatedQuery__bind(&vq, empty, &vb);
            if (JZ_IS_ERR(e)) jazz_model_ValidatedQuery_drop(&vq);
        }
        jazz_engine_SourceId source = c->source;
        jazz_model_Query_drop(&c->read);
        if (!JZ_IS_ERR(e)) {
            jazz_node_NarrowedSourceRead r = { vq, vb };
            jazz_node_NarrowedSourceRead old;
            if (jz_btree_insert(&narrowed.reads, &source, &r, &old)) { jazz_engine_SourceId_drop(&source); jazz_node_NarrowedSourceRead_drop(&old); }
        } else {
            jz_str msg = {0};
            jazz_model_QueryError_display(&msg, &e);
            jz_err_drop(&e);
            jz_str pattern = jz_format("`%.*s` correlated with `%.*s` (%.*s)", (int)source.table.len, source.table.ptr, rl, rp, (int)msg.len, msg.ptr);
            jz_str_drop(&msg);
            insert_unsupported(&narrowed.unsupported, source, pattern);
        }
    }
    jz_free(candidates.ptr);
    parent_drop(&root);
    *out = narrowed;
    return JZ_OK;

fail:
    jz_vec_drop(&candidates, sizeof(Candidate), candidate_drop);
    narrowed_drop(&narrowed);
    parent_drop(&root);
    return err;
}

/* The narrowed read of every source of `shape` beyond its root that decides
 * its result, for an exclusive transaction query. */
jz_err jazz_node__node__query_eval__narrowed_reads__NodeState__exclusive_source_reads(
    const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    bool include_deleted, jazz_node_ExclusiveSourceReads *out)
{
    jazz_engine_NormalizedRowSetShape normalized;
    JZ_TRY(jazz_node__node__query_eval__normalization__NodeState__normalized_row_set_shape(self, shape, binding, &normalized));
    jz_btree payload;
    jz_btree_init(&payload, &SOURCE_SET_DESC);
    for (u32 i = 0; i < normalized.closure_paths.len; i++) {
        const jazz_engine_ClosurePath *p = &normalized.closure_paths.ptr[i];
        switch (p->tag) {
        case JZ_CP_IMPLICIT_ROOT_REFERENCE: {
            jazz_engine_SourceId t;
            jazz_engine_SourceId_clone(&t, &p->segment.target);
            if (jz_btree_contains(&payload, &t)) jazz_engine_SourceId_drop(&t);
            else jz_btree_insert(&payload, &t, NULL, NULL);
            break;
        }
        case JZ_CP_EXPLICIT_INCLUDE: break;
        }
    }
    jz_btree sources;
    jz_btree_init(&sources, &SOURCE_REF_SET_DESC);
    {
        void *k; jazz_engine_RowSetExpr *node;
        JZ_BTREE_FOR(&normalized.nodes, k, node) {
            if (node->tag != JZ_RSE_SOURCE) continue;
            const jazz_engine_SourceId *s = &node->source;
            bool is_root = s->path.components.len == 1 && s->path.components.ptr[0].tag == JZ_SR_ROOT;
            if (!is_root && !jz_btree_contains(&payload, s)) jz_btree_insert(&sources, &s, NULL, NULL);
        }
        jazz_engine_SourceId *aux; void *unused;
        JZ_BTREE_FOR(&normalized.auxiliary_sources, aux, unused) {
            const jazz_engine_SourceId *s = aux;
            bool is_root = s->path.components.len == 1 && s->path.components.ptr[0].tag == JZ_SR_ROOT;
            if (!is_root && !jz_btree_contains(&payload, s)) jz_btree_insert(&sources, &s, NULL, NULL);
        }
    }
    jz_err err = JZ_OK;
    const jazz_engine_SourceId **first;
    void *unused;
    if (!jz_btree_first(&sources, (void **)&first, &unused)) {
        jz_btree_init(&out->reads, &READS_DESC);
        out->payload = payload;
        goto done;
    }
    const jz_str *root = &shape->query.table;
    NarrowedSources narrowed;
    if (include_deleted) {
        narrowed = jazz_node__node__query_eval__narrowed_reads__NarrowedSources__unsupported_query(
            jz_format("deleted `%.*s` rows together with `%.*s`", (int)root->len, root->ptr, (int)(*first)->table.len, (*first)->table.ptr));
    } else {
        err = jazz_node__node__query_eval__narrowed_reads__NodeState__narrowed_source_reads(self, shape, binding, &narrowed);
        if (JZ_IS_ERR(err)) { jz_btree_drop(&payload); goto done; }
    }
    const jazz_engine_SourceId **sp;
    JZ_BTREE_FOR(&sources, sp, unused) {
        if (!jz_btree_contains(&narrowed.reads, *sp)) {
            jz_str pattern = jazz_node__node__query_eval__narrowed_reads__NarrowedSources__unsupported_pattern(&narrowed, *sp, root);
            err = jz_err_fmt(JZN_E_UnsupportedExclusiveRead, "Reading %.*s is not supported in exclusive transactions yet", (int)pattern.len, pattern.ptr);
            jz_str_drop(&pattern);
            narrowed_drop(&narrowed);
            jz_btree_drop(&payload);
            goto done;
        }
    }
    out->reads = narrowed.reads;
    jz_btree_init(&narrowed.reads, &READS_DESC);
    narrowed_drop(&narrowed);
    out->payload = payload;
done:
    jz_btree_drop(&sources);
    jazz_engine_NormalizedRowSetShape_drop(&normalized);
    return err;
}
