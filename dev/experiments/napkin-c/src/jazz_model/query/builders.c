/* query/builders.rs: Query builder API (the subset the browser build ships). */
#include "jazz_model/jazz_model.h"

#define Q(x) jazz_model__query__##x

/* Construct a query rooted at `table` (TableRef: a table plus an optional flat-join root alias). */
jazz_model_Query Q(Query__from)(jz_str table, jazz_model_OptStr alias) {
    jazz_model_Query q;
    memset(&q, 0, sizeof(q));
    q.table = table;
    if (alias.some) {
        q.flat_join = JZ_NEW(jazz_model_FlatJoin);
        q.flat_join->root_alias = alias;
    }
    return q;
}

/* The row query an aggregate consumes: the same sources and filters, without the aggregate,
 * projection, ordering or pagination. None for a non-aggregate query. */
bool Q(Query__aggregate_input)(const jazz_model_Query *q, jazz_model_Query *out) {
    if (!q->aggregate) return false;
    Q(Query__Clone__clone)(out, q);
    Q(AggregateQuery__drop)(out->aggregate);
    jz_free(out->aggregate);
    out->aggregate = NULL;
    jazz_model__StrVec__drop(&out->select);
    out->has_select = false;
    jz_vec_drop(&out->order_by, sizeof(jazz_model_OrderBy), Q(OrderBy__drop_v));
    out->has_limit = false;
    out->limit = 0;
    out->offset = 0;
    return true;
}

/* Add a filter. */
void Q(Query__filter)(jazz_model_Query *q, jazz_model_Predicate predicate) { JZ_VEC_PUSH(q->filters, predicate); }

void Q(Query__join_via_row_id)(jazz_model_Query *q, jz_str table, jz_str source_column, jazz_model_PredicateVec filters) {
    jazz_model_JoinVia *j = JAZZ_VEC_SLOT(q->joins);
    j->table = table;
    j->on_column = jz_str_from("id");
    j->target = JAZZ_JOIN_TARGET_ROW_ID;
    j->source_column.some = true;
    j->source_column.s = source_column;
    j->filters = filters;
}

jz_err Q(Query__validate)(const jazz_model_Query *q, const jazz_model_JazzSchema *schema, jazz_model_ValidatedQuery *out) {
    return Q(Query__validate_runtime)(q, jazz_model__schema__JazzSchema__runtime(schema), out);
}
jz_err Q(Query__validate_runtime)(const jazz_model_Query *q, const jazz_model_RuntimeSchema *schema, jazz_model_ValidatedQuery *out) {
    return Q(validate_query)(q, schema, out);
}
jz_err Q(Query__validate_with_schema_version)(const jazz_model_Query *q, const jazz_model_RuntimeSchema *schema,
                                              jazz_types_SchemaVersionId version, jazz_model_ValidatedQuery *out) {
    return Q(validate_query_with_schema_version)(q, schema, version, out);
}
