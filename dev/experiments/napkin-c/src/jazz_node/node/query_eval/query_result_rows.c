/* Port of crates/jazz/layers/node/src/node/query_eval/query_result_rows.rs
 * Aggregate result shaping and ordering for query evaluation. */
#include "jazz_node/node/node_qe_b.h"

static int jazz_node__node__query_eval__query_result_rows__compare_order_values(const groove_Value *left, const groove_Value *right);

int jazz_node__node__query_eval__query_result_rows__compare_optional_values(const groove_Value *left, const groove_Value *right)
{
    if (left && right) return jazz_node__node__query_eval__query_result_rows__compare_order_values(left, right);
    if (!left && right) return -1;
    if (left && !right) return 1;
    return 0;
}

/* Option<Value> as a (has, value) pair */
typedef struct OptValue { bool has; groove_Value v; } OptValue;
static void opt_value_drop(void *p) { OptValue *o = p; if (o->has) groove_Value_drop(&o->v); }

static jz_err jazz_node__node__query_eval__query_result_rows__aggregate_row_cell(const jazz_node_CurrentRow *row, const jz_str *column, OptValue *out)
{
    u32 idx;
    if (!jazz_node__node__CurrentRow__application_column_index_by_name(row, column->ptr, column->len, &idx))
        return JZN_INVALID_STORED("aggregate ordering field has no publication binding");
    groove_BorrowedRecord br = groove__records__record__OwnedRecord__borrowed(row->record);
    groove_Value v;
    JZ_TRY(groove__records__record__BorrowedRecord__get_idx(&br, idx, &v));
    if (v.tag == GV_NULLABLE) {
        if (v.v.nullable) {
            out->has = true;
            out->v = *v.v.nullable;
            jz_free(v.v.nullable);
        } else {
            out->has = false;
        }
    } else {
        out->has = true;
        out->v = v;
    }
    return JZ_OK;
}

typedef struct SortKey { u32 index; JZ_VEC(OptValue) values; } SortKey;
typedef struct SortCtx { const jazz_model_Query *query; const u8 *rows; usize elem; const jazz_node_AggregateSortVt *vt; } SortCtx;

static int sort_key_cmp(const void *a, const void *b, void *p)
{
    const SortKey *l = a, *r = b;
    const SortCtx *c = p;
    u32 n = l->values.len;
    if (r->values.len < n) n = r->values.len;
    if (c->query->order_by.len < n) n = c->query->order_by.len;
    for (u32 i = 0; i < n; i++) {
        const OptValue *lv = &l->values.ptr[i], *rv = &r->values.ptr[i];
        int ord = jazz_node__node__query_eval__query_result_rows__compare_optional_values(lv->has ? &lv->v : NULL, rv->has ? &rv->v : NULL);
        switch (c->query->order_by.ptr[i].direction) {
        case JZ_ORDER_ASC: break;
        case JZ_ORDER_DESC: ord = -ord; break;
        }
        if (ord != 0) return ord;
    }
    return c->vt->tie_break(c->rows + (usize)l->index * c->elem, c->rows + (usize)r->index * c->elem, c->vt->ctx);
}
static void sort_key_drop(void *p) { SortKey *k = p; jz_vec_drop(&k->values, sizeof(OptValue), opt_value_drop); }

/* Decode ordering keys before sorting so malformed rows fail instead of
 * becoming null keys. */
jz_err jazz_node__node__query_eval__query_result_rows__sort_aggregate_rows(
    const jazz_model_Query *query, void *rows, u32 n, usize elem, const jazz_node_AggregateSortVt *vt)
{
    u8 *base = rows;
    JZ_VEC(SortKey) keys = {0};
    jz_vec_reserve(&keys, n, sizeof(SortKey));
    for (u32 i = 0; i < n; i++) {
        SortKey key = { i };
        memset(&key.values, 0, sizeof key.values);
        const jazz_node_CurrentRow *row = vt->row(base + (usize)i * elem, vt->ctx);
        for (u32 o = 0; o < query->order_by.len; o++) {
            OptValue v;
            jz_err e = jazz_node__node__query_eval__query_result_rows__aggregate_row_cell(row, &query->order_by.ptr[o].column, &v);
            if (JZ_IS_ERR(e)) {
                sort_key_drop(&key);
                jz_vec_drop(&keys, sizeof(SortKey), sort_key_drop);
                return e;
            }
            jz_vec_push(&key.values, &v, sizeof v);
        }
        jz_vec_push(&keys, &key, sizeof key);
    }
    SortCtx ctx = { query, base, elem, vt };
    jz_sort(keys.ptr, keys.len, sizeof(SortKey), sort_key_cmp, &ctx);
    /* Apply the sorted permutation without cloning rows. */
    u32 *destinations = jz_zalloc(sizeof(u32) * (n ? n : 1));
    for (u32 d = 0; d < keys.len; d++) destinations[keys.ptr[d].index] = d;
    jz_vec_drop(&keys, sizeof(SortKey), sort_key_drop);
    u8 *tmp = jz_alloc(elem ? elem : 1);
    for (u32 source = 0; source < n; source++) {
        while (destinations[source] != source) {
            u32 destination = destinations[source];
            memcpy(tmp, base + (usize)source * elem, elem);
            memcpy(base + (usize)source * elem, base + (usize)destination * elem, elem);
            memcpy(base + (usize)destination * elem, tmp, elem);
            u32 t = destinations[source];
            destinations[source] = destinations[destination];
            destinations[destination] = t;
        }
    }
    jz_free(tmp);
    jz_free(destinations);
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__query_result_rows__aggregate_result_column_type(
    const jazz_model_Aggregate *aggregate, const jazz_model_TableSchema *source_table, groove_ColumnType *out)
{
    switch (aggregate->function) {
    case JZ_AGG_COUNT:
        memset(out, 0, sizeof *out);
        out->tag = GCT_U64;
        return JZ_OK;
    case JZ_AGG_SUM:
    case JZ_AGG_MIN:
    case JZ_AGG_MAX: {
        if (!aggregate->has_column) return JZN_INVALID_STORED("aggregate input column missing");
        const jazz_model_ColumnSchema *col = NULL;
        for (u32 i = 0; i < source_table->columns.len; i++)
            if (jz_str_eq(&source_table->columns.ptr[i].name, &aggregate->column)) { col = &source_table->columns.ptr[i]; break; }
        if (!col) return JZN_INVALID_STORED("aggregate input column missing");
        /* CurrentRow supplies the public nullable envelope; flatten one layer. */
        if (col->column_type.tag == GCT_NULLABLE) groove_ColumnType_clone(out, col->column_type.inner);
        else groove_ColumnType_clone(out, &col->column_type);
        return JZ_OK;
    }
    case JZ_AGG_AVG:
        memset(out, 0, sizeof *out);
        out->tag = GCT_F64;
        return JZ_OK;
    }
    JZ_UNREACHABLE();
}

jz_err jazz_node__node__query_eval__query_result_rows__aggregate_result_table(
    const jazz_model_Query *query, const jazz_model_TableSchema *source_table, jazz_model_TableSchema *out)
{
    const jazz_model_AggregateQuery *aggregate = query->aggregate;
    if (!aggregate) return JZN_INVALID_STORED("aggregate query missing aggregate");
    JZ_VEC(jazz_model_ColumnSchema) columns = {0};
    if (aggregate->has_group_by) {
        const jazz_model_ColumnSchema *col = NULL;
        for (u32 i = 0; i < source_table->columns.len; i++)
            if (jz_str_eq(&source_table->columns.ptr[i].name, &aggregate->group_by)) { col = &source_table->columns.ptr[i]; break; }
        if (!col) return JZN_INVALID_STORED("aggregate group column missing");
        groove_ColumnType ty;
        groove_ColumnType_clone(&ty, &col->column_type);
        JZ_VEC_PUSH(columns, jazz_model__schema__ColumnSchema__new(col->name.ptr, col->name.len, ty));
    }
    for (u32 i = 0; i < aggregate->aggregates.len; i++) {
        const jazz_model_Aggregate *a = &aggregate->aggregates.ptr[i];
        groove_ColumnType ty;
        jz_err e = jazz_node__node__query_eval__query_result_rows__aggregate_result_column_type(a, source_table, &ty);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&columns, sizeof(jazz_model_ColumnSchema), jazz_model_ColumnSchema_drop); return e; }
        jz_str name = jazz_node__node__query_eval__aggregate_output_column(&a->alias);
        JZ_VEC_PUSH(columns, jazz_model__schema__ColumnSchema__new(name.ptr, name.len, ty));
        jz_str_drop(&name);
    }
    *out = jazz_model__schema__TableSchema__new(&query->table, columns.ptr, columns.len);
    jz_free(columns.ptr);
    return JZ_OK;
}

/* Use the same stable identity for direct aggregate reads and maintained
 * aggregate delivery. */
jz_err jazz_node__node__query_eval__query_result_rows__aggregate_output_schema(
    const jazz_engine_AppRowSchema *output, const jazz_engine_AggregateResultSchema **out)
{
    if (output->terminal_tag == JZ_ART_AGGREGATE) { *out = &output->aggregate; return JZ_OK; }
    return JZN_INVALID_STORED("aggregate materialization has no lowered aggregate schema");
}

jz_err jazz_node__node__query_eval__query_result_rows__aggregate_record_field_index(
    const groove_BorrowedRecord *record, const groove_DescriptorField *field, u32 *out)
{
    if (!field->has_identity) return JZN_INVALID_STORED("lowered aggregate field has no identity");
    if (!groove__records__descriptor__RecordDescriptor__field_index_by_identity(record->descriptor, &field->identity, out))
        return JZN_INVALID_STORED("aggregate output is missing its lowered field identity");
    return JZ_OK;
}

jz_err jazz_node__node__query_eval__query_result_rows__aggregate_query_row_uuid(
    const jazz_engine_AppRowSchema *output, const groove_BorrowedRecord *record, jazz_types_RowUuid *out)
{
    const jazz_engine_AggregateResultSchema *aggregate;
    JZ_TRY(jazz_node__node__query_eval__query_result_rows__aggregate_output_schema(output, &aggregate));
    if (aggregate->group_key_fields.len > 1)
        return JZN_INVALID_STORED("aggregate row identity requires one lowered group field");
    groove_Value row_value;
    groove_ValueType row_type;
    if (aggregate->group_key_fields.len == 1) {
        u32 index;
        JZ_TRY(jazz_node__node__query_eval__query_result_rows__aggregate_record_field_index(record, &aggregate->group_key_fields.ptr[0], &index));
        JZ_TRY(groove__records__record__BorrowedRecord__get_idx(record, index, &row_value));
        u32 nf;
        const groove_DescriptorField *fields = groove__records__descriptor__RecordDescriptor__fields(record->descriptor, &nf);
        groove_ValueType_clone(&row_type, &fields[index].value_type);
    } else {
        row_value = gv_str(jz_str_from("global"));
        row_type.tag = GVT_STRING;
        row_type.payload = NULL;
    }
    jz_bytes row;
    jz_err e = jazz_node__node__codec__runtime_result_identity_bytes(&row_value, &row_type, &row);
    groove_Value_drop(&row_value);
    groove_ValueType_drop(&row_type);
    if (JZ_IS_ERR(e)) return e;
    groove_Value ident = gv_str(jz_str_from("identity-only"));
    groove_ValueType st = { GVT_STRING, NULL };
    jz_bytes repl;
    e = jazz_node__node__codec__runtime_result_identity_bytes(&ident, &st, &repl);
    groove_Value_drop(&ident);
    if (JZ_IS_ERR(e)) { jz_bytes_drop(&row); return e; }
    jazz_node_ResultMemberEntry entry;
    entry.tag = JZ_RME_SYNTHETIC;
    entry.table = jz_str_from("aggregate_result");
    entry.row = row;
    entry.replacement = jazz_node__node__SyntheticReplacementToken__from_encoded_record(repl);
    e = jazz_node__node__query_eval__aggregate_result_member_row_uuid(&entry, out);
    jz_str_drop(&entry.table);
    jz_bytes_drop(&entry.row);
    jz_bytes_drop(&entry.replacement);
    return e;
}

static int cmp_scalar(u64 a, u64 b) { return a < b ? -1 : a > b; }
static int cmp_signed(i64 a, i64 b) { return a < b ? -1 : a > b; }
static int f64_total_cmp(f64 a, f64 b)
{
    i64 l, r;
    memcpy(&l, &a, 8);
    memcpy(&r, &b, 8);
    l ^= (i64)(((u64)(l >> 63)) >> 1);
    r ^= (i64)(((u64)(r >> 63)) >> 1);
    return cmp_signed(l, r);
}

static int jazz_node__node__query_eval__query_result_rows__compare_order_value_slices(const groove_ValueVec *left, const groove_ValueVec *right)
{
    u32 n = left->len < right->len ? left->len : right->len;
    for (u32 i = 0; i < n; i++) {
        int o = jazz_node__node__query_eval__query_result_rows__compare_order_values(&left->ptr[i], &right->ptr[i]);
        if (o != 0) return o;
    }
    return cmp_scalar(left->len, right->len);
}

static int jazz_node__node__query_eval__query_result_rows__compare_order_values(const groove_Value *left, const groove_Value *right)
{
    if (left->tag != right->tag) return 0;
    switch (left->tag) {
    case GV_U8: return cmp_scalar(left->v.u8_, right->v.u8_);
    case GV_U16: return cmp_scalar(left->v.u16_, right->v.u16_);
    case GV_U32: return cmp_scalar(left->v.u32_, right->v.u32_);
    case GV_U64: return cmp_scalar(left->v.u64_, right->v.u64_);
    case GV_I32: return cmp_signed(left->v.i32_, right->v.i32_);
    case GV_I64: return cmp_signed(left->v.i64_, right->v.i64_);
    case GV_F64: return f64_total_cmp(left->v.f64_, right->v.f64_);
    case GV_BOOL: return cmp_scalar(left->v.b, right->v.b);
    case GV_STRING: return jz_str_cmp(&left->v.s, &right->v.s);
    case GV_BYTES: return jz_bytes_cmp(&left->v.bytes, &right->v.bytes);
    case GV_UUID: return jz_uuid_cmp(&left->v.uuid, &right->v.uuid);
    case GV_ENUM_TAG: return cmp_scalar(left->v.enum_tag, right->v.enum_tag);
    case GV_TUPLE:
    case GV_ARRAY: return jazz_node__node__query_eval__query_result_rows__compare_order_value_slices(&left->v.items, &right->v.items);
    case GV_NULLABLE: {
        const groove_Value *l = left->v.nullable, *r = right->v.nullable;
        if (l && r) return jazz_node__node__query_eval__query_result_rows__compare_order_values(l, r);
        if (!l && r) return -1;
        if (l && !r) return 1;
        return 0;
    }
    default: return 0;
    }
}
