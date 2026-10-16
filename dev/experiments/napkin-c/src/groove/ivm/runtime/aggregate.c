/* groove::ivm::runtime::aggregate: aggregate planning and evaluation. */
#include "mod.h"

static const jz_str *plan_expr_field(const groove_PlanExpr *expr) {
    switch (expr->tag) {
    case groove_PlanExpr_Field: case groove_PlanExpr_Nullable: case groove_PlanExpr_NullableFlat:
    case groove_PlanExpr_EnumTagRemap: case groove_PlanExpr_EnumRemap: case groove_PlanExpr_RecursiveEnumRemap:
        return &expr->field;
    case groove_PlanExpr_Literal: case groove_PlanExpr_Null: default:
        return NULL;
    }
}

jz_err groove__ivm__runtime__aggregate__resolve_aggregate_expr(groove_RecordDescriptor input, const groove_AggregateExpr *aggregate, groove_AggregateExpr *out) {
    /* Only validate: evaluation resolves this same PlanExpr name again. */
    if (aggregate->has_expression) {
        const jz_str *field = plan_expr_field(&aggregate->expression);
        u32 idx;
        if (field && !groove__ivm__runtime__record_projection__resolve_field_name(input, field->ptr, field->len, &idx))
            return GROOVE_IVM_ERR_S(GROOVE_IVM_GraphFieldNotFound, field);
    }
    groove_AggregateExpr_clone(out, aggregate);
    return JZ_OK;
}

static bool positive_weight(void *item, void *ctx) { (void)ctx; return ((groove_RecordDelta *)item)->weight > 0; }

/* Reconstructs the positive pre-tick input multiset from its post-tick state. */
void groove__ivm__runtime__aggregate__records_before_from_deltas(groove_RecordDeltaVec after_records, groove_RecordDeltaVec deltas, groove_RecordDeltaVec *out) {
    groove__ivm__runtime__windows__records_before_deltas(after_records, deltas.ptr, deltas.len, out);
    jz_vec_drop(&deltas, sizeof(groove_RecordDelta), groove_RecordDelta_drop);
    jz_vec_retain(out, sizeof(groove_RecordDelta), positive_weight, NULL, groove_RecordDelta_drop);
}

static groove_Value null_value(void) { groove_Value v; v.tag = groove_Value_Nullable; v.v.nullable = NULL; return v; }

static jz_err evaluate_aggregate_expr(const groove_BorrowedRecord *record, const groove_PlanExpr *expr, groove_Value *out) {
    groove_Value fv;
    switch (expr->tag) {
    case groove_PlanExpr_Field: case groove_PlanExpr_Nullable: case groove_PlanExpr_NullableFlat:
    case groove_PlanExpr_EnumTagRemap: case groove_PlanExpr_EnumRemap: {
        u32 idx;
        if (!groove__ivm__runtime__record_projection__resolve_field_name(record->descriptor, expr->field.ptr, expr->field.len, &idx))
            return GROOVE_IVM_ERR_S(GROOVE_IVM_GraphFieldNotFound, &expr->field);
        JZ_TRY(groove__records__BorrowedRecord__get_idx(record, idx, &fv));
        if (expr->tag == groove_PlanExpr_EnumTagRemap)
            return groove__ivm__runtime__record_projection__remap_enum_tag(fv, expr->tags8.ptr, expr->tags8.len, out);
        if (expr->tag == groove_PlanExpr_EnumRemap)
            return groove__ivm__runtime__record_projection__remap_enum(fv, expr->tags32.ptr, expr->tags32.len, out);
        *out = fv;
        return JZ_OK;
    }
    /* Aggregates do not carry an output descriptor, so there is no
     * well-defined target boundary for a recursive re-encoding. */
    case groove_PlanExpr_RecursiveEnumRemap: return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
    case groove_PlanExpr_Literal: groove__ivm__op_types__LiteralValue__to_value(&expr->literal, out); return JZ_OK;
    case groove_PlanExpr_Null: default: *out = null_value(); return JZ_OK;
    }
}

/* unwrap_nullable_value: returns false for SQL null */
static bool unwrap_nullable_value(groove_Value *v) {
    if (v->tag != groove_Value_Nullable) return true;
    if (!v->v.nullable) return false;
    groove_Value inner = *v->v.nullable;
    jz_free(v->v.nullable);
    *v = inner;
    return true;
}

/* One live (positive-weight, non-null) value of `expr`; iterator over records. */
typedef struct LiveIter { const groove_RecordDelta *records; u32 n; u32 i; groove_RecordDescriptor desc; const groove_PlanExpr *expr; } LiveIter;
static jz_err live_next(LiveIter *it, bool *has, const groove_RecordDelta **record, i64 *weight, groove_Value *value) {
    while (it->i < it->n) {
        const groove_RecordDelta *r = &it->records[it->i++];
        if (r->weight <= 0) continue;
        groove_BorrowedRecord b = groove__records__BorrowedRecord__new(jz_bytes_slice(&r->record), it->desc);
        JZ_TRY(evaluate_aggregate_expr(&b, it->expr, value));
        if (!unwrap_nullable_value(value)) { groove_Value_drop(value); continue; }
        *has = true; *record = r; *weight = r->weight;
        return JZ_OK;
    }
    *has = false;
    return JZ_OK;
}

static jz_err add_weighted_u64(u64 current, u64 value, i64 weight, u64 *out) {
    if (weight < 0) return GROOVE_IVM_ERR(GROOVE_IVM_AggregateOverflow);
    u64 product;
    if (__builtin_mul_overflow(value, (u64)weight, &product)) return GROOVE_IVM_ERR(GROOVE_IVM_AggregateOverflow);
    if (__builtin_add_overflow(current, product, out)) return GROOVE_IVM_ERR(GROOVE_IVM_AggregateOverflow);
    return JZ_OK;
}

static jz_err add_weighted_i64(i64 current, i64 value, i64 weight, i64 *out) {
    i64 product;
    if (__builtin_mul_overflow(value, weight, &product)) return GROOVE_IVM_ERR(GROOVE_IVM_AggregateOverflow);
    if (__builtin_add_overflow(current, product, out)) return GROOVE_IVM_ERR(GROOVE_IVM_AggregateOverflow);
    return JZ_OK;
}

static jz_err numeric_value_as_f64(const groove_Value *v, f64 *out) {
    switch (v->tag) {
    case groove_Value_U8: *out = v->v.u8v; return JZ_OK;
    case groove_Value_U16: *out = v->v.u16v; return JZ_OK;
    case groove_Value_U32: *out = v->v.u32v; return JZ_OK;
    case groove_Value_U64: case groove_Value_U48: *out = (f64)v->v.u64v; return JZ_OK;
    case groove_Value_I32: *out = v->v.i32v; return JZ_OK;
    case groove_Value_I64: *out = (f64)v->v.i64v; return JZ_OK;
    case groove_Value_F64: *out = v->v.f64v; return JZ_OK;
    default: return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
    }
}

static jz_err aggregate_sum(const groove_RecordDelta *records, u32 n, groove_RecordDescriptor desc, const groove_AggregateExpr *aggregate, bool *some, groove_Value *out) {
    if (!aggregate->has_expression) return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
    int kind = -1;
    u64 u64_sum = 0; i64 i64_sum = 0; f64 f64_sum = 0;
    LiveIter it = { records, n, 0, desc, &aggregate->expression };
    for (;;) {
        bool has; const groove_RecordDelta *r; i64 weight; groove_Value value;
        JZ_TRY(live_next(&it, &has, &r, &weight, &value));
        if (!has) break;
        jz_err e = JZ_OK;
        switch (value.tag) {
        case groove_Value_U8: if (kind < 0) kind = groove_ValueType_U8; e = add_weighted_u64(u64_sum, value.v.u8v, weight, &u64_sum); break;
        case groove_Value_U16: if (kind < 0) kind = groove_ValueType_U16; e = add_weighted_u64(u64_sum, value.v.u16v, weight, &u64_sum); break;
        case groove_Value_U32: if (kind < 0) kind = groove_ValueType_U32; e = add_weighted_u64(u64_sum, value.v.u32v, weight, &u64_sum); break;
        case groove_Value_U64: if (kind < 0) kind = groove_ValueType_U64; e = add_weighted_u64(u64_sum, value.v.u64v, weight, &u64_sum); break;
        case groove_Value_I32: if (kind < 0) kind = groove_ValueType_I32; e = add_weighted_i64(i64_sum, value.v.i32v, weight, &i64_sum); break;
        case groove_Value_I64: if (kind < 0) kind = groove_ValueType_I64; e = add_weighted_i64(i64_sum, value.v.i64v, weight, &i64_sum); break;
        case groove_Value_F64: if (kind < 0) kind = groove_ValueType_F64; f64_sum += value.v.f64v * (f64)weight; break;
        default: e = GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator); break;
        }
        groove_Value_drop(&value);
        JZ_TRY(e);
    }
    *some = true;
    switch (kind) {
    case -1: *some = false; return JZ_OK;
    case groove_ValueType_U8: if (u64_sum > UINT8_MAX) return GROOVE_IVM_ERR(GROOVE_IVM_AggregateOverflow); out->tag = groove_Value_U8; out->v.u8v = (u8)u64_sum; return JZ_OK;
    case groove_ValueType_U16: if (u64_sum > UINT16_MAX) return GROOVE_IVM_ERR(GROOVE_IVM_AggregateOverflow); out->tag = groove_Value_U16; out->v.u16v = (u16)u64_sum; return JZ_OK;
    case groove_ValueType_U32: if (u64_sum > UINT32_MAX) return GROOVE_IVM_ERR(GROOVE_IVM_AggregateOverflow); out->tag = groove_Value_U32; out->v.u32v = (u32)u64_sum; return JZ_OK;
    case groove_ValueType_U64: out->tag = groove_Value_U64; out->v.u64v = u64_sum; return JZ_OK;
    case groove_ValueType_I32: if (i64_sum > INT32_MAX || i64_sum < INT32_MIN) return GROOVE_IVM_ERR(GROOVE_IVM_AggregateOverflow); out->tag = groove_Value_I32; out->v.i32v = (i32)i64_sum; return JZ_OK;
    case groove_ValueType_F64: out->tag = groove_Value_F64; out->v.f64v = f64_sum; return JZ_OK;
    case groove_ValueType_I64: out->tag = groove_Value_I64; out->v.i64v = i64_sum; return JZ_OK;
    default: return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
    }
}

static int finite(f64 x) { return x - x == 0; }

static jz_err aggregate_avg(const groove_RecordDelta *records, u32 n, groove_RecordDescriptor desc, const groove_AggregateExpr *aggregate, bool *some, groove_Value *out) {
    if (!aggregate->has_expression) return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
    f64 sum = 0, scale = 0;
    i64 count = 0;
    bool has_f64 = false, has_non_finite = false;
    LiveIter it = { records, n, 0, desc, &aggregate->expression };
    for (;;) {
        bool has; const groove_RecordDelta *r; i64 weight; groove_Value value;
        JZ_TRY(live_next(&it, &has, &r, &weight, &value));
        if (!has) break;
        f64 numeric;
        jz_err e = numeric_value_as_f64(&value, &numeric);
        has_f64 |= value.tag == groove_Value_F64;
        groove_Value_drop(&value);
        JZ_TRY(e);
        sum += numeric * (f64)weight;
        count += weight;
        if (finite(numeric)) { f64 a = numeric < 0 ? -numeric : numeric; if (a > scale) scale = a; }
        else has_non_finite = true;
    }
    *some = true;
    out->tag = groove_Value_F64;
    if (count <= 0) { *some = false; return JZ_OK; }
    /* Keep the established accumulation order for integer and non-finite
     * inputs. Scaling is only needed for finite F64 values. */
    if (!has_f64 || has_non_finite || finite(sum)) { out->v.f64v = sum / (f64)count; return JZ_OK; }
    if (scale == 0.0) { out->v.f64v = 0.0; return JZ_OK; }
    f64 scaled_sum = 0;
    LiveIter it2 = { records, n, 0, desc, &aggregate->expression };
    for (;;) {
        bool has; const groove_RecordDelta *r; i64 weight; groove_Value value;
        JZ_TRY(live_next(&it2, &has, &r, &weight, &value));
        if (!has) break;
        f64 numeric;
        jz_err e = numeric_value_as_f64(&value, &numeric);
        groove_Value_drop(&value);
        JZ_TRY(e);
        scaled_sum += (numeric / scale) * (f64)weight;
    }
    f64 normalized = scaled_sum / (f64)count;
    if (normalized < -1.0) normalized = -1.0;
    if (normalized > 1.0) normalized = 1.0;
    out->v.f64v = scale * normalized;
    return JZ_OK;
}

static jz_err aggregate_extremum(const groove_RecordDelta *records, u32 n, groove_RecordDescriptor desc, const groove_AggregateExpr *aggregate, bool *some, groove_Value *out) {
    if (!aggregate->has_expression) return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
    bool has_best = false;
    jz_bytes best_key = {0};
    const groove_RecordDelta *best_record = NULL;
    groove_Value best_value;
    LiveIter it = { records, n, 0, desc, &aggregate->expression };
    for (;;) {
        bool has; const groove_RecordDelta *r; i64 weight; groove_Value value;
        jz_err e = live_next(&it, &has, &r, &weight, &value);
        if (JZ_IS_ERR(e)) { if (has_best) { jz_bytes_drop(&best_key); groove_Value_drop(&best_value); } return e; }
        if (!has) break;
        jz_bytes value_key = {0};
        e = groove__ivm__runtime__key_encoding__encode_key_part(&value_key, &value);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&value_key); groove_Value_drop(&value); if (has_best) { jz_bytes_drop(&best_key); groove_Value_drop(&best_value); } return e; }
        bool replaces = !has_best;
        if (has_best) {
            int c = jz_bytes_cmp(&value_key, &best_key);
            int rc = jz_bytes_cmp(&r->record, &best_record->record);
            switch (aggregate->function) {
            case groove_AggFn_Min: replaces = c < 0 || (c == 0 && rc < 0); break;
            case groove_AggFn_Max: replaces = c > 0 || (c == 0 && rc < 0); break;
            default: replaces = false; break;
            }
        }
        if (replaces) {
            if (has_best) { jz_bytes_drop(&best_key); groove_Value_drop(&best_value); }
            best_key = value_key; best_record = r; best_value = value; has_best = true;
        } else {
            jz_bytes_drop(&value_key); groove_Value_drop(&value);
        }
    }
    *some = has_best;
    if (has_best) { jz_bytes_drop(&best_key); *out = best_value; }
    return JZ_OK;
}

static groove_Value nullable_aggregate_value(bool some, groove_Value v) {
    groove_Value out;
    out.tag = groove_Value_Nullable;
    out.v.nullable = NULL;
    if (some) { out.v.nullable = jz_alloc(sizeof(groove_Value)); *out.v.nullable = v; }
    return out;
}

static jz_err evaluate_aggregate(const groove_RecordDelta *records, u32 n, groove_RecordDescriptor desc, const groove_AggregateExpr *aggregate, groove_Value *out) {
    if (aggregate->distinct) return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
    bool some = false;
    groove_Value v;
    switch (aggregate->function) {
    case groove_AggFn_Count: {
        u64 count = 0;
        for (u32 i = 0; i < n; i++) {
            if (records[i].weight <= 0) continue;
            if (aggregate->has_expression) {
                groove_BorrowedRecord b = groove__records__BorrowedRecord__new(jz_bytes_slice(&records[i].record), desc);
                groove_Value value;
                JZ_TRY(evaluate_aggregate_expr(&b, &aggregate->expression, &value));
                bool is_null = value.tag == groove_Value_Nullable && !value.v.nullable;
                groove_Value_drop(&value);
                if (is_null) continue;
            }
            if (__builtin_add_overflow(count, (u64)records[i].weight, &count)) return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
        }
        out->tag = groove_Value_U64;
        out->v.u64v = count;
        return JZ_OK;
    }
    case groove_AggFn_Sum: JZ_TRY(aggregate_sum(records, n, desc, aggregate, &some, &v)); break;
    case groove_AggFn_Avg: JZ_TRY(aggregate_avg(records, n, desc, aggregate, &some, &v)); break;
    case groove_AggFn_Min: case groove_AggFn_Max: JZ_TRY(aggregate_extremum(records, n, desc, aggregate, &some, &v)); break;
    }
    *out = nullable_aggregate_value(some, v);
    return JZ_OK;
}

jz_err groove__ivm__runtime__aggregate__aggregate_row_from_records(groove_RecordDescriptor input_desc, groove_RecordDescriptor output_desc, const groove_AggregateOp *aggregate, const groove_RecordDelta *records, u32 n, bool *some, jz_bytes *out) {
    const groove_RecordDelta *first = NULL;
    i64 total_weight = 0;
    for (u32 i = 0; i < n; i++) {
        if (records[i].weight < 0) return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
        if (records[i].weight > 0) {
            total_weight += records[i].weight;
            if (!first) first = &records[i];
        }
    }
    if (total_weight == 0 && aggregate->group_key.len != 0) { *some = false; return JZ_OK; }
    groove_ValueVec values = {0};
    jz_err e = JZ_OK;
    if (first) {
        groove_BorrowedRecord b = groove__records__BorrowedRecord__new(jz_bytes_slice(&first->record), input_desc);
        for (u32 i = 0; i < aggregate->group_key.len && !JZ_IS_ERR(e); i++) {
            groove_Value v;
            e = evaluate_aggregate_expr(&b, &aggregate->group_key.ptr[i], &v);
            if (!JZ_IS_ERR(e)) JZ_VEC_PUSH(values, v);
        }
    }
    for (u32 i = 0; i < aggregate->aggregates.len && !JZ_IS_ERR(e); i++) {
        groove_Value v;
        e = evaluate_aggregate(records, n, input_desc, &aggregate->aggregates.ptr[i], &v);
        if (!JZ_IS_ERR(e)) JZ_VEC_PUSH(values, v);
    }
    if (!JZ_IS_ERR(e)) {
        e = groove__records__RecordDescriptor__create(output_desc, values.ptr, values.len, out);
        *some = !JZ_IS_ERR(e);
    }
    jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop_v);
    return e;
}
