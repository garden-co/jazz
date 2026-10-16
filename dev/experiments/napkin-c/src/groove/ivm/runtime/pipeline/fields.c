/* groove::ivm::runtime::pipeline::fields: process-local field routes. Total
 * projections change this mapping, not row bytes. */
#include "../mod.h"

static void const_bytes_drop(void *p) { jz_bytes_drop((jz_bytes *)p); }
static jz_bytes *const_new(const u8 *p, u32 n) {
    jz_bytes *b = jz_rc_new(sizeof(jz_bytes), const_bytes_drop);
    *b = jz_bytes_from(p, n);
    return b;
}

static void field_clone(groove_RouteField *dst, const groove_RouteField *src) {
    *dst = *src;
    memset(&dst->path, 0, sizeof(dst->path));
    jz_vec_clone(&dst->path, &src->path, sizeof(groove_DescIdx), NULL);
    if (src->constant) dst->constant = jz_rc_clone(src->constant);
}
static void field_drop(groove_RouteField *f) {
    jz_vec_drop(&f->path, sizeof(groove_DescIdx), NULL);
    if (f->constant) jz_rc_release(f->constant);
}

typedef struct FieldsArc { u32 len; groove_RouteField items[]; } FieldsArc;
static void fields_arc_drop(void *p) { FieldsArc *a = p; for (u32 i = 0; i < a->len; i++) field_drop(&a->items[i]); }
static groove_RouteField *fields_arc_new(u32 n) {
    FieldsArc *a = jz_rc_new(sizeof(FieldsArc) + (usize)n * sizeof(groove_RouteField), fields_arc_drop);
    a->len = n;
    memset(a->items, 0, (usize)n * sizeof(groove_RouteField));
    return a->items;
}
static void *fields_arc_of(groove_RouteField *items) { return (u8 *)items - offsetof(FieldsArc, items); }

void groove_FieldRoutes_clone(groove_FieldRoutes *dst, const groove_FieldRoutes *src) {
    *dst = *src;
    if (src->fields) jz_rc_clone(fields_arc_of(src->fields));
}
void groove_FieldRoutes_drop(groove_FieldRoutes *r) { if (r->fields) jz_rc_release(fields_arc_of(r->fields)); r->fields = NULL; }

jz_err groove__ivm__runtime__pipeline__fields__Field__bytes(const groove_RouteField *f, jz_slice raw, jz_slice *out) {
    if (f->origin_tag == groove_Origin_Constant) { *out = jz_bytes_slice(f->constant); return JZ_OK; }
    jz_slice bytes = raw;
    for (u32 i = 0; i < f->path.len; i++) {
        u32 start, end;
        JZ_TRY(groove__records__RecordDescriptor__field_span(f->path.ptr[i].descriptor, bytes, f->path.ptr[i].index, &start, &end));
        bytes = jz_slice_of(bytes.ptr + start, end - start);
    }
    *out = bytes;
    return JZ_OK;
}

void groove__ivm__runtime__pipeline__fields__FieldRoutes__identity(groove_RecordDescriptor descriptor, groove_FieldRoutes *out) {
    u32 n = groove__records__RecordDescriptor__len(descriptor);
    out->source = descriptor;
    out->descriptor = descriptor;
    out->fields = fields_arc_new(n);
    out->fields_len = n;
    for (u32 i = 0; i < n; i++) {
        groove_DescIdx di = { descriptor, i };
        out->fields[i].origin_tag = groove_Origin_Source;
        JZ_VEC_PUSH(out->fields[i].path, di);
    }
    out->reuses_input = true;
}

bool groove__ivm__runtime__pipeline__fields__FieldRoutes__compose(const groove_FieldRoutes *self, groove_RecordDescriptor descriptor, const groove_PreparedProjection *projection, groove_FieldRoutes *out) {
    u32 n = projection->fields.len;
    groove_RouteField *fields = fields_arc_new(n);
    bool ok = true;
    for (u32 i = 0; i < n && ok; i++) {
        const groove_RawProjectionField *pf = &projection->fields.ptr[i];
        groove_RouteField *field = &fields[i];
        switch (pf->tag) {
        case groove_RPF_Copy:
            if (pf->source_idx >= self->fields_len) { ok = false; break; }
            field_clone(field, &self->fields[pf->source_idx]);
            break;
        case groove_RPF_WrapNullable:
            if (pf->source_idx >= self->fields_len) { ok = false; break; }
            field_clone(field, &self->fields[pf->source_idx]);
            if (field->present_wrappers == UINT32_MAX) { ok = false; break; }
            field->present_wrappers += 1;
            break;
        case groove_RPF_Nested: {
            if (pf->path.len == 0) { ok = false; break; }
            u32 first = pf->path.ptr[0].index;
            if (first >= self->fields_len) { ok = false; break; }
            field_clone(field, &self->fields[first]);
            /* A nested record cannot be read through a nullable tag. */
            if (field->present_wrappers != 0) { ok = false; break; }
            if (field->origin_tag == groove_Origin_Source) {
                jz_vec_extend(&field->path, pf->path.ptr + 1, pf->path.len - 1, sizeof(groove_DescIdx));
            } else {
                jz_slice value = jz_bytes_slice(field->constant);
                for (u32 k = 1; k < pf->path.len && ok; k++) {
                    u32 start, end;
                    jz_err e = groove__records__RecordDescriptor__field_span(pf->path.ptr[k].descriptor, value, pf->path.ptr[k].index, &start, &end);
                    if (JZ_IS_ERR(e)) { jz_err_drop(&e); ok = false; break; }
                    value = jz_slice_of(value.ptr + start, end - start);
                }
                if (ok) {
                    jz_bytes *nb = const_new(value.ptr, value.len);
                    jz_rc_release(field->constant);
                    field->constant = nb;
                }
            }
            break;
        }
        case groove_RPF_Encoded:
            field->origin_tag = groove_Origin_Constant;
            field->constant = const_new(pf->bytes.ptr, pf->bytes.len);
            field->present_wrappers = 0;
            break;
        /* Never elide an expression which can fail or omit a row. */
        case groove_RPF_Error: case groove_RPF_Evaluate: default:
            ok = false;
            break;
        }
    }
    if (!ok) { jz_rc_release(fields_arc_of(fields)); return false; }
    JZ_VEC(groove_RawProjectionField) copies = {0};
    bool all_copies = true;
    for (u32 i = 0; i < n; i++) {
        if (fields[i].origin_tag != groove_Origin_Source || fields[i].present_wrappers != 0 || fields[i].path.len != 1) { all_copies = false; break; }
        groove_RawProjectionField c;
        memset(&c, 0, sizeof(c));
        c.tag = groove_RPF_Copy;
        c.source_idx = fields[i].path.ptr[0].index;
        JZ_VEC_PUSH(copies, c);
    }
    bool reuses_input = false;
    if (all_copies) {
        groove_PreparedProjection *p = groove__records__PreparedProjection__new(self->source, descriptor, (void *)&copies);
        reuses_input = p->reuses_input;
        jz_rc_release(p);
    } else {
        jz_vec_drop(&copies, sizeof(groove_RawProjectionField), NULL);
    }
    out->source = self->source;
    out->descriptor = descriptor;
    out->fields = fields;
    out->fields_len = n;
    out->reuses_input = reuses_input;
    return true;
}

typedef struct AppendCtx { const groove_FieldRoutes *self; jz_slice raw; } AppendCtx;
static jz_err append_field(u32 index, jz_bytes *output, void *c) {
    AppendCtx *a = c;
    const groove_RouteField *field = &a->self->fields[index];
    for (u32 i = 0; i < field->present_wrappers; i++) jz_bytes_push_u8(output, 1);
    jz_slice bytes;
    JZ_TRY(groove__ivm__runtime__pipeline__fields__Field__bytes(field, a->raw, &bytes));
    jz_bytes_push(output, bytes.ptr, bytes.len);
    return JZ_OK;
}

jz_err groove__ivm__runtime__pipeline__fields__FieldRoutes__append(const groove_FieldRoutes *self, jz_slice raw, jz_bytes *output, u32 *start, u32 *end) {
    AppendCtx ctx = { self, raw };
    return groove__records__RecordDescriptor__write_projected_fields_into(self->descriptor, output, append_field, &ctx, start, end);
}

groove_RoutedRecord groove__ivm__runtime__pipeline__fields__FieldRoutes__record(const groove_FieldRoutes *self, jz_slice raw) {
    groove_RoutedRecord r = { self, raw };
    return r;
}

static jz_err routed_value(const void *s, const char *name, u32 len, groove_Value *out) {
    const groove_RoutedRecord *self = s;
    u32 index;
    if (!groove__ivm__runtime__record_projection__resolve_field_name(self->routes->descriptor, name, len, &index))
        return groove__records__Error__field_not_found(name, len);
    const groove_RouteField *field = &self->routes->fields[index];
    const groove_ValueType *ty = &groove__records__RecordDescriptor__field(self->routes->descriptor, index)->value_type;
    for (u32 i = 0; i < field->present_wrappers; i++) {
        if (ty->tag != groove_ValueType_Nullable) jz_panic("validated field route");
        ty = ty->v.inner;
    }
    jz_slice bytes;
    JZ_TRY(groove__ivm__runtime__pipeline__fields__Field__bytes(field, self->raw, &bytes));
    groove_Value value;
    JZ_TRY(groove__records__decode_single_field_value(bytes, ty, &value));
    for (u32 i = 0; i < field->present_wrappers; i++) {
        groove_Value wrapped;
        wrapped.tag = groove_Value_Nullable;
        wrapped.v.nullable = jz_alloc(sizeof(groove_Value));
        *wrapped.v.nullable = value;
        value = wrapped;
    }
    *out = value;
    return JZ_OK;
}

static jz_err routed_literal_ordering(const void *s, const char *name, u32 len, const groove_LiteralValue *value, groove_FieldLiteralOrdering *out) {
    const groove_RoutedRecord *self = s;
    u32 index;
    if (!groove__ivm__runtime__record_projection__resolve_field_name(self->routes->descriptor, name, len, &index))
        return groove__ivm__runtime__IvmRuntimeError__with_str(GROOVE_IVM_GraphFieldNotFound, name, len);
    const groove_RouteField *field = &self->routes->fields[index];
    if (field->present_wrappers == 0 && field->origin_tag == groove_Origin_Source && field->path.len > 0) {
        jz_slice raw = self->raw;
        for (u32 i = 0; i + 1 < field->path.len; i++) {
            u32 start, end;
            JZ_TRY(groove__records__RecordDescriptor__field_span(field->path.ptr[i].descriptor, raw, field->path.ptr[i].index, &start, &end));
            raw = jz_slice_of(raw.ptr + start, end - start);
        }
        const groove_DescIdx *last = &field->path.ptr[field->path.len - 1];
        groove_BorrowedRecord b = groove__records__BorrowedRecord__new(raw, last->descriptor);
        return groove__ivm__runtime__key_encoding__record_field_literal_ordering(&b, last->index, value, out);
    }
    out->tag = groove_FLO_Unsupported;
    out->ordering = 0;
    return JZ_OK;
}

const groove_PredicateRecordVt groove__ivm__runtime__pipeline__fields__RoutedRecord__PredicateRecord__VT = { routed_value, routed_literal_ordering };
