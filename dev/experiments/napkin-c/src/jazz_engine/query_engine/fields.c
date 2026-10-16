/* fields.rs: compiler field-name vocabulary. */
#include "mod.h"

#define ROUTE_PARAM_PREFIX "__jazz_route_"
#define CLAIM_PARAM_PREFIX "__jazz_claim_"

static bool slice_strip_prefix(jz_slice s, const char *prefix, jz_slice *rest) {
    u32 n = (u32)strlen(prefix);
    if (s.len < n || memcmp(s.ptr, prefix, n) != 0) return false;
    rest->ptr = s.ptr + n;
    rest->len = s.len - n;
    return true;
}

static bool slice_eq_cstr(jz_slice s, const char *c) {
    u32 n = (u32)strlen(c);
    return s.len == n && memcmp(s.ptr, c, n) == 0;
}

jz_str jazz_engine__query_engine__fields__user_column_field(jz_slice column) {
    jz_str c = jz_str_from_slice(column);
    jz_str out = jazz_model__schema__app_storage_column_name(&c);
    jz_str_drop(&c);
    return out;
}

jazz_engine_StrVec jazz_engine__query_engine__fields__current_row_field_names(const jazz_model_TableSchema *table) {
    jazz_engine_StrVec fields = {0};
    jazz_engine__strvec_push_cstr(&fields, "row_uuid");
    JZ_VEC_FOR(const jazz_model_ColumnSchema, column, table->columns) {
        JZ_VEC_PUSH(fields, jazz_engine__query_engine__fields__user_column_field(JZ_S(&column->name)));
    }
    static const char *const tail[] = {"$createdBy", "$createdAt", "$updatedBy", "$updatedAt", "tx_time", "tx_node_id"};
    for (u32 i = 0; i < 6; i++) jazz_engine__strvec_push_cstr(&fields, tail[i]);
    return fields;
}

jz_str jazz_engine__query_engine__fields__aggregate_output_field(jz_slice output) {
    return jazz_engine__query_engine__fields__aggregate_output_column(output);
}

jz_str jazz_engine__query_engine__fields__aggregate_output_app_field(jz_slice output) {
    jz_str field = jazz_engine__query_engine__fields__aggregate_output_field(output);
    jz_str out = jazz_engine__query_engine__fields__user_column_field(JZ_S(&field));
    jz_str_drop(&field);
    return out;
}

jz_str jazz_engine__query_engine__fields__aggregate_output_column(jz_slice output) {
    jz_slice rest;
    if (slice_strip_prefix(output, JAZZ_ENGINE_AGGREGATE_OUTPUT_PREFIX, &rest)) return jz_str_from_slice(output);
    return jz_format("%s%.*s", JAZZ_ENGINE_AGGREGATE_OUTPUT_PREFIX, (int)output.len, output.ptr);
}

bool jazz_engine__query_engine__fields__aggregate_output_logical_name(jz_slice column, jz_slice *out) {
    return slice_strip_prefix(column, JAZZ_ENGINE_AGGREGATE_OUTPUT_PREFIX, out);
}

bool jazz_engine__query_engine__fields__descriptor_public_name(const groove_DescriptorField *field, jz_slice *out) {
    const jz_str *logical = groove__records__DescriptorField__logical_name(field);
    if (!logical) return false;
    jz_slice name = jz_str_slice(logical);
    if (!field->has_name || !jz_str_eq(&field->name, logical)) {
        *out = name;
        return true;
    }
    static const char *const private_names[] = {
        "$createdBy", "$createdAt", "$updatedBy", "$updatedAt", "created_by", "created_at", "updated_by",
        "updated_at", "branch_key", "row_uuid", "tx_time", "tx_node_id", "schema_version",
        "authored_columns", "global_time", "settle_position",
    };
    for (u32 i = 0; i < sizeof private_names / sizeof private_names[0]; i++)
        if (slice_eq_cstr(name, private_names[i])) return false;
    if (jazz_engine__query_engine__fields__aggregate_output_logical_name(name, out)) return true;
    jz_slice rest;
    if (!slice_strip_prefix(name, "__jazz_", &rest)) {
        *out = name;
        return true;
    }
    return false;
}

jz_str jazz_engine__query_engine__fields__join_field(const char *prefix, jz_slice field) {
    return jz_format("%s%.*s", prefix, (int)field.len, field.ptr);
}

jz_str jazz_engine__query_engine__fields__left_field(jz_slice field) {
    return jazz_engine__query_engine__fields__join_field(JAZZ_ENGINE_LEFT_JOIN_PREFIX, field);
}

jz_str jazz_engine__query_engine__fields__right_field(jz_slice field) {
    return jazz_engine__query_engine__fields__join_field(JAZZ_ENGINE_RIGHT_JOIN_PREFIX, field);
}

jz_str jazz_engine__query_engine__fields__route_param_field(jz_slice param) {
    return jz_format("%s%.*s", ROUTE_PARAM_PREFIX, (int)param.len, param.ptr);
}

bool jazz_engine__query_engine__fields__route_param_from_field(jz_slice field, jz_slice *out) {
    return slice_strip_prefix(field, ROUTE_PARAM_PREFIX, out);
}

static bool str_contains_char(const jz_str *s, char c) {
    return s->len && memchr(s->ptr, c, s->len) != NULL;
}

jz_str jazz_engine__query_engine__fields__claim_param_field(const jazz_engine_ClaimPath *path) {
    if (path->segments.len == 1) {
        const jz_str *segment = &path->segments.ptr[0];
        if (!str_contains_char(segment, '_') && !str_contains_char(segment, ':'))
            return jz_format("%s%.*s", CLAIM_PARAM_PREFIX, (int)segment->len, segment->ptr);
    }
    jz_str field = jz_format("%sv1:", CLAIM_PARAM_PREFIX);
    JZ_VEC_FOR(const jz_str, segment, path->segments) {
        jz_str_appendf(&field, "%u", segment->len);
        jz_str_push_char(&field, ':');
        jz_str_push(&field, segment->ptr, segment->len);
    }
    return field;
}

/* str::split_once(':') + usize::parse */
static bool split_len(jz_slice s, u32 *len, jz_slice *tail) {
    const u8 *colon = s.len ? memchr(s.ptr, ':', s.len) : NULL;
    if (!colon) return false;
    u32 n = (u32)(colon - s.ptr);
    if (n == 0) return false;
    u64 v = 0;
    for (u32 i = 0; i < n; i++) {
        u8 c = s.ptr[i];
        if (c == '+' && i == 0 && n > 1) continue;
        if (c < '0' || c > '9') return false;
        v = v * 10 + (u64)(c - '0');
        if (v > 0xffffffffu) return false;
    }
    *len = (u32)v;
    tail->ptr = colon + 1;
    tail->len = s.len - n - 1;
    return true;
}

bool jazz_engine__query_engine__fields__claim_path_from_param_field(jz_slice field, jazz_engine_ClaimPath *out) {
    jz_slice rest;
    if (!slice_strip_prefix(field, CLAIM_PARAM_PREFIX, &rest)) return false;
    jz_slice typed;
    if (slice_strip_prefix(rest, "typed:", &typed)) {
        u32 len;
        jz_slice tail;
        if (!split_len(typed, &len, &tail)) return false;
        if (tail.len <= len || tail.ptr[len] != ':') return false;
        return jazz_engine__query_engine__fields__claim_path_from_param_field(jz_slice_of(tail.ptr + len + 1, tail.len - len - 1), out);
    }
    jz_slice v1;
    if (!slice_strip_prefix(rest, "v1:", &v1)) {
        jazz_engine_StrVec segments = {0};
        u32 start = 0;
        for (u32 i = 0; i <= rest.len; i++) {
            if (i == rest.len || rest.ptr[i] == '_') {
                JZ_VEC_PUSH(segments, jz_str_from_slice(jz_slice_of(rest.ptr + start, i - start)));
                start = i + 1;
            }
        }
        out->segments = segments;
        return true;
    }
    rest = v1;
    jazz_engine_StrVec segments = {0};
    while (rest.len) {
        u32 len;
        jz_slice tail;
        if (!split_len(rest, &len, &tail) || tail.len < len) {
            jazz_engine__strvec_drop(&segments);
            return false;
        }
        JZ_VEC_PUSH(segments, jz_str_from_slice(jz_slice_of(tail.ptr, len)));
        rest = jz_slice_of(tail.ptr + len, tail.len - len);
    }
    out->segments = segments;
    return true;
}

jz_str jazz_engine__query_engine__fields__table_user_column_field(jz_slice table, jz_slice column) {
    return jz_format("%s_%.*s__%.*s", JAZZ_ENGINE_USER_COLUMN_PREFIX, (int)table.len, table.ptr, (int)column.len, column.ptr);
}
