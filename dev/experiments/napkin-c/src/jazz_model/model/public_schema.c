/* model/public_schema.rs: JSON-bearing public value / schema admission checks. */
#include "jazz_model/jazz_model.h"

#define PS(x) jazz_model__model__public_schema__##x

static const char *err_text(const jz_err *e, u32 *len) {
    *len = e->msg ? e->msg->len : 0;
    return e->msg ? e->msg->ptr : "";
}

jz_err PS(validate_json_value)(const jazz_model_public_Value *value, const jazz_model_public_ColumnType *t, jz_slice path) {
    if (value->tag == JAZZ_PV_NULL) return JZ_OK;
    if (value->tag == JAZZ_PV_TEXT && t->tag == JAZZ_CT_JSON) {
        jz_json *instance = NULL;
        jz_err e = jz_json_parse(jz_str_slice(&value->u.text), &instance);
        if (e.code) {
            u32 n;
            const char *m = err_text(&e, &n);
            jz_err r = jz_err_fmt(JAZZ_MODEL_ERR_STRING, "invalid JSON for column `%.*s`: %.*s", (int)path.len, path.ptr, (int)n, m);
            jz_err_drop(&e);
            return r;
        }
        if (!t->schema) { jz_json_free(instance); return JZ_OK; }
        e = jazz_model__model__json_schema__validate(t->schema, instance);
        jz_json_free(instance);
        if (!e.code) return JZ_OK;
        u32 n;
        const char *m = err_text(&e, &n);
        jz_err r;
        switch (e.code) {
        case JAZZ_MODEL_ERR_JSON_SCHEMA_MISMATCH:
            r = jz_err_fmt(JAZZ_MODEL_ERR_STRING, "JSON schema validation failed for column `%.*s`: %.*s", (int)path.len, path.ptr, (int)n, m);
            break;
        case JAZZ_MODEL_ERR_JSON_SCHEMA_INVALID:
            r = jz_err_fmt(JAZZ_MODEL_ERR_STRING, "JSON schema validation failed for column `%.*s`: invalid declared schema: %.*s",
                           (int)path.len, path.ptr, (int)n, m);
            break;
        default:
            r = jz_err_fmt(JAZZ_MODEL_ERR_STRING, "JSON schema validation unavailable for column `%.*s`: %.*s", (int)path.len, path.ptr, (int)n, m);
            break;
        }
        jz_err_drop(&e);
        return r;
    }
    if (value->tag == JAZZ_PV_ARRAY && t->tag == JAZZ_CT_ARRAY) {
        for (u32 i = 0; i < value->u.array.len; i++) {
            jz_str sub = jz_format("%.*s[%u]", (int)path.len, path.ptr, i);
            jz_err e = PS(validate_json_value)(&value->u.array.ptr[i], t->element, jz_str_slice(&sub));
            jz_str_drop(&sub);
            JZ_TRY(e);
        }
        return JZ_OK;
    }
    if (value->tag == JAZZ_PV_ENUM && (t->tag == JAZZ_CT_ENUM_PAYLOAD || t->tag == JAZZ_CT_CATALOGUE_ENUM_PAYLOAD)) {
        const jazz_model_EnumCaseDescriptor *selected = NULL;
        for (u32 i = 0; i < t->cases.len; i++)
            if (jz_str_eq(&t->cases.ptr[i].name, &value->u.enum_.case_)) { selected = &t->cases.ptr[i]; break; }
        if (!selected) return JZ_OK;
        u32 n = selected->fields.len < value->u.enum_.values.len ? selected->fields.len : value->u.enum_.values.len;
        for (u32 i = 0; i < n; i++) {
            const jazz_model_ColumnDescriptor *field = &selected->fields.ptr[i];
            jz_str sub = jz_format("%.*s.%.*s.%.*s", (int)path.len, path.ptr, (int)value->u.enum_.case_.len, value->u.enum_.case_.ptr,
                                   (int)field->name->len, field->name->ptr);
            jz_err e = PS(validate_json_value)(&value->u.enum_.values.ptr[i], &field->column_type, jz_str_slice(&sub));
            jz_str_drop(&sub);
            JZ_TRY(e);
        }
        return JZ_OK;
    }
    return JZ_OK;
}

jz_err PS(validate_json_schemas)(const jazz_model_public_ColumnType *t, jz_slice path) {
    switch (t->tag) {
    case JAZZ_CT_JSON: {
        if (!t->schema) return JZ_OK;
        jz_err e = jazz_model__model__json_schema__check_schema(t->schema);
        if (!e.code) return JZ_OK;
        u32 n;
        const char *m = err_text(&e, &n);
        jz_err r;
        if (e.code == JAZZ_MODEL_ERR_JSON_SCHEMA_INVALID || e.code == JAZZ_MODEL_ERR_JSON_SCHEMA_MISMATCH)
            r = jz_err_fmt(JAZZ_MODEL_ERR_STRING, "invalid JSON schema for column `%.*s`: %.*s", (int)path.len, path.ptr, (int)n, m);
        else
            r = jz_err_fmt(JAZZ_MODEL_ERR_STRING, "JSON schema validation unavailable for column `%.*s`: %.*s", (int)path.len, path.ptr, (int)n, m);
        jz_err_drop(&e);
        return r;
    }
    case JAZZ_CT_ARRAY: {
        jz_str sub = jz_format("%.*s[]", (int)path.len, path.ptr);
        jz_err e = PS(validate_json_schemas)(t->element, jz_str_slice(&sub));
        jz_str_drop(&sub);
        return e;
    }
    case JAZZ_CT_ROW:
        for (u32 i = 0; i < t->columns->columns.len; i++) {
            const jazz_model_ColumnDescriptor *field = &t->columns->columns.ptr[i];
            jz_str sub = jz_format("%.*s.%.*s", (int)path.len, path.ptr, (int)field->name->len, field->name->ptr);
            jz_err e = PS(validate_json_schemas)(&field->column_type, jz_str_slice(&sub));
            jz_str_drop(&sub);
            JZ_TRY(e);
        }
        return JZ_OK;
    case JAZZ_CT_ENUM_PAYLOAD: case JAZZ_CT_CATALOGUE_ENUM_PAYLOAD:
        for (u32 c = 0; c < t->cases.len; c++) {
            const jazz_model_EnumCaseDescriptor *ec = &t->cases.ptr[c];
            for (u32 i = 0; i < ec->fields.len; i++) {
                const jazz_model_ColumnDescriptor *field = &ec->fields.ptr[i];
                jz_str sub = jz_format("%.*s.%.*s.%.*s", (int)path.len, path.ptr, (int)ec->name.len, ec->name.ptr,
                                       (int)field->name->len, field->name->ptr);
                jz_err e = PS(validate_json_schemas)(&field->column_type, jz_str_slice(&sub));
                jz_str_drop(&sub);
                JZ_TRY(e);
            }
        }
        return JZ_OK;
    default: return JZ_OK;
    }
}
