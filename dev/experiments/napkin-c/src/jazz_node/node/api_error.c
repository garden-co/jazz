/* Port of crates/jazz/layers/node/src/node/api_error.rs */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

static const char *const ERROR_CODE_DEBUG[] = {
    "Schema", "Query", "WriteRejected", "TransactionConflict", "Storage", "Protocol", "Backpressure",
    "NotObserved", "HistoricalReadRequiresServer"
};

void jazz_node__node__api_error__Error__Display__fmt(const jazz_node_ApiError *e, jz_str *out) {
    switch (e->code) {
    case JZN_API_TRANSACTION_CONFLICT:
        jz_str_appendf(out, "(transaction_conflict): %.*s", (int)e->message.len, e->message.ptr);
        break;
    default:
        jz_str_appendf(out, "%s: %.*s", ERROR_CODE_DEBUG[e->code], (int)e->message.len, e->message.ptr);
        break;
    }
}

jazz_node_ApiError jazz_node__node__api_error__Error__new(jazz_node_ErrorCode code, jz_str message) {
    jazz_node_ApiError e = { code, message };
    return e;
}

const char *jazz_node__node__api_error__ErrorCode__as_str(jazz_node_ErrorCode c) {
    switch (c) {
    case JZN_API_SCHEMA: return "schema";
    case JZN_API_QUERY: return "query";
    case JZN_API_WRITE_REJECTED: return "write_rejected";
    case JZN_API_TRANSACTION_CONFLICT: return "transaction_conflict";
    case JZN_API_STORAGE: return "storage";
    case JZN_API_PROTOCOL: return "protocol";
    case JZN_API_BACKPRESSURE: return "backpressure";
    case JZN_API_NOT_OBSERVED: return "not_observed";
    case JZN_API_HISTORICAL_READ_REQUIRES_SERVER: return "historical_read_requires_server";
    }
    JZ_UNREACHABLE();
}

/* serde: Serialize/Deserialize for Error { code, message } (JSON, externally tagged unit enum) */
void jazz_node__node__api_error__Error__Serialize__serialize(const jazz_node_ApiError *e, jz_json_w *w) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "code");
    const char *n = ERROR_CODE_DEBUG[e->code];
    jz_jw_str(w, n, (u32)strlen(n));
    jz_jw_key(w, "message");
    jz_jw_str(w, e->message.ptr, e->message.len);
    jz_jw_end_obj(w);
}

jz_err jazz_node__node__api_error__Error__Deserialize__deserialize(const jz_json *v, jazz_node_ApiError *out) {
    JZ_TRY(jz_json_expect_obj(v, "struct Error"));
    const jz_json *code = jz_json_obj_get(v, "code");
    if (!code) return jz_err_new(JZ_JSON_ERR_MISSING_FIELD, "missing field `code`");
    u32 idx; const jz_json *payload;
    JZ_TRY(jz_json_variant(code, ERROR_CODE_DEBUG, 9, &idx, &payload));
    out->code = (jazz_node_ErrorCode)idx;
    return jz_json_field_str(v, "message", &out->message);
}

/* From<crate::node::Error> */
jazz_node_ApiError jazz_node__node__api_error__Error__From_NodeError__from(jz_err error) {
    jazz_node_ErrorCode code;
    switch (error.code) {
    case JZN_E_HISTORICAL_READ_REQUIRES_SERVER: code = JZN_API_HISTORICAL_READ_REQUIRES_SERVER; break;
    case JZN_E_STORAGE:
    case JZN_E_GROOVE: code = JZN_API_STORAGE; break;
    case JZN_E_QUERY:
    case JZN_E_UNSUPPORTED_EXCLUSIVE_READ: code = JZN_API_QUERY; break;
    case JZN_E_TRANSACTION_CONFLICT: code = JZN_API_TRANSACTION_CONFLICT; break;
    case JZN_E_TABLE_NOT_FOUND:
    case JZN_E_UNSUPPORTED_COLUMN_TYPE:
    case JZN_E_INVALID_MERGEABLE_COMMIT: code = JZN_API_SCHEMA; break;
    default: code = JZN_API_PROTOCOL; break;
    }
    jz_str msg = {0};
    jazz_node__node__Error__Display__fmt(&error, &msg);
    jz_err_drop(&error);
    return jazz_node__node__api_error__Error__new(code, msg);
}

/* From<QueryError> */
void jazz_model__query__QueryError__Display__fmt(const jazz_model_QueryError *e, jz_str *out);
jazz_node_ApiError jazz_node__node__api_error__Error__From_QueryError__from(const jazz_model_QueryError *error) {
    jz_str msg = {0};
    jazz_model__query__QueryError__Display__fmt(error, &msg);
    return jazz_node__node__api_error__Error__new(JZN_API_QUERY, msg);
}

jz_err jazz_node__node__api_error__Error__into_err(jazz_node_ErrorCode code, jz_str message) {
    jz_err e = jz_err_fmt(JZN_API_ERR_BASE + code, "%.*s", (int)message.len, message.ptr);
    jz_str_drop(&message);
    return e;
}
