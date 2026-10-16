/* model/transaction.rs: OpenTransactionId / TransactionId (transaction_id! macro,
 * expanded once per type in Rust; here one set of byte-array helpers). */
#include "jazz_model/jazz_model.h"

#define T(x) jazz_model__model__transaction__##x

static void id_display(jz_str *out, const u8 b[16]) { jz_hex_encode(b, 16, out); }

/* FromStr: "invalid {kind} hex: {err}" / "expected 16-byte {kind}, got {len}" */
static jz_err id_from_str(jz_slice raw, const char *kind, u8 out[16]) {
    jz_bytes bytes = {0};
    jz_err e = jz_hex_decode(raw, &bytes);
    if (e.code) {
        jz_err r = jz_err_fmt(JAZZ_MODEL_ERR_STRING, "invalid %s hex: %.*s", kind,
                              e.msg ? (int)e.msg->len : 0, e.msg ? e.msg->ptr : "");
        jz_err_drop(&e);
        return r;
    }
    u32 len = bytes.len;
    if (len != 16) {
        jz_bytes_drop(&bytes);
        return jz_err_fmt(JAZZ_MODEL_ERR_STRING, "expected 16-byte %s, got %u", kind, len);
    }
    memcpy(out, bytes.ptr, 16);
    jz_bytes_drop(&bytes);
    return JZ_OK;
}

static void id_serialize_json(jz_json_w *w, const u8 b[16]) {
    jz_str s = {0};
    id_display(&s, b);
    jz_jw_str(w, s.ptr, s.len);
    jz_str_drop(&s);
}

static jz_err id_deserialize_json(const jz_json *j, const char *kind, u8 out[16]) {
    if (!j || j->kind != JZ_JSON_STR) return jazz_model__serde_err("invalid type: expected a string");
    jz_err e = id_from_str(jz_str_slice(&j->v.s), kind, out);
    if (e.code) {
        jz_err r = jz_err_fmt(JAZZ_MODEL_ERR_SERDE, "%.*s", e.msg ? (int)e.msg->len : 0, e.msg ? e.msg->ptr : "");
        jz_err_drop(&e);
        return r;
    }
    return JZ_OK;
}

/* ---------------------------------------------------------------- OpenTransactionId */
jazz_model_OpenTransactionId T(OpenTransactionId__new)(void) {
    u8 rnd[10];
    jz_random_bytes(rnd, sizeof rnd);
    jz_uuid u = jz_uuid_v7(jz_now_ms(), rnd);
    jazz_model_OpenTransactionId id;
    memcpy(id.b, u.b, 16);
    return id;
}
void T(OpenTransactionId__Display__fmt)(jz_str *out, const jazz_model_OpenTransactionId *id) { id_display(out, id->b); }
void T(OpenTransactionId__Debug__fmt)(jz_str *out, const jazz_model_OpenTransactionId *id) {
    jz_str_push_cstr(out, "OpenTransactionId(");
    id_display(out, id->b);
    jz_str_push_char(out, ')');
}
jz_err T(OpenTransactionId__FromStr__from_str)(jz_slice raw, jazz_model_OpenTransactionId *out) {
    return id_from_str(raw, "open transaction id", out->b);
}
void T(OpenTransactionId__Serialize__serialize)(jz_pc_w *w, const jazz_model_OpenTransactionId *id) { jz_pc_raw(w, id->b, 16); }
jz_err T(OpenTransactionId__Deserialize__deserialize)(jz_pc_r *r, jazz_model_OpenTransactionId *out) { return jz_pc_r_raw(r, out->b, 16); }
void T(OpenTransactionId__Serialize__serialize_json)(jz_json_w *w, const jazz_model_OpenTransactionId *id) { id_serialize_json(w, id->b); }
jz_err T(OpenTransactionId__Deserialize__deserialize_json)(const jz_json *j, jazz_model_OpenTransactionId *out) {
    return id_deserialize_json(j, "open transaction id", out->b);
}

/* ---------------------------------------------------------------- TransactionId */
void T(TransactionId__Display__fmt)(jz_str *out, const jazz_model_TransactionId *id) { id_display(out, id->b); }
void T(TransactionId__Debug__fmt)(jz_str *out, const jazz_model_TransactionId *id) {
    jz_str_push_cstr(out, "TransactionId(");
    id_display(out, id->b);
    jz_str_push_char(out, ')');
}
jz_err T(TransactionId__FromStr__from_str)(jz_slice raw, jazz_model_TransactionId *out) {
    return id_from_str(raw, "transaction id", out->b);
}
void T(TransactionId__Serialize__serialize)(jz_pc_w *w, const jazz_model_TransactionId *id) { jz_pc_raw(w, id->b, 16); }
jz_err T(TransactionId__Deserialize__deserialize)(jz_pc_r *r, jazz_model_TransactionId *out) { return jz_pc_r_raw(r, out->b, 16); }
void T(TransactionId__Serialize__serialize_json)(jz_json_w *w, const jazz_model_TransactionId *id) { id_serialize_json(w, id->b); }
jz_err T(TransactionId__Deserialize__deserialize_json)(const jz_json *j, jazz_model_TransactionId *out) {
    return id_deserialize_json(j, "transaction id", out->b);
}
int T(TransactionId__Ord__cmp)(const void *a, const void *b, void *ctx) {
    (void)ctx;
    return memcmp(a, b, 16);
}

/* blake3::derive_key("jazz committed batch id v1", time_be || node) truncated to 16 bytes.
 * The runtime has no derive_key mode; the context string is absorbed first instead. */
jazz_model_TransactionId T(TransactionId__from_committed_tx)(jazz_model_TxId tx_id) {
    static const char context[] = "jazz committed batch id v1";
    u8 input[24];
    for (u32 i = 0; i < 8; i++) input[i] = (u8)(tx_id.time >> (56 - 8 * i));
    memcpy(input + 8, tx_id.node.b, 16);
    jz_blake3_hasher *h = jz_blake3_new();
    jz_blake3_update(h, (const u8 *)context, sizeof context - 1);
    jz_blake3_update(h, input, sizeof input);
    u8 digest[32];
    jz_blake3_finalize(h, digest);
    jz_blake3_free(h);
    jazz_model_TransactionId id;
    memcpy(id.b, digest, 16);
    return id;
}
