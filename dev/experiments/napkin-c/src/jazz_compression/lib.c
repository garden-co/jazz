/* crates/jazz-compression/src/lib.rs (browser cfg: feature "ruzstd" only).
 * compress_lz4 / decompress_lz4 / compress_zstd / decompress_zstd have no
 * caller in the shipped client (only jazz_protocol::wire::stream_backend uses
 * this crate, through `stream`), so they are absent from the browser build;
 * they are ported anyway for completeness of the module and LTO drops them. */
#include "stream.h"

jz_err jazz_compression__compress_lz4(jz_slice payload, jz_bytes *out) {
    (void)payload; (void)out;
    return jz_err_new(JAZZ_COMPRESSION_ERR, "lz4 transport compression feature is not compiled in");
}
jz_err jazz_compression__decompress_lz4(jz_slice payload, u32 max_decoded_len, jz_bytes *out) {
    (void)payload; (void)max_decoded_len; (void)out;
    return jz_err_new(JAZZ_COMPRESSION_ERR, "lz4 transport compression feature is not compiled in");
}
jz_err jazz_compression__compress_zstd(jz_slice payload, jz_bytes *out) {
    (void)payload; (void)out;
    return jz_err_new(JAZZ_COMPRESSION_ERR, "zstd transport compression feature is not compiled in");
}

static jz_err jazz_compression__validate_decoded_len(u32 len, u32 max_decoded_len) {
    if (len > max_decoded_len)
        return jz_err_fmt(JAZZ_COMPRESSION_ERR, "logical message payload size %u exceeds max %u", len, max_decoded_len);
    return JZ_OK;
}

static jz_err wrap(const char *ctx, jz_err e) {
    jz_err r = jz_err_fmt(JAZZ_COMPRESSION_ERR, "%s: %.*s", ctx, e.msg ? (int)e.msg->len : 0, e.msg ? e.msg->ptr : "");
    jz_err_drop(&e);
    return r;
}

jz_err jazz_compression__decompress_zstd(jz_slice payload, u32 max_decoded_len, jz_bytes *out) {
    ruzstd_StreamingDecoder *decoder;
    jz_err e = ruzstd__decoding__streaming_decoder__StreamingDecoder__new(payload.ptr, payload.len, &decoder);
    if (e.code) return wrap("failed to initialize ruzstd payload", e);
    jz_bytes output = {0};
    u64 limit = (u64)max_decoded_len + 1; /* saturating_add(1) on usize */
    e = ruzstd__decoding__streaming_decoder__StreamingDecoder__read_to_end_take(decoder, limit, &output);
    ruzstd__decoding__streaming_decoder__StreamingDecoder__drop(decoder);
    if (e.code) { jz_bytes_drop(&output); return wrap("failed to decompress ruzstd payload", e); }
    e = jazz_compression__validate_decoded_len(output.len, max_decoded_len);
    if (e.code) { jz_bytes_drop(&output); return e; }
    *out = output;
    return JZ_OK;
}
