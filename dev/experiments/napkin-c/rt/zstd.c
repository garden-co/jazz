/* zstd.c: zstd frame decompression, standing in for the vendored ruzstd crate, built on the
 * official zstd decompressor (v1.5.6, lib/decompress + needed lib/common, configured for size:
 * see ../third_party/README.md).
 *
 *  - jz_zstd_decompress: whole-frame(s) decode with an output cap (streaming, so frames without
 *    a content-size field work).
 *  - ruzstd::decoding::FrameDecoder / StreamingDecoder: the API jazz_compression::stream and
 *    jazz_compression::decompress_zstd call (declared by those ports in their _ext header);
 *    implemented with ZSTD_DCtx streaming. FrameDecoder keeps a "streaming" buffer of decoded,
 *    not yet read bytes like ruzstd's ring buffer. */
#include "rt_internal.h"
#define ZSTD_STATIC_LINKING_ONLY /* ZSTD_getFrameHeader */
#include "../third_party/zstd/lib/zstd.h"

static jz_err zstd_err(const char *what, size_t code) {
    return jz_err_fmt(JZ_ZSTD_ERR, "%s: %s", what, ZSTD_getErrorName(code));
}

/* decode from `in` into `out` until the input is consumed and the frame(s) end, or until
 * `limit` output bytes exist (stop_at_limit) / are exceeded (error). */
static jz_err zstd_pump(ZSTD_DStream *ds, ZSTD_inBuffer *in, jz_bytes *out, u64 limit, bool stop_at_limit, size_t *last_ret) {
    size_t ret = *last_ret;
    u32 want = 4096;
    while (in->pos < in->size || ret != 0) {
        if (stop_at_limit && out->len >= limit) break;
        if (out->cap == out->len) jz_bytes_reserve(out, want);
        size_t room = out->cap - out->len;
        if (stop_at_limit && room > limit - out->len) room = (size_t)(limit - out->len);
        ZSTD_outBuffer ob = {out->ptr + out->len, room, 0};
        size_t before = in->pos;
        ret = ZSTD_decompressStream(ds, &ob, in);
        if (ZSTD_isError(ret)) return zstd_err("zstd decompression failed", ret);
        out->len += (u32)ob.pos;
        if (!stop_at_limit && out->len > limit)
            return jz_err_fmt(JZ_ZSTD_ERR, "decompressed size exceeds the limit of %llu bytes", (unsigned long long)limit);
        if (ob.pos == 0 && in->pos == before) {
            if (in->pos >= in->size) {
                *last_ret = ret;
                return jz_err_new(JZ_ZSTD_ERR, "zstd decompression failed: unexpected end of frame");
            }
            if (ob.size != 0) return jz_err_new(JZ_ZSTD_ERR, "zstd decompression failed: no progress");
        }
        if (ob.pos == ob.size) want = out->cap < 4096 ? 4096 : out->cap;
    }
    *last_ret = ret;
    return JZ_OK;
}

jz_err jz_zstd_decompress(const u8 *src, u32 n, jz_bytes *out, u32 max_out) {
    unsigned long long hint = ZSTD_getFrameContentSize(src, n);
    if (hint != ZSTD_CONTENTSIZE_UNKNOWN && hint != ZSTD_CONTENTSIZE_ERROR) {
        if (hint > max_out)
            return jz_err_fmt(JZ_ZSTD_ERR, "decompressed size %llu exceeds the limit of %u bytes", hint, max_out);
        jz_bytes_reserve(out, (u32)hint ? (u32)hint : 1);
    }
    ZSTD_DStream *ds = ZSTD_createDStream();
    if (!ds) jz_oom();
    ZSTD_inBuffer in = {src, n, 0};
    size_t ret = 1;
    jz_err e = zstd_pump(ds, &in, out, max_out, false, &ret);
    ZSTD_freeDStream(ds);
    return e;
}

/* ---------- ruzstd::decoding::FrameDecoder ---------- */
typedef struct ruzstd_FrameDecoder {
    ZSTD_DStream *ds;
    jz_bytes buf;      /* decoded bytes not yet handed out by read_streaming */
    u32 read_pos;      /* start of unread bytes in buf */
    u32 max_block;     /* set_max_block_output; 0 = unlimited */
    size_t last_ret;   /* ZSTD_decompressStream hint: 0 when the frame is complete */
    bool initialized;
} ruzstd_FrameDecoder;

ruzstd_FrameDecoder *ruzstd__decoding__frame_decoder__FrameDecoder__default(void) {
    ruzstd_FrameDecoder *d = JZ_NEW(ruzstd_FrameDecoder);
    d->ds = ZSTD_createDStream();
    if (!d->ds) jz_oom();
    d->last_ret = 1;
    return d;
}

void ruzstd__decoding__frame_decoder__FrameDecoder__drop(ruzstd_FrameDecoder *d) {
    if (!d) return;
    ZSTD_freeDStream(d->ds);
    jz_bytes_drop(&d->buf);
    jz_free(d);
}

/* init(source): reset and parse a new frame header */
jz_err ruzstd__decoding__frame_decoder__FrameDecoder__init(ruzstd_FrameDecoder *d, const u8 *p, u32 n) {
    size_t r = ZSTD_DCtx_reset(d->ds, ZSTD_reset_session_only);
    if (ZSTD_isError(r)) return zstd_err("failed to reset frame decoder", r);
    ZSTD_frameHeader fh;
    r = ZSTD_getFrameHeader(&fh, p, n);
    if (ZSTD_isError(r)) return zstd_err("failed to read frame header", r);
    if (r > 0) return jz_err_fmt(JZ_ZSTD_ERR, "failed to read frame header: need %u more bytes", (u32)r);
    if (fh.frameType != ZSTD_frame) return jz_err_new(JZ_ZSTD_ERR, "failed to read frame header: skippable frame");
    d->buf.len = 0;
    d->read_pos = 0;
    d->last_ret = 1;
    ZSTD_inBuffer in = {p, n, 0};
    jz_err e = zstd_pump(d->ds, &in, &d->buf, UINT32_MAX, false, &d->last_ret);
    if (e.code && in.pos < in.size) return e;
    jz_err_drop(&e); /* header-only input naturally ends "mid-frame" */
    d->initialized = true;
    return JZ_OK;
}

jz_err ruzstd__decoding__frame_decoder__FrameDecoder__set_max_block_output(ruzstd_FrameDecoder *d, u32 limit) {
    if (!d->initialized) return jz_err_new(JZ_ZSTD_ERR, "No frame header was provided to the decoder. Call init() first");
    d->max_block = limit;
    return JZ_OK;
}

/* decode_blocks(source, UptoBlocks(n)): the caller hands over exactly the bytes of the next block(s) */
jz_err ruzstd__decoding__frame_decoder__FrameDecoder__decode_blocks_upto(ruzstd_FrameDecoder *d, const u8 *p, u32 n, u32 blocks) {
    if (!d->initialized) return jz_err_new(JZ_ZSTD_ERR, "No frame header was provided to the decoder. Call init() first");
    if (d->last_ret == 0) return jz_err_new(JZ_ZSTD_ERR, "The frame is already finished");
    /* compact the streaming buffer before appending */
    if (d->read_pos) {
        memmove(d->buf.ptr, d->buf.ptr + d->read_pos, d->buf.len - d->read_pos);
        d->buf.len -= d->read_pos;
        d->read_pos = 0;
    }
    u32 before = d->buf.len;
    ZSTD_inBuffer in = {p, n, 0};
    jz_err e = zstd_pump(d->ds, &in, &d->buf, UINT32_MAX, false, &d->last_ret);
    if (e.code) {
        if (in.pos < in.size) return e;
        jz_err_drop(&e); /* consumed the whole block, frame continues in the next packet */
    }
    u32 produced = d->buf.len - before;
    if (d->max_block && produced > d->max_block * (blocks ? blocks : 1))
        return jz_err_fmt(JZ_ZSTD_ERR, "Block output of %u bytes exceeds the configured maximum of %u bytes", produced, d->max_block);
    return JZ_OK;
}

u32 ruzstd__decoding__frame_decoder__FrameDecoder__read_streaming(ruzstd_FrameDecoder *d, u8 *out, u32 n) {
    u32 avail = d->buf.len - d->read_pos;
    u32 k = avail < n ? avail : n;
    memcpy(out, d->buf.ptr + d->read_pos, k);
    d->read_pos += k;
    if (d->read_pos == d->buf.len) d->read_pos = d->buf.len = 0;
    return k;
}

u32 ruzstd__decoding__frame_decoder__FrameDecoder__streaming_available(const ruzstd_FrameDecoder *d) {
    return d->buf.len - d->read_pos;
}

void ruzstd__decoding__errors__Display__fmt(jz_str *out, const jz_err *e) {
    if (e->msg) jz_str_push(out, e->msg->ptr, e->msg->len);
    else jz_str_appendf(out, "zstd error %u", e->code);
}

/* ---------- ruzstd::decoding::StreamingDecoder<&[u8]> + Read::take(limit).read_to_end ---------- */
typedef struct ruzstd_StreamingDecoder {
    ZSTD_DStream *ds;
    ZSTD_inBuffer in;
    size_t last_ret;
} ruzstd_StreamingDecoder;

jz_err ruzstd__decoding__streaming_decoder__StreamingDecoder__new(const u8 *p, u32 n, ruzstd_StreamingDecoder **out) {
    ZSTD_frameHeader fh;
    size_t r = ZSTD_getFrameHeader(&fh, p, n);
    if (ZSTD_isError(r)) return zstd_err("failed to read frame header", r);
    if (r > 0) return jz_err_new(JZ_ZSTD_ERR, "failed to read frame header: not enough bytes");
    ruzstd_StreamingDecoder *d = JZ_NEW(ruzstd_StreamingDecoder);
    d->ds = ZSTD_createDStream();
    if (!d->ds) jz_oom();
    d->in.src = p;
    d->in.size = n;
    d->in.pos = 0;
    d->last_ret = 1;
    *out = d;
    return JZ_OK;
}

jz_err ruzstd__decoding__streaming_decoder__StreamingDecoder__read_to_end_take(ruzstd_StreamingDecoder *d, u64 limit, jz_bytes *out) {
    if (limit > UINT32_MAX) limit = UINT32_MAX;
    return zstd_pump(d->ds, &d->in, out, limit, true, &d->last_ret);
}

void ruzstd__decoding__streaming_decoder__StreamingDecoder__drop(ruzstd_StreamingDecoder *d) {
    if (!d) return;
    ZSTD_freeDStream(d->ds);
    jz_free(d);
}
