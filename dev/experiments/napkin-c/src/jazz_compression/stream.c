/* crates/jazz-compression/src/stream.rs (browser cfg: feature "ruzstd" only;
 * the lz4 / native zstd arms are cfg'd out exactly as in the shipped build). */
#include "stream.h"

#define SERR(msg) jz_err_new(JAZZ_COMPRESSION_ERR, (msg))

/* fn err(error: impl Display) -> String { format!("channel codec: {error}") } */
static jz_err jazz_compression__stream__err(jz_err e) {
    jz_str s = {0};
    ruzstd__decoding__errors__Display__fmt(&s, &e);
    jz_err r = jz_err_fmt(JAZZ_COMPRESSION_ERR, "channel codec: %.*s", (int)s.len, s.ptr);
    jz_str_drop(&s);
    jz_err_drop(&e);
    return r;
}

/* ---------------- StreamEncoder ---------------- */
jz_err jazz_compression__stream__StreamEncoder__new(jazz_compression_Codec codec, jazz_compression_StreamEncoder *out) {
    (void)codec;
    u32 inner = 0; /* every arm is Encoder::Unavailable without lz4/zstd */
    if (inner == 0) return SERR("channel encoder feature is not compiled in");
    out->inner = inner;
    out->ending = out->ended = out->failed = false;
    return JZ_OK;
}

static jz_err jazz_compression__stream__StreamEncoder__encode_inner(jazz_compression_StreamEncoder *e, jz_slice input, u8 *output,
                                                                    u32 output_len, jazz_compression_Progress *out) {
    (void)e; (void)input; (void)output;
    if (output_len == 0) { memset(out, 0, sizeof *out); return JZ_OK; }
    return SERR("channel encoder unavailable");
}

jz_err jazz_compression__stream__StreamEncoder__encode(jazz_compression_StreamEncoder *e, jz_slice input, u8 *output, u32 output_len,
                                                       jazz_compression_Progress *out) {
    if (e->failed || e->ending || e->ended) return SERR("channel encoder is closed");
    jz_err r = jazz_compression__stream__StreamEncoder__encode_inner(e, input, output, output_len, out);
    if (r.code) e->failed = true;
    return r;
}

static jz_err jazz_compression__stream__StreamEncoder__flush_inner(jazz_compression_StreamEncoder *e, u8 *output, u32 output_len,
                                                                   bool finish, jazz_compression_Progress *out) {
    (void)e; (void)output; (void)output_len; (void)finish; (void)out;
    return SERR("channel encoder unavailable");
}

static jz_err jazz_compression__stream__StreamEncoder__flush_or_finish(jazz_compression_StreamEncoder *e, u8 *output, u32 output_len,
                                                                       bool finish, jazz_compression_Progress *out) {
    if (e->failed || e->ended || (e->ending && !finish)) return SERR("channel encoder is closed");
    if (output_len == 0) { memset(out, 0, sizeof *out); return JZ_OK; }
    jz_err r = jazz_compression__stream__StreamEncoder__flush_inner(e, output, output_len, finish, out);
    if (r.code) e->failed = true;
    if (!r.code && finish && out->finished) e->ended = true;
    return r;
}

jz_err jazz_compression__stream__StreamEncoder__flush(jazz_compression_StreamEncoder *e, u8 *output, u32 output_len, jazz_compression_Progress *out) {
    return jazz_compression__stream__StreamEncoder__flush_or_finish(e, output, output_len, false, out);
}
jz_err jazz_compression__stream__StreamEncoder__finish(jazz_compression_StreamEncoder *e, u8 *output, u32 output_len, jazz_compression_Progress *out) {
    return jazz_compression__stream__StreamEncoder__flush_or_finish(e, output, output_len, true, out);
}
void jazz_compression__stream__StreamEncoder__drop(jazz_compression_StreamEncoder *e) { (void)e; }

/* ---------------- StreamDecoder ---------------- */
jz_err jazz_compression__stream__StreamDecoder__new(jazz_compression_Codec codec, jazz_compression_StreamDecoder *out) {
    memset(out, 0, sizeof *out);
    switch (codec) {
    case JAZZ_CODEC_ZSTD:
        out->inner_tag = JAZZ_DECODER_RUZSTD;
        out->ruzstd = ruzstd__decoding__frame_decoder__FrameDecoder__default();
        break;
    default:
        out->inner_tag = JAZZ_DECODER_UNAVAILABLE;
        break;
    }
    if (out->inner_tag == JAZZ_DECODER_UNAVAILABLE) return SERR("channel decoder feature is not compiled in");
    out->codec = codec;
    jz_vec_reserve(&out->packet, JAZZ_COMPRESSION_MAX_STREAM_PENDING_BYTES, 1);
    out->target = codec == JAZZ_CODEC_LZ4 ? 7 : 6;
    out->packet_position = 0;
    out->block_limit = JAZZ_COMPRESSION_MAX_STREAM_CHUNK_BYTES;
    out->header = true;
    return JZ_OK;
}

void jazz_compression__stream__StreamDecoder__drop(jazz_compression_StreamDecoder *d) {
    if (d->inner_tag == JAZZ_DECODER_RUZSTD) ruzstd__decoding__frame_decoder__FrameDecoder__drop(d->ruzstd);
    jz_bytes_drop(&d->packet);
}

static u32 jazz_compression__stream__StreamDecoder__block_header_len(const jazz_compression_StreamDecoder *d) {
    switch (d->codec) {
    case JAZZ_CODEC_LZ4: return 4;
    case JAZZ_CODEC_ZSTD: return 3;
    }
    JZ_UNREACHABLE();
}

static jz_err jazz_compression__stream__StreamDecoder__validate_header(jazz_compression_StreamDecoder *d) {
    static const u8 LZ4_HEADER[7] = { 0x04, 0x22, 0x4d, 0x18, 0x40, 0x40, 0xc0 };
    static const u8 ZSTD_MAGIC[5] = { 0x28, 0xb5, 0x2f, 0xfd, 0 };
    bool valid;
    switch (d->codec) {
    case JAZZ_CODEC_LZ4: valid = d->packet.len == 7 && memcmp(d->packet.ptr, LZ4_HEADER, 7) == 0; break;
    case JAZZ_CODEC_ZSTD: valid = memcmp(d->packet.ptr, ZSTD_MAGIC, 5) == 0 && d->packet.ptr[5] <= 0x30; break;
    default: valid = false;
    }
    if (!valid) return SERR("unsupported channel codec header/window profile");
    if (d->codec == JAZZ_CODEC_ZSTD) {
        u8 descriptor = d->packet.ptr[5];
        u32 base = 1u << (10 + (descriptor >> 3));
        d->block_limit = base + (base / 8) * (u32)(descriptor & 7);
    }
    return JZ_OK;
}

static jz_err jazz_compression__stream__StreamDecoder__block_size(const jazz_compression_StreamDecoder *d, u32 *stored_out, bool *last_out) {
    u32 size, stored; bool last;
    const u8 *p = d->packet.ptr;
    switch (d->codec) {
    case JAZZ_CODEC_LZ4: {
        u32 value = (u32)p[0] | ((u32)p[1] << 8) | ((u32)p[2] << 16) | ((u32)p[3] << 24);
        size = value & 0x7fffffffu;
        if (value != 0 && size == 0) return SERR("empty nonterminal lz4 block");
        stored = size; last = value == 0;
        break;
    }
    case JAZZ_CODEC_ZSTD: {
        u32 value = (u32)p[0] | ((u32)p[1] << 8) | ((u32)p[2] << 16);
        size = value >> 3;
        u32 kind = (value >> 1) & 3;
        if (kind == 3) return SERR("reserved zstd block type");
        stored = kind == 1 ? 1 : size;
        last = (value & 1) != 0;
        break;
    }
    default: JZ_UNREACHABLE();
    }
    if (size > d->block_limit) return SERR("channel codec block exceeds 64KiB/profile window");
    *stored_out = stored; *last_out = last;
    return JZ_OK;
}

static jz_err jazz_compression__stream__StreamDecoder__process_packet(jazz_compression_StreamDecoder *d, u8 *output, u32 output_len,
                                                                      u32 *consumed, u32 *written, bool *drained) {
    const u8 *input = d->packet.ptr + d->packet_position;
    u32 input_len = d->packet.len - d->packet_position;
    switch (d->inner_tag) {
    case JAZZ_DECODER_UNAVAILABLE: return SERR("channel decoder unavailable");
    case JAZZ_DECODER_RUZSTD: {
        if (d->packet_position == 0) {
            if (d->header) {
                jz_err e = ruzstd__decoding__frame_decoder__FrameDecoder__init(d->ruzstd, input, input_len);
                if (e.code) return jazz_compression__stream__err(e);
                e = ruzstd__decoding__frame_decoder__FrameDecoder__set_max_block_output(d->ruzstd, d->block_limit);
                if (e.code) return jazz_compression__stream__err(e);
            } else {
                jz_err e = ruzstd__decoding__frame_decoder__FrameDecoder__decode_blocks_upto(d->ruzstd, input, input_len, 1);
                if (e.code) return jazz_compression__stream__err(e);
            }
        }
        *written = ruzstd__decoding__frame_decoder__FrameDecoder__read_streaming(d->ruzstd, output, output_len);
        *consumed = input_len;
        *drained = ruzstd__decoding__frame_decoder__FrameDecoder__streaming_available(d->ruzstd) == 0;
        return JZ_OK;
    }
    }
    JZ_UNREACHABLE();
}

static jz_err jazz_compression__stream__StreamDecoder__decode_inner(jazz_compression_StreamDecoder *d, jz_slice input, u8 *output,
                                                                    u32 output_len, jazz_compression_Progress *progress) {
    memset(progress, 0, sizeof *progress);
    if (d->ended) {
        if (input.len != 0) return SERR("bytes after channel codec end");
        progress->finished = true;
        return JZ_OK;
    }
    if (output_len == 0) return JZ_OK;
    for (;;) {
        if (!d->packet_ready) {
            u32 want = d->target - d->packet.len, have = input.len - progress->consumed;
            u32 amount = want < have ? want : have;
            jz_bytes_push(&d->packet, input.ptr + progress->consumed, amount);
            progress->consumed += amount;
            if (d->packet.len < d->target) return JZ_OK;
            if (d->header) {
                JZ_TRY(jazz_compression__stream__StreamDecoder__validate_header(d));
                d->packet_ready = true;
            } else {
                u32 header_len = jazz_compression__stream__StreamDecoder__block_header_len(d);
                if (d->target == header_len) {
                    u32 body_len; bool last;
                    JZ_TRY(jazz_compression__stream__StreamDecoder__block_size(d, &body_len, &last));
                    d->last_block = last;
                    d->target += body_len;
                    if (body_len != 0) continue;
                }
                d->packet_ready = true;
            }
        }
        u32 consumed = 0, written = 0; bool drained = false;
        JZ_TRY(jazz_compression__stream__StreamDecoder__process_packet(d, output + progress->written, output_len - progress->written,
                                                                      &consumed, &written, &drained));
        d->packet_position += consumed;
        progress->written += written;
        if (drained) {
            bool last = !d->header && d->last_block;
            d->header = false;
            d->packet.len = 0;
            d->packet_position = 0;
            d->packet_ready = false;
            d->target = jazz_compression__stream__StreamDecoder__block_header_len(d);
            if (last) {
                d->ended = true;
                progress->finished = true;
                return JZ_OK;
            }
        }
        if (progress->written == output_len) return JZ_OK;
        if (!drained && consumed == 0 && written == 0) return SERR("channel decoder stalled on a complete block");
    }
}

jz_err jazz_compression__stream__StreamDecoder__decode(jazz_compression_StreamDecoder *d, jz_slice input, u8 *output, u32 output_len,
                                                       jazz_compression_Progress *out) {
    if (d->failed) return SERR("channel decoder is poisoned");
    jz_err r = jazz_compression__stream__StreamDecoder__decode_inner(d, input, output, output_len, out);
    if (r.code) d->failed = true;
    return r;
}

jz_err jazz_compression__stream__StreamDecoder__finish(const jazz_compression_StreamDecoder *d) {
    if (!d->failed && d->ended) return JZ_OK;
    return SERR("truncated or undrained channel codec stream");
}
