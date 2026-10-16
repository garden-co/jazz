/* crates/jazz-compression/src/stream.rs (browser cfg: feature "ruzstd" only). */
#ifndef JAZZ_COMPRESSION_STREAM_H
#define JAZZ_COMPRESSION_STREAM_H
#include "jz_rt.h"
#include "jazz_protocol/_ext_protocol-types.h"

enum { JAZZ_COMPRESSION_ERR = 0x0300 }; /* Result<_, String> */
#define JAZZ_COMPRESSION_MAX_STREAM_CHUNK_BYTES (64u * 1024u)
#define JAZZ_COMPRESSION_MAX_STREAM_PENDING_BYTES (JAZZ_COMPRESSION_MAX_STREAM_CHUNK_BYTES + 11u)

typedef enum { JAZZ_CODEC_LZ4 = 0, JAZZ_CODEC_ZSTD = 1 } jazz_compression_Codec;
typedef struct jazz_compression_Progress { u32 consumed; u32 written; bool finished; } jazz_compression_Progress;

/* enum Encoder { Unavailable } (no lz4 / zstd encoder compiled in) */
typedef struct jazz_compression_StreamEncoder { u32 inner; bool ending, ended, failed; } jazz_compression_StreamEncoder;
/* enum Decoder { Unavailable, Ruzstd(Box<FrameDecoder>) } */
enum { JAZZ_DECODER_UNAVAILABLE = 0, JAZZ_DECODER_RUZSTD = 1 };
typedef struct jazz_compression_StreamDecoder {
    jazz_compression_Codec codec;
    u32 inner_tag; ruzstd_FrameDecoder *ruzstd;
    jz_bytes packet;
    u32 target;
    u32 packet_position;
    u32 block_limit;
    bool header, packet_ready, last_block, ended, failed;
} jazz_compression_StreamDecoder;

jz_err jazz_compression__stream__StreamEncoder__new(jazz_compression_Codec codec, jazz_compression_StreamEncoder *out);
jz_err jazz_compression__stream__StreamEncoder__encode(jazz_compression_StreamEncoder *e, jz_slice input, u8 *output, u32 output_len, jazz_compression_Progress *out);
jz_err jazz_compression__stream__StreamEncoder__flush(jazz_compression_StreamEncoder *e, u8 *output, u32 output_len, jazz_compression_Progress *out);
jz_err jazz_compression__stream__StreamEncoder__finish(jazz_compression_StreamEncoder *e, u8 *output, u32 output_len, jazz_compression_Progress *out);
void jazz_compression__stream__StreamEncoder__drop(jazz_compression_StreamEncoder *e);
jz_err jazz_compression__stream__StreamDecoder__new(jazz_compression_Codec codec, jazz_compression_StreamDecoder *out);
jz_err jazz_compression__stream__StreamDecoder__decode(jazz_compression_StreamDecoder *d, jz_slice input, u8 *output, u32 output_len, jazz_compression_Progress *out);
jz_err jazz_compression__stream__StreamDecoder__finish(const jazz_compression_StreamDecoder *d);
void jazz_compression__stream__StreamDecoder__drop(jazz_compression_StreamDecoder *d);
#endif
