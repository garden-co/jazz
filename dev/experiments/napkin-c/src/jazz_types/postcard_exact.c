/* crates/jazz/layers/types/src/postcard_exact.rs
 * Rust instantiates decode_postcard_exact<T> per T (and serializes once more
 * through the CanonicalBytesMatch flavor); in C there is one copy taking a
 * codec vtable. */
#include "jazz_types.h"

static jz_err bad_encoding(void) {
    return jz_err_new(JAZZ_TYPES_ERR_PC_BAD_ENCODING, "Found an invalid encoding (DeserializeBadEncoding)");
}

/* CanonicalBytesMatch flavor: consume `expected` instead of writing bytes.
 * try_push / try_extend fail (SerializeBufferFull) at the first differing byte. */
typedef struct jazz_types_CanonicalBytesMatch { const u8 *expected; u32 len; bool failed; } jazz_types_CanonicalBytesMatch;

static bool jazz_types__postcard_exact__CanonicalBytesMatch__try_extend(jazz_types_CanonicalBytesMatch *m, const u8 *bytes, u32 n) {
    if (n > m->len || memcmp(m->expected, bytes, n) != 0) { m->failed = true; return false; }
    m->expected += n; m->len -= n;
    return true;
}
static bool jazz_types__postcard_exact__CanonicalBytesMatch__try_push(jazz_types_CanonicalBytesMatch *m, u8 byte) {
    return jazz_types__postcard_exact__CanonicalBytesMatch__try_extend(m, &byte, 1);
}

bool jazz_types__postcard_exact__encodes_exactly(const jazz_types_PcCodec *codec, const void *value, jz_slice expected) {
    /* The shared postcard writer emits into a buffer; the flavor then consumes it. */
    jz_bytes buf = {0};
    jz_pc_w w = { &buf };
    codec->encode(&w, value);
    jazz_types_CanonicalBytesMatch m = { expected.ptr, expected.len, false };
    bool ok = jazz_types__postcard_exact__CanonicalBytesMatch__try_extend(&m, buf.ptr, buf.len);
    jz_bytes_drop(&buf);
    /* finalize: Ok(remaining expected) -> remaining.is_empty() */
    return ok && !m.failed && m.len == 0;
}

jz_err jazz_types__postcard_exact__decode_postcard_exact(const jazz_types_PcCodec *codec, jz_slice bytes, void *out) {
    jz_pc_r r = { bytes.ptr, bytes.ptr + bytes.len, 0 };
    JZ_TRY(codec->decode(&r, out)); /* take_from_bytes */
    bool remainder_empty = r.p == r.end;
    if (!remainder_empty || !jazz_types__postcard_exact__encodes_exactly(codec, out, bytes)) {
        codec->drop(out);
        return bad_encoding();
    }
    return JZ_OK;
}
