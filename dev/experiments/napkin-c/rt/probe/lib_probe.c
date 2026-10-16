/* lib_probe.c: size-measurement harness for one vendored library in isolation (not part of the
 * port). Compiles to nothing unless one of -DJZ_LIB_PROBE_EMPTY, _MONOCYPHER, _SHA512, _BLAKE3,
 * _SHA, _ZSTD is given; link it with --roots "third_party/<lib> rt/probe". */
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#define E(n) __attribute__((export_name(n)))
#ifdef JZ_LIB_PROBE_EMPTY
E("p_empty") uint32_t p_empty(uint32_t x) { return x + 1; }
#endif
#ifdef JZ_LIB_PROBE_MONOCYPHER
#include "../../third_party/monocypher/monocypher-ed25519.h"
E("ed_keypair") void ed_keypair(uint8_t *sk, uint8_t *pk, uint8_t *seed) { crypto_ed25519_key_pair(sk, pk, seed); }
E("ed_sign") void ed_sign(uint8_t *sig, const uint8_t *sk, const uint8_t *m, size_t n) { crypto_ed25519_sign(sig, sk, m, n); }
E("ed_check") int ed_check(const uint8_t *sig, const uint8_t *pk, const uint8_t *m, size_t n) { return crypto_ed25519_check(sig, pk, m, n); }
#endif
#ifdef JZ_LIB_PROBE_SHA512
#include "../../third_party/monocypher/monocypher-ed25519.h"
E("sha512") void sha512(uint8_t *h, const uint8_t *m, size_t n) { crypto_sha512(h, m, n); }
#endif
#ifdef JZ_LIB_PROBE_BLAKE3
#include "../../third_party/blake3/blake3.h"
E("b3") void b3(blake3_hasher *h, const uint8_t *m, size_t n, uint8_t *out) {
    blake3_hasher_init(h); blake3_hasher_update(h, m, n); blake3_hasher_finalize(h, out, 32); }
#endif
#ifdef JZ_LIB_PROBE_SHA
#include "../../third_party/crypto-algorithms/sha256.h"
#include "../../third_party/crypto-algorithms/sha1.h"
E("s256") void s256(const BYTE *m, size_t n, BYTE *out) { SHA256_CTX c; sha256_init(&c); sha256_update(&c, m, n); sha256_final(&c, out); }
E("s1") void s1(const BYTE *m, size_t n, BYTE *out) { SHA1_CTX c; sha1_init(&c); sha1_update(&c, m, n); sha1_final(&c, out); }
#endif
#ifdef JZ_LIB_PROBE_ZSTD
#define ZSTD_STATIC_LINKING_ONLY
#include "../../third_party/zstd/lib/zstd.h"
E("z_new") ZSTD_DStream *z_new(void) { return ZSTD_createDStream(); }
E("z_free") void z_free(ZSTD_DStream *d) { ZSTD_freeDStream(d); }
E("z_reset") size_t z_reset(ZSTD_DStream *d) { return ZSTD_DCtx_reset(d, ZSTD_reset_session_only); }
E("z_hdr") size_t z_hdr(ZSTD_frameHeader *h, const void *p, size_t n) { return ZSTD_getFrameHeader(h, p, n); }
E("z_csize") unsigned long long z_csize(const void *p, size_t n) { return ZSTD_getFrameContentSize(p, n); }
E("z_stream") size_t z_stream(ZSTD_DStream *d, ZSTD_outBuffer *o, ZSTD_inBuffer *i) { return ZSTD_decompressStream(d, o, i); }
E("z_err") const char *z_err(size_t c) { return ZSTD_isError(c) ? ZSTD_getErrorName(c) : 0; }
#endif
