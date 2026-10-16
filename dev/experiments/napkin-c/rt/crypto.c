/* crypto.c: hashes and signatures, wrapping vendored C libraries (see ../third_party/README.md):
 *   sha512 + ed25519 : Monocypher 4.0.2 (monocypher.c + optional/monocypher-ed25519.c)
 *   blake3           : official BLAKE3 C implementation 1.5.4, portable backend only
 *   sha256, sha1     : Brad Conte's crypto-algorithms (public domain)
 * These stand in for curve25519-dalek/ed25519-dalek, blake3, sha2 and sha1_smol. */
#include "rt_internal.h"
#include "../third_party/monocypher/monocypher.h"
#include "../third_party/monocypher/monocypher-ed25519.h"
#include "../third_party/blake3/blake3.h"
#include "../third_party/crypto-algorithms/sha256.h"
#include "../third_party/crypto-algorithms/sha1.h"

void jz_sha256(const u8 *p, usize n, u8 out[32]) {
    SHA256_CTX c;
    sha256_init(&c);
    sha256_update(&c, p, n);
    sha256_final(&c, out);
}

void jz_sha512(const u8 *p, usize n, u8 out[64]) { crypto_sha512(out, p, n); }

void jz_sha1(const u8 *p, usize n, u8 out[20]) {
    SHA1_CTX c;
    sha1_init(&c);
    sha1_update(&c, p, n);
    sha1_final(&c, out);
}

void jz_blake3(const u8 *p, usize n, u8 out[32]) {
    blake3_hasher h;
    blake3_hasher_init(&h);
    blake3_hasher_update(&h, p, n);
    blake3_hasher_finalize(&h, out, 32);
}

struct jz_blake3_hasher { blake3_hasher h; };

jz_blake3_hasher *jz_blake3_new(void) {
    jz_blake3_hasher *h = (jz_blake3_hasher *)jz_alloc(sizeof(*h));
    blake3_hasher_init(&h->h);
    return h;
}

void jz_blake3_update(jz_blake3_hasher *h, const u8 *p, usize n) { blake3_hasher_update(&h->h, p, n); }

void jz_blake3_finalize(jz_blake3_hasher *h, u8 out[32]) { blake3_hasher_finalize(&h->h, out, 32); }

void jz_blake3_free(jz_blake3_hasher *h) { jz_free(h); }

/* ed25519-dalek SigningKey::from_bytes(seed).verifying_key() */
void jz_ed25519_public_key(const u8 seed[32], u8 pk[32]) {
    u8 s[32], sk[64];
    memcpy(s, seed, 32); /* key_pair wipes the seed it is given */
    crypto_ed25519_key_pair(sk, pk, s);
    crypto_wipe(sk, sizeof(sk));
}

void jz_ed25519_sign(const u8 seed[32], const u8 pk[32], const u8 *msg, usize n, u8 sig[64]) {
    u8 sk[64];
    memcpy(sk, seed, 32);
    memcpy(sk + 32, pk, 32); /* Monocypher's secret key is seed || public key */
    crypto_ed25519_sign(sig, sk, msg, n);
    crypto_wipe(sk, sizeof(sk));
}

bool jz_ed25519_verify(const u8 pk[32], const u8 *msg, usize n, const u8 sig[64]) {
    return crypto_ed25519_check(sig, pk, msg, n) == 0;
}
