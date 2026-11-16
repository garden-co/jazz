/* Independent protocol fixture generator; it shares no code with jazz-e2ee.
 * Build it against libsodium 1.0.22 exactly as the crate's dev-dependency
 * `libsodium-sys-stable = "=1.24.0"` bundles and builds it (main() checks the
 * version), for example from that crate's build output:
 *   OUT=target/debug/build/libsodium-sys-stable-<hash>/out/installed
 *   cc -std=c99 -O1 -I"$OUT/include" e2ee-vectors.c "$OUT/lib/libsodium.a" -o e2ee-vectors
 * Every field is deterministic: the HPKE seal uses fixed input keying
 * material and the stream a fixed nonce prefix. Test keys only, never
 * production keys.
 */
#ifdef NDEBUG
#error "This fixture generator requires assertions enabled"
#endif
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <sodium.h>

static size_t header(unsigned char *out, const char *id) {
    size_t n = strlen(id);
    memcpy(out, "JE2E", 4);
    out[4] = 1; out[5] = (unsigned char)n;
    memcpy(out + 6, id, n);
    memset(out + 6 + n, 0, 3); out[9 + n] = 1;
    return 10 + n;
}
static void field(const char *name, const unsigned char *value, size_t n) {
    static int first = 1;
    printf("%s\"%s\":\"", first ? "" : ",", name); first = 0;
    for (size_t i = 0; i < n; i++) printf("%02x", value[i]);
    printf("\"");
}
/* HPKE (RFC 9180) base mode, single-shot, built from libsodium primitives:
 * DHKEM(X25519, HKDF-SHA256), HKDF-SHA256, ChaCha20Poly1305. */
static const unsigned char kem_suite[5] = { 'K', 'E', 'M', 0x00, 0x20 };
static const unsigned char hpke_suite[10] = { 'H', 'P', 'K', 'E', 0x00, 0x20, 0x00, 0x01, 0x00, 0x03 };
static void labeled_extract(unsigned char prk[32], const unsigned char *suite, size_t suite_len,
                            const unsigned char *salt, size_t salt_len, const char *label,
                            const unsigned char *ikm, size_t ikm_len) {
    unsigned char buffer[256];
    size_t n = 0, l = strlen(label);
    assert(7 + suite_len + l + ikm_len <= sizeof buffer);
    memcpy(buffer, "HPKE-v1", 7); n += 7;
    memcpy(buffer + n, suite, suite_len); n += suite_len;
    memcpy(buffer + n, label, l); n += l;
    if (ikm_len) { memcpy(buffer + n, ikm, ikm_len); n += ikm_len; }
    assert(crypto_kdf_hkdf_sha256_extract(prk, salt, salt_len, buffer, n) == 0);
}
static void labeled_expand(unsigned char *out, size_t out_len, const unsigned char prk[32],
                           const unsigned char *suite, size_t suite_len, const char *label,
                           const unsigned char *info, size_t info_len) {
    unsigned char buffer[256];
    size_t n = 0, l = strlen(label);
    assert(9 + suite_len + l + info_len <= sizeof buffer);
    buffer[n++] = (unsigned char)(out_len >> 8); buffer[n++] = (unsigned char)out_len;
    memcpy(buffer + n, "HPKE-v1", 7); n += 7;
    memcpy(buffer + n, suite, suite_len); n += suite_len;
    memcpy(buffer + n, label, l); n += l;
    if (info_len) { memcpy(buffer + n, info, info_len); n += info_len; }
    assert(crypto_kdf_hkdf_sha256_expand(out, out_len, (const char *)buffer, n, prk) == 0);
}
static void derive_keypair(unsigned char sk[32], unsigned char pk[32], const unsigned char ikm[32]) {
    unsigned char prk[32];
    labeled_extract(prk, kem_suite, 5, NULL, 0, "dkp_prk", ikm, 32);
    labeled_expand(sk, 32, prk, kem_suite, 5, "sk", NULL, 0);
    assert(crypto_scalarmult_base(pk, sk) == 0);
}
/* Writes enc (32 bytes) then ciphertext and tag; returns the bytes written. */
static size_t hpke_seal(unsigned char *out, const unsigned char pk_r[32], const unsigned char ikm_e[32],
                        const unsigned char *info, size_t info_len, const unsigned char *aad,
                        size_t aad_len, const unsigned char *pt, size_t pt_len) {
    unsigned char sk_e[32], dh[32], kem_context[64], prk[32], shared[32], ksc[65], secret[32];
    unsigned char key[32], base_nonce[12];
    unsigned long long written;
    derive_keypair(sk_e, out, ikm_e);
    assert(crypto_scalarmult(dh, sk_e, pk_r) == 0);
    memcpy(kem_context, out, 32); memcpy(kem_context + 32, pk_r, 32);
    labeled_extract(prk, kem_suite, 5, NULL, 0, "eae_prk", dh, 32);
    labeled_expand(shared, 32, prk, kem_suite, 5, "shared_secret", kem_context, 64);
    ksc[0] = 0;
    labeled_extract(ksc + 1, hpke_suite, 10, NULL, 0, "psk_id_hash", NULL, 0);
    labeled_extract(ksc + 33, hpke_suite, 10, NULL, 0, "info_hash", info, info_len);
    labeled_extract(secret, hpke_suite, 10, shared, 32, "secret", NULL, 0);
    labeled_expand(key, 32, secret, hpke_suite, 10, "key", ksc, 65);
    labeled_expand(base_nonce, 12, secret, hpke_suite, 10, "base_nonce", ksc, 65);
    assert(crypto_aead_chacha20poly1305_ietf_encrypt(out + 32, &written, pt, pt_len, aad, aad_len,
        NULL, base_nonce, key) == 0);
    sodium_memzero(sk_e, sizeof sk_e); sodium_memzero(key, sizeof key);
    return 32 + (size_t)written;
}
static void from_hex(unsigned char *out, const char *hex) {
    for (size_t i = 0; hex[2 * i]; i++) assert(sscanf(hex + 2 * i, "%2hhx", &out[i]) == 1);
}
/* RFC 9180 A.2.1, sequence number 0, checks the construction above. */
static void check_rfc9180(void) {
    unsigned char ikm_e[32], ikm_r[32], sk_r[32], pk_r[32], info[20], pt[29], aad[7], out[128], expected[77];
    from_hex(ikm_e, "909a9b35d3dc4713a5e72a4da274b55d3d3821a37e5d099e74a647db583a904b");
    from_hex(ikm_r, "1ac01f181fdf9f352797655161c58b75c656a6cc2716dcb66372da835542e1df");
    from_hex(info, "4f6465206f6e2061204772656369616e2055726e");
    from_hex(pt, "4265617574792069732074727574682c20747275746820626561757479");
    from_hex(aad, "436f756e742d30");
    from_hex(expected, "1afa08d3dec047a643885163f1180476fa7ddb54c6a8029ea33f95796bf2ac4a"
        "1c5250d8034ec2b784ba2cfd69dbdb8af406cfe3ff938e131f0def8c8b60b4db21993c62ce81883d2dd1b51a28");
    derive_keypair(sk_r, pk_r, ikm_r);
    assert(hpke_seal(out, pk_r, ikm_e, info, sizeof info, aad, sizeof aad, pt, sizeof pt) == sizeof expected);
    assert(memcmp(out, expected, sizeof expected) == 0);
}
int main(void) {
    assert(sodium_init() >= 0);
    assert(strcmp(sodium_version_string(), "1.0.22") == 0);
    unsigned char root[32], key[32], nonce[24], seed[32], ephemeral[32], pk[32], sk[32];
    for (size_t i = 0; i < 32; i++) {
        root[i] = i; key[i] = 32 + i; seed[i] = 42 + i; ephemeral[i] = 96 + i;
    }
    for (size_t i = 0; i < 24; i++) nonce[i] = i;
    check_rfc9180();
    derive_keypair(sk, pk, seed);
    unsigned char context[50] = { 'J', 'E', '2', 'C', 1 };
    const char values[] = "apsitrced";
    for (size_t i = 0; i < 9; i++) { context[5 + i * 5 + 3] = 1; context[5 + i * 5 + 4] = values[i]; }
    unsigned char cell[256], wrap[256], sealed[256], aad[128], input[256], derived[32];
    const unsigned char plaintext[] = "hello";
    const char cell_label[] = "jazz.e2ee.cell-key.v1";
    const char wrap_label[] = "jazz.e2ee.wrap-key.v1";
    const char equality_label[] = "jazz.e2ee.equality-key.v1";
    const char stream_label[] = "jazz.e2ee.stream-key.v1";
    unsigned char labelled[300];
    printf("{");
    field("root", root, 32); field("key", key, 32); field("nonce", nonce, 24);
    field("context", context, sizeof context); field("plaintext", plaintext, 5);
    field("seed", seed, 32); field("ephemeral", ephemeral, 32);
    field("publicKey", pk, 32); field("privateKey", sk, 32);
    size_t h = header(cell, "jazz.sodium.cell");
    memcpy(aad, cell, h); memcpy(aad + h, context, sizeof context);
    size_t a = h + sizeof context;
    memcpy(input, cell_label, sizeof cell_label); memcpy(input + sizeof cell_label, aad, a);
    assert(crypto_generichash(derived, 32, input, sizeof cell_label + a, root, 32) == 0);
    field("cellDerived", derived, 32); field("cellAad", aad, a);
    memcpy(cell + h, nonce, 24);
    assert(crypto_aead_xchacha20poly1305_ietf_encrypt(cell + h + 24, NULL,
        plaintext, 5, aad, a, NULL, nonce, derived) == 0);
    field("cell", cell, h + 24 + 5 + 16);
    h = header(wrap, "jazz.wrap.xchacha20poly1305"); wrap[h++] = 1;
    memcpy(aad, wrap, h); memcpy(aad + h, context, sizeof context);
    a = h + sizeof context;
    memcpy(input, wrap_label, sizeof wrap_label); memcpy(input + sizeof wrap_label, aad, a);
    assert(crypto_generichash(derived, 32, input, sizeof wrap_label + a, root, 32) == 0);
    field("wrapDerived", derived, 32); field("wrapAad", aad, a);
    memcpy(wrap + h, nonce, 24);
    assert(crypto_aead_xchacha20poly1305_ietf_encrypt(wrap + h + 24, NULL,
        key, 32, aad, a, NULL, nonce, derived) == 0);
    field("wrap", wrap, h + 24 + 32 + 16);
    /* HPKE key seal: info is the mechanism header, aad the context. */
    h = header(sealed, "jazz.wrap.hpke-x25519");
    size_t sealed_len = h + hpke_seal(sealed + h, pk, ephemeral, sealed, h, context,
        sizeof context, key, 32);
    assert(sealed_len == h + 80);
    field("sealed", sealed, sealed_len);
    unsigned char equality[128];
    h = header(equality, "jazz.sodium.equality");
    /* Two u32be-length-prefixed fields: mechanism header, then context. */
    memset(input, 0, 4); input[3] = (unsigned char)h;
    memcpy(input + 4, equality, h);
    memset(input + 4 + h, 0, 4); input[7 + h] = sizeof context;
    memcpy(input + 8 + h, context, sizeof context);
    field("equalityContext", input, 8 + h + sizeof context);
    /* The label, its terminating 00 (sizeof includes it), then the framed input. */
    memcpy(labelled, equality_label, sizeof equality_label);
    memcpy(labelled + sizeof equality_label, input, 8 + h + sizeof context);
    assert(crypto_generichash(derived, 32, labelled, sizeof equality_label + 8 + h + sizeof context,
        root, 32) == 0);
    field("equalityDerived", derived, 32);
    assert(crypto_generichash(equality + h, 32, plaintext, 5, derived, 32) == 0);
    field("equality", equality, h + 32);
    unsigned char stream[256];
    h = header(stream, "jazz.stream.xchacha20poly1305");
    memset(input, 0, 4); input[3] = (unsigned char)h;
    memcpy(input + 4, stream, h);
    memset(input + 4 + h, 0, 4); input[7 + h] = sizeof context;
    memcpy(input + 8 + h, context, sizeof context);
    a = 8 + h + sizeof context;
    memcpy(labelled, stream_label, sizeof stream_label);
    memcpy(labelled + sizeof stream_label, input, a);
    assert(crypto_generichash(derived, 32, labelled, sizeof stream_label + a, root, 32) == 0);
    field("streamDerived", derived, 32);
    /* STREAM (BE32): a 19-byte nonce prefix, then per record a u32be index
     * and a last-record flag byte form the 24-byte XChaCha20-Poly1305 nonce. */
    memcpy(stream + h, nonce, 19);
    size_t position = h + 19;
    unsigned long long written;
    /* Two authenticated data records, followed by a separate empty last record. */
    for (size_t i = 0; i < 3; i++) {
        unsigned char record_nonce[24];
        size_t size = i == 2 ? 0 : 5;
        size_t cipher_size = size + crypto_aead_xchacha20poly1305_ietf_ABYTES;
        memcpy(record_nonce, nonce, 19);
        memset(record_nonce + 19, 0, 3); record_nonce[22] = (unsigned char)i;
        record_nonce[23] = i == 2 ? 1 : 0;
        memset(stream + position, 0, 4); stream[position + 3] = (unsigned char)cipher_size;
        assert(crypto_aead_xchacha20poly1305_ietf_encrypt(stream + position + 4, &written,
            plaintext, size, input, a, NULL, record_nonce, derived) == 0);
        assert(written == cipher_size);
        position += 4 + cipher_size;
    }
    field("stream", stream, position);
    /* What a writer given the single input `hello` produces: data record 0,
     * then the empty last record as record 1. */
    {
        unsigned char record_nonce[24];
        size_t first = h + 19 + 4 + 5 + crypto_aead_xchacha20poly1305_ietf_ABYTES;
        size_t cipher_size = crypto_aead_xchacha20poly1305_ietf_ABYTES;
        memcpy(record_nonce, nonce, 19);
        memset(record_nonce + 19, 0, 3); record_nonce[22] = 1; record_nonce[23] = 1;
        memset(stream + first, 0, 4); stream[first + 3] = (unsigned char)cipher_size;
        assert(crypto_aead_xchacha20poly1305_ietf_encrypt(stream + first + 4, &written,
            plaintext, 0, input, a, NULL, record_nonce, derived) == 0);
        assert(written == cipher_size);
        field("streamHello", stream, first + 4 + cipher_size);
    }
    sodium_memzero(derived, sizeof derived);
    puts("}");
    return 0;
}
