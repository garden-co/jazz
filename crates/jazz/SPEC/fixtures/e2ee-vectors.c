/* Independent protocol fixture generator; no Jazz TypeScript helpers.
 * Link against the pinned libsodium archive. Sealed-box output is random;
 * all other fields are deterministic. Test keys only, never production keys.
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
int main(void) {
    assert(sodium_init() >= 0);
    assert(strcmp(sodium_version_string(), "1.0.22") == 0);
    unsigned char root[32], key[32], nonce[24], seed[32], pk[32], sk[32];
    for (size_t i = 0; i < 32; i++) { root[i] = i; key[i] = 32 + i; seed[i] = 42 + i; }
    for (size_t i = 0; i < 24; i++) nonce[i] = i;
    assert(crypto_box_seed_keypair(pk, sk, seed) == 0);
    unsigned char context[50] = { 'J', 'E', '2', 'C', 1 };
    const char values[] = "apsitrced";
    for (size_t i = 0; i < 9; i++) { context[5 + i * 5 + 3] = 1; context[5 + i * 5 + 4] = values[i]; }
    unsigned char cell[256], wrap[256], sealed[256], aad[128], input[256], derived[32];
    const unsigned char plaintext[] = "hello";
    const char cell_label[] = "jazz.e2ee.cell-key.v1";
    const char wrap_label[] = "jazz.e2ee.wrap-key.v1";
    printf("{");
    field("root", root, 32); field("key", key, 32); field("nonce", nonce, 24);
    field("context", context, sizeof context); field("plaintext", plaintext, 5);
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
    h = header(wrap, "jazz.sodium.key"); wrap[h++] = 1;
    memcpy(aad, wrap, h); memcpy(aad + h, context, sizeof context);
    a = h + sizeof context;
    memcpy(input, wrap_label, sizeof wrap_label); memcpy(input + sizeof wrap_label, aad, a);
    assert(crypto_generichash(derived, 32, input, sizeof wrap_label + a, root, 32) == 0);
    field("wrapDerived", derived, 32); field("wrapAad", aad, a);
    memcpy(wrap + h, nonce, 24);
    assert(crypto_aead_xchacha20poly1305_ietf_encrypt(wrap + h + 24, NULL,
        key, 32, aad, a, NULL, nonce, derived) == 0);
    field("wrap", wrap, h + 24 + 32 + 16);
    h = header(sealed, "jazz.sodium.key"); sealed[h++] = 2;
    memcpy(input, sealed, h);
    memset(input + h, 0, 3); input[h + 3] = sizeof context;
    memcpy(input + h + 4, context, sizeof context);
    memcpy(input + h + 4 + sizeof context, key, 32);
    size_t body = h + 4 + sizeof context + 32;
    field("sealedPlaintext", input, body);
    assert(crypto_box_seal(sealed + h, input, body, pk) == 0);
    field("sealed", sealed, h + body + 48);
    unsigned char equality[128];
    h = header(equality, "jazz.sodium.equality");
    /* Two u32be-length-prefixed fields: mechanism header, then context. */
    memset(input, 0, 4); input[3] = (unsigned char)h;
    memcpy(input + 4, equality, h);
    memset(input + 4 + h, 0, 4); input[7 + h] = sizeof context;
    memcpy(input + 8 + h, context, sizeof context);
    field("equalityContext", input, 8 + h + sizeof context);
    assert(crypto_generichash(derived, 32, input, 8 + h + sizeof context, root, 32) == 0);
    field("equalityDerived", derived, 32);
    assert(crypto_generichash(equality + h, 32, plaintext, 5, derived, 32) == 0);
    field("equality", equality, h + 32);
    sodium_memzero(derived, sizeof derived);
    puts("}");
    return 0;
}
