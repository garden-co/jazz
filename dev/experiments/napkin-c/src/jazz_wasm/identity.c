/* crates/jazz-wasm/src/identity.rs: local-first / anonymous self-signed JWTs */
#include "jazz_wasm/jazz_wasm.h"

static const jz_uuid KEY_NAMESPACE = {{0x6a, 0x61, 0x7a, 0x7a, 0x2d, 0x61, 0x75, 0x74, 0x68, 0x2d, 0x6b, 0x65, 0x79, 0x2d, 0x76, 0x31}};
static const jz_uuid NAMESPACE_DNS = {{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}};
static const char SIGN_DOMAIN[] = "jazz-auth-sign-v1";

const char jazz_wasm__identity__LOCAL_FIRST_ISSUER[] = "urn:jazz:local-first";
const char jazz_wasm__identity__ANONYMOUS_ISSUER[] = "urn:jazz:anonymous";

/* SigningKey: the 32-byte secret seed plus its derived verifying key */
typedef struct jazz_wasm_SigningKey { u8 secret[32]; u8 public_key[32]; } jazz_wasm_SigningKey;

static void jazz_wasm__identity__derive_signing_key(const u8 seed[32], jazz_wasm_SigningKey *out) {
    u8 buf[sizeof(SIGN_DOMAIN) - 1 + 32];
    memcpy(buf, SIGN_DOMAIN, sizeof(SIGN_DOMAIN) - 1);
    memcpy(buf + sizeof(SIGN_DOMAIN) - 1, seed, 32);
    u8 hash[64];
    jz_sha512(buf, sizeof buf, hash);
    memcpy(out->secret, hash, 32); /* hash[..32].try_into().expect("SHA-512 output is 64 bytes") */
    jz_ed25519_public_key(out->secret, out->public_key);
}

static jz_uuid jazz_wasm__identity__user_id_from_public_key(const u8 pub_key_bytes[32]) {
    return jz_uuid_v5(&KEY_NAMESPACE, pub_key_bytes, 32);
}

jz_uuid jazz_wasm__identity__derive_user_id(const u8 seed[32]) {
    jazz_wasm_SigningKey key;
    jazz_wasm__identity__derive_signing_key(seed, &key);
    return jazz_wasm__identity__user_id_from_public_key(key.public_key);
}

static void jw_str_field(jz_json_w *w, const char *k, const char *p, u32 n) {
    jz_jw_key(w, k);
    jz_jw_str(w, p, n);
}

jz_err jazz_wasm__identity__mint_jazz_self_signed_token_at(const u8 seed[32], const char *issuer, jz_slice audience, u64 ttl_seconds,
                                                           u64 now_seconds, jz_str *out) {
    jazz_wasm_SigningKey signing_key;
    jazz_wasm__identity__derive_signing_key(seed, &signing_key);
    jz_uuid user_id = jazz_wasm__identity__user_id_from_public_key(signing_key.public_key);
    char user_id_str[37];
    jz_uuid_to_string(&user_id, user_id_str);
    jz_str pub_key_b64 = {0};
    jz_base64_encode(signing_key.public_key, 32, true, false, &pub_key_b64);

    char normalized_aud[37];
    jz_uuid aud_uuid;
    jz_err pe = jz_uuid_parse(audience, &aud_uuid);
    if (pe.code) {
        jz_err_drop(&pe);
        aud_uuid = jz_uuid_v5(&NAMESPACE_DNS, audience.ptr, audience.len);
    }
    jz_uuid_to_string(&aud_uuid, normalized_aud);

    /* JwtHeader { alg, typ } */
    jz_str header_json = {0};
    jz_json_w w = {&header_json, 0, {0}};
    jz_jw_begin_obj(&w);
    jw_str_field(&w, "alg", "EdDSA", 5);
    jw_str_field(&w, "typ", "JWT", 3);
    jz_jw_end_obj(&w);
    jz_str header_b64 = {0};
    jz_base64_encode((const u8 *)header_json.ptr, header_json.len, true, false, &header_b64);

    /* LocalFirstClaims { iss, sub, aud, jazz_pub_key, iat, exp } */
    jz_str claims_json = {0};
    jz_json_w cw = {&claims_json, 0, {0}};
    jz_jw_begin_obj(&cw);
    jw_str_field(&cw, "iss", issuer, (u32)strlen(issuer));
    jw_str_field(&cw, "sub", user_id_str, 36);
    jw_str_field(&cw, "aud", normalized_aud, 36);
    jw_str_field(&cw, "jazz_pub_key", pub_key_b64.ptr, pub_key_b64.len);
    jz_jw_key(&cw, "iat");
    jz_jw_u64(&cw, now_seconds);
    jz_jw_key(&cw, "exp");
    jz_jw_u64(&cw, now_seconds + ttl_seconds);
    jz_jw_end_obj(&cw);
    jz_str claims_b64 = {0};
    jz_base64_encode((const u8 *)claims_json.ptr, claims_json.len, true, false, &claims_b64);

    jz_str signing_input = jz_format("%.*s.%.*s", (int)header_b64.len, header_b64.ptr, (int)claims_b64.len, claims_b64.ptr);
    u8 signature[64];
    jz_ed25519_sign(signing_key.secret, signing_key.public_key, (const u8 *)signing_input.ptr, signing_input.len, signature);
    jz_str signature_b64 = {0};
    jz_base64_encode(signature, 64, true, false, &signature_b64);

    *out = jz_format("%.*s.%.*s", (int)signing_input.len, signing_input.ptr, (int)signature_b64.len, signature_b64.ptr);

    jz_str_drop(&pub_key_b64);
    jz_str_drop(&header_json);
    jz_str_drop(&header_b64);
    jz_str_drop(&claims_json);
    jz_str_drop(&claims_b64);
    jz_str_drop(&signing_input);
    jz_str_drop(&signature_b64);
    return JZ_OK;
}
