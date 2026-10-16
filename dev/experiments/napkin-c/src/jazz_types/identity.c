/* crates/jazz/layers/types/src/identity.rs
 * Shipped through jazz_wasm::verify_self_signed_runtime_author (which inlines
 * verify_client_runtime_author_at and the max-ttl verifier). The minting /
 * key-derivation half of this file is not shipped from here: jazz_wasm has its
 * own copy in crates/jazz-wasm/src/identity.rs. */
#include "jazz_types.h"

static const jz_uuid KEY_NAMESPACE = {{0x6a,0x61,0x7a,0x7a,0x2d,0x61,0x75,0x74,0x68,0x2d,0x6b,0x65,0x79,0x2d,0x76,0x31}};
static const jz_uuid UUID_NAMESPACE_DNS = {{0x6b,0xa7,0xb8,0x10,0x9d,0xad,0x11,0xd1,0x80,0xb4,0x00,0xc0,0x4f,0xd4,0x30,0xc8}};
#define DEFAULT_MAX_TTL_SECONDS 3600u

const char jazz_types__identity__LOCAL_FIRST_ISSUER[] = "urn:jazz:local-first";
const char jazz_types__identity__ANONYMOUS_ISSUER[] = "urn:jazz:anonymous";
static const char *const KNOWN_SELF_SIGNED_ISSUERS[] = { jazz_types__identity__LOCAL_FIRST_ISSUER, jazz_types__identity__ANONYMOUS_ISSUER };

#define IERR(...) jz_err_fmt(JAZZ_TYPES_ERR_IDENTITY, __VA_ARGS__)
#define EMSG(e) ((e).msg ? (int)(e).msg->len : 0), ((e).msg ? (e).msg->ptr : "")

bool jazz_types__identity__principal_is_nonempty(jz_slice principal) {
    for (u32 i = 0; i < principal.len; i++) {
        u8 b = principal.ptr[i];
        if (!(b == ' ' || b == '\t' || b == '\n' || b == 0x0b || b == 0x0c || b == '\r')) return true;
    }
    return false;
}

static jz_uuid jazz_types__identity__user_id_from_public_key(const u8 pub_key_bytes[32]) {
    return jz_uuid_v5(&KEY_NAMESPACE, pub_key_bytes, 32);
}

/* Debug of Option<String> */
static void debug_opt_str(jz_str *out, bool some, const jz_str *s) {
    if (!some) { jz_str_push_cstr(out, "None"); return; }
    jz_str_appendf(out, "Some(\"%.*s\")", (int)s->len, s->ptr);
}

typedef struct LocalFirstClaimsRaw {
    bool has_iss; jz_str iss;
    bool has_sub; jz_str sub;
    bool has_aud; jz_str aud;
    bool has_jazz_pub_key; jz_str jazz_pub_key;
    bool has_iat; u64 iat;
    bool has_exp; u64 exp;
} LocalFirstClaimsRaw;

static void LocalFirstClaimsRaw_drop(LocalFirstClaimsRaw *c) {
    jz_str_drop(&c->iss); jz_str_drop(&c->sub); jz_str_drop(&c->aud); jz_str_drop(&c->jazz_pub_key);
}

static jz_err opt_string_field(const jz_json *v, const char *name, bool *has, jz_str *out) {
    if (*has) return jz_err_fmt(JZ_JSON_ERR_TYPE, "duplicate field `%s`", name);
    if (v->kind == JZ_JSON_NULL) return JZ_OK;
    if (v->kind != JZ_JSON_STR) return jz_err_fmt(JZ_JSON_ERR_TYPE, "invalid type: expected a string for `%s`", name);
    *has = true;
    *out = jz_str_clone(&v->v.s);
    return JZ_OK;
}
static jz_err opt_u64_field(const jz_json *v, const char *name, bool *has, u64 *out) {
    if (*has) return jz_err_fmt(JZ_JSON_ERR_TYPE, "duplicate field `%s`", name);
    if (v->kind == JZ_JSON_NULL) return JZ_OK;
    if (!jz_json_as_u64(v, out)) return jz_err_fmt(JZ_JSON_ERR_TYPE, "invalid type: expected u64 for `%s`", name);
    *has = true;
    return JZ_OK;
}

/* #[derive(Deserialize)] #[serde(deny_unknown_fields)] struct LocalFirstClaimsRaw */
static jz_err LocalFirstClaimsRaw_from_json(const jz_json *json, LocalFirstClaimsRaw *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_json_expect_obj(json, "struct LocalFirstClaimsRaw"));
    jz_str *key; jz_json **val;
    JZ_BTREE_FOR(&json->v.obj, key, val) {
        jz_err e;
        if (jz_str_eq_cstr(key, "iss")) e = opt_string_field(*val, "iss", &out->has_iss, &out->iss);
        else if (jz_str_eq_cstr(key, "sub")) e = opt_string_field(*val, "sub", &out->has_sub, &out->sub);
        else if (jz_str_eq_cstr(key, "aud")) e = opt_string_field(*val, "aud", &out->has_aud, &out->aud);
        else if (jz_str_eq_cstr(key, "jazz_pub_key")) e = opt_string_field(*val, "jazz_pub_key", &out->has_jazz_pub_key, &out->jazz_pub_key);
        else if (jz_str_eq_cstr(key, "iat")) e = opt_u64_field(*val, "iat", &out->has_iat, &out->iat);
        else if (jz_str_eq_cstr(key, "exp")) e = opt_u64_field(*val, "exp", &out->has_exp, &out->exp);
        else e = jz_err_fmt(JZ_JSON_ERR_UNKNOWN_VARIANT, "unknown field `%.*s`, expected one of `iss`, `sub`, `aud`, `jazz_pub_key`, `iat`, `exp`",
                            (int)key->len, key->ptr);
        if (e.code) { LocalFirstClaimsRaw_drop(out); return e; }
    }
    return JZ_OK;
}

static jz_str normalize_audience(jz_slice audience) {
    jz_uuid u;
    jz_err e = jz_uuid_parse(audience, &u);
    if (e.code) {
        jz_err_drop(&e);
        u = jz_uuid_v5(&UUID_NAMESPACE_DNS, audience.ptr, audience.len);
    }
    char buf[37];
    jz_uuid_to_string(&u, buf);
    return jz_str_from_slice(jz_slice_of(buf, 36));
}

jz_err jazz_types__identity__verify_jazz_self_signed_proof_with_max_ttl_at(jz_slice token, jz_slice expected_audience, u64 max_ttl_seconds,
                                                                         u64 now_seconds, jazz_types_VerifiedSelfSigned *out) {
    jz_err result = JZ_OK;
    jz_str normalized_expected = normalize_audience(expected_audience);
    jz_bytes header_bytes = {0}, claims_bytes = {0}, pub_key_bytes_vec = {0}, sig_bytes_vec = {0};
    jz_str signing_input = {0}, tmp = {0};
    jz_json *header = NULL, *claims_json = NULL;
    LocalFirstClaimsRaw claims; memset(&claims, 0, sizeof claims);

    /* token.splitn(3, '.') */
    u32 d1 = 0, d2;
    while (d1 < token.len && token.ptr[d1] != '.') d1++;
    if (d1 == token.len) { result = IERR("invalid token: expected 3 parts"); goto done; }
    d2 = d1 + 1;
    while (d2 < token.len && token.ptr[d2] != '.') d2++;
    if (d2 == token.len) { result = IERR("invalid token: expected 3 parts"); goto done; }
    jz_slice header_b64 = jz_slice_of(token.ptr, d1);
    jz_slice claims_b64 = jz_slice_of(token.ptr + d1 + 1, d2 - d1 - 1);
    jz_slice sig_b64 = jz_slice_of(token.ptr + d2 + 1, token.len - d2 - 1);

    jz_err e = jz_base64_decode(header_b64, true, &header_bytes);
    if (e.code) { result = IERR("header decode: %.*s", EMSG(e)); jz_err_drop(&e); goto done; }
    e = jz_json_parse(jz_bytes_slice(&header_bytes), &header);
    if (e.code) { result = IERR("header parse: %.*s", EMSG(e)); jz_err_drop(&e); goto done; }
    /* JwtHeaderRaw { alg: Option<String> } */
    {
        jz_err te = jz_json_expect_obj(header, "struct JwtHeaderRaw");
        if (te.code) { result = IERR("header parse: %.*s", EMSG(te)); jz_err_drop(&te); goto done; }
        jz_json *alg = jz_json_obj_get(header, "alg");
        bool has_alg = alg && alg->kind == JZ_JSON_STR;
        if (alg && alg->kind != JZ_JSON_STR && alg->kind != JZ_JSON_NULL) { result = IERR("header parse: invalid type: expected a string"); goto done; }
        if (!has_alg || !jz_str_eq_cstr(&alg->v.s, "EdDSA")) {
            debug_opt_str(&tmp, has_alg, has_alg ? &alg->v.s : NULL);
            result = IERR("unsupported alg: %.*s", (int)tmp.len, tmp.ptr);
            goto done;
        }
    }

    e = jz_base64_decode(claims_b64, true, &claims_bytes);
    if (e.code) { result = IERR("claims decode: %.*s", EMSG(e)); jz_err_drop(&e); goto done; }
    e = jz_json_parse(jz_bytes_slice(&claims_bytes), &claims_json);
    if (e.code) { result = IERR("claims parse: %.*s", EMSG(e)); jz_err_drop(&e); goto done; }
    e = LocalFirstClaimsRaw_from_json(claims_json, &claims);
    if (e.code) { result = IERR("claims parse: %.*s", EMSG(e)); jz_err_drop(&e); goto done; }

    const char *matched_issuer = NULL;
    for (u32 i = 0; i < 2; i++) {
        const char *known = KNOWN_SELF_SIGNED_ISSUERS[i];
        if (claims.has_iss ? jz_str_eq_cstr(&claims.iss, known) : known[0] == 0) { matched_issuer = known; break; }
    }
    if (!matched_issuer) {
        debug_opt_str(&tmp, claims.has_iss, &claims.iss);
        result = IERR("invalid issuer: %.*s", (int)tmp.len, tmp.ptr);
        goto done;
    }

    if (!(claims.has_aud && jz_str_eq(&claims.aud, &normalized_expected))) {
        debug_opt_str(&tmp, claims.has_aud, &claims.aud);
        result = IERR("audience mismatch: expected \"%.*s\", got %.*s", (int)normalized_expected.len, normalized_expected.ptr,
                      (int)tmp.len, tmp.ptr);
        goto done;
    }

    if (!claims.has_jazz_pub_key) { result = IERR("missing jazz_pub_key"); goto done; }
    e = jz_base64_decode(jz_str_slice(&claims.jazz_pub_key), true, &pub_key_bytes_vec);
    if (e.code) { result = IERR("pub key decode: %.*s", EMSG(e)); jz_err_drop(&e); goto done; }
    if (pub_key_bytes_vec.len != 32) { result = IERR("pub key must be 32 bytes, got %u", pub_key_bytes_vec.len); goto done; }
    u8 pub_key_bytes[32];
    memcpy(pub_key_bytes, pub_key_bytes_vec.ptr, 32);
    /* VerifyingKey::from_bytes (point decompression) is part of jz_ed25519_verify in the runtime. */

    signing_input = jz_format("%.*s.%.*s", (int)header_b64.len, header_b64.ptr, (int)claims_b64.len, claims_b64.ptr);
    e = jz_base64_decode(sig_b64, true, &sig_bytes_vec);
    if (e.code) { result = IERR("sig decode: %.*s", EMSG(e)); jz_err_drop(&e); goto done; }
    if (sig_bytes_vec.len != 64) { result = IERR("signature must be 64 bytes"); goto done; }
    if (!jz_ed25519_verify(pub_key_bytes, (const u8 *)signing_input.ptr, signing_input.len, sig_bytes_vec.ptr)) {
        result = IERR("signature verification failed: %s", "signature error");
        goto done;
    }

    jz_uuid derived_user_id = jazz_types__identity__user_id_from_public_key(pub_key_bytes);
    char derived[37];
    jz_uuid_to_string(&derived_user_id, derived);
    if (!claims.has_sub) { result = IERR("missing sub"); goto done; }
    if (!jz_str_eq_cstr(&claims.sub, derived)) {
        result = IERR("sub mismatch: expected %s, got %.*s", derived, (int)claims.sub.len, claims.sub.ptr);
        goto done;
    }
    if (!claims.has_iat) { result = IERR("missing iat"); goto done; }
    if (!claims.has_exp) { result = IERR("missing exp"); goto done; }
    u64 iat = claims.iat, exp = claims.exp, now = now_seconds;
    if (iat > now + 60) {
        result = IERR("token issued in the future: iat=%llu, now=%llu", (unsigned long long)iat, (unsigned long long)now);
        goto done;
    }
    if (exp <= now) {
        result = IERR("token expired: exp=%llu, now=%llu", (unsigned long long)exp, (unsigned long long)now);
        goto done;
    }
    u64 ttl = exp > iat ? exp - iat : 0;
    if (ttl > max_ttl_seconds) {
        result = IERR("token ttl %llus exceeds max %llus", (unsigned long long)ttl, (unsigned long long)max_ttl_seconds);
        goto done;
    }
    out->user_id = jz_str_from_slice(jz_slice_of(derived, 36));
    memcpy(out->public_key_bytes, pub_key_bytes, 32);
    out->issuer = matched_issuer;
done:
    LocalFirstClaimsRaw_drop(&claims);
    if (header) jz_json_free(header);
    if (claims_json) jz_json_free(claims_json);
    jz_bytes_drop(&header_bytes); jz_bytes_drop(&claims_bytes); jz_bytes_drop(&pub_key_bytes_vec); jz_bytes_drop(&sig_bytes_vec);
    jz_str_drop(&signing_input); jz_str_drop(&tmp); jz_str_drop(&normalized_expected);
    return result;
}

jz_err jazz_types__identity__verify_jazz_self_signed_proof_at(jz_slice token, jz_slice expected_audience, u64 now_seconds,
                                                            jazz_types_VerifiedSelfSigned *out) {
    return jazz_types__identity__verify_jazz_self_signed_proof_with_max_ttl_at(token, expected_audience, DEFAULT_MAX_TTL_SECONDS,
                                                                             now_seconds, out);
}

static jz_err to_string_err(jz_err e) {
    jz_err r = IERR("%.*s", EMSG(e));
    jz_err_drop(&e);
    return r;
}

jz_err jazz_types__identity__verify_client_runtime_author_at(jz_slice token, jz_slice expected_audience, jz_slice claimed_author,
                                                           u64 now_seconds, jazz_types_AuthorSubject *out) {
    jazz_types_VerifiedSelfSigned verified;
    JZ_TRY(jazz_types__identity__verify_jazz_self_signed_proof_at(token, expected_audience, now_seconds, &verified));
    jz_err result = JZ_OK;
    jazz_types_AuthorSubject author, claimed;
    jz_err e = jazz_types__ids__AuthorSubject__reserved(jz_slice_of(verified.issuer, (u32)strlen(verified.issuer)),
                                                        jz_str_slice(&verified.user_id), &author);
    if (e.code) { result = to_string_err(e); goto done; }
    e = jazz_types__ids__AuthorSubject__from_canonical(claimed_author, &claimed);
    if (e.code) { result = to_string_err(e); goto done; }
    jazz_types_AccountId claimed_account;
    if (jazz_types__ids__AuthorSubject__account_id(&claimed, &claimed_account)) {
        if (verified.issuer != jazz_types__identity__LOCAL_FIRST_ISSUER && strcmp(verified.issuer, jazz_types__identity__LOCAL_FIRST_ISSUER) != 0) {
            result = IERR("only local-first proofs can found accounts");
            goto done;
        }
        jazz_types_AppId app;
        e = jazz_types__app_id__AppId__from_string(expected_audience, &app);
        if (e.code) { jz_err_drop(&e); app = jazz_types__app_id__AppId__from_name(expected_audience); }
        author = jazz_types__ids__AuthorSubject__with_account(
            author, jazz_types__account_registry__local_first_account_id(*app, jz_str_slice(&verified.user_id)));
    }
    jz_slice canonical = jazz_types__ids__AuthorSubject__canonical(&author);
    if (jz_slice_cmp(canonical, claimed_author) != 0) {
        result = IERR("self-signed author mismatch: expected \"%.*s\", got \"%.*s\"", (int)canonical.len, canonical.ptr,
                      (int)claimed_author.len, claimed_author.ptr);
        goto done;
    }
    *out = author;
done:
    jz_str_drop(&verified.user_id);
    return result;
}
