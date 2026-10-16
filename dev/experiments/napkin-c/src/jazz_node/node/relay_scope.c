/* Port of crates/jazz/layers/node/src/node/relay_scope.rs */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

static void push_be_u64(jz_bytes *b, u64 v) {
    u8 buf[8];
    for (int i = 0; i < 8; i++) buf[i] = (u8)(v >> (56 - 8 * i));
    jz_bytes_push(b, buf, 8);
}

/* blake3::derive_key(context, material) */
static void blake3_derive_key(const char *context, const u8 *material, u32 n, u8 out[32]) {
    u8 context_key[32];
    jz_blake3(( const u8 *)context, strlen(context), context_key);
    jz_blake3_hasher *h = jz_blake3_new();
    jz_blake3_update(h, context_key, 32);
    jz_blake3_update(h, material, n);
    jz_blake3_finalize(h, out);
    jz_blake3_free(h);
}

void jazz_node__node__relay_scope__ClientRelayScope__durable_digest(const jazz_node_ClientRelayScope *s, u8 out[32]) {
    jz_bytes material = {0};
    push_be_u64(&material, s->storage_owner.len);
    jz_bytes_push(&material, (const u8 *)s->storage_owner.ptr, s->storage_owner.len);
    if (s->has_admitted_session) {
        jz_bytes_push_u8(&material, 1);
        jz_slice c = jazz_types__ids__AuthorSubject__canonical(&s->admitted_session);
        push_be_u64(&material, c.len);
        jz_bytes_push(&material, c.ptr, c.len);
    } else {
        jz_bytes_push_u8(&material, 0);
    }
    blake3_derive_key("jazz.scope-relay-repair-ledger.v1", material.ptr, material.len, out);
    jz_bytes_drop(&material);
}

void jazz_node__node__relay_scope__ClientRelayScope__durable_components(const jazz_node_ClientRelayScope *s, const jz_str **owner,
                                                                       bool *has_session, jz_str *session) {
    *owner = &s->storage_owner;
    *has_session = s->has_admitted_session;
    if (s->has_admitted_session) *session = jz_str_from_slice(jazz_types__ids__AuthorSubject__canonical(&s->admitted_session));
}

jazz_node_ClientRelayScope jazz_node__node__relay_scope__ClientRelayScope__from_admitted_storage_owner(jz_str storage_owner, jazz_types_AuthorSubject admitted) {
    jazz_node_ClientRelayScope s = { storage_owner, true, admitted };
    return s;
}

bool jazz_node__node__relay_scope__ClientRelayScope__same_owner(const jazz_node_ClientRelayScope *a, const jazz_node_ClientRelayScope *b) {
    if (!jz_str_eq(&a->storage_owner, &b->storage_owner)) return false;
    if (a->has_admitted_session != b->has_admitted_session) return false;
    return !a->has_admitted_session || jazz_types_AuthorSubject_eq(&a->admitted_session, &b->admitted_session);
}

bool jazz_node__node__relay_scope__ClientRelayScope__admits_session(const jazz_node_ClientRelayScope *s, jazz_types_AuthorSubject session) {
    return !s->has_admitted_session || jazz_types_AuthorSubject_eq(&s->admitted_session, &session);
}

void jazz_node__node__relay_scope__ClientRelayScope__clone(jazz_node_ClientRelayScope *dst, const jazz_node_ClientRelayScope *src) {
    *dst = *src;
    dst->storage_owner = jz_str_clone(&src->storage_owner);
}

void jazz_node__node__relay_scope__ClientRelayScope__drop(jazz_node_ClientRelayScope *s) { jz_str_drop(&s->storage_owner); }
