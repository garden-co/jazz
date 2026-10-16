/* policy.rs: identity, claims and policy mode used by policy augmentation. */
#include "mod.h"

void JE_POL(PolicyContext__Clone__clone)(jazz_engine_PolicyContext *dst, const jazz_engine_PolicyContext *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    if (src->tag == JAZZ_ENGINE_PCTX_SYSTEM) return;
    if (src->tag == JAZZ_ENGINE_PCTX_AUTHORIZATION_SUBPLAN) {
        sid_clone(&dst->protected_source, &src->protected_source);
        dst->role = src->role;
    }
    dst->mode = src->mode;
    dst->permission_subject = src->permission_subject;
    jz_btree_clone(&dst->claims, &src->claims, jazz_engine__str_clone_v, groove__records__values__Value__clone_v);
    dst->has_attribution = src->has_attribution;
    dst->attribution = src->attribution;
}

void JE_POL(PolicyContext__drop)(jazz_engine_PolicyContext *p) {
    if (p->tag == JAZZ_ENGINE_PCTX_SYSTEM) return;
    if (p->tag == JAZZ_ENGINE_PCTX_AUTHORIZATION_SUBPLAN) sid_drop(&p->protected_source);
    jz_btree_drop(&p->claims);
}

void JE_POL(PolicyEnforcementMode__Debug__fmt)(jz_str *out, u8 mode) {
    jz_str_push_cstr(out, mode == JAZZ_ENGINE_PEM_PERMISSIVE_LOCAL ? "PermissiveLocal" : "Enforcing");
}

void JE_POL(PolicyContext__Debug__fmt)(jz_str *out, const jazz_engine_PolicyContext *p) {
    if (p->tag == JAZZ_ENGINE_PCTX_SYSTEM) {
        jz_str_push_cstr(out, "System");
        return;
    }
    bool subplan = p->tag == JAZZ_ENGINE_PCTX_AUTHORIZATION_SUBPLAN;
    jazz_engine__fmt__struct_begin(out, subplan ? "AuthorizationSubplan" : "Identity");
    if (subplan) {
        jazz_engine__fmt__field(out, "protected_source", true);
        sid_debug(out, &p->protected_source);
        jazz_engine__fmt__field(out, "role", false);
        JE_OUT(PolicyDecisionRole__Debug__fmt)(out, p->role);
    }
    jazz_engine__fmt__field(out, "mode", !subplan);
    JE_POL(PolicyEnforcementMode__Debug__fmt)(out, p->mode);
    jazz_engine__fmt__field(out, "permission_subject", false);
    jazz_types__ids__AuthorSubject__Debug__fmt(out, &p->permission_subject);
    jazz_engine__fmt__field(out, "claims", false);
    jazz_engine__fmt__map(out, &p->claims, jazz_engine__fmt__str_v, jazz_engine__fmt__value_v);
    jazz_engine__fmt__field(out, "attribution", false);
    if (p->has_attribution) {
        jazz_engine__fmt__tuple_begin(out, "Some");
        jazz_types__ids__AuthorSubject__Debug__fmt(out, &p->attribution);
        jazz_engine__fmt__tuple_end(out);
    } else {
        jazz_engine__fmt__option_none(out);
    }
    jazz_engine__fmt__struct_end(out);
}

/* derived PartialEq */
bool JE_POL(PolicyContext__PartialEq__eq)(const jazz_engine_PolicyContext *a, const jazz_engine_PolicyContext *b) {
    if (a->tag != b->tag) return false;
    if (a->tag == JAZZ_ENGINE_PCTX_SYSTEM) return true;
    if (a->tag == JAZZ_ENGINE_PCTX_AUTHORIZATION_SUBPLAN && (!sid_eq(&a->protected_source, &b->protected_source) || a->role != b->role)) return false;
    return a->mode == b->mode && jazz_types__ids__AuthorSubject__PartialEq__eq(&a->permission_subject, &b->permission_subject) &&
           jz_btree_eq(&a->claims, &b->claims, groove__records__values__Value__eq_v) && a->has_attribution == b->has_attribution &&
           (!a->has_attribution || jazz_types__ids__AuthorSubject__PartialEq__eq(&a->attribution, &b->attribution));
}
