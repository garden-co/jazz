/* policy.h: policy context (query_engine/policy.rs). Included from mod.h. */
#ifndef JAZZ_ENGINE_QUERY_ENGINE_POLICY_H
#define JAZZ_ENGINE_QUERY_ENGINE_POLICY_H

enum { JAZZ_ENGINE_PEM_PERMISSIVE_LOCAL, JAZZ_ENGINE_PEM_ENFORCING };
enum { JAZZ_ENGINE_PCTX_SYSTEM, JAZZ_ENGINE_PCTX_IDENTITY, JAZZ_ENGINE_PCTX_AUTHORIZATION_SUBPLAN };
typedef struct jazz_engine_PolicyContext {
    u8 tag;
    jazz_engine_SourceId protected_source; /* AuthorizationSubplan */
    u8 role;                               /* AuthorizationSubplan */
    u8 mode;
    jazz_types_AuthorSubject permission_subject;
    jz_btree claims; /* BTreeMap<String, Value> */
    bool has_attribution; jazz_types_AuthorSubject attribution;
} jazz_engine_PolicyContext;

#define JE_POL(x) jazz_engine__query_engine__policy__##x
void JE_POL(PolicyContext__Clone__clone)(jazz_engine_PolicyContext *dst, const jazz_engine_PolicyContext *src);
void JE_POL(PolicyContext__drop)(jazz_engine_PolicyContext *p);
void JE_POL(PolicyContext__Debug__fmt)(jz_str *out, const jazz_engine_PolicyContext *p);
void JE_POL(PolicyEnforcementMode__Debug__fmt)(jz_str *out, u8 mode);
bool JE_POL(PolicyContext__PartialEq__eq)(const jazz_engine_PolicyContext *a, const jazz_engine_PolicyContext *b);

#endif
