/* authorization_scope.h: jazz_protocol::authorization_scope (crates/jazz/layers/protocol/src/authorization_scope.rs) */
#ifndef JAZZ_PROTOCOL_AUTHORIZATION_SCOPE_H
#define JAZZ_PROTOCOL_AUTHORIZATION_SCOPE_H
#include "jz_rt.h"
#include "jazz_protocol/protocol.h"

#define JAZZ_MAX_AUTHORIZATION_SCOPES 256u
typedef struct jazz_protocol_SupportClause { jazz_model_ShapeId shape; jazz_model_BindingId binding; } jazz_protocol_SupportClause;
typedef struct jazz_protocol_AppliedBound { jazz_types_GlobalTime settled; u64 progress; } jazz_protocol_AppliedBound;
typedef struct jazz_protocol_AuthorityScopeAggregate {
    jz_btree expected_support; /* BTreeSet<(ShapeId, BindingId)> */
    jz_btree members;          /* SubscriptionKey -> SupportClause */
    jz_btree applied;          /* SubscriptionKey -> AppliedBound */
} jazz_protocol_AuthorityScopeAggregate;
extern const jz_btree_desc jazz_protocol__authorization_scope__DESC_CLAUSE_SET;
void jazz_protocol__authorization_scope__AuthorityScopeAggregate__new(jazz_protocol_AuthorityScopeAggregate *out, jz_btree expected_support_moved);
void jazz_protocol__authorization_scope__AuthorityScopeAggregate__Clone__clone(jazz_protocol_AuthorityScopeAggregate *dst, const jazz_protocol_AuthorityScopeAggregate *src);
void jazz_protocol__authorization_scope__AuthorityScopeAggregate__drop(jazz_protocol_AuthorityScopeAggregate *a);
bool jazz_protocol__authorization_scope__AuthorityScopeAggregate__register(jazz_protocol_AuthorityScopeAggregate *a, jazz_protocol_SubscriptionKey sub, jazz_protocol_SupportClause clause);
void jazz_protocol__authorization_scope__AuthorityScopeAggregate__forget(jazz_protocol_AuthorityScopeAggregate *a, jazz_protocol_SubscriptionKey sub);
bool jazz_protocol__authorization_scope__AuthorityScopeAggregate__has_no_members(const jazz_protocol_AuthorityScopeAggregate *a);
bool jazz_protocol__authorization_scope__AuthorityScopeAggregate__apply(jazz_protocol_AuthorityScopeAggregate *a, jazz_protocol_SubscriptionKey sub, jazz_types_GlobalTime settled, u64 progress, jazz_protocol_AppliedBound *out);
bool jazz_protocol__authorization_scope__AuthorityScopeAggregate__bounds(const jazz_protocol_AuthorityScopeAggregate *a, jazz_protocol_AppliedBound *out);

typedef struct jazz_protocol_AuthorityContext {
    u8 authority[16]; jazz_types_AuthorSubject link;
    u64 connection_id, connection_epoch, claims_revision, policy_epoch, authorization_progress, settled_through;
} jazz_protocol_AuthorityContext;
bool jazz_protocol__authorization_scope__AuthorityContext__same_admitted_link(jazz_protocol_AuthorityContext a, jazz_protocol_AuthorityContext b);
bool jazz_protocol__authorization_scope__AuthorityContext__PartialEq__eq(const jazz_protocol_AuthorityContext *a, const jazz_protocol_AuthorityContext *b);

typedef u64 jazz_protocol_AuthorizationScopeOwnerToken;
enum { JAZZ_ASA_OWNER = 0, JAZZ_ASA_WAITING, JAZZ_ASA_PROVEN };
typedef struct jazz_protocol_AuthorizationScopeAcquisition { u32 tag; jazz_protocol_AuthorizationScopeOwnerToken token; } jazz_protocol_AuthorizationScopeAcquisition;
enum { JAZZ_ASR_PROVEN = 0, JAZZ_ASR_OWNER, JAZZ_ASR_WAITING };
typedef struct jazz_protocol_AuthorizationScopeReadiness { u32 tag; jazz_protocol_AuthorizationScopeReceipt receipt; jazz_protocol_AuthorizationScopeOwnerToken token; } jazz_protocol_AuthorizationScopeReadiness;
enum { JAZZ_ASI_INSTALLED = 0, JAZZ_ASI_RETRY, JAZZ_ASI_WAITING, JAZZ_ASI_REJECTED };
typedef struct jazz_protocol_AuthorizationScopeInstall { u32 tag; jazz_protocol_AuthorizationScopeOwnerToken token; } jazz_protocol_AuthorizationScopeInstall;

typedef struct jazz_protocol_AuthScopeState { jz_btree scopes; /* AuthorizationSupportScopeKey -> Entry */ u64 next_lease; } jazz_protocol_AuthScopeState;
/* AuthorizationScopeRegistry(Rc<RefCell<State>>): jz_rc payload pointer */
typedef jazz_protocol_AuthScopeState *jazz_protocol_AuthorizationScopeRegistry;
typedef struct jazz_protocol_AuthorizationScopeLease {
    jazz_protocol_AuthScopeState *state; /* rc */
    jazz_protocol_AuthorizationSupportScopeKey key;
    u64 id;
    bool has_owner_generation; u64 owner_generation; /* Cell<Option<u64>> */
} jazz_protocol_AuthorizationScopeLease;
jazz_protocol_AuthorizationScopeRegistry jazz_protocol__authorization_scope__AuthorizationScopeRegistry__default(void);
jazz_protocol_AuthorizationScopeRegistry jazz_protocol__authorization_scope__AuthorizationScopeRegistry__clone(jazz_protocol_AuthorizationScopeRegistry r);
void jazz_protocol__authorization_scope__AuthorizationScopeRegistry__drop(jazz_protocol_AuthorizationScopeRegistry r);
bool jazz_protocol__authorization_scope__AuthorizationScopeRegistry__acquire(jazz_protocol_AuthorizationScopeRegistry r, jazz_protocol_AuthorizationSupportScopeKey key, jazz_protocol_AuthorizationScopeLease *lease, jazz_protocol_AuthorizationScopeAcquisition *acq);
bool jazz_protocol__authorization_scope__AuthorizationScopeRegistry__take_promotion(jazz_protocol_AuthorizationScopeRegistry r, jazz_protocol_AuthorizationScopeLease *lease, jazz_protocol_AuthorizationScopeOwnerToken *out);
jazz_protocol_AuthorizationScopeInstall jazz_protocol__authorization_scope__AuthorizationScopeRegistry__install(jazz_protocol_AuthorizationScopeRegistry r, jazz_protocol_AuthorizationScopeLease *lease, jazz_protocol_AuthorizationScopeOwnerToken token, jazz_protocol_AuthorityContext ctx, const jazz_protocol_AuthorizationScopeReceipt *receipt);
jazz_protocol_AuthorizationScopeReadiness jazz_protocol__authorization_scope__AuthorizationScopeRegistry__receipt(jazz_protocol_AuthorizationScopeRegistry r, jazz_protocol_AuthorizationScopeLease *lease, jazz_protocol_AuthorityContext ctx, u64 required_progress, u64 required_cut);
usize jazz_protocol__authorization_scope__AuthorizationScopeRegistry__len(jazz_protocol_AuthorizationScopeRegistry r);
bool jazz_protocol__authorization_scope__AuthorizationScopeRegistry__is_empty(jazz_protocol_AuthorizationScopeRegistry r);
void jazz_protocol__authorization_scope__AuthorizationScopeLease__Drop__drop(jazz_protocol_AuthorizationScopeLease *lease);
#endif
