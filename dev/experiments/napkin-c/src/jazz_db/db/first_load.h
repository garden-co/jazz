/* first_load.h: crates/jazz/layers/db/src/db/first_load.rs */
#ifndef JAZZ_DB_DB_FIRST_LOAD_H
#define JAZZ_DB_DB_FIRST_LOAD_H
#include "../db.h"

#define JAZZ_DB_REMOTE_LINK_ATTEMPT_WINDOW_MS 5000u
typedef enum { JAZZ_RLH_NO_SERVER, JAZZ_RLH_ATTEMPTING, JAZZ_RLH_LIVE, JAZZ_RLH_FAILED } jazz_db_RemoteLinkHint;
typedef enum { JAZZ_RR_LIVE, JAZZ_RR_ATTEMPTING, JAZZ_RR_UNREACHABLE } jazz_db_RemoteReachTag;
typedef struct jazz_db_RemoteReach { u32 tag; u64 since; } jazz_db_RemoteReach;

typedef JZ_VEC(jz_waker) jazz_db_WakerVec;
struct jazz_db_RemoteLinkTracker {
    bool has_hint; u32 hint; u64 hint_since; /* Cell<Option<HostLink>> */
    usize live_upstreams;
    u64 loss_epoch;
    bool attempt_expiry_observed;
    JZ_VEC(u64) deadlines;
    jazz_db_SharedTickScheduler scheduler;
    jazz_db_WeakStateVec gated;
    jazz_db_WeakStateVec witnessed;
    jazz_db_WakerVec wakers;
};

#define RLT_FN(n) jazz_db__db__first_load__RemoteLinkTracker__##n
bool jazz_db__db__first_load__FirstLoad__requested_wait(jazz_db_FirstLoad f, u64 *out_ms);
bool jazz_db__db__first_load__RemoteLinkHint__from_host_str(jz_slice value, u32 *out);
jazz_db_RemoteLinkTracker *RLT_FN(new)(jazz_db_SharedTickScheduler scheduler);
bool RLT_FN(arm)(jazz_db_RemoteLinkTracker *self, u64 timeout_ms, u64 *epoch, u64 *deadline);
bool RLT_FN(arm_until_loss)(jazz_db_RemoteLinkTracker *self, u64 *epoch);
void RLT_FN(set_hint)(jazz_db_RemoteLinkTracker *self, u32 hint);
void RLT_FN(upstream_attached)(jazz_db_RemoteLinkTracker *self);
void RLT_FN(upstream_detached)(jazz_db_RemoteLinkTracker *self);
void RLT_FN(on_tick)(jazz_db_RemoteLinkTracker *self);
void RLT_FN(register)(jazz_db_RemoteLinkTracker *self, jazz_db_SubscriptionState *state);
bool RLT_FN(has_witnesses)(const jazz_db_RemoteLinkTracker *self);
void RLT_FN(register_witness)(jazz_db_RemoteLinkTracker *self, jazz_db_SubscriptionState *state);
void RLT_FN(resolve_witnesses)(jazz_db_RemoteLinkTracker *self, bool (*answered)(void *ctx, const jazz_db_UpstreamCoverageHandleVec *handles), void *ctx,
                               jazz_db_UpstreamCoverageHandleVec *retired);
void jazz_db__db__first_load__OpeningGate__armed(jazz_db_OpeningGate *out, u64 epoch, u64 deadline, u32 route);
bool jazz_db__db__first_load__SubscriptionSender__opening_gate(const jazz_db_SubscriptionSender *s, jazz_db_OpeningGate *out);
bool jazz_db__db__first_load__SubscriptionState__send_rejection(const jazz_db_SubscriptionState *state, jazz_db_SubscriptionEvent event);
void jazz_db__db__first_load__SubscriptionState__release_opening_gate(const jazz_db_SubscriptionState *state);
void jazz_db__db__first_load__Db__set_remote_link_hint(const jazz_db_Db *db, u32 hint);
bool jazz_db__db__first_load__Db__remote_link_hint_for_test(const jazz_db_Db *db, u32 *out);
u32 jazz_db__db__first_load__Db__resolve_first_load(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, const jazz_db_ReadOpts *opts,
                                                   u32 authorization_mode, u32 coverage_tier, bool *has_gate, jazz_db_OpeningGate *gate);
void jazz_db__db__first_load__Db__register_opening_gate(const jazz_db_Db *db, jazz_db_SubscriptionState *state);
/* remote/local are futures whose output is a jz_err (Result<T, E>; T is written by the future to its own slot) */
jz_err jazz_db__db__first_load__Db__read_local_first_within(const jazz_db_Db *db, u64 timeout_ms,
                                                           jazz_db_LocalFuture (*local)(void *ctx), jazz_db_LocalFuture (*remote)(void *ctx), void *ctx);
bool jazz_db__db__first_load__Db__race_remote_answer(const jazz_db_Db *db, u64 epoch, bool has_deadline, u64 deadline, jazz_db_LocalFuture remote, void *out);
#endif
