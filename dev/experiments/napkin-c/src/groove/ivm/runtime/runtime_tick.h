/* groove::ivm::runtime::runtime_tick — incremental tick, hydration and
 * pending-evaluation machinery of IvmRuntime. */
#ifndef GROOVE_IVM_RUNTIME_RUNTIME_TICK_H
#define GROOVE_IVM_RUNTIME_RUNTIME_TICK_H

#include "evaluator.h"

#define RTK(name) groove__ivm__runtime__runtime_tick__IvmRuntime__##name

/* Option<PublicationId> */
typedef struct groove_OptPublicationId { bool some; groove_PublicationId id; } groove_OptPublicationId;

/* Vec<(SubscriptionId, QueuedMultisinkDeltas)> */
typedef struct groove_SubscriptionNotification { groove_SubscriptionId id; groove_QueuedMultisinkDeltas queued; } groove_SubscriptionNotification;
typedef JZ_VEC(groove_SubscriptionNotification) groove_NotificationVec;
void groove__ivm__runtime__runtime_tick__NotificationVec__drop(void *v);

/* PendingResidentPublication: every field is shared (Rc<Cell>/Rc<RefCell>) */
typedef struct groove_PendingResidentPublication {
    groove_OptPublicationId *publication; /* Rc<Cell<Option<PublicationId>>> */
    groove_NotificationVec *notifications; /* Rc<RefCell<Vec<..>>> */
    bool *completed;                       /* Rc<Cell<bool>> */
    bool defer_notifications_until_durable;
    void *chunk_provider;                  /* Option<OwnedChunkProvider>, rc */
} groove_PendingResidentPublication;
void groove__ivm__runtime__runtime_tick__PendingResidentPublication__clone(groove_PendingResidentPublication *dst, const groove_PendingResidentPublication *src);
void groove__ivm__runtime__runtime_tick__PendingResidentPublication__drop(groove_PendingResidentPublication *p);

typedef struct groove_ResidentTick {
    groove_TickMetrics metrics;
    groove_PendingResidentPublication publication;
    groove_StagedWriteState *durable_writes; /* Rc<RefCell<StagedWriteState>> */
} groove_ResidentTick;
groove_OwnedWriteOperationVec groove__ivm__runtime__runtime_tick__ResidentTick__take_durable_writes(const groove_ResidentTick *t);
void groove__ivm__runtime__runtime_tick__ResidentTick__drop(groove_ResidentTick *t);

enum { GROOVE_DETACH_ANY_REQUEST, GROOVE_DETACH_CHUNK_REQUEST };
/* state::HydrationMode */
enum { GROOVE_HYDRATION_MODE_ORDINARY, GROOVE_HYDRATION_MODE_SUBSCRIPTION };

/* PendingIncrementalEvaluation(Rc<RefCell<PendingIncrementalState>>) */
struct groove_PendingIncrementalState *groove__ivm__runtime__runtime_tick__PendingIncrementalEvaluation__default(void);
bool groove__ivm__runtime__runtime_tick__PendingIncrementalEvaluation__is_pending(const struct groove_PendingIncrementalState *p);
void groove__ivm__runtime__runtime_tick__PendingIncrementalEvaluation__registered_nodes(const struct groove_PendingIncrementalState *p, jz_hmap *out);
void groove__ivm__runtime__runtime_tick__PendingIncrementalEvaluation__drop(struct groove_PendingIncrementalState *p);

jz_err RTK(enqueue_subscription_hydration)(groove_IvmRuntime *self, groove_SubscriptionId id, jz_btree outputs /* moved */,
    groove_OwnedStorage storage, groove_BindingSnapshots *binding_snapshots /* nullable rc, moved */,
    const jz_str *binding_frontier_advance /* nullable */, groove_InitialCell *initial /* rc, moved */,
    groove_SubscriptionLifetime lifetime, groove_RootIndirectValues root_indirect_values, jz_hmap borrowed /* moved */, u64 *out);
jz_err RTK(tick)(groove_IvmRuntime *self, groove_TableDeltaVec table_deltas, groove_OrderedKvStorage *storage, groove_TickMetrics *out);
jz_err RTK(flush_pending_binding_retractions)(groove_IvmRuntime *self, groove_OrderedKvStorage *storage);
jz_err RTK(tick_resident_staged)(groove_IvmRuntime *self, groove_TableDeltaVec table_deltas, groove_OwnedStorage storage,
    bool defer_notifications_until_durable, bool has_install, void *install_observer, void *install_failures, groove_ResidentTick *out);
jz_err RTK(tick_bindings_detaching_cold)(groove_IvmRuntime *self, groove_BindingDeltaVec binding_deltas, groove_OwnedStorage storage, groove_TickMetrics *out);
void RTK(assign_resident_publication)(groove_IvmRuntime *self, groove_ResidentTick tick, groove_PublicationId publication, groove_TickMetrics *out);
void RTK(settle_deferred_notifications)(groove_IvmRuntime *self, groove_PublicationId publication);
void RTK(discard_deferred_notifications)(groove_IvmRuntime *self, groove_PublicationId publication);
u32 RTK(poll_pending_incremental)(groove_IvmRuntime *self, const jz_waker *cx, jz_err *result); /* 0 ready, GROOVE_POLL_PENDING */
u32 RTK(poll_resident_incremental)(groove_IvmRuntime *self, const jz_waker *cx, jz_err *result);
void RTK(retain_owner_progress_waker)(groove_IvmRuntime *self, const jz_waker *owner);
u32 RTK(poll_storage_extraction)(groove_IvmRuntime *self, const groove_OwnedStorage *storage, const jz_waker *cx, jz_err *result);
bool RTK(has_pending_storage_writes)(const groove_IvmRuntime *self);
bool RTK(has_pending_incremental)(const groove_IvmRuntime *self);
bool RTK(subscription_has_pending_progress)(const groove_IvmRuntime *self, groove_SubscriptionId id);
bool RTK(subscription_has_pending_evaluation)(const groove_IvmRuntime *self, groove_SubscriptionId id);
bool RTK(has_resident_continuation)(const groove_IvmRuntime *self);
void RTK(cancel_pending_subscription_hydration)(groove_IvmRuntime *self, groove_SubscriptionId id);
jz_err RTK(drive_pending_incremental)(groove_IvmRuntime *self);
jz_err RTK(tick_with_params)(groove_IvmRuntime *self, groove_TableDeltaVec table_deltas, groove_BindingDeltaVec binding_deltas,
    groove_OwnedStorage storage, groove_OptPublicationId notification_publication, groove_TickMetrics *out);
void RTK(bump_input_frontiers)(groove_IvmRuntime *self, const groove_TableDelta *td, u32 ntd, const groove_BindingDelta *bd, u32 nbd);
jz_err RTK(hydration_snapshot)(groove_IvmRuntime *self, groove_NodeId output_node, groove_OrderedKvStorage *storage, u32 mode, groove_RecordDeltas *out);
jz_err RTK(hydration_snapshot_with_root_values)(groove_IvmRuntime *self, groove_NodeId output_node, groove_OrderedKvStorage *storage, u32 mode,
    groove_RootIndirectValues root_indirect_values, groove_RecordDeltas *out);
jz_err RTK(hydration_snapshots)(groove_IvmRuntime *self, const jz_btree *outputs, groove_OrderedKvStorage *storage, u32 mode, groove_MultisinkDeltas *out);

#endif
