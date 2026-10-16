/* row_availability.h: crates/jazz/layers/db/src/db/row_availability.rs */
#ifndef JAZZ_DB_DB_ROW_AVAILABILITY_H
#define JAZZ_DB_DB_ROW_AVAILABILITY_H
#include "../db.h"

#define JAZZ_DB_CURRENT_ROWS_MAX_PENDING 64u
typedef enum { JAZZ_CRR_APPLIED, JAZZ_CRR_UNKNOWN } jazz_db_CurrentRowsResultTag;
typedef struct jazz_db_CurrentRowsResult { u32 tag; jazz_protocol_CurrentRowsReceipt applied; } jazz_db_CurrentRowsResult;
typedef struct jazz_db_CurrentRowsRoute {
    jazz_protocol_CurrentRowsRequest request;
    jazz_protocol_PolicyBindingKey context;
    bool has_upstream; jazz_protocol_AuthorityContext upstream;
    bool has_downstream; u64 downstream_epoch; jazz_protocol_PermissionAdviceRequestId downstream_id;
    jz_chan *sender; /* Option<oneshot::Sender<CurrentRowsResult>> */
} jazz_db_CurrentRowsRoute;
typedef struct jazz_db_CurrentRowsFloor { jazz_types_GlobalTime cut; u64 progress; u64 policy; jz_btree protected_requests; } jazz_db_CurrentRowsFloor;
typedef struct jazz_db_FloorKey { jazz_types_NodeUuid core; u64 epoch; jazz_protocol_PolicyBindingKey context; } jazz_db_FloorKey;
typedef struct jazz_db_RouteCancel { jazz_protocol_AuthorityContext upstream; jazz_protocol_PermissionAdviceRequestId id; } jazz_db_RouteCancel;
typedef struct jazz_db_CurrentRowsRouter {
    jz_btree routes;     /* RequestId -> CurrentRowsRoute */
    jz_btree responses;  /* u64 -> VecDeque<CurrentRowsReceipt> */
    jz_deque cancels;    /* (AuthorityContext, RequestId) */
    jz_btree progress;   /* u64 -> u64 */
    jz_btree floors;     /* (NodeUuid, u64, PolicyBindingKey) -> CurrentRowsFloor */
} jazz_db_CurrentRowsRouter;
typedef jazz_db_CurrentRowsRouter *jazz_db_SharedCurrentRows;

#define CRR_FN(n) jazz_db__db__row_availability__CurrentRowsRouter__##n
void CRR_FN(default)(jazz_db_CurrentRowsRouter *out);
bool CRR_FN(admit)(jazz_db_CurrentRowsRouter *self, jazz_db_CurrentRowsRoute route);
void CRR_FN(finish)(jazz_db_CurrentRowsRouter *self, jazz_protocol_PermissionAdviceRequestId id, bool has_receipt, jazz_protocol_CurrentRowsReceipt receipt);
void CRR_FN(cancel_downstream)(jazz_db_CurrentRowsRouter *self, u64 epoch, jazz_protocol_PermissionAdviceRequestId id);
void CRR_FN(disconnect)(jazz_db_CurrentRowsRouter *self, u64 epoch);
bool jazz_db__db__row_availability__valid_request(const jazz_protocol_CurrentRowsRequest *request);
void jazz_db__db__row_availability__unknown_receipt(const jazz_protocol_CurrentRowsRequest *request, jazz_protocol_PolicyBindingKey context, jazz_protocol_CurrentRowsReceipt *out);
jazz_db_LocalFuture jazz_db__db__row_availability__Node__request_current_rows(const jazz_db_Node *self, jazz_db_CoordinateVec rows, jazz_protocol_PolicyBindingKey context);
jazz_db_LocalFuture jazz_db__db__row_availability__request_current_rows_for_owner(jazz_db_SharedCurrentRows router, jazz_db_SharedTickScheduler scheduler, bool has_upstream,
                                                                                  jazz_db_CoordinateVec rows, jazz_protocol_PolicyBindingKey context);
jz_err jazz_db__db__row_availability__PeerConnection__pump_current_rows(jazz_db_PeerConnection *self);
jz_err jazz_db__db__row_availability__receive_current_rows(jazz_db_SharedNodeState *node, jazz_db_SharedCurrentRows router, const jazz_protocol_AuthorityContext *selected,
                                                          const jazz_protocol_AuthorityContext *expected, bool eligible, jazz_protocol_CurrentRowsReceipt receipt);
void jazz_db__db__row_availability__CurrentRowsResult__drop(jazz_db_CurrentRowsResult *r);
#endif
