/* wire_transport.h: crates/jazz/layers/db/src/db/wire_transport.rs */
#ifndef JAZZ_DB_DB_WIRE_TRANSPORT_H
#define JAZZ_DB_DB_WIRE_TRANSPORT_H
#include "channel_endpoint.h"
#include "view_update_parts.h"

typedef enum { JAZZ_WFS_IDLE, JAZZ_WFS_MORE_READY, JAZZ_WFS_BACKPRESSURED } jazz_db_WireFlushStatus;
typedef enum { JAZZ_WSO_ACCEPTED, JAZZ_WSO_REJECTED } jazz_db_WireSendOutcomeTag;
typedef struct jazz_db_WireSendOutcome { u32 tag; jazz_protocol_SyncMessage rejected; } jazz_db_WireSendOutcome;

typedef struct jazz_db_InboundViewParts { jazz_db_ViewUpdatePayloadVec parts; bool receipts_validated; } jazz_db_InboundViewParts;

typedef struct jazz_db_WireTransportAdapter {
    jazz_protocol_WireTransportDyn inner;
    jazz_protocol_WireInboundContext *inbound_context;
    bool has_session_context;
    jazz_db_ConnectionSessionContext session_context;
    bool permits_delegated_sessions;
    jazz_db_ChannelEndpoint endpoint;
    jazz_db_SharedAuxiliaryEndpoint *auxiliary;
    jz_btree routes;               /* Vec<u8> -> (u16, u64) */
    u8 canonical_turns;
    jz_err terminal_error;         /* Option<TransportError>: code 0 = None */
    bool has_last_wire_error;
    jazz_protocol_WireError last_wire_error;
    bool received_wire_error;
    jz_deque outbound_view_parts;  /* VecDeque<SyncMessage> */
    jz_btree inbound_view_parts;   /* SubscriptionKey -> InboundViewParts */
} jazz_db_WireTransportAdapter;

#define WTA_FN(name) jazz_db__db__wire_transport__WireTransportAdapter__##name
void WTA_FN(current)(jazz_protocol_WireTransportDyn inner, jazz_db_WireTransportAdapter *out);
void WTA_FN(new)(jazz_protocol_WireTransportDyn inner, u16 protocol_version, jazz_protocol_WireFeatures features, jazz_protocol_WireSession *session, jazz_db_WireTransportAdapter *out);
void WTA_FN(new_with_session_context)(jazz_protocol_WireTransportDyn inner, u16 protocol_version, jazz_protocol_WireFeatures features, jazz_protocol_WireSession *session, const jazz_db_ConnectionSessionContext *session_context, jazz_db_WireTransportAdapter *out);
void WTA_FN(new_with_session_context_and_delegated_sessions)(jazz_protocol_WireTransportDyn inner, u16 protocol_version, jazz_protocol_WireFeatures features, jazz_protocol_WireSession *session, const jazz_db_ConnectionSessionContext *session_context, bool permits_delegated_sessions, jazz_db_WireTransportAdapter *out);
jazz_protocol_WireTransportDyn WTA_FN(into_inner)(jazz_db_WireTransportAdapter *self);
void WTA_FN(drop)(void *self);
jz_err WTA_FN(try_recv_strict)(jazz_db_WireTransportAdapter *self, bool *has, jazz_protocol_SyncMessage *out);
const jazz_protocol_WireError *WTA_FN(remote_wire_error)(const jazz_db_WireTransportAdapter *self);
jz_err WTA_FN(offer)(jazz_db_WireTransportAdapter *self, jazz_protocol_SyncMessage message, jazz_db_WireSendOutcome *out);
/* boxes the adapter as dyn Transport */
jazz_db_Transport jazz_db__db__wire_transport__WireTransportAdapter__into_transport(jazz_db_WireTransportAdapter *boxed);
#endif
