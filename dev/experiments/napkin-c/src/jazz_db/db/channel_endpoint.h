/* channel_endpoint.h: crates/jazz/layers/db/src/db/channel_endpoint.rs */
#ifndef JAZZ_DB_DB_CHANNEL_ENDPOINT_H
#define JAZZ_DB_DB_CHANNEL_ENDPOINT_H
#include "routed_messages.h"

typedef struct jazz_db_ChannelEndpoint {
    jazz_db_RoutedMessages routing;
    jazz_protocol_OrderedChannelBackend *raw;
    jazz_protocol_WireInboundContext *context;
    bool has_last_wire_error;
    jazz_protocol_WireError last_wire_error;
} jazz_db_ChannelEndpoint;

typedef struct jazz_db_AuxiliaryChannelEndpoint {
    jazz_db_ChannelEndpoint endpoint;
    bool has_message;
    jazz_protocol_SyncMessage message;
    bool pump_owned;
    bool has_waker;
    jz_waker waker;
} jazz_db_AuxiliaryChannelEndpoint;

/* SharedAuxiliaryEndpoint = Arc<Mutex<AuxiliaryChannelEndpoint>>: a jz_rc payload */
typedef struct jazz_db_SharedAuxiliaryEndpoint { bool poisoned; jazz_db_AuxiliaryChannelEndpoint inner; } jazz_db_SharedAuxiliaryEndpoint;
jazz_db_SharedAuxiliaryEndpoint *jazz_db__db__channel_endpoint__SharedAuxiliaryEndpoint__new(jazz_db_AuxiliaryChannelEndpoint endpoint);
/* lock(): NULL when poisoned */
static inline jazz_db_AuxiliaryChannelEndpoint *jazz_db_aux_lock(jazz_db_SharedAuxiliaryEndpoint *s) { return s->poisoned ? NULL : &s->inner; }

jz_err jazz_db__db__channel_endpoint__ChannelEndpoint__new(jazz_protocol_WireInboundContext *context, jazz_db_ChannelEndpoint *out);
void jazz_db__db__channel_endpoint__ChannelEndpoint__drop(jazz_db_ChannelEndpoint *e);
void jazz_db__db__channel_endpoint__ChannelEndpoint__set_trusted_encoder(jazz_db_ChannelEndpoint *e, bool trusted);
bool jazz_db__db__channel_endpoint__ChannelEndpoint__last_wire_error(const jazz_db_ChannelEndpoint *e, jazz_protocol_WireError *out);
bool jazz_db__db__channel_endpoint__ChannelEndpoint__incomplete_receive_timeout_ms(const jazz_db_ChannelEndpoint *e, u64 *out);
jz_err jazz_db__db__channel_endpoint__ChannelEndpoint__expire(jazz_db_ChannelEndpoint *e);
jz_err jazz_db__db__channel_endpoint__ChannelEndpoint__enqueue(jazz_db_ChannelEndpoint *e, u16 channel, u64 generation, u32 class_, const jazz_protocol_SyncMessage *message, bool barrier);
jazz_protocol_SharedChannelCredits *jazz_db__db__channel_endpoint__ChannelEndpoint__channel_credits(const jazz_db_ChannelEndpoint *e);
bool jazz_db__db__channel_endpoint__ChannelEndpoint__is_idle(const jazz_db_ChannelEndpoint *e, u16 channel);
jz_err jazz_db__db__channel_endpoint__ChannelEndpoint__next_idle_generation(const jazz_db_ChannelEndpoint *e, u16 channel, u64 *out);
bool jazz_db__db__channel_endpoint__ChannelEndpoint__has_pending(const jazz_db_ChannelEndpoint *e);
jz_err jazz_db__db__channel_endpoint__ChannelEndpoint__peek_outbound(jazz_db_ChannelEndpoint *e, bool *has, jz_bytes *out);
jz_err jazz_db__db__channel_endpoint__ChannelEndpoint__accept_outbound(jazz_db_ChannelEndpoint *e, bool *completed);
bool jazz_db__db__channel_endpoint__ChannelEndpoint__pop(jazz_db_ChannelEndpoint *e, jazz_db_ReceivedSyncMessage *out);
jz_err jazz_db__db__channel_endpoint__ChannelEndpoint__receive(jazz_db_ChannelEndpoint *e, jazz_protocol_WireChannelEnvelope frame, u32 encoded_len, bool *has, jazz_db_ReceivedSyncMessage *out);
u32 jazz_db__db__channel_endpoint__message_class(const jazz_protocol_SyncMessage *message, bool *barrier);

jz_err jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__new(jazz_protocol_WireInboundContext *context, jazz_db_AuxiliaryChannelEndpoint *out);
void jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__drop(jazz_db_AuxiliaryChannelEndpoint *e);
void jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__set_trusted_encoder(jazz_db_AuxiliaryChannelEndpoint *e, bool trusted);
bool jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__last_wire_error(const jazz_db_AuxiliaryChannelEndpoint *e, jazz_protocol_WireError *out);
bool jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__incomplete_receive_timeout_ms(const jazz_db_AuxiliaryChannelEndpoint *e, u64 *out);
jz_err jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__expire_incomplete_receive(jazz_db_AuxiliaryChannelEndpoint *e);
void jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__set_channel_credits(jazz_db_AuxiliaryChannelEndpoint *e, jazz_protocol_SharedChannelCredits *credits);
jazz_protocol_SharedChannelCredits *jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__channel_credits(const jazz_db_AuxiliaryChannelEndpoint *e);
bool jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__outbound_is_ready(const jazz_db_AuxiliaryChannelEndpoint *e);
void jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__set_pump_owned(jazz_db_AuxiliaryChannelEndpoint *e);
bool jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__pump_owned(const jazz_db_AuxiliaryChannelEndpoint *e);
jz_err jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__enqueue(jazz_db_AuxiliaryChannelEndpoint *e, jazz_protocol_SyncMessage message);
/* try_enqueue: on error, the rejected message is handed back in *rejected */
jz_err jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__try_enqueue(jazz_db_AuxiliaryChannelEndpoint *e, jazz_protocol_SyncMessage message, jazz_protocol_SyncMessage *rejected);
jz_err jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__peek_outbound(jazz_db_AuxiliaryChannelEndpoint *e, bool *has, jz_bytes *out);
jz_err jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__accept_outbound(jazz_db_AuxiliaryChannelEndpoint *e, bool *has, jazz_protocol_SyncMessage *out);
bool jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__has_pending_outbound(const jazz_db_AuxiliaryChannelEndpoint *e);
bool jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__poll_outbound_ready(jazz_db_AuxiliaryChannelEndpoint *e, const jz_waker *cx);
jz_err jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__receive(jazz_db_AuxiliaryChannelEndpoint *e, jazz_protocol_WireChannelEnvelope frame, u32 encoded_len, bool *has, jazz_protocol_SyncMessage *out);
#endif
