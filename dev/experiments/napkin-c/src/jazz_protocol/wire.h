/* wire.h: jazz_protocol::wire and its submodules channels / channel_credit / stream_backend. */
#ifndef JAZZ_PROTOCOL_WIRE_H
#define JAZZ_PROTOCOL_WIRE_H
#include "jz_rt.h"
#include "jazz_protocol/protocol.h"
#include "jazz_compression/stream.h"

#define JAZZ_WIRE_PROTOCOL_VERSION 6u
typedef u64 jazz_protocol_WireFeatures;
#define JAZZ_FEATURE_NONE 0ull
#define JAZZ_FEATURE_PAYLOAD_LZ4 (1ull << 3)
#define JAZZ_FEATURE_PAYLOAD_ZSTD (1ull << 4)
#define JAZZ_FEATURE_AUTHORIZATION_SCOPE_VIEWS (1ull << 7)
#define JAZZ_FEATURE_AUXILIARY_CHUNKS (1ull << 8)
#define JAZZ_FEATURE_SCOPE_ISOLATED_CLIENT_RELAY (1ull << 9)

/* WireError travels as a jz_err: code = JAZZ_WIRE_ERR | WireErrorCode << 4 | WireRetry, msg = message. */
enum { JAZZ_WIRE_ERR = 0x0500, JAZZ_TRANSPORT_BACKPRESSURE = 0x0580, JAZZ_TRANSPORT_FAILED, JAZZ_WIRE_STR_ERR /* Result<_, String> */ };
enum { JAZZ_WEC_UNSUPPORTED_PROTOCOL_VERSION = 0, JAZZ_WEC_UNSUPPORTED_FEATURE, JAZZ_WEC_MALFORMED_FRAME, JAZZ_WEC_AUTH_FAILED,
       JAZZ_WEC_BACKPRESSURE, JAZZ_WEC_INTERNAL, JAZZ_WEC_NOT_READY };
enum { JAZZ_RETRY_NEVER = 0, JAZZ_RETRY_AFTER_AUTH, JAZZ_RETRY_AFTER_RESUME, JAZZ_RETRY_LATER };
typedef struct jazz_protocol_WireError { u32 code; u32 retry; jz_str message; } jazz_protocol_WireError;
jz_err jazz_protocol__wire__WireError__new(u32 code, u32 retry, jz_str message);
void jazz_protocol__wire__WireError__from_err(const jz_err *e, jazz_protocol_WireError *out);
bool jazz_protocol__wire__WireError__asks_reconnect_later(const jazz_protocol_WireError *e);
void jazz_protocol__wire__WireError__Debug__fmt(jz_str *out, const jazz_protocol_WireError *e);
void jazz_protocol__wire__WireErrorCode__Debug__fmt(jz_str *out, u32 code);
void jazz_protocol__wire__WireRetry__Debug__fmt(jz_str *out, u32 retry);
/* <postcard::Error as Display>::fmt for a postcard-flavoured jz_err (C helper shared by the wire modules) */
void jazz_protocol__wire__postcard_error_display(jz_str *out, const jz_err *e);

/* ---------------- channels ---------------- */
#define JAZZ_MAX_CHANNELS 64u
#define JAZZ_CHANNEL_CHUNK_BYTES (64u * 1024u)
#define JAZZ_MAX_CHANNEL_FRAME_PAYLOAD (JAZZ_CHANNEL_CHUNK_BYTES + JAZZ_CHANNEL_CHUNK_BYTES / 10u + 64u)
#define JAZZ_CONTROL_RESERVE_BYTES (1024u * 1024u)
#define JAZZ_INTERACTIVE_RESERVE_BYTES (8u * 1024u * 1024u)
#define JAZZ_MAX_CHANNEL_BUFFER_BYTES (2u * JAZZ_MAX_LOGICAL_MESSAGE_BYTES + JAZZ_INTERACTIVE_RESERVE_BYTES + JAZZ_CONTROL_RESERVE_BYTES)
#define JAZZ_MAX_CHANNEL_QUEUED_MESSAGES 1024u
#define JAZZ_CONTROL_RESERVE_MESSAGES 8u
#define JAZZ_CHANNEL_HEADER_BYTES 64u
#define JAZZ_CONTROL_CHANNEL 0u
#define JAZZ_AUXILIARY_CHANNEL 63u
#define JAZZ_PROGRESS_CHANNEL 62u
typedef enum { JAZZ_CC_CONTROL = 0, JAZZ_CC_REQUESTS = 1, JAZZ_CC_DELIVERY = 2, JAZZ_CC_WRITES = 3,
               JAZZ_CC_LARGE_VALUE = 4, JAZZ_CC_AUXILIARY = 5, JAZZ_CC_PROGRESS = 6 } jazz_protocol_ChannelClass;
typedef struct jazz_protocol_ChannelFrame {
    u16 channel; u64 generation; u64 sequence; u32 class_; bool first, last;
    u32 message_len; u32 decoded_len; jz_bytes payload;
} jazz_protocol_ChannelFrame;
jz_err jazz_protocol__wire__channels__ChannelFrame__validate(const jazz_protocol_ChannelFrame *f);
jz_err jazz_protocol__wire__channels__ChannelFrame__encode(const jazz_protocol_ChannelFrame *f, jz_bytes *out);
jz_err jazz_protocol__wire__channels__ChannelFrame__decode(jz_slice bytes, jazz_protocol_ChannelFrame *out);
void jazz_protocol__wire__channels__ChannelFrame__Serialize__serialize(jz_pc_w *w, const jazz_protocol_ChannelFrame *f);
jz_err jazz_protocol__wire__channels__ChannelFrame__Deserialize__deserialize(jz_pc_r *r, jazz_protocol_ChannelFrame *out);

typedef struct jazz_protocol_ChannelMessage { u8 *bytes; u32 len; u32 offset; } jazz_protocol_ChannelMessage;
typedef struct jazz_protocol_OutboundChannel {
    u32 class_; u64 generation; u64 sequence; usize bytes; jz_deque messages; /* ChannelMessage */
} jazz_protocol_OutboundChannel;
typedef struct jazz_protocol_ScheduledChunk {
    u16 channel; u64 generation; u64 sequence; u32 class_; bool first, last; u32 message_len; jz_slice bytes;
} jazz_protocol_ScheduledChunk;
typedef struct jazz_protocol_ChannelScheduler {
    jz_btree channels; /* u16 -> OutboundChannel */
    u32 round_cursor;
    u16 class_cursor[7];
    bool has_selected; u16 selected;
    usize bytes, data_bytes, messages, data_messages, bulk_bytes, progress_bytes, progress_messages;
} jazz_protocol_ChannelScheduler;
void jazz_protocol__wire__channels__ChannelScheduler__default(jazz_protocol_ChannelScheduler *s);
void jazz_protocol__wire__channels__ChannelScheduler__drop(jazz_protocol_ChannelScheduler *s);
jz_err jazz_protocol__wire__channels__ChannelScheduler__enqueue(jazz_protocol_ChannelScheduler *s, u16 channel, u64 generation, u32 class_, jz_bytes payload);
typedef bool (*jazz_protocol_HasCreditFn)(u32 class_, void *ctx);
bool jazz_protocol__wire__channels__ChannelScheduler__next_chunk_where(jazz_protocol_ChannelScheduler *s, jazz_protocol_HasCreditFn has_credit, void *ctx, jazz_protocol_ScheduledChunk *out);
bool jazz_protocol__wire__channels__ChannelScheduler__next_chunk(jazz_protocol_ChannelScheduler *s, jazz_protocol_ScheduledChunk *out);
jz_err jazz_protocol__wire__channels__ChannelScheduler__accepted(jazz_protocol_ChannelScheduler *s);
jz_err jazz_protocol__wire__channels__ChannelScheduler__next_idle_generation(const jazz_protocol_ChannelScheduler *s, u16 channel, u64 *out);
bool jazz_protocol__wire__channels__ChannelScheduler__is_idle(const jazz_protocol_ChannelScheduler *s, u16 channel);
usize jazz_protocol__wire__channels__ChannelScheduler__queued_bytes(const jazz_protocol_ChannelScheduler *s);
usize jazz_protocol__wire__channels__ChannelScheduler__queued_messages(const jazz_protocol_ChannelScheduler *s);

/* ---------------- wire frames ---------------- */
typedef struct jazz_protocol_WireSession {
    jz_str session_id; u64 epoch; bool has_identity; jazz_types_AuthorSubject identity;
} jazz_protocol_WireSession;
void jazz_protocol__wire__WireSession__Clone__clone(jazz_protocol_WireSession *dst, const jazz_protocol_WireSession *src);
void jazz_protocol__wire__WireSession__drop(jazz_protocol_WireSession *s);
void jazz_protocol__wire__WireSession__Debug__fmt(jz_str *out, const jazz_protocol_WireSession *s);
enum { JAZZ_CREDIT_FRAMES = 0, JAZZ_CREDIT_MESSAGES = 1 };
typedef struct jazz_protocol_WireCreditKind { u32 tag; u32 count; bool bulk; } jazz_protocol_WireCreditKind;
typedef struct jazz_protocol_WireChannelCredit {
    u16 protocol_version; jazz_protocol_WireFeatures features;
    bool has_session; jazz_protocol_WireSession session;
    u32 class_; u64 sequence; u64 consumed_bytes; jazz_protocol_WireCreditKind kind;
} jazz_protocol_WireChannelCredit;
typedef struct jazz_protocol_WireChannelEnvelope {
    u16 protocol_version; jazz_protocol_WireFeatures features;
    bool has_session; jazz_protocol_WireSession session;
    jazz_protocol_ChannelFrame extent;
} jazz_protocol_WireChannelEnvelope;
void jazz_protocol__wire__WireChannelEnvelope__drop(jazz_protocol_WireChannelEnvelope *e);
enum { JAZZ_ROLE_CLIENT = 0, JAZZ_ROLE_CORE, JAZZ_ROLE_RELAY };
typedef struct jazz_protocol_WireAuthorityEndpoint { jazz_types_NodeUuid node; u64 epoch; } jazz_protocol_WireAuthorityEndpoint;
typedef struct jazz_protocol_WireHello {
    u16 min_protocol_version, max_protocol_version; jazz_protocol_WireFeatures features; u32 role;
    bool has_authority; jazz_protocol_WireAuthorityEndpoint authority;
} jazz_protocol_WireHello;
enum { JAZZ_WF_HELLO = 0, JAZZ_WF_ERROR, JAZZ_WF_CHANNEL, JAZZ_WF_CHANNEL_CREDIT };
typedef struct jazz_protocol_WireFrame {
    u32 tag;
    union {
        jazz_protocol_WireHello hello;
        jazz_protocol_WireError error;
        jazz_protocol_WireChannelEnvelope channel;
        jazz_protocol_WireChannelCredit credit;
    } u;
} jazz_protocol_WireFrame;
void jazz_protocol__wire__WireFrame__drop(jazz_protocol_WireFrame *f);
void jazz_protocol__wire__WireFrame__Serialize__serialize(jz_pc_w *w, const jazz_protocol_WireFrame *f);
jz_err jazz_protocol__wire__WireFrame__Deserialize__deserialize(jz_pc_r *r, jazz_protocol_WireFrame *out);
typedef struct jazz_protocol_WireNegotiated { u16 protocol_version; jazz_protocol_WireFeatures features; } jazz_protocol_WireNegotiated;
enum { JAZZ_WC_NONE = 0, JAZZ_WC_LZ4, JAZZ_WC_ZSTD };
u32 jazz_protocol__wire__WireCompression__from_features(jazz_protocol_WireFeatures f);
jazz_protocol_WireFeatures jazz_protocol__wire__WireCompression__feature(u32 c);
jazz_protocol_WireAuthorityEndpoint jazz_protocol__wire__WireAuthorityEndpoint__fresh(jazz_types_NodeUuid node);
void jazz_protocol__wire__WireHello__current(jazz_protocol_WireHello *out, u32 role, jazz_protocol_WireFeatures features);
void jazz_protocol__wire__WireHello__with_authority(jazz_protocol_WireHello *h, jazz_types_NodeUuid node, u64 epoch);

typedef struct jazz_protocol_WireInboundContext {
    u16 expected_protocol_version;
    jazz_protocol_WireFeatures negotiated_features;
    bool has_expected_session; jazz_protocol_WireSession expected_session;
    bool trusted_encoder;
} jazz_protocol_WireInboundContext;
void jazz_protocol__wire__WireInboundContext__new(jazz_protocol_WireInboundContext *out, u16 version, jazz_protocol_WireFeatures features, const jazz_protocol_WireSession *session_moved);
void jazz_protocol__wire__WireInboundContext__Clone__clone(jazz_protocol_WireInboundContext *dst, const jazz_protocol_WireInboundContext *src);
void jazz_protocol__wire__WireInboundContext__drop(jazz_protocol_WireInboundContext *c);
void jazz_protocol__wire__WireInboundContext__set_trusted_encoder(jazz_protocol_WireInboundContext *c, bool trusted);
bool jazz_protocol__wire__WireInboundContext__validates_receipts(const jazz_protocol_WireInboundContext *c);
jz_err jazz_protocol__wire__WireInboundContext__decode_semantic_payload(const jazz_protocol_WireInboundContext *c, jz_slice bytes, jazz_protocol_SyncMessage *out);
jz_err jazz_protocol__wire__WireInboundContext__validate_channel_metadata(const jazz_protocol_WireInboundContext *c, const jazz_protocol_WireChannelEnvelope *f);
jz_err jazz_protocol__wire__WireInboundContext__decode_frame(const jazz_protocol_WireInboundContext *c, jz_slice bytes, jazz_protocol_WireFrame *out);
jz_err jazz_protocol__wire__WireInboundContext__validate_metadata(const jazz_protocol_WireInboundContext *c, u16 protocol_version, jazz_protocol_WireFeatures features, const jazz_protocol_WireSession *session);

jz_err jazz_protocol__wire__encode_frame(const jazz_protocol_WireFrame *f, jz_bytes *out);
bool jazz_protocol__wire__is_channel_credit_frame(jz_slice bytes);
jz_err jazz_protocol__wire__decode_frame(jz_slice bytes, jazz_protocol_WireFrame *out);
jz_err jazz_protocol__wire__encode_sync_message(const jazz_protocol_SyncMessage *m, jz_bytes *out);
jz_err jazz_protocol__wire__encoded_sync_message_len(const jazz_protocol_SyncMessage *m, usize *out);
jz_err jazz_protocol__wire__encode_sync_message_for_features(const jazz_protocol_SyncMessage *m, jazz_protocol_WireFeatures f, jz_bytes *out);
jz_err jazz_protocol__wire__decode_sync_message(jz_slice bytes, jazz_protocol_SyncMessage *out);
jz_err jazz_protocol__wire__decode_sync_message_trusted(jz_slice bytes, jazz_protocol_SyncMessage *out);
typedef JZ_VEC(jz_bytes) jazz_protocol_BytesVec;
jz_err jazz_protocol__wire__decode_websocket_frame_batch(jz_slice bytes, jazz_protocol_BytesVec *out);
jz_err jazz_protocol__wire__encode_websocket_frame_batch(const jz_bytes *frames, u32 n, jz_bytes *out);
jz_err jazz_protocol__wire__decode_sync_message_for_features(jz_slice bytes, jazz_protocol_WireFeatures f, jazz_protocol_SyncMessage *out);
jz_err jazz_protocol__wire__decode_sync_message_for_receive(jz_slice bytes, jazz_protocol_SyncMessage *out);
jz_err jazz_protocol__wire__ensure_sync_message_features(const jazz_protocol_SyncMessage *m, jazz_protocol_WireFeatures f);
jazz_protocol_WireFeatures jazz_protocol__wire__runtime_transport_compression_features(void);
jazz_protocol_WireFeatures jazz_protocol__wire__current_wire_features(void);
bool jazz_protocol__wire__wire_hello_version_range(jz_slice bytes, u16 *min, u16 *max);
jz_err jazz_protocol__wire__decode_hello_frame(jz_slice bytes, jazz_protocol_WireHello *out);
jz_err jazz_protocol__wire__negotiate_wire(const jazz_protocol_WireHello *remote, jazz_protocol_WireFeatures local, jazz_protocol_WireNegotiated *out);
/* trait WireTransport -> vtable */
typedef struct jazz_protocol_WireTransportVtbl {
    jz_err (*send_frame)(void *self, jz_bytes frame); /* TransportError as jz_err */
    bool (*try_recv_frame)(void *self, jz_bytes *out);
} jazz_protocol_WireTransportVtbl;
typedef struct jazz_protocol_WireTransport { const jazz_protocol_WireTransportVtbl *vt; void *self; } jazz_protocol_WireTransport;
void jazz_protocol__wire__TransportError__Debug__fmt(jz_str *out, const jz_err *e);

/* ---------------- channel_credit ---------------- */
#define JAZZ_CHANNEL_FRAME_CREDIT_FLOOR (16u * 1024u)
usize jazz_protocol__wire__channel_credit__channel_frame_credit_cost(usize encoded_len);
typedef struct jazz_protocol_BufferCost { usize bytes; usize count; } jazz_protocol_BufferCost;
typedef struct jazz_protocol_ChannelCredits jazz_protocol_ChannelCredits;
/* Weak<Mutex<ChannelCredits>>: an rc cell the owner clears when it is dropped. */
typedef struct jazz_protocol_CreditsWeakCell { jazz_protocol_ChannelCredits *target; } jazz_protocol_CreditsWeakCell;
struct jazz_protocol_ChannelCredits {
    jazz_protocol_WireInboundContext context;
    usize outstanding[6];
    usize pending_grants[6];
    u64 next_sent, next_received;
    bool has_pending; jz_bytes pending_frame; u32 pending_index; usize pending_amount; jazz_protocol_WireCreditKind pending_kind;
    jazz_protocol_BufferCost sent_buffers[6], received_buffers[6], buffer_grants[6];
    bool closed;
    bool has_waker; jz_waker waker;
    jazz_protocol_CreditsWeakCell *weak; /* rc */
};
/* SharedChannelCredits = Arc<Mutex<ChannelCredits>>: jz_rc payload pointer */
typedef jazz_protocol_ChannelCredits *jazz_protocol_SharedChannelCredits;
typedef struct jazz_protocol_BufferLeaseInner { jazz_protocol_CreditsWeakCell *owner; u32 bucket; usize bytes; } jazz_protocol_BufferLeaseInner;
typedef jazz_protocol_BufferLeaseInner *jazz_protocol_BufferLease; /* Arc<BufferLeaseInner>: jz_rc payload */
jazz_protocol_SharedChannelCredits jazz_protocol__wire__channel_credit__ChannelCredits__new_shared(const jazz_protocol_WireInboundContext *ctx);
void jazz_protocol__wire__channel_credit__ChannelCredits__new(jazz_protocol_ChannelCredits *out, const jazz_protocol_WireInboundContext *ctx);
bool jazz_protocol__wire__channel_credit__ChannelCredits__can_reserve_message(const jazz_protocol_ChannelCredits *c, u32 class_, usize bytes);
void jazz_protocol__wire__channel_credit__ChannelCredits__reserve_message(jazz_protocol_ChannelCredits *c, u32 class_, usize bytes);
jz_err jazz_protocol__wire__channel_credit__ChannelCredits__receive_message(jazz_protocol_SharedChannelCredits owner, u32 class_, usize bytes, jazz_protocol_BufferLease *out);
void jazz_protocol__wire__channel_credit__ChannelCredits__close(jazz_protocol_ChannelCredits *c);
bool jazz_protocol__wire__channel_credit__ChannelCredits__can_send(const jazz_protocol_ChannelCredits *c, u32 class_);
jz_err jazz_protocol__wire__channel_credit__ChannelCredits__charge(jazz_protocol_ChannelCredits *c, u32 class_, usize encoded_len);
jz_err jazz_protocol__wire__channel_credit__ChannelCredits__consumed(jazz_protocol_ChannelCredits *c, u32 class_, usize encoded_len);
jz_err jazz_protocol__wire__channel_credit__ChannelCredits__receive_credit(jazz_protocol_ChannelCredits *c, jazz_protocol_WireChannelCredit grant);
jz_err jazz_protocol__wire__channel_credit__ChannelCredits__peek_grant(jazz_protocol_ChannelCredits *c, bool *some, jz_bytes *out);
jz_err jazz_protocol__wire__channel_credit__ChannelCredits__accept_grant(jazz_protocol_ChannelCredits *c);
bool jazz_protocol__wire__channel_credit__ChannelCredits__has_pending_grant(const jazz_protocol_ChannelCredits *c);
bool jazz_protocol__wire__channel_credit__ChannelCredits__poll_grant_ready(jazz_protocol_ChannelCredits *c, const jz_waker *cx);
void jazz_protocol__wire__channel_credit__ChannelCredits__register_waker(jazz_protocol_ChannelCredits *c, const jz_waker *cx);
jazz_protocol_BufferLease jazz_protocol__wire__channel_credit__BufferLease__clone(jazz_protocol_BufferLease l);
void jazz_protocol__wire__channel_credit__BufferLease__drop(jazz_protocol_BufferLease l);
void jazz_protocol__wire__channel_credit__BufferLease__Debug__fmt(jz_str *out, jazz_protocol_BufferLease l);

/* ---------------- stream_backend ---------------- */
typedef struct jazz_protocol_InboundChannel {
    u64 generation; u64 sequence; u32 class_; u64 active_features;
    bool has_decoder; jazz_compression_StreamDecoder decoder;
    usize message_len; usize encoded_len;
    bool has_started; u64 started_ms;
    bool has_progressed; u64 progressed_ms;
    jz_bytes payload;
    jazz_protocol_BufferLease lease; /* NULL = None */
} jazz_protocol_InboundChannel;
typedef struct jazz_protocol_ReceivedBytes { u16 channel; u32 class_; jz_bytes payload; jazz_protocol_BufferLease lease; } jazz_protocol_ReceivedBytes;
typedef struct jazz_protocol_ChannelEncoderSlot { u64 generation; bool has_encoder; jazz_compression_StreamEncoder encoder; } jazz_protocol_ChannelEncoderSlot;
typedef struct jazz_protocol_OrderedChannelBackend {
    jazz_protocol_WireInboundContext context;
    jazz_protocol_SharedChannelCredits credits;
    jazz_protocol_ChannelScheduler scheduler;
    jz_btree encoders; /* u16 -> ChannelEncoderSlot */
    jz_btree inbound;  /* u16 -> InboundChannel */
    u64 outbound_features;
    usize reserved;
    bool has_pending; jz_bytes pending;
    bool pending_last; u32 pending_class;
    bool has_failed; jz_str failed;
    u64 idle_timeout_ms, age_timeout_ms;
} jazz_protocol_OrderedChannelBackend;
jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__new(const jazz_protocol_WireInboundContext *ctx_moved, jazz_protocol_OrderedChannelBackend *out);
bool jazz_protocol__wire__stream_backend__OrderedChannelBackend__incomplete_receive_timeout_ms(const jazz_protocol_OrderedChannelBackend *b, u64 *out);
jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__expire(jazz_protocol_OrderedChannelBackend *b);
jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__enqueue(jazz_protocol_OrderedChannelBackend *b, u16 channel, u64 generation, u32 class_, jz_bytes payload);
void jazz_protocol__wire__stream_backend__OrderedChannelBackend__set_channel_credits(jazz_protocol_OrderedChannelBackend *b, jazz_protocol_SharedChannelCredits c);
jazz_protocol_SharedChannelCredits jazz_protocol__wire__stream_backend__OrderedChannelBackend__channel_credits(const jazz_protocol_OrderedChannelBackend *b);
bool jazz_protocol__wire__stream_backend__OrderedChannelBackend__is_idle(const jazz_protocol_OrderedChannelBackend *b, u16 channel);
jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__next_idle_generation(const jazz_protocol_OrderedChannelBackend *b, u16 channel, u64 *out);
bool jazz_protocol__wire__stream_backend__OrderedChannelBackend__has_pending(const jazz_protocol_OrderedChannelBackend *b);
jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__peek_outbound(jazz_protocol_OrderedChannelBackend *b, bool *some, jz_bytes *out);
jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__accept_outbound(jazz_protocol_OrderedChannelBackend *b, bool *last);
jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__receive(jazz_protocol_OrderedChannelBackend *b, jazz_protocol_WireChannelEnvelope frame, usize encoded_len, bool *some, jazz_protocol_ReceivedBytes *out);
void jazz_protocol__wire__stream_backend__OrderedChannelBackend__Drop__drop(jazz_protocol_OrderedChannelBackend *b);

#endif
