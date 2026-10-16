/* routed_messages.h: crates/jazz/layers/db/src/db/routed_messages.rs */
#ifndef JAZZ_DB_DB_ROUTED_MESSAGES_H
#define JAZZ_DB_DB_ROUTED_MESSAGES_H
#include "../_ext_db-b.h"

typedef struct jazz_db_ReceivedSyncMessage {
    jazz_protocol_SyncMessage message;
    jazz_protocol_BufferLease lease; /* Option */
    bool receipts_validated;
} jazz_db_ReceivedSyncMessage;
void jazz_db__db__routed_messages__ReceivedSyncMessage__unleased(jazz_db_ReceivedSyncMessage *out, jazz_protocol_SyncMessage message);
void jazz_db__db__routed_messages__ReceivedSyncMessage__drop(void *p);

typedef struct jazz_db_RoutedPending {
    u16 channel;
    u64 epoch, ordinal;
    bool has_predecessors;
    JZ_VEC(struct { u16 slot; u64 n; }) predecessors;
    jazz_protocol_SyncMessage message;
    jazz_protocol_BufferLease lease;
    bool receipts_validated;
} jazz_db_RoutedPending;

typedef struct jazz_db_RoutedMessages {
    u64 sent_epoch;
    u64 sent[JAZZ_WIRE_MAX_CHANNELS];
    u64 receive_epoch;
    u64 received[JAZZ_WIRE_MAX_CHANNELS];
    u64 admitted[JAZZ_WIRE_MAX_CHANNELS];
    jz_deque pending; /* RoutedPending */
    jz_deque ready;   /* ReceivedSyncMessage */
} jazz_db_RoutedMessages;

#define JAZZ_DB_ROUTED_ENVELOPE_VERSION 1
#define JAZZ_DB_ROUTED_MAX_ENVELOPE_OVERHEAD 1024u
void jazz_db__db__routed_messages__RoutedMessages__default(jazz_db_RoutedMessages *r);
void jazz_db__db__routed_messages__RoutedMessages__drop(jazz_db_RoutedMessages *r);
u32 jazz_db__db__routed_messages__max_routed_payload_bytes(void);
jz_err jazz_db__db__routed_messages__RoutedMessages__prepare(const jazz_db_RoutedMessages *r, u16 channel, bool barrier, jz_bytes payload, jz_bytes *out);
void jazz_db__db__routed_messages__RoutedMessages__accepted(jazz_db_RoutedMessages *r, u16 channel, bool barrier);
jz_err jazz_db__db__routed_messages__RoutedMessages__receive(jazz_db_RoutedMessages *r, u16 channel, u32 class_, jz_bytes bytes, jazz_protocol_BufferLease lease, const jazz_protocol_WireInboundContext *context);
bool jazz_db__db__routed_messages__RoutedMessages__pop(jazz_db_RoutedMessages *r, jazz_db_ReceivedSyncMessage *out);
#endif
