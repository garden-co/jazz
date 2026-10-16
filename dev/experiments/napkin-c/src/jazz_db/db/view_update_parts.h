/* view_update_parts.h: crates/jazz/layers/db/src/db/view_update_parts.rs */
#ifndef JAZZ_DB_DB_VIEW_UPDATE_PARTS_H
#define JAZZ_DB_DB_VIEW_UPDATE_PARTS_H
#include "../_ext_db-b.h"

typedef enum {
    JAZZ_OVU_HEADER_EXCEEDS_LIMIT,
    JAZZ_OVU_SUPPORTING_ROW_EXCEEDS_LIMIT,
    JAZZ_OVU_VERSION_BUNDLE_EXCEEDS_LIMIT,
    JAZZ_OVU_MALFORMED_CARRIER,
} jazz_db_OversizedViewUpdateTag;
typedef struct jazz_db_OversizedViewUpdate { u32 tag; usize bytes; usize bound; } jazz_db_OversizedViewUpdate;
void jazz_db__db__view_update_parts__OversizedViewUpdate__Display__fmt(const jazz_db_OversizedViewUpdate *e, jz_str *f);

typedef JZ_VEC(jazz_protocol_SyncMessage) jazz_db_SyncMessageVec;
/* Ok -> true, *out filled; Err -> false, *err filled */
bool jazz_db__db__view_update_parts__split_view_update(jazz_protocol_ViewUpdatePayload view, usize limit, jazz_db_SyncMessageVec *out, jazz_db_OversizedViewUpdate *err);
typedef JZ_VEC(jazz_protocol_ViewUpdatePayload) jazz_db_ViewUpdatePayloadVec;
jz_err jazz_db__db__view_update_parts__merge_view_update_parts(jazz_db_ViewUpdatePayloadVec parts, jazz_protocol_ViewUpdatePayload last, jazz_protocol_ViewUpdatePayload *out);
#endif
