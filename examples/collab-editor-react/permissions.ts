import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

// Open-collaboration model, appropriate for a shareable-link demo:
//  - Anyone can READ any row (so anyone with a room's link can collaborate).
//  - You may only INSERT rows attributed to your own session user_id.
//  - Room metadata (title / language) is editable by any collaborator.
//
// A production app would tighten reads so that only actual room members can
// read a room's documents instead of allowing open reads.
export default s.definePermissions(app, ({ policy, session }) => {
  policy.rooms.allowRead.where({});
  policy.rooms.allowInsert.where({ creator_session_user_id: session.user_id });
  policy.rooms.allowUpdate.always();

  policy.roomParticipants.allowRead.where({});
  policy.roomParticipants.allowInsert.where({ session_user_id: session.user_id });
  policy.roomParticipants.allowUpdate
    .whereOld({ session_user_id: session.user_id })
    .whereNew({ session_user_id: session.user_id });

  policy.roomYjsUpdates.allowRead.where({});
  policy.roomYjsUpdates.allowInsert.where({ session_user_id: session.user_id });

  policy.roomYjsSnapshots.allowRead.where({});
  policy.roomYjsSnapshots.allowInsert.where({ session_user_id: session.user_id });
});
