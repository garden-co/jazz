import { definePermissions, type RowContext } from "jazz-tools/permissions";
import { app, type Room } from "./schema.js";

export default definePermissions(app, ({ policy, session }) => {
  const isMember = (room: RowContext<Room>) =>
    policy.roomMembers.exists.where({ roomId: room.id, userId: session.user_id });

  policy.profiles.allowRead.where({});
  policy.profiles.allowInsert.where({ userId: session.user_id });
  policy.profiles.allowUpdate
    .whereOld({ userId: session.user_id })
    .whereNew({ userId: session.user_id });
  policy.profiles.allowDelete.where({ userId: session.user_id });

  policy.rooms.allowRead.where(isMember);
  policy.rooms.allowInsert.where({ createdBy: session.user_id });
  policy.rooms.allowUpdate.whereOld((room) => isMember(room)).whereNew((room) => isMember(room));
  policy.rooms.allowDelete.where(isMember);

  policy.roomMembers.allowRead.where((member) =>
    policy.roomMembers.exists.where({ roomId: member.roomId, userId: session.user_id }),
  );
  policy.roomMembers.allowInsert.where({ userId: session.user_id });
  policy.roomMembers.allowDelete.where({ userId: session.user_id });

  policy.messages.allowRead.where((message) =>
    policy.roomMembers.exists.where({ roomId: message.roomId, userId: session.user_id }),
  );
  policy.messages.allowInsert.where((message) =>
    policy.roomMembers.exists.where({ roomId: message.roomId, userId: session.user_id }),
  );
  policy.messages.allowDelete.where((message) =>
    policy.profiles.exists.where({ id: message.senderId, userId: session.user_id }),
  );
});
