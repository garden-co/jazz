import { definePermissions, type RowContext } from "jazz-tools/permissions";
import { app, type Room } from "./schema.js";

export default definePermissions(app, ({ policy, session, allOf, anyOf }) => {
  const isMember = (room: RowContext<Room>) =>
    policy.roomMembers.exists.where({ roomId: room.id, userId: session.user_id });

  policy.profiles.allowRead.where({});
  policy.profiles.allowInsert.where({ userId: session.user_id });
  policy.profiles.allowUpdate
    .whereOld({ userId: session.user_id })
    .whereNew({ userId: session.user_id });
  policy.profiles.allowDelete.where({ userId: session.user_id });

  // The creator must be able to recover the room before its bootstrap
  // membership row has replicated; other identities require membership.
  policy.rooms.allowRead.where((room) => anyOf([{ createdBy: session.user_id }, isMember(room)]));
  policy.rooms.allowInsert.where({ createdBy: session.user_id });
  policy.rooms.allowUpdate
    .whereOld({ createdBy: session.user_id })
    .whereNew({ createdBy: session.user_id });
  policy.rooms.allowDelete.where({ createdBy: session.user_id });

  policy.roomMembers.allowRead.where((member) =>
    policy.rooms.exists.where({ id: member.roomId, createdBy: session.user_id }),
  );
  // The creating identity bootstraps its own membership and is the sole
  // admission authority. There is deliberately no self-join rule.
  policy.roomMembers.allowInsert.where((member) =>
    policy.rooms.exists.where({ id: member.roomId, createdBy: session.user_id }),
  );
  policy.roomMembers.allowDelete.where((member) =>
    policy.rooms.exists.where({ id: member.roomId, createdBy: session.user_id }),
  );

  policy.messages.allowRead.where((message) =>
    policy.roomMembers.exists.where({ roomId: message.roomId, userId: session.user_id }),
  );
  policy.messages.allowInsert.where((message) =>
    allOf([
      policy.roomMembers.exists.where({ roomId: message.roomId, userId: session.user_id }),
      policy.profiles.exists.where({ id: message.senderId, userId: session.user_id }),
    ]),
  );
  policy.messages.allowDelete.where((message) =>
    policy.profiles.exists.where({ id: message.senderId, userId: session.user_id }),
  );
});
