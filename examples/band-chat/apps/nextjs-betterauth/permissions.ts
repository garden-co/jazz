import { definePermissions, type RowContext } from "jazz-tools/permissions";
import { app, type Room } from "./schema.js";

export default definePermissions(app, ({ policy, session, allOf, anyOf, allowedTo }) => {
  const isMember = (room: RowContext<Room>) =>
    policy.roomMembers.exists.where({ roomId: room.id, userId: session.user_id });

  // Better Auth persistence is backend-only. State every deny explicitly so
  // this example stays closed even on authorities predating omitted-policy deny.
  for (const authTable of [
    policy.better_auth_user,
    policy.better_auth_session,
    policy.better_auth_account,
    policy.better_auth_verification,
    policy.better_auth_jwks,
  ]) {
    authTable.allowRead.never();
    authTable.allowInsert.never();
    authTable.allowUpdate.never();
    authTable.allowDelete.never();
  }

  policy.profiles.allowRead.where({});
  policy.profiles.allowInsert.where({ userId: session.user_id });
  policy.profiles.allowUpdate
    .whereOld({ userId: session.user_id })
    .whereNew({ userId: session.user_id });
  policy.profiles.allowDelete.where({ userId: session.user_id });

  // The creator must be able to recover the room before its bootstrap
  // membership row has replicated; other identities require membership.
  policy.rooms.allowRead.where((room) => anyOf([{ $createdBy: session.user_id }, isMember(room)]));
  policy.rooms.allowInsert.always();
  policy.rooms.allowUpdate
    .whereOld({ $createdBy: session.user_id })
    .whereNew({ $createdBy: session.user_id });
  policy.rooms.allowDelete.where({ $createdBy: session.user_id });

  policy.roomMembers.allowRead.where(allowedTo.read("roomId"));
  // The creating identity bootstraps its own membership and is the sole
  // admission authority. There is deliberately no self-join rule.
  policy.roomMembers.allowInsert.where(allowedTo.update("roomId"));
  policy.roomMembers.allowUpdate.never();
  policy.roomMembers.allowDelete.where(allowedTo.update("roomId"));

  policy.messages.allowRead.where((message) =>
    policy.roomMembers.exists.where({ roomId: message.roomId, userId: session.user_id }),
  );
  policy.messages.allowInsert.where((message) =>
    allOf([
      policy.roomMembers.exists.where({ roomId: message.roomId, userId: session.user_id }),
      policy.profiles.exists.where({ id: message.senderId, userId: session.user_id }),
    ]),
  );
  policy.messages.allowUpdate.never();
  policy.messages.allowDelete.where((message) =>
    policy.profiles.exists.where({ id: message.senderId, userId: session.user_id }),
  );
});
