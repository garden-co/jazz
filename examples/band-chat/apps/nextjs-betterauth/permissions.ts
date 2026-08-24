import { definePermissions, type RowContext } from "jazz-tools/permissions";
import { app, type Room } from "./schema.js";

export default definePermissions(app, ({ policy, session, allOf, anyOf, allowedTo }) => {
  const isMember = (room: RowContext<Room>) =>
    policy.roomMembers.exists.where({ roomId: room.id, userId: session.user_id });
  const isLocalFirst = session.where({ authMode: "local-first" });

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
  // Better Auth provisions external profiles through its trusted backend
  // route. The standalone local-first variant remains self-contained.
  policy.profiles.allowInsert.where(allOf([isLocalFirst, { userId: session.user_id }]));
  policy.profiles.allowUpdate
    .whereOld(allOf([isLocalFirst, { userId: session.user_id }]))
    .whereNew({ userId: session.user_id });
  policy.profiles.allowDelete.where(allOf([isLocalFirst, { userId: session.user_id }]));

  // The creator must be able to recover the room before its bootstrap
  // membership row has replicated; other identities require membership.
  policy.rooms.allowRead.where((room) => anyOf([{ $createdBy: session.author }, isMember(room)]));
  policy.rooms.allowInsert.always();
  policy.rooms.allowUpdate
    .whereOld({ $createdBy: session.author })
    .whereNew({ $createdBy: session.author });
  policy.rooms.allowDelete.where({ $createdBy: session.author });

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

  policy.reactions.allowRead.where(allowedTo.read("messageId"));
  // A reaction author must be a current reader of the referenced message. Do
  // not delegate to message insertion: that policy compares a new message's
  // sender profile and has no meaningful operand for an existing message ref.
  policy.reactions.allowInsert.where(
    allOf([allowedTo.read("messageId"), { userId: session.user_id }]),
  );
  policy.reactions.allowUpdate.never();
  policy.reactions.allowDelete.where({ userId: session.user_id });
});
