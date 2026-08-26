import { definePermissions, type RowContext } from "jazz-tools/permissions";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";
import { app, type Room } from "./schema";

const bandChatPermissions = definePermissions(
  app,
  ({ policy, session, allOf, anyOf, allowedTo }) => {
    const isMember = (room: RowContext<Room>) =>
      policy.roomMembers.exists.where({ roomId: room.id, userId: session.user_id });

    policy.profiles.allowRead.where({});
    policy.profiles.allowInsert.where({ userId: session.user_id });
    policy.profiles.allowUpdate
      .whereOld({ userId: session.user_id })
      .whereNew({ userId: session.user_id });
    policy.profiles.allowDelete.where({ userId: session.user_id });

    // The room creator has a short local bootstrap window before its own
    // membership row is visible; every other identity must already be a member.
    policy.rooms.allowRead.where((room) => anyOf([{ $createdBy: session.author }, isMember(room)]));
    policy.rooms.allowInsert.always();
    policy.rooms.allowUpdate
      .whereOld({ $createdBy: session.author })
      .whereNew({ $createdBy: session.author });
    policy.rooms.allowDelete.where({ $createdBy: session.author });

    policy.roomMembers.allowRead.where(allowedTo.read("roomId"));
    // Only an editor of a room (its creator here) can admit or remove members.
    // In particular, no rule permits an identity to insert its own membership.
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
  },
);

export default { ...betterAuthPermissions, ...bandChatPermissions };
