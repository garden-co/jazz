import { definePermissions, type RowContext } from "jazz-tools/permissions";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";
import { app, type Room } from "./schema";

const bandChatPermissions = definePermissions(
  app,
  ({ policy, session, allOf, anyOf, allowedTo }) => {
    const isMember = (room: RowContext<Room>) =>
      policy.roomMembers.exists.where({ roomId: room.id, memberAuthor: session.user });

    policy.profiles.allowRead.where({ author: session.user });
    policy.profiles.allowInsert.where({ author: session.user });
    policy.profiles.allowUpdate
      .whereOld({ author: session.user })
      .whereNew({ author: session.user });
    policy.profiles.allowDelete.where({ author: session.user });

    // The room creator has a short local bootstrap window before its own
    // membership row is visible; every other identity must already be a member.
    policy.rooms.allowRead.where((room) => anyOf([{ $createdBy: session.user }, isMember(room)]));
    policy.rooms.allowInsert.always();
    policy.rooms.allowUpdate
      .whereOld({ $createdBy: session.user })
      .whereNew({ $createdBy: session.user });
    policy.rooms.allowDelete.where({ $createdBy: session.user });

    policy.roomMembers.allowRead.where(allowedTo.read("roomId"));
    // Only an editor of a room (its creator here) can admit or remove members.
    // In particular, no rule permits an identity to insert its own membership.
    policy.roomMembers.allowInsert.where(allowedTo.update("roomId"));
    policy.roomMembers.allowUpdate.never();
    policy.roomMembers.allowDelete.where(allowedTo.update("roomId"));

    policy.messages.allowRead.where((message) =>
      policy.roomMembers.exists.where({ roomId: message.roomId, memberAuthor: session.user }),
    );
    policy.messages.allowInsert.where((message) =>
      allOf([
        policy.roomMembers.exists.where({ roomId: message.roomId, memberAuthor: session.user }),
        policy.profiles.exists.where({ id: message.senderId, author: session.user }),
      ]),
    );
    policy.messages.allowUpdate.never();
    policy.messages.allowDelete.where((message) =>
      policy.profiles.exists.where({ id: message.senderId, author: session.user }),
    );
  },
);

export default { ...betterAuthPermissions, ...bandChatPermissions };
