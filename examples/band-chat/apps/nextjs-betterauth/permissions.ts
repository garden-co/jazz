import { definePermissions, type RowContext } from "jazz-tools/permissions";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";
import { app, type Room } from "./schema";

const bandChatPermissions = definePermissions(
  app,
  ({ policy, session, allOf, anyOf, allowedTo }) => {
    const isMember = (room: RowContext<Room>) =>
      policy.roomMembers.exists.where({ roomId: room.id, memberAuthor: session.author });

    policy.profiles.allowRead.where({ author: session.author });
    policy.profiles.allowInsert.where({ author: session.author });
    policy.profiles.allowUpdate
      .whereOld({ author: session.author })
      .whereNew({ author: session.author });
    policy.profiles.allowDelete.where({ author: session.author });

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
      policy.roomMembers.exists.where({ roomId: message.roomId, memberAuthor: session.author }),
    );
    policy.messages.allowInsert.where((message) =>
      allOf([
        policy.roomMembers.exists.where({ roomId: message.roomId, memberAuthor: session.author }),
        policy.profiles.exists.where({ id: message.senderId, author: session.author }),
      ]),
    );
    policy.messages.allowUpdate.never();
    policy.messages.allowDelete.where((message) =>
      policy.profiles.exists.where({ id: message.senderId, author: session.author }),
    );
  },
);

export default { ...betterAuthPermissions, ...bandChatPermissions };
