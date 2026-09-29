import { definePermissions, type RowContext, type RowRefValue } from "jazz-tools/permissions";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";
import { app, type Reaction, type Room } from "./schema";

const bandChatPermissions = definePermissions(
  app,
  ({ policy, session, allOf, anyOf, allowedTo }) => {
    const me = session.user.account;
    const isMember = (room: RowContext<Room>) =>
      policy.roomMembers.exists.where({ roomId: room.id, memberAuthor: me });
    const isMemberOf = (roomId: RowRefValue) =>
      policy.roomMembers.exists.where({ roomId, memberAuthor: me });
    // Admission is explicit about the creator rather than inheriting "may
    // update the room": members may also bump the room's last activity.
    const isCreatorOf = (roomId: RowRefValue) =>
      policy.rooms.exists.where({ id: roomId, "$createdBy.account": me });
    const canMutateReaction = (reaction: RowContext<Reaction>) =>
      allOf([
        { author: me },
        policy.messages.exists.where({ id: reaction.messageId, roomId: reaction.roomId }),
        isMemberOf(reaction.roomId),
      ]);

    // Profiles are visible to their owner, to anyone who can read a message
    // they sent or a membership that names them (co-members), and to a room
    // creator who can read their join request.
    policy.profiles.allowRead.where(
      anyOf([
        { author: me },
        allowedTo.read("messagesViaSender"),
        allowedTo.read("membershipsViaProfile"),
        allowedTo.read("joinRequestsViaProfile"),
      ]),
    );
    policy.profiles.allowInsert.where({ author: me });
    policy.profiles.allowUpdate.whereOld({ author: me }).whereNew({ author: me });
    policy.profiles.allowDelete.where({ author: me });

    // The room creator has a short local bootstrap window before its own
    // membership row is visible; every other identity must already be a member.
    policy.rooms.allowRead.where((room) => anyOf([{ "$createdBy.account": me }, isMember(room)]));
    policy.rooms.allowInsert.always();
    // The creator may rename the room. Any member may record new activity, but
    // an `exists` check against the stored row keeps the name unchanged.
    // `lastActivityAt` itself is not bounded: a member may write any time,
    // including a future one (documented in the README).
    policy.rooms.allowUpdate
      .whereOld((room) => anyOf([{ "$createdBy.account": me }, isMember(room)]))
      .whereNew((room) =>
        anyOf([
          { "$createdBy.account": me },
          allOf([isMember(room), policy.rooms.exists.where({ id: room.id, name: room.name })]),
        ]),
      );
    policy.rooms.allowDelete.where({ "$createdBy.account": me });

    policy.roomMembers.allowRead.where(allowedTo.read("room"));
    // Only the room creator can admit members. In particular, no rule permits an
    // identity to insert its own membership into someone else's room.
    //
    // Every membership names the admitted account's own profile, and that
    // account must be the creator or have asked to join this room. Knowing an account id is therefore not enough to put someone in
    // a room. The admission may delete the request in the same transaction:
    // the check sees the committed request (INV-RLS-9).
    //
    // Adding people the creator already shares another room with, without a
    // request, would need `allowedTo.read("memberProfile")` here, which is
    // denied even when the creator can read the profile (reported upstream).
    policy.roomMembers.allowInsert.where((member) =>
      allOf([
        isCreatorOf(member.roomId),
        { memberProfileId: { isNull: false } },
        policy.profiles.exists.where({
          id: member.memberProfileId,
          author: member.memberAuthor,
        }),
        anyOf([
          { memberAuthor: me },
          policy.joinRequests.exists.where({
            roomId: member.roomId,
            requester: member.memberAuthor,
          }),
        ]),
      ]),
    );
    policy.roomMembers.allowUpdate.never();
    // The creator removes members; a member may also leave on their own.
    policy.roomMembers.allowDelete.where((member) =>
      anyOf([isCreatorOf(member.roomId), { memberAuthor: me }]),
    );

    policy.joinRequests.allowRead.where((request) =>
      anyOf([{ requester: me }, isCreatorOf(request.roomId)]),
    );
    policy.joinRequests.allowInsert.where((request) =>
      allOf([
        { requester: me },
        policy.profiles.exists.where({ id: request.profileId, author: me }),
      ]),
    );
    policy.joinRequests.allowUpdate.never();
    policy.joinRequests.allowDelete.where((request) =>
      anyOf([{ requester: me }, isCreatorOf(request.roomId)]),
    );

    policy.readMarkers.allowRead.where({ reader: me });
    policy.readMarkers.allowInsert.where((marker) =>
      allOf([{ reader: me }, isMemberOf(marker.roomId)]),
    );
    policy.readMarkers.allowUpdate
      .whereOld({ reader: me })
      .whereNew((marker) => allOf([{ reader: me }, isMemberOf(marker.roomId)]));
    policy.readMarkers.allowDelete.where({ reader: me });

    policy.messages.allowRead.where((message) => isMemberOf(message.roomId));
    policy.messages.allowInsert.where((message) =>
      allOf([
        isMemberOf(message.roomId),
        policy.profiles.exists.where({ id: message.senderId, author: me }),
        anyOf([
          { canvasId: { isNull: true } },
          policy.canvases.exists.where({ id: message.canvasId, roomId: message.roomId }),
        ]),
      ]),
    );
    policy.messages.allowUpdate.never();
    policy.messages.allowDelete.where((message) =>
      policy.profiles.exists.where({ id: message.senderId, author: me }),
    );

    policy.reactions.allowRead.where(allowedTo.read("message"));
    // `roomId` is a denormalized authorization carrier: matching it against
    // both the referenced message and current membership is equivalent to the
    // message read policy without trusting a caller-supplied room id alone.
    policy.reactions.allowInsert.where(canMutateReaction);
    policy.reactions.allowUpdate.never();
    policy.reactions.allowDelete.where(canMutateReaction);

    policy.canvases.allowRead.where((canvas) => isMemberOf(canvas.roomId));
    policy.canvases.allowInsert.where((canvas) => isMemberOf(canvas.roomId));
    policy.canvases.allowUpdate.never();
    policy.canvases.allowDelete.never();

    // `roomId` is again a carrier: it must match the canvas and a current
    // membership, so a removed member can no longer draw.
    policy.strokes.allowRead.where((stroke) => isMemberOf(stroke.roomId));
    policy.strokes.allowInsert.where((stroke) =>
      allOf([
        { author: me },
        isMemberOf(stroke.roomId),
        policy.canvases.exists.where({ id: stroke.canvasId, roomId: stroke.roomId }),
      ]),
    );
    policy.strokes.allowUpdate.never();
    policy.strokes.allowDelete.where((stroke) =>
      allOf([{ author: me }, isMemberOf(stroke.roomId)]),
    );
  },
);

export default { ...betterAuthPermissions, ...bandChatPermissions };
