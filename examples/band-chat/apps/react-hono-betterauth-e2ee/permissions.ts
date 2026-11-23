import { schema as s } from "jazz-tools";
import { schema } from "./schema";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";

const { definePermissions, exists, allowedTo, session, anyOf, allOf, ne, isMember } = schema;

// #region bandchat-permissions-is-member
// Membership is Jazz's members table: the member rows are what decides who
// holds a room's key, so every "is a room member" rule reads them.
const isRoomMember = (roomId: Parameters<typeof isMember>[1]) => isMember("rooms", roomId);
// #endregion bandchat-permissions-is-member

const createdByMe = { "$createdBy.account": session.user };

const authoredByMe = { author: session.user };

const requestedByMe = { requester: session.user };

const myReadMarker = { reader: session.user };

const myMembership = { memberAuthor: session.user };

// `roomId` is a denormalized authorization carrier: matching it against
// both the referenced message and current membership is equivalent to the
// message read policy without trusting a caller-supplied room id alone.
const canMutateReaction = (reaction: s.Row<typeof schema, "reactions">) =>
  allOf(
    authoredByMe,
    reaction.message.where({ roomId: reaction.roomId }),
    isRoomMember(reaction.roomId),
  );

export const permissions = definePermissions({
  ...betterAuthPermissions,
  canvases: {
    read: (canvas) => isRoomMember(canvas.roomId),
    insert: (canvas) => isRoomMember(canvas.roomId),
    update: false,
    delete: false,
  },
  joinRequests: {
    read: (request) => anyOf(requestedByMe, request.room.where(createdByMe)),
    insert: (request) => allOf(requestedByMe, request.profile.where(authoredByMe)),
    update: false,
    delete: (request) => anyOf(requestedByMe, request.room.where(createdByMe)),
  },
  // #region bandchat-permissions-messages
  messages: {
    read: (message) => isRoomMember(message.roomId),
    insert: (message) =>
      allOf(
        isRoomMember(message.roomId),
        message.sender.where(authoredByMe),
        anyOf({ canvasId: null }, message.canvas.where({ roomId: message.roomId })),
      ),
    update: false,
    delete: (message) => message.sender.where(authoredByMe),
  },
  // #endregion bandchat-permissions-messages
  profiles: {
    // Profiles are visible to their owner, to anyone who can read a message
    // they sent or a membership that names them (co-members), and to a room
    // creator who can read their join request.
    read: (profile) =>
      anyOf(
        authoredByMe,
        allowedTo.read(profile.messagesViaSender),
        allowedTo.read(profile.membershipsViaProfile),
        allowedTo.read(profile.joinRequestsViaProfile),
      ),
    insert: authoredByMe,
    update: authoredByMe,
    delete: authoredByMe,
  },
  reactions: {
    read: (reaction) => allowedTo.read(reaction.message),
    insert: canMutateReaction,
    update: false,
    delete: canMutateReaction,
  },
  // #region bandchat-permissions-read-markers
  // Read markers: one "read up to" row per member and room, no history. The
  // stored time is sealed with the room's key, so every member of the room
  // could decrypt it; the read rule keeps the server from sending it to them.
  readMarkers: {
    read: (marker) => allOf(myReadMarker, isRoomMember(marker.roomId)),
    insert: (marker) => allOf(myReadMarker, isRoomMember(marker.roomId)),
    updateIf: myReadMarker,
    updateTo: (marker) => allOf(myReadMarker, isRoomMember(marker.roomId)),
    delete: myReadMarker,
  },
  // #endregion bandchat-permissions-read-markers
  roomMembers: {
    read: (member) => allowedTo.read(member.room),
    // Only the room creator can admit members. In particular, no rule permits an
    // identity to insert its own membership into someone else's room.
    //
    // Every membership names the admitted account's own profile, and that
    // account must be the creator or have asked to join this room. Knowing an
    // account id is therefore not enough to put someone in a room. The admission
    // may delete the request in the same transaction: the check sees the
    // committed request (INV-RLS-9).
    //
    // Adding people the creator already shares another room with, without a
    // request, would need `allowedTo.read(member.memberProfile)` here. On INSERT
    // that currently requires UPDATE authority on the profile, not READ
    // (garden-co/jazz#1900), so it is denied even when the creator can read it.
    insert: (member) =>
      allOf(
        member.room.where(createdByMe),
        { memberProfileId: ne(null) },
        member.memberProfile.where({ author: member.memberAuthor }),
        anyOf(
          myMembership,
          exists.joinRequests({ roomId: member.roomId, requester: member.memberAuthor }),
        ),
      ),
    update: false,
    // The creator removes members; a member may also leave on their own.
    delete: (member) => anyOf(member.room.where(createdByMe), myMembership),
  },
  // #region bandchat-permissions-rooms
  rooms: {
    // The creator's member row is inserted in the transaction that creates
    // the room, so the creator needs no clause of their own.
    read: (room) => isRoomMember(room.id),
    insert: true,
    // Whoever may update a room adds and removes its member rows (and so its
    // key holders): only the creator, who alone renames it.
    update: createdByMe,
    delete: createdByMe,
  },
  // #endregion bandchat-permissions-rooms
  // `roomId` is again a carrier: it must match the canvas and a current
  // membership, so a removed member can no longer draw.
  strokes: {
    read: (stroke) => isRoomMember(stroke.roomId),
    insert: (stroke) =>
      allOf(
        authoredByMe,
        isRoomMember(stroke.roomId),
        stroke.canvas.where({ roomId: stroke.roomId }),
      ),
    update: false,
    delete: (stroke) => allOf(authoredByMe, isRoomMember(stroke.roomId)),
  },
});
