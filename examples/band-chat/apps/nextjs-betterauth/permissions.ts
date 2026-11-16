import { schema as s } from "jazz-tools";
import { schema } from "./schema";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";

const { definePermissions, exists, allowedTo, session, anyOf, allOf, ne } = schema;

const isRoomMember = (roomId: s.Uuid) => exists.roomMembers({ roomId, memberAuthor: session.user });

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
  // Read markers: one "read up to" row per member and room, no history.
  readMarkers: {
    // Co-members see each other's markers (the check marks and "Read by"
    // under a message); outsiders and ex-members see none.
    read: (marker) => isRoomMember(marker.roomId),
    insert: (marker) => allOf(myReadMarker, isRoomMember(marker.roomId)),
    // Only the reader moves their own marker, and only while a member. That a
    // marker only moves forward is the app's rule (useMarkRead never writes
    // an earlier position), not this policy's: a policy cannot yet compare a
    // column with the written row's value other than for equality (#5130).
    updateIf: myReadMarker,
    updateTo: (marker) => allOf(myReadMarker, isRoomMember(marker.roomId)),
    // A reader drops their own marker when they leave; the room creator drops
    // a member's marker when removing them, so an ex-member leaves no trace.
    delete: (marker) => anyOf(myReadMarker, marker.room.where(createdByMe)),
  },
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
  rooms: {
    // The room creator has a short local bootstrap window before its own
    // membership row is visible; every other identity must already be a member.
    read: (room) => anyOf(createdByMe, isRoomMember(room.id)),
    insert: true,
    // Only the creator may rename the room. Members never write to it: the
    // room list orders and marks rooms by their newest message.
    update: createdByMe,
    delete: createdByMe,
  },
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
