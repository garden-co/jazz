import { schema as s } from "jazz-tools";
import { schema } from "./schema";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";

const { definePermissions, exists, allowedTo, session, anyOf, allOf, ne } = schema;

const isRoomMember = (roomId: s.Uuid) => exists.roomMembers({ roomId, memberAuthor: session.user });

const requesterIsMe = { requester: session.user };

const createdByMe = { $createdBy: session.user };

const authorIsMe = { author: session.user };

const readerIsMe = { reader: session.user };

const memberAuthorIsMe = { memberAuthor: session.user };

export const permissions = definePermissions({
  ...betterAuthPermissions,
  canvases: {
    read: (canvas) => isRoomMember(canvas.roomId),
    insert: (canvas) => isRoomMember(canvas.roomId),
    update: false,
    delete: false,
  },
  joinRequests: {
    read: (joinRequest) => anyOf(requesterIsMe, joinRequest.room.where(createdByMe)),
    insert: (joinRequest) => allOf(requesterIsMe, joinRequest.profile.where(authorIsMe)),
    update: false,
    delete: (joinRequest) => anyOf(requesterIsMe, joinRequest.room.where(createdByMe)),
  },
  messages: {
    read: (message) => isRoomMember(message.roomId),
    insert: (message) =>
      allOf(
        isRoomMember(message.roomId),
        message.sender.where(authorIsMe),
        anyOf({ canvasId: null }, message.canvas.where({ roomId: message.roomId })),
      ),
    update: false,
    delete: (message) => message.sender.where(authorIsMe),
  },
  profiles: {
    read: (profile) =>
      anyOf(
        authorIsMe,
        allowedTo.read(profile.messagesViaSender),
        allowedTo.read(profile.membershipsViaProfile),
        allowedTo.read(profile.joinRequestsViaProfile),
      ),
    insert: authorIsMe,
    update: authorIsMe,
    delete: authorIsMe,
  },
  reactions: {
    read: (reaction) => allowedTo.read(reaction.message),
    insert: (reaction) =>
      allOf(
        authorIsMe,
        reaction.message.where({ roomId: reaction.roomId }),
        isRoomMember(reaction.roomId),
      ),
    update: false,
    delete: (reaction) =>
      allOf(
        authorIsMe,
        reaction.message.where({ roomId: reaction.roomId }),
        isRoomMember(reaction.roomId),
      ),
  },
  readMarkers: {
    read: (readMarker) => isRoomMember(readMarker.roomId),
    insert: (readMarker) => allOf(readerIsMe, isRoomMember(readMarker.roomId)),
    updateIf: readerIsMe,
    updateTo: (readMarker) => allOf(readerIsMe, isRoomMember(readMarker.roomId)),
    delete: (readMarker) => anyOf(readerIsMe, readMarker.room.where(createdByMe)),
  },
  roomMembers: {
    read: (roomMember) => allowedTo.read(roomMember.room),
    insert: (roomMember) =>
      allOf(
        roomMember.room.where(createdByMe),
        { memberProfileId: ne(null) },
        roomMember.memberProfile.where({ author: roomMember.memberAuthor }),
        anyOf(
          memberAuthorIsMe,
          exists.joinRequests({ roomId: roomMember.roomId, requester: roomMember.memberAuthor }),
        ),
      ),
    update: false,
    delete: (roomMember) => anyOf(roomMember.room.where(createdByMe), memberAuthorIsMe),
  },
  rooms: {
    read: (room) => anyOf(createdByMe, isRoomMember(room.id)),
    insert: true,
    update: createdByMe,
    delete: createdByMe,
  },
  strokes: {
    read: (stroke) => isRoomMember(stroke.roomId),
    insert: (stroke) =>
      allOf(
        authorIsMe,
        isRoomMember(stroke.roomId),
        stroke.canvas.where({ roomId: stroke.roomId }),
      ),
    update: false,
    delete: (stroke) => allOf(authorIsMe, isRoomMember(stroke.roomId)),
  },
});
