import { schema as s } from "jazz-tools";
import { tables as betterAuthTables, permissions as betterAuthPermissions } from "./auth-schema";

export const schema = s.defineSchema({
  ...betterAuthTables,
  albums: s
    .table(
      {
        title: s.string(),
        artist: s.string(),
        cover_locator: s.string().optional(),
        // Small cover image bytes and their media type. Albums without one
        // render a generated placeholder cover.
        cover_image: s.bytes().optional(),
        cover_mime: s.string().optional(),
      },
      { tracksViaAlbum: s.reverse("tracks", "album") },
    )
    .indexOnly(["title"]),
  tracks: s
    .table(
      {
        album_id: s.uuid(),
        title: s.string(),
        ordinal: s.int(),
        duration_ms: s.int(),
        // Written with `insertStreaming`; played back through typed byte-range
        // selections (`select({ audio_bytes: { from, to } })`).
        audio_bytes: s.bytes().optional(),
        audio_mime: s.string().optional(),
        audio_byte_length: s.int().optional(),
      },
      {
        album: s.rel("albums", "album_id"),
        playlist_entriesViaTrack: s.reverse("playlist_entries", "track"),
        playback_positionsViaTrack: s.reverse("playback_positions", "track"),
      },
    )
    .indexOnly(["album_id", "ordinal"]),
  playlists: s.table(
    { name: s.string() },
    {
      playlist_entriesViaPlaylist: s.reverse("playlist_entries", "playlist"),
      invitationsViaPlaylist: s.reverse("invitations", "playlist"),
      playback_positionsViaPlaylist: s.reverse("playback_positions", "playlist"),
    },
  ),
  playlist_entries: s
    .table(
      {
        playlist_id: s.uuid(),
        track_id: s.uuid(),
        position: s.float(),
      },
      { playlist: s.rel("playlists", "playlist_id"), track: s.rel("tracks", "track_id") },
    )
    .indexOnly(["playlist_id", "position"]),
  invitations: s.table(
    {
      playlist_id: s.uuid(),
      // Account UUID, derived during JWT enrollment. Provider subject stays in
      // the account's identity and is never used as application membership.
      subject: s.uuid(),
      role: s.enum("listener", "editor"),
      status: s.enum("pending", "accepted", "revoked"),
    },
    { playlist: s.rel("playlists", "playlist_id") },
  ),
  playback_positions: s.table(
    {
      playlist_id: s.uuid(),
      track_id: s.uuid(),
      position_ms: s.int(),
    },
    { playlist: s.rel("playlists", "playlist_id"), track: s.rel("tracks", "track_id") },
  ),
});

const { definePermissions, exists, allowedTo, session, anyOf, allOf } = schema;

const createdByMe = { $createdBy: session.user };

const isInvitation = (playlist_id: s.Uuid) =>
  exists.invitations({ playlist_id, subject: session.user, role: "editor", status: "accepted" });

export const permissions = definePermissions({
  ...betterAuthPermissions,
  albums: {
    read: true,
    insert: createdByMe,
  },
  invitations: {
    read: (invitation) => anyOf({ subject: session.user }, invitation.playlist.where(createdByMe)),
    insert: (invitation) => invitation.playlist.where(createdByMe),
    updateIf: (invitation) =>
      anyOf(invitation.playlist.where(createdByMe), { subject: session.user, status: "pending" }),
    updateTo: (invitation) =>
      anyOf(
        invitation.playlist.where(createdByMe),
        allOf(
          exists.invitations({
            id: invitation.id,
            playlist_id: invitation.playlist_id,
            subject: invitation.subject,
            role: invitation.role,
            status: "pending",
          }),
          { subject: session.user, status: "accepted" },
        ),
      ),
    delete: (invitation) => invitation.playlist.where(createdByMe),
  },
  playback_positions: {
    read: createdByMe,
    insert: true,
    update: createdByMe,
    delete: createdByMe,
  },
  playlist_entries: {
    read: (playlistEntry) => allowedTo.read(playlistEntry.playlist),
    insert: (playlistEntry) =>
      anyOf(allowedTo.update(playlistEntry.playlist), isInvitation(playlistEntry.playlist_id)),
    update: (playlistEntry) => anyOf(createdByMe, isInvitation(playlistEntry.playlist_id)),
    delete: (playlistEntry) => anyOf(createdByMe, isInvitation(playlistEntry.playlist_id)),
  },
  playlists: {
    read: (playlist) =>
      anyOf(
        createdByMe,
        exists.invitations({ playlist_id: playlist.id, subject: session.user, status: "accepted" }),
      ),
    insert: true,
    update: createdByMe,
  },
  tracks: {
    read: true,
    insert: createdByMe,
  },
});
