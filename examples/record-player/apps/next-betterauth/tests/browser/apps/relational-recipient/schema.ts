import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  albums: s
    .table(
      {
        title: s.string(),
        artist: s.string(),
        cover_locator: s.string().optional(),
        cover_image: s.bytes().optional(),
        cover_mime: s.string().optional(),
      },
      {
        tracksViaAlbum: s.reverse("tracks", "album"),
      },
    )
    .indexOnly(["title"]),
  better_auth_account: s.table(
    {
      accountId: s.string(),
      providerId: s.string(),
      userId: s.uuid(),
      password: s.string().optional(),
      createdAt: s.timestamp(),
      updatedAt: s.timestamp(),
    },
    {
      user: s.rel("better_auth_user", "userId"),
    },
  ),
  better_auth_jwks: s.table({
    publicKey: s.string(),
    privateKey: s.string(),
    createdAt: s.timestamp(),
    expiresAt: s.timestamp().optional(),
    alg: s.string().optional(),
    crv: s.string().optional(),
  }),
  better_auth_session: s.table(
    {
      expiresAt: s.timestamp(),
      token: s.string(),
      createdAt: s.timestamp(),
      updatedAt: s.timestamp(),
      userId: s.uuid(),
      ipAddress: s.string().optional(),
      userAgent: s.string().optional(),
    },
    {
      user: s.rel("better_auth_user", "userId"),
    },
  ),
  better_auth_user: s.table(
    {
      name: s.string(),
      email: s.string(),
      emailVerified: s.boolean(),
      image: s.string().optional(),
      createdAt: s.timestamp(),
      updatedAt: s.timestamp(),
    },
    {
      better_auth_accountViaUser: s.reverse("better_auth_account", "user"),
      better_auth_sessionViaUser: s.reverse("better_auth_session", "user"),
    },
  ),
  better_auth_verification: s.table({
    identifier: s.string(),
    value: s.string(),
    expiresAt: s.timestamp(),
    createdAt: s.timestamp(),
    updatedAt: s.timestamp(),
  }),
  invitations: s.table(
    {
      playlist_id: s.uuid(),
      subject: s.uuid(),
      role: s.enum("listener", "editor"),
      status: s.enum("pending", "accepted", "revoked"),
    },
    {
      playlist: s.rel("playlists", "playlist_id"),
    },
  ),
  playback_positions: s.table(
    { playlist_id: s.uuid(), track_id: s.uuid(), position_ms: s.int() },
    {
      playlist: s.rel("playlists", "playlist_id"),
      track: s.rel("tracks", "track_id"),
    },
  ),
  playlist_entries: s
    .table(
      { playlist_id: s.uuid(), track_id: s.uuid(), position: s.float() },
      {
        playlist: s.rel("playlists", "playlist_id"),
        track: s.rel("tracks", "track_id"),
      },
    )
    .indexOnly(["playlist_id", "position"]),
  playlists: s.table(
    { name: s.string() },
    {
      invitationsViaPlaylist: s.reverse("invitations", "playlist"),
      playback_positionsViaPlaylist: s.reverse("playback_positions", "playlist"),
      playlist_entriesViaPlaylist: s.reverse("playlist_entries", "playlist"),
    },
  ),
  tracks: s
    .table(
      {
        album_id: s.uuid(),
        title: s.string(),
        ordinal: s.int(),
        duration_ms: s.int(),
        audio_bytes: s.bytes().optional(),
        audio_mime: s.string().optional(),
        audio_byte_length: s.int().optional(),
      },
      {
        album: s.rel("albums", "album_id"),
        playback_positionsViaTrack: s.reverse("playback_positions", "track"),
        playlist_entriesViaTrack: s.reverse("playlist_entries", "track"),
      },
    )
    .indexOnly(["album_id", "ordinal"]),
});

const { definePermissions, exists, allowedTo, session, anyOf, allOf } = schema;

const createdByMe = { "$createdBy.account": session.user };

const hasEditorInvitation = (playlist_id: s.Uuid) =>
  exists.invitations({ playlist_id, subject: session.user, role: "editor", status: "accepted" });

const canReadPlaylist = (playlist_id: s.Uuid) =>
  anyOf(
    createdByMe,
    exists.invitations({
      playlist_id,
      // This column carries Jazz's issuer-scoped canonical session user, not a
      // provider-local Better Auth user id. It stays stable across tokens
      // and avoids conflating external account storage with row authorship.
      subject: session.user,
      status: "accepted",
    }),
  );

const canEditPlaylist = (playlist_id: s.Uuid) =>
  anyOf(createdByMe, hasEditorInvitation(playlist_id));

const ownsPlaylist = (invitation: s.Row<typeof schema, "invitations">) =>
  invitation.playlist.where(createdByMe);

export const permissions = definePermissions({
  // RecordPlayer deliberately models a shared public catalogue: every
  // authenticated listener can publish album metadata and audio tracks, and
  // everyone can browse them. A production catalog would ordinarily replace
  // the album and track insert rules with a label/artist membership relation.
  albums: {
    read: true,
    insert: createdByMe,
  },
  better_auth_account: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_jwks: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_session: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_user: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_verification: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  invitations: {
    read: (invitation) => anyOf({ subject: session.user }, ownsPlaylist(invitation)),
    insert: ownsPlaylist,
    // Recipients may perform the one-way pending -> accepted transition; every
    // other invitation change (including revoke) remains owner-controlled.
    // Keep both alternatives in one rule: separate update rules would silently
    // form an old/new cross product. The recipient's persisted pending row and
    // accepted new row are both required.
    updateIf: (invitation) =>
      anyOf(ownsPlaylist(invitation), { subject: session.user, status: "pending" }),
    updateTo: (invitation) =>
      anyOf(
        ownsPlaylist(invitation),
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
    delete: ownsPlaylist,
  },
  playback_positions: {
    read: createdByMe,
    insert: true,
    update: createdByMe,
    delete: createdByMe,
  },
  playlist_entries: {
    read: (entry) => allowedTo.read(entry.playlist),
    insert: (entry) =>
      anyOf(allowedTo.update(entry.playlist), hasEditorInvitation(entry.playlist_id)),
    update: (entry) => canEditPlaylist(entry.playlist_id),
    delete: (entry) => canEditPlaylist(entry.playlist_id),
  },
  playlists: {
    read: (playlist) => canReadPlaylist(playlist.id),
    insert: true,
    update: createdByMe,
  },
  // Shared public catalogue, like albums.
  tracks: {
    read: true,
    insert: createdByMe,
  },
});
