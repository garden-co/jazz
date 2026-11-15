// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type InvitationRole = "listener" | "editor";
export type InvitationStatus = "pending" | "accepted" | "revoked";

export type Album = {
  readonly id: string;
  title: string;
  artist: string;
  cover_locator: string | null;
  cover_image: Uint8Array | null;
  cover_mime: string | null;
};

export type AlbumInsert = {
  title: string;
  artist: string;
  cover_locator?: string | null;
  cover_image?: Uint8Array | null;
  cover_mime?: string | null;
};

export type BetterAuthAccount = {
  readonly id: string;
  accountId: string;
  providerId: string;
  userId: string;
  password: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthAccountInsert = {
  accountId: string;
  providerId: string;
  userId: string;
  password?: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthJwk = {
  readonly id: string;
  publicKey: string;
  privateKey: string;
  createdAt: Date;
  expiresAt: Date | null;
  alg: string | null;
  crv: string | null;
};

export type BetterAuthJwkInsert = {
  publicKey: string;
  privateKey: string;
  createdAt: Date;
  expiresAt?: Date | null;
  alg?: string | null;
  crv?: string | null;
};

export type BetterAuthSession = {
  readonly id: string;
  expiresAt: Date;
  token: string;
  createdAt: Date;
  updatedAt: Date;
  userId: string;
  ipAddress: string | null;
  userAgent: string | null;
};

export type BetterAuthSessionInsert = {
  expiresAt: Date;
  token: string;
  createdAt: Date;
  updatedAt: Date;
  userId: string;
  ipAddress?: string | null;
  userAgent?: string | null;
};

export type BetterAuthUser = {
  readonly id: string;
  name: string;
  email: string;
  emailVerified: boolean;
  image: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthUserInsert = {
  name: string;
  email: string;
  emailVerified: boolean;
  image?: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthVerification = {
  readonly id: string;
  identifier: string;
  value: string;
  expiresAt: Date;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthVerificationInsert = {
  identifier: string;
  value: string;
  expiresAt: Date;
  createdAt: Date;
  updatedAt: Date;
};

export type Invitation = {
  readonly id: string;
  playlist_id: string;
  subject: string;
  role: InvitationRole;
  status: InvitationStatus;
};

export type InvitationInsert = {
  playlist_id: string;
  subject: string;
  role: InvitationRole;
  status: InvitationStatus;
};

export type PlaybackPosition = {
  readonly id: string;
  playlist_id: string;
  track_id: string;
  position_ms: number;
};

export type PlaybackPositionInsert = {
  playlist_id: string;
  track_id: string;
  position_ms: number;
};

export type PlaylistEntry = {
  readonly id: string;
  playlist_id: string;
  track_id: string;
  position: number;
};

export type PlaylistEntryInsert = {
  playlist_id: string;
  track_id: string;
  position: number;
};

export type Playlist = {
  readonly id: string;
  name: string;
};

export type PlaylistInsert = {
  name: string;
};

export type Track = {
  readonly id: string;
  album_id: string;
  title: string;
  ordinal: number;
  duration_ms: number;
  audio_bytes: Uint8Array | null;
  audio_mime: string | null;
  audio_byte_length: number | null;
};

export type TrackInsert = {
  album_id: string;
  title: string;
  ordinal: number;
  duration_ms: number;
  audio_bytes?: Uint8Array | null;
  audio_mime?: string | null;
  audio_byte_length?: number | null;
};

interface Types extends AppTypes {
  albums: {
    row: Album;
    insert: AlbumInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      artist: { kind: "string"; nullable: false; value: string };
      cover_locator: { kind: "string"; nullable: true; value: string };
      cover_image: { kind: "bytes"; nullable: true; value: Uint8Array };
      cover_mime: { kind: "string"; nullable: true; value: string };
    };
    relations: {
      tracksViaAlbum: { many: "tracks" };
    };
  };
  better_auth_account: {
    row: BetterAuthAccount;
    insert: BetterAuthAccountInsert;
    columns: {
      accountId: { kind: "string"; nullable: false; value: string };
      providerId: { kind: "string"; nullable: false; value: string };
      userId: { kind: "uuid"; nullable: false; value: string };
      password: { kind: "string"; nullable: true; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      user: { one: "better_auth_user"; nullable: false };
    };
  };
  better_auth_jwks: {
    row: BetterAuthJwk;
    insert: BetterAuthJwkInsert;
    columns: {
      publicKey: { kind: "string"; nullable: false; value: string };
      privateKey: { kind: "string"; nullable: false; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      expiresAt: { kind: "timestamp"; nullable: true; value: Date };
      alg: { kind: "string"; nullable: true; value: string };
      crv: { kind: "string"; nullable: true; value: string };
    };
    relations: {};
  };
  better_auth_session: {
    row: BetterAuthSession;
    insert: BetterAuthSessionInsert;
    columns: {
      expiresAt: { kind: "timestamp"; nullable: false; value: Date };
      token: { kind: "string"; nullable: false; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
      userId: { kind: "uuid"; nullable: false; value: string };
      ipAddress: { kind: "string"; nullable: true; value: string };
      userAgent: { kind: "string"; nullable: true; value: string };
    };
    relations: {
      user: { one: "better_auth_user"; nullable: false };
    };
  };
  better_auth_user: {
    row: BetterAuthUser;
    insert: BetterAuthUserInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      email: { kind: "string"; nullable: false; value: string };
      emailVerified: { kind: "boolean"; nullable: false; value: boolean };
      image: { kind: "string"; nullable: true; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      better_auth_accountViaUser: { many: "better_auth_account" };
      better_auth_sessionViaUser: { many: "better_auth_session" };
    };
  };
  better_auth_verification: {
    row: BetterAuthVerification;
    insert: BetterAuthVerificationInsert;
    columns: {
      identifier: { kind: "string"; nullable: false; value: string };
      value: { kind: "string"; nullable: false; value: string };
      expiresAt: { kind: "timestamp"; nullable: false; value: Date };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {};
  };
  invitations: {
    row: Invitation;
    insert: InvitationInsert;
    columns: {
      playlist_id: { kind: "uuid"; nullable: false; value: string };
      subject: { kind: "uuid"; nullable: false; value: string };
      role: { kind: "enum"; nullable: false; value: InvitationRole };
      status: { kind: "enum"; nullable: false; value: InvitationStatus };
    };
    relations: {
      playlist: { one: "playlists"; nullable: false };
    };
  };
  playback_positions: {
    row: PlaybackPosition;
    insert: PlaybackPositionInsert;
    columns: {
      playlist_id: { kind: "uuid"; nullable: false; value: string };
      track_id: { kind: "uuid"; nullable: false; value: string };
      position_ms: { kind: "int"; nullable: false; value: number };
    };
    relations: {
      playlist: { one: "playlists"; nullable: false };
      track: { one: "tracks"; nullable: false };
    };
  };
  playlist_entries: {
    row: PlaylistEntry;
    insert: PlaylistEntryInsert;
    columns: {
      playlist_id: { kind: "uuid"; nullable: false; value: string };
      track_id: { kind: "uuid"; nullable: false; value: string };
      position: { kind: "float"; nullable: false; value: number };
    };
    relations: {
      playlist: { one: "playlists"; nullable: false };
      track: { one: "tracks"; nullable: false };
    };
  };
  playlists: {
    row: Playlist;
    insert: PlaylistInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      invitationsViaPlaylist: { many: "invitations" };
      playback_positionsViaPlaylist: { many: "playback_positions" };
      playlist_entriesViaPlaylist: { many: "playlist_entries" };
    };
  };
  tracks: {
    row: Track;
    insert: TrackInsert;
    columns: {
      album_id: { kind: "uuid"; nullable: false; value: string };
      title: { kind: "string"; nullable: false; value: string };
      ordinal: { kind: "int"; nullable: false; value: number };
      duration_ms: { kind: "int"; nullable: false; value: number };
      audio_bytes: { kind: "bytes"; nullable: true; value: Uint8Array };
      audio_mime: { kind: "string"; nullable: true; value: string };
      audio_byte_length: { kind: "int"; nullable: true; value: number };
    };
    relations: {
      album: { one: "albums"; nullable: false };
      playback_positionsViaTrack: { many: "playback_positions" };
      playlist_entriesViaTrack: { many: "playlist_entries" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:cc22a16fa88acdd3e80c3b13b2e737c8bbd6ff5b0954cbc4cd3edfe05657c1c9",
  "schema": {
    "current": "fa66c6bd2a11cdf22cc6efb1e5c7737070b322de45163e636068309507671292",
    "schemas": [
      {
        "hash": "fa66c6bd2a11cdf22cc6efb1e5c7737070b322de45163e636068309507671292",
        "schema": {
          "tables": {
            "albums": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "artist", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "cover_locator", "nullable": true },
                { "column_type": { "type": "Bytea" }, "name": "cover_image", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "cover_mime", "nullable": true },
              ],
              "indexed_columns": ["title"],
            },
            "better_auth_account": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "accountId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "providerId", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
                { "column_type": { "type": "Text" }, "name": "password", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_jwks": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "publicKey", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "privateKey", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "alg", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "crv", "nullable": true },
              ],
            },
            "better_auth_session": {
              "columns": [
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "token", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
                { "column_type": { "type": "Text" }, "name": "ipAddress", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "userAgent", "nullable": true },
              ],
            },
            "better_auth_user": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "email", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "emailVerified", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "image", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_verification": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "identifier", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "value", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "invitations": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "playlist_id",
                  "nullable": false,
                  "references": "playlists",
                },
                { "column_type": { "type": "Uuid" }, "name": "subject", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["listener", "editor"] },
                  "name": "role",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Enum", "variants": ["pending", "accepted", "revoked"] },
                  "name": "status",
                  "nullable": false,
                },
              ],
            },
            "playback_positions": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "playlist_id",
                  "nullable": false,
                  "references": "playlists",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "track_id",
                  "nullable": false,
                  "references": "tracks",
                },
                { "column_type": { "type": "Integer" }, "name": "position_ms", "nullable": false },
              ],
            },
            "playlist_entries": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "playlist_id",
                  "nullable": false,
                  "references": "playlists",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "track_id",
                  "nullable": false,
                  "references": "tracks",
                },
                { "column_type": { "type": "Double" }, "name": "position", "nullable": false },
              ],
              "indexed_columns": ["playlist_id", "position"],
            },
            "playlists": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "tracks": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "album_id",
                  "nullable": false,
                  "references": "albums",
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "ordinal", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "duration_ms", "nullable": false },
                { "column_type": { "type": "Bytea" }, "name": "audio_bytes", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "audio_mime", "nullable": true },
                {
                  "column_type": { "type": "Integer" },
                  "name": "audio_byte_length",
                  "nullable": true,
                },
              ],
              "indexed_columns": ["album_id", "ordinal"],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "albums": { "tracksViaAlbum": { "many": "tracks", "relation": "album" } },
    "better_auth_account": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_jwks": {},
    "better_auth_session": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_user": {
      "better_auth_accountViaUser": { "many": "better_auth_account", "relation": "user" },
      "better_auth_sessionViaUser": { "many": "better_auth_session", "relation": "user" },
    },
    "better_auth_verification": {},
    "invitations": { "playlist": { "one": "playlists", "column": "playlist_id" } },
    "playback_positions": {
      "playlist": { "one": "playlists", "column": "playlist_id" },
      "track": { "one": "tracks", "column": "track_id" },
    },
    "playlist_entries": {
      "playlist": { "one": "playlists", "column": "playlist_id" },
      "track": { "one": "tracks", "column": "track_id" },
    },
    "playlists": {
      "invitationsViaPlaylist": { "many": "invitations", "relation": "playlist" },
      "playback_positionsViaPlaylist": { "many": "playback_positions", "relation": "playlist" },
      "playlist_entriesViaPlaylist": { "many": "playlist_entries", "relation": "playlist" },
    },
    "tracks": {
      "album": { "one": "albums", "column": "album_id" },
      "playback_positionsViaTrack": { "many": "playback_positions", "relation": "track" },
      "playlist_entriesViaTrack": { "many": "playlist_entries", "relation": "track" },
    },
  },
});
