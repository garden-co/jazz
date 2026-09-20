import { schema as s } from "jazz-tools";
import { betterAuthSchema } from "./auth-schema";

const schema = {
  ...betterAuthSchema,
  albums: s
    .table(
      { title: s.string(), artist: s.string(), cover_locator: s.string().optional() },
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
        // `insertStreaming` accepts this field today. Playback/range reads remain
        // intentionally outside this adapter until the typed Db gains that API.
        audio_bytes: s.bytes().optional(),
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
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
