import { schema as s } from "jazz-tools";
import { betterAuthSchema } from "./auth-schema";

const schema = {
  ...betterAuthSchema,
  albums: s
    .table({ title: s.string(), artist: s.string(), cover_locator: s.string().optional() })
    .indexOnly(["title"]),
  tracks: s
    .table({
      album_id: s.ref("albums"),
      title: s.string(),
      ordinal: s.int(),
      duration_ms: s.int(),
      // `insertStreaming` accepts this field today. Playback/range reads remain
      // intentionally outside this adapter until the typed Db gains that API.
      audio_bytes: s.bytes().optional(),
    })
    .indexOnly(["album_id", "ordinal"]),
  playlists: s.table({ name: s.string() }),
  playlist_entries: s
    .table({
      playlist_id: s.ref("playlists"),
      track_id: s.ref("tracks"),
      position: s.float(),
    })
    .indexOnly(["playlist_id", "position"]),
  invitations: s.table({
    playlist_id: s.ref("playlists"),
    subject: s.string(),
    role: s.enum("listener", "editor"),
    status: s.enum("pending", "accepted", "revoked"),
  }),
  playback_positions: s.table({
    playlist_id: s.ref("playlists"),
    track_id: s.ref("tracks"),
    position_ms: s.int(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
