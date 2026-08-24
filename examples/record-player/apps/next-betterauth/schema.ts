import { schema as s } from "jazz-tools";

const schema = {
  albums: s.table({ title: s.string(), artist: s.string(), cover_locator: s.string().optional() }),
  tracks: s.table({
    album_id: s.ref("albums"),
    title: s.string(),
    ordinal: s.int(),
    duration_ms: s.int(),
    // Streaming bytes/range reads are covered by expected-red integration work (#1833/#1839/#1844).
    audio_locator: s.string().optional(),
  }),
  playlists: s.table({ name: s.string(), owner_subject: s.string() }),
  playlist_entries: s.table({
    playlist_id: s.ref("playlists"),
    track_id: s.ref("tracks"),
    position: s.float(),
  }),
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
