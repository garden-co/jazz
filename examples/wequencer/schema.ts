import { schema as s } from "jazz-tools";

const schema = {
  files: s.table({
    name: s.string().optional(),
    mime_type: s.string(),
    data: s.bytes(),
  }),
  instruments: s.table({
    name: s.string(),
    soundFileId: s.ref("files"),
    display_order: s.int(),
  }),
  jams: s.table({
    created_at: s.timestamp(),
    transport_start: s.timestamp().optional(),
    bpm: s.int(),
    beat_count: s.int(),
  }),
  beats: s.table({
    jamId: s.ref("jams"),
    instrumentId: s.ref("instruments"),
    beat_index: s.int(),
    placed_by: s.string(),
  }),
  participants: s.table({
    jamId: s.ref("jams"),
    display_name: s.string(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

export type Instrument = s.RowOf<typeof app.instruments>;
export type StoredFile = s.RowOf<typeof app.files>;
export type Jam = s.RowOf<typeof app.jams>;
export type Beat = s.RowOf<typeof app.beats>;
export type Participant = s.RowOf<typeof app.participants>;
