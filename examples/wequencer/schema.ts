import { col, defineApp, type DefinedSchema, type RowOf, type TypedApp } from "jazz-tools";

const schemaDef = {
  instruments: {
    name: col.string(),
    sound: col.bytes(),
    display_order: col.int(),
  },
  jams: {
    created_at: col.timestamp(),
    transport_start: col.timestamp().optional(),
    bpm: col.int(),
    beat_count: col.int(),
  },
  beats: {
    jamId: col.ref("jams"),
    instrumentId: col.ref("instruments"),
    beat_index: col.int(),
    placed_by: col.string(),
  },
  participants: {
    jamId: col.ref("jams"),
    userId: col.string(),
    display_name: col.string(),
  },
};

type AppSchema = DefinedSchema<typeof schemaDef>;
export const app: TypedApp<AppSchema> = defineApp(schemaDef);

export type Instrument = RowOf<typeof app.instruments>;
export type Jam = RowOf<typeof app.jams>;
export type Beat = RowOf<typeof app.beats>;
export type Participant = RowOf<typeof app.participants>;
