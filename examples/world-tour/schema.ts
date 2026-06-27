import { schema as s } from "jazz-tools";

const schema = {
  files: s.table({
    name: s.string().optional(),
    mime_type: s.string(),
    data: s.bytes(),
  }),
  bands: s.table({
    name: s.string(),
    logoFileId: s.ref("files").optional(),
  }),
  venues: s.table({
    name: s.string(),
    city: s.string(),
    country: s.string(),
    lat: s.float(),
    lng: s.float(),
    capacity: s.int().optional(),
  }),
  members: s.table({
    bandId: s.ref("bands"),
    userId: s.string(),
  }),
  stops: s.table({
    bandId: s.ref("bands"),
    venueId: s.ref("venues"),
    date: s.timestamp(),
    status: s.enum("confirmed", "tentative", "cancelled"),
    publicDescription: s.string(),
    privateNotes: s.string().optional(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

const stopWithVenueQuery = app.stops.include({ venue: true });

export type Venue = s.RowOf<typeof app.venues>;
export type StopWithVenue = s.RowOf<typeof stopWithVenueQuery>;
