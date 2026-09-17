import { schema as s } from "jazz-tools";

const schema = {
  bands: s.table(
    {
      name: s.string(),
    },
    { membersViaBand: s.reverse("members", "band"), stopsViaBand: s.reverse("stops", "band") },
  ),
  venues: s.table(
    {
      name: s.string(),
      city: s.string(),
      country: s.string(),
      lat: s.float(),
      lng: s.float(),
      capacity: s.int().optional(),
    },
    { stopsViaVenue: s.reverse("stops", "venue") },
  ),
  members: s.table(
    {
      bandId: s.uuid(),
      userId: s.uuid(),
    },
    { band: s.rel("bands", "bandId") },
  ),
  stops: s.table(
    {
      bandId: s.uuid(),
      venueId: s.uuid(),
      date: s.timestamp(),
      status: s.enum("confirmed", "tentative", "cancelled"),
      publicDescription: s.string(),
      privateNotes: s.string().optional(),
    },
    { band: s.rel("bands", "bandId"), venue: s.rel("venues", "venueId") },
  ),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

const stopWithVenueQuery = app.stops.include({ venue: true });

export type Venue = s.RowOf<typeof app.venues>;
export type StopWithVenue = s.RowOf<typeof stopWithVenueQuery>;
