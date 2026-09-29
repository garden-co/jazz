import { schema as s } from "jazz-tools";

const schema = {
  bands: s.table(
    {
      name: s.string(),
      // The account that created the band. Only the owner manages invites and members.
      ownerId: s.uuid(),
    },
    {
      membersViaBand: s.reverse("members", "band"),
      invitesViaBand: s.reverse("bandInvites", "band"),
      stopsViaBand: s.reverse("stops", "band"),
    },
  ),
  // Invite codes live in their own table so only the band owner can read them.
  // Anyone holding a code can add themselves as a member until the owner resets it.
  bandInvites: s.table(
    {
      bandId: s.uuid(),
      code: s.string(),
    },
    { band: s.rel("bands", "bandId") },
  ),
  members: s.table(
    {
      bandId: s.uuid(),
      userId: s.uuid(),
      name: s.string(),
      // The invite code this member joined with. Empty for the band owner.
      inviteCode: s.string().optional(),
    },
    { band: s.rel("bands", "bandId") },
  ),
  venues: s.table(
    {
      name: s.string(),
      city: s.string(),
      country: s.string(),
      lat: s.float(),
      lng: s.float(),
      capacity: s.int().optional(),
      // Venues are shared places, readable by everyone. The creator owns a venue,
      // and so does the band it was added for.
      ownerId: s.uuid(),
      bandId: s.uuid().optional(),
    },
    { band: s.rel("bands", "bandId"), stopsViaVenue: s.reverse("stops", "venue") },
  ),
  stops: s.table(
    {
      bandId: s.uuid(),
      venueId: s.uuid(),
      date: s.timestamp(),
      status: s.enum("confirmed", "tentative", "cancelled"),
      publicDescription: s.string(),
    },
    {
      band: s.rel("bands", "bandId"),
      venue: s.rel("venues", "venueId"),
      notesViaStop: s.reverse("stopNotes", "stop"),
    },
  ),
  // Private notes sit in their own table: row-level permissions would otherwise
  // publish them alongside every confirmed stop.
  stopNotes: s.table(
    {
      stopId: s.uuid(),
      bandId: s.uuid(),
      body: s.string(),
    },
    { stop: s.rel("stops", "stopId"), band: s.rel("bands", "bandId") },
  ),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

const stopWithVenueQuery = app.stops.include({ venue: true });

export type Band = s.RowOf<typeof app.bands>;
export type Member = s.RowOf<typeof app.members>;
export type Venue = s.RowOf<typeof app.venues>;
export type StopWithVenue = s.RowOf<typeof stopWithVenueQuery>;
export type StopStatus = StopWithVenue["status"];
