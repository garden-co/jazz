import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
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
      // The invite code this member joined with; empty for the band owner. Other
      // members can read it. That is no leak beyond the band: the code only admits
      // people while it is current, the owner resets it whenever a member is removed,
      // and anyone already in the band can see everything a new member would.
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
      // Venues are public places, readable by everyone. A venue added for a band is
      // managed by that band's members; the app only offers a band its own venues,
      // so no other band's stops depend on it.
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
      // Markdown. Bandmates editing the description at the same time keep
      // both edits: the server merges them instead of the last save winning.
      publicDescription: s.string().merge("markdown"),
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
      // Markdown, merged like the public description.
      body: s.string().merge("markdown"),
    },
    { stop: s.rel("stops", "stopId"), band: s.rel("bands", "bandId") },
  ),
});

const stopWithVenueQuery = app.stops.include({ venue: true });

const { definePermissions, exists, session, anyOf, allOf } = schema;

const ownerIsMe = { ownerId: session.user };

const isMember = (bandId: s.Uuid) => exists.members({ bandId, userId: session.user });

const mine = { userId: session.user };

export const permissions = definePermissions({
  bandInvites: {
    read: (bandInvite) => bandInvite.band.where(ownerIsMe),
    insert: (bandInvite) => bandInvite.band.where(ownerIsMe),
    delete: (bandInvite) => bandInvite.band.where(ownerIsMe),
  },
  bands: {
    read: true,
    insert: ownerIsMe,
    updateIf: (band) => isMember(band.id),
    updateTo: (band, old) => allOf(isMember(band.id), { ownerId: old.ownerId }),
    delete: ownerIsMe,
  },
  members: {
    read: (member) => anyOf(mine, isMember(member.bandId)),
    insert: (member) =>
      allOf(
        mine,
        anyOf(
          member.band.where(ownerIsMe),
          exists.bandInvites({ bandId: member.bandId, code: member.inviteCode }),
        ),
      ),
    delete: (member) => anyOf(mine, member.band.where(ownerIsMe)),
  },
  stopNotes: {
    read: (stopNote) => isMember(stopNote.bandId),
    insert: (stopNote) =>
      allOf(isMember(stopNote.bandId), stopNote.stop.where({ bandId: stopNote.bandId })),
    updateIf: (stopNote) => isMember(stopNote.bandId),
    updateTo: (stopNote) =>
      allOf(isMember(stopNote.bandId), stopNote.stop.where({ bandId: stopNote.bandId })),
    delete: (stopNote) => isMember(stopNote.bandId),
  },
  stops: {
    read: (stop) => anyOf({ status: "confirmed" }, isMember(stop.bandId)),
    insert: (stop) => allOf(isMember(stop.bandId), stop.venue.where({ bandId: stop.bandId })),
    updateIf: (stop) => isMember(stop.bandId),
    updateTo: (stop) => allOf(isMember(stop.bandId), stop.venue.where({ bandId: stop.bandId })),
    delete: (stop) => isMember(stop.bandId),
  },
  venues: {
    read: true,
    insert: (venue) => allOf(ownerIsMe, anyOf({ bandId: null }, isMember(venue.bandId))),
    updateIf: (venue) => anyOf({ bandId: null, ownerId: session.user }, isMember(venue.bandId)),
    updateTo: (venue) =>
      allOf(
        anyOf({ bandId: null, ownerId: session.user }, isMember(venue.bandId)),
        anyOf(
          exists.venues({ id: venue.id, ownerId: venue.ownerId, bandId: venue.bandId }),
          allOf(
            { bandId: null },
            exists.venues({ id: venue.id, ownerId: venue.ownerId, bandId: null }),
          ),
        ),
      ),
    delete: (venue) => anyOf({ bandId: null, ownerId: session.user }, isMember(venue.bandId)),
  },
});
