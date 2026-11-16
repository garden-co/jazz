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

const { definePermissions, exists, session, anyOf, allOf } = schema;

const isBandMember = (bandId: s.Uuid) => exists.members({ bandId, userId: session.user });

const isBandOwner = (bandId: s.Uuid) => exists.bands({ id: bandId, ownerId: session.user });

const ownedByMe = { ownerId: session.user };

const mine = { userId: session.user };

// A band's venue is managed by the band's current members (a creator who
// leaves loses it); a venue without a band belongs to its creator.
const managesVenue = (bandId: s.Uuid) =>
  anyOf({ bandId: null, ownerId: session.user }, isBandMember(bandId));

export const permissions = definePermissions({
  // Invites: visible to and managed by the band owner only.
  bandInvites: {
    read: (invite) => isBandOwner(invite.bandId),
    insert: (invite) => isBandOwner(invite.bandId),
    delete: (invite) => isBandOwner(invite.bandId),
  },
  // Bands: the name is public. Members can rename the band; only the owner
  // deletes it, and nobody can hand ownership to someone else by editing the row.
  bands: {
    read: true,
    insert: ownedByMe,
    updateIf: (band) => isBandMember(band.id),
    updateTo: (band, old) => allOf(isBandMember(band.id), { ownerId: old.ownerId }),
    delete: ownedByMe,
  },
  // Members: you can only ever add yourself, either as the owner bootstrapping
  // the band or with a current invite code. The owner can remove anyone;
  // members can leave.
  members: {
    read: (member) => anyOf(mine, isBandMember(member.bandId)),
    insert: (member) =>
      allOf(
        mine,
        anyOf(
          isBandOwner(member.bandId),
          exists.bandInvites({ bandId: member.bandId, code: member.inviteCode }),
        ),
      ),
    delete: (member) => anyOf(mine, isBandOwner(member.bandId)),
  },
  // Private notes: band members only, and always attached to one of their stops.
  stopNotes: {
    read: (note) => isBandMember(note.bandId),
    insert: (note) => allOf(isBandMember(note.bandId), note.stop.where({ bandId: note.bandId })),
    updateIf: (note) => isBandMember(note.bandId),
    updateTo: (note) => allOf(isBandMember(note.bandId), note.stop.where({ bandId: note.bandId })),
    delete: (note) => isBandMember(note.bandId),
  },
  // Stops: the public sees confirmed dates; the band sees and edits everything.
  // A stop's venue must be one of its band's venues, so no other band can move
  // or delete it from under the stop.
  stops: {
    read: (stop) => anyOf({ status: "confirmed" }, isBandMember(stop.bandId)),
    insert: (stop) => allOf(isBandMember(stop.bandId), stop.venue.where({ bandId: stop.bandId })),
    updateIf: (stop) => isBandMember(stop.bandId),
    updateTo: (stop) => allOf(isBandMember(stop.bandId), stop.venue.where({ bandId: stop.bandId })),
    delete: (stop) => isBandMember(stop.bandId),
  },
  // Venues: public places.
  venues: {
    read: true,
    insert: (venue) => allOf(ownedByMe, anyOf({ bandId: null }, isBandMember(venue.bandId))),
    updateIf: (venue) => managesVenue(venue.bandId),
    updateTo: (venue) =>
      allOf(
        managesVenue(venue.bandId),
        // Neither the creator nor the band can be changed by an update.
        anyOf(
          exists.venues({ id: venue.id, ownerId: venue.ownerId, bandId: venue.bandId }),
          allOf(
            { bandId: null },
            exists.venues({ id: venue.id, ownerId: venue.ownerId, bandId: null }),
          ),
        ),
      ),
    delete: (venue) => managesVenue(venue.bandId),
  },
});
