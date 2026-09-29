import { schema as s } from "jazz-tools";
import type { RowContext } from "jazz-tools/permissions";
import { app } from "./schema.js";

type BandScoped = RowContext<{ bandId: string }>;

export default s.definePermissions(app, ({ policy, session, anyOf, allOf }) => {
  const me = session.user.account;
  const isMemberOf = (bandId: BandScoped["bandId"]) =>
    policy.members.exists.where({ bandId, userId: me });
  const isOwnerOf = (bandId: BandScoped["bandId"]) =>
    policy.bands.exists.where({ id: bandId, ownerId: me });

  // Bands: the name is public. Members can rename the band; only the owner deletes
  // it, and nobody can hand ownership to someone else by editing the row.
  policy.bands.allowRead.always();
  policy.bands.allowInsert.where({ ownerId: me });
  policy.bands.allowUpdate
    .whereOld((band) => isMemberOf(band.id))
    .whereNew((band) =>
      allOf([
        isMemberOf(band.id),
        policy.bands.exists.where({ id: band.id, ownerId: band.ownerId }),
      ]),
    );
  policy.bands.allowDelete.where({ ownerId: me });

  // Invites: visible to and managed by the band owner only.
  policy.bandInvites.allowRead.where((invite) => isOwnerOf(invite.bandId));
  policy.bandInvites.allowInsert.where((invite) => isOwnerOf(invite.bandId));
  policy.bandInvites.allowDelete.where((invite) => isOwnerOf(invite.bandId));

  // Members: you can only ever add yourself, either as the owner bootstrapping the
  // band or with a current invite code. The owner can remove anyone; members can leave.
  policy.members.allowRead.where((member) => anyOf([{ userId: me }, isMemberOf(member.bandId)]));
  policy.members.allowInsert.where((member) =>
    allOf([
      { userId: me },
      anyOf([
        isOwnerOf(member.bandId),
        policy.bandInvites.exists.where({ bandId: member.bandId, code: member.inviteCode }),
      ]),
    ]),
  );
  policy.members.allowDelete.where((member) => anyOf([{ userId: me }, isOwnerOf(member.bandId)]));

  // Venues: public places. A band's venue is managed by the band's current members
  // (a creator who leaves loses it); a venue without a band belongs to its creator.
  const managesVenue = (venue: BandScoped) =>
    anyOf([allOf([{ bandId: { isNull: true } }, { ownerId: me }]), isMemberOf(venue.bandId)]);
  policy.venues.allowRead.always();
  policy.venues.allowInsert.where((venue) =>
    allOf([{ ownerId: me }, anyOf([{ bandId: { isNull: true } }, isMemberOf(venue.bandId)])]),
  );
  policy.venues.allowUpdate.whereOld(managesVenue).whereNew((venue) =>
    allOf([
      managesVenue(venue),
      // Neither the creator nor the band can be changed by an update.
      anyOf([
        policy.venues.exists.where({ id: venue.id, ownerId: venue.ownerId, bandId: venue.bandId }),
        allOf([
          { bandId: { isNull: true } },
          policy.venues.exists.where({
            id: venue.id,
            ownerId: venue.ownerId,
            bandId: { isNull: true },
          }),
        ]),
      ]),
    ]),
  );
  policy.venues.allowDelete.where(managesVenue);

  // Stops: the public sees confirmed dates; the band sees and edits everything.
  const onMyBand = (row: BandScoped) => isMemberOf(row.bandId);
  policy.stops.allowRead.where((stop) => anyOf([{ status: "confirmed" }, onMyBand(stop)]));
  // A stop's venue must be one of its band's venues, so no other band can move or
  // delete it from under the stop.
  const writableStop = (stop: RowContext<{ bandId: string; venueId: string }>) =>
    allOf([onMyBand(stop), policy.venues.exists.where({ id: stop.venueId, bandId: stop.bandId })]);
  policy.stops.allowInsert.where(writableStop);
  policy.stops.allowUpdate.whereOld(onMyBand).whereNew(writableStop);
  policy.stops.allowDelete.where(onMyBand);

  // Private notes: band members only, and always attached to one of their stops.
  policy.stopNotes.allowRead.where(onMyBand);
  policy.stopNotes.allowInsert.where((note) =>
    allOf([onMyBand(note), policy.stops.exists.where({ id: note.stopId, bandId: note.bandId })]),
  );
  policy.stopNotes.allowUpdate
    .whereOld(onMyBand)
    .whereNew((note) =>
      allOf([onMyBand(note), policy.stops.exists.where({ id: note.stopId, bandId: note.bandId })]),
    );
  policy.stopNotes.allowDelete.where(onMyBand);
});
