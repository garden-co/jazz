import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";

const issuer = "https://world-tour.example";
const accounts = {
  owner: "00000000-0000-4000-8000-000000000001",
  member: "00000000-0000-4000-8000-000000000002",
  visitor: "00000000-0000-4000-8000-000000000003",
  outsider: "00000000-0000-4000-8000-000000000004",
  revoked: "00000000-0000-4000-8000-000000000005",
};
type Who = keyof typeof accounts;

let testApp: PolicyTestApp;

function as(who: Who) {
  return testApp.as({
    issuer,
    user_id: who,
    account_id: accounts[who],
    claims: {},
    authMode: "external",
  });
}

/** One band: owner + member, a confirmed and a tentative stop, a private note. */
async function seedTour() {
  const band = await testApp.seed((db) =>
    db.insert(app.bands, { name: "The Groove Syndicate", ownerId: accounts.owner }),
  );
  await testApp.seed((db) =>
    db.insert(app.bandInvites, { bandId: band.id, code: "invite-current" }),
  );
  await testApp.seed((db) =>
    db.insert(app.members, { bandId: band.id, userId: accounts.owner, name: "Owner" }),
  );
  const membership = await testApp.seed((db) =>
    db.insert(app.members, {
      bandId: band.id,
      userId: accounts.member,
      name: "Member",
      inviteCode: "invite-current",
    }),
  );
  const venue = await testApp.seed((db) =>
    db.insert(app.venues, {
      name: "Sala Apolo",
      city: "Barcelona",
      country: "Spain",
      lat: 41.37,
      lng: 2.17,
      ownerId: accounts.owner,
      bandId: band.id,
    }),
  );
  const confirmed = await testApp.seed((db) =>
    db.insert(app.stops, {
      bandId: band.id,
      venueId: venue.id,
      date: new Date(2026, 9, 1, 20),
      status: "confirmed",
      publicDescription: "Headline show",
    }),
  );
  const tentative = await testApp.seed((db) =>
    db.insert(app.stops, {
      bandId: band.id,
      venueId: venue.id,
      date: new Date(2026, 9, 3, 20),
      status: "tentative",
      publicDescription: "Rooftop sunset set",
    }),
  );
  const note = await testApp.seed((db) =>
    db.insert(app.stopNotes, {
      stopId: confirmed.id,
      bandId: band.id,
      body: "Load-in at 2pm.",
    }),
  );
  return { band, membership, venue, confirmed, tentative, note };
}

/** A second band, owned by the outsider, with its own venue and stop. */
async function seedOtherBand() {
  const band = await testApp.seed((db) =>
    db.insert(app.bands, { name: "The Other Band", ownerId: accounts.outsider }),
  );
  await testApp.seed((db) =>
    db.insert(app.members, { bandId: band.id, userId: accounts.outsider, name: "Other owner" }),
  );
  const venue = await testApp.seed((db) =>
    db.insert(app.venues, {
      name: "Paradiso",
      city: "Amsterdam",
      country: "Netherlands",
      lat: 52.36,
      lng: 4.88,
      ownerId: accounts.outsider,
      bandId: band.id,
    }),
  );
  const stop = await testApp.seed((db) =>
    db.insert(app.stops, {
      bandId: band.id,
      venueId: venue.id,
      date: new Date(2026, 9, 2, 20),
      status: "confirmed",
      publicDescription: "Other band's show",
    }),
  );
  return { band, venue, stop };
}

const ids = (rows: { id: string }[]) => rows.map((r) => r.id).sort();

beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});

afterEach(async () => {
  await testApp?.shutdown();
});

describe("public visitors", () => {
  it("see the band and its confirmed stops, and nothing private", async () => {
    const tour = await seedTour();
    const visitor = as("visitor");

    await expect(visitor.all(app.bands)).resolves.toEqual([
      expect.objectContaining({ id: tour.band.id }),
    ]);
    expect(ids(await visitor.all(app.stops))).toEqual([tour.confirmed.id]);
    await expect(visitor.all(app.stopNotes)).resolves.toEqual([]);
    await expect(visitor.all(app.bandInvites)).resolves.toEqual([]);
    await expect(visitor.all(app.members)).resolves.toEqual([]);
    expect(ids(await visitor.all(app.venues))).toEqual([tour.venue.id]);
  });

  it("cannot write to the band's tour", async () => {
    const tour = await seedTour();
    const visitor = as("visitor");

    await visitor.expectDenied((db) =>
      db.insert(app.stops, {
        bandId: tour.band.id,
        venueId: tour.venue.id,
        date: new Date(2026, 9, 5, 20),
        status: "confirmed",
        publicDescription: "Gatecrash",
      }),
    );
    await visitor.expectDenied((db) => db.update(app.bands, tour.band.id, { name: "Renamed" }));
    await visitor.expectDenied((db) => db.update(app.venues, tour.venue.id, { capacity: 1 }));
    await visitor.expectDenied((db) => db.delete(app.stops, tour.confirmed.id));
  });
});

describe("band members", () => {
  it("see every stop and the private notes, but not the invite code", async () => {
    const tour = await seedTour();
    const member = as("member");

    expect(ids(await member.all(app.stops))).toEqual(ids([tour.confirmed, tour.tentative]));
    expect(ids(await member.all(app.stopNotes))).toEqual([tour.note.id]);
    expect(await member.all(app.members)).toHaveLength(2);
    await expect(member.all(app.bandInvites)).resolves.toEqual([]);
  });

  it("plan the tour: add stops, edit them, rename the band, edit the band's venues", async () => {
    const tour = await seedTour();
    const member = as("member");

    await member
      .insert(app.stops, {
        bandId: tour.band.id,
        venueId: tour.venue.id,
        date: new Date(2026, 9, 7, 20),
        status: "tentative",
        publicDescription: "Late-night jazz session",
      })
      .wait({ tier: "global" });
    await member
      .update(app.stops, tour.tentative.id, { status: "confirmed" })
      .wait({ tier: "global" });
    await member
      .update(app.bands, tour.band.id, { name: "The Groove Collective" })
      .wait({ tier: "global" });
    await member.update(app.venues, tour.venue.id, { capacity: 1100 }).wait({ tier: "global" });
  });

  it("cannot take over the band or its venues", async () => {
    const tour = await seedTour();
    const member = as("member");

    await member.expectDenied((db) =>
      db.update(app.bands, tour.band.id, { ownerId: accounts.member }),
    );
    await member.expectDenied((db) =>
      db.update(app.venues, tour.venue.id, { ownerId: accounts.member }),
    );
    await member.expectDenied((db) => db.delete(app.bands, tour.band.id));
    await member.expectDenied((db) =>
      db.insert(app.bandInvites, { bandId: tour.band.id, code: "member-made" }),
    );
  });

  it("cannot remove other members, but can leave", async () => {
    const tour = await seedTour();
    const ownerMembership = (
      await as("owner").all(app.members.where({ userId: accounts.owner }))
    )[0];
    const member = as("member");

    await member.expectDenied((db) => db.delete(app.members, ownerMembership.id));
    await member.delete(app.members, tour.membership.id).wait({ tier: "global" });
    expect(ids(await member.all(app.stops))).toEqual([tour.confirmed.id]);
  });

  it("lose the band's venues when they leave, even ones they created", async () => {
    const tour = await seedTour();
    const member = as("member");
    const venue = member.insert(app.venues, {
      name: "Razzmatazz",
      city: "Barcelona",
      country: "Spain",
      lat: 41.4,
      lng: 2.19,
      ownerId: accounts.member,
      bandId: tour.band.id,
    });
    await venue.wait({ tier: "global" });

    await member.delete(app.members, tour.membership.id).wait({ tier: "global" });
    await member.expectDenied((db) => db.update(app.venues, venue.value.id, { capacity: 1 }));
    await member.expectDenied((db) => db.delete(app.venues, venue.value.id));
  });
});

describe("other bands", () => {
  it("cannot move or delete a venue this band's stops use", async () => {
    const tour = await seedTour();
    await seedOtherBand();
    const otherOwner = as("outsider");

    await otherOwner.expectDenied((db) => db.update(app.venues, tour.venue.id, { lat: 0, lng: 0 }));
    await otherOwner.expectDenied((db) => db.delete(app.venues, tour.venue.id));
  });

  it("cannot book a stop at another band's venue", async () => {
    const tour = await seedTour();
    const other = await seedOtherBand();
    const member = as("member");

    await member.expectDenied((db) =>
      db.insert(app.stops, {
        bandId: tour.band.id,
        venueId: other.venue.id,
        date: new Date(2026, 9, 9, 20),
        status: "tentative",
        publicDescription: "Borrowed venue",
      }),
    );
    await member.expectDenied((db) =>
      db.update(app.stops, tour.tentative.id, { venueId: other.venue.id }),
    );
  });

  it("cannot attach private notes to this band's stops", async () => {
    const tour = await seedTour();
    const other = await seedOtherBand();

    // A member of this band can't annotate the other band's stop, under either band.
    const member = as("member");
    await member.expectDenied((db) =>
      db.insert(app.stopNotes, { stopId: other.stop.id, bandId: tour.band.id, body: "Mine now" }),
    );
    await member.expectDenied((db) =>
      db.insert(app.stopNotes, { stopId: other.stop.id, bandId: other.band.id, body: "Mine now" }),
    );
    // Nor can the other band annotate this band's stop.
    await as("outsider").expectDenied((db) =>
      db.insert(app.stopNotes, { stopId: tour.confirmed.id, bandId: other.band.id, body: "Hi" }),
    );
  });

  it("cannot be handed a venue by changing its band", async () => {
    const tour = await seedTour();
    const other = await seedOtherBand();
    const owner = as("owner");

    await owner.expectDenied((db) =>
      db.update(app.venues, tour.venue.id, { bandId: other.band.id }),
    );
    // Dropping the band would leave the venue to its creator alone.
    await owner.expectDenied((db) => db.update(app.venues, tour.venue.id, { bandId: null }));
  });
});

describe("outsiders", () => {
  it("cannot enrol themselves without a current invite code", async () => {
    const tour = await seedTour();
    const outsider = as("outsider");
    const join =
      (inviteCode?: string) => (db: Parameters<Parameters<typeof outsider.expectDenied>[0]>[0]) =>
        db.insert(app.members, {
          bandId: tour.band.id,
          userId: accounts.outsider,
          name: "Outsider",
          ...(inviteCode ? { inviteCode } : {}),
        });

    await outsider.expectDenied(join());
    await outsider.expectDenied(join("invite-forged"));
    expect(ids(await outsider.all(app.stops))).toEqual([tour.confirmed.id]);
  });

  it("cannot enrol somebody else, even with a valid code", async () => {
    const tour = await seedTour();
    await as("outsider").expectDenied((db) =>
      db.insert(app.members, {
        bandId: tour.band.id,
        userId: accounts.visitor,
        name: "Visitor",
        inviteCode: "invite-current",
      }),
    );
  });

  it("cannot start a band in someone else's name or claim a venue for it", async () => {
    const tour = await seedTour();
    const outsider = as("outsider");

    await outsider.expectDenied((db) =>
      db.insert(app.bands, { name: "Impostors", ownerId: accounts.owner }),
    );
    await outsider.expectDenied((db) =>
      db.insert(app.venues, {
        name: "Fake venue",
        city: "Nowhere",
        country: "None",
        lat: 0,
        lng: 0,
        ownerId: accounts.outsider,
        bandId: tour.band.id,
      }),
    );
  });

  it("own the venues they create", async () => {
    await seedTour();
    const outsider = as("outsider");
    const venue = outsider.insert(app.venues, {
      name: "Paradiso",
      city: "Amsterdam",
      country: "Netherlands",
      lat: 52.36,
      lng: 4.88,
      ownerId: accounts.outsider,
    });
    await venue.wait({ tier: "global" });
    await outsider.update(app.venues, venue.value.id, { capacity: 1500 }).wait({ tier: "global" });
    await as("member").expectDenied((db) => db.update(app.venues, venue.value.id, { capacity: 1 }));
  });
});

describe("invites", () => {
  it("let the holder of the current code join, and only the owner sees the code", async () => {
    const tour = await seedTour();
    const invitee = as("outsider");

    await expect(as("owner").all(app.bandInvites)).resolves.toEqual([
      expect.objectContaining({ code: "invite-current" }),
    ]);
    await invitee
      .insert(app.members, {
        bandId: tour.band.id,
        userId: accounts.outsider,
        name: "New member",
        inviteCode: "invite-current",
      })
      .wait({ tier: "global" });

    expect(ids(await invitee.all(app.stops))).toEqual(ids([tour.confirmed, tour.tentative]));
  });

  it("stop working once the owner resets the code", async () => {
    const tour = await seedTour();
    const owner = as("owner");
    const [invite] = await owner.all(app.bandInvites);
    await owner.delete(app.bandInvites, invite.id).wait({ tier: "global" });
    await owner
      .insert(app.bandInvites, { bandId: tour.band.id, code: "invite-next" })
      .wait({ tier: "global" });

    await as("outsider").expectDenied((db) =>
      db.insert(app.members, {
        bandId: tour.band.id,
        userId: accounts.outsider,
        name: "Late",
        inviteCode: "invite-current",
      }),
    );
  });
});

describe("revoked members", () => {
  it("lose access as soon as the owner removes them, and cannot rejoin with the old code", async () => {
    const tour = await seedTour();
    const membership = await testApp.seed((db) =>
      db.insert(app.members, {
        bandId: tour.band.id,
        userId: accounts.revoked,
        name: "Soon gone",
        inviteCode: "invite-current",
      }),
    );
    const revoked = as("revoked");
    expect(ids(await revoked.all(app.stops))).toEqual(ids([tour.confirmed, tour.tentative]));

    // Revoking removes the membership and resets the invite, as the app does.
    const owner = as("owner");
    const [invite] = await owner.all(app.bandInvites);
    await owner.delete(app.members, membership.id).wait({ tier: "global" });
    await owner.delete(app.bandInvites, invite.id).wait({ tier: "global" });

    expect(ids(await revoked.all(app.stops))).toEqual([tour.confirmed.id]);
    await expect(revoked.all(app.stopNotes)).resolves.toEqual([]);
    await revoked.expectDenied((db) =>
      db.update(app.stops, tour.confirmed.id, { publicDescription: "Still here" }),
    );
    await revoked.expectDenied((db) =>
      db.insert(app.members, {
        bandId: tour.band.id,
        userId: accounts.revoked,
        name: "Back again",
        inviteCode: "invite-current",
      }),
    );
  });
});
