import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { afterEach, beforeEach, expect, it } from "vitest";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";
import { buildTourFixture, DEFAULT_SEED } from "../../src/fixture.js";
import { startDemoTour } from "../../src/seed-loader.js";

// A new band and its whole demo tour are one transaction: the membership needs
// the band, the venues the membership, the stops their venues and the notes
// their stops, and each sees the rows written earlier in the same transaction
// (garden-co/jazz#3755).

const owner = "00000000-0000-4000-8000-000000000011";

let testApp: PolicyTestApp;

beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});

afterEach(async () => {
  await testApp.shutdown();
});

it("writes a band and its demo tour in one transaction", async () => {
  const db = testApp.as({
    issuer: "https://world-tour.example",
    user_id: "owner",
    account_id: owner,
    claims: {},
    authMode: "external",
  });

  const bandId = await startDemoTour(db, { userId: owner, ownerName: "Tour manager" });

  const fixture = buildTourFixture({ seed: DEFAULT_SEED, start: new Date() });
  const global = { tier: "global" } as const;
  await expect(db.all(app.members.where({ bandId }), global)).resolves.toEqual([
    expect.objectContaining({ userId: owner, name: "Tour manager" }),
  ]);
  await expect(db.all(app.bandInvites.where({ bandId }), global)).resolves.toHaveLength(1);
  await expect(db.all(app.stops.where({ bandId }), global)).resolves.toHaveLength(
    fixture.stops.length,
  );
  await expect(db.all(app.stopNotes.where({ bandId }), global)).resolves.toHaveLength(
    fixture.stops.filter((stop) => stop.privateNote).length,
  );
});
