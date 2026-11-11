import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { afterEach, beforeEach, expect, it } from "vitest";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";
import { saveStopEdit } from "../../src/lib/stop-edit.js";
import { startDemoTour } from "../../src/seed-loader.js";

// The stop form saves its texts as edits of what it opened, so a bandmate's
// edit that lands while the form is open survives the save.

const owner = "00000000-0000-4000-8000-000000000011";
const device = (user_id: string) =>
  testApp.as({
    issuer: "https://world-tour.example",
    user_id,
    account_id: owner,
    claims: {},
    authMode: "external",
  });
const remote = { tier: "remote" } as const;

let testApp: PolicyTestApp;

beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});

afterEach(async () => {
  await testApp.shutdown();
});

it("keeps a bandmate's edits to the stop texts made while the form was open", async () => {
  const laptop = device("owner-laptop");
  const phone = device("owner-phone");
  const { bandId, accepted } = await startDemoTour(laptop, { userId: owner, ownerName: "TM" });
  await accepted;
  const stop = (await laptop.all(app.stops.where({ bandId }), remote))[0]!;
  const note = await laptop
    .insert(app.stopNotes, { stopId: stop.id, bandId, body: "Load-in at 2pm.\n" })
    .wait({ tier: "global" });
  await laptop
    .update(app.stops, stop.id, { publicDescription: "Doors at 8.\n" })
    .wait({ tier: "global" });

  // The form opens on the laptop; meanwhile the phone edits both texts.
  const opened = { description: "Doors at 8.\n", notes: "Load-in at 2pm.\n" };
  await phone
    .update(app.stops, stop.id, { publicDescription: "Doors at 7.\n" })
    .wait({ tier: "global" });
  await phone
    .update(app.stopNotes, note.id, { body: "Load-in at 3pm.\n" })
    .wait({ tier: "global" });

  const saving = await saveStopEdit(laptop, stop, note, opened, {
    date: stop.date,
    status: stop.status,
    description: "Doors at 8.\nAll ages.",
    notes: "Load-in at 2pm.\nSoundcheck at 5pm.",
  });
  await saving.wait({ tier: "global" });

  const saved = (await laptop.all(app.stops.where({ id: stop.id }), remote))[0]!;
  expect(saved.publicDescription).toBe("Doors at 7.\nAll ages.\n");
  const savedNote = (await laptop.all(app.stopNotes.where({ id: note.id }), remote))[0]!;
  expect(savedNote.body).toBe("Load-in at 3pm.\nSoundcheck at 5pm.\n");
});
