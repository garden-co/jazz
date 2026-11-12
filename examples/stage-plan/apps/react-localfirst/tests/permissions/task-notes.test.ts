import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { afterEach, beforeEach, expect, it } from "vitest";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";
import { updateTaskNotes } from "../../src/model/actions.js";

const chiefAccount = "00000000-0000-4000-8000-00000000c41f";
const session = (user_id: string) => ({
  issuer: "https://stage-plan.example",
  user_id,
  account_id: chiefAccount,
  claims: {},
  authMode: "external" as const,
});

let testApp: PolicyTestApp;

beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});

afterEach(async () => {
  await testApp?.shutdown();
});

/** The chief on two devices: the task dialog is open on `dialog`. */
async function setUp(notes: string | null) {
  const dialog = testApp.as(session("chiara-laptop"));
  const phone = testApp.as(session("chiara-phone"));
  const show = await dialog
    .insert(app.shows, {
      name: "Late show",
      venue: "Old Pump House",
      date: "2026-11-14",
      doors: "19:30",
      chiefAccount,
    })
    .wait({ tier: "global" });
  const task = await dialog
    .insert(app.tasks, { showId: show.id, title: "Load-in", status: "todo", rank: 1, notes })
    .wait({ tier: "global" });
  const read = async () =>
    (await dialog.all(app.tasks.where({ id: task.id }), { tier: "remote" }))[0]!.notes;
  return { dialog, phone, task, read };
}

it("keeps an edit made elsewhere while the task dialog was open", async () => {
  const opened = "Load-in at 2pm. Rider: extra monitors.";
  const { dialog, phone, task, read } = await setUp(opened);

  await phone
    .update(app.tasks, task.id, { notes: "Load-in at 3pm. Rider: extra monitors." })
    .wait({ tier: "global" });
  // The write updateTaskNotes makes. Test clients act as a session, which
  // takes edits (editedFrom) inside a transaction only.
  const saving = await dialog.transaction((tx) =>
    tx.update(
      app.tasks,
      task.id,
      { notes: "Load-in at 2pm. Rider: extra monitors. Bring the spare snare." },
      { editedFrom: { notes: opened } },
    ),
  );
  await saving.wait({ tier: "global" });

  expect(await read()).toBe("Load-in at 3pm. Rider: extra monitors. Bring the spare snare.");
});

it("writes first notes and cleared notes as plain values", async () => {
  const { dialog, task, read } = await setUp(null);

  await updateTaskNotes(dialog, task, "Bring the spare snare.", null).wait({ tier: "global" });
  expect(await read()).toBe("Bring the spare snare.");

  await updateTaskNotes(dialog, task, "", "Bring the spare snare.").wait({ tier: "global" });
  expect(await read()).toBeNull();
});
