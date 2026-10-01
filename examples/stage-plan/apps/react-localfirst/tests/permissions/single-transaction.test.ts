import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { afterEach, beforeEach, expect, it } from "vitest";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";
import { createShow } from "../../src/model/actions.js";
import { seedDemoShow } from "../../src/model/seed.js";

// A new show and the demo show are each one transaction: the crew chief's
// membership, the invite, the board and the comment pass their policies
// against rows written earlier in the same transaction (garden-co/jazz#3755).

const chiefAccount = "00000000-0000-4000-8000-00000000c420";

let testApp: PolicyTestApp;

beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});

afterEach(async () => {
  await testApp?.shutdown();
});

async function chiefWithProfile() {
  const chief = testApp.as({
    issuer: "https://stage-plan.example",
    user_id: "chiara",
    account_id: chiefAccount,
    claims: {},
    authMode: "external",
  });
  const profile = await chief
    .insert(app.crew, { account: chiefAccount, name: "Chiara" })
    .wait({ tier: "global" });
  return { chief, me: { account: chiefAccount, profile } };
}

it("creates a show with its crew chief and invite in one transaction", async () => {
  const { chief, me } = await chiefWithProfile();

  const { show, writes } = await createShow(chief, me, {
    name: "Matinee",
    venue: "Corn Exchange",
    date: "2026-12-05",
    doors: "13:00",
  });
  expect(writes).toHaveLength(1);
  await writes[0]!.wait({ tier: "global" });

  const global = { tier: "global" } as const;
  await expect(chief.all(app.showCrew.where({ showId: show.id }), global)).resolves.toEqual([
    expect.objectContaining({ account: chiefAccount, role: "chief" }),
  ]);
  await expect(chief.all(app.showInvites.where({ showId: show.id }), global)).resolves.toHaveLength(
    1,
  );
});

it("writes the whole demo show in one transaction", async () => {
  const { chief, me } = await chiefWithProfile();

  const { showId, writes } = await seedDemoShow(chief, me);
  expect(writes).toHaveLength(1);
  await writes[0]!.wait({ tier: "global" });

  const global = { tier: "global" } as const;
  await expect(chief.all(app.showCrew.where({ showId }), global)).resolves.toHaveLength(1);
  await expect(chief.all(app.tasks.where({ showId }), global)).resolves.toHaveLength(8);
  await expect(chief.all(app.activity.where({ showId }), global)).resolves.toHaveLength(9);
  await expect(chief.all(app.comments, global)).resolves.toHaveLength(1);
});
