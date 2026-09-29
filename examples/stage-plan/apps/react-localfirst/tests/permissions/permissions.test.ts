import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";
import { joinShow } from "../../src/model/actions.js";
import { DEMO_SHOW, resumeDemoShow, seedDemoShow } from "../../src/model/seed.js";

/**
 * Three people: Chiara runs the show (crew chief), Cole is on her crew,
 * Olive is an outsider who never got an invite.
 */
const chiefAccount = "00000000-0000-4000-8000-00000000c41f";
const crewAccount = "00000000-0000-4000-8000-00000000c3e3";
const outsiderAccount = "00000000-0000-4000-8000-0000000000a1";

const session = (user_id: string, account_id: string) => ({
  issuer: "https://stage-plan.example",
  user_id,
  account_id,
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

/**
 * Updating a row you can't read fails before it leaves your device: Jazz
 * needs read permission on the existing row to apply an update, so there is
 * nothing to send to the server. `expectDenied` waits for a server
 * rejection, so these cases assert the local denial instead.
 */
function expectUnreadableUpdate(write: () => unknown) {
  expect(write).toThrow(/read policy denied UPDATE/);
}

async function setUpShow() {
  const chief = testApp.as(session("chiara", chiefAccount));
  const crew = testApp.as(session("cole", crewAccount));
  const outsider = testApp.as(session("olive", outsiderAccount));

  const chiefProfile = await chief
    .insert(app.crew, { account: chiefAccount, name: "Chiara" })
    .wait({ tier: "global" });
  const crewProfile = await crew
    .insert(app.crew, { account: crewAccount, name: "Cole" })
    .wait({ tier: "global" });
  const outsiderProfile = await outsider
    .insert(app.crew, { account: outsiderAccount, name: "Olive" })
    .wait({ tier: "global" });

  const show = await chief
    .insert(app.shows, {
      name: "Late show",
      venue: "Old Pump House",
      date: "2026-11-14",
      doors: "19:30",
      chiefAccount,
    })
    .wait({ tier: "global" });
  await chief
    .insert(app.showCrew, {
      showId: show.id,
      crewId: chiefProfile.id,
      account: chiefAccount,
      role: "chief",
    })
    .wait({ tier: "global" });
  const invite = await chief
    .insert(app.showInvites, { showId: show.id, code: "synthetic-invite-code" })
    .wait({ tier: "global" });
  const task = await chief
    .insert(app.tasks, {
      showId: show.id,
      title: "Load-in",
      status: "todo",
      rank: 1,
    })
    .wait({ tier: "global" });

  return { chief, crew, outsider, chiefProfile, crewProfile, outsiderProfile, show, invite, task };
}

/** The demo show exactly once: chief, invite, eight tasks and one comment. */
async function expectCompleteDemoShow(chief: ReturnType<PolicyTestApp["as"]>) {
  const global = { tier: "global" } as const;
  const shows = await chief.all(app.shows.where({ chiefAccount }), global);
  expect(shows).toHaveLength(1);
  const showId = shows[0]!.id;
  await expect(chief.all(app.showCrew.where({ showId }), global)).resolves.toEqual([
    expect.objectContaining({ account: chiefAccount, role: "chief" }),
  ]);
  await expect(chief.all(app.showInvites.where({ showId }), global)).resolves.toHaveLength(1);
  await expect(chief.all(app.tasks.where({ showId }), global)).resolves.toHaveLength(8);
  await expect(chief.all(app.comments, global)).resolves.toHaveLength(1);
}

async function joinAsCrew(ctx: Awaited<ReturnType<typeof setUpShow>>) {
  return ctx.crew
    .insert(app.showCrew, {
      showId: ctx.show.id,
      crewId: ctx.crewProfile.id,
      account: crewAccount,
      role: "crew",
      inviteCode: ctx.invite.code,
    })
    .wait({ tier: "global" });
}

describe("StagePlan permissions", () => {
  it("hides a show and everything in it from outsiders", async () => {
    const ctx = await setUpShow();
    await ctx.chief
      .insert(app.comments, {
        taskId: ctx.task.id,
        authorId: ctx.chiefProfile.id,
        body: "Dock opens at two",
      })
      .wait({ tier: "global" });

    const { outsider } = ctx;
    await expect(outsider.all(app.shows.where({ id: ctx.show.id }))).resolves.toEqual([]);
    await expect(outsider.all(app.tasks.where({ showId: ctx.show.id }))).resolves.toEqual([]);
    await expect(outsider.all(app.comments.where({ taskId: ctx.task.id }))).resolves.toEqual([]);
    await expect(outsider.all(app.activity.where({ showId: ctx.show.id }))).resolves.toEqual([]);
    await expect(outsider.all(app.showCrew.where({ showId: ctx.show.id }))).resolves.toEqual([]);
    await expect(outsider.all(app.showInvites.where({ showId: ctx.show.id }))).resolves.toEqual([]);
  });

  it("saves a new account's demo show the way the app writes it", async () => {
    const chief = testApp.as(session("chiara", chiefAccount));
    const profile = await chief
      .insert(app.crew, { account: chiefAccount, name: "Chiara" })
      .wait({ tier: "global" });

    const writes = await seedDemoShow(chief, { account: chiefAccount, profile });
    await Promise.all(writes.map((write) => write.wait({ tier: "global" })));

    await expectCompleteDemoShow(chief);
  });

  it("finishes a demo show that was only partly saved, without duplicating it", async () => {
    const chief = testApp.as(session("chiara", chiefAccount));
    const profile = await chief
      .insert(app.crew, { account: chiefAccount, name: "Chiara" })
      .wait({ tier: "global" });
    const me = { account: chiefAccount, profile };
    // Only the show itself reached the server.
    await chief.insert(app.shows, { ...DEMO_SHOW, chiefAccount }).wait({ tier: "global" });

    const resumed = await resumeDemoShow(chief, me);
    await Promise.all(resumed.map((write) => write.wait({ tier: "global" })));
    await expectCompleteDemoShow(chief);

    await expect(resumeDemoShow(chief, me)).resolves.toEqual([]);
  });

  it("stops outsiders from writing to a show they are not on", async () => {
    const ctx = await setUpShow();
    const { outsider } = ctx;
    await outsider.expectDenied((db) =>
      db.insert(app.tasks, {
        showId: ctx.show.id,
        title: "Sneak in",
        status: "todo",
        rank: 2,
      }),
    );
    expectUnreadableUpdate(() => outsider.update(app.tasks, ctx.task.id, { status: "done" }));
    await outsider.expectDenied((db) =>
      db.insert(app.activity, {
        showId: ctx.show.id,
        taskId: ctx.task.id,
        actorId: ctx.outsiderProfile.id,
        kind: "moved",
      }),
    );
    expectUnreadableUpdate(() => outsider.update(app.shows, ctx.show.id, { name: "Mine now" }));
  });

  it("lets someone join only with the current invite code", async () => {
    const ctx = await setUpShow();
    await ctx.outsider.expectDenied((db) =>
      db.insert(app.showCrew, {
        showId: ctx.show.id,
        crewId: ctx.outsiderProfile.id,
        account: outsiderAccount,
        role: "crew",
        inviteCode: "guessed-code",
      }),
    );
    await ctx.outsider.expectDenied((db) =>
      db.insert(app.showCrew, {
        showId: ctx.show.id,
        crewId: ctx.outsiderProfile.id,
        account: outsiderAccount,
        role: "crew",
      }),
    );
    // A valid code still can't make you chief.
    await ctx.crew.expectDenied((db) =>
      db.insert(app.showCrew, {
        showId: ctx.show.id,
        crewId: ctx.crewProfile.id,
        account: crewAccount,
        role: "chief",
        inviteCode: ctx.invite.code,
      }),
    );

    await joinShow(
      ctx.crew,
      { account: crewAccount, profile: ctx.crewProfile },
      ctx.show.id,
      ctx.invite.code,
    );
    await expect(ctx.crew.all(app.shows.where({ id: ctx.show.id }))).resolves.toEqual([
      expect.objectContaining({ name: "Late show" }),
    ]);

    // Joining clears the code from the membership, so other crew can't read it.
    const membership = await ctx.chief.one(
      app.showCrew.where({ showId: ctx.show.id, account: crewAccount }),
      { tier: "global" },
    );
    expect(membership).toMatchObject({ role: "crew", inviteCode: null });

    // Clearing the code is the only change a member can make to their membership.
    const otherShow = await ctx.chief
      .insert(app.shows, { ...DEMO_SHOW, chiefAccount })
      .wait({ tier: "global" });
    await ctx.crew.expectDenied((db) =>
      db.update(app.showCrew, membership!.id, { showId: otherShow.id }),
    );
    await ctx.crew.expectDenied((db) => db.update(app.showCrew, membership!.id, { role: "chief" }));
    await ctx.crew.expectDenied((db) =>
      db.update(app.showCrew, membership!.id, { inviteCode: ctx.invite.code }),
    );
  });

  it("lets crew edit tasks, comment and log activity", async () => {
    const ctx = await setUpShow();
    await joinAsCrew(ctx);
    const { crew } = ctx;

    await crew
      .update(app.tasks, ctx.task.id, {
        status: "doing",
        assigneeId: ctx.crewProfile.id,
      })
      .wait({ tier: "global" });
    const added = await crew
      .insert(app.tasks, {
        showId: ctx.show.id,
        title: "Line check",
        status: "todo",
        rank: 2,
      })
      .wait({ tier: "global" });
    await crew
      .insert(app.comments, {
        taskId: ctx.task.id,
        authorId: ctx.crewProfile.id,
        body: "Riser is on the truck",
      })
      .wait({ tier: "global" });
    await crew
      .insert(app.activity, {
        showId: ctx.show.id,
        taskId: ctx.task.id,
        actorId: ctx.crewProfile.id,
        kind: "moved",
        detail: "In progress",
      })
      .wait({ tier: "global" });

    // Tasks go to someone on the crew, or to nobody.
    await crew.expectDenied((db) =>
      db.update(app.tasks, ctx.task.id, { assigneeId: ctx.outsiderProfile.id }),
    );
    await crew.update(app.tasks, ctx.task.id, { assigneeId: null }).wait({ tier: "global" });

    // Crew can delete tasks they created, but not the chief's.
    await crew.delete(app.tasks, added.id).wait({ tier: "global" });
    await crew.expectDenied((db) => db.delete(app.tasks, ctx.task.id));

    // Nobody can post as someone else.
    await crew.expectDenied((db) =>
      db.insert(app.comments, {
        taskId: ctx.task.id,
        authorId: ctx.chiefProfile.id,
        body: "Pretending to be the chief",
      }),
    );
  });

  it("keeps the show, its invite and its crew list in the chief's hands", async () => {
    const ctx = await setUpShow();
    await joinAsCrew(ctx);
    const { crew, chief } = ctx;

    await crew.expectDenied((db) => db.update(app.shows, ctx.show.id, { venue: "Elsewhere" }));
    await expect(crew.all(app.showInvites.where({ showId: ctx.show.id }))).resolves.toEqual([]);
    await crew.expectDenied((db) =>
      db.insert(app.showInvites, { showId: ctx.show.id, code: "crew-made-code" }),
    );
    const chiefMembership = await chief.one(
      app.showCrew.where({ showId: ctx.show.id, account: chiefAccount }),
    );
    await crew.expectDenied((db) => db.delete(app.showCrew, chiefMembership!.id));

    await chief.update(app.shows, ctx.show.id, { doors: "20:00" }).wait({ tier: "global" });
    await chief.delete(app.tasks, ctx.task.id).wait({ tier: "global" });

    // Removing a crew member takes the show away from them.
    const crewMembership = await chief.one(
      app.showCrew.where({ showId: ctx.show.id, account: crewAccount }),
    );
    await chief.delete(app.showCrew, crewMembership!.id).wait({ tier: "global" });
    await expect(crew.all(app.shows.where({ id: ctx.show.id }))).resolves.toEqual([]);
  });

  it("keeps the activity log append-only", async () => {
    const ctx = await setUpShow();
    const entry = await ctx.chief
      .insert(app.activity, {
        showId: ctx.show.id,
        taskId: ctx.task.id,
        actorId: ctx.chiefProfile.id,
        kind: "created",
      })
      .wait({ tier: "global" });
    await ctx.chief.expectDenied((db) => db.update(app.activity, entry.id, { kind: "moved" }));
    await ctx.chief.expectDenied((db) => db.delete(app.activity, entry.id));
  });

  it("keeps checklist items private to their owner", async () => {
    const ctx = await setUpShow();
    const item = await ctx.crew
      .insert(app.checklistItems, {
        title: "Spare in-ears",
        done: false,
        ownerAccount: crewAccount,
      })
      .wait({ tier: "global" });
    await expect(ctx.chief.all(app.checklistItems.where({ id: item.id }))).resolves.toEqual([]);
    expectUnreadableUpdate(() => ctx.chief.update(app.checklistItems, item.id, { done: true }));
    await ctx.crew.update(app.checklistItems, item.id, { done: true }).wait({ tier: "global" });
  });
});
