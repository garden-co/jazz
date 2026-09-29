import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import permissions from "../permissions";
import { app } from "../schema";
import { stepRow } from "./session-setup";

let testApp: PolicyTestApp;

beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
}, 30_000);

afterEach(async () => {
  await testApp.shutdown();
});

function bandmate(name: string) {
  const account = crypto.randomUUID();
  return {
    account,
    db: testApp.as({
      issuer: "https://wequencer.example.test",
      user_id: name,
      account_id: account,
      claims: {},
      authMode: "external",
    }),
  };
}

/** One session with an editor and a viewer, seeded as the backend. */
async function seedSession(creator: string, members: Array<[string, "editor" | "viewer"]>) {
  const session = await testApp.seed((db) =>
    db.insert(app.sessions, { title: "Rehearsal", tempo_bpm: 120 }),
  );
  for (const [account, role] of [[creator, "owner"] as const, ...members])
    await testApp.seed((db) =>
      db.insert(app.session_members, { session_id: session.id, member_author: account, role }),
    );
  const track = await testApp.seed((db) =>
    db.insert(app.tracks, { session_id: session.id, position: 0, name: "Kick", color: "red" }),
  );
  const pattern = await testApp.seed((db) =>
    db.insert(app.patterns, { session_id: session.id, position: 0, name: "Pattern 1", length: 16 }),
  );
  return { session, track, pattern };
}

describe("Wequencer permissions", () => {
  it("lets editors toggle pads and rejects viewers", async () => {
    const creator = bandmate("creator");
    const editor = bandmate("editor");
    const viewer = bandmate("viewer");
    const { session, track, pattern } = await seedSession(creator.account, [
      [editor.account, "editor"],
      [viewer.account, "viewer"],
    ]);
    const address = { sessionId: session.id, trackId: track.id, patternId: pattern.id };

    const first = stepRow({ ...address, position: 0 }, true);
    await editor.db.upsert(app.steps, first.id, first.data).wait({ tier: "global" });

    const second = stepRow({ ...address, position: 1 }, true);
    await viewer.db.expectDenied((db) => db.upsert(app.steps, second.id, second.data));
    await viewer.db.expectDenied((db) => db.update(app.steps, first.id, { enabled: false }));
    await viewer.db.expectDenied((db) =>
      db.insert(app.transport_observations, {
        session_id: session.id,
        playing: true,
        bar: 0,
        observed_at: new Date(),
      }),
    );
  }, 30_000);

  it("rejects steps and transport that mix in another session's pattern", async () => {
    const creator = bandmate("creator");
    const editor = bandmate("editor");
    const here = await seedSession(creator.account, [[editor.account, "editor"]]);
    // The editor also creates a session of their own, so they can edit both.
    const elsewhere = await seedSession(editor.account, []);

    const crossed = stepRow(
      {
        sessionId: here.session.id,
        trackId: here.track.id,
        patternId: elsewhere.pattern.id,
        position: 0,
      },
      true,
    );
    await editor.db.expectDenied((db) => db.upsert(app.steps, crossed.id, crossed.data));

    await editor.db.expectDenied((db) =>
      db.insert(app.transport_observations, {
        session_id: here.session.id,
        playing: true,
        bar: 0,
        observed_at: new Date(),
        pattern_id: elsewhere.pattern.id,
      }),
    );
    await editor.db
      .insert(app.transport_observations, {
        session_id: here.session.id,
        playing: true,
        bar: 0,
        observed_at: new Date(),
        pattern_id: here.pattern.id,
      })
      .wait({ tier: "global" });
  }, 30_000);

  it("gives a track added alongside a new pattern working pads", async () => {
    const creator = bandmate("creator");
    const editor = bandmate("editor");
    const { session } = await seedSession(creator.account, [[editor.account, "editor"]]);

    // Two bandmates add a track and a pattern concurrently; neither pre-creates rows.
    const [track, pattern] = await Promise.all([
      creator.db
        .insert(app.tracks, { session_id: session.id, position: 1, name: "Snare", color: "orange" })
        .wait({ tier: "global" }),
      editor.db
        .insert(app.patterns, {
          session_id: session.id,
          position: 1,
          name: "Pattern 2",
          length: 16,
        })
        .wait({ tier: "global" }),
    ]);

    // Both then press the same, never-written pad. The derived id makes that one row.
    const address = {
      sessionId: session.id,
      trackId: track.id,
      patternId: pattern.id,
      position: 3,
    };
    const fromCreator = stepRow(address, true);
    const fromEditor = stepRow(address, true);
    expect(fromEditor.id).toBe(fromCreator.id);
    // Awaited one after the other: two in-flight upserts of one id abort a
    // trusted-serving session db today (https://github.com/garden-co/jazz/issues/3758).
    // The app's own pad presses are client-local writes and are unaffected.
    // Order does not matter to the outcome: both writers target the same row.
    await creator.db.upsert(app.steps, fromCreator.id, fromCreator.data).wait({ tier: "global" });
    await editor.db.upsert(app.steps, fromEditor.id, fromEditor.data).wait({ tier: "global" });

    const rows = await creator.db.all(
      app.steps.where({ track_id: track.id, pattern_id: pattern.id }),
      { tier: "global" },
    );
    expect(rows.map((row) => [row.position, row.enabled])).toEqual([[3, true]]);
  }, 30_000);

  it("shows a bandmate's name only after they are present in a shared session", async () => {
    const creator = bandmate("creator");
    const editor = bandmate("editor");
    const stranger = bandmate("stranger");
    const { session } = await seedSession(creator.account, [[editor.account, "editor"]]);
    const profile = await testApp.seed((db) =>
      db.insert(app.profiles, { author: editor.account, displayName: "Edie" }),
    );
    await testApp.seed((db) =>
      db.insert(app.presence, {
        session_id: session.id,
        profile_id: profile.id,
        cursor_step: 0,
        heartbeat_at: new Date(),
      }),
    );

    await expect
      .poll(async () =>
        (await creator.db.all(app.profiles, { tier: "global" })).map((p) => p.displayName),
      )
      .toEqual(["Edie"]);
    await expect(stranger.db.all(app.profiles, { tier: "global" })).resolves.toEqual([]);
  }, 30_000);
});
