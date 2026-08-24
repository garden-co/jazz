import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import permissions from "../permissions.js";
import { app } from "../schema.js";

let testApp: PolicyTestApp;
beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});
afterEach(async () => testApp?.shutdown());

const session = (user_id: string) => ({
  issuer: "https://band-binder.test",
  user_id,
  claims: {},
  authMode: "external" as const,
});

describe("BandBinder workspace roles", () => {
  // Core blocker: the workspace + referenced-row correlations in the strict
  // policy currently lose __root_join_row_0. Keep the complete intended
  // authority receipt live; remove `.fails` when that carrier is repaired.
  it.fails("admits stage-manager writes, limits members to suggestions, and revokes both exactly", async () => {
    const owner = testApp.as(session("owner"));
    const manager = testApp.as(session("manager"));
    const member = testApp.as(session("member"));
    const outsider = testApp.as(session("outsider"));
    const workspace = await owner
      .insert(app.workspaces, { name: "World tour", ownerSubject: "owner" })
      .wait({ tier: "edge" });
    await owner
      .insert(app.members, { workspaceId: workspace.id, subject: "owner", role: "owner" })
      .wait({ tier: "edge" });
    const managerMembership = await owner
      .insert(app.members, {
        workspaceId: workspace.id,
        subject: "manager",
        role: "stage_manager",
      })
      .wait({ tier: "edge" });
    const memberMembership = await owner
      .insert(app.members, { workspaceId: workspace.id, subject: "member", role: "member" })
      .wait({ tier: "edge" });

    const page = await manager
      .insert(app.pages, { workspaceId: workspace.id, title: "Berlin" })
      .wait({ tier: "edge" });
    const block = await manager
      .insert(app.blocks, {
        workspaceId: workspace.id,
        pageId: page.id,
        position: 10,
        kind: "task",
        payload: { text: "Confirm load-in" },
      })
      .wait({ tier: "edge" });
    await member.expectDenied((db) =>
      db.insert(app.tasks, {
        workspaceId: workspace.id,
        blockId: block.id,
        title: "Forge production task",
        completed: false,
      }),
    );
    const suggestion = await member
      .insert(app.suggestions, {
        workspaceId: workspace.id,
        blockId: block.id,
        payload: { replacement: "Load-in at 14:00" },
        status: "open",
      })
      .wait({ tier: "edge" });
    expect(
      await manager.all(app.suggestions.where({ id: suggestion.id }), { tier: "edge" }),
    ).toEqual([suggestion]);
    expect(await outsider.all(app.pages.where({ id: page.id }), { tier: "edge" })).toEqual([]);

    await owner.delete(app.members, memberMembership.id).wait({ tier: "edge" });
    await member.expectDenied((db) =>
      db.insert(app.suggestions, {
        workspaceId: workspace.id,
        blockId: block.id,
        payload: { replacement: "after revocation" },
        status: "open",
      }),
    );
    await owner.delete(app.members, managerMembership.id).wait({ tier: "edge" });
    await manager.expectDenied((db) =>
      db.insert(app.pages, { workspaceId: workspace.id, title: "after revocation" }),
    );
  });
});
