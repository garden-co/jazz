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
  it("admits stage-manager writes, limits members to suggestions, and revokes both exactly", async () => {
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

    // The app discovers a grant first, then resolves the workspace through
    // the correlated membership EXISTS arm. Keep that authorization shape
    // explicit here: the browser topology receipt additionally proves its
    // transport/maintained path.
    expect(
      await manager.all(app.members.where({ id: managerMembership.id, subject: "manager" }), {
        tier: "edge",
      }),
    ).toEqual([managerMembership]);
    expect(await manager.all(app.workspaces.where({ id: workspace.id }), { tier: "edge" })).toEqual(
      [workspace],
    );

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

  it("rejects cross-workspace UUIDs for every recursive and block-dependent relation", async () => {
    const owner = testApp.as(session("owner"));
    const workspaceA = await owner
      .insert(app.workspaces, { name: "A", ownerSubject: "owner" })
      .wait({ tier: "edge" });
    const workspaceB = await owner
      .insert(app.workspaces, { name: "B", ownerSubject: "owner" })
      .wait({ tier: "edge" });
    for (const workspaceId of [workspaceA.id, workspaceB.id]) {
      await owner
        .insert(app.members, { workspaceId, subject: "owner", role: "owner" })
        .wait({ tier: "edge" });
    }
    const pageA = await owner
      .insert(app.pages, { workspaceId: workspaceA.id, title: "A" })
      .wait({ tier: "edge" });
    const pageB = await owner
      .insert(app.pages, { workspaceId: workspaceB.id, title: "B" })
      .wait({ tier: "edge" });
    await owner.expectDenied((db) =>
      db.insert(app.pages, {
        workspaceId: workspaceA.id,
        parentPageId: pageB.id,
        title: "cross-workspace child",
      }),
    );
    await owner.expectDenied((db) =>
      db.insert(app.blocks, {
        workspaceId: workspaceA.id,
        pageId: pageB.id,
        position: 1,
        kind: "text",
        payload: {},
      }),
    );
    await owner.expectDenied((db) => db.update(app.pages, pageA.id, { parentPageId: pageB.id }));
    const blockA = await owner
      .insert(app.blocks, {
        workspaceId: workspaceA.id,
        pageId: pageA.id,
        position: 1,
        kind: "text",
        payload: {},
      })
      .wait({ tier: "edge" });
    const blockB = await owner
      .insert(app.blocks, {
        workspaceId: workspaceB.id,
        pageId: pageB.id,
        position: 1,
        kind: "text",
        payload: {},
      })
      .wait({ tier: "edge" });
    await owner.expectDenied((db) => db.update(app.blocks, blockA.id, { pageId: pageB.id }));
    await owner.expectDenied((db) =>
      db.update(app.blocks, blockA.id, { parentBlockId: blockB.id }),
    );
    await owner.expectDenied((db) =>
      db.insert(app.blocks, {
        workspaceId: workspaceA.id,
        pageId: pageA.id,
        parentBlockId: blockB.id,
        position: 2,
        kind: "text",
        payload: {},
      }),
    );
    await owner.expectDenied((db) =>
      db.insert(app.tasks, {
        workspaceId: workspaceA.id,
        blockId: blockB.id,
        title: "cross task",
        completed: false,
      }),
    );
    await owner.expectDenied((db) =>
      db.insert(app.calendarEvents, {
        workspaceId: workspaceA.id,
        blockId: blockB.id,
        title: "cross event",
        startsAt: new Date(0),
        endsAt: new Date(1),
      }),
    );
    await owner.expectDenied((db) =>
      db.insert(app.songs, { workspaceId: workspaceA.id, blockId: blockB.id, title: "cross song" }),
    );
    await owner.expectDenied((db) =>
      db.insert(app.suggestions, {
        workspaceId: workspaceA.id,
        blockId: blockB.id,
        payload: {},
        status: "open",
      }),
    );
    await owner.expectDenied((db) =>
      db.insert(app.attachments, {
        workspaceId: workspaceA.id,
        blockId: blockB.id,
        name: "cross",
        mediaType: "text/plain",
        bytes: new Uint8Array(),
      }),
    );
    const task = await owner
      .insert(app.tasks, {
        workspaceId: workspaceA.id,
        blockId: blockA.id,
        title: "valid",
        completed: false,
      })
      .wait({ tier: "edge" });
    const event = await owner
      .insert(app.calendarEvents, {
        workspaceId: workspaceA.id,
        blockId: blockA.id,
        title: "valid",
        startsAt: new Date(0),
        endsAt: new Date(1),
      })
      .wait({ tier: "edge" });
    const song = await owner
      .insert(app.songs, {
        workspaceId: workspaceA.id,
        blockId: blockA.id,
        title: "valid",
      })
      .wait({ tier: "edge" });
    const suggestion = await owner
      .insert(app.suggestions, {
        workspaceId: workspaceA.id,
        blockId: blockA.id,
        payload: {},
        status: "open",
      })
      .wait({ tier: "edge" });
    const attachment = await owner
      .insert(app.attachments, {
        workspaceId: workspaceA.id,
        blockId: blockA.id,
        name: "valid",
        mediaType: "text/plain",
        bytes: new Uint8Array(),
      })
      .wait({ tier: "edge" });
    await owner.expectDenied((db) => db.update(app.tasks, task.id, { blockId: blockB.id }));
    await owner.expectDenied((db) =>
      db.update(app.calendarEvents, event.id, { blockId: blockB.id }),
    );
    await owner.expectDenied((db) => db.update(app.songs, song.id, { blockId: blockB.id }));
    await owner.expectDenied((db) =>
      db.update(app.suggestions, suggestion.id, { blockId: blockB.id }),
    );
    await owner.expectDenied((db) =>
      db.update(app.attachments, attachment.id, { blockId: blockB.id }),
    );
  });
});
