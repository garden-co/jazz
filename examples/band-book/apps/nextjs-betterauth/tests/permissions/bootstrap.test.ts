import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { app } from "../../schema.js";
import { ensureDemoWorkspace, seedId } from "../../src/lib/bootstrap.js";
import { redeemInvite } from "../../src/lib/invites.js";
import { deletePageTree } from "../../src/lib/page-actions.js";
import { buildPageTree } from "../../src/lib/tree.js";
import { DEMO_PAGES, flattenSeedPages } from "../../src/lib/seed.js";
import { startAuthority } from "./authority.js";

type Authority = Awaited<ReturnType<typeof startAuthority>>;
let authority: Authority | undefined;
const auth = (): Authority => {
  if (!authority) throw new Error("The local authority did not start");
  return authority;
};
const global = { tier: "global" } as const;

beforeEach(async () => {
  authority = await startAuthority();
});
afterEach(async () => {
  // Tolerate a setup that failed before the authority existed.
  await authority?.shutdown();
  authority = undefined;
});

describe("first-open bootstrap", () => {
  it("creates one deterministic demo band, however often it is retried", async () => {
    const account = crypto.randomUUID();
    const first = await ensureDemoWorkspace(auth().backend, account, "Ada");
    expect(first).toEqual({ workspaceId: seedId(account, "workspace"), created: true });

    // A retry after a lost response, and two tabs racing, change nothing.
    const [again, racing] = await Promise.all([
      ensureDemoWorkspace(auth().backend, account, "Ada"),
      ensureDemoWorkspace(auth().backend, account, "Ada"),
    ]);
    expect(again).toEqual({ workspaceId: first.workspaceId, created: false });
    expect(racing).toEqual({ workspaceId: first.workspaceId, created: false });

    const ada = auth().as("ada", account);
    const workspaces = await ada.all(app.workspaces, global);
    expect(workspaces.map((workspace) => workspace.id)).toEqual([first.workspaceId]);
    const members = await ada.all(app.members.where({ workspaceId: first.workspaceId }), global);
    expect(members.map((member) => [member.account, member.role])).toEqual([[account, "owner"]]);

    const pages = await ada.all(
      app.pages
        .where({ workspaceId: first.workspaceId })
        .select("title", "parentId", "$createdAt")
        .orderBy("$createdAt", "asc"),
      global,
    );
    const seeded = flattenSeedPages(DEMO_PAGES);
    expect(pages.map((page) => page.title)).toEqual(seeded.map((page) => page.title));
    expect(pages.map((page) => page.id)).toEqual(
      seeded.map((page) => seedId(account, `page:${page.key}`)),
    );
    // Top-level pages come back in the order seed.ts lists them.
    expect(pages.filter((page) => page.parentId === null).map((page) => page.title)).toEqual(
      DEMO_PAGES.map((page) => page.title),
    );

    const issues = await ada.all(app.issues.where({ workspaceId: first.workspaceId }), global);
    expect(issues).toHaveLength(4);
  });

  it("gives different accounts separate bands", async () => {
    const ada = crypto.randomUUID();
    const bo = crypto.randomUUID();
    const adaBand = await ensureDemoWorkspace(auth().backend, ada, "Ada");
    const boBand = await ensureDemoWorkspace(auth().backend, bo, "Bo");
    expect(adaBand.workspaceId).not.toBe(boBand.workspaceId);
    const seenByBo = await auth()
      .as("bo", bo)
      .all(app.pages.where({ workspaceId: adaBand.workspaceId }), global);
    expect(seenByBo).toEqual([]);
  });
});

describe("invite links", () => {
  it("admit a guest to one page subtree, idempotently, until revoked", async () => {
    const owner = crypto.randomUUID();
    const guest = crypto.randomUUID();
    const { workspaceId } = await ensureDemoWorkspace(auth().backend, owner, "Ada");
    const song = seedId(owner, "page:harbour-lights");
    const ownerDb = auth().as("ada", owner);
    const invite = await ownerDb
      .insert(app.invites, {
        workspaceId,
        pageId: song,
        role: "editor",
        token: "harbour-lights-invite-token",
        label: "Can edit: Harbour lights",
      })
      .wait(global);

    expect(await redeemInvite(auth().backend, invite.token, guest, "Guest")).toEqual({
      workspaceId,
      pageId: song,
    });
    // Opening the link again does not duplicate anything.
    await redeemInvite(auth().backend, invite.token, guest, "Guest");

    const guestDb = auth().as("guest", guest);
    const titles = (await guestDb.all(app.pages.where({ workspaceId }), global)).map(
      (page) => page.title,
    );
    expect(titles.sort()).toEqual(["Arrangement notes", "Harbour lights"]);
    const grants = await guestDb.all(app.pageGrants.where({ account: guest }), global);
    expect(grants.map((grant) => [grant.pageId, grant.role])).toEqual([[song, "editor"]]);
    const members = await guestDb.all(app.members.where({ workspaceId, account: guest }), global);
    expect(members.map((member) => member.role)).toEqual(["guest"]);

    // A later view-only link never lowers access someone already has.
    const viewLink = await ownerDb
      .insert(app.invites, {
        workspaceId,
        pageId: song,
        role: "viewer",
        token: "harbour-lights-view-token",
        label: "Can view: Harbour lights",
      })
      .wait(global);
    await redeemInvite(auth().backend, viewLink.token, guest, "Guest");
    const after = await guestDb.all(app.pageGrants.where({ account: guest }), global);
    expect(after.map((grant) => grant.role)).toEqual(["editor"]);

    await ownerDb.delete(app.invites, invite.id).wait(global);
    expect(
      await redeemInvite(auth().backend, invite.token, crypto.randomUUID(), "Late"),
    ).toBeNull();
  });

  it("stop working once their page is deleted, taking its grants along", async () => {
    const owner = crypto.randomUUID();
    const guest = crypto.randomUUID();
    const { workspaceId } = await ensureDemoWorkspace(auth().backend, owner, "Ada");
    const song = seedId(owner, "page:harbour-lights");
    const ownerDb = auth().as("ada", owner);
    const invite = await ownerDb
      .insert(app.invites, {
        workspaceId,
        pageId: song,
        role: "editor",
        token: "deleted-page-invite-token",
        label: "Can edit: Harbour lights",
      })
      .wait(global);
    await redeemInvite(auth().backend, invite.token, guest, "Guest");

    const pages = await ownerDb.all(app.pages.where({ workspaceId }), global);
    await deletePageTree(ownerDb, buildPageTree(pages), song, global);

    expect(
      await redeemInvite(auth().backend, invite.token, crypto.randomUUID(), "Late"),
    ).toBeNull();
    const inSubtree = { pageId: song };
    expect(await auth().backend.all(app.invites.where(inSubtree), global)).toEqual([]);
    expect(await auth().backend.all(app.pageGrants.where(inSubtree), global)).toEqual([]);
    expect(await auth().backend.all(app.pages.where({ id: song }), global)).toEqual([]);
  });

  it("promote a guest to a band role with a band invite", async () => {
    const owner = crypto.randomUUID();
    const drummer = crypto.randomUUID();
    const { workspaceId } = await ensureDemoWorkspace(auth().backend, owner, "Ada");
    const invite = await auth()
      .as("ada", owner)
      .insert(app.invites, {
        workspaceId,
        pageId: null,
        role: "member",
        token: "join-the-band-token-0001",
        label: "Band member",
      })
      .wait(global);
    await redeemInvite(auth().backend, invite.token, drummer, "Drummer");
    const drummerDb = auth().as("drummer", drummer);
    const pages = await drummerDb.all(app.pages.where({ workspaceId }), global);
    expect(pages.length).toBe(flattenSeedPages(DEMO_PAGES).length);
    await drummerDb
      .update(app.pages, seedId(owner, "page:setlist"), { title: "Setlist: summer tour" })
      .wait(global);
  });
});
