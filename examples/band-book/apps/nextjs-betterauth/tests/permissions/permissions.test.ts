import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { PermissionAdvice } from "jazz-tools";
import { createPolicyTestApp, type PolicyTestApp, type TestDb } from "jazz-tools/testing";
import { app, type BlockKind, type WorkspaceRole } from "../../schema.js";
import permissions from "../../permissions.js";

/**
 * Policy receipts for BandBook. Every assertion goes through the serving
 * authority (writes wait for `tier: "global"`, reads use `tier: "remote"`), so
 * these check permissions.ts itself, not any UI that hides a button.
 */

let testApp: PolicyTestApp | undefined;
const issuer = "https://band-book.test";
const accounts = new Map<string, string>();

/** One Jazz account per (issuer, subject), as the account registry assigns them. */
function accountFor(subject: string, identityIssuer = issuer): string {
  const key = `${identityIssuer}\u0000${subject}`;
  let account = accounts.get(key);
  if (!account) {
    account = crypto.randomUUID();
    accounts.set(key, account);
  }
  return account;
}

function person(subject: string, identityIssuer = issuer): TestDb {
  if (!testApp) throw new Error("The policy test app did not start");
  return testApp.as({
    issuer: identityIssuer,
    user_id: subject,
    account_id: accountFor(subject, identityIssuer),
    claims: {},
    authMode: "external",
  });
}

const global = { tier: "global" } as const;
const remote = { tier: "remote" } as const;

/**
 * Permission advice as the UI asks for it. "unknown" means Jazz could not give
 * a definite answer yet (for example while the authority connection settles;
 * garden-co/jazz#3750), so ask again, as a client would, before asserting.
 */
async function advice(ask: () => Promise<PermissionAdvice>): Promise<PermissionAdvice> {
  for (let attempt = 0; attempt < 20; attempt++) {
    const answer = await ask();
    if (answer !== "unknown") return answer;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  return "unknown";
}

/** A band created from a client: workspace first, then its owner's own membership. */
async function createBand(owner: TestDb, ownerSubject: string, name = "The Late Shift") {
  const workspace = await owner.insert(app.workspaces, { name }).wait(global);
  await owner
    .insert(app.members, {
      workspaceId: workspace.id,
      account: accountFor(ownerSubject),
      displayName: ownerSubject,
      role: "owner",
    })
    .wait(global);
  return workspace;
}

async function addMember(owner: TestDb, workspaceId: string, subject: string, role: WorkspaceRole) {
  return owner
    .insert(app.members, { workspaceId, account: accountFor(subject), displayName: subject, role })
    .wait(global);
}

async function page(
  db: TestDb,
  workspaceId: string,
  title: string,
  parentId: string | null = null,
  kind: "doc" | "issues" | "issue" = "doc",
) {
  return db.insert(app.pages, { workspaceId, parentId, title, kind }).wait(global);
}

function blockRow(
  workspaceId: string,
  pageId: string,
  text: string,
  kind: BlockKind = "paragraph",
) {
  return {
    workspaceId,
    pageId,
    parentBlockId: null,
    position: 1024,
    kind,
    text,
    checked: false,
    attachmentId: null,
  };
}

async function visibleTitles(db: TestDb, workspaceId: string): Promise<string[]> {
  const pages = await db.all(app.pages.where({ workspaceId }), remote);
  return pages.map((row) => row.title).sort();
}

beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});
afterEach(async () => {
  // Tolerate a setup that failed before the test app existed.
  await testApp?.shutdown();
  testApp = undefined;
});

describe("page grants", () => {
  it("reach every descendant of the granted page and nothing else", async () => {
    const owner = person("owner");
    const guest = person("guest");
    const band = await createBand(owner, "owner");
    const songs = await page(owner, band.id, "Songs");
    const harbour = await page(owner, band.id, "Harbour lights", songs.id);
    const arrangement = await page(owner, band.id, "Arrangement notes", harbour.id);
    const outro = await page(owner, band.id, "Outro ideas", arrangement.id);
    const paper = await page(owner, band.id, "Paper moon", songs.id);
    const tour = await page(owner, band.id, "Tour notes");
    await owner.insert(app.blocks, blockRow(band.id, outro.id, "Hold the last chord")).wait(global);
    await owner.insert(app.blocks, blockRow(band.id, tour.id, "Load-in 16:00")).wait(global);

    // Before the grant the guest sees nothing of the band.
    expect(await visibleTitles(guest, band.id)).toEqual([]);

    await owner
      .insert(app.pageGrants, {
        workspaceId: band.id,
        pageId: harbour.id,
        account: accountFor("guest"),
        role: "editor",
      })
      .wait(global);

    // The granted page and all pages below it, three levels deep; no parent,
    // no sibling song, no other top-level page.
    expect(await visibleTitles(guest, band.id)).toEqual([
      "Arrangement notes",
      "Harbour lights",
      "Outro ideas",
    ]);
    const blocks = await guest.all(app.blocks.where({ workspaceId: band.id }), remote);
    expect(blocks.map((block) => block.text)).toEqual(["Hold the last chord"]);

    // Edit access is inherited too: the guest writes deep inside the grant...
    await guest.insert(app.blocks, blockRow(band.id, outro.id, "Fade on the organ")).wait(global);
    const subpage = await page(guest, band.id, "Bridge sketch", arrangement.id);
    expect(subpage.parentId).toBe(arrangement.id);
    // ...and can rename the granted page itself,
    await guest.update(app.pages, harbour.id, { title: "Harbour lights (v2)" }).wait(global);

    // but not outside it, nor restructure the page they were invited to.
    await guest.expectDenied((db) =>
      db.insert(app.blocks, blockRow(band.id, tour.id, "Cancel Porto")),
    );
    await guest.expectDenied((db) =>
      db.insert(app.blocks, blockRow(band.id, paper.id, "New verse")),
    );
    await guest.expectDenied((db) =>
      db.insert(app.pages, {
        workspaceId: band.id,
        parentId: songs.id,
        title: "Mine",
        kind: "doc",
      }),
    );
    await guest.expectDenied((db) =>
      db.insert(app.pages, { workspaceId: band.id, parentId: null, title: "Top", kind: "doc" }),
    );
    await guest.expectDenied((db) => db.update(app.pages, harbour.id, { parentId: null }));
    await guest.expectDenied((db) => db.delete(app.pages, harbour.id));
    // The parent is outside the grant, so the guest cannot even read it. An
    // update needs read access to the existing row and is refused on the spot.
    expect(() => guest.update(app.pages, songs.id, { title: "Hijacked" })).toThrow(
      /read policy denied UPDATE/,
    );
    const songsNow = await owner.all(app.pages.where({ id: songs.id }), remote);
    expect(songsNow.map((row) => [row.title, row.parentId])).toEqual([["Songs", null]]);
    // Deleting inside the grant is fine: access comes from above.
    await guest.delete(app.pages, subpage.id).wait(global);
  });

  it("are what the permission advice behind the UI's controls reports", async () => {
    const owner = person("owner");
    const guest = person("guest");
    const band = await createBand(owner, "owner");
    const songs = await page(owner, band.id, "Songs");
    const harbour = await page(owner, band.id, "Harbour lights", songs.id);
    const notes = await page(owner, band.id, "Arrangement notes", harbour.id);
    await owner
      .insert(app.pageGrants, {
        workspaceId: band.id,
        pageId: harbour.id,
        account: accountFor("guest"),
        role: "editor",
      })
      .wait(global);
    await guest.all(app.pages.where({ workspaceId: band.id }), remote);

    // Edit: the granted page and below. Restructure: only below it.
    expect(await advice(() => guest.canUpdate(app.pages, harbour.id, { title: "x" }))).toBe(
      "allowed",
    );
    expect(await advice(() => guest.canUpdate(app.pages, notes.id, { title: "x" }))).toBe(
      "allowed",
    );
    expect(await advice(() => guest.canDelete(app.pages, notes.id))).toBe("allowed");
    expect(await advice(() => guest.canDelete(app.pages, harbour.id))).toBe("denied");
    // Sharing is for owners and band members.
    const grant = {
      workspaceId: band.id,
      pageId: harbour.id,
      account: accountFor("x"),
      role: "viewer" as const,
    };
    expect(await advice(() => guest.canInsert(app.pageGrants, grant))).toBe("denied");
    expect(await advice(() => owner.canInsert(app.pageGrants, grant))).toBe("allowed");
  });

  it("stop applying the moment the grant is removed", async () => {
    const owner = person("owner");
    const guest = person("guest");
    const band = await createBand(owner, "owner");
    const song = await page(owner, band.id, "Harbour lights");
    const grant = await owner
      .insert(app.pageGrants, {
        workspaceId: band.id,
        pageId: song.id,
        account: accountFor("guest"),
        role: "editor",
      })
      .wait(global);
    await guest.insert(app.blocks, blockRow(band.id, song.id, "First idea")).wait(global);

    await owner.delete(app.pageGrants, grant.id).wait(global);

    expect(await visibleTitles(guest, band.id)).toEqual([]);
    await guest.expectDenied((db) =>
      db.insert(app.blocks, blockRow(band.id, song.id, "Second idea")),
    );
  });

  it("can only be handed out by owners and band members", async () => {
    const owner = person("owner");
    const band = await createBand(owner, "owner");
    await addMember(owner, band.id, "crew", "viewer");
    const song = await page(owner, band.id, "Harbour lights");
    const guest = person("guest");
    await owner
      .insert(app.pageGrants, {
        workspaceId: band.id,
        pageId: song.id,
        account: accountFor("guest"),
        role: "editor",
      })
      .wait(global);

    const grantFor = (subject: string) => ({
      workspaceId: band.id,
      pageId: song.id,
      account: accountFor(subject),
      role: "editor" as const,
    });
    // A viewer cannot hand out access, and a guest editor cannot re-share.
    await person("crew").expectDenied((db) => db.insert(app.pageGrants, grantFor("friend")));
    await guest.expectDenied((db) => db.insert(app.pageGrants, grantFor("friend")));
    await guest.expectDenied((db) =>
      db.insert(app.invites, {
        workspaceId: band.id,
        pageId: song.id,
        role: "editor",
        token: "guest-made-token-000000",
        label: "sneaky",
      }),
    );
    // Guests cannot read invite tokens either, even for the page they edit.
    await owner
      .insert(app.invites, {
        workspaceId: band.id,
        pageId: song.id,
        role: "viewer",
        token: "owner-made-token-000000",
        label: "Can view: Harbour lights",
      })
      .wait(global);
    expect(await guest.all(app.invites.where({ workspaceId: band.id }), remote)).toEqual([]);
    expect(await person("crew").all(app.invites.where({ workspaceId: band.id }), remote)).toEqual(
      [],
    );
  });
});

describe("viewers", () => {
  it("read the whole band but cannot change anything", async () => {
    const owner = person("owner");
    const crew = person("crew");
    const band = await createBand(owner, "owner");
    await addMember(owner, band.id, "crew", "viewer");
    const setlist = await page(owner, band.id, "Setlist");
    const encore = await page(owner, band.id, "Encore", setlist.id);
    const block = await owner
      .insert(app.blocks, blockRow(band.id, encore.id, "Last orders"))
      .wait(global);

    expect(await visibleTitles(crew, band.id)).toEqual(["Encore", "Setlist"]);

    await crew.expectDenied((db) =>
      db.insert(app.blocks, blockRow(band.id, encore.id, "Extra song")),
    );
    await crew.expectDenied((db) => db.update(app.blocks, block.id, { text: "Changed" }));
    await crew.expectDenied((db) => db.delete(app.blocks, block.id));
    await crew.expectDenied((db) => db.update(app.pages, setlist.id, { title: "Changed" }));
    await crew.expectDenied((db) =>
      db.insert(app.pages, {
        workspaceId: band.id,
        parentId: setlist.id,
        title: "Mine",
        kind: "doc",
      }),
    );
    // Crew cannot promote themselves.
    const [crewRow] = await crew.all(
      app.members.where({ workspaceId: band.id, account: accountFor("crew") }),
      remote,
    );
    await crew.expectDenied((db) => db.update(app.members, crewRow!.id, { role: "owner" }));
  });

  it("with a page-scoped viewer grant see only that subtree, read-only", async () => {
    const owner = person("owner");
    const guest = person("guest");
    const band = await createBand(owner, "owner");
    const tour = await page(owner, band.id, "Tour notes");
    const lisbon = await page(owner, band.id, "Lisbon", tour.id);
    await page(owner, band.id, "Setlist");
    await owner
      .insert(app.pageGrants, {
        workspaceId: band.id,
        pageId: tour.id,
        account: accountFor("guest"),
        role: "viewer",
      })
      .wait(global);

    expect(await visibleTitles(guest, band.id)).toEqual(["Lisbon", "Tour notes"]);
    await guest.expectDenied((db) =>
      db.insert(app.blocks, blockRow(band.id, lisbon.id, "Venue moved")),
    );
    await guest.expectDenied((db) => db.update(app.pages, lisbon.id, { title: "Changed" }));
  });
});

describe("workspace isolation", () => {
  it("keeps bands apart, including against grafting pages across them", async () => {
    const alice = person("alice");
    const bob = person("bob");
    const aliceBand = await createBand(alice, "alice", "Alice's band");
    const bobBand = await createBand(bob, "bob", "Bob's band");
    const aliceSong = await page(alice, aliceBand.id, "Private song");
    const bobSong = await page(bob, bobBand.id, "Bob's song");

    expect(await visibleTitles(bob, aliceBand.id)).toEqual([]);
    expect(await bob.all(app.workspaces.where({ id: aliceBand.id }), remote)).toEqual([]);

    // Writing into another band directly...
    await bob.expectDenied((db) =>
      db.insert(app.pages, {
        workspaceId: aliceBand.id,
        parentId: null,
        title: "Spam",
        kind: "doc",
      }),
    );
    await bob.expectDenied((db) =>
      db.insert(app.blocks, blockRow(aliceBand.id, aliceSong.id, "Spam")),
    );
    // ...or by claiming your own band while pointing at theirs.
    await bob.expectDenied((db) =>
      db.insert(app.pages, {
        workspaceId: bobBand.id,
        parentId: aliceSong.id,
        title: "Graft",
        kind: "doc",
      }),
    );
    await bob.expectDenied((db) =>
      db.insert(app.blocks, blockRow(bobBand.id, aliceSong.id, "Graft")),
    );
    await bob.expectDenied((db) =>
      db.insert(app.pageGrants, {
        workspaceId: bobBand.id,
        pageId: aliceSong.id,
        account: accountFor("bob"),
        role: "editor",
      }),
    );
    // Moving your own page under another band's page is a graft too.
    await bob.expectDenied((db) => db.update(app.pages, bobSong.id, { parentId: aliceSong.id }));
    // Nobody can make themselves a member of someone else's band.
    await bob.expectDenied((db) =>
      db.insert(app.members, {
        workspaceId: aliceBand.id,
        account: accountFor("bob"),
        displayName: "bob",
        role: "owner",
      }),
    );
  });

  it("treats the same subject from another issuer as a different person", async () => {
    const owner = person("owner");
    const band = await createBand(owner, "owner");
    await addMember(owner, band.id, "shared-subject", "member");
    const song = await page(owner, band.id, "Harbour lights");
    const impostor = person("shared-subject", "https://other-provider.test");

    expect(await visibleTitles(person("shared-subject"), band.id)).toEqual(["Harbour lights"]);
    expect(await visibleTitles(impostor, band.id)).toEqual([]);
    await impostor.expectDenied((db) => db.insert(app.blocks, blockRow(band.id, song.id, "Hi")));
  });
});

describe("issues", () => {
  it("live under their database page and follow its access", async () => {
    const owner = person("owner");
    const member = person("member");
    const guest = person("guest");
    const band = await createBand(owner, "owner");
    await addMember(owner, band.id, "member", "member");
    const database = await page(owner, band.id, "Issues", null, "issues");
    const other = await page(owner, band.id, "Setlist");

    const issuePage = await page(member, band.id, "Restring the bass", database.id, "issue");
    const issue = await member
      .insert(app.issues, {
        workspaceId: band.id,
        pageId: issuePage.id,
        databaseId: database.id,
        status: "todo",
        priority: "high",
        assignee: accountFor("member"),
        labels: ["Gear"],
      })
      .wait(global);
    await member.update(app.issues, issue.id, { status: "in_progress" }).wait(global);

    // An issue row must describe a page that really is in that database.
    const stray = await page(member, band.id, "Stray", other.id, "issue");
    await member.expectDenied((db) =>
      db.insert(app.issues, {
        workspaceId: band.id,
        pageId: stray.id,
        databaseId: database.id,
        status: "todo",
        priority: "none",
        assignee: null,
        labels: [],
      }),
    );
    await member.expectDenied((db) => db.update(app.issues, issue.id, { databaseId: other.id }));

    // A guest who may view the tracker reads issues but cannot move them.
    await owner
      .insert(app.pageGrants, {
        workspaceId: band.id,
        pageId: database.id,
        account: accountFor("guest"),
        role: "viewer",
      })
      .wait(global);
    const seen = await guest.all(app.issues.where({ databaseId: database.id }), remote);
    expect(seen.map((row) => row.status)).toEqual(["in_progress"]);
    await guest.expectDenied((db) => db.update(app.issues, issue.id, { status: "done" }));
  });
});

describe("ordering", () => {
  it("orders sibling pages by their real $createdAt", async () => {
    const owner = person("owner");
    const band = await createBand(owner, "owner");
    const parent = await page(owner, band.id, "Tour notes");
    // Titles deliberately sort differently from creation order.
    const created = [];
    for (const title of ["Porto", "Lisbon", "Madrid"]) {
      created.push(await page(owner, band.id, title, parent.id));
    }

    const rows = await owner.all(
      app.pages
        .where({ parentId: parent.id })
        .select("title", "$createdAt")
        .orderBy("$createdAt", "asc"),
      remote,
    );
    expect(rows.map((row) => row.title)).toEqual(["Porto", "Lisbon", "Madrid"]);
    const times = rows.map((row) => new Date(row.$createdAt as Date | number).getTime());
    expect(times[0]).toBeLessThan(times[1]!);
    expect(times[1]).toBeLessThan(times[2]!);
    expect(rows.map((row) => row.id)).toEqual(created.map((row) => row.id));
  });
});
