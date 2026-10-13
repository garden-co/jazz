import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { app } from "../../schema.js";
import { ensureDemoWorkspace, seedId } from "../../src/lib/bootstrap.js";
import { redeemInvite } from "../../src/lib/invites.js";
import { startAuthority } from "./authority.js";

type Authority = Awaited<ReturnType<typeof startAuthority>>;
let authority: Authority | undefined;
const auth = (): Authority => {
  if (!authority) throw new Error("The local authority did not start");
  return authority;
};
const global = { tier: "global" } as const;
const remote = { tier: "remote" } as const;

beforeEach(async () => {
  authority = await startAuthority();
});
afterEach(async () => {
  await authority?.shutdown();
  authority = undefined;
});

// React strict mode, a double click or two tabs send the first request twice
// at once. Both answers must succeed, and only one of them writes.
describe("concurrent first requests", () => {
  it("bootstrap one demo band when two first opens race", async () => {
    const account = crypto.randomUUID();
    const results = await Promise.all([
      ensureDemoWorkspace(auth().backend, account, "Ada"),
      ensureDemoWorkspace(auth().backend, account, "Ada"),
    ]);

    const workspaceId = seedId(account, "workspace");
    expect(results.map((result) => result.workspaceId)).toEqual([workspaceId, workspaceId]);
    expect(results.filter((result) => result.created)).toHaveLength(1);
    const members = await auth().as("ada", account).all(app.members.where({ workspaceId }), remote);
    expect(members.map((member) => [member.account, member.role])).toEqual([[account, "owner"]]);
  });

  it("redeem an invite once when the invitee's first two requests race", async () => {
    const owner = crypto.randomUUID();
    const guest = crypto.randomUUID();
    const { workspaceId } = await ensureDemoWorkspace(auth().backend, owner, "Ada");
    const song = seedId(owner, "page:harbour-lights");
    const invite = await auth()
      .as("ada", owner)
      .insert(app.invites, {
        workspaceId,
        pageId: song,
        role: "editor",
        token: "racing-invite-token-harbour",
        label: "Can edit: Harbour lights",
      })
      .wait(global);

    const results = await Promise.all([
      redeemInvite(auth().backend, invite.token, guest, "Guest"),
      redeemInvite(auth().backend, invite.token, guest, "Guest"),
    ]);

    expect(results).toEqual([
      { workspaceId, pageId: song },
      { workspaceId, pageId: song },
    ]);
    const guestDb = auth().as("guest", guest);
    expect(
      await guestDb.all(app.members.where({ workspaceId, account: guest }), remote),
    ).toHaveLength(1);
    expect(await guestDb.all(app.pageGrants.where({ account: guest }), remote)).toHaveLength(1);
  });
});
