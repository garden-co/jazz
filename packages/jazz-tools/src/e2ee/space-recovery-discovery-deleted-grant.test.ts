import { expect, it } from "vitest";
import { app, fixture } from "./fixtures/space-recovery-discovery-scenarios.js";
it("fails closed when the only initial group-recipient grant is deleted", async () => {
  const f = await fixture(true);
  try {
    const target = await f.project("Deleted initial group grant", f.parent.id);
    expect(await f.owner.e2ee.explain(target)).toEqual({ state: "ready" });
    const { material } = await f.owner.e2ee.recovery.create().wait();
    const root = await f.owner.one(app.__e2ee_spaces.where({ identifier: target.identifier }), {
      tier: "global",
    });
    expect(root).not.toBeNull();
    const grants = await f.owner.all(app.__e2ee_space_grants.where({ spaceId: root!.id }), {
      tier: "global",
    });
    expect(grants).toEqual([
      expect.objectContaining({
        id: root!.initialGrantId,
        recipientKind: "group",
        recipientId: f.parent.id,
      }),
    ]);
    await f.owner.delete(app.__e2ee_space_grants, root!.initialGrantId).wait({ tier: "global" });
    // Inspect from the deleting client, with no cross-client propagation race.
    expect(
      await f.owner.all(app.__e2ee_space_grants.where({ spaceId: root!.id }), {
        tier: "global",
      }),
    ).toEqual([]);
    await expect
      .soft(f.owner.e2ee.recovery.status(material))
      .rejects.toThrow("Invalid or unsupported E2EE space membership");
    await expect
      .soft(f.owner.e2ee.recovery.use(material).wait())
      .rejects.toThrow("Invalid or unsupported E2EE space membership");
    await expect
      .soft(f.owner.e2ee.recovery.create().wait())
      .rejects.toThrow("Invalid or unsupported E2EE space membership");
  } finally {
    await f.cleanup();
  }
}, 180_000);
