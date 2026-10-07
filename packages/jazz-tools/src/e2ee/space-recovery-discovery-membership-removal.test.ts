import { expect, it } from "vitest";
import { app, fixture } from "./fixtures/space-recovery-discovery-scenarios.js";
it("requires an undelivered nested recipient space and excludes it after membership removal", async () => {
  const f = await fixture();
  try {
    const { material } = await f.recipient.e2ee.recovery.create().wait();
    const target = await f.project("Undelivered inherited access");
    // Ordinary administration permits this grant, but its author has no space key to deliver.
    await f.recipient.e2ee.spaces.grant(app.projects, target.identifier, f.parent.id).wait();
    const missing = await f.recipient.e2ee.recovery.status(material);
    expect(missing.spaces).toMatchObject({
      validation: "checked",
      paths: [
        {
          identifier: target.identifier,
          validation: "unavailable",
          reason: "missing-recovery-delivery",
        },
      ],
    });
    if (missing.spaces.validation !== "checked") throw new Error("Space recovery was not checked");
    expect(missing.spaces.paths).toHaveLength(1);
    await expect(f.recipient.e2ee.recovery.use(material).wait()).rejects.toMatchObject({
      code: "recovery-space-delivery-unavailable",
    });
    await expect(f.recipient.e2ee.recovery.create().wait()).rejects.toThrow(
      "Space key unavailable while creating recovery",
    );

    expect(await f.owner.e2ee.explain(target)).toEqual({ state: "ready" });
    const protectedRecovery = await f.recipient.e2ee.recovery.create().wait();
    expect(await f.recipient.e2ee.recovery.status(protectedRecovery.material)).toMatchObject({
      spaces: {
        validation: "checked",
        paths: [{ identifier: target.identifier, validation: "validated" }],
      },
    });
    await f.recipient.e2ee.groups.leave(f.child.id).wait();
    expect(await f.recipient.e2ee.recovery.status(protectedRecovery.material)).toMatchObject({
      spaces: { validation: "checked", paths: [] },
    });
    const removedRecovery = await f.recipient.e2ee.recovery.create().wait();
    await f.recipient.e2ee.recovery.use(removedRecovery.material).wait();
    expect(await f.recipient.e2ee.explain(target)).toMatchObject({
      state: "refused",
      reason: "not-a-space-recipient",
    });
  } finally {
    await f.cleanup();
  }
}, 180_000);
