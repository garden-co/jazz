import { expect, it } from "vitest";
import { app, fixture } from "./fixtures/space-recovery-discovery-scenarios.js";
it("discovers, backfills and recovers recipient spaces, then excludes removed membership", async () => {
  const f = await fixture();
  try {
    // Create recovery before any space exists. A grant authored by the recipient
    // establishes membership without giving it a key or a recovery envelope.
    const { material } = await f.recipient.e2ee.recovery.create().wait();
    const inherited = await f.project("Undelivered inherited access");
    await f.recipient.e2ee.spaces.grant(app.projects, inherited.identifier, f.parent.id).wait();
    const missing = await f.recipient.e2ee.recovery.status(material);
    expect(missing.spaces).toMatchObject({
      validation: "checked",
      paths: [
        {
          identifier: inherited.identifier,
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
    expect(await f.owner.e2ee.explain(inherited)).toEqual({ state: "ready" });
    const direct = await f.project("Direct noncreator access", f.recipientAccount.account.id);
    expect(await f.recipient.e2ee.explain(direct)).toEqual({ state: "ready" });
    expect(await f.recipient.e2ee.explain(inherited)).toEqual({ state: "ready" });
    const before = await f.recipient.e2ee.recovery.status(material);
    expect(before.spaces).toMatchObject({ validation: "checked" });
    if (before.spaces.validation !== "checked") throw new Error("Space recovery was not checked");
    expect(before.spaces.paths).toHaveLength(2);
    expect(before.spaces.paths).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ identifier: direct.identifier, validation: "validated" }),
        expect.objectContaining({ identifier: inherited.identifier, validation: "validated" }),
      ]),
    );

    // This ordinary, canonical space is readable, but grants no access to the recipient.
    const unrelated = await f.project("Unrelated space");
    f.failRoot(unrelated.identifier);
    await expect(f.recipient.e2ee.explain(unrelated)).rejects.toBe(f.verifierError);
    const observer = await f.open(f.recipientAccount, true);
    // Assert the first cold status result, before any recovery operation can warm it.
    const after = await observer.e2ee.recovery.status(material);
    expect(after.spaces).toEqual(before.spaces);
    // Fresh read-only recovery must propagate a required root's operational fault;
    // a live Spaces cache need not reverify after only adapter closure state changes.
    const requiredReader = await f.open(f.recipientAccount, true);
    f.failRoot(inherited.identifier);
    await expect(requiredReader.e2ee.recovery.status(material)).rejects.toBe(f.verifierError);
    f.failRoot();
    expect((await requiredReader.e2ee.recovery.status(material)).spaces).toEqual(before.spaces);
    await f.close(observer);
    await f.close(requiredReader);
    f.failRoot(unrelated.identifier);
    const originalRootId = before.account.validatedRootId;
    const accountDeliveries = app.__e2ee_recovery_deliveries.where({ rootId: originalRootId });
    const groupDeliveries = app.__e2ee_group_recovery_deliveries.where({
      recoveryRootId: originalRootId,
    });
    const spaceDeliveries = app.__e2ee_space_recovery_deliveries.where({
      recoveryRootId: originalRootId,
    });
    const retained = await Promise.all([
      f.recipient.all(accountDeliveries, { tier: "global" }),
      f.recipient.all(groupDeliveries, { tier: "global" }),
      f.recipient.all(spaceDeliveries, { tier: "global" }),
    ]);
    const created = await f.recipient.e2ee.recovery.create().wait();
    expect((await f.recipient.e2ee.recovery.status(created.material)).spaces).toEqual(
      before.spaces,
    );
    // Exact envelope preservation below and the later cold recovery with the
    // original material cover continued access without repeating warm reads.
    const afterward = await Promise.all([
      f.recipient.all(accountDeliveries, { tier: "global" }),
      f.recipient.all(groupDeliveries, { tier: "global" }),
      f.recipient.all(spaceDeliveries, { tier: "global" }),
    ]);
    const ordered = (rows: { id: string }[]) => [...rows].sort((a, b) => a.id.localeCompare(b.id));
    expect(afterward.map(ordered)).toEqual(retained.map(ordered));

    // Exercise use on a different fresh store, after closing every previous
    // client. Neither status inspection nor a live holder can supply its keys.
    await f.close(f.owner);
    await f.close(f.recipient);
    const recovered = await f.open(f.recipientAccount, true);
    const pending = (await recovered.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    );
    expect(pending).toBeDefined();
    // Use the original material: creating another recovery must preserve it.
    await recovered.e2ee.recovery.use(material).wait();
    expect((await recovered.e2ee.recovery.status(material)).spaces).toEqual(before.spaces);
    expect(await recovered.e2ee.explain(direct)).toEqual({ state: "ready" });
    expect(await recovered.e2ee.explain(inherited)).toEqual({ state: "ready" });
    expect(await recovered.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending!.id, state: "active", keyReadiness: "verified" }),
    );
    // The recovered device has both keys. Removing the independent direct grant
    // leaves only the nested path; leaving its child must remove that path too.
    await recovered.e2ee.spaces
      .revoke(app.projects, direct.identifier, f.recipientAccount.account.id)
      .wait();
    await recovered.e2ee.groups.leave(f.child.id).wait();
    expect(await recovered.e2ee.recovery.status(created.material)).toMatchObject({
      spaces: { validation: "checked", paths: [] },
    });
    const removedRecovery = await recovered.e2ee.recovery.create().wait();
    await recovered.e2ee.recovery.use(removedRecovery.material).wait();
    expect(await recovered.e2ee.explain(inherited)).toMatchObject({
      state: "refused",
      reason: "not-a-space-recipient",
    });
  } finally {
    await f.cleanup();
  }
}, 360_000);
