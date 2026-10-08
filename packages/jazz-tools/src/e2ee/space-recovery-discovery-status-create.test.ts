import { expect, it } from "vitest";
import { app, fixture } from "./fixtures/space-recovery-discovery-scenarios.js";
it.each(["status+create"] as const)(
  "discovers only recipient spaces during recovery %s without losing direct or nested access",
  async (operation: "status+create" | "use") => {
    const f = await fixture();
    try {
      const direct = await f.project("Direct noncreator access", f.recipientAccount.account.id);
      const inherited = await f.project("Nested recipient access", f.parent.id);
      expect(await f.recipient.e2ee.explain(direct)).toEqual({ state: "ready" });
      expect(await f.recipient.e2ee.explain(inherited)).toEqual({ state: "ready" });
      const { material } = await f.recipient.e2ee.recovery.create().wait();
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
      if (operation === "use") {
        // Recovery must work without a live holder delivering keys to the new device.
        await f.close(f.owner);
        await f.close(f.recipient);
      }
      // Compare the first cold reader's status directly. Recovery use still
      // needs its own post-operation status observation.
      if (operation === "use") await observer.e2ee.recovery.use(material).wait();
      const after = await observer.e2ee.recovery.status(material);
      expect(after.spaces).toEqual(before.spaces);
      if (operation === "use") {
        expect(await observer.e2ee.explain(direct)).toEqual({ state: "ready" });
        expect(await observer.e2ee.explain(inherited)).toEqual({ state: "ready" });
      }

      if (operation === "status+create") {
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
        expect((await f.recipient.e2ee.recovery.status(material)).spaces).toEqual(before.spaces);
        expect(await f.recipient.e2ee.explain(direct)).toEqual({ state: "ready" });
        expect(await f.recipient.e2ee.explain(inherited)).toEqual({ state: "ready" });
        const afterward = await Promise.all([
          f.recipient.all(accountDeliveries, { tier: "global" }),
          f.recipient.all(groupDeliveries, { tier: "global" }),
          f.recipient.all(spaceDeliveries, { tier: "global" }),
        ]);
        const ordered = (rows: { id: string }[]) =>
          [...rows].sort((a, b) => a.id.localeCompare(b.id));
        expect(afterward.map(ordered)).toEqual(retained.map(ordered));
      }
    } finally {
      await f.cleanup();
    }
  },
  360_000,
);
