import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { withGroupTopologyPermissions } from "./group-topology.js";
import { createNativeCrypto } from "./native.js";

it("inspects inherited recovery coverage without repairing or staging group keys", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
    const policies = definePermissions(app, ({ policy, session }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_membership.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_group_recovery_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_recovery_deliveries.allowInsert.where({
        senderAccountId: session.user.account,
      });
      policy.__e2ee_group_repairs.allowRead.where(authenticated);
      policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
    });
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: withGroupTopologyPermissions(app, { ...deviceRequestPermissions, ...policies }),
    });
    const adapters = await createNativeCrypto();
    let corrupt = false;
    let injected = 0;
    const open = async (
      account: Awaited<ReturnType<typeof localAccountConfig>>,
      inspect = false,
    ) => {
      let saved: string | null = null;
      const db = await createDb({
        ...account,
        e2ee: {
          app,
          store: {
            async read() {
              return saved;
            },
            async update(transform) {
              saved = transform(saved);
            },
          },
          crypto: {
            ...adapters,
            keyEnvelope: {
              ...adapters.keyEnvelope,
              async open(pair, context, envelope) {
                if (
                  inspect &&
                  corrupt &&
                  new TextDecoder().decode(context).includes("__e2ee_group_recovery_deliveries")
                ) {
                  injected++;
                  return new Uint8Array(32).fill(9);
                }
                return adapters.keyEnvelope.open(pair, context, envelope);
              },
            },
          },
        },
      });
      clients.push(db);
      return { db, saved: () => saved };
    };
    const { db: alice } = await open(await localAccountConfig(server.appId, server.url));
    const bobAccount = await localAccountConfig(server.appId, server.url);
    const { db: bob } = await open(bobAccount);
    const { material } = await bob.e2ee.recovery.create().wait();
    const parent = alice.e2ee.groups.create();
    await parent.wait();
    const child = bob.e2ee.groups.create();
    await child.wait();
    // Bob may administer this edge through ordinary policy, but has no parent key to share.
    await bob.e2ee.groups.add(parent.id, child.id).wait();
    const observer = await open(bobAccount, true);
    const requests = await bob.all(app.__e2ee_device_requests, { tier: "edge" });
    const deliveries = await bob.all(app.__e2ee_group_recovery_deliveries, { tier: "edge" });
    const missing = await observer.db.e2ee.recovery.status(material);
    expect(missing.account.validation).toBe("validated");
    expect(missing.groups).toMatchObject({
      validation: "checked",
      paths: expect.arrayContaining([
        expect.objectContaining({
          groupId: parent.id,
          validation: "unavailable",
          reason: "missing-recovery-delivery",
        }),
        expect.objectContaining({ groupId: child.id, validation: "validated" }),
      ]),
    });
    if (missing.groups.validation !== "checked") throw new Error("Group coverage was not checked");
    expect(missing.groups.paths).toHaveLength(2);
    expect(observer.saved()).toBeNull();
    expect(await bob.all(app.__e2ee_group_recovery_deliveries, { tier: "edge" })).toEqual(
      deliveries,
    );
    expect(await bob.all(app.__e2ee_device_requests, { tier: "edge" })).toEqual(requests);
    await alice.e2ee.explain({ groupId: parent.id });
    const ready = await observer.db.e2ee.recovery.status(material);
    if (ready.groups.validation !== "checked") throw new Error("Group coverage was not checked");
    expect(ready.groups.paths).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          groupId: parent.id,
          epochId: expect.any(String),
          validation: "validated",
        }),
        expect.objectContaining({
          groupId: child.id,
          epochId: expect.any(String),
          validation: "validated",
        }),
      ]),
    );
    corrupt = true;
    const faulty = await observer.db.e2ee.recovery.status(material);
    if (faulty.groups.validation !== "checked") throw new Error("Group coverage was not checked");
    expect(injected).toBeGreaterThan(0);
    expect(faulty.groups.paths).toHaveLength(2);
    for (const path of faulty.groups.paths)
      expect(path).toMatchObject({
        validation: "unavailable",
        reason: "unusable-recovery-delivery",
      });
    corrupt = false;
    // A departing member cannot create the replacement epoch. Inspection must
    // report that maintenance is pending without doing it on the observer's behalf.
    await alice.e2ee.groups.leave(parent.id).wait();
    const maintenance = await observer.db.e2ee.recovery.status(material);
    if (maintenance.groups.validation !== "checked")
      throw new Error("Group coverage was not checked");
    expect(maintenance.groups.paths).toContainEqual(
      expect.objectContaining({
        groupId: parent.id,
        validation: "unavailable",
        reason: "maintenance-required",
      }),
    );
    expect(observer.saved()).toBeNull();
    expect(await bob.e2ee.explain({ groupId: parent.id })).toEqual({ state: "ready" });
    const rotated = await observer.db.e2ee.recovery.status(material);
    if (rotated.groups.validation !== "checked") throw new Error("Group coverage was not checked");
    const parentPath = rotated.groups.paths.find((path) => path.groupId === parent.id)!;
    expect(parentPath.validation).toBe("validated");
    expect(parentPath.epochId).not.toBe(
      ready.groups.paths.find((path) => path.groupId === parent.id)!.epochId,
    );
    await alice.e2ee.groups.remove(parent.id, child.id).wait();
    const removed = await observer.db.e2ee.recovery.status(material);
    if (removed.groups.validation !== "checked") throw new Error("Group coverage was not checked");
    expect(removed.groups.paths).toHaveLength(1);
    expect(removed.groups.paths[0]).toMatchObject({ groupId: child.id, validation: "validated" });
    expect(observer.saved()).toBeNull();
    expect(await bob.all(app.__e2ee_device_requests, { tier: "edge" })).toEqual(requests);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 180000);
