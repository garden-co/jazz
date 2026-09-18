import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { withGroupTopologyPermissions } from "./group-topology.js";
import { createBrowserDeviceSigner } from "./browser.js";
import { groupMembershipBytes } from "./group-format.js";

it("accepts only one of two overlapping group edges that would form a cycle", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
    const policies = definePermissions(app, ({ policy, session, allOf }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_membership.allowInsert.where((row) =>
        allOf([
          { authorAccountId: session.user.account },
          policy.__e2ee_groups.exists.where({ id: row.groupId, accountId: session.user.account }),
        ]),
      );
      policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
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
    const signer = await createBrowserDeviceSigner();
    let armed = false;
    let arrivals = 0;
    let release!: () => void;
    const barrier = new Promise<void>((resolve) => {
      release = resolve;
    });
    const open = async () => {
      let saved: string | null = null;
      const account = await localAccountConfig(server.appId, server.url);
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
            deviceSigner: {
              ...signer,
              async sign(key, bytes) {
                const signature = await signer.sign(key, bytes);
                // The approved BYOC boundary holds both valid proposals before either
                // transaction can commit. No sleeps or mocked database acceptance.
                if (armed) {
                  arrivals++;
                  if (arrivals === 2) {
                    armed = false;
                    release();
                  }
                  await barrier;
                }
                return signature;
              },
            },
          },
        },
      });
      clients.push(db);
      await db.e2ee.devices.list();
      const group = db.e2ee.groups.create();
      await group.wait();
      return { db, id: group.id, stored: () => saved! };
    };
    const a = await open();
    const b = await open();
    await expect(a.db.e2ee.groups.add(a.id, a.id).wait()).rejects.toThrow("cycle or depth");
    armed = true;
    const results = await Promise.allSettled([
      a.db.e2ee.groups.add(a.id, b.id).wait(),
      b.db.e2ee.groups.add(b.id, a.id).wait(),
    ]);
    expect(arrivals).toBe(2);
    expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
    expect(results.filter((result) => result.status === "rejected")).toHaveLength(1);
    const edges = await a.db.all(app.__e2ee_group_membership, { tier: "edge" });
    expect(edges).toHaveLength(1);
    expect(edges[0]!.memberKind).toBe("group");
    const parent = edges[0]!.groupId === a.id ? a : b;
    const child = parent === a ? b : a;
    expect(edges[0]!.memberId).toBe(child.id);
    expect(await parent.db.e2ee.explain({ groupId: parent.id })).toEqual({ state: "ready" });
    expect(await child.db.e2ee.explain({ groupId: parent.id })).toEqual({ state: "ready" });
    expect(await parent.db.e2ee.explain({ groupId: child.id })).toMatchObject({ state: "refused" });
    await expect(child.db.e2ee.groups.add(child.id, parent.id).wait()).rejects.toThrow(
      "cycle or depth",
    );
    expect(await a.db.all(app.__e2ee_group_membership, { tier: "edge" })).toEqual(edges);
    const root = (await child.db.one(app.__e2ee_groups.where({ id: child.id }), { tier: "edge" }))!;
    const device = JSON.parse(child.stored()).devices[0];
    const key = Uint8Array.from(device.signingPrivateKey);
    const candidate = {
      id: crypto.randomUUID(),
      groupId: child.id,
      epochId: root.epochId,
      authorAccountId: root.accountId,
      authorDeviceId: root.deviceId,
      authorEpochId: root.accountEpochId,
      operation: "add",
      memberKind: "group",
      memberId: parent.id,
    };
    try {
      const bytes = groupMembershipBytes(device.scope, candidate);
      const signature = await signer.sign(key, bytes);
      expect(await signer.verify(Uint8Array.from(device.signingPublicKey), bytes, signature)).toBe(
        true,
      );
      const { id, ...values } = candidate;
      await child.db
        .insert(app.__e2ee_group_membership, { ...values, signature }, { id })
        .wait({ tier: "global" });
    } finally {
      key.fill(0);
    }
    // The ordinary policy admits the administrator's raw row; graph replay
    // must reject the cycle despite the valid signature and durable acceptance.
    expect(await a.db.all(app.__e2ee_group_membership, { tier: "edge" })).toHaveLength(2);
    expect(await parent.db.e2ee.explain({ groupId: child.id })).toMatchObject({ state: "refused" });
    expect(await child.db.e2ee.explain({ groupId: parent.id })).toEqual({ state: "ready" });
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 120000);
