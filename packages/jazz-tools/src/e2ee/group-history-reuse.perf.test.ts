import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { createNativeDeviceSigner } from "./native.js";

it.skipIf(process.env.JAZZ_E2EE_HISTORY_PERF !== "1")(
  "reuses unchanged group membership validation without hiding removal",
  async () => {
    const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
    const policies = definePermissions(app, ({ policy, session }) => {
      policy.__e2ee_groups.allowRead.always();
      policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_membership.allowRead.always();
      policy.__e2ee_group_membership.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_successors.allowRead.always();
      policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_deliveries.allowRead.always();
      policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_group_repairs.allowRead.always();
      policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
    });
    const permissions = { ...deviceRequestPermissions, ...policies };
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const sessions: Awaited<ReturnType<typeof createJazzSession>>[] = [];
    const memoryStore = () => {
      let value: string | null = null;
      return {
        async read() {
          return value;
        },
        async update(transform: (current: string | null) => string) {
          value = transform(value);
        },
      };
    };
    const signer = await createNativeDeviceSigner();
    let membershipSignature: string | undefined;
    let membershipVerifications = 0;
    const measuredSigner = {
      ...signer,
      async verify(...args: Parameters<typeof signer.verify>) {
        if (Buffer.from(args[2]).toString("base64") === membershipSignature)
          membershipVerifications++;
        return signer.verify(...args);
      },
    };
    const open = async () => {
      const session = await createJazzSession({
        appId: server.appId,
        serverUrl: server.url,
        app,
        permissions,
        driver: { type: "memory" },
        initial: "local-first",
        store: memoryStore(),
        e2ee: { app, store: memoryStore(), crypto: { deviceSigner: measuredSigner } },
      });
      sessions.push(session);
      return session.getSnapshot().client!.db;
    };
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      const owner = await open();
      const other = await open();
      await owner.e2ee.devices.list();
      await other.e2ee.devices.list();
      const otherId = sessions[1]!.getSnapshot().account!.id;
      const group = await owner.e2ee.groups.create().wait();
      await owner.e2ee.groups.add(group.id, otherId).wait();
      const target = { groupId: group.id };
      expect(await other.e2ee.explain(target)).toEqual({ state: "ready" });
      const membership = await owner.one(
        app.__e2ee_group_membership.where({
          groupId: group.id,
          memberId: otherId,
          operation: "add",
        }),
        { tier: "global" },
      );
      expect(membership).toBeDefined();
      membershipSignature = Buffer.from(membership!.signature).toString("base64");
      expect(await owner.e2ee.explain(target)).toEqual({ state: "ready" });
      membershipVerifications = 0;
      for (let sample = 0; sample < 3; sample++)
        expect(await owner.e2ee.explain(target)).toEqual({ state: "ready" });
      expect(membershipVerifications).toBe(0);
      await owner.e2ee.groups.remove(group.id, otherId).wait();
      expect(await other.e2ee.explain(target)).toMatchObject({ state: "refused" });
      expect(await owner.e2ee.explain(target)).toEqual({ state: "ready" });
    } finally {
      await Promise.all(sessions.map((session) => session.close()));
      await server.stop();
    }
  },
  60_000,
);
