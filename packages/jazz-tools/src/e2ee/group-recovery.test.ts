import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { createBrowserKeyEnvelope } from "./browser.js";

it.each([
  "ordinary",
  "read-only-recovered-device",
  "malformed-recovery-candidate",
  "interrupted-read-only-recovery",
] as const)(
  "recovers a group after losing the only enrolled device (%s)",
  async (scenario) => {
    const interrupted = scenario === "interrupted-read-only-recovery";
    const readOnly = scenario === "read-only-recovered-device" || interrupted;
    const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
    const policies = definePermissions(app, ({ policy, session, allOf }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.__e2ee_groups.allowRead.where(authenticated);
      policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_membership.allowRead.where(authenticated);
      policy.__e2ee_group_membership.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_deliveries.allowRead.where(authenticated);
      if (readOnly) {
        policy.__e2ee_group_deliveries.allowInsert.where((row) =>
          allOf([
            { senderAccountId: session.user.account },
            policy.__e2ee_groups.exists.where({ id: row.groupId, deviceId: row.senderDeviceId }),
          ]),
        );
      } else
        policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_group_recovery_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_recovery_deliveries.allowInsert.where({
        senderAccountId: session.user.account,
      });
      policy.__e2ee_group_repairs.allowRead.where(authenticated);
      policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_successors.allowRead.where(authenticated);
      policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    const store = () => {
      let saved: string | null = null;
      return {
        async read() {
          return saved;
        },
        async update(transform: (current: string | null) => string) {
          saved = transform(saved);
        },
      };
    };
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: { ...deviceRequestPermissions, ...policies },
      });
      const account = await localAccountConfig(server.appId, server.url);
      const keys = await createBrowserKeyEnvelope();
      let beforeRecoverySeal: (() => Promise<void>) | undefined;
      const first = await createDb({
        ...account,
        e2ee: {
          app,
          store: store(),
          crypto: {
            keyEnvelope: {
              ...keys,
              async seal(publicKey, context, secret) {
                if (
                  new TextDecoder().decode(context).includes("__e2ee_group_recovery_deliveries")
                ) {
                  const action = beforeRecoverySeal;
                  beforeRecoverySeal = undefined;
                  await action?.();
                }
                return keys.seal(publicKey, context, secret);
              },
            },
          },
        },
      });
      clients.push(first);
      const group = first.e2ee.groups.create();
      await group.wait();
      expect(await first.e2ee.explain({ groupId: group.id })).toEqual({ state: "ready" });
      const additional = interrupted ? first.e2ee.groups.create() : undefined;
      await additional?.wait();
      let injected = false;
      if (scenario === "malformed-recovery-candidate") {
        const writer = await createDb({ ...account });
        clients.push(writer);
        beforeRecoverySeal = async () => {
          const root = await writer.one(app.__e2ee_groups.where({ id: group.id }), {
            tier: "edge",
          });
          const recovery = await writer.all(
            app.__e2ee_recovery_roots.where({ accountId: account.account.id }),
            { tier: "edge" },
          );
          expect(recovery).toHaveLength(1);
          // A UUID v1 is a valid Jazz row ID but is invalid in the E2EE v1 transcript.
          // Publish it before the legitimate envelope, through ordinary account-owned policy.
          await writer
            .insert(
              app.__e2ee_group_recovery_deliveries,
              {
                groupId: group.id,
                epochId: root!.epochId,
                senderAccountId: account.account.id,
                senderDeviceId: root!.deviceId,
                recipientAccountId: account.account.id,
                recoveryRootId: recovery[0]!.id,
                envelope: Uint8Array.of(1),
                signature: new Uint8Array(64),
              },
              { id: "00000000-0000-1000-8000-000000000001" },
            )
            .wait({ tier: "global" });
          injected = true;
        };
      }
      const { material } = await first.e2ee.recovery.create().wait();
      if (scenario === "malformed-recovery-candidate") expect(injected).toBe(true);
      await first.shutdown();

      // No old local store or live key holder is available to the replacement.
      const recoveredStore = store();
      let interrupt = interrupted;
      let recoveryOpens = 0;
      const second = await createDb({
        ...account,
        e2ee: {
          app,
          store: recoveredStore,
          crypto: {
            keyEnvelope: {
              ...keys,
              async open(pair, context, envelope) {
                if (
                  interrupt &&
                  new TextDecoder().decode(context).includes("__e2ee_group_recovery_deliveries") &&
                  ++recoveryOpens === 2
                ) {
                  throw new Error("Injected later group recovery failure");
                }
                return keys.open(pair, context, envelope);
              },
            },
          },
        },
      });
      clients.push(second);
      const pending = (await second.e2ee.devices.list()).find(
        (device) => device.state === "pending",
      )!;
      expect(pending).toBeDefined();
      if (interrupted) {
        await expect(second.e2ee.recovery.use(material).wait()).rejects.toThrow();
        expect(recoveryOpens).toBe(2);
        interrupt = false;
      }
      await second.e2ee.recovery.use(material).wait();
      expect(await second.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({ id: pending.id, state: "active" }),
      );
      expect(await second.e2ee.explain({ groupId: group.id })).toEqual({ state: "ready" });
      if (additional) {
        expect(await second.e2ee.explain({ groupId: additional.id })).toEqual({ state: "ready" });
      }
      if (readOnly) {
        // Recovery makes the key usable; it must not bypass the creator-only delivery policy.
        expect(
          await second.all(
            app.__e2ee_group_deliveries.where({ groupId: group.id, recipientDeviceId: pending.id }),
            { tier: "edge" },
          ),
        ).toEqual([]);
        if (additional) {
          expect(
            await second.all(
              app.__e2ee_group_deliveries.where({
                groupId: additional.id,
                recipientDeviceId: pending.id,
              }),
              { tier: "edge" },
            ),
          ).toEqual([]);
        }
      }
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
      await server.stop();
    }
  },
  120_000,
);
