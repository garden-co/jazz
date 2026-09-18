import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { createBrowserKeyEnvelope, createBrowserDeviceSigner } from "./browser.js";
import { publicDeviceApprovalBytes } from "./public-device-approval.js";
import { groupDeliveryBytes } from "./group-format.js";

it.each([
  "ordinary",
  "resume",
  "resume-signed-malformed",
  "resume-wrong-key",
  "resume-wrong-epoch",
  "resume-revoked",
  "resume-bad-envelope",
  "staged-fixture",
  "forged",
  "signed-malformed",
  "root-signature",
  "delivery-signature",
  "root-history-race",
  "delivery-history-race",
] as const)(
  "authenticates accepted group creation: %s",
  async (scenario) => {
    const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
    const policies = definePermissions(app, ({ policy, session, allOf }) => {
      policy.__e2ee_group_repairs.allowRead.where(
        session.where({ authMode: { in: ["local-first", "external"] } }),
      );
      policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_successors.allowRead.where(
        session.where({ authMode: { in: ["local-first", "external"] } }),
      );
      policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
      const groups = policy.__e2ee_groups;
      groups.allowRead.where(session.where({ authMode: { in: ["local-first", "external"] } }));
      groups.allowInsert.where({ accountId: session.user.account });
      const deliveries = policy.__e2ee_group_deliveries;
      deliveries.allowRead.where(session.where({ authMode: { in: ["local-first", "external"] } }));
      deliveries.allowInsert.where((row) =>
        allOf([
          { senderAccountId: session.user.account },
          groups.exists.where({ id: row.groupId, accountId: session.user.account }),
        ]),
      );
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    const store = () => {
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
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: {
          ...deviceRequestPermissions,
          __e2ee_groups: policies.__e2ee_groups!,
          __e2ee_group_repairs: policies.__e2ee_group_repairs!,
          __e2ee_group_successors: policies.__e2ee_group_successors!,
          __e2ee_group_deliveries: policies.__e2ee_group_deliveries!,
        },
      });
      const account = await localAccountConfig(server.appId, server.url);
      const retained = store();
      const stagedFixture = JSON.parse(
        readFileSync(new URL("./fixtures/local-group-staging-v1.json", import.meta.url), "utf8"),
      );
      if (scenario === "staged-fixture") await retained.update(() => JSON.stringify(stagedFixture));
      const keys = await createBrowserKeyEnvelope();
      const signer = await createBrowserDeviceSigner();
      let signingAttempt = 0;
      let faultAt = 0;
      let beforeSeal: (() => Promise<void>) | undefined;
      let beforeOpen: (() => Promise<void>) | undefined;
      const first = await createDb({
        ...account,
        e2ee: {
          app,
          store: retained,
          crypto: {
            deviceSigner: {
              ...signer,
              async sign(privateKey, bytes) {
                if (faultAt && ++signingAttempt === faultAt) return new Uint8Array(64);
                return signer.sign(privateKey, bytes);
              },
            },
            keyEnvelope: {
              ...keys,
              async open(device, context, envelope) {
                await beforeOpen?.();
                return keys.open(device, context, envelope);
              },
              async seal(publicKey, context, key) {
                const action = beforeSeal;
                beforeSeal = undefined;
                await action?.();
                return keys.seal(publicKey, context, key);
              },
            },
          },
        },
      });
      clients.push(first);
      const creator = (await first.e2ee.devices.list()).find(
        (device) => device.state === "active",
      )!;
      const second = await createDb({ ...account, e2ee: { app, store: store() } });
      clients.push(second);
      const request = (await second.e2ee.devices.list()).find(
        (device) => device.state === "pending",
      )!;
      expect(request).toBeDefined();
      await first.e2ee.devices.approve(request.id).wait();
      const pending = await createDb({ ...account, e2ee: { app, store: store() } });
      clients.push(pending);
      const pendingDevice = (await pending.e2ee.devices.list()).find(
        (device) => device.state === "pending",
      )!;
      let injected = false;
      if (scenario.endsWith("history-race")) {
        const root = await pending.one(
          app.__e2ee_account_roots.where({ accountId: account.account.id }),
          { tier: "edge" },
        );
        const retainedDevice = JSON.parse((await retained.read())!).devices[0];
        const addition = {
          id: crypto.randomUUID(),
          accountId: account.account.id,
          epochId: root!.epochId,
          deviceId: pendingDevice.id,
          signerId: creator.id,
        };
        const privateKey = Uint8Array.from(retainedDevice.signingPrivateKey);
        let signature: Uint8Array;
        try {
          signature = await signer.sign(
            privateKey,
            publicDeviceApprovalBytes(retainedDevice.scope, addition),
          );
        } finally {
          privateKey.fill(0);
        }
        beforeOpen = async () => {
          const groupRoot = await pending.one(app.__e2ee_groups.where({ id: group.id }), {
            tier: "edge",
          });
          if ((groupRoot !== null) !== (scenario === "delivery-history-race")) return;
          beforeOpen = undefined;
          const { id, ...values } = addition;
          await pending
            .insert(app.__e2ee_public_device_approvals, { ...values, signature }, { id })
            .wait({ tier: "global" });
          injected = true;
        };
      }
      if (
        scenario === "forged" ||
        scenario === "signed-malformed" ||
        scenario === "resume-signed-malformed"
      )
        beforeSeal = async () => {
          const root = await pending.one(app.__e2ee_groups.where({ id: group.id }), {
            tier: "edge",
          });
          expect(root).toBeDefined();
          const delivery = {
            id: "00000000-0000-4000-8000-000000000001",
            groupId: group.id,
            epochId: root!.epochId,
            senderAccountId: root!.accountId,
            senderDeviceId: creator.id,
            recipientAccountId: root!.accountId,
            recipientDeviceId: creator.id,
            envelope: new Uint8Array([1]),
          };
          let signature = new Uint8Array(64);
          if (scenario !== "forged") {
            const retainedDevice = JSON.parse((await retained.read())!).devices[0];
            const privateKey = Uint8Array.from(retainedDevice.signingPrivateKey);
            try {
              signature = new Uint8Array(
                await signer.sign(
                  privateKey,
                  groupDeliveryBytes(retainedDevice.scope, root!, delivery),
                ),
              );
            } finally {
              privateKey.fill(0);
            }
          }
          // An account-owned insert is not proof that its claimed device signed it.
          const { id, ...values } = delivery;
          await pending
            .insert(app.__e2ee_group_deliveries, { ...values, signature }, { id })
            .wait({ tier: "global" });
        };
      faultAt = scenario === "root-signature" ? 1 : scenario === "delivery-signature" ? 2 : 0;
      if (scenario.startsWith("resume")) {
        const inject = beforeSeal;
        beforeSeal = async () => {
          await inject?.();
          throw new Error("delivery interrupted");
        };
      }
      const group = first.e2ee.groups.create();
      expect(group).not.toHaveProperty("then");
      expect(group.id).toEqual(expect.any(String));
      if (scenario.startsWith("resume")) {
        await expect(group.wait()).rejects.toThrow("delivery interrupted");
        const original = await pending.one(app.__e2ee_groups.where({ id: group.id }), {
          tier: "edge",
        });
        expect(original).not.toBeNull();
        const interruptedDeliveries = await pending.all(
          app.__e2ee_group_deliveries.where({ groupId: group.id }),
          { tier: "edge" },
        );
        if (scenario === "resume-signed-malformed") expect(interruptedDeliveries).toHaveLength(1);
        else expect(interruptedDeliveries).toEqual([]);
        await first.shutdown();
        const saved = (await retained.read())!;
        if (scenario === "resume-wrong-key" || scenario === "resume-wrong-epoch")
          await retained.update((current) => {
            const state = JSON.parse(current!);
            const staged = state.stagedGroupKeysV1.find(
              (entry: { groupId: string }) => entry.groupId === group.id,
            );
            if (scenario === "resume-wrong-key") staged.payload[0] ^= 1;
            else staged.epochId = "99999999-9999-4999-8999-999999999999";
            return JSON.stringify(state);
          });
        if (scenario === "resume-revoked") await second.e2ee.devices.revoke(creator.id).wait();
        let badEnvelope = false;
        const reopened = await createDb({
          ...account,
          e2ee: {
            app,
            store: retained,
            crypto: {
              keyEnvelope: {
                ...keys,
                async seal(publicKey, context, key) {
                  return badEnvelope ? new Uint8Array([1]) : keys.seal(publicKey, context, key);
                },
              },
            },
          },
        });
        clients.push(reopened);
        await reopened.e2ee.devices.list();
        badEnvelope = scenario === "resume-bad-envelope";
        if (scenario !== "resume" && scenario !== "resume-signed-malformed") {
          if (scenario === "resume-revoked")
            expect(await reopened.e2ee.explain({ groupId: group.id })).toMatchObject({
              state: "refused",
            });
          else
            await expect(reopened.e2ee.explain({ groupId: group.id })).rejects.toThrow(
              /authentication|epoch|envelope/i,
            );
          expect(
            await pending.all(app.__e2ee_group_deliveries.where({ groupId: group.id }), {
              tier: "edge",
            }),
          ).toEqual([]);
          expect(JSON.parse((await retained.read())!).stagedGroupKeysV1).toHaveLength(1);
          if (scenario === "resume-revoked") return;
          // Restoring the original host record permits retry; no device reset is needed.
          await retained.update(() => saved);
          badEnvelope = false;
        }
        expect(await reopened.e2ee.explain({ groupId: group.id })).toMatchObject({
          state: "ready",
        });
        expect(await second.e2ee.explain({ groupId: group.id })).toMatchObject({ state: "ready" });
        expect(
          await pending.one(app.__e2ee_groups.where({ id: group.id }), { tier: "edge" }),
        ).toEqual(original);
        expect(JSON.parse((await retained.read())!).stagedGroupKeysV1).toEqual([]);
        // With the staged secret removed, readiness must come from a usable
        // envelope, not merely a correctly signed but malformed candidate.
        expect(await reopened.e2ee.explain({ groupId: group.id })).toMatchObject({
          state: "ready",
        });
        return;
      }
      if (scenario.endsWith("history-race")) {
        const outcome = await group.wait().then(
          () => ({ error: null }),
          (error: unknown) => ({ error }),
        );
        expect(injected).toBe(true);
        expect(outcome.error).toBeInstanceOf(Error);
        expect((outcome.error as Error).message).toMatch(/incomplete|conflict/i);
        expect(
          await pending.all(app.__e2ee_group_deliveries.where({ groupId: group.id }), {
            tier: "edge",
          }),
        ).toEqual([]);
        if (scenario === "root-history-race")
          expect(
            await pending.one(app.__e2ee_groups.where({ id: group.id }), { tier: "edge" }),
          ).toBeNull();
        return;
      }
      if (faultAt) {
        await expect(group.wait()).rejects.toThrow(/signature/i);
        expect(
          await pending.all(app.__e2ee_group_deliveries.where({ groupId: group.id }), {
            tier: "edge",
          }),
        ).toEqual([]);
        if (scenario === "root-signature")
          expect(
            await pending.one(app.__e2ee_groups.where({ id: group.id }), { tier: "edge" }),
          ).toBeNull();
        faultAt = 0;
        const retry = first.e2ee.groups.create();
        await retry.wait();
        expect(await first.e2ee.explain({ groupId: retry.id })).toMatchObject({ state: "ready" });
        return;
      }
      expect(await group.wait()).toEqual({ id: group.id });
      if (scenario === "staged-fixture")
        expect(JSON.parse((await retained.read())!).stagedGroupKeysV1).toEqual(
          stagedFixture.stagedGroupKeysV1,
        );
      expect(await first.e2ee.explain({ groupId: group.id })).toMatchObject({ state: "ready" });
      expect(await second.e2ee.explain({ groupId: group.id })).toMatchObject({ state: "ready" });
      await first.shutdown();

      const reopened = await createDb({ ...account, e2ee: { app, store: retained } });
      clients.push(reopened);
      expect(await reopened.e2ee.explain({ groupId: group.id })).toMatchObject({ state: "ready" });

      await expect(pending.e2ee.groups.create().wait()).rejects.toThrow(/active|approved/i);

      const outsider = await createDb({
        ...(await localAccountConfig(server.appId, server.url)),
        e2ee: { app, store: store() },
      });
      clients.push(outsider);
      expect(await outsider.e2ee.explain({ groupId: group.id })).toMatchObject({
        state: "refused",
      });
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
      await server.stop();
    }
  },
  60000,
);
