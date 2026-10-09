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
  "delivery-candidates",
  "resume-staging",
  "resume-revoked",
  "root-history-race",
  "delivery-history-race",
] as const)(
  "authenticates accepted group creation: %s",
  async (variant) => {
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
      // The ordinary creation control also proves that the frozen host format
      // loads and preserves staging belonging to another scope.
      const checkStagedFixture = variant === "delivery-candidates";
      if (checkStagedFixture) await retained.update(() => JSON.stringify(stagedFixture));
      const keys = await createBrowserKeyEnvelope();
      const signer = await createBrowserDeviceSigner();
      let beforeSeal: (() => Promise<void>) | undefined;
      let beforeOpen: (() => Promise<void>) | undefined;
      const openCreator = () =>
        createDb({
          ...account,
          e2ee: {
            app,
            store: retained,
            crypto: {
              deviceSigner: signer,
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
      let first = await openCreator();
      clients.push(first);
      const creator = (await first.e2ee.devices.list()).find(
        (device) => device.state === "active",
      )!;
      const needsApprovedSecond = !variant.endsWith("history-race");
      let second: Awaited<ReturnType<typeof createDb>> | undefined;
      if (needsApprovedSecond) {
        second = await createDb({ ...account, e2ee: { app, store: store() } });
        clients.push(second);
        const request = (await second!.e2ee.devices.list()).find(
          (device) => device.state === "pending",
        )!;
        expect(request).toBeDefined();
        await first.e2ee.devices.approve(request.id).wait();
      }
      // Resume cases only need the approved device as an independent reader.
      // Malformed-candidate, history-race and inactive-creator checks retain a pending client.
      const pending =
        second && variant.startsWith("resume")
          ? second
          : await createDb({ ...account, e2ee: { app, store: store() } });
      if (pending !== second) clients.push(pending);
      const pendingDevice =
        pending === second
          ? undefined
          : (await pending.e2ee.devices.list()).find((device) => device.state === "pending");
      if (pending !== second) expect(pendingDevice).toBeDefined();
      // Signed malformed resumption uses the approved reader as its raw writer.
      // Clean and poisoned delivery histories each keep a reopened creator.
      // Clean, faulty and signed-malformed resumption keep separate interrupted groups.
      // Only account enrolment, approval and the independent readers are shared.
      const scenarios =
        variant === "delivery-candidates"
          ? (["ordinary", "mixed-deliveries"] as const)
          : variant === "resume-staging"
            ? (["resume", "resume-faults", "resume-signed-malformed"] as const)
            : [variant];
      for (const scenario of scenarios) {
        let injected = false;
        if (scenario.endsWith("history-race")) {
          const root = await pending.one(
            app.__e2ee_account_roots.where({ accountId: account.account.id }),
            { tier: "remote" },
          );
          const retainedDevice = JSON.parse((await retained.read())!).devices[0];
          const addition = {
            id: crypto.randomUUID(),
            accountId: account.account.id,
            epochId: root!.epochId,
            deviceId: pendingDevice!.id,
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
              tier: "remote",
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
        if (scenario === "mixed-deliveries" || scenario === "resume-signed-malformed")
          beforeSeal = async () => {
            const root = await pending.one(app.__e2ee_groups.where({ id: group.id }), {
              tier: "remote",
            });
            expect(root).toBeDefined();
            // Both invalid signatures and authenticated unusable envelopes must
            // leave concurrent creation free to publish its legitimate delivery.
            const candidates =
              scenario === "mixed-deliveries"
                ? ["forged", "signed-malformed"]
                : ["signed-malformed"];
            for (const kind of candidates) {
              const delivery = {
                id:
                  scenario === "mixed-deliveries" && kind === "signed-malformed"
                    ? "00000000-0000-4000-8000-000000000002"
                    : "00000000-0000-4000-8000-000000000001",
                groupId: group.id,
                epochId: root!.epochId,
                senderAccountId: root!.accountId,
                senderDeviceId: creator.id,
                recipientAccountId: root!.accountId,
                recipientDeviceId: creator.id,
                envelope: new Uint8Array([1]),
              };
              let signature = new Uint8Array(64);
              if (kind !== "forged") {
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
            }
            expect(
              await pending.all(app.__e2ee_group_deliveries.where({ groupId: group.id }), {
                tier: "remote",
              }),
            ).toHaveLength(candidates.length);
          };
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
            tier: "remote",
          });
          expect(original).not.toBeNull();
          const interruptedDeliveries = await pending.all(
            app.__e2ee_group_deliveries.where({ groupId: group.id }),
            { tier: "remote" },
          );
          if (scenario === "resume-signed-malformed") expect(interruptedDeliveries).toHaveLength(1);
          else expect(interruptedDeliveries).toEqual([]);
          await first.shutdown();
          const saved = (await retained.read())!;
          const corruptStaging = async (fault: "key" | "epoch") =>
            retained.update(() => {
              const state = JSON.parse(saved);
              const staged = state.stagedGroupKeysV1.find(
                (entry: { groupId: string }) => entry.groupId === group.id,
              );
              if (fault === "key") staged.payload[0] ^= 1;
              else staged.epochId = "99999999-9999-4999-8999-999999999999";
              return JSON.stringify(state);
            });
          if (scenario === "resume-faults") await corruptStaging("key");
          if (scenario === "resume-revoked") await second!.e2ee.devices.revoke(creator.id).wait();
          let badEnvelope = false;
          let badEnvelopeHits = 0;
          const reopened = await createDb({
            ...account,
            e2ee: {
              app,
              store: retained,
              crypto: {
                keyEnvelope: {
                  ...keys,
                  async seal(publicKey, context, key) {
                    const action = beforeSeal;
                    beforeSeal = undefined;
                    await action?.();
                    if (badEnvelope) {
                      badEnvelopeHits++;
                      return new Uint8Array([1]);
                    }
                    return keys.seal(publicKey, context, key);
                  },
                },
              },
            },
          });
          clients.push(reopened);
          const expectUnpublished = async () => {
            expect(
              await pending.all(app.__e2ee_group_deliveries.where({ groupId: group.id }), {
                tier: "remote",
              }),
            ).toEqual([]);
            expect(JSON.parse((await retained.read())!).stagedGroupKeysV1).toHaveLength(1);
            expect(
              await pending.one(app.__e2ee_groups.where({ id: group.id }), { tier: "remote" }),
            ).toEqual(original);
          };
          if (scenario === "resume-revoked") {
            expect(await reopened.e2ee.explain({ groupId: group.id })).toMatchObject({
              state: "refused",
            });
            await expectUnpublished();
            return;
          }
          if (scenario === "resume-faults") {
            // Each attempt reads staging from the host store. Correct one fault
            // before introducing the next, then finish on this same reopened client.
            await expect(reopened.e2ee.explain({ groupId: group.id })).rejects.toThrow(
              "Unable to authenticate E2EE group key",
            );
            await expectUnpublished();
            await corruptStaging("epoch");
            await expect(reopened.e2ee.explain({ groupId: group.id })).rejects.toThrow(
              "Staged E2EE group epoch does not match",
            );
            await expectUnpublished();
            await retained.update(() => saved);
            badEnvelope = true;
            await expect(reopened.e2ee.explain({ groupId: group.id })).rejects.toThrow();
            expect(badEnvelopeHits).toBeGreaterThan(0);
            await expectUnpublished();
            await retained.update(() => saved);
            badEnvelope = false;
          }
          expect(await reopened.e2ee.explain({ groupId: group.id })).toMatchObject({
            state: "ready",
          });
          // Check cross-device delivery on clean resume and malformed-candidate
          // replacement. The fault sequence proves each rejection and the final retry.
          if (scenario === "resume" || scenario === "resume-signed-malformed")
            expect(await second!.e2ee.explain({ groupId: group.id })).toMatchObject({
              state: "ready",
            });
          expect(
            await pending.one(app.__e2ee_groups.where({ id: group.id }), { tier: "remote" }),
          ).toEqual(original);
          expect(JSON.parse((await retained.read())!).stagedGroupKeysV1).toEqual([]);
          // With the staged secret removed, readiness must come from a usable
          // envelope, not merely a correctly signed but malformed candidate.
          expect(await reopened.e2ee.explain({ groupId: group.id })).toMatchObject({
            state: "ready",
          });
          first = reopened;
          continue;
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
              tier: "remote",
            }),
          ).toEqual([]);
          if (scenario === "root-history-race")
            expect(
              await pending.one(app.__e2ee_groups.where({ id: group.id }), { tier: "remote" }),
            ).toBeNull();
          return;
        }
        expect(await group.wait()).toEqual({ id: group.id });
        if (checkStagedFixture)
          expect(JSON.parse((await retained.read())!).stagedGroupKeysV1).toEqual(
            stagedFixture.stagedGroupKeysV1,
          );
        expect(await first.e2ee.explain({ groupId: group.id })).toMatchObject({ state: "ready" });
        // The malformed candidates target the creator, whose readiness and
        // reopening are checked for each. One control covers the second device.
        if (scenario === "ordinary")
          expect(await second!.e2ee.explain({ groupId: group.id })).toMatchObject({
            state: "ready",
          });
        await first.shutdown();

        const reopened = await openCreator();
        clients.push(reopened);
        expect(await reopened.e2ee.explain({ groupId: group.id })).toMatchObject({
          state: "ready",
        });
        if (checkStagedFixture)
          expect(JSON.parse((await retained.read())!).stagedGroupKeysV1).toEqual(
            stagedFixture.stagedGroupKeysV1,
          );

        // These account/device boundaries do not depend on delivery corruption or
        // retained staging. Keep one live check of each in the ordinary case.
        if (scenario === "ordinary") {
          await expect(pending.e2ee.groups.create().wait()).rejects.toThrow(/active|approved/i);

          const outsider = await createDb({
            ...(await localAccountConfig(server.appId, server.url)),
            e2ee: { app, store: store() },
          });
          clients.push(outsider);
          expect(await outsider.e2ee.explain({ groupId: group.id })).toMatchObject({
            state: "refused",
          });
          await outsider.shutdown();
        }
        first = reopened;
      }
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
      await server.stop();
    }
  },
  60000,
);
