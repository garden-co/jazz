import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createBrowserKeyEnvelope, createBrowserDeviceSigner } from "./browser.js";
import { spaceRecoveryBytes } from "./space-recovery-format.js";
import fixture from "./fixtures/local-space-recovery-v1.json";

for (const scenario of [
  "ordinary",
  "read-only-recovered-device",
  "read-only-creator-store",
  "invalid-recovery-candidates",
] as const) {
  it(
    `recovers an existing space after losing the only enrolled device (${scenario})`,
    async () => {
      const app = s.defineApp({
        ...deviceRequestSchema,
        ...spaceSchema,
        projects: s.table({ title: s.string() }, {}),
      });
      const policies = definePermissions(app, ({ policy, session, allOf }) => {
        const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
        policy.projects.allowRead.where(authenticated);
        policy.projects.allowInsert.where(authenticated);
        policy.__e2ee_spaces.allowRead.where(authenticated);
        policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
        policy.__e2ee_space_grants.allowRead.where(authenticated);
        policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
        if (scenario === "invalid-recovery-candidates")
          policy.__e2ee_space_grants.allowDelete.where({ authorAccountId: session.user.account });
        policy.__e2ee_space_deliveries.allowRead.where(authenticated);
        if (scenario === "read-only-recovered-device") {
          policy.__e2ee_space_deliveries.allowInsert.where((row) =>
            allOf([
              { senderAccountId: session.user.account },
              policy.__e2ee_spaces.exists.where({ id: row.spaceId, deviceId: row.senderDeviceId }),
            ]),
          );
        } else
          policy.__e2ee_space_deliveries.allowInsert.where({
            senderAccountId: session.user.account,
          });
        policy.__e2ee_space_successors.allowRead.where(authenticated);
        policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
        policy.__e2ee_space_recovery_deliveries.allowRead.where(authenticated);
        policy.__e2ee_space_recovery_deliveries.allowInsert.where({
          senderAccountId: session.user.account,
        });
      });
      const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
      const clients: Awaited<ReturnType<typeof createDb>>[] = [];
      const store = () => {
        let saved: string | null = JSON.stringify(fixture);
        let readOnly = false;
        return {
          async read() {
            return saved;
          },
          async update(transform: (current: string | null) => string) {
            const next = transform(saved);
            if (readOnly && next !== saved) throw new Error("Creator key store is read-only");
            saved = next;
          },
          freeze() {
            readOnly = true;
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
        const creatorStore = store();
        const keys = await createBrowserKeyEnvelope();
        let exhaustion:
          | { genuine: Uint8Array; genuineHits: number; malformedHits: number }
          | undefined;
        const first = await createDb({
          ...account,
          e2ee: {
            app,
            store: creatorStore,
            crypto: {
              keyEnvelope: {
                ...keys,
                async open(pair, context, envelope) {
                  if (
                    exhaustion &&
                    new TextDecoder().decode(context).includes('["recovery-delivery",')
                  ) {
                    if (
                      envelope.length === exhaustion.genuine.length &&
                      envelope.every((byte, index) => byte === exhaustion!.genuine[index])
                    ) {
                      exhaustion.genuineHits++;
                      throw new Error("Synthetic recovery envelope exhaustion");
                    }
                    exhaustion.malformedHits++;
                  }
                  return keys.open(pair, context, envelope);
                },
              },
            },
          },
        });
        clients.push(first);
        await first.e2ee.devices.list();
        const project = await first
          .insert(app.projects, { title: "Recoverable space" })
          .wait({ tier: "global" });
        await first.e2ee.spaces.grant(app.projects, project.id, account.account.id).wait();
        const target = { scope: app.projects, identifier: project.id };
        expect(await first.e2ee.explain(target)).toEqual({ state: "ready" });
        if (scenario === "read-only-creator-store") creatorStore.freeze();
        const previousRecoveryRoots =
          scenario === "invalid-recovery-candidates"
            ? new Set(
                (await first.all(app.__e2ee_recovery_roots, { tier: "remote" })).map(
                  (row) => row.id,
                ),
              )
            : undefined;
        const { material } = await first.e2ee.recovery.create().wait();
        // Both corruption classes coexist with the genuine space and delivery.
        // Share device loss/recovery/reopen, keeping each accepted fixture below.
        if (scenario === "invalid-recovery-candidates") {
          const writer = await createDb({ ...account });
          clients.push(writer);
          const [root] = await writer.all(app.__e2ee_spaces, { tier: "remote" });
          const recoveries = await writer.all(app.__e2ee_recovery_roots, { tier: "remote" });
          const created = recoveries.filter((row) => !previousRecoveryRoots!.has(row.id));
          expect(root).toBeDefined();
          expect(created).toHaveLength(1);
          const recovery = created[0]!;
          const genuine = await writer.all(
            app.__e2ee_space_recovery_deliveries.where({
              spaceId: root!.id,
              recoveryRootId: recovery.id,
            }),
            { tier: "remote" },
          );
          expect(genuine).toHaveLength(1);
          const { id: _deliveryId, signature: _signature, ...coordinates } = genuine[0]!;
          const sender = JSON.parse((await creatorStore.read())!).devices.find(
            (device: { id: string }) => device.id === coordinates.senderDeviceId,
          );
          expect(sender).toBeDefined();
          const signer = await createBrowserDeviceSigner();
          const signingKey = Uint8Array.from(sender.signingPrivateKey);
          try {
            // One authentic transcript carries malformed ciphertext; the other
            // candidate has an invalid signature. Both coexist with the genuine row.
            for (const signed of [true, false]) {
              const { id, ...values } = {
                ...coordinates,
                id: crypto.randomUUID(),
                envelope: Uint8Array.of(1),
              };
              const bytes = spaceRecoveryBytes(
                sender.scope,
                { ...root!, epochId: values.epochId },
                { id, ...values },
              );
              const signature = signed ? await signer.sign(signingKey, bytes) : new Uint8Array(64);
              expect(
                await signer.verify(Uint8Array.from(sender.signingPublicKey), bytes, signature),
              ).toBe(signed);
              await writer
                .insert(app.__e2ee_space_recovery_deliveries, { ...values, signature }, { id })
                .wait({ tier: "global" });
            }
          } finally {
            signingKey.fill(0);
          }
          expect(await first.e2ee.explain(target)).toEqual({ state: "ready" });
          expect(
            await first.all(app.__e2ee_space_recovery_deliveries, { tier: "remote" }),
          ).toHaveLength(3);
          // Exhaust the genuine envelope once so every malformed candidate must
          // be visited, regardless of database row order. None may abort status.
          exhaustion = { genuine: genuine[0]!.envelope, genuineHits: 0, malformedHits: 0 };
          try {
            const status = await first.e2ee.recovery.status(material);
            expect(status.account).toMatchObject({ validatedRootId: recovery.id });
            expect(status.spaces).toMatchObject({
              validation: "checked",
              paths: [
                {
                  spaceId: root!.id,
                  validation: "unavailable",
                  reason: "unusable-recovery-delivery",
                },
              ],
            });
            expect(exhaustion.genuineHits).toBeGreaterThan(0);
            // The authenticated bad ciphertext reaches open; the unsigned row does not.
            expect(exhaustion.malformedHits).toBe(1);
          } finally {
            exhaustion = undefined;
          }
        }
        if (scenario === "invalid-recovery-candidates") {
          const [root] = await first.all(app.__e2ee_spaces, { tier: "remote" });
          expect(root).toBeDefined();
          const other = await first
            .insert(app.projects, { title: "Unrelated scope" })
            .wait({ tier: "global" });
          const { id: _id, ...values } = root!;
          // Ordinary row policy does not authenticate the E2EE transcript. Copying
          // a root to another space invalidates its signature, even for its author.
          await first
            .insert(app.__e2ee_spaces, {
              ...values,
              identifier: other.id,
              initialGrantId: crypto.randomUUID(),
            })
            .wait({ tier: "global" });
          const status = await first.e2ee.recovery.status(material);
          expect(status.spaces).toEqual({
            validation: "checked",
            paths: [
              {
                scopeId: root!.scopeId,
                identifier: project.id,
                spaceId: root!.id,
                epochId: root!.epochId,
                validation: "validated",
              },
            ],
          });
          // Creation and later use must discover the same authenticated spaces.
          await first.e2ee.recovery.create().wait();
        }
        await first.shutdown();

        // Only exported recovery material survives: no old device store or live key holder.
        const replacementStore = store();
        const replacement = await createDb({ ...account, e2ee: { app, store: replacementStore } });
        clients.push(replacement);
        const pending = (await replacement.e2ee.devices.list()).find(
          (device) => device.state === "pending",
        );
        expect(pending).toBeDefined();
        expect(await replacement.e2ee.explain(target)).toMatchObject({ state: "refused" });
        await replacement.e2ee.recovery.use(material).wait();
        expect(await replacement.e2ee.devices.list()).toContainEqual(
          expect.objectContaining({ id: pending!.id, state: "active" }),
        );
        expect(await replacement.e2ee.explain(target)).toEqual({ state: "ready" });
        // Existing private-store entries survive; recovered candidates use the pinned format.
        const retained = JSON.parse((await replacementStore.read())!);
        expect(retained.recoveredSpaceKeysV1).toHaveLength(2);
        expect(retained.recoveredSpaceKeysV1).toContainEqual(fixture.recoveredSpaceKeysV1[0]);
        if (scenario === "read-only-recovered-device") {
          expect(
            await replacement.all(
              app.__e2ee_space_deliveries.where({ recipientDeviceId: pending!.id }),
              { tier: "remote" },
            ),
          ).toEqual([]);
        }
        await replacement.shutdown();
        const reopened = await createDb({ ...account, e2ee: { app, store: replacementStore } });
        clients.push(reopened);
        expect(await reopened.e2ee.explain(target)).toEqual({ state: "ready" });
        if (scenario === "invalid-recovery-candidates") {
          const [root] = await reopened.all(app.__e2ee_spaces.where({ identifier: project.id }), {
            tier: "remote",
          });
          expect(root).toBeDefined();
          await reopened
            .delete(app.__e2ee_space_grants, root!.initialGrantId)
            .wait({ tier: "global" });
          // This root's signature is genuine. Incomplete membership is not evidence
          // that the caller has no recovery obligations, so discovery must fail closed.
          await expect
            .soft(reopened.e2ee.recovery.status(material))
            .rejects.toThrow("Invalid or unsupported E2EE space membership");
          await expect
            .soft(reopened.e2ee.recovery.use(material).wait())
            .rejects.toThrow("Invalid or unsupported E2EE space membership");
          await expect
            .soft(reopened.e2ee.recovery.create().wait())
            .rejects.toThrow("Invalid or unsupported E2EE space membership");
        }
      } finally {
        await Promise.all(clients.map((client) => client.shutdown()));
        await server.stop();
      }
    },
    scenario === "invalid-recovery-candidates" ? 180_000 : 60_000,
  );
}
