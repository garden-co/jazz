import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { createBrowserKeyEnvelope, createBrowserDeviceSigner } from "./browser.js";
import { groupSuccessorSigningBytes } from "./group-successor.js";

// JE2C v1's seventh length-prefixed UTF-8 field identifies the encrypted column.
function contextColumn(context: Uint8Array): string {
  const view = new DataView(context.buffer, context.byteOffset, context.byteLength);
  let offset = 5;
  for (let field = 0; field < 6; field++) offset += 4 + view.getUint32(offset, false);
  return new TextDecoder().decode(
    context.subarray(offset + 4, offset + 4 + view.getUint32(offset, false)),
  );
}

it.each([
  "ordinary",
  "repair",
  "removal",
  "removal-forged-successor",
  "removal-invalid-verification",
  "removal-invalid-history",
  "creation-invalid-verification",
])(
  "adds an account to a group and supplies its active devices with the accepted key (%s)",
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
      policy.__e2ee_groups.allowRead.where(
        session.where({ authMode: { in: ["local-first", "external"] } }),
      );
      policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_membership.allowRead.where(
        session.where({ authMode: { in: ["local-first", "external"] } }),
      );
      policy.__e2ee_group_membership.allowInsert.where((row) =>
        allOf([
          { authorAccountId: session.user.account },
          policy.__e2ee_groups.exists.where({ id: row.groupId, accountId: session.user.account }),
        ]),
      );
      policy.__e2ee_group_deliveries.allowRead.where(
        session.where({ authMode: { in: ["local-first", "external"] } }),
      );
      policy.__e2ee_group_deliveries.allowInsert.where((row) =>
        allOf([
          { senderAccountId: session.user.account },
          policy.__e2ee_groups.exists.where({ id: row.groupId, accountId: session.user.account }),
        ]),
      );
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: {
          ...deviceRequestPermissions,
          __e2ee_groups: policies.__e2ee_groups!,
          __e2ee_group_membership: policies.__e2ee_group_membership!,
          __e2ee_group_repairs: policies.__e2ee_group_repairs!,
          __e2ee_group_successors: policies.__e2ee_group_successors!,
          __e2ee_group_deliveries: policies.__e2ee_group_deliveries!,
        },
      });
      const alice = await localAccountConfig(server.appId, server.url);
      const bob = await localAccountConfig(server.appId, server.url);
      const keys = await createBrowserKeyEnvelope();
      const stores = new Map<string, () => string | null>();
      let corruptNextEnvelope = false;
      let corruptWrapColumn: string | undefined;
      const open = async (account: typeof alice) => {
        let saved: string | null = null;
        stores.set(account.account.id, () => saved);
        const db = await createDb({
          ...account,
          e2ee: {
            app,
            crypto: {
              keyEnvelope: {
                ...keys,
                async wrap(secret, context, plaintext) {
                  if (account === alice && corruptWrapColumn === contextColumn(context)) {
                    corruptWrapColumn = undefined;
                    if (scenario === "removal-invalid-history") {
                      // A well-formed envelope can still contain the wrong predecessor key.
                      return keys.wrap(secret, context, new Uint8Array(plaintext.length).fill(7));
                    }
                    return new Uint8Array([1]);
                  }
                  return keys.wrap(secret, context, plaintext);
                },
                async seal(publicKey, context, secret) {
                  if (account === alice && corruptNextEnvelope) {
                    corruptNextEnvelope = false;
                    return new Uint8Array([1]);
                  }
                  return keys.seal(publicKey, context, secret);
                },
              },
            },
            store: {
              async read() {
                return saved;
              },
              async update(transform) {
                saved = transform(saved);
              },
            },
          },
        });
        clients.push(db);
        await db.e2ee.devices.list();
        return db;
      };
      const owner = await open(alice);
      const recipient = await open(bob);
      if (scenario === "creation-invalid-verification") {
        corruptWrapColumn = "verification";
        await expect(owner.e2ee.groups.create().wait()).rejects.toThrow();
        expect(corruptWrapColumn).toBeUndefined();
        expect(await owner.all(app.__e2ee_groups, { tier: "edge" })).toEqual([]);
      }
      const { id } = await owner.e2ee.groups.create().wait();
      if (scenario === "creation-invalid-verification") {
        expect(await owner.e2ee.explain({ groupId: id })).toEqual({ state: "ready" });
        return;
      }
      expect(await recipient.e2ee.explain({ groupId: id })).toMatchObject({ state: "refused" });
      expect(
        await recipient.all(
          app.__e2ee_group_deliveries.where({ recipientAccountId: bob.account.id }),
          { tier: "edge" },
        ),
      ).toEqual([]);
      corruptNextEnvelope = scenario === "repair";
      await owner.e2ee.groups.add(id, bob.account.id).wait();
      if (scenario === "repair") {
        expect(corruptNextEnvelope).toBe(false);
        const delivered = await recipient.all(
          app.__e2ee_group_deliveries.where({ groupId: id, recipientAccountId: bob.account.id }),
          { tier: "edge" },
        );
        expect(delivered).toHaveLength(1);
        const accountRoot = await recipient.one(
          app.__e2ee_account_roots.where({ accountId: bob.account.id }),
          { tier: "edge" },
        );
        // Ordinary account policy is not proof that the requesting device signed.
        await recipient
          .insert(app.__e2ee_group_repairs, {
            groupId: id,
            epochId: delivered[0]!.epochId,
            deliveryId: delivered[0]!.id,
            accountId: bob.account.id,
            deviceId: delivered[0]!.recipientDeviceId,
            accountEpochId: accountRoot!.epochId,
            signature: new Uint8Array(64),
          })
          .wait({ tier: "global" });
        expect(await owner.e2ee.explain({ groupId: id })).toEqual({ state: "ready" });
        expect(
          await recipient.all(
            app.__e2ee_group_deliveries.where({ groupId: id, recipientAccountId: bob.account.id }),
            { tier: "edge" },
          ),
        ).toHaveLength(1);
        await expect(recipient.e2ee.explain({ groupId: id })).rejects.toThrow();
        const requests = app.__e2ee_group_repairs.where({ groupId: id });
        expect(await recipient.all(requests, { tier: "edge" })).toHaveLength(2);
        await expect(recipient.e2ee.explain({ groupId: id })).rejects.toThrow();
        expect(await recipient.all(requests, { tier: "edge" })).toHaveLength(2);
        // A capable member loading the group repairs the recipient's unusable
        // delivery without changing membership or requiring a new public method.
        expect(await owner.e2ee.explain({ groupId: id })).toEqual({ state: "ready" });
      }
      expect(await recipient.e2ee.explain({ groupId: id })).toEqual({ state: "ready" });
      expect(await owner.e2ee.explain({ groupId: id })).toEqual({ state: "ready" });
      if (scenario.startsWith("removal")) {
        const before = await owner.all(app.__e2ee_group_deliveries.where({ groupId: id }), {
          tier: "edge",
        });
        const originalEpoch = before[0]!.epochId;
        corruptWrapColumn =
          scenario === "removal-invalid-verification"
            ? "verification"
            : scenario === "removal-invalid-history"
              ? "history"
              : undefined;
        const removal = owner.e2ee.groups.remove(id, bob.account.id);
        expect(removal).not.toHaveProperty("then");
        if (scenario.startsWith("removal-invalid-")) {
          await expect(removal.wait()).rejects.toThrow();
          expect(corruptWrapColumn).toBeUndefined();
          expect(
            await owner.all(app.__e2ee_group_successors.where({ groupId: id }), { tier: "edge" }),
          ).toEqual([]);
          // The removal remains accepted, but a faulty crypto adapter must not
          // publish an unusable epoch. Loading retries with fresh valid crypto.
        } else {
          await removal.wait();
        }
        expect(await recipient.e2ee.explain({ groupId: id })).toMatchObject({ state: "refused" });
        expect(await owner.e2ee.explain({ groupId: id })).toEqual({ state: "ready" });
        const after = await owner.all(app.__e2ee_group_deliveries.where({ groupId: id }), {
          tier: "edge",
        });
        expect(after.filter((row) => row.recipientAccountId === bob.account.id)).toEqual(
          before.filter((row) => row.recipientAccountId === bob.account.id),
        );
        expect(
          after.some(
            (row) => row.recipientAccountId === alice.account.id && row.epochId !== originalEpoch,
          ),
        ).toBe(true);
        if (scenario === "removal-forged-successor") {
          const successors = app.__e2ee_group_successors.where({ groupId: id });
          const accepted = (await owner.all(successors, { tier: "edge" }))[0]!;
          const bobRoot = await recipient.one(
            app.__e2ee_account_roots.where({ accountId: bob.account.id }),
            { tier: "edge" },
          );
          const bobDevice = JSON.parse(stores.get(bob.account.id)!()!).devices[0];
          const scope = JSON.parse(stores.get(alice.account.id)!()!).devices[0].scope;
          const signer = await createBrowserDeviceSigner();
          const privateKey = Uint8Array.from(bobDevice.signingPrivateKey);
          const record = {
            id: crypto.randomUUID(),
            groupId: id,
            predecessor: accepted.epochId,
            epochId: crypto.randomUUID(),
            authorAccountId: bob.account.id,
            authorDeviceId: bobRoot!.deviceId,
            authorEpochId: bobRoot!.epochId,
            revision: accepted.revision,
            membership: accepted.membership,
            verification: accepted.verification,
            history: accepted.history,
            authorEnvelope: accepted.authorEnvelope,
          };
          const bytes = groupSuccessorSigningBytes(scope, record);
          try {
            const signature = await signer.sign(privateKey, bytes);
            expect(
              await signer.verify(Uint8Array.from(bobDevice.signingPublicKey), bytes, signature),
            ).toBe(true);
            const { id: rowId, ...values } = record;
            // Jazz accepts this account-owned proposal, and Bob's device is
            // still active. Neither fact makes him a remaining group member.
            await recipient
              .insert(app.__e2ee_group_successors, { ...values, signature }, { id: rowId })
              .wait({ tier: "global" });
          } finally {
            privateKey.fill(0);
          }
          expect(
            (await recipient.e2ee.devices.list()).find((device) => device.id === bobRoot!.deviceId),
          ).toMatchObject({ state: "active" });
          expect(await owner.all(successors, { tier: "edge" })).toHaveLength(2);
          expect(await owner.e2ee.explain({ groupId: id })).toEqual({ state: "ready" });
          expect(await recipient.e2ee.explain({ groupId: id })).toMatchObject({ state: "refused" });
          expect(
            await owner.all(app.__e2ee_group_deliveries.where({ groupId: id }), { tier: "edge" }),
          ).toEqual(after);
        }
      }
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
      await server.stop();
    }
  },
  60_000,
);
