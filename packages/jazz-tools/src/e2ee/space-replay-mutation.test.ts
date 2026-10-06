import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { spaceSchema } from "./spaces.js";
import { createNativeCrypto } from "./native.js";
import type { CryptoAdapters } from "./types.js";

it.each(["unchanged", "replaced"] as const)(
  "enforces accepted group removal after verifier input mutation with %s verifier identity",
  async (identity) => {
    const app = s.defineApp({
      ...deviceRequestSchema,
      ...groupSchema,
      ...spaceSchema,
      projects: s.table({ title: s.string() }, {}),
    });
    const policies = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.always();
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_space_successors.allowRead.always();
      policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
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
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Db[] = [];
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
        permissions: { ...deviceRequestPermissions, ...policies },
      });
      const departed = await localAccountConfig(server.appId, server.url);
      const remaining = await localAccountConfig(server.appId, server.url);
      const saved = store();
      const native = await createNativeCrypto();
      const owner = await createDb({ ...departed, e2ee: { app, store: saved, crypto: native } });
      const member = await createDb({
        ...remaining,
        e2ee: { app, store: store(), crypto: native },
      });
      clients.push(owner, member);
      await owner.e2ee.devices.list();
      await member.e2ee.devices.list();
      const group = await owner.e2ee.groups.create().wait();
      const project = await owner.insert(app.projects, { title: "Replay isolation" }).wait({
        tier: "global",
      });
      // Only the group grants access, even though its creator authored the root.
      await owner.e2ee.spaces.grant(app.projects, project.id, group.id).wait();
      const target = { scope: app.projects, identifier: project.id };
      expect(await owner.e2ee.explain(target)).toEqual({ state: "ready" });
      // This add is after the space grant: historical grant validation cannot
      // consume the one-shot mutation intended for current group replay.
      await owner.e2ee.groups.add(group.id, { kind: "account", id: remaining.account.id }).wait();
      const addition = await owner.one(
        app.__e2ee_group_membership.where({ groupId: group.id, operation: "add" }),
        { tier: "remote" },
      );
      const root = await owner.one(app.__e2ee_spaces.where({ identifier: project.id }), {
        tier: "remote",
      });
      expect(addition).not.toBeNull();
      expect(root).not.toBeNull();
      await member.shutdown();
      await owner.e2ee.groups.leave(group.id).wait();
      expect(await owner.e2ee.explain(target)).toMatchObject({
        state: "refused",
        reason: "not-a-space-recipient",
      });
      expect(
        await owner.all(app.__e2ee_group_successors.where({ groupId: group.id }), {
          tier: "remote",
        }),
      ).toEqual([]);
      await owner.shutdown();

      const interrupted = new Error("Stop after current group replay");
      let mutate = true;
      let interruptOpen = true;
      let mutations = 0;
      let interruptions = 0;
      let openedSecret: Uint8Array | undefined;
      const crypto: CryptoAdapters = {
        ...native,
        deviceSigner: {
          ...native.deviceSigner,
          async verify(publicKey, record, signature) {
            const valid = await native.deviceSigner.verify(publicKey, record, signature);
            if (
              mutate &&
              valid &&
              signature.length === addition!.signature.length &&
              signature.every((byte, index) => byte === addition!.signature[index])
            ) {
              mutate = false;
              mutations++;
              publicKey.fill(0);
            }
            return valid;
          },
        },
        keyEnvelope: {
          ...native.keyEnvelope,
          async open(device, context, envelope) {
            const secret = await native.keyEnvelope.open(device, context, envelope);
            if (
              interruptOpen &&
              mutations === 1 &&
              envelope.length === root!.authorEnvelope.length &&
              envelope.every((byte, index) => byte === root!.authorEnvelope[index])
            ) {
              interruptOpen = false;
              interruptions++;
              openedSecret = secret;
              secret.fill(0);
              throw interrupted;
            }
            return secret;
          },
        },
      };
      const reader = await createDb({ ...departed, e2ee: { app, store: saved, crypto } });
      clients.push(reader);
      // Fixture-local operational control: stop at the public author-envelope
      // adapter before later history replay can replace the completed group graph.
      // This tests cache admission, not the first mutation-tainted read's outcome.
      await expect(reader.e2ee.explain(target)).rejects.toBe(interrupted);
      expect(mutations).toBe(1);
      expect(interruptions).toBe(1);
      expect(openedSecret).toBeDefined();
      expect(openedSecret!.every((byte) => byte === 0)).toBe(true);
      mutate = false;
      interruptOpen = false;
      if (identity === "replaced") {
        // Control: changing the public verifier must invalidate existing proofs.
        crypto.deviceSigner.verify = (key, record, signature) =>
          native.deviceSigner.verify(key, record, signature);
      }
      // A fresh covered read has pristine public rows. The same verifier's
      // completed nested Group cache must not retain the skipped removal.
      expect(await reader.e2ee.explain(target)).toMatchObject({
        state: "refused",
        reason: "not-a-space-recipient",
      });
      expect(mutations).toBe(1);
      expect(interruptions).toBe(1);
    } finally {
      try {
        await Promise.all(clients.map((client) => client.shutdown()));
      } finally {
        await server.stop();
      }
    }
  },
  120_000,
);
