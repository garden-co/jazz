import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createBrowserKeyEnvelope } from "./browser.js";

it.each([false, true])(
  "initialises an explicit non-creator recipient without retaining creator membership (interrupted: %s)",
  async (interrupted) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const alice = await localAccountConfig(server.appId, server.url);
    const bob = await localAccountConfig(server.appId, server.url);
    const app = s.defineApp({
      ...deviceRequestSchema,
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
      policy.__e2ee_space_successors.allowRead.always();
      policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    });
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
          projects: policies.projects!,
          __e2ee_spaces: policies.__e2ee_spaces!,
          __e2ee_space_grants: policies.__e2ee_space_grants!,
          __e2ee_space_deliveries: policies.__e2ee_space_deliveries!,
          __e2ee_space_successors: policies.__e2ee_space_successors!,
        },
      });
      expect(alice.account.id).not.toBe(bob.account.id);
      const keys = await createBrowserKeyEnvelope();
      let interruptDelivery = interrupted;
      const creator = await createDb({
        ...alice,
        e2ee: {
          app,
          store: store(),
          crypto: {
            keyEnvelope: {
              ...keys,
              async seal(secret, context, plaintext) {
                if (interruptDelivery && new TextDecoder().decode(context).includes('"delivery"')) {
                  interruptDelivery = false;
                  throw new Error("Interrupted initial delivery");
                }
                return keys.seal(secret, context, plaintext);
              },
            },
          },
        },
      });
      const recipient = await createDb({ ...bob, e2ee: { app, store: store() } });
      clients.push(creator, recipient);
      await creator.e2ee.devices.list();
      await recipient.e2ee.devices.list();
      const project = await creator
        .insert(app.projects, { title: "Shared scope" })
        .wait({ tier: "global" });
      const target = { scope: app.projects, identifier: project.id };
      const grant = creator.e2ee.spaces.grant(app.projects, project.id, bob.account.id);
      expect(grant).not.toBeInstanceOf(Promise);
      if (interrupted) {
        await expect(grant.wait()).rejects.toThrow("Interrupted initial delivery");
        expect(await creator.all(app.__e2ee_spaces, { tier: "global" })).toHaveLength(1);
        expect(await creator.all(app.__e2ee_space_deliveries, { tier: "global" })).toEqual([]);
        expect(await recipient.e2ee.explain(target)).toEqual({
          state: "unavailable",
          reason: "space-key-not-delivered",
        });
      } else {
        await grant.wait();
      }
      expect(await creator.e2ee.explain(target)).toMatchObject({
        state: "refused",
        reason: "not-a-space-recipient",
      });
      expect(await recipient.e2ee.explain(target)).toEqual({ state: "ready" });
      const root = await creator.one(app.__e2ee_spaces.where({ identifier: project.id }), {
        tier: "edge",
      });
      expect(root!.accountId).toBe(alice.account.id);
      const grants = await creator.all(app.__e2ee_space_grants.where({ spaceId: root!.id }), {
        tier: "edge",
      });
      expect(grants).toHaveLength(1);
      expect(grants[0]!.id).toBe(root!.initialGrantId);
      expect(grants[0]!.recipientId).toBe(bob.account.id);
      const initialDeliveries = await creator.all(
        app.__e2ee_space_deliveries.where({
          spaceId: root!.id,
        }),
        { tier: "edge" },
      );
      expect(initialDeliveries).toHaveLength(1);
      expect(initialDeliveries[0]!.recipientAccountId).toBe(bob.account.id);

      // Explicitly granting the creator later is an ordinary membership change.
      // Removing that grant must not reactivate an implicit creator default.
      await recipient.e2ee.spaces.grant(app.projects, project.id, alice.account.id).wait();
      expect(await creator.e2ee.explain(target)).toEqual({ state: "ready" });
      await recipient.e2ee.spaces.revoke(app.projects, project.id, alice.account.id).wait();
      expect(await creator.e2ee.explain(target)).toMatchObject({ state: "refused" });
      expect(await recipient.e2ee.explain(target)).toEqual({ state: "ready" });
      const successors = await recipient.all(
        app.__e2ee_space_successors.where({
          spaceId: root!.id,
        }),
        { tier: "edge" },
      );
      expect(successors).toHaveLength(1);
      const replacementDeliveries = await recipient.all(
        app.__e2ee_space_deliveries.where({
          spaceId: root!.id,
          epochId: successors[0]!.epochId,
        }),
        { tier: "edge" },
      );
      expect(replacementDeliveries).toHaveLength(1);
      expect(replacementDeliveries[0]!.recipientAccountId).toBe(bob.account.id);
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
      await server.stop();
    }
  },
  60_000,
);
