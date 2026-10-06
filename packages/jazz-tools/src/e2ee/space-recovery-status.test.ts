import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createNativeCrypto } from "./native.js";

it.each(["reject", "throw"] as const)(
  "inspects required space recovery paths without enrolment, repair or key retention (%s)",
  async (failureMode) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    try {
      const app = s.defineApp({
        ...deviceRequestSchema,
        ...spaceSchema,
        projects: s.table({ title: s.string() }, {}),
      });
      const policies = definePermissions(app, ({ policy, session }) => {
        const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
        policy.projects.allowRead.where(authenticated);
        policy.projects.allowInsert.where(authenticated);
        policy.__e2ee_spaces.allowRead.where(authenticated);
        policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
        policy.__e2ee_space_grants.allowRead.where(authenticated);
        policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
        policy.__e2ee_space_deliveries.allowRead.where(authenticated);
        policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
        policy.__e2ee_space_successors.allowRead.where(authenticated);
        policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
        policy.__e2ee_space_recovery_deliveries.allowRead.where(authenticated);
        policy.__e2ee_space_recovery_deliveries.allowInsert.where({
          senderAccountId: session.user.account,
        });
      });
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: { ...deviceRequestPermissions, ...policies },
      });
      const crypto = await createNativeCrypto();
      let corrupt = false;
      let injected = 0;
      let failHistory = false;
      let failOpen = false;
      let failCurrent = false;
      const envelopeError = new Error("Space envelope provider unavailable");
      const historyError = new Error("History provider unavailable");
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
              ...crypto,
              keyEnvelope: {
                ...crypto.keyEnvelope,
                // Non-async deliberately: BYOC adapters may throw before returning a Promise.
                unwrap(key, context, envelope) {
                  const text = new TextDecoder().decode(context);
                  if (inspect && failHistory && text.includes("history")) {
                    if (failureMode === "throw") throw historyError;
                    return Promise.reject(historyError);
                  }
                  if (
                    inspect &&
                    failCurrent &&
                    text.includes("jazz.e2ee.space.v1") &&
                    text.includes("verification")
                  ) {
                    if (failureMode === "throw") throw envelopeError;
                    return Promise.reject(envelopeError);
                  }
                  return crypto.keyEnvelope.unwrap(key, context, envelope);
                },
                open(pair, context, envelope) {
                  if (
                    inspect &&
                    failOpen &&
                    new TextDecoder().decode(context).includes("recovery-delivery")
                  ) {
                    if (failureMode === "throw") throw envelopeError;
                    return Promise.reject(envelopeError);
                  }
                  if (
                    inspect &&
                    corrupt &&
                    new TextDecoder().decode(context).includes("recovery-delivery")
                  ) {
                    injected++;
                    return Promise.resolve(new Uint8Array(32).fill(9));
                  }
                  return crypto.keyEnvelope.open(pair, context, envelope);
                },
              },
            },
          },
        });
        clients.push(db);
        return { db, saved: () => saved };
      };
      const aliceAccount = await localAccountConfig(server.appId, server.url);
      const bobAccount = await localAccountConfig(server.appId, server.url);
      const { db: alice } = await open(aliceAccount);
      const { db: bob } = await open(bobAccount);
      const { material } = await bob.e2ee.recovery.create().wait();
      const project = await alice
        .insert(app.projects, { title: "Recovery coverage" })
        .wait({ tier: "global" });
      await alice.e2ee.spaces.grant(app.projects, project.id, aliceAccount.account.id).wait();
      // Ordinary policy allows administration; Bob does not yet hold this space's key.
      await bob.e2ee.spaces.grant(app.projects, project.id, bobAccount.account.id).wait();
      const root = await alice.one(app.__e2ee_spaces.where({ identifier: project.id }), {
        tier: "remote",
      });
      expect(root).not.toBeNull();
      const observer = await open(bobAccount, true);
      const requests = await bob.all(app.__e2ee_device_requests, { tier: "remote" });
      const deliveries = await bob.all(app.__e2ee_space_recovery_deliveries, { tier: "remote" });
      const missing = await observer.db.e2ee.recovery.status(material);
      expect(missing.account.validation).toBe("validated");
      expect(missing).toMatchObject({
        spaces: {
          validation: "checked",
          paths: [
            {
              scopeId: root!.scopeId,
              identifier: project.id,
              spaceId: root!.id,
              epochId: root!.epochId,
              validation: "unavailable",
              reason: "missing-recovery-delivery",
            },
          ],
        },
      });
      expect(observer.saved()).toBeNull();
      expect(await bob.all(app.__e2ee_space_recovery_deliveries, { tier: "remote" })).toEqual(
        deliveries,
      );
      expect(await bob.all(app.__e2ee_device_requests, { tier: "remote" })).toEqual(requests);
      const target = { scope: app.projects, identifier: project.id };
      expect(await alice.e2ee.explain(target)).toEqual({ state: "ready" });
      const ready = await observer.db.e2ee.recovery.status(material);
      expect(ready).toMatchObject({
        spaces: {
          validation: "checked",
          paths: [
            {
              spaceId: root!.id,
              epochId: root!.epochId,
              validation: "validated",
            },
          ],
        },
      });
      corrupt = true;
      const faulty = await observer.db.e2ee.recovery.status(material);
      expect(injected).toBeGreaterThan(0);
      expect(faulty).toMatchObject({
        spaces: {
          validation: "checked",
          paths: [
            {
              spaceId: root!.id,
              validation: "unavailable",
              reason: "unusable-recovery-delivery",
            },
          ],
        },
      });
      corrupt = false;
      // Both failure modes reject only this candidate, including its initial confirmation.
      for (const operation of ["current-confirmation", "recovery-open"] as const) {
        failCurrent = operation === "current-confirmation";
        failOpen = operation === "recovery-open";
        expect(await observer.db.e2ee.recovery.status(material)).toMatchObject({
          spaces: {
            validation: "checked",
            paths: [
              {
                spaceId: root!.id,
                epochId: root!.epochId,
                validation: "unavailable",
                reason: "unusable-recovery-delivery",
              },
            ],
          },
        });
        failCurrent = false;
        failOpen = false;
        expect(await observer.db.e2ee.recovery.status(material)).toMatchObject({
          spaces: { validation: "checked", paths: [{ validation: "validated" }] },
        });
      }
      // Alice removes herself, so cannot rotate the space on behalf of remaining Bob.
      await alice.e2ee.spaces.revoke(app.projects, project.id, aliceAccount.account.id).wait();
      const successors = await bob.all(app.__e2ee_space_successors, { tier: "remote" });
      expect(await observer.db.e2ee.recovery.status(material)).toMatchObject({
        spaces: {
          validation: "checked",
          paths: [{ spaceId: root!.id, validation: "unavailable", reason: "maintenance-required" }],
        },
      });
      expect(await bob.all(app.__e2ee_space_successors, { tier: "remote" })).toEqual(successors);
      expect(await bob.e2ee.explain(target)).toEqual({ state: "ready" });
      const rotated = await observer.db.e2ee.recovery.status(material);
      expect(rotated).toMatchObject({
        spaces: {
          validation: "checked",
          paths: [
            {
              spaceId: root!.id,
              epochId: expect.not.stringMatching(root!.epochId),
              validation: "validated",
            },
          ],
        },
      });
      failHistory = true;
      await expect(observer.db.e2ee.recovery.status(material)).rejects.toBe(historyError);
      failHistory = false;
      expect(await observer.db.e2ee.recovery.status(material)).toMatchObject({
        spaces: { validation: "checked", paths: [{ validation: "validated" }] },
      });
      await alice.e2ee.spaces.revoke(app.projects, project.id, bobAccount.account.id).wait();
      expect(await observer.db.e2ee.recovery.status(material)).toMatchObject({
        spaces: { validation: "checked", paths: [] },
      });
      expect(observer.saved()).toBeNull();
      expect(await bob.all(app.__e2ee_device_requests, { tier: "remote" })).toEqual(requests);
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
      await server.stop();
    }
  },
  180_000,
);
