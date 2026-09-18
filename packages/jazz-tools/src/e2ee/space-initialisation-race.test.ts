import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createBrowserCellCipher, createBrowserKeyEnvelope } from "./browser.js";
import { beginInitialSpaceTransaction, prepareInitialSpaceForTransaction } from "./lifecycle.js";

function signal() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

it.each([false, true])(
  "accepts one competing space initialiser and retries against its accepted epoch (queued: %s)",
  async (queued) => {
    const app = s.defineApp({
      ...deviceRequestSchema,
      ...spaceSchema,
      projects: s.table({ title: s.string() }, {}),
      notes: s.table(
        { projectId: s.uuid(), epochId: s.uuid(), payload: s.bytes() },
        { project: s.rel("projects", "projectId") },
      ),
    });
    const policies = definePermissions(app, ({ policy, session }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.projects.allowRead.where(authenticated);
      policy.projects.allowInsert.where(authenticated);
      policy.notes.allowRead.where(authenticated);
      policy.notes.allowInsert.where(authenticated);
      policy.__e2ee_spaces.allowRead.where(authenticated);
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.where(authenticated);
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.where(authenticated);
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_space_successors.allowRead.where(authenticated);
      policy.__e2ee_space_recovery_deliveries.allowRead.where(authenticated);
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    const reached = [signal(), signal()];
    const resume = signal();
    let armed = false;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: { ...deviceRequestPermissions, ...policies },
      });
      const accounts = await Promise.all([
        localAccountConfig(server.appId, server.url),
        localAccountConfig(server.appId, server.url),
      ]);
      expect(accounts[0]!.account.id).not.toBe(accounts[1]!.account.id);
      const keys = await createBrowserKeyEnvelope();
      const cipher = await createBrowserCellCipher();
      const proposedEpochs: string[] = [];
      const provisionalKeys: Uint8Array[] = [];
      for (const [index, account] of accounts.entries()) {
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
              keyEnvelope: {
                ...keys,
                async wrap(secret, context, plaintext) {
                  if (armed) {
                    reached[index]!.resolve();
                    await resume.promise;
                  }
                  return keys.wrap(secret, context, plaintext);
                },
              },
            },
          },
        });
        clients.push(db);
        await db.e2ee.devices.list();
      }
      const project = await clients[0]!
        .insert(app.projects, { title: "Contended scope" })
        .wait({ tier: "global" });
      await clients[1]!.one(app.projects.where({ id: project.id }), { tier: "edge" });
      armed = true;
      const pending = clients.map((db, i) => {
        if (!queued)
          return db.e2ee.spaces.grant(app.projects, project.id, accounts[i]!.account.id).wait();
        const tx = beginInitialSpaceTransaction(db);
        const preparation = prepareInitialSpaceForTransaction(
          db,
          tx,
          app.projects,
          project.id,
          async (key, root, prepared) => {
            proposedEpochs[i] = root.epochId;
            provisionalKeys[i] = key;
            const plaintext = new Uint8Array([i + 1]);
            const context = new TextEncoder().encode("queued-initialisation-race");
            const payload = await cipher.encrypt(key, context, plaintext);
            expect(await cipher.decrypt(key, context, payload)).toEqual(plaintext);
            prepared.insert(app.notes, { projectId: project.id, epochId: root.epochId, payload });
          },
        );
        preparation.catch(() => {});
        const accepted = tx.commit().wait({ tier: "global" });
        return Promise.all([preparation, accepted]).then(() => {});
      });
      const settled = Promise.allSettled(pending);
      await Promise.race([
        Promise.all(reached.map((gate) => gate.promise)),
        ...pending.map(async (operation) => {
          await operation;
          throw new Error("Initialisation completed before both absent-space reads were held");
        }),
      ]);
      // Initial-key wrapping happens after each transaction has read the absence
      // predicate. Both proposals must therefore compete, not execute sequentially.
      armed = false;
      resume.resolve();
      const outcomes = await settled;
      expect(outcomes.filter((result) => result.status === "fulfilled")).toHaveLength(1);
      expect(outcomes.filter((result) => result.status === "rejected")).toHaveLength(1);
      const winnerIndex = outcomes.findIndex((result) => result.status === "fulfilled");
      const loserIndex = 1 - winnerIndex;
      const rejected = outcomes[loserIndex]!;
      if (rejected.status !== "rejected") throw new Error("Missing rejected initialisation");
      // Authority checks permissions before conflicts. A competing stable root
      // may therefore fail the update policy before its absence precondition.
      expect(["exclusive_conflict", "permission_denied"]).toContain(rejected.reason.code);
      const winner = clients[winnerIndex]!;
      const loser = clients[loserIndex]!;
      const target = { scope: app.projects, identifier: project.id };
      const roots = await winner.all(app.__e2ee_spaces, { tier: "edge" });
      expect(roots).toHaveLength(1);
      if (queued) {
        expect(new Set(proposedEpochs).size).toBe(2);
        expect(provisionalKeys).toHaveLength(2);
        for (const key of provisionalKeys) {
          expect(key).toHaveLength(32);
          expect(key.every((byte) => byte === 0)).toBe(true);
        }
        const notes = await winner.all(app.notes, { tier: "global" });
        expect(notes).toHaveLength(1);
        expect(notes[0]).toMatchObject({ projectId: project.id, epochId: roots[0]!.epochId });
        expect(roots[0]!.epochId).toBe(proposedEpochs[winnerIndex]);
        expect(notes[0]!.epochId).not.toBe(proposedEpochs[loserIndex]);
        expect(notes[0]!.payload).not.toEqual(new Uint8Array([winnerIndex + 1]));
      }
      const grants = await winner.all(app.__e2ee_space_grants, { tier: "edge" });
      expect(grants).toHaveLength(1);
      expect(grants[0]).toMatchObject({
        id: roots[0]!.initialGrantId,
        spaceId: roots[0]!.id,
        recipientId: accounts[winnerIndex]!.account.id,
        epochId: roots[0]!.epochId,
      });
      expect(await winner.e2ee.explain(target)).toEqual({ state: "ready" });
      expect(await loser.e2ee.explain(target)).toMatchObject({ state: "refused" });
      expect(
        await winner.all(
          app.__e2ee_space_deliveries.where({
            recipientAccountId: accounts[loserIndex]!.account.id,
          }),
          { tier: "edge" },
        ),
      ).toEqual([]);

      await loser.e2ee.spaces
        .grant(app.projects, project.id, accounts[loserIndex]!.account.id)
        .wait();
      expect(await winner.e2ee.explain(target)).toEqual({ state: "ready" });
      expect(await loser.e2ee.explain(target)).toEqual({ state: "ready" });
      expect(await winner.all(app.__e2ee_spaces, { tier: "edge" })).toEqual(roots);
      expect(await winner.all(app.__e2ee_space_grants, { tier: "edge" })).toHaveLength(2);
    } finally {
      resume.resolve();
      await Promise.all(clients.map((client) => client.shutdown()));
      await server.stop();
    }
  },
  60_000,
);
