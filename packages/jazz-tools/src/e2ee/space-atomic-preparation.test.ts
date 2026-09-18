import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { groupSchema } from "./groups.js";
import { createBrowserCrypto } from "./browser.js";
import { createNativeCrypto } from "./native.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { beginInitialSpaceTransaction, prepareInitialSpaceForTransaction } from "./lifecycle.js";

it.each(
  [
    "accepted",
    "denied",
    "invalid-identifier",
    "early-commit",
    "early-denied",
    "early-invalid-identifier",
    "rollback",
    "explicit",
    "explicit-denied",
    "explicit-set",
    "explicit-set-denied",
    "explicit-mixed-set",
    "explicit-mixed-set-denied",
  ].flatMap((outcome) => ["wasm", "native"].map((runtime) => ({ outcome, runtime }))),
)(
  "prepares real space metadata and ciphertext atomically ($runtime, $outcome)",
  async ({ outcome, runtime }) => {
    const denied = outcome === "denied" || outcome.endsWith("-denied");
    const explicit = outcome.startsWith("explicit");
    const mixed = outcome.includes("-mixed-");
    const app = s.defineApp({
      ...deviceRequestSchema,
      ...spaceSchema,
      ...groupSchema,
      projects: s.table({ title: s.string() }, {}),
      notes: s.table(
        { projectId: s.uuid(), epochId: s.string(), payload: s.bytes() },
        { project: s.rel("projects", "projectId") },
      ),
    });
    const policies = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.where({ title: "Allowed" });
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
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
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
    const recipients: Awaited<ReturnType<typeof createDb>>[] = [];
    let nativeSession: Awaited<ReturnType<typeof createJazzSession>> | undefined;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: { ...deviceRequestPermissions, ...policies },
      });
      let saved: string | null = null;
      const crypto = await (runtime === "native" ? createNativeCrypto() : createBrowserCrypto());
      const e2ee = {
        app,
        crypto,
        store: {
          async read() {
            return saved;
          },
          async update(transform: (current: string | null) => string) {
            saved = transform(saved);
          },
        },
      };
      let accountId: string;
      if (runtime === "native") {
        nativeSession = await createJazzSession({
          appId: server.appId,
          serverUrl: server.url,
          app,
          permissions: { ...deviceRequestPermissions, ...policies },
          driver: { type: "memory" },
          initial: "local-first",
          e2ee,
        });
        db = nativeSession.getSnapshot().client!.db;
        accountId = nativeSession.getSnapshot().account!.id;
      } else {
        const account = await localAccountConfig(server.appId, server.url);
        db = await createDb({ ...account, e2ee });
        accountId = account.account.id;
      }
      await db.e2ee.devices.list();
      const recipientIds: string[] = [];
      for (let i = 0; i < (explicit ? (outcome.includes("-set") ? 2 : 1) : 0); i++) {
        const account = await localAccountConfig(server.appId, server.url);
        recipientIds.push(account.account.id);
        let recipientSaved: string | null = null;
        const recipient = await createDb({
          ...account,
          e2ee: {
            app,
            store: {
              async read() {
                return recipientSaved;
              },
              async update(transform) {
                recipientSaved = transform(recipientSaved);
              },
            },
          },
        });
        recipients.push(recipient);
        await recipient.e2ee.devices.list();
      }
      const initialRecipients = explicit ? recipientIds : undefined;
      const recipientAccounts = recipientIds.slice();
      if (mixed) {
        const group = await recipients[0]!.e2ee.groups.create().wait();
        recipientIds[0] = group.id;
      }
      // The compiler opts in before staging writes; callers do not prefetch history.
      const tx = beginInitialSpaceTransaction(db, initialRecipients);
      const project = tx.insert(app.projects, { title: denied ? "Denied" : "Allowed" });
      if (outcome.endsWith("invalid-identifier")) {
        const preparation = prepareInitialSpaceForTransaction(
          db,
          tx,
          app.projects,
          "invalid",
          async () => {
            throw new Error("Data preparation must not run without a key");
          },
        );
        preparation.catch(() => {});
        if (outcome === "early-invalid-identifier")
          await expect(tx.commit().wait({ tier: "global" })).rejects.toThrow(
            "Invalid E2EE space identifier",
          );
        await expect(preparation).rejects.toThrow("Invalid E2EE space identifier");
        await expect(async () => await tx.commit().wait({ tier: "global" })).rejects.toThrow();
        expect(await db.all(app.projects, { tier: "global" })).toEqual([]);
        expect(await db.all(app.__e2ee_spaces, { tier: "global" })).toEqual([]);
        expect(await db.all(app.__e2ee_space_grants, { tier: "global" })).toEqual([]);
        expect(await db.all(app.notes, { tier: "global" })).toEqual([]);
        return;
      }
      const plaintext = new Uint8Array([11, 22, 33]);
      const context = new TextEncoder().encode("atomic-preparation-test");
      let keyReference: Uint8Array | undefined;
      let prepared = false;
      let retainedRead: (() => Promise<unknown>) | undefined;
      let retainedWrite: (() => unknown) | undefined;
      let resume!: () => void;
      let entered!: () => void;
      const resumed = new Promise<void>((resolve) => {
        resume = resolve;
      });
      const started = new Promise<void>((resolve) => {
        entered = resolve;
      });
      // Internal compiler seam only; this does not implement encrypted-column transforms.
      const preparation = prepareInitialSpaceForTransaction(
        db,
        tx,
        app.projects,
        project.id,
        async (key, root, preparedTx) => {
          keyReference = key;
          if (outcome === "rollback") {
            entered();
            await resumed;
          }
          const payload = await crypto.cellCipher.encrypt(key, context, plaintext);
          expect(await crypto.cellCipher.decrypt(key, context, payload)).toEqual(plaintext);
          preparedTx.insert(app.notes, { projectId: project.id, epochId: root.epochId, payload });
          retainedRead = () => preparedTx.all(app.notes, { tier: "local" });
          retainedWrite = () =>
            preparedTx.insert(app.notes, { projectId: project.id, epochId: root.epochId, payload });
          prepared = true;
        },
        initialRecipients,
      );
      preparation.catch(() => {});
      if (outcome === "rollback") {
        await Promise.race([started, preparation]);
        try {
          await tx.rollback();
        } finally {
          resume();
        }
        await expect(preparation).rejects.toThrow("Transaction preparation is no longer active");
        expect(keyReference).toHaveLength(32);
        expect(keyReference!.every((byte) => byte === 0)).toBe(true);
        await expect(async () => await tx.commit().wait({ tier: "global" })).rejects.toThrow();
        expect(await db.all(app.projects, { tier: "global" })).toEqual([]);
        expect(await db.all(app.__e2ee_spaces, { tier: "global" })).toEqual([]);
        expect(await db.all(app.__e2ee_space_grants, { tier: "global" })).toEqual([]);
        expect(await db.all(app.notes, { tier: "global" })).toEqual([]);
        return;
      }
      const earlyCommit = outcome.startsWith("early-") ? tx.commit() : undefined;
      if (earlyCommit) {
        if (denied)
          await expect(earlyCommit.wait({ tier: "global" })).rejects.toMatchObject({
            code: "permission_denied",
          });
        else await earlyCommit.wait({ tier: "global" });
        expect(prepared).toBe(true);
      }
      await preparation;
      expect(retainedWrite).toThrow("Transaction preparation is no longer active");
      await expect(retainedRead!()).rejects.toThrow("Transaction preparation is no longer active");
      expect(keyReference).toHaveLength(32);
      expect(keyReference!.every((byte) => byte === 0)).toBe(true);
      if (!earlyCommit) {
        expect(await db.all(app.__e2ee_spaces, { tier: "global" })).toEqual([]);
        expect(await db.all(app.__e2ee_space_deliveries, { tier: "global" })).toEqual([]);
      }
      const committed = earlyCommit ?? tx.commit();
      if (denied)
        await expect(committed.wait({ tier: "global" })).rejects.toMatchObject({
          code: "permission_denied",
        });
      else await committed.wait({ tier: "global" });
      const projects = await db.all(app.projects, { tier: "global" });
      const roots = await db.all(app.__e2ee_spaces, { tier: "global" });
      const grants = await db.all(app.__e2ee_space_grants, { tier: "global" });
      const notes = await db.all(app.notes, { tier: "global" });
      if (denied) {
        expect([projects, roots, grants, notes]).toEqual([[], [], [], []]);
      } else {
        expect(projects).toHaveLength(1);
        expect(roots).toHaveLength(1);
        expect(grants).toHaveLength(explicit ? recipientIds.length : 1);
        expect(grants.find((grant) => grant.id === roots[0]!.initialGrantId)).toMatchObject({
          id: roots[0]!.initialGrantId,
          recipientId: recipientIds[0] ?? accountId,
        });
        expect(grants.map((grant) => grant.recipientId).sort()).toEqual(
          (explicit ? [...recipientIds] : [accountId]).sort(),
        );
        if (mixed) {
          expect(grants.find((grant) => grant.recipientId === recipientIds[0])).toMatchObject({
            recipientKind: "group",
          });
          expect(grants.find((grant) => grant.recipientId === recipientIds[1])).toMatchObject({
            recipientKind: "account",
          });
        }
        expect(notes).toHaveLength(1);
        expect(notes[0]!.epochId).toBe(roots[0]!.epochId);
        expect(notes[0]!.payload).not.toEqual(plaintext);
        const target = { scope: app.projects, identifier: project.id };
        expect(await db.e2ee.explain(target)).toEqual(
          explicit ? { state: "refused", reason: "not-a-space-recipient" } : { state: "ready" },
        );
        for (const recipient of recipients)
          expect(await recipient.e2ee.explain(target)).toEqual({ state: "ready" });
        if (mixed) {
          const deliveries = await db.all(app.__e2ee_space_deliveries, { tier: "global" });
          expect([...new Set(deliveries.map((row) => row.recipientAccountId))].sort()).toEqual(
            recipientAccounts.sort(),
          );
          expect(deliveries.every((row) => row.epochId === roots[0]!.epochId)).toBe(true);
        }
      }
    } finally {
      await db?.shutdown();
      await Promise.all(recipients.map((recipient) => recipient.shutdown()));
      await nativeSession?.close();
      await server.stop();
    }
  },
  60_000,
);
