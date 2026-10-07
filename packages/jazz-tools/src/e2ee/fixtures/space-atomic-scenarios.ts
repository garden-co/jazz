import { afterAll, beforeAll, expect, it } from "vitest";
import { schema as s } from "../../schema-namespace.js";
import { definePermissions } from "../../permissions/index.js";
import { createDb } from "../../runtime/default-create-db.js";
import { localAccountConfig } from "../../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "../device-requests.js";
import { spaceSchema } from "../spaces.js";
import { groupSchema } from "../groups.js";
import { createBrowserCrypto } from "../browser.js";
import { createNativeCrypto } from "../native.js";
import { createJazzSession } from "../../backend/create-jazz-session.js";
import { beginInitialSpaceTransaction, prepareInitialSpaceForTransaction } from "../lifecycle.js";

function newRows<T extends { id: string }>(
  rows: T[],
  previous: T[],
  remember?: (rows: T[]) => void,
): T[] {
  const ids = new Set(previous.map((row) => row.id));
  expect(rows.filter((row) => ids.has(row.id)).sort((a, b) => a.id.localeCompare(b.id))).toEqual(
    [...previous].sort((a, b) => a.id.localeCompare(b.id)),
  );
  remember?.(rows);
  return rows.filter((row) => !ids.has(row.id));
}
async function prepare(runtime: "wasm" | "native") {
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
    return {
      app,
      db,
      crypto,
      accountId,
      server,
      cleanup: async () => {
        await db?.shutdown();
        await nativeSession?.close();
        await server.stop();
      },
    };
  } catch (error) {
    await db?.shutdown();
    await nativeSession?.close();
    await server.stop();
    throw error;
  }
}
export function registerSpaceAtomicPreparation(runtime: "wasm" | "native") {
  let fixture: Awaited<ReturnType<typeof prepare>>;
  type History = {
    [K in
      | "projects"
      | "notes"
      | "__e2ee_spaces"
      | "__e2ee_space_grants"
      | "__e2ee_space_deliveries"]: s.RowOf<(typeof fixture.app)[K]>[];
  };
  let history: History = {
    __e2ee_space_deliveries: [],
    __e2ee_space_grants: [],
    __e2ee_spaces: [],
    notes: [],
    projects: [],
  };
  beforeAll(async () => {
    fixture = await prepare(runtime);
  }, 180000);
  afterAll(async () => {
    if (fixture) await fixture.cleanup();
  });
  it.each([
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
  ] as const)(
    `prepares real space metadata and ciphertext atomically (${runtime}, %s)`,
    async (outcome) => {
      const denied = outcome === "denied" || outcome.endsWith("-denied");
      const explicit = outcome.startsWith("explicit");
      const mixed = outcome.includes("-mixed-");
      const cold = outcome === "accepted" || outcome === "explicit";
      const current = cold ? await prepare(runtime) : fixture;
      const { app, db, crypto, accountId, server } = current;
      const before = cold
        ? {
            __e2ee_space_deliveries: [],
            __e2ee_space_grants: [],
            __e2ee_spaces: [],
            notes: [],
            projects: [],
          }
        : history;
      const observed: History = { ...before };
      const recipients: Awaited<ReturnType<typeof createDb>>[] = [];
      try {
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
          expect(
            newRows(await db.all(app.projects, { tier: "global" }), before.projects, (rows) => {
              observed.projects = rows;
            }),
          ).toEqual([]);
          expect(
            newRows(
              await db.all(app.__e2ee_spaces, { tier: "global" }),
              before.__e2ee_spaces,
              (rows) => {
                observed.__e2ee_spaces = rows;
              },
            ),
          ).toEqual([]);
          expect(
            newRows(
              await db.all(app.__e2ee_space_grants, { tier: "global" }),
              before.__e2ee_space_grants,
              (rows) => {
                observed.__e2ee_space_grants = rows;
              },
            ),
          ).toEqual([]);
          expect(
            newRows(await db.all(app.notes, { tier: "global" }), before.notes, (rows) => {
              observed.notes = rows;
            }),
          ).toEqual([]);
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
              preparedTx.insert(app.notes, {
                projectId: project.id,
                epochId: root.epochId,
                payload,
              });
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
          expect(
            newRows(await db.all(app.projects, { tier: "global" }), before.projects, (rows) => {
              observed.projects = rows;
            }),
          ).toEqual([]);
          expect(
            newRows(
              await db.all(app.__e2ee_spaces, { tier: "global" }),
              before.__e2ee_spaces,
              (rows) => {
                observed.__e2ee_spaces = rows;
              },
            ),
          ).toEqual([]);
          expect(
            newRows(
              await db.all(app.__e2ee_space_grants, { tier: "global" }),
              before.__e2ee_space_grants,
              (rows) => {
                observed.__e2ee_space_grants = rows;
              },
            ),
          ).toEqual([]);
          expect(
            newRows(await db.all(app.notes, { tier: "global" }), before.notes, (rows) => {
              observed.notes = rows;
            }),
          ).toEqual([]);
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
        await expect(retainedRead!()).rejects.toThrow(
          "Transaction preparation is no longer active",
        );
        expect(keyReference).toHaveLength(32);
        expect(keyReference!.every((byte) => byte === 0)).toBe(true);
        if (!earlyCommit) {
          expect(
            newRows(
              await db.all(app.__e2ee_spaces, { tier: "global" }),
              before.__e2ee_spaces,
              (rows) => {
                observed.__e2ee_spaces = rows;
              },
            ),
          ).toEqual([]);
          expect(
            newRows(
              await db.all(app.__e2ee_space_deliveries, { tier: "global" }),
              before.__e2ee_space_deliveries,
              (rows) => {
                observed.__e2ee_space_deliveries = rows;
              },
            ),
          ).toEqual([]);
        }
        const committed = earlyCommit ?? tx.commit();
        if (denied)
          await expect(committed.wait({ tier: "global" })).rejects.toMatchObject({
            code: "permission_denied",
          });
        else await committed.wait({ tier: "global" });
        const projects = newRows(
          await db.all(app.projects, { tier: "global" }),
          before.projects,
          (rows) => {
            observed.projects = rows;
          },
        );
        const roots = newRows(
          await db.all(app.__e2ee_spaces, { tier: "global" }),
          before.__e2ee_spaces,
          (rows) => {
            observed.__e2ee_spaces = rows;
          },
        );
        const grants = newRows(
          await db.all(app.__e2ee_space_grants, { tier: "global" }),
          before.__e2ee_space_grants,
          (rows) => {
            observed.__e2ee_space_grants = rows;
          },
        );
        const notes = newRows(await db.all(app.notes, { tier: "global" }), before.notes, (rows) => {
          observed.notes = rows;
        });
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
            const deliveries = newRows(
              await db.all(app.__e2ee_space_deliveries, { tier: "global" }),
              before.__e2ee_space_deliveries,
              (rows) => {
                observed.__e2ee_space_deliveries = rows;
              },
            );
            expect([...new Set(deliveries.map((row) => row.recipientAccountId))].sort()).toEqual(
              recipientAccounts.sort(),
            );
            expect(deliveries.every((row) => row.epochId === roots[0]!.epochId)).toBe(true);
          }
        }
        const deliveries = newRows(
          await db.all(app.__e2ee_space_deliveries, { tier: "global" }),
          before.__e2ee_space_deliveries,
          (rows) => {
            observed.__e2ee_space_deliveries = rows;
          },
        );
        if (denied) expect(deliveries).toEqual([]);
      } finally {
        if (!cold) {
          const final___e2ee_space_deliveries =
            observed.__e2ee_space_deliveries === before.__e2ee_space_deliveries
              ? await db.all(app.__e2ee_space_deliveries, { tier: "global" })
              : observed.__e2ee_space_deliveries;
          const added___e2ee_space_deliveries = newRows(
            final___e2ee_space_deliveries,
            before.__e2ee_space_deliveries,
          );
          if (denied || outcome.includes("invalid-identifier") || outcome === "rollback")
            expect(added___e2ee_space_deliveries).toEqual([]);
          history.__e2ee_space_deliveries = final___e2ee_space_deliveries;
          const final___e2ee_space_grants =
            observed.__e2ee_space_grants === before.__e2ee_space_grants
              ? await db.all(app.__e2ee_space_grants, { tier: "global" })
              : observed.__e2ee_space_grants;
          const added___e2ee_space_grants = newRows(
            final___e2ee_space_grants,
            before.__e2ee_space_grants,
          );
          if (denied || outcome.includes("invalid-identifier") || outcome === "rollback")
            expect(added___e2ee_space_grants).toEqual([]);
          history.__e2ee_space_grants = final___e2ee_space_grants;
          const final___e2ee_spaces =
            observed.__e2ee_spaces === before.__e2ee_spaces
              ? await db.all(app.__e2ee_spaces, { tier: "global" })
              : observed.__e2ee_spaces;
          const added___e2ee_spaces = newRows(final___e2ee_spaces, before.__e2ee_spaces);
          if (denied || outcome.includes("invalid-identifier") || outcome === "rollback")
            expect(added___e2ee_spaces).toEqual([]);
          history.__e2ee_spaces = final___e2ee_spaces;
          const final_notes =
            observed.notes === before.notes
              ? await db.all(app.notes, { tier: "global" })
              : observed.notes;
          const added_notes = newRows(final_notes, before.notes);
          if (denied || outcome.includes("invalid-identifier") || outcome === "rollback")
            expect(added_notes).toEqual([]);
          history.notes = final_notes;
          const final_projects =
            observed.projects === before.projects
              ? await db.all(app.projects, { tier: "global" })
              : observed.projects;
          const added_projects = newRows(final_projects, before.projects);
          if (denied || outcome.includes("invalid-identifier") || outcome === "rollback")
            expect(added_projects).toEqual([]);
          history.projects = final_projects;
        }
        await Promise.all(recipients.map((recipient) => recipient.shutdown()));
        if (cold) await current.cleanup();
      }
    },
    60000,
  );
}
