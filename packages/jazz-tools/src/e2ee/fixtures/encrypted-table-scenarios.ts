import { expect } from "vitest";
import { Buffer } from "node:buffer";
import { schema as s } from "../../schema-namespace.js";
import { definePermissions } from "../../permissions/index.js";
import { createDb } from "../../runtime/default-create-db.js";
import type { QueryBuilder } from "../../runtime/db.js";
import { localAccountConfig } from "../../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../../testing/index.js";
import { createBrowserCrypto } from "../browser.js";
import { encodeEnvelope } from "../envelope.js";
import { deviceRequestSchema, deviceRequestPermissions } from "../device-requests.js";
import { spaceSchema } from "../spaces.js";

export type EncryptedTableScenario =
  | "immediate"
  | "after-yield"
  | "plaintext-first"
  | "transaction-read"
  | "projections"
  | "equality-index"
  | "equality-collision"
  | "equality-owned-bytes"
  | "equality-device-invalidation"
  | "equality-device-invalidation-recipient"
  | "equality-uuid-scope"
  | "equality-epoch-race"
  | "transaction-projection"
  | "offline-at-submit"
  | "byoc-buffer"
  | "ordinary-insert-and-update"
  | "ordinary-scope-upsert"
  | "transaction-scope-upsert";

export async function runEncryptedTableScenario(timing: EncryptedTableScenario) {
  // Value-type equality tests own their separate schema. These scenarios only
  // need the lifecycle tables and the relations used by projection checks.
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...spaceSchema,
    projects: s.table(
      { title: s.string() },
      {
        linkedNotesViaProject: s.reverse("linkedNotes", "projectRelation"),
        notesViaProject: s.reverse("notes", "project"),
      },
    ),
    events: s.table({ message: s.string() }, {}),
    linkedNotes: s
      .table(
        { project: s.uuid(), title: s.string() },
        { projectRelation: s.rel("projects", "project") },
      )
      .encrypted({ space: "project", columns: ["title"] }),
    notes: s
      .table(
        {
          projectId: s.uuid(),
          title: s.string(),
          done: s.boolean(),
          payload: s.bytes(),
        },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({
        space: "projectId",
        columns: ["title", "payload"],
        ...(timing.startsWith("equality-") ? { indexes: { title: "equality" as const } } : {}),
        ...(timing === "equality-owned-bytes" ? { indexes: { payload: "equality" as const } } : {}),
      }),
  });
  const policies = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.events.allowRead.always();
    policy.events.allowInsert.always();
    policy.projects.allowInsert.always();
    if (timing === "ordinary-scope-upsert") policy.projects.allowUpdate.always();
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
    policy.notes.allowUpdate.always();
    policy.notes.allowDelete.always();
    policy.linkedNotes.allowRead.always();
    policy.linkedNotes.allowInsert.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.always();
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: { ...deviceRequestPermissions, ...policies },
    });
    const account = await localAccountConfig(server.appId, server.url);
    const crypto = await createBrowserCrypto();
    let encrypted = 0;
    let decrypted = 0;
    let failEncryption = false;
    let failDecryption = false;
    let failHistory = false;
    let deniedHistory = 0;
    let duringToken: (() => Promise<void>) | undefined;
    let saved: string | null = null;
    db = await createDb({
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
            async unwrap(key, context, envelope) {
              if (failHistory) {
                const text = new TextDecoder().decode(context);
                if (text.includes('"successor"') && text.includes('"history"')) {
                  deniedHistory++;
                  throw new Error("Historical key unavailable");
                }
              }
              return crypto.keyEnvelope.unwrap(key, context, envelope);
            },
          },
          equalityIndex: {
            mechanism: crypto.equalityIndex!.mechanism,
            async token(...args) {
              const token = await crypto.equalityIndex!.token(...args);
              const action = duringToken;
              duringToken = undefined;
              await action?.();
              return token;
            },
          },
          ...(timing === "equality-collision"
            ? {
                equalityIndex: {
                  mechanism: crypto.equalityIndex!.mechanism,
                  async token() {
                    // Deliberately make every candidate collide through the public BYOC seam.
                    return encodeEnvelope(crypto.equalityIndex!.mechanism, new Uint8Array(32));
                  },
                },
              }
            : {}),
          cellCipher: {
            mechanism: crypto.cellCipher.mechanism,
            async encrypt(...args) {
              encrypted++;
              const ciphertext = await crypto.cellCipher.encrypt(...args);
              if (failEncryption) throw new Error("Test cell encryption failure");
              // Enrolment has completed, but the initialisation transaction
              // cannot reach the authority after its ciphertext is prepared.
              if (timing === "offline-at-submit" && encrypted === 2) await server.stop();
              return ciphertext;
            },
            async decrypt(...args) {
              decrypted++;
              if (failDecryption) throw new Error("Test cell decryption failure");
              const plaintext = await crypto.cellCipher.decrypt(...args);
              if (timing !== "byoc-buffer") return plaintext;
              const buffer = Buffer.from(plaintext);
              plaintext.fill(0);
              return buffer;
            },
          },
        },
      },
    });
    if (timing === "ordinary-insert-and-update") {
      const project = db.insert(app.projects, { title: "Project" });
      expect(project).not.toBeInstanceOf(Promise);
      await project.wait({ tier: "global" });
      expect(await db.e2ee.explain({ scope: app.projects, identifier: project.value.id })).toEqual({
        state: "ready",
      });
      const data = {
        projectId: project.value.id,
        title: "Ordinary encrypted insert",
        done: false,
        payload: new Uint8Array([7, 8, 9]),
      };
      failEncryption = true;
      const failedInsert = db.insert(app.notes, data);
      expect(failedInsert).not.toBeInstanceOf(Promise);
      expect(failedInsert.value.title).toBe(data.title);
      await expect(failedInsert.wait({ tier: "global" })).rejects.toThrow(
        "Could not encrypt the value",
      );
      expect(await db.all(app.notes, { tier: "remote" })).toEqual([]);
      expect(encrypted).toBeGreaterThan(0);
      failEncryption = false;

      // The same scope must remain usable after failed encryption. Continue
      // through accepted inserts and updates without repeating enrolment.
      const note = db.insert(app.notes, data);
      expect(note).not.toBeInstanceOf(Promise);
      expect(note.value.title).toBe(data.title);
      await note.wait({ tier: "global" });
      expect(await db.one(app.notes.where({ id: note.value.id }), { tier: "remote" })).toEqual(
        note.value,
      );
      expect(encrypted).toBeGreaterThan(0);
      expect(decrypted).toBeGreaterThan(0);
      const beforePlaintextUpdate = encrypted;
      const plaintext = db.update(app.notes, note.value.id, { done: true });
      expect(plaintext).not.toBeInstanceOf(Promise);
      await plaintext.wait({ tier: "global" });
      expect(encrypted).toBe(beforePlaintextUpdate);
      const update = db.update(app.notes, note.value.id, {
        title: "Changed encrypted title",
        payload: new Uint8Array([10, 11]),
      });
      expect(update).not.toBeInstanceOf(Promise);
      await update.wait({ tier: "global" });
      expect(
        await db.one(app.notes.where({ id: note.value.id }), { tier: "remote" }),
      ).toMatchObject({
        title: "Changed encrypted title",
        payload: new Uint8Array([10, 11]),
        done: true,
      });
      const tx = db.beginTransaction();
      expect(tx.update(app.notes, note.value.id, { title: "Transaction update" })).toBeUndefined();
      await tx.commit().wait({ tier: "global" });
      expect(
        await db.one(app.notes.where({ id: note.value.id }), { tier: "remote" }),
      ).toMatchObject({
        title: "Transaction update",
        payload: new Uint8Array([10, 11]),
        done: true,
      });
      const other = db.insert(app.projects, { title: "Other project" });
      await other.wait({ tier: "global" });
      await expect(
        db.update(app.notes, note.value.id, { projectId: other.value.id }).wait({ tier: "global" }),
      ).rejects.toThrow("immutable");
      const beforeSameSpace = encrypted;
      await db
        .update(app.notes, note.value.id, { projectId: project.value.id, done: false })
        .wait({ tier: "global" });
      expect(encrypted).toBe(beforeSameSpace);
      failEncryption = true;
      const failed = db.update(app.notes, note.value.id, {
        title: "Must not replace the value",
      });
      expect(failed).not.toBeInstanceOf(Promise);
      await expect(failed.wait({ tier: "global" })).rejects.toThrow("Could not encrypt the value");
      failEncryption = false;
      expect(
        await db.one(app.notes.where({ id: note.value.id }), { tier: "remote" }),
      ).toMatchObject({
        projectId: project.value.id,
        title: "Transaction update",
        done: false,
      });
      return;
    }
    const scopeId = globalThis.crypto.randomUUID();
    if (timing === "ordinary-scope-upsert") {
      const data = { title: "Project" };
      const first = db.upsert(app.projects, scopeId, data);
      data.title = "Changed after upsert returned";
      expect(first).not.toBeInstanceOf(Promise);
      await first.wait({ tier: "global" });
      expect(await db.one(app.projects.where({ id: scopeId }), { tier: "remote" })).toEqual({
        id: scopeId,
        title: "Project",
      });
      expect(await db.e2ee.explain({ scope: app.projects, identifier: scopeId })).toEqual({
        state: "ready",
      });
      await db.upsert(app.projects, scopeId, { title: "Project" }).wait({ tier: "global" });
      expect(await db.all(app.__e2ee_spaces, { tier: "remote" })).toHaveLength(1);
      expect(await db.all(app.__e2ee_space_grants, { tier: "remote" })).toHaveLength(1);
    }
    const tx = db.beginExclusiveTransaction();
    const event =
      timing === "plaintext-first"
        ? tx.insert(app.events, { message: "Created together" })
        : undefined;
    if (timing === "after-yield") await Promise.resolve();
    if (timing === "transaction-scope-upsert") {
      const data = { title: "Project" };
      tx.upsert(app.projects, scopeId, data);
      data.title = "Changed after upsert returned";
    }
    const project = timing.endsWith("scope-upsert")
      ? { id: scopeId, title: "Project" }
      : tx.insert(
          app.projects,
          { title: "Project" },
          timing === "equality-uuid-scope"
            ? { id: "abcdef01-2345-4678-9abc-def012345678" }
            : undefined,
        );
    const note = tx.insert(app.notes, {
      projectId: project.id,
      title: "Private title",
      done: false,
      payload: new Uint8Array([1, 2, 255]),
    });
    expect(project).not.toBeInstanceOf(Promise);
    expect(note).not.toBeInstanceOf(Promise);
    if (timing === "equality-uuid-scope") {
      for (const identifier of [project.id.toUpperCase(), project.id.replaceAll("-", "")]) {
        expect(
          await tx.all(app.notes.where({ projectId: identifier, title: note.title }), {
            tier: "local-first",
          }),
        ).toEqual([note]);
      }
    }
    if (timing === "equality-uuid-scope") {
      expect(
        await tx.all(
          app.notes.where({ projectId: project.id, title: "Private title" }).select("id", "title"),
          { tier: "local-first" },
        ),
      ).toEqual([{ id: note.id, title: note.title }]);
    }
    if (timing === "transaction-read") {
      expect(await tx.one(app.notes.where({ id: note.id }), { tier: "local-first" })).toEqual(note);
    }
    if (timing === "transaction-projection") {
      expect(
        await tx.one(app.notes.where({ id: note.id }).select("id", "title"), {
          tier: "local-first",
        }),
      ).toEqual({ id: note.id, title: "Private title" });
    }
    const committed = tx.commit();
    expect(committed).not.toBeInstanceOf(Promise);
    if (timing === "offline-at-submit") {
      const localWait = committed.wait({ tier: "local" });
      localWait.catch(() => {});
      await expect
        .poll(async () => (await db!.all(app.projects, { tier: "local-first" })).length, {
          timeout: 10_000,
        })
        .toBe(1);
      expect(encrypted).toBeGreaterThan(0);
      // Physical local visibility is not authoritative acceptance. Give an
      // incorrectly local-only wait time to settle before checking the floor.
      expect(
        await Promise.race([
          localWait.then(
            () => "resolved",
            () => "rejected",
          ),
          new Promise<string>((resolve) => setTimeout(() => resolve("pending"), 100)),
        ]),
      ).toBe("pending");
      return;
    }
    await committed.wait({ tier: "global" });
    // Read accepted keys in a fresh transaction before ordinary reads warm the
    // index fixture, and after the UUID fixture's uncommitted-key projection.
    if (timing === "equality-uuid-scope" || timing === "equality-index") {
      const reader = db.beginTransaction();
      try {
        expect(
          await reader.all(app.notes.where({ projectId: project.id, title: "Private title" }), {
            tier: "local-first",
          }),
        ).toEqual([note]);
      } finally {
        await reader.rollback();
      }
    }
    if (timing === "equality-uuid-scope") {
      for (const identifier of [project.id.toUpperCase(), project.id.replaceAll("-", "")]) {
        expect(
          await db.all(app.notes.where({ projectId: identifier, title: note.title }), {
            tier: "remote",
          }),
        ).toEqual([note]);
      }
      // Canonical and alternate UUID spellings must both track the same
      // membership dependency, including terminal failure rather than an empty result.
      const observers = [project.id, project.id.toUpperCase()].map((identifier) => {
        const state: { snapshots: unknown[][]; failure?: Error } = { snapshots: [] };
        const stop = db!.subscribe(
          app.notes.where({ projectId: identifier, title: note.title }),
          {
            onUpdate: (rows) => state.snapshots.push(rows),
            onError: (error) => {
              state.failure = error;
            },
          },
          { tier: "remote" },
        );
        return { state, stop };
      });
      try {
        for (const { state } of observers)
          await expect
            .poll(() => state.failure ?? state.snapshots.at(-1), { timeout: 10_000 })
            .toEqual([note]);
        await db.e2ee.spaces.revoke(app.projects, project.id, account.account.id).wait();
        for (const { state } of observers) {
          await expect.poll(() => state.failure?.name, { timeout: 10_000 }).toBe("E2eeDataError");
          expect(state.snapshots.at(-1)).toEqual([note]);
          expect(state.snapshots.some((rows) => rows.length === 0)).toBe(false);
        }
      } finally {
        for (const { stop } of observers) stop();
      }
      return;
    }

    if (timing === "equality-epoch-race") {
      const recipientAccount = await localAccountConfig(server.appId, server.url);
      let recipientSaved: string | null = null;
      const recipient = await createDb({
        ...recipientAccount,
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
      const database = db;
      const query = app.notes.where({ projectId: project.id, title: "After rotation" });
      const history = app.notes.where({ projectId: project.id, title: note.title }).orderBy("done");
      const snapshots: unknown[][] = [];
      const historySnapshots: Array<Array<typeof note>> = [];
      let failure: Error | undefined;
      let historyFailure: Error | undefined;
      let stop: (() => void) | undefined;
      let stopHistory: (() => void) | undefined;
      let expected: typeof note | undefined;
      let expectedHistory: Array<typeof note> | undefined;
      let pendingHistory: Promise<Array<typeof note>> | undefined;
      let historyReadStarted = false;
      let releaseHistoryRead = () => {};
      const historyReadGate = new Promise<void>((resolve) => {
        releaseHistoryRead = resolve;
      });
      try {
        await recipient.e2ee.devices.list();
        await database.e2ee.spaces
          .grant(app.projects, project.id, recipientAccount.account.id)
          .wait();
        duringToken = async () => {
          // Hold both an empty and a nonempty candidate query at their old
          // tokens. Claim the second gate before any subscription can use it.
          duringToken = async () => {
            historyReadStarted = true;
            await historyReadGate;
          };
          pendingHistory = database.all(history, { tier: "remote" });
          void pendingHistory.catch(() => {});
          await expect.poll(() => historyReadStarted, { timeout: 30_000 }).toBe(true);
          stop = database.subscribe(
            query,
            {
              onUpdate: (rows) => snapshots.push(rows),
              onError: (error) => {
                failure = error;
              },
            },
            { tier: "remote" },
          );
          stopHistory = database.subscribe(
            history,
            {
              onUpdate: (rows) => historySnapshots.push(rows),
              onError: (error) => {
                historyFailure = error;
              },
            },
            { tier: "remote" },
          );
          await expect.poll(() => failure ?? snapshots.at(-1), { timeout: 30_000 }).toEqual([]);
          await expect
            .poll(() => historyFailure ?? historySnapshots.at(-1), { timeout: 30_000 })
            .toEqual([note]);
          await database.e2ee.spaces
            .revoke(app.projects, project.id, recipientAccount.account.id)
            .wait();
          expect(
            await database.e2ee.explain({ scope: app.projects, identifier: project.id }),
          ).toEqual({ state: "ready" });
          const added = database.insert(app.notes, {
            projectId: project.id,
            title: "After rotation",
            done: false,
            payload: new Uint8Array([9]),
          });
          await added.wait({ tier: "global" });
          expected = added.value;
          const current = database.insert(app.notes, {
            projectId: project.id,
            title: note.title,
            done: true,
            payload: new Uint8Array([10]),
          });
          await current.wait({ tier: "global" });
          expectedHistory = [note, current.value];
          releaseHistoryRead();
        };
        const rows = await database.all(query, { tier: "remote" });
        expect(expected).toBeDefined();
        expect(rows).toEqual([expected]);
        expect(expectedHistory).toBeDefined();
        expect(await pendingHistory).toEqual(expectedHistory);
        await expect
          .poll(() => failure ?? snapshots.at(-1), { timeout: 30_000 })
          .toEqual([expected]);
        expect(failure).toBeUndefined();

        // Both foreground reads and both subscriptions predate this rotation.
        // Reuse its mixed-epoch dataset for ordering and pagination.

        expect(await database.all(history, { tier: "remote" })).toEqual(expectedHistory);
        expect(await database.all(history.limit(1), { tier: "remote" })).toEqual([note]);
        expect(await database.all(history.offset(1).limit(1), { tier: "remote" })).toEqual([
          expectedHistory![1],
        ]);
        await expect
          .poll(() => historyFailure ?? historySnapshots.at(-1), { timeout: 30_000 })
          .toEqual(expectedHistory);
        expect(historySnapshots.every((rows) => rows.some((row) => row.id === note.id))).toBe(true);
        expect(historyFailure).toBeUndefined();
        stop?.();
        stopHistory?.();

        failHistory = true;
        await expect(database.all(history, { tier: "remote" })).rejects.toMatchObject({
          name: "E2eeDataError",
        });
        expect(deniedHistory).toBeGreaterThan(0);
        const incomplete: unknown[][] = [];
        let incompleteFailure: Error | undefined;
        const stopIncomplete = database.subscribe(
          history,
          {
            onUpdate: (rows) => incomplete.push(rows),
            onError: (error) => {
              incompleteFailure = error;
            },
          },
          { tier: "remote" },
        );
        try {
          await expect
            .poll(() => incompleteFailure?.name, { timeout: 30_000 })
            .toBe("E2eeDataError");
          expect(incomplete).toEqual([]);
        } finally {
          stopIncomplete();
        }
      } finally {
        duringToken = undefined;
        releaseHistoryRead();
        stop?.();
        stopHistory?.();
        await pendingHistory?.catch(() => {});
        await recipient.shutdown();
      }
      return;
    }
    if (timing.startsWith("equality-device-invalidation")) {
      const subscriberAccount = timing.endsWith("recipient")
        ? await localAccountConfig(server.appId, server.url)
        : account;
      let subscriberSaved: string | null = null;
      const subscriber =
        subscriberAccount === account
          ? db
          : await createDb({
              ...subscriberAccount,
              e2ee: {
                app,
                store: {
                  async read() {
                    return subscriberSaved;
                  },
                  async update(transform) {
                    subscriberSaved = transform(subscriberSaved);
                  },
                },
              },
            });
      const [original] = await subscriber.e2ee.devices.list();
      if (subscriber !== db)
        await db.e2ee.spaces.grant(app.projects, project.id, subscriberAccount.account.id).wait();
      let saved: string | null = null;
      const other = await createDb({
        ...subscriberAccount,
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
        },
      });
      try {
        const pending = (await other.e2ee.devices.list()).find(
          (device) => device.state === "pending",
        )!;
        await subscriber.e2ee.devices.approve(pending.id).wait();
        const snapshots: unknown[][] = [];
        let failure: Error | undefined;
        const stop = subscriber.subscribe(
          app.notes.where({ projectId: project.id, title: note.title }),
          {
            onUpdate: (rows) => snapshots.push(rows),
            onError: (error) => {
              failure = error;
            },
          },
          { tier: "remote" },
        );
        try {
          await expect.poll(() => failure ?? snapshots.at(-1), { timeout: 30_000 }).toEqual([note]);
          await other.e2ee.devices.revoke(original!.id).wait();
          await expect.poll(() => failure?.name, { timeout: 10_000 }).toBe("E2eeDataError");
          expect(snapshots.some((rows) => rows.length === 0)).toBe(false);
        } finally {
          stop();
        }
      } finally {
        await other.shutdown();
        if (subscriber !== db) await subscriber.shutdown();
      }
      return;
    }
    if (timing === "equality-owned-bytes") {
      const snapshots: Array<Array<{ id: string; payload: Uint8Array }>> = [];
      let failure: Error | undefined;
      const stop = db.subscribe(
        app.notes.where({ projectId: project.id, payload: { eq: new Uint8Array([1, 2, 255]) } }),
        {
          onUpdate: (rows) => snapshots.push(rows),
          onError: (error) => {
            failure = error;
          },
        },
        { tier: "remote" },
      );
      try {
        await expect
          .poll(() => failure ?? snapshots.at(-1)?.map((row) => row.id), { timeout: 10_000 })
          .toEqual([note.id]);
        snapshots.at(-1)![0]!.payload.fill(0);
        const next = db.insert(app.notes, {
          projectId: project.id,
          title: "Another match",
          done: false,
          payload: new Uint8Array([1, 2, 255]),
        });
        await next.wait({ tier: "global" });
        await expect
          .poll(
            () =>
              failure ??
              snapshots
                .at(-1)
                ?.map((row) => row.id)
                .sort(),
            { timeout: 10_000 },
          )
          .toEqual([note.id, next.value.id].sort());
      } finally {
        stop();
      }
      return;
    }
    if (timing === "equality-index" || timing === "equality-collision") {
      const other = db.insert(app.notes, {
        projectId: project.id,
        title: "Other title",
        done: false,
        payload: new Uint8Array([3]),
      });
      await other.wait({ tier: "global" });
      const query = app.notes.where({ projectId: project.id, title: "Private title" });
      expect(await db.all(query, { tier: "remote" })).toEqual([note]);
      await db.update(app.notes, note.id, { done: true }).wait({ tier: "global" });
      // The false-positive candidate sorts first, but cannot consume the logical limit.
      expect(
        await db.all(query.orderBy("done").limit(1).select("id", "title"), { tier: "remote" }),
      ).toEqual([{ id: note.id, title: note.title }]);
      const snapshots: unknown[][] = [];
      let failure: Error | undefined;
      const stop = db.subscribe(
        query.orderBy("done").select("id", "title").limit(1),
        {
          onUpdate: (rows) => snapshots.push(rows),
          onError: (error) => {
            failure = error;
          },
        },
        { tier: "remote" },
      );
      try {
        await expect
          .poll(() => failure ?? snapshots.at(-1), { timeout: 30_000 })
          .toEqual([{ id: note.id, title: note.title }]);
        expect(await db.all(query.offset(1), { tier: "remote" })).toEqual([]);
        expect(
          await db.all(app.notes.where({ projectId: project.id, title: "Absent" }), {
            tier: "remote",
          }),
        ).toEqual([]);
        await expect(
          db.all(app.notes.where({ title: "Private title" }), { tier: "remote" }),
        ).rejects.toThrow("space");
        await db.update(app.notes, note.id, { title: "Changed title" }).wait({ tier: "global" });
        expect(await db.all(query, { tier: "remote" })).toEqual([]);
        await expect.poll(() => failure ?? snapshots.at(-1), { timeout: 30_000 }).toEqual([]);
        const changed = app.notes.where({ projectId: project.id, title: "Changed title" });
        expect(await db.all(changed, { tier: "remote" })).toEqual([
          { ...note, title: "Changed title", done: true },
        ]);
        failEncryption = true;
        await expect(
          db.update(app.notes, note.id, { title: "Failed title" }).wait({ tier: "global" }),
        ).rejects.toThrow("Could not encrypt the value");
        failEncryption = false;
        expect(await db.all(changed, { tier: "remote" })).toEqual([
          { ...note, title: "Changed title", done: true },
        ]);
        await db.update(app.notes, note.id, { title: note.title }).wait({ tier: "global" });
        await expect
          .poll(() => failure ?? snapshots.at(-1), { timeout: 30_000 })
          .toEqual([{ id: note.id, title: note.title }]);
        expect(failure).toBeUndefined();
      } finally {
        failEncryption = false;
        stop();
      }
      return;
    }
    if (timing === "projections") {
      // All read shapes use the same accepted ciphertext. Enrolment and atomic
      // creation have their own cold fixtures; these checks exercise decoding
      // and live results, including a separate terminal-error subscription.
      const database = db;
      const query = app.notes.where({ id: note.id });
      expect(await database.one(query.select("id", "title"), { tier: "remote" })).toEqual({
        id: note.id,
        title: note.title,
      });
      const target = { scope: app.projects, identifier: project.id };
      // Concurrent delivery checks may follow the read's background maintenance.
      expect(
        await Promise.allSettled([database.e2ee.explain(target), database.e2ee.explain(target)]),
      ).toEqual([
        { status: "fulfilled", value: { state: "ready" } },
        { status: "fulfilled", value: { state: "ready" } },
      ]);
      expect(await database.one(query, { tier: "remote" })).toEqual(note);
      expect(encrypted).toBeGreaterThan(0);
      expect(decrypted).toBeGreaterThan(0);

      const linked = database.insert(app.linkedNotes, {
        project: project.id,
        title: "Linked secret",
      });
      await linked.wait({ tier: "global" });
      const reference = app.projects.where({ id: project.id }).include({
        linkedNotesViaProject: app.linkedNotes.include({ projectRelation: true }),
      });
      const expectedReference = {
        ...project,
        linkedNotesViaProject: [{ ...linked.value, projectRelation: project }],
      };
      expect(await database.one(reference, { tier: "remote" })).toEqual(expectedReference);
      const referenceReader = database.beginTransaction();
      try {
        expect(await referenceReader.one(reference, { tier: "local-first" })).toEqual(
          expectedReference,
        );
      } finally {
        await referenceReader.rollback();
      }

      const stops: (() => void)[] = [];
      const checkUpdates: (() => Promise<void>)[] = [];
      async function observe<T extends { id: string }>(
        label: string,
        observedQuery: QueryBuilder<T>,
        initial: T,
        updated: T,
      ) {
        const snapshots: T[][] = [];
        let failure: Error | undefined;
        const before = decrypted;
        stops.push(
          database.subscribe(
            observedQuery,
            {
              onUpdate: (rows) => snapshots.push(rows),
              onError: (error) => {
                failure = error;
              },
            },
            { tier: "remote" },
          ),
        );
        await expect
          .poll(() => failure ?? snapshots.at(-1), { timeout: 30_000, message: label })
          .toEqual([initial]);
        expect(decrypted, label).toBeGreaterThan(before);
        checkUpdates.push(async () => {
          await expect
            .poll(() => failure ?? snapshots.at(-1), { timeout: 30_000, message: label })
            .toEqual([updated]);
          expect(failure, label).toBeUndefined();
        });
      }
      try {
        for (const projected of [false, true]) {
          const nested = app.projects.where({ id: project.id }).include({
            notesViaProject: projected ? app.notes.select("id", "title") : true,
          });
          const child = projected ? { id: note.id, title: note.title } : note;
          const expected = { ...project, notesViaProject: [child] };
          expect(await database.one(nested, { tier: "remote" })).toEqual(expected);
          const reader = database.beginTransaction();
          try {
            expect(await reader.one(nested, { tier: "local-first" })).toEqual(expected);
          } finally {
            await reader.rollback();
          }
          await observe(projected ? "nested projection" : "nested full row", nested, expected, {
            ...project,
            notesViaProject: [{ ...child, title: "Subscription update" }],
          });
        }
        await observe("full row", query, note, { ...note, title: "Subscription update" });
        await observe(
          "projection",
          query.select("id", "title"),
          { id: note.id, title: note.title },
          {
            id: note.id,
            title: "Subscription update",
          },
        );
        // Each subscription has its own callbacks and must observe both states.
        await database
          .update(app.notes, note.id, { title: "Subscription update" })
          .wait({ tier: "global" });
        for (const check of checkUpdates) await check();
      } finally {
        for (const stop of stops) stop();
      }

      const snapshots: unknown[][] = [];
      let failure: Error | undefined;
      const beforeFailure = decrypted;
      failDecryption = true;
      const stop = database.subscribe(
        query,
        {
          onUpdate: (rows) => snapshots.push(rows),
          onError: (error) => {
            failure = error;
          },
        },
        { tier: "remote" },
      );
      try {
        await expect
          .poll(() => failure?.message, { timeout: 10_000 })
          .toBe("Encrypted data could not be authenticated or decoded");
        expect(decrypted).toBeGreaterThan(beforeFailure);
        expect(failure).toMatchObject({ name: "E2eeDataError", code: "invalid-ciphertext" });
        expect(failure).not.toHaveProperty("cause");
        expect(snapshots).toEqual([]);
      } finally {
        stop();
        failDecryption = false;
      }
      return;
    }
    if (event) {
      expect(await db.one(app.events.where({ id: event.id }), { tier: "remote" })).toEqual(event);
    }
    if (timing === "transaction-scope-upsert") {
      expect(await db.one(app.projects.where({ id: scopeId }), { tier: "remote" })).toEqual({
        id: scopeId,
        title: "Project",
      });
    }
    expect(await db.e2ee.explain({ scope: app.projects, identifier: project.id })).toEqual({
      state: "ready",
    });
    expect(await db.one(app.notes.where({ id: note.id }), { tier: "remote" })).toMatchObject({
      id: note.id,
      projectId: project.id,
      title: "Private title",
      done: false,
      payload: new Uint8Array([1, 2, 255]),
    });
    expect(encrypted).toBeGreaterThan(0);
    expect(decrypted).toBeGreaterThan(0);
    if (timing === "transaction-read") {
      const reader = db.beginTransaction();
      try {
        expect(await reader.one(app.notes.where({ id: note.id }), { tier: "local-first" })).toEqual(
          note,
        );
      } finally {
        await reader.rollback();
      }
    }
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}
