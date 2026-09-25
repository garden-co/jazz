import { expect, it, vi } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { clearTimeout, setTimeout } from "node:timers";
import { createJazzSession, type JazzClient } from "../backend/create-jazz-session.js";
import type { JazzSession } from "../session/state.js";
import type { AccountStore } from "../accounts/persistence.js";
import { transportGate, type TransportGate } from "../runtime/testing/tcp-transport-gate.js";
import { schema as s } from "../schema-namespace.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { definePermissions } from "../permissions/index.js";
import { deploy, startLocalJazzServer, type LocalJazzServerHandle } from "../testing/index.js";
import { deviceRequestSchema, groupSchema, spaceSchema } from "./managed-schema.js";

it("streams a plaintext-only update without loading encryption keys", async () => {
  const physical = s.defineApp({
    ...deviceRequestSchema,
    ...groupSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
    files: s.table(
      { projectId: s.uuid(), payload: s.bytes(), notes: s.string() },
      { project: s.rel("projects", "projectId") },
    ),
  });
  const app = s.defineApp({
    projects: s.table({ title: s.string() }, {}),
    files: s
      .table(
        { projectId: s.uuid(), payload: s.bytes(), notes: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["payload"] }),
  });
  const permissions = definePermissions(physical, ({ policy }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.files.allowRead.always();
    policy.files.allowInsert.always();
    policy.files.allowUpdate.always();
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: physical,
      permissions,
    });
    db = await createDb(await localAccountConfig(server.appId, server.url));
    const project = db.insert(physical.projects, { title: "Public" });
    await project.wait({ tier: "global" });
    // Existing opaque bytes must survive without the caller knowing a key.
    const file = db.insert(physical.files, {
      projectId: project.value.id,
      payload: new Uint8Array([7, 8]),
      notes: "Before",
    });
    await file.wait({ tier: "global" });
    const source = (async function* () {
      yield "After";
    })();
    const update = await db.updateStreaming(app.files, file.value.id, { notes: source });
    await update.wait({ tier: "global" });
    expect(await db.one(physical.files.where({ id: file.value.id }), { tier: "global" })).toEqual({
      ...file.value,
      notes: "After",
    });
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 30_000);

it.each(["scope-insert", "scope-upsert", "encrypted-scalar", "space-change"])(
  "rejects streaming bypasses before consuming a source (%s)",
  async (operation) => {
    const app = s.defineApp({
      projects: s.table({ title: s.string() }, {}),
      files: s
        .table(
          { projectId: s.uuid(), payload: s.bytes(), notes: s.string() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["payload"] }),
    });
    const db = await createDb(
      await localAccountConfig(`encrypted-stream-bypass-${crypto.randomUUID()}`),
    );
    let consumed = 0;
    const source = (async function* () {
      consumed++;
      yield new Uint8Array([1, 2, 3]);
    })();
    try {
      const id = crypto.randomUUID();
      const pending =
        operation === "scope-insert"
          ? db.insertStreaming(app.projects, { title: source })
          : operation === "scope-upsert"
            ? db.upsertStreaming(app.projects, id, { title: source })
            : operation === "encrypted-scalar"
              ? db.updateStreaming(app.files, id, { notes: source, payload: new Uint8Array([9]) })
              : db.updateStreaming(app.files, id, {
                  notes: source,
                  projectId: crypto.randomUUID(),
                });
      await expect(pending).rejects.toThrow();
      expect(consumed).toBe(0);
    } finally {
      await db.shutdown();
    }
  },
);

it.each(["insert", "update", "upsert", "partial"])(
  "rejects keyless encrypted %s without consuming plaintext",
  async (operation) => {
    const app = s.defineApp({
      projects: s.table({ title: s.string() }, {}),
      files: s
        .table(
          { projectId: s.uuid(), payload: s.bytes() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["payload"] }),
    });
    const db = await createDb(await localAccountConfig(`encrypted-stream-${crypto.randomUUID()}`));
    let consumed = 0;
    const source = (async function* () {
      consumed++;
      yield new Uint8Array([1, 2, 3]);
    })();
    try {
      const id = crypto.randomUUID();
      const data = { projectId: crypto.randomUUID(), payload: source };
      if (operation === "partial") {
        expect(() =>
          db.update(
            app.files,
            id,
            {},
            {
              applyDiffs: {
                payload: {
                  within: { from: 0, to: 1 },
                  splices: [{ at: 0, delete: 1, insert: new Uint8Array([9]) }],
                },
              },
            },
          ),
        ).toThrow();
      } else {
        const pending =
          operation === "insert"
            ? db.insertStreaming(app.files, data)
            : operation === "update"
              ? db.updateStreaming(app.files, id, data)
              : db.upsertStreaming(app.files, id, data);
        await expect(pending).rejects.toThrow();
      }
      expect(consumed).toBe(0);
    } finally {
      await db.shutdown();
    }
  },
);

it.each(["text", "json", "indexed", "implicit-branch"] as const)(
  "rejects unsupported encrypted stream coordinates before source use (%s)",
  async (kind) => {
    const files = s
      .table(
        {
          projectId: s.uuid(),
          payload: kind === "text" ? s.string() : kind === "json" ? s.json() : s.bytes(),
        },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({
        space: "projectId",
        columns: ["payload"],
        ...(kind === "indexed" ? { indexes: { payload: "equality" as const } } : {}),
      });
    const app = s.defineApp({
      projects: s.table({ title: s.string() }, {}),
      files: kind === "implicit-branch" ? files.branchBy("projectId") : files,
    });
    const db = await createDb(
      await localAccountConfig(`encrypted-stream-coordinate-${crypto.randomUUID()}`),
    );
    let consumed = 0;
    try {
      await expect(
        db.insertStreaming(app.files, {
          projectId: crypto.randomUUID(),
          payload: (async function* () {
            consumed++;
            yield new Uint8Array([1]);
          })(),
        }),
      ).rejects.toThrow();
      expect(consumed).toBe(0);
    } finally {
      await db.shutdown();
    }
  },
);

it("uploads an encrypted image to an accepted space after a transport-offline persistent reopen", async () => {
  const app = s.defineApp({
    projects: s.table({ title: s.string() }, {}),
    files: s
      .table(
        { projectId: s.uuid(), name: s.string(), payload: s.bytes() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["payload"] }),
  });
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.files.allowRead.always();
    policy.files.allowInsert.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
  });
  const retainedStore = (): AccountStore => {
    let value: string | null = null;
    return {
      async read() {
        return value;
      },
      async update(transform) {
        value = transform(value);
      },
    };
  };
  const within = async <T>(operation: PromiseLike<T>, label: string): Promise<T> => {
    // Bound real TCP/native-owner progress without advancing its scheduler using
    // fake timers. Successful operations never wait for this failure deadline.
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        operation,
        new Promise<never>((_resolve, reject) => {
          timer = setTimeout(() => reject(new Error(`${label} waited for authority`)), 10_000);
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  };
  let server: LocalJazzServerHandle | undefined;
  let gate: TransportGate | undefined;
  let directory: string | undefined;
  let owner: JazzSession<JazzClient> | undefined;
  let failed = false;
  try {
    directory = await mkdtemp(join(tmpdir(), "jazz-accepted-offline-upload-"));
    server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    gate = await transportGate(server.url);
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    // Reuse both private stores and the actual persistent NAPI owner across close.
    // No crypto overrides: createJazzSession supplies production native crypto.
    const config = {
      appId: server.appId,
      serverUrl: gate.url,
      app,
      permissions,
      driver: { type: "persistent" as const, dataPath: join(directory, "database") },
      initial: "local-first" as const,
      store: retainedStore(),
      e2ee: { app, store: retainedStore() },
    };
    owner = await createJazzSession(config);
    const before = owner.getSnapshot().client!.db;
    const accountId = owner.getSnapshot().account!.id;
    const image = new Uint8Array(
      Buffer.from(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a5X8AAAAASUVORK5CYII=",
        "base64",
      ),
    );
    const tx = before.beginExclusiveTransaction();
    const project = tx.insert(
      app.projects,
      { title: "Accepted before transport loss" },
      { initialRecipients: [accountId] },
    );
    const original = tx.insert(app.files, {
      projectId: project.id,
      name: "online.png",
      payload: image,
    });
    await tx.commit().wait({ tier: "global" });
    expect(await before.one(app.files.where({ id: original.id }), { tier: "global" })).toEqual(
      original,
    );
    const acceptedRoot = await before.one(app.__e2ee_spaces.where({ identifier: project.id }), {
      tier: "global",
    });
    expect(acceptedRoot).toMatchObject({ accountId });
    const historyFailure = new Error("Accepted-history storage unavailable");
    const observeHistory = vi
      .spyOn(before, "observeE2eeHistory")
      .mockRejectedValueOnce(historyFailure);
    let failedSourceRuns = 0;
    try {
      await expect(
        before.streamingTransaction((plan) =>
          plan.insertStreaming(app.files, {
            projectId: project.id,
            name: "must-not-upload.png",
            payload: (async function* () {
              failedSourceRuns++;
              yield image;
            })(),
          }),
        ),
      ).rejects.toBe(historyFailure);
      expect(failedSourceRuns).toBe(0);
    } finally {
      observeHistory.mockRestore();
    }
    await owner.close();
    owner = undefined;

    gate.block();
    // Neither offline configuration nor disconnect() may enable this operation.
    owner = await createJazzSession(config);
    const reopened = owner.getSnapshot().client!.db;
    expect(owner.getSnapshot().account!.id).toBe(accountId);
    expect(
      await within(
        reopened.one(app.files.where({ id: original.id }), { tier: "local" }),
        "Retained encrypted image read",
      ),
    ).toEqual(original);
    let callbacks = 0;
    let sourceRuns = 0;
    let chunks = 0;
    const upload = await within(
      reopened.streamingTransaction((plan) => {
        callbacks++;
        return plan.insertStreaming(app.files, {
          projectId: project.id,
          name: "offline.png",
          payload: (async function* () {
            sourceRuns++;
            chunks++;
            yield image.subarray(0, 31);
            chunks++;
            yield image.subarray(31);
          })(),
        }).id;
      }),
      "Accepted-space encrypted stream preparation",
    );
    const imageId = await within(upload.wait({ tier: "local" }), "Encrypted upload Local receipt");
    const expected = { id: imageId, projectId: project.id, name: "offline.png", payload: image };
    expect(
      await within(
        reopened.one(app.files.where({ id: imageId }), { tier: "local" }),
        "Offline uploaded image read",
      ),
    ).toEqual(expected);
    expect({ callbacks, sourceRuns, chunks }).toEqual({ callbacks: 1, sourceRuns: 1, chunks: 2 });

    gate.unblock();
    await reopened.reconnect();
    await upload.wait({ tier: "global" });
    expect(await reopened.one(app.files.where({ id: imageId }), { tier: "global" })).toEqual(
      expected,
    );
    expect(
      await reopened.one(app.__e2ee_spaces.where({ identifier: project.id }), { tier: "global" }),
    ).toEqual(acceptedRoot);
    expect({ callbacks, sourceRuns, chunks }).toEqual({ callbacks: 1, sourceRuns: 1, chunks: 2 });
  } catch (error) {
    failed = true;
    throw error;
  } finally {
    // Release stalled preparation before closing; cleanup errors must not replace
    // the bounded offline-preparation failure, and every resource gets a close.
    gate?.unblock();
    const errors: unknown[] = [];
    for (const close of [
      () => owner?.close(),
      () => gate?.close(),
      () => server?.stop(),
      () => directory && rm(directory, { recursive: true, force: true }),
    ]) {
      try {
        await close();
      } catch (error) {
        errors.push(error);
      }
    }
    if (!failed && errors.length) throw new AggregateError(errors, "Offline upload cleanup failed");
  }
}, 60_000);

it.each(["warn", "reject"] as const)(
  "applies staleWrites %s before consuming an accepted-space offline stream",
  async (staleWrites) => {
    const app = s.defineApp({
      projects: s.table({ title: s.string() }, {}),
      files: s
        .table(
          { projectId: s.uuid(), payload: s.bytes() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["payload"] }),
    });
    const permissions = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.always();
      policy.files.allowRead.always();
      policy.files.allowInsert.always();
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      // Retain a known-stale accepted epoch rather than automatically rotating it.
      policy.__e2ee_space_successors.allowRead.always();
      policy.__e2ee_space_successors.allowInsert.never();
    });
    const sessions: JazzSession<JazzClient>[] = [];
    let server: LocalJazzServerHandle | undefined;
    let gate: TransportGate | undefined;
    let failed = false;
    const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
      gate = await transportGate(server.url);
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      for (let index = 0; index < 2; index++) {
        let stored: string | null = null;
        sessions.push(
          await createJazzSession({
            appId: server.appId,
            serverUrl: gate.url,
            app,
            permissions,
            driver: { type: "memory" },
            initial: "local-first",
            e2ee: {
              app,
              staleWrites,
              store: {
                async read() {
                  return stored;
                },
                async update(transform) {
                  stored = transform(stored);
                },
              },
            },
          }),
        );
      }
      const owner = sessions[0]!.getSnapshot().client!.db;
      const departing = sessions[1]!.getSnapshot().client!.db;
      const ownerId = sessions[0]!.getSnapshot().account!.id;
      const departingId = sessions[1]!.getSnapshot().account!.id;
      await owner.e2ee.devices.list();
      await departing.e2ee.devices.list();
      const project = await owner
        .insert(
          app.projects,
          { title: "Retained streaming epoch" },
          { initialRecipients: [ownerId, departingId] },
        )
        .wait({ tier: "global" });
      await departing.e2ee.spaces.revoke(app.projects, project.id, departingId).wait();
      expect(
        await owner.all(
          app.__e2ee_space_grants.where({ recipientId: departingId, operation: "remove" }),
          { tier: "global" },
        ),
      ).toEqual([expect.objectContaining({ recipientId: departingId, operation: "remove" })]);
      expect(await owner.e2ee.explain({ scope: app.projects, identifier: project.id })).toEqual({
        state: "maintenance-required",
        reason: "recipient-removed",
      });
      gate.block();
      warning.mockClear();
      let sourceRuns = 0;
      const upload = owner.insertStreaming(app.files, {
        projectId: project.id,
        payload: (async function* () {
          sourceRuns++;
          yield new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10]);
        })(),
      });
      if (staleWrites === "reject") {
        await expect(upload).rejects.toMatchObject({ code: "maintenance-required" });
        expect(sourceRuns).toBe(0);
        expect(warning).not.toHaveBeenCalled();
      } else {
        await (await upload).wait({ tier: "local" });
        expect(sourceRuns).toBe(1);
        expect(warning).toHaveBeenCalledOnce();
      }
    } catch (error) {
      failed = true;
      throw error;
    } finally {
      warning.mockRestore();
      gate?.unblock();
      const errors: unknown[] = [];
      for (const close of [
        ...sessions.map((session) => () => session.close()),
        () => gate?.close(),
        () => server?.stop(),
      ]) {
        try {
          await close();
        } catch (error) {
          errors.push(error);
        }
      }
      if (!failed && errors.length) throw new AggregateError(errors, "Stale stream cleanup failed");
    }
  },
  60_000,
);
