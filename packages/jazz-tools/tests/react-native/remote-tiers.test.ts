import { expect, it } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { schema } from "../../src/schema-namespace.js";
import { ReadTier } from "../../src/runtime/client.js";
import { withNativeRelayFixture } from "./fixture.js";

const app = schema.defineApp({
  todos: schema.table({ title: schema.string(), done: schema.boolean() }),
});

// These cases intentionally have no authority peer: an ordinary persisted
// local write is not evidence of Edge or Global admission.
it("keeps Edge and Global waits pending while local writes remain responsive", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const db = await fixture.createDb();
    const write = db.insert(app.todos, { title: "pending admission", done: false });
    const row = await write.wait({ tier: "local" });
    const settled: string[] = [];
    const waits = ["edge", "global"].map((tier) =>
      write.wait({ tier: tier as "edge" | "global" }).then(
        () => settled.push(tier),
        () => settled.push(`${tier}:closed`),
      ),
    );
    const later = await db
      .insert(app.todos, { title: "responsive local", done: false })
      .wait({ tier: "local" });
    expect(await db.all(app.todos.orderBy("title"))).toEqual([row, later]);
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(settled).toEqual([]);
    await db.shutdown();
    await Promise.all(waits);
    expect(settled.sort()).toEqual(["edge:closed", "global:closed"]);
  });
});

it("does not publish unconfirmed local rows to a strict remote subscription", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const db = await fixture.createDb();
    const remote: unknown[][] = [];
    const local: unknown[][] = [];
    const stopRemote = db.subscribe(app.todos, (rows) => remote.push(rows), {
      tier: ReadTier.Remote,
    });
    const stopLocal = db.subscribe(app.todos, (rows) => local.push(rows));
    try {
      const row = await db
        .insert(app.todos, { title: "local optimistic row", done: false })
        .wait({ tier: "local" });
      await expect.poll(() => local.at(-1)).toEqual([row]);
      expect(remote.flat()).toEqual([]);
      stopRemote();
      const count = remote.length;
      await db.update(app.todos, row.id, { done: true }).wait({ tier: "local" });
      await expect.poll(() => local.at(-1)).toEqual([{ ...row, done: true }]);
      expect(remote).toHaveLength(count);
    } finally {
      stopRemote();
      stopLocal();
    }
  });
});

it("resumes strict remote reads and both write tiers after native reconnect", async () => {
  const { startLocalJazzServer, startTestJwtIssuer, deploy, mergePermissionsIntoWasmSchema } =
    await import("../../src/testing/index.js");
  const issuer = await startTestJwtIssuer();
  const server = await startLocalJazzServer({
    inMemory: true,
    jwksUrl: issuer.jwksUrl,
    jwtIssuer: issuer.issuer,
    jwtAudience: issuer.audience,
  });
  try {
    const permissions = schema.definePermissions(app, ({ policy }) => [
      policy.todos.allowRead.always(),
      policy.todos.allowInsert.always(),
      policy.todos.allowUpdate.always(),
      policy.todos.allowDelete.always(),
    ]);
    await deploy({
      appId: server.appId,
      serverUrl: server.url,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    await withNativeRelayFixture(
      { wasmSchema: mergePermissionsIntoWasmSchema(app.wasmSchema, permissions) },
      async (fixture) => {
        const db = await fixture.createDb();
        expect(await db.all(app.todos, { tier: ReadTier.Remote })).toEqual([]);
        const strictSnapshots: unknown[][] = [];
        const stopStrict = db.subscribe(app.todos, (rows) => strictSnapshots.push(rows), {
          tier: ReadTier.Remote,
        });
        await expect.poll(() => strictSnapshots.at(-1)).toEqual([]);
        await db.disconnect();
        const write = db.insert(app.todos, { title: "offline queued", done: false });
        const row = await write.wait({ tier: "local" });
        expect(await db.all(app.todos, { tier: ReadTier.RemoteIfPossible })).toEqual([row]);
        const fallback: unknown[][] = [];
        const stopFallback = db.subscribe(app.todos, (rows) => fallback.push(rows), {
          tier: ReadTier.RemoteIfPossible,
        });
        await expect.poll(() => fallback.at(-1)).toEqual([row]);
        stopFallback();
        let remoteReady = false;
        let edgeReady = false;
        let globalReady = false;
        const remote = db.all(app.todos, { tier: ReadTier.Remote }).then((rows) => {
          remoteReady = true;
          return rows;
        });
        const edge = write.wait({ tier: "edge" }).then(() => {
          edgeReady = true;
        });
        const global = write.wait({ tier: "global" }).then(() => {
          globalReady = true;
        });
        // A parked remote read must not hold the native owner or prevent the
        // caller from persisting a subsequent local mutation.
        const laterWrite = db.update(app.todos, row.id, { done: true });
        await laterWrite.wait({ tier: "local" });
        await new Promise((resolve) => setTimeout(resolve, 100));
        expect(strictSnapshots.flat()).toEqual([]);
        expect([remoteReady, edgeReady, globalReady]).toEqual([false, false, false]);
        await db.reconnect();
        await Promise.all([edge, global]);
        // The parked read can observe an empty authority snapshot before the
        // queued upload is accepted. Completion proves fresh coverage; the
        // post-settlement read below asserts the accepted write is visible.
        const resumedRows = await remote;
        expect(resumedRows.every((candidate) => candidate.id === row.id)).toBe(true);
        await laterWrite.wait({ tier: "global" });
        await expect.poll(() => strictSnapshots.at(-1)).toEqual([{ ...row, done: true }]);
        stopStrict();
        const stoppedCount = strictSnapshots.length;
        await db.update(app.todos, row.id, { title: "after detach" }).wait({ tier: "global" });
        expect(strictSnapshots).toHaveLength(stoppedCount);
        expect(await db.all(app.todos, { tier: ReadTier.RemoteIfPossible })).toEqual([
          { ...row, title: "after detach", done: true },
        ]);
      },
      {
        appId: server.appId,
        session: {
          issuer: issuer.issuer,
          user_id: "rn-tier-reader",
          claims: { role: "user" },
          authMode: "external",
        },
        upstream: { serverUrl: server.url, jwt: issuer.jwtForUser("rn-tier-reader") },
      },
    );
  } finally {
    await server.stop();
    await issuer.stop();
  }
}, 30_000);

it("keeps local and RemoteIfPossible reads usable across an established native socket outage", async () => {
  const { startLocalJazzServer, startTestJwtIssuer, deploy, mergePermissionsIntoWasmSchema } =
    await import("../../src/testing/index.js");
  const issuer = await startTestJwtIssuer();
  const dataDir = await mkdtemp(join(tmpdir(), "jazz-rn-established-outage-"));
  let server = await startLocalJazzServer({
    dataDir,
    jwksUrl: issuer.jwksUrl,
    jwtIssuer: issuer.issuer,
    jwtAudience: issuer.audience,
  });
  try {
    const permissions = schema.definePermissions(app, ({ policy }) => [
      policy.todos.allowRead.always(),
      policy.todos.allowInsert.always(),
    ]);
    await deploy({
      appId: server.appId,
      serverUrl: server.url,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const initial = {
      appId: server.appId,
      port: server.port,
      adminSecret: server.adminSecret,
      backendSecret: server.backendSecret,
    };
    await withNativeRelayFixture(
      { wasmSchema: mergePermissionsIntoWasmSchema(app.wasmSchema, permissions) },
      async (fixture) => {
        const db = await fixture.createDb();
        expect(await db.all(app.todos, { tier: ReadTier.Remote })).toEqual([]);
        await server.stop();

        const write = db.insert(app.todos, { title: "durable through outage", done: false });
        const local = await write.wait({ tier: "local" });
        await expect.poll(async () => db.all(app.todos, { tier: ReadTier.Local })).toEqual([local]);
        await expect
          .poll(async () => db.all(app.todos, { tier: ReadTier.RemoteIfPossible }))
          .toEqual([local]);

        const strict = db.all(app.todos, { tier: ReadTier.Remote });
        await new Promise((resolve) => setTimeout(resolve, 100));
        let strictSettled = false;
        void strict.then(() => {
          strictSettled = true;
        });
        expect(strictSettled).toBe(false);

        server = await startLocalJazzServer({
          ...initial,
          dataDir,
          jwksUrl: issuer.jwksUrl,
          jwtIssuer: issuer.issuer,
          jwtAudience: issuer.audience,
        });
        await expect(strict).resolves.toEqual(expect.arrayContaining([local]));
        await write.wait({ tier: "global" });
      },
      {
        appId: initial.appId,
        session: {
          issuer: issuer.issuer,
          user_id: "rn-established-outage",
          claims: {},
          authMode: "external",
        },
        upstream: { serverUrl: server.url, jwt: issuer.jwtForUser("rn-established-outage") },
      },
    );
  } finally {
    await server.stop();
    await issuer.stop();
    await rm(dataDir, { recursive: true, force: true });
  }
}, 45_000);
