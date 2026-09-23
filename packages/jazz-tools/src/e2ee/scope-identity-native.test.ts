import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { createDb } from "../runtime/default-create-db.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createAccountDbWithRuntimeSource } from "../accounts/context.js";
import type { JazzClient } from "../runtime/client.js";
import type { Db } from "../runtime/db.js";
import { DefaultRuntimeSource } from "../runtime/default-runtime-source.js";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import type { RuntimeClientContext } from "../runtime/runtime-source.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";

it("resolves the same accepted scope through WASM and native Node clients", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const app = s.defineApp({ projects: s.table({ title: s.string() }, {}) });
  const permissions = definePermissions(app, ({ policy }) => {
    policy.projects.allowRead.always();
  });
  let owner: Awaited<ReturnType<typeof createJazzSession>> | undefined;
  let wasm: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    owner = await createJazzSession({
      appId: server.appId,
      serverUrl: server.url,
      app,
      permissions,
      driver: { type: "memory" },
      initial: "local-first",
    });
    const native = owner.getSnapshot().client!.db;
    wasm = await createDb({
      appId: server.appId,
      serverUrl: server.url,
      account: owner.getSnapshot().account!,
      driver: { type: "memory" },
    });
    await wasm.all(app.projects, { tier: "edge" });
    await native.all(app.projects, { tier: "edge" });
    const expected = await wasm.tableIdentity(app.projects);
    expect(expected).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    expect(await native.tableIdentity(app.projects)).toBe(expected);
    await wasm.shutdown();
    await expect(wasm.tableIdentity(app.projects)).rejects.toThrow();
  } finally {
    await wasm?.shutdown();
    await owner?.close();
    await server.stop();
  }
}, 30_000);

class IdentityRuntimeSource extends DefaultRuntimeSource {
  client!: JazzClient;

  override createClient(context: RuntimeClientContext): JazzClient {
    const client = super.createClient(context);
    // The first client owns the runtime; later clients can be fixed schema views.
    this.client ??= client;
    return client;
  }
}

it("preserves accepted scope identities across a server-published table rename", async () => {
  const before = { projects: s.table({ title: s.string() }, {}) };
  const after = { initiatives: s.table({ title: s.string() }, {}) };
  const oldApp = s.defineApp(before);
  const newApp = s.defineApp(after);
  const migration = s.defineMigration({
    from: before,
    to: after,
    renameTables: { initiatives: s.renameTableFrom("projects") },
  });
  const oldPermissions = definePermissions(oldApp, ({ policy }) => {
    policy.projects.allowRead.always();
  });
  const newPermissions = definePermissions(newApp, ({ policy }) => {
    policy.initiatives.allowRead.always();
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const aliceSource = new IdentityRuntimeSource();
  const bobSource = new IdentityRuntimeSource();
  let alice: Db | undefined;
  let bob: Db | undefined;
  const oldViews: NativeRuntimeAdapter[] = [];
  try {
    const target = { serverUrl: server.url, appId: server.appId, adminSecret: server.adminSecret };
    await deploy({ ...target, schema: oldApp, permissions: oldPermissions });
    alice = await createAccountDbWithRuntimeSource(
      await localAccountConfig(server.appId, server.url),
      aliceSource,
    );
    bob = await createAccountDbWithRuntimeSource(
      await localAccountConfig(server.appId, server.url),
      bobSource,
    );
    await alice.all(oldApp.projects, { tier: "edge" });
    await bob.all(oldApp.projects, { tier: "edge" });
    const aliceOwner = aliceSource.client.getRuntime();
    const bobOwner = bobSource.client.getRuntime();
    if (
      !(aliceOwner instanceof NativeRuntimeAdapter) ||
      !(bobOwner instanceof NativeRuntimeAdapter)
    ) {
      throw new Error("Scope identity migration requires the actual native runtime adapters");
    }
    const owners = [aliceOwner, bobOwner];
    const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
    await expect
      .poll(() => aliceOwner.tableIdentity("projects"), { timeout: 10_000 })
      .toMatch(uuid);
    await expect
      .poll(() => aliceOwner.columnIdentity("projects", "title"), { timeout: 10_000 })
      .toMatch(uuid);
    const tableIdentity = await aliceOwner.tableIdentity("projects");
    const columnEpoch = await aliceOwner.columnIdentity("projects", "title");
    for (const owner of owners) {
      await expect
        .poll(() => owner.tableIdentity("projects"), { timeout: 10_000 })
        .toBe(tableIdentity);
      await expect
        .poll(() => owner.columnIdentity("projects", "title"), { timeout: 10_000 })
        .toBe(columnEpoch);
      expect(await owner.tableIdentity("initiatives")).toBeNull();
      expect(await owner.columnIdentity("initiatives", "title")).toBeNull();
      // Register only an already accepted view: registering the future schema
      // here could author it locally and bypass the server publication under test.
      const oldView = owner.registerSchemaView(
        mergePermissionsIntoWasmSchema(oldApp.wasmSchema, oldPermissions),
      );
      oldViews.push(oldView);
      expect(await oldView.tableIdentity("projects")).toBe(tableIdentity);
      expect(await oldView.columnIdentity("projects", "title")).toBe(columnEpoch);
    }

    await deploy({ ...target, schema: newApp, permissions: newPermissions, migration });
    await alice.disconnect();
    await bob.disconnect();
    await alice.reconnect();
    await bob.reconnect();

    for (const owner of owners) {
      await expect
        .poll(() => owner.tableIdentity("initiatives"), { timeout: 10_000 })
        .toBe(tableIdentity);
      await expect
        .poll(() => owner.columnIdentity("initiatives", "title"), { timeout: 10_000 })
        .toBe(columnEpoch);
      expect(await owner.tableIdentity("projects")).toBeNull();
      expect(await owner.columnIdentity("projects", "title")).toBeNull();
      expect(await owner.tableIdentity("unknown")).toBeNull();
      expect(await owner.columnIdentity("initiatives", "unknown")).toBeNull();
    }
    for (const oldView of oldViews) {
      expect(await oldView.tableIdentity("projects")).toBe(tableIdentity);
      expect(await oldView.columnIdentity("projects", "title")).toBe(columnEpoch);
      expect(await oldView.tableIdentity("initiatives")).toBeNull();
      expect(await oldView.columnIdentity("initiatives", "title")).toBeNull();
      expect(await oldView.tableIdentity("unknown")).toBeNull();
      expect(await oldView.columnIdentity("projects", "unknown")).toBeNull();
    }
  } finally {
    for (const oldView of oldViews) await oldView.close();
    await bob?.shutdown();
    await alice?.shutdown();
    await server.stop();
  }
}, 30_000);
