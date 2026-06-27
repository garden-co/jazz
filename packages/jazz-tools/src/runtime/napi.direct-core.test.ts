import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WebSocket } from "undici";
import { afterEach, describe, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { startLocalJazzServer, type LocalJazzServerHandle } from "../testing/index.js";
import { directWebSocketUrl } from "./core-runtime/direct-websocket.js";
import { openConfig } from "./core-runtime/direct-codec.js";
import { CoreRuntime } from "./core-runtime/runtime.js";
import { encodeDirectSchema } from "./core-runtime/runtime.js";
import { hasJazzNapiBuild, loadNapiModule } from "./testing/napi-runtime-test-utils.js";

const TEST_SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

const ALICE_ID = "00000000-0000-4000-8000-0000000000a1";
const BOB_ID = "00000000-0000-4000-8000-0000000000b2";

const OWNED_TODOS_SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
      { name: "owner_id", column_type: { type: "Text" }, nullable: false },
    ],
    policies: {
      select: {
        using: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["user_id"] },
        },
      },
      insert: {
        with_check: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["user_id"] },
        },
      },
      update: {
        using: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["user_id"] },
        },
      },
      delete: {
        using: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["user_id"] },
        },
      },
    },
  },
};

describe.skipIf(!hasJazzNapiBuild())("jazz-napi core runtime memory DB", () => {
  let server: LocalJazzServerHandle | null = null;
  const runtimes: CoreRuntime[] = [];
  const previousWebSocket = globalThis.WebSocket;

  afterEach(async () => {
    for (const runtime of runtimes.splice(0)) {
      runtime.close();
    }
    await server?.stop();
    server = null;
    globalThis.WebSocket = previousWebSocket;
  });

  it("emits core tick scheduler wakes through the NAPI bridge", async () => {
    const { NapiDirectDb } = await loadNapiModule();
    const wakes: string[] = [];
    const db = NapiDirectDb.openMemory(
      encodeDirectSchema(TEST_SCHEMA),
      openConfig(
        deterministicBytes("jazz-napi-core-runtime:scheduler-node"),
        deterministicBytes("jazz-napi-core-runtime:scheduler-author"),
        1,
        true,
      ),
    );

    db.setTickScheduler((error: Error | null, urgency: string) => {
      if (error) throw error;
      wakes.push(urgency);
    });
    const transport = db.connectUpstream();

    await waitFor(
      async () => (wakes.length > 0 ? wakes : undefined),
      "NAPI tick scheduler did not emit a wake",
    );

    expect(wakes).toContain("immediate");
    expect(transport.close()).toBe(true);
    db.close?.();
  });

  it("opens, mutates one row, and queries it through the direct WASM adapter shape", async () => {
    const { NapiDirectDb } = await loadNapiModule();
    const runtime = new CoreRuntime(
      { openMemory: (schema, config) => NapiDirectDb.openMemory(schema, config) as never },
      TEST_SCHEMA,
      deterministicBytes("jazz-napi-core-runtime:node"),
      deterministicBytes("jazz-napi-core-runtime:author"),
      1,
      true,
    );

    const inserted = runtime.insert("todos", {
      title: { type: "Text", value: "direct napi memory row" },
      done: { type: "Boolean", value: false },
    });

    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
      {
        id: inserted.id,
        table: "todos",
        values: [
          { type: "Text", value: "direct napi memory row" },
          { type: "Boolean", value: false },
        ],
      },
    ]);

    runtime.update("todos", inserted.id, {
      title: { type: "Text", value: "direct napi updated row" },
    });

    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
      {
        id: inserted.id,
        table: "todos",
        values: [
          { type: "Text", value: "direct napi updated row" },
          { type: "Boolean", value: false },
        ],
      },
    ]);

    runtime.delete("todos", inserted.id);

    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([]);
  });

  it("delivers direct NAPI subscription updates through the native handle", async () => {
    const { NapiDirectDb } = await loadNapiModule();
    const runtime = new CoreRuntime(
      { openMemory: (schema, config) => NapiDirectDb.openMemory(schema, config) as never },
      TEST_SCHEMA,
      deterministicBytes("jazz-napi-core-runtime-subscription:node"),
      deterministicBytes("jazz-napi-core-runtime-subscription:author"),
      21,
      true,
    );
    runtimes.push(runtime);

    const updates: unknown[] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "local");
    runtime.executeSubscription(handle, (delta: unknown) => {
      updates.push(delta);
    });

    expect(updates).toEqual([[]]);

    const inserted = runtime.insert("todos", {
      title: { type: "Text", value: "direct napi subscribed row" },
      done: { type: "Boolean", value: false },
    });

    expect(updates).toHaveLength(2);
    expect(updates[1]).toEqual([
      {
        kind: 0,
        id: inserted.id,
        index: 0,
        row: {
          id: inserted.id,
          values: [
            { type: "Text", value: "direct napi subscribed row" },
            { type: "Boolean", value: false },
          ],
        },
      },
    ]);

    runtime.update("todos", inserted.id, {
      title: { type: "Text", value: "direct napi subscribed updated row" },
    });

    expect(updates).toHaveLength(3);
    expect(updates[2]).toEqual([
      {
        kind: 0,
        id: inserted.id,
        index: 0,
        row: {
          id: inserted.id,
          values: [
            { type: "Text", value: "direct napi subscribed updated row" },
            { type: "Boolean", value: false },
          ],
        },
      },
    ]);

    runtime.unsubscribe(handle);
  });

  it("applies session ownership policy to local direct NAPI inserts and reads", async () => {
    const { NapiDirectDb } = await loadNapiModule();
    const runtime = new CoreRuntime(
      { openMemory: (schema, config) => NapiDirectDb.openMemory(schema, config) as never },
      OWNED_TODOS_SCHEMA,
      deterministicBytes("jazz-napi-core-runtime-policy:node"),
      deterministicBytes("jazz-napi-core-runtime-policy:author"),
      11,
      true,
    );
    const aliceSession = JSON.stringify({ user_id: ALICE_ID });
    const bobSession = JSON.stringify({ user_id: BOB_ID });

    const aliceTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "alice local row" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: ALICE_ID },
      },
      aliceSession,
    );
    await runtime.waitForTransaction(aliceTodo.transactionId, "local");

    const aliceRows = await runtime.query(
      JSON.stringify({ table: "todos" }),
      aliceSession,
      "local",
    );
    expect(aliceRows).toHaveLength(1);
    expect(aliceRows).toEqual([
      expect.objectContaining({
        id: aliceTodo.id,
        table: "todos",
      }),
    ]);
    expect((aliceRows as Array<{ values: unknown[] }>)[0]?.values.slice(0, 3)).toEqual([
      { type: "Text", value: "alice local row" },
      { type: "Boolean", value: false },
      { type: "Text", value: ALICE_ID },
    ]);

    try {
      const foreignOwnerTodo = runtime.insert(
        "todos",
        {
          title: { type: "Text", value: "alice cannot claim bob" },
          done: { type: "Boolean", value: false },
          owner_id: { type: "Text", value: BOB_ID },
        },
        aliceSession,
      );
      await runtime.waitForTransaction(foreignOwnerTodo.transactionId, "local");
    } catch (error) {
      if (!String(error).includes("policy denied INSERT on table todos")) throw error;
    }

    const aliceRowsAfterForeignOwnerInsert = await runtime.query(
      JSON.stringify({ table: "todos" }),
      aliceSession,
      "local",
    );
    expect(aliceRowsAfterForeignOwnerInsert).toHaveLength(1);
    expect(aliceRowsAfterForeignOwnerInsert).toEqual([
      expect.objectContaining({
        id: aliceTodo.id,
        table: "todos",
      }),
    ]);

    const bobTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "bob local row" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: BOB_ID },
      },
      bobSession,
    );
    await runtime.waitForTransaction(bobTodo.transactionId, "local");

    const aliceRowsAfterBobInsert = await runtime.query(
      JSON.stringify({ table: "todos" }),
      aliceSession,
      "local",
    );
    expect(aliceRowsAfterBobInsert).toHaveLength(1);
    expect(aliceRowsAfterBobInsert).toEqual([
      expect.objectContaining({
        id: aliceTodo.id,
        table: "todos",
      }),
    ]);
    expect(
      (aliceRowsAfterBobInsert as Array<{ values: unknown[] }>)[0]?.values.slice(0, 3),
    ).toEqual([
      { type: "Text", value: "alice local row" },
      { type: "Boolean", value: false },
      { type: "Text", value: ALICE_ID },
    ]);
  });

  it("applies session ownership policy to direct NAPI subscriptions", async () => {
    const { NapiDirectDb } = await loadNapiModule();
    const runtime = new CoreRuntime(
      { openMemory: (schema, config) => NapiDirectDb.openMemory(schema, config) as never },
      OWNED_TODOS_SCHEMA,
      deterministicBytes("jazz-napi-core-runtime-policy-subscription:node"),
      deterministicBytes("jazz-napi-core-runtime-policy-subscription:author"),
      14,
      true,
    );
    runtimes.push(runtime);

    const aliceSession = JSON.stringify({ user_id: ALICE_ID });
    const bobSession = JSON.stringify({ user_id: BOB_ID });
    const query = JSON.stringify({ table: "todos" });
    const aliceUpdates: unknown[] = [];

    const aliceHandle = runtime.createSubscription(query, aliceSession, "local");
    runtime.executeSubscription(aliceHandle, (delta: unknown) => {
      aliceUpdates.push(delta);
    });

    expect(aliceUpdates).toEqual([[]]);

    const aliceTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "alice subscribed row" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: ALICE_ID },
      },
      aliceSession,
    );
    const bobTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "bob subscribed row" },
        done: { type: "Boolean", value: true },
        owner_id: { type: "Text", value: BOB_ID },
      },
      bobSession,
    );

    await Promise.all([
      runtime.waitForTransaction(aliceTodo.transactionId, "local"),
      runtime.waitForTransaction(bobTodo.transactionId, "local"),
    ]);

    expect(await runtime.query(query, aliceSession, "local")).toEqual([
      expect.objectContaining({ id: aliceTodo.id }),
    ]);
    expect(await runtime.query(query, bobSession, "local")).toEqual([
      expect.objectContaining({ id: bobTodo.id }),
    ]);

    expect(aliceUpdates).toEqual([
      [],
      [
        expect.objectContaining({
          kind: 0,
          id: aliceTodo.id,
          row: expect.objectContaining({
            id: aliceTodo.id,
            values: [
              { type: "Text", value: "alice subscribed row" },
              { type: "Boolean", value: false },
              { type: "Text", value: ALICE_ID },
            ],
          }),
        }),
      ],
    ]);

    runtime.unsubscribe(aliceHandle);
  });

  it("isolates two session identities sharing one direct NAPI runtime for owned deletes", async () => {
    const { NapiDirectDb } = await loadNapiModule();
    const runtime = new CoreRuntime(
      { openMemory: (schema, config) => NapiDirectDb.openMemory(schema, config) as never },
      OWNED_TODOS_SCHEMA,
      deterministicBytes("jazz-napi-core-runtime-delete-policy:node"),
      deterministicBytes("jazz-napi-core-runtime-delete-policy:author"),
      12,
      true,
    );
    const aliceSession = JSON.stringify({ user_id: ALICE_ID });
    const bobSession = JSON.stringify({ user_id: BOB_ID });

    const aliceTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "alice delete row" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: ALICE_ID },
      },
      aliceSession,
    );
    const bobTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "bob delete row" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: BOB_ID },
      },
      bobSession,
    );

    await Promise.all([
      runtime.waitForTransaction(aliceTodo.transactionId, "local"),
      runtime.waitForTransaction(bobTodo.transactionId, "local"),
    ]);

    expect(() => runtime.delete("todos", bobTodo.id, aliceSession)).toThrow(
      'Delete failed: WriteError("policy denied DELETE on table todos")',
    );
    expect(() => runtime.delete("todos", aliceTodo.id, bobSession)).toThrow(
      'Delete failed: WriteError("policy denied DELETE on table todos")',
    );

    const aliceDelete = runtime.delete("todos", aliceTodo.id, aliceSession);
    const bobDelete = runtime.delete("todos", bobTodo.id, bobSession);

    await Promise.all([
      runtime.waitForTransaction(aliceDelete.transactionId, "local"),
      runtime.waitForTransaction(bobDelete.transactionId, "local"),
    ]);

    await expect(runtime.query(JSON.stringify({ table: "todos" }), aliceSession)).resolves.toEqual(
      [],
    );
    await expect(runtime.query(JSON.stringify({ table: "todos" }), bobSession)).resolves.toEqual(
      [],
    );
  });

  it("isolates two session identities sharing one upstream direct NAPI runtime for owned deletes", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const { NapiDirectDb } = await loadNapiModule();
    const appId = "00000000-0000-0000-0000-00000000d003";
    server = await startLocalJazzServer({
      appId,
      inMemory: true,
      adminSecret: "direct-napi-owned-delete-admin",
      schema: encodeDirectSchema(OWNED_TODOS_SCHEMA),
    });

    const runtime = new CoreRuntime(
      { openMemory: (schema, config) => NapiDirectDb.openMemory(schema, config) as never },
      OWNED_TODOS_SCHEMA,
      deterministicBytes("jazz-napi-core-runtime-edge-delete-policy:node"),
      deterministicBytes("jazz-napi-core-runtime-edge-delete-policy:author"),
      13,
      true,
    );
    runtimes.push(runtime);
    runtime.connect(
      directWebSocketUrl(
        server.url,
        appId,
        deterministicBytes("jazz-napi-core-runtime-edge-delete-policy:author"),
      ),
      JSON.stringify({ admin_secret: server.adminSecret }),
    );

    const aliceSession = JSON.stringify({ user_id: ALICE_ID });
    const bobSession = JSON.stringify({ user_id: BOB_ID });

    const aliceTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "alice edge delete row" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: ALICE_ID },
      },
      aliceSession,
    );
    const bobTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "bob edge delete row" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: BOB_ID },
      },
      bobSession,
    );

    await Promise.all([
      waitForPromise(
        runtime.waitForTransaction(aliceTodo.transactionId, "edge"),
        "alice insert did not settle at edge",
      ),
      waitForPromise(
        runtime.waitForTransaction(bobTodo.transactionId, "edge"),
        "bob insert did not settle at edge",
      ),
    ]);

    expect(() => runtime.delete("todos", bobTodo.id, aliceSession)).toThrow(
      'Delete failed: WriteError("policy denied DELETE on table todos")',
    );
    expect(() => runtime.delete("todos", aliceTodo.id, bobSession)).toThrow(
      'Delete failed: WriteError("policy denied DELETE on table todos")',
    );

    const aliceDelete = runtime.delete("todos", aliceTodo.id, aliceSession);
    const bobDelete = runtime.delete("todos", bobTodo.id, bobSession);

    await Promise.all([
      waitForPromise(
        runtime.waitForTransaction(aliceDelete.transactionId, "edge"),
        "alice delete did not settle at edge",
      ),
      waitForPromise(
        runtime.waitForTransaction(bobDelete.transactionId, "edge"),
        "bob delete did not settle at edge",
      ),
    ]);

    await expect(
      runtime.query(JSON.stringify({ table: "todos" }), aliceSession, "edge"),
    ).resolves.toEqual([]);
    await expect(
      runtime.query(JSON.stringify({ table: "todos" }), bobSession, "edge"),
    ).resolves.toEqual([]);
  }, 15_000);

  it("isolates two session identities sharing one persistent upstream direct NAPI runtime for owned deletes", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const { NapiDirectDb } = await loadNapiModule();
    const appId = "00000000-0000-0000-0000-00000000d004";
    const tempDir = mkdtempSync(join(tmpdir(), "jazz-napi-direct-owned-delete-"));
    server = await startLocalJazzServer({
      appId,
      inMemory: true,
      adminSecret: "direct-napi-persistent-owned-delete-admin",
      schema: encodeDirectSchema(OWNED_TODOS_SCHEMA),
    });

    try {
      const runtime = new CoreRuntime(
        {
          openMemory: (schema, config) => NapiDirectDb.openMemory(schema, config) as never,
          openPersistent: (path, schema, config) =>
            NapiDirectDb.openPersistent(path, schema, config) as never,
        },
        OWNED_TODOS_SCHEMA,
        deterministicBytes("jazz-napi-core-runtime-persistent-edge-delete-policy:node"),
        deterministicBytes("jazz-napi-core-runtime-persistent-edge-delete-policy:author"),
        14,
        true,
        { persistentPath: join(tempDir, "db") },
      );
      runtimes.push(runtime);
      runtime.connect(
        directWebSocketUrl(
          server.url,
          appId,
          deterministicBytes("jazz-napi-core-runtime-persistent-edge-delete-policy:author"),
        ),
        JSON.stringify({ admin_secret: server.adminSecret }),
      );

      const aliceSession = JSON.stringify({ user_id: ALICE_ID });
      const bobSession = JSON.stringify({ user_id: BOB_ID });

      const aliceTodo = runtime.insert(
        "todos",
        {
          title: { type: "Text", value: "alice persistent edge delete row" },
          done: { type: "Boolean", value: false },
          owner_id: { type: "Text", value: ALICE_ID },
        },
        aliceSession,
      );
      const bobTodo = runtime.insert(
        "todos",
        {
          title: { type: "Text", value: "bob persistent edge delete row" },
          done: { type: "Boolean", value: false },
          owner_id: { type: "Text", value: BOB_ID },
        },
        bobSession,
      );

      await Promise.all([
        waitForPromise(
          runtime.waitForTransaction(aliceTodo.transactionId, "edge"),
          "alice persistent insert did not settle at edge",
        ),
        waitForPromise(
          runtime.waitForTransaction(bobTodo.transactionId, "edge"),
          "bob persistent insert did not settle at edge",
        ),
      ]);

      expect(() => runtime.delete("todos", bobTodo.id, aliceSession)).toThrow(
        'Delete failed: WriteError("policy denied DELETE on table todos")',
      );
      expect(() => runtime.delete("todos", aliceTodo.id, bobSession)).toThrow(
        'Delete failed: WriteError("policy denied DELETE on table todos")',
      );

      const aliceDelete = runtime.delete("todos", aliceTodo.id, aliceSession);
      const bobDelete = runtime.delete("todos", bobTodo.id, bobSession);

      await Promise.all([
        waitForPromise(
          runtime.waitForTransaction(aliceDelete.transactionId, "edge"),
          "alice persistent delete did not settle at edge",
        ),
        waitForPromise(
          runtime.waitForTransaction(bobDelete.transactionId, "edge"),
          "bob persistent delete did not settle at edge",
        ),
      ]);

      await expect(
        runtime.query(JSON.stringify({ table: "todos" }), aliceSession, "edge"),
      ).resolves.toEqual([]);
      await expect(
        runtime.query(JSON.stringify({ table: "todos" }), bobSession, "edge"),
      ).resolves.toEqual([]);
    } finally {
      rmSync(tempDir, { recursive: true, force: true });
    }
  }, 15_000);

  it("supports direct runtime parity writes, mergeable transactions, and upstream transport", async () => {
    const { NapiDirectDb } = await loadNapiModule();
    const runtime = new CoreRuntime(
      { openMemory: (schema, config) => NapiDirectDb.openMemory(schema, config) as never },
      TEST_SCHEMA,
      deterministicBytes("jazz-napi-core-runtime-parity:node"),
      deterministicBytes("jazz-napi-core-runtime-parity:author"),
      2,
      true,
    );

    const inserted = runtime.insert("todos", {
      title: { type: "Text", value: "direct napi parity row" },
      done: { type: "Boolean", value: false },
    });
    runtime.delete("todos", inserted.id);
    runtime.restore("todos", inserted.id, {
      title: { type: "Text", value: "direct napi restored row" },
      done: { type: "Boolean", value: false },
    });
    runtime.upsert("todos", "11111111-1111-4111-8111-111111111111", {
      title: { type: "Text", value: "direct napi upserted row" },
      done: { type: "Boolean", value: false },
    });

    const tx = runtime.beginTransaction("mergeable");
    runtime.update(
      "todos",
      inserted.id,
      { done: { type: "Boolean", value: true } },
      JSON.stringify({ batch_id: tx }),
    );
    runtime.upsert(
      "todos",
      inserted.id,
      {
        title: { type: "Text", value: "direct napi tx upserted row" },
        done: { type: "Boolean", value: true },
      },
      JSON.stringify({ batch_id: tx }),
    );
    runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "direct napi tx row" },
        done: { type: "Boolean", value: false },
      },
      JSON.stringify({ batch_id: tx }),
      "22222222-2222-4222-8222-222222222222",
    );
    runtime.commitTransaction(tx);
    await runtime.waitForTransaction(tx, "local");

    const rows = await runtime.query(JSON.stringify({ table: "todos" }));
    expect(rows).toHaveLength(3);
    expect(rows).toEqual(
      expect.arrayContaining([
        {
          id: inserted.id,
          table: "todos",
          values: [
            { type: "Text", value: "direct napi tx upserted row" },
            { type: "Boolean", value: true },
          ],
        },
        {
          id: "11111111-1111-4111-8111-111111111111",
          table: "todos",
          values: [
            { type: "Text", value: "direct napi upserted row" },
            { type: "Boolean", value: false },
          ],
        },
        {
          id: "22222222-2222-4222-8222-222222222222",
          table: "todos",
          values: [
            { type: "Text", value: "direct napi tx row" },
            { type: "Boolean", value: false },
          ],
        },
      ]),
    );

    const transport = runtime.connectUpstreamPeer();
    expect(transport.tick()).toBeGreaterThanOrEqual(0);
    expect(transport.recvWireFrames()).toEqual(expect.any(Array));
    expect(transport.close()).toBe(true);
    expect(transport.close()).toBe(false);
  });

  it("propagates an edge-tier query over the core runtime/server boundary and returns remote row adds", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const { NapiDirectDb } = await loadNapiModule();
    const appId = "00000000-0000-0000-0000-00000000d001";
    server = await startLocalJazzServer({
      appId,
      inMemory: true,
      adminSecret: "direct-napi-edge-query-admin",
      schema: encodeDirectSchema(TEST_SCHEMA),
    });

    const openRuntime = (peer: string, sourceId: number) => {
      const runtime = new CoreRuntime(
        { openMemory: (schema, config) => NapiDirectDb.openMemory(schema, config) as never },
        TEST_SCHEMA,
        deterministicBytes(`jazz-napi-direct-edge:${peer}:node`),
        deterministicBytes(`jazz-napi-direct-edge:${peer}:author`),
        sourceId,
        true,
      );
      runtimes.push(runtime);
      runtime.connect(
        directWebSocketUrl(
          server!.url,
          appId,
          deterministicBytes(`jazz-napi-direct-edge:${peer}:author`),
        ),
        JSON.stringify({ admin_secret: server!.adminSecret }),
      );
      return runtime;
    };

    const writer = openRuntime("writer", 31);
    const reader = openRuntime("reader", 32);
    const queryJson = JSON.stringify({ table: "todos" });

    expect(await reader.query(queryJson, null, "local")).toEqual([]);

    const inserted = writer.insert("todos", {
      title: { type: "Text", value: "direct napi propagated edge row" },
      done: { type: "Boolean", value: false },
    });
    await waitForPromise(
      writer.waitForTransaction(inserted.transactionId, "edge"),
      "writer insert did not settle at edge",
    );

    const propagatedRow = await waitFor(async () => {
      const rows = (await reader.query(queryJson, null, "edge")) as Array<{
        id: string;
        table: string;
        values: unknown[];
      }>;
      return rows.find((row) => row.id === inserted.id);
    }, "reader edge query did not receive the propagated row add");

    expect(propagatedRow).toEqual({
      id: inserted.id,
      table: "todos",
      values: [
        { type: "Text", value: "direct napi propagated edge row" },
        { type: "Boolean", value: false },
      ],
    });
  }, 15_000);

  it("propagates an edge-tier query through a persistent core server", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const { NapiDirectDb } = await loadNapiModule();
    const appId = "00000000-0000-0000-0000-00000000d002";
    const tempDir = mkdtempSync(join(tmpdir(), "jazz-napi-direct-server-"));
    server = await startLocalJazzServer({
      appId,
      dataDir: tempDir,
      adminSecret: "direct-napi-persistent-edge-query-admin",
      schema: encodeDirectSchema(TEST_SCHEMA),
    });

    const openRuntime = (peer: string, sourceId: number, targetServer: LocalJazzServerHandle) => {
      const runtime = new CoreRuntime(
        { openMemory: (schema, config) => NapiDirectDb.openMemory(schema, config) as never },
        TEST_SCHEMA,
        deterministicBytes(`jazz-napi-direct-persistent-edge:${peer}:node`),
        deterministicBytes(`jazz-napi-direct-persistent-edge:${peer}:author`),
        sourceId,
        true,
      );
      runtimes.push(runtime);
      runtime.connect(
        directWebSocketUrl(
          targetServer.url,
          appId,
          deterministicBytes(`jazz-napi-direct-persistent-edge:${peer}:author`),
        ),
        JSON.stringify({ admin_secret: targetServer.adminSecret }),
      );
      return runtime;
    };

    try {
      const writer = openRuntime("writer", 41, server);

      const inserted = writer.insert("todos", {
        title: { type: "Text", value: "direct napi persistent propagated edge row" },
        done: { type: "Boolean", value: false },
      });
      await waitForPromise(
        writer.waitForTransaction(inserted.transactionId, "edge"),
        "writer insert did not settle at persistent edge",
      );
      writer.close();
      runtimes.splice(runtimes.indexOf(writer), 1);

      await server.stop();
      server = await startLocalJazzServer({
        appId,
        dataDir: tempDir,
        adminSecret: "direct-napi-persistent-edge-query-admin",
        schema: encodeDirectSchema(TEST_SCHEMA),
      });

      const reader = openRuntime("reader", 42, server);
      const queryJson = JSON.stringify({ table: "todos" });
      const propagatedRow = await waitFor(async () => {
        const rows = (await reader.query(queryJson, null, "edge")) as Array<{
          id: string;
          table: string;
          values: unknown[];
        }>;
        return rows.find((row) => row.id === inserted.id);
      }, "reader persistent edge query did not receive the propagated row add after server recovery");

      expect(propagatedRow).toEqual({
        id: inserted.id,
        table: "todos",
        values: [
          { type: "Text", value: "direct napi persistent propagated edge row" },
          { type: "Boolean", value: false },
        ],
      });
    } finally {
      rmSync(tempDir, { recursive: true, force: true });
    }
  }, 15_000);

  it("reopens a persistent direct DB and reads previously written rows", async () => {
    const { NapiDirectDb } = await loadNapiModule();
    const tempDir = mkdtempSync(join(tmpdir(), "jazz-napi-direct-"));
    const dataPath = join(tempDir, "db");
    const node = deterministicBytes("jazz-napi-direct-persistent:node");
    const author = deterministicBytes("jazz-napi-direct-persistent:author");
    let firstRuntime: CoreRuntime | null = null;
    let secondRuntime: CoreRuntime | null = null;

    try {
      firstRuntime = new CoreRuntime(
        {
          openMemory: (schema, config) => NapiDirectDb.openMemory(schema, config) as never,
          openPersistent: (path, schema, config) =>
            NapiDirectDb.openPersistent(path, schema, config) as never,
        },
        TEST_SCHEMA,
        node,
        author,
        7,
        true,
        { persistentPath: dataPath },
      );

      const inserted = firstRuntime.insert("todos", {
        title: { type: "Text", value: "direct napi persistent row" },
        done: { type: "Boolean", value: false },
      });
      await firstRuntime.waitForTransaction(inserted.transactionId, "local");
      firstRuntime.close();
      firstRuntime = null;

      secondRuntime = new CoreRuntime(
        {
          openMemory: (schema, config) => NapiDirectDb.openMemory(schema, config) as never,
          openPersistent: (path, schema, config) =>
            NapiDirectDb.openPersistent(path, schema, config) as never,
        },
        TEST_SCHEMA,
        node,
        author,
        7,
        true,
        { persistentPath: dataPath },
      );

      await expect(secondRuntime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
        {
          id: inserted.id,
          table: "todos",
          values: [
            { type: "Text", value: "direct napi persistent row" },
            { type: "Boolean", value: false },
          ],
        },
      ]);
      secondRuntime.close();
      secondRuntime = null;
    } finally {
      firstRuntime?.close();
      secondRuntime?.close();
      rmSync(tempDir, { recursive: true, force: true });
    }
  });
});

function deterministicBytes(seed: string): Uint8Array {
  let hash = 0x811c9dc5;
  const bytes = new Uint8Array(16);
  const view = new DataView(bytes.buffer);
  for (let round = 0; round < 4; round += 1) {
    for (let i = 0; i < seed.length; i += 1) {
      hash ^= seed.charCodeAt(i) + round;
      hash = Math.imul(hash, 0x01000193);
    }
    view.setUint32(round * 4, hash >>> 0, true);
  }
  return bytes;
}

async function waitFor<T>(
  read: () => Promise<T | undefined>,
  message: string,
  timeoutMs = 5_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  do {
    const value = await read();
    if (value !== undefined) return value;
    await sleep(25);
  } while (Date.now() < deadline);
  throw new Error(message);
}

async function waitForPromise<T>(
  promise: Promise<T>,
  message: string,
  timeoutMs = 5_000,
): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timeout = setTimeout(() => reject(new Error(message)), timeoutMs);
  });
  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}
